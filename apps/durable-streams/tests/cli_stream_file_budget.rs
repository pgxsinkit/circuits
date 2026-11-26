//! A WAL server holding more streams than its open-file limit keeps serving.
//!
//! Production (2026-10-23) ran out of file descriptors after 15 days: every
//! stream's data file stayed open for the life of the process, so ~65k streams
//! filled the container's 65,536 `nofile` limit, and from then on WAL
//! checkpoints failed with `Too many open files (os error 24)` and requests on
//! cold streams failed. This drives the real binary under a deliberately small
//! `RLIMIT_NOFILE` and asserts the server still creates, appends to, reads and
//! checkpoints streams well past that many.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// The soft `nofile` limit the server runs under, and how many streams the test
/// creates: comfortably more streams than descriptors.
const NOFILE: u64 = 256;
const STREAMS: usize = 600;

fn server() -> Command {
    Command::new(env!("CARGO_BIN_EXE_durable-streams-server"))
}

fn unused_local_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0)).expect("reserve test port").local_addr().unwrap().port()
}

struct ServerUnderTest {
    child: Child,
    dir: std::path::PathBuf,
    log: std::path::PathBuf,
    port: u16,
}

impl ServerUnderTest {
    /// Spawn a WAL server whose `RLIMIT_NOFILE` (soft and hard) is `nofile`, logging stderr
    /// to a file next to the data directory.
    fn wal_with_nofile(dir: &std::path::Path, nofile: u64) -> Self {
        let log = dir.with_extension("stderr.log");
        let _ = std::fs::remove_file(&log);
        let (child, port) = Self::spawn(dir, &log, nofile);
        Self { child, dir: dir.to_path_buf(), log, port }
    }

    /// Stop the server and start it again over the same data directory, so boot
    /// recovery has to bring back every stream under the same limit.
    fn restart(&mut self, nofile: u64) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let (child, port) = Self::spawn(&self.dir, &self.log, nofile);
        self.child = child;
        self.port = port;
    }

    fn spawn(dir: &std::path::Path, log: &std::path::Path, nofile: u64) -> (Child, u16) {
        let port = unused_local_port();
        let stderr = std::fs::OpenOptions::new().create(true).append(true).open(log).expect("open stderr log");
        let mut command = server();
        command
            .args(["--durability", "wal", "--data-dir"])
            .arg(dir)
            .args(["--port", &port.to_string()])
            .args(["--wal-shards", "1", "--stream-lanes", "1"])
            .stdout(Stdio::null())
            .stderr(Stdio::from(stderr));
        // SAFETY: setrlimit is async-signal-safe and touches only the child.
        unsafe {
            command.pre_exec(move || {
                let mut limit = libc::rlimit { rlim_cur: 0, rlim_max: 0 };
                if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                // Lower the hard limit too: the server raises its soft limit to
                // the hard limit at startup.
                limit.rlim_cur = nofile;
                limit.rlim_max = nofile;
                if libc::setrlimit(libc::RLIMIT_NOFILE, &limit) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        (command.spawn().expect("spawn server"), port)
    }

    fn request(&self, request: &str) -> String {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match TcpStream::connect(("127.0.0.1", self.port)) {
                Ok(mut stream) => {
                    stream.write_all(request.as_bytes()).expect("write request");
                    let mut response = Vec::new();
                    let _ = stream.read_to_end(&mut response);
                    return String::from_utf8_lossy(&response).into_owned();
                }
                Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(25)),
                Err(error) => panic!("server did not listen: {error}"),
            }
        }
    }

    fn create(&self, path: &str) -> String {
        self.request(&format!(
            "PUT {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Type: application/octet-stream\r\nContent-Length: 0\r\n\r\n"
        ))
    }

    fn append(&self, path: &str, body: &str) -> String {
        self.request(&format!(
            "POST {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        ))
    }

    fn read(&self, path: &str) -> String {
        self.request(&format!("GET {path}?offset=-1 HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"))
    }

    fn stderr(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }
}

impl Drop for ServerUnderTest {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
        let _ = std::fs::remove_file(&self.log);
    }
}

fn status_line(response: &str) -> &str {
    response.lines().next().unwrap_or("")
}

fn body(response: &str) -> &str {
    response.split_once("\r\n\r\n").map(|(_, b)| b).unwrap_or("")
}

#[test]
fn wal_server_serves_and_checkpoints_more_streams_than_its_fd_limit() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("data");
    let mut server = ServerUnderTest::wal_with_nofile(&dir, NOFILE);

    for i in 0..STREAMS {
        let path = format!("/fd-budget/s{i}");
        let created = server.create(&path);
        assert!(
            created.starts_with("HTTP/1.1 201"),
            "create {path} (stream {i} of {STREAMS}, nofile {NOFILE}) failed: {}\nstderr tail:\n{}",
            status_line(&created),
            tail(&server.stderr())
        );
        let appended = server.append(&path, &format!("payload-{i}"));
        assert!(
            appended.starts_with("HTTP/1.1 204"),
            "append {path} failed: {}\nstderr tail:\n{}",
            status_line(&appended),
            tail(&server.stderr())
        );
    }

    // Every stream, including ones idle since the start, still reads back.
    for i in 0..STREAMS {
        let path = format!("/fd-budget/s{i}");
        let read = server.read(&path);
        assert!(
            read.starts_with("HTTP/1.1 200"),
            "read {path} failed: {}\nstderr tail:\n{}",
            status_line(&read),
            tail(&server.stderr())
        );
        assert_eq!(body(&read), format!("payload-{i}"), "read {path} body");
    }

    // Let at least one WAL checkpoint run over the whole dirty set, then append
    // to and read an old stream again.
    std::thread::sleep(Duration::from_secs(5));
    let appended = server.append("/fd-budget/s0", "-again");
    assert!(appended.starts_with("HTTP/1.1 204"), "re-append: {}", status_line(&appended));
    let read = server.read("/fd-budget/s0");
    assert_eq!(body(&read), "payload-0-again");

    assert!(server.child.try_wait().expect("try_wait").is_none(), "server exited:\n{}", tail(&server.stderr()));
    // A restart recovers every stream without holding one descriptor each.
    server.restart(NOFILE);
    for i in (0..STREAMS).step_by(37) {
        let path = format!("/fd-budget/s{i}");
        let read = server.read(&path);
        assert!(
            read.starts_with("HTTP/1.1 200"),
            "read {path} after restart failed: {}\nstderr tail:\n{}",
            status_line(&read),
            tail(&server.stderr())
        );
    }
    assert!(
        server.child.try_wait().expect("try_wait").is_none(),
        "server exited after restart:\n{}",
        tail(&server.stderr())
    );

    let stderr = server.stderr();
    // The descriptor gauge the deployment alarms on is reported.
    assert!(stderr.contains(r#""event":"ds_fd_stats""#), "no ds_fd_stats line:\n{}", tail(&stderr));
    assert!(
        !stderr.contains("Too many open files") && !stderr.contains("os error 24"),
        "server ran out of file descriptors:\n{}",
        tail(&stderr)
    );
    assert!(!stderr.contains("checkpoint failed"), "a WAL checkpoint failed:\n{}", tail(&stderr));
}

fn tail(text: &str) -> String {
    let lines: Vec<&str> = text.lines().collect();
    lines[lines.len().saturating_sub(20)..].join("\n")
}
