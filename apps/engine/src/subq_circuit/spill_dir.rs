use std::fs::{DirBuilder, File, OpenOptions};
use std::io;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// An OS lease protects a circuit's disposable files even when processes share a temporary
/// filesystem across PID namespaces. The registry lock serializes publication and sweeping.
pub(super) struct OwnedSpillDirectory {
    directory: PathBuf,
    data: PathBuf,
    _lease: File,
}

impl OwnedSpillDirectory {
    pub(super) fn create() -> io::Result<Self> {
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        // SAFETY: getuid reads this process's credentials and cannot fail.
        let uid = unsafe { libc::getuid() };
        // Older engines sweep numeric names under circuits-subq using namespace-local PIDs.
        // Keep these directories out of their reach, and leave unverifiable legacy owners alone.
        let base = std::env::temp_dir().join(format!("circuits-subq-owned-{uid}"));
        Self::create_in(&base, std::process::id(), SEQUENCE.fetch_add(1, Ordering::Relaxed))
    }

    fn create_in(base: &Path, pid: u32, mut sequence: u64) -> io::Result<Self> {
        let registry = lock_registry(base)?;
        sweep(base);
        let directory = loop {
            let candidate = base.join(format!("spill-{pid}-{sequence}"));
            match DirBuilder::new().mode(0o700).create(&candidate) {
                Ok(()) => break candidate,
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => sequence += 1,
                Err(error) => return Err(error),
            }
        };
        let result = (|| {
            let lease =
                OpenOptions::new().read(true).write(true).create_new(true).mode(0o600).open(directory.join("owner"))?;
            lease.lock()?;
            let data = directory.join("data");
            std::fs::create_dir(&data)?;
            Ok(Self { data, directory: directory.clone(), _lease: lease })
        })();
        if result.is_err() {
            let _ = std::fs::remove_dir_all(&directory);
        }
        // A fully constructed guard may acquire this lock again during Drop.
        drop(registry);
        result
    }

    pub(super) fn path(&self) -> &Path {
        &self.data
    }
}

impl Drop for OwnedSpillDirectory {
    fn drop(&mut self) {
        // Keep the lease until removal completes. A failed removal remains reclaimable at the
        // next startup once the file descriptor closes, including after initialization fails.
        let remove = || -> io::Result<()> {
            let _registry = lock_registry(self.directory.parent().expect("spill directory has a parent"))?;
            std::fs::remove_dir_all(&self.directory)
        };
        if let Err(error) = remove() {
            tracing::warn!("could not remove membership spill directory {}: {error}", self.directory.display());
        }
    }
}

fn lock_registry(base: &Path) -> io::Result<File> {
    DirBuilder::new().recursive(true).mode(0o700).create(base)?;
    let metadata = std::fs::symlink_metadata(base)?;
    // SAFETY: getuid reads this process's credentials and cannot fail.
    let uid = unsafe { libc::getuid() };
    if !metadata.is_dir() || metadata.uid() != uid || metadata.mode() & 0o077 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "membership spill root must be a private owned directory",
        ));
    }
    let registry = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(base.join("registry"))?;
    registry.lock()?;
    Ok(registry)
}

/// Caller owns the registry lock, so a missing owner file can only be an interrupted startup.
/// Failed opens and locks are retained conservatively. The lease is held throughout removal.
fn sweep(base: &Path) {
    let Ok(entries) = std::fs::read_dir(base) else { return };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(name) = name.to_str().and_then(|name| name.strip_prefix("spill-")) else { continue };
        let Some((pid, sequence)) = name.split_once('-') else { continue };
        if pid.parse::<u32>().is_err() || sequence.parse::<u64>().is_err() {
            continue;
        }
        if !entry.file_type().is_ok_and(|kind| kind.is_dir()) {
            continue;
        }
        let directory = entry.path();
        let lease = match OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(directory.join("owner"))
        {
            Ok(lease) => {
                if lease.try_lock().is_err() {
                    continue;
                }
                Some(lease)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(_) => continue,
        };
        if let Err(error) = std::fs::remove_dir_all(&directory) {
            tracing::warn!("could not sweep membership spill directory {}: {error}", directory.display());
        }
        drop(lease);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Write};
    use std::os::unix::fs::{PermissionsExt, symlink};
    use std::process::{Child, Command, Stdio};
    use std::time::Duration;

    struct ChildOwner(Child);

    impl Drop for ChildOwner {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    fn test_base() -> PathBuf {
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        loop {
            let base = std::env::temp_dir().join(format!(
                "circuits-spill-ownership-test-{}-{}",
                std::process::id(),
                SEQUENCE.fetch_add(1, Ordering::Relaxed)
            ));
            match DirBuilder::new().mode(0o700).create(&base) {
                Ok(()) => return base,
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => panic!("could not create test directory: {error}"),
            }
        }
    }

    #[test]
    fn child_owner() {
        let Some(base) = std::env::var_os("CIRCUITS_SPILL_TEST_ROOT") else { return };
        // Pretend both independent processes are pid 1 in their own namespace.
        let owner = OwnedSpillDirectory::create_in(Path::new(&base), 1, 0).unwrap();
        std::fs::write(owner.path().join("live"), "held").unwrap();
        println!("SPILL_OWNER_READY");
        io::stdout().flush().unwrap();
        let mut line = String::new();
        io::stdin().read_line(&mut line).unwrap();
        drop(owner);
    }

    #[test]
    fn independent_owner_survives_pid_collision_and_is_swept_after_crash() {
        let base = test_base();
        let mut child = ChildOwner(
            Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "subq_circuit::spill_dir::tests::child_owner", "--nocapture"])
                .env("CIRCUITS_SPILL_TEST_ROOT", &base)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .unwrap(),
        );
        let stdout = child.0.stdout.take().unwrap();
        let (ready, received) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if line == "SPILL_OWNER_READY" {
                    let _ = ready.send(());
                    return;
                }
            }
        });
        received.recv_timeout(Duration::from_secs(10)).expect("child must acquire its actual OS lease");
        let parent = OwnedSpillDirectory::create_in(&base, 1, 0).unwrap();
        assert_eq!(parent.path(), base.join("spill-1-1/data"));
        assert_eq!(std::fs::read_to_string(base.join("spill-1-0/data/live")).unwrap(), "held");
        // Killing the independent process releases its lease without running directory cleanup.
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        let next = OwnedSpillDirectory::create_in(&base, 1, 2).unwrap();
        assert!(!base.join("spill-1-0").exists(), "crashed owner's data must be reclaimed");
        assert!(parent.path().exists(), "another live owner must remain intact during reclamation");
        drop(parent);
        drop(next);
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn aborted_startup_is_reclaimed_and_guard_cleans_initialization_failure() {
        let base = test_base();
        {
            let _registry = lock_registry(&base).unwrap();
            std::fs::create_dir(base.join("spill-1-0")).unwrap();
            let contender = OpenOptions::new().read(true).write(true).open(base.join("registry")).unwrap();
            assert!(contender.try_lock().is_err(), "sweeper cannot enter while startup has not published its lease");
        }
        let owner = OwnedSpillDirectory::create_in(&base, 1, 1).unwrap();
        assert!(!base.join("spill-1-0").exists(), "incomplete publication is reclaimed under registry lock");
        let path = owner.path().to_owned();
        let initialize = || -> io::Result<()> {
            let _owner = owner;
            Err(io::Error::other("injected initialization failure"))
        };
        assert!(initialize().is_err());
        assert!(!path.exists(), "failed circuit initialization must release and remove its storage");
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn unsafe_roots_are_rejected_before_sweeping() {
        let base = test_base();
        let stale = base.join("spill-1-0");
        std::fs::create_dir(&stale).unwrap();
        std::fs::set_permissions(&base, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(OwnedSpillDirectory::create_in(&base, 1, 1).is_err());
        assert!(stale.exists(), "an untrusted root must never be swept");
        std::fs::set_permissions(&base, std::fs::Permissions::from_mode(0o700)).unwrap();
        let alias = base.join("alias");
        symlink(&base, &alias).unwrap();
        assert!(OwnedSpillDirectory::create_in(&alias, 1, 1).is_err());
        assert!(stale.exists(), "a symlinked root must never be swept");
        std::fs::remove_dir_all(base).unwrap();
    }
}
