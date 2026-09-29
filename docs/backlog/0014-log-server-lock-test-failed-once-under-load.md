# 0014 — The log server's second-server test failed once under heavy load

Status: parked (recorded 2026-09-29)
Opened: 2026-09-29 · Area: `apps/durable-streams/tests/` (`a_second_server_on_the_same_data_dir_is_refused`),
the test helper `unused_local_port()`
Reopen trigger: the test failing a second time, anywhere.

## The fact

- The test starts a second log server on a data directory a first one holds, and expects it to
  refuse and exit. Once, the second server was still running after the test's 10 s, with nothing on
  its stderr.
- It happened once, while the machine was heavily loaded (load average 12). It did not happen again
  in 30 serial runs, in 25 runs under CPU load, or in 25 runs of the binary built from `develop`.

## Candidate causes, neither established

- `unused_local_port()` picks a port that is free when asked and may be taken by the time the server
  binds it, so the second server could have been waiting on something other than the lock.
- The 10 s budget may simply be too short for a machine that busy.
