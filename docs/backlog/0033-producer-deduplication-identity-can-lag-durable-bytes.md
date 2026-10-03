# 0033 — Producer deduplication identity can lag durable bytes

Status: candidate (recorded 2026-10-03)
Opened: 2026-10-03 · Area: log-server producer headers, WAL recovery and metadata
Reopen trigger: execute a producer-only append/crash/retry regression, or adopt producer headers
for a workload requiring duplicate suppression across a log-server crash.

During [0011](0011-a-forced-exit-replays-the-checkpoint-window.md), source review found that ordinary
WAL append records retain bytes without producer ID/epoch/sequence. Request completion promotes
producer state after durability; the metadata sidecar can trail those bytes. The selected repair
couples `Stream-Seq` to bytes and checkpoint proofs, but does not add producer counters to that
proof. Conditional sequence requests deliberately reject combined producer headers.

Inference: a producer-only retry after a log-server crash can lack the identity required to suppress
already durable bytes. No fresh producer-only semantic red or production incident is claimed.
The engine's input checkpoint/highwater and catalog event IDs have their own duplicate defenses;
this entry concerns the public producer-header contract, not evidence that those defenses failed.

First qualification should use a real WAL handler and recovery, retain bytes before the producer
sidecar lands, and retry the same producer tuple. Cover lost acknowledgment, checkpoint before
callback promotion, compaction and a second boot. If reproduced, couple producer identity to the
same accepted-byte proof; flushing metadata after bytes alone still leaves an uncertain crash window.
