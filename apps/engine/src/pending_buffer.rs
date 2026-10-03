//! Process-local FIFO payloads with one nonblocking RAM reservation budget shared across queues.
//!
//! This bounds retained queue payload estimates, not total engine RSS: the caller's source page,
//! the returned record, decoding scratch, and derived state remain separate. A queue switches to
//! a private file when reservation fails, preserving FIFO order without waiting for permits that
//! its own eventual drain would release. Empty queues return to the RAM path.
//!
//! File I/O is synchronous, as in `txn_buffer`: streaming a borrowed record avoids cloning an
//! oversized transaction or building a serialized `Vec`. A spill can therefore occupy a runtime
//! worker; callers must not interpret this as asynchronous disk I/O. Files are disposable scratch,
//! never recovery state. An I/O or codec error permanently poisons the attempt until it is dropped.

use std::collections::LinkedList;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, bail};

use crate::ds::Envelope;
use crate::heap_size::HeapSize;

const PREFIX: &str = "circuits-pending-";
static FILE_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingBufferConfig {
    /// Shared retained-payload RAM estimate. Zero means always spill, not unlimited memory.
    pub memory_bytes: u64,
    pub spill_dir: PathBuf,
}

impl Default for PendingBufferConfig {
    fn default() -> Self {
        // SAFETY: reads this process's credentials only.
        let uid = unsafe { libc::getuid() };
        Self {
            memory_bytes: 128 * 1024 * 1024,
            spill_dir: std::env::temp_dir().join(format!("circuits-pending-spill-{uid}")),
        }
    }
}

impl PendingBufferConfig {
    pub fn resolve(get: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let mut cfg = Self::default();
        if let Some(value) = get("CIRCUITS_PENDING_MEMORY_BYTES")
            && !value.trim().is_empty()
        {
            cfg.memory_bytes =
                value.trim().parse().context("CIRCUITS_PENDING_MEMORY_BYTES must be a non-negative byte count")?;
        }
        if let Some(value) = get("CIRCUITS_PENDING_SPILL_DIR")
            && !value.trim().is_empty()
        {
            cfg.spill_dir = PathBuf::from(value.trim());
        }
        Ok(cfg)
    }

    pub fn from_env() -> Result<Self> {
        Self::resolve(|key| std::env::var(key).ok())
    }

    /// Probe even with a zero budget: zero makes disk mandatory.
    pub fn probe(&self) -> Result<()> {
        let mut spill = Spill::create(&self.spill_dir)?;
        spill.file.write_all(b"pending-probe").context("writing pending spill directory probe")?;
        spill.file.flush().context("flushing pending spill directory probe")?;
        std::fs::remove_file(&spill.path).context("removing pending spill directory probe")?;
        drop(spill);
        Ok(())
    }
}

/// Cheap gauges of all queues sharing one budget. Individual atomic loads need not be mutually
/// consistent while other tasks mutate queues. Spill bytes are live physical file bytes, including
/// an already-consumed prefix until the queue is completely drained.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct PendingStats {
    pub memory_bytes: u64,
    pub items: u64,
    pub spill_bytes: u64,
}

pub struct PendingBudget {
    cfg: PendingBufferConfig,
    memory_bytes: AtomicU64,
    items: AtomicU64,
    spill_bytes: AtomicU64,
}

impl PendingBudget {
    pub fn new(cfg: PendingBufferConfig) -> Arc<Self> {
        Arc::new(Self {
            cfg,
            memory_bytes: AtomicU64::new(0),
            items: AtomicU64::new(0),
            spill_bytes: AtomicU64::new(0),
        })
    }

    pub fn stats(&self) -> PendingStats {
        PendingStats {
            memory_bytes: self.memory_bytes.load(Ordering::Relaxed),
            items: self.items.load(Ordering::Relaxed),
            spill_bytes: self.spill_bytes.load(Ordering::Relaxed),
        }
    }

    fn reserve(&self, bytes: u64) -> bool {
        self.memory_bytes
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                used.checked_add(bytes).filter(|total| *total <= self.cfg.memory_bytes)
            })
            .is_ok()
    }
}

/// Lossless streaming codec. `memory_bytes` includes the record's inline representation and owned
/// heap; the queue additionally charges its node overhead. Encoding must be deterministic in length
/// and must not mutate the record. Decoding must consume exactly one framed record.
pub trait PendingRecord: Clone {
    fn memory_bytes(&self) -> u64;
    fn encode(&self, writer: &mut dyn Write) -> Result<()>;
    fn decode(reader: &mut dyn Read) -> Result<Self>;
}

/// A borrowed source such as a weighted row slice. The owned record is created only after its
/// reservation succeeds; spilling streams this view directly, without first allocating a `Vec`.
pub trait PendingRecordView {
    type Record: PendingRecord;
    fn memory_bytes(&self) -> u64;
    fn clone_record(&self) -> Self::Record;
    fn encode(&self, writer: &mut dyn Write) -> Result<()>;
}

impl<T: PendingRecord> PendingRecordView for T {
    type Record = T;
    fn memory_bytes(&self) -> u64 {
        PendingRecord::memory_bytes(self)
    }
    fn clone_record(&self) -> Self {
        self.clone()
    }
    fn encode(&self, writer: &mut dyn Write) -> Result<()> {
        PendingRecord::encode(self, writer)
    }
}

impl PendingRecord for Envelope {
    fn memory_bytes(&self) -> u64 {
        crate::ds::envelope_memory_bytes(self)
    }
    fn encode(&self, writer: &mut dyn Write) -> Result<()> {
        write_string(writer, &self.type_)?;
        write_string(writer, &self.key)?;
        for value in [&self.value, &self.old] {
            writer.write_all(&[u8::from(value.is_some())])?;
            if let Some(value) = value {
                write_json(writer, value)?;
            }
        }
        serde_json::to_writer(writer, &self.headers).context("encoding pending envelope headers")
    }
    fn decode(reader: &mut dyn Read) -> Result<Self> {
        let type_ = read_string(reader)?;
        let key = read_string(reader)?;
        let mut optional_value = || -> Result<Option<serde_json::Value>> {
            match read_tag(reader)? {
                0 => Ok(None),
                1 => Ok(Some(read_json(reader, 0)?)),
                _ => bail!("invalid pending envelope option tag"),
            }
        };
        let value = optional_value()?;
        let old = optional_value()?;
        let headers = serde_json::from_reader(reader).context("decoding pending envelope headers")?;
        Ok(Self { type_, key, value, old, headers })
    }
}

fn write_string(writer: &mut dyn Write, value: &str) -> Result<()> {
    writer.write_all(&(value.len() as u64).to_le_bytes())?;
    writer.write_all(value.as_bytes())?;
    Ok(())
}

fn read_u64(reader: &mut dyn Read) -> Result<u64> {
    let mut bytes = [0u8; 8];
    reader.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_tag(reader: &mut dyn Read) -> Result<u8> {
    let mut tag = [0u8; 1];
    reader.read_exact(&mut tag)?;
    Ok(tag[0])
}

fn read_string(reader: &mut dyn Read) -> Result<String> {
    let length = read_u64(reader)?;
    // Do not preallocate an untrusted frame's claimed string length.
    let mut limited = reader.take(length);
    let mut bytes = Vec::new();
    limited.read_to_end(&mut bytes)?;
    if limited.limit() != 0 {
        bail!("truncated pending string");
    }
    String::from_utf8(bytes).context("invalid pending string UTF-8")
}

/// JSON floating-point numbers travel as their exact IEEE bits, so spilling has no effect on
/// predicates or aggregates. Plain decimal JSON with the default parser is not this guarantee.
fn write_json(writer: &mut dyn Write, value: &serde_json::Value) -> Result<()> {
    use serde_json::Value;
    match value {
        Value::Null => writer.write_all(&[0])?,
        Value::Bool(value) => writer.write_all(&[1, u8::from(*value)])?,
        Value::Number(value) => {
            let (tag, bits) = if value.is_u64() {
                (2, value.as_u64().unwrap())
            } else if value.is_i64() {
                (3, value.as_i64().unwrap() as u64)
            } else {
                (4, value.as_f64().unwrap().to_bits())
            };
            writer.write_all(&[tag])?;
            writer.write_all(&bits.to_le_bytes())?;
        }
        Value::String(value) => {
            writer.write_all(&[5])?;
            write_string(writer, value)?;
        }
        Value::Array(values) => {
            writer.write_all(&[6])?;
            writer.write_all(&(values.len() as u64).to_le_bytes())?;
            for value in values {
                write_json(writer, value)?;
            }
        }
        Value::Object(values) => {
            writer.write_all(&[7])?;
            writer.write_all(&(values.len() as u64).to_le_bytes())?;
            for (key, value) in values {
                write_string(writer, key)?;
                write_json(writer, value)?;
            }
        }
    }
    Ok(())
}

fn read_json(reader: &mut dyn Read, depth: usize) -> Result<serde_json::Value> {
    if depth > 128 {
        bail!("pending JSON exceeds decode depth limit");
    }
    use serde_json::Value;
    Ok(match read_tag(reader)? {
        0 => Value::Null,
        1 => match read_tag(reader)? {
            0 => Value::Bool(false),
            1 => Value::Bool(true),
            _ => bail!("invalid pending boolean"),
        },
        2 => Value::Number(read_u64(reader)?.into()),
        3 => Value::Number((read_u64(reader)? as i64).into()),
        4 => Value::Number(
            serde_json::Number::from_f64(f64::from_bits(read_u64(reader)?)).context("nonfinite pending JSON float")?,
        ),
        5 => Value::String(read_string(reader)?),
        6 => {
            let length = read_u64(reader)?;
            let mut values = Vec::new();
            for _ in 0..length {
                values.push(read_json(reader, depth + 1)?);
            }
            Value::Array(values)
        }
        7 => {
            let length = read_u64(reader)?;
            let mut values = serde_json::Map::new();
            for _ in 0..length {
                let key = read_string(reader)?;
                if values.insert(key, read_json(reader, depth + 1)?).is_some() {
                    bail!("duplicate pending JSON object key");
                }
            }
            Value::Object(values)
        }
        _ => bail!("invalid pending JSON tag"),
    })
}

struct Spill {
    path: PathBuf,
    file: std::fs::File,
    read: u64,
    written: u64,
}

impl Spill {
    fn create(dir: &Path) -> Result<Self> {
        ensure_private_dir(dir)?;
        let namespace = pid_namespace()?;
        loop {
            let id = FILE_ID.fetch_add(1, Ordering::Relaxed);
            let path = dir.join(format!("{PREFIX}{namespace}-{}-{id}.bin", std::process::id()));
            match std::fs::OpenOptions::new().read(true).write(true).create_new(true).mode(0o600).open(&path) {
                Ok(file) => return Ok(Self { path, file, read: 0, written: 0 }),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error).context("creating pending spill file"),
            }
        }
    }

    fn append<T: PendingRecordView>(&mut self, record: &T) -> Result<u64> {
        let mut count = CountWriter(0);
        record.encode(&mut count)?;
        let start = self.written;
        self.file.seek(SeekFrom::Start(start))?;
        self.file.write_all(&count.0.to_le_bytes())?;
        let mut writer = BufWriter::with_capacity(64 * 1024, &mut self.file);
        record.encode(&mut writer)?;
        writer.flush()?;
        drop(writer);
        let end = self.file.stream_position()?;
        let expected = start
            .checked_add(8)
            .and_then(|value| value.checked_add(count.0))
            .context("pending record length overflow")?;
        if end != expected {
            bail!("pending codec wrote a different length on its second pass");
        }
        self.written = end;
        Ok(end - start)
    }

    fn pop<T: PendingRecord>(&mut self) -> Result<T> {
        self.file.seek(SeekFrom::Start(self.read))?;
        let mut header = [0u8; 8];
        self.file.read_exact(&mut header).context("reading pending record frame")?;
        let bytes = u64::from_le_bytes(header);
        let end = self
            .read
            .checked_add(8)
            .and_then(|value| value.checked_add(bytes))
            .context("pending frame length overflow")?;
        if end > self.written {
            bail!("pending record frame extends past the written queue");
        }
        let mut framed = (&mut self.file).take(bytes);
        let mut reader = BufReader::with_capacity(64 * 1024, &mut framed);
        let record = T::decode(&mut reader)?;
        if !reader.buffer().is_empty() {
            bail!("pending codec left trailing bytes in its record");
        }
        drop(reader);
        if framed.limit() != 0 {
            bail!("pending codec did not consume its complete record");
        }
        self.read = end;
        Ok(record)
    }
}

impl Drop for Spill {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_file(&self.path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(path = %self.path.display(), %error, "could not remove pending scratch file");
        }
    }
}

struct CountWriter(u64);
impl Write for CountWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| std::io::Error::other("pending encoding length overflow"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub struct SpillQueue<T: PendingRecord> {
    budget: Arc<PendingBudget>,
    mem: LinkedList<(T, u64)>,
    memory_bytes: u64,
    spill: Option<Spill>,
    spill_bytes: u64,
    count: u64,
    failure: Option<String>,
}

impl<T: PendingRecord> SpillQueue<T> {
    pub fn new(budget: Arc<PendingBudget>) -> Self {
        Self { budget, mem: LinkedList::new(), memory_bytes: 0, spill: None, spill_bytes: 0, count: 0, failure: None }
    }
    pub fn len(&self) -> u64 {
        self.count
    }
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }
    pub fn memory_bytes(&self) -> u64 {
        self.memory_bytes
    }
    pub fn spill_bytes(&self) -> u64 {
        self.spill_bytes
    }
    pub fn spill_path(&self) -> Option<&Path> {
        self.spill.as_ref().map(|spill| spill.path.as_path())
    }

    fn healthy(&self) -> Result<()> {
        if let Some(failure) = &self.failure {
            bail!("pending queue failed: {failure}");
        }
        Ok(())
    }
    pub fn check(&self) -> Result<()> {
        self.healthy()
    }
    pub fn is_failed(&self) -> bool {
        self.failure.is_some()
    }
    fn poison<R>(&mut self, result: Result<R>) -> Result<R> {
        if let Err(error) = &result {
            self.failure = Some(format!("{error:#}"));
        }
        result
    }

    pub fn push(&mut self, record: &T) -> Result<()> {
        self.push_borrowed(record)
    }
    pub fn push_borrowed<Q: PendingRecordView<Record = T>>(&mut self, record: &Q) -> Result<()> {
        self.healthy()?;
        let result = self.push_inner(record);
        self.poison(result)
    }
    fn push_inner<Q: PendingRecordView<Record = T>>(&mut self, record: &Q) -> Result<()> {
        let inline = std::mem::size_of::<T>();
        let node = (std::mem::size_of::<(T, u64)>() + 2 * std::mem::size_of::<usize>())
            .next_multiple_of(std::mem::align_of::<(T, u64)>());
        let bytes = record
            .memory_bytes()
            .max(inline as u64)
            .checked_add((node - inline) as u64)
            .context("pending RAM estimate overflow")?;
        if self.spill.is_none() && self.budget.reserve(bytes) {
            self.mem.push_back((record.clone_record(), bytes));
            self.memory_bytes += bytes;
        } else {
            if self.spill.is_none() {
                self.spill = Some(Spill::create(&self.budget.cfg.spill_dir)?);
                // Keep reservations and records until the entire migration succeeded. A failed
                // write poisons the queue and leaves all ownership with this attempt until drop.
                for (held, _) in &self.mem {
                    append_tracked(self.spill.as_mut().unwrap(), &mut self.spill_bytes, &self.budget, held)?;
                }
                self.mem.clear();
                self.budget.memory_bytes.fetch_sub(self.memory_bytes, Ordering::Relaxed);
                self.memory_bytes = 0;
            }
            append_tracked(self.spill.as_mut().unwrap(), &mut self.spill_bytes, &self.budget, record)?;
        }
        self.count += 1;
        self.budget.items.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn try_pop(&mut self) -> Result<Option<T>> {
        self.healthy()?;
        if self.count == 0 {
            return Ok(None);
        }
        let result = self.pop_inner();
        self.poison(result).map(Some)
    }
    fn pop_inner(&mut self) -> Result<T> {
        let record = if let Some(spill) = &mut self.spill {
            spill.pop()?
        } else {
            let (record, bytes) = self.mem.pop_front().expect("nonempty RAM queue");
            self.memory_bytes -= bytes;
            self.budget.memory_bytes.fetch_sub(bytes, Ordering::Relaxed);
            record
        };
        // File deletion is part of successful completion. Refuse the final pop if cleanup fails;
        // callers must discard the failed pending attempt rather than continue with partial state.
        if self.count == 1
            && let Some(spill) = &self.spill
        {
            std::fs::remove_file(&spill.path).context("removing drained pending spill file")?;
            self.spill = None;
            self.budget.spill_bytes.fetch_sub(self.spill_bytes, Ordering::Relaxed);
            self.spill_bytes = 0;
        }
        self.count -= 1;
        self.budget.items.fetch_sub(1, Ordering::Relaxed);
        Ok(record)
    }
}

fn append_tracked<T: PendingRecordView>(
    spill: &mut Spill,
    bytes: &mut u64,
    budget: &PendingBudget,
    record: &T,
) -> Result<()> {
    let result = spill.append(record);
    // A failed write may leave a partial frame. Count that scratch too until poisoned drop.
    let total = match &result {
        Ok(_) => spill.written,
        Err(_) => spill.file.metadata().map(|metadata| metadata.len()).unwrap_or(*bytes),
    };
    if total > *bytes {
        budget.spill_bytes.fetch_add(total - *bytes, Ordering::Relaxed);
        *bytes = total;
    }
    result.map(|_| ())
}

impl<T: PendingRecord> HeapSize for SpillQueue<T> {
    fn heap_bytes(&self) -> usize {
        self.memory_bytes.min(usize::MAX as u64) as usize
    }
}

impl<T: PendingRecord> Drop for SpillQueue<T> {
    fn drop(&mut self) {
        self.budget.memory_bytes.fetch_sub(self.memory_bytes, Ordering::Relaxed);
        self.budget.items.fetch_sub(self.count, Ordering::Relaxed);
        self.budget.spill_bytes.fetch_sub(self.spill_bytes, Ordering::Relaxed);
    }
}

fn ensure_private_dir(dir: &Path) -> Result<()> {
    std::fs::DirBuilder::new().recursive(true).mode(0o700).create(dir).context("creating pending spill directory")?;
    let metadata = std::fs::symlink_metadata(dir).context("checking pending spill directory")?;
    // SAFETY: reads this process's credentials only.
    let uid = unsafe { libc::getuid() };
    if !metadata.is_dir() || metadata.uid() != uid || metadata.permissions().mode() & 0o077 != 0 {
        bail!("pending spill directory {} must be a private directory owned by this user (0700)", dir.display());
    }
    Ok(())
}

fn pid_namespace() -> Result<u64> {
    Ok(std::fs::metadata("/proc/self/ns/pid").context("reading pending spill PID namespace")?.ino())
}

/// Remove only files naming a dead process in this PID namespace. Live/recycled PIDs, another
/// namespace, malformed names and unrelated files survive. Shared-directory users in different
/// namespaces cannot accidentally sweep each other's live queues.
pub fn sweep_spill_dir(dir: &Path) -> Result<usize> {
    ensure_private_dir(dir)?;
    let namespace = pid_namespace()?;
    let mut removed = 0;
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(parts) = name.strip_prefix(PREFIX).and_then(|name| name.strip_suffix(".bin")) else { continue };
        let mut parts = parts.split('-');
        let (Some(ns), Some(pid), Some(id), None) = (parts.next(), parts.next(), parts.next(), parts.next()) else {
            continue;
        };
        let (Ok(ns), Ok(pid), Ok(_)) = (ns.parse::<u64>(), pid.parse::<i32>(), id.parse::<u64>()) else { continue };
        if ns != namespace || pid <= 0 {
            continue;
        }
        // SAFETY: signal zero checks existence and permissions, without delivering a signal.
        let dead =
            unsafe { libc::kill(pid, 0) } != 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH);
        if dead && entry.file_type()?.is_file() {
            std::fs::remove_file(entry.path()).context("removing dead process pending spill file")?;
            removed += 1;
        }
    }
    Ok(removed)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::ds::EnvelopeHeaders;

    pub(crate) struct Scratch(PathBuf);
    impl Scratch {
        pub(crate) fn new() -> Self {
            let dir = std::env::temp_dir().join(format!(
                "circuits-pending-test-{}-{}",
                std::process::id(),
                FILE_ID.fetch_add(1, Ordering::Relaxed)
            ));
            ensure_private_dir(&dir).unwrap();
            Self(dir)
        }
        pub(crate) fn path(&self) -> &Path {
            &self.0
        }
        pub(crate) fn budget(&self, bytes: u64) -> Arc<PendingBudget> {
            PendingBudget::new(PendingBufferConfig { memory_bytes: bytes, spill_dir: self.0.clone() })
        }
    }
    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn envelope(key: &str, size: usize) -> Envelope {
        Envelope {
            type_: "public.items".into(),
            key: key.into(),
            value: Some(serde_json::json!({ "blob": "x".repeat(size) })),
            old: None,
            headers: EnvelopeHeaders {
                operation: "insert".into(),
                txid: Some("4294967295".into()),
                offset: Some("opaque-offset".into()),
                lsn: Some("FFFF/FFFF".into()),
                seq: Some(u64::MAX),
                last: Some(true),
                schema: Some("fingerprint".into()),
            },
        }
    }

    fn charged(env: &Envelope) -> u64 {
        PendingRecord::memory_bytes(env) + (std::mem::size_of::<u64>() + 2 * std::mem::size_of::<usize>()) as u64
    }

    #[test]
    fn pending_buffer_shared_budget_and_fifo_transition() {
        let scratch = Scratch::new();
        let one = envelope("one", 20);
        let budget = scratch.budget(charged(&one));
        let mut a = SpillQueue::new(budget.clone());
        let mut b = SpillQueue::new(budget.clone());
        a.push(&one).unwrap();
        b.push(&envelope("two", 20)).unwrap();
        assert_eq!(budget.stats().memory_bytes, charged(&one));
        assert!(b.spill_path().is_some());
        a.push(&envelope("three", 20)).unwrap();
        assert_eq!(budget.stats().memory_bytes, 0, "migration releases its RAM only after both writes succeed");
        assert_eq!(budget.stats().items, 3);
        assert_eq!(a.try_pop().unwrap().unwrap().key, "one");
        assert_eq!(a.try_pop().unwrap().unwrap().key, "three");
        assert!(a.spill_path().is_none());
        assert_eq!(b.try_pop().unwrap().unwrap().key, "two");
        assert_eq!(budget.stats(), PendingStats::default());
        a.push(&one).unwrap();
        assert!(a.spill_path().is_none(), "drained queues return to RAM");
        drop(a);
        assert_eq!(budget.stats(), PendingStats::default());
    }

    #[test]
    fn pending_buffer_oversized_borrowed_view_never_clones() {
        struct View<'a>(&'a Envelope);
        impl PendingRecordView for View<'_> {
            type Record = Envelope;
            fn memory_bytes(&self) -> u64 {
                PendingRecord::memory_bytes(self.0)
            }
            fn clone_record(&self) -> Envelope {
                panic!("oversized spill must not clone its borrowed source")
            }
            fn encode(&self, writer: &mut dyn Write) -> Result<()> {
                PendingRecord::encode(self.0, writer)
            }
        }
        let scratch = Scratch::new();
        let budget = scratch.budget(1024);
        let mut queue = SpillQueue::new(budget.clone());
        let large = envelope("large", 1024 * 1024);
        queue.push_borrowed(&View(&large)).unwrap();
        assert_eq!(queue.heap_bytes(), 0);
        assert_eq!(budget.stats().memory_bytes, 0);
        assert!(budget.stats().spill_bytes > 1024 * 1024);
        assert_eq!(queue.try_pop().unwrap().unwrap().value, large.value);
        assert_eq!(budget.stats(), PendingStats::default());
    }

    #[test]
    fn pending_buffer_envelope_codec_preserves_all_fields_and_float_bits() {
        let scratch = Scratch::new();
        let mut queue = SpillQueue::new(scratch.budget(0));
        let mut source = envelope("\n\0unicode-λ", 3);
        let floats =
            [0x8000000000000000, 0x0000000000000001, 0x3fd5555555555555, 0x7fefffffffffffff, 0x3ff0000000000001];
        source.value = Some(serde_json::json!({
            "numbers": floats.map(f64::from_bits), "signed": i64::MIN, "unsigned": u64::MAX,
            "nested": [null, true, false, { "λ": "\0\n" }],
        }));
        source.old = Some(serde_json::json!({ "before": "old" }));
        queue.push(&source).unwrap();
        let decoded = queue.try_pop().unwrap().unwrap();
        assert_eq!(serde_json::to_value(&decoded).unwrap(), serde_json::to_value(&source).unwrap());
        for (index, bits) in floats.into_iter().enumerate() {
            assert_eq!(decoded.value.as_ref().unwrap()["numbers"][index].as_f64().unwrap().to_bits(), bits);
        }
    }

    #[test]
    fn pending_buffer_truncation_permanently_latches_and_drop_cleans() {
        let scratch = Scratch::new();
        let budget = scratch.budget(0);
        let mut queue = SpillQueue::new(budget.clone());
        queue.push(&envelope("one", 100)).unwrap();
        queue.push(&envelope("two", 100)).unwrap();
        let path = queue.spill_path().unwrap().to_path_buf();
        std::fs::OpenOptions::new().write(true).open(&path).unwrap().set_len(10).unwrap();
        assert!(queue.try_pop().is_err());
        assert!(queue.is_failed());
        assert!(queue.check().is_err());
        assert!(queue.push(&envelope("later", 1)).is_err());
        assert!(queue.try_pop().is_err());
        assert_eq!(queue.len(), 2, "failed reads advance no logical cursor");
        drop(queue);
        assert!(!path.exists());
        assert_eq!(budget.stats(), PendingStats::default());
    }

    #[derive(Clone)]
    struct FaultRecord(bool);
    impl PendingRecord for FaultRecord {
        fn memory_bytes(&self) -> u64 {
            1
        }
        fn encode(&self, writer: &mut dyn Write) -> Result<()> {
            if self.0 {
                bail!("injected encode failure");
            }
            writer.write_all(b"valid")?;
            Ok(())
        }
        fn decode(reader: &mut dyn Read) -> Result<Self> {
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes)?;
            if bytes != b"valid" {
                bail!("invalid fault record");
            }
            Ok(Self(false))
        }
    }

    #[test]
    fn pending_buffer_failed_migration_keeps_reservations_until_drop() {
        let scratch = Scratch::new();
        // `(FaultRecord, u64)` has padding: one linked node occupies 32 bytes.
        let budget = scratch.budget(32);
        let mut queue = SpillQueue::new(budget.clone());
        queue.push(&FaultRecord(true)).unwrap();
        assert_eq!(budget.stats().memory_bytes, 32);
        assert!(queue.push(&FaultRecord(false)).is_err());
        assert_eq!(queue.len(), 1);
        assert_eq!(budget.stats().items, 1);
        assert_eq!(budget.stats().memory_bytes, 32);
        let path = queue.spill_path().unwrap().to_path_buf();
        assert!(queue.check().is_err());
        drop(queue);
        assert!(!path.exists());
        assert_eq!(budget.stats(), PendingStats::default());
    }

    #[test]
    fn pending_buffer_partial_encode_failure_counts_scratch_until_drop() {
        struct FailSecond(AtomicU64);
        impl PendingRecordView for FailSecond {
            type Record = FaultRecord;
            fn memory_bytes(&self) -> u64 {
                1
            }
            fn clone_record(&self) -> FaultRecord {
                panic!("always-spill view must not clone")
            }
            fn encode(&self, writer: &mut dyn Write) -> Result<()> {
                if self.0.fetch_add(1, Ordering::Relaxed) == 0 {
                    writer.write_all(b"valid")?;
                    Ok(())
                } else {
                    writer.write_all(b"partial")?;
                    bail!("injected second-pass failure")
                }
            }
        }
        let scratch = Scratch::new();
        let budget = scratch.budget(0);
        let mut queue = SpillQueue::new(budget.clone());
        assert!(queue.push_borrowed(&FailSecond(AtomicU64::new(0))).is_err());
        let path = queue.spill_path().unwrap().to_path_buf();
        let bytes = std::fs::metadata(&path).unwrap().len();
        assert!(bytes > 8, "failed writer retains its partial frame for owned cleanup");
        assert_eq!(budget.stats().spill_bytes, bytes);
        assert_eq!(budget.stats().items, 0);
        assert!(queue.check().is_err());
        drop(queue);
        assert!(!path.exists());
        assert_eq!(budget.stats(), PendingStats::default());
    }

    #[test]
    fn pending_buffer_unusable_spill_dir_fails_even_first_push() {
        let scratch = Scratch::new();
        let blocker = scratch.0.join("file");
        std::fs::write(&blocker, b"x").unwrap();
        let cfg = PendingBufferConfig { memory_bytes: 0, spill_dir: blocker.join("nested") };
        assert!(cfg.probe().is_err());
        let budget = PendingBudget::new(cfg);
        let mut queue = SpillQueue::new(budget.clone());
        assert!(queue.push(&envelope("first", 0)).is_err());
        assert!(queue.is_empty());
        assert!(queue.check().is_err(), "empty does not mean healthy after a failed first write");
        drop(queue);
        assert_eq!(budget.stats(), PendingStats::default());
    }

    #[test]
    fn pending_buffer_repeated_cycles_release_files_and_gauges() {
        let scratch = Scratch::new();
        let budget = scratch.budget(600);
        let mut queue = SpillQueue::new(budget.clone());
        for cycle in 0..20 {
            for record in 0..10 {
                queue.push(&envelope(&format!("{cycle}-{record}"), 100)).unwrap();
            }
            for record in 0..10 {
                assert_eq!(queue.try_pop().unwrap().unwrap().key, format!("{cycle}-{record}"));
            }
            assert!(queue.try_pop().unwrap().is_none());
            assert_eq!(budget.stats(), PendingStats::default());
            assert_eq!(std::fs::read_dir(&scratch.0).unwrap().count(), 0);
        }
    }

    #[test]
    fn pending_buffer_drop_and_task_cancellation_release_ownership() {
        let scratch = Scratch::new();
        let budget = scratch.budget(0);
        let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        runtime.block_on(async {
            let mut queue = SpillQueue::new(budget.clone());
            queue.push(&envelope("cancel", 100)).unwrap();
            let path = queue.spill_path().unwrap().to_path_buf();
            let task = tokio::spawn(async move {
                let _owned_queue = queue;
                std::future::pending::<()>().await;
            });
            tokio::task::yield_now().await;
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            assert!(!path.exists());
            assert_eq!(budget.stats(), PendingStats::default());
        });
    }

    #[test]
    fn pending_buffer_sweep_respects_live_processes_and_namespaces() {
        let scratch = Scratch::new();
        let ns = pid_namespace().unwrap();
        let dead = scratch.0.join(format!("{PREFIX}{ns}-999999999-0.bin"));
        let alive = scratch.0.join(format!("{PREFIX}{ns}-{}-0.bin", std::process::id()));
        let foreign = scratch.0.join(format!("{PREFIX}{}-999999999-0.bin", ns + 1));
        let unrelated = scratch.0.join("unrelated.bin");
        for path in [&dead, &alive, &foreign, &unrelated] {
            std::fs::write(path, b"data").unwrap();
        }
        assert_eq!(sweep_spill_dir(&scratch.0).unwrap(), 1);
        assert!(!dead.exists());
        for path in [&alive, &foreign, &unrelated] {
            assert!(path.exists());
        }
        let mut live_queue = SpillQueue::new(scratch.budget(0));
        live_queue.push(&envelope("live", 1)).unwrap();
        assert_eq!(sweep_spill_dir(&scratch.0).unwrap(), 0);
        assert!(live_queue.spill_path().unwrap().exists());
    }

    #[test]
    fn pending_buffer_private_permissions_config_and_probe() {
        let scratch = Scratch::new();
        let cfg = PendingBufferConfig::resolve(|key| match key {
            "CIRCUITS_PENDING_MEMORY_BYTES" => Some("0".into()),
            "CIRCUITS_PENDING_SPILL_DIR" => Some(scratch.0.to_str().unwrap().into()),
            _ => None,
        })
        .unwrap();
        assert_eq!(cfg.memory_bytes, 0);
        cfg.probe().unwrap();
        assert_eq!(std::fs::read_dir(&scratch.0).unwrap().count(), 0);
        for bad in ["-1", "18446744073709551616", "128MiB"] {
            assert!(
                PendingBufferConfig::resolve(|key| (key == "CIRCUITS_PENDING_MEMORY_BYTES").then(|| bad.into()))
                    .is_err()
            );
        }
        assert_eq!(PendingBufferConfig::resolve(|_| None).unwrap().memory_bytes, 128 * 1024 * 1024);
        let mut queue = SpillQueue::new(PendingBudget::new(cfg));
        queue.push(&envelope("private", 0)).unwrap();
        assert_eq!(std::fs::metadata(&scratch.0).unwrap().mode() & 0o777, 0o700);
        assert_eq!(std::fs::metadata(queue.spill_path().unwrap()).unwrap().mode() & 0o777, 0o600);
        let public = scratch.0.join("public");
        std::fs::create_dir(&public).unwrap();
        std::fs::set_permissions(&public, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(PendingBufferConfig { memory_bytes: 1, spill_dir: public }.probe().is_err());
    }
}
