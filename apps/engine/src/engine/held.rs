//! Transaction input retained between change-log pages. Spill records reuse the pending queue's
//! lossless codec and shared byte budget, so a large transaction does not grow a second RAM copy.

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{Context, Result};

use crate::ds::{Envelope, envelope_memory_bytes};
use crate::pending_buffer::{PendingBudget, SpillQueue};

const INPUT_CHUNK_BYTES: u64 = 1024 * 1024;

pub(super) struct Transaction {
    pub key: (Option<String>, Option<String>),
    pub last_seq: Option<u64>,
    pub complete: bool,
    pub count: usize,
    tables: HashSet<String>,
    source: Option<Source>,
}

enum Source {
    Page(Vec<Envelope>),
    Buffered(SpillQueue<Envelope>),
}

impl Transaction {
    pub fn page(envelopes: Vec<Envelope>) -> Self {
        let first = envelopes.first().expect("transaction page is nonempty");
        let last = envelopes.last().unwrap();
        Self {
            key: (first.headers.txid.clone(), first.headers.lsn.clone()),
            last_seq: last.headers.seq,
            complete: last.headers.last == Some(true),
            count: envelopes.len(),
            tables: HashSet::new(),
            source: Some(Source::Page(envelopes)),
        }
    }

    pub async fn held(envelopes: Vec<Envelope>, budget: Arc<PendingBudget>) -> Result<Self> {
        let mut transaction = Self::page(envelopes);
        let Some(Source::Page(envelopes)) = transaction.source.take() else { unreachable!() };
        transaction.tables.extend(envelopes.iter().map(|env| env.type_.clone()));
        let queue = tokio::task::spawn_blocking(move || {
            let mut queue = SpillQueue::new(budget);
            for env in envelopes {
                queue.push(&env)?;
            }
            Ok::<_, anyhow::Error>(queue)
        })
        .await
        .context("joining held transaction buffering")??;
        transaction.source = Some(Source::Buffered(queue));
        Ok(transaction)
    }

    pub async fn append(mut self, envelopes: Vec<Envelope>) -> Result<Self> {
        let Some(last) = envelopes.last() else { return Ok(self) };
        self.last_seq = last.headers.seq;
        self.complete = last.headers.last == Some(true);
        self.count += envelopes.len();
        self.tables.extend(envelopes.iter().map(|env| env.type_.clone()));
        let Some(Source::Buffered(mut queue)) = self.source.take() else { unreachable!() };
        // The blocking owner retains scratch and reservations if this waiter is cancelled.
        let queue = tokio::task::spawn_blocking(move || {
            for env in envelopes {
                queue.push(&env)?;
            }
            Ok::<_, anyhow::Error>(queue)
        })
        .await
        .context("joining held transaction append")??;
        self.source = Some(Source::Buffered(queue));
        Ok(self)
    }

    pub fn table_names(&self) -> impl Iterator<Item = &str> {
        let page = match &self.source {
            Some(Source::Page(envelopes)) => envelopes.as_slice(),
            _ => &[],
        };
        page.iter().map(|env| env.type_.as_str()).chain(self.tables.iter().map(String::as_str))
    }

    /// Validate all scratch before any envelope changes executor/registry state. The processing
    /// pass still checks each read, since a later storage failure must stop publication too.
    pub async fn validate(&mut self) -> Result<()> {
        let mut queue = match self.source.take().expect("transaction source is owned") {
            Source::Page(page) => {
                self.source = Some(Source::Page(page));
                return Ok(());
            }
            Source::Buffered(queue) => queue,
        };
        let (queue, result) = tokio::task::spawn_blocking(move || {
            let result = queue.validate();
            (queue, result)
        })
        .await
        .context("joining held transaction validation")?;
        self.source = Some(Source::Buffered(queue));
        result
    }

    /// Input chunks never flush transaction output or advance its durable frontier.
    pub async fn next_chunk(&mut self) -> Result<Option<Vec<Envelope>>> {
        let source = self.source.take().expect("transaction source is owned");
        let (source, result) = match source {
            Source::Page(mut page) => {
                // A normal read page is already bounded. Move it directly to the caller without
                // re-estimating each envelope or repacking it through the spill chunk path.
                let chunk = Ok((!page.is_empty()).then(|| std::mem::take(&mut page)));
                (Source::Page(page), chunk)
            }
            Source::Buffered(mut queue) => tokio::task::spawn_blocking(move || {
                let chunk = take_chunk(|| queue.try_pop());
                (Source::Buffered(queue), chunk)
            })
            .await
            .context("joining held transaction read")?,
        };
        self.source = Some(source);
        result
    }
}

fn take_chunk(mut next: impl FnMut() -> Result<Option<Envelope>>) -> Result<Option<Vec<Envelope>>> {
    let mut chunk = Vec::new();
    let mut bytes = 0;
    while let Some(envelope) = next()? {
        bytes += envelope_memory_bytes(&envelope);
        chunk.push(envelope);
        if bytes >= INPUT_CHUNK_BYTES {
            break;
        }
    }
    Ok((!chunk.is_empty()).then_some(chunk))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ds::EnvelopeHeaders;
    use crate::pending_buffer::PendingBufferConfig;

    fn envelope(seq: u64, last: bool) -> Envelope {
        Envelope {
            type_: "public.items".into(),
            key: seq.to_string(),
            value: Some(serde_json::json!({"id": seq, "body": "x".repeat(INPUT_CHUNK_BYTES as usize)})),
            old: Some(serde_json::json!({"precise": f64::from_bits(0x3ff0000000000001)})),
            headers: EnvelopeHeaders {
                operation: "update".into(),
                txid: Some("42".into()),
                lsn: Some("0/20".into()),
                seq: Some(seq),
                last: last.then_some(true),
                offset: Some(format!("original-{seq}")),
                schema: Some("deadbeef".into()),
            },
        }
    }

    #[tokio::test]
    async fn spilled_input_preserves_stamps_and_float_bits_and_drains_one_oversized_row() {
        let scratch = crate::pending_buffer::tests::Scratch::new();
        let path = scratch.path();
        let budget = scratch.budget(0);
        let expected = vec![envelope(0, false), envelope(1, true)];
        let mut transaction = Transaction::held(vec![expected[0].clone()], budget.clone()).await.unwrap();
        assert_eq!(budget.stats().memory_bytes, 0);
        assert!(budget.stats().spill_bytes > 0);
        transaction = transaction.append(vec![expected[1].clone()]).await.unwrap();
        assert!(transaction.complete);
        transaction.validate().await.unwrap();
        for envelope in expected {
            let chunk = transaction.next_chunk().await.unwrap().unwrap();
            assert_eq!(chunk.len(), 1);
            assert_eq!(serde_json::to_value(chunk).unwrap(), serde_json::json!([envelope]));
        }
        assert!(transaction.next_chunk().await.unwrap().is_none());
        drop(transaction);
        assert_eq!(budget.stats().items, 0);
        assert_eq!(budget.stats().spill_bytes, 0);
        assert_eq!(std::fs::read_dir(path).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn dropping_held_input_releases_its_file_and_shared_reservations() {
        let scratch = crate::pending_buffer::tests::Scratch::new();
        let path = scratch.path();
        let budget = scratch.budget(0);
        let transaction = Transaction::held(vec![envelope(0, false)], budget.clone()).await.unwrap();
        assert_eq!(std::fs::read_dir(path).unwrap().count(), 1);
        drop(transaction);
        assert_eq!(std::fs::read_dir(path).unwrap().count(), 0);
        assert_eq!(budget.stats().items, 0);
        assert_eq!(budget.stats().memory_bytes, 0);
        assert_eq!(budget.stats().spill_bytes, 0);
    }

    #[tokio::test]
    async fn validation_refuses_a_truncated_spill_before_its_first_record_is_consumed() {
        let scratch = crate::pending_buffer::tests::Scratch::new();
        let budget = scratch.budget(0);
        let mut transaction =
            Transaction::held(vec![envelope(0, false), envelope(1, true)], budget.clone()).await.unwrap();
        let Some(Source::Buffered(queue)) = transaction.source.as_ref() else { unreachable!() };
        let file = queue.spill_path().unwrap();
        let length = std::fs::metadata(file).unwrap().len();
        std::fs::OpenOptions::new().write(true).open(file).unwrap().set_len(length - 1).unwrap();
        assert!(transaction.validate().await.is_err());
        assert_eq!(budget.stats().items, 2, "validation consumes no prefix");
        assert!(transaction.next_chunk().await.is_err(), "a failed scratch queue stays poisoned");
        drop(transaction);
        assert_eq!(budget.stats().items, 0);
        assert_eq!(budget.stats().spill_bytes, 0);
    }

    #[tokio::test]
    async fn failed_spill_creation_releases_its_retained_input() {
        let scratch = crate::pending_buffer::tests::Scratch::new();
        let path = scratch.path().join("file");
        std::fs::write(&path, b"not a directory").unwrap();
        let budget = PendingBudget::new(PendingBufferConfig { memory_bytes: 0, spill_dir: path.clone() });
        assert!(Transaction::held(vec![envelope(0, false)], budget.clone()).await.is_err());
        assert_eq!(budget.stats().items, 0);
        assert_eq!(budget.stats().memory_bytes, 0);
        assert_eq!(budget.stats().spill_bytes, 0);
        std::fs::remove_file(path).unwrap();
    }
}
