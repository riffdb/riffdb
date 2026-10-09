//! Disposable payload retention, separate from complete exact index metadata.
use super::*;
use riffdb_storage_api::CommandSegmentManifestV1;
use riffdb_types::DatabaseId;
use std::collections::VecDeque;
use std::sync::Mutex;

const MAX_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;
const MAX_PAYLOAD_ENTRIES: usize = 64;
/// One seeded segment in this many is decoded from its canonical bytes and
/// compared with the writer's segment before it is retained.
pub(crate) const PAYLOAD_SEED_VERIFY_EVERY: u64 = 64;

#[cfg(test)]
thread_local! {
    pub(crate) static PAYLOAD_COLD_DECODES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}
// Normalize through the canonical decoder before retention. Charge non-value
// DTOs, shared scalar/key/string views and allocation headers conservatively at
// 64 times their wire footprint, plus the complete fixed command layouts.
// Canonical trees need a separate layout walk: a one-byte Null can occupy a
// whole enum slot, so wire length alone is not a decoded-memory budget.
// Fourfold vector capacity and a per-allocation header cover the normalized
// decoder's growth, including its minimum capacity for small collections.
const WIRE_OWNERSHIP_MULTIPLIER: usize = 64;

fn allocation_charge<T>(count: usize) -> Option<usize> {
    count
        .checked_mul(std::mem::size_of::<T>())?
        .checked_mul(4)?
        .checked_add(64)
}

fn record_charge(record: &riffdb_types::CanonicalRecord) -> Option<usize> {
    record.fields().iter().try_fold(
        allocation_charge::<(riffdb_types::FieldId, riffdb_types::CanonicalValue)>(record.len())?,
        |bytes, (_, value)| bytes.checked_add(value_charge(value)?),
    )
}

fn value_charge(value: &riffdb_types::CanonicalValue) -> Option<usize> {
    use riffdb_types::CanonicalValue as V;
    match value {
        V::String(value) => allocation_charge::<u8>(value.as_str().len()),
        V::Bytes(value) => allocation_charge::<u8>(value.as_bytes().len()),
        V::Vector(value) => allocation_charge::<f32>(value.components().len()),
        V::Record(value) => record_charge(value),
        V::List(value) => value.values().iter().try_fold(
            allocation_charge::<V>(value.values().len())?,
            |bytes, value| bytes.checked_add(value_charge(value)?),
        ),
        V::Null
        | V::Bool(_)
        | V::I64(_)
        | V::U64(_)
        | V::Decimal(_)
        | V::Money(_)
        | V::Timestamp(_)
        | V::Date(_)
        | V::Uuid(_)
        | V::Enum { .. } => Some(0),
    }
}

fn decoded_charge(segment: &StoredCommandSegmentV1, wire_bytes: usize) -> Option<usize> {
    let initial = wire_bytes
        .checked_mul(WIRE_OWNERSHIP_MULTIPLIER)?
        .checked_add(std::mem::size_of::<CachedPayload>())?
        .checked_add(allocation_charge::<StoredCommandCapsuleV2>(
            segment.commands().len(),
        )?)?;
    segment
        .commands()
        .iter()
        .try_fold(initial, |bytes, command| {
            let base = command.base();
            let bytes = bytes
                .checked_add(record_charge(base.outcome().declared_outcome().value())?)?
                .checked_add(record_charge(base.commit().declared_outcome().value())?)?
                .checked_add(record_charge(base.outcome().service_values())?)?;
            // Prefix mutations retain opaque encoded images, not decoded records.
            // Outcome/service values and event payloads are the segment's complete
            // retained canonical trees. Charge both outcome DTOs even if shared.
            command.events().iter().try_fold(bytes, |bytes, event| {
                bytes.checked_add(record_charge(event.payload())?)
            })
        })
}

pub(super) struct SegmentMetadata {
    pub(super) first: CommitSequence,
    pub(super) last: CommitSequence,
    pub(super) digest: CommandSegmentDigestV1,
    database: DatabaseId,
    history: u64,
    pub(super) manifest: CommandSegmentManifestV1,
    pub(super) events: BTreeMap<EventId, (PartitionKeyHash, StoredEventRouteV1)>,
}

impl SegmentMetadata {
    pub(super) fn new(segment: &StoredCommandSegmentV1) -> Result<Self, StorageError> {
        let mut events = BTreeMap::new();
        for command in segment.commands() {
            for event in command.events() {
                if events
                    .insert(
                        event.event_id(),
                        (
                            command.base().commit().partition_hash(),
                            StoredEventRouteV1::new(
                                event.event_id(),
                                event.event_type_id(),
                                event.event_hash(),
                            ),
                        ),
                    )
                    .is_some()
                {
                    return Err(corrupt());
                }
            }
        }
        Ok(Self {
            first: segment.first_commit_sequence(),
            last: segment.last_commit_sequence(),
            digest: segment.segment_digest(),
            database: segment.database_id(),
            history: segment.history_incarnation(),
            manifest: segment.manifest().clone(),
            events,
        })
    }

    fn validate(&self, segment: &StoredCommandSegmentV1) -> Result<(), StorageError> {
        if segment.first_commit_sequence() != self.first
            || segment.last_commit_sequence() != self.last
            || segment.segment_digest() != self.digest
            || segment.database_id() != self.database
            || segment.history_incarnation() != self.history
            || segment.manifest() != &self.manifest
        {
            return Err(corrupt());
        }
        Ok(())
    }
}

/// Whether stored `bytes` still name a cached segment: its exact canonical
/// bytes, or a history stub carrying its first sequence and digest.
fn cached_payload_matches(
    canonical: &[u8],
    digest: &[u8; 32],
    first: CommitSequence,
    bytes: &[u8],
    stub: Option<&crate::history_stub::SegmentSummary>,
) -> bool {
    match stub {
        None => canonical == bytes,
        Some(summary) => {
            summary.first_commit_sequence == Some(first) && summary.segment_digest == *digest
        }
    }
}

#[derive(Clone)]
struct CachedPayload {
    canonical: Arc<[u8]>,
    decoded: Arc<StoredCommandSegmentV1>,
    charge: usize,
}

#[derive(Default)]
struct PayloadEntries {
    entries: BTreeMap<CommitSequence, CachedPayload>,
    recency: VecDeque<CommitSequence>,
    bytes: usize,
}

impl PayloadEntries {
    fn remove(&mut self, first: CommitSequence) -> Result<(), StorageError> {
        let entry = self.entries.remove(&first);
        let position = self.recency.iter().position(|value| *value == first);
        match (entry, position) {
            (Some(entry), Some(position)) => {
                self.recency.remove(position).ok_or_else(corrupt)?;
                self.bytes = self.bytes.checked_sub(entry.charge).ok_or_else(corrupt)?;
                Ok(())
            }
            (None, None) => Ok(()),
            _ => Err(corrupt()),
        }
    }
}

#[derive(Default)]
pub(super) struct PayloadCache {
    state: Mutex<PayloadEntries>,
}

/// Exactly one selected metadata entry and at most one disposable cache entry.
/// The caller retains its independent immutable storage view as the authority.
pub(crate) struct CapturedCommandPayload {
    metadata: Arc<SegmentMetadata>,
    cached: Option<CachedPayload>,
}

pub(crate) struct ResolvedCommandPayload {
    metadata: Arc<SegmentMetadata>,
    pub(crate) segment: Arc<StoredCommandSegmentV1>,
    retained: Option<CachedPayload>,
}

impl CapturedCommandPayload {
    pub(crate) fn last_sequence(&self) -> CommitSequence {
        self.metadata.last
    }

    pub(crate) fn resolve(
        self,
        load: impl FnOnce(CommitSequence) -> Result<Option<Vec<u8>>, StorageError>,
    ) -> Result<ResolvedCommandPayload, StorageError> {
        #[cfg(test)]
        super::before_payload_load();
        let bytes = load(self.metadata.first)?.ok_or_else(corrupt)?;
        let (segment, retained) = decode_payload(&self.metadata, bytes, self.cached)?;
        Ok(ResolvedCommandPayload {
            metadata: self.metadata,
            segment,
            retained,
        })
    }
}

fn decode_payload(
    metadata: &SegmentMetadata,
    bytes: Vec<u8>,
    cached: Option<CachedPayload>,
) -> Result<(Arc<StoredCommandSegmentV1>, Option<CachedPayload>), StorageError> {
    let stub = crate::history_stub::segment_summary(&bytes)?;
    if let Some(cached) = cached.filter(|cached| {
        cached_payload_matches(
            &cached.canonical,
            cached.decoded.segment_digest().as_bytes(),
            metadata.first,
            &bytes,
            stub.as_ref(),
        )
    }) {
        metadata.validate(&cached.decoded)?;
        // Only an authority-checked hit refreshes retention. New writes must
        // not evict an active older reader solely because its sequence is old.
        return Ok((Arc::clone(&cached.decoded), Some(cached)));
    }
    #[cfg(test)]
    PAYLOAD_COLD_DECODES.with(|count| count.set(count.get() + 1));
    let segment = Arc::new(
        crate::history_stub::decode_retained_segment(&bytes)
            .map_err(|_| corrupt())?
            .into_parts()
            .0,
    );
    metadata.validate(&segment)?;
    let charge = decoded_charge(&segment, bytes.len()).ok_or_else(corrupt)?;
    let retained = (charge <= MAX_PAYLOAD_BYTES).then(|| CachedPayload {
        canonical: Arc::from(bytes),
        decoded: Arc::clone(&segment),
        charge,
    });
    Ok((segment, retained))
}

impl PayloadCache {
    fn candidate(&self, first: CommitSequence) -> Result<Option<CachedPayload>, StorageError> {
        Ok(self
            .state
            .lock()
            .map_err(|_| corrupt())?
            .entries
            .get(&first)
            .cloned())
    }

    pub(super) fn capture(
        &self,
        metadata: Arc<SegmentMetadata>,
    ) -> Result<CapturedCommandPayload, StorageError> {
        let cached = self.candidate(metadata.first)?;
        Ok(CapturedCommandPayload { metadata, cached })
    }

    pub(super) fn retain_resolved(
        &self,
        metadata: &Arc<SegmentMetadata>,
        resolved: &mut ResolvedCommandPayload,
    ) -> Result<(), StorageError> {
        // Prune or rebuild may have replaced the index while the old pinned
        // read resolved. Such a read never repopulates the new cache.
        if Arc::ptr_eq(metadata, &resolved.metadata)
            && let Some(retained) = resolved.retained.take()
        {
            self.retain_prepared(metadata.first, retained)?;
        }
        Ok(())
    }
    pub(super) fn remove(&mut self, first: CommitSequence) -> Result<(), StorageError> {
        let state = self.state.get_mut().map_err(|_| corrupt())?;
        state.remove(first)
    }

    pub(super) fn resolve(
        &self,
        metadata: &SegmentMetadata,
        load: impl FnOnce(CommitSequence) -> Result<Option<Vec<u8>>, StorageError>,
    ) -> Result<Arc<StoredCommandSegmentV1>, StorageError> {
        // Capture this caller's authority before consulting the disposable cache.
        // Never hold the cache mutex across storage I/O or canonical decoding.
        #[cfg(test)]
        super::before_payload_load();
        let bytes = load(metadata.first)?.ok_or_else(corrupt)?;
        let (segment, retained) = decode_payload(metadata, bytes, self.candidate(metadata.first)?)?;
        if let Some(retained) = retained {
            self.retain_prepared(metadata.first, retained)?;
        }
        Ok(segment)
    }

    /// Retains the writer's own segment for its exact canonical bytes.
    pub(super) fn seed(
        &self,
        metadata: &SegmentMetadata,
        segment: Arc<StoredCommandSegmentV1>,
        canonical: Arc<[u8]>,
    ) -> Result<(), StorageError> {
        metadata.validate(&segment)?;
        if metadata
            .first
            .get()
            .is_multiple_of(PAYLOAD_SEED_VERIFY_EVERY)
        {
            let decoded = crate::command_prefix::decode_segment(&canonical)
                .map_err(|_| corrupt())?
                .into_parts()
                .0;
            if decoded != *segment {
                return Err(corrupt());
            }
        }
        self.retain(metadata.first, canonical, segment)
    }

    fn retain(
        &self,
        first: CommitSequence,
        canonical: Arc<[u8]>,
        segment: Arc<StoredCommandSegmentV1>,
    ) -> Result<(), StorageError> {
        let charge = decoded_charge(&segment, canonical.len()).ok_or_else(corrupt)?;
        if charge > MAX_PAYLOAD_BYTES {
            return Ok(());
        }
        self.retain_prepared(
            first,
            CachedPayload {
                canonical,
                decoded: segment,
                charge,
            },
        )
    }

    fn retain_prepared(
        &self,
        first: CommitSequence,
        retained: CachedPayload,
    ) -> Result<(), StorageError> {
        let charge = retained.charge;
        let mut state = self.state.lock().map_err(|_| corrupt())?;
        state.remove(first)?;
        while state.entries.len() >= MAX_PAYLOAD_ENTRIES || state.bytes > MAX_PAYLOAD_BYTES - charge
        {
            let first = state.recency.front().copied().ok_or_else(corrupt)?;
            state.remove(first)?;
        }
        state.entries.insert(first, retained);
        state.recency.push_back(first);
        state.bytes += charge;
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn clear(&self) {
        *self.state.lock().unwrap() = PayloadEntries::default();
    }

    #[cfg(test)]
    pub(super) fn stats(&self) -> (usize, usize) {
        let state = self.state.lock().unwrap();
        (state.entries.len(), state.bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_types::{CanonicalRecord, CanonicalValue, FieldId};

    // req: REP-002
    #[test]
    fn decoded_tree_charge_covers_sparse_nested_values_and_variable_buffers() {
        let values = CanonicalValue::list(vec![CanonicalValue::Null; 1024]).unwrap();
        let record = CanonicalRecord::new(vec![(FieldId::new(1).unwrap(), values)]).unwrap();
        let charge = record_charge(&record).unwrap();
        assert!(charge >= 1024 * std::mem::size_of::<CanonicalValue>());
        let bytes = CanonicalValue::bytes(vec![42; 8192]).unwrap();
        let nested = CanonicalValue::list(vec![CanonicalValue::Record(record), bytes]).unwrap();
        assert!(value_charge(&nested).unwrap() >= charge + 8192);
    }
}

#[cfg(test)]
mod stub_match_tests {
    use super::*;

    // req: REC-005
    #[test]
    fn a_history_stub_names_the_cached_segment_by_first_sequence_and_digest() {
        let first = CommitSequence::new(9).expect("sequence");
        let summary = crate::history_stub::SegmentSummary {
            first_commit_sequence: Some(first),
            last_commit_sequence: Some(first),
            terminal_administration_sequence: None,
            command_count: 1,
            segment_digest: [7; 32],
        };
        let canonical = b"segment".as_slice();
        assert!(cached_payload_matches(
            canonical, &[7; 32], first, canonical, None
        ));
        assert!(!cached_payload_matches(
            canonical, &[7; 32], first, b"other", None
        ));
        assert!(cached_payload_matches(
            canonical,
            &[7; 32],
            first,
            b"stub",
            Some(&summary)
        ));
        assert!(
            !cached_payload_matches(canonical, &[8; 32], first, b"stub", Some(&summary)),
            "another segment's digest"
        );
        let later = CommitSequence::new(10).expect("sequence");
        assert!(
            !cached_payload_matches(canonical, &[7; 32], later, b"stub", Some(&summary)),
            "another segment's key"
        );
    }
}
