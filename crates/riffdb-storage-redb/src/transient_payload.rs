//! Disposable payload retention, separate from complete exact index metadata.
use super::*;
use riffdb_storage_api::CommandSegmentManifestV1;
use riffdb_types::DatabaseId;
use std::sync::Mutex;

const MAX_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;
const MAX_PAYLOAD_ENTRIES: usize = 64;
/// One seeded segment in this many is decoded from its canonical bytes and
/// compared with the writer's segment before it is retained.
pub(crate) const PAYLOAD_SEED_VERIFY_EVERY: u64 = 64;
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

struct CachedPayload {
    canonical: Arc<[u8]>,
    decoded: Arc<StoredCommandSegmentV1>,
    charge: usize,
}

#[derive(Default)]
struct PayloadEntries {
    entries: BTreeMap<CommitSequence, CachedPayload>,
    bytes: usize,
}

#[derive(Default)]
pub(super) struct PayloadCache {
    state: Mutex<PayloadEntries>,
}

impl PayloadCache {
    pub(super) fn remove(&mut self, first: CommitSequence) -> Result<(), StorageError> {
        let state = self.state.get_mut().map_err(|_| corrupt())?;
        if let Some(old) = state.entries.remove(&first) {
            state.bytes -= old.charge;
        }
        Ok(())
    }

    pub(super) fn resolve(
        &self,
        metadata: &SegmentMetadata,
        load: impl FnOnce(CommitSequence) -> Result<Option<Vec<u8>>, StorageError>,
    ) -> Result<Arc<StoredCommandSegmentV1>, StorageError> {
        // Capture this caller's authority before consulting the disposable cache.
        // Never hold the cache mutex across storage I/O or canonical decoding.
        let bytes = load(metadata.first)?.ok_or_else(corrupt)?;
        // The overlay and checkpoint hold a history stub for a journaled
        // segment (ADR-0263); it names the cached segment by its digest.
        let stub = crate::history_stub::segment_summary(&bytes)?;
        let cached = {
            let state = self.state.lock().map_err(|_| corrupt())?;
            state
                .entries
                .get(&metadata.first)
                .filter(|cached| {
                    cached_payload_matches(
                        &cached.canonical,
                        cached.decoded.segment_digest().as_bytes(),
                        metadata.first,
                        &bytes,
                        stub.as_ref(),
                    )
                })
                .map(|cached| Arc::clone(&cached.decoded))
        };
        if let Some(cached) = cached {
            metadata.validate(&cached)?;
            return Ok(cached);
        }
        let segment = Arc::new(
            crate::history_stub::decode_retained_segment(&bytes)
                .map_err(|_| corrupt())?
                .into_parts()
                .0,
        );
        metadata.validate(&segment)?;
        self.retain(metadata.first, Arc::from(bytes), Arc::clone(&segment))?;
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
        let mut state = self.state.lock().map_err(|_| corrupt())?;
        if let Some(old) = state.entries.remove(&first) {
            state.bytes -= old.charge;
        }
        while state.entries.len() >= MAX_PAYLOAD_ENTRIES || state.bytes > MAX_PAYLOAD_BYTES - charge
        {
            let (_, old) = state.entries.pop_first().ok_or_else(corrupt)?;
            state.bytes -= old.charge;
        }
        state.entries.insert(
            first,
            CachedPayload {
                canonical,
                decoded: segment,
                charge,
            },
        );
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
