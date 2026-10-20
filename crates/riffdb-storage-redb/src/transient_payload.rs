//! Disposable payload retention, separate from complete exact index metadata.
use super::*;
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

pub(super) type SegmentMetadataRef = ChargedLocatorAllocation<SegmentMetadata>;

pub(super) struct MetadataManifestEntry {
    kind: CommandDerivedIndexKindV1,
    key: ChargedLocatorKey,
    locator: CommandDerivedLocator,
}

impl MetadataManifestEntry {
    pub(super) fn kind(&self) -> CommandDerivedIndexKindV1 {
        self.kind
    }
    pub(super) fn exact_key(&self) -> &[u8] {
        self.key.as_ref()
    }
    pub(super) fn charged_key(&self) -> ChargedLocatorKey {
        self.key.clone()
    }
    pub(super) fn locator(&self) -> CommandDerivedLocator {
        self.locator
    }
    pub(super) fn segment_first_commit_sequence(&self) -> CommitSequence {
        self.locator.segment_first
    }
    pub(super) fn command_ordinal(&self) -> u16 {
        self.locator.command_ordinal
    }
    pub(super) fn member_ordinal(&self) -> u16 {
        self.locator.member_ordinal
    }
    pub(super) fn member(&self) -> CommandDerivedMemberV1 {
        self.locator.member
    }
}

#[derive(Default)]
pub(super) struct MetadataManifest {
    entries: Vec<MetadataManifestEntry>,
}
impl MetadataManifest {
    pub(super) fn entries(&self) -> &[MetadataManifestEntry] {
        &self.entries
    }
}

pub(super) struct SegmentMetadata {
    pub(super) first: CommitSequence,
    pub(super) last: CommitSequence,
    pub(super) digest: CommandSegmentDigestV1,
    database: DatabaseId,
    history: u64,
    pub(super) manifest: MetadataManifest,
    pub(super) events: Vec<(PartitionKeyHash, StoredEventRouteV1)>,
    pub(super) key_ownership_bytes: usize,
    construction_error: Option<StorageErrorKind>,
}

impl LocatorRetireContents for SegmentMetadata {
    fn retire_one(&mut self) -> bool {
        if self.manifest.entries.pop().is_some() {
            return false;
        }
        if self.events.pop().is_some() {
            return false;
        }
        true
    }
}

impl SegmentMetadata {
    pub(super) fn try_new(
        pool: &NativeLocatorPool,
        class: LocatorAllocationClass,
        segment: &StoredCommandSegmentV1,
    ) -> Result<SegmentMetadataRef, StorageError> {
        let entry_count = segment.manifest().entries().len();
        let event_count = segment
            .commands()
            .iter()
            .try_fold(0usize, |count, command| {
                count
                    .checked_add(command.events().len())
                    .ok_or_else(unavailable)
            })?;
        // Both the manifest and event vectors preallocate their exact final
        // counts and never grow, so each needs one element allocation rather
        // than growth headroom. The wrapper admits its live value and
        // retirement task separately.
        let content_bytes = entry_count
            .checked_mul(std::mem::size_of::<MetadataManifestEntry>())
            .and_then(|n| {
                n.checked_add(
                    event_count
                        .checked_mul(
                            std::mem::size_of::<(PartitionKeyHash, StoredEventRouteV1)>(),
                        )?,
                )
            })
            .and_then(|n| n.checked_add(64))
            .ok_or_else(unavailable)?;
        let metadata = ChargedLocatorAllocation::try_new(pool, class, content_bytes, || {
            let mut metadata = Self {
                first: segment.first_commit_sequence(),
                last: segment.last_commit_sequence(),
                digest: segment.segment_digest(),
                database: segment.database_id(),
                history: segment.history_incarnation(),
                manifest: MetadataManifest {
                    entries: Vec::with_capacity(entry_count),
                },
                events: Vec::with_capacity(event_count),
                key_ownership_bytes: 0,
                construction_error: None,
            };
            // Idempotency construction holds its canonical identity key and
            // one expected-key copy simultaneously. Admit both maximum legal
            // capacities and their allocation headers before encoding either.
            let built = (|| {
                let _scratch = pool.admit(
                    2 * riffdb_storage_api::MAX_IDEMPOTENCY_IDENTITY_KEY_V1_BYTES + 256,
                    class,
                )?;
                for entry in segment.manifest().entries() {
                    if entry.segment_first_commit_sequence() != metadata.first {
                        return Err(corrupt());
                    }
                    let command = segment
                        .commands()
                        .get(usize::from(entry.command_ordinal()))
                        .ok_or_else(corrupt)?;
                    let expected = expected_manifest_key(
                        command,
                        entry.kind(),
                        entry.member(),
                        entry.member_ordinal(),
                    )?;
                    let member = match entry.kind() {
                        CommandDerivedIndexKindV1::Idempotency
                        | CommandDerivedIndexKindV1::Provenance => CommandDerivedMemberV1::Command,
                        CommandDerivedIndexKindV1::AuditSequence
                        | CommandDerivedIndexKindV1::AuditRequest => entry.member(),
                        CommandDerivedIndexKindV1::EventRoute
                        | CommandDerivedIndexKindV1::PendingOutbox => CommandDerivedMemberV1::Event,
                    };
                    if entry.member() != member || entry.exact_key() != expected {
                        return Err(corrupt());
                    }
                    let key = ChargedLocatorKey::try_new(pool, class, entry.exact_key())?;
                    // Original manifest keys and the metadata wrapper belong to
                    // the same immutable sharing family before either publishes.
                    key.validate_family_pool(pool)?;
                    metadata.key_ownership_bytes = metadata
                        .key_ownership_bytes
                        .checked_add(key.retained_charge_bound())
                        .ok_or_else(unavailable)?;
                    metadata.manifest.entries.push(MetadataManifestEntry {
                        kind: entry.kind(),
                        key,
                        locator: CommandDerivedLocator {
                            segment_first: entry.segment_first_commit_sequence(),
                            command_ordinal: entry.command_ordinal(),
                            member_ordinal: entry.member_ordinal(),
                            member: entry.member(),
                        },
                    });
                }
                for command in segment.commands() {
                    for event in command.events() {
                        metadata.events.push((
                            command.base().commit().partition_hash(),
                            StoredEventRouteV1::new(
                                event.event_id(),
                                event.event_type_id(),
                                event.event_hash(),
                            ),
                        ));
                    }
                }
                metadata
                    .events
                    .sort_unstable_by_key(|(_, route)| route.event_id());
                if metadata
                    .events
                    .windows(2)
                    .any(|pair| pair[0].1.event_id() == pair[1].1.event_id())
                {
                    return Err(corrupt());
                }
                Ok::<(), StorageError>(())
            })();
            metadata.construction_error = built.err().map(|error| error.kind());
            Ok(metadata)
        })?;
        if let Some(kind) = metadata.construction_error {
            return Err(StorageError::new(kind, None));
        }
        Ok(metadata)
    }

    pub(super) fn validate(&self, segment: &StoredCommandSegmentV1) -> Result<(), StorageError> {
        if segment.first_commit_sequence() != self.first
            || segment.last_commit_sequence() != self.last
            || segment.segment_digest() != self.digest
            || segment.database_id() != self.database
            || segment.history_incarnation() != self.history
            || segment.manifest().entries().len() != self.manifest.entries.len()
            || !segment
                .manifest()
                .entries()
                .iter()
                .zip(&self.manifest.entries)
                .all(|(a, b)| {
                    a.kind() == b.kind()
                        && a.exact_key() == b.exact_key()
                        && a.segment_first_commit_sequence() == b.segment_first_commit_sequence()
                        && a.command_ordinal() == b.command_ordinal()
                        && a.member_ordinal() == b.member_ordinal()
                        && a.member() == b.member()
                })
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
    pub(super) metadata: SegmentMetadataRef,
    cached: Option<CachedPayload>,
}

pub(crate) struct ResolvedCommandPayload {
    metadata: SegmentMetadataRef,
    pub(crate) segment: Arc<StoredCommandSegmentV1>,
    retained: Option<CachedPayload>,
}

impl CapturedCommandPayload {
    pub(super) fn without_cache(metadata: SegmentMetadataRef) -> Self {
        Self {
            metadata,
            cached: None,
        }
    }
    pub(crate) fn resolve(
        self,
        load: impl FnOnce(CommitSequence) -> Result<Option<Vec<u8>>, StorageError>,
    ) -> Result<ResolvedCommandPayload, StorageError> {
        self.resolve_with_source(load, None)
    }

    pub(crate) fn resolve_at_access(
        self,
        access: &crate::store::RedbReadAccess,
    ) -> Result<ResolvedCommandPayload, StorageError> {
        self.resolve_with_source(
            |first| {
                access.read_value(
                    crate::journal::JournalTable::Commits,
                    &crate::keys::encode_application_sequence_key(first),
                )
            },
            Some(access),
        )
    }

    fn resolve_with_source(
        self,
        load: impl FnOnce(CommitSequence) -> Result<Option<Vec<u8>>, StorageError>,
        access: Option<&crate::store::RedbReadAccess>,
    ) -> Result<ResolvedCommandPayload, StorageError> {
        #[cfg(test)]
        super::before_payload_load();
        let bytes = load(self.metadata.first)?.ok_or_else(corrupt)?;
        let (segment, retained) = decode_payload(&self.metadata, bytes, self.cached, access)?;
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
    access: Option<&crate::store::RedbReadAccess>,
) -> Result<(Arc<StoredCommandSegmentV1>, Option<CachedPayload>), StorageError> {
    let decoded_stub = crate::history_stub::HistoryStub::decode(&bytes)?;
    if let Some(stub) = decoded_stub
        && (stub.kind != crate::history_stub::StubKind::Segment
            || stub.database_id != metadata.database
            || stub.summary.first_commit_sequence != Some(metadata.first)
            || stub.summary.last_commit_sequence != Some(metadata.last)
            || stub.summary.segment_digest != *metadata.digest.as_bytes()
            || u64::from(stub.summary.command_count)
                != metadata.last.get() - metadata.first.get() + 1)
    {
        return Err(corrupt());
    }
    let stub = decoded_stub.map(|stub| stub.summary);
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
    // Resolve only a cold miss, after checking the stub's complete metadata
    // binding. The captured source owns the media resolver for this read;
    // process-current resolver registration cannot authorize older snapshots.
    let resolved = match access {
        Some(access) => access.resolve_command_segment_value(&bytes)?,
        None => crate::history_stub::resolve_commits_value(&bytes)?,
    };
    let decoded_wire_bytes = bytes.len().max(resolved.len());
    let segment = Arc::new(
        crate::history_stub::decode_retained_segment(&resolved)
            .map_err(|_| corrupt())?
            .into_parts()
            .0,
    );
    drop(resolved);
    metadata.validate(&segment)?;
    let charge = decoded_charge(&segment, decoded_wire_bytes).ok_or_else(corrupt)?;
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
        metadata: SegmentMetadataRef,
    ) -> Result<CapturedCommandPayload, StorageError> {
        let cached = self.candidate(metadata.first)?;
        Ok(CapturedCommandPayload { metadata, cached })
    }

    pub(super) fn retain_resolved(
        &self,
        metadata: &SegmentMetadataRef,
        resolved: &mut ResolvedCommandPayload,
    ) -> Result<(), StorageError> {
        // Prune or rebuild may have replaced the index while the old pinned
        // read resolved. Such a read never repopulates the new cache.
        if SegmentMetadataRef::ptr_eq(metadata, &resolved.metadata)
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

    #[cfg(test)]
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
        let (segment, retained) =
            decode_payload(metadata, bytes, self.candidate(metadata.first)?, None)?;
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

    // req: REC-005, STO-032
    #[test]
    fn cold_stub_cache_charges_resolved_content_and_cached_hits_need_no_extent() {
        use crate::history_stub::{HistoryResolver, SegmentSummary, StubLocator};
        let command = crate::derived::follower_indexes::capsule(CommitSequence::first());
        let (segment, encoded) = crate::derived::follower_indexes::segment(vec![command], None);
        let metadata = SegmentMetadata::try_new(
            &NativeLocatorPool::global(),
            LocatorAllocationClass::Ordinary,
            &segment,
        )
        .unwrap();
        let resolver = HistoryResolver::register(
            segment.database_id(),
            std::path::Path::new("absent/payload-cache.redb"),
            Arc::new(crate::media::RealJournalMedia),
        );
        let hash = [0x9c; 32];
        let mutations = crate::changelog_v3_cursor::SourceMutations::try_new(
            vec![
                riffdb_storage_api::CompositeMutationV1::put(
                    riffdb_storage_api::CompositeTableV1::Commits,
                    crate::keys::encode_application_sequence_key(segment.first_commit_sequence())
                        .to_vec(),
                    encoded.as_bytes().to_vec(),
                )
                .unwrap(),
            ],
            crate::locator_memory::LocatorAllocationClass::Ordinary,
        )
        .unwrap();
        resolver.note_pending_frame(hash, &mutations);
        let stub = StubLocator {
            database_id: segment.database_id(),
            generation: 1,
            placement: crate::journal::FramePlacement {
                position: 8192,
                padded_len: 4096,
                frame_hash: hash,
            },
        }
        .summary_stub(SegmentSummary::of(&segment).unwrap())
        .unwrap();
        assert!(encoded.as_bytes().len() > stub.len());
        let (decoded, retained) = decode_payload(&metadata, stub.clone(), None, None).unwrap();
        assert_eq!(decoded.as_ref(), &segment);
        let cached = retained.expect("small fixture fits the disposable cache");
        assert_eq!(
            cached.charge,
            decoded_charge(&segment, encoded.as_bytes().len()).unwrap(),
            "a short locator must not replace the decoded content's ownership allowance"
        );
        drop(mutations);
        // There is no file and the pending source has gone. A metadata-bound
        // positive candidate remains usable without another extent allocation.
        let (hit, _) = decode_payload(&metadata, stub, Some(cached), None).unwrap();
        assert!(Arc::ptr_eq(&hit, &decoded));
    }

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
