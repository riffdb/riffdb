//! Test-only constructors for stopped-database compatibility fixtures.

use std::path::Path;

use redb::{Database, ReadableDatabase, ReadableTable};
use riffdb_storage_api::{
    ActiveCatalogPointerV1, ApplicationSequenceAllocator, DatabaseInitializationPort, StorageError,
    StorageErrorKind, StoredCommitRecordV1, StoredContractBundleV1, StoredEntityRecordV1,
    StoredIndexEntryV1, StoredOutcomeV1, StoredProvenanceRecordV1,
    decode_application_sequence_allocator_v1,
};

use crate::error::{
    codec_error, commit_error, database_error, precommit_storage_error, storage_error, table_error,
    transaction_error,
};
use crate::layout::{
    AUDIT, CATALOG_ACTIVE, CATALOG_ACTIVE_KEY, COMMITS, CONTRACT_BUNDLES, ENTITIES, EVENT_ROUTES,
    IDEMPOTENCY, META, META_APPLICATION_SEQUENCE, META_CLEAN_CLOSE_LIFECYCLE,
    META_VALIDATED_PREFIX_CHECKPOINT, PROVENANCE, SECONDARY_INDEXES,
};

/// Appends exact contiguous authoritative entity/commit pairs for server worker tests.
#[doc(hidden)]
pub fn append_columnar_worker_commit_fixture(
    ports: &crate::store::RedbOperationalPorts,
    rows: &[StoredEntityRecordV1],
    commits: &[StoredCommitRecordV1],
) -> Result<(), StorageError> {
    if rows.is_empty() || rows.len() != commits.len() || rows.len() > 16_384 {
        return Err(storage_error(StorageErrorKind::LimitExceeded));
    }
    let access = ports.begin_write()?;
    let transaction = access.transaction()?;
    let first_expected = {
        let meta = transaction.open_table(META).map_err(table_error)?;
        let encoded = meta
            .get(META_APPLICATION_SEQUENCE)
            .map_err(precommit_storage_error)?
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        match decode_application_sequence_allocator_v1(encoded.value())
            .map_err(codec_error)?
            .into_parts()
            .0
        {
            ApplicationSequenceAllocator::Next(next) => next,
            ApplicationSequenceAllocator::Exhausted => {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
        }
    };
    for (index, (row, commit)) in rows.iter().zip(commits).enumerate() {
        let expected_sequence = u64::try_from(index)
            .ok()
            .and_then(|offset| first_expected.get().checked_add(offset))
            .and_then(riffdb_types::CommitSequence::new)
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        if commit.commit_sequence() != expected_sequence {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let encoded_row = crate::codec::encode_entity_record_v1(row)?;
        if transaction
            .open_table(ENTITIES)
            .map_err(table_error)?
            .insert(
                crate::keys::encode_entity_key(row.target().key()),
                encoded_row.as_bytes(),
            )
            .map_err(precommit_storage_error)?
            .is_some()
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let encoded_commit = crate::codec::encode_commit_record_v1(commit)?;
        let commit_key = crate::keys::encode_application_sequence_key(commit.commit_sequence());
        if transaction
            .open_table(COMMITS)
            .map_err(table_error)?
            .insert(commit_key.as_slice(), encoded_commit.as_bytes())
            .map_err(precommit_storage_error)?
            .is_some()
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
    }
    let last = commits
        .last()
        .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
    let allocator = last.commit_sequence().checked_next().map_or(
        ApplicationSequenceAllocator::Exhausted,
        ApplicationSequenceAllocator::Next,
    );
    let encoded_allocator = crate::codec::encode_application_sequence_allocator_v1(allocator)?;
    transaction
        .open_table(META)
        .map_err(table_error)?
        .insert(META_APPLICATION_SEQUENCE, encoded_allocator.as_bytes())
        .map_err(precommit_storage_error)?;
    access.commit_for(crate::hooks::RedbTestOperation::CommandBatch)
}

/// Appends exact contiguous authoritative rows/commits plus their reciprocal
/// outcome and provenance records for process-reopen columnar tests.
#[doc(hidden)]
pub fn append_columnar_worker_commit_with_crosslinks_fixture(
    ports: &crate::store::RedbOperationalPorts,
    rows: &[StoredEntityRecordV1],
    commits: &[StoredCommitRecordV1],
    outcomes: &[StoredOutcomeV1],
    provenance: &[StoredProvenanceRecordV1],
) -> Result<(), StorageError> {
    if rows.is_empty()
        || rows.len() != commits.len()
        || rows.len() != outcomes.len()
        || rows.len() != provenance.len()
        || rows.len() > 16_384
    {
        return Err(storage_error(StorageErrorKind::LimitExceeded));
    }
    let access = ports.begin_write()?;
    let transaction = access.transaction()?;
    let first_expected = {
        let meta = transaction.open_table(META).map_err(table_error)?;
        let encoded = meta
            .get(META_APPLICATION_SEQUENCE)
            .map_err(precommit_storage_error)?
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        match decode_application_sequence_allocator_v1(encoded.value())
            .map_err(codec_error)?
            .into_parts()
            .0
        {
            ApplicationSequenceAllocator::Next(next) => next,
            ApplicationSequenceAllocator::Exhausted => {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            }
        }
    };
    for (index, (((row, commit), outcome), provenance)) in rows
        .iter()
        .zip(commits)
        .zip(outcomes)
        .zip(provenance)
        .enumerate()
    {
        let expected_sequence = u64::try_from(index)
            .ok()
            .and_then(|offset| first_expected.get().checked_add(offset))
            .and_then(riffdb_types::CommitSequence::new)
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
        if commit.commit_sequence() != expected_sequence
            || outcome.commit_sequence() != expected_sequence
            || provenance.commit_sequence() != expected_sequence
            || outcome.provenance_id() != provenance.provenance_id()
            || outcome.provenance_id() != commit.provenance_id()
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let encoded_row = crate::codec::encode_entity_record_v1(row)?;
        if transaction
            .open_table(ENTITIES)
            .map_err(table_error)?
            .insert(
                crate::keys::encode_entity_key(row.target().key()),
                encoded_row.as_bytes(),
            )
            .map_err(precommit_storage_error)?
            .is_some()
        {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let encoded_commit = crate::codec::encode_commit_record_v1(commit)?;
        let commit_key = crate::keys::encode_application_sequence_key(expected_sequence);
        transaction
            .open_table(COMMITS)
            .map_err(table_error)?
            .insert(commit_key.as_slice(), encoded_commit.as_bytes())
            .map_err(precommit_storage_error)?;
        let encoded_outcome = crate::codec::encode_stored_outcome_v1(outcome)?;
        let identity_key = outcome
            .identity()
            .storage_key()
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        transaction
            .open_table(IDEMPOTENCY)
            .map_err(table_error)?
            .insert(identity_key.as_bytes(), encoded_outcome.as_bytes())
            .map_err(precommit_storage_error)?;
        let encoded_provenance = crate::codec::encode_provenance_record_v1(provenance)?;
        let provenance_key = crate::keys::encode_provenance_key(provenance.provenance_id());
        transaction
            .open_table(PROVENANCE)
            .map_err(table_error)?
            .insert(provenance_key.as_slice(), encoded_provenance.as_bytes())
            .map_err(precommit_storage_error)?;
    }
    let last = commits
        .last()
        .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
    let allocator = last.commit_sequence().checked_next().map_or(
        ApplicationSequenceAllocator::Exhausted,
        ApplicationSequenceAllocator::Next,
    );
    let encoded_allocator = crate::codec::encode_application_sequence_allocator_v1(allocator)?;
    transaction
        .open_table(META)
        .map_err(table_error)?
        .insert(META_APPLICATION_SEQUENCE, encoded_allocator.as_bytes())
        .map_err(precommit_storage_error)?;
    access.commit_for(crate::hooks::RedbTestOperation::CommandBatch)
}

/// Seeds and opens least-authority ports for one isolated contract-migration fixture.
pub(crate) fn contract_migration_stage_ports_fixture(
    path: &Path,
    database_id: riffdb_types::DatabaseId,
    parent: &StoredContractBundleV1,
    rows: &[StoredEntityRecordV1],
) -> Result<crate::store::RedbOperationalPorts, StorageError> {
    let mut store = crate::RedbStore::open(path)?;
    store.initialize_database(database_id)?;
    let transaction = store
        .shared
        .database
        .begin_write()
        .map_err(transaction_error)?;
    let bundle_key =
        crate::keys::encode_contract_bundle_key(parent.lineage(), parent.contract_version())
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
    let encoded_bundle = crate::codec::encode_contract_bundle_v1(parent)?;
    transaction
        .open_table(CONTRACT_BUNDLES)
        .map_err(table_error)?
        .insert(bundle_key.as_slice(), encoded_bundle.as_bytes())
        .map_err(precommit_storage_error)?;
    let active = ActiveCatalogPointerV1::from_bundle(parent);
    let encoded_active = crate::codec::encode_active_catalog_pointer_v1(&active)?;
    transaction
        .open_table(CATALOG_ACTIVE)
        .map_err(table_error)?
        .insert(CATALOG_ACTIVE_KEY.as_slice(), encoded_active.as_bytes())
        .map_err(precommit_storage_error)?;
    let mut entities = transaction.open_table(ENTITIES).map_err(table_error)?;
    for row in rows {
        let encoded = crate::codec::encode_entity_record_v1(row)?;
        entities
            .insert(
                crate::keys::encode_entity_key(row.target().key()),
                encoded.as_bytes(),
            )
            .map_err(precommit_storage_error)?;
    }
    drop(entities);
    store.shared.commit_durable(transaction)?;
    // Fresh production initialization already installed the current V2 receipt
    // substrate. Seeding fixture rows must not re-activate or relabel it as V1.
    Ok(crate::store::RedbOperationalPorts {
        shared: store.shared,
    })
}

/// Replaces every canonical V2 secondary-index row in a stopped database with
/// its canonical V1 migration source.
///
/// The normal writer remains V2-only. This function is absent from default
/// builds and must be called only after the owning process has stopped.
pub fn downgrade_all_index_rows_to_v1_fixture(path: &Path) -> Result<usize, StorageError> {
    let database = Database::create(path).map_err(database_error)?;
    let transaction = database.begin_write().map_err(transaction_error)?;
    let replacements = {
        let table = transaction
            .open_table(SECONDARY_INDEXES)
            .map_err(table_error)?;
        let mut replacements = Vec::new();
        for entry in table.iter().map_err(precommit_storage_error)? {
            let (physical_key, envelope) = entry.map_err(precommit_storage_error)?;
            let physical_key =
                riffdb_types::IndexEntryKey::from_bytes(physical_key.value().to_vec())
                    .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
            let current = crate::codec::decode_index_entry_v2(envelope.value())?;
            if current.value().key() != &physical_key {
                return Err(storage_error(StorageErrorKind::CorruptData));
            }
            let legacy = StoredIndexEntryV1::new(
                physical_key.clone(),
                current.value().schema_binding().clone(),
                current.value().covered_values().clone(),
            )
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
            let legacy =
                riffdb_storage_api::encode_index_entry_v1_fixture(&legacy).map_err(codec_error)?;
            replacements.push((physical_key, legacy));
        }
        replacements
    };

    {
        let mut table = transaction
            .open_table(SECONDARY_INDEXES)
            .map_err(table_error)?;
        for (physical_key, legacy) in &replacements {
            table
                .insert(physical_key.as_bytes(), legacy.as_bytes())
                .map_err(precommit_storage_error)?;
        }
    }
    // This test-only stopped-database rewrite happens after the source process
    // closed. It must invalidate that process's clean evidence and its
    // validated-prefix checkpoint (a real database never holds V1 rows under a
    // current-registry checkpoint) so startup takes the complete
    // migration-discovery path rather than the ADR-0182 bounded dirty path.
    {
        let mut meta = transaction.open_table(META).map_err(table_error)?;
        meta.remove(META_CLEAN_CLOSE_LIFECYCLE)
            .map_err(precommit_storage_error)?;
        meta.remove(META_VALIDATED_PREFIX_CHECKPOINT)
            .map_err(precommit_storage_error)?;
    }
    transaction.commit().map_err(commit_error)?;
    Ok(replacements.len())
}

/// The fixed key and bytes of the malformed population row planted by
/// [`plant_malformed_population_row_fixture`].
pub const MALFORMED_POPULATION_ROW_FIXTURE: (&[u8], &[u8]) = (
    b"fixture-malformed-event-route-key",
    b"fixture-malformed-event-route-value",
);

/// Plants one malformed `EVENT_ROUTES` row in a stopped database, outside
/// every receipt, for corruption-detection tests. The table is population
/// state that bounded dirty admission and its header neither count, probe,
/// nor cross-link, so a later bounded dirty startup reaches readiness over it
/// and only first access or a complete walk can refuse it.
///
/// Absent from default builds; call only after the owning process stopped.
pub fn plant_malformed_population_row_fixture(path: &Path) -> Result<(), StorageError> {
    let database = Database::create(path).map_err(database_error)?;
    let transaction = database.begin_write().map_err(transaction_error)?;
    transaction
        .open_table(EVENT_ROUTES)
        .map_err(table_error)?
        .insert(
            MALFORMED_POPULATION_ROW_FIXTURE.0,
            MALFORMED_POPULATION_ROW_FIXTURE.1,
        )
        .map_err(precommit_storage_error)?;
    transaction.commit().map_err(commit_error)
}

/// Reads the planted malformed row's raw bytes, proving a detector neither
/// repaired nor removed it. Call only after the owning process stopped.
pub fn read_malformed_population_row_fixture(path: &Path) -> Result<Option<Vec<u8>>, StorageError> {
    let database = Database::create(path).map_err(database_error)?;
    let transaction = database.begin_read().map_err(transaction_error)?;
    let table = transaction.open_table(EVENT_ROUTES).map_err(table_error)?;
    let row = table
        .get(MALFORMED_POPULATION_ROW_FIXTURE.0)
        .map_err(precommit_storage_error)?
        .map(|value| value.value().to_vec());
    Ok(row)
}

/// Overwrites one existing, non-tail `AUDIT` row of a stopped V2 database with
/// a decodable primary-fence administration record for the current lineage,
/// written outside every transaction (ADR-0260's stray fence row).
///
/// Bounded startup reads neither that row (it is not the tail) nor, for an
/// unfenced admission, any fence; only the exhaustive retained-fence scan
/// refuses it. Absent from default builds; call only after the owning process
/// stopped.
pub fn replace_audit_row_with_stray_fence_fixture(
    path: &Path,
    administration_sequence: u64,
) -> Result<(), StorageError> {
    use riffdb_storage_api::{
        AuditPrincipalV1, ChangelogHistoryPointV3, StoredPrimaryFenceAdministrationV1,
        proto_codec::encode_primary_fence_administration_v1,
    };
    use riffdb_types::{
        ActorId, ActorKind, AdministrationSequence, CapabilityId, DualFrontier,
        ReplicationFenceOperationId, ReplicationFollowerAuditTargetV1, ReplicationSourceHoldIdV1,
        RequestId, Timestamp,
    };
    let invalid = || storage_error(StorageErrorKind::InvariantViolation);
    let uuid = |last: u8| {
        [
            0x01, 0x9b, 0xf6, 0xaa, 0xa6, 0x40, 0x7d, 0xe6, 0x89, 0xc9, 0x8a, 0x7f, 0x70, 0xbb,
            0xbd, last,
        ]
    };
    let database = Database::create(path).map_err(database_error)?;
    let (history, tail_key) = {
        let read = database.begin_read().map_err(transaction_error)?;
        let history =
            crate::changelog_v3_roots::read_checkpoint_roots(&read)?.ok_or_else(invalid)?;
        let audit = read.open_table(AUDIT).map_err(table_error)?;
        let tail_key = audit
            .last()
            .map_err(precommit_storage_error)?
            .map(|(key, _)| key.value().to_vec())
            .ok_or_else(invalid)?;
        (history, tail_key)
    };
    let sequence = AdministrationSequence::new(administration_sequence).ok_or_else(invalid)?;
    let key = crate::keys::encode_audit_key(sequence);
    if key.as_slice() >= tail_key.as_slice() {
        return Err(invalid());
    }
    let lineage = history.lineage();
    let tail = history.tail();
    let observed = ChangelogHistoryPointV3::new(
        tail.sequence(),
        tail.history_hash(),
        DualFrontier::new(
            tail.frontier().application(),
            AdministrationSequence::new(administration_sequence - 1),
        ),
    );
    let record = StoredPrimaryFenceAdministrationV1::new(
        sequence,
        Timestamp::new(1, 0).map_err(|_| invalid())?,
        ReplicationFenceOperationId::from_bytes(uuid(0x31)).map_err(|_| invalid())?,
        RequestId::from_bytes(uuid(0x32)).map_err(|_| invalid())?,
        AuditPrincipalV1::new(
            ActorId::new("stray-fence-fixture").map_err(|_| invalid())?,
            ActorKind::Human,
            CapabilityId::from_bytes(uuid(0x33)).map_err(|_| invalid())?,
            std::num::NonZeroU64::MIN,
        ),
        None,
        ReplicationFollowerAuditTargetV1::new(
            lineage.database_id(),
            lineage.history_incarnation(),
            lineage.leadership_epoch(),
            ReplicationSourceHoldIdV1::new([0x5a; 16]).ok_or_else(invalid)?,
        )
        .ok_or_else(invalid)?,
        tail.sequence(),
        observed,
    )
    .map_err(|_| invalid())?;
    let encoded = encode_primary_fence_administration_v1(&record).map_err(codec_error)?;
    let transaction = database.begin_write().map_err(transaction_error)?;
    transaction
        .open_table(AUDIT)
        .map_err(table_error)?
        .insert(key.as_slice(), encoded.as_bytes())
        .map_err(precommit_storage_error)?
        .ok_or_else(invalid)?;
    transaction.commit().map_err(commit_error)
}

/// Reads the commit sequence S bound in a stopped database's durable
/// validated-prefix checkpoint, or `None` when no checkpoint row exists.
///
/// Read-only observation for stopped-database recovery tests. Must be called
/// only after the owning process has stopped.
pub fn read_validated_prefix_checkpoint_commit_sequence_fixture(
    path: &Path,
) -> Result<Option<u64>, StorageError> {
    let database = Database::open(path).map_err(database_error)?;
    let transaction = database.begin_read().map_err(transaction_error)?;
    let meta = transaction.open_table(META).map_err(table_error)?;
    let Some(encoded) = meta
        .get(META_VALIDATED_PREFIX_CHECKPOINT)
        .map_err(precommit_storage_error)?
    else {
        return Ok(None);
    };
    let checkpoint =
        riffdb_storage_api::proto_codec::decode_validated_prefix_checkpoint_v2(encoded.value())
            .map_err(codec_error)?
            .into_parts()
            .0;
    Ok(Some(checkpoint.base().checkpoint_commit_sequence()))
}

/// Reads the exact retained checkpoint envelope bytes from a stopped database.
pub fn read_validated_prefix_checkpoint_bytes_fixture(
    path: &Path,
) -> Result<Option<Vec<u8>>, StorageError> {
    let database = Database::open(path).map_err(database_error)?;
    let transaction = database.begin_read().map_err(transaction_error)?;
    let meta = transaction.open_table(META).map_err(table_error)?;
    Ok(meta
        .get(META_VALIDATED_PREFIX_CHECKPOINT)
        .map_err(precommit_storage_error)?
        .map(|encoded| encoded.value().to_vec()))
}

/// Removes the retained validated-prefix checkpoint from a stopped database, so
/// its next open validates completely and may write a checkpoint covering the
/// current prefix. A bounded dirty open (ADR-0182) never writes one.
///
/// Absent from default builds; call only after the owning process stopped.
pub fn strip_validated_prefix_checkpoint_fixture(path: &Path) -> Result<(), StorageError> {
    let database = Database::create(path).map_err(database_error)?;
    let transaction = database.begin_write().map_err(transaction_error)?;
    transaction
        .open_table(META)
        .map_err(table_error)?
        .remove(META_VALIDATED_PREFIX_CHECKPOINT)
        .map_err(precommit_storage_error)?;
    transaction.commit().map_err(commit_error)?;
    Ok(())
}

/// Plants one projection frontier row no decoder accepts in the derived
/// sidecar (ADR-0240), creating the sidecar if needed. Startup does not read
/// sidecar frontiers, so the next online retention step is the first reader
/// and meets an unreadable fencing source.
///
/// Absent from default builds; call only after the owning process stopped.
#[doc(hidden)]
pub fn plant_unreadable_projection_frontier_fixture(path: &Path) -> Result<(), StorageError> {
    let database =
        Database::create(crate::store::derived_store_path(path)).map_err(database_error)?;
    let transaction = database.begin_write().map_err(transaction_error)?;
    transaction
        .open_table(crate::layout::PROJECTION_FRONTIER)
        .map_err(table_error)?
        .insert(b"unreadable".as_slice(), [0xff_u8; 3].as_slice())
        .map_err(precommit_storage_error)?;
    transaction.commit().map_err(commit_error)?;
    Ok(())
}

/// Constructs a frozen predecessor for V1 compatibility tests only. The normal
/// initialization port always creates current V2 source state.
#[doc(hidden)]
pub fn initialize_legacy_database_fixture(
    store: &mut crate::RedbStore,
    candidate: riffdb_types::DatabaseId,
) -> Result<riffdb_storage_api::DatabaseInitializationResult, StorageError> {
    store.initialize_legacy_fixture(candidate)
}

/// One sealed, retained journal extent as its verified footer describes it:
/// generation, frame count, and the first and last covered commit sequences.
#[doc(hidden)]
pub type SealedJournalExtentSummaryFixture = (u64, usize, Option<u64>, Option<u64>);

/// Fixture handle driving the existing validated source-reclamation owner.
/// It keeps its prior checkpoint observation; it edits no receipt or hold.
#[doc(hidden)]
pub struct SourceHistoryReclamationFixture(crate::store::ReplicationSourceControl);

/// Wait for the real extent sealer to finish; fixture synchronization only.
#[doc(hidden)]
pub fn join_extent_sealing_fixture(ports: &crate::RedbOperationalPorts) {
    ports.shared.join_journal_sealer();
}

impl SourceHistoryReclamationFixture {
    /// Attempt the existing bounded source-reclamation protocol.
    pub fn step(&mut self) -> Result<bool, riffdb_storage_api::ChangelogCursorErrorV3> {
        self.0.reclaim_history()
    }
}

/// Construct a fixture-only source-reclamation owner over a live primary.
#[doc(hidden)]
pub fn source_history_reclamation_fixture(
    ports: &crate::RedbOperationalPorts,
) -> SourceHistoryReclamationFixture {
    SourceHistoryReclamationFixture(ports.replication_source_control())
}

/// Lists the sealed journal extents beside `database_path`, in generation
/// order, verifying each footer against the frames it indexes (ADR-0263).
#[doc(hidden)]
pub fn sealed_journal_extents_fixture(
    database_path: &Path,
) -> Result<Vec<SealedJournalExtentSummaryFixture>, StorageError> {
    let directory = database_path
        .parent()
        .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?;
    let prefix = format!(
        "{}.riffjournal.sealed.",
        database_path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| storage_error(StorageErrorKind::InvariantViolation))?
    );
    let database_id = {
        let database = Database::open(database_path).map_err(database_error)?;
        let transaction = database.begin_read().map_err(transaction_error)?;
        crate::store::read_identity_from_read_transaction(&transaction)?
    };
    let mut paths = std::fs::read_dir(directory)
        .map_err(|_| storage_error(StorageErrorKind::Unavailable))?
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().starts_with(&prefix))
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
    paths.sort();
    paths
        .iter()
        .map(|path| {
            let footer = crate::journal::read_sealed_extent_with_media(
                &crate::media::RealJournalMedia,
                path,
                database_id,
            )
            .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
            let first = footer
                .entries
                .iter()
                .find_map(|entry| entry.covered_sequence)
                .map(riffdb_types::CommitSequence::get);
            let last = footer
                .entries
                .iter()
                .rev()
                .find_map(|entry| entry.covered_sequence)
                .map(riffdb_types::CommitSequence::get);
            Ok((footer.generation, footer.entries.len(), first, last))
        })
        .collect()
}

/// COMMITS and HISTORY rows by representation: history stubs (ADR-0263
/// Amendment 3) and full values. Each pair is `(stubs, full values)`.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HistoryStubCensusFixture {
    pub commits: (usize, usize),
    pub history: (usize, usize),
    /// Full HISTORY rows attributed to a journaled group: receipts a journal
    /// frame could have re-derived but whose bytes were written.
    pub journaled_full_receipts: usize,
}

/// Counts the COMMITS and HISTORY rows of a closed database by representation,
/// decoding every stub so a malformed one fails the census.
#[doc(hidden)]
pub fn history_stub_census_fixture(
    database_path: &Path,
) -> Result<HistoryStubCensusFixture, StorageError> {
    let database = Database::open(database_path).map_err(database_error)?;
    let transaction = database.begin_read().map_err(transaction_error)?;
    let mut census = HistoryStubCensusFixture::default();
    for (definition, counts) in [
        (COMMITS, &mut census.commits),
        (crate::changelog_v3_activation::HISTORY, &mut census.history),
    ] {
        let table = match transaction.open_table(definition) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => continue,
            Err(error) => return Err(table_error(error)),
        };
        for row in table.iter().map_err(precommit_storage_error)? {
            let (_, value) = row.map_err(precommit_storage_error)?;
            if crate::history_stub::HistoryStub::decode(value.value())?.is_some() {
                counts.0 += 1;
            } else {
                counts.1 += 1;
            }
        }
    }
    if let Ok(table) = transaction.open_table(crate::changelog_v3_activation::HISTORY) {
        for row in table.iter().map_err(precommit_storage_error)? {
            let (_, value) = row.map_err(precommit_storage_error)?;
            if crate::history_stub::HistoryStub::decode(value.value())?.is_some() {
                continue;
            }
            let receipt = riffdb_storage_api::AuthoritativeTransactionV3::decode(value.value())
                .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
            census.journaled_full_receipts += usize::from(matches!(
                receipt.attribution(),
                riffdb_storage_api::ChangelogAttributionV3::JournaledApplicationGroup
                    | riffdb_storage_api::ChangelogAttributionV3::JournaledServiceAudit
            ));
        }
    }
    Ok(census)
}

/// Rewrites every history stub of a closed database as its full value, as a
/// staged maintenance restore does (ADR-0263 Amendment 1).
#[doc(hidden)]
pub fn hydrate_history_stubs_fixture(database_path: &Path) -> Result<(), StorageError> {
    let database = Database::open(database_path).map_err(database_error)?;
    let database_id = {
        let transaction = database.begin_read().map_err(transaction_error)?;
        crate::store::read_identity_from_read_transaction(&transaction)?
    };
    let _resolver = crate::history_stub::HistoryResolver::register(
        database_id,
        database_path,
        std::sync::Arc::new(crate::media::RealJournalMedia),
    );
    crate::history_stub::hydrate_history_stubs(&database)
}

/// Resolves one COMMITS or HISTORY value read from a closed database: a
/// history stub becomes the exact value it names, from the extents beside
/// `database_path`; any other value is returned unchanged.
#[doc(hidden)]
pub fn resolve_history_row_fixture(
    database_path: &Path,
    value: &[u8],
) -> Result<Vec<u8>, StorageError> {
    let Some(stub) = crate::history_stub::HistoryStub::decode(value)? else {
        return Ok(value.to_vec());
    };
    let _resolver = crate::history_stub::HistoryResolver::register(
        stub.database_id,
        database_path,
        std::sync::Arc::new(crate::media::RealJournalMedia),
    );
    match stub.kind {
        crate::history_stub::StubKind::Segment => {
            crate::history_stub::resolve_commits_value(value).map(std::borrow::Cow::into_owned)
        }
        crate::history_stub::StubKind::Receipt => {
            crate::history_stub::resolve_history_value(value).map(std::borrow::Cow::into_owned)
        }
    }
}

/// The published overlay's COMMITS rows by representation, `(stubs, full
/// values)`, and its charged bytes (ADR-0263 Decision 4).
#[doc(hidden)]
pub fn overlay_commits_fixture(
    ports: &crate::store::RedbOperationalPorts,
) -> Result<((usize, usize), usize), StorageError> {
    let Some(view) = ports.shared.published_composite_view()? else {
        return Ok(((0, 0), 0));
    };
    let overlay = view.overlay();
    let page = overlay
        .merge_bounded(
            riffdb_storage_api::CompositeTableV1::Commits,
            std::iter::empty(),
            &[],
            None,
            usize::MAX,
            usize::MAX,
        )
        .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
    let mut counts = (0, 0);
    for (_, value) in page.rows() {
        if crate::history_stub::HistoryStub::decode(value)?.is_some() {
            counts.0 += 1;
        } else {
            counts.1 += 1;
        }
    }
    Ok((counts, overlay.charged_bytes()))
}

/// Removes the footer of the sealed extent of `generation` beside a closed
/// database and moves it to `name`, the journal-suffixed name a crash
/// window leaves it at (such as `checkpoint` or `sealing`).
#[doc(hidden)]
pub fn unseal_journal_extent_fixture(
    database_path: &Path,
    generation: u64,
    name: &str,
) -> Result<(), StorageError> {
    let database_id = {
        let database = Database::open(database_path).map_err(database_error)?;
        let transaction = database.begin_read().map_err(transaction_error)?;
        crate::store::read_identity_from_read_transaction(&transaction)?
    };
    let sealed = crate::journal::sealed_journal_path(database_path, generation);
    crate::journal::unseal_extent_with_media(&crate::media::RealJournalMedia, &sealed, database_id)
        .map_err(|_| storage_error(StorageErrorKind::CorruptData))?;
    let mut target = database_path.as_os_str().to_os_string();
    target.push(format!(".riffjournal.{name}"));
    std::fs::rename(&sealed, target).map_err(|_| storage_error(StorageErrorKind::Unavailable))
}
