//! Exclusive read-only startup evidence over one immutable redb snapshot.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use redb::{
    MultimapTableHandle, ReadTransaction, ReadableDatabase, ReadableTable, ReadableTableMetadata,
    TableDefinition, TableHandle,
};
use riffdb_storage_api::{
    ApplicationSequenceAllocator, CapabilityLifecycleV1, DormantPortBundle, EvidencePageLimit,
    HistoricalActiveCatalogEvidence, HistoricalBundleBytes, HistoricalBundleEvidence,
    HistoricalEvidenceCursor, HistoricalEvidenceEnd, HistoricalEvidencePage,
    HistoricalPersistedKeyEvidenceV1, HistoricalSemanticEvidence, OpenSessionId, ReadableDigestKey,
    StartupValidationInputs, StorageError, StorageErrorKind, StorageValueError,
    StructuralEvidenceCursor, StructuralEvidenceEnd, StructuralEvidenceOpen,
    StructuralEvidencePage, StructuralEvidenceSession, StructuralFinding, StructuralFindingCode,
    StructuralFindingScope, StructurallyOpened,
};
use riffdb_types::{
    CommitSequence, ContractBundleHash, ContractLineage, ContractVersion, DatabaseId,
    FrontierPosition, hash_contract_bundle,
};

use crate::codec::{self, IdempotencyRecordV1};
use crate::error::{precommit_storage_error, storage_error, table_error, transaction_error};
use crate::gate::ExclusiveLease;
use crate::keys;
use crate::layout::{
    AUDIT, CAPABILITIES, CAPABILITY_TOKENS, CATALOG_ACTIVE, CATALOG_ACTIVE_KEY, COMMITS,
    CONTRACT_BUNDLES, ENTITIES, EVENTS, IDEMPOTENCY, IDEMPOTENCY_PENDING, INDEX_EPOCHS, META,
    META_ADMINISTRATION_SEQUENCE, META_APPLICATION_SEQUENCE, META_CAPABILITY_BOOTSTRAP,
    META_DATABASE_ID, META_FORMAT_VERSION, META_KEYS, OUTBOX, OUTBOX_STATUS, PROJECTION_APPLIED,
    PROJECTION_FRONTIER, PROJECTION_STATE, PROVENANCE, SECONDARY_INDEXES, TABLE_NAMES,
};
use crate::store::{RedbDormantPorts, RedbStore, SharedRedb};

static NEXT_OPEN_SESSION: AtomicU64 = AtomicU64::new(1);
const STRUCTURAL_TABLE_COUNT: usize = 19;

/// Redb authority whose constructor is private to a completed startup session.
pub struct RedbCompletionAuthority {
    _private: (),
}

/// Unforgeable exact-end token for the redb structural stream.
#[derive(Debug)]
pub struct RedbStructuralEvidenceEnd {
    cursor: StructuralEvidenceCursor,
}

/// Unforgeable exact-end token for the redb historical stream.
#[derive(Debug)]
pub struct RedbHistoricalEvidenceEnd {
    cursor: HistoricalEvidenceCursor,
}

/// One exclusive startup session bound to a single immutable redb snapshot.
pub struct RedbStructuralEvidenceSession {
    shared: Arc<SharedRedb>,
    transaction: Option<ReadTransaction>,
    lease: Option<ExclusiveLease>,
    database_id: DatabaseId,
    open_session_id: OpenSessionId,
    inputs: StartupValidationInputs,
    structural_counts: [u64; STRUCTURAL_TABLE_COUNT],
    structural_total: u64,
    next_structural: StructuralEvidenceCursor,
    next_historical: HistoricalEvidenceCursor,
    last_historical_key: Option<Vec<u8>>,
    structural_finished: bool,
    historical_finished: bool,
    authoritative_finding_seen: bool,
}

impl fmt::Debug for RedbStructuralEvidenceSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RedbStructuralEvidenceSession")
            .field("database_id", &self.database_id)
            .field("open_session_id", &self.open_session_id)
            .field("state", &"[EXCLUSIVE READ SNAPSHOT]")
            .finish()
    }
}

impl DormantPortBundle for RedbDormantPorts {
    type CompletionAuthority = RedbCompletionAuthority;
}

impl StructuralEvidenceEnd for RedbStructuralEvidenceEnd {
    fn cursor(&self) -> StructuralEvidenceCursor {
        self.cursor
    }
}

impl HistoricalEvidenceEnd for RedbHistoricalEvidenceEnd {
    fn cursor(&self) -> HistoricalEvidenceCursor {
        self.cursor
    }
}

impl StructuralEvidenceOpen for RedbStore {
    type Session = RedbStructuralEvidenceSession;

    fn begin_structural_evidence(
        self,
        inputs: StartupValidationInputs,
    ) -> Result<Self::Session, StorageError> {
        let lease = self.acquire_mutation_lease()?;
        let transaction = self
            .shared
            .database
            .begin_read()
            .map_err(transaction_error)?;
        let (database_id, structural_counts, structural_total) =
            collect_startup_snapshot(&transaction)?;
        let open_session_id = allocate_open_session()?;
        Ok(Self::Session {
            shared: Arc::clone(&self.shared),
            transaction: Some(transaction),
            lease: Some(lease),
            database_id,
            open_session_id,
            inputs,
            structural_counts,
            structural_total,
            next_structural: StructuralEvidenceCursor::start(database_id, open_session_id),
            next_historical: HistoricalEvidenceCursor::start(database_id, open_session_id),
            last_historical_key: None,
            structural_finished: false,
            historical_finished: false,
            authoritative_finding_seen: false,
        })
    }
}

impl StructuralEvidenceSession for RedbStructuralEvidenceSession {
    type DormantPorts = RedbDormantPorts;
    type StructuralEnd = RedbStructuralEvidenceEnd;
    type HistoricalEnd = RedbHistoricalEvidenceEnd;

    fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    fn open_session_id(&self) -> OpenSessionId {
        self.open_session_id
    }

    fn read_structural_evidence(
        &mut self,
        cursor: StructuralEvidenceCursor,
        limit: EvidencePageLimit,
    ) -> Result<StructuralEvidencePage<Self::StructuralEnd>, StorageError> {
        if self.structural_finished || cursor != self.next_structural {
            return Err(invariant());
        }
        if cursor.position() > self.structural_total {
            return Err(invariant());
        }
        if cursor.position() == self.structural_total {
            self.structural_finished = true;
            return Ok(StructuralEvidencePage::ExactEnd(
                RedbStructuralEvidenceEnd { cursor },
            ));
        }

        let finding_bound = u64::try_from(riffdb_storage_api::MAX_INTEGRITY_FINDINGS)
            .map_err(|_| limit_exceeded())?;
        let inspected = (self.structural_total - cursor.position())
            .min(u64::from(limit.get()))
            .min(finding_bound);
        let mut findings = Vec::new();
        for offset in 0..inspected {
            let position = cursor
                .position()
                .checked_add(offset)
                .ok_or_else(limit_exceeded)?;
            if let Some(finding) = inspect_structural_item(
                self.transaction()?,
                &self.inputs,
                self.database_id,
                &self.structural_counts,
                position,
            )? {
                if finding.scope() == StructuralFindingScope::Authoritative {
                    self.authoritative_finding_seen = true;
                }
                findings.push(finding);
            }
        }
        let next = cursor.advanced(inspected).map_err(value_error_as_storage)?;
        let page =
            StructuralEvidencePage::page(cursor, findings, next).map_err(value_error_as_storage)?;
        self.next_structural = next;
        Ok(page)
    }

    fn read_historical_evidence(
        &mut self,
        cursor: HistoricalEvidenceCursor,
        limit: EvidencePageLimit,
    ) -> Result<HistoricalEvidencePage<Self::HistoricalEnd>, StorageError> {
        if self.historical_finished || cursor != self.next_historical {
            return Err(invariant());
        }
        let requested = usize::try_from(limit.get()).map_err(|_| limit_exceeded())?;
        let mut evidence = Vec::new();
        let mut bytes = 0usize;
        let mut last_key = self.last_historical_key.clone();
        let mut exhausted = false;
        while evidence.len() < requested {
            let Some(candidate) = select_next_historical(self.transaction()?, last_key.as_deref())?
            else {
                exhausted = true;
                break;
            };
            let next_bytes = bytes
                .checked_add(historical_semantic_bytes(&candidate.evidence)?)
                .ok_or_else(limit_exceeded)?;
            if next_bytes > riffdb_storage_api::MAX_HISTORICAL_EVIDENCE_PAGE_BYTES {
                if evidence.is_empty() {
                    return Err(limit_exceeded());
                }
                break;
            }
            bytes = next_bytes;
            last_key = Some(candidate.key);
            evidence.push(candidate.evidence);
        }
        if evidence.is_empty() {
            if !exhausted {
                return Err(invariant());
            }
            self.historical_finished = true;
            return Ok(HistoricalEvidencePage::ExactEnd(
                RedbHistoricalEvidenceEnd { cursor },
            ));
        }
        let count = u64::try_from(evidence.len()).map_err(|_| limit_exceeded())?;
        let next = cursor.advanced(count).map_err(value_error_as_storage)?;
        let page =
            HistoricalEvidencePage::page(cursor, evidence, next).map_err(value_error_as_storage)?;
        self.last_historical_key = last_key;
        self.next_historical = next;
        Ok(page)
    }

    fn read_historical_bundle(
        &mut self,
        lineage: &ContractLineage,
        version: ContractVersion,
        hash: ContractBundleHash,
    ) -> Result<Option<HistoricalBundleEvidence>, StorageError> {
        read_historical_bundle(self.transaction()?, lineage, version, hash)
    }

    fn finish(
        mut self,
        structural_end: Self::StructuralEnd,
        historical_end: Self::HistoricalEnd,
    ) -> Result<StructurallyOpened<Self::DormantPorts>, StorageError> {
        if !self.structural_finished
            || !self.historical_finished
            || structural_end.cursor != self.next_structural
            || historical_end.cursor != self.next_historical
        {
            return Err(invariant());
        }
        if self.authoritative_finding_seen {
            return Err(storage_error(StorageErrorKind::CorruptData));
        }
        drop(self.transaction.take());
        drop(self.lease.take());
        Ok(StructurallyOpened::from_finished_session(
            self.database_id,
            self.open_session_id,
            RedbDormantPorts {
                shared: Arc::clone(&self.shared),
            },
            RedbCompletionAuthority { _private: () },
        ))
    }
}

impl RedbStructuralEvidenceSession {
    fn transaction(&self) -> Result<&ReadTransaction, StorageError> {
        self.transaction.as_ref().ok_or_else(invariant)
    }
}

fn allocate_open_session() -> Result<OpenSessionId, StorageError> {
    let value = NEXT_OPEN_SESSION
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            current.checked_add(1)
        })
        .map_err(|_| storage_error(StorageErrorKind::SequenceExhausted))?;
    OpenSessionId::new(value).ok_or_else(|| storage_error(StorageErrorKind::SequenceExhausted))
}

fn collect_startup_snapshot(
    transaction: &ReadTransaction,
) -> Result<(DatabaseId, [u64; STRUCTURAL_TABLE_COUNT], u64), StorageError> {
    validate_table_inventory(transaction)?;
    let meta = transaction.open_table(META).map_err(table_error)?;
    let identity = meta
        .get(META_DATABASE_ID)
        .map_err(precommit_storage_error)?
        .ok_or_else(|| storage_error(StorageErrorKind::CorruptData))?;
    let database_id = *codec::decode_database_identity_v1(identity.value())?.value();
    let counts = [
        meta.len().map_err(precommit_storage_error)?,
        table_len(transaction, CONTRACT_BUNDLES)?,
        table_len(transaction, CATALOG_ACTIVE)?,
        table_len(transaction, ENTITIES)?,
        table_len(transaction, SECONDARY_INDEXES)?,
        table_len(transaction, INDEX_EPOCHS)?,
        table_len(transaction, IDEMPOTENCY)?,
        table_len(transaction, IDEMPOTENCY_PENDING)?,
        table_len(transaction, COMMITS)?,
        table_len(transaction, PROVENANCE)?,
        table_len(transaction, EVENTS)?,
        table_len(transaction, OUTBOX)?,
        table_len(transaction, OUTBOX_STATUS)?,
        table_len(transaction, PROJECTION_STATE)?,
        table_len(transaction, PROJECTION_FRONTIER)?,
        table_len(transaction, PROJECTION_APPLIED)?,
        table_len(transaction, CAPABILITIES)?,
        table_len(transaction, CAPABILITY_TOKENS)?,
        table_len(transaction, AUDIT)?,
    ];
    let total = counts.iter().try_fold(1u64, |total, count| {
        total.checked_add(*count).ok_or_else(limit_exceeded)
    })?;
    Ok((database_id, counts, total))
}

fn validate_table_inventory(transaction: &ReadTransaction) -> Result<(), StorageError> {
    let tables = transaction
        .list_tables()
        .map_err(precommit_storage_error)?
        .map(|table| table.name().to_owned())
        .collect::<BTreeSet<_>>();
    let multimaps = transaction
        .list_multimap_tables()
        .map_err(precommit_storage_error)?
        .map(|table| table.name().to_owned())
        .collect::<BTreeSet<_>>();
    if !multimaps.is_empty() {
        return Err(storage_error(StorageErrorKind::IncompatibleFormat));
    }
    let expected = TABLE_NAMES
        .iter()
        .map(|name| (*name).to_owned())
        .collect::<BTreeSet<_>>();
    if tables == expected {
        Ok(())
    } else if tables.iter().any(|name| !expected.contains(name)) {
        Err(storage_error(StorageErrorKind::IncompatibleFormat))
    } else {
        Err(storage_error(StorageErrorKind::CorruptData))
    }
}

fn table_len(
    transaction: &ReadTransaction,
    definition: TableDefinition<&'static [u8], &'static [u8]>,
) -> Result<u64, StorageError> {
    transaction
        .open_table(definition)
        .map_err(table_error)?
        .len()
        .map_err(precommit_storage_error)
}

fn inspect_structural_item(
    transaction: &ReadTransaction,
    inputs: &StartupValidationInputs,
    database_id: DatabaseId,
    counts: &[u64; STRUCTURAL_TABLE_COUNT],
    position: u64,
) -> Result<Option<StructuralFinding>, StorageError> {
    if position == 0 {
        return inspect_header(transaction, database_id);
    }
    let mut relative = position - 1;
    for (phase, count) in counts.iter().copied().enumerate() {
        if relative < count {
            return inspect_table_row(transaction, inputs, database_id, phase, relative);
        }
        relative = relative.checked_sub(count).ok_or_else(invariant)?;
    }
    Err(invariant())
}

fn nth_meta_entry(
    transaction: &ReadTransaction,
    index: u64,
) -> Result<(String, Vec<u8>), StorageError> {
    let table = transaction.open_table(META).map_err(table_error)?;
    let mut iterator = table.iter().map_err(precommit_storage_error)?;
    let mut current = 0u64;
    for entry in &mut iterator {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        if current == index {
            return Ok((key.value().to_owned(), value.value().to_vec()));
        }
        current = current.checked_add(1).ok_or_else(limit_exceeded)?;
    }
    Err(invariant())
}

fn nth_bytes_entry(
    transaction: &ReadTransaction,
    definition: TableDefinition<&'static [u8], &'static [u8]>,
    index: u64,
) -> Result<(Vec<u8>, Vec<u8>), StorageError> {
    let table = transaction.open_table(definition).map_err(table_error)?;
    let mut iterator = table.iter().map_err(precommit_storage_error)?;
    let mut current = 0u64;
    for entry in &mut iterator {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        if current == index {
            return Ok((key.value().to_vec(), value.value().to_vec()));
        }
        current = current.checked_add(1).ok_or_else(limit_exceeded)?;
    }
    Err(invariant())
}

struct HistoricalCandidate {
    key: Vec<u8>,
    evidence: HistoricalSemanticEvidence,
}

fn inspect_table_row(
    transaction: &ReadTransaction,
    inputs: &StartupValidationInputs,
    database_id: DatabaseId,
    phase: usize,
    index: u64,
) -> Result<Option<StructuralFinding>, StorageError> {
    if phase == 0 {
        let (key, value) = nth_meta_entry(transaction, index)?;
        return Ok(inspect_meta_row(&key, &value, database_id));
    }
    let definition = match phase {
        1 => CONTRACT_BUNDLES,
        2 => CATALOG_ACTIVE,
        3 => ENTITIES,
        4 => SECONDARY_INDEXES,
        5 => INDEX_EPOCHS,
        6 => IDEMPOTENCY,
        7 => IDEMPOTENCY_PENDING,
        8 => COMMITS,
        9 => PROVENANCE,
        10 => EVENTS,
        11 => OUTBOX,
        12 => OUTBOX_STATUS,
        13 => PROJECTION_STATE,
        14 => PROJECTION_FRONTIER,
        15 => PROJECTION_APPLIED,
        16 => CAPABILITIES,
        17 => CAPABILITY_TOKENS,
        18 => AUDIT,
        _ => return Err(invariant()),
    };
    let (key, value) = nth_bytes_entry(transaction, definition, index)?;
    match phase {
        1 => inspect_bundle_row(&key, &value),
        2 => inspect_active_row(transaction, &key, &value),
        3 => inspect_entity_row(transaction, &key, &value),
        4 => inspect_index_row(transaction, &key, &value),
        5 => inspect_epoch_row(transaction, &key, &value),
        6 => inspect_terminal_row(transaction, inputs, database_id, &key, &value),
        7 => inspect_pending_row(transaction, inputs, database_id, &key, &value),
        8 => inspect_commit_row(transaction, index, &key, &value),
        9 => inspect_provenance_row(transaction, &key, &value),
        10 => inspect_event_row(transaction, &key, &value),
        11 => inspect_outbox_row(transaction, &key, &value),
        12 => inspect_outbox_status_row(transaction, &key, &value),
        13 => inspect_projection_state_row(transaction, &key, &value),
        14 => inspect_projection_control_row(transaction, &key, &value),
        15 => inspect_projection_apply_row(transaction, &key, &value),
        16 => inspect_capability_row(transaction, inputs, database_id, &key, &value),
        17 => inspect_capability_lookup_row(transaction, &key, &value),
        18 => inspect_audit_row(transaction, index, &key, &value),
        _ => Err(invariant()),
    }
}

fn inspect_header(
    transaction: &ReadTransaction,
    database_id: DatabaseId,
) -> Result<Option<StructuralFinding>, StorageError> {
    let meta = transaction.open_table(META).map_err(table_error)?;
    let mut seen = BTreeSet::new();
    for entry in meta.iter().map_err(precommit_storage_error)? {
        let (key, _) = entry.map_err(precommit_storage_error)?;
        if !META_KEYS.contains(&key.value()) || !seen.insert(key.value().to_owned()) {
            return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
        }
    }
    if [
        META_FORMAT_VERSION,
        META_DATABASE_ID,
        META_APPLICATION_SEQUENCE,
        META_ADMINISTRATION_SEQUENCE,
    ]
    .iter()
    .any(|key| !seen.contains(*key))
    {
        return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
    }
    let application = match meta_decode(
        &meta,
        META_APPLICATION_SEQUENCE,
        codec::decode_application_sequence_allocator_v1,
    )? {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    let administration = match meta_decode(
        &meta,
        META_ADMINISTRATION_SEQUENCE,
        codec::decode_administration_sequence_allocator_v1,
    )? {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    if !application_allocator_matches(transaction, application)?
        || !administration_allocator_matches(transaction, administration)?
    {
        return Ok(Some(authoritative(
            StructuralFindingCode::SequenceDiscontinuity,
        )));
    }
    let active = match read_active_pointer(transaction)? {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    if let Some(active) = &active
        && !bundle_pointer_exists(transaction, active)?
    {
        return Ok(Some(authoritative(StructuralFindingCode::MissingCrossLink)));
    }
    if active.is_none()
        && (table_len(transaction, CONTRACT_BUNDLES)? != 0
            || has_application_authoritative_state(transaction)?)
    {
        return Ok(Some(authoritative(StructuralFindingCode::MissingCrossLink)));
    }
    if let Some(value) = meta
        .get(META_CAPABILITY_BOOTSTRAP)
        .map_err(precommit_storage_error)?
    {
        let marker = match decoded(codec::decode_capability_bootstrap_marker_v1(value.value())) {
            Ok(value) => value,
            Err(code) => return Ok(Some(authoritative(code))),
        };
        if marker.database_id() != database_id || !capability_marker_exists(transaction, marker)? {
            return Ok(Some(authoritative(
                StructuralFindingCode::CrossLinkMismatch,
            )));
        }
    } else if table_len(transaction, CAPABILITIES)? != 0 {
        return Ok(Some(authoritative(StructuralFindingCode::MissingCrossLink)));
    }
    Ok(None)
}

fn inspect_meta_row(key: &str, value: &[u8], database_id: DatabaseId) -> Option<StructuralFinding> {
    let valid = match key {
        META_FORMAT_VERSION => decoded(codec::decode_storage_format_version_v1(value))
            .is_ok_and(|version| version == riffdb_storage_api::StorageFormatVersion::V1),
        META_DATABASE_ID => decoded(codec::decode_database_identity_v1(value))
            .is_ok_and(|identity| identity == database_id),
        META_APPLICATION_SEQUENCE => {
            decoded(codec::decode_application_sequence_allocator_v1(value)).is_ok()
        }
        META_ADMINISTRATION_SEQUENCE => {
            decoded(codec::decode_administration_sequence_allocator_v1(value)).is_ok()
        }
        META_CAPABILITY_BOOTSTRAP => {
            decoded(codec::decode_capability_bootstrap_marker_v1(value)).is_ok()
        }
        _ => false,
    };
    (!valid).then(|| authoritative(StructuralFindingCode::MalformedRecord))
}

fn inspect_bundle_row(key: &[u8], value: &[u8]) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok((lineage, version)) = keys::decode_contract_bundle_key(key) else {
        return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
    };
    let bundle = match decoded(codec::decode_contract_bundle_v1(value)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    if bundle.lineage() != &lineage
        || bundle.contract_version() != version
        || hash_contract_bundle(bundle.canonical_bytes()) != bundle.bundle_hash()
    {
        return Ok(Some(authoritative(
            StructuralFindingCode::CrossLinkMismatch,
        )));
    }
    Ok(None)
}

fn inspect_active_row(
    transaction: &ReadTransaction,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    if keys::decode_singleton_key(key).is_err() {
        return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
    }
    let active = match decoded(codec::decode_active_catalog_pointer_v1(value)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    Ok((!bundle_pointer_exists(transaction, &active)?)
        .then(|| authoritative(StructuralFindingCode::MissingCrossLink)))
}

fn inspect_entity_row(
    transaction: &ReadTransaction,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok(key) = keys::decode_entity_key(key) else {
        return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
    };
    let record = match decoded(codec::decode_entity_record_v1(value)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    Ok((record.target().key() != &key
        || !binding_bundle_exists(transaction, record.schema_binding())?)
    .then(|| authoritative(StructuralFindingCode::MissingCrossLink)))
}

fn inspect_index_row(
    transaction: &ReadTransaction,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok(key) = keys::decode_index_entry_key(key) else {
        return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
    };
    let record = match decoded(codec::decode_index_entry_v1(value)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    Ok(
        (record.key() != &key || !binding_bundle_exists(transaction, record.schema_binding())?)
            .then(|| authoritative(StructuralFindingCode::MissingCrossLink)),
    )
}

fn inspect_epoch_row(
    transaction: &ReadTransaction,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok(key) = keys::decode_index_range_prefix_key(key) else {
        return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
    };
    let record = match decoded(codec::decode_index_epoch_v1(value)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    Ok(
        (record.target() != &key || !binding_bundle_exists(transaction, record.schema_binding())?)
            .then(|| authoritative(StructuralFindingCode::MissingCrossLink)),
    )
}

fn inspect_terminal_row(
    transaction: &ReadTransaction,
    inputs: &StartupValidationInputs,
    database_id: DatabaseId,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok(physical) = keys::decode_idempotency_key(key) else {
        return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
    };
    let record = match decoded(codec::decode_idempotency_record_v1(value)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    let (identity, plan) = match &record {
        IdempotencyRecordV1::StoredOutcome(value) => (value.identity(), value.plan()),
        IdempotencyRecordV1::ExecutionFailed(value) => {
            (value.pending().identity(), value.pending().plan())
        }
    };
    if identity.storage_key().ok().as_ref() != Some(&physical)
        || identity.database_id() != database_id
    {
        return Ok(Some(authoritative(
            StructuralFindingCode::CrossLinkMismatch,
        )));
    }
    if !idempotency_digest_is_readable(inputs, identity.caller_key_digest()) {
        return Ok(Some(authoritative(
            StructuralFindingCode::DigestUnavailable,
        )));
    }
    if !plan_bundle_exists(transaction, plan)? || raw_exists(transaction, IDEMPOTENCY_PENDING, key)?
    {
        return Ok(Some(authoritative(StructuralFindingCode::MissingCrossLink)));
    }
    if let IdempotencyRecordV1::StoredOutcome(outcome) = &record
        && !outcome_graph_is_reciprocal(transaction, outcome)?
    {
        return Ok(Some(authoritative(
            StructuralFindingCode::CrossLinkMismatch,
        )));
    }
    Ok(None)
}

fn inspect_pending_row(
    transaction: &ReadTransaction,
    inputs: &StartupValidationInputs,
    database_id: DatabaseId,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok(physical) = keys::decode_idempotency_key(key) else {
        return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
    };
    let pending = match decoded(codec::decode_pending_admission_v1(value)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    if pending.identity().storage_key().ok().as_ref() != Some(&physical)
        || pending.identity().database_id() != database_id
    {
        return Ok(Some(authoritative(
            StructuralFindingCode::CrossLinkMismatch,
        )));
    }
    if !idempotency_digest_is_readable(inputs, pending.identity().caller_key_digest()) {
        return Ok(Some(authoritative(
            StructuralFindingCode::DigestUnavailable,
        )));
    }
    Ok((!plan_bundle_exists(transaction, pending.plan())?
        || raw_exists(transaction, IDEMPOTENCY, key)?)
    .then(|| authoritative(StructuralFindingCode::MissingCrossLink)))
}

fn inspect_commit_row(
    transaction: &ReadTransaction,
    index: u64,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok(sequence) = keys::decode_application_sequence_key(key) else {
        return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
    };
    let record = match decoded(codec::decode_commit_record_v1(value)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    if index.checked_add(1).and_then(CommitSequence::new) != Some(sequence)
        || record.commit_sequence() != sequence
    {
        return Ok(Some(authoritative(
            StructuralFindingCode::SequenceDiscontinuity,
        )));
    }
    Ok((!plan_bundle_exists(transaction, record.plan())?
        || !commit_graph_is_reciprocal(transaction, &record)?)
    .then(|| authoritative(StructuralFindingCode::MissingCrossLink)))
}

fn inspect_provenance_row(
    transaction: &ReadTransaction,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok(id) = keys::decode_provenance_key(key) else {
        return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
    };
    let record = match decoded(codec::decode_provenance_record_v1(value)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    Ok((record.provenance_id() != id
        || !plan_bundle_exists(transaction, record.plan())?
        || !provenance_graph_is_reciprocal(transaction, &record)?)
    .then(|| authoritative(StructuralFindingCode::CrossLinkMismatch)))
}

fn inspect_event_row(
    transaction: &ReadTransaction,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok(id) = keys::decode_event_key(key) else {
        return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
    };
    let event = match decoded(codec::decode_durable_event_v1(value)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    Ok(
        (event.event_id() != id || !event_graph_is_reciprocal(transaction, &event)?)
            .then(|| authoritative(StructuralFindingCode::CrossLinkMismatch)),
    )
}

fn inspect_outbox_row(
    transaction: &ReadTransaction,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok(id) = keys::decode_event_key(key) else {
        return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
    };
    let intent = match decoded(codec::decode_outbox_intent_v1(value)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    Ok(
        (intent.event_id() != id || !event_graph_is_reciprocal(transaction, intent.event())?)
            .then(|| authoritative(StructuralFindingCode::CrossLinkMismatch)),
    )
}

fn inspect_outbox_status_row(
    transaction: &ReadTransaction,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok(id) = keys::decode_event_key(key) else {
        return Ok(Some(derived_outbox(StructuralFindingCode::MalformedRecord)));
    };
    let status = match decoded(codec::decode_outbox_status_v1(value)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(derived_outbox(code))),
    };
    Ok(
        (status.event_id() != id || !raw_exists(transaction, OUTBOX, key)?)
            .then(|| derived_outbox(StructuralFindingCode::OrphanedOutboxStatus)),
    )
}

fn inspect_projection_state_row(
    transaction: &ReadTransaction,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok(key) = keys::decode_projection_group_key(key) else {
        return Ok(Some(derived_projection(
            StructuralFindingCode::MalformedRecord,
        )));
    };
    let state = match decoded(codec::decode_projection_state_structural_v1(value)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(derived_projection(code))),
    };
    if state.key() != &key || !projection_control_exists(transaction, state.identity())? {
        return Ok(Some(derived_projection(
            StructuralFindingCode::ProjectionStateMismatch,
        )));
    }
    missing_projection_source_finding(transaction, state.last_changed_sequence())
}

fn inspect_projection_control_row(
    transaction: &ReadTransaction,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok(key) = keys::decode_projection_frontier_key(key) else {
        return Ok(Some(derived_projection(
            StructuralFindingCode::MalformedRecord,
        )));
    };
    let control = match decoded(codec::decode_projection_control_v1(value)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(derived_projection(code))),
    };
    if control.identity() != key.identity() {
        return Ok(Some(derived_projection(
            StructuralFindingCode::ProjectionStateMismatch,
        )));
    }
    for position in [control.published(), control.candidate()]
        .into_iter()
        .flatten()
    {
        if let FrontierPosition::AppliedThrough(sequence) = position.frontier()
            && let Some(finding) = missing_projection_source_finding(transaction, sequence)?
        {
            return Ok(Some(finding));
        }
    }
    Ok(None)
}

fn inspect_projection_apply_row(
    transaction: &ReadTransaction,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok(key) = keys::decode_projection_apply_key(key) else {
        return Ok(Some(derived_projection(
            StructuralFindingCode::MalformedRecord,
        )));
    };
    let marker = match decoded(codec::decode_projection_apply_v1(value)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(derived_projection(code))),
    };
    if marker.key() != &key {
        return Ok(Some(derived_projection(
            StructuralFindingCode::ProjectionStateMismatch,
        )));
    }
    missing_projection_source_finding(transaction, key.commit_sequence())
}

fn inspect_capability_row(
    transaction: &ReadTransaction,
    inputs: &StartupValidationInputs,
    database_id: DatabaseId,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok(id) = keys::decode_capability_key(key) else {
        return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
    };
    let capability = match decoded(codec::decode_capability_record_v1(value)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    if capability.capability_id() != id || capability.database_id() != database_id {
        return Ok(Some(authoritative(
            StructuralFindingCode::CrossLinkMismatch,
        )));
    }
    let lookup_key = keys::encode_capability_token_key(capability.token_digest());
    let lookup = get_decoded(
        transaction,
        CAPABILITY_TOKENS,
        &lookup_key,
        codec::decode_capability_token_lookup_v1,
    )?;
    if !matches!(lookup, Ok(Some(value)) if value.capability_id() == id) {
        return Ok(Some(authoritative(StructuralFindingCode::MissingCrossLink)));
    }
    if matches!(capability.lifecycle(), CapabilityLifecycleV1::Active)
        && inputs.authorization_time() < capability.expires_at()
        && !inputs
            .capability_digests()
            .as_slice()
            .contains(&ReadableDigestKey::v1(capability.token_digest().key_id()))
    {
        return Ok(Some(authoritative(
            StructuralFindingCode::DigestUnavailable,
        )));
    }
    Ok((!capability_audit_exists(transaction, &capability)?)
        .then(|| authoritative(StructuralFindingCode::MissingCrossLink)))
}

fn inspect_capability_lookup_row(
    transaction: &ReadTransaction,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok(digest) = keys::decode_capability_token_key(key) else {
        return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
    };
    let lookup = match decoded(codec::decode_capability_token_lookup_v1(value)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    let capability_key = keys::encode_capability_key(lookup.capability_id());
    let capability = get_decoded(
        transaction,
        CAPABILITIES,
        &capability_key,
        codec::decode_capability_record_v1,
    )?;
    Ok(
        (!matches!(capability, Ok(Some(value)) if value.token_digest() == digest))
            .then(|| authoritative(StructuralFindingCode::MissingCrossLink)),
    )
}

fn inspect_audit_row(
    transaction: &ReadTransaction,
    index: u64,
    key: &[u8],
    value: &[u8],
) -> Result<Option<StructuralFinding>, StorageError> {
    let Ok(sequence) = keys::decode_audit_key(key) else {
        return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
    };
    let record = match decoded(codec::decode_administration_audit_record_v1(value)) {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    if index
        .checked_add(1)
        .and_then(riffdb_types::AdministrationSequence::new)
        != Some(sequence)
        || record.administration_sequence() != sequence
    {
        return Ok(Some(authoritative(
            StructuralFindingCode::SequenceDiscontinuity,
        )));
    }
    let reciprocal = match &record {
        riffdb_storage_api::StoredAdministrationAuditRecordV1::Catalog(record) => {
            bundle_pointer_exists(transaction, record.activated())?
        }
        riffdb_storage_api::StoredAdministrationAuditRecordV1::Capability(record) => {
            let key = keys::encode_capability_key(record.target_capability_id());
            raw_exists(transaction, CAPABILITIES, &key)?
        }
        riffdb_storage_api::StoredAdministrationAuditRecordV1::Service(record) => {
            service_link_exists(transaction, record)?
        }
    };
    Ok((!reciprocal).then(|| authoritative(StructuralFindingCode::MissingCrossLink)))
}

fn select_next_historical(
    transaction: &ReadTransaction,
    after: Option<&[u8]>,
) -> Result<Option<HistoricalCandidate>, StorageError> {
    let mut selected = None;
    scan_bundle_candidates(transaction, after, &mut selected)?;
    scan_plan_candidates(transaction, after, &mut selected)?;
    scan_active_candidate(transaction, after, &mut selected)?;
    scan_persisted_key_candidates(transaction, after, &mut selected)?;
    Ok(selected)
}

fn consider_candidate(
    selected: &mut Option<HistoricalCandidate>,
    after: Option<&[u8]>,
    candidate: HistoricalCandidate,
) {
    if after.is_some_and(|after| candidate.key.as_slice() <= after) {
        return;
    }
    if selected
        .as_ref()
        .is_none_or(|current| candidate.key < current.key)
    {
        *selected = Some(candidate);
    }
}

fn scan_bundle_candidates(
    transaction: &ReadTransaction,
    after: Option<&[u8]>,
    selected: &mut Option<HistoricalCandidate>,
) -> Result<(), StorageError> {
    let table = transaction
        .open_table(CONTRACT_BUNDLES)
        .map_err(table_error)?;
    for entry in table.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let (lineage, version) =
            keys::decode_contract_bundle_key(key.value()).map_err(|_| corrupt())?;
        let bundle =
            decoded(codec::decode_contract_bundle_v1(value.value())).map_err(|_| corrupt())?;
        if bundle.lineage() != &lineage
            || bundle.contract_version() != version
            || hash_contract_bundle(bundle.canonical_bytes()) != bundle.bundle_hash()
        {
            return Err(corrupt());
        }
        let evidence = HistoricalSemanticEvidence::Bundle(HistoricalBundleEvidence::new(
            lineage,
            version,
            bundle.bundle_hash(),
            HistoricalBundleBytes::new(bundle.canonical_bytes().to_vec())
                .map_err(value_error_as_storage)?,
        ));
        consider_evidence(selected, after, evidence);
    }
    Ok(())
}

fn scan_plan_candidates(
    transaction: &ReadTransaction,
    after: Option<&[u8]>,
    selected: &mut Option<HistoricalCandidate>,
) -> Result<(), StorageError> {
    let terminal = transaction.open_table(IDEMPOTENCY).map_err(table_error)?;
    for entry in terminal.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let physical = keys::decode_idempotency_key(key.value()).map_err(|_| corrupt())?;
        let record =
            decoded(codec::decode_idempotency_record_v1(value.value())).map_err(|_| corrupt())?;
        let (identity, plan) = match &record {
            IdempotencyRecordV1::StoredOutcome(value) => (value.identity(), value.plan()),
            IdempotencyRecordV1::ExecutionFailed(value) => {
                (value.pending().identity(), value.pending().plan())
            }
        };
        if identity.storage_key().ok().as_ref() != Some(&physical) {
            return Err(corrupt());
        }
        consider_plan(selected, after, plan.clone());
    }
    let pending = transaction
        .open_table(IDEMPOTENCY_PENDING)
        .map_err(table_error)?;
    for entry in pending.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let physical = keys::decode_idempotency_key(key.value()).map_err(|_| corrupt())?;
        let record =
            decoded(codec::decode_pending_admission_v1(value.value())).map_err(|_| corrupt())?;
        if record.identity().storage_key().ok().as_ref() != Some(&physical) {
            return Err(corrupt());
        }
        consider_plan(selected, after, record.plan().clone());
    }
    let commits = transaction.open_table(COMMITS).map_err(table_error)?;
    for entry in commits.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let sequence = keys::decode_application_sequence_key(key.value()).map_err(|_| corrupt())?;
        let record =
            decoded(codec::decode_commit_record_v1(value.value())).map_err(|_| corrupt())?;
        if record.commit_sequence() != sequence {
            return Err(corrupt());
        }
        consider_plan(selected, after, record.plan().clone());
    }
    let provenance = transaction.open_table(PROVENANCE).map_err(table_error)?;
    for entry in provenance.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let id = keys::decode_provenance_key(key.value()).map_err(|_| corrupt())?;
        let record =
            decoded(codec::decode_provenance_record_v1(value.value())).map_err(|_| corrupt())?;
        if record.provenance_id() != id {
            return Err(corrupt());
        }
        consider_plan(selected, after, record.plan().clone());
    }
    Ok(())
}

fn scan_active_candidate(
    transaction: &ReadTransaction,
    after: Option<&[u8]>,
    selected: &mut Option<HistoricalCandidate>,
) -> Result<(), StorageError> {
    let active = read_active_pointer(transaction)?.map_err(|_| corrupt())?;
    let evidence = HistoricalSemanticEvidence::ActiveCatalog(active.map(|active| {
        HistoricalActiveCatalogEvidence::new(
            active.lineage().clone(),
            active.contract_version(),
            active.bundle_hash(),
        )
    }));
    consider_evidence(selected, after, evidence);
    Ok(())
}

fn scan_persisted_key_candidates(
    transaction: &ReadTransaction,
    after: Option<&[u8]>,
    selected: &mut Option<HistoricalCandidate>,
) -> Result<(), StorageError> {
    let entities = transaction.open_table(ENTITIES).map_err(table_error)?;
    for entry in entities.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let physical = keys::decode_entity_key(key.value()).map_err(|_| corrupt())?;
        let record =
            decoded(codec::decode_entity_record_v1(value.value())).map_err(|_| corrupt())?;
        if record.target().key() != &physical {
            return Err(corrupt());
        }
        consider_evidence(
            selected,
            after,
            HistoricalSemanticEvidence::PersistedKey(
                HistoricalPersistedKeyEvidenceV1::from_entity(&record),
            ),
        );
    }
    let indexes = transaction
        .open_table(SECONDARY_INDEXES)
        .map_err(table_error)?;
    for entry in indexes.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let physical = keys::decode_index_entry_key(key.value()).map_err(|_| corrupt())?;
        let record = decoded(codec::decode_index_entry_v1(value.value())).map_err(|_| corrupt())?;
        if record.key() != &physical {
            return Err(corrupt());
        }
        consider_evidence(
            selected,
            after,
            HistoricalSemanticEvidence::PersistedKey(
                HistoricalPersistedKeyEvidenceV1::from_index_entry(&record),
            ),
        );
    }
    let epochs = transaction.open_table(INDEX_EPOCHS).map_err(table_error)?;
    for entry in epochs.iter().map_err(precommit_storage_error)? {
        let (key, value) = entry.map_err(precommit_storage_error)?;
        let physical = keys::decode_index_range_prefix_key(key.value()).map_err(|_| corrupt())?;
        let record = decoded(codec::decode_index_epoch_v1(value.value())).map_err(|_| corrupt())?;
        if record.target() != &physical {
            return Err(corrupt());
        }
        consider_evidence(
            selected,
            after,
            HistoricalSemanticEvidence::PersistedKey(
                HistoricalPersistedKeyEvidenceV1::from_index_epoch(&record),
            ),
        );
    }
    Ok(())
}

fn consider_plan(
    selected: &mut Option<HistoricalCandidate>,
    after: Option<&[u8]>,
    plan: riffdb_storage_api::ExecutablePlanRef,
) {
    consider_evidence(
        selected,
        after,
        HistoricalSemanticEvidence::PlanReference(plan),
    );
}

fn consider_evidence(
    selected: &mut Option<HistoricalCandidate>,
    after: Option<&[u8]>,
    evidence: HistoricalSemanticEvidence,
) {
    consider_candidate(
        selected,
        after,
        HistoricalCandidate {
            key: historical_order_key(&evidence),
            evidence,
        },
    );
}

fn historical_order_key(evidence: &HistoricalSemanticEvidence) -> Vec<u8> {
    let mut key = Vec::new();
    match evidence {
        HistoricalSemanticEvidence::Bundle(bundle) => {
            key.push(0x01);
            push_lineage(&mut key, bundle.lineage());
            key.extend_from_slice(&bundle.version().to_be_bytes());
            key.extend_from_slice(bundle.bundle_hash().as_bytes());
        }
        HistoricalSemanticEvidence::PlanReference(plan) => {
            key.push(0x02);
            push_lineage(&mut key, plan.contract_lineage());
            key.extend_from_slice(&plan.contract_version().to_be_bytes());
            key.extend_from_slice(plan.contract_bundle_hash().as_bytes());
            key.extend_from_slice(&plan.command_id().to_be_bytes());
            key.extend_from_slice(plan.command_plan_hash().as_bytes());
        }
        HistoricalSemanticEvidence::ActiveCatalog(None) => key.extend_from_slice(&[0x03, 0x00]),
        HistoricalSemanticEvidence::ActiveCatalog(Some(active)) => {
            key.extend_from_slice(&[0x03, 0x01]);
            push_lineage(&mut key, active.lineage());
            key.extend_from_slice(&active.version().to_be_bytes());
            key.extend_from_slice(active.bundle_hash().as_bytes());
        }
        HistoricalSemanticEvidence::PersistedKey(persisted) => {
            key.push(0x04);
            push_lineage(&mut key, persisted.schema().lineage());
            key.extend_from_slice(&persisted.schema().contract_version().to_be_bytes());
            key.extend_from_slice(persisted.schema().bundle_hash().as_bytes());
            match persisted.key() {
                riffdb_storage_api::IrOpaquePersistedKeyV1::Entity {
                    entity_type_id,
                    key: entity_key,
                } => {
                    key.push(0x01);
                    key.extend_from_slice(&entity_type_id.to_be_bytes());
                    push_bytes(&mut key, entity_key.as_bytes());
                }
                riffdb_storage_api::IrOpaquePersistedKeyV1::IndexEntry {
                    index_id,
                    key: index_key,
                } => {
                    key.push(0x02);
                    key.extend_from_slice(&index_id.to_be_bytes());
                    push_bytes(&mut key, index_key.as_bytes());
                }
                riffdb_storage_api::IrOpaquePersistedKeyV1::IndexRangePrefix(prefix) => {
                    key.push(0x03);
                    key.extend_from_slice(&prefix.index_id().to_be_bytes());
                    push_bytes(&mut key, prefix.as_bytes());
                }
            }
        }
    }
    key
}

fn historical_semantic_bytes(evidence: &HistoricalSemanticEvidence) -> Result<usize, StorageError> {
    let bytes = match evidence {
        HistoricalSemanticEvidence::Bundle(bundle) => bundle
            .bytes()
            .as_bytes()
            .len()
            .checked_add(1 + 4 + bundle.lineage().as_bytes().len() + 8 + 32 + 4),
        HistoricalSemanticEvidence::PlanReference(plan) => {
            (1 + 4 + plan.contract_lineage().as_bytes().len()).checked_add(8 + 32 + 4 + 32)
        }
        HistoricalSemanticEvidence::ActiveCatalog(None) => Some(2),
        HistoricalSemanticEvidence::ActiveCatalog(Some(active)) => {
            (2 + 4 + active.lineage().as_bytes().len()).checked_add(8 + 32)
        }
        HistoricalSemanticEvidence::PersistedKey(persisted) => {
            let key_bytes = match persisted.key() {
                riffdb_storage_api::IrOpaquePersistedKeyV1::Entity { key, .. } => key.as_bytes(),
                riffdb_storage_api::IrOpaquePersistedKeyV1::IndexEntry { key, .. } => {
                    key.as_bytes()
                }
                riffdb_storage_api::IrOpaquePersistedKeyV1::IndexRangePrefix(key) => key.as_bytes(),
            };
            (1 + 4 + persisted.schema().lineage().as_bytes().len())
                .checked_add(8 + 32 + 1 + 4 + 4)
                .and_then(|value| value.checked_add(key_bytes.len()))
        }
    };
    bytes.ok_or_else(limit_exceeded)
}

fn read_historical_bundle(
    transaction: &ReadTransaction,
    lineage: &ContractLineage,
    version: ContractVersion,
    hash: ContractBundleHash,
) -> Result<Option<HistoricalBundleEvidence>, StorageError> {
    let key = keys::encode_contract_bundle_key(lineage, version).map_err(|_| invariant())?;
    let table = transaction
        .open_table(CONTRACT_BUNDLES)
        .map_err(table_error)?;
    let Some(value) = table.get(key.as_slice()).map_err(precommit_storage_error)? else {
        return Ok(None);
    };
    let bundle = decoded(codec::decode_contract_bundle_v1(value.value())).map_err(|_| corrupt())?;
    if bundle.lineage() != lineage
        || bundle.contract_version() != version
        || bundle.bundle_hash() != hash
        || hash_contract_bundle(bundle.canonical_bytes()) != hash
    {
        return Err(corrupt());
    }
    Ok(Some(HistoricalBundleEvidence::new(
        lineage.clone(),
        version,
        hash,
        HistoricalBundleBytes::new(bundle.canonical_bytes().to_vec())
            .map_err(value_error_as_storage)?,
    )))
}

fn decoded<T>(
    value: Result<riffdb_storage_api::EncodedPageItem<T>, StorageError>,
) -> Result<T, StructuralFindingCode> {
    value
        .map(riffdb_storage_api::EncodedPageItem::into_parts)
        .map(|(value, _)| value)
        .map_err(|error| match error.kind() {
            StorageErrorKind::LimitExceeded => StructuralFindingCode::LimitExceeded,
            _ => StructuralFindingCode::MalformedRecord,
        })
}

fn get_decoded<T>(
    transaction: &ReadTransaction,
    definition: TableDefinition<&'static [u8], &'static [u8]>,
    key: &[u8],
    decoder: impl FnOnce(&[u8]) -> Result<riffdb_storage_api::EncodedPageItem<T>, StorageError>,
) -> Result<Result<Option<T>, StructuralFindingCode>, StorageError> {
    let table = transaction.open_table(definition).map_err(table_error)?;
    let Some(value) = table.get(key).map_err(precommit_storage_error)? else {
        return Ok(Ok(None));
    };
    Ok(decoded(decoder(value.value())).map(Some))
}

fn meta_decode<T, R>(
    table: &R,
    key: &'static str,
    decoder: impl FnOnce(&[u8]) -> Result<riffdb_storage_api::EncodedPageItem<T>, StorageError>,
) -> Result<Result<T, StructuralFindingCode>, StorageError>
where
    R: ReadableTable<&'static str, &'static [u8]>,
{
    let Some(value) = table.get(key).map_err(precommit_storage_error)? else {
        return Ok(Err(StructuralFindingCode::MalformedRecord));
    };
    Ok(decoded(decoder(value.value())))
}

fn raw_exists(
    transaction: &ReadTransaction,
    definition: TableDefinition<&'static [u8], &'static [u8]>,
    key: &[u8],
) -> Result<bool, StorageError> {
    let table = transaction.open_table(definition).map_err(table_error)?;
    let found = table.get(key).map_err(precommit_storage_error)?.is_some();
    Ok(found)
}

fn plan_bundle_exists(
    transaction: &ReadTransaction,
    plan: &riffdb_storage_api::ExecutablePlanRef,
) -> Result<bool, StorageError> {
    bundle_exists(
        transaction,
        plan.contract_lineage(),
        plan.contract_version(),
        plan.contract_bundle_hash(),
    )
}

fn binding_bundle_exists(
    transaction: &ReadTransaction,
    binding: &riffdb_storage_api::DurableKeySchemaBindingV1,
) -> Result<bool, StorageError> {
    bundle_exists(
        transaction,
        binding.lineage(),
        binding.contract_version(),
        binding.bundle_hash(),
    )
}

fn bundle_pointer_exists(
    transaction: &ReadTransaction,
    pointer: &riffdb_storage_api::ActiveCatalogPointerV1,
) -> Result<bool, StorageError> {
    bundle_exists(
        transaction,
        pointer.lineage(),
        pointer.contract_version(),
        pointer.bundle_hash(),
    )
}

fn bundle_exists(
    transaction: &ReadTransaction,
    lineage: &ContractLineage,
    version: ContractVersion,
    hash: ContractBundleHash,
) -> Result<bool, StorageError> {
    let key = keys::encode_contract_bundle_key(lineage, version).map_err(|_| invariant())?;
    let bundle = get_decoded(
        transaction,
        CONTRACT_BUNDLES,
        &key,
        codec::decode_contract_bundle_v1,
    )?;
    Ok(matches!(bundle, Ok(Some(value))
        if value.lineage() == lineage
            && value.contract_version() == version
            && value.bundle_hash() == hash
            && hash_contract_bundle(value.canonical_bytes()) == hash))
}

fn commit_exists(
    transaction: &ReadTransaction,
    sequence: CommitSequence,
) -> Result<bool, StorageError> {
    Ok(get_commit(transaction, sequence)?.is_some())
}

fn missing_projection_source_finding(
    transaction: &ReadTransaction,
    sequence: CommitSequence,
) -> Result<Option<StructuralFinding>, StorageError> {
    if commit_exists(transaction, sequence)? {
        return Ok(None);
    }
    let meta = transaction.open_table(META).map_err(table_error)?;
    let allocator = match meta_decode(
        &meta,
        META_APPLICATION_SEQUENCE,
        codec::decode_application_sequence_allocator_v1,
    )? {
        Ok(value) => value,
        Err(code) => return Ok(Some(authoritative(code))),
    };
    let was_allocated = match allocator {
        ApplicationSequenceAllocator::Next(next) => sequence < next,
        ApplicationSequenceAllocator::Exhausted => true,
    };
    Ok(Some(if was_allocated {
        authoritative(StructuralFindingCode::MissingCrossLink)
    } else {
        derived_projection(StructuralFindingCode::ProjectionStateMismatch)
    }))
}

fn get_commit(
    transaction: &ReadTransaction,
    sequence: CommitSequence,
) -> Result<Option<riffdb_storage_api::StoredCommitRecordV1>, StorageError> {
    let key = keys::encode_application_sequence_key(sequence);
    match get_decoded(transaction, COMMITS, &key, codec::decode_commit_record_v1)? {
        Ok(value) => Ok(value.filter(|record| record.commit_sequence() == sequence)),
        Err(_) => Ok(None),
    }
}

fn get_provenance(
    transaction: &ReadTransaction,
    id: riffdb_types::ProvenanceId,
) -> Result<Option<riffdb_storage_api::StoredProvenanceRecordV1>, StorageError> {
    let key = keys::encode_provenance_key(id);
    match get_decoded(
        transaction,
        PROVENANCE,
        &key,
        codec::decode_provenance_record_v1,
    )? {
        Ok(value) => Ok(value.filter(|record| record.provenance_id() == id)),
        Err(_) => Ok(None),
    }
}

fn get_terminal(
    transaction: &ReadTransaction,
    identity: &riffdb_storage_api::IdempotencyIdentity,
) -> Result<Option<IdempotencyRecordV1>, StorageError> {
    let key = identity.storage_key().map_err(|_| corrupt())?;
    match get_decoded(
        transaction,
        IDEMPOTENCY,
        key.as_bytes(),
        codec::decode_idempotency_record_v1,
    )? {
        Ok(value) => Ok(value),
        Err(_) => Ok(None),
    }
}

fn get_event(
    transaction: &ReadTransaction,
    id: riffdb_types::EventId,
) -> Result<Option<riffdb_storage_api::StoredDurableEventV1>, StorageError> {
    let key = keys::encode_event_key(id);
    match get_decoded(transaction, EVENTS, &key, codec::decode_durable_event_v1)? {
        Ok(value) => Ok(value.filter(|event| event.event_id() == id)),
        Err(_) => Ok(None),
    }
}

fn get_outbox(
    transaction: &ReadTransaction,
    id: riffdb_types::EventId,
) -> Result<Option<riffdb_storage_api::StoredOutboxIntentV1>, StorageError> {
    let key = keys::encode_event_key(id);
    match get_decoded(transaction, OUTBOX, &key, codec::decode_outbox_intent_v1)? {
        Ok(value) => Ok(value.filter(|intent| intent.event_id() == id)),
        Err(_) => Ok(None),
    }
}

fn outcome_graph_is_reciprocal(
    transaction: &ReadTransaction,
    outcome: &riffdb_storage_api::StoredOutcomeV1,
) -> Result<bool, StorageError> {
    let Some(commit) = get_commit(transaction, outcome.commit_sequence())? else {
        return Ok(false);
    };
    let Some(provenance) = get_provenance(transaction, outcome.provenance_id())? else {
        return Ok(false);
    };
    Ok(outcome_matches_commit(outcome, &commit)
        && provenance_matches(outcome, &commit, &provenance))
}

fn commit_graph_is_reciprocal(
    transaction: &ReadTransaction,
    commit: &riffdb_storage_api::StoredCommitRecordV1,
) -> Result<bool, StorageError> {
    let Some(provenance) = get_provenance(transaction, commit.provenance_id())? else {
        return Ok(false);
    };
    let Some(IdempotencyRecordV1::StoredOutcome(outcome)) =
        get_terminal(transaction, provenance.identity())?
    else {
        return Ok(false);
    };
    if !outcome_matches_commit(&outcome, commit)
        || !provenance_matches(&outcome, commit, &provenance)
    {
        return Ok(false);
    }
    for event in commit.events() {
        if get_event(transaction, event.event_id())?.as_ref() != Some(event)
            || get_outbox(transaction, event.event_id())?
                .as_ref()
                .map(riffdb_storage_api::StoredOutboxIntentV1::event)
                != Some(event)
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn provenance_graph_is_reciprocal(
    transaction: &ReadTransaction,
    provenance: &riffdb_storage_api::StoredProvenanceRecordV1,
) -> Result<bool, StorageError> {
    let Some(commit) = get_commit(transaction, provenance.commit_sequence())? else {
        return Ok(false);
    };
    let Some(IdempotencyRecordV1::StoredOutcome(outcome)) =
        get_terminal(transaction, provenance.identity())?
    else {
        return Ok(false);
    };
    Ok(provenance_matches(&outcome, &commit, provenance))
}

fn event_graph_is_reciprocal(
    transaction: &ReadTransaction,
    event: &riffdb_storage_api::StoredDurableEventV1,
) -> Result<bool, StorageError> {
    let Some(commit) = get_commit(transaction, event.event_id().commit_sequence())? else {
        return Ok(false);
    };
    let Ok(ordinal) = usize::try_from(event.event_id().event_ordinal()) else {
        return Ok(false);
    };
    let Some(embedded) = commit.events().get(ordinal) else {
        return Ok(false);
    };
    let Some(intent) = get_outbox(transaction, event.event_id())? else {
        return Ok(false);
    };
    Ok(embedded == event && intent.event() == event)
}

fn outcome_matches_commit(
    outcome: &riffdb_storage_api::StoredOutcomeV1,
    commit: &riffdb_storage_api::StoredCommitRecordV1,
) -> bool {
    outcome.commit_sequence() == commit.commit_sequence()
        && outcome.admission_request_id() == commit.admission_request_id()
        && outcome.plan() == commit.plan()
        && outcome.canonical_input_hash() == commit.canonical_input_hash()
        && outcome.actor() == commit.actor()
        && outcome.logical_time() == commit.logical_time()
        && outcome.partition_hash() == commit.partition_hash()
        && outcome.conflict_hashes() == commit.conflict_hashes()
        && outcome.declared_outcome() == commit.declared_outcome()
        && outcome.provenance_id() == commit.provenance_id()
        && outcome.durability_mode() == commit.durability_mode()
}

fn provenance_matches(
    outcome: &riffdb_storage_api::StoredOutcomeV1,
    commit: &riffdb_storage_api::StoredCommitRecordV1,
    provenance: &riffdb_storage_api::StoredProvenanceRecordV1,
) -> bool {
    provenance.provenance_id() == outcome.provenance_id()
        && provenance.commit_sequence() == commit.commit_sequence()
        && provenance.identity() == outcome.identity()
        && provenance.admission_request_id() == commit.admission_request_id()
        && provenance.plan() == commit.plan()
        && provenance.canonical_input_hash() == commit.canonical_input_hash()
        && provenance.actor() == commit.actor()
        && provenance.logical_time() == commit.logical_time()
        && provenance.partition_hash() == commit.partition_hash()
        && provenance.conflict_hashes() == commit.conflict_hashes()
        && provenance.outcome_id() == commit.declared_outcome().outcome_id()
        && provenance.admitted_claims() == outcome.admitted_claims()
        && provenance.event_ids() == commit.outbox_event_ids()
        && provenance.affected_entities().len() == commit.mutations().len()
        && provenance
            .affected_entities()
            .iter()
            .zip(commit.mutations())
            .all(|(affected, mutation)| {
                affected
                    == &riffdb_storage_api::AffectedEntityV1::from_record(mutation.post_image())
            })
}

fn idempotency_digest_is_readable(
    inputs: &StartupValidationInputs,
    digest: riffdb_storage_api::IdempotencyKeyDigest,
) -> bool {
    ReadableDigestKey::new(digest.scheme(), digest.key_id())
        .is_ok_and(|key| inputs.idempotency_digests().as_slice().contains(&key))
}

fn projection_control_exists(
    transaction: &ReadTransaction,
    identity: &riffdb_types::ProjectionIdentity,
) -> Result<bool, StorageError> {
    let key = riffdb_types::ProjectionFrontierKey::new(identity.clone());
    let control = get_decoded(
        transaction,
        PROJECTION_FRONTIER,
        key.as_bytes(),
        codec::decode_projection_control_v1,
    )?;
    Ok(matches!(control, Ok(Some(value)) if value.identity() == identity))
}

fn capability_audit_exists(
    transaction: &ReadTransaction,
    capability: &riffdb_storage_api::StoredCapabilityRecordV1,
) -> Result<bool, StorageError> {
    let key = keys::encode_audit_key(capability.creation_sequence());
    let record = get_decoded(
        transaction,
        AUDIT,
        &key,
        codec::decode_administration_audit_record_v1,
    )?;
    Ok(matches!(record,
        Ok(Some(riffdb_storage_api::StoredAdministrationAuditRecordV1::Capability(record)))
            if record.target_capability_id() == capability.capability_id()
                && record.request_id() == capability.creation_request_id()
                && record.timestamp() == capability.issued_at()))
}

fn capability_marker_exists(
    transaction: &ReadTransaction,
    marker: riffdb_storage_api::CapabilityBootstrapMarkerV1,
) -> Result<bool, StorageError> {
    let key = keys::encode_capability_key(marker.capability_id());
    let capability = get_decoded(
        transaction,
        CAPABILITIES,
        &key,
        codec::decode_capability_record_v1,
    )?;
    Ok(matches!(capability, Ok(Some(value))
        if value.creation_sequence() == marker.administration_sequence()))
}

fn service_link_exists(
    transaction: &ReadTransaction,
    record: &riffdb_storage_api::StoredServiceAuditRecordV1,
) -> Result<bool, StorageError> {
    match record.link() {
        riffdb_types::ServiceAuditLinkV1::None => Ok(true),
        riffdb_types::ServiceAuditLinkV1::Command {
            commit_sequence,
            provenance_id,
        } => Ok(get_commit(transaction, commit_sequence)?
            .is_some_and(|commit| commit.provenance_id() == provenance_id)),
        riffdb_types::ServiceAuditLinkV1::ControlPlane {
            administration_sequence,
        } => {
            let key = keys::encode_audit_key(administration_sequence);
            raw_exists(transaction, AUDIT, &key)
        }
    }
}

fn application_allocator_matches(
    transaction: &ReadTransaction,
    allocator: ApplicationSequenceAllocator,
) -> Result<bool, StorageError> {
    let table = transaction.open_table(COMMITS).map_err(table_error)?;
    let expected = match table.last().map_err(precommit_storage_error)? {
        None => ApplicationSequenceAllocator::initial(),
        Some((key, value)) => {
            let Ok(sequence) = keys::decode_application_sequence_key(key.value()) else {
                return Ok(false);
            };
            let Ok(record) = decoded(codec::decode_commit_record_v1(value.value())) else {
                return Ok(false);
            };
            if record.commit_sequence() != sequence {
                return Ok(false);
            }
            sequence.checked_next().map_or(
                ApplicationSequenceAllocator::Exhausted,
                ApplicationSequenceAllocator::next,
            )
        }
    };
    Ok(allocator == expected)
}

fn administration_allocator_matches(
    transaction: &ReadTransaction,
    allocator: riffdb_storage_api::AdministrationSequenceAllocator,
) -> Result<bool, StorageError> {
    let table = transaction.open_table(AUDIT).map_err(table_error)?;
    let expected = match table.last().map_err(precommit_storage_error)? {
        None => riffdb_storage_api::AdministrationSequenceAllocator::initial(),
        Some((key, value)) => {
            let Ok(sequence) = keys::decode_audit_key(key.value()) else {
                return Ok(false);
            };
            let Ok(record) = decoded(codec::decode_administration_audit_record_v1(value.value()))
            else {
                return Ok(false);
            };
            if record.administration_sequence() != sequence {
                return Ok(false);
            }
            sequence.checked_next().map_or(
                riffdb_storage_api::AdministrationSequenceAllocator::Exhausted,
                riffdb_storage_api::AdministrationSequenceAllocator::next,
            )
        }
    };
    Ok(allocator == expected)
}

fn read_active_pointer(
    transaction: &ReadTransaction,
) -> Result<
    Result<Option<riffdb_storage_api::ActiveCatalogPointerV1>, StructuralFindingCode>,
    StorageError,
> {
    let table = transaction
        .open_table(CATALOG_ACTIVE)
        .map_err(table_error)?;
    if table.len().map_err(precommit_storage_error)? > 1 {
        return Ok(Err(StructuralFindingCode::CrossLinkMismatch));
    }
    let Some((key, value)) = table.first().map_err(precommit_storage_error)? else {
        return Ok(Ok(None));
    };
    if key.value() != CATALOG_ACTIVE_KEY {
        return Ok(Err(StructuralFindingCode::MalformedRecord));
    }
    Ok(decoded(codec::decode_active_catalog_pointer_v1(value.value())).map(Some))
}

fn has_application_authoritative_state(
    transaction: &ReadTransaction,
) -> Result<bool, StorageError> {
    for definition in [
        ENTITIES,
        SECONDARY_INDEXES,
        INDEX_EPOCHS,
        IDEMPOTENCY,
        IDEMPOTENCY_PENDING,
        COMMITS,
        PROVENANCE,
        EVENTS,
        OUTBOX,
    ] {
        if table_len(transaction, definition)? != 0 {
            return Ok(true);
        }
    }
    Ok(false)
}

fn push_lineage(output: &mut Vec<u8>, lineage: &ContractLineage) {
    let length =
        u32::try_from(lineage.as_bytes().len()).expect("foundational lineage hard bound fits u32");
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(lineage.as_bytes());
}

fn push_bytes(output: &mut Vec<u8>, bytes: &[u8]) {
    let length = u32::try_from(bytes.len()).expect("foundational key hard bound fits u32");
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(bytes);
}

fn authoritative(code: StructuralFindingCode) -> StructuralFinding {
    StructuralFinding::new(StructuralFindingScope::Authoritative, code)
}

fn derived_outbox(code: StructuralFindingCode) -> StructuralFinding {
    StructuralFinding::new(StructuralFindingScope::OutboxDelivery, code)
}

fn derived_projection(code: StructuralFindingCode) -> StructuralFinding {
    StructuralFinding::new(StructuralFindingScope::Projection, code)
}

fn invariant() -> StorageError {
    storage_error(StorageErrorKind::InvariantViolation)
}

fn corrupt() -> StorageError {
    storage_error(StorageErrorKind::CorruptData)
}

fn limit_exceeded() -> StorageError {
    storage_error(StorageErrorKind::LimitExceeded)
}

fn value_error_as_storage(error: StorageValueError) -> StorageError {
    match error {
        StorageValueError::LimitExceeded | StorageValueError::SizeOverflow => limit_exceeded(),
        _ => invariant(),
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use riffdb_storage_api::{
        DatabaseInitializationPort, HistoricalEvidencePage, ReadableCapabilityDigestInventory,
        ReadableIdempotencyDigestInventory, StructuralEvidenceEnd, StructuralEvidencePage,
    };
    use riffdb_types::{DigestKeyId, Timestamp};

    use super::*;

    static NEXT_TEST_PATH: AtomicU64 = AtomicU64::new(1);

    struct TestDatabasePath(PathBuf);

    impl TestDatabasePath {
        fn new(label: &str) -> Self {
            let ordinal = NEXT_TEST_PATH.fetch_add(1, Ordering::Relaxed);
            Self(std::env::temp_dir().join(format!(
                "riffdb-redb-startup-{label}-{}-{ordinal}.redb",
                std::process::id()
            )))
        }
    }

    impl Drop for TestDatabasePath {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn database_id(seed: u8) -> DatabaseId {
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [seed; 10])
            .expect("valid deterministic UUIDv7")
    }

    fn inputs() -> StartupValidationInputs {
        let key = ReadableDigestKey::v1(DigestKeyId::new(1).expect("digest key"));
        StartupValidationInputs::new(
            Timestamp::new(1, 0).expect("timestamp"),
            ReadableCapabilityDigestInventory::new(vec![key]).expect("capability inventory"),
            ReadableIdempotencyDigestInventory::new(vec![key]).expect("idempotency inventory"),
        )
    }

    fn initialized_store(path: &TestDatabasePath, id: DatabaseId) -> RedbStore {
        let mut store = RedbStore::open(&path.0).expect("open store");
        store.initialize_database(id).expect("initialize store");
        store
    }

    fn finish_structural(session: &mut RedbStructuralEvidenceSession) -> RedbStructuralEvidenceEnd {
        let mut cursor =
            StructuralEvidenceCursor::start(session.database_id(), session.open_session_id());
        loop {
            match session
                .read_structural_evidence(cursor, EvidencePageLimit::new(1).expect("page limit"))
                .expect("structural page")
            {
                StructuralEvidencePage::Page {
                    start,
                    findings,
                    next,
                } => {
                    assert_eq!(start, cursor);
                    assert!(findings.is_empty());
                    cursor = next;
                }
                StructuralEvidencePage::ExactEnd(end) => {
                    assert_eq!(end.cursor(), cursor);
                    return end;
                }
            }
        }
    }

    fn finish_historical(session: &mut RedbStructuralEvidenceSession) -> RedbHistoricalEvidenceEnd {
        let mut cursor =
            HistoricalEvidenceCursor::start(session.database_id(), session.open_session_id());
        let mut saw_absent_active = false;
        loop {
            match session
                .read_historical_evidence(cursor, EvidencePageLimit::new(1).expect("page limit"))
                .expect("historical page")
            {
                HistoricalEvidencePage::Page {
                    start,
                    evidence,
                    next,
                } => {
                    assert_eq!(start, cursor);
                    saw_absent_active |= matches!(
                        evidence.as_slice(),
                        [HistoricalSemanticEvidence::ActiveCatalog(None)]
                    );
                    cursor = next;
                }
                HistoricalEvidencePage::ExactEnd(end) => {
                    assert!(saw_absent_active);
                    assert_eq!(end.cursor(), cursor);
                    return end;
                }
            }
        }
    }

    #[test]
    fn initialized_empty_store_reaches_both_exact_ends_and_reopens() {
        let path = TestDatabasePath::new("empty");
        let id = database_id(0x11);
        let store = initialized_store(&path, id);
        let mut session = store
            .begin_structural_evidence(inputs())
            .expect("begin evidence");
        assert_eq!(session.database_id(), id);
        assert!(
            session
                .read_historical_bundle(
                    &ContractLineage::new("missing").expect("lineage"),
                    ContractVersion::new(1).expect("version"),
                    ContractBundleHash::from_bytes([0x33; 32]),
                )
                .expect("point lookup")
                .is_none()
        );
        let structural_end = finish_structural(&mut session);
        let historical_end = finish_historical(&mut session);
        assert!(
            session
                .read_historical_bundle(
                    &ContractLineage::new("missing").expect("lineage"),
                    ContractVersion::new(1).expect("version"),
                    ContractBundleHash::from_bytes([0x33; 32]),
                )
                .expect("point lookup remains live after exact ends")
                .is_none()
        );
        let opened = session
            .finish(structural_end, historical_end)
            .expect("finish evidence");
        assert_eq!(opened.database_id(), id);
        drop(opened);

        let reopened = RedbStore::open(&path.0).expect("reopen after handoff drop");
        assert_eq!(
            riffdb_storage_api::DatabaseIdentityProbePort::probe_database_identity(&reopened)
                .expect("probe reopened"),
            riffdb_storage_api::DatabaseIdentityProbe::Existing(id)
        );
    }

    #[test]
    fn cursors_are_session_bound_and_single_use() {
        let path = TestDatabasePath::new("cursor");
        let store = initialized_store(&path, database_id(0x22));
        let mut session = store
            .begin_structural_evidence(inputs())
            .expect("begin evidence");
        let start =
            StructuralEvidenceCursor::start(session.database_id(), session.open_session_id());
        let page = session
            .read_structural_evidence(start, EvidencePageLimit::new(1).expect("page limit"))
            .expect("first page");
        let StructuralEvidencePage::Page { next, .. } = page else {
            panic!("initialized metadata requires a non-final page");
        };
        assert_eq!(
            session
                .read_structural_evidence(start, EvidencePageLimit::new(1).expect("page limit"),)
                .expect_err("cursor replay must fail")
                .kind(),
            StorageErrorKind::InvariantViolation
        );
        let wrong_session = StructuralEvidenceCursor::start(
            session.database_id(),
            OpenSessionId::new(session.open_session_id().get() + 1).expect("session ID"),
        );
        assert_eq!(
            session
                .read_structural_evidence(
                    wrong_session,
                    EvidencePageLimit::new(1).expect("page limit"),
                )
                .expect_err("wrong session must fail")
                .kind(),
            StorageErrorKind::InvariantViolation
        );
        assert!(next.position() > start.position());
    }

    #[test]
    fn evidence_requires_initialization_and_dropped_session_releases_the_open() {
        let path = TestDatabasePath::new("exclusive");
        let store = RedbStore::open(&path.0).expect("open empty container");
        assert_eq!(
            store
                .begin_structural_evidence(inputs())
                .expect_err("uninitialized evidence must fail")
                .kind(),
            StorageErrorKind::CorruptData
        );

        let store = initialized_store(&path, database_id(0x44));
        let session = store
            .begin_structural_evidence(inputs())
            .expect("begin evidence");
        assert!(RedbStore::open(&path.0).is_err());
        drop(session);
        RedbStore::open(&path.0).expect("dropped session releases database open");
    }

    #[test]
    fn independent_stores_receive_process_unique_open_session_ids() {
        let first_path = TestDatabasePath::new("session-a");
        let second_path = TestDatabasePath::new("session-b");
        let first = initialized_store(&first_path, database_id(0x55))
            .begin_structural_evidence(inputs())
            .expect("first session");
        let second = initialized_store(&second_path, database_id(0x66))
            .begin_structural_evidence(inputs())
            .expect("second session");
        assert_ne!(first.open_session_id(), second.open_session_id());
    }
}
