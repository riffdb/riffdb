//! Bounded 256-target canonical page inspection and complete namespace
//! enumeration for correlated-index qualification (ADR-0212 / WP-684).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::path::Path;

use riffdb_storage_api::{
    AuthoritativeEntityPartitionScanRequest, AuthoritativeNamespaceV1, AuthoritativeScanReader,
    AuthoritativeStateStepV3, ChangelogAttributionV3, EntityTarget, IndexEpochPosition,
    IndexRangePrefixBuilder, IndexRangeTarget, MAX_AFFECTED_INDEX_EPOCH_TARGETS,
    MAX_READ_SNAPSHOT_BYTES, OpenSessionId, PartitionIndexTarget, RetainedMetadataV1,
    StartupValidationInputs, StorageScanLimit, StoredAdministrationAuditRecordV1,
    StoredCommitRecordV1, StoredEntityRecordV1, StoredIndexEntryV2, StructuralFinding,
};
use riffdb_storage_redb::{RedbOperationalPorts, RedbStore};
use riffdb_types::{
    AggregateTypeId, ContractLineage, DatabaseId, EntityKey, EntityTypeId, FrontierPosition,
    IndexEntryKey, IndexId, PartitionKey,
};

use super::{
    DurableInspectionError, IndexRangeSelection, MAX_INSPECTED_ORDERED_RECORDS,
    MAX_INSPECTION_TARGETS, PAGE_LIMIT, append_bounded, open_validated_with_session,
    scan_all_administration, scan_all_commits, scan_full_index_range,
};

/// Maximum nonempty canonical inspection pages (`256 * 256 >= 65,535`).
pub const MAX_CORRELATED_INSPECTION_PAGES: usize = 256;
/// Maximum total targets across all pages of one correlated-index inspection.
pub const MAX_CORRELATED_INSPECTION_TOTAL_TARGETS: usize = MAX_AFFECTED_INDEX_EPOCH_TARGETS;

/// Independently declared corpus namespace for engine-side target enumeration.
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct CorrelatedCorpusNamespace {
    lineage: ContractLineage,
    aggregate_type_id: AggregateTypeId,
    partition_key: PartitionKey,
    entity_prefixes: Vec<(EntityTypeId, Vec<u8>)>,
    index_ids: Vec<IndexId>,
}

impl CorrelatedCorpusNamespace {
    /// Checks strictly ordered, unique entity-partition prefixes and index IDs.
    pub fn new(
        lineage: ContractLineage,
        aggregate_type_id: AggregateTypeId,
        partition_key: PartitionKey,
        entity_prefixes: Vec<(EntityTypeId, Vec<u8>)>,
        index_ids: Vec<IndexId>,
    ) -> Result<Self, DurableInspectionError> {
        if partition_key.aggregate_type_id() != aggregate_type_id
            || entity_prefixes.is_empty()
            || index_ids.is_empty()
            || entity_prefixes.len() > MAX_INSPECTION_TARGETS
            || index_ids.len() > MAX_INSPECTION_TARGETS
        {
            return Err(DurableInspectionError::NonCanonicalTargets);
        }
        if entity_prefixes.windows(2).any(|pair| pair[0] >= pair[1])
            || index_ids.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(DurableInspectionError::NonCanonicalTargets);
        }
        for (entity_type, prefix) in &entity_prefixes {
            let expected_header = [
                0x45,
                0x01,
                entity_type.to_be_bytes()[0],
                entity_type.to_be_bytes()[1],
                entity_type.to_be_bytes()[2],
                entity_type.to_be_bytes()[3],
            ];
            if prefix.len() <= expected_header.len() || !prefix.starts_with(&expected_header) {
                return Err(DurableInspectionError::NonCanonicalTargets);
            }
        }
        Ok(Self {
            lineage,
            aggregate_type_id,
            partition_key,
            entity_prefixes,
            index_ids,
        })
    }

    /// Borrows the declared contract lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Returns the declared aggregate type ID.
    #[must_use]
    pub const fn aggregate_type_id(&self) -> AggregateTypeId {
        self.aggregate_type_id
    }

    /// Borrows the declared partition key.
    #[must_use]
    pub const fn partition_key(&self) -> &PartitionKey {
        &self.partition_key
    }

    /// Borrows the declared `(entity_type_id, partition_prefix)` pairs.
    #[must_use]
    pub fn entity_prefixes(&self) -> &[(EntityTypeId, Vec<u8>)] {
        &self.entity_prefixes
    }

    /// Borrows the declared secondary index IDs.
    #[must_use]
    pub fn index_ids(&self) -> &[IndexId] {
        &self.index_ids
    }
}

impl fmt::Debug for CorrelatedCorpusNamespace {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CorrelatedCorpusNamespace")
            .field("lineage", &self.lineage)
            .field("aggregate_type_id", &self.aggregate_type_id)
            .field("entity_prefix_count", &self.entity_prefixes.len())
            .field("index_count", &self.index_ids.len())
            .finish()
    }
}

/// Canonical semantic target in the correlated-index comparison universe.
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
pub enum CorrelatedTargetKey {
    /// Authoritative entity row target.
    Entity(EntityTarget),
    /// Authoritative secondary-index entry target bound to its exact prefix bytes.
    IndexEntry {
        /// Declared secondary index ID.
        index_id: IndexId,
        /// Canonical index-key prefix bytes (excluding the trailing entity key).
        prefix_bytes: Vec<u8>,
        /// Complete canonical secondary-index entry key.
        key: IndexEntryKey,
    },
    /// Authoritative index-epoch / prefix target bound to its exact prefix bytes.
    IndexEpoch {
        /// Partition and index generation identity.
        target: PartitionIndexTarget,
        /// Canonical prefix bytes covered by this target (`empty` for whole-index generation).
        prefix_bytes: Vec<u8>,
    },
    /// Undeclared physical row discovered by engine-side namespace enumeration.
    RedbOnlyUndeclared {
        /// Authoritative table where the undeclared row appeared.
        namespace: AuthoritativeNamespaceV1,
        /// Raw physical key bytes.
        raw_key: Vec<u8>,
    },
}

impl CorrelatedTargetKey {
    /// Checked semantic byte footprint of this target identity.
    pub fn semantic_bytes(&self) -> Result<usize, DurableInspectionError> {
        match self {
            Self::Entity(target) => target
                .key()
                .as_bytes()
                .len()
                .checked_add(8)
                .ok_or(DurableInspectionError::RecordLimit),
            Self::IndexEntry {
                prefix_bytes, key, ..
            } => prefix_bytes
                .len()
                .checked_add(key.as_bytes().len())
                .and_then(|bytes| bytes.checked_add(8))
                .ok_or(DurableInspectionError::RecordLimit),
            Self::IndexEpoch {
                target,
                prefix_bytes,
            } => target
                .partition_key()
                .as_bytes()
                .len()
                .checked_add(prefix_bytes.len())
                .and_then(|bytes| bytes.checked_add(17))
                .ok_or(DurableInspectionError::RecordLimit),
            Self::RedbOnlyUndeclared { raw_key, .. } => raw_key
                .len()
                .checked_add(8)
                .ok_or(DurableInspectionError::RecordLimit),
        }
    }
}

impl fmt::Debug for CorrelatedTargetKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Entity(target) => formatter
                .debug_struct("CorrelatedTargetKey::Entity")
                .field("entity_type_id", &target.entity_type_id())
                .field("key_len", &target.key().as_bytes().len())
                .finish(),
            Self::IndexEntry {
                index_id,
                prefix_bytes,
                key,
            } => formatter
                .debug_struct("CorrelatedTargetKey::IndexEntry")
                .field("index_id", index_id)
                .field("prefix_len", &prefix_bytes.len())
                .field("key_len", &key.as_bytes().len())
                .finish(),
            Self::IndexEpoch {
                target,
                prefix_bytes,
            } => formatter
                .debug_struct("CorrelatedTargetKey::IndexEpoch")
                .field("index_id", &target.index_id())
                .field("prefix_len", &prefix_bytes.len())
                .finish(),
            Self::RedbOnlyUndeclared { namespace, raw_key } => formatter
                .debug_struct("CorrelatedTargetKey::RedbOnlyUndeclared")
                .field("namespace", namespace)
                .field("key_len", &raw_key.len())
                .finish(),
        }
    }
}

/// Complete semantic observation for one target in a canonical inspection page.
#[derive(Clone, Eq, PartialEq)]
pub enum CorrelatedTargetObservation {
    /// Entity target and its present or absent stored record.
    Entity {
        /// Canonical entity target.
        target: EntityTarget,
        /// Present or absent authoritative entity record.
        record: Option<StoredEntityRecordV1>,
    },
    /// Secondary-index entry target and its present or absent stored row.
    IndexEntry {
        /// Declared secondary index ID.
        index_id: IndexId,
        /// Canonical index-key prefix bytes.
        prefix_bytes: Vec<u8>,
        /// Complete canonical secondary-index entry key.
        key: IndexEntryKey,
        /// Present or absent authoritative index entry row.
        entry: Option<StoredIndexEntryV2>,
    },
    /// Index-epoch / prefix target and its observed generation epoch.
    IndexEpoch {
        /// Partition and index generation identity.
        target: PartitionIndexTarget,
        /// Canonical prefix bytes covered by this target.
        prefix_bytes: Vec<u8>,
        /// Observed generation epoch position.
        epoch: IndexEpochPosition,
    },
    /// Undeclared physical row discovered by engine-side namespace enumeration.
    RedbOnlyUndeclared {
        /// Authoritative table where the undeclared row appeared.
        namespace: AuthoritativeNamespaceV1,
        /// Raw physical key bytes.
        raw_key: Vec<u8>,
    },
}

impl CorrelatedTargetObservation {
    /// Returns the exact canonical key for this observation.
    #[must_use]
    pub fn key(&self) -> CorrelatedTargetKey {
        match self {
            Self::Entity { target, .. } => CorrelatedTargetKey::Entity(target.clone()),
            Self::IndexEntry {
                index_id,
                prefix_bytes,
                key,
                ..
            } => CorrelatedTargetKey::IndexEntry {
                index_id: *index_id,
                prefix_bytes: prefix_bytes.clone(),
                key: key.clone(),
            },
            Self::IndexEpoch {
                target,
                prefix_bytes,
                ..
            } => CorrelatedTargetKey::IndexEpoch {
                target: target.clone(),
                prefix_bytes: prefix_bytes.clone(),
            },
            Self::RedbOnlyUndeclared { namespace, raw_key } => {
                CorrelatedTargetKey::RedbOnlyUndeclared {
                    namespace: *namespace,
                    raw_key: raw_key.clone(),
                }
            }
        }
    }

    /// Checked semantic byte footprint of this observation.
    pub fn semantic_bytes(&self) -> Result<usize, DurableInspectionError> {
        let base = self.key().semantic_bytes()?;
        let payload = match self {
            Self::Entity {
                record: Some(record),
                ..
            } => record
                .fields_encoded_len()
                .checked_add(32)
                .ok_or(DurableInspectionError::RecordLimit)?,
            Self::Entity { record: None, .. } => 1,
            Self::IndexEntry {
                entry: Some(entry), ..
            } => entry
                .covered_values_encoded_len()
                .checked_add(entry.partition_key().as_bytes().len())
                .and_then(|bytes| bytes.checked_add(16))
                .ok_or(DurableInspectionError::RecordLimit)?,
            Self::IndexEntry { entry: None, .. } => 1,
            Self::IndexEpoch { .. } => 9,
            Self::RedbOnlyUndeclared { .. } => 1,
        };
        base.checked_add(payload)
            .ok_or(DurableInspectionError::RecordLimit)
    }
}

impl fmt::Debug for CorrelatedTargetObservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CorrelatedTargetObservation")
            .field("key", &self.key())
            .finish()
    }
}

/// Immutable-snapshot continuation token binding one page to the next.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CorrelatedInspectionContinuation {
    database_id: DatabaseId,
    open_session_id: OpenSessionId,
    application_frontier: FrontierPosition,
    namespace: CorrelatedCorpusNamespace,
    exclusive_last_key: CorrelatedTargetKey,
    page_ordinal: u16,
}

impl CorrelatedInspectionContinuation {
    /// Returns the bound database ID.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Returns the bound startup open-session ID.
    #[must_use]
    pub const fn open_session_id(&self) -> OpenSessionId {
        self.open_session_id
    }

    /// Returns the frozen recovered application frontier.
    #[must_use]
    pub const fn application_frontier(&self) -> FrontierPosition {
        self.application_frontier
    }

    /// Borrows the bound corpus namespace.
    #[must_use]
    pub const fn namespace(&self) -> &CorrelatedCorpusNamespace {
        &self.namespace
    }

    /// Borrows the exclusive last target key of the completed page.
    #[must_use]
    pub const fn exclusive_last_key(&self) -> &CorrelatedTargetKey {
        &self.exclusive_last_key
    }

    /// Returns the 1-based ordinal of the next page to read.
    #[must_use]
    pub const fn page_ordinal(&self) -> u16 {
        self.page_ordinal
    }

    /// Returns a mutated continuation for negative-test injection.
    #[must_use]
    pub fn with_exclusive_last_key(&self, exclusive_last_key: CorrelatedTargetKey) -> Self {
        let mut next = self.clone();
        next.exclusive_last_key = exclusive_last_key;
        next
    }
}

/// One bounded canonical page of at most [`MAX_INSPECTION_TARGETS`] (`256`) observations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CorrelatedInspectionPage {
    page_ordinal: u16,
    observations: Vec<CorrelatedTargetObservation>,
    semantic_bytes: usize,
    next: Option<CorrelatedInspectionContinuation>,
}

impl CorrelatedInspectionPage {
    /// Constructs a checked canonical page of at most [`MAX_INSPECTION_TARGETS`] observations.
    pub fn new(
        page_ordinal: u16,
        observations: Vec<CorrelatedTargetObservation>,
        next: Option<CorrelatedInspectionContinuation>,
    ) -> Result<Self, DurableInspectionError> {
        if usize::from(page_ordinal) >= MAX_CORRELATED_INSPECTION_PAGES {
            return Err(DurableInspectionError::TargetLimit);
        }
        if observations.len() > MAX_INSPECTION_TARGETS {
            return Err(DurableInspectionError::TargetLimit);
        }
        if observations
            .windows(2)
            .any(|pair| pair[0].key() >= pair[1].key())
        {
            return Err(DurableInspectionError::NonCanonicalPage);
        }
        if let Some(continuation) = &next {
            let Some(last) = observations.last() else {
                return Err(DurableInspectionError::NonCanonicalPage);
            };
            if continuation.exclusive_last_key() != &last.key()
                || continuation.page_ordinal != page_ordinal.saturating_add(1)
            {
                return Err(DurableInspectionError::NonCanonicalPage);
            }
        }
        let mut semantic_bytes = 0usize;
        for observation in &observations {
            semantic_bytes = semantic_bytes
                .checked_add(observation.semantic_bytes()?)
                .ok_or(DurableInspectionError::RecordLimit)?;
            if semantic_bytes > MAX_READ_SNAPSHOT_BYTES {
                return Err(DurableInspectionError::RecordLimit);
            }
        }
        Ok(Self {
            page_ordinal,
            observations,
            semantic_bytes,
            next,
        })
    }

    /// Returns the 0-based page ordinal.
    #[must_use]
    pub const fn page_ordinal(&self) -> u16 {
        self.page_ordinal
    }

    /// Borrows the page's strictly ordered observations (`<= 256`).
    #[must_use]
    pub fn observations(&self) -> &[CorrelatedTargetObservation] {
        &self.observations
    }

    /// Returns the checked semantic byte count of this page.
    #[must_use]
    pub const fn semantic_bytes(&self) -> usize {
        self.semantic_bytes
    }

    /// Borrows the continuation token (`None` on exact terminal exhaustion).
    #[must_use]
    pub const fn next(&self) -> Option<&CorrelatedInspectionContinuation> {
        self.next.as_ref()
    }

    /// Returns a copy of this page with a mutated observation list for negative
    /// divergence-injection tests (OBL-0212-2).
    #[must_use]
    pub fn with_observations_for_divergence_test(
        &self,
        observations: Vec<CorrelatedTargetObservation>,
    ) -> Self {
        Self {
            page_ordinal: self.page_ordinal,
            observations,
            semantic_bytes: self.semantic_bytes,
            next: self.next.clone(),
        }
    }
}

/// One stopped-redb immutable-snapshot inspection session for a correlated-index corpus.
pub struct CorrelatedNamespaceInspectionSession {
    database_id: DatabaseId,
    open_session_id: OpenSessionId,
    retained_metadata: RetainedMetadataV1,
    structural_findings: Vec<StructuralFinding>,
    structural_pages: u32,
    application_frontier: FrontierPosition,
    commits: Vec<StoredCommitRecordV1>,
    administration: Vec<StoredAdministrationAuditRecordV1>,
    namespace: CorrelatedCorpusNamespace,
    live_entities: BTreeMap<EntityTarget, StoredEntityRecordV1>,
    live_index_entries: BTreeMap<IndexEntryKey, (IndexId, Vec<u8>, StoredIndexEntryV2)>,
    index_epochs: BTreeMap<PartitionIndexTarget, IndexEpochPosition>,
    sealed_targets: BTreeSet<CorrelatedTargetKey>,
    undeclared_targets: BTreeSet<CorrelatedTargetKey>,
}

impl fmt::Debug for CorrelatedNamespaceInspectionSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CorrelatedNamespaceInspectionSession")
            .field("database_id", &self.database_id)
            .field("open_session_id", &self.open_session_id)
            .field("application_frontier", &self.application_frontier)
            .field("commit_count", &self.commits.len())
            .field("live_entity_count", &self.live_entities.len())
            .field("live_index_entry_count", &self.live_index_entries.len())
            .field("index_epoch_count", &self.index_epochs.len())
            .field("sealed_target_count", &self.sealed_targets.len())
            .field("undeclared_target_count", &self.undeclared_targets.len())
            .finish()
    }
}

/// Opens a stopped redb file and freezes one immutable correlated-index inspection session.
pub fn open_correlated_redb_inspection(
    database_path: &Path,
    startup_inputs: StartupValidationInputs,
    namespace: CorrelatedCorpusNamespace,
) -> Result<CorrelatedNamespaceInspectionSession, DurableInspectionError> {
    let metadata = match fs::metadata(database_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(DurableInspectionError::MissingDatabase);
        }
        Err(error) => return Err(DurableInspectionError::Io(error)),
    };
    if !metadata.is_file() || metadata.len() == 0 {
        return Err(DurableInspectionError::MissingDatabase);
    }
    let store = RedbStore::open(database_path).map_err(DurableInspectionError::Storage)?;
    open_correlated_opened_redb_inspection(store, startup_inputs, namespace)
}

/// Opens an already-constructed stopped [`RedbStore`] and freezes one immutable
/// correlated-index inspection session.
pub fn open_correlated_opened_redb_inspection(
    store: RedbStore,
    startup_inputs: StartupValidationInputs,
    namespace: CorrelatedCorpusNamespace,
) -> Result<CorrelatedNamespaceInspectionSession, DurableInspectionError> {
    let (open_session_id, retained_metadata, structural_findings, structural_pages, ports) =
        open_validated_with_session(store, startup_inputs)?;
    let database_id = retained_metadata.database_id();
    let (commits, application_frontier) = scan_all_commits(&ports)?;
    let administration = scan_all_administration(&ports)?;

    let live_entities = enumerate_namespace_entities(&ports, &namespace)?;
    let (live_index_entries, index_epochs) =
        enumerate_namespace_indexes_and_epochs(&ports, &namespace)?;
    let (sealed_targets, mut undeclared_targets) =
        enumerate_sealed_changelog_targets(&ports, &namespace)?;
    undeclared_targets.extend(enumerate_undeclared_snapshot_rows(
        &ports,
        &live_entities,
        &live_index_entries,
        &index_epochs,
    )?);
    drop(ports);

    Ok(CorrelatedNamespaceInspectionSession {
        database_id,
        open_session_id,
        retained_metadata,
        structural_findings,
        structural_pages,
        application_frontier,
        commits,
        administration,
        namespace,
        live_entities,
        live_index_entries,
        index_epochs,
        sealed_targets,
        undeclared_targets,
    })
}

impl CorrelatedNamespaceInspectionSession {
    /// Returns the validated database ID.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Returns the validated startup open-session ID.
    #[must_use]
    pub const fn open_session_id(&self) -> OpenSessionId {
        self.open_session_id
    }

    /// Borrows the retained startup metadata.
    #[must_use]
    pub const fn retained_metadata(&self) -> &RetainedMetadataV1 {
        &self.retained_metadata
    }

    /// Borrows structural findings from the startup pass.
    #[must_use]
    pub fn structural_findings(&self) -> &[StructuralFinding] {
        &self.structural_findings
    }

    /// Returns how many structural evidence pages were inspected.
    #[must_use]
    pub const fn structural_pages(&self) -> u32 {
        self.structural_pages
    }

    /// Returns the frozen recovered application frontier.
    #[must_use]
    pub const fn recovered_application_frontier(&self) -> FrontierPosition {
        self.application_frontier
    }

    /// Borrows the contiguous commit log frozen by this session.
    #[must_use]
    pub fn commits(&self) -> &[StoredCommitRecordV1] {
        &self.commits
    }

    /// Borrows the contiguous administration log frozen by this session.
    #[must_use]
    pub fn administration(&self) -> &[StoredAdministrationAuditRecordV1] {
        &self.administration
    }

    /// Borrows the bound corpus namespace.
    #[must_use]
    pub const fn namespace(&self) -> &CorrelatedCorpusNamespace {
        &self.namespace
    }

    /// Borrows the coordinator's sealed target inventory captured from committed
    /// V3 changelog receipts.
    #[must_use]
    pub const fn sealed_targets(&self) -> &BTreeSet<CorrelatedTargetKey> {
        &self.sealed_targets
    }

    /// Injects a synthetic redb-only undeclared target into the engine-side
    /// enumeration to prove comparison rejects engine-invented targets.
    pub fn inject_undeclared_target(
        &mut self,
        namespace: AuthoritativeNamespaceV1,
        raw_key: Vec<u8>,
    ) {
        self.undeclared_targets
            .insert(CorrelatedTargetKey::RedbOnlyUndeclared { namespace, raw_key });
    }

    /// Returns the complete strictly ordered target inventory enumerated from the
    /// stopped redb engine snapshot across the declared corpus namespace.
    pub fn engine_enumerated_targets(
        &self,
    ) -> Result<Vec<CorrelatedTargetKey>, DurableInspectionError> {
        let mut targets = BTreeSet::new();
        for target in self.live_entities.keys() {
            targets.insert(CorrelatedTargetKey::Entity(target.clone()));
        }
        for (key, (index_id, prefix_bytes, _)) in &self.live_index_entries {
            targets.insert(CorrelatedTargetKey::IndexEntry {
                index_id: *index_id,
                prefix_bytes: prefix_bytes.clone(),
                key: key.clone(),
            });
        }
        for (target, epoch) in &self.index_epochs {
            if *epoch != IndexEpochPosition::BeforeFirst {
                targets.insert(CorrelatedTargetKey::IndexEpoch {
                    target: target.clone(),
                    prefix_bytes: IndexRangePrefixBuilder::new(target.index_id())
                        .finish()
                        .as_bytes()
                        .to_vec(),
                });
            }
        }
        for sealed in &self.sealed_targets {
            targets.insert(sealed.clone());
        }
        for undeclared in &self.undeclared_targets {
            targets.insert(undeclared.clone());
        }
        if targets.len() > MAX_CORRELATED_INSPECTION_TOTAL_TARGETS {
            return Err(DurableInspectionError::TargetLimit);
        }
        Ok(targets.into_iter().collect())
    }

    /// Reads one strictly ordered canonical page of at most [`MAX_INSPECTION_TARGETS`] (`256`)
    /// observations from `comparison_universe`, bound to the immutable snapshot and continuation.
    pub fn read_page(
        &self,
        comparison_universe: &[CorrelatedTargetKey],
        continuation: Option<&CorrelatedInspectionContinuation>,
    ) -> Result<CorrelatedInspectionPage, DurableInspectionError> {
        if comparison_universe.is_empty()
            || comparison_universe.len() > MAX_CORRELATED_INSPECTION_TOTAL_TARGETS
        {
            return Err(DurableInspectionError::TargetLimit);
        }
        if comparison_universe
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        {
            return Err(DurableInspectionError::NonCanonicalTargets);
        }
        // Every engine-enumerated target in the stopped snapshot must belong to
        // `comparison_universe` so an engine-invented row cannot hide outside
        // model-selected point reads.
        for engine_target in self.engine_enumerated_targets()? {
            if comparison_universe.binary_search(&engine_target).is_err() {
                return Err(DurableInspectionError::NonCanonicalTargets);
            }
        }

        let (start_index, page_ordinal) = match continuation {
            None => (0usize, 0u16),
            Some(token) => {
                if token.database_id != self.database_id
                    || token.open_session_id != self.open_session_id
                    || token.application_frontier != self.application_frontier
                    || token.namespace != self.namespace
                    || token.page_ordinal == 0
                    || usize::from(token.page_ordinal) >= MAX_CORRELATED_INSPECTION_PAGES
                {
                    return Err(DurableInspectionError::StartupIdentityMismatch);
                }
                let position = comparison_universe
                    .binary_search(&token.exclusive_last_key)
                    .map_err(|_| DurableInspectionError::NonCanonicalPage)?;
                let next_index = position
                    .checked_add(1)
                    .ok_or(DurableInspectionError::NonCanonicalPage)?;
                if next_index >= comparison_universe.len() {
                    return Err(DurableInspectionError::NonCanonicalPage);
                }
                (next_index, token.page_ordinal)
            }
        };

        let end_index = start_index
            .saturating_add(MAX_INSPECTION_TARGETS)
            .min(comparison_universe.len());
        let slice = &comparison_universe[start_index..end_index];
        let mut observations = Vec::with_capacity(slice.len());
        for target_key in slice {
            let observation = match target_key {
                CorrelatedTargetKey::Entity(target) => CorrelatedTargetObservation::Entity {
                    target: target.clone(),
                    record: self.live_entities.get(target).cloned(),
                },
                CorrelatedTargetKey::IndexEntry {
                    index_id,
                    prefix_bytes,
                    key,
                } => CorrelatedTargetObservation::IndexEntry {
                    index_id: *index_id,
                    prefix_bytes: prefix_bytes.clone(),
                    key: key.clone(),
                    entry: self.live_index_entries.get(key).and_then(
                        |(stored_index_id, stored_prefix, entry)| {
                            (*stored_index_id == *index_id && stored_prefix == prefix_bytes)
                                .then(|| entry.clone())
                        },
                    ),
                },
                CorrelatedTargetKey::IndexEpoch {
                    target,
                    prefix_bytes,
                } => CorrelatedTargetObservation::IndexEpoch {
                    target: target.clone(),
                    prefix_bytes: prefix_bytes.clone(),
                    epoch: self
                        .index_epochs
                        .get(target)
                        .copied()
                        .unwrap_or(IndexEpochPosition::BeforeFirst),
                },
                CorrelatedTargetKey::RedbOnlyUndeclared { namespace, raw_key } => {
                    CorrelatedTargetObservation::RedbOnlyUndeclared {
                        namespace: *namespace,
                        raw_key: raw_key.clone(),
                    }
                }
            };
            observations.push(observation);
        }

        let next = if end_index < comparison_universe.len() {
            let next_ordinal = page_ordinal
                .checked_add(1)
                .filter(|ordinal| usize::from(*ordinal) < MAX_CORRELATED_INSPECTION_PAGES)
                .ok_or(DurableInspectionError::TargetLimit)?;
            let last_key = observations
                .last()
                .ok_or(DurableInspectionError::NonCanonicalPage)?
                .key();
            Some(CorrelatedInspectionContinuation {
                database_id: self.database_id,
                open_session_id: self.open_session_id,
                application_frontier: self.application_frontier,
                namespace: self.namespace.clone(),
                exclusive_last_key: last_key,
                page_ordinal: next_ordinal,
            })
        } else {
            None
        };

        CorrelatedInspectionPage::new(page_ordinal, observations, next)
    }
}

fn enumerate_namespace_entities(
    ports: &RedbOperationalPorts,
    namespace: &CorrelatedCorpusNamespace,
) -> Result<BTreeMap<EntityTarget, StoredEntityRecordV1>, DurableInspectionError> {
    let limit = StorageScanLimit::new(PAGE_LIMIT).ok_or(DurableInspectionError::RecordLimit)?;
    let mut entities = BTreeMap::new();
    let mut frozen_head = None;

    for (entity_type, prefix) in namespace.entity_prefixes() {
        let mut after = None;
        loop {
            let request = AuthoritativeEntityPartitionScanRequest::new(
                *entity_type,
                prefix.clone(),
                after,
                limit,
            )
            .map_err(|_| DurableInspectionError::NonCanonicalTargets)?;
            let page = ports
                .scan_entity_partition(request)
                .map_err(DurableInspectionError::Storage)?;
            match frozen_head {
                None => frozen_head = Some(page.application_head()),
                Some(head) if head == page.application_head() => {}
                Some(_) => return Err(DurableInspectionError::NonCanonicalPage),
            }
            for item in page.records() {
                if entities.len() == MAX_INSPECTED_ORDERED_RECORDS {
                    return Err(DurableInspectionError::RecordLimit);
                }
                let record = item.value().clone();
                if entities.insert(record.target().clone(), record).is_some() {
                    return Err(DurableInspectionError::NonCanonicalPage);
                }
            }
            match page.next_after() {
                Some(next_after) => after = Some(next_after.clone()),
                None => break,
            }
        }
    }
    Ok(entities)
}

#[allow(clippy::type_complexity)]
fn enumerate_namespace_indexes_and_epochs(
    ports: &RedbOperationalPorts,
    namespace: &CorrelatedCorpusNamespace,
) -> Result<
    (
        BTreeMap<IndexEntryKey, (IndexId, Vec<u8>, StoredIndexEntryV2)>,
        BTreeMap<PartitionIndexTarget, IndexEpochPosition>,
    ),
    DurableInspectionError,
> {
    let mut entries = BTreeMap::new();
    let mut epochs = BTreeMap::new();

    for index_id in namespace.index_ids() {
        let prefix = IndexRangePrefixBuilder::new(*index_id).finish();
        let target = IndexRangeTarget::new(namespace.partition_key().clone(), prefix);
        let selection = IndexRangeSelection::new(target, namespace.lineage().clone());
        let inspected = scan_full_index_range(ports, &selection)?;
        let generation_target =
            PartitionIndexTarget::new(namespace.partition_key().clone(), *index_id);
        epochs.insert(generation_target, inspected.epoch());
        for entry in inspected.entries() {
            if entries.len() == MAX_INSPECTED_ORDERED_RECORDS {
                return Err(DurableInspectionError::RecordLimit);
            }
            let prefix_bytes = extract_index_entry_prefix_bytes(entry.key())
                .ok_or(DurableInspectionError::NonCanonicalPage)?;
            if entries
                .insert(
                    entry.key().clone(),
                    (*index_id, prefix_bytes, entry.clone()),
                )
                .is_some()
            {
                return Err(DurableInspectionError::NonCanonicalPage);
            }
        }
    }
    Ok((entries, epochs))
}

/// Extracts the exact canonical index prefix bytes (everything before the
/// trailing length-delimited entity key) from a complete [`IndexEntryKey`].
pub fn extract_index_entry_prefix_bytes(key: &IndexEntryKey) -> Option<Vec<u8>> {
    let bytes = key.as_bytes();
    // An IndexEntryKey starts with 6-byte INDEX_ENTRY_KEY_V1_PREFIX + index_id
    // and ends with a 4-byte big-endian length + entity_key (which starts with
    // `[0x45, 0x01, ...]`). Scan backwards for the trailing length-delimited
    // entity key.
    if bytes.len() < 16 {
        return None;
    }
    for split in 6..bytes.len().saturating_sub(10) {
        let len_bytes: [u8; 4] = bytes.get(split..split + 4)?.try_into().ok()?;
        let trailing_len = usize::try_from(u32::from_be_bytes(len_bytes)).ok()?;
        if split + 4 + trailing_len == bytes.len()
            && bytes.get(split + 4..split + 6) == Some(&[0x45, 0x01])
        {
            return Some(bytes[..split].to_vec());
        }
    }
    None
}

fn enumerate_undeclared_snapshot_rows(
    ports: &RedbOperationalPorts,
    live_entities: &BTreeMap<EntityTarget, StoredEntityRecordV1>,
    live_index_entries: &BTreeMap<IndexEntryKey, (IndexId, Vec<u8>, StoredIndexEntryV2)>,
    index_epochs: &BTreeMap<PartitionIndexTarget, IndexEpochPosition>,
) -> Result<BTreeSet<CorrelatedTargetKey>, DurableInspectionError> {
    let snapshot = ports
        .published_changelog_snapshot_v3()
        .map_err(DurableInspectionError::Storage)?;
    let mut cursor = snapshot
        .authoritative_state_v3()
        .map_err(|_| DurableInspectionError::NonCanonicalPage)?;

    let known_entity_keys = live_entities
        .keys()
        .map(|target| target.key().as_bytes().to_vec())
        .collect::<BTreeSet<_>>();
    let known_index_keys = live_index_entries
        .keys()
        .map(|key| key.as_bytes().to_vec())
        .collect::<BTreeSet<_>>();
    let known_epoch_keys = index_epochs
        .iter()
        .filter(|(_, epoch)| **epoch != IndexEpochPosition::BeforeFirst)
        .map(|(target, _)| target.to_key_bytes())
        .collect::<BTreeSet<_>>();

    let mut undeclared = BTreeSet::new();
    let mut rows_seen = Vec::new();
    while let Some(step) = cursor
        .next_item()
        .map_err(|_| DurableInspectionError::NonCanonicalPage)?
    {
        if let AuthoritativeStateStepV3::Row(row) = step {
            append_bounded(&mut rows_seen, [()])?;
            let (namespace, key, _) = row.into_parts();
            let raw_key = key.into_vec();
            let expected = match namespace {
                AuthoritativeNamespaceV1::Entities => known_entity_keys.contains(&raw_key),
                AuthoritativeNamespaceV1::SecondaryIndexes => known_index_keys.contains(&raw_key),
                AuthoritativeNamespaceV1::IndexEpochs => known_epoch_keys.contains(&raw_key),
                _ => true,
            };
            if !expected {
                undeclared.insert(CorrelatedTargetKey::RedbOnlyUndeclared { namespace, raw_key });
            }
        }
    }
    Ok(undeclared)
}

#[allow(clippy::type_complexity)]
fn enumerate_sealed_changelog_targets(
    ports: &RedbOperationalPorts,
    namespace: &CorrelatedCorpusNamespace,
) -> Result<(BTreeSet<CorrelatedTargetKey>, BTreeSet<CorrelatedTargetKey>), DurableInspectionError>
{
    let snapshot = ports
        .published_changelog_snapshot_v3()
        .map_err(DurableInspectionError::Storage)?;
    let progress = snapshot
        .replication_source_progress_v3()
        .map_err(|_| DurableInspectionError::NonCanonicalPage)?;
    let history = progress.history();
    let mut cursor = snapshot
        .changelog_receipts_v3(history.lineage(), history.minimum_resume())
        .map_err(|_| DurableInspectionError::NonCanonicalPage)?;

    let declared_epoch_map = namespace
        .index_ids()
        .iter()
        .map(|index_id| {
            let target = PartitionIndexTarget::new(namespace.partition_key().clone(), *index_id);
            let prefix_bytes = IndexRangePrefixBuilder::new(*index_id)
                .finish()
                .as_bytes()
                .to_vec();
            (target.to_key_bytes(), (target, prefix_bytes))
        })
        .collect::<BTreeMap<_, _>>();

    let mut sealed = BTreeSet::new();
    let mut undeclared = BTreeSet::new();
    let mut receipts_seen = Vec::new();

    while let Some(receipt) = cursor
        .next_receipt()
        .map_err(|_| DurableInspectionError::NonCanonicalPage)?
    {
        append_bounded(&mut receipts_seen, [()])?;
        if !matches!(
            receipt.attribution(),
            ChangelogAttributionV3::JournaledApplicationGroup
                | ChangelogAttributionV3::DirectApplicationOrServiceAuditGroup
        ) {
            continue;
        }
        let binding = receipt.binding();
        let app_advanced = binding
            .covered_frontier
            .application()
            .map_or(0, |v| v.get())
            > binding
                .predecessor_frontier
                .application()
                .map_or(0, |v| v.get());
        if !app_advanced {
            continue;
        }
        for mutation in receipt.mutations() {
            match mutation.namespace() {
                AuthoritativeNamespaceV1::Entities => {
                    let raw_key = mutation.key().to_vec();
                    let entity_ok = EntityKey::from_bytes(raw_key.clone())
                        .ok()
                        .and_then(|key| EntityTarget::new(key.entity_type_id(), key).ok())
                        .filter(|target| {
                            namespace.entity_prefixes().iter().any(|(etype, prefix)| {
                                *etype == target.entity_type_id()
                                    && target.key().as_bytes().starts_with(prefix.as_slice())
                            })
                        });
                    match entity_ok {
                        Some(target) => {
                            sealed.insert(CorrelatedTargetKey::Entity(target));
                        }
                        None => {
                            undeclared.insert(CorrelatedTargetKey::RedbOnlyUndeclared {
                                namespace: AuthoritativeNamespaceV1::Entities,
                                raw_key,
                            });
                        }
                    }
                }
                AuthoritativeNamespaceV1::SecondaryIndexes => {
                    let raw_key = mutation.key().to_vec();
                    let index_ok = IndexEntryKey::from_bytes(raw_key.clone())
                        .ok()
                        .and_then(|key| {
                            let index_id = key.index_id();
                            namespace
                                .index_ids()
                                .contains(&index_id)
                                .then_some((index_id, key))
                        })
                        .and_then(|(index_id, key)| {
                            let prefix_bytes = extract_index_entry_prefix_bytes(&key)?;
                            Some(CorrelatedTargetKey::IndexEntry {
                                index_id,
                                prefix_bytes,
                                key,
                            })
                        });
                    match index_ok {
                        Some(target_key) => {
                            sealed.insert(target_key);
                        }
                        None => {
                            undeclared.insert(CorrelatedTargetKey::RedbOnlyUndeclared {
                                namespace: AuthoritativeNamespaceV1::SecondaryIndexes,
                                raw_key,
                            });
                        }
                    }
                }
                AuthoritativeNamespaceV1::IndexEpochs => {
                    let raw_key = mutation.key().to_vec();
                    match declared_epoch_map.get(&raw_key) {
                        Some((target, prefix_bytes)) => {
                            sealed.insert(CorrelatedTargetKey::IndexEpoch {
                                target: target.clone(),
                                prefix_bytes: prefix_bytes.clone(),
                            });
                        }
                        None => {
                            undeclared.insert(CorrelatedTargetKey::RedbOnlyUndeclared {
                                namespace: AuthoritativeNamespaceV1::IndexEpochs,
                                raw_key,
                            });
                        }
                    }
                }
                _ => {}
            }
        }
    }
    Ok((sealed, undeclared))
}
