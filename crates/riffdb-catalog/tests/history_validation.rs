//! Exact-end same-session historical catalog validation.

use std::collections::VecDeque;
use std::num::{NonZeroU16, NonZeroU32};

use riffdb_catalog::{
    CatalogErrorKind, CatalogHistoryOutcome, CatalogHistoryValidation,
    CatalogIndexMigrationApplied, CatalogIndexMigrationBackend, CatalogIndexMigrationBundleRequest,
    CatalogIndexMigrationBundleResponse, CatalogIndexMigrationCompletion,
    CatalogIndexMigrationContext, CatalogIndexMigrationDriveError, CatalogIndexMigrationDriver,
    CatalogIndexMigrationInstruction, CatalogIndexMigrationPendingBatch, CatalogIndexMigrationScan,
    CatalogIndexMigrationScanRequest, ValidatedCatalogHistory, ValidatedContractBundle,
    validate_catalog_history,
};
use riffdb_contract_compiler::{compile_contract_source, compile_contract_successor};
use riffdb_contract_ir::{CompatibilityReport, ContractBundle, ParentBundleRef};
use riffdb_storage_api::{
    CapabilityGrantV1, CapabilityPermissionKindV1, CapabilityPermissionV1, CapabilityPermissionsV1,
    CapabilityRequestedRecordV1, DormantPortBundle, DurableKeySchemaBindingV1, EntityTarget,
    EvidencePageLimit, HistoricalActiveCatalogEvidence, HistoricalBundleBytes,
    HistoricalBundleEvidence, HistoricalCapabilityPartitionEvidenceV1, HistoricalEvidenceCursor,
    HistoricalEvidenceEnd, HistoricalEvidencePage, HistoricalPersistedKeyEvidenceV1,
    HistoricalSemanticEvidence, IndexMigrationCursor, IndexMigrationRowEvidence,
    LegacyStoredIndexEpochV1, OpenSessionId, PartitionScopeV1, StartupIndexMigrationPort,
    StorageError, StorageErrorKind, StorageValueError, StoredCapabilityRecordV1,
    StoredEntityRecordV1, StoredIndexEntryV1, StoredIndexEntryV2, StructuralEvidenceCursor,
    StructuralEvidenceEnd, StructuralEvidencePage, StructuralEvidenceSession,
    StructuralOpenOutcome, StructurallyDecodedIndexRangePrefixV1, UniqueIndexTarget,
    UniqueOccupancyKind,
};
use riffdb_types::{
    ActorId, ActorKind, AdministrationSequence, AggregateTypeId, Audience, CanonicalRecord,
    CanonicalValue, CapabilityId, CapabilityTokenDigest, ContractBundleHash, ContractLineage,
    ContractVersion, DatabaseId, DigestKeyId, EntityKey, EntityVersion, Environment, IndexEpoch,
    PartitionKey, PartitionKeyBuilder, PlanHash, RequestId, ScopedPartitionV1, TenantScope,
    Timestamp,
};

const BUDGET: &str = include_str!("../../../contracts/examples/budget.riff");

const CAPABILITY_KEYS: &str = r#"
contract CapabilityKeys version 1 {
  entity TextRow { key (id: string<4>) }
  aggregate TextRows { root TextRow partition_by id conflict_key (id) }
}
"#;

const UNIQUE_RECOVERY: &str = r#"
contract UniqueRecovery version 1 {
  entity Organization {
    key (organization_id: u64)
  }
  entity User {
    key (organization_id: u64, user_id: u64)
    field email: string<64>
    unique user_email (organization_id, email)
  }
  aggregate Organizations {
    root Organization
    child User
    partition_by organization_id
    conflict_key (organization_id)
  }
}
"#;

#[derive(Clone, Copy)]
enum CursorFault {
    None,
    Skip,
    WrongStart,
    WrongEnd,
    RawRepeatEmpty,
    RawNonAdvancing,
    RawOversized,
}

struct FakeHistoricalEnd(HistoricalEvidenceCursor);

impl HistoricalEvidenceEnd for FakeHistoricalEnd {
    fn cursor(&self) -> HistoricalEvidenceCursor {
        self.0
    }
}

struct FakeStructuralEnd(StructuralEvidenceCursor);

impl StructuralEvidenceEnd for FakeStructuralEnd {
    fn cursor(&self) -> StructuralEvidenceCursor {
        self.0
    }
}

struct FakeMigrationPort {
    database_id: DatabaseId,
    open_session_id: OpenSessionId,
}

impl StartupIndexMigrationPort for FakeMigrationPort {
    fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    fn open_session_id(&self) -> OpenSessionId {
        self.open_session_id
    }
}

struct FakeDormantPorts;

impl DormantPortBundle for FakeDormantPorts {
    type CompletionAuthority = ();
}

struct FakeSession {
    database_id: DatabaseId,
    open_session_id: OpenSessionId,
    pages: Vec<Option<Vec<HistoricalSemanticEvidence>>>,
    page: usize,
    bundles: Vec<HistoricalBundleEvidence>,
    entities: Vec<StoredEntityRecordV1>,
    unique_state: FakeUniqueState,
    fault: CursorFault,
}

#[derive(Clone, Copy)]
enum FakeUniqueState {
    Vacant,
    Owned,
    Conflict,
    Corrupt,
}

impl StructuralEvidenceSession for FakeSession {
    type DormantPorts = FakeDormantPorts;
    type StructuralEnd = FakeStructuralEnd;
    type HistoricalEnd = FakeHistoricalEnd;
    type MigrationPort = FakeMigrationPort;

    fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    fn open_session_id(&self) -> OpenSessionId {
        self.open_session_id
    }

    fn read_structural_evidence(
        &mut self,
        _cursor: StructuralEvidenceCursor,
        _limit: EvidencePageLimit,
    ) -> Result<StructuralEvidencePage<Self::StructuralEnd>, StorageError> {
        Err(storage_error(StorageErrorKind::InvariantViolation))
    }

    fn read_historical_evidence(
        &mut self,
        cursor: HistoricalEvidenceCursor,
        limit: EvidencePageLimit,
    ) -> Result<HistoricalEvidencePage<Self::HistoricalEnd>, StorageError> {
        if matches!(self.fault, CursorFault::RawRepeatEmpty) {
            assert_eq!(self.page, 0, "validator reread a non-advancing page");
            self.page += 1;
            return Ok(HistoricalEvidencePage::Page {
                start: cursor,
                evidence: Vec::new(),
                next: cursor,
            });
        }

        let Some(evidence) = self.pages.get_mut(self.page).and_then(Option::take) else {
            let end = if matches!(self.fault, CursorFault::WrongEnd) {
                cursor
                    .advanced(1)
                    .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
            } else {
                cursor
            };
            return Ok(HistoricalEvidencePage::ExactEnd(FakeHistoricalEnd(end)));
        };
        self.page += 1;

        let amount = u64::try_from(evidence.len())
            .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
        let start = if self.page == 1 && matches!(self.fault, CursorFault::WrongStart) {
            HistoricalEvidenceCursor::start(
                self.database_id,
                OpenSessionId::new(999).expect("session"),
            )
        } else {
            cursor
        };
        let advance = if self.page == 1 && matches!(self.fault, CursorFault::Skip) {
            amount + 1
        } else {
            amount
        };
        let next = start
            .advanced(advance)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        if matches!(self.fault, CursorFault::RawNonAdvancing) {
            return Ok(HistoricalEvidencePage::Page {
                start,
                evidence,
                next: start,
            });
        }
        if matches!(self.fault, CursorFault::RawOversized) && evidence.len() > limit.get() as usize
        {
            return Ok(HistoricalEvidencePage::Page {
                start,
                evidence,
                next,
            });
        }
        HistoricalEvidencePage::page(start, evidence, next)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))
    }

    fn read_historical_bundle(
        &mut self,
        lineage: &ContractLineage,
        contract_version: ContractVersion,
        bundle_hash: ContractBundleHash,
    ) -> Result<Option<HistoricalBundleEvidence>, StorageError> {
        Ok(self
            .bundles
            .iter()
            .find(|bundle| {
                bundle.lineage() == lineage
                    && bundle.version() == contract_version
                    && bundle.bundle_hash() == bundle_hash
            })
            .cloned())
    }

    fn read_integrity_entity(
        &mut self,
        target: &EntityTarget,
    ) -> Result<Option<StoredEntityRecordV1>, StorageError> {
        Ok(self
            .entities
            .iter()
            .find(|record| record.target() == target)
            .cloned())
    }

    fn read_integrity_unique_occupancy(
        &mut self,
        _target: &UniqueIndexTarget,
    ) -> Result<UniqueOccupancyKind, StorageError> {
        match self.unique_state {
            FakeUniqueState::Vacant => Ok(UniqueOccupancyKind::Vacant),
            FakeUniqueState::Owned => Ok(UniqueOccupancyKind::Owned),
            FakeUniqueState::Conflict => Ok(UniqueOccupancyKind::Conflict),
            FakeUniqueState::Corrupt => Err(storage_error(StorageErrorKind::CorruptData)),
        }
    }

    fn finish(
        self,
        _structural_end: Self::StructuralEnd,
        _historical_end: Self::HistoricalEnd,
    ) -> Result<StructuralOpenOutcome<Self::DormantPorts, Self::MigrationPort>, StorageError> {
        Err(storage_error(StorageErrorKind::InvariantViolation))
    }
}

fn database() -> DatabaseId {
    DatabaseId::from_bytes([0x01, 0x8f, 0, 0, 0, 0, 0x70, 1, 0x80, 2, 0, 0, 0, 0, 0, 1])
        .expect("database")
}

fn stored_bundle(bundle: &ValidatedContractBundle) -> HistoricalBundleEvidence {
    HistoricalBundleEvidence::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
        HistoricalBundleBytes::new(bundle.bundle().canonical_bytes().to_vec()).expect("bytes"),
    )
}

fn active(bundle: &ValidatedContractBundle) -> HistoricalSemanticEvidence {
    HistoricalSemanticEvidence::ActiveCatalog(Some(HistoricalActiveCatalogEvidence::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
    )))
}

fn session(
    pages: Vec<Vec<HistoricalSemanticEvidence>>,
    bundles: Vec<HistoricalBundleEvidence>,
    fault: CursorFault,
) -> FakeSession {
    FakeSession {
        database_id: database(),
        open_session_id: OpenSessionId::new(7).expect("session"),
        pages: pages.into_iter().map(Some).collect(),
        page: 0,
        bundles,
        entities: Vec::new(),
        unique_state: FakeUniqueState::Vacant,
        fault,
    }
}

fn ready_history<E>(validation: &CatalogHistoryValidation<E>) -> &ValidatedCatalogHistory {
    match validation.outcome() {
        CatalogHistoryOutcome::Ready(history) => history,
        CatalogHistoryOutcome::MigrationRequired(_) => {
            panic!("fixture unexpectedly requires index migration")
        }
    }
}

fn storage_error(kind: StorageErrorKind) -> StorageError {
    StorageError::new(kind, None)
}

fn uuid_v7(seed: u8) -> [u8; 16] {
    let mut bytes = [0; 16];
    bytes[..10].copy_from_slice(&[0x01, 0x8f, 0, 0, 0, 0, 0x70, 1, 0x80, 2]);
    bytes[15] = seed;
    bytes
}

fn capability_partition_evidence(
    capability_seed: u8,
    scope: PartitionScopeV1,
    entry_ordinal: usize,
) -> HistoricalCapabilityPartitionEvidenceV1 {
    let permissions = CapabilityPermissionsV1::new(vec![
        CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::AdministerCapabilities)
            .expect("permission shape"),
    ])
    .expect("permissions");
    let grant = CapabilityGrantV1::new(
        TenantScope::Global,
        scope,
        permissions,
        Vec::new(),
        NonZeroU16::MIN,
        Vec::new(),
    )
    .expect("grant");
    let requested = CapabilityRequestedRecordV1::new(
        database(),
        Environment::new("test").expect("environment"),
        ActorId::new("operator").expect("actor"),
        ActorKind::Human,
        NonZeroU32::new(60).expect("duration"),
        vec![Audience::new("riffdb-test").expect("audience")],
        grant,
    )
    .expect("requested capability");
    let capability = StoredCapabilityRecordV1::active(
        CapabilityId::from_bytes(uuid_v7(capability_seed)).expect("capability UUIDv7"),
        CapabilityTokenDigest::from_hmac_bytes(
            DigestKeyId::new(1).expect("digest key"),
            [capability_seed; 32],
        ),
        requested,
        Timestamp::new(10, 0).expect("issued at"),
        Timestamp::new(70, 0).expect("expires at"),
        AdministrationSequence::first(),
        RequestId::from_bytes(uuid_v7(capability_seed.wrapping_add(0x40))).expect("request UUIDv7"),
    )
    .expect("stored capability");
    HistoricalCapabilityPartitionEvidenceV1::from_capability_entry(&capability, entry_ordinal)
        .expect("capability partition evidence")
}

fn explicit_partition(lineage: ContractLineage, key: PartitionKey) -> PartitionScopeV1 {
    PartitionScopeV1::explicit(vec![ScopedPartitionV1::new(lineage, key)])
        .expect("one explicit partition")
}

#[test]
fn complete_history_resolves_every_plan_and_produces_a_session_bound_proof() {
    let bundle = ValidatedContractBundle::from_compiler_bundle(
        compile_contract_source(BUDGET).expect("budget"),
    )
    .expect("validated");
    let stored = stored_bundle(&bundle);
    let command = bundle.bundle().commands().first().expect("command");
    let plan = riffdb_storage_api::ExecutablePlanRef::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
        command.command_id(),
        command.plan_hash(),
    );
    let mut session = session(
        vec![
            vec![HistoricalSemanticEvidence::Bundle(stored.clone())],
            vec![HistoricalSemanticEvidence::PlanReference(plan)],
            vec![active(&bundle)],
        ],
        vec![stored],
        CursorFault::None,
    );

    let validation = validate_catalog_history(&mut session).expect("complete history");
    assert!(
        ready_history(&validation).matches(database(), OpenSessionId::new(7).expect("session"))
    );
    assert_eq!(ready_history(&validation).evidence_count(), 3);
    assert_eq!(
        ready_history(&validation)
            .active()
            .expect("active")
            .bundle_hash(),
        bundle.bundle_hash()
    );
}

#[test]
fn initialized_but_undeployed_catalog_has_an_exact_empty_history() {
    let mut session = session(
        vec![vec![HistoricalSemanticEvidence::ActiveCatalog(None)]],
        Vec::new(),
        CursorFault::None,
    );

    let validation = validate_catalog_history(&mut session).expect("empty exact history");
    assert!(ready_history(&validation).active().is_none());
    assert_eq!(ready_history(&validation).evidence_count(), 1);
}

#[test]
fn qualifying_capability_partitions_require_the_active_bundle_schema() {
    let bundle = ValidatedContractBundle::from_compiler_bundle(
        compile_contract_source(CAPABILITY_KEYS).expect("capability-key contract"),
    )
    .expect("validated");
    let stored = stored_bundle(&bundle);
    let aggregate = bundle
        .bundle()
        .schema()
        .aggregates()
        .first()
        .expect("aggregate");
    let key = aggregate
        .keys()
        .partition_schema()
        .encode_partition(&[CanonicalValue::string("key").expect("bounded text")])
        .expect("complete partition key");
    let evidence =
        capability_partition_evidence(0x21, explicit_partition(bundle.lineage().clone(), key), 0);
    let mut valid = session(
        vec![
            vec![HistoricalSemanticEvidence::Bundle(stored.clone())],
            vec![active(&bundle)],
            vec![HistoricalSemanticEvidence::CapabilityPartition(evidence)],
        ],
        vec![stored],
        CursorFault::None,
    );

    let validation = validate_catalog_history(&mut valid).expect("schema-valid capability key");
    assert_eq!(ready_history(&validation).evidence_count(), 3);
}

#[test]
fn invalid_capability_partition_semantics_fail_as_invalid_history() {
    let bundle = ValidatedContractBundle::from_compiler_bundle(
        compile_contract_source(CAPABILITY_KEYS).expect("capability-key contract"),
    )
    .expect("validated");
    let stored = stored_bundle(&bundle);
    let aggregate = bundle
        .bundle()
        .schema()
        .aggregates()
        .first()
        .expect("aggregate");
    let valid_key = aggregate
        .keys()
        .partition_schema()
        .encode_partition(&[CanonicalValue::string("key").expect("bounded text")])
        .expect("complete partition key");

    let mut unknown_owner =
        PartitionKeyBuilder::new(AggregateTypeId::new(u32::MAX).expect("nonzero aggregate ID"));
    unknown_owner.push_str("key").expect("bounded component");

    let mut invalid_utf8 = PartitionKeyBuilder::new(aggregate.id());
    invalid_utf8
        .push_bytes(&[0xff])
        .expect("structurally bounded component");

    let incomplete = PartitionKeyBuilder::new(aggregate.id())
        .finish()
        .expect("envelope-only key");

    let mut trailing = PartitionKeyBuilder::new(aggregate.id());
    trailing.push_str("key").expect("bounded component");
    trailing.push_bool(true).expect("bounded trailing value");

    let candidates = [
        explicit_partition(
            ContractLineage::new("Foreign").expect("foreign lineage"),
            valid_key,
        ),
        explicit_partition(
            bundle.lineage().clone(),
            unknown_owner.finish().expect("unknown-owner envelope"),
        ),
        explicit_partition(
            bundle.lineage().clone(),
            invalid_utf8.finish().expect("invalid UTF-8 envelope"),
        ),
        explicit_partition(bundle.lineage().clone(), incomplete),
        explicit_partition(
            bundle.lineage().clone(),
            trailing.finish().expect("trailing envelope"),
        ),
    ];

    for (index, scope) in candidates.into_iter().enumerate() {
        let evidence = capability_partition_evidence(
            u8::try_from(index + 0x30).expect("bounded seed"),
            scope,
            0,
        );
        let mut invalid = session(
            vec![
                vec![HistoricalSemanticEvidence::Bundle(stored.clone())],
                vec![active(&bundle)],
                vec![HistoricalSemanticEvidence::CapabilityPartition(evidence)],
            ],
            vec![stored.clone()],
            CursorFault::None,
        );
        assert_eq!(
            validate_catalog_history(&mut invalid)
                .err()
                .expect("invalid capability partition")
                .kind(),
            CatalogErrorKind::InvalidHistoricalEvidence
        );
    }
}

#[test]
fn capability_partition_without_an_active_bundle_fails_closed() {
    let bundle = ValidatedContractBundle::from_compiler_bundle(
        compile_contract_source(CAPABILITY_KEYS).expect("capability-key contract"),
    )
    .expect("validated");
    let stored = stored_bundle(&bundle);
    let aggregate = bundle
        .bundle()
        .schema()
        .aggregates()
        .first()
        .expect("aggregate");
    let key = aggregate
        .keys()
        .partition_schema()
        .encode_partition(&[CanonicalValue::string("key").expect("bounded text")])
        .expect("complete partition key");
    let evidence =
        capability_partition_evidence(0x41, explicit_partition(bundle.lineage().clone(), key), 0);
    let mut missing_active = session(
        vec![
            vec![HistoricalSemanticEvidence::Bundle(stored.clone())],
            vec![HistoricalSemanticEvidence::ActiveCatalog(None)],
            vec![HistoricalSemanticEvidence::CapabilityPartition(evidence)],
        ],
        vec![stored],
        CursorFault::None,
    );

    assert_eq!(
        validate_catalog_history(&mut missing_active)
            .err()
            .expect("capability partition requires active bundle")
            .kind(),
        CatalogErrorKind::InvalidHistoricalEvidence
    );
}

#[test]
fn duplicate_reordered_and_nonexact_capability_evidence_fails_closed() {
    let bundle = ValidatedContractBundle::from_compiler_bundle(
        compile_contract_source(CAPABILITY_KEYS).expect("capability-key contract"),
    )
    .expect("validated");
    let stored = stored_bundle(&bundle);
    let aggregate = bundle
        .bundle()
        .schema()
        .aggregates()
        .first()
        .expect("aggregate");
    let key = aggregate
        .keys()
        .partition_schema()
        .encode_partition(&[CanonicalValue::string("key").expect("bounded text")])
        .expect("complete partition key");
    let scope = explicit_partition(bundle.lineage().clone(), key);
    let lower = || {
        HistoricalSemanticEvidence::CapabilityPartition(capability_partition_evidence(
            0x51,
            scope.clone(),
            0,
        ))
    };
    let higher = HistoricalSemanticEvidence::CapabilityPartition(capability_partition_evidence(
        0x52,
        scope.clone(),
        0,
    ));

    let cases = [
        session(
            vec![
                vec![HistoricalSemanticEvidence::Bundle(stored.clone())],
                vec![active(&bundle)],
                vec![lower()],
                vec![lower()],
            ],
            vec![stored.clone()],
            CursorFault::None,
        ),
        session(
            vec![
                vec![HistoricalSemanticEvidence::Bundle(stored.clone())],
                vec![active(&bundle)],
                vec![higher],
                vec![lower()],
            ],
            vec![stored.clone()],
            CursorFault::None,
        ),
        session(
            vec![
                vec![HistoricalSemanticEvidence::Bundle(stored.clone())],
                vec![active(&bundle)],
                vec![HistoricalSemanticEvidence::CapabilityPartition(
                    capability_partition_evidence(
                        0x53,
                        explicit_partition(
                            bundle.lineage().clone(),
                            aggregate
                                .keys()
                                .partition_schema()
                                .encode_partition(&[
                                    CanonicalValue::string("key").expect("bounded text")
                                ])
                                .expect("complete partition key"),
                        ),
                        0,
                    ),
                )],
            ],
            vec![stored.clone()],
            CursorFault::WrongEnd,
        ),
    ];

    for mut invalid in cases {
        assert_eq!(
            validate_catalog_history(&mut invalid)
                .err()
                .expect("invalid capability evidence stream")
                .kind(),
            CatalogErrorKind::InvalidHistoricalEvidence
        );
    }
}

#[test]
fn skipped_repeated_reordered_cross_session_and_truncated_streams_fail_closed() {
    let bundle = ValidatedContractBundle::from_compiler_bundle(
        compile_contract_source(BUDGET).expect("budget"),
    )
    .expect("validated");
    let stored = stored_bundle(&bundle);
    let bundle_item = || HistoricalSemanticEvidence::Bundle(stored.clone());
    let active_item = || active(&bundle);

    let cases = [
        session(
            vec![vec![bundle_item()], vec![active_item()]],
            vec![stored.clone()],
            CursorFault::Skip,
        ),
        session(
            vec![
                vec![bundle_item()],
                vec![bundle_item()],
                vec![active_item()],
            ],
            vec![stored.clone()],
            CursorFault::None,
        ),
        session(
            vec![vec![active_item()], vec![bundle_item()]],
            vec![stored.clone()],
            CursorFault::None,
        ),
        session(
            vec![vec![bundle_item()], vec![active_item()]],
            vec![stored.clone()],
            CursorFault::WrongStart,
        ),
        session(
            vec![vec![bundle_item()], vec![active_item()]],
            vec![stored.clone()],
            CursorFault::WrongEnd,
        ),
        session(vec![vec![bundle_item()]], vec![stored], CursorFault::None),
    ];

    for mut invalid in cases {
        let error = validate_catalog_history(&mut invalid)
            .err()
            .expect("invalid sequence");
        assert_eq!(error.kind(), CatalogErrorKind::InvalidHistoricalEvidence);
    }
}

#[test]
fn raw_empty_nonadvancing_and_oversized_pages_fail_closed() {
    let mut repeat_empty = session(Vec::new(), Vec::new(), CursorFault::RawRepeatEmpty);
    assert_eq!(
        validate_catalog_history(&mut repeat_empty)
            .err()
            .expect("empty non-advancing page")
            .kind(),
        CatalogErrorKind::InvalidHistoricalEvidence
    );
    assert_eq!(repeat_empty.page, 1, "invalid page must not be reread");

    let mut nonadvancing = session(
        vec![vec![HistoricalSemanticEvidence::ActiveCatalog(None)]],
        Vec::new(),
        CursorFault::RawNonAdvancing,
    );
    assert_eq!(
        validate_catalog_history(&mut nonadvancing)
            .err()
            .expect("non-advancing page")
            .kind(),
        CatalogErrorKind::InvalidHistoricalEvidence
    );

    const INDEXED: &str = r#"
contract Indexed version 1 {
  entity Row {
    key (id: u64)
    field name: string<8>
    index ByName(name)
  }
  aggregate Rows {
    root Row
    partition_by id
    conflict_key (id)
  }
}
"#;
    let bundle = ValidatedContractBundle::from_compiler_bundle(
        compile_contract_source(INDEXED).expect("indexed contract"),
    )
    .expect("validated");
    let stored = stored_bundle(&bundle);
    let entity = bundle.bundle().schema().entities().first().expect("entity");
    let oversized = (0..=500)
        .map(|id| {
            let key = entity
                .primary_key()
                .encode_entity(&[CanonicalValue::U64(id)])
                .expect("entity key");
            HistoricalSemanticEvidence::PersistedKey(entity_key_evidence(&bundle, key))
        })
        .collect();
    let mut oversized_page = session(
        vec![
            vec![HistoricalSemanticEvidence::Bundle(stored.clone())],
            vec![active(&bundle)],
            oversized,
        ],
        vec![stored],
        CursorFault::RawOversized,
    );
    assert_eq!(
        validate_catalog_history(&mut oversized_page)
            .err()
            .expect("oversized raw page")
            .kind(),
        CatalogErrorKind::InvalidHistoricalEvidence
    );
}

#[test]
fn unknown_or_hash_mismatched_plan_fails_without_active_substitution() {
    let bundle = ValidatedContractBundle::from_compiler_bundle(
        compile_contract_source(BUDGET).expect("budget"),
    )
    .expect("validated");
    let stored = stored_bundle(&bundle);
    let command = bundle.bundle().commands().first().expect("command");
    let wrong = riffdb_storage_api::ExecutablePlanRef::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
        command.command_id(),
        PlanHash::from_bytes([0x88; 32]),
    );
    let mut session = session(
        vec![
            vec![HistoricalSemanticEvidence::Bundle(stored.clone())],
            vec![HistoricalSemanticEvidence::PlanReference(wrong)],
            vec![active(&bundle)],
        ],
        vec![stored],
        CursorFault::None,
    );
    assert_eq!(
        validate_catalog_history(&mut session)
            .err()
            .expect("unknown plan")
            .kind(),
        CatalogErrorKind::UnknownExecutablePlan
    );
}

#[test]
fn startup_unique_reciprocity_accepts_exact_owner_and_rejects_torn_or_orphan_state() {
    let (bundle, record, index_key, partition) = unique_recovery_fixture();
    let stored = stored_bundle(&bundle);
    let entity_evidence = HistoricalSemanticEvidence::PersistedKey(
        HistoricalPersistedKeyEvidenceV1::from_entity(&record),
    );
    for unique_state in [
        FakeUniqueState::Vacant,
        FakeUniqueState::Conflict,
        FakeUniqueState::Corrupt,
    ] {
        let mut invalid = session(
            vec![
                vec![HistoricalSemanticEvidence::Bundle(stored.clone())],
                vec![active(&bundle)],
                vec![HistoricalSemanticEvidence::PersistedKey(
                    HistoricalPersistedKeyEvidenceV1::from_entity(&record),
                )],
            ],
            vec![stored.clone()],
            CursorFault::None,
        );
        invalid.entities.push(record.clone());
        invalid.unique_state = unique_state;
        assert!(
            validate_catalog_history(&mut invalid).is_err(),
            "torn, conflicting, or duplicate unique state must withhold readiness"
        );
    }

    let mut exact = session(
        vec![
            vec![HistoricalSemanticEvidence::Bundle(stored.clone())],
            vec![active(&bundle)],
            vec![entity_evidence],
            vec![migration_v2_evidence(
                &bundle,
                index_key.clone(),
                partition.clone(),
            )],
        ],
        vec![stored.clone()],
        CursorFault::None,
    );
    exact.entities.push(record.clone());
    exact.unique_state = FakeUniqueState::Owned;
    assert!(matches!(
        validate_catalog_history(&mut exact)
            .expect("exact reciprocal unique state validates")
            .outcome(),
        CatalogHistoryOutcome::Ready(_)
    ));

    let mut orphan = session(
        vec![
            vec![HistoricalSemanticEvidence::Bundle(stored.clone())],
            vec![active(&bundle)],
            vec![migration_v2_evidence(&bundle, index_key, partition)],
        ],
        vec![stored],
        CursorFault::None,
    );
    orphan.unique_state = FakeUniqueState::Owned;
    assert!(
        validate_catalog_history(&mut orphan).is_err(),
        "an index row without its authoritative entity is nonreciprocal"
    );
}

#[test]
fn branched_bundle_history_and_nonterminal_active_pointer_fail_closed() {
    let genesis_compiled = compile_contract_source(BUDGET).expect("genesis");
    let genesis =
        ValidatedContractBundle::from_compiler_bundle(genesis_compiled).expect("validated genesis");
    let second_compiled =
        compile_contract_successor(&version(BUDGET, 2), genesis.bundle()).expect("second version");
    let second =
        ValidatedContractBundle::from_compiler_bundle(second_compiled).expect("validated second");
    let branched_third_compiled = compile_contract_successor(&version(BUDGET, 3), genesis.bundle())
        .expect("third version branched from genesis");
    let branched_third = ValidatedContractBundle::from_compiler_bundle(branched_third_compiled)
        .expect("validated branch");

    let genesis_stored = stored_bundle(&genesis);
    let second_stored = stored_bundle(&second);
    let third_stored = stored_bundle(&branched_third);
    let mut branch = session(
        vec![
            vec![HistoricalSemanticEvidence::Bundle(genesis_stored.clone())],
            vec![HistoricalSemanticEvidence::Bundle(second_stored.clone())],
            vec![HistoricalSemanticEvidence::Bundle(third_stored.clone())],
            vec![active(&branched_third)],
        ],
        vec![genesis_stored.clone(), second_stored.clone(), third_stored],
        CursorFault::None,
    );
    assert_eq!(
        validate_catalog_history(&mut branch)
            .err()
            .expect("branched activation history")
            .kind(),
        CatalogErrorKind::InvalidHistoricalEvidence
    );

    let mut rollback = session(
        vec![
            vec![HistoricalSemanticEvidence::Bundle(genesis_stored.clone())],
            vec![HistoricalSemanticEvidence::Bundle(second_stored.clone())],
            vec![active(&genesis)],
        ],
        vec![genesis_stored, second_stored],
        CursorFault::None,
    );
    assert_eq!(
        validate_catalog_history(&mut rollback)
            .err()
            .expect("active must name terminal bundle")
            .kind(),
        CatalogErrorKind::InvalidHistoricalEvidence
    );
}

#[test]
fn startup_history_wires_the_exact_bundle_count_boundary() {
    const MAX_ACTIVE_LINEAGE_BUNDLES_V1: usize = 4_096;

    let lineage = boundary_lineage(MAX_ACTIVE_LINEAGE_BUNDLES_V1 + 1);
    let mut exact_evidence = lineage[..MAX_ACTIVE_LINEAGE_BUNDLES_V1]
        .iter()
        .map(|bundle| HistoricalSemanticEvidence::Bundle(stored_bundle(bundle)))
        .collect::<Vec<_>>();
    exact_evidence.push(active(&lineage[MAX_ACTIVE_LINEAGE_BUNDLES_V1 - 1]));
    let exact_active = stored_bundle(&lineage[MAX_ACTIVE_LINEAGE_BUNDLES_V1 - 1]);
    let mut exact_session = session(
        evidence_pages(exact_evidence),
        vec![exact_active],
        CursorFault::None,
    );
    let exact = validate_catalog_history(&mut exact_session)
        .expect("startup accepts exactly 4,096 lineage bundles");
    assert_eq!(
        ready_history(&exact).evidence_count(),
        u64::try_from(MAX_ACTIVE_LINEAGE_BUNDLES_V1 + 1).expect("bounded evidence count")
    );
    drop(exact);
    drop(exact_session);

    let mut over_evidence = lineage
        .iter()
        .map(|bundle| HistoricalSemanticEvidence::Bundle(stored_bundle(bundle)))
        .collect::<Vec<_>>();
    over_evidence.push(active(&lineage[MAX_ACTIVE_LINEAGE_BUNDLES_V1]));
    let over_active = stored_bundle(&lineage[MAX_ACTIVE_LINEAGE_BUNDLES_V1]);
    let mut over_session = session(
        evidence_pages(over_evidence),
        vec![over_active],
        CursorFault::None,
    );
    assert_eq!(
        validate_catalog_history(&mut over_session)
            .err()
            .expect("startup must reject lineage bundle 4,097")
            .kind(),
        CatalogErrorKind::InvalidHistoricalEvidence
    );
}

#[test]
fn index_rows_require_migration_for_v1_and_exact_historical_partition_for_v2() {
    const INDEXED: &str = r#"
contract IndexedMigration version 1 {
  entity Row {
    key (id: u64)
    field name: string<8>
    index ByName(name)
  }
  aggregate Rows {
    root Row
    partition_by id
    conflict_key (id)
  }
}
"#;
    let bundle = ValidatedContractBundle::from_compiler_bundle(
        compile_contract_source(INDEXED).expect("indexed migration contract"),
    )
    .expect("validated bundle");
    let stored = stored_bundle(&bundle);
    let (key, derived_partition) = migration_index_key_and_partition(&bundle);

    let mut legacy = session(
        vec![
            vec![HistoricalSemanticEvidence::Bundle(stored.clone())],
            vec![active(&bundle)],
            vec![migration_v1_evidence(&bundle, key.clone())],
        ],
        vec![stored.clone()],
        CursorFault::None,
    );
    let legacy = validate_catalog_history(&mut legacy).expect("valid V1 history");
    let CatalogHistoryOutcome::MigrationRequired(context) = legacy.outcome() else {
        panic!("a checked V1 row must not produce readiness");
    };
    assert_eq!(context.database_id(), database());
    assert_eq!(
        context.open_session_id(),
        OpenSessionId::new(7).expect("session")
    );
    assert_eq!(context.evidence_count(), 3);

    let mut current = session(
        vec![
            vec![HistoricalSemanticEvidence::Bundle(stored.clone())],
            vec![active(&bundle)],
            vec![migration_v2_evidence(
                &bundle,
                key.clone(),
                derived_partition.clone(),
            )],
        ],
        vec![stored.clone()],
        CursorFault::None,
    );
    let current = validate_catalog_history(&mut current).expect("valid V2 history");
    assert!(matches!(current.outcome(), CatalogHistoryOutcome::Ready(_)));

    let mut wrong = PartitionKeyBuilder::new(AggregateTypeId::new(99).expect("aggregate"));
    wrong.push_u64(42).expect("partition component");
    let mut corrupt = session(
        vec![
            vec![HistoricalSemanticEvidence::Bundle(stored.clone())],
            vec![active(&bundle)],
            vec![migration_v2_evidence(
                &bundle,
                key,
                wrong.finish().expect("wrong partition"),
            )],
        ],
        vec![stored],
        CursorFault::None,
    );
    assert_eq!(
        validate_catalog_history(&mut corrupt)
            .err()
            .expect("wrong stored partition")
            .kind(),
        CatalogErrorKind::InvalidHistoricalEvidence
    );
}

#[derive(Debug)]
struct DriverOutput {
    final_cursor: IndexMigrationCursor,
    rewritten_partition: Option<PartitionKey>,
    bundle_reads: usize,
    apply_count: usize,
    instruction_count: usize,
}

struct DriverBackend {
    database_id: DatabaseId,
    open_session_id: OpenSessionId,
    pages: VecDeque<Vec<IndexMigrationRowEvidence>>,
    bundle: HistoricalBundleEvidence,
    next: IndexMigrationCursor,
    rewritten_partition: Option<PartitionKey>,
    bundle_reads: usize,
    apply_count: usize,
    instruction_count: usize,
}

impl StartupIndexMigrationPort for DriverBackend {
    fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    fn open_session_id(&self) -> OpenSessionId {
        self.open_session_id
    }
}

impl CatalogIndexMigrationBackend for DriverBackend {
    type Output = DriverOutput;

    fn read_index_migration_page(
        mut self,
        request: CatalogIndexMigrationScanRequest<Self>,
    ) -> Result<CatalogIndexMigrationScan<Self>, StorageError> {
        if request.cursor() != self.next {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        let Some(rows) = self.pages.pop_front() else {
            return request.exact_end(self).map_err(migration_value_error);
        };
        let next = request
            .cursor()
            .advanced(
                u64::try_from(rows.len())
                    .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?,
            )
            .map_err(migration_value_error)?;
        self.next = next;
        request
            .page(self, rows, next)
            .map_err(migration_value_error)
    }

    fn read_historical_bundle(
        mut self,
        request: CatalogIndexMigrationBundleRequest<Self>,
    ) -> Result<CatalogIndexMigrationBundleResponse<Self>, StorageError> {
        self.bundle_reads += 1;
        let bundle = self.bundle.clone();
        request.respond(self, bundle).map_err(migration_value_error)
    }

    fn apply_index_migration_batch(
        mut self,
        pending: CatalogIndexMigrationPendingBatch<Self>,
    ) -> Result<CatalogIndexMigrationApplied<Self>, StorageError> {
        if pending.batch().instructions().is_empty() {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        for instruction in pending.batch().instructions() {
            let CatalogIndexMigrationInstruction::V1Rewrite(rewrite) = instruction else {
                return Err(storage_error(StorageErrorKind::InvariantViolation));
            };
            self.rewritten_partition = Some(rewrite.replacement().partition_key().clone());
            self.instruction_count += 1;
        }
        self.apply_count += 1;
        pending.applied(self).map_err(migration_value_error)
    }

    fn finish_index_migration(
        self,
        completion: CatalogIndexMigrationCompletion<Self>,
    ) -> Result<Self::Output, StorageError> {
        if completion.final_cursor() != self.next {
            return Err(storage_error(StorageErrorKind::InvariantViolation));
        }
        Ok(DriverOutput {
            final_cursor: completion.final_cursor(),
            rewritten_partition: self.rewritten_partition,
            bundle_reads: self.bundle_reads,
            apply_count: self.apply_count,
            instruction_count: self.instruction_count,
        })
    }
}

fn migration_value_error(error: StorageValueError) -> StorageError {
    let kind = match error {
        StorageValueError::LimitExceeded | StorageValueError::SizeOverflow => {
            StorageErrorKind::LimitExceeded
        }
        StorageValueError::Empty
        | StorageValueError::NonCanonicalOrder
        | StorageValueError::Duplicate
        | StorageValueError::IdentityMismatch
        | StorageValueError::InvalidShape => StorageErrorKind::InvariantViolation,
    };
    storage_error(kind)
}

fn migration_context(
    bundle: &ValidatedContractBundle,
    stored: &HistoricalBundleEvidence,
    key: riffdb_types::IndexEntryKey,
) -> CatalogIndexMigrationContext {
    let mut startup = session(
        vec![
            vec![HistoricalSemanticEvidence::Bundle(stored.clone())],
            vec![active(bundle)],
            vec![migration_v1_evidence(bundle, key)],
        ],
        vec![stored.clone()],
        CursorFault::None,
    );
    let validation = validate_catalog_history(&mut startup).expect("valid V1 history");
    let (outcome, _) = validation.into_parts();
    let CatalogHistoryOutcome::MigrationRequired(context) = outcome else {
        panic!("V1 history requires migration");
    };
    context
}

#[test]
fn catalog_driver_owns_scan_bundle_derivation_apply_and_completion() {
    const INDEXED: &str = r#"
contract IndexedMigrationLinear version 1 {
  entity Row {
    key (id: u64)
    field name: string<8>
    index ByName(name)
  }
  aggregate Rows {
    root Row
    partition_by id
    conflict_key (id)
  }
}

"#;
    let bundle = ValidatedContractBundle::from_compiler_bundle(
        compile_contract_source(INDEXED).expect("indexed migration contract"),
    )
    .expect("validated bundle");
    let stored = stored_bundle(&bundle);
    let (key, expected_partition) = migration_index_key_and_partition(&bundle);
    let mut session = session(
        vec![
            vec![HistoricalSemanticEvidence::Bundle(stored.clone())],
            vec![active(&bundle)],
            vec![migration_v1_evidence(&bundle, key.clone())],
        ],
        vec![stored.clone()],
        CursorFault::None,
    );
    let validation = validate_catalog_history(&mut session).expect("valid V1 history");
    let (outcome, _) = validation.into_parts();
    let CatalogHistoryOutcome::MigrationRequired(context) = outcome else {
        panic!("V1 history requires migration");
    };
    let start = IndexMigrationCursor::start(database(), session.open_session_id);
    assert_eq!(
        start,
        IndexMigrationCursor::start(database(), session.open_session_id)
    );

    let HistoricalSemanticEvidence::IndexMigrationRow(evidence) =
        migration_v1_evidence(&bundle, key)
    else {
        unreachable!("helper returns migration evidence");
    };
    let next = start.advanced(1).expect("one-row continuation");
    let backend = DriverBackend {
        database_id: database(),
        open_session_id: session.open_session_id,
        pages: VecDeque::from([vec![evidence]]),
        bundle: stored,
        next: start,
        rewritten_partition: None,
        bundle_reads: 0,
        apply_count: 0,
        instruction_count: 0,
    };
    let output = CatalogIndexMigrationDriver::new(context, backend)
        .expect("same-session driver")
        .run()
        .expect("complete catalog-owned migration");
    assert_eq!(output.final_cursor, next);
    assert_eq!(output.rewritten_partition, Some(expected_partition));
    assert_eq!(output.bundle_reads, 1);
    assert_eq!(output.apply_count, 1);
    assert_eq!(output.instruction_count, 1);
}

#[test]
fn catalog_driver_rejects_duplicate_and_reordered_keys_across_pages() {
    const INDEXED: &str = r#"
contract IndexedMigrationCrossPage version 1 {
  entity Row {
    key (id: u64)
    field name: string<8>
    index ByName(name)
  }
  aggregate Rows { root Row partition_by id conflict_key (id) }
}
"#;
    let bundle = ValidatedContractBundle::from_compiler_bundle(
        compile_contract_source(INDEXED).expect("indexed migration contract"),
    )
    .expect("validated bundle");
    let stored = stored_bundle(&bundle);
    let (lower, _) = migration_index_key_and_partition_for(&bundle, 1);
    let (higher, _) = migration_index_key_and_partition_for(&bundle, 2);
    assert!(lower.as_bytes() < higher.as_bytes());

    for (second, label) in [(higher.clone(), "duplicate"), (lower.clone(), "reordered")] {
        let context = migration_context(&bundle, &stored, lower.clone());
        let start = IndexMigrationCursor::start(database(), context.open_session_id());
        let backend = DriverBackend {
            database_id: database(),
            open_session_id: context.open_session_id(),
            pages: VecDeque::from([
                vec![migration_v1_row_evidence(&bundle, higher.clone())],
                vec![migration_v1_row_evidence(&bundle, second)],
            ]),
            bundle: stored.clone(),
            next: start,
            rewritten_partition: None,
            bundle_reads: 0,
            apply_count: 0,
            instruction_count: 0,
        };
        let error = CatalogIndexMigrationDriver::new(context, backend)
            .expect("same-session driver")
            .run()
            .err()
            .unwrap_or_else(|| panic!("{label} cross-page key must fail closed"));
        let CatalogIndexMigrationDriveError::Storage(error) = error else {
            panic!("{label} page shape is a backend invariant failure");
        };
        assert_eq!(error.kind(), StorageErrorKind::InvariantViolation);
    }
}

#[test]
fn catalog_driver_accepts_500_rows_and_rejects_501_before_bundle_reads() {
    const INDEXED: &str = r#"
contract IndexedMigrationPageBound version 1 {
  entity Row {
    key (id: u64)
    field name: string<8>
    index ByName(name)
  }
  aggregate Rows { root Row partition_by id conflict_key (id) }
}
"#;
    let bundle = ValidatedContractBundle::from_compiler_bundle(
        compile_contract_source(INDEXED).expect("indexed migration contract"),
    )
    .expect("validated bundle");
    let stored = stored_bundle(&bundle);
    let (context_key, _) = migration_index_key_and_partition_for(&bundle, 0);
    let rows = |count: u64| {
        (1..=count)
            .map(|id| {
                let (key, _) = migration_index_key_and_partition_for(&bundle, id);
                migration_v1_row_evidence(&bundle, key)
            })
            .collect::<Vec<_>>()
    };

    let context = migration_context(&bundle, &stored, context_key.clone());
    let start = IndexMigrationCursor::start(database(), context.open_session_id());
    let backend = DriverBackend {
        database_id: database(),
        open_session_id: context.open_session_id(),
        pages: VecDeque::from([rows(500)]),
        bundle: stored.clone(),
        next: start,
        rewritten_partition: None,
        bundle_reads: 0,
        apply_count: 0,
        instruction_count: 0,
    };
    let output = CatalogIndexMigrationDriver::new(context, backend)
        .expect("same-session driver")
        .run()
        .expect("500-row page is accepted");
    assert_eq!(output.final_cursor.position(), 500);
    assert_eq!(output.bundle_reads, 500);
    assert_eq!(output.apply_count, 1);
    assert_eq!(output.instruction_count, 500);

    let context = migration_context(&bundle, &stored, context_key);
    let start = IndexMigrationCursor::start(database(), context.open_session_id());
    let backend = DriverBackend {
        database_id: database(),
        open_session_id: context.open_session_id(),
        pages: VecDeque::from([rows(501)]),
        bundle: stored,
        next: start,
        rewritten_partition: None,
        bundle_reads: 0,
        apply_count: 0,
        instruction_count: 0,
    };
    let error = CatalogIndexMigrationDriver::new(context, backend)
        .expect("same-session driver")
        .run()
        .expect_err("501-row page must fail before row consumption");
    let CatalogIndexMigrationDriveError::Storage(error) = error else {
        panic!("page count is a backend shape failure");
    };
    assert_eq!(error.kind(), StorageErrorKind::LimitExceeded);
}

#[test]
fn retained_entity_key_schema_accepts_complete_bytes_and_rejects_truncation() {
    const INDEXED: &str = r#"
contract Indexed version 1 {
  entity Row {
    key (id: u64)
    field name: string<8>
    index ByName(name)
  }
  aggregate Rows {
    root Row
    partition_by id
    conflict_key (id)
  }
}
"#;
    let bundle = ValidatedContractBundle::from_compiler_bundle(
        compile_contract_source(INDEXED).expect("indexed contract"),
    )
    .expect("validated");
    let stored = stored_bundle(&bundle);
    let entity = bundle.bundle().schema().entities().first().expect("entity");
    let complete = entity
        .primary_key()
        .encode_entity(&[CanonicalValue::U64(42)])
        .expect("entity key");
    let valid = entity_key_evidence(&bundle, complete.clone());

    let mut truncated = complete.as_bytes().to_vec();
    truncated.pop();
    let truncated = EntityKey::from_bytes(truncated).expect("structural envelope remains valid");
    let invalid = entity_key_evidence(&bundle, truncated);

    let mut valid_session = session(
        vec![
            vec![HistoricalSemanticEvidence::Bundle(stored.clone())],
            vec![active(&bundle)],
            vec![HistoricalSemanticEvidence::PersistedKey(valid)],
        ],
        vec![stored.clone()],
        CursorFault::None,
    );
    assert!(validate_catalog_history(&mut valid_session).is_ok());

    let mut invalid_session = session(
        vec![
            vec![HistoricalSemanticEvidence::Bundle(stored.clone())],
            vec![active(&bundle)],
            vec![HistoricalSemanticEvidence::PersistedKey(invalid)],
        ],
        vec![stored],
        CursorFault::None,
    );
    assert_eq!(
        validate_catalog_history(&mut invalid_session)
            .err()
            .expect("incomplete component")
            .kind(),
        CatalogErrorKind::InvalidHistoricalKey
    );
}

#[test]
fn retained_index_prefix_schema_rejects_a_partial_variable_component() {
    const INDEXED: &str = r#"
contract Indexed version 1 {
  entity Row {
    key (id: u64)
    field name: string<8>
    index ByName(name)
  }
  aggregate Rows {
    root Row
    partition_by id
    conflict_key (id)
  }
}
"#;
    let bundle = ValidatedContractBundle::from_compiler_bundle(
        compile_contract_source(INDEXED).expect("indexed contract"),
    )
    .expect("validated");
    let stored = stored_bundle(&bundle);
    let index = bundle.bundle().schema().entities()[0].indexes()[0].key_schema();
    let complete = index
        .encode_index_prefix(&[CanonicalValue::string("abc").expect("value")])
        .expect("prefix");
    let valid = index_prefix_evidence(&bundle, complete.as_bytes().to_vec());
    let mut partial = complete.as_bytes().to_vec();
    partial.pop();
    let invalid = index_prefix_evidence(&bundle, partial);

    for (evidence, succeeds) in [(valid, true), (invalid, false)] {
        let mut candidate = session(
            vec![
                vec![HistoricalSemanticEvidence::Bundle(stored.clone())],
                vec![active(&bundle)],
                vec![HistoricalSemanticEvidence::PersistedKey(evidence)],
            ],
            vec![stored.clone()],
            CursorFault::None,
        );
        let result = validate_catalog_history(&mut candidate);
        assert_eq!(result.is_ok(), succeeds);
        if !succeeds {
            assert_eq!(
                result.err().expect("partial prefix").kind(),
                CatalogErrorKind::InvalidHistoricalKey
            );
        }
    }
}

fn migration_index_key_and_partition(
    bundle: &ValidatedContractBundle,
) -> (riffdb_types::IndexEntryKey, PartitionKey) {
    migration_index_key_and_partition_for(bundle, 42)
}

fn unique_recovery_fixture() -> (
    ValidatedContractBundle,
    StoredEntityRecordV1,
    riffdb_types::IndexEntryKey,
    PartitionKey,
) {
    let bundle = ValidatedContractBundle::from_compiler_bundle(
        compile_contract_source(UNIQUE_RECOVERY).expect("unique recovery contract"),
    )
    .expect("validated unique recovery contract");
    let user = bundle
        .bundle()
        .schema()
        .entities()
        .iter()
        .find(|entity| entity.name() == "User")
        .expect("User entity");
    let field = |name: &str| {
        user.record()
            .fields()
            .iter()
            .find(|field| field.name() == name)
            .expect("User field")
            .id()
    };
    let values = [
        CanonicalValue::U64(7),
        CanonicalValue::U64(11),
        CanonicalValue::string("owner@example.test").expect("email"),
    ];
    let record = CanonicalRecord::new(vec![
        (field("organization_id"), values[0].clone()),
        (field("user_id"), values[1].clone()),
        (field("email"), values[2].clone()),
    ])
    .expect("canonical User record");
    let entity_key = user
        .primary_key()
        .encode_entity(&values[..2])
        .expect("User key");
    let stored = StoredEntityRecordV1::new(
        EntityTarget::new(user.id(), entity_key.clone()).expect("User target"),
        EntityVersion::first(),
        bundle.contract_version(),
        DurableKeySchemaBindingV1::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
        ),
        record,
    )
    .expect("stored User");
    let index = user.indexes().first().expect("unique backing index");
    let index_key = index
        .key_schema()
        .encode_index(&[values[0].clone(), values[2].clone()], entity_key)
        .expect("unique index key");
    let aggregate = bundle
        .bundle()
        .schema()
        .aggregate_for_entity(user.id())
        .expect("aggregate");
    let partition = aggregate
        .keys()
        .partition_schema()
        .encode_partition(&[values[0].clone()])
        .expect("partition");
    (bundle, stored, index_key, partition)
}

fn migration_index_key_and_partition_for(
    bundle: &ValidatedContractBundle,
    id: u64,
) -> (riffdb_types::IndexEntryKey, PartitionKey) {
    let entity = bundle.bundle().schema().entities().first().expect("entity");
    let entity_key = entity
        .primary_key()
        .encode_entity(&[CanonicalValue::U64(id)])
        .expect("entity key");
    let index = entity.indexes().first().expect("index");
    let key = index
        .key_schema()
        .encode_index(
            &[CanonicalValue::string("name").expect("index value")],
            entity_key,
        )
        .expect("index key");
    let aggregate = bundle
        .bundle()
        .schema()
        .aggregate_for_entity(entity.id())
        .expect("aggregate owner");
    let partition = aggregate
        .keys()
        .partition_schema()
        .encode_partition(&[CanonicalValue::U64(id)])
        .expect("partition key");
    (key, partition)
}

fn migration_v1_evidence(
    bundle: &ValidatedContractBundle,
    key: riffdb_types::IndexEntryKey,
) -> HistoricalSemanticEvidence {
    HistoricalSemanticEvidence::IndexMigrationRow(migration_v1_row_evidence(bundle, key))
}

fn migration_v1_row_evidence(
    bundle: &ValidatedContractBundle,
    key: riffdb_types::IndexEntryKey,
) -> IndexMigrationRowEvidence {
    let row = StoredIndexEntryV1::new(
        key.clone(),
        DurableKeySchemaBindingV1::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
        ),
        CanonicalRecord::new(Vec::new()).expect("covered values"),
    )
    .expect("V1 row");
    let envelope =
        riffdb_storage_api::encode_index_entry_v1_fixture(&row).expect("canonical V1 envelope");
    riffdb_storage_api::decode_index_migration_row(&key, envelope.as_bytes())
        .expect("checked V1 migration evidence")
}

fn migration_v2_evidence(
    bundle: &ValidatedContractBundle,
    key: riffdb_types::IndexEntryKey,
    partition: PartitionKey,
) -> HistoricalSemanticEvidence {
    let row = StoredIndexEntryV2::new(
        key.clone(),
        DurableKeySchemaBindingV1::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
        ),
        CanonicalRecord::new(Vec::new()).expect("covered values"),
        partition,
    )
    .expect("V2 row");
    let envelope = riffdb_storage_api::encode_index_entry_v2(&row).expect("canonical V2 envelope");
    HistoricalSemanticEvidence::IndexMigrationRow(
        riffdb_storage_api::decode_index_migration_row(&key, envelope.as_bytes())
            .expect("checked V2 migration evidence"),
    )
}

fn entity_key_evidence(
    bundle: &ValidatedContractBundle,
    key: EntityKey,
) -> HistoricalPersistedKeyEvidenceV1 {
    let entity = bundle.bundle().schema().entities().first().expect("entity");
    let record = StoredEntityRecordV1::new(
        EntityTarget::new(entity.id(), key).expect("target"),
        EntityVersion::first(),
        bundle.contract_version(),
        DurableKeySchemaBindingV1::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
        ),
        CanonicalRecord::new(Vec::new()).expect("record"),
    )
    .expect("stored row");
    HistoricalPersistedKeyEvidenceV1::from_entity(&record)
}

fn index_prefix_evidence(
    bundle: &ValidatedContractBundle,
    bytes: Vec<u8>,
) -> HistoricalPersistedKeyEvidenceV1 {
    let index = bundle.bundle().schema().entities()[0].indexes()[0].id();
    let prefix = StructurallyDecodedIndexRangePrefixV1::new(index, bytes)
        .expect("structural prefix envelope");
    let record = LegacyStoredIndexEpochV1::new(
        prefix,
        DurableKeySchemaBindingV1::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
        ),
        IndexEpoch::first(),
    );
    HistoricalPersistedKeyEvidenceV1::from_legacy_index_epoch(&record)
}

fn version(source: &str, number: u64) -> String {
    source.replacen("version 1", &format!("version {number}"), 1)
}

fn boundary_lineage(bundle_count: usize) -> Vec<ValidatedContractBundle> {
    const EMPTY_LINEAGE_SOURCE: &str = r#"
contract StartupBoundary version 1 {
}
"#;

    assert!(bundle_count > 0);
    let genesis = compile_contract_source(EMPTY_LINEAGE_SOURCE).expect("empty genesis");
    let mut bundles = Vec::with_capacity(bundle_count);
    bundles.push(
        ValidatedContractBundle::from_compiler_bundle(genesis.clone()).expect("checked genesis"),
    );
    let mut parent = genesis;
    for version in 2..=u64::try_from(bundle_count).expect("bounded fixture length") {
        let successor = ContractBundle::new(
            parent.compiler_version(),
            parent.lineage().clone(),
            ContractVersion::new(version).expect("positive boundary version"),
            Some(ParentBundleRef::new(
                parent.contract_version(),
                parent.bundle_hash(),
            )),
            parent.source_hash(),
            parent.ledger().clone(),
            parent.schema().clone(),
            parent.commands().to_vec(),
            parent.projections().to_vec(),
            parent.schema_artifacts().to_vec(),
            parent.mcp_command_names().clone(),
            CompatibilityReport::successor(Vec::new()).expect("no semantic change"),
        )
        .expect("unchanged valid successor");
        bundles.push(
            ValidatedContractBundle::from_compiler_bundle(successor.clone())
                .expect("checked successor"),
        );
        parent = successor;
    }
    bundles
}

fn evidence_pages(
    evidence: Vec<HistoricalSemanticEvidence>,
) -> Vec<Vec<HistoricalSemanticEvidence>> {
    let mut pages = Vec::new();
    let mut page = Vec::with_capacity(500);
    for item in evidence {
        page.push(item);
        if page.len() == 500 {
            pages.push(std::mem::replace(&mut page, Vec::with_capacity(500)));
        }
    }
    if !page.is_empty() {
        pages.push(page);
    }
    pages
}
