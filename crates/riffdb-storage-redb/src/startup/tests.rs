use std::collections::BTreeSet;
use std::io::{BufRead, BufReader, Write};
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::{Arc, OnceLock};

use riffdb_catalog::{
    CatalogHistoryOutcome, CatalogIndexMigrationDriveError, CatalogIndexMigrationDriver,
    ValidatedContractBundle, validate_catalog_history,
};
use riffdb_contract_compiler::compile_contract_source;
use riffdb_storage_api::{
    ActiveCatalogPointerV1, AdministrationSequenceAllocator, AffectedEntityV1,
    ApplicationSequenceAllocator, AuditPrincipalV1, CapabilityAdministrationOperationV1,
    CapabilityBootstrapMarkerV1, CapabilityGrantV1, CapabilityPermissionKindV1,
    CapabilityPermissionV1, CapabilityPermissionsV1, CapabilityRequestedRecordV1,
    CatalogActivationIntentV1, CatalogAdministrationRepository, CommittedEntityReferenceV2,
    CommittedEntityTransitionV1, DatabaseInitializationPort, DeclaredOutcome, DurabilityMode,
    DurableKeySchemaBindingV1, EntityChainHeadV1, EntityChainStateV1, EntityTarget,
    ExecutablePlanRef, ExpectedEntityState, HistoricalEvidencePage, IdempotencyIdentity,
    IdempotencyKeyDigest, PartitionScopeV1, ProjectionGenerationPosition, ProjectionLifecycleV1,
    PublishedApplyModeV1, ReactiveModuleAdministrationRepository,
    ReactiveModulePublicationIntentV1, ReadDependencies, ReadDependency,
    ReadableCapabilityDigestInventory, ReadableIdempotencyDigestInventory, RevocationReasonCodeV1,
    StoredAdministrationAuditRecordV1, StoredAdmittedProvenanceClaimsV1,
    StoredCapabilityAdministrationV1, StoredCapabilityRecordV1, StoredCatalogAdministrationV1,
    StoredCommitRecordV1, StoredContractBundleV1, StoredEntityRecordV1, StoredIndexEntryV1,
    StoredIndexEntryV2, StoredOutcomeV1, StoredProjectionApplyV1, StoredProjectionControlV1,
    StoredProvenanceRecordV1, StoredReactiveModuleV1, StoredReadDependenciesV1,
    StoredServiceAuditRecordV1, StructuralEvidenceEnd, StructuralEvidencePage,
    derive_entity_record_hash_v1,
};
use riffdb_types::{
    ActorId, ActorKind, AdministrationSequence, AdmittedActorContext, AggregateTypeId, Audience,
    CanonicalInputHash, CanonicalRecord, CanonicalValue, CapabilityId, CapabilityTokenDigest,
    CommandId, CommitSequence, DigestKeyId, EntityKeyBuilder, EntityRecordHash,
    EntityTransitionHash, EntityTypeId, EntityVersion, Environment, FieldId, IndexEntryKeyBuilder,
    LogicalTime, OutcomeId, PartitionKeyBuilder, PlanHash, ProjectionApplyHash, ProjectionApplyKey,
    ProjectionGeneration, ProjectionId, ProjectionIdentity, ProjectionPlanHash, ProvenanceId,
    RequestId, ScopedPartitionV1, ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceAuditTargetsV1,
    ServiceIngressKindV1, ServiceOperationV1, TenantScope, Timestamp, hash_partition_key,
};

use super::*;

type CompleteAuthoritySnapshot = (Vec<Vec<(Vec<u8>, Vec<u8>)>>, Vec<u8>, Vec<u8>);

const REDB_MIGRATION_CONTRACT: &str = r#"
contract RedbMigration version 1 {
  entity Row {
    key (id: u64)
    field value: u64
    index ByValue(value)
  }

  aggregate Rows {
    root Row
    partition_by id
    conflict_key (id)
  }
}
"#;
const SCRUB_OWNER_CHILD_PATH: &str = "RIFFDB_SCRUB_OWNER_CHILD_PATH";

fn validated_migration_bundle() -> &'static ValidatedContractBundle {
    static BUNDLE: OnceLock<ValidatedContractBundle> = OnceLock::new();
    BUNDLE.get_or_init(|| {
        ValidatedContractBundle::from_compiler_bundle(
            compile_contract_source(REDB_MIGRATION_CONTRACT)
                .expect("compile indexed redb migration contract"),
        )
        .expect("validate indexed redb migration bundle")
    })
}

/// Whole-directory scope: the database and every side file it grows live
/// in one [`crate::test_path::ScopedDirectory`] removed on drop — pass,
/// fail, or panic.
struct TestDatabasePath(
    PathBuf,
    // Held only so `Drop` removes the whole scope.
    #[allow(dead_code)] crate::test_path::ScopedDirectory,
);

impl TestDatabasePath {
    fn new(label: &str) -> Self {
        let scope = crate::test_path::ScopedDirectory::new(label);
        Self(scope.join("db.redb"), scope)
    }
}

include!("../startup_graceful_close_tests.rs");

fn database_id(seed: u8) -> DatabaseId {
    DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [seed; 10])
        .expect("valid deterministic UUIDv7")
}

fn uuid_bytes(seed: u8) -> [u8; 16] {
    let mut bytes = [seed; 16];
    bytes[6] = 0x70 | (seed & 0x0f);
    bytes[8] = 0x80 | (seed & 0x3f);
    bytes
}

fn capability_id(seed: u8) -> CapabilityId {
    CapabilityId::from_bytes(uuid_bytes(seed)).expect("capability ID")
}

fn request_id(seed: u8) -> RequestId {
    RequestId::from_bytes(uuid_bytes(seed)).expect("request ID")
}

fn audit_principal(seed: u8) -> AuditPrincipalV1 {
    AuditPrincipalV1::new(
        ActorId::new("operator").expect("actor ID"),
        ActorKind::Human,
        capability_id(seed),
        NonZeroU64::MIN,
    )
}

fn inputs() -> StartupValidationInputs {
    inputs_at(1)
}

fn inputs_at(seconds: i64) -> StartupValidationInputs {
    let key = ReadableDigestKey::v1(DigestKeyId::new(1).expect("digest key"));
    StartupValidationInputs::new(
        Timestamp::new(seconds, 0).expect("timestamp"),
        ReadableCapabilityDigestInventory::new(vec![key]).expect("capability inventory"),
        ReadableIdempotencyDigestInventory::new(vec![key]).expect("idempotency inventory"),
    )
}

fn initialized_store(path: &TestDatabasePath, id: DatabaseId) -> RedbStore {
    let mut store = RedbStore::open(&path.0).expect("open store");
    store.initialize_database(id).expect("initialize store");
    store
}

fn deployed_migration_store(path: &TestDatabasePath, id: DatabaseId) -> RedbStore {
    let store = initialized_store(path, id);
    let dormant = RedbDormantPorts {
        shared: Arc::clone(&store.shared),
    };
    drop(store);
    let mut ports = dormant
        .into_operational_after_catalog_validation()
        .expect("activate migration fixture ports");
    let bundle = validated_migration_bundle()
        .to_stored()
        .expect("stored migration bundle");
    let intent = CatalogActivationIntentV1::new(
        None,
        bundle,
        request_id(0x51),
        audit_principal(0x52),
        Timestamp::new(1, 0).expect("activation timestamp"),
        None,
    );
    ports
        .activate_catalog(&intent)
        .expect("activate migration catalog");
    drop(ports);
    RedbStore::open(&path.0).expect("reopen deployed migration store")
}

fn compiled_migration_legacy_row(value: u64) -> StoredIndexEntryV1 {
    let bundle = validated_migration_bundle();
    let entity_schema = bundle
        .bundle()
        .schema()
        .entities()
        .first()
        .expect("migration entity");
    let index_schema = entity_schema.indexes().first().expect("migration index");
    let mut entity = EntityKeyBuilder::new(entity_schema.id());
    entity.push_u64(value).expect("entity key component");
    let mut index = IndexEntryKeyBuilder::new(index_schema.id());
    index.push_u64(value).expect("index key component");
    StoredIndexEntryV1::new(
        index
            .finish(entity.finish().expect("entity key"))
            .expect("index key"),
        DurableKeySchemaBindingV1::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
        ),
        CanonicalRecord::new(Vec::new()).expect("covered values"),
    )
    .expect("legacy migration row")
}

fn compiled_migration_partition(value: u64) -> riffdb_types::PartitionKey {
    let aggregate = validated_migration_bundle()
        .bundle()
        .schema()
        .aggregates()
        .first()
        .expect("migration aggregate");
    let mut partition = PartitionKeyBuilder::new(aggregate.id());
    partition.push_u64(value).expect("partition key component");
    partition.finish().expect("partition key")
}

#[test]
fn cached_bundle_bindings_preserve_index_cross_link_verdicts() {
    let path = TestDatabasePath::new("cached-index-bundle-binding");
    let store = deployed_migration_store(&path, database_id(0xa4));
    let transaction = store
        .shared
        .database
        .begin_read()
        .expect("read deployed catalog");
    let bindings = collect_bundle_bindings(&transaction).expect("collect exact bindings");
    let row = compiled_migration_legacy_row(7);
    let encoded = riffdb_storage_api::encode_index_entry_v1_fixture(&row)
        .expect("encode legacy index row")
        .into_bytes();

    assert_eq!(
        inspect_index_row(&bindings, row.key().as_bytes(), &encoded)
            .expect("inspect bound index row"),
        None,
        "an exact retained bundle must satisfy the cached cross-link"
    );
    assert_eq!(
        inspect_index_row(&BTreeSet::new(), row.key().as_bytes(), &encoded)
            .expect("inspect unbound index row"),
        Some(authoritative(StructuralFindingCode::MissingCrossLink)),
        "absence from the exact cache must remain fail-closed"
    );
}

fn stored_bundle(lineage: &str, version: u64, bytes: &[u8]) -> StoredContractBundleV1 {
    StoredContractBundleV1::new(
        ContractLineage::new(lineage).expect("lineage"),
        ContractVersion::new(version).expect("version"),
        hash_contract_bundle(bytes),
        bytes.to_vec(),
    )
    .expect("stored bundle")
}

fn requested_capability(database_id: DatabaseId) -> CapabilityRequestedRecordV1 {
    requested_capability_with_scope(database_id, PartitionScopeV1::All)
}

fn requested_capability_with_scope(
    database_id: DatabaseId,
    partition_scope: PartitionScopeV1,
) -> CapabilityRequestedRecordV1 {
    let permissions = CapabilityPermissionsV1::new(vec![
        CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::AdministerCapabilities)
            .expect("permission"),
    ])
    .expect("permissions");
    let grant = CapabilityGrantV1::new(
        TenantScope::Global,
        partition_scope,
        permissions,
        Vec::new(),
        NonZeroU16::MIN,
        Vec::new(),
    )
    .expect("grant");
    CapabilityRequestedRecordV1::new(
        database_id,
        Environment::new("test").expect("environment"),
        ActorId::new("subject").expect("subject"),
        ActorKind::Human,
        NonZeroU32::new(60).expect("duration"),
        vec![Audience::new("riffdb-test").expect("audience")],
        grant,
    )
    .expect("requested capability")
}

fn scoped_partition(lineage: &ContractLineage, value: u64) -> ScopedPartitionV1 {
    let mut builder =
        PartitionKeyBuilder::new(AggregateTypeId::new(0x0102_0304).expect("aggregate ID"));
    builder.push_u64(value).expect("partition component");
    ScopedPartitionV1::new(lineage.clone(), builder.finish().expect("partition key"))
}

fn explicit_scope(lineage: &ContractLineage, values: &[u64]) -> PartitionScopeV1 {
    PartitionScopeV1::explicit(
        values
            .iter()
            .map(|value| scoped_partition(lineage, *value))
            .collect(),
    )
    .expect("explicit scope")
}

fn active_capability(
    database_id: DatabaseId,
    capability_id: CapabilityId,
    partition_scope: PartitionScopeV1,
    issued_at: i64,
    request_seed: u8,
) -> StoredCapabilityRecordV1 {
    StoredCapabilityRecordV1::active(
        capability_id,
        CapabilityTokenDigest::from_hmac_bytes(
            DigestKeyId::new(1).expect("digest key ID"),
            [request_seed; 32],
        ),
        requested_capability_with_scope(database_id, partition_scope),
        Timestamp::new(issued_at, 0).expect("issued at"),
        Timestamp::new(issued_at + 60, 0).expect("expires at"),
        AdministrationSequence::first(),
        request_id(request_seed),
    )
    .expect("active capability")
}

fn insert_capabilities(store: &RedbStore, records: &[StoredCapabilityRecordV1]) {
    let write = store
        .shared
        .database
        .begin_write()
        .expect("write transaction");
    {
        let mut capabilities = write.open_table(CAPABILITIES).expect("capability table");
        for record in records {
            let key = keys::encode_capability_key(record.capability_id());
            let value = codec::encode_capability_record_v1(record).expect("encode capability");
            capabilities
                .insert(key.as_slice(), value.as_bytes())
                .expect("insert capability");
        }
    }
    write.commit().expect("commit capabilities");
}

fn projection_identity() -> ProjectionIdentity {
    ProjectionIdentity::new(
        ContractLineage::new("projection-integrity").expect("lineage"),
        ProjectionId::first(),
        ProjectionPlanHash::from_bytes([0x91; 32]),
    )
}

fn collect_structural(
    session: &mut RedbStructuralEvidenceSession,
) -> (RedbStructuralEvidenceEnd, Vec<StructuralFinding>) {
    let mut cursor =
        StructuralEvidenceCursor::start(session.database_id(), session.open_session_id());
    let mut collected = Vec::new();
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
                collected.extend(findings);
                cursor = next;
            }
            StructuralEvidencePage::ExactEnd(end) => {
                assert_eq!(end.cursor(), cursor);
                return (end, collected);
            }
        }
    }
}

fn finish_structural(session: &mut RedbStructuralEvidenceSession) -> RedbStructuralEvidenceEnd {
    let (end, findings) = collect_structural(session);
    assert!(findings.is_empty());
    end
}

fn finish_historical(session: &mut RedbStructuralEvidenceSession) -> RedbHistoricalEvidenceEnd {
    let (end, evidence, _) = collect_historical(session, 1);
    assert!(
        evidence
            .iter()
            .any(|item| { matches!(item, HistoricalSemanticEvidence::ActiveCatalog(None)) })
    );
    end
}

fn collect_historical(
    session: &mut RedbStructuralEvidenceSession,
    page_limit: u32,
) -> (
    RedbHistoricalEvidenceEnd,
    Vec<HistoricalSemanticEvidence>,
    Vec<usize>,
) {
    let mut cursor =
        HistoricalEvidenceCursor::start(session.database_id(), session.open_session_id());
    let mut collected = Vec::new();
    let mut page_lengths = Vec::new();
    loop {
        match session
            .read_historical_evidence(
                cursor,
                EvidencePageLimit::new(page_limit).expect("page limit"),
            )
            .expect("historical page")
        {
            HistoricalEvidencePage::Page {
                start,
                evidence,
                next,
            } => {
                assert_eq!(start, cursor);
                page_lengths.push(evidence.len());
                collected.extend(evidence);
                cursor = next;
            }
            HistoricalEvidencePage::ExactEnd(end) => {
                assert_eq!(end.cursor(), cursor);
                return (end, collected, page_lengths);
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
    let outcome = session
        .finish(structural_end, historical_end)
        .expect("finish evidence");
    let StructuralOpenOutcome::Clean(opened) = outcome else {
        panic!("empty V2 store must finish cleanly");
    };
    assert_eq!(opened.database_id(), id);
    assert_eq!(opened.retained_metadata(), &RetainedMetadataV1::initial(id));
    let (opened_id, opened_session, metadata, dormant) = opened.into_parts();
    assert_eq!(opened_id, id);
    assert!(opened_session.get() > 0);
    assert_eq!(metadata, RetainedMetadataV1::initial(id));
    drop(dormant);

    let reopened = RedbStore::open(&path.0).expect("reopen after handoff drop");
    assert_eq!(
        riffdb_storage_api::DatabaseIdentityProbePort::probe_database_identity(&reopened)
            .expect("probe reopened"),
        riffdb_storage_api::DatabaseIdentityProbe::Existing(id)
    );
}

// req: STO-023, PERF-019
#[test]
fn graceful_clean_close_selects_bounded_startup_and_is_consumed_before_activation() {
    let path = TestDatabasePath::new("clean-close-bounded-startup");
    let id = database_id(0x12);
    let store = initialized_store(&path, id);
    let mut first = store
        .begin_structural_evidence(inputs())
        .expect("begin predecessor full validation");
    assert!(!first.clean_close_fast);
    // The open that CREATED this database necessarily rebuilt allocator
    // state for a file that did not exist, so redb reports a repair and the
    // gate declines on that alone. The declined open must name which
    // precondition closed the gate rather than decline anonymously, and
    // must distinguish this benign first boot from a pre-existing database
    // whose previous close left no allocator state.
    assert_eq!(
        first.clean_close_declined_reason(),
        Some("engine_initialized_at_open")
    );
    let structural_end = finish_structural(&mut first);
    let historical_end = finish_historical(&mut first);
    let StructuralOpenOutcome::Clean(opened) = first
        .finish(structural_end, historical_end)
        .expect("finish predecessor full validation")
    else {
        panic!("initialized current store must finish cleanly");
    };
    let (_, _, _, dormant) = opened.into_parts();
    let ports = dormant
        .into_operational_after_catalog_validation()
        .expect("activate full-validation ports");
    assert!(!ports.clean_close_fast_startup());
    ports
        .write_clean_close_lifecycle()
        .expect("write final clean lifecycle");
    let immediate_read = ports
        .shared
        .database
        .begin_read()
        .expect("immediate proof read");
    let immediate_meta = immediate_read
        .open_table(META)
        .expect("immediate proof meta");
    let immediate_lifecycle = crate::clean_close::CleanCloseLifecycle::decode(
        immediate_meta
            .get(META_CLEAN_CLOSE_LIFECYCLE)
            .expect("immediate lifecycle read")
            .expect("immediate lifecycle present")
            .value(),
    )
    .expect("immediate lifecycle decode");
    drop(immediate_meta);
    let immediate_header = crate::journal::verify_clean_close_header_digest_with_media(
        &crate::media::RealJournalMedia,
        &path.0,
        id,
        crate::store::read_commit_tail(&immediate_read).expect("immediate application"),
        crate::store::read_administration_tail(&immediate_read).expect("immediate administration"),
    )
    .expect("immediate journal proof");
    let immediate_binding =
        crate::clean_close::bounded_state_binding_hash(&immediate_read, immediate_header)
            .expect("immediate bounded proof");
    assert_eq!(
        immediate_lifecycle.state(),
        crate::clean_close::CleanCloseState::Clean(immediate_binding)
    );
    drop(immediate_read);
    drop(ports);

    let reopened = RedbStore::open(&path.0).expect("reopen clean database");
    let proof_read = reopened.shared.database.begin_read().expect("proof read");
    let proof_meta = proof_read.open_table(META).expect("proof meta");
    let proof_encoded = proof_meta
        .get(META_CLEAN_CLOSE_LIFECYCLE)
        .expect("proof lifecycle read")
        .expect("proof lifecycle present");
    let proof_lifecycle = crate::clean_close::CleanCloseLifecycle::decode(proof_encoded.value())
        .expect("proof lifecycle decodes");
    drop(proof_encoded);
    drop(proof_meta);
    let proof_header = crate::journal::verify_clean_close_header_digest_with_media(
        &crate::media::RealJournalMedia,
        &path.0,
        id,
        crate::store::read_commit_tail(&proof_read).expect("application frontier"),
        crate::store::read_administration_tail(&proof_read).expect("administration frontier"),
    )
    .expect("clean journal proof");
    let proof_binding = crate::clean_close::bounded_state_binding_hash(&proof_read, proof_header)
        .expect("bounded proof");
    assert_eq!(
        proof_lifecycle.state(),
        crate::clean_close::CleanCloseState::Clean(proof_binding)
    );
    drop(proof_read);
    let mut bounded = reopened
        .begin_structural_evidence(inputs())
        .expect("begin bounded clean startup");
    assert!(bounded.clean_close_fast);
    assert_eq!(bounded.clean_close_declined_reason(), None);
    assert_eq!(
        bounded.structural_counts[3..],
        [0; STRUCTURAL_TABLE_COUNT - 3]
    );
    assert_eq!(
        bounded.additive_structural_counts,
        [0; ADDITIVE_STRUCTURAL_TABLE_COUNT]
    );
    let structural_end = finish_structural(&mut bounded);
    let historical_end = finish_historical(&mut bounded);
    let StructuralOpenOutcome::Clean(opened) = bounded
        .finish(structural_end, historical_end)
        .expect("consume clean lifecycle")
    else {
        panic!("bounded startup must not request migration");
    };
    let (_, _, _, dormant) = opened.into_parts();
    let ports = dormant
        .into_operational_after_catalog_validation()
        .expect("activate bounded ports");
    assert!(ports.clean_close_fast_startup());

    let read = ports.shared.database.begin_read().expect("read lifecycle");
    let meta = read.open_table(META).expect("meta table");
    let encoded = meta
        .get(META_CLEAN_CLOSE_LIFECYCLE)
        .expect("read lifecycle row")
        .expect("lifecycle present");
    let lifecycle = crate::clean_close::CleanCloseLifecycle::decode(encoded.value())
        .expect("decode consumed lifecycle");
    assert_eq!(
        lifecycle.state(),
        crate::clean_close::CleanCloseState::Dirty
    );
    assert_eq!(lifecycle.lifecycle_generation(), 3);
    drop(encoded);
    drop(meta);
    drop(read);

    // A bounded-generation write does not grant permission to rebuild the
    // population proof at close. The clean selector deliberately leaves
    // `startup_validation_clean` false, so this call returns its closed
    // skipped value before the journal barrier or proof builder can run.
    ports
        .write_clean_close_lifecycle()
        .expect("bounded final certificate write");
    assert!(
        !ports
            .write_validated_prefix_checkpoint()
            .expect("bounded checkpoint gate"),
        "bounded clean startup must skip checkpoint publication"
    );
    assert_eq!(
        ports.checkpoint_count_rows_walked(),
        0,
        "the skipped gate must not invoke the population proof builder"
    );
}

/// Drives one initialized store through a full validation pass, activates
/// its ports, writes the ADR-0157 certificate, and closes — the exact
/// sequence a graceful `riffdbd` shutdown performs.
fn certify_clean_close(store: RedbStore) {
    let mut session = store
        .begin_structural_evidence(inputs())
        .expect("begin full validation before certification");
    let structural_end = finish_structural(&mut session);
    let (catalog, historical_end) = validate_catalog_history(&mut session)
        .expect("validate catalog before certification")
        .into_parts();
    assert!(matches!(catalog, CatalogHistoryOutcome::Ready(_)));
    let StructuralOpenOutcome::Clean(opened) = session
        .finish(structural_end, historical_end)
        .expect("finish full validation before certification")
    else {
        panic!("initialized store must finish cleanly");
    };
    let (_, _, _, dormant) = opened.into_parts();
    let ports = dormant
        .into_operational_after_catalog_validation()
        .expect("activate ports before certification");
    ports
        .write_clean_close_lifecycle()
        .expect("write clean-close certificate");
    assert_eq!(ports.clean_close_write_failures(), 0);
    drop(ports);
}

fn complete_authority_snapshot(path: &Path) -> CompleteAuthoritySnapshot {
    let database = redb::ReadOnlyDatabase::open(path).expect("open complete authority snapshot");
    let read = database.begin_read().expect("begin authority snapshot");
    let meta = read.open_table(META).expect("open meta snapshot");
    let mut tables = vec![
        meta.iter()
            .expect("iterate meta snapshot")
            .map(|entry| {
                let (key, value) = entry.expect("read meta snapshot entry");
                (key.value().as_bytes().to_vec(), value.value().to_vec())
            })
            .collect(),
    ];
    drop(meta);
    for definition in crate::layout::BYTE_TABLES.into_iter().chain([
        crate::layout::VALIDATED_PREFIX_ENTITY_HEADS,
        crate::layout::IDEMPOTENCY_LOCATORS,
        crate::layout::PROVENANCE_LOCATORS,
        crate::layout::AUDIT_BY_REQUEST_LOCATORS,
    ]) {
        let table = read
            .open_table(definition)
            .expect("open byte table snapshot");
        tables.push(
            table
                .iter()
                .expect("iterate byte table snapshot")
                .map(|entry| {
                    let (key, value) = entry.expect("read byte table snapshot entry");
                    (key.value().to_vec(), value.value().to_vec())
                })
                .collect(),
        );
    }
    drop(read);
    drop(database);
    let mut journal = path.as_os_str().to_os_string();
    journal.push(".riffjournal");
    (
        tables,
        std::fs::read(PathBuf::from(journal)).expect("snapshot complete durability journal"),
        std::fs::read(crate::durable_format_marker_path(path))
            .expect("snapshot complete format marker"),
    )
}

fn assert_authority_snapshot_unchanged(
    before: &CompleteAuthoritySnapshot,
    after: &CompleteAuthoritySnapshot,
) {
    assert!(before.0 == after.0, "authoritative table bytes changed");
    assert!(before.1 == after.1, "durability journal bytes changed");
    assert!(before.2 == after.2, "format marker bytes changed");
}

// req: STO-012, STO-023, REC-001, REC-002, REC-004, PERF-019, END-004
#[test]
fn sealed_offline_scrub_is_exact_end_idempotent_and_byte_read_only() {
    let path = TestDatabasePath::new("sealed-offline-scrub");
    certify_clean_close(initialized_store(&path, database_id(0x7a)));
    let before = complete_authority_snapshot(&path.0);
    let key = DigestKeyId::new(1).expect("digest key ID");
    let bind = || {
        RedbOfflineIntegrityScrub::bind(
            &path.0,
            Timestamp::new(1_700_000_000, 0).expect("scrub timestamp"),
            vec![key],
            vec![key],
        )
        .expect("bind sealed scrub")
    };

    let first = bind().run().expect("complete first sealed scrub");
    assert_authority_snapshot_unchanged(&before, &complete_authority_snapshot(&path.0));
    let second = bind().run().expect("complete repeated sealed scrub");
    assert_authority_snapshot_unchanged(&before, &complete_authority_snapshot(&path.0));
    assert_eq!(first, second);
    assert_eq!(first.receipt_version(), 1);
    assert!(first.structural_exact_end());
    assert!(first.catalog_exact_end());
    assert_eq!(first.authoritative_mutations(), 0);
    assert_eq!(first.lifecycle_mutations(), 0);

    let retained = RedbStore::open(&path.0).expect("reopen after scrub");
    let session = retained
        .begin_structural_evidence(inputs())
        .expect("inspect retained clean eligibility");
    assert!(session.clean_close_fast_path());
}

// req: STO-023, REC-004, END-004
#[test]
fn sealed_offline_scrub_refuses_exclusive_owner_and_corruption_without_mutation() {
    if let Ok(child_path) = std::env::var(SCRUB_OWNER_CHILD_PATH) {
        let _owner = RedbStore::open(child_path).expect("child acquires exclusive owner");
        println!("SCRUB_OWNER_READY");
        std::io::stdout().flush().expect("flush child readiness");
        let mut release = String::new();
        std::io::stdin()
            .read_line(&mut release)
            .expect("wait for parent release");
        return;
    }

    let path = TestDatabasePath::new("sealed-offline-scrub-refusal");
    certify_clean_close(initialized_store(&path, database_id(0x7b)));
    let key = DigestKeyId::new(1).expect("digest key ID");
    let bind = || {
        RedbOfflineIntegrityScrub::bind(
            &path.0,
            Timestamp::new(1_700_000_000, 0).expect("scrub timestamp"),
            vec![key],
            vec![key],
        )
        .expect("bind sealed scrub")
    };

    let before_refusal = complete_authority_snapshot(&path.0);
    let mut child = Command::new(std::env::current_exe().expect("current test executable"))
            .arg("--exact")
            .arg("startup::tests::sealed_offline_scrub_refuses_exclusive_owner_and_corruption_without_mutation")
            .arg("--nocapture")
            .env(SCRUB_OWNER_CHILD_PATH, &path.0)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn exclusive owner child");
    let stdout = child.stdout.take().expect("child stdout");
    let mut stdout = BufReader::new(stdout);
    let mut readiness = String::new();
    let mut ready = false;
    for _ in 0..16 {
        readiness.clear();
        if stdout
            .read_line(&mut readiness)
            .expect("read child readiness")
            == 0
        {
            break;
        }
        if readiness.trim() == "SCRUB_OWNER_READY" {
            ready = true;
            break;
        }
    }
    assert!(ready, "child must report bounded owner readiness");
    assert!(
        bind().run().is_err(),
        "a live owner in another process must keep scrub offline"
    );
    child
        .stdin
        .take()
        .expect("child stdin")
        .write_all(b"release\n")
        .expect("release child owner");
    assert!(child.wait().expect("wait for owner child").success());
    assert_authority_snapshot_unchanged(&before_refusal, &complete_authority_snapshot(&path.0));

    let raw = redb::Database::open(&path.0).expect("open raw database");
    let write = raw.begin_write().expect("begin raw corruption");
    {
        let mut meta = write.open_table(META).expect("open meta");
        meta.insert("storage_format_version", &[0xff][..])
            .expect("corrupt metadata inventory");
    }
    write.commit().expect("publish corruption");
    drop(raw);
    let before_corrupt_refusal = complete_authority_snapshot(&path.0);
    assert_eq!(
        bind()
            .run()
            .expect_err("corruption must not publish an internal receipt")
            .kind(),
        StorageErrorKind::CorruptData
    );
    assert_authority_snapshot_unchanged(
        &before_corrupt_refusal,
        &complete_authority_snapshot(&path.0),
    );
}

// req: STO-023, REC-004
#[test]
fn complete_copy_preserves_clean_eligibility_but_registry_migration_invalidates_it() {
    let source = TestDatabasePath::new("clean-lifecycle-copy-source");
    certify_clean_close(initialized_store(&source, database_id(0x7c)));
    let copied = source.1.join("complete-copy.redb");
    std::fs::copy(&source.0, &copied).expect("copy database file");
    let mut source_journal = source.0.as_os_str().to_os_string();
    source_journal.push(".riffjournal");
    let mut copied_journal = copied.as_os_str().to_os_string();
    copied_journal.push(".riffjournal");
    std::fs::copy(PathBuf::from(source_journal), PathBuf::from(copied_journal))
        .expect("copy matching journal");
    std::fs::copy(
        crate::durable_format_marker_path(&source.0),
        crate::durable_format_marker_path(&copied),
    )
    .expect("copy matching format marker");

    let copied_store = RedbStore::open(&copied).expect("open complete lifecycle copy");
    let copied_session = copied_store
        .begin_structural_evidence(inputs())
        .expect("select copied lifecycle mode");
    assert!(
        copied_session.clean_close_fast_path(),
        "a byte-exact complete stopped lifecycle unit preserves eligibility"
    );
    drop(copied_session);

    let raw = redb::Database::open(&source.0).expect("open registry migration source");
    let write = raw.begin_write().expect("begin registry migration fixture");
    {
        let mut meta = write.open_table(META).expect("open migration meta");
        let encoded = riffdb_storage_api::proto_codec::encode_record_registry_v2(
            riffdb_types::SchemaHash::from_bytes(PRE_RETENTION_WATERMARK_REGISTRY_DIGEST),
        )
        .expect("encode predecessor registry");
        meta.insert(META_RECORD_REGISTRY, encoded.as_bytes())
            .expect("install predecessor registry");
        meta.remove(META_CLEAN_CLOSE_LIFECYCLE)
            .expect("remove lifecycle absent from predecessor registry");
    }
    write.commit().expect("commit registry migration fixture");
    drop(raw);
    let migrated = RedbStore::open(&source.0).expect("migrate predecessor registry");
    let migrated_session = migrated
        .begin_structural_evidence(inputs())
        .expect("select post-migration lifecycle mode");
    assert!(
        !migrated_session.clean_close_fast_path(),
        "registry migration from a lifecycle-free predecessor must require complete validation"
    );
}

// req: STO-023, REC-004
#[test]
fn retention_hold_makes_prior_clean_certificate_ineligible() {
    let held = TestDatabasePath::new("clean-lifecycle-retention-hold");
    certify_clean_close(initialized_store(&held, database_id(0x7d)));
    crate::RedbOfflineRetention::bind(&held.0)
        .add_hold("bounded-hold", 0, "compatibility proof")
        .expect("add retention hold");
    let held_store = RedbStore::open(&held.0).expect("reopen held lifecycle");
    let held_session = held_store
        .begin_structural_evidence(inputs())
        .expect("select held lifecycle mode");
    assert!(
        !held_session.clean_close_fast_path(),
        "retention-hold mutation must invalidate the prior binding"
    );
}

/// Drives one initialized store through a full validation pass and
/// activates its ports, then closes WITHOUT certifying — what a killed
/// daemon leaves behind. Activation consumes the certificate to DIRTY.
fn activate_without_certifying(store: RedbStore) {
    let mut session = store
        .begin_structural_evidence(inputs())
        .expect("begin full validation before an uncertified close");
    let structural_end = finish_structural(&mut session);
    let historical_end = finish_historical(&mut session);
    let StructuralOpenOutcome::Clean(opened) = session
        .finish(structural_end, historical_end)
        .expect("finish full validation before an uncertified close")
    else {
        panic!("initialized store must finish cleanly");
    };
    let (_, _, _, dormant) = opened.into_parts();
    drop(
        dormant
            .into_operational_after_catalog_validation()
            .expect("activate ports before an uncertified close"),
    );
}

/// Bounded startup leaves the population caches cold, and the first
/// derived read warms them at full cost.
///
/// ADR-0156 buys its readiness saving by skipping the transient
/// population-index rebuild at activation — a walk of the whole `COMMITS`
/// table that decodes every command segment, re-derives every manifest key,
/// and retains every segment. `ensure_transient_indexes_ready` then
/// performs exactly that walk on the first derived read.
///
/// That is correct in itself: a derived read must have an index. What makes
/// it worth pinning is WHEN the first derived read happens. In `riffdbd` it
/// is `recover_outbox` during graph construction, i.e. before readiness, so
/// a start that reports the bounded path pays the rebuild anyway and the
/// saving is returned in full. Measured on this workstation, that rebuild
/// is 96% of a bounded start's wall clock at 115,690 retained commands and
/// scales linearly.
///
/// Whoever changes where that first derived read happens should keep this
/// test: it is the difference between "cold caches" as an ADR-0156 claim
/// and as an observed fact.
#[test]
fn bounded_startup_defers_the_population_rebuild_to_the_first_derived_read() {
    use riffdb_storage_api::{
        OutboxPageLimit, OutboxRepository, UndeliveredOutboxStatusScanRequestV1,
    };

    let path = TestDatabasePath::new("clean-close-cold-caches");
    let id = database_id(0x76);
    certify_clean_close(initialized_store(&path, id));

    let reopened = RedbStore::open(&path.0).expect("reopen certified database");
    let mut bounded = reopened
        .begin_structural_evidence(inputs())
        .expect("begin bounded clean startup");
    assert!(bounded.clean_close_fast);
    let structural_end = finish_structural(&mut bounded);
    let historical_end = finish_historical(&mut bounded);
    let StructuralOpenOutcome::Clean(opened) = bounded
        .finish(structural_end, historical_end)
        .expect("finish bounded clean startup")
    else {
        panic!("bounded startup must not request migration");
    };
    let (_, _, _, dormant) = opened.into_parts();
    let ports = dormant
        .into_operational_after_catalog_validation()
        .expect("activate bounded ports");

    // Activation skipped the rebuild: this is the ADR-0156 saving.
    assert!(ports.clean_close_fast_startup());
    assert_eq!(ports.transient_index_rebuilds(), 0);
    assert_eq!(ports.transient_index_commit_rows(), 0);

    // One derived read — the same call `recover_outbox` makes first — warms
    // the caches, and the rebuild is charged to whoever made that read.
    let limit = OutboxPageLimit::new(std::num::NonZeroU16::new(8).expect("nonzero"))
        .expect("bounded outbox page limit");
    let _ = OutboxRepository::scan_undelivered_outbox_statuses(
        &ports,
        UndeliveredOutboxStatusScanRequestV1::initial(None, limit),
    )
    .expect("derived read over a bounded-start database");
    assert_eq!(
        ports.transient_index_rebuilds(),
        1,
        "the first derived read must be what pays for the deferred rebuild"
    );

    // Idempotent: the rebuild happens once per handle, not per read.
    let _ = OutboxRepository::scan_undelivered_outbox_statuses(
        &ports,
        UndeliveredOutboxStatusScanRequestV1::initial(None, limit),
    )
    .expect("second derived read");
    assert_eq!(ports.transient_index_rebuilds(), 1);
}

/// A positively observed in-flight `Delivering` entry contradicts a clean
/// certificate, and the contradiction takes the complete path.
///
/// This is the fail-closed property that licenses skipping outbox
/// normalization on the readiness path at all. Skipping is justified only by
/// "there is nothing in `Delivering` to normalize"; if that is false the
/// certificate is describing a state the database is not in, and ADR-0156's
/// existing rule for contradictory state applies — complete validation,
/// named, never a fast readiness.
///
/// The planted row deliberately does NOT disturb the bounded-state binding:
/// `OUTBOX_STATUS` is outside the certificate's bounded roots, so this start
/// would otherwise verify and be admitted. That is what makes the assertion
/// load-bearing rather than incidental.
#[test]
fn an_in_flight_delivering_entry_contradicts_the_certificate_and_takes_the_complete_path() {
    use riffdb_storage_api::{
        OutboxDestinationIdV1, StoredOutboxStatusV1, encode_outbox_status_v1,
    };
    use riffdb_types::EventId;

    let path = TestDatabasePath::new("clean-close-delivering-contradiction");
    let id = database_id(0x77);
    certify_clean_close(initialized_store(&path, id));

    // Plant one in-flight attempt, as a killed delivery worker would leave.
    {
        let store = RedbStore::open(&path.0).expect("open to plant a Delivering row");
        let mut write = store
            .shared
            .database
            .begin_write()
            .expect("begin planting write");
        write
            .set_durability(redb::Durability::Immediate)
            .expect("immediate durability");
        let event_id = EventId::new(CommitSequence::first(), 0);
        let status = StoredOutboxStatusV1::delivering(
            event_id,
            std::num::NonZeroU32::new(1).expect("nonzero attempt"),
            OutboxDestinationIdV1::new("test/contradiction").expect("destination"),
            Timestamp::new(10, 0).expect("started at"),
            Timestamp::new(70, 0).expect("lease deadline"),
        );
        let encoded = encode_outbox_status_v1(&status).expect("encode delivering status");
        let mut statuses = write
            .open_table(crate::layout::OUTBOX_STATUS)
            .expect("outbox status table");
        statuses
            .insert(
                crate::keys::encode_event_key(event_id).as_slice(),
                encoded.as_bytes(),
            )
            .expect("insert delivering status");
        drop(statuses);
        write.commit().expect("commit planted status");
    }

    let reopened = RedbStore::open(&path.0).expect("reopen with an in-flight attempt");
    let handle = reopened.reopen_for_test();
    let session = reopened
        .begin_structural_evidence(inputs())
        .expect("begin startup over a contradicted certificate");
    assert!(
        !session.clean_close_fast_path(),
        "an in-flight Delivering entry must not reach a bounded readiness"
    );
    assert_eq!(
        session.clean_close_declined_reason(),
        Some("outbox_delivering_observed")
    );
    assert_eq!(
        handle
            .clean_close_decline_counts()
            .iter()
            .find(|(reason, _)| *reason == "outbox_delivering_observed")
            .map(|(_, count)| *count),
        Some(1)
    );
    drop(session);
    // Never proof after a contradiction: a caller must not skip
    // normalization on the strength of this flag here.
    assert!(!handle.shared.outbox_delivering_proven_absent());
}

/// Every declined bounded startup names its precondition.
///
/// Ten preconditions decline the ADR-0157 bounded path and all of them used
/// to share one observable: the next open silently took the complete
/// validation pass. On a large database that is tens of minutes of SHA-256
/// and record decoding, so "slow start" was the only evidence available and
/// the cost was misdiagnosed twice. These assertions pin that each decline
/// is now separately nameable from the session and separately counted on
/// the store. Fail-closed behavior is unchanged: every case below still
/// takes the complete pass.
#[test]
fn declined_bounded_startup_names_and_counts_its_precondition() {
    // (1) Certificate verified: no reason, no decline counted.
    let verified_path = TestDatabasePath::new("clean-close-reason-verified");
    let verified_id = database_id(0x71);
    certify_clean_close(initialized_store(&verified_path, verified_id));
    let reopened = RedbStore::open(&verified_path.0).expect("reopen certified database");
    let handle = reopened.reopen_for_test();
    let session = reopened
        .begin_structural_evidence(inputs())
        .expect("begin bounded startup over a certified database");
    assert!(session.clean_close_fast_path());
    assert_eq!(session.clean_close_declined_reason(), None);
    assert!(
        handle
            .clean_close_decline_counts()
            .iter()
            .all(|(_, count)| *count == 0),
        "a verified certificate must count no decline: {:?}",
        handle.clean_close_decline_counts()
    );
    drop(session);
    drop(handle);

    // (2) The certificate was consumed to DIRTY by an earlier activation
    // and no clean close replaced it. This is what a killed daemon leaves,
    // and what any process that activates the database and exits without a
    // graceful shutdown leaves behind.
    let dirty_path = TestDatabasePath::new("clean-close-reason-dirty");
    let dirty_id = database_id(0x72);
    activate_without_certifying(initialized_store(&dirty_path, dirty_id));
    let reopened = RedbStore::open(&dirty_path.0).expect("reopen dirty database");
    let handle = reopened.reopen_for_test();
    let session = reopened
        .begin_structural_evidence(inputs())
        .expect("begin full validation over a dirty database");
    assert!(!session.clean_close_fast_path());
    assert_eq!(
        session.clean_close_declined_reason(),
        Some("state_not_clean")
    );
    assert_eq!(
        handle
            .clean_close_decline_counts()
            .iter()
            .find(|(reason, _)| *reason == "state_not_clean")
            .map(|(_, count)| *count),
        Some(1)
    );
    drop(session);
    drop(handle);

    // (3) No certificate row at all.
    let absent_path = TestDatabasePath::new("clean-close-reason-absent");
    let absent_id = database_id(0x73);
    certify_clean_close(initialized_store(&absent_path, absent_id));
    {
        let store = RedbStore::open(&absent_path.0).expect("open to remove the certificate");
        let mut write = store
            .shared
            .database
            .begin_write()
            .expect("begin certificate removal");
        write
            .set_durability(redb::Durability::Immediate)
            .expect("immediate durability");
        let mut meta = write.open_table(META).expect("meta table");
        meta.remove(META_CLEAN_CLOSE_LIFECYCLE)
            .expect("remove certificate row");
        drop(meta);
        write.commit().expect("commit certificate removal");
    }
    let reopened = RedbStore::open(&absent_path.0).expect("reopen without a certificate");
    let session = reopened
        .begin_structural_evidence(inputs())
        .expect("begin full validation without a certificate");
    assert!(!session.clean_close_fast_path());
    assert_eq!(session.clean_close_declined_reason(), Some("record_absent"));
    drop(session);

    // (4) A non-authoritative journal extent name is present when the gate
    // runs, so the final journal boundary cannot be the clean-close
    // boundary. Planted after open because store open reconciles and
    // removes scratch extents. Fail-closed: still declined, now nameably.
    let journal_path_case = TestDatabasePath::new("clean-close-reason-journal");
    let journal_id = database_id(0x75);
    certify_clean_close(initialized_store(&journal_path_case, journal_id));
    let reopened =
        RedbStore::open(&journal_path_case.0).expect("reopen before planting a scratch extent");
    std::fs::write(
        crate::journal::spare_journal_path(&journal_path_case.0),
        [0_u8; 8],
    )
    .expect("plant a non-authoritative extent name");
    let session = reopened
        .begin_structural_evidence(inputs())
        .expect("begin full validation with a scratch extent present");
    assert!(!session.clean_close_fast_path());
    assert_eq!(
        session.clean_close_declined_reason(),
        Some("journal_boundary_unverified")
    );
    drop(session);
}

#[test]
fn migration_compare_mismatch_rolls_back_the_complete_catalog_batch() {
    let path = TestDatabasePath::new("migration-compare-mismatch");
    let id = database_id(0x18);
    let store = deployed_migration_store(&path, id);
    let rows = [
        compiled_migration_legacy_row(1),
        compiled_migration_legacy_row(2),
    ];
    let envelopes = rows
        .iter()
        .map(|row| {
            riffdb_storage_api::encode_index_entry_v1_fixture(row)
                .expect("encode V1 migration row")
                .into_bytes()
        })
        .collect::<Vec<_>>();
    let write = store
        .shared
        .database
        .begin_write()
        .expect("seed migration transaction");
    {
        let mut table = write
            .open_table(SECONDARY_INDEXES)
            .expect("secondary index table");
        for (row, envelope) in rows.iter().zip(&envelopes) {
            table
                .insert(row.key().as_bytes(), envelope.as_slice())
                .expect("insert V1 migration row");
        }
    }
    write.commit().expect("commit V1 migration rows");

    let substituted = StoredIndexEntryV2::new(
        rows[1].key().clone(),
        rows[1].schema_binding().clone(),
        CanonicalRecord::new(vec![(FieldId::first(), CanonicalValue::U64(999))])
            .expect("substituted covered values"),
        compiled_migration_partition(2),
    )
    .expect("substituted V2 row");
    let substituted_envelope = codec::encode_index_entry_v2(&substituted)
        .expect("encode substituted V2 row")
        .into_bytes();

    let mut session = store
        .begin_structural_evidence(inputs())
        .expect("begin migration evidence");
    let structural_end = finish_structural(&mut session);
    let (catalog_outcome, historical_end) = validate_catalog_history(&mut session)
        .expect("catalog validates migration history")
        .into_parts();
    let CatalogHistoryOutcome::MigrationRequired(context) = catalog_outcome else {
        panic!("V1 catalog history must require migration");
    };
    let StructuralOpenOutcome::MigrationRequired(mut port) = session
        .finish(structural_end, historical_end)
        .expect("finish migration evidence")
    else {
        panic!("V1 storage history must retain a migration port");
    };
    port.substitute_before_apply(substituted);
    let error = CatalogIndexMigrationDriver::new(context, port)
        .expect("bind same-session migration driver")
        .run()
        .expect_err("stale compare must reject the complete batch");
    assert!(matches!(
        error,
        CatalogIndexMigrationDriveError::Storage(ref error)
            if error.kind() == StorageErrorKind::CorruptData
    ));

    let reopened = RedbStore::open(&path.0).expect("reopen after rejected batch");
    let read = reopened
        .shared
        .database
        .begin_read()
        .expect("read rejected batch state");
    let table = read
        .open_table(SECONDARY_INDEXES)
        .expect("secondary index table");
    assert_eq!(
        table
            .get(rows[0].key().as_bytes())
            .expect("read first row")
            .expect("first row exists")
            .value(),
        envelopes[0].as_slice(),
        "the first rewrite must roll back when the second compare is stale"
    );
    assert_eq!(
        table
            .get(rows[1].key().as_bytes())
            .expect("read second row")
            .expect("second row exists")
            .value(),
        substituted_envelope.as_slice(),
        "the independently committed stale row must remain exact"
    );
}

#[test]
fn retained_metadata_decode_reads_all_six_categories_from_one_snapshot() {
    let path = TestDatabasePath::new("retained-metadata-six-categories");
    let id = database_id(0x19);
    let store = initialized_store(&path, id);
    let active = ActiveCatalogPointerV1::new(
        ContractLineage::new("retained").expect("lineage"),
        ContractVersion::new(11).expect("version"),
        ContractBundleHash::from_bytes([0x41; 32]),
    );
    let marker = CapabilityBootstrapMarkerV1::new(
        id,
        capability_id(0x42),
        AdministrationSequence::new(13).expect("administration sequence"),
    );
    let application =
        codec::encode_application_sequence_allocator_v1(ApplicationSequenceAllocator::Exhausted)
            .expect("encode application allocator");
    let administration = codec::encode_administration_sequence_allocator_v1(
        AdministrationSequenceAllocator::Exhausted,
    )
    .expect("encode administration allocator");
    let encoded_active =
        codec::encode_active_catalog_pointer_v1(&active).expect("encode active pointer");
    let encoded_marker =
        codec::encode_capability_bootstrap_marker_v1(marker).expect("encode bootstrap marker");
    let write = store
        .shared
        .database
        .begin_write()
        .expect("write transaction");
    {
        let mut meta = write.open_table(META).expect("metadata table");
        meta.insert(META_APPLICATION_SEQUENCE, application.as_bytes())
            .expect("replace application allocator");
        meta.insert(META_ADMINISTRATION_SEQUENCE, administration.as_bytes())
            .expect("replace administration allocator");
        meta.insert(META_CAPABILITY_BOOTSTRAP, encoded_marker.as_bytes())
            .expect("insert bootstrap marker");
    }
    {
        let mut catalog = write.open_table(CATALOG_ACTIVE).expect("active table");
        catalog
            .insert(CATALOG_ACTIVE_KEY.as_slice(), encoded_active.as_bytes())
            .expect("insert active pointer");
    }
    write.commit().expect("commit retained metadata fixture");

    let read = store
        .shared
        .database
        .begin_read()
        .expect("read transaction");
    let observed = read_retained_metadata(&read).expect("decode retained metadata");
    assert_eq!(observed.storage_format_version().get(), 2);
    assert_eq!(observed.database_id(), id);
    assert_eq!(
        observed.application_sequence(),
        ApplicationSequenceAllocator::Exhausted
    );
    assert_eq!(
        observed.administration_sequence(),
        AdministrationSequenceAllocator::Exhausted
    );
    assert_eq!(observed.active_catalog(), Some(&active));
    assert_eq!(observed.capability_bootstrap(), Some(marker));
}

#[test]
fn mismatched_retained_metadata_is_rejected_before_handoff() {
    let path = TestDatabasePath::new("retained-metadata-mismatch");
    let store = initialized_store(&path, database_id(0x1a));
    let marker = CapabilityBootstrapMarkerV1::new(
        database_id(0x1b),
        capability_id(0x43),
        AdministrationSequence::first(),
    );
    let encoded_marker =
        codec::encode_capability_bootstrap_marker_v1(marker).expect("encode marker");
    let write = store
        .shared
        .database
        .begin_write()
        .expect("write transaction");
    {
        let mut meta = write.open_table(META).expect("metadata table");
        meta.insert(META_CAPABILITY_BOOTSTRAP, encoded_marker.as_bytes())
            .expect("insert mismatched marker");
    }
    write.commit().expect("commit corrupt fixture");

    assert_eq!(
        store
            .begin_structural_evidence(inputs())
            .expect_err("database-mismatched marker cannot publish a session")
            .kind(),
        StorageErrorKind::CorruptData
    );
    RedbStore::open(&path.0).expect("failed open releases the database handle");
}

#[test]
fn capability_partition_history_emits_exact_qualifying_inventory() {
    let path = TestDatabasePath::new("capability-partition-inventory");
    let database_id = database_id(0x12);
    let store = initialized_store(&path, database_id);
    let lineage = ContractLineage::new("budget").expect("lineage");
    let active_first_id = capability_id(0x21);
    let active_duplicate_id = capability_id(0x22);
    let all_id = capability_id(0x23);
    let expired_id = capability_id(0x24);
    let exact_expiry_id = capability_id(0x25);
    let future_issued_id = capability_id(0x26);
    let revoked_id = capability_id(0x27);

    let active_first = active_capability(
        database_id,
        active_first_id,
        explicit_scope(&lineage, &[1, 2]),
        20,
        0x61,
    );
    let active_duplicate = active_capability(
        database_id,
        active_duplicate_id,
        explicit_scope(&lineage, &[1]),
        20,
        0x62,
    );
    let all = active_capability(database_id, all_id, PartitionScopeV1::All, 20, 0x63);
    let expired = active_capability(
        database_id,
        expired_id,
        explicit_scope(&lineage, &[3]),
        0,
        0x64,
    );
    let exact_expiry = active_capability(
        database_id,
        exact_expiry_id,
        explicit_scope(&lineage, &[4]),
        10,
        0x65,
    );
    let future_issued = active_capability(
        database_id,
        future_issued_id,
        explicit_scope(&lineage, &[5]),
        100,
        0x66,
    );
    let revoked = active_capability(
        database_id,
        revoked_id,
        explicit_scope(&lineage, &[6]),
        20,
        0x67,
    )
    .revoked(
        NonZeroU64::MIN,
        Timestamp::new(30, 0).expect("revoked at"),
        AdministrationSequence::new(2).expect("revoke sequence"),
        RevocationReasonCodeV1::Requested,
    )
    .expect("revoked capability");
    insert_capabilities(
        &store,
        &[
            active_first,
            active_duplicate,
            all,
            expired,
            exact_expiry,
            future_issued,
            revoked,
        ],
    );

    let mut session = store
        .begin_structural_evidence(inputs_at(70))
        .expect("begin evidence");
    let (_, evidence, page_lengths) = collect_historical(&mut session, 2);
    assert_eq!(page_lengths, vec![2, 2, 1]);
    assert!(matches!(
        evidence.first(),
        Some(HistoricalSemanticEvidence::ActiveCatalog(None))
    ));
    let partitions = evidence
        .iter()
        .filter_map(|item| match item {
            HistoricalSemanticEvidence::CapabilityPartition(evidence) => Some(evidence),
            _ => None,
        })
        .collect::<Vec<_>>();
    let observed = partitions
        .iter()
        .map(|evidence| (evidence.capability_id(), evidence.entry_ordinal()))
        .collect::<BTreeSet<_>>();
    let expected = BTreeSet::from([
        (active_first_id, 0),
        (active_first_id, 1),
        (active_duplicate_id, 0),
        (future_issued_id, 0),
    ]);
    assert_eq!(observed, expected);
    let duplicated_first = partitions
        .iter()
        .find(|evidence| {
            evidence.capability_id() == active_first_id && evidence.entry_ordinal() == 0
        })
        .expect("first duplicate");
    let duplicated_second = partitions
        .iter()
        .find(|evidence| evidence.capability_id() == active_duplicate_id)
        .expect("second duplicate");
    assert_eq!(
        duplicated_first.scoped_partition(),
        duplicated_second.scoped_partition(),
        "equal keys in distinct capabilities remain distinct evidence items"
    );
}

#[test]
fn capability_partition_history_preserves_all_1024_ordinals_across_pages() {
    let path = TestDatabasePath::new("capability-partition-pages");
    let database_id = database_id(0x13);
    let store = initialized_store(&path, database_id);
    let lineage = ContractLineage::new("budget").expect("lineage");
    let capability_id = capability_id(0x31);
    let values = (0..1_024).collect::<Vec<u64>>();
    let capability = active_capability(
        database_id,
        capability_id,
        explicit_scope(&lineage, &values),
        20,
        0x71,
    );
    insert_capabilities(&store, &[capability]);

    let mut session = store
        .begin_structural_evidence(inputs_at(70))
        .expect("begin evidence");
    let (_, evidence, page_lengths) = collect_historical(&mut session, 500);
    assert_eq!(page_lengths, vec![500, 500, 25]);
    let partitions = evidence
        .into_iter()
        .filter_map(|item| match item {
            HistoricalSemanticEvidence::CapabilityPartition(evidence) => Some(evidence),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(partitions.len(), 1_024);
    assert!(
        partitions
            .iter()
            .all(|evidence| evidence.capability_id() == capability_id)
    );
    assert_eq!(
        partitions
            .iter()
            .map(|evidence| evidence.entry_ordinal())
            .collect::<Vec<_>>(),
        (0..1_024).collect::<Vec<u16>>()
    );
    assert!(
        partitions
            .windows(2)
            .all(|pair| { pair[0].evidence_order_key() < pair[1].evidence_order_key() })
    );
}

#[test]
fn capability_partition_history_matches_the_shared_golden_vector() {
    let path = TestDatabasePath::new("capability-partition-golden");
    let database_id = database_id(0x14);
    let store = initialized_store(&path, database_id);
    let capability_id = CapabilityId::from_bytes([
        0x01, 0x8f, 0x00, 0x00, 0x00, 0x00, 0x70, 0x01, 0x80, 0x02, 0x11, 0x22, 0x33, 0x44, 0x55,
        0x66,
    ])
    .expect("golden capability UUIDv7");
    let budget = ContractLineage::new("budget").expect("lineage");
    let scope = PartitionScopeV1::explicit(vec![
        scoped_partition(&ContractLineage::new("a").expect("lineage"), 1),
        scoped_partition(&budget, 2),
        scoped_partition(&budget, 0x0102_0304_0506_0708),
    ])
    .expect("golden scope");
    let capability = active_capability(database_id, capability_id, scope, 20, 0x72);
    insert_capabilities(&store, &[capability]);

    let mut session = store
        .begin_structural_evidence(inputs_at(70))
        .expect("begin evidence");
    let (_, evidence, _) = collect_historical(&mut session, 500);
    let golden = evidence
        .into_iter()
        .find_map(|item| match item {
            HistoricalSemanticEvidence::CapabilityPartition(evidence)
                if evidence.entry_ordinal() == 2 =>
            {
                Some(evidence)
            }
            _ => None,
        })
        .expect("golden capability evidence");
    let expected = vec![
        0x05, 0x01, 0x8f, 0x00, 0x00, 0x00, 0x00, 0x70, 0x01, 0x80, 0x02, 0x11, 0x22, 0x33, 0x44,
        0x55, 0x66, 0x00, 0x02, 0x00, 0x00, 0x00, 0x06, b'b', b'u', b'd', b'g', b'e', b't', 0x01,
        0x02, 0x03, 0x04, 0x00, 0x00, 0x00, 0x0e, 0x50, 0x01, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02,
        0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
    ];
    assert_eq!(golden.capability_id(), capability_id);
    assert_eq!(golden.semantic_bytes().expect("semantic charge"), 51);
    assert_eq!(golden.evidence_order_key(), expected);
    assert_eq!(
        historical_order_key(&HistoricalSemanticEvidence::CapabilityPartition(golden)),
        expected
    );
}

#[test]
fn cursors_are_session_bound_and_single_use() {
    let path = TestDatabasePath::new("cursor");
    let store = initialized_store(&path, database_id(0x22));
    let mut session = store
        .begin_structural_evidence(inputs())
        .expect("begin evidence");
    let start = StructuralEvidenceCursor::start(session.database_id(), session.open_session_id());
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
fn same_count_value_drift_is_rejected_by_the_session_commit_epoch() {
    let path = TestDatabasePath::new("same-count-drift");
    let store = initialized_store(&path, database_id(0x23));
    let seed = store
        .shared
        .database
        .begin_write()
        .expect("begin seed transaction");
    {
        let mut table = seed
            .open_table(SECONDARY_INDEXES)
            .expect("open secondary-index table");
        table
            .insert(b"same-key".as_slice(), b"before".as_slice())
            .expect("insert seed value");
    }
    store
        .shared
        .commit_durable(seed)
        .expect("commit seed value");

    let mut session = store
        .begin_structural_evidence(inputs())
        .expect("begin anchored evidence session");
    let drift = session
        .shared
        .database
        .begin_write()
        .expect("begin deliberate internal bypass");
    {
        let mut table = drift
            .open_table(SECONDARY_INDEXES)
            .expect("open secondary-index table");
        assert!(
            table
                .insert(b"same-key".as_slice(), b"after!".as_slice())
                .expect("replace same-count value")
                .is_some()
        );
    }
    session
        .shared
        .commit_durable(drift)
        .expect("commit deliberate same-count drift");

    let start = StructuralEvidenceCursor::start(session.database_id(), session.open_session_id());
    assert_eq!(
        session
            .read_structural_evidence(start, EvidencePageLimit::new(1).expect("page limit"))
            .expect_err("commit-epoch drift must fail closed")
            .kind(),
        StorageErrorKind::CorruptData
    );
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

/// Per-entity oracle resurrected from pre-Package-E `entity_history_matches`
/// (6892c84), adapted to entity references (mutations are no longer stored).
fn entity_history_matches_oracle(
    transaction: &ReadTransaction,
    current: &StoredEntityRecordV1,
) -> Result<bool, StorageError> {
    let table = transaction.open_table(COMMITS).map_err(table_error)?;
    let mut prior_version: Option<EntityVersion> = None;
    let mut saw_mutation = false;
    let mut terminal_hash = None;
    for entry in table.iter().map_err(precommit_storage_error)? {
        let (physical_key, value) = entry.map_err(precommit_storage_error)?;
        let Ok(sequence) = keys::decode_application_sequence_key(physical_key.value()) else {
            return Ok(false);
        };
        let Ok(references) = decoded(codec::decode_commit_entity_references(value.value())) else {
            return Ok(false);
        };
        let _ = sequence;
        let mut matching = references
            .iter()
            .filter(|reference| reference.target() == current.target());
        let Some(reference) = matching.next() else {
            continue;
        };
        if matching.next().is_some() {
            return Ok(false);
        }
        let expected =
            CommittedEntityReferenceV2::expected_from_version(reference.entity_version());
        let expected_matches = match (prior_version, expected) {
            (None, ExpectedEntityState::Absent) => {
                reference.entity_version() == EntityVersion::first()
            }
            (Some(prior), ExpectedEntityState::Present(version)) => prior == version,
            (None, ExpectedEntityState::Present(_)) | (Some(_), ExpectedEntityState::Absent) => {
                false
            }
        };
        if !expected_matches {
            return Ok(false);
        }
        prior_version = Some(reference.entity_version());
        terminal_hash = Some(reference.post_image_hash());
        saw_mutation = true;
    }
    let current_hash = derive_entity_record_hash_v1(current).expect("hash current");
    Ok(saw_mutation
        && prior_version == Some(current.entity_version())
        && terminal_hash == Some(current_hash))
}

fn history_entity(
    seed: u64,
    version: EntityVersion,
    fields: &[u8],
    bundle: &StoredContractBundleV1,
) -> StoredEntityRecordV1 {
    let entity_type = EntityTypeId::new(7).expect("entity type");
    let mut key = EntityKeyBuilder::new(entity_type);
    key.push_u64(seed).expect("entity key component");
    let target = EntityTarget::new(entity_type, key.finish().expect("entity key")).expect("target");
    let binding = DurableKeySchemaBindingV1::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
    );
    let field_id = FieldId::new(1).expect("field");
    let record = CanonicalRecord::new(vec![(
        field_id,
        CanonicalValue::bytes(fields.to_vec()).expect("field bytes"),
    )])
    .expect("fields");
    StoredEntityRecordV1::new(target, version, bundle.contract_version(), binding, record)
        .expect("entity")
}

fn history_commit(
    sequence: CommitSequence,
    plan: ExecutablePlanRef,
    images: &[&StoredEntityRecordV1],
) -> StoredCommitRecordV1 {
    let sequence_byte = u8::try_from(sequence.get().min(250)).expect("sequence byte");
    let actor = AdmittedActorContext::new(
        ActorId::new("history-actor").expect("actor"),
        ActorKind::Human,
        TenantScope::Global,
        None,
    );
    let mut partition = PartitionKeyBuilder::new(AggregateTypeId::first());
    partition.push_u64(1).expect("partition component");
    let partition_hash = hash_partition_key(partition.finish().expect("partition key").as_bytes());
    let references = images
        .iter()
        .map(|image| CommittedEntityReferenceV2::from_post_image(image).expect("entity reference"))
        .collect::<Vec<_>>();
    let observations = images
        .iter()
        .map(|image| ReadDependency::EntityObservation {
            target: image.target().clone(),
            expected: CommittedEntityReferenceV2::expected_from_version(image.entity_version()),
        })
        .collect::<Vec<_>>();
    let read_dependencies = StoredReadDependenciesV1::from_live(
        &ReadDependencies::new(observations).expect("dependencies"),
    )
    .expect("stored dependencies");
    StoredCommitRecordV1::new(
        sequence,
        RequestId::from_bytes(uuid_bytes(sequence_byte.wrapping_add(0x20))).expect("request ID"),
        plan,
        CanonicalInputHash::from_bytes([sequence_byte; 32]),
        actor,
        LogicalTime::new(Timestamp::new(i64::from(sequence_byte), 0).expect("timestamp")),
        partition_hash,
        Vec::new(),
        read_dependencies,
        references,
        Vec::new(),
        DeclaredOutcome::new(
            OutcomeId::first(),
            CanonicalRecord::new(Vec::new()).expect("outcome fields"),
        )
        .expect("outcome"),
        ProvenanceId::from_bytes(uuid_bytes(sequence_byte.wrapping_add(0x40)))
            .expect("provenance ID"),
        Vec::new(),
        DurabilityMode::Sync,
    )
    .expect("history commit")
}

fn write_entities_and_commits(
    store: &RedbStore,
    id: DatabaseId,
    bundle: &StoredContractBundleV1,
    entities: &[StoredEntityRecordV1],
    commits: &[StoredCommitRecordV1],
) {
    let write = store
        .shared
        .database
        .begin_write()
        .expect("write history fixture");
    {
        let mut table = write.open_table(CONTRACT_BUNDLES).expect("bundles");
        let key = keys::encode_contract_bundle_key(bundle.lineage(), bundle.contract_version())
            .expect("bundle key");
        let encoded = codec::encode_contract_bundle_v1(bundle).expect("encode bundle");
        table
            .insert(key.as_slice(), encoded.as_bytes())
            .expect("insert bundle");
    }
    // Activate the bundle so inspect_header / inspect_bundle_row produce
    // zero residuals on an intact control (C1 falsifiability).
    let pointer = ActiveCatalogPointerV1::from_bundle(bundle);
    let activation = StoredCatalogAdministrationV1::from_stored_parts(
        AdministrationSequence::first(),
        request_id(0xe1),
        Timestamp::new(1, 0).expect("timestamp"),
        audit_principal(0xe2),
        None,
        pointer.clone(),
        None,
    );
    {
        let encoded_active =
            codec::encode_active_catalog_pointer_v1(&pointer).expect("encode active");
        write
            .open_table(CATALOG_ACTIVE)
            .expect("active")
            .insert(CATALOG_ACTIVE_KEY.as_slice(), encoded_active.as_bytes())
            .expect("insert active");
    }
    {
        let encoded_audit = codec::encode_administration_audit_record_v1(
            &StoredAdministrationAuditRecordV1::Catalog(activation),
        )
        .expect("encode catalog activation");
        write
            .open_table(AUDIT)
            .expect("audit")
            .insert(
                keys::encode_audit_key(AdministrationSequence::first()).as_slice(),
                encoded_audit.as_bytes(),
            )
            .expect("insert activation");
    }
    {
        let mut table = write.open_table(ENTITIES).expect("entities");
        let mut heads = write
            .open_table(crate::layout::ENTITY_CHAIN_HEADS)
            .expect("entity chain heads");
        for (ordinal, entity) in entities.iter().enumerate() {
            let encoded = codec::encode_entity_record_v1(entity).expect("encode entity");
            table
                .insert(entity.target().key().as_bytes(), encoded.as_bytes())
                .expect("insert entity");
            let sequence = commits
                .iter()
                .find(|commit| {
                    commit
                        .entity_references()
                        .iter()
                        .any(|reference| reference.target() == entity.target())
                })
                .map(StoredCommitRecordV1::commit_sequence)
                .unwrap_or_else(CommitSequence::first);
            let head = test_genesis_entity_head(entity, sequence, ordinal);
            let encoded_head = riffdb_storage_api::encode_entity_chain_head_v1(&head)
                .expect("encode entity chain head");
            heads
                .insert(entity.target().key().as_bytes(), encoded_head.as_bytes())
                .expect("insert entity chain head");
        }
    }
    {
        let mut commits_table = write.open_table(COMMITS).expect("commits");
        let mut outcomes = write.open_table(IDEMPOTENCY).expect("idempotency");
        let mut provenance_table = write.open_table(PROVENANCE).expect("provenance");
        for (ordinal, commit) in commits.iter().enumerate() {
            let key = keys::encode_application_sequence_key(commit.commit_sequence());
            let encoded = codec::encode_commit_record_v1(commit).expect("encode commit");
            commits_table
                .insert(key.as_slice(), encoded.as_bytes())
                .expect("insert commit");
            // Minimal reciprocal outcome/provenance so commit inspect noise
            // does not drown entity-history findings.
            let mut seed = [0x31u8; 32];
            seed[0] = u8::try_from(ordinal.wrapping_add(1)).unwrap_or(1);
            let identity = IdempotencyIdentity::new(
                id,
                Environment::new("test").expect("environment"),
                TenantScope::Global,
                commit.actor().principal_id().clone(),
                commit.plan().contract_lineage().clone(),
                commit.plan().command_id(),
                IdempotencyKeyDigest::from_hmac_bytes(
                    DigestKeyId::new(1).expect("digest key"),
                    seed,
                ),
            );
            let mut partition = PartitionKeyBuilder::new(AggregateTypeId::first());
            partition.push_u64(1).expect("partition");
            let partition_key = partition.finish().expect("partition key");
            let outcome = StoredOutcomeV1::new(
                identity.clone(),
                commit.commit_sequence(),
                commit.admission_request_id(),
                commit.plan().clone(),
                commit.canonical_input_hash(),
                commit.actor().clone(),
                commit.logical_time(),
                partition_key,
                commit.partition_hash(),
                commit.conflict_hashes().to_vec(),
                commit.declared_outcome().clone(),
                StoredAdmittedProvenanceClaimsV1::default(),
                commit.provenance_id(),
                commit.durability_mode(),
            )
            .expect("outcome");
            let affected = commit
                .entity_references()
                .iter()
                .map(|reference| {
                    AffectedEntityV1::from_stored_parts(
                        reference.target().clone(),
                        reference.entity_version(),
                    )
                })
                .collect();
            let provenance = StoredProvenanceRecordV1::new(
                commit.provenance_id(),
                commit.commit_sequence(),
                identity,
                commit.admission_request_id(),
                commit.plan().clone(),
                commit.canonical_input_hash(),
                commit.actor().clone(),
                commit.logical_time(),
                commit.partition_hash(),
                commit.conflict_hashes().to_vec(),
                commit.declared_outcome().outcome_id(),
                affected,
                commit.outbox_event_ids().to_vec(),
                StoredAdmittedProvenanceClaimsV1::default(),
            )
            .expect("provenance");
            let outcome_encoded =
                codec::encode_stored_outcome_v1(&outcome).expect("encode outcome");
            let identity_key = outcome.identity().storage_key().expect("identity key");
            outcomes
                .insert(identity_key.as_bytes(), outcome_encoded.as_bytes())
                .expect("insert outcome");
            let provenance_encoded =
                codec::encode_provenance_record_v1(&provenance).expect("encode provenance");
            let provenance_key = keys::encode_provenance_key(commit.provenance_id());
            provenance_table
                .insert(provenance_key.as_slice(), provenance_encoded.as_bytes())
                .expect("insert provenance");
        }
    }
    // Advance application sequence past the highest commit so sequence
    // continuity does not flag a discontinuity.
    if let Some(last) = commits.iter().map(|c| c.commit_sequence()).max()
        && let Some(next) = last.checked_next()
    {
        let encoded = codec::encode_application_sequence_allocator_v1(
            ApplicationSequenceAllocator::Next(next),
        )
        .expect("allocator");
        write
            .open_table(META)
            .expect("meta")
            .insert(META_APPLICATION_SEQUENCE, encoded.as_bytes())
            .expect("update allocator");
    }
    // Administration allocator must match the single catalog activation.
    {
        let next = AdministrationSequence::first()
            .checked_next()
            .expect("admin next");
        let encoded = codec::encode_administration_sequence_allocator_v1(
            AdministrationSequenceAllocator::Next(next),
        )
        .expect("admin allocator");
        write
            .open_table(META)
            .expect("meta")
            .insert(META_ADMINISTRATION_SEQUENCE, encoded.as_bytes())
            .expect("update admin allocator");
    }
    write.commit().expect("commit history fixture");
}

fn test_genesis_entity_head(
    entity: &StoredEntityRecordV1,
    sequence: CommitSequence,
    ordinal: usize,
) -> EntityChainHeadV1 {
    let mut transition_hash = [0_u8; 32];
    transition_hash[..8].copy_from_slice(&sequence.get().to_be_bytes());
    transition_hash[8..16].copy_from_slice(
        &u64::try_from(ordinal)
            .expect("transition ordinal")
            .to_be_bytes(),
    );
    EntityChainHeadV1::from_stored_parts(
        entity.target().clone(),
        1,
        EntityChainStateV1::Live {
            version: entity.entity_version(),
            value_hash: derive_entity_record_hash_v1(entity).expect("entity hash"),
        },
        sequence,
        EntityTransitionHash::from_bytes(transition_hash),
    )
    .expect("fixture entity head")
}

fn finding_codes(findings: &[StructuralFinding]) -> Vec<StructuralFindingCode> {
    findings.iter().map(|f| f.code()).collect()
}

fn count_code(findings: &[StructuralFinding], code: StructuralFindingCode) -> usize {
    findings.iter().filter(|f| f.code() == code).count()
}

/// Intact two-entity control used by corruption tests for a zero-finding baseline.
fn intact_two_entity_fixture(
    label: &str,
    id_seed: u8,
) -> (
    TestDatabasePath,
    RedbStore,
    StoredContractBundleV1,
    StoredEntityRecordV1,
    StoredEntityRecordV1,
) {
    let path = TestDatabasePath::new(label);
    let id = database_id(id_seed);
    let store = initialized_store(&path, id);
    let bundle = stored_bundle("entity-history", 1, b"entity-history-bundle");
    let plan = ExecutablePlanRef::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
        CommandId::first(),
        PlanHash::from_bytes([0x44; 32]),
    );
    let a = history_entity(1, EntityVersion::first(), b"a", &bundle);
    let b = history_entity(2, EntityVersion::first(), b"b", &bundle);
    let commit = history_commit(CommitSequence::first(), plan, &[&a, &b]);
    write_entities_and_commits(&store, id, &bundle, &[a.clone(), b.clone()], &[commit]);
    (path, store, bundle, a, b)
}

#[test]
fn entity_history_intact_control_produces_zero_structural_findings() {
    let (_path, store, _bundle, _a, _b) = intact_two_entity_fixture("entity-control", 0x80);
    let mut session = store
        .begin_structural_evidence(inputs())
        .expect("begin structural");
    let (_end, findings) = collect_structural(&mut session);
    assert_eq!(
        findings,
        Vec::new(),
        "intact activated fixture must be finding-clean: {findings:?}"
    );
}

#[test]
fn checkpoint_head_seed_advances_update_delete_recreate_exactly() {
    let bundle = stored_bundle("checkpoint-head-seed", 1, b"checkpoint-head-seed");
    let entity = history_entity(1, EntityVersion::first(), b"v1", &bundle);
    let target = entity.target().clone();
    let v1_hash = derive_entity_record_hash_v1(&entity).expect("v1 hash");
    let v2_hash = EntityRecordHash::from_bytes([0x22; 32]);
    let recreated_hash = EntityRecordHash::from_bytes([0x44; 32]);
    let create = CommittedEntityTransitionV1::new(
        CommitSequence::first(),
        0,
        target.clone(),
        EntityChainStateV1::NeverExisted,
        0,
        None,
        EntityChainStateV1::Live {
            version: EntityVersion::first(),
            value_hash: v1_hash,
        },
    )
    .expect("create transition");
    let at_s = EntityChainHeadV1::from_genesis(&create).expect("head at S");
    let update = CommittedEntityTransitionV1::new(
        CommitSequence::new(2).expect("sequence 2"),
        0,
        target.clone(),
        at_s.state(),
        at_s.chain_revision(),
        Some(at_s.last_transition_hash()),
        EntityChainStateV1::Live {
            version: EntityVersion::new(2).expect("version 2"),
            value_hash: v2_hash,
        },
    )
    .expect("update transition");
    let updated = at_s.apply(&update).expect("updated head");
    let delete = CommittedEntityTransitionV1::new(
        CommitSequence::new(3).expect("sequence 3"),
        0,
        target.clone(),
        updated.state(),
        updated.chain_revision(),
        Some(updated.last_transition_hash()),
        EntityChainStateV1::Deleted,
    )
    .expect("delete transition");
    let deleted = updated.apply(&delete).expect("deleted head");
    let recreate = CommittedEntityTransitionV1::new(
        CommitSequence::new(4).expect("sequence 4"),
        0,
        target.clone(),
        deleted.state(),
        deleted.chain_revision(),
        Some(deleted.last_transition_hash()),
        EntityChainStateV1::Live {
            version: EntityVersion::first(),
            value_hash: recreated_hash,
        },
    )
    .expect("recreate transition");
    let expected = deleted.apply(&recreate).expect("recreated head");

    let mut chains = std::collections::BTreeMap::from([(
        target.clone(),
        EntityChain {
            version: EntityVersion::first(),
            hash: Some(v1_hash),
            expected_bundle: None,
            migration_cursor: 0,
            intact: true,
            consumed: false,
        },
    )]);
    let mut heads = std::collections::BTreeMap::from([(target.clone(), at_s)]);
    let mut orphans = Vec::new();
    let mut overflow = false;
    for transition in [&update, &delete, &recreate] {
        apply_entity_transitions_to_startup_chains(
            &mut chains,
            &mut heads,
            &mut orphans,
            &mut overflow,
            1,
            1,
            false,
            transition.command_sequence(),
            std::slice::from_ref(transition),
        )
        .expect("advance suffix transition");
    }
    assert_eq!(heads.get(&target), Some(&expected));
    let chain = chains.get(&target).expect("materialized chain");
    assert!(chain.intact);
    assert_eq!(chain.version, EntityVersion::first());
    assert_eq!(chain.hash, Some(recreated_hash));
    assert!(orphans.is_empty());
    assert!(!overflow);
}

#[test]
fn entity_history_differential_oracle_agrees_on_populated_and_corrupted_histories() {
    let bundle = stored_bundle("entity-history", 1, b"entity-history-bundle");
    let plan = ExecutablePlanRef::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
        CommandId::first(),
        PlanHash::from_bytes([0x44; 32]),
    );

    // Intact histories plus one corrupted history with a false verdict.
    #[allow(clippy::type_complexity)]
    let cases: Vec<(
        &str,
        Vec<StoredEntityRecordV1>,
        Vec<StoredCommitRecordV1>,
        Vec<bool>, // expected oracle/chain per entity
    )> = {
        let e1_v1 = history_entity(1, EntityVersion::first(), b"a1", &bundle);
        let e1_v2 = history_entity(
            1,
            EntityVersion::first().checked_next().expect("v2"),
            b"a2",
            &bundle,
        );
        let e2_v1 = history_entity(2, EntityVersion::first(), b"b1", &bundle);
        let e2_v2 = history_entity(
            2,
            EntityVersion::first().checked_next().expect("v2"),
            b"b2",
            &bundle,
        );
        let h1_commits = vec![
            history_commit(CommitSequence::first(), plan.clone(), &[&e1_v1, &e2_v1]),
            history_commit(
                CommitSequence::new(2).expect("seq"),
                plan.clone(),
                &[&e1_v2],
            ),
            history_commit(
                CommitSequence::new(3).expect("seq"),
                plan.clone(),
                &[&e2_v2],
            ),
        ];

        let e3 = history_entity(3, EntityVersion::first(), b"c1", &bundle);
        let e4 = history_entity(4, EntityVersion::first(), b"d1", &bundle);
        let h2_commits = vec![
            history_commit(CommitSequence::first(), plan.clone(), &[&e3]),
            history_commit(CommitSequence::new(2).expect("seq"), plan.clone(), &[&e4]),
        ];

        // Corrupted: good sibling + gapped entity (false verdict required).
        let good = history_entity(10, EntityVersion::first(), b"good", &bundle);
        let bad_v1 = history_entity(11, EntityVersion::first(), b"bad1", &bundle);
        let bad_v3 = history_entity(11, EntityVersion::new(3).expect("v3"), b"bad3", &bundle);
        let h_bad_commits = vec![
            history_commit(CommitSequence::first(), plan.clone(), &[&good, &bad_v1]),
            history_commit(CommitSequence::new(2).expect("seq"), plan, &[&bad_v3]),
        ];

        vec![
            (
                "two-entity-versions",
                vec![e1_v2, e2_v2],
                h1_commits,
                vec![true, true],
            ),
            (
                "two-entity-creates",
                vec![e3, e4],
                h2_commits,
                vec![true, true],
            ),
            (
                "corrupted-gap",
                vec![good, bad_v3],
                h_bad_commits,
                vec![true, false],
            ),
        ]
    };

    for (index, (label, entities, commits, expected)) in cases.into_iter().enumerate() {
        let path = TestDatabasePath::new(&format!("entity-oracle-{index}"));
        let store = initialized_store(&path, database_id(0x81 + index as u8));
        write_entities_and_commits(
            &store,
            database_id(0x81 + index as u8),
            &bundle,
            &entities,
            &commits,
        );
        let transaction = store
            .shared
            .database
            .begin_read()
            .expect("read transaction");
        let chains = build_entity_chains(
            &transaction,
            u64::try_from(entities.len()).expect("entity count"),
            None,
        )
        .expect("build chains");
        assert!(
            !chains.chains.is_empty(),
            "{label}: must produce non-empty chains"
        );
        for (entity, expect_ok) in entities.iter().zip(expected.iter().copied()) {
            let oracle = entity_history_matches_oracle(&transaction, entity).expect("oracle check");
            // Production predicate (same function the structural session uses).
            let chain = entity_history_matches_chain(&chains.chains, entity);
            assert_eq!(
                oracle,
                chain,
                "{label} entity {:?} oracle={oracle} chain={chain}",
                entity.target().key().as_bytes()
            );
            assert_eq!(
                oracle, expect_ok,
                "{label} expected verdict {expect_ok}, got {oracle}"
            );
        }
        let mut session = store
            .begin_structural_evidence(inputs())
            .expect("begin structural");
        let (_end, findings) = collect_structural(&mut session);
        if expected.iter().all(|ok| *ok) {
            assert_eq!(
                findings,
                Vec::new(),
                "{label}: intact history must be clean: {:?}",
                finding_codes(&findings)
            );
        } else {
            assert_eq!(
                count_code(&findings, StructuralFindingCode::MissingCrossLink),
                1,
                "{label}: exactly one MissingCrossLink for the broken entity: {:?}",
                finding_codes(&findings)
            );
            assert_eq!(
                count_code(&findings, StructuralFindingCode::CrossLinkMismatch),
                0,
                "{label}: no orphan mismatches on a consumed broken chain: {:?}",
                finding_codes(&findings)
            );
        }
    }
}

#[test]
fn entity_chain_broken_version_gap_via_structural_session() {
    let path = TestDatabasePath::new("entity-gap");
    let store = initialized_store(&path, database_id(0x91));
    let bundle = stored_bundle("entity-history", 1, b"entity-history-bundle");
    let plan = ExecutablePlanRef::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
        CommandId::first(),
        PlanHash::from_bytes([0x45; 32]),
    );
    let good_v1 = history_entity(1, EntityVersion::first(), b"good1", &bundle);
    let good_v2 = history_entity(
        1,
        EntityVersion::first().checked_next().expect("v2"),
        b"good2",
        &bundle,
    );
    let bad_v1 = history_entity(2, EntityVersion::first(), b"bad1", &bundle);
    let bad_v3 = history_entity(2, EntityVersion::new(3).expect("v3"), b"bad3", &bundle);
    let commits = vec![
        history_commit(CommitSequence::first(), plan.clone(), &[&good_v1, &bad_v1]),
        history_commit(
            CommitSequence::new(2).expect("seq"),
            plan.clone(),
            &[&good_v2],
        ),
        history_commit(CommitSequence::new(3).expect("seq"), plan, &[&bad_v3]),
    ];
    write_entities_and_commits(
        &store,
        database_id(0x91),
        &bundle,
        &[good_v2.clone(), bad_v3.clone()],
        &commits,
    );

    let transaction = store.shared.database.begin_read().expect("read");
    let chains = build_entity_chains(&transaction, 2, None).expect("chains");
    assert!(entity_history_matches_chain(&chains.chains, &good_v2));
    assert!(!entity_history_matches_chain(&chains.chains, &bad_v3));

    let mut session = store
        .begin_structural_evidence(inputs())
        .expect("begin structural");
    let (_end, findings) = collect_structural(&mut session);
    // Exact set: one MissingCrossLink for the gapped entity; sibling clean.
    assert_eq!(
        finding_codes(&findings),
        vec![StructuralFindingCode::MissingCrossLink],
        "exact finding set for version gap"
    );
}

#[test]
fn entity_chain_fabricated_terminal_hash_via_structural_session() {
    let path = TestDatabasePath::new("entity-fab-hash");
    let store = initialized_store(&path, database_id(0x92));
    let bundle = stored_bundle("entity-history", 1, b"entity-history-bundle");
    let plan = ExecutablePlanRef::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
        CommandId::first(),
        PlanHash::from_bytes([0x46; 32]),
    );
    let a = history_entity(1, EntityVersion::first(), b"a", &bundle);
    let b = history_entity(2, EntityVersion::first(), b"b", &bundle);
    let commit = history_commit(CommitSequence::first(), plan, &[&a, &b]);
    let a_tampered = history_entity(1, EntityVersion::first(), b"TAMPERED", &bundle);
    write_entities_and_commits(
        &store,
        database_id(0x92),
        &bundle,
        &[a_tampered.clone(), b.clone()],
        &[commit],
    );

    let transaction = store.shared.database.begin_read().expect("read");
    let chains = build_entity_chains(&transaction, 2, None).expect("chains");
    assert!(entity_history_matches_chain(&chains.chains, &b));
    assert!(!entity_history_matches_chain(&chains.chains, &a_tampered));

    let mut session = store
        .begin_structural_evidence(inputs())
        .expect("begin structural");
    let (_end, findings) = collect_structural(&mut session);
    assert_eq!(
        finding_codes(&findings),
        vec![StructuralFindingCode::MissingCrossLink],
        "exact finding set for fabricated hash"
    );
}

#[test]
fn entity_chain_duplicate_target_in_one_commit_via_structural_session() {
    let path = TestDatabasePath::new("entity-dup-target");
    let store = initialized_store(&path, database_id(0x93));
    let bundle = stored_bundle("entity-history", 1, b"entity-history-bundle");
    let plan = ExecutablePlanRef::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
        CommandId::first(),
        PlanHash::from_bytes([0x47; 32]),
    );
    let a = history_entity(1, EntityVersion::first(), b"a", &bundle);
    let b = history_entity(2, EntityVersion::first(), b"b", &bundle);
    let commit = history_commit(CommitSequence::first(), plan, &[&a, &b]);
    // Write a clean graph first (outcomes/provenance/allocators), then corrupt
    // the commit wire with a duplicate entity reference.
    write_entities_and_commits(
        &store,
        database_id(0x93),
        &bundle,
        &[a.clone(), b.clone()],
        std::slice::from_ref(&commit),
    );
    let write = store.shared.database.begin_write().expect("write");
    {
        let encoded = codec::encode_commit_record_v1(&commit).expect("encode");
        let corrupted =
            riffdb_storage_api::inject_duplicate_entity_reference_v3(encoded.as_bytes())
                .expect("inject duplicate");
        let key = keys::encode_application_sequence_key(CommitSequence::first());
        write
            .open_table(COMMITS)
            .expect("commits")
            .insert(key.as_slice(), corrupted.as_bytes())
            .expect("insert");
    }
    write.commit().expect("commit");

    let transaction = store.shared.database.begin_read().expect("read");
    let chains = build_entity_chains(&transaction, 2, None).expect("chains");
    // First entity in the commit is marked broken by the duplicate; B intact.
    assert!(
        !entity_history_matches_chain(&chains.chains, &a),
        "duplicated target A must fail chain"
    );
    assert!(
        entity_history_matches_chain(&chains.chains, &b),
        "intact sibling B must pass chain"
    );

    let mut session = store
        .begin_structural_evidence(inputs())
        .expect("begin structural");
    let (_end, findings) = collect_structural(&mut session);
    // Chain finding for A + commit-row decode/reciprocal failure for the
    // duplicated wire (StoredCommitRecordV1 rejects duplicates on full decode).
    assert!(
        count_code(&findings, StructuralFindingCode::MissingCrossLink) >= 1,
        "duplicate must surface chain/commit findings: {:?}",
        finding_codes(&findings)
    );
    assert!(
        entity_history_matches_chain(&chains.chains, &b),
        "sibling must remain chain-intact"
    );
}

#[test]
fn entity_chain_orphan_overflow_via_structural_session() {
    let path = TestDatabasePath::new("entity-orphan-overflow");
    let store = initialized_store(&path, database_id(0x94));
    let bundle = stored_bundle("entity-history", 1, b"entity-history-bundle");
    let plan = ExecutablePlanRef::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
        CommandId::first(),
        PlanHash::from_bytes([0x48; 32]),
    );
    let real = history_entity(1, EntityVersion::first(), b"real", &bundle);
    let ghost = history_entity(99, EntityVersion::first(), b"ghost", &bundle);
    let commit = history_commit(CommitSequence::first(), plan, &[&real, &ghost]);
    write_entities_and_commits(
        &store,
        database_id(0x94),
        &bundle,
        std::slice::from_ref(&real),
        &[commit],
    );

    let transaction = store.shared.database.begin_read().expect("read");
    let chains = build_entity_chains(&transaction, 1, None).expect("chains");
    assert!(entity_history_matches_chain(&chains.chains, &real));
    assert!(chains.overflow || !chains.orphan_targets.is_empty());

    let mut session = store
        .begin_structural_evidence(inputs())
        .expect("begin structural");
    let (_end, findings) = collect_structural(&mut session);
    // Ghost is not covered by ENTITIES → commit MissingCrossLink + orphan
    // CrossLinkMismatch. Real entity stays chain-clean (no entity MissingCrossLink
    // for real alone beyond commit graph).
    assert!(
        count_code(&findings, StructuralFindingCode::CrossLinkMismatch) >= 1,
        "orphan CrossLinkMismatch required: {:?}",
        finding_codes(&findings)
    );
    assert!(
        count_code(&findings, StructuralFindingCode::MissingCrossLink) >= 1,
        "commit reciprocal for ghost: {:?}",
        finding_codes(&findings)
    );
}

#[test]
fn entity_chain_exact_count_masking_counterexample_reports_orphan() {
    // entities {E1,E2}, E2 never referenced, one commit references fabricated X
    // → len==2==count without the consumption/orphan fix would hide X.
    let path = TestDatabasePath::new("entity-masking");
    let store = initialized_store(&path, database_id(0x95));
    let bundle = stored_bundle("entity-history", 1, b"entity-history-bundle");
    let plan = ExecutablePlanRef::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
        CommandId::first(),
        PlanHash::from_bytes([0x49; 32]),
    );
    let e1 = history_entity(1, EntityVersion::first(), b"e1", &bundle);
    let e2 = history_entity(2, EntityVersion::first(), b"e2", &bundle);
    let fabricated = history_entity(77, EntityVersion::first(), b"fab", &bundle);
    // Commit references E1 and fabricated X — not E2.
    let commit = history_commit(CommitSequence::first(), plan, &[&e1, &fabricated]);
    write_entities_and_commits(
        &store,
        database_id(0x95),
        &bundle,
        &[e1.clone(), e2.clone()],
        &[commit],
    );

    let transaction = store.shared.database.begin_read().expect("read");
    let chains = build_entity_chains(&transaction, 2, None).expect("chains");
    assert!(
        entity_history_matches_chain(&chains.chains, &e1),
        "E1 referenced must pass"
    );
    assert!(
        !entity_history_matches_chain(&chains.chains, &e2),
        "E2 never referenced must fail"
    );
    // fabricated occupies a chain slot and remains unconsumed.
    assert!(
        chains.chains.values().any(|c| !c.consumed)
            || !chains.orphan_targets.is_empty()
            || chains.chains.len() == 2,
        "fabricated target must be tracked"
    );
    let mut session = store
        .begin_structural_evidence(inputs())
        .expect("begin structural");
    let (_end, findings) = collect_structural(&mut session);
    assert!(
        findings
            .iter()
            .any(|f| f.code() == StructuralFindingCode::MissingCrossLink),
        "E2 unreferenced: {findings:?}"
    );
    assert!(
        findings
            .iter()
            .any(|f| f.code() == StructuralFindingCode::CrossLinkMismatch),
        "fabricated orphan: {findings:?}"
    );
}

#[test]
fn entity_chain_empty_entities_with_referencing_commit_reports_orphan() {
    let path = TestDatabasePath::new("entity-empty-ents");
    let store = initialized_store(&path, database_id(0x96));
    let bundle = stored_bundle("entity-history", 1, b"entity-history-bundle");
    let plan = ExecutablePlanRef::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
        CommandId::first(),
        PlanHash::from_bytes([0x4a; 32]),
    );
    let ghost = history_entity(1, EntityVersion::first(), b"ghost", &bundle);
    let commit = history_commit(CommitSequence::first(), plan, &[&ghost]);
    // Zero ENTITIES rows; commit still references an entity.
    write_entities_and_commits(&store, database_id(0x96), &bundle, &[], &[commit]);

    let mut session = store
        .begin_structural_evidence(inputs())
        .expect("begin structural");
    let (_end, findings) = collect_structural(&mut session);
    assert!(
        count_code(&findings, StructuralFindingCode::CrossLinkMismatch) >= 1,
        "empty ENTITIES must still report commit-referenced orphans: {:?}",
        finding_codes(&findings)
    );
}

#[test]
fn entity_chain_missing_bundle_marks_row_not_orphan() {
    // NEW-2: commit-referenced entity with missing binding owns a chain;
    // row finding is MissingCrossLink and must NOT also orphan.
    let path = TestDatabasePath::new("entity-missing-bundle");
    let id = database_id(0x97);
    let store = initialized_store(&path, id);
    let bundle = stored_bundle("entity-history", 1, b"entity-history-bundle");
    let plan = ExecutablePlanRef::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
        CommandId::first(),
        PlanHash::from_bytes([0x4b; 32]),
    );
    // Entity with a binding that does not exist in CONTRACT_BUNDLES, but is
    // commit-referenced so it owns a chain slot.
    let entity_type = EntityTypeId::new(7).expect("type");
    let mut key = EntityKeyBuilder::new(entity_type);
    key.push_u64(2).expect("key");
    let target = EntityTarget::new(entity_type, key.finish().expect("finish")).expect("target");
    let ghost_binding = DurableKeySchemaBindingV1::new(
        ContractLineage::new("missing-lineage-for-binding").expect("lineage"),
        ContractVersion::new(1).expect("version"),
        ContractBundleHash::from_bytes([0xee; 32]),
    );
    let field_id = FieldId::new(1).expect("field");
    let record = CanonicalRecord::new(vec![(
        field_id,
        CanonicalValue::bytes(b"orphan-check".to_vec()).expect("bytes"),
    )])
    .expect("fields");
    let missing_binding_entity = StoredEntityRecordV1::new(
        target,
        EntityVersion::first(),
        ContractVersion::new(1).expect("version"),
        ghost_binding,
        record,
    )
    .expect("entity");
    let commit = history_commit(CommitSequence::first(), plan, &[&missing_binding_entity]);
    write_entities_and_commits(
        &store,
        id,
        &bundle,
        std::slice::from_ref(&missing_binding_entity),
        &[commit],
    );

    let mut session = store
        .begin_structural_evidence(inputs())
        .expect("begin structural");
    let (_end, findings) = collect_structural(&mut session);
    assert!(
        count_code(&findings, StructuralFindingCode::MissingCrossLink) >= 1,
        "missing binding must produce MissingCrossLink: {:?}",
        finding_codes(&findings)
    );
    assert_eq!(
        count_code(&findings, StructuralFindingCode::CrossLinkMismatch),
        0,
        "commit-referenced row with missing bundle must not orphan: {:?}",
        finding_codes(&findings)
    );
}

#[test]
fn entity_orphan_drain_on_single_finishing_page() {
    // NEW-1(a): empty-ENTITIES orphan, limit=256, structural_total < 239 →
    // single finishing page executes the reserved orphan drain.
    let path = TestDatabasePath::new("entity-orphan-finish");
    let store = initialized_store(&path, database_id(0x98));
    let bundle = stored_bundle("entity-history", 1, b"entity-history-bundle");
    let plan = ExecutablePlanRef::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
        CommandId::first(),
        PlanHash::from_bytes([0x4c; 32]),
    );
    let ghost = history_entity(1, EntityVersion::first(), b"ghost", &bundle);
    let commit = history_commit(CommitSequence::first(), plan, &[&ghost]);
    write_entities_and_commits(&store, database_id(0x98), &bundle, &[], &[commit]);

    let mut session = store
        .begin_structural_evidence(inputs())
        .expect("begin structural");
    assert!(
        session.structural_total < 239,
        "fixture must be a single finishing page under limit=256 (total={})",
        session.structural_total
    );
    let large = EvidencePageLimit::new(256).expect("limit");
    let mut cursor =
        StructuralEvidenceCursor::start(session.database_id(), session.open_session_id());
    let mut collected = Vec::new();
    let mut advancing_pages = 0u32;
    while let StructuralEvidencePage::Page {
        start,
        findings,
        next,
    } = session
        .read_structural_evidence(cursor, large)
        .expect("finishing page must advance and drain orphans")
    {
        assert!(next.position() > start.position());
        advancing_pages += 1;
        collected.extend(findings);
        cursor = next;
    }
    assert_eq!(
        advancing_pages, 1,
        "expected exactly one finishing page for total < 239"
    );
    assert!(
        count_code(&collected, StructuralFindingCode::CrossLinkMismatch) >= 1,
        "orphan findings must drain on the finishing page: {:?}",
        finding_codes(&collected)
    );
}

#[test]
fn entity_orphan_drain_after_reservation_clamp() {
    // NEW-1(b): structural_total in 240..=255 → reservation clamp splits the
    // would-be finishing page; follow-up page drains orphans.
    let path = TestDatabasePath::new("entity-orphan-clamp");
    let store = initialized_store(&path, database_id(0x99));
    let bundle = stored_bundle("entity-history", 1, b"entity-history-bundle");
    let plan = ExecutablePlanRef::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
        CommandId::first(),
        PlanHash::from_bytes([0x4d; 32]),
    );
    let ghost = history_entity(1, EntityVersion::first(), b"ghost", &bundle);
    // Probe baseline size with one commit, then pad commits to land total
    // in 240..=255.
    write_entities_and_commits(
        &store,
        database_id(0x99),
        &bundle,
        &[],
        &[history_commit(
            CommitSequence::first(),
            plan.clone(),
            &[&ghost],
        )],
    );
    let baseline = {
        let session = store
            .begin_structural_evidence(inputs())
            .expect("baseline session");
        session.structural_total
    };
    // Re-open after the baseline session consumes the store handle.
    let store = RedbStore::open(&path.0).expect("reopen");
    // Each additional commit also adds outcome + provenance rows (+3 total).
    // Need structural_total in 240..=255.
    let target_total = 248u64;
    assert!(
        baseline < target_total,
        "baseline {baseline} already exceeds clamp window"
    );
    let extra_needed = target_total.saturating_sub(baseline);
    // Rough: each commit group adds ~3 rows; overshoot slightly then trim.
    let extra_commits = (extra_needed / 3) + 2;
    let mut commits = vec![history_commit(
        CommitSequence::first(),
        plan.clone(),
        &[&ghost],
    )];
    for seq in 2..=(1 + extra_commits) {
        commits.push(history_commit(
            CommitSequence::new(seq).expect("seq"),
            plan.clone(),
            &[&ghost],
        ));
    }
    // Rewrite fixture with padded commits.
    write_entities_and_commits(&store, database_id(0x99), &bundle, &[], &commits);
    let mut session = store
        .begin_structural_evidence(inputs())
        .expect("begin structural");
    let total = session.structural_total;
    assert!(
        (240..=255).contains(&total),
        "need total in 240..=255 for clamp (got {total}; baseline was {baseline}, commits={})",
        commits.len()
    );

    let large = EvidencePageLimit::new(256).expect("limit");
    let mut cursor =
        StructuralEvidenceCursor::start(session.database_id(), session.open_session_id());
    let mut collected = Vec::new();
    let mut advancing_pages = 0u32;
    while let StructuralEvidencePage::Page {
        start,
        findings,
        next,
    } = session
        .read_structural_evidence(cursor, large)
        .expect("clamp + follow-up finishing page must complete")
    {
        assert!(next.position() > start.position());
        advancing_pages += 1;
        collected.extend(findings);
        cursor = next;
    }
    assert!(
        advancing_pages >= 2,
        "reservation clamp must force ≥2 advancing pages (got {advancing_pages}, total={total})"
    );
    assert!(
        count_code(&collected, StructuralFindingCode::CrossLinkMismatch) >= 1,
        "orphans must drain after clamp: {:?}",
        finding_codes(&collected)
    );
}

#[test]
fn mark_entity_chain_consumed_is_logarithmic_not_quadratic() {
    // NEW-6: O(N log N) map lookup vs O(N²) full-map scan.
    // Reviewer measured quadratic mark alone at ~181ms @ 8k (release);
    // logarithmic marking of 8k entries is sub-millisecond even in debug.
    use std::time::Instant;
    let mut chains = std::collections::BTreeMap::new();
    let mut keys = Vec::new();
    const N: u64 = 8_000;
    for i in 1..=N {
        let entity_type = EntityTypeId::new(7).expect("type");
        let mut key = EntityKeyBuilder::new(entity_type);
        key.push_u64(i).expect("component");
        let entity_key = key.finish().expect("key");
        let target = EntityTarget::new(entity_type, entity_key.clone()).expect("target");
        chains.insert(
            target,
            EntityChain {
                version: EntityVersion::first(),
                hash: Some(riffdb_types::EntityRecordHash::from_bytes([0xab; 32])),
                expected_bundle: None,
                migration_cursor: 0,
                intact: true,
                consumed: false,
            },
        );
        keys.push(entity_key);
    }
    let started = Instant::now();
    for key in &keys {
        mark_entity_chain_consumed(&mut chains, key);
    }
    let elapsed = started.elapsed();
    assert!(
        chains.values().all(|c| c.consumed),
        "every chain must be marked"
    );
    // Quadratic 8k was ~181ms release; allow 50ms debug headroom for log-time.
    assert!(
        elapsed.as_millis() < 50,
        "marking {N} chains took {elapsed:?} (expected O(N log N) ≪ 50ms)"
    );
}

#[test]
fn catalog_chain_accepts_reactivation_and_rejects_a_wrong_previous_pointer() {
    let path = TestDatabasePath::new("catalog-chain");
    let store = initialized_store(&path, database_id(0x72));
    let first_bundle = stored_bundle("catalog-chain", 1, b"catalog-one");
    let second_bundle = stored_bundle("catalog-chain", 2, b"catalog-two");
    let first_pointer = ActiveCatalogPointerV1::from_bundle(&first_bundle);
    let second_pointer = ActiveCatalogPointerV1::from_bundle(&second_bundle);
    let first = StoredCatalogAdministrationV1::from_stored_parts(
        AdministrationSequence::first(),
        request_id(0x31),
        Timestamp::new(1, 0).expect("timestamp"),
        audit_principal(0x41),
        None,
        first_pointer.clone(),
        None,
    );
    let second = StoredCatalogAdministrationV1::from_stored_parts(
        AdministrationSequence::new(2).expect("sequence"),
        request_id(0x32),
        Timestamp::new(2, 0).expect("timestamp"),
        audit_principal(0x41),
        Some(first_pointer.clone()),
        second_pointer.clone(),
        None,
    );
    let third = StoredCatalogAdministrationV1::from_stored_parts(
        AdministrationSequence::new(3).expect("sequence"),
        request_id(0x33),
        Timestamp::new(3, 0).expect("timestamp"),
        audit_principal(0x41),
        Some(second_pointer),
        first_pointer,
        None,
    );
    let encoded_first_bundle =
        codec::encode_contract_bundle_v1(&first_bundle).expect("encode first bundle");
    let encoded_second_bundle =
        codec::encode_contract_bundle_v1(&second_bundle).expect("encode second bundle");
    let encoded_first = codec::encode_administration_audit_record_v1(
        &StoredAdministrationAuditRecordV1::Catalog(first.clone()),
    )
    .expect("encode first activation");
    let encoded_second = codec::encode_administration_audit_record_v1(
        &StoredAdministrationAuditRecordV1::Catalog(second.clone()),
    )
    .expect("encode second activation");
    let encoded_third = codec::encode_administration_audit_record_v1(
        &StoredAdministrationAuditRecordV1::Catalog(third.clone()),
    )
    .expect("encode third activation");
    let first_bundle_key =
        keys::encode_contract_bundle_key(first_bundle.lineage(), first_bundle.contract_version())
            .expect("first bundle key");
    let second_bundle_key =
        keys::encode_contract_bundle_key(second_bundle.lineage(), second_bundle.contract_version())
            .expect("second bundle key");
    let write = store
        .shared
        .database
        .begin_write()
        .expect("write transaction");
    {
        let mut bundles = write.open_table(CONTRACT_BUNDLES).expect("bundle table");
        bundles
            .insert(first_bundle_key.as_slice(), encoded_first_bundle.as_bytes())
            .expect("insert first bundle");
        bundles
            .insert(
                second_bundle_key.as_slice(),
                encoded_second_bundle.as_bytes(),
            )
            .expect("insert second bundle");
    }
    {
        let mut audit = write.open_table(AUDIT).expect("audit table");
        audit
            .insert(
                keys::encode_audit_key(first.administration_sequence()).as_slice(),
                encoded_first.as_bytes(),
            )
            .expect("insert first activation");
        audit
            .insert(
                keys::encode_audit_key(second.administration_sequence()).as_slice(),
                encoded_second.as_bytes(),
            )
            .expect("insert second activation");
        audit
            .insert(
                keys::encode_audit_key(third.administration_sequence()).as_slice(),
                encoded_third.as_bytes(),
            )
            .expect("insert third activation");
    }
    write.commit().expect("commit fixture");

    let read = store
        .shared
        .database
        .begin_read()
        .expect("read transaction");
    assert!(catalog_record_is_reciprocal(&read, &first).expect("first chain link"));
    assert!(catalog_record_is_reciprocal(&read, &second).expect("second chain link"));
    assert!(catalog_record_is_reciprocal(&read, &third).expect("reactivation chain link"));
    assert!(bundle_has_activation(&read, &first_bundle).expect("first activation"));
    assert!(bundle_has_activation(&read, &second_bundle).expect("second activation"));
    assert!(
        active_catalog_matches_last_activation(&read, Some(third.activated()))
            .expect("latest activation")
    );
    drop(read);

    let wrong_second = StoredCatalogAdministrationV1::from_stored_parts(
        second.administration_sequence(),
        second.request_id(),
        second.timestamp(),
        second.principal().clone(),
        None,
        second.activated().clone(),
        second.approval_id().cloned(),
    );
    let encoded_wrong_second = codec::encode_administration_audit_record_v1(
        &StoredAdministrationAuditRecordV1::Catalog(wrong_second.clone()),
    )
    .expect("encode wrong second activation");
    let write = store
        .shared
        .database
        .begin_write()
        .expect("write transaction");
    {
        let mut audit = write.open_table(AUDIT).expect("audit table");
        audit
            .insert(
                keys::encode_audit_key(wrong_second.administration_sequence()).as_slice(),
                encoded_wrong_second.as_bytes(),
            )
            .expect("replace second activation");
    }
    write.commit().expect("commit corruption");
    let read = store
        .shared
        .database
        .begin_read()
        .expect("read transaction");
    assert!(!catalog_record_is_reciprocal(&read, &wrong_second).expect("wrong previous pointer"));
}

#[test]
fn capability_create_and_revoke_audits_are_checked_in_both_directions() {
    let path = TestDatabasePath::new("capability-audit");
    let database_id = database_id(0x73);
    let store = initialized_store(&path, database_id);
    let capability_id = capability_id(0x51);
    let issued_at = Timestamp::new(10, 0).expect("issued at");
    let active = StoredCapabilityRecordV1::active(
        capability_id,
        CapabilityTokenDigest::from_hmac_bytes(
            DigestKeyId::new(1).expect("digest key ID"),
            [0x61; 32],
        ),
        requested_capability(database_id),
        issued_at,
        Timestamp::new(70, 0).expect("expires at"),
        AdministrationSequence::first(),
        request_id(0x52),
    )
    .expect("active capability");
    let revoked_at = Timestamp::new(20, 0).expect("revoked at");
    let revoked = active
        .revoked(
            NonZeroU64::MIN,
            revoked_at,
            AdministrationSequence::new(2).expect("revoke sequence"),
            RevocationReasonCodeV1::Requested,
        )
        .expect("revoked capability");
    let create = StoredCapabilityAdministrationV1::new(
        AdministrationSequence::first(),
        active.creation_request_id(),
        CapabilityAdministrationOperationV1::Create,
        issued_at,
        Some(audit_principal(0x41)),
        capability_id,
        NonZeroU64::MIN,
        None,
        None,
    )
    .expect("create audit");
    let revoke = StoredCapabilityAdministrationV1::new(
        AdministrationSequence::new(2).expect("revoke sequence"),
        request_id(0x53),
        CapabilityAdministrationOperationV1::Revoke,
        revoked_at,
        Some(audit_principal(0x41)),
        capability_id,
        NonZeroU64::new(2).expect("revision"),
        None,
        Some(RevocationReasonCodeV1::Requested),
    )
    .expect("revoke audit");
    let encoded_capability =
        codec::encode_capability_record_v1(&revoked).expect("encode capability");
    let encoded_create = codec::encode_administration_audit_record_v1(
        &StoredAdministrationAuditRecordV1::Capability(create.clone()),
    )
    .expect("encode create");
    let encoded_revoke = codec::encode_administration_audit_record_v1(
        &StoredAdministrationAuditRecordV1::Capability(revoke.clone()),
    )
    .expect("encode revoke");
    let write = store
        .shared
        .database
        .begin_write()
        .expect("write transaction");
    {
        let mut capabilities = write.open_table(CAPABILITIES).expect("capability table");
        capabilities
            .insert(
                keys::encode_capability_key(capability_id).as_slice(),
                encoded_capability.as_bytes(),
            )
            .expect("insert capability");
    }
    {
        let mut audit = write.open_table(AUDIT).expect("audit table");
        audit
            .insert(
                keys::encode_audit_key(create.administration_sequence()).as_slice(),
                encoded_create.as_bytes(),
            )
            .expect("insert create");
        audit
            .insert(
                keys::encode_audit_key(revoke.administration_sequence()).as_slice(),
                encoded_revoke.as_bytes(),
            )
            .expect("insert revoke");
    }
    write.commit().expect("commit fixture");
    {
        let read = store
            .shared
            .database
            .begin_read()
            .expect("read transaction");
        assert_eq!(
            capability_record_audit_status(&read, &revoked).expect("record links"),
            CrossLinkStatus::Exact
        );
        assert_eq!(
            capability_administration_status(&read, &create).expect("create link"),
            CrossLinkStatus::Exact
        );
        assert_eq!(
            capability_administration_status(&read, &revoke).expect("revoke link"),
            CrossLinkStatus::Exact
        );
    }

    let wrong_revoke = StoredCapabilityAdministrationV1::new(
        revoke.administration_sequence(),
        revoke.request_id(),
        CapabilityAdministrationOperationV1::Revoke,
        revoke.timestamp(),
        Some(audit_principal(0x41)),
        capability_id,
        revoke.resulting_revision(),
        None,
        Some(RevocationReasonCodeV1::PolicyChange),
    )
    .expect("type-valid wrong revoke");
    let encoded_wrong = codec::encode_administration_audit_record_v1(
        &StoredAdministrationAuditRecordV1::Capability(wrong_revoke.clone()),
    )
    .expect("encode wrong revoke");
    let write = store
        .shared
        .database
        .begin_write()
        .expect("write transaction");
    {
        let mut audit = write.open_table(AUDIT).expect("audit table");
        audit
            .insert(
                keys::encode_audit_key(wrong_revoke.administration_sequence()).as_slice(),
                encoded_wrong.as_bytes(),
            )
            .expect("replace revoke");
    }
    write.commit().expect("commit corruption");
    let read = store
        .shared
        .database
        .begin_read()
        .expect("read transaction");
    assert_eq!(
        capability_record_audit_status(&read, &revoked).expect("record mismatch"),
        CrossLinkStatus::Mismatch
    );
    assert_eq!(
        capability_administration_status(&read, &wrong_revoke).expect("audit mismatch"),
        CrossLinkStatus::Mismatch
    );
}

#[test]
fn duplicate_standalone_service_lifecycle_is_authoritative_corruption() {
    let path = TestDatabasePath::new("duplicate-service");
    let store = initialized_store(&path, database_id(0x74));
    let request_id = request_id(0x61);
    let first = StoredServiceAuditRecordV1::from_stored_parts(
        AdministrationSequence::first(),
        request_id,
        Timestamp::new(1, 0).expect("timestamp"),
        ServiceOperationV1::GetHealth,
        ServiceAuditPhaseV1::Denied,
        Some(audit_principal(0x62)),
        ServiceIngressKindV1::Grpc,
        ServiceAuditTargetsV1::empty(),
        None,
        ServiceAuditLinkV1::None,
    )
    .expect("first standalone audit");
    let second = StoredServiceAuditRecordV1::from_stored_parts(
        AdministrationSequence::new(2).expect("sequence"),
        request_id,
        Timestamp::new(2, 0).expect("timestamp"),
        ServiceOperationV1::GetHealth,
        ServiceAuditPhaseV1::Denied,
        Some(audit_principal(0x62)),
        ServiceIngressKindV1::Grpc,
        ServiceAuditTargetsV1::empty(),
        None,
        ServiceAuditLinkV1::None,
    )
    .expect("second standalone audit");
    let encoded_first = codec::encode_administration_audit_record_v1(
        &StoredAdministrationAuditRecordV1::Service(first),
    )
    .expect("encode first service audit");
    let encoded_second = codec::encode_administration_audit_record_v1(
        &StoredAdministrationAuditRecordV1::Service(second),
    )
    .expect("encode second service audit");
    let allocator =
        codec::encode_administration_sequence_allocator_v1(AdministrationSequenceAllocator::next(
            AdministrationSequence::new(3).expect("allocator sequence"),
        ))
        .expect("encode allocator");
    let write = store
        .shared
        .database
        .begin_write()
        .expect("write transaction");
    {
        let mut audit = write.open_table(AUDIT).expect("audit table");
        audit
            .insert(
                keys::encode_audit_key(AdministrationSequence::first()).as_slice(),
                encoded_first.as_bytes(),
            )
            .expect("insert first service audit");
        audit
            .insert(
                keys::encode_audit_key(AdministrationSequence::new(2).expect("second sequence"))
                    .as_slice(),
                encoded_second.as_bytes(),
            )
            .expect("insert second service audit");
    }
    {
        // Index rows present so lifecycle (not MissingCrossLink) is the defect.
        let mut index = write
            .open_table(AUDIT_BY_REQUEST)
            .expect("audit-by-request table");
        for sequence in [
            AdministrationSequence::first(),
            AdministrationSequence::new(2).expect("second sequence"),
        ] {
            let key = keys::encode_audit_by_request_key(request_id, sequence);
            let encoded = codec::encode_service_audit_request_index_v1(
                riffdb_storage_api::StoredServiceAuditRequestIndexV1::new(request_id, sequence),
            )
            .expect("encode index");
            index
                .insert(key.as_slice(), encoded.as_bytes())
                .expect("insert index row");
        }
    }
    {
        let mut meta = write.open_table(META).expect("metadata table");
        meta.insert(META_ADMINISTRATION_SEQUENCE, allocator.as_bytes())
            .expect("advance allocator");
    }
    write.commit().expect("commit fixture");

    let mut session = store
        .begin_structural_evidence(inputs())
        .expect("begin evidence");
    let (structural_end, findings) = collect_structural(&mut session);
    assert!(findings.iter().all(|finding| {
        finding.scope() == StructuralFindingScope::Authoritative
            && finding.code() == StructuralFindingCode::CrossLinkMismatch
    }));
    assert_eq!(findings.len(), 2);
    let historical_end = finish_historical(&mut session);
    let error = session
        .finish(structural_end, historical_end)
        .expect_err("authoritative findings must withhold ports");
    assert_eq!(error.kind(), StorageErrorKind::CorruptData);

    let reopened = RedbStore::open(&path.0).expect("reopen corrupt fixture");
    let mut repeated = reopened
        .begin_structural_evidence(inputs())
        .expect("repeat evidence");
    let (repeated_structural_end, repeated_findings) = collect_structural(&mut repeated);
    assert_eq!(repeated_findings, findings);
    let repeated_historical_end = finish_historical(&mut repeated);
    let repeated_error = repeated
        .finish(repeated_structural_end, repeated_historical_end)
        .expect_err("repeated authoritative findings must withhold ports");
    assert_eq!(repeated_error.kind(), StorageErrorKind::CorruptData);
}

#[test]
fn projection_frontier_and_marker_prefix_defects_are_derived_only() {
    let path = TestDatabasePath::new("before-first-marker");
    let store = initialized_store(&path, database_id(0x75));
    let identity = projection_identity();
    let control = StoredProjectionControlV1::initial(identity.clone());
    let marker_key = ProjectionApplyKey::new(
        identity,
        ProjectionGeneration::first(),
        CommitSequence::first(),
    );
    let marker = StoredProjectionApplyV1::new(
        marker_key.clone(),
        ProjectionApplyHash::from_bytes([0x92; 32]),
    );
    let gap_identity = ProjectionIdentity::new(
        ContractLineage::new("projection-gap").expect("lineage"),
        ProjectionId::new(2).expect("projection ID"),
        ProjectionPlanHash::from_bytes([0x93; 32]),
    );
    let gap_frontier = CommitSequence::new(3).expect("gap frontier");
    let gap_control = StoredProjectionControlV1::new(
        gap_identity.clone(),
        ProjectionGeneration::first(),
        Some(ProjectionGenerationPosition::new(
            ProjectionGeneration::first(),
            FrontierPosition::AppliedThrough(gap_frontier),
        )),
        None,
        Some(PublishedApplyModeV1::Enabled),
        ProjectionLifecycleV1::Ready,
        None,
    )
    .expect("gap control");
    let gap_first_key = ProjectionApplyKey::new(
        gap_identity.clone(),
        ProjectionGeneration::first(),
        CommitSequence::first(),
    );
    let gap_last_key =
        ProjectionApplyKey::new(gap_identity, ProjectionGeneration::first(), gap_frontier);
    let gap_first = StoredProjectionApplyV1::new(
        gap_first_key.clone(),
        ProjectionApplyHash::from_bytes([0x94; 32]),
    );
    let gap_last = StoredProjectionApplyV1::new(
        gap_last_key.clone(),
        ProjectionApplyHash::from_bytes([0x95; 32]),
    );
    let encoded_control = codec::encode_projection_control_v1(&control).expect("encode control");
    let encoded_marker = codec::encode_projection_apply_v1(&marker).expect("encode marker");
    let encoded_gap_control =
        codec::encode_projection_control_v1(&gap_control).expect("encode gap control");
    let encoded_gap_first =
        codec::encode_projection_apply_v1(&gap_first).expect("encode first gap marker");
    let encoded_gap_last =
        codec::encode_projection_apply_v1(&gap_last).expect("encode last gap marker");
    let control_key = riffdb_types::ProjectionFrontierKey::new(control.identity().clone());
    let gap_control_key = riffdb_types::ProjectionFrontierKey::new(gap_control.identity().clone());
    let write = store
        .shared
        .database
        .begin_write()
        .expect("write transaction");
    {
        let mut controls = write
            .open_table(PROJECTION_FRONTIER)
            .expect("control table");
        controls
            .insert(control_key.as_bytes(), encoded_control.as_bytes())
            .expect("insert control");
        controls
            .insert(gap_control_key.as_bytes(), encoded_gap_control.as_bytes())
            .expect("insert gap control");
    }
    {
        let mut markers = write.open_table(PROJECTION_APPLIED).expect("marker table");
        markers
            .insert(marker_key.as_bytes(), encoded_marker.as_bytes())
            .expect("insert marker");
        markers
            .insert(gap_first_key.as_bytes(), encoded_gap_first.as_bytes())
            .expect("insert first gap marker");
        markers
            .insert(gap_last_key.as_bytes(), encoded_gap_last.as_bytes())
            .expect("insert last gap marker");
    }
    write.commit().expect("commit fixture");

    {
        let read = store
            .shared
            .database
            .begin_read()
            .expect("read transaction");
        assert_eq!(
            inspect_projection_apply_row(
                &read,
                gap_last_key.as_bytes(),
                encoded_gap_last.as_bytes(),
            )
            .expect("inspect marker gap"),
            Some(derived_projection(
                StructuralFindingCode::ProjectionStateMismatch,
            ))
        );
    }

    let mut session = store
        .begin_structural_evidence(inputs())
        .expect("begin evidence");
    let (structural_end, findings) = collect_structural(&mut session);
    assert!(!findings.is_empty());
    assert!(findings.iter().all(|finding| {
        finding.scope() == StructuralFindingScope::Projection
            && finding.code() == StructuralFindingCode::ProjectionStateMismatch
    }));
    let historical_end = finish_historical(&mut session);
    let outcome = session
        .finish(structural_end, historical_end)
        .expect("derived findings do not withhold ports");
    let StructuralOpenOutcome::Clean(opened) = outcome else {
        panic!("V2 derived-state fixture must finish cleanly");
    };
    assert_eq!(opened.database_id(), database_id(0x75));
}

/// Collects the linear historical plan as a full (order_key, evidence) sequence.
/// Each order_key is checked against `historical_order_key` of the materialized
/// evidence (self-oracle). Pre-deletion differential vs `select_next_historical`
/// was equal on empty/mixed/plans fixtures before that oracle was removed.
fn collect_historical_plan_sequence(
    path: &std::path::Path,
) -> Vec<(Vec<u8>, HistoricalSemanticEvidence)> {
    let store = RedbStore::open(path).expect("open");
    let transaction = store.shared.database.begin_read().expect("read");
    let validation = inputs_at(70);
    let mut plan = build_historical_evidence_plan(&transaction, &validation).expect("plan");
    let tables = HistoricalMaterializationTables::open(&transaction).expect("tables");
    let mut sequence = Vec::new();
    while let Some(PendingHistoricalEvidence {
        order_key,
        evidence,
    }) = plan.next(&transaction, &tables).expect("materialize")
    {
        assert_eq!(
            order_key,
            historical_order_key(&evidence),
            "plan order_key must match materialized evidence"
        );
        sequence.push((order_key, evidence));
    }
    sequence
}

/// Canonical characterization bytes for one complete historical item.
///
/// The order key already contains every field of plans, active-catalog,
/// persisted-key, and capability-partition evidence. Payload-bearing
/// variants additionally retain their exact canonical bytes here, so one
/// digest line pins both the locator order and the materialized item.
fn historical_sequence_fixture_line(
    order_key: &[u8],
    evidence: &HistoricalSemanticEvidence,
) -> String {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(
        &u32::try_from(order_key.len())
            .expect("bounded historical order key")
            .to_be_bytes(),
    );
    bytes.extend_from_slice(order_key);
    match evidence {
        HistoricalSemanticEvidence::Bundle(bundle) => {
            bytes.push(0x01);
            bytes.extend_from_slice(
                &u32::try_from(bundle.bytes().as_bytes().len())
                    .expect("bounded bundle")
                    .to_be_bytes(),
            );
            bytes.extend_from_slice(bundle.bytes().as_bytes());
        }
        HistoricalSemanticEvidence::ContractMigrationEdge(edge) => {
            bytes.push(0x02);
            let retirement = riffdb_storage_api::proto_codec::encode_contract_write_retirement_v1(
                edge.retirement(),
            )
            .expect("encode retirement fixture");
            let migration = riffdb_storage_api::proto_codec::encode_contract_migration_record_v1(
                edge.migration(),
            )
            .expect("encode migration fixture");
            for payload in [retirement.as_bytes(), migration.as_bytes()] {
                bytes.extend_from_slice(
                    &u32::try_from(payload.len())
                        .expect("bounded migration fixture")
                        .to_be_bytes(),
                );
                bytes.extend_from_slice(payload);
            }
        }
        HistoricalSemanticEvidence::PlanReference(_) => bytes.push(0x03),
        HistoricalSemanticEvidence::ActiveCatalog(_) => bytes.push(0x04),
        HistoricalSemanticEvidence::PersistedKey(_) => bytes.push(0x05),
        HistoricalSemanticEvidence::IndexMigrationRow(row) => {
            bytes.push(0x06);
            bytes.extend_from_slice(
                &u32::try_from(row.canonical_envelope().len())
                    .expect("bounded index envelope")
                    .to_be_bytes(),
            );
            bytes.extend_from_slice(row.canonical_envelope());
        }
        HistoricalSemanticEvidence::CapabilityPartition(_) => bytes.push(0x07),
    }
    let digest = hash_contract_bundle(&bytes);
    digest
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn historical_sequence_fixture(sequence: &[(Vec<u8>, HistoricalSemanticEvidence)]) -> String {
    let canonical_lines = sequence
        .iter()
        .map(|(order_key, evidence)| historical_sequence_fixture_line(order_key, evidence))
        .collect::<Vec<_>>()
        .join("\n");
    let digest = hash_contract_bundle(canonical_lines.as_bytes());
    let digest = digest
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("items={}\ndigest={digest}\n", sequence.len())
}

fn oracle_plan_ref(seed: u8) -> ExecutablePlanRef {
    ExecutablePlanRef::new(
        ContractLineage::new("oracle-app").expect("lineage"),
        ContractVersion::new(1).expect("version"),
        ContractBundleHash::from_bytes([seed; 32]),
        CommandId::new(u32::from(seed).max(1)).expect("command"),
        PlanHash::from_bytes([seed.wrapping_add(1); 32]),
    )
}

fn oracle_commit(sequence: CommitSequence, plan: ExecutablePlanRef) -> StoredCommitRecordV1 {
    let sequence_byte = u8::try_from(sequence.get()).expect("small sequence");
    let actor = AdmittedActorContext::new(
        ActorId::new("oracle-actor").expect("actor"),
        ActorKind::Human,
        TenantScope::Global,
        None,
    );
    let mut partition = PartitionKeyBuilder::new(AggregateTypeId::first());
    partition.push_u64(1).expect("partition component");
    let partition_hash = hash_partition_key(partition.finish().expect("partition key").as_bytes());
    StoredCommitRecordV1::new(
        sequence,
        RequestId::from_bytes(uuid_bytes(sequence_byte.wrapping_add(0x20))).expect("request ID"),
        plan,
        CanonicalInputHash::from_bytes([sequence_byte; 32]),
        actor,
        LogicalTime::new(Timestamp::new(i64::from(sequence_byte), 0).expect("timestamp")),
        partition_hash,
        Vec::new(),
        StoredReadDependenciesV1::from_live(
            &ReadDependencies::new(Vec::new()).expect("empty dependencies"),
        )
        .expect("stored dependencies"),
        Vec::new(),
        Vec::new(),
        DeclaredOutcome::new(
            OutcomeId::first(),
            CanonicalRecord::new(Vec::new()).expect("outcome fields"),
        )
        .expect("outcome"),
        ProvenanceId::from_bytes(uuid_bytes(sequence_byte.wrapping_add(0x40)))
            .expect("provenance ID"),
        Vec::new(),
        DurabilityMode::Sync,
    )
    .expect("stored commit")
}

fn populate_mixed_oracle_fixture(path: &TestDatabasePath, id: DatabaseId) {
    let store = initialized_store(path, id);
    let lineage = ContractLineage::new("oracle-app").expect("lineage");
    let bundles = (1..=4)
        .map(|version| {
            stored_bundle(
                "oracle-app",
                version,
                format!("oracle-bundle-{version}").as_bytes(),
            )
        })
        .collect::<Vec<_>>();
    let active = ActiveCatalogPointerV1::from_bundle(&bundles[3]);
    let binding = DurableKeySchemaBindingV1::new(
        lineage.clone(),
        bundles[3].contract_version(),
        bundles[3].bundle_hash(),
    );
    let entities = (1..=260)
        .map(|value| {
            let entity_type =
                EntityTypeId::new(if value % 2 == 0 { 7 } else { 9 }).expect("entity type");
            let mut key = EntityKeyBuilder::new(entity_type);
            key.push_u64(value).expect("entity component");
            StoredEntityRecordV1::new(
                EntityTarget::new(entity_type, key.finish().expect("entity key")).expect("target"),
                EntityVersion::new(1).expect("entity version"),
                bundles[3].contract_version(),
                binding.clone(),
                CanonicalRecord::new(Vec::new()).expect("fields"),
            )
            .expect("entity")
        })
        .collect::<Vec<_>>();
    let index_rows = (1..=6)
        .map(compiled_migration_legacy_row)
        .collect::<Vec<_>>();
    let capabilities = [
        active_capability(
            id,
            capability_id(0xa1),
            explicit_scope(&lineage, &[10, 11, 12]),
            20,
            0xb1,
        ),
        active_capability(
            id,
            capability_id(0xa2),
            explicit_scope(&lineage, &[20, 21]),
            20,
            0xb2,
        ),
    ];
    let commit = oracle_commit(CommitSequence::first(), oracle_plan_ref(0x31));
    let write = store
        .shared
        .database
        .begin_write()
        .expect("write oracle fixture");
    {
        let mut table = write.open_table(CONTRACT_BUNDLES).expect("bundles");
        for bundle in &bundles {
            let key = keys::encode_contract_bundle_key(bundle.lineage(), bundle.contract_version())
                .expect("bundle key");
            let encoded = codec::encode_contract_bundle_v1(bundle).expect("encode bundle");
            table
                .insert(key.as_slice(), encoded.as_bytes())
                .expect("insert bundle");
        }
    }
    {
        let mut table = write.open_table(CATALOG_ACTIVE).expect("active");
        let encoded = codec::encode_active_catalog_pointer_v1(&active).expect("encode active");
        table
            .insert(CATALOG_ACTIVE_KEY.as_slice(), encoded.as_bytes())
            .expect("insert active");
    }
    {
        let mut table = write.open_table(ENTITIES).expect("entities");
        for entity in &entities {
            let encoded = codec::encode_entity_record_v1(entity).expect("encode entity");
            table
                .insert(entity.target().key().as_bytes(), encoded.as_bytes())
                .expect("insert entity");
        }
    }
    {
        let mut table = write.open_table(SECONDARY_INDEXES).expect("indexes");
        for row in &index_rows {
            let encoded = riffdb_storage_api::encode_index_entry_v1_fixture(row)
                .expect("encode V1 index")
                .into_bytes();
            table
                .insert(row.key().as_bytes(), encoded.as_slice())
                .expect("insert index");
        }
    }
    {
        let mut table = write.open_table(CAPABILITIES).expect("capabilities");
        for capability in &capabilities {
            let key = keys::encode_capability_key(capability.capability_id());
            let encoded =
                codec::encode_capability_record_v1(capability).expect("encode capability");
            table
                .insert(key.as_slice(), encoded.as_bytes())
                .expect("insert capability");
        }
    }
    {
        let mut table = write.open_table(COMMITS).expect("commits");
        let key = keys::encode_application_sequence_key(commit.commit_sequence());
        let encoded = codec::encode_commit_record_v1(&commit).expect("encode commit");
        table
            .insert(key.as_slice(), encoded.as_bytes())
            .expect("insert commit");
    }
    write.commit().expect("commit oracle fixture");
}

#[test]
fn historical_plan_sequence_self_oracle_empty() {
    let path = TestDatabasePath::new("hist-oracle-empty");
    let mut store = RedbStore::open(&path.0).expect("open");
    store.initialize_database(database_id(0x81)).expect("init");
    drop(store);
    let sequence = collect_historical_plan_sequence(&path.0);
    assert_eq!(sequence.len(), 1, "empty DB emits only ActiveCatalog");
    assert!(matches!(
        sequence[0].1,
        HistoricalSemanticEvidence::ActiveCatalog(None)
    ));
}

#[test]
fn historical_plan_sequence_self_oracle_mixed() {
    let path = TestDatabasePath::new("hist-oracle-mixed");
    populate_mixed_oracle_fixture(&path, database_id(0x82));
    let sequence = collect_historical_plan_sequence(&path.0);
    let bundles = sequence
        .iter()
        .filter(|(_, e)| matches!(e, HistoricalSemanticEvidence::Bundle(_)))
        .count();
    let entities = sequence
        .iter()
        .filter(|(_, e)| {
            matches!(
                e,
                HistoricalSemanticEvidence::PersistedKey(key)
                    if matches!(
                        key.key(),
                        riffdb_storage_api::IrOpaquePersistedKeyV1::Entity { .. }
                    )
            )
        })
        .count();
    let indexes = sequence
        .iter()
        .filter(|(_, e)| matches!(e, HistoricalSemanticEvidence::IndexMigrationRow(_)))
        .count();
    let partitions = sequence
        .iter()
        .filter(|(_, e)| matches!(e, HistoricalSemanticEvidence::CapabilityPartition(_)))
        .count();
    let plans = sequence
        .iter()
        .filter(|(_, e)| matches!(e, HistoricalSemanticEvidence::PlanReference(_)))
        .count();
    let active = sequence
        .iter()
        .filter(|(_, e)| matches!(e, HistoricalSemanticEvidence::ActiveCatalog(Some(_))))
        .count();
    assert!(bundles >= 4, "bundles={bundles}");
    assert_eq!(entities, 260, "entities={entities}");
    assert!(indexes >= 6, "indexes={indexes}");
    assert!(partitions >= 5, "capability partitions={partitions}");
    assert!(plans >= 1, "plans={plans}");
    assert_eq!(active, 1, "active catalog");
    assert!(
        sequence.len() > 4 + 5 + 6 + 5 + 1,
        "mixed sequence length {}",
        sequence.len()
    );
    // Strict total order on order_keys.
    assert!(
        sequence.windows(2).all(|pair| pair[0].0 < pair[1].0),
        "order_keys must be strictly increasing"
    );
}

#[test]
fn historical_evidence_sequence_matches_the_golden_fixture_byte_for_byte() {
    let path = TestDatabasePath::new("hist-sequence-golden");
    populate_mixed_oracle_fixture(&path, database_id(0x8a));
    let sequence = collect_historical_plan_sequence(&path.0);
    let actual = historical_sequence_fixture(&sequence);
    let expected = include_str!("../historical-evidence-sequence-v1.fixture");
    assert_eq!(actual, expected, "historical evidence sequence changed");
}

fn historical_plan_retained_shape(entity_rows: u64) -> (usize, usize) {
    let path = TestDatabasePath::new("hist-plan-retained-shape");
    let store = initialized_store(&path, database_id(0x8b));
    let bundle = stored_bundle("plan-shape", 1, b"plan-shape-bundle");
    let binding = DurableKeySchemaBindingV1::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
    );
    let write = store
        .shared
        .database
        .begin_write()
        .expect("write plan-shape fixture");
    {
        let mut bundles = write.open_table(CONTRACT_BUNDLES).expect("bundles");
        let key = keys::encode_contract_bundle_key(bundle.lineage(), bundle.contract_version())
            .expect("bundle key");
        let encoded = codec::encode_contract_bundle_v1(&bundle).expect("encode bundle");
        bundles
            .insert(key.as_slice(), encoded.as_bytes())
            .expect("insert bundle");
    }
    {
        let mut entities = write.open_table(ENTITIES).expect("entities");
        for ordinal in 0..entity_rows {
            let entity_type =
                EntityTypeId::new(if ordinal % 2 == 0 { 7 } else { 9 }).expect("entity type");
            let mut key = EntityKeyBuilder::new(entity_type);
            key.push_u64(ordinal).expect("entity key component");
            let target = EntityTarget::new(entity_type, key.finish().expect("entity key"))
                .expect("entity target");
            let record = StoredEntityRecordV1::new(
                target,
                EntityVersion::first(),
                bundle.contract_version(),
                binding.clone(),
                CanonicalRecord::new(Vec::new()).expect("empty record"),
            )
            .expect("entity record");
            let encoded = codec::encode_entity_record_v1(&record).expect("encode entity");
            entities
                .insert(record.target().key().as_bytes(), encoded.as_bytes())
                .expect("insert entity");
        }
    }
    write.commit().expect("commit plan-shape fixture");
    let transaction = store.shared.database.begin_read().expect("read plan shape");
    let plan = build_historical_evidence_plan(&transaction, &inputs_at(70)).expect("plan");
    (plan.persisted_groups.len(), plan.retained_bytes)
}

#[test]
fn historical_plan_memory_does_not_scale_with_live_row_count() {
    let small = historical_plan_retained_shape(2);
    let large = historical_plan_retained_shape(50_000);
    assert_eq!(small.0, 2, "one group per entity type");
    assert_eq!(large.0, small.0, "row growth must not add locator groups");
    assert_eq!(
        large.1, small.1,
        "the retained historical index must depend on bounded key shapes, not live rows"
    );
    assert!(
        large.1 < 4 * 1024,
        "the 50,000-row fixture retained an unexpectedly large plan: {} bytes",
        large.1
    );
}

#[test]
fn historical_plan_sequence_self_oracle_commit_plans() {
    // Dedicated shape exercising collect_plan_locators via COMMITS rows.
    let path = TestDatabasePath::new("hist-oracle-plans");
    let id = database_id(0x83);
    let store = initialized_store(&path, id);
    let plans = [oracle_plan_ref(0x41), oracle_plan_ref(0x42)];
    let commits = [
        oracle_commit(CommitSequence::first(), plans[0].clone()),
        oracle_commit(CommitSequence::new(2).expect("seq"), plans[1].clone()),
    ];
    let write = store
        .shared
        .database
        .begin_write()
        .expect("write plan fixture");
    {
        let mut table = write.open_table(COMMITS).expect("commits");
        for commit in &commits {
            let key = keys::encode_application_sequence_key(commit.commit_sequence());
            let encoded = codec::encode_commit_record_v1(commit).expect("encode commit");
            table
                .insert(key.as_slice(), encoded.as_bytes())
                .expect("insert commit");
        }
    }
    write.commit().expect("commit plan fixture");
    drop(store);
    let sequence = collect_historical_plan_sequence(&path.0);
    let plan_count = sequence
        .iter()
        .filter(|(_, e)| matches!(e, HistoricalSemanticEvidence::PlanReference(_)))
        .count();
    assert_eq!(plan_count, 2, "two commit plan references");
}

#[test]
fn service_audit_without_request_index_reports_missing_cross_link() {
    let path = TestDatabasePath::new("missing-audit-request-index");
    let store = initialized_store(&path, database_id(0x84));
    let record = StoredServiceAuditRecordV1::from_stored_parts(
        AdministrationSequence::first(),
        request_id(0x71),
        Timestamp::new(1, 0).expect("timestamp"),
        ServiceOperationV1::GetHealth,
        ServiceAuditPhaseV1::Denied,
        Some(audit_principal(0x72)),
        ServiceIngressKindV1::Grpc,
        ServiceAuditTargetsV1::empty(),
        None,
        ServiceAuditLinkV1::None,
    )
    .expect("standalone service audit");
    let encoded = codec::encode_administration_audit_record_v1(
        &StoredAdministrationAuditRecordV1::Service(record),
    )
    .expect("encode service audit");
    let allocator =
        codec::encode_administration_sequence_allocator_v1(AdministrationSequenceAllocator::next(
            AdministrationSequence::new(2).expect("allocator sequence"),
        ))
        .expect("encode allocator");
    let write = store
        .shared
        .database
        .begin_write()
        .expect("write corrupt fixture");
    {
        let mut audit = write.open_table(AUDIT).expect("audit table");
        audit
            .insert(
                keys::encode_audit_key(AdministrationSequence::first()).as_slice(),
                encoded.as_bytes(),
            )
            .expect("insert service audit without index peer");
    }
    {
        let mut meta = write.open_table(META).expect("metadata table");
        meta.insert(META_ADMINISTRATION_SEQUENCE, allocator.as_bytes())
            .expect("advance allocator");
    }
    // Deliberately omit AUDIT_BY_REQUEST — reciprocity must fail closed.
    write.commit().expect("commit corrupt fixture");

    let mut session = store
        .begin_structural_evidence(inputs())
        .expect("begin evidence");
    let (_, findings) = collect_structural(&mut session);
    assert!(
        findings.iter().any(|finding| {
            finding.scope() == StructuralFindingScope::Authoritative
                && finding.code() == StructuralFindingCode::MissingCrossLink
        }),
        "expected MissingCrossLink for Service audit without AUDIT_BY_REQUEST peer; got {findings:?}"
    );
}

/// The write side now refuses a `DeployReactiveModule` linkless success. This
/// pass must NOT: a database written under the released allowance holds such
/// records, and refusing one here would refuse the daemon's whole startup.
/// Zero brick risk is the deliberate choice; the asymmetry is one-sided.
#[test]
fn a_durable_linkless_reactive_publication_success_still_opens_clean() {
    let path = TestDatabasePath::new("linkless-reactive-success");
    let store = initialized_store(&path, database_id(0x85));
    let request = request_id(0x73);
    let principal = audit_principal(0x74);
    let started = StoredServiceAuditRecordV1::from_stored_parts(
        AdministrationSequence::first(),
        request,
        Timestamp::new(1, 0).expect("timestamp"),
        ServiceOperationV1::DeployReactiveModule,
        ServiceAuditPhaseV1::Started,
        Some(principal.clone()),
        ServiceIngressKindV1::Grpc,
        ServiceAuditTargetsV1::empty(),
        None,
        ServiceAuditLinkV1::None,
    )
    .expect("started record");
    // The exact shape `ServiceAuditAppendIntentV1::new` now refuses. It must
    // still reconstruct, and this structural pass must still accept it.
    let terminal = StoredServiceAuditRecordV1::from_stored_parts(
        AdministrationSequence::new(2).expect("terminal sequence"),
        request,
        Timestamp::new(2, 0).expect("timestamp"),
        ServiceOperationV1::DeployReactiveModule,
        ServiceAuditPhaseV1::Succeeded,
        Some(principal),
        ServiceIngressKindV1::Grpc,
        ServiceAuditTargetsV1::empty(),
        None,
        ServiceAuditLinkV1::None,
    )
    .expect("a durable linkless success must reconstruct");
    let allocator =
        codec::encode_administration_sequence_allocator_v1(AdministrationSequenceAllocator::next(
            AdministrationSequence::new(3).expect("allocator sequence"),
        ))
        .expect("encode allocator");
    let write = store
        .shared
        .database
        .begin_write()
        .expect("write historical fixture");
    {
        let mut audit = write.open_table(AUDIT).expect("audit table");
        let mut index = write
            .open_table(AUDIT_BY_REQUEST)
            .expect("audit-by-request table");
        for record in [started, terminal] {
            let sequence = record.administration_sequence();
            let encoded = codec::encode_administration_audit_record_v1(
                &StoredAdministrationAuditRecordV1::Service(record),
            )
            .expect("encode service audit");
            audit
                .insert(
                    keys::encode_audit_key(sequence).as_slice(),
                    encoded.as_bytes(),
                )
                .expect("insert service audit");
            let index_value = codec::encode_service_audit_request_index_v1(
                riffdb_storage_api::StoredServiceAuditRequestIndexV1::new(request, sequence),
            )
            .expect("encode index");
            index
                .insert(
                    keys::encode_audit_by_request_key(request, sequence).as_slice(),
                    index_value.as_bytes(),
                )
                .expect("insert index row");
        }
    }
    {
        let mut meta = write.open_table(META).expect("metadata table");
        meta.insert(META_ADMINISTRATION_SEQUENCE, allocator.as_bytes())
            .expect("advance allocator");
    }
    write.commit().expect("commit historical fixture");

    let mut session = store
        .begin_structural_evidence(inputs())
        .expect("begin evidence");
    let (structural_end, findings) = collect_structural(&mut session);
    assert!(
        findings.is_empty(),
        "a historical linkless reactive-publication success must never be \
             refused at open; got {findings:?}"
    );
    let historical_end = finish_historical(&mut session);
    session
        .finish(structural_end, historical_end)
        .expect("a clean structural pass must release the ports");
}

fn reactive_fixture_module(
    contract: &StoredContractBundleV1,
    ordinal: u64,
) -> StoredReactiveModuleV1 {
    let source = format!("reactive witness source {ordinal}").into_bytes();
    let artifact = format!("reactive witness artifact {ordinal}").into_bytes();
    StoredReactiveModuleV1::new(
        "witness".to_owned(),
        ordinal.saturating_add(1),
        hash_reactive_module(&artifact),
        contract.lineage().clone(),
        contract.contract_version(),
        contract.bundle_hash(),
        hash_reactive_source(&source),
        Vec::new(),
        source,
        artifact,
    )
    .expect("stored reactive module")
}

/// Publishes `count` reactive modules through the real write path against a
/// freshly activated contract and returns the reopened store.
fn published_reactive_module_store(
    path: &TestDatabasePath,
    id: DatabaseId,
    count: u64,
) -> RedbStore {
    let store = deployed_migration_store(path, id);
    let dormant = RedbDormantPorts {
        shared: Arc::clone(&store.shared),
    };
    drop(store);
    let mut ports = dormant
        .into_operational_after_catalog_validation()
        .expect("activate reactive publication ports");
    let contract = validated_migration_bundle()
        .to_stored()
        .expect("stored migration bundle");
    for ordinal in 0..count {
        let module = reactive_fixture_module(&contract, ordinal);
        let request = u8::try_from(0x60 + ordinal).expect("fixture request seed");
        let intent = ReactiveModulePublicationIntentV1::new(
            module,
            request_id(request),
            audit_principal(0x61),
            Timestamp::new(2 + i64::try_from(ordinal).expect("fixture instant"), 0)
                .expect("publication timestamp"),
            None,
        );
        let published = ports
            .publish_reactive_module(&intent)
            .expect("publish the immutable module");
        assert!(
            matches!(
                published,
                riffdb_storage_api::ReactiveModulePublicationResult::Published { .. }
            ),
            "a first publication must publish, got {published:?}"
        );
    }
    drop(ports);
    RedbStore::open(&path.0).expect("reopen published reactive store")
}

fn audit_row_count(store: &RedbStore) -> u64 {
    let transaction = store
        .shared
        .database
        .begin_read()
        .expect("read the audit stream length");
    table_len(&transaction, AUDIT).expect("audit table length")
}

/// Installs one self-consistent reactive-module row that no publication
/// record names. The audit stream and the allocator stay untouched, so the
/// orphan is the only invariant under test.
fn install_orphan_reactive_module(store: &RedbStore) -> StoredReactiveModuleV1 {
    let contract = validated_migration_bundle()
        .to_stored()
        .expect("stored migration bundle");
    let orphan = reactive_fixture_module(&contract, 0x40);
    let encoded = codec::encode_reactive_module_v1(&orphan).expect("encode orphan module");
    let write = store
        .shared
        .database
        .begin_write()
        .expect("write orphan fixture");
    {
        let mut modules = write
            .open_table(REACTIVE_MODULES)
            .expect("reactive modules table");
        modules
            .insert(
                keys::encode_reactive_module_key(orphan.module_hash()).as_slice(),
                encoded.as_bytes(),
            )
            .expect("insert the orphaned reactive module row");
    }
    write.commit().expect("commit orphan fixture");
    orphan
}

/// The startup publication proof is single-pass: the whole REACTIVE_MODULES
/// phase decodes the audit stream ONCE, not once per retained row, and the
/// findings are unchanged (none, on a cleanly published database).
#[test]
fn reactive_publication_verification_decodes_the_audit_stream_once() {
    let path = TestDatabasePath::new("reactive-publication-witness");
    let store = published_reactive_module_store(&path, database_id(0x86), 3);
    let audit_rows = audit_row_count(&store);
    assert!(audit_rows >= 4, "three publications plus one activation");
    let mut session = store
        .begin_structural_evidence(inputs())
        .expect("begin evidence");
    let (structural_end, findings) = collect_structural(&mut session);
    assert!(
        findings.is_empty(),
        "three published reactive modules must validate clean; got {findings:?}"
    );
    assert_eq!(
        session.reactive_publication_audit_decodes(),
        audit_rows,
        "the publication witness must cost ONE audit pass for the phase, not \
             one full scan per retained reactive module"
    );
    let (historical_end, _, _) = collect_historical(&mut session, 8);
    session
        .finish(structural_end, historical_end)
        .expect("a clean structural pass must release the ports");
}

/// The single-pass proof still reports the orphan redb has always reported.
#[test]
fn a_reactive_module_without_a_publication_record_reports_missing_cross_link() {
    let path = TestDatabasePath::new("reactive-publication-orphan");
    let store = published_reactive_module_store(&path, database_id(0x87), 1);
    let orphan = install_orphan_reactive_module(&store);
    let audit_rows = audit_row_count(&store);
    let mut session = store
        .begin_structural_evidence(inputs())
        .expect("begin evidence");
    let (_, findings) = collect_structural(&mut session);
    assert_eq!(
        findings,
        vec![authoritative(StructuralFindingCode::MissingCrossLink)],
        "a retained reactive module with no publication record must be \
             reported exactly once, and the published row beside it must not be; \
             orphan {:?}",
        orphan.module_hash()
    );
    assert_eq!(
        session.reactive_publication_audit_decodes(),
        audit_rows,
        "two retained rows must still share one audit pass"
    );
}

/// The verification must stay UNCONDITIONAL under the ADR-0019
/// validated-prefix fast path, and its audit pass must not inherit that fast
/// path's audit-suffix range: reactive-module rows are an additive structural
/// count, so every retained row is judged at every open. This is the property
/// `read_reactive_module`'s ruling note depends on.
#[test]
fn the_publication_proof_survives_the_validated_prefix_fast_path() {
    let path = TestDatabasePath::new("reactive-publication-checkpointed");
    let store = published_reactive_module_store(&path, database_id(0x88), 1);
    let audit_rows = audit_row_count(&store);
    let mut session = store
        .begin_structural_evidence(inputs())
        .expect("begin evidence");
    let (structural_end, findings) = collect_structural(&mut session);
    assert!(
        findings.is_empty(),
        "the first open must validate clean so the checkpoint is written; got {findings:?}"
    );
    let (historical_end, _, _) = collect_historical(&mut session, 8);
    session
        .finish(structural_end, historical_end)
        .expect("a clean structural pass must release the ports");

    let store = RedbStore::open(&path.0).expect("reopen checkpointed store");
    install_orphan_reactive_module(&store);
    let mut session = store
        .begin_structural_evidence(inputs())
        .expect("begin evidence");
    assert!(
        session.checkpoint_verified(),
        "the validated-prefix fast path must be active or this proves nothing: {:?}",
        session.checkpoint_ignored_reason()
    );
    let (_, findings) = collect_structural(&mut session);
    assert_eq!(
        findings,
        vec![authoritative(StructuralFindingCode::MissingCrossLink)],
        "the fast path must not skip the reactive publication proof; got {findings:?}"
    );
    assert_eq!(
        session.reactive_publication_audit_decodes(),
        audit_rows,
        "the witness pass must read the whole audit stream, never the \
             checkpoint-truncated suffix the AUDIT phase walks"
    );
}

// ==== O(1) validated-prefix checkpoint counts (WP-448) ====
//
// Falsifiability notes (what a neutered implementation would break):
// - `checkpoint_counts_from_durable_lengths_are_byte_identical_to_the_walk`:
//   dropping the ExecutionFailed subtraction, or the S == 0 / bound == 0
//   guards, turns the encoded bytes unequal on the shapes that exercise them.
// - `the_shutdown_checkpoint_write_iterates_no_history_rows`: re-pointing the
//   production write at `CheckpointCountSource::Walked` turns the row tally
//   nonzero.
// - `a_drifted_terminal_census_is_refused_at_the_next_open`: it is the whole
//   fail-closed claim — a wrong maintained count can never be silently
//   trusted.

/// One deterministic step of the house LCG used for randomized shapes.
fn checkpoint_shape_mix(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *state >> 17
}

/// Row population of one generated checkpoint-count shape. Every field stays
/// inside the envelope the write lane can actually produce: no counted table
/// ever holds a row whose sequence exceeds the last `COMMITS`/`AUDIT` key,
/// because every such row is born in the transaction that writes that key.
#[derive(Clone, Copy, Debug)]
struct CheckpointCountShape {
    commits: u64,
    /// First commit sequence; above 1 models a retention-pruned prefix.
    first_commit: u64,
    events: u64,
    routes: u64,
    outbox: u64,
    outbox_status: u64,
    outcomes: u64,
    failures: u64,
    audits: u64,
    by_request: u64,
    entities: u64,
}

fn checkpoint_count_plan(bundle: &StoredContractBundleV1) -> ExecutablePlanRef {
    ExecutablePlanRef::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
        CommandId::first(),
        PlanHash::from_bytes([0x4c; 32]),
    )
}

fn checkpoint_count_identity(
    bundle: &StoredContractBundleV1,
    id: DatabaseId,
    ordinal: u64,
) -> IdempotencyIdentity {
    let mut digest = [0x31_u8; 32];
    digest[..8].copy_from_slice(&ordinal.to_be_bytes());
    IdempotencyIdentity::new(
        id,
        Environment::new("checkpoint-counts").expect("environment"),
        TenantScope::Global,
        ActorId::new("checkpoint-actor").expect("actor"),
        bundle.lineage().clone(),
        CommandId::first(),
        IdempotencyKeyDigest::from_hmac_bytes(DigestKeyId::new(1).expect("digest key"), digest),
    )
}

fn checkpoint_count_actor() -> AdmittedActorContext {
    AdmittedActorContext::new(
        ActorId::new("checkpoint-actor").expect("actor"),
        ActorKind::Human,
        TenantScope::Global,
        None,
    )
}

fn checkpoint_count_partition() -> riffdb_types::PartitionKey {
    let mut partition = PartitionKeyBuilder::new(AggregateTypeId::first());
    partition.push_u64(1).expect("partition component");
    partition.finish().expect("partition key")
}

/// One terminal stored outcome at `sequence`, keyed by `ordinal`.
fn checkpoint_count_outcome(
    bundle: &StoredContractBundleV1,
    id: DatabaseId,
    ordinal: u64,
    sequence: CommitSequence,
) -> StoredOutcomeV1 {
    let partition_key = checkpoint_count_partition();
    let partition_hash = hash_partition_key(partition_key.as_bytes());
    StoredOutcomeV1::new(
        checkpoint_count_identity(bundle, id, ordinal),
        sequence,
        request_id(0x4d),
        checkpoint_count_plan(bundle),
        CanonicalInputHash::from_bytes([0x4e; 32]),
        checkpoint_count_actor(),
        LogicalTime::new(Timestamp::new(1, 0).expect("timestamp")),
        partition_key,
        partition_hash,
        Vec::new(),
        DeclaredOutcome::new(
            OutcomeId::first(),
            CanonicalRecord::new(Vec::new()).expect("outcome fields"),
        )
        .expect("declared outcome"),
        StoredAdmittedProvenanceClaimsV1::default(),
        ProvenanceId::from_bytes(uuid_bytes(0x4f)).expect("provenance ID"),
        DurabilityMode::Sync,
    )
    .expect("stored outcome")
}

/// One terminal execution failure, keyed by `ordinal`. It carries no commit
/// sequence, which is exactly why the checkpoint count cannot be a row count.
fn checkpoint_count_failure(
    bundle: &StoredContractBundleV1,
    id: DatabaseId,
    ordinal: u64,
) -> riffdb_storage_api::StoredExecutionFailedV1 {
    let pending = riffdb_storage_api::StoredPendingAdmissionV1::new(
        checkpoint_count_identity(bundle, id, ordinal),
        CanonicalInputHash::from_bytes([0x5a; 32]),
        request_id(0x5b),
        checkpoint_count_plan(bundle),
        LogicalTime::new(Timestamp::new(1, 0).expect("timestamp")),
        checkpoint_count_actor(),
        checkpoint_count_partition(),
        StoredAdmittedProvenanceClaimsV1::default(),
    )
    .expect("pending admission");
    riffdb_storage_api::StoredExecutionFailedV1::new(
        pending,
        riffdb_types::ExecutionFailureCode::UniqueConflict,
    )
}

/// Writes one generated shape as raw rows. Row VALUES matter only where a
/// count classifies them (IDEMPOTENCY) or the fingerprint decodes them
/// (ENTITIES); the other counted tables are classified from their keys, so
/// realistic filler keeps the fixture cheap without weakening the property.
fn write_checkpoint_count_shape(
    store: &RedbStore,
    id: DatabaseId,
    bundle: &StoredContractBundleV1,
    shape: &CheckpointCountShape,
) {
    let partition_hash = hash_partition_key(checkpoint_count_partition().as_bytes());
    let last_commit = shape
        .first_commit
        .saturating_add(shape.commits.saturating_sub(1));
    // Event-keyed rows must reference a sequence at or below the last commit,
    // which is what the write lane guarantees by writing them in the same
    // transaction as that commit.
    let event_sequence = |ordinal: u64| {
        let span = shape.commits.max(1);
        CommitSequence::new(shape.first_commit.saturating_add(ordinal % span))
            .expect("event commit sequence")
    };
    let write = store.shared.database.begin_write().expect("begin fixture");
    {
        let mut commits = write.open_table(COMMITS).expect("commits");
        for ordinal in 0..shape.commits {
            let sequence = CommitSequence::new(shape.first_commit.saturating_add(ordinal))
                .expect("commit sequence");
            let commit = history_commit(sequence, checkpoint_count_plan(bundle), &[]);
            let encoded = codec::encode_commit_record_v1(&commit).expect("encode commit");
            commits
                .insert(
                    keys::encode_application_sequence_key(sequence).as_slice(),
                    encoded.as_bytes(),
                )
                .expect("insert commit");
        }
        let mut events = write.open_table(EVENTS).expect("events");
        for ordinal in 0..shape.events {
            let event = riffdb_types::EventId::new(
                event_sequence(ordinal),
                u32::try_from(ordinal % 4).expect("event ordinal"),
            );
            events
                .insert(
                    keys::encode_event_key(event).as_slice(),
                    [0x22_u8; 32].as_slice(),
                )
                .expect("insert event");
        }
        let mut routes = write.open_table(EVENT_ROUTES).expect("routes");
        for ordinal in 0..shape.routes {
            let event = riffdb_types::EventId::new(
                event_sequence(ordinal),
                u32::try_from(ordinal % 4).expect("event ordinal"),
            );
            routes
                .insert(
                    keys::encode_event_route_key(partition_hash, event).as_slice(),
                    [0x33_u8; 16].as_slice(),
                )
                .expect("insert route");
        }
        let mut outbox = write.open_table(OUTBOX).expect("outbox");
        for ordinal in 0..shape.outbox {
            let event = riffdb_types::EventId::new(
                event_sequence(ordinal),
                u32::try_from(ordinal % 4).expect("event ordinal"),
            );
            outbox
                .insert(
                    keys::encode_event_key(event).as_slice(),
                    [0x44_u8; 24].as_slice(),
                )
                .expect("insert outbox");
        }
        let mut status = write.open_table(OUTBOX_STATUS).expect("status");
        for ordinal in 0..shape.outbox_status {
            let event = riffdb_types::EventId::new(
                event_sequence(ordinal),
                u32::try_from(ordinal % 4).expect("event ordinal"),
            );
            status
                .insert(
                    keys::encode_event_key(event).as_slice(),
                    [0x55_u8; 8].as_slice(),
                )
                .expect("insert status");
        }
        let mut terminal = write.open_table(IDEMPOTENCY).expect("idempotency");
        for ordinal in 0..shape.outcomes {
            let outcome = checkpoint_count_outcome(bundle, id, ordinal, event_sequence(ordinal));
            let encoded = codec::encode_stored_outcome_v1(&outcome).expect("encode outcome");
            let key = outcome.identity().storage_key().expect("identity key");
            terminal
                .insert(key.as_bytes(), encoded.as_bytes())
                .expect("insert outcome");
        }
        for ordinal in 0..shape.failures {
            let failure = checkpoint_count_failure(
                bundle,
                id,
                shape.outcomes.saturating_add(ordinal).saturating_add(1),
            );
            let encoded =
                codec::encode_execution_failed_v1(&failure).expect("encode execution failure");
            let key = failure
                .pending()
                .identity()
                .storage_key()
                .expect("identity key");
            terminal
                .insert(key.as_bytes(), encoded.as_bytes())
                .expect("insert execution failure");
        }
        let mut audit = write.open_table(AUDIT).expect("audit");
        for ordinal in 0..shape.audits {
            let sequence = AdministrationSequence::new(ordinal.saturating_add(1))
                .expect("administration sequence");
            audit
                .insert(
                    keys::encode_audit_key(sequence).as_slice(),
                    [0x66_u8; 40].as_slice(),
                )
                .expect("insert audit");
        }
        let mut by_request = write.open_table(AUDIT_BY_REQUEST).expect("by request");
        for ordinal in 0..shape.by_request {
            let sequence =
                AdministrationSequence::new((ordinal % shape.audits.max(1)).saturating_add(1))
                    .expect("administration sequence");
            by_request
                .insert(
                    keys::encode_audit_by_request_key(
                        request_id(u8::try_from(ordinal % 251).expect("request seed")),
                        sequence,
                    )
                    .as_slice(),
                    [0x77_u8; 4].as_slice(),
                )
                .expect("insert audit index");
        }
        let mut entities = write.open_table(ENTITIES).expect("entities");
        let mut heads = write
            .open_table(crate::layout::ENTITY_CHAIN_HEADS)
            .expect("entity chain heads");
        for ordinal in 0..shape.entities {
            let record = history_entity(ordinal, EntityVersion::first(), b"counts", bundle);
            let encoded = codec::encode_entity_record_v1(&record).expect("encode entity");
            entities
                .insert(
                    keys::encode_entity_key(record.target().key()),
                    encoded.as_bytes(),
                )
                .expect("insert entity");
            let head = test_genesis_entity_head(
                &record,
                CommitSequence::new(last_commit.max(1)).expect("head sequence"),
                usize::try_from(ordinal).expect("head ordinal"),
            );
            let encoded_head = riffdb_storage_api::encode_entity_chain_head_v1(&head)
                .expect("encode entity chain head");
            heads
                .insert(record.target().key().as_bytes(), encoded_head.as_bytes())
                .expect("insert entity chain head");
        }
        let _ = last_commit;
    }
    write.commit().expect("commit fixture");
}

/// Encodes the checkpoint both count sources produce over one snapshot and
/// returns `(walked bytes, derived bytes, rows the derived path iterated)`.
fn encoded_checkpoints_from_both_count_sources(
    shared: &SharedRedb,
    execution_failed_rows: u64,
) -> (Vec<u8>, Vec<u8>, u64) {
    use crate::validated_prefix::{CheckpointCountSource, build_checkpoint_from_snapshot};

    let transaction = shared.database.begin_read().expect("checkpoint snapshot");
    let retained = read_retained_metadata_pub(&transaction).expect("retained metadata");
    let mut walked_rows = 0_u64;
    let walked = build_checkpoint_from_snapshot(
        &transaction,
        &retained,
        CheckpointCountSource::Walked,
        &mut walked_rows,
    )
    .expect("reference checkpoint");
    let mut derived_rows = 0_u64;
    let derived = build_checkpoint_from_snapshot(
        &transaction,
        &retained,
        CheckpointCountSource::DurableLengths {
            execution_failed_rows,
        },
        &mut derived_rows,
    )
    .expect("derived checkpoint");
    let encode = |checkpoint: &_| {
        riffdb_storage_api::proto_codec::encode_validated_prefix_checkpoint_v2(checkpoint)
            .expect("encode checkpoint")
            .as_bytes()
            .to_vec()
    };
    (encode(&walked), encode(&derived), derived_rows)
}

/// The O(1) count source must produce the SAME checkpoint bytes as the
/// reference walk on every history shape the write lane can reach — that
/// equality is the whole licence for not walking at shutdown.
#[test]
fn checkpoint_counts_from_durable_lengths_are_byte_identical_to_the_walk() {
    let mut state = 0x5EED_C0FF_EE01_u64;
    let mut saw_failures = false;
    let mut saw_empty_history = false;
    let mut saw_pruned_prefix = false;
    let mut saw_full_population = false;
    for trial in 0..24_u64 {
        // Trial 0 is the empty database; trial 1 pins the S == 0 guard with a
        // non-empty event population; the rest are randomized.
        let shape = match trial {
            0 => CheckpointCountShape {
                commits: 0,
                first_commit: 1,
                events: 0,
                routes: 0,
                outbox: 0,
                outbox_status: 0,
                outcomes: 0,
                failures: 0,
                audits: 0,
                by_request: 0,
                entities: 0,
            },
            1 => CheckpointCountShape {
                commits: 0,
                first_commit: 1,
                events: 3,
                routes: 3,
                outbox: 2,
                outbox_status: 1,
                outcomes: 0,
                failures: 0,
                audits: 0,
                by_request: 2,
                entities: 1,
            },
            _ => {
                let commits = 1 + checkpoint_shape_mix(&mut state) % 12;
                CheckpointCountShape {
                    commits,
                    first_commit: 1 + checkpoint_shape_mix(&mut state) % 5,
                    events: checkpoint_shape_mix(&mut state) % 17,
                    routes: checkpoint_shape_mix(&mut state) % 13,
                    outbox: checkpoint_shape_mix(&mut state) % 11,
                    outbox_status: checkpoint_shape_mix(&mut state) % 7,
                    outcomes: checkpoint_shape_mix(&mut state) % 9,
                    failures: checkpoint_shape_mix(&mut state) % 4,
                    audits: checkpoint_shape_mix(&mut state) % 15,
                    by_request: checkpoint_shape_mix(&mut state) % 15,
                    entities: checkpoint_shape_mix(&mut state) % 6,
                }
            }
        };
        saw_failures |= shape.failures > 0;
        saw_empty_history |= shape.commits == 0;
        saw_pruned_prefix |= shape.first_commit > 1;
        saw_full_population |= shape.commits > 0
            && shape.events > 0
            && shape.routes > 0
            && shape.outbox > 0
            && shape.outbox_status > 0
            && shape.outcomes > 0
            && shape.audits > 0
            && shape.by_request > 0;

        let path = TestDatabasePath::new("checkpoint-count-shape");
        let id = database_id(0x9a);
        let store = initialized_store(&path, id);
        let bundle = stored_bundle("checkpoint-counts", 1, b"checkpoint-counts-bundle");
        write_checkpoint_count_shape(&store, id, &bundle, &shape);
        let (walked, derived, derived_rows) =
            encoded_checkpoints_from_both_count_sources(&store.shared, shape.failures);
        assert_eq!(
            derived, walked,
            "trial {trial}: the O(1) count source must encode byte-identical \
                 checkpoint bytes to the reference walk; shape={shape:?}"
        );
        assert_eq!(
            derived_rows, 0,
            "trial {trial}: the O(1) count source must not iterate history; \
                 shape={shape:?}"
        );
    }
    assert!(
        saw_failures && saw_empty_history && saw_pruned_prefix && saw_full_population,
        "the generated shapes must cover execution failures, empty history, a \
             pruned prefix, and one fully populated database or the property is \
             vacuous: failures={saw_failures} empty={saw_empty_history} \
             pruned={saw_pruned_prefix} full={saw_full_population}"
    );
}

/// The O(1) derivation's precondition — no counted row carries a sequence
/// above S — is CHECKED for the sequence-ordered event tables, not assumed.
/// A row above S must send the build to the reference walk, whose answer is
/// always correct, rather than let it trust a row count that includes a row
/// the recorded count must exclude.
///
/// The write lane cannot produce this shape (every counted row is born in the
/// transaction that writes its `COMMITS` row), which is exactly why the probe
/// exists and why the row is constructed here directly.
#[test]
fn an_event_keyed_row_above_s_falls_back_to_the_reference_walk() {
    let path = TestDatabasePath::new("checkpoint-row-above-s");
    let id = database_id(0x9f);
    let store = initialized_store(&path, id);
    let bundle = stored_bundle("checkpoint-counts", 1, b"checkpoint-counts-bundle");
    let shape = CheckpointCountShape {
        commits: 4,
        first_commit: 1,
        events: 4,
        routes: 2,
        outbox: 3,
        outbox_status: 2,
        outcomes: 3,
        failures: 1,
        audits: 3,
        by_request: 3,
        entities: 2,
    };
    write_checkpoint_count_shape(&store, id, &bundle, &shape);

    // Control: with every row at or below S the build touches no row at all.
    let (walked, derived, derived_rows) =
        encoded_checkpoints_from_both_count_sources(&store.shared, shape.failures);
    assert_eq!(
        derived, walked,
        "control: an intact shape must derive the reference walk's bytes"
    );
    assert_eq!(
        derived_rows, 0,
        "control: an intact shape must be derived without iterating a row"
    );

    // One EVENTS row for a commit sequence above the last COMMITS key.
    let above = CommitSequence::new(shape.commits.saturating_add(1)).expect("sequence above S");
    let write = store
        .shared
        .database
        .begin_write()
        .expect("begin row-above-S write");
    {
        let mut events = write.open_table(EVENTS).expect("events");
        events
            .insert(
                keys::encode_event_key(riffdb_types::EventId::new(above, 0)).as_slice(),
                [0x22_u8; 32].as_slice(),
            )
            .expect("insert event above S");
    }
    write.commit().expect("commit row above S");

    let (walked, derived, derived_rows) =
        encoded_checkpoints_from_both_count_sources(&store.shared, shape.failures);
    assert!(
        derived_rows > 0,
        "a counted row above S must send the build to the reference walk; \
             trusting the row count here would record an events_count that \
             includes a row the count must exclude"
    );
    assert_eq!(
        derived, walked,
        "the fallback must produce exactly the reference walk's checkpoint"
    );
}

/// A clean-validating database of `commits` single-entity commits.
fn checkpointable_history_store(
    path: &TestDatabasePath,
    id: DatabaseId,
    commits: u64,
) -> (RedbStore, StoredContractBundleV1) {
    let store = initialized_store(path, id);
    let bundle = stored_bundle("checkpoint-history", 1, b"checkpoint-history-bundle");
    let plan = ExecutablePlanRef::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
        CommandId::first(),
        PlanHash::from_bytes([0x4b; 32]),
    );
    let entities = (0..commits)
        .map(|ordinal| {
            history_entity(
                ordinal.saturating_add(1),
                EntityVersion::first(),
                b"history",
                &bundle,
            )
        })
        .collect::<Vec<_>>();
    let records = entities
        .iter()
        .enumerate()
        .map(|(ordinal, entity)| {
            history_commit(
                CommitSequence::new(u64::try_from(ordinal).expect("ordinal") + 1)
                    .expect("commit sequence"),
                plan.clone(),
                &[entity],
            )
        })
        .collect::<Vec<_>>();
    write_entities_and_commits(&store, id, &bundle, &entities, &records);
    (store, bundle)
}

/// Drains historical evidence without asserting the catalog is inactive
/// (`finish_historical` is for empty fixtures; these fixtures activate one).
fn drain_historical(session: &mut RedbStructuralEvidenceSession) -> RedbHistoricalEvidenceEnd {
    let (end, _, _) = collect_historical(session, 8);
    end
}

/// Completes one clean open and returns the activated ports.
fn open_cleanly(store: RedbStore) -> crate::RedbOperationalPorts {
    let mut session = store
        .begin_structural_evidence(inputs())
        .expect("begin structural evidence");
    let structural_end = finish_structural(&mut session);
    let historical_end = drain_historical(&mut session);
    let outcome = session
        .finish(structural_end, historical_end)
        .expect("a clean structural pass must release the ports");
    let StructuralOpenOutcome::Clean(opened) = outcome else {
        panic!("an intact history must open clean");
    };
    let (_, _, _, dormant) = opened.into_parts();
    dormant
        .into_operational_after_catalog_validation()
        .expect("activate ports")
}

/// A validated-prefix checkpoint is an allocator anchor, not permission to
/// trust the live tail. The suffix must begin at the snapshotted next
/// administration sequence, remain gap-free, and end at the live allocator.
#[test]
fn checkpointed_administration_allocator_proves_only_the_suffix() {
    let path = TestDatabasePath::new("checkpoint-administration-suffix");
    let store = initialized_store(&path, database_id(0xa4));
    let third = AdministrationSequence::new(3).expect("third sequence");
    let fourth = AdministrationSequence::new(4).expect("fourth sequence");
    let allocator = AdministrationSequenceAllocator::next(fourth);
    let encoded_allocator =
        codec::encode_administration_sequence_allocator_v1(allocator).expect("allocator");
    let write = store.shared.database.begin_write().expect("write suffix");
    {
        let mut audit = write.open_table(AUDIT).expect("audit");
        audit
            .insert(
                keys::encode_audit_key(AdministrationSequence::first()).as_slice(),
                [0x11_u8].as_slice(),
            )
            .expect("prefix row");
        audit
            .insert(
                keys::encode_audit_key(third).as_slice(),
                [0x22_u8].as_slice(),
            )
            .expect("suffix row");
    }
    write
        .open_table(META)
        .expect("meta")
        .insert(META_ADMINISTRATION_SEQUENCE, encoded_allocator.as_bytes())
        .expect("advance allocator");
    write.commit().expect("commit suffix");

    let checkpoint = crate::validated_prefix::ActiveCheckpoint {
        checkpoint_commit_sequence: 0,
        audit_sequence_bound: 1,
        retained: riffdb_storage_api::ValidatedPrefixRetainedSnapshot {
            next_application_sequence: 1,
            application_sequence_exhausted: false,
            // Sequence 2 is a command audit embedded in the validated
            // COMMITS prefix, not a physical AUDIT row. This is the normal
            // segmented-command shape.
            next_administration_sequence: 3,
            administration_sequence_exhausted: false,
        },
        counts: riffdb_storage_api::ValidatedPrefixSequenceCounts {
            commits_count: 0,
            events_count: 0,
            event_routes_count: 0,
            outbox_count: 0,
            outbox_status_count: 0,
            idempotency_count: 0,
            audit_count: 1,
            audit_by_request_count: 0,
        },
        entity_heads_at_s: None,
        checkpoint_hash: [0; 32],
    };
    let covered_sequence = AdministrationSequence::first();
    let covered_record = StoredServiceAuditRecordV1::from_stored_parts(
        covered_sequence,
        request_id(0xa4),
        Timestamp::new(1, 0).expect("timestamp"),
        ServiceOperationV1::ExecuteCommand,
        ServiceAuditPhaseV1::Started,
        Some(audit_principal(0xa4)),
        ServiceIngressKindV1::Grpc,
        ServiceAuditTargetsV1::empty(),
        None,
        ServiceAuditLinkV1::None,
    )
    .expect("checkpoint-covered command audit");
    let derived = std::collections::BTreeMap::from([(
        covered_sequence,
        CachedCommandAudit {
            commit_sequence: CommitSequence::first(),
            member: riffdb_storage_api::StoredCommandAuditMemberV1::Started,
            record: covered_record,
            peer_sequence: covered_sequence,
        },
    )]);
    let read = store
        .shared
        .database
        .begin_read()
        .expect("read intact suffix");
    assert!(
        administration_allocator_matches(&read, allocator, Some(&checkpoint), &derived,)
            .expect("check intact suffix"),
        "a checkpoint-covered member must not be recounted before the exact suffix"
    );
    drop(read);

    let write = store.shared.database.begin_write().expect("remove suffix");
    write
        .open_table(AUDIT)
        .expect("audit")
        .remove(keys::encode_audit_key(third).as_slice())
        .expect("remove suffix row");
    write.commit().expect("commit suffix gap");
    let read = store
        .shared
        .database
        .begin_read()
        .expect("read gapped suffix");
    assert!(
        !administration_allocator_matches(&read, allocator, Some(&checkpoint), &derived,)
            .expect("check gapped suffix"),
        "a live allocator ahead of a missing suffix row must fail closed"
    );
}

/// The graceful-shutdown checkpoint write must read row COUNTS, not rows: no
/// history row may be iterated to build it, at any history length.
#[test]
fn the_shutdown_checkpoint_write_iterates_no_history_rows() {
    let path = TestDatabasePath::new("checkpoint-zero-walk");
    let id = database_id(0x9b);
    let (store, _bundle) = checkpointable_history_store(&path, id, 12);
    {
        let transaction = store.shared.database.begin_read().expect("read");
        let rows = table_len(&transaction, COMMITS).expect("commit rows");
        assert_eq!(
            rows, 12,
            "the pin proves nothing unless the database really holds history"
        );
    }
    let ports = open_cleanly(store);
    // Startup's own post-validation write comes first and is the same build.
    assert_eq!(
        ports.checkpoint_count_rows_walked(),
        0,
        "the post-validation checkpoint write must not walk history either"
    );
    let checkpoint_before = {
        let read = ports.shared.database.begin_read().expect("checkpoint read");
        let meta = read.open_table(META).expect("metadata table");
        meta.get(META_VALIDATED_PREFIX_CHECKPOINT)
            .expect("checkpoint lookup")
            .expect("startup checkpoint")
            .value()
            .to_vec()
    };
    assert!(
        ports
            .write_validated_prefix_checkpoint()
            .expect("graceful-shutdown checkpoint write"),
        "a clean validation must permit the shutdown checkpoint write"
    );
    let checkpoint_after = {
        let read = ports.shared.database.begin_read().expect("checkpoint read");
        let meta = read.open_table(META).expect("metadata table");
        meta.get(META_VALIDATED_PREFIX_CHECKPOINT)
            .expect("checkpoint lookup")
            .expect("retained checkpoint")
            .value()
            .to_vec()
    };
    assert_eq!(
        checkpoint_after, checkpoint_before,
        "an exact-current proof must be retained byte-for-byte instead of chained again"
    );
    assert_eq!(
        ports.checkpoint_count_rows_walked(),
        0,
        "the graceful-shutdown checkpoint write must iterate no history row"
    );
    assert_eq!(
        ports.terminal_execution_failure_rows(),
        0,
        "a history with no execution failures must census none"
    );
    drop(ports);

    // The counts written without walking must be the true ones: a reopen
    // verifies every recorded count against redb's own row counts and refuses
    // on any divergence, so an accepted fast path IS the count proof.
    let reopened = RedbStore::open(&path.0).expect("reopen checkpointed store");
    let mut session = reopened
        .begin_structural_evidence(inputs())
        .expect("begin structural evidence");
    assert!(
        session.checkpoint_verified(),
        "the checkpoint written without walking must be accepted: {:?}",
        session.checkpoint_ignored_reason()
    );
    let structural_end = finish_structural(&mut session);
    let historical_end = drain_historical(&mut session);
    session
        .finish(structural_end, historical_end)
        .expect("the metadata-derived counts must survive verification");
}

#[test]
fn startup_republishes_once_and_shutdown_reuses_that_process_proof() {
    let path = TestDatabasePath::new("checkpoint-process-proof");
    let id = database_id(0x99);
    let (store, _bundle) = checkpointable_history_store(&path, id, 4);
    let first_ports = open_cleanly(store);
    let first = {
        let read = first_ports
            .shared
            .database
            .begin_read()
            .expect("first checkpoint read");
        let meta = read.open_table(META).expect("metadata table");
        meta.get(META_VALIDATED_PREFIX_CHECKPOINT)
            .expect("checkpoint lookup")
            .expect("first startup checkpoint")
            .value()
            .to_vec()
    };
    drop(first_ports);

    let second_store = RedbStore::open(&path.0).expect("reopen checkpointed store");
    let second_ports = open_cleanly(second_store);
    let second = {
        let read = second_ports
            .shared
            .database
            .begin_read()
            .expect("second checkpoint read");
        let meta = read.open_table(META).expect("metadata table");
        meta.get(META_VALIDATED_PREFIX_CHECKPOINT)
            .expect("checkpoint lookup")
            .expect("second startup checkpoint")
            .value()
            .to_vec()
    };
    assert_ne!(
        second, first,
        "startup validation must publish its one process-generation proof"
    );
    assert!(
        second_ports
            .write_validated_prefix_checkpoint()
            .expect("graceful shutdown checkpoint")
    );
    let shutdown = {
        let read = second_ports
            .shared
            .database
            .begin_read()
            .expect("shutdown checkpoint read");
        let meta = read.open_table(META).expect("metadata table");
        meta.get(META_VALIDATED_PREFIX_CHECKPOINT)
            .expect("checkpoint lookup")
            .expect("shutdown checkpoint")
            .value()
            .to_vec()
    };
    assert_eq!(
        shutdown, second,
        "graceful shutdown must retain startup's exact process-generation proof"
    );
}

#[test]
fn an_uncertain_current_checkpoint_takes_the_full_repair_path() {
    let path = TestDatabasePath::new("checkpoint-current-corrupt");
    let id = database_id(0x9a);
    let (store, _bundle) = checkpointable_history_store(&path, id, 4);
    let ports = open_cleanly(store);

    let write = ports
        .shared
        .database
        .begin_write()
        .expect("damage checkpoint");
    write
        .open_table(META)
        .expect("metadata table")
        .insert(
            META_VALIDATED_PREFIX_CHECKPOINT,
            b"not-a-checkpoint".as_slice(),
        )
        .expect("replace checkpoint");
    write.commit().expect("commit checkpoint damage");

    assert!(
        ports
            .write_validated_prefix_checkpoint()
            .expect("repair uncertain checkpoint"),
        "a clean process generation must repair rather than trust uncertain proof bytes"
    );
    drop(ports);

    let reopened = RedbStore::open(&path.0).expect("reopen repaired checkpoint");
    let mut session = reopened
        .begin_structural_evidence(inputs())
        .expect("begin repaired validation");
    assert!(
        session.checkpoint_verified(),
        "the replacement must be a complete validated-prefix proof: {:?}",
        session.checkpoint_ignored_reason()
    );
    let structural_end = finish_structural(&mut session);
    let historical_end = drain_historical(&mut session);
    session
        .finish(structural_end, historical_end)
        .expect("repaired proof must survive full validation");
}

#[test]
fn missing_checkpoint_head_snapshot_row_falls_back_and_repairs() {
    let path = TestDatabasePath::new("checkpoint-head-snapshot-corrupt");
    let id = database_id(0x9f);
    let (store, _bundle) = checkpointable_history_store(&path, id, 4);
    let ports = open_cleanly(store);
    assert!(
        ports
            .write_validated_prefix_checkpoint()
            .expect("write checkpoint")
    );
    drop(ports);

    let store = RedbStore::open(&path.0).expect("reopen before corruption");
    let key = {
        let read = store.shared.database.begin_read().expect("read snapshot");
        let table = read
            .open_table(crate::layout::VALIDATED_PREFIX_ENTITY_HEADS)
            .expect("checkpoint heads");
        let (key, _) = table.first().expect("first snapshot row").expect("row");
        key.value().to_vec()
    };
    let write = store
        .shared
        .database
        .begin_write()
        .expect("damage snapshot");
    write
        .open_table(crate::layout::VALIDATED_PREFIX_ENTITY_HEADS)
        .expect("checkpoint heads")
        .remove(key.as_slice())
        .expect("remove snapshot row");
    write.commit().expect("commit damage");

    let mut session = store
        .begin_structural_evidence(inputs())
        .expect("begin fallback validation");
    assert!(!session.checkpoint_verified());
    assert_eq!(
        session.checkpoint_ignored_reason(),
        Some("entity_chain_mismatch")
    );
    let structural_end = finish_structural(&mut session);
    let historical_end = drain_historical(&mut session);
    session
        .finish(structural_end, historical_end)
        .expect("authoritative state remains intact");

    let repaired = RedbStore::open(&path.0).expect("reopen repaired checkpoint");
    let session = repaired
        .begin_structural_evidence(inputs())
        .expect("begin repaired validation");
    assert!(
        session.checkpoint_verified(),
        "a full clean fallback must atomically repair the snapshot: {:?}",
        session.checkpoint_ignored_reason()
    );
}

/// The census the O(1) count source subtracts must advance with the lane that
/// makes a terminal `ExecutionFailed` row durable. After one such commit the
/// derived counts must still be byte-identical to the reference walk, which
/// classifies that row for itself — so a lane that stopped counting turns this
/// red rather than shipping a checkpoint the next open would refuse.
#[test]
fn a_committed_terminal_execution_failure_advances_the_census_it_is_counted_by() {
    let path = TestDatabasePath::new("checkpoint-census-maintenance");
    let id = database_id(0x9e);
    let (store, bundle) = checkpointable_history_store(&path, id, 4);
    let ports = open_cleanly(store);
    assert_eq!(
        ports.terminal_execution_failure_rows(),
        0,
        "the seed for a history without execution failures is zero"
    );

    // The execution-failure lane's own shape: stage the terminal row, then
    // commit through the one path that counts it.
    let failure = checkpoint_count_failure(&bundle, id, 0x7000);
    let encoded = codec::encode_execution_failed_v1(&failure).expect("encode execution failure");
    let key = failure
        .pending()
        .identity()
        .storage_key()
        .expect("identity key");
    let access = ports.begin_write().expect("begin write");
    {
        let transaction = access.transaction().expect("staged transaction");
        let mut terminal = transaction.open_table(IDEMPOTENCY).expect("idempotency");
        terminal
            .insert(key.as_bytes(), encoded.as_bytes())
            .expect("stage execution failure");
    }
    access
        .commit_execution_failure()
        .expect("commit execution failure");
    assert_eq!(
        ports.terminal_execution_failure_rows(),
        1,
        "the committed terminal execution failure must be censused"
    );

    let (walked, derived, derived_rows) = encoded_checkpoints_from_both_count_sources(
        &ports.shared,
        ports.terminal_execution_failure_rows(),
    );
    assert_eq!(
        derived, walked,
        "with the census maintained, the O(1) source must still encode the \
             reference walk's checkpoint byte for byte"
    );
    assert_eq!(derived_rows, 0, "the O(1) source must not iterate history");
    assert!(
        ports
            .write_validated_prefix_checkpoint()
            .expect("graceful-shutdown checkpoint write"),
        "a clean validation must permit the shutdown checkpoint write"
    );
    assert_eq!(
        ports.checkpoint_count_rows_walked(),
        0,
        "the shutdown write must stay metadata-only with a maintained census"
    );
    drop(ports);

    let census = session_execution_failure_census(&path);
    assert_eq!(
        census, 1,
        "the next open must accept the checkpoint and re-seed the same census \
             from its own walk"
    );
}

/// Re-opens the database, requires the checkpoint fast path, and returns the
/// census this walk derived for itself.
fn session_execution_failure_census(path: &TestDatabasePath) -> u64 {
    let store = RedbStore::open(&path.0).expect("reopen for census");
    let mut session = store
        .begin_structural_evidence(inputs())
        .expect("begin structural evidence");
    assert!(
        session.checkpoint_verified(),
        "the checkpoint must be accepted: {:?}",
        session.checkpoint_ignored_reason()
    );
    let structural_end = finish_structural(&mut session);
    let historical_end = drain_historical(&mut session);
    let census = session.terminal_execution_failure_rows();
    session
        .finish(structural_end, historical_end)
        .expect("the censused terminal count must survive verification");
    census
}

/// The fail-closed claim, in code: a maintained census that has drifted can
/// never be silently trusted. Too high a census under-reports
/// `idempotency_count`, and the next open REFUSES rather than skipping a
/// prefix it cannot account for; an impossible census is caught before the
/// write and falls back to the reference walk.
#[test]
fn a_drifted_terminal_census_is_refused_at_the_next_open() {
    let path = TestDatabasePath::new("checkpoint-census-drift");
    let id = database_id(0x9c);
    let (store, _bundle) = checkpointable_history_store(&path, id, 6);
    let ports = open_cleanly(store);

    // Arm 1: an impossible census (more failures than terminal rows) is
    // detected before the write and falls back to the walk, which is always
    // correct. The row tally proves the fallback actually ran.
    ports.shared.seed_terminal_execution_failure_rows(u64::MAX);
    assert!(
        ports
            .write_validated_prefix_checkpoint()
            .expect("write under impossible census"),
        "the write must still succeed via the reference walk"
    );
    assert!(
        ports.checkpoint_count_rows_walked() > 0,
        "an impossible census must fall back to the reference walk"
    );

    // Arm 2: a census that is merely wrong (one too many) is NOT detectable
    // at write time; it under-reports the terminal prefix by one row.
    ports.shared.seed_terminal_execution_failure_rows(1);
    assert!(
        ports
            .write_validated_prefix_checkpoint()
            .expect("write under drifted census"),
        "a drifted census cannot be detected at write time"
    );
    drop(ports);

    let reopened = RedbStore::open(&path.0).expect("reopen drifted store");
    let mut session = reopened
        .begin_structural_evidence(inputs())
        .expect("begin structural evidence");
    assert!(
        session.checkpoint_verified(),
        "the drifted checkpoint binds and is accepted at load — refusal must \
             come from count verification, not from a binding check: {:?}",
        session.checkpoint_ignored_reason()
    );
    let mut cursor =
        StructuralEvidenceCursor::start(session.database_id(), session.open_session_id());
    let refused = loop {
        match session
            .read_structural_evidence(cursor, EvidencePageLimit::new(4).expect("page limit"))
        {
            Ok(StructuralEvidencePage::Page { next, .. }) => cursor = next,
            Ok(StructuralEvidencePage::ExactEnd(_)) => break false,
            Err(_) => break true,
        }
    };
    assert!(
        refused,
        "a checkpoint whose recorded prefix count disagrees with redb's own \
             row count must fail closed at open, never be silently trusted"
    );
}

/// Scale evidence for the record (not a gate): decomposes the shutdown
/// checkpoint build over a large synthetic history and times the reference
/// walk against the O(1) derivation.
///
/// Rows are raw and share one terminal value, which is faithful for the count
/// classes (they classify each row independently) and keeps generation cheap.
#[test]
#[ignore = "generates a large synthetic history to time the shutdown checkpoint build"]
fn shutdown_checkpoint_build_scale_evidence() {
    use crate::validated_prefix::{CheckpointCountSource, build_checkpoint_from_snapshot};
    use std::time::Instant;

    let commits = std::env::var("RIFFDB_CHECKPOINT_SCALE_COMMITS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(200_000);
    let entities = std::env::var("RIFFDB_CHECKPOINT_SCALE_ENTITIES")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(commits / 4);
    let path = TestDatabasePath::new("checkpoint-scale-evidence");
    let id = database_id(0x9d);
    let store = initialized_store(&path, id);
    let bundle = stored_bundle("checkpoint-scale", 1, b"checkpoint-scale-bundle");
    let started = Instant::now();
    let chunk = 20_000_u64;
    let mut next = 1_u64;
    while next <= commits {
        let take = chunk.min(commits.saturating_sub(next).saturating_add(1));
        write_checkpoint_count_shape(
            &store,
            id,
            &bundle,
            &CheckpointCountShape {
                commits: take,
                first_commit: next,
                events: take,
                routes: take,
                outbox: take,
                outbox_status: take,
                outcomes: 0,
                failures: 0,
                audits: 0,
                by_request: 0,
                entities: 0,
            },
        );
        next = next.saturating_add(take);
    }
    write_scale_terminal_and_audit_rows(&store, id, &bundle, commits, entities);
    println!(
        "generated commits={commits} entities={entities} in {:?}",
        started.elapsed()
    );

    let transaction = store.shared.database.begin_read().expect("read");
    let retained = read_retained_metadata_pub(&transaction).expect("retained metadata");
    let mut walked_rows = 0_u64;
    let walk_started = Instant::now();
    let walked = build_checkpoint_from_snapshot(
        &transaction,
        &retained,
        CheckpointCountSource::Walked,
        &mut walked_rows,
    )
    .expect("reference checkpoint");
    let walk_elapsed = walk_started.elapsed();
    let mut derived_rows = 0_u64;
    let derived_started = Instant::now();
    let derived = build_checkpoint_from_snapshot(
        &transaction,
        &retained,
        CheckpointCountSource::DurableLengths {
            execution_failed_rows: 0,
        },
        &mut derived_rows,
    )
    .expect("derived checkpoint");
    let derived_elapsed = derived_started.elapsed();
    println!(
        "walked: {walk_elapsed:?} over {walked_rows} rows; derived: \
             {derived_elapsed:?} over {derived_rows} rows"
    );
    assert_eq!(
        walked.base().counts(),
        derived.base().counts(),
        "the scale fixture must agree on counts or the timing compares \
             different work"
    );
}

/// Fills the terminal and administration tables for the scale fixture with
/// one shared value per class under distinct keys.
fn write_scale_terminal_and_audit_rows(
    store: &RedbStore,
    id: DatabaseId,
    bundle: &StoredContractBundleV1,
    rows: u64,
    entities: u64,
) {
    let outcome = checkpoint_count_outcome(bundle, id, 0, CommitSequence::first());
    let encoded = codec::encode_stored_outcome_v1(&outcome).expect("encode outcome");
    let chunk = 20_000_u64;
    let mut next = 0_u64;
    while next < rows {
        let end = chunk.saturating_add(next).min(rows);
        let write = store.shared.database.begin_write().expect("begin write");
        {
            let mut terminal = write.open_table(IDEMPOTENCY).expect("idempotency");
            let mut audit = write.open_table(AUDIT).expect("audit");
            let mut by_request = write.open_table(AUDIT_BY_REQUEST).expect("by request");
            for ordinal in next..end {
                terminal
                    .insert(&ordinal.to_be_bytes()[..], encoded.as_bytes())
                    .expect("insert outcome");
                let sequence = AdministrationSequence::new(ordinal.saturating_add(1))
                    .expect("administration sequence");
                audit
                    .insert(
                        keys::encode_audit_key(sequence).as_slice(),
                        [0x66_u8; 40].as_slice(),
                    )
                    .expect("insert audit");
                by_request
                    .insert(
                        keys::encode_audit_by_request_key(
                            request_id(u8::try_from(ordinal % 251).expect("seed")),
                            sequence,
                        )
                        .as_slice(),
                        [0x77_u8; 4].as_slice(),
                    )
                    .expect("insert audit index");
            }
        }
        write.commit().expect("commit scale chunk");
        next = end;
    }
    let mut written = 0_u64;
    while written < entities {
        let end = chunk.saturating_add(written).min(entities);
        let write = store.shared.database.begin_write().expect("begin write");
        {
            let mut table = write.open_table(ENTITIES).expect("entities");
            for ordinal in written..end {
                let record = history_entity(ordinal, EntityVersion::first(), b"scale", bundle);
                let encoded = codec::encode_entity_record_v1(&record).expect("encode entity");
                table
                    .insert(
                        keys::encode_entity_key(record.target().key()),
                        encoded.as_bytes(),
                    )
                    .expect("insert entity");
            }
        }
        write.commit().expect("commit entity chunk");
        written = end;
    }
}
