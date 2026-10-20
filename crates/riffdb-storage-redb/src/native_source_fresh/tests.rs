// req: REP-002, REP-003, REC-001, STO-012, PRJ-004
use super::*;
use redb::{ReadableDatabase, ReadableTable};
use riffdb_types::DatabaseId;
use std::collections::BTreeMap;

fn database() -> DatabaseId {
    DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x73; 10]).unwrap()
}
fn fresh(root: &crate::test_path::ScopedDirectory) -> crate::RedbStore {
    let mut store = crate::RedbStore::open(root.join("database.redb")).unwrap();
    assert!(
        matches!(store.initialize_projection_source_fixture(database()).unwrap(), DatabaseInitializationResult::Installed(id) if id==database())
    );
    store
}
fn metadata(store: &crate::RedbStore) -> BTreeMap<String, Vec<u8>> {
    store
        .shared
        .database
        .begin_read()
        .unwrap()
        .open_table(META)
        .unwrap()
        .iter()
        .unwrap()
        .map(|row| {
            let (key, value) = row.unwrap();
            (key.value().to_owned(), value.value().to_vec())
        })
        .collect()
}

#[test]
fn genuine_empty_v3_has_writer5_real_next1_and_exact_ten_insert_activation() {
    let root = crate::test_path::ScopedDirectory::new("native-empty-v3-net-recipe");
    let store = fresh(&root);
    let marker = decode_durable_format_marker(
        &std::fs::read(crate::durable_format_marker_path(
            &root.join("database.redb"),
        ))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(marker.identity().writer().get(), 5);
    assert_eq!(marker.registry_digest(), current_record_registry_digest());
    let read = store.shared.database.begin_read().unwrap();
    let meta = read.open_table(META).unwrap();
    let allocator = meta
        .get("projection_checkpoint_ordinal/v1")
        .unwrap()
        .unwrap();
    assert_eq!(
        *decode_projection_checkpoint_ordinal_allocator_inline_v1(allocator.value())
            .unwrap()
            .value(),
        ProjectionCheckpointOrdinalAllocatorV1::initial(1).unwrap()
    );
    let history_table = read
        .open_table(crate::changelog_v3_activation::HISTORY)
        .unwrap();
    let row = history_table
        .get(1_u64.to_be_bytes().as_slice())
        .unwrap()
        .unwrap();
    let receipt = AuthoritativeTransactionV3::decode(row.value()).unwrap();
    assert_eq!(receipt.attribution(), ChangelogAttributionV3::V3Activation);
    assert_eq!(receipt.mutations().len(), 10);
    assert!(
        receipt
            .mutations()
            .iter()
            .all(|mutation| mutation.matches_prior(None) && mutation.expected_hash().is_none())
    );
    assert_eq!(
        receipt.binding().predecessor_frontier,
        DualFrontier::INITIAL
    );
    assert_eq!(
        receipt.binding().covered_frontier,
        DualFrontier::new(None, Some(AdministrationSequence::first()))
    );
    let originals: [Vec<u8>; 9] =
        INITIAL_KEYS.map(|key| meta.get(key).unwrap().unwrap().value().to_vec());
    let key = crate::keys::encode_audit_key(AdministrationSequence::first());
    let audit = read.open_table(AUDIT).unwrap();
    let origin = audit.get(key.as_slice()).unwrap().unwrap();
    validate_fresh_recipe(&receipt, &originals, origin.value()).unwrap();
    let decoded = decode_native_catalog_transition_admitted_v1(
        origin.value(),
        store.shared.database.source_working_budget(),
    )
    .unwrap();
    assert!(matches!(
        decoded.value().parts().origin,
        NativeCatalogTransitionOriginV1::FreshV3
    ));
    validate_same_owner_cold_origin(&store.shared.database, &read).unwrap();
    let admission = crate::primary_admission_roots::read(&meta)
        .unwrap()
        .unwrap();
    assert_eq!(
        admission.lineage().catalog_digest(),
        AuthoritativeStateCatalogV3.digest()
    );
    assert!(admission.fence().is_none());
}

#[test]
fn fresh_fixture_refuses_existing_v2_without_rewriting_its_bytes() {
    let root = crate::test_path::ScopedDirectory::new("native-existing-v2-not-fresh");
    let mut store = crate::RedbStore::open(root.join("database.redb")).unwrap();
    DatabaseInitializationPort::initialize_database(&mut store, database()).unwrap();
    let before = metadata(&store);
    assert!(
        matches!(store.initialize_projection_source_fixture(database()),Err(error) if error.kind()==StorageErrorKind::IncompatibleFormat)
    );
    assert_eq!(metadata(&store), before);
    assert!(!before.contains_key("projection_checkpoint_ordinal/v1"));
}

#[test]
fn same_owner_origin_check_is_source_pressure_independent_and_exact() {
    let root = crate::test_path::ScopedDirectory::new("native-origin-hot-proof");
    let store = fresh(&root);
    let read = store.shared.database.begin_read().unwrap();
    let meta = read.open_table(META).unwrap();
    let key = crate::keys::encode_audit_key(AdministrationSequence::first());
    let audit = read.open_table(AUDIT).unwrap();
    let encoded = audit.get(key.as_slice()).unwrap().unwrap();
    let catalog = meta
        .get(metadata_key(AuthoritativeNamespaceV1::AuthoritativeStateCatalog).unwrap())
        .unwrap()
        .unwrap();
    let incarnation = meta.get(META_HISTORY_INCARNATION).unwrap().unwrap();
    let leadership = meta
        .get(metadata_key(AuthoritativeNamespaceV1::LeadershipEpoch).unwrap())
        .unwrap()
        .unwrap();
    let owner = &store.shared.database;
    let budget = owner.source_working_budget();
    let before = budget.observation().unwrap();
    let remaining = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        - before.fixed_owner_bytes
        - before.owned_bytes
        - before.reserved_bytes;
    let pressure = budget.reserve(remaining).unwrap();
    let saturated = budget.observation().unwrap();
    assert!(budget.reserve(1).is_err());
    assert_eq!(
        owner
            .native_origin_context()
            .check_control_head(
                &key,
                encoded.value(),
                catalog.value(),
                incarnation.value(),
                leadership.value()
            )
            .unwrap(),
        AdministrationSequence::first()
    );
    assert_eq!(budget.observation().unwrap(), saturated);
    let mut changed = encoded.value().to_vec();
    changed[0] ^= 1;
    assert!(
        owner
            .native_origin_context()
            .check_control_head(
                &key,
                &changed,
                catalog.value(),
                incarnation.value(),
                leadership.value()
            )
            .is_err()
    );
    assert!(
        owner
            .native_origin_context()
            .check_control_head(
                &crate::keys::encode_audit_key(AdministrationSequence::new(2).unwrap()),
                encoded.value(),
                catalog.value(),
                incarnation.value(),
                leadership.value()
            )
            .is_err()
    );
    assert!(
        owner
            .native_origin_context()
            .check_control_head(
                &key,
                encoded.value(),
                catalog.value(),
                incarnation.value(),
                b"changed-lineage"
            )
            .is_err()
    );
    assert_eq!(budget.observation().unwrap(), saturated);
    drop(pressure);
}

#[test]
fn a_new_engine_cannot_reuse_fresh_context_by_database_identity() {
    let root = crate::test_path::ScopedDirectory::new("native-origin-new-owner-refusal");
    let path = root.join("database.redb");
    {
        let store = fresh(&root);
        let read = store.shared.database.begin_read().unwrap();
        validate_same_owner_cold_origin(&store.shared.database, &read).unwrap();
    }
    let reopened = crate::RedbStore::open(path).unwrap();
    let read = reopened.shared.database.begin_read().unwrap();
    assert!(
        matches!(validate_same_owner_cold_origin(&reopened.shared.database,&read),Err(error) if error.kind()==StorageErrorKind::Unavailable)
    );
}

#[test]
fn fresh_capacity_refusal_preserves_empty_state_and_releases_all_work() {
    let root = crate::test_path::ScopedDirectory::new("native-empty-v3-capacity-refusal");
    let mut store = crate::RedbStore::open(root.join("database.redb")).unwrap();
    let budget = store.shared.database.source_working_budget().clone();
    let baseline = budget.observation().unwrap();
    let remaining = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        - baseline.fixed_owner_bytes
        - baseline.owned_bytes
        - baseline.reserved_bytes;
    let pressure = budget.reserve(remaining).unwrap();
    let saturated = budget.observation().unwrap();
    assert!(
        matches!(store.initialize_projection_source_fixture(database()),Err(error) if error.kind()==StorageErrorKind::Unavailable)
    );
    assert_eq!(
        store.probe_database_identity().unwrap(),
        DatabaseIdentityProbe::NeedsInitialization
    );
    assert_eq!(budget.observation().unwrap(), saturated);
    drop(pressure);
    assert_eq!(budget.observation().unwrap(), baseline);
    assert!(
        matches!(store.initialize_projection_source_fixture(database()).unwrap(),DatabaseInitializationResult::Installed(id) if id==database())
    );
}
// req: PRJ-004, STO-020, REC-001
#[test]
fn bounded_source_origin_observer_requires_actual_current_engine_context_after_reopen() {
    let root = crate::test_path::ScopedDirectory::new("source-origin-observer-engine-affinity");
    let path = root.join("database.redb");
    {
        let store = fresh(&root);
        let read = store.shared.database.begin_read().unwrap();
        let owner = &store.shared.database;
        let budget = owner.source_working_budget();
        let before = budget.observation().unwrap();
        let remaining = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
            - before.fixed_owner_bytes
            - before.owned_bytes
            - before.reserved_bytes;
        let pressure = budget.reserve(remaining).unwrap();
        let saturated = budget.observation().unwrap();
        let mut observation = SourceOriginObservationV1::new();
        for _ in 0..3 {
            observation.step(owner, &read).unwrap();
            assert!(!observation.complete());
            assert_eq!(budget.observation().unwrap(), saturated);
        }
        observation.step(owner, &read).unwrap();
        assert!(observation.complete());
        assert!(observation.checked().is_ok());
        assert_eq!(budget.observation().unwrap(), saturated);
        assert_eq!(
            observation.step(owner, &read).unwrap_err().kind(),
            StorageErrorKind::InvariantViolation
        );
        drop(pressure);
    }
    let pool = crate::locator_memory::NativeLocatorPool::global();
    let mut retired = false;
    for _ in 0..256 {
        if !pool.drain_retired_step() {
            retired = true;
            break;
        }
    }
    assert!(retired);
    // The exact same durable database/controls do not transfer volatile engine
    // context. This is an origin component control, never a Source publication.
    let reopened = crate::RedbStore::open(&path).unwrap();
    let read = reopened.shared.database.begin_read().unwrap();
    let owner = &reopened.shared.database;
    assert!(owner.native_origin_context().anchor_key().is_none());
    let budget = owner.source_working_budget();
    let before = budget.observation().unwrap();
    let remaining = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        - before.fixed_owner_bytes
        - before.owned_bytes
        - before.reserved_bytes;
    let pressure = budget.reserve(remaining).unwrap();
    let saturated = budget.observation().unwrap();
    let mut observation = SourceOriginObservationV1::new();
    for _ in 0..3 {
        observation.step(owner, &read).unwrap();
    }
    assert_eq!(
        observation.step(owner, &read).unwrap_err().kind(),
        StorageErrorKind::Unavailable
    );
    assert!(!observation.complete());
    assert!(observation.checked().is_err());
    assert!(owner.native_origin_context().anchor_key().is_none());
    assert_eq!(budget.observation().unwrap(), saturated);
    drop(pressure);
}
