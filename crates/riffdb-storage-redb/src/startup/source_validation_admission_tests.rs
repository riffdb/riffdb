//! Numerical Source scan work on the actual native engine, without publication.
use super::*;
use riffdb_storage_api::proto_codec::*;
use riffdb_types::{
    CanonicalRecord, CanonicalValue, ContractLineage, FieldId, ProjectionCheckpointOrdinal,
    ProjectionId, ProjectionIdentity, ProjectionPlanHash, ProjectionSourceLocatorV1,
};

// req: PRJ-004, STO-020, REC-001
#[test]
fn maintenance_source_decode_ignores_worker_pressure_and_retains_final_physical_charge() {
    let root = crate::test_path::ScopedDirectory::new("source-maintenance-decode-pressure");
    let store = crate::RedbStore::open(root.join("db.redb")).unwrap();
    let pool = crate::locator_memory::NativeLocatorPool::global();
    // A canonical structural fixture is not a published/evaluated source.
    let row = structural_row();
    let encoded = encode_projection_source_row_v1(&row).unwrap();
    let budget = store.shared.database.source_working_budget().clone();
    let observed = budget.observation().unwrap();
    let remaining = riffdb_types::PROJECTION_SOURCE_WORKING_MEMORY_BYTES
        - observed.fixed_owner_bytes
        - observed.owned_bytes
        - observed.reserved_bytes;
    let pressure = budget.reserve(remaining).unwrap();
    let saturated = budget.observation().unwrap();
    assert!(
        decode_projection_source_row_structural_admitted_v1(encoded.as_bytes(), &budget).is_err()
    );
    let decoded = decode(
        &store.shared,
        encoded.as_bytes(),
        inspect_projection_source_row_caller_decode_v1,
        decode_projection_source_row_with_caller_scratch_v1,
    )
    .unwrap();
    assert_eq!(decoded.value(), &row);
    assert_eq!(budget.observation().unwrap(), saturated);
    let bytes = decoded._work.bytes();
    let weak = std::sync::Arc::downgrade(&store.shared);
    drop(store);
    assert!(weak.upgrade().is_none());
    let held = pool.charged_bytes();
    assert!(held >= bytes);
    drop(decoded);
    assert_eq!(pool.charged_bytes(), held - bytes);
    drop(pressure);
}

// req: PRJ-004, STO-020, REC-001
#[test]
fn maintenance_raw_source_owner_keeps_copied_bytes_charged_after_engine_drop() {
    let root = crate::test_path::ScopedDirectory::new("source-maintenance-raw-owner");
    let store = crate::RedbStore::open(root.join("db.redb")).unwrap();
    let pool = crate::locator_memory::NativeLocatorPool::global();
    let row = structural_row();
    let encoded = encode_projection_source_row_v1(&row).unwrap();
    let raw =
        RawStructuralRowV1::source(&store.shared, 37, 0, b"structural-key", encoded.as_bytes())
            .unwrap();
    let bytes = raw._source_work.as_ref().unwrap().bytes();
    let weak = std::sync::Arc::downgrade(&store.shared);
    drop(store);
    assert!(weak.upgrade().is_none());
    assert_eq!(
        raw.parts(),
        (37, 0, b"structural-key".as_slice(), encoded.as_bytes())
    );
    let held = pool.charged_bytes();
    drop(raw);
    assert_eq!(pool.charged_bytes(), held - bytes);
}

fn structural_row() -> riffdb_storage_api::ProjectionSourceRowV1 {
    riffdb_storage_api::ProjectionSourceRowV1::from_stored_parts(
        ProjectionIdentity::new(
            ContractLineage::new("maintenance_structural").unwrap(),
            ProjectionId::first(),
            ProjectionPlanHash::from_bytes([7; 32]),
        ),
        ProjectionSourceLocatorV1::new(
            DatabaseId::from_unix_milliseconds_and_random(42, [7; 10]).unwrap(),
            1,
            ProjectionCheckpointOrdinal::new(1).unwrap(),
        )
        .unwrap(),
        vec![CanonicalValue::U64(7)],
        CanonicalRecord::new(vec![(FieldId::first(), CanonicalValue::U64(1))]).unwrap(),
        CommitSequence::first(),
    )
    .unwrap()
}

static REFUSAL_MAPPER_CALLED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
fn refusal_mapper(
    _: &[u8],
    _: &mut Vec<u8>,
) -> Result<
    riffdb_storage_api::EncodedPageItem<riffdb_storage_api::ProjectionSourceRowV1>,
    ProjectionSourceRecordDecodeErrorV1,
> {
    REFUSAL_MAPPER_CALLED.store(true, std::sync::atomic::Ordering::SeqCst);
    Err(ProjectionSourceRecordDecodeErrorV1::AdmissionUnavailable)
}
// Run alone: this test deliberately saturates the existing shared Maintenance
// counter rather than inventing a separate pool or borrowing Source capacity.
// req: PRJ-004, STO-020, REC-001
#[test]
fn maintenance_source_decode_actual_pool_pressure_refuses_before_mapper_and_refunds_failure() {
    let root = crate::test_path::ScopedDirectory::new("source-maintenance-pool-refusal");
    let store = crate::RedbStore::open(root.join("db.redb")).unwrap();
    let pool = crate::locator_memory::NativeLocatorPool::global();
    let row = structural_row();
    let encoded = encode_projection_source_row_v1(&row).unwrap();
    let source_before = store
        .shared
        .database
        .source_working_budget()
        .observation()
        .unwrap();
    let held = pool.charged_bytes();
    let pressure = pool
        .admit(
            crate::locator_memory::LOCATOR_TOTAL_BYTES - held,
            crate::locator_memory::LocatorAllocationClass::Maintenance,
        )
        .unwrap();
    REFUSAL_MAPPER_CALLED.store(false, std::sync::atomic::Ordering::SeqCst);
    let error = decode(
        &store.shared,
        encoded.as_bytes(),
        inspect_projection_source_row_caller_decode_v1,
        refusal_mapper,
    )
    .err()
    .unwrap();
    assert_eq!(error.kind(), StorageErrorKind::Unavailable);
    assert!(!REFUSAL_MAPPER_CALLED.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(
        pool.charged_bytes(),
        crate::locator_memory::LOCATOR_TOTAL_BYTES
    );
    assert_eq!(
        store
            .shared
            .database
            .source_working_budget()
            .observation()
            .unwrap(),
        source_before
    );
    drop(pressure);
    assert_eq!(pool.charged_bytes(), held);
    let error = decode(
        &store.shared,
        encoded.as_bytes(),
        inspect_projection_source_row_caller_decode_v1,
        refusal_mapper,
    )
    .err()
    .unwrap();
    assert_eq!(error.kind(), StorageErrorKind::Unavailable);
    assert!(REFUSAL_MAPPER_CALLED.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(pool.charged_bytes(), held);
    assert_eq!(
        store
            .shared
            .database
            .source_working_budget()
            .observation()
            .unwrap(),
        source_before
    );
}
