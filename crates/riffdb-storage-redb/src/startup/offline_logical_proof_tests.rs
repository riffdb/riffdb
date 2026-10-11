//! Actual native lifetime and refusal checks for complete logical evidence.
// req: REC-001, REC-006, END-001

use super::*;

fn fixture(
    seed: u8,
) -> (
    crate::test_path::ScopedDirectory,
    RedbStructuralEvidenceSession,
) {
    let scope = crate::test_path::ScopedDirectory::new("offline-logical-proof");
    let mut store = RedbStore::open(scope.join("db.redb")).unwrap();
    let database_id =
        DatabaseId::from_unix_milliseconds_and_random(1700000000000, [seed; 10]).unwrap();
    store.initialize_legacy_fixture(database_id).unwrap();
    let key = ReadableDigestKey::v1(DigestKeyId::new(1).unwrap());
    let inputs = StartupValidationInputs::new(
        Timestamp::new(1, 0).unwrap(),
        ReadableCapabilityDigestInventory::new(vec![key]).unwrap(),
        ReadableIdempotencyDigestInventory::new(vec![key]).unwrap(),
    );
    let session = store
        .begin_structural_evidence_for(inputs, EvidenceOpenPurpose::OfflineIntegrityScrub)
        .unwrap();
    (scope, session)
}

#[test]
fn complete_logical_proof_retains_native_owner_exclusion_and_exact_catalog_session() {
    let (scope, session) = fixture(0x62);
    let database_id = session.database_id();
    let session_id = session.open_session_id();
    let owner = Arc::downgrade(&session.shared);
    let proof = RedbOfflineIntegrityScrub::validate_complete_session(session).unwrap();
    let shared = owner
        .upgrade()
        .expect("complete proof retains the engine owner");
    let gate = proof.snapshot._lease.gate_for_test();
    assert!(gate.is_held());
    assert!(gate.try_acquire().unwrap().is_none());
    assert!(proof.catalog.matches(database_id, session_id));
    assert_eq!(
        read_retained_metadata(&proof.snapshot.read).unwrap(),
        proof.snapshot.retained_metadata
    );
    assert!(matches!(
        redb::Database::open(scope.join("db.redb")),
        Err(redb::DatabaseError::DatabaseAlreadyOpen)
    ));
    drop(shared);
    proof.into_receipt().unwrap();
    assert!(
        owner.upgrade().is_none(),
        "receipt conversion releases every native owner and pin"
    );
    drop(redb::Database::open(scope.join("db.redb")).unwrap());
}

#[test]
fn complete_logical_proof_refuses_a_later_unclassified_native_commit() {
    let (_scope, session) = fixture(0x63);
    let proof = RedbOfflineIntegrityScrub::validate_complete_session(session).unwrap();
    let shared = Arc::clone(&proof.snapshot.shared);
    let gate = proof.snapshot._lease.gate_for_test();
    // Explicitly exercise the internal raw engine boundary. Ordinary safe
    // writers cannot pass the retained mutation lease; an unclassified native
    // commit must invalidate the evidence rather than rebind its old read.
    let write = shared.database.begin_write().unwrap();
    write
        .open_table(OUTBOX_STATUS)
        .unwrap()
        .insert(b"proof-fixture".as_slice(), b"changed".as_slice())
        .unwrap();
    shared.commit_durable(write).unwrap();
    assert_ne!(
        shared.durable_commit_epoch(),
        proof.snapshot.durable_commit_epoch
    );
    assert!(
        proof
            .snapshot
            .read
            .open_table(OUTBOX_STATUS)
            .unwrap()
            .get(b"proof-fixture".as_slice())
            .unwrap()
            .is_none(),
        "the retained proof still owns its original immutable read"
    );
    assert!(
        matches!(proof.into_receipt(), Err(error) if error.kind() == StorageErrorKind::CorruptData)
    );
    assert!(!gate.is_held(), "refusal releases the nondurable lease");
    assert!(
        gate.try_acquire().unwrap().is_some(),
        "actual following admission succeeds after refusal"
    );
}

#[test]
fn complete_logical_proof_refuses_cancellation_after_both_exact_ends() {
    let (_scope, mut session) = fixture(0x64);
    let cancelled = Arc::new(AtomicBool::new(false));
    session.set_cancellation(Arc::clone(&cancelled));
    let owner = Arc::downgrade(&session.shared);
    let proof = RedbOfflineIntegrityScrub::validate_complete_session(session).unwrap();
    cancelled.store(true, Ordering::Release);
    assert!(
        matches!(proof.into_receipt(), Err(error) if error.kind() == StorageErrorKind::Unavailable)
    );
    assert!(
        owner.upgrade().is_none(),
        "cancelled evidence cannot retain an owner or pin"
    );
}

#[test]
fn complete_logical_proof_refuses_a_catalog_from_another_validated_session() {
    let (_first_scope, first) = fixture(0x65);
    let (_second_scope, second) = fixture(0x66);
    let first = RedbOfflineIntegrityScrub::validate_complete_session(first).unwrap();
    let second = RedbOfflineIntegrityScrub::validate_complete_session(second).unwrap();
    let CompleteLogicalValidation {
        snapshot,
        catalog: _,
    } = first;
    let CompleteLogicalValidation {
        snapshot: _,
        catalog,
    } = second;
    let substituted = CompleteLogicalValidation { snapshot, catalog };
    assert!(
        matches!(substituted.into_receipt(), Err(error) if error.kind() == StorageErrorKind::CorruptData)
    );
}

#[test]
fn complete_logical_proof_requires_both_exact_ends_from_the_owned_session() {
    let (_scope, session) = fixture(0x67);
    let structural = RedbStructuralEvidenceEnd {
        cursor: StructuralEvidenceCursor::start(session.database_id(), session.open_session_id()),
    };
    let historical = RedbHistoricalEvidenceEnd {
        cursor: HistoricalEvidenceCursor::start(session.database_id(), session.open_session_id()),
    };
    assert!(
        matches!(session.finish_offline_integrity_scrub(structural, historical),
        Err(error) if error.kind() == StorageErrorKind::CorruptData)
    );
}
