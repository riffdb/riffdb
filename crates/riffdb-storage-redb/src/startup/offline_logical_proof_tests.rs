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

#[test]
fn complete_logical_reader_clones_retain_the_original_pin_engine_and_mutation_lease() {
    use riffdb_storage_api::SnapshotFenceReader;
    let (scope, session) = fixture(0x68);
    let database_id = session.database_id();
    let session_id = session.open_session_id();
    let owner = Arc::downgrade(&session.shared);
    let proof = RedbOfflineIntegrityScrub::validate_complete_session(session).unwrap();
    let gate = proof.snapshot._lease.gate_for_test();
    let epoch = proof.snapshot.durable_commit_epoch;
    let original_read_frontier = read_retained_metadata(&proof.snapshot.read).unwrap();
    let (id, session, retained, catalog, reader) = proof.into_authorization_parts().unwrap();
    assert_eq!(id, database_id);
    assert_eq!(session, session_id);
    assert!(catalog.matches(id, session));
    assert_eq!(retained, original_read_frontier);
    let last_reader = reader.clone();
    assert_eq!(
        reader.application_frontier().unwrap(),
        last_reader.application_frontier().unwrap()
    );
    drop(reader);
    drop(catalog);
    assert!(
        owner.upgrade().is_some(),
        "logical reader clones must retain the SAME native engine owner"
    );
    assert!(
        gate.is_held(),
        "logical reader clones must retain the completed mutation lease"
    );
    assert!(gate.try_acquire().unwrap().is_none());
    last_reader.verify_complete_logical_read().unwrap();
    assert_eq!(owner.upgrade().unwrap().durable_commit_epoch(), epoch);
    assert!(matches!(
        redb::Database::open(scope.join("db.redb")),
        Err(redb::DatabaseError::DatabaseAlreadyOpen)
    ));
    drop(last_reader);
    assert!(
        owner.upgrade().is_none(),
        "the final reader releases all native ownership"
    );
    assert!(!gate.is_held());
    drop(redb::Database::open(scope.join("db.redb")).unwrap());
}

#[test]
fn complete_logical_reader_handoff_refuses_a_changed_epoch_catalog_or_cancellation() {
    for refusal in 0..3 {
        let (_scope, mut session) = fixture(0x69 + refusal);
        let cancelled = Arc::new(AtomicBool::new(false));
        session.set_cancellation(Arc::clone(&cancelled));
        let owner = Arc::downgrade(&session.shared);
        let mut proof = RedbOfflineIntegrityScrub::validate_complete_session(session).unwrap();
        let gate = proof.snapshot._lease.gate_for_test();
        if refusal == 0 {
            cancelled.store(true, Ordering::Release);
        } else if refusal == 1 {
            let shared = Arc::clone(&proof.snapshot.shared);
            let write = shared.database.begin_write().unwrap();
            write
                .open_table(OUTBOX_STATUS)
                .unwrap()
                .insert(b"reader-proof-fixture".as_slice(), b"changed".as_slice())
                .unwrap();
            shared.commit_durable(write).unwrap();
        } else {
            let (_other_scope, other_session) = fixture(0x70);
            let other =
                RedbOfflineIntegrityScrub::validate_complete_session(other_session).unwrap();
            proof.catalog = other.catalog;
        }
        let result = proof.into_authorization_parts();
        assert!(
            matches!(result, Err(error) if error.kind() == if refusal == 0 {StorageErrorKind::Unavailable} else {StorageErrorKind::CorruptData})
        );
        assert!(owner.upgrade().is_none());
        assert!(!gate.is_held());
    }
}

#[test]
fn complete_logical_reader_keeps_original_bytes_and_refuses_later_mutation_or_cancellation() {
    for cancel in [false, true] {
        let (_scope, mut session) = fixture(if cancel { 0x73 } else { 0x72 });
        let cancelled = Arc::new(AtomicBool::new(false));
        session.set_cancellation(Arc::clone(&cancelled));
        let proof = RedbOfflineIntegrityScrub::validate_complete_session(session).unwrap();
        let shared = Arc::clone(&proof.snapshot.shared);
        let gate = proof.snapshot._lease.gate_for_test();
        let (_, _, _, _, reader) = proof.into_authorization_parts().unwrap();
        let clone = reader.clone();
        if cancel {
            cancelled.store(true, Ordering::Release);
        } else {
            let write = shared.database.begin_write().unwrap();
            write
                .open_table(OUTBOX_STATUS)
                .unwrap()
                .insert(
                    b"reader-original-pin".as_slice(),
                    b"later-native-write".as_slice(),
                )
                .unwrap();
            shared.commit_durable(write).unwrap();
            assert!(
                reader
                    .complete_logical_access_for_test()
                    .read_value(
                        crate::journal::JournalTable::OutboxStatus,
                        b"reader-original-pin",
                    )
                    .unwrap()
                    .is_none(),
                "authorization must retain the original validated read"
            );
            let current = shared.database.begin_read().unwrap();
            assert_eq!(
                current
                    .open_table(OUTBOX_STATUS)
                    .unwrap()
                    .get(b"reader-original-pin".as_slice(),)
                    .unwrap()
                    .unwrap()
                    .value(),
                b"later-native-write"
            );
        }
        assert!(reader.verify_complete_logical_read().is_err());
        cancelled.store(false, Ordering::Release);
        assert!(
            clone.verify_complete_logical_read().is_err(),
            "another clone cannot launder invalidated logical evidence"
        );
        drop(reader);
        assert!(gate.is_held());
        drop(clone);
        assert!(!gate.is_held());
    }
}
