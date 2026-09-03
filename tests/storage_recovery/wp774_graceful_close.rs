#[test]
// req: STO-023, REC-001, REC-002, REC-004, PERF-019
fn crash_before_graceful_clean_commit_preserves_checkpoint_and_dirty_fallback() {
    let path = TestDatabasePath::new("before-graceful-clean-commit");
    let _ = prepare_committed_command_database(&path.0);
    let ports = open_operational(RedbStore::open(&path.0).expect("open to seed CLEAN"));
    assert_eq!(
        ports.complete_graceful_close().lifecycle(),
        riffdb_storage_redb::GracefulLifecycleOutcomeV1::CleanCommitted
    );
    drop(ports);
    let checkpoint = checkpoint_full_bytes(&path.0);
    let allocators = allocator_metadata_bytes(&path.0);
    run_crashing_child("before-graceful-clean-commit", &path.0);

    assert_eq!(
        checkpoint_full_bytes(&path.0),
        checkpoint,
        "pre-CLEAN crash must not create, refresh, or delete checkpoint bytes"
    );
    assert_eq!(allocator_metadata_bytes(&path.0), allocators);
    let (clean_fast, checkpoint_verified) = startup_path_observation(&path.0);
    assert!(!clean_fast, "pre-CLEAN crash must leave lifecycle DIRTY");
    assert!(
        checkpoint_verified,
        "dirty recovery may verify the unchanged prefix"
    );
    let store = RedbStore::open(&path.0).expect("reopen for complete dirty drain");
    let (_, catalog, structural, _, _) = complete_startup_observing_checkpoint(store);
    assert!(matches!(catalog, CatalogHistoryOutcome::Ready(_)));
    assert!(matches!(structural, StructuralOpenOutcome::Clean(_)));
    drop(catalog);
    drop(structural);
    assert_eq!(allocator_metadata_bytes(&path.0), allocators);
}

#[test]
// req: STO-023, REC-001, REC-002, REC-004, PERF-019
fn crash_after_graceful_clean_commit_preserves_checkpoint_and_atomic_clean() {
    let path = TestDatabasePath::new("after-graceful-clean-commit");
    let _ = prepare_committed_command_database(&path.0);
    let ports = open_operational(RedbStore::open(&path.0).expect("open to seed CLEAN"));
    assert_eq!(
        ports.complete_graceful_close().lifecycle(),
        riffdb_storage_redb::GracefulLifecycleOutcomeV1::CleanCommitted
    );
    drop(ports);
    let checkpoint = checkpoint_full_bytes(&path.0);
    let allocators = allocator_metadata_bytes(&path.0);
    run_crashing_child("after-graceful-clean-commit", &path.0);

    assert_eq!(
        checkpoint_full_bytes(&path.0),
        checkpoint,
        "post-CLEAN crash must retain checkpoint bytes exactly"
    );
    assert_eq!(allocator_metadata_bytes(&path.0), allocators);
    let (clean_fast, _) = startup_path_observation(&path.0);
    assert!(
        clean_fast,
        "post-commit crash must expose the complete CLEAN successor"
    );
    let store = RedbStore::open(&path.0).expect("reopen complete CLEAN successor");
    let (_, catalog, structural, _, _) = complete_startup_observing_checkpoint(store);
    assert!(matches!(catalog, CatalogHistoryOutcome::Ready(_)));
    assert!(matches!(structural, StructuralOpenOutcome::Clean(_)));
    drop(catalog);
    drop(structural);
    assert_eq!(allocator_metadata_bytes(&path.0), allocators);
}

#[test]
// req: STO-023, REC-001, REC-002, REC-004, PERF-019
fn segmented_command_checkpoint_is_exact_current_without_decoding_audit_population() {
    use riffdb_storage_api::{
        decode_administration_sequence_allocator_v1, decode_validated_prefix_checkpoint_v2,
    };

    let path = TestDatabasePath::new("graceful-segmented-command-exact-current");
    let _ = prepare_committed_command_database(&path.0);
    let ports = open_operational(RedbStore::open(&path.0).expect("checkpoint command history"));
    drop(ports);
    let database = Database::create(&path.0).expect("open bounded metadata view");
    let read = database.begin_read().expect("read bounded metadata");
    let meta = read.open_table(META).expect("meta");
    let checkpoint = decode_validated_prefix_checkpoint_v2(
        meta.get(CHECKPOINT_META_KEY)
            .expect("checkpoint lookup")
            .expect("checkpoint present")
            .value(),
    )
    .expect("decode checkpoint")
    .into_parts()
    .0;
    let allocator = decode_administration_sequence_allocator_v1(
        meta.get(META_ADMINISTRATION_SEQUENCE)
            .expect("allocator lookup")
            .expect("allocator present")
            .value(),
    )
    .expect("decode administration allocator")
    .into_parts()
    .0;
    let AdministrationSequenceAllocator::Next(next) = allocator else {
        panic!("small command fixture cannot exhaust administration sequences")
    };
    assert!(
        next.get() > checkpoint.base().audit_sequence_bound().saturating_add(1),
        "segmented command audit records must advance the logical allocator beyond the physical AUDIT bound"
    );
    drop(meta);
    drop(read);
    drop(database);

    let ports = open_operational(RedbStore::open(&path.0).expect("reopen segmented history"));
    assert_eq!(
        ports.complete_graceful_close().disposition(),
        riffdb_storage_redb::GracefulCheckpointDispositionV1::RetainedExactCurrent
    );
}

#[test]
// req: STO-023, REC-001, REC-002, REC-004, PERF-019
fn process_graceful_checkpoint_close_crash_matrix_reaches_exact_dirty_ends() {
    for (ordinal, mode) in [
        "before-graceful-close-barrier",
        "during-graceful-close-barrier",
        "after-graceful-close-barrier",
        "before-graceful-checkpoint-classification",
        "after-graceful-checkpoint-classification",
    ]
    .into_iter()
    .enumerate()
    {
        let path = TestDatabasePath::new(&format!("graceful-stage-crash-{ordinal}"));
        let _ = prepare_committed_command_database(&path.0);
        let ports = open_operational(RedbStore::open(&path.0).expect("open to seed CLEAN"));
        assert_eq!(
            ports.complete_graceful_close().lifecycle(),
            riffdb_storage_redb::GracefulLifecycleOutcomeV1::CleanCommitted
        );
        drop(ports);
        let checkpoint = checkpoint_full_bytes(&path.0);
        let allocators_before_child = allocator_metadata_bytes(&path.0);

        run_crashing_child(mode, &path.0);

        assert_eq!(
            checkpoint_full_bytes(&path.0),
            checkpoint,
            "barrier and classification crashes must retain checkpoint bytes"
        );
        let allocators_after_crash = allocator_metadata_bytes(&path.0);
        if mode != "during-graceful-close-barrier" {
            assert_eq!(
                allocators_after_crash, allocators_before_child,
                "a crashing close must advance neither allocator"
            );
        } else {
            assert_ne!(
                allocators_after_crash, allocators_before_child,
                "the deliberately acknowledged deferred command must advance authority"
            );
        }
        let (clean_fast, checkpoint_verified) = startup_path_observation(&path.0);
        assert!(
            !clean_fast,
            "every pre-CLEAN crash must leave lifecycle DIRTY"
        );
        assert!(
            checkpoint_verified,
            "dirty recovery may verify the retained exact prefix"
        );
        let store = RedbStore::open(&path.0).expect("reopen for complete dirty drain");
        let (_, catalog, structural, _, _) = complete_startup_observing_checkpoint(store);
        assert!(
            matches!(catalog, CatalogHistoryOutcome::Ready(_)),
            "dirty recovery must consume catalog-semantic history through exact end"
        );
        assert!(
            matches!(structural, StructuralOpenOutcome::Clean(_)),
            "dirty recovery must consume structural history through exact end"
        );
        drop(catalog);
        drop(structural);
        assert_eq!(
            allocator_metadata_bytes(&path.0),
            allocators_after_crash,
            "dirty recovery must advance neither allocator"
        );
        if mode == "during-graceful-close-barrier" {
            let ports = open_operational(
                RedbStore::open(&path.0).expect("reopen acknowledged deferred suffix"),
            );
            let fixture = command_fixture_at(2);
            assert_eq!(
                ports
                    .read_entity(&fixture.target)
                    .expect("read suffix entity"),
                Some(fixture.records.entities()[0].post_image().clone()),
                "the acknowledged deferred suffix must survive the mid-barrier crash"
            );
            assert!(matches!(
                ports
                    .lookup_admission(fixture.candidates)
                    .expect("lookup suffix admission"),
                AdmissionLookupResultV1::Found(_)
            ));
        }
    }
}

type CheckpointFullBytes = (Option<Vec<u8>>, Vec<(Vec<u8>, Vec<u8>)>);

fn checkpoint_full_bytes(path: &Path) -> CheckpointFullBytes {
    let database = Database::create(path).expect("open for complete checkpoint bytes");
    let txn = database
        .begin_read()
        .expect("begin complete checkpoint read");
    let meta = txn.open_table(META).expect("open meta");
    let checkpoint = meta
        .get(CHECKPOINT_META_KEY)
        .expect("get checkpoint")
        .map(|value| value.value().to_vec());
    let heads = txn
        .open_table(VALIDATED_PREFIX_ENTITY_HEADS)
        .expect("checkpoint heads")
        .iter()
        .expect("iterate checkpoint heads")
        .map(|row| {
            let (key, value) = row.expect("checkpoint head row");
            (key.value().to_vec(), value.value().to_vec())
        })
        .collect();
    (checkpoint, heads)
}

fn metadata_bytes(path: &Path, key: &str) -> Vec<u8> {
    let database = Database::create(path).expect("open for metadata bytes");
    let txn = database.begin_read().expect("begin metadata read");
    let meta = txn.open_table(META).expect("open meta");
    meta.get(key)
        .expect("metadata lookup")
        .expect("metadata present")
        .value()
        .to_vec()
}

fn allocator_metadata_bytes(path: &Path) -> (Vec<u8>, Vec<u8>) {
    (
        metadata_bytes(path, "next_application_sequence"),
        metadata_bytes(path, META_ADMINISTRATION_SEQUENCE),
    )
}
