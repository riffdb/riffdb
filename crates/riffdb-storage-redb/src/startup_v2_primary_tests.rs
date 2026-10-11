//! Production structural/catalog handoff must establish checked source admission.
// req: REP-005, REC-001, STO-012
use super::*;
use riffdb_storage_api::{
    AuthoritativeNamespaceV1, AuthoritativeNamespaceV2, AuthoritativeStateCatalogV2,
    proto_codec::{decode_changelog_history_state_v3, decode_replication_primary_admission_v1},
};

#[test]
fn fresh_source_handoff_installs_active_v2_before_granting_operational_ports() {
    let path = TestDatabasePath::new("v2-source-startup-admission");
    let mut store = RedbStore::open(&path.0).unwrap();
    store.initialize_database(database_id(0xe1)).unwrap();
    let shared = Arc::clone(&store.shared);
    let ports = finish_source(store);
    let read = shared.database.begin_read().unwrap();
    let meta = read.open_table(META).unwrap();
    let admission = meta
        .get(
            AuthoritativeNamespaceV2::ReplicationPrimaryAdmission
                .metadata_key()
                .unwrap(),
        )
        .unwrap();
    assert!(
        admission.is_some(),
        "source operational ports require explicit checked primary admission"
    );
    let admission = decode_replication_primary_admission_v1(admission.unwrap().value()).unwrap();
    let history = decode_changelog_history_state_v3(
        meta.get(
            AuthoritativeNamespaceV1::ChangelogHistoryState
                .metadata_key()
                .unwrap(),
        )
        .unwrap()
        .unwrap()
        .value(),
    )
    .unwrap();
    assert_eq!(
        history.value().lineage().catalog_digest(),
        AuthoritativeStateCatalogV2.digest()
    );
    assert_eq!(admission.value().lineage(), history.value().lineage());
    assert!(admission.value().fence().is_none());
    use riffdb_storage_api::ReplicationPrimaryAdmissionReadPort;
    assert_eq!(
        ports.read_replication_primary_admission().unwrap(),
        *admission.value()
    );
    assert_eq!(
        ports
            .shared_ports()
            .read_replication_primary_admission()
            .unwrap(),
        *admission.value()
    );
}

#[test]
fn coordinator_admission_read_refuses_legacy_or_lost_source_state_without_repair() {
    use riffdb_storage_api::ReplicationPrimaryAdmissionReadPort;
    for legacy in [false, true] {
        let path = TestDatabasePath::new("v2-coordinator-admission-refusal");
        let mut store = RedbStore::open(&path.0).unwrap();
        if legacy {
            store.initialize_legacy_fixture(database_id(0xe2)).unwrap();
        } else {
            store.initialize_database(database_id(0xe2)).unwrap();
        }
        let shared = Arc::clone(&store.shared);
        let ports = finish_source(store);
        if !legacy {
            let write = shared.database.begin_write().unwrap();
            write
                .open_table(META)
                .unwrap()
                .remove(crate::primary_admission_roots::key().unwrap())
                .unwrap();
            write.commit().unwrap();
        }
        assert!(ports.read_replication_primary_admission().is_err());
        assert!(
            ports
                .shared_ports()
                .read_replication_primary_admission()
                .is_err()
        );
        let read = shared.database.begin_read().unwrap();
        assert!(
            read.open_table(META)
                .unwrap()
                .get(crate::primary_admission_roots::key().unwrap())
                .unwrap()
                .is_none()
        );
    }
}

fn finish_source(store: RedbStore) -> crate::RedbOperationalPorts {
    let mut session = store.begin_structural_evidence(inputs()).unwrap();
    let structural_end = finish_structural(&mut session);
    let (catalog, historical_end) = session
        .validate_startup_catalog_history()
        .unwrap()
        .into_parts();
    let CatalogHistoryOutcome::Ready(catalog) = catalog else {
        panic!("valid fresh catalog");
    };
    let StructuralOpenOutcome::Clean(opened) =
        session.finish(structural_end, historical_end).unwrap()
    else {
        panic!("fresh structural proof");
    };
    let (id, session_id, _, dormant) = opened.into_parts();
    assert!(catalog.matches(id, session_id));
    dormant.into_operational_after_catalog_validation().unwrap()
}

#[test]
fn fresh_v2_initialization_crashes_leave_empty_or_complete_source_and_reopen() {
    use riffdb_storage_api::{DatabaseIdentityProbe, DatabaseIdentityProbePort};
    for edge in ["preflight", "roots", "receipt", "committed"] {
        let path = TestDatabasePath::new("v2-source-initialization-crash");
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "startup::tests::v2_primary::fresh_v2_initialization_process_child",
                "--nocapture",
            ])
            .env("RIFFDB_WP748_FRESH_V2_PATH", &path.0)
            .env("RIFFDB_WP772_ACTIVATION_CRASH", edge)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(91), "edge {edge}");
        let mut store = RedbStore::open(&path.0).unwrap();
        assert_eq!(
            store.probe_database_identity().unwrap(),
            if edge == "committed" {
                DatabaseIdentityProbe::Existing(database_id(0xe1))
            } else {
                DatabaseIdentityProbe::NeedsInitialization
            }
        );
        store.initialize_database(database_id(0xe1)).unwrap();
        let ports = finish_source(store);
        let read = ports.shared.database.begin_read().unwrap();
        let history = crate::changelog_v3_roots::validate_retained_history(&read)
            .unwrap()
            .unwrap();
        assert_eq!(
            history.lineage().catalog_digest(),
            AuthoritativeStateCatalogV2.digest()
        );
        assert_eq!(history.lineage().history_incarnation(), 1);
        drop(read);
        drop(ports);
        drop(finish_source(RedbStore::open(&path.0).unwrap()));
    }
}

#[test]
fn fresh_v2_initialization_process_child() {
    let Ok(path) = std::env::var("RIFFDB_WP748_FRESH_V2_PATH") else {
        return;
    };
    let mut store = RedbStore::open(path).unwrap();
    store.initialize_database(database_id(0xe1)).unwrap();
    panic!("armed initialization crash did not fire");
}

#[test]
fn source_reopen_refuses_erased_v2_admission_instead_of_recreating_active() {
    let path = TestDatabasePath::new("v2-source-missing-admission");
    let mut store = RedbStore::open(&path.0).unwrap();
    store.initialize_database(database_id(0xe1)).unwrap();
    let write = store.shared.database.begin_write().unwrap();
    write
        .open_table(META)
        .unwrap()
        .remove(crate::primary_admission_roots::key().unwrap())
        .unwrap();
    write.commit().unwrap();
    drop(store);
    let error = RedbStore::open(&path.0).unwrap_err();
    assert_eq!(error.kind(), StorageErrorKind::CorruptData);
    let raw = redb::Database::open(&path.0).unwrap();
    let read = raw.begin_read().unwrap();
    assert!(
        read.open_table(META)
            .unwrap()
            .get(crate::primary_admission_roots::key().unwrap())
            .unwrap()
            .is_none()
    );
}

// req: REC-001, REC-005, REC-006, REC-007, STO-032, END-008
#[test]
fn restored_incarnation_requires_full_proof_before_registry_completion_renewal() {
    use riffdb_storage_api::{decode_record_registry, decode_validated_prefix_checkpoint_v2};
    let path = TestDatabasePath::new("registry-completion-restored-incarnation");
    let id = database_id(0xe9);
    let mut store = RedbStore::open(&path.0).unwrap();
    store.initialize_database(id).unwrap();
    let ports = finish_source(store);
    ports.write_clean_close_lifecycle().unwrap();
    drop(ports);

    // The public restore owner advances the incarnation with its existing
    // atomic anchor. Copied registry/checkpoint evidence cannot certify it.
    crate::stamp_history_incarnation(&path.0, 2).unwrap();
    let store = RedbStore::open(&path.0).unwrap();
    let read = store.shared.database.begin_read().unwrap();
    let meta = read.open_table(META).unwrap();
    let registry =
        decode_record_registry(meta.get(META_RECORD_REGISTRY).unwrap().unwrap().value()).unwrap();
    assert!(!registry.value().completed_for(
        id,
        2,
        riffdb_storage_api::current_record_registry_digest()
    ));
    let retained = crate::startup::read_retained_metadata_pub(&read).unwrap();
    let mut walked = 0;
    let unsealed = crate::validated_prefix::build_checkpoint_from_snapshot(
        &read,
        &retained,
        crate::validated_prefix::CheckpointCountSource::DurableLengths {
            execution_failed_rows: 0,
        },
        &mut walked,
    )
    .unwrap();
    // An ordinary checkpoint plan cannot turn a copied certificate into
    // successor eligibility without the exact complete-validation owner.
    assert!(crate::validated_prefix::plan_checkpoint_receipt(&read, &unsealed).is_err());
    drop(meta);
    drop(read);
    let mut session = store.begin_structural_evidence(inputs()).unwrap();
    assert!(!session.bounded_dirty_startup_path());
    assert!(!session.clean_close_fast_path());
    let structural_end = finish_structural(&mut session);
    let (catalog, historical_end) = session
        .validate_startup_catalog_history()
        .unwrap()
        .into_parts();
    assert!(matches!(catalog, CatalogHistoryOutcome::Ready(_)));
    let StructuralOpenOutcome::Clean(opened) =
        session.finish(structural_end, historical_end).unwrap()
    else {
        panic!("complete successor structural proof");
    };
    let ports = opened
        .into_parts()
        .3
        .into_operational_after_catalog_validation()
        .unwrap();
    let read = ports.shared.database.begin_read().unwrap();
    let meta = read.open_table(META).unwrap();
    let registry =
        decode_record_registry(meta.get(META_RECORD_REGISTRY).unwrap().unwrap().value()).unwrap();
    assert!(registry.value().completed_for(
        id,
        2,
        riffdb_storage_api::current_record_registry_digest()
    ));
    let checkpoint = decode_validated_prefix_checkpoint_v2(
        meta.get(META_VALIDATED_PREFIX_CHECKPOINT)
            .unwrap()
            .unwrap()
            .value(),
    )
    .unwrap();
    assert_eq!(checkpoint.value().base().history_incarnation(), 2);
    assert_eq!(checkpoint.value().base().previous_checkpoint_hash(), None);
}

// req: REC-005, REC-006, REC-007, STO-030, STO-032, END-008
#[test]
fn registry_completion_requires_exact_end_and_survives_only_compatible_writes() {
    use riffdb_storage_api::{RecordRegistryValidation, decode_record_registry};
    let path = TestDatabasePath::new("registry-completion-proof-owner");
    let mut store = RedbStore::open(&path.0).unwrap();
    let id = database_id(0xe4);
    store.initialize_database(id).unwrap();
    let shared = Arc::clone(&store.shared);
    let registry = || {
        let read = shared.database.begin_read().unwrap();
        let meta = read.open_table(META).unwrap();
        *decode_record_registry(meta.get(META_RECORD_REGISTRY).unwrap().unwrap().value())
            .unwrap()
            .value()
    };
    assert_eq!(registry().validation(), RecordRegistryValidation::Pending);
    let mut session = store.begin_structural_evidence(inputs()).unwrap();
    let _structural = finish_structural(&mut session);
    assert_eq!(registry().validation(), RecordRegistryValidation::Pending);
    let validation = session.validate_startup_catalog_history().unwrap();
    assert!(matches!(
        validation.outcome(),
        CatalogHistoryOutcome::Ready(_)
    ));
    // Even both successful walks are insufficient until their owner's atomic
    // completion receipt commits. Cancellation drops the exclusive capability.
    drop(validation);
    drop(session);
    assert_eq!(registry().validation(), RecordRegistryValidation::Pending);
    drop(shared);

    let ports = finish_source(RedbStore::open(&path.0).unwrap());
    let read = ports.shared.database.begin_read().unwrap();
    let meta = read.open_table(META).unwrap();
    let bytes = meta
        .get(META_RECORD_REGISTRY)
        .unwrap()
        .unwrap()
        .value()
        .to_vec();
    let completed = *decode_record_registry(&bytes).unwrap().value();
    assert!(completed.completed_for(id, 1, riffdb_storage_api::current_record_registry_digest()));
    let receipts = read
        .open_table(crate::changelog_v3_activation::HISTORY)
        .unwrap();
    let completion = receipts
        .iter()
        .unwrap()
        .map(|row| {
            riffdb_storage_api::AuthoritativeTransactionV3::decode(row.unwrap().1.value()).unwrap()
        })
        .find(|receipt| {
            receipt.attribution()
                == riffdb_storage_api::ChangelogAttributionV3::ValidatedPrefixCheckpoint
        })
        .unwrap();
    assert!(
        completion
            .mutations()
            .iter()
            .any(|mutation| mutation.namespace()
                == riffdb_storage_api::AuthoritativeNamespaceV1::RecordRegistry
                && mutation.value() == Some(bytes.as_slice()))
    );
    assert!(
        completion
            .mutations()
            .iter()
            .any(|mutation| mutation.namespace()
                == riffdb_storage_api::AuthoritativeNamespaceV1::ValidatedPrefixCheckpoint)
    );
    drop(receipts);
    drop(meta);
    drop(read);
    ports.write_clean_close_lifecycle().unwrap();
    let read = ports.shared.database.begin_read().unwrap();
    assert_eq!(
        read.open_table(META)
            .unwrap()
            .get(META_RECORD_REGISTRY)
            .unwrap()
            .unwrap()
            .value(),
        bytes
    );
    drop(read);
    drop(ports);

    // A valid envelope copied from a different incarnation grants no bounded
    // eligibility when the optional prefix certificate is absent.
    {
        let database = redb::Database::open(&path.0).unwrap();
        let write = database.begin_write().unwrap();
        let stale = riffdb_storage_api::encode_record_registry_v3(
            riffdb_storage_api::StoredRecordRegistry::completed(
                riffdb_storage_api::current_record_registry_digest(),
                id,
                2,
            )
            .unwrap(),
        )
        .unwrap();
        {
            let mut meta = write.open_table(META).unwrap();
            meta.insert(META_RECORD_REGISTRY, stale.as_bytes()).unwrap();
            meta.remove(META_VALIDATED_PREFIX_CHECKPOINT).unwrap();
        }
        write.commit().unwrap();
    }
    let session = RedbStore::open(&path.0)
        .unwrap()
        .begin_structural_evidence(inputs())
        .unwrap();
    assert!(!session.bounded_dirty_startup_path());
    assert!(!session.clean_close_fast_path());
}

// req: REC-005, REC-006, REC-007, STO-032, END-008
#[test]
fn registry_completion_receipt_refusal_and_uncertainty_never_release_ports() {
    use crate::{RedbTestController, RedbTestOperation};
    for committed in [false, true] {
        let path = TestDatabasePath::new("registry-completion-uncertainty");
        let controller = if committed {
            RedbTestController::return_unknown_after_commit(
                RedbTestOperation::ValidatedPrefixCheckpoint,
            )
        } else {
            RedbTestController::return_before_commit(RedbTestOperation::ValidatedPrefixCheckpoint)
        };
        let mut store = RedbStore::open_with_test_controller(&path.0, controller).unwrap();
        let id = database_id(0xe5);
        store.initialize_database(id).unwrap();
        let shared = Arc::clone(&store.shared);
        let mut session = store.begin_structural_evidence(inputs()).unwrap();
        let structural = finish_structural(&mut session);
        let (_, historical) = session
            .validate_startup_catalog_history()
            .unwrap()
            .into_parts();
        let error = session.finish(structural, historical).unwrap_err();
        assert_eq!(
            error.kind(),
            if committed {
                StorageErrorKind::CommitStatusUnknown
            } else {
                StorageErrorKind::Unavailable
            }
        );
        let read = shared.database.begin_read().unwrap();
        let meta = read.open_table(META).unwrap();
        let registry = riffdb_storage_api::decode_record_registry(
            meta.get(META_RECORD_REGISTRY).unwrap().unwrap().value(),
        )
        .unwrap();
        assert_eq!(
            registry.value().completed_for(
                id,
                1,
                riffdb_storage_api::current_record_registry_digest()
            ),
            committed
        );
        assert_eq!(
            meta.get(META_VALIDATED_PREFIX_CHECKPOINT)
                .unwrap()
                .is_some(),
            committed
        );
        if committed {
            assert_eq!(
                RedbStore {
                    shared: Arc::clone(&shared)
                }
                .ensure_writable()
                .unwrap_err()
                .kind(),
                StorageErrorKind::Unavailable
            );
        }
    }
}

// req: REC-005, REC-006, REC-007, STO-032, END-008
#[test]
fn registry_completion_receipt_process_child() {
    let Some(path) = std::env::var_os("RIFFDB_REGISTRY_COMPLETION_CRASH_PATH") else {
        return;
    };
    let mut store = RedbStore::open(path).unwrap();
    if std::env::var_os("RIFFDB_REGISTRY_COMPLETION_RESTORED").is_none() {
        store.initialize_database(database_id(0xe6)).unwrap();
    }
    drop(finish_source(store));
    panic!("the completion receipt crash edge was not reached");
}

// req: REC-005, REC-006, REC-007, STO-032, END-008
#[test]
fn legacy_activation_completion_requires_native_catalog_semantics() {
    for native_proof in [false, true] {
        let path = TestDatabasePath::new("legacy-registry-completion-proof");
        let mut store = RedbStore::open(&path.0).unwrap();
        let id = database_id(0xe7);
        store.initialize_legacy_fixture(id).unwrap();
        drop(store);
        let store = RedbStore::open(&path.0).unwrap();
        let shared = Arc::clone(&store.shared);
        let mut session = store.begin_structural_evidence(inputs()).unwrap();
        let structural = finish_structural(&mut session);
        let validation = if native_proof {
            session.validate_startup_catalog_history().unwrap()
        } else {
            validate_catalog_history(&mut session).unwrap()
        };
        let (catalog, historical) = validation.into_parts();
        assert!(matches!(catalog, CatalogHistoryOutcome::Ready(_)));
        let StructuralOpenOutcome::Clean(opened) = session.finish(structural, historical).unwrap()
        else {
            panic!("legacy fixture must complete validation");
        };
        let (_, _, _, dormant) = opened.into_parts();
        let ports = dormant.into_operational_after_catalog_validation().unwrap();
        let read = shared.database.begin_read().unwrap();
        let meta = read.open_table(META).unwrap();
        let registry = riffdb_storage_api::decode_record_registry(
            meta.get(META_RECORD_REGISTRY).unwrap().unwrap().value(),
        )
        .unwrap();
        assert_eq!(
            registry.value().completed_for(
                id,
                1,
                riffdb_storage_api::current_record_registry_digest()
            ),
            native_proof
        );
        drop(meta);
        drop(read);
        drop(ports);
    }
}

// req: REC-005, REC-006, REC-007, STO-032, END-008
#[test]
fn registry_completion_process_crashes_preserve_pending_or_atomic_completed_checkpoint() {
    for edge in ["mutations", "receipt", "roots", "committed"] {
        let path = TestDatabasePath::new("registry-completion-crash");
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "startup::tests::v2_primary::registry_completion_receipt_process_child",
                "--nocapture",
            ])
            .env("RIFFDB_REGISTRY_COMPLETION_CRASH_PATH", &path.0)
            .env("RIFFDB_V3_DIRECT_EDGE", edge)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(93), "edge {edge}");
        let mut previous = None;
        for _ in 0..2 {
            let database = redb::Database::open(&path.0).unwrap();
            let read = database.begin_read().unwrap();
            let meta = read.open_table(META).unwrap();
            let registry_bytes = meta
                .get(META_RECORD_REGISTRY)
                .unwrap()
                .unwrap()
                .value()
                .to_vec();
            let registry = riffdb_storage_api::decode_record_registry(&registry_bytes).unwrap();
            let committed = edge == "committed";
            assert_eq!(
                registry.value().completed_for(
                    database_id(0xe6),
                    1,
                    riffdb_storage_api::current_record_registry_digest()
                ),
                committed
            );
            assert_eq!(
                meta.get(META_VALIDATED_PREFIX_CHECKPOINT)
                    .unwrap()
                    .is_some(),
                committed
            );
            let history = crate::changelog_v3_roots::validate_retained_history(&read)
                .unwrap()
                .unwrap();
            assert_eq!(
                history.tail().sequence().get(),
                if committed { 2 } else { 1 }
            );
            let state = (registry_bytes, history);
            if let Some(previous) = previous.as_ref() {
                assert_eq!(&state, previous);
            }
            previous = Some(state);
        }
        drop(finish_source(RedbStore::open(&path.0).unwrap()));
    }
}

// req: REC-001, REC-005, REC-006, REC-007, STO-032, END-008
#[test]
fn restored_completion_process_crashes_preserve_exact_old_or_atomic_successor_proofs() {
    for edge in ["mutations", "receipt", "roots", "committed"] {
        let path = TestDatabasePath::new("restored-registry-completion-crash");
        let id = database_id(0xe6);
        let mut store = RedbStore::open(&path.0).unwrap();
        store.initialize_database(id).unwrap();
        let ports = finish_source(store);
        ports.write_clean_close_lifecycle().unwrap();
        drop(ports);
        crate::stamp_history_incarnation(&path.0, 2).unwrap();
        let prior = {
            let database = redb::Database::open(&path.0).unwrap();
            let read = database.begin_read().unwrap();
            let meta = read.open_table(META).unwrap();
            (
                meta.get(META_RECORD_REGISTRY)
                    .unwrap()
                    .unwrap()
                    .value()
                    .to_vec(),
                meta.get(META_VALIDATED_PREFIX_CHECKPOINT)
                    .unwrap()
                    .unwrap()
                    .value()
                    .to_vec(),
                crate::changelog_v3_roots::validate_retained_history(&read)
                    .unwrap()
                    .unwrap(),
            )
        };
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "startup::tests::v2_primary::registry_completion_receipt_process_child",
                "--nocapture",
            ])
            .env("RIFFDB_REGISTRY_COMPLETION_CRASH_PATH", &path.0)
            .env("RIFFDB_REGISTRY_COMPLETION_RESTORED", "1")
            .env("RIFFDB_V3_DIRECT_EDGE", edge)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(93), "edge {edge}");
        {
            let database = redb::Database::open(&path.0).unwrap();
            let read = database.begin_read().unwrap();
            let meta = read.open_table(META).unwrap();
            let registry = meta.get(META_RECORD_REGISTRY).unwrap().unwrap();
            let checkpoint = meta.get(META_VALIDATED_PREFIX_CHECKPOINT).unwrap().unwrap();
            let history = crate::changelog_v3_roots::validate_retained_history(&read)
                .unwrap()
                .unwrap();
            if edge == "committed" {
                assert!(riffdb_storage_api::decode_record_registry(registry.value()).unwrap().value().completed_for(
                    id, 2, riffdb_storage_api::current_record_registry_digest(),
                ));
                let checkpoint =
                    riffdb_storage_api::decode_validated_prefix_checkpoint_v2(checkpoint.value())
                        .unwrap();
                assert_eq!(checkpoint.value().base().history_incarnation(), 2);
                assert_eq!(checkpoint.value().base().previous_checkpoint_hash(), None);
                assert_eq!(
                    history.tail().sequence().get(),
                    prior.2.tail().sequence().get() + 1
                );
            } else {
                assert_eq!(registry.value(), prior.0);
                assert_eq!(checkpoint.value(), prior.1);
                assert_eq!(history, prior.2);
            }
            assert_eq!(history.tail().frontier(), prior.2.tail().frontier());
        }
        drop(finish_source(RedbStore::open(&path.0).unwrap()));
    }
}

// req: PERF-019, REP-005, STO-012
#[test]
fn v2_clean_close_requires_complete_startup_and_keeps_lifecycle_bytes() {
    let path = TestDatabasePath::new("v2-clean-complete-startup");
    let mut store = RedbStore::open(&path.0).unwrap();
    store.initialize_database(database_id(0xe3)).unwrap();
    let ports = finish_source(store);
    ports.write_clean_close_lifecycle().unwrap();
    assert_eq!(ports.clean_close_write_failures(), 0);
    let read = ports.shared.database.begin_read().unwrap();
    let meta = read.open_table(META).unwrap();
    let encoded = meta
        .get(META_CLEAN_CLOSE_LIFECYCLE)
        .unwrap()
        .unwrap()
        .value()
        .to_vec();
    let lifecycle = crate::clean_close::CleanCloseLifecycle::decode(&encoded).unwrap();
    assert!(matches!(
        lifecycle.state(),
        crate::clean_close::CleanCloseState::Clean(_)
    ));
    assert_eq!(lifecycle.encode().unwrap(), encoded);
    drop(meta);
    drop(read);
    drop(ports);

    let store = RedbStore::open(&path.0).unwrap();
    let mut session = store.begin_structural_evidence(inputs()).unwrap();
    assert!(
        !session.clean_close_fast_path(),
        "CLEAN is insufficient for V2 authority"
    );
    assert_eq!(
        session.clean_close_declined_reason(),
        Some("bounded_roots_unavailable")
    );
    // ADR-0260: the declined certificate routes this V2 reopen to the
    // ADR-0182 bounded dirty path (its admission and any referenced fence are
    // point-validated at open), never to the clean-certificate fast path.
    assert!(session.bounded_dirty_startup_path());
    let structural_end = finish_structural(&mut session);
    let (catalog, historical_end) = validate_catalog_history(&mut session).unwrap().into_parts();
    assert!(matches!(catalog, CatalogHistoryOutcome::Ready(_)));
    let StructuralOpenOutcome::Clean(opened) =
        session.finish(structural_end, historical_end).unwrap()
    else {
        panic!("complete V2 validation must succeed");
    };
    let (_, _, _, dormant) = opened.into_parts();
    let ports = dormant.into_operational_after_catalog_validation().unwrap();
    // The ports-level `clean_close_fast_startup` flag now covers both bounded
    // paths (clean certificate OR ADR-0182 bounded dirty); the precise claim —
    // no clean CERTIFICATE was trusted for V2 authority — is the closed health
    // mode flag below.
    assert!(!ports.clean_certificate_startup());
    use riffdb_storage_api::ReplicationPrimaryAdmissionReadPort;
    assert!(
        ports
            .read_replication_primary_admission()
            .unwrap()
            .fence()
            .is_none()
    );
    let read = ports.shared.database.begin_read().unwrap();
    let meta = read.open_table(META).unwrap();
    let encoded = meta.get(META_CLEAN_CLOSE_LIFECYCLE).unwrap().unwrap();
    let consumed = crate::clean_close::CleanCloseLifecycle::decode(encoded.value()).unwrap();
    assert_eq!(consumed, lifecycle.successor_dirty().unwrap());
}
