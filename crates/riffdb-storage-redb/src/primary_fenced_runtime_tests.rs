// Real source storage and complete startup; policy authorization is tested separately.
// req: REP-005, REC-001, STO-012
use super::*;
use riffdb_storage_api::{
    ChangelogHistoryStateV3 as History, FollowerHoldBudget, PrimaryFenceRequestV1,
    ReadableCapabilityDigestInventory, ReadableDigestKey, ReadableIdempotencyDigestInventory,
    ReplicationAdministrationAwaitingDecision, ReplicationAdministrationCandidateTransaction,
    ReplicationAdministrationCandidateV1, ReplicationAdministrationIntentV1,
    ReplicationAdministrationRequestV1, ReplicationAdministrationResultV1,
    ReplicationAdministrationTransactionPort, ReplicationPrimaryAdmissionReadPort,
    ReplicationSourceHoldKindV1 as Kind, ReplicationSourceHoldV1 as Hold, StartupValidationInputs,
    StoredPrimaryFenceAdministrationV1,
};
use riffdb_types::{
    ReplicationFenceOperationId, ReplicationFollowerAuditTargetV1, ReplicationSourceHoldIdV1,
};

fn inputs() -> StartupValidationInputs {
    let key = ReadableDigestKey::v1(DigestKeyId::new(1).unwrap());
    StartupValidationInputs::new(
        Timestamp::new(20, 0).unwrap(),
        ReadableCapabilityDigestInventory::new(vec![key]).unwrap(),
        ReadableIdempotencyDigestInventory::new(vec![key]).unwrap(),
    )
}

fn history(ports: &RedbOperationalPorts) -> History {
    ports
        .published_changelog_snapshot_v3()
        .unwrap()
        .authoritative_state_v3()
        .unwrap()
        .history()
}

fn submit_required_audit(ports: &mut RedbOperationalPorts, request: u8) {
    let before = history(ports);
    let result = match ports
        .submit_service_audit_group(&[denied_audit(request)])
        .unwrap()
    {
        riffdb_storage_api::ServiceAuditGroupAppend::Complete(result) => result,
        riffdb_storage_api::ServiceAuditGroupAppend::Submitted(fence) => {
            assert_eq!(history(ports), before, "unpublished audit remains private");
            fence.wait().unwrap()
        }
    };
    assert!(matches!(
        result.as_slice(),
        [ServiceAuditAppendResult::Appended(_)]
    ));
    assert_eq!(
        history(ports).tail().frontier().application(),
        before.tail().frontier().application()
    );
    assert!(
        history(ports).tail().frontier().administration()
            > before.tail().frontier().administration()
    );
}

fn attached_source(
    profile: crate::RedbCommitProfile,
) -> (TestPath, RedbOperationalPorts, PrimaryFenceRequestV1) {
    let path = TestPath::new("primary-fenced-runtime");
    let mut store = RedbStore::open_with_commit_profile(&path.0, profile).unwrap();
    store.initialize_database(database_id()).unwrap();
    drop(store);
    let mut ports = crate::startup::open_validated_source_fixture(&path.0, profile, inputs());
    ports
        .bootstrap_capability(&bootstrap_intent(20, capability_id(3), digest(1), 10))
        .unwrap();
    let source = history(&ports).lineage();
    let target = ReplicationFollowerAuditTargetV1::new(
        source.database_id(),
        source.history_incarnation(),
        source.leadership_epoch(),
        ReplicationSourceHoldIdV1::new([0x79; 16]).unwrap(),
    )
    .unwrap();
    let request = ReplicationAdministrationRequestV1::register(
        request_id(30),
        target,
        FollowerHoldBudget::new(100).unwrap(),
        None,
    );
    let candidate = ReplicationAdministrationCandidateV1::new(request, principal(capability_id(3)));
    let (awaiting, _) = ports
        .begin_replication_administration(candidate.clone())
        .unwrap()
        .read_transaction_current()
        .unwrap();
    let ReplicationAdministrationResultV1::Applied(registration) = awaiting
        .commit(ReplicationAdministrationIntentV1::new(
            candidate,
            Timestamp::new(15, 0).unwrap(),
        ))
        .unwrap()
    else {
        panic!("new registration");
    };
    let cut = history(&ports);
    let bootstrap = Hold::new(target.hold_id(), Kind::Bootstrap, source, cut.tail());
    let mut control = ports.replication_source_control();
    control.register(bootstrap).unwrap();
    control.attach_bootstrap(bootstrap).unwrap();
    drop(control);
    let request = PrimaryFenceRequestV1::new(
        request_id(31),
        ReplicationFenceOperationId::from_bytes(uuid_bytes(32)).unwrap(),
        target,
        registration.generation(),
    );
    (path, ports, request)
}

fn fenced_source(
    profile: crate::RedbCommitProfile,
) -> (TestPath, StoredPrimaryFenceAdministrationV1) {
    let (path, ports, request) = attached_source(profile);
    // Attachment drained the journal under the real barrier. Close all handles
    // before the isolated physical fence. This fixture does not claim
    // coordinator authorization or live post-fence publication.
    drop(ports);
    let store = RedbStore::open_with_commit_profile(&path.0, profile).unwrap();
    let fence = crate::primary_fence_write::commit_fixture_fence(
        &store.shared.database,
        request,
        principal(capability_id(3)),
        Timestamp::new(16, 0).unwrap(),
    )
    .unwrap();
    drop(store);
    (path, fence)
}

mod fence_control {
    include!("primary_fence_control_tests.rs");
}

#[test]
fn fenced_source_reopen_keeps_audit_and_acknowledgement_without_new_authority() {
    for profile in [
        crate::RedbCommitProfile::Standard,
        crate::RedbCommitProfile::Hardened,
    ] {
        let (path, fence) = fenced_source(profile);
        let mut ports = crate::startup::open_validated_source_fixture(&path.0, profile, inputs());
        assert_eq!(
            ports.read_replication_primary_admission().unwrap().fence(),
            Some(&fence)
        );
        let before = history(&ports);
        assert!(matches!(
            ports
                .append_service_audit_group(&[denied_audit(40)])
                .unwrap()
                .as_slice(),
            [ServiceAuditAppendResult::Appended(_)]
        ));
        submit_required_audit(&mut ports, 41);
        let audited = history(&ports);
        assert_eq!(
            audited.tail().frontier().application(),
            fence.final_application_head()
        );
        assert!(
            audited.tail().frontier().administration() > before.tail().frontier().administration()
        );
        let ack = Hold::new(
            fence.target().hold_id(),
            Kind::FollowerAcknowledgement,
            fence.lineage(),
            audited.tail(),
        );
        assert!(
            ports
                .replication_source_control()
                .advance_acknowledgement(ack)
                .unwrap()
        );
        let drained = history(&ports);
        assert_eq!(drained.tail().frontier(), audited.tail().frontier());
        assert!(drained.tail().sequence() > audited.tail().sequence());
        assert_eq!(
            ports
                .activate_catalog(&catalog_intent(None, bundle("refused", 1, 0x74), 42))
                .unwrap_err()
                .kind(),
            StorageErrorKind::Unavailable,
        );
        assert_eq!(history(&ports), drained);
        assert_eq!(
            ports.read_replication_primary_admission().unwrap().fence(),
            Some(&fence)
        );
        drop(ports);
        let reopened = crate::startup::open_validated_source_fixture(&path.0, profile, inputs());
        assert_eq!(
            history(&reopened).tail().frontier(),
            drained.tail().frontier()
        );
        assert_eq!(
            reopened
                .read_replication_primary_admission()
                .unwrap()
                .fence(),
            Some(&fence)
        );
    }
}

#[test]
fn fenced_source_required_audit_survives_process_exit_without_clearing_fence() {
    for (profile, name) in [
        (crate::RedbCommitProfile::Standard, "standard"),
        (crate::RedbCommitProfile::Hardened, "hardened"),
    ] {
        let (path, fence) = fenced_source(profile);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "administration::tests::fenced_runtime::fenced_source_audit_process_child",
                "--nocapture",
            ])
            .env("RIFFDB_WP748_FENCED_AUDIT_PATH", &path.0)
            .env("RIFFDB_WP748_FENCED_AUDIT_PROFILE", name)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(92));
        let mut reopened =
            crate::startup::open_validated_source_fixture(&path.0, profile, inputs());
        assert_eq!(
            reopened
                .read_replication_primary_admission()
                .unwrap()
                .fence(),
            Some(&fence)
        );
        let recovered = history(&reopened);
        assert_eq!(
            recovered.tail().frontier().application(),
            fence.final_application_head()
        );
        assert_eq!(
            recovered.tail().frontier().administration(),
            fence.administration_sequence().checked_next()
        );
        assert!(matches!(
            reopened
                .append_service_audit_group(&[denied_audit(50)])
                .unwrap()
                .as_slice(),
            [ServiceAuditAppendResult::PhaseConflict]
        ));
        assert_eq!(history(&reopened), recovered);
    }
}

#[test]
fn fenced_source_audit_process_child() {
    let Some(path) = std::env::var_os("RIFFDB_WP748_FENCED_AUDIT_PATH") else {
        return;
    };
    let profile = match std::env::var("RIFFDB_WP748_FENCED_AUDIT_PROFILE")
        .unwrap()
        .as_str()
    {
        "standard" => crate::RedbCommitProfile::Standard,
        "hardened" => crate::RedbCommitProfile::Hardened,
        _ => panic!("unknown fixture profile"),
    };
    let mut ports = crate::startup::open_validated_source_fixture(
        std::path::Path::new(&path),
        profile,
        inputs(),
    );
    submit_required_audit(&mut ports, 50);
    // No Rust drops or graceful clean lifecycle. The acknowledged audit and
    // original primary fence must both survive the production recovery path.
    std::process::exit(92);
}

// req: PERF-019, REP-005, STO-012
#[test]
fn fenced_v2_clean_reopen_validates_retained_admission_without_repair() {
    use riffdb_storage_api::{
        ReplicationPrimaryAdmissionV1, StructuralEvidenceOpen,
        proto_codec::encode_replication_primary_admission_v1,
    };
    for profile in [
        crate::RedbCommitProfile::Standard,
        crate::RedbCommitProfile::Hardened,
    ] {
        for corruption in ["intact", "missing", "stale_active", "foreign_lineage"] {
            let (path, fence) = fenced_source(profile);
            let ports = crate::startup::open_validated_source_fixture(&path.0, profile, inputs());
            ports.write_clean_close_lifecycle().unwrap();
            assert_eq!(ports.clean_close_write_failures(), 0);
            let read = ports.shared.database.begin_read().unwrap();
            let clean = read
                .open_table(META)
                .unwrap()
                .get(crate::layout::META_CLEAN_CLOSE_LIFECYCLE)
                .unwrap()
                .unwrap()
                .value()
                .to_vec();
            assert!(matches!(
                crate::clean_close::CleanCloseLifecycle::decode(&clean)
                    .unwrap()
                    .state(),
                crate::clean_close::CleanCloseState::Clean(_)
            ));
            drop(read);
            drop(ports);
            let key = crate::primary_admission_roots::key().unwrap();
            let raw = redb::Database::open(&path.0).unwrap();
            let write = raw.begin_write().unwrap();
            let expected = {
                let mut meta = write.open_table(META).unwrap();
                match corruption {
                    "missing" => {
                        meta.remove(key).unwrap();
                    }
                    "stale_active" | "foreign_lineage" => {
                        let lineage = if corruption == "stale_active" {
                            fence.lineage()
                        } else {
                            riffdb_storage_api::ChangelogLineageV3::new_with_catalog(
                                fence.lineage().database_id(),
                                fence.lineage().history_incarnation() + 1,
                                fence.lineage().leadership_epoch(),
                                fence.lineage().catalog_digest(),
                            )
                            .unwrap()
                        };
                        let active = ReplicationPrimaryAdmissionV1::active(lineage).unwrap();
                        meta.insert(
                            key,
                            encode_replication_primary_admission_v1(&active)
                                .unwrap()
                                .as_bytes(),
                        )
                        .unwrap();
                    }
                    "intact" => {}
                    _ => unreachable!(),
                }
                meta.get(key).unwrap().map(|row| row.value().to_vec())
            };
            write.commit().unwrap();
            drop(raw);
            if corruption == "intact" {
                let ports =
                    crate::startup::open_validated_source_fixture(&path.0, profile, inputs());
                assert!(!ports.clean_close_fast_startup());
                assert_eq!(
                    ports.read_replication_primary_admission().unwrap().fence(),
                    Some(&fence)
                );
                continue;
            }
            let error = match RedbStore::open_with_commit_profile(&path.0, profile) {
                Err(error) => error,
                Ok(store) => match store.begin_structural_evidence(inputs()) {
                    Err(error) => error,
                    Ok(_) => panic!("{corruption} must not grant source startup"),
                },
            };
            assert_eq!(error.kind(), StorageErrorKind::CorruptData, "{corruption}");
            let raw = redb::Database::open(&path.0).unwrap();
            let read = raw.begin_read().unwrap();
            let meta = read.open_table(META).unwrap();
            assert_eq!(
                meta.get(key).unwrap().map(|row| row.value().to_vec()),
                expected,
                "no admission repair"
            );
            assert_eq!(
                meta.get(crate::layout::META_CLEAN_CLOSE_LIFECYCLE)
                    .unwrap()
                    .unwrap()
                    .value(),
                clean,
                "refused startup must not consume CLEAN"
            );
        }
    }
}
