// req: STO-030, STO-031, STO-032, REC-001, END-007, END-008
use super::*;
use crate::changelog_v3_retained_validation::RetainedHistoryValidation;
use crate::replication_registration_links::tests::{store_record, stored_registration};
use riffdb_storage_api::{
    AuditPrincipalV1, FollowerRegistrationPhaseV1, ReplicationAdministrationActionV1,
    ReplicationSourceHoldStateV1, ReplicationSourceHoldV2, ServiceAuditAppendIntentV1,
    ServiceAuditAppendRepository, ServiceAuditGroupAppend, StoredReplicationAdministrationV1,
};
use riffdb_types::{
    ActorId, ActorKind, CapabilityId, RequestId, ServiceAuditLinkV1, ServiceAuditPhaseV1,
    ServiceAuditTargetsV1, ServiceIngressKindV1, ServiceOperationV1, Timestamp,
};

fn epoch(ports: &RedbOperationalPorts) -> u64 {
    ports
        .shared
        .source_continuation_epoch
        .load(Ordering::Acquire)
}

fn audit_intent(ordinal: u64) -> ServiceAuditAppendIntentV1 {
    ServiceAuditAppendIntentV1::new(
        RequestId::from_unix_milliseconds_and_random(100 + ordinal, [0x74; 10]).unwrap(),
        Timestamp::new(1_700_000_000, 0).unwrap(),
        ServiceOperationV1::GetHealth,
        ServiceAuditPhaseV1::Denied,
        AuditPrincipalV1::new(
            ActorId::new("accounting-fixture").unwrap(),
            ActorKind::Human,
            CapabilityId::from_unix_milliseconds_and_random(100, [0x75; 10]).unwrap(),
            std::num::NonZeroU64::MIN,
        ),
        ServiceIngressKindV1::Grpc,
        ServiceAuditTargetsV1::empty(),
        None,
        ServiceAuditLinkV1::None,
    )
    .unwrap()
}

fn append_audit(ports: &mut RedbOperationalPorts, ordinal: u64) {
    let ServiceAuditGroupAppend::Submitted(fence) = ports
        .submit_service_audit_group(&[audit_intent(ordinal)])
        .unwrap()
    else {
        panic!("the native fixture must use an acknowledged journal publication");
    };
    assert_eq!(fence.wait().unwrap().len(), 1);
}

fn retire(
    ports: &RedbOperationalPorts,
    registration: &StoredReplicationAdministrationV1,
) -> StoredReplicationAdministrationV1 {
    let history = ports
        .shared
        .capture_source_validation_pin()
        .unwrap()
        .history()
        .unwrap()
        .unwrap();
    let prior = registration.after();
    let retired = ReplicationSourceHoldV2::new(
        prior.hold(),
        prior.registered_at(),
        prior.budget(),
        prior.expires_at(),
        FollowerRegistrationPhaseV1::Retired,
        prior.degraded_at(),
    )
    .unwrap();
    let release = StoredReplicationAdministrationV1::new(
        history
            .tail()
            .frontier()
            .administration()
            .unwrap()
            .checked_next()
            .unwrap(),
        registration.timestamp(),
        ReplicationAdministrationActionV1::RetireFollower,
        registration.target(),
        Some(ReplicationSourceHoldStateV1::Registered(prior)),
        retired,
        history.tail(),
        registration.origin().clone(),
    )
    .unwrap();
    store_record(ports, &release, history);
    release
}

#[test]
fn accounting_hold_validation_requires_exact_audit_end_before_borrowing_a_floor() {
    let (_scope, ports, registration) = stored_registration();
    let source = ports.shared.capture_source_validation_pin().unwrap();
    let history = source.history().unwrap().unwrap();
    let proof = RetainedHistoryValidation::new_source(source.clone())
        .unwrap()
        .unwrap();
    assert!(!proof.is_complete());
    let gate = ports.shared.mutation_gate.source_census().unwrap();
    let mut holds = AccountingHolds::capture(source, epoch(&ports)).unwrap();
    assert!(
        matches!(holds.validated(epoch(&ports)), Err(error) if error.kind() == StorageErrorKind::Unavailable)
    );
    assert!(holds.advance().unwrap());
    let floor = holds
        .validated(epoch(&ports))
        .unwrap()
        .floor(history.tail(), None)
        .unwrap();
    assert_eq!(
        floor[5],
        registration.after().hold().fence().sequence().get()
    );
    assert_eq!(floor[6], 1);
    assert!(!proof.is_complete());
    assert_eq!(proof.rows_read(), [0; 2]);
    assert_eq!(ports.shared.mutation_gate.source_census().unwrap(), gate);
}

#[test]
fn accounting_hold_validation_retired_policy_requires_its_genuine_release_evidence() {
    for erase_registration in [false, true] {
        let (_scope, ports, registration) = stored_registration();
        let release = retire(&ports, &registration);
        let source = ports.shared.capture_source_validation_pin().unwrap();
        let history = source.history().unwrap().unwrap();
        let original_epoch = epoch(&ports);
        let mut original = AccountingHolds::capture(source, original_epoch).unwrap();
        assert!(original.advance().unwrap());
        let floor = original
            .validated(original_epoch)
            .unwrap()
            .floor(history.tail(), None)
            .unwrap();
        assert_eq!(floor[5], history.tail().sequence().get());
        assert_eq!(floor[6], 0);

        // Deliberate media corruption after preserving an actual immutable pin.
        let erased = if erase_registration {
            &registration
        } else {
            &release
        };
        let write = ports.shared.database.begin_write().unwrap();
        write
            .open_table(crate::layout::AUDIT)
            .unwrap()
            .remove(crate::keys::encode_audit_key(erased.administration_sequence()).as_slice())
            .unwrap();
        ports.shared.commit_durable(write).unwrap();
        let current_epoch = epoch(&ports);
        assert_ne!(current_epoch, original_epoch);
        assert!(
            matches!(original.validated(current_epoch), Err(error) if error.kind() == StorageErrorKind::Unavailable)
        );
        assert_eq!(
            original
                .validated(original_epoch)
                .unwrap()
                .floor(history.tail(), None)
                .unwrap(),
            floor,
            "the old reader stays byte-exact but cannot qualify current accounting"
        );
        let damaged = ports.shared.capture_source_validation_pin().unwrap();
        let result =
            AccountingHolds::capture(damaged, current_epoch).and_then(|mut walk| walk.advance());
        assert!(matches!(result, Err(error) if error.kind() == StorageErrorKind::CorruptData));
    }
}

#[test]
fn accounting_hold_validation_rechecks_native_suffix_and_releases_obsolete_pins() {
    let (_scope, mut ports, registration) = stored_registration();
    let source = ports.shared.capture_source_validation_pin().unwrap();
    let original_epoch = epoch(&ports);
    let old_pin = Arc::downgrade(source.checkpoint_pin());
    let mut holds = AccountingHolds::capture(source, original_epoch).unwrap();
    assert!(holds.advance().unwrap());
    holds.release_completed_pin();
    assert!(
        matches!(holds.validated(original_epoch), Err(error) if error.kind() == StorageErrorKind::Unavailable)
    );
    append_audit(&mut ports, 0);
    let source = ports.shared.capture_source_validation_pin().unwrap();
    let accounting_pin = match &source {
        ValidationSource::Composite(view) => Arc::downgrade(view),
        _ => panic!("the actual native publication must retain its acknowledged composite"),
    };
    let owners_before = accounting_pin.strong_count();
    assert_eq!(epoch(&ports), original_epoch);
    let history = source.history().unwrap().unwrap();
    assert!(holds.continue_on(source, original_epoch).unwrap());
    assert!(
        !holds.complete,
        "appended audit input must be checked before borrowing the next floor"
    );
    assert!(holds.advance().unwrap());
    let floor = holds
        .validated(original_epoch)
        .unwrap()
        .floor(history.tail(), None)
        .unwrap();
    assert_eq!(
        floor[5],
        registration.after().hold().fence().sequence().get()
    );
    holds.release_completed_pin();
    assert!(holds.source.is_none());
    // The new publication owns its actual checkpoint pin; the completed
    // observer must release both its obsolete checkpoint and composite handle.
    assert!(old_pin.upgrade().is_none());
    assert_eq!(accounting_pin.strong_count() + 1, owners_before);
}

#[test]
fn accounting_hold_validation_refuses_changed_qualification_before_reuse() {
    let (_scope, ports, _) = stored_registration();
    let source = ports.shared.capture_source_validation_pin().unwrap();
    let original_epoch = epoch(&ports);
    let mut holds = AccountingHolds::capture(source, original_epoch).unwrap();
    assert!(holds.advance().unwrap());
    holds.release_completed_pin();
    ports.shared.invalidate_source_continuation();
    let current_epoch = epoch(&ports);
    assert_ne!(current_epoch, original_epoch);
    let source = ports.shared.capture_source_validation_pin().unwrap();
    assert!(!holds.continue_on(source, current_epoch).unwrap());
    assert!(
        matches!(holds.validated(current_epoch), Err(error) if error.kind() == StorageErrorKind::Unavailable)
    );
}

#[test]
fn accounting_hold_validation_pages_audit_input_without_claiming_partial_completion() {
    let (_scope, mut ports, _) = stored_registration();
    for ordinal in 0..260 {
        append_audit(&mut ports, ordinal);
    }
    let source = ports.shared.capture_source_validation_pin().unwrap();
    assert!(matches!(&source, ValidationSource::Composite(_)));
    let history = source.history().unwrap().unwrap();
    let mut holds = AccountingHolds::capture(source, epoch(&ports)).unwrap();
    assert!(!holds.advance().unwrap());
    assert!(!holds.complete);
    assert!(
        matches!(holds.validated(epoch(&ports)), Err(error) if error.kind() == StorageErrorKind::Unavailable)
    );
    assert!(holds.advance().unwrap());
    assert!(
        holds
            .validated(epoch(&ports))
            .unwrap()
            .floor(history.tail(), Some(1))
            .unwrap()[5]
            <= 1
    );
}

#[test]
fn source_accounting_refuses_changed_or_incomplete_floor_evidence() {
    for corrupt_hold in [false, true] {
        let (_scope, mut ports, registration) = stored_registration();
        for ordinal in 0..260 {
            append_audit(&mut ports, ordinal);
        }
        let mut control = ports.replication_source_control();
        control.tick_source_accounting().unwrap();
        control.advance_source_byte_observation_off_gate().unwrap();
        assert!(control.accounting.has_pin());
        assert!(
            !control
                .eligible_bytes
                .sample_available(std::time::Instant::now())
        );
        assert!(!control.eligible_bytes.pending());
        control.advance_source_byte_observation_off_gate().unwrap();
        assert!(!control.accounting.has_pin());
        assert!(
            !control
                .eligible_bytes
                .sample_available(std::time::Instant::now())
        );

        // The independent native actor installs acknowledged work. The census
        // itself neither requests a checkpoint nor enters this ordinary lease.
        let writer = ports.shared.mutation_gate.acquire().unwrap();
        ports
            .shared
            .checkpoint_published_journal_suffix_for_barrier()
            .unwrap();
        drop(writer);
        let gate = ports.shared.mutation_gate.source_census().unwrap();
        control.tick_source_accounting().unwrap();
        control.advance_source_byte_observation_off_gate().unwrap();
        assert!(control.eligible_bytes.pending());

        if corrupt_hold {
            // A valid policy envelope under the wrong physical hold key cannot
            // become a default floor, even after a previously valid capture.
            let write = ports.shared.database.begin_write().unwrap();
            {
                let mut holds = write.open_table(SOURCE_HOLDS).unwrap();
                let bytes = holds
                    .get(registration.after().hold().storage_key().as_slice())
                    .unwrap()
                    .unwrap()
                    .value()
                    .to_vec();
                holds
                    .insert(b"invalid-accounting-hold".as_slice(), bytes.as_slice())
                    .unwrap();
            }
            ports.shared.commit_durable(write).unwrap();
        } else {
            ports.shared.invalidate_source_continuation();
        }
        control.advance_source_byte_observation_off_gate().unwrap();
        assert!(!control.eligible_bytes.pending());
        assert!(
            !control
                .eligible_bytes
                .sample_available(std::time::Instant::now())
        );
        assert!(!control.eligible_bytes.pending());
        if corrupt_hold {
            // The unchanged capture cadence remains closed after revocation;
            // a scheduled tick emits nothing and cannot recapture early.
            control.tick_source_accounting().unwrap();
            assert!(!control.accounting.has_pin());
            // The actual capture factory still refuses this floor when a new
            // capture is admitted. It cannot turn the valid envelope into zero.
            let source = ports.shared.capture_source_validation_pin().unwrap();
            let result = control.accounting.capture(
                source,
                &ports.shared,
                epoch(&ports),
                std::time::Instant::now(),
            );
            assert!(matches!(result, Err(error) if error.kind() == StorageErrorKind::CorruptData));
            assert!(
                !control
                    .eligible_bytes
                    .sample_available(std::time::Instant::now())
            );
        }
        assert!(control.observed.is_none());
        assert!(control.online_license.is_none());
        assert_eq!(control.online_independent_sequence, 0);
        assert_eq!(ports.shared.mutation_gate.source_census().unwrap(), gate);
    }
}

#[test]
fn source_accounting_keeps_exact_legacy_inactivity_unknown_without_spinning() {
    let scope = crate::test_path::ScopedDirectory::new("legacy-source-accounting");
    let mut store = crate::RedbStore::open(scope.join("db.redb")).unwrap();
    let database =
        riffdb_types::DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x76; 10])
            .unwrap();
    store.initialize_legacy_fixture(database).unwrap();
    let ports = RedbOperationalPorts {
        shared: Arc::clone(&store.shared),
    };
    assert!(
        ports
            .shared
            .capture_source_validation_pin()
            .unwrap()
            .history()
            .unwrap()
            .is_none()
    );
    let mut control = ports.replication_source_control();
    assert!(control.source_accounting_ready().unwrap());
    control.tick_source_accounting().unwrap();
    assert!(!control.source_accounting_ready().unwrap());
    assert!(!control.accounting.has_pin());
    assert!(
        !control
            .eligible_bytes
            .sample_available(std::time::Instant::now())
    );
    assert!(!control.eligible_bytes.pending());
    assert!(control.online_license.is_none());
    assert!(control.observed.is_none());
}
