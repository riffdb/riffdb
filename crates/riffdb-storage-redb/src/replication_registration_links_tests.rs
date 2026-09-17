// req: REP-006, STO-012
use redb::{ReadableDatabase, ReadableTable};
use riffdb_storage_api::{
    AuditPrincipalV1, ChangelogHistoryPointV3 as Point, ChangelogHistoryStateV3 as History,
    ChangelogLineageV3 as Lineage, ChangelogTransactionSequence as Sequence, FollowerHoldBudget,
    FollowerRegistrationPhaseV1 as Phase, ReplicationAdministrationActionV1 as Action,
    ReplicationAdministrationOriginV1 as Origin, ReplicationSourceHoldKindV1 as Kind,
    ReplicationSourceHoldStateV1 as State, ReplicationSourceHoldV1 as Hold,
    ReplicationSourceHoldV2 as Policy, StoredReplicationAdministrationV1 as Record,
    proto_codec::{encode_replication_administration_v1, encode_replication_source_hold_v2},
};
use riffdb_types::{
    ActorId, ActorKind, AdministrationSequence, CapabilityId, CommitSequence, DatabaseId,
    DualFrontier, LeadershipEpochV1, ReplicationFollowerAuditTargetV1 as Target,
    ReplicationSourceHoldIdV1, RequestId, Timestamp,
};
use std::num::NonZeroU64;

#[path = "replication_policy_ack_tests.rs"]
mod acknowledgements;
#[path = "replication_policy_bootstrap_tests.rs"]
mod bootstrap;

fn point(physical: u64, app: u64, admin: u64) -> Point {
    Point::new(
        Sequence::new(physical).unwrap(),
        [physical as u8; 32],
        DualFrontier::new(CommitSequence::new(app), AdministrationSequence::new(admin)),
    )
}
fn lineage() -> Lineage {
    Lineage::new(
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [1; 10]).unwrap(),
        1,
        LeadershipEpochV1::initial(),
    )
    .unwrap()
}
fn policy(fence: Point, phase: Phase, degraded: Option<Point>) -> Policy {
    Policy::new(
        Hold::new(
            ReplicationSourceHoldIdV1::new([1; 16]).unwrap(),
            Kind::FollowerAcknowledgement,
            lineage(),
            fence,
        ),
        point(8, 3, 2),
        FollowerHoldBudget::new(2).unwrap(),
        CommitSequence::new(9),
        phase,
        degraded,
    )
    .unwrap()
}
fn explicit() -> Origin {
    Origin::Explicit {
        request_id: RequestId::from_unix_milliseconds_and_random(1_700_000_000_000, [2; 10])
            .unwrap(),
        principal: AuditPrincipalV1::new(
            ActorId::new("operator").unwrap(),
            ActorKind::Human,
            CapabilityId::from_unix_milliseconds_and_random(1_700_000_000_000, [3; 10]).unwrap(),
            NonZeroU64::new(1).unwrap(),
        ),
        approval_id: None,
    }
}
fn record(action: Action, before: Option<State>, after: Policy, observed: Point) -> Record {
    let hold = after.hold();
    Record::new(
        AdministrationSequence::new(observed.frontier().administration().unwrap().get() + 1)
            .unwrap(),
        Timestamp::new(1_700_000_000, 0).unwrap(),
        action,
        Target::new(
            hold.lineage().database_id(),
            hold.lineage().history_incarnation(),
            hold.lineage().leadership_epoch(),
            hold.id(),
        )
        .unwrap(),
        before,
        after,
        observed,
        if action == Action::ExpireFollower {
            Origin::ConfiguredExpiry {
                registration: AdministrationSequence::new(3).unwrap(),
            }
        } else {
            explicit()
        },
    )
    .unwrap()
}
fn registration() -> Record {
    record(
        Action::RegisterFollower,
        None,
        policy(point(8, 3, 2), Phase::AwaitingBootstrap, None),
        point(8, 3, 2),
    )
}

fn check(
    current: Option<Policy>,
    records: &[Record],
) -> Result<(), riffdb_storage_api::StorageError> {
    check_in_lineage(current, records, lineage())
}

fn check_in_lineage(
    current: Option<Policy>,
    records: &[Record],
    history_lineage: Lineage,
) -> Result<(), riffdb_storage_api::StorageError> {
    let scope = crate::test_path::ScopedDirectory::new("registration-links");
    let database = redb::Database::create(scope.join("db.redb")).unwrap();
    let write = database.begin_write().unwrap();
    {
        let mut holds = write
            .open_table(crate::changelog_v3_activation::SOURCE_HOLDS)
            .unwrap();
        if let Some(current) = current {
            let bytes = encode_replication_source_hold_v2(current).unwrap();
            holds
                .insert(current.hold().storage_key().as_slice(), bytes.as_bytes())
                .unwrap();
        }
        let mut audit = write.open_table(crate::layout::AUDIT).unwrap();
        write
            .open_table(crate::changelog_v3_activation::HISTORY)
            .unwrap();
        for record in records {
            let bytes = encode_replication_administration_v1(record).unwrap();
            audit
                .insert(
                    crate::keys::encode_audit_key(record.administration_sequence()).as_slice(),
                    bytes.as_bytes(),
                )
                .unwrap();
        }
    }
    write.commit().unwrap();
    let pin = database.begin_read().unwrap();
    let holds = pin
        .open_table(crate::changelog_v3_activation::SOURCE_HOLDS)
        .unwrap();
    let audit = pin.open_table(crate::layout::AUDIT).unwrap();
    let receipts = pin
        .open_table(crate::changelog_v3_activation::HISTORY)
        .unwrap();
    let history = History::new(
        history_lineage,
        point(1, 0, 0),
        point(20, 12, 8),
        point(16, 11, 7),
    )
    .unwrap();
    super::validate(&holds, &audit, history, &receipts)
}

#[test]
fn current_policy_requires_exact_original_registration() {
    let registration = registration();
    let current = policy(point(16, 11, 7), Phase::Attached, None);
    assert!(check(Some(current), std::slice::from_ref(&registration)).is_ok());
    assert!(check(Some(current), &[]).is_err(), "missing registration");
    assert!(
        check(None, &[registration]).is_err(),
        "erased hold/tombstone"
    );
    let substituted = Policy::new(
        current.hold(),
        current.registered_at(),
        FollowerHoldBudget::new(3).unwrap(),
        current.expires_at(),
        current.phase(),
        current.degraded_at(),
    )
    .unwrap();
    assert!(
        check(Some(substituted), &[self::registration()]).is_err(),
        "substituted budget"
    );
}

#[test]
fn retired_policy_requires_exact_release_and_cannot_be_resurrected() {
    let current = policy(point(12, 8, 4), Phase::Attached, None);
    let retired = policy(current.hold().fence(), Phase::Retired, None);
    let release = record(
        Action::RetireFollower,
        Some(State::Registered(current)),
        retired,
        point(14, 9, 5),
    );
    assert!(check(Some(retired), &[registration(), release.clone()]).is_ok());
    assert!(
        check(Some(retired), &[registration()]).is_err(),
        "a retired bit is not release authority"
    );
    assert!(
        check(Some(current), &[registration(), release]).is_err(),
        "resurrection"
    );
}

#[test]
fn configured_expiry_keeps_the_original_registration_and_degradation_evidence() {
    let degraded = point(13, 9, 4);
    let current = policy(point(12, 8, 4), Phase::Attached, Some(degraded));
    let retired = policy(current.hold().fence(), Phase::Retired, Some(degraded));
    let expiry = record(
        Action::ExpireFollower,
        Some(State::Registered(current)),
        retired,
        point(14, 9, 5),
    );
    assert!(check(Some(retired), &[registration(), expiry.clone()]).is_ok());
    assert!(check(Some(retired), &[expiry]).is_err());
    assert!(
        check(
            Some(policy(current.hold().fence(), Phase::Retired, None)),
            &[
                registration(),
                record(
                    Action::ExpireFollower,
                    Some(State::Registered(current)),
                    retired,
                    point(14, 9, 5)
                )
            ]
        )
        .is_err(),
        "erased degradation"
    );
}

fn stored_registration() -> (
    crate::test_path::ScopedDirectory,
    crate::RedbOperationalPorts,
    Record,
) {
    let (scope, ports, states) = crate::changelog_v3_control_tests::fixture();
    let history = *states.last().unwrap();
    let hold = Hold::new(
        ReplicationSourceHoldIdV1::new([0x41; 16]).unwrap(),
        Kind::FollowerAcknowledgement,
        history.lineage(),
        history.tail(),
    );
    let policy = Policy::new(
        hold,
        history.tail(),
        FollowerHoldBudget::new(2).unwrap(),
        CommitSequence::new(9),
        Phase::AwaitingBootstrap,
        None,
    )
    .unwrap();
    let record = Record::new(
        AdministrationSequence::first(),
        Timestamp::new(1_700_000_000, 0).unwrap(),
        Action::RegisterFollower,
        Target::new(
            hold.lineage().database_id(),
            hold.lineage().history_incarnation(),
            hold.lineage().leadership_epoch(),
            hold.id(),
        )
        .unwrap(),
        None,
        policy,
        history.tail(),
        explicit(),
    )
    .unwrap();
    store_record(&ports, &record, history);
    (scope, ports, record)
}

fn store_record(ports: &crate::RedbOperationalPorts, record: &Record, history: History) {
    use riffdb_storage_api::{
        AdministrationSequenceAllocator, AuthoritativeMutationV3 as Mutation,
        AuthoritativeNamespaceV1 as N, AuthoritativeTransactionBindingV3,
        AuthoritativeTransactionV3, ChangelogAttributionV3,
        proto_codec::encode_administration_sequence_allocator_v1,
    };
    let policy = record.after();
    let hold = policy.hold();
    let audit = encode_replication_administration_v1(record).unwrap();
    let hold_bytes = encode_replication_source_hold_v2(policy).unwrap();
    let allocator =
        encode_administration_sequence_allocator_v1(AdministrationSequenceAllocator::Next(
            record.administration_sequence().checked_next().unwrap(),
        ))
        .unwrap();
    let write = ports.shared.database.begin_write().unwrap();
    let old_allocator = write
        .open_table(crate::layout::META)
        .unwrap()
        .get(crate::layout::META_ADMINISTRATION_SEQUENCE)
        .unwrap()
        .unwrap()
        .value()
        .to_vec();
    let mut mutations = vec![
        Mutation::put(
            N::Audit,
            &crate::keys::encode_audit_key(record.administration_sequence()),
            None,
            audit.as_bytes(),
        )
        .unwrap(),
        Mutation::replace(
            N::NextAdministrationSequence,
            crate::layout::META_ADMINISTRATION_SEQUENCE.as_bytes(),
            &old_allocator,
            allocator.as_bytes(),
        )
        .unwrap(),
    ];
    mutations.sort_by(|left, right| {
        (left.namespace(), left.key()).cmp(&(right.namespace(), right.key()))
    });
    let receipt = AuthoritativeTransactionV3::new(
        AuthoritativeTransactionBindingV3 {
            database_id: history.lineage().database_id(),
            history_incarnation: history.lineage().history_incarnation(),
            predecessor: Some(history.tail().sequence()),
            sequence: history.tail().sequence().checked_next().unwrap(),
            predecessor_frontier: history.tail().frontier(),
            covered_frontier: DualFrontier::new(
                history.tail().frontier().application(),
                Some(record.administration_sequence()),
            ),
            prior_history_hash: history.tail().history_hash(),
        },
        ChangelogAttributionV3::RetentionHold,
        mutations,
    )
    .unwrap();
    let prepared =
        crate::changelog_v3_write::PreparedHistoryAdvance::prepare(&write, &receipt).unwrap();
    write
        .open_table(crate::layout::AUDIT)
        .unwrap()
        .insert(
            crate::keys::encode_audit_key(record.administration_sequence()).as_slice(),
            audit.as_bytes(),
        )
        .unwrap();
    write
        .open_table(crate::layout::META)
        .unwrap()
        .insert(
            crate::layout::META_ADMINISTRATION_SEQUENCE,
            allocator.as_bytes(),
        )
        .unwrap();
    write
        .open_table(crate::changelog_v3_activation::SOURCE_HOLDS)
        .unwrap()
        .insert(hold.storage_key().as_slice(), hold_bytes.as_bytes())
        .unwrap();
    prepared.stage(&write).unwrap();
    crate::changelog_v3_roots::validate_retained_history_for_write(&write).unwrap();
    ports.shared.commit_durable(write).unwrap();
}

#[test]
fn complete_source_validation_uses_one_pin_and_refuses_erased_registration_evidence() {
    use riffdb_storage_api::StoredAdministrationAuditRecordV1;
    let (_scope, ports, record) = stored_registration();
    let encoded = encode_replication_administration_v1(&record).unwrap();
    let decoded = crate::codec::decode_administration_audit_record_v1(encoded.as_bytes()).unwrap();
    assert!(matches!(
        decoded.value(),
        StoredAdministrationAuditRecordV1::Replication(decoded) if **decoded == record
    ));
    assert_eq!(
        crate::codec::encode_administration_audit_record_v1(decoded.value())
            .unwrap()
            .as_bytes(),
        encoded.as_bytes()
    );
    let old_pin = ports.shared.database.begin_read().unwrap();
    let before = crate::changelog_v3_roots::validate_retained_history(&old_pin).unwrap();
    assert!(before.is_some());
    let write = ports.shared.database.begin_write().unwrap();
    write
        .open_table(crate::layout::AUDIT)
        .unwrap()
        .remove(crate::keys::encode_audit_key(record.administration_sequence()).as_slice())
        .unwrap();
    assert!(crate::changelog_v3_roots::validate_retained_history_for_write(&write).is_err());
    // Deliberate media-fault fixture; production has no raw mutation endpoint.
    ports.shared.commit_durable(write).unwrap();
    assert_eq!(
        crate::changelog_v3_roots::validate_retained_history(&old_pin).unwrap(),
        before
    );
    let fresh_pin = ports.shared.database.begin_read().unwrap();
    assert!(
        fresh_pin
            .open_table(crate::layout::AUDIT)
            .unwrap()
            .get(crate::keys::encode_audit_key(record.administration_sequence()).as_slice())
            .unwrap()
            .is_none()
    );
    assert!(crate::changelog_v3_roots::validate_retained_history(&fresh_pin).is_err());
}

#[test]
fn complete_source_validation_refuses_a_substituted_control_record_or_erased_hold() {
    for erase_hold in [false, true] {
        let (_scope, ports, record) = stored_registration();
        let write = ports.shared.database.begin_write().unwrap();
        if erase_hold {
            write
                .open_table(crate::changelog_v3_activation::SOURCE_HOLDS)
                .unwrap()
                .remove(record.after().hold().storage_key().as_slice())
                .unwrap();
        } else {
            // Every semantic policy field still matches; only the initiating
            // request differs from the complete retained physical receipt.
            let Origin::Explicit {
                principal,
                approval_id,
                ..
            } = record.origin()
            else {
                panic!("explicit fixture")
            };
            let substituted = Record::new(
                record.administration_sequence(),
                record.timestamp(),
                record.action(),
                record.target(),
                record.before(),
                record.after(),
                record.observed(),
                Origin::Explicit {
                    request_id: RequestId::from_unix_milliseconds_and_random(
                        1_700_000_000_000,
                        [9; 10],
                    )
                    .unwrap(),
                    principal: principal.clone(),
                    approval_id: approval_id.clone(),
                },
            )
            .unwrap();
            let bytes = encode_replication_administration_v1(&substituted).unwrap();
            write
                .open_table(crate::layout::AUDIT)
                .unwrap()
                .insert(
                    crate::keys::encode_audit_key(record.administration_sequence()).as_slice(),
                    bytes.as_bytes(),
                )
                .unwrap();
        }
        assert!(crate::changelog_v3_roots::validate_retained_history_for_write(&write).is_err());
        write.abort().unwrap();
    }
}

#[test]
fn retired_tombstone_retains_audit_links_without_pinning_old_physical_receipts() {
    use riffdb_storage_api::{
        AuthoritativeNamespaceV1 as N, proto_codec::encode_changelog_history_state_v3,
    };
    let (_scope, ports, registration) = stored_registration();
    let history = crate::changelog_v3_roots::validate_retained_history(
        &ports.shared.database.begin_read().unwrap(),
    )
    .unwrap()
    .unwrap();
    let prior = registration.after();
    let retired = Policy::new(
        prior.hold(),
        prior.registered_at(),
        prior.budget(),
        prior.expires_at(),
        Phase::Retired,
        None,
    )
    .unwrap();
    let release = Record::new(
        registration
            .administration_sequence()
            .checked_next()
            .unwrap(),
        registration.timestamp(),
        Action::RetireFollower,
        registration.target(),
        Some(State::Registered(prior)),
        retired,
        history.tail(),
        Origin::Explicit {
            request_id: RequestId::from_unix_milliseconds_and_random(1_700_000_000_000, [8; 10])
                .unwrap(),
            principal: match explicit() {
                Origin::Explicit { principal, .. } => principal,
                _ => unreachable!(),
            },
            approval_id: None,
        },
    )
    .unwrap();
    store_record(&ports, &release, history);
    let history = crate::changelog_v3_roots::validate_retained_history(
        &ports.shared.database.begin_read().unwrap(),
    )
    .unwrap()
    .unwrap();
    let write = ports.shared.database.begin_write().unwrap();
    {
        let mut receipts = write
            .open_table(crate::changelog_v3_activation::HISTORY)
            .unwrap();
        for sequence in history.minimum_resume().sequence().get()..history.tail().sequence().get() {
            assert!(
                receipts
                    .remove(sequence.to_be_bytes().as_slice())
                    .unwrap()
                    .is_some()
            );
        }
    }
    let pruned = History::new(
        history.lineage(),
        history.anchor(),
        history.tail(),
        history.tail(),
    )
    .unwrap();
    let bytes = encode_changelog_history_state_v3(pruned).unwrap();
    write
        .open_table(crate::layout::META)
        .unwrap()
        .insert(
            N::ChangelogHistoryState.metadata_key().unwrap(),
            bytes.as_bytes(),
        )
        .unwrap();
    assert_eq!(
        crate::changelog_v3_roots::validate_retained_history_for_write(&write).unwrap(),
        Some(pruned)
    );
    ports.shared.commit_durable(write).unwrap();
    let pin = ports.shared.database.begin_read().unwrap();
    assert_eq!(
        crate::changelog_v3_roots::validate_retained_history(&pin).unwrap(),
        Some(pruned)
    );
    drop(pin);
    // Even after pruning, the original authorization receipt cannot disappear.
    let write = ports.shared.database.begin_write().unwrap();
    write
        .open_table(crate::layout::AUDIT)
        .unwrap()
        .remove(crate::keys::encode_audit_key(registration.administration_sequence()).as_slice())
        .unwrap();
    assert!(crate::changelog_v3_roots::validate_retained_history_for_write(&write).is_err());
    write.abort().unwrap();
}

#[test]
fn advisory_progress_refuses_a_retired_policy_stored_under_a_foreign_key() {
    let (_scope, ports, registration) = stored_registration();
    let original = registration.after();
    let retired = Policy::new(
        original.hold(),
        original.registered_at(),
        original.budget(),
        original.expires_at(),
        Phase::Retired,
        None,
    )
    .unwrap();
    let bytes = encode_replication_source_hold_v2(retired).unwrap();
    let mut foreign_key = original.hold().storage_key();
    let last = foreign_key.last_mut().unwrap();
    *last ^= 1;
    let write = ports.shared.database.begin_write().unwrap();
    write
        .open_table(crate::changelog_v3_activation::SOURCE_HOLDS)
        .unwrap()
        .insert(foreign_key.as_slice(), bytes.as_bytes())
        .unwrap();
    ports.shared.commit_durable(write).unwrap();
    let root = std::sync::Arc::new(crate::checkpoint_root::CheckpointRoot::new(
        ports.shared.database.begin_read().unwrap(),
        1,
    ));
    let access = crate::store::RedbReadAccess::Current(root);
    assert!(crate::changelog_v3_cursor::progress::observe(&access).is_err());
}

#[test]
fn source_write_preflight_refuses_a_missing_audit_table_without_creating_it() {
    use redb::TableHandle;
    let (_scope, ports, _) = stored_registration();
    let write = ports.shared.database.begin_write().unwrap();
    assert!(write.delete_table(crate::layout::AUDIT).unwrap());
    assert!(crate::changelog_v3_roots::validate_retained_history_for_write(&write).is_err());
    assert!(
        !write
            .list_tables()
            .unwrap()
            .any(|table| table.name() == crate::layout::AUDIT.name())
    );
    write.abort().unwrap();
}

#[test]
fn source_receipts_allow_only_current_or_older_incarnations_of_the_same_database() {
    let original = registration();
    let source = lineage();
    let restored =
        Lineage::new(source.database_id(), 2, LeadershipEpochV1::new(2).unwrap()).unwrap();
    assert!(check_in_lineage(None, std::slice::from_ref(&original), restored).is_ok());
    let foreign =
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [9; 10]).unwrap();
    let invalid = [
        Lineage::new(foreign, 1, LeadershipEpochV1::initial()).unwrap(),
        restored,
        Lineage::new(source.database_id(), 1, LeadershipEpochV1::new(2).unwrap()).unwrap(),
    ];
    for lineage in invalid {
        let policy = original.after();
        let substituted = Policy::new(
            Hold::new(
                policy.hold().id(),
                Kind::FollowerAcknowledgement,
                lineage,
                policy.hold().fence(),
            ),
            policy.registered_at(),
            policy.budget(),
            policy.expires_at(),
            policy.phase(),
            None,
        )
        .unwrap();
        let record = record(
            Action::RegisterFollower,
            None,
            substituted,
            original.observed(),
        );
        assert!(check(None, &[record]).is_err());
    }
}
