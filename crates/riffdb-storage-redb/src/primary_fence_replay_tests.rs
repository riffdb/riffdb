//! Exact, freshly authorized retries retain the original durable fence.
// req: REP-005, REC-001, STO-012
use super::*;

#[path = "primary_fence_replay_corruption_tests.rs"]
mod corruption;

fn execute(
    f: &Fixture,
    database: &Database,
) -> Result<authority::PrimaryFenceCompletion, StorageError> {
    let (awaiting, _) =
        authority::PrimaryFenceCandidate::begin(database, f.request, f.principal.clone())?
            .read_transaction_current()?;
    awaiting.stage(f.request, f.principal.clone(), f.timestamp)
}

#[test]
fn fence_exact_retry_retains_original_receipt_on_new_invocation_without_mutation() {
    let mut f = Fixture::new(5);
    let scope = crate::test_path::ScopedDirectory::new("primary-fence-exact-retry");
    let path = scope.join("db.redb");
    let database = f.database(&path);
    let original = f
        .plan()
        .unwrap()
        .stage(&database)
        .unwrap()
        .commit_for_test()
        .unwrap();
    drop(database);
    let database = Database::open(&path).unwrap();
    let before = snapshot(&database);
    f.request = Request::new(
        RequestId::from_unix_milliseconds_and_random(1_700_000_000_000, [10; 10]).unwrap(),
        f.request.operation_id(),
        f.request.target(),
        f.request.generation(),
    );
    f.timestamp = Timestamp::new(1_700_000_001, 0).unwrap();
    let retry = execute(&f, &database);
    assert!(
        retry.is_ok(),
        "a freshly authorized exact fence retry must resolve after reopening"
    );
    assert_eq!(retry.unwrap().commit_for_test().unwrap(), original);
    assert_eq!(snapshot(&database), before);
}

#[test]
fn fence_exact_retry_requires_current_authority_and_original_selection() {
    for case in 0..7 {
        let mut f = Fixture::new(5);
        let scope = crate::test_path::ScopedDirectory::new("primary-fence-replay-refusal");
        let database = f.database(&scope.join("db.redb"));
        execute(&f, &database).unwrap().commit_for_test().unwrap();
        match case {
            0 => {
                let write = database.begin_write().unwrap();
                write
                    .open_table(crate::layout::CAPABILITIES)
                    .unwrap()
                    .remove(
                        crate::keys::encode_capability_key(f.principal.capability_id()).as_slice(),
                    )
                    .unwrap();
                write.commit().unwrap();
            }
            1 => {
                let write = database.begin_write().unwrap();
                let revoked = authorization::capability(&f)
                    .revoked(
                        NonZeroU64::MIN,
                        f.timestamp,
                        Admin::new(2).unwrap(),
                        riffdb_storage_api::RevocationReasonCodeV1::Requested,
                    )
                    .unwrap();
                authorization::seed(&write, &revoked);
                write.commit().unwrap();
            }
            2 => {
                f.timestamp = authorization::capability(&f).expires_at();
            }
            3 => {
                f.request = Request::new(
                    f.request.request_id(),
                    ReplicationFenceOperationId::from_unix_milliseconds_and_random(
                        1_700_000_000_000,
                        [99; 10],
                    )
                    .unwrap(),
                    f.request.target(),
                    f.request.generation(),
                );
            }
            4 => {
                f.request = Request::new(
                    f.request.request_id(),
                    f.request.operation_id(),
                    f.request.target(),
                    Sequence::new(9).unwrap(),
                );
            }
            5 => {
                let target = f.request.target();
                f.request = Request::new(
                    f.request.request_id(),
                    f.request.operation_id(),
                    Target::new(
                        target.database_id(),
                        target.history_incarnation(),
                        target.leadership_epoch(),
                        ReplicationSourceHoldIdV1::new([88; 16]).unwrap(),
                    )
                    .unwrap(),
                    f.request.generation(),
                );
            }
            _ => {
                f.principal = AuditPrincipalV1::new(
                    ActorId::new("different-operator").unwrap(),
                    ActorKind::Human,
                    f.principal.capability_id(),
                    NonZeroU64::MIN,
                );
                let write = database.begin_write().unwrap();
                authorization::seed(&write, &authorization::capability(&f));
                write.commit().unwrap();
            }
        }
        let before = snapshot(&database);
        let result = execute(&f, &database);
        match case {
            3 | 6 => assert!(
                matches!(
                    result,
                    Ok(authority::PrimaryFenceCompletion::Refused(
                        riffdb_storage_api::PrimaryFenceRefusalV1::FenceConflict
                    ))
                ),
                "case {case}"
            ),
            4 | 5 => assert!(
                matches!(
                    result,
                    Ok(authority::PrimaryFenceCompletion::Refused(
                        riffdb_storage_api::PrimaryFenceRefusalV1::RegistrationMissingOrStale
                    ))
                ),
                "case {case}"
            ),
            _ => assert!(result.is_err(), "case {case}"),
        }
        assert_eq!(snapshot(&database), before, "case {case}");
    }
}

#[test]
fn fence_replay_survives_verified_acknowledgement_after_original_fence() {
    let f = Fixture::new(5);
    let scope = crate::test_path::ScopedDirectory::new("primary-fence-replay-after-ack");
    let database = f.database(&scope.join("db.redb"));
    let original = execute(&f, &database).unwrap().commit_for_test().unwrap();
    let plan = f.plan().unwrap();
    let predecessor = plan.successor;
    let hold = Hold::new(
        f.policy.hold().id(),
        Kind::FollowerAcknowledgement,
        f.history.lineage(),
        predecessor.tail(),
    );
    let policy = Policy::new(
        hold,
        f.policy.registered_at(),
        f.policy.budget(),
        f.policy.expires_at(),
        Phase::Attached,
        None,
    )
    .unwrap();
    let receipt = Receipt::new_for_catalog(
        Binding {
            database_id: f.history.lineage().database_id(),
            history_incarnation: f.history.lineage().history_incarnation(),
            predecessor: Some(predecessor.tail().sequence()),
            sequence: predecessor.tail().sequence().checked_next().unwrap(),
            predecessor_frontier: predecessor.tail().frontier(),
            covered_frontier: predecessor.tail().frontier(),
            prior_history_hash: predecessor.tail().history_hash(),
        },
        Source::ReplicationSourceHold,
        vec![],
        f.history.lineage().catalog_digest(),
    )
    .unwrap();
    let after = predecessor.advance(&receipt).unwrap();
    let write = database.begin_write().unwrap();
    let encoded = encode_replication_source_hold_v2(policy).unwrap();
    write
        .open_table(SOURCE_HOLDS)
        .unwrap()
        .insert(hold.storage_key().as_slice(), encoded.as_bytes())
        .unwrap();
    write
        .open_table(HISTORY)
        .unwrap()
        .insert(
            receipt.binding().sequence.get().to_be_bytes().as_slice(),
            receipt.encode().unwrap().as_slice(),
        )
        .unwrap();
    for (key, value) in
        transaction::source_metadata(after, plan.next_administration, &plan.admission).unwrap()
    {
        write
            .open_table(META)
            .unwrap()
            .insert(key, value.as_bytes())
            .unwrap();
    }
    write.commit().unwrap();
    let before = snapshot(&database);
    let retry = execute(&f, &database).unwrap();
    assert!(matches!(
        retry,
        authority::PrimaryFenceCompletion::Replay(_)
    ));
    assert_eq!(retry.commit_for_test().unwrap(), original);
    assert_eq!(snapshot(&database), before);
}

#[test]
fn fence_replay_preserves_original_administration_sequence_after_service_audit() {
    use riffdb_storage_api::{
        ServiceAuditAppendIntentV1, StoredServiceAuditRecordV1, StoredServiceAuditRequestIndexV1,
    };
    use riffdb_types::{
        ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceAuditTargetV1, ServiceAuditTargetsV1,
        ServiceIngressKindV1, ServiceOperationV1,
    };
    let f = Fixture::new(5);
    let scope = crate::test_path::ScopedDirectory::new("primary-fence-after-audit");
    let database = f.database(&scope.join("db.redb"));
    let original = execute(&f, &database).unwrap().commit_for_test().unwrap();
    let plan = f.plan().unwrap();
    let invocation =
        RequestId::from_unix_milliseconds_and_random(1_700_000_000_000, [44; 10]).unwrap();
    let intent = ServiceAuditAppendIntentV1::new(
        invocation,
        f.timestamp,
        ServiceOperationV1::RegisterFollower,
        ServiceAuditPhaseV1::Started,
        f.principal.clone(),
        ServiceIngressKindV1::Grpc,
        ServiceAuditTargetsV1::new([ServiceAuditTargetV1::ReplicationFollower(
            f.request.target(),
        )])
        .unwrap(),
        None,
        ServiceAuditLinkV1::None,
    )
    .unwrap();
    let assigned = Admin::new(4).unwrap();
    let next = Allocator::next(Admin::new(5).unwrap());
    let record = StoredServiceAuditRecordV1::from_intent(assigned, &intent);
    let index = StoredServiceAuditRequestIndexV1::new(invocation, assigned);
    let mut mutations = vec![
        Mutation::put(
            N::Audit,
            &crate::keys::encode_audit_key(assigned),
            None,
            encode_service_audit_record_v3(&record).unwrap().as_bytes(),
        )
        .unwrap(),
        Mutation::put(
            N::AuditByRequest,
            &crate::keys::encode_audit_by_request_key(invocation, assigned),
            None,
            encode_service_audit_request_index_v1(index)
                .unwrap()
                .as_bytes(),
        )
        .unwrap(),
        Mutation::replace(
            N::NextAdministrationSequence,
            crate::layout::META_ADMINISTRATION_SEQUENCE.as_bytes(),
            encode_administration_sequence_allocator_v1(plan.next_administration)
                .unwrap()
                .as_bytes(),
            encode_administration_sequence_allocator_v1(next)
                .unwrap()
                .as_bytes(),
        )
        .unwrap(),
    ];
    mutations.sort_by(|a, b| (a.namespace(), a.key()).cmp(&(b.namespace(), b.key())));
    let predecessor = plan.successor;
    let receipt = Receipt::new_for_catalog(
        Binding {
            database_id: predecessor.lineage().database_id(),
            history_incarnation: predecessor.lineage().history_incarnation(),
            predecessor: Some(predecessor.tail().sequence()),
            sequence: predecessor.tail().sequence().checked_next().unwrap(),
            predecessor_frontier: predecessor.tail().frontier(),
            covered_frontier: DualFrontier::new(
                predecessor.tail().frontier().application(),
                Some(assigned),
            ),
            prior_history_hash: predecessor.tail().history_hash(),
        },
        Source::DirectApplicationOrServiceAuditGroup,
        mutations,
        predecessor.lineage().catalog_digest(),
    )
    .unwrap();
    crate::changelog_v3_write::PreparedImmediateReceipt::apply(
        &database,
        crate::store::RedbCommitProfile::Hardened,
        &receipt,
    )
    .unwrap()
    .commit_for_test()
    .unwrap();
    let before = snapshot(&database);
    let retry = execute(&f, &database).unwrap();
    assert!(matches!(
        retry,
        authority::PrimaryFenceCompletion::Replay(_)
    ));
    assert_eq!(retry.commit_for_test().unwrap(), original);
    assert_eq!(original.administration_sequence(), Admin::new(3).unwrap());
    assert_eq!(snapshot(&database), before);
}
