//! Isolated V2 transaction fixtures, never a production V1 upgrade path.
// req: REP-005, REC-001, STO-012
use super::*;
use riffdb_storage_api::{
    AdministrationSequenceAllocator as Allocator, AuditPrincipalV1,
    AuthoritativeMutationV3 as Mutation, AuthoritativeNamespaceV1 as N,
    AuthoritativeStateCatalogV2, AuthoritativeTransactionBindingV3 as Binding,
    AuthoritativeTransactionV3 as Receipt, ChangelogAttributionV3 as Source,
    ChangelogHistoryPointV3 as Point, ChangelogHistoryStateV3 as History,
    ChangelogLineageV3 as Lineage, ChangelogTransactionSequence as Sequence, FollowerHoldBudget,
    FollowerRegistrationPhaseV1 as Phase, PrimaryFenceRequestV1 as Request,
    ReplicationAdministrationActionV1 as Action, ReplicationAdministrationOriginV1 as Origin,
    ReplicationPrimaryAdmissionV1 as Admission, ReplicationSourceHoldKindV1 as Kind,
    ReplicationSourceHoldV1 as Hold, ReplicationSourceHoldV2 as Policy,
    StoredReplicationAdministrationV1 as Registration, proto_codec::*,
};
use riffdb_types::{
    ActorId, ActorKind, AdministrationSequence as Admin, CapabilityId, CommitSequence, DatabaseId,
    DualFrontier, LeadershipEpochV1, ReplicationFenceOperationId,
    ReplicationFollowerAuditTargetV1 as Target, ReplicationSourceHoldIdV1, RequestId, Timestamp,
};
use std::num::NonZeroU64;

#[path = "primary_fence_authorization_tests.rs"]
mod authorization;

#[path = "primary_fence_replay_tests.rs"]
mod replay;

#[path = "primary_admission_root_tests.rs"]
mod root_validation;

struct Fixture {
    history: History,
    admission: Admission,
    policy: Policy,
    request: Request,
    principal: AuditPrincipalV1,
    timestamp: Timestamp,
    allocator: Allocator,
    registration: Registration,
    registration_receipt: Receipt,
    prior_receipt: Receipt,
}
impl Fixture {
    fn new(application: u64) -> Self {
        let lineage = Lineage::new_with_catalog(
            DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [1; 10]).unwrap(),
            2,
            LeadershipEpochV1::new(3).unwrap(),
            AuthoritativeStateCatalogV2.digest(),
        )
        .unwrap();
        let principal = AuditPrincipalV1::new(
            ActorId::new("fence-operator").unwrap(),
            ActorKind::Human,
            CapabilityId::from_unix_milliseconds_and_random(1_700_000_000_000, [2; 10]).unwrap(),
            NonZeroU64::MIN,
        );
        let timestamp = Timestamp::new(1_700_000_000, 0).unwrap();
        let prior_receipt = Receipt::new_for_catalog(
            Binding {
                database_id: lineage.database_id(),
                history_incarnation: 2,
                predecessor: Some(Sequence::new(6).unwrap()),
                sequence: Sequence::new(7).unwrap(),
                predecessor_frontier: DualFrontier::new(
                    CommitSequence::new(application),
                    Admin::new(1),
                ),
                covered_frontier: DualFrontier::new(
                    CommitSequence::new(application),
                    Admin::new(1),
                ),
                prior_history_hash: [6; 32],
            },
            Source::ReplicationSourceHold,
            vec![],
            lineage.catalog_digest(),
        )
        .unwrap();
        let registered_at = Point::from_receipt(&prior_receipt).unwrap();
        let hold = Hold::new(
            ReplicationSourceHoldIdV1::new([3; 16]).unwrap(),
            Kind::FollowerAcknowledgement,
            lineage,
            registered_at,
        );
        let policy = Policy::new(
            hold,
            registered_at,
            FollowerHoldBudget::new(10).unwrap(),
            None,
            Phase::AwaitingBootstrap,
            None,
        )
        .unwrap();
        let target = Target::new(
            lineage.database_id(),
            2,
            lineage.leadership_epoch(),
            hold.id(),
        )
        .unwrap();
        let request = Request::new(
            RequestId::from_unix_milliseconds_and_random(1_700_000_000_000, [4; 10]).unwrap(),
            ReplicationFenceOperationId::from_unix_milliseconds_and_random(
                1_700_000_000_000,
                [5; 10],
            )
            .unwrap(),
            target,
            policy.generation(),
        );
        let registration = Registration::new(
            Admin::new(2).unwrap(),
            timestamp,
            Action::RegisterFollower,
            target,
            None,
            policy,
            registered_at,
            Origin::Explicit {
                request_id: request.request_id(),
                principal: principal.clone(),
                approval_id: None,
            },
        )
        .unwrap();
        let audit = encode_replication_administration_v1(&registration).unwrap();
        let before =
            encode_administration_sequence_allocator_v1(Allocator::next(Admin::new(2).unwrap()))
                .unwrap();
        let allocator = Allocator::next(Admin::new(3).unwrap());
        let after = encode_administration_sequence_allocator_v1(allocator).unwrap();
        let mut mutations = vec![
            Mutation::put(
                N::Audit,
                &crate::keys::encode_audit_key(Admin::new(2).unwrap()),
                None,
                audit.as_bytes(),
            )
            .unwrap(),
            Mutation::replace(
                N::NextAdministrationSequence,
                crate::layout::META_ADMINISTRATION_SEQUENCE.as_bytes(),
                before.as_bytes(),
                after.as_bytes(),
            )
            .unwrap(),
        ];
        mutations.sort_by(|a, b| (a.namespace(), a.key()).cmp(&(b.namespace(), b.key())));
        let registration_receipt = Receipt::new_for_catalog(
            Binding {
                database_id: lineage.database_id(),
                history_incarnation: 2,
                predecessor: Some(registered_at.sequence()),
                sequence: policy.generation(),
                predecessor_frontier: registered_at.frontier(),
                covered_frontier: DualFrontier::new(
                    CommitSequence::new(application),
                    Admin::new(2),
                ),
                prior_history_hash: registered_at.history_hash(),
            },
            Source::RetentionHold,
            mutations,
            lineage.catalog_digest(),
        )
        .unwrap();
        let tail = Point::from_receipt(&registration_receipt).unwrap();
        let anchor = Point::new(Sequence::new(1).unwrap(), [1; 32], DualFrontier::INITIAL);
        let history = History::new(lineage, anchor, tail, registered_at).unwrap();
        Self {
            history,
            admission: Admission::active(lineage).unwrap(),
            policy,
            request,
            principal,
            timestamp,
            allocator,
            registration,
            registration_receipt,
            prior_receipt,
        }
    }
    fn plan(&self) -> Result<PrimaryFencePlan, riffdb_storage_api::StorageError> {
        PrimaryFencePlan::new(
            self.history,
            self.admission.clone(),
            self.policy,
            self.request,
            self.principal.clone(),
            self.timestamp,
            self.allocator,
        )
    }
}

#[test]
fn fence_plan_keeps_application_head_and_binds_exact_control_receipt() {
    for application in [0, 5] {
        let f = Fixture::new(application);
        let plan = f.plan().unwrap();
        assert_eq!(plan.record.operation_id(), f.request.operation_id());
        assert_eq!(plan.record.target(), f.request.target());
        assert_eq!(plan.record.generation(), f.policy.generation());
        assert_eq!(plan.record.observed(), f.history.tail());
        assert_eq!(
            plan.record.administration_sequence(),
            Admin::new(3).unwrap()
        );
        assert_eq!(
            plan.record.final_application_head(),
            CommitSequence::new(application)
        );
        assert_eq!(plan.admission.fence(), Some(&plan.record));
        assert_eq!(plan.receipt.attribution(), Source::PrimaryFence);
        assert_eq!(plan.receipt.mutations().len(), 2);
        assert_eq!(plan.successor, f.history.advance(&plan.receipt).unwrap());
        assert_eq!(
            plan.successor.tail().frontier().application(),
            f.history.tail().frontier().application()
        );
        assert_eq!(
            plan.next_administration,
            Allocator::next(Admin::new(4).unwrap())
        );
        assert_eq!(
            plan.receipt
                .mutations()
                .iter()
                .filter(|m| m.namespace() == N::Audit)
                .count(),
            1
        );
        assert!(
            plan.receipt
                .mutations()
                .iter()
                .all(|m| matches!(m.namespace(), N::Audit | N::NextAdministrationSequence))
        );
        let audit = plan
            .receipt
            .mutations()
            .iter()
            .find(|m| m.namespace() == N::Audit)
            .unwrap();
        assert_eq!(
            decode_primary_fence_administration_v1(audit.value().unwrap())
                .unwrap()
                .value(),
            &plan.record
        );
        assert_eq!(format!("{plan:?}"), "PrimaryFencePlan([REDACTED])");
    }
}

#[test]
fn fence_plan_refuses_retargeted_retired_or_already_fenced_sources() {
    for changed in 0..6 {
        let mut f = Fixture::new(5);
        match changed {
            0 => f.admission = f.plan().unwrap().admission,
            1 => f.allocator = Allocator::next(Admin::new(4).unwrap()),
            2 => f.allocator = Allocator::Exhausted,
            3 => {
                f.request = Request::new(
                    f.request.request_id(),
                    f.request.operation_id(),
                    f.request.target(),
                    Sequence::new(9).unwrap(),
                )
            }
            4 => {
                f.policy = Policy::new(
                    f.policy.hold(),
                    f.policy.registered_at(),
                    f.policy.budget(),
                    None,
                    Phase::Retired,
                    None,
                )
                .unwrap()
            }
            5 => {
                f.request = Request::new(
                    f.request.request_id(),
                    f.request.operation_id(),
                    Target::new(
                        f.history.lineage().database_id(),
                        2,
                        LeadershipEpochV1::new(3).unwrap(),
                        ReplicationSourceHoldIdV1::new([9; 16]).unwrap(),
                    )
                    .unwrap(),
                    f.policy.generation(),
                )
            }
            _ => unreachable!(),
        }
        assert!(f.plan().is_err(), "changed {changed}");
    }
}

#[test]
fn fence_transaction_commits_complete_bundle_and_drop_aborts_every_row() {
    for application in [0, 5] {
        let f = Fixture::new(application);
        let scope = crate::test_path::ScopedDirectory::new("primary-fence-atomic");
        let path = scope.join("db.redb");
        let database = f.database(&path);
        let before = snapshot(&database);
        drop(f.plan().unwrap().stage(&database).unwrap());
        assert_eq!(snapshot(&database), before);
        let expected = f.plan().unwrap();
        let record = f
            .plan()
            .unwrap()
            .stage(&database)
            .unwrap()
            .commit_for_test()
            .unwrap();
        assert_eq!(record, expected.record);
        f.assert_state(&database, true);
        let after = snapshot(&database);
        let retry = f.plan().unwrap().stage(&database).unwrap();
        assert!(matches!(
            retry,
            authority::PrimaryFenceCompletion::Replay(_)
        ));
        assert_eq!(retry.commit_for_test().unwrap(), record);
        assert_eq!(snapshot(&database), after);
        drop(database);
        f.assert_state(&redb::Database::open(path).unwrap(), true);
    }
}

use crate::changelog_v3_activation::{HISTORY, SOURCE_HOLDS};
use crate::layout::{AUDIT, META};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition, TableHandle};

impl Fixture {
    fn database(&self, path: &std::path::Path) -> Database {
        let database = Database::create(path).unwrap();
        let write = database.begin_write().unwrap();
        crate::layout::create_all_tables(&write).unwrap();
        authorization::seed(&write, &authorization::capability(self));
        for (key, value) in self.plan().unwrap().metadata(false).unwrap() {
            write
                .open_table(META)
                .unwrap()
                .insert(key, value.as_bytes())
                .unwrap();
        }
        let policy = encode_replication_source_hold_v2(self.policy).unwrap();
        write
            .open_table(SOURCE_HOLDS)
            .unwrap()
            .insert(
                self.policy.hold().storage_key().as_slice(),
                policy.as_bytes(),
            )
            .unwrap();
        let audit = encode_replication_administration_v1(&self.registration).unwrap();
        write
            .open_table(AUDIT)
            .unwrap()
            .insert(
                crate::keys::encode_audit_key(self.registration.administration_sequence())
                    .as_slice(),
                audit.as_bytes(),
            )
            .unwrap();
        for receipt in [&self.prior_receipt, &self.registration_receipt] {
            write
                .open_table(HISTORY)
                .unwrap()
                .insert(
                    receipt.binding().sequence.get().to_be_bytes().as_slice(),
                    receipt.encode().unwrap().as_slice(),
                )
                .unwrap();
        }
        write.commit().unwrap();
        database
    }
    fn assert_state(&self, database: &Database, fenced: bool) {
        let plan = self.plan().unwrap();
        let read = database.begin_read().unwrap();
        let meta = read.open_table(META).unwrap();
        for (key, value) in plan.metadata(fenced).unwrap() {
            assert_eq!(
                meta.get(key).unwrap().unwrap().value(),
                value.as_bytes(),
                "{key}"
            );
        }
        let audit = read.open_table(AUDIT).unwrap();
        let receipts = read.open_table(HISTORY).unwrap();
        let holds = read.open_table(SOURCE_HOLDS).unwrap();
        let history = if fenced { plan.successor } else { self.history };
        crate::changelog_v3_roots::validate_retained_rows(history, &receipts, &holds).unwrap();
        crate::replication_registration_links::validate(&holds, &audit, history, &receipts)
            .unwrap();
        assert_eq!(
            history.tail().frontier().application(),
            self.history.tail().frontier().application()
        );
        let record = audit
            .get(crate::keys::encode_audit_key(plan.record.administration_sequence()).as_slice())
            .unwrap();
        let receipt = receipts
            .get(
                plan.receipt
                    .binding()
                    .sequence
                    .get()
                    .to_be_bytes()
                    .as_slice(),
            )
            .unwrap();
        assert_eq!(record.is_some(), fenced);
        assert_eq!(receipt.is_some(), fenced);
        if fenced {
            let record = decode_primary_fence_administration_v1(record.unwrap().value()).unwrap();
            assert_eq!(record.value(), &plan.record);
            assert_eq!(record.value().principal(), &self.principal);
            assert_eq!(record.value().request_id(), self.request.request_id());
            assert_eq!(receipt.unwrap().value(), plan.receipt.encode().unwrap());
        }
    }
}
type Rows = std::collections::BTreeMap<String, Vec<(Vec<u8>, Vec<u8>)>>;
fn snapshot(database: &Database) -> Rows {
    let read = database.begin_read().unwrap();
    let mut all = Rows::new();
    for handle in read.list_tables().unwrap() {
        let name = handle.name();
        let rows = if name == META.name() {
            read.open_table(META)
                .unwrap()
                .iter()
                .unwrap()
                .map(|r| {
                    let (k, v) = r.unwrap();
                    (k.value().as_bytes().to_vec(), v.value().to_vec())
                })
                .collect()
        } else {
            let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new(name);
            read.open_table(definition)
                .unwrap()
                .iter()
                .unwrap()
                .map(|r| {
                    let (k, v) = r.unwrap();
                    (k.value().to_vec(), v.value().to_vec())
                })
                .collect()
        };
        all.insert(name.to_owned(), rows);
    }
    all
}

#[test]
fn fence_transaction_refuses_changed_or_missing_evidence_without_mutation() {
    for changed in 0..13 {
        let f = Fixture::new(5);
        let scope = crate::test_path::ScopedDirectory::new("primary-fence-beforeimage");
        let database = f.database(&scope.join("db.redb"));
        let write = database.begin_write().unwrap();
        match changed {
            0 => {
                write
                    .open_table(META)
                    .unwrap()
                    .remove(
                        riffdb_storage_api::AuthoritativeNamespaceV2::ReplicationPrimaryAdmission
                            .metadata_key()
                            .unwrap(),
                    )
                    .unwrap();
            }
            1 => {
                let bytes =
                    encode_replication_primary_admission_v1(&f.plan().unwrap().admission).unwrap();
                write
                    .open_table(META)
                    .unwrap()
                    .insert(
                        riffdb_storage_api::AuthoritativeNamespaceV2::ReplicationPrimaryAdmission
                            .metadata_key()
                            .unwrap(),
                        bytes.as_bytes(),
                    )
                    .unwrap();
            }
            2 => {
                let bytes = encode_authoritative_state_catalog_v1(
                    riffdb_storage_api::AuthoritativeStateCatalogV1,
                )
                .unwrap();
                write
                    .open_table(META)
                    .unwrap()
                    .insert(
                        N::AuthoritativeStateCatalog.metadata_key().unwrap(),
                        bytes.as_bytes(),
                    )
                    .unwrap();
            }
            3 => {
                write
                    .open_table(SOURCE_HOLDS)
                    .unwrap()
                    .remove(f.policy.hold().storage_key().as_slice())
                    .unwrap();
            }
            4 => {
                write
                    .open_table(AUDIT)
                    .unwrap()
                    .remove(
                        crate::keys::encode_audit_key(f.registration.administration_sequence())
                            .as_slice(),
                    )
                    .unwrap();
            }
            5 => {
                write
                    .open_table(HISTORY)
                    .unwrap()
                    .remove(
                        f.registration_receipt
                            .binding()
                            .sequence
                            .get()
                            .to_be_bytes()
                            .as_slice(),
                    )
                    .unwrap();
            }
            6 => {
                let plan = f.plan().unwrap();
                let bytes = encode_primary_fence_administration_v1(&plan.record).unwrap();
                write
                    .open_table(AUDIT)
                    .unwrap()
                    .insert(
                        crate::keys::encode_audit_key(plan.record.administration_sequence())
                            .as_slice(),
                        bytes.as_bytes(),
                    )
                    .unwrap();
            }
            7 => {
                write
                    .open_table(META)
                    .unwrap()
                    .insert(crate::layout::META_APPLICATION_SEQUENCE, b"bad".as_slice())
                    .unwrap();
            }
            8 => {
                write.delete_table(HISTORY).unwrap();
            }
            9 => {
                write.delete_table(SOURCE_HOLDS).unwrap();
            }
            10 => {
                write.delete_table(AUDIT).unwrap();
            }
            11 => {
                let bytes = encode_changelog_transaction_allocator_v3(
                    f.plan().unwrap().successor.expected_allocator(),
                )
                .unwrap();
                write
                    .open_table(META)
                    .unwrap()
                    .insert(
                        N::NextChangelogTransaction.metadata_key().unwrap(),
                        bytes.as_bytes(),
                    )
                    .unwrap();
            }
            12 => {
                let bytes = encode_replication_source_hold_v2(
                    Policy::new(
                        f.policy.hold(),
                        f.policy.registered_at(),
                        FollowerHoldBudget::new(11).unwrap(),
                        None,
                        Phase::AwaitingBootstrap,
                        None,
                    )
                    .unwrap(),
                )
                .unwrap();
                write
                    .open_table(SOURCE_HOLDS)
                    .unwrap()
                    .insert(f.policy.hold().storage_key().as_slice(), bytes.as_bytes())
                    .unwrap();
            }
            _ => unreachable!(),
        }
        write.commit().unwrap();
        let before = snapshot(&database);
        assert!(
            f.plan().unwrap().stage(&database).is_err(),
            "changed {changed}"
        );
        assert_eq!(snapshot(&database), before, "changed {changed}");
    }
}

#[test]
fn fence_process_child() {
    let Some(path) = std::env::var_os("RIFFDB_PRIMARY_FENCE_PATH") else {
        return;
    };
    let f = Fixture::new(5);
    let database = f.database(std::path::Path::new(&path));
    f.plan()
        .unwrap()
        .stage(&database)
        .unwrap()
        .commit_for_test()
        .unwrap();
    panic!("requested deterministic crash edge was not reached");
}

#[test]
fn fence_process_crashes_preserve_old_or_complete_fence_after_lost_response() {
    for edge in ["audit", "admission", "receipt", "roots", "committed"] {
        let scope = crate::test_path::ScopedDirectory::new("primary-fence-crash");
        let path = scope.join("db.redb");
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg("primary_fence_write::tests::fence_process_child")
            .arg("--nocapture")
            .env("RIFFDB_PRIMARY_FENCE_PATH", &path)
            .env("RIFFDB_PRIMARY_FENCE_EDGE", edge)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(94), "crash edge {edge}");
        let f = Fixture::new(5);
        let database = Database::open(path).unwrap();
        f.assert_state(&database, edge == "committed");
        if edge == "committed" {
            let before = snapshot(&database);
            let retry = f.plan().unwrap().stage(&database).unwrap();
            assert!(matches!(
                retry,
                authority::PrimaryFenceCompletion::Replay(_)
            ));
            assert_eq!(retry.commit_for_test().unwrap(), f.plan().unwrap().record);
            assert_eq!(snapshot(&database), before);
        } else {
            f.plan()
                .unwrap()
                .stage(&database)
                .unwrap()
                .commit_for_test()
                .unwrap();
            f.assert_state(&database, true);
        }
    }
}

#[test]
fn retained_fence_refuses_replaced_active_metadata_and_a_second_fence() {
    let f = Fixture::new(5);
    let scope = crate::test_path::ScopedDirectory::new("primary-fence-no-unfence");
    let database = f.database(&scope.join("db.redb"));
    let plan = f.plan().unwrap();
    let successor = plan.successor;
    let next_administration = plan.next_administration;
    plan.stage(&database).unwrap().commit_for_test().unwrap();
    let active = Admission::active(successor.lineage()).unwrap();
    let bytes = encode_replication_primary_admission_v1(&active).unwrap();
    let write = database.begin_write().unwrap();
    write
        .open_table(META)
        .unwrap()
        .insert(
            riffdb_storage_api::AuthoritativeNamespaceV2::ReplicationPrimaryAdmission
                .metadata_key()
                .unwrap(),
            bytes.as_bytes(),
        )
        .unwrap();
    write.commit().unwrap();
    // Every current root and allocator matches this new recipe. Only the retained
    // first fence proves that the substituted Active row cannot grant authority.
    let second = PrimaryFencePlan::new(
        successor,
        active,
        f.policy,
        Request::new(
            f.request.request_id(),
            ReplicationFenceOperationId::from_unix_milliseconds_and_random(
                1_700_000_000_000,
                [9; 10],
            )
            .unwrap(),
            f.request.target(),
            f.request.generation(),
        ),
        f.principal.clone(),
        f.timestamp,
        next_administration,
    )
    .unwrap();
    let before = snapshot(&database);
    assert!(second.stage(&database).is_err());
    assert_eq!(snapshot(&database), before);
}
