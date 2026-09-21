//! Retained evidence must prove the complete original fence before replay.
// req: REP-005, REC-001, STO-012
use super::*;

#[test]
fn fence_replay_refuses_missing_contradictory_or_checksum_correct_incomplete_evidence() {
    for case in 0..11 {
        let f = Fixture::new(5);
        let scope = crate::test_path::ScopedDirectory::new("primary-fence-replay-corruption");
        let database = f.database(&scope.join("db.redb"));
        execute(&f, &database).unwrap().commit_for_test().unwrap();
        let plan = f.plan().unwrap();
        let write = database.begin_write().unwrap();
        let admission_key =
            riffdb_storage_api::AuthoritativeNamespaceV2::ReplicationPrimaryAdmission
                .metadata_key()
                .unwrap();
        match case {
            0 => {
                write
                    .open_table(AUDIT)
                    .unwrap()
                    .remove(
                        crate::keys::encode_audit_key(plan.record.administration_sequence())
                            .as_slice(),
                    )
                    .unwrap();
            }
            1 => {
                write
                    .open_table(META)
                    .unwrap()
                    .remove(admission_key)
                    .unwrap();
            }
            2 => {
                write
                    .open_table(META)
                    .unwrap()
                    .insert(
                        admission_key,
                        encode_replication_primary_admission_v1(&f.admission)
                            .unwrap()
                            .as_bytes(),
                    )
                    .unwrap();
            }
            3 => {
                write
                    .open_table(HISTORY)
                    .unwrap()
                    .remove(
                        plan.receipt
                            .binding()
                            .sequence
                            .get()
                            .to_be_bytes()
                            .as_slice(),
                    )
                    .unwrap();
            }
            4 => {
                write
                    .open_table(META)
                    .unwrap()
                    .insert(
                        crate::layout::META_APPLICATION_SEQUENCE,
                        encode_application_sequence_allocator_v1(
                            riffdb_storage_api::ApplicationSequenceAllocator::next(
                                CommitSequence::new(7).unwrap(),
                            ),
                        )
                        .unwrap()
                        .as_bytes(),
                    )
                    .unwrap();
            }
            5 => {
                write
                    .open_table(META)
                    .unwrap()
                    .insert(
                        N::LeadershipEpoch.metadata_key().unwrap(),
                        encode_leadership_epoch_v1(LeadershipEpochV1::new(4).unwrap())
                            .unwrap()
                            .as_bytes(),
                    )
                    .unwrap();
            }
            6 => {
                write
                    .open_table(AUDIT)
                    .unwrap()
                    .remove(
                        crate::keys::encode_audit_key(f.registration.administration_sequence())
                            .as_slice(),
                    )
                    .unwrap();
            }
            7 => {
                let substituted = StoredPrimaryFenceAdministrationV1::new(
                    plan.record.administration_sequence(),
                    Timestamp::new(1_700_000_001, 0).unwrap(),
                    f.request.operation_id(),
                    f.request.request_id(),
                    f.principal.clone(),
                    None,
                    f.request.target(),
                    f.request.generation(),
                    f.history.tail(),
                )
                .unwrap();
                write
                    .open_table(META)
                    .unwrap()
                    .insert(
                        admission_key,
                        encode_replication_primary_admission_v1(&Admission::fenced(substituted))
                            .unwrap()
                            .as_bytes(),
                    )
                    .unwrap();
            }
            8 => {
                let other = StoredPrimaryFenceAdministrationV1::new(
                    Admin::new(4).unwrap(),
                    f.timestamp,
                    f.request.operation_id(),
                    f.request.request_id(),
                    f.principal.clone(),
                    None,
                    f.request.target(),
                    f.request.generation(),
                    plan.successor.tail(),
                )
                .unwrap();
                let receipt = receipt::reconstruct(&other).unwrap();
                write
                    .open_table(AUDIT)
                    .unwrap()
                    .insert(
                        crate::keys::encode_audit_key(other.administration_sequence()).as_slice(),
                        encode_primary_fence_administration_v1(&other)
                            .unwrap()
                            .as_bytes(),
                    )
                    .unwrap();
                install_receipt(
                    &write,
                    &receipt,
                    plan.successor.advance(&receipt).unwrap(),
                    Allocator::next(Admin::new(5).unwrap()),
                    &plan.admission,
                );
            }
            9 => {
                // The audit insert, after allocator, frontier and checksum are
                // valid, but the allocator beforeimage is from another state.
                let mut mutations = plan.receipt.mutations().to_vec();
                let index = mutations
                    .iter()
                    .position(|m| m.namespace() == N::NextAdministrationSequence)
                    .unwrap();
                mutations[index] = Mutation::replace(
                    N::NextAdministrationSequence,
                    crate::layout::META_ADMINISTRATION_SEQUENCE.as_bytes(),
                    encode_administration_sequence_allocator_v1(Allocator::next(
                        Admin::new(2).unwrap(),
                    ))
                    .unwrap()
                    .as_bytes(),
                    encode_administration_sequence_allocator_v1(plan.next_administration)
                        .unwrap()
                        .as_bytes(),
                )
                .unwrap();
                let substituted = Receipt::new_for_catalog(
                    plan.receipt.binding(),
                    Source::PrimaryFence,
                    mutations,
                    f.history.lineage().catalog_digest(),
                )
                .unwrap();
                install_receipt(
                    &write,
                    &substituted,
                    f.history.advance(&substituted).unwrap(),
                    plan.next_administration,
                    &plan.admission,
                );
            }
            _ => {
                // A consistent later chain/root still cannot move the application
                // head frozen by the original admission record.
                let predecessor = plan.successor;
                let mutation = Mutation::replace(
                    N::NextApplicationSequence,
                    crate::layout::META_APPLICATION_SEQUENCE.as_bytes(),
                    encode_application_sequence_allocator_v1(
                        riffdb_storage_api::ApplicationSequenceAllocator::next(
                            CommitSequence::new(6).unwrap(),
                        ),
                    )
                    .unwrap()
                    .as_bytes(),
                    encode_application_sequence_allocator_v1(
                        riffdb_storage_api::ApplicationSequenceAllocator::next(
                            CommitSequence::new(7).unwrap(),
                        ),
                    )
                    .unwrap()
                    .as_bytes(),
                )
                .unwrap();
                let receipt = Receipt::new_for_catalog(
                    Binding {
                        database_id: predecessor.lineage().database_id(),
                        history_incarnation: predecessor.lineage().history_incarnation(),
                        predecessor: Some(predecessor.tail().sequence()),
                        sequence: predecessor.tail().sequence().checked_next().unwrap(),
                        predecessor_frontier: predecessor.tail().frontier(),
                        covered_frontier: DualFrontier::new(
                            CommitSequence::new(6),
                            predecessor.tail().frontier().administration(),
                        ),
                        prior_history_hash: predecessor.tail().history_hash(),
                    },
                    Source::DirectApplicationOrServiceAuditGroup,
                    vec![mutation],
                    predecessor.lineage().catalog_digest(),
                )
                .unwrap();
                install_receipt(
                    &write,
                    &receipt,
                    predecessor.advance(&receipt).unwrap(),
                    plan.next_administration,
                    &plan.admission,
                );
            }
        }
        write.commit().unwrap();
        if case >= 8 {
            assert_chain_and_registration(&database);
        }
        let before = snapshot(&database);
        assert!(execute(&f, &database).is_err(), "case {case}");
        super::super::root_validation::assert_source_validation_refuses(&database);
        assert_eq!(snapshot(&database), before, "case {case}");
    }
}

#[test]
fn fence_replay_requires_every_source_metadata_root_without_repair() {
    let f = Fixture::new(5);
    for (key, _) in f.plan().unwrap().metadata(true).unwrap() {
        let scope = crate::test_path::ScopedDirectory::new("primary-fence-replay-missing-root");
        let database = f.database(&scope.join("db.redb"));
        execute(&f, &database).unwrap().commit_for_test().unwrap();
        let write = database.begin_write().unwrap();
        write.open_table(META).unwrap().remove(key).unwrap();
        write.commit().unwrap();
        let before = snapshot(&database);
        assert!(execute(&f, &database).is_err(), "root {key}");
        super::super::root_validation::assert_source_validation_refuses(&database);
        assert_eq!(snapshot(&database), before, "root {key}");
    }
}

fn install_receipt(
    write: &redb::WriteTransaction,
    receipt: &Receipt,
    history: History,
    allocator: Allocator,
    admission: &Admission,
) {
    write
        .open_table(HISTORY)
        .unwrap()
        .insert(
            receipt.binding().sequence.get().to_be_bytes().as_slice(),
            receipt.encode().unwrap().as_slice(),
        )
        .unwrap();
    for (key, value) in transaction::source_metadata(history, allocator, admission).unwrap() {
        write
            .open_table(META)
            .unwrap()
            .insert(key, value.as_bytes())
            .unwrap();
    }
}

fn assert_chain_and_registration(database: &Database) {
    let read = database.begin_read().unwrap();
    let meta = read.open_table(META).unwrap();
    let history = *decode_changelog_history_state_v3(
        meta.get(N::ChangelogHistoryState.metadata_key().unwrap())
            .unwrap()
            .unwrap()
            .value(),
    )
    .unwrap()
    .value();
    let holds = read.open_table(SOURCE_HOLDS).unwrap();
    let receipts = read.open_table(HISTORY).unwrap();
    let audit = read.open_table(AUDIT).unwrap();
    crate::changelog_v3_roots::validate_retained_rows(history, &receipts, &holds).unwrap();
    crate::replication_registration_links::validate(&holds, &audit, history, &receipts).unwrap();
}

#[test]
fn fence_replay_refuses_pruned_original_receipt_even_with_valid_remaining_chain() {
    let f = Fixture::new(5);
    let scope = crate::test_path::ScopedDirectory::new("primary-fence-pruned-proof");
    let database = f.database(&scope.join("db.redb"));
    execute(&f, &database).unwrap().commit_for_test().unwrap();
    let plan = f.plan().unwrap();
    let mut history = plan.successor;
    let write = database.begin_write().unwrap();
    let mut minimum = history.tail();
    for sequence in [10, 11] {
        let receipt = Receipt::new_for_catalog(
            Binding {
                database_id: history.lineage().database_id(),
                history_incarnation: history.lineage().history_incarnation(),
                predecessor: Some(history.tail().sequence()),
                sequence: Sequence::new(sequence).unwrap(),
                predecessor_frontier: history.tail().frontier(),
                covered_frontier: history.tail().frontier(),
                prior_history_hash: history.tail().history_hash(),
            },
            Source::ReplicationSourceHold,
            vec![],
            history.lineage().catalog_digest(),
        )
        .unwrap();
        history = history.advance(&receipt).unwrap();
        if sequence == 10 {
            minimum = history.tail();
        }
        install_receipt(
            &write,
            &receipt,
            history,
            plan.next_administration,
            &plan.admission,
        );
    }
    for sequence in [7u64, 8, 9] {
        write
            .open_table(HISTORY)
            .unwrap()
            .remove(sequence.to_be_bytes().as_slice())
            .unwrap();
    }
    let pruned =
        History::new(history.lineage(), history.anchor(), history.tail(), minimum).unwrap();
    write
        .open_table(META)
        .unwrap()
        .insert(
            N::ChangelogHistoryState.metadata_key().unwrap(),
            encode_changelog_history_state_v3(pruned)
                .unwrap()
                .as_bytes(),
        )
        .unwrap();
    let hold = Hold::new(
        f.policy.hold().id(),
        Kind::FollowerAcknowledgement,
        history.lineage(),
        minimum,
    );
    let policy = Policy::new(
        hold,
        f.policy.registered_at(),
        f.policy.budget(),
        None,
        Phase::Attached,
        None,
    )
    .unwrap();
    write
        .open_table(SOURCE_HOLDS)
        .unwrap()
        .insert(
            hold.storage_key().as_slice(),
            encode_replication_source_hold_v2(policy)
                .unwrap()
                .as_bytes(),
        )
        .unwrap();
    write.commit().unwrap();
    assert_chain_and_registration(&database);
    let before = snapshot(&database);
    assert!(execute(&f, &database).is_err());
    assert_eq!(snapshot(&database), before);
}
