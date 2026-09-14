//! Refusals and admission races cannot weaken retained source fences.
// req: REP-003, REC-001, PERF-007, STO-012

use super::*;
use crate::{
    changelog_v3_activation::{HISTORY, SOURCE_HOLDS},
    layout::META,
};
use redb::ReadableTableMetadata;
use riffdb_storage_api::{
    AuthoritativeNamespaceV1 as N, AuthoritativeTransactionBindingV3 as Binding,
    AuthoritativeTransactionV3 as Receipt, ChangelogCursorErrorV3 as Refusal,
    ChangelogHistoryPointV3 as Point, ChangelogTransactionSequence as Sequence, StorageErrorKind,
    proto_codec::*,
};

#[test]
fn source_hold_refuses_substitution_foreign_epoch_regression_and_unknown_rows_without_writes() {
    let (_scope, ports, states) = fixture();
    let mut control = ports.replication_source_control();
    let fence = hold(Kind::FollowerAcknowledgement, states[3]);
    control.register(fence).unwrap();
    let expected = history(&ports);
    let epoch = ports.shared.durable_commit_epoch();
    let foreign = ChangelogLineageV3::new(
        fence.lineage().database_id(),
        2,
        LeadershipEpochV1::initial(),
    )
    .unwrap();
    let stale = ChangelogLineageV3::new(
        fence.lineage().database_id(),
        1,
        LeadershipEpochV1::new(2).unwrap(),
    )
    .unwrap();
    for (lineage, outcome) in [
        (foreign, Refusal::ForeignLineage),
        (stale, Refusal::StaleEpoch),
    ] {
        assert_eq!(
            control.register(Hold::new(fence.id(), fence.kind(), lineage, fence.fence())),
            Err(outcome)
        );
    }
    let substitute = Point::new(
        fence.fence().sequence(),
        [0x99; 32],
        fence.fence().frontier(),
    );
    assert_eq!(
        control.register(Hold::new(
            fence.id(),
            fence.kind(),
            fence.lineage(),
            substitute
        )),
        Err(Refusal::InvalidPosition)
    );
    assert_eq!(
        control.advance_acknowledgement(hold(Kind::FollowerAcknowledgement, states[2])),
        Err(Refusal::InvalidPosition)
    );
    assert_eq!(
        control.advance_acknowledgement(hold(Kind::ArchiveAcknowledgement, states[3])),
        Err(Refusal::InvalidPosition)
    );
    assert_eq!(history(&ports), expected);
    assert_eq!(ports.shared.durable_commit_epoch(), epoch);
    let transaction = ports.shared.database.begin_write().unwrap();
    transaction
        .open_table(SOURCE_HOLDS)
        .unwrap()
        .insert(
            b"unknown-source-kind".as_slice(),
            b"invalid-source-hold".as_slice(),
        )
        .unwrap();
    ports.shared.commit_durable(transaction).unwrap();
    let epoch = ports.shared.durable_commit_epoch();
    assert!(matches!(control.register(fence), Err(Refusal::Storage(_))));
    assert!(matches!(
        control.reclaim_history(),
        Err(Refusal::Storage(_))
    ));
    assert_eq!(
        ports.shared.durable_commit_epoch(),
        epoch,
        "even equal retry validates all registered fences first"
    );
    assert_eq!(
        crate::changelog_v3_roots::read_checkpoint_roots(
            &ports.shared.database.begin_read().unwrap()
        )
        .unwrap(),
        Some(expected)
    );
}

#[test]
fn source_hold_population_bound_includes_all_kinds_and_refuses_before_allocation() {
    let (_scope, ports, states) = fixture();
    let transaction = ports.shared.database.begin_write().unwrap();
    let mut first = None;
    {
        let mut table = transaction.open_table(SOURCE_HOLDS).unwrap();
        for id in 1_u128..=u128::from(riffdb_storage_api::MAX_REPLICATION_SOURCE_HOLDS_V1) {
            let kind = match id % 3 {
                0 => Kind::FollowerAcknowledgement,
                1 => Kind::ArchiveAcknowledgement,
                _ => Kind::Bootstrap,
            };
            let fence = Hold::new(
                ReplicationSourceHoldIdV1::new(id.to_be_bytes()).unwrap(),
                kind,
                states[0].lineage(),
                states[0].tail(),
            );
            first.get_or_insert(fence);
            let bytes = encode_replication_source_hold_v1(fence).unwrap();
            table
                .insert(fence.storage_key().as_slice(), bytes.as_bytes())
                .unwrap();
        }
    }
    ports.shared.commit_durable(transaction).unwrap();
    let expected = history(&ports);
    let epoch = ports.shared.durable_commit_epoch();
    let mut control = ports.replication_source_control();
    assert!(!control.register(first.unwrap()).unwrap());
    assert!(
        matches!(control.register(hold(Kind::FollowerAcknowledgement, states[0])),
        Err(Refusal::Storage(error)) if error.kind() == StorageErrorKind::LimitExceeded)
    );
    assert_eq!(history(&ports), expected);
    assert_eq!(ports.shared.durable_commit_epoch(), epoch);
}

fn exhaust_allocator(ports: &RedbOperationalPorts) {
    let old = history(ports);
    let mut prior_hash = [0x95; 32];
    let mut receipts = Vec::new();
    for sequence in [u64::MAX - 1, u64::MAX] {
        let receipt = Receipt::new(
            Binding {
                database_id: old.lineage().database_id(),
                history_incarnation: old.lineage().history_incarnation(),
                predecessor: Sequence::new(sequence - 1),
                sequence: Sequence::new(sequence).unwrap(),
                predecessor_frontier: old.tail().frontier(),
                covered_frontier: old.tail().frontier(),
                prior_history_hash: prior_hash,
            },
            ChangelogAttributionV3::ReplicationSourceHold,
            Vec::new(),
        )
        .unwrap();
        prior_hash = receipt.history_hash().unwrap();
        receipts.push(receipt);
    }
    let exhausted = ChangelogHistoryStateV3::new(
        old.lineage(),
        old.anchor(),
        Point::from_receipt(&receipts[1]).unwrap(),
        Point::from_receipt(&receipts[0]).unwrap(),
    )
    .unwrap();
    let transaction = ports.shared.database.begin_write().unwrap();
    {
        let mut table = transaction.open_table(HISTORY).unwrap();
        let keys: Vec<_> = table
            .iter()
            .unwrap()
            .map(|row| row.unwrap().0.value().to_vec())
            .collect();
        for key in keys {
            table.remove(key.as_slice()).unwrap();
        }
        for receipt in receipts {
            table
                .insert(
                    receipt.binding().sequence.get().to_be_bytes().as_slice(),
                    receipt.encode().unwrap().as_slice(),
                )
                .unwrap();
        }
        let mut meta = transaction.open_table(META).unwrap();
        for (namespace, bytes) in [
            (
                N::ChangelogHistoryState,
                encode_changelog_history_state_v3(exhausted)
                    .unwrap()
                    .into_bytes(),
            ),
            (
                N::NextChangelogTransaction,
                encode_changelog_transaction_allocator_v3(exhausted.expected_allocator())
                    .unwrap()
                    .into_bytes(),
            ),
        ] {
            meta.insert(namespace.metadata_key().unwrap(), bytes.as_slice())
                .unwrap();
        }
    }
    ports.shared.commit_durable(transaction).unwrap();
    assert_eq!(history(ports), exhausted);
}

#[test]
fn exhausted_control_allocator_refuses_hold_and_prune_before_any_mutation() {
    let (_scope, ports, _) = fixture();
    exhaust_allocator(&ports);
    let expected = history(&ports);
    let mut control = ports.replication_source_control();
    assert!(!control.reclaim_history().unwrap());
    // A later known-durable checkpoint with unchanged authoritative state.
    ports
        .shared
        .commit_durable(ports.shared.database.begin_write().unwrap())
        .unwrap();
    let epoch = ports.shared.durable_commit_epoch();
    for result in [
        control.register(hold(Kind::FollowerAcknowledgement, expected)),
        control.reclaim_history(),
    ] {
        assert!(
            matches!(result, Err(Refusal::Storage(error)) if error.kind() == StorageErrorKind::SequenceExhausted)
        );
    }
    assert_eq!(history(&ports), expected);
    assert_eq!(ports.shared.durable_commit_epoch(), epoch);
    assert!(
        ports
            .shared
            .database
            .begin_read()
            .unwrap()
            .open_table(SOURCE_HOLDS)
            .unwrap()
            .is_empty()
            .unwrap()
    );
}

#[test]
fn concurrent_equal_source_registrations_have_one_durable_winner() {
    let (_scope, ports, states) = fixture();
    let expected = history(&ports);
    let gate = std::sync::Barrier::new(4);
    let hold = hold(Kind::Bootstrap, states[3]);
    let winners = std::thread::scope(|scope| {
        let jobs: Vec<_> = (0..4)
            .map(|_| {
                let mut control = ports.replication_source_control();
                let gate = &gate;
                scope.spawn(move || {
                    gate.wait();
                    control.register(hold).unwrap()
                })
            })
            .collect();
        jobs.into_iter()
            .map(|job| usize::from(job.join().unwrap()))
            .sum::<usize>()
    });
    assert_eq!(winners, 1);
    assert_eq!(
        history(&ports).tail().sequence().get(),
        expected.tail().sequence().get() + 1
    );
}

#[test]
fn retained_control_handles_refuse_after_clean_or_writer_fencing() {
    for clean in [true, false] {
        let (_scope, ports, states) = fixture();
        let mut control = ports.replication_source_control();
        if clean {
            // A valid CLEAN encoding is enough to refuse; it need not be a
            // serving-readiness certificate for this opaque physical fixture.
            let value = crate::clean_close::CleanCloseLifecycle::clean(
                states[0].lineage().database_id(),
                1,
                1,
                [0x51; 32],
            )
            .unwrap()
            .encode()
            .unwrap();
            let transaction = ports.shared.database.begin_write().unwrap();
            transaction
                .open_table(META)
                .unwrap()
                .insert(crate::layout::META_CLEAN_CLOSE_LIFECYCLE, value.as_slice())
                .unwrap();
            ports.shared.commit_durable(transaction).unwrap();
        } else {
            ports.shared.fence_writes();
        }
        let epoch = ports.shared.durable_commit_epoch();
        let expected = history(&ports);
        for result in [
            control.register(hold(Kind::Bootstrap, states[0])),
            control.reclaim_history(),
        ] {
            assert!(
                matches!(result, Err(Refusal::Storage(error)) if error.kind() == StorageErrorKind::Unavailable)
            );
        }
        assert_eq!(history(&ports), expected);
        assert_eq!(ports.shared.durable_commit_epoch(), epoch);
    }
}
