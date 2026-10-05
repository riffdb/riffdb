//! Actual native prune/drain qualification and preservation evidence.
use super::engine_compatibility::restore_fixture_bytes;
use super::*;

// req: STO-030, STO-032, END-008, REC-001
#[test]
fn native_prune_then_outbox_drain_preserves_source_validation_qualification() {
    use riffdb_storage_api::{
        AuthoritativePointReader, OutboxClaimV1, OutboxDestinationIdV1, OutboxPageLimit,
        OutboxRepository, OutboxSucceedV1, OutboxTransitionResultV1, PendingOutboxScanV1,
    };
    use std::sync::atomic::Ordering;
    let directory = crate::test_path::ScopedDirectory::new("prune-outbox-drain-proof");
    restore_fixture_bytes(
        &directory.join(""),
        include_str!(
            "../../../../fixtures/storage/redb-4.2.0-native-commands-and-backup.sparsehex"
        ),
        &[
            "backup/database.redb",
            "backup/format.riffdb",
            "backup/journal.riffextent",
            "backup/manifest.riffdb",
            "db.redb",
            "db.redb.riffdb-format-v1",
            "expected-outcomes.hex",
        ],
    );
    let path = directory.join("db.redb");
    let store =
        RedbStore::open_with_commit_profile(&path, crate::RedbCommitProfile::Standard).unwrap();
    let mut session = store.begin_structural_evidence(inputs()).unwrap();
    let structural = finish_structural(&mut session);
    let (_, historical) = validate_catalog_history(&mut session).unwrap().into_parts();
    let StructuralOpenOutcome::Clean(opened) = session.finish(structural, historical).unwrap()
    else {
        panic!("actual native command source must validate");
    };
    let (_, _, _, dormant) = opened.into_parts();
    let mut ports = dormant.into_operational_after_catalog_validation().unwrap();
    let outcomes_text = std::fs::read_to_string(directory.join("expected-outcomes.hex")).unwrap();
    assert!(outcomes_text.len() <= 64 * 1024);
    let frozen: Vec<_> = outcomes_text
        .lines()
        .take(3)
        .map(|line| {
            assert!(line.len() <= 32 * 1024 && line.len() % 2 == 0);
            let bytes: Vec<_> = line
                .as_bytes()
                .chunks_exact(2)
                .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
                .collect();
            let outcome = codec::decode_stored_outcome_v1(&bytes)
                .unwrap()
                .into_parts()
                .0;
            assert_eq!(
                ports.read_stored_outcome(outcome.identity()).unwrap(),
                Some(outcome.clone())
            );
            let provenance = ports
                .read_provenance(outcome.provenance_id())
                .unwrap()
                .unwrap();
            (outcome, bytes, provenance)
        })
        .collect();
    assert_eq!(frozen.len(), 2);
    let PendingOutboxScanV1::ExactEnd { items } = ports
        .scan_pending_outbox(
            None,
            OutboxPageLimit::new(NonZeroU16::new(8).unwrap()).unwrap(),
        )
        .unwrap()
    else {
        panic!("two-command fixture has a bounded exact pending set");
    };
    assert_eq!(items.len(), 2);
    let first = items[0].value();
    let second = items[1].value();
    let time = Timestamp::new(1_700_000_000, 0).unwrap();
    let lease = Timestamp::new(1_700_000_060, 0).unwrap();
    let claim = OutboxClaimV1::new(
        first.event_id(),
        first.status().clone(),
        OutboxDestinationIdV1::new("test-sink").unwrap(),
        time,
        lease,
    )
    .unwrap();
    assert_eq!(
        ports.claim_outbox(&claim).unwrap(),
        OutboxTransitionResultV1::Applied(claim.updated().clone())
    );
    // Controlled native transition fixture: completion releases only the first
    // event's existing hold. No campaign/archive qualification is claimed.
    let success = OutboxSucceedV1::new(claim.updated().clone(), time).unwrap();
    assert_eq!(
        ports.succeed_outbox(&success).unwrap(),
        OutboxTransitionResultV1::Applied(success.updated().clone())
    );
    let epoch = ports
        .shared
        .source_continuation_epoch
        .load(Ordering::Acquire);
    let before = crate::changelog_v3_roots::validate_retained_history(
        &ports.shared.database.begin_read().unwrap(),
    )
    .unwrap()
    .unwrap();
    let pruned = ports
        .online_retention_step(NonZeroU64::new(1).unwrap(), NonZeroU64::new(1).unwrap())
        .unwrap();
    assert!(matches!(
        pruned,
        crate::OnlineRetentionStep::Advanced {
            through: 1,
            sequences: 1
        }
    ));
    assert_eq!(
        ports
            .shared
            .source_continuation_epoch
            .load(Ordering::Acquire),
        epoch
    );
    let next = OutboxClaimV1::new(
        second.event_id(),
        second.status().clone(),
        OutboxDestinationIdV1::new("test-sink").unwrap(),
        time,
        lease,
    )
    .unwrap();
    assert_eq!(
        ports.claim_outbox(&next).unwrap(),
        OutboxTransitionResultV1::Applied(next.updated().clone())
    );
    let successor_epoch = ports
        .shared
        .source_continuation_epoch
        .load(Ordering::Acquire);
    assert_eq!(
        successor_epoch, epoch,
        "checked canonical prune materialization must not discard the source proof"
    );
    let after = crate::changelog_v3_roots::validate_retained_history(
        &ports.shared.database.begin_read().unwrap(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(after.minimum_resume(), before.minimum_resume());
    assert_eq!(
        after.tail().sequence().get(),
        before.tail().sequence().get() + 2
    );
    assert_eq!(after.tail().frontier(), before.tail().frontier());
    for (outcome, bytes, provenance) in frozen {
        let actual = ports
            .read_stored_outcome(outcome.identity())
            .unwrap()
            .unwrap();
        assert_eq!(actual, outcome);
        assert_eq!(
            codec::encode_stored_outcome_v1(&actual).unwrap().as_bytes(),
            bytes
        );
        assert_eq!(
            ports.read_provenance(outcome.provenance_id()).unwrap(),
            Some(provenance)
        );
    }
}
