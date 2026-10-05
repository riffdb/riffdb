//! Real acknowledged journal transitions, source admission and semantic oracles.
use super::engine_compatibility::restore_fixture_bytes;
use super::*;
use riffdb_storage_api::AuthoritativePointReader;

fn fixture() -> (
    crate::test_path::ScopedDirectory,
    crate::RedbOperationalPorts,
) {
    let directory = crate::test_path::ScopedDirectory::new("source-admission");
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
    let ports = open_existing(&directory.join(""));
    (directory, ports)
}

fn open_existing(directory: &std::path::Path) -> crate::RedbOperationalPorts {
    open_existing_at(directory, None)
}

fn open_existing_at(
    directory: &std::path::Path,
    expected_tail: Option<u64>,
) -> crate::RedbOperationalPorts {
    let store = RedbStore::open_with_commit_profile(
        directory.join("db.redb"),
        crate::RedbCommitProfile::Standard,
    )
    .unwrap();
    if let Some(expected_tail) = expected_tail {
        // Check recovered authority before startup publishes its own closed
        // lifecycle receipts. The complete validator proves the native point.
        let recovered = crate::changelog_v3_roots::validate_retained_history(
            &store.shared.database.begin_read().unwrap(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(recovered.tail().sequence().get(), expected_tail);
    }
    let mut session = store.begin_structural_evidence(inputs()).unwrap();
    let structural = finish_structural(&mut session);
    let (_, historical) = validate_catalog_history(&mut session).unwrap().into_parts();
    let StructuralOpenOutcome::Clean(opened) = session.finish(structural, historical).unwrap()
    else {
        panic!("genuine predecessor source must validate");
    };
    let (_, _, _, dormant) = opened.into_parts();
    dormant.into_operational_after_catalog_validation().unwrap()
}

// req: STO-030, STO-031, STO-032, END-008, REC-001, LOG-001
#[test]
fn source_capture_drains_one_real_published_prune_without_reclaiming_or_changing_outcomes() {
    let (directory, mut ports) = fixture();
    let frozen: Vec<_> = std::fs::read_to_string(directory.join("expected-outcomes.hex"))
        .unwrap()
        .lines()
        .take(3)
        .map(|line| {
            assert!(line.len() <= 32 * 1024 && line.len().is_multiple_of(2));
            let bytes: Vec<_> = line
                .as_bytes()
                .chunks_exact(2)
                .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
                .collect();
            let outcome = codec::decode_stored_outcome_v1(&bytes)
                .unwrap()
                .into_parts()
                .0;
            let provenance = ports
                .read_provenance(outcome.provenance_id())
                .unwrap()
                .unwrap();
            (outcome, bytes, provenance)
        })
        .collect();
    assert_eq!(frozen.len(), 2);
    complete_first_event(&mut ports);
    let before = crate::changelog_v3_roots::validate_retained_history(
        &ports.shared.database.begin_read().unwrap(),
    )
    .unwrap()
    .unwrap();
    let epoch = ports
        .shared
        .source_continuation_epoch
        .load(Ordering::Acquire);
    assert!(matches!(
        ports
            .online_retention_step(NonZeroU64::new(1).unwrap(), NonZeroU64::new(1).unwrap())
            .unwrap(),
        crate::OnlineRetentionStep::Advanced {
            through: 1,
            sequences: 1
        }
    ));
    assert_eq!(ports.shared.journal_suffix_activity().unwrap().1, 1);
    // Settle the existing off-gate extent publication explicitly. Production
    // admission must yield to an unfinished sealer rather than join it.
    ports.shared.join_journal_sealer();
    let mut source = ports.replication_source_control();
    let (removed, _) = source.reclaim_history_online().unwrap();
    assert_eq!(removed, 0, "first capture has no reclamation authority");
    assert_eq!(
        ports.shared.journal_suffix_activity().unwrap().1,
        0,
        "source admission must durably materialize this bounded acknowledged suffix; turn census {:?}",
        source.source_admission_census()
    );
    assert_eq!(
        ports
            .shared
            .source_continuation_epoch
            .load(Ordering::Acquire),
        epoch
    );
    let after = crate::changelog_v3_roots::validate_retained_history(
        &ports.shared.database.begin_read().unwrap(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(after.minimum_resume(), before.minimum_resume());
    assert_eq!(
        after.tail().sequence().get(),
        before.tail().sequence().get() + 1
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

fn complete_first_event(ports: &mut crate::RedbOperationalPorts) {
    use riffdb_storage_api::{
        OutboxClaimV1, OutboxDestinationIdV1, OutboxPageLimit, OutboxRepository, OutboxSucceedV1,
        OutboxTransitionResultV1, PendingOutboxScanV1,
    };
    let PendingOutboxScanV1::ExactEnd { items } = ports
        .scan_pending_outbox(
            None,
            OutboxPageLimit::new(NonZeroU16::new(8).unwrap()).unwrap(),
        )
        .unwrap()
    else {
        panic!("two-command fixture has an exact pending set");
    };
    assert_eq!(items.len(), 2);
    let first = items[0].value();
    let time = Timestamp::new(1_700_000_000, 0).unwrap();
    let claim = OutboxClaimV1::new(
        first.event_id(),
        first.status().clone(),
        OutboxDestinationIdV1::new("test-sink").unwrap(),
        time,
        Timestamp::new(1_700_000_060, 0).unwrap(),
    )
    .unwrap();
    assert_eq!(
        ports.claim_outbox(&claim).unwrap(),
        OutboxTransitionResultV1::Applied(claim.updated().clone())
    );
    let success = OutboxSucceedV1::new(claim.updated().clone(), time).unwrap();
    assert_eq!(
        ports.succeed_outbox(&success).unwrap(),
        OutboxTransitionResultV1::Applied(success.updated().clone())
    );
}

// req: STO-030, STO-031, STO-032, REC-001, LOG-001
#[test]
fn source_drain_crash_child() {
    let Some(directory) = std::env::var_os("RIFFDB_NATIVE_SOURCE_DRAIN_PATH") else {
        return;
    };
    let mut ports = open_existing(std::path::Path::new(&directory));
    complete_first_event(&mut ports);
    let prior = crate::changelog_v3_roots::validate_retained_history(
        &ports.shared.database.begin_read().unwrap(),
    )
    .unwrap()
    .unwrap();
    // Freeze the actual pre-prune point plus the single acknowledged prune,
    // rather than count unrelated startup lifecycle receipts as workload.
    std::fs::write(
        std::path::Path::new(&directory).join("expected-source-tail"),
        (prior.tail().sequence().get() + 1).to_be_bytes(),
    )
    .unwrap();
    assert!(matches!(
        ports
            .online_retention_step(NonZeroU64::new(1).unwrap(), NonZeroU64::new(1).unwrap())
            .unwrap(),
        crate::OnlineRetentionStep::Advanced { .. }
    ));
    assert_eq!(ports.shared.journal_suffix_activity().unwrap().1, 1);
    ports.shared.join_journal_sealer();
    let mut source = ports.replication_source_control();
    source.reclaim_history_online().unwrap();
    panic!("selected process crash edge must execute");
}

// req: STO-030, STO-031, STO-032, REC-001, LOG-001
#[test]
fn source_drain_process_crashes_preserve_acknowledged_prune_outcomes_and_provenance() {
    for edge in [
        "source-drain-staged",
        "source-drain-committed",
        "source-drain-anchored",
    ] {
        let (directory, ports) = fixture();
        let before = crate::changelog_v3_roots::validate_retained_history(
            &ports.shared.database.begin_read().unwrap(),
        )
        .unwrap()
        .unwrap();
        let mut frozen = Vec::new();
        for line in std::fs::read_to_string(directory.join("expected-outcomes.hex"))
            .unwrap()
            .lines()
        {
            assert!(line.len() <= 32 * 1024 && line.len().is_multiple_of(2));
            let bytes: Vec<_> = line
                .as_bytes()
                .chunks_exact(2)
                .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
                .collect();
            let outcome = codec::decode_stored_outcome_v1(&bytes)
                .unwrap()
                .into_parts()
                .0;
            let provenance = ports
                .read_provenance(outcome.provenance_id())
                .unwrap()
                .unwrap();
            frozen.push((outcome, bytes, provenance));
        }
        assert_eq!(frozen.len(), 2);
        drop(ports);
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "startup::tests::native_source_admission::source_drain_crash_child",
                "--nocapture",
            ])
            .env("RIFFDB_NATIVE_SOURCE_DRAIN_PATH", directory.join(""))
            .env("RIFFDB_V3_SOURCE_CONTROL_EDGE", edge)
            .output()
            .unwrap();
        assert_eq!(
            result.status.code(),
            Some(93),
            "edge {edge}; child {}",
            String::from_utf8_lossy(&result.stderr)
        );
        // Reopen through the normal complete startup/catalog proof, including
        // canonical replay if the engine commit or journal anchor was absent.
        let expected = std::fs::read(directory.join("expected-source-tail")).unwrap();
        assert_eq!(expected.len(), 8);
        let expected_tail = u64::from_be_bytes(expected.try_into().unwrap());
        let ports = open_existing_at(&directory.join(""), Some(expected_tail));
        let after = crate::changelog_v3_roots::validate_retained_history(
            &ports.shared.database.begin_read().unwrap(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(after.minimum_resume(), before.minimum_resume());
        assert!(after.tail().sequence().get() >= expected_tail);
        assert_eq!(
            after.tail().frontier().application(),
            before.tail().frontier().application()
        );
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
}
