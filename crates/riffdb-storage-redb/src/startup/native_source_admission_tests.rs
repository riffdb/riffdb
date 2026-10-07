//! Real acknowledged journal transitions, source admission and semantic oracles.
use super::engine_compatibility::restore_fixture_bytes;
use super::*;
use riffdb_storage_api::AuthoritativePointReader;

fn fixture() -> (
    crate::test_path::ScopedDirectory,
    crate::RedbOperationalPorts,
) {
    let directory = restored_fixture();
    upgrade_fixture(&directory.join(""));
    let ports = open_existing(&directory.join(""));
    (directory, ports)
}

fn restored_fixture() -> crate::test_path::ScopedDirectory {
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
    directory
}

pub(super) fn upgrade_fixture(directory: &std::path::Path) {
    crate::RedbDurableFormatUpgrade::bind(directory.join("db.redb"), directory.join("backup"))
        .run()
        .expect("explicit backup-bound predecessor upgrade");
}

// req: AFC-001, AFC-002, REC-005, REC-006, REC-007, STO-032, END-008
#[test]
fn registry_completion_writer_upgrade_preserves_predecessor_and_refuses_old_binary() {
    let directory = restored_fixture();
    let database = directory.join("db.redb");
    let marker_path = crate::durable_format_marker_path(&database);
    let old_marker_bytes = std::fs::read(&marker_path).unwrap();
    let old_marker = riffdb_storage_api::decode_durable_format_marker(&old_marker_bytes).unwrap();
    assert_eq!(old_marker.identity().writer().get(), 2);
    let before = std::fs::read(&database).unwrap();
    assert_eq!(
        RedbStore::open(&database).err().unwrap().kind(),
        StorageErrorKind::IncompatibleFormat
    );
    assert_eq!(std::fs::read(&database).unwrap(), before);
    assert_eq!(std::fs::read(&marker_path).unwrap(), old_marker_bytes);
    assert!(
        crate::RedbDurableFormatUpgrade::bind(&database, directory.join("missing-backup"))
            .run()
            .is_err()
    );
    assert_eq!(std::fs::read(&database).unwrap(), before);
    let backup_before = std::fs::read(directory.join("backup/database.redb")).unwrap();
    let backup_marker_before = std::fs::read(directory.join("backup/format.riffdb")).unwrap();
    upgrade_fixture(&directory.join(""));
    assert_eq!(
        std::fs::read(directory.join("backup/database.redb")).unwrap(),
        backup_before
    );
    assert_eq!(
        std::fs::read(directory.join("backup/format.riffdb")).unwrap(),
        backup_marker_before
    );
    let current =
        riffdb_storage_api::decode_durable_format_marker(&std::fs::read(&marker_path).unwrap())
            .unwrap();
    assert_eq!(current.identity().writer().get(), 3);
    // These are distinct physical identities. The frozen predecessor pre-open
    // policy is exercised by frozen_writer_two_preopen_policy_refuses_writer_three
    // in the API crate, using the same source-free classifier as production.
    assert_ne!(current.identity(), old_marker.identity());
    assert_ne!(current.registry_digest(), old_marker.registry_digest());
    let store = RedbStore::open(&database).unwrap();
    let read = store.shared.database.begin_read().unwrap();
    let meta = read.open_table(META).unwrap();
    let registry = riffdb_storage_api::proto_codec::decode_record_registry(
        meta.get(META_RECORD_REGISTRY).unwrap().unwrap().value(),
    )
    .unwrap();
    assert_eq!(
        registry.value().validation(),
        riffdb_storage_api::proto_codec::RecordRegistryValidation::Pending
    );
    drop(meta);
    drop(read);
    drop(store);
    let _ports = open_existing(&directory.join(""));
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
    let (_, historical) = session
        .validate_startup_catalog_history()
        .unwrap()
        .into_parts();
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

// req: END-007, END-008, STO-030, STO-032
#[test]
fn ordinary_retention_steps_observe_physical_bytes_without_a_checkpoint_wait() {
    let (_scope, mut ports) = fixture();
    complete_first_event(&mut ports);
    assert!(matches!(
        ports
            .online_retention_step(NonZeroU64::MIN, NonZeroU64::MIN)
            .unwrap(),
        crate::OnlineRetentionStep::Advanced { through: 1, .. }
    ));
    ports.shared.join_journal_sealer();
    let lane = ports.online_retention_lane();
    assert_eq!(lane.physical_observations_for_test(), 0);
    for _ in 0..4 {
        lane.step(
            NonZeroU64::new(16_384).unwrap(),
            NonZeroU64::new(1).unwrap(),
        )
        .unwrap();
    }
    assert_eq!(
        lane.physical_observations_for_test(),
        1,
        "ordinary retention must complete a physical census without checkpoint-wait callbacks"
    );
}

// req: STO-030, STO-031, END-007, END-008
#[test]
fn captured_physical_pages_finish_off_gate_without_writer_turns_or_new_captures() {
    let (scope, mut ports) = fixture();
    complete_first_event(&mut ports);
    assert!(matches!(
        ports
            .online_retention_step(NonZeroU64::MIN, NonZeroU64::MIN)
            .unwrap(),
        crate::OnlineRetentionStep::Advanced { through: 1, .. }
    ));
    ports.shared.join_journal_sealer();
    // Actual media-directory pagination above the one-second-per-page
    // policy's 30-page lifetime, without synthetic measurement records.
    for index in 0..2048 {
        std::fs::write(scope.join(&format!("unrelated-{index}")), []).unwrap();
    }
    let lane = ports.online_retention_lane();
    assert!(!lane.physical_observation_pending().unwrap());
    assert!(!lane.advance_physical_observation_off_gate().unwrap());
    lane.step(NonZeroU64::new(16_384).unwrap(), NonZeroU64::MIN)
        .unwrap();
    assert!(lane.physical_observation_pending().unwrap());
    let epoch = ports.shared.durable_commit_epoch();
    let mut pages = 0;
    while lane.physical_observation_pending().unwrap() {
        assert!(
            pages < 34,
            "the complete bounded directory must reach exact end"
        );
        lane.advance_physical_observation_off_gate().unwrap();
        pages += 1;
    }
    assert!(pages >= 31);
    assert_eq!(lane.physical_observations_for_test(), 1);
    assert_eq!(ports.shared.durable_commit_epoch(), epoch);
    assert!(!lane.advance_physical_observation_off_gate().unwrap());
    assert_eq!(lane.physical_observations_for_test(), 1);
}

// req: REC-005, REC-006, REC-007, STO-030, STO-032, END-008
#[test]
fn pruned_v2_startup_uses_registry_completion_without_population_walk() {
    let directory = restored_fixture();
    upgrade_fixture(&directory.join(""));
    // Remove only optional predecessor shortcut evidence so this fixture earns
    // completion from a real full structural and catalog-semantic pass.
    {
        let database = redb::Database::open(directory.join("db.redb")).unwrap();
        let write = database.begin_write().unwrap();
        write
            .open_table(META)
            .unwrap()
            .remove("validated_prefix_checkpoint/v1")
            .unwrap();
        write.commit().unwrap();
    }
    let mut ports = open_existing(&directory.join(""));
    assert!(ports.shared.startup_validation_clean());
    complete_first_event(&mut ports);
    assert!(matches!(
        ports
            .online_retention_step(NonZeroU64::MIN, NonZeroU64::MIN)
            .unwrap(),
        crate::OnlineRetentionStep::Advanced {
            through: 1,
            sequences: 1
        }
    ));
    drop(ports);
    // Reopen reconciles the acknowledged journaled prune before inspecting
    // physical metadata; no prefix refresh or further validation has run.
    let store = RedbStore::open(directory.join("db.redb")).unwrap();
    {
        let read = store.shared.database.begin_read().unwrap();
        let meta = read.open_table(META).unwrap();
        assert!(
            meta.get("validated_prefix_checkpoint/v1")
                .unwrap()
                .is_none()
        );
        let history = crate::changelog_v3_roots::read_checkpoint_roots(&read)
            .unwrap()
            .unwrap();
        assert_eq!(
            history.lineage().catalog_digest(),
            riffdb_storage_api::AuthoritativeStateCatalogV2.digest()
        );
    }
    let mut session = store.begin_structural_evidence(inputs()).unwrap();
    assert!(!session.clean_close_fast_path());
    assert!(
        session.bounded_dirty_startup_path(),
        "online prune must preserve completed registry eligibility after deleting the prefix checkpoint"
    );
    let structural = finish_structural(&mut session);
    let (catalog, historical) = session
        .validate_startup_catalog_history()
        .unwrap()
        .into_parts();
    assert!(matches!(catalog, CatalogHistoryOutcome::Ready(_)));
    assert_eq!(session.command_history_rows_decoded(), 0);
    assert_eq!(
        session.walked_suffix_counts[3..],
        [0; STRUCTURAL_TABLE_COUNT - 3]
    );
    assert_eq!(
        session.walked_prefix_counts[3..],
        [0; STRUCTURAL_TABLE_COUNT - 3]
    );
    let StructuralOpenOutcome::Clean(opened) = session.finish(structural, historical).unwrap()
    else {
        panic!("pruned current-format source must remain operational");
    };
    let (_, _, _, dormant) = opened.into_parts();
    let ports = dormant.into_operational_after_catalog_validation().unwrap();
    assert_eq!(ports.transient_index_rebuilds(), 0);
    assert!(ports.clean_close_fast_startup());
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

// req: AFC-001, AFC-002, REC-005, REC-006, REC-007, STO-032, END-008
#[test]
fn registry_completion_upgrade_process_child() {
    let Some(path) = std::env::var_os("RIFFDB_REGISTRY_UPGRADE_CRASH_PATH") else {
        return;
    };
    upgrade_fixture(&std::path::PathBuf::from(path));
}

// req: AFC-001, AFC-002, REC-005, REC-006, REC-007, STO-032, END-008
#[test]
fn registry_completion_upgrade_crash_retry_never_grants_completed_eligibility() {
    for (variable, edge, exit_code) in [
        ("RIFFDB_FORMAT_UPGRADE_EDGE", "accepted", 95),
        ("RIFFDB_FORMAT_UPGRADE_EDGE", "migration", 95),
        ("RIFFDB_FORMAT_UPGRADE_EDGE", "migrated", 95),
        ("RIFFDB_FORMAT_UPGRADE_EDGE", "marker", 95),
        ("RIFFDB_FORMAT_UPGRADE_EDGE", "complete", 95),
        ("RIFFDB_V3_DIRECT_EDGE", "mutations", 93),
        ("RIFFDB_V3_DIRECT_EDGE", "receipt", 93),
        ("RIFFDB_V3_DIRECT_EDGE", "roots", 93),
        ("RIFFDB_V3_DIRECT_EDGE", "committed", 93),
    ] {
        let directory = restored_fixture();
        let backup_before = std::fs::read(directory.join("backup/database.redb")).unwrap();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("startup::tests::native_source_admission::registry_completion_upgrade_process_child")
            .args(["--exact", "--nocapture"])
            .env("RIFFDB_REGISTRY_UPGRADE_CRASH_PATH", directory.join(""))
            .env(variable, edge).status().unwrap();
        assert_eq!(status.code(), Some(exit_code), "{variable}/{edge}");
        upgrade_fixture(&directory.join(""));
        upgrade_fixture(&directory.join(""));
        assert_eq!(
            std::fs::read(directory.join("backup/database.redb")).unwrap(),
            backup_before
        );
        let store = RedbStore::open(directory.join("db.redb")).unwrap();
        let read = store.shared.database.begin_read().unwrap();
        let meta = read.open_table(META).unwrap();
        let row = meta.get(META_RECORD_REGISTRY).unwrap().unwrap();
        let registry = riffdb_storage_api::decode_record_registry(row.value()).unwrap();
        assert_eq!(
            registry.value().validation(),
            riffdb_storage_api::RecordRegistryValidation::Pending,
            "{variable}/{edge}"
        );
    }
}
