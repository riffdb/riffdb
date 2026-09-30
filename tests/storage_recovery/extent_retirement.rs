//! Whole-file retirement after legitimate application and receipt reclamation.
// req: STO-030, STO-031, STO-032, REC-001, REC-002

use super::*;
use riffdb_storage_redb::{OnlineRetentionStep, source_history_reclamation_fixture};
use std::num::NonZeroU64;

const MODE: &str = "RIFFDB_EXTENT_RETIREMENT_CHILD_MODE";
const SOURCE: &str = "RIFFDB_EXTENT_RETIREMENT_CHILD_PATH";

#[test]
fn extent_retirement_crash_child() {
    let Ok(mode) = std::env::var(MODE) else {
        return;
    };
    let path = PathBuf::from(std::env::var_os(SOURCE).unwrap());
    let (operation, after) = match mode.as_str() {
        "before-file" => (RedbTestOperation::RetentionExtentRetire, false),
        "after-file" => (RedbTestOperation::RetentionExtentRetire, true),
        "before-directory" => (RedbTestOperation::RetentionExtentDirectory, false),
        "after-directory" => (RedbTestOperation::RetentionExtentDirectory, true),
        _ => panic!("unknown fixture edge"),
    };
    let controller = if after {
        RedbTestController::abort_after_commit(operation)
    } else {
        RedbTestController::abort_before_commit(operation)
    };
    let ports = open_operational(RedbStore::open_with_test_controller(&path, controller).unwrap());
    ports
        .online_retention_lane()
        .step(NonZeroU64::new(2).unwrap(), NonZeroU64::new(256).unwrap())
        .unwrap();
    panic!("the armed physical retirement did not terminate the child");
}

#[test]
fn physical_retirement_crash_windows_preserve_every_authoritative_frontier() {
    for mode in [
        "before-file",
        "after-file",
        "before-directory",
        "after-directory",
    ] {
        let path = TestDatabasePath::new("extent-retirement-crash");
        let extent = retained_extents::prepare_two_sealed_commands(&path.0);
        {
            let ports = open_operational(RedbStore::open(&path.0).unwrap());
            commit_command_group(&ports, &[command_fixture_at(3)]);
            assert_eq!(
                ports
                    .online_retention_step(
                        NonZeroU64::new(1).unwrap(),
                        NonZeroU64::new(256).unwrap()
                    )
                    .unwrap(),
                OnlineRetentionStep::Advanced {
                    through: 2,
                    sequences: 2
                }
            );
            let mut source = source_history_reclamation_fixture(&ports);
            assert!(!source.step().unwrap());
            commit_command_group(&ports, &[command_fixture_at(4)]);
            assert!(source.step().unwrap());
        }
        assert!(
            extent.exists(),
            "the parent leaves the eligible file intact"
        );
        let before =
            riffdb_storage_redb::benchmark_support::recovery_authority_snapshot_v1(&path.0)
                .unwrap();
        let status = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "extent_retirement::extent_retirement_crash_child",
            ])
            .env(MODE, mode)
            .env(SOURCE, &path.0)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        assert_child_aborted(status, mode);
        let findings = collect_structural_findings(RedbStore::open(&path.0).unwrap());
        assert!(findings.is_empty(), "{mode}: {findings:?}");
        let after = riffdb_storage_redb::benchmark_support::recovery_authority_snapshot_v1(&path.0)
            .unwrap();
        assert!(
            before.same_authority(&after),
            "{mode}: file reclamation advanced authority"
        );
        assert_eq!(
            retention_tombstones(&path.0)
                .last()
                .unwrap()
                .last_sequence(),
            2
        );
        let ports = open_operational(RedbStore::open(&path.0).unwrap());
        assert!(matches!(
            ports
                .lookup_admission(command_fixture_at(1).candidates)
                .unwrap(),
            AdmissionLookupResultV1::Found(_)
        ));
        assert!(
            ports
                .read_commit(CommitSequence::new(4).unwrap())
                .unwrap()
                .is_some()
        );
        let lane = ports.online_retention_lane();
        for _ in 0..3 {
            lane.step(NonZeroU64::new(2).unwrap(), NonZeroU64::new(256).unwrap())
                .unwrap();
        }
        assert!(
            !extent.exists(),
            "{mode}: the bounded lane resumes retirement"
        );
    }
}

#[test]
fn an_unreferenced_extent_with_a_corrupt_footer_is_never_retired() {
    use std::io::{Read, Seek, SeekFrom, Write};
    let path = TestDatabasePath::new("extent-retirement-corrupt-footer");
    let extent = retained_extents::prepare_two_sealed_commands(&path.0);
    let ports = open_operational(RedbStore::open(&path.0).unwrap());
    commit_command_group(&ports, &[command_fixture_at(3)]);
    assert!(matches!(
        ports
            .online_retention_step(NonZeroU64::new(1).unwrap(), NonZeroU64::new(256).unwrap())
            .unwrap(),
        OnlineRetentionStep::Advanced {
            through: 2,
            sequences: 2
        }
    ));
    let mut source = source_history_reclamation_fixture(&ports);
    assert!(!source.step().unwrap());
    commit_command_group(&ports, &[command_fixture_at(4)]);
    assert!(source.step().unwrap());
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&extent)
        .unwrap();
    file.seek(SeekFrom::End(-1)).unwrap();
    let mut original = [0];
    file.read_exact(&mut original).unwrap();
    file.seek(SeekFrom::End(-1)).unwrap();
    file.write_all(&[original[0] ^ 1]).unwrap();
    file.sync_all().unwrap();
    let lane = ports.online_retention_lane();
    let error = lane
        .step(NonZeroU64::new(2).unwrap(), NonZeroU64::new(256).unwrap())
        .unwrap_err();
    assert_eq!(
        error.kind(),
        riffdb_storage_api::StorageErrorKind::CorruptData
    );
    assert!(
        extent.exists(),
        "unreadable evidence cannot license reclamation"
    );
    file.seek(SeekFrom::End(-1)).unwrap();
    file.write_all(&original).unwrap();
    file.sync_all().unwrap();
    drop(file);
    for _ in 0..3 {
        lane.step(NonZeroU64::new(2).unwrap(), NonZeroU64::new(256).unwrap())
            .unwrap();
    }
    assert!(
        !extent.exists(),
        "the repaired fixture resumes bounded reclamation"
    );
}
