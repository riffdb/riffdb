//! End-to-end immutable V2 generation finalization and validate-once reopen.

mod common;

use std::collections::BTreeMap;

use riffdb_columnar::{
    ColumnarManifestV2, ColumnarSnapshot, ColumnarTestBoundary, ColumnarTestController,
    ColumnarV2GenerationError, LiveRow, OrgKey, PrimaryKeyBytes, SegmentV2Codec,
    ValidatedColumnarV2Generation,
};
use riffdb_types::{CanonicalValue, EntityVersion, FrontierPosition, ProjectionGeneration};

use common::{compile_bundle, register_ticket_board, temp_dir};

fn row(version: u64, status: u64, title: &str, priority: i64) -> LiveRow {
    LiveRow {
        entity_version: EntityVersion::new(version).expect("entity version"),
        cells: vec![
            CanonicalValue::U64(status),
            CanonicalValue::string(title).expect("title"),
            CanonicalValue::I64(priority),
        ],
    }
}

fn test_snapshot() -> ColumnarSnapshot {
    let org = OrgKey::from_value(&CanonicalValue::Uuid([0x33; 16])).expect("org");
    let mut snapshot = ColumnarSnapshot::empty();
    snapshot.visible_frontier =
        FrontierPosition::AppliedThrough(riffdb_types::CommitSequence::new(3).expect("frontier"));
    snapshot.delta = BTreeMap::from([(
        org,
        BTreeMap::from([(
            PrimaryKeyBytes::from_entity_key_bytes(vec![0x03]),
            row(3, 30, "retained-tail", 3),
        )]),
    )]);
    snapshot
}

// req: PRJ-002, PRJ-004, PRJ-008, PRJ-009, PRJ-010, OQ-020, OQ-022, OQ-024
#[test]
fn columnar_v2_generation_root_publication_is_atomic_across_partitions() {
    let definition = register_ticket_board(&compile_bundle());
    let source_directory = temp_dir("v2-generation-atomic");
    let generation = ProjectionGeneration::new(2).expect("generation");
    let snapshot_frontier = FrontierPosition::BeforeFirst;
    let applied_frontier =
        FrontierPosition::AppliedThrough(riffdb_types::CommitSequence::new(3).expect("frontier"));
    let org_a = OrgKey::from_value(&CanonicalValue::Uuid([0x11; 16])).expect("org A");
    let org_b = OrgKey::from_value(&CanonicalValue::Uuid([0x22; 16])).expect("org B");
    let mut snapshot = ColumnarSnapshot::empty();
    snapshot.visible_frontier = applied_frontier;
    snapshot.delta = BTreeMap::from([
        (
            org_a.clone(),
            BTreeMap::from([(
                PrimaryKeyBytes::from_entity_key_bytes(vec![0x01]),
                row(1, 10, "alpha", -1),
            )]),
        ),
        (
            org_b.clone(),
            BTreeMap::from([(
                PrimaryKeyBytes::from_entity_key_bytes(vec![0x02]),
                row(2, 20, "beta", 2),
            )]),
        ),
    ]);

    let prepared = ValidatedColumnarV2Generation::prepare(
        &source_directory,
        definition.clone(),
        1,
        generation,
        snapshot_frontier,
        &snapshot,
    )
    .expect("prepare complete generation");
    assert_eq!(prepared.root().partitions().len(), 2);
    assert_eq!(prepared.root().total_segments(), 2);
    assert_eq!(prepared.root().total_rows(), 2);
    for partition in prepared.root().partitions() {
        let manifest = ColumnarManifestV2::decode(
            &std::fs::read(prepared.directory().join(partition.file_name()))
                .expect("read partition manifest"),
        )
        .expect("decode partition manifest");
        assert_eq!(manifest.durable_frontier(), applied_frontier);
        for entry in manifest.segments() {
            assert_eq!(entry.frontier_start(), snapshot_frontier);
            assert_eq!(entry.frontier_end(), applied_frontier);
            let segment = SegmentV2Codec::decode(
                &std::fs::read(prepared.directory().join(entry.file_name()))
                    .expect("read retained-tail segment"),
            )
            .expect("decode retained-tail segment");
            assert_eq!(segment.identity().frontier_start(), snapshot_frontier);
            assert_eq!(segment.identity().frontier_end(), applied_frontier);
        }
    }
    assert_eq!(
        prepared.snapshot().merged_org(&org_a),
        snapshot.merged_org(&org_a)
    );
    assert_eq!(
        prepared.snapshot().merged_org(&org_b),
        snapshot.merged_org(&org_b)
    );

    let reopened = ValidatedColumnarV2Generation::open(
        &source_directory,
        definition,
        1,
        generation,
        applied_frontier,
        prepared.artifact_identity(),
        prepared.root().physical_generation_fingerprint(),
    )
    .expect("open exact complete generation");
    assert_eq!(reopened.root(), prepared.root());
    assert_eq!(
        reopened.snapshot().merged_org(&org_a),
        snapshot.merged_org(&org_a)
    );
    assert_eq!(
        reopened.snapshot().merged_org(&org_b),
        snapshot.merged_org(&org_b)
    );

    let mut unequal = snapshot.clone();
    unequal
        .delta
        .get_mut(&org_a)
        .expect("org A")
        .values_mut()
        .next()
        .expect("row")
        .cells[1] = CanonicalValue::string("not-alpha").expect("changed title");
    assert!(matches!(
        ValidatedColumnarV2Generation::prepare(
            &source_directory,
            register_ticket_board(&compile_bundle()),
            1,
            generation,
            snapshot_frontier,
            &unequal,
        ),
        Err(ColumnarV2GenerationError::LogicalMismatch)
    ));
}

// req: PRJ-002, PRJ-004, PRJ-008, PRJ-009, PRJ-010, OQ-020, OQ-022
#[test]
fn columnar_v2_crash_reopens_exactly_one_published_generation() {
    const CHILD_MODE: &str = "RIFFDB_COLUMNAR_V2_CRASH_CHILD";
    const CHILD_PATH: &str = "RIFFDB_COLUMNAR_V2_CRASH_PATH";
    const CHILD_BOUNDARY: &str = "RIFFDB_COLUMNAR_V2_CRASH_BOUNDARY";

    if std::env::var(CHILD_MODE).as_deref() == Ok("1") {
        let source_directory =
            std::path::PathBuf::from(std::env::var_os(CHILD_PATH).expect("child path"));
        let boundary = match std::env::var(CHILD_BOUNDARY)
            .expect("child boundary")
            .as_str()
        {
            "before-segment-sync" => ColumnarTestBoundary::BeforeV2SegmentSync,
            "after-segment-rename" => ColumnarTestBoundary::AfterV2SegmentRename,
            "after-manifest-rename" => ColumnarTestBoundary::AfterV2ManifestRename,
            "after-root-rename" => ColumnarTestBoundary::AfterV2RootRename,
            "after-generation-rename" => ColumnarTestBoundary::AfterV2GenerationRename,
            other => panic!("unknown V2 boundary {other}"),
        };
        let controller = ColumnarTestController::new();
        controller.arm_abort_at(boundary);
        let _ = ValidatedColumnarV2Generation::prepare_with_controller(
            &source_directory,
            register_ticket_board(&compile_bundle()),
            1,
            ProjectionGeneration::new(7).expect("generation"),
            FrontierPosition::BeforeFirst,
            &test_snapshot(),
            &controller,
        );
        std::process::exit(0);
    }

    for (name, _) in [
        (
            "before-segment-sync",
            ColumnarTestBoundary::BeforeV2SegmentSync,
        ),
        (
            "after-segment-rename",
            ColumnarTestBoundary::AfterV2SegmentRename,
        ),
        (
            "after-manifest-rename",
            ColumnarTestBoundary::AfterV2ManifestRename,
        ),
        ("after-root-rename", ColumnarTestBoundary::AfterV2RootRename),
        (
            "after-generation-rename",
            ColumnarTestBoundary::AfterV2GenerationRename,
        ),
    ] {
        let source_directory = temp_dir(&format!("v2-crash-{name}"));
        let status = std::process::Command::new(std::env::current_exe().expect("test executable"))
            .arg("--exact")
            .arg("columnar_v2_crash_reopens_exactly_one_published_generation")
            .arg("--nocapture")
            .env(CHILD_MODE, "1")
            .env(CHILD_PATH, &source_directory)
            .env(CHILD_BOUNDARY, name)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .expect("spawn V2 crash child");
        assert!(!status.success(), "child must abort at {name}");

        let snapshot = test_snapshot();
        let generation = ProjectionGeneration::new(7).expect("generation");
        let recovered = ValidatedColumnarV2Generation::prepare(
            &source_directory,
            register_ticket_board(&compile_bundle()),
            1,
            generation,
            FrontierPosition::BeforeFirst,
            &snapshot,
        )
        .unwrap_or_else(|error| panic!("recover {name}: {error}"));
        assert_eq!(recovered.root().frontier(), snapshot.visible_frontier);
        assert_eq!(recovered.root().total_rows(), 1);
        assert!(
            !ValidatedColumnarV2Generation::temporary_directory(&source_directory, generation)
                .exists(),
            "recovery removes the interrupted private directory"
        );
    }
}

// req: PRJ-004, PRJ-008, PRJ-009, PRJ-010, OQ-020
#[test]
fn columnar_v2_retirement_crash_never_rolls_back_and_reclamation_is_idempotent() {
    const CHILD_MODE: &str = "RIFFDB_COLUMNAR_V2_RECLAIM_CHILD";
    const CHILD_PATH: &str = "RIFFDB_COLUMNAR_V2_RECLAIM_PATH";
    const CHILD_BOUNDARY: &str = "RIFFDB_COLUMNAR_V2_RECLAIM_BOUNDARY";
    let generation = ProjectionGeneration::new(9).expect("generation");
    let selected_generation = ProjectionGeneration::new(10).expect("selected generation");

    if std::env::var(CHILD_MODE).as_deref() == Ok("1") {
        let source_directory =
            std::path::PathBuf::from(std::env::var_os(CHILD_PATH).expect("child path"));
        let boundary = match std::env::var(CHILD_BOUNDARY)
            .expect("child boundary")
            .as_str()
        {
            "before-reclaim" => ColumnarTestBoundary::BeforeV2GenerationReclaim,
            "after-reclaim" => ColumnarTestBoundary::AfterV2GenerationReclaim,
            other => panic!("unknown reclaim boundary {other}"),
        };
        ValidatedColumnarV2Generation::prepare(
            &source_directory,
            register_ticket_board(&compile_bundle()),
            1,
            generation,
            FrontierPosition::BeforeFirst,
            &test_snapshot(),
        )
        .expect("prepare unselected generation");
        ValidatedColumnarV2Generation::prepare(
            &source_directory,
            register_ticket_board(&compile_bundle()),
            1,
            selected_generation,
            FrontierPosition::BeforeFirst,
            &test_snapshot(),
        )
        .expect("prepare independently selected generation");
        let controller = ColumnarTestController::new();
        controller.arm_abort_at(boundary);
        let _ = ValidatedColumnarV2Generation::reclaim_with_controller(
            &source_directory,
            generation,
            &controller,
        );
        std::process::exit(0);
    }

    for name in ["before-reclaim", "after-reclaim"] {
        let source_directory = temp_dir(&format!("v2-{name}"));
        let status = std::process::Command::new(std::env::current_exe().expect("test executable"))
            .arg("--exact")
            .arg("columnar_v2_retirement_crash_never_rolls_back_and_reclamation_is_idempotent")
            .arg("--nocapture")
            .env(CHILD_MODE, "1")
            .env(CHILD_PATH, &source_directory)
            .env(CHILD_BOUNDARY, name)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .expect("spawn reclaim crash child");
        assert!(!status.success(), "child must abort at {name}");
        let selected = ValidatedColumnarV2Generation::prepare(
            &source_directory,
            register_ticket_board(&compile_bundle()),
            1,
            selected_generation,
            FrontierPosition::BeforeFirst,
            &test_snapshot(),
        )
        .unwrap_or_else(|error| panic!("selected generation after {name}: {error}"));
        assert_eq!(selected.root().generation(), selected_generation);
        ValidatedColumnarV2Generation::reclaim(&source_directory, generation)
            .unwrap_or_else(|error| panic!("retry {name}: {error}"));
        assert!(
            !source_directory
                .join(format!("generation-{:016x}", generation.get()))
                .exists()
        );
        ValidatedColumnarV2Generation::reclaim(&source_directory, generation)
            .expect("repeated reclaim remains a no-op");
    }
}

// req: PRJ-002, PRJ-004, PRJ-009, PRJ-010, OQ-020
#[test]
fn columnar_v2_enospc_candidate_failure_leaves_prior_generation_exactly_readable() {
    let source_directory = temp_dir("v2-enospc-preserves-prior");
    let definition = register_ticket_board(&compile_bundle());
    let snapshot = test_snapshot();
    let predecessor_generation = ProjectionGeneration::new(11).expect("predecessor generation");
    let predecessor = ValidatedColumnarV2Generation::prepare(
        &source_directory,
        definition.clone(),
        1,
        predecessor_generation,
        FrontierPosition::BeforeFirst,
        &snapshot,
    )
    .expect("prepare predecessor");
    let predecessor_identity = predecessor.artifact_identity();
    let predecessor_physical = predecessor.root().physical_generation_fingerprint();

    let controller = ColumnarTestController::new();
    controller.arm_io_failure_at(ColumnarTestBoundary::BeforeV2SegmentSync);
    let failure = ValidatedColumnarV2Generation::prepare_with_controller(
        &source_directory,
        definition.clone(),
        1,
        ProjectionGeneration::new(12).expect("candidate generation"),
        FrontierPosition::BeforeFirst,
        &snapshot,
        &controller,
    );
    assert!(matches!(failure, Err(ColumnarV2GenerationError::Io)));

    let reopened = ValidatedColumnarV2Generation::open(
        &source_directory,
        definition,
        1,
        predecessor_generation,
        snapshot.visible_frontier,
        predecessor_identity,
        predecessor_physical,
    )
    .expect("prior selected generation remains exact");
    assert_eq!(reopened.root(), predecessor.root());
}
