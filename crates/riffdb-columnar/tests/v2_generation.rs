//! End-to-end immutable V2 generation finalization and validate-once reopen.

mod common;

use std::collections::BTreeMap;
use std::hint::black_box;
use std::num::NonZeroU64;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use riffdb_columnar::{
    AggregateOp, AggregateValue, ColumnPredicate, ColumnarEngine, ColumnarManifestV2,
    ColumnarProjectionSpecV1, ColumnarQueryRequest, ColumnarSnapshot, ColumnarSpecReplayLimitsV1,
    ColumnarTestBoundary, ColumnarTestController, ColumnarV2GenerationError,
    ColumnarV2StreamingError, LiveRow, OpenOptions, OrgKey, PrimaryKeyBytes, QueryBudget,
    QueryError, QueryResult, SegmentV2Codec, ValidatedColumnarV2Generation, query_snapshot,
    query_snapshot_with_policy_admission,
};
use riffdb_policy::AuthorizedProjectedRowAdmissionV1;
use riffdb_storage_api::{
    ApplicationExportSnapshotReader, ApplicationExportSourcePageV1,
    ApplicationExportSourceRecordV1, ColumnarProjectionArtifactV1, ColumnarProjectionLayoutV1,
    CommittedEntityReferenceV2, EntityTarget, ExpectedEntityState, StorageError, StorageErrorKind,
    StorageScanLimit, StoredColumnarProjectionGenerationV1, StoredEntityRecordV1,
};
use riffdb_types::{
    ApplicationExportClassV1, ApplicationExportSnapshotBindingV1, CanonicalValue,
    ContractBundleHash, ContractVersion, DatabaseId, EntityKey, EntityKeyBuilder, EntityVersion,
    FrontierPosition, PartitionKey, ProjectionGeneration, Timestamp,
};

use common::{
    HistorySource, Oracle, assert_corpus_equivalence, compile_bundle, corpus_requests,
    corpus_results, entity_type_id, field_id, open_engine, push_open_race_v1, push_ticket_create,
    register_ticket_board, resolve_open_race, temp_dir,
};

fn replay_sample() -> Result<Timestamp, ColumnarV2StreamingError> {
    Ok(Timestamp::new(1_700_000_000, 0).expect("canonical test UTC"))
}

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

fn ticket_key(
    entity: riffdb_types::EntityTypeId,
    organization: &[u8; 16],
    ticket: u64,
) -> PrimaryKeyBytes {
    let mut key = EntityKeyBuilder::new(entity);
    key.push_uuid(organization).expect("organization key");
    key.push_u64(ticket).expect("ticket key");
    PrimaryKeyBytes::from_entity_key_bytes(key.finish().expect("entity key").into_bytes())
}

struct CapturedEntitySnapshot {
    binding: ApplicationExportSnapshotBindingV1,
    records: Vec<StoredEntityRecordV1>,
    passes: AtomicUsize,
}

impl CapturedEntitySnapshot {
    fn new(frontier: u64, mut records: Vec<StoredEntityRecordV1>) -> Self {
        records.sort_by(|left, right| {
            left.target()
                .key()
                .as_bytes()
                .cmp(right.target().key().as_bytes())
        });
        Self {
            binding: ApplicationExportSnapshotBindingV1::new(
                DatabaseId::from_bytes(common::uuid(0x71)).expect("database"),
                NonZeroU64::new(1).expect("incarnation"),
                Some(riffdb_types::CommitSequence::new(frontier).expect("frontier")),
                None,
                ContractVersion::new(1).expect("version"),
                ContractBundleHash::from_bytes([0x72; 32]),
                Vec::new(),
                Vec::new(),
            )
            .expect("binding"),
            records,
            passes: AtomicUsize::new(0),
        }
    }

    fn passes(&self) -> usize {
        self.passes.load(Ordering::Relaxed)
    }
}

impl ApplicationExportSnapshotReader for CapturedEntitySnapshot {
    fn binding(&self) -> &ApplicationExportSnapshotBindingV1 {
        &self.binding
    }

    fn contract_bundle_bytes(&self) -> &[u8] {
        &[]
    }

    fn read_application_export_source_page(
        &self,
        _class: ApplicationExportClassV1,
        _after: Option<&[u8]>,
        _limit: StorageScanLimit,
    ) -> Result<ApplicationExportSourcePageV1, StorageError> {
        Err(StorageError::new(StorageErrorKind::Unavailable, None))
    }

    fn read_application_export_entity_page(
        &self,
        _entity_type: riffdb_types::EntityTypeId,
        after: Option<&[u8]>,
        limit: StorageScanLimit,
    ) -> Result<ApplicationExportSourcePageV1, StorageError> {
        let start = match after {
            None => {
                self.passes.fetch_add(1, Ordering::Relaxed);
                0
            }
            Some(bytes) if bytes.len() == 8 => {
                let mut value = [0u8; 8];
                value.copy_from_slice(bytes);
                usize::try_from(u64::from_be_bytes(value))
                    .map_err(|_| StorageError::new(StorageErrorKind::CorruptData, None))?
            }
            Some(_) => return Err(StorageError::new(StorageErrorKind::CorruptData, None)),
        };
        let end = start
            .saturating_add(usize::from(limit.get()))
            .min(self.records.len());
        let records = self.records[start..end]
            .iter()
            .cloned()
            .map(|record| ApplicationExportSourceRecordV1::Entity(Box::new(record)))
            .collect::<Vec<_>>();
        let exact_end = end == self.records.len();
        let continuation = (!exact_end).then(|| end.to_be_bytes().to_vec().into_boxed_slice());
        ApplicationExportSourcePageV1::new(
            ApplicationExportClassV1::Entity,
            records,
            continuation,
            exact_end,
            (end - start).saturating_mul(64),
        )
        .map_err(|_| StorageError::new(StorageErrorKind::CorruptData, None))
    }

    fn application_export_indexed_relationship_exists(
        &self,
        _index_prefix: &[u8],
        _partition: &PartitionKey,
    ) -> Result<bool, StorageError> {
        Err(StorageError::new(StorageErrorKind::Unavailable, None))
    }

    fn read_application_export_policy_anchor(
        &self,
        _target: &EntityTarget,
    ) -> Result<Option<StoredEntityRecordV1>, StorageError> {
        Err(StorageError::new(StorageErrorKind::Unavailable, None))
    }
}

// The follower can supply this owner without any export or writer executor.
struct EntityOnlySnapshot(CapturedEntitySnapshot);
impl riffdb_storage_api::AuthoritativeEntitySnapshotReader for EntityOnlySnapshot {
    fn read_entity_type_page(
        &self,
        entity: riffdb_types::EntityTypeId,
        after: Option<&[u8]>,
        limit: StorageScanLimit,
    ) -> Result<ApplicationExportSourcePageV1, StorageError> {
        self.0
            .read_application_export_entity_page(entity, after, limit)
    }
}

fn moved_ticket(
    bundle: &riffdb_contract_ir::ContractBundle,
    target: EntityTarget,
    version: u64,
    organization: [u8; 16],
) -> StoredEntityRecordV1 {
    HistorySource::make_entity(
        target,
        EntityVersion::new(version).expect("version"),
        common::ticket_fields(bundle, organization, 1, version, "moved", version as i64),
    )
}

// req: REP-004, PRJ-002, PRJ-004, PRJ-009, PRJ-010, OQ-020, OQ-022
#[test]
fn streaming_v2_rebuild_preserves_org_move_d4_bounds_and_zero_pre_root_scratch() {
    let bundle = compile_bundle();
    let definition = register_ticket_board(&bundle);
    let entity = definition.entity_type_id();
    let organization_a = common::uuid(0x11);
    let organization_b = common::uuid(0x22);
    let organization_c = common::uuid(0x33);
    let target = HistorySource::ticket_target(entity, organization_a, 1);
    let row_a = moved_ticket(&bundle, target.clone(), 1, organization_a);
    let row_b = moved_ticket(&bundle, target.clone(), 2, organization_b);
    let row_c = moved_ticket(&bundle, target.clone(), 3, organization_c);
    let snapshot = EntityOnlySnapshot(CapturedEntitySnapshot::new(1, vec![row_a]));
    let mut source = HistorySource::default();
    source.append_commit(
        riffdb_types::CommitSequence::new(2).expect("two"),
        vec![CommittedEntityReferenceV2::from_post_image(&row_b).expect("B reference")],
        vec![(
            target.clone(),
            ExpectedEntityState::Present(EntityVersion::new(1).expect("one")),
        )],
    );
    source.append_commit(
        riffdb_types::CommitSequence::new(3).expect("three"),
        vec![CommittedEntityReferenceV2::from_post_image(&row_c).expect("C reference")],
        vec![(
            target,
            ExpectedEntityState::Present(EntityVersion::new(2).expect("two")),
        )],
    );
    source.put_entity(row_c);

    let directory = temp_dir("v2-streaming-org-move");
    let generation = ProjectionGeneration::new(81).expect("generation");
    let limits = ColumnarSpecReplayLimitsV1::new(60, 128, 2).expect("inclusive limits");
    let scans_before_sample = source.scan_calls();
    let sample_calls = AtomicUsize::new(0);
    let prepared = ValidatedColumnarV2Generation::prepare_streaming(
        &directory,
        definition.clone(),
        1,
        generation,
        FrontierPosition::AppliedThrough(riffdb_types::CommitSequence::first()),
        &snapshot,
        &source,
        limits,
        || {
            assert!(
                source.scan_calls() > scans_before_sample,
                "UTC must be sampled after the exact frozen-tail scan"
            );
            sample_calls.fetch_add(1, Ordering::AcqRel);
            replay_sample()
        },
        || false,
    )
    .expect("streaming generation");
    assert_eq!(sample_calls.load(Ordering::Acquire), 1);
    assert_eq!(
        prepared.root().frontier(),
        FrontierPosition::AppliedThrough(riffdb_types::CommitSequence::new(3).expect("three"))
    );
    assert_eq!(prepared.root().partitions().len(), 1);
    assert_eq!(
        prepared.root().partitions()[0].organization(),
        &OrgKey::from_value(&CanonicalValue::Uuid(organization_c)).expect("C org")
    );
    assert_eq!(
        snapshot.0.passes(),
        3,
        "one early bound pass plus two exact merged passes"
    );
    assert!(!ValidatedColumnarV2Generation::temporary_directory(&directory, generation).exists());
    assert!(!prepared.directory().join(".rebuild-scratch-v1").exists());
    assert!(
        common::rows_of(
            query_snapshot(
                &definition,
                prepared.snapshot(),
                &common::board_query(CanonicalValue::Uuid(organization_a)),
            )
            .expect("A query")
        )
        .is_empty()
    );
    assert_eq!(
        common::rows_of(
            query_snapshot(
                &definition,
                prepared.snapshot(),
                &common::board_query(CanonicalValue::Uuid(organization_c)),
            )
            .expect("C query")
        )
        .len(),
        1
    );

    for (name, limits, expected) in [
        (
            "backlog",
            ColumnarSpecReplayLimitsV1::new(60, 128, 1).expect("limits"),
            ColumnarV2StreamingError::ReplayBacklog,
        ),
        (
            "bytes",
            ColumnarSpecReplayLimitsV1::new(60, 127, 2).expect("limits"),
            ColumnarV2StreamingError::ReplayBytes,
        ),
    ] {
        let failed = temp_dir(&format!("v2-streaming-{name}"));
        let generation = ProjectionGeneration::new(82).expect("generation");
        assert_eq!(
            ValidatedColumnarV2Generation::prepare_streaming(
                &failed,
                definition.clone(),
                1,
                generation,
                FrontierPosition::AppliedThrough(riffdb_types::CommitSequence::first()),
                &snapshot,
                &source,
                limits,
                replay_sample,
                || false,
            )
            .map(|_| ()),
            Err(expected)
        );
        assert!(!ValidatedColumnarV2Generation::temporary_directory(&failed, generation).exists());
    }

    let unresolved = temp_dir("v2-streaming-multiple-replay-limits");
    let generation = ProjectionGeneration::new(86).expect("generation");
    assert_eq!(
        ValidatedColumnarV2Generation::prepare_streaming(
            &unresolved,
            definition.clone(),
            1,
            generation,
            FrontierPosition::AppliedThrough(riffdb_types::CommitSequence::first()),
            &snapshot,
            &source,
            ColumnarSpecReplayLimitsV1::new(60, 1, 1).expect("limits"),
            replay_sample,
            || false,
        )
        .map(|_| ()),
        Err(ColumnarV2StreamingError::ReplayBytes)
    );
    assert!(!ValidatedColumnarV2Generation::temporary_directory(&unresolved, generation).exists());

    let aged = temp_dir("v2-streaming-aged-replay");
    let generation = ProjectionGeneration::new(87).expect("generation");
    assert_eq!(
        ValidatedColumnarV2Generation::prepare_streaming(
            &aged,
            definition.clone(),
            1,
            generation,
            FrontierPosition::AppliedThrough(riffdb_types::CommitSequence::first()),
            &snapshot,
            &source,
            ColumnarSpecReplayLimitsV1::new(60, 1, 1).expect("limits"),
            || Ok(Timestamp::new(1_700_000_061, 0).expect("aged UTC")),
            || false,
        )
        .map(|_| ()),
        Err(ColumnarV2StreamingError::ReplayAge),
        "age has deterministic precedence over bytes and backlog"
    );
    assert!(!ValidatedColumnarV2Generation::temporary_directory(&aged, generation).exists());

    let clock_failed = temp_dir("v2-streaming-clock-failure");
    let generation = ProjectionGeneration::new(88).expect("generation");
    let scans_before_sample = source.scan_calls();
    assert_eq!(
        ValidatedColumnarV2Generation::prepare_streaming(
            &clock_failed,
            definition.clone(),
            1,
            generation,
            FrontierPosition::AppliedThrough(riffdb_types::CommitSequence::first()),
            &snapshot,
            &source,
            limits,
            || {
                assert!(source.scan_calls() > scans_before_sample);
                Err(ColumnarV2StreamingError::Clock)
            },
            || false,
        )
        .map(|_| ()),
        Err(ColumnarV2StreamingError::Clock)
    );
    assert!(
        !ValidatedColumnarV2Generation::temporary_directory(&clock_failed, generation).exists()
    );

    let failed = temp_dir("v2-streaming-pre-root-failure");
    let generation = ProjectionGeneration::new(83).expect("generation");
    let controller = ColumnarTestController::new();
    controller.arm_io_failure_at(ColumnarTestBoundary::BeforeV2RootFinalize);
    assert_eq!(
        ValidatedColumnarV2Generation::prepare_streaming_with_controller(
            &failed,
            definition,
            1,
            generation,
            FrontierPosition::AppliedThrough(riffdb_types::CommitSequence::first()),
            &snapshot,
            &source,
            limits,
            replay_sample,
            || false,
            &controller,
        )
        .map(|_| ()),
        Err(ColumnarV2StreamingError::Io)
    );
    assert!(
        controller
            .events()
            .contains(&ColumnarTestBoundary::BeforeV2RootFinalize)
    );
    assert!(!ValidatedColumnarV2Generation::temporary_directory(&failed, generation).exists());
}

// req: PRJ-004, PRJ-009, PRJ-010, OQ-020
#[test]
fn streaming_v2_refuses_4097_snapshot_partitions_before_generation_scratch() {
    let bundle = compile_bundle();
    let definition = register_ticket_board(&bundle);
    let entity = definition.entity_type_id();
    let mut records = Vec::with_capacity(4_097);
    for ordinal in 0..4_097u64 {
        let mut organization = [0u8; 16];
        organization[6] = 0x70;
        organization[8] = 0x80;
        organization[8..].copy_from_slice(&(ordinal | (0x8000_0000_0000_0000)).to_be_bytes());
        let target = HistorySource::ticket_target(entity, organization, ordinal + 1);
        records.push(moved_ticket(&bundle, target, 1, organization));
    }
    let snapshot = CapturedEntitySnapshot::new(1, records);
    let source = HistorySource::default();
    let parent = temp_dir("v2-streaming-partition-overflow");
    let source_directory = parent.join("not-created-source");
    let generation = ProjectionGeneration::new(84).expect("generation");
    assert_eq!(
        ValidatedColumnarV2Generation::prepare_streaming(
            &source_directory,
            definition,
            1,
            generation,
            FrontierPosition::AppliedThrough(riffdb_types::CommitSequence::first()),
            &snapshot,
            &source,
            ColumnarSpecReplayLimitsV1::new(60, 128, 2).expect("limits"),
            replay_sample,
            || false,
        )
        .map(|_| ()),
        Err(ColumnarV2StreamingError::BoundExceeded)
    );
    assert!(!source_directory.exists());
    assert!(
        !ValidatedColumnarV2Generation::temporary_directory(&source_directory, generation).exists()
    );
    assert_eq!(snapshot.passes(), 1);
}

fn multi_run_streaming_fixture() -> (
    riffdb_columnar::RegisteredDefinition,
    CapturedEntitySnapshot,
    HistorySource,
) {
    let bundle = compile_bundle();
    let definition = register_ticket_board(&bundle);
    let entity = definition.entity_type_id();
    let organization = common::uuid(0x44);
    let snapshot = CapturedEntitySnapshot::new(1, Vec::new());
    let mut source = HistorySource::default();
    for sequence in 2..=260u64 {
        let target = HistorySource::ticket_target(entity, organization, sequence);
        let record = moved_ticket(&bundle, target.clone(), 1, organization);
        source.append_commit(
            riffdb_types::CommitSequence::new(sequence).expect("sequence"),
            vec![CommittedEntityReferenceV2::from_post_image(&record).expect("reference")],
            vec![(target, ExpectedEntityState::Absent)],
        );
        source.put_entity(record);
    }
    (definition, snapshot, source)
}

// req: PRJ-002, PRJ-004, PRJ-009, PRJ-010, OQ-020
#[test]
fn streaming_v2_crash_cleanup_covers_run_merge_partition_and_pre_root() {
    const CHILD_MODE: &str = "RIFFDB_COLUMNAR_V2_STREAMING_CRASH_CHILD";
    const CHILD_PATH: &str = "RIFFDB_COLUMNAR_V2_STREAMING_CRASH_PATH";
    const CHILD_BOUNDARY: &str = "RIFFDB_COLUMNAR_V2_STREAMING_CRASH_BOUNDARY";
    let generation = ProjectionGeneration::new(85).expect("generation");
    let limits = ColumnarSpecReplayLimitsV1::new(60, 1_048_576, 1_000).expect("limits");

    if std::env::var(CHILD_MODE).as_deref() == Ok("1") {
        let directory =
            std::path::PathBuf::from(std::env::var_os(CHILD_PATH).expect("streaming child path"));
        let boundary = match std::env::var(CHILD_BOUNDARY)
            .expect("streaming child boundary")
            .as_str()
        {
            "scratch-run" => ColumnarTestBoundary::AfterV2ScratchRun,
            "scratch-merge" => ColumnarTestBoundary::AfterV2ScratchMerge,
            "partition-finalize" => ColumnarTestBoundary::AfterV2PartitionFinalize,
            "pre-root" => ColumnarTestBoundary::BeforeV2RootFinalize,
            other => panic!("unknown streaming boundary {other}"),
        };
        let (definition, snapshot, source) = multi_run_streaming_fixture();
        let controller = ColumnarTestController::new();
        controller.arm_abort_at(boundary);
        let _ = ValidatedColumnarV2Generation::prepare_streaming_with_controller(
            &directory,
            definition,
            1,
            generation,
            FrontierPosition::AppliedThrough(riffdb_types::CommitSequence::first()),
            &snapshot,
            &source,
            limits,
            replay_sample,
            || false,
            &controller,
        );
        std::process::exit(0);
    }

    for name in [
        "scratch-run",
        "scratch-merge",
        "partition-finalize",
        "pre-root",
    ] {
        let directory = temp_dir(&format!("v2-streaming-crash-{name}"));
        let status = std::process::Command::new(std::env::current_exe().expect("test executable"))
            .arg("--exact")
            .arg("streaming_v2_crash_cleanup_covers_run_merge_partition_and_pre_root")
            .arg("--nocapture")
            .env(CHILD_MODE, "1")
            .env(CHILD_PATH, &directory)
            .env(CHILD_BOUNDARY, name)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .expect("spawn streaming crash child");
        assert!(!status.success(), "child must abort at {name}");
        let (definition, snapshot, source) = multi_run_streaming_fixture();
        let recovered = ValidatedColumnarV2Generation::prepare_streaming(
            &directory,
            definition,
            1,
            generation,
            FrontierPosition::AppliedThrough(riffdb_types::CommitSequence::first()),
            &snapshot,
            &source,
            limits,
            replay_sample,
            || false,
        )
        .unwrap_or_else(|error| panic!("recover {name}: {error}"));
        assert_eq!(recovered.root().total_rows(), 259);
        assert!(
            !ValidatedColumnarV2Generation::temporary_directory(&directory, generation).exists()
        );
        assert!(!recovered.directory().join(".rebuild-scratch-v1").exists());
    }
}

// req: PRJ-002, PRJ-004, PRJ-009, PRJ-010, OQ-020, OQ-022
#[test]
fn dataful_v2_rebuild_reopens_repeatedly_and_refuses_mixed_or_stale_state() {
    let bundle = compile_bundle();
    let definition = register_ticket_board(&bundle);
    let organization = common::uuid(0x53);
    let organization_value = CanonicalValue::Uuid(organization);
    let organization_key = OrgKey::from_value(&organization_value).expect("organization");
    let mut source = HistorySource::default();
    let mut oracle = Oracle::default();
    let mut engine = open_engine(definition.clone(), "v2-dataful-rebuild");

    push_ticket_create(
        &mut source,
        &mut oracle,
        &bundle,
        1,
        organization,
        1,
        1,
        "one",
        1,
    );
    push_ticket_create(
        &mut source,
        &mut oracle,
        &bundle,
        2,
        organization,
        2,
        2,
        "two",
        2,
    );
    engine.apply_available(&source).expect("initial V1 apply");

    let race = push_open_race_v1(&mut source, &mut oracle, &bundle, 3, organization, 3);
    engine.apply_available(&source).expect("held V1 apply");
    assert_eq!(
        engine.published_frontier_position(),
        FrontierPosition::AppliedThrough(
            riffdb_types::CommitSequence::new(2).expect("safe prefix")
        )
    );
    assert_eq!(engine.deferred_set_size(), 1);
    assert_corpus_equivalence(&engine, &oracle, &bundle, &[organization], "held V1");

    resolve_open_race(&mut source, &mut oracle, race, 4);
    engine
        .apply_available(&source)
        .expect("resolve supersession holdback");
    assert_eq!(engine.deferred_set_size(), 0);
    assert_corpus_equivalence(&engine, &oracle, &bundle, &[organization], "resolved V1");
    let before_replay = corpus_results(&engine, &bundle, &organization_value);
    engine
        .apply_available(&source)
        .expect("idempotent repeated replay");
    assert_eq!(
        corpus_results(&engine, &bundle, &organization_value),
        before_replay
    );

    let mut matched = ColumnarSnapshot::empty();
    matched.visible_frontier = engine.published_frontier_position();
    let flattened = engine
        .published_snapshot()
        .merged_org(&OrgKey::from_value(&organization_value).expect("organization"))
        .into_iter()
        .map(|(key, row)| {
            (
                key,
                LiveRow {
                    entity_version: row.entity_version,
                    cells: row.cells,
                },
            )
        })
        .collect();
    matched.delta.insert(organization_key, flattened);
    let source_directory = temp_dir("v2-dataful-reopen");
    let generation = ProjectionGeneration::new(41).expect("generation");
    let prepared = ValidatedColumnarV2Generation::prepare(
        &source_directory,
        definition.clone(),
        1,
        generation,
        FrontierPosition::BeforeFirst,
        &matched,
    )
    .expect("prepare dataful V2");
    let artifact = prepared.artifact_identity();
    let physical = prepared.root().physical_generation_fingerprint();
    let expected_results = corpus_requests(&bundle, &organization_value)
        .iter()
        .map(|request| query_snapshot(&definition, &matched, request).expect("V1 result"))
        .collect::<Vec<_>>();

    for _ in 0..3 {
        let reopened = ValidatedColumnarV2Generation::open(
            &source_directory,
            definition.clone(),
            1,
            generation,
            matched.visible_frontier,
            artifact,
            physical,
        )
        .expect("repeat exact V2 open");
        let actual = corpus_requests(&bundle, &organization_value)
            .iter()
            .map(|request| {
                query_snapshot(&definition, reopened.snapshot(), request).expect("V2 result")
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, expected_results);
    }

    assert!(matches!(
        ValidatedColumnarV2Generation::open(
            &source_directory,
            definition.clone(),
            2,
            generation,
            matched.visible_frontier,
            artifact,
            physical,
        ),
        Err(ColumnarV2GenerationError::Invalid)
    ));

    let mixed_v1 = prepared.directory().join("MANIFEST-V1-mixed");
    std::fs::write(&mixed_v1, b"mixed V1 member").expect("write mixed member");
    assert!(matches!(
        ValidatedColumnarV2Generation::open(
            &source_directory,
            definition.clone(),
            1,
            generation,
            matched.visible_frontier,
            artifact,
            physical,
        ),
        Err(ColumnarV2GenerationError::Invalid)
    ));
    std::fs::remove_file(&mixed_v1).expect("remove mixed member");
    ValidatedColumnarV2Generation::open(
        &source_directory,
        definition,
        1,
        generation,
        matched.visible_frontier,
        artifact,
        physical,
    )
    .expect("exact V2 remains reopenable after mixed member removal");
}

// req: PRJ-002, PRJ-004, PRJ-009, OQ-020, OQ-022, PERF-007, PERF-008
#[test]
#[ignore = "non-terminal lower-level mechanics diagnostic; production receipt runs in riffdb-server"]
fn wp711_lower_level_v2_activation_diagnostic() {
    const ROWS: usize = 16_384;
    const PARTITIONS: usize = 2;
    const ROWS_PER_PARTITION: usize = ROWS / PARTITIONS;
    const SAMPLES: usize = 31;
    let scope = temp_dir("wp711-activation-receipt");
    let bundle = compile_bundle();
    let definition = register_ticket_board(&bundle);
    let organizations = [common::uuid(0x63), common::uuid(0x64)];
    let organization_values = organizations.map(CanonicalValue::Uuid);
    let organization_keys = organization_values
        .iter()
        .map(|value| OrgKey::from_value(value).expect("organization"))
        .collect::<Vec<_>>();
    let no_projection_directory = scope.join("no-projection");
    assert!(!no_projection_directory.exists());

    let mut source = HistorySource::default();
    let mut oracle = Oracle::default();
    for index in 0..ROWS {
        let partition = index / ROWS_PER_PARTITION;
        let partition_row = index % ROWS_PER_PARTITION;
        push_ticket_create(
            &mut source,
            &mut oracle,
            &bundle,
            u64::try_from(index + 1).expect("sequence"),
            organizations[partition],
            u64::try_from(partition_row + 1).expect("ticket"),
            u64::try_from(partition_row % 4).expect("status"),
            match partition_row % 4 {
                0 => "open",
                1 => "closed",
                2 => "queued",
                _ => "running",
            },
            i64::try_from(partition_row).expect("priority"),
        );
    }

    let v1_directory = scope.join("v1");
    let mut v1 = ColumnarEngine::open(
        definition.clone(),
        OpenOptions::new(v1_directory.clone()).with_history_incarnation(1),
    )
    .expect("open V1 control");
    v1.apply_available(&source).expect("build V1 control");
    v1.checkpoint().expect("checkpoint V1 control");
    let frontier = v1.published_frontier_position();
    let head = u64::try_from(source.commits.len()).expect("head");
    let lag = head.saturating_sub(frontier_sequence(frontier));
    assert_eq!(lag, 0);
    let v1_bytes = directory_bytes(&v1_directory);
    let queries = organization_values
        .iter()
        .cloned()
        .map(|org_scope| ColumnarQueryRequest {
            org_scope,
            select: Vec::new(),
            predicates: Vec::new(),
            order: Vec::new(),
            limit: None,
            group_by: None,
            aggregate: Some(AggregateOp::Count),
            budget: QueryBudget::default(),
        })
        .collect::<Vec<_>>();
    let expected_partition =
        QueryResult::Aggregate(AggregateValue::Count(ROWS_PER_PARTITION as u64));
    let expected = vec![expected_partition; PARTITIONS];
    let initial_results = queries
        .iter()
        .map(|query| v1.query(query).expect("V1 partition result"))
        .collect::<Vec<_>>();
    assert_eq!(initial_results, expected);
    for key in &organization_keys {
        assert_eq!(
            v1.published_snapshot().merged_org(key).len(),
            ROWS_PER_PARTITION,
            "every production request stays inside one compiled org partition"
        );
    }
    let v1_partition_query_ns = queries
        .iter()
        .map(|query| {
            stage_samples(SAMPLES, || {
                assert_eq!(
                    black_box(&v1).query(black_box(query)).expect("V1 query"),
                    expected[0]
                );
            })
        })
        .collect::<Vec<_>>();
    let v1_query_ns = stage_samples(SAMPLES, || {
        let actual = queries
            .iter()
            .map(|query| black_box(&v1).query(black_box(query)).expect("V1 query"))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    });
    let v1_recovery_ns = stage_samples(SAMPLES, || {
        let reopened = ColumnarEngine::open(
            definition.clone(),
            OpenOptions::new(v1_directory.clone()).with_history_incarnation(1),
        )
        .expect("V1 recovery");
        let actual = queries
            .iter()
            .map(|query| reopened.query(query).expect("recovered V1 query"))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    });

    let mut matched = ColumnarSnapshot::empty();
    matched.visible_frontier = frontier;
    for organization_key in &organization_keys {
        let flattened = v1
            .published_snapshot()
            .merged_org(organization_key)
            .into_iter()
            .map(|(key, row)| {
                (
                    key,
                    LiveRow {
                        entity_version: row.entity_version,
                        cells: row.cells,
                    },
                )
            })
            .collect();
        matched.delta.insert(organization_key.clone(), flattened);
    }

    let v2_directory = scope.join("v2");
    let rebuild_start = Instant::now();
    let prepared = ValidatedColumnarV2Generation::prepare(
        &v2_directory,
        definition.clone(),
        1,
        ProjectionGeneration::new(101).expect("rebuild generation"),
        FrontierPosition::BeforeFirst,
        &matched,
    )
    .expect("production V2 rebuild");
    let rebuild_ns = rebuild_start.elapsed().as_nanos();
    let root = prepared.root().clone();
    let artifact = prepared.artifact_identity();
    let physical = root.physical_generation_fingerprint();
    let generation = root.generation();
    let v2_bytes = directory_bytes(prepared.directory());
    assert_eq!(root.frontier(), frontier);
    assert_eq!(root.total_rows(), ROWS as u64);
    let v2_matched_results = queries
        .iter()
        .map(|query| {
            query_snapshot(&definition, prepared.snapshot(), query).expect("matched V2 result")
        })
        .collect::<Vec<_>>();
    assert_eq!(v2_matched_results, expected);
    let matched_query_cases = v2_matched_results.len();
    let v2_recovery_ns = stage_samples(SAMPLES, || {
        let reopened = ValidatedColumnarV2Generation::open(
            &v2_directory,
            definition.clone(),
            1,
            generation,
            frontier,
            artifact,
            physical,
        )
        .expect("V2 recovery");
        let actual = queries
            .iter()
            .map(|query| {
                query_snapshot(&definition, reopened.snapshot(), query).expect("recovered V2 query")
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    });
    let v2 = ColumnarEngine::from_validated_v2(definition.clone(), prepared)
        .expect("install validated V2");
    let v2_partition_query_ns = queries
        .iter()
        .map(|query| {
            stage_samples(SAMPLES, || {
                assert_eq!(
                    black_box(&v2).query(black_box(query)).expect("V2 query"),
                    expected[0]
                );
            })
        })
        .collect::<Vec<_>>();
    let v2_query_ns = stage_samples(SAMPLES, || {
        let actual = queries
            .iter()
            .map(|query| black_box(&v2).query(black_box(query)).expect("V2 query"))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    });

    let compaction_start = Instant::now();
    let compacted = ValidatedColumnarV2Generation::prepare(
        &v2_directory,
        definition.clone(),
        1,
        ProjectionGeneration::new(102).expect("compaction generation"),
        FrontierPosition::BeforeFirst,
        &matched,
    )
    .expect("production V2 compaction");
    let compaction_ns = compaction_start.elapsed().as_nanos();
    let compacted_results = queries
        .iter()
        .map(|query| {
            query_snapshot(&definition, compacted.snapshot(), query).expect("compacted V2 query")
        })
        .collect::<Vec<_>>();
    assert_eq!(compacted_results, expected);

    let segment_count = usize::try_from(root.total_segments()).expect("segments");
    let v1_modeled_owned_allocations = ROWS * 6;
    let v2_modeled_owned_allocations = ROWS * 6 + segment_count * 17;
    println!(
        "WP711_LOWER_LEVEL corpus=wp711-low-cardinality-v1-v2-v1 rows={ROWS} partitions={PARTITIONS} rows_per_partition={ROWS_PER_PARTITION} samples={SAMPLES} cpu_method=single-thread_elapsed_ns allocation_method=wp710_modeled_owned_allocations_not_allocator_calls matched_frontier={} matched_query_cases={matched_query_cases} partition_0_result_count={ROWS_PER_PARTITION} partition_1_result_count={ROWS_PER_PARTITION} projection_lag={lag} no_projection_bytes=0 no_projection_modeled_owned_allocations=0 no_projection_population_passes=0 no_v2_bytes={v1_bytes} no_v2_modeled_owned_allocations={v1_modeled_owned_allocations} v1_partition_0_query_p50_ns={} v1_partition_1_query_p50_ns={} v1_query_p50_ns={} v1_query_p95_ns={} v1_query_p99_ns={} v1_recovery_p50_ns={} v1_recovery_p95_ns={} v1_recovery_p99_ns={} v2_bytes={v2_bytes} v2_modeled_owned_allocations={v2_modeled_owned_allocations} v2_partition_0_query_p50_ns={} v2_partition_1_query_p50_ns={} v2_query_p50_ns={} v2_query_p95_ns={} v2_query_p99_ns={} v2_recovery_p50_ns={} v2_recovery_p95_ns={} v2_recovery_p99_ns={} rebuild_ns={rebuild_ns} compaction_ns={compaction_ns} result_count={ROWS}",
        frontier_sequence(frontier),
        percentile(&v1_partition_query_ns[0], 50),
        percentile(&v1_partition_query_ns[1], 50),
        percentile(&v1_query_ns, 50),
        percentile(&v1_query_ns, 95),
        percentile(&v1_query_ns, 99),
        percentile(&v1_recovery_ns, 50),
        percentile(&v1_recovery_ns, 95),
        percentile(&v1_recovery_ns, 99),
        percentile(&v2_partition_query_ns[0], 50),
        percentile(&v2_partition_query_ns[1], 50),
        percentile(&v2_query_ns, 50),
        percentile(&v2_query_ns, 95),
        percentile(&v2_query_ns, 99),
        percentile(&v2_recovery_ns, 50),
        percentile(&v2_recovery_ns, 95),
        percentile(&v2_recovery_ns, 99),
    );
}

fn frontier_sequence(frontier: FrontierPosition) -> u64 {
    match frontier {
        FrontierPosition::BeforeFirst => 0,
        FrontierPosition::AppliedThrough(sequence) => sequence.get(),
    }
}

fn directory_bytes(directory: &Path) -> u64 {
    std::fs::read_dir(directory)
        .expect("read receipt directory")
        .map(|entry| {
            let entry = entry.expect("read receipt member");
            let metadata = entry.metadata().expect("receipt member metadata");
            if metadata.is_dir() {
                directory_bytes(&entry.path())
            } else {
                metadata.len()
            }
        })
        .sum()
}

fn stage_samples(samples: usize, mut operation: impl FnMut()) -> Vec<u128> {
    for _ in 0..5 {
        operation();
    }
    let mut measured = (0..samples)
        .map(|_| {
            let start = Instant::now();
            operation();
            start.elapsed().as_nanos()
        })
        .collect::<Vec<_>>();
    measured.sort_unstable();
    measured
}

fn percentile(samples: &[u128], percentile: usize) -> u128 {
    samples[(samples.len() - 1) * percentile / 100]
}

// req: PRJ-004, PRJ-008, PRJ-009, PRJ-010, OQ-020, OQ-022, OQ-024, PERF-007
#[test]
fn selected_v2_queries_use_open_validated_private_pruning_with_fixed_policy_work() {
    let bundle = compile_bundle();
    let definition = register_ticket_board(&bundle);
    let entity = entity_type_id(&bundle, "Ticket");
    let title = field_id(&bundle, "Ticket", "title");
    let organization = [0x45; 16];
    let org = OrgKey::from_value(&CanonicalValue::Uuid(organization)).expect("organization");
    let mut rows = BTreeMap::new();
    for index in 0..=riffdb_columnar::MAX_SEGMENT_V2_ROWS {
        let title_value = if index == riffdb_columnar::MAX_SEGMENT_V2_ROWS {
            "middle"
        } else if index % 2 == 0 {
            "alpha"
        } else {
            "omega"
        };
        rows.insert(
            ticket_key(entity, &organization, index as u64 + 1),
            row(index as u64 + 1, 1, title_value, 0),
        );
    }
    let frontier =
        FrontierPosition::AppliedThrough(riffdb_types::CommitSequence::new(1).expect("frontier"));
    let mut snapshot = ColumnarSnapshot::empty();
    snapshot.visible_frontier = frontier;
    snapshot.delta.insert(org.clone(), rows);

    let source_directory = temp_dir("v2-selected-private-pruning");
    let generation = ProjectionGeneration::new(20).expect("generation");
    let selected = ValidatedColumnarV2Generation::prepare(
        &source_directory,
        definition.clone(),
        1,
        generation,
        FrontierPosition::BeforeFirst,
        &snapshot,
    )
    .expect("open-validate selected V2");
    assert_eq!(selected.root().total_segments(), 2);

    let request = ColumnarQueryRequest {
        org_scope: CanonicalValue::Uuid(organization),
        select: Vec::new(),
        predicates: vec![ColumnPredicate::Eq {
            field: title,
            value: CanonicalValue::string("middle").expect("needle"),
        }],
        order: Vec::new(),
        limit: None,
        group_by: None,
        aggregate: Some(AggregateOp::Count),
        budget: QueryBudget {
            max_scanned_rows: 1,
            max_group_cardinality: 1,
        },
    };

    let offline_directory = source_directory.join("selected.offline");
    std::fs::rename(selected.directory(), &offline_directory)
        .expect("hide all durable V2 material after selected open");
    for _ in 0..2 {
        assert_eq!(
            query_snapshot(&definition, selected.snapshot(), &request).expect("private pruning"),
            QueryResult::Aggregate(AggregateValue::Count(1)),
            "a within-zone dictionary miss is rejected without any hot durable access"
        );
    }

    let candidate_keys = selected
        .snapshot()
        .merged_org(&org)
        .keys()
        .map(|key| EntityKey::from_bytes(key.as_bytes().to_vec()).expect("candidate key"))
        .collect::<Vec<_>>();
    for admitted in [
        vec![candidate_keys[0].clone(), candidate_keys[1].clone()],
        vec![candidate_keys[2].clone(), candidate_keys[3].clone()],
    ] {
        let admission = AuthorizedProjectedRowAdmissionV1::test_fixture(
            entity,
            candidate_keys.clone(),
            admitted,
        )
        .expect("complete policy proof");
        assert_eq!(
            query_snapshot_with_policy_admission(
                &definition,
                selected.snapshot(),
                &request,
                &admission,
            ),
            Err(QueryError::ScanBudgetExceeded { max: 1 }),
            "policy shape cannot enable statistics or alter the fixed scan-work class"
        );
    }
    std::fs::rename(&offline_directory, selected.directory())
        .expect("restore selected directory for cleanup");
}

// req: PRJ-002, PRJ-004, PRJ-008, PRJ-009, PRJ-010, OQ-020, OQ-022, OQ-024
#[test]
fn columnar_v2_generation_root_is_structurally_atomic_across_partitions() {
    let bundle = compile_bundle();
    let definition = register_ticket_board(&bundle);
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

    let spec = ColumnarProjectionSpecV1::for_scalar(&definition, &bundle).expect("scalar spec");
    let (length, checksum) = prepared.artifact_identity();
    let pointer = StoredColumnarProjectionGenerationV1::prepared_candidate(
        generation,
        ColumnarProjectionLayoutV1::V2,
        applied_frontier,
        applied_frontier,
        1,
        ColumnarProjectionArtifactV1::new(length, checksum).expect("root artifact"),
        definition.fingerprint(),
        spec.hash(),
        Some(*prepared.root().physical_generation_fingerprint().as_bytes()),
    )
    .expect("mismatched snapshot pointer");
    assert!(
        prepared
            .prepared_generation(&spec, pointer, [0x71; 16])
            .is_err(),
        "a validated root cannot mint a witness for another snapshot frontier"
    );

    let exact_pointer = StoredColumnarProjectionGenerationV1::prepared_candidate(
        generation,
        ColumnarProjectionLayoutV1::V2,
        snapshot_frontier,
        applied_frontier,
        1,
        ColumnarProjectionArtifactV1::new(length, checksum).expect("root artifact"),
        definition.fingerprint(),
        spec.hash(),
        Some(*prepared.root().physical_generation_fingerprint().as_bytes()),
    )
    .expect("exact snapshot pointer");
    let reopened = ValidatedColumnarV2Generation::open_prepared_candidate(
        &source_directory,
        definition.clone(),
        &exact_pointer,
    )
    .expect("reopen exact prepared generation");
    assert!(
        reopened
            .prepared_generation(&spec, exact_pointer.clone(), [0x71; 16])
            .is_ok(),
        "only the exact validated V2 root/control identity mints the witness"
    );
    assert!(
        ValidatedColumnarV2Generation::open(
            &source_directory,
            definition.clone(),
            1,
            generation,
            applied_frontier,
            prepared.artifact_identity(),
            prepared.root().physical_generation_fingerprint(),
        )
        .expect("ordinary selected reopen")
        .prepared_generation(&spec, exact_pointer, [0x71; 16])
        .is_err(),
        "a selected reopen without Candidate snapshot evidence cannot mint"
    );
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
fn columnar_v2_member_build_crashes_recover_one_complete_generation() {
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
            .arg("columnar_v2_member_build_crashes_recover_one_complete_generation")
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
fn v2_reclamation_delete_and_parent_sync_are_idempotent() {
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
            .arg("v2_reclamation_delete_and_parent_sync_are_idempotent")
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
