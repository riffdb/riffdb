// req: STO-030, STO-031
use super::*;
use riffdb_storage_api::{
    DatabaseInitializationPort, EvidencePageLimit, ReadableCapabilityDigestInventory,
    ReadableDigestKey, ReadableIdempotencyDigestInventory, StartupValidationInputs,
    StructuralEvidenceCursor, StructuralEvidenceOpen, StructuralEvidencePage,
    StructuralEvidenceSession,
};
use riffdb_storage_redb::{RedbOperationalPorts, RedbStore};
use riffdb_types::{DatabaseId, DigestKeyId, IncidentId, Timestamp};

// req: STO-030, STO-031, END-008
#[test]
fn completed_publication_wait_still_advances_one_captured_observation_page() {
    let mut pages = 0;
    let checkpoint = wait_with_source_validation(
        &LaneControl::default(),
        7,
        (true, || Ok(false)),
        (SourceCheckpointWait::Journal, || Ok(false)),
        |_seen, slice| {
            assert_eq!(slice, RETENTION_LANE_CHECKPOINT_SLICE);
            8
        },
        || Duration::ZERO,
        || {
            pages += 1;
            Ok(false)
        },
    )
    .unwrap();
    assert!(checkpoint);
    assert_eq!(
        pages, 1,
        "completed publication must not starve captured byte work"
    );
}

// req: STO-030, STO-031, END-008
#[test]
fn checkpoint_wait_continues_pages_until_exact_end_without_inventing_a_checkpoint() {
    let control = LaneControl::default();
    let mut waits = 0;
    let mut pages = 0;
    let checkpoint = wait_with_source_validation(
        &control,
        7,
        (true, || Ok(false)),
        (SourceCheckpointWait::None, || {
            panic!("legacy wait must not inspect source publication")
        }),
        |_seen, slice| {
            assert_eq!(slice, Duration::from_millis(100));
            waits += 1;
            if waits == 4 { 8 } else { 7 }
        },
        || Duration::from_millis(100),
        || {
            pages += 1;
            Ok(pages < 2)
        },
    )
    .unwrap();
    assert!(checkpoint);
    assert_eq!((waits, pages), (4, 2));
}

// req: STO-030, STO-031, END-008
#[test]
fn checkpoint_wait_preserves_deadline_and_cancellation_before_page_work() {
    for cancel in [false, true] {
        let control = LaneControl::default();
        let checkpoint = wait_with_source_validation(
            &control,
            7,
            (true, || Ok(false)),
            (SourceCheckpointWait::None, || {
                panic!("legacy wait must not inspect source publication")
            }),
            |_seen, slice| {
                assert_eq!(slice, RETENTION_LANE_CHECKPOINT_SLICE);
                if cancel {
                    control.stop();
                }
                7
            },
            || {
                if cancel {
                    Duration::ZERO
                } else {
                    RETENTION_LANE_RECHECK
                }
            },
            || panic!("expired or cancelled wait must not advance a page"),
        )
        .unwrap();
        assert!(!checkpoint);
    }
    let control = LaneControl::default();
    control.stop();
    assert!(
        !wait_with_source_validation(
            &control,
            7,
            (true, || Ok(false)),
            (SourceCheckpointWait::None, || panic!(
                "legacy wait must not inspect source publication"
            )),
            |_seen, _| panic!("stopped wait must not block"),
            || panic!("stopped wait must not inspect time"),
            || panic!("stopped wait must not advance"),
        )
        .unwrap()
    );
}

// req: STO-030, STO-031, END-008
#[test]
fn checkpoint_wait_does_not_capture_missing_validation_and_propagates_failure() {
    let control = LaneControl::default();
    let mut slices = 0;
    assert!(
        wait_with_source_validation(
            &control,
            7,
            (false, || Ok(false)),
            (SourceCheckpointWait::None, || panic!(
                "legacy wait must not inspect source publication"
            )),
            |_seen, _| {
                slices += 1;
                if slices == 2 { 8 } else { 7 }
            },
            || Duration::ZERO,
            || panic!("no candidate must not start validation"),
        )
        .unwrap()
    );
    assert_eq!(slices, 2);
    let error = wait_with_source_validation(
        &control,
        7,
        (true, || Ok(false)),
        (SourceCheckpointWait::None, || {
            panic!("legacy wait must not inspect source publication")
        }),
        |_seen, _| 7,
        || Duration::ZERO,
        || {
            Err(riffdb_storage_api::StorageError::new(
                riffdb_storage_api::StorageErrorKind::CorruptData,
                None,
            ))
        },
    )
    .unwrap_err();
    assert_eq!(
        error.kind(),
        riffdb_storage_api::StorageErrorKind::CorruptData
    );
}

// req: STO-030, END-007, END-008
#[test]
fn native_retention_refusal_preserves_closed_failure_phase_and_kind() {
    use riffdb_storage_api::StorageErrorKind as K;
    let kinds = [
        (K::Unavailable, 1),
        (K::CommitStatusUnknown, 2),
        (K::CorruptData, 3),
        (K::IncompatibleFormat, 4),
        (K::LimitExceeded, 5),
        (K::InvariantViolation, 6),
        (K::SequenceExhausted, 7),
        (K::HistoryPruned, 8),
    ];
    for (phase, name) in [
        (RetentionRefusalPhase::Step, 1),
        (RetentionRefusalPhase::PrefixRefresh, 2),
    ] {
        for (kind, label) in kinds {
            let line = format_retention_refusal(phase, kind);
            assert_eq!(
                line,
                format!("riffdb-retention-refusal-v1\tphase={name}\tkind={label}")
            );
            assert!(line.len() < 128);
        }
    }
}

// req: STO-030, STO-031, END-008
#[test]
fn completed_source_deletion_is_progress_even_when_logical_retention_is_idle() {
    let idle = OnlineRetentionPhases::default();
    assert_eq!(idle_recheck_pause(&idle), RETENTION_LANE_RECHECK);
    let validating = OnlineRetentionPhases {
        receipt_validation_pending: true,
        ..OnlineRetentionPhases::default()
    };
    assert_eq!(idle_recheck_pause(&validating), Duration::ZERO);
    let planned = OnlineRetentionPhases {
        source_reclamation_attempts: 1,
        source_reclamation_sample: Some([1, 1_000, 900, 0, 0, 900, 0, 0]),
        source_initial_rows: 256,
        source_continuation_rows: 256,
        source_admission_census: [1, 1, 0, 0, 0, 100_000, 17, 0],
        ..OnlineRetentionPhases::default()
    };
    assert_eq!(
        idle_recheck_pause(&planned),
        RETENTION_LANE_RECHECK,
        "a floor plan, completed validation and granted turn are not deletion"
    );
    // These are actual removed receipts in the production phase type, not a
    // planning ceiling or a fabricated pending-validation flag. The separate
    // writer-residence rest remains mandatory after this progress.
    for removed in [1, 256] {
        let progressed = OnlineRetentionPhases {
            reclaimed_receipts: removed,
            receipt_gate_us: 1_000,
            ..OnlineRetentionPhases::default()
        };
        assert_eq!(progressed.gate_held_us(), 1_000);
        assert_eq!(idle_recheck_pause(&progressed), Duration::ZERO);
    }
}

// req: STO-030, STO-031, END-007, END-008
#[test]
fn pending_source_validation_observes_charged_rest_without_polling_full_maintenance() {
    use std::cell::Cell;
    for remaining in [Duration::from_millis(17), Duration::from_secs(19)] {
        let phases = OnlineRetentionPhases {
            receipt_validation_pending: true,
            source_work_wait: remaining,
            ..OnlineRetentionPhases::default()
        };
        let expected = remaining.min(RETENTION_LANE_CHECKPOINT_SLICE);
        let pause = idle_recheck_pause(&phases);
        assert_eq!(
            pause, expected,
            "pending proof work cannot bypass charged source rest"
        );
        let (rest, checkpoint) = maintenance_wait_policy(pause, false, Duration::ZERO, &phases);
        assert!(
            !checkpoint,
            "source rest cannot manufacture checkpoint authority"
        );
        assert_eq!(phases.gate_held_us(), 0, "waiting is not writer residence");
        assert_eq!(
            maintenance_wait_policy(pause, false, Duration::from_secs(1), &phases),
            (Duration::from_secs(19), false),
            "source rest cannot shorten the existing writer-residence rest"
        );
        let clock = Cell::new(Duration::ZERO);
        let pages = Cell::new(0);
        assert!(
            !pause_with_captured_observations(
                rest,
                (true, || Ok(pages.get() == 0)),
                || clock.get(),
                |slice| {
                    clock.set(clock.get() + slice);
                    (false, false)
                },
                || {
                    pages.set(pages.get() + 1);
                    clock.set(clock.get() + Duration::from_millis(1));
                    Ok(false)
                },
            )
            .unwrap()
        );
        assert_eq!(
            pages.get(),
            1,
            "captured read-only work must continue inside source rest"
        );
        assert_eq!(
            clock.get(),
            expected,
            "the recheck cannot consume or reset source rest"
        );
    }
    let ready = OnlineRetentionPhases {
        receipt_validation_pending: true,
        ..OnlineRetentionPhases::default()
    };
    assert_eq!(idle_recheck_pause(&ready), Duration::ZERO);
    let progressed = OnlineRetentionPhases {
        reclaimed_receipts: 256,
        source_work_wait: Duration::from_secs(19),
        ..OnlineRetentionPhases::default()
    };
    assert_eq!(idle_recheck_pause(&progressed), Duration::ZERO);
    let control = LaneControl::default();
    control.stop();
    assert!(control.pause(RETENTION_LANE_CHECKPOINT_SLICE));
}

// req: STO-030, STO-031, END-008
#[test]
fn source_progress_rechecks_preserve_gate_rest_and_idle_backoff() {
    let phases = OnlineRetentionPhases {
        reclaimed_receipts: 256,
        receipt_gate_us: 17,
        prefix_gate_us: 23,
        drain_us: 5,
        plan_us: 7,
        digest_us: 11,
        materialize_us: 13,
        delete_us: 19,
        commit_us: 29,
        gate_wait_us: 100_000,
        source_admission_census: [1, 1, 0, 0, 0, 100_000, 17, 0],
        ..OnlineRetentionPhases::default()
    };
    // The complete native residence is charged once. Neither ordinary nor
    // source queue waits add budget or replace residence with a deadline.
    assert_eq!(phases.gate_held_us(), 124);
    assert_eq!(
        idle_recheck_pause(&phases).max(gate_residence_rest(Duration::from_micros(
            phases.gate_held_us()
        ))),
        Duration::from_micros(2_356)
    );
    assert_eq!(
        gate_residence_rest(Duration::from_micros(phases.gate_held_us())),
        Duration::from_micros(2_356)
    );
    assert_eq!(gate_residence_rest(Duration::ZERO), Duration::ZERO);
    for held in [
        Duration::from_secs(1),
        Duration::from_secs(2),
        Duration::MAX,
    ] {
        assert_eq!(gate_residence_rest(held), Duration::from_secs(19));
    }
    let idle = OnlineRetentionPhases::default();
    assert_eq!(idle_recheck_pause(&idle), Duration::from_secs(1));
    let control = LaneControl::default();
    control.stop();
    for pause in [
        Duration::ZERO,
        Duration::from_micros(2_356),
        Duration::from_secs(19),
    ] {
        assert!(control.pause(pause), "stop cancels every rest policy");
    }
    assert!(control.stop_requested());
    assert!(control.cancellation.load(Ordering::Acquire));
}

// req: STO-030, STO-031, END-008
#[test]
fn source_ceiling_samples_preserve_measurement_age_instead_of_summing_fences() {
    let status = RetentionLaneStatus::new(RetentionLaneState::Idle);
    let first = [1, 50, 40, 20, 30, 20, 1, 3];
    status.note_phases(&OnlineRetentionPhases {
        source_reclamation_attempts: 1,
        source_reclamation_sample: Some(first),
        ..OnlineRetentionPhases::default()
    });
    status.note_phases(&OnlineRetentionPhases {
        source_reclamation_attempts: 1,
        source_reclamation_yields: [0, 1, 0],
        ..OnlineRetentionPhases::default()
    });
    // An independently accounted prefix-refresh phase is not another source
    // attempt and cannot replace a real floor plan with zero-filled fields.
    status.note_phases(&OnlineRetentionPhases {
        prefix_gate_us: 7,
        ..OnlineRetentionPhases::default()
    });
    let prior = status.observation().phases;
    assert_eq!(prior.source_reclamation_attempts, 2);
    assert_eq!(prior.source_reclamation_sample_attempt, 1);
    assert_eq!(prior.source_reclamation_sample, Some(first));
    assert_eq!(prior.source_reclamation_yields, [0, 1, 0]);
    let next = [20, 80, 70, 0, 60, 60, 1, 2];
    status.note_phases(&OnlineRetentionPhases {
        source_reclamation_attempts: 1,
        source_reclamation_sample: Some(next),
        source_reclamation_yields: [0, 0, 1],
        ..OnlineRetentionPhases::default()
    });
    let observed = status.observation().phases;
    assert_eq!(observed.source_reclamation_attempts, 3);
    assert_eq!(observed.source_reclamation_sample_attempt, 3);
    assert_eq!(observed.source_reclamation_sample, Some(next));
    assert_eq!(observed.source_reclamation_yields, [0, 1, 1]);
    assert_eq!(observed.prefix_gate_us, 7);
}

// req: STO-030, STO-031, END-008
#[test]
fn source_queue_wait_is_observed_without_increasing_actual_gate_pacing() {
    let status = RetentionLaneStatus::new(RetentionLaneState::Idle);
    status.note_phases(&OnlineRetentionPhases {
        receipt_gate_us: 17,
        source_admission_census: [1, 1, 0, 0, 0, 75_000, 17, 0],
        ..OnlineRetentionPhases::default()
    });
    status.note_phases(&OnlineRetentionPhases {
        source_admission_census: [1, 0, 1, 0, 0, 100_000, 0, 0],
        ..OnlineRetentionPhases::default()
    });
    let phases = status.observation().phases;
    assert_eq!(phases.gate_held_us(), 17);
    assert_eq!(
        phases.source_admission_census,
        [2, 1, 1, 0, 0, 175_000, 17, 0]
    );
}

#[derive(Default)]
struct RecordingDiagnostics {
    incidents: Mutex<Vec<(IncidentId, DefectScope)>>,
}

impl ServiceDiagnostics for RecordingDiagnostics {
    fn record_internal(&self, error: InternalError) {
        self.incidents
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((*error.incident_id(), error.scope()));
    }
}

struct FixedIncidents;

impl IncidentIdSource for FixedIncidents {
    fn next_incident_id(&self) -> Result<IncidentId, riffdb_errors::IncidentIdSourceError> {
        IncidentId::from_bytes([
            0x01, 0x9b, 0xf6, 0xaa, 0xa6, 0x40, 0x7d, 0xe6, 0x89, 0xc9, 0x8a, 0x7f, 0x70, 0xbb,
            0xbd, 0x61,
        ])
        .map_err(|_| riffdb_errors::IncidentIdSourceError)
    }
}

fn sinks(diagnostics: &Arc<RecordingDiagnostics>) -> RetentionLaneFailureSinks {
    RetentionLaneFailureSinks {
        incident_ids: Arc::new(FixedIncidents),
        diagnostics: diagnostics.clone(),
    }
}

fn inputs() -> StartupValidationInputs {
    let key = ReadableDigestKey::v1(DigestKeyId::new(1).unwrap());
    StartupValidationInputs::new(
        Timestamp::new(1_700_000_000, 0).unwrap(),
        ReadableCapabilityDigestInventory::new(vec![key]).unwrap(),
        ReadableIdempotencyDigestInventory::new(vec![key]).unwrap(),
    )
}

struct Scratch {
    _directory: riffdb_testkit::scratch::ScratchDir,
    path: std::path::PathBuf,
}

/// One initialized source database, stopped.
fn scratch(label: &str) -> Scratch {
    let directory = riffdb_testkit::scratch::ScratchDir::new_in(
        riffdb_testkit::scratch::test_data_root(
            "server-retention-lane",
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target"),
        ),
        "retention-lane",
    )
    .expect("scratch directory");
    let path = directory.join(format!("{label}.redb"));
    RedbStore::open(&path)
        .expect("open scratch store")
        .initialize_database(DatabaseId::from_unix_milliseconds_and_random(1, [0x61; 10]).unwrap())
        .expect("initialize scratch database");
    drop(open_ports(&path));
    Scratch {
        _directory: directory,
        path,
    }
}

/// Opens the scratch source through readiness.
fn open_ports(path: &std::path::Path) -> RedbOperationalPorts {
    let store = RedbStore::open(path).expect("open scratch store");
    let mut session = store
        .begin_structural_evidence(inputs())
        .expect("begin scratch evidence");
    let limit = EvidencePageLimit::new(64).expect("page limit");
    let mut cursor =
        StructuralEvidenceCursor::start(session.database_id(), session.open_session_id());
    let structural_end = loop {
        match session
            .read_structural_evidence(cursor, limit)
            .expect("structural page")
        {
            StructuralEvidencePage::Page { next, .. } => cursor = next,
            StructuralEvidencePage::ExactEnd(end) => break end,
        }
    };
    let (_, historical_end) = riffdb_catalog::validate_catalog_history(&mut session)
        .expect("catalog history")
        .into_parts();
    let riffdb_storage_api::StructuralOpenOutcome::Clean(opened) = session
        .finish(structural_end, historical_end)
        .expect("finish scratch evidence")
    else {
        panic!("scratch source must open cleanly");
    };
    let (_, _, _, dormant) = opened.into_parts();
    dormant
        .into_operational_after_catalog_validation()
        .expect("activate scratch ports")
}

fn retaining(count: u64) -> RetentionConfig {
    RetentionConfig::for_test(
        RetentionTarget::RetainSequences(NonZeroU64::new(count).unwrap()),
        NonZeroU64::new(256).unwrap(),
    )
}

/// Waits on the lane's closed state without sleeping as synchronization.
fn await_state(lane: &RunningRetentionLane, wanted: RetentionLaneState) {
    for _ in 0..10_000_000 {
        if lane.status().state() == wanted {
            return;
        }
        thread::yield_now();
    }
    panic!(
        "lane never reached {wanted}; it is {}",
        lane.status().state()
    );
}

/// ADR-0182 sections 6 and 7 lane health: a hold starts nothing, a lane at
/// its target idles, shutdown cancels it, and a refused step records one
/// internal incident and stops the lane without retrying.
// req: STO-030, STO-031, END-007, END-008
#[test]
fn retention_lane_states_are_closed_and_shutdown_cancels_the_lane() {
    let diagnostics = Arc::new(RecordingDiagnostics::default());

    // A hold runs no thread and removes nothing.
    let source = scratch("hold");
    let ports = open_ports(&source.path);
    let native = ports.online_retention_lane();
    assert!(!native.source_scheduler_bound_fixture().unwrap());
    let mut lane = RunningRetentionLane::start(
        RetentionConfig::default(),
        native.clone(),
        sinks(&diagnostics),
        inputs(),
    )
    .expect("start held lane");
    assert!(native.source_scheduler_bound_fixture().unwrap());
    assert!(lane.thread.is_none(), "a hold starts no lane thread");
    assert_eq!(lane.status().state(), RetentionLaneState::Holding);
    assert_eq!(lane.shutdown(), RetentionLaneState::Holding);

    // An empty history is already at its target: the lane idles, a wake
    // re-checks, and shutdown cancels it from its pause.
    drop(ports);
    let source = scratch("idle");
    let ports = open_ports(&source.path);
    let mut lane = RunningRetentionLane::start(
        retaining(1),
        ports.online_retention_lane(),
        sinks(&diagnostics),
        inputs(),
    )
    .expect("start idle lane");
    await_state(&lane, RetentionLaneState::Idle);
    lane.wake();
    await_state(&lane, RetentionLaneState::Idle);
    // The lane starts idle, so wait on the counter itself, not the state.
    let mut spins = 0_u32;
    while lane.observation().holds[0] == 0 {
        spins += 1;
        assert!(
            spins < 10_000_000,
            "an idle lane counts its target-reached holds"
        );
        thread::yield_now();
    }
    let phases = lane.observation().phases;
    assert_eq!(
        phases
            .gate_held_us()
            .saturating_sub(phases.receipt_gate_us)
            .saturating_sub(phases.prefix_gate_us),
        0,
        "logical planning at the target never takes the writer gate; receipt pin capture is counted separately"
    );
    assert_eq!(phases.reclaimed_receipts, 0);
    assert_eq!(lane.shutdown(), RetentionLaneState::Cancelled);
    assert!(
        crate::shutdown_census::format_v1_line().contains("\tonline_retention=cancelled\t"),
        "shutdown evidence must describe the joined owner, not its live state"
    );
    assert_eq!(
        lane.observation(),
        RetentionLaneObservation {
            state: RetentionLaneState::Cancelled,
            fence: None,
            steps: 0,
            sequences: 0,
            phases: lane.observation().phases,
            holds: lane.observation().holds,
        }
    );
    assert!(diagnostics.incidents.lock().unwrap().is_empty());

    drop(ports);
    // An unreadable fencing source refuses the step: one internal incident,
    // the lane stops, and it never transitions again.
    let source = scratch("refused");
    riffdb_storage_redb::plant_unreadable_projection_frontier_fixture(&source.path)
        .expect("plant unreadable projection frontier");
    let ports = open_ports(&source.path);
    let mut lane = RunningRetentionLane::start(
        retaining(1),
        ports.online_retention_lane(),
        sinks(&diagnostics),
        inputs(),
    )
    .expect("start refused lane");
    await_state(&lane, RetentionLaneState::Refused);
    assert_eq!(lane.shutdown(), RetentionLaneState::Refused);
    assert!(
        crate::shutdown_census::format_v1_line().contains("\tonline_retention=refused\t"),
        "joining a refused owner must preserve its actual terminal failure"
    );
    assert_eq!(
        *diagnostics.incidents.lock().unwrap(),
        vec![(
            FixedIncidents.next_incident_id().unwrap(),
            DefectScope::Process
        )],
        "exactly one internal incident is recorded"
    );

    // The closed registry, by name.
    let names = [
        RetentionLaneState::Holding,
        RetentionLaneState::Idle,
        RetentionLaneState::Advancing,
        RetentionLaneState::Fenced,
        RetentionLaneState::Refused,
        RetentionLaneState::Cancelled,
    ]
    .map(RetentionLaneState::as_str);
    assert_eq!(
        names,
        [
            "holding",
            "idle",
            "advancing",
            "fenced",
            "refused",
            "cancelled"
        ]
    );
    let status = RetentionLaneStatus::new(RetentionLaneState::Idle);
    for binding in FENCE_BINDINGS {
        status.note_fence(Some(binding));
        assert_eq!(status.fence(), Some(binding), "every binding round-trips");
    }
    assert_eq!(
        fence_name(Some(RetentionFenceBinding::ProjectionDurableFrontier)),
        "projection_durable_frontier"
    );
    status.note_fence(None);
    assert_eq!(status.fence(), None);
    for state in [RetentionLaneState::Refused, RetentionLaneState::Cancelled] {
        let status = RetentionLaneStatus::new(state);
        status.set(RetentionLaneState::Advancing);
        assert_eq!(status.state(), state, "terminal states never transition");
    }
}

// req: STO-030, STO-031, END-007, END-008
#[test]
fn writer_rest_continues_captured_physical_pages_without_starting_another_writer() {
    use std::cell::Cell;
    // An idle lane's old one-second pause cannot finish a 31-page census
    // inside its unchanged 30-second lifetime. Exercise the actual worker
    // wait policy with a deterministic clock, without wall-clock sleeps.
    let clock = Cell::new(Duration::ZERO);
    let pages = Cell::new(0);
    let waits = Cell::new(0);
    let rest = Duration::from_secs(19);
    assert!(
        !pause_with_captured_observations(
            rest,
            (true, || Ok(false)),
            || clock.get(),
            |slice| {
                waits.set(waits.get() + 1);
                clock.set(clock.get() + slice);
                (false, false)
            },
            || {
                pages.set(pages.get() + 1);
                // Charge actual page execution time to the same rest clock.
                clock.set(clock.get() + Duration::from_millis(10));
                Ok(pages.get() < 31)
            },
        )
        .unwrap()
    );
    assert_eq!(
        pages.get(),
        31,
        "captured pages must not wait for writer turns"
    );
    assert_eq!(
        clock.get(),
        rest,
        "read-only work cannot shorten writer rest"
    );
    assert_eq!(
        waits.get(),
        32,
        "exact end stops page work and sleeps the remainder"
    );
}

// req: STO-030, STO-031, END-007, END-008
#[test]
fn physical_census_rest_preserves_cancellation_wake_and_no_candidate_backoff() {
    for pending in [false, true] {
        for (stopped, woke) in [(true, false), (false, true)] {
            assert_eq!(
                pause_with_captured_observations(
                    Duration::from_secs(19),
                    (pending, || Ok(false)),
                    || Duration::ZERO,
                    |_| (stopped, woke),
                    || panic!("stop or explicit wake must precede physical page work"),
                )
                .unwrap(),
                stopped
            );
        }
    }
    let clock = std::cell::Cell::new(Duration::ZERO);
    let mut waits = 0;
    assert!(
        !pause_with_captured_observations(
            RETENTION_LANE_RECHECK,
            (false, || Ok(false)),
            || clock.get(),
            |slice| {
                assert_eq!(slice, RETENTION_LANE_RECHECK);
                clock.set(slice);
                waits += 1;
                (false, false)
            },
            || panic!("idle wait must not capture a new census"),
        )
        .unwrap()
    );
    assert_eq!(waits, 1);
    let error = pause_with_captured_observations(
        RETENTION_LANE_RECHECK,
        (true, || Ok(false)),
        || Duration::ZERO,
        |_| (false, false),
        || {
            Err(riffdb_storage_api::StorageError::new(
                riffdb_storage_api::StorageErrorKind::InvariantViolation,
                None,
            ))
        },
    )
    .unwrap_err();
    assert_eq!(
        error.kind(),
        riffdb_storage_api::StorageErrorKind::InvariantViolation
    );
}

// req: STO-030, STO-031, END-007, END-008
#[test]
fn physical_census_rest_charges_page_time_and_does_not_work_past_the_rest_deadline() {
    let clock = std::cell::Cell::new(Duration::ZERO);
    let mut pages = 0;
    assert!(
        !pause_with_captured_observations(
            Duration::from_millis(150),
            (true, || Ok(false)),
            || clock.get(),
            |slice| {
                clock.set(clock.get() + slice);
                (false, false)
            },
            || {
                pages += 1;
                clock.set(clock.get() + Duration::from_millis(200));
                Ok(true)
            },
        )
        .unwrap()
    );
    assert_eq!(pages, 1);
    assert_eq!(clock.get(), Duration::from_millis(300));
    assert!(
        pause_with_captured_observations(
            Duration::ZERO,
            (true, || Ok(false)),
            || panic!("zero rest must not inspect the clock"),
            |slice| {
                assert!(slice.is_zero());
                (true, false)
            },
            || panic!("zero rest must not advance a page"),
        )
        .unwrap()
    );
}

// req: STO-030, STO-031, END-007, END-008
#[test]
fn source_checkpoint_rechecks_wait_for_completion_after_the_entire_writer_rest() {
    for checkpoint_publication in [false, true] {
        let phases = OnlineRetentionPhases {
            source_admission_pending_journal: u64::from(!checkpoint_publication),
            source_admission_checkpoint: u64::from(checkpoint_publication),
            receipt_gate_us: 500,
            source_admission_census: [1, 1, 0, 0, 0, 100_000, 500, 0],
            ..OnlineRetentionPhases::default()
        };
        // Native source work can need a checkpoint while logical retention
        // remains fenced or at target. Ordinary idle backoff misses the small
        // post-checkpoint suffix under continuing foreground publication.
        let (rest, checkpoint) = maintenance_wait_policy(
            RETENTION_LANE_RECHECK,
            false,
            Duration::from_micros(phases.gate_held_us()),
            &phases,
        );
        assert!(
            checkpoint,
            "source readiness must select checkpoint completion"
        );
        assert_eq!(rest, Duration::from_micros(9_500));
        let clock = std::cell::Cell::new(Duration::ZERO);
        assert!(
            !pause_with_captured_observations(
                rest,
                (false, || Ok(false)),
                || clock.get(),
                |slice| {
                    clock.set(clock.get() + slice);
                    (false, false)
                },
                || panic!("no physical candidate must not create one"),
            )
            .unwrap()
        );
        assert!(
            wait_with_source_validation(
                &LaneControl::default(),
                7,
                (false, || Ok(false)),
                (SourceCheckpointWait::from_phases(&phases), || Ok(false)),
                |_seen, _| {
                    assert_eq!(clock.get(), Duration::from_micros(9_500));
                    // Completion during mandatory rest remains visible through
                    // the counter captured before the native source attempt.
                    8
                },
                || Duration::ZERO,
                || panic!("a completed checkpoint must precede page work"),
            )
            .unwrap()
        );
    }
}

// req: STO-030, STO-031, END-007, END-008
#[test]
fn source_checkpoint_wait_policy_preserves_idle_and_noncheckpoint_refusal_backoff() {
    for phases in [
        OnlineRetentionPhases::default(),
        OnlineRetentionPhases {
            source_admission_busy_writer: 1,
            ..OnlineRetentionPhases::default()
        },
        OnlineRetentionPhases {
            source_admission_qualification: 1,
            ..OnlineRetentionPhases::default()
        },
        OnlineRetentionPhases {
            source_admission_page_bound: 1,
            ..OnlineRetentionPhases::default()
        },
    ] {
        assert_eq!(
            maintenance_wait_policy(RETENTION_LANE_RECHECK, false, Duration::ZERO, &phases),
            (RETENTION_LANE_RECHECK, false)
        );
    }
    let pending = OnlineRetentionPhases {
        source_admission_pending_journal: 1,
        ..OnlineRetentionPhases::default()
    };
    assert_eq!(
        maintenance_wait_policy(RETENTION_LANE_RECHECK, false, Duration::ZERO, &pending),
        (Duration::ZERO, true)
    );
    assert_eq!(
        maintenance_wait_policy(
            Duration::ZERO,
            true,
            Duration::from_secs(1),
            &OnlineRetentionPhases::default()
        ),
        (Duration::from_secs(19), false)
    );
}

// req: STO-030, STO-031, END-007, END-008
#[test]
fn source_journal_wait_requires_a_real_checkpoint_then_completed_publication() {
    let waits = std::cell::Cell::new(0);
    let mut publications = 0;
    let mut pages = 0;
    let observed = wait_with_source_validation(
        &LaneControl::default(),
        7,
        (true, || Ok(false)),
        (SourceCheckpointWait::Journal, || {
            assert!(
                waits.get() >= 2,
                "ordinary writer must start and finish its checkpoint first"
            );
            publications += 1;
            Ok(publications == 1)
        }),
        |_seen, _| {
            waits.set(waits.get() + 1);
            assert!(waits.get() <= 3);
            if waits.get() == 1 { 7 } else { 8 }
        },
        || Duration::ZERO,
        || {
            pages += 1;
            Ok(true)
        },
    )
    .unwrap();
    assert!(observed);
    assert_eq!((waits.get(), publications, pages), (3, 2, 3));
}

// req: STO-030, STO-031, END-007, END-008
#[test]
fn source_publication_wait_rechecks_a_finished_sealer_without_inventing_a_checkpoint() {
    let mut waits = 0;
    let mut publications = 0;
    let observed = wait_with_source_validation(
        &LaneControl::default(),
        7,
        (false, || Ok(false)),
        (SourceCheckpointWait::Publication, || {
            publications += 1;
            Ok(publications == 1)
        }),
        |_seen, _| {
            waits += 1;
            assert!(waits <= 2);
            7
        },
        || Duration::ZERO,
        || panic!("publication waiting must not capture source validation"),
    )
    .unwrap();
    assert!(
        !observed,
        "sealing completion is not a new checkpoint count"
    );
    assert_eq!((waits, publications), (2, 2));
}

// req: STO-030, STO-031, END-007, END-008
#[test]
fn source_journal_wait_preserves_its_deadline_without_a_checkpoint_or_metadata_query() {
    let waits = std::cell::Cell::new(0);
    assert!(
        !wait_with_source_validation(
            &LaneControl::default(),
            7,
            (false, || Ok(false)),
            (SourceCheckpointWait::Journal, || panic!(
                "no completed checkpoint must not permit retry"
            )),
            |waiting_seen, _| {
                assert_eq!(waiting_seen, if waits.get() < 2 { 7 } else { 8 });
                waits.set(waits.get() + 1);
                7
            },
            || if waits.get() >= 2 {
                RETENTION_LANE_RECHECK
            } else {
                Duration::ZERO
            },
            || panic!("no validation candidate must not be started"),
        )
        .unwrap()
    );
    assert_eq!(waits.get(), 2);
}

// req: STO-030, STO-031, END-007, END-008
#[test]
fn source_publication_wait_preserves_cancellation_and_typed_metadata_failure() {
    let control = LaneControl::default();
    assert!(
        !wait_with_source_validation(
            &control,
            7,
            (true, || Ok(false)),
            (SourceCheckpointWait::Publication, || panic!(
                "cancel must precede metadata query"
            )),
            |_seen, _| {
                control.stop();
                7
            },
            || Duration::ZERO,
            || panic!("cancel must precede page work"),
        )
        .unwrap()
    );
    let error = wait_with_source_validation(
        &LaneControl::default(),
        7,
        (false, || Ok(false)),
        (SourceCheckpointWait::Publication, || {
            Err(riffdb_storage_api::StorageError::new(
                riffdb_storage_api::StorageErrorKind::Unavailable,
                None,
            ))
        }),
        |_seen, _| 7,
        || Duration::ZERO,
        || panic!("metadata failure must not advance validation"),
    )
    .unwrap_err();
    assert_eq!(
        error.kind(),
        riffdb_storage_api::StorageErrorKind::Unavailable
    );
}

// req: STO-030, STO-031, STO-032, END-007, END-008
#[test]
fn ready_read_only_pages_finish_without_polling_sleeps_and_preserve_full_writer_rest() {
    use std::cell::Cell;
    let clock = Cell::new(Duration::ZERO);
    let pages = Cell::new(0_u64);
    let rest = Duration::from_secs(19);
    // A 77,103-row interval needs 302 bounded pages. A mandatory 100-ms
    // checkpoint poll per page already exceeds 30 seconds, before real work.
    assert!(
        !pause_with_captured_observations(
            rest,
            (true, || Ok(pages.get() < 302)),
            || clock.get(),
            |slice| {
                clock.set(clock.get() + slice);
                (false, false)
            },
            || {
                pages.set(pages.get() + 1);
                clock.set(clock.get() + Duration::from_millis(20));
                Ok(pages.get() < 302)
            },
        )
        .unwrap()
    );
    assert_eq!(
        pages.get(),
        302,
        "ready captured pages must not wait for checkpoint polls"
    );
    assert_eq!(
        clock.get(),
        rest,
        "read-only execution must preserve the entire writer rest"
    );
}

// req: STO-030, STO-031, STO-032, END-007, END-008
#[test]
fn ready_read_only_pages_progress_inside_checkpoint_wait_without_source_authority() {
    use std::cell::Cell;
    let clock = Cell::new(Duration::ZERO);
    let pages = Cell::new(0_u64);
    assert!(
        !wait_with_source_validation(
            &LaneControl::default(),
            7,
            (true, || Ok(pages.get() < 30)),
            (SourceCheckpointWait::Publication, || Ok(pages.get() < 30)),
            |_seen, slice| {
                clock.set(clock.get() + slice);
                7
            },
            || clock.get(),
            || {
                pages.set(pages.get() + 1);
                clock.set(clock.get() + Duration::from_millis(5));
                Ok(pages.get() < 30)
            },
        )
        .unwrap()
    );
    assert_eq!(
        pages.get(),
        30,
        "read-only work must progress while publication is pending"
    );
    assert!(clock.get() < RETENTION_LANE_RECHECK);
}

// req: STO-030, STO-031, STO-032, END-007, END-008
#[test]
fn ready_read_only_pages_check_stop_and_wake_before_every_page() {
    for (stopped, woke) in [(true, false), (false, true)] {
        assert_eq!(
            pause_with_captured_observations(
                Duration::from_secs(19),
                (true, || Ok(true)),
                || Duration::ZERO,
                |slice| {
                    assert!(slice.is_zero());
                    (stopped, woke)
                },
                || panic!("stop or wake must precede every ready captured page"),
            )
            .unwrap(),
            stopped
        );
    }
    let control = LaneControl::default();
    control.wake();
    assert_eq!(
        control.pause_observation_slice(Duration::ZERO),
        (false, true)
    );
    assert_eq!(
        control.pause_observation_slice(Duration::ZERO),
        (false, false)
    );
    control.stop();
    assert_eq!(
        control.pause_observation_slice(Duration::ZERO),
        (true, false)
    );
}

// req: STO-030, STO-031, STO-032, END-007, END-008
#[test]
fn read_only_readiness_failure_never_advances_a_page_or_changes_checkpoint_authority() {
    let error = pause_with_captured_observations(
        Duration::from_secs(19),
        (true, || {
            Err(riffdb_storage_api::StorageError::new(
                riffdb_storage_api::StorageErrorKind::InvariantViolation,
                None,
            ))
        }),
        || Duration::ZERO,
        |_| panic!("readiness failure must propagate before waiting"),
        || panic!("readiness failure must not advance any page"),
    )
    .unwrap_err();
    assert_eq!(
        error.kind(),
        riffdb_storage_api::StorageErrorKind::InvariantViolation
    );
    let error = wait_with_source_validation(
        &LaneControl::default(),
        7,
        (true, || {
            Err(riffdb_storage_api::StorageError::new(
                riffdb_storage_api::StorageErrorKind::InvariantViolation,
                None,
            ))
        }),
        (SourceCheckpointWait::Publication, || {
            panic!("readiness failure grants no publication")
        }),
        |_, _| panic!("readiness failure must not observe a checkpoint"),
        || Duration::ZERO,
        || panic!("readiness failure must not advance any page"),
    )
    .unwrap_err();
    assert_eq!(
        error.kind(),
        riffdb_storage_api::StorageErrorKind::InvariantViolation
    );
}
