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
fn checkpoint_wait_continues_pages_until_exact_end_without_inventing_a_checkpoint() {
    let control = LaneControl::default();
    let mut waits = 0;
    let mut pages = 0;
    let checkpoint = wait_with_source_validation(
        &control,
        7,
        true,
        |slice| {
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
            true,
            |slice| {
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
            true,
            |_| panic!("stopped wait must not block"),
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
            false,
            |_| {
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
        true,
        |_| 7,
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
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target"),
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
    let mut lane = RunningRetentionLane::start(
        RetentionConfig::default(),
        ports.online_retention_lane(),
        sinks(&diagnostics),
        inputs(),
    )
    .expect("start held lane");
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
        !pause_with_physical_observation(
            rest,
            true,
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
                pause_with_physical_observation(
                    Duration::from_secs(19),
                    pending,
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
        !pause_with_physical_observation(
            RETENTION_LANE_RECHECK,
            false,
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
    let error = pause_with_physical_observation(
        RETENTION_LANE_RECHECK,
        true,
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
        !pause_with_physical_observation(
            Duration::from_millis(150),
            true,
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
        pause_with_physical_observation(
            Duration::ZERO,
            true,
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
