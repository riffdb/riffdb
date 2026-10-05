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
