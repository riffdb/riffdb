#![forbid(unsafe_code)]
//! ADR-0250 on the path production actually wires.
//!
//! The record's implementation was applied to `ProductionServiceDiagnostics`,
//! which is constructed only inside a test module and never by a running
//! server. The graph wires `ProductionObservabilityDiagnostics`, which forwards
//! `record_internal` to `Observability` -- a second bounded registry, also
//! capped at 256, which also fails authoritative readiness when full and takes
//! no account of a defect's scope.
//!
//! Both constants are 256, so the daemon that died at demand 258 matched either
//! implementation equally well. That is how the wrong one came to be fixed.
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use riffdb_errors::{DefectScope, IncidentIdSource, IncidentIdSourceError, InternalError};
use riffdb_observability::{
    AuthoritativeComponent, AuthoritativeCondition, DEFECT_BURST_CAPACITY, DEFECT_REFILL_INTERVAL,
    DefectClock, MAX_RETAINED_INCIDENTS, Observability, ServiceDiagnostics,
};
use riffdb_types::IncidentId;

/// Marks every authoritative component healthy, as startup does.
fn ready(observability: &Observability) {
    for component in [
        AuthoritativeComponent::Storage,
        AuthoritativeComponent::Catalog,
        AuthoritativeComponent::CommitCoordinator,
    ] {
        observability
            .health()
            .set_authoritative(component, AuthoritativeCondition::Healthy);
    }
    assert!(
        observability.health().snapshot().authoritative_ready(),
        "the registry is ready once startup has proved its components"
    );
}

/// A clock the test moves by hand, so a refill is asserted rather than waited
/// for.
struct ManualClock {
    now: std::sync::Mutex<std::time::Instant>,
}

impl ManualClock {
    fn new() -> Self {
        Self {
            now: std::sync::Mutex::new(std::time::Instant::now()),
        }
    }

    fn advance(&self, by: std::time::Duration) {
        *self.now.lock().expect("manual clock") += by;
    }
}

impl DefectClock for ManualClock {
    fn now(&self) -> std::time::Instant {
        *self.now.lock().expect("manual clock")
    }
}

#[derive(Default)]
struct CountingSource {
    minted: AtomicU64,
}

impl IncidentIdSource for CountingSource {
    fn next_incident_id(&self) -> Result<IncidentId, IncidentIdSourceError> {
        let ordinal = self.minted.fetch_add(1, Ordering::Relaxed) + 1;
        let mut bytes = [0x11_u8; 16];
        bytes[..8].copy_from_slice(&ordinal.to_be_bytes());
        bytes[6] = 0x70 | (bytes[6] & 0x0F);
        bytes[8] = 0x80 | (bytes[8] & 0x3F);
        IncidentId::from_bytes(bytes).map_err(|_| IncidentIdSourceError)
    }
}

#[derive(Debug)]
struct TestSource;

impl std::fmt::Display for TestSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("test internal source")
    }
}

impl std::error::Error for TestSource {}

fn defect(scope: DefectScope, source: &Arc<CountingSource>) -> InternalError {
    let incident = source.next_incident_id().expect("incident id");
    InternalError::new(incident, scope, TestSource)
}

/// OBL-0250-1. A defect one client can reach, repeated far past both the
/// retention bound and the burst capacity, leaves the runtime serving.
#[test]
fn a_client_reachable_defect_path_cannot_stop_the_runtime() {
    let source = Arc::new(CountingSource::default());
    let observability =
        Observability::new(Arc::clone(&source) as Arc<dyn IncidentIdSource>, 64).expect("build");

    // A fresh registry is not ready until startup proves its components, which
    // is what the graph does before any request can arrive.
    ready(&observability);

    for _ in 0..(MAX_RETAINED_INCIDENTS * 2) {
        ServiceDiagnostics::record_internal(&observability, defect(DefectScope::Request, &source));
    }

    assert!(
        observability.health().snapshot().authoritative_ready(),
        "no number of request-scoped defects may fail authoritative readiness"
    );
}

/// OBL-0250-2. Narrowing what feeds the breaker must not make it fail open: a
/// burst of process-scoped defects still stops the runtime.
#[test]
fn a_burst_of_process_scoped_defects_still_stops_the_runtime() {
    let source = Arc::new(CountingSource::default());
    let observability =
        Observability::new(Arc::clone(&source) as Arc<dyn IncidentIdSource>, 64).expect("build");
    ready(&observability);

    for _ in 0..DEFECT_BURST_CAPACITY {
        ServiceDiagnostics::record_internal(&observability, defect(DefectScope::Process, &source));
        assert!(
            observability.health().snapshot().authoritative_ready(),
            "the budget covers exactly its burst capacity"
        );
    }

    ServiceDiagnostics::record_internal(&observability, defect(DefectScope::Process, &source));
    assert!(
        !observability.health().snapshot().authoritative_ready(),
        "the defect past the burst capacity fails readiness"
    );
}

/// Retention bounds memory and nothing more: the registry keeps its cap and
/// drops the oldest, rather than refusing.
#[test]
fn retention_rings_rather_than_refusing() {
    let source = Arc::new(CountingSource::default());
    let observability =
        Observability::new(Arc::clone(&source) as Arc<dyn IncidentIdSource>, 64).expect("build");
    ready(&observability);

    for _ in 0..(MAX_RETAINED_INCIDENTS + 8) {
        ServiceDiagnostics::record_internal(&observability, defect(DefectScope::Request, &source));
    }

    assert_eq!(
        observability.incident_snapshot().len(),
        MAX_RETAINED_INCIDENTS,
        "the registry retains its cap and no more"
    );
}

/// OBL-0250-3. A spent budget recovers with time, so defects spread across a
/// long uptime never accumulate into a shutdown. The clock is advanced rather
/// than slept on.
#[test]
fn the_defect_budget_refills_over_time() {
    let clock = Arc::new(ManualClock::new());
    let source = Arc::new(CountingSource::default());
    let observability = Observability::with_defect_clock(
        Arc::clone(&source) as Arc<dyn IncidentIdSource>,
        64,
        Arc::clone(&clock) as Arc<dyn DefectClock>,
    )
    .expect("build");
    ready(&observability);

    for _ in 0..DEFECT_BURST_CAPACITY {
        ServiceDiagnostics::record_internal(&observability, defect(DefectScope::Process, &source));
    }
    assert!(
        observability.health().snapshot().authoritative_ready(),
        "the burst is exactly covered"
    );

    clock.advance(DEFECT_REFILL_INTERVAL);
    ServiceDiagnostics::record_internal(&observability, defect(DefectScope::Process, &source));
    assert!(
        observability.health().snapshot().authoritative_ready(),
        "one refill interval returns one unit of budget"
    );

    // Spending the refilled unit leaves the budget empty again, so the next
    // defect with no further time fails readiness. Without this the test would
    // also pass against a breaker that never fires.
    ServiceDiagnostics::record_internal(&observability, defect(DefectScope::Process, &source));
    assert!(
        !observability.health().snapshot().authoritative_ready(),
        "the refill is one unit, not a reset"
    );
}
