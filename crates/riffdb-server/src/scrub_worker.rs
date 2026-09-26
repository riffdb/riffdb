#![expect(
    clippy::expect_used,
    reason = "a running scrub worker retains exactly one owned task until shutdown"
)]

//! ADR-0182 background scrub: the complete exact-end validation over a
//! least-authority MVCC reader after readiness.
//!
//! The scrub exposes exactly the closed states `pending`, `running`,
//! `complete`, `failed`, and `cancelled`. It advances under a rows-per-step
//! rate bound (one bounded page per scheduling step, never wall-clock terms),
//! is cancelled by shutdown, and a failed scrub degrades the `Storage`
//! authoritative component with an incident identifier without repairing,
//! hiding, or retrying past the incident. Progress is process-local; a
//! restart restarts the scrub.

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};

use riffdb_errors::{DefectScope, IncidentIdSource, InternalError};
use riffdb_observability::{ServiceDiagnostics, ServiceHealthHooks};
use riffdb_storage_api::{EvidencePageLimit, StorageError, StorageErrorKind};
use riffdb_storage_redb::{BackgroundScrubPage, RedbSharedPorts};
use riffdb_types::IncidentId;

/// Closed background-scrub states, exactly the five ADR-0182 Decision 3 names.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum BackgroundScrubState {
    /// The scrub is armed and has not begun walking.
    Pending,
    /// The scrub is walking its bounded pages.
    Running,
    /// Both streams reached their exact ends with zero findings.
    Complete,
    /// The scrub observed a finding or failed; the Storage component is
    /// degraded with the recorded incident.
    Failed,
    /// Shutdown cancelled the scrub before completion.
    Cancelled,
}

impl BackgroundScrubState {
    /// Closed state discriminant for bounded observation.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Complete => "complete",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

impl fmt::Display for BackgroundScrubState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// The scrub's bounded work observation for the ADR-0182 evidence: closed
/// state, rows walked, total time, and rows per second when complete.
/// Redaction-safe: counters only, no hash, frontier, key, path, or value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ScrubObservation {
    pub(crate) state: BackgroundScrubState,
    pub(crate) rows_walked: u64,
    pub(crate) total_us: Option<u64>,
    pub(crate) rows_per_second: Option<u64>,
}

/// Rows per scheduling step: one page of at most this many rows, then the
/// worker yields. The bound is expressed in rows, never wall-clock terms,
/// exactly as ADR-0182 Decision 3 requires.
pub(crate) const BACKGROUND_SCRUB_ROWS_PER_STEP: u32 = 64;

/// Redaction-safe, process-local scrub status. Carries no hash, frontier,
/// key, path, or application value; the row counters are the scrub's own
/// bounded work observations for the ADR-0182 evidence.
#[derive(Clone)]
pub(crate) struct BackgroundScrubStatus {
    state: Arc<AtomicU8>,
    rows_walked: Arc<AtomicU64>,
    started_at: Arc<Mutex<Option<std::time::Instant>>>,
    finished_at: Arc<Mutex<Option<std::time::Instant>>>,
    incident: Arc<Mutex<Option<IncidentId>>>,
}

const STATE_PENDING: u8 = 0;
const STATE_RUNNING: u8 = 1;
const STATE_COMPLETE: u8 = 2;
const STATE_FAILED: u8 = 3;
const STATE_CANCELLED: u8 = 4;

impl BackgroundScrubStatus {
    /// A fresh process-local status. Every process starts at `pending`.
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(AtomicU8::new(STATE_PENDING)),
            rows_walked: Arc::new(AtomicU64::new(0)),
            started_at: Arc::new(Mutex::new(None)),
            finished_at: Arc::new(Mutex::new(None)),
            incident: Arc::new(Mutex::new(None)),
        }
    }

    /// Applies one ADR-0203 Decision 4 transition: `pending -> running |
    /// cancelled` and `running -> complete | failed | cancelled`. Terminal
    /// states never transition; an invalid request is ignored and reported.
    fn transition(&self, to: u8) -> bool {
        let valid_from: &[u8] = match to {
            STATE_RUNNING => &[STATE_PENDING],
            STATE_CANCELLED => &[STATE_PENDING, STATE_RUNNING],
            STATE_COMPLETE | STATE_FAILED => &[STATE_RUNNING],
            _ => &[],
        };
        valid_from.iter().any(|from| {
            self.state
                .compare_exchange(*from, to, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        })
    }

    fn fail(&self, incident: Option<IncidentId>) {
        *self
            .incident
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = incident;
        self.note_finished();
        let _ = self.transition(STATE_FAILED);
    }

    /// The authenticated-health member (ADR-0203 Decision 4), or `None` for a
    /// failure that could not be assigned an incident identity; that case has
    /// already failed readiness, so no FAILED member without its incident is
    /// ever published.
    pub(crate) fn health(&self) -> Option<riffdb_service::StorageScrubHealth> {
        use riffdb_service::{StorageScrubHealth, StorageScrubHealthState};
        let state = match self.state() {
            BackgroundScrubState::Pending => StorageScrubHealthState::Pending,
            BackgroundScrubState::Running => StorageScrubHealthState::Running,
            BackgroundScrubState::Complete => StorageScrubHealthState::Complete,
            BackgroundScrubState::Cancelled => StorageScrubHealthState::Cancelled,
            BackgroundScrubState::Failed => {
                return self.incident().map(StorageScrubHealth::failed);
            }
        };
        StorageScrubHealth::new(state).ok()
    }

    /// The incident identity recorded with a failure.
    pub(crate) fn incident(&self) -> Option<IncidentId> {
        *self
            .incident
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn note_running(&self) {
        *self
            .started_at
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(std::time::Instant::now());
    }

    fn note_finished(&self) {
        *self
            .finished_at
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(std::time::Instant::now());
    }

    fn note_rows(&self, rows: u64) {
        let _ = self.rows_walked.fetch_add(rows, Ordering::AcqRel);
    }

    /// The closed scrub state.
    pub(crate) fn state(&self) -> BackgroundScrubState {
        match self.state.load(Ordering::Acquire) {
            STATE_PENDING => BackgroundScrubState::Pending,
            STATE_RUNNING => BackgroundScrubState::Running,
            STATE_COMPLETE => BackgroundScrubState::Complete,
            STATE_FAILED => BackgroundScrubState::Failed,
            STATE_CANCELLED => BackgroundScrubState::Cancelled,
            _ => BackgroundScrubState::Failed,
        }
    }

    /// Rows the scrub walked (bounded work observation for evidence).
    pub(crate) fn rows_walked(&self) -> u64 {
        self.rows_walked.load(Ordering::Acquire)
    }

    /// Total scrub time in microseconds, or `None` before a terminal state.
    pub(crate) fn total_us(&self) -> Option<u64> {
        let started = {
            let guard = self
                .started_at
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (*guard)?
        };
        let finished = {
            let guard = self
                .finished_at
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            (*guard)?
        };
        Some(u64::try_from(finished.duration_since(started).as_micros()).unwrap_or(u64::MAX))
    }

    /// Rows per second over the completed walk, or `None` before completion.
    pub(crate) fn rows_per_second(&self) -> Option<u64> {
        let rows = self.rows_walked();
        let total_us = self.total_us()?;
        if total_us == 0 {
            return None;
        }
        Some(rows.saturating_mul(1_000_000) / total_us)
    }
}

struct ScrubStopState {
    requested: Mutex<bool>,
    changed: Condvar,
}

impl ScrubStopState {
    fn request(&self) {
        let mut requested = self
            .requested
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *requested = true;
        self.changed.notify_all();
    }

    fn is_requested(&self) -> bool {
        *self
            .requested
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// A running background scrub owner. Dropping the handle does not cancel the
/// scrub; shutdown does.
pub(crate) struct RunningScrubWorker {
    status: BackgroundScrubStatus,
    stop: Arc<ScrubStopState>,
    thread: Option<JoinHandle<()>>,
}

/// Why the scrub worker stopped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScrubStopObservation {
    /// Both streams reached exact end with zero findings.
    Complete,
    /// A finding or failure degraded Storage; the incident is recorded.
    Failed,
    /// Shutdown cancelled the scrub.
    Cancelled,
}

impl RunningScrubWorker {
    /// Starts the one background scrub over the live shared handle, reporting
    /// through the process's shared `status`. A failure is recorded as an
    /// internal incident through `diagnostics` and degrades Storage through
    /// the status's health member while serving continues (ADR-0203 Decision
    /// 5). `health` is the fail-closed fallback when no incident identity can
    /// be assigned.
    pub(crate) fn start(
        storage: RedbSharedPorts,
        inputs: riffdb_storage_api::StartupValidationInputs,
        status: BackgroundScrubStatus,
        failure: ScrubFailureSinks,
    ) -> Result<Self, StorageError> {
        Self::spawn(storage, inputs, status, failure, false)
    }

    /// Starts a worker whose shutdown was requested before its first page,
    /// the deterministic cancellation schedule for tests.
    #[cfg(test)]
    fn start_with_shutdown_requested(
        storage: RedbSharedPorts,
        inputs: riffdb_storage_api::StartupValidationInputs,
        status: BackgroundScrubStatus,
        failure: ScrubFailureSinks,
    ) -> Result<Self, StorageError> {
        Self::spawn(storage, inputs, status, failure, true)
    }

    fn spawn(
        storage: RedbSharedPorts,
        inputs: riffdb_storage_api::StartupValidationInputs,
        status: BackgroundScrubStatus,
        failure: ScrubFailureSinks,
        shutdown_requested: bool,
    ) -> Result<Self, StorageError> {
        let stop = Arc::new(ScrubStopState {
            requested: Mutex::new(shutdown_requested),
            changed: Condvar::new(),
        });
        let cancellation = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let worker_status = status.clone();
        let worker_cancellation = Arc::clone(&cancellation);
        let thread = thread::Builder::new()
            .name("riffdb-scrub".to_owned())
            .spawn(move || {
                run_scrub(
                    storage,
                    inputs,
                    &worker_stop,
                    &worker_status,
                    worker_cancellation,
                    &failure,
                );
            })
            .map_err(|_| storage_error(StorageErrorKind::Unavailable))?;
        Ok(Self {
            status,
            stop,
            thread: Some(thread),
        })
    }

    /// The closed scrub status.
    pub(crate) fn status(&self) -> BackgroundScrubStatus {
        self.status.clone()
    }

    /// The bounded, redaction-safe scrub work observation.
    pub(crate) fn observation(&self) -> ScrubObservation {
        let status = self.status();
        ScrubObservation {
            state: status.state(),
            rows_walked: status.rows_walked(),
            total_us: status.total_us(),
            rows_per_second: status.rows_per_second(),
        }
    }

    /// Requests cancellation and joins the worker. The observation names how
    /// the scrub ended; it carries no diagnostic detail.
    pub(crate) fn shutdown(&mut self) -> ScrubStopObservation {
        self.stop.request();
        let thread = self.thread.take();
        if let Some(thread) = thread {
            let _ = thread.join();
        }
        match self.status.state() {
            BackgroundScrubState::Complete => ScrubStopObservation::Complete,
            BackgroundScrubState::Failed => ScrubStopObservation::Failed,
            _ => ScrubStopObservation::Cancelled,
        }
    }
}

impl Drop for RunningScrubWorker {
    fn drop(&mut self) {
        // Dropping the handle does not cancel a running scrub; only shutdown
        // does. A leaked thread cannot outlive the process's storage handle.
        if self.thread.is_some() {
            self.stop.request();
            if let Some(thread) = self.thread.take() {
                let _ = thread.join();
            }
        }
    }
}

/// Where a failed scrub records itself: the incident identity source, the
/// trusted diagnostic sink, and the fail-closed readiness fallback.
#[derive(Clone)]
pub(crate) struct ScrubFailureSinks {
    pub(crate) incident_ids: Arc<dyn IncidentIdSource>,
    pub(crate) diagnostics: Arc<dyn ServiceDiagnostics>,
    pub(crate) health: Arc<dyn ServiceHealthHooks>,
}

/// The closed, payload-free reason a scrub failed, retained only in trusted
/// diagnostics under its incident identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScrubFailure {
    SnapshotUnavailable,
    StructuralFinding,
    StructuralRead,
    CatalogHistory,
    ExactEnd,
}

impl fmt::Display for ScrubFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::SnapshotUnavailable => "background scrub could not pin its snapshot",
            Self::StructuralFinding => "background scrub observed a structural finding",
            Self::StructuralRead => "background scrub structural read failed",
            Self::CatalogHistory => "background scrub catalog-history validation failed",
            Self::ExactEnd => "background scrub exact-end proof failed",
        })
    }
}

impl std::error::Error for ScrubFailure {}

fn storage_error(kind: StorageErrorKind) -> StorageError {
    StorageError::new(kind, None)
}

/// Records one failure: an incident identity, its trusted diagnostic, and the
/// failed state. Without an identity the failure cannot be published as a
/// FAILED member, so readiness fails closed instead.
fn record_failure(
    status: &BackgroundScrubStatus,
    sinks: &ScrubFailureSinks,
    failure: ScrubFailure,
) {
    match sinks.incident_ids.next_incident_id() {
        Ok(incident) => {
            sinks.diagnostics.record_internal(InternalError::new(
                incident,
                DefectScope::Process,
                failure,
            ));
            status.fail(Some(incident));
        }
        Err(_) => {
            status.fail(None);
            sinks.health.fail_authoritative_readiness(
                riffdb_observability::AuthoritativeReadinessFailure::Integrity,
            );
        }
    }
}

fn run_scrub(
    storage: RedbSharedPorts,
    inputs: riffdb_storage_api::StartupValidationInputs,
    stop: &ScrubStopState,
    status: &BackgroundScrubStatus,
    cancellation: Arc<AtomicBool>,
    sinks: &ScrubFailureSinks,
) {
    let cancel = || {
        status.note_finished();
        let _ = status.transition(STATE_CANCELLED);
    };
    if stop.is_requested() {
        cancel();
        return;
    }
    // The scrub is running from its first instruction: the session open
    // itself performs the retained-history validation, which is
    // population-proportional work the state must not hide as pending.
    status.note_running();
    if !status.transition(STATE_RUNNING) {
        return;
    }
    let mut session = match storage.begin_background_scrub(inputs) {
        Ok(session) => session,
        Err(_) => {
            record_failure(status, sinks, ScrubFailure::SnapshotUnavailable);
            return;
        }
    };
    session.set_cancellation(cancellation);
    let limit = EvidencePageLimit::new(BACKGROUND_SCRUB_ROWS_PER_STEP)
        .unwrap_or(EvidencePageLimit::new(1).expect("bounded page limit"));
    // The structural stream, one bounded page per scheduling step.
    let structural_end = loop {
        if stop.is_requested() {
            cancel();
            return;
        }
        match session.read_structural_page(limit) {
            Ok(BackgroundScrubPage::Page { findings, .. }) => {
                if !findings.is_empty() {
                    record_failure(status, sinks, ScrubFailure::StructuralFinding);
                    return;
                }
                status.note_rows(session.rows_walked());
                // Yield so the writer and workers keep their share of the core.
                thread::yield_now();
            }
            Ok(BackgroundScrubPage::ExactEnd(end)) => break end,
            Err(_) if stop.is_requested() => {
                cancel();
                return;
            }
            Err(_) => {
                record_failure(status, sinks, ScrubFailure::StructuralRead);
                return;
            }
        }
    };
    // The catalog-semantic validation over the historical stream, under the
    // same rows-per-step bound, yielding between pages and observing shutdown.
    let historical_end = match session.validate_catalog_history(limit, || {
        if stop.is_requested() {
            false
        } else {
            thread::yield_now();
            true
        }
    }) {
        Ok(end) => end,
        Err(_) if stop.is_requested() => {
            cancel();
            return;
        }
        Err(_) => {
            record_failure(status, sinks, ScrubFailure::CatalogHistory);
            return;
        }
    };
    match session.finish(structural_end, historical_end) {
        Ok(()) => {
            status.note_finished();
            let _ = status.transition(STATE_COMPLETE);
        }
        Err(_) => record_failure(status, sinks, ScrubFailure::ExactEnd),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_observability::AuthoritativeReadinessFailure;
    use riffdb_storage_api::{
        DatabaseInitializationPort, ReadableCapabilityDigestInventory, ReadableDigestKey,
        ReadableIdempotencyDigestInventory, StartupValidationInputs, StructuralEvidenceCursor,
        StructuralEvidenceOpen, StructuralEvidencePage, StructuralEvidenceSession,
    };
    use riffdb_storage_redb::RedbStore;
    use riffdb_types::{DatabaseId, DigestKeyId, Timestamp};

    #[derive(Default)]
    struct RecordingHealth {
        failures: Mutex<Vec<AuthoritativeReadinessFailure>>,
    }

    impl riffdb_observability::ServiceHealthHooks for RecordingHealth {
        fn fail_authoritative_readiness(&self, reason: AuthoritativeReadinessFailure) {
            self.failures
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(reason);
        }
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
                0xbd, 0x23,
            ])
            .map_err(|_| riffdb_errors::IncidentIdSourceError)
        }
    }

    struct Sinks {
        health: Arc<RecordingHealth>,
        diagnostics: Arc<RecordingDiagnostics>,
    }

    impl Sinks {
        fn new() -> Self {
            Self {
                health: Arc::default(),
                diagnostics: Arc::default(),
            }
        }

        fn failure(&self) -> ScrubFailureSinks {
            ScrubFailureSinks {
                incident_ids: Arc::new(FixedIncidents),
                diagnostics: self.diagnostics.clone(),
                health: self.health.clone(),
            }
        }
    }

    fn await_terminal(worker: &RunningScrubWorker) -> BackgroundScrubState {
        // The scrub runs on its own thread; the closed state is the only
        // signal, observed without sleeps-as-synchronization.
        for _ in 0..1_000_000 {
            let state = worker.status().state();
            if matches!(
                state,
                BackgroundScrubState::Complete
                    | BackgroundScrubState::Failed
                    | BackgroundScrubState::Cancelled
            ) {
                return state;
            }
            std::thread::yield_now();
        }
        panic!("scrub must reach a terminal state");
    }

    struct Scratch {
        _directory: riffdb_testkit::scratch::ScratchDir,
        path: std::path::PathBuf,
    }

    /// One initialized V3 source database with its operational ports closed
    /// again; the scrub runs over the shared read handle.
    fn scratch_source(label: &str) -> Scratch {
        let directory = riffdb_testkit::scratch::ScratchDir::new_in(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target"),
            "scrub-worker",
        )
        .expect("scratch directory");
        let path = directory.join(format!("{label}.redb"));
        let mut store = RedbStore::open(&path).expect("open scratch store");
        store
            .initialize_database(
                DatabaseId::from_unix_milliseconds_and_random(1, [0x5c; 10]).unwrap(),
            )
            .expect("initialize scratch database");
        let mut session = store
            .begin_structural_evidence(test_inputs())
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
        let (catalog, historical_end) = riffdb_catalog::validate_catalog_history(&mut session)
            .expect("catalog history")
            .into_parts();
        assert!(matches!(
            catalog,
            riffdb_catalog::CatalogHistoryOutcome::Ready(_)
        ));
        let outcome = session
            .finish(structural_end, historical_end)
            .expect("finish scratch evidence");
        let riffdb_storage_api::StructuralOpenOutcome::Clean(opened) = outcome else {
            panic!("scratch source must open cleanly");
        };
        let (_, _, _, dormant) = opened.into_parts();
        let ports = dormant
            .into_operational_after_catalog_validation()
            .expect("activate scratch ports");
        drop(ports);
        Scratch {
            _directory: directory,
            path,
        }
    }

    /// Opens the scratch source's shared read handle for the scrub: one
    /// bounded dirty open (the scratch has no clean certificate), then the
    /// activated ports' cloneable pure-read handle.
    fn scratch_shared(scratch: &Scratch) -> riffdb_storage_redb::RedbSharedPorts {
        let store = RedbStore::open(&scratch.path).expect("open for shared handle");
        let mut session = store
            .begin_structural_evidence(test_inputs())
            .expect("begin shared-handle evidence");
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
        let (catalog, historical_end) = riffdb_catalog::validate_catalog_history(&mut session)
            .expect("catalog history")
            .into_parts();
        assert!(matches!(
            catalog,
            riffdb_catalog::CatalogHistoryOutcome::Ready(_)
        ));
        let outcome = session
            .finish(structural_end, historical_end)
            .expect("finish shared-handle evidence");
        let riffdb_storage_api::StructuralOpenOutcome::Clean(opened) = outcome else {
            panic!("shared-handle open must be clean")
        };
        let (_, _, _, dormant) = opened.into_parts();
        let ports = dormant
            .into_operational_after_catalog_validation()
            .expect("activate shared-handle ports");
        let shared = ports.shared_ports();
        drop(ports);
        shared
    }

    fn test_inputs() -> StartupValidationInputs {
        let key = ReadableDigestKey::v1(DigestKeyId::new(1).unwrap());
        StartupValidationInputs::new(
            Timestamp::new(1_700_000_000, 0).unwrap(),
            ReadableCapabilityDigestInventory::new(vec![key]).unwrap(),
            ReadableIdempotencyDigestInventory::new(vec![key]).unwrap(),
        )
    }

    // req: REC-001, REC-002, REC-007
    /// OBL-0182-3 proof (ADR-0182 Decision 3, ADR-0203 Decisions 4 and 5).
    /// The background scrub exposes exactly the closed states with only the
    /// valid transitions, honors its rows-per-step bound, is cancelled by
    /// shutdown, and a failed scrub records an internal incident and degrades
    /// Storage through its health member without failing readiness, repairing,
    /// or hiding the affected row.
    #[test]
    fn background_scrub_health_transitions_are_closed_and_cancellable() {
        // pending -> running -> complete over a healthy source.
        {
            let scratch = scratch_source("scrub-complete");
            let sinks = Sinks::new();
            let status = BackgroundScrubStatus::new();
            assert_eq!(status.state(), BackgroundScrubState::Pending);
            let health = status.health().expect("pending member");
            assert_eq!(
                health.state(),
                riffdb_service::StorageScrubHealthState::Pending
            );
            let mut worker = RunningScrubWorker::start(
                scratch_shared(&scratch),
                test_inputs(),
                status.clone(),
                sinks.failure(),
            )
            .expect("start scrub worker");
            assert_eq!(await_terminal(&worker), BackgroundScrubState::Complete);
            assert_eq!(worker.shutdown(), ScrubStopObservation::Complete);
            assert_eq!(
                status.state(),
                BackgroundScrubState::Complete,
                "terminal states never transition"
            );
            let health = status.health().expect("complete member");
            assert_eq!(
                health.state(),
                riffdb_service::StorageScrubHealthState::Complete
            );
            assert_eq!(health.incident_id(), None);
            assert!(sinks.health.failures.lock().unwrap().is_empty());
            assert!(sinks.diagnostics.incidents.lock().unwrap().is_empty());
        }
        // Shutdown before the first page: pending -> cancelled, deterministically.
        {
            let scratch = scratch_source("scrub-cancel");
            let sinks = Sinks::new();
            let status = BackgroundScrubStatus::new();
            let mut worker = RunningScrubWorker::start_with_shutdown_requested(
                scratch_shared(&scratch),
                test_inputs(),
                status.clone(),
                sinks.failure(),
            )
            .expect("start scrub worker for cancellation");
            assert_eq!(worker.shutdown(), ScrubStopObservation::Cancelled);
            assert_eq!(status.state(), BackgroundScrubState::Cancelled);
            assert_eq!(status.rows_walked(), 0, "a cancelled scrub walked no page");
            assert!(sinks.health.failures.lock().unwrap().is_empty());
            assert!(sinks.diagnostics.incidents.lock().unwrap().is_empty());
        }
        // A planted malformed population row: running -> failed with one
        // internal incident, Storage degraded through the FAILED member,
        // readiness untouched, and the row left exactly as planted.
        {
            let scratch = scratch_source("scrub-failed");
            riffdb_storage_redb::plant_malformed_population_row_fixture(&scratch.path)
                .expect("plant malformed row");
            let sinks = Sinks::new();
            let status = BackgroundScrubStatus::new();
            let mut worker = RunningScrubWorker::start(
                scratch_shared(&scratch),
                test_inputs(),
                status.clone(),
                sinks.failure(),
            )
            .expect("start scrub worker over corruption");
            assert_eq!(await_terminal(&worker), BackgroundScrubState::Failed);
            assert_eq!(worker.shutdown(), ScrubStopObservation::Failed);
            drop(worker);
            let incident = FixedIncidents.next_incident_id().expect("fixed incident");
            let health = status.health().expect("failed member carries its incident");
            assert_eq!(
                health.state(),
                riffdb_service::StorageScrubHealthState::Failed
            );
            assert_eq!(health.incident_id(), Some(incident));
            assert_eq!(
                *sinks.diagnostics.incidents.lock().unwrap(),
                vec![(incident, DefectScope::Process)],
                "exactly one internal incident is recorded"
            );
            assert!(
                sinks.health.failures.lock().unwrap().is_empty(),
                "a failed scrub degrades Storage; it does not fail readiness"
            );
            assert_eq!(
                riffdb_storage_redb::read_malformed_population_row_fixture(&scratch.path)
                    .expect("read planted row"),
                Some(
                    riffdb_storage_redb::MALFORMED_POPULATION_ROW_FIXTURE
                        .1
                        .to_vec()
                ),
                "the scrub neither repairs nor removes the affected row"
            );
        }
        // Closed-state registry: exactly the five ADR-0182 states, by name.
        assert_eq!(BackgroundScrubState::Pending.as_str(), "pending");
        assert_eq!(BackgroundScrubState::Running.as_str(), "running");
        assert_eq!(BackgroundScrubState::Complete.as_str(), "complete");
        assert_eq!(BackgroundScrubState::Failed.as_str(), "failed");
        assert_eq!(BackgroundScrubState::Cancelled.as_str(), "cancelled");
        // Only the ADR-0203 transitions apply.
        let status = BackgroundScrubStatus::new();
        assert!(
            !status.transition(STATE_COMPLETE),
            "pending cannot complete"
        );
        assert!(!status.transition(STATE_FAILED), "pending cannot fail");
        assert!(status.transition(STATE_RUNNING));
        assert!(
            !status.transition(STATE_RUNNING),
            "running does not re-enter"
        );
        assert!(status.transition(STATE_COMPLETE));
        assert!(
            !status.transition(STATE_CANCELLED),
            "terminal states never transition"
        );
        // The rate bound is rows per scheduling step, never wall-clock.
        assert_eq!(BACKGROUND_SCRUB_ROWS_PER_STEP, 64);
    }
}
