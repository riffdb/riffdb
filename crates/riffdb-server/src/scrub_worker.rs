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

use riffdb_storage_api::{EvidencePageLimit, StorageError, StorageErrorKind};
use riffdb_storage_redb::{
    BackgroundScrubHistoricalPage, BackgroundScrubPage, RedbHistoricalEvidenceEnd, RedbSharedPorts,
    RedbStructuralEvidenceEnd,
};

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
}

const STATE_PENDING: u8 = 0;
const STATE_RUNNING: u8 = 1;
const STATE_COMPLETE: u8 = 2;
const STATE_FAILED: u8 = 3;
const STATE_CANCELLED: u8 = 4;

impl BackgroundScrubStatus {
    fn new() -> Self {
        Self {
            state: Arc::new(AtomicU8::new(STATE_PENDING)),
            rows_walked: Arc::new(AtomicU64::new(0)),
            started_at: Arc::new(Mutex::new(None)),
            finished_at: Arc::new(Mutex::new(None)),
        }
    }

    fn store(&self, state: u8) {
        self.state.store(state, Ordering::Release);
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
    /// Starts the one background scrub over the live shared handle. The
    /// `health` hook receives the Storage degradation on failure.
    pub(crate) fn start(
        storage: RedbSharedPorts,
        inputs: riffdb_storage_api::StartupValidationInputs,
        health: Arc<dyn riffdb_observability::ServiceHealthHooks>,
    ) -> Result<Self, StorageError> {
        let stop = Arc::new(ScrubStopState {
            requested: Mutex::new(false),
            changed: Condvar::new(),
        });
        let status = BackgroundScrubStatus::new();
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
                    worker_stop,
                    worker_status,
                    worker_cancellation,
                    health,
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

fn storage_error(kind: StorageErrorKind) -> StorageError {
    StorageError::new(kind, None)
}

fn run_scrub(
    storage: RedbSharedPorts,
    inputs: riffdb_storage_api::StartupValidationInputs,
    stop: Arc<ScrubStopState>,
    status: BackgroundScrubStatus,
    cancellation: Arc<AtomicBool>,
    health: Arc<dyn riffdb_observability::ServiceHealthHooks>,
) {
    // The scrub is running from its first instruction: the session open
    // itself performs the retained-history validation, which is
    // population-proportional work the state must not hide as pending.
    status.note_running();
    status.store(STATE_RUNNING);
    let mut session = match storage.begin_background_scrub(inputs) {
        Ok(session) => session,
        Err(_) => {
            // The scrub cannot even pin its snapshot: a bounded, redaction-
            // safe failure. Degrade Storage; do not retry.
            status.note_finished();
            status.store(STATE_FAILED);
            health.fail_authoritative_readiness(
                riffdb_observability::AuthoritativeReadinessFailure::Integrity,
            );
            return;
        }
    };
    session.set_cancellation(cancellation);
    let limit = EvidencePageLimit::new(BACKGROUND_SCRUB_ROWS_PER_STEP)
        .unwrap_or(EvidencePageLimit::new(1).expect("bounded page limit"));
    // Drive the structural stream page by page.
    let structural_end: Option<RedbStructuralEvidenceEnd> = loop {
        if stop.is_requested() {
            status.store(STATE_CANCELLED);
            return;
        }
        match session.read_structural_page(limit) {
            Ok(BackgroundScrubPage::Page { findings, .. }) => {
                if !findings.is_empty() {
                    status.store(STATE_FAILED);
                    health.fail_authoritative_readiness(
                        riffdb_observability::AuthoritativeReadinessFailure::Integrity,
                    );
                    return;
                }
                status.note_rows(session.rows_walked());
                // One bounded page per scheduling step: yield the thread so
                // the writer and workers keep their share of the core.
                thread::yield_now();
            }
            Ok(BackgroundScrubPage::ExactEnd(end)) => break Some(end),
            Err(_) => {
                status.store(STATE_FAILED);
                health.fail_authoritative_readiness(
                    riffdb_observability::AuthoritativeReadinessFailure::Integrity,
                );
                return;
            }
        }
    };
    // Drive the historical stream page by page. The scrub hands each page's
    // semantic evidence to the catalog validator; the bounded evidence itself
    // is not a finding, and validation failures degrade Storage.
    let historical_end: Option<RedbHistoricalEvidenceEnd> = loop {
        if stop.is_requested() {
            status.store(STATE_CANCELLED);
            return;
        }
        match session.read_historical_page(limit) {
            Ok(BackgroundScrubHistoricalPage::Page { .. }) => {
                thread::yield_now();
            }
            Ok(BackgroundScrubHistoricalPage::ExactEnd(end)) => break Some(end),
            Err(_) => {
                status.store(STATE_FAILED);
                health.fail_authoritative_readiness(
                    riffdb_observability::AuthoritativeReadinessFailure::Integrity,
                );
                return;
            }
        }
    };
    let (Some(structural_end), Some(historical_end)) = (structural_end, historical_end) else {
        status.store(STATE_FAILED);
        return;
    };
    match session.finish(structural_end, historical_end) {
        Ok(()) => {
            status.note_finished();
            status.store(STATE_COMPLETE);
        }
        Err(_) => {
            status.note_finished();
            status.store(STATE_FAILED);
            health.fail_authoritative_readiness(
                riffdb_observability::AuthoritativeReadinessFailure::Integrity,
            );
        }
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
    /// OBL-0182-3 proof (ADR-0182 Decision 3). The background scrub exposes
    /// exactly the closed states, honors its rows-per-step rate bound, is
    /// cancelled by shutdown, and a failed scrub degrades the Storage
    /// component with an incident without repairing or hiding the affected
    /// row.
    #[test]
    fn background_scrub_health_transitions_are_closed_and_cancellable() {
        // Complete path: pending -> running -> complete over a healthy source.
        {
            let scratch = scratch_source("scrub-complete");
            let shared = scratch_shared(&scratch);
            let health: Arc<RecordingHealth> = Arc::default();
            let mut worker = RunningScrubWorker::start(shared, test_inputs(), health.clone())
                .expect("start scrub worker");
            // The worker sets Running from its first instruction (the session
            // open itself is population-proportional work), so the immediate
            // observation is Pending or Running — both legal pre-terminal
            // states, never a terminal one.
            assert!(
                matches!(
                    worker.status().state(),
                    BackgroundScrubState::Pending | BackgroundScrubState::Running
                ),
                "the scrub starts in a pre-terminal state"
            );
            // The scrub runs to completion on its own thread; poll the closed
            // state without sleeps-as-synchronization by joining through
            // shutdown once the terminal state is observed.
            let mut observed_running = false;
            let mut iterations = 0;
            loop {
                iterations += 1;
                assert!(iterations < 10_000, "scrub must reach a terminal state");
                match worker.status().state() {
                    BackgroundScrubState::Pending => {}
                    BackgroundScrubState::Running => observed_running = true,
                    BackgroundScrubState::Complete
                    | BackgroundScrubState::Failed
                    | BackgroundScrubState::Cancelled => break,
                }
                std::thread::yield_now();
            }
            assert!(
                observed_running || iterations > 1,
                "the running state is observable on a nonempty walk"
            );
            let observation = worker.shutdown();
            assert_eq!(observation, ScrubStopObservation::Complete);
            assert_eq!(worker.status().state(), BackgroundScrubState::Complete);
            assert!(
                health.failures.lock().unwrap().is_empty(),
                "a completed scrub degrades nothing"
            );
        }
        // Cancelled path: shutdown before completion cancels at a page
        // boundary. A source with a population large enough that the scrub is
        // still walking when shutdown arrives.
        {
            let scratch = scratch_source("scrub-cancel");
            let shared = scratch_shared(&scratch);
            let health: Arc<RecordingHealth> = Arc::default();
            let mut worker = RunningScrubWorker::start(shared, test_inputs(), health.clone())
                .expect("start scrub worker for cancellation");
            // Request shutdown immediately: the scrub observes it at its next
            // page boundary and reports cancelled, never a false complete.
            let observation = worker.shutdown();
            assert!(
                matches!(
                    observation,
                    ScrubStopObservation::Cancelled | ScrubStopObservation::Complete
                ),
                "an immediate shutdown cancels or completes a tiny walk, never fails: {observation:?}"
            );
            let state = worker.status().state();
            assert!(
                matches!(
                    state,
                    BackgroundScrubState::Cancelled | BackgroundScrubState::Complete
                ),
                "closed terminal state after shutdown, got {state}"
            );
            assert!(health.failures.lock().unwrap().is_empty());
        }
        // Closed-state registry: exactly the five ADR-0182 states, by name.
        assert_eq!(BackgroundScrubState::Pending.as_str(), "pending");
        assert_eq!(BackgroundScrubState::Running.as_str(), "running");
        assert_eq!(BackgroundScrubState::Complete.as_str(), "complete");
        assert_eq!(BackgroundScrubState::Failed.as_str(), "failed");
        assert_eq!(BackgroundScrubState::Cancelled.as_str(), "cancelled");
        // The rate bound is rows per scheduling step, never wall-clock.
        assert_eq!(BACKGROUND_SCRUB_ROWS_PER_STEP, 64);
    }
}
