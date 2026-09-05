//! Process-wide startup stage census (`riffdb-startup-stages-v1`).
//!
//! Readiness cost was attributable only from outside the process, by polling
//! pid state every fifteen seconds, because nothing inside `riffdbd` reported
//! where startup spent its time. That is enough to see that a start took
//! twenty minutes and not enough to say which stage took them, so a bounded
//! readiness path that is nonetheless slow reads exactly like an unbounded one.
//!
//! This is the readiness peer of `riffdb-shutdown-stages-v1`: one tagged
//! stdout line, emitted immediately before the ready line, carrying elapsed
//! microseconds per named stage. Stage names are a closed set carrying no
//! path, key, value, or identity.
//!
//! Observation only. Nothing here can select startup behavior, and a stage
//! that is never reached simply reports zero.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// One named startup stage, in emission order.
///
/// Ordered as the process performs them so a reader can attribute wall clock
/// by scanning left to right.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StartupStage {
    /// Durable-format preflight plus `RedbStore::open` (includes any redb
    /// allocator repair, which is charged here and nowhere else).
    StoreOpen,
    /// `begin_structural_evidence`: startup snapshot, clean-close gate, and —
    /// on the full path only — the validated-prefix checkpoint load and its
    /// deterministic sample windows.
    EvidenceBegin,
    /// Draining every structural evidence page.
    StructuralDrain,
    /// Catalog historical evidence validation.
    CatalogHistory,
    /// `finish`: checkpoint write gate, lifecycle consumption, handoff.
    EvidenceFinish,
    /// `into_operational_after_catalog_validation`: transient index rebuild on
    /// the full path; a bounded no-op on the clean-close path.
    PortActivation,
    /// Current catalog, capability, and query-module view rebuilds.
    CurrentViews,
    /// Event-consumer lease recovery.
    ConsumerRecovery,
    /// Outbox recovery.
    OutboxRecovery,
    /// The remainder of production graph construction (workers, transports,
    /// coordinator) — total graph build minus the three stages above.
    GraphRest,
    /// Process entry to the ready line. The stages above should account for
    /// nearly all of it; a large remainder means an unnamed stage exists.
    ProcessToReady,
}

impl StartupStage {
    /// Every stage, in emission order (see [`Self::index`]).
    pub(crate) const ALL: [Self; 11] = [
        Self::StoreOpen,
        Self::EvidenceBegin,
        Self::StructuralDrain,
        Self::CatalogHistory,
        Self::EvidenceFinish,
        Self::PortActivation,
        Self::CurrentViews,
        Self::ConsumerRecovery,
        Self::OutboxRecovery,
        Self::GraphRest,
        Self::ProcessToReady,
    ];

    const fn index(self) -> usize {
        match self {
            Self::StoreOpen => 0,
            Self::EvidenceBegin => 1,
            Self::StructuralDrain => 2,
            Self::CatalogHistory => 3,
            Self::EvidenceFinish => 4,
            Self::PortActivation => 5,
            Self::CurrentViews => 6,
            Self::ConsumerRecovery => 7,
            Self::OutboxRecovery => 8,
            Self::GraphRest => 9,
            Self::ProcessToReady => 10,
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::StoreOpen => "store_open",
            Self::EvidenceBegin => "evidence_begin",
            Self::StructuralDrain => "structural_drain",
            Self::CatalogHistory => "catalog_history",
            Self::EvidenceFinish => "evidence_finish",
            Self::PortActivation => "port_activation",
            Self::CurrentViews => "current_views",
            Self::ConsumerRecovery => "consumer_recovery",
            Self::OutboxRecovery => "outbox_recovery",
            Self::GraphRest => "graph_rest",
            Self::ProcessToReady => "process_to_ready",
        }
    }
}

static ELAPSED_US: [AtomicU64; StartupStage::ALL.len()] =
    [const { AtomicU64::new(0) }; StartupStage::ALL.len()];

/// Records one stage's elapsed microseconds, accumulating on repeat.
///
/// Accumulates rather than overwrites because an index migration reruns the
/// whole validation pass; two passes must show as their sum, not as the second
/// one alone.
pub(crate) fn record(stage: StartupStage, started: Instant) {
    let elapsed = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
    let _ = ELAPSED_US[stage.index()].fetch_add(elapsed, Ordering::Relaxed);
}

/// Times one fallible stage and records it whether it succeeds or fails.
pub(crate) fn timed<T, E>(
    stage: StartupStage,
    body: impl FnOnce() -> Result<T, E>,
) -> Result<T, E> {
    let started = Instant::now();
    let outcome = body();
    record(stage, started);
    outcome
}

/// Records graph construction time minus the stages the build names itself.
///
/// A remainder rather than a raw total, so `graph_rest` answers "is there an
/// unnamed cost inside the build?" directly instead of double-counting the
/// three stages already reported.
pub(crate) fn record_graph_rest(started: Instant) {
    let whole = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
    let named = [
        StartupStage::CurrentViews,
        StartupStage::ConsumerRecovery,
        StartupStage::OutboxRecovery,
    ]
    .into_iter()
    .fold(0_u64, |total, stage| {
        total.saturating_add(ELAPSED_US[stage.index()].load(Ordering::Relaxed))
    });
    let _ = ELAPSED_US[StartupStage::GraphRest.index()]
        .fetch_add(whole.saturating_sub(named), Ordering::Relaxed);
}

/// Transient population-index rebuilds observed before readiness, and the
/// `COMMITS` rows they walked.
///
/// Carried on the same line as the stage timings because the two together are
/// the whole answer to "why was a bounded start slow?": a nonzero rebuild count
/// beside a long stage names both the cost and the code path that paid it.
static TRANSIENT_REBUILDS: AtomicU64 = AtomicU64::new(0);
static TRANSIENT_COMMIT_ROWS: AtomicU64 = AtomicU64::new(0);
static COLUMNAR_COLD_SOURCES: AtomicU64 = AtomicU64::new(0);
static COLUMNAR_ACTIVATIONS: AtomicU64 = AtomicU64::new(0);
static COLUMNAR_POPULATION_PASSES: AtomicU64 = AtomicU64::new(0);

/// Records the transient-index rebuild census observed at graph build.
pub(crate) fn record_transient_index_rebuilds(rebuilds: u64, commit_rows: u64) {
    TRANSIENT_REBUILDS.store(rebuilds, Ordering::Relaxed);
    TRANSIENT_COMMIT_ROWS.store(commit_rows, Ordering::Relaxed);
}

/// Records the bounded columnar lifecycle state at graph readiness.
pub(crate) fn record_columnar_lifecycle(sources: u64, activations: u64, population_passes: u64) {
    COLUMNAR_COLD_SOURCES.fetch_add(sources, Ordering::AcqRel);
    COLUMNAR_ACTIVATIONS.fetch_add(activations, Ordering::AcqRel);
    COLUMNAR_POPULATION_PASSES.fetch_add(population_passes, Ordering::AcqRel);
}

/// Records one first-demand activation transition.
pub(crate) fn record_columnar_activation() {
    COLUMNAR_ACTIVATIONS.fetch_add(1, Ordering::AcqRel);
}

/// Records one worker apply or authoritative rebuild pass.
pub(crate) fn record_columnar_population_pass() {
    COLUMNAR_POPULATION_PASSES.fetch_add(1, Ordering::AcqRel);
}

pub(crate) fn columnar_activations() -> u64 {
    COLUMNAR_ACTIVATIONS.load(Ordering::Acquire)
}

pub(crate) fn columnar_population_passes() -> u64 {
    COLUMNAR_POPULATION_PASSES.load(Ordering::Acquire)
}

static PROCESS_START: OnceLock<Instant> = OnceLock::new();

/// Stamps process entry. Idempotent; the first call wins.
pub(crate) fn mark_process_start() {
    let _ = PROCESS_START.set(Instant::now());
}

/// Renders the census as one tagged line for the readiness stream.
pub(crate) fn format_v1_line() -> String {
    if let Some(started) = PROCESS_START.get() {
        record(StartupStage::ProcessToReady, *started);
    }
    let mut line = String::from("riffdb-startup-stages-v1");
    for stage in StartupStage::ALL {
        line.push('\t');
        line.push_str(stage.as_str());
        line.push('=');
        line.push_str(
            &ELAPSED_US[stage.index()]
                .load(Ordering::Relaxed)
                .to_string(),
        );
    }
    line.push_str("\ttransient_index_rebuilds=");
    line.push_str(&TRANSIENT_REBUILDS.load(Ordering::Relaxed).to_string());
    line.push_str("\ttransient_index_commit_rows=");
    line.push_str(&TRANSIENT_COMMIT_ROWS.load(Ordering::Relaxed).to_string());
    line.push_str("\tcolumnar_cold_sources=");
    line.push_str(&COLUMNAR_COLD_SOURCES.load(Ordering::Acquire).to_string());
    line.push_str("\tcolumnar_activations=");
    line.push_str(&columnar_activations().to_string());
    line.push_str("\tcolumnar_population_passes=");
    line.push_str(&columnar_population_passes().to_string());
    line
}

#[cfg(test)]
mod tests {
    use super::{StartupStage, format_v1_line};

    #[test]
    fn stage_indices_are_unique_and_dense() {
        let mut seen = [false; StartupStage::ALL.len()];
        for stage in StartupStage::ALL {
            assert!(!seen[stage.index()], "duplicate index for {stage:?}");
            seen[stage.index()] = true;
        }
        assert!(seen.into_iter().all(|present| present));
    }

    #[test]
    fn every_stage_appears_in_the_rendered_line() {
        let line = format_v1_line();
        assert!(line.starts_with("riffdb-startup-stages-v1\t"));
        for stage in StartupStage::ALL {
            assert!(
                line.contains(&format!("\t{}=", stage.as_str())),
                "{} is missing from the census line",
                stage.as_str()
            );
        }
        assert!(line.contains("\tcolumnar_cold_sources="));
        assert!(line.contains("\tcolumnar_activations="));
        assert!(line.contains("\tcolumnar_population_passes="));
    }
}
