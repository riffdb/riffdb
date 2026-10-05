//! Process-wide shutdown release census (`riffdb-shutdown-release-v1`).
//!
//! `riffdb-shutdown-stages-v1` stops the clock when the graph's last worker is
//! joined and the final checkpoint is written. Everything after that — above
//! all `redb::Database::drop`, which persists redb's allocator-state table and
//! trims the file so the next open can skip a full repair — was outside every
//! receipt the process emits.
//!
//! That gap is why a graceful shutdown observed from outside at ~12 minutes
//! could sit beside a stage line reporting milliseconds: the line was not
//! wrong about the stages it covers, it simply ended before the expensive one
//! began. (The other half of that discrepancy is that one benchmark run starts
//! three daemons, and only the post-seed one has a large history to release.)
//!
//! Observation only; nothing here changes what is dropped or in what order.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// One named post-drain release stage, in emission order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ShutdownReleaseStage {
    /// Dropping the graph's storage owners. When the graph holds the last
    /// handles this includes `redb::Database::drop`; a near-zero value beside a
    /// long process exit means some clone outlives the graph.
    GraphStorageRelease,
    /// From the graph-shutdown receipt to the process boundary: transports,
    /// services, and every storage clone that outlived the graph — including
    /// `redb::Database::drop` when the graph was not the last owner.
    ///
    /// This is the stage a shutdown receipt printed at graph teardown can never
    /// contain, because the receipt is printed before it happens.
    PostGraphRelease,
}

impl ShutdownReleaseStage {
    /// Every stage, in emission order (see [`Self::index`]).
    pub(crate) const ALL: [Self; 2] = [Self::GraphStorageRelease, Self::PostGraphRelease];

    const fn index(self) -> usize {
        match self {
            Self::GraphStorageRelease => 0,
            Self::PostGraphRelease => 1,
        }
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::GraphStorageRelease => "graph_storage_release",
            Self::PostGraphRelease => "post_graph_release",
        }
    }
}

static ELAPSED_US: [AtomicU64; ShutdownReleaseStage::ALL.len()] =
    [const { AtomicU64::new(0) }; ShutdownReleaseStage::ALL.len()];

/// Records one release stage's elapsed microseconds, accumulating on repeat.
pub(crate) fn record(stage: ShutdownReleaseStage, started: Instant) {
    let elapsed = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
    let _ = ELAPSED_US[stage.index()].fetch_add(elapsed, Ordering::Relaxed);
}

static GRAPH_RECEIPT_AT: OnceLock<Instant> = OnceLock::new();

/// Stamps the moment the graph-shutdown receipt was emitted. Idempotent.
pub(crate) fn mark_graph_receipt() {
    let _ = GRAPH_RECEIPT_AT.set(Instant::now());
}

/// Records everything between the graph receipt and the process boundary.
///
/// A no-op when no graph receipt was emitted (a start that never became ready
/// has nothing to attribute).
pub(crate) fn record_post_graph_release() {
    if let Some(stamped) = GRAPH_RECEIPT_AT.get() {
        record(ShutdownReleaseStage::PostGraphRelease, *stamped);
    }
}

/// Physical reclamation is reported independently of logical sequence pruning.
/// The existing shutdown-release V1 field registry stays unchanged.
pub(crate) fn format_extent_line() -> String {
    let phases = RETENTION_OBSERVATION
        .get()
        .map(|lane| lane.phases)
        .unwrap_or_default();
    format!(
        "riffdb-retention-extents-v1\tdeleted={}\trecycled={}\theld_references={}\theld_readers={}\treclaim_us={}",
        phases.deleted_extents,
        phases.recycled_extents,
        phases.extent_reference_holds,
        phases.extent_reader_holds,
        phases.reclaim_us
    )
}

/// Source receipt deletion and its writer occupancy are separate from logical
/// command pruning and physical extent reclamation. No identities or payloads.
pub(crate) fn format_receipt_line() -> String {
    let phases = RETENTION_OBSERVATION
        .get()
        .map(|lane| lane.phases)
        .unwrap_or_default();
    format!(
        "riffdb-retention-receipts-v1\tremoved={}\tgate_us={}",
        phases.reclaimed_receipts, phases.receipt_gate_us
    )
}

/// Fixed-cardinality native proof lifecycle diagnostics. Counts are streamed
/// receipt/audit rows, rather than payloads or authority/source identities.
pub(crate) fn format_source_validation_line() -> String {
    let phases = RETENTION_OBSERVATION
        .get()
        .map(|lane| lane.phases)
        .unwrap_or_default();
    format!(
        "riffdb-retention-source-validation-v1\tinitial_rows={}\tcontinuation_rows={}\tinvalidations={}\tadmission_refusals={}",
        phases.source_initial_rows,
        phases.source_continuation_rows,
        phases.source_validation_invalidations,
        phases.source_admission_refusals
    )
}

/// Renders the census as one tagged line for the shutdown evidence stream.
pub(crate) fn format_v1_line() -> String {
    let mut line = String::from("riffdb-shutdown-release-v1");
    for stage in ShutdownReleaseStage::ALL {
        line.push('\t');
        line.push_str(stage.as_str());
        line.push('=');
        line.push_str(
            &ELAPSED_US[stage.index()]
                .load(Ordering::Relaxed)
                .to_string(),
        );
    }
    line.push_str("\tcolumnar_activations=");
    line.push_str(&crate::startup_census::columnar_activations().to_string());
    line.push_str("\tcolumnar_population_passes=");
    line.push_str(&crate::startup_census::columnar_population_passes().to_string());
    line.push_str("\tbackground_scrub=");
    line.push_str(scrub_terminal_state());
    line.push_str("\tbackground_scrub_rows=");
    line.push_str(&scrub_rows().to_string());
    line.push_str("\tbackground_scrub_total_us=");
    line.push_str(&scrub_total_us().to_string());
    line.push_str("\tbackground_scrub_rows_per_second=");
    line.push_str(&scrub_rows_per_second().to_string());
    let lane = RETENTION_OBSERVATION.get();
    let state = lane.map_or(
        crate::retention_worker::RetentionLaneState::Holding,
        |lane| lane.state,
    );
    line.push_str("\tonline_retention=");
    line.push_str(state.as_str());
    line.push_str("\tonline_retention_fence=");
    line.push_str(crate::retention_worker::fence_name(
        lane.and_then(|lane| lane.fence),
    ));
    line.push_str("\tonline_retention_steps=");
    line.push_str(&lane.map_or(0, |lane| lane.steps).to_string());
    line.push_str("\tonline_retention_sequences=");
    line.push_str(&lane.map_or(0, |lane| lane.sequences).to_string());
    // WP-811: summed phase microseconds (preplan, gate wait, drain, plan,
    // digest, materialize, delete, commit) and holds by reason (target,
    // fenced, checkpoint in flight, awaiting checkpoint).
    let phases = lane.map(|lane| lane.phases).unwrap_or_default();
    line.push_str("\tonline_retention_phases_us=");
    line.push_str(
        &[
            phases.preplan_us,
            phases.gate_wait_us,
            phases.drain_us,
            phases.plan_us,
            phases.digest_us,
            phases.materialize_us,
            phases.delete_us,
            phases.commit_us,
        ]
        .map(|value| value.to_string())
        .join(","),
    );
    line.push_str("\tonline_retention_holds=");
    line.push_str(
        &lane
            .map_or([0; 4], |lane| lane.holds)
            .map(|value| value.to_string())
            .join(","),
    );
    line
}

/// The background scrub's closed terminal observation at shutdown, or the
/// absent zero shape when no scrub was recorded (a start that never became
/// ready). Redaction-safe: closed discriminants and bounded counters only.
fn scrub_terminal_state() -> &'static str {
    SCRUB_OBSERVATION.with(|state| {
        let (state, _, _, _) = state.get();
        state.as_str()
    })
}

fn scrub_rows() -> u64 {
    SCRUB_OBSERVATION.with(|state| {
        let (_, rows, _, _) = state.get();
        rows
    })
}

fn scrub_total_us() -> u64 {
    SCRUB_OBSERVATION.with(|state| {
        let (_, _, total_us, _) = state.get();
        total_us
    })
}

fn scrub_rows_per_second() -> u64 {
    SCRUB_OBSERVATION.with(|state| {
        let (_, _, _, rows_per_second) = state.get();
        rows_per_second
    })
}

thread_local! {
    static SCRUB_OBSERVATION: std::cell::Cell<(
        crate::scrub_worker::BackgroundScrubState,
        u64,
        u64,
        u64,
    )> = const { std::cell::Cell::new((crate::scrub_worker::BackgroundScrubState::Pending, 0, 0, 0)) };
}

/// Records the background scrub's closed terminal observation for the shutdown
/// census line. Redaction-safe: a closed discriminant and bounded counters.
pub(crate) fn record_background_scrub(
    state: crate::scrub_worker::BackgroundScrubState,
    rows_walked: u64,
    total_us: u64,
    rows_per_second: u64,
) {
    SCRUB_OBSERVATION.set((state, rows_walked, total_us, rows_per_second));
}

thread_local! {
    static RETENTION_OBSERVATION: std::cell::Cell<
        Option<crate::retention_worker::RetentionLaneObservation>,
    > = const { std::cell::Cell::new(None) };
}

/// Records the online retention lane's closed state and bounded counters for
/// the shutdown census line (ADR-0182 section 7 evidence). Internal only.
pub(crate) fn record_online_retention(
    observation: crate::retention_worker::RetentionLaneObservation,
) {
    RETENTION_OBSERVATION.set(Some(observation));
}

#[cfg(test)]
mod tests {
    use super::{ShutdownReleaseStage, format_v1_line};

    // req: STO-030, STO-031, STO-032
    #[test]
    fn receipt_reclamation_census_has_only_bounded_numeric_fields() {
        let line = super::format_receipt_line();
        let fields = line.split('\t').collect::<Vec<_>>();
        assert_eq!(fields[0], "riffdb-retention-receipts-v1");
        assert_eq!(fields.len(), 3);
        for (field, expected) in fields[1..].iter().zip(["removed", "gate_us"]) {
            let (name, value) = field.split_once('=').unwrap();
            assert_eq!(name, expected);
            assert!(value.parse::<u64>().is_ok());
        }
        assert!(line.len() <= 96);
    }

    // req: STO-030, STO-031, END-008
    #[test]
    fn source_validation_census_contains_only_bounded_numeric_fields() {
        let line = super::format_source_validation_line();
        let fields = line.split('\t').collect::<Vec<_>>();
        assert_eq!(fields[0], "riffdb-retention-source-validation-v1");
        assert_eq!(fields.len(), 5);
        for (field, expected) in fields[1..].iter().zip([
            "initial_rows",
            "continuation_rows",
            "invalidations",
            "admission_refusals",
        ]) {
            let (name, value) = field.split_once('=').unwrap();
            assert_eq!(name, expected);
            assert!(value.parse::<u64>().is_ok());
        }
        assert!(line.len() <= 224);
    }

    // req: STO-031
    #[test]
    fn physical_retention_census_has_only_bounded_numeric_fields() {
        let line = super::format_extent_line();
        let mut fields = line.split('\t');
        assert_eq!(fields.next(), Some("riffdb-retention-extents-v1"));
        let actual = fields
            .map(|field| {
                let (name, value) = field.split_once('=').unwrap();
                assert!(value.parse::<u64>().is_ok());
                name
            })
            .collect::<Vec<_>>();
        assert_eq!(
            actual,
            [
                "deleted",
                "recycled",
                "held_references",
                "held_readers",
                "reclaim_us"
            ]
        );
        assert!(
            line.len() <= 222,
            "five u64 counters have a fixed maximum size"
        );
    }

    #[test]
    fn every_stage_appears_in_the_rendered_line() {
        let line = format_v1_line();
        assert!(line.starts_with("riffdb-shutdown-release-v1\t"));
        for stage in ShutdownReleaseStage::ALL {
            assert!(
                line.contains(&format!("\t{}=", stage.as_str())),
                "{} is missing from the census line",
                stage.as_str()
            );
        }
        assert!(line.contains("\tcolumnar_activations="));
        assert!(line.contains("\tcolumnar_population_passes="));
        assert!(line.contains("\tonline_retention=holding"));
        assert!(line.contains("\tonline_retention_fence=none"));
        assert!(line.contains("\tonline_retention_phases_us=0,0,0,0,0,0,0,0"));
        assert!(line.contains("\tonline_retention_holds=0,0,0,0"));
        assert!(line.contains("\tonline_retention_sequences="));
    }
}
