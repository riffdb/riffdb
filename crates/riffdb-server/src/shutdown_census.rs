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

/// Closed refusal causes from the actual native admission checks. No payloads,
/// paths, source identities or application-facing controls.
pub(crate) fn format_source_admission_line() -> String {
    let phases = RETENTION_OBSERVATION
        .get()
        .map(|lane| lane.phases)
        .unwrap_or_default();
    format!(
        "riffdb-retention-source-admission-v1\tbusy_writer={}\tpending_journal={}\tcheckpoint={}\tqualification={}\tpage_bound={}",
        phases.source_admission_busy_writer,
        phases.source_admission_pending_journal,
        phases.source_admission_checkpoint,
        phases.source_admission_qualification,
        phases.source_admission_page_bound
    )
}

pub(crate) fn format_source_reclamation_line() -> String {
    let phases = RETENTION_OBSERVATION
        .get()
        .map(|lane| lane.phases)
        .unwrap_or_default();
    format_source_reclamation_sample(&phases)
}

pub(crate) fn format_source_turn_line() -> String {
    let phases = RETENTION_OBSERVATION
        .get()
        .map(|lane| lane.phases)
        .unwrap_or_default();
    format_source_admission_census(phases.source_admission_census)
}

pub(crate) fn format_source_admission_census(values: [u64; 8]) -> String {
    use std::fmt::Write as _;
    let mut line = String::from("riffdb-retention-source-turns-v1");
    for (name, value) in [
        "queued",
        "granted",
        "expired",
        "cancelled",
        "capacity",
        "wait_us",
        "held_us",
        "drain_yields",
    ]
    .into_iter()
    .zip(values)
    {
        let _ = write!(line, "\t{name}={value}");
    }
    line
}

/// Fixed-cardinality commands-per-durable-flush histogram and epoch-seal reason
/// counters (ADR-0183 Decision 3). Distinguishes an unbatched flush path from a
/// saturated writer without exposing application identities or payloads.
pub(crate) fn format_durable_flush_group_line() -> String {
    let (buckets, queue_drained_seals, ceiling_seals) =
        riffdb_storage_redb::writer_durable_flush_group_census_v1();
    format_durable_flush_group_census(buckets, queue_drained_seals, ceiling_seals)
}

pub(crate) fn format_durable_flush_group_census(
    buckets: [u64; riffdb_storage_redb::COMMANDS_PER_DURABLE_FLUSH_BUCKETS_V1],
    epoch_queue_drained_seals: u64,
    epoch_ceiling_seals: u64,
) -> String {
    let encoded_buckets = buckets.map(|value| value.to_string()).join(",");
    format!(
        "riffdb-durable-flush-group-v1\tcommands_per_durable_flush={encoded_buckets}\tepoch_queue_drained_seals={epoch_queue_drained_seals}\tepoch_ceiling_seals={epoch_ceiling_seals}"
    )
}

/// Last exact native floor plan, with the attempt on which it was measured.
/// An unavailable or stale observation never stands for an unprotected floor.
pub(crate) fn format_source_reclamation_sample(
    phases: &riffdb_storage_redb::OnlineRetentionPhases,
) -> String {
    let [first, independent, floor] = phases.source_reclamation_yields;
    let mut line = format!(
        "riffdb-retention-source-ceiling-v1\tattempts={}\tfirst_observation={}\tprior_unusable={}\tfloor_hold={}",
        phases.source_reclamation_attempts, first, independent, floor,
    );
    let Some(sample) = phases.source_reclamation_sample else {
        line.push_str("\tsample=unavailable");
        return line;
    };
    use std::fmt::Write as _;
    let _ = write!(
        line,
        "\tsample_attempt={}",
        phases.source_reclamation_sample_attempt
    );
    for (name, value) in [
        "resume",
        "tail",
        "checkpoint",
        "derived",
        "durable_hold",
        "ceiling",
        "hold_count",
        "presence",
    ]
    .into_iter()
    .zip(sample)
    {
        let _ = write!(line, "\t{name}={value}");
    }
    line
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

    // req: STO-030, STO-031, END-007, END-008
    #[test]
    fn source_admission_census_is_bounded_even_at_counter_capacity() {
        use crate::retention_worker::{RetentionLaneObservation, RetentionLaneState};
        use riffdb_storage_redb::OnlineRetentionPhases;
        let previous = super::RETENTION_OBSERVATION.replace(Some(RetentionLaneObservation {
            state: RetentionLaneState::Idle,
            fence: None,
            steps: 0,
            sequences: 0,
            holds: [0; 4],
            phases: OnlineRetentionPhases {
                source_admission_busy_writer: u64::MAX,
                source_admission_pending_journal: u64::MAX,
                source_admission_checkpoint: u64::MAX,
                source_admission_qualification: u64::MAX,
                source_admission_page_bound: u64::MAX,
                ..OnlineRetentionPhases::default()
            },
        }));
        let line = super::format_source_admission_line();
        super::RETENTION_OBSERVATION.set(previous);
        assert!(line.len() <= 256);
        let fields: Vec<_> = line.split('\t').collect();
        assert_eq!(fields[0], "riffdb-retention-source-admission-v1");
        assert_eq!(fields.len(), 6);
        for (field, expected) in fields[1..].iter().zip([
            "busy_writer",
            "pending_journal",
            "checkpoint",
            "qualification",
            "page_bound",
        ]) {
            let (name, value) = field.split_once('=').unwrap();
            assert_eq!(name, expected);
            assert_eq!(value.parse::<u64>().unwrap(), u64::MAX);
        }
    }

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

    // req: STO-030, STO-031, END-008
    #[test]
    fn source_ceiling_census_distinguishes_missing_and_stale_observations() {
        use super::format_source_reclamation_sample;
        let mut phases = riffdb_storage_redb::OnlineRetentionPhases::default();
        assert!(format_source_reclamation_sample(&phases).ends_with("\tsample=unavailable"));
        phases.source_reclamation_attempts = u64::MAX;
        phases.source_reclamation_yields = [u64::MAX; 3];
        phases.source_reclamation_sample_attempt = 1;
        phases.source_reclamation_sample = Some([u64::MAX; 8]);
        let line = format_source_reclamation_sample(&phases);
        assert!(line.contains("\tsample_attempt=1\tresume="));
        assert!(!line.contains("sample=unavailable"));
        assert!(line.len() <= 768);
        assert!(
            line.split('\t').skip(1).all(|field| field
                .split_once('=')
                .unwrap()
                .1
                .parse::<u64>()
                .is_ok())
        );
    }

    // req: STO-030, STO-031, END-008
    #[test]
    fn source_turn_census_is_closed_numeric_and_bounded_at_saturation() {
        let line = super::format_source_admission_census([u64::MAX; 8]);
        let mut fields = line.split('\t');
        assert_eq!(fields.next(), Some("riffdb-retention-source-turns-v1"));
        for (field, expected) in fields.zip([
            "queued",
            "granted",
            "expired",
            "cancelled",
            "capacity",
            "wait_us",
            "held_us",
            "drain_yields",
        ]) {
            let (name, value) = field.split_once('=').unwrap();
            assert_eq!(name, expected);
            assert_eq!(value.parse::<u64>().unwrap(), u64::MAX);
        }
        assert_eq!(line.split('\t').count(), 9);
        assert!(line.len() <= 384);
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

    // req: PERF-022
    #[test]
    fn shutdown_evidence_reports_commands_per_durable_flush() {
        let live_line = super::format_durable_flush_group_line();
        assert!(live_line.starts_with("riffdb-durable-flush-group-v1\t"));
        assert!(live_line.contains("\tcommands_per_durable_flush="));
        assert!(live_line.contains("\tepoch_queue_drained_seals="));
        assert!(live_line.contains("\tepoch_ceiling_seals="));

        let line = super::format_durable_flush_group_census([11, 7, 5, 3, 2, 1, 0, 0, 4, 0], 29, 4);
        let fields: Vec<_> = line.split('\t').collect();
        assert_eq!(fields[0], "riffdb-durable-flush-group-v1");
        assert_eq!(fields.len(), 4);
        assert_eq!(fields[1], "commands_per_durable_flush=11,7,5,3,2,1,0,0,4,0");
        assert_eq!(fields[2], "epoch_queue_drained_seals=29");
        assert_eq!(fields[3], "epoch_ceiling_seals=4");

        let saturated = super::format_durable_flush_group_census(
            [u64::MAX; riffdb_storage_redb::COMMANDS_PER_DURABLE_FLUSH_BUCKETS_V1],
            u64::MAX,
            u64::MAX,
        );
        assert!(
            saturated.len() <= 384,
            "ten bucket counters and two seal counters have a fixed maximum size"
        );
    }
}
