//! Diagnostic writer-batch stage census.
//!
//! The writer thread reports `busy_us` and `idle_us`, and neither tiles its
//! wall clock: post-submission completion draining runs after `busy` stops and
//! before the idle edge resets, so it lands in neither counter. Roughly a
//! quarter of `busy` is also unnamed by any existing stage.
//!
//! This census closes both gaps at once. It measures one complete writer loop
//! iteration and decomposes it into two disjoint levels:
//!
//! * Level 0 tiles the iteration's wall clock. `unit_execute` is exactly the
//!   window the `busy` counter reports; every other level-0 stage is work the
//!   writer performs outside it.
//! * Level 1 decomposes `unit_execute` exactly. Two derived stages carry what
//!   direct measurement cannot reach: `exec_drive_unnamed` is the group
//!   driver's own overhead, and `exec_outer_residual` is the writer unit's
//!   wrapper around it. Both mirror `program_drive_exclusive` on the read
//!   path -- parent minus measured children, with `saturating_sub` so clock
//!   skew can never underflow.
//!
//! Charging is a thread-local `Cell` add, because the writer is the sole
//! writer thread and the deep generic command-execution call stack cannot
//! thread a profile value through its signatures. Publication happens once per
//! iteration into a process-global window array.
//!
//! The whole surface is gated behind `RIFFDB_WRITER_BATCH_DIAGNOSTICS=1`. When
//! it is off, [`stage_start`] returns `None` and every charge is a null check.

use std::cell::Cell;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Level-0 stage: non-blocking completion drain at the top of the loop.
pub(crate) const LOOP_DRAIN_READY: usize = 0;
/// Level-0 stage: blocking receive of the next work unit. True idle.
pub(crate) const LOOP_WORK_RECV: usize = 1;
/// Level-0 stage: pipeline-footprint and deferral-eligibility computation.
pub(crate) const LOOP_ADMIT_GATE: usize = 2;
/// Level-0 stage: blocking pipeline drain taken before execution.
///
/// The writer waits here for the completion thread to publish every submitted
/// fence. The existing counters charge this to `idle`.
pub(crate) const LOOP_DRAIN_ALL_PRE: usize = 3;
/// Level-0 stage: the window the `busy` counter reports.
pub(crate) const UNIT_EXECUTE: usize = 4;
/// Level-0 stage: completion-lane submission and in-flight bookkeeping.
///
/// Runs after `busy` stops. Charged to neither existing counter.
pub(crate) const POST_SUBMIT_ENQUEUE: usize = 5;
/// Level-0 stage: blocking pipeline drain taken after submission.
///
/// Runs after `busy` stops. Charged to neither existing counter.
pub(crate) const POST_DRAIN_ALL: usize = 6;
/// Level-0 stage: intake feedback send.
pub(crate) const POST_FEEDBACK: usize = 7;
/// Level-0 residual: the iteration minus every other level-0 stage.
pub(crate) const LOOP_RESIDUAL: usize = 8;

/// Exclusive end of the disjoint level-0 run subtracted from the iteration.
const LEVEL0_END: usize = 8;

/// Level-1 stage: per-command storage-queue telemetry and group unzip.
pub(crate) const EXEC_QUEUE_TELEMETRY: usize = 9;
/// Level-1 stage: immediate (non-deferred) group completion.
pub(crate) const EXEC_FINISH_GROUP: usize = 10;
/// Level-1 stage: durable admission selection and transition.
pub(crate) const EXEC_ADMISSION: usize = 11;
/// Level-1 stage: lowering admission results into pending attempts.
pub(crate) const EXEC_ADMISSION_LOWER: usize = 12;
/// Level-1 stage: FIFO compatibility partition and serial-eligibility proof.
pub(crate) const EXEC_COMPATIBILITY: usize = 13;
/// Level-1 stage: conflict-capability acquisition for the serial group.
pub(crate) const EXEC_CONFLICT_ACQUIRE: usize = 14;
/// Level-1 stage: opening the storage batch or deferred epoch.
pub(crate) const EXEC_BATCH_BEGIN: usize = 15;
/// Level-1 stage: writer-private snapshot capture for the group.
pub(crate) const EXEC_SNAPSHOT_CAPTURE: usize = 16;
/// Level-1 stage: deterministic runtime evaluation.
pub(crate) const EXEC_EVALUATE: usize = 17;
/// Level-1 stage: detaching evaluated attempts onto the open batch.
pub(crate) const EXEC_DETACH: usize = 18;
/// Level-1 stage: detached validation, encoding, and preparation.
pub(crate) const EXEC_PREPARE_DETACHED: usize = 19;
/// Level-1 stage: joining prepared bodies and staging the group.
pub(crate) const EXEC_STAGE_GROUP: usize = 20;
/// Level-1 stage: command-terminal audit transition preparation.
pub(crate) const EXEC_AUDIT_PREPARE: usize = 21;
/// Level-1 stage: final apply to writer-private authoritative state.
pub(crate) const EXEC_APPLY: usize = 22;
/// Level-1 stage: sealing the deferred epoch into a journal fence.
pub(crate) const EXEC_SEAL: usize = 23;
/// Level-1 stage: the per-group drive loop.
///
/// NOT a disjoint sibling. The loop it wraps calls the compatible-group driver
/// whenever a group has more than one item or every group is deferral
/// eligible, so `exec_evaluate`, `exec_seal`, `exec_apply`, and their
/// neighbours are charged *inside* this window and are double counted against
/// it. Read it as a parent, like `drive_total`, and use the level-2
/// `serial_*` run for what the truly serial branch costs.
pub(crate) const EXEC_ALTERNATE_PATH: usize = 24;
/// Level-1 stage: boxing the group-driver future.
///
/// `drive_repeatable_group` returns `Box::pin(..)` of a large generic async
/// state machine. This is the heap allocation and the argument move, not the
/// execution -- polling happens later, inside the other stages.
pub(crate) const EXEC_FUTURE_BOX: usize = 25;
/// Level-1 stage: marshalling the serial group into and out of the driver.
///
/// The compatibility groups are flattened into one serial vector, unzipped
/// into indices and states, and re-zipped into a `VecDeque`.
pub(crate) const EXEC_SERIAL_SETUP: usize = 26;
/// Level-1 stage: marshalling around the parallel evaluation pool.
///
/// Splitting captured snapshots into pool inputs, and scattering the pool's
/// results back onto their admission ordinals.
pub(crate) const EXEC_POOL_MARSHAL: usize = 27;
/// Level-1 stage: serial (non-pool) staging of each evaluated command.
///
/// `stage_first_evaluated_command_on_empty` and `append_evaluated_command`.
/// The detached path's equivalent is `exec_detach` plus `exec_stage_group`.
pub(crate) const EXEC_STAGE_SERIAL: usize = 28;
/// Level-1 stage: scattering results and finalizing the group drive result.
pub(crate) const EXEC_GROUP_FINALIZE: usize = 29;
/// Level-1 residual inside the group driver, derived from [`DRIVE_TOTAL`].
///
/// Whatever remains of the driver once every measured stage is removed: its
/// own control flow, the telemetry records it makes, and async poll dispatch.
pub(crate) const EXEC_DRIVE_UNNAMED: usize = 30;
/// Level-1 residual outside the group driver, derived from [`DRIVE_TOTAL`].
///
/// The `block_on` park/unpark around the driver future and the writer unit
/// the wrapper builds from the driver's result.
pub(crate) const EXEC_OUTER_RESIDUAL: usize = 31;
/// Bookkeeping: the complete `drive_command_group` await.
///
/// This is the parent of stages 11..=30 and is therefore NOT part of the
/// disjoint level-1 run. It exists only so both residuals can be derived.
pub(crate) const DRIVE_TOTAL: usize = 32;

/// Level-2 stage: conflict-attempt acquisition on the serial path.
pub(crate) const SERIAL_ACQUIRE: usize = 33;
/// Level-2 stage: deriving input command facts from the frozen request.
/// Retained slot: the input-derived proof is now carried from preparation
/// rather than re-derived on the writer, so this stage reports zero. The slot
/// stays in the census so the emitted stage vector keeps its fixed shape.
#[allow(dead_code)]
pub(crate) const SERIAL_INPUT_FACTS: usize = 34;
/// Level-2 stage: reading the declared dependency snapshot.
pub(crate) const SERIAL_DEPENDENCY_READ: usize = 35;
/// Level-2 stage: acquiring the conflict lease.
pub(crate) const SERIAL_LEASE: usize = 36;
/// Level-2 stage: durable idempotency admission.
pub(crate) const SERIAL_ADMISSION: usize = 37;
/// Level-2 stage: capturing and materializing the read snapshot.
pub(crate) const SERIAL_SNAPSHOT: usize = 38;
/// Level-2 stage: deterministic runtime evaluation on the serial path.
pub(crate) const SERIAL_EVALUATE: usize = 39;
/// Level-2 residual: [`EXEC_ALTERNATE_PATH`] minus the seven named stages.
///
/// Derived, never charged. Single-client commands take the serial path
/// exclusively, so without this tiling the largest block of their cost is one
/// unattributed bucket.
pub(crate) const SERIAL_RESIDUAL: usize = 40;

// Level 2, inside `exec_stage_serial`'s append path. These are children of a
// stage that already has a total, not siblings of it: they are appended past
// every residual range on purpose, so adding them cannot change what any
// existing residual means. They do not tile the append path exactly; what they
// leave out is its own control flow.
/// Level 2: post-evaluation authorization, request recheck, and provenance bind.
pub(crate) const APPEND_AUTHORIZE: usize = 41;
/// Level 2: beginning the bound candidate on the prior staged command.
pub(crate) const APPEND_BEGIN: usize = 42;
/// Level 2: reading transaction-current state and rechecking the row policy.
pub(crate) const APPEND_CURRENT: usize = 43;
/// Level 2: reading the affected epoch and assigning the sequence.
pub(crate) const APPEND_EPOCH: usize = 44;
/// Level 2: building the candidate and staging it onto the open batch.
pub(crate) const APPEND_BUILD_STAGE: usize = 45;

/// First and exclusive-end index of the run nested inside `exec_alternate_path`.
const SERIAL_NESTED_START: usize = 33;
const SERIAL_NESTED_END: usize = 40;

/// First and exclusive-end index of the disjoint level-1 run.
///
/// `drive_total` sits past the end: it is the parent of `exec_admission`
/// through `exec_drive_unnamed` and would double-count if summed.
///
/// [`EXEC_ALTERNATE_PATH`] sits *inside* the range and is the same kind of
/// parent, so every sum over this run must skip it. It could not be moved past
/// the end without renumbering a published stage order.
#[cfg(test)]
const LEVEL1_START: usize = 9;
#[cfg(test)]
const LEVEL1_END: usize = 32;
/// First and exclusive-end index of the run nested inside `drive_total`.
const DRIVE_NESTED_START: usize = 11;
const DRIVE_NESTED_END: usize = 30;

/// Closed stage order for `riffdb-writer-batch-stages-v1`.
///
/// Indices 0..=8 tile one writer loop iteration's wall clock; `loop_residual`
/// is the iteration minus the eight named level-0 stages. Indices 9..=31 tile
/// `unit_execute`, with `exec_drive_unnamed` and `exec_outer_residual`
/// derived. Level-1 stages are already inside `unit_execute` and must never be
/// added to the level-0 total. `drive_total` is the parent of indices 11..=30
/// and belongs to neither tiling. Indices 33..=40 tile `exec_alternate_path`
/// -- the serial single-command path every unbatched command takes -- with
/// `serial_residual` derived. They are already inside it and must never be
/// added to the level-1 total.
#[doc(hidden)]
pub const WRITER_BATCH_STAGE_LABELS_V1: [&str; 46] = [
    "loop_drain_ready",
    "loop_work_recv",
    "loop_admit_gate",
    "loop_drain_all_pre",
    "unit_execute",
    "post_submit_enqueue",
    "post_drain_all",
    "post_feedback",
    "loop_residual",
    "exec_queue_telemetry",
    "exec_finish_group",
    "exec_admission",
    "exec_admission_lower",
    "exec_compatibility",
    "exec_conflict_acquire",
    "exec_batch_begin",
    "exec_snapshot_capture",
    "exec_evaluate",
    "exec_detach",
    "exec_prepare_detached",
    "exec_stage_group",
    "exec_audit_prepare",
    "exec_apply",
    "exec_seal",
    "exec_alternate_path",
    "exec_future_box",
    "exec_serial_setup",
    "exec_pool_marshal",
    "exec_stage_serial",
    "exec_group_finalize",
    "exec_drive_unnamed",
    "exec_outer_residual",
    "drive_total",
    "serial_acquire",
    "serial_input_facts",
    "serial_dependency_read",
    "serial_lease",
    "serial_admission",
    "serial_snapshot",
    "serial_evaluate",
    "serial_residual",
    "append_authorize",
    "append_begin",
    "append_current",
    "append_epoch",
    "append_build_stage",
];

/// Number of writer loop iterations merged into one ordinal window.
#[doc(hidden)]
pub const WRITER_BATCH_WINDOW_WIDTH_V1: usize = 256;
/// Maximum retained ordinal windows; the final window absorbs later samples.
#[doc(hidden)]
pub const WRITER_BATCH_WINDOW_COUNT_V1: usize = 64;

/// One bounded writer-batch ordinal window.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriterBatchWindowV1 {
    /// Writer loop iterations merged into this window.
    pub count: u64,
    /// Summed nanoseconds in [`WRITER_BATCH_STAGE_LABELS_V1`] order.
    pub stage_ns: [u64; WRITER_BATCH_STAGE_LABELS_V1.len()],
    /// Logical commands carried by the iterations in this window.
    pub commands: u64,
    /// Iterations that submitted a deferred unit to the completion lane.
    pub submitted_units: u64,
}

// `Default` is derived for arrays only up to 32 elements, and the stage
// vector is longer than that.
impl Default for WriterBatchWindowV1 {
    fn default() -> Self {
        Self {
            count: 0,
            stage_ns: [0; WRITER_BATCH_STAGE_LABELS_V1.len()],
            commands: 0,
            submitted_units: 0,
        }
    }
}

/// Complete bounded writer-batch census for one process generation.
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub struct WriterBatchCensusV1 {
    /// Total writer loop iterations observed.
    pub total_count: u64,
    /// Bounded ordinal windows in admission order.
    pub windows: [WriterBatchWindowV1; WRITER_BATCH_WINDOW_COUNT_V1],
}

struct WindowCounters {
    count: AtomicU64,
    stage_ns: [AtomicU64; WRITER_BATCH_STAGE_LABELS_V1.len()],
    commands: AtomicU64,
    submitted_units: AtomicU64,
}

impl WindowCounters {
    fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
            stage_ns: std::array::from_fn(|_| AtomicU64::new(0)),
            commands: AtomicU64::new(0),
            submitted_units: AtomicU64::new(0),
        }
    }
}

struct Census {
    total_count: AtomicU64,
    windows: [WindowCounters; WRITER_BATCH_WINDOW_COUNT_V1],
}

impl Census {
    fn new() -> Self {
        Self {
            total_count: AtomicU64::new(0),
            windows: std::array::from_fn(|_| WindowCounters::new()),
        }
    }
}

static WRITER_BATCH_DIAGNOSTICS: OnceLock<bool> = OnceLock::new();
static WRITER_BATCH_CENSUS: OnceLock<Census> = OnceLock::new();

thread_local! {
    /// Per-iteration stage accumulation for the sole writer thread.
    static PROFILE: Cell<[u64; WRITER_BATCH_STAGE_LABELS_V1.len()]> =
        const { Cell::new([0; WRITER_BATCH_STAGE_LABELS_V1.len()]) };
    /// Logical commands carried by the iteration in progress.
    static COMMANDS: Cell<u64> = const { Cell::new(0) };
}

/// Reports whether the diagnostic census is enabled for this process.
#[must_use]
pub(crate) fn writer_batch_diagnostics_enabled() -> bool {
    *WRITER_BATCH_DIAGNOSTICS.get_or_init(|| {
        std::env::var_os("RIFFDB_WRITER_BATCH_DIAGNOSTICS").is_some_and(|value| value == "1")
    })
}

/// Opens a stage window, or returns `None` when diagnostics are disabled.
#[must_use]
pub(crate) fn stage_start() -> Option<Instant> {
    writer_batch_diagnostics_enabled().then(Instant::now)
}

/// Charges one stage window to the iteration in progress.
pub(crate) fn charge(index: usize, started: Option<Instant>) {
    let Some(started) = started else {
        return;
    };
    let elapsed = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    charge_nanos(index, elapsed);
}

/// Charges an already-measured duration to the iteration in progress.
pub(crate) fn charge_nanos(index: usize, elapsed: u64) {
    if !writer_batch_diagnostics_enabled() {
        return;
    }
    PROFILE.with(|profile| {
        let mut stages = profile.get();
        stages[index] = stages[index].saturating_add(elapsed);
        profile.set(stages);
    });
}

/// Records the logical command count carried by the iteration in progress.
pub(crate) fn observe_commands(commands: u64) {
    if !writer_batch_diagnostics_enabled() {
        return;
    }
    COMMANDS.with(|count| count.set(count.get().saturating_add(commands)));
}

/// Begins one writer loop iteration, discarding any partial prior profile.
pub(crate) fn begin_iteration() {
    if !writer_batch_diagnostics_enabled() {
        return;
    }
    PROFILE.with(|profile| profile.set([0; WRITER_BATCH_STAGE_LABELS_V1.len()]));
    COMMANDS.with(|count| count.set(0));
}

/// Closes one writer loop iteration and publishes it into the census.
///
/// `iteration_ns` is the iteration's complete wall clock. Both residuals are
/// derived here with `saturating_sub`, so clock skew can never underflow.
pub(crate) fn end_iteration(iteration_ns: u64, submitted_unit: bool) {
    if !writer_batch_diagnostics_enabled() {
        return;
    }
    let mut stages = PROFILE.with(Cell::get);
    let level0: u64 = stages[..LEVEL0_END]
        .iter()
        .copied()
        .fold(0_u64, u64::saturating_add);
    stages[LOOP_RESIDUAL] = iteration_ns.saturating_sub(level0);
    // `exec_alternate_path` sits inside this index run but is a parent, not a
    // sibling: the loop it wraps calls the compatible-group driver, which
    // charges `exec_evaluate`, `exec_seal`, `exec_apply` and their neighbours
    // within the same window. Summing it here double counted those children and
    // drove `exec_drive_unnamed` to a saturated zero, which reads as "fully
    // attributed" when the opposite is true.
    let nested: u64 = nested_sum(&stages);
    stages[EXEC_DRIVE_UNNAMED] = stages[DRIVE_TOTAL].saturating_sub(nested);
    let serial: u64 = stages[SERIAL_NESTED_START..SERIAL_NESTED_END]
        .iter()
        .copied()
        .fold(0_u64, u64::saturating_add);
    stages[SERIAL_RESIDUAL] = stages[EXEC_ALTERNATE_PATH].saturating_sub(serial);
    stages[EXEC_OUTER_RESIDUAL] = stages[UNIT_EXECUTE]
        .saturating_sub(stages[DRIVE_TOTAL])
        .saturating_sub(stages[EXEC_QUEUE_TELEMETRY])
        .saturating_sub(stages[EXEC_FINISH_GROUP]);
    publish(stages, COMMANDS.with(Cell::get), submitted_unit);
}

fn publish(stages: [u64; WRITER_BATCH_STAGE_LABELS_V1.len()], commands: u64, submitted_unit: bool) {
    let census = WRITER_BATCH_CENSUS.get_or_init(Census::new);
    let ordinal = census.total_count.fetch_add(1, Ordering::Relaxed);
    let unbounded_window = usize::try_from(ordinal)
        .unwrap_or(usize::MAX)
        .saturating_div(WRITER_BATCH_WINDOW_WIDTH_V1);
    let window = &census.windows[unbounded_window.min(WRITER_BATCH_WINDOW_COUNT_V1 - 1)];
    window.count.fetch_add(1, Ordering::Relaxed);
    for (counter, elapsed) in window.stage_ns.iter().zip(stages) {
        saturating_atomic_add(counter, elapsed);
    }
    saturating_atomic_add(&window.commands, commands);
    if submitted_unit {
        window.submitted_units.fetch_add(1, Ordering::Relaxed);
    }
}

fn saturating_atomic_add(target: &AtomicU64, value: u64) {
    let _ = target.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(value))
    });
}

/// Reads the bounded writer-batch census for shutdown evidence.
#[doc(hidden)]
#[must_use]
pub fn writer_batch_stage_census_v1() -> WriterBatchCensusV1 {
    let Some(census) = WRITER_BATCH_CENSUS.get() else {
        return WriterBatchCensusV1 {
            total_count: 0,
            windows: [WriterBatchWindowV1::default(); WRITER_BATCH_WINDOW_COUNT_V1],
        };
    };
    WriterBatchCensusV1 {
        total_count: census.total_count.load(Ordering::Relaxed),
        windows: std::array::from_fn(|index| {
            let window = &census.windows[index];
            WriterBatchWindowV1 {
                count: window.count.load(Ordering::Relaxed),
                stage_ns: std::array::from_fn(|stage| {
                    window.stage_ns[stage].load(Ordering::Relaxed)
                }),
                commands: window.commands.load(Ordering::Relaxed),
                submitted_units: window.submitted_units.load(Ordering::Relaxed),
            }
        }),
    }
}

/// Sums the disjoint level-1 run nested inside `drive_total`.
///
/// Excludes [`EXEC_ALTERNATE_PATH`], which is a parent of part of that run.
fn nested_sum(stages: &[u64; WRITER_BATCH_STAGE_LABELS_V1.len()]) -> u64 {
    stages[DRIVE_NESTED_START..DRIVE_NESTED_END]
        .iter()
        .enumerate()
        .filter(|(offset, _)| DRIVE_NESTED_START + offset != EXEC_ALTERNATE_PATH)
        .map(|(_, value)| *value)
        .fold(0_u64, u64::saturating_add)
}

/// Renders one payload-free writer-batch census line.
#[doc(hidden)]
#[must_use]
pub fn format_writer_batch_stages_v1_line(census: &WriterBatchCensusV1) -> String {
    let labels = WRITER_BATCH_STAGE_LABELS_V1.join(",");
    let windows = census
        .windows
        .iter()
        .filter(|window| window.count > 0)
        .map(|window| {
            let mut values = Vec::with_capacity(WRITER_BATCH_STAGE_LABELS_V1.len() + 3);
            values.push(window.count.to_string());
            values.extend(window.stage_ns.iter().map(u64::to_string));
            values.push(window.commands.to_string());
            values.push(window.submitted_units.to_string());
            values.join(",")
        })
        .collect::<Vec<_>>()
        .join(";");
    format!(
        "riffdb-writer-batch-stages-v1\ttotal={}\tlabels={labels}\twindows={windows}",
        census.total_count
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stage_labels_are_unique_and_snake_case() {
        for (index, label) in WRITER_BATCH_STAGE_LABELS_V1.iter().enumerate() {
            assert!(
                label
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte == b'_'),
                "label {label} is not snake_case"
            );
            assert!(
                !WRITER_BATCH_STAGE_LABELS_V1[..index].contains(label),
                "duplicate label {label}"
            );
        }
    }

    #[test]
    fn residual_indices_bound_the_disjoint_runs() {
        assert_eq!(WRITER_BATCH_STAGE_LABELS_V1[LEVEL0_END], "loop_residual");
        assert_eq!(
            WRITER_BATCH_STAGE_LABELS_V1[LEVEL1_START],
            "exec_queue_telemetry"
        );
        assert_eq!(WRITER_BATCH_STAGE_LABELS_V1[LEVEL1_END], "drive_total");
        assert_eq!(WRITER_BATCH_STAGE_LABELS_V1[UNIT_EXECUTE], "unit_execute");
        assert_eq!(
            WRITER_BATCH_STAGE_LABELS_V1[DRIVE_NESTED_START],
            "exec_admission"
        );
        assert_eq!(
            WRITER_BATCH_STAGE_LABELS_V1[DRIVE_NESTED_END], "exec_drive_unnamed",
            "the nested run must stop before its own derived residual"
        );
        assert_eq!(
            WRITER_BATCH_STAGE_LABELS_V1[SERIAL_NESTED_START],
            "serial_acquire"
        );
        assert_eq!(
            WRITER_BATCH_STAGE_LABELS_V1[SERIAL_NESTED_END], "serial_residual",
            "the serial run must stop before its own derived residual"
        );
        assert_eq!(
            WRITER_BATCH_STAGE_LABELS_V1.len(),
            APPEND_BUILD_STAGE + 1,
            "every declared index must have a label"
        );
        // The level-2 append stages are children of `exec_stage_serial`, which
        // already has a total, so they must sit outside every run a residual
        // subtracts. Inside one, they would be counted twice and the residual
        // they fell into would read as attributed when it is not.
        for level2 in [
            APPEND_AUTHORIZE,
            APPEND_BEGIN,
            APPEND_CURRENT,
            APPEND_EPOCH,
            APPEND_BUILD_STAGE,
        ] {
            assert!(
                !(DRIVE_NESTED_START..DRIVE_NESTED_END).contains(&level2),
                "a level-2 stage must not fall inside the drive run"
            );
            assert!(
                !(SERIAL_NESTED_START..SERIAL_NESTED_END).contains(&level2),
                "a level-2 stage must not fall inside the serial run"
            );
            assert!(
                level2 > SERIAL_RESIDUAL,
                "a level-2 stage is appended past every derived residual"
            );
        }
    }

    #[test]
    fn the_level_one_tiling_sums_to_unit_execute() {
        // Mirrors `end_iteration`'s derivation on a hand-built profile.
        let mut stages = [0_u64; WRITER_BATCH_STAGE_LABELS_V1.len()];
        stages[UNIT_EXECUTE] = 1_000;
        stages[EXEC_QUEUE_TELEMETRY] = 30;
        stages[EXEC_FINISH_GROUP] = 20;
        stages[DRIVE_TOTAL] = 900;
        stages[EXEC_ADMISSION] = 400;
        stages[EXEC_APPLY] = 250;
        // A parent charge over the two named children, as the real writer
        // records it. Counting it would leave 0 rather than 250.
        stages[EXEC_ALTERNATE_PATH] = 650;
        stages[EXEC_DRIVE_UNNAMED] = stages[DRIVE_TOTAL].saturating_sub(nested_sum(&stages));
        stages[EXEC_OUTER_RESIDUAL] = stages[UNIT_EXECUTE]
            .saturating_sub(stages[DRIVE_TOTAL])
            .saturating_sub(stages[EXEC_QUEUE_TELEMETRY])
            .saturating_sub(stages[EXEC_FINISH_GROUP]);
        assert_eq!(stages[EXEC_DRIVE_UNNAMED], 250);
        assert_eq!(stages[EXEC_OUTER_RESIDUAL], 50);
        // `exec_alternate_path` is excluded here for the same reason
        // `nested_sum` excludes it and `drive_total` sits past `LEVEL1_END`:
        // it is a parent of part of the run, so counting it would tile
        // `unit_execute` at more than 100%.
        let level1: u64 = stages[LEVEL1_START..LEVEL1_END]
            .iter()
            .enumerate()
            .filter(|(offset, _)| LEVEL1_START + offset != EXEC_ALTERNATE_PATH)
            .map(|(_, value)| *value)
            .fold(0, u64::saturating_add);
        assert_eq!(
            level1, stages[UNIT_EXECUTE],
            "level 1 must tile unit_execute exactly"
        );
    }

    #[test]
    fn a_child_longer_than_its_parent_cannot_underflow() {
        let mut stages = [0_u64; WRITER_BATCH_STAGE_LABELS_V1.len()];
        stages[DRIVE_TOTAL] = 10;
        stages[EXEC_ADMISSION] = 99;
        let nested: u64 = stages[DRIVE_NESTED_START..DRIVE_NESTED_END]
            .iter()
            .copied()
            .fold(0, u64::saturating_add);
        assert_eq!(stages[DRIVE_TOTAL].saturating_sub(nested), 0);
    }

    #[test]
    fn a_disabled_census_reports_no_samples() {
        // The process running the test suite does not set the gate.
        assert!(!writer_batch_diagnostics_enabled());
        assert!(stage_start().is_none());
        charge(UNIT_EXECUTE, None);
        assert_eq!(writer_batch_stage_census_v1().total_count, 0);
    }

    #[test]
    fn an_empty_census_formats_without_windows() {
        let census = WriterBatchCensusV1 {
            total_count: 0,
            windows: [WriterBatchWindowV1::default(); WRITER_BATCH_WINDOW_COUNT_V1],
        };
        let line = format_writer_batch_stages_v1_line(&census);
        assert!(line.starts_with("riffdb-writer-batch-stages-v1\ttotal=0\t"));
        assert!(line.ends_with("\twindows="));
    }

    #[test]
    fn a_populated_window_renders_count_stages_and_trailers() {
        let mut window = WriterBatchWindowV1 {
            count: 3,
            ..WriterBatchWindowV1::default()
        };
        window.stage_ns[UNIT_EXECUTE] = 900;
        window.commands = 12;
        window.submitted_units = 2;
        let mut windows = [WriterBatchWindowV1::default(); WRITER_BATCH_WINDOW_COUNT_V1];
        windows[0] = window;
        let census = WriterBatchCensusV1 {
            total_count: 3,
            windows,
        };
        let line = format_writer_batch_stages_v1_line(&census);
        let payload = line
            .split('\t')
            .next_back()
            .expect("windows field")
            .trim_start_matches("windows=");
        let values = payload.split(',').collect::<Vec<_>>();
        assert_eq!(values.len(), WRITER_BATCH_STAGE_LABELS_V1.len() + 3);
        assert_eq!(values[0], "3");
        assert_eq!(values[1 + UNIT_EXECUTE], "900");
        assert_eq!(values[WRITER_BATCH_STAGE_LABELS_V1.len() + 1], "12");
        assert_eq!(values[WRITER_BATCH_STAGE_LABELS_V1.len() + 2], "2");
    }
}
