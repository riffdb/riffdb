//! Diagnostic command-service stage census.
//!
//! The writer censuses account for a command from the moment the writer picks
//! it up. Measured on the OpenFGA adapter, that is only 41% of a single-tuple
//! write's client latency; the journal lane is another 28%, and the remaining
//! 30% is the service path -- everything before the writer sees the command and
//! everything after it releases the outcome -- which had no stage census at
//! all. This is that census.
//!
//! Stages 0..=7 tile [`SERVICE_TOTAL`], with [`SVC_RESIDUAL`] derived. Nothing
//! is charged unless `RIFFDB_COMMAND_SERVICE_DIAGNOSTICS=1` is set, and when it
//! is off [`stage_start`] returns `None` and every charge is a null check.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Resolving the catalog, plan, and active command identity.
pub(crate) const SVC_PREPARE_ACTIVE: usize = 0;
/// Acquiring the read snapshot under request control.
pub(crate) const SVC_SNAPSHOT_WAIT: usize = 1;
/// Normalizing the submitted input into its canonical record.
pub(crate) const SVC_NORMALIZE_INPUT: usize = 2;
/// Deriving input command facts from the normalized record.
pub(crate) const SVC_INPUT_FACTS: usize = 3;
/// Admitting the command against service capacity.
pub(crate) const SVC_ADMIT_CAPACITY: usize = 4;
/// Opening the audited command lifecycle.
pub(crate) const SVC_AUDIT_BEGIN: usize = 5;
/// Submitting to the writer and awaiting its committed outcome.
///
/// This is the one stage that is mostly *waiting*: it contains the writer's own
/// execution and the journal lane, both of which have their own censuses. It is
/// named so the rest of the service path can be read without it.
pub(crate) const SVC_COMMIT_WAIT: usize = 6;
/// Releasing the outcome: audit completion and response construction.
pub(crate) const SVC_RELEASE: usize = 7;
/// Derived: [`SERVICE_TOTAL`] minus the eight named stages.
pub(crate) const SVC_RESIDUAL: usize = 8;
/// Bookkeeping parent: the complete `execute_command` call.
pub(crate) const SERVICE_TOTAL: usize = 9;

/// First and exclusive-end index of the disjoint run inside `service_total`.
const NAMED_START: usize = 0;
const NAMED_END: usize = 8;

/// Closed stage order for `riffdb-command-service-stages-v1`.
#[doc(hidden)]
pub const COMMAND_SERVICE_STAGE_LABELS_V1: [&str; 10] = [
    "svc_prepare_active",
    "svc_snapshot_wait",
    "svc_normalize_input",
    "svc_input_facts",
    "svc_admit_capacity",
    "svc_audit_begin",
    "svc_commit_wait",
    "svc_release",
    "svc_residual",
    "service_total",
];

static STAGE_NANOS: [AtomicU64; COMMAND_SERVICE_STAGE_LABELS_V1.len()] =
    [const { AtomicU64::new(0) }; COMMAND_SERVICE_STAGE_LABELS_V1.len()];
static COMMANDS: AtomicU64 = AtomicU64::new(0);

/// Reports whether the diagnostic census is enabled for this process.
pub(crate) fn command_service_diagnostics_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var_os("RIFFDB_COMMAND_SERVICE_DIAGNOSTICS").is_some_and(|value| value == "1")
    })
}

/// Starts one stage, or returns `None` when the census is off.
#[must_use]
pub(crate) fn stage_start() -> Option<Instant> {
    command_service_diagnostics_enabled().then(Instant::now)
}

/// Charges one stage with the elapsed time since [`stage_start`].
pub(crate) fn charge(stage: usize, started: Option<Instant>) {
    let Some(started) = started else {
        return;
    };
    charge_duration(stage, started.elapsed());
}

fn charge_duration(stage: usize, elapsed: Duration) {
    let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
    let _ = STAGE_NANOS[stage].fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(nanos))
    });
}

/// Closes one complete `execute_command` call.
pub(crate) fn finish_command(started: Option<Instant>) {
    let Some(started) = started else {
        return;
    };
    charge_duration(SERVICE_TOTAL, started.elapsed());
    let _ = COMMANDS.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(1))
    });
}

/// Renders one payload-free command-service census line.
///
/// The residual is derived here rather than charged, so a stage added to the
/// path without a charge shows up as residual growth instead of vanishing.
#[doc(hidden)]
#[must_use]
pub fn command_service_stage_census_v1() -> [u64; COMMAND_SERVICE_STAGE_LABELS_V1.len() + 1] {
    let mut stages = [0_u64; COMMAND_SERVICE_STAGE_LABELS_V1.len() + 1];
    for (index, cell) in STAGE_NANOS.iter().enumerate() {
        stages[index] = cell.load(Ordering::Relaxed);
    }
    let named: u64 = stages[NAMED_START..NAMED_END]
        .iter()
        .copied()
        .fold(0_u64, u64::saturating_add);
    stages[SVC_RESIDUAL] = stages[SERVICE_TOTAL].saturating_sub(named);
    stages[COMMAND_SERVICE_STAGE_LABELS_V1.len()] = COMMANDS.load(Ordering::Relaxed);
    stages
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_residual_is_the_total_minus_the_named_run() {
        assert_eq!(
            COMMAND_SERVICE_STAGE_LABELS_V1[NAMED_START],
            "svc_prepare_active"
        );
        assert_eq!(
            COMMAND_SERVICE_STAGE_LABELS_V1[NAMED_END], "svc_residual",
            "the named run must stop before its own derived residual"
        );
        assert_eq!(
            COMMAND_SERVICE_STAGE_LABELS_V1[SERVICE_TOTAL], "service_total",
            "service_total is the parent and belongs to no tiling"
        );
        assert_eq!(
            COMMAND_SERVICE_STAGE_LABELS_V1.len(),
            SERVICE_TOTAL + 1,
            "every declared index must have a label"
        );
    }

    #[test]
    fn a_disabled_census_charges_nothing() {
        // The environment is not set under test, so every start is `None` and
        // every charge is a null check.
        assert!(stage_start().is_none());
        charge(SVC_PREPARE_ACTIVE, None);
        finish_command(None);
        assert_eq!(command_service_stage_census_v1()[SERVICE_TOTAL], 0);
    }
}
