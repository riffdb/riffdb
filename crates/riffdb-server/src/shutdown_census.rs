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
    line
}

#[cfg(test)]
mod tests {
    use super::{ShutdownReleaseStage, format_v1_line};

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
    }
}
