//! Explicit fixture-only observations; normal builds contain no probe or hook.
use std::{path::Path, sync::OnceLock};

use riffdb_types::CommitSequence;

/// Closed boundaries for the exact provider's process recovery proofs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExactProviderTestPoint {
    /// An immutable source is about to be captured for this slot.
    Preparing,
    /// The full authoritative partition reader is about to run.
    FullPartitionRead,
    /// Complete candidate bytes have been written to the pending file.
    BeforeFileSync,
    /// The pending file has been synchronized, before its atomic rename.
    AfterFileSync,
    /// The reopened complete candidate is about to replace the checked prior selection.
    BeforeSelection,
    /// The complete candidate has replaced the prior selection.
    AfterSelection,
}

type Probe = dyn Fn(ExactProviderTestPoint, &Path, Option<CommitSequence>) + Send + Sync;
static PROBE: OnceLock<Box<Probe>> = OnceLock::new();

/// Installs one process fixture observer before starting the daemon.
pub fn install_exact_provider_probe(
    probe: impl Fn(ExactProviderTestPoint, &Path, Option<CommitSequence>) + Send + Sync + 'static,
) -> bool {
    PROBE.set(Box::new(probe)).is_ok()
}

pub(crate) fn observe(point: ExactProviderTestPoint, path: &Path, head: Option<CommitSequence>) {
    if let Some(probe) = PROBE.get() {
        probe(point, path, head);
    }
}
