//! One-shot test executable signal; absent from normal builds and never installed by riffdbd.
use riffdb_storage_api::{ArchiveConsumerErrorV1, ChangelogHistoryPointV3};
use riffdb_types::{ArchiveNameV1, CommitSequence};
use std::sync::{
    OnceLock,
    atomic::{AtomicBool, Ordering},
};
struct Probe {
    name: ArchiveNameV1,
    through: CommitSequence,
    observer: fn(),
    failure_observer: fn(),
    emitted: AtomicBool,
    failure_emitted: AtomicBool,
}
static PROBE: OnceLock<Probe> = OnceLock::new();
/// Installs a bounded observer before a dedicated test executable starts the daemon.
/// Signals follow sink-confirmed progress or terminal sink failure; neither influences validation.
pub fn install_archive_progress_probe(
    name: &'static str,
    through: u64,
    observer: fn(),
    failure_observer: fn(),
) -> bool {
    let (Ok(name), Some(through)) = (ArchiveNameV1::new(name), CommitSequence::new(through)) else {
        return false;
    };
    PROBE
        .set(Probe {
            name,
            through,
            observer,
            failure_observer,
            emitted: AtomicBool::new(false),
            failure_emitted: AtomicBool::new(false),
        })
        .is_ok()
}
pub(super) fn observe(name: &ArchiveNameV1, position: ChangelogHistoryPointV3) {
    if let Some(probe) = PROBE.get()
        && &probe.name == name
        && position
            .frontier()
            .application()
            .is_some_and(|value| value >= probe.through)
        && !probe.emitted.swap(true, Ordering::AcqRel)
    {
        (probe.observer)();
    }
}

pub(super) fn observe_failure(name: &ArchiveNameV1, error: ArchiveConsumerErrorV1) {
    if let Some(probe) = PROBE.get()
        && &probe.name == name
        && error == ArchiveConsumerErrorV1::SinkUnavailable
        && !probe.failure_emitted.swap(true, Ordering::AcqRel)
    {
        (probe.failure_observer)();
    }
}
