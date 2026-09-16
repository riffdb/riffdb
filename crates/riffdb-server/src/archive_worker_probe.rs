//! Two bounded one-shot test executable progress signals; absent from normal builds and never installed by riffdbd.
use riffdb_storage_api::{ArchiveConsumerErrorV1, ChangelogHistoryPointV3};
use riffdb_types::{ArchiveNameV1, CommitSequence};
use std::sync::{
    OnceLock,
    atomic::{AtomicBool, Ordering},
};
struct Probe {
    name: ArchiveNameV1,
    through: [CommitSequence; 2],
    observers: [fn(); 2],
    failure_observer: fn(),
    emitted: [AtomicBool; 2],
    failure_emitted: AtomicBool,
}
static PROBE: OnceLock<Probe> = OnceLock::new();
/// Installs a bounded observer before a dedicated test executable starts the daemon.
/// Signals follow sink-confirmed progress or terminal sink failure; neither influences validation.
pub fn install_archive_progress_probe(
    name: &'static str,
    through: [u64; 2],
    observers: [fn(); 2],
    failure_observer: fn(),
) -> bool {
    let (Ok(name), Some(first), Some(second)) = (
        ArchiveNameV1::new(name),
        CommitSequence::new(through[0]),
        CommitSequence::new(through[1]),
    ) else {
        return false;
    };
    if first >= second {
        return false;
    }
    let through = [first, second];
    PROBE
        .set(Probe {
            name,
            through,
            observers,
            failure_observer,
            emitted: [AtomicBool::new(false), AtomicBool::new(false)],
            failure_emitted: AtomicBool::new(false),
        })
        .is_ok()
}
pub(super) fn observe(name: &ArchiveNameV1, position: ChangelogHistoryPointV3) {
    if let Some(probe) = PROBE.get()
        && &probe.name == name
    {
        for index in 0..probe.through.len() {
            if position
                .frontier()
                .application()
                .is_some_and(|value| value >= probe.through[index])
                && !probe.emitted[index].swap(true, Ordering::AcqRel)
            {
                (probe.observers[index])();
            }
        }
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
