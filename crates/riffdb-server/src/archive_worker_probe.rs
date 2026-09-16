//! One-shot test executable signal; absent from normal builds and never installed by riffdbd.
use riffdb_storage_api::ChangelogHistoryPointV3;
use riffdb_types::{ArchiveNameV1, CommitSequence};
use std::sync::{
    OnceLock,
    atomic::{AtomicBool, Ordering},
};
struct Probe {
    name: ArchiveNameV1,
    through: CommitSequence,
    observer: fn(),
    emitted: AtomicBool,
}
static PROBE: OnceLock<Probe> = OnceLock::new();
/// Installs a bounded observer before a dedicated test executable starts the daemon.
/// The signal follows sink-confirmed progress and cannot influence frame validation.
pub fn install_archive_progress_probe(name: &'static str, through: u64, observer: fn()) -> bool {
    let (Ok(name), Some(through)) = (ArchiveNameV1::new(name), CommitSequence::new(through)) else {
        return false;
    };
    PROBE
        .set(Probe {
            name,
            through,
            observer,
            emitted: AtomicBool::new(false),
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
