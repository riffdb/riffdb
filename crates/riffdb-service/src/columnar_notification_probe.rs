//! One-shot evidence for a dedicated process-test executable, absent in normal builds.

use std::sync::{
    OnceLock,
    atomic::{AtomicBool, Ordering},
};

struct Probe {
    name: &'static str,
    observer: fn(),
    emitted: AtomicBool,
}
static PROBE: OnceLock<Probe> = OnceLock::new();

/// Installs one bounded, one-shot test observer before starting a dedicated daemon.
///
/// It runs only after the requested projection has a registered, still-unsatisfied
/// waiter immediately before its condition-variable park. It grants no authority
/// and supplies no input to the read. The normal daemon never installs a probe.
#[doc(hidden)]
pub fn install_columnar_wait_probe(name: &'static str, observer: fn()) -> bool {
    !name.is_empty()
        && name.len() <= 256
        && PROBE
            .set(Probe {
                name,
                observer,
                emitted: AtomicBool::new(false),
            })
            .is_ok()
}

pub(super) fn observe(name: &str) {
    if let Some(probe) = PROBE.get()
        && probe.name == name
        && !probe.emitted.swap(true, Ordering::AcqRel)
    {
        (probe.observer)();
    }
}
