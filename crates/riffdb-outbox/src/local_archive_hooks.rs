//! Deterministic process-crash probes, unavailable to production configuration.

/// Durable publication boundaries explored by the process-kill tests.
#[cfg(feature = "test-fixtures")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalArchiveCrashPoint {
    /// Initial metadata is staged but not durable.
    InitializationBeforeCommit,
    /// Initial metadata is durable but directory publication is incomplete.
    InitializationAfterCommit,
    /// One record and both counters are staged but not durable.
    RecordBeforeCommit,
    /// Record and counters are durable but the connector has not acknowledged.
    RecordAfterCommit,
}

/// Test-only hook; a process harness blocks on an explicit pipe at one boundary.
#[cfg(feature = "test-fixtures")]
pub trait LocalArchiveCrashHooks: Send + Sync {
    /// The hook receives only a closed boundary, never record content or paths.
    fn reached(&self, point: LocalArchiveCrashPoint);
}

#[derive(Default)]
pub(crate) struct Hooks {
    #[cfg(feature = "test-fixtures")]
    pub(crate) inner: Option<std::sync::Arc<dyn LocalArchiveCrashHooks>>,
}

impl Hooks {
    pub(crate) fn initialization_before_commit(&self) {
        #[cfg(feature = "test-fixtures")]
        self.reached(LocalArchiveCrashPoint::InitializationBeforeCommit);
    }
    pub(crate) fn initialization_after_commit(&self) {
        #[cfg(feature = "test-fixtures")]
        self.reached(LocalArchiveCrashPoint::InitializationAfterCommit);
    }
    pub(crate) fn record_before_commit(&self) {
        #[cfg(feature = "test-fixtures")]
        self.reached(LocalArchiveCrashPoint::RecordBeforeCommit);
    }
    pub(crate) fn record_after_commit(&self) {
        #[cfg(feature = "test-fixtures")]
        self.reached(LocalArchiveCrashPoint::RecordAfterCommit);
    }
    #[cfg(feature = "test-fixtures")]
    fn reached(&self, point: LocalArchiveCrashPoint) {
        if let Some(hook) = &self.inner {
            hook.reached(point);
        }
    }
}
