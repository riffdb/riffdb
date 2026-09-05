//! Closed test controller for fixed checkpoint crash boundaries (D9).
//!
//! Production opens never install this controller. Its constructors admit only
//! fixed actions at fixed boundaries; arbitrary callbacks are unsupported.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// Fixed durability boundaries for checkpoint crash injection.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ColumnarTestBoundary {
    /// Before `sync_all` on a newly written segment file.
    BeforeSegmentSync,
    /// After segment `sync_all`, before writing the temp manifest.
    AfterSegmentSync,
    /// Before renaming the temp manifest over `MANIFEST`.
    BeforeManifestRename,
    /// After manifest rename, before parent-directory `sync_all`.
    AfterManifestRename,
    /// Before syncing one V2 candidate segment body.
    BeforeV2SegmentSync,
    /// After renaming one fully synced V2 candidate segment.
    AfterV2SegmentRename,
    /// After renaming one fully synced V2 partition manifest.
    AfterV2ManifestRename,
    /// After renaming the fully synced complete V2 generation root.
    AfterV2RootRename,
    /// After renaming the complete V2 candidate directory.
    AfterV2GenerationRename,
    /// Before removing one durably unselected V2 generation directory.
    BeforeV2GenerationReclaim,
    /// After removing one durably unselected V2 generation, before parent sync.
    AfterV2GenerationReclaim,
}

/// Fixed failpoint actions.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FailpointAction {
    AbortProcess,
    ReturnIo,
}

#[derive(Clone, Copy)]
struct ArmedFailpoint {
    boundary: ColumnarTestBoundary,
    action: FailpointAction,
}

struct Inner {
    armed: Mutex<Option<ArmedFailpoint>>,
    fired: AtomicBool,
    events: Mutex<Vec<ColumnarTestBoundary>>,
}

/// Closed controller for process-level columnar recovery tests.
#[doc(hidden)]
#[derive(Clone)]
pub struct ColumnarTestController {
    inner: Arc<Inner>,
}

impl ColumnarTestController {
    /// Creates an idle controller (no failpoint armed).
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                armed: Mutex::new(None),
                fired: AtomicBool::new(false),
                events: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Arms a process abort at exactly one boundary.
    pub fn arm_abort_at(&self, boundary: ColumnarTestBoundary) {
        let mut armed = self.inner.armed.lock().unwrap_or_else(|e| e.into_inner());
        *armed = Some(ArmedFailpoint {
            boundary,
            action: FailpointAction::AbortProcess,
        });
        self.inner.fired.store(false, Ordering::SeqCst);
    }

    /// Arms one synthetic ENOSPC-class I/O refusal at a durability boundary.
    pub fn arm_io_failure_at(&self, boundary: ColumnarTestBoundary) {
        let mut armed = self.inner.armed.lock().unwrap_or_else(|e| e.into_inner());
        *armed = Some(ArmedFailpoint {
            boundary,
            action: FailpointAction::ReturnIo,
        });
        self.inner.fired.store(false, Ordering::SeqCst);
    }

    /// Clears any armed failpoint.
    pub fn clear(&self) {
        let mut armed = self.inner.armed.lock().unwrap_or_else(|e| e.into_inner());
        *armed = None;
    }

    /// Whether the failpoint has fired.
    #[must_use]
    pub fn fired(&self) -> bool {
        self.inner.fired.load(Ordering::SeqCst)
    }

    /// Observed boundaries in order (diagnostics only).
    #[must_use]
    pub fn events(&self) -> Vec<ColumnarTestBoundary> {
        self.inner
            .events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Invoked by checkpoint code at each fixed boundary.
    pub(crate) fn hit(&self, boundary: ColumnarTestBoundary) -> bool {
        if let Ok(mut events) = self.inner.events.lock() {
            events.push(boundary);
        }
        let armed = *self.inner.armed.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(armed) = armed
            && armed.boundary == boundary
        {
            self.inner.fired.store(true, Ordering::SeqCst);
            match armed.action {
                FailpointAction::AbortProcess => {
                    // Abrupt child death for recovery tests.
                    std::process::abort();
                }
                FailpointAction::ReturnIo => return true,
            }
        }
        false
    }
}

impl Default for ColumnarTestController {
    fn default() -> Self {
        Self::new()
    }
}
