use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use riffdb_storage_api::{StorageError, StorageErrorKind};

use crate::error::storage_error;

/// Closed external-maintenance durability boundaries for process recovery tests.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RedbMaintenanceFailpoint {
    /// Before a complete replacement receipt file is synchronized.
    BeforeReceiptFileSync,
    /// After replacement bytes are synchronized but before atomic rename.
    AfterReceiptFileSync,
    /// Immediately before the receipt rename.
    BeforeReceiptRename,
    /// After receipt rename but before parent-directory synchronization.
    AfterReceiptRename,
    /// After the receipt and its directory entry are synchronized.
    AfterReceiptParentSync,
    /// After one immutable named backup is durably published.
    AfterNamedBackupPublication,
    /// After a private staged restore is complete and checksum-validated.
    AfterStagedMaterialization,
    /// Before the configured database file is atomically replaced.
    BeforeTargetPublication,
    /// After configured database replacement but before parent synchronization.
    AfterTargetPublication,
    /// After the configured database file and parent directory are synchronized.
    AfterTargetParentSync,
}

/// One redaction-safe observation from a maintenance test controller.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RedbMaintenanceTestEvent {
    failpoint: RedbMaintenanceFailpoint,
}

impl RedbMaintenanceTestEvent {
    /// Returns the fixed boundary that was observed.
    #[must_use]
    pub const fn failpoint(self) -> RedbMaintenanceFailpoint {
        self.failpoint
    }
}

/// Closed one-shot maintenance failure controller.
///
/// Production construction installs no controller. This type admits no
/// callbacks, filesystem text, payloads, or application data.
#[doc(hidden)]
#[derive(Clone)]
pub struct RedbMaintenanceTestController {
    inner: Arc<ControllerInner>,
}

struct ControllerInner {
    armed: RedbMaintenanceFailpoint,
    action: FailpointAction,
    fired: AtomicBool,
    events: Mutex<Vec<RedbMaintenanceTestEvent>>,
}

#[derive(Clone, Copy)]
enum FailpointAction {
    Return,
    AbortProcess,
}

impl RedbMaintenanceTestController {
    /// Returns a typed failure once at the selected closed boundary.
    #[must_use]
    pub fn return_at(failpoint: RedbMaintenanceFailpoint) -> Self {
        Self::new(failpoint, FailpointAction::Return)
    }

    /// Aborts a child process once at the selected closed boundary.
    #[must_use]
    pub fn abort_at(failpoint: RedbMaintenanceFailpoint) -> Self {
        Self::new(failpoint, FailpointAction::AbortProcess)
    }

    /// Returns the bounded redaction-safe boundary history.
    #[must_use]
    pub fn events(&self) -> Vec<RedbMaintenanceTestEvent> {
        self.inner
            .events
            .lock()
            .map_or_else(|_| Vec::new(), |events| events.clone())
    }

    fn new(failpoint: RedbMaintenanceFailpoint, action: FailpointAction) -> Self {
        Self {
            inner: Arc::new(ControllerInner {
                armed: failpoint,
                action,
                fired: AtomicBool::new(false),
                events: Mutex::new(Vec::new()),
            }),
        }
    }

    pub(super) fn hit(
        &self,
        failpoint: RedbMaintenanceFailpoint,
        publication_may_be_durable: bool,
    ) -> Result<(), StorageError> {
        if let Ok(mut events) = self.inner.events.lock()
            && events.len() < 32
        {
            events.push(RedbMaintenanceTestEvent { failpoint });
        }
        if self.inner.armed != failpoint || self.inner.fired.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        match self.inner.action {
            FailpointAction::Return => Err(storage_error(if publication_may_be_durable {
                StorageErrorKind::CommitStatusUnknown
            } else {
                StorageErrorKind::Unavailable
            })),
            FailpointAction::AbortProcess => std::process::abort(),
        }
    }
}
