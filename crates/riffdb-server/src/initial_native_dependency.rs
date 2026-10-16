//! Request-owned native dependency waiting on the existing blocking driver.

use riffdb_service::{
    InitialNativeDependencyAdmission, InitialNativeDependencyError, InitialNativeDependencyPort,
    RequestControl,
};
use riffdb_storage_api::{StorageError, StorageErrorKind};
use riffdb_storage_redb::{NativeDependencyCancellation, OwnedNativeDependencyWait};

use crate::port_driver::{BlockingPortDriver, BlockingPortExecutor};
use crate::storage::SharedRedbOperationalPorts;

pub(crate) struct ServerInitialNativeDependencyPort {
    storage: SharedRedbOperationalPorts,
    wait: BlockingPortExecutor<OwnedNativeDependencyWait, (), InitialNativeDependencyError>,
}

impl ServerInitialNativeDependencyPort {
    pub(crate) fn new(storage: SharedRedbOperationalPorts, driver: &BlockingPortDriver) -> Self {
        Self {
            storage,
            wait: driver
                .executor(|wait: OwnedNativeDependencyWait| wait.wait().map_err(map_storage_error)),
        }
    }
}

/// Cancels only this admitted waiter, including an unpolled or dropped future.
/// The native construction owner is independent and retains its own authority.
struct CancelNativeWaitOnDrop(NativeDependencyCancellation);

impl Drop for CancelNativeWaitOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

impl InitialNativeDependencyPort for ServerInitialNativeDependencyPort {
    fn begin_initial_admission<'a>(
        &'a self,
        control: &'a RequestControl,
    ) -> Result<InitialNativeDependencyAdmission<'a>, InitialNativeDependencyError> {
        // A complete source follows the ordinary cancelled/expired request
        // audit rules. Probe it before the driver's RequestControl precheck.
        let Some(wait) = self
            .storage
            .initial_native_dependency()
            .map_err(map_storage_error)?
        else {
            return Ok(InitialNativeDependencyAdmission::Ready);
        };
        // This guard is captured before constructing the future, so dropping
        // Pending without polling also wakes and releases the native waiter.
        let cancel = CancelNativeWaitOnDrop(wait.cancellation());
        let reservation = self.wait.reserve(control);
        Ok(InitialNativeDependencyAdmission::Pending(Box::pin(
            async move {
                let _cancel = cancel;
                let permit = reservation.map_err(|_| InitialNativeDependencyError::Unavailable)?;
                permit
                    .submit(wait)
                    .map_err(|_| InitialNativeDependencyError::Unavailable)?
                    .completion()
                    .await
                    .map_err(|_| InitialNativeDependencyError::Unavailable)?
            },
        )))
    }
}

fn map_storage_error(error: StorageError) -> InitialNativeDependencyError {
    match error.kind() {
        StorageErrorKind::Unavailable | StorageErrorKind::LimitExceeded => {
            InitialNativeDependencyError::Unavailable
        }
        // An unexpected proof, source, engine or durability failure must reach
        // the service's existing integrity health handling, never healthy Pending.
        _ => InitialNativeDependencyError::Integrity,
    }
}
