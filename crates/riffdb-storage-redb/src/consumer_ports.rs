//! Restricted native event-consumer composition over one activated database.
//! Admission waits retain neither the server's shared port bundle nor another
//! writer lease. The existing consumer implementation owns all semantics.

use std::sync::Arc;

use riffdb_storage_api::{
    CoordinateConsumerLeaseResultV1, EventConsumerRepository, EventConsumerSnapshotV1,
    EventConsumerTransitionResultV1, EventConsumerTransitionV1, StorageError,
};
use riffdb_types::EventConsumerIdentityHash;

use crate::{
    ProtectedEventConsumerLeaseV1, ProtectedEventConsumerLeaseValidationResultV1,
    ProtectedEventConsumerLeaseValidationV1, ProtectedEventConsumerResolutionV1,
    ProtectedEventReplayResultV1, ProtectedEventReplayV1, RedbOperationalPorts,
};

/// Move-only native consumer capability from an activated storage bundle.
/// All protected operations retain the original transaction-current authority,
/// lease validation, FIFO writer, Immediate commit and uncertainty fencing.
/// No command, administration or raw transaction authority is exposed.
///
/// ```compile_fail
/// fn requires_clone<T: Clone>() {}
/// requires_clone::<riffdb_storage_redb::RedbEventConsumerPorts>();
/// ```
pub struct RedbEventConsumerPorts {
    ports: RedbOperationalPorts,
}

impl RedbOperationalPorts {
    /// Obtains the narrow native consumer port over this activated database.
    #[must_use]
    pub fn event_consumer_ports(&self) -> RedbEventConsumerPorts {
        RedbEventConsumerPorts {
            ports: RedbOperationalPorts {
                shared: Arc::clone(&self.shared),
            },
        }
    }
}

impl RedbEventConsumerPorts {
    /// Uses the existing protected selection and atomic lease coordinator.
    pub fn coordinate_protected_event_consumer_lease(
        &mut self,
        request: ProtectedEventConsumerLeaseV1,
    ) -> Result<CoordinateConsumerLeaseResultV1, StorageError> {
        self.ports
            .coordinate_protected_event_consumer_lease(request)
    }

    /// Revalidates current lease and authority inside the existing writer fence.
    pub fn validate_protected_event_consumer_lease(
        &mut self,
        request: ProtectedEventConsumerLeaseValidationV1,
    ) -> Result<ProtectedEventConsumerLeaseValidationResultV1, StorageError> {
        self.ports.validate_protected_event_consumer_lease(request)
    }

    /// Resolves the original protected lease in its transaction-current fence.
    pub fn coordinate_protected_event_consumer_resolution(
        &mut self,
        request: ProtectedEventConsumerResolutionV1,
    ) -> Result<EventConsumerTransitionResultV1, StorageError> {
        self.ports
            .coordinate_protected_event_consumer_resolution(request)
    }

    /// Replays through the existing transaction-current protected reader.
    pub fn replay_protected_events(
        &mut self,
        request: ProtectedEventReplayV1,
    ) -> Result<ProtectedEventReplayResultV1, StorageError> {
        self.ports.replay_protected_events(request)
    }
}

impl EventConsumerRepository for RedbEventConsumerPorts {
    fn inspect_event_consumer(
        &self,
        consumer_identity_hash: EventConsumerIdentityHash,
    ) -> Result<Option<EventConsumerSnapshotV1>, StorageError> {
        self.ports.inspect_event_consumer(consumer_identity_hash)
    }

    fn transition_event_consumer(
        &mut self,
        transition: EventConsumerTransitionV1,
    ) -> Result<EventConsumerTransitionResultV1, StorageError> {
        self.ports.transition_event_consumer(transition)
    }

    fn inspect_event_consumer_inventory(
        &self,
    ) -> Result<Vec<EventConsumerSnapshotV1>, StorageError> {
        self.ports.inspect_event_consumer_inventory()
    }

    fn event_consumer_retention_low_water(&self) -> Result<Option<u64>, StorageError> {
        self.ports.event_consumer_retention_low_water()
    }
}
