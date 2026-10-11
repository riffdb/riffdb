//! Restricted native dispatcher handle over an activated database.
//!
//! Admission can wait for checkpoints without retaining the server's port
//! bundle lock. Every transition still uses the same sole FIFO mutation gate,
//! canonical validation, captured Immediate transaction and commit fencing.

use std::sync::Arc;

use riffdb_storage_api::{
    OutboxClaimV1, OutboxDeadLetterV1, OutboxPageLimit, OutboxRenewV1, OutboxRepository,
    OutboxRetryV1, OutboxStatusReadResultV1, OutboxSucceedV1, OutboxTransitionResultV1,
    PendingOutboxScanV1, StorageError, UndeliveredOutboxStatusScanRequestV1,
    UndeliveredOutboxStatusScanV1,
};
use riffdb_types::EventId;

use crate::RedbOperationalPorts;

/// Move-only native outbox capability obtained from the activated storage bundle.
///
/// This is a server composition port, not an application write interface.
/// It exposes no command, administration or raw transaction authority.
///
/// ```compile_fail
/// fn requires_clone<T: Clone>() {}
/// requires_clone::<riffdb_storage_redb::RedbOutboxPorts>();
/// ```
pub struct RedbOutboxPorts {
    ports: RedbOperationalPorts,
}

impl RedbOperationalPorts {
    /// Obtains the narrow dispatcher capability over this activated database.
    /// Mutation ownership remains in the existing storage transaction states.
    #[must_use]
    pub fn outbox_ports(&self) -> RedbOutboxPorts {
        RedbOutboxPorts {
            ports: RedbOperationalPorts {
                shared: Arc::clone(&self.shared),
            },
        }
    }
}

impl OutboxRepository for RedbOutboxPorts {
    fn supports_outbox_claim_groups(&self) -> bool {
        OutboxRepository::supports_outbox_claim_groups(&self.ports)
    }

    fn claim_outbox_group(
        &mut self,
        group: &riffdb_storage_api::OutboxClaimGroupV1,
    ) -> Result<riffdb_storage_api::OutboxClaimGroupResultV1, StorageError> {
        OutboxRepository::claim_outbox_group(&mut self.ports, group)
    }

    fn supports_outbox_succeed_groups(&self) -> bool {
        OutboxRepository::supports_outbox_succeed_groups(&self.ports)
    }

    fn succeed_outbox_group(
        &mut self,
        group: &riffdb_storage_api::OutboxSucceedGroupV1,
    ) -> Result<riffdb_storage_api::OutboxSucceedGroupResultV1, StorageError> {
        OutboxRepository::succeed_outbox_group(&mut self.ports, group)
    }

    fn has_undelivered_outbox(&self) -> Result<bool, StorageError> {
        OutboxRepository::has_undelivered_outbox(&self.ports)
    }

    fn read_outbox_status(
        &self,
        event_id: EventId,
    ) -> Result<OutboxStatusReadResultV1, StorageError> {
        OutboxRepository::read_outbox_status(&self.ports, event_id)
    }

    fn scan_pending_outbox(
        &self,
        after: Option<EventId>,
        limit: OutboxPageLimit,
    ) -> Result<PendingOutboxScanV1, StorageError> {
        OutboxRepository::scan_pending_outbox(&self.ports, after, limit)
    }

    fn scan_undelivered_outbox_statuses(
        &self,
        request: UndeliveredOutboxStatusScanRequestV1,
    ) -> Result<UndeliveredOutboxStatusScanV1, StorageError> {
        OutboxRepository::scan_undelivered_outbox_statuses(&self.ports, request)
    }

    fn claim_outbox(
        &mut self,
        transition: &OutboxClaimV1,
    ) -> Result<OutboxTransitionResultV1, StorageError> {
        OutboxRepository::claim_outbox(&mut self.ports, transition)
    }

    fn renew_outbox(
        &mut self,
        transition: &OutboxRenewV1,
    ) -> Result<OutboxTransitionResultV1, StorageError> {
        OutboxRepository::renew_outbox(&mut self.ports, transition)
    }

    fn succeed_outbox(
        &mut self,
        transition: &OutboxSucceedV1,
    ) -> Result<OutboxTransitionResultV1, StorageError> {
        OutboxRepository::succeed_outbox(&mut self.ports, transition)
    }

    fn retry_outbox(
        &mut self,
        transition: &OutboxRetryV1,
    ) -> Result<OutboxTransitionResultV1, StorageError> {
        OutboxRepository::retry_outbox(&mut self.ports, transition)
    }

    fn dead_letter_outbox(
        &mut self,
        transition: &OutboxDeadLetterV1,
    ) -> Result<OutboxTransitionResultV1, StorageError> {
        OutboxRepository::dead_letter_outbox(&mut self.ports, transition)
    }
}
