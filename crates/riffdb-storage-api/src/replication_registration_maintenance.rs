//! Internal continuation of an already authorized, immutable registration policy.
use crate::{ReplicationSourceHoldV2, StorageError, StoredReplicationAdministrationV1};
use riffdb_types::Timestamp;

/// One bounded maintenance step, with health publication before any release.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplicationRegistrationMaintenanceResultV1 {
    /// No live registration needs a policy transition.
    Idle,
    /// Persisted typed degradation; its live hold still fences retention.
    HealthRecorded(Box<ReplicationSourceHoldV2>),
    /// Configured expiry continued its exact original authorized registration.
    Expired(Box<StoredReplicationAdministrationV1>),
}

/// Coordinator-only closed continuation. Callers supply no identity, expiry,
/// policy, sequence, authenticated invocation or override. Each step changes at
/// most one of the existing bounded registrations under the source barrier.
pub trait ReplicationRegistrationMaintenancePort {
    /// Persists degradation or expires a previously degraded registration. Budget
    /// exhaustion alone never releases custody. A later step revalidates exact
    /// stored policy and original receipt before allocating expiry's audit.
    fn maintain_replication_registration(
        &self,
        timestamp: Timestamp,
    ) -> Result<ReplicationRegistrationMaintenanceResultV1, StorageError>;
}
