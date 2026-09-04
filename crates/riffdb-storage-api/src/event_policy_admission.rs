//! API-neutral optimistic fence for current-row event-policy admission.
//!
//! The policy engine produces this bounded observation after evaluating one
//! pinned application snapshot. A concrete backend receives no policy program:
//! it only rechecks the exact capability, event, row, and relationship
//! observations at its final mutation or release safe point.

use riffdb_types::{EventId, MAX_KEY_BYTES, PartitionKey, Timestamp};

use crate::{
    StorageValueError, StoredCapabilityRecordV1, StoredDurableEventV1, StoredEntityRecordV1,
};

/// Maximum event observations carried into one final safe-point recheck.
pub const MAX_EVENT_POLICY_ADMISSION_OBSERVATIONS_V1: usize = 1_024;
/// Maximum relationship observations charged to one event row.
pub const MAX_EVENT_POLICY_RELATIONSHIP_OBSERVATIONS_V1: usize = 1_024;

/// One exact neutral relationship-existence observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventPolicyRelationshipObservationV1 {
    partition: PartitionKey,
    index_prefix: Vec<u8>,
    exists: bool,
}

impl EventPolicyRelationshipObservationV1 {
    /// Checks one bounded nonempty physical prefix.
    pub fn new(
        partition: PartitionKey,
        index_prefix: Vec<u8>,
        exists: bool,
    ) -> Result<Self, StorageValueError> {
        if index_prefix.is_empty() || index_prefix.len() > MAX_KEY_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(Self {
            partition,
            index_prefix,
            exists,
        })
    }

    /// Exact aggregate partition expected in a matching current index row.
    #[must_use]
    pub const fn partition(&self) -> &PartitionKey {
        &self.partition
    }

    /// Complete physical index prefix inspected by the policy engine.
    #[must_use]
    pub fn index_prefix(&self) -> &[u8] {
        &self.index_prefix
    }

    /// Exact existence result consumed by policy evaluation.
    #[must_use]
    pub const fn exists(&self) -> bool {
        self.exists
    }
}

/// One event and every current authoritative observation used to admit it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventPolicyAdmissionObservationV1 {
    event: StoredDurableEventV1,
    current: Option<StoredEntityRecordV1>,
    relationships: Vec<EventPolicyRelationshipObservationV1>,
    admitted: bool,
}

impl EventPolicyAdmissionObservationV1 {
    /// Binds an immutable event to its exact current-row observation.
    pub fn new(
        event: StoredDurableEventV1,
        current: Option<StoredEntityRecordV1>,
        relationships: Vec<EventPolicyRelationshipObservationV1>,
        admitted: bool,
    ) -> Result<Self, StorageValueError> {
        if relationships.len() > MAX_EVENT_POLICY_RELATIONSHIP_OBSERVATIONS_V1
            || event.policy_anchor().is_none() && (current.is_some() || admitted)
            || current.as_ref().is_some_and(|record| {
                event
                    .policy_anchor()
                    .is_none_or(|anchor| record.target() != anchor.source())
            })
            || admitted && current.is_none()
        {
            return Err(StorageValueError::InvalidShape);
        }
        Ok(Self {
            event,
            current,
            relationships,
            admitted,
        })
    }

    /// Stable immutable event rechecked at the final safe point.
    #[must_use]
    pub const fn event(&self) -> &StoredDurableEventV1 {
        &self.event
    }

    /// Exact current source row, including absence.
    #[must_use]
    pub const fn current(&self) -> Option<&StoredEntityRecordV1> {
        self.current.as_ref()
    }

    /// Ordered relationship observations consumed by the policy engine.
    #[must_use]
    pub fn relationships(&self) -> &[EventPolicyRelationshipObservationV1] {
        &self.relationships
    }

    /// Whether the policy engine admitted this exact observation.
    #[must_use]
    pub const fn admitted(&self) -> bool {
        self.admitted
    }
}

/// Complete bounded optimistic authority fence for one protected event operation.
#[derive(Clone, Eq, PartialEq)]
pub struct EventPolicyAdmissionFenceV1 {
    capability: StoredCapabilityRecordV1,
    observed_at: Timestamp,
    observations: Vec<EventPolicyAdmissionObservationV1>,
}

impl EventPolicyAdmissionFenceV1 {
    /// Checks nonempty strict event order and the independent candidate bound.
    pub fn new(
        capability: StoredCapabilityRecordV1,
        observed_at: Timestamp,
        observations: Vec<EventPolicyAdmissionObservationV1>,
    ) -> Result<Self, StorageValueError> {
        if observations.len() > MAX_EVENT_POLICY_ADMISSION_OBSERVATIONS_V1
            || observations
                .windows(2)
                .any(|pair| pair[0].event().event_id() >= pair[1].event().event_id())
        {
            return Err(StorageValueError::InvalidShape);
        }
        Ok(Self {
            capability,
            observed_at,
            observations,
        })
    }

    /// Exact durable capability record seen by authorization.
    #[must_use]
    pub const fn capability(&self) -> &StoredCapabilityRecordV1 {
        &self.capability
    }

    /// Service-owned instant used for capability validity.
    #[must_use]
    pub const fn observed_at(&self) -> Timestamp {
        self.observed_at
    }

    /// Strictly event-ordered observations.
    #[must_use]
    pub fn observations(&self) -> &[EventPolicyAdmissionObservationV1] {
        &self.observations
    }

    /// Stable event IDs admitted by policy, retaining selected order.
    pub fn admitted_event_ids(&self) -> impl Iterator<Item = EventId> + '_ {
        self.observations
            .iter()
            .filter(|observation| observation.admitted())
            .map(|observation| observation.event().event_id())
    }

    /// Retains the exact candidate prefix through the requested admitted row.
    /// If fewer rows were admitted, the complete observed prefix remains.
    #[must_use]
    pub fn through_admitted_limit(mut self, limit: usize) -> Self {
        if limit == 0 {
            self.observations.clear();
            return self;
        }
        let mut admitted = 0usize;
        let mut truncate = None;
        for (index, observation) in self.observations.iter().enumerate() {
            if observation.admitted() {
                admitted = admitted.saturating_add(1);
                if admitted == limit {
                    truncate = Some(index.saturating_add(1));
                    break;
                }
            }
        }
        if let Some(length) = truncate {
            self.observations.truncate(length);
        }
        self
    }
}

impl std::fmt::Debug for EventPolicyAdmissionFenceV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EventPolicyAdmissionFenceV1")
            .field("capability_id", &self.capability.capability_id())
            .field("capability_revision", &self.capability.revision())
            .field("observations", &self.observations.len())
            .field("authority", &"[REDACTED]")
            .finish()
    }
}
