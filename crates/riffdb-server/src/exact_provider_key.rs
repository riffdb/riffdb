//! Exact existing registration bytes, with no pre-admission heap allocation.

use super::{CapabilityId, NonZeroU64, PartitionKeyHash, SLOT_KEY_BYTES};
use std::ops::Deref;

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct SlotKey([u8; SLOT_KEY_BYTES]);

impl SlotKey {
    pub(super) fn new(
        plan: riffdb_types::QueryPlanHash,
        partition: PartitionKeyHash,
        policy: riffdb_types::ApplicationRoleHash,
        row_policy: Option<(CapabilityId, NonZeroU64)>,
    ) -> Self {
        let mut bytes = [0; SLOT_KEY_BYTES];
        bytes[..32].copy_from_slice(plan.as_bytes());
        bytes[32..64].copy_from_slice(partition.as_bytes());
        bytes[64..96].copy_from_slice(policy.as_bytes());
        if let Some((capability, revision)) = row_policy {
            bytes[96] = 1;
            bytes[97..113].copy_from_slice(capability.as_bytes());
            bytes[113..121].copy_from_slice(&revision.get().to_be_bytes());
        }
        Self(bytes)
    }
}

impl Deref for SlotKey {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Debug for SlotKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SlotKey([REDACTED])")
    }
}
