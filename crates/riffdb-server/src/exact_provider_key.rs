//! Exact existing registration bytes, with no pre-admission heap allocation.

use super::{CapabilityId, NonZeroU64, PartitionKeyHash, SLOT_KEY_BYTES};
use std::ops::Deref;

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct SlotKey([u8; SLOT_KEY_BYTES]);

impl SlotKey {
    /// A fresh authorized request can supersede only lower revisions of its
    /// exact capability. Plan/partition/role bytes remain independent identities;
    /// this comparison grants no reuse or authority to any other registration.
    pub(super) fn supersedes_capability(&self, prior: &Self) -> bool {
        self.0[96] == 1
            && prior.0[96] == 1
            && self.0[97..113] == prior.0[97..113]
            && self.0[113..121] > prior.0[113..121]
    }
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

#[cfg(test)]
mod tests {
    // req: OQ-040
    use super::*;
    #[test]
    fn capability_supersession_requires_same_authority_and_strictly_newer_revision() {
        let capability = CapabilityId::from_unix_milliseconds_and_random(7, [0x44; 10]).unwrap();
        let other = CapabilityId::from_unix_milliseconds_and_random(7, [0x45; 10]).unwrap();
        let key = |seed: u8, capability: Option<CapabilityId>, revision: u64| {
            SlotKey::new(
                riffdb_types::QueryPlanHash::from_bytes([seed; 32]),
                PartitionKeyHash::from_bytes([seed; 32]),
                riffdb_types::ApplicationRoleHash::from_bytes([seed; 32]),
                capability.map(|id| (id, NonZeroU64::new(revision).unwrap())),
            )
        };
        let original = key(1, Some(capability), 2);
        assert!(key(2, Some(capability), 3).supersedes_capability(&original));
        assert!(!key(2, Some(capability), 2).supersedes_capability(&original));
        assert!(!key(2, Some(capability), 1).supersedes_capability(&original));
        assert!(!key(2, Some(other), 3).supersedes_capability(&original));
        assert!(!key(2, None, 3).supersedes_capability(&original));
        assert!(!key(2, Some(capability), 3).supersedes_capability(&key(1, None, 2)));
    }
}
