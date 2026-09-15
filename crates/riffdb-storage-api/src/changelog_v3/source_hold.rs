use super::{ChangelogHistoryPointV3, ChangelogLineageV3};

/// Hard total ceiling, including every follower, archive and bootstrap hold.
/// This bounds follower count as well as the combined source-only population.
pub const MAX_REPLICATION_SOURCE_HOLDS_V1: u64 = 4096;

/// Opaque storage-local hold identity, never a NodeId or authority token.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ReplicationSourceHoldIdV1([u8; 16]);

impl ReplicationSourceHoldIdV1 {
    /// Zero is unassigned. Constructing an ID grants no mutation capability.
    #[must_use]
    pub fn new(bytes: [u8; 16]) -> Option<Self> {
        (bytes != [0; 16]).then_some(Self(bytes))
    }

    /// Canonical bytes for the storage owner's codec and key.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl std::fmt::Debug for ReplicationSourceHoldIdV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReplicationSourceHoldIdV1([redacted])")
    }
}

/// Closed source-fence owners. Discriminants are the frozen V1 key tags.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum ReplicationSourceHoldKindV1 {
    /// Registered follower's exact durable acknowledged position.
    FollowerAcknowledgement = 1,
    /// Registered archive's exact durable acknowledged position.
    ArchiveAcknowledgement = 2,
    /// Live bootstrap's exact snapshot-to-tail fence.
    Bootstrap = 3,
}

/// A bounded source-only fence value. Decoding or constructing it does not prove
/// registration, durable acknowledgement, ancestry, or permission to reclaim.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ReplicationSourceHoldV1 {
    id: ReplicationSourceHoldIdV1,
    kind: ReplicationSourceHoldKindV1,
    lineage: ChangelogLineageV3,
    fence: ChangelogHistoryPointV3,
}

impl ReplicationSourceHoldV1 {
    /// Assembles typed values; the storage owner must validate the fence against
    /// the retained receipt and lineage from the same transaction before use.
    #[must_use]
    pub const fn new(
        id: ReplicationSourceHoldIdV1,
        kind: ReplicationSourceHoldKindV1,
        lineage: ChangelogLineageV3,
        fence: ChangelogHistoryPointV3,
    ) -> Self {
        Self {
            id,
            kind,
            lineage,
            fence,
        }
    }

    /// Storage-local identity.
    #[must_use]
    pub const fn id(self) -> ReplicationSourceHoldIdV1 {
        self.id
    }
    /// Closed owner kind.
    #[must_use]
    pub const fn kind(self) -> ReplicationSourceHoldKindV1 {
        self.kind
    }
    /// Exact lineage binding, including the accepted authority catalog.
    #[must_use]
    pub const fn lineage(self) -> ChangelogLineageV3 {
        self.lineage
    }
    /// Exact retained position, history hash and dual frontier.
    #[must_use]
    pub const fn fence(self) -> ChangelogHistoryPointV3 {
        self.fence
    }
    /// Canonical fixed-size key, repeated in the durable value to detect substitution.
    #[must_use]
    pub fn storage_key(self) -> [u8; 17] {
        Self::storage_key_for(self.id, self.kind)
    }
    /// Canonical lookup key without inventing a lineage or fence for a probe.
    #[must_use]
    pub fn storage_key_for(
        id: ReplicationSourceHoldIdV1,
        kind: ReplicationSourceHoldKindV1,
    ) -> [u8; 17] {
        let mut key = [0; 17];
        key[0] = kind as u8;
        key[1..].copy_from_slice(id.as_bytes());
        key
    }
}

impl std::fmt::Debug for ReplicationSourceHoldV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReplicationSourceHoldV1([redacted])")
    }
}
