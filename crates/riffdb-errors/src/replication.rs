//! Shared payload-free replication refusals across service and storage boundaries.

/// Closed, payload-free replication negotiation or stream refusal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicationStreamErrorV3 {
    /// The database or history incarnation differs from the durable pin.
    ForeignLineage,
    /// The leadership epoch differs from the durable pin.
    StaleEpoch,
    /// Retention has removed the requested successor history.
    HistoryPruned,
    /// The peer cannot read the production V3 format.
    UnsupportedFormat,
    /// The peer names a different authoritative inventory.
    UnsupportedCatalog,
    /// The peer cannot honor the exact production frame ceilings.
    UnsupportedBounds,
    /// The requested position, hash, or frontier does not match retained history.
    InvalidPosition,
    /// A source violated exact receipt continuity or its pinned end.
    CorruptHistory,
    /// The published source could not be read.
    Unavailable,
}

impl std::fmt::Display for ReplicationStreamErrorV3 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::ForeignLineage => "foreign replication lineage",
            Self::StaleEpoch => "stale replication epoch",
            Self::HistoryPruned => "replication history has been pruned",
            Self::UnsupportedFormat => "unsupported replication format",
            Self::UnsupportedCatalog => "unsupported replication catalog",
            Self::UnsupportedBounds => "unsupported replication bounds",
            Self::InvalidPosition => "invalid replication resume position",
            Self::CorruptHistory => "invalid replication history",
            Self::Unavailable => "replication source is unavailable",
        })
    }
}

impl std::error::Error for ReplicationStreamErrorV3 {}
