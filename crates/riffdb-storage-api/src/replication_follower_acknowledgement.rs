//! Closed native source acknowledgement; never an application write surface.
use crate::{
    ChangelogHistoryPointV3, ChangelogLineageV3, ReplicationSourceHoldIdV1,
    ReplicationSourceHoldKindV1, ReplicationSourceHoldV1,
};

/// Fixed-size native request. The receiver's independently durable evidence
/// and current transport authorization remain the replication owner's duties.
#[doc(hidden)]
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ReplicationFollowerAcknowledgementV3(ReplicationSourceHoldV1);

impl ReplicationFollowerAcknowledgementV3 {
    #[must_use]
    pub const fn new(
        id: ReplicationSourceHoldIdV1,
        lineage: ChangelogLineageV3,
        acknowledged: ChangelogHistoryPointV3,
    ) -> Self {
        Self(ReplicationSourceHoldV1::new(
            id,
            ReplicationSourceHoldKindV1::FollowerAcknowledgement,
            lineage,
            acknowledged,
        ))
    }

    #[must_use]
    pub const fn hold(self) -> ReplicationSourceHoldV1 {
        self.0
    }
}

impl std::fmt::Debug for ReplicationFollowerAcknowledgementV3 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReplicationFollowerAcknowledgementV3([redacted])")
    }
}
