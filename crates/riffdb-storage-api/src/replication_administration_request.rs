//! Bounded operation facts; construction grants no lifecycle authority.
use crate::{ChangelogTransactionSequence, FollowerHoldBudget, ReplicationAdministrationActionV1};
use riffdb_types::{CommitSequence, ReplicationFollowerAuditTargetV1, RequestId};

#[derive(Clone, Copy, Eq, PartialEq)]
enum Selection {
    Register {
        budget: FollowerHoldBudget,
        expires_at: Option<CommitSequence>,
    },
    Retire {
        generation: ChangelogTransactionSequence,
    },
}

/// Exact explicit lifecycle request, with no result sequence or authority token.
/// Configured expiry is an internal continuation and has no request constructor.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ReplicationAdministrationRequestV1 {
    request_id: RequestId,
    target: ReplicationFollowerAuditTargetV1,
    selection: Selection,
}

impl ReplicationAdministrationRequestV1 {
    /// Requests one immutable bounded policy. The drained source owner checks
    /// current lineage, exact replay, capacity and expiry against its own head.
    #[must_use]
    pub const fn register(
        request_id: RequestId,
        target: ReplicationFollowerAuditTargetV1,
        budget: FollowerHoldBudget,
        expires_at: Option<CommitSequence>,
    ) -> Self {
        Self {
            request_id,
            target,
            selection: Selection::Register { budget, expires_at },
        }
    }

    /// Selects the exact original registration generation to retire. A stale
    /// generation cannot release another registration or grant cleanup authority.
    #[must_use]
    pub const fn retire(
        request_id: RequestId,
        target: ReplicationFollowerAuditTargetV1,
        generation: ChangelogTransactionSequence,
    ) -> Self {
        Self {
            request_id,
            target,
            selection: Selection::Retire { generation },
        }
    }

    /// Exact authenticated invocation identity.
    #[must_use]
    pub const fn request_id(self) -> RequestId {
        self.request_id
    }

    /// Request-selected lineage and opaque hold identity, never a result target.
    #[must_use]
    pub const fn target(self) -> ReplicationFollowerAuditTargetV1 {
        self.target
    }

    /// Existing closed durable action; requests cannot impersonate expiry.
    #[must_use]
    pub const fn action(self) -> ReplicationAdministrationActionV1 {
        match self.selection {
            Selection::Register { .. } => ReplicationAdministrationActionV1::RegisterFollower,
            Selection::Retire { .. } => ReplicationAdministrationActionV1::RetireFollower,
        }
    }

    /// Complete requested registration policy, absent on retirement.
    #[must_use]
    pub const fn registration_policy(self) -> Option<(FollowerHoldBudget, Option<CommitSequence>)> {
        match self.selection {
            Selection::Register { budget, expires_at } => Some((budget, expires_at)),
            Selection::Retire { .. } => None,
        }
    }

    /// Original generation selected by a retirement request.
    #[must_use]
    pub const fn retirement_generation(self) -> Option<ChangelogTransactionSequence> {
        match self.selection {
            Selection::Retire { generation } => Some(generation),
            Selection::Register { .. } => None,
        }
    }
}

impl std::fmt::Debug for ReplicationAdministrationRequestV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReplicationAdministrationRequestV1([REDACTED])")
    }
}
