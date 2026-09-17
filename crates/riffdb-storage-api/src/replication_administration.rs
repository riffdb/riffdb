//! Checked follower lifecycle receipt values. These are evidence shapes, never
//! authorization, ancestry, durability, acknowledgement or release capabilities.
#[path = "replication_administration_request.rs"]
mod request;
pub use request::ReplicationAdministrationRequestV1;

use crate::{
    AuditPrincipalV1, ChangelogHistoryPointV3 as Point, ChangelogTransactionSequence,
    FollowerRegistrationPhaseV1 as Phase, ReplicationSourceHoldKindV1 as Kind,
    ReplicationSourceHoldV1 as Hold, ReplicationSourceHoldV2 as Policy, StorageValueError as Error,
};
use riffdb_types::{
    AdministrationSequence, ApprovalId, ReplicationFollowerAuditTargetV1, RequestId, Timestamp,
};

/// The two accepted generations of source-hold evidence; neither grants authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicationSourceHoldStateV1 {
    /// Frozen unregistered hold, also used by archive and bootstrap owners.
    Legacy(Hold),
    /// Audited follower registration policy, including retired tombstones.
    Registered(Policy),
}

/// Closed actions in the accepted replication administration receipt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicationAdministrationActionV1 {
    /// Register a fresh identity or preserve a legacy follower's exact fence.
    RegisterFollower,
    /// Explicitly retire one exact live generation.
    RetireFollower,
    /// Continue a previously authorized configured expiry after degradation.
    ExpireFollower,
}

/// Explicit invocation or the original registration's expiry provenance.
#[derive(Clone, Eq, PartialEq)]
pub enum ReplicationAdministrationOriginV1 {
    /// The service supplies checked current authority; the value cannot prove it.
    Explicit {
        /// Exact invocation identity used for replay.
        request_id: RequestId,
        /// Authenticated principal and the authorized capability revision.
        principal: AuditPrincipalV1,
        /// Approval checked by the service, where required.
        approval_id: Option<ApprovalId>,
    },
    /// No new authenticated invocation is invented for internal expiry.
    ConfiguredExpiry {
        /// Original authorized registration's administration receipt sequence.
        registration: AdministrationSequence,
    },
}
impl std::fmt::Debug for ReplicationAdministrationOriginV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReplicationAdministrationOriginV1([redacted])")
    }
}

/// Complete bounded administration receipt. The owner additionally validates
/// current authorization, exact stored before-image, original receipt and history
/// ancestry, and atomically persists the receipt, hold and V3 transaction.
#[derive(Clone, Eq, PartialEq)]
pub struct StoredReplicationAdministrationV1 {
    administration_sequence: AdministrationSequence,
    timestamp: Timestamp,
    action: ReplicationAdministrationActionV1,
    target: ReplicationFollowerAuditTargetV1,
    before: Option<ReplicationSourceHoldStateV1>,
    after: Policy,
    observed: Point,
    origin: ReplicationAdministrationOriginV1,
}
impl StoredReplicationAdministrationV1 {
    /// Checks a complete transition without granting permission to perform it.
    /// The administration sequence must immediately follow the drained source
    /// predecessor. Exact retries return the old receipt instead of a new value.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        administration_sequence: AdministrationSequence,
        timestamp: Timestamp,
        action: ReplicationAdministrationActionV1,
        target: ReplicationFollowerAuditTargetV1,
        before: Option<ReplicationSourceHoldStateV1>,
        after: Policy,
        observed: Point,
        origin: ReplicationAdministrationOriginV1,
    ) -> Result<Self, Error> {
        use ReplicationAdministrationActionV1 as Action;
        use ReplicationAdministrationOriginV1 as Origin;
        use ReplicationSourceHoldStateV1 as State;
        let hold = after.hold();
        let lineage = hold.lineage();
        if target.database_id() != lineage.database_id()
            || target.history_incarnation() != lineage.history_incarnation()
            || target.leadership_epoch() != lineage.leadership_epoch()
            || target.hold_id() != hold.id()
            || next_administration(observed)? != administration_sequence
            || observed.sequence().checked_next().is_none()
            || !hold.fence().precedes_or_equals(observed)
            || !after.registered_at().precedes_or_equals(observed)
            || after
                .degraded_at()
                .is_some_and(|p| !p.precedes_or_equals(observed))
        {
            return Err(Error::IdentityMismatch);
        }
        match (action, before, &origin) {
            (Action::RegisterFollower, prior, Origin::Explicit { .. }) => {
                if after.registered_at() != observed || after.degraded_at().is_some() {
                    return Err(Error::InvalidShape);
                }
                match prior {
                    None if after.phase() == Phase::AwaitingBootstrap
                        && hold.fence() == observed => {}
                    Some(State::Legacy(prior))
                        if prior.kind() == Kind::FollowerAcknowledgement
                            && prior == hold
                            && after.phase() == Phase::Attached => {}
                    _ => return Err(Error::InvalidShape),
                }
            }
            (
                Action::RetireFollower | Action::ExpireFollower,
                Some(State::Registered(prior)),
                _,
            ) => {
                if prior.phase() == Phase::Retired
                    || after.phase() != Phase::Retired
                    || prior.hold() != hold
                    || prior.registered_at() != after.registered_at()
                    || prior.budget() != after.budget()
                    || prior.expires_at() != after.expires_at()
                    || prior.degraded_at() != after.degraded_at()
                    || prior.generation() > observed.sequence()
                    || next_administration(prior.registered_at())? >= administration_sequence
                {
                    return Err(Error::InvalidShape);
                }
                match (action, &origin) {
                    (Action::RetireFollower, Origin::Explicit { .. }) => {}
                    (Action::ExpireFollower, Origin::ConfiguredExpiry { registration }) => {
                        let expiry = prior.expires_at().ok_or(Error::InvalidShape)?;
                        if prior.degraded_at().is_none()
                            || observed
                                .frontier()
                                .application()
                                .is_none_or(|head| head < expiry)
                            || next_administration(prior.registered_at())? != *registration
                            || *registration >= administration_sequence
                        {
                            return Err(Error::InvalidShape);
                        }
                    }
                    _ => return Err(Error::InvalidShape),
                }
            }
            _ => return Err(Error::InvalidShape),
        }
        Ok(Self {
            administration_sequence,
            timestamp,
            action,
            target,
            before,
            after,
            observed,
            origin,
        })
    }
    /// Coordinator-assigned administration sequence, not a caller-selected result.
    #[must_use]
    pub const fn administration_sequence(&self) -> AdministrationSequence {
        self.administration_sequence
    }
    /// Coordinator timestamp, never a source of deterministic runtime time.
    #[must_use]
    pub const fn timestamp(&self) -> Timestamp {
        self.timestamp
    }
    /// Exact closed lifecycle action.
    #[must_use]
    pub const fn action(&self) -> ReplicationAdministrationActionV1 {
        self.action
    }
    /// Request-selected, lineage-scoped follower identity.
    #[must_use]
    pub const fn target(&self) -> ReplicationFollowerAuditTargetV1 {
        self.target
    }
    /// Exact immutable physical registration generation.
    #[must_use]
    pub const fn generation(&self) -> ChangelogTransactionSequence {
        self.after.generation()
    }
    /// Absent, frozen legacy or checked registered before-image.
    #[must_use]
    pub const fn before(&self) -> Option<ReplicationSourceHoldStateV1> {
        self.before
    }
    /// Exact post-transition policy; retirement preserves all policy fields.
    #[must_use]
    pub const fn after(&self) -> Policy {
        self.after
    }
    /// Drained source predecessor; storage must additionally prove its ancestry.
    #[must_use]
    pub const fn observed(&self) -> Point {
        self.observed
    }
    /// Explicit authenticated invocation or prior authorization's continuation.
    #[must_use]
    pub const fn origin(&self) -> &ReplicationAdministrationOriginV1 {
        &self.origin
    }
}
impl std::fmt::Debug for StoredReplicationAdministrationV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("StoredReplicationAdministrationV1([redacted])")
    }
}
fn next_administration(point: Point) -> Result<AdministrationSequence, Error> {
    point
        .frontier()
        .administration()
        .map_or(0, |s| s.get())
        .checked_add(1)
        .and_then(AdministrationSequence::new)
        .ok_or(Error::InvalidShape)
}
