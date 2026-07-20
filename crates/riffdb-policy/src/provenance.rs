//! Policy-owned admission of untrusted provenance and agent-session claims.

use std::fmt;

use riffdb_auth::AuthenticatedPrincipal;
use riffdb_types::{
    ActorKind, AgentSessionId, ApprovalId, ApprovalReference, ProvenanceReason, SourceCommit,
    SourceRepository,
};

/// Bounded but untrusted claims supplied with one invocation.
///
/// Construction proves only the foundational size and character bounds of the
/// component types. It does not make any claim approved for persistence.
#[derive(Clone, Eq, PartialEq)]
pub struct UntrustedInvocationClaims {
    source_repository: Option<SourceRepository>,
    source_commit: Option<SourceCommit>,
    reason: Option<ProvenanceReason>,
    approval_reference: Option<ApprovalReference>,
    agent_session_id: Option<AgentSessionId>,
}

impl UntrustedInvocationClaims {
    /// Collects already bounded caller claims without granting them trust.
    #[must_use]
    pub const fn new(
        source_repository: Option<SourceRepository>,
        source_commit: Option<SourceCommit>,
        reason: Option<ProvenanceReason>,
        approval_reference: Option<ApprovalReference>,
        agent_session_id: Option<AgentSessionId>,
    ) -> Self {
        Self {
            source_repository,
            source_commit,
            reason,
            approval_reference,
            agent_session_id,
        }
    }
}

impl fmt::Debug for UntrustedInvocationClaims {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("UntrustedInvocationClaims([REDACTED])")
    }
}

/// Explicit local policy for retaining an untrusted agent-session identity.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum AgentSessionAdmissionPolicy {
    /// Discard every supplied session identity. This is the POC default.
    #[default]
    Discard,
    /// Retain a session identity only for an authenticated agent principal.
    AllowForAgent,
}

/// Provenance values admitted by policy for durable command provenance.
///
/// Agent-session identity is intentionally not part of this type. It belongs to
/// the admitted actor context and is returned separately by [`ClaimAdmission`].
#[derive(Clone, Eq, PartialEq)]
pub struct AuthorizedProvenanceClaims {
    source_repository: Option<SourceRepository>,
    source_commit: Option<SourceCommit>,
    reason: Option<ProvenanceReason>,
    approval_id: Option<ApprovalId>,
}

impl AuthorizedProvenanceClaims {
    /// Constructs claims only from values approved inside the policy crate.
    ///
    /// WP-110's POC policy has no provider that approves caller provenance, so
    /// its public admission path invokes this with no admitted values.
    pub(crate) const fn from_approved_parts(
        source_repository: Option<SourceRepository>,
        source_commit: Option<SourceCommit>,
        reason: Option<ProvenanceReason>,
        approval_id: Option<ApprovalId>,
    ) -> Self {
        Self {
            source_repository,
            source_commit,
            reason,
            approval_id,
        }
    }

    /// Borrows the policy-approved source repository, when present.
    #[must_use]
    pub const fn source_repository(&self) -> Option<&SourceRepository> {
        self.source_repository.as_ref()
    }

    /// Borrows the policy-approved source commit, when present.
    #[must_use]
    pub const fn source_commit(&self) -> Option<&SourceCommit> {
        self.source_commit.as_ref()
    }

    /// Borrows the policy-approved provenance reason, when present.
    #[must_use]
    pub const fn reason(&self) -> Option<&ProvenanceReason> {
        self.reason.as_ref()
    }

    /// Borrows the policy-validated approval identity, when present.
    #[must_use]
    pub const fn approval_id(&self) -> Option<&ApprovalId> {
        self.approval_id.as_ref()
    }
}

impl fmt::Debug for AuthorizedProvenanceClaims {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AuthorizedProvenanceClaims([REDACTED])")
    }
}

/// Complete policy result for invocation-claim admission.
#[derive(Clone, Eq, PartialEq)]
pub struct ClaimAdmission {
    provenance: AuthorizedProvenanceClaims,
    agent_session_id: Option<AgentSessionId>,
}

impl ClaimAdmission {
    /// Borrows the provenance claims approved for durable admission.
    #[must_use]
    pub const fn provenance(&self) -> &AuthorizedProvenanceClaims {
        &self.provenance
    }

    /// Returns the separately admitted agent-session identity, when permitted.
    #[must_use]
    pub const fn agent_session_id(&self) -> Option<AgentSessionId> {
        self.agent_session_id
    }

    pub(crate) fn into_parts(self) -> (AuthorizedProvenanceClaims, Option<AgentSessionId>) {
        (self.provenance, self.agent_session_id)
    }
}

impl fmt::Debug for ClaimAdmission {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ClaimAdmission([REDACTED])")
    }
}

/// Applies the POC claim policy to one authenticated invocation.
///
/// Source, commit, reason, and approval-reference claims are discarded because
/// the POC has no reviewed provider that can validate them. A supplied session
/// identity is retained only for an authenticated agent and only when the
/// explicit local policy enables it.
#[must_use]
pub fn admit_invocation_claims(
    principal: &AuthenticatedPrincipal,
    claims: UntrustedInvocationClaims,
    agent_session_policy: AgentSessionAdmissionPolicy,
) -> ClaimAdmission {
    admit_for_actor_kind(principal.actor_kind(), claims, agent_session_policy)
}

pub(crate) fn admit_for_actor_kind(
    actor_kind: ActorKind,
    claims: UntrustedInvocationClaims,
    agent_session_policy: AgentSessionAdmissionPolicy,
) -> ClaimAdmission {
    let UntrustedInvocationClaims {
        source_repository: _,
        source_commit: _,
        reason: _,
        approval_reference: _,
        agent_session_id,
    } = claims;
    let agent_session_id = match (actor_kind, agent_session_policy) {
        (ActorKind::Agent, AgentSessionAdmissionPolicy::AllowForAgent) => agent_session_id,
        (ActorKind::Human | ActorKind::Service | ActorKind::Agent, _) => None,
    };
    ClaimAdmission {
        provenance: AuthorizedProvenanceClaims::from_approved_parts(None, None, None, None),
        agent_session_id,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session_id() -> AgentSessionId {
        AgentSessionId::from_unix_milliseconds_and_random(7, [0x2a; 10]).expect("valid test UUIDv7")
    }

    fn untrusted_claims() -> UntrustedInvocationClaims {
        UntrustedInvocationClaims::new(
            Some(SourceRepository::new("private/repository").expect("bounded repository")),
            Some(SourceCommit::new("0123456789abcdef").expect("bounded commit")),
            Some(ProvenanceReason::new("caller supplied reason").expect("bounded reason")),
            Some(ApprovalReference::new("caller-approval").expect("bounded reference")),
            Some(session_id()),
        )
    }

    #[test]
    fn poc_default_discards_every_untrusted_claim() {
        for actor_kind in ActorKind::ALL {
            let admitted = admit_for_actor_kind(
                actor_kind,
                untrusted_claims(),
                AgentSessionAdmissionPolicy::Discard,
            );
            assert_eq!(admitted.provenance().source_repository(), None);
            assert_eq!(admitted.provenance().source_commit(), None);
            assert_eq!(admitted.provenance().reason(), None);
            assert_eq!(admitted.provenance().approval_id(), None);
            assert_eq!(admitted.agent_session_id(), None);
        }
    }

    #[test]
    fn explicit_local_policy_retains_a_session_only_for_agents() {
        let agent = admit_for_actor_kind(
            ActorKind::Agent,
            untrusted_claims(),
            AgentSessionAdmissionPolicy::AllowForAgent,
        );
        assert_eq!(agent.agent_session_id(), Some(session_id()));

        for actor_kind in [ActorKind::Human, ActorKind::Service] {
            let admitted = admit_for_actor_kind(
                actor_kind,
                untrusted_claims(),
                AgentSessionAdmissionPolicy::AllowForAgent,
            );
            assert_eq!(admitted.agent_session_id(), None);
        }
    }

    #[test]
    fn admitted_provenance_can_contain_only_policy_approved_parts() {
        let approved = AuthorizedProvenanceClaims::from_approved_parts(
            Some(SourceRepository::new("approved/repository").expect("bounded repository")),
            Some(SourceCommit::new("fedcba9876543210").expect("bounded commit")),
            Some(ProvenanceReason::new("approved reason").expect("bounded reason")),
            Some(ApprovalId::new("approval-id").expect("bounded approval ID")),
        );
        assert!(approved.source_repository().is_some());
        assert!(approved.source_commit().is_some());
        assert!(approved.reason().is_some());
        assert!(approved.approval_id().is_some());
    }

    #[test]
    fn debug_output_never_contains_claim_facts() {
        let claims = untrusted_claims();
        let admitted = admit_for_actor_kind(
            ActorKind::Agent,
            claims.clone(),
            AgentSessionAdmissionPolicy::AllowForAgent,
        );
        for rendered in [
            format!("{claims:?}"),
            format!("{:?}", admitted.provenance()),
            format!("{admitted:?}"),
        ] {
            assert!(rendered.contains("[REDACTED]"));
            assert!(!rendered.contains("private/repository"));
            assert!(!rendered.contains("0123456789abcdef"));
            assert!(!rendered.contains("caller supplied reason"));
            assert!(!rendered.contains("caller-approval"));
        }
    }
}
