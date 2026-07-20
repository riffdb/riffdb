//! Consumer-owned service-audit interfaces for coordinator orchestration.

use std::num::NonZeroU64;

use riffdb_types::{
    ActorId, ActorKind, ApprovalId, CapabilityId, RequestId, ServiceAuditLinkV1,
    ServiceAuditPhaseV1, ServiceAuditTargetsV1, ServiceIngressKindV1, ServiceOperationV1,
};

/// Borrowed, checked semantic input for one authenticated service-audit record.
///
/// The application service owns the concrete input and its constructors. The
/// commit coordinator consumes only this object-safe view, then supplies its
/// own timestamp and delegates the new audit record's sequence assignment to
/// the atomic storage transition. Implementations must therefore return the
/// complete immutable values established for one invocation; this trait grants
/// no authorization and performs no shape repair. A closed result link may
/// carry the sequence of an already authoritative control-plane result; that
/// sequence is not the sequence assigned to the new audit record.
///
/// Principal-less bootstrap audit records deliberately cannot use this view.
/// They require the separate, opaque [`BootstrapCompoundAuditProof`].
pub trait AdministrationAuditInputView: Send + Sync {
    /// Returns the transport submission identity.
    fn request_id(&self) -> &RequestId;

    /// Returns the closed service operation.
    fn operation(&self) -> &ServiceOperationV1;

    /// Returns the selected audit lifecycle phase.
    fn phase(&self) -> &ServiceAuditPhaseV1;

    /// Returns the stable authenticated principal identity.
    fn principal_id(&self) -> &ActorId;

    /// Returns the authenticated principal's checked actor class.
    fn actor_kind(&self) -> &ActorKind;

    /// Returns the exact authorizing capability identity.
    fn capability_id(&self) -> &CapabilityId;

    /// Returns the exact authorizing capability revision.
    fn capability_revision(&self) -> &NonZeroU64;

    /// Returns the trusted ingress class.
    fn ingress(&self) -> &ServiceIngressKindV1;

    /// Returns the already checked canonical target collection.
    fn targets(&self) -> &ServiceAuditTargetsV1;

    /// Returns the optional validated approval identity.
    fn approval_id(&self) -> Option<&ApprovalId>;

    /// Returns the independent closed authoritative-result link.
    ///
    /// A control-plane link contains the sequence of the already authoritative
    /// result being audited. It does not select or expose the sequence that the
    /// storage transition will assign to this new audit record.
    fn link(&self) -> &ServiceAuditLinkV1;
}

/// Move-only evidence reserved for the principal-less bootstrap transition.
///
/// This value is opaque, nonserializable, and has no public constructor. The
/// checked bootstrap coordinator path will construct and consume it together
/// with the accepted bootstrap preparation when that path is implemented. It
/// cannot be fabricated by the application service or substituted for an
/// authenticated [`AdministrationAuditInputView`].
///
/// External code cannot construct the proof:
///
/// ```compile_fail
/// use riffdb_commit::BootstrapCompoundAuditProof;
///
/// let _ = BootstrapCompoundAuditProof {};
/// ```
///
/// The proof is intentionally move-only:
///
/// ```compile_fail
/// use riffdb_commit::BootstrapCompoundAuditProof;
///
/// fn duplicate(proof: &BootstrapCompoundAuditProof) -> BootstrapCompoundAuditProof {
///     proof.clone()
/// }
/// ```
///
/// The proof has no default fabrication path:
///
/// ```compile_fail
/// use riffdb_commit::BootstrapCompoundAuditProof;
///
/// fn require_default<T: Default>() {}
/// require_default::<BootstrapCompoundAuditProof>();
/// ```
#[must_use = "bootstrap audit authority must be consumed by the checked compound transition"]
pub struct BootstrapCompoundAuditProof {
    _private: BootstrapCompoundAuditProofSeal,
}

struct BootstrapCompoundAuditProofSeal;

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_types::AdministrationSequence;

    struct CheckedAuditInput {
        request_id: RequestId,
        operation: ServiceOperationV1,
        phase: ServiceAuditPhaseV1,
        principal_id: ActorId,
        actor_kind: ActorKind,
        capability_id: CapabilityId,
        capability_revision: NonZeroU64,
        ingress: ServiceIngressKindV1,
        targets: ServiceAuditTargetsV1,
        approval_id: Option<ApprovalId>,
        link: ServiceAuditLinkV1,
    }

    impl AdministrationAuditInputView for CheckedAuditInput {
        fn request_id(&self) -> &RequestId {
            &self.request_id
        }

        fn operation(&self) -> &ServiceOperationV1 {
            &self.operation
        }

        fn phase(&self) -> &ServiceAuditPhaseV1 {
            &self.phase
        }

        fn principal_id(&self) -> &ActorId {
            &self.principal_id
        }

        fn actor_kind(&self) -> &ActorKind {
            &self.actor_kind
        }

        fn capability_id(&self) -> &CapabilityId {
            &self.capability_id
        }

        fn capability_revision(&self) -> &NonZeroU64 {
            &self.capability_revision
        }

        fn ingress(&self) -> &ServiceIngressKindV1 {
            &self.ingress
        }

        fn targets(&self) -> &ServiceAuditTargetsV1 {
            &self.targets
        }

        fn approval_id(&self) -> Option<&ApprovalId> {
            self.approval_id.as_ref()
        }

        fn link(&self) -> &ServiceAuditLinkV1 {
            &self.link
        }
    }

    fn uuid_bytes(fill: u8) -> [u8; 16] {
        let mut bytes = [fill; 16];
        bytes[6] = 0x70 | (fill & 0x0f);
        bytes[8] = 0x80 | (fill & 0x3f);
        bytes
    }

    fn checked_input() -> CheckedAuditInput {
        CheckedAuditInput {
            request_id: RequestId::from_bytes(uuid_bytes(0x11)).expect("request UUIDv7"),
            operation: ServiceOperationV1::GetCommit,
            phase: ServiceAuditPhaseV1::Started,
            principal_id: ActorId::new("maintainer").expect("principal ID"),
            actor_kind: ActorKind::Human,
            capability_id: CapabilityId::from_bytes(uuid_bytes(0x12)).expect("capability UUIDv7"),
            capability_revision: NonZeroU64::new(7).expect("nonzero revision"),
            ingress: ServiceIngressKindV1::Grpc,
            targets: ServiceAuditTargetsV1::empty(),
            approval_id: Some(ApprovalId::new("change-42").expect("approval ID")),
            link: ServiceAuditLinkV1::None,
        }
    }

    fn assert_send_sync<T: Send + Sync + ?Sized>() {}

    #[test]
    fn audit_input_is_an_object_safe_send_sync_borrowed_view() {
        assert_send_sync::<dyn AdministrationAuditInputView>();

        let input = checked_input();
        let view: &dyn AdministrationAuditInputView = &input;

        assert!(std::ptr::eq(view.request_id(), &input.request_id));
        assert!(std::ptr::eq(view.operation(), &input.operation));
        assert!(std::ptr::eq(view.phase(), &input.phase));
        assert!(std::ptr::eq(view.principal_id(), &input.principal_id));
        assert!(std::ptr::eq(view.actor_kind(), &input.actor_kind));
        assert!(std::ptr::eq(view.capability_id(), &input.capability_id));
        assert!(std::ptr::eq(
            view.capability_revision(),
            &input.capability_revision
        ));
        assert!(std::ptr::eq(view.ingress(), &input.ingress));
        assert!(std::ptr::eq(view.targets(), &input.targets));
        assert!(std::ptr::eq(
            view.approval_id().expect("approval remains present"),
            input
                .approval_id
                .as_ref()
                .expect("approval remains present")
        ));
        assert!(std::ptr::eq(view.link(), &input.link));
    }

    #[test]
    fn optional_approval_is_exposed_as_absence_without_a_sentinel() {
        let mut input = checked_input();
        input.approval_id = None;
        let view: &dyn AdministrationAuditInputView = &input;

        assert!(view.approval_id().is_none());
    }

    #[test]
    fn control_plane_link_retains_only_the_prior_authoritative_result_sequence() {
        let mut input = checked_input();
        let prior_result_sequence = AdministrationSequence::first();
        input.link = ServiceAuditLinkV1::ControlPlane {
            administration_sequence: prior_result_sequence,
        };
        let view: &dyn AdministrationAuditInputView = &input;

        assert_eq!(
            view.link(),
            &ServiceAuditLinkV1::ControlPlane {
                administration_sequence: prior_result_sequence,
            }
        );
    }
}
