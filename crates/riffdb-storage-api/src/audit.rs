//! Durable service and control-plane administration audit semantics.

use std::num::NonZeroU64;

use riffdb_types::{
    ActorId, ActorKind, AdministrationSequence, ApprovalId, CapabilityId, RequestId,
    ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceAuditTargetsV1, ServiceIngressKindV1,
    ServiceOperationV1, Timestamp,
};

use crate::{
    EncodedPageItem, MAX_GROUPED_WRITE_TRANSITIONS, MAX_SCAN_PAGE_BYTES, MAX_SERVICE_AUDIT_BYTES,
    StorageError, StorageErrorKind, StorageScanLimit, StorageValueError,
    checked_encoded_page_content,
};

/// Stable authenticated identity retained in an administration audit record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuditPrincipalV1 {
    principal_id: ActorId,
    actor_kind: ActorKind,
    capability_id: CapabilityId,
    capability_revision: NonZeroU64,
}

impl AuditPrincipalV1 {
    /// Constructs a complete authenticated principal reference.
    #[must_use]
    pub const fn new(
        principal_id: ActorId,
        actor_kind: ActorKind,
        capability_id: CapabilityId,
        capability_revision: NonZeroU64,
    ) -> Self {
        Self {
            principal_id,
            actor_kind,
            capability_id,
            capability_revision,
        }
    }

    /// Returns the stable principal identity.
    #[must_use]
    pub const fn principal_id(&self) -> &ActorId {
        &self.principal_id
    }

    /// Returns the trusted actor class.
    #[must_use]
    pub const fn actor_kind(&self) -> ActorKind {
        self.actor_kind
    }

    /// Returns the authorizing capability identity.
    #[must_use]
    pub const fn capability_id(&self) -> CapabilityId {
        self.capability_id
    }

    /// Returns the exact authorized capability revision.
    #[must_use]
    pub const fn capability_revision(&self) -> NonZeroU64 {
        self.capability_revision
    }
}

/// Complete pre-sequence input for one standalone service-audit append.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceAuditAppendIntentV1 {
    request_id: RequestId,
    timestamp: Timestamp,
    operation: ServiceOperationV1,
    phase: ServiceAuditPhaseV1,
    principal: Option<AuditPrincipalV1>,
    ingress: ServiceIngressKindV1,
    targets: ServiceAuditTargetsV1,
    approval_id: Option<ApprovalId>,
    link: ServiceAuditLinkV1,
    semantic_bytes: usize,
}

impl ServiceAuditAppendIntentV1 {
    /// Constructs a bounded record input with no caller-selected sequence.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        request_id: RequestId,
        timestamp: Timestamp,
        operation: ServiceOperationV1,
        phase: ServiceAuditPhaseV1,
        principal: AuditPrincipalV1,
        ingress: ServiceIngressKindV1,
        targets: ServiceAuditTargetsV1,
        approval_id: Option<ApprovalId>,
        link: ServiceAuditLinkV1,
    ) -> Result<Self, StorageValueError> {
        validate_service_audit_phase_link(operation, phase, link)?;
        let semantic_bytes =
            service_audit_semantic_bytes(Some(&principal), &targets, approval_id.as_ref(), link)?;
        if semantic_bytes > MAX_SERVICE_AUDIT_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(Self {
            request_id,
            timestamp,
            operation,
            phase,
            principal: Some(principal),
            ingress,
            targets,
            approval_id,
            link,
            semantic_bytes,
        })
    }

    /// Returns the invocation request identity.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Returns the coordinator-supplied administration timestamp.
    #[must_use]
    pub const fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    /// Returns the closed service operation.
    #[must_use]
    pub const fn operation(&self) -> ServiceOperationV1 {
        self.operation
    }

    /// Returns the closed invocation phase.
    #[must_use]
    pub const fn phase(&self) -> ServiceAuditPhaseV1 {
        self.phase
    }

    /// Returns the authenticated principal reference, absent only for the
    /// checked bootstrap-success exception.
    #[must_use]
    pub const fn principal(&self) -> Option<&AuditPrincipalV1> {
        self.principal.as_ref()
    }

    /// Returns the trusted ingress class.
    #[must_use]
    pub const fn ingress(&self) -> ServiceIngressKindV1 {
        self.ingress
    }

    /// Returns the already checked canonical target collection.
    #[must_use]
    pub const fn targets(&self) -> &ServiceAuditTargetsV1 {
        &self.targets
    }

    /// Returns the optional validated approval identity.
    #[must_use]
    pub const fn approval_id(&self) -> Option<&ApprovalId> {
        self.approval_id.as_ref()
    }

    /// Returns the independent closed authoritative-result link.
    #[must_use]
    pub const fn link(&self) -> ServiceAuditLinkV1 {
        self.link
    }

    /// Returns the checked semantic size.
    #[must_use]
    pub const fn semantic_bytes(&self) -> usize {
        self.semantic_bytes
    }

    /// Constructs the principal-less terminal append for one successful
    /// bootstrap invocation.
    ///
    /// The invocation identity and safe audit fields are copied from the
    /// checked bootstrap start. The caller supplies only the independently
    /// sampled terminal timestamp and the authoritative bootstrap transition
    /// proven by the compound transition result.
    pub fn for_bootstrap_succeeded(
        start: &BootstrapServiceAuditStartV1,
        timestamp: Timestamp,
        transition_sequence: AdministrationSequence,
    ) -> Result<Self, StorageValueError> {
        let link = ServiceAuditLinkV1::ControlPlane {
            administration_sequence: transition_sequence,
        };
        let semantic_bytes =
            service_audit_semantic_bytes(None, &start.targets, start.approval_id.as_ref(), link)?;
        if semantic_bytes > MAX_SERVICE_AUDIT_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(Self {
            request_id: start.request_id,
            timestamp,
            operation: ServiceOperationV1::CreateCapability,
            phase: ServiceAuditPhaseV1::Succeeded,
            principal: None,
            ingress: start.ingress,
            targets: start.targets.clone(),
            approval_id: start.approval_id.clone(),
            link,
            semantic_bytes,
        })
    }
}

/// Principal-less start input accepted only by the compound bootstrap transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BootstrapServiceAuditStartV1 {
    request_id: RequestId,
    timestamp: Timestamp,
    ingress: ServiceIngressKindV1,
    targets: ServiceAuditTargetsV1,
    approval_id: Option<ApprovalId>,
}

impl BootstrapServiceAuditStartV1 {
    /// Constructs the fixed bootstrap `CreateCapability/Started` input.
    pub fn new(
        request_id: RequestId,
        timestamp: Timestamp,
        ingress: ServiceIngressKindV1,
        targets: ServiceAuditTargetsV1,
        approval_id: Option<ApprovalId>,
    ) -> Result<Self, StorageValueError> {
        if matches!(ingress, ServiceIngressKindV1::McpHttp) {
            return Err(StorageValueError::InvalidShape);
        }
        let fixed = service_audit_semantic_bytes(
            None,
            &targets,
            approval_id.as_ref(),
            ServiceAuditLinkV1::ControlPlane {
                administration_sequence: AdministrationSequence::first(),
            },
        )?;
        if fixed > MAX_SERVICE_AUDIT_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(Self {
            request_id,
            timestamp,
            ingress,
            targets,
            approval_id,
        })
    }

    /// Returns the bootstrap invocation request ID.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Returns the one timestamp shared with new-bootstrap administration.
    #[must_use]
    pub const fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    /// Returns the trusted bootstrap ingress.
    #[must_use]
    pub const fn ingress(&self) -> ServiceIngressKindV1 {
        self.ingress
    }

    /// Returns the exact checked target collection.
    #[must_use]
    pub const fn targets(&self) -> &ServiceAuditTargetsV1 {
        &self.targets
    }

    /// Returns the optional validated approval identity.
    #[must_use]
    pub const fn approval_id(&self) -> Option<&ApprovalId> {
        self.approval_id.as_ref()
    }
}

/// Semantic durable standalone or bootstrap service-audit record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredServiceAuditRecordV1 {
    administration_sequence: AdministrationSequence,
    request_id: RequestId,
    timestamp: Timestamp,
    operation: ServiceOperationV1,
    phase: ServiceAuditPhaseV1,
    principal: Option<AuditPrincipalV1>,
    ingress: ServiceIngressKindV1,
    targets: ServiceAuditTargetsV1,
    approval_id: Option<ApprovalId>,
    link: ServiceAuditLinkV1,
}

impl StoredServiceAuditRecordV1 {
    /// Reconstructs and validates one exact durable service-audit record.
    ///
    /// Principal-less records are accepted only for the closed bootstrap
    /// lifecycle. Cross-record target and invocation reciprocity remains the
    /// responsibility of the structural startup pass.
    #[allow(clippy::too_many_arguments)]
    pub fn from_stored_parts(
        administration_sequence: AdministrationSequence,
        request_id: RequestId,
        timestamp: Timestamp,
        operation: ServiceOperationV1,
        phase: ServiceAuditPhaseV1,
        principal: Option<AuditPrincipalV1>,
        ingress: ServiceIngressKindV1,
        targets: ServiceAuditTargetsV1,
        approval_id: Option<ApprovalId>,
        link: ServiceAuditLinkV1,
    ) -> Result<Self, StorageValueError> {
        validate_stored_service_audit_shape(
            administration_sequence,
            operation,
            phase,
            principal.as_ref(),
            ingress,
            link,
        )?;
        let semantic_bytes =
            service_audit_semantic_bytes(principal.as_ref(), &targets, approval_id.as_ref(), link)?;
        if semantic_bytes > MAX_SERVICE_AUDIT_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(Self {
            administration_sequence,
            request_id,
            timestamp,
            operation,
            phase,
            principal,
            ingress,
            targets,
            approval_id,
            link,
        })
    }

    /// Lowers a normal checked append intent after sequence assignment.
    #[must_use]
    pub fn from_intent(
        administration_sequence: AdministrationSequence,
        intent: &ServiceAuditAppendIntentV1,
    ) -> Self {
        Self {
            administration_sequence,
            request_id: intent.request_id,
            timestamp: intent.timestamp,
            operation: intent.operation,
            phase: intent.phase,
            principal: intent.principal.clone(),
            ingress: intent.ingress,
            targets: intent.targets.clone(),
            approval_id: intent.approval_id.clone(),
            link: intent.link,
        }
    }

    /// Lowers the principal-less start inside the typed bootstrap transition.
    ///
    /// The start must immediately precede the linked transition in the shared
    /// administration sequence without wrapping.
    pub fn from_bootstrap_start(
        administration_sequence: AdministrationSequence,
        start: &BootstrapServiceAuditStartV1,
        transition_sequence: AdministrationSequence,
    ) -> Result<Self, StorageValueError> {
        if administration_sequence.checked_next() != Some(transition_sequence) {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(Self {
            administration_sequence,
            request_id: start.request_id,
            timestamp: start.timestamp,
            operation: ServiceOperationV1::CreateCapability,
            phase: ServiceAuditPhaseV1::Started,
            principal: None,
            ingress: start.ingress,
            targets: start.targets.clone(),
            approval_id: start.approval_id.clone(),
            link: ServiceAuditLinkV1::ControlPlane {
                administration_sequence: transition_sequence,
            },
        })
    }

    /// Lowers a principal-less exact-replay start linked to the original
    /// bootstrap transition.
    ///
    /// A replay start is newly sequenced after the retained authoritative
    /// transition. Unlike a new bootstrap, adjacency is not required and no
    /// capability record is rewritten.
    pub fn from_bootstrap_replay_start(
        administration_sequence: AdministrationSequence,
        start: &BootstrapServiceAuditStartV1,
        original_transition_sequence: AdministrationSequence,
    ) -> Result<Self, StorageValueError> {
        if original_transition_sequence >= administration_sequence {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(Self {
            administration_sequence,
            request_id: start.request_id,
            timestamp: start.timestamp,
            operation: ServiceOperationV1::CreateCapability,
            phase: ServiceAuditPhaseV1::Started,
            principal: None,
            ingress: start.ingress,
            targets: start.targets.clone(),
            approval_id: start.approval_id.clone(),
            link: ServiceAuditLinkV1::ControlPlane {
                administration_sequence: original_transition_sequence,
            },
        })
    }

    /// Returns the assigned shared administration order.
    #[must_use]
    pub const fn administration_sequence(&self) -> AdministrationSequence {
        self.administration_sequence
    }

    /// Returns the invocation request identity.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    /// Returns the coordinator-observed timestamp.
    #[must_use]
    pub const fn timestamp(&self) -> Timestamp {
        self.timestamp
    }

    /// Returns the closed operation.
    #[must_use]
    pub const fn operation(&self) -> ServiceOperationV1 {
        self.operation
    }

    /// Returns the closed lifecycle phase.
    #[must_use]
    pub const fn phase(&self) -> ServiceAuditPhaseV1 {
        self.phase
    }

    /// Returns the authenticated principal, absent only for checked bootstrap.
    #[must_use]
    pub const fn principal(&self) -> Option<&AuditPrincipalV1> {
        self.principal.as_ref()
    }

    /// Returns trusted ingress.
    #[must_use]
    pub const fn ingress(&self) -> ServiceIngressKindV1 {
        self.ingress
    }

    /// Returns the canonical target collection.
    #[must_use]
    pub const fn targets(&self) -> &ServiceAuditTargetsV1 {
        &self.targets
    }

    /// Returns the optional validated approval.
    #[must_use]
    pub const fn approval_id(&self) -> Option<&ApprovalId> {
        self.approval_id.as_ref()
    }

    /// Returns the independent authoritative-result link.
    #[must_use]
    pub const fn link(&self) -> ServiceAuditLinkV1 {
        self.link
    }
}

/// Closed result of the specialized atomic standalone append.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ServiceAuditAppendResult {
    /// The exact record and allocator advance committed.
    Appended(StoredServiceAuditRecordV1),
    /// The request already has a start, standalone terminal, or terminal phase
    /// inconsistent with this attempted transition; no sequence was allocated.
    PhaseConflict,
}

/// One item in the shared administration sequence space.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StoredAdministrationAuditRecordV1 {
    /// Contract catalog activation.
    Catalog(crate::StoredCatalogAdministrationV1),
    /// Immutable query-module activation.
    QueryModule(crate::StoredQueryModuleAdministrationV1),
    /// Immutable reactive-module publication.
    ReactiveModule(crate::StoredReactiveModuleAdministrationV1),
    /// Capability bootstrap, creation, or revocation.
    Capability(crate::StoredCapabilityAdministrationV1),
    /// Application-service invocation phase.
    Service(StoredServiceAuditRecordV1),
    /// Offline retention administration action (projection detach/reattach).
    Retention(crate::StoredRetentionAdministrationV1),
}

impl StoredAdministrationAuditRecordV1 {
    /// Returns the shared nonzero ordering sequence.
    #[must_use]
    pub const fn administration_sequence(&self) -> AdministrationSequence {
        match self {
            Self::Catalog(record) => record.administration_sequence(),
            Self::QueryModule(record) => record.administration_sequence(),
            Self::ReactiveModule(record) => record.administration_sequence(),
            Self::Capability(record) => record.administration_sequence(),
            Self::Service(record) => record.administration_sequence(),
            Self::Retention(record) => record.administration_sequence(),
        }
    }
}

/// One bounded shared-administration scan after an optional sequence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdministrationAuditScanRequest {
    after: Option<AdministrationSequence>,
    limit: StorageScanLimit,
}

impl AdministrationAuditScanRequest {
    /// Constructs an exclusive sequence scan with a checked nonzero row limit.
    #[must_use]
    pub const fn new(after: Option<AdministrationSequence>, limit: StorageScanLimit) -> Self {
        Self { after, limit }
    }

    /// Returns the exclusive prior sequence, or `None` before sequence one.
    #[must_use]
    pub const fn after(self) -> Option<AdministrationSequence> {
        self.after
    }

    /// Returns the checked requested row count.
    #[must_use]
    pub const fn limit(self) -> StorageScanLimit {
        self.limit
    }
}

/// Exact-end bounded result of scanning the shared administration stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdministrationAuditScan {
    /// A bounded page with more records after its continuation.
    Page {
        /// Strictly increasing records.
        records: Vec<EncodedPageItem<StoredAdministrationAuditRecordV1>>,
        /// Exclusive sequence lower bound for the next page.
        next_after: AdministrationSequence,
    },
    /// The supplied page reaches the exact end, including an empty page.
    ExactEnd {
        /// Remaining strictly increasing records.
        records: Vec<EncodedPageItem<StoredAdministrationAuditRecordV1>>,
    },
}

impl AdministrationAuditScan {
    /// Validates request count, contiguity, and exact envelope charges.
    pub fn page(
        request: AdministrationAuditScanRequest,
        records: Vec<EncodedPageItem<StoredAdministrationAuditRecordV1>>,
        has_more: bool,
    ) -> Result<Self, StorageValueError> {
        if records.len() > usize::from(request.limit.get()) {
            return Err(StorageValueError::LimitExceeded);
        }
        checked_encoded_page_content(&records, MAX_SCAN_PAGE_BYTES)?;
        let mut expected = match request.after {
            None => Some(AdministrationSequence::first()),
            Some(sequence) => sequence.checked_next(),
        };
        for record in &records {
            let sequence = record.value().administration_sequence();
            if expected != Some(sequence) {
                return Err(StorageValueError::NonCanonicalOrder);
            }
            expected = sequence.checked_next();
        }
        if has_more {
            let next_after = records
                .last()
                .ok_or(StorageValueError::Empty)?
                .value()
                .administration_sequence();
            Ok(Self::Page {
                records,
                next_after,
            })
        } else {
            Ok(Self::ExactEnd { records })
        }
    }
}

/// Coordinator-only service-audit lifecycle append port.
///
/// A conforming repository enforces invocation history, not merely record shape.
/// An authenticated `Started` must be the invocation's first record. A pre-start
/// standalone terminal is limited to `Denied`, `Cancelled`, or `Failed` with no
/// authoritative link. Once `Started` exists, at most one later non-`Started`
/// terminal may be appended, and its request, operation, principal, ingress,
/// targets, and approval must exactly match that start. A principal-less
/// `Succeeded` is accepted only when it matches a principal-less bootstrap start
/// previously written by the compound bootstrap transition, including the same
/// control-plane link. Duplicate starts, duplicate terminals, terminal-before-
/// start success/uncertainty, and mismatched lifecycle fields fail closed without
/// advancing the administration allocator.
pub trait ServiceAuditAppendRepository {
    /// Appends exactly one checked service-audit record and advances the shared
    /// administration allocator atomically.
    fn append_service_audit(
        &mut self,
        intent: &ServiceAuditAppendIntentV1,
    ) -> Result<ServiceAuditAppendResult, StorageError>;

    /// Appends a bounded FIFO group in one physical durable transition.
    ///
    /// Results retain input order and independent lifecycle classification.
    /// Production repositories override this method atomically. The default is
    /// a conformance adapter for small test repositories and rejects groups
    /// larger than the production scheduler ceiling.
    fn append_service_audit_group(
        &mut self,
        intents: &[ServiceAuditAppendIntentV1],
    ) -> Result<Vec<ServiceAuditAppendResult>, StorageError> {
        if intents.is_empty() || intents.len() > MAX_GROUPED_WRITE_TRANSITIONS {
            return Err(StorageError::new(StorageErrorKind::LimitExceeded, None));
        }
        intents
            .iter()
            .map(|intent| self.append_service_audit(intent))
            .collect()
    }

    /// Appends one Started+terminal pair in a single durable transition.
    ///
    /// Default conformance shells may return unsupported. Production redb uses
    /// the fused in-write staging path so the pair is atomic (pair-or-absent).
    fn append_service_audit_fused_pair(
        &mut self,
        started: &ServiceAuditAppendIntentV1,
        terminal: &ServiceAuditAppendIntentV1,
    ) -> Result<(), StorageError> {
        let _ = (started, terminal);
        // Call-scoped unsupported: LimitExceeded does not fence or stop the
        // coordinator (mapped to PhaseConflict on the fused path). InvariantViolation
        // would stop the coordinator.
        Err(StorageError::new(StorageErrorKind::LimitExceeded, None))
    }
}

/// Least-authority ordered administration-audit read port.
pub trait AdministrationAuditReader {
    /// Scans one contiguous page after an optional exclusive sequence lower bound.
    fn scan_administration_audit(
        &self,
        request: AdministrationAuditScanRequest,
    ) -> Result<AdministrationAuditScan, StorageError>;
}

fn service_audit_semantic_bytes(
    principal: Option<&AuditPrincipalV1>,
    targets: &ServiceAuditTargetsV1,
    approval_id: Option<&ApprovalId>,
    link: ServiceAuditLinkV1,
) -> Result<usize, StorageValueError> {
    checked_semantic_sum([
        8,  // administration sequence
        16, // request ID
        12, // canonical timestamp
        1,  // operation tag
        1,  // phase tag
        1,  // principal presence
        principal.map_or(0, principal_semantic_bytes),
        1, // ingress tag
        4, // target count
        target_key_bytes(targets)?,
        1, // approval presence
        approval_id.map_or(0, |approval| approval.as_bytes().len()),
        service_link_semantic_bytes(link),
    ])
}

fn principal_semantic_bytes(principal: &AuditPrincipalV1) -> usize {
    4 + principal.principal_id.as_str().len() + 1 + 16 + 8
}

const fn service_link_semantic_bytes(link: ServiceAuditLinkV1) -> usize {
    match link {
        ServiceAuditLinkV1::None => 1,
        ServiceAuditLinkV1::Command { .. } => 1 + 8 + 16,
        ServiceAuditLinkV1::ControlPlane { .. } => 1 + 8,
    }
}

fn validate_service_audit_phase_link(
    operation: ServiceOperationV1,
    phase: ServiceAuditPhaseV1,
    link: ServiceAuditLinkV1,
) -> Result<(), StorageValueError> {
    let valid = match phase {
        ServiceAuditPhaseV1::Started
        | ServiceAuditPhaseV1::Denied
        | ServiceAuditPhaseV1::Cancelled
        | ServiceAuditPhaseV1::Failed
        | ServiceAuditPhaseV1::OutcomeUncertain => matches!(link, ServiceAuditLinkV1::None),
        ServiceAuditPhaseV1::Succeeded => match link {
            ServiceAuditLinkV1::None => !matches!(
                operation,
                ServiceOperationV1::ExecuteCommand
                    | ServiceOperationV1::ResolveCommandOutcome
                    | ServiceOperationV1::DeployContract
                    | ServiceOperationV1::DeployQueryModule
                    | ServiceOperationV1::ApplyContractMigration
                    | ServiceOperationV1::CreateCapability
                    | ServiceOperationV1::RevokeCapability
            ),
            ServiceAuditLinkV1::Command { .. } => matches!(
                operation,
                ServiceOperationV1::ExecuteCommand | ServiceOperationV1::ResolveCommandOutcome
            ),
            ServiceAuditLinkV1::ControlPlane { .. } => matches!(
                operation,
                ServiceOperationV1::DeployContract
                    | ServiceOperationV1::DeployQueryModule
                    | ServiceOperationV1::ApplyContractMigration
                    | ServiceOperationV1::CreateCapability
                    | ServiceOperationV1::RevokeCapability
            ),
        },
    };
    if !valid {
        return Err(StorageValueError::InvalidShape);
    }
    Ok(())
}

fn validate_stored_service_audit_shape(
    administration_sequence: AdministrationSequence,
    operation: ServiceOperationV1,
    phase: ServiceAuditPhaseV1,
    principal: Option<&AuditPrincipalV1>,
    ingress: ServiceIngressKindV1,
    link: ServiceAuditLinkV1,
) -> Result<(), StorageValueError> {
    if principal.is_some() {
        return validate_service_audit_phase_link(operation, phase, link);
    }

    let ServiceAuditLinkV1::ControlPlane {
        administration_sequence: transition_sequence,
    } = link
    else {
        return Err(StorageValueError::InvalidShape);
    };
    if operation != ServiceOperationV1::CreateCapability || ingress == ServiceIngressKindV1::McpHttp
    {
        return Err(StorageValueError::InvalidShape);
    }

    let valid_sequence = match phase {
        ServiceAuditPhaseV1::Started => {
            administration_sequence.checked_next() == Some(transition_sequence)
                || transition_sequence < administration_sequence
        }
        ServiceAuditPhaseV1::Succeeded => transition_sequence < administration_sequence,
        ServiceAuditPhaseV1::Denied
        | ServiceAuditPhaseV1::Cancelled
        | ServiceAuditPhaseV1::Failed
        | ServiceAuditPhaseV1::OutcomeUncertain => {
            return Err(StorageValueError::InvalidShape);
        }
    };
    if !valid_sequence {
        return Err(StorageValueError::IdentityMismatch);
    }
    Ok(())
}

fn target_key_bytes(targets: &ServiceAuditTargetsV1) -> Result<usize, StorageValueError> {
    targets.as_slice().iter().try_fold(0usize, |total, target| {
        total
            .checked_add(target.canonical_key().len())
            .ok_or(StorageValueError::SizeOverflow)
    })
}

fn checked_semantic_sum(
    parts: impl IntoIterator<Item = usize>,
) -> Result<usize, StorageValueError> {
    parts.into_iter().try_fold(0usize, |total, part| {
        total
            .checked_add(part)
            .ok_or(StorageValueError::SizeOverflow)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EncodedContentCharge, MAX_SCAN_PAGE_ENTRIES};

    fn audit_principal() -> AuditPrincipalV1 {
        AuditPrincipalV1::new(
            ActorId::new("maintainer").expect("actor"),
            ActorKind::Human,
            CapabilityId::from_bytes([
                0x01, 0x8f, 0x00, 0x00, 0x00, 0x00, 0x70, 0x01, 0x80, 0x02, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x05,
            ])
            .expect("valid UUIDv7"),
            NonZeroU64::MIN,
        )
    }

    fn bootstrap_start() -> BootstrapServiceAuditStartV1 {
        BootstrapServiceAuditStartV1::new(
            RequestId::from_bytes([
                0x01, 0x8f, 0x00, 0x00, 0x00, 0x00, 0x70, 0x01, 0x80, 0x02, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x04,
            ])
            .expect("valid UUIDv7"),
            Timestamp::new(1, 0).expect("timestamp"),
            ServiceIngressKindV1::InProcessTestComparison,
            ServiceAuditTargetsV1::new([]).expect("empty targets are canonical"),
            None,
        )
        .expect("bounded bootstrap start")
    }

    #[test]
    fn bootstrap_start_accepts_only_loopback_grpc_and_test_comparison_ingress() {
        let template = bootstrap_start();
        assert!(
            BootstrapServiceAuditStartV1::new(
                template.request_id(),
                template.timestamp(),
                ServiceIngressKindV1::Grpc,
                template.targets().clone(),
                template.approval_id().cloned(),
            )
            .is_ok()
        );
        assert!(
            BootstrapServiceAuditStartV1::new(
                template.request_id(),
                template.timestamp(),
                ServiceIngressKindV1::InProcessTestComparison,
                template.targets().clone(),
                template.approval_id().cloned(),
            )
            .is_ok()
        );
        assert_eq!(
            BootstrapServiceAuditStartV1::new(
                template.request_id(),
                template.timestamp(),
                ServiceIngressKindV1::McpHttp,
                template.targets().clone(),
                template.approval_id().cloned(),
            ),
            Err(StorageValueError::InvalidShape)
        );
    }

    fn reconstruct_service_record(
        sequence: AdministrationSequence,
        operation: ServiceOperationV1,
        phase: ServiceAuditPhaseV1,
        principal: Option<AuditPrincipalV1>,
        ingress: ServiceIngressKindV1,
        link: ServiceAuditLinkV1,
    ) -> Result<StoredServiceAuditRecordV1, StorageValueError> {
        let template = bootstrap_start();
        StoredServiceAuditRecordV1::from_stored_parts(
            sequence,
            template.request_id(),
            template.timestamp(),
            operation,
            phase,
            principal,
            ingress,
            template.targets().clone(),
            template.approval_id().cloned(),
            link,
        )
    }

    #[test]
    fn normal_service_audit_record_round_trips_through_stored_parts() {
        let intent = ServiceAuditAppendIntentV1::new(
            bootstrap_start().request_id(),
            Timestamp::new(7, 8).expect("timestamp"),
            ServiceOperationV1::GetEntity,
            ServiceAuditPhaseV1::Started,
            audit_principal(),
            ServiceIngressKindV1::Grpc,
            ServiceAuditTargetsV1::empty(),
            None,
            ServiceAuditLinkV1::None,
        )
        .expect("normal audit intent");
        let stored =
            StoredServiceAuditRecordV1::from_intent(AdministrationSequence::first(), &intent);
        let reconstructed = StoredServiceAuditRecordV1::from_stored_parts(
            stored.administration_sequence(),
            stored.request_id(),
            stored.timestamp(),
            stored.operation(),
            stored.phase(),
            stored.principal().cloned(),
            stored.ingress(),
            stored.targets().clone(),
            stored.approval_id().cloned(),
            stored.link(),
        )
        .expect("stored parts remain valid");

        assert_eq!(reconstructed, stored);
    }

    #[test]
    fn stored_parts_accept_only_the_exact_principal_less_bootstrap_shapes() {
        let one = AdministrationSequence::first();
        let two = one.checked_next().expect("sequence two");
        let three = two.checked_next().expect("sequence three");
        let control = |sequence| ServiceAuditLinkV1::ControlPlane {
            administration_sequence: sequence,
        };

        assert!(
            reconstruct_service_record(
                one,
                ServiceOperationV1::CreateCapability,
                ServiceAuditPhaseV1::Started,
                None,
                ServiceIngressKindV1::Grpc,
                control(two),
            )
            .is_ok(),
            "new bootstrap start immediately precedes its transition"
        );
        assert!(
            reconstruct_service_record(
                three,
                ServiceOperationV1::CreateCapability,
                ServiceAuditPhaseV1::Started,
                None,
                ServiceIngressKindV1::InProcessTestComparison,
                control(two),
            )
            .is_ok(),
            "replay start links an earlier transition"
        );
        assert!(
            reconstruct_service_record(
                three,
                ServiceOperationV1::CreateCapability,
                ServiceAuditPhaseV1::Succeeded,
                None,
                ServiceIngressKindV1::Grpc,
                control(two),
            )
            .is_ok(),
            "bootstrap success links an earlier transition"
        );

        let invalid_sequence_links = [
            reconstruct_service_record(
                one,
                ServiceOperationV1::CreateCapability,
                ServiceAuditPhaseV1::Started,
                None,
                ServiceIngressKindV1::Grpc,
                control(three),
            ),
            reconstruct_service_record(
                two,
                ServiceOperationV1::CreateCapability,
                ServiceAuditPhaseV1::Started,
                None,
                ServiceIngressKindV1::Grpc,
                control(two),
            ),
            reconstruct_service_record(
                one,
                ServiceOperationV1::CreateCapability,
                ServiceAuditPhaseV1::Succeeded,
                None,
                ServiceIngressKindV1::Grpc,
                control(two),
            ),
            reconstruct_service_record(
                two,
                ServiceOperationV1::CreateCapability,
                ServiceAuditPhaseV1::Succeeded,
                None,
                ServiceIngressKindV1::Grpc,
                control(two),
            ),
        ];
        assert!(
            invalid_sequence_links
                .into_iter()
                .all(|result| result == Err(StorageValueError::IdentityMismatch))
        );

        let invalid_shapes = [
            reconstruct_service_record(
                three,
                ServiceOperationV1::CreateCapability,
                ServiceAuditPhaseV1::Failed,
                None,
                ServiceIngressKindV1::Grpc,
                control(two),
            ),
            reconstruct_service_record(
                three,
                ServiceOperationV1::CreateCapability,
                ServiceAuditPhaseV1::Succeeded,
                None,
                ServiceIngressKindV1::McpHttp,
                control(two),
            ),
            reconstruct_service_record(
                three,
                ServiceOperationV1::DeployContract,
                ServiceAuditPhaseV1::Succeeded,
                None,
                ServiceIngressKindV1::Grpc,
                control(two),
            ),
            reconstruct_service_record(
                three,
                ServiceOperationV1::CreateCapability,
                ServiceAuditPhaseV1::Succeeded,
                None,
                ServiceIngressKindV1::Grpc,
                ServiceAuditLinkV1::None,
            ),
        ];
        assert!(
            invalid_shapes
                .into_iter()
                .all(|result| result == Err(StorageValueError::InvalidShape))
        );
    }

    fn audit_record(
        sequence: u64,
        encoded_bytes: usize,
    ) -> EncodedPageItem<StoredAdministrationAuditRecordV1> {
        let sequence = AdministrationSequence::new(sequence).expect("nonzero sequence");
        let transition = sequence
            .checked_next()
            .expect("test sequence has successor");
        let record = StoredServiceAuditRecordV1::from_bootstrap_start(
            sequence,
            &bootstrap_start(),
            transition,
        )
        .expect("adjacent bootstrap records");
        EncodedPageItem::new(
            StoredAdministrationAuditRecordV1::Service(record),
            EncodedContentCharge::new(encoded_bytes).expect("valid envelope charge"),
        )
    }

    fn scan_request(after: Option<u64>, limit: u16) -> AdministrationAuditScanRequest {
        AdministrationAuditScanRequest::new(
            after.map(|value| AdministrationSequence::new(value).expect("nonzero sequence")),
            StorageScanLimit::new(limit).expect("bounded scan limit"),
        )
    }

    #[test]
    fn exact_end_can_represent_an_empty_audit_stream() {
        let request = scan_request(None, 1);
        assert_eq!(
            AdministrationAuditScan::page(request, Vec::new(), false).expect("empty exact end"),
            AdministrationAuditScan::ExactEnd {
                records: Vec::new()
            }
        );
        assert_eq!(
            AdministrationAuditScan::page(request, Vec::new(), true),
            Err(StorageValueError::Empty)
        );
    }

    #[test]
    fn bootstrap_start_requires_the_immediate_transition_successor() {
        let start = bootstrap_start();
        let one = AdministrationSequence::first();
        let two = one.checked_next().expect("sequence two");
        let three = two.checked_next().expect("sequence three");
        assert!(StoredServiceAuditRecordV1::from_bootstrap_start(one, &start, two).is_ok());
        assert_eq!(
            StoredServiceAuditRecordV1::from_bootstrap_start(one, &start, three),
            Err(StorageValueError::IdentityMismatch)
        );
        assert_eq!(
            StoredServiceAuditRecordV1::from_bootstrap_start(two, &start, one),
            Err(StorageValueError::IdentityMismatch)
        );
        let maximum = AdministrationSequence::new(u64::MAX).expect("maximum sequence");
        assert_eq!(
            StoredServiceAuditRecordV1::from_bootstrap_start(maximum, &start, maximum),
            Err(StorageValueError::IdentityMismatch)
        );
    }

    #[test]
    fn bootstrap_replay_start_links_only_a_prior_authoritative_transition() {
        let start = bootstrap_start();
        let one = AdministrationSequence::first();
        let two = one.checked_next().expect("sequence two");
        let three = two.checked_next().expect("sequence three");
        let four = three.checked_next().expect("sequence four");

        let adjacent = StoredServiceAuditRecordV1::from_bootstrap_replay_start(three, &start, two)
            .expect("a crash can leave replay adjacent to the original transition");
        assert_eq!(adjacent.administration_sequence(), three);
        assert_eq!(adjacent.principal(), None);
        assert_eq!(
            adjacent.link(),
            ServiceAuditLinkV1::ControlPlane {
                administration_sequence: two
            }
        );
        assert!(StoredServiceAuditRecordV1::from_bootstrap_replay_start(four, &start, two).is_ok());
        assert_eq!(
            StoredServiceAuditRecordV1::from_bootstrap_replay_start(two, &start, two),
            Err(StorageValueError::IdentityMismatch)
        );
        assert_eq!(
            StoredServiceAuditRecordV1::from_bootstrap_replay_start(two, &start, three),
            Err(StorageValueError::IdentityMismatch)
        );
    }

    #[test]
    fn bootstrap_success_is_the_only_principal_less_standalone_intent() {
        let start = bootstrap_start();
        let transition = AdministrationSequence::new(2).expect("sequence two");
        let terminal_timestamp = Timestamp::new(3, 4).expect("terminal timestamp");
        let intent = ServiceAuditAppendIntentV1::for_bootstrap_succeeded(
            &start,
            terminal_timestamp,
            transition,
        )
        .expect("checked bootstrap success");

        assert_eq!(intent.request_id(), start.request_id());
        assert_eq!(intent.timestamp(), terminal_timestamp);
        assert_eq!(intent.operation(), ServiceOperationV1::CreateCapability);
        assert_eq!(intent.phase(), ServiceAuditPhaseV1::Succeeded);
        assert_eq!(intent.principal(), None);
        assert_eq!(intent.ingress(), start.ingress());
        assert_eq!(intent.targets(), start.targets());
        assert_eq!(intent.approval_id(), start.approval_id());
        assert_eq!(
            intent.link(),
            ServiceAuditLinkV1::ControlPlane {
                administration_sequence: transition
            }
        );

        let record = StoredServiceAuditRecordV1::from_intent(
            AdministrationSequence::new(3).expect("sequence three"),
            &intent,
        );
        assert_eq!(record.principal(), None);
        assert_eq!(record.phase(), ServiceAuditPhaseV1::Succeeded);
    }

    #[test]
    fn administration_audit_pages_require_the_first_successor_and_full_contiguity() {
        let request = scan_request(Some(1), 3);
        assert!(matches!(
            AdministrationAuditScan::page(
                request,
                vec![audit_record(2, 1), audit_record(3, 1)],
                false,
            ),
            Ok(AdministrationAuditScan::ExactEnd { .. })
        ));
        assert_eq!(
            AdministrationAuditScan::page(request, vec![audit_record(3, 1)], false),
            Err(StorageValueError::NonCanonicalOrder)
        );
        assert_eq!(
            AdministrationAuditScan::page(
                request,
                vec![audit_record(2, 1), audit_record(4, 1)],
                false,
            ),
            Err(StorageValueError::NonCanonicalOrder)
        );
        assert_eq!(
            AdministrationAuditScan::page(
                request,
                vec![audit_record(2, 1), audit_record(1, 1)],
                false,
            ),
            Err(StorageValueError::NonCanonicalOrder)
        );
    }

    #[test]
    fn administration_audit_pages_enforce_request_and_envelope_boundaries() {
        assert!(StorageScanLimit::new(0).is_none());
        assert!(StorageScanLimit::new(MAX_SCAN_PAGE_ENTRIES as u16).is_some());
        assert!(StorageScanLimit::new((MAX_SCAN_PAGE_ENTRIES + 1) as u16).is_none());

        assert_eq!(
            AdministrationAuditScan::page(
                scan_request(None, 1),
                vec![audit_record(1, 1), audit_record(2, 1)],
                false,
            ),
            Err(StorageValueError::LimitExceeded)
        );

        let maximum_count = (1..=MAX_SCAN_PAGE_ENTRIES)
            .map(|sequence| audit_record(sequence as u64, 1))
            .collect();
        assert!(
            AdministrationAuditScan::page(
                scan_request(None, MAX_SCAN_PAGE_ENTRIES as u16),
                maximum_count,
                false,
            )
            .is_ok()
        );

        assert!(
            AdministrationAuditScan::page(
                scan_request(None, 1),
                vec![audit_record(1, MAX_SCAN_PAGE_BYTES)],
                false,
            )
            .is_ok()
        );
        assert_eq!(
            AdministrationAuditScan::page(
                scan_request(None, 1),
                vec![audit_record(1, MAX_SCAN_PAGE_BYTES + 1)],
                false,
            ),
            Err(StorageValueError::LimitExceeded)
        );
    }

    #[test]
    fn service_audit_phase_and_link_matrix_is_exhaustive() {
        let command = ServiceAuditLinkV1::Command {
            commit_sequence: riffdb_types::CommitSequence::first(),
            provenance_id: riffdb_types::ProvenanceId::from_bytes([
                0x01, 0x8f, 0x00, 0x00, 0x00, 0x00, 0x70, 0x01, 0x80, 0x02, 0x00, 0x00, 0x00, 0x00,
                0x00, 0x03,
            ])
            .expect("valid UUIDv7"),
        };
        let control = ServiceAuditLinkV1::ControlPlane {
            administration_sequence: AdministrationSequence::first(),
        };

        for operation in ServiceOperationV1::ALL {
            for phase in ServiceAuditPhaseV1::ALL {
                for link in [ServiceAuditLinkV1::None, command, control] {
                    let expected = match phase {
                        ServiceAuditPhaseV1::Started
                        | ServiceAuditPhaseV1::Denied
                        | ServiceAuditPhaseV1::Cancelled
                        | ServiceAuditPhaseV1::Failed
                        | ServiceAuditPhaseV1::OutcomeUncertain => {
                            matches!(link, ServiceAuditLinkV1::None)
                        }
                        ServiceAuditPhaseV1::Succeeded => match link {
                            ServiceAuditLinkV1::None => !matches!(
                                operation,
                                ServiceOperationV1::ExecuteCommand
                                    | ServiceOperationV1::ResolveCommandOutcome
                                    | ServiceOperationV1::DeployContract
                                    | ServiceOperationV1::DeployQueryModule
                                    | ServiceOperationV1::ApplyContractMigration
                                    | ServiceOperationV1::CreateCapability
                                    | ServiceOperationV1::RevokeCapability
                            ),
                            ServiceAuditLinkV1::Command { .. } => matches!(
                                operation,
                                ServiceOperationV1::ExecuteCommand
                                    | ServiceOperationV1::ResolveCommandOutcome
                            ),
                            ServiceAuditLinkV1::ControlPlane { .. } => matches!(
                                operation,
                                ServiceOperationV1::DeployContract
                                    | ServiceOperationV1::DeployQueryModule
                                    | ServiceOperationV1::ApplyContractMigration
                                    | ServiceOperationV1::CreateCapability
                                    | ServiceOperationV1::RevokeCapability
                            ),
                        },
                    };
                    assert_eq!(
                        validate_service_audit_phase_link(operation, phase, link).is_ok(),
                        expected,
                        "unexpected matrix result for {operation:?}/{phase:?}/{link:?}"
                    );
                }
            }
        }
    }
}
