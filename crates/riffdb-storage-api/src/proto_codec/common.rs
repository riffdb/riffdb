use std::num::NonZeroU64;

use riffdb_proto::storage::v1 as wire;
use riffdb_types::{
    ActorId, ActorKind, AdmittedActorContext, AgentSessionId, CanonicalRecord, CapabilityId,
    CommandId, CommitSequence, ContractBundleHash, ContractLineage, ContractVersion, DigestKeyId,
    EntityKey, EntityTypeId, EntityVersion, EventId, FieldId, IndexEpoch, IndexId, LogicalTime,
    OutcomeId, PlanHash, TenantId, TenantScope, Timestamp,
};

use crate::records::VerifiedCanonicalRecordV1;
use crate::{
    AuditPrincipalV1, DeclaredOutcome, DurableKeySchemaBindingV1, EntityTarget, ExecutablePlanRef,
    ExpectedEntityState, IdempotencyIdentity, IdempotencyKeyDigest, IndexEpochPosition,
    StorageValueError, StoredAdmittedProvenanceClaimsV1, StructurallyDecodedIndexRangePrefixV1,
};

use super::{DurableCodecError, fixed, require};

pub(super) fn timestamp_to_proto(value: Timestamp) -> wire::TimestampV1 {
    wire::TimestampV1 {
        seconds: value.seconds(),
        nanos: value.nanoseconds(),
    }
}

pub(super) fn timestamp_from_proto(
    value: wire::TimestampV1,
) -> Result<Timestamp, DurableCodecError> {
    Timestamp::new(value.seconds, value.nanos).map_err(|_| DurableCodecError::corrupt())
}

pub(super) const fn actor_kind_to_proto(value: ActorKind) -> i32 {
    match value {
        ActorKind::Human => wire::ActorKindV1::ActorKindHuman as i32,
        ActorKind::Agent => wire::ActorKindV1::ActorKindAgent as i32,
        ActorKind::Service => wire::ActorKindV1::ActorKindService as i32,
    }
}

pub(super) fn actor_kind_from_proto(value: i32) -> Result<ActorKind, DurableCodecError> {
    match wire::ActorKindV1::try_from(value).map_err(|_| DurableCodecError::corrupt())? {
        wire::ActorKindV1::ActorKindHuman => Ok(ActorKind::Human),
        wire::ActorKindV1::ActorKindAgent => Ok(ActorKind::Agent),
        wire::ActorKindV1::ActorKindService => Ok(ActorKind::Service),
        wire::ActorKindV1::ActorKindUnspecified => Err(DurableCodecError::corrupt()),
    }
}

pub(super) fn tenant_scope_to_proto(value: &TenantScope) -> wire::TenantScopeV1 {
    use wire::tenant_scope_v1::Scope;
    let scope = match value {
        TenantScope::Global => Scope::Global(wire::UnitV1 {}),
        TenantScope::Tenant(tenant) => Scope::TenantId(tenant.as_str().to_owned()),
    };
    wire::TenantScopeV1 { scope: Some(scope) }
}

pub(super) fn tenant_scope_from_proto(
    value: wire::TenantScopeV1,
) -> Result<TenantScope, DurableCodecError> {
    use wire::tenant_scope_v1::Scope;
    match require(value.scope)? {
        Scope::Global(_) => Ok(TenantScope::Global),
        Scope::TenantId(value) => TenantId::new(value)
            .map(TenantScope::Tenant)
            .map_err(|_| DurableCodecError::corrupt()),
    }
}

pub(super) fn actor_to_proto(value: &AdmittedActorContext) -> wire::AdmittedActorContextV1 {
    wire::AdmittedActorContextV1 {
        principal_id: value.principal_id().as_str().to_owned(),
        actor_kind: actor_kind_to_proto(value.actor_kind()),
        tenant_scope: Some(tenant_scope_to_proto(value.tenant_scope())),
        agent_session_id: value.agent_session_id().map(|id| id.as_bytes().to_vec()),
    }
}

pub(super) fn actor_from_proto(
    value: wire::AdmittedActorContextV1,
) -> Result<AdmittedActorContext, DurableCodecError> {
    let principal_id =
        ActorId::new(value.principal_id).map_err(|_| DurableCodecError::corrupt())?;
    let actor_kind = actor_kind_from_proto(value.actor_kind)?;
    let tenant_scope = tenant_scope_from_proto(require(value.tenant_scope)?)?;
    let agent_session_id = value
        .agent_session_id
        .map(|bytes| {
            AgentSessionId::from_bytes(fixed(bytes)?).map_err(|_| DurableCodecError::corrupt())
        })
        .transpose()?;
    Ok(AdmittedActorContext::new(
        principal_id,
        actor_kind,
        tenant_scope,
        agent_session_id,
    ))
}

pub(super) fn audit_principal_to_proto(value: &AuditPrincipalV1) -> wire::AuditPrincipalV1 {
    wire::AuditPrincipalV1 {
        principal_id: value.principal_id().as_str().to_owned(),
        actor_kind: actor_kind_to_proto(value.actor_kind()),
        capability_id: value.capability_id().as_bytes().to_vec(),
        capability_revision: value.capability_revision().get(),
    }
}

pub(super) fn audit_principal_from_proto(
    value: wire::AuditPrincipalV1,
) -> Result<AuditPrincipalV1, DurableCodecError> {
    Ok(AuditPrincipalV1::new(
        ActorId::new(value.principal_id).map_err(|_| DurableCodecError::corrupt())?,
        actor_kind_from_proto(value.actor_kind)?,
        CapabilityId::from_bytes(fixed(value.capability_id)?)
            .map_err(|_| DurableCodecError::corrupt())?,
        NonZeroU64::new(value.capability_revision).ok_or_else(DurableCodecError::corrupt)?,
    ))
}

pub(super) fn event_id_to_proto(value: EventId) -> wire::EventIdV1 {
    wire::EventIdV1 {
        commit_sequence: value.commit_sequence().get(),
        event_ordinal: value.event_ordinal(),
    }
}

pub(super) fn event_id_from_proto(value: wire::EventIdV1) -> Result<EventId, DurableCodecError> {
    Ok(EventId::new(
        CommitSequence::new(value.commit_sequence).ok_or_else(DurableCodecError::corrupt)?,
        value.event_ordinal,
    ))
}

pub(super) fn plan_to_proto(value: &ExecutablePlanRef) -> wire::ExecutablePlanRefV1 {
    wire::ExecutablePlanRefV1 {
        contract_lineage: value.contract_lineage().as_str().to_owned(),
        contract_version: value.contract_version().get(),
        contract_bundle_hash: value.contract_bundle_hash().as_bytes().to_vec(),
        command_id: value.command_id().get(),
        command_plan_hash: value.command_plan_hash().as_bytes().to_vec(),
    }
}

pub(super) fn plan_from_proto(
    value: wire::ExecutablePlanRefV1,
) -> Result<ExecutablePlanRef, DurableCodecError> {
    Ok(ExecutablePlanRef::new(
        ContractLineage::new(value.contract_lineage).map_err(|_| DurableCodecError::corrupt())?,
        ContractVersion::new(value.contract_version).ok_or_else(DurableCodecError::corrupt)?,
        ContractBundleHash::from_bytes(fixed(value.contract_bundle_hash)?),
        CommandId::new(value.command_id).ok_or_else(DurableCodecError::corrupt)?,
        PlanHash::from_bytes(fixed(value.command_plan_hash)?),
    ))
}

pub(super) fn binding_to_proto(
    value: &DurableKeySchemaBindingV1,
) -> wire::DurableKeySchemaBindingV1 {
    wire::DurableKeySchemaBindingV1 {
        contract_lineage: value.lineage().as_str().to_owned(),
        contract_version: value.contract_version().get(),
        contract_bundle_hash: value.bundle_hash().as_bytes().to_vec(),
    }
}

pub(super) fn binding_from_proto(
    value: wire::DurableKeySchemaBindingV1,
) -> Result<DurableKeySchemaBindingV1, DurableCodecError> {
    Ok(DurableKeySchemaBindingV1::new(
        ContractLineage::new(value.contract_lineage).map_err(|_| DurableCodecError::corrupt())?,
        ContractVersion::new(value.contract_version).ok_or_else(DurableCodecError::corrupt)?,
        ContractBundleHash::from_bytes(fixed(value.contract_bundle_hash)?),
    ))
}

pub(super) fn identity_to_proto(value: &IdempotencyIdentity) -> wire::IdempotencyIdentityV1 {
    let digest = value.caller_key_digest();
    wire::IdempotencyIdentityV1 {
        database_id: value.database_id().as_bytes().to_vec(),
        environment: value.environment().as_str().to_owned(),
        tenant_scope: Some(tenant_scope_to_proto(value.tenant_scope())),
        principal_id: value.principal_id().as_str().to_owned(),
        contract_lineage: value.contract_lineage().as_str().to_owned(),
        command_id: value.command_id().get(),
        caller_key_digest: Some(wire::IdempotencyKeyDigestV1 {
            digest_scheme: u32::from(digest.scheme()),
            digest_key_id: digest.key_id().get(),
            digest: digest.as_bytes().to_vec(),
        }),
    }
}

pub(super) fn identity_from_proto(
    value: wire::IdempotencyIdentityV1,
) -> Result<IdempotencyIdentity, DurableCodecError> {
    use riffdb_types::{DatabaseId, Environment};
    let digest = require(value.caller_key_digest)?;
    if digest.digest_scheme != 1 {
        return Err(DurableCodecError::corrupt());
    }
    Ok(IdempotencyIdentity::new(
        DatabaseId::from_bytes(fixed(value.database_id)?)
            .map_err(|_| DurableCodecError::corrupt())?,
        Environment::new(value.environment).map_err(|_| DurableCodecError::corrupt())?,
        tenant_scope_from_proto(require(value.tenant_scope)?)?,
        ActorId::new(value.principal_id).map_err(|_| DurableCodecError::corrupt())?,
        ContractLineage::new(value.contract_lineage).map_err(|_| DurableCodecError::corrupt())?,
        CommandId::new(value.command_id).ok_or_else(DurableCodecError::corrupt)?,
        IdempotencyKeyDigest::from_hmac_bytes(
            DigestKeyId::new(digest.digest_key_id).ok_or_else(DurableCodecError::corrupt)?,
            fixed(digest.digest)?,
        ),
    ))
}

pub(super) fn claims_to_proto(
    value: &StoredAdmittedProvenanceClaimsV1,
) -> wire::StoredAdmittedProvenanceClaimsV1 {
    wire::StoredAdmittedProvenanceClaimsV1 {
        source_repository: value
            .source_repository()
            .map(|value| value.as_str().to_owned()),
        source_commit: value.source_commit().map(|value| value.as_str().to_owned()),
        reason: value.reason().map(|value| value.as_str().to_owned()),
        approval_id: value.approval_id().map(|value| value.as_str().to_owned()),
    }
}

pub(super) fn claims_from_proto(
    value: wire::StoredAdmittedProvenanceClaimsV1,
) -> Result<StoredAdmittedProvenanceClaimsV1, DurableCodecError> {
    use riffdb_types::{ApprovalId, ProvenanceReason, SourceCommit, SourceRepository};
    StoredAdmittedProvenanceClaimsV1::new(
        value
            .source_repository
            .map(SourceRepository::new)
            .transpose()
            .map_err(|_| DurableCodecError::corrupt())?,
        value
            .source_commit
            .map(SourceCommit::new)
            .transpose()
            .map_err(|_| DurableCodecError::corrupt())?,
        value
            .reason
            .map(ProvenanceReason::new)
            .transpose()
            .map_err(|_| DurableCodecError::corrupt())?,
        value
            .approval_id
            .map(ApprovalId::new)
            .transpose()
            .map_err(|_| DurableCodecError::corrupt())?,
    )
    .map_err(DurableCodecError::from_storage_value)
}

pub(super) fn entity_target_to_proto(value: &EntityTarget) -> wire::EntityTargetV1 {
    wire::EntityTargetV1 {
        entity_type_id: value.entity_type_id().get(),
        entity_key: value.key().as_bytes().to_vec(),
    }
}

pub(super) fn entity_target_from_proto(
    value: wire::EntityTargetV1,
) -> Result<EntityTarget, DurableCodecError> {
    let entity_type_id =
        EntityTypeId::new(value.entity_type_id).ok_or_else(DurableCodecError::corrupt)?;
    let key = EntityKey::from_bytes(value.entity_key).map_err(|_| DurableCodecError::corrupt())?;
    EntityTarget::new(entity_type_id, key).map_err(DurableCodecError::from_storage_value)
}

pub(super) fn expected_to_proto(value: ExpectedEntityState) -> wire::ExpectedEntityStateV1 {
    use wire::expected_entity_state_v1::State;
    wire::ExpectedEntityStateV1 {
        state: Some(match value {
            ExpectedEntityState::Absent => State::Absent(wire::UnitV1 {}),
            ExpectedEntityState::Present(version) => State::PresentEntityVersion(version.get()),
        }),
    }
}

pub(super) fn expected_from_proto(
    value: wire::ExpectedEntityStateV1,
) -> Result<ExpectedEntityState, DurableCodecError> {
    use wire::expected_entity_state_v1::State;
    match require(value.state)? {
        State::Absent(_) => Ok(ExpectedEntityState::Absent),
        State::PresentEntityVersion(value) => EntityVersion::new(value)
            .map(ExpectedEntityState::Present)
            .ok_or_else(DurableCodecError::corrupt),
    }
}

pub(super) fn epoch_to_proto(value: IndexEpochPosition) -> wire::IndexEpochPositionV1 {
    use wire::index_epoch_position_v1::Position;
    wire::IndexEpochPositionV1 {
        position: Some(match value {
            IndexEpochPosition::BeforeFirst => Position::BeforeFirst(wire::UnitV1 {}),
            IndexEpochPosition::Value(epoch) => Position::Epoch(epoch.get()),
        }),
    }
}

pub(super) fn epoch_from_proto(
    value: wire::IndexEpochPositionV1,
) -> Result<IndexEpochPosition, DurableCodecError> {
    use wire::index_epoch_position_v1::Position;
    match require(value.position)? {
        Position::BeforeFirst(_) => Ok(IndexEpochPosition::BeforeFirst),
        Position::Epoch(value) => IndexEpoch::new(value)
            .map(IndexEpochPosition::Value)
            .ok_or_else(DurableCodecError::corrupt),
    }
}

pub(super) fn declared_outcome_to_proto(value: &DeclaredOutcome) -> wire::DeclaredOutcomeV1 {
    wire::DeclaredOutcomeV1 {
        outcome_id: value.outcome_id().get(),
        canonical_value: value.value_encoded().to_vec(),
    }
}

pub(super) fn declared_outcome_from_proto(
    value: wire::DeclaredOutcomeV1,
) -> Result<DeclaredOutcome, DurableCodecError> {
    DeclaredOutcome::new(
        OutcomeId::new(value.outcome_id).ok_or_else(DurableCodecError::corrupt)?,
        canonical_record_from_bytes(&value.canonical_value)?,
    )
    .map_err(DurableCodecError::from_storage_value)
}

pub(super) fn canonical_record_from_bytes(
    bytes: &[u8],
) -> Result<CanonicalRecord, DurableCodecError> {
    verified_canonical_record_from_bytes(bytes).map(VerifiedCanonicalRecordV1::into_record)
}

/// Decodes one canonical record and retains the buffer that proved it canonical.
///
/// The check is exactly [`canonical_record_from_bytes`]'s: decode, re-encode,
/// byte-compare, fail closed on any difference. Callers that go on to build a
/// stored row take the proved buffer instead of encoding the same record again.
pub(super) fn verified_canonical_record_from_bytes(
    bytes: &[u8],
) -> Result<VerifiedCanonicalRecordV1, DurableCodecError> {
    VerifiedCanonicalRecordV1::verify(bytes).map_err(|_| DurableCodecError::corrupt())
}

pub(super) fn structural_prefix_from_bytes(
    bytes: Vec<u8>,
) -> Result<StructurallyDecodedIndexRangePrefixV1, DurableCodecError> {
    let owner = bytes
        .get(2..6)
        .ok_or_else(DurableCodecError::corrupt)?
        .try_into()
        .map(u32::from_be_bytes)
        .map_err(|_| DurableCodecError::corrupt())?;
    let index_id = IndexId::new(owner).ok_or_else(DurableCodecError::corrupt)?;
    StructurallyDecodedIndexRangePrefixV1::new(index_id, bytes)
        .map_err(DurableCodecError::from_storage_value)
}

pub(super) fn logical_time_from_proto(
    value: wire::TimestampV1,
) -> Result<LogicalTime, DurableCodecError> {
    Ok(LogicalTime::new(timestamp_from_proto(value)?))
}

pub(super) fn storage_result<T>(
    value: Result<T, StorageValueError>,
) -> Result<T, DurableCodecError> {
    value.map_err(DurableCodecError::from_storage_value)
}

pub(super) fn field_id(value: u32) -> Result<FieldId, DurableCodecError> {
    FieldId::new(value).ok_or_else(DurableCodecError::corrupt)
}
