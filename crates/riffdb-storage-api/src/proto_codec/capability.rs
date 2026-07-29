use std::num::{NonZeroU16, NonZeroU64};

use riffdb_proto::storage::v1 as wire;
use riffdb_types::{
    ActorId, AdministrationSequence, ApplicationRoleHash, ApprovalId, Audience,
    CapabilityGrantError, CapabilityId, CapabilityTokenDigest, CommandId, ContractLineage,
    DatabaseId, DigestKeyId, EntityTypeId, Environment, FieldId, IndexId, PartitionKey,
    ProjectionId, QueryModuleHash, QueryOperationName, RequestId,
};

use crate::{
    CapabilityAdministrationOperationV1, CapabilityBootstrapMarkerV1, CapabilityGrantV1,
    CapabilityLifecycleV1, CapabilityPermissionKindV1, CapabilityPermissionV1,
    CapabilityPermissionsV1, CapabilityTokenLookupV1, EncodedPageItem, EntityFieldVisibilityV1,
    PartitionScopeV1, RevocationReasonCodeV1, ScopedPartitionV1, StorageValueError,
    StoredCapabilityAdministrationV1, StoredCapabilityRecordV1,
};

use super::{
    CanonicalStoredEnvelopeV1, DurableCodecError, actor_kind_from_proto, actor_kind_to_proto,
    audit_principal_from_proto, audit_principal_to_proto, decode_message, encode_message, field_id,
    fixed, require, storage_result, tenant_scope_from_proto, tenant_scope_to_proto,
    timestamp_from_proto, timestamp_to_proto,
};

const RECORD: &str = "riffdb.storage.v1.CapabilityRecordV1";
const LOOKUP: &str = "riffdb.storage.v1.CapabilityTokenLookupV1";
const BOOTSTRAP: &str = "riffdb.storage.v1.CapabilityBootstrapMarkerV1";
const ADMINISTRATION: &str = "riffdb.storage.v1.CapabilityAdministrationAuditV1";

fn grant_result<T>(value: Result<T, CapabilityGrantError>) -> Result<T, DurableCodecError> {
    storage_result(value.map_err(StorageValueError::from))
}

fn permission_to_proto(value: &CapabilityPermissionV1) -> wire::CapabilityPermissionV1 {
    use CapabilityPermissionV1::{
        ExplainCommand, InvokeCommand, QueryProjection, ReadEntity, ReadProjectionStatus,
        ScanIndex, Unparameterized,
    };

    let (contract_lineage, stable_id, query_module_hash, query_name, application_role_hash) =
        match value {
            Unparameterized(_) => (None, None, None, None, None),
            ExplainCommand(lineage, id) | InvokeCommand(lineage, id) => (
                Some(lineage.as_str().to_owned()),
                Some(id.get()),
                None,
                None,
                None,
            ),
            ReadEntity(lineage, id) => (
                Some(lineage.as_str().to_owned()),
                Some(id.get()),
                None,
                None,
                None,
            ),
            ScanIndex(lineage, id) => (
                Some(lineage.as_str().to_owned()),
                Some(id.get()),
                None,
                None,
                None,
            ),
            QueryProjection(lineage, id) | ReadProjectionStatus(lineage, id) => (
                Some(lineage.as_str().to_owned()),
                Some(id.get()),
                None,
                None,
                None,
            ),
            CapabilityPermissionV1::ExplainNamedQuery(lineage, hash, name)
            | CapabilityPermissionV1::ExecuteNamedQuery(lineage, hash, name) => (
                Some(lineage.as_str().to_owned()),
                None,
                Some(hash.as_bytes().to_vec()),
                Some(name.as_str().to_owned()),
                None,
            ),
            CapabilityPermissionV1::ApplicationRoleIdentity(hash) => {
                (None, None, None, None, Some(hash.as_bytes().to_vec()))
            }
        };
    wire::CapabilityPermissionV1 {
        kind: i32::from(value.kind().tag()),
        contract_lineage,
        stable_id,
        query_module_hash,
        query_name,
        application_role_hash,
    }
}

fn permission_from_proto(
    value: wire::CapabilityPermissionV1,
) -> Result<CapabilityPermissionV1, DurableCodecError> {
    let kind = u8::try_from(value.kind)
        .ok()
        .and_then(CapabilityPermissionKindV1::from_tag)
        .ok_or_else(DurableCodecError::corrupt)?;
    let parameter = match (
        value.contract_lineage,
        value.stable_id,
        value.query_module_hash,
        value.query_name,
        value.application_role_hash,
    ) {
        (None, None, None, None, None) => PermissionParameter::None,
        (Some(lineage), Some(id), None, None, None) => PermissionParameter::StableId(
            ContractLineage::new(lineage).map_err(|_| DurableCodecError::corrupt())?,
            id,
        ),
        (Some(lineage), None, Some(hash), Some(name), None) => {
            let hash: [u8; 32] = hash.try_into().map_err(|_| DurableCodecError::corrupt())?;
            PermissionParameter::NamedQuery(
                ContractLineage::new(lineage).map_err(|_| DurableCodecError::corrupt())?,
                QueryModuleHash::from_bytes(hash),
                QueryOperationName::new(name).map_err(|_| DurableCodecError::corrupt())?,
            )
        }
        (None, None, None, None, Some(hash)) => {
            let hash: [u8; 32] = hash.try_into().map_err(|_| DurableCodecError::corrupt())?;
            PermissionParameter::ApplicationRole(ApplicationRoleHash::from_bytes(hash))
        }
        _ => return Err(DurableCodecError::corrupt()),
    };
    match (kind, parameter) {
        (
            CapabilityPermissionKindV1::ExplainCommand,
            PermissionParameter::StableId(lineage, id),
        ) => Ok(CapabilityPermissionV1::ExplainCommand(
            lineage,
            CommandId::new(id).ok_or_else(DurableCodecError::corrupt)?,
        )),
        (CapabilityPermissionKindV1::InvokeCommand, PermissionParameter::StableId(lineage, id)) => {
            Ok(CapabilityPermissionV1::InvokeCommand(
                lineage,
                CommandId::new(id).ok_or_else(DurableCodecError::corrupt)?,
            ))
        }
        (CapabilityPermissionKindV1::ReadEntity, PermissionParameter::StableId(lineage, id)) => {
            Ok(CapabilityPermissionV1::ReadEntity(
                lineage,
                EntityTypeId::new(id).ok_or_else(DurableCodecError::corrupt)?,
            ))
        }
        (CapabilityPermissionKindV1::ScanIndex, PermissionParameter::StableId(lineage, id)) => {
            Ok(CapabilityPermissionV1::ScanIndex(
                lineage,
                IndexId::new(id).ok_or_else(DurableCodecError::corrupt)?,
            ))
        }
        (
            CapabilityPermissionKindV1::QueryProjection,
            PermissionParameter::StableId(lineage, id),
        ) => Ok(CapabilityPermissionV1::QueryProjection(
            lineage,
            ProjectionId::new(id).ok_or_else(DurableCodecError::corrupt)?,
        )),
        (
            CapabilityPermissionKindV1::ReadProjectionStatus,
            PermissionParameter::StableId(lineage, id),
        ) => Ok(CapabilityPermissionV1::ReadProjectionStatus(
            lineage,
            ProjectionId::new(id).ok_or_else(DurableCodecError::corrupt)?,
        )),
        (
            CapabilityPermissionKindV1::ExplainNamedQuery,
            PermissionParameter::NamedQuery(lineage, hash, name),
        ) => Ok(CapabilityPermissionV1::ExplainNamedQuery(
            lineage, hash, name,
        )),
        (
            CapabilityPermissionKindV1::ExecuteNamedQuery,
            PermissionParameter::NamedQuery(lineage, hash, name),
        ) => Ok(CapabilityPermissionV1::ExecuteNamedQuery(
            lineage, hash, name,
        )),
        (
            CapabilityPermissionKindV1::ApplicationRoleIdentity,
            PermissionParameter::ApplicationRole(hash),
        ) => Ok(CapabilityPermissionV1::ApplicationRoleIdentity(hash)),
        (kind, PermissionParameter::None) => {
            grant_result(CapabilityPermissionV1::unparameterized(kind))
        }
        (
            _,
            PermissionParameter::StableId(..)
            | PermissionParameter::NamedQuery(..)
            | PermissionParameter::ApplicationRole(..),
        ) => Err(DurableCodecError::corrupt()),
    }
}

enum PermissionParameter {
    None,
    StableId(ContractLineage, u32),
    NamedQuery(ContractLineage, QueryModuleHash, QueryOperationName),
    ApplicationRole(ApplicationRoleHash),
}

fn permissions_to_proto(value: &CapabilityPermissionsV1) -> wire::CapabilityPermissionsV1 {
    wire::CapabilityPermissionsV1 {
        values: value.as_slice().iter().map(permission_to_proto).collect(),
    }
}

fn permissions_from_proto(
    value: wire::CapabilityPermissionsV1,
) -> Result<CapabilityPermissionsV1, DurableCodecError> {
    let raw = value
        .values
        .into_iter()
        .map(permission_from_proto)
        .collect::<Result<Vec<_>, _>>()?;
    let checked = grant_result(CapabilityPermissionsV1::new(raw.clone()))?;
    if checked.as_slice() != raw {
        return Err(DurableCodecError::corrupt());
    }
    Ok(checked)
}

fn scoped_partition_to_proto(value: &ScopedPartitionV1) -> wire::ScopedPartitionV1 {
    wire::ScopedPartitionV1 {
        contract_lineage: value.lineage().as_str().to_owned(),
        partition_key: value.partition_key().as_bytes().to_vec(),
    }
}

fn scoped_partition_from_proto(
    value: wire::ScopedPartitionV1,
) -> Result<ScopedPartitionV1, DurableCodecError> {
    Ok(ScopedPartitionV1::new(
        ContractLineage::new(value.contract_lineage).map_err(|_| DurableCodecError::corrupt())?,
        PartitionKey::from_bytes(value.partition_key).map_err(|_| DurableCodecError::corrupt())?,
    ))
}

fn partition_scope_to_proto(value: &PartitionScopeV1) -> wire::PartitionScopeV1 {
    use wire::partition_scope_v1::Scope;
    let scope = match value {
        PartitionScopeV1::All => Scope::All(wire::UnitV1 {}),
        PartitionScopeV1::Explicit(values) => Scope::Explicit(wire::ExplicitPartitionScopeV1 {
            partitions: values.iter().map(scoped_partition_to_proto).collect(),
        }),
    };
    wire::PartitionScopeV1 { scope: Some(scope) }
}

fn partition_scope_from_proto(
    value: wire::PartitionScopeV1,
) -> Result<PartitionScopeV1, DurableCodecError> {
    use wire::partition_scope_v1::Scope;
    match require(value.scope)? {
        Scope::All(_) => Ok(PartitionScopeV1::All),
        Scope::Explicit(value) => {
            let raw = value
                .partitions
                .into_iter()
                .map(scoped_partition_from_proto)
                .collect::<Result<Vec<_>, _>>()?;
            let checked = grant_result(PartitionScopeV1::explicit(raw.clone()))?;
            if checked.explicit_entries() != Some(raw.as_slice()) {
                return Err(DurableCodecError::corrupt());
            }
            Ok(checked)
        }
    }
}

fn visibility_to_proto(value: &EntityFieldVisibilityV1) -> wire::EntityFieldVisibilityV1 {
    wire::EntityFieldVisibilityV1 {
        contract_lineage: value.lineage().as_str().to_owned(),
        entity_type_id: value.entity_type().get(),
        field_ids: value.fields().iter().map(|value| value.get()).collect(),
    }
}

fn visibility_from_proto(
    value: wire::EntityFieldVisibilityV1,
) -> Result<EntityFieldVisibilityV1, DurableCodecError> {
    let raw_fields = value
        .field_ids
        .into_iter()
        .map(field_id)
        .collect::<Result<Vec<FieldId>, _>>()?;
    let checked = grant_result(EntityFieldVisibilityV1::new(
        ContractLineage::new(value.contract_lineage).map_err(|_| DurableCodecError::corrupt())?,
        EntityTypeId::new(value.entity_type_id).ok_or_else(DurableCodecError::corrupt)?,
        raw_fields.clone(),
    ))?;
    if checked.fields() != raw_fields {
        return Err(DurableCodecError::corrupt());
    }
    Ok(checked)
}

fn grant_to_proto(value: &CapabilityGrantV1) -> wire::CapabilityGrantV1 {
    wire::CapabilityGrantV1 {
        tenant_scope: Some(tenant_scope_to_proto(value.tenant_scope())),
        partition_scope: Some(partition_scope_to_proto(value.partition_scope())),
        permissions: Some(permissions_to_proto(value.permissions())),
        field_visibility: value
            .field_visibility()
            .iter()
            .map(visibility_to_proto)
            .collect(),
        max_scan_rows: u32::from(value.max_scan_rows().get()),
        approval_required: value
            .approval_required()
            .iter()
            .map(|value| i32::from(value.tag()))
            .collect(),
    }
}

fn grant_from_proto(
    value: wire::CapabilityGrantV1,
) -> Result<CapabilityGrantV1, DurableCodecError> {
    let raw_visibility = value
        .field_visibility
        .into_iter()
        .map(visibility_from_proto)
        .collect::<Result<Vec<_>, _>>()?;
    let raw_approval = value
        .approval_required
        .into_iter()
        .map(|value| {
            u8::try_from(value)
                .ok()
                .and_then(CapabilityPermissionKindV1::from_tag)
                .ok_or_else(DurableCodecError::corrupt)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let checked = grant_result(CapabilityGrantV1::new(
        tenant_scope_from_proto(require(value.tenant_scope)?)?,
        partition_scope_from_proto(require(value.partition_scope)?)?,
        permissions_from_proto(require(value.permissions)?)?,
        raw_visibility.clone(),
        u16::try_from(value.max_scan_rows)
            .ok()
            .and_then(NonZeroU16::new)
            .ok_or_else(DurableCodecError::corrupt)?,
        raw_approval.clone(),
    ))?;
    if checked.field_visibility() != raw_visibility || checked.approval_required() != raw_approval {
        return Err(DurableCodecError::corrupt());
    }
    Ok(checked)
}

fn digest_to_proto(value: CapabilityTokenDigest) -> wire::CapabilityTokenDigestV1 {
    wire::CapabilityTokenDigestV1 {
        digest_scheme: u32::from(value.scheme()),
        digest_key_id: value.key_id().get(),
        digest: value.as_bytes().to_vec(),
    }
}

fn digest_from_proto(
    value: wire::CapabilityTokenDigestV1,
) -> Result<CapabilityTokenDigest, DurableCodecError> {
    if value.digest_scheme != 1 {
        return Err(DurableCodecError::corrupt());
    }
    Ok(CapabilityTokenDigest::from_hmac_bytes(
        DigestKeyId::new(value.digest_key_id).ok_or_else(DurableCodecError::corrupt)?,
        fixed(value.digest)?,
    ))
}

fn lifecycle_to_proto(value: &CapabilityLifecycleV1) -> wire::CapabilityLifecycleV1 {
    use wire::capability_lifecycle_v1::State;
    let state = match value {
        CapabilityLifecycleV1::Active => State::Active(wire::UnitV1 {}),
        CapabilityLifecycleV1::Revoked {
            revoked_at,
            administration_sequence,
            reason,
        } => State::Revoked(wire::RevokedCapabilityV1 {
            revoked_at: Some(timestamp_to_proto(*revoked_at)),
            administration_sequence: administration_sequence.get(),
            reason: i32::from(reason.tag()),
        }),
    };
    wire::CapabilityLifecycleV1 { state: Some(state) }
}

fn lifecycle_from_proto(
    value: wire::CapabilityLifecycleV1,
) -> Result<CapabilityLifecycleV1, DurableCodecError> {
    use wire::capability_lifecycle_v1::State;
    match require(value.state)? {
        State::Active(_) => Ok(CapabilityLifecycleV1::Active),
        State::Revoked(value) => Ok(CapabilityLifecycleV1::Revoked {
            revoked_at: timestamp_from_proto(require(value.revoked_at)?)?,
            administration_sequence: AdministrationSequence::new(value.administration_sequence)
                .ok_or_else(DurableCodecError::corrupt)?,
            reason: u8::try_from(value.reason)
                .ok()
                .and_then(RevocationReasonCodeV1::from_tag)
                .ok_or_else(DurableCodecError::corrupt)?,
        }),
    }
}

/// Encodes one complete durable capability record.
pub fn encode_capability_record_v1(
    value: &StoredCapabilityRecordV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(
        RECORD,
        &wire::CapabilityRecordV1 {
            capability_id: value.capability_id().as_bytes().to_vec(),
            revision: value.revision().get(),
            token_digest: Some(digest_to_proto(value.token_digest())),
            database_id: value.database_id().as_bytes().to_vec(),
            environment: value.environment().as_str().to_owned(),
            principal_id: value.principal_id().as_str().to_owned(),
            actor_kind: actor_kind_to_proto(value.actor_kind()),
            audiences: value
                .audiences()
                .iter()
                .map(|value| value.as_str().to_owned())
                .collect(),
            issued_at: Some(timestamp_to_proto(value.issued_at())),
            expires_at: Some(timestamp_to_proto(value.expires_at())),
            creation_sequence: value.creation_sequence().get(),
            creation_request_id: value.creation_request_id().as_bytes().to_vec(),
            grant: Some(grant_to_proto(value.grant())),
            lifecycle: Some(lifecycle_to_proto(value.lifecycle())),
        },
    )
}

/// Decodes one complete durable capability record.
pub fn decode_capability_record_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredCapabilityRecordV1>, DurableCodecError> {
    decode_message::<wire::CapabilityRecordV1, _, _>(RECORD, encoded, |value| {
        storage_result(StoredCapabilityRecordV1::from_stored_parts(
            CapabilityId::from_bytes(fixed(value.capability_id)?)
                .map_err(|_| DurableCodecError::corrupt())?,
            NonZeroU64::new(value.revision).ok_or_else(DurableCodecError::corrupt)?,
            digest_from_proto(require(value.token_digest)?)?,
            DatabaseId::from_bytes(fixed(value.database_id)?)
                .map_err(|_| DurableCodecError::corrupt())?,
            Environment::new(value.environment).map_err(|_| DurableCodecError::corrupt())?,
            ActorId::new(value.principal_id).map_err(|_| DurableCodecError::corrupt())?,
            actor_kind_from_proto(value.actor_kind)?,
            value
                .audiences
                .into_iter()
                .map(Audience::new)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| DurableCodecError::corrupt())?,
            timestamp_from_proto(require(value.issued_at)?)?,
            timestamp_from_proto(require(value.expires_at)?)?,
            AdministrationSequence::new(value.creation_sequence)
                .ok_or_else(DurableCodecError::corrupt)?,
            RequestId::from_bytes(fixed(value.creation_request_id)?)
                .map_err(|_| DurableCodecError::corrupt())?,
            grant_from_proto(require(value.grant)?)?,
            lifecycle_from_proto(require(value.lifecycle)?)?,
        ))
    })
}

/// Encodes the reciprocal token-digest lookup value.
pub fn encode_capability_token_lookup_v1(
    value: CapabilityTokenLookupV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(
        LOOKUP,
        &wire::CapabilityTokenLookupV1 {
            capability_id: value.capability_id().as_bytes().to_vec(),
        },
    )
}

/// Decodes the reciprocal token-digest lookup value.
pub fn decode_capability_token_lookup_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<CapabilityTokenLookupV1>, DurableCodecError> {
    decode_message::<wire::CapabilityTokenLookupV1, _, _>(LOOKUP, encoded, |value| {
        Ok(CapabilityTokenLookupV1::new(
            CapabilityId::from_bytes(fixed(value.capability_id)?)
                .map_err(|_| DurableCodecError::corrupt())?,
        ))
    })
}

/// Encodes the irreversible capability-bootstrap marker.
pub fn encode_capability_bootstrap_marker_v1(
    value: CapabilityBootstrapMarkerV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(
        BOOTSTRAP,
        &wire::CapabilityBootstrapMarkerV1 {
            database_id: value.database_id().as_bytes().to_vec(),
            capability_id: value.capability_id().as_bytes().to_vec(),
            administration_sequence: value.administration_sequence().get(),
        },
    )
}

/// Decodes the irreversible capability-bootstrap marker.
pub fn decode_capability_bootstrap_marker_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<CapabilityBootstrapMarkerV1>, DurableCodecError> {
    decode_message::<wire::CapabilityBootstrapMarkerV1, _, _>(BOOTSTRAP, encoded, |value| {
        Ok(CapabilityBootstrapMarkerV1::new(
            DatabaseId::from_bytes(fixed(value.database_id)?)
                .map_err(|_| DurableCodecError::corrupt())?,
            CapabilityId::from_bytes(fixed(value.capability_id)?)
                .map_err(|_| DurableCodecError::corrupt())?,
            AdministrationSequence::new(value.administration_sequence)
                .ok_or_else(DurableCodecError::corrupt)?,
        ))
    })
}

/// Encodes one capability-administration audit record.
pub fn encode_capability_administration_v1(
    value: &StoredCapabilityAdministrationV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(
        ADMINISTRATION,
        &wire::CapabilityAdministrationAuditV1 {
            administration_sequence: value.administration_sequence().get(),
            request_id: value.request_id().as_bytes().to_vec(),
            operation: i32::from(value.operation().tag()),
            timestamp: Some(timestamp_to_proto(value.timestamp())),
            initiator: value.initiator().map(audit_principal_to_proto),
            target_capability_id: value.target_capability_id().as_bytes().to_vec(),
            resulting_revision: value.resulting_revision().get(),
            approval_id: value.approval_id().map(|value| value.as_str().to_owned()),
            revocation_reason: value
                .revocation_reason()
                .map(|value| i32::from(value.tag())),
        },
    )
}

/// Decodes one capability-administration audit record.
pub fn decode_capability_administration_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredCapabilityAdministrationV1>, DurableCodecError> {
    decode_message::<wire::CapabilityAdministrationAuditV1, _, _>(
        ADMINISTRATION,
        encoded,
        |value| {
            storage_result(StoredCapabilityAdministrationV1::new(
                AdministrationSequence::new(value.administration_sequence)
                    .ok_or_else(DurableCodecError::corrupt)?,
                RequestId::from_bytes(fixed(value.request_id)?)
                    .map_err(|_| DurableCodecError::corrupt())?,
                u8::try_from(value.operation)
                    .ok()
                    .and_then(CapabilityAdministrationOperationV1::from_tag)
                    .ok_or_else(DurableCodecError::corrupt)?,
                timestamp_from_proto(require(value.timestamp)?)?,
                value
                    .initiator
                    .map(audit_principal_from_proto)
                    .transpose()?,
                CapabilityId::from_bytes(fixed(value.target_capability_id)?)
                    .map_err(|_| DurableCodecError::corrupt())?,
                NonZeroU64::new(value.resulting_revision).ok_or_else(DurableCodecError::corrupt)?,
                value
                    .approval_id
                    .map(ApprovalId::new)
                    .transpose()
                    .map_err(|_| DurableCodecError::corrupt())?,
                value
                    .revocation_reason
                    .map(|value| {
                        u8::try_from(value)
                            .ok()
                            .and_then(RevocationReasonCodeV1::from_tag)
                            .ok_or_else(DurableCodecError::corrupt)
                    })
                    .transpose()?,
            ))
        },
    )
}

#[cfg(test)]
mod permission_tests {
    use super::*;

    #[test]
    fn exact_named_and_ad_hoc_query_permissions_round_trip_durably() {
        let lineage = ContractLineage::new("ticketdesk").expect("lineage");
        let module = QueryModuleHash::from_bytes([0x41; 32]);
        let name = QueryOperationName::new("TicketPage").expect("query name");
        for permission in [
            CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::CheckAdHocQuery)
                .expect("ad-hoc permission"),
            CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::ExplainAdHocQuery)
                .expect("ad-hoc permission"),
            CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::ExecuteAdHocQuery)
                .expect("ad-hoc permission"),
            CapabilityPermissionV1::ExplainNamedQuery(lineage.clone(), module, name.clone()),
            CapabilityPermissionV1::ExecuteNamedQuery(lineage.clone(), module, name.clone()),
            CapabilityPermissionV1::ApplicationRoleIdentity(ApplicationRoleHash::from_bytes(
                [0x42; 32],
            )),
        ] {
            assert_eq!(
                permission_from_proto(permission_to_proto(&permission)).expect("round trip"),
                permission
            );
        }
    }
}
