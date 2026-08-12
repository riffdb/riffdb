use std::num::{NonZeroU16, NonZeroU64};

use prost::Message;
use riffdb_proto::durable::readable_record_registry;
use riffdb_proto::storage::v1 as wire;
use riffdb_types::{
    ActorId, AdministrationSequence, ApplicationRoleHash, ApprovalId, Audience,
    CapabilityApplicationExportGrantV1, CapabilityApplicationExportScopeV1,
    CapabilityExportGrantV1, CapabilityGrantError, CapabilityId, CapabilityPrincipalFactsV1,
    CapabilityRowPolicyBindingV1, CapabilityRowPolicyGrantV1, CapabilityRowPolicyOperationV1,
    CapabilityTokenDigest, CommandId, ContractLineage, DatabaseId, DigestKeyId, EntityTypeId,
    Environment, FieldId, IndexId, PartitionKey, ProjectionId, QueryModuleHash, QueryOperationName,
    ReactiveModuleHash, ReactiveOperationName, RequestId, RowPolicyName,
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
const RECORD_V2: &str = "riffdb.storage.v1.CapabilityRecordV2";
const RECORD_V3: &str = "riffdb.storage.v1.CapabilityRecordV3";
const RECORD_V4: &str = "riffdb.storage.v1.CapabilityRecordV4";
const RECORD_V5: &str = "riffdb.storage.v1.CapabilityRecordV5";
const RECORD_V6: &str = "riffdb.storage.v1.CapabilityRecordV6";
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

    let (
        contract_lineage,
        stable_id,
        query_module_hash,
        query_name,
        application_role_hash,
        reactive_module_hash,
        reactive_operation_name,
    ) = match value {
        Unparameterized(_) => (None, None, None, None, None, None, None),
        ExplainCommand(lineage, id) | InvokeCommand(lineage, id) => (
            Some(lineage.as_str().to_owned()),
            Some(id.get()),
            None,
            None,
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
            None,
            None,
        ),
        ScanIndex(lineage, id) => (
            Some(lineage.as_str().to_owned()),
            Some(id.get()),
            None,
            None,
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
            None,
            None,
        ),
        CapabilityPermissionV1::ApplicationRoleIdentity(hash) => (
            None,
            None,
            None,
            None,
            Some(hash.as_bytes().to_vec()),
            None,
            None,
        ),
        CapabilityPermissionV1::MigrateContract(lineage) => (
            Some(lineage.as_str().to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
        ),
        CapabilityPermissionV1::InstallApplication(lineage) => (
            Some(lineage.as_str().to_owned()),
            None,
            None,
            None,
            None,
            None,
            None,
        ),
        CapabilityPermissionV1::ConsumeEventStream(lineage, hash, name)
        | CapabilityPermissionV1::SeekEventStreamConsumer(lineage, hash, name)
        | CapabilityPermissionV1::WatchNamedQuery(lineage, hash, name)
        | CapabilityPermissionV1::ConsumeContextualSubscription(lineage, hash, name) => (
            Some(lineage.as_str().to_owned()),
            None,
            None,
            None,
            None,
            Some(hash.as_bytes().to_vec()),
            Some(name.as_str().to_owned()),
        ),
    };
    wire::CapabilityPermissionV1 {
        kind: i32::from(value.kind().tag()),
        contract_lineage,
        stable_id,
        query_module_hash,
        query_name,
        application_role_hash,
        reactive_module_hash,
        reactive_operation_name,
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
        value.reactive_module_hash,
        value.reactive_operation_name,
    ) {
        (None, None, None, None, None, None, None) => PermissionParameter::None,
        (Some(lineage), None, None, None, None, None, None) => {
            ContractLineage::new(lineage).map_err(|_| DurableCodecError::corrupt())?;
            PermissionParameter::Lineage
        }
        (Some(lineage), Some(id), None, None, None, None, None) => PermissionParameter::StableId(
            ContractLineage::new(lineage).map_err(|_| DurableCodecError::corrupt())?,
            id,
        ),
        (Some(lineage), None, Some(hash), Some(name), None, None, None) => {
            let hash: [u8; 32] = hash.try_into().map_err(|_| DurableCodecError::corrupt())?;
            PermissionParameter::NamedQuery(
                ContractLineage::new(lineage).map_err(|_| DurableCodecError::corrupt())?,
                QueryModuleHash::from_bytes(hash),
                QueryOperationName::new(name).map_err(|_| DurableCodecError::corrupt())?,
            )
        }
        (None, None, None, None, Some(hash), None, None) => {
            let hash: [u8; 32] = hash.try_into().map_err(|_| DurableCodecError::corrupt())?;
            PermissionParameter::ApplicationRole(ApplicationRoleHash::from_bytes(hash))
        }
        (Some(lineage), None, None, None, None, Some(hash), Some(name)) => {
            let hash: [u8; 32] = hash.try_into().map_err(|_| DurableCodecError::corrupt())?;
            PermissionParameter::ReactiveOperation(
                ContractLineage::new(lineage).map_err(|_| DurableCodecError::corrupt())?,
                ReactiveModuleHash::from_bytes(hash),
                ReactiveOperationName::new(name).map_err(|_| DurableCodecError::corrupt())?,
            )
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
        // Migration authority is durable only in CapabilityRecordV2's required
        // extension. The frozen V1 base rejects the otherwise-decodable tag.
        (CapabilityPermissionKindV1::MigrateContract, PermissionParameter::Lineage) => {
            Err(DurableCodecError::corrupt())
        }
        // Installation authority is durable only in CapabilityRecordV3's
        // required extension. The frozen V1 base rejects the bare tag.
        (CapabilityPermissionKindV1::InstallApplication, PermissionParameter::Lineage) => {
            Err(DurableCodecError::corrupt())
        }
        (
            CapabilityPermissionKindV1::ConsumeEventStream,
            PermissionParameter::ReactiveOperation(lineage, hash, name),
        ) => Ok(CapabilityPermissionV1::ConsumeEventStream(
            lineage, hash, name,
        )),
        (
            CapabilityPermissionKindV1::SeekEventStreamConsumer,
            PermissionParameter::ReactiveOperation(lineage, hash, name),
        ) => Ok(CapabilityPermissionV1::SeekEventStreamConsumer(
            lineage, hash, name,
        )),
        (
            CapabilityPermissionKindV1::WatchNamedQuery,
            PermissionParameter::ReactiveOperation(lineage, hash, name),
        ) => Ok(CapabilityPermissionV1::WatchNamedQuery(lineage, hash, name)),
        (
            CapabilityPermissionKindV1::ConsumeContextualSubscription,
            PermissionParameter::ReactiveOperation(lineage, hash, name),
        ) => Ok(CapabilityPermissionV1::ConsumeContextualSubscription(
            lineage, hash, name,
        )),
        (kind, PermissionParameter::None) => {
            grant_result(CapabilityPermissionV1::unparameterized(kind))
        }
        (
            _,
            PermissionParameter::StableId(..)
            | PermissionParameter::Lineage
            | PermissionParameter::NamedQuery(..)
            | PermissionParameter::ApplicationRole(..)
            | PermissionParameter::ReactiveOperation(..),
        ) => Err(DurableCodecError::corrupt()),
    }
}

enum PermissionParameter {
    None,
    Lineage,
    StableId(ContractLineage, u32),
    NamedQuery(ContractLineage, QueryModuleHash, QueryOperationName),
    ApplicationRole(ApplicationRoleHash),
    ReactiveOperation(ContractLineage, ReactiveModuleHash, ReactiveOperationName),
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

/// Derives the dedicated secret-field naming extension (ADR-0118): entries
/// exist only for visibility rows whose grants explicitly name secret
/// fields. The base grant message never carries the naming — the ordinary
/// `field_ids` list stays inert for secrets on every reader.
fn secret_extension_from_grant(
    value: &CapabilityGrantV1,
) -> Option<wire::CapabilitySecretGrantExtensionV1> {
    let entries = value
        .field_visibility()
        .iter()
        .filter(|visibility| !visibility.secret_fields().is_empty())
        .map(|visibility| wire::CapabilitySecretVisibilityV1 {
            contract_lineage: visibility.lineage().as_str().to_owned(),
            entity_type_id: visibility.entity_type().get(),
            secret_field_ids: visibility
                .secret_fields()
                .iter()
                .map(|value| value.get())
                .collect(),
        })
        .collect::<Vec<_>>();
    (!entries.is_empty()).then_some(wire::CapabilitySecretGrantExtensionV1 { entries })
}

/// Reapplies the secret-field naming extension onto a decoded grant,
/// rebuilding the named visibility rows through the dedicated constructor.
/// Extension rows that match no visibility entry, duplicate one, or fail
/// the exact sorted-identity re-check are corrupt (fail closed).
fn apply_secret_extension(
    grant: CapabilityGrantV1,
    extension: Option<wire::CapabilitySecretGrantExtensionV1>,
) -> Result<CapabilityGrantV1, DurableCodecError> {
    let Some(extension) = extension else {
        return Ok(grant);
    };
    if extension.entries.is_empty() {
        return Err(DurableCodecError::corrupt());
    }
    let mut visibility = grant.field_visibility().to_vec();
    for entry in extension.entries {
        let lineage =
            ContractLineage::new(entry.contract_lineage).map_err(|_| DurableCodecError::corrupt())?;
        let entity_type =
            EntityTypeId::new(entry.entity_type_id).ok_or_else(DurableCodecError::corrupt)?;
        let raw_secret = entry
            .secret_field_ids
            .into_iter()
            .map(field_id)
            .collect::<Result<Vec<FieldId>, _>>()?;
        if raw_secret.is_empty() {
            return Err(DurableCodecError::corrupt());
        }
        let position = visibility
            .iter()
            .position(|candidate| {
                candidate.lineage() == &lineage && candidate.entity_type() == entity_type
            })
            .ok_or_else(DurableCodecError::corrupt)?;
        if !visibility[position].secret_fields().is_empty() {
            return Err(DurableCodecError::corrupt());
        }
        let checked = grant_result(EntityFieldVisibilityV1::with_secret_fields(
            lineage,
            entity_type,
            visibility[position].fields().to_vec(),
            raw_secret.clone(),
        ))?;
        if checked.secret_fields() != raw_secret {
            return Err(DurableCodecError::corrupt());
        }
        visibility[position] = checked;
    }
    grant_result(CapabilityGrantV1::new(
        grant.tenant_scope().clone(),
        grant.partition_scope().clone(),
        grant.permissions().clone(),
        visibility,
        grant.max_scan_rows(),
        grant.approval_required().to_vec(),
    ))
}

fn grant_to_proto_with_extensions(
    value: &CapabilityGrantV1,
) -> (
    wire::CapabilityGrantV1,
    Option<wire::CapabilityMigrationGrantExtensionV1>,
    Option<wire::CapabilityInstallationGrantExtensionV1>,
    Option<wire::CapabilityRowPolicyGrantExtensionV1>,
    Option<wire::CapabilityExportGrantExtensionV1>,
) {
    let mut legacy = grant_to_proto(value);
    let lineages = value
        .permissions()
        .as_slice()
        .iter()
        .filter_map(|permission| match permission {
            CapabilityPermissionV1::MigrateContract(lineage) => Some(lineage.as_str().to_owned()),
            _ => None,
        })
        .collect::<Vec<_>>();
    if let Some(permissions) = legacy.permissions.as_mut() {
        permissions.values.retain(|permission| {
            permission.kind != i32::from(CapabilityPermissionKindV1::MigrateContract.tag())
        });
    }
    let approval_required = value
        .approval_required()
        .contains(&CapabilityPermissionKindV1::MigrateContract);
    legacy
        .approval_required
        .retain(|kind| *kind != i32::from(CapabilityPermissionKindV1::MigrateContract.tag()));
    let extension = (!lineages.is_empty()).then_some(wire::CapabilityMigrationGrantExtensionV1 {
        contract_lineages: lineages,
        approval_required,
    });
    let installation_lineages = value
        .permissions()
        .as_slice()
        .iter()
        .filter_map(|permission| match permission {
            CapabilityPermissionV1::InstallApplication(lineage) => {
                Some(lineage.as_str().to_owned())
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    if let Some(permissions) = legacy.permissions.as_mut() {
        permissions.values.retain(|permission| {
            permission.kind != i32::from(CapabilityPermissionKindV1::InstallApplication.tag())
        });
    }
    let installation_approval_required = value
        .approval_required()
        .contains(&CapabilityPermissionKindV1::InstallApplication);
    legacy
        .approval_required
        .retain(|kind| *kind != i32::from(CapabilityPermissionKindV1::InstallApplication.tag()));
    let installation = (!installation_lineages.is_empty()).then_some(
        wire::CapabilityInstallationGrantExtensionV1 {
            contract_lineages: installation_lineages,
            approval_required: installation_approval_required,
        },
    );
    let row_policy =
        value
            .internal_row_policy()
            .map(|extension| wire::CapabilityRowPolicyGrantExtensionV1 {
                application_role_hash: extension.application_role_hash().as_bytes().to_vec(),
                canonical_principal_facts: extension
                    .internal_principal_facts()
                    .internal_canonical_bytes()
                    .to_vec(),
                policies: extension
                    .bindings()
                    .iter()
                    .map(|binding| wire::CapabilityRowPolicyBindingV1 {
                        contract_lineage: binding.lineage().as_str().to_owned(),
                        policy_name: binding.policy_name().as_str().to_owned(),
                        entity_type_id: binding.entity_type().get(),
                        operations: binding
                            .operations()
                            .iter()
                            .map(|operation| i32::from(operation.tag()))
                            .collect(),
                    })
                    .collect(),
            });
    let export = value
        .internal_export()
        .map(|extension| wire::CapabilityExportGrantExtensionV1 {
            applications: extension
                .applications()
                .iter()
                .map(|grant| wire::CapabilityApplicationExportGrantV1 {
                    contract_lineage: grant.lineage().as_str().to_owned(),
                    scope: i32::from(grant.scope().tag()),
                    entities: grant.entities(),
                    events: grant.events(),
                    provenance: grant.provenance(),
                    public_audit: grant.public_audit(),
                })
                .collect(),
        });
    (legacy, extension, installation, row_policy, export)
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

fn grant_from_proto_with_migration_extension(
    value: wire::CapabilityGrantV1,
    extension: wire::CapabilityMigrationGrantExtensionV1,
) -> Result<CapabilityGrantV1, DurableCodecError> {
    if extension.contract_lineages.is_empty() {
        return Err(DurableCodecError::corrupt());
    }
    let base = grant_from_proto(value)?;
    let mut lineages = extension
        .contract_lineages
        .into_iter()
        .map(|lineage| ContractLineage::new(lineage).map_err(|_| DurableCodecError::corrupt()))
        .collect::<Result<Vec<_>, _>>()?;
    if !lineages.windows(2).all(|pair| pair[0] < pair[1]) {
        return Err(DurableCodecError::corrupt());
    }
    let mut permissions = base.permissions().as_slice().to_vec();
    permissions.extend(
        lineages
            .drain(..)
            .map(CapabilityPermissionV1::MigrateContract),
    );
    let permissions = grant_result(CapabilityPermissionsV1::new(permissions))?;
    let mut approval_required = base.approval_required().to_vec();
    if extension.approval_required {
        approval_required.push(CapabilityPermissionKindV1::MigrateContract);
    }
    grant_result(CapabilityGrantV1::new(
        base.tenant_scope().clone(),
        base.partition_scope().clone(),
        permissions,
        base.field_visibility().to_vec(),
        base.max_scan_rows(),
        approval_required,
    ))
}

fn grant_from_proto_with_extensions(
    value: wire::CapabilityGrantV1,
    migration: Option<wire::CapabilityMigrationGrantExtensionV1>,
    installation: Option<wire::CapabilityInstallationGrantExtensionV1>,
) -> Result<CapabilityGrantV1, DurableCodecError> {
    if migration.is_none() && installation.is_none() {
        return Err(DurableCodecError::corrupt());
    }
    let base = match migration {
        Some(extension) => grant_from_proto_with_migration_extension(value, extension)?,
        None => grant_from_proto(value)?,
    };
    let Some(extension) = installation else {
        return Ok(base);
    };
    if extension.contract_lineages.is_empty() {
        return Err(DurableCodecError::corrupt());
    }
    let mut lineages = extension
        .contract_lineages
        .into_iter()
        .map(|lineage| ContractLineage::new(lineage).map_err(|_| DurableCodecError::corrupt()))
        .collect::<Result<Vec<_>, _>>()?;
    if !lineages.windows(2).all(|pair| pair[0] < pair[1]) {
        return Err(DurableCodecError::corrupt());
    }
    let mut permissions = base.permissions().as_slice().to_vec();
    permissions.extend(
        lineages
            .drain(..)
            .map(CapabilityPermissionV1::InstallApplication),
    );
    let permissions = grant_result(CapabilityPermissionsV1::new(permissions))?;
    let mut approval_required = base.approval_required().to_vec();
    if extension.approval_required {
        approval_required.push(CapabilityPermissionKindV1::InstallApplication);
    }
    grant_result(CapabilityGrantV1::new(
        base.tenant_scope().clone(),
        base.partition_scope().clone(),
        permissions,
        base.field_visibility().to_vec(),
        base.max_scan_rows(),
        approval_required,
    ))
}

fn grant_from_proto_with_row_policy(
    value: wire::CapabilityGrantV1,
    migration: Option<wire::CapabilityMigrationGrantExtensionV1>,
    installation: Option<wire::CapabilityInstallationGrantExtensionV1>,
    row_policy: wire::CapabilityRowPolicyGrantExtensionV1,
) -> Result<CapabilityGrantV1, DurableCodecError> {
    let base = match (migration, installation) {
        (None, None) => grant_from_proto(value)?,
        (migration, installation) => {
            grant_from_proto_with_extensions(value, migration, installation)?
        }
    };
    let role_hash = ApplicationRoleHash::from_bytes(fixed(row_policy.application_role_hash)?);
    let principal_facts =
        CapabilityPrincipalFactsV1::decode_canonical(&row_policy.canonical_principal_facts)
            .map_err(|_| DurableCodecError::corrupt())?;
    if row_policy.policies.is_empty() {
        return Err(DurableCodecError::corrupt());
    }
    let raw_bindings = row_policy
        .policies
        .into_iter()
        .map(|binding| {
            let raw_operations = binding
                .operations
                .into_iter()
                .map(|operation| {
                    u8::try_from(operation)
                        .ok()
                        .and_then(CapabilityRowPolicyOperationV1::from_tag)
                        .ok_or_else(DurableCodecError::corrupt)
                })
                .collect::<Result<Vec<_>, _>>()?;
            if raw_operations.is_empty() || !raw_operations.windows(2).all(|pair| pair[0] < pair[1])
            {
                return Err(DurableCodecError::corrupt());
            }
            grant_result(CapabilityRowPolicyBindingV1::new(
                ContractLineage::new(binding.contract_lineage)
                    .map_err(|_| DurableCodecError::corrupt())?,
                RowPolicyName::new(binding.policy_name)
                    .map_err(|_| DurableCodecError::corrupt())?,
                EntityTypeId::new(binding.entity_type_id).ok_or_else(DurableCodecError::corrupt)?,
                raw_operations,
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let extension = grant_result(CapabilityRowPolicyGrantV1::new(
        role_hash,
        principal_facts,
        raw_bindings.clone(),
    ))?;
    if extension.bindings() != raw_bindings {
        return Err(DurableCodecError::corrupt());
    }
    grant_result(base.with_row_policy(extension))
}

fn grant_from_proto_with_export(
    value: wire::CapabilityGrantV1,
    migration: Option<wire::CapabilityMigrationGrantExtensionV1>,
    installation: Option<wire::CapabilityInstallationGrantExtensionV1>,
    row_policy: Option<wire::CapabilityRowPolicyGrantExtensionV1>,
    export: wire::CapabilityExportGrantExtensionV1,
) -> Result<CapabilityGrantV1, DurableCodecError> {
    let base = match row_policy {
        Some(row_policy) => {
            grant_from_proto_with_row_policy(value, migration, installation, row_policy)?
        }
        None => match (migration, installation) {
            (None, None) => grant_from_proto(value)?,
            (migration, installation) => {
                grant_from_proto_with_extensions(value, migration, installation)?
            }
        },
    };
    if export.applications.is_empty() {
        return Err(DurableCodecError::corrupt());
    }
    let raw = export
        .applications
        .into_iter()
        .map(|application| {
            grant_result(CapabilityApplicationExportGrantV1::new(
                ContractLineage::new(application.contract_lineage)
                    .map_err(|_| DurableCodecError::corrupt())?,
                u8::try_from(application.scope)
                    .ok()
                    .and_then(CapabilityApplicationExportScopeV1::from_tag)
                    .ok_or_else(DurableCodecError::corrupt)?,
                application.entities,
                application.events,
                application.provenance,
                application.public_audit,
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let extension = grant_result(CapabilityExportGrantV1::new(raw.clone()))?;
    if extension.applications() != raw {
        return Err(DurableCodecError::corrupt());
    }
    grant_result(base.with_export(extension))
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
    let (grant, migration, installation, row_policy, export) =
        grant_to_proto_with_extensions(value.grant());
    let base = wire::CapabilityRecordV1 {
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
        grant: Some(grant),
        lifecycle: Some(lifecycle_to_proto(value.lifecycle())),
    };
    // ADR-0118: any explicit secret-field naming rides the dedicated V6
    // extension; the base grant message never carries it.
    if let Some(secret) = secret_extension_from_grant(value.grant()) {
        return encode_message(
            RECORD_V6,
            &wire::CapabilityRecordV6 {
                base: Some(base),
                migration,
                installation,
                row_policy,
                export,
                secret: Some(secret),
            },
        );
    }
    match (migration, installation, row_policy, export) {
        (migration, installation, row_policy, Some(export)) => encode_message(
            RECORD_V5,
            &wire::CapabilityRecordV5 {
                base: Some(base),
                migration,
                installation,
                row_policy,
                export: Some(export),
            },
        ),
        (migration, installation, Some(row_policy), None) => encode_message(
            RECORD_V4,
            &wire::CapabilityRecordV4 {
                base: Some(base),
                migration,
                installation,
                row_policy: Some(row_policy),
            },
        ),
        (None, None, None, None) => encode_message(RECORD, &base),
        (Some(migration), None, None, None) => encode_message(
            RECORD_V2,
            &wire::CapabilityRecordV2 {
                base: Some(base),
                migration: Some(migration),
            },
        ),
        (migration, Some(installation), None, None) => encode_message(
            RECORD_V3,
            &wire::CapabilityRecordV3 {
                base: Some(base),
                migration,
                installation: Some(installation),
            },
        ),
    }
}

/// Decodes one complete durable capability record.
pub fn decode_capability_record_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredCapabilityRecordV1>, DurableCodecError> {
    let decoded = readable_record_registry()
        .decode(encoded)
        .map_err(DurableCodecError::from_decode_envelope)?;
    let record = match decoded.record_type() {
        RECORD => {
            let value = wire::CapabilityRecordV1::decode(decoded.payload())
                .map_err(|_| DurableCodecError::corrupt())?;
            record_from_proto(value, None, None, None, None)?
        }
        RECORD_V2 => {
            let value = wire::CapabilityRecordV3::decode(decoded.payload())
                .map_err(|_| DurableCodecError::corrupt())?;
            if value.installation.is_some() {
                record_from_proto(
                    require(value.base)?,
                    value.migration,
                    value.installation,
                    None,
                    None,
                )?
            } else {
                record_from_proto(
                    require(value.base)?,
                    Some(require(value.migration)?),
                    None,
                    None,
                    None,
                )?
            }
        }
        RECORD_V3 => {
            let value = wire::CapabilityRecordV3::decode(decoded.payload())
                .map_err(|_| DurableCodecError::corrupt())?;
            record_from_proto(
                require(value.base)?,
                value.migration,
                Some(require(value.installation)?),
                None,
                None,
            )?
        }
        RECORD_V4 => {
            let value = wire::CapabilityRecordV4::decode(decoded.payload())
                .map_err(|_| DurableCodecError::corrupt())?;
            record_from_proto(
                require(value.base)?,
                value.migration,
                value.installation,
                Some(require(value.row_policy)?),
                None,
            )?
        }
        RECORD_V5 => {
            let value = wire::CapabilityRecordV5::decode(decoded.payload())
                .map_err(|_| DurableCodecError::corrupt())?;
            record_from_proto(
                require(value.base)?,
                value.migration,
                value.installation,
                value.row_policy,
                Some(require(value.export)?),
            )?
        }
        RECORD_V6 => {
            let value = wire::CapabilityRecordV6::decode(decoded.payload())
                .map_err(|_| DurableCodecError::corrupt())?;
            let secret = Some(require(value.secret)?);
            let record = record_from_proto(
                require(value.base)?,
                value.migration,
                value.installation,
                value.row_policy,
                value.export,
            )?;
            let grant = apply_secret_extension(record.grant().clone(), secret)?;
            StoredCapabilityRecordV1::from_stored_parts(
                record.capability_id(),
                record.revision(),
                record.token_digest().clone(),
                record.database_id(),
                record.environment().clone(),
                record.principal_id().clone(),
                record.actor_kind(),
                record.audiences().to_vec(),
                record.issued_at(),
                record.expires_at(),
                record.creation_sequence(),
                record.creation_request_id().clone(),
                grant,
                record.lifecycle().clone(),
            )
            .map_err(|_| DurableCodecError::corrupt())?
        }
        _ => {
            return Err(DurableCodecError::new(
                super::DurableCodecErrorKind::UnexpectedRecordType,
            ));
        }
    };
    let charge =
        crate::EncodedContentCharge::new(encoded.len()).ok_or_else(DurableCodecError::corrupt)?;
    Ok(EncodedPageItem::new(record, charge))
}

fn record_from_proto(
    value: wire::CapabilityRecordV1,
    migration: Option<wire::CapabilityMigrationGrantExtensionV1>,
    installation: Option<wire::CapabilityInstallationGrantExtensionV1>,
    row_policy: Option<wire::CapabilityRowPolicyGrantExtensionV1>,
    export: Option<wire::CapabilityExportGrantExtensionV1>,
) -> Result<StoredCapabilityRecordV1, DurableCodecError> {
    let grant = match export {
        Some(export) => grant_from_proto_with_export(
            require(value.grant.clone())?,
            migration,
            installation,
            row_policy,
            export,
        )?,
        None => match row_policy {
            Some(row_policy) => grant_from_proto_with_row_policy(
                require(value.grant.clone())?,
                migration,
                installation,
                row_policy,
            )?,
            None => match (migration, installation) {
                (None, None) => grant_from_proto(require(value.grant.clone())?)?,
                (migration, installation) => grant_from_proto_with_extensions(
                    require(value.grant.clone())?,
                    migration,
                    installation,
                )?,
            },
        },
    };
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
        grant,
        lifecycle_from_proto(require(value.lifecycle)?)?,
    ))
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

    #[test]
    fn exact_reactive_permissions_round_trip_durably() {
        let lineage = ContractLineage::new("ticketdesk").expect("lineage");
        let module = ReactiveModuleHash::from_bytes([0x43; 32]);
        let name = ReactiveOperationName::new("TicketActivity").expect("operation");
        for permission in [
            CapabilityPermissionV1::ConsumeEventStream(lineage.clone(), module, name.clone()),
            CapabilityPermissionV1::SeekEventStreamConsumer(lineage.clone(), module, name.clone()),
            CapabilityPermissionV1::WatchNamedQuery(lineage.clone(), module, name.clone()),
            CapabilityPermissionV1::ConsumeContextualSubscription(
                lineage.clone(),
                module,
                name.clone(),
            ),
        ] {
            assert_eq!(
                permission_from_proto(permission_to_proto(&permission)).expect("round trip"),
                permission
            );
        }
    }
}
