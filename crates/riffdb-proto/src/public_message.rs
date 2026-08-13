//! Bounded, context-free validation for the completed public API messages.

use std::cmp::Ordering;
use std::error::Error;
use std::fmt;

use prost::Message;
use riffdb_errors::ApplicationOperation;
use riffdb_types::{
    AgentSessionId, ApplicationExportClassV1, ApplicationExportOperationId,
    ApplicationInstallationCampaignId, ApplicationRoleHash, Audience, BackupNameV1,
    CapabilityApplicationExportGrantV1, CapabilityApplicationExportScopeV1,
    CapabilityExportGrantV1, CapabilityId, CapabilityPrincipalFactV1, CapabilityPrincipalFactsV1,
    CapabilityRowPolicyBindingV1, CapabilityRowPolicyGrantV1, CapabilityRowPolicyOperationV1,
    ContractLineage, ContractMigrationOperationId, DatabaseId, EntityKey, EntityTypeId,
    EventConsumerName, IndexEntryKey, MAX_ACTOR_ID_BYTES, MAX_APPLICATION_EXPORT_MODULES,
    MAX_CAPABILITY_APPLICATION_EXPORT_GRANTS, MAX_CAPABILITY_AUDIENCES,
    MAX_CAPABILITY_FIELD_VISIBILITY, MAX_CAPABILITY_LIFETIME_SECONDS, MAX_CAPABILITY_PARTITIONS,
    MAX_CAPABILITY_PAYLOAD_BYTES, MAX_CAPABILITY_PERMISSIONS, MAX_CAPABILITY_ROW_POLICY_BINDINGS,
    MAX_COMMAND_CONFLICT_KEYS_V1, MAX_CONTRACT_LINEAGE_BYTES, MAX_IDEMPOTENCY_KEY_BYTES,
    MAX_KEY_BYTES, MAX_PRINCIPAL_FACTS_V1, MAX_PROJECTION_GROUP_COMPONENTS, MAX_TENANT_ID_BYTES,
    OfflineMaintenanceOperationId, PartitionKey, ProvenanceId, RequestId, RowPolicyName, Timestamp,
    canonical_application_export_page_preimage, hash_application_export_manifest,
    hash_application_export_page, hash_application_export_receipt,
    hash_application_installation_plan, hash_application_installation_receipt, hash_schema,
    offline_maintenance_input_hash,
};

use crate::command::validate_provenance_uri;
use crate::v1;
use crate::value::{
    MAX_PROTOCOL_NAME_BYTES, canonical_value_from_proto, validate_value, validate_value_record,
};
use crate::wire::{self, Cursor, PreflightError};

/// Exact maximum encoded size of one public request.
pub const MAX_PUBLIC_REQUEST_BYTES: usize = 1_048_576;
/// Exact maximum encoded size of one artifact-carrying migration request.
pub const MAX_CONTRACT_MIGRATION_REQUEST_BYTES: usize = 32 * 1_024 * 1_024;
/// Exact maximum encoded size of one canonical application installation request.
///
/// The additional bounded allowance carries at most 256 exact seed receipts;
/// it is not available to the canonical plan itself.
pub const MAX_APPLICATION_INSTALLATION_REQUEST_BYTES: usize = 4 * 1_024 * 1_024 + 128 * 1_024;
/// Exact maximum encoded size of one public unary response or stream item.
pub const MAX_PUBLIC_RESPONSE_BYTES: usize = 4_194_304;

const MAX_PAGE_ITEMS: usize = 500;
const MAX_FIELD_SELECTION_ITEMS: usize = 1_024;
const MAX_COMMIT_COLLECTION_ITEMS: usize = 4_096;
const MAX_DIAGNOSTICS: usize = 32;
const MAX_EXPECTED_TOKENS: usize = 16;
const MAX_BUILD_FEATURES: usize = 64;
const MAX_BUILD_STRING_BYTES: usize = 128;
const MAX_COMMAND_EXPLAIN_ITEMS: usize = 4_096;
const MAX_COMMAND_BATCH_ITEMS: usize = 16;
const MAX_DISCOVERY_PAGE_BYTES: usize = 2_621_440;
const MAX_OPERATION_SCHEMA_BYTES: usize = 65_536;
const MAX_INSTALLATION_DRIVERS: usize = 4;
const MAX_INSTALLATION_SEEDS: usize = 256;
const MAX_INSTALLATION_SYMBOL_BYTES: usize = 256;
const MAX_APPLICATION_EXPORT_JSON_LINE_BYTES: usize = 64 * 1024;
const MAX_APPLICATION_EXPORT_PAGE_ROWS: usize = 500;
const MAX_APPLICATION_EXPORT_PAGE_BYTES: usize = 4 * 1024 * 1024;
const MAX_APPLICATION_EXPORT_TERMINAL_DOCUMENT_BYTES: usize = 256 * 1024;
const MAX_APPLICATION_EXPORT_CURSOR_BYTES: usize = 512;
const MIN_APPLICATION_EXPORT_LEASE_SECONDS: u32 = 60;
const MAX_APPLICATION_EXPORT_LEASE_SECONDS: u32 = 24 * 60 * 60;
const MAX_PROVENANCE_LINKS: usize = 4_096;
const MAX_SOURCE_REPOSITORY_BYTES: usize = 512;
const MAX_SOURCE_COMMIT_BYTES: usize = 128;
const MAX_PROVENANCE_REASON_BYTES: usize = 1_024;
const MAX_APPROVAL_ID_BYTES: usize = 256;
const MAX_MCP_COMMAND_TOOL_NAME_BYTES: usize = 128;
const MAX_PROJECTION_WAIT_NANOS: u64 = 30_000_000_000;
const MAX_SUBSCRIPTION_LIFETIME_NANOS: u64 = 900_000_000_000;
const JSON_SCHEMA_DIALECT: &str = "https://json-schema.org/draft/2020-12/schema";
const OPERATION_ENVELOPE_SCHEMA_ID: &str = "riffdb.command-operation-envelope/v1";
const GET_OUTCOME_RESULT_SCHEMA_ID: &str = "riffdb.command-get-outcome-result/v1";
const OPERATION_ENVELOPE_SCHEMA_HASH: &str =
    "1f83b878c052f53c6eb733b67cd7926fb7ea3729f1d469e0fd9aa8a13f6b44d2";
const GET_OUTCOME_RESULT_SCHEMA_HASH: &str =
    "0c1f33fbc613b9e87c4a54ccc2f7c1d426625cc0cb98237e2bded4bec073ddde";

type DiagnosticRegistryEntry = (&'static str, Option<&'static str>);
type DiagnosticRegistry = fn(&str) -> Option<DiagnosticRegistryEntry>;

const EXPECTED_TOKEN_NAMES: &[&str] = &[
    "contract",
    "version",
    "entity",
    "key",
    "field",
    "invariant",
    "index",
    "event",
    "enum",
    "aggregate",
    "root",
    "child",
    "partition_by",
    "conflict_key",
    "projection",
    "source",
    "where",
    "measure",
    "count",
    "sum",
    "frontier",
    "transactionally_ordered",
    "command",
    "input",
    "idempotency_key",
    "read",
    "mutate",
    "create",
    "as",
    "else",
    "require",
    "set",
    "emit",
    "return",
    "bool",
    "i64",
    "u64",
    "timestamp",
    "date",
    "uuid",
    "decimal",
    "money",
    "string",
    "bytes",
    "optional",
    "list",
    "true",
    "false",
    "null",
    "{",
    "}",
    "(",
    ")",
    "<=",
    ">=",
    "==",
    "!=",
    "&&",
    "||",
    "<",
    ">",
    ",",
    ":",
    ".",
    "=",
    "!",
    "-",
    "*",
    "/",
    "+",
    "fixed decimal literal",
    "unsigned integer literal",
    "string literal",
    "identifier",
];

/// A public Protobuf message with one closed, bounded structural contract.
///
/// Implementations deliberately perform only context-free validation. In
/// particular, typed key components and compiled names remain owned by the
/// selected validated contract bundle in the API-neutral service.
pub trait PublicMessage: Message + Default + Sized {
    /// Maximum accepted encoded length for this message family.
    const MAX_ENCODED_BYTES: usize;

    /// Application operation for the symbolic error boundary, when applicable.
    const APPLICATION_OPERATION: Option<ApplicationOperation> = None;

    #[doc(hidden)]
    fn preflight(input: &[u8]) -> Result<(), PublicWireError>;

    #[doc(hidden)]
    fn validate_structure(&self) -> Result<(), PublicWireError>;
}

/// Decodes one bounded public message through its context-free validator.
pub fn decode_public_message<M: PublicMessage>(input: &[u8]) -> Result<M, PublicWireError> {
    if input.len() > M::MAX_ENCODED_BYTES {
        return Err(PublicWireError::MessageTooLarge);
    }
    M::preflight(input)?;
    let message = M::decode(input).map_err(|_| PublicWireError::MalformedEncoding)?;
    validate_public_message(&message)?;
    Ok(message)
}

/// Validates an already decoded public message without service or catalog context.
pub fn validate_public_message<M: PublicMessage>(message: &M) -> Result<(), PublicWireError> {
    validate_public_message_encoded_len(message).map(|_| ())
}

/// Validates structure and bound, returning the computed encoded length so a
/// subsequent encode can reuse it instead of walking the message again.
pub fn validate_public_message_encoded_len<M: PublicMessage>(
    message: &M,
) -> Result<usize, PublicWireError> {
    message.validate_structure()?;
    let encoded_len = message.encoded_len();
    if encoded_len > M::MAX_ENCODED_BYTES {
        return Err(PublicWireError::MessageTooLarge);
    }
    Ok(encoded_len)
}

/// Validates the request/response relation that is not carried in either
/// capability-create message alone.
pub fn validate_create_capability_exchange(
    request: &v1::CreateCapabilityRequest,
    response: &v1::CreateCapabilityResponse,
) -> Result<(), PublicWireError> {
    validate_public_message(request)?;
    validate_public_message(response)?;
    let normal_request = request.mode == v1::CapabilityCreateMode::Normal as i32;
    let normal_response = matches!(
        response.result,
        Some(v1::create_capability_response::Result::Normal(_))
    );
    if normal_request != normal_response {
        return Err(PublicWireError::InconsistentFields);
    }
    Ok(())
}

/// Validates create-backup request/response semantic identity.
pub fn validate_create_offline_backup_exchange(
    request: &v1::CreateOfflineBackupRequest,
    response: &v1::CreateOfflineBackupResponse,
) -> Result<(), PublicWireError> {
    validate_public_message(request)?;
    validate_public_message(response)?;
    validate_offline_maintenance_exchange(
        &request.operation_id,
        &request.backup_name,
        riffdb_types::OfflineMaintenanceOperationKind::CreateBackup,
        riffdb_types::OfflineMaintenanceReplacementConfirmation::NotProvided,
        response
            .operation
            .as_ref()
            .ok_or(PublicWireError::MissingRequiredField)?,
    )
}

/// Validates restore-backup request/response semantic identity.
pub fn validate_restore_offline_backup_exchange(
    request: &v1::RestoreOfflineBackupRequest,
    response: &v1::RestoreOfflineBackupResponse,
) -> Result<(), PublicWireError> {
    validate_public_message(request)?;
    validate_public_message(response)?;
    let confirmation = match v1::OfflineMaintenanceReplacementConfirmation::try_from(
        request.replacement_confirmation,
    )
    .map_err(|_| PublicWireError::InvalidEnum)?
    {
        v1::OfflineMaintenanceReplacementConfirmation::Unspecified => {
            riffdb_types::OfflineMaintenanceReplacementConfirmation::NotProvided
        }
        v1::OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget => {
            riffdb_types::OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget
        }
    };
    validate_offline_maintenance_exchange(
        &request.operation_id,
        &request.backup_name,
        riffdb_types::OfflineMaintenanceOperationKind::RestoreBackup,
        confirmation,
        response
            .operation
            .as_ref()
            .ok_or(PublicWireError::MissingRequiredField)?,
    )
}

/// Validates maintenance-poll request/response identity without expanding `not_found`.
pub fn validate_get_offline_maintenance_operation_exchange(
    request: &v1::GetOfflineMaintenanceOperationRequest,
    response: &v1::GetOfflineMaintenanceOperationResponse,
) -> Result<(), PublicWireError> {
    validate_public_message(request)?;
    validate_public_message(response)?;
    if let Some(v1::get_offline_maintenance_operation_response::Result::Found(operation)) =
        &response.result
        && operation.operation_id != request.operation_id
    {
        return Err(PublicWireError::InconsistentFields);
    }
    Ok(())
}

/// Validates migration-check request/response identity and operation kind.
pub fn validate_check_contract_migration_exchange(
    request: &v1::CheckContractMigrationRequest,
    response: &v1::CheckContractMigrationResponse,
) -> Result<(), PublicWireError> {
    validate_public_message(request)?;
    validate_public_message(response)?;
    let operation = response
        .operation
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?;
    if operation.operation_id != request.operation_id
        || operation.kind != v1::ContractMigrationOperationKind::Check as i32
        || response.disposition == v1::ContractMigrationStartDisposition::AlreadyApplied as i32
    {
        return Err(PublicWireError::InconsistentFields);
    }
    Ok(())
}

/// Validates migration-apply request/response identity and confirmed hash.
pub fn validate_apply_contract_migration_exchange(
    request: &v1::ApplyContractMigrationRequest,
    response: &v1::ApplyContractMigrationResponse,
) -> Result<(), PublicWireError> {
    validate_public_message(request)?;
    validate_public_message(response)?;
    let operation = response
        .operation
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?;
    let already_applied =
        response.disposition == v1::ContractMigrationStartDisposition::AlreadyApplied as i32;
    if operation.kind != v1::ContractMigrationOperationKind::Apply as i32
        || operation.migration_bundle_hash != request.confirmed_migration_hash
        || (!already_applied && operation.operation_id != request.operation_id)
    {
        return Err(PublicWireError::InconsistentFields);
    }
    Ok(())
}

/// Validates migration-operation polling without disclosing absent identities.
pub fn validate_get_contract_migration_operation_exchange(
    request: &v1::GetContractMigrationOperationRequest,
    response: &v1::GetContractMigrationOperationResponse,
) -> Result<(), PublicWireError> {
    validate_public_message(request)?;
    validate_public_message(response)?;
    if let Some(v1::get_contract_migration_operation_response::Result::Found(operation)) =
        &response.result
        && operation.operation_id != request.operation_id
    {
        return Err(PublicWireError::InconsistentFields);
    }
    Ok(())
}

/// Validates installation start identity and content-addressed plan/receipt relations.
pub fn validate_start_application_installation_exchange(
    request: &v1::StartApplicationInstallationRequest,
    response: &v1::StartApplicationInstallationResponse,
) -> Result<(), PublicWireError> {
    validate_public_message(request)?;
    validate_public_message(response)?;
    let observation = response
        .observation
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?;
    if observation.campaign_id != request.campaign_id
        || observation.plan_hash.as_slice()
            != hash_application_installation_plan(&request.canonical_plan).as_bytes()
    {
        return Err(PublicWireError::InconsistentFields);
    }
    Ok(())
}

/// Validates installation observation identity without expanding an absent campaign.
pub fn validate_get_application_installation_exchange(
    request: &v1::GetApplicationInstallationRequest,
    response: &v1::GetApplicationInstallationResponse,
) -> Result<(), PublicWireError> {
    validate_public_message(request)?;
    validate_public_message(response)?;
    if let Some(v1::get_application_installation_response::Result::Found(found)) =
        response.result.as_ref()
    {
        let observation = found
            .observation
            .as_ref()
            .ok_or(PublicWireError::MissingRequiredField)?;
        if observation.campaign_id != request.campaign_id {
            return Err(PublicWireError::InconsistentFields);
        }
    }
    Ok(())
}

/// Validates export start identity and terminal/cursor relations.
pub fn validate_start_application_export_exchange(
    request: &v1::StartApplicationExportRequest,
    response: &v1::StartApplicationExportResponse,
) -> Result<(), PublicWireError> {
    validate_public_message(request)?;
    validate_public_message(response)?;
    let operation = response
        .operation
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?;
    if operation.operation_id != request.operation_id
        || operation.selection.as_ref() != request.selection.as_ref()
    {
        return Err(PublicWireError::InconsistentFields);
    }
    Ok(())
}

/// Validates one exact export page against its operation and requested bound.
pub fn validate_get_application_export_page_exchange(
    request: &v1::GetApplicationExportPageRequest,
    response: &v1::GetApplicationExportPageResponse,
) -> Result<(), PublicWireError> {
    validate_public_message(request)?;
    validate_public_message(response)?;
    let page = response
        .page
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?;
    if page.operation_id != request.operation_id
        || page.canonical_json_lines.len()
            > usize::try_from(request.max_rows).map_err(|_| PublicWireError::InvalidValue)?
    {
        return Err(PublicWireError::InconsistentFields);
    }
    Ok(())
}

/// Validates one protected export status observation without expanding absence.
pub fn validate_get_application_export_exchange(
    request: &v1::GetApplicationExportRequest,
    response: &v1::GetApplicationExportResponse,
) -> Result<(), PublicWireError> {
    validate_public_message(request)?;
    validate_public_message(response)?;
    if let Some(v1::get_application_export_response::Result::Found(operation)) = &response.result
        && operation.operation_id != request.operation_id
    {
        return Err(PublicWireError::InconsistentFields);
    }
    Ok(())
}

/// Validates one export cancellation observation without expanding absence.
pub fn validate_cancel_application_export_exchange(
    request: &v1::CancelApplicationExportRequest,
    response: &v1::CancelApplicationExportResponse,
) -> Result<(), PublicWireError> {
    validate_public_message(request)?;
    validate_public_message(response)?;
    if let Some(v1::cancel_application_export_response::Result::Found(operation)) = &response.result
        && operation.operation_id != request.operation_id
    {
        return Err(PublicWireError::InconsistentFields);
    }
    Ok(())
}

fn validate_offline_maintenance_exchange(
    operation_id: &[u8],
    backup_name: &str,
    expected_kind: riffdb_types::OfflineMaintenanceOperationKind,
    confirmation: riffdb_types::OfflineMaintenanceReplacementConfirmation,
    operation: &v1::OfflineMaintenanceOperation,
) -> Result<(), PublicWireError> {
    let checked_name =
        BackupNameV1::new(backup_name.to_owned()).map_err(|_| PublicWireError::InvalidIdentity)?;
    let wire_kind = match expected_kind {
        riffdb_types::OfflineMaintenanceOperationKind::CreateBackup => {
            v1::OfflineMaintenanceOperationKind::CreateBackup
        }
        riffdb_types::OfflineMaintenanceOperationKind::RestoreBackup => {
            v1::OfflineMaintenanceOperationKind::RestoreBackup
        }
    };
    let expected_hash =
        offline_maintenance_input_hash(expected_kind, &checked_name, confirmation).into_bytes();
    if operation.operation_id != operation_id
        || operation.kind != wire_kind as i32
        || operation.backup_name != backup_name
        || operation.input_hash != expected_hash
    {
        return Err(PublicWireError::InconsistentFields);
    }
    Ok(())
}

/// Validates the source-relative relations in one contract-validation exchange.
///
/// Individual response validation freezes the closed diagnostic registry and
/// internally ordered spans. This exchange check additionally proves that every
/// half-open byte span lies within the exact submitted source.
pub fn validate_contract_validation_exchange(
    request: &v1::ValidateContractRequest,
    response: &v1::ValidateContractResponse,
) -> Result<(), PublicWireError> {
    validate_public_message(request)?;
    validate_public_message(response)?;
    let Some(v1::validate_contract_response::Result::Invalid(diagnostics)) = &response.result
    else {
        return Ok(());
    };
    let source_len =
        u32::try_from(request.source.len()).map_err(|_| PublicWireError::InconsistentFields)?;
    let span_in_source = |span: &v1::SourceSpan| {
        if span.end <= source_len {
            Ok(())
        } else {
            Err(PublicWireError::InconsistentFields)
        }
    };
    match diagnostics
        .diagnostics
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        v1::compilation_diagnostics::Diagnostics::Syntax(list) => {
            for diagnostic in &list.diagnostics {
                span_in_source(
                    diagnostic
                        .span
                        .as_ref()
                        .ok_or(PublicWireError::MissingRequiredField)?,
                )?;
            }
        }
        v1::compilation_diagnostics::Diagnostics::Semantic(list) => {
            for diagnostic in &list.diagnostics {
                span_in_source(
                    diagnostic
                        .primary_span
                        .as_ref()
                        .ok_or(PublicWireError::MissingRequiredField)?,
                )?;
                if let Some(span) = &diagnostic.related_span {
                    span_in_source(span)?;
                }
            }
        }
    }
    Ok(())
}

/// Validates the selected-contract relation in one explain exchange.
pub fn validate_explain_command_exchange(
    request: &v1::ExplainCommandRequest,
    response: &v1::ExplainCommandResponse,
) -> Result<(), PublicWireError> {
    validate_public_message(request)?;
    validate_public_message(response)?;
    let Some(v1::contract_selection::Selection::Exact(selected)) = request
        .contract
        .as_ref()
        .and_then(|contract| contract.selection.as_ref())
    else {
        return Ok(());
    };
    let Some(v1::explain_command_response::Result::Found(found)) = response.result.as_ref() else {
        return Ok(());
    };
    let descriptor = found
        .contract
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?;
    if descriptor.contract_lineage != selected.contract_lineage
        || descriptor.contract_version != selected.contract_version
    {
        return Err(PublicWireError::InconsistentFields);
    }
    Ok(())
}

/// Validates the effective page limit in one index-scan exchange.
pub fn validate_scan_index_exchange(
    request: &v1::ScanIndexRequest,
    response: &v1::ScanIndexResponse,
) -> Result<(), PublicWireError> {
    validate_public_message(request)?;
    validate_public_message(response)?;
    let limit = effective_page_limit(request.page.as_ref())?;
    let item_count = response
        .page
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
        .items
        .len();
    validate_effective_page_count(item_count, limit)
}

/// Validates the effective page limit in one projection-query exchange.
pub fn validate_query_projection_exchange(
    request: &v1::QueryProjectionRequest,
    response: &v1::QueryProjectionResponse,
) -> Result<(), PublicWireError> {
    validate_public_message(request)?;
    validate_public_message(response)?;
    let limit = effective_page_limit(request.page.as_ref())?;
    let item_count = match response.result.as_ref() {
        Some(v1::query_projection_response::Result::Ready(ready)) => ready
            .data
            .as_ref()
            .ok_or(PublicWireError::MissingRequiredField)?
            .items
            .len(),
        Some(
            v1::query_projection_response::Result::WaitTimedOut(_)
            | v1::query_projection_response::Result::Degraded(_)
            | v1::query_projection_response::Result::Invalid(_),
        ) => 0,
        None => return Err(PublicWireError::MissingRequiredField),
    };
    validate_effective_page_count(item_count, limit)
}

/// Validates the effective page limit in one commit-scan exchange.
pub fn validate_scan_commits_exchange(
    request: &v1::ScanCommitsRequest,
    response: &v1::ScanCommitsResponse,
) -> Result<(), PublicWireError> {
    validate_public_message(request)?;
    validate_public_message(response)?;
    let limit = effective_page_limit(request.page.as_ref())?;
    let item_count = response
        .page
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
        .items
        .len();
    validate_effective_page_count(item_count, limit)
}

/// Validates request/response identity relations for an exact contract lookup.
pub fn validate_get_contract_version_exchange(
    request: &v1::GetContractVersionRequest,
    response: &v1::GetContractVersionResponse,
) -> Result<(), PublicWireError> {
    validate_public_message(request)?;
    validate_public_message(response)?;
    if let Some(v1::get_contract_version_response::Result::Found(found)) = &response.result
        && (found.contract_lineage != request.contract_lineage
            || found.contract_version != request.contract_version)
    {
        return Err(PublicWireError::InconsistentFields);
    }
    Ok(())
}

/// Validates request/response identity relations for a projection-status lookup.
pub fn validate_get_projection_status_exchange(
    request: &v1::GetProjectionStatusRequest,
    response: &v1::GetProjectionStatusResponse,
) -> Result<(), PublicWireError> {
    validate_public_message(request)?;
    validate_public_message(response)?;
    let Some(v1::get_projection_status_response::Result::Found(found)) = &response.result else {
        return Ok(());
    };
    let identity = found
        .identity
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?;
    if identity.projection_id != request.projection_id {
        return Err(PublicWireError::InconsistentFields);
    }
    if let Some(v1::contract_selection::Selection::Exact(exact)) = request
        .contract
        .as_ref()
        .and_then(|contract| contract.selection.as_ref())
        && identity.contract_lineage != exact.contract_lineage
    {
        return Err(PublicWireError::InconsistentFields);
    }
    Ok(())
}

/// Validates request/response selector relations for one provenance trace.
pub fn validate_trace_provenance_exchange(
    request: &v1::TraceProvenanceRequest,
    response: &v1::TraceProvenanceResponse,
) -> Result<(), PublicWireError> {
    validate_public_message(request)?;
    validate_public_message(response)?;
    let Some(v1::trace_provenance_response::Result::Found(found)) = &response.result else {
        return Ok(());
    };
    let selection = request
        .selector
        .as_ref()
        .and_then(|selector| selector.selection.as_ref())
        .ok_or(PublicWireError::MissingRequiredField)?;
    let matches = match selection {
        v1::provenance_selection::Selection::CommitSequence(sequence) => {
            *sequence == found.commit_sequence
        }
        v1::provenance_selection::Selection::ProvenanceId(id) => id == &found.provenance_id,
    };
    if matches {
        Ok(())
    } else {
        Err(PublicWireError::InconsistentFields)
    }
}

/// Validates the effective page limit for pending-outbox discovery.
pub fn validate_list_pending_outbox_deliveries_exchange(
    request: &v1::ListPendingOutboxDeliveriesRequest,
    response: &v1::ListPendingOutboxDeliveriesResponse,
) -> Result<(), PublicWireError> {
    validate_public_message(request)?;
    validate_public_message(response)?;
    let limit = effective_page_limit(request.page.as_ref())?;
    let count = response
        .page
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
        .items
        .len();
    validate_effective_page_count(count, limit)
}

/// Validates raw-key/locator exclusivity and locator echoing for GetOutcome.
pub fn validate_get_outcome_exchange(
    request: &v1::GetOutcomeRequest,
    response: &v1::GetOutcomeResponse,
) -> Result<(), PublicWireError> {
    validate_public_message(request)?;
    validate_public_message(response)?;
    let Some(request_locator) = request.outcome_uri.as_deref() else {
        return Ok(());
    };
    let Some(v1::get_outcome_response::Result::Found(found)) = &response.result else {
        return Ok(());
    };
    if found.outcome_uri.as_deref() == Some(request_locator) {
        Ok(())
    } else {
        Err(PublicWireError::InconsistentFields)
    }
}

/// Validates input-order preservation for one bounded command batch.
///
/// Prefer `items` length when present (current servers). When `items` is empty,
/// fall back to the legacy `responses` length rule for older servers. Never
/// apply the legacy fallback when `items` is present.
pub fn validate_execute_command_batch_exchange(
    request: &v1::ExecuteCommandBatchRequest,
    response: &v1::ExecuteCommandBatchResponse,
) -> Result<(), PublicWireError> {
    validate_public_message(request)?;
    validate_public_message(response)?;
    if !response.items.is_empty() {
        // Field-1 length/consistency vs items is enforced by validate_structure
        // (via validate_public_message above). Exchange only checks the
        // request/response length relation.
        if request.commands.len() != response.items.len() {
            return Err(PublicWireError::InconsistentFields);
        }
        return Ok(());
    }
    if request.commands.len() != response.responses.len() {
        return Err(PublicWireError::InconsistentFields);
    }
    Ok(())
}

/// Validates representation, fence, and page relations for command discovery.
pub fn validate_discover_command_tools_exchange(
    request: &v1::DiscoverCommandToolsRequest,
    response: &v1::DiscoverCommandToolsResponse,
) -> Result<(), PublicWireError> {
    validate_public_message(request)?;
    validate_public_message(response)?;
    validate_discovery_exchange(
        request.representation,
        request.page.as_ref(),
        request.prior_fence.as_ref(),
        response.result.as_ref().map(|result| match result {
            v1::discover_command_tools_response::Result::CatalogUnchanged(fence) => {
                DiscoveryExchangeResult::Unchanged(fence)
            }
            v1::discover_command_tools_response::Result::Page(page) => {
                DiscoveryExchangeResult::Full(&page.items, &page.observed_fence)
            }
            v1::discover_command_tools_response::Result::CompactPage(page) => {
                DiscoveryExchangeResult::Compact(&page.items, &page.observed_fence)
            }
        }),
    )
}

/// Validates representation, kind, fence, and page relations for resource discovery.
pub fn validate_discover_resources_exchange(
    request: &v1::DiscoverResourcesRequest,
    response: &v1::DiscoverResourcesResponse,
) -> Result<(), PublicWireError> {
    validate_public_message(request)?;
    validate_public_message(response)?;
    let result = response
        .result
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?;
    match result {
        v1::discover_resources_response::Result::Page(page) => {
            validate_resource_kind(request.kind, &page.items)?;
        }
        v1::discover_resources_response::Result::CompactPage(page) => {
            validate_compact_resource_kind(request.kind, &page.items)?;
        }
        v1::discover_resources_response::Result::CatalogUnchanged(_) => {}
    }
    validate_discovery_exchange(
        request.representation,
        request.page.as_ref(),
        request.prior_fence.as_ref(),
        Some(match result {
            v1::discover_resources_response::Result::CatalogUnchanged(fence) => {
                DiscoveryExchangeResult::Unchanged(fence)
            }
            v1::discover_resources_response::Result::Page(page) => {
                DiscoveryExchangeResult::Full(&page.items, &page.observed_fence)
            }
            v1::discover_resources_response::Result::CompactPage(page) => {
                DiscoveryExchangeResult::Compact(&page.items, &page.observed_fence)
            }
        }),
    )
}

enum DiscoveryExchangeResult<'a, T, C> {
    Unchanged(&'a v1::DiscoveryCatalogFence),
    Full(&'a [T], &'a Option<v1::DiscoveryCatalogFence>),
    Compact(&'a [C], &'a Option<v1::DiscoveryCatalogFence>),
}

fn validate_discovery_exchange<T, C>(
    representation: i32,
    page: Option<&v1::PageRequest>,
    prior: Option<&v1::DiscoveryCatalogFence>,
    result: Option<DiscoveryExchangeResult<'_, T, C>>,
) -> Result<(), PublicWireError> {
    let limit = effective_page_limit(page)?;
    let result = result.ok_or(PublicWireError::MissingRequiredField)?;
    match result {
        DiscoveryExchangeResult::Unchanged(fence)
            if representation == v1::DiscoveryRepresentation::CompactObservation as i32
                && prior == Some(fence) =>
        {
            Ok(())
        }
        DiscoveryExchangeResult::Full(items, observed)
            if representation == v1::DiscoveryRepresentation::Full as i32 =>
        {
            validate_effective_page_count(items.len(), limit)?;
            if prior.is_some_and(|prior| observed.as_ref() == Some(prior)) {
                return Err(PublicWireError::InconsistentFields);
            }
            Ok(())
        }
        DiscoveryExchangeResult::Compact(items, observed)
            if representation == v1::DiscoveryRepresentation::CompactObservation as i32 =>
        {
            validate_effective_page_count(items.len(), limit)?;
            if prior.is_some_and(|prior| observed.as_ref() == Some(prior)) {
                return Err(PublicWireError::InconsistentFields);
            }
            Ok(())
        }
        _ => Err(PublicWireError::InconsistentFields),
    }
}

/// A bounded, non-secret structural failure at the public wire boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicWireError {
    /// The encoded message exceeds its request or response ceiling.
    MessageTooLarge,
    /// The Protobuf wire representation is malformed or merges known singular fields.
    MalformedEncoding,
    /// A nested length, recursion depth, or item count exceeds a pre-allocation bound.
    PreflightLimitExceeded,
    /// A required message, oneof, or semantic value is absent.
    MissingRequiredField,
    /// A stable numeric identity or sequence uses its reserved zero sentinel.
    InvalidIdentity,
    /// A system identifier is not an exact network-order UUIDv7.
    InvalidUuidV7,
    /// A closed enum contains its unspecified value or an unknown value.
    InvalidEnum,
    /// A string, hash, cursor, token, or key has an invalid bounded representation.
    InvalidBytes,
    /// A typed key fails context-free purpose/version/owner-envelope validation.
    InvalidKeyEnvelope,
    /// A key envelope disagrees with a separately carried owner identity.
    KeyOwnerMismatch,
    /// A public business value or record is not structurally canonical.
    InvalidValue,
    /// A repeated value exceeds its hard count bound.
    TooManyItems,
    /// A collection is duplicate, out of canonical order, or otherwise noncanonical.
    NonCanonical,
    /// Required fields disagree or form an impossible closed result shape.
    InconsistentFields,
}

impl fmt::Display for PublicWireError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("public Protobuf message failed structural validation")
    }
}

impl Error for PublicWireError {}

fn preflight_result(result: Result<(), PreflightError>) -> Result<(), PublicWireError> {
    match result {
        Ok(()) => Ok(()),
        Err(PreflightError::Malformed) => Err(PublicWireError::MalformedEncoding),
        Err(PreflightError::LimitExceeded) => Err(PublicWireError::PreflightLimitExceeded),
    }
}

fn preflight_root(
    input: &[u8],
    maximum: usize,
    maximum_known_field: u32,
    repeated_fields: &[u32],
    oneof_groups: &[&[u32]],
) -> Result<(), PublicWireError> {
    preflight_result(wire::bounded_message(input, maximum))?;
    let mut seen_fields = [false; 32];
    let mut seen_oneofs = [false; 4];
    let mut cursor = Cursor::new(input);
    while let Some(field) = cursor
        .next()
        .map_err(|_| PublicWireError::MalformedEncoding)?
    {
        if field.number <= maximum_known_field && !repeated_fields.contains(&field.number) {
            let index =
                usize::try_from(field.number).map_err(|_| PublicWireError::MalformedEncoding)?;
            if seen_fields[index] {
                return Err(PublicWireError::MalformedEncoding);
            }
            seen_fields[index] = true;
        }
        for (index, group) in oneof_groups.iter().enumerate() {
            if group.contains(&field.number) {
                if seen_oneofs[index] {
                    return Err(PublicWireError::MalformedEncoding);
                }
                seen_oneofs[index] = true;
            }
        }
    }
    Ok(())
}

fn valid_name(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_PROTOCOL_NAME_BYTES
}

fn valid_bounded_text(value: &str, maximum: usize) -> bool {
    !value.is_empty() && value.len() <= maximum
}

fn valid_ascii(value: &str, maximum: usize) -> bool {
    valid_bounded_text(value, maximum) && value.is_ascii()
}

fn valid_uuid<T>(
    bytes: &[u8],
    constructor: impl FnOnce([u8; 16]) -> Result<T, riffdb_types::UuidV7Error>,
) -> bool {
    bytes
        .try_into()
        .ok()
        .and_then(|bytes| constructor(bytes).ok())
        .is_some()
}

fn request_id(bytes: &[u8]) -> Result<(), PublicWireError> {
    if valid_uuid(bytes, RequestId::from_bytes) {
        Ok(())
    } else {
        Err(PublicWireError::InvalidUuidV7)
    }
}

fn capability_id(bytes: &[u8]) -> Result<(), PublicWireError> {
    if valid_uuid(bytes, CapabilityId::from_bytes) {
        Ok(())
    } else {
        Err(PublicWireError::InvalidUuidV7)
    }
}

fn agent_session_id(bytes: &[u8]) -> Result<(), PublicWireError> {
    if valid_uuid(bytes, AgentSessionId::from_bytes) {
        Ok(())
    } else {
        Err(PublicWireError::InvalidUuidV7)
    }
}

fn provenance_id(bytes: &[u8]) -> Result<(), PublicWireError> {
    if valid_uuid(bytes, ProvenanceId::from_bytes) {
        Ok(())
    } else {
        Err(PublicWireError::InvalidUuidV7)
    }
}

fn hash(bytes: &[u8]) -> Result<(), PublicWireError> {
    if bytes.len() == 32 {
        Ok(())
    } else {
        Err(PublicWireError::InvalidBytes)
    }
}

fn cursor(bytes: &Option<Vec<u8>>) -> Result<(), PublicWireError> {
    if bytes.as_ref().is_none_or(|value| value.len() == 16) {
        Ok(())
    } else {
        Err(PublicWireError::InvalidBytes)
    }
}

fn validate_record(record: Option<&v1::ValueRecord>) -> Result<(), PublicWireError> {
    validate_value_record(record.ok_or(PublicWireError::MissingRequiredField)?)
        .map_err(|_| PublicWireError::InvalidValue)
}

fn validate_timestamp(timestamp: Option<&v1::Timestamp>) -> Result<(), PublicWireError> {
    let timestamp = timestamp.ok_or(PublicWireError::MissingRequiredField)?;
    Timestamp::new(timestamp.seconds, timestamp.nanos)
        .map(|_| ())
        .map_err(|_| PublicWireError::InvalidValue)
}

fn validate_contract_selection(
    selection: Option<&v1::ContractSelection>,
) -> Result<(), PublicWireError> {
    match selection
        .ok_or(PublicWireError::MissingRequiredField)?
        .selection
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        v1::contract_selection::Selection::Active(_) => Ok(()),
        v1::contract_selection::Selection::Exact(exact)
            if valid_bounded_text(&exact.contract_lineage, MAX_CONTRACT_LINEAGE_BYTES)
                && exact.contract_version != 0 =>
        {
            Ok(())
        }
        v1::contract_selection::Selection::Exact(_) => Err(PublicWireError::InvalidIdentity),
    }
}

fn validate_page_request(page: Option<&v1::PageRequest>) -> Result<(), PublicWireError> {
    let page = page.ok_or(PublicWireError::MissingRequiredField)?;
    if !matches!(page.limit, Some(1..=500)) {
        return Err(PublicWireError::InvalidIdentity);
    }
    cursor(&page.cursor)
}

fn effective_page_limit(page: Option<&v1::PageRequest>) -> Result<usize, PublicWireError> {
    validate_page_request(page)?;
    usize::try_from(
        page.and_then(|page| page.limit)
            .ok_or(PublicWireError::MissingRequiredField)?,
    )
    .map_err(|_| PublicWireError::InvalidIdentity)
}

fn validate_effective_page_count(
    item_count: usize,
    effective_limit: usize,
) -> Result<(), PublicWireError> {
    if item_count <= effective_limit {
        Ok(())
    } else {
        Err(PublicWireError::InconsistentFields)
    }
}

fn validate_field_selection(fields: Option<&v1::FieldSelection>) -> Result<(), PublicWireError> {
    let fields = fields.ok_or(PublicWireError::MissingRequiredField)?;
    if fields.field_ids.len() > MAX_FIELD_SELECTION_ITEMS {
        return Err(PublicWireError::TooManyItems);
    }
    strictly_increasing_nonzero(&fields.field_ids)
}

fn strictly_increasing_nonzero(values: &[u32]) -> Result<(), PublicWireError> {
    if values.contains(&0) || values.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(PublicWireError::NonCanonical);
    }
    Ok(())
}

fn validate_frontier(frontier: Option<&v1::FrontierPosition>) -> Result<(), PublicWireError> {
    match frontier
        .ok_or(PublicWireError::MissingRequiredField)?
        .position
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        v1::frontier_position::Position::BeforeFirst(_) => Ok(()),
        v1::frontier_position::Position::AppliedThrough(sequence) if *sequence != 0 => Ok(()),
        v1::frontier_position::Position::AppliedThrough(_) => Err(PublicWireError::InvalidIdentity),
    }
}

fn frontier_value(frontier: &v1::FrontierPosition) -> Result<Option<u64>, PublicWireError> {
    validate_frontier(Some(frontier))?;
    Ok(match frontier.position {
        Some(v1::frontier_position::Position::BeforeFirst(_)) => None,
        Some(v1::frontier_position::Position::AppliedThrough(value)) => Some(value),
        None => return Err(PublicWireError::MissingRequiredField),
    })
}

fn validate_tenant_scope(scope: Option<&v1::TenantScope>) -> Result<(), PublicWireError> {
    match scope
        .ok_or(PublicWireError::MissingRequiredField)?
        .scope
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        v1::tenant_scope::Scope::Global(_) => Ok(()),
        v1::tenant_scope::Scope::TenantId(value)
            if valid_bounded_text(value, MAX_TENANT_ID_BYTES) =>
        {
            Ok(())
        }
        v1::tenant_scope::Scope::TenantId(_) => Err(PublicWireError::InvalidBytes),
    }
}

fn validate_actor(actor: Option<&v1::AdmittedActor>) -> Result<(), PublicWireError> {
    let actor = actor.ok_or(PublicWireError::MissingRequiredField)?;
    if !valid_bounded_text(&actor.principal_id, MAX_ACTOR_ID_BYTES)
        || !matches!(
            v1::ActorKind::try_from(actor.actor_kind),
            Ok(v1::ActorKind::Human | v1::ActorKind::Agent | v1::ActorKind::Service)
        )
    {
        return Err(PublicWireError::InvalidEnum);
    }
    validate_tenant_scope(actor.tenant_scope.as_ref())?;
    if let Some(session) = &actor.agent_session_id {
        agent_session_id(session)?;
    }
    Ok(())
}

fn validate_declared_outcome(outcome: Option<&v1::DeclaredOutcome>) -> Result<(), PublicWireError> {
    let outcome = outcome.ok_or(PublicWireError::MissingRequiredField)?;
    if outcome.outcome_id == 0 || !valid_name(&outcome.outcome_name) {
        return Err(PublicWireError::InvalidIdentity);
    }
    validate_record(outcome.value.as_ref())
}

#[derive(Clone, Copy)]
enum KeyPurpose {
    Entity,
    Index,
    Partition,
}

fn key_envelope(bytes: &[u8], purpose: KeyPurpose) -> Result<u32, PublicWireError> {
    let (purpose_byte, minimum) = match purpose {
        KeyPurpose::Entity => (0x45, 6),
        KeyPurpose::Index => (0x49, 16),
        KeyPurpose::Partition => (0x50, 6),
    };
    if bytes.len() < minimum
        || bytes.len() > MAX_KEY_BYTES
        || bytes[0] != purpose_byte
        || bytes[1] != 0x01
    {
        return Err(PublicWireError::InvalidKeyEnvelope);
    }
    let owner = u32::from_be_bytes([bytes[2], bytes[3], bytes[4], bytes[5]]);
    if owner == 0 {
        return Err(PublicWireError::InvalidKeyEnvelope);
    }
    Ok(owner)
}

fn validate_entity_key(bytes: &[u8], owner: Option<u32>) -> Result<(), PublicWireError> {
    let key =
        EntityKey::from_bytes(bytes.to_vec()).map_err(|_| PublicWireError::InvalidKeyEnvelope)?;
    if owner.is_some_and(|owner| owner != key.entity_type_id().get()) {
        return Err(PublicWireError::KeyOwnerMismatch);
    }
    Ok(())
}

fn validate_index_key(bytes: &[u8]) -> Result<(), PublicWireError> {
    IndexEntryKey::from_bytes(bytes.to_vec())
        .map(|_| ())
        .map_err(|_| PublicWireError::InvalidKeyEnvelope)
}

fn validate_partition_key(bytes: &[u8]) -> Result<(), PublicWireError> {
    PartitionKey::from_bytes(bytes.to_vec())
        .map(|_| ())
        .map_err(|_| PublicWireError::InvalidKeyEnvelope)
}

fn validate_values(values: &[v1::Value], maximum: usize) -> Result<(), PublicWireError> {
    if values.len() > maximum {
        return Err(PublicWireError::TooManyItems);
    }
    for value in values {
        validate_value(value).map_err(|_| PublicWireError::InvalidValue)?;
    }
    Ok(())
}

fn validate_contract_descriptor(
    descriptor: Option<&v1::ContractDescriptor>,
) -> Result<(), PublicWireError> {
    let descriptor = descriptor.ok_or(PublicWireError::MissingRequiredField)?;
    if !valid_bounded_text(&descriptor.contract_lineage, MAX_CONTRACT_LINEAGE_BYTES)
        || descriptor.contract_version == 0
    {
        return Err(PublicWireError::InvalidIdentity);
    }
    hash(&descriptor.bundle_hash)?;
    hash(&descriptor.source_hash)?;
    hash(&descriptor.plan_root_hash)?;
    if let Some(compatibility) = descriptor.compatibility.as_ref() {
        validate_contract_compatibility(compatibility, descriptor.contract_version)?;
    }
    Ok(())
}

fn validate_contract_compatibility(
    compatibility: &v1::ContractCompatibilitySummary,
    contract_version: u64,
) -> Result<(), PublicWireError> {
    let parent = match (
        compatibility.parent_contract_version,
        compatibility.parent_bundle_hash.as_deref(),
    ) {
        (None, None) => None,
        (Some(version), Some(bundle_hash)) if version != 0 && version < contract_version => {
            hash(bundle_hash)?;
            Some(version)
        }
        _ => return Err(PublicWireError::InconsistentFields),
    };
    let overall = v1::ContractCompatibilityClass::try_from(compatibility.overall)
        .map_err(|_| PublicWireError::InvalidEnum)?;
    if overall == v1::ContractCompatibilityClass::Unspecified {
        return Err(PublicWireError::InvalidEnum);
    }
    if compatibility.code_counts.len() > 20 {
        return Err(PublicWireError::TooManyItems);
    }
    if parent.is_none() {
        return if overall == v1::ContractCompatibilityClass::Compatible
            && compatibility.code_counts.is_empty()
        {
            Ok(())
        } else {
            Err(PublicWireError::InconsistentFields)
        };
    }
    if compatibility.code_counts.is_empty() {
        return Err(PublicWireError::InconsistentFields);
    }

    let mut previous = None;
    let mut total = 0_u32;
    let mut derived = v1::ContractCompatibilityClass::Compatible;
    for entry in &compatibility.code_counts {
        if entry.count == 0 || previous.is_some_and(|code: &str| code >= entry.code.as_str()) {
            return Err(PublicWireError::InconsistentFields);
        }
        let class =
            public_compatibility_code_class(&entry.code).ok_or(PublicWireError::InvalidIdentity)?;
        if compatibility_severity(class) > compatibility_severity(derived) {
            derived = class;
        }
        total = total
            .checked_add(entry.count)
            .ok_or(PublicWireError::TooManyItems)?;
        if total > 4_096 {
            return Err(PublicWireError::TooManyItems);
        }
        previous = Some(entry.code.as_str());
    }
    if derived != overall {
        return Err(PublicWireError::InconsistentFields);
    }
    Ok(())
}

fn public_compatibility_code_class(code: &str) -> Option<v1::ContractCompatibilityClass> {
    match code {
        "RDB-K001" | "RDB-K010" | "RDB-K011" | "RDB-K012" | "RDB-K013" | "RDB-K014"
        | "RDB-K015" | "RDB-K016" => Some(v1::ContractCompatibilityClass::Compatible),
        "RDB-K020" | "RDB-K021" | "RDB-K022" => {
            Some(v1::ContractCompatibilityClass::RequiresExplicitVersion)
        }
        "RDB-K030" | "RDB-K031" | "RDB-K032" | "RDB-K033" | "RDB-K034" | "RDB-K035" => {
            Some(v1::ContractCompatibilityClass::RequiresMigration)
        }
        "RDB-K100" | "RDB-K101" | "RDB-K102" | "RDB-K103" | "RDB-K104" | "RDB-K105"
        | "RDB-K106" | "RDB-K107" | "RDB-K108" | "RDB-K109" | "RDB-K110" | "RDB-K111"
        | "RDB-K112" => Some(v1::ContractCompatibilityClass::Incompatible),
        _ => None,
    }
}

const fn compatibility_severity(class: v1::ContractCompatibilityClass) -> u8 {
    match class {
        v1::ContractCompatibilityClass::Unspecified => 0,
        v1::ContractCompatibilityClass::Compatible => 1,
        v1::ContractCompatibilityClass::RequiresExplicitVersion => 2,
        v1::ContractCompatibilityClass::RequiresMigration => 3,
        v1::ContractCompatibilityClass::Incompatible => 4,
    }
}

fn validate_span(span: Option<&v1::SourceSpan>) -> Result<(), PublicWireError> {
    let span = span.ok_or(PublicWireError::MissingRequiredField)?;
    if span.start <= span.end {
        Ok(())
    } else {
        Err(PublicWireError::InconsistentFields)
    }
}

fn syntax_diagnostic_registry(code: &str) -> Option<DiagnosticRegistryEntry> {
    let entry = match code {
        "RDB-S001" => (
            "contract source exceeds the byte limit",
            Some("reduce the contract source to the documented grammar-version-1 bounds"),
        ),
        "RDB-S002" => (
            "contract syntax exceeds the parser node limit",
            Some("reduce the contract source to the documented grammar-version-1 bounds"),
        ),
        "RDB-S003" => (
            "contract source contains an invalid token or literal",
            Some("use the grammar-version-1 spelling shown in the language reference"),
        ),
        "RDB-S004" => (
            "contract source contains an unexpected token",
            Some("use the grammar-version-1 spelling shown in the language reference"),
        ),
        "RDB-S005" => (
            "contract source ended before the declaration was complete",
            Some("use the grammar-version-1 spelling shown in the language reference"),
        ),
        "RDB-S006" => (
            "contract syntax exceeds the nesting limit",
            Some("simplify nested types, expressions, or objects"),
        ),
        "RDB-S007" => (
            "contract declaration contains too many items",
            Some("reduce the contract source to the documented grammar-version-1 bounds"),
        ),
        "RDB-S008" => (
            "contract source uses unsupported grammar syntax",
            Some("remove the deferred construct or use the bounded public query/policy API"),
        ),
        _ => return None,
    };
    Some(entry)
}

#[allow(clippy::too_many_lines)]
fn semantic_diagnostic_registry(code: &str) -> Option<DiagnosticRegistryEntry> {
    let entry = match code {
        "RDB-C001" => (
            "contract version must be a supported nonzero integer",
            Some("use a base-10 application version in 1..=u64::MAX"),
        ),
        "RDB-C002" => (
            "a name is declared more than once in this namespace",
            Some("rename or remove one declaration in the shared namespace"),
        ),
        "RDB-C003" => (
            "a required declaration or singleton item is missing",
            Some("add the required grammar-version-1 declaration"),
        ),
        "RDB-C004" => (
            "a referenced declaration, field, or binding is unknown",
            Some("reference an exact case-sensitive declared name"),
        ),
        "RDB-C005" => (
            "the declared type is invalid or unsupported",
            Some("use a bounded grammar-version-1 value type"),
        ),
        "RDB-C006" => (
            "an expression does not have the required exact type",
            Some("make both sides use the same complete static type"),
        ),
        "RDB-C007" => (
            "the expression is invalid in this context",
            Some("use an expression allowed by this declaration context"),
        ),
        "RDB-C008" => (
            "aggregate ownership or key shape is invalid",
            Some("declare one root and the required root-key prefix ownership"),
        ),
        "RDB-C009" => (
            "the command binding is invalid or ambiguously owned",
            Some("bind an entity owned by the command's one aggregate"),
        ),
        "RDB-C010" => (
            "a mutating command must declare one idempotency key",
            Some("declare a direct bounded string input as idempotency_key"),
        ),
        "RDB-C011" => (
            "the idempotency expression is invalid or used outside its clause",
            Some("use one required string<1..=128> input only in the idempotency clause"),
        ),
        "RDB-C012" => (
            "the create binding does not definitely initialize its record",
            Some("assign every required non-key field exactly once before return"),
        ),
        "RDB-C013" => (
            "the command mutation target is invalid",
            Some("write one declared non-key field through a mutable binding"),
        ),
        "RDB-C014" => (
            "an outcome name or payload shape is invalid",
            Some("use one consistent typed payload for each declared outcome name"),
        ),
        "RDB-C015" => (
            "an event name or payload shape is invalid",
            Some("construct every declared event field with its exact type"),
        ),
        "RDB-C016" => (
            "partition and conflict keys must be computable from validated inputs",
            Some("derive aggregate keys only from root-key inputs and constants"),
        ),
        "RDB-C017" => (
            "all command bindings must be statically colocated in one partition",
            Some("make all bindings use the same structural partition derivation"),
        ),
        "RDB-C018" => (
            "a required relationship must map stored fields to one complete same-partition target key",
            Some("map required non-optional fields to the complete target key in canonical order"),
        ),
        "RDB-C019" => (
            "the projection uses an unsupported or invalid operation",
            Some("use equality/conjunction filters and bounded count or sum aggregation"),
        ),
        "RDB-C020" => (
            "a compiled artifact exceeds a fixed semantic bound",
            Some("reduce declared bounds or the number of schema components"),
        ),
        "RDB-C021" => (
            "stable semantic identifiers cannot be allocated compatibly",
            Some("preserve lineage identities and do not reuse removed identifiers"),
        ),
        "RDB-C022" => (
            "the parent bundle is not a valid predecessor",
            Some("compile against the exact validated predecessor bundle"),
        ),
        "RDB-C023" => (
            "checked executable IR construction rejected the compiled plan",
            None,
        ),
        "RDB-C024" => (
            "a relationship change lacks a dominating exact target binding and missing-target outcome",
            Some(
                "bind the complete referenced key before the mutable binding and declare its failure outcome",
            ),
        ),
        "RDB-C025" => (
            "a unique key must use required fields and begin with the complete partition route",
            Some(
                "declare required key-compatible fields beginning with the canonical partition prefix",
            ),
        ),
        "RDB-C026" => (
            "a changed unique value must be computable from validated command inputs",
            Some(
                "assign every changed unique component from command inputs or input-only expressions",
            ),
        ),
        "RDB-C027" => (
            "migration source does not bind the exact parent and candidate",
            Some("use the exact lineage, parent version, and candidate version"),
        ),
        "RDB-C028" => (
            "a required migration proof is missing",
            Some("add the source clause required by the reported compatibility change"),
        ),
        "RDB-C029" => (
            "a migration change is proved more than once",
            Some("retain exactly one proof for the change"),
        ),
        "RDB-C030" => (
            "a migration clause does not correspond to the exact contract change",
            Some("remove the clause or compile it against the intended exact parent"),
        ),
        "RDB-C031" => (
            "the migration step is not executable in the current implementation gate",
            Some("wait for the documented migration implementation gate"),
        ),
        "RDB-C032" => (
            "the migration expression or conversion is not exact and deterministic",
            Some(
                "use only the old row, canonical literals, checked operators, and closed conversions",
            ),
        ),
        "RDB-C201" => (
            "an identifier cannot form a valid MCP command tool-name segment",
            Some("start contract and command identifiers with an ASCII letter"),
        ),
        "RDB-C202" => (
            "the complete MCP command tool name exceeds 128 bytes",
            Some("shorten the source contract or command identifier"),
        ),
        "RDB-C203" => (
            "two commands normalize to the same MCP command tool name",
            Some("rename one command so lowercase identifiers remain distinct"),
        ),
        _ => return None,
    };
    Some(entry)
}

fn validate_diagnostic_text(
    code: &str,
    summary: &str,
    help: Option<&str>,
    registry: DiagnosticRegistry,
) -> Result<(), PublicWireError> {
    if registry(code).is_some_and(|entry| entry == (summary, help)) {
        Ok(())
    } else {
        Err(PublicWireError::InvalidBytes)
    }
}

fn validate_diagnostics(diagnostics: &v1::CompilationDiagnostics) -> Result<(), PublicWireError> {
    match diagnostics
        .diagnostics
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        v1::compilation_diagnostics::Diagnostics::Syntax(list) => {
            if list.diagnostics.is_empty() || list.diagnostics.len() > MAX_DIAGNOSTICS {
                return Err(PublicWireError::TooManyItems);
            }
            for diagnostic in &list.diagnostics {
                if diagnostic.expected.len() > MAX_EXPECTED_TOKENS
                    || diagnostic
                        .expected
                        .iter()
                        .any(|name| !EXPECTED_TOKEN_NAMES.contains(&name.as_str()))
                    || diagnostic
                        .expected
                        .windows(2)
                        .any(|pair| pair[0] >= pair[1])
                {
                    return Err(PublicWireError::InvalidBytes);
                }
                validate_diagnostic_text(
                    &diagnostic.code,
                    &diagnostic.summary,
                    diagnostic.help.as_deref(),
                    syntax_diagnostic_registry,
                )?;
                validate_span(diagnostic.span.as_ref())?;
            }
        }
        v1::compilation_diagnostics::Diagnostics::Semantic(list) => {
            if list.diagnostics.is_empty() || list.diagnostics.len() > MAX_DIAGNOSTICS {
                return Err(PublicWireError::TooManyItems);
            }
            for diagnostic in &list.diagnostics {
                validate_diagnostic_text(
                    &diagnostic.code,
                    &diagnostic.summary,
                    diagnostic.help.as_deref(),
                    semantic_diagnostic_registry,
                )?;
                validate_span(diagnostic.primary_span.as_ref())?;
                if diagnostic.related_span.is_some() {
                    validate_span(diagnostic.related_span.as_ref())?;
                }
            }
        }
    }
    Ok(())
}

fn validate_schema_artifact(
    artifact: Option<&v1::GeneratedSchemaArtifact>,
) -> Result<(u8, u32), PublicWireError> {
    let artifact = artifact.ok_or(PublicWireError::MissingRequiredField)?;
    let key = artifact
        .key
        .as_ref()
        .and_then(|key| key.artifact.as_ref())
        .ok_or(PublicWireError::MissingRequiredField)?;
    let (kind, owner) = match key {
        v1::schema_artifact_key::Artifact::EntityId(value) => (1, *value),
        v1::schema_artifact_key::Artifact::EventTypeId(value) => (2, *value),
        v1::schema_artifact_key::Artifact::CommandInputId(value) => (3, *value),
        v1::schema_artifact_key::Artifact::CommandOutcomeUnionId(value) => (4, *value),
        v1::schema_artifact_key::Artifact::ProjectionResultId(value) => (5, *value),
    };
    if owner == 0
        || artifact.dialect != JSON_SCHEMA_DIALECT
        || artifact.canonical_json.len() > MAX_PUBLIC_REQUEST_BYTES
    {
        return Err(PublicWireError::InvalidIdentity);
    }
    hash(&artifact.schema_hash)?;
    if artifact.schema_hash.as_slice() != hash_schema(artifact.canonical_json.as_bytes()).as_bytes()
    {
        return Err(PublicWireError::InconsistentFields);
    }
    Ok((kind, owner))
}

fn validate_command_explain(explain: Option<&v1::CommandExplain>) -> Result<(), PublicWireError> {
    let explain = explain.ok_or(PublicWireError::MissingRequiredField)?;
    if explain.command_id == 0 || explain.partition_component_count != 1 {
        return Err(PublicWireError::InvalidIdentity);
    }
    if usize::try_from(explain.conflict_key_count)
        .ok()
        .is_none_or(|count| count > MAX_COMMAND_CONFLICT_KEYS_V1)
    {
        return Err(PublicWireError::TooManyItems);
    }
    if [
        explain.binding_ids.len(),
        explain.read_fields.len(),
        explain.write_fields.len(),
        explain.invariant_ids.len(),
        explain.event_type_ids.len(),
        explain.outcome_ids.len(),
    ]
    .into_iter()
    .any(|count| count > MAX_COMMAND_EXPLAIN_ITEMS)
    {
        return Err(PublicWireError::TooManyItems);
    }
    if explain
        .binding_ids
        .iter()
        .enumerate()
        .any(|(position, binding)| usize::try_from(*binding).ok() != Some(position))
    {
        return Err(PublicWireError::NonCanonical);
    }
    if explain.invariant_ids.contains(&0)
        || explain
            .invariant_ids
            .windows(2)
            .any(|pair| pair[0] > pair[1])
    {
        return Err(PublicWireError::NonCanonical);
    }
    strictly_increasing_nonzero(&explain.outcome_ids)?;
    if explain.event_type_ids.contains(&0) {
        return Err(PublicWireError::InvalidIdentity);
    }
    for fields in [&explain.read_fields, &explain.write_fields] {
        let mut previous = None;
        for field in fields {
            if usize::try_from(field.binding_id)
                .ok()
                .is_none_or(|binding| binding >= explain.binding_ids.len())
                || field.field_id == 0
                || previous.is_some_and(|value| value >= (field.binding_id, field.field_id))
            {
                return Err(PublicWireError::NonCanonical);
            }
            previous = Some((field.binding_id, field.field_id));
        }
    }
    if !matches!(
        v1::ExecutionClass::try_from(explain.execution_class),
        Ok(v1::ExecutionClass::ReadOnly | v1::ExecutionClass::IdempotentMutation)
    ) || explain.rendered_text.len() > MAX_PUBLIC_RESPONSE_BYTES
    {
        return Err(PublicWireError::InvalidEnum);
    }
    Ok(())
}

fn validate_explained_command(command: &v1::ExplainedCommand) -> Result<(), PublicWireError> {
    validate_contract_descriptor(command.contract.as_ref())?;
    if command.command_id == 0 || !valid_mcp_tool_name(&command.tool_name) {
        return Err(PublicWireError::InvalidIdentity);
    }
    hash(&command.plan_hash)?;
    validate_command_explain(command.explanation.as_ref())?;
    let input_owner = validate_schema_artifact(command.input_schema.as_ref())?;
    let outcome_owner = validate_schema_artifact(command.outcome_schema.as_ref())?;
    let explanation = command
        .explanation
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?;
    if explanation.command_id != command.command_id
        || input_owner != (3, command.command_id)
        || outcome_owner != (4, command.command_id)
    {
        return Err(PublicWireError::InconsistentFields);
    }
    Ok(())
}

fn validate_projection_identity(
    identity: Option<&v1::ProjectionIdentity>,
) -> Result<(), PublicWireError> {
    let identity = identity.ok_or(PublicWireError::MissingRequiredField)?;
    if !valid_bounded_text(&identity.contract_lineage, MAX_CONTRACT_LINEAGE_BYTES)
        || identity.projection_id == 0
    {
        return Err(PublicWireError::InvalidIdentity);
    }
    hash(&identity.projection_plan_hash)
}

fn validate_projection_failure_code(value: i32) -> Result<(), PublicWireError> {
    if matches!(
        v1::ProjectionFailureCode::try_from(value),
        Ok(v1::ProjectionFailureCode::ArithmeticOverflow
            | v1::ProjectionFailureCode::MalformedDurableEvent
            | v1::ProjectionFailureCode::MissingCommit
            | v1::ProjectionFailureCode::PlanOrSchemaUnavailable
            | v1::ProjectionFailureCode::ProjectionStateIntegrity
            | v1::ProjectionFailureCode::HardLimitExceeded)
    ) {
        Ok(())
    } else {
        Err(PublicWireError::InvalidEnum)
    }
}

fn validate_validate_contract_request(
    message: &v1::ValidateContractRequest,
) -> Result<(), PublicWireError> {
    request_id(&message.request_id)
}

fn validate_validate_contract_response(
    message: &v1::ValidateContractResponse,
) -> Result<(), PublicWireError> {
    match message
        .result
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        v1::validate_contract_response::Result::Valid(_) => Ok(()),
        v1::validate_contract_response::Result::Invalid(diagnostics) => {
            validate_diagnostics(diagnostics)
        }
        v1::validate_contract_response::Result::Candidate(candidate) => {
            if candidate.parent_version.is_some() != !candidate.parent_bundle_hash.is_empty()
                || candidate.parent_version == Some(0)
                || candidate.canonical_bundle.is_empty()
                || candidate.canonical_bundle.len() > MAX_PUBLIC_RESPONSE_BYTES
            {
                return Err(PublicWireError::InvalidIdentity);
            }
            if !candidate.parent_bundle_hash.is_empty() {
                hash(&candidate.parent_bundle_hash)?;
            }
            validate_contract_descriptor(candidate.candidate.as_ref())
        }
    }
}

fn validate_explain_command_request(
    message: &v1::ExplainCommandRequest,
) -> Result<(), PublicWireError> {
    request_id(&message.request_id)?;
    validate_contract_selection(message.contract.as_ref())?;
    if valid_name(&message.command_name) {
        Ok(())
    } else {
        Err(PublicWireError::InvalidBytes)
    }
}

fn validate_explain_command_response(
    message: &v1::ExplainCommandResponse,
) -> Result<(), PublicWireError> {
    match message
        .result
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        v1::explain_command_response::Result::NotFound(_) => Ok(()),
        v1::explain_command_response::Result::Found(command) => validate_explained_command(command),
    }
}

fn validate_deploy_contract_request(
    message: &v1::DeployContractRequest,
) -> Result<(), PublicWireError> {
    request_id(&message.request_id)?;
    if message.expected_active_version == Some(0) {
        return Err(PublicWireError::InvalidIdentity);
    }
    let exact = !message.expected_candidate_bundle_hash.is_empty();
    if !exact && !message.expected_active_bundle_hash.is_empty() {
        return Err(PublicWireError::InvalidIdentity);
    }
    if exact {
        hash(&message.expected_candidate_bundle_hash)?;
        if message.expected_active_version.is_some()
            != !message.expected_active_bundle_hash.is_empty()
        {
            return Err(PublicWireError::InvalidIdentity);
        }
        if !message.expected_active_bundle_hash.is_empty() {
            hash(&message.expected_active_bundle_hash)?;
        }
    }
    Ok(())
}

fn validate_deploy_contract_response(
    message: &v1::DeployContractResponse,
) -> Result<(), PublicWireError> {
    match message
        .result
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        v1::deploy_contract_response::Result::Activated(descriptor)
        | v1::deploy_contract_response::Result::AlreadyActive(descriptor)
        | v1::deploy_contract_response::Result::IncompatibleCandidate(descriptor)
        | v1::deploy_contract_response::Result::MigrationRequired(descriptor) => {
            validate_contract_descriptor(Some(descriptor))
        }
        v1::deploy_contract_response::Result::ExpectedActiveVersionMismatch(mismatch) => {
            if mismatch.actual_active_version == Some(0) {
                Err(PublicWireError::InvalidIdentity)
            } else {
                Ok(())
            }
        }
        v1::deploy_contract_response::Result::BundleConflict(_) => Ok(()),
        v1::deploy_contract_response::Result::InvalidSource(diagnostics) => {
            validate_diagnostics(diagnostics)
        }
        v1::deploy_contract_response::Result::ExpectedApplicationIdentityMismatch(mismatch) => {
            if let Some(active) = mismatch.actual_active.as_ref() {
                validate_contract_descriptor(Some(active))?;
            }
            validate_contract_descriptor(mismatch.compiled_candidate.as_ref())
        }
    }
}

fn validate_get_active_contract_request(
    message: &v1::GetActiveContractRequest,
) -> Result<(), PublicWireError> {
    request_id(&message.request_id)
}

fn validate_get_active_contract_response(
    message: &v1::GetActiveContractResponse,
) -> Result<(), PublicWireError> {
    match message
        .result
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        v1::get_active_contract_response::Result::Absent(_) => Ok(()),
        v1::get_active_contract_response::Result::Present(descriptor) => {
            validate_contract_descriptor(Some(descriptor))
        }
    }
}

fn validate_get_outcome_request(message: &v1::GetOutcomeRequest) -> Result<(), PublicWireError> {
    request_id(&message.request_id)?;
    if let Some(locator) = message.outcome_uri.as_deref() {
        if !message.contract_lineage.is_empty()
            || !message.command_name.is_empty()
            || !message.idempotency_key.is_empty()
            || crate::validate_outcome_resource_locator(locator).is_err()
        {
            return Err(PublicWireError::InconsistentFields);
        }
    } else if !valid_bounded_text(&message.contract_lineage, MAX_CONTRACT_LINEAGE_BYTES)
        || !valid_name(&message.command_name)
        || !valid_bounded_text(&message.idempotency_key, MAX_IDEMPOTENCY_KEY_BYTES)
    {
        return Err(PublicWireError::InvalidBytes);
    }
    Ok(())
}

fn validate_get_outcome_response(message: &v1::GetOutcomeResponse) -> Result<(), PublicWireError> {
    match message
        .result
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        v1::get_outcome_response::Result::NotFound(_) => Ok(()),
        v1::get_outcome_response::Result::Found(response) => {
            crate::validate_execute_response(response)
                .map_err(|_| PublicWireError::InconsistentFields)?;
            if response.status != v1::execute_command_response::CompletionStatus::Replayed as i32 {
                return Err(PublicWireError::InconsistentFields);
            }
            Ok(())
        }
    }
}

fn validate_get_entity_request(message: &v1::GetEntityRequest) -> Result<(), PublicWireError> {
    request_id(&message.request_id)?;
    validate_contract_selection(message.contract.as_ref())?;
    if message.entity_type_id == 0 {
        return Err(PublicWireError::InvalidIdentity);
    }
    validate_entity_key(&message.entity_key, Some(message.entity_type_id))?;
    validate_field_selection(message.fields.as_ref())
}

fn validate_entity(entity: &v1::Entity) -> Result<(), PublicWireError> {
    validate_entity_key(&entity.entity_key, None)?;
    if entity.entity_version == 0 || entity.written_by_contract_version == 0 {
        return Err(PublicWireError::InvalidIdentity);
    }
    validate_record(entity.fields.as_ref())
}

fn validate_get_entity_response(message: &v1::GetEntityResponse) -> Result<(), PublicWireError> {
    match message
        .result
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        v1::get_entity_response::Result::NotFound(_) => Ok(()),
        v1::get_entity_response::Result::Found(entity) => validate_entity(entity),
    }
}

fn validate_scan_index_request(message: &v1::ScanIndexRequest) -> Result<(), PublicWireError> {
    request_id(&message.request_id)?;
    validate_contract_selection(message.contract.as_ref())?;
    if message.index_id == 0 {
        return Err(PublicWireError::InvalidIdentity);
    }
    validate_values(&message.leading_components, MAX_PROJECTION_GROUP_COMPONENTS)?;
    validate_field_selection(message.fields.as_ref())?;
    validate_page_request(message.page.as_ref())
}

fn validate_index_page(page: Option<&v1::IndexPage>) -> Result<(), PublicWireError> {
    let page = page.ok_or(PublicWireError::MissingRequiredField)?;
    if page.items.len() > MAX_PAGE_ITEMS {
        return Err(PublicWireError::TooManyItems);
    }
    cursor(&page.next_cursor)?;
    let mut previous_key: Option<&[u8]> = None;
    for row in &page.items {
        validate_index_key(&row.index_entry_key)?;
        if previous_key.is_some_and(|previous| previous >= row.index_entry_key.as_slice()) {
            return Err(PublicWireError::NonCanonical);
        }
        previous_key = Some(&row.index_entry_key);
        validate_record(row.values.as_ref())?;
    }
    let fence = page
        .observed_fence
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?;
    match fence
        .position
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        v1::index_scan_fence::Position::BeforeFirst(_) => {}
        v1::index_scan_fence::Position::AppliedEpoch(0) => {
            return Err(PublicWireError::InvalidIdentity);
        }
        v1::index_scan_fence::Position::AppliedEpoch(_) => {}
    }
    Ok(())
}

fn validate_scan_index_response(message: &v1::ScanIndexResponse) -> Result<(), PublicWireError> {
    validate_index_page(message.page.as_ref())
}

fn validate_projection_generation_frontier(
    pointer: &v1::ProjectionGenerationFrontier,
) -> Result<(), PublicWireError> {
    if pointer.generation == 0 {
        return Err(PublicWireError::InvalidIdentity);
    }
    validate_frontier(pointer.frontier.as_ref())
}

fn validate_projection_failure(failure: &v1::ProjectionFailure) -> Result<(), PublicWireError> {
    if failure.generation == 0 || failure.at_sequence == Some(0) {
        return Err(PublicWireError::InvalidIdentity);
    }
    validate_projection_failure_code(failure.code)
}

fn validate_projection_unavailable_reason(
    reason: Option<&v1::ProjectionUnavailableReason>,
) -> Result<(), PublicWireError> {
    match reason
        .ok_or(PublicWireError::MissingRequiredField)?
        .reason
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        v1::projection_unavailable_reason::Reason::Building(_)
        | v1::projection_unavailable_reason::Reason::Rebuilding(_) => Ok(()),
        v1::projection_unavailable_reason::Reason::Failure(code) => {
            validate_projection_failure_code(*code)
        }
    }
}

fn validate_projection_page(page: Option<&v1::ProjectionPage>) -> Result<(), PublicWireError> {
    let page = page.ok_or(PublicWireError::MissingRequiredField)?;
    if page.items.len() > MAX_PAGE_ITEMS {
        return Err(PublicWireError::TooManyItems);
    }
    if page.items.is_empty() && page.next_cursor.is_some() {
        return Err(PublicWireError::InconsistentFields);
    }
    cursor(&page.next_cursor)?;
    for row in &page.items {
        validate_values(&row.group, MAX_PROJECTION_GROUP_COMPONENTS)?;
        validate_record(row.values.as_ref())?;
    }
    let fence = page
        .observed_fence
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?;
    validate_projection_identity(fence.identity.as_ref())?;
    if fence.generation == 0 {
        return Err(PublicWireError::InvalidIdentity);
    }
    validate_frontier(fence.frontier.as_ref())
}

fn validate_query_projection_request(
    message: &v1::QueryProjectionRequest,
) -> Result<(), PublicWireError> {
    request_id(&message.request_id)?;
    validate_contract_selection(message.contract.as_ref())?;
    if message.projection_id == 0
        || message.required_sequence == Some(0)
        || message.wait_nanos > MAX_PROJECTION_WAIT_NANOS
        || (message.required_sequence.is_none() && message.wait_nanos != 0)
    {
        return Err(PublicWireError::InvalidIdentity);
    }
    validate_values(&message.leading_components, MAX_PROJECTION_GROUP_COMPONENTS)?;
    validate_page_request(message.page.as_ref())
}

fn validate_query_projection_response(
    message: &v1::QueryProjectionResponse,
) -> Result<(), PublicWireError> {
    match message
        .result
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        v1::query_projection_response::Result::Ready(ready) => {
            validate_projection_page(ready.data.as_ref())?;
            validate_frontier(ready.frontier.as_ref())?;
            let observed = ready
                .data
                .as_ref()
                .and_then(|page| page.observed_fence.as_ref())
                .and_then(|fence| fence.frontier.as_ref())
                .ok_or(PublicWireError::MissingRequiredField)?;
            if ready.frontier.as_ref() != Some(observed) {
                return Err(PublicWireError::InconsistentFields);
            }
            Ok(())
        }
        v1::query_projection_response::Result::WaitTimedOut(result) => {
            if result.required_sequence == 0 {
                return Err(PublicWireError::InvalidIdentity);
            }
            validate_frontier(result.current.as_ref())
        }
        v1::query_projection_response::Result::Degraded(result) => {
            validate_frontier(result.current.as_ref())?;
            validate_projection_unavailable_reason(result.reason.as_ref())
        }
        v1::query_projection_response::Result::Invalid(result) => {
            validate_projection_failure_code(result.reason)
        }
    }
}

fn validate_projection_status(message: &v1::ProjectionStatus) -> Result<(), PublicWireError> {
    validate_projection_identity(message.identity.as_ref())?;
    validate_frontier(message.authoritative_head.as_ref())?;
    if let Some(pointer) = &message.published {
        validate_projection_generation_frontier(pointer)?;
    }
    if let Some(pointer) = &message.candidate {
        validate_projection_generation_frontier(pointer)?;
    }
    if message
        .published
        .as_ref()
        .zip(message.candidate.as_ref())
        .is_some_and(|(published, candidate)| published.generation >= candidate.generation)
    {
        return Err(PublicWireError::InconsistentFields);
    }
    if let Some(failure) = &message.failure {
        validate_projection_failure(failure)?;
        if ![message.published.as_ref(), message.candidate.as_ref()]
            .into_iter()
            .flatten()
            .any(|pointer| pointer.generation == failure.generation)
        {
            return Err(PublicWireError::InconsistentFields);
        }
        let pointer = [message.published.as_ref(), message.candidate.as_ref()]
            .into_iter()
            .flatten()
            .find(|pointer| pointer.generation == failure.generation)
            .ok_or(PublicWireError::InconsistentFields)?;
        if let Some(at_sequence) = failure.at_sequence {
            let expected = frontier_value(
                pointer
                    .frontier
                    .as_ref()
                    .ok_or(PublicWireError::MissingRequiredField)?,
            )?
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(PublicWireError::InconsistentFields)?;
            if at_sequence != expected {
                return Err(PublicWireError::InconsistentFields);
            }
        }
        if message
            .published
            .as_ref()
            .is_some_and(|pointer| pointer.generation == failure.generation)
            && message.published_apply_mode != Some(v1::PublishedApplyMode::Suspended as i32)
        {
            return Err(PublicWireError::InconsistentFields);
        }
    }
    let head = frontier_value(
        message
            .authoritative_head
            .as_ref()
            .ok_or(PublicWireError::MissingRequiredField)?,
    )?
    .unwrap_or(0);
    for pointer in [message.published.as_ref(), message.candidate.as_ref()]
        .into_iter()
        .flatten()
    {
        if frontier_value(
            pointer
                .frontier
                .as_ref()
                .ok_or(PublicWireError::MissingRequiredField)?,
        )?
        .unwrap_or(0)
            > head
        {
            return Err(PublicWireError::InconsistentFields);
        }
    }
    let mode = message
        .published_apply_mode
        .map(v1::PublishedApplyMode::try_from)
        .transpose()
        .map_err(|_| PublicWireError::InvalidEnum)?;
    if mode == Some(v1::PublishedApplyMode::Unspecified)
        || message.published.is_some() != mode.is_some()
    {
        return Err(PublicWireError::InconsistentFields);
    }
    let lifecycle = v1::ProjectionLifecycle::try_from(message.lifecycle)
        .map_err(|_| PublicWireError::InvalidEnum)?;
    let before_first_candidate = message.candidate.as_ref().is_some_and(|pointer| {
        matches!(
            pointer
                .frontier
                .as_ref()
                .and_then(|frontier| frontier.position.as_ref()),
            Some(v1::frontier_position::Position::BeforeFirst(_))
        )
    });
    let valid_shape = match lifecycle {
        v1::ProjectionLifecycle::Building => {
            (message.published.is_none()
                && message.candidate.is_none()
                && mode.is_none()
                && message.failure.is_none())
                || (message.published.is_none()
                    && message.candidate.is_some()
                    && before_first_candidate
                    && mode.is_none()
                    && message.failure.is_none())
        }
        v1::ProjectionLifecycle::CatchingUp => {
            message.published.is_none()
                && message.candidate.is_some()
                && mode.is_none()
                && message.failure.is_none()
        }
        v1::ProjectionLifecycle::Ready => {
            message.published.is_some()
                && message.candidate.is_none()
                && mode == Some(v1::PublishedApplyMode::Enabled)
                && message.failure.is_none()
        }
        v1::ProjectionLifecycle::Rebuilding => {
            message.published.is_some()
                && message.candidate.is_some()
                && matches!(
                    mode,
                    Some(v1::PublishedApplyMode::Enabled | v1::PublishedApplyMode::Suspended)
                )
                && message.failure.is_none()
                && message
                    .published
                    .as_ref()
                    .zip(message.candidate.as_ref())
                    .is_some_and(|(published, candidate)| {
                        published.generation < candidate.generation
                    })
        }
        v1::ProjectionLifecycle::Degraded | v1::ProjectionLifecycle::Invalid => {
            message.failure.is_some()
                && ((message.published.is_none() && mode.is_none())
                    || (message.published.is_some()
                        && matches!(
                            mode,
                            Some(
                                v1::PublishedApplyMode::Enabled | v1::PublishedApplyMode::Suspended
                            )
                        )))
        }
        v1::ProjectionLifecycle::Unspecified => false,
    };
    if valid_shape {
        Ok(())
    } else {
        Err(PublicWireError::InconsistentFields)
    }
}

fn validate_get_projection_status_request(
    message: &v1::GetProjectionStatusRequest,
) -> Result<(), PublicWireError> {
    request_id(&message.request_id)?;
    validate_contract_selection(message.contract.as_ref())?;
    if message.projection_id == 0 {
        Err(PublicWireError::InvalidIdentity)
    } else {
        Ok(())
    }
}

fn validate_get_projection_status_response(
    message: &v1::GetProjectionStatusResponse,
) -> Result<(), PublicWireError> {
    match message
        .result
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        v1::get_projection_status_response::Result::NotFound(_) => Ok(()),
        v1::get_projection_status_response::Result::Found(status) => {
            validate_projection_status(status)
        }
    }
}

fn validate_event(
    event: &v1::DurableEvent,
    sequence: u64,
    ordinal: usize,
) -> Result<(), PublicWireError> {
    let event_id = event
        .event_id
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?;
    if event_id.commit_sequence != sequence
        || event_id.commit_sequence == 0
        || usize::try_from(event_id.event_ordinal).ok() != Some(ordinal)
        || event.event_type_id == 0
    {
        return Err(PublicWireError::InconsistentFields);
    }
    validate_record(event.payload.as_ref())
}

fn validate_commit(commit: &v1::Commit) -> Result<(), PublicWireError> {
    if commit.commit_sequence == 0
        || !valid_bounded_text(&commit.contract_lineage, MAX_CONTRACT_LINEAGE_BYTES)
        || commit.contract_version == 0
        || commit.command_id == 0
    {
        return Err(PublicWireError::InvalidIdentity);
    }
    request_id(&commit.admission_request_id)?;
    hash(&commit.plan_hash)?;
    hash(&commit.canonical_input_hash)?;
    validate_actor(commit.actor.as_ref())?;
    validate_timestamp(commit.logical_time.as_ref())?;
    hash(&commit.partition_hash)?;
    if commit.conflict_hashes.len() > MAX_COMMIT_COLLECTION_ITEMS
        || commit.affected_entities.len() > MAX_COMMIT_COLLECTION_ITEMS
        || commit.events.len() > MAX_COMMIT_COLLECTION_ITEMS
    {
        return Err(PublicWireError::TooManyItems);
    }
    let mut previous_conflict: Option<&[u8]> = None;
    for value in &commit.conflict_hashes {
        hash(value)?;
        if previous_conflict.is_some_and(|previous| previous >= value.as_slice()) {
            return Err(PublicWireError::NonCanonical);
        }
        previous_conflict = Some(value);
    }
    let mut previous_entity: Option<&[u8]> = None;
    for affected in &commit.affected_entities {
        validate_entity_key(&affected.entity_key, None)?;
        if affected.entity_version == 0 {
            return Err(PublicWireError::InvalidIdentity);
        }
        if previous_entity.is_some_and(|previous| previous >= affected.entity_key.as_slice()) {
            return Err(PublicWireError::NonCanonical);
        }
        previous_entity = Some(&affected.entity_key);
    }
    for (ordinal, event) in commit.events.iter().enumerate() {
        validate_event(event, commit.commit_sequence, ordinal)?;
    }
    validate_declared_outcome(commit.outcome.as_ref())?;
    validate_provenance_uri(&commit.provenance_uri).map_err(|_| PublicWireError::InvalidBytes)?;
    if !matches!(
        v1::CommandDurability::try_from(commit.durability),
        Ok(v1::CommandDurability::Synchronous | v1::CommandDurability::Group)
    ) {
        return Err(PublicWireError::InvalidEnum);
    }
    Ok(())
}

fn validate_get_commit_request(message: &v1::GetCommitRequest) -> Result<(), PublicWireError> {
    request_id(&message.request_id)?;
    if message.commit_sequence == 0 {
        Err(PublicWireError::InvalidIdentity)
    } else {
        Ok(())
    }
}

fn validate_get_commit_response(message: &v1::GetCommitResponse) -> Result<(), PublicWireError> {
    if message.history_incarnation == 0 {
        return Err(PublicWireError::InvalidIdentity);
    }
    match message
        .result
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        v1::get_commit_response::Result::NotFound(_) => Ok(()),
        v1::get_commit_response::Result::Found(commit) => validate_commit(commit),
    }
}

fn validate_commit_page(page: Option<&v1::CommitPage>) -> Result<(), PublicWireError> {
    let page = page.ok_or(PublicWireError::MissingRequiredField)?;
    if page.history_incarnation == 0 {
        return Err(PublicWireError::InvalidIdentity);
    }
    if page.items.len() > MAX_PAGE_ITEMS {
        return Err(PublicWireError::TooManyItems);
    }
    if page.items.is_empty() && page.next_cursor.is_some() {
        return Err(PublicWireError::InconsistentFields);
    }
    cursor(&page.next_cursor)?;
    let fence = page
        .observed_fence
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?;
    let fence_value = frontier_value(fence)?.unwrap_or(0);
    let mut previous = 0_u64;
    for commit in &page.items {
        validate_commit(commit)?;
        if (previous != 0
            && previous
                .checked_add(1)
                .is_none_or(|expected| commit.commit_sequence != expected))
            || commit.commit_sequence > fence_value
        {
            return Err(PublicWireError::InconsistentFields);
        }
        previous = commit.commit_sequence;
    }
    Ok(())
}

fn validate_scan_commits_request(message: &v1::ScanCommitsRequest) -> Result<(), PublicWireError> {
    request_id(&message.request_id)?;
    validate_page_request(message.page.as_ref())
}

fn validate_scan_commits_response(
    message: &v1::ScanCommitsResponse,
) -> Result<(), PublicWireError> {
    validate_commit_page(message.page.as_ref())
}

fn validate_subscribe_commits_request(
    message: &v1::SubscribeCommitsRequest,
) -> Result<(), PublicWireError> {
    request_id(&message.request_id)?;
    if message.after_sequence == Some(0)
        || !(1..=MAX_SUBSCRIPTION_LIFETIME_NANOS).contains(&message.maximum_lifetime_nanos)
    {
        return Err(PublicWireError::InvalidIdentity);
    }
    Ok(())
}

fn validate_commit_notification(message: &v1::CommitNotification) -> Result<(), PublicWireError> {
    if message.history_incarnation == 0 {
        return Err(PublicWireError::InvalidIdentity);
    }
    match message
        .notification
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        v1::commit_notification::Notification::Commit(commit) => validate_commit(commit),
        v1::commit_notification::Notification::Terminal(terminal) => {
            if terminal.history_incarnation == 0 {
                return Err(PublicWireError::InvalidIdentity);
            }
            if !matches!(
                v1::CommitSubscriptionEndReason::try_from(terminal.reason),
                Ok(v1::CommitSubscriptionEndReason::LifetimeElapsed
                    | v1::CommitSubscriptionEndReason::Lagged
                    | v1::CommitSubscriptionEndReason::ScanGap
                    | v1::CommitSubscriptionEndReason::PolicyDenied
                    | v1::CommitSubscriptionEndReason::Cancelled
                    | v1::CommitSubscriptionEndReason::DeadlineExceeded
                    | v1::CommitSubscriptionEndReason::ServiceShutdown
                    | v1::CommitSubscriptionEndReason::Unavailable)
            ) {
                return Err(PublicWireError::InvalidEnum);
            }
            validate_frontier(terminal.resume_after.as_ref())
        }
    }
}

fn validate_health_request(message: &v1::HealthRequest) -> Result<(), PublicWireError> {
    if let Some(id) = &message.request_id {
        request_id(id)?;
    }
    Ok(())
}

fn validate_build_info(build: Option<&v1::BuildInfo>) -> Result<(), PublicWireError> {
    let build = build.ok_or(PublicWireError::MissingRequiredField)?;
    for value in [
        build.semantic_version.as_str(),
        build.git_revision.as_str(),
        build.rust_version.as_str(),
        build.mcp_protocol_baseline.as_str(),
    ] {
        if !valid_ascii(value, MAX_BUILD_STRING_BYTES) {
            return Err(PublicWireError::InvalidBytes);
        }
    }
    if build.enabled_features.len() > MAX_BUILD_FEATURES
        || build
            .enabled_features
            .iter()
            .any(|feature| !valid_ascii(feature, MAX_BUILD_STRING_BYTES))
        || build
            .enabled_features
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        || build.storage_format_version == 0
        || build.contract_ir_version == 0
    {
        return Err(PublicWireError::NonCanonical);
    }
    Ok(())
}

fn validate_health_response(message: &v1::HealthResponse) -> Result<(), PublicWireError> {
    match message
        .result
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        v1::health_response::Result::PreBootstrap(health) => {
            let lifecycle = v1::PreBootstrapLifecycle::try_from(health.lifecycle)
                .map_err(|_| PublicWireError::InvalidEnum)?;
            let process_only = lifecycle == v1::PreBootstrapLifecycle::Unspecified
                && health.liveness
                && !health.readiness
                && message.database_alias.is_empty()
                && message.authentication_audience.is_empty();
            let initializing = matches!(
                lifecycle,
                v1::PreBootstrapLifecycle::InitializingValidation
                    | v1::PreBootstrapLifecycle::InitializingBootstrap
            ) && !health.readiness;
            if !process_only && !initializing {
                return Err(PublicWireError::InconsistentFields);
            }
            Ok(())
        }
        v1::health_response::Result::Authenticated(health) => {
            // history_incarnation may be 0 while the server is still initializing
            // (pre-ready); once Ready it must be ≥ 1.
            if message.database_alias.is_empty() || message.authentication_audience.is_empty() {
                return Err(PublicWireError::InconsistentFields);
            }
            if health.status == v1::HealthStatus::Ready as i32 && health.history_incarnation == 0 {
                return Err(PublicWireError::InvalidIdentity);
            }
            if !matches!(
                v1::HealthStatus::try_from(health.status),
                Ok(v1::HealthStatus::Ready
                    | v1::HealthStatus::NotReady
                    | v1::HealthStatus::Degraded)
            ) || health.active_contract_version == Some(0)
                || health.last_commit_sequence == Some(0)
                || health.components.len() > 5
            {
                return Err(PublicWireError::InvalidEnum);
            }
            let mut previous = 0;
            for component in &health.components {
                let kind = v1::HealthComponentKind::try_from(component.component)
                    .map_err(|_| PublicWireError::InvalidEnum)?;
                if kind == v1::HealthComponentKind::Unspecified
                    || component.component <= previous
                    || !matches!(
                        v1::HealthComponentStatus::try_from(component.status),
                        Ok(v1::HealthComponentStatus::Healthy
                            | v1::HealthComponentStatus::Degraded
                            | v1::HealthComponentStatus::Unavailable)
                    )
                {
                    return Err(PublicWireError::NonCanonical);
                }
                previous = component.component;
            }
            validate_timestamp(health.started_at.as_ref())?;
            validate_build_info(health.build.as_ref())
        }
    }
}

fn validate_stats_request(message: &v1::StatsRequest) -> Result<(), PublicWireError> {
    request_id(&message.request_id)
}

fn validate_stats_response(message: &v1::StatsResponse) -> Result<(), PublicWireError> {
    if message.history_incarnation == 0
        || message.active_cursors > 4_096
        || message.active_commit_subscribers > 128
        || message.last_commit_sequence == Some(0)
    {
        return Err(PublicWireError::InvalidIdentity);
    }
    Ok(())
}

fn permission_key(
    permission: &v1::CapabilityPermission,
) -> Result<(u8, &str, u32, &[u8], &str), PublicWireError> {
    let permission = permission
        .permission
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?;
    let key: (u8, &str, u32, &[u8], &str) = match permission {
        v1::capability_permission::Permission::ValidateContract(_) => (1, "", 0, &[], ""),
        v1::capability_permission::Permission::ReadContract(_) => (2, "", 0, &[], ""),
        v1::capability_permission::Permission::ExplainCommand(value) => {
            (3, value.contract_lineage.as_str(), value.stable_id, &[], "")
        }
        v1::capability_permission::Permission::DeployContract(_) => (4, "", 0, &[], ""),
        v1::capability_permission::Permission::InvokeCommand(value) => {
            (5, value.contract_lineage.as_str(), value.stable_id, &[], "")
        }
        v1::capability_permission::Permission::ReadEntity(value) => {
            (6, value.contract_lineage.as_str(), value.stable_id, &[], "")
        }
        v1::capability_permission::Permission::ScanIndex(value) => {
            (7, value.contract_lineage.as_str(), value.stable_id, &[], "")
        }
        v1::capability_permission::Permission::QueryProjection(value) => {
            (8, value.contract_lineage.as_str(), value.stable_id, &[], "")
        }
        v1::capability_permission::Permission::ReadProjectionStatus(value) => {
            (9, value.contract_lineage.as_str(), value.stable_id, &[], "")
        }
        v1::capability_permission::Permission::ReadCommit(_) => (10, "", 0, &[], ""),
        v1::capability_permission::Permission::ScanCommits(_) => (11, "", 0, &[], ""),
        v1::capability_permission::Permission::SubscribeCommits(_) => (12, "", 0, &[], ""),
        v1::capability_permission::Permission::ReadProvenance(_) => (13, "", 0, &[], ""),
        v1::capability_permission::Permission::InspectOutbox(_) => (14, "", 0, &[], ""),
        v1::capability_permission::Permission::ReadHealth(_) => (15, "", 0, &[], ""),
        v1::capability_permission::Permission::ReadStatistics(_) => (16, "", 0, &[], ""),
        v1::capability_permission::Permission::CreateCapability(_) => (17, "", 0, &[], ""),
        v1::capability_permission::Permission::RevokeCapability(_) => (18, "", 0, &[], ""),
        v1::capability_permission::Permission::AdministerCapabilities(_) => (19, "", 0, &[], ""),
        v1::capability_permission::Permission::CheckAdHocQuery(_) => (20, "", 0, &[], ""),
        v1::capability_permission::Permission::ExplainAdHocQuery(_) => (21, "", 0, &[], ""),
        v1::capability_permission::Permission::ExecuteAdHocQuery(_) => (22, "", 0, &[], ""),
        v1::capability_permission::Permission::ExplainNamedQuery(value) => (
            23,
            value.contract_lineage.as_str(),
            0,
            value.query_module_hash.as_slice(),
            value.query_name.as_str(),
        ),
        v1::capability_permission::Permission::ExecuteNamedQuery(value) => (
            24,
            value.contract_lineage.as_str(),
            0,
            value.query_module_hash.as_slice(),
            value.query_name.as_str(),
        ),
        v1::capability_permission::Permission::ApplicationRoleIdentity(value) => {
            (25, "", 0, value.as_slice(), "")
        }
        v1::capability_permission::Permission::MigrateContract(lineage) => {
            (26, lineage.as_str(), 0, &[], "")
        }
        v1::capability_permission::Permission::ConsumeEventStream(value) => (
            27,
            value.contract_lineage.as_str(),
            0,
            value.reactive_module_hash.as_slice(),
            value.operation_name.as_str(),
        ),
        v1::capability_permission::Permission::SeekEventStreamConsumer(value) => (
            28,
            value.contract_lineage.as_str(),
            0,
            value.reactive_module_hash.as_slice(),
            value.operation_name.as_str(),
        ),
        v1::capability_permission::Permission::WatchNamedQuery(value) => (
            29,
            value.contract_lineage.as_str(),
            0,
            value.reactive_module_hash.as_slice(),
            value.operation_name.as_str(),
        ),
        v1::capability_permission::Permission::ConsumeContextualSubscription(value) => (
            30,
            value.contract_lineage.as_str(),
            0,
            value.reactive_module_hash.as_slice(),
            value.operation_name.as_str(),
        ),
        v1::capability_permission::Permission::InstallApplication(lineage) => {
            (31, lineage.as_str(), 0, &[], "")
        }
    };
    if key.0 >= 3
        && matches!(key.0, 3 | 5 | 6 | 7 | 8 | 9)
        && (!valid_bounded_text(key.1, MAX_CONTRACT_LINEAGE_BYTES) || key.2 == 0)
    {
        return Err(PublicWireError::InvalidIdentity);
    }
    if matches!(key.0, 23 | 24)
        && (!valid_bounded_text(key.1, MAX_CONTRACT_LINEAGE_BYTES)
            || key.3.len() != 32
            || !valid_bounded_text(key.4, 256))
    {
        return Err(PublicWireError::InvalidIdentity);
    }
    if key.0 == 25 && key.3.len() != 32 {
        return Err(PublicWireError::InvalidIdentity);
    }
    if matches!(key.0, 26 | 31) && !valid_bounded_text(key.1, MAX_CONTRACT_LINEAGE_BYTES) {
        return Err(PublicWireError::InvalidIdentity);
    }
    if matches!(key.0, 27..=30)
        && (!valid_bounded_text(key.1, MAX_CONTRACT_LINEAGE_BYTES)
            || key.3.len() != 32
            || !valid_bounded_text(key.4, 256))
    {
        return Err(PublicWireError::InvalidIdentity);
    }
    Ok(key)
}

fn compare_framed_bytes(left: &[u8], right: &[u8]) -> Ordering {
    left.len().cmp(&right.len()).then_with(|| left.cmp(right))
}

fn compare_permission_keys(
    left: (u8, &str, u32, &[u8], &str),
    right: (u8, &str, u32, &[u8], &str),
) -> Ordering {
    left.0
        .cmp(&right.0)
        .then_with(|| compare_framed_bytes(left.1.as_bytes(), right.1.as_bytes()))
        .then_with(|| left.2.cmp(&right.2))
        .then_with(|| left.3.cmp(right.3))
        .then_with(|| compare_framed_bytes(left.4.as_bytes(), right.4.as_bytes()))
}

fn compare_scoped_partitions(left: (&str, &[u8]), right: (&str, &[u8])) -> Ordering {
    compare_framed_bytes(left.0.as_bytes(), right.0.as_bytes())
        .then_with(|| compare_framed_bytes(left.1, right.1))
}

fn compare_field_visibility(left: (&str, u32), right: (&str, u32)) -> Ordering {
    compare_framed_bytes(left.0.as_bytes(), right.0.as_bytes()).then_with(|| left.1.cmp(&right.1))
}

fn compare_row_policy_bindings(left: (&str, u32, &str), right: (&str, u32, &str)) -> Ordering {
    compare_framed_bytes(left.0.as_bytes(), right.0.as_bytes())
        .then_with(|| left.1.cmp(&right.1))
        .then_with(|| left.2.as_bytes().cmp(right.2.as_bytes()))
}

fn checked_capability_sum(
    parts: impl IntoIterator<Item = usize>,
) -> Result<usize, PublicWireError> {
    parts.into_iter().try_fold(0usize, |total, part| {
        total.checked_add(part).ok_or(PublicWireError::TooManyItems)
    })
}

fn framed_capability_bytes(content_bytes: usize) -> Result<usize, PublicWireError> {
    4usize
        .checked_add(content_bytes)
        .ok_or(PublicWireError::TooManyItems)
}

fn validate_capability_row_policy(
    row_policy: Option<&v1::CapabilityRowPolicyGrant>,
) -> Result<usize, PublicWireError> {
    let Some(row_policy) = row_policy else {
        return Ok(0);
    };
    let application_role_hash: [u8; 32] = row_policy
        .application_role_hash
        .as_slice()
        .try_into()
        .map_err(|_| PublicWireError::InvalidIdentity)?;
    if row_policy.principal_facts.len() > MAX_PRINCIPAL_FACTS_V1
        || row_policy.policies.len() > MAX_CAPABILITY_ROW_POLICY_BINDINGS
    {
        return Err(PublicWireError::TooManyItems);
    }
    if row_policy.policies.is_empty() {
        return Err(PublicWireError::MissingRequiredField);
    }

    let mut facts = Vec::with_capacity(row_policy.principal_facts.len());
    let mut previous_fact_name: Option<&str> = None;
    for fact in &row_policy.principal_facts {
        if previous_fact_name.is_some_and(|previous| previous >= fact.name.as_str()) {
            return Err(PublicWireError::NonCanonical);
        }
        let value = canonical_value_from_proto(
            fact.value
                .clone()
                .ok_or(PublicWireError::MissingRequiredField)?,
        )
        .map_err(|_| PublicWireError::InvalidValue)?;
        facts.push(
            CapabilityPrincipalFactV1::new(fact.name.clone(), value)
                .map_err(|_| PublicWireError::InvalidValue)?,
        );
        previous_fact_name = Some(fact.name.as_str());
    }
    let principal_facts =
        CapabilityPrincipalFactsV1::new(facts).map_err(|_| PublicWireError::NonCanonical)?;

    let mut bindings = Vec::with_capacity(row_policy.policies.len());
    let mut previous_binding: Option<(&str, u32, &str)> = None;
    for binding in &row_policy.policies {
        let lineage = ContractLineage::new(binding.contract_lineage.clone())
            .map_err(|_| PublicWireError::InvalidIdentity)?;
        let policy_name = RowPolicyName::new(binding.policy_name.clone())
            .map_err(|_| PublicWireError::InvalidIdentity)?;
        let entity_type =
            EntityTypeId::new(binding.entity_type_id).ok_or(PublicWireError::InvalidIdentity)?;
        if binding.operations.is_empty() || binding.operations.len() > 4 {
            return Err(PublicWireError::TooManyItems);
        }
        let key = (
            binding.contract_lineage.as_str(),
            binding.entity_type_id,
            binding.policy_name.as_str(),
        );
        if previous_binding
            .is_some_and(|previous| compare_row_policy_bindings(previous, key) != Ordering::Less)
        {
            return Err(PublicWireError::NonCanonical);
        }

        let mut operations = Vec::with_capacity(binding.operations.len());
        let mut previous_operation = 0i32;
        for raw_operation in &binding.operations {
            if *raw_operation <= previous_operation {
                return Err(PublicWireError::NonCanonical);
            }
            let operation = match v1::CapabilityRowPolicyOperation::try_from(*raw_operation)
                .map_err(|_| PublicWireError::InvalidEnum)?
            {
                v1::CapabilityRowPolicyOperation::Read => CapabilityRowPolicyOperationV1::Read,
                v1::CapabilityRowPolicyOperation::Create => CapabilityRowPolicyOperationV1::Create,
                v1::CapabilityRowPolicyOperation::Update => CapabilityRowPolicyOperationV1::Update,
                v1::CapabilityRowPolicyOperation::Delete => CapabilityRowPolicyOperationV1::Delete,
                v1::CapabilityRowPolicyOperation::Unspecified => {
                    return Err(PublicWireError::InvalidEnum);
                }
            };
            operations.push(operation);
            previous_operation = *raw_operation;
        }
        bindings.push(
            CapabilityRowPolicyBindingV1::new(lineage, policy_name, entity_type, operations)
                .map_err(|_| PublicWireError::NonCanonical)?,
        );
        previous_binding = Some(key);
    }

    let grant = CapabilityRowPolicyGrantV1::new(
        ApplicationRoleHash::from_bytes(application_role_hash),
        principal_facts,
        bindings,
    )
    .map_err(|_| PublicWireError::NonCanonical)?;
    grant.bindings().iter().try_fold(
        checked_capability_sum([
            32,
            grant
                .internal_principal_facts()
                .internal_canonical_bytes()
                .len(),
            4,
        ])?,
        |total, binding| {
            checked_capability_sum([
                total,
                binding.lineage().as_bytes().len(),
                binding.policy_name().as_str().len(),
                4,
                4,
                binding.operations().len(),
            ])
        },
    )
}

fn validate_capability_export(
    export: Option<&v1::CapabilityExportGrant>,
) -> Result<usize, PublicWireError> {
    let Some(export) = export else {
        return Ok(0);
    };
    if export.applications.is_empty() {
        return Err(PublicWireError::MissingRequiredField);
    }
    if export.applications.len() > MAX_CAPABILITY_APPLICATION_EXPORT_GRANTS {
        return Err(PublicWireError::TooManyItems);
    }

    let mut applications = Vec::with_capacity(export.applications.len());
    let mut previous_lineage: Option<&str> = None;
    for application in &export.applications {
        if previous_lineage.is_some_and(|previous| {
            compare_framed_bytes(previous.as_bytes(), application.contract_lineage.as_bytes())
                != Ordering::Less
        }) {
            return Err(PublicWireError::NonCanonical);
        }
        let lineage = ContractLineage::new(application.contract_lineage.clone())
            .map_err(|_| PublicWireError::InvalidIdentity)?;
        let scope = match v1::CapabilityApplicationExportScope::try_from(application.scope)
            .map_err(|_| PublicWireError::InvalidEnum)?
        {
            v1::CapabilityApplicationExportScope::PrincipalFiltered => {
                CapabilityApplicationExportScopeV1::PrincipalFiltered
            }
            v1::CapabilityApplicationExportScope::WholeApplication => {
                CapabilityApplicationExportScopeV1::WholeApplication
            }
            v1::CapabilityApplicationExportScope::Unspecified => {
                return Err(PublicWireError::InvalidEnum);
            }
        };
        applications.push(
            CapabilityApplicationExportGrantV1::new(
                lineage,
                scope,
                application.entities,
                application.events,
                application.provenance,
                application.public_audit,
            )
            .map_err(|_| PublicWireError::InvalidIdentity)?,
        );
        previous_lineage = Some(application.contract_lineage.as_str());
    }
    let export =
        CapabilityExportGrantV1::new(applications).map_err(|_| PublicWireError::NonCanonical)?;
    export
        .applications()
        .iter()
        .try_fold(4usize, |total, grant| {
            checked_capability_sum([
                total,
                framed_capability_bytes(grant.lineage().as_bytes().len())?,
                5,
            ])
        })
}

fn capability_grant_semantic_bytes(grant: &v1::CapabilityGrant) -> Result<usize, PublicWireError> {
    let tenant_scope = grant
        .tenant_scope
        .as_ref()
        .and_then(|scope| scope.scope.as_ref())
        .ok_or(PublicWireError::MissingRequiredField)?;
    let tenant_content = match tenant_scope {
        v1::tenant_scope::Scope::Global(_) => 1,
        v1::tenant_scope::Scope::TenantId(tenant_id) => {
            checked_capability_sum([1, framed_capability_bytes(tenant_id.len())?])?
        }
    };

    let partition_scope = grant
        .partition_scope
        .as_ref()
        .and_then(|scope| scope.scope.as_ref())
        .ok_or(PublicWireError::MissingRequiredField)?;
    let partition_bytes = match partition_scope {
        v1::partition_scope::Scope::All(_) => 1,
        v1::partition_scope::Scope::Explicit(explicit) => {
            explicit
                .partitions
                .iter()
                .try_fold(5usize, |total, value| {
                    let entry = checked_capability_sum([
                        framed_capability_bytes(value.contract_lineage.len())?,
                        framed_capability_bytes(value.partition_key.len())?,
                    ])?;
                    checked_capability_sum([total, framed_capability_bytes(entry)?])
                })?
        }
    };

    let permission_bytes = grant
        .permissions
        .iter()
        .try_fold(4usize, |total, permission| {
            let (tag, lineage, _, module_hash, query_name) = permission_key(permission)?;
            let content = if matches!(tag, 3 | 5 | 6 | 7 | 8 | 9) {
                checked_capability_sum([1, framed_capability_bytes(lineage.len())?, 4])?
            } else if matches!(tag, 23 | 24) {
                checked_capability_sum([
                    1,
                    framed_capability_bytes(lineage.len())?,
                    framed_capability_bytes(module_hash.len())?,
                    framed_capability_bytes(query_name.len())?,
                ])?
            } else if tag == 26 {
                checked_capability_sum([1, framed_capability_bytes(lineage.len())?])?
            } else {
                1
            };
            checked_capability_sum([total, framed_capability_bytes(content)?])
        })?;

    let field_visibility_bytes =
        grant
            .field_visibility
            .iter()
            .try_fold(4usize, |total, entry| {
                let fields = entry
                    .field_ids
                    .len()
                    .checked_add(entry.secret_field_ids.len())
                    .and_then(|count| count.checked_mul(4))
                    .ok_or(PublicWireError::TooManyItems)?;
                let content = checked_capability_sum([
                    framed_capability_bytes(entry.contract_lineage.len())?,
                    4,
                    4,
                    fields,
                ])?;
                checked_capability_sum([total, framed_capability_bytes(content)?])
            })?;

    checked_capability_sum([
        framed_capability_bytes(tenant_content)?,
        partition_bytes,
        permission_bytes,
        field_visibility_bytes,
        2,
        checked_capability_sum([4, grant.approval_required.len()])?,
        validate_capability_row_policy(grant.row_policy.as_ref())?,
        validate_capability_export(grant.export.as_ref())?,
    ])
}

fn validate_capability_grant(grant: Option<&v1::CapabilityGrant>) -> Result<(), PublicWireError> {
    let grant = grant.ok_or(PublicWireError::MissingRequiredField)?;
    validate_tenant_scope(grant.tenant_scope.as_ref())?;
    match grant
        .partition_scope
        .as_ref()
        .and_then(|scope| scope.scope.as_ref())
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        v1::partition_scope::Scope::All(_) => {}
        v1::partition_scope::Scope::Explicit(explicit) => {
            if explicit.partitions.is_empty()
                || explicit.partitions.len() > MAX_CAPABILITY_PARTITIONS
            {
                return Err(PublicWireError::TooManyItems);
            }
            let mut previous: Option<(&str, &[u8])> = None;
            for partition in &explicit.partitions {
                if !valid_bounded_text(&partition.contract_lineage, MAX_CONTRACT_LINEAGE_BYTES) {
                    return Err(PublicWireError::InvalidBytes);
                }
                validate_partition_key(&partition.partition_key)?;
                let key = (
                    partition.contract_lineage.as_str(),
                    partition.partition_key.as_slice(),
                );
                if previous.is_some_and(|previous| {
                    compare_scoped_partitions(previous, key) != Ordering::Less
                }) {
                    return Err(PublicWireError::NonCanonical);
                }
                previous = Some(key);
            }
        }
    }
    if grant.permissions.len() > MAX_CAPABILITY_PERMISSIONS
        || grant.field_visibility.len() > MAX_CAPABILITY_FIELD_VISIBILITY
        || grant.approval_required.len() > 25
        || !(1..=500).contains(&grant.max_scan_rows)
    {
        return Err(PublicWireError::TooManyItems);
    }
    let mut previous_permission = None;
    for permission in &grant.permissions {
        let key = permission_key(permission)?;
        if previous_permission
            .is_some_and(|previous| compare_permission_keys(previous, key) != Ordering::Less)
        {
            return Err(PublicWireError::NonCanonical);
        }
        previous_permission = Some(key);
    }
    let mut total_fields = 0usize;
    let mut previous_visibility: Option<(&str, u32)> = None;
    for visibility in &grant.field_visibility {
        if !valid_bounded_text(&visibility.contract_lineage, MAX_CONTRACT_LINEAGE_BYTES)
            || visibility.entity_type_id == 0
            || visibility.field_ids.is_empty()
        {
            return Err(PublicWireError::InvalidIdentity);
        }
        total_fields = total_fields
            .checked_add(visibility.field_ids.len())
            .ok_or(PublicWireError::TooManyItems)?;
        strictly_increasing_nonzero(&visibility.field_ids)?;
        let key = (
            visibility.contract_lineage.as_str(),
            visibility.entity_type_id,
        );
        if previous_visibility
            .is_some_and(|previous| compare_field_visibility(previous, key) != Ordering::Less)
        {
            return Err(PublicWireError::NonCanonical);
        }
        previous_visibility = Some(key);
    }
    if total_fields > MAX_CAPABILITY_FIELD_VISIBILITY {
        return Err(PublicWireError::TooManyItems);
    }
    let mut previous_approval = 0;
    for approval in &grant.approval_required {
        if !(1..=24).contains(approval) || *approval <= previous_approval {
            return Err(PublicWireError::NonCanonical);
        }
        previous_approval = *approval;
    }
    if capability_grant_semantic_bytes(grant)? > MAX_CAPABILITY_PAYLOAD_BYTES {
        return Err(PublicWireError::TooManyItems);
    }
    Ok(())
}

fn validate_capability_identity(
    identity: Option<&v1::CapabilityIdentity>,
) -> Result<(), PublicWireError> {
    let identity = identity.ok_or(PublicWireError::MissingRequiredField)?;
    capability_id(&identity.capability_id)?;
    if identity.revision == 0 {
        Err(PublicWireError::InvalidIdentity)
    } else {
        Ok(())
    }
}

fn validate_capability_transition(
    transition: Option<&v1::CapabilityTransition>,
) -> Result<(), PublicWireError> {
    let transition = transition.ok_or(PublicWireError::MissingRequiredField)?;
    validate_capability_identity(transition.identity.as_ref())?;
    if transition.administration_sequence == 0 {
        Err(PublicWireError::InvalidIdentity)
    } else {
        Ok(())
    }
}

fn validate_capability_token(token: &str) -> Result<(), PublicWireError> {
    if token.len() == 43
        && token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        && token.as_bytes().last().is_some_and(|last| {
            matches!(
                last,
                b'A' | b'E'
                    | b'I'
                    | b'M'
                    | b'Q'
                    | b'U'
                    | b'Y'
                    | b'c'
                    | b'g'
                    | b'k'
                    | b'o'
                    | b's'
                    | b'w'
                    | b'0'
                    | b'4'
                    | b'8'
            )
        })
    {
        Ok(())
    } else {
        Err(PublicWireError::InvalidBytes)
    }
}

fn validate_create_capability_request(
    message: &v1::CreateCapabilityRequest,
) -> Result<(), PublicWireError> {
    request_id(&message.request_id)?;
    validate_create_capability_body(message)
}

/// Validates a capability-create retry template before a fresh request ID is assigned.
///
/// The template must use the exact empty request-ID sentinel. Every other
/// structural rule and the public request-size ceiling are identical to a
/// submitted [`v1::CreateCapabilityRequest`].
pub fn validate_create_capability_template(
    message: &v1::CreateCapabilityRequest,
) -> Result<(), PublicWireError> {
    if !message.request_id.is_empty() {
        return Err(PublicWireError::InconsistentFields);
    }
    validate_create_capability_body(message)?;
    if message
        .encoded_len()
        .checked_add(18)
        .is_none_or(|submitted_len| submitted_len > MAX_PUBLIC_REQUEST_BYTES)
    {
        return Err(PublicWireError::MessageTooLarge);
    }
    Ok(())
}

fn validate_create_capability_body(
    message: &v1::CreateCapabilityRequest,
) -> Result<(), PublicWireError> {
    if !matches!(
        v1::CapabilityCreateMode::try_from(message.mode),
        Ok(v1::CapabilityCreateMode::Normal | v1::CapabilityCreateMode::Bootstrap)
    ) || !valid_bounded_text(&message.principal_id, MAX_ACTOR_ID_BYTES)
        || !matches!(
            v1::ActorKind::try_from(message.actor_kind),
            Ok(v1::ActorKind::Human | v1::ActorKind::Agent | v1::ActorKind::Service)
        )
        || !(1..=MAX_CAPABILITY_LIFETIME_SECONDS).contains(&message.requested_lifetime_seconds)
        || message.audiences.is_empty()
        || message.audiences.len() > MAX_CAPABILITY_AUDIENCES
        || message
            .audiences
            .iter()
            .any(|audience| Audience::new(audience.as_str()).is_err())
        || message.audiences.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(PublicWireError::NonCanonical);
    }
    capability_id(&message.capability_id)?;
    validate_capability_grant(message.grant.as_ref())?;
    if message.mode == v1::CapabilityCreateMode::Bootstrap as i32 {
        let grant = message
            .grant
            .as_ref()
            .ok_or(PublicWireError::MissingRequiredField)?;
        if message.actor_kind != v1::ActorKind::Human as i32
            || !grant.permissions.iter().any(|permission| {
                matches!(
                    permission.permission.as_ref(),
                    Some(v1::capability_permission::Permission::AdministerCapabilities(_))
                )
            })
        {
            return Err(PublicWireError::InconsistentFields);
        }
    }
    Ok(())
}

fn validate_create_capability_response(
    message: &v1::CreateCapabilityResponse,
) -> Result<(), PublicWireError> {
    match message
        .result
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        v1::create_capability_response::Result::Normal(normal) => match normal
            .result
            .as_ref()
            .ok_or(PublicWireError::MissingRequiredField)?
        {
            v1::normal_create_capability_result::Result::Created(created) => {
                validate_capability_transition(created.transition.as_ref())?;
                validate_capability_token(&created.token)
            }
            v1::normal_create_capability_result::Result::AlreadyCreatedTokenUnavailable(
                identity,
            ) => validate_capability_identity(Some(identity)),
            v1::normal_create_capability_result::Result::CapabilityIdConflict(_) => Ok(()),
        },
        v1::create_capability_response::Result::Bootstrap(bootstrap) => match bootstrap
            .result
            .as_ref()
            .ok_or(PublicWireError::MissingRequiredField)?
        {
            v1::bootstrap_create_capability_result::Result::Created(transition)
            | v1::bootstrap_create_capability_result::Result::Replayed(transition) => {
                validate_capability_transition(Some(transition))
            }
            v1::bootstrap_create_capability_result::Result::BootstrapConflict(_) => Ok(()),
        },
    }
}

fn validate_revoke_capability_request(
    message: &v1::RevokeCapabilityRequest,
) -> Result<(), PublicWireError> {
    request_id(&message.request_id)?;
    capability_id(&message.capability_id)?;
    if matches!(
        v1::RevocationReason::try_from(message.reason),
        Ok(v1::RevocationReason::Requested
            | v1::RevocationReason::Replaced
            | v1::RevocationReason::SuspectedCompromise
            | v1::RevocationReason::PolicyChange)
    ) {
        Ok(())
    } else {
        Err(PublicWireError::InvalidEnum)
    }
}

fn validate_revoke_capability_response(
    message: &v1::RevokeCapabilityResponse,
) -> Result<(), PublicWireError> {
    match message
        .result
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        v1::revoke_capability_response::Result::Revoked(transition)
        | v1::revoke_capability_response::Result::AlreadyRevoked(transition) => {
            validate_capability_transition(Some(transition))
        }
        v1::revoke_capability_response::Result::CapabilityNotFound(_) => Ok(()),
    }
}

fn validate_get_contract_version_request(
    message: &v1::GetContractVersionRequest,
) -> Result<(), PublicWireError> {
    request_id(&message.request_id)?;
    if valid_bounded_text(&message.contract_lineage, MAX_CONTRACT_LINEAGE_BYTES)
        && message.contract_version != 0
    {
        Ok(())
    } else {
        Err(PublicWireError::InvalidIdentity)
    }
}

fn validate_get_contract_version_response(
    message: &v1::GetContractVersionResponse,
) -> Result<(), PublicWireError> {
    match message
        .result
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        v1::get_contract_version_response::Result::NotFound(_) => Ok(()),
        v1::get_contract_version_response::Result::Found(found) => {
            validate_contract_descriptor(Some(found))
        }
    }
}

fn validate_event_id_value(event_id: Option<&v1::EventId>) -> Result<(u64, u32), PublicWireError> {
    let event_id = event_id.ok_or(PublicWireError::MissingRequiredField)?;
    if event_id.commit_sequence == 0 {
        Err(PublicWireError::InvalidIdentity)
    } else {
        Ok((event_id.commit_sequence, event_id.event_ordinal))
    }
}

fn validate_provenance_claims(
    claims: Option<&v1::ProvenanceClaims>,
) -> Result<(), PublicWireError> {
    let claims = claims.ok_or(PublicWireError::MissingRequiredField)?;
    if claims
        .source_repository
        .as_deref()
        .is_some_and(|value| !valid_bounded_text(value, MAX_SOURCE_REPOSITORY_BYTES))
        || claims.source_commit.as_deref().is_some_and(|value| {
            !valid_ascii(value, MAX_SOURCE_COMMIT_BYTES)
                || value.bytes().any(|byte| !(0x21..=0x7e).contains(&byte))
        })
        || claims
            .reason
            .as_deref()
            .is_some_and(|value| !valid_bounded_text(value, MAX_PROVENANCE_REASON_BYTES))
        || claims.approval_id.as_deref().is_some_and(|value| {
            !valid_ascii(value, MAX_APPROVAL_ID_BYTES)
                || value.bytes().any(|byte| !(0x21..=0x7e).contains(&byte))
        })
    {
        Err(PublicWireError::InvalidBytes)
    } else {
        Ok(())
    }
}

fn validate_provenance(message: &v1::Provenance) -> Result<(), PublicWireError> {
    provenance_id(&message.provenance_id)?;
    request_id(&message.admission_request_id)?;
    if message.commit_sequence == 0
        || !valid_bounded_text(&message.contract_lineage, MAX_CONTRACT_LINEAGE_BYTES)
        || message.contract_version == 0
        || message.command_id == 0
        || message.outcome_id == 0
        || message.affected_entities.len() > MAX_PROVENANCE_LINKS
        || message.event_ids.len() > MAX_PROVENANCE_LINKS
    {
        return Err(PublicWireError::InvalidIdentity);
    }
    hash(&message.plan_hash)?;
    validate_actor(message.actor.as_ref())?;
    validate_timestamp(message.logical_time.as_ref())?;
    validate_provenance_claims(message.claims.as_ref())?;
    for entity in &message.affected_entities {
        validate_entity_key(&entity.entity_key, None)?;
        if entity.entity_version == 0 {
            return Err(PublicWireError::InvalidIdentity);
        }
    }
    for event_id in &message.event_ids {
        validate_event_id_value(Some(event_id))?;
    }
    Ok(())
}

fn validate_trace_provenance_request(
    message: &v1::TraceProvenanceRequest,
) -> Result<(), PublicWireError> {
    request_id(&message.request_id)?;
    match message
        .selector
        .as_ref()
        .and_then(|selector| selector.selection.as_ref())
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        v1::provenance_selection::Selection::CommitSequence(0) => {
            Err(PublicWireError::InvalidIdentity)
        }
        v1::provenance_selection::Selection::CommitSequence(_) => Ok(()),
        v1::provenance_selection::Selection::ProvenanceId(value) => provenance_id(value),
    }
}

fn validate_trace_provenance_response(
    message: &v1::TraceProvenanceResponse,
) -> Result<(), PublicWireError> {
    match message
        .result
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        v1::trace_provenance_response::Result::NotFound(_) => Ok(()),
        v1::trace_provenance_response::Result::Found(found) => validate_provenance(found),
    }
}

fn validate_outbox_summary(summary: &v1::OutboxDeliverySummary) -> Result<(), PublicWireError> {
    validate_event_id_value(summary.event_id.as_ref())?;
    if !matches!(
        v1::OutboxDeliveryState::try_from(summary.state),
        Ok(v1::OutboxDeliveryState::Pending
            | v1::OutboxDeliveryState::RetryScheduled
            | v1::OutboxDeliveryState::Delivering
            | v1::OutboxDeliveryState::DeadLetter)
    ) {
        return Err(PublicWireError::InvalidEnum);
    }
    if summary.next_attempt_at.is_some() {
        validate_timestamp(summary.next_attempt_at.as_ref())?;
    }
    Ok(())
}

fn validate_outbox_page(page: Option<&v1::OutboxDeliveryPage>) -> Result<(), PublicWireError> {
    let page = page.ok_or(PublicWireError::MissingRequiredField)?;
    if page.items.len() > MAX_PAGE_ITEMS {
        return Err(PublicWireError::TooManyItems);
    }
    cursor(&page.next_cursor)?;
    if page.items.is_empty() && page.next_cursor.is_some() {
        return Err(PublicWireError::InconsistentFields);
    }
    let mut prior = None;
    for item in &page.items {
        validate_outbox_summary(item)?;
        let key = validate_event_id_value(item.event_id.as_ref())?;
        if prior.is_some_and(|prior| prior >= key) {
            return Err(PublicWireError::NonCanonical);
        }
        prior = Some(key);
    }
    Ok(())
}

fn validate_list_pending_outbox_deliveries_request(
    message: &v1::ListPendingOutboxDeliveriesRequest,
) -> Result<(), PublicWireError> {
    request_id(&message.request_id)?;
    validate_page_request(message.page.as_ref())
}

fn validate_list_pending_outbox_deliveries_response(
    message: &v1::ListPendingOutboxDeliveriesResponse,
) -> Result<(), PublicWireError> {
    validate_outbox_page(message.page.as_ref())
}

fn offline_maintenance_operation_id(bytes: &[u8]) -> Result<(), PublicWireError> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| PublicWireError::InvalidIdentity)?;
    OfflineMaintenanceOperationId::from_bytes(bytes)
        .map(|_| ())
        .map_err(|_| PublicWireError::InvalidIdentity)
}

fn validate_backup_name_v1(value: &str) -> Result<(), PublicWireError> {
    BackupNameV1::new(value.to_owned())
        .map(|_| ())
        .map_err(|_| PublicWireError::InvalidIdentity)
}

fn validate_offline_maintenance_operation(
    operation: &v1::OfflineMaintenanceOperation,
) -> Result<(), PublicWireError> {
    offline_maintenance_operation_id(&operation.operation_id)?;
    if !matches!(
        v1::OfflineMaintenanceOperationKind::try_from(operation.kind),
        Ok(v1::OfflineMaintenanceOperationKind::CreateBackup
            | v1::OfflineMaintenanceOperationKind::RestoreBackup)
    ) {
        return Err(PublicWireError::InvalidEnum);
    }
    validate_backup_name_v1(&operation.backup_name)?;
    hash(&operation.input_hash)?;
    let phase = v1::OfflineMaintenancePhase::try_from(operation.phase)
        .map_err(|_| PublicWireError::InvalidEnum)?;
    if phase == v1::OfflineMaintenancePhase::Unspecified {
        return Err(PublicWireError::InvalidEnum);
    }
    let failure = v1::OfflineMaintenanceFailureClass::try_from(operation.failure)
        .map_err(|_| PublicWireError::InvalidEnum)?;
    if (phase == v1::OfflineMaintenancePhase::FailedClosed)
        != (failure != v1::OfflineMaintenanceFailureClass::Unspecified)
    {
        return Err(PublicWireError::InconsistentFields);
    }
    Ok(())
}

fn validate_offline_maintenance_start(
    disposition: i32,
    operation: Option<&v1::OfflineMaintenanceOperation>,
) -> Result<(), PublicWireError> {
    let operation = operation.ok_or(PublicWireError::MissingRequiredField)?;
    validate_offline_maintenance_operation(operation)?;
    let disposition = v1::OfflineMaintenanceStartDisposition::try_from(disposition)
        .map_err(|_| PublicWireError::InvalidEnum)?;
    let terminal_phase = matches!(
        v1::OfflineMaintenancePhase::try_from(operation.phase),
        Ok(v1::OfflineMaintenancePhase::Succeeded | v1::OfflineMaintenancePhase::FailedClosed)
    );
    match disposition {
        v1::OfflineMaintenanceStartDisposition::Accepted
        | v1::OfflineMaintenanceStartDisposition::AlreadyAccepted
            if !terminal_phase =>
        {
            Ok(())
        }
        v1::OfflineMaintenanceStartDisposition::Terminal if terminal_phase => Ok(()),
        _ => Err(PublicWireError::InconsistentFields),
    }
}

fn validate_create_offline_backup_request(
    message: &v1::CreateOfflineBackupRequest,
) -> Result<(), PublicWireError> {
    request_id(&message.request_id)?;
    offline_maintenance_operation_id(&message.operation_id)?;
    validate_backup_name_v1(&message.backup_name)
}

fn validate_create_offline_backup_response(
    message: &v1::CreateOfflineBackupResponse,
) -> Result<(), PublicWireError> {
    validate_offline_maintenance_start(message.disposition, message.operation.as_ref())
}

fn validate_restore_offline_backup_request(
    message: &v1::RestoreOfflineBackupRequest,
) -> Result<(), PublicWireError> {
    request_id(&message.request_id)?;
    offline_maintenance_operation_id(&message.operation_id)?;
    validate_backup_name_v1(&message.backup_name)?;
    if matches!(
        v1::OfflineMaintenanceReplacementConfirmation::try_from(message.replacement_confirmation),
        Ok(v1::OfflineMaintenanceReplacementConfirmation::Unspecified
            | v1::OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget)
    ) {
        Ok(())
    } else {
        Err(PublicWireError::InvalidEnum)
    }
}

fn validate_restore_offline_backup_response(
    message: &v1::RestoreOfflineBackupResponse,
) -> Result<(), PublicWireError> {
    validate_offline_maintenance_start(message.disposition, message.operation.as_ref())
}

fn validate_get_offline_maintenance_operation_request(
    message: &v1::GetOfflineMaintenanceOperationRequest,
) -> Result<(), PublicWireError> {
    request_id(&message.request_id)?;
    offline_maintenance_operation_id(&message.operation_id)
}

fn validate_get_offline_maintenance_operation_response(
    message: &v1::GetOfflineMaintenanceOperationResponse,
) -> Result<(), PublicWireError> {
    match message
        .result
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        v1::get_offline_maintenance_operation_response::Result::NotFound(_) => Ok(()),
        v1::get_offline_maintenance_operation_response::Result::Found(operation) => {
            validate_offline_maintenance_operation(operation)
        }
    }
}

fn contract_migration_operation_id(bytes: &[u8]) -> Result<(), PublicWireError> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| PublicWireError::InvalidIdentity)?;
    ContractMigrationOperationId::from_bytes(bytes)
        .map(|_| ())
        .map_err(|_| PublicWireError::InvalidIdentity)
}

fn validate_contract_migration_operation(
    operation: &v1::ContractMigrationOperation,
) -> Result<(), PublicWireError> {
    contract_migration_operation_id(&operation.operation_id)?;
    if !matches!(
        v1::ContractMigrationOperationKind::try_from(operation.kind),
        Ok(v1::ContractMigrationOperationKind::Check | v1::ContractMigrationOperationKind::Apply)
    ) || !valid_bounded_text(&operation.contract_lineage, MAX_CONTRACT_LINEAGE_BYTES)
    {
        return Err(PublicWireError::InvalidEnum);
    }
    for identity in [
        &operation.input_hash,
        &operation.parent_bundle_hash,
        &operation.candidate_bundle_hash,
        &operation.migration_bundle_hash,
    ] {
        hash(identity)?;
    }
    let phase = v1::ContractMigrationPhase::try_from(operation.phase)
        .map_err(|_| PublicWireError::InvalidEnum)?;
    if phase == v1::ContractMigrationPhase::Unspecified {
        return Err(PublicWireError::InvalidEnum);
    }
    let failure = v1::ContractMigrationFailureClass::try_from(operation.failure)
        .map_err(|_| PublicWireError::InvalidEnum)?;
    let failed = matches!(
        phase,
        v1::ContractMigrationPhase::FailedClosed | v1::ContractMigrationPhase::FailedRolledBack
    );
    let backup_valid = if operation.backup_name.is_empty() {
        operation.backup_manifest_hash.is_empty()
    } else {
        validate_backup_name_v1(&operation.backup_name).is_ok()
            && operation.backup_manifest_hash.len() == 32
    };
    if failed != (failure != v1::ContractMigrationFailureClass::Unspecified) || !backup_valid {
        return Err(PublicWireError::InconsistentFields);
    }
    Ok(())
}

fn validate_contract_migration_start(
    disposition: i32,
    operation: Option<&v1::ContractMigrationOperation>,
) -> Result<(), PublicWireError> {
    let operation = operation.ok_or(PublicWireError::MissingRequiredField)?;
    validate_contract_migration_operation(operation)?;
    let disposition = v1::ContractMigrationStartDisposition::try_from(disposition)
        .map_err(|_| PublicWireError::InvalidEnum)?;
    let terminal = matches!(
        v1::ContractMigrationPhase::try_from(operation.phase),
        Ok(v1::ContractMigrationPhase::Succeeded
            | v1::ContractMigrationPhase::FailedClosed
            | v1::ContractMigrationPhase::FailedRolledBack)
    );
    match disposition {
        v1::ContractMigrationStartDisposition::Accepted
        | v1::ContractMigrationStartDisposition::AlreadyAccepted
            if !terminal =>
        {
            Ok(())
        }
        v1::ContractMigrationStartDisposition::Terminal
        | v1::ContractMigrationStartDisposition::AlreadyApplied
            if terminal =>
        {
            if disposition == v1::ContractMigrationStartDisposition::AlreadyApplied
                && (operation.kind != v1::ContractMigrationOperationKind::Apply as i32
                    || operation.phase != v1::ContractMigrationPhase::Succeeded as i32)
            {
                Err(PublicWireError::InconsistentFields)
            } else {
                Ok(())
            }
        }
        _ => Err(PublicWireError::InconsistentFields),
    }
}

fn validate_contract_migration_artifact_request(
    request_id_bytes: &[u8],
    operation_id: &[u8],
    candidate: &[u8],
    migration: &[u8],
) -> Result<(), PublicWireError> {
    request_id(request_id_bytes)?;
    contract_migration_operation_id(operation_id)?;
    if candidate.is_empty()
        || candidate.len() > 15 * 1_024 * 1_024
        || migration.is_empty()
        || migration.len() > 16 * 1_024 * 1_024
    {
        return Err(PublicWireError::InvalidBytes);
    }
    Ok(())
}

fn validate_check_contract_migration_request(
    message: &v1::CheckContractMigrationRequest,
) -> Result<(), PublicWireError> {
    validate_contract_migration_artifact_request(
        &message.request_id,
        &message.operation_id,
        &message.candidate_bundle,
        &message.migration_bundle,
    )
}

fn validate_check_contract_migration_response(
    message: &v1::CheckContractMigrationResponse,
) -> Result<(), PublicWireError> {
    validate_contract_migration_start(message.disposition, message.operation.as_ref())
}

fn validate_apply_contract_migration_request(
    message: &v1::ApplyContractMigrationRequest,
) -> Result<(), PublicWireError> {
    validate_contract_migration_artifact_request(
        &message.request_id,
        &message.operation_id,
        &message.candidate_bundle,
        &message.migration_bundle,
    )?;
    if message.confirmation
        != v1::ContractMigrationApplyConfirmation::AllowApplyContractMigration as i32
    {
        return Err(PublicWireError::InvalidEnum);
    }
    hash(&message.confirmed_migration_hash)
}

fn validate_apply_contract_migration_response(
    message: &v1::ApplyContractMigrationResponse,
) -> Result<(), PublicWireError> {
    validate_contract_migration_start(message.disposition, message.operation.as_ref())
}

fn validate_get_contract_migration_operation_request(
    message: &v1::GetContractMigrationOperationRequest,
) -> Result<(), PublicWireError> {
    request_id(&message.request_id)?;
    contract_migration_operation_id(&message.operation_id)
}

fn validate_get_contract_migration_operation_response(
    message: &v1::GetContractMigrationOperationResponse,
) -> Result<(), PublicWireError> {
    match message
        .result
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        v1::get_contract_migration_operation_response::Result::NotFound(_) => Ok(()),
        v1::get_contract_migration_operation_response::Result::Found(operation) => {
            validate_contract_migration_operation(operation)
        }
    }
}

fn application_installation_campaign_id(bytes: &[u8]) -> Result<(), PublicWireError> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| PublicWireError::InvalidIdentity)?;
    ApplicationInstallationCampaignId::from_bytes(bytes)
        .map(|_| ())
        .map_err(|_| PublicWireError::InvalidIdentity)
}

fn validate_application_installation_failure(
    failure: &v1::ApplicationInstallationFailure,
) -> Result<(), PublicWireError> {
    let stage = v1::ApplicationInstallationStage::try_from(failure.stage)
        .map_err(|_| PublicWireError::InvalidEnum)?;
    let code = v1::ApplicationInstallationFailureCode::try_from(failure.code)
        .map_err(|_| PublicWireError::InvalidEnum)?;
    let action = v1::ApplicationInstallationNextAction::try_from(failure.next_action)
        .map_err(|_| PublicWireError::InvalidEnum)?;
    if stage == v1::ApplicationInstallationStage::Unspecified
        || code == v1::ApplicationInstallationFailureCode::Unspecified
        || matches!(
            action,
            v1::ApplicationInstallationNextAction::Unspecified
                | v1::ApplicationInstallationNextAction::None
        )
    {
        return Err(PublicWireError::InvalidEnum);
    }
    Ok(())
}

fn validate_application_installation_observation(
    observation: &v1::ApplicationInstallationObservation,
) -> Result<(), PublicWireError> {
    application_installation_campaign_id(&observation.campaign_id)?;
    hash(&observation.plan_hash)?;
    if !valid_bounded_text(&observation.contract_lineage, MAX_CONTRACT_LINEAGE_BYTES)
        || observation.completed_stages.len() > 10
    {
        return Err(PublicWireError::InvalidIdentity);
    }
    for (index, stage) in observation.completed_stages.iter().enumerate() {
        if *stage != i32::try_from(index + 1).map_err(|_| PublicWireError::TooManyItems)? {
            return Err(PublicWireError::NonCanonical);
        }
    }
    let phase = v1::ApplicationInstallationPhase::try_from(observation.phase)
        .map_err(|_| PublicWireError::InvalidEnum)?;
    let next_stage = v1::ApplicationInstallationStage::try_from(observation.next_stage)
        .map_err(|_| PublicWireError::InvalidEnum)?;
    let next_action = v1::ApplicationInstallationNextAction::try_from(observation.next_action)
        .map_err(|_| PublicWireError::InvalidEnum)?;
    if phase == v1::ApplicationInstallationPhase::Unspecified
        || next_action == v1::ApplicationInstallationNextAction::Unspecified
    {
        return Err(PublicWireError::InvalidEnum);
    }
    let expected_next = i32::try_from(observation.completed_stages.len() + 1)
        .map_err(|_| PublicWireError::TooManyItems)?;
    match phase {
        v1::ApplicationInstallationPhase::Running => {
            if observation.failure.is_some()
                || !observation.receipt_hash.is_empty()
                || observation.completed_stages.len() >= 10
                || observation.next_stage != expected_next
                || next_stage == v1::ApplicationInstallationStage::Unspecified
                || next_action == v1::ApplicationInstallationNextAction::None
            {
                return Err(PublicWireError::InconsistentFields);
            }
        }
        v1::ApplicationInstallationPhase::Partial => {
            let failure = observation
                .failure
                .as_ref()
                .ok_or(PublicWireError::MissingRequiredField)?;
            validate_application_installation_failure(failure)?;
            if !observation.receipt_hash.is_empty()
                || observation.completed_stages.len() >= 10
                || observation.next_stage != expected_next
                || failure.stage != observation.next_stage
                || failure.next_action != observation.next_action
                || next_action == v1::ApplicationInstallationNextAction::None
            {
                return Err(PublicWireError::InconsistentFields);
            }
        }
        v1::ApplicationInstallationPhase::Installed => {
            if observation.completed_stages.len() != 10
                || next_stage != v1::ApplicationInstallationStage::Unspecified
                || next_action != v1::ApplicationInstallationNextAction::None
                || observation.failure.is_some()
            {
                return Err(PublicWireError::InconsistentFields);
            }
            hash(&observation.receipt_hash)?;
        }
        v1::ApplicationInstallationPhase::Unspecified => {
            return Err(PublicWireError::InvalidEnum);
        }
    }
    Ok(())
}

fn validate_start_application_installation_request(
    request: &v1::StartApplicationInstallationRequest,
) -> Result<(), PublicWireError> {
    request_id(&request.request_id)?;
    application_installation_campaign_id(&request.campaign_id)?;
    if request.canonical_plan.is_empty() || request.canonical_plan.len() > 4 * 1_024 * 1_024 {
        return Err(PublicWireError::InvalidBytes);
    }
    if let Some(completion) = request.external_completion.as_ref() {
        validate_application_installation_external_completion(completion)?;
    }
    Ok(())
}

fn validate_application_installation_external_completion(
    completion: &v1::ApplicationInstallationExternalCompletion,
) -> Result<(), PublicWireError> {
    match completion
        .completion
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        v1::application_installation_external_completion::Completion::DriverProof(proof) => {
            if proof.drivers.is_empty() || proof.drivers.len() > MAX_INSTALLATION_DRIVERS {
                return Err(PublicWireError::TooManyItems);
            }
            let mut previous = 0;
            for &driver in &proof.drivers {
                let driver = v1::ApplicationInstallationDriver::try_from(driver)
                    .map_err(|_| PublicWireError::InvalidEnum)?;
                if driver == v1::ApplicationInstallationDriver::Unspecified {
                    return Err(PublicWireError::InvalidEnum);
                }
                let current = driver as i32;
                if current <= previous {
                    return Err(PublicWireError::NonCanonical);
                }
                previous = current;
            }
            Ok(())
        }
        v1::application_installation_external_completion::Completion::SeedReceipts(receipts) => {
            if receipts.seeds.is_empty() || receipts.seeds.len() > MAX_INSTALLATION_SEEDS {
                return Err(PublicWireError::TooManyItems);
            }
            let mut previous_name: Option<&str> = None;
            for seed in &receipts.seeds {
                if !valid_installation_symbol(&seed.name) {
                    return Err(PublicWireError::InvalidIdentity);
                }
                if previous_name.is_some_and(|previous| previous >= seed.name.as_str()) {
                    return Err(PublicWireError::NonCanonical);
                }
                hash(&seed.content_hash)?;
                if seed
                    .succeeded
                    .checked_add(seed.replayed)
                    .filter(|total| *total != 0)
                    .is_none()
                {
                    return Err(PublicWireError::InvalidIdentity);
                }
                previous_name = Some(seed.name.as_str());
            }
            Ok(())
        }
    }
}

fn valid_installation_symbol(value: &str) -> bool {
    valid_ascii(value, MAX_INSTALLATION_SYMBOL_BYTES)
        && value.bytes().enumerate().all(|(index, byte)| {
            byte == b'_'
                || byte == b'-'
                || byte == b'.'
                || byte.is_ascii_alphabetic()
                || index > 0 && byte.is_ascii_digit()
        })
}

fn validate_start_application_installation_response(
    response: &v1::StartApplicationInstallationResponse,
) -> Result<(), PublicWireError> {
    let observation = response
        .observation
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?;
    validate_application_installation_observation(observation)?;
    let installed = observation.phase == v1::ApplicationInstallationPhase::Installed as i32;
    if installed != !response.canonical_receipt.is_empty() {
        return Err(PublicWireError::InconsistentFields);
    }
    if installed
        && observation.receipt_hash.as_slice()
            != hash_application_installation_receipt(&response.canonical_receipt).as_bytes()
    {
        return Err(PublicWireError::InconsistentFields);
    }
    Ok(())
}

fn validate_get_application_installation_request(
    request: &v1::GetApplicationInstallationRequest,
) -> Result<(), PublicWireError> {
    request_id(&request.request_id)?;
    application_installation_campaign_id(&request.campaign_id)
}

fn validate_get_application_installation_response(
    response: &v1::GetApplicationInstallationResponse,
) -> Result<(), PublicWireError> {
    match response
        .result
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        v1::get_application_installation_response::Result::NotFound(_) => Ok(()),
        v1::get_application_installation_response::Result::Found(found) => {
            validate_start_application_installation_response(found)
        }
    }
}

fn application_export_operation_id(bytes: &[u8]) -> Result<(), PublicWireError> {
    if valid_uuid(bytes, ApplicationExportOperationId::from_bytes) {
        Ok(())
    } else {
        Err(PublicWireError::InvalidUuidV7)
    }
}

fn validate_application_export_selection(
    selection: Option<&v1::ApplicationExportSelection>,
) -> Result<(), PublicWireError> {
    let selection = selection.ok_or(PublicWireError::MissingRequiredField)?;
    ContractLineage::new(selection.contract_lineage.clone())
        .map_err(|_| PublicWireError::InvalidIdentity)?;
    if !matches!(
        v1::CapabilityApplicationExportScope::try_from(selection.scope),
        Ok(v1::CapabilityApplicationExportScope::PrincipalFiltered
            | v1::CapabilityApplicationExportScope::WholeApplication)
    ) {
        return Err(PublicWireError::InvalidEnum);
    }
    if !selection.entities && !selection.events {
        return Err(PublicWireError::InconsistentFields);
    }
    Ok(())
}

fn validate_application_export_snapshot(
    snapshot: Option<&v1::ApplicationExportSnapshotBinding>,
) -> Result<(), PublicWireError> {
    let snapshot = snapshot.ok_or(PublicWireError::MissingRequiredField)?;
    if !valid_uuid(&snapshot.database_id, DatabaseId::from_bytes) {
        return Err(PublicWireError::InvalidUuidV7);
    }
    if snapshot.history_incarnation == 0 || snapshot.contract_version == 0 {
        return Err(PublicWireError::InvalidIdentity);
    }
    hash(&snapshot.contract_bundle_hash)?;
    for identities in [
        snapshot.query_module_hashes.as_slice(),
        snapshot.reactive_module_hashes.as_slice(),
    ] {
        if identities.len() > MAX_APPLICATION_EXPORT_MODULES {
            return Err(PublicWireError::TooManyItems);
        }
        let mut previous: Option<&[u8]> = None;
        for identity in identities {
            hash(identity)?;
            if previous.is_some_and(|prior| prior >= identity.as_slice()) {
                return Err(PublicWireError::NonCanonical);
            }
            previous = Some(identity);
        }
    }
    Ok(())
}

fn valid_application_export_json(value: &[u8], maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && std::str::from_utf8(value).is_ok()
        && !value.contains(&b'\n')
        && !value.contains(&b'\r')
        && value.first() == Some(&b'{')
        && value.last() == Some(&b'}')
}

fn validate_application_export_operation(
    operation: &v1::ApplicationExportOperation,
) -> Result<(), PublicWireError> {
    application_export_operation_id(&operation.operation_id)?;
    validate_application_export_selection(operation.selection.as_ref())?;
    validate_application_export_snapshot(operation.snapshot.as_ref())?;
    validate_timestamp(operation.lease_expires_at.as_ref())?;
    let phase = v1::ApplicationExportPhase::try_from(operation.phase)
        .map_err(|_| PublicWireError::InvalidEnum)?;
    let failure = v1::ApplicationExportFailure::try_from(operation.failure)
        .map_err(|_| PublicWireError::InvalidEnum)?;
    let terminal = matches!(
        phase,
        v1::ApplicationExportPhase::Completed
            | v1::ApplicationExportPhase::Cancelled
            | v1::ApplicationExportPhase::Expired
            | v1::ApplicationExportPhase::FailedClosed
    );
    let documents_present = !operation.canonical_manifest_json.is_empty()
        && !operation.canonical_receipt_json.is_empty()
        && !operation.manifest_hash.is_empty()
        && !operation.receipt_hash.is_empty();
    let documents_absent = operation.canonical_manifest_json.is_empty()
        && operation.canonical_receipt_json.is_empty()
        && operation.manifest_hash.is_empty()
        && operation.receipt_hash.is_empty();
    if phase == v1::ApplicationExportPhase::Unspecified
        || terminal && !documents_present
        || !terminal && !documents_absent
    {
        return Err(PublicWireError::InconsistentFields);
    }
    match phase {
        v1::ApplicationExportPhase::Accepted | v1::ApplicationExportPhase::Exporting
            if failure == v1::ApplicationExportFailure::Unspecified => {}
        v1::ApplicationExportPhase::Completed
            if failure == v1::ApplicationExportFailure::Unspecified => {}
        v1::ApplicationExportPhase::Cancelled
            if failure == v1::ApplicationExportFailure::Cancelled => {}
        v1::ApplicationExportPhase::Expired
            if failure == v1::ApplicationExportFailure::LeaseExpired => {}
        v1::ApplicationExportPhase::FailedClosed
            if matches!(
                failure,
                v1::ApplicationExportFailure::AuthorityChanged
                    | v1::ApplicationExportFailure::SnapshotUnavailable
                    | v1::ApplicationExportFailure::SourceInvalid
                    | v1::ApplicationExportFailure::LimitExceeded
                    | v1::ApplicationExportFailure::Internal
            ) => {}
        _ => return Err(PublicWireError::InconsistentFields),
    }
    if terminal {
        if !valid_application_export_json(
            &operation.canonical_manifest_json,
            MAX_APPLICATION_EXPORT_TERMINAL_DOCUMENT_BYTES,
        ) || !valid_application_export_json(
            &operation.canonical_receipt_json,
            MAX_APPLICATION_EXPORT_TERMINAL_DOCUMENT_BYTES,
        ) {
            return Err(PublicWireError::InvalidBytes);
        }
        hash(&operation.manifest_hash)?;
        hash(&operation.receipt_hash)?;
        if operation.manifest_hash.as_slice()
            != hash_application_export_manifest(&operation.canonical_manifest_json).as_bytes()
            || operation.receipt_hash.as_slice()
                != hash_application_export_receipt(&operation.canonical_receipt_json).as_bytes()
        {
            return Err(PublicWireError::InconsistentFields);
        }
    }
    Ok(())
}

fn validate_start_application_export_request(
    request: &v1::StartApplicationExportRequest,
) -> Result<(), PublicWireError> {
    request_id(&request.request_id)?;
    application_export_operation_id(&request.operation_id)?;
    validate_application_export_selection(request.selection.as_ref())?;
    if !(MIN_APPLICATION_EXPORT_LEASE_SECONDS..=MAX_APPLICATION_EXPORT_LEASE_SECONDS)
        .contains(&request.lease_seconds)
    {
        return Err(PublicWireError::InvalidValue);
    }
    let manifest = &request.canonical_portability_manifest_json;
    if !manifest.is_empty()
        && (manifest.len() > MAX_PUBLIC_REQUEST_BYTES
            || std::str::from_utf8(manifest).is_err()
            || manifest.first() != Some(&b'{')
            || manifest.last() != Some(&b'\n'))
    {
        return Err(PublicWireError::InvalidBytes);
    }
    Ok(())
}

fn validate_start_application_export_response(
    response: &v1::StartApplicationExportResponse,
) -> Result<(), PublicWireError> {
    let operation = response
        .operation
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?;
    validate_application_export_operation(operation)?;
    let disposition = v1::ApplicationExportStartDisposition::try_from(response.disposition)
        .map_err(|_| PublicWireError::InvalidEnum)?;
    let phase = v1::ApplicationExportPhase::try_from(operation.phase)
        .map_err(|_| PublicWireError::InvalidEnum)?;
    let terminal = matches!(
        phase,
        v1::ApplicationExportPhase::Completed
            | v1::ApplicationExportPhase::Cancelled
            | v1::ApplicationExportPhase::Expired
            | v1::ApplicationExportPhase::FailedClosed
    );
    if response.cursor.len() > MAX_APPLICATION_EXPORT_CURSOR_BYTES
        || terminal != response.cursor.is_empty()
    {
        return Err(PublicWireError::InconsistentFields);
    }
    match disposition {
        v1::ApplicationExportStartDisposition::Accepted
        | v1::ApplicationExportStartDisposition::AlreadyAccepted
            if !terminal =>
        {
            Ok(())
        }
        v1::ApplicationExportStartDisposition::Terminal if terminal => Ok(()),
        _ => Err(PublicWireError::InconsistentFields),
    }
}

fn validate_get_application_export_page_request(
    request: &v1::GetApplicationExportPageRequest,
) -> Result<(), PublicWireError> {
    request_id(&request.request_id)?;
    application_export_operation_id(&request.operation_id)?;
    if request.cursor.is_empty()
        || request.cursor.len() > MAX_APPLICATION_EXPORT_CURSOR_BYTES
        || request.max_rows == 0
        || usize::try_from(request.max_rows)
            .map_or(true, |rows| rows > MAX_APPLICATION_EXPORT_PAGE_ROWS)
    {
        return Err(PublicWireError::InvalidValue);
    }
    Ok(())
}

fn validate_application_export_page(
    page: Option<&v1::ApplicationExportPage>,
) -> Result<(), PublicWireError> {
    let page = page.ok_or(PublicWireError::MissingRequiredField)?;
    application_export_operation_id(&page.operation_id)?;
    let page_number =
        std::num::NonZeroU64::new(page.page_number).ok_or(PublicWireError::InvalidIdentity)?;
    let class = match v1::ApplicationExportRecordClass::try_from(page.record_class)
        .map_err(|_| PublicWireError::InvalidEnum)?
    {
        v1::ApplicationExportRecordClass::Entity => ApplicationExportClassV1::Entity,
        v1::ApplicationExportRecordClass::Event => ApplicationExportClassV1::Event,
        v1::ApplicationExportRecordClass::Provenance => ApplicationExportClassV1::Provenance,
        v1::ApplicationExportRecordClass::PublicAudit => ApplicationExportClassV1::PublicAudit,
        v1::ApplicationExportRecordClass::Unspecified => {
            return Err(PublicWireError::InvalidEnum);
        }
    };
    if page.canonical_json_lines.len() > MAX_APPLICATION_EXPORT_PAGE_ROWS
        || page.next_cursor.len() > MAX_APPLICATION_EXPORT_CURSOR_BYTES
        || page.operation_complete != page.next_cursor.is_empty()
        || page.operation_complete && !page.class_complete
    {
        return Err(PublicWireError::InconsistentFields);
    }
    let mut total = 0usize;
    for line in &page.canonical_json_lines {
        if !valid_application_export_json(line, MAX_APPLICATION_EXPORT_JSON_LINE_BYTES) {
            return Err(PublicWireError::InvalidBytes);
        }
        total = total
            .checked_add(line.len() + 1)
            .ok_or(PublicWireError::MessageTooLarge)?;
    }
    if total > MAX_APPLICATION_EXPORT_PAGE_BYTES {
        return Err(PublicWireError::MessageTooLarge);
    }
    hash(&page.page_hash)?;
    let operation_id = ApplicationExportOperationId::from_bytes(
        page.operation_id
            .as_slice()
            .try_into()
            .map_err(|_| PublicWireError::InvalidUuidV7)?,
    )
    .map_err(|_| PublicWireError::InvalidUuidV7)?;
    let lines = page
        .canonical_json_lines
        .iter()
        .map(Vec::as_slice)
        .collect::<Vec<_>>();
    let preimage = canonical_application_export_page_preimage(
        operation_id,
        page_number,
        class,
        &lines,
        page.class_complete,
        page.operation_complete,
    )
    .map_err(|_| PublicWireError::MessageTooLarge)?;
    if page.page_hash.as_slice() != hash_application_export_page(&preimage).as_bytes() {
        return Err(PublicWireError::InconsistentFields);
    }
    Ok(())
}

fn validate_get_application_export_page_response(
    response: &v1::GetApplicationExportPageResponse,
) -> Result<(), PublicWireError> {
    validate_application_export_page(response.page.as_ref())
}

fn validate_application_export_operation_request(
    request_id_bytes: &[u8],
    operation_id: &[u8],
) -> Result<(), PublicWireError> {
    request_id(request_id_bytes)?;
    application_export_operation_id(operation_id)
}

fn validate_get_application_export_request(
    request: &v1::GetApplicationExportRequest,
) -> Result<(), PublicWireError> {
    validate_application_export_operation_request(&request.request_id, &request.operation_id)
}

fn validate_cancel_application_export_request(
    request: &v1::CancelApplicationExportRequest,
) -> Result<(), PublicWireError> {
    validate_application_export_operation_request(&request.request_id, &request.operation_id)
}

fn validate_get_application_export_response(
    response: &v1::GetApplicationExportResponse,
) -> Result<(), PublicWireError> {
    match response
        .result
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        v1::get_application_export_response::Result::NotFound(_) => Ok(()),
        v1::get_application_export_response::Result::Found(operation) => {
            validate_application_export_operation(operation)
        }
    }
}

fn validate_cancel_application_export_response(
    response: &v1::CancelApplicationExportResponse,
) -> Result<(), PublicWireError> {
    match response
        .result
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        v1::cancel_application_export_response::Result::NotFound(_) => Ok(()),
        v1::cancel_application_export_response::Result::Found(operation) => {
            validate_application_export_operation(operation)
        }
    }
}

fn schema_key(key: Option<&v1::SchemaArtifactKey>) -> Result<(u8, u32), PublicWireError> {
    let (kind, owner) = match key
        .and_then(|key| key.artifact.as_ref())
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        v1::schema_artifact_key::Artifact::EntityId(value) => (1, *value),
        v1::schema_artifact_key::Artifact::EventTypeId(value) => (2, *value),
        v1::schema_artifact_key::Artifact::CommandInputId(value) => (3, *value),
        v1::schema_artifact_key::Artifact::CommandOutcomeUnionId(value) => (4, *value),
        v1::schema_artifact_key::Artifact::ProjectionResultId(value) => (5, *value),
    };
    if owner == 0 {
        Err(PublicWireError::InvalidIdentity)
    } else {
        Ok((kind, owner))
    }
}

fn validate_generated_schema_identity(
    identity: Option<&v1::GeneratedSchemaIdentity>,
) -> Result<(u8, u32), PublicWireError> {
    let identity = identity.ok_or(PublicWireError::MissingRequiredField)?;
    let key = schema_key(identity.key.as_ref())?;
    hash(&identity.schema_hash)?;
    Ok(key)
}

fn valid_mcp_tool_name(value: &str) -> bool {
    value.len() <= MAX_MCP_COMMAND_TOOL_NAME_BYTES
        && value.starts_with("riffdb_cmd_")
        && value.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

fn normalized_mcp_segment_matches(source: &str, normalized: &str) -> bool {
    source.is_ascii()
        && source
            .bytes()
            .map(|byte| byte.to_ascii_lowercase())
            .eq(normalized.bytes())
}

fn mcp_tool_name_matches_declared_source(
    value: &str,
    contract_source: &str,
    command_source: Option<&str>,
) -> bool {
    if !contract_source.is_ascii() {
        return false;
    }
    let mut prefix = String::from("riffdb_cmd_");
    prefix.extend(
        contract_source
            .bytes()
            .map(|byte| char::from(byte.to_ascii_lowercase())),
    );
    prefix.push('_');
    let Some(command) = value.strip_prefix(&prefix) else {
        return false;
    };
    command_source.is_none_or(|source| normalized_mcp_segment_matches(source, command))
}

fn validate_command_tool_descriptor(
    descriptor: &v1::CommandToolDescriptor,
) -> Result<(), PublicWireError> {
    if !valid_mcp_tool_name(&descriptor.tool_name)
        || !valid_name(&descriptor.source_command)
        || !valid_bounded_text(&descriptor.contract_lineage, MAX_CONTRACT_LINEAGE_BYTES)
        || descriptor.contract_version == 0
        || descriptor.command_id == 0
    {
        return Err(PublicWireError::InvalidIdentity);
    }
    if !mcp_tool_name_matches_declared_source(
        &descriptor.tool_name,
        &descriptor.contract_lineage,
        Some(&descriptor.source_command),
    ) {
        return Err(PublicWireError::InconsistentFields);
    }
    if validate_schema_artifact(descriptor.input_schema.as_ref())? != (3, descriptor.command_id)
        || validate_schema_artifact(descriptor.outcome_schema.as_ref())?
            != (4, descriptor.command_id)
    {
        return Err(PublicWireError::InconsistentFields);
    }
    Ok(())
}

fn validate_compact_command_tool_descriptor(
    descriptor: &v1::CompactCommandToolDescriptor,
) -> Result<(), PublicWireError> {
    if !valid_mcp_tool_name(&descriptor.tool_name)
        || !valid_name(&descriptor.source_command)
        || !valid_bounded_text(&descriptor.contract_lineage, MAX_CONTRACT_LINEAGE_BYTES)
        || descriptor.contract_version == 0
        || descriptor.command_id == 0
    {
        return Err(PublicWireError::InvalidIdentity);
    }
    if !mcp_tool_name_matches_declared_source(
        &descriptor.tool_name,
        &descriptor.contract_lineage,
        Some(&descriptor.source_command),
    ) {
        return Err(PublicWireError::InconsistentFields);
    }
    if validate_generated_schema_identity(descriptor.input_schema.as_ref())?
        != (3, descriptor.command_id)
        || validate_generated_schema_identity(descriptor.outcome_schema.as_ref())?
            != (4, descriptor.command_id)
    {
        return Err(PublicWireError::InconsistentFields);
    }
    Ok(())
}

fn validate_named_query_schema_artifact(
    artifact: Option<&v1::NamedQueryToolSchemaArtifact>,
) -> Result<(), PublicWireError> {
    let artifact = artifact.ok_or(PublicWireError::MissingRequiredField)?;
    hash(&artifact.schema_hash)?;
    if artifact.canonical_json.is_empty()
        || artifact.canonical_json.len() > MAX_OPERATION_SCHEMA_BYTES
        || artifact.schema_hash != hash_schema(artifact.canonical_json.as_bytes()).as_bytes()
    {
        return Err(PublicWireError::InvalidBytes);
    }
    Ok(())
}

fn query_tool_segment(name: &str) -> String {
    let mut output = String::new();
    let mut word_start = true;
    for character in name.chars() {
        if !character.is_ascii_alphanumeric() {
            word_start = true;
            continue;
        }
        if (character.is_ascii_uppercase() && !word_start) || (word_start && !output.is_empty()) {
            output.push('_');
        }
        output.push(character.to_ascii_lowercase());
        word_start = false;
    }
    output
}

fn validate_named_query_tool_descriptor(
    descriptor: &v1::NamedQueryToolDescriptor,
) -> Result<(), PublicWireError> {
    let expected = format!(
        "{}_{}",
        query_tool_segment(&descriptor.query_module_name),
        query_tool_segment(&descriptor.source_query)
    );
    if descriptor.tool_name != expected
        || descriptor.tool_name.len() > MAX_MCP_COMMAND_TOOL_NAME_BYTES
        || !valid_name(&descriptor.source_query)
        || !valid_name(&descriptor.query_module_name)
        || !valid_bounded_text(&descriptor.contract_lineage, MAX_CONTRACT_LINEAGE_BYTES)
        || descriptor.contract_version == 0
        || descriptor.query_module_version == 0
    {
        return Err(PublicWireError::InvalidIdentity);
    }
    hash(&descriptor.query_module_hash)?;
    validate_named_query_schema_artifact(descriptor.input_schema.as_ref())?;
    validate_named_query_schema_artifact(descriptor.result_schema.as_ref())
}

fn validate_compact_named_query_tool_descriptor(
    descriptor: &v1::CompactNamedQueryToolDescriptor,
) -> Result<(), PublicWireError> {
    let expected = format!(
        "{}_{}",
        query_tool_segment(&descriptor.query_module_name),
        query_tool_segment(&descriptor.source_query)
    );
    if descriptor.tool_name != expected
        || descriptor.tool_name.len() > MAX_MCP_COMMAND_TOOL_NAME_BYTES
        || !valid_name(&descriptor.source_query)
        || !valid_name(&descriptor.query_module_name)
        || !valid_bounded_text(&descriptor.contract_lineage, MAX_CONTRACT_LINEAGE_BYTES)
        || descriptor.contract_version == 0
        || descriptor.query_module_version == 0
    {
        return Err(PublicWireError::InvalidIdentity);
    }
    hash(&descriptor.query_module_hash)?;
    hash(&descriptor.input_schema_hash)?;
    hash(&descriptor.result_schema_hash)
}

fn hash_matches_hex(bytes: &[u8], expected: &str) -> bool {
    bytes.len() == 32
        && expected.len() == 64
        && bytes.iter().enumerate().all(|(index, byte)| {
            u8::from_str_radix(&expected[index * 2..index * 2 + 2], 16) == Ok(*byte)
        })
}

fn validate_operation_schema_artifact(
    artifact: Option<&v1::OperationSchemaArtifact>,
    schema_id: &str,
    expected_hash: &str,
) -> Result<(), PublicWireError> {
    let artifact = artifact.ok_or(PublicWireError::MissingRequiredField)?;
    if artifact.schema_id != schema_id
        || artifact.dialect != JSON_SCHEMA_DIALECT
        || artifact.canonical_json.is_empty()
        || artifact.canonical_json.len() > MAX_OPERATION_SCHEMA_BYTES
        || artifact.canonical_json.as_bytes().last() != Some(&b'}')
        || artifact
            .canonical_json
            .bytes()
            .any(|byte| matches!(byte, b'\r' | b'\n'))
        || !hash_matches_hex(&artifact.schema_hash, expected_hash)
        || artifact.schema_hash != hash_schema(artifact.canonical_json.as_bytes()).as_bytes()
    {
        Err(PublicWireError::InconsistentFields)
    } else {
        Ok(())
    }
}

fn validate_operation_schema_catalog(
    catalog: Option<&v1::OperationSchemaCatalog>,
) -> Result<(), PublicWireError> {
    let catalog = catalog.ok_or(PublicWireError::MissingRequiredField)?;
    validate_operation_schema_artifact(
        catalog.command_operation_envelope.as_ref(),
        OPERATION_ENVELOPE_SCHEMA_ID,
        OPERATION_ENVELOPE_SCHEMA_HASH,
    )?;
    validate_operation_schema_artifact(
        catalog.command_get_outcome_result.as_ref(),
        GET_OUTCOME_RESULT_SCHEMA_ID,
        GET_OUTCOME_RESULT_SCHEMA_HASH,
    )
}

fn validate_operation_schema_identity(
    identity: Option<&v1::OperationSchemaIdentity>,
    schema_id: &str,
    expected_hash: &str,
) -> Result<(), PublicWireError> {
    let identity = identity.ok_or(PublicWireError::MissingRequiredField)?;
    if identity.schema_id == schema_id && hash_matches_hex(&identity.schema_hash, expected_hash) {
        Ok(())
    } else {
        Err(PublicWireError::InconsistentFields)
    }
}

fn validate_operation_schema_catalog_identity(
    identity: Option<&v1::OperationSchemaCatalogIdentity>,
) -> Result<(), PublicWireError> {
    let identity = identity.ok_or(PublicWireError::MissingRequiredField)?;
    validate_operation_schema_identity(
        identity.command_operation_envelope.as_ref(),
        OPERATION_ENVELOPE_SCHEMA_ID,
        OPERATION_ENVELOPE_SCHEMA_HASH,
    )?;
    validate_operation_schema_identity(
        identity.command_get_outcome_result.as_ref(),
        GET_OUTCOME_RESULT_SCHEMA_ID,
        GET_OUTCOME_RESULT_SCHEMA_HASH,
    )
}

fn operation_catalog_matches_fence(
    catalog: &v1::OperationSchemaCatalog,
    fence: &v1::DiscoveryCatalogFence,
) -> bool {
    let Some(identity) = fence.operation_schemas.as_ref() else {
        return false;
    };
    let Some(envelope) = catalog.command_operation_envelope.as_ref() else {
        return false;
    };
    let Some(get_outcome) = catalog.command_get_outcome_result.as_ref() else {
        return false;
    };
    identity
        .command_operation_envelope
        .as_ref()
        .is_some_and(|value| {
            value.schema_id == envelope.schema_id && value.schema_hash == envelope.schema_hash
        })
        && identity
            .command_get_outcome_result
            .as_ref()
            .is_some_and(|value| {
                value.schema_id == get_outcome.schema_id
                    && value.schema_hash == get_outcome.schema_hash
            })
}

fn validate_discovery_fence(
    fence: Option<&v1::DiscoveryCatalogFence>,
) -> Result<(), PublicWireError> {
    let fence = fence.ok_or(PublicWireError::MissingRequiredField)?;
    if fence.history_incarnation == 0 {
        return Err(PublicWireError::InvalidIdentity);
    }
    match fence
        .state
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        v1::discovery_catalog_fence::State::NoActiveContract(_) => {}
        v1::discovery_catalog_fence::State::ActiveContract(active) => {
            if !valid_bounded_text(&active.contract_lineage, MAX_CONTRACT_LINEAGE_BYTES)
                || active.contract_version == 0
            {
                return Err(PublicWireError::InvalidIdentity);
            }
            hash(&active.bundle_hash)?;
            if !active.active_query_module_hash.is_empty() {
                hash(&active.active_query_module_hash)?;
            }
        }
    }
    if fence.server_generation.len() != 16 {
        return Err(PublicWireError::InvalidBytes);
    }
    validate_operation_schema_catalog_identity(fence.operation_schemas.as_ref())
}

fn validate_fixed_tool(value: i32) -> Result<u8, PublicWireError> {
    match v1::FixedToolKind::try_from(value) {
        Ok(v1::FixedToolKind::Unspecified) | Err(_) => Err(PublicWireError::InvalidEnum),
        Ok(value) => u8::try_from(value as i32).map_err(|_| PublicWireError::InvalidEnum),
    }
}

fn validate_command_tool_items(
    items: &[v1::CommandToolDiscoveryItem],
) -> Result<(), PublicWireError> {
    let mut prior_fixed = None;
    let mut prior_command: Option<&str> = None;
    for item in items {
        match item
            .item
            .as_ref()
            .ok_or(PublicWireError::MissingRequiredField)?
        {
            v1::command_tool_discovery_item::Item::FixedTool(value) if prior_command.is_none() => {
                let value = validate_fixed_tool(*value)?;
                if prior_fixed.is_some_and(|prior| prior >= value) {
                    return Err(PublicWireError::NonCanonical);
                }
                prior_fixed = Some(value);
            }
            v1::command_tool_discovery_item::Item::CommandTool(descriptor) => {
                validate_command_tool_descriptor(descriptor)?;
                if prior_command.is_some_and(|prior| prior >= descriptor.tool_name.as_str()) {
                    return Err(PublicWireError::NonCanonical);
                }
                prior_command = Some(&descriptor.tool_name);
            }
            v1::command_tool_discovery_item::Item::NamedQueryTool(descriptor) => {
                validate_named_query_tool_descriptor(descriptor)?;
                if prior_command.is_some_and(|prior| prior >= descriptor.tool_name.as_str()) {
                    return Err(PublicWireError::NonCanonical);
                }
                prior_command = Some(&descriptor.tool_name);
            }
            v1::command_tool_discovery_item::Item::FixedTool(_) => {
                return Err(PublicWireError::NonCanonical);
            }
        }
    }
    Ok(())
}

fn validate_compact_command_tool_items(
    items: &[v1::CompactCommandToolDiscoveryItem],
) -> Result<(), PublicWireError> {
    let mut prior_fixed = None;
    let mut prior_command: Option<&str> = None;
    for item in items {
        match item
            .item
            .as_ref()
            .ok_or(PublicWireError::MissingRequiredField)?
        {
            v1::compact_command_tool_discovery_item::Item::FixedTool(value)
                if prior_command.is_none() =>
            {
                let value = validate_fixed_tool(*value)?;
                if prior_fixed.is_some_and(|prior| prior >= value) {
                    return Err(PublicWireError::NonCanonical);
                }
                prior_fixed = Some(value);
            }
            v1::compact_command_tool_discovery_item::Item::CommandTool(descriptor) => {
                validate_compact_command_tool_descriptor(descriptor)?;
                if prior_command.is_some_and(|prior| prior >= descriptor.tool_name.as_str()) {
                    return Err(PublicWireError::NonCanonical);
                }
                prior_command = Some(&descriptor.tool_name);
            }
            v1::compact_command_tool_discovery_item::Item::NamedQueryTool(descriptor) => {
                validate_compact_named_query_tool_descriptor(descriptor)?;
                if prior_command.is_some_and(|prior| prior >= descriptor.tool_name.as_str()) {
                    return Err(PublicWireError::NonCanonical);
                }
                prior_command = Some(&descriptor.tool_name);
            }
            v1::compact_command_tool_discovery_item::Item::FixedTool(_) => {
                return Err(PublicWireError::NonCanonical);
            }
        }
    }
    Ok(())
}

fn validate_discovery_page_shape<T>(
    items: &[T],
    cursor_value: &Option<Vec<u8>>,
    fence: Option<&v1::DiscoveryCatalogFence>,
) -> Result<(), PublicWireError> {
    if items.len() > MAX_PAGE_ITEMS {
        return Err(PublicWireError::TooManyItems);
    }
    cursor(cursor_value)?;
    if items.is_empty() && cursor_value.is_some() {
        return Err(PublicWireError::InconsistentFields);
    }
    validate_discovery_fence(fence)
}

fn validate_discover_command_tools_request(
    message: &v1::DiscoverCommandToolsRequest,
) -> Result<(), PublicWireError> {
    request_id(&message.request_id)?;
    validate_page_request(message.page.as_ref())?;
    let representation = v1::DiscoveryRepresentation::try_from(message.representation)
        .map_err(|_| PublicWireError::InvalidEnum)?;
    if !matches!(
        representation,
        v1::DiscoveryRepresentation::Full | v1::DiscoveryRepresentation::CompactObservation
    ) {
        return Err(PublicWireError::InvalidEnum);
    }
    if let Some(prior) = message.prior_fence.as_ref() {
        validate_discovery_fence(Some(prior))?;
        if representation != v1::DiscoveryRepresentation::CompactObservation
            || message
                .page
                .as_ref()
                .is_some_and(|page| page.cursor.is_some())
        {
            return Err(PublicWireError::InconsistentFields);
        }
    }
    Ok(())
}

fn validate_discover_command_tools_response(
    message: &v1::DiscoverCommandToolsResponse,
) -> Result<(), PublicWireError> {
    match message
        .result
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        v1::discover_command_tools_response::Result::CatalogUnchanged(fence) => {
            validate_discovery_fence(Some(fence))
        }
        v1::discover_command_tools_response::Result::Page(page) => {
            validate_discovery_page_shape(
                &page.items,
                &page.next_cursor,
                page.observed_fence.as_ref(),
            )?;
            validate_command_tool_items(&page.items)?;
            validate_operation_schema_catalog(page.operation_schemas.as_ref())?;
            if !operation_catalog_matches_fence(
                page.operation_schemas
                    .as_ref()
                    .ok_or(PublicWireError::MissingRequiredField)?,
                page.observed_fence
                    .as_ref()
                    .ok_or(PublicWireError::MissingRequiredField)?,
            ) {
                return Err(PublicWireError::InconsistentFields);
            }
            if message.encoded_len() > MAX_DISCOVERY_PAGE_BYTES {
                return Err(PublicWireError::MessageTooLarge);
            }
            Ok(())
        }
        v1::discover_command_tools_response::Result::CompactPage(page) => {
            validate_discovery_page_shape(
                &page.items,
                &page.next_cursor,
                page.observed_fence.as_ref(),
            )?;
            validate_compact_command_tool_items(&page.items)
        }
    }
}

fn validate_contract_version_resource(
    resource: &v1::ContractVersionResource,
) -> Result<Vec<u8>, PublicWireError> {
    if !valid_bounded_text(&resource.contract_lineage, MAX_CONTRACT_LINEAGE_BYTES)
        || resource.contract_version == 0
    {
        return Err(PublicWireError::InvalidIdentity);
    }
    let mut key = resource.contract_lineage.as_bytes().to_vec();
    key.push(0);
    key.extend_from_slice(&resource.contract_version.to_be_bytes());
    Ok(key)
}

fn validate_entity_schema_resource(
    resource: &v1::EntitySchemaResource,
) -> Result<Vec<u8>, PublicWireError> {
    if !valid_bounded_text(&resource.contract_lineage, MAX_CONTRACT_LINEAGE_BYTES)
        || resource.entity_type_id == 0
    {
        return Err(PublicWireError::InvalidIdentity);
    }
    if validate_schema_artifact(resource.schema.as_ref())? != (1, resource.entity_type_id) {
        return Err(PublicWireError::InconsistentFields);
    }
    let mut key = resource.contract_lineage.as_bytes().to_vec();
    key.push(0);
    key.extend_from_slice(&resource.entity_type_id.to_be_bytes());
    Ok(key)
}

fn validate_compact_entity_schema_resource(
    resource: &v1::CompactEntitySchemaResource,
) -> Result<Vec<u8>, PublicWireError> {
    if !valid_bounded_text(&resource.contract_lineage, MAX_CONTRACT_LINEAGE_BYTES)
        || resource.entity_type_id == 0
    {
        return Err(PublicWireError::InvalidIdentity);
    }
    if validate_generated_schema_identity(resource.schema.as_ref())? != (1, resource.entity_type_id)
    {
        return Err(PublicWireError::InconsistentFields);
    }
    let mut key = resource.contract_lineage.as_bytes().to_vec();
    key.push(0);
    key.extend_from_slice(&resource.entity_type_id.to_be_bytes());
    Ok(key)
}

fn validate_command_resource(resource: &v1::CommandResource) -> Result<Vec<u8>, PublicWireError> {
    if !valid_bounded_text(&resource.contract_lineage, MAX_CONTRACT_LINEAGE_BYTES)
        || resource.command_id == 0
        || resource.contract_version == 0
        || !valid_name(&resource.source_command)
    {
        return Err(PublicWireError::InvalidIdentity);
    }
    let mut key = resource.contract_lineage.as_bytes().to_vec();
    key.push(0);
    key.extend_from_slice(&resource.command_id.to_be_bytes());
    Ok(key)
}

fn validate_command_outcome_resource(
    resource: &v1::CommandOutcomeResource,
) -> Result<Vec<u8>, PublicWireError> {
    if !valid_bounded_text(&resource.contract_lineage, MAX_CONTRACT_LINEAGE_BYTES)
        || resource.command_id == 0
        || !valid_mcp_tool_name(&resource.tool_name)
    {
        return Err(PublicWireError::InvalidIdentity);
    }
    if !mcp_tool_name_matches_declared_source(&resource.tool_name, &resource.contract_lineage, None)
    {
        return Err(PublicWireError::InconsistentFields);
    }
    let mut key = resource.contract_lineage.as_bytes().to_vec();
    key.push(0);
    key.extend_from_slice(&resource.command_id.to_be_bytes());
    Ok(key)
}

fn validate_commit_resource(resource: &v1::CommitResource) -> Result<Vec<u8>, PublicWireError> {
    match resource
        .target
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        v1::commit_resource::Target::ClassTemplate(_) => Ok(vec![0]),
        v1::commit_resource::Target::CommitSequence(0) => Err(PublicWireError::InvalidIdentity),
        v1::commit_resource::Target::CommitSequence(value) => {
            let mut key = vec![1];
            key.extend_from_slice(&value.to_be_bytes());
            Ok(key)
        }
    }
}

fn validate_provenance_resource(
    resource: &v1::ProvenanceResource,
) -> Result<Vec<u8>, PublicWireError> {
    match resource
        .target
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        v1::provenance_resource::Target::ClassTemplate(_) => Ok(vec![0]),
        v1::provenance_resource::Target::ProvenanceId(value) => {
            provenance_id(value)?;
            let mut key = vec![1];
            key.extend_from_slice(value);
            Ok(key)
        }
    }
}

fn validate_projection_status_resource(
    resource: &v1::ProjectionStatusResource,
) -> Result<Vec<u8>, PublicWireError> {
    if !valid_bounded_text(&resource.contract_lineage, MAX_CONTRACT_LINEAGE_BYTES)
        || resource.projection_id == 0
    {
        return Err(PublicWireError::InvalidIdentity);
    }
    let mut key = resource.contract_lineage.as_bytes().to_vec();
    key.push(0);
    key.extend_from_slice(&resource.projection_id.to_be_bytes());
    Ok(key)
}

fn validate_resource_descriptor(
    descriptor: &v1::ResourceDescriptor,
) -> Result<Vec<u8>, PublicWireError> {
    let (tag, tail) = match descriptor
        .resource
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        v1::resource_descriptor::Resource::ActiveContract(_) => (1, Vec::new()),
        v1::resource_descriptor::Resource::ContractVersion(value) => {
            (2, validate_contract_version_resource(value)?)
        }
        v1::resource_descriptor::Resource::EntitySchema(value) => {
            (3, validate_entity_schema_resource(value)?)
        }
        v1::resource_descriptor::Resource::CommandPlan(value) => {
            (4, validate_command_resource(value)?)
        }
        v1::resource_descriptor::Resource::CommandDocumentation(value) => {
            (5, validate_command_resource(value)?)
        }
        v1::resource_descriptor::Resource::CommandOutcome(value) => {
            (6, validate_command_outcome_resource(value)?)
        }
        v1::resource_descriptor::Resource::Commit(value) => (7, validate_commit_resource(value)?),
        v1::resource_descriptor::Resource::Provenance(value) => {
            (8, validate_provenance_resource(value)?)
        }
        v1::resource_descriptor::Resource::ProjectionStatus(value) => {
            (9, validate_projection_status_resource(value)?)
        }
        v1::resource_descriptor::Resource::ServerHealth(_) => (10, Vec::new()),
        v1::resource_descriptor::Resource::ReactiveWakeup(_) => (11, Vec::new()),
    };
    let mut key = vec![tag];
    key.extend_from_slice(&tail);
    Ok(key)
}

fn validate_compact_resource_descriptor(
    descriptor: &v1::CompactResourceDescriptor,
) -> Result<Vec<u8>, PublicWireError> {
    let (tag, tail) = match descriptor
        .resource
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        v1::compact_resource_descriptor::Resource::ActiveContract(_) => (1, Vec::new()),
        v1::compact_resource_descriptor::Resource::ContractVersion(value) => {
            (2, validate_contract_version_resource(value)?)
        }
        v1::compact_resource_descriptor::Resource::EntitySchema(value) => {
            (3, validate_compact_entity_schema_resource(value)?)
        }
        v1::compact_resource_descriptor::Resource::CommandPlan(value) => {
            (4, validate_command_resource(value)?)
        }
        v1::compact_resource_descriptor::Resource::CommandDocumentation(value) => {
            (5, validate_command_resource(value)?)
        }
        v1::compact_resource_descriptor::Resource::CommandOutcome(value) => {
            (6, validate_command_outcome_resource(value)?)
        }
        v1::compact_resource_descriptor::Resource::Commit(value) => {
            (7, validate_commit_resource(value)?)
        }
        v1::compact_resource_descriptor::Resource::Provenance(value) => {
            (8, validate_provenance_resource(value)?)
        }
        v1::compact_resource_descriptor::Resource::ProjectionStatus(value) => {
            (9, validate_projection_status_resource(value)?)
        }
        v1::compact_resource_descriptor::Resource::ServerHealth(_) => (10, Vec::new()),
        v1::compact_resource_descriptor::Resource::ReactiveWakeup(_) => (11, Vec::new()),
    };
    let mut key = vec![tag];
    key.extend_from_slice(&tail);
    Ok(key)
}

fn validate_resource_items(items: &[v1::ResourceDescriptor]) -> Result<(), PublicWireError> {
    let mut prior: Option<Vec<u8>> = None;
    for item in items {
        let key = validate_resource_descriptor(item)?;
        if prior.as_ref().is_some_and(|prior| prior >= &key) {
            return Err(PublicWireError::NonCanonical);
        }
        prior = Some(key);
    }
    Ok(())
}

fn validate_compact_resource_items(
    items: &[v1::CompactResourceDescriptor],
) -> Result<(), PublicWireError> {
    let mut prior: Option<Vec<u8>> = None;
    for item in items {
        let key = validate_compact_resource_descriptor(item)?;
        if prior.as_ref().is_some_and(|prior| prior >= &key) {
            return Err(PublicWireError::NonCanonical);
        }
        prior = Some(key);
    }
    Ok(())
}

fn resource_is_template(resource: &v1::ResourceDescriptor) -> bool {
    matches!(
        resource.resource.as_ref(),
        Some(v1::resource_descriptor::Resource::CommandOutcome(_))
            | Some(v1::resource_descriptor::Resource::Commit(
                v1::CommitResource {
                    target: Some(v1::commit_resource::Target::ClassTemplate(_)),
                }
            ))
            | Some(v1::resource_descriptor::Resource::Provenance(
                v1::ProvenanceResource {
                    target: Some(v1::provenance_resource::Target::ClassTemplate(_)),
                }
            ))
    )
}

fn compact_resource_is_template(resource: &v1::CompactResourceDescriptor) -> bool {
    matches!(
        resource.resource.as_ref(),
        Some(v1::compact_resource_descriptor::Resource::CommandOutcome(_))
            | Some(v1::compact_resource_descriptor::Resource::Commit(
                v1::CommitResource {
                    target: Some(v1::commit_resource::Target::ClassTemplate(_)),
                }
            ))
            | Some(v1::compact_resource_descriptor::Resource::Provenance(
                v1::ProvenanceResource {
                    target: Some(v1::provenance_resource::Target::ClassTemplate(_)),
                }
            ))
    )
}

fn validate_resource_kind(
    kind: i32,
    items: &[v1::ResourceDescriptor],
) -> Result<(), PublicWireError> {
    match v1::ResourceDiscoveryKind::try_from(kind) {
        Ok(v1::ResourceDiscoveryKind::All) => Ok(()),
        Ok(v1::ResourceDiscoveryKind::Concrete)
            if items.iter().all(|item| !resource_is_template(item)) =>
        {
            Ok(())
        }
        Ok(v1::ResourceDiscoveryKind::Template) if items.iter().all(resource_is_template) => Ok(()),
        _ => Err(PublicWireError::InconsistentFields),
    }
}

fn validate_compact_resource_kind(
    kind: i32,
    items: &[v1::CompactResourceDescriptor],
) -> Result<(), PublicWireError> {
    match v1::ResourceDiscoveryKind::try_from(kind) {
        Ok(v1::ResourceDiscoveryKind::All) => Ok(()),
        Ok(v1::ResourceDiscoveryKind::Concrete)
            if items.iter().all(|item| !compact_resource_is_template(item)) =>
        {
            Ok(())
        }
        Ok(v1::ResourceDiscoveryKind::Template)
            if items.iter().all(compact_resource_is_template) =>
        {
            Ok(())
        }
        _ => Err(PublicWireError::InconsistentFields),
    }
}

fn validate_discover_resources_request(
    message: &v1::DiscoverResourcesRequest,
) -> Result<(), PublicWireError> {
    request_id(&message.request_id)?;
    validate_page_request(message.page.as_ref())?;
    let representation = v1::DiscoveryRepresentation::try_from(message.representation)
        .map_err(|_| PublicWireError::InvalidEnum)?;
    if !matches!(
        representation,
        v1::DiscoveryRepresentation::Full | v1::DiscoveryRepresentation::CompactObservation
    ) || !matches!(
        v1::ResourceDiscoveryKind::try_from(message.kind),
        Ok(v1::ResourceDiscoveryKind::All
            | v1::ResourceDiscoveryKind::Concrete
            | v1::ResourceDiscoveryKind::Template)
    ) {
        return Err(PublicWireError::InvalidEnum);
    }
    if let Some(prior) = message.prior_fence.as_ref() {
        validate_discovery_fence(Some(prior))?;
        if representation != v1::DiscoveryRepresentation::CompactObservation
            || message
                .page
                .as_ref()
                .is_some_and(|page| page.cursor.is_some())
        {
            return Err(PublicWireError::InconsistentFields);
        }
    }
    Ok(())
}

fn validate_get_reactive_wakeup_request(
    message: &v1::GetReactiveWakeupRequest,
) -> Result<(), PublicWireError> {
    request_id(&message.request_id)
}

fn validate_get_reactive_wakeup_response(
    message: &v1::GetReactiveWakeupResponse,
) -> Result<(), PublicWireError> {
    if message.generation.len() != 32 {
        return Err(PublicWireError::InvalidBytes);
    }
    Ok(())
}

fn validate_discover_resources_response(
    message: &v1::DiscoverResourcesResponse,
) -> Result<(), PublicWireError> {
    match message
        .result
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        v1::discover_resources_response::Result::CatalogUnchanged(fence) => {
            validate_discovery_fence(Some(fence))
        }
        v1::discover_resources_response::Result::Page(page) => {
            validate_discovery_page_shape(
                &page.items,
                &page.next_cursor,
                page.observed_fence.as_ref(),
            )?;
            validate_resource_items(&page.items)?;
            if message.encoded_len() > MAX_DISCOVERY_PAGE_BYTES {
                return Err(PublicWireError::MessageTooLarge);
            }
            Ok(())
        }
        v1::discover_resources_response::Result::CompactPage(page) => {
            validate_discovery_page_shape(
                &page.items,
                &page.next_cursor,
                page.observed_fence.as_ref(),
            )?;
            validate_compact_resource_items(&page.items)
        }
    }
}

fn wire_field_bytes(field: wire::Field<'_>) -> Result<&[u8], PublicWireError> {
    field
        .require_wire(2)
        .map(|field| field.bytes)
        .map_err(|_| PublicWireError::MalformedEncoding)
}

#[derive(Clone, Copy)]
struct NestedRule {
    field: u32,
    preflight: fn(&[u8]) -> Result<(), PublicWireError>,
}

#[derive(Clone, Copy)]
enum RepeatedWire {
    LengthDelimited,
    PackableVarint,
}

#[derive(Clone, Copy)]
struct RepeatedRule {
    field: u32,
    maximum: usize,
    wire: RepeatedWire,
}

fn packed_varint_count(field: wire::Field<'_>) -> Result<usize, PublicWireError> {
    match field.wire_type {
        0 => {
            field
                .require_varint()
                .map_err(|_| PublicWireError::MalformedEncoding)?;
            Ok(1)
        }
        2 => {
            let mut count = 0usize;
            let mut cursor = Cursor::new(wire_field_bytes(field)?);
            while !cursor.input_is_empty() {
                cursor
                    .read_varint()
                    .map_err(|_| PublicWireError::MalformedEncoding)?;
                count = count
                    .checked_add(1)
                    .ok_or(PublicWireError::PreflightLimitExceeded)?;
            }
            Ok(count)
        }
        _ => Err(PublicWireError::MalformedEncoding),
    }
}

fn preflight_nested_message(
    input: &[u8],
    maximum_known_field: u32,
    repeated_fields: &[u32],
    oneof_groups: &[&[u32]],
    nested_rules: &[NestedRule],
    repeated_rules: &[RepeatedRule],
) -> Result<(), PublicWireError> {
    preflight_root(
        input,
        MAX_PUBLIC_RESPONSE_BYTES,
        maximum_known_field,
        repeated_fields,
        oneof_groups,
    )?;
    let mut counts = [0usize; 8];
    if repeated_rules.len() > counts.len() {
        return Err(PublicWireError::PreflightLimitExceeded);
    }
    let mut cursor = Cursor::new(input);
    while let Some(field) = cursor
        .next()
        .map_err(|_| PublicWireError::MalformedEncoding)?
    {
        for rule in nested_rules
            .iter()
            .filter(|rule| rule.field == field.number)
        {
            (rule.preflight)(wire_field_bytes(field)?)?;
        }
        for (index, rule) in repeated_rules.iter().enumerate() {
            if rule.field != field.number {
                continue;
            }
            let increment = match rule.wire {
                RepeatedWire::LengthDelimited => {
                    wire_field_bytes(field)?;
                    1
                }
                RepeatedWire::PackableVarint => packed_varint_count(field)?,
            };
            counts[index] = counts[index]
                .checked_add(increment)
                .ok_or(PublicWireError::PreflightLimitExceeded)?;
            if counts[index] > rule.maximum {
                return Err(PublicWireError::PreflightLimitExceeded);
            }
        }
    }
    Ok(())
}

fn preflight_value(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_result(wire::value(input))
}

fn preflight_value_record(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_result(wire::value_record(input))
}

fn preflight_entity_key(input: &[u8]) -> Result<(), PublicWireError> {
    key_envelope(input, KeyPurpose::Entity).map(|_| ())
}

fn preflight_index_key(input: &[u8]) -> Result<(), PublicWireError> {
    key_envelope(input, KeyPurpose::Index).map(|_| ())
}

fn preflight_partition_key(input: &[u8]) -> Result<(), PublicWireError> {
    key_envelope(input, KeyPurpose::Partition).map(|_| ())
}

fn preflight_noop(_: &[u8]) -> Result<(), PublicWireError> {
    Ok(())
}

fn preflight_unit(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(input, 0, &[], &[], &[], &[])
}

fn preflight_exact_contract_selection(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(input, 2, &[], &[], &[], &[])
}

fn preflight_contract_selection(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[&[1, 2]],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_unit,
            },
            NestedRule {
                field: 2,
                preflight: preflight_exact_contract_selection,
            },
        ],
        &[],
    )
}

fn preflight_page_request(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(input, 2, &[], &[], &[], &[])
}

fn preflight_field_selection(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        1,
        &[1],
        &[],
        &[],
        &[RepeatedRule {
            field: 1,
            maximum: MAX_FIELD_SELECTION_ITEMS,
            wire: RepeatedWire::PackableVarint,
        }],
    )
}

fn preflight_frontier(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[&[1, 2]],
        &[NestedRule {
            field: 1,
            preflight: preflight_unit,
        }],
        &[],
    )
}

fn preflight_tenant_scope(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[&[1, 2]],
        &[NestedRule {
            field: 1,
            preflight: preflight_unit,
        }],
        &[],
    )
}

fn preflight_actor(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        4,
        &[],
        &[],
        &[NestedRule {
            field: 3,
            preflight: preflight_tenant_scope,
        }],
        &[],
    )
}

fn preflight_timestamp(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(input, 2, &[], &[], &[], &[])
}

fn preflight_contract_descriptor(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        6,
        &[],
        &[],
        &[NestedRule {
            field: 6,
            preflight: preflight_contract_compatibility_summary,
        }],
        &[],
    )
}

fn preflight_contract_compatibility_summary(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        4,
        &[4],
        &[],
        &[NestedRule {
            field: 4,
            preflight: preflight_contract_compatibility_code_count,
        }],
        &[RepeatedRule {
            field: 4,
            maximum: 20,
            wire: RepeatedWire::LengthDelimited,
        }],
    )
}

fn preflight_contract_compatibility_code_count(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(input, 2, &[], &[], &[], &[])
}

fn preflight_source_span(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(input, 2, &[], &[], &[], &[])
}

fn preflight_syntax_diagnostic(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        5,
        &[5],
        &[],
        &[NestedRule {
            field: 4,
            preflight: preflight_source_span,
        }],
        &[RepeatedRule {
            field: 5,
            maximum: MAX_EXPECTED_TOKENS,
            wire: RepeatedWire::LengthDelimited,
        }],
    )
}

fn preflight_semantic_diagnostic(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        5,
        &[],
        &[],
        &[
            NestedRule {
                field: 4,
                preflight: preflight_source_span,
            },
            NestedRule {
                field: 5,
                preflight: preflight_source_span,
            },
        ],
        &[],
    )
}

fn preflight_syntax_diagnostic_list(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        1,
        &[1],
        &[],
        &[NestedRule {
            field: 1,
            preflight: preflight_syntax_diagnostic,
        }],
        &[RepeatedRule {
            field: 1,
            maximum: MAX_DIAGNOSTICS,
            wire: RepeatedWire::LengthDelimited,
        }],
    )
}

fn preflight_semantic_diagnostic_list(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        1,
        &[1],
        &[],
        &[NestedRule {
            field: 1,
            preflight: preflight_semantic_diagnostic,
        }],
        &[RepeatedRule {
            field: 1,
            maximum: MAX_DIAGNOSTICS,
            wire: RepeatedWire::LengthDelimited,
        }],
    )
}

fn preflight_compilation_diagnostics(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[&[1, 2]],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_syntax_diagnostic_list,
            },
            NestedRule {
                field: 2,
                preflight: preflight_semantic_diagnostic_list,
            },
        ],
        &[],
    )
}

fn preflight_schema_artifact_key(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(input, 5, &[], &[&[1, 2, 3, 4, 5]], &[], &[])
}

fn preflight_schema_artifact(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        4,
        &[],
        &[],
        &[NestedRule {
            field: 1,
            preflight: preflight_schema_artifact_key,
        }],
        &[],
    )
}

fn preflight_binding_field_ref(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(input, 2, &[], &[], &[], &[])
}

fn preflight_command_explain(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        11,
        &[5, 6, 7, 8, 9, 10],
        &[],
        &[
            NestedRule {
                field: 6,
                preflight: preflight_binding_field_ref,
            },
            NestedRule {
                field: 7,
                preflight: preflight_binding_field_ref,
            },
        ],
        &[
            RepeatedRule {
                field: 5,
                maximum: MAX_COMMAND_EXPLAIN_ITEMS,
                wire: RepeatedWire::PackableVarint,
            },
            RepeatedRule {
                field: 6,
                maximum: MAX_COMMAND_EXPLAIN_ITEMS,
                wire: RepeatedWire::LengthDelimited,
            },
            RepeatedRule {
                field: 7,
                maximum: MAX_COMMAND_EXPLAIN_ITEMS,
                wire: RepeatedWire::LengthDelimited,
            },
            RepeatedRule {
                field: 8,
                maximum: MAX_COMMAND_EXPLAIN_ITEMS,
                wire: RepeatedWire::PackableVarint,
            },
            RepeatedRule {
                field: 9,
                maximum: MAX_COMMAND_EXPLAIN_ITEMS,
                wire: RepeatedWire::PackableVarint,
            },
            RepeatedRule {
                field: 10,
                maximum: MAX_COMMAND_EXPLAIN_ITEMS,
                wire: RepeatedWire::PackableVarint,
            },
        ],
    )
}

fn preflight_explained_command(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        6,
        &[],
        &[],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_contract_descriptor,
            },
            NestedRule {
                field: 4,
                preflight: preflight_command_explain,
            },
            NestedRule {
                field: 5,
                preflight: preflight_schema_artifact,
            },
            NestedRule {
                field: 6,
                preflight: preflight_schema_artifact,
            },
        ],
        &[],
    )
}

fn preflight_validate_contract_response(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[&[1, 2]],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_unit,
            },
            NestedRule {
                field: 2,
                preflight: preflight_compilation_diagnostics,
            },
        ],
        &[],
    )
}

fn preflight_explain_command_request(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        3,
        &[],
        &[],
        &[NestedRule {
            field: 2,
            preflight: preflight_contract_selection,
        }],
        &[],
    )
}

fn preflight_explain_command_response(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[&[1, 2]],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_unit,
            },
            NestedRule {
                field: 2,
                preflight: preflight_explained_command,
            },
        ],
        &[],
    )
}

fn preflight_expected_version_mismatch(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(input, 1, &[], &[], &[], &[])
}

fn preflight_deploy_contract_response(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        4,
        &[],
        &[&[1, 2, 3, 4]],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_contract_descriptor,
            },
            NestedRule {
                field: 2,
                preflight: preflight_contract_descriptor,
            },
            NestedRule {
                field: 3,
                preflight: preflight_expected_version_mismatch,
            },
            NestedRule {
                field: 4,
                preflight: preflight_unit,
            },
        ],
        &[],
    )
}

fn preflight_get_active_contract_response(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[&[1, 2]],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_unit,
            },
            NestedRule {
                field: 2,
                preflight: preflight_contract_descriptor,
            },
        ],
        &[],
    )
}

fn preflight_execute_request(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_result(wire::execute_request(input))
}

fn preflight_execute_response(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_result(wire::execute_response(input))
}

fn preflight_execute_batch_request(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        1,
        &[1],
        &[],
        &[NestedRule {
            field: 1,
            preflight: preflight_execute_request,
        }],
        &[RepeatedRule {
            field: 1,
            maximum: MAX_COMMAND_BATCH_ITEMS,
            wire: RepeatedWire::LengthDelimited,
        }],
    )
}

fn preflight_application_error_public(input: &[u8]) -> Result<(), PublicWireError> {
    // Pure preflight only emits structural classes (never registry/context).
    match crate::preflight_application_error(input) {
        Ok(()) => Ok(()),
        Err(crate::ApplicationErrorWireError::MalformedEncoding)
        | Err(crate::ApplicationErrorWireError::UnknownField)
        | Err(crate::ApplicationErrorWireError::DuplicateField) => {
            Err(PublicWireError::MalformedEncoding)
        }
        Err(crate::ApplicationErrorWireError::PreflightLimitExceeded)
        | Err(crate::ApplicationErrorWireError::MessageTooLarge) => {
            Err(PublicWireError::PreflightLimitExceeded)
        }
        Err(
            crate::ApplicationErrorWireError::UnknownCode
            | crate::ApplicationErrorWireError::UnknownOperation
            | crate::ApplicationErrorWireError::UnknownFix
            | crate::ApplicationErrorWireError::InconsistentRegistry
            | crate::ApplicationErrorWireError::InvalidContext
            | crate::ApplicationErrorWireError::MissingInternalIncident,
        ) => unreachable!("application-error preflight never returns registry errors"),
    }
}

fn preflight_execute_batch_item(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[&[1, 2]],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_execute_response,
            },
            NestedRule {
                field: 2,
                preflight: preflight_application_error_public,
            },
        ],
        &[],
    )
}

fn preflight_execute_batch_response(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[1, 2],
        &[],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_execute_response,
            },
            NestedRule {
                field: 2,
                preflight: preflight_execute_batch_item,
            },
        ],
        &[
            RepeatedRule {
                field: 1,
                maximum: MAX_COMMAND_BATCH_ITEMS,
                wire: RepeatedWire::LengthDelimited,
            },
            RepeatedRule {
                field: 2,
                maximum: MAX_COMMAND_BATCH_ITEMS,
                wire: RepeatedWire::LengthDelimited,
            },
        ],
    )
}

fn preflight_get_entity_request(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        5,
        &[],
        &[],
        &[
            NestedRule {
                field: 2,
                preflight: preflight_contract_selection,
            },
            NestedRule {
                field: 4,
                preflight: preflight_entity_key,
            },
            NestedRule {
                field: 5,
                preflight: preflight_field_selection,
            },
        ],
        &[],
    )?;
    let mut owner = None;
    let mut key = None;
    let mut cursor = Cursor::new(input);
    while let Some(field) = cursor
        .next()
        .map_err(|_| PublicWireError::MalformedEncoding)?
    {
        match field.number {
            3 => {
                let value = field
                    .require_varint()
                    .map_err(|_| PublicWireError::MalformedEncoding)?;
                owner = Some(u32::try_from(value).map_err(|_| PublicWireError::MalformedEncoding)?);
            }
            4 => key = Some(wire_field_bytes(field)?),
            _ => {}
        }
    }
    if let Some(key) = key {
        let encoded_owner = key_envelope(key, KeyPurpose::Entity)?;
        if owner.is_some_and(|owner| owner != encoded_owner) {
            return Err(PublicWireError::KeyOwnerMismatch);
        }
    }
    Ok(())
}

fn preflight_entity(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        4,
        &[],
        &[],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_entity_key,
            },
            NestedRule {
                field: 4,
                preflight: preflight_value_record,
            },
        ],
        &[],
    )
}

fn preflight_get_entity_response(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[&[1, 2]],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_unit,
            },
            NestedRule {
                field: 2,
                preflight: preflight_entity,
            },
        ],
        &[],
    )
}

fn preflight_scan_index_request(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        6,
        &[4],
        &[],
        &[
            NestedRule {
                field: 2,
                preflight: preflight_contract_selection,
            },
            NestedRule {
                field: 4,
                preflight: preflight_value,
            },
            NestedRule {
                field: 5,
                preflight: preflight_field_selection,
            },
            NestedRule {
                field: 6,
                preflight: preflight_page_request,
            },
        ],
        &[RepeatedRule {
            field: 4,
            maximum: MAX_PROJECTION_GROUP_COMPONENTS,
            wire: RepeatedWire::LengthDelimited,
        }],
    )
}

fn preflight_index_row(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_index_key,
            },
            NestedRule {
                field: 2,
                preflight: preflight_value_record,
            },
        ],
        &[],
    )
}

fn preflight_index_fence(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(input, 1, &[], &[], &[], &[])
}

fn preflight_index_page(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        3,
        &[1],
        &[],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_index_row,
            },
            NestedRule {
                field: 3,
                preflight: preflight_index_fence,
            },
        ],
        &[RepeatedRule {
            field: 1,
            maximum: MAX_PAGE_ITEMS,
            wire: RepeatedWire::LengthDelimited,
        }],
    )
}

fn preflight_scan_index_response(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        1,
        &[],
        &[],
        &[NestedRule {
            field: 1,
            preflight: preflight_index_page,
        }],
        &[],
    )
}

fn preflight_query_projection_request(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        7,
        &[4],
        &[],
        &[
            NestedRule {
                field: 2,
                preflight: preflight_contract_selection,
            },
            NestedRule {
                field: 4,
                preflight: preflight_value,
            },
            NestedRule {
                field: 7,
                preflight: preflight_page_request,
            },
        ],
        &[RepeatedRule {
            field: 4,
            maximum: MAX_PROJECTION_GROUP_COMPONENTS,
            wire: RepeatedWire::LengthDelimited,
        }],
    )
}

fn preflight_projection_identity(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(input, 3, &[], &[], &[], &[])
}

fn preflight_projection_generation_frontier(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[],
        &[NestedRule {
            field: 2,
            preflight: preflight_frontier,
        }],
        &[],
    )
}

fn preflight_projection_failure(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(input, 3, &[], &[], &[], &[])
}

fn preflight_projection_unavailable(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        3,
        &[],
        &[&[1, 2, 3]],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_unit,
            },
            NestedRule {
                field: 2,
                preflight: preflight_unit,
            },
        ],
        &[],
    )
}

fn preflight_projection_page_fence(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        3,
        &[],
        &[],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_projection_identity,
            },
            NestedRule {
                field: 3,
                preflight: preflight_frontier,
            },
        ],
        &[],
    )
}

fn preflight_projection_row(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[1],
        &[],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_value,
            },
            NestedRule {
                field: 2,
                preflight: preflight_value_record,
            },
        ],
        &[RepeatedRule {
            field: 1,
            maximum: MAX_PROJECTION_GROUP_COMPONENTS,
            wire: RepeatedWire::LengthDelimited,
        }],
    )
}

fn preflight_projection_page(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        3,
        &[1],
        &[],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_projection_row,
            },
            NestedRule {
                field: 3,
                preflight: preflight_projection_page_fence,
            },
        ],
        &[RepeatedRule {
            field: 1,
            maximum: MAX_PAGE_ITEMS,
            wire: RepeatedWire::LengthDelimited,
        }],
    )
}

fn preflight_query_projection_ready(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_projection_page,
            },
            NestedRule {
                field: 2,
                preflight: preflight_frontier,
            },
        ],
        &[],
    )
}

fn preflight_query_projection_wait(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[],
        &[NestedRule {
            field: 2,
            preflight: preflight_frontier,
        }],
        &[],
    )
}

fn preflight_query_projection_degraded(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_frontier,
            },
            NestedRule {
                field: 2,
                preflight: preflight_projection_unavailable,
            },
        ],
        &[],
    )
}

fn preflight_query_projection_invalid(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(input, 1, &[], &[], &[], &[])
}

fn preflight_query_projection_response(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        4,
        &[],
        &[&[1, 2, 3, 4]],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_query_projection_ready,
            },
            NestedRule {
                field: 2,
                preflight: preflight_query_projection_wait,
            },
            NestedRule {
                field: 3,
                preflight: preflight_query_projection_degraded,
            },
            NestedRule {
                field: 4,
                preflight: preflight_query_projection_invalid,
            },
        ],
        &[],
    )
}

fn preflight_projection_status(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        7,
        &[],
        &[],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_projection_identity,
            },
            NestedRule {
                field: 3,
                preflight: preflight_projection_generation_frontier,
            },
            NestedRule {
                field: 4,
                preflight: preflight_projection_generation_frontier,
            },
            NestedRule {
                field: 6,
                preflight: preflight_projection_failure,
            },
            NestedRule {
                field: 7,
                preflight: preflight_frontier,
            },
        ],
        &[],
    )
}

fn preflight_get_projection_status_request(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        3,
        &[],
        &[],
        &[NestedRule {
            field: 2,
            preflight: preflight_contract_selection,
        }],
        &[],
    )
}

fn preflight_get_projection_status_response(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[&[1, 2]],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_unit,
            },
            NestedRule {
                field: 2,
                preflight: preflight_projection_status,
            },
        ],
        &[],
    )
}

fn preflight_declared_outcome(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        3,
        &[],
        &[],
        &[NestedRule {
            field: 3,
            preflight: preflight_value_record,
        }],
        &[],
    )
}

fn preflight_event_id(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(input, 2, &[], &[], &[], &[])
}

fn preflight_durable_event(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        3,
        &[],
        &[],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_event_id,
            },
            NestedRule {
                field: 3,
                preflight: preflight_value_record,
            },
        ],
        &[],
    )
}

fn preflight_affected_entity(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[],
        &[NestedRule {
            field: 1,
            preflight: preflight_entity_key,
        }],
        &[],
    )
}

fn preflight_commit(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        16,
        &[11, 12, 13],
        &[],
        &[
            NestedRule {
                field: 8,
                preflight: preflight_actor,
            },
            NestedRule {
                field: 9,
                preflight: preflight_timestamp,
            },
            NestedRule {
                field: 12,
                preflight: preflight_affected_entity,
            },
            NestedRule {
                field: 13,
                preflight: preflight_durable_event,
            },
            NestedRule {
                field: 14,
                preflight: preflight_declared_outcome,
            },
        ],
        &[
            RepeatedRule {
                field: 11,
                maximum: MAX_COMMIT_COLLECTION_ITEMS,
                wire: RepeatedWire::LengthDelimited,
            },
            RepeatedRule {
                field: 12,
                maximum: MAX_COMMIT_COLLECTION_ITEMS,
                wire: RepeatedWire::LengthDelimited,
            },
            RepeatedRule {
                field: 13,
                maximum: MAX_COMMIT_COLLECTION_ITEMS,
                wire: RepeatedWire::LengthDelimited,
            },
        ],
    )?;
    let mut cursor = Cursor::new(input);
    while let Some(field) = cursor
        .next()
        .map_err(|_| PublicWireError::MalformedEncoding)?
    {
        if field.number == 11 && wire_field_bytes(field)?.len() != 32 {
            return Err(PublicWireError::PreflightLimitExceeded);
        }
    }
    Ok(())
}

fn preflight_get_commit_response(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[&[1, 2]],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_unit,
            },
            NestedRule {
                field: 2,
                preflight: preflight_commit,
            },
        ],
        &[],
    )
}

fn preflight_commit_page(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        3,
        &[1],
        &[],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_commit,
            },
            NestedRule {
                field: 3,
                preflight: preflight_frontier,
            },
        ],
        &[RepeatedRule {
            field: 1,
            maximum: MAX_PAGE_ITEMS,
            wire: RepeatedWire::LengthDelimited,
        }],
    )
}

fn preflight_scan_commits_response(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        1,
        &[],
        &[],
        &[NestedRule {
            field: 1,
            preflight: preflight_commit_page,
        }],
        &[],
    )
}

fn preflight_scan_commits_request(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[],
        &[NestedRule {
            field: 2,
            preflight: preflight_page_request,
        }],
        &[],
    )
}

fn preflight_commit_terminal(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[],
        &[NestedRule {
            field: 2,
            preflight: preflight_frontier,
        }],
        &[],
    )
}

fn preflight_commit_notification(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[&[1, 2]],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_commit,
            },
            NestedRule {
                field: 2,
                preflight: preflight_commit_terminal,
            },
        ],
        &[],
    )
}

fn preflight_prebootstrap_health(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(input, 3, &[], &[], &[], &[])
}

fn preflight_health_component(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(input, 2, &[], &[], &[], &[])
}

fn preflight_build_info(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        7,
        &[4],
        &[],
        &[],
        &[RepeatedRule {
            field: 4,
            maximum: MAX_BUILD_FEATURES,
            wire: RepeatedWire::LengthDelimited,
        }],
    )
}

fn preflight_authenticated_health(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        6,
        &[4],
        &[],
        &[
            NestedRule {
                field: 4,
                preflight: preflight_health_component,
            },
            NestedRule {
                field: 5,
                preflight: preflight_timestamp,
            },
            NestedRule {
                field: 6,
                preflight: preflight_build_info,
            },
        ],
        &[RepeatedRule {
            field: 4,
            maximum: 5,
            wire: RepeatedWire::LengthDelimited,
        }],
    )
}

fn preflight_health_response(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[&[1, 2]],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_prebootstrap_health,
            },
            NestedRule {
                field: 2,
                preflight: preflight_authenticated_health,
            },
        ],
        &[],
    )
}

fn preflight_lineage_stable_id(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(input, 2, &[], &[], &[], &[])
}

fn preflight_capability_permission(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        19,
        &[],
        &[&[
            1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
        ]],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_unit,
            },
            NestedRule {
                field: 2,
                preflight: preflight_unit,
            },
            NestedRule {
                field: 3,
                preflight: preflight_lineage_stable_id,
            },
            NestedRule {
                field: 4,
                preflight: preflight_unit,
            },
            NestedRule {
                field: 5,
                preflight: preflight_lineage_stable_id,
            },
            NestedRule {
                field: 6,
                preflight: preflight_lineage_stable_id,
            },
            NestedRule {
                field: 7,
                preflight: preflight_lineage_stable_id,
            },
            NestedRule {
                field: 8,
                preflight: preflight_lineage_stable_id,
            },
            NestedRule {
                field: 9,
                preflight: preflight_lineage_stable_id,
            },
            NestedRule {
                field: 10,
                preflight: preflight_unit,
            },
            NestedRule {
                field: 11,
                preflight: preflight_unit,
            },
            NestedRule {
                field: 12,
                preflight: preflight_unit,
            },
            NestedRule {
                field: 13,
                preflight: preflight_unit,
            },
            NestedRule {
                field: 14,
                preflight: preflight_unit,
            },
            NestedRule {
                field: 15,
                preflight: preflight_unit,
            },
            NestedRule {
                field: 16,
                preflight: preflight_unit,
            },
            NestedRule {
                field: 17,
                preflight: preflight_unit,
            },
            NestedRule {
                field: 18,
                preflight: preflight_unit,
            },
            NestedRule {
                field: 19,
                preflight: preflight_unit,
            },
        ],
        &[],
    )
}

fn preflight_scoped_partition(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[],
        &[NestedRule {
            field: 2,
            preflight: preflight_partition_key,
        }],
        &[],
    )
}

fn preflight_explicit_partitions(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        1,
        &[1],
        &[],
        &[NestedRule {
            field: 1,
            preflight: preflight_scoped_partition,
        }],
        &[RepeatedRule {
            field: 1,
            maximum: MAX_CAPABILITY_PARTITIONS,
            wire: RepeatedWire::LengthDelimited,
        }],
    )
}

fn preflight_partition_scope(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[&[1, 2]],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_unit,
            },
            NestedRule {
                field: 2,
                preflight: preflight_explicit_partitions,
            },
        ],
        &[],
    )
}

fn preflight_field_visibility(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        4,
        &[3, 4],
        &[],
        &[],
        &[
            RepeatedRule {
                field: 3,
                maximum: MAX_CAPABILITY_FIELD_VISIBILITY,
                wire: RepeatedWire::PackableVarint,
            },
            RepeatedRule {
                field: 4,
                maximum: MAX_CAPABILITY_FIELD_VISIBILITY,
                wire: RepeatedWire::PackableVarint,
            },
        ],
    )
}

fn preflight_capability_principal_fact(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[],
        &[NestedRule {
            field: 2,
            preflight: preflight_value,
        }],
        &[],
    )
}

fn preflight_capability_row_policy_binding(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        4,
        &[4],
        &[],
        &[],
        &[RepeatedRule {
            field: 4,
            maximum: 4,
            wire: RepeatedWire::PackableVarint,
        }],
    )
}

fn preflight_capability_row_policy_grant(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        3,
        &[2, 3],
        &[],
        &[
            NestedRule {
                field: 2,
                preflight: preflight_capability_principal_fact,
            },
            NestedRule {
                field: 3,
                preflight: preflight_capability_row_policy_binding,
            },
        ],
        &[
            RepeatedRule {
                field: 2,
                maximum: MAX_PRINCIPAL_FACTS_V1,
                wire: RepeatedWire::LengthDelimited,
            },
            RepeatedRule {
                field: 3,
                maximum: MAX_CAPABILITY_ROW_POLICY_BINDINGS,
                wire: RepeatedWire::LengthDelimited,
            },
        ],
    )
}

fn preflight_capability_application_export_grant(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(input, 6, &[], &[], &[], &[])
}

fn preflight_capability_export_grant(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        1,
        &[1],
        &[],
        &[NestedRule {
            field: 1,
            preflight: preflight_capability_application_export_grant,
        }],
        &[RepeatedRule {
            field: 1,
            maximum: MAX_CAPABILITY_APPLICATION_EXPORT_GRANTS,
            wire: RepeatedWire::LengthDelimited,
        }],
    )
}

fn count_packed_field(input: &[u8], field_number: u32) -> Result<usize, PublicWireError> {
    let mut total = 0usize;
    let mut cursor = Cursor::new(input);
    while let Some(field) = cursor
        .next()
        .map_err(|_| PublicWireError::MalformedEncoding)?
    {
        if field.number == field_number {
            total = total
                .checked_add(packed_varint_count(field)?)
                .ok_or(PublicWireError::PreflightLimitExceeded)?;
        }
    }
    Ok(total)
}

fn preflight_capability_grant(input: &[u8]) -> Result<(), PublicWireError> {
    if input.len() > MAX_CAPABILITY_PAYLOAD_BYTES {
        return Err(PublicWireError::PreflightLimitExceeded);
    }
    preflight_nested_message(
        input,
        8,
        &[3, 4, 6],
        &[],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_tenant_scope,
            },
            NestedRule {
                field: 2,
                preflight: preflight_partition_scope,
            },
            NestedRule {
                field: 3,
                preflight: preflight_capability_permission,
            },
            NestedRule {
                field: 4,
                preflight: preflight_field_visibility,
            },
            NestedRule {
                field: 7,
                preflight: preflight_capability_row_policy_grant,
            },
            NestedRule {
                field: 8,
                preflight: preflight_capability_export_grant,
            },
        ],
        &[
            RepeatedRule {
                field: 3,
                maximum: MAX_CAPABILITY_PERMISSIONS,
                wire: RepeatedWire::LengthDelimited,
            },
            RepeatedRule {
                field: 4,
                maximum: MAX_CAPABILITY_FIELD_VISIBILITY,
                wire: RepeatedWire::LengthDelimited,
            },
            RepeatedRule {
                field: 6,
                maximum: 19,
                wire: RepeatedWire::PackableVarint,
            },
        ],
    )?;
    let mut total_fields = 0usize;
    let mut cursor = Cursor::new(input);
    while let Some(field) = cursor
        .next()
        .map_err(|_| PublicWireError::MalformedEncoding)?
    {
        if field.number == 4 {
            total_fields = total_fields
                .checked_add(count_packed_field(wire_field_bytes(field)?, 3)?)
                .ok_or(PublicWireError::PreflightLimitExceeded)?;
            if total_fields > MAX_CAPABILITY_FIELD_VISIBILITY {
                return Err(PublicWireError::PreflightLimitExceeded);
            }
        }
    }
    Ok(())
}

fn preflight_create_capability_request(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        8,
        &[7],
        &[],
        &[NestedRule {
            field: 8,
            preflight: preflight_capability_grant,
        }],
        &[RepeatedRule {
            field: 7,
            maximum: MAX_CAPABILITY_AUDIENCES,
            wire: RepeatedWire::LengthDelimited,
        }],
    )
}

fn preflight_capability_identity(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(input, 2, &[], &[], &[], &[])
}

fn preflight_capability_transition(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[],
        &[NestedRule {
            field: 1,
            preflight: preflight_capability_identity,
        }],
        &[],
    )
}

fn preflight_normal_capability_created(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[],
        &[NestedRule {
            field: 1,
            preflight: preflight_capability_transition,
        }],
        &[],
    )
}

fn preflight_normal_create_result(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        3,
        &[],
        &[&[1, 2, 3]],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_normal_capability_created,
            },
            NestedRule {
                field: 2,
                preflight: preflight_capability_identity,
            },
            NestedRule {
                field: 3,
                preflight: preflight_unit,
            },
        ],
        &[],
    )
}

fn preflight_bootstrap_create_result(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        3,
        &[],
        &[&[1, 2, 3]],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_capability_transition,
            },
            NestedRule {
                field: 2,
                preflight: preflight_capability_transition,
            },
            NestedRule {
                field: 3,
                preflight: preflight_unit,
            },
        ],
        &[],
    )
}

fn preflight_create_capability_response(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[&[1, 2]],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_normal_create_result,
            },
            NestedRule {
                field: 2,
                preflight: preflight_bootstrap_create_result,
            },
        ],
        &[],
    )
}

fn preflight_revoke_capability_response(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        3,
        &[],
        &[&[1, 2, 3]],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_capability_transition,
            },
            NestedRule {
                field: 2,
                preflight: preflight_capability_transition,
            },
            NestedRule {
                field: 3,
                preflight: preflight_unit,
            },
        ],
        &[],
    )
}

fn preflight_get_contract_version_response(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[&[1, 2]],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_unit,
            },
            NestedRule {
                field: 2,
                preflight: preflight_contract_descriptor,
            },
        ],
        &[],
    )
}

fn preflight_provenance_selection(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(input, 2, &[], &[&[1, 2]], &[], &[])
}

fn preflight_provenance_claims(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(input, 4, &[], &[], &[], &[])
}

fn preflight_provenance(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        13,
        &[11, 12],
        &[],
        &[
            NestedRule {
                field: 8,
                preflight: preflight_actor,
            },
            NestedRule {
                field: 9,
                preflight: preflight_timestamp,
            },
            NestedRule {
                field: 11,
                preflight: preflight_affected_entity,
            },
            NestedRule {
                field: 12,
                preflight: preflight_event_id,
            },
            NestedRule {
                field: 13,
                preflight: preflight_provenance_claims,
            },
        ],
        &[
            RepeatedRule {
                field: 11,
                maximum: MAX_PROVENANCE_LINKS,
                wire: RepeatedWire::LengthDelimited,
            },
            RepeatedRule {
                field: 12,
                maximum: MAX_PROVENANCE_LINKS,
                wire: RepeatedWire::LengthDelimited,
            },
        ],
    )
}

fn preflight_trace_provenance_request(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[],
        &[NestedRule {
            field: 2,
            preflight: preflight_provenance_selection,
        }],
        &[],
    )
}

fn preflight_trace_provenance_response(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[&[1, 2]],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_unit,
            },
            NestedRule {
                field: 2,
                preflight: preflight_provenance,
            },
        ],
        &[],
    )
}

fn preflight_outbox_summary(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        4,
        &[],
        &[],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_event_id,
            },
            NestedRule {
                field: 4,
                preflight: preflight_timestamp,
            },
        ],
        &[],
    )
}

fn preflight_outbox_page(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[1],
        &[],
        &[NestedRule {
            field: 1,
            preflight: preflight_outbox_summary,
        }],
        &[RepeatedRule {
            field: 1,
            maximum: MAX_PAGE_ITEMS,
            wire: RepeatedWire::LengthDelimited,
        }],
    )
}

fn preflight_list_outbox_request(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[],
        &[NestedRule {
            field: 2,
            preflight: preflight_page_request,
        }],
        &[],
    )
}

fn preflight_list_outbox_response(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        1,
        &[],
        &[],
        &[NestedRule {
            field: 1,
            preflight: preflight_outbox_page,
        }],
        &[],
    )
}

fn preflight_offline_maintenance_operation(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(input, 6, &[], &[], &[], &[])
}

fn preflight_offline_maintenance_start_response(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[],
        &[NestedRule {
            field: 2,
            preflight: preflight_offline_maintenance_operation,
        }],
        &[],
    )
}

fn preflight_get_offline_maintenance_operation_response(
    input: &[u8],
) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[&[1, 2]],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_unit,
            },
            NestedRule {
                field: 2,
                preflight: preflight_offline_maintenance_operation,
            },
        ],
        &[],
    )
}

fn preflight_contract_migration_operation(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(input, 11, &[], &[], &[], &[])
}

fn preflight_contract_migration_start_response(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[],
        &[NestedRule {
            field: 2,
            preflight: preflight_contract_migration_operation,
        }],
        &[],
    )
}

fn preflight_get_contract_migration_operation_response(
    input: &[u8],
) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[&[1, 2]],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_unit,
            },
            NestedRule {
                field: 2,
                preflight: preflight_contract_migration_operation,
            },
        ],
        &[],
    )
}

fn preflight_application_installation_failure(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(input, 3, &[], &[], &[], &[])
}

fn preflight_application_installation_observation(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        9,
        &[5],
        &[],
        &[NestedRule {
            field: 8,
            preflight: preflight_application_installation_failure,
        }],
        &[RepeatedRule {
            field: 5,
            maximum: 10,
            wire: RepeatedWire::PackableVarint,
        }],
    )
}

fn preflight_application_installation_driver_proof(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        1,
        &[1],
        &[],
        &[],
        &[RepeatedRule {
            field: 1,
            maximum: MAX_INSTALLATION_DRIVERS,
            wire: RepeatedWire::PackableVarint,
        }],
    )
}

fn preflight_application_installation_seed_receipt(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(input, 4, &[], &[], &[], &[])
}

fn preflight_application_installation_seed_receipts(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        1,
        &[1],
        &[],
        &[NestedRule {
            field: 1,
            preflight: preflight_application_installation_seed_receipt,
        }],
        &[RepeatedRule {
            field: 1,
            maximum: MAX_INSTALLATION_SEEDS,
            wire: RepeatedWire::LengthDelimited,
        }],
    )
}

fn preflight_application_installation_external_completion(
    input: &[u8],
) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[&[1, 2]],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_application_installation_driver_proof,
            },
            NestedRule {
                field: 2,
                preflight: preflight_application_installation_seed_receipts,
            },
        ],
        &[],
    )
}

fn preflight_start_application_installation_request(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        4,
        &[],
        &[],
        &[NestedRule {
            field: 4,
            preflight: preflight_application_installation_external_completion,
        }],
        &[],
    )
}

fn preflight_start_application_installation_response(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[],
        &[NestedRule {
            field: 1,
            preflight: preflight_application_installation_observation,
        }],
        &[],
    )
}

fn preflight_get_application_installation_response(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[&[1, 2]],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_unit,
            },
            NestedRule {
                field: 2,
                preflight: preflight_start_application_installation_response,
            },
        ],
        &[],
    )
}

fn preflight_application_export_selection(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(input, 6, &[], &[], &[], &[])
}

fn preflight_application_export_snapshot(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        8,
        &[7, 8],
        &[],
        &[],
        &[
            RepeatedRule {
                field: 7,
                maximum: MAX_APPLICATION_EXPORT_MODULES,
                wire: RepeatedWire::LengthDelimited,
            },
            RepeatedRule {
                field: 8,
                maximum: MAX_APPLICATION_EXPORT_MODULES,
                wire: RepeatedWire::LengthDelimited,
            },
        ],
    )
}

fn preflight_application_export_operation(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        13,
        &[],
        &[],
        &[
            NestedRule {
                field: 2,
                preflight: preflight_application_export_selection,
            },
            NestedRule {
                field: 3,
                preflight: preflight_application_export_snapshot,
            },
            NestedRule {
                field: 5,
                preflight: preflight_timestamp,
            },
        ],
        &[],
    )
}

fn preflight_start_application_export_request(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        5,
        &[],
        &[],
        &[NestedRule {
            field: 3,
            preflight: preflight_application_export_selection,
        }],
        &[],
    )
}

fn preflight_start_application_export_response(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        3,
        &[],
        &[],
        &[NestedRule {
            field: 2,
            preflight: preflight_application_export_operation,
        }],
        &[],
    )
}

fn preflight_application_export_page(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        8,
        &[4],
        &[],
        &[],
        &[RepeatedRule {
            field: 4,
            maximum: MAX_APPLICATION_EXPORT_PAGE_ROWS,
            wire: RepeatedWire::LengthDelimited,
        }],
    )
}

fn preflight_get_application_export_page_response(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        1,
        &[],
        &[],
        &[NestedRule {
            field: 1,
            preflight: preflight_application_export_page,
        }],
        &[],
    )
}

fn preflight_get_application_export_response(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[&[1, 2]],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_unit,
            },
            NestedRule {
                field: 2,
                preflight: preflight_application_export_operation,
            },
        ],
        &[],
    )
}

fn preflight_cancel_application_export_response(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[&[1, 2]],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_unit,
            },
            NestedRule {
                field: 2,
                preflight: preflight_application_export_operation,
            },
        ],
        &[],
    )
}

fn preflight_generated_schema_identity(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[],
        &[NestedRule {
            field: 1,
            preflight: preflight_schema_artifact_key,
        }],
        &[],
    )
}

fn preflight_command_tool_descriptor(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        7,
        &[],
        &[],
        &[
            NestedRule {
                field: 6,
                preflight: preflight_schema_artifact,
            },
            NestedRule {
                field: 7,
                preflight: preflight_schema_artifact,
            },
        ],
        &[],
    )
}

fn preflight_compact_command_tool_descriptor(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        7,
        &[],
        &[],
        &[
            NestedRule {
                field: 6,
                preflight: preflight_generated_schema_identity,
            },
            NestedRule {
                field: 7,
                preflight: preflight_generated_schema_identity,
            },
        ],
        &[],
    )
}

fn preflight_command_tool_item(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[&[1, 2]],
        &[NestedRule {
            field: 2,
            preflight: preflight_command_tool_descriptor,
        }],
        &[],
    )
}

fn preflight_compact_command_tool_item(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[&[1, 2]],
        &[NestedRule {
            field: 2,
            preflight: preflight_compact_command_tool_descriptor,
        }],
        &[],
    )
}

fn preflight_operation_schema_artifact(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(input, 4, &[], &[], &[], &[])
}

fn preflight_operation_schema_catalog(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_operation_schema_artifact,
            },
            NestedRule {
                field: 2,
                preflight: preflight_operation_schema_artifact,
            },
        ],
        &[],
    )
}

fn preflight_operation_schema_identity(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(input, 2, &[], &[], &[], &[])
}

fn preflight_operation_schema_catalog_identity(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_operation_schema_identity,
            },
            NestedRule {
                field: 2,
                preflight: preflight_operation_schema_identity,
            },
        ],
        &[],
    )
}

fn preflight_active_discovery_fence(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(input, 3, &[], &[], &[], &[])
}

fn preflight_discovery_fence(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        4,
        &[],
        &[&[1, 2]],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_unit,
            },
            NestedRule {
                field: 2,
                preflight: preflight_active_discovery_fence,
            },
            NestedRule {
                field: 4,
                preflight: preflight_operation_schema_catalog_identity,
            },
        ],
        &[],
    )
}

fn preflight_command_tool_page(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        4,
        &[1],
        &[],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_command_tool_item,
            },
            NestedRule {
                field: 3,
                preflight: preflight_discovery_fence,
            },
            NestedRule {
                field: 4,
                preflight: preflight_operation_schema_catalog,
            },
        ],
        &[RepeatedRule {
            field: 1,
            maximum: MAX_PAGE_ITEMS,
            wire: RepeatedWire::LengthDelimited,
        }],
    )
}

fn preflight_compact_command_tool_page(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        3,
        &[1],
        &[],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_compact_command_tool_item,
            },
            NestedRule {
                field: 3,
                preflight: preflight_discovery_fence,
            },
        ],
        &[RepeatedRule {
            field: 1,
            maximum: MAX_PAGE_ITEMS,
            wire: RepeatedWire::LengthDelimited,
        }],
    )
}

fn preflight_discover_command_tools_request(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        4,
        &[],
        &[],
        &[
            NestedRule {
                field: 2,
                preflight: preflight_page_request,
            },
            NestedRule {
                field: 3,
                preflight: preflight_discovery_fence,
            },
        ],
        &[],
    )
}

fn preflight_discover_command_tools_response(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        3,
        &[],
        &[&[1, 2, 3]],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_discovery_fence,
            },
            NestedRule {
                field: 2,
                preflight: preflight_command_tool_page,
            },
            NestedRule {
                field: 3,
                preflight: preflight_compact_command_tool_page,
            },
        ],
        &[],
    )
}

fn preflight_contract_version_resource(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(input, 2, &[], &[], &[], &[])
}

fn preflight_entity_schema_resource(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        3,
        &[],
        &[],
        &[NestedRule {
            field: 3,
            preflight: preflight_schema_artifact,
        }],
        &[],
    )
}

fn preflight_compact_entity_schema_resource(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        3,
        &[],
        &[],
        &[NestedRule {
            field: 3,
            preflight: preflight_generated_schema_identity,
        }],
        &[],
    )
}

fn preflight_command_resource(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(input, 4, &[], &[], &[], &[])
}

fn preflight_command_outcome_resource(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(input, 3, &[], &[], &[], &[])
}

fn preflight_commit_resource(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[&[1, 2]],
        &[NestedRule {
            field: 1,
            preflight: preflight_unit,
        }],
        &[],
    )
}

fn preflight_provenance_resource(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[&[1, 2]],
        &[NestedRule {
            field: 1,
            preflight: preflight_unit,
        }],
        &[],
    )
}

fn preflight_projection_status_resource(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(input, 2, &[], &[], &[], &[])
}

fn preflight_resource_descriptor(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        10,
        &[],
        &[&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_unit,
            },
            NestedRule {
                field: 2,
                preflight: preflight_contract_version_resource,
            },
            NestedRule {
                field: 3,
                preflight: preflight_entity_schema_resource,
            },
            NestedRule {
                field: 4,
                preflight: preflight_command_resource,
            },
            NestedRule {
                field: 5,
                preflight: preflight_command_resource,
            },
            NestedRule {
                field: 6,
                preflight: preflight_command_outcome_resource,
            },
            NestedRule {
                field: 7,
                preflight: preflight_commit_resource,
            },
            NestedRule {
                field: 8,
                preflight: preflight_provenance_resource,
            },
            NestedRule {
                field: 9,
                preflight: preflight_projection_status_resource,
            },
            NestedRule {
                field: 10,
                preflight: preflight_unit,
            },
        ],
        &[],
    )
}

fn preflight_compact_resource_descriptor(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        10,
        &[],
        &[&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_unit,
            },
            NestedRule {
                field: 2,
                preflight: preflight_contract_version_resource,
            },
            NestedRule {
                field: 3,
                preflight: preflight_compact_entity_schema_resource,
            },
            NestedRule {
                field: 4,
                preflight: preflight_command_resource,
            },
            NestedRule {
                field: 5,
                preflight: preflight_command_resource,
            },
            NestedRule {
                field: 6,
                preflight: preflight_command_outcome_resource,
            },
            NestedRule {
                field: 7,
                preflight: preflight_commit_resource,
            },
            NestedRule {
                field: 8,
                preflight: preflight_provenance_resource,
            },
            NestedRule {
                field: 9,
                preflight: preflight_projection_status_resource,
            },
            NestedRule {
                field: 10,
                preflight: preflight_unit,
            },
        ],
        &[],
    )
}

fn preflight_resource_page(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        3,
        &[1],
        &[],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_resource_descriptor,
            },
            NestedRule {
                field: 3,
                preflight: preflight_discovery_fence,
            },
        ],
        &[RepeatedRule {
            field: 1,
            maximum: MAX_PAGE_ITEMS,
            wire: RepeatedWire::LengthDelimited,
        }],
    )
}

fn preflight_compact_resource_page(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        3,
        &[1],
        &[],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_compact_resource_descriptor,
            },
            NestedRule {
                field: 3,
                preflight: preflight_discovery_fence,
            },
        ],
        &[RepeatedRule {
            field: 1,
            maximum: MAX_PAGE_ITEMS,
            wire: RepeatedWire::LengthDelimited,
        }],
    )
}

fn preflight_discover_resources_request(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        5,
        &[],
        &[],
        &[
            NestedRule {
                field: 2,
                preflight: preflight_page_request,
            },
            NestedRule {
                field: 3,
                preflight: preflight_discovery_fence,
            },
        ],
        &[],
    )
}

fn preflight_discover_resources_response(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        3,
        &[],
        &[&[1, 2, 3]],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_discovery_fence,
            },
            NestedRule {
                field: 2,
                preflight: preflight_resource_page,
            },
            NestedRule {
                field: 3,
                preflight: preflight_compact_resource_page,
            },
        ],
        &[],
    )
}

fn preflight_get_outcome_response(input: &[u8]) -> Result<(), PublicWireError> {
    preflight_nested_message(
        input,
        2,
        &[],
        &[&[1, 2]],
        &[
            NestedRule {
                field: 1,
                preflight: preflight_unit,
            },
            NestedRule {
                field: 2,
                preflight: preflight_execute_response,
            },
        ],
        &[],
    )
}

fn validate_event_symbol(value: &str) -> Result<(), PublicWireError> {
    if valid_bounded_text(value, MAX_PROTOCOL_NAME_BYTES) {
        Ok(())
    } else {
        Err(PublicWireError::InvalidValue)
    }
}

fn validate_event_selection(selection: Option<&v1::EventSelection>) -> Result<(), PublicWireError> {
    let selection = selection.ok_or(PublicWireError::MissingRequiredField)?;
    validate_event_symbol(&selection.event_name)?;
    if selection.partition.is_empty()
        || selection.partition.len() > 32
        || selection.selected_fields.is_empty()
        || selection.selected_fields.len() > 256
    {
        return Err(PublicWireError::TooManyItems);
    }
    let mut names = std::collections::BTreeSet::new();
    for component in &selection.partition {
        validate_event_symbol(&component.name)?;
        if !names.insert(component.name.as_str()) {
            return Err(PublicWireError::NonCanonical);
        }
        validate_value(
            component
                .value
                .as_ref()
                .ok_or(PublicWireError::MissingRequiredField)?,
        )
        .map_err(|_| PublicWireError::InvalidValue)?;
    }
    names.clear();
    for field in &selection.selected_fields {
        validate_event_symbol(field)?;
        if !names.insert(field.as_str()) {
            return Err(PublicWireError::NonCanonical);
        }
    }
    Ok(())
}

fn validate_event_field_descriptor(
    value: &v1::EventFieldDescriptor,
) -> Result<(), PublicWireError> {
    validate_event_symbol(&value.name)?;
    validate_event_symbol(&value.value_type)
}

fn validate_event_descriptor(value: &v1::EventDescriptor) -> Result<(), PublicWireError> {
    if !valid_bounded_text(&value.contract_lineage, MAX_CONTRACT_LINEAGE_BYTES)
        || value.contract_version == 0
        || value.partition_fields.len() > 32
        || value.payload_fields.len() > 256
        || value.application_streamable != !value.partition_fields.is_empty()
    {
        return Err(PublicWireError::InvalidValue);
    }
    hash(&value.contract_bundle_hash)?;
    validate_event_symbol(&value.event_name)?;
    let mut payload = std::collections::BTreeMap::new();
    for field in &value.payload_fields {
        validate_event_field_descriptor(field)?;
        if payload
            .insert(field.name.as_str(), field.value_type.as_str())
            .is_some()
        {
            return Err(PublicWireError::NonCanonical);
        }
    }
    let mut partition = std::collections::BTreeSet::new();
    for field in &value.partition_fields {
        validate_event_field_descriptor(field)?;
        if !partition.insert(field.name.as_str()) {
            return Err(PublicWireError::NonCanonical);
        }
        if payload.get(field.name.as_str()).copied() != Some(field.value_type.as_str()) {
            return Err(PublicWireError::InconsistentFields);
        }
    }
    Ok(())
}

fn validate_describe_event_request(
    message: &v1::DescribeEventRequest,
) -> Result<(), PublicWireError> {
    request_id(&message.request_id)?;
    validate_event_symbol(&message.event_name)
}

fn validate_describe_event_response(
    message: &v1::DescribeEventResponse,
) -> Result<(), PublicWireError> {
    match message
        .result
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        v1::describe_event_response::Result::NotFound(_) => Ok(()),
        v1::describe_event_response::Result::Found(descriptor) => {
            validate_event_descriptor(descriptor)
        }
    }
}

fn validate_symbolic_event(value: &v1::SymbolicEvent) -> Result<(), PublicWireError> {
    validate_event_id_value(value.event_id.as_ref())?;
    validate_event_symbol(&value.event_name)?;
    if value.writer_contract_version == 0 || value.history_incarnation == 0 {
        return Err(PublicWireError::InvalidIdentity);
    }
    hash(&value.writer_plan_hash)?;
    validate_event_symbol(&value.command_name)?;
    request_id(&value.request_id)?;
    request_id(&value.root_request_id)?;
    if value.causing_event_id.is_some() {
        validate_event_id_value(value.causing_event_id.as_ref())?;
    }
    validate_timestamp(value.occurred_at.as_ref())?;
    if value.actor_kind == v1::ActorKind::Unspecified as i32
        || v1::ActorKind::try_from(value.actor_kind).is_err()
        || value.provenance_uri.len() > 56
        || !value.provenance_uri.starts_with("riffdb://provenance/")
        || value.fields.len() > 256
    {
        return Err(PublicWireError::InvalidValue);
    }
    let mut names = std::collections::BTreeSet::new();
    for field in &value.fields {
        validate_event_symbol(&field.name)?;
        if !names.insert(field.name.as_str()) {
            return Err(PublicWireError::NonCanonical);
        }
        validate_value(
            field
                .value
                .as_ref()
                .ok_or(PublicWireError::MissingRequiredField)?,
        )
        .map_err(|_| PublicWireError::InvalidValue)?;
    }
    Ok(())
}

fn validate_event_page(page: Option<&v1::EventPage>) -> Result<(), PublicWireError> {
    let page = page.ok_or(PublicWireError::MissingRequiredField)?;
    if page.items.len() > MAX_PAGE_ITEMS || page.history_incarnation == 0 {
        return Err(PublicWireError::TooManyItems);
    }
    if !page.next_cursor.is_empty() && page.next_cursor.len() != 16 {
        return Err(PublicWireError::InvalidBytes);
    }
    let disposition = v1::EventPageDisposition::try_from(page.disposition)
        .map_err(|_| PublicWireError::InvalidEnum)?;
    if disposition == v1::EventPageDisposition::Unspecified
        || (disposition == v1::EventPageDisposition::BoundedProgress && page.next_cursor.is_empty())
    {
        return Err(PublicWireError::InconsistentFields);
    }
    // A partition route page may contain no selected event type while still
    // advancing its opaque physical continuation.
    let observed_upper = page
        .observed_upper
        .as_ref()
        .map(|_| validate_event_id_value(page.observed_upper.as_ref()))
        .transpose()?;
    if observed_upper.is_none() && (!page.items.is_empty() || !page.next_cursor.is_empty()) {
        return Err(PublicWireError::InconsistentFields);
    }
    let mut prior = None;
    for item in &page.items {
        validate_symbolic_event(item)?;
        let id = validate_event_id_value(item.event_id.as_ref())?;
        if prior.is_some_and(|prior| prior >= id)
            || observed_upper.is_some_and(|upper| id > upper)
            || item.history_incarnation != page.history_incarnation
        {
            return Err(PublicWireError::NonCanonical);
        }
        prior = Some(id);
    }
    Ok(())
}

fn validate_replay_events_request(
    message: &v1::ReplayEventsRequest,
) -> Result<(), PublicWireError> {
    request_id(&message.request_id)?;
    validate_event_selection(message.selection.as_ref())?;
    if message.after_event_id.is_some() {
        validate_event_id_value(message.after_event_id.as_ref())?;
    }
    validate_page_request(message.page.as_ref())?;
    if message.after_event_id.is_some()
        && message
            .page
            .as_ref()
            .and_then(|page| page.cursor.as_ref())
            .is_some()
    {
        return Err(PublicWireError::InconsistentFields);
    }
    Ok(())
}

fn validate_replay_events_response(
    message: &v1::ReplayEventsResponse,
) -> Result<(), PublicWireError> {
    validate_event_page(message.page.as_ref())
}

fn validate_tail_events_request(message: &v1::TailEventsRequest) -> Result<(), PublicWireError> {
    request_id(&message.request_id)?;
    validate_event_selection(message.selection.as_ref())?;
    if message.after_event_id.is_some() {
        validate_event_id_value(message.after_event_id.as_ref())?;
    }
    validate_page_request(message.page.as_ref())?;
    if !(1..=MAX_PROJECTION_WAIT_NANOS).contains(&message.maximum_wait_nanos)
        || message
            .page
            .as_ref()
            .and_then(|page| page.cursor.as_ref())
            .is_some()
    {
        return Err(PublicWireError::InvalidValue);
    }
    Ok(())
}

fn validate_tail_events_response(message: &v1::TailEventsResponse) -> Result<(), PublicWireError> {
    validate_event_page(message.page.as_ref())?;
    if message.wait_timed_out
        && message.page.as_ref().is_some_and(|page| {
            !page.items.is_empty()
                || !page.next_cursor.is_empty()
                || page.disposition != i32::from(v1::EventPageDisposition::Page)
        })
    {
        return Err(PublicWireError::InconsistentFields);
    }
    Ok(())
}

fn validate_event_consumer_selection(
    selection: Option<&v1::EventConsumerSelection>,
) -> Result<(), PublicWireError> {
    let selection = selection.ok_or(PublicWireError::MissingRequiredField)?;
    hash(&selection.reactive_module_hash)?;
    validate_event_symbol(&selection.operation_name)?;
    EventConsumerName::new(selection.consumer_name.clone())
        .map_err(|_| PublicWireError::InvalidValue)?;
    if selection.parameters.len() > 1_024 {
        return Err(PublicWireError::TooManyItems);
    }
    let mut prior = None;
    for parameter in &selection.parameters {
        validate_event_symbol(&parameter.name)?;
        if prior.is_some_and(|value: &str| value >= parameter.name.as_str()) {
            return Err(PublicWireError::NonCanonical);
        }
        validate_value(
            parameter
                .value
                .as_ref()
                .ok_or(PublicWireError::MissingRequiredField)?,
        )
        .map_err(|_| PublicWireError::InvalidValue)?;
        prior = Some(parameter.name.as_str());
    }
    Ok(())
}

fn validate_event_consumer_checkpoint(
    checkpoint: Option<&v1::EventConsumerCheckpoint>,
) -> Result<(), PublicWireError> {
    use v1::event_consumer_checkpoint::Position;
    match checkpoint
        .and_then(|value| value.position.as_ref())
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        Position::BeforeFirst(_) => Ok(()),
        Position::AfterEventId(event_id) => validate_event_id_value(Some(event_id)).map(|_| ()),
    }
}

fn validate_event_consumer_status(
    status: Option<&v1::EventConsumerStatus>,
) -> Result<(), PublicWireError> {
    let status = status.ok_or(PublicWireError::MissingRequiredField)?;
    if status.revision == 0
        || status.history_incarnation == 0
        || status.live_leases > 64
        || status.retries > 4_096
        || status.dead_letters > 4_096
    {
        return Err(PublicWireError::InvalidValue);
    }
    validate_event_consumer_checkpoint(status.checkpoint.as_ref())
}

fn validate_protected_event_consumer_status(
    status: Option<&v1::ProtectedEventConsumerStatus>,
) -> Result<(), PublicWireError> {
    let status = status.ok_or(PublicWireError::MissingRequiredField)?;
    if status.history_incarnation == 0 || status.progress_cursor.len() != 16 {
        return Err(PublicWireError::InvalidValue);
    }
    Ok(())
}

fn validate_consumer_pull_shape(
    exact: Option<&v1::EventConsumerStatus>,
    protected: Option<&v1::ProtectedEventConsumerStatus>,
    disposition: i32,
    wait_timed_out: bool,
    item_count: usize,
) -> Result<u64, PublicWireError> {
    let disposition = v1::EventConsumerPullDisposition::try_from(disposition)
        .map_err(|_| PublicWireError::InvalidValue)?;
    if disposition == v1::EventConsumerPullDisposition::Unspecified
        || wait_timed_out != (disposition == v1::EventConsumerPullDisposition::WaitTimedOut)
        || (disposition != v1::EventConsumerPullDisposition::Ready && item_count != 0)
        || (disposition == v1::EventConsumerPullDisposition::BoundedProgress && protected.is_none())
        || exact.is_some() == protected.is_some()
    {
        return Err(PublicWireError::InconsistentFields);
    }
    match (exact, protected) {
        (Some(status), None) => {
            validate_event_consumer_status(Some(status))?;
            Ok(status.history_incarnation)
        }
        (None, Some(status)) => {
            validate_protected_event_consumer_status(Some(status))?;
            Ok(status.history_incarnation)
        }
        _ => Err(PublicWireError::InconsistentFields),
    }
}

fn validate_consume_event_stream_request(
    message: &v1::ConsumeEventStreamRequest,
) -> Result<(), PublicWireError> {
    request_id(&message.request_id)?;
    validate_event_consumer_selection(message.selection.as_ref())?;
    if !(1..=64).contains(&message.batch_limit)
        || !(1..=64).contains(&message.in_flight_limit)
        || !(5..=900).contains(&message.lease_seconds)
        || message.maximum_wait_nanos > MAX_PROJECTION_WAIT_NANOS
        || (!message.progress_cursor.is_empty() && message.progress_cursor.len() != 16)
    {
        return Err(PublicWireError::InvalidValue);
    }
    Ok(())
}

fn validate_consume_event_stream_response(
    message: &v1::ConsumeEventStreamResponse,
) -> Result<(), PublicWireError> {
    if message.events.len() > 64 || (message.wait_timed_out && !message.events.is_empty()) {
        return Err(PublicWireError::TooManyItems);
    }
    let history_incarnation = validate_consumer_pull_shape(
        message.status.as_ref(),
        message.protected_status.as_ref(),
        message.disposition,
        message.wait_timed_out,
        message.events.len(),
    )?;
    let mut prior = None;
    for item in &message.events {
        let event = item
            .event
            .as_ref()
            .ok_or(PublicWireError::MissingRequiredField)?;
        validate_symbolic_event(event)?;
        let event_id = validate_event_id_value(event.event_id.as_ref())?;
        if prior.is_some_and(|value| value >= event_id)
            || event.history_incarnation != history_incarnation
            || !(1..=10).contains(&item.attempt)
            || item.lease_token.len() != 32
        {
            return Err(PublicWireError::NonCanonical);
        }
        validate_timestamp(item.expires_at.as_ref())?;
        prior = Some(event_id);
    }
    Ok(())
}

fn validate_consumer_lease_request_parts(
    request_id_bytes: &[u8],
    selection: Option<&v1::EventConsumerSelection>,
    event_id: Option<&v1::EventId>,
    lease_token: &[u8],
    history_incarnation: u64,
) -> Result<(), PublicWireError> {
    request_id(request_id_bytes)?;
    validate_event_consumer_selection(selection)?;
    validate_event_id_value(event_id)?;
    if lease_token.len() != 32 || history_incarnation == 0 {
        return Err(PublicWireError::InvalidValue);
    }
    Ok(())
}

fn validate_acknowledge_event_stream_request(
    message: &v1::AcknowledgeEventStreamRequest,
) -> Result<(), PublicWireError> {
    validate_consumer_lease_request_parts(
        &message.request_id,
        message.selection.as_ref(),
        message.event_id.as_ref(),
        &message.lease_token,
        message.history_incarnation,
    )
}

fn validate_negative_acknowledge_event_stream_request(
    message: &v1::NegativeAcknowledgeEventStreamRequest,
) -> Result<(), PublicWireError> {
    validate_consumer_lease_request_parts(
        &message.request_id,
        message.selection.as_ref(),
        message.event_id.as_ref(),
        &message.lease_token,
        message.history_incarnation,
    )?;
    if message.retry_delay_nanos > 3_600_000_000_000 {
        return Err(PublicWireError::InvalidValue);
    }
    Ok(())
}

fn validate_seek_event_stream_consumer_request(
    message: &v1::SeekEventStreamConsumerRequest,
) -> Result<(), PublicWireError> {
    request_id(&message.request_id)?;
    validate_event_consumer_selection(message.selection.as_ref())?;
    match (message.checkpoint.as_ref(), message.progress_cursor.len()) {
        (Some(checkpoint), 0) => validate_event_consumer_checkpoint(Some(checkpoint)),
        (None, 16) => Ok(()),
        _ => Err(PublicWireError::InvalidValue),
    }
}

fn validate_retire_event_stream_consumer_request(
    message: &v1::RetireEventStreamConsumerRequest,
) -> Result<(), PublicWireError> {
    request_id(&message.request_id)?;
    validate_event_consumer_selection(message.selection.as_ref())
}

fn validate_get_event_stream_consumer_status_request(
    message: &v1::GetEventStreamConsumerStatusRequest,
) -> Result<(), PublicWireError> {
    request_id(&message.request_id)?;
    validate_event_consumer_selection(message.selection.as_ref())
}

fn validate_event_consumer_mutation_response(
    message: &v1::EventConsumerMutationResponse,
) -> Result<(), PublicWireError> {
    if message.result == v1::EventConsumerMutationResult::Unspecified as i32
        || v1::EventConsumerMutationResult::try_from(message.result).is_err()
    {
        return Err(PublicWireError::InvalidValue);
    }
    Ok(())
}

fn validate_get_event_stream_consumer_status_response(
    message: &v1::GetEventStreamConsumerStatusResponse,
) -> Result<(), PublicWireError> {
    use v1::get_event_stream_consumer_status_response::Result;
    match message
        .result
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        Result::NotFound(_) => Ok(()),
        Result::Found(status) => validate_event_consumer_status(Some(status)),
        Result::Protected(status) => validate_protected_event_consumer_status(Some(status)),
    }
}

fn validate_consume_contextual_subscription_request(
    message: &v1::ConsumeContextualSubscriptionRequest,
) -> Result<(), PublicWireError> {
    request_id(&message.request_id)?;
    validate_event_consumer_selection(message.selection.as_ref())?;
    if message.maximum_wait_nanos > 30_000_000_000
        || (!message.progress_cursor.is_empty() && message.progress_cursor.len() != 16)
    {
        return Err(PublicWireError::InvalidValue);
    }
    Ok(())
}

fn validate_contextual_query_row(row: &v1::ContextualQueryRow) -> Result<(), PublicWireError> {
    validate_event_symbol(&row.entity)?;
    if row.fields.len() > 1_024 {
        return Err(PublicWireError::TooManyItems);
    }
    let mut prior = None;
    for field in &row.fields {
        validate_event_symbol(&field.name)?;
        if prior.is_some_and(|value: &str| value >= field.name.as_str()) {
            return Err(PublicWireError::NonCanonical);
        }
        validate_value(
            field
                .value
                .as_ref()
                .ok_or(PublicWireError::MissingRequiredField)?,
        )
        .map_err(|_| PublicWireError::InvalidValue)?;
        prior = Some(field.name.as_str());
    }
    Ok(())
}

fn validate_contextual_hydration(
    hydration: &v1::ContextualHydration,
) -> Result<(), PublicWireError> {
    validate_event_symbol(&hydration.name)?;
    validate_event_symbol(&hydration.outcome)?;
    if hydration.fields.len() > 1_024 {
        return Err(PublicWireError::TooManyItems);
    }
    let mut prior = None;
    for field in &hydration.fields {
        validate_event_symbol(&field.name)?;
        if prior.is_some_and(|value: &str| value >= field.name.as_str()) {
            return Err(PublicWireError::NonCanonical);
        }
        let cardinality = v1::ContextualQueryCardinality::try_from(field.cardinality)
            .map_err(|_| PublicWireError::InvalidValue)?;
        let valid_count = match cardinality {
            v1::ContextualQueryCardinality::One => field.rows.len() == 1,
            v1::ContextualQueryCardinality::Maybe => field.rows.len() <= 1,
            v1::ContextualQueryCardinality::Many => field.rows.len() <= MAX_PAGE_ITEMS,
            v1::ContextualQueryCardinality::Unspecified => false,
        };
        if !valid_count {
            return Err(PublicWireError::InvalidValue);
        }
        for row in &field.rows {
            validate_contextual_query_row(row)?;
        }
        prior = Some(field.name.as_str());
    }
    Ok(())
}

fn validate_consume_contextual_subscription_response(
    message: &v1::ConsumeContextualSubscriptionResponse,
) -> Result<(), PublicWireError> {
    if message.items.len() > 1 || (message.wait_timed_out && !message.items.is_empty()) {
        return Err(PublicWireError::TooManyItems);
    }
    let history_incarnation = validate_consumer_pull_shape(
        message.status.as_ref(),
        message.protected_status.as_ref(),
        message.disposition,
        message.wait_timed_out,
        message.items.len(),
    )?;
    for item in &message.items {
        if item.context_head == 0
            || item.hydrations.len() > 16
            || item.available_reactions.len() > 64
        {
            return Err(PublicWireError::TooManyItems);
        }
        let delivery = item
            .delivery
            .as_ref()
            .ok_or(PublicWireError::MissingRequiredField)?;
        let event = delivery
            .event
            .as_ref()
            .ok_or(PublicWireError::MissingRequiredField)?;
        validate_symbolic_event(event)?;
        if event.history_incarnation != history_incarnation
            || !(1..=10).contains(&delivery.attempt)
            || delivery.lease_token.len() != 32
        {
            return Err(PublicWireError::InvalidValue);
        }
        validate_timestamp(delivery.expires_at.as_ref())?;
        for hydration in &item.hydrations {
            validate_contextual_hydration(hydration)?;
        }
        for reaction in &item.available_reactions {
            validate_event_symbol(&reaction.name)?;
            validate_event_symbol(&reaction.command_name)?;
            if reaction.command_id == 0
                || reaction.causation_token.len() <= 32
                || reaction.causation_token.len() > 1_024
            {
                return Err(PublicWireError::InvalidValue);
            }
        }
    }
    Ok(())
}

fn validate_acknowledge_contextual_subscription_request(
    message: &v1::AcknowledgeContextualSubscriptionRequest,
) -> Result<(), PublicWireError> {
    validate_consumer_lease_request_parts(
        &message.request_id,
        message.selection.as_ref(),
        message.event_id.as_ref(),
        &message.lease_token,
        message.history_incarnation,
    )
}

fn validate_negative_acknowledge_contextual_subscription_request(
    message: &v1::NegativeAcknowledgeContextualSubscriptionRequest,
) -> Result<(), PublicWireError> {
    validate_consumer_lease_request_parts(
        &message.request_id,
        message.selection.as_ref(),
        message.event_id.as_ref(),
        &message.lease_token,
        message.history_incarnation,
    )?;
    if message.retry_delay_nanos > 3_600_000_000_000 {
        return Err(PublicWireError::InvalidValue);
    }
    Ok(())
}

fn validate_get_contextual_subscription_status_request(
    message: &v1::GetContextualSubscriptionStatusRequest,
) -> Result<(), PublicWireError> {
    request_id(&message.request_id)?;
    validate_event_consumer_selection(message.selection.as_ref())
}

fn validate_execute_contextual_reaction_request(
    message: &v1::ExecuteContextualReactionRequest,
) -> Result<(), PublicWireError> {
    request_id(&message.request_id)?;
    validate_event_consumer_selection(message.selection.as_ref())?;
    validate_event_symbol(&message.reaction_name)?;
    if message.causation_token.len() <= 32 || message.causation_token.len() > 1_024 {
        return Err(PublicWireError::InvalidValue);
    }
    let command = message
        .command
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?;
    request_id(&command.request_id)?;
    if command.request_id != message.request_id {
        return Err(PublicWireError::InconsistentFields);
    }
    crate::validate_execute_request(command).map_err(|_| PublicWireError::InconsistentFields)
}

fn validate_watch_named_query_request(
    message: &v1::WatchNamedQueryRequest,
) -> Result<(), PublicWireError> {
    request_id(&message.request_id)?;
    hash(&message.reactive_module_hash)?;
    validate_event_symbol(&message.operation_name)?;
    if message.parameters.len() > 1_024
        || message
            .cursor
            .as_ref()
            .is_some_and(|cursor| cursor.is_empty() || cursor.len() > 4_096)
    {
        return Err(PublicWireError::TooManyItems);
    }
    let mut prior = None;
    for parameter in &message.parameters {
        validate_event_symbol(&parameter.name)?;
        if prior.is_some_and(|value: &str| value >= parameter.name.as_str()) {
            return Err(PublicWireError::NonCanonical);
        }
        validate_value(
            parameter
                .value
                .as_ref()
                .ok_or(PublicWireError::MissingRequiredField)?,
        )
        .map_err(|_| PublicWireError::InvalidValue)?;
        prior = Some(parameter.name.as_str());
    }
    Ok(())
}

fn validate_live_query_frontier(
    frontier: Option<&v1::LiveQueryFrontier>,
) -> Result<(), PublicWireError> {
    let frontier = frontier.ok_or(PublicWireError::MissingRequiredField)?;
    if frontier.history_incarnation == 0 {
        return Err(PublicWireError::InvalidValue);
    }
    Ok(())
}

fn validate_live_value_record(record: Option<&v1::ValueRecord>) -> Result<(), PublicWireError> {
    let record = record.ok_or(PublicWireError::MissingRequiredField)?;
    validate_value_record(record).map_err(|_| PublicWireError::InvalidValue)?;
    let mut prior = None;
    for field in &record.fields {
        if field.field_id.is_some() || !valid_name(&field.name) {
            return Err(PublicWireError::InvalidValue);
        }
        if prior.is_some_and(|value: &str| value >= field.name.as_str()) {
            return Err(PublicWireError::NonCanonical);
        }
        prior = Some(field.name.as_str());
    }
    Ok(())
}

fn validate_live_query_record(
    record: Option<&v1::LiveQueryResultRecord>,
) -> Result<(), PublicWireError> {
    let record = record.ok_or(PublicWireError::MissingRequiredField)?;
    validate_event_symbol(&record.entity)?;
    validate_live_value_record(record.fields.as_ref())
}

fn validate_live_query_result(result: Option<&v1::LiveQueryResult>) -> Result<(), PublicWireError> {
    let result = result.ok_or(PublicWireError::MissingRequiredField)?;
    let identity = result
        .identity
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?;
    if !valid_bounded_text(&identity.contract_lineage, MAX_CONTRACT_LINEAGE_BYTES)
        || identity.contract_version == 0
    {
        return Err(PublicWireError::InvalidValue);
    }
    hash(&identity.contract_bundle_hash)?;
    validate_event_symbol(&identity.query_name)?;
    hash(&identity.query_module_hash)?;
    hash(&identity.query_plan_hash)?;
    validate_event_symbol(&result.outcome)?;
    if result.fields.len() > 64 {
        return Err(PublicWireError::TooManyItems);
    }
    let mut prior = None;
    for field in &result.fields {
        validate_event_symbol(&field.name)?;
        if prior.is_some_and(|value: &str| value >= field.name.as_str()) {
            return Err(PublicWireError::NonCanonical);
        }
        let cardinality = v1::LiveQueryResultCardinality::try_from(field.cardinality)
            .map_err(|_| PublicWireError::InvalidValue)?;
        let valid_count = match cardinality {
            v1::LiveQueryResultCardinality::One => field.records.len() == 1,
            v1::LiveQueryResultCardinality::Maybe => field.records.len() <= 1,
            v1::LiveQueryResultCardinality::Many => field.records.len() <= 4_096,
            v1::LiveQueryResultCardinality::Unspecified => false,
        };
        if !valid_count {
            return Err(PublicWireError::TooManyItems);
        }
        for record in &field.records {
            validate_live_query_record(Some(record))?;
        }
        prior = Some(field.name.as_str());
    }
    Ok(())
}

fn validate_live_cursor(cursor: &[u8]) -> Result<(), PublicWireError> {
    if cursor.is_empty() || cursor.len() > 4_096 {
        Err(PublicWireError::InvalidBytes)
    } else {
        Ok(())
    }
}

fn validate_live_query_update(message: &v1::LiveQueryUpdate) -> Result<(), PublicWireError> {
    use v1::live_query_patch_operation::Operation;
    use v1::live_query_update::Update;
    match message
        .update
        .as_ref()
        .ok_or(PublicWireError::MissingRequiredField)?
    {
        Update::Snapshot(snapshot) => {
            validate_live_query_result(snapshot.result.as_ref())?;
            validate_live_query_frontier(snapshot.frontier.as_ref())?;
            validate_live_cursor(&snapshot.cursor)
        }
        Update::Reset(reset) => {
            match v1::LiveQueryResetReason::try_from(reset.reason)
                .map_err(|_| PublicWireError::InvalidValue)?
            {
                v1::LiveQueryResetReason::Unspecified => {
                    return Err(PublicWireError::InvalidValue);
                }
                v1::LiveQueryResetReason::OutcomeChanged
                | v1::LiveQueryResetReason::DiffLimitExceeded
                | v1::LiveQueryResetReason::DefinitionChanged
                | v1::LiveQueryResetReason::HistoryChanged
                | v1::LiveQueryResetReason::CursorExpired => {}
            }
            validate_live_query_result(reset.result.as_ref())?;
            validate_live_query_frontier(reset.frontier.as_ref())?;
            validate_live_cursor(&reset.cursor)
        }
        Update::Checkpoint(checkpoint) => {
            validate_live_query_frontier(checkpoint.frontier.as_ref())?;
            validate_live_cursor(&checkpoint.cursor)
        }
        Update::Terminal(terminal) => {
            if matches!(
                v1::LiveQueryTerminalReason::try_from(terminal.reason)
                    .map_err(|_| PublicWireError::InvalidValue)?,
                v1::LiveQueryTerminalReason::Unspecified
            ) {
                return Err(PublicWireError::InvalidValue);
            }
            validate_live_query_frontier(terminal.last_frontier.as_ref())
        }
        Update::Patch(patch) => {
            validate_event_symbol(&patch.result_field)?;
            validate_live_query_frontier(patch.frontier.as_ref())?;
            validate_live_cursor(&patch.cursor)?;
            if patch.operations.is_empty() || patch.operations.len() > 500 {
                return Err(PublicWireError::TooManyItems);
            }
            for operation in &patch.operations {
                match operation
                    .operation
                    .as_ref()
                    .ok_or(PublicWireError::MissingRequiredField)?
                {
                    Operation::Insert(insert) => {
                        if insert.index > u32::from(u16::MAX) {
                            return Err(PublicWireError::InvalidValue);
                        }
                        validate_live_query_record(insert.record.as_ref())?;
                    }
                    Operation::Remove(remove) => {
                        if remove.index > u32::from(u16::MAX) {
                            return Err(PublicWireError::InvalidValue);
                        }
                        validate_live_value_record(remove.key.as_ref())?;
                    }
                    Operation::Replace(replace) => {
                        if replace.index > u32::from(u16::MAX) {
                            return Err(PublicWireError::InvalidValue);
                        }
                        validate_live_query_record(replace.record.as_ref())?;
                    }
                    Operation::Move(movement) => {
                        if movement.from > u32::from(u16::MAX) || movement.to > u32::from(u16::MAX)
                        {
                            return Err(PublicWireError::InvalidValue);
                        }
                        validate_live_value_record(movement.key.as_ref())?;
                    }
                }
            }
            Ok(())
        }
    }
}

macro_rules! impl_public_message {
    ($type:ty, $maximum:expr, $maximum_field:expr, $repeated:expr, $oneofs:expr, $preflight:path, $validate:path) => {
        impl PublicMessage for $type {
            const MAX_ENCODED_BYTES: usize = $maximum;

            fn preflight(input: &[u8]) -> Result<(), PublicWireError> {
                preflight_root(input, $maximum, $maximum_field, $repeated, $oneofs)?;
                $preflight(input)
            }

            fn validate_structure(&self) -> Result<(), PublicWireError> {
                $validate(self)
            }
        }
    };
}

impl_public_message!(
    v1::ValidateContractRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    2,
    &[],
    &[],
    preflight_noop,
    validate_validate_contract_request
);
impl_public_message!(
    v1::DescribeEventRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    2,
    &[],
    &[],
    preflight_noop,
    validate_describe_event_request
);
impl_public_message!(
    v1::DescribeEventResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    2,
    &[],
    &[&[1, 2]],
    preflight_noop,
    validate_describe_event_response
);
impl_public_message!(
    v1::ReplayEventsRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    5,
    &[],
    &[],
    preflight_noop,
    validate_replay_events_request
);
impl_public_message!(
    v1::ReplayEventsResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    1,
    &[],
    &[],
    preflight_noop,
    validate_replay_events_response
);
impl_public_message!(
    v1::TailEventsRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    6,
    &[],
    &[],
    preflight_noop,
    validate_tail_events_request
);
impl_public_message!(
    v1::TailEventsResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    2,
    &[],
    &[],
    preflight_noop,
    validate_tail_events_response
);
impl_public_message!(
    v1::ConsumeEventStreamRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    7,
    &[],
    &[],
    preflight_noop,
    validate_consume_event_stream_request
);
impl_public_message!(
    v1::ConsumeEventStreamResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    5,
    &[1],
    &[],
    preflight_noop,
    validate_consume_event_stream_response
);
impl_public_message!(
    v1::AcknowledgeEventStreamRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    5,
    &[],
    &[],
    preflight_noop,
    validate_acknowledge_event_stream_request
);
impl_public_message!(
    v1::NegativeAcknowledgeEventStreamRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    6,
    &[],
    &[],
    preflight_noop,
    validate_negative_acknowledge_event_stream_request
);
impl_public_message!(
    v1::SeekEventStreamConsumerRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    4,
    &[],
    &[],
    preflight_noop,
    validate_seek_event_stream_consumer_request
);
impl_public_message!(
    v1::RetireEventStreamConsumerRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    2,
    &[],
    &[],
    preflight_noop,
    validate_retire_event_stream_consumer_request
);
impl_public_message!(
    v1::GetEventStreamConsumerStatusRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    2,
    &[],
    &[],
    preflight_noop,
    validate_get_event_stream_consumer_status_request
);
impl_public_message!(
    v1::ConsumeContextualSubscriptionRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    4,
    &[],
    &[],
    preflight_noop,
    validate_consume_contextual_subscription_request
);
impl_public_message!(
    v1::ConsumeContextualSubscriptionResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    5,
    &[1],
    &[],
    preflight_noop,
    validate_consume_contextual_subscription_response
);
impl_public_message!(
    v1::AcknowledgeContextualSubscriptionRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    5,
    &[],
    &[],
    preflight_noop,
    validate_acknowledge_contextual_subscription_request
);
impl_public_message!(
    v1::NegativeAcknowledgeContextualSubscriptionRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    6,
    &[],
    &[],
    preflight_noop,
    validate_negative_acknowledge_contextual_subscription_request
);
impl_public_message!(
    v1::GetContextualSubscriptionStatusRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    2,
    &[],
    &[],
    preflight_noop,
    validate_get_contextual_subscription_status_request
);
impl_public_message!(
    v1::ExecuteContextualReactionRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    5,
    &[],
    &[],
    preflight_noop,
    validate_execute_contextual_reaction_request
);
impl_public_message!(
    v1::EventConsumerMutationResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    1,
    &[],
    &[],
    preflight_noop,
    validate_event_consumer_mutation_response
);
impl_public_message!(
    v1::GetEventStreamConsumerStatusResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    3,
    &[],
    &[&[1, 2, 3]],
    preflight_noop,
    validate_get_event_stream_consumer_status_response
);

#[cfg(test)]
mod protected_event_consumer_tests {
    use super::*;

    fn seek_request() -> v1::SeekEventStreamConsumerRequest {
        v1::SeekEventStreamConsumerRequest {
            request_id: RequestId::from_unix_milliseconds_and_random(1, [2; 10])
                .expect("valid uuidv7")
                .into_bytes()
                .to_vec(),
            selection: Some(v1::EventConsumerSelection {
                reactive_module_hash: vec![3; 32],
                operation_name: "TicketActivity".to_owned(),
                parameters: Vec::new(),
                consumer_name: "triage-agent".to_owned(),
            }),
            checkpoint: Some(v1::EventConsumerCheckpoint {
                position: Some(v1::event_consumer_checkpoint::Position::BeforeFirst(
                    v1::Unit {},
                )),
            }),
            progress_cursor: Vec::new(),
        }
    }

    #[test]
    fn protected_status_exposes_only_incarnation_and_fixed_opaque_cursor() {
        use v1::get_event_stream_consumer_status_response::Result;

        let response = v1::GetEventStreamConsumerStatusResponse {
            result: Some(Result::Protected(v1::ProtectedEventConsumerStatus {
                history_incarnation: 7,
                progress_cursor: vec![0xa5; 16],
            })),
        };
        assert_eq!(validate_public_message(&response), Ok(()));

        let mut missing_incarnation = response.clone();
        let Some(Result::Protected(status)) = missing_incarnation.result.as_mut() else {
            panic!("protected status")
        };
        status.history_incarnation = 0;
        assert_eq!(
            validate_public_message(&missing_incarnation),
            Err(PublicWireError::InvalidValue)
        );

        let mut raw_progress_shape = response;
        let Some(Result::Protected(status)) = raw_progress_shape.result.as_mut() else {
            panic!("protected status")
        };
        status.progress_cursor = vec![0; 15];
        assert_eq!(
            validate_public_message(&raw_progress_shape),
            Err(PublicWireError::InvalidValue)
        );
    }

    #[test]
    fn seek_accepts_exact_or_opaque_target_but_never_both_or_neither() {
        let exact = seek_request();
        assert_eq!(validate_public_message(&exact), Ok(()));

        let mut protected = exact.clone();
        protected.checkpoint = None;
        protected.progress_cursor = vec![0x5a; 16];
        assert_eq!(validate_public_message(&protected), Ok(()));

        let mut both = exact.clone();
        both.progress_cursor = vec![0x5a; 16];
        assert_eq!(
            validate_public_message(&both),
            Err(PublicWireError::InvalidValue)
        );

        let mut neither = exact;
        neither.checkpoint = None;
        assert_eq!(
            validate_public_message(&neither),
            Err(PublicWireError::InvalidValue)
        );

        let mut wrong_width = protected;
        wrong_width.progress_cursor.pop();
        assert_eq!(
            validate_public_message(&wrong_width),
            Err(PublicWireError::InvalidValue)
        );
    }
}
impl_public_message!(
    v1::ValidateContractResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    2,
    &[],
    &[&[1, 2]],
    preflight_validate_contract_response,
    validate_validate_contract_response
);
impl_public_message!(
    v1::ExplainCommandRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    3,
    &[],
    &[],
    preflight_explain_command_request,
    validate_explain_command_request
);
impl_public_message!(
    v1::ExplainCommandResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    2,
    &[],
    &[&[1, 2]],
    preflight_explain_command_response,
    validate_explain_command_response
);
impl_public_message!(
    v1::DeployContractRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    3,
    &[],
    &[],
    preflight_noop,
    validate_deploy_contract_request
);
impl_public_message!(
    v1::DeployContractResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    4,
    &[],
    &[&[1, 2, 3, 4]],
    preflight_deploy_contract_response,
    validate_deploy_contract_response
);
impl_public_message!(
    v1::GetActiveContractRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    1,
    &[],
    &[],
    preflight_noop,
    validate_get_active_contract_request
);
impl_public_message!(
    v1::GetActiveContractResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    2,
    &[],
    &[&[1, 2]],
    preflight_get_active_contract_response,
    validate_get_active_contract_response
);
impl_public_message!(
    v1::GetContractVersionRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    3,
    &[],
    &[],
    preflight_noop,
    validate_get_contract_version_request
);
impl_public_message!(
    v1::GetContractVersionResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    2,
    &[],
    &[&[1, 2]],
    preflight_get_contract_version_response,
    validate_get_contract_version_response
);
impl_public_message!(
    v1::DiscoverCommandToolsRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    4,
    &[],
    &[],
    preflight_discover_command_tools_request,
    validate_discover_command_tools_request
);
impl_public_message!(
    v1::DiscoverCommandToolsResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    3,
    &[],
    &[&[1, 2, 3]],
    preflight_discover_command_tools_response,
    validate_discover_command_tools_response
);
impl_public_message!(
    v1::DiscoverResourcesRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    5,
    &[],
    &[],
    preflight_discover_resources_request,
    validate_discover_resources_request
);
impl_public_message!(
    v1::DiscoverResourcesResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    3,
    &[],
    &[&[1, 2, 3]],
    preflight_discover_resources_response,
    validate_discover_resources_response
);
impl_public_message!(
    v1::GetReactiveWakeupRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    1,
    &[],
    &[],
    preflight_noop,
    validate_get_reactive_wakeup_request
);
impl_public_message!(
    v1::GetReactiveWakeupResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    1,
    &[],
    &[],
    preflight_noop,
    validate_get_reactive_wakeup_response
);
impl_public_message!(
    v1::GetOutcomeRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    5,
    &[],
    &[],
    preflight_noop,
    validate_get_outcome_request
);
impl_public_message!(
    v1::GetOutcomeResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    2,
    &[],
    &[&[1, 2]],
    preflight_get_outcome_response,
    validate_get_outcome_response
);
impl_public_message!(
    v1::GetEntityRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    5,
    &[],
    &[],
    preflight_get_entity_request,
    validate_get_entity_request
);
impl_public_message!(
    v1::GetEntityResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    2,
    &[],
    &[&[1, 2]],
    preflight_get_entity_response,
    validate_get_entity_response
);
impl_public_message!(
    v1::ScanIndexRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    6,
    &[4],
    &[],
    preflight_scan_index_request,
    validate_scan_index_request
);
impl_public_message!(
    v1::ScanIndexResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    1,
    &[],
    &[],
    preflight_scan_index_response,
    validate_scan_index_response
);
impl_public_message!(
    v1::QueryProjectionRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    7,
    &[4],
    &[],
    preflight_query_projection_request,
    validate_query_projection_request
);
impl_public_message!(
    v1::QueryProjectionResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    4,
    &[],
    &[&[1, 2, 3, 4]],
    preflight_query_projection_response,
    validate_query_projection_response
);
impl_public_message!(
    v1::ProjectionStatus,
    MAX_PUBLIC_RESPONSE_BYTES,
    7,
    &[],
    &[],
    preflight_projection_status,
    validate_projection_status
);
impl_public_message!(
    v1::GetProjectionStatusRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    3,
    &[],
    &[],
    preflight_get_projection_status_request,
    validate_get_projection_status_request
);
impl_public_message!(
    v1::GetProjectionStatusResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    2,
    &[],
    &[&[1, 2]],
    preflight_get_projection_status_response,
    validate_get_projection_status_response
);
impl_public_message!(
    v1::GetCommitRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    2,
    &[],
    &[],
    preflight_noop,
    validate_get_commit_request
);
impl_public_message!(
    v1::GetCommitResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    2,
    &[],
    &[&[1, 2]],
    preflight_get_commit_response,
    validate_get_commit_response
);
impl_public_message!(
    v1::ScanCommitsRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    2,
    &[],
    &[],
    preflight_scan_commits_request,
    validate_scan_commits_request
);
impl_public_message!(
    v1::ScanCommitsResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    1,
    &[],
    &[],
    preflight_scan_commits_response,
    validate_scan_commits_response
);
impl_public_message!(
    v1::SubscribeCommitsRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    3,
    &[],
    &[],
    preflight_noop,
    validate_subscribe_commits_request
);
impl_public_message!(
    v1::CommitNotification,
    MAX_PUBLIC_RESPONSE_BYTES,
    2,
    &[],
    &[&[1, 2]],
    preflight_commit_notification,
    validate_commit_notification
);
impl_public_message!(
    v1::TraceProvenanceRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    2,
    &[],
    &[],
    preflight_trace_provenance_request,
    validate_trace_provenance_request
);
impl_public_message!(
    v1::TraceProvenanceResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    2,
    &[],
    &[&[1, 2]],
    preflight_trace_provenance_response,
    validate_trace_provenance_response
);
impl_public_message!(
    v1::HealthRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    1,
    &[],
    &[],
    preflight_noop,
    validate_health_request
);
impl_public_message!(
    v1::HealthResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    2,
    &[],
    &[&[1, 2]],
    preflight_health_response,
    validate_health_response
);
impl_public_message!(
    v1::StatsRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    1,
    &[],
    &[],
    preflight_noop,
    validate_stats_request
);
impl_public_message!(
    v1::StatsResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    5,
    &[],
    &[],
    preflight_noop,
    validate_stats_response
);
impl_public_message!(
    v1::CreateCapabilityRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    8,
    &[7],
    &[],
    preflight_create_capability_request,
    validate_create_capability_request
);
impl_public_message!(
    v1::CreateCapabilityResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    2,
    &[],
    &[&[1, 2]],
    preflight_create_capability_response,
    validate_create_capability_response
);
impl_public_message!(
    v1::RevokeCapabilityRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    3,
    &[],
    &[],
    preflight_noop,
    validate_revoke_capability_request
);
impl_public_message!(
    v1::RevokeCapabilityResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    3,
    &[],
    &[&[1, 2, 3]],
    preflight_revoke_capability_response,
    validate_revoke_capability_response
);
impl_public_message!(
    v1::ListPendingOutboxDeliveriesRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    2,
    &[],
    &[],
    preflight_list_outbox_request,
    validate_list_pending_outbox_deliveries_request
);
impl_public_message!(
    v1::ListPendingOutboxDeliveriesResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    1,
    &[],
    &[],
    preflight_list_outbox_response,
    validate_list_pending_outbox_deliveries_response
);
impl_public_message!(
    v1::CreateOfflineBackupRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    3,
    &[],
    &[],
    preflight_noop,
    validate_create_offline_backup_request
);
impl_public_message!(
    v1::CreateOfflineBackupResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    2,
    &[],
    &[],
    preflight_offline_maintenance_start_response,
    validate_create_offline_backup_response
);
impl_public_message!(
    v1::RestoreOfflineBackupRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    4,
    &[],
    &[],
    preflight_noop,
    validate_restore_offline_backup_request
);
impl_public_message!(
    v1::RestoreOfflineBackupResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    2,
    &[],
    &[],
    preflight_offline_maintenance_start_response,
    validate_restore_offline_backup_response
);
impl_public_message!(
    v1::GetOfflineMaintenanceOperationRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    2,
    &[],
    &[],
    preflight_noop,
    validate_get_offline_maintenance_operation_request
);
impl_public_message!(
    v1::GetOfflineMaintenanceOperationResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    2,
    &[],
    &[&[1, 2]],
    preflight_get_offline_maintenance_operation_response,
    validate_get_offline_maintenance_operation_response
);
impl_public_message!(
    v1::CheckContractMigrationRequest,
    MAX_CONTRACT_MIGRATION_REQUEST_BYTES,
    4,
    &[],
    &[],
    preflight_noop,
    validate_check_contract_migration_request
);
impl_public_message!(
    v1::CheckContractMigrationResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    2,
    &[],
    &[],
    preflight_contract_migration_start_response,
    validate_check_contract_migration_response
);
impl_public_message!(
    v1::ApplyContractMigrationRequest,
    MAX_CONTRACT_MIGRATION_REQUEST_BYTES,
    6,
    &[],
    &[],
    preflight_noop,
    validate_apply_contract_migration_request
);
impl_public_message!(
    v1::ApplyContractMigrationResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    2,
    &[],
    &[],
    preflight_contract_migration_start_response,
    validate_apply_contract_migration_response
);
impl_public_message!(
    v1::GetContractMigrationOperationRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    2,
    &[],
    &[],
    preflight_noop,
    validate_get_contract_migration_operation_request
);
impl_public_message!(
    v1::GetContractMigrationOperationResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    2,
    &[],
    &[&[1, 2]],
    preflight_get_contract_migration_operation_response,
    validate_get_contract_migration_operation_response
);
impl_public_message!(
    v1::StartApplicationInstallationRequest,
    MAX_APPLICATION_INSTALLATION_REQUEST_BYTES,
    4,
    &[],
    &[],
    preflight_start_application_installation_request,
    validate_start_application_installation_request
);
impl_public_message!(
    v1::StartApplicationInstallationResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    2,
    &[],
    &[],
    preflight_start_application_installation_response,
    validate_start_application_installation_response
);
impl_public_message!(
    v1::GetApplicationInstallationRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    2,
    &[],
    &[],
    preflight_noop,
    validate_get_application_installation_request
);
impl_public_message!(
    v1::GetApplicationInstallationResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    2,
    &[],
    &[&[1, 2]],
    preflight_get_application_installation_response,
    validate_get_application_installation_response
);
impl_public_message!(
    v1::StartApplicationExportRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    4,
    &[],
    &[],
    preflight_start_application_export_request,
    validate_start_application_export_request
);
impl_public_message!(
    v1::StartApplicationExportResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    3,
    &[],
    &[],
    preflight_start_application_export_response,
    validate_start_application_export_response
);
impl_public_message!(
    v1::GetApplicationExportPageRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    4,
    &[],
    &[],
    preflight_noop,
    validate_get_application_export_page_request
);
impl_public_message!(
    v1::GetApplicationExportPageResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    1,
    &[],
    &[],
    preflight_get_application_export_page_response,
    validate_get_application_export_page_response
);
impl_public_message!(
    v1::GetApplicationExportRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    2,
    &[],
    &[],
    preflight_noop,
    validate_get_application_export_request
);
impl_public_message!(
    v1::GetApplicationExportResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    2,
    &[],
    &[&[1, 2]],
    preflight_get_application_export_response,
    validate_get_application_export_response
);
impl_public_message!(
    v1::CancelApplicationExportRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    2,
    &[],
    &[],
    preflight_noop,
    validate_cancel_application_export_request
);
impl_public_message!(
    v1::CancelApplicationExportResponse,
    MAX_PUBLIC_RESPONSE_BYTES,
    2,
    &[],
    &[&[1, 2]],
    preflight_cancel_application_export_response,
    validate_cancel_application_export_response
);

impl_public_message!(
    v1::WatchNamedQueryRequest,
    MAX_PUBLIC_REQUEST_BYTES,
    5,
    &[4],
    &[],
    preflight_noop,
    validate_watch_named_query_request
);
impl_public_message!(
    v1::LiveQueryUpdate,
    MAX_PUBLIC_RESPONSE_BYTES,
    5,
    &[],
    &[&[1, 2, 3, 4, 5]],
    preflight_noop,
    validate_live_query_update
);

impl PublicMessage for v1::ExecuteCommandRequest {
    const MAX_ENCODED_BYTES: usize = crate::MAX_EXECUTE_REQUEST_BYTES;

    fn preflight(input: &[u8]) -> Result<(), PublicWireError> {
        preflight_root(input, Self::MAX_ENCODED_BYTES, 4, &[], &[])?;
        preflight_execute_request(input)
    }

    fn validate_structure(&self) -> Result<(), PublicWireError> {
        crate::validate_execute_request(self).map_err(|_| PublicWireError::InconsistentFields)
    }
}

impl PublicMessage for v1::ExecuteCommandResponse {
    const MAX_ENCODED_BYTES: usize = crate::MAX_EXECUTE_RESPONSE_BYTES;

    fn preflight(input: &[u8]) -> Result<(), PublicWireError> {
        preflight_root(input, Self::MAX_ENCODED_BYTES, 9, &[], &[])?;
        preflight_execute_response(input)
    }

    fn validate_structure(&self) -> Result<(), PublicWireError> {
        crate::validate_execute_response(self).map_err(|_| PublicWireError::InconsistentFields)
    }
}

impl PublicMessage for v1::ExecuteCommandBatchRequest {
    const MAX_ENCODED_BYTES: usize = MAX_PUBLIC_REQUEST_BYTES;

    fn preflight(input: &[u8]) -> Result<(), PublicWireError> {
        preflight_root(input, Self::MAX_ENCODED_BYTES, 1, &[1], &[])?;
        preflight_execute_batch_request(input)
    }

    fn validate_structure(&self) -> Result<(), PublicWireError> {
        if self.commands.is_empty() || self.commands.len() > MAX_COMMAND_BATCH_ITEMS {
            return Err(PublicWireError::TooManyItems);
        }
        let mut request_ids = std::collections::BTreeSet::new();
        for command in &self.commands {
            crate::validate_execute_request(command)
                .map_err(|_| PublicWireError::InconsistentFields)?;
            if !request_ids.insert(command.request_id.as_slice()) {
                return Err(PublicWireError::NonCanonical);
            }
        }
        Ok(())
    }
}

impl PublicMessage for v1::ExecuteCommandBatchResponse {
    const MAX_ENCODED_BYTES: usize = MAX_PUBLIC_RESPONSE_BYTES;

    fn preflight(input: &[u8]) -> Result<(), PublicWireError> {
        preflight_root(input, Self::MAX_ENCODED_BYTES, 2, &[1, 2], &[])?;
        preflight_execute_batch_response(input)
    }

    fn validate_structure(&self) -> Result<(), PublicWireError> {
        if self.responses.len() > MAX_COMMAND_BATCH_ITEMS
            || self.items.len() > MAX_COMMAND_BATCH_ITEMS
        {
            return Err(PublicWireError::TooManyItems);
        }
        // Current servers always populate `items`. Older servers may omit it and
        // only fill legacy success rows; accept that empty-items shape here.
        if self.items.is_empty() {
            if self.responses.is_empty() {
                return Err(PublicWireError::TooManyItems);
            }
            for response in &self.responses {
                crate::validate_execute_response(response)
                    .map_err(|_| PublicWireError::InconsistentFields)?;
            }
            return Ok(());
        }
        let mut all_success = true;
        for item in &self.items {
            match item.result.as_ref() {
                Some(v1::execute_command_batch_item::Result::Response(response)) => {
                    crate::validate_execute_response(response)
                        .map_err(|_| PublicWireError::InconsistentFields)?;
                }
                Some(v1::execute_command_batch_item::Result::Error(error)) => {
                    all_success = false;
                    crate::application_error_from_proto(error)
                        .map_err(|_| PublicWireError::InconsistentFields)?;
                }
                None => return Err(PublicWireError::MissingRequiredField),
            }
        }
        // Field-1 rule: responses is populated iff every item succeeded, and
        // then mirrors items positionally. Never partially populate field 1.
        if all_success {
            if self.responses.len() != self.items.len() {
                return Err(PublicWireError::InconsistentFields);
            }
            for (response, item) in self.responses.iter().zip(&self.items) {
                let Some(v1::execute_command_batch_item::Result::Response(item_response)) =
                    item.result.as_ref()
                else {
                    return Err(PublicWireError::InconsistentFields);
                };
                if response != item_response {
                    return Err(PublicWireError::InconsistentFields);
                }
            }
        } else if !self.responses.is_empty() {
            return Err(PublicWireError::InconsistentFields);
        }
        Ok(())
    }
}

#[cfg(test)]
mod live_query_tests {
    use super::*;

    fn request_id_bytes() -> Vec<u8> {
        RequestId::from_unix_milliseconds_and_random(1, [2; 10])
            .expect("valid uuidv7")
            .into_bytes()
            .to_vec()
    }

    fn value(value: u64) -> v1::Value {
        v1::Value {
            kind: Some(v1::value::Kind::U64Value(value)),
        }
    }

    #[test]
    fn live_watch_request_requires_canonical_parameters_and_bounded_cursor() {
        let mut request = v1::WatchNamedQueryRequest {
            request_id: request_id_bytes(),
            reactive_module_hash: vec![1; 32],
            operation_name: "WatchItems".to_owned(),
            parameters: vec![
                v1::LiveQueryParameter {
                    name: "z".to_owned(),
                    value: Some(value(1)),
                },
                v1::LiveQueryParameter {
                    name: "a".to_owned(),
                    value: Some(value(2)),
                },
            ],
            cursor: None,
        };
        assert_eq!(
            validate_public_message(&request),
            Err(PublicWireError::NonCanonical)
        );
        request
            .parameters
            .sort_by(|left, right| left.name.cmp(&right.name));
        assert_eq!(validate_public_message(&request), Ok(()));
        request.cursor = Some(vec![0; 4_097]);
        assert!(validate_public_message(&request).is_err());
    }

    #[test]
    fn live_update_is_closed_and_restore_fenced() {
        let mut update = v1::LiveQueryUpdate {
            update: Some(v1::live_query_update::Update::Checkpoint(
                v1::LiveQueryCheckpoint {
                    frontier: Some(v1::LiveQueryFrontier {
                        history_incarnation: 1,
                        application_head: 7,
                    }),
                    cursor: vec![1],
                },
            )),
        };
        assert_eq!(validate_public_message(&update), Ok(()));
        let Some(v1::live_query_update::Update::Checkpoint(checkpoint)) = update.update.as_mut()
        else {
            panic!("checkpoint")
        };
        checkpoint
            .frontier
            .as_mut()
            .expect("frontier")
            .history_incarnation = 0;
        assert_eq!(
            validate_public_message(&update),
            Err(PublicWireError::InvalidValue)
        );
    }
}

#[cfg(test)]
mod application_export_tests {
    use super::*;

    fn uuid_bytes(fill: u8) -> Vec<u8> {
        [
            0x01, 0x8f, 0, 0, 0, fill, 0x70, 1, 0x80, fill, 0, 0, 0, 0, 0, fill,
        ]
        .to_vec()
    }

    fn selection() -> v1::ApplicationExportSelection {
        v1::ApplicationExportSelection {
            contract_lineage: "TicketDesk".to_owned(),
            scope: v1::CapabilityApplicationExportScope::WholeApplication as i32,
            entities: true,
            events: true,
            provenance: false,
            public_audit: false,
        }
    }

    fn snapshot() -> v1::ApplicationExportSnapshotBinding {
        v1::ApplicationExportSnapshotBinding {
            database_id: uuid_bytes(3),
            history_incarnation: 1,
            application_frontier: 7,
            administration_frontier: 8,
            contract_version: 2,
            contract_bundle_hash: vec![4; 32],
            query_module_hashes: vec![vec![5; 32], vec![6; 32]],
            reactive_module_hashes: vec![vec![7; 32]],
        }
    }

    fn operation(phase: v1::ApplicationExportPhase) -> v1::ApplicationExportOperation {
        let terminal = matches!(
            phase,
            v1::ApplicationExportPhase::Completed
                | v1::ApplicationExportPhase::Cancelled
                | v1::ApplicationExportPhase::Expired
                | v1::ApplicationExportPhase::FailedClosed
        );
        let manifest = if terminal {
            br#"{"complete":true}"#.to_vec()
        } else {
            Vec::new()
        };
        let receipt = if terminal {
            br#"{"terminal":true}"#.to_vec()
        } else {
            Vec::new()
        };
        let failure = match phase {
            v1::ApplicationExportPhase::Cancelled => v1::ApplicationExportFailure::Cancelled,
            v1::ApplicationExportPhase::Expired => v1::ApplicationExportFailure::LeaseExpired,
            v1::ApplicationExportPhase::FailedClosed => {
                v1::ApplicationExportFailure::SnapshotUnavailable
            }
            _ => v1::ApplicationExportFailure::Unspecified,
        };
        v1::ApplicationExportOperation {
            operation_id: uuid_bytes(2),
            selection: Some(selection()),
            snapshot: Some(snapshot()),
            phase: phase as i32,
            lease_expires_at: Some(v1::Timestamp {
                seconds: 1,
                nanos: 0,
            }),
            pages_released: 0,
            rows_released: 0,
            bytes_released: 0,
            failure: failure as i32,
            manifest_hash: if terminal {
                hash_application_export_manifest(&manifest)
                    .into_bytes()
                    .to_vec()
            } else {
                Vec::new()
            },
            receipt_hash: if terminal {
                hash_application_export_receipt(&receipt)
                    .into_bytes()
                    .to_vec()
            } else {
                Vec::new()
            },
            canonical_manifest_json: manifest,
            canonical_receipt_json: receipt,
        }
    }

    fn page(lines: Vec<Vec<u8>>) -> v1::ApplicationExportPage {
        let operation_id = ApplicationExportOperationId::from_bytes(
            uuid_bytes(2).try_into().expect("operation bytes"),
        )
        .expect("operation ID");
        let borrowed = lines.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let preimage = canonical_application_export_page_preimage(
            operation_id,
            std::num::NonZeroU64::new(1).expect("page"),
            ApplicationExportClassV1::Entity,
            &borrowed,
            false,
            false,
        )
        .expect("preimage");
        v1::ApplicationExportPage {
            operation_id: uuid_bytes(2),
            page_number: 1,
            record_class: v1::ApplicationExportRecordClass::Entity as i32,
            canonical_json_lines: lines,
            next_cursor: vec![9; 32],
            class_complete: false,
            operation_complete: false,
            page_hash: hash_application_export_page(&preimage)
                .into_bytes()
                .to_vec(),
        }
    }

    #[test]
    fn export_start_and_terminal_shapes_are_closed() {
        let response = v1::StartApplicationExportResponse {
            disposition: v1::ApplicationExportStartDisposition::Accepted as i32,
            operation: Some(operation(v1::ApplicationExportPhase::Accepted)),
            cursor: vec![9; 32],
        };
        assert_eq!(validate_public_message(&response), Ok(()));

        let mut invalid = response;
        invalid.operation.as_mut().expect("operation").phase =
            v1::ApplicationExportPhase::Completed as i32;
        assert_eq!(
            validate_public_message(&invalid),
            Err(PublicWireError::InconsistentFields)
        );

        assert_eq!(
            validate_application_export_operation(&operation(
                v1::ApplicationExportPhase::Completed
            )),
            Ok(())
        );
    }

    #[test]
    fn export_page_hash_and_row_preflight_are_enforced() {
        let mut response = v1::GetApplicationExportPageResponse {
            page: Some(page(vec![br#"{"entity":"Ticket"}"#.to_vec()])),
        };
        assert_eq!(validate_public_message(&response), Ok(()));
        response.page.as_mut().expect("page").page_hash[0] ^= 1;
        assert_eq!(
            validate_public_message(&response),
            Err(PublicWireError::InconsistentFields)
        );

        let oversized = v1::GetApplicationExportPageResponse {
            page: Some(page(vec![
                b"{}".to_vec();
                MAX_APPLICATION_EXPORT_PAGE_ROWS + 1
            ])),
        };
        let bytes = oversized.encode_to_vec();
        assert_eq!(
            decode_public_message::<v1::GetApplicationExportPageResponse>(&bytes),
            Err(PublicWireError::PreflightLimitExceeded)
        );
    }

    #[test]
    fn export_exchange_rejects_operation_identity_substitution() {
        let request = v1::StartApplicationExportRequest {
            request_id: uuid_bytes(1),
            operation_id: uuid_bytes(2),
            selection: Some(selection()),
            lease_seconds: MIN_APPLICATION_EXPORT_LEASE_SECONDS,
            canonical_portability_manifest_json: Vec::new(),
        };
        let mut response = v1::StartApplicationExportResponse {
            disposition: v1::ApplicationExportStartDisposition::Accepted as i32,
            operation: Some(operation(v1::ApplicationExportPhase::Accepted)),
            cursor: vec![9; 32],
        };
        assert_eq!(
            validate_start_application_export_exchange(&request, &response),
            Ok(())
        );
        let mut malformed_portability = request.clone();
        malformed_portability.canonical_portability_manifest_json = b"{}".to_vec();
        assert_eq!(
            validate_public_message(&malformed_portability),
            Err(PublicWireError::InvalidBytes)
        );
        response.operation.as_mut().expect("operation").operation_id = uuid_bytes(8);
        assert_eq!(
            validate_start_application_export_exchange(&request, &response),
            Err(PublicWireError::InconsistentFields)
        );
    }
}
