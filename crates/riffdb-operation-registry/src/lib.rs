#![forbid(unsafe_code)]

//! Closed declarative inventory for every public RiffDB operation.
//!
//! This leaf crate is data only: declarations grant no authority and perform
//! no validation or I/O. Offline generators consume the inventory and emit
//! adapters into their owning crates.

use riffdb_types::{CapabilityPermissionKindV1, ServiceIngressKindV1, ServiceOperationV1};

/// Registry schema version. Advance only with a reviewed generator change.
pub const OPERATION_REGISTRY_VERSION: u32 = 1;

/// Closed idempotency classification used by generated adapters.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdempotencyClass {
    /// A nonmutating operation safe to repeat.
    ReadOnly,
    /// A mutation carrying a stable replay identity.
    IdempotentMutation,
    /// A bounded subscription or server-streaming read.
    StreamingRead,
}

/// Closed audience vocabulary for public operations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationAudience {
    /// Application-authored code and application SDKs.
    Application,
    /// Database operators and administrative tooling.
    Operator,
    /// Authorized agent-facing discovery or invocation.
    Agent,
}

/// ADR-0118 output-redaction classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutputRedactionClass {
    /// Safe after the service has applied its semantic redaction policy.
    ServiceRedacted,
    /// Operational output requiring operator authorization.
    OperatorProtected,
    /// Opaque token or secret material that adapters must never print.
    Secret,
}

/// Closed field-mapping vocabulary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FieldMappingKind {
    /// The checked DTO and Protobuf value have the same scalar representation.
    Identity,
    /// A semantic newtype owns construction and validation.
    Newtype,
    /// Bytes cross a textual surface as canonical base64.
    Base64,
    /// A closed enumeration crosses as its stable tag.
    EnumTag,
    /// A nested checked record is mapped recursively.
    NestedRecord,
    /// A repeated value is checked against a declared bound.
    BoundedRepeated,
}

/// One named bound consumed by generated validation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BoundDeclaration {
    /// Closed message boundary to which the maximum applies.
    pub target: BoundTarget,
    /// Inclusive maximum.
    pub maximum: usize,
}

/// Closed message boundaries that every operation must declare.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum BoundTarget {
    /// Maximum encoded Protobuf request bytes.
    EncodedRequestBytes,
    /// Maximum encoded Protobuf response or stream-item bytes.
    EncodedResponseBytes,
}

/// One closed root mapping between a Protobuf message and its checked DTO.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FieldMapping {
    /// Protobuf field path, with `$request` or `$response` as the root.
    pub proto_path: &'static str,
    /// Checked DTO field path.
    pub dto_path: &'static str,
    /// Required conversion kind.
    pub kind: FieldMappingKind,
}

/// Complete registry declaration for one public semantic operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OperationDeclaration {
    /// Closed semantic operation identity.
    pub operation: ServiceOperationV1,
    /// Fully qualified Protobuf request message.
    pub proto_request: &'static str,
    /// Fully qualified Protobuf response message or stream item.
    pub proto_response: &'static str,
    /// Checked service request DTO path.
    pub dto_request: &'static str,
    /// Checked service result DTO path.
    pub dto_result: &'static str,
    /// Every bound enforced by the generated public-message exchange.
    pub bounds: &'static [BoundDeclaration],
    /// Trusted ingress kinds permitted to reach the shared service path.
    pub ingress: &'static [ServiceIngressKindV1],
    /// Capability permission checked by the shared authorizer.
    pub permission: CapabilityPermissionKindV1,
    /// Replay classification.
    pub idempotency: IdempotencyClass,
    /// Redaction class of released output.
    pub output_redaction: OutputRedactionClass,
    /// Intended public audiences.
    pub audiences: &'static [OperationAudience],
    /// Closed message-root mappings; field expansion is descriptor-driven.
    pub field_map: &'static [FieldMapping],
}

const ALL_INGRESS: &[ServiceIngressKindV1] = &ServiceIngressKindV1::ALL;
const APP_AUDIENCE: &[OperationAudience] = &[OperationAudience::Application];
const OPERATOR_AUDIENCE: &[OperationAudience] = &[OperationAudience::Operator];
const AGENT_AUDIENCE: &[OperationAudience] =
    &[OperationAudience::Application, OperationAudience::Agent];
const STANDARD_EXCHANGE_BOUNDS: &[BoundDeclaration] = &[
    BoundDeclaration {
        target: BoundTarget::EncodedRequestBytes,
        maximum: 1_048_576,
    },
    BoundDeclaration {
        target: BoundTarget::EncodedResponseBytes,
        maximum: 4_194_304,
    },
];
const COMMAND_EXCHANGE_BOUNDS: &[BoundDeclaration] = &[
    BoundDeclaration {
        target: BoundTarget::EncodedRequestBytes,
        maximum: 8_388_608,
    },
    BoundDeclaration {
        target: BoundTarget::EncodedResponseBytes,
        maximum: 4_194_304,
    },
];
const CONTRACT_MIGRATION_EXCHANGE_BOUNDS: &[BoundDeclaration] = &[
    BoundDeclaration {
        target: BoundTarget::EncodedRequestBytes,
        maximum: 33_554_432,
    },
    BoundDeclaration {
        target: BoundTarget::EncodedResponseBytes,
        maximum: 4_194_304,
    },
];
const APPLICATION_BUNDLE_EXCHANGE_BOUNDS: &[BoundDeclaration] = &[
    BoundDeclaration {
        target: BoundTarget::EncodedRequestBytes,
        maximum: 4_325_376,
    },
    BoundDeclaration {
        target: BoundTarget::EncodedResponseBytes,
        maximum: 4_194_304,
    },
];
const ROOT_FIELD_MAP: &[FieldMapping] = &[
    FieldMapping {
        proto_path: "$request",
        dto_path: "request",
        kind: FieldMappingKind::NestedRecord,
    },
    FieldMapping {
        proto_path: "$response",
        dto_path: "result",
        kind: FieldMappingKind::NestedRecord,
    },
];

macro_rules! declaration_with_types_and_bounds {
    ($operation:ident, $package:literal, $request:literal, $response:literal,
     $dto_request:expr, $dto_result:expr, $bounds:ident,
     $permission:ident, $idempotency:ident, $redaction:ident, $audience:ident) => {
        OperationDeclaration {
            operation: ServiceOperationV1::$operation,
            proto_request: concat!($package, ".", $request),
            proto_response: concat!($package, ".", $response),
            dto_request: $dto_request,
            dto_result: $dto_result,
            bounds: $bounds,
            ingress: ALL_INGRESS,
            permission: CapabilityPermissionKindV1::$permission,
            idempotency: IdempotencyClass::$idempotency,
            output_redaction: OutputRedactionClass::$redaction,
            audiences: $audience,
            field_map: ROOT_FIELD_MAP,
        }
    };
}

macro_rules! declaration_with_types {
    ($operation:ident, $package:literal, $request:literal, $response:literal,
     $dto_request:expr, $dto_result:expr,
     $permission:ident, $idempotency:ident, $redaction:ident, $audience:ident) => {
        declaration_with_types_and_bounds!(
            $operation,
            $package,
            $request,
            $response,
            $dto_request,
            $dto_result,
            STANDARD_EXCHANGE_BOUNDS,
            $permission,
            $idempotency,
            $redaction,
            $audience
        )
    };
}

macro_rules! declaration {
    ($operation:ident, $package:literal, $request:literal, $response:literal,
     $permission:ident, $idempotency:ident, $redaction:ident, $audience:ident) => {
        declaration_with_types!(
            $operation,
            $package,
            $request,
            $response,
            concat!("riffdb_service::", $request),
            concat!("riffdb_service::", stringify!($operation), "Result"),
            $permission,
            $idempotency,
            $redaction,
            $audience
        )
    };
}

/// Exactly one declaration per [`ServiceOperationV1`], in stable tag order.
pub const OPERATIONS: [OperationDeclaration; 57] = [
    declaration_with_types!(
        ValidateContract,
        "riffdb.v1",
        "ValidateContractRequest",
        "ValidateContractResponse",
        "riffdb_service::ValidateContractRequest",
        "riffdb_service::ContractValidationResult",
        ValidateContract,
        ReadOnly,
        ServiceRedacted,
        APP_AUDIENCE
    ),
    declaration!(
        ExplainCommand,
        "riffdb.v1",
        "ExplainCommandRequest",
        "ExplainCommandResponse",
        ExplainCommand,
        ReadOnly,
        ServiceRedacted,
        APP_AUDIENCE
    ),
    declaration!(
        DeployContract,
        "riffdb.v1",
        "DeployContractRequest",
        "DeployContractResponse",
        DeployContract,
        IdempotentMutation,
        OperatorProtected,
        OPERATOR_AUDIENCE
    ),
    declaration!(
        GetActiveContract,
        "riffdb.v1",
        "GetActiveContractRequest",
        "GetActiveContractResponse",
        ReadContract,
        ReadOnly,
        ServiceRedacted,
        APP_AUDIENCE
    ),
    declaration!(
        GetContractVersion,
        "riffdb.v1",
        "GetContractVersionRequest",
        "GetContractVersionResponse",
        ReadContract,
        ReadOnly,
        ServiceRedacted,
        APP_AUDIENCE
    ),
    declaration_with_types_and_bounds!(
        ExecuteCommand,
        "riffdb.v1",
        "ExecuteCommandRequest",
        "ExecuteCommandResponse",
        "riffdb_service::ExecuteCommandRequest",
        "riffdb_service::ExecuteCommandResult",
        COMMAND_EXCHANGE_BOUNDS,
        InvokeCommand,
        IdempotentMutation,
        ServiceRedacted,
        APP_AUDIENCE
    ),
    declaration_with_types!(
        ResolveCommandOutcome,
        "riffdb.v1",
        "GetOutcomeRequest",
        "GetOutcomeResponse",
        "riffdb_service::ResolveCommandOutcomeRequest",
        "riffdb_service::ResolveCommandOutcomeResult",
        InvokeCommand,
        ReadOnly,
        ServiceRedacted,
        APP_AUDIENCE
    ),
    declaration!(
        GetEntity,
        "riffdb.v1",
        "GetEntityRequest",
        "GetEntityResponse",
        ReadEntity,
        ReadOnly,
        ServiceRedacted,
        APP_AUDIENCE
    ),
    declaration!(
        ScanIndex,
        "riffdb.v1",
        "ScanIndexRequest",
        "ScanIndexResponse",
        ScanIndex,
        ReadOnly,
        ServiceRedacted,
        APP_AUDIENCE
    ),
    declaration!(
        QueryProjection,
        "riffdb.v1",
        "QueryProjectionRequest",
        "QueryProjectionResponse",
        QueryProjection,
        ReadOnly,
        ServiceRedacted,
        APP_AUDIENCE
    ),
    declaration!(
        GetProjectionStatus,
        "riffdb.v1",
        "GetProjectionStatusRequest",
        "GetProjectionStatusResponse",
        ReadProjectionStatus,
        ReadOnly,
        ServiceRedacted,
        APP_AUDIENCE
    ),
    declaration!(
        GetCommit,
        "riffdb.v1",
        "GetCommitRequest",
        "GetCommitResponse",
        ReadCommit,
        ReadOnly,
        ServiceRedacted,
        APP_AUDIENCE
    ),
    declaration!(
        ScanCommits,
        "riffdb.v1",
        "ScanCommitsRequest",
        "ScanCommitsResponse",
        ScanCommits,
        ReadOnly,
        ServiceRedacted,
        APP_AUDIENCE
    ),
    declaration_with_types!(
        SubscribeToCommits,
        "riffdb.v1",
        "SubscribeCommitsRequest",
        "CommitNotification",
        "riffdb_service::SubscribeToCommitsRequest",
        "riffdb_service::SubscribeToCommitsResult",
        SubscribeCommits,
        StreamingRead,
        ServiceRedacted,
        APP_AUDIENCE
    ),
    declaration!(
        TraceProvenance,
        "riffdb.v1",
        "TraceProvenanceRequest",
        "TraceProvenanceResponse",
        ReadProvenance,
        ReadOnly,
        OperatorProtected,
        OPERATOR_AUDIENCE
    ),
    declaration_with_types!(
        GetHealth,
        "riffdb.v1",
        "HealthRequest",
        "HealthResponse",
        "riffdb_service::HealthRequest",
        "riffdb_service::HealthResult",
        ReadHealth,
        ReadOnly,
        ServiceRedacted,
        OPERATOR_AUDIENCE
    ),
    declaration_with_types!(
        GetStatistics,
        "riffdb.v1",
        "StatsRequest",
        "StatsResponse",
        "riffdb_service::StatisticsRequest",
        "riffdb_service::StatisticsResult",
        ReadStatistics,
        ReadOnly,
        OperatorProtected,
        OPERATOR_AUDIENCE
    ),
    declaration_with_types!(
        CreateCapability,
        "riffdb.v1",
        "CreateCapabilityRequest",
        "CreateCapabilityResponse",
        "riffdb_service::CreateCapabilityInvocation",
        "riffdb_service::CreateCapabilityResult",
        CreateCapability,
        IdempotentMutation,
        Secret,
        OPERATOR_AUDIENCE
    ),
    declaration!(
        RevokeCapability,
        "riffdb.v1",
        "RevokeCapabilityRequest",
        "RevokeCapabilityResponse",
        RevokeCapability,
        IdempotentMutation,
        OperatorProtected,
        OPERATOR_AUDIENCE
    ),
    declaration!(
        ListPendingOutboxDeliveries,
        "riffdb.v1",
        "ListPendingOutboxDeliveriesRequest",
        "ListPendingOutboxDeliveriesResponse",
        InspectOutbox,
        ReadOnly,
        OperatorProtected,
        OPERATOR_AUDIENCE
    ),
    declaration!(
        DiscoverCommandTools,
        "riffdb.v1",
        "DiscoverCommandToolsRequest",
        "DiscoverCommandToolsResponse",
        ReadContract,
        ReadOnly,
        ServiceRedacted,
        AGENT_AUDIENCE
    ),
    declaration!(
        DiscoverResources,
        "riffdb.v1",
        "DiscoverResourcesRequest",
        "DiscoverResourcesResponse",
        ReadContract,
        ReadOnly,
        ServiceRedacted,
        AGENT_AUDIENCE
    ),
    declaration_with_types!(
        DescribeContract,
        "riffdb.app.v1",
        "DescribeContractRequest",
        "DescribeContractResponse",
        "riffdb_service::SymbolicContractSelector",
        "riffdb_service::DescribeSymbolicContractResult",
        ReadContract,
        ReadOnly,
        ServiceRedacted,
        AGENT_AUDIENCE
    ),
    declaration_with_types!(
        CheckQuery,
        "riffdb.app.v1",
        "CheckQueryRequest",
        "CheckQueryResponse",
        "riffdb_service::CompileSymbolicQueryRequest",
        "riffdb_service::CheckSymbolicQueryResult",
        CheckAdHocQuery,
        ReadOnly,
        ServiceRedacted,
        APP_AUDIENCE
    ),
    declaration_with_types!(
        ExplainQuery,
        "riffdb.app.v1",
        "ExplainQueryRequest",
        "ExplainQueryResponse",
        "riffdb_service::CompileSymbolicQueryRequest",
        "riffdb_service::ExplainSymbolicQueryResult",
        ExplainAdHocQuery,
        ReadOnly,
        ServiceRedacted,
        APP_AUDIENCE
    ),
    declaration_with_types!(
        ExecuteQuery,
        "riffdb.app.v1",
        "ExecuteQueryRequest",
        "ExecuteQueryResponse",
        "riffdb_service::ExecuteSymbolicQueryRequest",
        "riffdb_service::ExecuteSymbolicQueryResult",
        ExecuteAdHocQuery,
        ReadOnly,
        ServiceRedacted,
        APP_AUDIENCE
    ),
    declaration!(
        DeployQueryModule,
        "riffdb.app.v1",
        "DeployQueryModuleRequest",
        "DeployQueryModuleResponse",
        DeployContract,
        IdempotentMutation,
        OperatorProtected,
        OPERATOR_AUDIENCE
    ),
    declaration_with_types_and_bounds!(
        ApplyContractMigration,
        "riffdb.v1",
        "ApplyContractMigrationRequest",
        "ApplyContractMigrationResponse",
        "riffdb_service::ApplyContractMigrationRequest",
        "riffdb_service::ContractMigrationStartResult",
        CONTRACT_MIGRATION_EXCHANGE_BOUNDS,
        MigrateContract,
        IdempotentMutation,
        OperatorProtected,
        OPERATOR_AUDIENCE
    ),
    declaration!(
        DescribeEvent,
        "riffdb.v1",
        "DescribeEventRequest",
        "DescribeEventResponse",
        ConsumeEventStream,
        ReadOnly,
        ServiceRedacted,
        APP_AUDIENCE
    ),
    declaration!(
        ReplayEvents,
        "riffdb.v1",
        "ReplayEventsRequest",
        "ReplayEventsResponse",
        ConsumeEventStream,
        ReadOnly,
        ServiceRedacted,
        APP_AUDIENCE
    ),
    declaration!(
        TailEvents,
        "riffdb.v1",
        "TailEventsRequest",
        "TailEventsResponse",
        ConsumeEventStream,
        StreamingRead,
        ServiceRedacted,
        APP_AUDIENCE
    ),
    declaration!(
        ExecuteProjectedQuery,
        "riffdb.app.v1",
        "ExecuteProjectedQueryRequest",
        "ExecuteProjectedQueryResponse",
        ExecuteNamedQuery,
        ReadOnly,
        ServiceRedacted,
        APP_AUDIENCE
    ),
    declaration!(
        DeployReactiveModule,
        "riffdb.app.v1",
        "DeployReactiveModuleRequest",
        "DeployReactiveModuleResponse",
        DeployContract,
        IdempotentMutation,
        OperatorProtected,
        OPERATOR_AUDIENCE
    ),
    declaration!(
        ConsumeEventStream,
        "riffdb.v1",
        "ConsumeEventStreamRequest",
        "ConsumeEventStreamResponse",
        ConsumeEventStream,
        StreamingRead,
        ServiceRedacted,
        APP_AUDIENCE
    ),
    declaration_with_types!(
        AcknowledgeEventStream,
        "riffdb.v1",
        "AcknowledgeEventStreamRequest",
        "EventConsumerMutationResponse",
        "riffdb_service::EventConsumerLeaseSelection",
        "riffdb_service::EventConsumerMutationResult",
        ConsumeEventStream,
        IdempotentMutation,
        ServiceRedacted,
        APP_AUDIENCE
    ),
    declaration_with_types!(
        NegativeAcknowledgeEventStream,
        "riffdb.v1",
        "NegativeAcknowledgeEventStreamRequest",
        "EventConsumerMutationResponse",
        "riffdb_service::NegativeAcknowledgeEventStreamRequest",
        "riffdb_service::EventConsumerMutationResult",
        ConsumeEventStream,
        IdempotentMutation,
        ServiceRedacted,
        APP_AUDIENCE
    ),
    declaration_with_types!(
        SeekEventStreamConsumer,
        "riffdb.v1",
        "SeekEventStreamConsumerRequest",
        "EventConsumerMutationResponse",
        "riffdb_service::SeekEventStreamConsumerRequest",
        "riffdb_service::EventConsumerMutationResult",
        SeekEventStreamConsumer,
        IdempotentMutation,
        OperatorProtected,
        OPERATOR_AUDIENCE
    ),
    declaration_with_types!(
        RetireEventStreamConsumer,
        "riffdb.v1",
        "RetireEventStreamConsumerRequest",
        "EventConsumerMutationResponse",
        "riffdb_service::EventConsumerSelection",
        "riffdb_service::EventConsumerMutationResult",
        SeekEventStreamConsumer,
        IdempotentMutation,
        OperatorProtected,
        OPERATOR_AUDIENCE
    ),
    declaration_with_types!(
        GetEventStreamConsumerStatus,
        "riffdb.v1",
        "GetEventStreamConsumerStatusRequest",
        "GetEventStreamConsumerStatusResponse",
        "riffdb_service::EventConsumerSelection",
        "core::option::Option<riffdb_service::EventConsumerPublicStatus>",
        ConsumeEventStream,
        ReadOnly,
        ServiceRedacted,
        APP_AUDIENCE
    ),
    declaration_with_types!(
        WatchNamedQuery,
        "riffdb.v1",
        "WatchNamedQueryRequest",
        "LiveQueryUpdate",
        "riffdb_service::WatchLiveNamedQueryRequest",
        "riffdb_service::WatchLiveNamedQueryResult",
        WatchNamedQuery,
        StreamingRead,
        ServiceRedacted,
        APP_AUDIENCE
    ),
    declaration!(
        ConsumeContextualSubscription,
        "riffdb.v1",
        "ConsumeContextualSubscriptionRequest",
        "ConsumeContextualSubscriptionResponse",
        ConsumeContextualSubscription,
        StreamingRead,
        ServiceRedacted,
        APP_AUDIENCE
    ),
    declaration_with_types!(
        AcknowledgeContextualSubscription,
        "riffdb.v1",
        "AcknowledgeContextualSubscriptionRequest",
        "EventConsumerMutationResponse",
        "riffdb_service::EventConsumerLeaseSelection",
        "riffdb_service::EventConsumerMutationResult",
        ConsumeContextualSubscription,
        IdempotentMutation,
        ServiceRedacted,
        APP_AUDIENCE
    ),
    declaration_with_types!(
        NegativeAcknowledgeContextualSubscription,
        "riffdb.v1",
        "NegativeAcknowledgeContextualSubscriptionRequest",
        "EventConsumerMutationResponse",
        "riffdb_service::NegativeAcknowledgeEventStreamRequest",
        "riffdb_service::EventConsumerMutationResult",
        ConsumeContextualSubscription,
        IdempotentMutation,
        ServiceRedacted,
        APP_AUDIENCE
    ),
    declaration_with_types!(
        GetContextualSubscriptionStatus,
        "riffdb.v1",
        "GetContextualSubscriptionStatusRequest",
        "GetEventStreamConsumerStatusResponse",
        "riffdb_service::EventConsumerSelection",
        "core::option::Option<riffdb_service::EventConsumerPublicStatus>",
        ConsumeContextualSubscription,
        ReadOnly,
        ServiceRedacted,
        APP_AUDIENCE
    ),
    declaration_with_types_and_bounds!(
        ExecuteContextualReaction,
        "riffdb.v1",
        "ExecuteContextualReactionRequest",
        "ExecuteCommandResponse",
        "riffdb_service::ExecuteContextualReactionRequest",
        "riffdb_service::ExecuteCommandResult",
        COMMAND_EXCHANGE_BOUNDS,
        ConsumeContextualSubscription,
        IdempotentMutation,
        ServiceRedacted,
        APP_AUDIENCE
    ),
    declaration_with_types!(
        GetReactiveWakeup,
        "riffdb.v1",
        "GetReactiveWakeupRequest",
        "GetReactiveWakeupResponse",
        "()",
        "riffdb_service::GetReactiveWakeupResult",
        WatchNamedQuery,
        ReadOnly,
        ServiceRedacted,
        APP_AUDIENCE
    ),
    declaration_with_types_and_bounds!(
        StartApplicationInstallation,
        "riffdb.v1",
        "StartApplicationInstallationRequest",
        "StartApplicationInstallationResponse",
        "riffdb_service::StartApplicationInstallationRequest",
        "riffdb_service::ApplicationInstallationOperationResult",
        APPLICATION_BUNDLE_EXCHANGE_BOUNDS,
        InstallApplication,
        IdempotentMutation,
        OperatorProtected,
        OPERATOR_AUDIENCE
    ),
    declaration!(
        GetApplicationInstallation,
        "riffdb.v1",
        "GetApplicationInstallationRequest",
        "GetApplicationInstallationResponse",
        InstallApplication,
        ReadOnly,
        OperatorProtected,
        OPERATOR_AUDIENCE
    ),
    declaration_with_types!(
        StartApplicationExport,
        "riffdb.v1",
        "StartApplicationExportRequest",
        "StartApplicationExportResponse",
        "riffdb_service::StartApplicationExportRequest",
        "riffdb_service::ApplicationExportStartResultV1",
        InstallApplication,
        IdempotentMutation,
        OperatorProtected,
        OPERATOR_AUDIENCE
    ),
    declaration_with_types!(
        GetApplicationExportPage,
        "riffdb.v1",
        "GetApplicationExportPageRequest",
        "GetApplicationExportPageResponse",
        "riffdb_service::GetApplicationExportPageRequest",
        "riffdb_service::ApplicationExportPageV1",
        InstallApplication,
        ReadOnly,
        OperatorProtected,
        OPERATOR_AUDIENCE
    ),
    declaration_with_types!(
        GetApplicationExport,
        "riffdb.v1",
        "GetApplicationExportRequest",
        "GetApplicationExportResponse",
        "riffdb_service::ApplicationExportOperationRequest",
        "riffdb_service::GetApplicationExportResultV1",
        InstallApplication,
        ReadOnly,
        OperatorProtected,
        OPERATOR_AUDIENCE
    ),
    declaration_with_types!(
        CancelApplicationExport,
        "riffdb.v1",
        "CancelApplicationExportRequest",
        "CancelApplicationExportResponse",
        "riffdb_service::ApplicationExportOperationRequest",
        "riffdb_service::GetApplicationExportResultV1",
        InstallApplication,
        IdempotentMutation,
        OperatorProtected,
        OPERATOR_AUDIENCE
    ),
    declaration_with_types!(
        StartApplicationReimport,
        "riffdb.v1",
        "StartApplicationReimportRequest",
        "StartApplicationReimportResponse",
        "riffdb_service::StartApplicationReimportRequestV1",
        "riffdb_service::ApplicationReimportOperationResultV1",
        InstallApplication,
        IdempotentMutation,
        OperatorProtected,
        OPERATOR_AUDIENCE
    ),
    declaration_with_types_and_bounds!(
        ApplyApplicationReimportPage,
        "riffdb.v1",
        "ApplyApplicationReimportPageRequest",
        "ApplyApplicationReimportPageResponse",
        "riffdb_service::ApplyApplicationReimportPageRequestV1",
        "riffdb_service::ApplicationReimportOperationResultV1",
        APPLICATION_BUNDLE_EXCHANGE_BOUNDS,
        InstallApplication,
        IdempotentMutation,
        OperatorProtected,
        OPERATOR_AUDIENCE
    ),
    declaration_with_types!(
        GetApplicationReimport,
        "riffdb.v1",
        "GetApplicationReimportRequest",
        "GetApplicationReimportResponse",
        "riffdb_service::ApplicationReimportOperationRequestV1",
        "riffdb_service::GetApplicationReimportResultV1",
        InstallApplication,
        ReadOnly,
        OperatorProtected,
        OPERATOR_AUDIENCE
    ),
    declaration_with_types!(
        CancelApplicationReimport,
        "riffdb.v1",
        "CancelApplicationReimportRequest",
        "CancelApplicationReimportResponse",
        "riffdb_service::ApplicationReimportOperationRequestV1",
        "riffdb_service::GetApplicationReimportResultV1",
        InstallApplication,
        IdempotentMutation,
        OperatorProtected,
        OPERATOR_AUDIENCE
    ),
    declaration!(
        InspectVectorState,
        "riffdb.app.v1",
        "InspectVectorStateRequest",
        "InspectVectorStateResponse",
        InspectVectorState,
        ReadOnly,
        OperatorProtected,
        OPERATOR_AUDIENCE
    ),
];

/// Looks up one declaration by its stable semantic operation.
#[must_use]
pub fn declaration(operation: ServiceOperationV1) -> &'static OperationDeclaration {
    &OPERATIONS[usize::from(operation.tag() - 1)]
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn registry_inventory_is_exact_complete_and_bounded() {
        assert_eq!(OPERATIONS.len(), ServiceOperationV1::ALL.len());
        let operations = OPERATIONS
            .iter()
            .map(|entry| entry.operation)
            .collect::<BTreeSet<_>>();
        assert_eq!(operations.len(), ServiceOperationV1::ALL.len());
        for operation in ServiceOperationV1::ALL {
            let entry = declaration(operation);
            assert_eq!(entry.operation, operation);
            assert!(!entry.proto_request.is_empty());
            assert!(!entry.proto_response.is_empty());
            assert!(!entry.dto_request.is_empty());
            assert!(!entry.dto_result.is_empty());
            assert!(!entry.bounds.is_empty());
            assert!(!entry.ingress.is_empty());
            assert!(!entry.audiences.is_empty());
            assert!(!entry.field_map.is_empty());
            assert!(entry.bounds.iter().all(|bound| bound.maximum > 0));
            assert!(
                entry
                    .field_map
                    .iter()
                    .all(|field| !field.proto_path.contains('*'))
            );
        }
    }

    #[test]
    fn registry_is_a_leaf_with_closed_bounded_mapping_values() {
        assert_eq!(OPERATION_REGISTRY_VERSION, 1);
        assert!(OPERATIONS.iter().all(|entry| entry.ingress == ALL_INGRESS));
        for entry in OPERATIONS {
            let bound_targets = entry
                .bounds
                .iter()
                .map(|bound| bound.target)
                .collect::<BTreeSet<_>>();
            assert_eq!(bound_targets.len(), entry.bounds.len());
            assert_eq!(
                bound_targets,
                BTreeSet::from([
                    BoundTarget::EncodedRequestBytes,
                    BoundTarget::EncodedResponseBytes,
                ])
            );
            let paths = entry
                .field_map
                .iter()
                .map(|mapping| mapping.proto_path)
                .collect::<BTreeSet<_>>();
            assert_eq!(paths.len(), entry.field_map.len());
        }
    }
}
