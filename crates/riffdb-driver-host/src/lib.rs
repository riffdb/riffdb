//! Long-lived, application-only host for RiffDB target-language drivers.

#![forbid(unsafe_code)]

mod catalog;
mod config;
mod host;
mod operator_config;
mod operator_host;
mod operator_protocol;
mod operator_socket;
mod protocol;
mod protocol_core;
mod socket;

pub use catalog::{
    ApplicationCatalog, CatalogError, OperationKind, OperationSpec, ReactionSpec, ReactiveKind,
    VectorInspectionKind, VectorInspectionSpec,
};
pub use config::{DriverRuntime, DriverRuntimeError};
pub use host::{
    DRIVER_ERROR_REGISTRY_HASH, DRIVER_IDENTITY, DRIVER_VALUE_REGISTRY_HASH, DriverHost,
    DriverHostError, DriverPool,
};
pub use operator_config::{OperatorDriverRuntime, OperatorRuntimeError};
pub use operator_host::OperatorDriverHost;
pub use operator_protocol::{
    MAX_OPERATOR_DRIVER_FRAME_BYTES, OPERATOR_DRIVER_PROTOCOL_VERSION, OperatorDriverRequest,
    OperatorDriverResponse, OperatorFrameCodec, OperatorProtocolError, OperatorReimportOperation,
};
pub use operator_socket::{OperatorDriverSocket, OperatorSocketError};
pub use protocol::{
    DRIVER_PROTOCOL_VERSION, DriverBatchItem, DriverBatchOutcome, DriverDecimal, DriverMoney,
    DriverPackedColumn, DriverQueryConsistency, DriverRequest, DriverResponse, DriverTimestamp,
    DriverValue, DriverVector, FrameCodec, InvokeOptions, MAX_DRIVER_FRAME_BYTES, ProtocolError,
};
pub use protocol_core::{
    BindingError, InProcessCommand, InProcessQuery, ProtocolCoreError, QueryDispatchResult,
    application_value_to_python_json, classify_application_client_error, classify_client_error,
    dispatch_command, dispatch_named_query, lower_driver_value, normalize_python_value,
    normalize_python_value_request, parse_in_process_command, parse_in_process_query,
    raise_driver_value,
};
pub use socket::{DriverSocket, SocketError};

/// Transport-free in-process types retained for the self-contained Python
/// wheel. Bindings consume them through this protocol-core module rather than
/// depending on the lower client crate directly.
#[doc(hidden)]
pub mod in_process {
    pub use riffdb_client_rust::{
        ApplicationCardinality, ApplicationClientError, ApplicationCommand,
        ApplicationCommandResult, ApplicationContextualBatch, ApplicationContextualReaction,
        ApplicationContract, ApplicationError, ApplicationErrorCode, ApplicationEventBatch,
        ApplicationEventCheckpoint, ApplicationEventConsumer, ApplicationEventConsumerPublicStatus,
        ApplicationEventConsumerStatus, ApplicationEventId, ApplicationEventLeaseEvidence,
        ApplicationEventMutationResult, ApplicationEventProgressCursor,
        ApplicationEventPullDisposition, ApplicationLiveQueryUpdate, ApplicationReactiveOperation,
        ApplicationRecord, ApplicationValue, BearerCredential, CallMetadata, ClientError,
        DatabaseAlias, DetailsFreeStatus, EventConsumerOptions, LiveQueryCursor,
        LiveQueryPatchOperation, NamedQueryResult, PublicError, StableApplicationClient,
        TraceParent, VectorStateInspection, VectorStateInspectionKind, VectorStateInspectionResult,
        app_v1, load_protected_bearer_credential, raise_query_result, raise_value,
    };
}
