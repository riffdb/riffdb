//! Long-lived, application-only host for RiffDB target-language drivers.

#![forbid(unsafe_code)]

mod catalog;
mod config;
mod host;
mod operator_host;
mod operator_protocol;
mod protocol;
mod socket;

pub use catalog::{
    ApplicationCatalog, CatalogError, OperationKind, OperationSpec, ReactionSpec, ReactiveKind,
};
pub use config::{DriverRuntime, DriverRuntimeError};
pub use host::{
    DRIVER_ERROR_REGISTRY_HASH, DRIVER_IDENTITY, DRIVER_VALUE_REGISTRY_HASH, DriverHost,
    DriverHostError, DriverPool,
};
pub use operator_host::OperatorDriverHost;
pub use operator_protocol::{
    MAX_OPERATOR_DRIVER_FRAME_BYTES, OPERATOR_DRIVER_PROTOCOL_VERSION, OperatorDriverRequest,
    OperatorDriverResponse, OperatorFrameCodec, OperatorProtocolError, OperatorReimportOperation,
};
pub use protocol::{
    DRIVER_PROTOCOL_VERSION, DriverBatchItem, DriverBatchOutcome, DriverDecimal, DriverMoney,
    DriverRequest, DriverResponse, DriverTimestamp, DriverValue, FrameCodec, InvokeOptions,
    MAX_DRIVER_FRAME_BYTES, ProtocolError,
};
pub use socket::{DriverSocket, SocketError};
