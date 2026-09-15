#![forbid(unsafe_code)]
#![cfg_attr(
    not(test),
    deny(
        clippy::expect_used,
        clippy::panic,
        clippy::unreachable,
        clippy::unwrap_used
    )
)]

//! API-neutral application-service contracts and orchestration.

mod administration_operations;
mod application;
mod application_error;
mod audit;
mod columnar_notification;
mod command_census;
pub use command_census::{COMMAND_SERVICE_STAGE_LABELS_V1, command_service_stage_census_v1};
mod command_operations;
mod commit_operations;
mod consumer_operations;
mod context;
mod contextual;
mod contract_operations;
mod cursor;
mod dto;
mod event_operations;
mod export;
mod export_operations;
mod failure;
mod installation;
mod installation_operations;
mod live_query;
mod maintenance_operations;
mod migration_operations;
mod orchestration;
mod ports;
mod projected_query;
mod query_discovery_operations;
mod read_retry;
mod reimport;
mod reimport_operations;
mod response;
mod service;
mod submitted;
mod symbolic_query;
mod wait;

pub use application::*;
pub use application_error::*;
pub use audit::*;
pub use columnar_notification::*;
pub use consumer_operations::*;
pub use context::*;
pub use contextual::*;
pub use cursor::*;
pub use dto::*;
pub use event_operations::*;
pub use export::*;
pub use failure::*;
pub use installation::*;
pub use live_query::*;
pub use maintenance_operations::{
    RecoveryOfflineMaintenanceService, RestoreRetryOfflineMaintenanceService,
};
pub use ports::*;
pub use projected_query::*;
pub use reimport::*;
pub use response::*;
mod replication;
pub use replication::*;
pub use riffdb_query_executor::{QueryParameters, QueryResultValue, QueryRow};
pub use service::*;
pub use submitted::*;
pub use symbolic_query::*;
