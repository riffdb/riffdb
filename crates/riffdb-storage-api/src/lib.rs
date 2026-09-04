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

//! Engine-neutral semantic storage contracts for RiffDB.

pub use riffdb_types::IndexEpochPosition;

pub mod admission;
pub mod application_export;
pub mod application_installation;
pub mod audit;
pub mod authoritative;
pub mod backup;
pub mod bounds;
pub mod capability;
pub mod catalog;
pub mod changelog;
pub mod changelog_v2;
pub mod command;
pub mod command_capsule;
pub mod command_segment;
pub mod command_txn;
pub mod composite_view;
pub mod consumer;
pub mod contract_migration;
pub mod durable_format;
pub mod entity_transition;
pub mod error;
pub mod event_policy_admission;
pub mod event_route;
pub mod identity;
pub mod integrity;
pub mod migration;
pub mod outbox;
pub mod owned_snapshot;
pub mod plan;
pub mod projection;
pub mod projection_schema;
pub mod proto_codec;
pub mod query_module;
pub mod reactive_module;
pub mod records;
pub mod retention;
pub mod schema_binding;
pub mod sequence;
pub mod snapshot;
pub mod startup;
pub mod validated_prefix_checkpoint;
pub mod vector_evidence;

pub use admission::*;
pub use application_export::*;
pub use application_installation::*;
pub use audit::*;
pub use authoritative::*;
pub use backup::*;
pub use bounds::*;
pub use capability::*;
pub use catalog::*;
pub use changelog::*;
pub use changelog_v2::*;
pub use command::*;
pub use command_capsule::*;
pub use command_segment::*;
pub use command_txn::*;
pub use composite_view::*;
pub use consumer::*;
pub use contract_migration::*;
pub use durable_format::*;
pub use entity_transition::*;
pub use error::*;
pub use event_policy_admission::*;
pub use event_route::*;
pub use identity::*;
pub use integrity::*;
pub use migration::*;
pub use outbox::*;
pub use owned_snapshot::*;
pub use plan::*;
pub use projection::*;
pub use projection_schema::*;
pub use proto_codec::*;
pub use query_module::*;
pub use reactive_module::*;
pub use records::*;
pub use retention::*;
pub use schema_binding::*;
pub use sequence::*;
pub use snapshot::*;
pub use startup::*;
pub use validated_prefix_checkpoint::*;
pub use vector_evidence::*;
