#![forbid(unsafe_code)]

//! Engine-neutral semantic storage contracts for RiffDB.

pub use riffdb_types::IndexEpochPosition;

pub mod admission;
pub mod audit;
pub mod authoritative;
pub mod backup;
pub mod bounds;
pub mod capability;
pub mod catalog;
pub mod command;
pub mod command_txn;
pub mod error;
pub mod identity;
pub mod integrity;
pub mod outbox;
pub mod plan;
pub mod projection;
pub mod projection_schema;
pub mod proto_codec;
pub mod records;
pub mod schema_binding;
pub mod sequence;
pub mod snapshot;
pub mod startup;

pub use admission::*;
pub use audit::*;
pub use authoritative::*;
pub use backup::*;
pub use bounds::*;
pub use capability::*;
pub use catalog::*;
pub use command::*;
pub use command_txn::*;
pub use error::*;
pub use identity::*;
pub use integrity::*;
pub use outbox::*;
pub use plan::*;
pub use projection::*;
pub use projection_schema::*;
pub use proto_codec::*;
pub use records::*;
pub use schema_binding::*;
pub use sequence::*;
pub use snapshot::*;
pub use startup::*;
