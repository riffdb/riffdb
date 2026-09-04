#![forbid(unsafe_code)]

//! Versioned public and durable Protobuf boundaries for RiffDB.

extern crate self as riffdb_proto;

mod app_public_message;
mod application_error;
#[cfg(feature = "client")]
mod client_codec;
mod command;
pub mod durable;
mod durable_wire;
pub mod envelope;
mod public_error;
mod public_message;
mod value;
mod wire;

/// Checked-in Prost messages for the `riffdb.v1` public package.
#[allow(missing_docs, clippy::large_enum_variant)]
pub mod v1 {
    include!("generated/riffdb.v1.rs");
}

/// Checked-in Prost messages for the `riffdb.app.v1` application package.
pub mod app {
    /// Symbolic application API v1.
    #[allow(missing_docs, clippy::large_enum_variant)]
    pub mod v1 {
        include!("generated/riffdb.app.v1.rs");
    }
}

/// Checked-in Prost messages for RiffDB durable packages.
pub mod storage {
    /// Checked-in Prost messages for the `riffdb.storage.v1` package.
    #[allow(missing_docs)]
    pub mod v1 {
        include!("generated/riffdb.storage.v1.rs");
    }
}

pub use application_error::*;
pub use command::*;
pub use public_error::*;
pub use public_message::*;
pub use value::*;

/// Exact ASCII metadata key selecting one database before authentication.
pub const DATABASE_METADATA_KEY: &str = "riffdb-database";

/// Generated client-only Tonic bindings for `riffdb.v1`.
#[cfg(feature = "client")]
#[allow(missing_docs, clippy::large_enum_variant)]
pub mod generated {
    include!(concat!(env!("OUT_DIR"), "/riffdb.v1.rs"));
}

/// Generated client-only Tonic bindings for `riffdb.app.v1`.
#[cfg(feature = "client")]
#[allow(missing_docs, clippy::large_enum_variant)]
pub mod generated_app {
    include!(concat!(env!("OUT_DIR"), "/riffdb.app.v1.rs"));
}

/// Source-info-stripped, path-sorted descriptors for all current production schemas.
pub const PRODUCTION_FILE_DESCRIPTOR_SET: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/descriptors/riffdb-v1-descriptor-set.bin"
));

/// Source-info-stripped descriptors for the nine durable v1 schema sources.
pub const STORAGE_FILE_DESCRIPTOR_SET: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/descriptors/riffdb-storage-v1-descriptor-set.bin"
));
