#![forbid(unsafe_code)]

//! Versioned public and durable Protobuf boundaries for RiffDB.

mod command;
pub mod durable;
mod durable_wire;
pub mod envelope;
mod public_error;
mod value;
mod wire;

/// Checked-in Prost messages for the `riffdb.v1` public package.
#[allow(missing_docs)]
pub mod v1 {
    include!("generated/riffdb.v1.rs");
}

/// Checked-in Prost messages for RiffDB durable packages.
pub mod storage {
    /// Checked-in Prost messages for the `riffdb.storage.v1` package.
    #[allow(missing_docs)]
    pub mod v1 {
        include!("generated/riffdb.storage.v1.rs");
    }
}

pub use command::*;
pub use public_error::*;
pub use value::*;

/// Source-info-stripped, path-sorted descriptors for all current production schemas.
pub const PRODUCTION_FILE_DESCRIPTOR_SET: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/descriptors/riffdb-v1-descriptor-set.bin"
));

/// Source-info-stripped descriptors for the nine durable v1 schema sources.
pub const STORAGE_FILE_DESCRIPTOR_SET: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/descriptors/riffdb-storage-v1-descriptor-set.bin"
));
