//! Compatibility reexports of the single structural Source row hash owner.
//! This primitive grants no evaluation, custody or publication authority.
#[cfg(test)]
use riffdb_storage_api::{
    MAX_PROJECTION_SOURCE_OBJECT_BYTES, MAX_PROJECTION_SOURCE_ROWS, ProjectionSourceRowV1,
    StorageValueError,
};
pub use riffdb_storage_api::{
    ProjectionSourceRowRootV1, projection_source_row_preimage_bytes_v1,
    projection_source_row_scratch_bytes_v1,
};
#[cfg(test)]
use riffdb_types::{
    CommitSequence, ContentDigest, HashDomain, ProjectionGeneration, ProjectionGroupKey,
    ProjectionSourceLocatorV1,
};
#[cfg(test)]
#[path = "source_row_root_tests.rs"]
mod tests;
