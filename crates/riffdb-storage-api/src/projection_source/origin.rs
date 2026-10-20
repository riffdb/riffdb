//! Fixed structural origin commitments; lookup fields confer no native authority.
use crate::{NativeCatalogOriginReferenceV1, StorageValueError};
use riffdb_types::{ContentDigest, ContentHasher, DatabaseId, HashDomain};

/// Fixed V1 preimage: version BE32, allocation database16, allocation incarnation
/// BE64, original native origin administration BE64, canonical envelope digest32.
/// The immutable descriptor separately binds captured History/F/prior source.
#[doc(hidden)]
pub fn projection_source_origin_preimage_v1(
    database: DatabaseId,
    allocation_incarnation: u64,
    origin: NativeCatalogOriginReferenceV1,
) -> Result<[u8; 68], StorageValueError> {
    if allocation_incarnation == 0 {
        return Err(StorageValueError::InvalidShape);
    }
    let mut bytes = [0; 68];
    bytes[..4].copy_from_slice(&1_u32.to_be_bytes());
    bytes[4..20].copy_from_slice(database.as_bytes());
    bytes[20..28].copy_from_slice(&allocation_incarnation.to_be_bytes());
    bytes[28..36].copy_from_slice(&origin.administration_sequence().get().to_be_bytes());
    bytes[36..].copy_from_slice(origin.digest().as_bytes());
    Ok(bytes)
}

/// Hashes the complete fixed preimage using the existing SourceOrigin V1 domain.
/// Native custody must first check the actual origin, engine and transition.
/// This pure helper accepts structural values and cannot certify those facts.
/// Inheritance preserves the original descriptor/commitment byte-for-byte;
/// callers must never recompute an inherited origin with a successor incarnation.
#[doc(hidden)]
pub fn projection_source_origin_commitment_v1(
    database: DatabaseId,
    allocation_incarnation: u64,
    origin: NativeCatalogOriginReferenceV1,
) -> Result<ContentDigest, StorageValueError> {
    let preimage = projection_source_origin_preimage_v1(database, allocation_incarnation, origin)?;
    let mut hasher = ContentHasher::new(HashDomain::ProjectionSourceOrigin, 68);
    hasher
        .update(&preimage)
        .map_err(|_| StorageValueError::InvalidShape)?;
    hasher.finish().map_err(|_| StorageValueError::InvalidShape)
}

#[cfg(test)]
mod tests;
