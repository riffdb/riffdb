//! Streaming structural row commitments. A digest never proves evaluation,
//! pinned source closure, staging custody or coordinator publication.

use riffdb_storage_api::{
    MAX_PROJECTION_SOURCE_OBJECT_BYTES, MAX_PROJECTION_SOURCE_ROWS, ProjectionSourceRowV1,
    StorageValueError,
};
use riffdb_types::{
    CommitSequence, ContentDigest, ContentHasher, HashDomain, MAX_KEY_BYTES, ProjectionGeneration,
    ProjectionGroupKey, ProjectionIdentity, ProjectionSourceLocatorV1,
    canonical_record_encoded_len, canonical_value_encoded_len, encode_canonical_record_into,
    encode_canonical_value_into,
};

/// Measures one semantic row frame without allocating. This excludes the native
/// locator/generation/key envelope. The descriptor separately counts the exact
/// stored key/value charge, which can vary with the allocation ordinal.
pub fn projection_source_row_preimage_bytes_v1(
    row: &ProjectionSourceRowV1,
) -> Result<u64, StorageValueError> {
    // Row length, component count, measure length and last-changed sequence.
    let mut bytes = 8_u64 + 4 + 8 + 8;
    for value in row.group_values() {
        bytes = add(bytes, add(8, length(canonical_value_encoded_len(value))?)?)?;
    }
    add(bytes, length(canonical_record_encoded_len(row.measures()))?)
}

/// Exact minimum reusable encoding capacity. The owner must reserve its actual
/// allocation charge before allocating this scratch; this does not admit it.
pub fn projection_source_row_scratch_bytes_v1(
    row: &ProjectionSourceRowV1,
) -> Result<usize, StorageValueError> {
    let mut bytes = canonical_record_encoded_len(row.measures())
        .map_err(|_| StorageValueError::InvalidShape)?;
    for value in row.group_values() {
        bytes = bytes
            .max(canonical_value_encoded_len(value).map_err(|_| StorageValueError::InvalidShape)?);
    }
    Ok(bytes)
}

/// One move-only exact-length row hash owner. The worker must charge this fixed
/// state, its borrowed input ownership and reusable scratch before construction.
/// No population-sized collection or heap allocation is made by this owner.
/// The first linear staging pass supplies count and semantic bytes; the second
/// supplies the same immutable rows. Neither pass certifies their authority.
pub struct ProjectionSourceRowRootV1<'identity> {
    identity: &'identity ProjectionIdentity,
    locator: Option<ProjectionSourceLocatorV1>,
    frontier: CommitSequence,
    hasher: ContentHasher,
    expected_rows: u32,
    expected_bytes: u64,
    rows: u32,
    bytes: u64,
    last_key: [u8; MAX_KEY_BYTES],
    last_key_len: usize,
    poisoned: bool,
}

impl<'identity> ProjectionSourceRowRootV1<'identity> {
    /// Starts a structural hash over the exact independently measured stream.
    /// The optional locator is required for nonempty streams and checked on
    /// every row, but is deliberately excluded from the semantic row hash.
    pub fn new(
        identity: &'identity ProjectionIdentity,
        locator: Option<ProjectionSourceLocatorV1>,
        frontier: CommitSequence,
        rows: u32,
        semantic_bytes: u64,
    ) -> Result<Self, StorageValueError> {
        if rows > MAX_PROJECTION_SOURCE_ROWS || semantic_bytes > MAX_PROJECTION_SOURCE_OBJECT_BYTES
        {
            return Err(StorageValueError::LimitExceeded);
        }
        if (rows == 0) != (semantic_bytes == 0)
            || (rows != 0 && locator.is_none())
            || semantic_bytes < u64::from(rows) * 28
        {
            return Err(StorageValueError::InvalidShape);
        }
        let identity_bytes = add(40, identity.contract_lineage().as_bytes().len() as u64)?;
        let mut hasher = ContentHasher::new(
            HashDomain::ProjectionSourceRows,
            add(add(4 + 4 + 8 + 4 + 8, identity_bytes)?, semantic_bytes)?,
        );
        update(&mut hasher, &1_u32.to_be_bytes())?;
        update(
            &mut hasher,
            &u32::try_from(identity_bytes)
                .map_err(|_| StorageValueError::SizeOverflow)?
                .to_be_bytes(),
        )?;
        // Stream the existing canonical identity bytes without allocating a Vec.
        let lineage = identity.contract_lineage().as_bytes();
        update(
            &mut hasher,
            &u32::try_from(lineage.len())
                .map_err(|_| StorageValueError::SizeOverflow)?
                .to_be_bytes(),
        )?;
        update(&mut hasher, lineage)?;
        update(&mut hasher, &identity.projection_id().to_be_bytes())?;
        update(&mut hasher, identity.plan_hash().as_bytes())?;
        update(&mut hasher, &frontier.to_be_bytes())?;
        Ok(Self {
            identity,
            locator,
            frontier,
            hasher,
            expected_rows: rows,
            expected_bytes: semantic_bytes,
            rows: 0,
            bytes: 0,
            last_key: [0; MAX_KEY_BYTES],
            last_key_len: 0,
            poisoned: false,
        })
    }

    /// Appends one row in canonical group order using already admitted scratch.
    /// `order_key` is only an ordering witness and must use the fixed first
    /// generation with the exact row components. Its bytes are never hashed.
    /// Any error poisons this owner, including a refusal before hashing starts.
    pub fn append(
        &mut self,
        order_key: &ProjectionGroupKey,
        row: &ProjectionSourceRowV1,
        scratch: &mut Vec<u8>,
    ) -> Result<(), StorageValueError> {
        if self.poisoned {
            return Err(StorageValueError::InvalidShape);
        }
        let result = self.append_inner(order_key, row, scratch);
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    fn append_inner(
        &mut self,
        order_key: &ProjectionGroupKey,
        row: &ProjectionSourceRowV1,
        scratch: &mut Vec<u8>,
    ) -> Result<(), StorageValueError> {
        if row.identity() != self.identity
            || Some(row.locator()) != self.locator
            || order_key.identity() != self.identity
            || order_key.components() != row.group_values()
            || order_key.generation() != ProjectionGeneration::first()
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        if row.last_changed_sequence() > self.frontier {
            return Err(StorageValueError::InvalidShape);
        }
        let key = order_key.as_bytes();
        if key.len() > MAX_KEY_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        if self.rows != 0 && self.last_key[..self.last_key_len] >= *key {
            return Err(StorageValueError::NonCanonicalOrder);
        }
        let row_bytes = projection_source_row_preimage_bytes_v1(row)?;
        let bytes = add(self.bytes, row_bytes)?;
        if self.rows >= self.expected_rows || bytes > self.expected_bytes {
            return Err(StorageValueError::InvalidShape);
        }
        if scratch.capacity() < projection_source_row_scratch_bytes_v1(row)? {
            return Err(StorageValueError::LimitExceeded);
        }
        update(&mut self.hasher, &(row_bytes - 8).to_be_bytes())?;
        update(
            &mut self.hasher,
            &u32::try_from(row.group_values().len())
                .map_err(|_| StorageValueError::SizeOverflow)?
                .to_be_bytes(),
        )?;
        for value in row.group_values() {
            scratch.clear();
            encode_canonical_value_into(scratch, value)
                .map_err(|_| StorageValueError::InvalidShape)?;
            update(&mut self.hasher, &(scratch.len() as u64).to_be_bytes())?;
            update(&mut self.hasher, scratch)?;
        }
        scratch.clear();
        encode_canonical_record_into(scratch, row.measures())
            .map_err(|_| StorageValueError::InvalidShape)?;
        update(&mut self.hasher, &(scratch.len() as u64).to_be_bytes())?;
        update(&mut self.hasher, scratch)?;
        update(&mut self.hasher, &row.last_changed_sequence().to_be_bytes())?;
        self.last_key[..key.len()].copy_from_slice(key);
        self.last_key_len = key.len();
        self.rows += 1;
        self.bytes = bytes;
        Ok(())
    }

    /// Finishes only an unpoisoned complete stream with exact count and bytes.
    /// The returned digest is structural evidence, never a publication token.
    pub fn finish(mut self) -> Result<ContentDigest, StorageValueError> {
        if self.poisoned || self.rows != self.expected_rows || self.bytes != self.expected_bytes {
            return Err(StorageValueError::InvalidShape);
        }
        update(&mut self.hasher, &self.rows.to_be_bytes())?;
        update(&mut self.hasher, &self.bytes.to_be_bytes())?;
        self.hasher
            .finish()
            .map_err(|_| StorageValueError::InvalidShape)
    }
}

impl std::fmt::Debug for ProjectionSourceRowRootV1<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProjectionSourceRowRootV1")
            .field("rows", &self.rows)
            .field("semantic_bytes", &self.bytes)
            .field("poisoned", &self.poisoned)
            .finish()
    }
}

fn add(a: u64, b: u64) -> Result<u64, StorageValueError> {
    a.checked_add(b).ok_or(StorageValueError::SizeOverflow)
}
fn length(
    result: Result<usize, riffdb_types::CanonicalCodecError>,
) -> Result<u64, StorageValueError> {
    u64::try_from(result.map_err(|_| StorageValueError::InvalidShape)?)
        .map_err(|_| StorageValueError::SizeOverflow)
}
fn update(hasher: &mut ContentHasher, bytes: &[u8]) -> Result<(), StorageValueError> {
    hasher
        .update(bytes)
        .map_err(|_| StorageValueError::InvalidShape)
}

#[cfg(test)]
#[path = "source_row_root_tests.rs"]
mod tests;
