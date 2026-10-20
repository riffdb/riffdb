//! Canonical Source and local generation-base keys. Syntax never grants custody.
//!
//! Source keys bind their original full locator; local base keys bind exact
//! identity and generation. Row suffixes use the existing canonical scalar
//! frames. Complete checks require the already admitted typed context.
#![cfg_attr(not(feature = "projection-source-driver"), allow(dead_code))]
use crate::keys::PhysicalKeyError;
use riffdb_types::{
    DatabaseId, MAX_KEY_BYTES, ProjectionCheckpointOrdinal, ProjectionGeneration,
    ProjectionGroupKey, ProjectionIdentity, ProjectionSourceLocatorV1,
};
use std::mem::size_of;

pub(crate) const SOURCE_ROW_KEY_V1: [u8; 2] = [0x53, 0x01];
pub(crate) const SOURCE_OBJECT_KEY_V1: [u8; 2] = [0x4f, 0x01];
pub(crate) const SOURCE_ROOT_KEY_V1: [u8; 2] = [0x52, 0x01];
pub(crate) const SOURCE_MAINTENANCE_KEY_V1: [u8; 2] = [0x4d, 0x01];
// Derived-local identity+generation key; never a Source locator or grant.
pub(crate) const GENERATION_SOURCE_BASE_KEY_V1: [u8; 2] = [0x42, 0x01];
const LOCATOR_BYTES: usize = 32;
pub(crate) const SOURCE_LOCATOR_KEY_BYTES: usize = 2 + LOCATOR_BYTES;

pub(crate) fn source_row_prefix(
    locator: ProjectionSourceLocatorV1,
) -> [u8; SOURCE_LOCATOR_KEY_BYTES] {
    fixed_locator_key(SOURCE_ROW_KEY_V1, locator)
}
pub(crate) fn source_object_key(
    locator: ProjectionSourceLocatorV1,
) -> [u8; SOURCE_LOCATOR_KEY_BYTES] {
    fixed_locator_key(SOURCE_OBJECT_KEY_V1, locator)
}
pub(crate) fn source_maintenance_key(
    locator: ProjectionSourceLocatorV1,
) -> [u8; SOURCE_LOCATOR_KEY_BYTES] {
    fixed_locator_key(SOURCE_MAINTENANCE_KEY_V1, locator)
}
pub(crate) fn decode_source_object_key(
    bytes: &[u8],
) -> Result<ProjectionSourceLocatorV1, PhysicalKeyError> {
    exact_locator_key(bytes, SOURCE_OBJECT_KEY_V1)
}
pub(crate) fn decode_source_maintenance_key(
    bytes: &[u8],
) -> Result<ProjectionSourceLocatorV1, PhysicalKeyError> {
    exact_locator_key(bytes, SOURCE_MAINTENANCE_KEY_V1)
}

/// Partial routing syntax only. The suffix is not validated by this function.
/// Call check_source_row_key after decoding its admitted row/object context.
pub(crate) fn source_row_locator_prefix(
    bytes: &[u8],
) -> Result<ProjectionSourceLocatorV1, PhysicalKeyError> {
    if bytes.len() > MAX_KEY_BYTES || bytes.len() <= SOURCE_LOCATOR_KEY_BYTES {
        return Err(PhysicalKeyError::InvalidLength);
    }
    let prefix = bytes
        .get(..SOURCE_LOCATOR_KEY_BYTES)
        .ok_or(PhysicalKeyError::InvalidLength)?;
    exact_locator_key(prefix, SOURCE_ROW_KEY_V1)
}

pub(crate) fn source_row_key_len(group: &ProjectionGroupKey) -> Result<usize, PhysicalKeyError> {
    let suffix = scalar_suffix(group)?;
    let length = SOURCE_LOCATOR_KEY_BYTES
        .checked_add(suffix.len())
        .ok_or(PhysicalKeyError::SizeOverflow)?;
    if length > MAX_KEY_BYTES {
        return Err(PhysicalKeyError::InvalidLength);
    }
    Ok(length)
}

/// Caller admits this exact scratch capacity before allocation. Refusal leaves
/// the supplied buffer unchanged; successful encoding never grows its capacity.
pub(crate) fn encode_source_row_key_into(
    output: &mut Vec<u8>,
    locator: ProjectionSourceLocatorV1,
    group: &ProjectionGroupKey,
) -> Result<(), PhysicalKeyError> {
    let length = source_row_key_len(group)?;
    if output.capacity() < length {
        return Err(PhysicalKeyError::InvalidLength);
    }
    let suffix = scalar_suffix(group)?;
    output.clear();
    output.extend_from_slice(&source_row_prefix(locator));
    output.extend_from_slice(suffix);
    Ok(())
}

pub(crate) fn check_source_row_key(
    bytes: &[u8],
    locator: ProjectionSourceLocatorV1,
    group: &ProjectionGroupKey,
) -> Result<(), PhysicalKeyError> {
    if source_row_locator_prefix(bytes)? != locator
        || bytes.len() != source_row_key_len(group)?
        || bytes[SOURCE_LOCATOR_KEY_BYTES..] != *scalar_suffix(group)?
    {
        return Err(PhysicalKeyError::NonCanonical);
    }
    Ok(())
}

/// Exact identity prefix for bounded selected-root lookup. It is not a root key:
/// selection must read at most64 complete roots and require exactly one match.
pub(crate) fn encode_source_root_identity_prefix_into(
    output: &mut Vec<u8>,
    identity: &ProjectionIdentity,
) -> Result<(), PhysicalKeyError> {
    let length = 2 + identity_bytes(identity);
    if output.capacity() < length {
        return Err(PhysicalKeyError::InvalidLength);
    }
    output.clear();
    output.extend_from_slice(&SOURCE_ROOT_KEY_V1);
    append_identity(output, identity);
    Ok(())
}

pub(crate) fn source_root_key_len(identity: &ProjectionIdentity) -> usize {
    2 + identity_bytes(identity) + LOCATOR_BYTES
}

pub(crate) fn encode_source_root_key_into(
    output: &mut Vec<u8>,
    identity: &ProjectionIdentity,
    locator: ProjectionSourceLocatorV1,
) -> Result<(), PhysicalKeyError> {
    if output.capacity() < source_root_key_len(identity) {
        return Err(PhysicalKeyError::InvalidLength);
    }
    encode_source_root_identity_prefix_into(output, identity)?;
    append_locator(output, locator);
    Ok(())
}

/// Full root syntax/context check, including the immutable original locator.
/// The supplied identity must come from the admitted root/object value, never
/// a caller-selected latest root or an identity inferred from this key alone.
pub(crate) fn check_source_root_key(
    bytes: &[u8],
    identity: &ProjectionIdentity,
    locator: ProjectionSourceLocatorV1,
) -> Result<(), PhysicalKeyError> {
    if bytes.len() != source_root_key_len(identity)
        || bytes.get(..2) != Some(SOURCE_ROOT_KEY_V1.as_slice())
    {
        return Err(PhysicalKeyError::InvalidLength);
    }
    let identity_end = check_identity_prefix(bytes, identity)?;
    if decode_locator(&bytes[identity_end..])? != locator {
        return Err(PhysicalKeyError::NonCanonical);
    }
    Ok(())
}

/// Exact derived-local base key. The same canonical identity framing is shared
/// with Source roots; the final generation is fixed-width big-endian.
pub(crate) fn generation_source_base_key_len(identity: &ProjectionIdentity) -> usize {
    2 + identity_bytes(identity) + size_of::<u64>()
}

/// Caller admits the complete scratch capacity before allocation. A refusal
/// preserves the supplied bytes and capacity; success never grows the Vec.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn encode_generation_source_base_key_into(
    output: &mut Vec<u8>,
    identity: &ProjectionIdentity,
    generation: ProjectionGeneration,
) -> Result<(), PhysicalKeyError> {
    if output.capacity() < generation_source_base_key_len(identity) {
        return Err(PhysicalKeyError::InvalidLength);
    }
    output.resize(generation_source_base_key_len(identity), 0);
    encode_generation_source_base_key_slice_into(output, identity, generation)?;
    Ok(())
}

// One allocation-free encoder also serves refusal-only ordinary native reads.
// No Source value or selection authority is reconstructed from these bytes.
pub(crate) fn encode_generation_source_base_key_slice_into(
    output: &mut [u8],
    identity: &ProjectionIdentity,
    generation: ProjectionGeneration,
) -> Result<usize, PhysicalKeyError> {
    let length = generation_source_base_key_len(identity);
    if output.len() < length {
        return Err(PhysicalKeyError::InvalidLength);
    }
    let lineage = identity.contract_lineage().as_bytes();
    let lineage_end = 6 + lineage.len();
    let identity_end = lineage_end + 36;
    output[..2].copy_from_slice(&GENERATION_SOURCE_BASE_KEY_V1);
    output[2..6].copy_from_slice(&(lineage.len() as u32).to_be_bytes());
    output[6..lineage_end].copy_from_slice(lineage);
    output[lineage_end..lineage_end + 4].copy_from_slice(&identity.projection_id().to_be_bytes());
    output[lineage_end + 4..identity_end].copy_from_slice(identity.plan_hash().as_bytes());
    output[identity_end..length].copy_from_slice(&generation.to_be_bytes());
    Ok(length)
}

/// Context checks only. A matching key/value cannot authorize seeding or source
/// selection; the actual native owner must retain that separate evidence.
pub(crate) fn check_generation_source_base_key(
    bytes: &[u8],
    identity: &ProjectionIdentity,
    generation: ProjectionGeneration,
) -> Result<(), PhysicalKeyError> {
    if bytes.len() != generation_source_base_key_len(identity)
        || bytes.get(..2) != Some(GENERATION_SOURCE_BASE_KEY_V1.as_slice())
    {
        return Err(PhysicalKeyError::InvalidLength);
    }
    let identity_end = check_identity_prefix(bytes, identity)?;
    if bytes[identity_end..] != generation.to_be_bytes() {
        return Err(PhysicalKeyError::NonCanonical);
    }
    Ok(())
}

fn check_identity_prefix(
    bytes: &[u8],
    identity: &ProjectionIdentity,
) -> Result<usize, PhysicalKeyError> {
    let lineage = identity.contract_lineage().as_bytes();
    let lineage_end = 6 + lineage.len();
    let identity_end = lineage_end + 36;
    if bytes.len() < identity_end {
        return Err(PhysicalKeyError::InvalidLength);
    }
    if bytes[2..6] != (lineage.len() as u32).to_be_bytes()
        || bytes[6..lineage_end] != *lineage
        || bytes[lineage_end..lineage_end + 4] != identity.projection_id().to_be_bytes()
        || bytes[lineage_end + 4..identity_end] != *identity.plan_hash().as_bytes()
    {
        return Err(PhysicalKeyError::NonCanonical);
    }
    Ok(identity_end)
}

fn scalar_suffix(group: &ProjectionGroupKey) -> Result<&[u8], PhysicalKeyError> {
    // This is an order witness only. The generation is checked and discarded;
    // no derived generation field enters any Source key or row.
    if group.generation() != ProjectionGeneration::first() {
        return Err(PhysicalKeyError::InvalidComponent);
    }
    let offset = 2 + identity_bytes(group.identity()) + 8;
    let suffix = group
        .as_bytes()
        .get(offset..)
        .ok_or(PhysicalKeyError::InvalidLength)?;
    if suffix.is_empty() {
        return Err(PhysicalKeyError::InvalidLength);
    }
    // Existing valid keys have header>=2+41+8=51 bytes. The Source header34 is
    // shorter, preserving all existing4096-byte complete group-key capacity.
    Ok(suffix)
}
fn identity_bytes(identity: &ProjectionIdentity) -> usize {
    40 + identity.contract_lineage().as_bytes().len()
}
fn append_identity(output: &mut Vec<u8>, identity: &ProjectionIdentity) {
    let lineage = identity.contract_lineage().as_bytes();
    output.extend_from_slice(&(lineage.len() as u32).to_be_bytes());
    output.extend_from_slice(lineage);
    output.extend_from_slice(&identity.projection_id().to_be_bytes());
    output.extend_from_slice(identity.plan_hash().as_bytes());
}
fn append_locator(output: &mut Vec<u8>, locator: ProjectionSourceLocatorV1) {
    output.extend_from_slice(locator.database_id().as_bytes());
    output.extend_from_slice(&locator.allocation_history_incarnation().to_be_bytes());
    output.extend_from_slice(&locator.ordinal().to_be_bytes());
}
fn fixed_locator_key(
    purpose: [u8; 2],
    locator: ProjectionSourceLocatorV1,
) -> [u8; SOURCE_LOCATOR_KEY_BYTES] {
    let mut key = [0; SOURCE_LOCATOR_KEY_BYTES];
    key[..2].copy_from_slice(&purpose);
    key[2..18].copy_from_slice(locator.database_id().as_bytes());
    key[18..26].copy_from_slice(&locator.allocation_history_incarnation().to_be_bytes());
    key[26..34].copy_from_slice(&locator.ordinal().to_be_bytes());
    key
}
fn exact_locator_key(
    bytes: &[u8],
    purpose: [u8; 2],
) -> Result<ProjectionSourceLocatorV1, PhysicalKeyError> {
    if bytes.len() != SOURCE_LOCATOR_KEY_BYTES {
        return Err(PhysicalKeyError::InvalidLength);
    }
    if bytes[..2] != purpose {
        return Err(PhysicalKeyError::InvalidPrefix);
    }
    decode_locator(&bytes[2..])
}
fn decode_locator(bytes: &[u8]) -> Result<ProjectionSourceLocatorV1, PhysicalKeyError> {
    if bytes.len() != LOCATOR_BYTES {
        return Err(PhysicalKeyError::InvalidLength);
    }
    let db = DatabaseId::from_bytes(
        bytes[..16]
            .try_into()
            .map_err(|_| PhysicalKeyError::InvalidLength)?,
    )
    .map_err(|_| PhysicalKeyError::InvalidComponent)?;
    let incarnation = u64::from_be_bytes(
        bytes[16..24]
            .try_into()
            .map_err(|_| PhysicalKeyError::InvalidLength)?,
    );
    let ordinal = ProjectionCheckpointOrdinal::new(u64::from_be_bytes(
        bytes[24..32]
            .try_into()
            .map_err(|_| PhysicalKeyError::InvalidLength)?,
    ))
    .ok_or(PhysicalKeyError::InvalidComponent)?;
    ProjectionSourceLocatorV1::new(db, incarnation, ordinal)
        .ok_or(PhysicalKeyError::InvalidComponent)
}

#[cfg(test)]
#[path = "source_keys_tests.rs"]
mod tests;
