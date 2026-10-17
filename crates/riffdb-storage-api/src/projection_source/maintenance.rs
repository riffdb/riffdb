//! Bounded immutable copy custody and checked progress; no publication capability.

use super::{
    MAX_PROJECTION_SOURCE_CONTROL_BYTES, MAX_PROJECTION_SOURCE_STEP_BYTES,
    MAX_PROJECTION_SOURCE_STEP_ROWS, ProjectionSourceDescriptorV1, ProjectionSourceReferenceV1,
    ProjectionSourceRootV1,
};
use crate::{AuthoritativeTransactionBindingV3, ChangelogHistoryStateV3, StorageValueError};
use riffdb_types::{ContentDigest, ContentHasher, DUAL_FRONTIER_BYTES, HashDomain, MAX_KEY_BYTES};
use std::{fmt, mem::size_of};

/// Structural copy custody of one exact immutable candidate.
/// Native/coordinator code must compare persisted before/after bytes, verify each
/// full key and typed row against sealed candidate custody, and independently
/// compare final root/count/charge/expected chain before publication. Neither a
/// reconstructed hash nor structural completion grants any of that authority.
#[derive(Clone, Eq, PartialEq)]
pub struct ProjectionSourceMaintenanceV1 {
    descriptor: ProjectionSourceDescriptorV1,
    expected_prior_root: Option<ProjectionSourceRootV1>,
    copied_rows: u32,
    copied_canonical_row_bytes: u64,
    last_copied_key: Option<Vec<u8>>,
    rolling_hash: ContentDigest,
}

impl fmt::Debug for ProjectionSourceMaintenanceV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProjectionSourceMaintenanceV1")
            .field("descriptor", &self.descriptor)
            .field("expected_prior_root", &self.expected_prior_root)
            .field("copied_rows", &self.copied_rows)
            .field(
                "copied_canonical_row_bytes",
                &self.copied_canonical_row_bytes,
            )
            .field("last_copied_key", &"[REDACTED]")
            .field("rolling_hash", &self.rolling_hash)
            .finish()
    }
}

impl ProjectionSourceMaintenanceV1 {
    /// Starts a structural candidate with a deterministic, fully bound seed.
    /// Descriptor/root input ownership and copy_step_work_bound_v1's conservative
    /// metadata/scratch allowance must be reserved before constructing it.
    pub fn initial(
        descriptor: ProjectionSourceDescriptorV1,
        expected_prior_root: Option<ProjectionSourceRootV1>,
    ) -> Result<Self, StorageValueError> {
        validate_context(&descriptor, expected_prior_root.as_ref())?;
        let rolling_hash = initial_hash(&descriptor, expected_prior_root.as_ref())?;
        Ok(Self {
            descriptor,
            expected_prior_root,
            copied_rows: 0,
            copied_canonical_row_bytes: 0,
            last_copied_key: None,
            rolling_hash,
        })
    }

    /// Decodes a structurally consistent progress record, never a verified copy.
    /// The separately reviewed codec also bounds the complete encoded control
    /// record to4KiB before persistence. Native replay verifies actual row bytes.
    pub fn from_stored_parts(
        descriptor: ProjectionSourceDescriptorV1,
        expected_prior_root: Option<ProjectionSourceRootV1>,
        copied_rows: u32,
        copied_canonical_row_bytes: u64,
        last_copied_key: Option<Vec<u8>>,
        rolling_hash: ContentDigest,
    ) -> Result<Self, StorageValueError> {
        validate_context(&descriptor, expected_prior_root.as_ref())?;
        if rolling_hash.domain() != HashDomain::ProjectionSourceCopyProgress {
            return Err(StorageValueError::InvalidShape);
        }
        if last_copied_key
            .as_ref()
            .is_some_and(|key| key.len() > MAX_KEY_BYTES)
        {
            return Err(StorageValueError::LimitExceeded);
        }
        if copied_rows > descriptor.row_count()
            || copied_canonical_row_bytes > descriptor.canonical_row_bytes()
            || (copied_rows == 0) != (copied_canonical_row_bytes == 0)
            || (copied_rows == 0) != last_copied_key.is_none()
            || last_copied_key.as_ref().is_some_and(Vec::is_empty)
            || copied_canonical_row_bytes < u64::from(copied_rows)
            || (copied_rows == descriptor.row_count())
                != (copied_canonical_row_bytes == descriptor.canonical_row_bytes())
        {
            return Err(StorageValueError::InvalidShape);
        }
        if copied_rows == 0
            && rolling_hash != initial_hash(&descriptor, expected_prior_root.as_ref())?
        {
            return Err(StorageValueError::InvalidShape);
        }
        Ok(Self {
            descriptor,
            expected_prior_root,
            copied_rows,
            copied_canonical_row_bytes,
            last_copied_key,
            rolling_hash,
        })
    }

    /// Pure checked copy post-image from already codec-checked borrowed key/value
    /// pairs. This uses their exact complete native bytes as charge, never an
    /// estimate based on a fixed-width ordinal. It performs no durable mutation.
    /// Reserve copy_step_work_bound_v1 before this call; retain old ownership
    /// until native outcome reconciliation finishes. A stale/unequal before
    /// image refuses. Unknown outcomes still require exact persisted-page checks.
    pub fn after_copy_step(
        &self,
        expected_before: &Self,
        canonical_rows: &[(&[u8], &[u8])],
    ) -> Result<Self, StorageValueError> {
        if self != expected_before || canonical_rows.is_empty() {
            return Err(StorageValueError::InvalidShape);
        }
        if canonical_rows.len() > usize::from(MAX_PROJECTION_SOURCE_STEP_ROWS) {
            return Err(StorageValueError::LimitExceeded);
        }
        let mut last = self.last_copied_key.as_deref();
        let mut step_bytes = 0_u64;
        for &(key, value) in canonical_rows {
            if key.len() > MAX_KEY_BYTES {
                return Err(StorageValueError::LimitExceeded);
            }
            if key.is_empty() || value.is_empty() || last.is_some_and(|prior| prior >= key) {
                return Err(StorageValueError::InvalidShape);
            }
            let bytes = key
                .len()
                .checked_add(value.len())
                .and_then(|bytes| u64::try_from(bytes).ok())
                .ok_or(StorageValueError::SizeOverflow)?;
            step_bytes = step_bytes
                .checked_add(bytes)
                .ok_or(StorageValueError::SizeOverflow)?;
            if step_bytes > MAX_PROJECTION_SOURCE_STEP_BYTES {
                return Err(StorageValueError::LimitExceeded);
            }
            last = Some(key);
        }
        let final_rows = self
            .copied_rows
            .checked_add(
                u32::try_from(canonical_rows.len()).map_err(|_| StorageValueError::SizeOverflow)?,
            )
            .ok_or(StorageValueError::SizeOverflow)?;
        let final_bytes = self
            .copied_canonical_row_bytes
            .checked_add(step_bytes)
            .ok_or(StorageValueError::SizeOverflow)?;
        if final_rows > self.descriptor.row_count()
            || final_bytes > self.descriptor.canonical_row_bytes()
            || (final_rows == self.descriptor.row_count())
                != (final_bytes == self.descriptor.canonical_row_bytes())
        {
            return Err(StorageValueError::InvalidShape);
        }
        let mut rolling_hash = self.rolling_hash;
        let mut copied_rows = self.copied_rows;
        let mut copied_bytes = self.copied_canonical_row_bytes;
        for &(key, value) in canonical_rows {
            copied_rows = copied_rows
                .checked_add(1)
                .ok_or(StorageValueError::SizeOverflow)?;
            copied_bytes = copied_bytes
                .checked_add(
                    u64::try_from(key.len() + value.len())
                        .map_err(|_| StorageValueError::SizeOverflow)?,
                )
                .ok_or(StorageValueError::SizeOverflow)?;
            rolling_hash = row_hash(rolling_hash, copied_rows, copied_bytes, key, value)?;
        }
        let last_copied_key = canonical_rows.last().map(|(key, _)| key.to_vec());
        Ok(Self {
            descriptor: self.descriptor.clone(),
            expected_prior_root: self.expected_prior_root.clone(),
            copied_rows: final_rows,
            copied_canonical_row_bytes: final_bytes,
            last_copied_key,
            rolling_hash,
        })
    }

    /// Exact immutable descriptor, including independently expected final root.
    #[must_use]
    pub const fn descriptor(&self) -> &ProjectionSourceDescriptorV1 {
        &self.descriptor
    }
    /// Exact publication record expected by the candidate's eventual CAS.
    #[must_use]
    pub const fn expected_prior_root(&self) -> Option<&ProjectionSourceRootV1> {
        self.expected_prior_root.as_ref()
    }
    /// Captured immutable authority fence; appends never recapture it.
    #[must_use]
    pub const fn source_fence(&self) -> ChangelogHistoryStateV3 {
        self.descriptor.captured_source()
    }
    /// Number of complete ordered rows already accounted for.
    #[must_use]
    pub const fn copied_rows(&self) -> u32 {
        self.copied_rows
    }
    /// Exact complete native key/value bytes copied, excluding control metadata.
    #[must_use]
    pub const fn copied_canonical_row_bytes(&self) -> u64 {
        self.copied_canonical_row_bytes
    }
    /// Last complete native key, never a raw payload in public diagnostics.
    #[must_use]
    pub fn last_copied_key(&self) -> Option<&[u8]> {
        self.last_copied_key.as_deref()
    }
    /// Domain-separated per-row chain; distinct from the independent final root.
    #[must_use]
    pub const fn rolling_hash(&self) -> ContentDigest {
        self.rolling_hash
    }
    /// Structural count/charge completion only. Native final row-root and custody
    /// verification outside the primary gate is still mandatory.
    #[must_use]
    pub fn copy_complete(&self) -> bool {
        self.copied_rows == self.descriptor.row_count()
            && self.copied_canonical_row_bytes == self.descriptor.canonical_row_bytes()
    }
    /// Cheap allocation-free conservative metadata/post-image/scratch allowance.
    /// This does not include caller-owned typed rows, decode pages or prepared
    /// writes: those remain separately admitted in the same64MiB worker budget.
    pub fn copy_step_work_bound_v1(&self) -> Result<u64, StorageValueError> {
        Self::initial_work_bound_v1()
    }
    /// Reserves before constructing initial descriptor/root/hash custody.
    pub fn initial_work_bound_v1() -> Result<u64, StorageValueError> {
        (4 * MAX_PROJECTION_SOURCE_CONTROL_BYTES + MAX_KEY_BYTES + size_of::<Self>())
            .checked_add(512)
            .and_then(|bytes| bytes.checked_mul(8))
            .and_then(|bytes| u64::try_from(bytes).ok())
            .ok_or(StorageValueError::SizeOverflow)
    }
}

fn validate_context(
    descriptor: &ProjectionSourceDescriptorV1,
    prior: Option<&ProjectionSourceRootV1>,
) -> Result<(), StorageValueError> {
    match (descriptor.prior_source_digest(), prior) {
        (None, None) => Ok(()),
        (Some(digest), Some(prior))
            if digest == prior.reference().digest()
                && descriptor.reference().identity() == prior.reference().identity()
                && descriptor.reference().locator() != prior.reference().locator() =>
        {
            Ok(())
        }
        _ => Err(StorageValueError::IdentityMismatch),
    }
}

fn initial_hash(
    descriptor: &ProjectionSourceDescriptorV1,
    prior: Option<&ProjectionSourceRootV1>,
) -> Result<ContentDigest, StorageValueError> {
    let reference = descriptor.reference();
    let mut length = 4 + 1 + reference_length(reference)? + 1;
    if let Some(prior) = prior {
        length = length
            .checked_add(reference_length(prior.reference())?)
            .and_then(|length| {
                length.checked_add(binding_length(
                    prior.publication_binding(),
                    prior.administration_sequence().is_some(),
                ))
            })
            .ok_or(StorageValueError::SizeOverflow)?;
    }
    let mut hash = ContentHasher::new(
        HashDomain::ProjectionSourceCopyProgress,
        u64::try_from(length).map_err(|_| StorageValueError::SizeOverflow)?,
    );
    update(&mut hash, &1_u32.to_be_bytes())?;
    update(&mut hash, &[0])?;
    update_reference(&mut hash, reference)?;
    match prior {
        None => update(&mut hash, &[0])?,
        Some(prior) => {
            update(&mut hash, &[1])?;
            update_reference(&mut hash, prior.reference())?;
            let binding = prior.publication_binding();
            update(&mut hash, binding.database_id.as_bytes())?;
            update(&mut hash, &binding.history_incarnation.to_be_bytes())?;
            match binding.predecessor {
                None => update(&mut hash, &[0])?,
                Some(sequence) => {
                    update(&mut hash, &[1])?;
                    update(&mut hash, &sequence.get().to_be_bytes())?;
                }
            }
            update(&mut hash, &binding.sequence.get().to_be_bytes())?;
            update(
                &mut hash,
                &binding.predecessor_frontier.to_canonical_bytes(),
            )?;
            update(&mut hash, &binding.covered_frontier.to_canonical_bytes())?;
            update(&mut hash, &binding.prior_history_hash)?;
            match prior.administration_sequence() {
                None => update(&mut hash, &[0])?,
                Some(sequence) => {
                    update(&mut hash, &[1])?;
                    update(&mut hash, &sequence.get().to_be_bytes())?;
                }
            }
        }
    }
    hash.finish().map_err(|_| StorageValueError::InvalidShape)
}
fn reference_length(reference: &ProjectionSourceReferenceV1) -> Result<usize, StorageValueError> {
    reference
        .identity()
        .contract_lineage()
        .as_bytes()
        .len()
        .checked_add(4 + 4 + 32 + 4 + 32 + 32 + 32)
        .ok_or(StorageValueError::SizeOverflow)
}
fn binding_length(binding: AuthoritativeTransactionBindingV3, has_admin: bool) -> usize {
    16 + 8
        + 1
        + if binding.predecessor.is_some() { 8 } else { 0 }
        + 8
        + 2 * DUAL_FRONTIER_BYTES
        + 32
        + 1
        + if has_admin { 8 } else { 0 }
}
fn update_reference(
    hash: &mut ContentHasher,
    reference: &ProjectionSourceReferenceV1,
) -> Result<(), StorageValueError> {
    let identity = reference.identity().to_canonical_bytes();
    update(
        hash,
        &u32::try_from(identity.len())
            .map_err(|_| StorageValueError::SizeOverflow)?
            .to_be_bytes(),
    )?;
    update(hash, &identity)?;
    update(hash, reference.locator().database_id().as_bytes())?;
    update(
        hash,
        &reference
            .locator()
            .allocation_history_incarnation()
            .to_be_bytes(),
    )?;
    update(hash, &reference.locator().ordinal().to_be_bytes())?;
    update(hash, reference.digest().as_bytes())?;
    update(hash, reference.origin().as_bytes())
}
fn row_hash(
    prior: ContentDigest,
    copied_rows: u32,
    copied_bytes: u64,
    key: &[u8],
    value: &[u8],
) -> Result<ContentDigest, StorageValueError> {
    let length = key
        .len()
        .checked_add(value.len())
        .and_then(|b| b.checked_add(4 + 1 + 32 + 4 + 8 + 4 + 4))
        .and_then(|b| u64::try_from(b).ok())
        .ok_or(StorageValueError::SizeOverflow)?;
    let mut hash = ContentHasher::new(HashDomain::ProjectionSourceCopyProgress, length);
    update(&mut hash, &1_u32.to_be_bytes())?;
    update(&mut hash, &[1])?;
    update(&mut hash, prior.as_bytes())?;
    update(&mut hash, &copied_rows.to_be_bytes())?;
    update(&mut hash, &copied_bytes.to_be_bytes())?;
    update(
        &mut hash,
        &u32::try_from(key.len())
            .map_err(|_| StorageValueError::SizeOverflow)?
            .to_be_bytes(),
    )?;
    update(&mut hash, key)?;
    update(
        &mut hash,
        &u32::try_from(value.len())
            .map_err(|_| StorageValueError::SizeOverflow)?
            .to_be_bytes(),
    )?;
    update(&mut hash, value)?;
    hash.finish().map_err(|_| StorageValueError::InvalidShape)
}
fn update(hash: &mut ContentHasher, bytes: &[u8]) -> Result<(), StorageValueError> {
    hash.update(bytes)
        .map_err(|_| StorageValueError::InvalidShape)
}

riffdb_types::inspect_owned_capacity_struct_v1!(ProjectionSourceMaintenanceV1 {
    owned: [descriptor, expected_prior_root, last_copied_key],
    inline: [copied_rows, copied_canonical_row_bytes, rolling_hash]
});
