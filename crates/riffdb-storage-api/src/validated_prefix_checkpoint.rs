//! Proof-carrying validated-prefix startup checkpoint (ADR-0019 Amendment 1 / ADR-0085 A1).

use std::borrow::Borrow;

use riffdb_types::{ContentHasher, DatabaseId, EntityVersion, HashDomain, SchemaHash, hash};

use crate::{EntityChainHeadV1, EntityChainStateV1, EntityTarget, StorageValueError};

/// Domain label framed into the checkpoint self-hash preimage.
const CHECKPOINT_HASH_LABEL: &[u8] = b"riffdb.validated-prefix-checkpoint/v1\0";
/// Domain label framed into the entity-chain fingerprint preimage.
const ENTITY_CHAIN_FINGERPRINT_LABEL: &[u8] = b"riffdb.entity-chain-fingerprint/v1\0";
/// Domain label for the delete-aware entity-transition checkpoint fingerprint.
const ENTITY_TRANSITION_FINGERPRINT_LABEL: &[u8] = b"riffdb.entity-transition-fingerprint/v1\0";
/// Domain label for the delete-aware checkpoint successor self-hash.
const CHECKPOINT_V2_HASH_LABEL: &[u8] = b"riffdb.validated-prefix-checkpoint/v2\0";

/// Chained self-hash of one validated-prefix checkpoint.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ValidatedPrefixCheckpointHash([u8; 32]);

impl ValidatedPrefixCheckpointHash {
    /// Constructs a hash from exact 32 digest bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the exact digest bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Domain-separated fingerprint of the entity-chain map at a commit sequence.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct EntityChainFingerprint([u8; 32]);

impl EntityChainFingerprint {
    /// Constructs a fingerprint from exact 32 digest bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the exact digest bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Digests the canonical sorted `(target, entity_version)` pairs.
    ///
    /// Callers must pass pairs already sorted by [`EntityTarget`] order.
    pub fn from_sorted_pairs<'a, I>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (&'a EntityTarget, EntityVersion)>,
    {
        let mut preimage = Vec::new();
        preimage.extend_from_slice(ENTITY_CHAIN_FINGERPRINT_LABEL);
        for (target, version) in pairs {
            preimage.extend_from_slice(&target.entity_type_id().get().to_be_bytes());
            let key = target.key().as_bytes();
            preimage.extend_from_slice(&(key.len() as u32).to_be_bytes());
            preimage.extend_from_slice(key);
            preimage.extend_from_slice(&version.get().to_be_bytes());
        }
        let digest = hash(HashDomain::Schema, &preimage);
        Self::from_bytes(*digest.as_bytes())
    }
}

/// Sequence-keyed table row counts for rows with sequence ≤ S.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValidatedPrefixSequenceCounts {
    /// Rows in `COMMITS` with sequence ≤ S.
    pub commits_count: u64,
    /// Rows in `EVENTS` with commit sequence ≤ S.
    pub events_count: u64,
    /// Rows in `EVENT_ROUTES` whose event commit sequence ≤ S.
    pub event_routes_count: u64,
    /// Rows in `OUTBOX` with event commit sequence ≤ S.
    pub outbox_count: u64,
    /// Rows in `OUTBOX_STATUS` with event commit sequence ≤ S.
    pub outbox_status_count: u64,
    /// Materialized terminal idempotency rows whose outcome commit sequence ≤ S.
    /// Successful outcomes still owned by retained command segments are not
    /// physical rows and therefore are not included.
    pub idempotency_count: u64,
    /// Rows in `AUDIT` with administration sequence ≤ audit bound.
    pub audit_count: u64,
    /// Rows in `AUDIT_BY_REQUEST` with administration sequence ≤ audit bound.
    pub audit_by_request_count: u64,
}

/// Retained-metadata allocator scalars snapshotted with the checkpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValidatedPrefixRetainedSnapshot {
    /// Next application sequence get() value, or 0 when exhausted.
    pub next_application_sequence: u64,
    /// True when the application allocator is exhausted.
    pub application_sequence_exhausted: bool,
    /// Next administration sequence get() value, or 0 when exhausted.
    pub next_administration_sequence: u64,
    /// True when the administration allocator is exhausted.
    pub administration_sequence_exhausted: bool,
}

/// Proof-carrying validated-prefix startup checkpoint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredValidatedPrefixCheckpointV1 {
    database_id: DatabaseId,
    history_incarnation: u64,
    registry_digest: SchemaHash,
    /// Highest fully-validated commit sequence S; 0 means empty application history.
    checkpoint_commit_sequence: u64,
    /// Highest fully-validated administration sequence bound at S; 0 means none.
    audit_sequence_bound: u64,
    counts: ValidatedPrefixSequenceCounts,
    entity_chain_fingerprint: EntityChainFingerprint,
    retained: ValidatedPrefixRetainedSnapshot,
    previous_checkpoint_hash: Option<ValidatedPrefixCheckpointHash>,
    checkpoint_hash: ValidatedPrefixCheckpointHash,
    /// Retention watermark sequence bound at write time (ADR-0085 Amendment 2).
    retention_watermark_sequence: u64,
}

impl StoredValidatedPrefixCheckpointV1 {
    /// Constructs a checkpoint and computes its chained self-hash.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        database_id: DatabaseId,
        history_incarnation: u64,
        registry_digest: SchemaHash,
        checkpoint_commit_sequence: u64,
        audit_sequence_bound: u64,
        counts: ValidatedPrefixSequenceCounts,
        entity_chain_fingerprint: EntityChainFingerprint,
        retained: ValidatedPrefixRetainedSnapshot,
        previous_checkpoint_hash: Option<ValidatedPrefixCheckpointHash>,
        retention_watermark_sequence: u64,
    ) -> Result<Self, StorageValueError> {
        let mut value = Self {
            database_id,
            history_incarnation,
            registry_digest,
            checkpoint_commit_sequence,
            audit_sequence_bound,
            counts,
            entity_chain_fingerprint,
            retained,
            previous_checkpoint_hash,
            checkpoint_hash: ValidatedPrefixCheckpointHash::from_bytes([0; 32]),
            retention_watermark_sequence,
        };
        value.checkpoint_hash = value.computed_hash()?;
        Self::from_stored_parts(
            value.database_id,
            value.history_incarnation,
            value.registry_digest,
            value.checkpoint_commit_sequence,
            value.audit_sequence_bound,
            value.counts,
            value.entity_chain_fingerprint,
            value.retained,
            value.previous_checkpoint_hash,
            value.checkpoint_hash,
            value.retention_watermark_sequence,
        )
    }

    /// Reconstructs a semantically checked durable checkpoint.
    #[allow(clippy::too_many_arguments)]
    pub fn from_stored_parts(
        database_id: DatabaseId,
        history_incarnation: u64,
        registry_digest: SchemaHash,
        checkpoint_commit_sequence: u64,
        audit_sequence_bound: u64,
        counts: ValidatedPrefixSequenceCounts,
        entity_chain_fingerprint: EntityChainFingerprint,
        retained: ValidatedPrefixRetainedSnapshot,
        previous_checkpoint_hash: Option<ValidatedPrefixCheckpointHash>,
        checkpoint_hash: ValidatedPrefixCheckpointHash,
        retention_watermark_sequence: u64,
    ) -> Result<Self, StorageValueError> {
        if history_incarnation == 0 {
            return Err(StorageValueError::InvalidShape);
        }
        if retained.application_sequence_exhausted && retained.next_application_sequence != 0 {
            return Err(StorageValueError::InvalidShape);
        }
        if retained.administration_sequence_exhausted && retained.next_administration_sequence != 0
        {
            return Err(StorageValueError::InvalidShape);
        }
        if !retained.application_sequence_exhausted && retained.next_application_sequence == 0 {
            return Err(StorageValueError::InvalidShape);
        }
        if !retained.administration_sequence_exhausted && retained.next_administration_sequence == 0
        {
            return Err(StorageValueError::InvalidShape);
        }
        let value = Self {
            database_id,
            history_incarnation,
            registry_digest,
            checkpoint_commit_sequence,
            audit_sequence_bound,
            counts,
            entity_chain_fingerprint,
            retained,
            previous_checkpoint_hash,
            checkpoint_hash,
            retention_watermark_sequence,
        };
        if value.computed_hash()? != checkpoint_hash {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(value)
    }

    /// Returns the bound database identity.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Returns the history incarnation at write time.
    #[must_use]
    pub const fn history_incarnation(&self) -> u64 {
        self.history_incarnation
    }

    /// Returns the registry digest bound at write time.
    #[must_use]
    pub const fn registry_digest(&self) -> SchemaHash {
        self.registry_digest
    }

    /// Returns the highest fully-validated commit sequence S (0 = empty history).
    #[must_use]
    pub const fn checkpoint_commit_sequence(&self) -> u64 {
        self.checkpoint_commit_sequence
    }

    /// Returns the administration-sequence bound at S (0 = none).
    #[must_use]
    pub const fn audit_sequence_bound(&self) -> u64 {
        self.audit_sequence_bound
    }

    /// Returns sequence-keyed prefix row counts.
    #[must_use]
    pub const fn counts(&self) -> ValidatedPrefixSequenceCounts {
        self.counts
    }

    /// Returns the entity-chain fingerprint at S.
    #[must_use]
    pub const fn entity_chain_fingerprint(&self) -> EntityChainFingerprint {
        self.entity_chain_fingerprint
    }

    /// Returns retained-metadata allocator scalars.
    #[must_use]
    pub const fn retained(&self) -> ValidatedPrefixRetainedSnapshot {
        self.retained
    }

    /// Returns the previous checkpoint hash, if chained.
    #[must_use]
    pub const fn previous_checkpoint_hash(&self) -> Option<ValidatedPrefixCheckpointHash> {
        self.previous_checkpoint_hash
    }

    /// Returns this checkpoint's chained self-hash.
    #[must_use]
    pub const fn checkpoint_hash(&self) -> ValidatedPrefixCheckpointHash {
        self.checkpoint_hash
    }

    /// Returns the retention watermark sequence bound at write time.
    #[must_use]
    pub const fn retention_watermark_sequence(&self) -> u64 {
        self.retention_watermark_sequence
    }

    /// Recomputes the domain-separated self-hash over all fields except `checkpoint_hash`.
    pub fn computed_hash(&self) -> Result<ValidatedPrefixCheckpointHash, StorageValueError> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(CHECKPOINT_HASH_LABEL);
        bytes.extend_from_slice(&1_u32.to_be_bytes());
        bytes.extend_from_slice(self.database_id.as_bytes());
        bytes.extend_from_slice(&self.history_incarnation.to_be_bytes());
        bytes.extend_from_slice(self.registry_digest.as_bytes());
        bytes.extend_from_slice(&self.checkpoint_commit_sequence.to_be_bytes());
        bytes.extend_from_slice(&self.audit_sequence_bound.to_be_bytes());
        bytes.extend_from_slice(&self.counts.commits_count.to_be_bytes());
        bytes.extend_from_slice(&self.counts.events_count.to_be_bytes());
        bytes.extend_from_slice(&self.counts.event_routes_count.to_be_bytes());
        bytes.extend_from_slice(&self.counts.outbox_count.to_be_bytes());
        bytes.extend_from_slice(&self.counts.outbox_status_count.to_be_bytes());
        bytes.extend_from_slice(&self.counts.idempotency_count.to_be_bytes());
        bytes.extend_from_slice(&self.counts.audit_count.to_be_bytes());
        bytes.extend_from_slice(&self.counts.audit_by_request_count.to_be_bytes());
        bytes.extend_from_slice(self.entity_chain_fingerprint.as_bytes());
        bytes.extend_from_slice(&self.retained.next_application_sequence.to_be_bytes());
        bytes.push(u8::from(self.retained.application_sequence_exhausted));
        bytes.extend_from_slice(&self.retained.next_administration_sequence.to_be_bytes());
        bytes.push(u8::from(self.retained.administration_sequence_exhausted));
        match self.previous_checkpoint_hash {
            None => bytes.push(0),
            Some(hash) => {
                bytes.push(1);
                bytes.extend_from_slice(hash.as_bytes());
            }
        }
        bytes.extend_from_slice(&self.retention_watermark_sequence.to_be_bytes());
        let digest = hash(HashDomain::Schema, &bytes);
        Ok(ValidatedPrefixCheckpointHash::from_bytes(
            *digest.as_bytes(),
        ))
    }
}

/// Domain-separated fingerprint of every retained entity-chain head.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct EntityTransitionFingerprint([u8; 32]);

impl EntityTransitionFingerprint {
    /// Reconstructs an exact fingerprint.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Borrows the digest bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Digests canonical heads already sorted by exact entity target.
    /// The cloned iterator must traverse the same immutable heads. Only one
    /// bounded head preimage and the prior target are retained while hashing.
    pub fn from_sorted_heads<'a, I>(heads: I) -> Result<Self, StorageValueError>
    where
        I: IntoIterator<Item = &'a EntityChainHeadV1>,
        I::IntoIter: Clone,
    {
        let heads = heads.into_iter();
        fingerprint_from_passes(|| Ok(heads.clone().map(Ok)), |error| error)
    }

    /// Digests two ordered passes over one immutable storage view, propagating
    /// read failures without buffering the head population. The reader must
    /// return the same pinned view on both calls, not fresh/latest snapshots.
    pub fn from_sorted_head_reader<F, I>(heads: F) -> Result<Self, crate::StorageError>
    where
        F: FnMut() -> Result<I, crate::StorageError>,
        I: Iterator<Item = Result<EntityChainHeadV1, crate::StorageError>>,
    {
        fingerprint_from_passes(heads, |_| {
            crate::StorageError::new(crate::StorageErrorKind::CorruptData, None)
        })
    }
}

fn fingerprint_from_passes<F, I, H, E>(
    mut heads: F,
    invalid: impl Fn(StorageValueError) -> E,
) -> Result<EntityTransitionFingerprint, E>
where
    F: FnMut() -> Result<I, E>,
    I: Iterator<Item = Result<H, E>>,
    H: Borrow<EntityChainHeadV1>,
{
    let mut length = ENTITY_TRANSITION_FINGERPRINT_LABEL.len() as u64;
    let mut previous: Option<EntityTarget> = None;
    for head in heads()? {
        let head = head?;
        let head = head.borrow();
        let bytes = head_fingerprint_preimage(head, &mut previous).map_err(&invalid)?;
        length = length
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| invalid(StorageValueError::SizeOverflow))?;
    }
    let mut digest = ContentHasher::new(HashDomain::Schema, length);
    digest
        .update(ENTITY_TRANSITION_FINGERPRINT_LABEL)
        .map_err(|_| invalid(StorageValueError::InvalidShape))?;
    previous = None;
    for head in heads()? {
        let head = head?;
        let bytes = head_fingerprint_preimage(head.borrow(), &mut previous).map_err(&invalid)?;
        digest
            .update(&bytes)
            .map_err(|_| invalid(StorageValueError::InvalidShape))?;
    }
    let digest = digest
        .finish()
        .map_err(|_| invalid(StorageValueError::InvalidShape))?;
    Ok(EntityTransitionFingerprint::from_bytes(*digest.as_bytes()))
}

fn head_fingerprint_preimage(
    head: &EntityChainHeadV1,
    previous: &mut Option<EntityTarget>,
) -> Result<Vec<u8>, StorageValueError> {
    if previous
        .as_ref()
        .is_some_and(|prior| prior >= head.target())
    {
        return Err(StorageValueError::NonCanonicalOrder);
    }
    *previous = Some(head.target().clone());
    let key = head.target().key().as_bytes();
    let key_len = u32::try_from(key.len()).map_err(|_| StorageValueError::LimitExceeded)?;
    let mut preimage = Vec::with_capacity(key.len() + 97);
    preimage.extend_from_slice(&head.target().entity_type_id().get().to_be_bytes());
    preimage.extend_from_slice(&key_len.to_be_bytes());
    preimage.extend_from_slice(key);
    preimage.extend_from_slice(&head.chain_revision().to_be_bytes());
    match head.state() {
        EntityChainStateV1::Live {
            version,
            value_hash,
        } => {
            preimage.push(1);
            preimage.extend_from_slice(&version.get().to_be_bytes());
            preimage.extend_from_slice(value_hash.as_bytes());
        }
        EntityChainStateV1::Deleted => preimage.push(2),
        EntityChainStateV1::NeverExisted => return Err(StorageValueError::InvalidShape),
    }
    preimage.extend_from_slice(&head.last_command_sequence().get().to_be_bytes());
    preimage.extend_from_slice(head.last_transition_hash().as_bytes());
    Ok(preimage)
}

/// Delete-aware cardinality and history counters at one validated prefix.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValidatedPrefixEntityTransitionCounts {
    /// Materialized live entity rows at the checkpoint.
    pub live_entity_count: u64,
    /// Retained deleted chain heads at the checkpoint.
    pub deleted_entity_count: u64,
    /// Historical transitions represented by all chain revisions.
    pub entity_transition_count: u64,
}

/// Chained self-hash of a delete-aware checkpoint successor.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ValidatedPrefixCheckpointV2Hash([u8; 32]);

impl ValidatedPrefixCheckpointV2Hash {
    /// Reconstructs an exact hash.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Borrows the digest bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Validated-prefix successor whose entity proof survives current-row deletion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredValidatedPrefixCheckpointV2 {
    base: StoredValidatedPrefixCheckpointV1,
    entity_counts: ValidatedPrefixEntityTransitionCounts,
    entity_transition_fingerprint: EntityTransitionFingerprint,
    checkpoint_hash: ValidatedPrefixCheckpointV2Hash,
}

impl StoredValidatedPrefixCheckpointV2 {
    /// Constructs and self-hashes one delete-aware checkpoint.
    pub fn new(
        base: StoredValidatedPrefixCheckpointV1,
        entity_counts: ValidatedPrefixEntityTransitionCounts,
        entity_transition_fingerprint: EntityTransitionFingerprint,
    ) -> Result<Self, StorageValueError> {
        let head_count = entity_counts
            .live_entity_count
            .checked_add(entity_counts.deleted_entity_count)
            .ok_or(StorageValueError::SizeOverflow)?;
        if entity_counts.entity_transition_count < head_count {
            return Err(StorageValueError::InvalidShape);
        }
        let mut value = Self {
            base,
            entity_counts,
            entity_transition_fingerprint,
            checkpoint_hash: ValidatedPrefixCheckpointV2Hash::from_bytes([0; 32]),
        };
        value.checkpoint_hash = value.computed_hash();
        Ok(value)
    }

    /// Reconstructs a durable successor and verifies its complete self-hash.
    pub fn from_stored_parts(
        base: StoredValidatedPrefixCheckpointV1,
        entity_counts: ValidatedPrefixEntityTransitionCounts,
        entity_transition_fingerprint: EntityTransitionFingerprint,
        checkpoint_hash: ValidatedPrefixCheckpointV2Hash,
    ) -> Result<Self, StorageValueError> {
        let value = Self::new(base, entity_counts, entity_transition_fingerprint)?;
        if value.checkpoint_hash != checkpoint_hash {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(value)
    }

    /// Borrows the established validated-prefix proof.
    #[must_use]
    pub const fn base(&self) -> &StoredValidatedPrefixCheckpointV1 {
        &self.base
    }

    /// Returns separated current-state and historical counts.
    #[must_use]
    pub const fn entity_counts(&self) -> ValidatedPrefixEntityTransitionCounts {
        self.entity_counts
    }

    /// Returns the complete entity-chain-head fingerprint.
    #[must_use]
    pub const fn entity_transition_fingerprint(&self) -> EntityTransitionFingerprint {
        self.entity_transition_fingerprint
    }

    /// Returns the successor self-hash.
    #[must_use]
    pub const fn checkpoint_hash(&self) -> ValidatedPrefixCheckpointV2Hash {
        self.checkpoint_hash
    }

    /// Recomputes the self-hash over the V1 proof plus all delete-aware fields.
    #[must_use]
    pub fn computed_hash(&self) -> ValidatedPrefixCheckpointV2Hash {
        let mut preimage = Vec::new();
        preimage.extend_from_slice(CHECKPOINT_V2_HASH_LABEL);
        preimage.extend_from_slice(self.base.checkpoint_hash().as_bytes());
        preimage.extend_from_slice(&self.entity_counts.live_entity_count.to_be_bytes());
        preimage.extend_from_slice(&self.entity_counts.deleted_entity_count.to_be_bytes());
        preimage.extend_from_slice(&self.entity_counts.entity_transition_count.to_be_bytes());
        preimage.extend_from_slice(self.entity_transition_fingerprint.as_bytes());
        let digest = hash(HashDomain::Schema, &preimage);
        ValidatedPrefixCheckpointV2Hash::from_bytes(*digest.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_types::DatabaseId;

    fn fingerprint_head(entity_type: u32, key: &[u8], deleted: bool) -> EntityChainHeadV1 {
        use riffdb_types::{
            CommitSequence, EntityKeyBuilder, EntityRecordHash, EntityTransitionHash, EntityTypeId,
        };
        let entity_type = EntityTypeId::new(entity_type).expect("nonzero type");
        let mut builder = EntityKeyBuilder::new(entity_type);
        builder.push_bytes(key).expect("bounded key");
        let target =
            EntityTarget::new(entity_type, builder.finish().expect("key")).expect("target");
        let state = if deleted {
            EntityChainStateV1::Deleted
        } else {
            EntityChainStateV1::Live {
                version: EntityVersion::first(),
                value_hash: EntityRecordHash::from_bytes([0x42; 32]),
            }
        };
        EntityChainHeadV1::from_stored_parts(
            target,
            3,
            state,
            CommitSequence::new(7).expect("sequence"),
            EntityTransitionHash::from_bytes([0x53; 32]),
        )
        .expect("head")
    }

    #[test]
    // req: REP-002
    fn physical_entity_key_order_matches_checkpoint_fingerprint_target_order() {
        let mut heads = Vec::new();
        for entity_type in [u32::MAX, 65536, 65535, 256, 255, 1] {
            for key in [
                &b"\xff"[..],
                &b"\0\xff"[..],
                &b"a\0b"[..],
                &b"a"[..],
                &b""[..],
            ] {
                heads.push(fingerprint_head(entity_type, key, false));
            }
        }
        let mut semantic = heads.clone();
        semantic.sort_by(|left, right| left.target().cmp(right.target()));
        heads.sort_by(|left, right| {
            left.target()
                .key()
                .as_bytes()
                .cmp(right.target().key().as_bytes())
        });
        assert_eq!(heads, semantic);
    }

    #[test]
    // req: REP-002
    fn streaming_checkpoint_fingerprint_preserves_population_preimage_bytes() {
        for count in [0, 1, 2, 257] {
            let heads: Vec<_> = (1..=count)
                .map(|id| fingerprint_head(id, &[0, 0xff, 0x42], id % 2 == 0))
                .collect();
            // Independent frozen predecessor framing, deliberately not the new
            // per-head streaming encoder. This buffer exists only in the oracle.
            let mut bytes = b"riffdb.entity-transition-fingerprint/v1\0".to_vec();
            for head in &heads {
                bytes.extend(head.target().entity_type_id().get().to_be_bytes());
                bytes.extend((head.target().key().as_bytes().len() as u32).to_be_bytes());
                bytes.extend(head.target().key().as_bytes());
                bytes.extend(head.chain_revision().to_be_bytes());
                match head.state() {
                    EntityChainStateV1::Live {
                        version,
                        value_hash,
                    } => {
                        bytes.push(1);
                        bytes.extend(version.get().to_be_bytes());
                        bytes.extend(value_hash.as_bytes());
                    }
                    EntityChainStateV1::Deleted => bytes.push(2),
                    EntityChainStateV1::NeverExisted => panic!("stored head cannot be absent"),
                }
                bytes.extend(head.last_command_sequence().get().to_be_bytes());
                bytes.extend(head.last_transition_hash().as_bytes());
            }
            let expected = EntityTransitionFingerprint::from_bytes(
                *hash(HashDomain::Schema, &bytes).as_bytes(),
            );
            assert_eq!(
                EntityTransitionFingerprint::from_sorted_heads(&heads).expect("borrowed heads"),
                expected
            );
            let mut passes = 0;
            let actual = EntityTransitionFingerprint::from_sorted_head_reader(|| {
                passes += 1;
                Ok(heads.clone().into_iter().map(Ok))
            })
            .expect("owned heads");
            assert_eq!(actual, expected);
            assert_eq!(passes, 2);
        }
    }

    #[test]
    // req: REP-002
    fn streaming_checkpoint_fingerprint_retains_only_one_owned_head() {
        use std::{cell::Cell, rc::Rc};
        struct TrackedHead(EntityChainHeadV1, Rc<Cell<usize>>);
        impl Borrow<EntityChainHeadV1> for TrackedHead {
            fn borrow(&self) -> &EntityChainHeadV1 {
                &self.0
            }
        }
        impl Drop for TrackedHead {
            fn drop(&mut self) {
                self.1.set(self.1.get() - 1);
            }
        }
        let live = Rc::new(Cell::new(0));
        let result = fingerprint_from_passes(
            || {
                let live = live.clone();
                Ok((1..=4096).map(move |id| {
                    assert_eq!(
                        live.get(),
                        0,
                        "prior owned row must be released before the next read"
                    );
                    live.set(1);
                    Ok(TrackedHead(
                        fingerprint_head(id, b"key", id % 2 == 0),
                        live.clone(),
                    ))
                }))
            },
            |error: StorageValueError| error,
        );
        result.expect("bounded streaming");
        assert_eq!(live.get(), 0);
    }

    #[test]
    // req: REP-002
    fn streaming_checkpoint_fingerprint_refuses_read_failure_reordering_and_changed_length() {
        use crate::{StorageError, StorageErrorKind};
        for failing_pass in [1, 2] {
            let mut pass = 0;
            let error = EntityTransitionFingerprint::from_sorted_head_reader(|| {
                pass += 1;
                Ok(std::iter::once(if pass == failing_pass {
                    Err(StorageError::new(StorageErrorKind::Unavailable, None))
                } else {
                    Ok(fingerprint_head(1, b"key", false))
                }))
            })
            .expect_err("read failure preserved");
            assert_eq!(error.kind(), StorageErrorKind::Unavailable);
        }
        let heads = [
            fingerprint_head(2, b"key", false),
            fingerprint_head(1, b"key", true),
        ];
        assert_eq!(
            EntityTransitionFingerprint::from_sorted_heads(&heads),
            Err(StorageValueError::NonCanonicalOrder)
        );
        assert_eq!(
            EntityTransitionFingerprint::from_sorted_heads([&heads[0], &heads[0]]),
            Err(StorageValueError::NonCanonicalOrder)
        );
        for first_deleted in [false, true] {
            let mut pass = 0;
            let error = EntityTransitionFingerprint::from_sorted_head_reader(|| {
                pass += 1;
                Ok(std::iter::once(Ok(fingerprint_head(
                    1,
                    b"key",
                    (pass == 1) == first_deleted,
                ))))
            })
            .expect_err("different pass lengths");
            assert_eq!(error.kind(), StorageErrorKind::CorruptData);
        }
    }

    fn sample_counts() -> ValidatedPrefixSequenceCounts {
        ValidatedPrefixSequenceCounts {
            commits_count: 3,
            events_count: 4,
            event_routes_count: 0,
            outbox_count: 4,
            outbox_status_count: 4,
            idempotency_count: 3,
            audit_count: 1,
            audit_by_request_count: 1,
        }
    }

    fn sample_retained() -> ValidatedPrefixRetainedSnapshot {
        ValidatedPrefixRetainedSnapshot {
            next_application_sequence: 4,
            application_sequence_exhausted: false,
            next_administration_sequence: 2,
            administration_sequence_exhausted: false,
        }
    }

    fn sample_database_id() -> DatabaseId {
        // Valid UUIDv7: version nibble 7, RFC 4122 variant 10xxxxxx.
        let mut bytes = [0_u8; 16];
        bytes[0] = 0x01;
        bytes[6] = 0x70;
        bytes[8] = 0x80;
        DatabaseId::from_bytes(bytes).expect("valid UUIDv7")
    }

    #[test]
    fn checkpoint_self_hash_is_stable_and_verified() {
        let database_id = sample_database_id();
        let checkpoint = StoredValidatedPrefixCheckpointV1::new(
            database_id,
            1,
            SchemaHash::from_bytes([9; 32]),
            3,
            1,
            sample_counts(),
            EntityChainFingerprint::from_bytes([0xab; 32]),
            sample_retained(),
            None,
            0,
        )
        .expect("construct");
        assert_eq!(
            checkpoint.computed_hash().expect("hash"),
            checkpoint.checkpoint_hash()
        );
        let again = StoredValidatedPrefixCheckpointV1::from_stored_parts(
            database_id,
            1,
            SchemaHash::from_bytes([9; 32]),
            3,
            1,
            sample_counts(),
            EntityChainFingerprint::from_bytes([0xab; 32]),
            sample_retained(),
            None,
            checkpoint.checkpoint_hash(),
            0,
        )
        .expect("verified");
        assert_eq!(again, checkpoint);
        let mut bad = *checkpoint.checkpoint_hash().as_bytes();
        bad[0] ^= 0xff;
        assert!(
            StoredValidatedPrefixCheckpointV1::from_stored_parts(
                database_id,
                1,
                SchemaHash::from_bytes([9; 32]),
                3,
                1,
                sample_counts(),
                EntityChainFingerprint::from_bytes([0xab; 32]),
                sample_retained(),
                None,
                ValidatedPrefixCheckpointHash::from_bytes(bad),
                0,
            )
            .is_err()
        );
    }

    #[test]
    fn empty_entity_chain_fingerprint_is_deterministic() {
        let empty = EntityChainFingerprint::from_sorted_pairs(std::iter::empty());
        let again = EntityChainFingerprint::from_sorted_pairs(std::iter::empty());
        assert_eq!(empty, again);
    }
}
