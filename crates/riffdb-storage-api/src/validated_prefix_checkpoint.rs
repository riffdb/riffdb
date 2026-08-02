//! Proof-carrying validated-prefix startup checkpoint (ADR-0019 Amendment 1 / ADR-0085 A1).

use riffdb_types::{DatabaseId, EntityVersion, HashDomain, SchemaHash, hash};

use crate::{EntityTarget, StorageValueError};

/// Domain label framed into the checkpoint self-hash preimage.
const CHECKPOINT_HASH_LABEL: &[u8] = b"riffdb.validated-prefix-checkpoint/v1\0";
/// Domain label framed into the entity-chain fingerprint preimage.
const ENTITY_CHAIN_FINGERPRINT_LABEL: &[u8] = b"riffdb.entity-chain-fingerprint/v1\0";

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
    /// Terminal idempotency rows whose outcome commit sequence ≤ S.
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

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_types::DatabaseId;

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
