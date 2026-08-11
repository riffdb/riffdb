//! Delete-aware authoritative entity-chain transitions (ADR-0100 Amendment 2).
//!
//! These values are engine-neutral. The commit coordinator derives them and
//! storage/changelog code consumes them; no public application request can
//! submit a transition or chain head.

use std::fmt;

use riffdb_types::{
    CommitSequence, EntityKey, EntityRecordHash, EntityTransitionHash, EntityTypeId, EntityVersion,
    hash_entity_transition,
};

use crate::{EntityTarget, StorageValueError};

const TRANSITION_FORMAT_V1: u8 = 1;

/// Closed state of one entity identity at a chain boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EntityChainStateV1 {
    /// The identity has never had an authoritative transition.
    NeverExisted,
    /// The identity has one exact materialized post-image.
    Live {
        /// Entity-local version of the current post-image.
        version: EntityVersion,
        /// Hash of the complete canonical current post-image.
        value_hash: EntityRecordHash,
    },
    /// The identity existed but its current materialized row was deleted.
    Deleted,
}

impl EntityChainStateV1 {
    const fn tag(self) -> u8 {
        match self {
            Self::NeverExisted => 0,
            Self::Live { .. } => 1,
            Self::Deleted => 2,
        }
    }
}

/// One immutable command-attributed entity transition.
#[derive(Clone, Eq, PartialEq)]
pub struct CommittedEntityTransitionV1 {
    command_sequence: CommitSequence,
    mutation_ordinal: u32,
    target: EntityTarget,
    prior_state: EntityChainStateV1,
    prior_chain_revision: u64,
    prior_transition_hash: Option<EntityTransitionHash>,
    next_state: EntityChainStateV1,
    transition_hash: EntityTransitionHash,
}

impl CommittedEntityTransitionV1 {
    /// Constructs and hashes one checked transition.
    pub fn new(
        command_sequence: CommitSequence,
        mutation_ordinal: u32,
        target: EntityTarget,
        prior_state: EntityChainStateV1,
        prior_chain_revision: u64,
        prior_transition_hash: Option<EntityTransitionHash>,
        next_state: EntityChainStateV1,
    ) -> Result<Self, StorageValueError> {
        validate_transition_shape(
            prior_state,
            prior_chain_revision,
            prior_transition_hash,
            next_state,
        )?;
        let mut value = Self {
            command_sequence,
            mutation_ordinal,
            target,
            prior_state,
            prior_chain_revision,
            prior_transition_hash,
            next_state,
            transition_hash: EntityTransitionHash::from_bytes([0; 32]),
        };
        value.transition_hash = hash_entity_transition(&value.canonical_preimage()?);
        Ok(value)
    }

    /// Reconstructs a stored transition and proves its retained hash.
    #[allow(clippy::too_many_arguments)]
    pub fn from_stored_parts(
        command_sequence: CommitSequence,
        mutation_ordinal: u32,
        target: EntityTarget,
        prior_state: EntityChainStateV1,
        prior_chain_revision: u64,
        prior_transition_hash: Option<EntityTransitionHash>,
        next_state: EntityChainStateV1,
        transition_hash: EntityTransitionHash,
    ) -> Result<Self, StorageValueError> {
        let value = Self::new(
            command_sequence,
            mutation_ordinal,
            target,
            prior_state,
            prior_chain_revision,
            prior_transition_hash,
            next_state,
        )?;
        if value.transition_hash != transition_hash {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(value)
    }

    /// Returns the command sequence carrying this transition.
    #[must_use]
    pub const fn command_sequence(&self) -> CommitSequence {
        self.command_sequence
    }

    /// Returns the mutation ordinal within its command.
    #[must_use]
    pub const fn mutation_ordinal(&self) -> u32 {
        self.mutation_ordinal
    }

    /// Borrows the exact entity identity.
    #[must_use]
    pub const fn target(&self) -> &EntityTarget {
        &self.target
    }

    /// Returns the required prior chain state.
    #[must_use]
    pub const fn prior_state(&self) -> EntityChainStateV1 {
        self.prior_state
    }

    /// Returns the required prior chain revision (zero only for never-existed).
    #[must_use]
    pub const fn prior_chain_revision(&self) -> u64 {
        self.prior_chain_revision
    }

    /// Returns the required preceding transition hash.
    #[must_use]
    pub const fn prior_transition_hash(&self) -> Option<EntityTransitionHash> {
        self.prior_transition_hash
    }

    /// Returns the complete next chain state.
    #[must_use]
    pub const fn next_state(&self) -> EntityChainStateV1 {
        self.next_state
    }

    /// Returns the domain-separated hash of the complete transition.
    #[must_use]
    pub const fn transition_hash(&self) -> EntityTransitionHash {
        self.transition_hash
    }

    /// Returns the canonical transition preimage used by the hash domain.
    pub fn canonical_preimage(&self) -> Result<Vec<u8>, StorageValueError> {
        let key = self.target.key().as_bytes();
        let key_len = u32::try_from(key.len()).map_err(|_| StorageValueError::LimitExceeded)?;
        let mut bytes = Vec::with_capacity(128 + key.len());
        bytes.push(TRANSITION_FORMAT_V1);
        bytes.extend_from_slice(&self.command_sequence.to_be_bytes());
        bytes.extend_from_slice(&self.mutation_ordinal.to_be_bytes());
        bytes.extend_from_slice(&self.target.entity_type_id().to_be_bytes());
        bytes.extend_from_slice(&key_len.to_be_bytes());
        bytes.extend_from_slice(key);
        encode_state(&mut bytes, self.prior_state);
        bytes.extend_from_slice(&self.prior_chain_revision.to_be_bytes());
        match self.prior_transition_hash {
            Some(hash) => {
                bytes.push(1);
                bytes.extend_from_slice(hash.as_bytes());
            }
            None => bytes.push(0),
        }
        encode_state(&mut bytes, self.next_state);
        Ok(bytes)
    }

    /// Decodes one exact canonical preimage and proves its retained hash.
    pub fn decode_canonical_preimage(
        bytes: &[u8],
        transition_hash: EntityTransitionHash,
    ) -> Result<Self, StorageValueError> {
        let mut cursor = TransitionCursor::new(bytes);
        if cursor.take_u8()? != TRANSITION_FORMAT_V1 {
            return Err(StorageValueError::InvalidShape);
        }
        let command_sequence =
            CommitSequence::new(cursor.take_u64()?).ok_or(StorageValueError::InvalidShape)?;
        let mutation_ordinal = cursor.take_u32()?;
        let entity_type_id =
            EntityTypeId::new(cursor.take_u32()?).ok_or(StorageValueError::InvalidShape)?;
        let key_len =
            usize::try_from(cursor.take_u32()?).map_err(|_| StorageValueError::LimitExceeded)?;
        let key = EntityKey::from_bytes(cursor.take(key_len)?.to_vec())
            .map_err(|_| StorageValueError::InvalidShape)?;
        let target = EntityTarget::new(entity_type_id, key)?;
        let prior_state = decode_state(&mut cursor)?;
        let prior_chain_revision = cursor.take_u64()?;
        let prior_transition_hash = match cursor.take_u8()? {
            0 => None,
            1 => Some(EntityTransitionHash::from_bytes(cursor.take_array()?)),
            _ => return Err(StorageValueError::InvalidShape),
        };
        let next_state = decode_state(&mut cursor)?;
        if !cursor.is_complete() {
            return Err(StorageValueError::InvalidShape);
        }
        Self::from_stored_parts(
            command_sequence,
            mutation_ordinal,
            target,
            prior_state,
            prior_chain_revision,
            prior_transition_hash,
            next_state,
            transition_hash,
        )
    }
}

impl fmt::Debug for CommittedEntityTransitionV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommittedEntityTransitionV1")
            .field("command_sequence", &self.command_sequence)
            .field("mutation_ordinal", &self.mutation_ordinal)
            .field("target", &self.target)
            .field("prior_state", &self.prior_state)
            .field("prior_chain_revision", &self.prior_chain_revision)
            .field("next_state", &self.next_state)
            .field("transition_hash", &self.transition_hash)
            .finish()
    }
}

/// Authoritative current chain state for one entity identity.
#[derive(Clone, Eq, PartialEq)]
pub struct EntityChainHeadV1 {
    target: EntityTarget,
    chain_revision: u64,
    state: EntityChainStateV1,
    last_command_sequence: CommitSequence,
    last_transition_hash: EntityTransitionHash,
}

impl EntityChainHeadV1 {
    /// Builds the first head for one never-before-transitioned identity.
    pub fn from_genesis(
        transition: &CommittedEntityTransitionV1,
    ) -> Result<Self, StorageValueError> {
        if transition.prior_state != EntityChainStateV1::NeverExisted
            || transition.prior_chain_revision != 0
            || transition.prior_transition_hash.is_some()
        {
            return Err(StorageValueError::InvalidShape);
        }
        Ok(Self {
            target: transition.target.clone(),
            chain_revision: 1,
            state: transition.next_state,
            last_command_sequence: transition.command_sequence,
            last_transition_hash: transition.transition_hash,
        })
    }

    /// Reconstructs one stored head after checking its closed shape.
    pub fn from_stored_parts(
        target: EntityTarget,
        chain_revision: u64,
        state: EntityChainStateV1,
        last_command_sequence: CommitSequence,
        last_transition_hash: EntityTransitionHash,
    ) -> Result<Self, StorageValueError> {
        if chain_revision == 0 || state == EntityChainStateV1::NeverExisted {
            return Err(StorageValueError::InvalidShape);
        }
        Ok(Self {
            target,
            chain_revision,
            state,
            last_command_sequence,
            last_transition_hash,
        })
    }

    /// Advances this head only when the transition names its exact predecessor.
    pub fn apply(
        &self,
        transition: &CommittedEntityTransitionV1,
    ) -> Result<Self, StorageValueError> {
        if self.target != transition.target
            || self.chain_revision != transition.prior_chain_revision
            || self.state != transition.prior_state
            || Some(self.last_transition_hash) != transition.prior_transition_hash
            || transition.command_sequence <= self.last_command_sequence
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        let chain_revision = self
            .chain_revision
            .checked_add(1)
            .ok_or(StorageValueError::SizeOverflow)?;
        Ok(Self {
            target: self.target.clone(),
            chain_revision,
            state: transition.next_state,
            last_command_sequence: transition.command_sequence,
            last_transition_hash: transition.transition_hash,
        })
    }

    /// Borrows the exact entity identity.
    #[must_use]
    pub const fn target(&self) -> &EntityTarget {
        &self.target
    }

    /// Returns the monotonic chain revision.
    #[must_use]
    pub const fn chain_revision(&self) -> u64 {
        self.chain_revision
    }

    /// Returns the current live/deleted state.
    #[must_use]
    pub const fn state(&self) -> EntityChainStateV1 {
        self.state
    }

    /// Returns the sequence of the terminal transition.
    #[must_use]
    pub const fn last_command_sequence(&self) -> CommitSequence {
        self.last_command_sequence
    }

    /// Returns the terminal transition hash.
    #[must_use]
    pub const fn last_transition_hash(&self) -> EntityTransitionHash {
        self.last_transition_hash
    }
}

impl fmt::Debug for EntityChainHeadV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EntityChainHeadV1")
            .field("target", &self.target)
            .field("chain_revision", &self.chain_revision)
            .field("state", &self.state)
            .field("last_command_sequence", &self.last_command_sequence)
            .field("last_transition_hash", &self.last_transition_hash)
            .finish()
    }
}

fn validate_transition_shape(
    prior_state: EntityChainStateV1,
    prior_chain_revision: u64,
    prior_transition_hash: Option<EntityTransitionHash>,
    next_state: EntityChainStateV1,
) -> Result<(), StorageValueError> {
    let prior_is_genesis = prior_state == EntityChainStateV1::NeverExisted;
    if prior_is_genesis != (prior_chain_revision == 0)
        || prior_is_genesis != prior_transition_hash.is_none()
        || next_state == EntityChainStateV1::NeverExisted
    {
        return Err(StorageValueError::InvalidShape);
    }
    match (prior_state, next_state) {
        (EntityChainStateV1::NeverExisted, EntityChainStateV1::Live { version, .. })
        | (EntityChainStateV1::Deleted, EntityChainStateV1::Live { version, .. })
            if version == EntityVersion::first() =>
        {
            Ok(())
        }
        (
            EntityChainStateV1::Live { version: prior, .. },
            EntityChainStateV1::Live { version: next, .. },
        ) if prior.checked_next() == Some(next) => Ok(()),
        (EntityChainStateV1::Live { .. }, EntityChainStateV1::Deleted) => Ok(()),
        _ => Err(StorageValueError::InvalidShape),
    }
}

fn encode_state(bytes: &mut Vec<u8>, state: EntityChainStateV1) {
    bytes.push(state.tag());
    if let EntityChainStateV1::Live {
        version,
        value_hash,
    } = state
    {
        bytes.extend_from_slice(&version.to_be_bytes());
        bytes.extend_from_slice(value_hash.as_bytes());
    }
}

fn decode_state(
    cursor: &mut TransitionCursor<'_>,
) -> Result<EntityChainStateV1, StorageValueError> {
    match cursor.take_u8()? {
        0 => Ok(EntityChainStateV1::NeverExisted),
        1 => Ok(EntityChainStateV1::Live {
            version: EntityVersion::new(cursor.take_u64()?)
                .ok_or(StorageValueError::InvalidShape)?,
            value_hash: EntityRecordHash::from_bytes(cursor.take_array()?),
        }),
        2 => Ok(EntityChainStateV1::Deleted),
        _ => Err(StorageValueError::InvalidShape),
    }
}

struct TransitionCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> TransitionCursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], StorageValueError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(StorageValueError::SizeOverflow)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(StorageValueError::InvalidShape)?;
        self.offset = end;
        Ok(value)
    }

    fn take_u8(&mut self) -> Result<u8, StorageValueError> {
        self.take(1)?
            .first()
            .copied()
            .ok_or(StorageValueError::InvalidShape)
    }

    fn take_u32(&mut self) -> Result<u32, StorageValueError> {
        Ok(u32::from_be_bytes(self.take_array()?))
    }

    fn take_u64(&mut self) -> Result<u64, StorageValueError> {
        Ok(u64::from_be_bytes(self.take_array()?))
    }

    fn take_array<const N: usize>(&mut self) -> Result<[u8; N], StorageValueError> {
        self.take(N)?
            .try_into()
            .map_err(|_| StorageValueError::InvalidShape)
    }

    fn is_complete(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use riffdb_types::{EntityKeyBuilder, EntityRecordHash, EntityTypeId};

    use super::*;

    fn target() -> EntityTarget {
        let entity_type = EntityTypeId::new(7).expect("entity type");
        let mut key = EntityKeyBuilder::new(entity_type);
        key.push_u64(11).expect("key component");
        EntityTarget::new(entity_type, key.finish().expect("key")).expect("target")
    }

    fn live(version: u64, hash: u8) -> EntityChainStateV1 {
        EntityChainStateV1::Live {
            version: EntityVersion::new(version).expect("version"),
            value_hash: EntityRecordHash::from_bytes([hash; 32]),
        }
    }

    #[test]
    fn create_update_delete_recreate_chain_is_exact_and_monotonic() {
        let create = CommittedEntityTransitionV1::new(
            CommitSequence::new(1).expect("sequence"),
            0,
            target(),
            EntityChainStateV1::NeverExisted,
            0,
            None,
            live(1, 1),
        )
        .expect("create");
        let first = EntityChainHeadV1::from_genesis(&create).expect("head");
        let update = CommittedEntityTransitionV1::new(
            CommitSequence::new(2).expect("sequence"),
            0,
            target(),
            first.state(),
            first.chain_revision(),
            Some(first.last_transition_hash()),
            live(2, 2),
        )
        .expect("update");
        let second = first.apply(&update).expect("second");
        let delete = CommittedEntityTransitionV1::new(
            CommitSequence::new(3).expect("sequence"),
            0,
            target(),
            second.state(),
            second.chain_revision(),
            Some(second.last_transition_hash()),
            EntityChainStateV1::Deleted,
        )
        .expect("delete");
        let deleted = second.apply(&delete).expect("deleted");
        let recreate = CommittedEntityTransitionV1::new(
            CommitSequence::new(4).expect("sequence"),
            0,
            target(),
            deleted.state(),
            deleted.chain_revision(),
            Some(deleted.last_transition_hash()),
            live(1, 4),
        )
        .expect("recreate");
        let recreated = deleted.apply(&recreate).expect("recreated");
        assert_eq!(recreated.chain_revision(), 4);
        assert_eq!(recreated.state(), live(1, 4));
    }

    #[test]
    fn stale_prior_and_hash_substitution_fail_closed() {
        let create = CommittedEntityTransitionV1::new(
            CommitSequence::new(1).expect("sequence"),
            0,
            target(),
            EntityChainStateV1::NeverExisted,
            0,
            None,
            live(1, 1),
        )
        .expect("create");
        let head = EntityChainHeadV1::from_genesis(&create).expect("head");
        let stale = CommittedEntityTransitionV1::new(
            CommitSequence::new(2).expect("sequence"),
            0,
            target(),
            live(1, 9),
            head.chain_revision(),
            Some(head.last_transition_hash()),
            live(2, 2),
        )
        .expect("structurally valid stale transition");
        assert_eq!(head.apply(&stale), Err(StorageValueError::IdentityMismatch));
        assert!(
            CommittedEntityTransitionV1::from_stored_parts(
                create.command_sequence(),
                create.mutation_ordinal(),
                target(),
                create.prior_state(),
                create.prior_chain_revision(),
                create.prior_transition_hash(),
                create.next_state(),
                EntityTransitionHash::from_bytes([0xff; 32]),
            )
            .is_err()
        );
        let encoded = create.canonical_preimage().expect("preimage");
        assert_eq!(
            CommittedEntityTransitionV1::decode_canonical_preimage(
                &encoded,
                create.transition_hash(),
            )
            .expect("decode"),
            create
        );
        let mut trailing = encoded;
        trailing.push(0);
        assert!(
            CommittedEntityTransitionV1::decode_canonical_preimage(
                &trailing,
                create.transition_hash(),
            )
            .is_err()
        );
    }
}
