//! Bounded segmented successful-command authority and rebuildable exact indexes.

use std::fmt;

use riffdb_types::{AdministrationSequence, CommitSequence, DatabaseId, MAX_KEY_BYTES};

use crate::{
    CommittedEntityTransitionV1, EntityChainStateV1, IndexEpochAdvanceV1,
    MAX_AFFECTED_INDEX_EPOCH_TARGETS, MAX_ENTITY_MUTATIONS, MAX_EVENT_INTENTS, MAX_STAGED_COMMANDS,
    StorageValueError, StoredCommandCapsuleV1, StoredDurableEventV1,
};

/// Maximum exact derived-index entries retained by one segment or checkpoint.
pub const MAX_COMMAND_SEGMENT_INDEX_ENTRIES: usize = 65_535;

/// Domain-separated digest over one canonical command-segment component.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CommandSegmentDigestV1([u8; 32]);

impl CommandSegmentDigestV1 {
    /// Constructs a digest from exactly 32 verified bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Borrows the exact digest bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for CommandSegmentDigestV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CommandSegmentDigestV1([REDACTED])")
    }
}

/// Closed exact accelerator rebuilt from command authority.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CommandDerivedIndexKindV1 {
    /// Equal-input retry and outcome resolution.
    Idempotency,
    /// Provenance-identity lookup.
    Provenance,
    /// Administration-sequence audit lookup.
    AuditSequence,
    /// Request-scoped audit scan.
    AuditRequest,
    /// Partition-local durable-event routing.
    EventRoute,
    /// Pending outbox work membership.
    PendingOutbox,
}

/// Closed member selected by a derived-index entry.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CommandDerivedMemberV1 {
    /// The command as a whole.
    Command,
    /// The Started audit member.
    AuditStarted,
    /// The successful terminal audit member.
    AuditTerminal,
    /// One event selected by `member_ordinal`.
    Event,
}

/// One exact key-to-segment-member mapping. It carries no business payload.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CommandDerivedIndexManifestEntryV1 {
    kind: CommandDerivedIndexKindV1,
    member: CommandDerivedMemberV1,
    exact_key: Vec<u8>,
    command_ordinal: u16,
    member_ordinal: u16,
    segment_first_commit_sequence: CommitSequence,
}

impl CommandDerivedIndexManifestEntryV1 {
    /// Validates the closed kind/member relation and bounded exact key.
    pub fn new(
        kind: CommandDerivedIndexKindV1,
        member: CommandDerivedMemberV1,
        exact_key: Vec<u8>,
        command_ordinal: u16,
        member_ordinal: u16,
        segment_first_commit_sequence: CommitSequence,
    ) -> Result<Self, StorageValueError> {
        if exact_key.is_empty() || exact_key.len() > MAX_KEY_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        let member_is_valid = match kind {
            CommandDerivedIndexKindV1::Idempotency | CommandDerivedIndexKindV1::Provenance => {
                member == CommandDerivedMemberV1::Command && member_ordinal == 0
            }
            CommandDerivedIndexKindV1::AuditSequence | CommandDerivedIndexKindV1::AuditRequest => {
                matches!(
                    member,
                    CommandDerivedMemberV1::AuditStarted | CommandDerivedMemberV1::AuditTerminal
                ) && member_ordinal == 0
            }
            CommandDerivedIndexKindV1::EventRoute | CommandDerivedIndexKindV1::PendingOutbox => {
                member == CommandDerivedMemberV1::Event
            }
        };
        if !member_is_valid {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(Self {
            kind,
            member,
            exact_key,
            command_ordinal,
            member_ordinal,
            segment_first_commit_sequence,
        })
    }

    /// Returns the exact accelerator role.
    #[must_use]
    pub const fn kind(&self) -> CommandDerivedIndexKindV1 {
        self.kind
    }

    /// Returns the exact segment member role.
    #[must_use]
    pub const fn member(&self) -> CommandDerivedMemberV1 {
        self.member
    }

    /// Borrows the complete physical lookup key.
    #[must_use]
    pub fn exact_key(&self) -> &[u8] {
        &self.exact_key
    }

    /// Returns the zero-based command ordinal.
    #[must_use]
    pub const fn command_ordinal(&self) -> u16 {
        self.command_ordinal
    }

    /// Returns the zero-based member ordinal for event entries.
    #[must_use]
    pub const fn member_ordinal(&self) -> u16 {
        self.member_ordinal
    }

    /// Returns the containing segment's first command sequence.
    #[must_use]
    pub const fn segment_first_commit_sequence(&self) -> CommitSequence {
        self.segment_first_commit_sequence
    }
}

impl fmt::Debug for CommandDerivedIndexManifestEntryV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommandDerivedIndexManifestEntryV1")
            .field("kind", &self.kind)
            .field("member", &self.member)
            .field("exact_key", &"[REDACTED]")
            .field("command_ordinal", &self.command_ordinal)
            .field("member_ordinal", &self.member_ordinal)
            .field(
                "segment_first_commit_sequence",
                &self.segment_first_commit_sequence,
            )
            .finish()
    }
}

/// Canonical, duplicate-free exact-index manifest for one segment.
#[derive(Clone, Eq, PartialEq)]
pub struct CommandSegmentManifestV1 {
    entries: Vec<CommandDerivedIndexManifestEntryV1>,
}

impl CommandSegmentManifestV1 {
    /// Accepts only a nonempty, strictly canonical entry sequence.
    pub fn new(
        entries: Vec<CommandDerivedIndexManifestEntryV1>,
    ) -> Result<Self, StorageValueError> {
        if entries.is_empty() || entries.len() > MAX_COMMAND_SEGMENT_INDEX_ENTRIES {
            return Err(StorageValueError::LimitExceeded);
        }
        if entries.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(StorageValueError::NonCanonicalOrder);
        }
        Ok(Self { entries })
    }

    /// Borrows entries in exact canonical order.
    #[must_use]
    pub fn entries(&self) -> &[CommandDerivedIndexManifestEntryV1] {
        &self.entries
    }
}

impl fmt::Debug for CommandSegmentManifestV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CommandSegmentManifestV1")
            .field("entry_count", &self.entries.len())
            .finish()
    }
}

/// Successful-command capsule with full immutable events and logical generations.
#[derive(Clone, Eq, PartialEq)]
pub struct StoredCommandCapsuleV2 {
    base: StoredCommandCapsuleV1,
    index_generation_transitions: Vec<IndexEpochAdvanceV1>,
    entity_transitions: Vec<CommittedEntityTransitionV1>,
}

impl StoredCommandCapsuleV2 {
    /// Proves event reciprocity and canonical logical-generation ordering.
    pub fn new(
        base: StoredCommandCapsuleV1,
        events: Vec<StoredDurableEventV1>,
        index_generation_transitions: Vec<IndexEpochAdvanceV1>,
    ) -> Result<Self, StorageValueError> {
        if base.commit().events() != events {
            return Err(StorageValueError::IdentityMismatch);
        }
        Self::from_base(base, index_generation_transitions)
    }

    /// Constructs a V2 capsule from a base that already owns the complete
    /// checked durable-event collection.
    pub fn from_base(
        base: StoredCommandCapsuleV1,
        index_generation_transitions: Vec<IndexEpochAdvanceV1>,
    ) -> Result<Self, StorageValueError> {
        Self::from_base_with_entity_transitions(base, index_generation_transitions, Vec::new())
    }

    /// Constructs current command authority with exact entity-chain transitions.
    pub fn from_base_with_entity_transitions(
        base: StoredCommandCapsuleV1,
        index_generation_transitions: Vec<IndexEpochAdvanceV1>,
        entity_transitions: Vec<CommittedEntityTransitionV1>,
    ) -> Result<Self, StorageValueError> {
        if base.commit().events().len() > MAX_EVENT_INTENTS
            || index_generation_transitions.len() > MAX_AFFECTED_INDEX_EPOCH_TARGETS
            || entity_transitions.len() > MAX_ENTITY_MUTATIONS
            || index_generation_transitions
                .windows(2)
                .any(|pair| pair[0].target() >= pair[1].target())
            || entity_transitions
                .iter()
                .enumerate()
                .any(|(ordinal, transition)| {
                    transition.command_sequence() != base.commit_sequence()
                        || usize::try_from(transition.mutation_ordinal()).ok() != Some(ordinal)
                })
            || entity_transitions
                .windows(2)
                .any(|pair| pair[0].target() >= pair[1].target())
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        if entity_transitions.is_empty() {
            if base.provenance().affected_entities().len()
                != base.commit().entity_references().len()
                || base
                    .provenance()
                    .affected_entities()
                    .iter()
                    .zip(base.commit().entity_references())
                    .any(|(affected, reference)| {
                        affected.target() != reference.target()
                            || affected.entity_version() != reference.entity_version()
                    })
            {
                return Err(StorageValueError::IdentityMismatch);
            }
        } else {
            if base.provenance().affected_entities().len() != entity_transitions.len()
                || base
                    .provenance()
                    .affected_entities()
                    .iter()
                    .zip(&entity_transitions)
                    .any(|(affected, transition)| {
                        if affected.target() != transition.target() {
                            return true;
                        }
                        let expected_version = match transition.next_state() {
                            EntityChainStateV1::Live { version, .. } => Some(version),
                            EntityChainStateV1::Deleted => match transition.prior_state() {
                                EntityChainStateV1::Live { version, .. } => Some(version),
                                EntityChainStateV1::Deleted | EntityChainStateV1::NeverExisted => {
                                    None
                                }
                            },
                            EntityChainStateV1::NeverExisted => None,
                        };
                        expected_version != Some(affected.entity_version())
                    })
            {
                return Err(StorageValueError::IdentityMismatch);
            }
            let live = entity_transitions
                .iter()
                .filter_map(|transition| match transition.next_state() {
                    EntityChainStateV1::Live {
                        version,
                        value_hash,
                    } => Some((transition.target(), version, value_hash)),
                    EntityChainStateV1::Deleted | EntityChainStateV1::NeverExisted => None,
                })
                .collect::<Vec<_>>();
            let references = base.commit().entity_references();
            if live.len() != references.len()
                || live
                    .iter()
                    .zip(references)
                    .any(|((target, version, value_hash), reference)| {
                        *target != reference.target()
                            || *version != reference.entity_version()
                            || *value_hash != reference.post_image_hash()
                    })
            {
                return Err(StorageValueError::IdentityMismatch);
            }
        }
        Ok(Self {
            base,
            index_generation_transitions,
            entity_transitions,
        })
    }

    /// Borrows the established five semantic command views.
    #[must_use]
    pub const fn base(&self) -> &StoredCommandCapsuleV1 {
        &self.base
    }

    /// Returns the command sequence.
    #[must_use]
    pub const fn commit_sequence(&self) -> CommitSequence {
        self.base.commit_sequence()
    }

    /// Borrows full durable events in ordinal order.
    #[must_use]
    pub fn events(&self) -> &[StoredDurableEventV1] {
        self.base.commit().events()
    }

    /// Borrows exact logical generation transitions in target order.
    #[must_use]
    pub fn index_generation_transitions(&self) -> &[IndexEpochAdvanceV1] {
        &self.index_generation_transitions
    }

    /// Borrows exact entity-chain transitions in mutation ordinal order.
    #[must_use]
    pub fn entity_transitions(&self) -> &[CommittedEntityTransitionV1] {
        &self.entity_transitions
    }
}

impl fmt::Debug for StoredCommandCapsuleV2 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StoredCommandCapsuleV2([REDACTED])")
    }
}

/// One bounded contiguous successful-command segment.
#[derive(Clone, Eq, PartialEq)]
pub struct StoredCommandSegmentV1 {
    database_id: DatabaseId,
    history_incarnation: u64,
    predecessor_segment_digest: Option<CommandSegmentDigestV1>,
    commands: Vec<StoredCommandCapsuleV2>,
    manifest: CommandSegmentManifestV1,
    segment_digest: CommandSegmentDigestV1,
}

impl StoredCommandSegmentV1 {
    /// Validates segment identity, contiguous command order, and manifest bounds.
    pub fn new(
        database_id: DatabaseId,
        history_incarnation: u64,
        predecessor_segment_digest: Option<CommandSegmentDigestV1>,
        commands: Vec<StoredCommandCapsuleV2>,
        manifest: CommandSegmentManifestV1,
        segment_digest: CommandSegmentDigestV1,
    ) -> Result<Self, StorageValueError> {
        if history_incarnation < crate::HISTORY_INCARNATION_INITIAL
            || commands.is_empty()
            || commands.len() > MAX_STAGED_COMMANDS
        {
            return Err(StorageValueError::LimitExceeded);
        }
        for pair in commands.windows(2) {
            if pair[0].commit_sequence().checked_next() != Some(pair[1].commit_sequence()) {
                return Err(StorageValueError::NonCanonicalOrder);
            }
        }
        if manifest.entries().iter().any(|entry| {
            entry.segment_first_commit_sequence() != commands[0].commit_sequence()
                || usize::from(entry.command_ordinal()) >= commands.len()
                || (entry.member() == CommandDerivedMemberV1::Event
                    && usize::from(entry.member_ordinal())
                        >= commands[usize::from(entry.command_ordinal())]
                            .events()
                            .len())
        }) {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(Self {
            database_id,
            history_incarnation,
            predecessor_segment_digest,
            commands,
            manifest,
            segment_digest,
        })
    }

    /// Returns the database identity bound into the hash chain.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Returns the retained-history incarnation.
    #[must_use]
    pub const fn history_incarnation(&self) -> u64 {
        self.history_incarnation
    }

    /// Returns the predecessor segment digest, absent only at the retained root.
    #[must_use]
    pub const fn predecessor_segment_digest(&self) -> Option<CommandSegmentDigestV1> {
        self.predecessor_segment_digest
    }

    /// Borrows commands in contiguous sequence order.
    #[must_use]
    pub fn commands(&self) -> &[StoredCommandCapsuleV2] {
        &self.commands
    }

    /// Borrows the exact rebuild manifest.
    #[must_use]
    pub const fn manifest(&self) -> &CommandSegmentManifestV1 {
        &self.manifest
    }

    /// Returns the verified segment digest.
    #[must_use]
    pub const fn segment_digest(&self) -> CommandSegmentDigestV1 {
        self.segment_digest
    }

    pub(crate) fn with_segment_digest(mut self, segment_digest: CommandSegmentDigestV1) -> Self {
        self.segment_digest = segment_digest;
        self
    }

    /// Returns the first command sequence.
    #[must_use]
    pub fn first_commit_sequence(&self) -> CommitSequence {
        self.commands[0].commit_sequence()
    }

    /// Returns the last command sequence.
    #[must_use]
    pub fn last_commit_sequence(&self) -> CommitSequence {
        self.commands
            .last()
            .expect("constructor requires a nonempty segment")
            .commit_sequence()
    }

    /// Returns the minimum administration sequence represented by the segment.
    #[must_use]
    pub fn first_administration_sequence(&self) -> AdministrationSequence {
        self.commands
            .iter()
            .flat_map(|command| {
                [
                    command.base().started_audit().administration_sequence(),
                    command.base().terminal_audit().administration_sequence(),
                ]
            })
            .min()
            .expect("constructor requires a nonempty segment")
    }

    /// Returns the maximum administration sequence represented by the segment.
    #[must_use]
    pub fn last_administration_sequence(&self) -> AdministrationSequence {
        self.commands
            .iter()
            .flat_map(|command| {
                [
                    command.base().started_audit().administration_sequence(),
                    command.base().terminal_audit().administration_sequence(),
                ]
            })
            .max()
            .expect("constructor requires a nonempty segment")
    }
}

impl fmt::Debug for StoredCommandSegmentV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoredCommandSegmentV1")
            .field("command_count", &self.commands.len())
            .field("first_commit_sequence", &self.first_commit_sequence())
            .field("last_commit_sequence", &self.last_commit_sequence())
            .finish_non_exhaustive()
    }
}

/// Derived exact-index snapshot bound to one validated segment frontier.
#[derive(Clone, Eq, PartialEq)]
pub struct StoredCommandDerivedIndexCheckpointV1 {
    database_id: DatabaseId,
    history_incarnation: u64,
    registry_digest: [u8; 32],
    segment_frontier: CommitSequence,
    segment_root_digest: CommandSegmentDigestV1,
    entries: Vec<CommandDerivedIndexManifestEntryV1>,
    checkpoint_digest: CommandSegmentDigestV1,
}

impl StoredCommandDerivedIndexCheckpointV1 {
    /// Validates an exact canonical checkpoint snapshot.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        database_id: DatabaseId,
        history_incarnation: u64,
        registry_digest: [u8; 32],
        segment_frontier: CommitSequence,
        segment_root_digest: CommandSegmentDigestV1,
        entries: Vec<CommandDerivedIndexManifestEntryV1>,
        checkpoint_digest: CommandSegmentDigestV1,
    ) -> Result<Self, StorageValueError> {
        if history_incarnation < crate::HISTORY_INCARNATION_INITIAL
            || entries.len() > MAX_COMMAND_SEGMENT_INDEX_ENTRIES
            || entries.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(StorageValueError::NonCanonicalOrder);
        }
        Ok(Self {
            database_id,
            history_incarnation,
            registry_digest,
            segment_frontier,
            segment_root_digest,
            entries,
            checkpoint_digest,
        })
    }

    /// Returns the database identity.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Returns the retained-history incarnation.
    #[must_use]
    pub const fn history_incarnation(&self) -> u64 {
        self.history_incarnation
    }

    /// Returns the exact writable-registry digest.
    #[must_use]
    pub const fn registry_digest(&self) -> [u8; 32] {
        self.registry_digest
    }

    /// Returns the last segment command sequence covered by the snapshot.
    #[must_use]
    pub const fn segment_frontier(&self) -> CommitSequence {
        self.segment_frontier
    }

    /// Returns the segment-chain root at the covered frontier.
    #[must_use]
    pub const fn segment_root_digest(&self) -> CommandSegmentDigestV1 {
        self.segment_root_digest
    }

    /// Borrows all exact derived-index entries in canonical order.
    #[must_use]
    pub fn entries(&self) -> &[CommandDerivedIndexManifestEntryV1] {
        &self.entries
    }

    /// Returns the verified checkpoint digest.
    #[must_use]
    pub const fn checkpoint_digest(&self) -> CommandSegmentDigestV1 {
        self.checkpoint_digest
    }
}

impl fmt::Debug for StoredCommandDerivedIndexCheckpointV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoredCommandDerivedIndexCheckpointV1")
            .field("segment_frontier", &self.segment_frontier)
            .field("entry_count", &self.entries.len())
            .finish_non_exhaustive()
    }
}
