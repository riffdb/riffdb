//! Delete-aware changelog V2 framing (ADR-0100 Amendment 2).
//!
//! V1 stays byte-immutable and put-only. V2 has a disjoint magic/version/hash
//! chain and adds one semantic entity-delete tombstone class plus final
//! entity-chain-head puts. Negotiation and the receipted rotation boundary are
//! explicit; no cursor crosses formats by inference.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use riffdb_types::{DUAL_FRONTIER_BYTES, DatabaseId, DualFrontier, EntityTransitionHash};
use sha2::{Digest, Sha256};

use crate::{
    CommittedEntityTransitionV1, CompositeTableV1, EntityTransitionFingerprint,
    MAX_CHANGELOG_FRAME_BYTES, MAX_CHANGELOG_FRAME_ENTRIES, ValidatedPrefixEntityTransitionCounts,
};

const FRAME_MAGIC: [u8; 8] = *b"RDBCLF02";
const FOOTER_MAGIC: [u8; 8] = *b"RDBCLE02";
const FORMAT_VERSION: u16 = 2;
const HASH_BYTES: usize = 32;
const FLAG_JOURNALED: u8 = 1;
const ENTRY_CLASS_COUNT: usize = 10;
const HEADER_BYTES: usize = 8
    + 2
    + 1
    + 1
    + 16
    + 8
    + 2 * DUAL_FRONTIER_BYTES
    + 4
    + 4 * ENTRY_CLASS_COUNT
    + 4
    + HASH_BYTES
    + HASH_BYTES;
const ENTRY_PREFIX_BYTES: usize = 12;
const FOOTER_BYTES: usize = 8 + 8 + HASH_BYTES;

/// Closed entry classes accepted by a delete-aware frame.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum ChangelogEntryClassV2 {
    /// One command segment or legacy commit record.
    Commit = 1,
    /// One durable event.
    Event = 2,
    /// One event route.
    EventRoute = 3,
    /// One outbox intent.
    OutboxIntent = 4,
    /// One provenance record.
    Provenance = 5,
    /// One final live entity post-image.
    Entity = 6,
    /// One administration audit row.
    AdministrationAudit = 7,
    /// One service-audit request index row.
    ServiceAuditRequestIndex = 8,
    /// One exact `Live -> Deleted` entity transition.
    EntityDeleteTombstone = 9,
    /// One final authoritative live/deleted entity-chain head.
    EntityChainHead = 10,
}

impl ChangelogEntryClassV2 {
    /// Every class in stable tag order.
    pub const ALL: [Self; ENTRY_CLASS_COUNT] = [
        Self::Commit,
        Self::Event,
        Self::EventRoute,
        Self::OutboxIntent,
        Self::Provenance,
        Self::Entity,
        Self::AdministrationAudit,
        Self::ServiceAuditRequestIndex,
        Self::EntityDeleteTombstone,
        Self::EntityChainHead,
    ];

    /// Decodes the closed tag.
    #[must_use]
    pub const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::Commit),
            2 => Some(Self::Event),
            3 => Some(Self::EventRoute),
            4 => Some(Self::OutboxIntent),
            5 => Some(Self::Provenance),
            6 => Some(Self::Entity),
            7 => Some(Self::AdministrationAudit),
            8 => Some(Self::ServiceAuditRequestIndex),
            9 => Some(Self::EntityDeleteTombstone),
            10 => Some(Self::EntityChainHead),
            _ => None,
        }
    }

    /// Returns the existing put-only table, if this class belongs to V1's table set.
    #[must_use]
    pub const fn legacy_put_table(self) -> Option<CompositeTableV1> {
        match self {
            Self::Commit => Some(CompositeTableV1::Commits),
            Self::Event => Some(CompositeTableV1::Events),
            Self::EventRoute => Some(CompositeTableV1::EventRoutes),
            Self::OutboxIntent => Some(CompositeTableV1::Outbox),
            Self::Provenance => Some(CompositeTableV1::Provenance),
            Self::Entity => Some(CompositeTableV1::Entities),
            Self::AdministrationAudit => Some(CompositeTableV1::Audit),
            Self::ServiceAuditRequestIndex => Some(CompositeTableV1::AuditByRequest),
            Self::EntityDeleteTombstone | Self::EntityChainHead => None,
        }
    }

    const fn index(self) -> usize {
        self as usize - 1
    }
}

/// One V2 put or exact entity-delete tombstone.
#[derive(Clone, Eq, PartialEq)]
pub enum ChangelogEntryV2 {
    /// Inserts or replaces one complete authoritative row.
    Put {
        /// Closed row class; never [`ChangelogEntryClassV2::EntityDeleteTombstone`].
        class: ChangelogEntryClassV2,
        /// Exact physical key.
        key: Box<[u8]>,
        /// Complete canonical stored value.
        value: Box<[u8]>,
    },
    /// Removes one live entity only after validating this exact transition.
    EntityDeleteTombstone(CommittedEntityTransitionV1),
}

impl ChangelogEntryV2 {
    /// Builds one put entry. Delete tombstones have a distinct constructor.
    pub fn put(
        class: ChangelogEntryClassV2,
        key: impl Into<Box<[u8]>>,
        value: impl Into<Box<[u8]>>,
    ) -> Result<Self, ChangelogFrameV2Error> {
        if class == ChangelogEntryClassV2::EntityDeleteTombstone {
            return Err(ChangelogFrameV2Error::InvalidEntry);
        }
        let key = key.into();
        let value = value.into();
        if key.is_empty() || key.len() > u32::MAX as usize || value.len() > u32::MAX as usize {
            return Err(ChangelogFrameV2Error::LimitExceeded);
        }
        Ok(Self::Put { class, key, value })
    }

    /// Builds the one semantic delete entry class.
    pub fn entity_delete_tombstone(
        transition: CommittedEntityTransitionV1,
    ) -> Result<Self, ChangelogFrameV2Error> {
        if transition.next_state() != crate::EntityChainStateV1::Deleted {
            return Err(ChangelogFrameV2Error::InvalidEntry);
        }
        Ok(Self::EntityDeleteTombstone(transition))
    }

    /// Returns the closed class.
    #[must_use]
    pub const fn class(&self) -> ChangelogEntryClassV2 {
        match self {
            Self::Put { class, .. } => *class,
            Self::EntityDeleteTombstone(_) => ChangelogEntryClassV2::EntityDeleteTombstone,
        }
    }

    /// Borrows the exact physical key.
    #[must_use]
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Put { key, .. } => key,
            Self::EntityDeleteTombstone(transition) => transition.target().key().as_bytes(),
        }
    }

    /// Borrows the complete put value, or `None` for a semantic deletion.
    #[must_use]
    pub fn put_value(&self) -> Option<&[u8]> {
        match self {
            Self::Put { value, .. } => Some(value),
            Self::EntityDeleteTombstone(_) => None,
        }
    }

    /// Borrows delete-transition evidence when this is the tombstone class.
    #[must_use]
    pub const fn delete_transition(&self) -> Option<&CommittedEntityTransitionV1> {
        match self {
            Self::Put { .. } => None,
            Self::EntityDeleteTombstone(transition) => Some(transition),
        }
    }

    fn value_bytes(&self) -> Result<Vec<u8>, ChangelogFrameV2Error> {
        match self {
            Self::Put { value, .. } => Ok(value.to_vec()),
            Self::EntityDeleteTombstone(transition) => {
                let mut value = transition
                    .canonical_preimage()
                    .map_err(|_| ChangelogFrameV2Error::InvalidEntry)?;
                value.extend_from_slice(transition.transition_hash().as_bytes());
                Ok(value)
            }
        }
    }

    fn order_key(&self) -> (u8, u64, u32, &[u8]) {
        match self {
            Self::Put { class, key, .. } => (*class as u8, 0, 0, key),
            Self::EntityDeleteTombstone(transition) => (
                ChangelogEntryClassV2::EntityDeleteTombstone as u8,
                transition.command_sequence().get(),
                transition.mutation_ordinal(),
                transition.target().key().as_bytes(),
            ),
        }
    }
}

impl fmt::Debug for ChangelogEntryV2 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChangelogEntryV2")
            .field("class", &self.class())
            .field("key_bytes", &self.key().len())
            .field("value", &"[REDACTED]")
            .finish()
    }
}

/// Receipt establishing the only legal V1-to-V2 stream boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChangelogV2RotationReceipt {
    database_id: DatabaseId,
    history_incarnation: u64,
    predecessor: DualFrontier,
    v1_terminal_hash: [u8; HASH_BYTES],
    v2_chain_anchor: [u8; HASH_BYTES],
    receipt_hash: [u8; HASH_BYTES],
}

impl ChangelogV2RotationReceipt {
    /// Derives a deterministic V2 chain anchor and self-hashed receipt.
    pub fn new(
        database_id: DatabaseId,
        history_incarnation: u64,
        predecessor: DualFrontier,
        v1_terminal_hash: [u8; HASH_BYTES],
    ) -> Result<Self, ChangelogFrameV2Error> {
        if history_incarnation == 0 {
            return Err(ChangelogFrameV2Error::InvalidBinding);
        }
        let v2_chain_anchor = digest_many(&[
            b"riffdb.changelog-v2-anchor/v1",
            database_id.as_bytes(),
            &history_incarnation.to_be_bytes(),
            &predecessor.to_canonical_bytes(),
            &v1_terminal_hash,
        ]);
        let receipt_hash = digest_many(&[
            b"riffdb.changelog-v2-rotation-receipt/v1",
            database_id.as_bytes(),
            &history_incarnation.to_be_bytes(),
            &predecessor.to_canonical_bytes(),
            &v1_terminal_hash,
            &v2_chain_anchor,
        ]);
        Ok(Self {
            database_id,
            history_incarnation,
            predecessor,
            v1_terminal_hash,
            v2_chain_anchor,
            receipt_hash,
        })
    }

    /// Reconstructs a durable receipt and verifies both derived hashes.
    #[allow(clippy::too_many_arguments)]
    pub fn from_stored_parts(
        database_id: DatabaseId,
        history_incarnation: u64,
        predecessor: DualFrontier,
        v1_terminal_hash: [u8; HASH_BYTES],
        v2_chain_anchor: [u8; HASH_BYTES],
        receipt_hash: [u8; HASH_BYTES],
    ) -> Result<Self, ChangelogFrameV2Error> {
        let value = Self::new(
            database_id,
            history_incarnation,
            predecessor,
            v1_terminal_hash,
        )?;
        if value.v2_chain_anchor != v2_chain_anchor || value.receipt_hash != receipt_hash {
            return Err(ChangelogFrameV2Error::ChecksumMismatch);
        }
        Ok(value)
    }

    /// Database identity at rotation.
    #[must_use]
    pub const fn database_id(self) -> DatabaseId {
        self.database_id
    }

    /// History incarnation at rotation.
    #[must_use]
    pub const fn history_incarnation(self) -> u64 {
        self.history_incarnation
    }

    /// Last V1 dual frontier and first V2 predecessor.
    #[must_use]
    pub const fn predecessor(self) -> DualFrontier {
        self.predecessor
    }

    /// Last V1 frame hash.
    #[must_use]
    pub const fn v1_terminal_hash(self) -> [u8; HASH_BYTES] {
        self.v1_terminal_hash
    }

    /// Required chain hash of the first V2 frame.
    #[must_use]
    pub const fn v2_chain_anchor(self) -> [u8; HASH_BYTES] {
        self.v2_chain_anchor
    }

    /// Self-hash of the complete receipt.
    #[must_use]
    pub const fn receipt_hash(self) -> [u8; HASH_BYTES] {
        self.receipt_hash
    }
}

/// Delete-aware entity snapshot manifest at one exact V2 resume boundary.
///
/// The complete chain-head catalog, not merely the live entity table, is the
/// bootstrap authority. Binding its cardinalities and fingerprint prevents a
/// receiver from accepting a snapshot that silently omitted deleted identity
/// history and would therefore accept a stale recreate later.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EntityReplicaBootstrapManifestV2 {
    database_id: DatabaseId,
    history_incarnation: u64,
    rotation_receipt_hash: [u8; HASH_BYTES],
    frontier: DualFrontier,
    tail_chain_hash: [u8; HASH_BYTES],
    counts: ValidatedPrefixEntityTransitionCounts,
    entity_transition_fingerprint: EntityTransitionFingerprint,
    manifest_hash: [u8; HASH_BYTES],
}

impl EntityReplicaBootstrapManifestV2 {
    /// Constructs and self-hashes one exact entity bootstrap manifest.
    pub fn new(
        receipt: ChangelogV2RotationReceipt,
        frontier: DualFrontier,
        tail_chain_hash: [u8; HASH_BYTES],
        counts: ValidatedPrefixEntityTransitionCounts,
        entity_transition_fingerprint: EntityTransitionFingerprint,
    ) -> Result<Self, ChangelogFrameV2Error> {
        if frontier != receipt.predecessor() && !frontier.advances_from(receipt.predecessor()) {
            return Err(ChangelogFrameV2Error::InvalidBinding);
        }
        let total_heads = counts
            .live_entity_count
            .checked_add(counts.deleted_entity_count)
            .ok_or(ChangelogFrameV2Error::LimitExceeded)?;
        if counts.entity_transition_count < total_heads {
            return Err(ChangelogFrameV2Error::InvalidBinding);
        }
        let manifest_hash = digest_many(&[
            b"riffdb.entity-replica-bootstrap-manifest/v2",
            receipt.database_id().as_bytes(),
            &receipt.history_incarnation().to_be_bytes(),
            &receipt.receipt_hash(),
            &frontier.to_canonical_bytes(),
            &tail_chain_hash,
            &counts.live_entity_count.to_be_bytes(),
            &counts.deleted_entity_count.to_be_bytes(),
            &counts.entity_transition_count.to_be_bytes(),
            entity_transition_fingerprint.as_bytes(),
        ]);
        Ok(Self {
            database_id: receipt.database_id(),
            history_incarnation: receipt.history_incarnation(),
            rotation_receipt_hash: receipt.receipt_hash(),
            frontier,
            tail_chain_hash,
            counts,
            entity_transition_fingerprint,
            manifest_hash,
        })
    }

    /// Database lineage captured by the snapshot.
    #[must_use]
    pub const fn database_id(self) -> DatabaseId {
        self.database_id
    }

    /// Retained-history incarnation captured by the snapshot.
    #[must_use]
    pub const fn history_incarnation(self) -> u64 {
        self.history_incarnation
    }

    /// Exact rotation receipt the snapshot extends.
    #[must_use]
    pub const fn rotation_receipt_hash(self) -> [u8; HASH_BYTES] {
        self.rotation_receipt_hash
    }

    /// Exact dual frontier represented by the snapshot.
    #[must_use]
    pub const fn frontier(self) -> DualFrontier {
        self.frontier
    }

    /// V2 chain hash required by the first post-bootstrap frame.
    #[must_use]
    pub const fn tail_chain_hash(self) -> [u8; HASH_BYTES] {
        self.tail_chain_hash
    }

    /// Separated live, deleted, and historical transition counts.
    #[must_use]
    pub const fn counts(self) -> ValidatedPrefixEntityTransitionCounts {
        self.counts
    }

    /// Canonical fingerprint of every entity-chain head.
    #[must_use]
    pub const fn entity_transition_fingerprint(self) -> EntityTransitionFingerprint {
        self.entity_transition_fingerprint
    }

    /// Self-hash binding the receipt, resume cursor, and complete head proof.
    #[must_use]
    pub const fn manifest_hash(self) -> [u8; HASH_BYTES] {
        self.manifest_hash
    }
}

/// Shared identity and durability-fence binding for one V2 frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChangelogFrameBindingV2 {
    /// Database lineage.
    pub database_id: DatabaseId,
    /// History incarnation.
    pub history_incarnation: u64,
    /// Hash of the predecessor V2 frame or rotation anchor.
    pub chain_hash: [u8; HASH_BYTES],
    /// Hash of the local durability fence that published this state.
    pub journal_frame_hash: [u8; HASH_BYTES],
    /// Whether the covering fence was a journal flush.
    pub journaled: bool,
}

/// Complete V2 frame header.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChangelogFrameHeaderV2 {
    binding: ChangelogFrameBindingV2,
    predecessor: DualFrontier,
    covered: DualFrontier,
    entry_counts: [u32; ENTRY_CLASS_COUNT],
}

impl ChangelogFrameHeaderV2 {
    /// Shared frame binding.
    #[must_use]
    pub const fn binding(self) -> ChangelogFrameBindingV2 {
        self.binding
    }

    /// Exact predecessor frontier.
    #[must_use]
    pub const fn predecessor(self) -> DualFrontier {
        self.predecessor
    }

    /// Exact covered frontier.
    #[must_use]
    pub const fn covered(self) -> DualFrontier {
        self.covered
    }

    /// Per-class counts in tag order.
    #[must_use]
    pub const fn entry_counts(self) -> [u32; ENTRY_CLASS_COUNT] {
        self.entry_counts
    }
}

/// One complete delete-aware changelog frame.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangelogFrameV2 {
    header: ChangelogFrameHeaderV2,
    entries: Vec<ChangelogEntryV2>,
}

impl ChangelogFrameV2 {
    /// Constructs a bounded, canonically ordered V2 frame.
    pub fn new(
        binding: ChangelogFrameBindingV2,
        predecessor: DualFrontier,
        covered: DualFrontier,
        entries: Vec<ChangelogEntryV2>,
    ) -> Result<Self, ChangelogFrameV2Error> {
        if binding.history_incarnation == 0 || !covered.advances_from(predecessor) {
            return Err(ChangelogFrameV2Error::InvalidBinding);
        }
        if entries.len() > MAX_CHANGELOG_FRAME_ENTRIES {
            return Err(ChangelogFrameV2Error::LimitExceeded);
        }
        let mut counts = [0_u32; ENTRY_CLASS_COUNT];
        let mut prior = None;
        for entry in &entries {
            let key = entry.order_key();
            if prior.is_some_and(|prior| prior >= key) {
                return Err(ChangelogFrameV2Error::NonCanonicalOrder);
            }
            prior = Some(key);
            counts[entry.class().index()] = counts[entry.class().index()]
                .checked_add(1)
                .ok_or(ChangelogFrameV2Error::LimitExceeded)?;
        }
        let value = Self {
            header: ChangelogFrameHeaderV2 {
                binding,
                predecessor,
                covered,
                entry_counts: counts,
            },
            entries,
        };
        if value.encoded_len()? > MAX_CHANGELOG_FRAME_BYTES {
            return Err(ChangelogFrameV2Error::LimitExceeded);
        }
        Ok(value)
    }

    /// Header.
    #[must_use]
    pub const fn header(&self) -> &ChangelogFrameHeaderV2 {
        &self.header
    }

    /// Canonical entries.
    #[must_use]
    pub fn entries(&self) -> &[ChangelogEntryV2] {
        &self.entries
    }

    fn encoded_len(&self) -> Result<usize, ChangelogFrameV2Error> {
        self.entries
            .iter()
            .try_fold(HEADER_BYTES + FOOTER_BYTES, |sum, entry| {
                sum.checked_add(ENTRY_PREFIX_BYTES)
                    .and_then(|sum| sum.checked_add(entry.key().len()))
                    .and_then(|sum| {
                        entry
                            .value_bytes()
                            .ok()
                            .and_then(|value| sum.checked_add(value.len()))
                    })
                    .ok_or(ChangelogFrameV2Error::LimitExceeded)
            })
    }

    /// Canonically encodes and checksums the complete frame.
    pub fn encode(&self) -> Result<EncodedChangelogFrameV2, ChangelogFrameV2Error> {
        let total_len = self.encoded_len()?;
        if total_len > MAX_CHANGELOG_FRAME_BYTES {
            return Err(ChangelogFrameV2Error::LimitExceeded);
        }
        let mut payloads = Vec::with_capacity(self.entries.len());
        let mut payload_len = 0_usize;
        for entry in &self.entries {
            let value = entry.value_bytes()?;
            payload_len = payload_len
                .checked_add(ENTRY_PREFIX_BYTES + entry.key().len() + value.len())
                .ok_or(ChangelogFrameV2Error::LimitExceeded)?;
            payloads.push(value);
        }
        let mut bytes = Vec::with_capacity(total_len);
        bytes.extend_from_slice(&FRAME_MAGIC);
        bytes.extend_from_slice(&FORMAT_VERSION.to_be_bytes());
        bytes.push(if self.header.binding.journaled {
            FLAG_JOURNALED
        } else {
            0
        });
        bytes.push(0);
        bytes.extend_from_slice(self.header.binding.database_id.as_bytes());
        bytes.extend_from_slice(&self.header.binding.history_incarnation.to_be_bytes());
        bytes.extend_from_slice(&self.header.predecessor.to_canonical_bytes());
        bytes.extend_from_slice(&self.header.covered.to_canonical_bytes());
        bytes.extend_from_slice(&(self.entries.len() as u32).to_be_bytes());
        for count in self.header.entry_counts {
            bytes.extend_from_slice(&count.to_be_bytes());
        }
        bytes.extend_from_slice(&(payload_len as u32).to_be_bytes());
        bytes.extend_from_slice(&self.header.binding.chain_hash);
        bytes.extend_from_slice(&self.header.binding.journal_frame_hash);
        for (entry, value) in self.entries.iter().zip(payloads) {
            bytes.push(entry.class() as u8);
            bytes.extend_from_slice(&[0; 3]);
            bytes.extend_from_slice(&(entry.key().len() as u32).to_be_bytes());
            bytes.extend_from_slice(&(value.len() as u32).to_be_bytes());
            bytes.extend_from_slice(entry.key());
            bytes.extend_from_slice(&value);
        }
        bytes.extend_from_slice(&FOOTER_MAGIC);
        bytes.extend_from_slice(&(total_len as u64).to_be_bytes());
        let frame_hash = digest(&bytes);
        bytes.extend_from_slice(&frame_hash);
        Ok(EncodedChangelogFrameV2 {
            bytes: Arc::from(bytes),
            frame_hash,
        })
    }

    /// Decodes one complete canonical V2 frame.
    pub fn decode(bytes: &[u8]) -> Result<(Self, [u8; HASH_BYTES]), ChangelogFrameV2Error> {
        if bytes.len() < HEADER_BYTES + FOOTER_BYTES {
            return Err(ChangelogFrameV2Error::Truncated);
        }
        let mut cursor = FrameCursor::new(bytes);
        if cursor.take(8)? != FRAME_MAGIC {
            return Err(ChangelogFrameV2Error::UnknownFormat);
        }
        if cursor.take_u16()? != FORMAT_VERSION {
            return Err(ChangelogFrameV2Error::UnknownFormat);
        }
        let journaled = match cursor.take_u8()? {
            0 => false,
            FLAG_JOURNALED => true,
            _ => return Err(ChangelogFrameV2Error::InvalidBinding),
        };
        if cursor.take_u8()? != 0 {
            return Err(ChangelogFrameV2Error::InvalidBinding);
        }
        let database_id = DatabaseId::from_bytes(cursor.take_array()?)
            .map_err(|_| ChangelogFrameV2Error::InvalidBinding)?;
        let history_incarnation = cursor.take_u64()?;
        let predecessor = DualFrontier::from_canonical_bytes(cursor.take_array()?)
            .map_err(|_| ChangelogFrameV2Error::InvalidBinding)?;
        let covered = DualFrontier::from_canonical_bytes(cursor.take_array()?)
            .map_err(|_| ChangelogFrameV2Error::InvalidBinding)?;
        let entry_count = cursor.take_u32()? as usize;
        if entry_count > MAX_CHANGELOG_FRAME_ENTRIES {
            return Err(ChangelogFrameV2Error::LimitExceeded);
        }
        let mut expected_counts = [0_u32; ENTRY_CLASS_COUNT];
        for count in &mut expected_counts {
            *count = cursor.take_u32()?;
        }
        let payload_len = cursor.take_u32()? as usize;
        let chain_hash = cursor.take_array()?;
        let journal_frame_hash = cursor.take_array()?;
        if HEADER_BYTES + payload_len + FOOTER_BYTES != bytes.len() {
            return Err(ChangelogFrameV2Error::Truncated);
        }
        let payload_end = HEADER_BYTES + payload_len;
        let mut entries = Vec::with_capacity(entry_count);
        while cursor.offset < payload_end {
            let class = ChangelogEntryClassV2::from_tag(cursor.take_u8()?)
                .ok_or(ChangelogFrameV2Error::UnknownEntryClass)?;
            if cursor.take(3)? != [0; 3] {
                return Err(ChangelogFrameV2Error::InvalidEntry);
            }
            let key_len = cursor.take_u32()? as usize;
            let value_len = cursor.take_u32()? as usize;
            let key: Box<[u8]> = cursor.take(key_len)?.into();
            let value = cursor.take(value_len)?;
            let entry = if class == ChangelogEntryClassV2::EntityDeleteTombstone {
                if value.len() < HASH_BYTES {
                    return Err(ChangelogFrameV2Error::InvalidEntry);
                }
                let split = value.len() - HASH_BYTES;
                let hash = EntityTransitionHash::from_bytes(
                    value[split..]
                        .try_into()
                        .map_err(|_| ChangelogFrameV2Error::InvalidEntry)?,
                );
                let transition =
                    CommittedEntityTransitionV1::decode_canonical_preimage(&value[..split], hash)
                        .map_err(|_| ChangelogFrameV2Error::InvalidEntry)?;
                if key.as_ref() != transition.target().key().as_bytes() {
                    return Err(ChangelogFrameV2Error::InvalidEntry);
                }
                ChangelogEntryV2::entity_delete_tombstone(transition)?
            } else {
                ChangelogEntryV2::put(class, key, Box::<[u8]>::from(value))?
            };
            entries.push(entry);
        }
        if entries.len() != entry_count || cursor.offset != payload_end {
            return Err(ChangelogFrameV2Error::CountMismatch);
        }
        if cursor.take(8)? != FOOTER_MAGIC || cursor.take_u64()? as usize != bytes.len() {
            return Err(ChangelogFrameV2Error::Truncated);
        }
        let retained_hash = cursor.take_array()?;
        if !cursor.is_complete() {
            return Err(ChangelogFrameV2Error::TrailingBytes);
        }
        let computed_hash = digest(&bytes[..bytes.len() - HASH_BYTES]);
        if retained_hash != computed_hash {
            return Err(ChangelogFrameV2Error::ChecksumMismatch);
        }
        let frame = Self::new(
            ChangelogFrameBindingV2 {
                database_id,
                history_incarnation,
                chain_hash,
                journal_frame_hash,
                journaled,
            },
            predecessor,
            covered,
            entries,
        )?;
        if frame.header.entry_counts != expected_counts {
            return Err(ChangelogFrameV2Error::CountMismatch);
        }
        Ok((frame, retained_hash))
    }
}

/// Canonical V2 bytes and frame hash.
#[derive(Clone, Eq, PartialEq)]
pub struct EncodedChangelogFrameV2 {
    bytes: Arc<[u8]>,
    frame_hash: [u8; HASH_BYTES],
}

impl EncodedChangelogFrameV2 {
    /// Complete bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Frame hash used by the next frame.
    #[must_use]
    pub const fn frame_hash(&self) -> [u8; HASH_BYTES] {
        self.frame_hash
    }
}

impl fmt::Debug for EncodedChangelogFrameV2 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EncodedChangelogFrameV2")
            .field("bytes", &"[REDACTED]")
            .field("length", &self.bytes.len())
            .finish()
    }
}

/// Stateful exact V2 stream validator anchored by one rotation receipt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangelogStreamValidatorV2 {
    database_id: DatabaseId,
    history_incarnation: u64,
    expected_predecessor: DualFrontier,
    expected_chain_hash: [u8; HASH_BYTES],
}

impl ChangelogStreamValidatorV2 {
    /// Starts V2 at the exact receipted V1 terminal boundary.
    #[must_use]
    pub const fn from_rotation(receipt: ChangelogV2RotationReceipt) -> Self {
        Self {
            database_id: receipt.database_id,
            history_incarnation: receipt.history_incarnation,
            expected_predecessor: receipt.predecessor,
            expected_chain_hash: receipt.v2_chain_anchor,
        }
    }

    /// Starts after one completely verified delete-aware bootstrap snapshot.
    #[must_use]
    pub const fn from_bootstrap(manifest: EntityReplicaBootstrapManifestV2) -> Self {
        Self {
            database_id: manifest.database_id,
            history_incarnation: manifest.history_incarnation,
            expected_predecessor: manifest.frontier,
            expected_chain_hash: manifest.tail_chain_hash,
        }
    }

    /// Returns the exact predecessor frontier required by the next frame.
    #[must_use]
    pub const fn expected_predecessor(&self) -> DualFrontier {
        self.expected_predecessor
    }

    /// Returns the chain hash required by the next frame.
    #[must_use]
    pub const fn expected_chain_hash(&self) -> [u8; HASH_BYTES] {
        self.expected_chain_hash
    }

    /// Validates and advances over exactly one frame.
    pub fn accept(&mut self, bytes: &[u8]) -> Result<ChangelogFrameV2, ChangelogFrameV2Error> {
        let (frame, hash) = ChangelogFrameV2::decode(bytes)?;
        let binding = frame.header.binding;
        if binding.database_id != self.database_id {
            return Err(ChangelogFrameV2Error::DatabaseMismatch);
        }
        if binding.history_incarnation != self.history_incarnation {
            return Err(ChangelogFrameV2Error::IncarnationMismatch);
        }
        if frame.header.predecessor != self.expected_predecessor {
            return Err(ChangelogFrameV2Error::Gap);
        }
        if binding.chain_hash != self.expected_chain_hash {
            return Err(ChangelogFrameV2Error::ChainMismatch);
        }
        self.expected_predecessor = frame.header.covered;
        self.expected_chain_hash = hash;
        Ok(frame)
    }
}

/// In-process consumer of complete delete-aware changelog frames.
///
/// The emitter validates every frame against the receipted V2 chain before
/// invoking this boundary. A consumer still has to apply each frame atomically
/// and must retain its own durable applied frontier before acknowledging any
/// remote carriage built on top of this interface.
pub trait ChangelogFrameConsumerV2: Send + Sync {
    /// Accepts one complete, already-validated V2 frame.
    fn accept_frame(&self, frame: &ChangelogFrameV2, encoded: &EncodedChangelogFrameV2);

    /// Observes a typed stream hole or derivation failure requiring bootstrap.
    fn note_resync_required(&self, reason: crate::ChangelogResyncReasonV1);
}

/// One canonical entity/head pair from a V2 bootstrap snapshot page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EntityReplicaBootstrapRowV2 {
    key: Box<[u8]>,
    entity_value: Option<Box<[u8]>>,
    chain_head_value: Box<[u8]>,
}

impl EntityReplicaBootstrapRowV2 {
    /// Constructs one bounded bootstrap row. Semantic equality is checked when
    /// the row is installed into a follower.
    pub fn new(
        key: impl Into<Box<[u8]>>,
        entity_value: Option<impl Into<Box<[u8]>>>,
        chain_head_value: impl Into<Box<[u8]>>,
    ) -> Result<Self, ChangelogFrameV2Error> {
        let key = key.into();
        let entity_value = entity_value.map(Into::into);
        let chain_head_value = chain_head_value.into();
        if key.is_empty()
            || key.len() > u32::MAX as usize
            || entity_value
                .as_ref()
                .is_some_and(|value| value.len() > u32::MAX as usize)
            || chain_head_value.is_empty()
            || chain_head_value.len() > u32::MAX as usize
        {
            return Err(ChangelogFrameV2Error::LimitExceeded);
        }
        Ok(Self {
            key,
            entity_value,
            chain_head_value,
        })
    }
}

/// Engine-neutral delete-aware entity follower.
///
/// This component owns the semantic apply rule that storage engines must use:
/// bootstrap pages are canonical and bounded, every V2 frame is validated on a
/// cloned cursor, entity transitions are replayed against exact prior heads,
/// delete tombstones are one-for-one with delete transitions, and final entity
/// rows and chain-head puts must agree before any local state changes. It does
/// not claim that the in-memory maps are a production follower database; they
/// are the conformance oracle shared by follower adapters and recovery tests.
pub struct DeleteAwareEntityFollowerV2 {
    validator: ChangelogStreamValidatorV2,
    bootstrap_manifest: EntityReplicaBootstrapManifestV2,
    bootstrap_complete: bool,
    last_bootstrap_key: Option<Box<[u8]>>,
    entities: BTreeMap<Box<[u8]>, Box<[u8]>>,
    chain_head_values: BTreeMap<Box<[u8]>, Box<[u8]>>,
    chain_heads: BTreeMap<Box<[u8]>, crate::EntityChainHeadV1>,
}

impl DeleteAwareEntityFollowerV2 {
    /// Starts an empty follower for one manifest-bound bootstrap snapshot.
    pub fn from_bootstrap(
        receipt: ChangelogV2RotationReceipt,
        manifest: EntityReplicaBootstrapManifestV2,
    ) -> Result<Self, ChangelogFrameV2Error> {
        if manifest.database_id() != receipt.database_id()
            || manifest.history_incarnation() != receipt.history_incarnation()
            || manifest.rotation_receipt_hash() != receipt.receipt_hash()
        {
            return Err(ChangelogFrameV2Error::InvalidBinding);
        }
        Ok(Self {
            validator: ChangelogStreamValidatorV2::from_bootstrap(manifest),
            bootstrap_manifest: manifest,
            bootstrap_complete: false,
            last_bootstrap_key: None,
            entities: BTreeMap::new(),
            chain_head_values: BTreeMap::new(),
            chain_heads: BTreeMap::new(),
        })
    }

    /// Installs one canonical bounded bootstrap page.
    ///
    /// Pages must be globally ascending and disjoint. `final_page` seals the
    /// snapshot; no tail frame is accepted before that seal and no later page
    /// can mutate the anchor.
    pub fn install_bootstrap_page(
        &mut self,
        rows: Vec<EntityReplicaBootstrapRowV2>,
        final_page: bool,
    ) -> Result<(), ChangelogFrameV2Error> {
        if self.bootstrap_complete || rows.len() > MAX_CHANGELOG_FRAME_ENTRIES {
            return Err(ChangelogFrameV2Error::InvalidBinding);
        }
        let mut staged = Vec::with_capacity(rows.len());
        let mut prior = self.last_bootstrap_key.clone();
        for row in rows {
            if prior
                .as_deref()
                .is_some_and(|prior| prior >= row.key.as_ref())
            {
                return Err(ChangelogFrameV2Error::NonCanonicalOrder);
            }
            let head = crate::decode_entity_chain_head_v1(&row.chain_head_value)
                .map_err(|_| ChangelogFrameV2Error::InvalidEntry)?
                .into_parts()
                .0;
            if head.target().key().as_bytes() != row.key.as_ref() {
                return Err(ChangelogFrameV2Error::InvalidEntry);
            }
            match (head.state(), row.entity_value.as_deref()) {
                (
                    crate::EntityChainStateV1::Live {
                        version,
                        value_hash,
                    },
                    Some(encoded),
                ) => {
                    let entity = crate::decode_entity_record_v1(encoded)
                        .map_err(|_| ChangelogFrameV2Error::InvalidEntry)?
                        .into_parts()
                        .0;
                    if entity.target() != head.target()
                        || entity.entity_version() != version
                        || crate::derive_entity_record_hash_v1(&entity)
                            .map_err(|_| ChangelogFrameV2Error::InvalidEntry)?
                            != value_hash
                    {
                        return Err(ChangelogFrameV2Error::InvalidEntry);
                    }
                }
                (crate::EntityChainStateV1::Deleted, None) => {}
                _ => return Err(ChangelogFrameV2Error::InvalidEntry),
            }
            prior = Some(row.key.clone());
            staged.push((row, head));
        }
        for (row, head) in staged {
            if let Some(entity) = row.entity_value {
                self.entities.insert(row.key.clone(), entity);
            }
            self.chain_head_values
                .insert(row.key.clone(), row.chain_head_value);
            self.chain_heads.insert(row.key.clone(), head);
            self.last_bootstrap_key = Some(row.key);
        }
        if final_page {
            let mut live_entity_count = 0_u64;
            let mut deleted_entity_count = 0_u64;
            let mut entity_transition_count = 0_u64;
            for head in self.chain_heads.values() {
                match head.state() {
                    crate::EntityChainStateV1::Live { .. } => {
                        live_entity_count = live_entity_count
                            .checked_add(1)
                            .ok_or(ChangelogFrameV2Error::LimitExceeded)?;
                    }
                    crate::EntityChainStateV1::Deleted => {
                        deleted_entity_count = deleted_entity_count
                            .checked_add(1)
                            .ok_or(ChangelogFrameV2Error::LimitExceeded)?;
                    }
                    crate::EntityChainStateV1::NeverExisted => {
                        return Err(ChangelogFrameV2Error::InvalidEntry);
                    }
                }
                entity_transition_count = entity_transition_count
                    .checked_add(head.chain_revision())
                    .ok_or(ChangelogFrameV2Error::LimitExceeded)?;
            }
            let counts = ValidatedPrefixEntityTransitionCounts {
                live_entity_count,
                deleted_entity_count,
                entity_transition_count,
            };
            let fingerprint =
                EntityTransitionFingerprint::from_sorted_heads(self.chain_heads.values())
                    .map_err(|_| ChangelogFrameV2Error::InvalidEntry)?;
            if counts != self.bootstrap_manifest.counts()
                || fingerprint != self.bootstrap_manifest.entity_transition_fingerprint()
            {
                return Err(ChangelogFrameV2Error::ChecksumMismatch);
            }
        }
        self.bootstrap_complete = final_page;
        Ok(())
    }

    /// Validates and atomically applies one encoded V2 frame.
    pub fn apply_encoded(&mut self, encoded: &[u8]) -> Result<DualFrontier, ChangelogFrameV2Error> {
        if !self.bootstrap_complete {
            return Err(ChangelogFrameV2Error::InvalidBinding);
        }
        let mut candidate_validator = self.validator.clone();
        let frame = candidate_validator.accept(encoded)?;
        let predecessor = frame.header().predecessor();
        let covered = frame.header().covered();

        let mut transitions = Vec::new();
        let mut delete_hashes = Vec::new();
        let mut entity_puts: BTreeMap<Box<[u8]>, Box<[u8]>> = BTreeMap::new();
        let mut head_puts: BTreeMap<Box<[u8]>, Box<[u8]>> = BTreeMap::new();
        for entry in frame.entries() {
            match entry {
                ChangelogEntryV2::EntityDeleteTombstone(transition) => {
                    delete_hashes.push(transition.transition_hash());
                }
                ChangelogEntryV2::Put { class, key, value } => match class {
                    ChangelogEntryClassV2::Commit => {
                        match crate::decode_command_segment_v1(value) {
                            Ok(segment) => {
                                for capsule in segment.value().commands() {
                                    transitions
                                        .extend(capsule.entity_transitions().iter().cloned());
                                }
                            }
                            Err(error)
                                if error.kind()
                                    == crate::DurableCodecErrorKind::UnexpectedRecordType => {}
                            Err(_) => return Err(ChangelogFrameV2Error::InvalidEntry),
                        }
                    }
                    ChangelogEntryClassV2::Entity
                        if entity_puts.insert(key.clone(), value.clone()).is_some() =>
                    {
                        return Err(ChangelogFrameV2Error::InvalidEntry);
                    }
                    ChangelogEntryClassV2::Entity => {}
                    ChangelogEntryClassV2::EntityChainHead
                        if head_puts.insert(key.clone(), value.clone()).is_some() =>
                    {
                        return Err(ChangelogFrameV2Error::InvalidEntry);
                    }
                    ChangelogEntryClassV2::EntityChainHead => {}
                    _ => {}
                },
            }
        }

        let mut expected_deletes = Vec::new();
        let mut staged_heads: BTreeMap<Box<[u8]>, crate::EntityChainHeadV1> = BTreeMap::new();
        let mut prior_position = None;
        for transition in &transitions {
            let position = (transition.command_sequence(), transition.mutation_ordinal());
            if prior_position.is_some_and(|prior| prior >= position)
                || predecessor
                    .application()
                    .is_some_and(|prior| transition.command_sequence() <= prior)
                || covered
                    .application()
                    .is_none_or(|last| transition.command_sequence() > last)
            {
                return Err(ChangelogFrameV2Error::InvalidEntry);
            }
            prior_position = Some(position);
            let key: Box<[u8]> = transition.target().key().as_bytes().into();
            let next = match staged_heads
                .get(&key)
                .or_else(|| self.chain_heads.get(&key))
            {
                Some(head) => head
                    .apply(transition)
                    .map_err(|_| ChangelogFrameV2Error::InvalidEntry)?,
                None => crate::EntityChainHeadV1::from_genesis(transition)
                    .map_err(|_| ChangelogFrameV2Error::InvalidEntry)?,
            };
            if transition.next_state() == crate::EntityChainStateV1::Deleted {
                expected_deletes.push(transition.transition_hash());
            }
            staged_heads.insert(key, next);
        }
        if delete_hashes != expected_deletes
            || entity_puts
                .keys()
                .any(|key| !staged_heads.contains_key(key))
            || head_puts.len() != staged_heads.len()
            || head_puts.keys().any(|key| !staged_heads.contains_key(key))
        {
            return Err(ChangelogFrameV2Error::InvalidEntry);
        }

        let mut applies = Vec::with_capacity(staged_heads.len());
        for (key, expected_head) in &staged_heads {
            let encoded_head = head_puts
                .get(key)
                .ok_or(ChangelogFrameV2Error::InvalidEntry)?;
            let observed_head = crate::decode_entity_chain_head_v1(encoded_head)
                .map_err(|_| ChangelogFrameV2Error::InvalidEntry)?
                .into_parts()
                .0;
            if &observed_head != expected_head || observed_head.target().key().as_bytes() != &**key
            {
                return Err(ChangelogFrameV2Error::InvalidEntry);
            }
            let entity_value = match expected_head.state() {
                crate::EntityChainStateV1::Live {
                    version,
                    value_hash,
                } => {
                    let encoded_entity = entity_puts
                        .get(key)
                        .ok_or(ChangelogFrameV2Error::InvalidEntry)?;
                    let entity = crate::decode_entity_record_v1(encoded_entity)
                        .map_err(|_| ChangelogFrameV2Error::InvalidEntry)?
                        .into_parts()
                        .0;
                    if entity.target() != expected_head.target()
                        || entity.entity_version() != version
                        || crate::derive_entity_record_hash_v1(&entity)
                            .map_err(|_| ChangelogFrameV2Error::InvalidEntry)?
                            != value_hash
                    {
                        return Err(ChangelogFrameV2Error::InvalidEntry);
                    }
                    Some(encoded_entity.clone())
                }
                crate::EntityChainStateV1::Deleted => {
                    if entity_puts.contains_key(key) {
                        return Err(ChangelogFrameV2Error::InvalidEntry);
                    }
                    None
                }
                crate::EntityChainStateV1::NeverExisted => {
                    return Err(ChangelogFrameV2Error::InvalidEntry);
                }
            };
            applies.push((
                key.clone(),
                expected_head.clone(),
                encoded_head.clone(),
                entity_value,
            ));
        }

        for (key, head, encoded_head, entity_value) in applies {
            if let Some(entity_value) = entity_value {
                self.entities.insert(key.clone(), entity_value);
            } else {
                self.entities.remove(&key);
            }
            self.chain_head_values.insert(key.clone(), encoded_head);
            self.chain_heads.insert(key, head);
        }
        self.validator = candidate_validator;
        Ok(covered)
    }

    /// Returns the exact applied V2 frontier.
    #[must_use]
    pub const fn applied_frontier(&self) -> DualFrontier {
        self.validator.expected_predecessor()
    }

    /// Borrows one exact materialized entity value.
    #[must_use]
    pub fn entity_value(&self, key: &[u8]) -> Option<&[u8]> {
        self.entities.get(key).map(AsRef::as_ref)
    }

    /// Borrows one exact current chain-head value.
    #[must_use]
    pub fn chain_head_value(&self, key: &[u8]) -> Option<&[u8]> {
        self.chain_head_values.get(key).map(AsRef::as_ref)
    }
}

/// Typed V2 framing/negotiation failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChangelogFrameV2Error {
    /// Bytes are not the V2 format; includes V1 bytes.
    UnknownFormat,
    /// Frame ended before its claimed footer.
    Truncated,
    /// Extra bytes followed a complete frame.
    TrailingBytes,
    /// A closed field or identity was invalid.
    InvalidBinding,
    /// An entry was malformed or not semantic for its class.
    InvalidEntry,
    /// Unknown class tag.
    UnknownEntryClass,
    /// Entry order or identity was noncanonical.
    NonCanonicalOrder,
    /// Declared and observed counts differ.
    CountMismatch,
    /// Fixed frame bound was exceeded.
    LimitExceeded,
    /// Checksum mismatch.
    ChecksumMismatch,
    /// Another database lineage.
    DatabaseMismatch,
    /// Another history incarnation.
    IncarnationMismatch,
    /// Missing or reordered frame.
    Gap,
    /// Wrong V2 predecessor frame hash.
    ChainMismatch,
}

impl fmt::Display for ChangelogFrameV2Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::UnknownFormat => "changelog format is unsupported",
            Self::Truncated => "changelog frame is truncated",
            Self::TrailingBytes => "changelog frame has trailing bytes",
            Self::InvalidBinding => "changelog frame binding is invalid",
            Self::InvalidEntry => "changelog entry is invalid",
            Self::UnknownEntryClass => "changelog entry class is unsupported",
            Self::NonCanonicalOrder => "changelog entries are not canonical",
            Self::CountMismatch => "changelog entry counts do not match",
            Self::LimitExceeded => "changelog frame exceeds a fixed bound",
            Self::ChecksumMismatch => "changelog frame checksum does not match",
            Self::DatabaseMismatch => "changelog frame belongs to another database",
            Self::IncarnationMismatch => "changelog frame belongs to another history incarnation",
            Self::Gap => "changelog frame does not continue the applied frontier",
            Self::ChainMismatch => "changelog frame does not continue the V2 hash chain",
        })
    }
}

impl std::error::Error for ChangelogFrameV2Error {}

struct FrameCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> FrameCursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], ChangelogFrameV2Error> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(ChangelogFrameV2Error::LimitExceeded)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(ChangelogFrameV2Error::Truncated)?;
        self.offset = end;
        Ok(value)
    }

    fn take_u8(&mut self) -> Result<u8, ChangelogFrameV2Error> {
        self.take(1)?
            .first()
            .copied()
            .ok_or(ChangelogFrameV2Error::Truncated)
    }

    fn take_u16(&mut self) -> Result<u16, ChangelogFrameV2Error> {
        Ok(u16::from_be_bytes(self.take_array()?))
    }

    fn take_u32(&mut self) -> Result<u32, ChangelogFrameV2Error> {
        Ok(u32::from_be_bytes(self.take_array()?))
    }

    fn take_u64(&mut self) -> Result<u64, ChangelogFrameV2Error> {
        Ok(u64::from_be_bytes(self.take_array()?))
    }

    fn take_array<const N: usize>(&mut self) -> Result<[u8; N], ChangelogFrameV2Error> {
        self.take(N)?
            .try_into()
            .map_err(|_| ChangelogFrameV2Error::Truncated)
    }

    fn is_complete(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

fn digest(bytes: &[u8]) -> [u8; HASH_BYTES] {
    Sha256::digest(bytes).into()
}

fn digest_many(parts: &[&[u8]]) -> [u8; HASH_BYTES] {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update(part);
    }
    hasher.finalize().into()
}

#[cfg(test)]
mod tests {
    use riffdb_types::{
        CommitSequence, EntityKeyBuilder, EntityRecordHash, EntityTypeId, EntityVersion,
    };

    use super::*;
    use crate::{
        ChangelogEntryClassV1, ChangelogEntryV1, ChangelogFrameBindingV1, ChangelogFrameV1,
        EntityChainStateV1, EntityTarget,
    };

    fn database_id() -> DatabaseId {
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x77; 10])
            .expect("deterministic database identity")
    }

    fn target() -> EntityTarget {
        let entity_type = EntityTypeId::new(3).expect("type");
        let mut key = EntityKeyBuilder::new(entity_type);
        key.push_u64(9).expect("key component");
        EntityTarget::new(entity_type, key.finish().expect("key")).expect("target")
    }

    fn deletion() -> CommittedEntityTransitionV1 {
        CommittedEntityTransitionV1::new(
            CommitSequence::new(2).expect("sequence"),
            0,
            target(),
            EntityChainStateV1::Live {
                version: EntityVersion::first(),
                value_hash: EntityRecordHash::from_bytes([4; 32]),
            },
            1,
            Some(EntityTransitionHash::from_bytes([5; 32])),
            EntityChainStateV1::Deleted,
        )
        .expect("deletion")
    }

    fn empty_bootstrap_manifest(
        receipt: ChangelogV2RotationReceipt,
    ) -> EntityReplicaBootstrapManifestV2 {
        EntityReplicaBootstrapManifestV2::new(
            receipt,
            receipt.predecessor(),
            receipt.v2_chain_anchor(),
            ValidatedPrefixEntityTransitionCounts {
                live_entity_count: 0,
                deleted_entity_count: 0,
                entity_transition_count: 0,
            },
            EntityTransitionFingerprint::from_sorted_heads(std::iter::empty())
                .expect("empty fingerprint"),
        )
        .expect("empty bootstrap manifest")
    }

    #[test]
    fn bootstrap_manifest_binds_rotation_identity_and_complete_head_proof() {
        let receipt =
            ChangelogV2RotationReceipt::new(database_id(), 1, DualFrontier::INITIAL, [6; 32])
                .expect("receipt");
        let manifest = empty_bootstrap_manifest(receipt);
        let mut follower = DeleteAwareEntityFollowerV2::from_bootstrap(receipt, manifest)
            .expect("matching manifest");
        follower
            .install_bootstrap_page(Vec::new(), true)
            .expect("empty manifest seals empty bootstrap");

        let other_receipt = ChangelogV2RotationReceipt::new(
            DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_001, [0x78; 10])
                .expect("other identity"),
            1,
            DualFrontier::INITIAL,
            [6; 32],
        )
        .expect("other receipt");
        assert!(matches!(
            DeleteAwareEntityFollowerV2::from_bootstrap(other_receipt, manifest),
            Err(ChangelogFrameV2Error::InvalidBinding)
        ));

        let wrong_counts = EntityReplicaBootstrapManifestV2::new(
            receipt,
            receipt.predecessor(),
            receipt.v2_chain_anchor(),
            ValidatedPrefixEntityTransitionCounts {
                live_entity_count: 1,
                deleted_entity_count: 0,
                entity_transition_count: 1,
            },
            manifest.entity_transition_fingerprint(),
        )
        .expect("structurally valid but false manifest");
        let mut follower = DeleteAwareEntityFollowerV2::from_bootstrap(receipt, wrong_counts)
            .expect("identity still matches");
        assert_eq!(
            follower.install_bootstrap_page(Vec::new(), true),
            Err(ChangelogFrameV2Error::ChecksumMismatch)
        );
    }

    #[test]
    fn v2_round_trip_retains_exact_delete_evidence_and_rejects_v1_magic() {
        let receipt =
            ChangelogV2RotationReceipt::new(database_id(), 1, DualFrontier::INITIAL, [6; 32])
                .expect("receipt");
        let covered = DualFrontier::new(Some(CommitSequence::new(2).expect("sequence")), None);
        let frame = ChangelogFrameV2::new(
            ChangelogFrameBindingV2 {
                database_id: database_id(),
                history_incarnation: 1,
                chain_hash: receipt.v2_chain_anchor(),
                journal_frame_hash: [8; 32],
                journaled: true,
            },
            DualFrontier::INITIAL,
            covered,
            vec![ChangelogEntryV2::entity_delete_tombstone(deletion()).expect("entry")],
        )
        .expect("frame");
        let encoded = frame.encode().expect("encode");
        let (decoded, hash) = ChangelogFrameV2::decode(encoded.as_bytes()).expect("decode");
        assert_eq!(decoded, frame);
        assert_eq!(hash, encoded.frame_hash());
        assert_eq!(decoded.entries()[0].delete_transition(), Some(&deletion()));
        let v1 = ChangelogFrameV1::new(
            ChangelogFrameBindingV1::new(database_id(), 1, [0; 32], [9; 32], true),
            DualFrontier::INITIAL,
            DualFrontier::new(Some(CommitSequence::first()), None),
            vec![ChangelogEntryV1::new(
                ChangelogEntryClassV1::Commit,
                CommitSequence::first().to_be_bytes().into(),
                b"legacy-commit".as_slice().into(),
            )],
        )
        .expect("v1 frame")
        .encode()
        .expect("v1 encode");
        assert_eq!(
            ChangelogFrameV2::decode(v1.as_bytes()),
            Err(ChangelogFrameV2Error::UnknownFormat)
        );
        assert_eq!(
            ChangelogFrameV1::decode(encoded.as_bytes()),
            Err(crate::ChangelogFrameError::UnknownFormat)
        );
    }

    #[test]
    fn rotation_anchored_validator_rejects_gap_chain_and_identity_without_advancing() {
        let receipt =
            ChangelogV2RotationReceipt::new(database_id(), 1, DualFrontier::INITIAL, [1; 32])
                .expect("receipt");
        let frame = ChangelogFrameV2::new(
            ChangelogFrameBindingV2 {
                database_id: database_id(),
                history_incarnation: 1,
                chain_hash: receipt.v2_chain_anchor(),
                journal_frame_hash: [2; 32],
                journaled: true,
            },
            DualFrontier::INITIAL,
            DualFrontier::new(Some(CommitSequence::first()), None),
            vec![
                ChangelogEntryV2::put(
                    ChangelogEntryClassV2::Commit,
                    [0, 0, 0, 0, 0, 0, 0, 1],
                    b"commit".as_slice(),
                )
                .expect("entry"),
            ],
        )
        .expect("frame");
        let encoded = frame.encode().expect("encode");
        let mut validator = ChangelogStreamValidatorV2::from_rotation(receipt);
        assert_eq!(validator.accept(encoded.as_bytes()).expect("accept"), frame);
        assert_eq!(
            validator.accept(encoded.as_bytes()),
            Err(ChangelogFrameV2Error::Gap)
        );
    }

    #[test]
    fn tombstone_requires_a_delete_and_debug_is_redacted() {
        let create = CommittedEntityTransitionV1::new(
            CommitSequence::first(),
            0,
            target(),
            EntityChainStateV1::NeverExisted,
            0,
            None,
            EntityChainStateV1::Live {
                version: EntityVersion::first(),
                value_hash: EntityRecordHash::from_bytes([0xaa; 32]),
            },
        )
        .expect("create");
        assert_eq!(
            ChangelogEntryV2::entity_delete_tombstone(create),
            Err(ChangelogFrameV2Error::InvalidEntry)
        );
        let entry = ChangelogEntryV2::entity_delete_tombstone(deletion()).expect("delete");
        let rendered = format!("{entry:?}");
        assert!(rendered.contains("[REDACTED]"));
        assert!(!rendered.contains("commit"));
    }
}
