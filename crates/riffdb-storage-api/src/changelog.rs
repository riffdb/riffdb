//! Changelog frames derived from published durable frontiers (ADR-0100).
//!
//! # What this module is
//!
//! ADR-0093 defines the authoritative changelog; ADR-0100 §1 makes its unit of
//! shipping the **changelog frame**: every ADR-0093 §1 entry newly covered when
//! the primary publishes one durable frontier advancement. This module owns the
//! engine-neutral half of that contract:
//!
//! - [`ChangelogPublicationPort`] — the observer storage calls at its
//!   publication edge (ADR-0101 §4). It is a *wakeup carrying a pinned
//!   published snapshot*, never a data channel, and it can neither stall nor
//!   fail the writer.
//! - [`PublishedDurableSnapshot`] — the **only** read capability an emitter is
//!   ever handed. It is already-published durable state by construction, which
//!   is how ADR-0100 §2's "never reads writer-private applied roots" becomes a
//!   structural property rather than a review rule.
//! - [`ChangelogFrameV1`] and its closed, versioned, checksummed encoding.
//! - [`ChangelogStreamValidatorV1`] — the fail-closed decode: database, history
//!   incarnation, gap-freeness, hash chain, checksum, and torn-tail
//!   discrimination.
//!
//! # What this module is deliberately not
//!
//! Per ADR-0101 §6 and ADR-0104 §8, the local journal's frames and the physical
//! composite overlay are **not** a replication format. Nothing here shares an
//! encoder, a magic, a version space, or a byte layout with
//! `riffdb-storage-redb`'s journal. The two boundaries coincide by design; the
//! two encodings are separate on purpose, and unifying them would need its own
//! accepted amendment.

use std::fmt;
use std::sync::Arc;

use riffdb_types::{
    AdministrationSequence, CommitSequence, DUAL_FRONTIER_BYTES, DatabaseId, DualFrontier,
};
use sha2::{Digest, Sha256};

use crate::composite_view::{CompositeRow, CompositeTableV1};
use crate::error::StorageError;

/// Frame magic for the changelog encoding. Deliberately distinct from every
/// journal magic (ADR-0101 §6): a journal frame must never decode as a
/// changelog frame, or the reverse.
const CHANGELOG_FRAME_MAGIC: [u8; 8] = *b"RDBCLF01";
/// Footer magic closing a complete frame.
const CHANGELOG_FOOTER_MAGIC: [u8; 8] = *b"RDBCLE01";
/// The only encoding version this build writes or accepts.
const CHANGELOG_FORMAT_VERSION: u16 = 1;
/// Digest width used for the chain hash, frame hash, and checksum.
const HASH_BYTES: usize = 32;

/// Number of closed changelog entry classes.
pub const CHANGELOG_ENTRY_CLASS_COUNT: usize = 8;

/// Fixed header byte length.
const HEADER_BYTES: usize = 8  // magic
    + 2  // version
    + 1  // flags
    + 1  // reserved
    + 16 // database id
    + 8  // history incarnation
    + DUAL_FRONTIER_BYTES // predecessor
    + DUAL_FRONTIER_BYTES // covered
    + 4  // entry count
    + 4 * CHANGELOG_ENTRY_CLASS_COUNT // per-class counts
    + 4  // payload length
    + HASH_BYTES // chain hash
    + HASH_BYTES; // bound journal frame hash

/// Fixed footer byte length.
const FOOTER_BYTES: usize = 8 + 8 + HASH_BYTES;

/// Per-entry fixed prefix: class, three reserved bytes, key length, value length.
const ENTRY_PREFIX_BYTES: usize = 1 + 3 + 4 + 4;

/// Flag bit: the covering durability fence was a journal flush.
const FLAG_JOURNALED: u8 = 0b0000_0001;

/// Hard ceiling on one encoded changelog frame.
///
/// ADR-0100 §3 inherits the ADR-0101 §3 flush-cover ceiling (16 MiB of encoded
/// journal bytes per flush). A derived frame restates the same covered rows
/// under a different framing, so this doubles that ceiling for per-entry
/// framing headroom and still bounds a follower's apply transaction by a limit
/// the primary's storage engine already accepts.
pub const MAX_CHANGELOG_FRAME_BYTES: usize = 32 * 1024 * 1024;

/// Hard ceiling on entries in one changelog frame.
pub const MAX_CHANGELOG_FRAME_ENTRIES: usize = 262_144;

/// The closed set of ADR-0093 §1 entry classes RiffDB derives today.
///
/// Each class names one authoritative table and one attribution rule from the
/// covered sequence interval. The set is closed so a follower can reject an
/// unknown class rather than silently skipping durable state.
///
/// Classes deliberately **not** in v1, recorded so the omission is never
/// mistaken for completeness: secondary index rows and index-epoch records
/// (derived, rebuildable from entity state), idempotency and pending-admission
/// rows (identity-keyed with no mapped per-sequence attribution), meta
/// allocator counters (derivable from the applied frontier), and the
/// control-plane registries — capabilities, contract bundles and migrations,
/// query and reactive modules, consumers, projections (administration-attributed
/// but not yet mapped). Each needs its own attribution rule and its own
/// exactness evidence before a follower may claim to hold them.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum ChangelogEntryClassV1 {
    /// The authoritative commit record for one covered application sequence.
    Commit = 1,
    /// One durable event of a covered commit.
    Event = 2,
    /// The partition route row of a covered event.
    EventRoute = 3,
    /// The outbox intent row of a covered event.
    OutboxIntent = 4,
    /// The provenance record of a covered commit.
    Provenance = 5,
    /// An entity post-image attributed through the ADR-0083 supersession chain.
    Entity = 6,
    /// The administration audit record for one covered administration sequence.
    AdministrationAudit = 7,
    /// The request-lookup row of a covered service-audit record.
    ServiceAuditRequestIndex = 8,
}

impl ChangelogEntryClassV1 {
    /// Every class in durable tag order.
    pub const ALL: [Self; CHANGELOG_ENTRY_CLASS_COUNT] = [
        Self::Commit,
        Self::Event,
        Self::EventRoute,
        Self::OutboxIntent,
        Self::Provenance,
        Self::Entity,
        Self::AdministrationAudit,
        Self::ServiceAuditRequestIndex,
    ];

    /// Returns the authoritative table this class writes into.
    #[must_use]
    pub const fn table(self) -> CompositeTableV1 {
        match self {
            Self::Commit => CompositeTableV1::Commits,
            Self::Event => CompositeTableV1::Events,
            Self::EventRoute => CompositeTableV1::EventRoutes,
            Self::OutboxIntent => CompositeTableV1::Outbox,
            Self::Provenance => CompositeTableV1::Provenance,
            Self::Entity => CompositeTableV1::Entities,
            Self::AdministrationAudit => CompositeTableV1::Audit,
            Self::ServiceAuditRequestIndex => CompositeTableV1::AuditByRequest,
        }
    }

    /// Returns the closed durable tag.
    #[must_use]
    pub const fn tag(self) -> u8 {
        self as u8
    }

    /// Decodes a closed durable tag, failing closed on anything unknown.
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
            _ => None,
        }
    }

    const fn index(self) -> usize {
        self as usize - 1
    }
}

/// One authoritative row bound to a covered sequence.
///
/// v1 entries are inserts or replacements only: every class in
/// [`ChangelogEntryClassV1`] is insert-once or last-writer-wins within a frame,
/// so a follower applies a frame as a set of puts. Deletions belong to the
/// classes deferred out of v1 and would need their own before-image evidence.
#[derive(Clone, Eq, PartialEq)]
pub struct ChangelogEntryV1 {
    class: ChangelogEntryClassV1,
    key: Box<[u8]>,
    value: Box<[u8]>,
}

impl ChangelogEntryV1 {
    /// Builds one entry from its class and exact physical row.
    #[must_use]
    pub fn new(class: ChangelogEntryClassV1, key: Box<[u8]>, value: Box<[u8]>) -> Self {
        Self { class, key, value }
    }

    /// Returns the closed entry class.
    #[must_use]
    pub const fn class(&self) -> ChangelogEntryClassV1 {
        self.class
    }

    /// Returns the authoritative table this entry applies to.
    #[must_use]
    pub const fn table(&self) -> CompositeTableV1 {
        self.class.table()
    }

    /// Borrows the exact canonical physical key.
    #[must_use]
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Borrows the exact canonical stored value.
    #[must_use]
    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

impl fmt::Debug for ChangelogEntryV1 {
    /// Redacted: class and byte lengths only. Entry payloads are business data.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChangelogEntryV1")
            .field("class", &self.class)
            .field("key_bytes", &self.key.len())
            .field("value_bytes", &self.value.len())
            .finish()
    }
}

/// The ADR-0100 §1 changelog frame header.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChangelogFrameHeaderV1 {
    database_id: DatabaseId,
    history_incarnation: u64,
    predecessor: DualFrontier,
    covered: DualFrontier,
    entry_counts: [u32; CHANGELOG_ENTRY_CLASS_COUNT],
    chain_hash: [u8; HASH_BYTES],
    journal_frame_hash: [u8; HASH_BYTES],
    journaled: bool,
}

impl ChangelogFrameHeaderV1 {
    /// Returns the database lineage this frame belongs to.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Returns the history incarnation binding (ADR-0086 fail-closed identity).
    #[must_use]
    pub const fn history_incarnation(&self) -> u64 {
        self.history_incarnation
    }

    /// Returns the predecessor dual frontier this frame continues from.
    #[must_use]
    pub const fn predecessor(&self) -> DualFrontier {
        self.predecessor
    }

    /// Returns the dual frontier this frame covers.
    #[must_use]
    pub const fn covered(&self) -> DualFrontier {
        self.covered
    }

    /// Returns per-class entry counts in [`ChangelogEntryClassV1::ALL`] order.
    #[must_use]
    pub const fn entry_counts(&self) -> &[u32; CHANGELOG_ENTRY_CLASS_COUNT] {
        &self.entry_counts
    }

    /// Returns the total entry count.
    #[must_use]
    pub fn entry_count(&self) -> u64 {
        self.entry_counts.iter().copied().map(u64::from).sum()
    }

    /// Returns the frame hash of the preceding changelog frame (zeros for the first).
    #[must_use]
    pub const fn chain_hash(&self) -> [u8; HASH_BYTES] {
        self.chain_hash
    }

    /// Returns the authoritative-chain binding: the journal frame hash the
    /// covering publication carried.
    ///
    /// This is publication metadata, not journal bytes. It lets a follower and
    /// an operator tie a derived changelog frame back to the exact local
    /// durability fence that released it, without the changelog inheriting the
    /// journal's encoding (ADR-0101 §6).
    #[must_use]
    pub const fn journal_frame_hash(&self) -> [u8; HASH_BYTES] {
        self.journal_frame_hash
    }

    /// Returns true when the covering fence was a journal flush.
    #[must_use]
    pub const fn journaled(&self) -> bool {
        self.journaled
    }
}

/// A complete derived changelog frame.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangelogFrameV1 {
    header: ChangelogFrameHeaderV1,
    entries: Vec<ChangelogEntryV1>,
}

/// The identity and chain binding every frame of one stream shares.
///
/// Kept separate from the frontier pair so a frame's *address* (predecessor,
/// covered) and its *binding* (lineage, incarnation, chain, covering journal
/// fence) never blur into one another.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChangelogFrameBindingV1 {
    database_id: DatabaseId,
    history_incarnation: u64,
    chain_hash: [u8; HASH_BYTES],
    journal_frame_hash: [u8; HASH_BYTES],
    journaled: bool,
}

impl ChangelogFrameBindingV1 {
    /// Builds the shared binding for one frame.
    #[must_use]
    pub const fn new(
        database_id: DatabaseId,
        history_incarnation: u64,
        chain_hash: [u8; HASH_BYTES],
        journal_frame_hash: [u8; HASH_BYTES],
        journaled: bool,
    ) -> Self {
        Self {
            database_id,
            history_incarnation,
            chain_hash,
            journal_frame_hash,
            journaled,
        }
    }
}

impl ChangelogFrameV1 {
    /// Assembles a frame, checking every closed structural bound.
    ///
    /// Entries must already be in canonical order — ascending by
    /// `(class tag, key)` — with no duplicate `(class, key)`. Canonical order
    /// is what makes the frame hash and checksum reproducible, so a
    /// noncanonical input is refused rather than sorted.
    pub fn new(
        binding: ChangelogFrameBindingV1,
        predecessor: DualFrontier,
        covered: DualFrontier,
        entries: Vec<ChangelogEntryV1>,
    ) -> Result<Self, ChangelogFrameError> {
        if !covered.advances_from(predecessor) {
            return Err(ChangelogFrameError::FrontierDoesNotAdvance);
        }
        if entries.len() > MAX_CHANGELOG_FRAME_ENTRIES {
            return Err(ChangelogFrameError::LimitExceeded);
        }
        let mut entry_counts = [0_u32; CHANGELOG_ENTRY_CLASS_COUNT];
        let mut previous: Option<(u8, &[u8])> = None;
        for entry in &entries {
            let current = (entry.class.tag(), entry.key());
            if let Some(previous) = previous
                && previous >= current
            {
                return Err(ChangelogFrameError::NonCanonicalOrder);
            }
            previous = Some(current);
            let slot = &mut entry_counts[entry.class.index()];
            *slot = slot
                .checked_add(1)
                .ok_or(ChangelogFrameError::LimitExceeded)?;
        }
        let frame = Self {
            header: ChangelogFrameHeaderV1 {
                database_id: binding.database_id,
                history_incarnation: binding.history_incarnation,
                predecessor,
                covered,
                entry_counts,
                chain_hash: binding.chain_hash,
                journal_frame_hash: binding.journal_frame_hash,
                journaled: binding.journaled,
            },
            entries,
        };
        if frame.encoded_len()? > MAX_CHANGELOG_FRAME_BYTES {
            return Err(ChangelogFrameError::LimitExceeded);
        }
        Ok(frame)
    }

    /// Borrows the frame header.
    #[must_use]
    pub const fn header(&self) -> &ChangelogFrameHeaderV1 {
        &self.header
    }

    /// Borrows the canonically ordered entries.
    #[must_use]
    pub fn entries(&self) -> &[ChangelogEntryV1] {
        &self.entries
    }

    fn payload_len(&self) -> Result<usize, ChangelogFrameError> {
        let mut total = 0_usize;
        for entry in &self.entries {
            total = total
                .checked_add(ENTRY_PREFIX_BYTES)
                .and_then(|value| value.checked_add(entry.key.len()))
                .and_then(|value| value.checked_add(entry.value.len()))
                .ok_or(ChangelogFrameError::LimitExceeded)?;
        }
        Ok(total)
    }

    fn encoded_len(&self) -> Result<usize, ChangelogFrameError> {
        HEADER_BYTES
            .checked_add(self.payload_len()?)
            .and_then(|value| value.checked_add(FOOTER_BYTES))
            .ok_or(ChangelogFrameError::LimitExceeded)
    }

    /// Encodes the frame into its canonical, checksummed, torn-tail-discriminable form.
    ///
    /// Layout, deliberately its own thing (ADR-0101 §6, ADR-0104 §8):
    ///
    /// ```text
    /// header  magic | version | flags | reserved | database id | incarnation
    ///         | predecessor pair | covered pair | entry count | per-class counts
    ///         | payload length | chain hash | bound journal frame hash
    /// payload class | reserved*3 | key length | value length | key | value  (repeated)
    /// footer  footer magic | total length | checksum
    /// ```
    ///
    /// The footer's explicit total length plus the fixed footer magic is what
    /// makes a truncated tail *discriminable* rather than merely invalid: a
    /// reader that cannot find a complete footer knows it holds a torn frame,
    /// not a corrupt one.
    pub fn encode(&self) -> Result<EncodedChangelogFrameV1, ChangelogFrameError> {
        let payload_len = self.payload_len()?;
        let total_len = self.encoded_len()?;
        if total_len > MAX_CHANGELOG_FRAME_BYTES {
            return Err(ChangelogFrameError::LimitExceeded);
        }
        let mut bytes = Vec::with_capacity(total_len);
        bytes.extend_from_slice(&CHANGELOG_FRAME_MAGIC);
        bytes.extend_from_slice(&CHANGELOG_FORMAT_VERSION.to_be_bytes());
        bytes.push(if self.header.journaled {
            FLAG_JOURNALED
        } else {
            0
        });
        bytes.push(0);
        bytes.extend_from_slice(self.header.database_id.as_bytes());
        bytes.extend_from_slice(&self.header.history_incarnation.to_be_bytes());
        bytes.extend_from_slice(&self.header.predecessor.to_canonical_bytes());
        bytes.extend_from_slice(&self.header.covered.to_canonical_bytes());
        bytes.extend_from_slice(
            &u32::try_from(self.entries.len())
                .map_err(|_| ChangelogFrameError::LimitExceeded)?
                .to_be_bytes(),
        );
        for count in self.header.entry_counts {
            bytes.extend_from_slice(&count.to_be_bytes());
        }
        bytes.extend_from_slice(
            &u32::try_from(payload_len)
                .map_err(|_| ChangelogFrameError::LimitExceeded)?
                .to_be_bytes(),
        );
        bytes.extend_from_slice(&self.header.chain_hash);
        bytes.extend_from_slice(&self.header.journal_frame_hash);
        debug_assert_eq!(bytes.len(), HEADER_BYTES);
        for entry in &self.entries {
            bytes.push(entry.class.tag());
            bytes.extend_from_slice(&[0, 0, 0]);
            bytes.extend_from_slice(
                &u32::try_from(entry.key.len())
                    .map_err(|_| ChangelogFrameError::LimitExceeded)?
                    .to_be_bytes(),
            );
            bytes.extend_from_slice(
                &u32::try_from(entry.value.len())
                    .map_err(|_| ChangelogFrameError::LimitExceeded)?
                    .to_be_bytes(),
            );
            bytes.extend_from_slice(&entry.key);
            bytes.extend_from_slice(&entry.value);
        }
        bytes.extend_from_slice(&CHANGELOG_FOOTER_MAGIC);
        bytes.extend_from_slice(
            &u64::try_from(total_len)
                .map_err(|_| ChangelogFrameError::LimitExceeded)?
                .to_be_bytes(),
        );
        let checksum = digest(&bytes);
        bytes.extend_from_slice(&checksum);
        debug_assert_eq!(bytes.len(), total_len);
        Ok(EncodedChangelogFrameV1 {
            bytes: Arc::from(bytes),
            frame_hash: checksum,
        })
    }

    /// Decodes a complete frame, checking magic, version, reserved bytes,
    /// declared lengths, footer, and checksum. Structural only: continuity and
    /// identity binding belong to [`ChangelogStreamValidatorV1`].
    pub fn decode(bytes: &[u8]) -> Result<(Self, [u8; HASH_BYTES]), ChangelogFrameError> {
        if bytes.len() < HEADER_BYTES + FOOTER_BYTES {
            return Err(ChangelogFrameError::Truncated);
        }
        if bytes[..8] != CHANGELOG_FRAME_MAGIC {
            return Err(ChangelogFrameError::UnknownFormat);
        }
        if read_u16(bytes, 8)? != CHANGELOG_FORMAT_VERSION {
            return Err(ChangelogFrameError::UnknownVersion);
        }
        let flags = bytes[10];
        if flags & !FLAG_JOURNALED != 0 || bytes[11] != 0 {
            return Err(ChangelogFrameError::InvalidValue);
        }
        let journaled = flags & FLAG_JOURNALED != 0;
        let database_id = DatabaseId::from_bytes(
            bytes[12..28]
                .try_into()
                .map_err(|_| ChangelogFrameError::InvalidValue)?,
        )
        .map_err(|_| ChangelogFrameError::InvalidValue)?;
        let history_incarnation = read_u64(bytes, 28)?;
        let predecessor = DualFrontier::from_canonical_bytes(
            bytes[36..36 + DUAL_FRONTIER_BYTES]
                .try_into()
                .map_err(|_| ChangelogFrameError::InvalidValue)?,
        )
        .map_err(|_| ChangelogFrameError::InvalidValue)?;
        let covered = DualFrontier::from_canonical_bytes(
            bytes[54..54 + DUAL_FRONTIER_BYTES]
                .try_into()
                .map_err(|_| ChangelogFrameError::InvalidValue)?,
        )
        .map_err(|_| ChangelogFrameError::InvalidValue)?;
        let entry_count = usize::try_from(read_u32(bytes, 72)?)
            .map_err(|_| ChangelogFrameError::LimitExceeded)?;
        if entry_count > MAX_CHANGELOG_FRAME_ENTRIES {
            return Err(ChangelogFrameError::LimitExceeded);
        }
        let mut declared_counts = [0_u32; CHANGELOG_ENTRY_CLASS_COUNT];
        for (index, slot) in declared_counts.iter_mut().enumerate() {
            *slot = read_u32(
                bytes,
                76 + index
                    .checked_mul(4)
                    .ok_or(ChangelogFrameError::LimitExceeded)?,
            )?;
        }
        let payload_len = usize::try_from(read_u32(bytes, 76 + 4 * CHANGELOG_ENTRY_CLASS_COUNT)?)
            .map_err(|_| ChangelogFrameError::LimitExceeded)?;
        let chain_hash = read_hash(bytes, 76 + 4 * CHANGELOG_ENTRY_CLASS_COUNT + 4)?;
        let journal_frame_hash =
            read_hash(bytes, 76 + 4 * CHANGELOG_ENTRY_CLASS_COUNT + 4 + HASH_BYTES)?;
        let expected_len = HEADER_BYTES
            .checked_add(payload_len)
            .and_then(|value| value.checked_add(FOOTER_BYTES))
            .ok_or(ChangelogFrameError::LimitExceeded)?;
        if expected_len > MAX_CHANGELOG_FRAME_BYTES {
            return Err(ChangelogFrameError::LimitExceeded);
        }
        if bytes.len() < expected_len {
            return Err(ChangelogFrameError::Truncated);
        }
        if bytes.len() != expected_len {
            return Err(ChangelogFrameError::TrailingBytes);
        }
        let footer_start = HEADER_BYTES + payload_len;
        if bytes[footer_start..footer_start + 8] != CHANGELOG_FOOTER_MAGIC {
            return Err(ChangelogFrameError::Truncated);
        }
        let declared_total = read_u64(bytes, footer_start + 8)?;
        if declared_total != u64::try_from(expected_len).unwrap_or(u64::MAX) {
            return Err(ChangelogFrameError::InvalidValue);
        }
        let checksum = read_hash(bytes, footer_start + 16)?;
        if digest(&bytes[..footer_start + 16]) != checksum {
            return Err(ChangelogFrameError::ChecksumMismatch);
        }

        let mut entries = Vec::with_capacity(entry_count);
        let mut cursor = HEADER_BYTES;
        let mut previous: Option<(u8, Vec<u8>)> = None;
        for _ in 0..entry_count {
            if cursor
                .checked_add(ENTRY_PREFIX_BYTES)
                .is_none_or(|end| end > footer_start)
            {
                return Err(ChangelogFrameError::Truncated);
            }
            let class = ChangelogEntryClassV1::from_tag(bytes[cursor])
                .ok_or(ChangelogFrameError::UnknownEntryClass)?;
            if bytes[cursor + 1..cursor + 4] != [0, 0, 0] {
                return Err(ChangelogFrameError::InvalidValue);
            }
            let key_len = usize::try_from(read_u32(bytes, cursor + 4)?).unwrap_or(usize::MAX);
            let value_len = usize::try_from(read_u32(bytes, cursor + 8)?).unwrap_or(usize::MAX);
            let key_start = cursor + ENTRY_PREFIX_BYTES;
            let value_start = key_start
                .checked_add(key_len)
                .ok_or(ChangelogFrameError::LimitExceeded)?;
            let next = value_start
                .checked_add(value_len)
                .ok_or(ChangelogFrameError::LimitExceeded)?;
            if next > footer_start || key_len == 0 {
                return Err(ChangelogFrameError::InvalidValue);
            }
            let key = bytes[key_start..value_start].to_vec();
            let current = (class.tag(), key);
            if let Some(previous) = previous.as_ref()
                && *previous >= current
            {
                return Err(ChangelogFrameError::NonCanonicalOrder);
            }
            entries.push(ChangelogEntryV1::new(
                class,
                current.1.clone().into_boxed_slice(),
                bytes[value_start..next].to_vec().into_boxed_slice(),
            ));
            previous = Some(current);
            cursor = next;
        }
        if cursor != footer_start {
            return Err(ChangelogFrameError::TrailingBytes);
        }
        let frame = Self::new(
            ChangelogFrameBindingV1::new(
                database_id,
                history_incarnation,
                chain_hash,
                journal_frame_hash,
                journaled,
            ),
            predecessor,
            covered,
            entries,
        )?;
        if frame.header.entry_counts != declared_counts {
            return Err(ChangelogFrameError::CountMismatch);
        }
        Ok((frame, checksum))
    }
}

/// One encoded changelog frame plus its chain-forming hash.
#[derive(Clone, Eq, PartialEq)]
pub struct EncodedChangelogFrameV1 {
    bytes: Arc<[u8]>,
    frame_hash: [u8; HASH_BYTES],
}

impl EncodedChangelogFrameV1 {
    /// Borrows the complete encoded frame.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Returns the hash the successor frame carries as its chain hash.
    #[must_use]
    pub const fn frame_hash(&self) -> [u8; HASH_BYTES] {
        self.frame_hash
    }
}

impl fmt::Debug for EncodedChangelogFrameV1 {
    /// Redacted: length only. Encoded frames carry business data.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EncodedChangelogFrameV1")
            .field("encoded_bytes", &self.bytes.len())
            .finish()
    }
}

/// The fail-closed validating decoder for a changelog stream.
///
/// The validator is the ADR-0093 §1 "exact, gap-free" predicate made
/// executable. It holds the expected identity and the expected continuation
/// point, and refuses anything that does not continue the chain **exactly**:
/// there is no recovery-by-skipping and no best-effort acceptance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangelogStreamValidatorV1 {
    database_id: DatabaseId,
    history_incarnation: u64,
    expected_predecessor: DualFrontier,
    expected_chain_hash: [u8; HASH_BYTES],
}

impl ChangelogStreamValidatorV1 {
    /// Anchors a validator at a known frame boundary.
    ///
    /// `anchor_chain_hash` is the frame hash of the last accepted frame, or
    /// zeros when anchoring at the initial frontier.
    #[must_use]
    pub const fn anchored_at(
        database_id: DatabaseId,
        history_incarnation: u64,
        anchor: DualFrontier,
        anchor_chain_hash: [u8; HASH_BYTES],
    ) -> Self {
        Self {
            database_id,
            history_incarnation,
            expected_predecessor: anchor,
            expected_chain_hash: anchor_chain_hash,
        }
    }

    /// Returns the frontier the next frame must name as its predecessor.
    #[must_use]
    pub const fn expected_predecessor(&self) -> DualFrontier {
        self.expected_predecessor
    }

    /// Returns the chain hash the next frame must carry.
    #[must_use]
    pub const fn expected_chain_hash(&self) -> [u8; HASH_BYTES] {
        self.expected_chain_hash
    }

    /// Validates and accepts the next encoded frame, advancing the anchor.
    ///
    /// On any failure the validator is left unchanged, so a caller that
    /// retries with the correct frame resumes exactly where it stopped.
    pub fn accept(&mut self, bytes: &[u8]) -> Result<ChangelogFrameV1, ChangelogFrameError> {
        let (frame, frame_hash) = ChangelogFrameV1::decode(bytes)?;
        if frame.header.database_id != self.database_id {
            return Err(ChangelogFrameError::DatabaseMismatch);
        }
        if frame.header.history_incarnation != self.history_incarnation {
            return Err(ChangelogFrameError::IncarnationMismatch);
        }
        if frame.header.predecessor != self.expected_predecessor {
            return Err(ChangelogFrameError::Gap);
        }
        if frame.header.chain_hash != self.expected_chain_hash {
            return Err(ChangelogFrameError::ChainMismatch);
        }
        self.expected_predecessor = frame.header.covered;
        self.expected_chain_hash = frame_hash;
        Ok(frame)
    }
}

/// A safe failure to build, encode, decode, or continue a changelog frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChangelogFrameError {
    /// The encoded bytes end before a complete frame.
    Truncated,
    /// Bytes remain after a complete frame.
    TrailingBytes,
    /// The frame magic is not a changelog frame magic.
    UnknownFormat,
    /// The encoding version is not supported by this build.
    UnknownVersion,
    /// A reserved byte, tag, or declared value is invalid.
    InvalidValue,
    /// An entry class tag is outside the closed set.
    UnknownEntryClass,
    /// The frame checksum does not match its contents.
    ChecksumMismatch,
    /// Per-class counts disagree with the decoded entries.
    CountMismatch,
    /// Entries are not in canonical `(class, key)` order or repeat a key.
    NonCanonicalOrder,
    /// The covered frontier does not strictly advance the predecessor.
    FrontierDoesNotAdvance,
    /// A closed structural bound was exceeded.
    LimitExceeded,
    /// The frame's predecessor frontier is not the expected continuation point.
    Gap,
    /// The frame's chain hash is not the prior frame's hash.
    ChainMismatch,
    /// The frame belongs to a different database lineage.
    DatabaseMismatch,
    /// The frame belongs to a different history incarnation.
    IncarnationMismatch,
}

impl fmt::Display for ChangelogFrameError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Truncated => "changelog frame bytes are truncated",
            Self::TrailingBytes => "changelog frame bytes have a trailing remainder",
            Self::UnknownFormat => "bytes are not a changelog frame",
            Self::UnknownVersion => "changelog frame version is unsupported",
            Self::InvalidValue => "changelog frame contains an invalid value",
            Self::UnknownEntryClass => "changelog frame contains an unknown entry class",
            Self::ChecksumMismatch => "changelog frame checksum does not match",
            Self::CountMismatch => "changelog frame entry counts do not match",
            Self::NonCanonicalOrder => "changelog frame entries are not canonically ordered",
            Self::FrontierDoesNotAdvance => "changelog frame does not advance its frontier",
            Self::LimitExceeded => "changelog frame exceeds a hard limit",
            Self::Gap => "changelog frame does not continue the expected frontier",
            Self::ChainMismatch => "changelog frame does not continue the expected hash chain",
            Self::DatabaseMismatch => "changelog frame belongs to another database",
            Self::IncarnationMismatch => "changelog frame belongs to another history incarnation",
        })
    }
}

impl std::error::Error for ChangelogFrameError {}

/// A pinned, already-published durable snapshot.
///
/// This trait is the whole of an emitter's read capability. It is handed to the
/// emitter by storage at the publication edge, *after* the frontier swap, and
/// it names the exact snapshot that publication installed. An emitter holding
/// one of these cannot reach a writer-private applied root, an unflushed
/// subgroup, a journal byte, or a composite overlay internal — not because it
/// declines to, but because no method exists. That is ADR-0100 §2 discharged
/// structurally.
///
/// Implementations must be immutable: the pinned snapshot never observes a
/// later publication, which is what lets an emitter derive asynchronously
/// without ever reading state past its covered frontier.
pub trait PublishedDurableSnapshot: Send + Sync {
    /// Observes bounded source-only follower acknowledgements and the published
    /// head from this pin. No application rows or receipt population are scanned.
    /// Legacy backends refuse rather than inventing an empty follower inventory.
    fn replication_source_progress_v3(
        &self,
    ) -> Result<crate::ReplicationSourceProgressV3, crate::ChangelogCursorErrorV3> {
        Err(crate::StorageError::new(crate::StorageErrorKind::IncompatibleFormat, None).into())
    }

    /// Opens the complete catalog-owned authoritative inventory at this pin.
    /// A legacy backend refuses instead of offering a partial bootstrap.
    fn authoritative_state_v3(
        &self,
    ) -> Result<Box<dyn crate::AuthoritativeStateCursorV3>, crate::ChangelogCursorErrorV3> {
        Err(crate::StorageError::new(crate::StorageErrorKind::IncompatibleFormat, None).into())
    }

    /// Opens an exact V3 successor cursor, fenced to this immutable snapshot.
    /// Legacy-only implementations refuse; this default never selects a fallback.
    fn changelog_receipts_v3(
        &self,
        _lineage: crate::ChangelogLineageV3,
        _after: crate::ChangelogHistoryPointV3,
    ) -> Result<Box<dyn crate::ChangelogReceiptCursorV3>, crate::ChangelogCursorErrorV3> {
        Err(crate::StorageError::new(crate::StorageErrorKind::IncompatibleFormat, None).into())
    }

    /// Reads one exact row from the pinned published snapshot.
    fn read_value(
        &self,
        table: CompositeTableV1,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StorageError>;

    /// Reads a bounded ascending key range from the pinned published snapshot.
    fn read_range(
        &self,
        table: CompositeTableV1,
        start_inclusive: &[u8],
        end_exclusive: &[u8],
        max_rows: usize,
    ) -> Result<Vec<CompositeRow>, StorageError>;

    /// Returns the snapshot's own published application commit frontier.
    fn application_frontier(&self) -> Result<Option<CommitSequence>, StorageError>;

    /// Returns the snapshot's own published administration frontier.
    fn administration_frontier(&self) -> Result<Option<AdministrationSequence>, StorageError>;
}

/// One observed publication of a durable frontier advancement (ADR-0101 §4).
///
/// The value is a **wakeup plus a pinned snapshot**, never a data channel: it
/// carries no entries, no journal bytes, and no writer state. Everything an
/// emitter needs is either in the header fields or reachable through
/// [`PublishedDurableSnapshot`].
#[derive(Clone)]
pub struct PublishedFrontierAdvancement {
    predecessor: DualFrontier,
    covered: DualFrontier,
    frame_hash: [u8; HASH_BYTES],
    transition_count: u32,
    journaled: bool,
    snapshot: Arc<dyn PublishedDurableSnapshot>,
}

impl PublishedFrontierAdvancement {
    /// Builds one advancement observation.
    #[must_use]
    pub fn new(
        predecessor: DualFrontier,
        covered: DualFrontier,
        frame_hash: [u8; HASH_BYTES],
        transition_count: u32,
        journaled: bool,
        snapshot: Arc<dyn PublishedDurableSnapshot>,
    ) -> Self {
        Self {
            predecessor,
            covered,
            frame_hash,
            transition_count,
            journaled,
            snapshot,
        }
    }

    /// Returns the frontier published before this advancement.
    #[must_use]
    pub const fn predecessor(&self) -> DualFrontier {
        self.predecessor
    }

    /// Returns the frontier this advancement published.
    #[must_use]
    pub const fn covered(&self) -> DualFrontier {
        self.covered
    }

    /// Returns the covering journal frame hash (publication metadata).
    #[must_use]
    pub const fn frame_hash(&self) -> [u8; HASH_BYTES] {
        self.frame_hash
    }

    /// Returns the logical writer transitions this publication covered.
    #[must_use]
    pub const fn transition_count(&self) -> u32 {
        self.transition_count
    }

    /// Returns true when a journal flush covered this publication.
    #[must_use]
    pub const fn journaled(&self) -> bool {
        self.journaled
    }

    /// Borrows the pinned published snapshot.
    #[must_use]
    pub fn snapshot(&self) -> &Arc<dyn PublishedDurableSnapshot> {
        &self.snapshot
    }
}

impl fmt::Debug for PublishedFrontierAdvancement {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PublishedFrontierAdvancement")
            .field("predecessor", &self.predecessor)
            .field("covered", &self.covered)
            .field("transition_count", &self.transition_count)
            .field("journaled", &self.journaled)
            .finish_non_exhaustive()
    }
}

/// Observes durable-frontier publications at the storage boundary.
///
/// The contract is deliberately narrow and one-way, mirroring the fail-closed
/// posture of `ApplicationCommitNotificationSink` while inverting its failure
/// mode: a commit notification may refuse, but a publication observation may
/// not, because the frontier is *already published* by the time this is called.
/// Refusing would fence a durable write that already happened.
///
/// Implementations MUST therefore:
///
/// - return without blocking — no I/O, no unbounded wait, no lock held across
///   work, no allocation-heavy path;
/// - never panic;
/// - absorb their own overflow as a typed lagging state rather than applying
///   backpressure to the writer.
///
/// Storage calls this strictly **after** the frontier swap, at the same release
/// rank as every other covered effect (ADR-0101 §4).
///
/// The legacy advancement callback represents command/service-audit frontier
/// publication. V3 additionally reports pinned direct, source-control, and
/// lifecycle publications, including receipts that advance neither frontier.
/// V3 consumers follow the snapshot's retained receipt cursor from an exact
/// history point; they never derive mutations from current row values or from
/// the legacy frontier interval. Repeated/coalesced pins are harmless: the
/// cursor owns continuity and refuses missing or pruned history.
///
/// Complete replication still requires an exact-fence bootstrap before tail
/// attachment. A publication notification is neither a bootstrap manifest nor
/// a receiver acknowledgement and must never release a retention hold.
pub trait ChangelogPublicationPort: Send + Sync + fmt::Debug {
    /// Observes one published durable-frontier advancement.
    fn observe_published_advancement(&self, advancement: PublishedFrontierAdvancement);

    /// Observes a V3 publication whose physical receipt position may advance
    /// without either application or administration frontier advancing. Legacy
    /// observers ignore this edge; production V3 observers enqueue only the pin
    /// and read its receipt cursor after leaving the publication callback.
    fn observe_published_snapshot_v3(&self, _snapshot: Arc<dyn PublishedDurableSnapshot>) {}

    /// Closes V3 consumers when a known-durable publication could not be pinned.
    /// The observer cannot retroactively refuse or delay the durable write.
    fn observe_source_unavailable_v3(&self) {}
}

/// The default port: a publication observer that observes nothing.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct NoChangelogPublicationPort;

impl ChangelogPublicationPort for NoChangelogPublicationPort {
    fn observe_published_advancement(&self, _advancement: PublishedFrontierAdvancement) {}
}

/// Why an emitter stopped emitting and needs to re-anchor.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ChangelogResyncReasonV1 {
    /// The bounded delivery buffer overflowed; at least one advancement was dropped.
    BufferOverflow,
    /// An observed advancement did not continue the emitted frontier exactly.
    FrontierGap,
    /// Deriving a frame from its pinned published snapshot failed.
    DerivationFailed,
    /// A derived frame exceeded a closed structural bound.
    FrameLimitExceeded,
    /// A derived frame failed its own validating decode.
    ValidationFailed,
    /// The pinned snapshot did not match the advancement's covered frontier.
    ///
    /// This is the ADR-0100 §2 gate firing. It means the emitter was handed
    /// something other than the exact published snapshot — the one condition
    /// under which it must stop rather than ship.
    UnpublishedStateVisible,
}

impl fmt::Display for ChangelogResyncReasonV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::BufferOverflow => "changelog delivery buffer overflowed",
            Self::FrontierGap => "observed advancement does not continue the emitted frontier",
            Self::DerivationFailed => "changelog frame derivation failed",
            Self::FrameLimitExceeded => "derived changelog frame exceeds a hard limit",
            Self::ValidationFailed => "derived changelog frame failed validation",
            Self::UnpublishedStateVisible => {
                "changelog snapshot did not match the published covered frontier"
            }
        })
    }
}

/// The emitter's closed lifecycle state.
///
/// # Named deferral: no frame-boundary coalescing for a lagging emitter
///
/// v1 emits exactly one changelog frame per observed advancement. When the
/// bounded buffer overflows, the emitter enters [`Self::Resync`] and **stops**;
/// it never coalesces the dropped advancements into a wider frame, because the
/// boundaries it would have to invent are exactly the boundaries ADR-0100 §1
/// defines as observed facts. Coalescing would need its own amendment to
/// ADR-0100 (a frame would no longer be "one published durable-frontier
/// advancement"), and a follower's crash contract (§3) is stated in terms of
/// those boundaries. A resynced emitter re-anchors through the bootstrap path
/// instead. There is never a silent gap: the chain either continues exactly or
/// the emitter is typed-lagging.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ChangelogEmissionStateV1 {
    /// Emitting a gap-free chain.
    Streaming,
    /// Stopped; the chain cannot be continued exactly and must be re-anchored.
    Resync(ChangelogResyncReasonV1),
}

impl ChangelogEmissionStateV1 {
    /// Returns true while the emitter is still producing a gap-free chain.
    #[must_use]
    pub const fn is_streaming(self) -> bool {
        matches!(self, Self::Streaming)
    }
}

impl fmt::Display for ChangelogEmissionStateV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Streaming => formatter.write_str("streaming"),
            Self::Resync(reason) => write!(formatter, "resync required: {reason}"),
        }
    }
}

/// An in-process consumer of derived changelog frames.
///
/// RE1 ends here: a first-party consumer inside the process receives exact,
/// gap-free, checksummed frames. Wire carriage, capability checks, and follower
/// apply are later packages and are deliberately not expressible through this
/// trait.
///
/// A consumer may take as long as it likes; it can never delay the writer,
/// because the emitter drains its bounded buffer on its own thread and absorbs
/// overflow as [`ChangelogEmissionStateV1::Resync`].
pub trait ChangelogFrameConsumer: Send + Sync {
    /// Accepts one complete, already-validated changelog frame.
    fn accept_frame(&self, frame: &ChangelogFrameV1, encoded: &EncodedChangelogFrameV1);

    /// Observes a transition into a typed lagging state.
    fn note_resync_required(&self, reason: ChangelogResyncReasonV1);
}

fn digest(bytes: &[u8]) -> [u8; HASH_BYTES] {
    Sha256::digest(bytes).into()
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16, ChangelogFrameError> {
    bytes
        .get(offset..offset + 2)
        .and_then(|slice| slice.try_into().ok())
        .map(u16::from_be_bytes)
        .ok_or(ChangelogFrameError::Truncated)
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32, ChangelogFrameError> {
    bytes
        .get(offset..offset + 4)
        .and_then(|slice| slice.try_into().ok())
        .map(u32::from_be_bytes)
        .ok_or(ChangelogFrameError::Truncated)
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, ChangelogFrameError> {
    bytes
        .get(offset..offset + 8)
        .and_then(|slice| slice.try_into().ok())
        .map(u64::from_be_bytes)
        .ok_or(ChangelogFrameError::Truncated)
}

fn read_hash(bytes: &[u8], offset: usize) -> Result<[u8; HASH_BYTES], ChangelogFrameError> {
    bytes
        .get(offset..offset + HASH_BYTES)
        .and_then(|slice| slice.try_into().ok())
        .ok_or(ChangelogFrameError::Truncated)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn database_id() -> DatabaseId {
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10])
            .expect("deterministic database identity")
    }

    fn other_database_id() -> DatabaseId {
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_001, [0x72; 10])
            .expect("deterministic database identity")
    }

    fn frontier(application: u64, administration: u64) -> DualFrontier {
        DualFrontier::new(
            CommitSequence::new(application),
            AdministrationSequence::new(administration),
        )
    }

    fn entry(class: ChangelogEntryClassV1, key: &[u8], value: &[u8]) -> ChangelogEntryV1 {
        ChangelogEntryV1::new(
            class,
            key.to_vec().into_boxed_slice(),
            value.to_vec().into_boxed_slice(),
        )
    }

    fn sample_entries() -> Vec<ChangelogEntryV1> {
        vec![
            entry(
                ChangelogEntryClassV1::Commit,
                &[0, 0, 0, 0, 0, 0, 0, 1],
                b"commit-one",
            ),
            entry(
                ChangelogEntryClassV1::Commit,
                &[0, 0, 0, 0, 0, 0, 0, 2],
                b"commit-two",
            ),
            entry(
                ChangelogEntryClassV1::Event,
                &[0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0],
                b"event",
            ),
            entry(
                ChangelogEntryClassV1::Entity,
                b"entity-key",
                b"entity-post-image",
            ),
            entry(
                ChangelogEntryClassV1::AdministrationAudit,
                &[1, 0, 0, 0, 0, 0, 0, 0, 4],
                b"audit",
            ),
        ]
    }

    fn binding(chain: u8, journal: u8, journaled: bool) -> ChangelogFrameBindingV1 {
        ChangelogFrameBindingV1::new(
            database_id(),
            7,
            [chain; HASH_BYTES],
            [journal; HASH_BYTES],
            journaled,
        )
    }

    fn sample_frame() -> ChangelogFrameV1 {
        ChangelogFrameV1::new(
            binding(0x11, 0x22, true),
            frontier(0, 3),
            frontier(2, 4),
            sample_entries(),
        )
        .expect("well-formed sample frame")
    }

    #[test]
    fn encode_decode_round_trips_every_header_field_and_entry() {
        let frame = sample_frame();
        let encoded = frame.encode().expect("encode sample frame");
        let (decoded, frame_hash) =
            ChangelogFrameV1::decode(encoded.as_bytes()).expect("decode sample frame");
        assert_eq!(decoded, frame);
        assert_eq!(frame_hash, encoded.frame_hash());
        assert_eq!(decoded.header().database_id(), database_id());
        assert_eq!(decoded.header().history_incarnation(), 7);
        assert_eq!(decoded.header().predecessor(), frontier(0, 3));
        assert_eq!(decoded.header().covered(), frontier(2, 4));
        assert_eq!(decoded.header().chain_hash(), [0x11; HASH_BYTES]);
        assert_eq!(decoded.header().journal_frame_hash(), [0x22; HASH_BYTES]);
        assert!(decoded.header().journaled());
        assert_eq!(decoded.header().entry_count(), 5);
        assert_eq!(decoded.header().entry_counts()[0], 2);
    }

    #[test]
    fn encoding_is_deterministic_and_reencodes_byte_identically() {
        let frame = sample_frame();
        let first = frame.encode().expect("first encode");
        let second = frame.encode().expect("second encode");
        assert_eq!(first.as_bytes(), second.as_bytes());
        let (decoded, _) = ChangelogFrameV1::decode(first.as_bytes()).expect("decode");
        assert_eq!(
            decoded
                .encode()
                .expect("re-encode decoded frame")
                .as_bytes(),
            first.as_bytes()
        );
    }

    #[test]
    fn an_unjournaled_single_group_frame_round_trips_with_its_flag_clear() {
        let frame = ChangelogFrameV1::new(
            ChangelogFrameBindingV1::new(
                database_id(),
                1,
                [0; HASH_BYTES],
                [0x33; HASH_BYTES],
                false,
            ),
            DualFrontier::INITIAL,
            frontier(1, 0),
            vec![entry(
                ChangelogEntryClassV1::Commit,
                &[0, 0, 0, 0, 0, 0, 0, 1],
                b"direct-path-commit",
            )],
        )
        .expect("single-group direct-path frame");
        let encoded = frame.encode().expect("encode");
        let (decoded, _) = ChangelogFrameV1::decode(encoded.as_bytes()).expect("decode");
        assert!(!decoded.header().journaled());
        assert_eq!(decoded, frame);
    }

    #[test]
    fn every_truncation_of_a_frame_is_discriminated_as_torn_and_never_accepted() {
        let encoded = sample_frame().encode().expect("encode");
        let bytes = encoded.as_bytes();
        for length in 0..bytes.len() {
            let error = ChangelogFrameV1::decode(&bytes[..length])
                .expect_err("a truncated frame must never decode");
            assert!(
                matches!(
                    error,
                    ChangelogFrameError::Truncated
                        | ChangelogFrameError::UnknownFormat
                        | ChangelogFrameError::UnknownVersion
                ),
                "truncation at {length} produced {error:?}"
            );
        }
        assert!(ChangelogFrameV1::decode(bytes).is_ok());
    }

    #[test]
    fn trailing_bytes_after_a_complete_frame_fail_closed() {
        let encoded = sample_frame().encode().expect("encode");
        let mut bytes = encoded.as_bytes().to_vec();
        bytes.push(0);
        assert_eq!(
            ChangelogFrameV1::decode(&bytes),
            Err(ChangelogFrameError::TrailingBytes)
        );
    }

    #[test]
    fn every_single_byte_mutation_is_rejected() {
        let encoded = sample_frame().encode().expect("encode");
        let bytes = encoded.as_bytes().to_vec();
        for index in 0..bytes.len() {
            let mut corrupted = bytes.clone();
            corrupted[index] ^= 0xff;
            assert!(
                ChangelogFrameV1::decode(&corrupted).is_err(),
                "byte {index} flipped without detection"
            );
        }
    }

    #[test]
    fn a_journal_frame_magic_never_decodes_as_a_changelog_frame() {
        let encoded = sample_frame().encode().expect("encode");
        let mut bytes = encoded.as_bytes().to_vec();
        bytes[..8].copy_from_slice(b"RDBFRM01");
        assert_eq!(
            ChangelogFrameV1::decode(&bytes),
            Err(ChangelogFrameError::UnknownFormat)
        );
    }

    #[test]
    fn an_unknown_version_fails_closed_before_anything_else() {
        let encoded = sample_frame().encode().expect("encode");
        let mut bytes = encoded.as_bytes().to_vec();
        bytes[8..10].copy_from_slice(&2_u16.to_be_bytes());
        assert_eq!(
            ChangelogFrameV1::decode(&bytes),
            Err(ChangelogFrameError::UnknownVersion)
        );
    }

    #[test]
    fn an_unknown_entry_class_fails_closed() {
        let encoded = sample_frame().encode().expect("encode");
        let mut bytes = encoded.as_bytes().to_vec();
        bytes[HEADER_BYTES] = 0x7f;
        let footer_start = bytes.len() - FOOTER_BYTES;
        let checksum = digest(&bytes[..footer_start + 16]);
        bytes[footer_start + 16..].copy_from_slice(&checksum);
        assert_eq!(
            ChangelogFrameV1::decode(&bytes),
            Err(ChangelogFrameError::UnknownEntryClass)
        );
    }

    #[test]
    fn noncanonical_entry_order_is_refused_at_construction_and_at_decode() {
        let mut entries = sample_entries();
        entries.swap(0, 1);
        assert_eq!(
            ChangelogFrameV1::new(
                binding(0x11, 0x22, true),
                frontier(0, 3),
                frontier(2, 4),
                entries,
            ),
            Err(ChangelogFrameError::NonCanonicalOrder)
        );
        let duplicate = vec![
            entry(ChangelogEntryClassV1::Commit, &[1], b"a"),
            entry(ChangelogEntryClassV1::Commit, &[1], b"b"),
        ];
        assert_eq!(
            ChangelogFrameV1::new(
                binding(0x11, 0x22, true),
                frontier(0, 3),
                frontier(2, 4),
                duplicate,
            ),
            Err(ChangelogFrameError::NonCanonicalOrder)
        );
    }

    #[test]
    fn a_frame_that_does_not_advance_its_frontier_is_refused() {
        assert_eq!(
            ChangelogFrameV1::new(
                binding(0, 0, true),
                frontier(2, 4),
                frontier(2, 4),
                Vec::new(),
            ),
            Err(ChangelogFrameError::FrontierDoesNotAdvance)
        );
        assert_eq!(
            ChangelogFrameV1::new(
                binding(0, 0, true),
                frontier(2, 4),
                frontier(1, 4),
                Vec::new(),
            ),
            Err(ChangelogFrameError::FrontierDoesNotAdvance)
        );
    }

    fn chain(count: u64) -> (Vec<EncodedChangelogFrameV1>, ChangelogStreamValidatorV1) {
        let mut frames = Vec::new();
        let mut chain_hash = [0_u8; HASH_BYTES];
        let mut predecessor = DualFrontier::INITIAL;
        for ordinal in 1..=count {
            let covered = frontier(ordinal, 0);
            let frame = ChangelogFrameV1::new(
                ChangelogFrameBindingV1::new(
                    database_id(),
                    7,
                    chain_hash,
                    [u8::try_from(ordinal % 256).expect("bounded"); HASH_BYTES],
                    true,
                ),
                predecessor,
                covered,
                vec![entry(
                    ChangelogEntryClassV1::Commit,
                    &ordinal.to_be_bytes(),
                    b"row",
                )],
            )
            .expect("chained frame");
            let encoded = frame.encode().expect("encode chained frame");
            chain_hash = encoded.frame_hash();
            predecessor = covered;
            frames.push(encoded);
        }
        (
            frames,
            ChangelogStreamValidatorV1::anchored_at(
                database_id(),
                7,
                DualFrontier::INITIAL,
                [0; HASH_BYTES],
            ),
        )
    }

    #[test]
    fn a_contiguous_chain_validates_end_to_end() {
        let (frames, mut validator) = chain(4);
        for (ordinal, encoded) in frames.iter().enumerate() {
            let frame = validator.accept(encoded.as_bytes()).expect("accept frame");
            assert_eq!(
                frame.header().covered(),
                frontier(u64::try_from(ordinal + 1).expect("bounded"), 0)
            );
        }
        assert_eq!(validator.expected_predecessor(), frontier(4, 0));
    }

    #[test]
    fn a_dropped_frame_is_rejected_as_a_gap_and_leaves_the_validator_unchanged() {
        let (frames, mut validator) = chain(3);
        validator.accept(frames[0].as_bytes()).expect("first frame");
        let anchor = validator.clone();
        assert_eq!(
            validator.accept(frames[2].as_bytes()),
            Err(ChangelogFrameError::Gap)
        );
        assert_eq!(validator, anchor);
        validator
            .accept(frames[1].as_bytes())
            .expect("resume exactly where the stream stopped");
        validator.accept(frames[2].as_bytes()).expect("third frame");
    }

    #[test]
    fn a_broken_hash_chain_is_rejected_even_when_the_frontiers_line_up() {
        let mut chain_hash = [0_u8; HASH_BYTES];
        let first = ChangelogFrameV1::new(
            ChangelogFrameBindingV1::new(database_id(), 7, chain_hash, [0x01; HASH_BYTES], true),
            DualFrontier::INITIAL,
            frontier(1, 0),
            Vec::new(),
        )
        .expect("first frame");
        let encoded_first = first.encode().expect("encode");
        chain_hash = encoded_first.frame_hash();
        let mut wrong = chain_hash;
        wrong[0] ^= 0xff;
        let second = ChangelogFrameV1::new(
            ChangelogFrameBindingV1::new(database_id(), 7, wrong, [0x02; HASH_BYTES], true),
            frontier(1, 0),
            frontier(2, 0),
            Vec::new(),
        )
        .expect("second frame");
        let encoded_second = second.encode().expect("encode");
        let mut validator = ChangelogStreamValidatorV1::anchored_at(
            database_id(),
            7,
            DualFrontier::INITIAL,
            [0; HASH_BYTES],
        );
        validator
            .accept(encoded_first.as_bytes())
            .expect("first frame");
        assert_eq!(
            validator.accept(encoded_second.as_bytes()),
            Err(ChangelogFrameError::ChainMismatch)
        );
    }

    #[test]
    fn a_corrupted_bound_journal_frame_hash_is_rejected() {
        let (frames, mut validator) = chain(2);
        let offset = 76 + 4 * CHANGELOG_ENTRY_CLASS_COUNT + 4 + HASH_BYTES;

        // Tampering alone breaks the frame's own checksum.
        let mut tampered = frames[0].as_bytes().to_vec();
        tampered[offset] ^= 0xff;
        assert_eq!(
            validator.accept(&tampered),
            Err(ChangelogFrameError::ChecksumMismatch)
        );

        // Re-checksumming repairs the frame in isolation, but the bound journal
        // frame hash is covered by the chain: the successor no longer continues
        // it. The authoritative-chain binding cannot be edited away.
        let footer_start = tampered.len() - FOOTER_BYTES;
        let checksum = digest(&tampered[..footer_start + 16]);
        tampered[footer_start + 16..].copy_from_slice(&checksum);
        let accepted = validator.accept(&tampered).expect("re-checksummed frame");
        assert_eq!(accepted.header().journal_frame_hash()[0], 0xfe);
        assert_eq!(
            validator.accept(frames[1].as_bytes()),
            Err(ChangelogFrameError::ChainMismatch)
        );

        // The untampered chain validates end to end.
        let mut honest = ChangelogStreamValidatorV1::anchored_at(
            database_id(),
            7,
            DualFrontier::INITIAL,
            [0; HASH_BYTES],
        );
        honest.accept(frames[0].as_bytes()).expect("first frame");
        honest.accept(frames[1].as_bytes()).expect("second frame");
    }

    #[test]
    fn another_database_lineage_and_another_incarnation_both_fail_closed() {
        let (frames, _) = chain(1);
        let mut foreign = ChangelogStreamValidatorV1::anchored_at(
            other_database_id(),
            7,
            DualFrontier::INITIAL,
            [0; HASH_BYTES],
        );
        assert_eq!(
            foreign.accept(frames[0].as_bytes()),
            Err(ChangelogFrameError::DatabaseMismatch)
        );
        let mut restored = ChangelogStreamValidatorV1::anchored_at(
            database_id(),
            8,
            DualFrontier::INITIAL,
            [0; HASH_BYTES],
        );
        assert_eq!(
            restored.accept(frames[0].as_bytes()),
            Err(ChangelogFrameError::IncarnationMismatch)
        );
    }

    #[test]
    fn entry_class_registry_is_exact_and_closed() {
        assert_eq!(
            ChangelogEntryClassV1::ALL.len(),
            CHANGELOG_ENTRY_CLASS_COUNT
        );
        for (index, class) in ChangelogEntryClassV1::ALL.into_iter().enumerate() {
            assert_eq!(class.index(), index);
            assert_eq!(
                class.tag(),
                u8::try_from(index + 1).expect("bounded class tag")
            );
            assert_eq!(ChangelogEntryClassV1::from_tag(class.tag()), Some(class));
        }
        assert_eq!(ChangelogEntryClassV1::from_tag(0), None);
        assert_eq!(
            ChangelogEntryClassV1::from_tag(
                u8::try_from(CHANGELOG_ENTRY_CLASS_COUNT + 1).expect("bounded")
            ),
            None
        );
        let tables = ChangelogEntryClassV1::ALL.map(ChangelogEntryClassV1::table);
        assert_eq!(
            tables,
            [
                CompositeTableV1::Commits,
                CompositeTableV1::Events,
                CompositeTableV1::EventRoutes,
                CompositeTableV1::Outbox,
                CompositeTableV1::Provenance,
                CompositeTableV1::Entities,
                CompositeTableV1::Audit,
                CompositeTableV1::AuditByRequest,
            ]
        );
    }

    #[test]
    fn debug_output_redacts_entry_and_frame_payloads() {
        let entry = entry(
            ChangelogEntryClassV1::Entity,
            b"tenant-a-secret",
            b"payroll",
        );
        let rendered = format!("{entry:?}");
        assert!(!rendered.contains("tenant-a-secret"), "{rendered}");
        assert!(!rendered.contains("payroll"), "{rendered}");
        assert!(rendered.contains("key_bytes"), "{rendered}");
        let encoded = sample_frame().encode().expect("encode");
        let rendered = format!("{encoded:?}");
        assert!(!rendered.contains("commit-one"), "{rendered}");
    }

    #[test]
    fn emission_state_and_resync_reasons_render_safely() {
        assert!(ChangelogEmissionStateV1::Streaming.is_streaming());
        let lagging =
            ChangelogEmissionStateV1::Resync(ChangelogResyncReasonV1::UnpublishedStateVisible);
        assert!(!lagging.is_streaming());
        assert_eq!(
            lagging.to_string(),
            "resync required: changelog snapshot did not match the published covered frontier"
        );
    }
}
