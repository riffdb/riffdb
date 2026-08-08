//! Closed, bounded checkpoint-plus-journal-overlay read-view primitives.
//!
//! These values implement ADR-0104's engine-neutral overlay semantics. They do
//! not select the overlay for production acknowledgement; the redb adapter owns
//! the checkpoint root and publication boundary.

use std::array;
use std::collections::BTreeMap;
use std::fmt;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::Arc;

use riffdb_types::{AdministrationSequence, CommitSequence, DatabaseId, SchemaHash};
use sha2::{Digest, Sha256};

use crate::StorageValueError;

/// Maximum complete published journal suffix represented by one overlay.
pub const MAX_COMPOSITE_OVERLAY_TRANSITIONS: usize = 4_096;
/// Independent in-memory charge for one published overlay.
pub const MAX_COMPOSITE_OVERLAY_BYTES: usize = 64 * 1024 * 1024;
/// Maximum key or value component accepted from one journal mutation.
pub const MAX_COMPOSITE_COMPONENT_BYTES: usize = 16 * 1024 * 1024;
/// Conservative per-entry ordered-map and enum overhead charge.
const OVERLAY_ENTRY_OVERHEAD: usize = 64;
const TABLE_COUNT: usize = 13;

/// Closed authoritative table catalog shared by journal overlays.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum CompositeTableV1 {
    /// Database metadata rows permitted in application or audit frames.
    Meta = 1,
    /// Materialized entity records.
    Entities = 2,
    /// Secondary-index records.
    SecondaryIndexes = 3,
    /// Logical index-generation records.
    IndexEpochs = 4,
    /// Terminal idempotency records or locators.
    Idempotency = 5,
    /// Pending command identities.
    IdempotencyPending = 6,
    /// Durable domain events.
    Events = 7,
    /// Event route records.
    EventRoutes = 8,
    /// Durable outbox intents.
    Outbox = 9,
    /// Command provenance records or locators.
    Provenance = 10,
    /// Commit records or command segments.
    Commits = 11,
    /// Service-audit records or command audit evidence.
    Audit = 12,
    /// Service-audit request lookup rows.
    AuditByRequest = 13,
}

impl CompositeTableV1 {
    /// All tables in durable tag order.
    pub const ALL: [Self; TABLE_COUNT] = [
        Self::Meta,
        Self::Entities,
        Self::SecondaryIndexes,
        Self::IndexEpochs,
        Self::Idempotency,
        Self::IdempotencyPending,
        Self::Events,
        Self::EventRoutes,
        Self::Outbox,
        Self::Provenance,
        Self::Commits,
        Self::Audit,
        Self::AuditByRequest,
    ];

    const fn index(self) -> usize {
        self as usize - 1
    }
}

/// One exact journal mutation used to build an immutable overlay.
#[derive(Clone, Eq, PartialEq)]
pub enum CompositeMutationV1 {
    /// Inserts an absent key or replaces the value matching `expected_hash`.
    Put {
        /// Closed authoritative table.
        table: CompositeTableV1,
        /// Canonical physical table key.
        key: Box<[u8]>,
        /// `None` means the key must be absent.
        expected_hash: Option<[u8; 32]>,
        /// Complete canonical stored value.
        value: Box<[u8]>,
    },
    /// Removes the value matching `expected_hash`.
    Delete {
        /// Closed authoritative table.
        table: CompositeTableV1,
        /// Canonical physical table key.
        key: Box<[u8]>,
        /// SHA-256 of the exact required prior value.
        expected_hash: [u8; 32],
    },
}

impl fmt::Debug for CompositeMutationV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Put { .. } => "CompositeMutationV1::Put([REDACTED])",
            Self::Delete { .. } => "CompositeMutationV1::Delete([REDACTED])",
        })
    }
}

impl CompositeMutationV1 {
    /// Constructs an absence-checked put.
    pub fn put(
        table: CompositeTableV1,
        key: impl Into<Box<[u8]>>,
        value: impl Into<Box<[u8]>>,
    ) -> Result<Self, StorageValueError> {
        Self::put_checked(table, key, None, value)
    }

    /// Constructs a prior-value-checked replacement.
    pub fn replace(
        table: CompositeTableV1,
        key: impl Into<Box<[u8]>>,
        expected: &[u8],
        value: impl Into<Box<[u8]>>,
    ) -> Result<Self, StorageValueError> {
        validate_component(expected)?;
        Self::put_checked(table, key, Some(digest(expected)), value)
    }

    /// Constructs a prior-value-checked tombstone.
    pub fn delete_matching(
        table: CompositeTableV1,
        key: impl Into<Box<[u8]>>,
        expected: &[u8],
    ) -> Result<Self, StorageValueError> {
        let key = key.into();
        validate_component(&key)?;
        validate_component(expected)?;
        Ok(Self::Delete {
            table,
            key,
            expected_hash: digest(expected),
        })
    }

    /// Constructs a decoded journal put while preserving its expected hash.
    pub fn put_checked(
        table: CompositeTableV1,
        key: impl Into<Box<[u8]>>,
        expected_hash: Option<[u8; 32]>,
        value: impl Into<Box<[u8]>>,
    ) -> Result<Self, StorageValueError> {
        let key = key.into();
        let value = value.into();
        validate_component(&key)?;
        validate_component(&value)?;
        Ok(Self::Put {
            table,
            key,
            expected_hash,
            value,
        })
    }

    /// Constructs a decoded journal delete while preserving its expected hash.
    pub fn delete_checked(
        table: CompositeTableV1,
        key: impl Into<Box<[u8]>>,
        expected_hash: [u8; 32],
    ) -> Result<Self, StorageValueError> {
        let key = key.into();
        validate_component(&key)?;
        Ok(Self::Delete {
            table,
            key,
            expected_hash,
        })
    }

    /// Returns the mutation table.
    #[must_use]
    pub const fn table(&self) -> CompositeTableV1 {
        match self {
            Self::Put { table, .. } | Self::Delete { table, .. } => *table,
        }
    }

    /// Returns the canonical physical key.
    #[must_use]
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Put { key, .. } | Self::Delete { key, .. } => key,
        }
    }

    /// Returns the complete replacement value, or `None` for a tombstone.
    #[must_use]
    pub fn value(&self) -> Option<&[u8]> {
        match self {
            Self::Put { value, .. } => Some(value),
            Self::Delete { .. } => None,
        }
    }

    /// Returns the required prior-value hash; `None` requires absence.
    #[must_use]
    pub const fn expected_hash(&self) -> Option<[u8; 32]> {
        match self {
            Self::Put { expected_hash, .. } => *expected_hash,
            Self::Delete { expected_hash, .. } => Some(*expected_hash),
        }
    }
}

/// Immutable identity of the redb checkpoint beneath an overlay.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompositeCheckpointV1 {
    database_id: DatabaseId,
    history_incarnation: u64,
    registry_digest: SchemaHash,
    application_frontier: Option<CommitSequence>,
    administration_frontier: Option<AdministrationSequence>,
    terminal_frame_hash: [u8; 32],
}

impl CompositeCheckpointV1 {
    /// Constructs one validated checkpoint identity.
    pub fn new(
        database_id: DatabaseId,
        history_incarnation: u64,
        registry_digest: SchemaHash,
        application_frontier: Option<CommitSequence>,
        administration_frontier: Option<AdministrationSequence>,
        terminal_frame_hash: [u8; 32],
    ) -> Result<Self, StorageValueError> {
        if history_incarnation == 0 {
            return Err(StorageValueError::InvalidShape);
        }
        Ok(Self {
            database_id,
            history_incarnation,
            registry_digest,
            application_frontier,
            administration_frontier,
            terminal_frame_hash,
        })
    }

    /// Returns the database identity.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Returns the history incarnation.
    #[must_use]
    pub const fn history_incarnation(&self) -> u64 {
        self.history_incarnation
    }

    /// Returns the durable record-registry digest.
    #[must_use]
    pub const fn registry_digest(&self) -> SchemaHash {
        self.registry_digest
    }

    /// Returns the checkpoint application frontier.
    #[must_use]
    pub const fn application_frontier(&self) -> Option<CommitSequence> {
        self.application_frontier
    }

    /// Returns the checkpoint administration frontier.
    #[must_use]
    pub const fn administration_frontier(&self) -> Option<AdministrationSequence> {
        self.administration_frontier
    }

    /// Returns the terminal journal-frame hash covered by the checkpoint.
    #[must_use]
    pub const fn terminal_frame_hash(&self) -> [u8; 32] {
        self.terminal_frame_hash
    }
}

/// One complete validated journal frame presented to an overlay builder.
#[derive(Clone, Eq, PartialEq)]
pub struct CompositeFrameV1 {
    kind: CompositeFrameKindV1,
    database_id: DatabaseId,
    predecessor_application: Option<CommitSequence>,
    covered_application: Option<CommitSequence>,
    predecessor_administration: Option<AdministrationSequence>,
    covered_administration: Option<AdministrationSequence>,
    transition_count: u16,
    encoded_bytes: usize,
    previous_hash: [u8; 32],
    frame_hash: [u8; 32],
    mutations: Vec<CompositeMutationV1>,
}

impl fmt::Debug for CompositeFrameV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompositeFrameV1")
            .field("kind", &self.kind)
            .field("transition_count", &self.transition_count)
            .field("encoded_bytes", &self.encoded_bytes)
            .field("mutations", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

/// Closed journal-frame class used by overlay continuity validation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompositeFrameKindV1 {
    /// One or more complete command transitions, including their audit rows.
    Command,
    /// One or more standalone service-audit transitions.
    ServiceAudit,
}

impl CompositeFrameV1 {
    /// Constructs one bounded frame description. Continuity is checked when it
    /// is applied to a specific checkpoint or predecessor frame.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        kind: CompositeFrameKindV1,
        database_id: DatabaseId,
        predecessor_application: Option<CommitSequence>,
        covered_application: Option<CommitSequence>,
        predecessor_administration: Option<AdministrationSequence>,
        covered_administration: Option<AdministrationSequence>,
        transition_count: u16,
        encoded_bytes: usize,
        previous_hash: [u8; 32],
        frame_hash: [u8; 32],
        mutations: Vec<CompositeMutationV1>,
    ) -> Result<Self, StorageValueError> {
        if transition_count == 0
            || encoded_bytes == 0
            || encoded_bytes > MAX_COMPOSITE_COMPONENT_BYTES
            || mutations.is_empty()
        {
            return Err(StorageValueError::InvalidShape);
        }
        let app_delta = sequence_delta(
            predecessor_application.map(CommitSequence::get),
            covered_application.map(CommitSequence::get),
        )?;
        let admin_delta = sequence_delta(
            predecessor_administration.map(AdministrationSequence::get),
            covered_administration.map(AdministrationSequence::get),
        )?;
        let transitions = u64::from(transition_count);
        let counts_match = match kind {
            CompositeFrameKindV1::Command => {
                app_delta == transitions && (admin_delta == 0 || admin_delta >= transitions)
            }
            CompositeFrameKindV1::ServiceAudit => app_delta == 0 && admin_delta == transitions,
        };
        if !counts_match {
            return Err(StorageValueError::InvalidShape);
        }
        Ok(Self {
            kind,
            database_id,
            predecessor_application,
            covered_application,
            predecessor_administration,
            covered_administration,
            transition_count,
            encoded_bytes,
            previous_hash,
            frame_hash,
            mutations,
        })
    }

    /// Returns the closed frame class.
    #[must_use]
    pub const fn kind(&self) -> CompositeFrameKindV1 {
        self.kind
    }
}

/// Checkpoint lookup and canonical-byte validation used while rebuilding an
/// overlay. A backend must answer from one frozen checkpoint root.
pub trait CompositeViewBase {
    /// Reads one exact checkpoint value.
    fn read_base(
        &self,
        table: CompositeTableV1,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StorageValueError>;

    /// Validates the canonical key and optional complete value for this table.
    fn validate_entry(
        &self,
        table: CompositeTableV1,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> Result<(), StorageValueError>;
}

/// Mutable, unpublished construction state for one exact overlay suffix.
pub struct CompositeOverlayBuilder {
    checkpoint: CompositeCheckpointV1,
    current_application: Option<CommitSequence>,
    current_administration: Option<AdministrationSequence>,
    current_hash: [u8; 32],
    transition_count: usize,
    encoded_frame_bytes: usize,
    charged_bytes: usize,
    tables: [PersistentOverlayMap; TABLE_COUNT],
}

impl CompositeOverlayBuilder {
    /// Starts an empty suffix over one frozen checkpoint identity.
    #[must_use]
    pub fn new(checkpoint: CompositeCheckpointV1) -> Self {
        Self {
            current_application: checkpoint.application_frontier,
            current_administration: checkpoint.administration_frontier,
            current_hash: checkpoint.terminal_frame_hash,
            checkpoint,
            transition_count: 0,
            encoded_frame_bytes: 0,
            charged_bytes: 0,
            tables: array::from_fn(|_| PersistentOverlayMap::default()),
        }
    }

    /// Forks an immutable published suffix into writer-private construction
    /// state in O(1). Only paths touched by successor mutations are copied.
    /// Captured readers therefore retain their exact prior roots while the sole
    /// writer can continue from the published frontier without cloning the
    /// complete bounded overlay.
    #[must_use]
    pub fn from_published(published: &FrozenCompositeOverlay) -> Self {
        Self {
            checkpoint: published.checkpoint.clone(),
            current_application: published.published_application,
            current_administration: published.published_administration,
            current_hash: published.terminal_frame_hash,
            transition_count: published.transition_count,
            encoded_frame_bytes: published.encoded_frame_bytes,
            charged_bytes: published.charged_bytes,
            tables: published.tables.clone(),
        }
    }

    /// Applies one complete successor frame or fails without changing the
    /// builder. Prior-value checks resolve through the newest overlay value and
    /// then the captured checkpoint.
    pub fn apply_frame(
        &mut self,
        frame: &CompositeFrameV1,
        base: &impl CompositeViewBase,
    ) -> Result<(), StorageValueError> {
        self.validate_frame_identity(frame)?;
        apply_mutations_atomically(
            &mut self.tables,
            &mut self.charged_bytes,
            &frame.mutations,
            base,
        )?;
        self.transition_count = self
            .transition_count
            .checked_add(usize::from(frame.transition_count))
            .ok_or(StorageValueError::SizeOverflow)?;
        self.encoded_frame_bytes = self
            .encoded_frame_bytes
            .checked_add(frame.encoded_bytes)
            .ok_or(StorageValueError::SizeOverflow)?;
        self.current_application = frame.covered_application;
        self.current_administration = frame.covered_administration;
        self.current_hash = frame.frame_hash;
        Ok(())
    }

    fn validate_frame_identity(&self, frame: &CompositeFrameV1) -> Result<(), StorageValueError> {
        if frame.database_id != self.checkpoint.database_id
            || frame.predecessor_application != self.current_application
            || frame.predecessor_administration != self.current_administration
            || frame.previous_hash != self.current_hash
            || self
                .transition_count
                .checked_add(usize::from(frame.transition_count))
                .is_none_or(|count| count > MAX_COMPOSITE_OVERLAY_TRANSITIONS)
            || self
                .encoded_frame_bytes
                .checked_add(frame.encoded_bytes)
                .is_none_or(|bytes| bytes > MAX_COMPOSITE_COMPONENT_BYTES)
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(())
    }

    /// Freezes the exact checkpoint-plus-suffix view.
    #[must_use]
    pub fn freeze(self) -> FrozenCompositeOverlay {
        FrozenCompositeOverlay {
            checkpoint: self.checkpoint,
            published_application: self.current_application,
            published_administration: self.current_administration,
            terminal_frame_hash: self.current_hash,
            transition_count: self.transition_count,
            encoded_frame_bytes: self.encoded_frame_bytes,
            charged_bytes: self.charged_bytes,
            tables: self.tables,
        }
    }
}

/// Unpublished read-your-writes stage for one command subgroup.
///
/// This value intentionally has no publication or frontier-advancing method.
/// Its ordered mutations must first become one validated journal frame; only
/// applying that frame to [`CompositeOverlayBuilder`] creates a publishable
/// successor. Dropping the stage publishes nothing.
pub struct CompositeMutationStage {
    tables: [PersistentOverlayMap; TABLE_COUNT],
    charged_bytes: usize,
    mutations: Vec<CompositeMutationV1>,
}

impl CompositeMutationStage {
    /// Forks one captured view for private subgroup staging in O(1).
    #[must_use]
    pub fn new(predecessor: &FrozenCompositeOverlay) -> Self {
        Self {
            tables: predecessor.tables.clone(),
            charged_bytes: predecessor.charged_bytes,
            mutations: Vec::new(),
        }
    }

    /// Applies one exact mutation to private read-your-writes state. A failed
    /// validation leaves the complete stage unchanged.
    pub fn apply(
        &mut self,
        mutation: CompositeMutationV1,
        base: &impl CompositeViewBase,
    ) -> Result<(), StorageValueError> {
        apply_mutations_atomically(
            &mut self.tables,
            &mut self.charged_bytes,
            std::slice::from_ref(&mutation),
            base,
        )?;
        self.mutations.push(mutation);
        Ok(())
    }

    /// Resolves a private point through staged state and then the checkpoint.
    pub fn resolve_point(
        &self,
        base: &impl CompositeViewBase,
        table: CompositeTableV1,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StorageValueError> {
        resolve_overlay_point(&self.tables, base, table, key)
    }

    /// Canonically merges one bounded checkpoint range with private staged
    /// values for transaction-current validation.
    pub fn merge_bounded<I>(
        &self,
        table: CompositeTableV1,
        base: I,
        start_inclusive: &[u8],
        end_exclusive: Option<&[u8]>,
        max_rows: usize,
        max_inspected: usize,
    ) -> Result<BoundedCompositePage, StorageValueError>
    where
        I: IntoIterator<Item = Result<(Box<[u8]>, Box<[u8]>), StorageValueError>>,
    {
        merge_overlay_bounded(
            &self.tables,
            table,
            base,
            start_inclusive,
            end_exclusive,
            max_rows,
            max_inspected,
        )
    }

    /// Returns the number of exact ordered mutations retained for framing.
    #[must_use]
    pub fn mutation_count(&self) -> usize {
        self.mutations.len()
    }

    /// Consumes the stage into the exact ordered mutations that must be encoded
    /// in its journal frame.
    #[must_use]
    pub fn into_mutations(self) -> Vec<CompositeMutationV1> {
        self.mutations
    }
}

/// One immutable validated overlay bound to one checkpoint identity.
#[derive(Clone)]
pub struct FrozenCompositeOverlay {
    checkpoint: CompositeCheckpointV1,
    published_application: Option<CommitSequence>,
    published_administration: Option<AdministrationSequence>,
    terminal_frame_hash: [u8; 32],
    transition_count: usize,
    encoded_frame_bytes: usize,
    charged_bytes: usize,
    tables: [PersistentOverlayMap; TABLE_COUNT],
}

impl FrozenCompositeOverlay {
    /// Returns the exact checkpoint identity.
    #[must_use]
    pub const fn checkpoint(&self) -> &CompositeCheckpointV1 {
        &self.checkpoint
    }

    /// Returns the published application frontier.
    #[must_use]
    pub const fn published_application(&self) -> Option<CommitSequence> {
        self.published_application
    }

    /// Returns the published administration frontier.
    #[must_use]
    pub const fn published_administration(&self) -> Option<AdministrationSequence> {
        self.published_administration
    }

    /// Returns the terminal frame hash at the published frontier.
    #[must_use]
    pub const fn terminal_frame_hash(&self) -> [u8; 32] {
        self.terminal_frame_hash
    }

    /// Returns the exact suffix transition count.
    #[must_use]
    pub const fn transition_count(&self) -> usize {
        self.transition_count
    }

    /// Returns the encoded journal-frame bytes represented by the overlay.
    #[must_use]
    pub const fn encoded_frame_bytes(&self) -> usize {
        self.encoded_frame_bytes
    }

    /// Returns the conservative overlay-memory charge.
    #[must_use]
    pub const fn charged_bytes(&self) -> usize {
        self.charged_bytes
    }

    /// Resolves one point from the overlay only. `Unchanged` falls back to the
    /// captured checkpoint; `Tombstone` is authoritative absence.
    #[must_use]
    pub fn lookup(&self, table: CompositeTableV1, key: &[u8]) -> OverlayLookup<'_> {
        match self.tables[table.index()].get(key) {
            None => OverlayLookup::Unchanged,
            Some(OverlayValue::Tombstone) => OverlayLookup::Tombstone,
            Some(OverlayValue::Value(value)) => OverlayLookup::Value(value),
        }
    }

    /// Resolves one complete point using overlay-first/tombstone semantics.
    pub fn resolve_point(
        &self,
        base: &impl CompositeViewBase,
        table: CompositeTableV1,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, StorageValueError> {
        resolve_overlay_point(&self.tables, base, table, key)
    }

    /// Canonically merges a sorted checkpoint range with this overlay without
    /// materializing an unbounded union.
    pub fn merge_bounded<I>(
        &self,
        table: CompositeTableV1,
        base: I,
        start_inclusive: &[u8],
        end_exclusive: Option<&[u8]>,
        max_rows: usize,
        max_inspected: usize,
    ) -> Result<BoundedCompositePage, StorageValueError>
    where
        I: IntoIterator<Item = Result<(Box<[u8]>, Box<[u8]>), StorageValueError>>,
    {
        merge_overlay_bounded(
            &self.tables,
            table,
            base,
            start_inclusive,
            end_exclusive,
            max_rows,
            max_inspected,
        )
    }
}

#[allow(clippy::too_many_arguments)]
fn merge_overlay_bounded<I>(
    tables: &[PersistentOverlayMap; TABLE_COUNT],
    table: CompositeTableV1,
    base: I,
    start_inclusive: &[u8],
    end_exclusive: Option<&[u8]>,
    max_rows: usize,
    max_inspected: usize,
) -> Result<BoundedCompositePage, StorageValueError>
where
    I: IntoIterator<Item = Result<(Box<[u8]>, Box<[u8]>), StorageValueError>>,
{
    if max_rows == 0 || max_inspected == 0 || max_rows > max_inspected {
        return Err(StorageValueError::InvalidShape);
    }
    let mut base = base.into_iter().peekable();
    let end_bound = end_exclusive.map(Excluded).unwrap_or(Unbounded);
    let mut overlay = tables[table.index()]
        .range(Included(start_inclusive), end_bound)
        .peekable();
    let mut rows = Vec::with_capacity(max_rows.min(256));
    let mut inspected = 0usize;
    let mut last_base_key: Option<Box<[u8]>> = None;

    while rows.len() < max_rows {
        let base_key = match base.peek() {
            Some(Ok((key, _))) => {
                validate_range_key(key, start_inclusive, end_exclusive)?;
                if last_base_key
                    .as_deref()
                    .is_some_and(|last| last >= key.as_ref())
                {
                    return Err(StorageValueError::NonCanonicalOrder);
                }
                Some(key.as_ref())
            }
            Some(Err(_)) => {
                return match base.next().ok_or(StorageValueError::InvalidShape)? {
                    Err(error) => Err(error),
                    Ok(_) => Err(StorageValueError::InvalidShape),
                };
            }
            None => None,
        };
        let overlay_key = overlay.peek().map(|(key, _)| *key);
        if base_key.is_none() && overlay_key.is_none() {
            break;
        }
        inspected = inspected
            .checked_add(1)
            .ok_or(StorageValueError::SizeOverflow)?;
        if inspected > max_inspected {
            return Err(StorageValueError::LimitExceeded);
        }

        match (base_key, overlay_key) {
            (Some(base_key), Some(overlay_key)) if base_key < overlay_key => {
                let (key, value) = base.next().ok_or(StorageValueError::InvalidShape)??;
                last_base_key = Some(key.clone());
                rows.push((key, value));
            }
            (Some(base_key), Some(overlay_key)) if base_key == overlay_key => {
                let (key, _) = base.next().ok_or(StorageValueError::InvalidShape)??;
                last_base_key = Some(key.clone());
                let (_, value) = overlay.next().ok_or(StorageValueError::InvalidShape)?;
                if let OverlayValue::Value(value) = value {
                    rows.push((key, value.as_ref().into()));
                }
            }
            (Some(_), Some(_)) | (None, Some(_)) => {
                let (key, value) = overlay.next().ok_or(StorageValueError::InvalidShape)?;
                if let OverlayValue::Value(value) = value {
                    rows.push((key.into(), value.as_ref().into()));
                }
            }
            (Some(_), None) => {
                let (key, value) = base.next().ok_or(StorageValueError::InvalidShape)??;
                last_base_key = Some(key.clone());
                rows.push((key, value));
            }
            (None, None) => break,
        }
    }
    Ok(BoundedCompositePage { rows, inspected })
}

/// Overlay-only point result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OverlayLookup<'a> {
    /// The checkpoint value remains authoritative.
    Unchanged,
    /// The key is absent even if the checkpoint contains it.
    Tombstone,
    /// The newest complete overlay value.
    Value(&'a [u8]),
}

/// One owned canonical physical key and complete stored value.
pub type CompositeRow = (Box<[u8]>, Box<[u8]>);

/// Bounded canonical merged rows and inspected-work evidence.
#[derive(Clone, Eq, PartialEq)]
pub struct BoundedCompositePage {
    rows: Vec<CompositeRow>,
    inspected: usize,
}

impl fmt::Debug for BoundedCompositePage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BoundedCompositePage")
            .field("row_count", &self.rows.len())
            .field("inspected", &self.inspected)
            .field("rows", &"[REDACTED]")
            .finish()
    }
}

impl BoundedCompositePage {
    /// Returns the canonical merged rows.
    #[must_use]
    pub fn rows(&self) -> &[CompositeRow] {
        &self.rows
    }

    /// Returns the number of merge candidates inspected.
    #[must_use]
    pub const fn inspected(&self) -> usize {
        self.inspected
    }
}

#[derive(Clone)]
enum OverlayValue {
    Tombstone,
    Value(Arc<[u8]>),
}

fn apply_mutations_atomically(
    tables: &mut [PersistentOverlayMap; TABLE_COUNT],
    charged_bytes: &mut usize,
    mutations: &[CompositeMutationV1],
    base: &impl CompositeViewBase,
) -> Result<(), StorageValueError> {
    let mut pending_tables: [BTreeMap<Box<[u8]>, OverlayValue>; TABLE_COUNT] =
        array::from_fn(|_| BTreeMap::new());
    let mut candidate_charge = *charged_bytes;
    for mutation in mutations {
        base.validate_entry(mutation.table(), mutation.key(), mutation.value())?;
        let table_index = mutation.table().index();
        let pending = pending_tables[table_index].get(mutation.key());
        let current = match pending.or_else(|| tables[table_index].get(mutation.key())) {
            Some(OverlayValue::Value(value)) => Some(value.as_ref().to_vec()),
            Some(OverlayValue::Tombstone) => None,
            None => base.read_base(mutation.table(), mutation.key())?,
        };
        validate_before_image(current.as_deref(), mutation.expected_hash())?;

        let key: Box<[u8]> = mutation.key().into();
        let replacement = mutation
            .value()
            .map(|value| OverlayValue::Value(Arc::from(value)))
            .unwrap_or(OverlayValue::Tombstone);
        let prior_overlay = pending_tables[table_index]
            .get(key.as_ref())
            .or_else(|| tables[table_index].get(key.as_ref()));
        if let Some(prior) = prior_overlay {
            candidate_charge = candidate_charge
                .checked_sub(entry_charge(&key, prior)?)
                .ok_or(StorageValueError::SizeOverflow)?;
        }
        candidate_charge = candidate_charge
            .checked_add(entry_charge(&key, &replacement)?)
            .ok_or(StorageValueError::SizeOverflow)?;
        if candidate_charge > MAX_COMPOSITE_OVERLAY_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        pending_tables[table_index].insert(key, replacement);
    }

    for (target, pending) in tables.iter_mut().zip(pending_tables) {
        for (key, value) in pending {
            target.insert(key, value);
        }
    }
    *charged_bytes = candidate_charge;
    Ok(())
}

fn resolve_overlay_point(
    tables: &[PersistentOverlayMap; TABLE_COUNT],
    base: &impl CompositeViewBase,
    table: CompositeTableV1,
    key: &[u8],
) -> Result<Option<Vec<u8>>, StorageValueError> {
    match tables[table.index()].get(key) {
        None => base.read_base(table, key),
        Some(OverlayValue::Tombstone) => Ok(None),
        Some(OverlayValue::Value(value)) => Ok(Some(value.to_vec())),
    }
}

/// Persistent AVL map used by immutable published views.
///
/// A successor reuses every untouched node and copies only one logarithmic
/// search path. This is intentionally private: callers can observe only the
/// closed overlay lookup and bounded merge contracts above.
#[derive(Clone, Default)]
struct PersistentOverlayMap {
    root: Option<Arc<OverlayNode>>,
}

struct OverlayNode {
    key: Box<[u8]>,
    value: OverlayValue,
    height: u16,
    left: Option<Arc<Self>>,
    right: Option<Arc<Self>>,
}

impl PersistentOverlayMap {
    fn get(&self, key: &[u8]) -> Option<&OverlayValue> {
        let mut current = self.root.as_deref();
        while let Some(node) = current {
            match key.cmp(node.key.as_ref()) {
                std::cmp::Ordering::Less => current = node.left.as_deref(),
                std::cmp::Ordering::Equal => return Some(&node.value),
                std::cmp::Ordering::Greater => current = node.right.as_deref(),
            }
        }
        None
    }

    fn insert(&mut self, key: Box<[u8]>, value: OverlayValue) {
        self.root = Some(insert_overlay_node(self.root.as_ref(), key, value));
    }

    fn range<'map>(
        &'map self,
        start: std::ops::Bound<&'map [u8]>,
        end: std::ops::Bound<&'map [u8]>,
    ) -> PersistentOverlayRange<'map> {
        PersistentOverlayRange::new(self.root.as_deref(), start, end)
    }
}

fn insert_overlay_node(
    node: Option<&Arc<OverlayNode>>,
    key: Box<[u8]>,
    value: OverlayValue,
) -> Arc<OverlayNode> {
    let Some(node) = node else {
        return overlay_node(key, value, None, None);
    };
    match key.as_ref().cmp(node.key.as_ref()) {
        std::cmp::Ordering::Less => rebalance_overlay_node(overlay_node(
            node.key.clone(),
            node.value.clone(),
            Some(insert_overlay_node(node.left.as_ref(), key, value)),
            node.right.clone(),
        )),
        std::cmp::Ordering::Equal => {
            overlay_node(key, value, node.left.clone(), node.right.clone())
        }
        std::cmp::Ordering::Greater => rebalance_overlay_node(overlay_node(
            node.key.clone(),
            node.value.clone(),
            node.left.clone(),
            Some(insert_overlay_node(node.right.as_ref(), key, value)),
        )),
    }
}

fn overlay_node(
    key: Box<[u8]>,
    value: OverlayValue,
    left: Option<Arc<OverlayNode>>,
    right: Option<Arc<OverlayNode>>,
) -> Arc<OverlayNode> {
    let height = overlay_height(&left)
        .max(overlay_height(&right))
        .saturating_add(1);
    Arc::new(OverlayNode {
        key,
        value,
        height,
        left,
        right,
    })
}

fn overlay_height(node: &Option<Arc<OverlayNode>>) -> u16 {
    node.as_ref().map_or(0, |node| node.height)
}

fn rebalance_overlay_node(node: Arc<OverlayNode>) -> Arc<OverlayNode> {
    let balance = i32::from(overlay_height(&node.left)) - i32::from(overlay_height(&node.right));
    if balance > 1 {
        let left = node
            .left
            .as_ref()
            .expect("positive AVL balance has left child");
        if overlay_height(&left.left) < overlay_height(&left.right) {
            let rotated_left = rotate_overlay_left(Arc::clone(left));
            return rotate_overlay_right(overlay_node(
                node.key.clone(),
                node.value.clone(),
                Some(rotated_left),
                node.right.clone(),
            ));
        }
        return rotate_overlay_right(node);
    }
    if balance < -1 {
        let right = node
            .right
            .as_ref()
            .expect("negative AVL balance has right child");
        if overlay_height(&right.right) < overlay_height(&right.left) {
            let rotated_right = rotate_overlay_right(Arc::clone(right));
            return rotate_overlay_left(overlay_node(
                node.key.clone(),
                node.value.clone(),
                node.left.clone(),
                Some(rotated_right),
            ));
        }
        return rotate_overlay_left(node);
    }
    node
}

fn rotate_overlay_left(node: Arc<OverlayNode>) -> Arc<OverlayNode> {
    let right = node
        .right
        .as_ref()
        .expect("AVL left rotation has right child");
    let new_left = overlay_node(
        node.key.clone(),
        node.value.clone(),
        node.left.clone(),
        right.left.clone(),
    );
    overlay_node(
        right.key.clone(),
        right.value.clone(),
        Some(new_left),
        right.right.clone(),
    )
}

fn rotate_overlay_right(node: Arc<OverlayNode>) -> Arc<OverlayNode> {
    let left = node
        .left
        .as_ref()
        .expect("AVL right rotation has left child");
    let new_right = overlay_node(
        node.key.clone(),
        node.value.clone(),
        left.right.clone(),
        node.right.clone(),
    );
    overlay_node(
        left.key.clone(),
        left.value.clone(),
        left.left.clone(),
        Some(new_right),
    )
}

struct PersistentOverlayRange<'map> {
    stack: Vec<&'map OverlayNode>,
    end: std::ops::Bound<&'map [u8]>,
}

impl<'map> PersistentOverlayRange<'map> {
    fn new(
        root: Option<&'map OverlayNode>,
        start: std::ops::Bound<&'map [u8]>,
        end: std::ops::Bound<&'map [u8]>,
    ) -> Self {
        let mut range = Self {
            stack: Vec::new(),
            end,
        };
        let mut current = root;
        while let Some(node) = current {
            let before_start = match start {
                Included(start) => node.key.as_ref() < start,
                Excluded(start) => node.key.as_ref() <= start,
                Unbounded => false,
            };
            if before_start {
                current = node.right.as_deref();
            } else {
                range.stack.push(node);
                current = node.left.as_deref();
            }
        }
        range
    }

    fn push_left(&mut self, mut current: Option<&'map OverlayNode>) {
        while let Some(node) = current {
            self.stack.push(node);
            current = node.left.as_deref();
        }
    }
}

impl<'map> Iterator for PersistentOverlayRange<'map> {
    type Item = (&'map [u8], &'map OverlayValue);

    fn next(&mut self) -> Option<Self::Item> {
        let node = self.stack.pop()?;
        let beyond_end = match self.end {
            Included(end) => node.key.as_ref() > end,
            Excluded(end) => node.key.as_ref() >= end,
            Unbounded => false,
        };
        if beyond_end {
            self.stack.clear();
            return None;
        }
        self.push_left(node.right.as_deref());
        Some((node.key.as_ref(), &node.value))
    }
}

fn validate_before_image(
    current: Option<&[u8]>,
    expected_hash: Option<[u8; 32]>,
) -> Result<(), StorageValueError> {
    match (current, expected_hash) {
        (None, None) => Ok(()),
        (Some(value), Some(expected)) if digest(value) == expected => Ok(()),
        _ => Err(StorageValueError::IdentityMismatch),
    }
}

fn validate_component(bytes: &[u8]) -> Result<(), StorageValueError> {
    if bytes.is_empty() {
        return Err(StorageValueError::Empty);
    }
    if bytes.len() > MAX_COMPOSITE_COMPONENT_BYTES {
        return Err(StorageValueError::LimitExceeded);
    }
    Ok(())
}

fn validate_range_key(
    key: &[u8],
    start_inclusive: &[u8],
    end_exclusive: Option<&[u8]>,
) -> Result<(), StorageValueError> {
    if key < start_inclusive || end_exclusive.is_some_and(|end| key >= end) {
        return Err(StorageValueError::InvalidShape);
    }
    validate_component(key)
}

fn entry_charge(key: &[u8], value: &OverlayValue) -> Result<usize, StorageValueError> {
    key.len()
        .checked_add(match value {
            OverlayValue::Tombstone => 1,
            OverlayValue::Value(value) => value.len(),
        })
        .and_then(|bytes| bytes.checked_add(OVERLAY_ENTRY_OVERHEAD))
        .ok_or(StorageValueError::SizeOverflow)
}

fn sequence_delta(
    predecessor: Option<u64>,
    covered: Option<u64>,
) -> Result<u64, StorageValueError> {
    match (predecessor, covered) {
        (None, None) => Ok(0),
        (None, Some(covered)) => Ok(covered),
        (Some(predecessor), Some(covered)) => covered
            .checked_sub(predecessor)
            .ok_or(StorageValueError::InvalidShape),
        (Some(_), None) => Err(StorageValueError::InvalidShape),
    }
}

fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct Base(BTreeMap<(CompositeTableV1, Vec<u8>), Vec<u8>>);

    impl Base {
        fn insert(&mut self, table: CompositeTableV1, key: &[u8], value: &[u8]) {
            self.0.insert((table, key.to_vec()), value.to_vec());
        }
    }

    impl CompositeViewBase for Base {
        fn read_base(
            &self,
            table: CompositeTableV1,
            key: &[u8],
        ) -> Result<Option<Vec<u8>>, StorageValueError> {
            Ok(self.0.get(&(table, key.to_vec())).cloned())
        }

        fn validate_entry(
            &self,
            _table: CompositeTableV1,
            key: &[u8],
            value: Option<&[u8]>,
        ) -> Result<(), StorageValueError> {
            validate_component(key)?;
            if let Some(value) = value {
                validate_component(value)?;
            }
            Ok(())
        }
    }

    fn database_id() -> DatabaseId {
        DatabaseId::from_unix_milliseconds_and_random(1, [7; 10]).expect("database")
    }

    fn checkpoint() -> CompositeCheckpointV1 {
        CompositeCheckpointV1::new(
            database_id(),
            1,
            SchemaHash::from_bytes([3; 32]),
            Some(CommitSequence::first()),
            Some(AdministrationSequence::first()),
            [5; 32],
        )
        .expect("checkpoint")
    }

    fn frame(mutations: Vec<CompositeMutationV1>) -> CompositeFrameV1 {
        CompositeFrameV1::new(
            CompositeFrameKindV1::Command,
            database_id(),
            Some(CommitSequence::first()),
            CommitSequence::new(2),
            Some(AdministrationSequence::first()),
            AdministrationSequence::new(2),
            1,
            512,
            [5; 32],
            [6; 32],
            mutations,
        )
        .expect("frame")
    }

    #[test]
    fn point_reads_distinguish_value_tombstone_and_checkpoint_fallback() {
        let mut base = Base::default();
        base.insert(CompositeTableV1::Entities, b"kept", b"old-kept");
        base.insert(CompositeTableV1::Entities, b"deleted", b"old-deleted");
        let mutations = vec![
            CompositeMutationV1::replace(
                CompositeTableV1::Entities,
                b"kept".as_slice(),
                b"old-kept",
                b"new-kept".as_slice(),
            )
            .expect("replace"),
            CompositeMutationV1::delete_matching(
                CompositeTableV1::Entities,
                b"deleted".as_slice(),
                b"old-deleted",
            )
            .expect("delete"),
            CompositeMutationV1::put(
                CompositeTableV1::Entities,
                b"created".as_slice(),
                b"new-created".as_slice(),
            )
            .expect("put"),
        ];
        let mut builder = CompositeOverlayBuilder::new(checkpoint());
        builder
            .apply_frame(&frame(mutations), &base)
            .expect("apply");
        let view = builder.freeze();

        assert_eq!(
            view.resolve_point(&base, CompositeTableV1::Entities, b"kept"),
            Ok(Some(b"new-kept".to_vec()))
        );
        assert_eq!(
            view.resolve_point(&base, CompositeTableV1::Entities, b"deleted"),
            Ok(None)
        );
        assert_eq!(
            view.resolve_point(&base, CompositeTableV1::Entities, b"created"),
            Ok(Some(b"new-created".to_vec()))
        );
        assert_eq!(
            view.resolve_point(&base, CompositeTableV1::Entities, b"missing"),
            Ok(None)
        );
    }

    #[test]
    fn frame_application_is_atomic_on_prior_hash_failure() {
        let mut base = Base::default();
        base.insert(CompositeTableV1::Entities, b"a", b"one");
        let mutations = vec![
            CompositeMutationV1::replace(
                CompositeTableV1::Entities,
                b"a".as_slice(),
                b"wrong",
                b"two".as_slice(),
            )
            .expect("replace"),
        ];
        let mut builder = CompositeOverlayBuilder::new(checkpoint());
        assert_eq!(
            builder.apply_frame(&frame(mutations), &base),
            Err(StorageValueError::IdentityMismatch)
        );
        let view = builder.freeze();
        assert_eq!(view.transition_count(), 0);
        assert_eq!(
            view.lookup(CompositeTableV1::Entities, b"a"),
            OverlayLookup::Unchanged
        );
    }

    #[test]
    fn frames_must_be_gap_free_and_hash_chained() {
        let base = Base::default();
        let mut builder = CompositeOverlayBuilder::new(checkpoint());
        let wrong = CompositeFrameV1::new(
            CompositeFrameKindV1::Command,
            database_id(),
            CommitSequence::new(2),
            CommitSequence::new(3),
            AdministrationSequence::new(2),
            AdministrationSequence::new(3),
            1,
            128,
            [9; 32],
            [10; 32],
            vec![
                CompositeMutationV1::put(
                    CompositeTableV1::Entities,
                    b"a".as_slice(),
                    b"one".as_slice(),
                )
                .expect("put"),
            ],
        )
        .expect("frame shape");
        assert_eq!(
            builder.apply_frame(&wrong, &base),
            Err(StorageValueError::IdentityMismatch)
        );
    }

    #[test]
    fn bounded_merge_replaces_deletes_and_inserts_canonically() {
        let mut base = Base::default();
        base.insert(CompositeTableV1::SecondaryIndexes, b"b", b"base-b");
        base.insert(CompositeTableV1::SecondaryIndexes, b"d", b"base-d");
        base.insert(CompositeTableV1::SecondaryIndexes, b"f", b"base-f");
        let mutations = vec![
            CompositeMutationV1::put(
                CompositeTableV1::SecondaryIndexes,
                b"a".as_slice(),
                b"new-a".as_slice(),
            )
            .expect("put"),
            CompositeMutationV1::replace(
                CompositeTableV1::SecondaryIndexes,
                b"d".as_slice(),
                b"base-d",
                b"new-d".as_slice(),
            )
            .expect("replace"),
            CompositeMutationV1::delete_matching(
                CompositeTableV1::SecondaryIndexes,
                b"f".as_slice(),
                b"base-f",
            )
            .expect("delete"),
        ];
        let mut builder = CompositeOverlayBuilder::new(checkpoint());
        builder
            .apply_frame(&frame(mutations), &base)
            .expect("apply");
        let view = builder.freeze();
        let base_rows = [
            Ok((
                b"b".to_vec().into_boxed_slice(),
                b"base-b".to_vec().into_boxed_slice(),
            )),
            Ok((
                b"d".to_vec().into_boxed_slice(),
                b"base-d".to_vec().into_boxed_slice(),
            )),
            Ok((
                b"f".to_vec().into_boxed_slice(),
                b"base-f".to_vec().into_boxed_slice(),
            )),
        ];
        let page = view
            .merge_bounded(
                CompositeTableV1::SecondaryIndexes,
                base_rows,
                b"a",
                Some(b"z"),
                10,
                10,
            )
            .expect("merge");
        let rows: Vec<(&[u8], &[u8])> = page
            .rows()
            .iter()
            .map(|(key, value)| (key.as_ref(), value.as_ref()))
            .collect();
        assert_eq!(
            rows,
            vec![
                (b"a".as_slice(), b"new-a".as_slice()),
                (b"b".as_slice(), b"base-b".as_slice()),
                (b"d".as_slice(), b"new-d".as_slice()),
            ]
        );
    }

    #[test]
    fn bounded_merge_rejects_noncanonical_base_and_work_exhaustion() {
        let view = CompositeOverlayBuilder::new(checkpoint()).freeze();
        let unsorted = [
            Ok((
                b"b".to_vec().into_boxed_slice(),
                b"one".to_vec().into_boxed_slice(),
            )),
            Ok((
                b"a".to_vec().into_boxed_slice(),
                b"two".to_vec().into_boxed_slice(),
            )),
        ];
        assert_eq!(
            view.merge_bounded(CompositeTableV1::Entities, unsorted, b"a", Some(b"z"), 2, 2,),
            Err(StorageValueError::NonCanonicalOrder)
        );

        let base = [
            Ok((
                b"a".to_vec().into_boxed_slice(),
                b"one".to_vec().into_boxed_slice(),
            )),
            Ok((
                b"b".to_vec().into_boxed_slice(),
                b"two".to_vec().into_boxed_slice(),
            )),
        ];
        assert_eq!(
            view.merge_bounded(CompositeTableV1::Entities, base, b"a", Some(b"z"), 2, 1,),
            Err(StorageValueError::InvalidShape)
        );
    }

    #[test]
    fn persistent_overlay_map_stays_balanced_for_ordered_keys() {
        for descending in [false, true] {
            let mut map = PersistentOverlayMap::default();
            let keys: Vec<u16> = if descending {
                (0..4_096).rev().collect()
            } else {
                (0..4_096).collect()
            };
            for key in keys {
                map.insert(
                    key.to_be_bytes().to_vec().into_boxed_slice(),
                    OverlayValue::Value(Arc::from(key.to_be_bytes())),
                );
            }
            let root = map.root.as_ref().expect("nonempty map");
            assert!(root.height <= 14, "AVL height was {}", root.height);
            for key in [0_u16, 1, 2_047, 4_095] {
                assert!(matches!(
                    map.get(&key.to_be_bytes()),
                    Some(OverlayValue::Value(value)) if value.as_ref() == key.to_be_bytes()
                ));
            }
            let rows: Vec<u16> = map
                .range(
                    Included(&1_000_u16.to_be_bytes()),
                    Excluded(&1_100_u16.to_be_bytes()),
                )
                .map(|(key, _)| u16::from_be_bytes([key[0], key[1]]))
                .collect();
            assert_eq!(rows, (1_000..1_100).collect::<Vec<_>>());
        }
    }
}
