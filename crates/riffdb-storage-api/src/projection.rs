//! Typed projection state, control, apply, query, and lifecycle persistence.

use std::fmt;
use std::num::NonZeroU16;

use riffdb_types::{
    CanonicalRecord, CanonicalValue, CommitSequence, FrontierPosition,
    MAX_PROJECTION_APPLY_SEMANTIC_BYTES, MAX_PROJECTION_APPLY_SNAPSHOT_BYTES,
    MAX_PROJECTION_QUERY_CONTENT_BYTES, MAX_PROJECTION_QUERY_ROWS, MAX_PROJECTION_ROW_UPDATES,
    MAX_PROJECTION_STATE_SEMANTIC_BYTES, MAX_PROJECTION_WRITE_SET_BYTES, ProjectionApplyHash,
    ProjectionApplyKey, ProjectionGeneration, ProjectionGroupKey, ProjectionGroupKeyBuilder,
    ProjectionIdentity, ProjectionKeyError, encode_canonical_record, hash_projection_apply,
};

use crate::{
    CheckedProjectionSchema, EncodedPageItem, StorageError, StorageValueError,
    canonical_codec_storage_error, checked_encoded_page_content,
};

const APPLY_PAYLOAD_PREFIX: &[u8] = b"RIFFDB-PROJECTION-APPLY\0";
const APPLY_CODEC_VERSION_V1: u32 = 1;

/// Durable lifecycle of one exact projection identity.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ProjectionLifecycleV1 {
    /// Initial candidate exists but its contiguous scan has not started.
    Building,
    /// Initial candidate is consuming the authoritative log.
    CatchingUp,
    /// One published generation is current and queryable.
    Ready,
    /// A replacement candidate is being built beside a published generation.
    Rebuilding,
    /// One retained generation has a closed recoverable failure.
    Degraded,
    /// Required authoritative history or plan is unavailable for rebuilding.
    Invalid,
}

impl ProjectionLifecycleV1 {
    /// Returns the stable v1 semantic tag.
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::Building => 0x01,
            Self::CatchingUp => 0x02,
            Self::Ready => 0x03,
            Self::Degraded => 0x04,
            Self::Rebuilding => 0x05,
            Self::Invalid => 0x06,
        }
    }

    /// Decodes a stable v1 semantic tag.
    #[must_use]
    pub const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0x01 => Some(Self::Building),
            0x02 => Some(Self::CatchingUp),
            0x03 => Some(Self::Ready),
            0x04 => Some(Self::Degraded),
            0x05 => Some(Self::Rebuilding),
            0x06 => Some(Self::Invalid),
            _ => None,
        }
    }
}

/// Whether a retained published generation may continue applying commits.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum PublishedApplyModeV1 {
    /// Published application remains enabled.
    Enabled,
    /// A published-generation failure suspends further application.
    Suspended,
}

impl PublishedApplyModeV1 {
    /// Returns the stable v1 semantic tag.
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::Enabled => 0x01,
            Self::Suspended => 0x02,
        }
    }

    /// Decodes a stable v1 semantic tag.
    #[must_use]
    pub const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0x01 => Some(Self::Enabled),
            0x02 => Some(Self::Suspended),
            _ => None,
        }
    }
}

/// Closed safe reason that one generation could not continue.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ProjectionFailureCodeV1 {
    /// Checked count or sum arithmetic overflowed.
    ArithmeticOverflow,
    /// A relevant durable event is malformed.
    MalformedDurableEvent,
    /// The required authoritative commit is absent.
    MissingCommit,
    /// The exact plan or schema is unavailable or mismatched.
    PlanOrSchemaUnavailable,
    /// Derived state, marker, key, or control integrity failed.
    ProjectionStateIntegrity,
    /// A fixed process hard limit was exceeded.
    HardLimitExceeded,
}

impl ProjectionFailureCodeV1 {
    /// Returns the stable v1 semantic tag.
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::ArithmeticOverflow => 0x01,
            Self::MalformedDurableEvent => 0x02,
            Self::MissingCommit => 0x03,
            Self::PlanOrSchemaUnavailable => 0x04,
            Self::ProjectionStateIntegrity => 0x05,
            Self::HardLimitExceeded => 0x06,
        }
    }

    /// Decodes a stable v1 semantic tag.
    #[must_use]
    pub const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0x01 => Some(Self::ArithmeticOverflow),
            0x02 => Some(Self::MalformedDurableEvent),
            0x03 => Some(Self::MissingCommit),
            0x04 => Some(Self::PlanOrSchemaUnavailable),
            0x05 => Some(Self::ProjectionStateIntegrity),
            0x06 => Some(Self::HardLimitExceeded),
            _ => None,
        }
    }
}

/// Closed durable evidence for one failed retained generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionFailureV1 {
    generation: ProjectionGeneration,
    code: ProjectionFailureCodeV1,
    at_sequence: Option<CommitSequence>,
}

impl ProjectionFailureV1 {
    /// Constructs a closed failure without arbitrary diagnostic text.
    #[must_use]
    pub const fn new(
        generation: ProjectionGeneration,
        code: ProjectionFailureCodeV1,
        at_sequence: Option<CommitSequence>,
    ) -> Self {
        Self {
            generation,
            code,
            at_sequence,
        }
    }

    /// Returns the affected retained generation.
    #[must_use]
    pub const fn generation(&self) -> ProjectionGeneration {
        self.generation
    }

    /// Returns the safe closed failure code.
    #[must_use]
    pub const fn code(&self) -> ProjectionFailureCodeV1 {
        self.code
    }

    /// Returns the sequence whose evaluation failed, when applicable.
    #[must_use]
    pub const fn at_sequence(&self) -> Option<CommitSequence> {
        self.at_sequence
    }
}

/// One retained generation pointer and its contiguous frontier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProjectionGenerationPosition {
    generation: ProjectionGeneration,
    frontier: FrontierPosition,
}

impl ProjectionGenerationPosition {
    /// Constructs a retained generation position.
    #[must_use]
    pub const fn new(generation: ProjectionGeneration, frontier: FrontierPosition) -> Self {
        Self {
            generation,
            frontier,
        }
    }

    /// Returns the retained generation.
    #[must_use]
    pub const fn generation(self) -> ProjectionGeneration {
        self.generation
    }

    /// Returns its contiguous applied position.
    #[must_use]
    pub const fn frontier(self) -> FrontierPosition {
        self.frontier
    }
}

/// Semantic durable projection control record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredProjectionControlV1 {
    identity: ProjectionIdentity,
    highest_allocated_generation: ProjectionGeneration,
    published: Option<ProjectionGenerationPosition>,
    candidate: Option<ProjectionGenerationPosition>,
    published_apply_mode: Option<PublishedApplyModeV1>,
    lifecycle: ProjectionLifecycleV1,
    failure: Option<ProjectionFailureV1>,
}

impl StoredProjectionControlV1 {
    /// Validates and constructs one exact durable control shape.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        identity: ProjectionIdentity,
        highest_allocated_generation: ProjectionGeneration,
        published: Option<ProjectionGenerationPosition>,
        candidate: Option<ProjectionGenerationPosition>,
        published_apply_mode: Option<PublishedApplyModeV1>,
        lifecycle: ProjectionLifecycleV1,
        failure: Option<ProjectionFailureV1>,
    ) -> Result<Self, StorageValueError> {
        let value = Self {
            identity,
            highest_allocated_generation,
            published,
            candidate,
            published_apply_mode,
            lifecycle,
            failure,
        };
        value.validate_shape()?;
        Ok(value)
    }

    /// Creates generation one as an unpublished building candidate.
    #[must_use]
    pub fn initial(identity: ProjectionIdentity) -> Self {
        Self {
            identity,
            highest_allocated_generation: ProjectionGeneration::first(),
            published: None,
            candidate: Some(ProjectionGenerationPosition::new(
                ProjectionGeneration::first(),
                FrontierPosition::BeforeFirst,
            )),
            published_apply_mode: None,
            lifecycle: ProjectionLifecycleV1::Building,
            failure: None,
        }
    }

    /// Returns the exact projection identity.
    #[must_use]
    pub const fn identity(&self) -> &ProjectionIdentity {
        &self.identity
    }

    /// Returns the highest generation ever allocated for this identity.
    #[must_use]
    pub const fn highest_allocated_generation(&self) -> ProjectionGeneration {
        self.highest_allocated_generation
    }

    /// Returns the retained published generation and frontier.
    #[must_use]
    pub const fn published(&self) -> Option<ProjectionGenerationPosition> {
        self.published
    }

    /// Returns the retained candidate generation and frontier.
    #[must_use]
    pub const fn candidate(&self) -> Option<ProjectionGenerationPosition> {
        self.candidate
    }

    /// Returns whether application to a published generation is enabled.
    #[must_use]
    pub const fn published_apply_mode(&self) -> Option<PublishedApplyModeV1> {
        self.published_apply_mode
    }

    /// Returns the durable lifecycle.
    #[must_use]
    pub const fn lifecycle(&self) -> ProjectionLifecycleV1 {
        self.lifecycle
    }

    /// Returns closed failure evidence for degraded or invalid state.
    #[must_use]
    pub const fn failure(&self) -> Option<&ProjectionFailureV1> {
        self.failure.as_ref()
    }

    fn retained_position(
        &self,
        generation: ProjectionGeneration,
    ) -> Option<ProjectionGenerationPosition> {
        self.published
            .filter(|position| position.generation == generation)
            .or_else(|| {
                self.candidate
                    .filter(|position| position.generation == generation)
            })
    }

    fn failure_matches_retained_position(&self, failure: &ProjectionFailureV1) -> bool {
        self.retained_position(failure.generation())
            .is_some_and(|position| {
                failure
                    .at_sequence()
                    .is_none_or(|sequence| is_exact_successor(position.frontier(), sequence))
            })
    }

    fn validate_shape(&self) -> Result<(), StorageValueError> {
        let highest = self.highest_allocated_generation.get();
        for position in [self.published, self.candidate].into_iter().flatten() {
            if position.generation.get() > highest {
                return Err(StorageValueError::InvalidShape);
            }
        }
        if self.published.map(|value| value.generation)
            == self.candidate.map(|value| value.generation)
            && self.published.is_some()
        {
            return Err(StorageValueError::InvalidShape);
        }
        if self
            .candidate
            .is_some_and(|value| value.generation != self.highest_allocated_generation)
            || (self.published.is_some() != self.published_apply_mode.is_some())
        {
            return Err(StorageValueError::InvalidShape);
        }

        let active = self.failure.is_none();
        let shape_valid = match self.lifecycle {
            ProjectionLifecycleV1::Building | ProjectionLifecycleV1::CatchingUp => {
                self.published.is_none()
                    && self
                        .candidate
                        .is_some_and(|value| value.generation == self.highest_allocated_generation)
                    && self.published_apply_mode.is_none()
                    && active
                    && (self.lifecycle != ProjectionLifecycleV1::Building
                        || self
                            .candidate
                            .is_some_and(|value| value.frontier == FrontierPosition::BeforeFirst))
            }
            ProjectionLifecycleV1::Ready => {
                self.published
                    .is_some_and(|value| value.generation == self.highest_allocated_generation)
                    && self.candidate.is_none()
                    && self.published_apply_mode == Some(PublishedApplyModeV1::Enabled)
                    && active
            }
            ProjectionLifecycleV1::Rebuilding => {
                self.published.is_some_and(|value| {
                    value.generation.get() < self.highest_allocated_generation.get()
                }) && self
                    .candidate
                    .is_some_and(|value| value.generation == self.highest_allocated_generation)
                    && active
            }
            ProjectionLifecycleV1::Degraded | ProjectionLifecycleV1::Invalid => {
                let Some(failure) = &self.failure else {
                    return Err(StorageValueError::InvalidShape);
                };
                let failure_targets_published = self
                    .published
                    .is_some_and(|position| position.generation == failure.generation);
                self.failure_matches_retained_position(failure)
                    && (self.published.is_some() || self.candidate.is_some())
                    && (!failure_targets_published
                        || self.published_apply_mode == Some(PublishedApplyModeV1::Suspended))
            }
        };
        if shape_valid {
            Ok(())
        } else {
            Err(StorageValueError::InvalidShape)
        }
    }

    fn position_mut(
        &mut self,
        generation: ProjectionGeneration,
    ) -> Option<&mut ProjectionGenerationPosition> {
        if self
            .published
            .is_some_and(|position| position.generation == generation)
        {
            return self.published.as_mut();
        }
        if self
            .candidate
            .is_some_and(|position| position.generation == generation)
        {
            return self.candidate.as_mut();
        }
        None
    }

    /// Returns whether this lifecycle permits application to the generation.
    #[must_use]
    pub fn permits_application(&self, generation: ProjectionGeneration) -> bool {
        match self.lifecycle {
            ProjectionLifecycleV1::CatchingUp => self
                .candidate
                .is_some_and(|position| position.generation == generation),
            ProjectionLifecycleV1::Ready => {
                self.published_apply_mode == Some(PublishedApplyModeV1::Enabled)
                    && self
                        .published
                        .is_some_and(|position| position.generation == generation)
            }
            ProjectionLifecycleV1::Rebuilding => {
                self.candidate
                    .is_some_and(|position| position.generation == generation)
                    || (self.published_apply_mode == Some(PublishedApplyModeV1::Enabled)
                        && self
                            .published
                            .is_some_and(|position| position.generation == generation))
            }
            ProjectionLifecycleV1::Building
            | ProjectionLifecycleV1::Degraded
            | ProjectionLifecycleV1::Invalid => false,
        }
    }

    /// Returns the frontier for one retained generation.
    #[must_use]
    pub fn frontier_for(&self, generation: ProjectionGeneration) -> Option<FrontierPosition> {
        self.retained_position(generation)
            .map(ProjectionGenerationPosition::frontier)
    }

    /// Produces the checked control post-image for one successful apply.
    pub fn after_apply(
        &self,
        generation: ProjectionGeneration,
        expected: FrontierPosition,
        applied: CommitSequence,
    ) -> Result<Self, StorageValueError> {
        if !self.permits_application(generation)
            || self.frontier_for(generation) != Some(expected)
            || !is_exact_successor(expected, applied)
        {
            return Err(StorageValueError::InvalidShape);
        }
        let mut next = self.clone();
        let position = next
            .position_mut(generation)
            .ok_or(StorageValueError::InvalidShape)?;
        position.frontier = FrontierPosition::AppliedThrough(applied);
        next.validate_shape()?;
        Ok(next)
    }
}

/// Semantic durable state for one projection group row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredProjectionStateV1 {
    key: ProjectionGroupKey,
    measures: CanonicalRecord,
    last_changed_sequence: CommitSequence,
}

impl StoredProjectionStateV1 {
    /// Constructs and schema-validates a complete row post-image.
    pub fn new(
        schema: &CheckedProjectionSchema,
        key: ProjectionGroupKey,
        measures: CanonicalRecord,
        last_changed_sequence: CommitSequence,
    ) -> Result<Self, StorageValueError> {
        validate_projection_state_structure(&key, &measures)?;
        schema.validate_group_key(&key)?;
        schema.validate_measure_record(&measures)?;
        Ok(Self {
            key,
            measures,
            last_changed_sequence,
        })
    }

    /// Returns the complete canonical group key.
    #[must_use]
    pub const fn key(&self) -> &ProjectionGroupKey {
        &self.key
    }

    /// Returns the exact projection identity repeated by the key.
    #[must_use]
    pub const fn identity(&self) -> &ProjectionIdentity {
        self.key.identity()
    }

    /// Returns the generation repeated by the key.
    #[must_use]
    pub const fn generation(&self) -> ProjectionGeneration {
        self.key.generation()
    }

    /// Returns group values in declared order.
    #[must_use]
    pub fn group_values(&self) -> &[CanonicalValue] {
        self.key.components()
    }

    /// Returns the canonical measure record.
    #[must_use]
    pub const fn measures(&self) -> &CanonicalRecord {
        &self.measures
    }

    /// Returns the sequence that most recently changed this row.
    #[must_use]
    pub const fn last_changed_sequence(&self) -> CommitSequence {
        self.last_changed_sequence
    }
}

/// IR-opaque, structurally decoded durable projection group row.
///
/// This type proves canonical scalar components, a complete bounded group key,
/// a canonical measure record, and the complete stored-state semantic bound. It
/// does not prove group arity, component types, measure schema, or plan identity.
#[derive(Clone, Eq, PartialEq)]
pub struct StructurallyDecodedProjectionStateV1 {
    key: ProjectionGroupKey,
    measures: CanonicalRecord,
    last_changed_sequence: CommitSequence,
}

impl StructurallyDecodedProjectionStateV1 {
    /// Reconstructs the structural row from the exact durable payload parts.
    pub fn from_stored_parts(
        identity: ProjectionIdentity,
        generation: ProjectionGeneration,
        group_values: Vec<CanonicalValue>,
        measures: CanonicalRecord,
        last_changed_sequence: CommitSequence,
    ) -> Result<Self, StorageValueError> {
        let mut key = ProjectionGroupKeyBuilder::new(identity, generation);
        for value in group_values {
            key.push_component(value)
                .map_err(projection_key_storage_error)?;
        }
        let key = key.finish().map_err(projection_key_storage_error)?;
        validate_projection_state_structure(&key, &measures)?;
        Ok(Self {
            key,
            measures,
            last_changed_sequence,
        })
    }

    /// Borrows the structurally canonical complete group key.
    #[must_use]
    pub const fn key(&self) -> &ProjectionGroupKey {
        &self.key
    }

    /// Returns the exact projection identity repeated by the payload parts.
    #[must_use]
    pub const fn identity(&self) -> &ProjectionIdentity {
        self.key.identity()
    }

    /// Returns the nonzero generation repeated by the payload parts.
    #[must_use]
    pub const fn generation(&self) -> ProjectionGeneration {
        self.key.generation()
    }

    /// Borrows the structurally canonical scalar values in durable order.
    #[must_use]
    pub fn group_values(&self) -> &[CanonicalValue] {
        self.key.components()
    }

    /// Borrows the canonical measure record without claiming schema validity.
    #[must_use]
    pub const fn measures(&self) -> &CanonicalRecord {
        &self.measures
    }

    /// Returns the nonzero sequence repeated by the payload.
    #[must_use]
    pub const fn last_changed_sequence(&self) -> CommitSequence {
        self.last_changed_sequence
    }

    /// Consumes the structural row and validates it against the exact schema.
    pub fn into_checked(
        self,
        schema: &CheckedProjectionSchema,
    ) -> Result<StoredProjectionStateV1, StorageValueError> {
        StoredProjectionStateV1::new(schema, self.key, self.measures, self.last_changed_sequence)
    }
}

impl fmt::Debug for StructurallyDecodedProjectionStateV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StructurallyDecodedProjectionStateV1")
            .field("key", &self.key)
            .field("measures", &"[REDACTED]")
            .field("last_changed_sequence", &self.last_changed_sequence)
            .finish()
    }
}

/// Semantic durable marker for one applied sequence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredProjectionApplyV1 {
    key: ProjectionApplyKey,
    apply_hash: ProjectionApplyHash,
}

impl StoredProjectionApplyV1 {
    /// Constructs a marker whose repeated fields are derived from its key.
    #[must_use]
    pub const fn new(key: ProjectionApplyKey, apply_hash: ProjectionApplyHash) -> Self {
        Self { key, apply_hash }
    }

    /// Returns the complete marker key.
    #[must_use]
    pub const fn key(&self) -> &ProjectionApplyKey {
        &self.key
    }

    /// Returns the exact request hash.
    #[must_use]
    pub const fn canonical_hash(&self) -> ProjectionApplyHash {
        self.apply_hash
    }
}

/// Optimistic prior evidence for one projection state row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectionRowPrior {
    /// The complete key was absent in the apply snapshot.
    Absent,
    /// The row existed with this last-changing sequence.
    Present(CommitSequence),
}

/// One canonical projection row post-image and its prior evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionRowUpdateV1 {
    key: ProjectionGroupKey,
    prior: ProjectionRowPrior,
    measures: CanonicalRecord,
}

impl ProjectionRowUpdateV1 {
    /// Constructs a schema-validated row update.
    pub fn new(
        schema: &CheckedProjectionSchema,
        key: ProjectionGroupKey,
        prior: ProjectionRowPrior,
        measures: CanonicalRecord,
    ) -> Result<Self, StorageValueError> {
        schema.validate_group_key(&key)?;
        schema.validate_measure_record(&measures)?;
        Ok(Self {
            key,
            prior,
            measures,
        })
    }

    /// Returns the complete group key.
    #[must_use]
    pub const fn key(&self) -> &ProjectionGroupKey {
        &self.key
    }

    /// Returns expected prior row evidence.
    #[must_use]
    pub const fn prior(&self) -> ProjectionRowPrior {
        self.prior
    }

    /// Returns the complete measure post-image.
    #[must_use]
    pub const fn measures(&self) -> &CanonicalRecord {
        &self.measures
    }
}

/// Complete bounded request to apply one authoritative sequence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionApplyRequestV1 {
    schema: CheckedProjectionSchema,
    generation: ProjectionGeneration,
    sequence: CommitSequence,
    expected_frontier: FrontierPosition,
    row_updates: Vec<ProjectionRowUpdateV1>,
    apply_hash: ProjectionApplyHash,
    semantic_bytes: usize,
    write_set_semantic_bytes: usize,
}

impl ProjectionApplyRequestV1 {
    /// Checks identity, order, bounds, schema, and computes the canonical hash.
    pub fn new(
        schema: CheckedProjectionSchema,
        generation: ProjectionGeneration,
        sequence: CommitSequence,
        expected_frontier: FrontierPosition,
        row_updates: Vec<ProjectionRowUpdateV1>,
    ) -> Result<Self, StorageValueError> {
        if !is_exact_successor(expected_frontier, sequence) {
            return Err(StorageValueError::InvalidShape);
        }
        if row_updates.len() > MAX_PROJECTION_ROW_UPDATES {
            return Err(StorageValueError::LimitExceeded);
        }
        let mut previous: Option<&[u8]> = None;
        for row in &row_updates {
            schema.validate_group_key(&row.key)?;
            schema.validate_measure_record(&row.measures)?;
            if row.key.identity() != schema.identity() || row.key.generation() != generation {
                return Err(StorageValueError::IdentityMismatch);
            }
            if let Some(previous) = previous
                && previous >= row.key.as_bytes()
            {
                return Err(if previous == row.key.as_bytes() {
                    StorageValueError::Duplicate
                } else {
                    StorageValueError::NonCanonicalOrder
                });
            }
            if let ProjectionRowPrior::Present(prior) = row.prior
                && !sequence_at_or_before(prior, expected_frontier)
            {
                return Err(StorageValueError::InvalidShape);
            }
            previous = Some(row.key.as_bytes());
        }
        let payload = encode_apply_payload(
            schema.identity(),
            generation,
            sequence,
            expected_frontier,
            &row_updates,
        )?;
        if payload.len() > MAX_PROJECTION_APPLY_SEMANTIC_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        let apply_hash = hash_projection_apply(&payload);
        let write_set_semantic_bytes = projection_write_set_semantic_bytes(
            schema.identity(),
            generation,
            sequence,
            &row_updates,
        )?;
        validate_projection_write_set_semantic_bytes(write_set_semantic_bytes)?;
        Ok(Self {
            schema,
            generation,
            sequence,
            expected_frontier,
            row_updates,
            apply_hash,
            semantic_bytes: payload.len(),
            write_set_semantic_bytes,
        })
    }

    /// Returns the exact checked schema.
    #[must_use]
    pub const fn schema(&self) -> &CheckedProjectionSchema {
        &self.schema
    }

    /// Returns the projection identity.
    #[must_use]
    pub const fn identity(&self) -> &ProjectionIdentity {
        self.schema.identity()
    }

    /// Returns the target generation.
    #[must_use]
    pub const fn generation(&self) -> ProjectionGeneration {
        self.generation
    }

    /// Returns the applied authoritative sequence.
    #[must_use]
    pub const fn sequence(&self) -> CommitSequence {
        self.sequence
    }

    /// Returns the expected prior frontier.
    #[must_use]
    pub const fn expected_frontier(&self) -> FrontierPosition {
        self.expected_frontier
    }

    /// Returns canonically key-ordered row updates.
    #[must_use]
    pub fn row_updates(&self) -> &[ProjectionRowUpdateV1] {
        &self.row_updates
    }

    /// Returns the canonical typed apply hash.
    #[must_use]
    pub const fn apply_hash(&self) -> ProjectionApplyHash {
        self.apply_hash
    }

    /// Returns checked canonical semantic content size.
    #[must_use]
    pub const fn semantic_bytes(&self) -> usize {
        self.semantic_bytes
    }

    /// Returns checked complete staged rows, marker, and control semantic bytes.
    #[must_use]
    pub const fn write_set_semantic_bytes(&self) -> usize {
        self.write_set_semantic_bytes
    }
}

fn is_exact_successor(frontier: FrontierPosition, sequence: CommitSequence) -> bool {
    match frontier {
        FrontierPosition::BeforeFirst => sequence == CommitSequence::first(),
        FrontierPosition::AppliedThrough(previous) => previous.checked_next() == Some(sequence),
    }
}

fn sequence_at_or_before(sequence: CommitSequence, frontier: FrontierPosition) -> bool {
    match frontier {
        FrontierPosition::BeforeFirst => false,
        FrontierPosition::AppliedThrough(applied) => sequence <= applied,
    }
}

fn encode_apply_payload(
    identity: &ProjectionIdentity,
    generation: ProjectionGeneration,
    sequence: CommitSequence,
    expected_frontier: FrontierPosition,
    updates: &[ProjectionRowUpdateV1],
) -> Result<Vec<u8>, StorageValueError> {
    let identity_bytes = identity.to_canonical_bytes();
    let mut bytes = Vec::new();
    bytes.extend_from_slice(APPLY_PAYLOAD_PREFIX);
    bytes.extend_from_slice(&APPLY_CODEC_VERSION_V1.to_be_bytes());
    push_len(&mut bytes, identity_bytes.len())?;
    bytes.extend_from_slice(&identity_bytes);
    bytes.extend_from_slice(&generation.to_be_bytes());
    bytes.extend_from_slice(&sequence.to_be_bytes());
    match expected_frontier {
        FrontierPosition::BeforeFirst => bytes.push(0),
        FrontierPosition::AppliedThrough(value) => {
            bytes.push(1);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
    }
    push_count(&mut bytes, updates.len())?;
    for update in updates {
        push_len(&mut bytes, update.key.as_bytes().len())?;
        bytes.extend_from_slice(update.key.as_bytes());
        match update.prior {
            ProjectionRowPrior::Absent => bytes.push(0),
            ProjectionRowPrior::Present(sequence) => {
                bytes.push(1);
                bytes.extend_from_slice(&sequence.to_be_bytes());
            }
        }
        let measures = encode_canonical_record(&update.measures)
            .map_err(|error| canonical_codec_storage_error(&error))?;
        push_len(&mut bytes, measures.len())?;
        bytes.extend_from_slice(&measures);
        if bytes.len() > MAX_PROJECTION_APPLY_SEMANTIC_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
    }
    Ok(bytes)
}

fn push_len(bytes: &mut Vec<u8>, length: usize) -> Result<(), StorageValueError> {
    let length = u32::try_from(length).map_err(|_| StorageValueError::LimitExceeded)?;
    bytes.extend_from_slice(&length.to_be_bytes());
    Ok(())
}

fn push_count(bytes: &mut Vec<u8>, count: usize) -> Result<(), StorageValueError> {
    push_len(bytes, count)
}

/// One row observation in an internal projection apply snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProjectionApplyRowObservation {
    /// The complete group key is absent.
    Absent(ProjectionGroupKey),
    /// The complete durable row is present.
    Present(StoredProjectionStateV1),
}

impl ProjectionApplyRowObservation {
    /// Returns the complete group key.
    #[must_use]
    pub const fn key(&self) -> &ProjectionGroupKey {
        match self {
            Self::Absent(key) => key,
            Self::Present(row) => row.key(),
        }
    }
}

/// One bounded request for candidate or published row evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionApplySnapshotRequest {
    schema: CheckedProjectionSchema,
    generation: ProjectionGeneration,
    group_keys: Vec<ProjectionGroupKey>,
    minimum_response_semantic_bytes: usize,
}

impl ProjectionApplySnapshotRequest {
    /// Validates identity, generation, canonical order, and hard count bounds.
    pub fn new(
        schema: CheckedProjectionSchema,
        generation: ProjectionGeneration,
        group_keys: Vec<ProjectionGroupKey>,
    ) -> Result<Self, StorageValueError> {
        if group_keys.len() > MAX_PROJECTION_ROW_UPDATES {
            return Err(StorageValueError::LimitExceeded);
        }
        let minimum_response_semantic_bytes = minimum_projection_snapshot_semantic_bytes(
            group_keys.iter().map(|key| key.as_bytes().len()),
        )?;
        validate_projection_snapshot_semantic_bytes(minimum_response_semantic_bytes)?;
        let mut previous: Option<&[u8]> = None;
        for key in &group_keys {
            schema.validate_group_key(key)?;
            if key.identity() != schema.identity() || key.generation() != generation {
                return Err(StorageValueError::IdentityMismatch);
            }
            if let Some(previous) = previous
                && previous >= key.as_bytes()
            {
                return Err(if previous == key.as_bytes() {
                    StorageValueError::Duplicate
                } else {
                    StorageValueError::NonCanonicalOrder
                });
            }
            previous = Some(key.as_bytes());
        }
        Ok(Self {
            schema,
            generation,
            group_keys,
            minimum_response_semantic_bytes,
        })
    }

    /// Returns the exact checked schema.
    #[must_use]
    pub const fn schema(&self) -> &CheckedProjectionSchema {
        &self.schema
    }

    /// Returns the requested generation.
    #[must_use]
    pub const fn generation(&self) -> ProjectionGeneration {
        self.generation
    }

    /// Returns canonical complete keys.
    #[must_use]
    pub fn group_keys(&self) -> &[ProjectionGroupKey] {
        &self.group_keys
    }

    /// Returns the checked all-absent, before-first minimum response size.
    #[must_use]
    pub const fn minimum_response_semantic_bytes(&self) -> usize {
        self.minimum_response_semantic_bytes
    }
}

/// Complete owned row evidence from one storage read transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionApplySnapshot {
    expected_frontier: FrontierPosition,
    rows: Vec<ProjectionApplyRowObservation>,
}

/// Incremental adapter-facing construction of one projection apply snapshot.
///
/// Each complete projection row is charged before retention, so a valid
/// high-count request cannot first clone an over-limit set of large measures.
pub struct ProjectionApplySnapshotBuilder<'request> {
    request: &'request ProjectionApplySnapshotRequest,
    expected_frontier: FrontierPosition,
    rows: Vec<ProjectionApplyRowObservation>,
    semantic_bytes: usize,
}

impl<'request> ProjectionApplySnapshotBuilder<'request> {
    /// Starts bounded materialization at one transaction-current frontier.
    pub fn new(
        request: &'request ProjectionApplySnapshotRequest,
        expected_frontier: FrontierPosition,
    ) -> Result<Self, StorageValueError> {
        let semantic_bytes = projection_snapshot_fixed_semantic_bytes(expected_frontier)?;
        validate_projection_snapshot_semantic_bytes(semantic_bytes)?;
        Ok(Self {
            request,
            expected_frontier,
            rows: Vec::new(),
            semantic_bytes,
        })
    }

    /// Retains the next request-ordered row only after its bound passes.
    pub fn push_row(
        &mut self,
        observation: ProjectionApplyRowObservation,
    ) -> Result<(), StorageValueError> {
        let expected = self
            .request
            .group_keys()
            .get(self.rows.len())
            .ok_or(StorageValueError::InvalidShape)?;
        if expected != observation.key() {
            return Err(StorageValueError::IdentityMismatch);
        }
        let observation_bytes =
            projection_snapshot_observation_semantic_bytes(&observation, self.expected_frontier)?;
        let next = self
            .semantic_bytes
            .checked_add(observation_bytes)
            .ok_or(StorageValueError::SizeOverflow)?;
        validate_projection_snapshot_semantic_bytes(next)?;
        self.semantic_bytes = next;
        self.rows.push(observation);
        Ok(())
    }

    /// Finishes only after every requested key has one observation.
    pub fn finish(self) -> Result<ProjectionApplySnapshot, StorageValueError> {
        if self.rows.len() != self.request.group_keys().len() {
            return Err(StorageValueError::InvalidShape);
        }
        Ok(ProjectionApplySnapshot {
            expected_frontier: self.expected_frontier,
            rows: self.rows,
        })
    }
}

impl ProjectionApplySnapshot {
    /// Constructs a bounded, request-ordered snapshot.
    pub fn new(
        request: &ProjectionApplySnapshotRequest,
        expected_frontier: FrontierPosition,
        rows: Vec<ProjectionApplyRowObservation>,
    ) -> Result<Self, StorageValueError> {
        if rows.len() != request.group_keys.len() {
            return Err(StorageValueError::InvalidShape);
        }
        let mut bytes = projection_snapshot_fixed_semantic_bytes(expected_frontier)?;
        for (expected, observed) in request.group_keys.iter().zip(&rows) {
            if expected != observed.key() {
                return Err(StorageValueError::IdentityMismatch);
            }
            let observation_bytes =
                projection_snapshot_observation_semantic_bytes(observed, expected_frontier)?;
            bytes = bytes
                .checked_add(observation_bytes)
                .ok_or(StorageValueError::SizeOverflow)?;
        }
        validate_projection_snapshot_semantic_bytes(bytes)?;
        Ok(Self {
            expected_frontier,
            rows,
        })
    }

    /// Returns the generation frontier observed with every row.
    #[must_use]
    pub const fn expected_frontier(&self) -> FrontierPosition {
        self.expected_frontier
    }

    /// Returns observations in request key order.
    #[must_use]
    pub fn rows(&self) -> &[ProjectionApplyRowObservation] {
        &self.rows
    }
}

fn projection_snapshot_fixed_semantic_bytes(
    expected_frontier: FrontierPosition,
) -> Result<usize, StorageValueError> {
    frontier_semantic_bytes(expected_frontier)
        .checked_add(4)
        .ok_or(StorageValueError::SizeOverflow)
}

fn projection_snapshot_observation_semantic_bytes(
    observed: &ProjectionApplyRowObservation,
    expected_frontier: FrontierPosition,
) -> Result<usize, StorageValueError> {
    match observed {
        ProjectionApplyRowObservation::Absent(key) => 1usize
            .checked_add(framed_bytes(key.as_bytes().len())?)
            .ok_or(StorageValueError::SizeOverflow),
        ProjectionApplyRowObservation::Present(row) => {
            if !sequence_at_or_before(row.last_changed_sequence(), expected_frontier) {
                return Err(StorageValueError::InvalidShape);
            }
            1usize
                .checked_add(projection_state_semantic_bytes(row.key(), row.measures())?)
                .ok_or(StorageValueError::SizeOverflow)
        }
    }
}

/// Generation-neutral checked selector for a complete or prefix query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionQuerySelector {
    schema: CheckedProjectionSchema,
    leading_components: Vec<CanonicalValue>,
}

impl ProjectionQuerySelector {
    /// Validates zero or more complete leading components against the schema.
    pub fn new(
        schema: CheckedProjectionSchema,
        leading_components: Vec<CanonicalValue>,
    ) -> Result<Self, StorageValueError> {
        schema.group_prefix(ProjectionGeneration::first(), &leading_components)?;
        Ok(Self {
            schema,
            leading_components,
        })
    }

    /// Returns the exact generation-neutral identity.
    #[must_use]
    pub const fn identity(&self) -> &ProjectionIdentity {
        self.schema.identity()
    }

    /// Returns the checked schema.
    #[must_use]
    pub const fn schema(&self) -> &CheckedProjectionSchema {
        &self.schema
    }

    /// Returns complete leading group values.
    #[must_use]
    pub fn leading_components(&self) -> &[CanonicalValue] {
        &self.leading_components
    }
}

/// Internal lower-bound continuation fenced to one published snapshot.
#[derive(Clone, Eq, PartialEq)]
pub struct ProjectionLowerContinuation {
    identity: ProjectionIdentity,
    generation: ProjectionGeneration,
    prefix: Vec<u8>,
    exclusive_last_key: ProjectionGroupKey,
    observed_frontier: FrontierPosition,
}

impl fmt::Debug for ProjectionLowerContinuation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProjectionLowerContinuation")
            .field("identity", &self.identity)
            .field("generation", &self.generation)
            .field("prefix", &"[REDACTED]")
            .field("prefix_length", &self.prefix.len())
            .field("exclusive_last_key", &self.exclusive_last_key)
            .field("observed_frontier", &self.observed_frontier)
            .finish()
    }
}

impl ProjectionLowerContinuation {
    /// Constructs a checked internal continuation; it is never client-authored.
    pub fn new(
        selector: &ProjectionQuerySelector,
        generation: ProjectionGeneration,
        exclusive_last_key: ProjectionGroupKey,
        observed_frontier: FrontierPosition,
    ) -> Result<Self, StorageValueError> {
        selector.schema.validate_group_key(&exclusive_last_key)?;
        if exclusive_last_key.identity() != selector.identity()
            || exclusive_last_key.generation() != generation
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        let prefix = selector
            .schema
            .group_prefix(generation, &selector.leading_components)?
            .as_bytes()
            .to_vec();
        if !exclusive_last_key.as_bytes().starts_with(&prefix) {
            return Err(StorageValueError::InvalidShape);
        }
        Ok(Self {
            identity: selector.identity().clone(),
            generation,
            prefix,
            exclusive_last_key,
            observed_frontier,
        })
    }

    /// Returns the exact projection identity.
    #[must_use]
    pub const fn identity(&self) -> &ProjectionIdentity {
        &self.identity
    }

    /// Returns the selected published generation.
    #[must_use]
    pub const fn generation(&self) -> ProjectionGeneration {
        self.generation
    }

    /// Returns the complete generated prefix bytes.
    #[must_use]
    pub fn prefix(&self) -> &[u8] {
        &self.prefix
    }

    /// Returns the exclusive complete lower key.
    #[must_use]
    pub const fn exclusive_last_key(&self) -> &ProjectionGroupKey {
        &self.exclusive_last_key
    }

    /// Returns the frontier fencing the page sequence.
    #[must_use]
    pub const fn observed_frontier(&self) -> FrontierPosition {
        self.observed_frontier
    }
}

/// One bounded projection query request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionQueryRequest {
    selector: ProjectionQuerySelector,
    limit: NonZeroU16,
    continuation: Option<ProjectionLowerContinuation>,
}

impl ProjectionQueryRequest {
    /// Constructs a request with a hard maximum of 500 rows.
    pub fn new(
        selector: ProjectionQuerySelector,
        limit: NonZeroU16,
        continuation: Option<ProjectionLowerContinuation>,
    ) -> Result<Self, StorageValueError> {
        if usize::from(limit.get()) > MAX_PROJECTION_QUERY_ROWS {
            return Err(StorageValueError::LimitExceeded);
        }
        if let Some(continuation) = &continuation {
            selector
                .schema
                .validate_group_key(&continuation.exclusive_last_key)?;
            let expected_prefix = selector
                .schema
                .group_prefix(continuation.generation, &selector.leading_components)?;
            if continuation.identity != *selector.identity()
                || continuation.exclusive_last_key.identity() != selector.identity()
                || continuation.exclusive_last_key.generation() != continuation.generation
                || continuation.prefix != expected_prefix.as_bytes()
                || !continuation
                    .exclusive_last_key
                    .as_bytes()
                    .starts_with(&continuation.prefix)
            {
                return Err(StorageValueError::IdentityMismatch);
            }
        }
        Ok(Self {
            selector,
            limit,
            continuation,
        })
    }

    /// Returns the checked selector.
    #[must_use]
    pub const fn selector(&self) -> &ProjectionQuerySelector {
        &self.selector
    }

    /// Returns the nonzero row limit.
    #[must_use]
    pub const fn limit(&self) -> NonZeroU16 {
        self.limit
    }

    /// Returns the optional server-owned continuation.
    #[must_use]
    pub const fn continuation(&self) -> Option<&ProjectionLowerContinuation> {
        self.continuation.as_ref()
    }
}

/// Safe reason for a non-ready projection query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectionUnavailableReason {
    /// Initial state is not yet published.
    Building,
    /// A replacement generation is in progress.
    Rebuilding,
    /// One retained generation has a closed failure.
    Failure(ProjectionFailureCodeV1),
}

impl ProjectionUnavailableReason {
    /// Returns the stable v1 unavailable-reason tag.
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::Building => 0x01,
            Self::Rebuilding => 0x02,
            Self::Failure(_) => 0x03,
        }
    }
}

/// One storage-atomic projection query result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProjectionQueryResult {
    /// Published rows and frontier from one read transaction.
    Ready {
        /// Selected published generation.
        generation: ProjectionGeneration,
        /// Frontier observed with the row set.
        frontier: FrontierPosition,
        /// Complete bounded rows in key order.
        rows: Vec<EncodedPageItem<StoredProjectionStateV1>>,
        /// Internal next lower bound, when another page exists.
        next: Option<Box<ProjectionLowerContinuation>>,
    },
    /// No rows are exposed while the lifecycle is unavailable.
    Degraded {
        /// Visible affected-generation position.
        current: FrontierPosition,
        /// Closed safe reason.
        reason: ProjectionUnavailableReason,
    },
    /// Rebuild is impossible from retained authoritative history.
    Invalid {
        /// Closed safe failure reason.
        reason: ProjectionFailureCodeV1,
    },
    /// Publication or frontier advancement invalidated the lower fence.
    ContinuationInvalidated,
}

impl ProjectionQueryResult {
    /// Validates row ordering and query content bounds for a ready page.
    pub fn ready(
        request: &ProjectionQueryRequest,
        generation: ProjectionGeneration,
        frontier: FrontierPosition,
        rows: Vec<EncodedPageItem<StoredProjectionStateV1>>,
        next: Option<ProjectionLowerContinuation>,
    ) -> Result<Self, StorageValueError> {
        if rows.len() > usize::from(request.limit.get()) {
            return Err(StorageValueError::LimitExceeded);
        }
        let expected_prefix = request
            .selector
            .schema
            .group_prefix(generation, &request.selector.leading_components)?;
        if let Some(continuation) = &request.continuation
            && (continuation.identity != *request.selector.identity()
                || continuation.generation != generation
                || continuation.prefix != expected_prefix.as_bytes()
                || continuation.observed_frontier != frontier)
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        let encoded_row_bytes = checked_encoded_page_content(&rows, usize::MAX)?;
        let mut previous: Option<&ProjectionGroupKey> = None;
        for charged in &rows {
            let row = charged.value();
            if row.identity() != request.selector.identity()
                || row.generation() != generation
                || !row.key().as_bytes().starts_with(expected_prefix.as_bytes())
                || !sequence_at_or_before(row.last_changed_sequence(), frontier)
            {
                return Err(StorageValueError::IdentityMismatch);
            }
            if previous.is_some_and(|key| key >= row.key()) {
                return Err(StorageValueError::NonCanonicalOrder);
            }
            previous = Some(row.key());
        }
        if let (Some(continuation), Some(first)) = (&request.continuation, rows.first())
            && first.value().key() <= &continuation.exclusive_last_key
        {
            return Err(StorageValueError::NonCanonicalOrder);
        }
        if let Some(next) = &next {
            let last = rows
                .last()
                .map(EncodedPageItem::value)
                .ok_or(StorageValueError::InvalidShape)?;
            if next.identity != *request.selector.identity()
                || next.generation != generation
                || next.prefix != expected_prefix.as_bytes()
                || next.observed_frontier != frontier
                || next.exclusive_last_key != *last.key()
            {
                return Err(StorageValueError::IdentityMismatch);
            }
        }
        validate_projection_query_encoded_bytes(encoded_row_bytes)?;
        Ok(Self::Ready {
            generation,
            frontier,
            rows,
            next: next.map(Box::new),
        })
    }
}

/// API-neutral status assembled from one storage read transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionStatus {
    identity: ProjectionIdentity,
    lifecycle: ProjectionLifecycleV1,
    published: Option<ProjectionGenerationPosition>,
    candidate: Option<ProjectionGenerationPosition>,
    published_apply_mode: Option<PublishedApplyModeV1>,
    failure: Option<ProjectionFailureV1>,
    authoritative_head: FrontierPosition,
}

impl ProjectionStatus {
    /// Maps a control record and transaction-current authoritative head.
    #[must_use]
    pub fn from_control(
        control: &StoredProjectionControlV1,
        authoritative_head: FrontierPosition,
    ) -> Self {
        Self {
            identity: control.identity.clone(),
            lifecycle: control.lifecycle,
            published: control.published,
            candidate: control.candidate,
            published_apply_mode: control.published_apply_mode,
            failure: control.failure.clone(),
            authoritative_head,
        }
    }

    /// Maps normal absence for a known identity to building at before-first.
    #[must_use]
    pub fn uninitialized(
        identity: ProjectionIdentity,
        authoritative_head: FrontierPosition,
    ) -> Self {
        Self {
            identity,
            lifecycle: ProjectionLifecycleV1::Building,
            published: None,
            candidate: None,
            published_apply_mode: None,
            failure: None,
            authoritative_head,
        }
    }

    /// Returns the exact identity.
    #[must_use]
    pub const fn identity(&self) -> &ProjectionIdentity {
        &self.identity
    }

    /// Returns the closed lifecycle.
    #[must_use]
    pub const fn lifecycle(&self) -> ProjectionLifecycleV1 {
        self.lifecycle
    }

    /// Returns the published pointer and frontier.
    #[must_use]
    pub const fn published(&self) -> Option<ProjectionGenerationPosition> {
        self.published
    }

    /// Returns the candidate pointer and frontier.
    #[must_use]
    pub const fn candidate(&self) -> Option<ProjectionGenerationPosition> {
        self.candidate
    }

    /// Returns the published application mode.
    #[must_use]
    pub const fn published_apply_mode(&self) -> Option<PublishedApplyModeV1> {
        self.published_apply_mode
    }

    /// Returns optional closed failure detail.
    #[must_use]
    pub const fn failure(&self) -> Option<&ProjectionFailureV1> {
        self.failure.as_ref()
    }

    /// Returns the authoritative application-log head seen in the same read.
    #[must_use]
    pub const fn authoritative_head(&self) -> FrontierPosition {
        self.authoritative_head
    }
}

/// One exact specialized projection control operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProjectionControlOperation {
    /// Insert generation one only if no control exists.
    CreateInitial {
        /// Exact checked projection schema and identity.
        schema: CheckedProjectionSchema,
    },
    /// Move initial building state to catching up.
    StartInitialScan {
        /// Complete expected prior record.
        expected: StoredProjectionControlV1,
    },
    /// Allocate the next never-reused rebuild candidate.
    AllocateRebuild {
        /// Complete expected prior record.
        expected: StoredProjectionControlV1,
    },
    /// Publish a candidate only at the transaction-current authoritative head.
    PublishCandidate {
        /// Complete expected prior record.
        expected: StoredProjectionControlV1,
    },
    /// Persist one closed generation failure without advancing its retained frontier.
    RecordFailure {
        /// Complete expected prior record.
        expected: StoredProjectionControlV1,
        /// Failure naming one retained pointer.
        failure: ProjectionFailureV1,
    },
    /// Apply the exact ADR-0017 recovery edge for degraded state.
    RecoverDegraded {
        /// Complete expected prior record.
        expected: StoredProjectionControlV1,
    },
    /// Mark a degraded projection invalid when rebuild inputs are unavailable.
    MarkInvalid {
        /// Complete expected degraded record.
        expected: StoredProjectionControlV1,
    },
}

/// Closed result of a projection control compare-and-set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProjectionControlResult {
    /// The operation durably installed this complete control post-image.
    Updated(StoredProjectionControlV1),
    /// Transaction-current control did not equal the expected prior record.
    StateChanged,
    /// An insert found an existing control record.
    AlreadyInitialized(StoredProjectionControlV1),
    /// The generation space is exhausted; no record changed.
    GenerationExhausted,
}

/// Pure ADR-0017 projection-control compare-and-transition evaluator.
///
/// Concrete engines supply the exact transaction-current record and application
/// head, then persist only an [`ProjectionControlResult::Updated`] post-image.
/// Keeping lifecycle calculation here prevents memory and durable adapters from
/// developing different recovery, publication, or exhaustion behavior.
pub fn evaluate_projection_control_operation(
    current: Option<&StoredProjectionControlV1>,
    operation: &ProjectionControlOperation,
    authoritative_head: FrontierPosition,
) -> Result<ProjectionControlResult, StorageValueError> {
    let expected = match operation {
        ProjectionControlOperation::CreateInitial { schema } => {
            return evaluate_projection_control_creation(current, schema.identity());
        }
        ProjectionControlOperation::StartInitialScan { expected }
        | ProjectionControlOperation::AllocateRebuild { expected }
        | ProjectionControlOperation::PublishCandidate { expected }
        | ProjectionControlOperation::RecordFailure { expected, .. }
        | ProjectionControlOperation::RecoverDegraded { expected }
        | ProjectionControlOperation::MarkInvalid { expected } => expected,
    };
    if current != Some(expected) {
        return Ok(ProjectionControlResult::StateChanged);
    }

    let result = match operation {
        ProjectionControlOperation::CreateInitial { .. } => {
            return Err(StorageValueError::InvalidShape);
        }
        ProjectionControlOperation::StartInitialScan { .. } => {
            if expected.lifecycle() != ProjectionLifecycleV1::Building {
                return Err(StorageValueError::InvalidShape);
            }
            ProjectionControlResult::Updated(rebuild_control(
                expected,
                expected.highest_allocated_generation(),
                expected.published(),
                expected.candidate(),
                expected.published_apply_mode(),
                ProjectionLifecycleV1::CatchingUp,
                None,
            )?)
        }
        ProjectionControlOperation::AllocateRebuild { .. } => {
            if expected.lifecycle() != ProjectionLifecycleV1::Ready {
                return Err(StorageValueError::InvalidShape);
            }
            let Some(next) = expected.highest_allocated_generation().checked_next() else {
                return Ok(ProjectionControlResult::GenerationExhausted);
            };
            ProjectionControlResult::Updated(rebuild_control(
                expected,
                next,
                expected.published(),
                Some(ProjectionGenerationPosition::new(
                    next,
                    FrontierPosition::BeforeFirst,
                )),
                expected.published_apply_mode(),
                ProjectionLifecycleV1::Rebuilding,
                None,
            )?)
        }
        ProjectionControlOperation::PublishCandidate { .. } => {
            if !matches!(
                expected.lifecycle(),
                ProjectionLifecycleV1::Building
                    | ProjectionLifecycleV1::CatchingUp
                    | ProjectionLifecycleV1::Rebuilding
            ) {
                return Err(StorageValueError::InvalidShape);
            }
            let candidate = expected
                .candidate()
                .ok_or(StorageValueError::InvalidShape)?;
            if candidate.frontier() != authoritative_head
                || (expected.lifecycle() == ProjectionLifecycleV1::Rebuilding
                    && expected
                        .published()
                        .is_none_or(|published| candidate.frontier() < published.frontier()))
            {
                return Err(StorageValueError::InvalidShape);
            }
            ProjectionControlResult::Updated(rebuild_control(
                expected,
                expected.highest_allocated_generation(),
                Some(candidate),
                None,
                Some(PublishedApplyModeV1::Enabled),
                ProjectionLifecycleV1::Ready,
                None,
            )?)
        }
        ProjectionControlOperation::RecordFailure { failure, .. } => {
            if !matches!(
                expected.lifecycle(),
                ProjectionLifecycleV1::Building
                    | ProjectionLifecycleV1::CatchingUp
                    | ProjectionLifecycleV1::Ready
                    | ProjectionLifecycleV1::Rebuilding
            ) || !expected.failure_matches_retained_position(failure)
            {
                return Err(StorageValueError::InvalidShape);
            }
            let published_apply_mode = if expected
                .published()
                .is_some_and(|published| published.generation() == failure.generation())
            {
                Some(PublishedApplyModeV1::Suspended)
            } else {
                expected.published_apply_mode()
            };
            ProjectionControlResult::Updated(rebuild_control(
                expected,
                expected.highest_allocated_generation(),
                expected.published(),
                expected.candidate(),
                published_apply_mode,
                ProjectionLifecycleV1::Degraded,
                Some(failure.clone()),
            )?)
        }
        ProjectionControlOperation::RecoverDegraded { .. } => {
            if expected.lifecycle() != ProjectionLifecycleV1::Degraded {
                return Err(StorageValueError::InvalidShape);
            }
            let failure = expected.failure().ok_or(StorageValueError::InvalidShape)?;
            let failure_is_published = expected
                .published()
                .is_some_and(|published| published.generation() == failure.generation());
            let (highest, candidate, lifecycle) =
                if failure_is_published && expected.candidate().is_some() {
                    (
                        expected.highest_allocated_generation(),
                        expected.candidate(),
                        ProjectionLifecycleV1::Rebuilding,
                    )
                } else {
                    let Some(next) = expected.highest_allocated_generation().checked_next() else {
                        return Ok(ProjectionControlResult::GenerationExhausted);
                    };
                    (
                        next,
                        Some(ProjectionGenerationPosition::new(
                            next,
                            FrontierPosition::BeforeFirst,
                        )),
                        if expected.published().is_some() {
                            ProjectionLifecycleV1::Rebuilding
                        } else {
                            ProjectionLifecycleV1::Building
                        },
                    )
                };
            ProjectionControlResult::Updated(rebuild_control(
                expected,
                highest,
                expected.published(),
                candidate,
                expected.published_apply_mode(),
                lifecycle,
                None,
            )?)
        }
        ProjectionControlOperation::MarkInvalid { .. } => {
            if expected.lifecycle() != ProjectionLifecycleV1::Degraded {
                return Err(StorageValueError::InvalidShape);
            }
            ProjectionControlResult::Updated(rebuild_control(
                expected,
                expected.highest_allocated_generation(),
                expected.published(),
                expected.candidate(),
                expected.published_apply_mode(),
                ProjectionLifecycleV1::Invalid,
                expected.failure().cloned(),
            )?)
        }
    };
    Ok(result)
}

fn evaluate_projection_control_creation(
    current: Option<&StoredProjectionControlV1>,
    identity: &ProjectionIdentity,
) -> Result<ProjectionControlResult, StorageValueError> {
    Ok(match current {
        Some(existing) => {
            if existing.identity() != identity {
                return Err(StorageValueError::IdentityMismatch);
            }
            ProjectionControlResult::AlreadyInitialized(existing.clone())
        }
        None => {
            ProjectionControlResult::Updated(StoredProjectionControlV1::initial(identity.clone()))
        }
    })
}

#[allow(clippy::too_many_arguments)]
fn rebuild_control(
    prior: &StoredProjectionControlV1,
    highest_allocated_generation: ProjectionGeneration,
    published: Option<ProjectionGenerationPosition>,
    candidate: Option<ProjectionGenerationPosition>,
    published_apply_mode: Option<PublishedApplyModeV1>,
    lifecycle: ProjectionLifecycleV1,
    failure: Option<ProjectionFailureV1>,
) -> Result<StoredProjectionControlV1, StorageValueError> {
    StoredProjectionControlV1::new(
        prior.identity().clone(),
        highest_allocated_generation,
        published,
        candidate,
        published_apply_mode,
        lifecycle,
        failure,
    )
}

/// Closed result of one projection apply transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProjectionApplyResult {
    /// Rows, marker, and frontier advanced atomically.
    Applied {
        /// Newly durable equality marker.
        marker: StoredProjectionApplyV1,
        /// Complete control post-image.
        control: StoredProjectionControlV1,
    },
    /// An equal historical marker proves the request was already applied.
    AlreadyApplied(StoredProjectionApplyV1),
    /// Control or row prior evidence changed before the transaction.
    StateChanged,
}

/// Worker-only transaction-atomic apply-snapshot read port.
pub trait ProjectionApplySnapshotReader {
    /// Reads control, frontier, and every requested row in one read view.
    fn read_apply_snapshot(
        &self,
        request: &ProjectionApplySnapshotRequest,
    ) -> Result<ProjectionApplySnapshot, StorageError>;
}

/// Worker-only derived projection mutation and control port.
pub trait ProjectionMutationRepository {
    /// Applies rows, exact marker, and frontier in one atomic transaction.
    fn apply_projection(
        &mut self,
        request: &ProjectionApplyRequestV1,
    ) -> Result<ProjectionApplyResult, StorageError>;

    /// Executes one closed compare-and-set lifecycle transition.
    fn transition_projection_control(
        &mut self,
        operation: ProjectionControlOperation,
    ) -> Result<ProjectionControlResult, StorageError>;
}

/// Least-authority projection query and status read port.
pub trait ProjectionQueryReader {
    /// Reads lifecycle, selected published rows, frontier, and continuation in
    /// one storage read transaction.
    fn query_projection(
        &self,
        request: &ProjectionQueryRequest,
    ) -> Result<ProjectionQueryResult, StorageError>;

    /// Reads status and authoritative head in one storage read transaction.
    fn read_projection_status(
        &self,
        identity: &ProjectionIdentity,
    ) -> Result<ProjectionStatus, StorageError>;
}

fn projection_write_set_semantic_bytes(
    identity: &ProjectionIdentity,
    generation: ProjectionGeneration,
    sequence: CommitSequence,
    rows: &[ProjectionRowUpdateV1],
) -> Result<usize, StorageValueError> {
    let rows_bytes = rows.iter().try_fold(4usize, |total, row| {
        total
            .checked_add(projection_state_semantic_bytes(&row.key, &row.measures)?)
            .ok_or(StorageValueError::SizeOverflow)
    })?;
    let marker_key = ProjectionApplyKey::new(identity.clone(), generation, sequence);
    let marker_bytes = framed_bytes(marker_key.as_bytes().len())?
        .checked_add(32)
        .ok_or(StorageValueError::SizeOverflow)?;
    let control_bytes = maximum_projection_control_semantic_bytes(identity)?;
    checked_projection_sum([rows_bytes, marker_bytes, control_bytes])
}

fn projection_state_semantic_bytes(
    key: &ProjectionGroupKey,
    measures: &CanonicalRecord,
) -> Result<usize, StorageValueError> {
    let measures =
        encode_canonical_record(measures).map_err(|error| canonical_codec_storage_error(&error))?;
    let identity_generation_and_components = key
        .as_bytes()
        .len()
        .checked_sub(2)
        .ok_or(StorageValueError::InvalidShape)?;
    checked_projection_sum([
        identity_generation_and_components,
        framed_bytes(measures.len())?,
        8,
        32,
    ])
}

fn validate_projection_state_structure(
    key: &ProjectionGroupKey,
    measures: &CanonicalRecord,
) -> Result<(), StorageValueError> {
    validate_projection_bound(
        projection_state_semantic_bytes(key, measures)?,
        MAX_PROJECTION_STATE_SEMANTIC_BYTES,
    )
}

const fn projection_key_storage_error(error: ProjectionKeyError) -> StorageValueError {
    match error {
        ProjectionKeyError::TooLong { .. } | ProjectionKeyError::TooManyComponents => {
            StorageValueError::LimitExceeded
        }
        ProjectionKeyError::Truncated
        | ProjectionKeyError::TrailingBytes
        | ProjectionKeyError::TruncatedOrTrailing
        | ProjectionKeyError::WrongPurpose
        | ProjectionKeyError::UnsupportedVersion
        | ProjectionKeyError::InvalidIdentity
        | ProjectionKeyError::ZeroProjectionId
        | ProjectionKeyError::ZeroGeneration
        | ProjectionKeyError::ZeroCommitSequence
        | ProjectionKeyError::EmptyGroupKey
        | ProjectionKeyError::EmptyComponent
        | ProjectionKeyError::NonScalarComponent
        | ProjectionKeyError::InvalidCanonicalComponent => StorageValueError::InvalidShape,
    }
}

fn maximum_projection_control_semantic_bytes(
    identity: &ProjectionIdentity,
) -> Result<usize, StorageValueError> {
    let retained_position =
        1 + 8 + frontier_semantic_bytes(FrontierPosition::AppliedThrough(CommitSequence::first()));
    let maximum_failure = 1 + 8 + 1 + 1 + 8;
    checked_projection_sum([
        framed_bytes(identity.to_canonical_bytes().len())?,
        8,
        retained_position,
        retained_position,
        1 + 1,
        1,
        maximum_failure,
    ])
}

fn minimum_projection_snapshot_semantic_bytes(
    key_lengths: impl IntoIterator<Item = usize>,
) -> Result<usize, StorageValueError> {
    key_lengths.into_iter().try_fold(5usize, |total, length| {
        total
            .checked_add(1)
            .and_then(|value| value.checked_add(framed_bytes(length).ok()?))
            .ok_or(StorageValueError::SizeOverflow)
    })
}

const fn frontier_semantic_bytes(frontier: FrontierPosition) -> usize {
    match frontier {
        FrontierPosition::BeforeFirst => 1,
        FrontierPosition::AppliedThrough(_) => 1 + 8,
    }
}

fn framed_bytes(content_bytes: usize) -> Result<usize, StorageValueError> {
    4usize
        .checked_add(content_bytes)
        .ok_or(StorageValueError::SizeOverflow)
}

fn checked_projection_sum(
    parts: impl IntoIterator<Item = usize>,
) -> Result<usize, StorageValueError> {
    parts.into_iter().try_fold(0usize, |total, part| {
        total
            .checked_add(part)
            .ok_or(StorageValueError::SizeOverflow)
    })
}

fn validate_projection_write_set_semantic_bytes(bytes: usize) -> Result<(), StorageValueError> {
    validate_projection_bound(bytes, MAX_PROJECTION_WRITE_SET_BYTES)
}

fn validate_projection_snapshot_semantic_bytes(bytes: usize) -> Result<(), StorageValueError> {
    validate_projection_bound(bytes, MAX_PROJECTION_APPLY_SNAPSHOT_BYTES)
}

fn validate_projection_query_encoded_bytes(bytes: usize) -> Result<(), StorageValueError> {
    validate_projection_bound(bytes, MAX_PROJECTION_QUERY_CONTENT_BYTES)
}

fn validate_projection_bound(bytes: usize, maximum: usize) -> Result<(), StorageValueError> {
    if bytes > maximum {
        return Err(StorageValueError::LimitExceeded);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use riffdb_contract_compiler::compile_contract_source;
    use riffdb_types::{
        CanonicalRecord, CanonicalValue, ContractLineage, FieldId, ProjectionGroupKeyBuilder,
        ProjectionId, ProjectionPlanHash,
    };

    use super::*;

    fn projection_identity() -> ProjectionIdentity {
        ProjectionIdentity::new(
            ContractLineage::new("budget").expect("lineage"),
            ProjectionId::try_from(1).expect("projection ID"),
            ProjectionPlanHash::from_bytes([7; 32]),
        )
    }

    fn other_projection_identity() -> ProjectionIdentity {
        ProjectionIdentity::new(
            ContractLineage::new("ledger").expect("lineage"),
            ProjectionId::try_from(2).expect("projection ID"),
            ProjectionPlanHash::from_bytes([8; 32]),
        )
    }

    fn maximum_key_projection_schema() -> CheckedProjectionSchema {
        let contract_name = "A".repeat(256);
        let source = format!(
            "contract {contract_name} version 1 {{\n  event Source {{ group: string<3780> }}\n  projection Totals {{\n    source event Source\n    key (group)\n    measure total = count()\n    frontier transactionally_ordered\n  }}\n}}\n"
        );
        let bundle = compile_contract_source(&source).expect("maximum-key projection compiles");
        CheckedProjectionSchema::new(
            bundle
                .bound_projection_group_schema(ProjectionId::first())
                .expect("projection one"),
        )
    }

    fn count_measures() -> CanonicalRecord {
        CanonicalRecord::new(vec![(FieldId::first(), CanonicalValue::U64(1))])
            .expect("count measures")
    }

    #[test]
    fn structural_projection_state_requires_context_before_operational_use() {
        let schema = maximum_key_projection_schema();
        let generation = ProjectionGeneration::first();
        let group_value = CanonicalValue::string("group-a").expect("group value");
        let key = schema
            .group_key(generation, std::slice::from_ref(&group_value))
            .expect("schema-valid key");
        let expected =
            StoredProjectionStateV1::new(&schema, key, count_measures(), CommitSequence::first())
                .expect("schema-valid state");

        let structural = StructurallyDecodedProjectionStateV1::from_stored_parts(
            schema.identity().clone(),
            generation,
            vec![group_value],
            count_measures(),
            CommitSequence::first(),
        )
        .expect("structurally valid state");
        assert_eq!(structural.identity(), schema.identity());
        assert_eq!(structural.generation(), generation);
        assert_eq!(structural.group_values().len(), 1);
        assert_eq!(structural.measures(), expected.measures());
        assert_eq!(structural.last_changed_sequence(), CommitSequence::first());
        assert_eq!(structural.into_checked(&schema), Ok(expected));

        let wrong_type = StructurallyDecodedProjectionStateV1::from_stored_parts(
            schema.identity().clone(),
            generation,
            vec![CanonicalValue::U64(1)],
            count_measures(),
            CommitSequence::first(),
        )
        .expect("u64 is structurally canonical");
        assert_eq!(
            wrong_type.into_checked(&schema),
            Err(StorageValueError::InvalidShape)
        );
    }

    #[test]
    fn structural_projection_state_rejects_non_scalar_empty_and_over_limit_parts() {
        let identity = projection_identity();
        let generation = ProjectionGeneration::first();
        let measures = count_measures();
        assert_eq!(
            StructurallyDecodedProjectionStateV1::from_stored_parts(
                identity.clone(),
                generation,
                Vec::new(),
                measures.clone(),
                CommitSequence::first(),
            ),
            Err(StorageValueError::InvalidShape)
        );
        assert_eq!(
            StructurallyDecodedProjectionStateV1::from_stored_parts(
                identity.clone(),
                generation,
                vec![CanonicalValue::Null],
                measures,
                CommitSequence::first(),
            ),
            Err(StorageValueError::InvalidShape)
        );

        let maximum_measures = CanonicalRecord::new(vec![(
            FieldId::first(),
            CanonicalValue::bytes(vec![0xa5; riffdb_types::MAX_CANONICAL_DOCUMENT_BYTES - 16])
                .expect("bounded bytes"),
        )])
        .expect("bounded record");
        assert_eq!(
            StructurallyDecodedProjectionStateV1::from_stored_parts(
                identity,
                generation,
                vec![CanonicalValue::U64(1)],
                maximum_measures,
                CommitSequence::first(),
            ),
            Err(StorageValueError::LimitExceeded)
        );
    }

    fn generation(value: u64) -> ProjectionGeneration {
        ProjectionGeneration::new(value).expect("nonzero generation")
    }

    fn sequence(value: u64) -> CommitSequence {
        CommitSequence::new(value).expect("nonzero commit sequence")
    }

    fn applied(value: u64) -> FrontierPosition {
        FrontierPosition::AppliedThrough(sequence(value))
    }

    fn position(generation: u64, frontier: FrontierPosition) -> ProjectionGenerationPosition {
        ProjectionGenerationPosition::new(self::generation(generation), frontier)
    }

    #[allow(clippy::too_many_arguments)]
    fn control(
        highest: u64,
        published: Option<ProjectionGenerationPosition>,
        candidate: Option<ProjectionGenerationPosition>,
        published_apply_mode: Option<PublishedApplyModeV1>,
        lifecycle: ProjectionLifecycleV1,
        failure: Option<ProjectionFailureV1>,
    ) -> StoredProjectionControlV1 {
        StoredProjectionControlV1::new(
            projection_identity(),
            generation(highest),
            published,
            candidate,
            published_apply_mode,
            lifecycle,
            failure,
        )
        .expect("valid test control")
    }

    fn updated(result: ProjectionControlResult) -> StoredProjectionControlV1 {
        let ProjectionControlResult::Updated(control) = result else {
            panic!("expected updated projection control");
        };
        control
    }

    fn evaluate(
        current: &StoredProjectionControlV1,
        operation: ProjectionControlOperation,
        head: FrontierPosition,
    ) -> Result<ProjectionControlResult, StorageValueError> {
        evaluate_projection_control_operation(Some(current), &operation, head)
    }

    #[test]
    fn lifecycle_and_failure_tags_are_closed() {
        for tag in 1..=6 {
            assert_eq!(ProjectionLifecycleV1::from_tag(tag).unwrap().tag(), tag);
            assert_eq!(ProjectionFailureCodeV1::from_tag(tag).unwrap().tag(), tag);
        }
        assert_eq!(ProjectionLifecycleV1::from_tag(0), None);
        assert_eq!(ProjectionFailureCodeV1::from_tag(7), None);
        assert_eq!(PublishedApplyModeV1::from_tag(0), None);
        assert_eq!(
            PublishedApplyModeV1::from_tag(1),
            Some(PublishedApplyModeV1::Enabled)
        );
        assert_eq!(
            PublishedApplyModeV1::from_tag(2),
            Some(PublishedApplyModeV1::Suspended)
        );
    }

    #[test]
    fn projection_snapshot_builder_rejects_present_overflow_before_retention_and_finishes_complete()
    {
        let schema = maximum_key_projection_schema();
        assert_eq!(schema.maximum_complete_key_bytes(), 4_096);
        let generation = ProjectionGeneration::first();
        let prefix = "x".repeat(3_772);
        let keys = (0..4_090u32)
            .map(|index| {
                let value = CanonicalValue::string(format!("{prefix}{index:08x}"))
                    .expect("maximum string value");
                schema
                    .group_key(generation, &[value])
                    .expect("maximum group key")
            })
            .collect::<Vec<_>>();
        let request = ProjectionApplySnapshotRequest::new(schema.clone(), generation, keys.clone())
            .expect("all-absent minimum fits");
        assert!(request.minimum_response_semantic_bytes() <= MAX_PROJECTION_APPLY_SNAPSHOT_BYTES);

        let frontier = FrontierPosition::AppliedThrough(CommitSequence::first());
        let mut builder = ProjectionApplySnapshotBuilder::new(&request, frontier)
            .expect("projection snapshot builder");
        let mut rejected = false;
        for key in &keys {
            let row = StoredProjectionStateV1::new(
                &schema,
                key.clone(),
                count_measures(),
                CommitSequence::first(),
            )
            .expect("projection row");
            let retained = builder.rows.len();
            match builder.push_row(ProjectionApplyRowObservation::Present(row)) {
                Ok(()) => {}
                Err(StorageValueError::LimitExceeded) => {
                    assert_eq!(builder.rows.len(), retained);
                    rejected = true;
                    break;
                }
                Err(error) => panic!("unexpected builder error: {error:?}"),
            }
        }
        assert!(rejected, "present rows must exceed the all-absent minimum");
        assert_eq!(
            builder.finish(),
            Err(StorageValueError::InvalidShape),
            "the rejected observation was not retained"
        );

        let small_request =
            ProjectionApplySnapshotRequest::new(schema.clone(), generation, keys[..2].to_vec())
                .expect("small request");
        let present = StoredProjectionStateV1::new(
            &schema,
            keys[0].clone(),
            count_measures(),
            CommitSequence::first(),
        )
        .expect("present row");
        let observations = vec![
            ProjectionApplyRowObservation::Present(present),
            ProjectionApplyRowObservation::Absent(keys[1].clone()),
        ];
        let direct = ProjectionApplySnapshot::new(&small_request, frontier, observations.clone())
            .expect("direct snapshot");
        let mut complete = ProjectionApplySnapshotBuilder::new(&small_request, frontier)
            .expect("complete builder");
        for observation in observations {
            complete.push_row(observation).expect("complete row");
        }
        assert_eq!(complete.finish().expect("complete snapshot"), direct);
    }

    #[test]
    fn continuation_debug_redacts_key_and_prefix_content() {
        let identity = projection_identity();
        let generation = ProjectionGeneration::first();
        let mut key = ProjectionGroupKeyBuilder::new(identity.clone(), generation);
        key.push_component(CanonicalValue::string("projection-secret").expect("value"))
            .expect("component");
        let continuation = ProjectionLowerContinuation {
            identity,
            generation,
            prefix: b"prefix-secret".to_vec(),
            exclusive_last_key: key.finish().expect("group key"),
            observed_frontier: FrontierPosition::BeforeFirst,
        };

        let diagnostic = format!("{continuation:?}");
        assert!(diagnostic.contains("[REDACTED]"));
        assert!(!diagnostic.contains("projection-secret"));
        assert!(!diagnostic.contains("prefix-secret"));
        assert!(!diagnostic.contains("112, 114, 101, 102, 105, 120"));
    }

    #[test]
    fn building_candidate_cannot_have_applied_a_commit() {
        assert_eq!(
            StoredProjectionControlV1::new(
                projection_identity(),
                ProjectionGeneration::first(),
                None,
                Some(ProjectionGenerationPosition::new(
                    ProjectionGeneration::first(),
                    FrontierPosition::AppliedThrough(CommitSequence::first()),
                )),
                None,
                ProjectionLifecycleV1::Building,
                None,
            ),
            Err(StorageValueError::InvalidShape)
        );
    }

    #[test]
    fn projection_semantic_byte_bounds_accept_exact_limit_only() {
        for (maximum, validate) in [
            (
                MAX_PROJECTION_WRITE_SET_BYTES,
                validate_projection_write_set_semantic_bytes as fn(usize) -> _,
            ),
            (
                MAX_PROJECTION_APPLY_SNAPSHOT_BYTES,
                validate_projection_snapshot_semantic_bytes as fn(usize) -> _,
            ),
            (
                MAX_PROJECTION_QUERY_CONTENT_BYTES,
                validate_projection_query_encoded_bytes as fn(usize) -> _,
            ),
        ] {
            assert_eq!(validate(maximum), Ok(()));
            assert_eq!(validate(maximum + 1), Err(StorageValueError::LimitExceeded));
        }
    }

    #[test]
    fn stored_projection_state_charge_includes_the_exact_framing_reserve() {
        let mut key =
            ProjectionGroupKeyBuilder::new(projection_identity(), ProjectionGeneration::first());
        key.push_component(CanonicalValue::U64(1))
            .expect("group component");
        let key = key.finish().expect("group key");
        let measures = CanonicalRecord::new(Vec::new()).expect("empty measures");
        let measure_bytes = encode_canonical_record(&measures)
            .expect("canonical measures")
            .len();
        let direct_adr_0017 =
            (key.as_bytes().len() - 2) + framed_bytes(measure_bytes).expect("framing") + 8 + 32;
        let old_key_framed_formula = framed_bytes(key.as_bytes().len()).expect("framing")
            + framed_bytes(measure_bytes).expect("framing")
            + 8;

        assert_eq!(
            projection_state_semantic_bytes(&key, &measures),
            Ok(direct_adr_0017)
        );
        assert_eq!(direct_adr_0017 - old_key_framed_formula, 26);
    }

    #[test]
    fn apply_snapshot_request_preflights_all_absent_minimum() {
        let maximum_key_lengths =
            std::iter::repeat_n(riffdb_types::MAX_KEY_BYTES, MAX_PROJECTION_ROW_UPDATES);
        let minimum = minimum_projection_snapshot_semantic_bytes(maximum_key_lengths)
            .expect("checked minimum");

        assert!(minimum > MAX_PROJECTION_APPLY_SNAPSHOT_BYTES);
        assert_eq!(
            validate_projection_snapshot_semantic_bytes(minimum),
            Err(StorageValueError::LimitExceeded)
        );
    }

    #[test]
    fn successor_rule_never_uses_sequence_zero() {
        assert!(is_exact_successor(
            FrontierPosition::BeforeFirst,
            CommitSequence::first()
        ));
        assert!(!is_exact_successor(
            FrontierPosition::AppliedThrough(CommitSequence::first()),
            CommitSequence::first()
        ));
        let two = CommitSequence::new(2).expect("nonzero");
        assert!(is_exact_successor(
            FrontierPosition::AppliedThrough(CommitSequence::first()),
            two
        ));
    }

    #[test]
    fn projection_control_creation_is_insert_if_absent() {
        let identity = projection_identity();
        assert_eq!(
            evaluate_projection_control_creation(None, &identity),
            Ok(ProjectionControlResult::Updated(
                StoredProjectionControlV1::initial(identity.clone())
            ))
        );

        let existing = StoredProjectionControlV1::initial(identity.clone());
        assert_eq!(
            evaluate_projection_control_creation(Some(&existing), &identity),
            Ok(ProjectionControlResult::AlreadyInitialized(
                existing.clone()
            ))
        );
        assert_eq!(
            evaluate_projection_control_creation(Some(&existing), &other_projection_identity()),
            Err(StorageValueError::IdentityMismatch)
        );
    }

    #[test]
    fn initial_scan_and_publication_follow_transaction_current_head() {
        let initial = StoredProjectionControlV1::initial(projection_identity());
        let catching_up = updated(
            evaluate(
                &initial,
                ProjectionControlOperation::StartInitialScan {
                    expected: initial.clone(),
                },
                FrontierPosition::BeforeFirst,
            )
            .expect("start scan"),
        );
        assert_eq!(catching_up.lifecycle(), ProjectionLifecycleV1::CatchingUp);
        assert_eq!(catching_up.candidate(), initial.candidate());

        assert_eq!(
            evaluate(
                &initial,
                ProjectionControlOperation::PublishCandidate {
                    expected: initial.clone(),
                },
                applied(1),
            ),
            Err(StorageValueError::InvalidShape)
        );
        let ready = updated(
            evaluate(
                &initial,
                ProjectionControlOperation::PublishCandidate {
                    expected: initial.clone(),
                },
                FrontierPosition::BeforeFirst,
            )
            .expect("publish empty database"),
        );
        assert_eq!(ready.lifecycle(), ProjectionLifecycleV1::Ready);
        assert_eq!(ready.published(), initial.candidate());
        assert_eq!(ready.candidate(), None);
        assert_eq!(
            ready.published_apply_mode(),
            Some(PublishedApplyModeV1::Enabled)
        );

        let caught_up = control(
            1,
            None,
            Some(position(1, applied(2))),
            None,
            ProjectionLifecycleV1::CatchingUp,
            None,
        );
        assert_eq!(
            updated(
                evaluate(
                    &caught_up,
                    ProjectionControlOperation::PublishCandidate {
                        expected: caught_up.clone(),
                    },
                    applied(2),
                )
                .expect("publish caught-up initial generation")
            )
            .published(),
            Some(position(1, applied(2)))
        );
    }

    #[test]
    fn rebuild_allocation_and_publication_are_monotonic() {
        let ready = control(
            1,
            Some(position(1, applied(2))),
            None,
            Some(PublishedApplyModeV1::Enabled),
            ProjectionLifecycleV1::Ready,
            None,
        );
        let rebuilding = updated(
            evaluate(
                &ready,
                ProjectionControlOperation::AllocateRebuild {
                    expected: ready.clone(),
                },
                applied(2),
            )
            .expect("allocate rebuild"),
        );
        assert_eq!(rebuilding.highest_allocated_generation(), generation(2));
        assert_eq!(
            rebuilding.candidate(),
            Some(position(2, FrontierPosition::BeforeFirst))
        );
        assert_eq!(rebuilding.published(), ready.published());

        let caught_up = control(
            2,
            Some(position(1, applied(2))),
            Some(position(2, applied(3))),
            Some(PublishedApplyModeV1::Enabled),
            ProjectionLifecycleV1::Rebuilding,
            None,
        );
        let replacement = updated(
            evaluate(
                &caught_up,
                ProjectionControlOperation::PublishCandidate {
                    expected: caught_up.clone(),
                },
                applied(3),
            )
            .expect("publish replacement"),
        );
        assert_eq!(replacement.lifecycle(), ProjectionLifecycleV1::Ready);
        assert_eq!(replacement.published(), Some(position(2, applied(3))));
        assert_eq!(replacement.candidate(), None);

        let behind = control(
            2,
            Some(position(1, applied(2))),
            Some(position(2, applied(1))),
            Some(PublishedApplyModeV1::Enabled),
            ProjectionLifecycleV1::Rebuilding,
            None,
        );
        assert_eq!(
            evaluate(
                &behind,
                ProjectionControlOperation::PublishCandidate {
                    expected: behind.clone(),
                },
                applied(1),
            ),
            Err(StorageValueError::InvalidShape)
        );
    }

    #[test]
    fn failure_requires_exact_successor_or_omission_and_suspends_only_published() {
        let ready = control(
            1,
            Some(position(1, applied(2))),
            None,
            Some(PublishedApplyModeV1::Enabled),
            ProjectionLifecycleV1::Ready,
            None,
        );
        let matching_published_failure = ProjectionFailureV1::new(
            generation(1),
            ProjectionFailureCodeV1::ProjectionStateIntegrity,
            Some(sequence(3)),
        );
        let degraded = updated(
            evaluate(
                &ready,
                ProjectionControlOperation::RecordFailure {
                    expected: ready.clone(),
                    failure: matching_published_failure.clone(),
                },
                applied(2),
            )
            .expect("record published failure"),
        );
        assert_eq!(degraded.lifecycle(), ProjectionLifecycleV1::Degraded);
        assert_eq!(degraded.failure(), Some(&matching_published_failure));
        assert_eq!(
            degraded.published_apply_mode(),
            Some(PublishedApplyModeV1::Suspended)
        );

        for stale_failure in [
            ProjectionFailureV1::new(
                generation(1),
                ProjectionFailureCodeV1::ProjectionStateIntegrity,
                Some(sequence(2)),
            ),
            ProjectionFailureV1::new(
                generation(2),
                ProjectionFailureCodeV1::ProjectionStateIntegrity,
                Some(sequence(2)),
            ),
        ] {
            assert_eq!(
                evaluate(
                    &ready,
                    ProjectionControlOperation::RecordFailure {
                        expected: ready.clone(),
                        failure: stale_failure,
                    },
                    applied(2),
                ),
                Err(StorageValueError::InvalidShape)
            );
        }

        let rebuilding = control(
            2,
            Some(position(1, applied(2))),
            Some(position(2, applied(1))),
            Some(PublishedApplyModeV1::Enabled),
            ProjectionLifecycleV1::Rebuilding,
            None,
        );
        let candidate_failure = ProjectionFailureV1::new(
            generation(2),
            ProjectionFailureCodeV1::ArithmeticOverflow,
            Some(sequence(2)),
        );
        let degraded_candidate = updated(
            evaluate(
                &rebuilding,
                ProjectionControlOperation::RecordFailure {
                    expected: rebuilding.clone(),
                    failure: candidate_failure,
                },
                applied(2),
            )
            .expect("record candidate failure"),
        );
        assert_eq!(
            degraded_candidate.published_apply_mode(),
            Some(PublishedApplyModeV1::Enabled)
        );

        let initial = StoredProjectionControlV1::initial(projection_identity());
        let before_first_failure = ProjectionFailureV1::new(
            generation(1),
            ProjectionFailureCodeV1::PlanOrSchemaUnavailable,
            None,
        );
        assert!(
            evaluate(
                &initial,
                ProjectionControlOperation::RecordFailure {
                    expected: initial.clone(),
                    failure: before_first_failure,
                },
                FrontierPosition::BeforeFirst,
            )
            .is_ok()
        );
        let first_commit_failure = ProjectionFailureV1::new(
            generation(1),
            ProjectionFailureCodeV1::PlanOrSchemaUnavailable,
            Some(sequence(1)),
        );
        let degraded_before_first = updated(
            evaluate(
                &initial,
                ProjectionControlOperation::RecordFailure {
                    expected: initial.clone(),
                    failure: first_commit_failure.clone(),
                },
                FrontierPosition::BeforeFirst,
            )
            .expect("first commit failure follows before-first frontier"),
        );
        assert_eq!(degraded_before_first.candidate(), initial.candidate());
        assert_eq!(degraded_before_first.failure(), Some(&first_commit_failure));

        assert_eq!(
            evaluate(
                &initial,
                ProjectionControlOperation::RecordFailure {
                    expected: initial.clone(),
                    failure: ProjectionFailureV1::new(
                        generation(1),
                        ProjectionFailureCodeV1::PlanOrSchemaUnavailable,
                        Some(sequence(2)),
                    ),
                },
                FrontierPosition::BeforeFirst,
            ),
            Err(StorageValueError::InvalidShape)
        );

        let omitted_sequence_failure = ProjectionFailureV1::new(
            generation(1),
            ProjectionFailureCodeV1::PlanOrSchemaUnavailable,
            None,
        );
        let degraded_after_progress = updated(
            evaluate(
                &ready,
                ProjectionControlOperation::RecordFailure {
                    expected: ready.clone(),
                    failure: omitted_sequence_failure.clone(),
                },
                applied(2),
            )
            .expect("pre-application failure may omit a sequence after progress"),
        );
        assert_eq!(degraded_after_progress.published(), ready.published());
        assert_eq!(
            degraded_after_progress.failure(),
            Some(&omitted_sequence_failure)
        );
    }

    #[test]
    fn degraded_recovery_covers_every_pointer_shape() {
        let initial = StoredProjectionControlV1::initial(projection_identity());
        let failed_initial = updated(
            evaluate(
                &initial,
                ProjectionControlOperation::RecordFailure {
                    expected: initial.clone(),
                    failure: ProjectionFailureV1::new(
                        generation(1),
                        ProjectionFailureCodeV1::PlanOrSchemaUnavailable,
                        None,
                    ),
                },
                FrontierPosition::BeforeFirst,
            )
            .expect("fail initial candidate"),
        );
        let rebuilt_initial = updated(
            evaluate(
                &failed_initial,
                ProjectionControlOperation::RecoverDegraded {
                    expected: failed_initial.clone(),
                },
                FrontierPosition::BeforeFirst,
            )
            .expect("recover initial candidate"),
        );
        assert_eq!(rebuilt_initial.lifecycle(), ProjectionLifecycleV1::Building);
        assert_eq!(rebuilt_initial.published(), None);
        assert_eq!(
            rebuilt_initial.candidate(),
            Some(position(2, FrontierPosition::BeforeFirst))
        );

        let rebuilding = control(
            2,
            Some(position(1, applied(2))),
            Some(position(2, applied(1))),
            Some(PublishedApplyModeV1::Enabled),
            ProjectionLifecycleV1::Rebuilding,
            None,
        );
        let failed_candidate = updated(
            evaluate(
                &rebuilding,
                ProjectionControlOperation::RecordFailure {
                    expected: rebuilding.clone(),
                    failure: ProjectionFailureV1::new(
                        generation(2),
                        ProjectionFailureCodeV1::ArithmeticOverflow,
                        Some(sequence(2)),
                    ),
                },
                applied(2),
            )
            .expect("fail replacement candidate"),
        );
        let replaced_candidate = updated(
            evaluate(
                &failed_candidate,
                ProjectionControlOperation::RecoverDegraded {
                    expected: failed_candidate.clone(),
                },
                applied(2),
            )
            .expect("replace failed candidate"),
        );
        assert_eq!(
            replaced_candidate.lifecycle(),
            ProjectionLifecycleV1::Rebuilding
        );
        assert_eq!(replaced_candidate.published(), rebuilding.published());
        assert_eq!(
            replaced_candidate.candidate(),
            Some(position(3, FrontierPosition::BeforeFirst))
        );

        let failed_published_with_candidate = updated(
            evaluate(
                &rebuilding,
                ProjectionControlOperation::RecordFailure {
                    expected: rebuilding.clone(),
                    failure: ProjectionFailureV1::new(
                        generation(1),
                        ProjectionFailureCodeV1::MalformedDurableEvent,
                        Some(sequence(3)),
                    ),
                },
                applied(2),
            )
            .expect("fail retained published generation"),
        );
        let resumed_candidate = updated(
            evaluate(
                &failed_published_with_candidate,
                ProjectionControlOperation::RecoverDegraded {
                    expected: failed_published_with_candidate.clone(),
                },
                applied(2),
            )
            .expect("resume retained candidate"),
        );
        assert_eq!(resumed_candidate.candidate(), rebuilding.candidate());
        assert_eq!(
            resumed_candidate.published_apply_mode(),
            Some(PublishedApplyModeV1::Suspended)
        );

        let ready = control(
            1,
            Some(position(1, applied(2))),
            None,
            Some(PublishedApplyModeV1::Enabled),
            ProjectionLifecycleV1::Ready,
            None,
        );
        let failed_published = updated(
            evaluate(
                &ready,
                ProjectionControlOperation::RecordFailure {
                    expected: ready.clone(),
                    failure: ProjectionFailureV1::new(
                        generation(1),
                        ProjectionFailureCodeV1::MissingCommit,
                        Some(sequence(3)),
                    ),
                },
                applied(2),
            )
            .expect("fail ready generation"),
        );
        let replacement = updated(
            evaluate(
                &failed_published,
                ProjectionControlOperation::RecoverDegraded {
                    expected: failed_published.clone(),
                },
                applied(2),
            )
            .expect("allocate replacement for published failure"),
        );
        assert_eq!(
            replacement.candidate(),
            Some(position(2, FrontierPosition::BeforeFirst))
        );
        assert_eq!(
            replacement.published_apply_mode(),
            Some(PublishedApplyModeV1::Suspended)
        );

        let invalid = updated(
            evaluate(
                &failed_published,
                ProjectionControlOperation::MarkInvalid {
                    expected: failed_published.clone(),
                },
                applied(2),
            )
            .expect("mark invalid"),
        );
        assert_eq!(invalid.lifecycle(), ProjectionLifecycleV1::Invalid);
        assert_eq!(invalid.published(), failed_published.published());
        assert_eq!(invalid.candidate(), failed_published.candidate());
        assert_eq!(invalid.failure(), failed_published.failure());
        assert_eq!(
            invalid.published_apply_mode(),
            failed_published.published_apply_mode()
        );
        assert_eq!(
            evaluate(
                &invalid,
                ProjectionControlOperation::RecoverDegraded {
                    expected: invalid.clone(),
                },
                applied(2),
            ),
            Err(StorageValueError::InvalidShape)
        );
    }

    #[test]
    fn generation_exhaustion_is_a_non_mutating_typed_result() {
        let maximum = u64::MAX;
        let ready = control(
            maximum,
            Some(position(maximum, applied(1))),
            None,
            Some(PublishedApplyModeV1::Enabled),
            ProjectionLifecycleV1::Ready,
            None,
        );
        assert_eq!(
            evaluate(
                &ready,
                ProjectionControlOperation::AllocateRebuild {
                    expected: ready.clone(),
                },
                applied(1),
            ),
            Ok(ProjectionControlResult::GenerationExhausted)
        );

        let degraded = control(
            maximum,
            None,
            Some(position(maximum, FrontierPosition::BeforeFirst)),
            None,
            ProjectionLifecycleV1::Degraded,
            Some(ProjectionFailureV1::new(
                generation(maximum),
                ProjectionFailureCodeV1::HardLimitExceeded,
                None,
            )),
        );
        assert_eq!(
            evaluate(
                &degraded,
                ProjectionControlOperation::RecoverDegraded {
                    expected: degraded.clone(),
                },
                FrontierPosition::BeforeFirst,
            ),
            Ok(ProjectionControlResult::GenerationExhausted)
        );
    }

    #[test]
    fn all_expected_state_operations_return_state_changed_before_policy_evaluation() {
        let expected = StoredProjectionControlV1::initial(projection_identity());
        let operations = [
            ProjectionControlOperation::StartInitialScan {
                expected: expected.clone(),
            },
            ProjectionControlOperation::AllocateRebuild {
                expected: expected.clone(),
            },
            ProjectionControlOperation::PublishCandidate {
                expected: expected.clone(),
            },
            ProjectionControlOperation::RecordFailure {
                expected: expected.clone(),
                failure: ProjectionFailureV1::new(
                    generation(1),
                    ProjectionFailureCodeV1::ProjectionStateIntegrity,
                    None,
                ),
            },
            ProjectionControlOperation::RecoverDegraded {
                expected: expected.clone(),
            },
            ProjectionControlOperation::MarkInvalid {
                expected: expected.clone(),
            },
        ];
        for operation in operations {
            assert_eq!(
                evaluate_projection_control_operation(
                    None,
                    &operation,
                    FrontierPosition::BeforeFirst,
                ),
                Ok(ProjectionControlResult::StateChanged)
            );
        }
    }

    #[test]
    fn invalid_lifecycle_operations_fail_closed() {
        let initial = StoredProjectionControlV1::initial(projection_identity());
        let ready = control(
            1,
            Some(position(1, FrontierPosition::BeforeFirst)),
            None,
            Some(PublishedApplyModeV1::Enabled),
            ProjectionLifecycleV1::Ready,
            None,
        );
        let invalid_operations = [
            (
                initial.clone(),
                ProjectionControlOperation::AllocateRebuild {
                    expected: initial.clone(),
                },
            ),
            (
                ready.clone(),
                ProjectionControlOperation::StartInitialScan {
                    expected: ready.clone(),
                },
            ),
            (
                ready.clone(),
                ProjectionControlOperation::PublishCandidate {
                    expected: ready.clone(),
                },
            ),
            (
                ready.clone(),
                ProjectionControlOperation::RecoverDegraded {
                    expected: ready.clone(),
                },
            ),
            (
                ready.clone(),
                ProjectionControlOperation::MarkInvalid {
                    expected: ready.clone(),
                },
            ),
        ];
        for (current, operation) in invalid_operations {
            assert_eq!(
                evaluate(&current, operation, FrontierPosition::BeforeFirst),
                Err(StorageValueError::InvalidShape)
            );
        }
    }

    #[test]
    fn degraded_control_rejects_failure_that_is_not_frontier_successor() {
        assert_eq!(
            StoredProjectionControlV1::new(
                projection_identity(),
                generation(1),
                Some(position(1, applied(2))),
                None,
                Some(PublishedApplyModeV1::Suspended),
                ProjectionLifecycleV1::Degraded,
                Some(ProjectionFailureV1::new(
                    generation(1),
                    ProjectionFailureCodeV1::ProjectionStateIntegrity,
                    Some(sequence(2)),
                )),
            ),
            Err(StorageValueError::InvalidShape)
        );
    }
}
