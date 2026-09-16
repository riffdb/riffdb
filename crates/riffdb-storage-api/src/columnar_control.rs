//! Closed durable selector semantics for schema-bound columnar generations.

use std::fmt;
use std::num::NonZeroU64;

use riffdb_types::{
    ColumnarProjectionSourceV1, ColumnarProjectionSpecHashV1, DefinitionFingerprint,
    FrontierPosition, ProjectionGeneration,
};

pub use riffdb_types::ColumnarProjectionReplayLimitsV1;

use crate::HISTORY_INCARNATION_INITIAL;
use crate::StorageError;

/// Physical layout selected by one immutable columnar generation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ColumnarProjectionLayoutV1 {
    /// Accepted row-oriented V1 manifest.
    V1,
    /// Accepted columnar V2 generation root.
    V2,
}

impl ColumnarProjectionLayoutV1 {
    /// Stable Protobuf enum value.
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::V1 => 1,
            Self::V2 => 2,
        }
    }

    /// Decodes a nonzero, nonreserved enum value.
    #[must_use]
    pub const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::V1),
            2 => Some(Self::V2),
            _ => None,
        }
    }
}

/// Exact role of one control-selected generation pointer.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ColumnarProjectionGenerationRoleV1 {
    /// Currently servable selector, subject to lifecycle health.
    Published,
    /// Private build or replacement selector.
    Candidate,
    /// Unservable artifact retained while a disjoint rebuild proceeds.
    Predecessor,
}

impl ColumnarProjectionGenerationRoleV1 {
    /// Stable Protobuf enum value.
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::Published => 1,
            Self::Candidate => 2,
            Self::Predecessor => 3,
        }
    }

    /// Decodes a nonzero, nonreserved enum value.
    #[must_use]
    pub const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::Published),
            2 => Some(Self::Candidate),
            3 => Some(Self::Predecessor),
            _ => None,
        }
    }
}

/// Closed common-control lifecycle.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ColumnarProjectionLifecycleV1 {
    /// A fresh V1 candidate has no published predecessor.
    Building,
    /// Published V1 remains servable while a V2 candidate catches up.
    CatchingUp,
    /// Exactly one published generation is healthy.
    Ready,
    /// A replacement candidate is building.
    Rebuilding,
    /// A retained failure closes some or all serving.
    Degraded,
    /// Control corruption or irrecoverable control failure closes all serving.
    Invalid,
}

impl ColumnarProjectionLifecycleV1 {
    /// Stable Protobuf enum value.
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::Building => 1,
            Self::CatchingUp => 2,
            Self::Ready => 3,
            Self::Rebuilding => 4,
            Self::Degraded => 5,
            Self::Invalid => 6,
        }
    }

    /// Decodes a nonzero, nonreserved enum value.
    #[must_use]
    pub const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::Building),
            2 => Some(Self::CatchingUp),
            3 => Some(Self::Ready),
            4 => Some(Self::Rebuilding),
            5 => Some(Self::Degraded),
            6 => Some(Self::Invalid),
            _ => None,
        }
    }
}

/// Pointer or control targeted by one durable failure.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ColumnarProjectionFailureTargetV1 {
    /// The private candidate failed.
    Candidate,
    /// The selected published artifact is corrupt.
    Published,
    /// Allocated for the closed durable registry; no V1 state admits it.
    Predecessor,
    /// The control itself is invalid.
    Control,
}

impl ColumnarProjectionFailureTargetV1 {
    /// Stable Protobuf enum value.
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::Candidate => 1,
            Self::Published => 2,
            Self::Predecessor => 3,
            Self::Control => 4,
        }
    }

    /// Decodes a nonzero, nonreserved enum value.
    #[must_use]
    pub const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::Candidate),
            2 => Some(Self::Published),
            3 => Some(Self::Predecessor),
            4 => Some(Self::Control),
            _ => None,
        }
    }
}

/// Closed reason for a columnar-control failure.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ColumnarProjectionFailureReasonV1 {
    /// Retained commit age exceeded the sealed limit.
    ReplayAge,
    /// Retained commit bytes exceeded the sealed limit.
    ReplayBytes,
    /// Retained sequence backlog exceeded the sealed limit.
    ReplayBacklog,
    /// The complete same-source specification changed.
    SpecChanged,
    /// Selected immutable evidence failed validation.
    ArtifactInvalid,
    /// A bounded resource ceiling was reached.
    ResourceLimit,
    /// Durable storage failed.
    Storage,
    /// Private work was cancelled.
    Cancelled,
}

impl ColumnarProjectionFailureReasonV1 {
    /// Stable Protobuf enum value.
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::ReplayAge => 1,
            Self::ReplayBytes => 2,
            Self::ReplayBacklog => 3,
            Self::SpecChanged => 4,
            Self::ArtifactInvalid => 5,
            Self::ResourceLimit => 6,
            Self::Storage => 7,
            Self::Cancelled => 8,
        }
    }

    /// Decodes a nonzero, nonreserved enum value.
    #[must_use]
    pub const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::ReplayAge),
            2 => Some(Self::ReplayBytes),
            3 => Some(Self::ReplayBacklog),
            4 => Some(Self::SpecChanged),
            5 => Some(Self::ArtifactInvalid),
            6 => Some(Self::ResourceLimit),
            7 => Some(Self::Storage),
            8 => Some(Self::Cancelled),
            _ => None,
        }
    }
}

/// Positive immutable artifact length and exact checksum selected by control.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ColumnarProjectionArtifactV1 {
    length: NonZeroU64,
    checksum: [u8; 32],
}

impl ColumnarProjectionArtifactV1 {
    /// Constructs exact immutable-artifact evidence.
    #[must_use]
    pub const fn new(length: u64, checksum: [u8; 32]) -> Option<Self> {
        match NonZeroU64::new(length) {
            Some(length) => Some(Self { length, checksum }),
            None => None,
        }
    }

    /// Selected byte extent.
    #[must_use]
    pub const fn length(self) -> u64 {
        self.length.get()
    }

    /// Checksum naming the selected immutable artifact.
    #[must_use]
    pub const fn checksum(self) -> [u8; 32] {
        self.checksum
    }
}

/// One exact generation pointer in the common durable control.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredColumnarProjectionGenerationV1 {
    generation: ProjectionGeneration,
    layout: ColumnarProjectionLayoutV1,
    frontier: FrontierPosition,
    history_incarnation: u64,
    artifact: Option<ColumnarProjectionArtifactV1>,
    definition_fingerprint: DefinitionFingerprint,
    spec_hash: ColumnarProjectionSpecHashV1,
    physical_generation_fingerprint: Option<[u8; 32]>,
    snapshot_frontier: Option<FrontierPosition>,
    role: ColumnarProjectionGenerationRoleV1,
}

impl StoredColumnarProjectionGenerationV1 {
    /// Constructs and closes every role/evidence/layout presence invariant.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        generation: ProjectionGeneration,
        layout: ColumnarProjectionLayoutV1,
        frontier: FrontierPosition,
        history_incarnation: u64,
        artifact: Option<ColumnarProjectionArtifactV1>,
        definition_fingerprint: DefinitionFingerprint,
        spec_hash: ColumnarProjectionSpecHashV1,
        physical_generation_fingerprint: Option<[u8; 32]>,
        snapshot_frontier: Option<FrontierPosition>,
        role: ColumnarProjectionGenerationRoleV1,
    ) -> Result<Self, ColumnarControlError> {
        if history_incarnation < HISTORY_INCARNATION_INITIAL
            || (layout == ColumnarProjectionLayoutV1::V1)
                != physical_generation_fingerprint.is_none()
        {
            return Err(ColumnarControlError);
        }
        match role {
            ColumnarProjectionGenerationRoleV1::Candidate => match (artifact, snapshot_frontier) {
                (None, None) if frontier == FrontierPosition::BeforeFirst => {}
                (Some(_), Some(snapshot)) if snapshot <= frontier => {}
                _ => return Err(ColumnarControlError),
            },
            ColumnarProjectionGenerationRoleV1::Published
            | ColumnarProjectionGenerationRoleV1::Predecessor => {
                if artifact.is_none() || snapshot_frontier.is_some() {
                    return Err(ColumnarControlError);
                }
            }
        }
        Ok(Self {
            generation,
            layout,
            frontier,
            history_incarnation,
            artifact,
            definition_fingerprint,
            spec_hash,
            physical_generation_fingerprint,
            snapshot_frontier,
            role,
        })
    }

    /// Creates an unprepared Candidate at BeforeFirst.
    pub fn unprepared_candidate(
        generation: ProjectionGeneration,
        layout: ColumnarProjectionLayoutV1,
        history_incarnation: u64,
        definition_fingerprint: DefinitionFingerprint,
        spec_hash: ColumnarProjectionSpecHashV1,
        physical_generation_fingerprint: Option<[u8; 32]>,
    ) -> Result<Self, ColumnarControlError> {
        Self::new(
            generation,
            layout,
            FrontierPosition::BeforeFirst,
            history_incarnation,
            None,
            definition_fingerprint,
            spec_hash,
            physical_generation_fingerprint,
            None,
            ColumnarProjectionGenerationRoleV1::Candidate,
        )
    }

    /// Creates a fully prepared Candidate with its durable snapshot selector.
    #[allow(clippy::too_many_arguments)]
    pub fn prepared_candidate(
        generation: ProjectionGeneration,
        layout: ColumnarProjectionLayoutV1,
        snapshot_frontier: FrontierPosition,
        frontier: FrontierPosition,
        history_incarnation: u64,
        artifact: ColumnarProjectionArtifactV1,
        definition_fingerprint: DefinitionFingerprint,
        spec_hash: ColumnarProjectionSpecHashV1,
        physical_generation_fingerprint: Option<[u8; 32]>,
    ) -> Result<Self, ColumnarControlError> {
        Self::new(
            generation,
            layout,
            frontier,
            history_incarnation,
            Some(artifact),
            definition_fingerprint,
            spec_hash,
            physical_generation_fingerprint,
            Some(snapshot_frontier),
            ColumnarProjectionGenerationRoleV1::Candidate,
        )
    }

    /// Creates a Published or Predecessor pointer with complete artifact evidence.
    #[allow(clippy::too_many_arguments)]
    pub fn selected(
        generation: ProjectionGeneration,
        layout: ColumnarProjectionLayoutV1,
        frontier: FrontierPosition,
        history_incarnation: u64,
        artifact: ColumnarProjectionArtifactV1,
        definition_fingerprint: DefinitionFingerprint,
        spec_hash: ColumnarProjectionSpecHashV1,
        physical_generation_fingerprint: Option<[u8; 32]>,
        role: ColumnarProjectionGenerationRoleV1,
    ) -> Result<Self, ColumnarControlError> {
        if role == ColumnarProjectionGenerationRoleV1::Candidate {
            return Err(ColumnarControlError);
        }
        Self::new(
            generation,
            layout,
            frontier,
            history_incarnation,
            Some(artifact),
            definition_fingerprint,
            spec_hash,
            physical_generation_fingerprint,
            None,
            role,
        )
    }

    /// Never-reused generation.
    #[must_use]
    pub const fn generation(&self) -> ProjectionGeneration {
        self.generation
    }

    /// Physical layout.
    #[must_use]
    pub const fn layout(&self) -> ColumnarProjectionLayoutV1 {
        self.layout
    }

    /// Complete applied frontier selected by this pointer.
    #[must_use]
    pub const fn frontier(&self) -> FrontierPosition {
        self.frontier
    }

    /// Matching authoritative history incarnation.
    #[must_use]
    pub const fn history_incarnation(&self) -> u64 {
        self.history_incarnation
    }

    /// Complete immutable artifact evidence, absent only for Unprepared Candidate.
    #[must_use]
    pub const fn artifact(&self) -> Option<ColumnarProjectionArtifactV1> {
        self.artifact
    }

    /// Accepted definition fingerprint.
    #[must_use]
    pub const fn definition_fingerprint(&self) -> DefinitionFingerprint {
        self.definition_fingerprint
    }

    /// Complete target spec hash.
    #[must_use]
    pub const fn spec_hash(&self) -> ColumnarProjectionSpecHashV1 {
        self.spec_hash
    }

    /// V2-only physical-generation fingerprint.
    #[must_use]
    pub const fn physical_generation_fingerprint(&self) -> Option<[u8; 32]> {
        self.physical_generation_fingerprint
    }

    /// Captured snapshot, present only for Prepared Candidate.
    #[must_use]
    pub const fn snapshot_frontier(&self) -> Option<FrontierPosition> {
        self.snapshot_frontier
    }

    /// Exact pointer role.
    #[must_use]
    pub const fn role(&self) -> ColumnarProjectionGenerationRoleV1 {
        self.role
    }

    /// Effective retention frontier of a Candidate.
    #[must_use]
    pub fn candidate_effective_frontier(&self) -> Option<FrontierPosition> {
        if self.role != ColumnarProjectionGenerationRoleV1::Candidate {
            return None;
        }
        Some(if self.artifact.is_some() {
            self.frontier
        } else {
            FrontierPosition::BeforeFirst
        })
    }

    fn with_role(
        &self,
        role: ColumnarProjectionGenerationRoleV1,
    ) -> Result<Self, ColumnarControlError> {
        let artifact = self.artifact.ok_or(ColumnarControlError)?;
        Self::selected(
            self.generation,
            self.layout,
            self.frontier,
            self.history_incarnation,
            artifact,
            self.definition_fingerprint,
            self.spec_hash,
            self.physical_generation_fingerprint,
            role,
        )
    }
}

/// Exact durable failure and its matching pointer identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StoredColumnarProjectionFailureV1 {
    target: ColumnarProjectionFailureTargetV1,
    reason: ColumnarProjectionFailureReasonV1,
    generation: Option<ProjectionGeneration>,
}

impl StoredColumnarProjectionFailureV1 {
    /// Constructs a failure with exact target/generation presence.
    pub fn new(
        target: ColumnarProjectionFailureTargetV1,
        reason: ColumnarProjectionFailureReasonV1,
        generation: Option<ProjectionGeneration>,
    ) -> Result<Self, ColumnarControlError> {
        if (target == ColumnarProjectionFailureTargetV1::Control) != generation.is_none() {
            return Err(ColumnarControlError);
        }
        Ok(Self {
            target,
            reason,
            generation,
        })
    }

    /// Targeted pointer or control.
    #[must_use]
    pub const fn target(self) -> ColumnarProjectionFailureTargetV1 {
        self.target
    }

    /// Closed failure reason.
    #[must_use]
    pub const fn reason(self) -> ColumnarProjectionFailureReasonV1 {
        self.reason
    }

    /// Matching pointer generation; absent exactly for Control.
    #[must_use]
    pub const fn generation(self) -> Option<ProjectionGeneration> {
        self.generation
    }
}

/// One exact bounded schema-bound columnar control.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredColumnarProjectionControlV1 {
    source: ColumnarProjectionSourceV1,
    target_definition_fingerprint: DefinitionFingerprint,
    target_spec_hash: ColumnarProjectionSpecHashV1,
    highest_generation: ProjectionGeneration,
    published: Option<StoredColumnarProjectionGenerationV1>,
    candidate: Option<StoredColumnarProjectionGenerationV1>,
    predecessor: Option<StoredColumnarProjectionGenerationV1>,
    lifecycle: ColumnarProjectionLifecycleV1,
    failure: Option<StoredColumnarProjectionFailureV1>,
    replay_limits: ColumnarProjectionReplayLimitsV1,
}

impl StoredColumnarProjectionControlV1 {
    /// Creates the sole valid absent-control initialization: fresh Unprepared V1.
    pub fn initialize_fresh_v1(
        source: ColumnarProjectionSourceV1,
        target_definition_fingerprint: DefinitionFingerprint,
        target_spec_hash: ColumnarProjectionSpecHashV1,
        replay_limits: ColumnarProjectionReplayLimitsV1,
        history_incarnation: u64,
    ) -> Result<Self, ColumnarControlError> {
        let candidate = StoredColumnarProjectionGenerationV1::unprepared_candidate(
            ProjectionGeneration::first(),
            ColumnarProjectionLayoutV1::V1,
            history_incarnation,
            target_definition_fingerprint,
            target_spec_hash,
            None,
        )?;
        Self::new(
            source,
            target_definition_fingerprint,
            target_spec_hash,
            ProjectionGeneration::first(),
            None,
            Some(candidate),
            None,
            ColumnarProjectionLifecycleV1::Building,
            None,
            replay_limits,
        )
    }

    /// Replaces one uniformly stale control with the sole fresh-current-history shape.
    ///
    /// The caller cannot choose this value across the durable boundary: repository
    /// implementations obtain `current_history_incarnation` from authoritative
    /// metadata inside the same transaction that compares this complete control.
    pub fn reset_for_current_history_incarnation(
        self,
        current_history_incarnation: u64,
    ) -> Result<Self, ColumnarControlError> {
        let mut pointer_incarnation = None;
        for pointer in [
            self.published.as_ref(),
            self.candidate.as_ref(),
            self.predecessor.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            if pointer.history_incarnation() == 0
                || pointer_incarnation.is_some_and(|value| value != pointer.history_incarnation())
            {
                return Err(ColumnarControlError);
            }
            pointer_incarnation = Some(pointer.history_incarnation());
        }
        let stale_incarnation = pointer_incarnation.ok_or(ColumnarControlError)?;
        if current_history_incarnation == 0 || stale_incarnation >= current_history_incarnation {
            return Err(ColumnarControlError);
        }
        let next = self
            .highest_generation
            .checked_next()
            .ok_or(ColumnarControlError)?;
        let candidate = StoredColumnarProjectionGenerationV1::unprepared_candidate(
            next,
            ColumnarProjectionLayoutV1::V1,
            current_history_incarnation,
            self.target_definition_fingerprint,
            self.target_spec_hash,
            None,
        )?;
        Self::new(
            self.source,
            self.target_definition_fingerprint,
            self.target_spec_hash,
            next,
            None,
            Some(candidate),
            None,
            ColumnarProjectionLifecycleV1::Building,
            None,
            self.replay_limits,
        )
    }

    /// Reconstructs and validates one complete durable Protobuf state.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source: ColumnarProjectionSourceV1,
        target_definition_fingerprint: DefinitionFingerprint,
        target_spec_hash: ColumnarProjectionSpecHashV1,
        highest_generation: ProjectionGeneration,
        published: Option<StoredColumnarProjectionGenerationV1>,
        candidate: Option<StoredColumnarProjectionGenerationV1>,
        predecessor: Option<StoredColumnarProjectionGenerationV1>,
        lifecycle: ColumnarProjectionLifecycleV1,
        failure: Option<StoredColumnarProjectionFailureV1>,
        replay_limits: ColumnarProjectionReplayLimitsV1,
    ) -> Result<Self, ColumnarControlError> {
        let control = Self {
            source,
            target_definition_fingerprint,
            target_spec_hash,
            highest_generation,
            published,
            candidate,
            predecessor,
            lifecycle,
            failure,
            replay_limits,
        };
        control.validate()?;
        Ok(control)
    }

    fn validate(&self) -> Result<(), ColumnarControlError> {
        if matches!(
            &self.source,
            ColumnarProjectionSourceV1::Scalar {
                definition_fingerprint,
                ..
            } if *definition_fingerprint != self.target_definition_fingerprint
        ) {
            return Err(ColumnarControlError);
        }
        let mut seen_generation = None;
        let mut history_incarnation = None;
        for pointer in [
            self.published.as_ref(),
            self.candidate.as_ref(),
            self.predecessor.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            if pointer.generation() > self.highest_generation
                || seen_generation.is_some_and(|generation| generation == pointer.generation())
                || history_incarnation
                    .is_some_and(|incarnation| incarnation != pointer.history_incarnation())
            {
                return Err(ColumnarControlError);
            }
            seen_generation = Some(pointer.generation());
            history_incarnation = Some(pointer.history_incarnation());
        }
        if self.candidate.as_ref().is_some_and(|candidate| {
            candidate.generation() != self.highest_generation
                || candidate.definition_fingerprint() != self.target_definition_fingerprint
                || candidate.spec_hash() != self.target_spec_hash
        }) || self.published.as_ref().is_some_and(|published| {
            published.definition_fingerprint() != self.target_definition_fingerprint
                || published.spec_hash() != self.target_spec_hash
        }) {
            return Err(ColumnarControlError);
        }
        if self
            .published
            .as_ref()
            .is_some_and(|pointer| pointer.role() != ColumnarProjectionGenerationRoleV1::Published)
            || self.candidate.as_ref().is_some_and(|pointer| {
                pointer.role() != ColumnarProjectionGenerationRoleV1::Candidate
            })
            || self.predecessor.as_ref().is_some_and(|pointer| {
                pointer.role() != ColumnarProjectionGenerationRoleV1::Predecessor
            })
        {
            return Err(ColumnarControlError);
        }
        let pointer_count = usize::from(self.published.is_some())
            + usize::from(self.candidate.is_some())
            + usize::from(self.predecessor.is_some());
        if pointer_count > 2 || (self.published.is_some() && self.predecessor.is_some()) {
            return Err(ColumnarControlError);
        }
        match self.lifecycle {
            ColumnarProjectionLifecycleV1::Building => {
                if self.published.is_some()
                    || self.predecessor.is_some()
                    || self.failure.is_some()
                    || self.candidate.as_ref().is_none_or(|candidate| {
                        candidate.layout() != ColumnarProjectionLayoutV1::V1
                    })
                {
                    return Err(ColumnarControlError);
                }
            }
            ColumnarProjectionLifecycleV1::CatchingUp => {
                if self.failure.is_some()
                    || self.predecessor.is_some()
                    || !matches!(
                        (self.published.as_ref(), self.candidate.as_ref()),
                        (Some(published), Some(candidate))
                            if published.layout() == ColumnarProjectionLayoutV1::V1
                                && candidate.layout() == ColumnarProjectionLayoutV1::V2
                    )
                {
                    return Err(ColumnarControlError);
                }
            }
            ColumnarProjectionLifecycleV1::Ready => {
                if self.published.is_none()
                    || self.candidate.is_some()
                    || self.predecessor.is_some()
                    || self.failure.is_some()
                {
                    return Err(ColumnarControlError);
                }
            }
            ColumnarProjectionLifecycleV1::Rebuilding => {
                let same_spec_v2 = matches!(
                    (self.published.as_ref(), self.candidate.as_ref(), self.predecessor.as_ref()),
                    (Some(published), Some(candidate), None)
                        if published.layout() == ColumnarProjectionLayoutV1::V2
                            && candidate.layout() == ColumnarProjectionLayoutV1::V2
                );
                let unservable = self.published.is_none()
                    && self.predecessor.is_some()
                    && self.candidate.is_some();
                if self.failure.is_some() || (!same_spec_v2 && !unservable) {
                    return Err(ColumnarControlError);
                }
            }
            ColumnarProjectionLifecycleV1::Degraded => self.validate_degraded()?,
            ColumnarProjectionLifecycleV1::Invalid => {
                if pointer_count != 0
                    || !matches!(
                        self.failure,
                        Some(failure)
                            if failure.target() == ColumnarProjectionFailureTargetV1::Control
                                && failure.generation().is_none()
                    )
                {
                    return Err(ColumnarControlError);
                }
            }
        }
        Ok(())
    }

    fn validate_degraded(&self) -> Result<(), ColumnarControlError> {
        let failure = self.failure.ok_or(ColumnarControlError)?;
        match failure.target() {
            ColumnarProjectionFailureTargetV1::Candidate => {
                let candidate = self.candidate.as_ref().ok_or(ColumnarControlError)?;
                if failure.generation() != Some(candidate.generation())
                    || (self.published.is_none()
                        && self.predecessor.is_none()
                        && candidate.layout() != ColumnarProjectionLayoutV1::V1)
                {
                    return Err(ColumnarControlError);
                }
            }
            ColumnarProjectionFailureTargetV1::Published => {
                let published = self.published.as_ref().ok_or(ColumnarControlError)?;
                if failure.generation() != Some(published.generation())
                    || failure.reason() != ColumnarProjectionFailureReasonV1::ArtifactInvalid
                    || self.candidate.is_some()
                    || self.predecessor.is_some()
                {
                    return Err(ColumnarControlError);
                }
            }
            ColumnarProjectionFailureTargetV1::Predecessor
            | ColumnarProjectionFailureTargetV1::Control => return Err(ColumnarControlError),
        }
        Ok(())
    }

    /// Installs the first complete immutable artifact for an Unprepared Candidate.
    pub fn record_durable_snapshot(
        mut self,
        prepared: StoredColumnarProjectionGenerationV1,
    ) -> Result<Self, ColumnarControlError> {
        let current = self.candidate.as_ref().ok_or(ColumnarControlError)?;
        if !matches!(
            self.lifecycle,
            ColumnarProjectionLifecycleV1::Building
                | ColumnarProjectionLifecycleV1::CatchingUp
                | ColumnarProjectionLifecycleV1::Rebuilding
        ) || current.artifact().is_some()
            || prepared.artifact().is_none()
            || prepared.role() != ColumnarProjectionGenerationRoleV1::Candidate
            || !same_generation_identity(current, &prepared)
        {
            return Err(ColumnarControlError);
        }
        self.candidate = Some(prepared);
        self.validate()?;
        Ok(self)
    }

    /// Replaces a Prepared V1 Candidate with a strictly newer complete manifest.
    pub fn record_candidate_frontier(
        mut self,
        replacement: StoredColumnarProjectionGenerationV1,
        transaction_current_head: FrontierPosition,
    ) -> Result<Self, ColumnarControlError> {
        let current = self.candidate.as_ref().ok_or(ColumnarControlError)?;
        if !matches!(
            self.lifecycle,
            ColumnarProjectionLifecycleV1::Building
                | ColumnarProjectionLifecycleV1::CatchingUp
                | ColumnarProjectionLifecycleV1::Rebuilding
        ) || current.layout() != ColumnarProjectionLayoutV1::V1
            || current.artifact().is_none()
            || replacement.layout() != ColumnarProjectionLayoutV1::V1
            || replacement.artifact().is_none()
            || replacement.snapshot_frontier() != current.snapshot_frontier()
            || !same_generation_identity(current, &replacement)
            || !frontier_lt(current.frontier(), replacement.frontier())
            || !frontier_le(replacement.frontier(), transaction_current_head)
        {
            return Err(ColumnarControlError);
        }
        self.candidate = Some(replacement);
        self.validate()?;
        Ok(self)
    }

    /// Publishes a complete candidate at its validated frontier, no greater than head.
    /// Same-specification replacements must advance the selected frontier strictly.
    pub fn publish_prepared_generation(
        self,
        transaction_current_head: FrontierPosition,
    ) -> Result<Self, ColumnarControlError> {
        let candidate = self.candidate.as_ref().ok_or(ColumnarControlError)?;
        if !matches!(
            self.lifecycle,
            ColumnarProjectionLifecycleV1::Building
                | ColumnarProjectionLifecycleV1::CatchingUp
                | ColumnarProjectionLifecycleV1::Rebuilding
        ) || self.failure.is_some()
            || candidate.artifact().is_none()
            || !frontier_le(candidate.frontier(), transaction_current_head)
            || self
                .published
                .as_ref()
                .is_some_and(|published| !frontier_lt(published.frontier(), candidate.frontier()))
            || self.predecessor.as_ref().is_some_and(|predecessor| {
                !frontier_le(predecessor.frontier(), candidate.frontier())
            })
        {
            return Err(ColumnarControlError);
        }
        let published = candidate.with_role(ColumnarProjectionGenerationRoleV1::Published)?;
        Self::new(
            self.source,
            self.target_definition_fingerprint,
            self.target_spec_hash,
            self.highest_generation,
            Some(published),
            None,
            None,
            ColumnarProjectionLifecycleV1::Ready,
            None,
            self.replay_limits,
        )
    }

    /// Allocates the first disjoint V2 Candidate while a healthy V1 remains published.
    pub fn begin_v2_candidate(
        self,
        physical_generation_fingerprint: [u8; 32],
    ) -> Result<Self, ColumnarControlError> {
        let published = self.published.as_ref().ok_or(ColumnarControlError)?;
        if self.lifecycle != ColumnarProjectionLifecycleV1::Ready
            || published.layout() != ColumnarProjectionLayoutV1::V1
        {
            return Err(ColumnarControlError);
        }
        self.allocate_servable_candidate(
            ColumnarProjectionLayoutV1::V2,
            Some(physical_generation_fingerprint),
        )
    }

    /// Replaces a same-spec Candidate while retaining the Published pointer byte-exact.
    pub fn allocate_same_spec_candidate(
        self,
        physical_generation_fingerprint: [u8; 32],
    ) -> Result<Self, ColumnarControlError> {
        let published = self.published.as_ref().ok_or(ColumnarControlError)?;
        let accepted_shape = match self.lifecycle {
            ColumnarProjectionLifecycleV1::Ready => {
                published.layout() == ColumnarProjectionLayoutV1::V2
            }
            ColumnarProjectionLifecycleV1::CatchingUp
            | ColumnarProjectionLifecycleV1::Rebuilding => self.candidate.is_some(),
            ColumnarProjectionLifecycleV1::Degraded => matches!(
                self.failure,
                Some(failure)
                    if failure.target() == ColumnarProjectionFailureTargetV1::Candidate
            ),
            ColumnarProjectionLifecycleV1::Building | ColumnarProjectionLifecycleV1::Invalid => {
                false
            }
        };
        if !accepted_shape || self.predecessor.is_some() {
            return Err(ColumnarControlError);
        }
        self.allocate_servable_candidate(
            ColumnarProjectionLayoutV1::V2,
            Some(physical_generation_fingerprint),
        )
    }

    fn allocate_servable_candidate(
        self,
        layout: ColumnarProjectionLayoutV1,
        physical_generation_fingerprint: Option<[u8; 32]>,
    ) -> Result<Self, ColumnarControlError> {
        let published = self.published.clone().ok_or(ColumnarControlError)?;
        let next = self
            .highest_generation
            .checked_next()
            .ok_or(ColumnarControlError)?;
        let candidate = StoredColumnarProjectionGenerationV1::unprepared_candidate(
            next,
            layout,
            published.history_incarnation(),
            self.target_definition_fingerprint,
            self.target_spec_hash,
            physical_generation_fingerprint,
        )?;
        let lifecycle = if published.layout() == ColumnarProjectionLayoutV1::V1 {
            ColumnarProjectionLifecycleV1::CatchingUp
        } else {
            ColumnarProjectionLifecycleV1::Rebuilding
        };
        Self::new(
            self.source,
            self.target_definition_fingerprint,
            self.target_spec_hash,
            next,
            Some(published),
            Some(candidate),
            None,
            lifecycle,
            None,
            self.replay_limits,
        )
    }

    /// Moves a selected artifact to unservable Predecessor and allocates a fresh Candidate.
    #[allow(clippy::too_many_arguments)]
    pub fn allocate_unservable_rebuild_candidate(
        self,
        target_definition_fingerprint: DefinitionFingerprint,
        target_spec_hash: ColumnarProjectionSpecHashV1,
        replay_limits: ColumnarProjectionReplayLimitsV1,
        layout: ColumnarProjectionLayoutV1,
        physical_generation_fingerprint: Option<[u8; 32]>,
    ) -> Result<Self, ColumnarControlError> {
        let selected = match (self.published.as_ref(), self.predecessor.as_ref()) {
            (Some(published), None)
                if target_spec_hash != self.target_spec_hash
                    || matches!(
                        self.failure,
                        Some(failure)
                            if self.lifecycle == ColumnarProjectionLifecycleV1::Degraded
                                && failure.target()
                                    == ColumnarProjectionFailureTargetV1::Published
                                && failure.reason()
                                    == ColumnarProjectionFailureReasonV1::ArtifactInvalid
                    ) =>
            {
                published
            }
            (None, Some(predecessor)) => predecessor,
            _ => return Err(ColumnarControlError),
        };
        let predecessor = selected.with_role(ColumnarProjectionGenerationRoleV1::Predecessor)?;
        let next = self
            .highest_generation
            .checked_next()
            .ok_or(ColumnarControlError)?;
        let candidate = StoredColumnarProjectionGenerationV1::unprepared_candidate(
            next,
            layout,
            selected.history_incarnation(),
            target_definition_fingerprint,
            target_spec_hash,
            physical_generation_fingerprint,
        )?;
        Self::new(
            self.source,
            target_definition_fingerprint,
            target_spec_hash,
            next,
            None,
            Some(candidate),
            Some(predecessor),
            ColumnarProjectionLifecycleV1::Rebuilding,
            None,
            replay_limits,
        )
    }

    /// Records one Candidate failure without changing its Published or Predecessor pointer.
    pub fn record_candidate_failure(
        self,
        reason: ColumnarProjectionFailureReasonV1,
    ) -> Result<Self, ColumnarControlError> {
        if !matches!(
            self.lifecycle,
            ColumnarProjectionLifecycleV1::Building
                | ColumnarProjectionLifecycleV1::CatchingUp
                | ColumnarProjectionLifecycleV1::Rebuilding
        ) {
            return Err(ColumnarControlError);
        }
        let candidate = self.candidate.as_ref().ok_or(ColumnarControlError)?;
        let failure = StoredColumnarProjectionFailureV1::new(
            ColumnarProjectionFailureTargetV1::Candidate,
            reason,
            Some(candidate.generation()),
        )?;
        Self::new(
            self.source,
            self.target_definition_fingerprint,
            self.target_spec_hash,
            self.highest_generation,
            self.published,
            self.candidate,
            self.predecessor,
            ColumnarProjectionLifecycleV1::Degraded,
            Some(failure),
            self.replay_limits,
        )
    }

    /// Records selected-artifact corruption and detaches any Candidate.
    pub fn record_published_failure(self) -> Result<Self, ColumnarControlError> {
        let published = self.published.as_ref().ok_or(ColumnarControlError)?;
        let failure = StoredColumnarProjectionFailureV1::new(
            ColumnarProjectionFailureTargetV1::Published,
            ColumnarProjectionFailureReasonV1::ArtifactInvalid,
            Some(published.generation()),
        )?;
        Self::new(
            self.source,
            self.target_definition_fingerprint,
            self.target_spec_hash,
            self.highest_generation,
            self.published,
            None,
            None,
            ColumnarProjectionLifecycleV1::Degraded,
            Some(failure),
            self.replay_limits,
        )
    }

    /// Replaces a failed Candidate and returns to its exact predecessor family.
    pub fn replace_failed_candidate(
        self,
        layout: ColumnarProjectionLayoutV1,
        physical_generation_fingerprint: Option<[u8; 32]>,
    ) -> Result<Self, ColumnarControlError> {
        if self.lifecycle != ColumnarProjectionLifecycleV1::Degraded
            || self.failure.is_none_or(|failure| {
                failure.target() != ColumnarProjectionFailureTargetV1::Candidate
            })
        {
            return Err(ColumnarControlError);
        }
        let next = self
            .highest_generation
            .checked_next()
            .ok_or(ColumnarControlError)?;
        let incarnation = self
            .candidate
            .as_ref()
            .ok_or(ColumnarControlError)?
            .history_incarnation();
        let candidate = StoredColumnarProjectionGenerationV1::unprepared_candidate(
            next,
            layout,
            incarnation,
            self.target_definition_fingerprint,
            self.target_spec_hash,
            physical_generation_fingerprint,
        )?;
        let lifecycle = match (self.published.as_ref(), self.predecessor.as_ref()) {
            (None, None) if layout == ColumnarProjectionLayoutV1::V1 => {
                ColumnarProjectionLifecycleV1::Building
            }
            (Some(published), None)
                if published.layout() == ColumnarProjectionLayoutV1::V1
                    && layout == ColumnarProjectionLayoutV1::V2 =>
            {
                ColumnarProjectionLifecycleV1::CatchingUp
            }
            (Some(published), None)
                if published.layout() == ColumnarProjectionLayoutV1::V2
                    && layout == ColumnarProjectionLayoutV1::V2 =>
            {
                ColumnarProjectionLifecycleV1::Rebuilding
            }
            (None, Some(_)) => ColumnarProjectionLifecycleV1::Rebuilding,
            _ => return Err(ColumnarControlError),
        };
        Self::new(
            self.source,
            self.target_definition_fingerprint,
            self.target_spec_hash,
            next,
            self.published,
            Some(candidate),
            self.predecessor,
            lifecycle,
            None,
            self.replay_limits,
        )
    }

    /// Retargets only an unpublished initial V1 Candidate after semantic drift.
    pub fn retarget_initial_candidate(
        self,
        target_definition_fingerprint: DefinitionFingerprint,
        target_spec_hash: ColumnarProjectionSpecHashV1,
        replay_limits: ColumnarProjectionReplayLimitsV1,
    ) -> Result<Self, ColumnarControlError> {
        if !matches!(
            self.lifecycle,
            ColumnarProjectionLifecycleV1::Building | ColumnarProjectionLifecycleV1::Degraded
        ) || self.published.is_some()
            || self.predecessor.is_some()
            || (target_definition_fingerprint == self.target_definition_fingerprint
                && target_spec_hash == self.target_spec_hash)
        {
            return Err(ColumnarControlError);
        }
        if self.lifecycle == ColumnarProjectionLifecycleV1::Degraded
            && self.failure.is_none_or(|failure| {
                failure.target() != ColumnarProjectionFailureTargetV1::Candidate
            })
        {
            return Err(ColumnarControlError);
        }
        let current = self.candidate.as_ref().ok_or(ColumnarControlError)?;
        if current.layout() != ColumnarProjectionLayoutV1::V1 {
            return Err(ColumnarControlError);
        }
        let next = self
            .highest_generation
            .checked_next()
            .ok_or(ColumnarControlError)?;
        let candidate = StoredColumnarProjectionGenerationV1::unprepared_candidate(
            next,
            ColumnarProjectionLayoutV1::V1,
            current.history_incarnation(),
            target_definition_fingerprint,
            target_spec_hash,
            None,
        )?;
        Self::new(
            self.source,
            target_definition_fingerprint,
            target_spec_hash,
            next,
            None,
            Some(candidate),
            None,
            ColumnarProjectionLifecycleV1::Building,
            None,
            replay_limits,
        )
    }

    /// Advances only a Published V1 artifact, preserving an independent V2 Candidate.
    pub fn advance_published_v1(
        self,
        replacement: StoredColumnarProjectionGenerationV1,
        transaction_current_head: FrontierPosition,
    ) -> Result<Self, ColumnarControlError> {
        let current = self.published.as_ref().ok_or(ColumnarControlError)?;
        if !matches!(
            self.lifecycle,
            ColumnarProjectionLifecycleV1::Ready | ColumnarProjectionLifecycleV1::CatchingUp
        ) || current.layout() != ColumnarProjectionLayoutV1::V1
            || replacement.layout() != ColumnarProjectionLayoutV1::V1
            || replacement.artifact().is_none()
            || !same_generation_identity(current, &replacement)
            || !frontier_lt(current.frontier(), replacement.frontier())
            || !frontier_le(replacement.frontier(), transaction_current_head)
        {
            return Err(ColumnarControlError);
        }
        let published = replacement.with_role(ColumnarProjectionGenerationRoleV1::Published)?;
        Self::new(
            self.source,
            self.target_definition_fingerprint,
            self.target_spec_hash,
            self.highest_generation,
            Some(published),
            self.candidate,
            None,
            self.lifecycle,
            None,
            self.replay_limits,
        )
    }

    /// Closes all pointers under one control-target failure.
    pub fn mark_invalid(
        self,
        reason: ColumnarProjectionFailureReasonV1,
    ) -> Result<Self, ColumnarControlError> {
        let failure = StoredColumnarProjectionFailureV1::new(
            ColumnarProjectionFailureTargetV1::Control,
            reason,
            None,
        )?;
        Self::new(
            self.source,
            self.target_definition_fingerprint,
            self.target_spec_hash,
            self.highest_generation,
            None,
            None,
            None,
            ColumnarProjectionLifecycleV1::Invalid,
            Some(failure),
            self.replay_limits,
        )
    }

    /// Canonical schema-bound source key.
    #[must_use]
    pub const fn source(&self) -> &ColumnarProjectionSourceV1 {
        &self.source
    }

    /// Current target definition fingerprint.
    #[must_use]
    pub const fn target_definition_fingerprint(&self) -> DefinitionFingerprint {
        self.target_definition_fingerprint
    }

    /// Current complete specification hash.
    #[must_use]
    pub const fn target_spec_hash(&self) -> ColumnarProjectionSpecHashV1 {
        self.target_spec_hash
    }

    /// Highest allocated, never-reused generation.
    #[must_use]
    pub const fn highest_generation(&self) -> ProjectionGeneration {
        self.highest_generation
    }

    /// Published pointer, whether or not its current lifecycle is servable.
    #[must_use]
    pub const fn published(&self) -> Option<&StoredColumnarProjectionGenerationV1> {
        self.published.as_ref()
    }

    /// Candidate pointer.
    #[must_use]
    pub const fn candidate(&self) -> Option<&StoredColumnarProjectionGenerationV1> {
        self.candidate.as_ref()
    }

    /// Unservable predecessor pointer.
    #[must_use]
    pub const fn predecessor(&self) -> Option<&StoredColumnarProjectionGenerationV1> {
        self.predecessor.as_ref()
    }

    /// Closed lifecycle.
    #[must_use]
    pub const fn lifecycle(&self) -> ColumnarProjectionLifecycleV1 {
        self.lifecycle
    }

    /// Exact durable failure.
    #[must_use]
    pub const fn failure(&self) -> Option<StoredColumnarProjectionFailureV1> {
        self.failure
    }

    /// Compiler-sealed replay limits.
    #[must_use]
    pub const fn replay_limits(&self) -> ColumnarProjectionReplayLimitsV1 {
        self.replay_limits
    }

    /// The sole servable pointer in this exact lifecycle shape.
    #[must_use]
    pub const fn servable_generation(&self) -> Option<&StoredColumnarProjectionGenerationV1> {
        match (self.lifecycle, self.failure) {
            (ColumnarProjectionLifecycleV1::Ready, _)
            | (ColumnarProjectionLifecycleV1::CatchingUp, _)
            | (ColumnarProjectionLifecycleV1::Rebuilding, _) => self.published.as_ref(),
            (
                ColumnarProjectionLifecycleV1::Degraded,
                Some(StoredColumnarProjectionFailureV1 {
                    target: ColumnarProjectionFailureTargetV1::Candidate,
                    ..
                }),
            ) => self.published.as_ref(),
            _ => None,
        }
    }

    /// Exact per-source retention input, or none for a detached shape.
    #[must_use]
    pub fn retention_frontier(&self) -> Option<FrontierPosition> {
        match self.lifecycle {
            ColumnarProjectionLifecycleV1::Building => candidate_frontier(self.candidate.as_ref()),
            ColumnarProjectionLifecycleV1::Ready => pointer_frontier(self.published.as_ref()),
            ColumnarProjectionLifecycleV1::CatchingUp => min_frontier(
                pointer_frontier(self.published.as_ref()),
                candidate_frontier(self.candidate.as_ref()),
            ),
            ColumnarProjectionLifecycleV1::Rebuilding if self.published.is_some() => min_frontier(
                pointer_frontier(self.published.as_ref()),
                candidate_frontier(self.candidate.as_ref()),
            ),
            ColumnarProjectionLifecycleV1::Rebuilding => {
                candidate_frontier(self.candidate.as_ref())
            }
            ColumnarProjectionLifecycleV1::Degraded => match self.failure {
                Some(StoredColumnarProjectionFailureV1 {
                    target: ColumnarProjectionFailureTargetV1::Candidate,
                    ..
                }) if self.published.is_some() => pointer_frontier(self.published.as_ref()),
                _ => None,
            },
            ColumnarProjectionLifecycleV1::Invalid => None,
        }
    }
}

const fn pointer_frontier(
    pointer: Option<&StoredColumnarProjectionGenerationV1>,
) -> Option<FrontierPosition> {
    match pointer {
        Some(pointer) => Some(pointer.frontier()),
        None => None,
    }
}

fn candidate_frontier(
    pointer: Option<&StoredColumnarProjectionGenerationV1>,
) -> Option<FrontierPosition> {
    match pointer {
        Some(pointer) => pointer.candidate_effective_frontier(),
        None => None,
    }
}

fn min_frontier(
    left: Option<FrontierPosition>,
    right: Option<FrontierPosition>,
) -> Option<FrontierPosition> {
    match (left, right) {
        (Some(left), Some(right)) => Some(if frontier_le(left, right) {
            left
        } else {
            right
        }),
        _ => None,
    }
}

const fn frontier_le(left: FrontierPosition, right: FrontierPosition) -> bool {
    match (left, right) {
        (FrontierPosition::BeforeFirst, _) => true,
        (FrontierPosition::AppliedThrough(_), FrontierPosition::BeforeFirst) => false,
        (FrontierPosition::AppliedThrough(left), FrontierPosition::AppliedThrough(right)) => {
            left.get() <= right.get()
        }
    }
}

const fn frontier_lt(left: FrontierPosition, right: FrontierPosition) -> bool {
    match (left, right) {
        (FrontierPosition::BeforeFirst, FrontierPosition::AppliedThrough(_)) => true,
        (FrontierPosition::AppliedThrough(left), FrontierPosition::AppliedThrough(right)) => {
            left.get() < right.get()
        }
        _ => false,
    }
}

fn same_generation_identity(
    left: &StoredColumnarProjectionGenerationV1,
    right: &StoredColumnarProjectionGenerationV1,
) -> bool {
    left.generation() == right.generation()
        && left.layout() == right.layout()
        && left.history_incarnation() == right.history_incarnation()
        && left.definition_fingerprint() == right.definition_fingerprint()
        && left.spec_hash() == right.spec_hash()
        && left.physical_generation_fingerprint() == right.physical_generation_fingerprint()
}

/// Closed rejection of malformed or impossible columnar-control state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ColumnarControlError;

impl fmt::Display for ColumnarControlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("invalid schema-bound columnar control")
    }
}

impl std::error::Error for ColumnarControlError {}

/// Exact outcome of one specialized expected-control operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ColumnarProjectionControlWriteResultV1 {
    /// The requested transition became durable.
    Applied,
    /// The transaction-current row did not equal the complete expected control.
    StateChanged,
}

/// Fields-private proof of the sole legal absent-control insertion state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FreshColumnarProjectionControlV1(StoredColumnarProjectionControlV1);

impl FreshColumnarProjectionControlV1 {
    /// Validates and captures one fresh V1 control at its durable BeforeFirst fence.
    pub fn new(
        source: ColumnarProjectionSourceV1,
        target_definition_fingerprint: DefinitionFingerprint,
        target_spec_hash: ColumnarProjectionSpecHashV1,
        replay_limits: ColumnarProjectionReplayLimitsV1,
        history_incarnation: u64,
    ) -> Result<Self, ColumnarControlError> {
        StoredColumnarProjectionControlV1::initialize_fresh_v1(
            source,
            target_definition_fingerprint,
            target_spec_hash,
            replay_limits,
            history_incarnation,
        )
        .map(Self)
    }

    /// Returns the exact validated initial control for repository encoders.
    #[must_use]
    pub const fn control(&self) -> &StoredColumnarProjectionControlV1 {
        &self.0
    }
}

/// Sole specialized durable selector repository for schema-bound columnar state.
///
/// Every mutating operation is a closed named transition. Implementations must
/// compare the complete expected control and, where required, the authoritative
/// transaction-current application head in the same atomic transaction.
pub trait ColumnarProjectionControlRepository {
    /// Atomically inserts all supplied legal absent-control states.
    fn initialize_fresh_v1(
        &self,
        controls: &[FreshColumnarProjectionControlV1],
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError>;

    /// Resets one exact uniformly stale control using transaction-current history metadata.
    fn reset_for_current_history_incarnation(
        &self,
        expected: &StoredColumnarProjectionControlV1,
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError>;

    /// Allocates the first V2 Candidate beside healthy Published V1.
    fn begin_v2_candidate(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        physical_generation_fingerprint: [u8; 32],
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError>;

    /// Replaces or starts one same-spec V2 Candidate beside Published state.
    fn allocate_same_spec_candidate(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        physical_generation_fingerprint: [u8; 32],
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError>;

    /// Allocates a Candidate while retaining an explicitly unservable predecessor.
    #[allow(clippy::too_many_arguments)]
    fn allocate_unservable_rebuild_candidate(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        target_definition_fingerprint: DefinitionFingerprint,
        target_spec_hash: ColumnarProjectionSpecHashV1,
        replay_limits: ColumnarProjectionReplayLimitsV1,
        layout: ColumnarProjectionLayoutV1,
        physical_generation_fingerprint: Option<[u8; 32]>,
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError>;

    /// Suspends one exact Candidate failure without changing its other pointer.
    fn record_candidate_failure(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        reason: ColumnarProjectionFailureReasonV1,
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError>;

    /// Replaces only an exact failed initial, candidate, or unservable Candidate.
    fn replace_failed_candidate(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        layout: ColumnarProjectionLayoutV1,
        physical_generation_fingerprint: Option<[u8; 32]>,
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError>;

    /// Retargets only an exact unpublished initial V1 Candidate after drift.
    fn retarget_initial_candidate(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        target_definition_fingerprint: DefinitionFingerprint,
        target_spec_hash: ColumnarProjectionSpecHashV1,
        replay_limits: ColumnarProjectionReplayLimitsV1,
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError>;

    /// Records selected-artifact corruption and detaches any Candidate.
    fn record_published_failure(
        &self,
        expected: &StoredColumnarProjectionControlV1,
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError>;

    /// Rereads the complete durable result after mismatch or uncertainty.
    fn recover_expected_control(
        &self,
        source: &ColumnarProjectionSourceV1,
    ) -> Result<Option<StoredColumnarProjectionControlV1>, StorageError>;

    /// Closes all pointers under one exact Control-target failure.
    fn mark_invalid(
        &self,
        expected: &StoredColumnarProjectionControlV1,
        reason: ColumnarProjectionFailureReasonV1,
    ) -> Result<ColumnarProjectionControlWriteResultV1, StorageError>;
}

/// Bounded control-derived retention inputs, separate from mutation authority.
pub trait ColumnarProjectionRetentionRepository {
    /// Returns at most 256 exact per-source retention inputs in source order.
    fn columnar_projection_retention_frontiers(
        &self,
    ) -> Result<Vec<(ColumnarProjectionSourceV1, Option<FrontierPosition>)>, StorageError>;
}
