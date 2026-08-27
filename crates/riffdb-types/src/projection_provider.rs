//! Sealed compiler-owned projection-provider descriptor (ADR-0130).

use std::error::Error;
use std::fmt;
use std::num::NonZeroU32;
use std::ops::{BitOr, BitOrAssign};

use crate::{
    AggregateSemanticSetV1, ProjectionProviderDescriptorHash, hash_projection_provider_descriptor,
};

/// Canonical projection-provider descriptor identity.
pub const PROJECTION_PROVIDER_DESCRIPTOR_VERSION_V1: u16 = 1;
/// Exact fixed byte length of the V1 descriptor.
pub const PROJECTION_PROVIDER_DESCRIPTOR_V1_BYTES: usize = 112;

const MAGIC: [u8; 4] = *b"RPPD";
const ALL_CAPABILITY_BITS: u16 = ProjectionProviderCapabilitiesV1::CANDIDATE.0
    | ProjectionProviderCapabilitiesV1::FILTER.0
    | ProjectionProviderCapabilitiesV1::RANK.0
    | ProjectionProviderCapabilitiesV1::ORDER.0
    | ProjectionProviderCapabilitiesV1::MEASURE.0
    | ProjectionProviderCapabilitiesV1::FACET.0
    | ProjectionProviderCapabilitiesV1::WINDOW.0
    | ProjectionProviderCapabilitiesV1::OUTPUT.0;

/// Closed real provider families admitted by ADR-0130.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum ProjectionProviderKindV1 {
    /// Existing partition-scoped columnar engine.
    Columnar = 1,
    /// Existing per-organization exact/ANN vector engine.
    Vector = 2,
    /// Partition-scoped exact binary UTF-8 text engine.
    ExactText = 3,
}

/// Exact or explicitly bounded approximate result posture.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectionProviderPostureV1 {
    /// Exact reference-equivalent result.
    Exact,
    /// Approximate result meeting a compiler-owned recall target in basis points.
    Approximate {
        /// Minimum compiler-owned recall target in basis points.
        recall_target_bps: u16,
    },
}

impl ProjectionProviderPostureV1 {
    /// Constructs an approximate posture with a nonzero target at most 100%.
    pub const fn approximate(
        recall_target_bps: u16,
    ) -> Result<Self, ProjectionProviderValidationError> {
        if recall_target_bps == 0 || recall_target_bps > 10_000 {
            Err(ProjectionProviderValidationError::InvalidRecallTarget)
        } else {
            Ok(Self::Approximate { recall_target_bps })
        }
    }
}

/// Closed capabilities a compiler may require from one provider.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProjectionProviderCapabilitiesV1(u16);

impl ProjectionProviderCapabilitiesV1 {
    /// Bounded candidate production.
    pub const CANDIDATE: Self = Self(1 << 0);
    /// Predicate filtering before result shaping.
    pub const FILTER: Self = Self(1 << 1);
    /// Relevance or distance ranking.
    pub const RANK: Self = Self(1 << 2);
    /// Declared exact total ordering.
    pub const ORDER: Self = Self(1 << 3);
    /// Whole-admitted-set measures.
    pub const MEASURE: Self = Self(1 << 4);
    /// Whole-admitted-set facets.
    pub const FACET: Self = Self(1 << 5);
    /// Bounded top or ordinal windowing.
    pub const WINDOW: Self = Self(1 << 6);
    /// Typed result projection.
    pub const OUTPUT: Self = Self(1 << 7);

    /// Returns the stable bit representation.
    #[must_use]
    pub const fn bits(self) -> u16 {
        self.0
    }

    /// Whether all requested capabilities are present.
    #[must_use]
    pub const fn contains(self, required: Self) -> bool {
        self.0 & required.0 == required.0
    }
}

impl BitOr for ProjectionProviderCapabilitiesV1 {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self(self.0 | rhs.0)
    }
}

impl BitOrAssign for ProjectionProviderCapabilitiesV1 {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

/// Ranked policy modes, strongest alignment first.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum ProjectionProviderPolicyModeV1 {
    /// One provider partition exactly matches the authorized tenant partition.
    PartitionAligned = 1,
    /// Provider state is split by a compiler-proven policy subpartition.
    PolicySubpartition = 2,
    /// Every bounded candidate receives authoritative row admission before shaping.
    BoundedRowAdmission = 3,
}

/// Freshness semantics supported by descriptor V1.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum ProjectionProviderFreshnessV1 {
    /// Exact snapshots are retained over a declared servable interval.
    RetainedExactEpoch = 1,
}

/// Provider lifecycle semantics supported by descriptor V1.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum ProjectionProviderLifecycleModelV1 {
    /// Derived state uses never-reused rebuildable generations.
    RebuildableGeneration = 1,
}

/// Complete finite static cost and retention contract for one provider plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProjectionProviderStaticBoundsV1 {
    /// Maximum candidates admitted before shaping.
    pub max_candidates: u32,
    /// Maximum typed output rows.
    pub max_output_rows: u32,
    /// Maximum whole-result measures or facets.
    pub max_measures: u16,
    /// Maximum canonical input bytes.
    pub max_input_bytes: u32,
    /// Maximum provider-owned deterministic work units.
    pub max_work_units: u64,
    /// Maximum derived state bytes added per authoritative row.
    pub max_state_bytes_per_row: u32,
    /// Maximum safe diagnostic bytes.
    pub max_diagnostic_bytes: u32,
    /// Minimum retained provider epochs.
    pub retained_epochs: u64,
    /// Maximum admitted catch-up lag before health degrades.
    pub max_catchup_lag: u64,
    /// Maximum deterministic lease/schedule steps retaining an opened epoch.
    pub max_epoch_lease_steps: u64,
}

/// Immutable provider state schema identity, distinct from a live generation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProjectionProviderStateIdentityV1 {
    layout_version: NonZeroU32,
    schema_hash: [u8; 32],
}

impl ProjectionProviderStateIdentityV1 {
    /// Constructs an already-validated provider state identity.
    #[must_use]
    pub const fn new(layout_version: NonZeroU32, schema_hash: [u8; 32]) -> Self {
        Self {
            layout_version,
            schema_hash,
        }
    }

    /// Returns the never-zero layout version.
    #[must_use]
    pub const fn layout_version(self) -> NonZeroU32 {
        self.layout_version
    }

    /// Returns the provider-state schema digest.
    #[must_use]
    pub const fn schema_hash(self) -> [u8; 32] {
        self.schema_hash
    }
}

/// One fixed-size compiler-owned provider contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionProviderDescriptorV1 {
    kind: ProjectionProviderKindV1,
    posture: ProjectionProviderPostureV1,
    capabilities: ProjectionProviderCapabilitiesV1,
    policy_mode: ProjectionProviderPolicyModeV1,
    freshness: ProjectionProviderFreshnessV1,
    lifecycle: ProjectionProviderLifecycleModelV1,
    bounds: ProjectionProviderStaticBoundsV1,
    state: ProjectionProviderStateIdentityV1,
}

impl ProjectionProviderDescriptorV1 {
    /// Constructs and validates a sealed descriptor.
    pub fn new(
        kind: ProjectionProviderKindV1,
        posture: ProjectionProviderPostureV1,
        capabilities: ProjectionProviderCapabilitiesV1,
        policy_mode: ProjectionProviderPolicyModeV1,
        bounds: ProjectionProviderStaticBoundsV1,
        state: ProjectionProviderStateIdentityV1,
    ) -> Result<Self, ProjectionProviderValidationError> {
        let descriptor = Self {
            kind,
            posture,
            capabilities,
            policy_mode,
            freshness: ProjectionProviderFreshnessV1::RetainedExactEpoch,
            lifecycle: ProjectionProviderLifecycleModelV1::RebuildableGeneration,
            bounds,
            state,
        };
        descriptor.validate()?;
        Ok(descriptor)
    }

    /// Decodes and strictly validates one canonical V1 descriptor.
    pub fn from_canonical_bytes(bytes: &[u8]) -> Result<Self, ProjectionProviderValidationError> {
        if bytes.len() != PROJECTION_PROVIDER_DESCRIPTOR_V1_BYTES {
            return Err(ProjectionProviderValidationError::InvalidLength);
        }
        if bytes[..4] != MAGIC || u16::from_be_bytes([bytes[4], bytes[5]]) != 1 {
            return Err(ProjectionProviderValidationError::InvalidVersion);
        }
        if bytes[15] != 0 || bytes[26..28] != [0; 2] || bytes[108..112] != [0; 4] {
            return Err(ProjectionProviderValidationError::NonCanonicalReservedBytes);
        }
        let kind = match bytes[6] {
            1 => ProjectionProviderKindV1::Columnar,
            2 => ProjectionProviderKindV1::Vector,
            3 => ProjectionProviderKindV1::ExactText,
            _ => return Err(ProjectionProviderValidationError::UnknownProvider),
        };
        let recall = u16::from_be_bytes([bytes[8], bytes[9]]);
        let posture = match bytes[7] {
            1 if recall == 0 => ProjectionProviderPostureV1::Exact,
            2 => ProjectionProviderPostureV1::approximate(recall)?,
            _ => return Err(ProjectionProviderValidationError::InvalidPosture),
        };
        let capability_bits = u16::from_be_bytes([bytes[10], bytes[11]]);
        if capability_bits & !ALL_CAPABILITY_BITS != 0 {
            return Err(ProjectionProviderValidationError::UnknownCapability);
        }
        let capabilities = ProjectionProviderCapabilitiesV1(capability_bits);
        let policy_mode = match bytes[12] {
            1 => ProjectionProviderPolicyModeV1::PartitionAligned,
            2 => ProjectionProviderPolicyModeV1::PolicySubpartition,
            3 => ProjectionProviderPolicyModeV1::BoundedRowAdmission,
            _ => return Err(ProjectionProviderValidationError::UnknownPolicyMode),
        };
        if bytes[13] != ProjectionProviderFreshnessV1::RetainedExactEpoch as u8
            || bytes[14] != ProjectionProviderLifecycleModelV1::RebuildableGeneration as u8
        {
            return Err(ProjectionProviderValidationError::UnknownLifecycle);
        }
        let max_candidates = read_u32(bytes, 16);
        let max_output_rows = read_u32(bytes, 20);
        let max_measures = u16::from_be_bytes([bytes[24], bytes[25]]);
        let retained_epochs = read_u64(bytes, 28);
        let max_catchup_lag = read_u64(bytes, 36);
        let layout_version = NonZeroU32::new(read_u32(bytes, 44))
            .ok_or(ProjectionProviderValidationError::InvalidStateIdentity)?;
        let mut schema_hash = [0; 32];
        schema_hash.copy_from_slice(&bytes[48..80]);
        let bounds = ProjectionProviderStaticBoundsV1 {
            max_candidates,
            max_output_rows,
            max_measures,
            retained_epochs,
            max_catchup_lag,
            max_input_bytes: read_u32(bytes, 80),
            max_work_units: read_u64(bytes, 84),
            max_state_bytes_per_row: read_u32(bytes, 92),
            max_diagnostic_bytes: read_u32(bytes, 96),
            max_epoch_lease_steps: read_u64(bytes, 100),
        };
        Self::new(
            kind,
            posture,
            capabilities,
            policy_mode,
            bounds,
            ProjectionProviderStateIdentityV1::new(layout_version, schema_hash),
        )
    }

    /// Encodes the exact canonical fixed-size V1 form.
    #[must_use]
    pub fn to_canonical_bytes(&self) -> [u8; PROJECTION_PROVIDER_DESCRIPTOR_V1_BYTES] {
        let mut bytes = [0; PROJECTION_PROVIDER_DESCRIPTOR_V1_BYTES];
        bytes[..4].copy_from_slice(&MAGIC);
        bytes[4..6].copy_from_slice(&PROJECTION_PROVIDER_DESCRIPTOR_VERSION_V1.to_be_bytes());
        bytes[6] = self.kind as u8;
        let (posture, recall) = match self.posture {
            ProjectionProviderPostureV1::Exact => (1, 0),
            ProjectionProviderPostureV1::Approximate { recall_target_bps } => {
                (2, recall_target_bps)
            }
        };
        bytes[7] = posture;
        bytes[8..10].copy_from_slice(&recall.to_be_bytes());
        bytes[10..12].copy_from_slice(&self.capabilities.bits().to_be_bytes());
        bytes[12] = self.policy_mode as u8;
        bytes[13] = self.freshness as u8;
        bytes[14] = self.lifecycle as u8;
        bytes[16..20].copy_from_slice(&self.bounds.max_candidates.to_be_bytes());
        bytes[20..24].copy_from_slice(&self.bounds.max_output_rows.to_be_bytes());
        bytes[24..26].copy_from_slice(&self.bounds.max_measures.to_be_bytes());
        bytes[28..36].copy_from_slice(&self.bounds.retained_epochs.to_be_bytes());
        bytes[36..44].copy_from_slice(&self.bounds.max_catchup_lag.to_be_bytes());
        bytes[44..48].copy_from_slice(&self.state.layout_version().get().to_be_bytes());
        bytes[48..80].copy_from_slice(&self.state.schema_hash());
        bytes[80..84].copy_from_slice(&self.bounds.max_input_bytes.to_be_bytes());
        bytes[84..92].copy_from_slice(&self.bounds.max_work_units.to_be_bytes());
        bytes[92..96].copy_from_slice(&self.bounds.max_state_bytes_per_row.to_be_bytes());
        bytes[96..100].copy_from_slice(&self.bounds.max_diagnostic_bytes.to_be_bytes());
        bytes[100..108].copy_from_slice(&self.bounds.max_epoch_lease_steps.to_be_bytes());
        bytes
    }

    /// Returns the exact descriptor digest.
    #[must_use]
    pub fn digest(&self) -> ProjectionProviderDescriptorHash {
        hash_projection_provider_descriptor(&self.to_canonical_bytes())
    }

    /// Provider family pinned by the compiler.
    #[must_use]
    pub const fn kind(&self) -> ProjectionProviderKindV1 {
        self.kind
    }
    /// Exactness posture pinned by the compiler.
    #[must_use]
    pub const fn posture(&self) -> ProjectionProviderPostureV1 {
        self.posture
    }
    /// Closed supported capabilities.
    #[must_use]
    pub const fn capabilities(&self) -> ProjectionProviderCapabilitiesV1 {
        self.capabilities
    }
    /// Exact aggregate semantic subset implemented by this provider.
    ///
    /// A generic `MEASURE` stage is insufficient to prove a particular fold;
    /// this closed subset prevents a compiler from treating the union of real
    /// providers as capabilities of any one provider.
    #[must_use]
    pub const fn aggregate_semantics(&self) -> AggregateSemanticSetV1 {
        if !self
            .capabilities
            .contains(ProjectionProviderCapabilitiesV1::MEASURE)
        {
            return AggregateSemanticSetV1::NONE;
        }
        match self.kind {
            ProjectionProviderKindV1::Columnar => AggregateSemanticSetV1::BOUNDED_EXACT_CORE,
            ProjectionProviderKindV1::ExactText => AggregateSemanticSetV1::EXACT_COUNT_ONLY,
            ProjectionProviderKindV1::Vector => AggregateSemanticSetV1::NONE,
        }
    }
    /// Ranked authorization-shaping mode.
    #[must_use]
    pub const fn policy_mode(&self) -> ProjectionProviderPolicyModeV1 {
        self.policy_mode
    }
    /// Exact retained-epoch freshness model.
    #[must_use]
    pub const fn freshness(&self) -> ProjectionProviderFreshnessV1 {
        self.freshness
    }
    /// Rebuildable provider-generation lifecycle model.
    #[must_use]
    pub const fn lifecycle(&self) -> ProjectionProviderLifecycleModelV1 {
        self.lifecycle
    }
    /// Complete static cost and retention bounds.
    #[must_use]
    pub const fn static_bounds(&self) -> ProjectionProviderStaticBoundsV1 {
        self.bounds
    }
    /// Maximum candidates before any result shaping.
    #[must_use]
    pub const fn max_candidates(&self) -> u32 {
        self.bounds.max_candidates
    }
    /// Maximum result rows.
    #[must_use]
    pub const fn max_output_rows(&self) -> u32 {
        self.bounds.max_output_rows
    }
    /// Maximum whole-set measures/facets.
    #[must_use]
    pub const fn max_measures(&self) -> u16 {
        self.bounds.max_measures
    }
    /// Minimum retained epoch count promised by the provider.
    #[must_use]
    pub const fn retained_epochs(&self) -> u64 {
        self.bounds.retained_epochs
    }
    /// Maximum admitted catch-up lag before health degrades.
    #[must_use]
    pub const fn max_catchup_lag(&self) -> u64 {
        self.bounds.max_catchup_lag
    }
    /// Maximum deterministic epoch-lease lifetime retained for continuations.
    #[must_use]
    pub const fn max_epoch_lease_steps(&self) -> u64 {
        self.bounds.max_epoch_lease_steps
    }
    /// Immutable provider-state schema identity.
    #[must_use]
    pub const fn state_identity(&self) -> ProjectionProviderStateIdentityV1 {
        self.state
    }

    fn validate(&self) -> Result<(), ProjectionProviderValidationError> {
        let supported = match self.kind {
            ProjectionProviderKindV1::Columnar => {
                ALL_CAPABILITY_BITS & !ProjectionProviderCapabilitiesV1::RANK.bits()
            }
            ProjectionProviderKindV1::Vector => (ProjectionProviderCapabilitiesV1::CANDIDATE
                | ProjectionProviderCapabilitiesV1::FILTER
                | ProjectionProviderCapabilitiesV1::RANK
                | ProjectionProviderCapabilitiesV1::WINDOW
                | ProjectionProviderCapabilitiesV1::OUTPUT)
                .bits(),
            ProjectionProviderKindV1::ExactText => (ProjectionProviderCapabilitiesV1::CANDIDATE
                | ProjectionProviderCapabilitiesV1::FILTER
                | ProjectionProviderCapabilitiesV1::ORDER
                | ProjectionProviderCapabilitiesV1::MEASURE
                | ProjectionProviderCapabilitiesV1::WINDOW
                | ProjectionProviderCapabilitiesV1::OUTPUT)
                .bits(),
        };
        if self.capabilities.bits() & !supported != 0 {
            return Err(ProjectionProviderValidationError::UnsupportedCapability);
        }
        let required = ProjectionProviderCapabilitiesV1::CANDIDATE
            | ProjectionProviderCapabilitiesV1::WINDOW
            | ProjectionProviderCapabilitiesV1::OUTPUT;
        if !self.capabilities.contains(required) {
            return Err(ProjectionProviderValidationError::MissingRequiredCapability);
        }
        if self.bounds.max_candidates == 0
            || self.bounds.max_output_rows == 0
            || self.bounds.max_output_rows > self.bounds.max_candidates
            || self.bounds.max_input_bytes == 0
            || self.bounds.max_work_units == 0
            || self.bounds.max_state_bytes_per_row == 0
            || self.bounds.max_diagnostic_bytes == 0
            || self.bounds.retained_epochs == 0
            || self.bounds.max_catchup_lag == 0
            || self.bounds.max_epoch_lease_steps == 0
        {
            return Err(ProjectionProviderValidationError::InvalidBound);
        }
        if matches!(
            self.kind,
            ProjectionProviderKindV1::Columnar | ProjectionProviderKindV1::ExactText
        ) && !matches!(self.posture, ProjectionProviderPostureV1::Exact)
        {
            return Err(ProjectionProviderValidationError::InvalidPosture);
        }
        if self.bounds.max_measures == 0
            && (self
                .capabilities
                .contains(ProjectionProviderCapabilitiesV1::MEASURE)
                || self
                    .capabilities
                    .contains(ProjectionProviderCapabilitiesV1::FACET))
        {
            return Err(ProjectionProviderValidationError::InvalidBound);
        }
        Ok(())
    }
}

/// Safe closed validation failures for a provider descriptor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectionProviderValidationError {
    /// Input does not have the exact V1 size.
    InvalidLength,
    /// Magic or version does not identify V1.
    InvalidVersion,
    /// Provider tag is not admitted by V1.
    UnknownProvider,
    /// Exactness/approximation encoding is invalid for the provider.
    InvalidPosture,
    /// Recall target is zero or exceeds 100%.
    InvalidRecallTarget,
    /// Capability mask contains an unassigned bit.
    UnknownCapability,
    /// Candidate, window, or output capability is absent.
    MissingRequiredCapability,
    /// The real provider family cannot supply a requested capability.
    UnsupportedCapability,
    /// Policy-mode tag is unknown.
    UnknownPolicyMode,
    /// Freshness or lifecycle tag is unknown.
    UnknownLifecycle,
    /// A declared finite bound is zero, inverted, or inconsistent.
    InvalidBound,
    /// Provider state layout identity is invalid.
    InvalidStateIdentity,
    /// Reserved bytes are not zero.
    NonCanonicalReservedBytes,
}

impl fmt::Display for ProjectionProviderValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid projection provider descriptor: {self:?}"
        )
    }
}

impl Error for ProjectionProviderValidationError {}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("fixed descriptor"),
    )
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_be_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("fixed descriptor"),
    )
}
