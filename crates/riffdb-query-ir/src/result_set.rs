//! Compiler-owned projection result-set plan vocabulary (ADR-0130).

use std::error::Error;
use std::fmt;
use std::num::NonZeroU16;

use riffdb_types::{
    PROJECTION_PROVIDER_DESCRIPTOR_V1_BYTES, ProjectionProviderCapabilitiesV1,
    ProjectionProviderDescriptorHash, ProjectionProviderDescriptorV1, QueryPlanHash,
    hash_query_plan,
};

/// Canonical result-set plan identity, independent of query IR V1--V4.
pub const PROJECTION_RESULT_SET_PLAN_VERSION_V1: u16 = 1;
/// Exact fixed length of a canonical V1 result-set plan.
pub const PROJECTION_RESULT_SET_PLAN_V1_BYTES: usize = 160;

const MAGIC: [u8; 4] = *b"RPRS";
const STAGE_FILTER: u8 = 1 << 0;
const STAGE_RANK_ORDER: u8 = 1 << 1;
const STAGE_MEASURES: u8 = 1 << 2;

/// Compiler-selected bounded result window.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResultSetWindowV1 {
    /// First `limit` rows under the declared provider ordering.
    Top {
        /// Nonzero maximum returned rows.
        limit: NonZeroU16,
    },
    /// Indexed ordinal seek followed by at most `limit` rows.
    Ordinal {
        /// Zero-based ordinal selected without walking earlier rows.
        offset: u32,
        /// Nonzero maximum returned rows.
        limit: NonZeroU16,
    },
}

/// Closed typed-output forms in the foundation slice.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum ResultSetOutputShapeV1 {
    /// Compiler-declared typed rows.
    TypedRows = 1,
}

/// Sealed six-stage projection result-set plan with one pinned provider.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionResultSetPlanV1 {
    provider: ProjectionProviderDescriptorV1,
    provider_digest: ProjectionProviderDescriptorHash,
    filtering: bool,
    rank_or_order: bool,
    whole_set_measures: bool,
    window: ResultSetWindowV1,
    output: ResultSetOutputShapeV1,
}

impl ProjectionResultSetPlanV1 {
    /// Constructs a plan, rejecting every stage the pinned provider cannot supply.
    pub fn new(
        provider: ProjectionProviderDescriptorV1,
        filtering: bool,
        rank_or_order: bool,
        whole_set_measures: bool,
        window: ResultSetWindowV1,
        output: ResultSetOutputShapeV1,
    ) -> Result<Self, ProjectionResultSetPlanError> {
        let required =
            required_capabilities(filtering, rank_or_order, whole_set_measures, &provider);
        if !provider.capabilities().contains(required) {
            return Err(ProjectionResultSetPlanError::UnsupportedStage);
        }
        let (offset, limit) = window_parts(window);
        if u32::from(limit.get()) > provider.max_output_rows()
            || offset
                .checked_add(u32::from(limit.get()))
                .is_none_or(|end| end > provider.max_candidates())
        {
            return Err(ProjectionResultSetPlanError::WindowExceedsProviderBound);
        }
        let provider_digest = provider.digest();
        Ok(Self {
            provider,
            provider_digest,
            filtering,
            rank_or_order,
            whole_set_measures,
            window,
            output,
        })
    }

    /// Strictly decodes and validates one canonical fixed-size V1 plan.
    pub fn from_canonical_bytes(bytes: &[u8]) -> Result<Self, ProjectionResultSetPlanError> {
        if bytes.len() != PROJECTION_RESULT_SET_PLAN_V1_BYTES {
            return Err(ProjectionResultSetPlanError::InvalidLength);
        }
        if bytes[..4] != MAGIC || u16::from_be_bytes([bytes[4], bytes[5]]) != 1 {
            return Err(ProjectionResultSetPlanError::InvalidVersion);
        }
        if bytes[159] != 0 || bytes[150] & !(STAGE_FILTER | STAGE_RANK_ORDER | STAGE_MEASURES) != 0
        {
            return Err(ProjectionResultSetPlanError::NonCanonicalEncoding);
        }
        let provider = ProjectionProviderDescriptorV1::from_canonical_bytes(
            &bytes[6..6 + PROJECTION_PROVIDER_DESCRIPTOR_V1_BYTES],
        )
        .map_err(|_| ProjectionResultSetPlanError::InvalidProvider)?;
        if provider.digest().as_bytes() != &bytes[118..150] {
            return Err(ProjectionResultSetPlanError::ProviderDigestMismatch);
        }
        let offset = u32::from_be_bytes(bytes[152..156].try_into().expect("fixed plan"));
        let limit = NonZeroU16::new(u16::from_be_bytes([bytes[156], bytes[157]]))
            .ok_or(ProjectionResultSetPlanError::InvalidWindow)?;
        let window = match bytes[151] {
            1 if offset == 0 => ResultSetWindowV1::Top { limit },
            2 => ResultSetWindowV1::Ordinal { offset, limit },
            _ => return Err(ProjectionResultSetPlanError::InvalidWindow),
        };
        let output = match bytes[158] {
            1 => ResultSetOutputShapeV1::TypedRows,
            _ => return Err(ProjectionResultSetPlanError::InvalidOutput),
        };
        Self::new(
            provider,
            bytes[150] & STAGE_FILTER != 0,
            bytes[150] & STAGE_RANK_ORDER != 0,
            bytes[150] & STAGE_MEASURES != 0,
            window,
            output,
        )
    }

    /// Canonically encodes the exact provider descriptor and its digest binding.
    #[must_use]
    pub fn to_canonical_bytes(&self) -> [u8; PROJECTION_RESULT_SET_PLAN_V1_BYTES] {
        let mut bytes = [0; PROJECTION_RESULT_SET_PLAN_V1_BYTES];
        bytes[..4].copy_from_slice(&MAGIC);
        bytes[4..6].copy_from_slice(&PROJECTION_RESULT_SET_PLAN_VERSION_V1.to_be_bytes());
        bytes[6..118].copy_from_slice(&self.provider.to_canonical_bytes());
        bytes[118..150].copy_from_slice(self.provider_digest.as_bytes());
        bytes[150] = (u8::from(self.filtering) * STAGE_FILTER)
            | (u8::from(self.rank_or_order) * STAGE_RANK_ORDER)
            | (u8::from(self.whole_set_measures) * STAGE_MEASURES);
        let (offset, limit) = window_parts(self.window);
        bytes[151] = match self.window {
            ResultSetWindowV1::Top { .. } => 1,
            ResultSetWindowV1::Ordinal { .. } => 2,
        };
        bytes[152..156].copy_from_slice(&offset.to_be_bytes());
        bytes[156..158].copy_from_slice(&limit.get().to_be_bytes());
        bytes[158] = self.output as u8;
        bytes
    }

    /// Domain-separated identity of the exact canonical result-set plan.
    #[must_use]
    pub fn identity(&self) -> QueryPlanHash {
        hash_query_plan(&self.to_canonical_bytes())
    }

    /// Exact provider descriptor pinned by the compiler.
    #[must_use]
    pub const fn provider(&self) -> &ProjectionProviderDescriptorV1 {
        &self.provider
    }
    /// Digest carried alongside the provider descriptor.
    #[must_use]
    pub const fn provider_digest(&self) -> ProjectionProviderDescriptorHash {
        self.provider_digest
    }
    /// Fixed semantic stage order; optional stages remain in their declared slot.
    #[must_use]
    pub const fn stage_names(&self) -> [&'static str; 6] {
        [
            "candidates",
            "policy_filter",
            "rank_order",
            "whole_set_measures",
            "window",
            "typed_output",
        ]
    }
    /// Whether predicate filtering is present.
    #[must_use]
    pub const fn filtering(&self) -> bool {
        self.filtering
    }
    /// Whether ranking or exact ordering is present.
    #[must_use]
    pub const fn rank_or_order(&self) -> bool {
        self.rank_or_order
    }
    /// Whether whole-admitted-set measures are present.
    #[must_use]
    pub const fn whole_set_measures(&self) -> bool {
        self.whole_set_measures
    }
    /// Compiler-owned result window.
    #[must_use]
    pub const fn window(&self) -> ResultSetWindowV1 {
        self.window
    }
}

/// Closed result-set plan validation failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectionResultSetPlanError {
    /// Byte length is not the exact V1 length.
    InvalidLength,
    /// Magic or version does not identify V1.
    InvalidVersion,
    /// Embedded provider descriptor is invalid.
    InvalidProvider,
    /// Carried digest does not match the exact embedded descriptor.
    ProviderDigestMismatch,
    /// An optional stage is unsupported by the pinned provider.
    UnsupportedStage,
    /// Window exceeds a compiler-owned provider bound.
    WindowExceedsProviderBound,
    /// Window tag, zero limit, or canonical top offset is invalid.
    InvalidWindow,
    /// Typed output tag is unknown.
    InvalidOutput,
    /// Flags or reserved bytes are noncanonical.
    NonCanonicalEncoding,
}

impl fmt::Display for ProjectionResultSetPlanError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid projection result-set plan: {self:?}")
    }
}

impl Error for ProjectionResultSetPlanError {}

fn required_capabilities(
    filtering: bool,
    rank_or_order: bool,
    whole_set_measures: bool,
    provider: &ProjectionProviderDescriptorV1,
) -> ProjectionProviderCapabilitiesV1 {
    let mut required = ProjectionProviderCapabilitiesV1::CANDIDATE
        | ProjectionProviderCapabilitiesV1::WINDOW
        | ProjectionProviderCapabilitiesV1::OUTPUT;
    if filtering {
        required |= ProjectionProviderCapabilitiesV1::FILTER;
    }
    if rank_or_order {
        let offered = provider.capabilities();
        required |= if offered.contains(ProjectionProviderCapabilitiesV1::RANK) {
            ProjectionProviderCapabilitiesV1::RANK
        } else {
            ProjectionProviderCapabilitiesV1::ORDER
        };
    }
    if whole_set_measures {
        required |= ProjectionProviderCapabilitiesV1::MEASURE;
    }
    required
}

const fn window_parts(window: ResultSetWindowV1) -> (u32, NonZeroU16) {
    match window {
        ResultSetWindowV1::Top { limit } => (0, limit),
        ResultSetWindowV1::Ordinal { offset, limit } => (offset, limit),
    }
}
