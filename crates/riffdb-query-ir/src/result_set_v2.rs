//! Runtime-parameter bounds for projection result-set windows (ADR-0131).

use std::error::Error;
use std::fmt;
use std::num::NonZeroU16;

use riffdb_types::{
    PROJECTION_PROVIDER_DESCRIPTOR_V1_BYTES, ProjectionProviderCapabilitiesV1,
    ProjectionProviderDescriptorHash, ProjectionProviderDescriptorV1, QueryPlanHash,
    hash_query_plan,
};

use crate::ResultSetOutputShapeV1;

/// Canonical parameter-bounds plan version.
pub const PROJECTION_RESULT_SET_PLAN_VERSION_V2: u16 = 2;
/// Exact fixed length of one canonical V2 result-set plan.
pub const PROJECTION_RESULT_SET_PLAN_V2_BYTES: usize = 160;

const MAGIC: [u8; 4] = *b"RPRS";
const STAGE_FILTER: u8 = 1 << 0;
const STAGE_RANK_ORDER: u8 = 1 << 1;
const STAGE_MEASURES: u8 = 1 << 2;

/// Compiler-owned bounds for a runtime-selected result window.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResultSetWindowBoundsV2 {
    /// A bounded first-page limit.
    Top {
        /// Maximum caller-selected nonzero limit.
        max_limit: NonZeroU16,
    },
    /// A bounded numeric ordinal and bounded nonzero limit.
    Ordinal {
        /// Greatest zero-based ordinal accepted at bind time.
        max_offset: u32,
        /// Maximum caller-selected nonzero limit.
        max_limit: NonZeroU16,
    },
}

/// Sealed result-set plan whose window values are typed runtime parameters.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionResultSetPlanV2 {
    provider: ProjectionProviderDescriptorV1,
    provider_digest: ProjectionProviderDescriptorHash,
    filtering: bool,
    rank_or_order: bool,
    whole_set_measures: bool,
    window: ResultSetWindowBoundsV2,
    output: ResultSetOutputShapeV1,
}

impl ProjectionResultSetPlanV2 {
    /// Constructs one plan and proves every maximum against provider bounds.
    pub fn new(
        provider: ProjectionProviderDescriptorV1,
        filtering: bool,
        rank_or_order: bool,
        whole_set_measures: bool,
        window: ResultSetWindowBoundsV2,
        output: ResultSetOutputShapeV1,
    ) -> Result<Self, ProjectionResultSetPlanV2Error> {
        let required =
            required_capabilities(filtering, rank_or_order, whole_set_measures, &provider);
        if !provider.capabilities().contains(required) {
            return Err(ProjectionResultSetPlanV2Error::UnsupportedStage);
        }
        let (max_offset, max_limit) = window_parts(window);
        if u32::from(max_limit.get()) > provider.max_output_rows()
            || max_offset > provider.max_candidates()
        {
            return Err(ProjectionResultSetPlanV2Error::WindowExceedsProviderBound);
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

    /// Strictly decodes one canonical V2 plan.
    pub fn from_canonical_bytes(bytes: &[u8]) -> Result<Self, ProjectionResultSetPlanV2Error> {
        if bytes.len() != PROJECTION_RESULT_SET_PLAN_V2_BYTES {
            return Err(ProjectionResultSetPlanV2Error::InvalidLength);
        }
        if bytes[..4] != MAGIC || u16::from_be_bytes([bytes[4], bytes[5]]) != 2 {
            return Err(ProjectionResultSetPlanV2Error::InvalidVersion);
        }
        if bytes[159] != 0 || bytes[150] & !(STAGE_FILTER | STAGE_RANK_ORDER | STAGE_MEASURES) != 0
        {
            return Err(ProjectionResultSetPlanV2Error::NonCanonicalEncoding);
        }
        let provider = ProjectionProviderDescriptorV1::from_canonical_bytes(
            &bytes[6..6 + PROJECTION_PROVIDER_DESCRIPTOR_V1_BYTES],
        )
        .map_err(|_| ProjectionResultSetPlanV2Error::InvalidProvider)?;
        if provider.digest().as_bytes() != &bytes[118..150] {
            return Err(ProjectionResultSetPlanV2Error::ProviderDigestMismatch);
        }
        let max_offset = u32::from_be_bytes(bytes[152..156].try_into().expect("fixed plan"));
        let max_limit = NonZeroU16::new(u16::from_be_bytes([bytes[156], bytes[157]]))
            .ok_or(ProjectionResultSetPlanV2Error::InvalidWindow)?;
        let window = match bytes[151] {
            1 if max_offset == 0 => ResultSetWindowBoundsV2::Top { max_limit },
            2 => ResultSetWindowBoundsV2::Ordinal {
                max_offset,
                max_limit,
            },
            _ => return Err(ProjectionResultSetPlanV2Error::InvalidWindow),
        };
        let output = match bytes[158] {
            1 => ResultSetOutputShapeV1::TypedRows,
            _ => return Err(ProjectionResultSetPlanV2Error::InvalidOutput),
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

    /// Canonical bytes; V1 bytes and interpretation remain untouched.
    #[must_use]
    pub fn to_canonical_bytes(&self) -> [u8; PROJECTION_RESULT_SET_PLAN_V2_BYTES] {
        let mut bytes = [0; PROJECTION_RESULT_SET_PLAN_V2_BYTES];
        bytes[..4].copy_from_slice(&MAGIC);
        bytes[4..6].copy_from_slice(&PROJECTION_RESULT_SET_PLAN_VERSION_V2.to_be_bytes());
        bytes[6..118].copy_from_slice(&self.provider.to_canonical_bytes());
        bytes[118..150].copy_from_slice(self.provider_digest.as_bytes());
        bytes[150] = (u8::from(self.filtering) * STAGE_FILTER)
            | (u8::from(self.rank_or_order) * STAGE_RANK_ORDER)
            | (u8::from(self.whole_set_measures) * STAGE_MEASURES);
        let (max_offset, max_limit) = window_parts(self.window);
        bytes[151] = match self.window {
            ResultSetWindowBoundsV2::Top { .. } => 1,
            ResultSetWindowBoundsV2::Ordinal { .. } => 2,
        };
        bytes[152..156].copy_from_slice(&max_offset.to_be_bytes());
        bytes[156..158].copy_from_slice(&max_limit.get().to_be_bytes());
        bytes[158] = self.output as u8;
        bytes
    }

    /// Exact plan identity including every runtime maximum.
    #[must_use]
    pub fn identity(&self) -> QueryPlanHash {
        hash_query_plan(&self.to_canonical_bytes())
    }

    /// Pinned provider descriptor.
    #[must_use]
    pub const fn provider(&self) -> &ProjectionProviderDescriptorV1 {
        &self.provider
    }

    /// Pinned provider descriptor digest.
    #[must_use]
    pub const fn provider_digest(&self) -> ProjectionProviderDescriptorHash {
        self.provider_digest
    }

    /// Runtime window bounds.
    #[must_use]
    pub const fn window(&self) -> ResultSetWindowBoundsV2 {
        self.window
    }

    /// Checks caller values before provider work or allocation.
    pub fn bind_window(
        &self,
        offset: u32,
        limit: NonZeroU16,
    ) -> Result<(), ProjectionResultSetPlanV2Error> {
        let (max_offset, max_limit) = window_parts(self.window);
        if offset > max_offset || limit > max_limit {
            return Err(ProjectionResultSetPlanV2Error::WindowExceedsProviderBound);
        }
        if matches!(self.window, ResultSetWindowBoundsV2::Top { .. }) && offset != 0 {
            return Err(ProjectionResultSetPlanV2Error::InvalidWindow);
        }
        Ok(())
    }
}

/// Closed V2 plan failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectionResultSetPlanV2Error {
    /// Byte length is not the exact V2 length.
    InvalidLength,
    /// Magic or version is not V2.
    InvalidVersion,
    /// Embedded provider is malformed.
    InvalidProvider,
    /// Descriptor digest does not match the embedded descriptor.
    ProviderDigestMismatch,
    /// A required stage is unsupported.
    UnsupportedStage,
    /// Declared or submitted window exceeds a compiled maximum.
    WindowExceedsProviderBound,
    /// Window tag, zero limit, or top offset is invalid.
    InvalidWindow,
    /// Output shape is unknown.
    InvalidOutput,
    /// Reserved bytes or flags are noncanonical.
    NonCanonicalEncoding,
}

impl fmt::Display for ProjectionResultSetPlanV2Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid projection result-set V2 plan: {self:?}")
    }
}

impl Error for ProjectionResultSetPlanV2Error {}

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
        required |= if provider
            .capabilities()
            .contains(ProjectionProviderCapabilitiesV1::RANK)
        {
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

const fn window_parts(window: ResultSetWindowBoundsV2) -> (u32, NonZeroU16) {
    match window {
        ResultSetWindowBoundsV2::Top { max_limit } => (0, max_limit),
        ResultSetWindowBoundsV2::Ordinal {
            max_offset,
            max_limit,
        } => (max_offset, max_limit),
    }
}
