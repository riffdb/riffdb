//! Separately versioned named result-set binding artifact (ADR-0130).

use std::error::Error;
use std::fmt;

use riffdb_query_ir::{PROJECTION_RESULT_SET_PLAN_V1_BYTES, ProjectionResultSetPlanV1};
use riffdb_types::QueryOperationName;

/// Canonical named provider-plan binding version.
pub const PROJECTION_RESULT_SET_BINDING_VERSION_V1: u16 = 1;
/// Maximum canonical binding size.
pub const MAX_PROJECTION_RESULT_SET_BINDING_BYTES: usize =
    4 + 2 + 2 + QueryOperationName::MAX_BYTES + PROJECTION_RESULT_SET_PLAN_V1_BYTES;

const MAGIC: [u8; 4] = *b"RPRB";

/// One compiler-owned named-query binding to an exact provider plan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionResultSetBindingV1 {
    query_name: QueryOperationName,
    plan: ProjectionResultSetPlanV1,
}

impl ProjectionResultSetBindingV1 {
    /// Constructs a binding from already checked compiler artifacts.
    #[must_use]
    pub const fn new(query_name: QueryOperationName, plan: ProjectionResultSetPlanV1) -> Self {
        Self { query_name, plan }
    }

    /// Exact symbolic query operation name.
    #[must_use]
    pub const fn query_name(&self) -> &QueryOperationName {
        &self.query_name
    }
    /// Exact sealed result-set plan.
    #[must_use]
    pub const fn plan(&self) -> &ProjectionResultSetPlanV1 {
        &self.plan
    }

    /// Canonically encodes the binding without changing query-module V1--V4.
    #[must_use]
    pub fn to_canonical_bytes(&self) -> Vec<u8> {
        let name = self.query_name.as_str().as_bytes();
        let mut bytes = Vec::with_capacity(8 + name.len() + PROJECTION_RESULT_SET_PLAN_V1_BYTES);
        bytes.extend_from_slice(&MAGIC);
        bytes.extend_from_slice(&PROJECTION_RESULT_SET_BINDING_VERSION_V1.to_be_bytes());
        bytes.extend_from_slice(&(name.len() as u16).to_be_bytes());
        bytes.extend_from_slice(name);
        bytes.extend_from_slice(&self.plan.to_canonical_bytes());
        bytes
    }

    /// Strictly decodes and validates one complete binding.
    pub fn from_canonical_bytes(bytes: &[u8]) -> Result<Self, ProjectionResultSetBindingError> {
        if bytes.len() < 8 + PROJECTION_RESULT_SET_PLAN_V1_BYTES
            || bytes.len() > MAX_PROJECTION_RESULT_SET_BINDING_BYTES
        {
            return Err(ProjectionResultSetBindingError::InvalidLength);
        }
        if bytes[..4] != MAGIC || u16::from_be_bytes([bytes[4], bytes[5]]) != 1 {
            return Err(ProjectionResultSetBindingError::InvalidVersion);
        }
        let name_len = usize::from(u16::from_be_bytes([bytes[6], bytes[7]]));
        if bytes.len() != 8 + name_len + PROJECTION_RESULT_SET_PLAN_V1_BYTES {
            return Err(ProjectionResultSetBindingError::InvalidLength);
        }
        let name = std::str::from_utf8(&bytes[8..8 + name_len])
            .map_err(|_| ProjectionResultSetBindingError::InvalidQueryName)?;
        let query_name = QueryOperationName::new(name)
            .map_err(|_| ProjectionResultSetBindingError::InvalidQueryName)?;
        let plan = ProjectionResultSetPlanV1::from_canonical_bytes(&bytes[8 + name_len..])
            .map_err(|_| ProjectionResultSetBindingError::InvalidPlan)?;
        Ok(Self { query_name, plan })
    }
}

/// Closed named binding validation failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectionResultSetBindingError {
    /// Total or name-derived length is invalid.
    InvalidLength,
    /// Magic or version is unknown.
    InvalidVersion,
    /// Query name is not an exact checked symbol.
    InvalidQueryName,
    /// Embedded result-set plan is invalid.
    InvalidPlan,
}

impl fmt::Display for ProjectionResultSetBindingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid projection result-set binding: {self:?}")
    }
}

impl Error for ProjectionResultSetBindingError {}
