//! Separately versioned named exact-text plan-family binding (ADR-0131).

use std::error::Error;
use std::fmt;

use riffdb_query_ir::ExactTextPlanFamilyV1;
use riffdb_types::{QueryOperationName, QueryPlanHash, hash_query_plan};

/// Canonical named exact-text binding version.
pub const EXACT_TEXT_QUERY_BINDING_VERSION_V1: u16 = 1;
/// Maximum canonical binding size.
pub const MAX_EXACT_TEXT_QUERY_BINDING_BYTES: usize =
    4 + 2 + 2 + QueryOperationName::MAX_BYTES + 32;

const MAGIC: [u8; 4] = *b"RXTB";

/// One compiler-owned named-query binding to a finite exact-text family.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactTextQueryBindingV1 {
    query_name: QueryOperationName,
    plan_hash: QueryPlanHash,
}

impl ExactTextQueryBindingV1 {
    /// Binds the canonical family bytes to one checked symbolic query name.
    #[must_use]
    pub fn new(query_name: QueryOperationName, family: &ExactTextPlanFamilyV1) -> Self {
        Self {
            query_name,
            plan_hash: hash_query_plan(&family.to_canonical_bytes()),
        }
    }

    /// Exact symbolic query operation name.
    #[must_use]
    pub const fn query_name(&self) -> &QueryOperationName {
        &self.query_name
    }

    /// Domain-separated identity of the canonical exact-text family.
    #[must_use]
    pub const fn plan_hash(&self) -> QueryPlanHash {
        self.plan_hash
    }

    /// Canonically encodes the least-sufficient binding without rotating query-module V1--V4.
    #[must_use]
    pub fn to_canonical_bytes(&self) -> Vec<u8> {
        let name = self.query_name.as_str().as_bytes();
        let mut bytes = Vec::with_capacity(8 + name.len() + 32);
        bytes.extend_from_slice(&MAGIC);
        bytes.extend_from_slice(&EXACT_TEXT_QUERY_BINDING_VERSION_V1.to_be_bytes());
        bytes.extend_from_slice(&(name.len() as u16).to_be_bytes());
        bytes.extend_from_slice(name);
        bytes.extend_from_slice(self.plan_hash.as_bytes());
        bytes
    }

    /// Strictly decodes one complete binding or refuses mixed/unknown identity.
    pub fn from_canonical_bytes(bytes: &[u8]) -> Result<Self, ExactTextQueryBindingError> {
        if bytes.len() < 40 || bytes.len() > MAX_EXACT_TEXT_QUERY_BINDING_BYTES {
            return Err(ExactTextQueryBindingError::InvalidLength);
        }
        if bytes[..4] != MAGIC
            || u16::from_be_bytes([bytes[4], bytes[5]]) != EXACT_TEXT_QUERY_BINDING_VERSION_V1
        {
            return Err(ExactTextQueryBindingError::InvalidVersion);
        }
        let name_len = usize::from(u16::from_be_bytes([bytes[6], bytes[7]]));
        if bytes.len() != 8 + name_len + 32 {
            return Err(ExactTextQueryBindingError::InvalidLength);
        }
        let name = std::str::from_utf8(&bytes[8..8 + name_len])
            .map_err(|_| ExactTextQueryBindingError::InvalidQueryName)?;
        let query_name = QueryOperationName::new(name)
            .map_err(|_| ExactTextQueryBindingError::InvalidQueryName)?;
        let plan_hash = QueryPlanHash::from_bytes(
            bytes[8 + name_len..]
                .try_into()
                .map_err(|_| ExactTextQueryBindingError::InvalidLength)?,
        );
        Ok(Self {
            query_name,
            plan_hash,
        })
    }
}

/// Closed exact-text binding validation failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExactTextQueryBindingError {
    /// Total or name-derived length is invalid.
    InvalidLength,
    /// Magic or version is unknown.
    InvalidVersion,
    /// Query name is not a checked symbolic operation.
    InvalidQueryName,
}

impl fmt::Display for ExactTextQueryBindingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid exact-text query binding: {self:?}")
    }
}

impl Error for ExactTextQueryBindingError {}
