//! Exact named result-set binding activated by WP-647.

use std::error::Error;
use std::fmt;

use std::sync::Arc;

use riffdb_query_ir::{
    AuthorizationEntityAccess, ExactTextPlanFamilyV1, NamedQuerySchemas, OperationalQueryFamilyV1,
    PROJECTION_RESULT_SET_PLAN_V2_BYTES, ProjectionResultSetPlanV2, QueryAccessProgramV1,
    QuerySourceMap, SecretOutputRequirement,
};
use riffdb_types::{
    ExactTextOperatorV1, ExactTextOrderV1, FieldId, QueryCostVectorV1, QueryOperationName,
    QueryPlanHash, hash_query_plan,
};

/// Canonical exact-text result-set binding version.
pub const EXACT_TEXT_RESULT_SET_BINDING_VERSION_V1: u16 = 1;
/// Canonical compiled exact-query identity without a typed equality filter.
pub const COMPILED_EXACT_TEXT_QUERY_VERSION_V1: u16 = 1;
/// Canonical compiled exact-query identity with one typed equality filter.
pub const COMPILED_EXACT_TEXT_QUERY_VERSION_V2: u16 = 2;
/// Fixed non-name bytes in one canonical binding.
pub const EXACT_TEXT_RESULT_SET_BINDING_FIXED_BYTES_V1: usize =
    8 + 64 + PROJECTION_RESULT_SET_PLAN_V2_BYTES;

/// One compiler-owned named exact family, runtime bounds plan, and identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactTextResultSetBindingV1 {
    query_name: QueryOperationName,
    family: ExactTextPlanFamilyV1,
    plan: ProjectionResultSetPlanV2,
    identity: QueryPlanHash,
}

impl ExactTextResultSetBindingV1 {
    /// Constructs and cross-validates one complete exact binding.
    pub fn new(
        query_name: QueryOperationName,
        family: ExactTextPlanFamilyV1,
        plan: ProjectionResultSetPlanV2,
    ) -> Result<Self, ExactTextResultSetBindingError> {
        if family.descriptor().digest() != plan.provider_digest() {
            return Err(ExactTextResultSetBindingError::ProviderMismatch);
        }
        let identity = binding_identity(&family, &plan);
        Ok(Self {
            query_name,
            family,
            plan,
            identity,
        })
    }

    /// Exact symbolic operation name.
    #[must_use]
    pub const fn query_name(&self) -> &QueryOperationName {
        &self.query_name
    }

    /// Compiler-enumerated predicate/order family.
    #[must_use]
    pub const fn family(&self) -> &ExactTextPlanFamilyV1 {
        &self.family
    }

    /// Parameter-bounds result-set plan.
    #[must_use]
    pub const fn plan(&self) -> &ProjectionResultSetPlanV2 {
        &self.plan
    }

    /// Identity binding family and result-set stage plan, excluding display name.
    #[must_use]
    pub const fn identity(&self) -> QueryPlanHash {
        self.identity
    }

    /// Canonical separately versioned binding bytes.
    #[must_use]
    pub fn to_canonical_bytes(&self) -> Vec<u8> {
        let name = self.query_name.as_str().as_bytes();
        let mut bytes =
            Vec::with_capacity(EXACT_TEXT_RESULT_SET_BINDING_FIXED_BYTES_V1 + name.len());
        bytes.extend_from_slice(b"RXRB");
        bytes.extend_from_slice(&EXACT_TEXT_RESULT_SET_BINDING_VERSION_V1.to_be_bytes());
        bytes.extend_from_slice(&(name.len() as u16).to_be_bytes());
        bytes.extend_from_slice(name);
        bytes.extend_from_slice(&self.family.to_canonical_bytes());
        bytes.extend_from_slice(&self.plan.to_canonical_bytes());
        bytes
    }

    /// Strictly decodes and re-proves the complete binding.
    pub fn from_canonical_bytes(bytes: &[u8]) -> Result<Self, ExactTextResultSetBindingError> {
        if bytes.len() < EXACT_TEXT_RESULT_SET_BINDING_FIXED_BYTES_V1
            || &bytes[..4] != b"RXRB"
            || u16::from_be_bytes([bytes[4], bytes[5]]) != EXACT_TEXT_RESULT_SET_BINDING_VERSION_V1
        {
            return Err(ExactTextResultSetBindingError::InvalidEncoding);
        }
        let name_len = usize::from(u16::from_be_bytes([bytes[6], bytes[7]]));
        if bytes.len() != EXACT_TEXT_RESULT_SET_BINDING_FIXED_BYTES_V1 + name_len {
            return Err(ExactTextResultSetBindingError::InvalidEncoding);
        }
        let name_end = 8 + name_len;
        let query_name = std::str::from_utf8(&bytes[8..name_end])
            .map_err(|_| ExactTextResultSetBindingError::InvalidEncoding)
            .and_then(|name| {
                QueryOperationName::new(name)
                    .map_err(|_| ExactTextResultSetBindingError::InvalidEncoding)
            })?;
        let family_end = name_end + 64;
        let plan = ProjectionResultSetPlanV2::from_canonical_bytes(&bytes[family_end..])
            .map_err(|_| ExactTextResultSetBindingError::InvalidEncoding)?;
        let family = ExactTextPlanFamilyV1::from_canonical_bytes(
            &bytes[name_end..family_end],
            plan.provider().clone(),
        )
        .map_err(|_| ExactTextResultSetBindingError::InvalidEncoding)?;
        let binding = Self::new(query_name, family, plan)?;
        if binding.to_canonical_bytes() != bytes {
            return Err(ExactTextResultSetBindingError::InvalidEncoding);
        }
        Ok(binding)
    }
}

/// Closed binding failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExactTextResultSetBindingError {
    /// Framing, version, name, family, plan, or canonical bytes are invalid.
    InvalidEncoding,
    /// Exact family and stage plan pin different providers.
    ProviderMismatch,
}

impl fmt::Display for ExactTextResultSetBindingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid exact result-set binding: {self:?}")
    }
}

impl Error for ExactTextResultSetBindingError {}

/// One fully compiled exact named query.
///
/// `metadata` is a non-executable lowering used only for the already-proven
/// symbolic schemas, authorization union, source map, and bounded result byte
/// charge. The exact binding is the sole executable semantic plan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompiledExactTextResultSetV1 {
    binding: ExactTextResultSetBindingV1,
    metadata: Arc<OperationalQueryFamilyV1>,
    operator: ExactTextOperatorV1,
    order: ExactTextOrderV1,
    needle_parameter: String,
    limit_parameter: String,
    offset_parameter: String,
    filter: Option<ExactTextFilterBindingV1>,
    cost: QueryCostVectorV1,
    canonical_bytes: Vec<u8>,
    identity: QueryPlanHash,
}

/// One compiler-bound optional equality filter; callers supply only its typed value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactTextFilterBindingV1 {
    field: FieldId,
    parameter: String,
}

impl ExactTextFilterBindingV1 {
    /// Constructs a closed filter binding from compiler-owned identities.
    pub fn new(
        field: FieldId,
        parameter: impl Into<String>,
    ) -> Result<Self, ExactTextResultSetBindingError> {
        let parameter = parameter.into();
        if parameter.is_empty() || parameter.len() > u16::MAX as usize {
            return Err(ExactTextResultSetBindingError::InvalidEncoding);
        }
        Ok(Self { field, parameter })
    }

    /// Compiler-resolved filtered field.
    #[doc(hidden)]
    #[must_use]
    pub const fn internal_field(&self) -> FieldId {
        self.field
    }

    /// Typed optional parameter controlling filter presence.
    #[must_use]
    pub fn parameter(&self) -> &str {
        &self.parameter
    }
}

impl CompiledExactTextResultSetV1 {
    /// Seals one finite exact operation and its non-executable metadata proof.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        binding: ExactTextResultSetBindingV1,
        metadata: OperationalQueryFamilyV1,
        operator: ExactTextOperatorV1,
        order: ExactTextOrderV1,
        needle_parameter: impl Into<String>,
        limit_parameter: impl Into<String>,
        offset_parameter: impl Into<String>,
        filter: Option<ExactTextFilterBindingV1>,
    ) -> Result<Self, ExactTextResultSetBindingError> {
        let expected_presence = filter
            .as_ref()
            .map_or(&[][..], |filter| std::slice::from_ref(&filter.parameter));
        if !binding.family().contains_member(operator, order)
            || metadata.presence_parameters() != expected_presence
        {
            return Err(ExactTextResultSetBindingError::ProviderMismatch);
        }
        let needle_parameter = needle_parameter.into();
        let limit_parameter = limit_parameter.into();
        let offset_parameter = offset_parameter.into();
        if [
            needle_parameter.as_str(),
            limit_parameter.as_str(),
            offset_parameter.as_str(),
        ]
        .iter()
        .any(|name| name.is_empty() || name.len() > u16::MAX as usize)
        {
            return Err(ExactTextResultSetBindingError::InvalidEncoding);
        }
        let metadata_cost = metadata.maximum_cost();
        let maximum_rows = u64::from(binding.plan().provider().max_output_rows());
        let cost = QueryCostVectorV1::new(
            1,
            u64::from(binding.family().max_candidates()),
            0,
            0,
            maximum_rows,
            metadata_cost.projected_values(),
            metadata_cost.encoded_result_bytes(),
        )
        .ok_or(ExactTextResultSetBindingError::ProviderMismatch)?;
        let canonical_bytes = encode_compiled_exact(
            &binding,
            &metadata,
            operator,
            order,
            &needle_parameter,
            &limit_parameter,
            &offset_parameter,
            filter.as_ref(),
            cost,
        )?;
        let identity = hash_query_plan(&canonical_bytes);
        Ok(Self {
            binding,
            metadata: Arc::new(metadata),
            operator,
            order,
            needle_parameter,
            limit_parameter,
            offset_parameter,
            filter,
            cost,
            canonical_bytes,
            identity,
        })
    }

    /// Complete exact provider binding.
    #[must_use]
    pub const fn binding(&self) -> &ExactTextResultSetBindingV1 {
        &self.binding
    }

    /// Fixed exact predicate selected by source compilation.
    #[must_use]
    pub const fn operator(&self) -> ExactTextOperatorV1 {
        self.operator
    }

    /// Fixed total order selected by source compilation.
    #[must_use]
    pub const fn order(&self) -> ExactTextOrderV1 {
        self.order
    }

    /// Typed needle parameter name.
    #[must_use]
    pub fn needle_parameter(&self) -> &str {
        &self.needle_parameter
    }

    /// Typed page-limit parameter name.
    #[must_use]
    pub fn limit_parameter(&self) -> &str {
        &self.limit_parameter
    }

    /// Typed zero-based ordinal parameter name.
    #[must_use]
    pub fn offset_parameter(&self) -> &str {
        &self.offset_parameter
    }

    /// Optional compiler-bound equality filter executed before count and windowing.
    #[must_use]
    pub const fn filter(&self) -> Option<&ExactTextFilterBindingV1> {
        self.filter.as_ref()
    }

    /// Public typed schemas proved by the symbolic resolver.
    #[must_use]
    pub fn schemas(&self) -> &NamedQuerySchemas {
        self.metadata.surface().schemas()
    }

    /// Compiler-derived secret result leaves.
    #[must_use]
    pub fn secret_outputs(&self) -> &[SecretOutputRequirement] {
        self.metadata.surface().secret_outputs()
    }

    /// Complete read authority proved before exact execution.
    #[must_use]
    pub fn authorization(&self) -> &[AuthorizationEntityAccess] {
        self.metadata.authorization_union()
    }

    /// Whole-request static charge including the provider candidate ceiling.
    #[must_use]
    pub const fn cost(&self) -> QueryCostVectorV1 {
        self.cost
    }

    /// Request-time authorization charge after compiler-owned provider work is separated.
    ///
    /// `cost` retains the complete provider candidate/rebuild ceiling in the
    /// immutable plan identity. Public execution performs no authoritative
    /// index scan or point read: it consumes one already policy-aligned derived
    /// posting and shapes at most the declared output window. The application
    /// role therefore grants only this request-time charge rather than gaining
    /// authority for the provider's background maintenance scan.
    #[must_use]
    pub fn authorization_cost(&self) -> QueryCostVectorV1 {
        QueryCostVectorV1::new(
            self.cost.access_steps(),
            0,
            0,
            self.cost.dependent_keys(),
            self.cost.intermediate_rows(),
            self.cost.projected_values(),
            self.cost.encoded_result_bytes(),
        )
        .expect("lowering valid internal work dimensions preserves a valid query cost")
    }

    /// Source map proved by the symbolic resolver.
    #[must_use]
    pub fn source_map(&self) -> &QuerySourceMap {
        self.metadata.surface().source_map()
    }

    /// One non-executable access member used only by shared metadata callers.
    #[doc(hidden)]
    #[must_use]
    pub fn representative_program(&self) -> &QueryAccessProgramV1 {
        self.metadata.members()[0].program()
    }

    /// Canonical bytes covered by this exact plan identity.
    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    /// Exact plan identity.
    #[must_use]
    pub const fn identity(&self) -> QueryPlanHash {
        self.identity
    }
}

#[allow(clippy::too_many_arguments)]
fn encode_compiled_exact(
    binding: &ExactTextResultSetBindingV1,
    metadata: &OperationalQueryFamilyV1,
    operator: ExactTextOperatorV1,
    order: ExactTextOrderV1,
    needle_parameter: &str,
    limit_parameter: &str,
    offset_parameter: &str,
    filter: Option<&ExactTextFilterBindingV1>,
    cost: QueryCostVectorV1,
) -> Result<Vec<u8>, ExactTextResultSetBindingError> {
    let binding_bytes = binding.to_canonical_bytes();
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"RXCQ");
    bytes.extend_from_slice(
        &if filter.is_some() {
            COMPILED_EXACT_TEXT_QUERY_VERSION_V2
        } else {
            COMPILED_EXACT_TEXT_QUERY_VERSION_V1
        }
        .to_be_bytes(),
    );
    bytes.push(operator as u8);
    bytes.push(order as u8);
    if let Some(filter) = filter {
        bytes.extend_from_slice(&filter.field.to_be_bytes());
        write_blob(&mut bytes, filter.parameter.as_bytes())?;
    }
    write_blob(&mut bytes, &binding_bytes)?;
    write_blob(&mut bytes, metadata.canonical_bytes())?;
    for value in [needle_parameter, limit_parameter, offset_parameter] {
        write_blob(&mut bytes, value.as_bytes())?;
    }
    for value in [
        cost.access_steps(),
        cost.scanned_index_rows(),
        cost.point_reads(),
        cost.dependent_keys(),
        cost.intermediate_rows(),
        cost.projected_values(),
        cost.encoded_result_bytes(),
    ] {
        bytes.extend_from_slice(&value.to_be_bytes());
    }
    Ok(bytes)
}

fn write_blob(output: &mut Vec<u8>, value: &[u8]) -> Result<(), ExactTextResultSetBindingError> {
    output.extend_from_slice(
        &u32::try_from(value.len())
            .map_err(|_| ExactTextResultSetBindingError::InvalidEncoding)?
            .to_be_bytes(),
    );
    output.extend_from_slice(value);
    Ok(())
}

fn binding_identity(
    family: &ExactTextPlanFamilyV1,
    plan: &ProjectionResultSetPlanV2,
) -> QueryPlanHash {
    let mut bytes = Vec::with_capacity(64 + PROJECTION_RESULT_SET_PLAN_V2_BYTES);
    bytes.extend_from_slice(&family.to_canonical_bytes());
    bytes.extend_from_slice(&plan.to_canonical_bytes());
    hash_query_plan(&bytes)
}
