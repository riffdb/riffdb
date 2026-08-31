//! Immutable named tokenized-text result-set binding (ADR-0173).

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use riffdb_query_compiler::CompiledTokenizedTextQueryV1;
use riffdb_query_ir::{
    AuthorizationEntityAccess, NamedQuerySchemas, OperationalQueryFamilyV1, QueryAccessProgramV1,
    QuerySourceMap, SecretOutputRequirement, TokenizedTextPlanV1,
};
use riffdb_types::{QueryCostVectorV1, QueryOperationName, QueryPlanHash, hash_query_plan};

/// Canonical compiled tokenized operation version.
pub const COMPILED_TOKENIZED_TEXT_QUERY_VERSION_V1: u16 = 1;

/// One fully sealed exact boolean tokenized named query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompiledTokenizedTextResultSetV1 {
    operation: QueryOperationName,
    plan: TokenizedTextPlanV1,
    metadata: Arc<OperationalQueryFamilyV1>,
    query_parameter: String,
    cost: QueryCostVectorV1,
    canonical_bytes: Vec<u8>,
    identity: QueryPlanHash,
}

impl CompiledTokenizedTextResultSetV1 {
    /// Seals provider semantics and existing public schema/authorization proof.
    pub fn new(
        operation: QueryOperationName,
        compiled: CompiledTokenizedTextQueryV1,
    ) -> Result<Self, CompiledTokenizedTextResultSetErrorV1> {
        if compiled.query_parameter.is_empty()
            || compiled.query_parameter.len() > u16::MAX as usize
            || compiled.metadata.members().is_empty()
        {
            return Err(CompiledTokenizedTextResultSetErrorV1::InvalidBinding);
        }
        let metadata_cost = compiled.metadata.maximum_cost();
        let cost = QueryCostVectorV1::new(
            metadata_cost.access_steps(),
            u64::from(compiled.plan.max_candidates()),
            0,
            metadata_cost.dependent_keys(),
            u64::from(compiled.plan.max_results()),
            metadata_cost.projected_values(),
            metadata_cost.encoded_result_bytes(),
        )
        .ok_or(CompiledTokenizedTextResultSetErrorV1::InvalidBinding)?;
        let canonical_bytes = encode(
            &operation,
            &compiled.plan,
            &compiled.metadata,
            &compiled.query_parameter,
            cost,
        )?;
        let identity = hash_query_plan(&canonical_bytes);
        Ok(Self {
            operation,
            plan: compiled.plan,
            metadata: Arc::new(compiled.metadata),
            query_parameter: compiled.query_parameter,
            cost,
            canonical_bytes,
            identity,
        })
    }

    /// Public operation name.
    #[must_use]
    pub const fn operation(&self) -> &QueryOperationName {
        &self.operation
    }

    /// Complete provider-semantic plan.
    #[must_use]
    pub const fn tokenized_plan(&self) -> &TokenizedTextPlanV1 {
        &self.plan
    }

    /// Typed bounded query-string parameter.
    #[must_use]
    pub fn query_parameter(&self) -> &str {
        &self.query_parameter
    }

    /// Public typed parameter/result schemas.
    #[must_use]
    pub fn schemas(&self) -> &NamedQuerySchemas {
        self.metadata.surface().schemas()
    }

    /// Compiler-derived secret result leaves.
    #[must_use]
    pub fn secret_outputs(&self) -> &[SecretOutputRequirement] {
        self.metadata.surface().secret_outputs()
    }

    /// Complete authorization union proved before provider selection.
    #[must_use]
    pub fn authorization(&self) -> &[AuthorizationEntityAccess] {
        self.metadata.authorization_union()
    }

    /// Static charge including the maintained-posting candidate ceiling.
    #[must_use]
    pub const fn cost(&self) -> QueryCostVectorV1 {
        self.cost
    }

    /// Request authorization charge excludes provider-maintenance scan work.
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
        .expect("lowering internal provider work preserves a valid cost")
    }

    /// Source map retained from symbolic resolution.
    #[must_use]
    pub fn source_map(&self) -> &QuerySourceMap {
        self.metadata.surface().source_map()
    }

    /// One non-executable metadata member for shared schema callers.
    #[doc(hidden)]
    #[must_use]
    pub fn representative_program(&self) -> &QueryAccessProgramV1 {
        self.metadata.members()[0].program()
    }

    /// Canonical bytes covered by the plan identity.
    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    /// Exact immutable plan identity.
    #[must_use]
    pub const fn identity(&self) -> QueryPlanHash {
        self.identity
    }
}

fn encode(
    operation: &QueryOperationName,
    plan: &TokenizedTextPlanV1,
    metadata: &OperationalQueryFamilyV1,
    query_parameter: &str,
    cost: QueryCostVectorV1,
) -> Result<Vec<u8>, CompiledTokenizedTextResultSetErrorV1> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"RTQM");
    bytes.extend_from_slice(&COMPILED_TOKENIZED_TEXT_QUERY_VERSION_V1.to_be_bytes());
    push_blob(&mut bytes, operation.as_str().as_bytes())?;
    push_blob(&mut bytes, &plan.to_canonical_bytes())?;
    push_blob(&mut bytes, metadata.canonical_bytes())?;
    push_blob(&mut bytes, query_parameter.as_bytes())?;
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

fn push_blob(
    bytes: &mut Vec<u8>,
    value: &[u8],
) -> Result<(), CompiledTokenizedTextResultSetErrorV1> {
    bytes.extend_from_slice(
        &u32::try_from(value.len())
            .map_err(|_| CompiledTokenizedTextResultSetErrorV1::InvalidBinding)?
            .to_be_bytes(),
    );
    bytes.extend_from_slice(value);
    Ok(())
}

/// Closed construction failure with no query values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompiledTokenizedTextResultSetErrorV1 {
    /// Name, metadata, cost, or canonical length is inconsistent.
    InvalidBinding,
}

impl fmt::Display for CompiledTokenizedTextResultSetErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("compiled tokenized text result set is invalid")
    }
}

impl Error for CompiledTokenizedTextResultSetErrorV1 {}
