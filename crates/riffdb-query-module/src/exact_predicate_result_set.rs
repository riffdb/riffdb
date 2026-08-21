//! Canonical module binding for ADR-0134 semantic families.

use riffdb_query_compiler::CompiledExactPredicateQueryV1;
use riffdb_query_ir::{
    AuthorizationEntityAccess, ExactPredicateProgramV1, NamedQuerySchemas,
    OperationalQueryFamilyV1, QuerySourceMap, SecretOutputRequirement,
};
use riffdb_types::{QueryCostVectorV1, QueryOperationName, QueryPlanHash, hash_query_plan};

use crate::{MAX_QUERY_MODULE_BYTES, QueryModuleError, QueryModuleErrorKind};

const MAGIC: &[u8] = b"RIFFDB-EXACT-PREDICATE-QUERY\0";
const VERSION: u16 = 1;

/// Immutable query-module binding for one compiler-owned exact family.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompiledExactPredicateResultSetV1 {
    name: QueryOperationName,
    program: ExactPredicateProgramV1,
    metadata: OperationalQueryFamilyV1,
    value_parameters: Vec<String>,
    presence_parameters: Vec<String>,
    limit_parameter: String,
    offset_parameter: String,
    canonical_bytes: Vec<u8>,
    identity: QueryPlanHash,
}

impl CompiledExactPredicateResultSetV1 {
    /// Seals compiler output into one canonical module artifact.
    pub fn new(
        name: QueryOperationName,
        compiled: CompiledExactPredicateQueryV1,
    ) -> Result<Self, QueryModuleError> {
        let program = compiled.program().clone();
        let metadata = compiled.metadata().clone();
        let value_parameters = compiled.value_parameters().to_vec();
        let presence_parameters = compiled.presence_parameters().to_vec();
        let limit_parameter = compiled.limit_parameter().to_owned();
        let offset_parameter = compiled.offset_parameter().to_owned();
        let mut canonical_bytes = Vec::new();
        canonical_bytes.extend_from_slice(MAGIC);
        canonical_bytes.extend_from_slice(&VERSION.to_be_bytes());
        write_text(&mut canonical_bytes, name.as_str())?;
        write_bytes(&mut canonical_bytes, program.canonical_bytes())?;
        write_bytes(&mut canonical_bytes, metadata.canonical_bytes())?;
        write_strings(&mut canonical_bytes, &value_parameters)?;
        write_strings(&mut canonical_bytes, &presence_parameters)?;
        write_text(&mut canonical_bytes, &limit_parameter)?;
        write_text(&mut canonical_bytes, &offset_parameter)?;
        if canonical_bytes.len() > MAX_QUERY_MODULE_BYTES {
            return Err(QueryModuleError::new(QueryModuleErrorKind::LimitExceeded));
        }
        let identity = hash_query_plan(&canonical_bytes);
        Ok(Self {
            name,
            program,
            metadata,
            value_parameters,
            presence_parameters,
            limit_parameter,
            offset_parameter,
            canonical_bytes,
            identity,
        })
    }

    /// Exact public operation name.
    #[must_use]
    pub const fn name(&self) -> &QueryOperationName {
        &self.name
    }

    /// Provider-independent predicate/order program.
    #[must_use]
    pub const fn program(&self) -> &ExactPredicateProgramV1 {
        &self.program
    }

    /// Canonically ordered value parameters.
    #[must_use]
    pub fn value_parameters(&self) -> &[String] {
        &self.value_parameters
    }

    /// Canonically ordered optional-presence parameters.
    #[must_use]
    pub fn presence_parameters(&self) -> &[String] {
        &self.presence_parameters
    }

    /// Typed page-limit parameter.
    #[must_use]
    pub fn limit_parameter(&self) -> &str {
        &self.limit_parameter
    }

    /// Typed ordinal-offset parameter.
    #[must_use]
    pub fn offset_parameter(&self) -> &str {
        &self.offset_parameter
    }

    /// Public typed parameter and result schemas.
    #[must_use]
    pub fn schemas(&self) -> &NamedQuerySchemas {
        self.metadata.surface().schemas()
    }

    /// Exact secret-output requirements.
    #[must_use]
    pub fn secret_outputs(&self) -> &[SecretOutputRequirement] {
        self.metadata.surface().secret_outputs()
    }

    /// Complete source map.
    #[must_use]
    pub fn source_map(&self) -> &QuerySourceMap {
        self.metadata.surface().source_map()
    }

    /// Authorization union for the complete named operation.
    #[must_use]
    pub fn authorization(&self) -> &[AuthorizationEntityAccess] {
        self.metadata.authorization_union()
    }

    /// Conservative request cost before provider activation.
    #[must_use]
    pub fn cost(&self) -> QueryCostVectorV1 {
        self.metadata.maximum_cost()
    }

    /// Non-executable representative used only for shared metadata.
    #[doc(hidden)]
    #[must_use]
    pub fn representative_program(&self) -> &riffdb_query_ir::QueryAccessProgramV1 {
        self.metadata.members()[0].program()
    }

    /// Canonical complete plan bytes.
    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    /// Complete plan identity.
    #[must_use]
    pub const fn identity(&self) -> QueryPlanHash {
        self.identity
    }
}

fn write_strings(output: &mut Vec<u8>, values: &[String]) -> Result<(), QueryModuleError> {
    output.extend_from_slice(
        &u32::try_from(values.len())
            .map_err(|_| QueryModuleError::new(QueryModuleErrorKind::LimitExceeded))?
            .to_be_bytes(),
    );
    for value in values {
        write_text(output, value)?;
    }
    Ok(())
}

fn write_text(output: &mut Vec<u8>, value: &str) -> Result<(), QueryModuleError> {
    write_bytes(output, value.as_bytes())
}

fn write_bytes(output: &mut Vec<u8>, value: &[u8]) -> Result<(), QueryModuleError> {
    output.extend_from_slice(
        &u32::try_from(value.len())
            .map_err(|_| QueryModuleError::new(QueryModuleErrorKind::LimitExceeded))?
            .to_be_bytes(),
    );
    output.extend_from_slice(value);
    Ok(())
}
