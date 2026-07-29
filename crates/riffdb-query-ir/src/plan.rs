use riffdb_riffql_syntax::Cardinality;
use riffdb_types::{
    ContractBundleHash, ContractLineage, ContractVersion, EntityTypeId, FieldId, IndexId,
    QueryPlanHash, hash_query_plan,
};

use crate::{ExactContractIdentity, MAX_QUERY_ARTIFACT_BYTES, QUERY_IR_VERSION_V1};

const PROGRAM_MAGIC: &[u8] = b"RIFFDB-QUERY-ACCESS-PROGRAM\0";

/// Direction of one complete ordered index walk.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccessDirection {
    /// Declared index order.
    Forward,
    /// Complete reverse of declared index order.
    Reverse,
}

/// Closed physical access selected for one binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryAccessKind {
    /// Exact primary-key lookup.
    Point {
        /// Primary-key fields in contract order.
        key_fields: Vec<String>,
    },
    /// Bounded secondary-index range.
    Index {
        /// Exact contract index name.
        index: String,
        /// Index fields in contract order.
        fields: Vec<String>,
        /// Whole-index traversal direction.
        direction: AccessDirection,
    },
}

/// One ordered, bounded access in a closed program.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryAccessStep {
    binding: String,
    entity: String,
    cardinality: Cardinality,
    maximum_rows: u64,
    access: QueryAccessKind,
    predicate_fields: Vec<String>,
    selected_fields: Vec<String>,
    dependencies: Vec<String>,
    entity_id: EntityTypeId,
    index_id: Option<IndexId>,
}

impl QueryAccessStep {
    /// Query-local binding name.
    #[must_use]
    pub fn binding(&self) -> &str {
        &self.binding
    }

    /// Contract entity name.
    #[must_use]
    pub fn entity(&self) -> &str {
        &self.entity
    }

    /// Expected result cardinality.
    #[must_use]
    pub const fn cardinality(&self) -> Cardinality {
        self.cardinality
    }

    /// Compiler-proven maximum rows.
    #[must_use]
    pub const fn maximum_rows(&self) -> u64 {
        self.maximum_rows
    }

    /// Selected physical access.
    #[must_use]
    pub const fn access(&self) -> &QueryAccessKind {
        &self.access
    }

    /// Fields read while evaluating predicates.
    #[must_use]
    pub fn predicate_fields(&self) -> &[String] {
        &self.predicate_fields
    }

    /// Fields returned from this binding.
    #[must_use]
    pub fn selected_fields(&self) -> &[String] {
        &self.selected_fields
    }

    /// Earlier bindings supplying key values.
    #[must_use]
    pub fn dependencies(&self) -> &[String] {
        &self.dependencies
    }

    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    pub fn checked(
        binding: String,
        entity: String,
        cardinality: Cardinality,
        maximum_rows: u64,
        access: QueryAccessKind,
        predicate_fields: Vec<String>,
        selected_fields: Vec<String>,
        dependencies: Vec<String>,
        entity_id: EntityTypeId,
        index_id: Option<IndexId>,
    ) -> Option<Self> {
        if binding.is_empty()
            || entity.is_empty()
            || maximum_rows == 0
            || predicate_fields.windows(2).any(|pair| pair[0] >= pair[1])
            || selected_fields.windows(2).any(|pair| pair[0] >= pair[1])
            || dependencies.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return None;
        }
        Some(Self {
            binding,
            entity,
            cardinality,
            maximum_rows,
            access,
            predicate_fields,
            selected_fields,
            dependencies,
            entity_id,
            index_id,
        })
    }
}

/// Complete compiler-derived read authority for one entity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorizationEntityAccess {
    entity: String,
    fields: Vec<String>,
    indexes: Vec<String>,
    maximum_rows: u64,
    entity_id: EntityTypeId,
    field_ids: Vec<FieldId>,
    index_ids: Vec<IndexId>,
}

impl AuthorizationEntityAccess {
    /// Contract entity name.
    #[must_use]
    pub fn entity(&self) -> &str {
        &self.entity
    }

    /// Complete visible/read field set in exact name order.
    #[must_use]
    pub fn fields(&self) -> &[String] {
        &self.fields
    }

    /// Required index names in exact order.
    #[must_use]
    pub fn indexes(&self) -> &[String] {
        &self.indexes
    }

    /// Aggregate maximum rows for the request.
    #[must_use]
    pub const fn maximum_rows(&self) -> u64 {
        self.maximum_rows
    }

    #[doc(hidden)]
    pub fn checked(
        entity: String,
        fields: Vec<String>,
        indexes: Vec<String>,
        maximum_rows: u64,
        entity_id: EntityTypeId,
        field_ids: Vec<FieldId>,
        index_ids: Vec<IndexId>,
    ) -> Option<Self> {
        if entity.is_empty()
            || maximum_rows == 0
            || fields.len() != field_ids.len()
            || indexes.len() != index_ids.len()
            || fields.windows(2).any(|pair| pair[0] >= pair[1])
            || indexes.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return None;
        }
        Some(Self {
            entity,
            fields,
            indexes,
            maximum_rows,
            entity_id,
            field_ids,
            index_ids,
        })
    }
}

/// Stable identity of one canonical query access program.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct QueryPlanIdentity(QueryPlanHash);

impl QueryPlanIdentity {
    /// Typed domain-separated digest.
    #[must_use]
    pub const fn hash(self) -> QueryPlanHash {
        self.0
    }
}

/// Bounded name-only explain representation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryPlanExplain {
    lines: Vec<String>,
}

impl QueryPlanExplain {
    /// Stable explain lines.
    #[must_use]
    pub fn lines(&self) -> &[String] {
        &self.lines
    }
}

/// Closed, ordered, same-partition RiffQL v1 access program.
#[derive(Clone, Eq, PartialEq)]
pub struct QueryAccessProgramV1 {
    contract: ExactContractIdentity,
    name: Option<String>,
    partition_parameter: String,
    steps: Vec<QueryAccessStep>,
    authorization: Vec<AuthorizationEntityAccess>,
    canonical_bytes: Vec<u8>,
    identity: QueryPlanIdentity,
    explain: QueryPlanExplain,
}

impl std::fmt::Debug for QueryAccessProgramV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QueryAccessProgramV1")
            .field("contract_lineage", &self.contract.lineage().as_str())
            .field("contract_version", &self.contract.version())
            .field("name", &self.name)
            .field("partition_parameter", &self.partition_parameter)
            .field("steps", &self.steps)
            .field("authorization", &self.authorization)
            .field("identity", &self.identity)
            .finish()
    }
}

impl QueryAccessProgramV1 {
    #[doc(hidden)]
    pub fn checked(
        contract: ExactContractIdentity,
        name: Option<String>,
        partition_parameter: String,
        steps: Vec<QueryAccessStep>,
        authorization: Vec<AuthorizationEntityAccess>,
    ) -> Option<Self> {
        if partition_parameter.is_empty()
            || steps.is_empty()
            || authorization.is_empty()
            || authorization
                .windows(2)
                .any(|pair| pair[0].entity >= pair[1].entity)
        {
            return None;
        }
        let canonical_bytes = encode_program(
            &contract,
            name.as_deref(),
            &partition_parameter,
            &steps,
            &authorization,
        )?;
        let identity = QueryPlanIdentity(hash_query_plan(&canonical_bytes));
        let explain = build_explain(&partition_parameter, &steps, &authorization);
        Some(Self {
            contract,
            name,
            partition_parameter,
            steps,
            authorization,
            canonical_bytes,
            identity,
            explain,
        })
    }

    /// Exact contract identity.
    #[must_use]
    pub const fn contract(&self) -> &ExactContractIdentity {
        &self.contract
    }

    /// Optional declared query name.
    #[must_use]
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// One parameter routing every access to the same partition.
    #[must_use]
    pub fn partition_parameter(&self) -> &str {
        &self.partition_parameter
    }

    /// Ordered access steps.
    #[must_use]
    pub fn steps(&self) -> &[QueryAccessStep] {
        &self.steps
    }

    /// Complete authorization requirement set.
    #[must_use]
    pub fn authorization(&self) -> &[AuthorizationEntityAccess] {
        &self.authorization
    }

    /// Canonical bytes covered by the plan identity.
    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    /// Stable domain-separated plan identity.
    #[must_use]
    pub const fn identity(&self) -> QueryPlanIdentity {
        self.identity
    }

    /// Bounded name-only explain form.
    #[must_use]
    pub const fn explain(&self) -> &QueryPlanExplain {
        &self.explain
    }
}

fn encode_program(
    contract: &ExactContractIdentity,
    name: Option<&str>,
    partition_parameter: &str,
    steps: &[QueryAccessStep],
    authorization: &[AuthorizationEntityAccess],
) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(PROGRAM_MAGIC);
    out.extend_from_slice(&QUERY_IR_VERSION_V1.to_be_bytes());
    write_text(&mut out, contract.lineage().as_str())?;
    out.extend_from_slice(&contract.version().get().to_be_bytes());
    out.extend_from_slice(contract.bundle_hash().as_bytes());
    write_text(&mut out, name.unwrap_or(""))?;
    write_text(&mut out, partition_parameter)?;
    write_count(&mut out, steps.len())?;
    for step in steps {
        write_text(&mut out, &step.binding)?;
        write_text(&mut out, &step.entity)?;
        out.push(match step.cardinality {
            Cardinality::One => 1,
            Cardinality::Maybe => 2,
            Cardinality::Many => 3,
        });
        out.extend_from_slice(&step.maximum_rows.to_be_bytes());
        match &step.access {
            QueryAccessKind::Point { key_fields } => {
                out.push(1);
                write_strings(&mut out, key_fields)?;
            }
            QueryAccessKind::Index {
                index,
                fields,
                direction,
            } => {
                out.push(2);
                write_text(&mut out, index)?;
                write_strings(&mut out, fields)?;
                out.push(match direction {
                    AccessDirection::Forward => 1,
                    AccessDirection::Reverse => 2,
                });
            }
        }
        write_strings(&mut out, &step.predicate_fields)?;
        write_strings(&mut out, &step.selected_fields)?;
        write_strings(&mut out, &step.dependencies)?;
    }
    write_count(&mut out, authorization.len())?;
    for access in authorization {
        write_text(&mut out, &access.entity)?;
        write_strings(&mut out, &access.fields)?;
        write_strings(&mut out, &access.indexes)?;
        out.extend_from_slice(&access.maximum_rows.to_be_bytes());
    }
    (out.len() <= MAX_QUERY_ARTIFACT_BYTES).then_some(out)
}

fn write_count(out: &mut Vec<u8>, value: usize) -> Option<()> {
    out.extend_from_slice(&u32::try_from(value).ok()?.to_be_bytes());
    Some(())
}

fn write_text(out: &mut Vec<u8>, value: &str) -> Option<()> {
    write_count(out, value.len())?;
    out.extend_from_slice(value.as_bytes());
    Some(())
}

fn write_strings(out: &mut Vec<u8>, values: &[String]) -> Option<()> {
    write_count(out, values.len())?;
    for value in values {
        write_text(out, value)?;
    }
    Some(())
}

fn build_explain(
    partition_parameter: &str,
    steps: &[QueryAccessStep],
    authorization: &[AuthorizationEntityAccess],
) -> QueryPlanExplain {
    let mut lines = vec![format!("partition ${partition_parameter}")];
    for step in steps {
        let access = match &step.access {
            QueryAccessKind::Point { .. } => "primary-key".to_owned(),
            QueryAccessKind::Index {
                index, direction, ..
            } => format!(
                "index {index} {}",
                match direction {
                    AccessDirection::Forward => "forward",
                    AccessDirection::Reverse => "reverse",
                }
            ),
        };
        lines.push(format!(
            "{}: {} via {} max {}",
            step.binding, step.entity, access, step.maximum_rows
        ));
    }
    for access in authorization {
        lines.push(format!(
            "authorize {} fields [{}] indexes [{}] max {}",
            access.entity,
            access.fields.join(","),
            access.indexes.join(","),
            access.maximum_rows
        ));
    }
    QueryPlanExplain { lines }
}

#[allow(dead_code)]
fn _identity_type_guard(
    _: ContractLineage,
    _: ContractVersion,
    _: ContractBundleHash,
    _: EntityTypeId,
    _: FieldId,
    _: IndexId,
) {
}
