use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;

use riffdb_contract_ir::{ValueType, ValueTypeTag};
use riffdb_riffql_syntax::{
    BinaryOperator, CandidateSetExpression, CandidateSource, Cardinality, Document, Expression,
    FieldSelection, Literal, Path, RIFFQL_LANGUAGE_VERSION_BOUNDED_LIMIT_V1,
    RIFFQL_LANGUAGE_VERSION_BOUNDED_RESULT_PIPELINE_V1, RIFFQL_LANGUAGE_VERSION_EXACT_AGGREGATE_V1,
    RIFFQL_LANGUAGE_VERSION_PARTITION_SET_V1, Selection, Span, TypeReference, format_query,
};
use riffdb_types::{
    AggregateSemanticIdentityV1, ContractBundleHash, ContractLineage, ContractVersion,
    EntityTypeId, FieldId, LongPatternOperatorV1, MAX_DECIMAL_PRECISION,
    ProjectionProviderCapabilitiesV1, ProjectionProviderDescriptorV1, ProjectionProviderKindV1,
    ProjectionProviderPolicyModeV1, ProjectionProviderPostureV1, ProjectionProviderStateIdentityV1,
    ProjectionProviderStaticBoundsV1,
};

use crate::{
    AggregateExecutionBudgetV1, CandidateBindingV1, CandidateSetOperatorV1, CandidateSourceV1,
    EntitySymbol, LongPatternCandidateV1, MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1,
    MAX_AGGREGATE_DISTINCT_VALUES_V1, MAX_AGGREGATE_STATE_BYTES_V1, MAX_QUERY_ARTIFACT_BYTES,
    MAX_SOURCE_MAP_ENTRIES, NamedFieldSchema, NamedParameterSchema, NamedQuerySchemas,
    NamedResultBranchSchema, NamedTypeSchema, OperationalAggregateFunctionV1,
    OperationalAggregateGroupKeyV1, OperationalAggregateMeasureV1, OperationalAggregateV1,
    PageBound, QUERY_IR_VERSION_BOUNDED_LIMIT_V1, QUERY_IR_VERSION_BOUNDED_RESULT_PIPELINE_V1,
    QUERY_IR_VERSION_EXACT_AGGREGATE_V1, QUERY_IR_VERSION_OPERATIONAL_AGGREGATE_V1,
    QUERY_IR_VERSION_PARTITION_SET_V1, QUERY_IR_VERSION_PROJECTED_VECTOR_V1,
    QUERY_IR_VERSION_SECRET_OUTPUT_V1, QUERY_IR_VERSION_V1, QueryDiagnostic, QueryDiagnosticCode,
    QueryDiagnosticStage, QueryDiagnostics, SymbolicCatalog, page_take_within_scan_bound,
    source_aggregate_semantic_identity,
};

const IR_MAGIC: &[u8] = b"RIFFDB-QUERY-SURFACE\0";
const OPERATIONAL_AGGREGATES_MAGIC: &[u8] = b"OPERATIONAL-AGGREGATES\0";
const CANDIDATE_BINDINGS_MAGIC: &[u8] = b"CANDIDATE-BINDINGS\0";
const LONG_PATTERN_CANDIDATES_MAGIC: &[u8] = b"LONG-PATTERN-CANDIDATES\0";

fn long_pattern_invocations(
    expression: &Expression,
) -> Vec<(String, LongPatternOperatorV1, String)> {
    let mut output = Vec::new();
    collect_long_pattern_invocations(expression, &mut output);
    output
}

fn collect_long_pattern_invocations(
    expression: &Expression,
    output: &mut Vec<(String, LongPatternOperatorV1, String)>,
) {
    let Expression::Binary {
        operator,
        left,
        right,
    } = expression
    else {
        return;
    };
    if operator.value == BinaryOperator::And {
        collect_long_pattern_invocations(&left.value, output);
        collect_long_pattern_invocations(&right.value, output);
        return;
    }
    let operator = match operator.value {
        BinaryOperator::Equal => LongPatternOperatorV1::Equals,
        BinaryOperator::StartsWith => LongPatternOperatorV1::StartsWith,
        BinaryOperator::EndsWith => LongPatternOperatorV1::EndsWith,
        BinaryOperator::Contains => LongPatternOperatorV1::Contains,
        BinaryOperator::Like => LongPatternOperatorV1::Like,
        BinaryOperator::ILike => LongPatternOperatorV1::ILike,
        BinaryOperator::NotLike => LongPatternOperatorV1::NotLike,
        BinaryOperator::NotILike => LongPatternOperatorV1::NotILike,
        _ => return,
    };
    let (Expression::Path(path), Expression::Parameter(parameter)) = (&left.value, &right.value)
    else {
        return;
    };
    if let Some(field) = path.0.last() {
        output.push((
            field.value.as_str().to_owned(),
            operator,
            parameter.value.as_str().to_owned(),
        ));
    }
}

/// Exact contract identity repeated by resolved query artifacts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactContractIdentity {
    lineage: ContractLineage,
    version: ContractVersion,
    bundle_hash: ContractBundleHash,
}

impl ExactContractIdentity {
    /// Constructs one exact checked identity.
    #[must_use]
    pub const fn new(
        lineage: ContractLineage,
        version: ContractVersion,
        bundle_hash: ContractBundleHash,
    ) -> Self {
        Self {
            lineage,
            version,
            bundle_hash,
        }
    }

    /// Exact lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Exact application contract version.
    #[must_use]
    pub const fn version(&self) -> ContractVersion {
        self.version
    }

    /// Exact bundle hash.
    #[must_use]
    pub const fn bundle_hash(&self) -> ContractBundleHash {
        self.bundle_hash
    }
}

/// Resolved binding identity used by the later planner.
#[derive(Clone, Eq, PartialEq)]
pub struct BindingSymbol {
    name: String,
    entity_name: String,
    entity_id: EntityTypeId,
    cardinality: Cardinality,
}

impl std::fmt::Debug for BindingSymbol {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BindingSymbol")
            .field("name", &self.name)
            .field("entity_name", &self.entity_name)
            .field("cardinality", &self.cardinality)
            .finish()
    }
}

impl BindingSymbol {
    /// Query-local binding name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Exact contract entity source name.
    #[must_use]
    pub fn entity_name(&self) -> &str {
        &self.entity_name
    }

    /// Declared expected cardinality.
    #[must_use]
    pub const fn cardinality(&self) -> Cardinality {
        self.cardinality
    }

    /// Compiler-internal entity identity.
    #[doc(hidden)]
    #[must_use]
    pub const fn internal_entity_id(&self) -> EntityTypeId {
        self.entity_id
    }
}

/// Closed source-map symbol kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourceSymbolKind {
    /// Query parameter.
    Parameter,
    /// Contract entity.
    Entity,
    /// Contract field.
    Field,
    /// Contract enum.
    Enum,
    /// Contract enum variant.
    EnumVariant,
    /// Query-local binding.
    Binding,
    /// Query-local aggregate result.
    Aggregate,
    /// Aggregate measure alias.
    AggregateMeasure,
    /// Query-local non-output candidate binding.
    Candidate,
    /// Declared ordinary/provider candidate access.
    CandidateAccess,
    /// Returned field alias.
    ResultField,
    /// Exact stored secret field intentionally returned by one result slot.
    SecretOutput,
}

/// One compiler-derived exact secret output requirement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SecretOutputRequirement {
    binding: String,
    entity: String,
    entity_id: EntityTypeId,
    field: String,
    field_id: FieldId,
    result_path: Vec<String>,
    declaration_span: Span,
}

impl SecretOutputRequirement {
    /// Query-local binding named by the declaration.
    #[must_use]
    pub fn binding(&self) -> &str {
        &self.binding
    }

    /// Exact contract entity name.
    #[must_use]
    pub fn entity(&self) -> &str {
        &self.entity
    }

    /// Exact contract field name.
    #[must_use]
    pub fn field(&self) -> &str {
        &self.field
    }

    /// Output branch and nested result-slot path.
    #[must_use]
    pub fn result_path(&self) -> &[String] {
        &self.result_path
    }

    /// Source span of the exact declared secret path.
    #[must_use]
    pub const fn declaration_span(&self) -> Span {
        self.declaration_span
    }

    /// Compiler-internal exact entity identity.
    #[doc(hidden)]
    #[must_use]
    pub const fn internal_entity_id(&self) -> EntityTypeId {
        self.entity_id
    }

    /// Compiler-internal exact field identity.
    #[doc(hidden)]
    #[must_use]
    pub const fn internal_field_id(&self) -> FieldId {
        self.field_id
    }
}

/// One safe source-to-symbol association.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceMapEntry {
    span: Span,
    kind: SourceSymbolKind,
    symbolic_path: Vec<String>,
}

impl SourceMapEntry {
    /// Source span.
    #[must_use]
    pub const fn span(&self) -> Span {
        self.span
    }

    /// Resolved symbol kind.
    #[must_use]
    pub const fn kind(&self) -> SourceSymbolKind {
        self.kind
    }

    /// Safe exact source-name path.
    #[must_use]
    pub fn symbolic_path(&self) -> &[String] {
        &self.symbolic_path
    }
}

/// Bounded source map in source order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuerySourceMap(Vec<SourceMapEntry>);

impl QuerySourceMap {
    /// Entries in source order.
    #[must_use]
    pub fn entries(&self) -> &[SourceMapEntry] {
        &self.0
    }
}

/// Canonical exact-contract typed query surface.
#[derive(Clone, Eq, PartialEq)]
pub struct ResolvedQueryV1 {
    identity: ExactContractIdentity,
    name: Option<String>,
    bindings: Vec<BindingSymbol>,
    candidates: Vec<CandidateBindingV1>,
    aggregates: Vec<OperationalAggregateV1>,
    secret_outputs: Vec<SecretOutputRequirement>,
    projected: bool,
    schemas: NamedQuerySchemas,
    source_map: QuerySourceMap,
    canonical_bytes: Vec<u8>,
}

impl std::fmt::Debug for ResolvedQueryV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResolvedQueryV1")
            .field("contract_lineage", &self.identity.lineage().as_str())
            .field("contract_version", &self.identity.version())
            .field("name", &self.name)
            .field("bindings", &self.bindings)
            .field("candidates", &self.candidates)
            .field("aggregates", &self.aggregates)
            .field("secret_outputs", &self.secret_outputs)
            .field("projected", &self.projected)
            .field("schemas", &self.schemas)
            .field("source_map_entries", &self.source_map.0.len())
            .field("canonical_length", &self.canonical_bytes.len())
            .finish()
    }
}

impl ResolvedQueryV1 {
    /// Query IR version.
    #[must_use]
    pub fn ir_version(&self) -> u32 {
        if self.has_bounded_set() {
            QUERY_IR_VERSION_PARTITION_SET_V1
        } else if !self.candidates.is_empty() || self.has_extended_bounded_limit() {
            QUERY_IR_VERSION_BOUNDED_RESULT_PIPELINE_V1
        } else if self.has_bounded_limit() {
            QUERY_IR_VERSION_BOUNDED_LIMIT_V1
        } else if self.projected {
            QUERY_IR_VERSION_PROJECTED_VECTOR_V1
        } else if self.has_exact_aggregate_core() {
            QUERY_IR_VERSION_EXACT_AGGREGATE_V1
        } else if self.secret_outputs.is_empty() {
            // Declaration-free member access programs retain their original
            // V1 identity. Operational and aggregate versions belong to the
            // enclosing family codec, not this member program.
            QUERY_IR_VERSION_V1
        } else {
            QUERY_IR_VERSION_SECRET_OUTPUT_V1
        }
    }

    /// Whether any public parameter uses ADR-0175's explicitly bounded set type.
    #[must_use]
    pub fn has_bounded_set(&self) -> bool {
        self.schemas
            .parameters()
            .iter()
            .any(|parameter| matches!(parameter.value_type(), NamedTypeSchema::BoundedSet { .. }))
    }

    /// Whether any public parameter uses ADR-0158's bounded page-limit type.
    #[must_use]
    pub fn has_bounded_limit(&self) -> bool {
        self.schemas
            .parameters()
            .iter()
            .any(|parameter| matches!(parameter.value_type(), NamedTypeSchema::BoundedLimit { .. }))
    }

    /// Whether any bounded limit requires ADR-0174's enlarged page identity.
    #[must_use]
    pub fn has_extended_bounded_limit(&self) -> bool {
        self.schemas.parameters().iter().any(|parameter| {
            matches!(
                parameter.value_type(),
                NamedTypeSchema::BoundedLimit { maximum }
                    if *maximum > riffdb_types::MAX_APPLICATION_QUERY_PAGE_ROWS_BOUNDED_LIMIT_V1
            )
        })
    }

    /// Whether this surface requires ADR-0174's V11/V14 bounded-result family.
    #[must_use]
    pub fn has_bounded_result_pipeline(&self) -> bool {
        !self.candidates.is_empty() || self.has_extended_bounded_limit()
    }

    /// Whether the surface contains an ADR-0152 additive aggregate semantic.
    #[must_use]
    pub fn has_exact_aggregate_core(&self) -> bool {
        self.aggregates.iter().any(|aggregate| {
            aggregate.measures().iter().any(|measure| {
                matches!(
                    measure.function().semantic_identity(),
                    AggregateSemanticIdentityV1::CountPresent
                        | AggregateSemanticIdentityV1::CountDistinct
                        | AggregateSemanticIdentityV1::CountDistinctPresent
                        | AggregateSemanticIdentityV1::Mean
                        | AggregateSemanticIdentityV1::Any
                        | AggregateSemanticIdentityV1::All
                )
            })
        })
    }

    /// Exact contract identity.
    #[must_use]
    pub const fn contract(&self) -> &ExactContractIdentity {
        &self.identity
    }

    /// Optional query declaration name.
    #[must_use]
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Resolved bindings in source order.
    #[must_use]
    pub fn bindings(&self) -> &[BindingSymbol] {
        &self.bindings
    }

    /// Compiler-checked non-output candidate bindings in source order.
    #[must_use]
    pub fn candidates(&self) -> &[CandidateBindingV1] {
        &self.candidates
    }

    /// Compiler-resolved bounded aggregate declarations.
    #[must_use]
    pub fn aggregates(&self) -> &[OperationalAggregateV1] {
        &self.aggregates
    }

    /// Compiler-derived exact secret outputs in result traversal order.
    #[must_use]
    pub fn secret_outputs(&self) -> &[SecretOutputRequirement] {
        &self.secret_outputs
    }

    /// Name-addressed parameter/result schemas.
    #[must_use]
    pub const fn schemas(&self) -> &NamedQuerySchemas {
        &self.schemas
    }

    /// Safe source map.
    #[must_use]
    pub const fn source_map(&self) -> &QuerySourceMap {
        &self.source_map
    }

    /// Canonical typed surface bytes.
    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }
}

/// Resolves a parsed query entirely through one exact symbolic contract.
pub fn resolve_query_surface(
    document: &Document,
    catalog: &SymbolicCatalog,
) -> Result<ResolvedQueryV1, QueryDiagnostics> {
    Resolver::new(catalog).resolve(document)
}

#[derive(Clone)]
struct ResolvedBinding<'a> {
    symbol: BindingSymbol,
    entity: &'a EntitySymbol,
    take: Option<PageBound>,
}

#[derive(Clone)]
struct ResolvedAggregateSelection {
    fields: BTreeMap<String, NamedTypeSchema>,
    grouped: bool,
    maximum_groups: PageBound,
}

/// Which source clause supplied a page bound, so its diagnostics name the
/// construct the author actually wrote (`take 500` versus `nearest(.., 500)`).
#[derive(Clone, Copy)]
enum PageBoundClause {
    Take,
    NearestK,
}

struct Resolver<'a> {
    catalog: &'a SymbolicCatalog,
    parameters: BTreeMap<String, NamedTypeSchema>,
    // Keep semantic parameter types separate from compatibility-preserved
    // name-addressed schemas so field references and direct bounded strings
    // participate in provider checks identically.
    parameter_value_types: BTreeMap<String, ValueType>,
    bindings: BTreeMap<String, ResolvedBinding<'a>>,
    expansion_drivers: BTreeMap<String, String>,
    candidates: BTreeMap<String, CandidateBindingV1>,
    candidate_consumers: BTreeMap<String, usize>,
    aggregates: BTreeMap<String, ResolvedAggregateSelection>,
    source_map: Vec<SourceMapEntry>,
    secret_outputs: Vec<SecretOutputRequirement>,
}

impl<'a> Resolver<'a> {
    fn new(catalog: &'a SymbolicCatalog) -> Self {
        Self {
            catalog,
            parameters: BTreeMap::new(),
            parameter_value_types: BTreeMap::new(),
            bindings: BTreeMap::new(),
            expansion_drivers: BTreeMap::new(),
            candidates: BTreeMap::new(),
            candidate_consumers: BTreeMap::new(),
            aggregates: BTreeMap::new(),
            source_map: Vec::new(),
            secret_outputs: Vec::new(),
        }
    }

    fn resolve(mut self, document: &Document) -> Result<ResolvedQueryV1, QueryDiagnostics> {
        if document.name.is_none()
            && let Some(reveal) = first_secret_output_declaration(&document.body.selection)
        {
            return Err(self.diagnostic(
                QueryDiagnosticCode::SecretOutputDeclaration,
                reveal.span,
                names(&reveal.value),
                "secret outputs are available only in immutable named queries",
            ));
        }
        let mut parameter_schemas = Vec::with_capacity(document.parameters.len());
        for parameter in &document.parameters {
            let name = parameter.name.value.as_str();
            if self.parameters.contains_key(name) {
                return Err(self.diagnostic(
                    QueryDiagnosticCode::DuplicateName,
                    parameter.name.span,
                    vec![name.to_owned()],
                    "duplicate query parameter",
                ));
            }
            let ty = self.resolve_type(&parameter.ty.value, parameter.ty.span)?;
            if let Some(value_type) = self.resolve_parameter_value_type(&parameter.ty.value) {
                self.parameter_value_types
                    .insert(name.to_owned(), value_type);
            }
            if matches!(
                ty,
                NamedTypeSchema::Limit | NamedTypeSchema::BoundedLimit { .. }
            ) && let Some(default) = parameter.default.as_ref()
            {
                let (declared_maximum, exceeded_summary) = match &ty {
                    NamedTypeSchema::Limit => (
                        crate::max_query_page_take(),
                        "Limit default exceeds the maximum page take of 65534 (scan ceiling reserves one row for the continuation probe)",
                    ),
                    NamedTypeSchema::BoundedLimit { maximum } => (
                        *maximum,
                        "Limit default exceeds its compiler-declared maximum",
                    ),
                    _ => unreachable!("matched limit schema"),
                };
                match &default.value {
                    Literal::Unsigned(value) => {
                        let parsed = value.parse::<u64>().ok().filter(|value| *value > 0);
                        match parsed {
                            Some(limit)
                                if page_take_within_scan_bound(limit)
                                    && limit <= declared_maximum => {}
                            Some(_) => {
                                return Err(self.diagnostic(
                                    QueryDiagnosticCode::ArtifactLimit,
                                    default.span,
                                    vec![name.to_owned()],
                                    exceeded_summary,
                                ));
                            }
                            None => {
                                return Err(self.diagnostic(
                                    QueryDiagnosticCode::InvalidType,
                                    default.span,
                                    vec![name.to_owned()],
                                    "Limit default is not a positive u64",
                                ));
                            }
                        }
                    }
                    _ => {
                        return Err(self.diagnostic(
                            QueryDiagnosticCode::InvalidType,
                            default.span,
                            vec![name.to_owned()],
                            "Limit default must be a positive unsigned literal",
                        ));
                    }
                }
            }
            self.parameters.insert(name.to_owned(), ty.clone());
            self.push_map(
                parameter.name.span,
                SourceSymbolKind::Parameter,
                vec![name.to_owned()],
            )?;
            parameter_schemas.push(NamedParameterSchema::new(
                name.to_owned(),
                ty,
                parameter.default.is_some(),
            ));
        }

        let mut candidate_symbols = Vec::with_capacity(document.body.candidates.len());
        for candidate in &document.body.candidates {
            let name = candidate.name.value.as_str();
            if self.parameters.contains_key(name) || self.candidates.contains_key(name) {
                return Err(self.diagnostic(
                    QueryDiagnosticCode::DuplicateName,
                    candidate.name.span,
                    vec![name.to_owned()],
                    "duplicate query-local candidate name",
                ));
            }
            let root = names(&candidate.root_key.value);
            let [root_entity_name, root_key_name] = root.as_slice() else {
                return Err(self.diagnostic(
                    QueryDiagnosticCode::InvalidPath,
                    candidate.root_key.span,
                    root,
                    "candidate root key must be Entity.primary_key_field",
                ));
            };
            let root_entity = self.catalog.entity(root_entity_name).ok_or_else(|| {
                self.diagnostic(
                    QueryDiagnosticCode::UnknownSymbol,
                    candidate.root_key.span,
                    vec![root_entity_name.clone()],
                    "unknown candidate root entity",
                )
            })?;
            let root_field = root_entity.field(root_key_name).ok_or_else(|| {
                self.diagnostic(
                    QueryDiagnosticCode::UnknownSymbol,
                    candidate.root_key.span,
                    root.clone(),
                    "unknown candidate root-key field",
                )
            })?;
            let partition_local_key = root_entity
                .primary_key()
                .iter()
                .filter(|field| field.as_str() != root_entity.partition_field())
                .collect::<Vec<_>>();
            if partition_local_key.as_slice() != [root_key_name] {
                return Err(self.diagnostic(
                    QueryDiagnosticCode::InvalidPath,
                    candidate.root_key.span,
                    root,
                    "candidate V1 requires one complete scalar root key",
                ));
            }
            let (operator, source_nodes, positive_source_count) = match &candidate.expression {
                CandidateSetExpression::Single(source) => {
                    (CandidateSetOperatorV1::Single, vec![source], 1)
                }
                CandidateSetExpression::Intersection(sources) => (
                    CandidateSetOperatorV1::Intersection,
                    sources.iter().collect(),
                    sources.len() as u8,
                ),
                CandidateSetExpression::Union(sources) => (
                    CandidateSetOperatorV1::Union,
                    sources.iter().collect(),
                    sources.len() as u8,
                ),
                CandidateSetExpression::Difference { positive, negative } => {
                    let mut sources = Vec::with_capacity(negative.len() + 1);
                    sources.push(positive);
                    sources.extend(negative);
                    (CandidateSetOperatorV1::Difference, sources, 1)
                }
            };
            let mut sources = Vec::with_capacity(source_nodes.len());
            for source in source_nodes {
                sources.push(self.resolve_candidate_source(
                    source,
                    root_entity,
                    root_key_name,
                    root_field.value_type(),
                )?);
            }
            let symbol = CandidateBindingV1::checked(
                name.to_owned(),
                root_entity_name.clone(),
                root_key_name.clone(),
                operator,
                sources,
                positive_source_count,
                candidate.within,
                candidate.refusal_outcome.value.as_str().to_owned(),
            )
            .ok_or_else(|| {
                self.diagnostic(
                    QueryDiagnosticCode::ArtifactLimit,
                    candidate.span,
                    vec![name.to_owned()],
                    "candidate binding exceeds its closed V1 shape",
                )
            })?;
            self.push_map(
                candidate.name.span,
                SourceSymbolKind::Candidate,
                vec![name.to_owned()],
            )?;
            self.candidate_consumers.insert(name.to_owned(), 0);
            self.candidates.insert(name.to_owned(), symbol.clone());
            candidate_symbols.push(symbol);
        }

        let mut binding_symbols = Vec::with_capacity(document.body.bindings.len());
        let mut expanded_bindings = BTreeSet::new();
        for binding in &document.body.bindings {
            let name = binding.name.value.as_str();
            if self.bindings.contains_key(name)
                || self.parameters.contains_key(name)
                || self.candidates.contains_key(name)
            {
                return Err(self.diagnostic(
                    QueryDiagnosticCode::DuplicateName,
                    binding.name.span,
                    vec![name.to_owned()],
                    "duplicate query-local name",
                ));
            }
            let entity_name = binding.entity.value.as_str();
            let entity = self.catalog.entity(entity_name).ok_or_else(|| {
                self.diagnostic(
                    QueryDiagnosticCode::UnknownSymbol,
                    binding.entity.span,
                    vec![entity_name.to_owned()],
                    "unknown contract entity",
                )
            })?;
            let symbol = BindingSymbol {
                name: name.to_owned(),
                entity_name: entity_name.to_owned(),
                entity_id: entity.internal_id(),
                cardinality: binding.cardinality.value,
            };
            self.push_map(
                binding.entity.span,
                SourceSymbolKind::Entity,
                vec![entity_name.to_owned()],
            )?;
            self.push_map(
                binding.name.span,
                SourceSymbolKind::Binding,
                vec![name.to_owned()],
            )?;
            let take = binding
                .take
                .as_ref()
                .map(|take| {
                    self.page_bound(&take.limit.value, take.limit.span, PageBoundClause::Take)
                })
                .transpose()?;
            // A nearest binding declares K instead of `take` (the parser
            // rejects combining them); K is that binding's checked page bound
            // (VEC-010: the query declares K), subject to the same page-take
            // ceiling as any other bounded many binding.
            let take = match (take, binding.nearest.as_ref()) {
                (None, Some(nearest)) => Some(self.page_bound(
                    &nearest.k.value,
                    nearest.k.span,
                    PageBoundClause::NearestK,
                )?),
                (take, _) => take,
            };
            self.bindings.insert(
                name.to_owned(),
                ResolvedBinding {
                    symbol: symbol.clone(),
                    entity,
                    take,
                },
            );
            binding_symbols.push(symbol);
            if let Some(expansion) = &binding.expansion {
                let driver_name = expansion.binding.value.as_str();
                let driver = self.bindings.get(driver_name).cloned().ok_or_else(|| {
                    self.diagnostic(
                        QueryDiagnosticCode::InvalidPath,
                        expansion.binding.span,
                        vec![driver_name.to_owned()],
                        "expansion driver must name an earlier binding",
                    )
                })?;
                if driver.symbol.cardinality != Cardinality::Many || driver.take.is_none() {
                    return Err(self.diagnostic(
                        QueryDiagnosticCode::InvalidPath,
                        expansion.binding.span,
                        vec![driver_name.to_owned()],
                        "expansion driver must be an earlier bounded many binding",
                    ));
                }
                if expanded_bindings.contains(driver_name) {
                    return Err(self.diagnostic(
                        QueryDiagnosticCode::InvalidPath,
                        expansion.binding.span,
                        vec![driver_name.to_owned()],
                        "relational expansion depth is limited to one",
                    ));
                }
                let item_name = expansion.item.value.as_str();
                if self.bindings.contains_key(item_name)
                    || self.parameters.contains_key(item_name)
                    || self.candidates.contains_key(item_name)
                {
                    return Err(self.diagnostic(
                        QueryDiagnosticCode::DuplicateName,
                        expansion.item.span,
                        vec![item_name.to_owned()],
                        "expansion item duplicates a query-local name",
                    ));
                }
                self.push_map(
                    expansion.item.span,
                    SourceSymbolKind::Binding,
                    vec![driver_name.to_owned()],
                )?;
                self.push_map(
                    expansion.binding.span,
                    SourceSymbolKind::Binding,
                    vec![driver_name.to_owned()],
                )?;
                self.bindings.insert(item_name.to_owned(), driver);
                let resolved = self.resolve_expression(
                    &binding.predicate.value,
                    binding.predicate.span,
                    entity,
                );
                self.bindings.remove(item_name);
                resolved?;
                self.expansion_drivers
                    .insert(name.to_owned(), driver_name.to_owned());
                expanded_bindings.insert(name.to_owned());
            } else {
                self.resolve_expression(&binding.predicate.value, binding.predicate.span, entity)?;
            }
        }

        for candidate in &candidate_symbols {
            if self.candidate_consumers.get(candidate.name()) != Some(&1) {
                return Err(self.diagnostic(
                    QueryDiagnosticCode::InvalidPath,
                    document
                        .body
                        .candidates
                        .iter()
                        .find(|source| source.name.value.as_str() == candidate.name())
                        .map_or(Span { start: 0, end: 0 }, |source| source.name.span),
                    vec![candidate.name().to_owned()],
                    "candidate binding must have exactly one root consumer",
                ));
            }
        }

        let mut aggregate_symbols = Vec::with_capacity(document.body.aggregates.len());
        for aggregate in &document.body.aggregates {
            let name = aggregate.name.value.as_str();
            if self.parameters.contains_key(name)
                || self.bindings.contains_key(name)
                || self.aggregates.contains_key(name)
            {
                return Err(self.diagnostic(
                    QueryDiagnosticCode::DuplicateName,
                    aggregate.name.span,
                    vec![name.to_owned()],
                    "duplicate query-local aggregate name",
                ));
            }
            let source_name = aggregate.source.value.as_str();
            let source = self.bindings.get(source_name).cloned().ok_or_else(|| {
                self.diagnostic(
                    QueryDiagnosticCode::UnknownSymbol,
                    aggregate.source.span,
                    vec![source_name.to_owned()],
                    "unknown aggregate source binding",
                )
            })?;
            if source.symbol.cardinality != Cardinality::Many {
                return Err(self.diagnostic(
                    QueryDiagnosticCode::InvalidPath,
                    aggregate.source.span,
                    vec![source_name.to_owned()],
                    "aggregate source must be one bounded many binding",
                ));
            }

            self.push_map(
                aggregate.name.span,
                SourceSymbolKind::Aggregate,
                vec![name.to_owned()],
            )?;
            self.push_map(
                aggregate.source.span,
                SourceSymbolKind::Binding,
                vec![source_name.to_owned()],
            )?;

            let mut result_names = BTreeSet::new();
            let mut result_fields = BTreeMap::new();
            let mut group_keys = Vec::with_capacity(aggregate.group_by.len());
            for group in &aggregate.group_by {
                let (field_name, field_type) =
                    self.resolve_aggregate_field(&source, source_name, &group.value, group.span)?;
                if !result_names.insert(field_name.clone()) {
                    return Err(self.diagnostic(
                        QueryDiagnosticCode::DuplicateName,
                        group.span,
                        vec![name.to_owned(), field_name],
                        "duplicate aggregate result field",
                    ));
                }
                let named = self.named_type(&field_type, group.span)?;
                result_fields.insert(field_name.clone(), named.clone());
                group_keys.push(
                    OperationalAggregateGroupKeyV1::checked(field_name, named)
                        .ok_or_else(|| self.aggregate_invariant(group.span))?,
                );
            }

            let mut measures = Vec::with_capacity(aggregate.measures.len());
            for measure in &aggregate.measures {
                let alias = measure.alias.value.as_str().to_owned();
                if !result_names.insert(alias.clone()) {
                    return Err(self.diagnostic(
                        QueryDiagnosticCode::DuplicateName,
                        measure.alias.span,
                        vec![name.to_owned(), alias],
                        "duplicate aggregate result field",
                    ));
                }
                let semantic = source_aggregate_semantic_identity(measure.function.value);
                let (function, input_field, result_type) = match semantic {
                    // `exact_count` has the same public scalar schema as the
                    // bounded operational count. Its whole-population
                    // semantics are retained by the exact result-set plan;
                    // the ordinary operational compiler rejects it before
                    // member lowering.
                    AggregateSemanticIdentityV1::Count
                    | AggregateSemanticIdentityV1::ExactCount => (
                        OperationalAggregateFunctionV1::Count,
                        None,
                        NamedTypeSchema::Scalar("u64".to_owned()),
                    ),
                    AggregateSemanticIdentityV1::Sum => {
                        let field = measure
                            .field
                            .as_ref()
                            .ok_or_else(|| self.aggregate_invariant(measure.function.span))?;
                        let (field_name, field_type) = self.resolve_aggregate_field(
                            &source,
                            source_name,
                            &field.value,
                            field.span,
                        )?;
                        (
                            OperationalAggregateFunctionV1::Sum,
                            Some(field_name),
                            self.aggregate_sum_type(&field_type, field.span)?,
                        )
                    }
                    AggregateSemanticIdentityV1::Min | AggregateSemanticIdentityV1::Max => {
                        let field = measure
                            .field
                            .as_ref()
                            .ok_or_else(|| self.aggregate_invariant(measure.function.span))?;
                        let (field_name, field_type) = self.resolve_aggregate_field(
                            &source,
                            source_name,
                            &field.value,
                            field.span,
                        )?;
                        if matches!(field_type.tag(), ValueTypeTag::List | ValueTypeTag::Record) {
                            return Err(self.diagnostic(
                                QueryDiagnosticCode::InvalidType,
                                field.span,
                                vec![source.entity.name().to_owned(), field_name],
                                "min/max input must be an ordered scalar field",
                            ));
                        }
                        let function = if semantic == AggregateSemanticIdentityV1::Min {
                            OperationalAggregateFunctionV1::Min
                        } else {
                            OperationalAggregateFunctionV1::Max
                        };
                        let result = NamedTypeSchema::Optional(Box::new(
                            self.named_type(&field_type, field.span)?,
                        ));
                        (function, Some(field_name), result)
                    }
                    AggregateSemanticIdentityV1::CountPresent
                    | AggregateSemanticIdentityV1::CountDistinct
                    | AggregateSemanticIdentityV1::CountDistinctPresent => {
                        let field = measure
                            .field
                            .as_ref()
                            .ok_or_else(|| self.aggregate_invariant(measure.function.span))?;
                        let (field_name, field_type) = self.resolve_aggregate_field(
                            &source,
                            source_name,
                            &field.value,
                            field.span,
                        )?;
                        if matches!(field_type.tag(), ValueTypeTag::List | ValueTypeTag::Record) {
                            return Err(self.diagnostic(
                                QueryDiagnosticCode::InvalidType,
                                field.span,
                                vec![source.entity.name().to_owned(), field_name],
                                "count_present/distinct input must be a canonical scalar field",
                            ));
                        }
                        let function = match semantic {
                            AggregateSemanticIdentityV1::CountPresent => {
                                OperationalAggregateFunctionV1::CountPresent
                            }
                            AggregateSemanticIdentityV1::CountDistinct => {
                                OperationalAggregateFunctionV1::CountDistinct
                            }
                            AggregateSemanticIdentityV1::CountDistinctPresent => {
                                OperationalAggregateFunctionV1::CountDistinctPresent
                            }
                            _ => return Err(self.aggregate_invariant(measure.function.span)),
                        };
                        (
                            function,
                            Some(field_name),
                            NamedTypeSchema::Scalar("u64".to_owned()),
                        )
                    }
                    AggregateSemanticIdentityV1::Mean => {
                        let field = measure
                            .field
                            .as_ref()
                            .ok_or_else(|| self.aggregate_invariant(measure.function.span))?;
                        let (field_name, field_type) = self.resolve_aggregate_field(
                            &source,
                            source_name,
                            &field.value,
                            field.span,
                        )?;
                        let total = self.aggregate_sum_type(&field_type, field.span)?;
                        (
                            OperationalAggregateFunctionV1::Mean,
                            Some(field_name),
                            NamedTypeSchema::Record(vec![
                                NamedFieldSchema::new("total".to_owned(), total),
                                NamedFieldSchema::new(
                                    "count".to_owned(),
                                    NamedTypeSchema::Scalar("u64".to_owned()),
                                ),
                            ]),
                        )
                    }
                    AggregateSemanticIdentityV1::Any | AggregateSemanticIdentityV1::All => {
                        let field = measure
                            .field
                            .as_ref()
                            .ok_or_else(|| self.aggregate_invariant(measure.function.span))?;
                        let (field_name, field_type) = self.resolve_aggregate_field(
                            &source,
                            source_name,
                            &field.value,
                            field.span,
                        )?;
                        if field_type.tag() != ValueTypeTag::Bool {
                            return Err(self.diagnostic(
                                QueryDiagnosticCode::InvalidType,
                                field.span,
                                vec![source.entity.name().to_owned(), field_name],
                                "any/all input must be a required bool field",
                            ));
                        }
                        (
                            if semantic == AggregateSemanticIdentityV1::Any {
                                OperationalAggregateFunctionV1::Any
                            } else {
                                OperationalAggregateFunctionV1::All
                            },
                            Some(field_name),
                            NamedTypeSchema::Scalar("bool".to_owned()),
                        )
                    }
                };
                result_fields.insert(alias.clone(), result_type.clone());
                self.push_map(
                    measure.alias.span,
                    SourceSymbolKind::AggregateMeasure,
                    vec![name.to_owned(), alias.clone()],
                )?;
                measures.push(
                    OperationalAggregateMeasureV1::checked(
                        alias,
                        function,
                        input_field,
                        result_type,
                    )
                    .ok_or_else(|| self.aggregate_invariant(measure.alias.span))?,
                );
            }

            let maximum_groups = if group_keys.is_empty() {
                PageBound::Literal(1)
            } else {
                source.take.clone().ok_or_else(|| {
                    self.diagnostic(
                        QueryDiagnosticCode::InvalidPath,
                        aggregate.source.span,
                        vec![source_name.to_owned()],
                        "grouped aggregate source is missing its checked row bound",
                    )
                })?
            };
            let maximum_input_rows = match source.take.as_ref() {
                Some(PageBound::Literal(value)) => *value,
                Some(PageBound::Parameter(_)) => crate::max_query_page_take(),
                Some(PageBound::BoundedParameter { maximum, .. }) => *maximum,
                None => return Err(self.aggregate_invariant(aggregate.source.span)),
            };
            let maximum_arithmetic_operations = maximum_input_rows
                .checked_mul(
                    u64::try_from(measures.len())
                        .map_err(|_| self.aggregate_invariant(aggregate.name.span))?,
                )
                .and_then(|value| u32::try_from(value).ok())
                .filter(|value| *value <= MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1)
                .ok_or_else(|| self.aggregate_invariant(aggregate.name.span))?;
            let execution_budget = AggregateExecutionBudgetV1::checked(
                MAX_AGGREGATE_DISTINCT_VALUES_V1,
                MAX_AGGREGATE_STATE_BYTES_V1,
                maximum_arithmetic_operations,
            )
            .ok_or_else(|| self.aggregate_invariant(aggregate.name.span))?;
            let resolved = OperationalAggregateV1::checked(
                name.to_owned(),
                source_name.to_owned(),
                source.entity.name().to_owned(),
                group_keys,
                measures,
                maximum_groups.clone(),
                execution_budget,
            )
            .ok_or_else(|| self.aggregate_invariant(aggregate.name.span))?;
            self.aggregates.insert(
                name.to_owned(),
                ResolvedAggregateSelection {
                    fields: result_fields,
                    grouped: !resolved.group_keys().is_empty(),
                    maximum_groups,
                },
            );
            aggregate_symbols.push(resolved);
        }

        let branch_name = document
            .body
            .outcome
            .as_ref()
            .map_or("Result", |outcome| outcome.value.as_str())
            .to_owned();
        let fields = self.resolve_selection(
            &document.body.selection,
            None,
            std::slice::from_ref(&branch_name),
        )?;
        let declared = document
            .body
            .outcomes
            .iter()
            .map(|outcome| outcome.value.as_str())
            .collect::<BTreeSet<_>>();
        if !declared.is_empty() && !declared.contains(branch_name.as_str()) {
            return Err(self.diagnostic(
                QueryDiagnosticCode::InvalidPath,
                document
                    .body
                    .outcome
                    .as_ref()
                    .map_or(Span { start: 0, end: 0 }, |outcome| outcome.span),
                vec![branch_name],
                "returned branch is absent from the declared outcome union",
            ));
        }
        let mut results = vec![NamedResultBranchSchema::new(branch_name, fields)];
        for outcome in &document.body.outcomes {
            if results
                .iter()
                .any(|branch| branch.name() == outcome.value.as_str())
            {
                continue;
            }
            results.push(NamedResultBranchSchema::new(
                outcome.value.as_str().to_owned(),
                Vec::new(),
            ));
        }
        let schemas = NamedQuerySchemas::new(parameter_schemas, results);
        let canonical_bytes = canonical_surface(
            document,
            self.catalog.identity(),
            &binding_symbols,
            &candidate_symbols,
            &aggregate_symbols,
            &schemas,
            &self.secret_outputs,
        )?;
        self.source_map
            .sort_by_key(|entry| (entry.span.start, entry.span.end));
        Ok(ResolvedQueryV1 {
            identity: self.catalog.identity().clone(),
            name: document
                .name
                .as_ref()
                .map(|name| name.value.as_str().to_owned()),
            bindings: binding_symbols,
            candidates: candidate_symbols,
            aggregates: aggregate_symbols,
            secret_outputs: self.secret_outputs,
            projected: document.projected_source.is_some(),
            schemas,
            source_map: QuerySourceMap(self.source_map),
            canonical_bytes,
        })
    }

    fn resolve_aggregate_field(
        &mut self,
        source: &ResolvedBinding<'a>,
        source_name: &str,
        path: &Path,
        span: Span,
    ) -> Result<(String, ValueType), QueryDiagnostics> {
        let segments = names(path);
        let field_name = match segments.as_slice() {
            [field] => field,
            [binding, field] if binding == source_name => field,
            _ => {
                return Err(self.diagnostic(
                    QueryDiagnosticCode::InvalidPath,
                    span,
                    segments,
                    "aggregate field must belong to its declared source binding",
                ));
            }
        };
        let field = source.entity.field(field_name).ok_or_else(|| {
            self.diagnostic(
                QueryDiagnosticCode::UnknownSymbol,
                span,
                vec![source.entity.name().to_owned(), field_name.clone()],
                "unknown aggregate source field",
            )
        })?;
        self.push_map(
            span,
            SourceSymbolKind::Field,
            vec![source.entity.name().to_owned(), field_name.clone()],
        )?;
        Ok((field_name.clone(), field.value_type().clone()))
    }

    fn aggregate_sum_type(
        &self,
        value_type: &ValueType,
        span: Span,
    ) -> Result<NamedTypeSchema, QueryDiagnostics> {
        let widened_precision = u16::from(MAX_DECIMAL_PRECISION) + 1;
        match value_type.tag() {
            ValueTypeTag::I64 | ValueTypeTag::U64 => Ok(NamedTypeSchema::Scalar(format!(
                "decimal<{widened_precision},0>"
            ))),
            ValueTypeTag::Decimal => {
                let Some(spec) = value_type.decimal_spec() else {
                    return Err(self.diagnostic(
                        QueryDiagnosticCode::InvalidType,
                        span,
                        Vec::new(),
                        "decimal sum input is missing its exact scale",
                    ));
                };
                let scale = spec.scale();
                Ok(NamedTypeSchema::Scalar(format!(
                    "decimal<{widened_precision},{scale}>"
                )))
            }
            _ => Err(self.diagnostic(
                QueryDiagnosticCode::InvalidType,
                span,
                Vec::new(),
                "sum input must be i64, u64, or decimal; money sums require an explicit currency rule",
            )),
        }
    }

    fn aggregate_invariant(&self, span: Span) -> QueryDiagnostics {
        self.diagnostic(
            QueryDiagnosticCode::InvalidPath,
            span,
            Vec::new(),
            "aggregate declaration failed its closed structural invariant",
        )
    }

    fn resolve_type(
        &mut self,
        ty: &TypeReference,
        span: Span,
    ) -> Result<NamedTypeSchema, QueryDiagnostics> {
        match ty {
            TypeReference::Named(path) => {
                let segments = names(path);
                let value_type = match segments.as_slice() {
                    [enumeration] => {
                        if enumeration == "u64" {
                            return Ok(NamedTypeSchema::Scalar("u64".to_owned()));
                        }
                        let symbol = self.catalog.enumeration(enumeration).ok_or_else(|| {
                            self.diagnostic(
                                QueryDiagnosticCode::UnknownSymbol,
                                span,
                                segments.clone(),
                                "unknown contract enum type",
                            )
                        })?;
                        self.push_map(span, SourceSymbolKind::Enum, segments.clone())?;
                        ValueType::enumeration(symbol.internal_id())
                    }
                    [entity, field] => {
                        let entity_symbol = self.catalog.entity(entity).ok_or_else(|| {
                            self.diagnostic(
                                QueryDiagnosticCode::UnknownSymbol,
                                span,
                                segments.clone(),
                                "unknown contract entity in field-referenced type",
                            )
                        })?;
                        let field_symbol = entity_symbol.field(field).ok_or_else(|| {
                            self.diagnostic(
                                QueryDiagnosticCode::UnknownSymbol,
                                span,
                                segments.clone(),
                                "unknown contract field in field-referenced type",
                            )
                        })?;
                        self.push_map(span, SourceSymbolKind::Field, segments.clone())?;
                        field_symbol.value_type().clone()
                    }
                    _ => {
                        return Err(self.diagnostic(
                            QueryDiagnosticCode::InvalidType,
                            span,
                            segments,
                            "query type must be an enum or Entity.field reference",
                        ));
                    }
                };
                self.named_type(&value_type, span)
            }
            TypeReference::Optional(inner) => Ok(NamedTypeSchema::Optional(Box::new(
                self.resolve_type(&inner.value, inner.span)?,
            ))),
            TypeReference::Set(inner) => {
                let inner = self.resolve_type(&inner.value, inner.span)?;
                if !matches!(inner, NamedTypeSchema::Scalar(_)) {
                    return Err(self.diagnostic(
                        QueryDiagnosticCode::InvalidType,
                        span,
                        Vec::new(),
                        "query set element must be a contract scalar or enum",
                    ));
                }
                Ok(NamedTypeSchema::Set(Box::new(inner)))
            }
            TypeReference::BoundedSet { element, maximum } => {
                let inner = self.resolve_type(&element.value, element.span)?;
                if !matches!(inner, NamedTypeSchema::Scalar(_)) {
                    return Err(self.diagnostic(
                        QueryDiagnosticCode::InvalidType,
                        span,
                        Vec::new(),
                        "bounded query set element must be a contract scalar or enum",
                    ));
                }
                Ok(NamedTypeSchema::BoundedSet {
                    element: Box::new(inner),
                    maximum: *maximum,
                })
            }
            TypeReference::Cursor => Ok(NamedTypeSchema::Cursor),
            TypeReference::Limit => Ok(NamedTypeSchema::Limit),
            TypeReference::BoundedLimit(maximum) => {
                Ok(NamedTypeSchema::BoundedLimit { maximum: *maximum })
            }
            TypeReference::BoundedString(_) => Ok(NamedTypeSchema::Scalar("string".to_owned())),
        }
    }

    fn resolve_parameter_value_type(&self, ty: &TypeReference) -> Option<ValueType> {
        match ty {
            TypeReference::Named(path) => match path.0.as_slice() {
                [name] if name.value.as_str() == "u64" => Some(ValueType::u64()),
                [name] => self
                    .catalog
                    .enumeration(name.value.as_str())
                    .map(|enumeration| ValueType::enumeration(enumeration.internal_id())),
                [entity, field] => self
                    .catalog
                    .entity(entity.value.as_str())?
                    .field(field.value.as_str())
                    .map(|field| field.value_type().clone()),
                _ => None,
            },
            TypeReference::Optional(inner) => {
                ValueType::optional(self.resolve_parameter_value_type(&inner.value)?).ok()
            }
            TypeReference::BoundedString(maximum) => ValueType::string(*maximum as usize).ok(),
            TypeReference::Set(_)
            | TypeReference::BoundedSet { .. }
            | TypeReference::Cursor
            | TypeReference::Limit
            | TypeReference::BoundedLimit(_) => None,
        }
    }

    fn named_type(
        &self,
        value_type: &ValueType,
        span: Span,
    ) -> Result<NamedTypeSchema, QueryDiagnostics> {
        let scalar = match value_type.tag() {
            ValueTypeTag::Bool => "bool".to_owned(),
            ValueTypeTag::I64 => "i64".to_owned(),
            ValueTypeTag::U64 => "u64".to_owned(),
            ValueTypeTag::Decimal => {
                let spec = value_type.decimal_spec().expect("tag checked");
                format!("decimal<{},{}>", spec.precision(), spec.scale())
            }
            ValueTypeTag::Money => {
                format!("money<{}>", value_type.currency().expect("tag checked"))
            }
            ValueTypeTag::String => {
                format!("string<{}>", value_type.byte_bound().expect("tag checked"))
            }
            ValueTypeTag::Bytes => {
                format!("bytes<{}>", value_type.byte_bound().expect("tag checked"))
            }
            ValueTypeTag::Timestamp => "timestamp".to_owned(),
            ValueTypeTag::Date => "date".to_owned(),
            ValueTypeTag::Uuid => "uuid".to_owned(),
            ValueTypeTag::Enum => self
                .catalog
                .enum_name(value_type.enum_type_id().expect("tag checked"))
                .ok_or_else(|| {
                    self.diagnostic(
                        QueryDiagnosticCode::UnknownSymbol,
                        span,
                        Vec::new(),
                        "contract enum type is absent from the exact symbolic catalog",
                    )
                })?
                .to_owned(),
            ValueTypeTag::Optional => {
                return Ok(NamedTypeSchema::Optional(Box::new(self.named_type(
                    value_type.optional_inner().expect("tag checked"),
                    span,
                )?)));
            }
            ValueTypeTag::List => {
                let (element, maximum) = value_type.list_parts().expect("tag checked");
                return Ok(NamedTypeSchema::List {
                    element: Box::new(self.named_type(element, span)?),
                    maximum: PageBound::Literal(maximum as u64),
                });
            }
            ValueTypeTag::Record => {
                return Err(self.diagnostic(
                    QueryDiagnosticCode::InvalidType,
                    span,
                    Vec::new(),
                    "record references are not legal scalar query parameters or fields",
                ));
            }
            ValueTypeTag::Vector => {
                let dim = value_type.vector_dimension().expect("tag checked");
                format!("vector<{}>", dim.get())
            }
        };
        Ok(NamedTypeSchema::Scalar(scalar))
    }

    fn resolve_candidate_source(
        &mut self,
        source: &CandidateSource,
        root_entity: &EntitySymbol,
        root_key_name: &str,
        root_key_type: &ValueType,
    ) -> Result<CandidateSourceV1, QueryDiagnostics> {
        let projected = names(&source.projected_key.value);
        let [entity_name, field_name] = projected.as_slice() else {
            return Err(self.diagnostic(
                QueryDiagnosticCode::InvalidPath,
                source.projected_key.span,
                projected,
                "candidate source projection must be Entity.field",
            ));
        };
        let entity = self.catalog.entity(entity_name).ok_or_else(|| {
            self.diagnostic(
                QueryDiagnosticCode::UnknownSymbol,
                source.projected_key.span,
                vec![entity_name.clone()],
                "unknown candidate source entity",
            )
        })?;
        let field = entity.field(field_name).ok_or_else(|| {
            self.diagnostic(
                QueryDiagnosticCode::UnknownSymbol,
                source.projected_key.span,
                projected.clone(),
                "unknown candidate projected-key field",
            )
        })?;
        if field.value_type() != root_key_type {
            return Err(self.diagnostic(
                QueryDiagnosticCode::InvalidType,
                source.projected_key.span,
                projected,
                "candidate source and root key types differ",
            ));
        }
        let declared_mapping = entity.name() == root_entity.name()
            || entity.relationships().any(|relationship| {
                relationship.target_entity() == root_entity.name()
                    && relationship
                        .source_fields()
                        .iter()
                        .zip(relationship.target_fields())
                        .any(|(source, target)| source == field_name && target == root_key_name)
                    && relationship
                        .source_fields()
                        .iter()
                        .zip(relationship.target_fields())
                        .any(|(source, target)| {
                            source == entity.partition_field()
                                && target == root_entity.partition_field()
                        })
            });
        if !declared_mapping {
            return Err(self.diagnostic(
                QueryDiagnosticCode::InvalidPath,
                source.projected_key.span,
                vec![entity_name.clone(), field_name.clone()],
                "candidate source lacks a declared same-partition relationship to its root",
            ));
        }
        let access_name = source.access.value.as_str();
        self.push_map(
            source.projected_key.span,
            SourceSymbolKind::Field,
            vec![entity_name.clone(), field_name.clone()],
        )?;
        self.push_map(
            source.access.span,
            SourceSymbolKind::CandidateAccess,
            vec![entity_name.clone(), access_name.to_owned()],
        )?;
        self.resolve_expression(&source.predicate.value, source.predicate.span, entity)?;
        let checked = if let Some(provider) = entity.long_pattern_index(access_name) {
            if !entity.primary_key().iter().any(|name| name == field_name) {
                return Err(self.diagnostic(
                    QueryDiagnosticCode::InvalidPath,
                    source.projected_key.span,
                    vec![entity_name.clone(), field_name.clone()],
                    "pattern provider can release only a projected key retained in the entity key",
                ));
            }
            let invocations = long_pattern_invocations(&source.predicate.value)
                .into_iter()
                .filter(|(field, _, _)| field == provider.field())
                .collect::<Vec<_>>();
            let [(pattern_field, operator, parameter)] = invocations.as_slice() else {
                return Err(self.diagnostic(
                    QueryDiagnosticCode::InvalidPath,
                    source.predicate.span,
                    vec![entity_name.clone(), access_name.to_owned()],
                    "pattern candidate source requires exactly one field pattern parameter",
                ));
            };
            if operator.is_negated() {
                return Err(self.diagnostic(
                    QueryDiagnosticCode::InvalidPath,
                    source.predicate.span,
                    vec![entity_name.clone(), access_name.to_owned()],
                    "NOT LIKE requires an explicit candidate difference with a positive LIKE provider source",
                ));
            }
            if pattern_field != provider.field()
                || !provider.operators().contains(operator)
                || self.parameter_value_types.get(parameter.as_str())
                    != entity
                        .field(provider.field())
                        .map(|field| field.value_type())
            {
                return Err(self.diagnostic(
                    QueryDiagnosticCode::InvalidType,
                    source.predicate.span,
                    vec![entity_name.clone(), access_name.to_owned()],
                    "pattern field, operator, or parameter is incompatible with its declared provider",
                ));
            }
            let bounds = provider.bounds();
            let policy_mode = if self
                .catalog
                .row_policies()
                .any(|policy| policy.entity() == entity.name())
            {
                ProjectionProviderPolicyModeV1::BoundedRowAdmission
            } else {
                ProjectionProviderPolicyModeV1::PartitionAligned
            };
            let maximum_state_bytes = bounds
                .matched_bytes()
                .saturating_add(bounds.grams_per_row().saturating_mul(3))
                .saturating_add(128);
            let descriptor = ProjectionProviderDescriptorV1::new(
                ProjectionProviderKindV1::LongPattern,
                ProjectionProviderPostureV1::Exact,
                ProjectionProviderCapabilitiesV1::CANDIDATE
                    | ProjectionProviderCapabilitiesV1::FILTER
                    | ProjectionProviderCapabilitiesV1::WINDOW
                    | ProjectionProviderCapabilitiesV1::OUTPUT,
                policy_mode,
                ProjectionProviderStaticBoundsV1 {
                    max_candidates: bounds.candidates(),
                    max_output_rows: bounds.results(),
                    max_measures: 0,
                    max_input_bytes: bounds.pattern_bytes(),
                    max_work_units: bounds
                        .verification_bytes()
                        .saturating_add(bounds.postings()),
                    max_state_bytes_per_row: maximum_state_bytes,
                    max_diagnostic_bytes: 1024,
                    retained_epochs: u64::from(provider.retained_generations()),
                    max_catchup_lag: u64::from(provider.staleness_slo()),
                    max_epoch_lease_steps: provider.replay_age_seconds(),
                },
                ProjectionProviderStateIdentityV1::new(
                    NonZeroU32::new(1).expect("one is nonzero"),
                    provider.state_schema_hash(),
                ),
            )
            .map_err(|_| {
                self.diagnostic(
                    QueryDiagnosticCode::ArtifactLimit,
                    source.span,
                    vec![entity_name.clone(), access_name.to_owned()],
                    "pattern provider descriptor is inconsistent",
                )
            })?;
            let pattern = LongPatternCandidateV1::checked(
                provider.field().to_owned(),
                *operator,
                parameter.clone(),
                provider.profile(),
                bounds,
                descriptor,
            )
            .ok_or_else(|| {
                self.diagnostic(
                    QueryDiagnosticCode::ArtifactLimit,
                    source.span,
                    vec![entity_name.clone(), access_name.to_owned()],
                    "pattern provider invocation exceeds its closed V1 shape",
                )
            })?;
            CandidateSourceV1::checked_long_pattern(
                entity_name.clone(),
                field_name.clone(),
                access_name.to_owned(),
                pattern,
            )
        } else {
            let access = entity.index(access_name).ok_or_else(|| {
                self.diagnostic(
                    QueryDiagnosticCode::UnknownSymbol,
                    source.access.span,
                    vec![entity_name.clone(), access_name.to_owned()],
                    "candidate source must name one declared ordinary index or pattern provider",
                )
            })?;
            if !access
                .fields()
                .iter()
                .chain(access.cover_fields())
                .any(|name| name == field_name)
            {
                return Err(self.diagnostic(
                    QueryDiagnosticCode::InvalidPath,
                    source.projected_key.span,
                    vec![entity_name.clone(), field_name.clone()],
                    "candidate index does not carry the complete projected root key",
                ));
            }
            CandidateSourceV1::checked(
                entity_name.clone(),
                field_name.clone(),
                access_name.to_owned(),
            )
        };
        checked.ok_or_else(|| {
            self.diagnostic(
                QueryDiagnosticCode::ArtifactLimit,
                source.span,
                vec![entity_name.clone(), access_name.to_owned()],
                "candidate source exceeds its closed V1 shape",
            )
        })
    }

    fn resolve_expression(
        &mut self,
        expression: &Expression,
        span: Span,
        current_entity: &EntitySymbol,
    ) -> Result<(), QueryDiagnostics> {
        match expression {
            Expression::Parameter(parameter) => {
                let name = parameter.value.as_str();
                if !self.parameters.contains_key(name) {
                    return Err(self.diagnostic(
                        QueryDiagnosticCode::UnknownSymbol,
                        parameter.span,
                        vec![name.to_owned()],
                        "unknown query parameter",
                    ));
                }
                self.push_map(
                    parameter.span,
                    SourceSymbolKind::Parameter,
                    vec![name.to_owned()],
                )
            }
            Expression::Path(path) => self
                .resolve_value_path(path, span, Some(current_entity))
                .map(|_| ()),
            Expression::Literal(_) => Ok(()),
            Expression::PresenceGuard {
                parameter,
                predicate,
            } => {
                let name = parameter.value.as_str();
                if !self.parameters.contains_key(name) {
                    return Err(self.diagnostic(
                        QueryDiagnosticCode::UnknownSymbol,
                        parameter.span,
                        vec![name.to_owned()],
                        "unknown optional predicate parameter",
                    ));
                }
                self.push_map(
                    parameter.span,
                    SourceSymbolKind::Parameter,
                    vec![name.to_owned()],
                )?;
                self.resolve_expression(&predicate.value, predicate.span, current_entity)
            }
            Expression::Unary { operand, .. } => {
                self.resolve_expression(&operand.value, operand.span, current_entity)
            }
            Expression::Binary {
                operator,
                left,
                right,
            } if matches!(operator.value, BinaryOperator::In | BinaryOperator::NotIn)
                && matches!(&right.value, Expression::Path(path) if path.0.len() == 1) =>
            {
                let Expression::Path(path) = &right.value else {
                    unreachable!("guarded candidate path")
                };
                let candidate_name = path.0[0].value.as_str();
                let Some(candidate) = self.candidates.get(candidate_name).cloned() else {
                    self.resolve_expression(&left.value, left.span, current_entity)?;
                    return self.resolve_expression(&right.value, right.span, current_entity);
                };
                if operator.value != BinaryOperator::In {
                    return Err(self.diagnostic(
                        QueryDiagnosticCode::InvalidPath,
                        operator.span,
                        vec![candidate_name.to_owned()],
                        "candidate bindings are consumed only by positive root-key membership",
                    ));
                }
                let Expression::Path(left_path) = &left.value else {
                    return Err(self.diagnostic(
                        QueryDiagnosticCode::InvalidPath,
                        left.span,
                        vec![candidate_name.to_owned()],
                        "candidate consumer must be the complete root-key field",
                    ));
                };
                let left_names = names(left_path);
                if current_entity.name() != candidate.root_entity()
                    || left_names.last().map(String::as_str) != Some(candidate.root_key())
                {
                    return Err(self.diagnostic(
                        QueryDiagnosticCode::InvalidPath,
                        left.span,
                        vec![candidate_name.to_owned()],
                        "candidate consumer must be its declared root entity and complete key",
                    ));
                }
                self.resolve_expression(&left.value, left.span, current_entity)?;
                self.push_map(
                    right.span,
                    SourceSymbolKind::Candidate,
                    vec![candidate_name.to_owned()],
                )?;
                let count = self
                    .candidate_consumers
                    .get_mut(candidate_name)
                    .expect("resolved candidate has a consumer counter");
                *count = count.saturating_add(1);
                Ok(())
            }
            Expression::Binary { left, right, .. } => {
                self.resolve_expression(&left.value, left.span, current_entity)?;
                self.resolve_expression(&right.value, right.span, current_entity)
            }
        }
    }

    fn resolve_selection(
        &mut self,
        selection: &Selection,
        context: Option<&ResolvedBinding<'a>>,
        parent_result_path: &[String],
    ) -> Result<Vec<NamedFieldSchema>, QueryDiagnostics> {
        let mut names_seen = BTreeSet::new();
        let mut fields = Vec::with_capacity(selection.fields.len());
        for field in &selection.fields {
            let output_name = field.alias.as_ref().map_or_else(
                || {
                    field
                        .source
                        .value
                        .0
                        .last()
                        .expect("parser path is nonempty")
                        .value
                        .as_str()
                },
                |alias| alias.value.as_str(),
            );
            if !names_seen.insert(output_name.to_owned()) {
                return Err(self.diagnostic(
                    QueryDiagnosticCode::DuplicateName,
                    field
                        .alias
                        .as_ref()
                        .map_or(field.source.span, |alias| alias.span),
                    vec![output_name.to_owned()],
                    "duplicate result field name",
                ));
            }
            let mut result_path = parent_result_path.to_vec();
            result_path.push(output_name.to_owned());
            let value_type = self.resolve_selection_field(field, context, &result_path)?;
            let output_span = field
                .alias
                .as_ref()
                .map_or(field.source.span, |alias| alias.span);
            self.push_map(
                output_span,
                SourceSymbolKind::ResultField,
                vec![output_name.to_owned()],
            )?;
            fields.push(NamedFieldSchema::new(output_name.to_owned(), value_type));
        }
        Ok(fields)
    }

    fn resolve_selection_field(
        &mut self,
        field: &FieldSelection,
        context: Option<&ResolvedBinding<'a>>,
        result_path: &[String],
    ) -> Result<NamedTypeSchema, QueryDiagnostics> {
        let segments = names(&field.source.value);
        if let Some(nested) = &field.nested {
            if let Some(reveal) = field.reveals.first() {
                return Err(self.diagnostic(
                    QueryDiagnosticCode::SecretOutputDeclaration,
                    reveal.span,
                    names(&reveal.value),
                    "secret output declarations apply only to direct stored scalar leaves",
                ));
            }
            let binding_name = match segments.as_slice() {
                [name] => name,
                _ => {
                    return Err(self.diagnostic(
                        QueryDiagnosticCode::InvalidPath,
                        field.source.span,
                        segments,
                        "nested selection source must be one binding name",
                    ));
                }
            };
            if let Some(aggregate) = self.aggregates.get(binding_name).cloned() {
                if let Some(alias) = &field.alias
                    && alias.value.as_str() != binding_name
                {
                    return Err(self.diagnostic(
                        QueryDiagnosticCode::InvalidPath,
                        alias.span,
                        vec![binding_name.clone()],
                        "operational aggregate result aliases are not available in v1",
                    ));
                }
                let nested_fields =
                    self.resolve_aggregate_selection(nested, binding_name, &aggregate)?;
                let record = NamedTypeSchema::Record(nested_fields);
                self.push_map(
                    field.source.span,
                    SourceSymbolKind::Aggregate,
                    vec![binding_name.clone()],
                )?;
                return Ok(if aggregate.grouped {
                    NamedTypeSchema::List {
                        element: Box::new(record),
                        maximum: aggregate.maximum_groups,
                    }
                } else {
                    record
                });
            }
            let binding = self.bindings.get(binding_name).cloned().ok_or_else(|| {
                self.diagnostic(
                    QueryDiagnosticCode::UnknownSymbol,
                    field.source.span,
                    vec![binding_name.clone()],
                    "unknown nested selection binding or aggregate",
                )
            })?;
            if let Some(driver) = self.expansion_drivers.get(binding_name)
                && context.map(|parent| parent.symbol.name.as_str()) != Some(driver.as_str())
            {
                return Err(self.diagnostic(
                    QueryDiagnosticCode::InvalidPath,
                    field.source.span,
                    vec![driver.clone(), binding_name.clone()],
                    "expanded binding must be nested directly under its declared driver",
                ));
            }
            let nested_fields = self.resolve_selection(nested, Some(&binding), result_path)?;
            let record = NamedTypeSchema::Record(nested_fields);
            self.push_map(
                field.source.span,
                SourceSymbolKind::Binding,
                vec![binding_name.clone()],
            )?;
            return Ok(match binding.symbol.cardinality {
                Cardinality::One => record,
                Cardinality::Maybe => NamedTypeSchema::Optional(Box::new(record)),
                Cardinality::Many => NamedTypeSchema::List {
                    element: Box::new(record),
                    maximum: binding.take.clone().ok_or_else(|| {
                        self.diagnostic(
                            QueryDiagnosticCode::InvalidPath,
                            field.source.span,
                            vec![binding_name.clone()],
                            "many binding is missing its checked page bound",
                        )
                    })?,
                },
            });
        }
        let (binding_name, entity, field_name, symbolic_path) = match segments.as_slice() {
            [field_name] => {
                let context = context.ok_or_else(|| {
                    self.diagnostic(
                        QueryDiagnosticCode::InvalidPath,
                        field.source.span,
                        segments.clone(),
                        "top-level scalar selection requires a binding-qualified path",
                    )
                })?;
                (
                    context.symbol.name.clone(),
                    context.entity,
                    field_name.as_str(),
                    vec![context.symbol.entity_name.clone(), field_name.clone()],
                )
            }
            [binding_name, field_name] => {
                let binding = self.bindings.get(binding_name).cloned().ok_or_else(|| {
                    self.diagnostic(
                        QueryDiagnosticCode::UnknownSymbol,
                        field.source.span,
                        segments.clone(),
                        "unknown selection binding",
                    )
                })?;
                (
                    binding_name.clone(),
                    binding.entity,
                    field_name.as_str(),
                    vec![binding.symbol.entity_name.clone(), field_name.clone()],
                )
            }
            _ => {
                return Err(self.diagnostic(
                    QueryDiagnosticCode::InvalidPath,
                    field.source.span,
                    segments,
                    "invalid result field path",
                ));
            }
        };
        let symbol = entity.field(field_name).ok_or_else(|| {
            self.diagnostic(
                QueryDiagnosticCode::UnknownSymbol,
                field.source.span,
                symbolic_path.clone(),
                "unknown selected contract field",
            )
        })?;
        if field.reveals.len() > 1 {
            let duplicate = &field.reveals[1];
            return Err(self.diagnostic(
                QueryDiagnosticCode::SecretOutputDeclaration,
                duplicate.span,
                names(&duplicate.value),
                "secret output declaration is duplicated for one result leaf",
            ));
        }
        match (symbol.is_secret(), field.reveals.first()) {
            (true, None) => {
                return Err(self.diagnostic(
                    QueryDiagnosticCode::SecretOutputDeclaration,
                    field.source.span,
                    symbolic_path,
                    "projected secret field requires an exact reveals declaration",
                ));
            }
            (false, Some(reveal)) => {
                return Err(self.diagnostic(
                    QueryDiagnosticCode::SecretOutputDeclaration,
                    reveal.span,
                    names(&reveal.value),
                    "reveals declaration must name a secret-classified stored field",
                ));
            }
            (true, Some(reveal)) => {
                let declared = names(&reveal.value);
                let expected = vec![binding_name.clone(), field_name.to_owned()];
                if declared != expected {
                    return Err(self.diagnostic(
                        QueryDiagnosticCode::SecretOutputDeclaration,
                        reveal.span,
                        declared,
                        "reveals declaration must exactly match the projected binding field",
                    ));
                }
                if self.secret_outputs.len() == riffdb_riffql_syntax::MAX_COLLECTION_ITEMS {
                    return Err(self.diagnostic(
                        QueryDiagnosticCode::ArtifactLimit,
                        reveal.span,
                        expected,
                        "secret output requirement limit exceeded",
                    ));
                }
                self.secret_outputs.push(SecretOutputRequirement {
                    binding: binding_name,
                    entity: entity.name().to_owned(),
                    entity_id: entity.internal_id(),
                    field: field_name.to_owned(),
                    field_id: symbol.internal_id(),
                    result_path: result_path.to_vec(),
                    declaration_span: reveal.span,
                });
                self.push_map(
                    reveal.span,
                    SourceSymbolKind::SecretOutput,
                    vec![entity.name().to_owned(), field_name.to_owned()],
                )?;
            }
            (false, None) => {}
        }
        self.push_map(field.source.span, SourceSymbolKind::Field, symbolic_path)?;
        self.named_type(symbol.value_type(), field.source.span)
    }

    fn resolve_aggregate_selection(
        &mut self,
        selection: &Selection,
        aggregate_name: &str,
        aggregate: &ResolvedAggregateSelection,
    ) -> Result<Vec<NamedFieldSchema>, QueryDiagnostics> {
        let mut names_seen = BTreeSet::new();
        let mut fields = Vec::with_capacity(selection.fields.len());
        for field in &selection.fields {
            if let Some(reveal) = field.reveals.first() {
                return Err(self.diagnostic(
                    QueryDiagnosticCode::SecretOutputDeclaration,
                    reveal.span,
                    names(&reveal.value),
                    "aggregate results cannot declare secret stored-field outputs",
                ));
            }
            if let Some(alias) = &field.alias {
                return Err(self.diagnostic(
                    QueryDiagnosticCode::InvalidPath,
                    alias.span,
                    vec![aggregate_name.to_owned()],
                    "operational aggregate field aliases are not available in v1",
                ));
            }
            if field.nested.is_some() || field.source.value.0.len() != 1 {
                return Err(self.diagnostic(
                    QueryDiagnosticCode::InvalidPath,
                    field.source.span,
                    vec![aggregate_name.to_owned()],
                    "aggregate result selection must contain scalar result fields",
                ));
            }
            let source_name = field.source.value.0[0].value.as_str();
            let output_name = field
                .alias
                .as_ref()
                .map_or(source_name, |alias| alias.value.as_str());
            if !names_seen.insert(output_name.to_owned()) {
                return Err(self.diagnostic(
                    QueryDiagnosticCode::DuplicateName,
                    field
                        .alias
                        .as_ref()
                        .map_or(field.source.span, |alias| alias.span),
                    vec![aggregate_name.to_owned(), output_name.to_owned()],
                    "duplicate aggregate result selection field",
                ));
            }
            let value_type = aggregate.fields.get(source_name).cloned().ok_or_else(|| {
                self.diagnostic(
                    QueryDiagnosticCode::UnknownSymbol,
                    field.source.span,
                    vec![aggregate_name.to_owned(), source_name.to_owned()],
                    "unknown aggregate result field",
                )
            })?;
            self.push_map(
                field.source.span,
                SourceSymbolKind::ResultField,
                vec![aggregate_name.to_owned(), source_name.to_owned()],
            )?;
            fields.push(NamedFieldSchema::new(output_name.to_owned(), value_type));
        }
        Ok(fields)
    }

    fn resolve_value_path(
        &mut self,
        path: &Path,
        span: Span,
        current_entity: Option<&EntitySymbol>,
    ) -> Result<ValueType, QueryDiagnostics> {
        let segments = names(path);
        match segments.as_slice() {
            [field] => {
                let entity = current_entity.ok_or_else(|| {
                    self.diagnostic(
                        QueryDiagnosticCode::InvalidPath,
                        span,
                        segments.clone(),
                        "unqualified field has no entity context",
                    )
                })?;
                let symbol = entity.field(field).ok_or_else(|| {
                    self.diagnostic(
                        QueryDiagnosticCode::UnknownSymbol,
                        span,
                        vec![entity.name().to_owned(), field.clone()],
                        "unknown contract field",
                    )
                })?;
                self.push_map(
                    span,
                    SourceSymbolKind::Field,
                    vec![entity.name().to_owned(), field.clone()],
                )?;
                Ok(symbol.value_type().clone())
            }
            [first, second] => {
                let binding = self.bindings.get(first).cloned();
                let enumeration = self.catalog.enumeration(first);
                match (binding, enumeration) {
                    (Some(_), Some(_)) => Err(self.diagnostic(
                        QueryDiagnosticCode::AmbiguousSymbol,
                        span,
                        segments,
                        "path is ambiguous between a binding and enum type",
                    )),
                    (Some(binding), None) => {
                        let field = binding.entity.field(second).ok_or_else(|| {
                            self.diagnostic(
                                QueryDiagnosticCode::UnknownSymbol,
                                span,
                                vec![binding.symbol.entity_name.clone(), second.clone()],
                                "unknown bound-entity field",
                            )
                        })?;
                        let symbolic = vec![binding.symbol.entity_name.clone(), second.clone()];
                        self.push_map(span, SourceSymbolKind::Field, symbolic)?;
                        Ok(field.value_type().clone())
                    }
                    (None, Some(enumeration)) => {
                        let variant = enumeration.variant(second).ok_or_else(|| {
                            self.diagnostic(
                                QueryDiagnosticCode::UnknownSymbol,
                                span,
                                segments.clone(),
                                "unknown enum variant",
                            )
                        })?;
                        self.push_map(span, SourceSymbolKind::EnumVariant, segments)?;
                        let _ = variant;
                        Ok(ValueType::enumeration(enumeration.internal_id()))
                    }
                    (None, None) => Err(self.diagnostic(
                        QueryDiagnosticCode::UnknownSymbol,
                        span,
                        segments,
                        "unknown symbolic path",
                    )),
                }
            }
            _ => Err(self.diagnostic(
                QueryDiagnosticCode::InvalidPath,
                span,
                segments,
                "symbol path has unsupported depth",
            )),
        }
    }

    fn page_bound(
        &self,
        expression: &Expression,
        span: Span,
        clause: PageBoundClause,
    ) -> Result<PageBound, QueryDiagnostics> {
        match expression {
            Expression::Literal(Literal::Unsigned(value)) => {
                let parsed = value.parse::<u64>().ok().filter(|value| *value > 0);
                match parsed {
                    Some(take) if page_take_within_scan_bound(take) => Ok(PageBound::Literal(take)),
                    // The bound plus
                    // one probe row must stay within MAX_QUERY_SCANNED_ROWS
                    // one probe stays within the 65,535-row scan ceiling. The
                    // nearest K inherits the same ceiling but mints no
                    // continuation, so its message names the shared ceiling
                    // instead.
                    Some(_) => Err(self.diagnostic(
                        QueryDiagnosticCode::ArtifactLimit,
                        span,
                        Vec::new(),
                        match clause {
                            PageBoundClause::Take => "static take exceeds the maximum page take of 65534 (scan ceiling reserves one row for the continuation probe)",
                            PageBoundClause::NearestK => "nearest k exceeds the maximum page bound of 65534 (K is the binding's checked page bound and shares the take ceiling)",
                        },
                    )),
                    None => Err(self.diagnostic(
                        QueryDiagnosticCode::InvalidType,
                        span,
                        Vec::new(),
                        match clause {
                            PageBoundClause::Take => "take literal is not a positive u64",
                            PageBoundClause::NearestK => "nearest k literal is not a positive u64",
                        },
                    )),
                }
            }
            Expression::Parameter(parameter)
                if matches!(
                    self.parameters.get(parameter.value.as_str()),
                    Some(NamedTypeSchema::Limit)
                ) =>
            {
                Ok(PageBound::Parameter(parameter.value.as_str().to_owned()))
            }
            Expression::Parameter(parameter) => {
                let Some(NamedTypeSchema::BoundedLimit { maximum }) =
                    self.parameters.get(parameter.value.as_str())
                else {
                    return Err(self.diagnostic(
                        QueryDiagnosticCode::InvalidType,
                        span,
                        Vec::new(),
                        match clause {
                            PageBoundClause::Take => {
                                "take parameter must have type Limit or Limit<MAX>"
                            }
                            PageBoundClause::NearestK => {
                                "nearest k parameter must have type Limit or Limit<MAX>"
                            }
                        },
                    ));
                };
                Ok(PageBound::BoundedParameter {
                    name: parameter.value.as_str().to_owned(),
                    maximum: *maximum,
                })
            }
            _ => Err(self.diagnostic(
                QueryDiagnosticCode::InvalidType,
                span,
                Vec::new(),
                match clause {
                    PageBoundClause::Take => "take parameter must have type Limit",
                    PageBoundClause::NearestK => "nearest k parameter must have type Limit",
                },
            )),
        }
    }

    fn push_map(
        &mut self,
        span: Span,
        kind: SourceSymbolKind,
        symbolic_path: Vec<String>,
    ) -> Result<(), QueryDiagnostics> {
        if self.source_map.len() == MAX_SOURCE_MAP_ENTRIES {
            return Err(self.diagnostic(
                QueryDiagnosticCode::ArtifactLimit,
                span,
                symbolic_path,
                "query source-map entry limit exceeded",
            ));
        }
        self.source_map.push(SourceMapEntry {
            span,
            kind,
            symbolic_path,
        });
        Ok(())
    }

    fn diagnostic(
        &self,
        code: QueryDiagnosticCode,
        span: Span,
        path: Vec<String>,
        summary: &'static str,
    ) -> QueryDiagnostics {
        QueryDiagnostics::one(QueryDiagnostic::new(
            code,
            QueryDiagnosticStage::Resolution,
            span,
            path,
            summary,
            None,
        ))
    }
}

fn names(path: &Path) -> Vec<String> {
    path.0
        .iter()
        .map(|segment| segment.value.as_str().to_owned())
        .collect()
}

fn first_secret_output_declaration(
    selection: &Selection,
) -> Option<&riffdb_riffql_syntax::Spanned<Path>> {
    selection.fields.iter().find_map(|field| {
        field.reveals.first().or_else(|| {
            field
                .nested
                .as_ref()
                .and_then(first_secret_output_declaration)
        })
    })
}

fn canonical_surface(
    document: &Document,
    identity: &ExactContractIdentity,
    bindings: &[BindingSymbol],
    candidates: &[CandidateBindingV1],
    aggregates: &[OperationalAggregateV1],
    schemas: &NamedQuerySchemas,
    secret_outputs: &[SecretOutputRequirement],
) -> Result<Vec<u8>, QueryDiagnostics> {
    let source = format_query(document);
    let mut bytes = Vec::with_capacity(
        IR_MAGIC.len() + 4 + 32 + 4 + identity.lineage().as_str().len() + 8 + 4 + source.len(),
    );
    bytes.extend_from_slice(IR_MAGIC);
    bytes.extend_from_slice(
        &if document.language_version == RIFFQL_LANGUAGE_VERSION_PARTITION_SET_V1 {
            QUERY_IR_VERSION_PARTITION_SET_V1
        } else if matches!(
            document.language_version,
            riffdb_riffql_syntax::RIFFQL_LANGUAGE_VERSION_ORDER_FAMILY_V1
                | RIFFQL_LANGUAGE_VERSION_BOUNDED_RESULT_PIPELINE_V1
        ) {
            QUERY_IR_VERSION_BOUNDED_RESULT_PIPELINE_V1
        } else if document.language_version == RIFFQL_LANGUAGE_VERSION_BOUNDED_LIMIT_V1 {
            QUERY_IR_VERSION_BOUNDED_LIMIT_V1
        } else if document.language_version == RIFFQL_LANGUAGE_VERSION_EXACT_AGGREGATE_V1 {
            QUERY_IR_VERSION_EXACT_AGGREGATE_V1
        } else if document.projected_source.is_some() {
            QUERY_IR_VERSION_PROJECTED_VECTOR_V1
        } else if !secret_outputs.is_empty() {
            QUERY_IR_VERSION_SECRET_OUTPUT_V1
        } else if aggregates.is_empty() {
            QUERY_IR_VERSION_V1
        } else {
            QUERY_IR_VERSION_OPERATIONAL_AGGREGATE_V1
        }
        .to_be_bytes(),
    );
    bytes.extend_from_slice(identity.bundle_hash().as_bytes());
    push_bytes(&mut bytes, identity.lineage().as_str().as_bytes())?;
    bytes.extend_from_slice(&identity.version().get().to_be_bytes());
    push_bytes(&mut bytes, source.as_bytes())?;
    push_count(&mut bytes, bindings.len())?;
    for binding in bindings {
        push_bytes(&mut bytes, binding.name.as_bytes())?;
        push_bytes(&mut bytes, binding.entity_name.as_bytes())?;
        bytes.extend_from_slice(&binding.entity_id.get().to_be_bytes());
        bytes.push(match binding.cardinality {
            Cardinality::One => 1,
            Cardinality::Maybe => 2,
            Cardinality::Many => 3,
        });
    }
    if !candidates.is_empty() {
        bytes.extend_from_slice(CANDIDATE_BINDINGS_MAGIC);
        push_count(&mut bytes, candidates.len())?;
        for candidate in candidates {
            push_bytes(&mut bytes, candidate.name().as_bytes())?;
            push_bytes(&mut bytes, candidate.root_entity().as_bytes())?;
            push_bytes(&mut bytes, candidate.root_key().as_bytes())?;
            bytes.push(candidate.operator().durable_tag());
            bytes.push(candidate.positive_source_count());
            bytes.extend_from_slice(&candidate.maximum_distinct_keys().to_be_bytes());
            push_bytes(&mut bytes, candidate.refusal_outcome().as_bytes())?;
            push_count(&mut bytes, candidate.sources().len())?;
            for source in candidate.sources() {
                push_bytes(&mut bytes, source.entity().as_bytes())?;
                push_bytes(&mut bytes, source.projected_key().as_bytes())?;
                push_bytes(&mut bytes, source.access().as_bytes())?;
            }
        }
        let pattern_sources =
            candidates
                .iter()
                .enumerate()
                .flat_map(|(candidate_index, candidate)| {
                    candidate.sources().iter().enumerate().filter_map(
                        move |(source_index, source)| {
                            source
                                .long_pattern()
                                .map(|pattern| (candidate_index, source_index, pattern))
                        },
                    )
                })
                .collect::<Vec<_>>();
        if !pattern_sources.is_empty() {
            bytes.extend_from_slice(LONG_PATTERN_CANDIDATES_MAGIC);
            push_count(&mut bytes, pattern_sources.len())?;
            for (candidate_index, source_index, pattern) in pattern_sources {
                push_count(&mut bytes, candidate_index)?;
                push_count(&mut bytes, source_index)?;
                push_bytes(&mut bytes, pattern.field().as_bytes())?;
                bytes.push(pattern.operator() as u8);
                bytes.push(pattern.profile() as u8);
                push_bytes(&mut bytes, pattern.pattern_parameter().as_bytes())?;
                bytes.extend_from_slice(&pattern.descriptor().to_canonical_bytes());
            }
        }
    }
    if !aggregates.is_empty() {
        bytes.extend_from_slice(OPERATIONAL_AGGREGATES_MAGIC);
        push_count(&mut bytes, aggregates.len())?;
        for aggregate in aggregates {
            push_bytes(&mut bytes, aggregate.name().as_bytes())?;
            push_bytes(&mut bytes, aggregate.source_binding().as_bytes())?;
            push_bytes(&mut bytes, aggregate.source_entity().as_bytes())?;
            push_count(&mut bytes, aggregate.group_keys().len())?;
            for group in aggregate.group_keys() {
                push_bytes(&mut bytes, group.field().as_bytes())?;
                encode_named_type(&mut bytes, group.value_type())?;
            }
            push_count(&mut bytes, aggregate.measures().len())?;
            for measure in aggregate.measures() {
                push_bytes(&mut bytes, measure.alias().as_bytes())?;
                bytes.push(measure.function().durable_tag());
                match measure.input_field() {
                    Some(field) => {
                        bytes.push(1);
                        push_bytes(&mut bytes, field.as_bytes())?;
                    }
                    None => bytes.push(0),
                }
                encode_named_type(&mut bytes, measure.result_type())?;
            }
            encode_page_bound(&mut bytes, aggregate.maximum_groups())?;
            if matches!(
                document.language_version,
                RIFFQL_LANGUAGE_VERSION_EXACT_AGGREGATE_V1
                    | RIFFQL_LANGUAGE_VERSION_BOUNDED_LIMIT_V1
            ) {
                bytes.extend_from_slice(
                    &aggregate
                        .execution_budget()
                        .maximum_distinct_values_per_measure()
                        .to_be_bytes(),
                );
                bytes.extend_from_slice(
                    &aggregate
                        .execution_budget()
                        .maximum_state_bytes()
                        .to_be_bytes(),
                );
                bytes.extend_from_slice(
                    &aggregate
                        .execution_budget()
                        .maximum_arithmetic_operations()
                        .to_be_bytes(),
                );
            }
        }
    }
    push_count(&mut bytes, schemas.parameters().len())?;
    for parameter in schemas.parameters() {
        push_bytes(&mut bytes, parameter.name().as_bytes())?;
        encode_named_type(&mut bytes, parameter.value_type())?;
        bytes.push(u8::from(parameter.has_default()));
    }
    push_count(&mut bytes, schemas.results().len())?;
    for branch in schemas.results() {
        push_bytes(&mut bytes, branch.name().as_bytes())?;
        encode_fields(&mut bytes, branch.fields())?;
    }
    if !secret_outputs.is_empty() {
        bytes.extend_from_slice(b"SECRET-OUTPUT-REQUIREMENTS\0");
        push_count(&mut bytes, secret_outputs.len())?;
        for requirement in secret_outputs {
            push_bytes(&mut bytes, requirement.binding.as_bytes())?;
            push_bytes(&mut bytes, requirement.entity.as_bytes())?;
            bytes.extend_from_slice(&requirement.entity_id.get().to_be_bytes());
            push_bytes(&mut bytes, requirement.field.as_bytes())?;
            bytes.extend_from_slice(&requirement.field_id.get().to_be_bytes());
            push_count(&mut bytes, requirement.result_path.len())?;
            for segment in &requirement.result_path {
                push_bytes(&mut bytes, segment.as_bytes())?;
            }
            bytes.extend_from_slice(&requirement.declaration_span.start.to_be_bytes());
            bytes.extend_from_slice(&requirement.declaration_span.end.to_be_bytes());
        }
    }
    if bytes.len() > MAX_QUERY_ARTIFACT_BYTES {
        return Err(QueryDiagnostics::one(QueryDiagnostic::new(
            QueryDiagnosticCode::ArtifactLimit,
            QueryDiagnosticStage::Schema,
            Span { start: 0, end: 0 },
            Vec::new(),
            "canonical query surface exceeds the artifact limit",
            None,
        )));
    }
    Ok(bytes)
}

fn encode_page_bound(output: &mut Vec<u8>, bound: &PageBound) -> Result<(), QueryDiagnostics> {
    match bound {
        PageBound::Literal(value) => {
            output.push(1);
            output.extend_from_slice(&value.to_be_bytes());
        }
        PageBound::Parameter(name) => {
            output.push(2);
            push_bytes(output, name.as_bytes())?;
        }
        PageBound::BoundedParameter { name, maximum } => {
            output.push(3);
            push_bytes(output, name.as_bytes())?;
            output.extend_from_slice(&maximum.to_be_bytes());
        }
    }
    Ok(())
}

fn encode_fields(
    output: &mut Vec<u8>,
    fields: &[NamedFieldSchema],
) -> Result<(), QueryDiagnostics> {
    push_count(output, fields.len())?;
    for field in fields {
        push_bytes(output, field.name().as_bytes())?;
        encode_named_type(output, field.value_type())?;
    }
    Ok(())
}

fn encode_named_type(
    output: &mut Vec<u8>,
    value_type: &NamedTypeSchema,
) -> Result<(), QueryDiagnostics> {
    match value_type {
        NamedTypeSchema::Scalar(name) => {
            output.push(1);
            push_bytes(output, name.as_bytes())?;
        }
        NamedTypeSchema::Optional(inner) => {
            output.push(2);
            encode_named_type(output, inner)?;
        }
        NamedTypeSchema::Set(inner) => {
            output.push(3);
            encode_named_type(output, inner)?;
        }
        NamedTypeSchema::BoundedSet { element, maximum } => {
            output.push(9);
            encode_named_type(output, element)?;
            output.extend_from_slice(&maximum.to_be_bytes());
        }
        NamedTypeSchema::Record(fields) => {
            output.push(4);
            encode_fields(output, fields)?;
        }
        NamedTypeSchema::List { element, maximum } => {
            output.push(5);
            encode_named_type(output, element)?;
            match maximum {
                PageBound::Literal(value) => {
                    output.push(1);
                    output.extend_from_slice(&value.to_be_bytes());
                }
                PageBound::Parameter(name) => {
                    output.push(2);
                    push_bytes(output, name.as_bytes())?;
                }
                PageBound::BoundedParameter { name, maximum } => {
                    output.push(3);
                    push_bytes(output, name.as_bytes())?;
                    output.extend_from_slice(&maximum.to_be_bytes());
                }
            }
        }
        NamedTypeSchema::Cursor => output.push(6),
        NamedTypeSchema::Limit => output.push(7),
        NamedTypeSchema::BoundedLimit { maximum } => {
            output.push(8);
            output.extend_from_slice(&maximum.to_be_bytes());
        }
    }
    Ok(())
}

fn push_count(output: &mut Vec<u8>, count: usize) -> Result<(), QueryDiagnostics> {
    let count = u32::try_from(count).map_err(|_| artifact_limit())?;
    output.extend_from_slice(&count.to_be_bytes());
    Ok(())
}

fn push_bytes(output: &mut Vec<u8>, bytes: &[u8]) -> Result<(), QueryDiagnostics> {
    let length = u32::try_from(bytes.len()).map_err(|_| artifact_limit())?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(bytes);
    Ok(())
}

fn artifact_limit() -> QueryDiagnostics {
    QueryDiagnostics::one(QueryDiagnostic::new(
        QueryDiagnosticCode::ArtifactLimit,
        QueryDiagnosticStage::Schema,
        Span { start: 0, end: 0 },
        Vec::new(),
        "canonical query component exceeds the artifact limit",
        None,
    ))
}
