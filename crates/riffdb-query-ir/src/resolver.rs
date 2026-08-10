use std::collections::{BTreeMap, BTreeSet};

use riffdb_contract_ir::{ValueType, ValueTypeTag};
use riffdb_riffql_syntax::{
    AggregateFunction, Cardinality, Document, Expression, FieldSelection, Literal, Path, Selection,
    Span, TypeReference, format_query,
};
use riffdb_types::{
    ContractBundleHash, ContractLineage, ContractVersion, EntityTypeId, MAX_DECIMAL_PRECISION,
};

use crate::{
    EntitySymbol, MAX_QUERY_ARTIFACT_BYTES, MAX_SOURCE_MAP_ENTRIES, NamedFieldSchema,
    NamedParameterSchema, NamedQuerySchemas, NamedResultBranchSchema, NamedTypeSchema,
    OperationalAggregateFunctionV1, OperationalAggregateGroupKeyV1, OperationalAggregateMeasureV1,
    OperationalAggregateV1, PageBound, QUERY_IR_VERSION_OPERATIONAL_AGGREGATE_V1,
    QUERY_IR_VERSION_V1, QueryDiagnostic, QueryDiagnosticCode, QueryDiagnosticStage,
    QueryDiagnostics, SymbolicCatalog, page_take_within_scan_bound,
};

const IR_MAGIC: &[u8] = b"RIFFDB-QUERY-SURFACE\0";
const OPERATIONAL_AGGREGATES_MAGIC: &[u8] = b"OPERATIONAL-AGGREGATES\0";

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
    /// Returned field alias.
    ResultField,
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
    aggregates: Vec<OperationalAggregateV1>,
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
            .field("aggregates", &self.aggregates)
            .field("schemas", &self.schemas)
            .field("source_map_entries", &self.source_map.0.len())
            .field("canonical_length", &self.canonical_bytes.len())
            .finish()
    }
}

impl ResolvedQueryV1 {
    /// Query IR version.
    #[must_use]
    pub const fn ir_version(&self) -> u32 {
        QUERY_IR_VERSION_V1
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

    /// Compiler-resolved bounded aggregate declarations.
    #[must_use]
    pub fn aggregates(&self) -> &[OperationalAggregateV1] {
        &self.aggregates
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

struct Resolver<'a> {
    catalog: &'a SymbolicCatalog,
    parameters: BTreeMap<String, NamedTypeSchema>,
    bindings: BTreeMap<String, ResolvedBinding<'a>>,
    aggregates: BTreeMap<String, ResolvedAggregateSelection>,
    source_map: Vec<SourceMapEntry>,
}

impl<'a> Resolver<'a> {
    fn new(catalog: &'a SymbolicCatalog) -> Self {
        Self {
            catalog,
            parameters: BTreeMap::new(),
            bindings: BTreeMap::new(),
            aggregates: BTreeMap::new(),
            source_map: Vec::new(),
        }
    }

    fn resolve(mut self, document: &Document) -> Result<ResolvedQueryV1, QueryDiagnostics> {
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
            if matches!(ty, NamedTypeSchema::Limit)
                && let Some(default) = parameter.default.as_ref()
            {
                match &default.value {
                    Literal::Unsigned(value) => {
                        let parsed = value.parse::<u64>().ok().filter(|value| *value > 0);
                        match parsed {
                            Some(limit) if page_take_within_scan_bound(limit) => {}
                            Some(_) => {
                                return Err(self.diagnostic(
                                    QueryDiagnosticCode::ArtifactLimit,
                                    default.span,
                                    vec![name.to_owned()],
                                    "Limit default exceeds the maximum page take of 499 (scan ceiling reserves one row for the continuation probe)",
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

        let mut binding_symbols = Vec::with_capacity(document.body.bindings.len());
        for binding in &document.body.bindings {
            let name = binding.name.value.as_str();
            if self.bindings.contains_key(name) || self.parameters.contains_key(name) {
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
                .map(|take| self.page_bound(&take.limit.value, take.limit.span))
                .transpose()?;
            self.bindings.insert(
                name.to_owned(),
                ResolvedBinding {
                    symbol: symbol.clone(),
                    entity,
                    take,
                },
            );
            binding_symbols.push(symbol);
            self.resolve_expression(&binding.predicate.value, binding.predicate.span, entity)?;
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
                let (function, input_field, result_type) = match measure.function.value {
                    AggregateFunction::Count => (
                        OperationalAggregateFunctionV1::Count,
                        None,
                        NamedTypeSchema::Scalar("u64".to_owned()),
                    ),
                    AggregateFunction::Sum => {
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
                    AggregateFunction::Min | AggregateFunction::Max => {
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
                        let function = if measure.function.value == AggregateFunction::Min {
                            OperationalAggregateFunctionV1::Min
                        } else {
                            OperationalAggregateFunctionV1::Max
                        };
                        let result = NamedTypeSchema::Optional(Box::new(
                            self.named_type(&field_type, field.span)?,
                        ));
                        (function, Some(field_name), result)
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
            let resolved = OperationalAggregateV1::checked(
                name.to_owned(),
                source_name.to_owned(),
                source.entity.name().to_owned(),
                group_keys,
                measures,
                maximum_groups.clone(),
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

        let fields = self.resolve_selection(&document.body.selection, None)?;
        let branch_name = document
            .body
            .outcome
            .as_ref()
            .map_or("Result", |outcome| outcome.value.as_str())
            .to_owned();
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
            &aggregate_symbols,
            &schemas,
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
            aggregates: aggregate_symbols,
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
            TypeReference::Cursor => Ok(NamedTypeSchema::Cursor),
            TypeReference::Limit => Ok(NamedTypeSchema::Limit),
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
        };
        Ok(NamedTypeSchema::Scalar(scalar))
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
            let value_type = self.resolve_selection_field(field, context)?;
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
    ) -> Result<NamedTypeSchema, QueryDiagnostics> {
        let segments = names(&field.source.value);
        if let Some(nested) = &field.nested {
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
            let nested_fields = self.resolve_selection(nested, Some(&binding))?;
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
        let (entity, field_name, symbolic_path) = match segments.as_slice() {
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
    ) -> Result<PageBound, QueryDiagnostics> {
        match expression {
            Expression::Literal(Literal::Unsigned(value)) => {
                let parsed = value.parse::<u64>().ok().filter(|value| *value > 0);
                match parsed {
                    Some(take) if page_take_within_scan_bound(take) => Ok(PageBound::Literal(take)),
                    Some(_) => Err(self.diagnostic(
                        QueryDiagnosticCode::ArtifactLimit,
                        span,
                        Vec::new(),
                        // Bound is max_query_page_take() (= 499): take + continuation probe
                        // must stay within MAX_QUERY_SCANNED_ROWS (500).
                        "static take exceeds the maximum page take of 499 (scan ceiling reserves one row for the continuation probe)",
                    )),
                    None => Err(self.diagnostic(
                        QueryDiagnosticCode::InvalidType,
                        span,
                        Vec::new(),
                        "take literal is not a positive u64",
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
            _ => Err(self.diagnostic(
                QueryDiagnosticCode::InvalidType,
                span,
                Vec::new(),
                "take parameter must have type Limit",
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

fn canonical_surface(
    document: &Document,
    identity: &ExactContractIdentity,
    bindings: &[BindingSymbol],
    aggregates: &[OperationalAggregateV1],
    schemas: &NamedQuerySchemas,
) -> Result<Vec<u8>, QueryDiagnostics> {
    let source = format_query(document);
    let mut bytes = Vec::with_capacity(
        IR_MAGIC.len() + 4 + 32 + 4 + identity.lineage().as_str().len() + 8 + 4 + source.len(),
    );
    bytes.extend_from_slice(IR_MAGIC);
    bytes.extend_from_slice(
        &if aggregates.is_empty() {
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
                bytes.push(match measure.function() {
                    OperationalAggregateFunctionV1::Count => 1,
                    OperationalAggregateFunctionV1::Sum => 2,
                    OperationalAggregateFunctionV1::Min => 3,
                    OperationalAggregateFunctionV1::Max => 4,
                });
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
            }
        }
        NamedTypeSchema::Cursor => output.push(6),
        NamedTypeSchema::Limit => output.push(7),
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
