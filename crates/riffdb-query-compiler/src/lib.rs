#![forbid(unsafe_code)]

//! Deterministic type checking, authorization analysis, and bounded planning for RiffQL v1.

use std::collections::{BTreeMap, BTreeSet};

use riffdb_contract_ir::{ValueType, ValueTypeTag};
use riffdb_query_ir::{
    AccessDirection, AuthorizationEntityAccess, EntitySymbol, QueryAccessKind,
    QueryAccessProgramV1, QueryAccessStep, QueryLiteral, QueryPredicate, QueryPredicateOperator,
    QueryPredicateValue, QueryRowLimit, SymbolicCatalog, resolve_query_surface,
};
use riffdb_riffql_syntax::{
    BinaryOperator, Cardinality, Direction, Document, Expression, FieldSelection, Literal, Path,
    Span, TypeReference,
};
use riffdb_types::QueryCostVectorV1;

const MAX_QUERY_ROWS: u64 = 500;

/// Stable planner/type-checker diagnostic code.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlannerDiagnosticCode {
    /// Predicate operands are not type compatible.
    TypeMismatch,
    /// Every access is not routed by one exact partition parameter.
    NonLocal,
    /// A `many` access has no legal bounded index plan.
    Unindexed,
    /// Requested ordering is not a complete forward or reverse index suffix.
    Unordered,
    /// A cardinality or total-work bound cannot be proven.
    Unbounded,
    /// Resolved input could not form a closed internal program.
    InternalInvariant,
    /// Scalar and bounded-collection cardinality are used incompatibly.
    Cardinality,
}

impl PlannerDiagnosticCode {
    /// Stable public code.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TypeMismatch => "RDB-QP001",
            Self::NonLocal => "RDB-QP002",
            Self::Unindexed => "RDB-QP003",
            Self::Unordered => "RDB-QP004",
            Self::Unbounded => "RDB-QP005",
            Self::InternalInvariant => "RDB-QP006",
            Self::Cardinality => "RDB-QP007",
        }
    }
}

/// One value-free source-spanned planner diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlannerDiagnostic {
    code: PlannerDiagnosticCode,
    primary: Span,
    symbol_path: Vec<String>,
    summary: &'static str,
    suggested_index: Option<String>,
}

impl PlannerDiagnostic {
    /// Stable code.
    #[must_use]
    pub const fn code(&self) -> PlannerDiagnosticCode {
        self.code
    }

    /// Primary source span.
    #[must_use]
    pub const fn primary(&self) -> Span {
        self.primary
    }

    /// Safe symbolic path.
    #[must_use]
    pub fn symbol_path(&self) -> &[String] {
        &self.symbol_path
    }

    /// Static value-free summary.
    #[must_use]
    pub const fn summary(&self) -> &'static str {
        self.summary
    }

    /// Suggested compatible index declaration, when one can be stated safely.
    #[must_use]
    pub fn suggested_index(&self) -> Option<&str> {
        self.suggested_index.as_deref()
    }
}

/// Deterministically ordered diagnostics; no partial program accompanies failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlannerDiagnostics(Vec<PlannerDiagnostic>);

impl PlannerDiagnostics {
    /// Diagnostics in source order.
    #[must_use]
    pub fn as_slice(&self) -> &[PlannerDiagnostic] {
        &self.0
    }
}

/// Resolves, type checks, authorizes, and plans one parsed query against one exact catalog.
pub fn compile_query(
    document: &Document,
    catalog: &SymbolicCatalog,
) -> Result<QueryAccessProgramV1, PlannerDiagnostics> {
    let surface = resolve_query_surface(document, catalog).map_err(|_| {
        one(
            PlannerDiagnosticCode::InternalInvariant,
            Span { start: 0, end: 0 },
            Vec::new(),
            "symbolic resolution failed before planning",
            None,
        )
    })?;
    Planner::new(document, catalog).compile(surface)
}

struct Planner<'a> {
    document: &'a Document,
    catalog: &'a SymbolicCatalog,
    parameters: BTreeMap<&'a str, &'a TypeReference>,
    binding_entities: BTreeMap<&'a str, &'a EntitySymbol>,
    binding_cardinalities: BTreeMap<&'a str, Cardinality>,
    binding_maximum_rows: BTreeMap<&'a str, u64>,
}

impl<'a> Planner<'a> {
    fn new(document: &'a Document, catalog: &'a SymbolicCatalog) -> Self {
        Self {
            document,
            catalog,
            parameters: document
                .parameters
                .iter()
                .map(|parameter| (parameter.name.value.as_str(), &parameter.ty.value))
                .collect(),
            binding_entities: BTreeMap::new(),
            binding_cardinalities: BTreeMap::new(),
            binding_maximum_rows: BTreeMap::new(),
        }
    }

    fn compile(
        mut self,
        surface: riffdb_query_ir::ResolvedQueryV1,
    ) -> Result<QueryAccessProgramV1, PlannerDiagnostics> {
        let mut partition_parameter: Option<String> = None;
        let selections = selected_fields(self.document);
        let dependency_fields = dependency_fields(self.document);
        let result_names = result_names(self.document);
        let mut steps = Vec::with_capacity(self.document.body.bindings.len());
        let mut auth = BTreeMap::<String, AuthAccumulator>::new();
        let mut cost = QueryCostAccumulator::new(self.document);

        for binding in &self.document.body.bindings {
            let entity = self
                .catalog
                .entity(binding.entity.value.as_str())
                .ok_or_else(internal)?;
            let comparisons = comparisons(&binding.predicate.value);
            let maximum_rows = maximum_rows(binding)?;
            self.type_check(entity, binding, &comparisons)?;

            let partition_field = entity.partition_field();
            let route = comparisons.iter().find_map(|comparison| {
                (comparison.field == partition_field
                    && comparison.operator == BinaryOperator::Equal)
                    .then(|| parameter_name(comparison.value))
                    .flatten()
            });
            let Some(route) = route else {
                return Err(one(
                    PlannerDiagnosticCode::NonLocal,
                    binding.predicate.span,
                    vec![entity.name().to_owned(), partition_field.to_owned()],
                    "query access is not routed by an exact partition parameter",
                    None,
                ));
            };
            match &partition_parameter {
                None => partition_parameter = Some(route.to_owned()),
                Some(existing) if existing == route => {}
                Some(_) => {
                    return Err(one(
                        PlannerDiagnosticCode::NonLocal,
                        binding.predicate.span,
                        vec![binding.name.value.as_str().to_owned()],
                        "query bindings do not share one partition route",
                        None,
                    ));
                }
            }

            let row_limit = row_limit(binding, self.document)?;
            let (access, index_id) = choose_access(
                entity,
                binding,
                &comparisons,
                &self.binding_cardinalities,
                &self.binding_maximum_rows,
                self.document,
                maximum_rows,
            )?;
            let predicates = self.normalize_predicates(&comparisons)?;
            let mut predicate_fields = comparisons
                .iter()
                .map(|comparison| comparison.field.to_owned())
                .collect::<BTreeSet<_>>();
            if let Some(fields) = dependency_fields.get(binding.name.value.as_str()) {
                predicate_fields.extend(fields.iter().cloned());
            }
            let mut dependencies = BTreeSet::new();
            collect_expression_dependencies(
                &binding.predicate.value,
                &self.binding_entities,
                &mut dependencies,
            );
            let selected = selections
                .get(binding.name.value.as_str())
                .cloned()
                .unwrap_or_default();
            for order in &binding.order {
                if let Some(field) = path_field(&order.path.value) {
                    predicate_fields.insert(field.to_owned());
                }
            }
            match &access {
                QueryAccessKind::Point { key_fields } => {
                    predicate_fields.extend(key_fields.iter().cloned());
                }
                QueryAccessKind::DependentPointBatch { key_fields, .. } => {
                    predicate_fields.extend(key_fields.iter().cloned());
                }
                QueryAccessKind::Index { fields, .. } => {
                    predicate_fields.extend(fields.iter().cloned());
                }
            }
            let predicate_fields = predicate_fields.into_iter().collect::<Vec<_>>();
            let selected_fields = selected.into_iter().collect::<Vec<_>>();
            let binding_results = result_names
                .get(binding.name.value.as_str())
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .collect::<Vec<_>>();
            let dependencies = dependencies
                .into_iter()
                .map(str::to_owned)
                .collect::<Vec<_>>();
            let step = QueryAccessStep::checked(
                binding.name.value.as_str().to_owned(),
                entity.name().to_owned(),
                binding.cardinality.value,
                maximum_rows,
                row_limit,
                access.clone(),
                predicates,
                predicate_fields.clone(),
                selected_fields.clone(),
                binding_results,
                binding
                    .absence_outcome
                    .as_ref()
                    .map(|outcome| outcome.value.as_str().to_owned()),
                binding
                    .take
                    .as_ref()
                    .and_then(|take| take.after.as_ref())
                    .map(|cursor| cursor.value.as_str().to_owned()),
                dependencies,
                entity.internal_id(),
                index_id,
                entity.internal_primary_key_schema().clone(),
                match &access {
                    QueryAccessKind::Point { .. } | QueryAccessKind::DependentPointBatch { .. } => {
                        None
                    }
                    QueryAccessKind::Index { index, .. } => entity
                        .index(index)
                        .map(|symbol| symbol.internal_key_schema().clone()),
                },
            )
            .ok_or_else(internal)?;
            cost.add_step(entity, &step)?;

            let accumulator = auth
                .entry(entity.name().to_owned())
                .or_insert_with(|| AuthAccumulator::new(entity));
            accumulator.maximum_rows = accumulator
                .maximum_rows
                .checked_add(maximum_rows)
                .ok_or_else(|| {
                    one(
                        PlannerDiagnosticCode::Unbounded,
                        binding.cardinality.span,
                        vec![binding.name.value.as_str().to_owned()],
                        "query row budget exceeds the closed planner bound",
                        None,
                    )
                })?;
            accumulator.fields.extend(predicate_fields);
            accumulator.fields.extend(selected_fields);
            if let QueryAccessKind::Index { index, .. } = access {
                accumulator.indexes.insert(index);
            }
            steps.push(step);
            self.binding_entities
                .insert(binding.name.value.as_str(), entity);
            self.binding_cardinalities
                .insert(binding.name.value.as_str(), binding.cardinality.value);
            self.binding_maximum_rows
                .insert(binding.name.value.as_str(), maximum_rows);
        }

        let authorization = auth
            .into_values()
            .map(AuthAccumulator::finish)
            .collect::<Result<Vec<_>, _>>()?;
        let cost = cost.finish(steps.len())?;
        QueryAccessProgramV1::checked(
            self.catalog.identity().clone(),
            surface,
            self.document
                .name
                .as_ref()
                .map(|name| name.value.as_str().to_owned()),
            partition_parameter.ok_or_else(internal)?,
            steps,
            authorization,
            cost,
        )
        .ok_or_else(internal)
    }

    fn type_check(
        &self,
        entity: &EntitySymbol,
        binding: &riffdb_riffql_syntax::Binding,
        comparisons: &[Comparison<'_>],
    ) -> Result<(), PlannerDiagnostics> {
        for comparison in comparisons {
            let field = entity.field(comparison.field).ok_or_else(internal)?;
            if let Expression::Parameter(parameter) = comparison.value {
                let parameter_type = self
                    .parameters
                    .get(parameter.value.as_str())
                    .ok_or_else(internal)?;
                let expected = field.value_type();
                let actual = if comparison.operator == BinaryOperator::In {
                    match parameter_type {
                        TypeReference::Set(inner) => self.resolve_parameter_type(&inner.value),
                        _ => None,
                    }
                } else {
                    self.resolve_parameter_type(parameter_type)
                };
                let compatible = if comparison.operator == BinaryOperator::In {
                    actual.as_ref().is_some_and(|actual| expected == actual)
                } else {
                    actual.as_ref().is_some_and(|actual| expected == actual)
                };
                if !compatible {
                    return Err(one(
                        PlannerDiagnosticCode::TypeMismatch,
                        parameter.span,
                        vec![entity.name().to_owned(), comparison.field.to_owned()],
                        "predicate operand type does not match the contract field",
                        None,
                    ));
                }
            } else if let Expression::Path(path) = comparison.value
                && path.0.len() == 2
                && let Some(source_entity) = self.binding_entities.get(path.0[0].value.as_str())
            {
                let source_binding = path.0[0].value.as_str();
                let source_cardinality = self
                    .binding_cardinalities
                    .get(source_binding)
                    .copied()
                    .ok_or_else(internal)?;
                let cardinality_is_valid = match comparison.operator {
                    BinaryOperator::In => {
                        source_cardinality == Cardinality::Many
                            && binding.cardinality.value == Cardinality::Many
                    }
                    _ => source_cardinality != Cardinality::Many,
                };
                if !cardinality_is_valid {
                    return Err(one(
                        PlannerDiagnosticCode::Cardinality,
                        path.0[1].span,
                        vec![
                            source_binding.to_owned(),
                            path.0[1].value.as_str().to_owned(),
                        ],
                        "predicate consumes a binding field with incompatible cardinality",
                        None,
                    ));
                }
                let source = source_entity
                    .field(path.0[1].value.as_str())
                    .ok_or_else(internal)?
                    .value_type();
                let compatible = field.value_type() == source
                    || source
                        .optional_inner()
                        .is_some_and(|inner| inner == field.value_type());
                if !compatible {
                    return Err(one(
                        PlannerDiagnosticCode::TypeMismatch,
                        path.0[1].span,
                        vec![entity.name().to_owned(), comparison.field.to_owned()],
                        "dependent field type does not match the contract field",
                        None,
                    ));
                }
            } else if let Expression::Literal(literal) = comparison.value
                && !literal_compatible(field.value_type(), literal)
            {
                return Err(one(
                    PlannerDiagnosticCode::TypeMismatch,
                    Span { start: 0, end: 0 },
                    vec![entity.name().to_owned(), comparison.field.to_owned()],
                    "literal type does not match the contract field",
                    None,
                ));
            }
        }
        Ok(())
    }

    fn normalize_predicates(
        &self,
        comparisons: &[Comparison<'_>],
    ) -> Result<Vec<QueryPredicate>, PlannerDiagnostics> {
        comparisons
            .iter()
            .map(|comparison| {
                let value = match comparison.value {
                    Expression::Parameter(parameter) => {
                        QueryPredicateValue::Parameter(parameter.value.as_str().to_owned())
                    }
                    Expression::Path(path) if path.0.len() == 2 => {
                        let first = path.0[0].value.as_str();
                        let second = path.0[1].value.as_str();
                        if self.binding_entities.contains_key(first) {
                            if self.binding_cardinalities.get(first) == Some(&Cardinality::Many) {
                                QueryPredicateValue::BindingFieldSet {
                                    binding: first.to_owned(),
                                    field: second.to_owned(),
                                }
                            } else {
                                QueryPredicateValue::BindingField {
                                    binding: first.to_owned(),
                                    field: second.to_owned(),
                                }
                            }
                        } else if let Some((type_id, variant_id)) =
                            self.catalog.enumeration(first).and_then(|enumeration| {
                                enumeration
                                    .variant(second)
                                    .map(|variant| (enumeration.internal_id(), variant))
                            })
                        {
                            QueryPredicateValue::EnumVariant {
                                enumeration: first.to_owned(),
                                variant: second.to_owned(),
                                type_id,
                                variant_id,
                            }
                        } else {
                            return Err(internal());
                        }
                    }
                    Expression::Literal(literal) => QueryPredicateValue::Literal(match literal {
                        Literal::Unsigned(value) => QueryLiteral::Unsigned(value.clone()),
                        Literal::String(value) => QueryLiteral::String(value.clone()),
                        Literal::Boolean(value) => QueryLiteral::Boolean(*value),
                        Literal::Null => QueryLiteral::Null,
                    }),
                    Expression::Path(_) | Expression::Binary { .. } => {
                        return Err(internal());
                    }
                };
                QueryPredicate::checked(
                    comparison.field.to_owned(),
                    predicate_operator(comparison.operator)?,
                    value,
                )
                .ok_or_else(internal)
            })
            .collect()
    }

    fn resolve_parameter_type(&self, reference: &TypeReference) -> Option<ValueType> {
        match reference {
            TypeReference::Named(path) if path.0.len() == 2 => self
                .catalog
                .entity(path.0[0].value.as_str())?
                .field(path.0[1].value.as_str())
                .map(|field| field.value_type().clone()),
            TypeReference::Optional(inner) => self
                .resolve_parameter_type(&inner.value)
                .and_then(|inner| ValueType::optional(inner).ok()),
            TypeReference::Named(path) => match path_field(path)? {
                "bool" => Some(ValueType::bool()),
                "i64" => Some(ValueType::i64()),
                "u64" => Some(ValueType::u64()),
                "timestamp" => Some(ValueType::timestamp()),
                "date" => Some(ValueType::date()),
                "uuid" => Some(ValueType::uuid()),
                name => self
                    .catalog
                    .enumeration(name)
                    .map(|enumeration| ValueType::enumeration(enumeration.internal_id())),
            },
            TypeReference::Set(_) | TypeReference::Cursor | TypeReference::Limit => None,
        }
    }
}

struct QueryCostAccumulator {
    primary_span: Span,
    scanned_index_rows: u64,
    point_reads: u64,
    dependent_keys: u64,
    intermediate_rows: u64,
    projected_values: u64,
    encoded_result_bytes: u64,
}

impl QueryCostAccumulator {
    fn new(document: &Document) -> Self {
        let primary_span = document
            .name
            .as_ref()
            .map(|name| name.span)
            .or_else(|| {
                document
                    .body
                    .bindings
                    .first()
                    .map(|binding| binding.cardinality.span)
            })
            .unwrap_or(Span { start: 0, end: 0 });
        let identity_and_envelope = document
            .name
            .as_ref()
            .map_or(0, |name| name.value.as_str().len() as u64)
            .saturating_add(
                document
                    .body
                    .outcomes
                    .iter()
                    .map(|outcome| outcome.value.as_str().len() as u64)
                    .max()
                    .unwrap_or(0),
            )
            .saturating_add(1_024);
        Self {
            primary_span,
            scanned_index_rows: 0,
            point_reads: 0,
            dependent_keys: 0,
            intermediate_rows: 0,
            projected_values: 0,
            encoded_result_bytes: identity_and_envelope,
        }
    }

    fn add_step(
        &mut self,
        entity: &EntitySymbol,
        step: &QueryAccessStep,
    ) -> Result<(), PlannerDiagnostics> {
        let rows = step.maximum_rows();
        match step.access() {
            QueryAccessKind::Point { .. } => {
                self.point_reads = checked_cost_add(self.point_reads, 1, self.primary_span)?;
            }
            QueryAccessKind::DependentPointBatch { .. } => {
                self.point_reads = checked_cost_add(self.point_reads, rows, self.primary_span)?;
                self.dependent_keys =
                    checked_cost_add(self.dependent_keys, rows, self.primary_span)?;
            }
            QueryAccessKind::Index { .. } => {
                self.scanned_index_rows =
                    checked_cost_add(self.scanned_index_rows, rows, self.primary_span)?;
                self.point_reads = checked_cost_add(self.point_reads, rows, self.primary_span)?;
            }
        }
        self.intermediate_rows = checked_cost_add(self.intermediate_rows, rows, self.primary_span)?;

        let result_copies = u64::try_from(step.result_names().len()).map_err(|_| internal())?;
        if result_copies == 0 {
            return Ok(());
        }
        let selected = u64::try_from(step.selected_fields().len()).map_err(|_| internal())?;
        let values = checked_cost_product(
            checked_cost_product(rows, selected, self.primary_span)?,
            result_copies,
            self.primary_span,
        )?;
        self.projected_values = checked_cost_add(self.projected_values, values, self.primary_span)?;

        let mut row_bytes = u64::try_from(entity.name().len())
            .map_err(|_| internal())?
            .checked_add(64)
            .ok_or_else(internal)?;
        for field_name in step.selected_fields() {
            let field = entity.field(field_name).ok_or_else(internal)?;
            let maximum = field
                .value_type()
                .maximum_canonical_bytes()
                .map_err(|_| internal())?
                .ok_or_else(internal)?;
            let field_bytes = u64::try_from(field_name.len())
                .ok()
                .and_then(|name| name.checked_add(maximum as u64))
                .and_then(|value| value.checked_add(160))
                .ok_or_else(internal)?;
            row_bytes = checked_cost_add(row_bytes, field_bytes, self.primary_span)?;
        }
        let record_bytes = checked_cost_product(
            checked_cost_product(rows, result_copies, self.primary_span)?,
            row_bytes,
            self.primary_span,
        )?;
        let result_name_bytes = step.result_names().iter().try_fold(0_u64, |total, name| {
            total
                .checked_add(name.len() as u64)
                .and_then(|value| value.checked_add(64))
                .ok_or_else(internal)
        })?;
        self.encoded_result_bytes =
            checked_cost_add(self.encoded_result_bytes, record_bytes, self.primary_span)?;
        self.encoded_result_bytes = checked_cost_add(
            self.encoded_result_bytes,
            result_name_bytes,
            self.primary_span,
        )?;
        if step.cursor_parameter().is_some() {
            self.encoded_result_bytes =
                checked_cost_add(self.encoded_result_bytes, 128, self.primary_span)?;
        }
        Ok(())
    }

    fn finish(self, steps: usize) -> Result<QueryCostVectorV1, PlannerDiagnostics> {
        QueryCostVectorV1::new(
            u64::try_from(steps).map_err(|_| internal())?,
            self.scanned_index_rows,
            self.point_reads,
            self.dependent_keys,
            self.intermediate_rows,
            self.projected_values,
            self.encoded_result_bytes,
        )
        .ok_or_else(|| {
            one(
                PlannerDiagnosticCode::Unbounded,
                self.primary_span,
                Vec::new(),
                "query whole-request cost exceeds a closed planner ceiling",
                None,
            )
        })
    }
}

fn checked_cost_add(left: u64, right: u64, span: Span) -> Result<u64, PlannerDiagnostics> {
    left.checked_add(right).ok_or_else(|| {
        one(
            PlannerDiagnosticCode::Unbounded,
            span,
            Vec::new(),
            "query whole-request cost arithmetic overflowed",
            None,
        )
    })
}

fn checked_cost_product(left: u64, right: u64, span: Span) -> Result<u64, PlannerDiagnostics> {
    left.checked_mul(right).ok_or_else(|| {
        one(
            PlannerDiagnosticCode::Unbounded,
            span,
            Vec::new(),
            "query whole-request cost arithmetic overflowed",
            None,
        )
    })
}

fn predicate_operator(
    operator: BinaryOperator,
) -> Result<QueryPredicateOperator, PlannerDiagnostics> {
    Ok(match operator {
        BinaryOperator::Equal => QueryPredicateOperator::Equal,
        BinaryOperator::NotEqual => QueryPredicateOperator::NotEqual,
        BinaryOperator::Less => QueryPredicateOperator::Less,
        BinaryOperator::LessEqual => QueryPredicateOperator::LessEqual,
        BinaryOperator::Greater => QueryPredicateOperator::Greater,
        BinaryOperator::GreaterEqual => QueryPredicateOperator::GreaterEqual,
        BinaryOperator::In => QueryPredicateOperator::In,
        BinaryOperator::And | BinaryOperator::Or => return Err(internal()),
    })
}

fn literal_compatible(value_type: &ValueType, literal: &Literal) -> bool {
    match literal {
        Literal::Unsigned(_) => matches!(value_type.tag(), ValueTypeTag::U64 | ValueTypeTag::I64),
        Literal::String(_) => value_type.tag() == ValueTypeTag::String,
        Literal::Boolean(_) => value_type.tag() == ValueTypeTag::Bool,
        Literal::Null => value_type.is_optional(),
    }
}

struct AuthAccumulator<'a> {
    entity: &'a EntitySymbol,
    fields: BTreeSet<String>,
    indexes: BTreeSet<String>,
    maximum_rows: u64,
}

impl<'a> AuthAccumulator<'a> {
    fn new(entity: &'a EntitySymbol) -> Self {
        Self {
            entity,
            fields: BTreeSet::new(),
            indexes: BTreeSet::new(),
            maximum_rows: 0,
        }
    }

    fn finish(self) -> Result<AuthorizationEntityAccess, PlannerDiagnostics> {
        let fields = self.fields.into_iter().collect::<Vec<_>>();
        let field_ids = fields
            .iter()
            .map(|name| {
                self.entity
                    .field(name)
                    .map(|field| field.internal_id())
                    .ok_or_else(internal)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let indexes = self.indexes.into_iter().collect::<Vec<_>>();
        let index_ids = indexes
            .iter()
            .map(|name| {
                self.entity
                    .index(name)
                    .map(|index| index.internal_id())
                    .ok_or_else(internal)
            })
            .collect::<Result<Vec<_>, _>>()?;
        AuthorizationEntityAccess::checked(
            self.entity.name().to_owned(),
            fields,
            indexes,
            self.maximum_rows,
            self.entity.internal_id(),
            field_ids,
            index_ids,
        )
        .ok_or_else(internal)
    }
}

struct Comparison<'a> {
    field: &'a str,
    operator: BinaryOperator,
    value: &'a Expression,
}

fn comparisons(expression: &Expression) -> Vec<Comparison<'_>> {
    let mut output = Vec::new();
    collect_comparisons(expression, &mut output);
    output
}

fn collect_comparisons<'a>(expression: &'a Expression, output: &mut Vec<Comparison<'a>>) {
    if let Expression::Binary {
        operator,
        left,
        right,
    } = expression
    {
        if operator.value == BinaryOperator::And {
            collect_comparisons(&left.value, output);
            collect_comparisons(&right.value, output);
        } else if let Expression::Path(path) = &left.value
            && let Some(field) = path_field(path)
        {
            output.push(Comparison {
                field,
                operator: operator.value,
                value: &right.value,
            });
        }
    }
}

fn choose_access(
    entity: &EntitySymbol,
    binding: &riffdb_riffql_syntax::Binding,
    comparisons: &[Comparison<'_>],
    binding_cardinalities: &BTreeMap<&str, Cardinality>,
    binding_maximum_rows: &BTreeMap<&str, u64>,
    document: &Document,
    maximum_rows: u64,
) -> Result<(QueryAccessKind, Option<riffdb_types::IndexId>), PlannerDiagnostics> {
    let collection_dependencies = comparisons
        .iter()
        .filter_map(|comparison| {
            if comparison.operator != BinaryOperator::In {
                return None;
            }
            let Expression::Path(path) = comparison.value else {
                return None;
            };
            let [source_binding, source_field] = path.0.as_slice() else {
                return None;
            };
            (binding_cardinalities.get(source_binding.value.as_str()) == Some(&Cardinality::Many))
                .then_some((comparison, source_binding, source_field))
        })
        .collect::<Vec<_>>();
    if !collection_dependencies.is_empty() {
        if collection_dependencies.len() != 1 {
            return Err(one(
                PlannerDiagnosticCode::Cardinality,
                binding.predicate.span,
                vec![binding.name.value.as_str().to_owned()],
                "dependent key batch requires exactly one bounded collection field",
                None,
            ));
        }
        let (collection, source_binding, source_field) = collection_dependencies[0];
        let key_fields = entity.primary_key();
        let complete_key = key_fields.iter().all(|field| {
            comparisons.iter().any(|comparison| {
                comparison.field == field
                    && (comparison.operator == BinaryOperator::Equal
                        || std::ptr::eq(comparison, collection))
            })
        });
        let key_only = comparisons
            .iter()
            .all(|comparison| key_fields.iter().any(|field| field == comparison.field));
        let source_maximum = binding_maximum_rows
            .get(source_binding.value.as_str())
            .copied()
            .ok_or_else(internal)?;
        let source = document
            .body
            .bindings
            .iter()
            .find(|candidate| candidate.name.value == source_binding.value)
            .ok_or_else(internal)?;
        let source_order = source
            .order
            .iter()
            .filter_map(|term| path_field(&term.path.value))
            .collect::<Vec<_>>();
        let source_order_is_forward = source
            .order
            .iter()
            .all(|term| term.direction.value == Direction::Ascending);
        let target_order = binding
            .order
            .iter()
            .filter_map(|term| path_field(&term.path.value))
            .collect::<Vec<_>>();
        let forward_order = binding
            .order
            .iter()
            .all(|term| term.direction.value == Direction::Ascending);
        let valid = binding.cardinality.value == Cardinality::Many
            && binding.absence_outcome.is_some()
            && binding
                .take
                .as_ref()
                .is_some_and(|take| take.after.is_none())
            && complete_key
            && key_only
            && maximum_rows <= source_maximum
            && source_order == [source_field.value.as_str()]
            && source_order_is_forward
            && target_order == [collection.field]
            && forward_order
            && collection.field == source_field.value.as_str();
        if !valid {
            return Err(one(
                PlannerDiagnosticCode::Cardinality,
                binding.predicate.span,
                vec![
                    binding.name.value.as_str().to_owned(),
                    source_binding.value.as_str().to_owned(),
                    source_field.value.as_str().to_owned(),
                ],
                "dependent key batch is not a complete ordered bounded primary-key traversal",
                None,
            ));
        }
        return Ok((
            QueryAccessKind::DependentPointBatch {
                key_fields: key_fields.to_vec(),
                source_binding: source_binding.value.as_str().to_owned(),
                source_field: source_field.value.as_str().to_owned(),
            },
            None,
        ));
    }

    if entity.primary_key().iter().all(|field| {
        comparisons.iter().any(|comparison| {
            comparison.field == field && comparison.operator == BinaryOperator::Equal
        })
    }) {
        return Ok((
            QueryAccessKind::Point {
                key_fields: entity.primary_key().to_vec(),
            },
            None,
        ));
    }

    if binding.cardinality.value != Cardinality::Many {
        return Err(one(
            PlannerDiagnosticCode::Unindexed,
            binding.predicate.span,
            vec![entity.name().to_owned()],
            "one or maybe binding does not constrain a complete primary key",
            suggested_index(entity, comparisons, binding),
        ));
    }
    if binding.order.is_empty() {
        return Err(one(
            PlannerDiagnosticCode::Unordered,
            binding.cardinality.span,
            vec![binding.name.value.as_str().to_owned()],
            "many binding has no stable index order",
            suggested_index(entity, comparisons, binding),
        ));
    }
    let order_fields = binding
        .order
        .iter()
        .filter_map(|term| path_field(&term.path.value))
        .collect::<Vec<_>>();
    let first_direction = binding.order[0].direction.value;
    if binding
        .order
        .iter()
        .any(|term| term.direction.value != first_direction)
    {
        return Err(one(
            PlannerDiagnosticCode::Unordered,
            binding.order[0].direction.span,
            vec![binding.name.value.as_str().to_owned()],
            "index ordering must be wholly forward or wholly reverse",
            suggested_index(entity, comparisons, binding),
        ));
    }

    for index in entity.indexes() {
        let mut order_start = 0;
        while order_start < index.fields().len() {
            let field = index.fields()[order_start].as_str();
            let equality = comparisons.iter().any(|comparison| {
                comparison.field == field && comparison.operator == BinaryOperator::Equal
            });
            if equality {
                order_start += 1;
                continue;
            }
            let membership = comparisons.iter().any(|comparison| {
                comparison.field == field && comparison.operator == BinaryOperator::In
            });
            if !membership {
                break;
            }
            break;
        }
        if order_start == 0
            && !comparisons.iter().any(|comparison| {
                comparison.field == index.fields()[0]
                    && matches!(
                        comparison.operator,
                        BinaryOperator::Equal | BinaryOperator::In
                    )
            })
        {
            continue;
        }
        let expected = &index.fields()[order_start..];
        if expected.len() != order_fields.len()
            || !expected
                .iter()
                .map(String::as_str)
                .eq(order_fields.iter().copied())
        {
            continue;
        }
        return Ok((
            QueryAccessKind::Index {
                index: index.name().to_owned(),
                fields: index.fields().to_vec(),
                direction: match first_direction {
                    Direction::Ascending => AccessDirection::Forward,
                    Direction::Descending => AccessDirection::Reverse,
                },
            },
            Some(index.internal_id()),
        ));
    }

    Err(one(
        PlannerDiagnosticCode::Unindexed,
        binding.predicate.span,
        vec![entity.name().to_owned()],
        "no declared index proves the requested bounded order",
        suggested_index(entity, comparisons, binding),
    ))
}

fn suggested_index(
    entity: &EntitySymbol,
    comparisons: &[Comparison<'_>],
    binding: &riffdb_riffql_syntax::Binding,
) -> Option<String> {
    let mut fields = Vec::new();
    for comparison in comparisons {
        if matches!(
            comparison.operator,
            BinaryOperator::Equal | BinaryOperator::In
        ) && !fields.contains(&comparison.field)
        {
            fields.push(comparison.field);
        }
    }
    for order in &binding.order {
        let field = path_field(&order.path.value)?;
        if !fields.contains(&field) {
            fields.push(field);
        }
    }
    (!fields.is_empty()).then(|| {
        format!(
            "index by_query on {} ({})",
            entity.name(),
            fields.join(", ")
        )
    })
}

fn maximum_rows(binding: &riffdb_riffql_syntax::Binding) -> Result<u64, PlannerDiagnostics> {
    if binding.cardinality.value != Cardinality::Many {
        return Ok(1);
    }
    let Some(take) = &binding.take else {
        return Err(one(
            PlannerDiagnosticCode::Unbounded,
            binding.cardinality.span,
            vec![binding.name.value.as_str().to_owned()],
            "many binding has no explicit bound",
            None,
        ));
    };
    match &take.limit.value {
        Expression::Literal(Literal::Unsigned(value)) => value.parse::<u64>().ok(),
        Expression::Parameter(_) => Some(MAX_QUERY_ROWS),
        _ => None,
    }
    .filter(|value| (1..=MAX_QUERY_ROWS).contains(value))
    .ok_or_else(|| {
        one(
            PlannerDiagnosticCode::Unbounded,
            take.limit.span,
            vec![binding.name.value.as_str().to_owned()],
            "binding bound exceeds the service row ceiling",
            None,
        )
    })
}

fn row_limit(
    binding: &riffdb_riffql_syntax::Binding,
    document: &Document,
) -> Result<QueryRowLimit, PlannerDiagnostics> {
    if binding.cardinality.value != Cardinality::Many {
        return Ok(QueryRowLimit::Literal(1));
    }
    let take = binding.take.as_ref().ok_or_else(internal)?;
    match &take.limit.value {
        Expression::Literal(Literal::Unsigned(value)) => value
            .parse::<u64>()
            .ok()
            .map(QueryRowLimit::Literal)
            .ok_or_else(internal),
        Expression::Parameter(parameter) => {
            let declared = document
                .parameters
                .iter()
                .find(|candidate| candidate.name.value == parameter.value)
                .ok_or_else(internal)?;
            let default = match declared.default.as_ref().map(|value| &value.value) {
                Some(Literal::Unsigned(value)) => {
                    Some(value.parse::<u64>().map_err(|_| internal())?)
                }
                None => None,
                Some(_) => return Err(internal()),
            };
            Ok(QueryRowLimit::Parameter {
                name: parameter.value.as_str().to_owned(),
                default,
            })
        }
        _ => Err(internal()),
    }
}

fn selected_fields(document: &Document) -> BTreeMap<String, BTreeSet<String>> {
    let mut output = BTreeMap::new();
    for selection in &document.body.selection.fields {
        collect_selected(selection, &mut output);
    }
    output
}

fn dependency_fields(document: &Document) -> BTreeMap<String, BTreeSet<String>> {
    let mut output = BTreeMap::new();
    for binding in &document.body.bindings {
        collect_dependency_fields(&binding.predicate.value, &mut output);
    }
    output
}

fn collect_dependency_fields(
    expression: &Expression,
    output: &mut BTreeMap<String, BTreeSet<String>>,
) {
    match expression {
        Expression::Path(path) if path.0.len() == 2 => {
            output
                .entry(path.0[0].value.as_str().to_owned())
                .or_default()
                .insert(path.0[1].value.as_str().to_owned());
        }
        Expression::Binary { left, right, .. } => {
            collect_dependency_fields(&left.value, output);
            collect_dependency_fields(&right.value, output);
        }
        Expression::Path(_) | Expression::Parameter(_) | Expression::Literal(_) => {}
    }
}

fn result_names(document: &Document) -> BTreeMap<String, BTreeSet<String>> {
    let mut output = BTreeMap::<String, BTreeSet<String>>::new();
    for selection in &document.body.selection.fields {
        if selection.nested.is_none() {
            continue;
        }
        let Some(binding) = selection.source.value.0.first() else {
            continue;
        };
        let output_name = selection
            .alias
            .as_ref()
            .map_or(binding.value.as_str(), |alias| alias.value.as_str());
        output
            .entry(binding.value.as_str().to_owned())
            .or_default()
            .insert(output_name.to_owned());
    }
    output
}

fn collect_selected(selection: &FieldSelection, output: &mut BTreeMap<String, BTreeSet<String>>) {
    let Some(nested) = &selection.nested else {
        return;
    };
    let Some(binding) = selection.source.value.0.first() else {
        return;
    };
    let fields = output.entry(binding.value.as_str().to_owned()).or_default();
    for field in &nested.fields {
        if field.nested.is_none()
            && let Some(name) = path_field(&field.source.value)
        {
            fields.insert(name.to_owned());
        }
    }
}

fn collect_expression_dependencies<'a>(
    expression: &'a Expression,
    known: &BTreeMap<&'a str, &'a EntitySymbol>,
    output: &mut BTreeSet<&'a str>,
) {
    match expression {
        Expression::Path(path) => {
            if path.0.len() == 2 {
                let name = path.0[0].value.as_str();
                if known.contains_key(name) {
                    output.insert(name);
                }
            }
        }
        Expression::Binary { left, right, .. } => {
            collect_expression_dependencies(&left.value, known, output);
            collect_expression_dependencies(&right.value, known, output);
        }
        Expression::Parameter(_) | Expression::Literal(_) => {}
    }
}

fn path_field(path: &Path) -> Option<&str> {
    path.0.last().map(|part| part.value.as_str())
}

fn parameter_name(expression: &Expression) -> Option<&str> {
    if let Expression::Parameter(parameter) = expression {
        Some(parameter.value.as_str())
    } else {
        None
    }
}

fn one(
    code: PlannerDiagnosticCode,
    primary: Span,
    symbol_path: Vec<String>,
    summary: &'static str,
    suggested_index: Option<String>,
) -> PlannerDiagnostics {
    PlannerDiagnostics(vec![PlannerDiagnostic {
        code,
        primary,
        symbol_path,
        summary,
        suggested_index,
    }])
}

fn internal() -> PlannerDiagnostics {
    one(
        PlannerDiagnosticCode::InternalInvariant,
        Span { start: 0, end: 0 },
        Vec::new(),
        "resolved query could not form a closed access program",
        None,
    )
}
