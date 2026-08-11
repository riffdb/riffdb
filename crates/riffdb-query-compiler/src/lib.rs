#![forbid(unsafe_code)]

//! Deterministic type checking, authorization analysis, and bounded planning for RiffQL v1.

mod reactive;

pub use reactive::*;

use std::collections::{BTreeMap, BTreeSet};

use riffdb_contract_ir::{IndexFieldEncodingV1, ValueType, ValueTypeTag};
use riffdb_query_ir::{
    AccessDirection, AuthorizationEntityAccess, EntitySymbol, MAX_OPERATIONAL_PRESENCE_PARAMETERS,
    OperationalPlanMemberV1, OperationalQueryFamilyV1, QueryAccessKind, QueryAccessProgramV1,
    QueryAccessStep, QueryLiteral, QueryPredicate, QueryPredicateOperator, QueryPredicateValue,
    QueryRowLimit, SymbolicCatalog, resolve_query_surface,
};
use riffdb_riffql_syntax::{
    BinaryOperator, Cardinality, Direction, Document, Expression, FieldSelection, Literal, Path,
    Span, Spanned, TypeReference, UnaryOperator,
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
    /// Operational predicates require finite-family compilation.
    OperationalFamilyRequired,
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
            Self::OperationalFamilyRequired => "RDB-QP008",
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
    reject_unlowered_aggregates(document)?;
    if let Some(span) = document
        .body
        .bindings
        .iter()
        .find_map(|binding| first_operational_expression_span(&binding.predicate.value))
    {
        return Err(one(
            PlannerDiagnosticCode::OperationalFamilyRequired,
            span,
            Vec::new(),
            "operational predicates require finite plan-family compilation",
            None,
        ));
    }
    compile_query_member(document, catalog, BTreeSet::new())
}

/// Compiles every optional-presence combination into one closed bounded family.
pub fn compile_operational_query_family(
    document: &Document,
    catalog: &SymbolicCatalog,
) -> Result<OperationalQueryFamilyV1, PlannerDiagnostics> {
    let surface = resolve_query_surface(document, catalog).map_err(|_| {
        one(
            PlannerDiagnosticCode::InternalInvariant,
            Span { start: 0, end: 0 },
            Vec::new(),
            "symbolic resolution failed before operational planning",
            None,
        )
    })?;
    let declared_optional = document
        .parameters
        .iter()
        .filter(|parameter| matches!(parameter.ty.value, TypeReference::Optional(_)))
        .map(|parameter| (parameter.name.value.as_str(), parameter.name.span))
        .collect::<BTreeMap<_, _>>();
    let mut guards = BTreeMap::<String, Span>::new();
    for binding in &document.body.bindings {
        validate_operational_expression(
            &binding.predicate,
            true,
            None,
            &declared_optional,
            &mut guards,
        )?;
    }
    if guards.len() > MAX_OPERATIONAL_PRESENCE_PARAMETERS {
        let span = guards
            .values()
            .nth(MAX_OPERATIONAL_PRESENCE_PARAMETERS)
            .copied()
            .unwrap_or(Span { start: 0, end: 0 });
        return Err(one(
            PlannerDiagnosticCode::Unbounded,
            span,
            Vec::new(),
            "optional predicate combinations exceed the finite family bound",
            None,
        ));
    }
    let presence_parameters = guards.keys().cloned().collect::<Vec<_>>();
    let unwrapped_parameters = presence_parameters.iter().cloned().collect::<BTreeSet<_>>();
    let member_count = 1_usize
        .checked_shl(u32::try_from(presence_parameters.len()).map_err(|_| internal())?)
        .ok_or_else(internal)?;
    let mut members = Vec::with_capacity(member_count);
    for mask in 0..member_count {
        let enabled = presence_parameters
            .iter()
            .enumerate()
            .map(|(bit, name)| (name.as_str(), mask & (1_usize << bit) != 0))
            .collect::<BTreeMap<_, _>>();
        let expanded = expand_operational_document(document, &enabled)?;
        let program = compile_query_member(&expanded, catalog, unwrapped_parameters.clone())?;
        members.push(OperationalPlanMemberV1::checked(
            u16::try_from(mask).map_err(|_| internal())?,
            program,
        ));
    }
    OperationalQueryFamilyV1::checked(surface, presence_parameters, members).ok_or_else(internal)
}

fn compile_query_member(
    document: &Document,
    catalog: &SymbolicCatalog,
    unwrapped_optional_parameters: BTreeSet<String>,
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
    Planner::new(document, catalog, unwrapped_optional_parameters).compile(surface)
}

fn reject_unlowered_aggregates(document: &Document) -> Result<(), PlannerDiagnostics> {
    let Some(aggregate) = document.body.aggregates.first() else {
        return Ok(());
    };
    Err(one(
        PlannerDiagnosticCode::OperationalFamilyRequired,
        aggregate.name.span,
        vec![aggregate.name.value.as_str().to_owned()],
        "operational aggregates require exact aggregate lowering",
        None,
    ))
}

fn validate_operational_expression(
    expression: &Spanned<Expression>,
    top_level_conjunct: bool,
    active_guard: Option<&str>,
    declared_optional: &BTreeMap<&str, Span>,
    guards: &mut BTreeMap<String, Span>,
) -> Result<(), PlannerDiagnostics> {
    match &expression.value {
        Expression::PresenceGuard {
            parameter,
            predicate,
        } => {
            let name = parameter.value.as_str();
            if !top_level_conjunct || active_guard.is_some() {
                return Err(one(
                    PlannerDiagnosticCode::OperationalFamilyRequired,
                    parameter.span,
                    vec![name.to_owned()],
                    "optional predicate guard must be one top-level conjunct",
                    None,
                ));
            }
            if !declared_optional.contains_key(name) {
                return Err(one(
                    PlannerDiagnosticCode::TypeMismatch,
                    parameter.span,
                    vec![name.to_owned()],
                    "presence guard parameter must have an optional type",
                    None,
                ));
            }
            if !expression_references_parameter(&predicate.value, name) {
                return Err(one(
                    PlannerDiagnosticCode::OperationalFamilyRequired,
                    parameter.span,
                    vec![name.to_owned()],
                    "presence guard predicate must consume its guarded parameter",
                    None,
                ));
            }
            guards.entry(name.to_owned()).or_insert(parameter.span);
            validate_operational_expression(predicate, false, Some(name), declared_optional, guards)
        }
        Expression::Parameter(parameter) => {
            let name = parameter.value.as_str();
            if declared_optional.contains_key(name) && active_guard != Some(name) {
                return Err(one(
                    PlannerDiagnosticCode::OperationalFamilyRequired,
                    parameter.span,
                    vec![name.to_owned()],
                    "optional parameter may only appear inside its matching presence guard",
                    None,
                ));
            }
            Ok(())
        }
        Expression::Binary {
            operator,
            left,
            right,
        } => {
            let children_are_conjuncts =
                top_level_conjunct && operator.value == BinaryOperator::And;
            validate_operational_expression(
                left,
                children_are_conjuncts,
                active_guard,
                declared_optional,
                guards,
            )?;
            validate_operational_expression(
                right,
                children_are_conjuncts,
                active_guard,
                declared_optional,
                guards,
            )
        }
        Expression::Unary { operand, .. } => {
            validate_operational_expression(operand, false, active_guard, declared_optional, guards)
        }
        Expression::Path(_) | Expression::Literal(_) => Ok(()),
    }
}

fn expression_references_parameter(expression: &Expression, expected: &str) -> bool {
    match expression {
        Expression::Parameter(parameter) => parameter.value.as_str() == expected,
        Expression::PresenceGuard { predicate, .. } => {
            expression_references_parameter(&predicate.value, expected)
        }
        Expression::Unary { operand, .. } => {
            expression_references_parameter(&operand.value, expected)
        }
        Expression::Binary { left, right, .. } => {
            expression_references_parameter(&left.value, expected)
                || expression_references_parameter(&right.value, expected)
        }
        Expression::Path(_) | Expression::Literal(_) => false,
    }
}

fn expand_operational_document(
    document: &Document,
    enabled: &BTreeMap<&str, bool>,
) -> Result<Document, PlannerDiagnostics> {
    let mut expanded = document.clone();
    for binding in &mut expanded.body.bindings {
        binding.predicate = expand_operational_expression(&binding.predicate, enabled)?
            .ok_or_else(|| {
                one(
                    PlannerDiagnosticCode::NonLocal,
                    binding.predicate.span,
                    vec![binding.name.value.as_str().to_owned()],
                    "optional predicates cannot remove the complete partition route",
                    None,
                )
            })?;
    }
    Ok(expanded)
}

fn expand_operational_expression(
    expression: &Spanned<Expression>,
    enabled: &BTreeMap<&str, bool>,
) -> Result<Option<Spanned<Expression>>, PlannerDiagnostics> {
    match &expression.value {
        Expression::PresenceGuard {
            parameter,
            predicate,
        } => {
            let include = enabled
                .get(parameter.value.as_str())
                .copied()
                .ok_or_else(internal)?;
            Ok(include.then(|| predicate.as_ref().clone()))
        }
        Expression::Binary {
            operator,
            left,
            right,
        } if operator.value == BinaryOperator::And => {
            let left = expand_operational_expression(left, enabled)?;
            let right = expand_operational_expression(right, enabled)?;
            Ok(match (left, right) {
                (Some(left), Some(right)) => Some(Spanned {
                    span: expression.span,
                    value: Expression::Binary {
                        operator: operator.clone(),
                        left: Box::new(left),
                        right: Box::new(right),
                    },
                }),
                (Some(remaining), None) | (None, Some(remaining)) => Some(remaining),
                (None, None) => None,
            })
        }
        Expression::Binary {
            operator,
            left,
            right,
        } => {
            let left = expand_operational_expression(left, enabled)?.ok_or_else(internal)?;
            let right = expand_operational_expression(right, enabled)?.ok_or_else(internal)?;
            Ok(Some(Spanned {
                span: expression.span,
                value: Expression::Binary {
                    operator: operator.clone(),
                    left: Box::new(left),
                    right: Box::new(right),
                },
            }))
        }
        Expression::Unary { operator, operand } => {
            let operand = expand_operational_expression(operand, enabled)?.ok_or_else(internal)?;
            Ok(Some(Spanned {
                span: expression.span,
                value: Expression::Unary {
                    operator: operator.clone(),
                    operand: Box::new(operand),
                },
            }))
        }
        Expression::Parameter(_) | Expression::Path(_) | Expression::Literal(_) => {
            Ok(Some(expression.clone()))
        }
    }
}

struct Planner<'a> {
    document: &'a Document,
    catalog: &'a SymbolicCatalog,
    parameters: BTreeMap<&'a str, &'a TypeReference>,
    binding_entities: BTreeMap<&'a str, &'a EntitySymbol>,
    binding_cardinalities: BTreeMap<&'a str, Cardinality>,
    binding_maximum_rows: BTreeMap<&'a str, u64>,
    unwrapped_optional_parameters: BTreeSet<String>,
}

impl<'a> Planner<'a> {
    fn new(
        document: &'a Document,
        catalog: &'a SymbolicCatalog,
        unwrapped_optional_parameters: BTreeSet<String>,
    ) -> Self {
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
            unwrapped_optional_parameters,
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
                    && comparison.operator.is_binary(BinaryOperator::Equal))
                .then(|| comparison.value.and_then(parameter_name))
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
            let (access, index_id) = if let Some(nearest) = &binding.nearest {
                // ADR-0091: nearest clause produces a Nearest access kind.
                let vector_field_name = nearest.field.value.as_str();
                // Validate the field exists and is a vector type.
                let field_symbol = entity.field(vector_field_name).ok_or_else(|| {
                    one(
                        PlannerDiagnosticCode::Unindexed,
                        nearest.field.span,
                        vec![entity.name().to_owned(), vector_field_name.to_owned()],
                        "nearest references a field that does not exist on the entity",
                        None,
                    )
                })?;
                if field_symbol.value_type().vector_dimension().is_none() {
                    return Err(one(
                        PlannerDiagnosticCode::Unindexed,
                        nearest.field.span,
                        vec![entity.name().to_owned(), vector_field_name.to_owned()],
                        "nearest field is not a vector type",
                        None,
                    ));
                }
                let vector_parameter = nearest.vector.value.as_str().to_owned();
                let k = match &nearest.k.value {
                    riffdb_riffql_syntax::Expression::Literal(
                        riffdb_riffql_syntax::Literal::Unsigned(value),
                    ) => value.parse::<u32>().map_err(|_| {
                        one(
                            PlannerDiagnosticCode::Cardinality,
                            nearest.k.span,
                            vec![],
                            "nearest k must be a positive integer",
                            None,
                        )
                    })?,
                    _ => {
                        return Err(one(
                            PlannerDiagnosticCode::Cardinality,
                            nearest.k.span,
                            vec![],
                            "nearest k must be a positive integer literal",
                            None,
                        ));
                    }
                };
                if k == 0 {
                    return Err(one(
                        PlannerDiagnosticCode::Cardinality,
                        nearest.k.span,
                        vec![],
                        "nearest k must be positive",
                        None,
                    ));
                }
                (
                    QueryAccessKind::Nearest {
                        vector_field: vector_field_name.to_owned(),
                        vector_parameter,
                        k,
                    },
                    None,
                )
            } else {
                choose_access(
                    entity,
                    binding,
                    &comparisons,
                    &self.binding_cardinalities,
                    &self.binding_maximum_rows,
                    self.document,
                    maximum_rows,
                )?
            };
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
                QueryAccessKind::Nearest { vector_field, .. } => {
                    predicate_fields.insert(vector_field.clone());
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
                entity.internal_partition_key_schema().clone(),
                entity.internal_primary_key_schema().clone(),
                match &access {
                    QueryAccessKind::Point { .. } | QueryAccessKind::DependentPointBatch { .. } => {
                        None
                    }
                    QueryAccessKind::Index { index, .. } => entity
                        .index(index)
                        .map(|symbol| symbol.internal_key_schema().clone()),
                    QueryAccessKind::Nearest { .. } => None,
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
        cost.add_aggregates(surface.aggregates(), self.catalog)?;
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
            if matches!(comparison.operator, SourcePredicateOperator::Unary(_)) {
                if !field.value_type().is_optional() {
                    return Err(one(
                        PlannerDiagnosticCode::TypeMismatch,
                        binding.predicate.span,
                        vec![entity.name().to_owned(), comparison.field.to_owned()],
                        "null/existence predicate requires an optional contract field",
                        None,
                    ));
                }
                continue;
            }
            if let Some(Expression::Parameter(parameter)) = comparison.value {
                let parameter_type = self
                    .parameters
                    .get(parameter.value.as_str())
                    .ok_or_else(internal)?;
                let expected = field.value_type();
                let effective_parameter_type = if self
                    .unwrapped_optional_parameters
                    .contains(parameter.value.as_str())
                {
                    match parameter_type {
                        TypeReference::Optional(inner) => &inner.value,
                        _ => parameter_type,
                    }
                } else {
                    parameter_type
                };
                let actual = if comparison.operator.is_binary(BinaryOperator::In) {
                    match effective_parameter_type {
                        TypeReference::Set(inner) => self.resolve_parameter_type(&inner.value),
                        _ => None,
                    }
                } else {
                    self.resolve_parameter_type(effective_parameter_type)
                };
                let compatible = if comparison.operator.is_binary(BinaryOperator::In) {
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
            } else if let Some(Expression::Path(path)) = comparison.value
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
                    SourcePredicateOperator::Binary(BinaryOperator::In) => {
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
            } else if let Some(Expression::Literal(literal)) = comparison.value
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
                    Some(Expression::Parameter(parameter)) => {
                        QueryPredicateValue::Parameter(parameter.value.as_str().to_owned())
                    }
                    Some(Expression::Path(path)) if path.0.len() == 2 => {
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
                    Some(Expression::Literal(literal)) => {
                        QueryPredicateValue::Literal(match literal {
                            Literal::Unsigned(value) => QueryLiteral::Unsigned(value.clone()),
                            Literal::String(value) => QueryLiteral::String(value.clone()),
                            Literal::Boolean(value) => QueryLiteral::Boolean(*value),
                            Literal::Null => QueryLiteral::Null,
                        })
                    }
                    None => QueryPredicateValue::Literal(QueryLiteral::Null),
                    Some(
                        Expression::Path(_)
                        | Expression::PresenceGuard { .. }
                        | Expression::Unary { .. }
                        | Expression::Binary { .. },
                    ) => {
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
            QueryAccessKind::Nearest { .. } => {
                // Exact KNN examines every row in the org partition, bounded
                // only by the physical scan ceiling — the honest static charge
                // is that ceiling, not K (charging the declared output rows
                // under-billed a partition scan by orders of magnitude and
                // left the runtime fuel unable to fund the real scan).
                self.scanned_index_rows = checked_cost_add(
                    self.scanned_index_rows,
                    riffdb_query_ir::MAX_QUERY_SCANNED_ROWS,
                    self.primary_span,
                )?;
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

    fn add_aggregates(
        &mut self,
        aggregates: &[riffdb_query_ir::OperationalAggregateV1],
        catalog: &SymbolicCatalog,
    ) -> Result<(), PlannerDiagnostics> {
        for aggregate in aggregates {
            let entity = catalog
                .entity(aggregate.source_entity())
                .ok_or_else(internal)?;
            let groups = match aggregate.maximum_groups() {
                riffdb_query_ir::PageBound::Literal(value) => *value,
                riffdb_query_ir::PageBound::Parameter(_) => riffdb_query_ir::max_query_page_take(),
            };
            let cells = u64::try_from(aggregate.group_keys().len() + aggregate.measures().len())
                .map_err(|_| internal())?;
            self.projected_values = checked_cost_add(
                self.projected_values,
                checked_cost_product(groups, cells, self.primary_span)?,
                self.primary_span,
            )?;

            let mut group_bytes = 192_u64;
            for key in aggregate.group_keys() {
                group_bytes = checked_cost_add(
                    group_bytes,
                    aggregate_field_bytes(entity, key.field())?,
                    self.primary_span,
                )?;
            }
            for measure in aggregate.measures() {
                let value_bytes = match measure.function() {
                    riffdb_query_ir::OperationalAggregateFunctionV1::Count => 10,
                    riffdb_query_ir::OperationalAggregateFunctionV1::Sum => 48,
                    riffdb_query_ir::OperationalAggregateFunctionV1::Min
                    | riffdb_query_ir::OperationalAggregateFunctionV1::Max => {
                        aggregate_field_bytes(entity, measure.input_field().ok_or_else(internal)?)?
                    }
                };
                group_bytes = checked_cost_add(
                    group_bytes,
                    u64::try_from(measure.alias().len())
                        .ok()
                        .and_then(|name| name.checked_add(value_bytes))
                        .and_then(|value| value.checked_add(160))
                        .ok_or_else(internal)?,
                    self.primary_span,
                )?;
            }
            self.encoded_result_bytes = checked_cost_add(
                self.encoded_result_bytes,
                checked_cost_product(groups, group_bytes, self.primary_span)?,
                self.primary_span,
            )?;
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

fn aggregate_field_bytes(
    entity: &EntitySymbol,
    field_name: &str,
) -> Result<u64, PlannerDiagnostics> {
    let field = entity.field(field_name).ok_or_else(internal)?;
    let maximum = field
        .value_type()
        .maximum_canonical_bytes()
        .map_err(|_| internal())?
        .ok_or_else(internal)?;
    u64::try_from(field_name.len())
        .ok()
        .and_then(|name| name.checked_add(maximum as u64))
        .and_then(|value| value.checked_add(160))
        .ok_or_else(internal)
}

fn predicate_operator(
    operator: SourcePredicateOperator,
) -> Result<QueryPredicateOperator, PlannerDiagnostics> {
    Ok(match operator {
        SourcePredicateOperator::Binary(BinaryOperator::Equal) => QueryPredicateOperator::Equal,
        SourcePredicateOperator::Binary(BinaryOperator::NotEqual) => {
            QueryPredicateOperator::NotEqual
        }
        SourcePredicateOperator::Binary(BinaryOperator::Less) => QueryPredicateOperator::Less,
        SourcePredicateOperator::Binary(BinaryOperator::LessEqual) => {
            QueryPredicateOperator::LessEqual
        }
        SourcePredicateOperator::Binary(BinaryOperator::Greater) => QueryPredicateOperator::Greater,
        SourcePredicateOperator::Binary(BinaryOperator::GreaterEqual) => {
            QueryPredicateOperator::GreaterEqual
        }
        SourcePredicateOperator::Binary(BinaryOperator::In) => QueryPredicateOperator::In,
        SourcePredicateOperator::Binary(BinaryOperator::Prefix) => QueryPredicateOperator::Prefix,
        SourcePredicateOperator::Unary(UnaryOperator::IsNull) => QueryPredicateOperator::IsNull,
        SourcePredicateOperator::Unary(UnaryOperator::IsNotNull) => {
            QueryPredicateOperator::IsNotNull
        }
        SourcePredicateOperator::Unary(UnaryOperator::Exists) => QueryPredicateOperator::Exists,
        SourcePredicateOperator::Binary(BinaryOperator::And | BinaryOperator::Or) => {
            return Err(internal());
        }
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
    operator: SourcePredicateOperator,
    value: Option<&'a Expression>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SourcePredicateOperator {
    Binary(BinaryOperator),
    Unary(UnaryOperator),
}

impl SourcePredicateOperator {
    fn is_binary(self, expected: BinaryOperator) -> bool {
        matches!(self, Self::Binary(actual) if actual == expected)
    }
}

fn comparisons(expression: &Expression) -> Vec<Comparison<'_>> {
    let mut output = Vec::new();
    collect_comparisons(expression, &mut output);
    output
}

fn collect_comparisons<'a>(expression: &'a Expression, output: &mut Vec<Comparison<'a>>) {
    match expression {
        Expression::Binary {
            operator,
            left,
            right,
        } => {
            if operator.value == BinaryOperator::And {
                collect_comparisons(&left.value, output);
                collect_comparisons(&right.value, output);
            } else if let Expression::Path(path) = &left.value
                && let Some(field) = path_field(path)
            {
                output.push(Comparison {
                    field,
                    operator: SourcePredicateOperator::Binary(operator.value),
                    value: Some(&right.value),
                });
            }
        }
        Expression::Unary { operator, operand } => {
            if let Expression::Path(path) = &operand.value
                && let Some(field) = path_field(path)
            {
                output.push(Comparison {
                    field,
                    operator: SourcePredicateOperator::Unary(operator.value),
                    value: None,
                });
            }
        }
        Expression::PresenceGuard { predicate, .. } => {
            collect_comparisons(&predicate.value, output);
        }
        Expression::Parameter(_) | Expression::Path(_) | Expression::Literal(_) => {}
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
            if !comparison.operator.is_binary(BinaryOperator::In) {
                return None;
            }
            let Some(Expression::Path(path)) = comparison.value else {
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
                    && (comparison.operator.is_binary(BinaryOperator::Equal)
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
            comparison.field == field && comparison.operator.is_binary(BinaryOperator::Equal)
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
        if !operational_predicates_supported(index, comparisons) {
            continue;
        }
        let mut order_start = 0;
        while order_start < index.fields().len() {
            let field = index.fields()[order_start].as_str();
            let encoding = index.internal_encodings()[order_start];
            let equality = comparisons.iter().any(|comparison| {
                comparison.field == field
                    && comparison.operator.is_binary(BinaryOperator::Equal)
                    && encoding == IndexFieldEncodingV1::Canonical
            });
            if equality {
                order_start += 1;
                continue;
            }
            let membership = comparisons.iter().any(|comparison| {
                comparison.field == field
                    && comparison.operator.is_binary(BinaryOperator::In)
                    && encoding == IndexFieldEncodingV1::Canonical
            });
            let exact_null = comparisons.iter().any(|comparison| {
                comparison.field == field
                    && comparison.operator == SourcePredicateOperator::Unary(UnaryOperator::IsNull)
                    && encoding == IndexFieldEncodingV1::Presence
            });
            if exact_null {
                order_start += 1;
                continue;
            }
            if !membership {
                break;
            }
            break;
        }
        if order_start == 0
            && !comparisons.iter().any(|comparison| {
                comparison.field == index.fields()[0]
                    && (comparison.operator.is_binary(BinaryOperator::Equal)
                        || comparison.operator.is_binary(BinaryOperator::In))
            })
        {
            continue;
        }
        if !index.fields()[..order_start]
            .iter()
            .any(|field| field == entity.partition_field())
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
        if (comparison.operator.is_binary(BinaryOperator::Equal)
            || comparison.operator.is_binary(BinaryOperator::In)
            || matches!(comparison.operator, SourcePredicateOperator::Unary(_))
            || comparison.operator.is_binary(BinaryOperator::Prefix))
            && !fields.contains(&comparison.field)
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
        let mut suggestion = format!(
            "index by_query on {} ({})",
            entity.name(),
            fields.join(", ")
        );
        for comparison in comparisons {
            match comparison.operator {
                SourcePredicateOperator::Unary(_) => {
                    suggestion.push_str(&format!(" presence({})", comparison.field));
                }
                SourcePredicateOperator::Binary(BinaryOperator::Prefix) => {
                    suggestion
                        .push_str(&format!(" text_key({}, binary_utf8_v1)", comparison.field));
                }
                _ => {}
            }
        }
        suggestion
    })
}

fn operational_predicates_supported(
    index: &riffdb_query_ir::IndexSymbol,
    comparisons: &[Comparison<'_>],
) -> bool {
    let predicates_match = comparisons.iter().all(|comparison| {
        let Some(position) = index
            .fields()
            .iter()
            .position(|field| field == comparison.field)
        else {
            return !matches!(
                comparison.operator,
                SourcePredicateOperator::Unary(_)
                    | SourcePredicateOperator::Binary(BinaryOperator::Prefix)
            );
        };
        match comparison.operator {
            SourcePredicateOperator::Unary(_) => {
                index.internal_encodings()[position] == IndexFieldEncodingV1::Presence
            }
            SourcePredicateOperator::Binary(BinaryOperator::Prefix) => matches!(
                index.internal_encodings()[position],
                IndexFieldEncodingV1::TextKey(_)
            ),
            _ => true,
        }
    });
    predicates_match
        && index.internal_encodings().iter().enumerate().all(
            |(position, encoding)| match encoding {
                IndexFieldEncodingV1::Canonical => true,
                IndexFieldEncodingV1::Presence => comparisons.iter().any(|comparison| {
                    comparison.field == index.fields()[position]
                        && matches!(comparison.operator, SourcePredicateOperator::Unary(_))
                }),
                IndexFieldEncodingV1::TextKey(_) => comparisons.iter().any(|comparison| {
                    comparison.field == index.fields()[position]
                        && comparison.operator.is_binary(BinaryOperator::Prefix)
                }),
            },
        )
}

fn maximum_rows(binding: &riffdb_riffql_syntax::Binding) -> Result<u64, PlannerDiagnostics> {
    if binding.cardinality.value != Cardinality::Many {
        return Ok(1);
    }
    // A nearest binding declares K instead of `take` (the parser rejects
    // combining them); K is the binding's explicit bound (VEC-010). The
    // ceiling here is the resolver's page-take ceiling (499), not
    // MAX_QUERY_ROWS: the resolver already refuses K = 500, so allowing it
    // here would leave two layers holding different beliefs about the same
    // bound (fail-closed but incoherent). For every compilable K (1..=499)
    // the two filters agree, so no plan cost changes.
    if let (None, Some(nearest)) = (&binding.take, &binding.nearest) {
        return match &nearest.k.value {
            Expression::Literal(Literal::Unsigned(value)) => value.parse::<u64>().ok(),
            _ => None,
        }
        .filter(|value| (1..=riffdb_query_ir::max_query_page_take()).contains(value))
        .ok_or_else(|| {
            one(
                PlannerDiagnosticCode::Unbounded,
                nearest.k.span,
                vec![binding.name.value.as_str().to_owned()],
                "nearest k must be a positive literal within the 499 page bound",
                None,
            )
        });
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
    if let (None, Some(nearest)) = (&binding.take, &binding.nearest) {
        // K is the nearest binding's literal row limit (validated positive
        // by the planner's access construction).
        return match &nearest.k.value {
            Expression::Literal(Literal::Unsigned(value)) => value
                .parse::<u64>()
                .ok()
                .map(QueryRowLimit::Literal)
                .ok_or_else(internal),
            _ => Err(internal()),
        };
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
    for aggregate in &document.body.aggregates {
        let fields = output
            .entry(aggregate.source.value.as_str().to_owned())
            .or_default();
        fields.extend(
            aggregate
                .group_by
                .iter()
                .filter_map(|field| path_field(&field.value))
                .map(str::to_owned),
        );
        fields.extend(
            aggregate
                .measures
                .iter()
                .filter_map(|measure| measure.field.as_ref())
                .filter_map(|field| path_field(&field.value))
                .map(str::to_owned),
        );
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
        Expression::PresenceGuard { predicate, .. } => {
            collect_dependency_fields(&predicate.value, output);
        }
        Expression::Unary { operand, .. } => collect_dependency_fields(&operand.value, output),
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
        Expression::PresenceGuard { predicate, .. } => {
            collect_expression_dependencies(&predicate.value, known, output);
        }
        Expression::Unary { operand, .. } => {
            collect_expression_dependencies(&operand.value, known, output);
        }
        Expression::Parameter(_) | Expression::Literal(_) => {}
    }
}

fn first_operational_expression_span(expression: &Expression) -> Option<Span> {
    match expression {
        Expression::PresenceGuard { parameter, .. } => Some(parameter.span),
        Expression::Unary { operator, .. } => Some(operator.span),
        Expression::Binary {
            operator,
            left,
            right,
        } => {
            if operator.value == BinaryOperator::Prefix {
                Some(operator.span)
            } else {
                first_operational_expression_span(&left.value)
                    .or_else(|| first_operational_expression_span(&right.value))
            }
        }
        Expression::Parameter(_) | Expression::Path(_) | Expression::Literal(_) => None,
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
