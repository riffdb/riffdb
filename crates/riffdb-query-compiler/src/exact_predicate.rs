//! Compiler-only closure for ADR-0134 exact predicate and independent-order families.

use std::collections::{BTreeMap, BTreeSet};

use riffdb_contract_ir::{ValueType, ValueTypeTag};
use riffdb_query_ir::{
    ExactComparisonProfileV1, ExactOrderDirectionV1, ExactOrderProgramV1, ExactOrderProgramV2,
    ExactOrderTermV1, ExactOrderTermV2, ExactPredicateLeafV1, ExactPredicateNodeV1,
    ExactPredicateOperatorV1, ExactPredicateProgramV1, ExactPredicateProgramV2,
    ExactProviderRequirementV1, ExactStatePlacementV1, ExactValueSlotV1, OperationalQueryFamilyV1,
    ResolvedQueryV1, SymbolicCatalog, resolve_query_surface, source_aggregate_semantic_identity,
};
use riffdb_riffql_syntax::{
    AggregateFunction, BinaryOperator, Cardinality, Direction, Document, Expression, NullPlacement,
    Path, RIFFQL_LANGUAGE_VERSION_BOUNDED_LIMIT_V1, RIFFQL_LANGUAGE_VERSION_EXACT_PREDICATE_V1,
    RIFFQL_LANGUAGE_VERSION_NULLABLE_EXACT_ORDER_V1, Span, Spanned, TypeReference, UnaryOperator,
};
use riffdb_types::{
    AggregateSemanticIdentityV1, MAX_EXACT_TEXT_ROWS_PER_PARTITION_V1,
    ProjectionProviderPolicyModeV1,
};

use crate::{
    PlannerDiagnosticCode, PlannerDiagnostics, compile_operational_query_family,
    declared_limit_maximum, one,
};

/// Compiler-owned exact predicate operation before a physical provider exists.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompiledExactPredicateQueryV1 {
    program: ExactPredicateProgramV1,
    surface: ResolvedQueryV1,
    value_parameters: Vec<String>,
    presence_parameters: Vec<String>,
    limit_parameter: String,
    offset_parameter: String,
    metadata: OperationalQueryFamilyV1,
}

impl CompiledExactPredicateQueryV1 {
    /// Canonical semantic program and complete provider requirement.
    #[must_use]
    pub const fn program(&self) -> &ExactPredicateProgramV1 {
        &self.program
    }

    /// Exact symbolic query surface.
    #[must_use]
    pub const fn surface(&self) -> &ResolvedQueryV1 {
        &self.surface
    }

    /// Canonically ordered scalar/set parameter names addressed by value slots.
    #[must_use]
    pub fn value_parameters(&self) -> &[String] {
        &self.value_parameters
    }

    /// Canonically ordered optional-presence parameters addressed by family bits.
    #[must_use]
    pub fn presence_parameters(&self) -> &[String] {
        &self.presence_parameters
    }

    /// Typed page limit parameter.
    #[must_use]
    pub fn limit_parameter(&self) -> &str {
        &self.limit_parameter
    }

    /// Typed zero-based ordinal parameter.
    #[must_use]
    pub fn offset_parameter(&self) -> &str {
        &self.offset_parameter
    }

    /// Non-executable ordinary metadata proof used for authorization and cost.
    #[doc(hidden)]
    #[must_use]
    pub const fn metadata(&self) -> &OperationalQueryFamilyV1 {
        &self.metadata
    }
}

/// Compiler-owned exact predicate operation with nullable total-order placement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompiledExactPredicateQueryV2 {
    program: ExactPredicateProgramV2,
    surface: ResolvedQueryV1,
    value_parameters: Vec<String>,
    presence_parameters: Vec<String>,
    limit_parameter: String,
    offset_parameter: String,
    metadata: OperationalQueryFamilyV1,
}

impl CompiledExactPredicateQueryV2 {
    /// Canonical nullable-order semantic program.
    #[must_use]
    pub const fn program(&self) -> &ExactPredicateProgramV2 {
        &self.program
    }

    /// Exact symbolic query surface.
    #[must_use]
    pub const fn surface(&self) -> &ResolvedQueryV1 {
        &self.surface
    }

    /// Canonically ordered scalar/set parameter names addressed by value slots.
    #[must_use]
    pub fn value_parameters(&self) -> &[String] {
        &self.value_parameters
    }

    /// Canonically ordered optional-presence parameters addressed by family bits.
    #[must_use]
    pub fn presence_parameters(&self) -> &[String] {
        &self.presence_parameters
    }

    /// Typed page limit parameter.
    #[must_use]
    pub fn limit_parameter(&self) -> &str {
        &self.limit_parameter
    }

    /// Typed zero-based ordinal parameter.
    #[must_use]
    pub fn offset_parameter(&self) -> &str {
        &self.offset_parameter
    }

    /// Non-executable ordinary metadata proof used for authorization and cost.
    #[doc(hidden)]
    #[must_use]
    pub const fn metadata(&self) -> &OperationalQueryFamilyV1 {
        &self.metadata
    }
}

/// Compiles one V6 source into provider-independent, finite semantic IR.
pub fn compile_exact_predicate_query_v1(
    document: &Document,
    catalog: &SymbolicCatalog,
) -> Result<CompiledExactPredicateQueryV1, PlannerDiagnostics> {
    compile_exact_predicate_query_core(document, catalog, false)
}

fn compile_exact_predicate_query_core(
    document: &Document,
    catalog: &SymbolicCatalog,
    allow_nullable_order: bool,
) -> Result<CompiledExactPredicateQueryV1, PlannerDiagnostics> {
    let primary = document
        .name
        .as_ref()
        .map_or(Span { start: 0, end: 0 }, |name| name.span);
    if !matches!(
        document.language_version,
        RIFFQL_LANGUAGE_VERSION_EXACT_PREDICATE_V1 | RIFFQL_LANGUAGE_VERSION_BOUNDED_LIMIT_V1
    ) || document.body.bindings.len() != 1
        || document.body.aggregates.len() != 1
    {
        return Err(diagnostic(
            primary,
            "exact predicate query shape is incomplete",
        ));
    }
    let surface = resolve_query_surface(document, catalog)
        .map_err(|_| diagnostic(primary, "exact predicate symbolic resolution failed"))?;
    let binding = &document.body.bindings[0];
    if binding.cardinality.value != Cardinality::Many || binding.nearest.is_some() {
        return Err(diagnostic(
            binding.cardinality.span,
            "exact predicate query requires one bounded many binding",
        ));
    }
    let entity = catalog
        .entity(binding.entity.value.as_str())
        .ok_or_else(|| diagnostic(binding.entity.span, "exact predicate entity is unknown"))?;

    let aggregate = &document.body.aggregates[0];
    if aggregate.source.value != binding.name.value
        || !aggregate.group_by.is_empty()
        || aggregate.measures.len() != 1
        || source_aggregate_semantic_identity(aggregate.measures[0].function.value)
            != AggregateSemanticIdentityV1::ExactCount
        || aggregate.measures[0].field.is_some()
    {
        return Err(diagnostic(
            aggregate.name.span,
            "exact predicate query requires one complete exact_count",
        ));
    }
    let take = binding.take.as_ref().ok_or_else(|| {
        diagnostic(
            binding.cardinality.span,
            "exact predicate ordinal window is required",
        )
    })?;
    if take.after.is_some() || take.offset.is_none() {
        return Err(diagnostic(
            take.limit.span,
            "exact predicate query requires offset pagination",
        ));
    }
    let limit_parameter = parameter_name(&take.limit.value).ok_or_else(|| {
        diagnostic(
            take.limit.span,
            "exact predicate limit must be a typed parameter",
        )
    })?;
    let offset = take
        .offset
        .as_ref()
        .ok_or_else(|| diagnostic(take.limit.span, "exact predicate offset is required"))?;
    let offset_parameter = parameter_name(&offset.value).ok_or_else(|| {
        diagnostic(
            offset.span,
            "exact predicate offset must be a typed parameter",
        )
    })?;

    let parameters = document
        .parameters
        .iter()
        .map(|parameter| (parameter.name.value.as_str(), &parameter.ty.value))
        .collect::<BTreeMap<_, _>>();
    let max_limit = declared_limit_maximum(document, &limit_parameter)
        .ok_or_else(|| diagnostic(take.limit.span, "exact predicate limit type is invalid"))?;
    if !matches!(
        parameters.get(limit_parameter.as_str()),
        Some(TypeReference::Limit | TypeReference::BoundedLimit(_))
    ) {
        return Err(diagnostic(
            take.limit.span,
            "exact predicate limit type is invalid",
        ));
    }

    let mut value_names = BTreeSet::new();
    let mut presence_names = BTreeSet::new();
    collect_parameter_names(&binding.predicate, &mut value_names, &mut presence_names)?;
    let value_parameters = value_names.into_iter().collect::<Vec<_>>();
    let presence_parameters = presence_names.into_iter().collect::<Vec<_>>();
    let value_slots = value_parameters
        .iter()
        .enumerate()
        .map(|(ordinal, name)| {
            u16::try_from(ordinal)
                .map(|ordinal| (name.as_str(), ordinal))
                .map_err(|_| diagnostic(primary, "exact predicate parameter bound exceeded"))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let presence_slots = presence_parameters
        .iter()
        .enumerate()
        .map(|(ordinal, name)| {
            u8::try_from(ordinal)
                .map(|ordinal| (name.as_str(), ordinal))
                .map_err(|_| diagnostic(primary, "exact predicate family bound exceeded"))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    for name in &presence_parameters {
        if !matches!(
            parameters.get(name.as_str()),
            Some(TypeReference::Optional(_))
        ) {
            return Err(diagnostic(
                primary,
                "presence guards require declared optional parameters",
            ));
        }
    }

    let mut predicate_fields = BTreeSet::new();
    let predicate = compile_node(
        &binding.predicate,
        entity,
        &parameters,
        &value_slots,
        &presence_slots,
        &mut predicate_fields,
    )?;
    let order = compile_order(binding, entity, allow_nullable_order)?;
    let order_fields = order
        .terms()
        .iter()
        .map(|term| term.field())
        .collect::<BTreeSet<_>>();
    validate_index_requirements(
        entity,
        &predicate_fields,
        &order_fields,
        binding.entity.span,
    )?;

    let policy_mode = if catalog
        .row_policies()
        .any(|policy| policy.entity() == entity.name())
    {
        ProjectionProviderPolicyModeV1::BoundedRowAdmission
    } else {
        ProjectionProviderPolicyModeV1::PartitionAligned
    };
    let max_candidates = u32::try_from(MAX_EXACT_TEXT_ROWS_PER_PARTITION_V1)
        .map_err(|_| diagnostic(primary, "exact predicate provider bound is invalid"))?;
    let work = u64::from(max_candidates)
        .checked_mul(
            u64::try_from(predicate_fields.len() + order_fields.len() + 1)
                .map_err(|_| diagnostic(primary, "exact predicate provider work is invalid"))?,
        )
        .and_then(|work| {
            work.checked_mul(
                u64::try_from(riffdb_types::MAX_EXACT_TEXT_VALUE_BYTES_V1)
                    .expect("fixed exact-text value bound fits u64"),
            )
        })
        .ok_or_else(|| diagnostic(primary, "exact predicate provider work is invalid"))?;
    let requirement = ExactProviderRequirementV1::new(policy_mode, max_candidates, work, 65_536)
        .map_err(|_| diagnostic(primary, "exact predicate provider requirement is invalid"))?;
    let program = ExactPredicateProgramV1::new(
        predicate,
        vec![order],
        u8::try_from(presence_parameters.len())
            .map_err(|_| diagnostic(primary, "exact predicate family bound exceeded"))?,
        true,
        max_candidates,
        max_limit.get(),
        requirement,
    )
    .map_err(|_| diagnostic(primary, "exact predicate family exceeds a static bound"))?;
    let metadata = compile_metadata(document, entity.partition_field(), catalog)?;

    Ok(CompiledExactPredicateQueryV1 {
        program,
        surface,
        value_parameters,
        presence_parameters,
        limit_parameter,
        offset_parameter,
        metadata,
    })
}

/// Compiles one V7 source into provider-independent nullable-order semantic IR.
pub fn compile_nullable_exact_predicate_query_v1(
    document: &Document,
    catalog: &SymbolicCatalog,
) -> Result<CompiledExactPredicateQueryV2, PlannerDiagnostics> {
    let primary = document
        .name
        .as_ref()
        .map_or(Span { start: 0, end: 0 }, |name| name.span);
    if !matches!(
        document.language_version,
        RIFFQL_LANGUAGE_VERSION_NULLABLE_EXACT_ORDER_V1 | RIFFQL_LANGUAGE_VERSION_BOUNDED_LIMIT_V1
    ) {
        return Err(diagnostic(
            primary,
            "nullable exact-order query shape is incomplete",
        ));
    }
    let mut compatibility = document.clone();
    compatibility.language_version = RIFFQL_LANGUAGE_VERSION_EXACT_PREDICATE_V1;
    let compiled = compile_exact_predicate_query_core(&compatibility, catalog, true)?;
    let binding = &document.body.bindings[0];
    let entity = catalog
        .entity(binding.entity.value.as_str())
        .ok_or_else(|| diagnostic(binding.entity.span, "exact predicate entity is unknown"))?;
    let order = compile_nullable_order(binding, entity)?;
    let program = ExactPredicateProgramV2::new(
        compiled.program().predicate().clone(),
        vec![order],
        compiled.program().presence_parameter_count(),
        compiled.program().exact_count(),
        compiled.program().max_offset(),
        compiled.program().max_limit(),
        compiled.program().provider_requirement(),
    )
    .map_err(|_| {
        diagnostic(
            primary,
            "nullable exact-order family exceeds a static bound",
        )
    })?;
    Ok(CompiledExactPredicateQueryV2 {
        program,
        surface: compiled.surface,
        value_parameters: compiled.value_parameters,
        presence_parameters: compiled.presence_parameters,
        limit_parameter: compiled.limit_parameter,
        offset_parameter: compiled.offset_parameter,
        metadata: compiled.metadata,
    })
}

fn compile_metadata(
    document: &Document,
    partition_field: &str,
    catalog: &SymbolicCatalog,
) -> Result<OperationalQueryFamilyV1, PlannerDiagnostics> {
    let mut metadata = document.clone();
    metadata.language_version = riffdb_riffql_syntax::RIFFQL_LANGUAGE_VERSION_OPERATIONAL_V1;
    for binding in &mut metadata.body.bindings {
        binding.predicate = find_partition_predicate(&binding.predicate, partition_field)
            .cloned()
            .ok_or_else(|| {
                diagnostic(
                    binding.predicate.span,
                    "exact predicate partition proof is absent",
                )
            })?;
        for term in &mut binding.order {
            term.direction.value = Direction::Ascending;
        }
        if let Some(take) = &mut binding.take {
            take.offset = None;
        }
    }
    for aggregate in &mut metadata.body.aggregates {
        for measure in &mut aggregate.measures {
            if source_aggregate_semantic_identity(measure.function.value)
                == AggregateSemanticIdentityV1::ExactCount
            {
                measure.function.value = AggregateFunction::Count;
            }
        }
    }
    compile_operational_query_family(&metadata, catalog)
}

fn find_partition_predicate<'a>(
    expression: &'a Spanned<Expression>,
    partition_field: &str,
) -> Option<&'a Spanned<Expression>> {
    match &expression.value {
        Expression::Binary {
            operator,
            left,
            right,
        } => {
            if operator.value == BinaryOperator::Equal
                && expression_field(left) == Some(partition_field)
                && matches!(right.value, Expression::Parameter(_))
            {
                Some(expression)
            } else {
                find_partition_predicate(left, partition_field)
                    .or_else(|| find_partition_predicate(right, partition_field))
            }
        }
        Expression::PresenceGuard { predicate, .. } => {
            find_partition_predicate(predicate, partition_field)
        }
        Expression::Unary { operand, .. } => find_partition_predicate(operand, partition_field),
        Expression::Path(_) | Expression::Parameter(_) | Expression::Literal(_) => None,
    }
}

fn collect_parameter_names(
    expression: &Spanned<Expression>,
    values: &mut BTreeSet<String>,
    presence: &mut BTreeSet<String>,
) -> Result<(), PlannerDiagnostics> {
    match &expression.value {
        Expression::PresenceGuard {
            parameter,
            predicate,
        } => {
            presence.insert(parameter.value.as_str().to_owned());
            collect_parameter_names(predicate, values, presence)
        }
        Expression::Unary { .. } => Ok(()),
        Expression::Binary {
            operator,
            left,
            right,
        } if matches!(operator.value, BinaryOperator::And | BinaryOperator::Or) => {
            collect_parameter_names(left, values, presence)?;
            collect_parameter_names(right, values, presence)
        }
        Expression::Binary { right, .. } => {
            let name = parameter_name(&right.value).ok_or_else(|| {
                diagnostic(
                    right.span,
                    "exact predicate values must be typed parameters",
                )
            })?;
            values.insert(name);
            Ok(())
        }
        Expression::Path(_) | Expression::Parameter(_) | Expression::Literal(_) => Err(diagnostic(
            expression.span,
            "exact predicate expression is incomplete",
        )),
    }
}

fn compile_node(
    expression: &Spanned<Expression>,
    entity: &riffdb_query_ir::EntitySymbol,
    parameters: &BTreeMap<&str, &TypeReference>,
    value_slots: &BTreeMap<&str, u16>,
    presence_slots: &BTreeMap<&str, u8>,
    fields: &mut BTreeSet<riffdb_types::FieldId>,
) -> Result<ExactPredicateNodeV1, PlannerDiagnostics> {
    match &expression.value {
        Expression::PresenceGuard {
            parameter,
            predicate,
        } => {
            Ok(ExactPredicateNodeV1::When {
                presence_ordinal: *presence_slots.get(parameter.value.as_str()).ok_or_else(
                    || diagnostic(parameter.span, "presence guard is not compiler declared"),
                )?,
                child: Box::new(compile_node(
                    predicate,
                    entity,
                    parameters,
                    value_slots,
                    presence_slots,
                    fields,
                )?),
            })
        }
        Expression::Binary {
            operator,
            left,
            right,
        } if matches!(operator.value, BinaryOperator::And | BinaryOperator::Or) => {
            let mut children = Vec::new();
            flatten_boolean(expression, operator.value, &mut children);
            let children = children
                .into_iter()
                .map(|child| {
                    compile_node(
                        child,
                        entity,
                        parameters,
                        value_slots,
                        presence_slots,
                        fields,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(if operator.value == BinaryOperator::And {
                ExactPredicateNodeV1::And(children)
            } else {
                ExactPredicateNodeV1::Or(children)
            })
        }
        Expression::Binary {
            operator,
            left,
            right,
        } => {
            let field_name = expression_field(left).ok_or_else(|| {
                diagnostic(
                    left.span,
                    "exact predicate left operand must be an entity field",
                )
            })?;
            let field = entity
                .field(field_name)
                .ok_or_else(|| diagnostic(left.span, "exact predicate field is unknown"))?;
            let profile = comparison_profile(field.value_type()).ok_or_else(|| {
                diagnostic(left.span, "exact predicate field type is unsupported")
            })?;
            let parameter = parameter_name(&right.value).ok_or_else(|| {
                diagnostic(
                    right.span,
                    "exact predicate value must be a typed parameter",
                )
            })?;
            let parameter_type = parameters
                .get(parameter.as_str())
                .ok_or_else(|| diagnostic(right.span, "exact predicate parameter is undeclared"))?;
            let set = matches!(operator.value, BinaryOperator::In | BinaryOperator::NotIn);
            if set != matches!(parameter_type, TypeReference::Set(_)) {
                return Err(diagnostic(
                    right.span,
                    "membership predicates require a bounded typed set",
                ));
            }
            let operator = predicate_operator(operator.value, profile, operator.span)?;
            fields.insert(field.internal_id());
            ExactPredicateLeafV1::new(
                field.internal_id(),
                operator,
                profile,
                Some(if set {
                    ExactValueSlotV1::Set(*value_slots.get(parameter.as_str()).ok_or_else(
                        || diagnostic(right.span, "exact predicate parameter slot is absent"),
                    )?)
                } else {
                    ExactValueSlotV1::Scalar(*value_slots.get(parameter.as_str()).ok_or_else(
                        || diagnostic(right.span, "exact predicate parameter slot is absent"),
                    )?)
                }),
            )
            .map(ExactPredicateNodeV1::Leaf)
            .map_err(|_| diagnostic(operator_span(expression), "exact predicate leaf is invalid"))
        }
        Expression::Unary { operator, operand } => {
            let field_name = expression_field(operand).ok_or_else(|| {
                diagnostic(
                    operand.span,
                    "state predicate operand must be an entity field",
                )
            })?;
            let field = entity
                .field(field_name)
                .ok_or_else(|| diagnostic(operand.span, "state predicate field is unknown"))?;
            let profile = comparison_profile(field.value_type()).ok_or_else(|| {
                diagnostic(operand.span, "state predicate field type is unsupported")
            })?;
            fields.insert(field.internal_id());
            let operator = match operator.value {
                UnaryOperator::IsNull => ExactPredicateOperatorV1::IsNull,
                UnaryOperator::IsNotNull => ExactPredicateOperatorV1::IsNotNull,
                UnaryOperator::Exists => ExactPredicateOperatorV1::Exists,
            };
            ExactPredicateLeafV1::new(field.internal_id(), operator, profile, None)
                .map(ExactPredicateNodeV1::Leaf)
                .map_err(|_| diagnostic(expression.span, "state predicate is invalid"))
        }
        Expression::Path(_) | Expression::Parameter(_) | Expression::Literal(_) => Err(diagnostic(
            expression.span,
            "exact predicate expression is incomplete",
        )),
    }
}

fn flatten_boolean<'a>(
    expression: &'a Spanned<Expression>,
    expected: BinaryOperator,
    output: &mut Vec<&'a Spanned<Expression>>,
) {
    if let Expression::Binary {
        operator,
        left,
        right,
    } = &expression.value
        && operator.value == expected
    {
        flatten_boolean(left, expected, output);
        flatten_boolean(right, expected, output);
    } else {
        output.push(expression);
    }
}

fn compile_order(
    binding: &riffdb_riffql_syntax::Binding,
    entity: &riffdb_query_ir::EntitySymbol,
    allow_nullable: bool,
) -> Result<ExactOrderProgramV1, PlannerDiagnostics> {
    if binding.order.is_empty() {
        return Err(diagnostic(
            binding.cardinality.span,
            "exact predicate total order is required",
        ));
    }
    let key_fields = entity
        .primary_key()
        .iter()
        .filter(|name| name.as_str() != entity.partition_field())
        .collect::<Vec<_>>();
    if binding.order.len() < key_fields.len() {
        return Err(diagnostic(
            binding.order[0].path.span,
            "exact predicate order requires the complete ascending entity key",
        ));
    }
    let key_start = binding.order.len() - key_fields.len();
    let mut terms = Vec::with_capacity(binding.order.len());
    for (ordinal, term) in binding.order.iter().enumerate() {
        let field_name = path_tail(&term.path.value)
            .ok_or_else(|| diagnostic(term.path.span, "exact predicate order field is invalid"))?;
        let field = entity
            .field(field_name)
            .ok_or_else(|| diagnostic(term.path.span, "exact predicate order field is unknown"))?;
        if !field.is_present_and_non_null_across_lineage() && !allow_nullable {
            return Err(diagnostic(
                term.path.span,
                "exact predicate order field lacks a complete present/non-null proof; add nulls first or nulls last",
            ));
        }
        let profile = comparison_profile(field.value_type()).ok_or_else(|| {
            diagnostic(term.path.span, "exact predicate order type is unsupported")
        })?;
        let key = ordinal >= key_start;
        if key
            && (field_name != key_fields[ordinal - key_start].as_str()
                || term.direction.value != Direction::Ascending)
        {
            return Err(diagnostic(
                term.path.span,
                "exact predicate order requires the complete ascending entity key",
            ));
        }
        terms.push(ExactOrderTermV1::new(
            field.internal_id(),
            profile,
            match term.direction.value {
                Direction::Ascending => ExactOrderDirectionV1::Ascending,
                Direction::Descending => ExactOrderDirectionV1::Descending,
            },
            key,
        ));
    }
    ExactOrderProgramV1::new(terms).map_err(|_| {
        diagnostic(
            binding.order[0].path.span,
            "exact predicate order is incomplete",
        )
    })
}

fn compile_nullable_order(
    binding: &riffdb_riffql_syntax::Binding,
    entity: &riffdb_query_ir::EntitySymbol,
) -> Result<ExactOrderProgramV2, PlannerDiagnostics> {
    if binding.order.is_empty() {
        return Err(diagnostic(
            binding.cardinality.span,
            "nullable exact-order total order is required",
        ));
    }
    let key_fields = entity
        .primary_key()
        .iter()
        .filter(|name| name.as_str() != entity.partition_field())
        .collect::<Vec<_>>();
    if binding.order.len() < key_fields.len() {
        return Err(diagnostic(
            binding.order[0].path.span,
            "exact predicate order requires the complete ascending entity key",
        ));
    }
    let key_start = binding.order.len() - key_fields.len();
    let mut terms = Vec::with_capacity(binding.order.len());
    for (ordinal, term) in binding.order.iter().enumerate() {
        let field_name = path_tail(&term.path.value)
            .ok_or_else(|| diagnostic(term.path.span, "exact predicate order field is invalid"))?;
        let field = entity
            .field(field_name)
            .ok_or_else(|| diagnostic(term.path.span, "exact predicate order field is unknown"))?;
        let profile = comparison_profile(field.value_type()).ok_or_else(|| {
            diagnostic(term.path.span, "exact predicate order type is unsupported")
        })?;
        let key = ordinal >= key_start;
        if key
            && (field_name != key_fields[ordinal - key_start].as_str()
                || term.direction.value != Direction::Ascending
                || term.null_placement.is_some())
        {
            return Err(diagnostic(
                term.path.span,
                "exact predicate order requires a present-only complete ascending entity key",
            ));
        }
        let placement = match term
            .null_placement
            .as_ref()
            .map(|placement| placement.value)
        {
            Some(NullPlacement::First) => ExactStatePlacementV1::NullsFirstV1,
            Some(NullPlacement::Last) => ExactStatePlacementV1::NullsLastV1,
            None if !field.is_present_and_non_null_across_lineage() => {
                return Err(diagnostic(
                    term.path.span,
                    "nullable exact-order field requires nulls first or nulls last",
                ));
            }
            None => ExactStatePlacementV1::PresentOnlyV1,
        };
        terms.push(ExactOrderTermV2::new(
            field.internal_id(),
            profile,
            match term.direction.value {
                Direction::Ascending => ExactOrderDirectionV1::Ascending,
                Direction::Descending => ExactOrderDirectionV1::Descending,
            },
            placement,
            key,
        ));
    }
    ExactOrderProgramV2::new(terms).map_err(|_| {
        diagnostic(
            binding.order[0].path.span,
            "nullable exact-order is incomplete",
        )
    })
}

fn validate_index_requirements(
    entity: &riffdb_query_ir::EntitySymbol,
    predicate_fields: &BTreeSet<riffdb_types::FieldId>,
    order_fields: &BTreeSet<riffdb_types::FieldId>,
    span: Span,
) -> Result<(), PlannerDiagnostics> {
    for field_id in predicate_fields.union(order_fields) {
        let field = entity
            .fields()
            .find(|field| field.internal_id() == *field_id)
            .ok_or_else(|| diagnostic(span, "exact predicate field proof is absent"))?;
        if field.is_key() || field.name() == entity.partition_field() {
            continue;
        }
        let indexed = entity.indexes().any(|index| {
            index
                .fields()
                .first()
                .is_some_and(|name| name == entity.partition_field())
                && index.fields().iter().any(|name| name == field.name())
        });
        if !indexed {
            return Err(diagnostic(
                span,
                "exact predicate family member lacks a declared provider index",
            ));
        }
    }
    Ok(())
}

fn predicate_operator(
    operator: BinaryOperator,
    profile: ExactComparisonProfileV1,
    span: Span,
) -> Result<ExactPredicateOperatorV1, PlannerDiagnostics> {
    let operator = match operator {
        BinaryOperator::Equal => ExactPredicateOperatorV1::Equal,
        BinaryOperator::NotEqual => ExactPredicateOperatorV1::NotEqual,
        BinaryOperator::Less => ExactPredicateOperatorV1::Less,
        BinaryOperator::LessEqual => ExactPredicateOperatorV1::LessEqual,
        BinaryOperator::Greater => ExactPredicateOperatorV1::Greater,
        BinaryOperator::GreaterEqual => ExactPredicateOperatorV1::GreaterEqual,
        BinaryOperator::In => ExactPredicateOperatorV1::In,
        BinaryOperator::NotIn => ExactPredicateOperatorV1::NotIn,
        BinaryOperator::Prefix | BinaryOperator::StartsWith => ExactPredicateOperatorV1::StartsWith,
        BinaryOperator::EndsWith => ExactPredicateOperatorV1::EndsWith,
        BinaryOperator::Contains => ExactPredicateOperatorV1::Contains,
        BinaryOperator::Like
        | BinaryOperator::ILike
        | BinaryOperator::NotLike
        | BinaryOperator::NotILike => {
            return Err(diagnostic(
                span,
                "wildcard operators require a declared pattern provider",
            ));
        }
        BinaryOperator::And | BinaryOperator::Or => {
            return Err(diagnostic(span, "exact predicate Boolean edge is invalid"));
        }
    };
    if matches!(
        operator,
        ExactPredicateOperatorV1::StartsWith
            | ExactPredicateOperatorV1::EndsWith
            | ExactPredicateOperatorV1::Contains
    ) && profile != ExactComparisonProfileV1::BinaryUtf8
    {
        return Err(diagnostic(
            span,
            "exact text predicates require binary UTF-8",
        ));
    }
    Ok(operator)
}

fn comparison_profile(value_type: &ValueType) -> Option<ExactComparisonProfileV1> {
    let value_type = value_type.optional_inner().unwrap_or(value_type);
    Some(match value_type.tag() {
        ValueTypeTag::Bool => ExactComparisonProfileV1::Bool,
        ValueTypeTag::I64 => ExactComparisonProfileV1::I64,
        ValueTypeTag::U64 => ExactComparisonProfileV1::U64,
        ValueTypeTag::Decimal => ExactComparisonProfileV1::Decimal,
        ValueTypeTag::Money => ExactComparisonProfileV1::Money,
        ValueTypeTag::String => ExactComparisonProfileV1::BinaryUtf8,
        ValueTypeTag::Bytes => ExactComparisonProfileV1::Bytes,
        ValueTypeTag::Timestamp => ExactComparisonProfileV1::Timestamp,
        ValueTypeTag::Date => ExactComparisonProfileV1::Date,
        ValueTypeTag::Uuid => ExactComparisonProfileV1::Uuid,
        ValueTypeTag::Enum => ExactComparisonProfileV1::Enum,
        ValueTypeTag::Optional
        | ValueTypeTag::List
        | ValueTypeTag::Record
        | ValueTypeTag::Vector => return None,
    })
}

fn expression_field(expression: &Spanned<Expression>) -> Option<&str> {
    match &expression.value {
        Expression::Path(path) => path_tail(path),
        _ => None,
    }
}

fn parameter_name(expression: &Expression) -> Option<String> {
    match expression {
        Expression::Parameter(parameter) => Some(parameter.value.as_str().to_owned()),
        _ => None,
    }
}

fn path_tail(path: &Path) -> Option<&str> {
    path.0.last().map(|part| part.value.as_str())
}

fn operator_span(expression: &Spanned<Expression>) -> Span {
    match &expression.value {
        Expression::Binary { operator, .. } => operator.span,
        _ => expression.span,
    }
}

fn diagnostic(span: Span, summary: &'static str) -> PlannerDiagnostics {
    one(
        PlannerDiagnosticCode::ExactTextProvider,
        span,
        Vec::new(),
        summary,
        None,
    )
}

/// Produces the closed invariant diagnostic when downstream artifact sealing fails.
#[doc(hidden)]
#[must_use]
pub fn exact_predicate_artifact_invariant(span: Span) -> PlannerDiagnostics {
    diagnostic(span, "exact predicate artifact is inconsistent")
}
