#![forbid(unsafe_code)]

//! Deterministic type checking, authorization analysis, and bounded planning for RiffQL v1.

mod exact_predicate;
mod reactive;
mod tokenized_text;

pub use exact_predicate::*;
pub use reactive::*;
pub use tokenized_text::*;

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;

use riffdb_contract_ir::{IndexFieldEncodingV1, TextKeyProfileV1, ValueType, ValueTypeTag};
use riffdb_query_ir::{
    AccessDirection, AuthorizationEntityAccess, CandidateBindingV1, CoveredResultFieldV1,
    CoveredResultLayoutV1, CoveredResultSourceV1, EntitySymbol, ExactTextOperatorSetV1,
    ExactTextOrderSetV1, ExactTextPlanFamilyErrorV1, ExactTextPlanFamilyV1,
    MAX_OPERATIONAL_PRESENCE_PARAMETERS, OperationalPlanMemberV1, OperationalQueryFamilyV1,
    OrderFamilyMemberV1, OrderQueryFamilyV1, ProjectedVectorFreshnessV1, ProjectedVectorSourceV1,
    ProjectionResultSetPlanError, ProjectionResultSetPlanV1, ProjectionResultSetPlanV2,
    ProjectionResultSetPlanV2Error, QueryAccessKind, QueryAccessProgramV1, QueryAccessStep,
    QueryLiteral, QueryPredicate, QueryPredicateOperator, QueryPredicateValue,
    QueryRootOrderTermV1, QueryRowLimit, ResultSetOutputShapeV1, ResultSetWindowBoundsV2,
    ResultSetWindowV1, SymbolicCatalog, candidate_source_binding_name, resolve_query_surface,
    source_aggregate_semantic_identity,
};
use riffdb_riffql_syntax::{
    AggregateFunction, BinaryOperator, Cardinality, Direction, Document, Expression,
    FieldSelection, Literal, NullPlacement, Path, ProjectedFreshness,
    RIFFQL_LANGUAGE_VERSION_BOUNDED_LIMIT_V1, RIFFQL_LANGUAGE_VERSION_EXACT_RESULT_SET_V1, Span,
    Spanned, TypeReference, UnaryOperator,
};
use riffdb_types::{
    AggregateResultSchemaV1, AggregateSemanticIdentityV1, EXACT_TEXT_PROVIDER_STATE_SCHEMA_HASH_V1,
    EXACT_TEXT_PROVIDER_STATE_SCHEMA_HASH_V2, EXACT_TEXT_PROVIDER_STATE_SCHEMA_HASH_V3, FieldId,
    MAX_EXACT_TEXT_NEEDLE_BYTES_V1, MAX_EXACT_TEXT_ROWS_PER_PARTITION_V1,
    MAX_EXACT_TEXT_TERMS_PER_ROW_V1, MAX_EXACT_TEXT_VALUE_BYTES_V1,
    ProjectionProviderCapabilitiesV1, ProjectionProviderDescriptorV1, ProjectionProviderKindV1,
    ProjectionProviderPolicyModeV1, ProjectionProviderPostureV1, ProjectionProviderStateIdentityV1,
    ProjectionProviderStaticBoundsV1, QueryCostVectorV1,
};

/// Raw compiler declaration for one finite exact binary UTF-8 family.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExactTextCompilerDeclarationV1 {
    /// Exact indexed contract field.
    pub field: FieldId,
    /// Equals, starts-with, ends-with, and contains membership.
    pub operators: [bool; 4],
    /// Ascending and descending value orders, each with an entity-key tie-breaker.
    pub orders: [bool; 2],
    /// Compiler-proven complete partition or policy shape.
    pub policy_mode: ProjectionProviderPolicyModeV1,
    /// Maximum indexed UTF-8 bytes.
    pub max_value_bytes: u16,
    /// Maximum bound needle bytes.
    pub max_needle_bytes: u16,
    /// Maximum candidates in one policy partition.
    pub max_candidates: u32,
    /// Source span of the complete declaration.
    pub span: Span,
}

impl ExactTextCompilerDeclarationV1 {
    /// Constructs the fixed safe V1 declaration defaults.
    #[must_use]
    pub const fn bounded_binary_utf8(
        field: FieldId,
        operators: [bool; 4],
        policy_mode: ProjectionProviderPolicyModeV1,
        span: Span,
    ) -> Self {
        Self {
            field,
            operators,
            orders: [true, true],
            policy_mode,
            max_value_bytes: MAX_EXACT_TEXT_VALUE_BYTES_V1 as u16,
            max_needle_bytes: MAX_EXACT_TEXT_NEEDLE_BYTES_V1 as u16,
            max_candidates: MAX_EXACT_TEXT_ROWS_PER_PARTITION_V1 as u32,
            span,
        }
    }
}

/// Compiles and pins one exact-text provider without exposing provider choice.
pub fn compile_exact_text_family_v1(
    declaration: ExactTextCompilerDeclarationV1,
) -> Result<ExactTextPlanFamilyV1, PlannerDiagnostics> {
    compile_exact_text_family_with_state_v1(
        declaration,
        NonZeroU32::new(1).expect("state layout is nonzero"),
        EXACT_TEXT_PROVIDER_STATE_SCHEMA_HASH_V1,
    )
}

/// Compiles the activated family whose provider retains typed output rows.
///
/// The compile-only V1 family and checkpoint identity remain available for
/// strict compatibility. Public execution pins V2 so output never requires a
/// remote fetch or hash reversal.
pub fn compile_exact_text_result_family_v1(
    declaration: ExactTextCompilerDeclarationV1,
) -> Result<ExactTextPlanFamilyV1, PlannerDiagnostics> {
    compile_exact_text_family_with_state_v1(
        declaration,
        NonZeroU32::new(2).expect("state layout is nonzero"),
        EXACT_TEXT_PROVIDER_STATE_SCHEMA_HASH_V2,
    )
}

/// Compiles the additive filtered family whose V3 provider binds one typed equality dimension.
pub fn compile_exact_text_filtered_result_family_v1(
    declaration: ExactTextCompilerDeclarationV1,
) -> Result<ExactTextPlanFamilyV1, PlannerDiagnostics> {
    compile_exact_text_family_with_state_v1(
        declaration,
        NonZeroU32::new(3).expect("state layout is nonzero"),
        EXACT_TEXT_PROVIDER_STATE_SCHEMA_HASH_V3,
    )
}

fn compile_exact_text_family_with_state_v1(
    declaration: ExactTextCompilerDeclarationV1,
    state_version: NonZeroU32,
    state_schema_hash: [u8; 32],
) -> Result<ExactTextPlanFamilyV1, PlannerDiagnostics> {
    if declaration.max_value_bytes == 0
        || usize::from(declaration.max_value_bytes) > MAX_EXACT_TEXT_VALUE_BYTES_V1
        || declaration.max_needle_bytes == 0
        || usize::from(declaration.max_needle_bytes) > MAX_EXACT_TEXT_NEEDLE_BYTES_V1
        || declaration.max_candidates == 0
        || usize::try_from(declaration.max_candidates)
            .map_or(true, |count| count > MAX_EXACT_TEXT_ROWS_PER_PARTITION_V1)
    {
        return Err(exact_text_diagnostic(
            declaration.span,
            "exact text provider declaration exceeds a static bound",
        ));
    }
    let descriptor = ProjectionProviderDescriptorV1::new(
        ProjectionProviderKindV1::ExactText,
        ProjectionProviderPostureV1::Exact,
        ProjectionProviderCapabilitiesV1::CANDIDATE
            | ProjectionProviderCapabilitiesV1::FILTER
            | ProjectionProviderCapabilitiesV1::ORDER
            | ProjectionProviderCapabilitiesV1::MEASURE
            | ProjectionProviderCapabilitiesV1::WINDOW
            | ProjectionProviderCapabilitiesV1::OUTPUT,
        declaration.policy_mode,
        ProjectionProviderStaticBoundsV1 {
            max_candidates: declaration.max_candidates,
            max_output_rows: 500,
            max_measures: 1,
            max_input_bytes: u32::from(declaration.max_needle_bytes),
            max_work_units: u64::try_from(MAX_EXACT_TEXT_TERMS_PER_ROW_V1)
                .expect("fixed term bound")
                .saturating_mul(u64::from(declaration.max_candidates)),
            max_state_bytes_per_row: 4_194_304,
            max_diagnostic_bytes: 4_096,
            retained_epochs: 8_192,
            max_catchup_lag: 100,
            max_epoch_lease_steps: 1_024,
        },
        ProjectionProviderStateIdentityV1::new(state_version, state_schema_hash),
    )
    .map_err(|_| exact_text_diagnostic(declaration.span, "exact text provider is inconsistent"))?;
    ExactTextPlanFamilyV1::new(
        declaration.field,
        ExactTextOperatorSetV1::from_flags(declaration.operators),
        ExactTextOrderSetV1::from_flags(declaration.orders),
        declaration.policy_mode,
        declaration.max_value_bytes,
        declaration.max_needle_bytes,
        declaration.max_candidates,
        descriptor,
        declaration.span,
    )
    .map_err(|error| match error {
        ExactTextPlanFamilyErrorV1::StaticBound => exact_text_diagnostic(
            declaration.span,
            "exact text provider declaration exceeds a static bound",
        ),
        ExactTextPlanFamilyErrorV1::EmptyOrUnknownOperatorSet => exact_text_diagnostic(
            declaration.span,
            "exact text provider requires a declared operator",
        ),
        ExactTextPlanFamilyErrorV1::EmptyOrUnknownOrderSet => exact_text_diagnostic(
            declaration.span,
            "exact text provider requires a declared total order",
        ),
        ExactTextPlanFamilyErrorV1::ProviderMismatch => {
            exact_text_diagnostic(declaration.span, "exact text provider is inconsistent")
        }
        ExactTextPlanFamilyErrorV1::NonCanonicalEncoding => {
            exact_text_diagnostic(declaration.span, "exact text provider is inconsistent")
        }
    })
}

fn exact_text_diagnostic(span: Span, summary: &'static str) -> PlannerDiagnostics {
    one(
        PlannerDiagnosticCode::ExactTextProvider,
        span,
        Vec::new(),
        summary,
        None,
    )
}

/// Produces the closed exact-provider invariant diagnostic for a failed
/// downstream artifact seal. Public callers cannot supply the summary.
#[doc(hidden)]
#[must_use]
pub fn exact_text_artifact_invariant(span: Span) -> PlannerDiagnostics {
    exact_text_diagnostic(span, "exact result artifact is inconsistent")
}

/// Compiler-owned pieces of one exact whole-result named query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompiledExactTextQueryV1 {
    /// Exact provider family pinned to durable output-capable state V2.
    pub family: ExactTextPlanFamilyV1,
    /// Count/window/output stage plan.
    pub plan: ProjectionResultSetPlanV2,
    /// Non-executable symbolic metadata proof used for schemas and authority.
    pub metadata: OperationalQueryFamilyV1,
    /// Fixed predicate member.
    pub operator: riffdb_types::ExactTextOperatorV1,
    /// Fixed total order member.
    pub order: riffdb_types::ExactTextOrderV1,
    /// Typed exact needle parameter.
    pub needle_parameter: String,
    /// Typed page limit parameter.
    pub limit_parameter: String,
    /// Typed ordinal parameter.
    pub offset_parameter: String,
    /// Optional compiler-owned typed equality filter.
    pub filter: Option<CompiledExactTextFilterV1>,
}

/// One optional, top-level, typed equality filter executed by provider V3.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompiledExactTextFilterV1 {
    field: FieldId,
    field_name: String,
    parameter: String,
}

impl CompiledExactTextFilterV1 {
    /// Compiler-internal field identity retained by the provider plan.
    #[doc(hidden)]
    #[must_use]
    pub const fn internal_field(&self) -> FieldId {
        self.field
    }

    /// Symbolic filtered field.
    #[must_use]
    pub fn field_name(&self) -> &str {
        &self.field_name
    }

    /// Optional typed parameter controlling filter presence.
    #[must_use]
    pub fn parameter(&self) -> &str {
        &self.parameter
    }
}

/// Compiles the finite ADR-0131 source form into one exact provider plan.
///
/// This is deliberately not a general expression compiler: exactly one
/// partition-routed `many`, one exact text predicate, one total-order field
/// plus the entity primary-key tie breaker, one bounded ordinal window, and
/// one whole-set `exact_count` are admitted.
pub fn compile_exact_text_query_v1(
    document: &Document,
    catalog: &SymbolicCatalog,
) -> Result<CompiledExactTextQueryV1, PlannerDiagnostics> {
    if !matches!(
        document.language_version,
        RIFFQL_LANGUAGE_VERSION_EXACT_RESULT_SET_V1 | RIFFQL_LANGUAGE_VERSION_BOUNDED_LIMIT_V1
    ) || document.body.bindings.len() != 1
        || document.body.aggregates.len() != 1
    {
        return Err(exact_text_diagnostic(
            document
                .name
                .as_ref()
                .map_or(Span { start: 0, end: 0 }, |name| name.span),
            "exact result query must use one bounded binding and one exact count",
        ));
    }
    let binding = &document.body.bindings[0];
    if binding.cardinality.value != Cardinality::Many || binding.nearest.is_some() {
        return Err(exact_text_diagnostic(
            binding.cardinality.span,
            "exact result query requires one bounded many binding",
        ));
    }
    let entity = catalog
        .entity(binding.entity.value.as_str())
        .ok_or_else(|| {
            exact_text_diagnostic(binding.entity.span, "exact result entity is unknown")
        })?;
    let policy_mode = if catalog
        .row_policies()
        .any(|policy| policy.entity() == entity.name())
    {
        ProjectionProviderPolicyModeV1::BoundedRowAdmission
    } else {
        ProjectionProviderPolicyModeV1::PartitionAligned
    };
    let (operator, field_name, needle_parameter, predicate_span) =
        exact_predicate(&binding.predicate, entity.partition_field())?;
    let filter = exact_filter(&binding.predicate)?
        .map(|(filter_field_name, parameter, span)| {
            let field = entity.field(&filter_field_name).ok_or_else(|| {
                exact_text_diagnostic(span, "exact result filter field is unknown")
            })?;
            if filter_field_name == entity.partition_field() {
                return Err(exact_text_diagnostic(
                    span,
                    "exact result filter must be a distinct entity field",
                ));
            }
            Ok(CompiledExactTextFilterV1 {
                field: field.internal_id(),
                field_name: filter_field_name,
                parameter,
            })
        })
        .transpose()?;
    if filter
        .as_ref()
        .is_some_and(|filter| filter.field_name() == field_name)
    {
        return Err(exact_text_diagnostic(
            predicate_span,
            "exact result filter must differ from the exact text field",
        ));
    }
    validate_exact_predicate_completeness(
        &binding.predicate,
        entity.partition_field(),
        &field_name,
        &needle_parameter,
        filter.as_ref(),
    )?;
    let field = entity
        .field(&field_name)
        .ok_or_else(|| exact_text_diagnostic(predicate_span, "exact text field is unknown"))?;
    if field.value_type().tag() != ValueTypeTag::String {
        return Err(exact_text_diagnostic(
            predicate_span,
            "exact text field must be a bounded string",
        ));
    }
    let compatible_index = entity.indexes().find(|index| {
        let fields = index.fields();
        let encodings = index.internal_encodings();
        fields
            .first()
            .is_some_and(|name| name == entity.partition_field())
            && fields
                .iter()
                .position(|name| name == &field_name)
                .is_some_and(|position| {
                    encodings.get(position)
                        == Some(&IndexFieldEncodingV1::TextKey(TextKeyProfileV1::BinaryUtf8))
                })
    });
    if compatible_index.is_none() {
        return Err(exact_text_diagnostic(
            predicate_span,
            "exact text field requires a partition-routed binary_utf8_v1 text key",
        ));
    }
    let first_order = binding.order.first().ok_or_else(|| {
        exact_text_diagnostic(binding.cardinality.span, "exact result order is required")
    })?;
    if path_tail(&first_order.path.value) != Some(field_name.as_str()) {
        return Err(exact_text_diagnostic(
            first_order.path.span,
            "exact result order must begin with the exact text field",
        ));
    }
    let order = match first_order.direction.value {
        Direction::Ascending => riffdb_types::ExactTextOrderV1::ValueAscEntityKey,
        Direction::Descending => riffdb_types::ExactTextOrderV1::ValueDescEntityKey,
    };
    let tie_breakers = binding.order[1..]
        .iter()
        .map(|term| path_tail(&term.path.value))
        .collect::<Option<Vec<_>>>();
    let expected_tie_breakers = entity
        .primary_key()
        .iter()
        .filter(|name| name.as_str() != entity.partition_field())
        .map(String::as_str)
        .collect::<Vec<_>>();
    if tie_breakers.as_deref() != Some(expected_tie_breakers.as_slice())
        || binding.order[1..]
            .iter()
            .any(|term| term.direction.value != Direction::Ascending)
    {
        return Err(exact_text_diagnostic(
            first_order.path.span,
            "exact result order must append the complete ascending entity key tie breaker",
        ));
    }
    let take = binding.take.as_ref().ok_or_else(|| {
        exact_text_diagnostic(binding.cardinality.span, "exact result window is required")
    })?;
    if take.after.is_some() || take.offset.is_none() {
        return Err(exact_text_diagnostic(
            take.limit.span,
            "exact result window requires bounded numeric offset without a cursor",
        ));
    }
    let limit_parameter = exact_parameter_name(&take.limit.value).ok_or_else(|| {
        exact_text_diagnostic(
            take.limit.span,
            "exact result limit must be a typed parameter",
        )
    })?;
    let max_limit = declared_limit_maximum(document, &limit_parameter).ok_or_else(|| {
        exact_text_diagnostic(take.limit.span, "exact result limit type is invalid")
    })?;
    let offset = take.offset.as_ref().expect("checked exact offset");
    let offset_parameter = exact_parameter_name(&offset.value).ok_or_else(|| {
        exact_text_diagnostic(offset.span, "exact result offset must be a typed parameter")
    })?;
    let aggregate = &document.body.aggregates[0];
    if aggregate.source.value != binding.name.value
        || !aggregate.group_by.is_empty()
        || aggregate.measures.len() != 1
        || source_aggregate_semantic_identity(aggregate.measures[0].function.value)
            != AggregateSemanticIdentityV1::ExactCount
        || aggregate.measures[0].field.is_some()
    {
        return Err(exact_text_diagnostic(
            aggregate.name.span,
            "exact result query requires one ungrouped exact_count over the complete binding",
        ));
    }

    let declaration = ExactTextCompilerDeclarationV1::bounded_binary_utf8(
        field.internal_id(),
        operator_flags(operator),
        policy_mode,
        predicate_span,
    );
    let family = if filter.is_some() {
        compile_exact_text_filtered_result_family_v1(declaration)?
    } else {
        compile_exact_text_result_family_v1(declaration)?
    };
    let plan = pin_projection_result_set_provider_v2(
        family.descriptor().clone(),
        ProjectionResultSetRequirementsV2 {
            filtering: true,
            rank_or_order: true,
            whole_set_measures: true,
            window: ResultSetWindowBoundsV2::Ordinal {
                max_offset: family.max_candidates(),
                max_limit,
            },
            output: ResultSetOutputShapeV1::TypedRows,
        },
    )
    .map_err(|_| exact_text_diagnostic(predicate_span, "exact result plan is inconsistent"))?;
    let mut metadata_document = document.clone();
    rewrite_exact_metadata(&mut metadata_document, &field_name, &needle_parameter);
    let metadata = compile_operational_query_family(&metadata_document, catalog)?;
    let expected_presence = filter
        .as_ref()
        .map_or(&[][..], |filter| std::slice::from_ref(&filter.parameter));
    if metadata.presence_parameters() != expected_presence {
        return Err(exact_text_diagnostic(
            predicate_span,
            "optional exact predicate combinations are not available in this profile",
        ));
    }
    Ok(CompiledExactTextQueryV1 {
        family,
        plan,
        metadata,
        operator,
        order,
        needle_parameter,
        limit_parameter,
        offset_parameter,
        filter,
    })
}

pub(crate) fn declared_limit_maximum(
    document: &Document,
    parameter_name: &str,
) -> Option<std::num::NonZeroU16> {
    let maximum = document
        .parameters
        .iter()
        .find(|parameter| parameter.name.value.as_str() == parameter_name)
        .and_then(|parameter| match parameter.ty.value {
            TypeReference::Limit => Some(riffdb_query_ir::max_query_page_take()),
            TypeReference::BoundedLimit(maximum) => Some(maximum),
            _ => None,
        })?;
    u16::try_from(maximum)
        .ok()
        .and_then(std::num::NonZeroU16::new)
}

fn validate_exact_predicate_completeness(
    expression: &Spanned<Expression>,
    partition_field: &str,
    exact_field: &str,
    needle_parameter: &str,
    filter: Option<&CompiledExactTextFilterV1>,
) -> Result<(), PlannerDiagnostics> {
    fn is_path_parameter(
        left: &Spanned<Expression>,
        right: &Spanned<Expression>,
        field: &str,
        parameter: Option<&str>,
    ) -> bool {
        matches!(
            (&left.value, &right.value),
            (Expression::Path(path), Expression::Parameter(candidate))
                if path_tail(path) == Some(field)
                    && parameter.is_none_or(|expected| candidate.value.as_str() == expected)
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn visit(
        expression: &Spanned<Expression>,
        partition_field: &str,
        exact_field: &str,
        needle_parameter: &str,
        filter: Option<&CompiledExactTextFilterV1>,
        partition_seen: &mut bool,
        exact_seen: &mut bool,
        filter_seen: &mut bool,
    ) -> Result<(), PlannerDiagnostics> {
        if let Expression::PresenceGuard {
            parameter,
            predicate,
        } = &expression.value
        {
            let Some(filter) = filter else {
                return Err(exact_text_diagnostic(
                    expression.span,
                    "exact result predicate is not implemented by the selected provider",
                ));
            };
            let Expression::Binary {
                operator,
                left,
                right,
            } = &predicate.value
            else {
                return Err(exact_text_diagnostic(
                    predicate.span,
                    "exact result predicate is not implemented by the selected provider",
                ));
            };
            if !*filter_seen
                && parameter.value.as_str() == filter.parameter()
                && operator.value == BinaryOperator::Equal
                && is_path_parameter(left, right, filter.field_name(), Some(filter.parameter()))
            {
                *filter_seen = true;
                return Ok(());
            }
            return Err(exact_text_diagnostic(
                expression.span,
                "exact result predicate is not implemented by the selected provider",
            ));
        }
        let Expression::Binary {
            operator,
            left,
            right,
        } = &expression.value
        else {
            return Err(exact_text_diagnostic(
                expression.span,
                "exact result predicate is not implemented by the selected provider",
            ));
        };
        if operator.value == BinaryOperator::And {
            visit(
                left,
                partition_field,
                exact_field,
                needle_parameter,
                filter,
                partition_seen,
                exact_seen,
                filter_seen,
            )?;
            return visit(
                right,
                partition_field,
                exact_field,
                needle_parameter,
                filter,
                partition_seen,
                exact_seen,
                filter_seen,
            );
        }
        if operator.value == BinaryOperator::Equal
            && is_path_parameter(left, right, partition_field, None)
            && !*partition_seen
        {
            *partition_seen = true;
            return Ok(());
        }
        if matches!(
            operator.value,
            BinaryOperator::Equal
                | BinaryOperator::StartsWith
                | BinaryOperator::EndsWith
                | BinaryOperator::Contains
        ) && is_path_parameter(left, right, exact_field, Some(needle_parameter))
            && !*exact_seen
        {
            *exact_seen = true;
            return Ok(());
        }
        Err(exact_text_diagnostic(
            expression.span,
            "exact result predicate is not implemented by the selected provider",
        ))
    }

    let mut partition_seen = false;
    let mut exact_seen = false;
    let mut filter_seen = false;
    visit(
        expression,
        partition_field,
        exact_field,
        needle_parameter,
        filter,
        &mut partition_seen,
        &mut exact_seen,
        &mut filter_seen,
    )?;
    if partition_seen && exact_seen && filter.is_some() == filter_seen {
        Ok(())
    } else {
        Err(exact_text_diagnostic(
            expression.span,
            "exact result predicate is not implemented by the selected provider",
        ))
    }
}

fn exact_filter(
    expression: &Spanned<Expression>,
) -> Result<Option<(String, String, Span)>, PlannerDiagnostics> {
    fn visit(
        expression: &Spanned<Expression>,
        found: &mut Option<(String, String, Span)>,
    ) -> Result<(), PlannerDiagnostics> {
        match &expression.value {
            Expression::Binary {
                operator,
                left,
                right,
            } if operator.value == BinaryOperator::And => {
                visit(left, found)?;
                visit(right, found)
            }
            Expression::PresenceGuard {
                parameter,
                predicate,
            } => {
                let Expression::Binary {
                    operator,
                    left,
                    right,
                } = &predicate.value
                else {
                    return Err(exact_text_diagnostic(
                        predicate.span,
                        "exact result filter requires one typed equality",
                    ));
                };
                let (Expression::Path(path), Expression::Parameter(value)) =
                    (&left.value, &right.value)
                else {
                    return Err(exact_text_diagnostic(
                        predicate.span,
                        "exact result filter requires one typed equality",
                    ));
                };
                if operator.value != BinaryOperator::Equal
                    || parameter.value.as_str() != value.value.as_str()
                    || found.is_some()
                {
                    return Err(exact_text_diagnostic(
                        expression.span,
                        "exact result admits one optional typed equality filter",
                    ));
                }
                let field = path_tail(path).ok_or_else(|| {
                    exact_text_diagnostic(predicate.span, "exact result filter field is invalid")
                })?;
                *found = Some((
                    field.to_owned(),
                    parameter.value.as_str().to_owned(),
                    expression.span,
                ));
                Ok(())
            }
            Expression::Binary { .. }
            | Expression::Path(_)
            | Expression::Parameter(_)
            | Expression::Literal(_) => Ok(()),
            Expression::Unary { .. } => Err(exact_text_diagnostic(
                expression.span,
                "exact result predicate is not implemented by the selected provider",
            )),
        }
    }
    let mut found = None;
    visit(expression, &mut found)?;
    Ok(found)
}

fn operator_flags(operator: riffdb_types::ExactTextOperatorV1) -> [bool; 4] {
    let mut flags = [false; 4];
    flags[usize::from(operator as u8) - 1] = true;
    flags
}

fn exact_parameter_name(expression: &Expression) -> Option<String> {
    match expression {
        Expression::Parameter(parameter) => Some(parameter.value.as_str().to_owned()),
        _ => None,
    }
}

fn path_tail(path: &Path) -> Option<&str> {
    path.0.last().map(|part| part.value.as_str())
}

fn exact_predicate(
    expression: &Spanned<Expression>,
    partition_field: &str,
) -> Result<(riffdb_types::ExactTextOperatorV1, String, String, Span), PlannerDiagnostics> {
    fn has_text_operator(expression: &Spanned<Expression>) -> bool {
        match &expression.value {
            Expression::Binary {
                operator,
                left,
                right,
            } => {
                matches!(
                    operator.value,
                    BinaryOperator::StartsWith
                        | BinaryOperator::EndsWith
                        | BinaryOperator::Contains
                ) || has_text_operator(left)
                    || has_text_operator(right)
            }
            Expression::PresenceGuard { .. } => false,
            Expression::Unary { operand, .. } => has_text_operator(operand),
            Expression::Path(_) | Expression::Parameter(_) | Expression::Literal(_) => false,
        }
    }

    fn visit(
        expression: &Spanned<Expression>,
        partition_field: &str,
        allow_equality: bool,
        found: &mut Option<(riffdb_types::ExactTextOperatorV1, String, String, Span)>,
    ) -> Result<(), PlannerDiagnostics> {
        if matches!(expression.value, Expression::PresenceGuard { .. }) {
            return Ok(());
        }
        if let Expression::Binary {
            operator,
            left,
            right,
        } = &expression.value
        {
            let exact = match operator.value {
                BinaryOperator::Equal
                    if allow_equality
                        && !matches!(
                            &left.value,
                            Expression::Path(path)
                                if path_tail(path) == Some(partition_field)
                        ) =>
                {
                    Some(riffdb_types::ExactTextOperatorV1::Equals)
                }
                BinaryOperator::StartsWith => Some(riffdb_types::ExactTextOperatorV1::StartsWith),
                BinaryOperator::EndsWith => Some(riffdb_types::ExactTextOperatorV1::EndsWith),
                BinaryOperator::Contains => Some(riffdb_types::ExactTextOperatorV1::Contains),
                _ => None,
            };
            if let Some(exact) = exact {
                let (Expression::Path(path), Expression::Parameter(parameter)) =
                    (&left.value, &right.value)
                else {
                    return Err(exact_text_diagnostic(
                        operator.span,
                        "exact text predicate requires a field and typed parameter",
                    ));
                };
                let field = path_tail(path).ok_or_else(|| {
                    exact_text_diagnostic(operator.span, "exact text field path is invalid")
                })?;
                if found.is_some() {
                    return Err(exact_text_diagnostic(
                        operator.span,
                        "exact result query admits one exact text predicate",
                    ));
                }
                *found = Some((
                    exact,
                    field.to_owned(),
                    parameter.value.as_str().to_owned(),
                    operator.span,
                ));
            } else {
                visit(left, partition_field, allow_equality, found)?;
                visit(right, partition_field, allow_equality, found)?;
            }
        }
        Ok(())
    }
    let mut found = None;
    let allow_equality = !has_text_operator(expression);
    visit(expression, partition_field, allow_equality, &mut found)?;
    found.ok_or_else(|| {
        exact_text_diagnostic(
            expression.span,
            "exact result query requires one exact text predicate",
        )
    })
}

fn rewrite_exact_metadata(document: &mut Document, exact_field: &str, needle_parameter: &str) {
    fn rewrite(expression: &mut Spanned<Expression>, exact_field: &str, needle_parameter: &str) {
        match &mut expression.value {
            Expression::Binary {
                operator,
                left,
                right,
            } => {
                let exact_equality = operator.value == BinaryOperator::Equal
                    && matches!(
                        (&left.value, &right.value),
                        (Expression::Path(path), Expression::Parameter(parameter))
                            if path_tail(path) == Some(exact_field)
                                && parameter.value.as_str() == needle_parameter
                    );
                if exact_equality
                    || matches!(
                        operator.value,
                        BinaryOperator::StartsWith
                            | BinaryOperator::EndsWith
                            | BinaryOperator::Contains
                    )
                {
                    operator.value = BinaryOperator::Prefix;
                }
                rewrite(left, exact_field, needle_parameter);
                rewrite(right, exact_field, needle_parameter);
            }
            Expression::PresenceGuard { predicate, .. } => {
                rewrite(predicate, exact_field, needle_parameter);
            }
            Expression::Unary { operand, .. } => {
                rewrite(operand, exact_field, needle_parameter);
            }
            Expression::Path(_) | Expression::Parameter(_) | Expression::Literal(_) => {}
        }
    }
    document.language_version = riffdb_riffql_syntax::RIFFQL_LANGUAGE_VERSION_OPERATIONAL_V1;
    for binding in &mut document.body.bindings {
        rewrite(&mut binding.predicate, exact_field, needle_parameter);
        // This ordinary operational plan is authority/schema metadata only.
        // The exact provider owns the source-declared mixed value/key order;
        // normalize this non-executable proof to an index-checkable direction.
        for term in &mut binding.order {
            term.direction.value = Direction::Ascending;
        }
        if let Some(take) = &mut binding.take {
            take.offset = None;
        }
    }
    for aggregate in &mut document.body.aggregates {
        for measure in &mut aggregate.measures {
            if source_aggregate_semantic_identity(measure.function.value)
                == AggregateSemanticIdentityV1::ExactCount
            {
                measure.function.value = AggregateFunction::Count;
            }
        }
    }
}

/// Closed compiler input for the ADR-0130 result-set stage family.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProjectionResultSetRequirementsV1 {
    /// Predicate filtering stage is required.
    pub filtering: bool,
    /// Ranking or exact total ordering stage is required.
    pub rank_or_order: bool,
    /// Whole-admitted-set measures stage is required.
    pub whole_set_measures: bool,
    /// Bounded compiler-owned window.
    pub window: ResultSetWindowV1,
    /// Closed typed output shape.
    pub output: ResultSetOutputShapeV1,
}

/// Pins one validated provider into a sealed result-set plan.
///
/// Application parameters cannot reach this function. The query compiler calls
/// it only after resolving a finite declared plan family from contract and
/// catalog artifacts; there is no runtime selection or fallback branch.
pub fn pin_projection_result_set_provider_v1(
    provider: ProjectionProviderDescriptorV1,
    requirements: ProjectionResultSetRequirementsV1,
) -> Result<ProjectionResultSetPlanV1, ProjectionResultSetPlanError> {
    ProjectionResultSetPlanV1::new(
        provider,
        requirements.filtering,
        requirements.rank_or_order,
        requirements.whole_set_measures,
        requirements.window,
        requirements.output,
    )
}

/// Closed compiler input for parameter-bounded result windows.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProjectionResultSetRequirementsV2 {
    /// Predicate filtering stage is required.
    pub filtering: bool,
    /// Ranking or exact total ordering stage is required.
    pub rank_or_order: bool,
    /// Whole-admitted-set measures stage is required.
    pub whole_set_measures: bool,
    /// Compiler-owned maxima for runtime window values.
    pub window: ResultSetWindowBoundsV2,
    /// Closed typed output shape.
    pub output: ResultSetOutputShapeV1,
}

/// Pins one provider into a plan whose offset and limit are typed values.
pub fn pin_projection_result_set_provider_v2(
    provider: ProjectionProviderDescriptorV1,
    requirements: ProjectionResultSetRequirementsV2,
) -> Result<ProjectionResultSetPlanV2, ProjectionResultSetPlanV2Error> {
    ProjectionResultSetPlanV2::new(
        provider,
        requirements.filtering,
        requirements.rank_or_order,
        requirements.whole_set_measures,
        requirements.window,
        requirements.output,
    )
}

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
    /// Exact or tokenized text provider declaration is unsupported or exceeds a bound.
    ExactTextProvider,
    /// Every cardinality is bounded, but the bounded whole-request cost is over
    /// a closed planner ceiling. Distinct from `Unbounded`: the author declared
    /// a bound and the fix is to lower it, not to add one.
    CostCeilingExceeded,
    /// Candidate algebra, root-key, partition, universe, or consumer proof failed.
    CandidateInvalid,
    /// Long-pattern source/provider proof failed.
    LongPatternInvalid,
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
            Self::ExactTextProvider => "RDB-QP009",
            Self::CostCeilingExceeded => "RDB-QP010",
            Self::CandidateInvalid => "RDB-QP011",
            Self::LongPatternInvalid => "RDB-QP012",
        }
    }
}

/// Closed planner resource a whole-request cost ceiling governs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PlannerBoundResource {
    /// Access steps in one compiled query.
    AccessSteps,
    /// Encoded whole-request result bytes.
    EncodedResultBytes,
}

impl PlannerBoundResource {
    /// Stable lowercase identifier.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AccessSteps => "access_steps",
            Self::EncodedResultBytes => "encoded_result_bytes",
        }
    }
}

/// One closed numeric observation explaining a ceiling rejection.
///
/// Both values are compiler-derived from schema and declared maxima, never from
/// stored data or a submitted request, so reporting them discloses nothing a
/// reader of the contract source could not already compute. This mirrors the
/// contract compiler's `CompilerBoundObservation`, which established that a
/// typed, closed observation is compatible with value-free diagnostics: what
/// stays excluded is free-form text and runtime values, not the bound itself.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PlannerBoundObservation {
    resource: PlannerBoundResource,
    actual: u64,
    maximum: u64,
}

impl PlannerBoundObservation {
    /// The governed resource.
    #[must_use]
    pub const fn resource(self) -> PlannerBoundResource {
        self.resource
    }

    /// Statically charged amount for the whole request.
    #[must_use]
    pub const fn actual(self) -> u64 {
        self.actual
    }

    /// Closed ceiling the amount exceeded.
    #[must_use]
    pub const fn maximum(self) -> u64 {
        self.maximum
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
    bound: Option<PlannerBoundObservation>,
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

    /// The closed numeric observation, when this diagnostic reports a ceiling.
    #[must_use]
    pub const fn bound(&self) -> Option<PlannerBoundObservation> {
        self.bound
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
    if let Some(family) = document
        .body
        .bindings
        .iter()
        .find_map(|binding| binding.order_family.as_ref())
    {
        return Err(one(
            PlannerDiagnosticCode::OperationalFamilyRequired,
            family.span,
            vec![family.parameter.value.as_str().to_owned()],
            "order-family syntax requires finite family compilation",
            None,
        ));
    }
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

/// Expands one contract-enum selector into complete immutable order programs.
pub fn compile_order_query_family(
    document: &Document,
    catalog: &SymbolicCatalog,
) -> Result<OrderQueryFamilyV1, PlannerDiagnostics> {
    let surface = resolve_query_surface(document, catalog).map_err(|_| {
        one(
            PlannerDiagnosticCode::InternalInvariant,
            Span { start: 0, end: 0 },
            Vec::new(),
            "symbolic resolution failed before order-family planning",
            None,
        )
    })?;
    let families = document
        .body
        .bindings
        .iter()
        .enumerate()
        .filter_map(|(index, binding)| binding.order_family.as_ref().map(|family| (index, family)))
        .collect::<Vec<_>>();
    let [(binding_index, family)] = families.as_slice() else {
        return Err(one(
            PlannerDiagnosticCode::OperationalFamilyRequired,
            Span { start: 0, end: 0 },
            Vec::new(),
            "exactly one order family is required",
            None,
        ));
    };
    if document
        .body
        .bindings
        .iter()
        .any(|binding| first_operational_expression_span(&binding.predicate.value).is_some())
    {
        return Err(one(
            PlannerDiagnosticCode::OperationalFamilyRequired,
            family.span,
            Vec::new(),
            "order and optional-presence families cannot be combined in V1",
            None,
        ));
    }
    let parameter = document
        .parameters
        .iter()
        .find(|parameter| parameter.name.value == family.parameter.value)
        .ok_or_else(|| {
            one(
                PlannerDiagnosticCode::TypeMismatch,
                family.parameter.span,
                vec![family.parameter.value.as_str().to_owned()],
                "unknown order-family selector parameter",
                None,
            )
        })?;
    let TypeReference::Named(path) = &parameter.ty.value else {
        return Err(one(
            PlannerDiagnosticCode::TypeMismatch,
            parameter.ty.span,
            Vec::new(),
            "order-family selector must be a contract enum",
            None,
        ));
    };
    let [enum_name] = path.0.as_slice() else {
        return Err(one(
            PlannerDiagnosticCode::TypeMismatch,
            parameter.ty.span,
            Vec::new(),
            "order-family selector must be a contract enum",
            None,
        ));
    };
    let enumeration = catalog
        .enumeration(enum_name.value.as_str())
        .ok_or_else(|| {
            one(
                PlannerDiagnosticCode::TypeMismatch,
                parameter.ty.span,
                Vec::new(),
                "order-family selector must name a contract enum",
                None,
            )
        })?;
    let declared = family
        .variants
        .iter()
        .map(|variant| variant.variant.value.as_str())
        .collect::<BTreeSet<_>>();
    let expected = enumeration.variants().collect::<BTreeSet<_>>();
    if declared != expected || declared.len() != family.variants.len() {
        return Err(one(
            PlannerDiagnosticCode::TypeMismatch,
            family.span,
            Vec::new(),
            "order family must declare every enum variant exactly once",
            None,
        ));
    }
    if family.variants.len() > riffdb_query_ir::MAX_ORDER_FAMILY_MEMBERS_V1 {
        return Err(one(
            PlannerDiagnosticCode::Unbounded,
            family.span,
            Vec::new(),
            "order family exceeds the finite member bound",
            None,
        ));
    }
    let mut members = Vec::with_capacity(family.variants.len());
    for variant in &family.variants {
        let mut expanded = document.clone();
        let binding = &mut expanded.body.bindings[*binding_index];
        binding.order.clone_from(&variant.order);
        binding.order_family = None;
        let program = compile_query_member(&expanded, catalog, BTreeSet::new())?;
        members.push(OrderFamilyMemberV1::checked(
            variant.variant.value.as_str().to_owned(),
            enumeration
                .variant(variant.variant.value.as_str())
                .ok_or_else(internal)?,
            program,
        ));
    }
    OrderQueryFamilyV1::checked(
        surface,
        family.parameter.value.as_str().to_owned(),
        enumeration.internal_id(),
        members,
    )
    .ok_or_else(internal)
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
    candidates: BTreeMap<String, CandidateBindingV1>,
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
            candidates: BTreeMap::new(),
        }
    }

    fn compile(
        mut self,
        surface: riffdb_query_ir::ResolvedQueryV1,
    ) -> Result<QueryAccessProgramV1, PlannerDiagnostics> {
        self.candidates = surface
            .candidates()
            .iter()
            .cloned()
            .map(|candidate| (candidate.name().to_owned(), candidate))
            .collect();
        let projected_source = compile_projected_vector_source(self.document, self.catalog)?;
        let mut partition_parameter: Option<String> = None;
        let selections = selected_fields(self.document);
        let dependency_fields = dependency_fields(self.document);
        let result_names = result_names(self.document);
        let candidate_source_count = surface
            .candidates()
            .iter()
            .map(|candidate| candidate.sources().len())
            .sum::<usize>();
        let mut steps = Vec::with_capacity(
            self.document
                .body
                .bindings
                .len()
                .saturating_add(candidate_source_count),
        );
        let mut auth = BTreeMap::<String, AuthAccumulator>::new();
        let mut cost = QueryCostAccumulator::new(self.document);

        for (candidate_ast, candidate) in self
            .document
            .body
            .candidates
            .iter()
            .zip(surface.candidates())
        {
            let source_nodes = candidate_ast_sources(&candidate_ast.expression);
            for (source_index, (source_ast, source)) in source_nodes
                .into_iter()
                .zip(candidate.sources())
                .enumerate()
            {
                let entity = self.catalog.entity(source.entity()).ok_or_else(internal)?;
                let comparisons = comparisons(&source_ast.predicate.value);
                let ordinary_comparisons = comparisons
                    .iter()
                    .filter(|comparison| {
                        source.long_pattern().is_none_or(|pattern| {
                            !comparison_is_long_pattern_invocation(comparison, pattern)
                        })
                    })
                    .map(|comparison| Comparison {
                        field: comparison.field,
                        operator: comparison.operator,
                        value: comparison.value,
                    })
                    .collect::<Vec<_>>();
                self.type_check_candidate(
                    entity,
                    source_ast.predicate.span,
                    &ordinary_comparisons,
                )?;
                if candidate.operator() == riffdb_query_ir::CandidateSetOperatorV1::Difference
                    && source_index == 0
                    && (source.entity() != candidate.root_entity()
                        || source.projected_key() != candidate.root_key()
                        || comparisons.len() != 1
                        || comparisons[0].field != entity.partition_field()
                        || !comparisons[0].operator.is_binary(BinaryOperator::Equal)
                        || comparisons[0].value.and_then(parameter_name).is_none())
                {
                    return Err(one(
                        PlannerDiagnosticCode::CandidateInvalid,
                        source_ast.span,
                        vec![candidate.name().to_owned()],
                        "candidate difference requires one policy-filtered partition-complete positive root universe",
                        None,
                    ));
                }
                let partition_field = entity.partition_field();
                let route = comparisons.iter().find_map(|comparison| {
                    (comparison.field == partition_field
                        && comparison.operator.is_binary(BinaryOperator::Equal))
                    .then(|| comparison.value.and_then(parameter_name))
                    .flatten()
                });
                let Some(route) = route else {
                    return Err(one(
                        PlannerDiagnosticCode::CandidateInvalid,
                        source_ast.predicate.span,
                        vec![entity.name().to_owned(), partition_field.to_owned()],
                        "candidate source is not routed by an exact partition parameter",
                        None,
                    ));
                };
                match &partition_parameter {
                    None => partition_parameter = Some(route.to_owned()),
                    Some(existing) if existing == route => {}
                    Some(_) => {
                        return Err(one(
                            PlannerDiagnosticCode::CandidateInvalid,
                            source_ast.predicate.span,
                            vec![candidate.name().to_owned()],
                            "candidate sources do not share one partition route",
                            None,
                        ));
                    }
                }
                let (access, index_id, index_key_schema) = if let Some(pattern) =
                    source.long_pattern()
                {
                    let provider = entity
                        .long_pattern_index(source.access())
                        .ok_or_else(internal)?;
                    if u32::from(candidate.maximum_distinct_keys()) > pattern.bounds().candidates()
                    {
                        return Err(one(
                            PlannerDiagnosticCode::CandidateInvalid,
                            source_ast.span,
                            vec![entity.name().to_owned(), source.access().to_owned()],
                            "candidate ceiling exceeds the declared pattern provider ceiling",
                            None,
                        ));
                    }
                    (
                        QueryAccessKind::LongPatternCandidate {
                            provider: source.access().to_owned(),
                            pattern: pattern.clone(),
                        },
                        Some(provider.id()),
                        None,
                    )
                } else {
                    let index = entity.index(source.access()).ok_or_else(internal)?;
                    if !candidate_index_is_complete_prefix(index, &comparisons) {
                        return Err(one(
                            PlannerDiagnosticCode::CandidateInvalid,
                            source_ast.span,
                            vec![entity.name().to_owned(), source.access().to_owned()],
                            "candidate source predicates are not a complete bounded index prefix",
                            None,
                        ));
                    }
                    (
                        QueryAccessKind::Index {
                            index: index.name().to_owned(),
                            fields: index.fields().to_vec(),
                            direction: AccessDirection::Forward,
                        },
                        Some(index.internal_id()),
                        Some(index.internal_key_schema().clone()),
                    )
                };
                let predicates = self.normalize_predicates(&ordinary_comparisons)?;
                let predicate_fields = ordinary_comparisons
                    .iter()
                    .map(|comparison| comparison.field.to_owned())
                    .chain(
                        source
                            .long_pattern()
                            .into_iter()
                            .map(|pattern| pattern.field().to_owned()),
                    )
                    .chain(std::iter::once(source.projected_key().to_owned()))
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>();
                let binding = candidate_source_binding_name(candidate.name(), source_index)
                    .ok_or_else(internal)?;
                let step = QueryAccessStep::checked(
                    binding,
                    entity.name().to_owned(),
                    Cardinality::Many,
                    u64::from(candidate.maximum_distinct_keys()),
                    QueryRowLimit::CandidateComplete {
                        maximum: candidate.maximum_distinct_keys(),
                    },
                    access,
                    predicates,
                    predicate_fields.clone(),
                    vec![source.projected_key().to_owned()],
                    Vec::new(),
                    Some(candidate.refusal_outcome().to_owned()),
                    None,
                    Vec::new(),
                    entity.internal_id(),
                    index_id,
                    entity.internal_partition_key_schema().clone(),
                    entity.internal_primary_key_schema().clone(),
                    index_key_schema,
                    None,
                )
                .ok_or_else(internal)?;
                cost.add_step(entity, &step)?;
                let accumulator = auth
                    .entry(entity.name().to_owned())
                    .or_insert_with(|| AuthAccumulator::new(entity));
                accumulator.maximum_rows = accumulator
                    .maximum_rows
                    .checked_add(u64::from(candidate.maximum_distinct_keys()))
                    .ok_or_else(internal)?;
                accumulator.fields.extend(predicate_fields);
                accumulator.indexes.insert(source.access().to_owned());
                steps.push(step);
            }
        }

        for binding in &self.document.body.bindings {
            let entity = self
                .catalog
                .entity(binding.entity.value.as_str())
                .ok_or_else(internal)?;
            let comparisons = comparisons(&binding.predicate.value);
            let maximum_rows = maximum_rows(binding, self.document)?;
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
            let mut cover_required_fields = comparisons
                .iter()
                .map(|comparison| comparison.field.to_owned())
                .collect::<BTreeSet<_>>();
            if let Some(fields) = selections.get(binding.name.value.as_str()) {
                cover_required_fields.extend(fields.iter().cloned());
            }
            if let Some(fields) = dependency_fields.get(binding.name.value.as_str()) {
                cover_required_fields.extend(fields.iter().cloned());
            }
            for order in &binding.order {
                if let Some(field) = path_field(&order.path.value) {
                    cover_required_fields.insert(field.to_owned());
                }
            }
            let candidate_membership = comparisons.iter().find_map(|comparison| {
                if !comparison.operator.is_binary(BinaryOperator::In) {
                    return None;
                }
                let Some(Expression::Path(path)) = comparison.value else {
                    return None;
                };
                let [name] = path.0.as_slice() else {
                    return None;
                };
                self.candidates.get(name.value.as_str())
            });
            let (access, index_id) = if let Some(candidate) = candidate_membership {
                let order = binding
                    .order
                    .iter()
                    .map(|term| {
                        QueryRootOrderTermV1::checked(
                            path_field(&term.path.value)
                                .ok_or_else(internal)?
                                .to_owned(),
                            match term.direction.value {
                                Direction::Ascending => AccessDirection::Forward,
                                Direction::Descending => AccessDirection::Reverse,
                            },
                            matches!(
                                term.null_placement
                                    .as_ref()
                                    .map(|placement| placement.value),
                                Some(NullPlacement::First)
                            ),
                        )
                        .ok_or_else(internal)
                    })
                    .collect::<Result<Vec<_>, PlannerDiagnostics>>()?;
                (
                    QueryAccessKind::CandidateRootHydration {
                        candidate: candidate.name().to_owned(),
                        maximum_candidates: candidate.maximum_distinct_keys(),
                        key_fields: entity.primary_key().to_vec(),
                        order,
                    },
                    None,
                )
            } else if let Some(nearest) = &binding.nearest {
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
                // Nearest.k is the compiler-proven maximum retained in the
                // stable plan encoding. Literal K uses its exact value;
                // parameterized K uses maximum_rows (the checked page ceiling),
                // while row_limit retains the runtime parameter binding.
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
                    riffdb_riffql_syntax::Expression::Parameter(_) => {
                        u32::try_from(maximum_rows).map_err(|_| internal())?
                    }
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
                if k == 0 || u64::from(k) > riffdb_query_ir::max_query_page_take() {
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
                    AccessContext {
                        binding_maximum_rows: &self.binding_maximum_rows,
                        document: self.document,
                        maximum_rows,
                        cover_required_fields: &cover_required_fields,
                    },
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
                QueryAccessKind::LongPatternCandidate { .. } => return Err(internal()),
                QueryAccessKind::Nearest { vector_field, .. } => {
                    predicate_fields.insert(vector_field.clone());
                }
                QueryAccessKind::CandidateRootHydration {
                    key_fields, order, ..
                } => {
                    predicate_fields.extend(key_fields.iter().cloned());
                    predicate_fields.extend(order.iter().map(|term| term.field().to_owned()));
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
            let covered_result_layout = match &access {
                QueryAccessKind::Index { index, .. }
                    if surface.aggregates().is_empty()
                        && self.document.body.bindings.len() == 1
                        && binding.cardinality.value == Cardinality::Many
                        && binding_results.len() == 1
                        && selected_fields.iter().all(|name| {
                            entity.field(name).is_some_and(|field| {
                                compact_result_type_supported(field.value_type())
                            })
                        })
                        && dependency_fields
                            .get(binding.name.value.as_str())
                            .is_none_or(BTreeSet::is_empty)
                        && !self.catalog.internal_has_read_policy(entity.name()) =>
                {
                    entity.index(index).and_then(|index_symbol| {
                        (!index_symbol.cover_fields().is_empty()
                            && index_covers_fields(entity, index_symbol, &cover_required_fields))
                        .then(|| {
                            cover_required_fields
                                .iter()
                                .map(|name| {
                                    let field = entity.field(name)?;
                                    let source = if let Some(position) = index_symbol
                                        .fields()
                                        .iter()
                                        .position(|candidate| candidate == name)
                                    {
                                        CoveredResultSourceV1::IndexKey(
                                            u16::try_from(position).ok()?,
                                        )
                                    } else if let Some(position) = entity
                                        .primary_key()
                                        .iter()
                                        .position(|candidate| candidate == name)
                                    {
                                        CoveredResultSourceV1::EntityKey(
                                            u16::try_from(position).ok()?,
                                        )
                                    } else if index_symbol.cover_fields().contains(name) {
                                        CoveredResultSourceV1::Cover
                                    } else {
                                        return None;
                                    };
                                    CoveredResultFieldV1::checked(
                                        name.clone(),
                                        field.internal_id(),
                                        source,
                                    )
                                })
                                .collect::<Option<Vec<_>>>()
                                .and_then(|fields| {
                                    CoveredResultLayoutV1::checked(
                                        entity.name().to_owned(),
                                        index_symbol.name().to_owned(),
                                        fields,
                                        index_symbol
                                            .cover_fields()
                                            .iter()
                                            .map(|name| {
                                                entity.field(name).map(|field| field.internal_id())
                                            })
                                            .collect::<Option<Vec<_>>>()?,
                                    )
                                })
                        })
                        .flatten()
                    })
                }
                _ => None,
            };
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
                    QueryAccessKind::LongPatternCandidate { .. } => None,
                    QueryAccessKind::Nearest { .. } => None,
                    QueryAccessKind::CandidateRootHydration { .. } => None,
                },
                covered_result_layout,
            )
            .ok_or_else(internal)?;
            cost.add_step(entity, &step)?;

            let accumulator = auth
                .entry(entity.name().to_owned())
                .or_insert_with(|| AuthAccumulator::new(entity));
            let authorized_rows = match &access {
                QueryAccessKind::CandidateRootHydration {
                    maximum_candidates, ..
                } => u64::from(*maximum_candidates),
                _ => maximum_rows,
            };
            accumulator.maximum_rows = accumulator
                .maximum_rows
                .checked_add(authorized_rows)
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
        let name = self
            .document
            .name
            .as_ref()
            .map(|name| name.value.as_str().to_owned());
        let partition_parameter = partition_parameter.ok_or_else(internal)?;
        match projected_source {
            Some(source) => QueryAccessProgramV1::checked_projected(
                self.catalog.identity().clone(),
                surface,
                name,
                partition_parameter,
                steps,
                authorization,
                cost,
                source,
            ),
            None => QueryAccessProgramV1::checked(
                self.catalog.identity().clone(),
                surface,
                name,
                partition_parameter,
                steps,
                authorization,
                cost,
            ),
        }
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
                && path.0.len() == 1
                && let Some(candidate) = self.candidates.get(path.0[0].value.as_str())
            {
                if !comparison.operator.is_binary(BinaryOperator::In)
                    || entity.name() != candidate.root_entity()
                    || comparison.field != candidate.root_key()
                {
                    return Err(one(
                        PlannerDiagnosticCode::TypeMismatch,
                        path.0[0].span,
                        vec![candidate.name().to_owned()],
                        "candidate membership does not target its declared complete root key",
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

    fn type_check_candidate(
        &self,
        entity: &EntitySymbol,
        span: Span,
        comparisons: &[Comparison<'_>],
    ) -> Result<(), PlannerDiagnostics> {
        for comparison in comparisons {
            let field = entity.field(comparison.field).ok_or_else(internal)?;
            if !comparison.operator.is_binary(BinaryOperator::Equal) {
                return Err(one(
                    PlannerDiagnosticCode::TypeMismatch,
                    span,
                    vec![entity.name().to_owned(), comparison.field.to_owned()],
                    "ordinary candidate V1 sources require exact equality predicates",
                    None,
                ));
            }
            match comparison.value {
                Some(Expression::Parameter(parameter)) => {
                    let actual = self
                        .parameters
                        .get(parameter.value.as_str())
                        .and_then(|value| self.resolve_parameter_type(value));
                    if actual.as_ref() != Some(field.value_type()) {
                        return Err(one(
                            PlannerDiagnosticCode::TypeMismatch,
                            parameter.span,
                            vec![entity.name().to_owned(), comparison.field.to_owned()],
                            "candidate predicate parameter type does not match its field",
                            None,
                        ));
                    }
                }
                Some(Expression::Literal(literal))
                    if literal_compatible(field.value_type(), literal) => {}
                _ => {
                    return Err(one(
                        PlannerDiagnosticCode::TypeMismatch,
                        span,
                        vec![entity.name().to_owned(), comparison.field.to_owned()],
                        "candidate predicate must compare a field to a typed parameter or literal",
                        None,
                    ));
                }
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
                    Some(Expression::Path(path)) if path.0.len() == 1 => {
                        let name = path.0[0].value.as_str();
                        if self.candidates.contains_key(name) {
                            QueryPredicateValue::CandidateBinding {
                                name: name.to_owned(),
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
            TypeReference::BoundedString(maximum) => ValueType::string(*maximum as usize).ok(),
            TypeReference::Set(_)
            | TypeReference::Cursor
            | TypeReference::Limit
            | TypeReference::BoundedLimit(_) => None,
        }
    }
}

fn compile_projected_vector_source(
    document: &Document,
    catalog: &SymbolicCatalog,
) -> Result<Option<ProjectedVectorSourceV1>, PlannerDiagnostics> {
    let nearest = document
        .body
        .bindings
        .iter()
        .filter(|binding| binding.nearest.is_some())
        .collect::<Vec<_>>();
    let Some(source) = &document.projected_source else {
        if let Some(binding) = nearest.first() {
            return Err(one(
                PlannerDiagnosticCode::Unindexed,
                binding.nearest.as_ref().expect("filtered").span,
                vec![binding.entity.value.as_str().to_owned()],
                "production nearest requires an explicit projected source and freshness policy",
                None,
            ));
        }
        return Ok(None);
    };
    if nearest.len() != 1
        || document.body.bindings.len() != 1
        || !document.body.aggregates.is_empty()
    {
        return Err(one(
            PlannerDiagnosticCode::Cardinality,
            source.path.span,
            Vec::new(),
            "projected nearest requires exactly one nearest collection binding and no aggregates",
            None,
        ));
    }
    let binding = nearest[0];
    if binding.cardinality.value != Cardinality::Many {
        return Err(one(
            PlannerDiagnosticCode::Cardinality,
            binding.cardinality.span,
            vec![binding.name.value.as_str().to_owned()],
            "projected nearest requires one bounded many binding",
            None,
        ));
    }
    let [source_entity, source_field] = source.path.value.0.as_slice() else {
        return Err(one(
            PlannerDiagnosticCode::Unindexed,
            source.path.span,
            Vec::new(),
            "projected source must be the exact Entity.vector_field path",
            None,
        ));
    };
    let nearest_clause = binding.nearest.as_ref().expect("counted above");
    if comparisons(&binding.predicate.value).iter().any(|term| {
        !term.operator.is_binary(BinaryOperator::Equal)
            || !matches!(term.value, Some(Expression::Parameter(_)))
    }) {
        return Err(one(
            PlannerDiagnosticCode::Cardinality,
            binding.predicate.span,
            vec![binding.name.value.as_str().to_owned()],
            "projected nearest v1 accepts only compiler-typed equality parameters",
            None,
        ));
    }
    if source_entity.value != binding.entity.value
        || source_field.value != nearest_clause.field.value
    {
        return Err(one(
            PlannerDiagnosticCode::Unindexed,
            source.path.span,
            vec![
                source_entity.value.as_str().to_owned(),
                source_field.value.as_str().to_owned(),
            ],
            "projected source does not match the nearest entity and vector field",
            None,
        ));
    }
    let entity = catalog
        .entity(source_entity.value.as_str())
        .ok_or_else(internal)?;
    let field = entity.field(source_field.value.as_str()).ok_or_else(|| {
        one(
            PlannerDiagnosticCode::Unindexed,
            source_field.span,
            vec![
                source_entity.value.as_str().to_owned(),
                source_field.value.as_str().to_owned(),
            ],
            "projected source field is absent from the exact contract",
            None,
        )
    })?;
    if field.value_type().vector_dimension().is_none() || !field.is_production_vector() {
        return Err(one(
            PlannerDiagnosticCode::Unindexed,
            source_field.span,
            vec![
                source_entity.value.as_str().to_owned(),
                source_field.value.as_str().to_owned(),
            ],
            "projected source is not a production-capable vector field",
            None,
        ));
    }
    let freshness = match source.freshness.value {
        ProjectedFreshness::Available => ProjectedVectorFreshnessV1::Available,
        ProjectedFreshness::Causal {
            inherit_session_commit,
            max_wait_ms,
        } => ProjectedVectorFreshnessV1::Causal {
            inherit_session_commit,
            max_wait_ms,
        },
        ProjectedFreshness::Bounded { max_lag_ms } => {
            ProjectedVectorFreshnessV1::Bounded { max_lag_ms }
        }
    };
    ProjectedVectorSourceV1::checked(
        entity.name().to_owned(),
        field.name().to_owned(),
        entity.internal_id(),
        field.internal_id(),
        freshness,
    )
    .map(Some)
    .ok_or_else(internal)
}

fn compact_result_type_supported(value_type: &ValueType) -> bool {
    let value_type = value_type.optional_inner().unwrap_or(value_type);
    matches!(
        value_type.tag(),
        ValueTypeTag::Bool
            | ValueTypeTag::I64
            | ValueTypeTag::U64
            | ValueTypeTag::String
            | ValueTypeTag::Timestamp
            | ValueTypeTag::Date
            | ValueTypeTag::Uuid
            | ValueTypeTag::Enum
    )
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
            QueryAccessKind::LongPatternCandidate { pattern, .. } => {
                self.scanned_index_rows = checked_cost_add(
                    self.scanned_index_rows,
                    u64::from(pattern.bounds().rows()),
                    self.primary_span,
                )?;
                self.point_reads = checked_cost_add(self.point_reads, rows, self.primary_span)?;
            }
            QueryAccessKind::Nearest { .. } => {
                // Exact KNN examines every row in the org partition. Its
                // existing provider-specific 500-row ceiling remains
                // independent from ADR-0174's wider ordinary page ceiling.
                self.scanned_index_rows = checked_cost_add(
                    self.scanned_index_rows,
                    riffdb_types::MAX_EXACT_VECTOR_PARTITION_ROWS_V1,
                    self.primary_span,
                )?;
            }
            QueryAccessKind::CandidateRootHydration {
                maximum_candidates, ..
            } => {
                self.point_reads = checked_cost_add(
                    self.point_reads,
                    u64::from(*maximum_candidates),
                    self.primary_span,
                )?;
                self.dependent_keys = checked_cost_add(
                    self.dependent_keys,
                    u64::from(*maximum_candidates),
                    self.primary_span,
                )?;
                self.intermediate_rows = checked_cost_add(
                    self.intermediate_rows,
                    u64::from(*maximum_candidates),
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
                riffdb_query_ir::PageBound::BoundedParameter { maximum, .. } => *maximum,
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
                let value_bytes = match measure
                    .function()
                    .semantic_identity()
                    .descriptor()
                    .result_schema()
                {
                    AggregateResultSchemaV1::U64 => 10,
                    AggregateResultSchemaV1::ExactDecimalAtInputScale => 48,
                    AggregateResultSchemaV1::ExactMeanV1 => 80,
                    AggregateResultSchemaV1::Bool => 1,
                    AggregateResultSchemaV1::OptionalInputScalar => {
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
        let access_steps = u64::try_from(steps).map_err(|_| internal())?;
        // Report which ceiling was exceeded and by how much. Every cardinality
        // here is already bounded -- the author declared a bound and it is too
        // large -- so this is `CostCeilingExceeded` and the fix is to lower the
        // declared maximum, not to add a bound. Answering "which resource, what
        // amount, what ceiling" is the whole difference between a mechanical
        // correction and guessing at limit values.
        if access_steps > riffdb_types::MAX_APPLICATION_QUERY_STEPS {
            return Err(ceiling(
                self.primary_span,
                "query access steps exceed a closed planner ceiling",
                PlannerBoundObservation {
                    resource: PlannerBoundResource::AccessSteps,
                    actual: access_steps,
                    maximum: riffdb_types::MAX_APPLICATION_QUERY_STEPS,
                },
            ));
        }
        if self.encoded_result_bytes > riffdb_types::MAX_APPLICATION_QUERY_RESULT_BYTES {
            return Err(ceiling(
                self.primary_span,
                "query whole-request result bytes exceed a closed planner ceiling",
                PlannerBoundObservation {
                    resource: PlannerBoundResource::EncodedResultBytes,
                    actual: self.encoded_result_bytes,
                    maximum: riffdb_types::MAX_APPLICATION_QUERY_RESULT_BYTES,
                },
            ));
        }
        QueryCostVectorV1::new(
            access_steps,
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
        SourcePredicateOperator::Binary(BinaryOperator::NotIn) => return Err(internal()),
        SourcePredicateOperator::Binary(BinaryOperator::Prefix) => QueryPredicateOperator::Prefix,
        SourcePredicateOperator::Binary(
            BinaryOperator::StartsWith
            | BinaryOperator::EndsWith
            | BinaryOperator::Contains
            | BinaryOperator::Like
            | BinaryOperator::ILike
            | BinaryOperator::NotLike
            | BinaryOperator::NotILike,
        ) => return Err(internal()),
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
                    .or_else(|| {
                        self.entity
                            .long_pattern_index(name)
                            .map(riffdb_query_ir::LongPatternSymbol::id)
                    })
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

fn comparison_is_long_pattern_invocation(
    comparison: &Comparison<'_>,
    pattern: &riffdb_query_ir::LongPatternCandidateV1,
) -> bool {
    let operator = match comparison.operator {
        SourcePredicateOperator::Binary(BinaryOperator::Equal) => {
            Some(riffdb_types::LongPatternOperatorV1::Equals)
        }
        SourcePredicateOperator::Binary(BinaryOperator::StartsWith) => {
            Some(riffdb_types::LongPatternOperatorV1::StartsWith)
        }
        SourcePredicateOperator::Binary(BinaryOperator::EndsWith) => {
            Some(riffdb_types::LongPatternOperatorV1::EndsWith)
        }
        SourcePredicateOperator::Binary(BinaryOperator::Contains) => {
            Some(riffdb_types::LongPatternOperatorV1::Contains)
        }
        SourcePredicateOperator::Binary(BinaryOperator::Like) => {
            Some(riffdb_types::LongPatternOperatorV1::Like)
        }
        SourcePredicateOperator::Binary(BinaryOperator::ILike) => {
            Some(riffdb_types::LongPatternOperatorV1::ILike)
        }
        SourcePredicateOperator::Binary(BinaryOperator::NotLike) => {
            Some(riffdb_types::LongPatternOperatorV1::NotLike)
        }
        SourcePredicateOperator::Binary(BinaryOperator::NotILike) => {
            Some(riffdb_types::LongPatternOperatorV1::NotILike)
        }
        _ => None,
    };
    comparison.field == pattern.field()
        && operator == Some(pattern.operator())
        && matches!(
            comparison.value,
            Some(Expression::Parameter(parameter))
                if parameter.value.as_str() == pattern.pattern_parameter()
        )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OperationalComponentCapabilities {
    exact: bool,
    membership: bool,
    range_or_complement: bool,
    presence_state: bool,
    prefix: bool,
    order: bool,
}

const OPERATIONAL_COMPONENT_CAPABILITY_REGISTRY: [(
    IndexFieldEncodingV1,
    OperationalComponentCapabilities,
); 4] = [
    (
        IndexFieldEncodingV1::Canonical,
        OperationalComponentCapabilities {
            exact: true,
            membership: true,
            range_or_complement: true,
            presence_state: false,
            prefix: false,
            order: true,
        },
    ),
    (
        IndexFieldEncodingV1::Presence,
        OperationalComponentCapabilities {
            exact: false,
            membership: false,
            range_or_complement: false,
            presence_state: true,
            prefix: false,
            order: true,
        },
    ),
    (
        IndexFieldEncodingV1::TextKey(TextKeyProfileV1::BinaryUtf8),
        OperationalComponentCapabilities {
            exact: true,
            membership: true,
            range_or_complement: true,
            presence_state: false,
            prefix: true,
            order: true,
        },
    ),
    (
        // ADR-0172. Folded comparison is still bytewise, so cursor safety and
        // order stability carry over from the binary profile unchanged; the
        // capability set is therefore identical to it.
        IndexFieldEncodingV1::TextKey(TextKeyProfileV1::UnicodeFold),
        OperationalComponentCapabilities {
            exact: true,
            membership: true,
            range_or_complement: true,
            presence_state: false,
            prefix: true,
            order: true,
        },
    ),
];

fn operational_component_capabilities(
    encoding: IndexFieldEncodingV1,
) -> OperationalComponentCapabilities {
    OPERATIONAL_COMPONENT_CAPABILITY_REGISTRY
        .iter()
        .find_map(|(candidate, capabilities)| (*candidate == encoding).then_some(*capabilities))
        .expect("closed registry covers every operational component encoding")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OperationalAccessShape {
    order_start: usize,
}

fn comparisons(expression: &Expression) -> Vec<Comparison<'_>> {
    let mut output = Vec::new();
    collect_comparisons(expression, &mut output);
    output
}

fn candidate_ast_sources(
    expression: &riffdb_riffql_syntax::CandidateSetExpression,
) -> Vec<&riffdb_riffql_syntax::CandidateSource> {
    match expression {
        riffdb_riffql_syntax::CandidateSetExpression::Single(source) => vec![source],
        riffdb_riffql_syntax::CandidateSetExpression::Intersection(sources)
        | riffdb_riffql_syntax::CandidateSetExpression::Union(sources) => sources.iter().collect(),
        riffdb_riffql_syntax::CandidateSetExpression::Difference { positive, negative } => {
            let mut sources = Vec::with_capacity(negative.len() + 1);
            sources.push(positive);
            sources.extend(negative);
            sources
        }
    }
}

fn candidate_index_is_complete_prefix(
    index: &riffdb_query_ir::IndexSymbol,
    comparisons: &[Comparison<'_>],
) -> bool {
    if comparisons.is_empty() || comparisons.len() >= index.fields().len() {
        return false;
    }
    index.fields().iter().take(comparisons.len()).all(|field| {
        comparisons.iter().any(|comparison| {
            comparison.field == field && comparison.operator.is_binary(BinaryOperator::Equal)
        })
    }) && comparisons.iter().all(|comparison| {
        index
            .fields()
            .iter()
            .take(comparisons.len())
            .any(|field| field == comparison.field)
    })
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

struct AccessContext<'a, 'query> {
    binding_maximum_rows: &'a BTreeMap<&'query str, u64>,
    document: &'a Document,
    maximum_rows: u64,
    cover_required_fields: &'a BTreeSet<String>,
}

fn choose_access(
    entity: &EntitySymbol,
    binding: &riffdb_riffql_syntax::Binding,
    comparisons: &[Comparison<'_>],
    binding_cardinalities: &BTreeMap<&str, Cardinality>,
    context: AccessContext<'_, '_>,
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
        let source_maximum = context
            .binding_maximum_rows
            .get(source_binding.value.as_str())
            .copied()
            .ok_or_else(internal)?;
        let source = context
            .document
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
        // Report WHICH condition failed. This shape has eleven requirements and
        // reporting the shape as a whole tells an author nothing about the edit
        // to make -- one adapter read the combined message as proof that
        // composing two indexed predicates was unsupported and went looking for
        // a new engine capability, when the query was two edits from compiling.
        //
        // Ordered most-structural first, so the message names the earliest
        // thing that has to change rather than an incidental later one.
        let failure = if collection.field != source_field.value.as_str() {
            Some(
                "dependent key batch must join on the same field it selects from the source \
                 binding",
            )
        } else if binding.cardinality.value != Cardinality::Many {
            Some("dependent key batch must bind `many`, because a driver key may match no row")
        } else if !complete_key {
            Some(
                "dependent key batch must constrain every primary-key field, by equality or by \
                 the bounded collection",
            )
        } else if !key_only {
            Some(
                "dependent key batch may only compare primary-key fields; a predicate over a \
                 non-key field is a residual filter. Move the value into the key, or compare a \
                 fixed-size digest of it that is part of the key",
            )
        } else if binding.absence_outcome.is_none() {
            Some(
                "dependent key batch must declare an absence outcome with `else`; for a `many` \
                 binding it never fires, because an empty match is zero rows rather than an \
                 outcome",
            )
        } else if !binding
            .take
            .as_ref()
            .is_some_and(|take| take.after.is_none())
        {
            Some(
                "dependent key batch must declare `take` without an `after` cursor; the source \
                 binding carries the pagination",
            )
        } else if context.maximum_rows > source_maximum {
            Some(
                "dependent key batch `take` must be no larger than the source binding's, so the \
                 batch cannot exceed the keys driving it",
            )
        } else if source_order != [source_field.value.as_str()] || !source_order_is_forward {
            Some(
                "the source binding must order ascending by exactly the joining field, so its \
                 keys arrive in traversal order",
            )
        } else if target_order != [collection.field] || !forward_order {
            Some(
                "dependent key batch must order ascending by exactly the joining field, matching \
                 the source binding's order",
            )
        } else {
            None
        };
        if let Some(summary) = failure {
            return Err(one(
                PlannerDiagnosticCode::Cardinality,
                binding.predicate.span,
                vec![
                    binding.name.value.as_str().to_owned(),
                    source_binding.value.as_str().to_owned(),
                    source_field.value.as_str().to_owned(),
                ],
                summary,
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

    let mut first_compatible = None;
    for index in entity.indexes() {
        let Some(shape) = operational_access_shape(entity, index, comparisons, binding) else {
            continue;
        };
        if !index.fields()[..shape.order_start]
            .iter()
            .any(|field| field == entity.partition_field())
        {
            continue;
        }
        let expected = &index.fields()[shape.order_start..];
        if expected.len() != order_fields.len()
            || !expected
                .iter()
                .map(String::as_str)
                .eq(order_fields.iter().copied())
        {
            continue;
        }
        let candidate = (
            QueryAccessKind::Index {
                index: index.name().to_owned(),
                fields: index.fields().to_vec(),
                direction: match first_direction {
                    Direction::Ascending => AccessDirection::Forward,
                    Direction::Descending => AccessDirection::Reverse,
                },
            },
            Some(index.internal_id()),
        );
        if !index.cover_fields().is_empty()
            && index_covers_fields(entity, index, context.cover_required_fields)
        {
            return Ok(candidate);
        }
        if first_compatible.is_none() {
            first_compatible = Some(candidate);
        }
    }

    if let Some(candidate) = first_compatible {
        return Ok(candidate);
    }

    Err(one(
        PlannerDiagnosticCode::Unindexed,
        binding.predicate.span,
        vec![entity.name().to_owned()],
        "no declared index proves the requested bounded order",
        suggested_index(entity, comparisons, binding),
    ))
}

fn index_covers_fields(
    entity: &EntitySymbol,
    index: &riffdb_query_ir::IndexSymbol,
    required: &BTreeSet<String>,
) -> bool {
    required.iter().all(|field| {
        entity.primary_key().contains(field)
            || index.fields().contains(field)
            || index.cover_fields().contains(field)
    })
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
        let mut text_key_fields = BTreeSet::new();
        for comparison in comparisons {
            match comparison.operator {
                SourcePredicateOperator::Unary(_) => {
                    suggestion.push_str(&format!(" presence({})", comparison.field));
                }
                SourcePredicateOperator::Binary(BinaryOperator::Prefix) => {
                    text_key_fields.insert(comparison.field);
                }
                SourcePredicateOperator::Binary(
                    BinaryOperator::NotEqual
                    | BinaryOperator::Less
                    | BinaryOperator::LessEqual
                    | BinaryOperator::Greater
                    | BinaryOperator::GreaterEqual,
                ) if entity
                    .field(comparison.field)
                    .is_some_and(|field| field.value_type().tag() == ValueTypeTag::String) =>
                {
                    text_key_fields.insert(comparison.field);
                }
                _ => {}
            }
        }
        for field in text_key_fields {
            suggestion.push_str(&format!(" text_key({field}, binary_utf8_v1)"));
        }
        suggestion
    })
}

fn operational_access_shape(
    entity: &EntitySymbol,
    index: &riffdb_query_ir::IndexSymbol,
    comparisons: &[Comparison<'_>],
    binding: &riffdb_riffql_syntax::Binding,
) -> Option<OperationalAccessShape> {
    let mut consumed = vec![false; comparisons.len()];
    let mut order_start = 0;

    while order_start < index.fields().len() {
        let field = index.fields()[order_start].as_str();
        let matching = comparisons
            .iter()
            .enumerate()
            .filter(|(_, comparison)| comparison.field == field)
            .collect::<Vec<_>>();
        if matching.is_empty() {
            break;
        }
        let capabilities =
            operational_component_capabilities(index.internal_encodings()[order_start]);
        if matching.iter().all(|(_, comparison)| {
            matches!(
                comparison.operator,
                SourcePredicateOperator::Binary(
                    BinaryOperator::NotEqual
                        | BinaryOperator::Less
                        | BinaryOperator::LessEqual
                        | BinaryOperator::Greater
                        | BinaryOperator::GreaterEqual
                )
            )
        }) {
            let has_complement = matching
                .iter()
                .any(|(_, comparison)| comparison.operator.is_binary(BinaryOperator::NotEqual));
            let lower_count = matching
                .iter()
                .filter(|(_, comparison)| {
                    matches!(
                        comparison.operator,
                        SourcePredicateOperator::Binary(
                            BinaryOperator::Greater | BinaryOperator::GreaterEqual
                        )
                    )
                })
                .count();
            let upper_count = matching
                .iter()
                .filter(|(_, comparison)| {
                    matches!(
                        comparison.operator,
                        SourcePredicateOperator::Binary(
                            BinaryOperator::Less | BinaryOperator::LessEqual
                        )
                    )
                })
                .count();
            let logical_order_is_physical = entity.field(field).is_some_and(|field| {
                matches!(
                    (
                        index.internal_encodings()[order_start],
                        field.value_type().tag()
                    ),
                    (
                        IndexFieldEncodingV1::Canonical,
                        ValueTypeTag::I64
                            | ValueTypeTag::U64
                            | ValueTypeTag::Timestamp
                            | ValueTypeTag::Date
                            | ValueTypeTag::Uuid
                            | ValueTypeTag::Enum
                    ) | (
                        IndexFieldEncodingV1::TextKey(TextKeyProfileV1::BinaryUtf8),
                        ValueTypeTag::String
                    )
                )
            });
            let comparison_shape_is_finite = if has_complement {
                matching.len() == 1
            } else {
                matching.len() <= 2 && lower_count <= 1 && upper_count <= 1
            };
            if !capabilities.range_or_complement
                || !logical_order_is_physical
                || !comparison_shape_is_finite
            {
                return None;
            }
            for (comparison_index, _) in matching {
                consumed[comparison_index] = true;
            }
            break;
        }
        let [(comparison_index, comparison)] = matching.as_slice() else {
            return None;
        };
        match comparison.operator {
            SourcePredicateOperator::Binary(BinaryOperator::Equal) if capabilities.exact => {
                consumed[*comparison_index] = true;
                order_start += 1;
            }
            SourcePredicateOperator::Unary(UnaryOperator::IsNull)
                if capabilities.presence_state =>
            {
                consumed[*comparison_index] = true;
                order_start += 1;
            }
            SourcePredicateOperator::Binary(BinaryOperator::In) if capabilities.membership => {
                consumed[*comparison_index] = true;
                break;
            }
            SourcePredicateOperator::Binary(BinaryOperator::Prefix) if capabilities.prefix => {
                consumed[*comparison_index] = true;
                break;
            }
            SourcePredicateOperator::Unary(UnaryOperator::IsNotNull | UnaryOperator::Exists)
                if capabilities.presence_state =>
            {
                consumed[*comparison_index] = true;
                break;
            }
            _ => return None,
        }
    }

    if consumed.iter().any(|consumed| !consumed) {
        return None;
    }

    let order_fields = binding
        .order
        .iter()
        .filter_map(|term| path_field(&term.path.value))
        .collect::<Vec<_>>();
    let remaining_fields = &index.fields()[order_start..];
    if remaining_fields.len() != order_fields.len()
        || !remaining_fields
            .iter()
            .map(String::as_str)
            .eq(order_fields.iter().copied())
    {
        return None;
    }
    for (position, term) in (order_start..index.fields().len()).zip(&binding.order) {
        let encoding = index.internal_encodings()[position];
        let capabilities = operational_component_capabilities(encoding);
        let present_only = comparisons.iter().any(|comparison| {
            comparison.field == index.fields()[position]
                && comparison.operator == SourcePredicateOperator::Unary(UnaryOperator::IsNotNull)
        });
        if !capabilities.order
            || (encoding == IndexFieldEncodingV1::Presence
                && !present_only
                && term.null_placement.is_none())
        {
            return None;
        }
    }

    Some(OperationalAccessShape { order_start })
}

fn maximum_rows(
    binding: &riffdb_riffql_syntax::Binding,
    document: &Document,
) -> Result<u64, PlannerDiagnostics> {
    if binding.cardinality.value != Cardinality::Many {
        return Ok(1);
    }
    // A nearest binding declares K instead of `take` (the parser rejects
    // combining them). A literal K is its exact maximum; a typed Limit
    // parameter is bounded by the shared page-take ceiling. The durable
    // Nearest.k field retains that compiler-proven maximum while row_limit
    // retains the request-specific source.
    if let (None, Some(nearest)) = (&binding.take, &binding.nearest) {
        return match &nearest.k.value {
            Expression::Literal(Literal::Unsigned(value)) => value.parse::<u64>().ok(),
            Expression::Parameter(parameter) => {
                document_limit_maximum(document, parameter.value.as_str())
            }
            _ => None,
        }
        .filter(|value| (1..=riffdb_query_ir::max_query_page_take()).contains(value))
        .ok_or_else(|| {
            one(
                PlannerDiagnosticCode::Unbounded,
                nearest.k.span,
                vec![binding.name.value.as_str().to_owned()],
                "nearest k must be a positive literal within the 65534 page bound",
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
        Expression::Parameter(parameter) => {
            document_limit_maximum(document, parameter.value.as_str())
        }
        _ => None,
    }
    .filter(|value| (1..=riffdb_query_ir::max_query_page_take()).contains(value))
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

fn lowered_row_limit(
    expression: &Expression,
    document: &Document,
) -> Result<QueryRowLimit, PlannerDiagnostics> {
    match expression {
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
            match declared.ty.value {
                TypeReference::Limit => Ok(QueryRowLimit::Parameter {
                    name: parameter.value.as_str().to_owned(),
                    default,
                }),
                TypeReference::BoundedLimit(maximum) => Ok(QueryRowLimit::BoundedParameter {
                    name: parameter.value.as_str().to_owned(),
                    maximum,
                    default,
                }),
                _ => Err(internal()),
            }
        }
        _ => Err(internal()),
    }
}

fn document_limit_maximum(document: &Document, parameter: &str) -> Option<u64> {
    document
        .parameters
        .iter()
        .find(|candidate| candidate.name.value.as_str() == parameter)
        .and_then(|candidate| match candidate.ty.value {
            TypeReference::Limit => Some(riffdb_query_ir::max_query_page_take()),
            TypeReference::BoundedLimit(maximum) => Some(maximum),
            _ => None,
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
        return lowered_row_limit(&nearest.k.value, document);
    }
    let take = binding.take.as_ref().ok_or_else(internal)?;
    lowered_row_limit(&take.limit.value, document)
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
        bound: None,
    }])
}

fn ceiling(
    primary: Span,
    summary: &'static str,
    bound: PlannerBoundObservation,
) -> PlannerDiagnostics {
    PlannerDiagnostics(vec![PlannerDiagnostic {
        code: PlannerDiagnosticCode::CostCeilingExceeded,
        primary,
        symbol_path: Vec::new(),
        summary,
        suggested_index: None,
        bound: Some(bound),
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

#[cfg(test)]
mod operational_component_registry_tests {
    use super::{
        IndexFieldEncodingV1, OPERATIONAL_COMPONENT_CAPABILITY_REGISTRY, TextKeyProfileV1,
        operational_component_capabilities,
    };

    #[test]
    fn closed_registry_freezes_every_executable_component_role() {
        assert_eq!(OPERATIONAL_COMPONENT_CAPABILITY_REGISTRY.len(), 4);
        assert_eq!(
            operational_component_capabilities(IndexFieldEncodingV1::Canonical),
            super::OperationalComponentCapabilities {
                exact: true,
                membership: true,
                range_or_complement: true,
                presence_state: false,
                prefix: false,
                order: true,
            }
        );
        assert_eq!(
            operational_component_capabilities(IndexFieldEncodingV1::Presence),
            super::OperationalComponentCapabilities {
                exact: false,
                membership: false,
                range_or_complement: false,
                presence_state: true,
                prefix: false,
                order: true,
            }
        );
        assert_eq!(
            operational_component_capabilities(IndexFieldEncodingV1::TextKey(
                TextKeyProfileV1::BinaryUtf8,
            )),
            super::OperationalComponentCapabilities {
                exact: true,
                membership: true,
                range_or_complement: true,
                presence_state: false,
                prefix: true,
                order: true,
            }
        );
        assert_eq!(
            operational_component_capabilities(IndexFieldEncodingV1::TextKey(
                TextKeyProfileV1::UnicodeFold,
            )),
            super::OperationalComponentCapabilities {
                exact: true,
                membership: true,
                range_or_complement: true,
                presence_state: false,
                prefix: true,
                order: true,
            }
        );
    }
}
