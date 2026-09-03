//! Checked event-derived projection lowering from compiler-private typed HIR.

use riffdb_contract_ir::{
    FieldSchema, ProjectionFrontierPolicy, ProjectionGroupComponentSchema, ProjectionGroupSchema,
    ProjectionMeasurePlan, ProjectionPlan, RecordSchema, RecordTypeRef, SchemaIr, ValueType,
    ValueTypeTag,
};
use riffdb_types::EnumVariantId;

use crate::diagnostic::{CompilerDiagnostic, CompilerDiagnosticCode, CompilerDiagnostics};
use crate::hir::{HirMeasureKind, TypedContractHir};

/// Lowers every bounded HIR projection into a hashed, checked incremental plan.
pub(crate) fn lower_projections(
    hir: &TypedContractHir,
    schema: &SchemaIr,
) -> Result<Vec<ProjectionPlan>, CompilerDiagnostics> {
    let mut diagnostics = Vec::new();
    let mut plans = Vec::new();
    for projection in &hir.projections {
        let mut invalid = false;
        let mut group_components = Vec::new();
        for expression in &projection.key {
            let variants = expression
                .value_type
                .enum_type_id()
                .map(|enum_id| enum_variants(enum_id, hir))
                .unwrap_or_default();
            match ProjectionGroupComponentSchema::new(expression.value_type.clone(), variants) {
                Ok(component) => group_components.push(component),
                Err(_) => {
                    diagnostics.push(CompilerDiagnostic::new(
                        CompilerDiagnosticCode::InvalidProjection,
                        expression.span,
                    ));
                    invalid = true;
                }
            }
        }
        let mut measures_by_id = Vec::new();
        for measure in &projection.measures {
            let (value_type, expression) = match &measure.kind {
                HirMeasureKind::Count => (ValueType::u64(), None),
                HirMeasureKind::Sum(expression)
                    if matches!(
                        expression.value_type.tag(),
                        ValueTypeTag::I64
                            | ValueTypeTag::U64
                            | ValueTypeTag::Decimal
                            | ValueTypeTag::Money
                    ) =>
                {
                    (expression.value_type.clone(), Some(expression.id))
                }
                HirMeasureKind::Sum(expression) => {
                    diagnostics.push(CompilerDiagnostic::new(
                        CompilerDiagnosticCode::InvalidProjection,
                        expression.span,
                    ));
                    invalid = true;
                    continue;
                }
            };
            let field = match FieldSchema::new(measure.id, measure.name.clone(), value_type) {
                Ok(field) => field,
                Err(_) => {
                    diagnostics.push(CompilerDiagnostic::new(
                        CompilerDiagnosticCode::InvalidIr,
                        measure.name_span,
                    ));
                    invalid = true;
                    continue;
                }
            };
            let plan = match expression {
                Some(expression) => ProjectionMeasurePlan::sum(field.clone(), expression),
                None => ProjectionMeasurePlan::count(field.clone()),
            };
            match plan {
                Ok(plan) => measures_by_id.push((measure.id, field, plan)),
                Err(_) => {
                    diagnostics.push(CompilerDiagnostic::new(
                        CompilerDiagnosticCode::InvalidProjection,
                        measure.name_span,
                    ));
                    invalid = true;
                }
            }
        }
        if invalid {
            continue;
        }
        measures_by_id.sort_by_key(|entry| entry.0);
        let measure_fields = measures_by_id
            .iter()
            .map(|(_, field, _)| field.clone())
            .collect();
        let measure_plans = measures_by_id
            .into_iter()
            .map(|(_, _, plan)| plan)
            .collect();
        let measures = match RecordSchema::new(
            RecordTypeRef::ProjectionResult(projection.id),
            measure_fields,
        ) {
            Ok(measures) => measures,
            Err(_) => {
                diagnostics.push(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::InvalidIr,
                    projection.span,
                ));
                continue;
            }
        };
        let group_schema =
            match ProjectionGroupSchema::new(projection.id, group_components, measures) {
                Ok(group_schema) => group_schema,
                Err(error) => {
                    diagnostics.push(CompilerDiagnostic::from_ir_error(error, projection.span));
                    continue;
                }
            };
        let expressions = match projection.expressions.to_ir(projection.span) {
            Ok(expressions) => expressions,
            Err(diagnostic) => {
                diagnostics.push(diagnostic);
                continue;
            }
        };
        let frontier = match projection.frontier {
            crate::hir::HirFrontier::TransactionallyOrdered => {
                ProjectionFrontierPolicy::TransactionallyOrdered
            }
        };
        match ProjectionPlan::new(
            projection.id,
            projection.name.clone(),
            projection.source_event,
            expressions,
            projection.filter.as_ref().map(|filter| filter.id),
            projection.key.iter().map(|root| root.id).collect(),
            measure_plans,
            frontier,
            group_schema,
            schema,
        ) {
            Ok(plan) => plans.push(plan),
            Err(_) => diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::InvalidProjection,
                projection.span,
            )),
        }
    }
    if diagnostics.is_empty() {
        plans.sort_by_key(ProjectionPlan::projection_id);
        Ok(plans)
    } else {
        Err(CompilerDiagnostics::new(diagnostics).expect("nonempty diagnostics"))
    }
}

fn enum_variants(enum_id: riffdb_types::EnumTypeId, hir: &TypedContractHir) -> Vec<EnumVariantId> {
    hir.enums
        .iter()
        .find(|enumeration| enumeration.id == enum_id)
        .map(|enumeration| {
            enumeration
                .variants
                .iter()
                .map(|variant| variant.id)
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use riffdb_contract_syntax::{Span, SyntaxDiagnosticCode, ast::Declaration, parse_contract};

    use super::*;
    use crate::hir::lower_contract_hir;
    use crate::schema_lowering::lower_schema;
    use crate::symbols::allocate_genesis_symbols;
    use crate::typecheck::resolve_declared_types;

    fn compile_hir(source: &str) -> TypedContractHir {
        let document = parse_contract(source).expect("syntax");
        let symbols = allocate_genesis_symbols(&document).expect("symbols");
        let types = resolve_declared_types(&document, &symbols).expect("types");
        lower_contract_hir(&document, &symbols, &types).expect("HIR")
    }

    fn boundary_source(value_type: &str, component_count: usize) -> String {
        let components = vec!["group"; component_count].join(", ");
        format!(
            "contract ProjectionBoundary version 1 {{\n  event Source {{ group: {value_type} }}\n  projection Totals {{\n    source event Source\n    key ({components})\n    measure total = count()\n    frontier transactionally_ordered\n  }}\n}}\n"
        )
    }

    fn projection_spans(source: &str) -> (Span, Span) {
        let document = parse_contract(source).expect("syntax");
        let projection = document
            .contract
            .value
            .declarations
            .iter()
            .find_map(|declaration| match &declaration.value {
                Declaration::Projection(projection) => Some(projection),
                _ => None,
            })
            .expect("projection");
        (projection.name.span, projection.key[0].span)
    }

    fn assert_source_diagnostic(source: &str, code: CompilerDiagnosticCode, expected_span: Span) {
        let error = crate::validate_contract_source(source).expect_err("source must reject");
        let diagnostics = error.semantic().expect("semantic diagnostics");
        let matching = diagnostics
            .as_slice()
            .iter()
            .filter(|diagnostic| diagnostic.code() == code)
            .collect::<Vec<_>>();
        assert_eq!(matching.len(), 1, "{diagnostics:?}");
        assert_eq!(matching[0].primary_span(), expected_span);
    }

    #[test]
    fn canonical_budget_projection_has_decimal_measure_and_checked_bounds() {
        let hir = compile_hir(include_str!("../../../contracts/examples/budget.riff"));
        let schema = lower_schema(&hir).expect("schema");
        let projections = lower_projections(&hir, &schema).expect("projection");
        assert_eq!(projections.len(), 1);
        let plan = &projections[0];
        assert_eq!(plan.key_expressions().len(), 3);
        assert_eq!(plan.measures().len(), 1);
        assert!(plan.group_schema().maximum_complete_key_bytes() <= 4_096);
    }

    #[test]
    fn ordering_filter_rejects() {
        let source = r#"
contract Invalid version 1 {
  event E { value: i64 }
  projection P {
    source event E
    where value > 0
    key (value)
    measure total = count()
    frontier transactionally_ordered
  }
}
"#;
        let hir = compile_hir(source);
        let schema = lower_schema(&hir).expect("schema");
        let diagnostics = lower_projections(&hir, &schema).expect_err("rejects");
        assert!(
            diagnostics
                .as_slice()
                .iter()
                .any(|diagnostic| diagnostic.code() == CompilerDiagnosticCode::InvalidProjection)
        );
    }

    #[test]
    fn source_filters_accept_field_equality_and_event_date_equality() {
        let source = r#"
contract FilterAtoms version 1 {
  event E { first: i64 second: i64 day: date }
  projection FieldEquality {
    source event E
    where first == second
    key (first)
    measure total = count()
    frontier transactionally_ordered
  }
  projection DateEquality {
    source event E
    where day == tx.date
    key (day)
    measure total = count()
    frontier transactionally_ordered
  }
}
"#;
        let hir = compile_hir(source);
        let schema = lower_schema(&hir).expect("schema");
        let projections = lower_projections(&hir, &schema).expect("atomic equality filters");
        assert_eq!(projections.len(), 2);
        assert!(
            projections
                .iter()
                .all(|projection| projection.filter().is_some())
        );
    }

    #[test]
    fn source_filter_rejects_arithmetic_nested_under_equality_at_projection_name() {
        let source = r#"
contract FilterArithmetic version 1 {
  event E { first: i64 second: i64 third: i64 }
  projection ArithmeticEquality {
    source event E
    where first + second == third
    key (first)
    measure total = count()
    frontier transactionally_ordered
  }
}
"#;
        let start = source.find("ArithmeticEquality").expect("projection name");
        let span = Span::new(start, start + "ArithmeticEquality".len()).expect("span");
        assert_source_diagnostic(source, CompilerDiagnosticCode::InvalidProjection, span);
    }

    #[test]
    fn source_group_types_reject_optional_and_collection_at_the_key_expression() {
        for value_type in ["optional<i64>", "list<i64,4>"] {
            let source = boundary_source(value_type, 1);
            let (_, key_span) = projection_spans(&source);
            assert_source_diagnostic(&source, CompilerDiagnosticCode::InvalidProjection, key_span);
        }
    }

    #[test]
    fn source_group_count_and_complete_key_boundaries_have_exact_spans() {
        let at_component_limit = boundary_source("bool", 1_024);
        let (projection_span, _) = projection_spans(&at_component_limit);
        assert_source_diagnostic(
            &at_component_limit,
            CompilerDiagnosticCode::BoundExceeded,
            projection_span,
        );

        let above_component_limit = boundary_source("bool", 1_025);
        let start = above_component_limit
            .rfind("group")
            .expect("last component");
        let expected_span = Span::new(start, start + "group".len()).expect("span");
        let error = crate::validate_contract_source(&above_component_limit)
            .expect_err("1,025 components must reject during bounded parsing");
        let diagnostics = error.syntax().expect("syntax diagnostics");
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(
            diagnostics.as_slice()[0].code(),
            SyntaxDiagnosticCode::CollectionLimit
        );
        assert_eq!(diagnostics.as_slice()[0].span(), expected_span);

        let exact = boundary_source("string<3780>", 1);
        let bundle = crate::compile_contract_source(&exact).expect("4,096-byte maximum compiles");
        assert_eq!(
            bundle.projections()[0]
                .group_schema()
                .maximum_complete_key_bytes(),
            4_096
        );

        let above = boundary_source("string<3781>", 1);
        let (projection_span, _) = projection_spans(&above);
        assert_source_diagnostic(
            &above,
            CompilerDiagnosticCode::BoundExceeded,
            projection_span,
        );
    }
}
