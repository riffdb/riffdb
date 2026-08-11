//! Source-spanned lowering for closed principal-aware row policies.

use std::collections::{BTreeMap, BTreeSet};

use riffdb_contract_ir::{
    PrincipalFactSchemaV1, RowPolicyCatalogV1, RowPolicyExpressionNodeV1, RowPolicyOperandV1,
    RowPolicyOperationV1, RowPolicyPlanV1, RowPolicyRuleV1, RowPolicyValueSourceV1, SchemaIr,
    ValueType,
};
use riffdb_contract_syntax::ast::{
    BinaryOperator, Declaration, Path, RowPolicyExpression, RowPolicyOperation,
};
use riffdb_contract_syntax::{ContractDocument, Span, Spanned};
use riffdb_types::CanonicalValue;

use crate::diagnostic::{CompilerDiagnostic, CompilerDiagnosticCode, CompilerDiagnostics};
use crate::expression_lowering::lower_literal;
use crate::hir::{HirEntity, TypedContractHir};
use crate::locality::LocalityAnalysis;
use crate::symbols::GenesisSymbols;
use crate::typecheck::ResolvedTypes;

pub(crate) fn lower_row_policy_catalog(
    document: &ContractDocument,
    symbols: &GenesisSymbols,
    types: &ResolvedTypes,
    hir: &TypedContractHir,
    locality: &LocalityAnalysis,
    schema: &SchemaIr,
) -> Result<RowPolicyCatalogV1, CompilerDiagnostics> {
    let mut diagnostics = Vec::new();
    let facts = document
        .contract
        .value
        .declarations
        .iter()
        .filter_map(|declaration| {
            let Declaration::PrincipalFact(fact) = &declaration.value else {
                return None;
            };
            let value_type = types.principal_facts.get(&fact.name.value).cloned()?;
            match PrincipalFactSchemaV1::new(fact.name.value.clone(), value_type) {
                Ok(fact) => Some(fact),
                Err(_) => {
                    diagnostics.push(CompilerDiagnostic::new(
                        CompilerDiagnosticCode::InvalidPrincipalFact,
                        declaration.span,
                    ));
                    None
                }
            }
        })
        .collect::<Vec<_>>();
    let fact_types = facts
        .iter()
        .map(|fact| (fact.name().to_owned(), fact.value_type().clone()))
        .collect::<BTreeMap<_, _>>();

    let mut policies = Vec::new();
    for declaration in &document.contract.value.declarations {
        let Declaration::RowPolicy(policy) = &declaration.value else {
            continue;
        };
        let Some(entity_id) = symbols.entities.get(&policy.entity.value).copied() else {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::UnknownName,
                policy.entity.span,
            ));
            continue;
        };
        let Some(entity) = hir.entity(entity_id) else {
            diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::UnknownName,
                policy.entity.span,
            ));
            continue;
        };
        let mut operations = BTreeSet::new();
        let mut rules = Vec::new();
        for rule in &policy.rules {
            let operation = operation(rule.value.operation.value);
            if !operations.insert(operation) {
                diagnostics.push(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::InvalidRowPolicy,
                    rule.value.operation.span,
                ));
                continue;
            }
            let mut lowerer = PolicyExpressionLowerer {
                symbols,
                types,
                hir,
                locality,
                schema,
                entity,
                nodes: Vec::new(),
                relationship_probes: 0,
            };
            match lowerer.lower(&rule.value.expression, Some(&ValueType::bool())) {
                Ok((root, value_type)) if value_type == ValueType::bool() => {
                    match RowPolicyRuleV1::new(
                        operation,
                        lowerer.nodes,
                        root,
                        entity_id,
                        schema,
                        &fact_types,
                    ) {
                        Ok(rule) => rules.push(rule),
                        Err(error) => diagnostics.push(CompilerDiagnostic::new(
                            ir_policy_code(&error),
                            rule.value.expression.span,
                        )),
                    }
                }
                Ok(_) => diagnostics.push(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::InvalidRowPolicy,
                    rule.value.expression.span,
                )),
                Err(diagnostic) => diagnostics.push(diagnostic),
            }
        }
        match RowPolicyPlanV1::new(policy.name.value.clone(), entity_id, rules, schema) {
            Ok(policy) => policies.push(policy),
            Err(_) => diagnostics.push(CompilerDiagnostic::new(
                CompilerDiagnosticCode::InvalidRowPolicy,
                declaration.span,
            )),
        }
    }

    if !diagnostics.is_empty() {
        return Err(CompilerDiagnostics::new(diagnostics).expect("nonempty diagnostics"));
    }
    RowPolicyCatalogV1::new(facts, policies, schema).map_err(|error| {
        CompilerDiagnostics::single(CompilerDiagnostic::new(
            ir_policy_code(&error),
            document.contract.span,
        ))
    })
}

fn operation(value: RowPolicyOperation) -> RowPolicyOperationV1 {
    match value {
        RowPolicyOperation::Read => RowPolicyOperationV1::Read,
        RowPolicyOperation::Create => RowPolicyOperationV1::Create,
        RowPolicyOperation::Update => RowPolicyOperationV1::Update,
        RowPolicyOperation::Delete => RowPolicyOperationV1::Delete,
    }
}

fn ir_policy_code(error: &riffdb_contract_ir::IrValidationError) -> CompilerDiagnosticCode {
    match error {
        riffdb_contract_ir::IrValidationError::LimitExceeded { .. }
        | riffdb_contract_ir::IrValidationError::SizeOverflow { .. } => {
            CompilerDiagnosticCode::UnboundedRowPolicy
        }
        riffdb_contract_ir::IrValidationError::InvalidReference { kind }
            if kind.contains("index") || kind.contains("target") =>
        {
            CompilerDiagnosticCode::InvalidRowPolicyRelationship
        }
        _ => CompilerDiagnosticCode::InvalidRowPolicy,
    }
}

struct PolicyExpressionLowerer<'a> {
    symbols: &'a GenesisSymbols,
    types: &'a ResolvedTypes,
    hir: &'a TypedContractHir,
    locality: &'a LocalityAnalysis,
    schema: &'a SchemaIr,
    entity: &'a HirEntity,
    nodes: Vec<RowPolicyExpressionNodeV1>,
    relationship_probes: usize,
}

impl PolicyExpressionLowerer<'_> {
    fn lower(
        &mut self,
        expression: &Spanned<RowPolicyExpression>,
        expected: Option<&ValueType>,
    ) -> Result<(u16, ValueType), CompilerDiagnostic> {
        match &expression.value {
            RowPolicyExpression::Literal(literal) => {
                let (value, value_type) = lower_literal(&literal.value, expected)
                    .ok_or_else(|| invalid(expression.span))?;
                self.push_operand(
                    RowPolicyOperandV1::new(RowPolicyValueSourceV1::Constant(value), value_type),
                    expression.span,
                )
            }
            RowPolicyExpression::Path(path) => {
                let operand = self.resolve_path(path, expected)?;
                self.push_operand(operand, expression.span)
            }
            RowPolicyExpression::Parenthesized(inner) => self.lower(inner, expected),
            RowPolicyExpression::Not(inner) => {
                let (value, value_type) = self.lower(inner, Some(&ValueType::bool()))?;
                if value_type != ValueType::bool() {
                    return Err(invalid(expression.span));
                }
                self.push(
                    RowPolicyExpressionNodeV1::Not { value },
                    ValueType::bool(),
                    expression.span,
                )
            }
            RowPolicyExpression::Binary {
                left,
                operator,
                right,
            } => match operator.value {
                BinaryOperator::And | BinaryOperator::Or => {
                    let (left, left_type) = self.lower(left, Some(&ValueType::bool()))?;
                    let (right, right_type) = self.lower(right, Some(&ValueType::bool()))?;
                    if left_type != ValueType::bool() || right_type != ValueType::bool() {
                        return Err(invalid(expression.span));
                    }
                    let node = if operator.value == BinaryOperator::And {
                        RowPolicyExpressionNodeV1::And { left, right }
                    } else {
                        RowPolicyExpressionNodeV1::Or { left, right }
                    };
                    self.push(node, ValueType::bool(), expression.span)
                }
                BinaryOperator::Equal | BinaryOperator::NotEqual => {
                    let (left, left_type) = self.lower(left, None)?;
                    let (right, right_type) = self.lower(right, Some(&left_type))?;
                    if !left_type.accepts_contextual(&right_type)
                        && !right_type.accepts_contextual(&left_type)
                    {
                        return Err(type_mismatch(expression.span));
                    }
                    let node = if operator.value == BinaryOperator::Equal {
                        RowPolicyExpressionNodeV1::Equal { left, right }
                    } else {
                        RowPolicyExpressionNodeV1::NotEqual { left, right }
                    };
                    self.push(node, ValueType::bool(), expression.span)
                }
                _ => Err(invalid(operator.span)),
            },
            RowPolicyExpression::In { needle, haystack } => {
                let (haystack, haystack_type) = self.lower(haystack, None)?;
                let Some((element, _)) = haystack_type.list_parts() else {
                    return Err(type_mismatch(haystack_type_span(
                        haystack,
                        &self.nodes,
                        expression.span,
                    )));
                };
                let element = element.clone();
                let (needle, needle_type) = self.lower(needle, Some(&element))?;
                let needle_matches = element.accepts_contextual(&needle_type)
                    || needle_type
                        .optional_inner()
                        .is_some_and(|inner| inner == &element);
                if !needle_matches {
                    return Err(type_mismatch(expression.span));
                }
                self.push(
                    RowPolicyExpressionNodeV1::In { needle, haystack },
                    ValueType::bool(),
                    expression.span,
                )
            }
            RowPolicyExpression::IsNull { value, negated } => {
                let (value, value_type) = self.lower(value, None)?;
                if !value_type.is_optional() {
                    return Err(type_mismatch(expression.span));
                }
                self.push(
                    RowPolicyExpressionNodeV1::IsNull {
                        value,
                        negated: *negated,
                    },
                    ValueType::bool(),
                    expression.span,
                )
            }
            RowPolicyExpression::Exists {
                entity,
                index,
                arguments,
            } => self.lower_exists(entity, index, arguments, expression.span),
        }
    }

    fn lower_exists(
        &mut self,
        target_name: &Spanned<String>,
        index_name: &Spanned<String>,
        arguments: &[Spanned<RowPolicyExpression>],
        span: Span,
    ) -> Result<(u16, ValueType), CompilerDiagnostic> {
        self.relationship_probes += 1;
        if self.relationship_probes > 1 {
            return Err(CompilerDiagnostic::new(
                CompilerDiagnosticCode::UnboundedRowPolicy,
                span,
            ));
        }
        let target_id = self
            .symbols
            .entities
            .get(&target_name.value)
            .copied()
            .ok_or_else(|| unknown(target_name.span))?;
        if self.locality.entity_owner.get(&self.entity.id)
            != self.locality.entity_owner.get(&target_id)
        {
            return Err(CompilerDiagnostic::new(
                CompilerDiagnosticCode::CrossPartitionRowPolicy,
                target_name.span,
            ));
        }
        let target = self
            .hir
            .entity(target_id)
            .ok_or_else(|| unknown(target_name.span))?;
        let target_schema = self
            .schema
            .entity(target_id)
            .ok_or_else(|| unknown(target_name.span))?;
        let target_index = target
            .indexes
            .iter()
            .find(|candidate| candidate.name == index_name.value)
            .ok_or_else(|| {
                CompilerDiagnostic::new(
                    CompilerDiagnosticCode::InvalidRowPolicyRelationship,
                    index_name.span,
                )
            })?;
        let index_schema = target_schema
            .indexes()
            .iter()
            .find(|candidate| candidate.id() == target_index.id)
            .ok_or_else(|| invalid(index_name.span))?;
        if index_schema.fields().len() != arguments.len() {
            return Err(type_mismatch(span));
        }
        let aggregate = self
            .hir
            .aggregate_for_entity(target_id)
            .ok_or_else(|| invalid(target_name.span))?;
        let root = self
            .hir
            .entity(aggregate.root)
            .ok_or_else(|| invalid(target_name.span))?;
        let route_width = root.key_fields.len();
        if index_schema.fields().len() < route_width
            || target.key_fields.len() < route_width
            || index_schema.fields()[..route_width] != target.key_fields[..route_width]
        {
            return Err(CompilerDiagnostic::new(
                CompilerDiagnosticCode::CrossPartitionRowPolicy,
                index_name.span,
            ));
        }
        let mut lowered = Vec::with_capacity(arguments.len());
        for (position, (argument, field_id)) in
            arguments.iter().zip(index_schema.fields()).enumerate()
        {
            let expected = target_schema
                .record()
                .field(*field_id)
                .ok_or_else(|| invalid(index_name.span))?
                .value_type();
            let operand = self.lower_operand(argument, Some(expected))?;
            if position < route_width {
                let RowPolicyValueSourceV1::RowField(source_field) = operand.source() else {
                    return Err(CompilerDiagnostic::new(
                        CompilerDiagnosticCode::CrossPartitionRowPolicy,
                        argument.span,
                    ));
                };
                let source = self
                    .entity
                    .fields
                    .iter()
                    .find(|field| field.id == *source_field)
                    .ok_or_else(|| invalid(argument.span))?;
                let target = target
                    .fields
                    .iter()
                    .find(|field| field.id == *field_id)
                    .ok_or_else(|| invalid(argument.span))?;
                if source.name != target.name || source.value_type != target.value_type {
                    return Err(CompilerDiagnostic::new(
                        CompilerDiagnosticCode::CrossPartitionRowPolicy,
                        argument.span,
                    ));
                }
            }
            lowered.push(operand);
        }
        self.push(
            RowPolicyExpressionNodeV1::IndexedExists {
                target_entity: target_id,
                index_id: target_index.id,
                arguments: lowered,
            },
            ValueType::bool(),
            span,
        )
    }

    fn lower_operand(
        &self,
        expression: &Spanned<RowPolicyExpression>,
        expected: Option<&ValueType>,
    ) -> Result<RowPolicyOperandV1, CompilerDiagnostic> {
        match &expression.value {
            RowPolicyExpression::Literal(literal) => {
                let (value, value_type) = lower_literal(&literal.value, expected)
                    .ok_or_else(|| invalid(expression.span))?;
                Ok(RowPolicyOperandV1::new(
                    RowPolicyValueSourceV1::Constant(value),
                    value_type,
                ))
            }
            RowPolicyExpression::Path(path) => self.resolve_path(path, expected),
            RowPolicyExpression::Parenthesized(inner) => self.lower_operand(inner, expected),
            _ => Err(CompilerDiagnostic::new(
                CompilerDiagnosticCode::InvalidRowPolicyRelationship,
                expression.span,
            )),
        }
    }

    fn resolve_path(
        &self,
        path: &Spanned<Path>,
        expected: Option<&ValueType>,
    ) -> Result<RowPolicyOperandV1, CompilerDiagnostic> {
        let segments = &path.value.segments;
        if segments.len() == 1 {
            if let Some(field) = self
                .entity
                .fields
                .iter()
                .find(|field| field.name == segments[0].value)
            {
                return Ok(RowPolicyOperandV1::new(
                    RowPolicyValueSourceV1::RowField(field.id),
                    field.value_type.clone(),
                ));
            }
            if let Some(expected_enum) = expected.and_then(ValueType::enum_type_id)
                && let Some(variant_id) = self
                    .symbols
                    .enum_variants
                    .get(&(expected_enum, segments[0].value.clone()))
                    .copied()
            {
                return Ok(RowPolicyOperandV1::new(
                    RowPolicyValueSourceV1::Constant(CanonicalValue::Enum {
                        type_id: expected_enum,
                        variant_id,
                    }),
                    ValueType::enumeration(expected_enum),
                ));
            }
            if expected.is_some_and(|ty| ty == &ValueType::string(16).expect("static bound"))
                && matches!(segments[0].value.as_str(), "Human" | "Agent" | "Service")
            {
                return Ok(RowPolicyOperandV1::new(
                    RowPolicyValueSourceV1::Constant(
                        CanonicalValue::string(segments[0].value.clone())
                            .expect("bounded actor-kind spelling"),
                    ),
                    ValueType::string(16).expect("static bound"),
                ));
            }
        }
        if segments.len() == 2 && segments[0].value == "principal" {
            return match segments[1].value.as_str() {
                "id" => Ok(RowPolicyOperandV1::new(
                    RowPolicyValueSourceV1::PrincipalId,
                    ValueType::uuid(),
                )),
                "kind" => Ok(RowPolicyOperandV1::new(
                    RowPolicyValueSourceV1::PrincipalKind,
                    ValueType::string(16).expect("static bound"),
                )),
                _ => Err(unknown(path.span)),
            };
        }
        if segments.len() == 3 && segments[0].value == "principal" && segments[1].value == "fact" {
            let name = &segments[2].value;
            let value_type = self
                .types
                .principal_facts
                .get(name)
                .cloned()
                .ok_or_else(|| unknown(path.span))?;
            return Ok(RowPolicyOperandV1::new(
                RowPolicyValueSourceV1::PrincipalFact(name.clone()),
                value_type,
            ));
        }
        if segments.len() == 2
            && let Some(enum_id) = self.symbols.enums.get(&segments[0].value).copied()
            && let Some(variant_id) = self
                .symbols
                .enum_variants
                .get(&(enum_id, segments[1].value.clone()))
                .copied()
        {
            return Ok(RowPolicyOperandV1::new(
                RowPolicyValueSourceV1::Constant(CanonicalValue::Enum {
                    type_id: enum_id,
                    variant_id,
                }),
                ValueType::enumeration(enum_id),
            ));
        }
        Err(unknown(path.span))
    }

    fn push_operand(
        &mut self,
        operand: RowPolicyOperandV1,
        span: Span,
    ) -> Result<(u16, ValueType), CompilerDiagnostic> {
        let value_type = operand.value_type().clone();
        self.push(
            RowPolicyExpressionNodeV1::Operand(operand),
            value_type,
            span,
        )
    }

    fn push(
        &mut self,
        node: RowPolicyExpressionNodeV1,
        value_type: ValueType,
        span: Span,
    ) -> Result<(u16, ValueType), CompilerDiagnostic> {
        let id = u16::try_from(self.nodes.len()).map_err(|_| {
            CompilerDiagnostic::new(CompilerDiagnosticCode::UnboundedRowPolicy, span)
        })?;
        self.nodes.push(node);
        Ok((id, value_type))
    }
}

fn invalid(span: Span) -> CompilerDiagnostic {
    CompilerDiagnostic::new(CompilerDiagnosticCode::InvalidRowPolicy, span)
}

fn unknown(span: Span) -> CompilerDiagnostic {
    CompilerDiagnostic::new(CompilerDiagnosticCode::UnknownName, span)
}

fn type_mismatch(span: Span) -> CompilerDiagnostic {
    CompilerDiagnostic::new(CompilerDiagnosticCode::TypeMismatch, span)
}

fn haystack_type_span(_node: u16, _nodes: &[RowPolicyExpressionNodeV1], fallback: Span) -> Span {
    fallback
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{compile_contract_source, validate_contract_source};

    const VALID: &str =
        include_str!("../../../fixtures/compiler/row-policy/valid/document-access.riff");

    #[test]
    fn owner_enum_and_bounded_fact_membership_lower_to_closed_ir() {
        let bundle = compile_contract_source(VALID).expect("policy contract compiles");
        assert_eq!(bundle.format_version(), 4);
        assert_eq!(bundle.grammar_version(), 4);
        assert_eq!(bundle.ir_version(), 4);
        let policy = &bundle.row_policies().policies()[0];
        assert_eq!(policy.name(), "DocumentAccess");
        assert_eq!(policy.rules().len(), 4);
        assert_eq!(bundle.row_policies().facts()[0].name(), "team_ids");
        assert_eq!(
            riffdb_contract_ir::ContractBundle::decode(bundle.canonical_bytes())
                .expect("v4 bundle decodes"),
            bundle
        );
    }

    #[test]
    fn callbacks_and_unknown_facts_fail_at_the_source_path() {
        let source = VALID.replace("principal.fact.team_ids", "principal.fact.admin_groups");
        let error = validate_contract_source(&source).expect_err("unknown fact must fail");
        let diagnostics = error.semantic().expect("semantic diagnostics").as_slice();
        assert!(
            diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code() == CompilerDiagnosticCode::UnknownName)
        );
    }

    #[test]
    fn one_complete_local_index_probe_compiles_and_unsafe_probe_shapes_have_exact_spans() {
        let valid = include_str!("../../../fixtures/compiler/row-policy/valid/document-grant.riff");
        let bundle = compile_contract_source(valid).expect("one local indexed probe");
        let rule = &bundle.row_policies().policies()[0].rules()[0];
        assert_eq!(
            rule.nodes()
                .iter()
                .filter(|node| matches!(node, RowPolicyExpressionNodeV1::IndexedExists { .. }))
                .count(),
            1
        );

        let cases = [
            (
                include_str!(
                    "../../../fixtures/compiler/row-policy/invalid/cross-aggregate-exists.riff"
                ),
                CompilerDiagnosticCode::CrossPartitionRowPolicy,
                "DocumentGrant",
            ),
            (
                include_str!("../../../fixtures/compiler/row-policy/invalid/unindexed-exists.riff"),
                CompilerDiagnosticCode::InvalidRowPolicyRelationship,
                "by_document_subject",
            ),
            (
                include_str!("../../../fixtures/compiler/row-policy/invalid/two-exists.riff"),
                CompilerDiagnosticCode::UnboundedRowPolicy,
                "exists DocumentGrant.by_document_subject",
            ),
        ];
        for (source, code, marker) in cases {
            let error = validate_contract_source(source).expect_err("unsafe probe must fail");
            let diagnostic = error
                .semantic()
                .expect("semantic diagnostics")
                .as_slice()
                .iter()
                .find(|diagnostic| diagnostic.code() == code)
                .expect("expected diagnostic");
            let marker_start = source.rfind(marker).expect("source marker") as u32;
            assert_eq!(diagnostic.primary_span().start(), marker_start);
            assert!(diagnostic.primary_span().end() > diagnostic.primary_span().start());
        }
    }
}
