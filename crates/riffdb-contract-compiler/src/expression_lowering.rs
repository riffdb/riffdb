//! Typed source-expression lowering into IR-owned topological arenas.

use std::collections::BTreeMap;

use riffdb_contract_ir::{
    BinaryOperator as IrBinaryOperator, BindingId, ExprId, ExpressionKind, RecordTypeRef,
    UnaryOperator as IrUnaryOperator, ValueType, ValueTypeTag,
};
use riffdb_contract_syntax::ast::{
    BinaryOperator, Expression, Literal, UnaryOperator as SourceUnaryOperator,
};
use riffdb_contract_syntax::{Span, Spanned};
use riffdb_types::{
    CanonicalValue, CommandId, Decimal, DecimalSpec, EntityTypeId, EventTypeId, FieldId, Money,
};

use crate::diagnostic::{CompilerDiagnostic, CompilerDiagnosticCode};
use crate::hir::{HirExpressionArena, HirExpressionNode};
use crate::literal::decode_json_string_lexeme;
use crate::symbols::GenesisSymbols;

/// One resolved command binding available to expression paths.
#[derive(Clone, Debug)]
pub(crate) struct BindingExpressionScope {
    pub(crate) id: BindingId,
    pub(crate) entity_id: EntityTypeId,
    pub(crate) fields: BTreeMap<String, (FieldId, ValueType)>,
}

/// Names available to one expression arena.
#[derive(Clone, Debug)]
pub(crate) enum ExpressionScope {
    Schema {
        entity_id: EntityTypeId,
        fields: BTreeMap<String, (FieldId, ValueType)>,
    },
    Migration {
        entity_id: EntityTypeId,
        fields: BTreeMap<String, (FieldId, ValueType)>,
    },
    Command {
        command_id: CommandId,
        inputs: BTreeMap<String, (FieldId, ValueType)>,
        bindings: BTreeMap<String, BindingExpressionScope>,
    },
    Projection {
        event_id: EventTypeId,
        fields: BTreeMap<String, (FieldId, ValueType)>,
    },
}

/// Stateful lowering for one deterministic expression arena.
pub(crate) struct ExpressionLowerer<'a> {
    symbols: &'a GenesisSymbols,
    scope: ExpressionScope,
    input_only: bool,
    nodes: Vec<HirExpressionNode>,
}

impl<'a> ExpressionLowerer<'a> {
    pub(crate) fn new(symbols: &'a GenesisSymbols, scope: ExpressionScope) -> Self {
        Self {
            symbols,
            scope,
            input_only: false,
            nodes: Vec::new(),
        }
    }

    /// Lowers one expression using an optional exact contextual type.
    pub(crate) fn lower(
        &mut self,
        expression: &Spanned<Expression>,
        expected: Option<&ValueType>,
    ) -> Result<(ExprId, ValueType), CompilerDiagnostic> {
        match &expression.value {
            Expression::Literal(literal) => {
                let (value, value_type) = lower_literal(&literal.value, expected)
                    .ok_or_else(|| type_diagnostic(expression.span))?;
                self.push(ExpressionKind::Constant(value), value_type, expression.span)
            }
            Expression::Path(path) => {
                let (kind, value_type) = self.resolve_path(path)?;
                require_expected(&value_type, expected, expression.span)?;
                self.push(kind, value_type, expression.span)
            }
            Expression::Parenthesized(inner) => self.lower(inner, expected),
            Expression::Unary { operator, operand } => {
                if matches!(operator.value, SourceUnaryOperator::Negate)
                    && let Some(folded) = lower_min_i64_literal(operand, expected)
                {
                    return self.push(
                        ExpressionKind::Constant(folded),
                        ValueType::i64(),
                        expression.span,
                    );
                }
                let operand_expected = expected.filter(|expected| {
                    nonoptional_expected(expected).is_some_and(|expected| {
                        matches!(
                            (operator.value, expected.tag()),
                            (SourceUnaryOperator::Not, ValueTypeTag::Bool)
                                | (
                                    SourceUnaryOperator::Negate,
                                    ValueTypeTag::I64 | ValueTypeTag::Decimal | ValueTypeTag::Money
                                )
                        )
                    })
                });
                let (operand_id, operand_type) = self.lower(operand, operand_expected)?;
                let result_type = match operator.value {
                    SourceUnaryOperator::Not if operand_type.tag() == ValueTypeTag::Bool => {
                        ValueType::bool()
                    }
                    SourceUnaryOperator::Negate
                        if matches!(
                            operand_type.tag(),
                            ValueTypeTag::I64 | ValueTypeTag::Decimal | ValueTypeTag::Money
                        ) =>
                    {
                        operand_type.clone()
                    }
                    _ => return Err(type_diagnostic(expression.span)),
                };
                require_expected(&result_type, expected, expression.span)?;
                self.push(
                    ExpressionKind::Unary {
                        operator: map_unary(operator.value),
                        operand: operand_id,
                    },
                    result_type,
                    expression.span,
                )
            }
            Expression::Binary {
                left,
                operator,
                right,
            } => {
                let (left_expected, right_expected, result_type) =
                    self.infer_binary(left, operator.value, right, expected, expression.span)?;
                let (left_id, left_type) = self.lower(left, Some(&left_expected))?;
                let (right_id, right_type) = self.lower(right, Some(&right_expected))?;
                if left_type != left_expected || right_type != right_expected {
                    return Err(type_diagnostic(expression.span));
                }
                self.push(
                    ExpressionKind::Binary {
                        operator: map_binary(operator.value),
                        left: left_id,
                        right: right_id,
                    },
                    result_type,
                    expression.span,
                )
            }
        }
    }

    /// Lowers an ADR-0015 binding-failure expression from inputs and constants only.
    pub(crate) fn lower_input_only(
        &mut self,
        expression: &Spanned<Expression>,
        expected: Option<&ValueType>,
    ) -> Result<(ExprId, ValueType), CompilerDiagnostic> {
        let previous = std::mem::replace(&mut self.input_only, true);
        let result = self.lower(expression, expected);
        self.input_only = previous;
        result
    }

    /// Appends a compiler-synthesized canonical constant such as optional null fill.
    pub(crate) fn push_constant(
        &mut self,
        value: CanonicalValue,
        value_type: ValueType,
        span: Span,
    ) -> Result<ExprId, CompilerDiagnostic> {
        value_type
            .validate_value(&value)
            .map_err(|_| type_diagnostic(span))?;
        self.push(ExpressionKind::Constant(value), value_type, span)
            .map(|(id, _)| id)
    }

    /// Finishes the source-spanned resolved typed HIR arena.
    pub(crate) fn finish_hir(self, span: Span) -> Result<HirExpressionArena, CompilerDiagnostic> {
        let arena = HirExpressionArena { nodes: self.nodes };
        arena.to_ir(span)?;
        Ok(arena)
    }

    fn infer_type(
        &self,
        expression: &Spanned<Expression>,
        expected: Option<&ValueType>,
    ) -> Result<ValueType, CompilerDiagnostic> {
        match &expression.value {
            Expression::Literal(literal) => lower_literal(&literal.value, expected)
                .map(|(_, value_type)| value_type)
                .ok_or_else(|| type_diagnostic(expression.span)),
            Expression::Path(path) => {
                let (_, value_type) = self.resolve_path(path)?;
                require_expected(&value_type, expected, expression.span)?;
                Ok(value_type)
            }
            Expression::Parenthesized(inner) => self.infer_type(inner, expected),
            Expression::Unary { operator, operand } => {
                if matches!(operator.value, SourceUnaryOperator::Negate)
                    && lower_min_i64_literal(operand, expected).is_some()
                {
                    return Ok(ValueType::i64());
                }
                let operand_type = self.infer_type(operand, expected)?;
                let result = match operator.value {
                    SourceUnaryOperator::Not if operand_type.tag() == ValueTypeTag::Bool => {
                        ValueType::bool()
                    }
                    SourceUnaryOperator::Negate
                        if matches!(
                            operand_type.tag(),
                            ValueTypeTag::I64 | ValueTypeTag::Decimal | ValueTypeTag::Money
                        ) =>
                    {
                        operand_type
                    }
                    _ => return Err(type_diagnostic(expression.span)),
                };
                require_expected(&result, expected, expression.span)?;
                Ok(result)
            }
            Expression::Binary {
                left,
                operator,
                right,
            } => self
                .infer_binary(left, operator.value, right, expected, expression.span)
                .map(|(_, _, result)| result),
        }
    }

    fn infer_binary(
        &self,
        left: &Spanned<Expression>,
        operator: BinaryOperator,
        right: &Spanned<Expression>,
        expected: Option<&ValueType>,
        span: Span,
    ) -> Result<(ValueType, ValueType, ValueType), CompilerDiagnostic> {
        let arithmetic = matches!(
            operator,
            BinaryOperator::Multiply
                | BinaryOperator::Divide
                | BinaryOperator::Add
                | BinaryOperator::Subtract
        );
        let (left_type, right_type) = if let (true, Some(expected)) = (arithmetic, expected) {
            let expected = expected.optional_inner().unwrap_or(expected).clone();
            (expected.clone(), expected)
        } else if is_null_literal(left) {
            let right_type = self.infer_type(right, None)?;
            if !right_type.is_optional() {
                return Err(type_diagnostic(span));
            }
            (right_type.clone(), right_type)
        } else if is_null_literal(right) {
            let left_type = self.infer_type(left, None)?;
            if !left_type.is_optional() {
                return Err(type_diagnostic(span));
            }
            (left_type.clone(), left_type)
        } else if is_contextual_literal(left) && !is_contextual_literal(right) {
            let right_type = self.infer_type(right, None)?;
            let left_type = self.infer_type(left, Some(&right_type))?;
            (left_type, right_type)
        } else if !is_contextual_literal(left) && is_contextual_literal(right) {
            let left_type = self.infer_type(left, None)?;
            let right_type = self.infer_type(right, Some(&left_type))?;
            (left_type, right_type)
        } else if is_contextual_literal(left) && is_contextual_literal(right) {
            infer_literal_pair(left, right, span)?
        } else {
            (self.infer_type(left, None)?, self.infer_type(right, None)?)
        };
        if left_type != right_type {
            return Err(type_diagnostic(span));
        }

        let result_type = match operator {
            BinaryOperator::Multiply | BinaryOperator::Divide
                if matches!(left_type.tag(), ValueTypeTag::I64 | ValueTypeTag::U64) =>
            {
                left_type.clone()
            }
            BinaryOperator::Add | BinaryOperator::Subtract
                if matches!(
                    left_type.tag(),
                    ValueTypeTag::I64
                        | ValueTypeTag::U64
                        | ValueTypeTag::Decimal
                        | ValueTypeTag::Money
                ) =>
            {
                left_type.clone()
            }
            BinaryOperator::Equal | BinaryOperator::NotEqual if left_type.supports_equality() => {
                ValueType::bool()
            }
            BinaryOperator::Less
            | BinaryOperator::LessEqual
            | BinaryOperator::Greater
            | BinaryOperator::GreaterEqual
                if matches!(
                    left_type.tag(),
                    ValueTypeTag::I64
                        | ValueTypeTag::U64
                        | ValueTypeTag::Decimal
                        | ValueTypeTag::Money
                        | ValueTypeTag::Timestamp
                        | ValueTypeTag::Date
                ) =>
            {
                ValueType::bool()
            }
            BinaryOperator::And | BinaryOperator::Or if left_type.tag() == ValueTypeTag::Bool => {
                ValueType::bool()
            }
            _ => return Err(type_diagnostic(span)),
        };
        require_expected(&result_type, expected, span)?;
        Ok((left_type, right_type, result_type))
    }

    fn resolve_path(
        &self,
        path: &Spanned<riffdb_contract_syntax::ast::Path>,
    ) -> Result<(ExpressionKind, ValueType), CompilerDiagnostic> {
        let segments = &path.value.segments;
        if segments.len() == 2 {
            let first = &segments[0].value;
            let second = &segments[1].value;
            let binding_ambiguous = matches!(
                &self.scope,
                ExpressionScope::Command { bindings, .. } if bindings.contains_key(first)
            ) && self.symbols.enums.contains_key(first);
            if binding_ambiguous {
                return Err(CompilerDiagnostic::new(
                    CompilerDiagnosticCode::InvalidExpression,
                    path.span,
                ));
            }
            if let Some(enum_id) = self.symbols.enums.get(first).copied()
                && let Some(variant_id) = self
                    .symbols
                    .enum_variants
                    .get(&(enum_id, second.clone()))
                    .copied()
            {
                return Ok((
                    ExpressionKind::Constant(CanonicalValue::Enum {
                        type_id: enum_id,
                        variant_id,
                    }),
                    ValueType::enumeration(enum_id),
                ));
            }
        }

        match &self.scope {
            ExpressionScope::Schema { entity_id, fields } if segments.len() == 1 => fields
                .get(&segments[0].value)
                .cloned()
                .map(|(field, value_type)| {
                    (
                        ExpressionKind::SchemaField {
                            entity_type: *entity_id,
                            field,
                        },
                        value_type,
                    )
                }),
            ExpressionScope::Migration { entity_id, fields }
                if segments.len() == 2 && segments[0].value == "old" =>
            {
                fields
                    .get(&segments[1].value)
                    .cloned()
                    .map(|(field, value_type)| {
                        (
                            ExpressionKind::SchemaField {
                                entity_type: *entity_id,
                                field,
                            },
                            value_type,
                        )
                    })
            }
            ExpressionScope::Command {
                command_id,
                inputs,
                bindings,
            } => {
                let _ = command_id;
                if segments.len() == 1 {
                    if let Some((field, value_type)) = inputs.get(&segments[0].value).cloned() {
                        Some((ExpressionKind::InputField(field), value_type))
                    } else if self.input_only {
                        None
                    } else {
                        bindings.get(&segments[0].value).map(|binding| {
                            (
                                ExpressionKind::CompleteBinding(binding.id),
                                ValueType::record(RecordTypeRef::Entity(binding.entity_id)),
                            )
                        })
                    }
                } else if self.input_only {
                    None
                } else if segments.len() == 2
                    && segments[0].value == "tx"
                    && segments[1].value == "time"
                {
                    Some((ExpressionKind::TransactionTime, ValueType::timestamp()))
                } else if segments.len() == 2 {
                    bindings.get(&segments[0].value).and_then(|binding| {
                        binding.fields.get(&segments[1].value).cloned().map(
                            |(field, value_type)| {
                                (
                                    ExpressionKind::BoundField {
                                        binding: binding.id,
                                        field,
                                    },
                                    value_type,
                                )
                            },
                        )
                    })
                } else {
                    None
                }
            }
            ExpressionScope::Projection { event_id, fields } => {
                let _ = event_id;
                if segments.len() == 1 {
                    fields
                        .get(&segments[0].value)
                        .cloned()
                        .map(|(field, value_type)| {
                            (ExpressionKind::SourceEventField(field), value_type)
                        })
                } else if segments.len() == 2
                    && segments[0].value == "tx"
                    && segments[1].value == "date"
                {
                    Some((ExpressionKind::TransactionDate, ValueType::date()))
                } else {
                    None
                }
            }
            _ => None,
        }
        .ok_or_else(|| CompilerDiagnostic::new(CompilerDiagnosticCode::UnknownName, path.span))
    }

    fn push(
        &mut self,
        kind: ExpressionKind,
        value_type: ValueType,
        span: Span,
    ) -> Result<(ExprId, ValueType), CompilerDiagnostic> {
        let index = u32::try_from(self.nodes.len())
            .map_err(|_| CompilerDiagnostic::new(CompilerDiagnosticCode::BoundExceeded, span))?;
        self.nodes.push(HirExpressionNode {
            kind,
            value_type: value_type.clone(),
            span,
        });
        Ok((ExprId::new(index), value_type))
    }
}

fn require_expected(
    actual: &ValueType,
    expected: Option<&ValueType>,
    span: Span,
) -> Result<(), CompilerDiagnostic> {
    let valid = expected.is_none_or(|expected| {
        actual == expected
            || expected
                .optional_inner()
                .is_some_and(|inner| inner == actual)
    });
    if valid {
        Ok(())
    } else {
        Err(type_diagnostic(span))
    }
}

fn lower_literal(
    literal: &Literal,
    expected: Option<&ValueType>,
) -> Option<(CanonicalValue, ValueType)> {
    match literal {
        Literal::Bool(value) => {
            let value_type = ValueType::bool();
            require_literal_expected(&value_type, expected)?;
            Some((CanonicalValue::Bool(*value), value_type))
        }
        Literal::Null => {
            let expected = expected?.clone();
            expected
                .is_optional()
                .then_some((CanonicalValue::Null, expected))
        }
        Literal::UInt(lexeme) => lower_uint_literal(lexeme, expected),
        Literal::FixedDecimal(lexeme) => lower_fixed_decimal_literal(lexeme, expected),
        Literal::String(lexeme) => {
            let value = decode_json_string_lexeme(lexeme)?;
            let value_type = match expected {
                Some(expected) if expected.tag() == ValueTypeTag::String => expected.clone(),
                Some(expected)
                    if expected
                        .optional_inner()
                        .is_some_and(|inner| inner.tag() == ValueTypeTag::String) =>
                {
                    expected.optional_inner()?.clone()
                }
                Some(_) => return None,
                None => ValueType::string(value.len().max(1)).ok()?,
            };
            let canonical = CanonicalValue::string(value).ok()?;
            value_type.validate_value(&canonical).ok()?;
            Some((canonical, value_type))
        }
    }
}

fn lower_uint_literal(
    lexeme: &str,
    expected: Option<&ValueType>,
) -> Option<(CanonicalValue, ValueType)> {
    let exact_expected = expected.and_then(nonoptional_expected);
    match exact_expected.map(ValueType::tag) {
        Some(ValueTypeTag::I64) => {
            let value = lexeme.parse::<i64>().ok()?;
            Some((CanonicalValue::I64(value), ValueType::i64()))
        }
        Some(ValueTypeTag::U64) => {
            let value = lexeme.parse::<u64>().ok()?;
            Some((CanonicalValue::U64(value), ValueType::u64()))
        }
        Some(ValueTypeTag::Decimal) => {
            let value_type = exact_expected?.clone();
            let spec = value_type.decimal_spec()?;
            let coefficient = parse_scaled_uint(lexeme, spec.scale())?;
            Some((
                CanonicalValue::Decimal(Decimal::new(spec, coefficient).ok()?),
                value_type,
            ))
        }
        Some(ValueTypeTag::Money) => {
            let value_type = exact_expected?.clone();
            let currency = value_type.currency()?;
            let spec = DecimalSpec::new(38, 2).ok()?;
            let coefficient = parse_scaled_uint(lexeme, 2)?;
            Some((
                CanonicalValue::Money(Money::new(currency, Decimal::new(spec, coefficient).ok()?)),
                value_type,
            ))
        }
        Some(_) => None,
        None => {
            if let Ok(value) = lexeme.parse::<i64>() {
                Some((CanonicalValue::I64(value), ValueType::i64()))
            } else {
                let value = lexeme.parse::<u64>().ok()?;
                Some((CanonicalValue::U64(value), ValueType::u64()))
            }
        }
    }
}

fn lower_fixed_decimal_literal(
    lexeme: &str,
    expected: Option<&ValueType>,
) -> Option<(CanonicalValue, ValueType)> {
    let (_, fraction) = lexeme.split_once('.')?;
    let scale = u8::try_from(fraction.len()).ok()?;
    let exact_expected = expected.and_then(nonoptional_expected);
    match exact_expected.map(ValueType::tag) {
        Some(ValueTypeTag::Decimal) => {
            let value_type = exact_expected?.clone();
            let spec = value_type.decimal_spec()?;
            if spec.scale() != scale {
                return None;
            }
            let coefficient = parse_fixed_coefficient(lexeme)?;
            Some((
                CanonicalValue::Decimal(Decimal::new(spec, coefficient).ok()?),
                value_type,
            ))
        }
        Some(ValueTypeTag::Money) => {
            let value_type = exact_expected?.clone();
            if scale != 2 {
                return None;
            }
            let currency = value_type.currency()?;
            let spec = DecimalSpec::new(38, 2).ok()?;
            let coefficient = parse_fixed_coefficient(lexeme)?;
            Some((
                CanonicalValue::Money(Money::new(currency, Decimal::new(spec, coefficient).ok()?)),
                value_type,
            ))
        }
        Some(_) => None,
        None => {
            let coefficient = parse_fixed_coefficient(lexeme)?;
            let digits = decimal_precision(coefficient).max(scale).max(1);
            let spec = DecimalSpec::new(digits, scale).ok()?;
            Some((
                CanonicalValue::Decimal(Decimal::new(spec, coefficient).ok()?),
                ValueType::decimal(spec),
            ))
        }
    }
}

fn nonoptional_expected(expected: &ValueType) -> Option<&ValueType> {
    expected.optional_inner().or(Some(expected))
}

fn require_literal_expected(actual: &ValueType, expected: Option<&ValueType>) -> Option<()> {
    expected
        .is_none_or(|expected| {
            expected == actual
                || expected
                    .optional_inner()
                    .is_some_and(|inner| inner == actual)
        })
        .then_some(())
}

fn parse_scaled_uint(lexeme: &str, scale: u8) -> Option<i128> {
    let value = lexeme.parse::<i128>().ok()?;
    value.checked_mul(10_i128.checked_pow(u32::from(scale))?)
}

fn parse_fixed_coefficient(lexeme: &str) -> Option<i128> {
    let (integer, fraction) = lexeme.split_once('.')?;
    format!("{integer}{fraction}").parse::<i128>().ok()
}

fn decimal_precision(value: i128) -> u8 {
    let digits = value.unsigned_abs().to_string().len();
    u8::try_from(digits).expect("i128 decimal digits fit u8")
}

fn lower_min_i64_literal(
    operand: &Spanned<Expression>,
    expected: Option<&ValueType>,
) -> Option<CanonicalValue> {
    if expected
        .and_then(nonoptional_expected)
        .is_some_and(|expected| expected.tag() != ValueTypeTag::I64)
    {
        return None;
    }
    let Expression::Literal(literal) = &operand.value else {
        return None;
    };
    let Literal::UInt(lexeme) = &literal.value else {
        return None;
    };
    (lexeme == "9223372036854775808").then_some(CanonicalValue::I64(i64::MIN))
}

fn infer_literal_pair(
    left: &Spanned<Expression>,
    right: &Spanned<Expression>,
    span: Span,
) -> Result<(ValueType, ValueType), CompilerDiagnostic> {
    let left_literal = root_literal(left);
    let right_literal = root_literal(right);
    match (left_literal, right_literal) {
        (Some(Literal::UInt(left)), Some(Literal::UInt(right))) => {
            let use_i64 = left.parse::<i64>().is_ok() && right.parse::<i64>().is_ok();
            let value_type = if use_i64 {
                ValueType::i64()
            } else {
                ValueType::u64()
            };
            Ok((value_type.clone(), value_type))
        }
        (Some(Literal::FixedDecimal(left)), Some(Literal::FixedDecimal(right))) => {
            let left_scale = left.split_once('.').map(|(_, value)| value.len());
            let right_scale = right.split_once('.').map(|(_, value)| value.len());
            if left_scale != right_scale {
                return Err(type_diagnostic(span));
            }
            let (_, left_type) =
                lower_fixed_decimal_literal(left, None).ok_or_else(|| type_diagnostic(span))?;
            let (_, right_type) =
                lower_fixed_decimal_literal(right, None).ok_or_else(|| type_diagnostic(span))?;
            let precision = left_type
                .decimal_spec()
                .expect("decimal")
                .precision()
                .max(right_type.decimal_spec().expect("decimal").precision());
            let scale = left_type.decimal_spec().expect("decimal").scale();
            let value_type = ValueType::decimal(
                DecimalSpec::new(precision, scale).map_err(|_| type_diagnostic(span))?,
            );
            Ok((value_type.clone(), value_type))
        }
        (Some(Literal::String(left)), Some(Literal::String(right))) => {
            let left = decode_json_string_lexeme(left).ok_or_else(|| type_diagnostic(span))?;
            let right = decode_json_string_lexeme(right).ok_or_else(|| type_diagnostic(span))?;
            let value_type = ValueType::string(left.len().max(right.len()).max(1))
                .map_err(|_| type_diagnostic(span))?;
            Ok((value_type.clone(), value_type))
        }
        _ => {
            let (_, left_type) = root_literal(left)
                .and_then(|literal| lower_literal(literal, None))
                .ok_or_else(|| type_diagnostic(span))?;
            let (_, right_type) = root_literal(right)
                .and_then(|literal| lower_literal(literal, Some(&left_type)))
                .ok_or_else(|| type_diagnostic(span))?;
            Ok((left_type, right_type))
        }
    }
}

fn root_literal(expression: &Spanned<Expression>) -> Option<&Literal> {
    match &expression.value {
        Expression::Literal(literal) => Some(&literal.value),
        Expression::Parenthesized(inner) => root_literal(inner),
        _ => None,
    }
}

fn is_contextual_literal(expression: &Spanned<Expression>) -> bool {
    root_literal(expression).is_some()
}

fn is_null_literal(expression: &Spanned<Expression>) -> bool {
    matches!(root_literal(expression), Some(Literal::Null))
}

fn map_unary(operator: SourceUnaryOperator) -> IrUnaryOperator {
    match operator {
        SourceUnaryOperator::Not => IrUnaryOperator::Not,
        SourceUnaryOperator::Negate => IrUnaryOperator::Negate,
    }
}

fn map_binary(operator: BinaryOperator) -> IrBinaryOperator {
    match operator {
        BinaryOperator::Multiply => IrBinaryOperator::Multiply,
        BinaryOperator::Divide => IrBinaryOperator::Divide,
        BinaryOperator::Add => IrBinaryOperator::Add,
        BinaryOperator::Subtract => IrBinaryOperator::Subtract,
        BinaryOperator::Equal => IrBinaryOperator::Equal,
        BinaryOperator::NotEqual => IrBinaryOperator::NotEqual,
        BinaryOperator::Less => IrBinaryOperator::Less,
        BinaryOperator::LessEqual => IrBinaryOperator::LessEqual,
        BinaryOperator::Greater => IrBinaryOperator::Greater,
        BinaryOperator::GreaterEqual => IrBinaryOperator::GreaterEqual,
        BinaryOperator::And => IrBinaryOperator::And,
        BinaryOperator::Or => IrBinaryOperator::Or,
    }
}

fn type_diagnostic(span: Span) -> CompilerDiagnostic {
    CompilerDiagnostic::new(CompilerDiagnosticCode::TypeMismatch, span)
}

#[cfg(test)]
mod tests {
    use riffdb_contract_syntax::parse_contract;
    use riffdb_types::{CommandId, EntityTypeId, FieldId};

    use super::*;
    use crate::symbols::allocate_genesis_symbols;

    fn expression(
        source: &str,
    ) -> (
        riffdb_contract_syntax::ContractDocument,
        Spanned<Expression>,
    ) {
        let document = parse_contract(source).expect("valid syntax");
        let expression = match &document.contract.value.declarations[0].value {
            riffdb_contract_syntax::ast::Declaration::Entity(entity) => {
                match &entity.items[1].value {
                    riffdb_contract_syntax::ast::EntityItem::Invariant(invariant) => {
                        invariant.expression.clone()
                    }
                    _ => panic!("expected invariant"),
                }
            }
            _ => panic!("expected entity"),
        };
        (document, expression)
    }

    #[test]
    fn contextual_decimal_and_schema_field_lower_to_checked_arena() {
        let source = r#"
contract Example version 1 {
  entity Row {
    key (id: uuid)
    invariant positive: amount > 0.00
    field amount: decimal<12,2>
  }
}
"#;
        let (document, expression) = expression(source);
        let symbols = allocate_genesis_symbols(&document).expect("symbols");
        let entity_id = symbols.entities["Row"];
        let amount_id = symbols.entity_fields[&(entity_id, "amount".to_owned())];
        let mut fields = BTreeMap::new();
        fields.insert(
            "amount".to_owned(),
            (
                amount_id,
                ValueType::decimal(DecimalSpec::new(12, 2).expect("spec")),
            ),
        );
        let mut lowerer =
            ExpressionLowerer::new(&symbols, ExpressionScope::Schema { entity_id, fields });
        let (predicate, ty) = lowerer
            .lower(&expression, Some(&ValueType::bool()))
            .expect("lower");
        assert_eq!(ty, ValueType::bool());
        let arena = lowerer
            .finish_hir(expression.span)
            .expect("HIR arena")
            .to_ir(expression.span)
            .expect("IR arena");
        assert_eq!(
            arena.get(predicate).expect("predicate").result_type(),
            &ValueType::bool()
        );
    }

    #[test]
    fn command_scope_resolves_inputs_bindings_and_transaction_time() {
        let document =
            parse_contract("contract Example version 1 { enum State { Open } }").expect("syntax");
        let symbols = allocate_genesis_symbols(&document).expect("symbols");
        let command_id = CommandId::new(1).expect("id");
        let entity_id = EntityTypeId::new(1).expect("id");
        let field_id = FieldId::new(1).expect("id");
        let scope = ExpressionScope::Command {
            command_id,
            inputs: BTreeMap::from([("amount".to_owned(), (field_id, ValueType::i64()))]),
            bindings: BTreeMap::from([(
                "row".to_owned(),
                BindingExpressionScope {
                    id: BindingId::new(0),
                    entity_id,
                    fields: BTreeMap::from([("amount".to_owned(), (field_id, ValueType::i64()))]),
                },
            )]),
        };
        let mut lowerer = ExpressionLowerer::new(&symbols, scope);
        let source = r#"
contract C version 1 {
  entity E { key (id: i64) invariant x: 1 == 1 }
}
"#;
        let (_, expression) = expression(source);
        lowerer
            .lower(&expression, Some(&ValueType::bool()))
            .expect("lower");
        lowerer
            .finish_hir(expression.span)
            .expect("HIR arena")
            .to_ir(expression.span)
            .expect("IR arena");
    }

    #[test]
    fn optional_list_equality_is_a_source_spanned_type_error() {
        let source = r#"
contract Example version 1 {
  entity Row {
    key (id: uuid)
    invariant invalid: values == null
    field values: optional<list<i64, 4>>
  }
}
"#;
        let (document, expression) = expression(source);
        let symbols = allocate_genesis_symbols(&document).expect("symbols");
        let entity_id = symbols.entities["Row"];
        let field_id = symbols.entity_fields[&(entity_id, "values".to_owned())];
        let list = ValueType::list(ValueType::i64(), 4).expect("list type");
        let optional = ValueType::optional(list).expect("optional list");
        let fields = BTreeMap::from([("values".to_owned(), (field_id, optional))]);
        let mut lowerer =
            ExpressionLowerer::new(&symbols, ExpressionScope::Schema { entity_id, fields });
        let diagnostic = lowerer
            .lower(&expression, Some(&ValueType::bool()))
            .expect_err("optional list equality rejects during inference");
        assert_eq!(diagnostic.code(), CompilerDiagnosticCode::TypeMismatch);
        assert_eq!(diagnostic.primary_span(), expression.span);
    }

    #[test]
    fn signed_minimum_uses_the_inner_optional_i64_context() {
        let document =
            parse_contract("contract Example version 1 { enum State { Open } }").expect("syntax");
        let symbols = allocate_genesis_symbols(&document).expect("symbols");
        let command_id = CommandId::new(1).expect("id");
        let mut lowerer = ExpressionLowerer::new(
            &symbols,
            ExpressionScope::Command {
                command_id,
                inputs: BTreeMap::new(),
                bindings: BTreeMap::new(),
            },
        );
        let source = r#"
contract C version 1 {
  entity E { key (id: i64) invariant minimum: -9223372036854775808 < 0 }
}
"#;
        let (_, predicate) = expression(source);
        let Expression::Binary { left, .. } = &predicate.value else {
            panic!("binary predicate");
        };
        let expected = ValueType::optional(ValueType::i64()).expect("optional i64");
        let (minimum, value_type) = lowerer
            .lower(left, Some(&expected))
            .expect("contextual signed minimum");
        assert_eq!(value_type, ValueType::i64());
        let arena = lowerer.finish_hir(left.span).expect("HIR arena");
        assert!(matches!(
            arena.node(minimum).map(|node| &node.kind),
            Some(ExpressionKind::Constant(CanonicalValue::I64(i64::MIN)))
        ));
    }
}
