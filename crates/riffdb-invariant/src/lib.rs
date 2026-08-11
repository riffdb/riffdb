#![forbid(unsafe_code)]

//! Pure evaluation of checked RiffDB expressions and commit-check predicates.
//!
//! The crate evaluates only compiler-produced, typed IR. Callers provide values
//! through [`ExpressionValueSource`]; the evaluator owns no clock, storage,
//! randomness, or process-global state.

mod input_facts;

pub use input_facts::{InputDerivedCommandFacts, derive_input_command_facts};

use std::cmp::Ordering;
use std::error::Error;
use std::fmt;
use std::rc::Rc;

use riffdb_contract_ir::{
    BinaryOperator, BindingId, CommitCheckPlan, ExprId, ExpressionArena, ExpressionKind,
    RootValidationReadId, UnaryOperator,
};
use riffdb_types::{CanonicalValue, Date, EntityTypeId, FieldId, InvariantId, LogicalTime};

/// A closed deterministic expression-evaluation failure.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum EvaluationError {
    /// Checked arithmetic overflow, underflow, or invalid division.
    Arithmetic,
    /// A checked plan and supplied value context are structurally inconsistent.
    Integrity,
}

impl fmt::Display for EvaluationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Arithmetic => "checked arithmetic failed",
            Self::Integrity => "expression context is inconsistent with the checked plan",
        })
    }
}

impl Error for EvaluationError {}

/// Values visible to one pure expression evaluation.
///
/// Default methods make each evaluation context explicit about the value kinds
/// it supports. A missing value is an integrity failure, never canonical null.
pub trait ExpressionValueSource {
    /// Resolves a checked command-input field.
    fn input_field(&self, _field: FieldId) -> Option<CanonicalValue> {
        None
    }

    /// Resolves a compiler-declared service-owned command value.
    fn service_value(&self, _field: FieldId) -> Option<CanonicalValue> {
        None
    }

    /// Resolves the current submitted element in one compiler-owned collection expansion.
    fn collection_element(&self) -> Option<CanonicalValue> {
        None
    }

    /// Resolves one stable field of the current record-valued collection element.
    fn collection_element_field(&self, _field: FieldId) -> Option<CanonicalValue> {
        None
    }

    /// Resolves one complete bound entity record.
    fn complete_binding(&self, _binding: BindingId) -> Option<CanonicalValue> {
        None
    }

    /// Resolves one field of a bound entity record.
    fn bound_field(&self, _binding: BindingId, _field: FieldId) -> Option<CanonicalValue> {
        None
    }

    /// Resolves a field while evaluating an uninstantiated schema invariant.
    fn schema_field(&self, _entity_type: EntityTypeId, _field: FieldId) -> Option<CanonicalValue> {
        None
    }

    /// Resolves one field of a compiler-declared aggregate-root observation.
    fn root_validation_field(
        &self,
        _read: RootValidationReadId,
        _field: FieldId,
    ) -> Option<CanonicalValue> {
        None
    }

    /// Resolves one projection source-event field.
    fn source_event_field(&self, _field: FieldId) -> Option<CanonicalValue> {
        None
    }

    /// Returns the coordinator-supplied logical command time.
    fn transaction_time(&self) -> Option<LogicalTime> {
        None
    }

    /// Returns the date derived by projection orchestration from committed time.
    fn transaction_date(&self) -> Option<Date> {
        None
    }
}

/// Result of evaluating all exact historical commit checks in canonical order.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CommitCheckResult {
    /// Every commit-check predicate evaluated true.
    Satisfied,
    /// The first canonical commit check that evaluated false.
    Rejected {
        /// Stable identity of the rejected invariant.
        invariant_id: InvariantId,
    },
}

/// Evaluates one typed expression template.
///
/// Every top-level call uses a fresh cache. Shared nodes are evaluated once
/// within that call, while no value is retained across instructions or calls.
pub fn evaluate_expression(
    arena: &ExpressionArena,
    expression: ExprId,
    values: &impl ExpressionValueSource,
) -> Result<CanonicalValue, EvaluationError> {
    let mut evaluator = ExpressionEvaluator::new(arena);
    evaluator.batch(values).evaluate(expression)
}

/// Reusable allocation for expression batches over one checked arena.
///
/// Each [`EvaluationBatch`] memoizes values only for its immutable value
/// source. Dropping the batch clears exactly the visited slots, so callers can
/// reuse the arena-sized allocation after command state changes without
/// retaining stale values.
pub struct ExpressionEvaluator<'arena> {
    arena: &'arena ExpressionArena,
    cache: Vec<Option<Result<Rc<CanonicalValue>, EvaluationError>>>,
    visited: Vec<usize>,
}

impl<'arena> ExpressionEvaluator<'arena> {
    /// Allocates one cache for repeated bounded batches over `arena`.
    #[must_use]
    pub fn new(arena: &'arena ExpressionArena) -> Self {
        Self {
            arena,
            cache: vec![None; arena.len()],
            visited: Vec::new(),
        }
    }

    /// Starts one memoized batch against an immutable value source.
    pub fn batch<'evaluation, 'values, Values: ExpressionValueSource + ?Sized>(
        &'evaluation mut self,
        values: &'values Values,
    ) -> EvaluationBatch<'evaluation, 'arena, 'values, Values> {
        // Safe code can forget a prior batch and skip its Drop implementation.
        // Clearing again here prevents stale values from crossing sources.
        self.clear_visited();
        EvaluationBatch {
            evaluator: self,
            values,
        }
    }

    fn evaluate_shared(
        &mut self,
        expression: ExprId,
        values: &(impl ExpressionValueSource + ?Sized),
    ) -> Result<Rc<CanonicalValue>, EvaluationError> {
        let index = expression.get() as usize;
        if let Some(cached) = self.cache.get(index).and_then(Option::as_ref) {
            return cached.clone();
        }

        let (kind, result_type) = self
            .arena
            .get(expression)
            .map(|node| (node.kind().clone(), node.result_type().clone()))
            .ok_or(EvaluationError::Integrity)?;
        let result = self.evaluate_kind(kind, values).and_then(|value| {
            result_type
                .validate_value(value.as_ref())
                .map_err(|_| EvaluationError::Integrity)?;
            Ok(value)
        });
        let slot = self
            .cache
            .get_mut(index)
            .ok_or(EvaluationError::Integrity)?;
        *slot = Some(result.clone());
        self.visited.push(index);
        result
    }

    fn evaluate_kind(
        &mut self,
        kind: ExpressionKind,
        values: &(impl ExpressionValueSource + ?Sized),
    ) -> Result<Rc<CanonicalValue>, EvaluationError> {
        let value = match kind {
            ExpressionKind::Constant(value) => Some(value),
            ExpressionKind::InputField(field) => values.input_field(field),
            ExpressionKind::ServiceValue(field) => values.service_value(field),
            ExpressionKind::CollectionElement => values.collection_element(),
            ExpressionKind::CollectionElementField(field) => values.collection_element_field(field),
            ExpressionKind::CompleteBinding(binding) => values.complete_binding(binding),
            ExpressionKind::BoundField { binding, field } => values.bound_field(binding, field),
            ExpressionKind::SchemaField { entity_type, field } => {
                values.schema_field(entity_type, field)
            }
            ExpressionKind::RootValidationField { read, field } => {
                values.root_validation_field(read, field)
            }
            ExpressionKind::SourceEventField(field) => values.source_event_field(field),
            ExpressionKind::TransactionTime => values
                .transaction_time()
                .map(LogicalTime::timestamp)
                .map(CanonicalValue::Timestamp),
            ExpressionKind::TransactionDate => values.transaction_date().map(CanonicalValue::Date),
            ExpressionKind::Unary { operator, operand } => Some(evaluate_unary(
                operator,
                self.evaluate_shared(operand, values)?.as_ref(),
            )?),
            ExpressionKind::Binary {
                operator,
                left,
                right,
            } => Some(self.evaluate_binary(operator, left, right, values)?),
        };
        value.map(Rc::new).ok_or(EvaluationError::Integrity)
    }

    fn evaluate_binary(
        &mut self,
        operator: BinaryOperator,
        left: ExprId,
        right: ExprId,
        values: &(impl ExpressionValueSource + ?Sized),
    ) -> Result<CanonicalValue, EvaluationError> {
        let left = self.evaluate_shared(left, values)?;
        match (operator, left.as_ref()) {
            (BinaryOperator::And, CanonicalValue::Bool(false)) => {
                return Ok(CanonicalValue::Bool(false));
            }
            (BinaryOperator::Or, CanonicalValue::Bool(true)) => {
                return Ok(CanonicalValue::Bool(true));
            }
            (BinaryOperator::And | BinaryOperator::Or, CanonicalValue::Bool(_)) => {}
            (BinaryOperator::And | BinaryOperator::Or, _) => {
                return Err(EvaluationError::Integrity);
            }
            _ => {}
        }
        let right = self.evaluate_shared(right, values)?;
        evaluate_binary(operator, left.as_ref(), right.as_ref())
    }

    fn clear_visited(&mut self) {
        for index in self.visited.drain(..) {
            self.cache[index] = None;
        }
    }
}

/// One memoized expression batch bound to an immutable value source.
pub struct EvaluationBatch<'evaluation, 'arena, 'values, Values: ?Sized> {
    evaluator: &'evaluation mut ExpressionEvaluator<'arena>,
    values: &'values Values,
}

impl<Values: ExpressionValueSource + ?Sized> EvaluationBatch<'_, '_, '_, Values> {
    /// Evaluates one expression, reusing values already visited by this batch.
    pub fn evaluate(&mut self, expression: ExprId) -> Result<CanonicalValue, EvaluationError> {
        self.evaluator
            .evaluate_shared(expression, self.values)
            .map(|value| value.as_ref().clone())
    }

    /// Evaluates one expression and requires a Boolean result.
    pub fn evaluate_predicate(&mut self, expression: ExprId) -> Result<bool, EvaluationError> {
        match self
            .evaluator
            .evaluate_shared(expression, self.values)?
            .as_ref()
        {
            CanonicalValue::Bool(value) => Ok(*value),
            _ => Err(EvaluationError::Integrity),
        }
    }
}

impl<Values: ?Sized> Drop for EvaluationBatch<'_, '_, '_, Values> {
    fn drop(&mut self) {
        self.evaluator.clear_visited();
    }
}

/// Evaluates one expression and requires a Boolean result.
pub fn evaluate_predicate(
    arena: &ExpressionArena,
    expression: ExprId,
    values: &impl ExpressionValueSource,
) -> Result<bool, EvaluationError> {
    let mut evaluator = ExpressionEvaluator::new(arena);
    evaluator.batch(values).evaluate_predicate(expression)
}

/// Evaluates exact commit checks in their already-validated canonical order.
pub fn evaluate_commit_checks(
    arena: &ExpressionArena,
    checks: &[CommitCheckPlan],
    values: &impl ExpressionValueSource,
) -> Result<CommitCheckResult, EvaluationError> {
    let mut evaluator = ExpressionEvaluator::new(arena);
    let mut batch = evaluator.batch(values);
    for check in checks {
        if !batch.evaluate_predicate(check.predicate())? {
            return Ok(CommitCheckResult::Rejected {
                invariant_id: check.invariant_id(),
            });
        }
    }
    Ok(CommitCheckResult::Satisfied)
}

fn evaluate_unary(
    operator: UnaryOperator,
    operand: &CanonicalValue,
) -> Result<CanonicalValue, EvaluationError> {
    match (operator, operand) {
        (UnaryOperator::Not, CanonicalValue::Bool(value)) => Ok(CanonicalValue::Bool(!value)),
        (UnaryOperator::Negate, CanonicalValue::I64(value)) => value
            .checked_neg()
            .map(CanonicalValue::I64)
            .ok_or(EvaluationError::Arithmetic),
        (UnaryOperator::Negate, CanonicalValue::Decimal(value)) => {
            let coefficient = value
                .coefficient()
                .checked_neg()
                .ok_or(EvaluationError::Arithmetic)?;
            riffdb_types::Decimal::new(value.spec(), coefficient)
                .map(CanonicalValue::Decimal)
                .map_err(|_| EvaluationError::Arithmetic)
        }
        (UnaryOperator::Negate, CanonicalValue::Money(value)) => {
            let amount = value.amount();
            let coefficient = amount
                .coefficient()
                .checked_neg()
                .ok_or(EvaluationError::Arithmetic)?;
            let amount = riffdb_types::Decimal::new(amount.spec(), coefficient)
                .map_err(|_| EvaluationError::Arithmetic)?;
            Ok(CanonicalValue::Money(riffdb_types::Money::new(
                value.currency(),
                amount,
            )))
        }
        _ => Err(EvaluationError::Integrity),
    }
}

fn evaluate_binary(
    operator: BinaryOperator,
    left: &CanonicalValue,
    right: &CanonicalValue,
) -> Result<CanonicalValue, EvaluationError> {
    match operator {
        BinaryOperator::Multiply => multiply(left, right),
        BinaryOperator::Divide => divide(left, right),
        BinaryOperator::Add => add(left, right),
        BinaryOperator::Subtract => subtract(left, right),
        BinaryOperator::Equal => Ok(CanonicalValue::Bool(left == right)),
        BinaryOperator::NotEqual => Ok(CanonicalValue::Bool(left != right)),
        BinaryOperator::Less => compare(left, right, |ordering| ordering.is_lt()),
        BinaryOperator::LessEqual => compare(left, right, |ordering| ordering.is_le()),
        BinaryOperator::Greater => compare(left, right, |ordering| ordering.is_gt()),
        BinaryOperator::GreaterEqual => compare(left, right, |ordering| ordering.is_ge()),
        BinaryOperator::And => boolean(left, right, |left, right| left && right),
        BinaryOperator::Or => boolean(left, right, |left, right| left || right),
    }
}

fn multiply(
    left: &CanonicalValue,
    right: &CanonicalValue,
) -> Result<CanonicalValue, EvaluationError> {
    match (left, right) {
        (CanonicalValue::I64(left), CanonicalValue::I64(right)) => left
            .checked_mul(*right)
            .map(CanonicalValue::I64)
            .ok_or(EvaluationError::Arithmetic),
        (CanonicalValue::U64(left), CanonicalValue::U64(right)) => left
            .checked_mul(*right)
            .map(CanonicalValue::U64)
            .ok_or(EvaluationError::Arithmetic),
        _ => Err(EvaluationError::Integrity),
    }
}

fn divide(
    left: &CanonicalValue,
    right: &CanonicalValue,
) -> Result<CanonicalValue, EvaluationError> {
    match (left, right) {
        (CanonicalValue::I64(left), CanonicalValue::I64(right)) => left
            .checked_div(*right)
            .map(CanonicalValue::I64)
            .ok_or(EvaluationError::Arithmetic),
        (CanonicalValue::U64(left), CanonicalValue::U64(right)) => left
            .checked_div(*right)
            .map(CanonicalValue::U64)
            .ok_or(EvaluationError::Arithmetic),
        _ => Err(EvaluationError::Integrity),
    }
}

fn add(left: &CanonicalValue, right: &CanonicalValue) -> Result<CanonicalValue, EvaluationError> {
    match (left, right) {
        (CanonicalValue::I64(left), CanonicalValue::I64(right)) => left
            .checked_add(*right)
            .map(CanonicalValue::I64)
            .ok_or(EvaluationError::Arithmetic),
        (CanonicalValue::U64(left), CanonicalValue::U64(right)) => left
            .checked_add(*right)
            .map(CanonicalValue::U64)
            .ok_or(EvaluationError::Arithmetic),
        (CanonicalValue::Decimal(left), CanonicalValue::Decimal(right)) => left
            .checked_add(*right)
            .map(CanonicalValue::Decimal)
            .map_err(|_| EvaluationError::Arithmetic),
        (CanonicalValue::Money(left), CanonicalValue::Money(right)) => left
            .checked_add(*right)
            .map(CanonicalValue::Money)
            .map_err(|_| EvaluationError::Arithmetic),
        _ => Err(EvaluationError::Integrity),
    }
}

fn subtract(
    left: &CanonicalValue,
    right: &CanonicalValue,
) -> Result<CanonicalValue, EvaluationError> {
    match (left, right) {
        (CanonicalValue::I64(left), CanonicalValue::I64(right)) => left
            .checked_sub(*right)
            .map(CanonicalValue::I64)
            .ok_or(EvaluationError::Arithmetic),
        (CanonicalValue::U64(left), CanonicalValue::U64(right)) => left
            .checked_sub(*right)
            .map(CanonicalValue::U64)
            .ok_or(EvaluationError::Arithmetic),
        (CanonicalValue::Decimal(left), CanonicalValue::Decimal(right)) => left
            .checked_sub(*right)
            .map(CanonicalValue::Decimal)
            .map_err(|_| EvaluationError::Arithmetic),
        (CanonicalValue::Money(left), CanonicalValue::Money(right)) => left
            .checked_sub(*right)
            .map(CanonicalValue::Money)
            .map_err(|_| EvaluationError::Arithmetic),
        _ => Err(EvaluationError::Integrity),
    }
}

fn compare(
    left: &CanonicalValue,
    right: &CanonicalValue,
    predicate: impl FnOnce(Ordering) -> bool,
) -> Result<CanonicalValue, EvaluationError> {
    let ordering = match (left, right) {
        (CanonicalValue::I64(left), CanonicalValue::I64(right)) => left.cmp(right),
        (CanonicalValue::U64(left), CanonicalValue::U64(right)) => left.cmp(right),
        (CanonicalValue::Decimal(left), CanonicalValue::Decimal(right)) => left
            .checked_cmp(*right)
            .map_err(|_| EvaluationError::Integrity)?,
        (CanonicalValue::Money(left), CanonicalValue::Money(right)) => left
            .checked_cmp(*right)
            .map_err(|_| EvaluationError::Integrity)?,
        (CanonicalValue::Timestamp(left), CanonicalValue::Timestamp(right)) => left.cmp(right),
        (CanonicalValue::Date(left), CanonicalValue::Date(right)) => left.cmp(right),
        _ => return Err(EvaluationError::Integrity),
    };
    Ok(CanonicalValue::Bool(predicate(ordering)))
}

fn boolean(
    left: &CanonicalValue,
    right: &CanonicalValue,
    operator: impl FnOnce(bool, bool) -> bool,
) -> Result<CanonicalValue, EvaluationError> {
    match (left, right) {
        (CanonicalValue::Bool(left), CanonicalValue::Bool(right)) => {
            Ok(CanonicalValue::Bool(operator(*left, *right)))
        }
        _ => Err(EvaluationError::Integrity),
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::collections::BTreeMap;

    use riffdb_contract_ir::{ExpressionKind, ValueType};
    use riffdb_types::{
        CanonicalString, CurrencyCode, Decimal, DecimalSpec, FieldId, MAX_DECIMAL_PRECISION, Money,
        Timestamp,
    };

    use super::*;

    #[derive(Default)]
    struct Values {
        inputs: BTreeMap<FieldId, CanonicalValue>,
        service_values: BTreeMap<FieldId, CanonicalValue>,
        logical_time: Option<LogicalTime>,
    }

    impl ExpressionValueSource for Values {
        fn input_field(&self, field: FieldId) -> Option<CanonicalValue> {
            self.inputs.get(&field).cloned()
        }

        fn service_value(&self, field: FieldId) -> Option<CanonicalValue> {
            self.service_values.get(&field).cloned()
        }

        fn transaction_time(&self) -> Option<LogicalTime> {
            self.logical_time
        }
    }

    fn binary_arena(
        left: CanonicalValue,
        right: CanonicalValue,
        value_type: ValueType,
        operator: BinaryOperator,
        result_type: ValueType,
    ) -> ExpressionArena {
        ExpressionArena::new(vec![
            (ExpressionKind::Constant(left), value_type.clone()),
            (ExpressionKind::Constant(right), value_type),
            (
                ExpressionKind::Binary {
                    operator,
                    left: ExprId::new(0),
                    right: ExprId::new(1),
                },
                result_type,
            ),
        ])
        .expect("valid expression arena")
    }

    #[test]
    fn service_values_resolve_through_the_source_and_fail_closed_when_absent() {
        let field = FieldId::first();
        let arena = ExpressionArena::new(vec![(
            ExpressionKind::ServiceValue(field),
            ValueType::bool(),
        )])
        .expect("single service-value expression");
        assert_eq!(
            evaluate_expression(&arena, ExprId::new(0), &Values::default()),
            Err(EvaluationError::Integrity)
        );
        let values = Values {
            service_values: BTreeMap::from([(field, CanonicalValue::Bool(true))]),
            ..Values::default()
        };
        assert_eq!(
            evaluate_expression(&arena, ExprId::new(0), &values),
            Ok(CanonicalValue::Bool(true))
        );
    }

    #[test]
    fn integer_arithmetic_is_checked_at_every_boundary() {
        for (left, right, operator) in [
            (i64::MAX, 1, BinaryOperator::Add),
            (i64::MIN, 1, BinaryOperator::Subtract),
            (i64::MAX, 2, BinaryOperator::Multiply),
            (1, 0, BinaryOperator::Divide),
            (i64::MIN, -1, BinaryOperator::Divide),
        ] {
            let arena = binary_arena(
                CanonicalValue::I64(left),
                CanonicalValue::I64(right),
                ValueType::i64(),
                operator,
                ValueType::i64(),
            );
            assert_eq!(
                evaluate_expression(&arena, ExprId::new(2), &Values::default()),
                Err(EvaluationError::Arithmetic)
            );
        }

        for (left, right, operator) in [
            (0, 1, BinaryOperator::Subtract),
            (u64::MAX, 1, BinaryOperator::Add),
            (u64::MAX, 2, BinaryOperator::Multiply),
            (1, 0, BinaryOperator::Divide),
        ] {
            let arena = binary_arena(
                CanonicalValue::U64(left),
                CanonicalValue::U64(right),
                ValueType::u64(),
                operator,
                ValueType::u64(),
            );
            assert_eq!(
                evaluate_expression(&arena, ExprId::new(2), &Values::default()),
                Err(EvaluationError::Arithmetic)
            );
        }
    }

    #[test]
    fn decimal_and_money_precision_overflow_is_arithmetic() {
        let decimal_spec = DecimalSpec::new(3, 2).expect("decimal spec");
        for (left, right, operator) in [
            (999, 1, BinaryOperator::Add),
            (-999, 1, BinaryOperator::Subtract),
        ] {
            let decimal = binary_arena(
                CanonicalValue::Decimal(Decimal::new(decimal_spec, left).expect("decimal")),
                CanonicalValue::Decimal(Decimal::new(decimal_spec, right).expect("decimal")),
                ValueType::decimal(decimal_spec),
                operator,
                ValueType::decimal(decimal_spec),
            );
            assert_eq!(
                evaluate_expression(&decimal, ExprId::new(2), &Values::default()),
                Err(EvaluationError::Arithmetic)
            );
        }

        let money_spec = DecimalSpec::new(MAX_DECIMAL_PRECISION, 2).expect("money spec");
        let currency = CurrencyCode::new("USD").expect("currency");
        let maximum = 10_i128.pow(u32::from(MAX_DECIMAL_PRECISION)) - 1;
        for (left, right, operator) in [
            (maximum, 1, BinaryOperator::Add),
            (-maximum, 1, BinaryOperator::Subtract),
        ] {
            let money = binary_arena(
                CanonicalValue::Money(Money::new(
                    currency,
                    Decimal::new(money_spec, left).expect("money amount"),
                )),
                CanonicalValue::Money(Money::new(
                    currency,
                    Decimal::new(money_spec, right).expect("money amount"),
                )),
                ValueType::money(currency),
                operator,
                ValueType::money(currency),
            );
            assert_eq!(
                evaluate_expression(&money, ExprId::new(2), &Values::default()),
                Err(EvaluationError::Arithmetic)
            );
        }
    }

    #[test]
    fn unary_negation_is_checked() {
        let arena = ExpressionArena::new(vec![
            (
                ExpressionKind::Constant(CanonicalValue::I64(i64::MIN)),
                ValueType::i64(),
            ),
            (
                ExpressionKind::Unary {
                    operator: UnaryOperator::Negate,
                    operand: ExprId::new(0),
                },
                ValueType::i64(),
            ),
        ])
        .expect("valid arena");
        assert_eq!(
            evaluate_expression(&arena, ExprId::new(1), &Values::default()),
            Err(EvaluationError::Arithmetic)
        );

        let spec = DecimalSpec::new(MAX_DECIMAL_PRECISION, 2).expect("decimal spec");
        let magnitude = 10_i128.pow(u32::from(MAX_DECIMAL_PRECISION)) - 1;
        for (value, value_type, expected) in [
            (
                CanonicalValue::Decimal(Decimal::new(spec, -magnitude).expect("minimum decimal")),
                ValueType::decimal(spec),
                CanonicalValue::Decimal(Decimal::new(spec, magnitude).expect("maximum decimal")),
            ),
            (
                CanonicalValue::Money(Money::new(
                    CurrencyCode::new("USD").expect("currency"),
                    Decimal::new(spec, -magnitude).expect("minimum money"),
                )),
                ValueType::money(CurrencyCode::new("USD").expect("currency")),
                CanonicalValue::Money(Money::new(
                    CurrencyCode::new("USD").expect("currency"),
                    Decimal::new(spec, magnitude).expect("maximum money"),
                )),
            ),
        ] {
            let arena = ExpressionArena::new(vec![
                (ExpressionKind::Constant(value), value_type.clone()),
                (
                    ExpressionKind::Unary {
                        operator: UnaryOperator::Negate,
                        operand: ExprId::new(0),
                    },
                    value_type,
                ),
            ])
            .expect("valid bounded negation");
            assert_eq!(
                evaluate_expression(&arena, ExprId::new(1), &Values::default()),
                Ok(expected)
            );
        }
    }

    #[test]
    fn boolean_operators_short_circuit_left_to_right() {
        let missing = FieldId::first();
        let arena = ExpressionArena::new(vec![
            (
                ExpressionKind::Constant(CanonicalValue::Bool(false)),
                ValueType::bool(),
            ),
            (ExpressionKind::InputField(missing), ValueType::bool()),
            (
                ExpressionKind::Binary {
                    operator: BinaryOperator::And,
                    left: ExprId::new(0),
                    right: ExprId::new(1),
                },
                ValueType::bool(),
            ),
        ])
        .expect("valid arena");
        assert_eq!(
            evaluate_expression(&arena, ExprId::new(2), &Values::default()),
            Ok(CanonicalValue::Bool(false))
        );
    }

    #[test]
    fn depth_32_shared_dag_evaluates_each_shared_node_once() {
        struct CountedValue {
            reads: Cell<usize>,
        }

        impl ExpressionValueSource for CountedValue {
            fn input_field(&self, _field: FieldId) -> Option<CanonicalValue> {
                self.reads.set(self.reads.get() + 1);
                Some(CanonicalValue::Bool(true))
            }
        }

        let mut nodes = vec![(
            ExpressionKind::InputField(FieldId::first()),
            ValueType::bool(),
        )];
        for index in 1..riffdb_contract_ir::MAX_EXPRESSION_NESTING {
            nodes.push((
                ExpressionKind::Binary {
                    operator: BinaryOperator::And,
                    left: ExprId::new((index - 1) as u32),
                    right: ExprId::new((index - 1) as u32),
                },
                ValueType::bool(),
            ));
        }
        let arena = ExpressionArena::new(nodes).expect("maximum-depth shared DAG");
        let values = CountedValue {
            reads: Cell::new(0),
        };

        assert_eq!(
            evaluate_expression(
                &arena,
                ExprId::new((riffdb_contract_ir::MAX_EXPRESSION_NESTING - 1) as u32),
                &values,
            ),
            Ok(CanonicalValue::Bool(true))
        );
        assert_eq!(values.reads.get(), 1);
    }

    #[test]
    fn memoized_large_values_are_pointer_shared_within_one_batch() {
        let text = CanonicalValue::String(
            CanonicalString::new("x".repeat(64 * 1024)).expect("bounded string"),
        );
        let arena = ExpressionArena::new(vec![(
            ExpressionKind::Constant(text),
            ValueType::string(64 * 1024).expect("string type"),
        )])
        .expect("single large expression");
        let mut evaluator = ExpressionEvaluator::new(&arena);
        let values = Values::default();

        let first = evaluator
            .evaluate_shared(ExprId::new(0), &values)
            .expect("first evaluation");
        let second = evaluator
            .evaluate_shared(ExprId::new(0), &values)
            .expect("memoized evaluation");

        assert!(Rc::ptr_eq(&first, &second));
        assert_eq!(evaluator.visited, vec![0]);
    }

    #[test]
    fn evaluator_reuses_one_arena_cache_and_batches_clear_only_visited_slots() {
        let field = FieldId::first();
        let arena =
            ExpressionArena::new(vec![(ExpressionKind::InputField(field), ValueType::bool())])
                .expect("single input expression");
        let mut evaluator = ExpressionEvaluator::new(&arena);
        let cache_allocation = evaluator.cache.as_ptr();

        for expected in [true, false] {
            let values = Values {
                inputs: BTreeMap::from([(field, CanonicalValue::Bool(expected))]),
                ..Values::default()
            };
            assert_eq!(
                evaluator.batch(&values).evaluate_predicate(ExprId::new(0)),
                Ok(expected)
            );
            assert!(evaluator.visited.is_empty());
            assert!(evaluator.cache.iter().all(Option::is_none));
            assert_eq!(evaluator.cache.as_ptr(), cache_allocation);
        }
    }

    #[test]
    fn forgotten_batch_cannot_leak_cached_values_into_the_next_source() {
        let field = FieldId::first();
        let arena =
            ExpressionArena::new(vec![(ExpressionKind::InputField(field), ValueType::bool())])
                .expect("single input expression");
        let mut evaluator = ExpressionEvaluator::new(&arena);
        let first_values = Values {
            inputs: BTreeMap::from([(field, CanonicalValue::Bool(true))]),
            ..Values::default()
        };
        let mut forgotten = evaluator.batch(&first_values);
        assert_eq!(forgotten.evaluate_predicate(ExprId::new(0)), Ok(true));
        std::mem::forget(forgotten);

        let second_values = Values {
            inputs: BTreeMap::from([(field, CanonicalValue::Bool(false))]),
            ..Values::default()
        };
        assert_eq!(
            evaluator
                .batch(&second_values)
                .evaluate_predicate(ExprId::new(0)),
            Ok(false)
        );
    }

    #[test]
    fn logical_time_is_only_the_supplied_value() {
        let arena = ExpressionArena::new(vec![(
            ExpressionKind::TransactionTime,
            ValueType::timestamp(),
        )])
        .expect("valid arena");
        for timestamp in [
            Timestamp::new(i64::MIN, 0).expect("minimum timestamp"),
            Timestamp::new(0, 999_999_999).expect("maximum nanoseconds"),
            Timestamp::new(i64::MAX, 999_999_999).expect("maximum timestamp"),
        ] {
            let values = Values {
                logical_time: Some(LogicalTime::new(timestamp)),
                ..Values::default()
            };
            assert_eq!(
                evaluate_expression(&arena, ExprId::new(0), &values),
                Ok(CanonicalValue::Timestamp(timestamp))
            );
        }
    }

    #[test]
    fn commit_checks_report_the_first_rejected_invariant() {
        let arena = ExpressionArena::new(vec![(
            ExpressionKind::Constant(CanonicalValue::Bool(false)),
            ValueType::bool(),
        )])
        .expect("valid arena");
        let invariant_id = InvariantId::first();
        let check = CommitCheckPlan::new(
            invariant_id,
            ExprId::new(0),
            vec![BindingId::new(0)],
            vec![],
        )
        .expect("valid commit check");
        assert_eq!(
            evaluate_commit_checks(&arena, &[check], &Values::default()),
            Ok(CommitCheckResult::Rejected { invariant_id })
        );
    }
}
