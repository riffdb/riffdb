//! Allocation-free sizing of the existing expression evaluator's source path.
//! This classifies allocations; it never evaluates expressions or grants proof.
use super::{EvaluationCacheCell, ExpressionArena, ExpressionKind};
use riffdb_contract_ir::{BinaryOperator, ExprId, UnaryOperator};
use riffdb_types::{
    CanonicalRecord, CanonicalValue, OwnedCapacityInspectionErrorV1,
    canonical_value_clone_work_bound_v1, owned_allocation_capacity_bound_v1,
};
use std::{
    alloc::Layout,
    mem::{align_of, size_of},
};

/// Fixed cache and retained visited-vector growth for one unchanged evaluator.
#[doc(hidden)]
pub fn source_expression_control_work_bound_v1(
    arena: &ExpressionArena,
) -> Result<u64, OwnedCapacityInspectionErrorV1> {
    let cache = allocation::<EvaluationCacheCell>(arena.len())?;
    cache
        .checked_add(growing_vector::<usize>(arena.len())?)
        .ok_or(OwnedCapacityInspectionErrorV1)
}

/// One event's complete possible Rc/cache value graphs, before any evaluation.
/// Original normalized event ownership is charged separately by catalog.
#[doc(hidden)]
pub fn source_expression_values_work_bound_v1(
    arena: &ExpressionArena,
    source: &CanonicalRecord,
) -> Result<u64, OwnedCapacityInspectionErrorV1> {
    let (rc_layout, _) = Layout::new::<[usize; 2]>()
        .extend(Layout::new::<CanonicalValue>())
        .map_err(|_| OwnedCapacityInspectionErrorV1)?;
    let rc_layout = rc_layout.pad_to_align();
    let rc = owned_allocation_capacity_bound_v1(rc_layout.size(), rc_layout.align())?;
    let mut bytes = 0_u64;
    for node in arena.nodes() {
        let cloned = value_clone(node.kind(), source)?;
        bytes = bytes
            .checked_add(rc)
            .and_then(|v| v.checked_add(cloned))
            .ok_or(OwnedCapacityInspectionErrorV1)?;
    }
    Ok(bytes)
}

/// Additional clone performed by EvaluationBatch::evaluate at a root expression.
#[doc(hidden)]
pub fn source_expression_result_clone_work_bound_v1(
    arena: &ExpressionArena,
    expression: ExprId,
    source: &CanonicalRecord,
) -> Result<u64, OwnedCapacityInspectionErrorV1> {
    value_clone(
        arena
            .get(expression)
            .ok_or(OwnedCapacityInspectionErrorV1)?
            .kind(),
        source,
    )
}

fn value_clone(
    kind: &ExpressionKind,
    source: &CanonicalRecord,
) -> Result<u64, OwnedCapacityInspectionErrorV1> {
    match kind {
        ExpressionKind::Constant(value) => canonical_value_clone_work_bound_v1(value),
        ExpressionKind::SourceEventField(field) => source
            .fields()
            .binary_search_by_key(field, |(id, _)| *id)
            .ok()
            .map_or(Ok(0), |index| {
                canonical_value_clone_work_bound_v1(&source.fields()[index].1)
            }),
        // Existing closed unary/binary operations and transaction values return
        // inline scalars. Their recursively visited operands are counted above.
        ExpressionKind::Unary { operator, .. } => match operator {
            UnaryOperator::Not
            | UnaryOperator::Negate
            | UnaryOperator::TemporalHour
            | UnaryOperator::TemporalDay
            | UnaryOperator::TemporalWeek
            | UnaryOperator::TemporalMonth
            | UnaryOperator::TemporalYear => Ok(0),
        },
        ExpressionKind::Binary { operator, .. } => match operator {
            BinaryOperator::Multiply
            | BinaryOperator::Divide
            | BinaryOperator::Add
            | BinaryOperator::Subtract
            | BinaryOperator::Equal
            | BinaryOperator::NotEqual
            | BinaryOperator::Less
            | BinaryOperator::LessEqual
            | BinaryOperator::Greater
            | BinaryOperator::GreaterEqual
            | BinaryOperator::And
            | BinaryOperator::Or
            | BinaryOperator::Coalesce => Ok(0),
        },
        ExpressionKind::TransactionTime | ExpressionKind::TransactionDate => Ok(0),
        ExpressionKind::InputField(_)
        | ExpressionKind::ServiceValue(_)
        | ExpressionKind::CollectionElement
        | ExpressionKind::CollectionElementField(_)
        | ExpressionKind::CompleteBinding(_)
        | ExpressionKind::BoundField { .. }
        | ExpressionKind::SchemaField { .. }
        | ExpressionKind::RootValidationField { .. } => Err(OwnedCapacityInspectionErrorV1),
    }
}

fn allocation<T>(count: usize) -> Result<u64, OwnedCapacityInspectionErrorV1> {
    owned_allocation_capacity_bound_v1(
        count
            .checked_mul(size_of::<T>())
            .ok_or(OwnedCapacityInspectionErrorV1)?,
        align_of::<T>(),
    )
}
fn growing_vector<T>(count: usize) -> Result<u64, OwnedCapacityInspectionErrorV1> {
    if count == 0 {
        return Ok(0);
    }
    // Existing Vec::push starts at four non-ZST cells, then doubles. Retain the
    // final capacity and one old allocation during growth; only one thread runs.
    let capacity = count
        .max(4)
        .checked_next_power_of_two()
        .ok_or(OwnedCapacityInspectionErrorV1)?;
    allocation::<T>(capacity)?
        .checked_add(allocation::<T>(capacity / 2)?)
        .ok_or(OwnedCapacityInspectionErrorV1)
}

#[cfg(test)]
mod temporal_capacity_tests {
    use super::*;

    // req: PRJ-004, STO-020
    #[test]
    fn source_temporal_clone_classification_matches_actual_inline_bucket_results() {
        let source = CanonicalRecord::new(Vec::new()).unwrap();
        let value = CanonicalValue::Timestamp(riffdb_types::Timestamp::new(1_234_567, 0).unwrap());
        for (operator, seconds) in [
            (UnaryOperator::TemporalHour, 1_231_200),
            (UnaryOperator::TemporalDay, 1_209_600),
            (UnaryOperator::TemporalWeek, 950_400),
            (UnaryOperator::TemporalMonth, 0),
            (UnaryOperator::TemporalYear, 0),
        ] {
            let actual = crate::evaluate_unary(operator, &value).unwrap();
            assert_eq!(
                actual,
                CanonicalValue::Timestamp(riffdb_types::Timestamp::new(seconds, 0).unwrap())
            );
            let kind = ExpressionKind::Unary {
                operator,
                operand: ExprId::new(0),
            };
            assert_eq!(
                value_clone(&kind, &source).unwrap(),
                canonical_value_clone_work_bound_v1(&actual).unwrap()
            );
        }
        for (operator, day) in [
            (UnaryOperator::TemporalDay, 15),
            (UnaryOperator::TemporalWeek, 11),
            (UnaryOperator::TemporalMonth, 0),
            (UnaryOperator::TemporalYear, 0),
        ] {
            let actual =
                crate::evaluate_unary(operator, &CanonicalValue::Date(riffdb_types::Date::new(15)))
                    .unwrap();
            assert_eq!(actual, CanonicalValue::Date(riffdb_types::Date::new(day)));
            let kind = ExpressionKind::Unary {
                operator,
                operand: ExprId::new(0),
            };
            assert_eq!(
                value_clone(&kind, &source).unwrap(),
                canonical_value_clone_work_bound_v1(&actual).unwrap()
            );
        }
    }
}
