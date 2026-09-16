//! Streaming state for the scan executor's existing exact aggregate semantics.
use super::*;

pub(super) struct Accumulator {
    semantic: AggregateSemanticIdentityV1,
    index: Option<usize>,
    count: u64,
    total: i128,
    scalar: Option<CanonicalValue>,
    boolean: bool,
    distinct: BTreeSet<Vec<u8>>,
    scratch: Vec<u8>,
}

impl Accumulator {
    pub(super) fn new(
        definition: &RegisteredDefinition,
        op: &AggregateOp,
    ) -> Result<Self, QueryError> {
        let semantic = op.semantic_identity();
        let index = if semantic == AggregateSemanticIdentityV1::Count {
            None
        } else {
            Some(projected_field_index(
                definition,
                aggregate_input_field(op)?,
            )?)
        };
        Ok(Self {
            semantic,
            index,
            count: 0,
            total: 0,
            scalar: None,
            boolean: semantic == AggregateSemanticIdentityV1::All,
            distinct: BTreeSet::new(),
            scratch: Vec::new(),
        })
    }

    pub(super) fn push(
        &mut self,
        row: &MergedRow,
        fuel: &mut ColumnarAggregateFuel,
    ) -> Result<(), QueryError> {
        fuel.consume_operations(1)?;
        let value = self.index.map(|index| &row.cells[index]);
        use AggregateSemanticIdentityV1 as S;
        match self.semantic {
            S::Count | S::CountPresent => {
                if self.semantic == S::Count || value != Some(&CanonicalValue::Null) {
                    self.count = self
                        .count
                        .checked_add(1)
                        .ok_or(QueryError::InvalidAggregate("count overflow"))?;
                }
            }
            S::Sum | S::Mean => {
                self.total = self
                    .total
                    .checked_add(numeric_as_i128(value.expect("resolved input"))?)
                    .ok_or(QueryError::InvalidAggregate(if self.semantic == S::Sum {
                        "sum overflow"
                    } else {
                        "mean total overflow"
                    }))?;
                if self.semantic == S::Mean {
                    self.count = self
                        .count
                        .checked_add(1)
                        .ok_or(QueryError::InvalidAggregate("mean count overflow"))?;
                }
            }
            S::Min | S::Max => {
                let value = value.expect("resolved input");
                let preferred = if self.semantic == S::Min {
                    Ordering::Less
                } else {
                    Ordering::Greater
                };
                if self
                    .scalar
                    .as_ref()
                    .is_none_or(|current| compare_values(value, current) == preferred)
                {
                    self.scalar = Some(value.clone());
                }
            }
            S::Any | S::All => {
                let CanonicalValue::Bool(value) = value.expect("resolved input") else {
                    return Err(QueryError::InvalidAggregate("any/all requires bool"));
                };
                if self.semantic == S::Any {
                    self.boolean |= value;
                } else {
                    self.boolean &= value;
                }
            }
            S::CountDistinct | S::CountDistinctPresent => {
                let value = value.expect("resolved input");
                if self.semantic == S::CountDistinctPresent && matches!(value, CanonicalValue::Null)
                {
                    return Ok(());
                }
                self.scratch.clear();
                riffdb_types::encode_canonical_value_into(&mut self.scratch, value)
                    .map_err(|_| QueryError::InvalidAggregate("distinct encoding"))?;
                if !self.distinct.contains(&self.scratch) {
                    if self.distinct.len() >= usize::from(MAX_AGGREGATE_DISTINCT_VALUES_V1) {
                        return Err(QueryError::AggregateBudgetExceeded {
                            resource: "distinct values",
                            max: usize::from(MAX_AGGREGATE_DISTINCT_VALUES_V1),
                        });
                    }
                    fuel.consume_state(self.scratch.len().checked_add(32).ok_or(
                        QueryError::AggregateBudgetExceeded {
                            resource: "state bytes",
                            max: MAX_AGGREGATE_STATE_BYTES_V1 as usize,
                        },
                    )?)?;
                    self.distinct.insert(std::mem::take(&mut self.scratch));
                }
            }
            S::ExactCount => {
                return Err(QueryError::InvalidAggregate(
                    "unsupported aggregate semantic",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn finish(
        self,
        fuel: &mut ColumnarAggregateFuel,
    ) -> Result<AggregateValue, QueryError> {
        use AggregateSemanticIdentityV1 as S;
        Ok(match self.semantic {
            S::Count | S::CountPresent => {
                fuel.consume_state(std::mem::size_of::<u64>())?;
                AggregateValue::Count(self.count)
            }
            S::Sum => {
                fuel.consume_state(std::mem::size_of::<i128>())?;
                AggregateValue::Sum(self.total)
            }
            S::Mean => {
                fuel.consume_state(std::mem::size_of::<i128>() + std::mem::size_of::<u64>())?;
                AggregateValue::ExactMean {
                    total: self.total,
                    count: self.count,
                }
            }
            S::Min | S::Max => {
                if let Some(value) = self.scalar.as_ref() {
                    fuel.consume_canonical_state(value)?;
                }
                AggregateValue::Scalar(self.scalar)
            }
            S::Any | S::All => {
                fuel.consume_state(std::mem::size_of::<bool>())?;
                AggregateValue::Bool(self.boolean)
            }
            S::CountDistinct | S::CountDistinctPresent => AggregateValue::Count(
                u64::try_from(self.distinct.len())
                    .map_err(|_| QueryError::InvalidAggregate("count overflow"))?,
            ),
            S::ExactCount => {
                return Err(QueryError::InvalidAggregate(
                    "unsupported aggregate semantic",
                ));
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fold(semantic: AggregateSemanticIdentityV1, values: &[CanonicalValue]) -> AggregateValue {
        let mut state = Accumulator {
            semantic,
            index: Some(0),
            count: 0,
            total: 0,
            scalar: None,
            boolean: semantic == AggregateSemanticIdentityV1::All,
            distinct: BTreeSet::new(),
            scratch: Vec::new(),
        };
        let mut fuel = ColumnarAggregateFuel::new();
        for value in values {
            state
                .push(
                    &MergedRow {
                        entity_version: EntityVersion::new(1).unwrap(),
                        cells: vec![value.clone()],
                    },
                    &mut fuel,
                )
                .unwrap();
        }
        state.finish(&mut fuel).unwrap()
    }

    // req: OQ-045, OQ-046, OQ-047, OQ-048
    #[test]
    fn streaming_folds_preserve_null_empty_typed_distinct_and_exact_mean_semantics() {
        use AggregateSemanticIdentityV1 as S;
        let values = [
            CanonicalValue::I64(7),
            CanonicalValue::Null,
            CanonicalValue::I64(3),
        ];
        assert_eq!(
            fold(S::Min, &values),
            AggregateValue::Scalar(Some(CanonicalValue::Null))
        );
        assert_eq!(
            fold(S::Max, &values),
            AggregateValue::Scalar(Some(CanonicalValue::I64(7)))
        );
        for semantic in [S::Min, S::Max] {
            assert_eq!(fold(semantic, &[]), AggregateValue::Scalar(None));
            assert_eq!(
                fold(semantic, &[CanonicalValue::Null]),
                AggregateValue::Scalar(Some(CanonicalValue::Null))
            );
        }
        let distinct = [
            CanonicalValue::Null,
            CanonicalValue::U64(1),
            CanonicalValue::I64(1),
            CanonicalValue::string("1").unwrap(),
            CanonicalValue::U64(1),
            CanonicalValue::Null,
        ];
        assert_eq!(fold(S::CountDistinct, &distinct), AggregateValue::Count(4));
        assert_eq!(
            fold(S::CountDistinctPresent, &distinct),
            AggregateValue::Count(3)
        );
        assert_eq!(fold(S::CountPresent, &distinct), AggregateValue::Count(4));
        assert_eq!(
            fold(S::Mean, &[CanonicalValue::I64(3), CanonicalValue::I64(4)]),
            AggregateValue::ExactMean { total: 7, count: 2 }
        );
        assert_eq!(
            fold(S::Mean, &[]),
            AggregateValue::ExactMean { total: 0, count: 0 }
        );
        assert_eq!(fold(S::Any, &[]), AggregateValue::Bool(false));
        assert_eq!(fold(S::All, &[]), AggregateValue::Bool(true));
    }
}
