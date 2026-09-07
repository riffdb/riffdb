use riffdb_types::{
    AggregateArithmeticV1, AggregateEmptyResultV1, AggregateInputClassV1, AggregateNoValueRuleV1,
    AggregatePartialStateV1, AggregateResultSchemaV1, AggregateSemanticIdentityV1, CanonicalValue,
    DecimalSpec, MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1, MAX_AGGREGATE_STATE_BYTES_V1,
};

use crate::segment_v2::{SegmentV2Cell, SegmentV2LogicalType, SegmentV2SegmentId};

const CANONICAL_PARTIAL_ORDER_BYTES: u32 = 2 + 16 + 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AggregatePartialError {
    UnsupportedSemantic,
    RegistryMismatch,
    InputShape,
    InvalidArithmeticBound,
    ArithmeticBoundExceeded,
    InvalidStateBound,
    StateBoundExceeded,
    MissingField,
    NoValue,
    ArithmeticOverflow,
    IncompatiblePartial,
    NonCanonicalMergeOrder,
    ContributionAfterMerge,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct AggregatePartialBudget {
    remaining_arithmetic_operations: u32,
    remaining_state_bytes: u32,
}

impl AggregatePartialBudget {
    pub(super) const fn new(
        maximum_arithmetic_operations: u32,
        maximum_state_bytes: u32,
    ) -> Result<Self, AggregatePartialError> {
        if maximum_arithmetic_operations == 0
            || maximum_arithmetic_operations > MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1
        {
            return Err(AggregatePartialError::InvalidArithmeticBound);
        }
        if maximum_state_bytes == 0 || maximum_state_bytes > MAX_AGGREGATE_STATE_BYTES_V1 {
            return Err(AggregatePartialError::InvalidStateBound);
        }
        Ok(Self {
            remaining_arithmetic_operations: maximum_arithmetic_operations,
            remaining_state_bytes: maximum_state_bytes,
        })
    }

    pub(super) const fn remaining_arithmetic_operations(self) -> u32 {
        self.remaining_arithmetic_operations
    }

    pub(super) const fn remaining_state_bytes(self) -> u32 {
        self.remaining_state_bytes
    }

    fn admits_operations(&self, operations: usize) -> Result<u32, AggregatePartialError> {
        let operations = u32::try_from(operations)
            .map_err(|_| AggregatePartialError::ArithmeticBoundExceeded)?;
        if operations > self.remaining_arithmetic_operations {
            return Err(AggregatePartialError::ArithmeticBoundExceeded);
        }
        Ok(operations)
    }

    const fn admits_state(&self, bytes: u32) -> Result<(), AggregatePartialError> {
        if bytes > self.remaining_state_bytes {
            return Err(AggregatePartialError::StateBoundExceeded);
        }
        Ok(())
    }

    fn commit_operations(&mut self, operations: u32) {
        self.remaining_arithmetic_operations -= operations;
    }

    fn commit_state(&mut self, bytes: u32) {
        self.remaining_state_bytes -= bytes;
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct CanonicalPartialOrder {
    root_inventory_ordinal: u16,
    segment_id: SegmentV2SegmentId,
    batch_ordinal: u32,
}

impl CanonicalPartialOrder {
    pub(super) const fn new(
        root_inventory_ordinal: u16,
        segment_id: SegmentV2SegmentId,
        batch_ordinal: u32,
    ) -> Self {
        Self {
            root_inventory_ordinal,
            segment_id,
            batch_ordinal,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExactNumericLane {
    I64,
    U64,
    Decimal(DecimalSpec),
}

impl ExactNumericLane {
    fn from_logical_type(value: &SegmentV2LogicalType) -> Result<Self, AggregatePartialError> {
        match value {
            SegmentV2LogicalType::I64 => Ok(Self::I64),
            SegmentV2LogicalType::U64 => Ok(Self::U64),
            SegmentV2LogicalType::Decimal(spec) => Ok(Self::Decimal(*spec)),
            SegmentV2LogicalType::Bool
            | SegmentV2LogicalType::String
            | SegmentV2LogicalType::Bytes
            | SegmentV2LogicalType::Timestamp
            | SegmentV2LogicalType::Date
            | SegmentV2LogicalType::Uuid
            | SegmentV2LogicalType::Enum(_)
            | SegmentV2LogicalType::Money { .. } => Err(AggregatePartialError::InputShape),
        }
    }

    const fn scale(self) -> u8 {
        match self {
            Self::I64 | Self::U64 => 0,
            Self::Decimal(spec) => spec.scale(),
        }
    }

    fn contribution(self, cell: &SegmentV2Cell) -> Result<i128, AggregatePartialError> {
        match (self, cell) {
            (_, SegmentV2Cell::Missing) => Err(AggregatePartialError::MissingField),
            (_, SegmentV2Cell::Null) => Err(AggregatePartialError::NoValue),
            (Self::I64, SegmentV2Cell::Value(CanonicalValue::I64(value))) => Ok(i128::from(*value)),
            (Self::U64, SegmentV2Cell::Value(CanonicalValue::U64(value))) => Ok(i128::from(*value)),
            (Self::Decimal(expected), SegmentV2Cell::Value(CanonicalValue::Decimal(value)))
                if value.spec() == expected =>
            {
                Ok(value.coefficient())
            }
            (Self::I64 | Self::U64 | Self::Decimal(_), SegmentV2Cell::Value(_)) => {
                Err(AggregatePartialError::InputShape)
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ExactAggregatePartialValue {
    Count(u64),
    Sum {
        coefficient: i128,
        scale: u8,
    },
    Mean {
        coefficient: i128,
        scale: u8,
        count: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ExactAggregatePartial {
    semantic: AggregateSemanticIdentityV1,
    numeric_lane: Option<ExactNumericLane>,
    value: ExactAggregatePartialValue,
    last_merge_order: Option<CanonicalPartialOrder>,
}

impl ExactAggregatePartial {
    pub(super) fn new(
        semantic: AggregateSemanticIdentityV1,
        logical_type: Option<&SegmentV2LogicalType>,
        budget: &mut AggregatePartialBudget,
    ) -> Result<Self, AggregatePartialError> {
        Self::validate_registry(semantic)?;
        let (numeric_lane, value) = match semantic {
            AggregateSemanticIdentityV1::Count => {
                if logical_type.is_some() {
                    return Err(AggregatePartialError::InputShape);
                }
                (None, ExactAggregatePartialValue::Count(0))
            }
            AggregateSemanticIdentityV1::Sum | AggregateSemanticIdentityV1::Mean => {
                let numeric_lane = ExactNumericLane::from_logical_type(
                    logical_type.ok_or(AggregatePartialError::InputShape)?,
                )?;
                let value = if semantic == AggregateSemanticIdentityV1::Sum {
                    ExactAggregatePartialValue::Sum {
                        coefficient: 0,
                        scale: numeric_lane.scale(),
                    }
                } else {
                    ExactAggregatePartialValue::Mean {
                        coefficient: 0,
                        scale: numeric_lane.scale(),
                        count: 0,
                    }
                };
                (Some(numeric_lane), value)
            }
            AggregateSemanticIdentityV1::ExactCount
            | AggregateSemanticIdentityV1::Min
            | AggregateSemanticIdentityV1::Max
            | AggregateSemanticIdentityV1::CountPresent
            | AggregateSemanticIdentityV1::CountDistinct
            | AggregateSemanticIdentityV1::CountDistinctPresent
            | AggregateSemanticIdentityV1::Any
            | AggregateSemanticIdentityV1::All => {
                return Err(AggregatePartialError::UnsupportedSemantic);
            }
        };
        let state_bytes = Self::required_state_bytes(semantic)?;
        budget.admits_state(state_bytes)?;
        budget.commit_state(state_bytes);
        Ok(Self {
            semantic,
            numeric_lane,
            value,
            last_merge_order: None,
        })
    }

    fn validate_registry(
        semantic: AggregateSemanticIdentityV1,
    ) -> Result<(), AggregatePartialError> {
        let descriptor = semantic.descriptor();
        let matches = match semantic {
            AggregateSemanticIdentityV1::Count => {
                descriptor.partial_state() == AggregatePartialStateV1::CheckedU64
                    && descriptor.arithmetic() == AggregateArithmeticV1::CheckedU64
                    && descriptor.input_class() == AggregateInputClassV1::NoField
                    && descriptor.no_value_rule() == AggregateNoValueRuleV1::CountsRow
                    && descriptor.empty_result() == AggregateEmptyResultV1::UnsignedZero
                    && descriptor.result_schema() == AggregateResultSchemaV1::U64
            }
            AggregateSemanticIdentityV1::Sum => {
                descriptor.partial_state() == AggregatePartialStateV1::CheckedI128AtInputScale
                    && descriptor.arithmetic() == AggregateArithmeticV1::CheckedI128
                    && descriptor.input_class() == AggregateInputClassV1::ExactNumericField
                    && descriptor.no_value_rule() == AggregateNoValueRuleV1::InvalidInputType
                    && descriptor.empty_result() == AggregateEmptyResultV1::ExactNumericZero
                    && descriptor.result_schema()
                        == AggregateResultSchemaV1::ExactDecimalAtInputScale
            }
            AggregateSemanticIdentityV1::Mean => {
                descriptor.partial_state() == AggregatePartialStateV1::ExactMeanV1
                    && descriptor.arithmetic() == AggregateArithmeticV1::CheckedExactMean
                    && descriptor.input_class() == AggregateInputClassV1::ExactNumericField
                    && descriptor.no_value_rule() == AggregateNoValueRuleV1::InvalidInputType
                    && descriptor.empty_result() == AggregateEmptyResultV1::ExactMeanZero
                    && descriptor.result_schema() == AggregateResultSchemaV1::ExactMeanV1
            }
            AggregateSemanticIdentityV1::ExactCount
            | AggregateSemanticIdentityV1::Min
            | AggregateSemanticIdentityV1::Max
            | AggregateSemanticIdentityV1::CountPresent
            | AggregateSemanticIdentityV1::CountDistinct
            | AggregateSemanticIdentityV1::CountDistinctPresent
            | AggregateSemanticIdentityV1::Any
            | AggregateSemanticIdentityV1::All => {
                return Err(AggregatePartialError::UnsupportedSemantic);
            }
        };
        if matches {
            Ok(())
        } else {
            Err(AggregatePartialError::RegistryMismatch)
        }
    }

    pub(super) fn required_state_bytes(
        semantic: AggregateSemanticIdentityV1,
    ) -> Result<u32, AggregatePartialError> {
        let bytes = match semantic {
            AggregateSemanticIdentityV1::Count => std::mem::size_of::<u64>(),
            AggregateSemanticIdentityV1::Sum => {
                std::mem::size_of::<i128>() + std::mem::size_of::<u8>()
            }
            AggregateSemanticIdentityV1::Mean => {
                std::mem::size_of::<i128>() + std::mem::size_of::<u8>() + std::mem::size_of::<u64>()
            }
            AggregateSemanticIdentityV1::ExactCount
            | AggregateSemanticIdentityV1::Min
            | AggregateSemanticIdentityV1::Max
            | AggregateSemanticIdentityV1::CountPresent
            | AggregateSemanticIdentityV1::CountDistinct
            | AggregateSemanticIdentityV1::CountDistinctPresent
            | AggregateSemanticIdentityV1::Any
            | AggregateSemanticIdentityV1::All => {
                return Err(AggregatePartialError::UnsupportedSemantic);
            }
        };
        u32::try_from(bytes).map_err(|_| AggregatePartialError::StateBoundExceeded)
    }

    pub(super) const fn value(&self) -> ExactAggregatePartialValue {
        self.value
    }

    pub(super) fn accumulate(
        &mut self,
        cells: &[SegmentV2Cell],
        budget: &mut AggregatePartialBudget,
    ) -> Result<(), AggregatePartialError> {
        if self.last_merge_order.is_some() {
            return Err(AggregatePartialError::ContributionAfterMerge);
        }
        let operations = budget.admits_operations(cells.len())?;
        let mut next = self.value;
        match &mut next {
            ExactAggregatePartialValue::Count(count) => {
                let contribution = u64::try_from(cells.len())
                    .map_err(|_| AggregatePartialError::ArithmeticOverflow)?;
                *count = count
                    .checked_add(contribution)
                    .ok_or(AggregatePartialError::ArithmeticOverflow)?;
            }
            ExactAggregatePartialValue::Sum { coefficient, .. } => {
                let lane = self
                    .numeric_lane
                    .ok_or(AggregatePartialError::RegistryMismatch)?;
                for cell in cells {
                    *coefficient = coefficient
                        .checked_add(lane.contribution(cell)?)
                        .ok_or(AggregatePartialError::ArithmeticOverflow)?;
                }
            }
            ExactAggregatePartialValue::Mean {
                coefficient, count, ..
            } => {
                let lane = self
                    .numeric_lane
                    .ok_or(AggregatePartialError::RegistryMismatch)?;
                for cell in cells {
                    *coefficient = coefficient
                        .checked_add(lane.contribution(cell)?)
                        .ok_or(AggregatePartialError::ArithmeticOverflow)?;
                    *count = count
                        .checked_add(1)
                        .ok_or(AggregatePartialError::ArithmeticOverflow)?;
                }
            }
        }
        self.value = next;
        budget.commit_operations(operations);
        Ok(())
    }

    pub(super) fn merge_from(
        &mut self,
        order: CanonicalPartialOrder,
        other: &Self,
        budget: &mut AggregatePartialBudget,
    ) -> Result<(), AggregatePartialError> {
        if self
            .last_merge_order
            .is_some_and(|previous| order <= previous)
        {
            return Err(AggregatePartialError::NonCanonicalMergeOrder);
        }
        if self.semantic != other.semantic || self.numeric_lane != other.numeric_lane {
            return Err(AggregatePartialError::IncompatiblePartial);
        }
        let operations = budget.admits_operations(1)?;
        let order_bytes = if self.last_merge_order.is_none() {
            CANONICAL_PARTIAL_ORDER_BYTES
        } else {
            0
        };
        budget.admits_state(order_bytes)?;

        let next = match (self.value, other.value) {
            (ExactAggregatePartialValue::Count(left), ExactAggregatePartialValue::Count(right)) => {
                ExactAggregatePartialValue::Count(
                    left.checked_add(right)
                        .ok_or(AggregatePartialError::ArithmeticOverflow)?,
                )
            }
            (
                ExactAggregatePartialValue::Sum {
                    coefficient: left,
                    scale,
                },
                ExactAggregatePartialValue::Sum {
                    coefficient: right,
                    scale: right_scale,
                },
            ) if scale == right_scale => ExactAggregatePartialValue::Sum {
                coefficient: left
                    .checked_add(right)
                    .ok_or(AggregatePartialError::ArithmeticOverflow)?,
                scale,
            },
            (
                ExactAggregatePartialValue::Mean {
                    coefficient: left,
                    scale,
                    count,
                },
                ExactAggregatePartialValue::Mean {
                    coefficient: right,
                    scale: right_scale,
                    count: right_count,
                },
            ) if scale == right_scale => ExactAggregatePartialValue::Mean {
                coefficient: left
                    .checked_add(right)
                    .ok_or(AggregatePartialError::ArithmeticOverflow)?,
                scale,
                count: count
                    .checked_add(right_count)
                    .ok_or(AggregatePartialError::ArithmeticOverflow)?,
            },
            (
                ExactAggregatePartialValue::Count(_)
                | ExactAggregatePartialValue::Sum { .. }
                | ExactAggregatePartialValue::Mean { .. },
                ExactAggregatePartialValue::Count(_)
                | ExactAggregatePartialValue::Sum { .. }
                | ExactAggregatePartialValue::Mean { .. },
            ) => return Err(AggregatePartialError::IncompatiblePartial),
        };

        self.value = next;
        self.last_merge_order = Some(order);
        budget.commit_operations(operations);
        budget.commit_state(order_bytes);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment_v2::{SegmentV2Cell, SegmentV2LogicalType, SegmentV2SegmentId};
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use riffdb_types::{
        AggregateArithmeticV1, AggregateEmptyResultV1, AggregateInputClassV1,
        AggregateNoValueRuleV1, AggregatePartialStateV1, AggregateSemanticIdentityV1,
        CanonicalValue, Decimal, DecimalSpec,
    };

    fn budget(operations: u32, state_bytes: u32) -> AggregatePartialBudget {
        AggregatePartialBudget::new(operations, state_bytes).expect("bounded aggregate budget")
    }

    fn cells_i64(values: &[i64]) -> Vec<SegmentV2Cell> {
        values
            .iter()
            .copied()
            .map(|value| SegmentV2Cell::Value(CanonicalValue::I64(value)))
            .collect()
    }

    fn partial_for_values(
        semantic: AggregateSemanticIdentityV1,
        values: &[i64],
    ) -> ExactAggregatePartial {
        let mut charge = budget(
            u32::try_from(values.len().max(1)).expect("small fixture"),
            256,
        );
        let logical_type =
            (semantic != AggregateSemanticIdentityV1::Count).then_some(&SegmentV2LogicalType::I64);
        let mut partial = ExactAggregatePartial::new(semantic, logical_type, &mut charge)
            .expect("supported partial");
        partial
            .accumulate(&cells_i64(values), &mut charge)
            .expect("bounded values");
        partial
    }

    fn order(root: u8, segment: u8, batch: u32) -> CanonicalPartialOrder {
        CanonicalPartialOrder::new(
            u16::from(root),
            SegmentV2SegmentId::from_bytes([segment; 16]),
            batch,
        )
    }

    // Inert aggregate-partial checkpoint only. This does not discharge a
    // complete ADR-0161 obligation or select batch execution in production.
    #[test]
    fn count_sum_and_mean_partials_match_independent_scalar_oracle() {
        let count_cells = vec![
            SegmentV2Cell::Missing,
            SegmentV2Cell::Null,
            SegmentV2Cell::Value(CanonicalValue::Bool(false)),
        ];
        let mut count_charge = budget(3, 64);
        let mut count =
            ExactAggregatePartial::new(AggregateSemanticIdentityV1::Count, None, &mut count_charge)
                .expect("count partial");
        count
            .accumulate(&count_cells, &mut count_charge)
            .expect("count accepts every row state");
        assert_eq!(count.value(), ExactAggregatePartialValue::Count(3));

        let empty_cases = [
            (
                AggregateSemanticIdentityV1::Sum,
                ExactAggregatePartialValue::Sum {
                    coefficient: 0,
                    scale: 0,
                },
            ),
            (
                AggregateSemanticIdentityV1::Mean,
                ExactAggregatePartialValue::Mean {
                    coefficient: 0,
                    scale: 0,
                    count: 0,
                },
            ),
        ];
        for (semantic, expected) in empty_cases {
            let mut charge = budget(1, 64);
            let partial =
                ExactAggregatePartial::new(semantic, Some(&SegmentV2LogicalType::I64), &mut charge)
                    .expect("empty numeric partial");
            assert_eq!(partial.value(), expected);
        }

        let mut rng = StdRng::seed_from_u64(0xA661_5EED);
        for _ in 0..128 {
            let values = (0..rng.gen_range(0..=64))
                .map(|_| rng.gen_range(-1_000_000_i64..=1_000_000))
                .collect::<Vec<_>>();
            let expected_sum = values
                .iter()
                .fold(0_i128, |total, value| total + i128::from(*value));
            let sum = partial_for_values(AggregateSemanticIdentityV1::Sum, &values);
            assert_eq!(
                sum.value(),
                ExactAggregatePartialValue::Sum {
                    coefficient: expected_sum,
                    scale: 0,
                }
            );
            let mean = partial_for_values(AggregateSemanticIdentityV1::Mean, &values);
            assert_eq!(
                mean.value(),
                ExactAggregatePartialValue::Mean {
                    coefficient: expected_sum,
                    scale: 0,
                    count: u64::try_from(values.len()).expect("small fixture"),
                }
            );
        }

        let u64_values = [0_u64, u64::MAX, 7];
        let u64_cells = u64_values
            .iter()
            .copied()
            .map(|value| SegmentV2Cell::Value(CanonicalValue::U64(value)))
            .collect::<Vec<_>>();
        let expected_u64_sum = u64_values
            .iter()
            .fold(0_i128, |total, value| total + i128::from(*value));
        for semantic in [
            AggregateSemanticIdentityV1::Sum,
            AggregateSemanticIdentityV1::Mean,
        ] {
            let mut charge = budget(3, 64);
            let mut partial =
                ExactAggregatePartial::new(semantic, Some(&SegmentV2LogicalType::U64), &mut charge)
                    .expect("u64 partial");
            partial
                .accumulate(&u64_cells, &mut charge)
                .expect("u64 contributions");
            let expected = if semantic == AggregateSemanticIdentityV1::Sum {
                ExactAggregatePartialValue::Sum {
                    coefficient: expected_u64_sum,
                    scale: 0,
                }
            } else {
                ExactAggregatePartialValue::Mean {
                    coefficient: expected_u64_sum,
                    scale: 0,
                    count: 3,
                }
            };
            assert_eq!(partial.value(), expected);
        }

        let decimal = DecimalSpec::new(12, 3).expect("decimal type");
        let decimal_cells = [-4_200_i128, 1_100, 8_005]
            .into_iter()
            .map(|coefficient| {
                SegmentV2Cell::Value(CanonicalValue::Decimal(
                    Decimal::new(decimal, coefficient).expect("decimal value"),
                ))
            })
            .collect::<Vec<_>>();
        for semantic in [
            AggregateSemanticIdentityV1::Sum,
            AggregateSemanticIdentityV1::Mean,
        ] {
            let mut charge = budget(3, 64);
            let mut partial = ExactAggregatePartial::new(
                semantic,
                Some(&SegmentV2LogicalType::Decimal(decimal)),
                &mut charge,
            )
            .expect("decimal partial");
            partial
                .accumulate(&decimal_cells, &mut charge)
                .expect("decimal contributions");
            let expected = if semantic == AggregateSemanticIdentityV1::Sum {
                ExactAggregatePartialValue::Sum {
                    coefficient: 4_905,
                    scale: 3,
                }
            } else {
                ExactAggregatePartialValue::Mean {
                    coefficient: 4_905,
                    scale: 3,
                    count: 3,
                }
            };
            assert_eq!(partial.value(), expected);
        }
    }

    #[test]
    fn aggregate_partials_bind_the_authoritative_registry() {
        for (semantic, partial_state, arithmetic, input, no_value, empty, result) in [
            (
                AggregateSemanticIdentityV1::Count,
                AggregatePartialStateV1::CheckedU64,
                AggregateArithmeticV1::CheckedU64,
                AggregateInputClassV1::NoField,
                AggregateNoValueRuleV1::CountsRow,
                AggregateEmptyResultV1::UnsignedZero,
                AggregateResultSchemaV1::U64,
            ),
            (
                AggregateSemanticIdentityV1::Sum,
                AggregatePartialStateV1::CheckedI128AtInputScale,
                AggregateArithmeticV1::CheckedI128,
                AggregateInputClassV1::ExactNumericField,
                AggregateNoValueRuleV1::InvalidInputType,
                AggregateEmptyResultV1::ExactNumericZero,
                AggregateResultSchemaV1::ExactDecimalAtInputScale,
            ),
            (
                AggregateSemanticIdentityV1::Mean,
                AggregatePartialStateV1::ExactMeanV1,
                AggregateArithmeticV1::CheckedExactMean,
                AggregateInputClassV1::ExactNumericField,
                AggregateNoValueRuleV1::InvalidInputType,
                AggregateEmptyResultV1::ExactMeanZero,
                AggregateResultSchemaV1::ExactMeanV1,
            ),
        ] {
            let descriptor = semantic.descriptor();
            assert_eq!(descriptor.partial_state(), partial_state);
            assert_eq!(descriptor.arithmetic(), arithmetic);
            assert_eq!(descriptor.input_class(), input);
            assert_eq!(descriptor.no_value_rule(), no_value);
            assert_eq!(descriptor.empty_result(), empty);
            assert_eq!(descriptor.result_schema(), result);
        }

        let mut charge = budget(1, 64);
        assert_eq!(
            ExactAggregatePartial::new(AggregateSemanticIdentityV1::ExactCount, None, &mut charge,)
                .err(),
            Some(AggregatePartialError::UnsupportedSemantic)
        );
        assert_eq!(
            ExactAggregatePartial::new(
                AggregateSemanticIdentityV1::Count,
                Some(&SegmentV2LogicalType::U64),
                &mut charge,
            )
            .err(),
            Some(AggregatePartialError::InputShape)
        );
        assert_eq!(
            ExactAggregatePartial::new(
                AggregateSemanticIdentityV1::Sum,
                Some(&SegmentV2LogicalType::Money {
                    currency: riffdb_types::CurrencyCode::new(b"USD").expect("currency"),
                    amount: DecimalSpec::new(10, 2).expect("money decimal"),
                }),
                &mut charge,
            )
            .err(),
            Some(AggregatePartialError::InputShape)
        );
    }

    #[test]
    fn aggregate_partial_merge_laws_are_checked_and_canonical() {
        let values = [-9_i64, 4, 12, -2, 8, 1];
        let expected = values
            .iter()
            .fold(0_i128, |total, value| total + i128::from(*value));
        for semantic in [
            AggregateSemanticIdentityV1::Count,
            AggregateSemanticIdentityV1::Sum,
            AggregateSemanticIdentityV1::Mean,
        ] {
            for partitions in [
                vec![&values[..]],
                vec![&values[..0], &values[..2], &values[2..]],
                vec![&values[..1], &values[1..4], &values[4..]],
            ] {
                let leaves = partitions
                    .iter()
                    .map(|partition| partial_for_values(semantic, partition))
                    .collect::<Vec<_>>();
                let mut merge_charge =
                    budget(u32::try_from(leaves.len()).expect("small fixture"), 256);
                let logical_type = (semantic != AggregateSemanticIdentityV1::Count)
                    .then_some(&SegmentV2LogicalType::I64);
                let mut merged =
                    ExactAggregatePartial::new(semantic, logical_type, &mut merge_charge)
                        .expect("merge destination");
                for (index, leaf) in leaves.iter().enumerate() {
                    merged
                        .merge_from(
                            order(0, 1, u32::try_from(index).expect("small fixture")),
                            leaf,
                            &mut merge_charge,
                        )
                        .expect("canonical merge");
                }
                let expected_value = match semantic {
                    AggregateSemanticIdentityV1::Count => ExactAggregatePartialValue::Count(
                        u64::try_from(values.len()).expect("small fixture"),
                    ),
                    AggregateSemanticIdentityV1::Sum => ExactAggregatePartialValue::Sum {
                        coefficient: expected,
                        scale: 0,
                    },
                    AggregateSemanticIdentityV1::Mean => ExactAggregatePartialValue::Mean {
                        coefficient: expected,
                        scale: 0,
                        count: u64::try_from(values.len()).expect("small fixture"),
                    },
                    _ => unreachable!("closed test semantics"),
                };
                assert_eq!(merged.value(), expected_value);
                assert_eq!(merge_charge.remaining_arithmetic_operations(), 0);
            }
        }

        assert!(order(0, 1, 9) < order(0, 2, 0));
        assert!(order(0, 2, 0) < order(1, 0, 0));
        let leaf = partial_for_values(AggregateSemanticIdentityV1::Sum, &[7]);
        let mut charge = budget(3, 256);
        let mut merged = ExactAggregatePartial::new(
            AggregateSemanticIdentityV1::Sum,
            Some(&SegmentV2LogicalType::I64),
            &mut charge,
        )
        .expect("merge destination");
        merged
            .merge_from(order(0, 2, 0), &leaf, &mut charge)
            .expect("first merge");
        let before = merged.clone();
        let charge_before = charge;
        assert_eq!(
            merged.accumulate(&[SegmentV2Cell::Value(CanonicalValue::I64(1))], &mut charge,),
            Err(AggregatePartialError::ContributionAfterMerge)
        );
        assert_eq!(merged, before);
        assert_eq!(charge, charge_before);
        assert_eq!(
            merged.merge_from(order(0, 2, 0), &leaf, &mut charge),
            Err(AggregatePartialError::NonCanonicalMergeOrder)
        );
        assert_eq!(merged, before);
        assert_eq!(charge, charge_before);
        assert_eq!(
            merged.merge_from(order(0, 1, 9), &leaf, &mut charge),
            Err(AggregatePartialError::NonCanonicalMergeOrder)
        );
        assert_eq!(merged, before);
        assert_eq!(charge, charge_before);

        let incompatible = partial_for_values(AggregateSemanticIdentityV1::Mean, &[7]);
        assert_eq!(
            merged.merge_from(order(1, 0, 0), &incompatible, &mut charge),
            Err(AggregatePartialError::IncompatiblePartial)
        );
        assert_eq!(merged, before);
        assert_eq!(charge, charge_before);

        let sum_state =
            ExactAggregatePartial::required_state_bytes(AggregateSemanticIdentityV1::Sum)
                .expect("sum state bytes");
        let mut state_exact = budget(1, sum_state);
        let mut state_bounded = ExactAggregatePartial::new(
            AggregateSemanticIdentityV1::Sum,
            Some(&SegmentV2LogicalType::I64),
            &mut state_exact,
        )
        .expect("state-exact destination");
        let state_before = state_bounded.clone();
        let state_charge_before = state_exact;
        assert_eq!(
            state_bounded.merge_from(order(0, 0, 0), &leaf, &mut state_exact),
            Err(AggregatePartialError::StateBoundExceeded)
        );
        assert_eq!(state_bounded, state_before);
        assert_eq!(state_exact, state_charge_before);

        let mut one_merge_charge = budget(1, 256);
        let mut one_merge = ExactAggregatePartial::new(
            AggregateSemanticIdentityV1::Sum,
            Some(&SegmentV2LogicalType::I64),
            &mut one_merge_charge,
        )
        .expect("one-merge destination");
        one_merge
            .merge_from(order(0, 0, 0), &leaf, &mut one_merge_charge)
            .expect("one admitted merge");
        let one_merge_before = one_merge.clone();
        let one_merge_charge_before = one_merge_charge;
        assert_eq!(
            one_merge.merge_from(order(0, 0, 1), &leaf, &mut one_merge_charge),
            Err(AggregatePartialError::ArithmeticBoundExceeded)
        );
        assert_eq!(one_merge, one_merge_before);
        assert_eq!(one_merge_charge, one_merge_charge_before);

        for (semantic, left_value, right_value) in [
            (
                AggregateSemanticIdentityV1::Count,
                ExactAggregatePartialValue::Count(u64::MAX),
                ExactAggregatePartialValue::Count(1),
            ),
            (
                AggregateSemanticIdentityV1::Sum,
                ExactAggregatePartialValue::Sum {
                    coefficient: i128::MAX,
                    scale: 0,
                },
                ExactAggregatePartialValue::Sum {
                    coefficient: 1,
                    scale: 0,
                },
            ),
            (
                AggregateSemanticIdentityV1::Mean,
                ExactAggregatePartialValue::Mean {
                    coefficient: i128::MAX,
                    scale: 0,
                    count: 0,
                },
                ExactAggregatePartialValue::Mean {
                    coefficient: 1,
                    scale: 0,
                    count: 1,
                },
            ),
            (
                AggregateSemanticIdentityV1::Mean,
                ExactAggregatePartialValue::Mean {
                    coefficient: 0,
                    scale: 0,
                    count: u64::MAX,
                },
                ExactAggregatePartialValue::Mean {
                    coefficient: 1,
                    scale: 0,
                    count: 1,
                },
            ),
        ] {
            let logical_type = (semantic != AggregateSemanticIdentityV1::Count)
                .then_some(&SegmentV2LogicalType::I64);
            let mut left_charge = budget(1, 256);
            let mut left = ExactAggregatePartial::new(semantic, logical_type, &mut left_charge)
                .expect("overflow destination");
            left.value = left_value;
            let mut right_charge = budget(1, 256);
            let mut right = ExactAggregatePartial::new(semantic, logical_type, &mut right_charge)
                .expect("overflow source");
            right.value = right_value;
            let mut merge_charge = budget(1, 256);
            let left_before = left.clone();
            let merge_charge_before = merge_charge;
            assert_eq!(
                left.merge_from(order(0, 0, 0), &right, &mut merge_charge),
                Err(AggregatePartialError::ArithmeticOverflow)
            );
            assert_eq!(left, left_before);
            assert_eq!(merge_charge, merge_charge_before);
        }
    }

    #[test]
    fn aggregate_partial_bounds_overflow_and_failures_are_atomic() {
        assert_eq!(
            AggregatePartialBudget::new(0, 1),
            Err(AggregatePartialError::InvalidArithmeticBound)
        );
        assert_eq!(
            AggregatePartialBudget::new(1, 0),
            Err(AggregatePartialError::InvalidStateBound)
        );
        assert_eq!(
            AggregatePartialBudget::new(MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1 + 1, 1),
            Err(AggregatePartialError::InvalidArithmeticBound)
        );
        assert_eq!(
            AggregatePartialBudget::new(1, MAX_AGGREGATE_STATE_BYTES_V1 + 1),
            Err(AggregatePartialError::InvalidStateBound)
        );

        let required =
            ExactAggregatePartial::required_state_bytes(AggregateSemanticIdentityV1::Mean)
                .expect("mean state bytes");
        let mut short = budget(1, required - 1);
        let short_before = short;
        assert_eq!(
            ExactAggregatePartial::new(
                AggregateSemanticIdentityV1::Mean,
                Some(&SegmentV2LogicalType::I64),
                &mut short,
            )
            .err(),
            Some(AggregatePartialError::StateBoundExceeded)
        );
        assert_eq!(short, short_before);
        let mut exact = budget(1, required);
        ExactAggregatePartial::new(
            AggregateSemanticIdentityV1::Mean,
            Some(&SegmentV2LogicalType::I64),
            &mut exact,
        )
        .expect("exact state bound");
        assert_eq!(exact.remaining_state_bytes(), 0);

        let mut charge = budget(1, 64);
        let mut sum = ExactAggregatePartial::new(
            AggregateSemanticIdentityV1::Sum,
            Some(&SegmentV2LogicalType::I64),
            &mut charge,
        )
        .expect("sum partial");
        let sum_before = sum.clone();
        let charge_before = charge;
        assert_eq!(
            sum.accumulate(
                &[
                    SegmentV2Cell::Missing,
                    SegmentV2Cell::Value(CanonicalValue::I64(1)),
                ],
                &mut charge,
            ),
            Err(AggregatePartialError::ArithmeticBoundExceeded),
            "work bound precedes dependent input inspection"
        );
        assert_eq!(sum, sum_before);
        assert_eq!(charge, charge_before);

        for (cell, error) in [
            (SegmentV2Cell::Missing, AggregatePartialError::MissingField),
            (SegmentV2Cell::Null, AggregatePartialError::NoValue),
            (
                SegmentV2Cell::Value(CanonicalValue::U64(1)),
                AggregatePartialError::InputShape,
            ),
        ] {
            assert_eq!(sum.accumulate(&[cell], &mut charge), Err(error));
            assert_eq!(sum, sum_before);
            assert_eq!(charge, charge_before);
        }

        sum.value = ExactAggregatePartialValue::Sum {
            coefficient: i128::MAX,
            scale: 0,
        };
        let overflow_before = sum.clone();
        assert_eq!(
            sum.accumulate(&[SegmentV2Cell::Value(CanonicalValue::I64(1))], &mut charge,),
            Err(AggregatePartialError::ArithmeticOverflow)
        );
        assert_eq!(sum, overflow_before);
        assert_eq!(charge, charge_before);

        let mut count_charge = budget(1, 64);
        let mut count =
            ExactAggregatePartial::new(AggregateSemanticIdentityV1::Count, None, &mut count_charge)
                .expect("count partial");
        count.value = ExactAggregatePartialValue::Count(u64::MAX);
        let count_before = count.clone();
        let count_charge_before = count_charge;
        assert_eq!(
            count.accumulate(&[SegmentV2Cell::Null], &mut count_charge),
            Err(AggregatePartialError::ArithmeticOverflow)
        );
        assert_eq!(count, count_before);
        assert_eq!(count_charge, count_charge_before);

        let mut mean_charge = budget(1, 64);
        let mut mean = ExactAggregatePartial::new(
            AggregateSemanticIdentityV1::Mean,
            Some(&SegmentV2LogicalType::I64),
            &mut mean_charge,
        )
        .expect("mean partial");
        mean.value = ExactAggregatePartialValue::Mean {
            coefficient: 0,
            scale: 0,
            count: u64::MAX,
        };
        let mean_before = mean.clone();
        let mean_charge_before = mean_charge;
        assert_eq!(
            mean.accumulate(
                &[SegmentV2Cell::Value(CanonicalValue::I64(0))],
                &mut mean_charge,
            ),
            Err(AggregatePartialError::ArithmeticOverflow)
        );
        assert_eq!(mean, mean_before);
        assert_eq!(mean_charge, mean_charge_before);

        mean.value = ExactAggregatePartialValue::Mean {
            coefficient: i128::MAX,
            scale: 0,
            count: 0,
        };
        let mean_before = mean.clone();
        assert_eq!(
            mean.accumulate(
                &[SegmentV2Cell::Value(CanonicalValue::I64(1))],
                &mut mean_charge,
            ),
            Err(AggregatePartialError::ArithmeticOverflow)
        );
        assert_eq!(mean, mean_before);
        assert_eq!(mean_charge, mean_charge_before);
    }
}
