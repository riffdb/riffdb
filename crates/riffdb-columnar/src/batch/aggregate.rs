use riffdb_types::{
    AggregateArithmeticV1, AggregateEmptyResultV1, AggregateInputClassV1, AggregateNoValueRuleV1,
    AggregatePartialStateV1, AggregateResultSchemaV1, AggregateSemanticIdentityV1, CanonicalValue,
    DecimalSpec, MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1, MAX_AGGREGATE_STATE_BYTES_V1,
};

use crate::segment_v2::{SegmentV2Cell, SegmentV2LogicalType, SegmentV2SegmentId};

const CANONICAL_INVENTORY_IDENTITY_BYTES: u32 = 2 + 16 + 4;

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
    IncompatibleLeaf,
    InvalidInventory,
    DuplicateOrReorderedLeaf,
    InventoryGap,
    ForeignRootInventory,
    ForeignSegment,
    ExcessLeaf,
    InventoryOmission,
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
pub(super) struct CanonicalPartialIdentity {
    root_inventory_ordinal: u16,
    segment_id: SegmentV2SegmentId,
    batch_ordinal: u32,
}

impl CanonicalPartialIdentity {
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
struct ExactAggregateDefinition {
    semantic: AggregateSemanticIdentityV1,
    numeric_lane: Option<ExactNumericLane>,
}

impl ExactAggregateDefinition {
    fn new(
        semantic: AggregateSemanticIdentityV1,
        logical_type: Option<&SegmentV2LogicalType>,
    ) -> Result<Self, AggregatePartialError> {
        validate_registry(semantic)?;
        let numeric_lane = match semantic {
            AggregateSemanticIdentityV1::Count => {
                if logical_type.is_some() {
                    return Err(AggregatePartialError::InputShape);
                }
                None
            }
            AggregateSemanticIdentityV1::Sum | AggregateSemanticIdentityV1::Mean => {
                Some(ExactNumericLane::from_logical_type(
                    logical_type.ok_or(AggregatePartialError::InputShape)?,
                )?)
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
        Ok(Self {
            semantic,
            numeric_lane,
        })
    }

    fn empty_value(self) -> Result<ExactAggregatePartialValue, AggregatePartialError> {
        match self.semantic {
            AggregateSemanticIdentityV1::Count => Ok(ExactAggregatePartialValue::Count(0)),
            AggregateSemanticIdentityV1::Sum => Ok(ExactAggregatePartialValue::Sum {
                coefficient: 0,
                scale: self
                    .numeric_lane
                    .ok_or(AggregatePartialError::RegistryMismatch)?
                    .scale(),
            }),
            AggregateSemanticIdentityV1::Mean => Ok(ExactAggregatePartialValue::Mean {
                coefficient: 0,
                scale: self
                    .numeric_lane
                    .ok_or(AggregatePartialError::RegistryMismatch)?
                    .scale(),
                count: 0,
            }),
            AggregateSemanticIdentityV1::ExactCount
            | AggregateSemanticIdentityV1::Min
            | AggregateSemanticIdentityV1::Max
            | AggregateSemanticIdentityV1::CountPresent
            | AggregateSemanticIdentityV1::CountDistinct
            | AggregateSemanticIdentityV1::CountDistinctPresent
            | AggregateSemanticIdentityV1::Any
            | AggregateSemanticIdentityV1::All => Err(AggregatePartialError::UnsupportedSemantic),
        }
    }

    fn state_bytes(self) -> Result<u32, AggregatePartialError> {
        let bytes = match self.semantic {
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

impl ExactAggregatePartialValue {
    fn accumulate(
        self,
        definition: ExactAggregateDefinition,
        cells: &[SegmentV2Cell],
    ) -> Result<Self, AggregatePartialError> {
        match self {
            Self::Count(count) => {
                let contribution = u64::try_from(cells.len())
                    .map_err(|_| AggregatePartialError::ArithmeticOverflow)?;
                Ok(Self::Count(
                    count
                        .checked_add(contribution)
                        .ok_or(AggregatePartialError::ArithmeticOverflow)?,
                ))
            }
            Self::Sum {
                mut coefficient,
                scale,
            } => {
                let lane = definition
                    .numeric_lane
                    .ok_or(AggregatePartialError::RegistryMismatch)?;
                for cell in cells {
                    coefficient = coefficient
                        .checked_add(lane.contribution(cell)?)
                        .ok_or(AggregatePartialError::ArithmeticOverflow)?;
                }
                Ok(Self::Sum { coefficient, scale })
            }
            Self::Mean {
                mut coefficient,
                scale,
                mut count,
            } => {
                let lane = definition
                    .numeric_lane
                    .ok_or(AggregatePartialError::RegistryMismatch)?;
                for cell in cells {
                    coefficient = coefficient
                        .checked_add(lane.contribution(cell)?)
                        .ok_or(AggregatePartialError::ArithmeticOverflow)?;
                    count = count
                        .checked_add(1)
                        .ok_or(AggregatePartialError::ArithmeticOverflow)?;
                }
                Ok(Self::Mean {
                    coefficient,
                    scale,
                    count,
                })
            }
        }
    }

    fn merge(self, other: Self) -> Result<Self, AggregatePartialError> {
        match (self, other) {
            (Self::Count(left), Self::Count(right)) => Ok(Self::Count(
                left.checked_add(right)
                    .ok_or(AggregatePartialError::ArithmeticOverflow)?,
            )),
            (
                Self::Sum {
                    coefficient: left,
                    scale,
                },
                Self::Sum {
                    coefficient: right,
                    scale: right_scale,
                },
            ) if scale == right_scale => Ok(Self::Sum {
                coefficient: left
                    .checked_add(right)
                    .ok_or(AggregatePartialError::ArithmeticOverflow)?,
                scale,
            }),
            (
                Self::Mean {
                    coefficient: left,
                    scale,
                    count,
                },
                Self::Mean {
                    coefficient: right,
                    scale: right_scale,
                    count: right_count,
                },
            ) if scale == right_scale => Ok(Self::Mean {
                coefficient: left
                    .checked_add(right)
                    .ok_or(AggregatePartialError::ArithmeticOverflow)?,
                scale,
                count: count
                    .checked_add(right_count)
                    .ok_or(AggregatePartialError::ArithmeticOverflow)?,
            }),
            _ => Err(AggregatePartialError::IncompatibleLeaf),
        }
    }
}

pub(super) struct ExactAggregateLeafBuilder {
    identity: CanonicalPartialIdentity,
    definition: ExactAggregateDefinition,
    value: ExactAggregatePartialValue,
}

impl ExactAggregateLeafBuilder {
    pub(super) fn new(
        identity: CanonicalPartialIdentity,
        semantic: AggregateSemanticIdentityV1,
        logical_type: Option<&SegmentV2LogicalType>,
        budget: &mut AggregatePartialBudget,
    ) -> Result<Self, AggregatePartialError> {
        let definition = ExactAggregateDefinition::new(semantic, logical_type)?;
        let state_bytes = definition.state_bytes()?;
        budget.admits_state(state_bytes)?;
        let value = definition.empty_value()?;
        budget.commit_state(state_bytes);
        Ok(Self {
            identity,
            definition,
            value,
        })
    }

    pub(super) fn accumulate(
        &mut self,
        cells: &[SegmentV2Cell],
        budget: &mut AggregatePartialBudget,
    ) -> Result<(), AggregatePartialError> {
        let operations = budget.admits_operations(cells.len())?;
        let next = self.value.accumulate(self.definition, cells)?;
        self.value = next;
        budget.commit_operations(operations);
        Ok(())
    }

    pub(super) fn finalize(self) -> FinalizedAggregateLeaf {
        FinalizedAggregateLeaf {
            identity: self.identity,
            definition: self.definition,
            value: self.value,
        }
    }
}

pub(super) struct FinalizedAggregateLeaf {
    identity: CanonicalPartialIdentity,
    definition: ExactAggregateDefinition,
    value: ExactAggregatePartialValue,
}

struct SealedCanonicalPartialInventory {
    identities: Box<[CanonicalPartialIdentity]>,
    cursor: usize,
}

impl SealedCanonicalPartialInventory {
    fn validate(identities: &[CanonicalPartialIdentity]) -> Result<(), AggregatePartialError> {
        if identities.len() > MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1 as usize
            || identities
                .first()
                .is_some_and(|identity| identity.batch_ordinal != 0)
        {
            return Err(AggregatePartialError::InvalidInventory);
        }
        for pair in identities.windows(2) {
            let previous = pair[0];
            let next = pair[1];
            if next <= previous {
                return Err(AggregatePartialError::InvalidInventory);
            }
            if previous.root_inventory_ordinal == next.root_inventory_ordinal
                && previous.segment_id == next.segment_id
            {
                if previous.batch_ordinal.checked_add(1) != Some(next.batch_ordinal) {
                    return Err(AggregatePartialError::InvalidInventory);
                }
            } else if next.batch_ordinal != 0 {
                return Err(AggregatePartialError::InvalidInventory);
            }
        }
        Ok(())
    }

    fn state_bytes(identities: &[CanonicalPartialIdentity]) -> Result<u32, AggregatePartialError> {
        let count = u32::try_from(identities.len())
            .map_err(|_| AggregatePartialError::StateBoundExceeded)?;
        count
            .checked_mul(CANONICAL_INVENTORY_IDENTITY_BYTES)
            .ok_or(AggregatePartialError::StateBoundExceeded)
    }

    fn classify_mismatch(&self, actual: CanonicalPartialIdentity) -> AggregatePartialError {
        if self.identities[..self.cursor].contains(&actual) {
            return AggregatePartialError::DuplicateOrReorderedLeaf;
        }
        if self.cursor >= self.identities.len() {
            return AggregatePartialError::ExcessLeaf;
        }
        if self.identities[self.cursor + 1..].contains(&actual) {
            return AggregatePartialError::InventoryGap;
        }
        if !self
            .identities
            .iter()
            .any(|identity| identity.root_inventory_ordinal == actual.root_inventory_ordinal)
        {
            return AggregatePartialError::ForeignRootInventory;
        }
        if !self.identities.iter().any(|identity| {
            identity.root_inventory_ordinal == actual.root_inventory_ordinal
                && identity.segment_id == actual.segment_id
        }) {
            return AggregatePartialError::ForeignSegment;
        }
        if actual < self.identities[self.cursor] {
            AggregatePartialError::DuplicateOrReorderedLeaf
        } else {
            AggregatePartialError::InventoryGap
        }
    }
}

pub(super) struct ExactAggregateMergeAccumulator {
    inventory: SealedCanonicalPartialInventory,
    definition: ExactAggregateDefinition,
    value: ExactAggregatePartialValue,
}

impl ExactAggregateMergeAccumulator {
    pub(super) fn new(
        semantic: AggregateSemanticIdentityV1,
        logical_type: Option<&SegmentV2LogicalType>,
        exact_inventory: &[CanonicalPartialIdentity],
        budget: &mut AggregatePartialBudget,
    ) -> Result<Self, AggregatePartialError> {
        let definition = ExactAggregateDefinition::new(semantic, logical_type)?;
        SealedCanonicalPartialInventory::validate(exact_inventory)?;
        let state_bytes = definition
            .state_bytes()?
            .checked_add(SealedCanonicalPartialInventory::state_bytes(
                exact_inventory,
            )?)
            .ok_or(AggregatePartialError::StateBoundExceeded)?;
        budget.admits_state(state_bytes)?;
        let inventory = SealedCanonicalPartialInventory {
            identities: exact_inventory.to_vec().into_boxed_slice(),
            cursor: 0,
        };
        let value = definition.empty_value()?;
        budget.commit_state(state_bytes);
        Ok(Self {
            inventory,
            definition,
            value,
        })
    }

    pub(super) const fn value(&self) -> ExactAggregatePartialValue {
        self.value
    }

    pub(super) const fn consumed_leaves(&self) -> usize {
        self.inventory.cursor
    }

    pub(super) fn merge_leaf(
        &mut self,
        leaf: FinalizedAggregateLeaf,
        budget: &mut AggregatePartialBudget,
    ) -> Result<(), AggregatePartialError> {
        let Some(expected) = self.inventory.identities.get(self.inventory.cursor) else {
            return Err(self.inventory.classify_mismatch(leaf.identity));
        };
        if leaf.identity != *expected {
            return Err(self.inventory.classify_mismatch(leaf.identity));
        }
        if leaf.definition != self.definition {
            return Err(AggregatePartialError::IncompatibleLeaf);
        }
        let operations = budget.admits_operations(1)?;
        let next = self.value.merge(leaf.value)?;
        self.value = next;
        self.inventory.cursor += 1;
        budget.commit_operations(operations);
        Ok(())
    }

    pub(super) fn finish(self) -> Result<ExactAggregatePartialValue, AggregatePartialError> {
        if self.inventory.cursor == self.inventory.identities.len() {
            Ok(self.value)
        } else {
            Err(AggregatePartialError::InventoryOmission)
        }
    }
}

fn validate_registry(semantic: AggregateSemanticIdentityV1) -> Result<(), AggregatePartialError> {
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
                && descriptor.result_schema() == AggregateResultSchemaV1::ExactDecimalAtInputScale
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

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use riffdb_types::{CanonicalValue, Decimal, DecimalSpec};

    fn budget(operations: u32, state_bytes: u32) -> AggregatePartialBudget {
        AggregatePartialBudget::new(operations, state_bytes).expect("bounded aggregate budget")
    }

    fn identity(root: u16, segment: u8, batch: u32) -> CanonicalPartialIdentity {
        CanonicalPartialIdentity::new(root, SegmentV2SegmentId::from_bytes([segment; 16]), batch)
    }

    fn i64_cells(values: &[i64]) -> Vec<SegmentV2Cell> {
        values
            .iter()
            .copied()
            .map(|value| SegmentV2Cell::Value(CanonicalValue::I64(value)))
            .collect()
    }

    fn i64_leaf(
        id: CanonicalPartialIdentity,
        semantic: AggregateSemanticIdentityV1,
        values: &[i64],
    ) -> FinalizedAggregateLeaf {
        let mut charge = budget(
            u32::try_from(values.len().max(1)).expect("small fixture"),
            256,
        );
        let logical_type =
            (semantic != AggregateSemanticIdentityV1::Count).then_some(&SegmentV2LogicalType::I64);
        let mut builder = ExactAggregateLeafBuilder::new(id, semantic, logical_type, &mut charge)
            .expect("supported leaf");
        builder
            .accumulate(&i64_cells(values), &mut charge)
            .expect("bounded leaf values");
        builder.finalize()
    }

    // Inert aggregate-partial checkpoint only. This does not discharge a
    // complete ADR-0161 obligation or select batch execution in production.
    #[test]
    fn aggregate_leaf_states_match_independent_scalar_oracles() {
        let mut empty_charge = budget(1, 64);
        let empty = ExactAggregateMergeAccumulator::new(
            AggregateSemanticIdentityV1::Count,
            None,
            &[],
            &mut empty_charge,
        )
        .expect("direct empty count");
        assert_eq!(empty.finish(), Ok(ExactAggregatePartialValue::Count(0)));

        let count_id = identity(0, 1, 0);
        let mut count_charge = budget(3, 64);
        let mut count = ExactAggregateLeafBuilder::new(
            count_id,
            AggregateSemanticIdentityV1::Count,
            None,
            &mut count_charge,
        )
        .expect("count leaf");
        count
            .accumulate(
                &[
                    SegmentV2Cell::Missing,
                    SegmentV2Cell::Null,
                    SegmentV2Cell::Value(CanonicalValue::Bool(false)),
                ],
                &mut count_charge,
            )
            .expect("count accepts every row state");
        let mut merge_charge = budget(1, 128);
        let mut merged = ExactAggregateMergeAccumulator::new(
            AggregateSemanticIdentityV1::Count,
            None,
            &[count_id],
            &mut merge_charge,
        )
        .expect("count accumulator");
        merged
            .merge_leaf(count.finalize(), &mut merge_charge)
            .expect("count merge");
        assert_eq!(merged.finish(), Ok(ExactAggregatePartialValue::Count(3)));

        let mut rng = StdRng::seed_from_u64(0xA661_5EED);
        for _ in 0..128 {
            let values = (0..rng.gen_range(0..=64))
                .map(|_| rng.gen_range(-1_000_000_i64..=1_000_000))
                .collect::<Vec<_>>();
            let expected_sum = values
                .iter()
                .fold(0_i128, |total, value| total + i128::from(*value));
            for semantic in [
                AggregateSemanticIdentityV1::Sum,
                AggregateSemanticIdentityV1::Mean,
            ] {
                let id = identity(0, 1, 0);
                let leaf = i64_leaf(id, semantic, &values);
                let mut charge = budget(1, 128);
                let mut accumulator = ExactAggregateMergeAccumulator::new(
                    semantic,
                    Some(&SegmentV2LogicalType::I64),
                    &[id],
                    &mut charge,
                )
                .expect("numeric accumulator");
                accumulator
                    .merge_leaf(leaf, &mut charge)
                    .expect("numeric merge");
                let expected = if semantic == AggregateSemanticIdentityV1::Sum {
                    ExactAggregatePartialValue::Sum {
                        coefficient: expected_sum,
                        scale: 0,
                    }
                } else {
                    ExactAggregatePartialValue::Mean {
                        coefficient: expected_sum,
                        scale: 0,
                        count: u64::try_from(values.len()).expect("small fixture"),
                    }
                };
                assert_eq!(accumulator.finish(), Ok(expected));
            }
        }
    }

    #[test]
    fn sealed_inventory_consumes_each_finalized_leaf_exactly_once() {
        let ids = [
            identity(0, 1, 0),
            identity(0, 1, 1),
            identity(0, 2, 0),
            identity(1, 0, 0),
        ];
        let mut charge = budget(4, 512);
        let mut accumulator = ExactAggregateMergeAccumulator::new(
            AggregateSemanticIdentityV1::Sum,
            Some(&SegmentV2LogicalType::I64),
            &ids,
            &mut charge,
        )
        .expect("sealed inventory");
        for id in ids {
            accumulator
                .merge_leaf(
                    i64_leaf(id, AggregateSemanticIdentityV1::Sum, &[1]),
                    &mut charge,
                )
                .expect("exact next leaf");
        }
        assert_eq!(
            accumulator.finish(),
            Ok(ExactAggregatePartialValue::Sum {
                coefficient: 4,
                scale: 0,
            })
        );

        for (actual, error) in [
            (ids[1], AggregatePartialError::InventoryGap),
            (
                identity(9, 1, 0),
                AggregatePartialError::ForeignRootInventory,
            ),
            (identity(0, 9, 0), AggregatePartialError::ForeignSegment),
        ] {
            let mut charge = budget(2, 512);
            let mut accumulator = ExactAggregateMergeAccumulator::new(
                AggregateSemanticIdentityV1::Sum,
                Some(&SegmentV2LogicalType::I64),
                &ids,
                &mut charge,
            )
            .expect("sealed inventory");
            let value_before = accumulator.value();
            let cursor_before = accumulator.consumed_leaves();
            let charge_before = charge;
            assert_eq!(
                accumulator.merge_leaf(
                    i64_leaf(actual, AggregateSemanticIdentityV1::Sum, &[1]),
                    &mut charge,
                ),
                Err(error)
            );
            assert_eq!(accumulator.value(), value_before);
            assert_eq!(accumulator.consumed_leaves(), cursor_before);
            assert_eq!(charge, charge_before);
        }

        let mut charge = budget(2, 512);
        let mut duplicate = ExactAggregateMergeAccumulator::new(
            AggregateSemanticIdentityV1::Sum,
            Some(&SegmentV2LogicalType::I64),
            &ids,
            &mut charge,
        )
        .expect("sealed inventory");
        duplicate
            .merge_leaf(
                i64_leaf(ids[0], AggregateSemanticIdentityV1::Sum, &[1]),
                &mut charge,
            )
            .expect("first leaf");
        let value_before = duplicate.value();
        let cursor_before = duplicate.consumed_leaves();
        let charge_before = charge;
        assert_eq!(
            duplicate.merge_leaf(
                i64_leaf(ids[0], AggregateSemanticIdentityV1::Sum, &[1]),
                &mut charge,
            ),
            Err(AggregatePartialError::DuplicateOrReorderedLeaf)
        );
        assert_eq!(duplicate.value(), value_before);
        assert_eq!(duplicate.consumed_leaves(), cursor_before);
        assert_eq!(charge, charge_before);

        let mut charge = budget(1, 512);
        let mut omitted = ExactAggregateMergeAccumulator::new(
            AggregateSemanticIdentityV1::Sum,
            Some(&SegmentV2LogicalType::I64),
            &ids,
            &mut charge,
        )
        .expect("sealed inventory");
        omitted
            .merge_leaf(
                i64_leaf(ids[0], AggregateSemanticIdentityV1::Sum, &[1]),
                &mut charge,
            )
            .expect("first leaf");
        assert_eq!(
            omitted.finish(),
            Err(AggregatePartialError::InventoryOmission)
        );

        for invalid in [
            vec![ids[0], ids[0]],
            vec![ids[1], ids[0]],
            vec![ids[0], identity(0, 1, 2)],
            vec![identity(0, 1, 1)],
        ] {
            let mut charge = budget(1, 512);
            let before = charge;
            assert_eq!(
                ExactAggregateMergeAccumulator::new(
                    AggregateSemanticIdentityV1::Sum,
                    Some(&SegmentV2LogicalType::I64),
                    &invalid,
                    &mut charge,
                )
                .err(),
                Some(AggregatePartialError::InvalidInventory)
            );
            assert_eq!(charge, before);
        }
    }

    #[test]
    fn leaf_and_merge_failures_are_atomic() {
        let id = identity(0, 1, 0);
        let mut charge = budget(2, 128);
        let mut mean = ExactAggregateLeafBuilder::new(
            id,
            AggregateSemanticIdentityV1::Mean,
            Some(&SegmentV2LogicalType::I64),
            &mut charge,
        )
        .expect("mean leaf");
        for (cell, error) in [
            (SegmentV2Cell::Missing, AggregatePartialError::MissingField),
            (SegmentV2Cell::Null, AggregatePartialError::NoValue),
        ] {
            let value_before = mean.value;
            let charge_before = charge;
            assert_eq!(mean.accumulate(&[cell], &mut charge), Err(error));
            assert_eq!(mean.value, value_before);
            assert_eq!(charge, charge_before);
        }

        let expected = DecimalSpec::new(12, 2).expect("expected decimal");
        let actual = DecimalSpec::new(11, 2).expect("different decimal");
        let mut decimal_charge = budget(1, 128);
        let mut decimal = ExactAggregateLeafBuilder::new(
            id,
            AggregateSemanticIdentityV1::Sum,
            Some(&SegmentV2LogicalType::Decimal(expected)),
            &mut decimal_charge,
        )
        .expect("decimal leaf");
        let value_before = decimal.value;
        let charge_before = decimal_charge;
        assert_eq!(
            decimal.accumulate(
                &[SegmentV2Cell::Value(CanonicalValue::Decimal(
                    Decimal::new(actual, 7).expect("different decimal value"),
                ))],
                &mut decimal_charge,
            ),
            Err(AggregatePartialError::InputShape)
        );
        assert_eq!(decimal.value, value_before);
        assert_eq!(decimal_charge, charge_before);

        let mut merge_charge = budget(1, 128);
        let mut accumulator = ExactAggregateMergeAccumulator::new(
            AggregateSemanticIdentityV1::Sum,
            Some(&SegmentV2LogicalType::I64),
            &[id],
            &mut merge_charge,
        )
        .expect("sum accumulator");
        let value_before = accumulator.value();
        let cursor_before = accumulator.consumed_leaves();
        let charge_before = merge_charge;
        assert_eq!(
            accumulator.merge_leaf(
                i64_leaf(id, AggregateSemanticIdentityV1::Mean, &[1]),
                &mut merge_charge,
            ),
            Err(AggregatePartialError::IncompatibleLeaf)
        );
        assert_eq!(accumulator.value(), value_before);
        assert_eq!(accumulator.consumed_leaves(), cursor_before);
        assert_eq!(merge_charge, charge_before);
    }
}
