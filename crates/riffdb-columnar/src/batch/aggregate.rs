use std::cmp::Ordering;

use riffdb_types::{
    AggregateArithmeticV1, AggregateEmptyResultV1, AggregateInputClassV1, AggregateNoValueRuleV1,
    AggregatePartialStateV1, AggregateResultSchemaV1, AggregateSemanticIdentityV1, CanonicalValue,
    DecimalSpec, MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1, MAX_AGGREGATE_DISTINCT_VALUES_V1,
    MAX_AGGREGATE_STATE_BYTES_V1,
};

use crate::segment_v2::{SegmentV2Cell, SegmentV2LogicalType, SegmentV2SegmentId};

use super::top_n::{
    TopNComparisonProfile, TopNProgramBinding, compare_present, validate_present_type,
};

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
    InvalidDistinctBound,
    DistinctBoundExceeded,
    AmbiguousComparisonProfile,
    ProgramBindingSubstitution,
    OptionalBooleanField,
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

/// A non-owning aggregate input retained from one immutable V2 lane.
/// `Missing` and `Null` normalize to the single ADR-0152 `NoValue` class.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BorrowedAggregateScalar<'view> {
    NoValue,
    Value(&'view CanonicalValue),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AggregateFieldOptionality {
    Required,
    Optional,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct SealedAggregateInputFacts {
    binding: Option<TopNProgramBinding>,
    checked_binding: TopNProgramBinding,
    optionality: AggregateFieldOptionality,
}

impl SealedAggregateInputFacts {
    pub(super) const fn new(
        binding: Option<TopNProgramBinding>,
        checked_binding: TopNProgramBinding,
        optionality: AggregateFieldOptionality,
    ) -> Self {
        Self {
            binding,
            checked_binding,
            optionality,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BorrowedAggregateDefinition<'view> {
    binding: TopNProgramBinding,
    semantic: AggregateSemanticIdentityV1,
    logical_type: &'view SegmentV2LogicalType,
    optionality: AggregateFieldOptionality,
}

impl<'view> BorrowedAggregateDefinition<'view> {
    fn new(
        facts: SealedAggregateInputFacts,
        semantic: AggregateSemanticIdentityV1,
        logical_type: &'view SegmentV2LogicalType,
        distinct_limit: Option<u16>,
    ) -> Result<Self, AggregatePartialError> {
        let binding = facts
            .binding
            .ok_or(AggregatePartialError::AmbiguousComparisonProfile)?;
        if binding != facts.checked_binding {
            return Err(AggregatePartialError::ProgramBindingSubstitution);
        }
        match binding.comparison_profile() {
            TopNComparisonProfile::CanonicalScalarV1 => {}
        }
        if distinct_limit.is_some() {
            return Err(AggregatePartialError::UnsupportedSemantic);
        }
        validate_borrowed_registry(semantic)?;
        if matches!(
            semantic,
            AggregateSemanticIdentityV1::Any | AggregateSemanticIdentityV1::All
        ) && (*logical_type != SegmentV2LogicalType::Bool
            || facts.optionality != AggregateFieldOptionality::Required)
        {
            return Err(if *logical_type != SegmentV2LogicalType::Bool {
                AggregatePartialError::InputShape
            } else {
                AggregatePartialError::OptionalBooleanField
            });
        }
        Ok(Self {
            binding,
            semantic,
            logical_type,
            optionality: facts.optionality,
        })
    }

    fn empty_value(self) -> Result<BorrowedAggregatePartialValue<'view>, AggregatePartialError> {
        match self.semantic {
            AggregateSemanticIdentityV1::Min | AggregateSemanticIdentityV1::Max => {
                Ok(BorrowedAggregatePartialValue::Extreme(None))
            }
            AggregateSemanticIdentityV1::Any => Ok(BorrowedAggregatePartialValue::Boolean(false)),
            AggregateSemanticIdentityV1::All => Ok(BorrowedAggregatePartialValue::Boolean(true)),
            _ => Err(AggregatePartialError::UnsupportedSemantic),
        }
    }

    fn state_bytes(self) -> Result<u32, AggregatePartialError> {
        let bytes = match self.semantic {
            AggregateSemanticIdentityV1::Min | AggregateSemanticIdentityV1::Max => {
                std::mem::size_of::<Option<BorrowedAggregateScalar<'view>>>()
            }
            AggregateSemanticIdentityV1::Any | AggregateSemanticIdentityV1::All => {
                std::mem::size_of::<bool>()
            }
            _ => return Err(AggregatePartialError::UnsupportedSemantic),
        };
        u32::try_from(bytes).map_err(|_| AggregatePartialError::StateBoundExceeded)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum BorrowedAggregatePartialValue<'view> {
    Extreme(Option<BorrowedAggregateScalar<'view>>),
    Boolean(bool),
}

impl<'view> BorrowedAggregatePartialValue<'view> {
    fn accumulate(
        self,
        definition: BorrowedAggregateDefinition<'view>,
        cells: &'view [SegmentV2Cell],
    ) -> Result<Self, AggregatePartialError> {
        match (definition.semantic, self) {
            (
                semantic @ (AggregateSemanticIdentityV1::Min | AggregateSemanticIdentityV1::Max),
                Self::Extreme(mut selected),
            ) => {
                for cell in cells {
                    let candidate = borrowed_extreme_input(definition.logical_type, cell)?;
                    selected = Some(match selected {
                        None => candidate,
                        Some(current) => {
                            let compared = compare_borrowed_scalars(
                                definition.binding.comparison_profile(),
                                definition.logical_type,
                                candidate,
                                current,
                            )?;
                            let replace = match semantic {
                                AggregateSemanticIdentityV1::Min => compared == Ordering::Less,
                                AggregateSemanticIdentityV1::Max => compared == Ordering::Greater,
                                _ => return Err(AggregatePartialError::RegistryMismatch),
                            };
                            if replace { candidate } else { current }
                        }
                    });
                }
                Ok(Self::Extreme(selected))
            }
            (AggregateSemanticIdentityV1::Any, Self::Boolean(mut selected)) => {
                for cell in cells {
                    selected |= required_boolean(cell)?;
                }
                Ok(Self::Boolean(selected))
            }
            (AggregateSemanticIdentityV1::All, Self::Boolean(mut selected)) => {
                for cell in cells {
                    selected &= required_boolean(cell)?;
                }
                Ok(Self::Boolean(selected))
            }
            _ => Err(AggregatePartialError::RegistryMismatch),
        }
    }

    fn merge(
        self,
        definition: BorrowedAggregateDefinition<'view>,
        other: Self,
    ) -> Result<Self, AggregatePartialError> {
        match (definition.semantic, self, other) {
            (
                semantic @ (AggregateSemanticIdentityV1::Min | AggregateSemanticIdentityV1::Max),
                Self::Extreme(left),
                Self::Extreme(right),
            ) => match (left, right) {
                (None, other) | (other, None) => Ok(Self::Extreme(other)),
                (Some(left), Some(right)) => {
                    let compared = compare_borrowed_scalars(
                        definition.binding.comparison_profile(),
                        definition.logical_type,
                        right,
                        left,
                    )?;
                    let replace = match semantic {
                        AggregateSemanticIdentityV1::Min => compared == Ordering::Less,
                        AggregateSemanticIdentityV1::Max => compared == Ordering::Greater,
                        _ => return Err(AggregatePartialError::RegistryMismatch),
                    };
                    Ok(Self::Extreme(Some(if replace { right } else { left })))
                }
            },
            (AggregateSemanticIdentityV1::Any, Self::Boolean(left), Self::Boolean(right)) => {
                Ok(Self::Boolean(left | right))
            }
            (AggregateSemanticIdentityV1::All, Self::Boolean(left), Self::Boolean(right)) => {
                Ok(Self::Boolean(left & right))
            }
            _ => Err(AggregatePartialError::IncompatibleLeaf),
        }
    }
}

pub(super) struct ExactBorrowedAggregateLeafBuilder<'view> {
    identity: CanonicalPartialIdentity,
    definition: BorrowedAggregateDefinition<'view>,
    value: BorrowedAggregatePartialValue<'view>,
}

impl<'view> ExactBorrowedAggregateLeafBuilder<'view> {
    pub(super) fn new(
        identity: CanonicalPartialIdentity,
        facts: SealedAggregateInputFacts,
        semantic: AggregateSemanticIdentityV1,
        logical_type: &'view SegmentV2LogicalType,
        distinct_limit: Option<u16>,
        budget: &mut AggregatePartialBudget,
    ) -> Result<Self, AggregatePartialError> {
        let definition =
            BorrowedAggregateDefinition::new(facts, semantic, logical_type, distinct_limit)?;
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
        cells: &'view [SegmentV2Cell],
        budget: &mut AggregatePartialBudget,
    ) -> Result<(), AggregatePartialError> {
        let operations = budget.admits_operations(cells.len())?;
        let next = self.value.accumulate(self.definition, cells)?;
        self.value = next;
        budget.commit_operations(operations);
        Ok(())
    }

    pub(super) fn finalize(self) -> FinalizedBorrowedAggregateLeaf<'view> {
        FinalizedBorrowedAggregateLeaf {
            identity: self.identity,
            definition: self.definition,
            value: self.value,
        }
    }
}

pub(super) struct FinalizedBorrowedAggregateLeaf<'view> {
    identity: CanonicalPartialIdentity,
    definition: BorrowedAggregateDefinition<'view>,
    value: BorrowedAggregatePartialValue<'view>,
}

pub(super) struct ExactBorrowedAggregateMergeAccumulator<'view> {
    inventory: SealedCanonicalPartialInventory,
    definition: BorrowedAggregateDefinition<'view>,
    value: BorrowedAggregatePartialValue<'view>,
}

impl<'view> ExactBorrowedAggregateMergeAccumulator<'view> {
    pub(super) fn new(
        facts: SealedAggregateInputFacts,
        semantic: AggregateSemanticIdentityV1,
        logical_type: &'view SegmentV2LogicalType,
        distinct_limit: Option<u16>,
        exact_inventory: &[CanonicalPartialIdentity],
        budget: &mut AggregatePartialBudget,
    ) -> Result<Self, AggregatePartialError> {
        let definition =
            BorrowedAggregateDefinition::new(facts, semantic, logical_type, distinct_limit)?;
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

    pub(super) const fn value(&self) -> BorrowedAggregatePartialValue<'view> {
        self.value
    }

    pub(super) const fn consumed_leaves(&self) -> usize {
        self.inventory.cursor
    }

    pub(super) fn merge_leaf(
        &mut self,
        leaf: FinalizedBorrowedAggregateLeaf<'view>,
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
        let next = self.value.merge(self.definition, leaf.value)?;
        self.value = next;
        self.inventory.cursor += 1;
        budget.commit_operations(operations);
        Ok(())
    }

    pub(super) fn finish(
        self,
    ) -> Result<BorrowedAggregatePartialValue<'view>, AggregatePartialError> {
        if self.inventory.cursor == self.inventory.identities.len() {
            Ok(self.value)
        } else {
            Err(AggregatePartialError::InventoryOmission)
        }
    }
}

fn borrowed_extreme_input<'view>(
    logical_type: &SegmentV2LogicalType,
    cell: &'view SegmentV2Cell,
) -> Result<BorrowedAggregateScalar<'view>, AggregatePartialError> {
    match cell {
        SegmentV2Cell::Missing | SegmentV2Cell::Null => Ok(BorrowedAggregateScalar::NoValue),
        SegmentV2Cell::Value(value) if validate_present_type(logical_type, value) => {
            Ok(BorrowedAggregateScalar::Value(value))
        }
        SegmentV2Cell::Value(_) => Err(AggregatePartialError::InputShape),
    }
}

fn compare_borrowed_scalars(
    profile: TopNComparisonProfile,
    logical_type: &SegmentV2LogicalType,
    left: BorrowedAggregateScalar<'_>,
    right: BorrowedAggregateScalar<'_>,
) -> Result<Ordering, AggregatePartialError> {
    match (left, right) {
        (BorrowedAggregateScalar::NoValue, BorrowedAggregateScalar::NoValue) => Ok(Ordering::Equal),
        (BorrowedAggregateScalar::NoValue, BorrowedAggregateScalar::Value(_)) => Ok(Ordering::Less),
        (BorrowedAggregateScalar::Value(_), BorrowedAggregateScalar::NoValue) => {
            Ok(Ordering::Greater)
        }
        (BorrowedAggregateScalar::Value(left), BorrowedAggregateScalar::Value(right)) => {
            compare_present(profile, logical_type, left, right)
                .map_err(|_| AggregatePartialError::InputShape)
        }
    }
}

fn required_boolean(cell: &SegmentV2Cell) -> Result<bool, AggregatePartialError> {
    match cell {
        SegmentV2Cell::Missing => Err(AggregatePartialError::MissingField),
        SegmentV2Cell::Null => Err(AggregatePartialError::NoValue),
        SegmentV2Cell::Value(CanonicalValue::Bool(value)) => Ok(*value),
        SegmentV2Cell::Value(_) => Err(AggregatePartialError::InputShape),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ExactDistinctDefinition<'view> {
    semantic: AggregateSemanticIdentityV1,
    logical_type: &'view SegmentV2LogicalType,
    maximum_distinct: usize,
}

impl<'view> ExactDistinctDefinition<'view> {
    fn new(
        semantic: AggregateSemanticIdentityV1,
        logical_type: &'view SegmentV2LogicalType,
        maximum_distinct: u16,
    ) -> Result<Self, AggregatePartialError> {
        validate_distinct_registry(semantic)?;
        if maximum_distinct == 0 || maximum_distinct > MAX_AGGREGATE_DISTINCT_VALUES_V1 {
            return Err(AggregatePartialError::InvalidDistinctBound);
        }
        Ok(Self {
            semantic,
            logical_type,
            maximum_distinct: usize::from(maximum_distinct),
        })
    }

    fn member(
        self,
        cell: &'view SegmentV2Cell,
    ) -> Result<Option<BorrowedAggregateScalar<'view>>, AggregatePartialError> {
        match cell {
            SegmentV2Cell::Missing | SegmentV2Cell::Null
                if self.semantic == AggregateSemanticIdentityV1::CountDistinctPresent =>
            {
                Ok(None)
            }
            SegmentV2Cell::Missing | SegmentV2Cell::Null => {
                Ok(Some(BorrowedAggregateScalar::NoValue))
            }
            SegmentV2Cell::Value(value) if validate_present_type(self.logical_type, value) => {
                Ok(Some(BorrowedAggregateScalar::Value(value)))
            }
            SegmentV2Cell::Value(_) => Err(AggregatePartialError::InputShape),
        }
    }
}

#[derive(Debug)]
struct BoundedDistinctSet<'view> {
    members: Vec<BorrowedAggregateScalar<'view>>,
    scratch: Vec<BorrowedAggregateScalar<'view>>,
    maximum: usize,
}

impl<'view> BoundedDistinctSet<'view> {
    fn requested_state_bytes(maximum: usize) -> Result<u32, AggregatePartialError> {
        let member_bytes = maximum
            .checked_mul(std::mem::size_of::<BorrowedAggregateScalar<'view>>())
            .and_then(|bytes| bytes.checked_mul(2))
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Self>()))
            .ok_or(AggregatePartialError::StateBoundExceeded)?;
        u32::try_from(member_bytes).map_err(|_| AggregatePartialError::StateBoundExceeded)
    }

    fn new(maximum: usize) -> Result<Self, AggregatePartialError> {
        let mut members = Vec::new();
        members
            .try_reserve_exact(maximum)
            .map_err(|_| AggregatePartialError::StateBoundExceeded)?;
        let mut scratch = Vec::new();
        scratch
            .try_reserve_exact(maximum)
            .map_err(|_| AggregatePartialError::StateBoundExceeded)?;
        Ok(Self {
            members,
            scratch,
            maximum,
        })
    }

    fn actual_state_bytes(&self) -> Result<u32, AggregatePartialError> {
        let member_capacity = self
            .members
            .capacity()
            .checked_add(self.scratch.capacity())
            .and_then(|capacity| {
                capacity.checked_mul(std::mem::size_of::<BorrowedAggregateScalar<'view>>())
            })
            .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Self>()))
            .ok_or(AggregatePartialError::StateBoundExceeded)?;
        u32::try_from(member_capacity).map_err(|_| AggregatePartialError::StateBoundExceeded)
    }

    const fn len(&self) -> usize {
        self.members.len()
    }

    fn union_cells(
        &mut self,
        definition: ExactDistinctDefinition<'view>,
        cells: &'view [SegmentV2Cell],
    ) -> Result<(), AggregatePartialError> {
        self.scratch.clear();
        self.scratch.extend_from_slice(&self.members);
        let result = (|| {
            for cell in cells {
                if let Some(member) = definition.member(cell)? {
                    Self::insert_unique(&mut self.scratch, self.maximum, member)?;
                }
            }
            Ok(())
        })();
        if let Err(error) = result {
            self.scratch.clear();
            return Err(error);
        }
        std::mem::swap(&mut self.members, &mut self.scratch);
        self.scratch.clear();
        Ok(())
    }

    fn union_members(
        &mut self,
        members: &[BorrowedAggregateScalar<'view>],
    ) -> Result<(), AggregatePartialError> {
        self.scratch.clear();
        self.scratch.extend_from_slice(&self.members);
        let result = (|| {
            for member in members {
                Self::insert_unique(&mut self.scratch, self.maximum, *member)?;
            }
            Ok(())
        })();
        if let Err(error) = result {
            self.scratch.clear();
            return Err(error);
        }
        std::mem::swap(&mut self.members, &mut self.scratch);
        self.scratch.clear();
        Ok(())
    }

    fn insert_unique(
        members: &mut Vec<BorrowedAggregateScalar<'view>>,
        maximum: usize,
        member: BorrowedAggregateScalar<'view>,
    ) -> Result<(), AggregatePartialError> {
        if members.contains(&member) {
            return Ok(());
        }
        if members.len() >= maximum {
            return Err(AggregatePartialError::DistinctBoundExceeded);
        }
        members.push(member);
        Ok(())
    }
}

pub(super) struct ExactDistinctAggregateLeafBuilder<'view> {
    identity: CanonicalPartialIdentity,
    definition: ExactDistinctDefinition<'view>,
    state: BoundedDistinctSet<'view>,
}

impl<'view> ExactDistinctAggregateLeafBuilder<'view> {
    pub(super) fn new(
        identity: CanonicalPartialIdentity,
        semantic: AggregateSemanticIdentityV1,
        logical_type: &'view SegmentV2LogicalType,
        maximum_distinct: u16,
        budget: &mut AggregatePartialBudget,
    ) -> Result<Self, AggregatePartialError> {
        let definition = ExactDistinctDefinition::new(semantic, logical_type, maximum_distinct)?;
        budget.admits_state(BoundedDistinctSet::requested_state_bytes(
            definition.maximum_distinct,
        )?)?;
        let state = BoundedDistinctSet::new(definition.maximum_distinct)?;
        let state_bytes = state.actual_state_bytes()?;
        budget.admits_state(state_bytes)?;
        budget.commit_state(state_bytes);
        Ok(Self {
            identity,
            definition,
            state,
        })
    }

    pub(super) fn accumulate(
        &mut self,
        cells: &'view [SegmentV2Cell],
        budget: &mut AggregatePartialBudget,
    ) -> Result<(), AggregatePartialError> {
        let operations = budget.admits_operations(cells.len())?;
        self.state.union_cells(self.definition, cells)?;
        budget.commit_operations(operations);
        Ok(())
    }

    pub(super) const fn distinct_count(&self) -> usize {
        self.state.len()
    }

    pub(super) fn finalize(self) -> FinalizedDistinctAggregateLeaf<'view> {
        FinalizedDistinctAggregateLeaf {
            identity: self.identity,
            definition: self.definition,
            state: self.state,
        }
    }
}

pub(super) struct FinalizedDistinctAggregateLeaf<'view> {
    identity: CanonicalPartialIdentity,
    definition: ExactDistinctDefinition<'view>,
    state: BoundedDistinctSet<'view>,
}

pub(super) struct ExactDistinctAggregateMergeAccumulator<'view> {
    inventory: SealedCanonicalPartialInventory,
    definition: ExactDistinctDefinition<'view>,
    state: BoundedDistinctSet<'view>,
}

impl<'view> ExactDistinctAggregateMergeAccumulator<'view> {
    pub(super) fn new(
        semantic: AggregateSemanticIdentityV1,
        logical_type: &'view SegmentV2LogicalType,
        maximum_distinct: u16,
        exact_inventory: &[CanonicalPartialIdentity],
        budget: &mut AggregatePartialBudget,
    ) -> Result<Self, AggregatePartialError> {
        let definition = ExactDistinctDefinition::new(semantic, logical_type, maximum_distinct)?;
        SealedCanonicalPartialInventory::validate(exact_inventory)?;
        let requested_state =
            BoundedDistinctSet::requested_state_bytes(definition.maximum_distinct)?
                .checked_add(SealedCanonicalPartialInventory::state_bytes(
                    exact_inventory,
                )?)
                .ok_or(AggregatePartialError::StateBoundExceeded)?;
        budget.admits_state(requested_state)?;
        let state = BoundedDistinctSet::new(definition.maximum_distinct)?;
        let actual_state = state
            .actual_state_bytes()?
            .checked_add(SealedCanonicalPartialInventory::state_bytes(
                exact_inventory,
            )?)
            .ok_or(AggregatePartialError::StateBoundExceeded)?;
        budget.admits_state(actual_state)?;
        let inventory = SealedCanonicalPartialInventory {
            identities: exact_inventory.to_vec().into_boxed_slice(),
            cursor: 0,
        };
        budget.commit_state(actual_state);
        Ok(Self {
            inventory,
            definition,
            state,
        })
    }

    pub(super) const fn distinct_count(&self) -> usize {
        self.state.len()
    }

    pub(super) const fn consumed_leaves(&self) -> usize {
        self.inventory.cursor
    }

    pub(super) fn merge_leaf(
        &mut self,
        leaf: FinalizedDistinctAggregateLeaf<'view>,
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
        let operations = budget.admits_operations(leaf.state.len().max(1))?;
        self.state.union_members(&leaf.state.members)?;
        self.inventory.cursor += 1;
        budget.commit_operations(operations);
        Ok(())
    }

    pub(super) fn finish(self) -> Result<u64, AggregatePartialError> {
        if self.inventory.cursor != self.inventory.identities.len() {
            return Err(AggregatePartialError::InventoryOmission);
        }
        u64::try_from(self.state.len()).map_err(|_| AggregatePartialError::ArithmeticOverflow)
    }
}

fn validate_distinct_registry(
    semantic: AggregateSemanticIdentityV1,
) -> Result<(), AggregatePartialError> {
    if !matches!(
        semantic,
        AggregateSemanticIdentityV1::CountDistinct
            | AggregateSemanticIdentityV1::CountDistinctPresent
    ) {
        return Err(AggregatePartialError::UnsupportedSemantic);
    }
    let descriptor = semantic.descriptor();
    let no_value = match semantic {
        AggregateSemanticIdentityV1::CountDistinct => AggregateNoValueRuleV1::DistinctValue,
        AggregateSemanticIdentityV1::CountDistinctPresent => AggregateNoValueRuleV1::Excluded,
        _ => return Err(AggregatePartialError::UnsupportedSemantic),
    };
    if descriptor.partial_state() == AggregatePartialStateV1::BoundedCanonicalSet
        && descriptor.arithmetic() == AggregateArithmeticV1::CanonicalDistinctSet
        && descriptor.input_class() == AggregateInputClassV1::CanonicalScalarField
        && descriptor.no_value_rule() == no_value
        && descriptor.empty_result() == AggregateEmptyResultV1::UnsignedZero
        && descriptor.result_schema() == AggregateResultSchemaV1::U64
    {
        Ok(())
    } else {
        Err(AggregatePartialError::RegistryMismatch)
    }
}

fn validate_borrowed_registry(
    semantic: AggregateSemanticIdentityV1,
) -> Result<(), AggregatePartialError> {
    let descriptor = semantic.descriptor();
    let matches = match semantic {
        AggregateSemanticIdentityV1::Min | AggregateSemanticIdentityV1::Max => {
            descriptor.partial_state() == AggregatePartialStateV1::OptionalOrderedScalar
                && descriptor.arithmetic() == AggregateArithmeticV1::FrozenTypedComparator
                && descriptor.input_class() == AggregateInputClassV1::OrderedScalarField
                && descriptor.no_value_rule() == AggregateNoValueRuleV1::ComparableState
                && descriptor.empty_result() == AggregateEmptyResultV1::Absent
                && descriptor.result_schema() == AggregateResultSchemaV1::OptionalInputScalar
        }
        AggregateSemanticIdentityV1::Any => {
            descriptor.partial_state() == AggregatePartialStateV1::BooleanAny
                && descriptor.arithmetic() == AggregateArithmeticV1::BooleanOr
                && descriptor.input_class() == AggregateInputClassV1::RequiredBooleanField
                && descriptor.no_value_rule() == AggregateNoValueRuleV1::InvalidInputType
                && descriptor.empty_result() == AggregateEmptyResultV1::BooleanFalse
                && descriptor.result_schema() == AggregateResultSchemaV1::Bool
        }
        AggregateSemanticIdentityV1::All => {
            descriptor.partial_state() == AggregatePartialStateV1::BooleanAll
                && descriptor.arithmetic() == AggregateArithmeticV1::BooleanAnd
                && descriptor.input_class() == AggregateInputClassV1::RequiredBooleanField
                && descriptor.no_value_rule() == AggregateNoValueRuleV1::InvalidInputType
                && descriptor.empty_result() == AggregateEmptyResultV1::BooleanTrue
                && descriptor.result_schema() == AggregateResultSchemaV1::Bool
        }
        _ => return Err(AggregatePartialError::UnsupportedSemantic),
    };
    if matches {
        Ok(())
    } else {
        Err(AggregatePartialError::RegistryMismatch)
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
    use rand::seq::SliceRandom;
    use rand::{Rng, SeedableRng};
    use riffdb_types::{
        CanonicalValue, CurrencyCode, Date, Decimal, DecimalSpec, EnumTypeId, EnumVariantId, Money,
        ProjectionGeneration, QueryPlanHash, Timestamp, encode_canonical_value,
    };
    use std::collections::BTreeSet;

    fn budget(operations: u32, state_bytes: u32) -> AggregatePartialBudget {
        AggregatePartialBudget::new(operations, state_bytes).expect("bounded aggregate budget")
    }

    fn identity(root: u16, segment: u8, batch: u32) -> CanonicalPartialIdentity {
        CanonicalPartialIdentity::new(root, SegmentV2SegmentId::from_bytes([segment; 16]), batch)
    }

    fn aggregate_binding(seed: u8) -> TopNProgramBinding {
        TopNProgramBinding::new(
            Some(TopNComparisonProfile::CanonicalScalarV1),
            QueryPlanHash::from_bytes([seed; 32]),
            ProjectionGeneration::new(7).expect("generation"),
        )
        .expect("canonical scalar V1 binding")
    }

    fn optionality(semantic: AggregateSemanticIdentityV1) -> AggregateFieldOptionality {
        if matches!(
            semantic,
            AggregateSemanticIdentityV1::Any | AggregateSemanticIdentityV1::All
        ) {
            AggregateFieldOptionality::Required
        } else {
            AggregateFieldOptionality::Optional
        }
    }

    fn aggregate_facts(
        seed: u8,
        optionality: AggregateFieldOptionality,
    ) -> SealedAggregateInputFacts {
        let binding = aggregate_binding(seed);
        SealedAggregateInputFacts::new(Some(binding), binding, optionality)
    }

    fn i64_cells(values: &[i64]) -> Vec<SegmentV2Cell> {
        values
            .iter()
            .copied()
            .map(|value| SegmentV2Cell::Value(CanonicalValue::I64(value)))
            .collect()
    }

    fn present(cell: &SegmentV2Cell) -> &CanonicalValue {
        let SegmentV2Cell::Value(value) = cell else {
            panic!("fixture cell must be present")
        };
        value
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

    fn numeric_leaf(
        id: CanonicalPartialIdentity,
        semantic: AggregateSemanticIdentityV1,
        logical_type: &SegmentV2LogicalType,
        cells: &[SegmentV2Cell],
    ) -> FinalizedAggregateLeaf {
        let mut charge = budget(
            u32::try_from(cells.len().max(1)).expect("small fixture"),
            256,
        );
        let mut builder =
            ExactAggregateLeafBuilder::new(id, semantic, Some(logical_type), &mut charge)
                .expect("supported numeric leaf");
        builder
            .accumulate(cells, &mut charge)
            .expect("bounded numeric leaf values");
        builder.finalize()
    }

    fn leaf_with_value(
        id: CanonicalPartialIdentity,
        semantic: AggregateSemanticIdentityV1,
        value: ExactAggregatePartialValue,
    ) -> FinalizedAggregateLeaf {
        let logical_type =
            (semantic != AggregateSemanticIdentityV1::Count).then_some(&SegmentV2LogicalType::I64);
        let mut charge = budget(1, 256);
        let mut builder = ExactAggregateLeafBuilder::new(id, semantic, logical_type, &mut charge)
            .expect("supported boundary leaf");
        builder.value = value;
        builder.finalize()
    }

    // Inert aggregate-partial checkpoint only. This does not discharge a
    // complete ADR-0161 obligation or select batch execution in production.
    #[test]
    fn aggregate_leaf_states_match_independent_scalar_oracles() {
        for (semantic, expected) in [
            (
                AggregateSemanticIdentityV1::Count,
                ExactAggregatePartialValue::Count(0),
            ),
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
        ] {
            let logical_type = (semantic != AggregateSemanticIdentityV1::Count)
                .then_some(&SegmentV2LogicalType::I64);
            let mut empty_charge = budget(1, 64);
            let empty =
                ExactAggregateMergeAccumulator::new(semantic, logical_type, &[], &mut empty_charge)
                    .expect("direct empty aggregate");
            assert_eq!(empty.finish(), Ok(expected));
        }

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

        let count_ids = [identity(0, 1, 0), identity(0, 1, 1), identity(0, 2, 0)];
        let count_partitions: [&[i64]; 3] = [&[], &[1, 2], &[3]];
        let mut merge_charge = budget(3, 256);
        let mut merged = ExactAggregateMergeAccumulator::new(
            AggregateSemanticIdentityV1::Count,
            None,
            &count_ids,
            &mut merge_charge,
        )
        .expect("partitioned count accumulator");
        for (id, partition) in count_ids.into_iter().zip(count_partitions) {
            merged
                .merge_leaf(
                    i64_leaf(id, AggregateSemanticIdentityV1::Count, partition),
                    &mut merge_charge,
                )
                .expect("partitioned count merge");
        }
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
        let value_before = accumulator.value();
        let cursor_before = accumulator.consumed_leaves();
        let charge_before = charge;
        assert_eq!(
            accumulator.merge_leaf(
                i64_leaf(identity(1, 0, 1), AggregateSemanticIdentityV1::Sum, &[1],),
                &mut charge,
            ),
            Err(AggregatePartialError::ExcessLeaf)
        );
        assert_eq!(accumulator.value(), value_before);
        assert_eq!(accumulator.consumed_leaves(), cursor_before);
        assert_eq!(charge, charge_before);
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
        for semantic in [
            AggregateSemanticIdentityV1::Sum,
            AggregateSemanticIdentityV1::Mean,
        ] {
            let mut charge = budget(2, 128);
            let mut builder = ExactAggregateLeafBuilder::new(
                id,
                semantic,
                Some(&SegmentV2LogicalType::I64),
                &mut charge,
            )
            .expect("required numeric leaf");
            for (cell, error) in [
                (SegmentV2Cell::Missing, AggregatePartialError::MissingField),
                (SegmentV2Cell::Null, AggregatePartialError::NoValue),
            ] {
                let value_before = builder.value;
                let charge_before = charge;
                assert_eq!(builder.accumulate(&[cell], &mut charge), Err(error));
                assert_eq!(builder.value, value_before);
                assert_eq!(charge, charge_before);
            }
        }

        let expected = DecimalSpec::new(12, 2).expect("expected decimal");
        let actual = DecimalSpec::new(11, 2).expect("different decimal");
        for semantic in [
            AggregateSemanticIdentityV1::Sum,
            AggregateSemanticIdentityV1::Mean,
        ] {
            let mut decimal_charge = budget(1, 128);
            let mut decimal = ExactAggregateLeafBuilder::new(
                id,
                semantic,
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
        }

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

    fn assert_randomized_partition_merges(
        logical_type: &SegmentV2LogicalType,
        cells: &[SegmentV2Cell],
        expected_sum: i128,
        rng: &mut StdRng,
    ) {
        for _ in 0..32 {
            let mut ranges = Vec::new();
            ranges.push(0..0);
            let mut start = 0;
            while start < cells.len() {
                let width = rng.gen_range(1..=usize::min(11, cells.len() - start));
                let end = start + width;
                ranges.push(start..end);
                start = end;
            }
            let ids = (0..ranges.len())
                .map(|batch| identity(0, 1, u32::try_from(batch).expect("small fixture")))
                .collect::<Vec<_>>();

            for semantic in [
                AggregateSemanticIdentityV1::Sum,
                AggregateSemanticIdentityV1::Mean,
            ] {
                let mut merge_charge = budget(
                    u32::try_from(ids.len()).expect("small fixture"),
                    MAX_AGGREGATE_STATE_BYTES_V1,
                );
                let mut accumulator = ExactAggregateMergeAccumulator::new(
                    semantic,
                    Some(logical_type),
                    &ids,
                    &mut merge_charge,
                )
                .expect("partition accumulator");
                for (id, range) in ids.iter().copied().zip(ranges.iter()) {
                    accumulator
                        .merge_leaf(
                            numeric_leaf(id, semantic, logical_type, &cells[range.clone()]),
                            &mut merge_charge,
                        )
                        .expect("canonical partition merge");
                }
                let expected = if semantic == AggregateSemanticIdentityV1::Sum {
                    ExactAggregatePartialValue::Sum {
                        coefficient: expected_sum,
                        scale: match logical_type {
                            SegmentV2LogicalType::Decimal(spec) => spec.scale(),
                            _ => 0,
                        },
                    }
                } else {
                    ExactAggregatePartialValue::Mean {
                        coefficient: expected_sum,
                        scale: match logical_type {
                            SegmentV2LogicalType::Decimal(spec) => spec.scale(),
                            _ => 0,
                        },
                        count: u64::try_from(cells.len()).expect("small fixture"),
                    }
                };
                assert_eq!(accumulator.finish(), Ok(expected));

                if ids.len() > 1 {
                    let mut permutation = (0..ids.len()).collect::<Vec<_>>();
                    permutation.shuffle(rng);
                    if permutation.iter().copied().eq(0..ids.len()) {
                        permutation.rotate_left(1);
                    }
                    let mut merge_charge = budget(
                        u32::try_from(ids.len()).expect("small fixture"),
                        MAX_AGGREGATE_STATE_BYTES_V1,
                    );
                    let mut rejected = ExactAggregateMergeAccumulator::new(
                        semantic,
                        Some(logical_type),
                        &ids,
                        &mut merge_charge,
                    )
                    .expect("permutation accumulator");
                    let mut refusal = None;
                    for index in permutation {
                        if let Err(error) = rejected.merge_leaf(
                            numeric_leaf(
                                ids[index],
                                semantic,
                                logical_type,
                                &cells[ranges[index].clone()],
                            ),
                            &mut merge_charge,
                        ) {
                            refusal = Some(error);
                            break;
                        }
                    }
                    assert!(matches!(
                        refusal,
                        Some(
                            AggregatePartialError::InventoryGap
                                | AggregatePartialError::DuplicateOrReorderedLeaf
                        )
                    ));
                    assert!(rejected.consumed_leaves() < ids.len());
                }
            }
        }
    }

    #[test]
    fn randomized_exact_numeric_partitions_match_independent_merge_oracles() {
        let mut rng = StdRng::seed_from_u64(0xA661_C0DE);

        let i64_values = std::iter::once(i64::MIN)
            .chain(std::iter::once(i64::MAX))
            .chain((0..95).map(|_| rng.gen_range(-1_000_000_i64..=1_000_000)))
            .collect::<Vec<_>>();
        let i64_sum = i64_values
            .iter()
            .fold(0_i128, |total, value| total + i128::from(*value));
        assert_randomized_partition_merges(
            &SegmentV2LogicalType::I64,
            &i64_cells(&i64_values),
            i64_sum,
            &mut rng,
        );

        let u64_values = std::iter::once(u64::MAX)
            .chain((0..96).map(|_| rng.gen_range(0_u64..=1_000_000)))
            .collect::<Vec<_>>();
        let u64_sum = u64_values
            .iter()
            .fold(0_i128, |total, value| total + i128::from(*value));
        let u64_cells = u64_values
            .iter()
            .copied()
            .map(|value| SegmentV2Cell::Value(CanonicalValue::U64(value)))
            .collect::<Vec<_>>();
        assert_randomized_partition_merges(
            &SegmentV2LogicalType::U64,
            &u64_cells,
            u64_sum,
            &mut rng,
        );

        let decimal_spec = DecimalSpec::new(18, 4).expect("decimal corpus type");
        let decimal_coefficients = (0..97)
            .map(|_| rng.gen_range(-1_000_000_i128..=1_000_000))
            .collect::<Vec<_>>();
        let decimal_sum = decimal_coefficients.iter().copied().sum::<i128>();
        let decimal_cells = decimal_coefficients
            .iter()
            .copied()
            .map(|coefficient| {
                SegmentV2Cell::Value(CanonicalValue::Decimal(
                    Decimal::new(decimal_spec, coefficient).expect("decimal corpus value"),
                ))
            })
            .collect::<Vec<_>>();
        assert_randomized_partition_merges(
            &SegmentV2LogicalType::Decimal(decimal_spec),
            &decimal_cells,
            decimal_sum,
            &mut rng,
        );
    }

    #[test]
    fn checked_merge_extrema_refuse_i128_and_u64_overflow_atomically() {
        let ids = [identity(0, 1, 0), identity(0, 1, 1)];
        for (semantic, left, right, expected) in [
            (
                AggregateSemanticIdentityV1::Count,
                ExactAggregatePartialValue::Count(u64::MAX - 1),
                ExactAggregatePartialValue::Count(1),
                ExactAggregatePartialValue::Count(u64::MAX),
            ),
            (
                AggregateSemanticIdentityV1::Sum,
                ExactAggregatePartialValue::Sum {
                    coefficient: i128::MAX - 1,
                    scale: 0,
                },
                ExactAggregatePartialValue::Sum {
                    coefficient: 1,
                    scale: 0,
                },
                ExactAggregatePartialValue::Sum {
                    coefficient: i128::MAX,
                    scale: 0,
                },
            ),
            (
                AggregateSemanticIdentityV1::Sum,
                ExactAggregatePartialValue::Sum {
                    coefficient: i128::MIN,
                    scale: 0,
                },
                ExactAggregatePartialValue::Sum {
                    coefficient: 0,
                    scale: 0,
                },
                ExactAggregatePartialValue::Sum {
                    coefficient: i128::MIN,
                    scale: 0,
                },
            ),
            (
                AggregateSemanticIdentityV1::Mean,
                ExactAggregatePartialValue::Mean {
                    coefficient: i128::MAX - 1,
                    scale: 0,
                    count: 0,
                },
                ExactAggregatePartialValue::Mean {
                    coefficient: 1,
                    scale: 0,
                    count: 1,
                },
                ExactAggregatePartialValue::Mean {
                    coefficient: i128::MAX,
                    scale: 0,
                    count: 1,
                },
            ),
            (
                AggregateSemanticIdentityV1::Mean,
                ExactAggregatePartialValue::Mean {
                    coefficient: i128::MIN,
                    scale: 0,
                    count: 0,
                },
                ExactAggregatePartialValue::Mean {
                    coefficient: 0,
                    scale: 0,
                    count: 1,
                },
                ExactAggregatePartialValue::Mean {
                    coefficient: i128::MIN,
                    scale: 0,
                    count: 1,
                },
            ),
        ] {
            let logical_type = (semantic != AggregateSemanticIdentityV1::Count)
                .then_some(&SegmentV2LogicalType::I64);
            let mut charge = budget(2, 256);
            let mut accumulator =
                ExactAggregateMergeAccumulator::new(semantic, logical_type, &ids, &mut charge)
                    .expect("boundary accumulator");
            accumulator
                .merge_leaf(leaf_with_value(ids[0], semantic, left), &mut charge)
                .expect("boundary left");
            accumulator
                .merge_leaf(leaf_with_value(ids[1], semantic, right), &mut charge)
                .expect("exact boundary merge");
            assert_eq!(accumulator.finish(), Ok(expected));
        }

        for (semantic, left, right) in [
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
                AggregateSemanticIdentityV1::Sum,
                ExactAggregatePartialValue::Sum {
                    coefficient: i128::MIN,
                    scale: 0,
                },
                ExactAggregatePartialValue::Sum {
                    coefficient: -1,
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
                    coefficient: i128::MIN,
                    scale: 0,
                    count: 0,
                },
                ExactAggregatePartialValue::Mean {
                    coefficient: -1,
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
                    coefficient: 0,
                    scale: 0,
                    count: 1,
                },
            ),
        ] {
            let logical_type = (semantic != AggregateSemanticIdentityV1::Count)
                .then_some(&SegmentV2LogicalType::I64);
            let mut charge = budget(2, 256);
            let mut accumulator =
                ExactAggregateMergeAccumulator::new(semantic, logical_type, &ids, &mut charge)
                    .expect("overflow accumulator");
            accumulator
                .merge_leaf(leaf_with_value(ids[0], semantic, left), &mut charge)
                .expect("overflow left");
            let value_before = accumulator.value();
            let cursor_before = accumulator.consumed_leaves();
            let charge_before = charge;
            assert_eq!(
                accumulator.merge_leaf(leaf_with_value(ids[1], semantic, right), &mut charge,),
                Err(AggregatePartialError::ArithmeticOverflow)
            );
            assert_eq!(accumulator.value(), value_before);
            assert_eq!(accumulator.consumed_leaves(), cursor_before);
            assert_eq!(charge, charge_before);
        }
    }

    #[test]
    fn exact_global_operation_and_state_bounds_are_enforced() {
        assert_eq!(
            AggregatePartialBudget::new(MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1 + 1, 1),
            Err(AggregatePartialError::InvalidArithmeticBound)
        );
        assert_eq!(
            AggregatePartialBudget::new(1, MAX_AGGREGATE_STATE_BYTES_V1 + 1),
            Err(AggregatePartialError::InvalidStateBound)
        );

        let mut operation_charge = budget(
            MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1,
            MAX_AGGREGATE_STATE_BYTES_V1,
        );
        let mut count = ExactAggregateLeafBuilder::new(
            identity(0, 1, 0),
            AggregateSemanticIdentityV1::Count,
            None,
            &mut operation_charge,
        )
        .expect("maximum-operation leaf");
        let maximum_cells =
            vec![SegmentV2Cell::Null; MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1 as usize];
        count
            .accumulate(&maximum_cells, &mut operation_charge)
            .expect("exact maximum operations");
        assert_eq!(operation_charge.remaining_arithmetic_operations(), 0);
        let value_before = count.value;
        let charge_before = operation_charge;
        assert_eq!(
            count.accumulate(&[SegmentV2Cell::Null], &mut operation_charge),
            Err(AggregatePartialError::ArithmeticBoundExceeded)
        );
        assert_eq!(count.value, value_before);
        assert_eq!(operation_charge, charge_before);

        let definition = ExactAggregateDefinition::new(AggregateSemanticIdentityV1::Count, None)
            .expect("count definition");
        let exact_state = definition
            .state_bytes()
            .expect("count state")
            .checked_add(CANONICAL_INVENTORY_IDENTITY_BYTES)
            .expect("small exact state");
        let id = identity(0, 1, 0);
        let mut exact_charge = budget(1, exact_state);
        ExactAggregateMergeAccumulator::new(
            AggregateSemanticIdentityV1::Count,
            None,
            &[id],
            &mut exact_charge,
        )
        .expect("exact state budget");
        assert_eq!(exact_charge.remaining_state_bytes(), 0);

        let mut short_charge = budget(1, exact_state - 1);
        let short_before = short_charge;
        assert_eq!(
            ExactAggregateMergeAccumulator::new(
                AggregateSemanticIdentityV1::Count,
                None,
                &[id],
                &mut short_charge,
            )
            .err(),
            Some(AggregatePartialError::StateBoundExceeded)
        );
        assert_eq!(short_charge, short_before);
    }

    fn borrowed_leaf<'view>(
        id: CanonicalPartialIdentity,
        semantic: AggregateSemanticIdentityV1,
        logical_type: &'view SegmentV2LogicalType,
        cells: &'view [SegmentV2Cell],
    ) -> FinalizedBorrowedAggregateLeaf<'view> {
        borrowed_leaf_with_binding(id, aggregate_binding(1), semantic, logical_type, cells)
    }

    fn borrowed_leaf_with_binding<'view>(
        id: CanonicalPartialIdentity,
        binding: TopNProgramBinding,
        semantic: AggregateSemanticIdentityV1,
        logical_type: &'view SegmentV2LogicalType,
        cells: &'view [SegmentV2Cell],
    ) -> FinalizedBorrowedAggregateLeaf<'view> {
        let mut charge = budget(
            u32::try_from(cells.len().max(1)).expect("small fixture"),
            256,
        );
        let mut builder = ExactBorrowedAggregateLeafBuilder::new(
            id,
            SealedAggregateInputFacts::new(Some(binding), binding, optionality(semantic)),
            semantic,
            logical_type,
            None,
            &mut charge,
        )
        .expect("supported borrowed leaf");
        builder
            .accumulate(cells, &mut charge)
            .expect("valid borrowed contributions");
        builder.finalize()
    }

    fn merge_borrowed<'view>(
        semantic: AggregateSemanticIdentityV1,
        logical_type: &'view SegmentV2LogicalType,
        partitions: &'view [&'view [SegmentV2Cell]],
    ) -> BorrowedAggregatePartialValue<'view> {
        let ids = (0..partitions.len())
            .map(|batch| identity(0, 1, u32::try_from(batch).expect("small fixture")))
            .collect::<Vec<_>>();
        let mut charge = budget(
            u32::try_from(
                partitions.iter().map(|cells| cells.len()).sum::<usize>() + partitions.len(),
            )
            .expect("small fixture"),
            4_096,
        );
        let mut accumulator = ExactBorrowedAggregateMergeAccumulator::new(
            aggregate_facts(1, optionality(semantic)),
            semantic,
            logical_type,
            None,
            &ids,
            &mut charge,
        )
        .expect("borrowed merge accumulator");
        for (id, cells) in ids.into_iter().zip(partitions.iter().copied()) {
            accumulator
                .merge_leaf(
                    borrowed_leaf(id, semantic, logical_type, cells),
                    &mut charge,
                )
                .expect("canonical borrowed merge");
        }
        accumulator.finish().expect("complete borrowed inventory")
    }

    fn independent_canonical_scalar_v1_order(
        left: &CanonicalValue,
        right: &CanonicalValue,
    ) -> Ordering {
        let left_bytes = encode_canonical_value(left).expect("canonical left value");
        let right_bytes = encode_canonical_value(right).expect("canonical right value");
        match (left, right) {
            (CanonicalValue::Bool(left), CanonicalValue::Bool(right)) => left.cmp(right),
            (CanonicalValue::I64(left), CanonicalValue::I64(right)) => left.cmp(right),
            (CanonicalValue::U64(left), CanonicalValue::U64(right)) => left.cmp(right),
            (CanonicalValue::String(left), CanonicalValue::String(right)) => {
                left.as_str().cmp(right.as_str())
            }
            (CanonicalValue::Bytes(left), CanonicalValue::Bytes(right)) => {
                left.as_bytes().cmp(right.as_bytes())
            }
            (CanonicalValue::Timestamp(left), CanonicalValue::Timestamp(right)) => left.cmp(right),
            (CanonicalValue::Date(left), CanonicalValue::Date(right)) => left.cmp(right),
            (CanonicalValue::Uuid(left), CanonicalValue::Uuid(right)) => left.cmp(right),
            (
                CanonicalValue::Enum {
                    type_id: left_type,
                    variant_id: left,
                },
                CanonicalValue::Enum {
                    type_id: right_type,
                    variant_id: right,
                },
            ) if left_type == right_type => left.cmp(right),
            (CanonicalValue::Decimal(_), CanonicalValue::Decimal(_))
            | (CanonicalValue::Money(_), CanonicalValue::Money(_)) => left_bytes.cmp(&right_bytes),
            _ => panic!("fixture values must share one admitted scalar type"),
        }
    }

    fn assert_extreme_pair(logical_type: &SegmentV2LogicalType, cells: &[SegmentV2Cell]) {
        let minimum = cells
            .iter()
            .min_by(|left, right| {
                independent_canonical_scalar_v1_order(present(left), present(right))
            })
            .expect("nonempty extrema fixture");
        let maximum = cells
            .iter()
            .max_by(|left, right| {
                independent_canonical_scalar_v1_order(present(left), present(right))
            })
            .expect("nonempty extrema fixture");
        let split = cells.len() / 2;
        let partitions = [&cells[..split], &cells[split..]];
        assert_eq!(
            merge_borrowed(AggregateSemanticIdentityV1::Min, logical_type, &partitions,),
            BorrowedAggregatePartialValue::Extreme(Some(BorrowedAggregateScalar::Value(present(
                minimum
            ))))
        );
        assert_eq!(
            merge_borrowed(AggregateSemanticIdentityV1::Max, logical_type, &partitions,),
            BorrowedAggregatePartialValue::Extreme(Some(BorrowedAggregateScalar::Value(present(
                maximum
            ))))
        );
    }

    #[test]
    fn borrowed_extrema_and_boolean_partials_match_independent_identities() {
        let i64_type = SegmentV2LogicalType::I64;
        let i64_value_cells = i64_cells(&[5, -7, 9, 9]);
        let partitions = [
            &i64_value_cells[..1],
            &i64_value_cells[1..3],
            &i64_value_cells[3..],
        ];
        assert_eq!(
            merge_borrowed(AggregateSemanticIdentityV1::Min, &i64_type, &partitions),
            BorrowedAggregatePartialValue::Extreme(Some(BorrowedAggregateScalar::Value(present(
                &i64_value_cells[1]
            ))))
        );
        assert_eq!(
            merge_borrowed(AggregateSemanticIdentityV1::Max, &i64_type, &partitions),
            BorrowedAggregatePartialValue::Extreme(Some(BorrowedAggregateScalar::Value(present(
                &i64_value_cells[2]
            ))))
        );

        let decimal = DecimalSpec::new(8, 2).expect("decimal spec");
        let decimal_type = SegmentV2LogicalType::Decimal(decimal);
        let decimal_cells = [-1, 0, 1]
            .into_iter()
            .map(|coefficient| {
                SegmentV2Cell::Value(CanonicalValue::Decimal(
                    Decimal::new(decimal, coefficient).expect("decimal"),
                ))
            })
            .collect::<Vec<_>>();
        let decimal_partitions = [&decimal_cells[..1], &decimal_cells[1..]];
        assert_eq!(
            merge_borrowed(
                AggregateSemanticIdentityV1::Min,
                &decimal_type,
                &decimal_partitions,
            ),
            BorrowedAggregatePartialValue::Extreme(Some(BorrowedAggregateScalar::Value(present(
                &decimal_cells[1]
            ))))
        );
        assert_eq!(
            merge_borrowed(
                AggregateSemanticIdentityV1::Max,
                &decimal_type,
                &decimal_partitions,
            ),
            BorrowedAggregatePartialValue::Extreme(Some(BorrowedAggregateScalar::Value(present(
                &decimal_cells[0]
            ))))
        );

        let usd = CurrencyCode::new("USD").expect("currency");
        let money_type = SegmentV2LogicalType::Money {
            currency: usd,
            amount: decimal,
        };
        let money_cells = [-2, 7, 1]
            .into_iter()
            .map(|coefficient| {
                SegmentV2Cell::Value(CanonicalValue::Money(Money::new(
                    usd,
                    Decimal::new(decimal, coefficient).expect("amount"),
                )))
            })
            .collect::<Vec<_>>();
        let money_partitions = [&money_cells[..2], &money_cells[2..]];
        assert_eq!(
            merge_borrowed(
                AggregateSemanticIdentityV1::Min,
                &money_type,
                &money_partitions,
            ),
            BorrowedAggregatePartialValue::Extreme(Some(BorrowedAggregateScalar::Value(present(
                &money_cells[2]
            ))))
        );

        let bool_type = SegmentV2LogicalType::Bool;
        let bool_cells = [
            SegmentV2Cell::Value(CanonicalValue::Bool(false)),
            SegmentV2Cell::Value(CanonicalValue::Bool(true)),
            SegmentV2Cell::Value(CanonicalValue::Bool(false)),
        ];
        let bool_partitions = [&bool_cells[..1], &bool_cells[1..]];
        assert_eq!(
            merge_borrowed(
                AggregateSemanticIdentityV1::Any,
                &bool_type,
                &bool_partitions
            ),
            BorrowedAggregatePartialValue::Boolean(true)
        );
        assert_eq!(
            merge_borrowed(
                AggregateSemanticIdentityV1::All,
                &bool_type,
                &bool_partitions
            ),
            BorrowedAggregatePartialValue::Boolean(false)
        );

        let enum_type = EnumTypeId::new(7).expect("enum type");
        for (logical_type, cells) in [
            (
                SegmentV2LogicalType::Bool,
                vec![
                    SegmentV2Cell::Value(CanonicalValue::Bool(true)),
                    SegmentV2Cell::Value(CanonicalValue::Bool(false)),
                ],
            ),
            (
                SegmentV2LogicalType::U64,
                vec![
                    SegmentV2Cell::Value(CanonicalValue::U64(u64::MAX)),
                    SegmentV2Cell::Value(CanonicalValue::U64(0)),
                ],
            ),
            (
                SegmentV2LogicalType::I64,
                i64_cells(&[i64::MIN, -1, 0, i64::MAX]),
            ),
            (
                SegmentV2LogicalType::Decimal(decimal),
                [-1, 0, 1]
                    .into_iter()
                    .map(|coefficient| {
                        SegmentV2Cell::Value(CanonicalValue::Decimal(
                            Decimal::new(decimal, coefficient).expect("decimal"),
                        ))
                    })
                    .collect(),
            ),
            (
                SegmentV2LogicalType::Money {
                    currency: usd,
                    amount: decimal,
                },
                [-1, 0, 1]
                    .into_iter()
                    .map(|coefficient| {
                        SegmentV2Cell::Value(CanonicalValue::Money(Money::new(
                            usd,
                            Decimal::new(decimal, coefficient).expect("money"),
                        )))
                    })
                    .collect(),
            ),
            (
                SegmentV2LogicalType::String,
                vec![
                    SegmentV2Cell::Value(CanonicalValue::string("z").expect("string")),
                    SegmentV2Cell::Value(CanonicalValue::string("a").expect("string")),
                ],
            ),
            (
                SegmentV2LogicalType::Bytes,
                vec![
                    SegmentV2Cell::Value(CanonicalValue::bytes([2]).expect("bytes")),
                    SegmentV2Cell::Value(CanonicalValue::bytes([1]).expect("bytes")),
                ],
            ),
            (
                SegmentV2LogicalType::Timestamp,
                vec![
                    SegmentV2Cell::Value(CanonicalValue::Timestamp(
                        Timestamp::new(1, 0).expect("timestamp"),
                    )),
                    SegmentV2Cell::Value(CanonicalValue::Timestamp(
                        Timestamp::new(-1, 0).expect("timestamp"),
                    )),
                ],
            ),
            (
                SegmentV2LogicalType::Date,
                vec![
                    SegmentV2Cell::Value(CanonicalValue::Date(Date::new(1))),
                    SegmentV2Cell::Value(CanonicalValue::Date(Date::new(-1))),
                ],
            ),
            (
                SegmentV2LogicalType::Uuid,
                vec![
                    SegmentV2Cell::Value(CanonicalValue::Uuid([2; 16])),
                    SegmentV2Cell::Value(CanonicalValue::Uuid([1; 16])),
                ],
            ),
            (
                SegmentV2LogicalType::Enum(enum_type),
                vec![
                    SegmentV2Cell::Value(CanonicalValue::Enum {
                        type_id: enum_type,
                        variant_id: EnumVariantId::new(2).expect("variant"),
                    }),
                    SegmentV2Cell::Value(CanonicalValue::Enum {
                        type_id: enum_type,
                        variant_id: EnumVariantId::new(1).expect("variant"),
                    }),
                ],
            ),
        ] {
            assert_extreme_pair(&logical_type, &cells);
        }
    }

    #[test]
    fn borrowed_extrema_bind_canonical_scalar_v1_encoded_decimal_and_money_order() {
        let decimal = DecimalSpec::new(8, 2).expect("decimal spec");
        let usd = CurrencyCode::new("USD").expect("currency");
        for (logical_type, cells) in [
            (
                SegmentV2LogicalType::Decimal(decimal),
                [-1, 0, 1]
                    .into_iter()
                    .map(|coefficient| {
                        SegmentV2Cell::Value(CanonicalValue::Decimal(
                            Decimal::new(decimal, coefficient).expect("decimal"),
                        ))
                    })
                    .collect::<Vec<_>>(),
            ),
            (
                SegmentV2LogicalType::Money {
                    currency: usd,
                    amount: decimal,
                },
                [-2, 7, 1]
                    .into_iter()
                    .map(|coefficient| {
                        SegmentV2Cell::Value(CanonicalValue::Money(Money::new(
                            usd,
                            Decimal::new(decimal, coefficient).expect("amount"),
                        )))
                    })
                    .collect::<Vec<_>>(),
            ),
        ] {
            let expected_minimum = cells
                .iter()
                .min_by_key(|cell| encode_canonical_value(present(cell)).expect("canonical value"))
                .expect("nonempty fixture");
            let expected_maximum = cells
                .iter()
                .max_by_key(|cell| encode_canonical_value(present(cell)).expect("canonical value"))
                .expect("nonempty fixture");
            let partitions = [&cells[..1], &cells[1..]];
            assert_eq!(
                merge_borrowed(AggregateSemanticIdentityV1::Min, &logical_type, &partitions),
                BorrowedAggregatePartialValue::Extreme(Some(BorrowedAggregateScalar::Value(
                    present(expected_minimum)
                )))
            );
            assert_eq!(
                merge_borrowed(AggregateSemanticIdentityV1::Max, &logical_type, &partitions),
                BorrowedAggregatePartialValue::Extreme(Some(BorrowedAggregateScalar::Value(
                    present(expected_maximum)
                )))
            );
        }
    }

    #[test]
    fn borrowed_extrema_boolean_empty_no_value_bounds_and_failures_are_closed() {
        let i64_type = SegmentV2LogicalType::I64;
        let bool_type = SegmentV2LogicalType::Bool;
        assert_eq!(
            aggregate_binding(1).comparison_profile(),
            TopNComparisonProfile::CanonicalScalarV1
        );
        for (semantic, logical_type, expected) in [
            (
                AggregateSemanticIdentityV1::Min,
                &i64_type,
                BorrowedAggregatePartialValue::Extreme(None),
            ),
            (
                AggregateSemanticIdentityV1::Max,
                &i64_type,
                BorrowedAggregatePartialValue::Extreme(None),
            ),
            (
                AggregateSemanticIdentityV1::Any,
                &bool_type,
                BorrowedAggregatePartialValue::Boolean(false),
            ),
            (
                AggregateSemanticIdentityV1::All,
                &bool_type,
                BorrowedAggregatePartialValue::Boolean(true),
            ),
        ] {
            let mut charge = budget(1, 64);
            let accumulator = ExactBorrowedAggregateMergeAccumulator::new(
                aggregate_facts(1, optionality(semantic)),
                semantic,
                logical_type,
                None,
                &[],
                &mut charge,
            )
            .expect("empty identity");
            assert_eq!(accumulator.finish(), Ok(expected));
        }

        let no_value_cells = [
            SegmentV2Cell::Value(CanonicalValue::I64(5)),
            SegmentV2Cell::Missing,
            SegmentV2Cell::Null,
        ];
        let no_value_partitions = [&no_value_cells[..1], &no_value_cells[1..]];
        assert_eq!(
            merge_borrowed(
                AggregateSemanticIdentityV1::Min,
                &i64_type,
                &no_value_partitions,
            ),
            BorrowedAggregatePartialValue::Extreme(Some(BorrowedAggregateScalar::NoValue))
        );
        assert_eq!(
            merge_borrowed(
                AggregateSemanticIdentityV1::Max,
                &i64_type,
                &no_value_partitions,
            ),
            BorrowedAggregatePartialValue::Extreme(Some(BorrowedAggregateScalar::Value(present(
                &no_value_cells[0]
            ))))
        );

        let id = identity(0, 1, 0);
        let mut ambiguous_charge = budget(1, 64);
        let ambiguous_before = ambiguous_charge;
        assert_eq!(
            ExactBorrowedAggregateLeafBuilder::new(
                id,
                SealedAggregateInputFacts::new(
                    None,
                    aggregate_binding(1),
                    AggregateFieldOptionality::Required,
                ),
                AggregateSemanticIdentityV1::Min,
                &i64_type,
                None,
                &mut ambiguous_charge,
            )
            .err(),
            Some(AggregatePartialError::AmbiguousComparisonProfile)
        );
        assert_eq!(ambiguous_charge, ambiguous_before);

        let mut substituted_charge = budget(1, 64);
        let substituted_before = substituted_charge;
        assert_eq!(
            ExactBorrowedAggregateLeafBuilder::new(
                id,
                SealedAggregateInputFacts::new(
                    Some(aggregate_binding(2)),
                    aggregate_binding(1),
                    AggregateFieldOptionality::Required,
                ),
                AggregateSemanticIdentityV1::Min,
                &i64_type,
                None,
                &mut substituted_charge,
            )
            .err(),
            Some(AggregatePartialError::ProgramBindingSubstitution)
        );
        assert_eq!(substituted_charge, substituted_before);

        let mut optional_boolean_charge = budget(1, 64);
        let optional_boolean_before = optional_boolean_charge;
        assert_eq!(
            ExactBorrowedAggregateLeafBuilder::new(
                id,
                aggregate_facts(1, AggregateFieldOptionality::Optional),
                AggregateSemanticIdentityV1::Any,
                &bool_type,
                None,
                &mut optional_boolean_charge,
            )
            .err(),
            Some(AggregatePartialError::OptionalBooleanField)
        );
        assert_eq!(optional_boolean_charge, optional_boolean_before);

        let mut non_boolean_charge = budget(1, 64);
        let non_boolean_before = non_boolean_charge;
        assert_eq!(
            ExactBorrowedAggregateLeafBuilder::new(
                id,
                aggregate_facts(1, AggregateFieldOptionality::Required),
                AggregateSemanticIdentityV1::All,
                &i64_type,
                None,
                &mut non_boolean_charge,
            )
            .err(),
            Some(AggregatePartialError::InputShape)
        );
        assert_eq!(non_boolean_charge, non_boolean_before);

        for (invalid, expected) in [
            (SegmentV2Cell::Missing, AggregatePartialError::MissingField),
            (SegmentV2Cell::Null, AggregatePartialError::NoValue),
            (
                SegmentV2Cell::Value(CanonicalValue::I64(1)),
                AggregatePartialError::InputShape,
            ),
        ] {
            for (semantic, valid, decisive_before_error) in [
                (AggregateSemanticIdentityV1::Any, false, true),
                (AggregateSemanticIdentityV1::All, true, false),
            ] {
                let valid = [SegmentV2Cell::Value(CanonicalValue::Bool(valid))];
                let invalid = [
                    SegmentV2Cell::Value(CanonicalValue::Bool(decisive_before_error)),
                    invalid.clone(),
                ];
                let mut charge = budget(3, 64);
                let mut builder = ExactBorrowedAggregateLeafBuilder::new(
                    id,
                    aggregate_facts(1, AggregateFieldOptionality::Required),
                    semantic,
                    &bool_type,
                    None,
                    &mut charge,
                )
                .expect("required Boolean leaf");
                builder
                    .accumulate(&valid, &mut charge)
                    .expect("valid Boolean contribution");
                let value_before = builder.value;
                let charge_before = charge;
                assert_eq!(builder.accumulate(&invalid, &mut charge), Err(expected));
                assert_eq!(builder.value, value_before);
                assert_eq!(charge, charge_before);
            }
        }

        let definition = BorrowedAggregateDefinition::new(
            aggregate_facts(1, AggregateFieldOptionality::Optional),
            AggregateSemanticIdentityV1::Min,
            &i64_type,
            None,
        )
        .expect("minimum definition");
        let exact_state = definition
            .state_bytes()
            .expect("minimum state")
            .checked_add(CANONICAL_INVENTORY_IDENTITY_BYTES)
            .expect("small exact state");
        let mut exact_charge = budget(1, exact_state);
        ExactBorrowedAggregateMergeAccumulator::new(
            aggregate_facts(1, AggregateFieldOptionality::Optional),
            AggregateSemanticIdentityV1::Min,
            &i64_type,
            None,
            &[id],
            &mut exact_charge,
        )
        .expect("exact borrowed state budget");
        assert_eq!(exact_charge.remaining_state_bytes(), 0);
        let mut short_charge = budget(1, exact_state - 1);
        let before = short_charge;
        assert_eq!(
            ExactBorrowedAggregateMergeAccumulator::new(
                aggregate_facts(1, AggregateFieldOptionality::Optional),
                AggregateSemanticIdentityV1::Min,
                &i64_type,
                None,
                &[id],
                &mut short_charge,
            )
            .err(),
            Some(AggregatePartialError::StateBoundExceeded)
        );
        assert_eq!(short_charge, before);
    }

    #[test]
    fn extrema_are_partition_and_input_permutation_invariant() {
        let logical_type = SegmentV2LogicalType::I64;
        let mut rng = StdRng::seed_from_u64(0xE7E0_A11A);
        for _ in 0..64 {
            let mut values = (0..rng.gen_range(1..=64))
                .map(|_| rng.gen_range(i64::MIN..=i64::MAX))
                .collect::<Vec<_>>();
            let expected_minimum = *values.iter().min().expect("nonempty values");
            let expected_maximum = *values.iter().max().expect("nonempty values");
            values.shuffle(&mut rng);
            let cells = i64_cells(&values);
            let split = rng.gen_range(0..=cells.len());
            let partitions = [&cells[..split], &cells[split..]];
            for (semantic, expected) in [
                (AggregateSemanticIdentityV1::Min, expected_minimum),
                (AggregateSemanticIdentityV1::Max, expected_maximum),
            ] {
                let actual = merge_borrowed(semantic, &logical_type, &partitions);
                let BorrowedAggregatePartialValue::Extreme(Some(BorrowedAggregateScalar::Value(
                    CanonicalValue::I64(actual),
                ))) = actual
                else {
                    panic!("expected present i64 extreme")
                };
                assert_eq!(*actual, expected);
            }
        }
    }

    #[test]
    fn borrowed_partial_merge_uses_the_sealed_consuming_inventory() {
        let logical_type = SegmentV2LogicalType::I64;
        let first_cells = i64_cells(&[3]);
        let second_cells = i64_cells(&[1]);
        let ids = [identity(0, 1, 0), identity(0, 1, 1)];
        let mut charge = budget(4, 256);
        let mut accumulator = ExactBorrowedAggregateMergeAccumulator::new(
            aggregate_facts(1, AggregateFieldOptionality::Optional),
            AggregateSemanticIdentityV1::Min,
            &logical_type,
            None,
            &ids,
            &mut charge,
        )
        .expect("sealed borrowed inventory");

        let value_before = accumulator.value();
        let cursor_before = accumulator.consumed_leaves();
        let charge_before = charge;
        assert_eq!(
            accumulator.merge_leaf(
                borrowed_leaf(
                    ids[1],
                    AggregateSemanticIdentityV1::Min,
                    &logical_type,
                    &second_cells,
                ),
                &mut charge,
            ),
            Err(AggregatePartialError::InventoryGap)
        );
        assert_eq!(accumulator.value(), value_before);
        assert_eq!(accumulator.consumed_leaves(), cursor_before);
        assert_eq!(charge, charge_before);

        assert_eq!(
            accumulator.merge_leaf(
                borrowed_leaf_with_binding(
                    ids[0],
                    aggregate_binding(2),
                    AggregateSemanticIdentityV1::Min,
                    &logical_type,
                    &first_cells,
                ),
                &mut charge,
            ),
            Err(AggregatePartialError::IncompatibleLeaf)
        );
        assert_eq!(accumulator.value(), value_before);
        assert_eq!(accumulator.consumed_leaves(), cursor_before);
        assert_eq!(charge, charge_before);

        assert_eq!(
            accumulator.merge_leaf(
                borrowed_leaf(
                    ids[0],
                    AggregateSemanticIdentityV1::Max,
                    &logical_type,
                    &first_cells,
                ),
                &mut charge,
            ),
            Err(AggregatePartialError::IncompatibleLeaf)
        );
        assert_eq!(accumulator.value(), value_before);
        assert_eq!(accumulator.consumed_leaves(), cursor_before);
        assert_eq!(charge, charge_before);

        accumulator
            .merge_leaf(
                borrowed_leaf(
                    ids[0],
                    AggregateSemanticIdentityV1::Min,
                    &logical_type,
                    &first_cells,
                ),
                &mut charge,
            )
            .expect("first exact leaf");
        let value_after_first = accumulator.value();
        let charge_after_first = charge;
        assert_eq!(
            accumulator.merge_leaf(
                borrowed_leaf(
                    ids[0],
                    AggregateSemanticIdentityV1::Min,
                    &logical_type,
                    &first_cells,
                ),
                &mut charge,
            ),
            Err(AggregatePartialError::DuplicateOrReorderedLeaf)
        );
        assert_eq!(accumulator.value(), value_after_first);
        assert_eq!(accumulator.consumed_leaves(), 1);
        assert_eq!(charge, charge_after_first);
        accumulator
            .merge_leaf(
                borrowed_leaf(
                    ids[1],
                    AggregateSemanticIdentityV1::Min,
                    &logical_type,
                    &second_cells,
                ),
                &mut charge,
            )
            .expect("second exact leaf");
        assert_eq!(
            accumulator.finish(),
            Ok(BorrowedAggregatePartialValue::Extreme(Some(
                BorrowedAggregateScalar::Value(present(&second_cells[0]))
            )))
        );
    }

    fn distinct_leaf<'view>(
        id: CanonicalPartialIdentity,
        semantic: AggregateSemanticIdentityV1,
        logical_type: &'view SegmentV2LogicalType,
        maximum_distinct: u16,
        cells: &'view [SegmentV2Cell],
    ) -> FinalizedDistinctAggregateLeaf<'view> {
        let mut charge = budget(
            u32::try_from(cells.len().max(1)).expect("small fixture"),
            MAX_AGGREGATE_STATE_BYTES_V1,
        );
        let mut builder = ExactDistinctAggregateLeafBuilder::new(
            id,
            semantic,
            logical_type,
            maximum_distinct,
            &mut charge,
        )
        .expect("distinct leaf");
        builder
            .accumulate(cells, &mut charge)
            .expect("bounded distinct cells");
        builder.finalize()
    }

    fn merge_distinct<'view>(
        semantic: AggregateSemanticIdentityV1,
        logical_type: &'view SegmentV2LogicalType,
        maximum_distinct: u16,
        partitions: &'view [&'view [SegmentV2Cell]],
    ) -> u64 {
        let ids = (0..partitions.len())
            .map(|batch| identity(0, 1, u32::try_from(batch).expect("small fixture")))
            .collect::<Vec<_>>();
        let mut charge = budget(
            MAX_AGGREGATE_ARITHMETIC_OPERATIONS_V1,
            MAX_AGGREGATE_STATE_BYTES_V1,
        );
        let mut accumulator = ExactDistinctAggregateMergeAccumulator::new(
            semantic,
            logical_type,
            maximum_distinct,
            &ids,
            &mut charge,
        )
        .expect("distinct accumulator");
        for (id, cells) in ids.into_iter().zip(partitions.iter().copied()) {
            accumulator
                .merge_leaf(
                    distinct_leaf(id, semantic, logical_type, maximum_distinct, cells),
                    &mut charge,
                )
                .expect("canonical distinct merge");
        }
        accumulator.finish().expect("complete distinct inventory")
    }

    fn independent_distinct_count(cells: &[SegmentV2Cell], present_only: bool) -> u64 {
        let mut members = BTreeSet::new();
        let mut saw_no_value = false;
        for cell in cells {
            match cell {
                SegmentV2Cell::Missing | SegmentV2Cell::Null if present_only => {}
                SegmentV2Cell::Missing | SegmentV2Cell::Null => saw_no_value = true,
                SegmentV2Cell::Value(value) => {
                    members.insert(encode_canonical_value(value).expect("canonical fixture"));
                }
            }
        }
        u64::try_from(members.len() + usize::from(saw_no_value)).expect("small fixture")
    }

    #[test]
    fn bounded_distinct_partials_match_independent_canonical_sets() {
        let logical_type = SegmentV2LogicalType::U64;
        let mut cells = vec![
            SegmentV2Cell::Missing,
            SegmentV2Cell::Null,
            SegmentV2Cell::Value(CanonicalValue::U64(1)),
            SegmentV2Cell::Value(CanonicalValue::U64(2)),
            SegmentV2Cell::Value(CanonicalValue::U64(1)),
            SegmentV2Cell::Value(CanonicalValue::U64(u64::MAX)),
        ];
        let mut rng = StdRng::seed_from_u64(0xD157_1AC7);
        for _ in 0..64 {
            cells.shuffle(&mut rng);
            let first = rng.gen_range(0..=cells.len());
            let second = rng.gen_range(first..=cells.len());
            let partitions = [&cells[..first], &cells[first..second], &cells[second..]];
            for (semantic, present_only) in [
                (AggregateSemanticIdentityV1::CountDistinct, false),
                (AggregateSemanticIdentityV1::CountDistinctPresent, true),
            ] {
                assert_eq!(
                    merge_distinct(semantic, &logical_type, 16, &partitions),
                    independent_distinct_count(&cells, present_only)
                );
            }
        }

        let decimal = DecimalSpec::new(8, 2).expect("decimal spec");
        let usd = CurrencyCode::new("USD").expect("currency");
        for (logical_type, cells) in [
            (
                SegmentV2LogicalType::Decimal(decimal),
                vec![
                    SegmentV2Cell::Value(CanonicalValue::Decimal(
                        Decimal::new(decimal, -1).expect("decimal"),
                    )),
                    SegmentV2Cell::Value(CanonicalValue::Decimal(
                        Decimal::new(decimal, -1).expect("decimal"),
                    )),
                    SegmentV2Cell::Value(CanonicalValue::Decimal(
                        Decimal::new(decimal, 1).expect("decimal"),
                    )),
                ],
            ),
            (
                SegmentV2LogicalType::Money {
                    currency: usd,
                    amount: decimal,
                },
                vec![
                    SegmentV2Cell::Value(CanonicalValue::Money(Money::new(
                        usd,
                        Decimal::new(decimal, -1).expect("amount"),
                    ))),
                    SegmentV2Cell::Value(CanonicalValue::Money(Money::new(
                        usd,
                        Decimal::new(decimal, -1).expect("amount"),
                    ))),
                    SegmentV2Cell::Value(CanonicalValue::Money(Money::new(
                        usd,
                        Decimal::new(decimal, 1).expect("amount"),
                    ))),
                ],
            ),
        ] {
            let partitions = [&cells[..1], &cells[1..]];
            assert_eq!(
                merge_distinct(
                    AggregateSemanticIdentityV1::CountDistinct,
                    &logical_type,
                    3,
                    &partitions,
                ),
                2
            );
        }

        let enum_type = EnumTypeId::new(9).expect("enum type");
        for (logical_type, cells) in [
            (
                SegmentV2LogicalType::Bool,
                vec![
                    SegmentV2Cell::Value(CanonicalValue::Bool(false)),
                    SegmentV2Cell::Value(CanonicalValue::Bool(false)),
                    SegmentV2Cell::Value(CanonicalValue::Bool(true)),
                ],
            ),
            (
                SegmentV2LogicalType::I64,
                i64_cells(&[i64::MIN, i64::MIN, i64::MAX]),
            ),
            (
                SegmentV2LogicalType::String,
                vec![
                    SegmentV2Cell::Value(CanonicalValue::string("a").expect("string")),
                    SegmentV2Cell::Value(CanonicalValue::string("a").expect("string")),
                    SegmentV2Cell::Value(CanonicalValue::string("z").expect("string")),
                ],
            ),
            (
                SegmentV2LogicalType::Bytes,
                vec![
                    SegmentV2Cell::Value(CanonicalValue::bytes([0]).expect("bytes")),
                    SegmentV2Cell::Value(CanonicalValue::bytes([0]).expect("bytes")),
                    SegmentV2Cell::Value(CanonicalValue::bytes([255]).expect("bytes")),
                ],
            ),
            (
                SegmentV2LogicalType::Timestamp,
                vec![
                    SegmentV2Cell::Value(CanonicalValue::Timestamp(
                        Timestamp::new(-1, 0).expect("timestamp"),
                    )),
                    SegmentV2Cell::Value(CanonicalValue::Timestamp(
                        Timestamp::new(-1, 0).expect("timestamp"),
                    )),
                    SegmentV2Cell::Value(CanonicalValue::Timestamp(
                        Timestamp::new(1, 0).expect("timestamp"),
                    )),
                ],
            ),
            (
                SegmentV2LogicalType::Date,
                vec![
                    SegmentV2Cell::Value(CanonicalValue::Date(Date::new(-1))),
                    SegmentV2Cell::Value(CanonicalValue::Date(Date::new(-1))),
                    SegmentV2Cell::Value(CanonicalValue::Date(Date::new(1))),
                ],
            ),
            (
                SegmentV2LogicalType::Uuid,
                vec![
                    SegmentV2Cell::Value(CanonicalValue::Uuid([0; 16])),
                    SegmentV2Cell::Value(CanonicalValue::Uuid([0; 16])),
                    SegmentV2Cell::Value(CanonicalValue::Uuid([255; 16])),
                ],
            ),
            (
                SegmentV2LogicalType::Enum(enum_type),
                vec![
                    SegmentV2Cell::Value(CanonicalValue::Enum {
                        type_id: enum_type,
                        variant_id: EnumVariantId::new(1).expect("variant"),
                    }),
                    SegmentV2Cell::Value(CanonicalValue::Enum {
                        type_id: enum_type,
                        variant_id: EnumVariantId::new(1).expect("variant"),
                    }),
                    SegmentV2Cell::Value(CanonicalValue::Enum {
                        type_id: enum_type,
                        variant_id: EnumVariantId::new(2).expect("variant"),
                    }),
                ],
            ),
        ] {
            let partitions = [&cells[..1], &cells[1..]];
            assert_eq!(
                merge_distinct(
                    AggregateSemanticIdentityV1::CountDistinct,
                    &logical_type,
                    3,
                    &partitions,
                ),
                2
            );
        }
    }

    #[test]
    fn bounded_distinct_limits_allocation_and_failures_are_atomic() {
        let logical_type = SegmentV2LogicalType::U64;
        let id = identity(0, 1, 0);
        for invalid in [0, MAX_AGGREGATE_DISTINCT_VALUES_V1 + 1] {
            let mut charge = budget(1, MAX_AGGREGATE_STATE_BYTES_V1);
            let before = charge;
            assert_eq!(
                ExactDistinctAggregateLeafBuilder::new(
                    id,
                    AggregateSemanticIdentityV1::CountDistinct,
                    &logical_type,
                    invalid,
                    &mut charge,
                )
                .err(),
                Some(AggregatePartialError::InvalidDistinctBound)
            );
            assert_eq!(charge, before);
        }

        let maximum_cells = (0..MAX_AGGREGATE_DISTINCT_VALUES_V1)
            .map(|value| SegmentV2Cell::Value(CanonicalValue::U64(u64::from(value))))
            .collect::<Vec<_>>();
        let allocated_state =
            BoundedDistinctSet::new(usize::from(MAX_AGGREGATE_DISTINCT_VALUES_V1))
                .expect("bounded state probe")
                .actual_state_bytes()
                .expect("actual allocation charge");
        let maximum_operations = u32::from(MAX_AGGREGATE_DISTINCT_VALUES_V1) + 2;
        let mut charge = budget(maximum_operations, allocated_state);
        let mut builder = ExactDistinctAggregateLeafBuilder::new(
            id,
            AggregateSemanticIdentityV1::CountDistinct,
            &logical_type,
            MAX_AGGREGATE_DISTINCT_VALUES_V1,
            &mut charge,
        )
        .expect("maximum distinct builder");
        assert_eq!(builder.state.actual_state_bytes(), Ok(allocated_state));
        assert_eq!(charge.remaining_state_bytes(), 0);
        let member_capacity = builder.state.members.capacity();
        let scratch_capacity = builder.state.scratch.capacity();
        builder
            .accumulate(&maximum_cells, &mut charge)
            .expect("exact maximum distinct values");
        assert_eq!(
            builder.distinct_count(),
            usize::from(MAX_AGGREGATE_DISTINCT_VALUES_V1)
        );
        assert_eq!(builder.state.members.capacity(), member_capacity);
        assert_eq!(builder.state.scratch.capacity(), scratch_capacity);
        assert_eq!(
            charge.remaining_arithmetic_operations(),
            maximum_operations - u32::from(MAX_AGGREGATE_DISTINCT_VALUES_V1)
        );

        let duplicate = [SegmentV2Cell::Value(CanonicalValue::U64(0))];
        builder
            .accumulate(&duplicate, &mut charge)
            .expect("duplicate at the exact distinct ceiling");
        assert_eq!(
            builder.distinct_count(),
            usize::from(MAX_AGGREGATE_DISTINCT_VALUES_V1)
        );
        assert_eq!(charge.remaining_arithmetic_operations(), 1);
        assert_eq!(builder.state.members.capacity(), member_capacity);
        assert_eq!(builder.state.scratch.capacity(), scratch_capacity);

        let extra = [SegmentV2Cell::Value(CanonicalValue::U64(u64::from(
            MAX_AGGREGATE_DISTINCT_VALUES_V1,
        )))];
        let count_before = builder.distinct_count();
        let charge_before = charge;
        assert_eq!(
            builder.accumulate(&extra, &mut charge),
            Err(AggregatePartialError::DistinctBoundExceeded)
        );
        assert_eq!(builder.distinct_count(), count_before);
        assert!(builder.state.scratch.is_empty());
        assert_eq!(builder.state.members.capacity(), member_capacity);
        assert_eq!(builder.state.scratch.capacity(), scratch_capacity);
        assert_eq!(charge, charge_before);

        let wrong_type = [SegmentV2Cell::Value(CanonicalValue::I64(1))];
        assert_eq!(
            builder.accumulate(&wrong_type, &mut charge),
            Err(AggregatePartialError::InputShape)
        );
        assert_eq!(builder.distinct_count(), count_before);
        assert!(builder.state.scratch.is_empty());
        assert_eq!(charge, charge_before);

        let mut short_charge = budget(1, allocated_state - 1);
        let short_before = short_charge;
        assert_eq!(
            ExactDistinctAggregateLeafBuilder::new(
                id,
                AggregateSemanticIdentityV1::CountDistinct,
                &logical_type,
                MAX_AGGREGATE_DISTINCT_VALUES_V1,
                &mut short_charge,
            )
            .err(),
            Some(AggregatePartialError::StateBoundExceeded)
        );
        assert_eq!(short_charge, short_before);

        let one = [SegmentV2Cell::Value(CanonicalValue::U64(7))];
        let exact_one_state = BoundedDistinctSet::new(1)
            .expect("one-value state probe")
            .actual_state_bytes()
            .expect("one-value actual allocation charge");
        let mut exact_operation_charge = budget(1, exact_one_state);
        let mut exact_operation_builder = ExactDistinctAggregateLeafBuilder::new(
            id,
            AggregateSemanticIdentityV1::CountDistinct,
            &logical_type,
            1,
            &mut exact_operation_charge,
        )
        .expect("exact operation builder");
        assert_eq!(exact_operation_charge.remaining_state_bytes(), 0);
        exact_operation_builder
            .accumulate(&one, &mut exact_operation_charge)
            .expect("one exact charged contribution");
        assert_eq!(exact_operation_charge.remaining_arithmetic_operations(), 0);
        let count_before = exact_operation_builder.distinct_count();
        let charge_before = exact_operation_charge;
        assert_eq!(
            exact_operation_builder.accumulate(&one, &mut exact_operation_charge),
            Err(AggregatePartialError::ArithmeticBoundExceeded)
        );
        assert_eq!(exact_operation_builder.distinct_count(), count_before);
        assert_eq!(exact_operation_charge, charge_before);
    }

    #[test]
    fn distinct_merge_bound_and_inventory_refusals_release_no_partial_count() {
        let logical_type = SegmentV2LogicalType::U64;
        let first_cells = [
            SegmentV2Cell::Value(CanonicalValue::U64(1)),
            SegmentV2Cell::Value(CanonicalValue::U64(2)),
        ];
        let second_cells = [SegmentV2Cell::Value(CanonicalValue::U64(3))];
        let ids = [identity(0, 1, 0), identity(0, 1, 1)];
        let exact_merge_state = BoundedDistinctSet::new(2)
            .expect("merge state probe")
            .actual_state_bytes()
            .expect("actual merge allocation charge")
            .checked_add(
                SealedCanonicalPartialInventory::state_bytes(&ids).expect("exact inventory charge"),
            )
            .expect("bounded combined state");
        let mut charge = budget(8, exact_merge_state);
        let mut accumulator = ExactDistinctAggregateMergeAccumulator::new(
            AggregateSemanticIdentityV1::CountDistinct,
            &logical_type,
            2,
            &ids,
            &mut charge,
        )
        .expect("bounded distinct accumulator");
        assert_eq!(charge.remaining_state_bytes(), 0);
        accumulator
            .merge_leaf(
                distinct_leaf(
                    ids[0],
                    AggregateSemanticIdentityV1::CountDistinct,
                    &logical_type,
                    2,
                    &first_cells,
                ),
                &mut charge,
            )
            .expect("first bounded leaf");
        let count_before = accumulator.distinct_count();
        let cursor_before = accumulator.consumed_leaves();
        let charge_before = charge;
        assert_eq!(
            accumulator.merge_leaf(
                distinct_leaf(
                    ids[1],
                    AggregateSemanticIdentityV1::CountDistinct,
                    &logical_type,
                    2,
                    &second_cells,
                ),
                &mut charge,
            ),
            Err(AggregatePartialError::DistinctBoundExceeded)
        );
        assert_eq!(accumulator.distinct_count(), count_before);
        assert_eq!(accumulator.consumed_leaves(), cursor_before);
        assert!(accumulator.state.scratch.is_empty());
        assert_eq!(charge, charge_before);

        assert_eq!(
            accumulator.merge_leaf(
                distinct_leaf(
                    ids[0],
                    AggregateSemanticIdentityV1::CountDistinct,
                    &logical_type,
                    2,
                    &first_cells,
                ),
                &mut charge,
            ),
            Err(AggregatePartialError::DuplicateOrReorderedLeaf)
        );
        assert_eq!(accumulator.distinct_count(), count_before);
        assert_eq!(accumulator.consumed_leaves(), cursor_before);
        assert_eq!(charge, charge_before);

        for (foreign, expected) in [
            (
                identity(1, 1, 0),
                AggregatePartialError::ForeignRootInventory,
            ),
            (identity(0, 9, 0), AggregatePartialError::ForeignSegment),
        ] {
            assert_eq!(
                accumulator.merge_leaf(
                    distinct_leaf(
                        foreign,
                        AggregateSemanticIdentityV1::CountDistinct,
                        &logical_type,
                        2,
                        &second_cells,
                    ),
                    &mut charge,
                ),
                Err(expected)
            );
            assert_eq!(accumulator.distinct_count(), count_before);
            assert_eq!(accumulator.consumed_leaves(), cursor_before);
            assert_eq!(charge, charge_before);
        }

        let mut omission_charge = budget(4, MAX_AGGREGATE_STATE_BYTES_V1);
        let mut omission = ExactDistinctAggregateMergeAccumulator::new(
            AggregateSemanticIdentityV1::CountDistinct,
            &logical_type,
            2,
            &ids,
            &mut omission_charge,
        )
        .expect("omission accumulator");
        omission
            .merge_leaf(
                distinct_leaf(
                    ids[0],
                    AggregateSemanticIdentityV1::CountDistinct,
                    &logical_type,
                    2,
                    &first_cells,
                ),
                &mut omission_charge,
            )
            .expect("first omission leaf");
        assert_eq!(
            omission.finish(),
            Err(AggregatePartialError::InventoryOmission)
        );

        let one_id = [ids[0]];
        let mut excess_charge = budget(4, MAX_AGGREGATE_STATE_BYTES_V1);
        let mut excess = ExactDistinctAggregateMergeAccumulator::new(
            AggregateSemanticIdentityV1::CountDistinct,
            &logical_type,
            2,
            &one_id,
            &mut excess_charge,
        )
        .expect("excess accumulator");
        excess
            .merge_leaf(
                distinct_leaf(
                    ids[0],
                    AggregateSemanticIdentityV1::CountDistinct,
                    &logical_type,
                    2,
                    &first_cells,
                ),
                &mut excess_charge,
            )
            .expect("complete sole leaf");
        let excess_count = excess.distinct_count();
        let excess_cursor = excess.consumed_leaves();
        let excess_charge_before = excess_charge;
        assert_eq!(
            excess.merge_leaf(
                distinct_leaf(
                    ids[1],
                    AggregateSemanticIdentityV1::CountDistinct,
                    &logical_type,
                    2,
                    &second_cells,
                ),
                &mut excess_charge,
            ),
            Err(AggregatePartialError::ExcessLeaf)
        );
        assert_eq!(excess.distinct_count(), excess_count);
        assert_eq!(excess.consumed_leaves(), excess_cursor);
        assert_eq!(excess_charge, excess_charge_before);

        for semantic in [
            AggregateSemanticIdentityV1::CountDistinct,
            AggregateSemanticIdentityV1::CountDistinctPresent,
        ] {
            let mut empty_charge = budget(1, MAX_AGGREGATE_STATE_BYTES_V1);
            let empty = ExactDistinctAggregateMergeAccumulator::new(
                semantic,
                &logical_type,
                1,
                &[],
                &mut empty_charge,
            )
            .expect("empty distinct accumulator");
            assert_eq!(empty.finish(), Ok(0));
        }
    }
}
