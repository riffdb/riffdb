//! Canonical exact predicate and independent-order semantics (ADR-0134).

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::num::NonZeroU32;

use riffdb_types::{
    EXACT_PREDICATE_PROVIDER_STATE_SCHEMA_HASH_V4, FieldId, ProjectionProviderCapabilitiesV1,
    ProjectionProviderDescriptorV1, ProjectionProviderKindV1, ProjectionProviderPolicyModeV1,
    ProjectionProviderPostureV1, ProjectionProviderStateIdentityV1,
    ProjectionProviderStaticBoundsV1, QueryPlanHash, hash_query_plan,
};

/// Canonical exact predicate/order program identity.
pub const EXACT_PREDICATE_PROGRAM_VERSION_V1: u16 = 1;
/// Canonical nullable-order semantic-program identity.
pub const EXACT_PREDICATE_PROGRAM_VERSION_V2: u16 = 2;
/// Maximum predicate leaves in one compiled family member.
pub const MAX_EXACT_PREDICATE_LEAVES_V1: usize = 16;
/// Maximum Boolean nesting in one compiled predicate.
pub const MAX_EXACT_PREDICATE_DEPTH_V1: usize = 4;
/// Maximum children of one conjunction or disjunction.
pub const MAX_EXACT_BOOLEAN_BRANCHES_V1: usize = 4;
/// Maximum values in one canonical membership parameter.
pub const MAX_EXACT_SET_VALUES_V1: usize = 64;
/// Maximum terms in one compiler-owned total order, including key terms.
pub const MAX_EXACT_ORDER_TERMS_V1: usize = 8;
/// Maximum optional-presence combinations in one compiled family.
pub const MAX_EXACT_FAMILY_MEMBERS_V1: usize = 64;
/// Maximum ordinal offset accepted by one compiled family.
pub const MAX_EXACT_OFFSET_V1: u32 = 1_000_000;
/// Maximum returned rows in one exact page.
pub const MAX_EXACT_LIMIT_V1: u16 = 499;
/// Maximum canonical bytes for one semantic program.
pub const MAX_EXACT_PREDICATE_PROGRAM_BYTES_V1: usize = 65_536;

/// Complete statically charged requirement for any physical exact provider.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExactProviderRequirementV1 {
    policy_mode: ProjectionProviderPolicyModeV1,
    max_candidates: u32,
    max_work_units: u64,
    max_state_bytes_per_row: u32,
}

impl ExactProviderRequirementV1 {
    /// Constructs one bounded, policy-scoped provider requirement.
    pub fn new(
        policy_mode: ProjectionProviderPolicyModeV1,
        max_candidates: u32,
        max_work_units: u64,
        max_state_bytes_per_row: u32,
    ) -> Result<Self, ExactPredicateProgramErrorV1> {
        if max_candidates == 0 || max_work_units == 0 || max_state_bytes_per_row == 0 {
            return Err(ExactPredicateProgramErrorV1::BoundExceeded);
        }
        Ok(Self {
            policy_mode,
            max_candidates,
            max_work_units,
            max_state_bytes_per_row,
        })
    }

    /// Policy proof required before the provider forms its candidate universe.
    #[must_use]
    pub const fn policy_mode(self) -> ProjectionProviderPolicyModeV1 {
        self.policy_mode
    }

    /// Maximum rows in one authorized provider partition.
    #[must_use]
    pub const fn max_candidates(self) -> u32 {
        self.max_candidates
    }

    /// Maximum provider-owned work charged to one operation.
    #[must_use]
    pub const fn max_work_units(self) -> u64 {
        self.max_work_units
    }

    /// Maximum rebuildable state amplification per admitted row.
    #[must_use]
    pub const fn max_state_bytes_per_row(self) -> u32 {
        self.max_state_bytes_per_row
    }
}

/// Frozen scalar comparison identity.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ExactComparisonProfileV1 {
    /// Boolean false before true.
    Bool = 1,
    /// Signed 64-bit numeric order.
    I64 = 2,
    /// Unsigned 64-bit numeric order.
    U64 = 3,
    /// Checked coefficient then fixed scale.
    Decimal = 4,
    /// Currency, checked coefficient, then fixed scale.
    Money = 5,
    /// Exact binary UTF-8 byte order.
    BinaryUtf8 = 6,
    /// Exact byte order.
    Bytes = 7,
    /// UTC nanosecond order.
    Timestamp = 8,
    /// Days-since-epoch order.
    Date = 9,
    /// UUID network-byte order.
    Uuid = 10,
    /// Stable enum type then variant identity.
    Enum = 11,
}

/// Closed exact predicate operator identity.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExactPredicateOperatorV1 {
    /// Equality.
    Equal = 1,
    /// Inequality over a present non-null value.
    NotEqual = 2,
    /// Strictly less.
    Less = 3,
    /// Less or equal.
    LessEqual = 4,
    /// Strictly greater.
    Greater = 5,
    /// Greater or equal.
    GreaterEqual = 6,
    /// Membership in one canonical set.
    In = 7,
    /// Non-membership in one canonical set over a present non-null value.
    NotIn = 8,
    /// Exact binary prefix.
    StartsWith = 9,
    /// Exact binary suffix.
    EndsWith = 10,
    /// Exact binary contiguous substring.
    Contains = 11,
    /// Explicit null state.
    IsNull = 12,
    /// Present non-null state.
    IsNotNull = 13,
    /// Present state, including explicit null.
    Exists = 14,
}

impl ExactPredicateOperatorV1 {
    const fn needs_value(self) -> bool {
        !matches!(self, Self::IsNull | Self::IsNotNull | Self::Exists)
    }
}

/// Exact runtime value used by the independent evaluator.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ExactScalarV1 {
    /// Boolean.
    Bool(bool),
    /// Signed integer.
    I64(i64),
    /// Unsigned integer.
    U64(u64),
    /// Fixed-scale decimal.
    Decimal {
        /// Signed fixed-point coefficient.
        coefficient: i128,
        /// Decimal scale.
        scale: u8,
    },
    /// Currency-qualified fixed-scale decimal.
    Money {
        /// ISO-style three-byte currency identity.
        currency: [u8; 3],
        /// Signed fixed-point coefficient.
        coefficient: i128,
        /// Decimal scale.
        scale: u8,
    },
    /// Exact UTF-8.
    String(String),
    /// Exact bytes.
    Bytes(Vec<u8>),
    /// UTC nanoseconds.
    Timestamp(i128),
    /// Days since the Unix epoch.
    Date(i32),
    /// UUID network bytes.
    Uuid([u8; 16]),
    /// Stable enum identity.
    Enum {
        /// Stable enum type identity.
        type_id: u32,
        /// Stable variant identity.
        variant_id: u32,
    },
}

impl ExactScalarV1 {
    /// Frozen comparison identity for this value.
    #[must_use]
    pub const fn profile(&self) -> ExactComparisonProfileV1 {
        match self {
            Self::Bool(_) => ExactComparisonProfileV1::Bool,
            Self::I64(_) => ExactComparisonProfileV1::I64,
            Self::U64(_) => ExactComparisonProfileV1::U64,
            Self::Decimal { .. } => ExactComparisonProfileV1::Decimal,
            Self::Money { .. } => ExactComparisonProfileV1::Money,
            Self::String(_) => ExactComparisonProfileV1::BinaryUtf8,
            Self::Bytes(_) => ExactComparisonProfileV1::Bytes,
            Self::Timestamp(_) => ExactComparisonProfileV1::Timestamp,
            Self::Date(_) => ExactComparisonProfileV1::Date,
            Self::Uuid(_) => ExactComparisonProfileV1::Uuid,
            Self::Enum { .. } => ExactComparisonProfileV1::Enum,
        }
    }
}

/// Compiler-owned right-hand value slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExactValueSlotV1 {
    /// Scalar parameter ordinal.
    Scalar(u16),
    /// Canonical-set parameter ordinal.
    Set(u16),
}

/// One compiler-resolved predicate leaf.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactPredicateLeafV1 {
    field: FieldId,
    operator: ExactPredicateOperatorV1,
    profile: ExactComparisonProfileV1,
    value: Option<ExactValueSlotV1>,
}

impl ExactPredicateLeafV1 {
    /// Constructs one complete leaf.
    pub fn new(
        field: FieldId,
        operator: ExactPredicateOperatorV1,
        profile: ExactComparisonProfileV1,
        value: Option<ExactValueSlotV1>,
    ) -> Result<Self, ExactPredicateProgramErrorV1> {
        if operator.needs_value() != value.is_some()
            || matches!(
                operator,
                ExactPredicateOperatorV1::In | ExactPredicateOperatorV1::NotIn
            ) != matches!(value, Some(ExactValueSlotV1::Set(_)))
            || matches!(
                operator,
                ExactPredicateOperatorV1::StartsWith
                    | ExactPredicateOperatorV1::EndsWith
                    | ExactPredicateOperatorV1::Contains
            ) && profile != ExactComparisonProfileV1::BinaryUtf8
        {
            return Err(ExactPredicateProgramErrorV1::InvalidLeaf);
        }
        Ok(Self {
            field,
            operator,
            profile,
            value,
        })
    }

    /// Compiler-resolved field.
    #[must_use]
    pub const fn field(&self) -> FieldId {
        self.field
    }

    /// Closed operator.
    #[must_use]
    pub const fn operator(&self) -> ExactPredicateOperatorV1 {
        self.operator
    }

    /// Frozen scalar comparison profile.
    #[must_use]
    pub const fn profile(&self) -> ExactComparisonProfileV1 {
        self.profile
    }

    /// Compiler-owned parameter slot, absent only for state predicates.
    #[must_use]
    pub const fn value(&self) -> Option<ExactValueSlotV1> {
        self.value
    }
}

/// Bounded normalized Boolean predicate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExactPredicateNodeV1 {
    /// One typed leaf.
    Leaf(ExactPredicateLeafV1),
    /// All children must be true.
    And(Vec<Self>),
    /// At least one child must be true.
    Or(Vec<Self>),
    /// Child is included only when one compiler-declared optional parameter is present.
    When {
        /// Compiler-declared optional parameter ordinal.
        presence_ordinal: u8,
        /// Predicate enabled when that parameter is present.
        child: Box<Self>,
    },
}

/// Fixed order direction.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExactOrderDirectionV1 {
    /// Ascending.
    Ascending = 1,
    /// Descending.
    Descending = 2,
}

/// One compiler-owned total-order term.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExactOrderTermV1 {
    field: FieldId,
    profile: ExactComparisonProfileV1,
    direction: ExactOrderDirectionV1,
    key_tie_breaker: bool,
}

impl ExactOrderTermV1 {
    /// Constructs one present/non-null order term.
    #[must_use]
    pub const fn new(
        field: FieldId,
        profile: ExactComparisonProfileV1,
        direction: ExactOrderDirectionV1,
        key_tie_breaker: bool,
    ) -> Self {
        Self {
            field,
            profile,
            direction,
            key_tie_breaker,
        }
    }

    /// Compiler-resolved field.
    #[must_use]
    pub const fn field(self) -> FieldId {
        self.field
    }

    /// Frozen scalar comparison profile.
    #[must_use]
    pub const fn profile(self) -> ExactComparisonProfileV1 {
        self.profile
    }

    /// Fixed direction for this term.
    #[must_use]
    pub const fn direction(self) -> ExactOrderDirectionV1 {
        self.direction
    }

    /// Whether this term belongs to the complete ascending key suffix.
    #[must_use]
    pub const fn is_key_tie_breaker(self) -> bool {
        self.key_tie_breaker
    }
}

/// One independent complete total order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactOrderProgramV1 {
    terms: Vec<ExactOrderTermV1>,
}

impl ExactOrderProgramV1 {
    /// Constructs a total order ending in one or more ascending entity-key terms.
    pub fn new(terms: Vec<ExactOrderTermV1>) -> Result<Self, ExactPredicateProgramErrorV1> {
        if terms.is_empty() || terms.len() > MAX_EXACT_ORDER_TERMS_V1 {
            return Err(ExactPredicateProgramErrorV1::InvalidOrder);
        }
        let first_key = terms
            .iter()
            .position(|term| term.key_tie_breaker)
            .ok_or(ExactPredicateProgramErrorV1::InvalidOrder)?;
        if terms[first_key..]
            .iter()
            .any(|term| !term.key_tie_breaker || term.direction != ExactOrderDirectionV1::Ascending)
            || terms[..first_key].iter().any(|term| term.key_tie_breaker)
        {
            return Err(ExactPredicateProgramErrorV1::InvalidOrder);
        }
        Ok(Self { terms })
    }

    /// Compiler-resolved complete total-order terms.
    #[must_use]
    pub fn terms(&self) -> &[ExactOrderTermV1] {
        &self.terms
    }
}

/// Closed placement of the shared missing/null order class.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExactStatePlacementV1 {
    /// Every admitted row is compiler-proven present and non-null.
    PresentOnlyV1 = 1,
    /// Missing and explicit null precede every present value.
    NullsFirstV1 = 2,
    /// Missing and explicit null follow every present value.
    NullsLastV1 = 3,
}

/// One compiler-owned total-order term with explicit state placement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExactOrderTermV2 {
    field: FieldId,
    profile: ExactComparisonProfileV1,
    direction: ExactOrderDirectionV1,
    placement: ExactStatePlacementV1,
    key_tie_breaker: bool,
}

impl ExactOrderTermV2 {
    /// Constructs one state-aware order term.
    #[must_use]
    pub const fn new(
        field: FieldId,
        profile: ExactComparisonProfileV1,
        direction: ExactOrderDirectionV1,
        placement: ExactStatePlacementV1,
        key_tie_breaker: bool,
    ) -> Self {
        Self {
            field,
            profile,
            direction,
            placement,
            key_tie_breaker,
        }
    }

    /// Compiler-resolved field.
    #[must_use]
    pub const fn field(self) -> FieldId {
        self.field
    }

    /// Frozen scalar comparison profile.
    #[must_use]
    pub const fn profile(self) -> ExactComparisonProfileV1 {
        self.profile
    }

    /// Fixed direction for present-value comparison.
    #[must_use]
    pub const fn direction(self) -> ExactOrderDirectionV1 {
        self.direction
    }

    /// Closed state placement.
    #[must_use]
    pub const fn placement(self) -> ExactStatePlacementV1 {
        self.placement
    }

    /// Whether this term belongs to the complete ascending key suffix.
    #[must_use]
    pub const fn is_key_tie_breaker(self) -> bool {
        self.key_tie_breaker
    }
}

/// One independent nullable-aware complete total order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactOrderProgramV2 {
    terms: Vec<ExactOrderTermV2>,
}

impl ExactOrderProgramV2 {
    /// Constructs a total order ending in present-only ascending entity-key terms.
    pub fn new(terms: Vec<ExactOrderTermV2>) -> Result<Self, ExactPredicateProgramErrorV1> {
        if terms.is_empty() || terms.len() > MAX_EXACT_ORDER_TERMS_V1 {
            return Err(ExactPredicateProgramErrorV1::InvalidOrder);
        }
        let first_key = terms
            .iter()
            .position(|term| term.key_tie_breaker)
            .ok_or(ExactPredicateProgramErrorV1::InvalidOrder)?;
        if terms[first_key..].iter().any(|term| {
            !term.key_tie_breaker
                || term.direction != ExactOrderDirectionV1::Ascending
                || term.placement != ExactStatePlacementV1::PresentOnlyV1
        }) || terms[..first_key].iter().any(|term| term.key_tie_breaker)
        {
            return Err(ExactPredicateProgramErrorV1::InvalidOrder);
        }
        Ok(Self { terms })
    }

    /// Compiler-resolved complete total-order terms.
    #[must_use]
    pub fn terms(&self) -> &[ExactOrderTermV2] {
        &self.terms
    }
}

/// One finite optional-presence/order family member.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExactPredicateFamilyMemberV1 {
    presence_bits: u64,
    order_ordinal: u8,
}

impl ExactPredicateFamilyMemberV1 {
    /// Canonical optional-presence bitset.
    #[must_use]
    pub const fn presence_bits(self) -> u64 {
        self.presence_bits
    }

    /// Compiler-owned order ordinal.
    #[must_use]
    pub const fn order_ordinal(self) -> u8 {
        self.order_ordinal
    }
}

/// Canonical semantic family, independent of every physical provider.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactPredicateProgramV1 {
    predicate: ExactPredicateNodeV1,
    orders: Vec<ExactOrderProgramV1>,
    presence_parameter_count: u8,
    exact_count: bool,
    max_offset: u32,
    max_limit: u16,
    provider_requirement: ExactProviderRequirementV1,
    members: Vec<ExactPredicateFamilyMemberV1>,
    canonical_bytes: Vec<u8>,
    identity: QueryPlanHash,
}

impl ExactPredicateProgramV1 {
    /// Validates all finite bounds and constructs every presence/order member.
    pub fn new(
        predicate: ExactPredicateNodeV1,
        orders: Vec<ExactOrderProgramV1>,
        presence_parameter_count: u8,
        exact_count: bool,
        max_offset: u32,
        max_limit: u16,
        provider_requirement: ExactProviderRequirementV1,
    ) -> Result<Self, ExactPredicateProgramErrorV1> {
        let (leaves, depth) = predicate_shape(&predicate)?;
        if leaves == 0
            || leaves > MAX_EXACT_PREDICATE_LEAVES_V1
            || depth > MAX_EXACT_PREDICATE_DEPTH_V1
            || orders.is_empty()
            || orders.len() > u8::MAX as usize
            || max_offset > MAX_EXACT_OFFSET_V1
            || max_limit == 0
            || max_limit > MAX_EXACT_LIMIT_V1
            || u32::from(max_limit) > provider_requirement.max_candidates
        {
            return Err(ExactPredicateProgramErrorV1::BoundExceeded);
        }
        validate_presence_ordinals(&predicate, presence_parameter_count)?;
        let combinations = 1usize
            .checked_shl(u32::from(presence_parameter_count))
            .ok_or(ExactPredicateProgramErrorV1::BoundExceeded)?;
        let member_count = combinations
            .checked_mul(orders.len())
            .filter(|count| *count <= MAX_EXACT_FAMILY_MEMBERS_V1)
            .ok_or(ExactPredicateProgramErrorV1::BoundExceeded)?;
        let mut members = Vec::with_capacity(member_count);
        for bits in 0..combinations {
            for order in 0..orders.len() {
                members.push(ExactPredicateFamilyMemberV1 {
                    presence_bits: bits as u64,
                    order_ordinal: order as u8,
                });
            }
        }
        let canonical_bytes = encode_program(
            &predicate,
            &orders,
            presence_parameter_count,
            exact_count,
            max_offset,
            max_limit,
            provider_requirement,
        )?;
        let identity = hash_query_plan(&canonical_bytes);
        Ok(Self {
            predicate,
            orders,
            presence_parameter_count,
            exact_count,
            max_offset,
            max_limit,
            provider_requirement,
            members,
            canonical_bytes,
            identity,
        })
    }

    /// Strictly decodes and reproduces one canonical program.
    pub fn from_canonical_bytes(bytes: &[u8]) -> Result<Self, ExactPredicateProgramErrorV1> {
        if bytes.len() > MAX_EXACT_PREDICATE_PROGRAM_BYTES_V1 {
            return Err(ExactPredicateProgramErrorV1::BoundExceeded);
        }
        let mut reader = Reader::new(bytes);
        reader.exact(b"REPF")?;
        if reader.u16()? != EXACT_PREDICATE_PROGRAM_VERSION_V1 {
            return Err(ExactPredicateProgramErrorV1::InvalidEncoding);
        }
        let presence = reader.u8()?;
        let exact_count = match reader.u8()? {
            0 => false,
            1 => true,
            _ => return Err(ExactPredicateProgramErrorV1::InvalidEncoding),
        };
        let max_offset = reader.u32()?;
        let max_limit = reader.u16()?;
        let policy_mode = match reader.u8()? {
            1 => ProjectionProviderPolicyModeV1::PartitionAligned,
            2 => ProjectionProviderPolicyModeV1::PolicySubpartition,
            3 => ProjectionProviderPolicyModeV1::BoundedRowAdmission,
            _ => return Err(ExactPredicateProgramErrorV1::InvalidEncoding),
        };
        let provider_requirement = ExactProviderRequirementV1::new(
            policy_mode,
            reader.u32()?,
            reader.u64()?,
            reader.u32()?,
        )?;
        let predicate = decode_node(&mut reader, 0)?;
        let order_count = usize::from(reader.u8()?);
        let mut orders = Vec::with_capacity(order_count);
        for _ in 0..order_count {
            let term_count = usize::from(reader.u8()?);
            let mut terms = Vec::with_capacity(term_count);
            for _ in 0..term_count {
                let field = FieldId::new(reader.u32()?)
                    .ok_or(ExactPredicateProgramErrorV1::InvalidEncoding)?;
                let profile = decode_profile(reader.u8()?)?;
                let direction = match reader.u8()? {
                    1 => ExactOrderDirectionV1::Ascending,
                    2 => ExactOrderDirectionV1::Descending,
                    _ => return Err(ExactPredicateProgramErrorV1::InvalidEncoding),
                };
                let key = match reader.u8()? {
                    0 => false,
                    1 => true,
                    _ => return Err(ExactPredicateProgramErrorV1::InvalidEncoding),
                };
                terms.push(ExactOrderTermV1::new(field, profile, direction, key));
            }
            orders.push(ExactOrderProgramV1::new(terms)?);
        }
        reader.finish()?;
        let program = Self::new(
            predicate,
            orders,
            presence,
            exact_count,
            max_offset,
            max_limit,
            provider_requirement,
        )?;
        if program.canonical_bytes != bytes {
            return Err(ExactPredicateProgramErrorV1::InvalidEncoding);
        }
        Ok(program)
    }

    /// Canonical bytes included in plan/module identity.
    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    /// Domain-separated semantic identity.
    #[must_use]
    pub const fn identity(&self) -> QueryPlanHash {
        self.identity
    }

    /// Finite compiler-enumerated members.
    #[must_use]
    pub fn members(&self) -> &[ExactPredicateFamilyMemberV1] {
        &self.members
    }

    /// Independent order programs.
    #[must_use]
    pub fn orders(&self) -> &[ExactOrderProgramV1] {
        &self.orders
    }

    /// Complete normalized predicate tree.
    #[must_use]
    pub const fn predicate(&self) -> &ExactPredicateNodeV1 {
        &self.predicate
    }

    /// Number of compiler-declared optional-presence bits.
    #[must_use]
    pub const fn presence_parameter_count(&self) -> u8 {
        self.presence_parameter_count
    }

    /// Whether execution returns the whole-result cardinality.
    #[must_use]
    pub const fn exact_count(&self) -> bool {
        self.exact_count
    }

    /// Maximum zero-based ordinal accepted by this program.
    #[must_use]
    pub const fn max_offset(&self) -> u32 {
        self.max_offset
    }

    /// Maximum page rows accepted by this program.
    #[must_use]
    pub const fn max_limit(&self) -> u16 {
        self.max_limit
    }

    /// Deterministically derives the additive V4 physical-provider contract.
    pub fn provider_descriptor(
        &self,
    ) -> Result<ProjectionProviderDescriptorV1, ExactPredicateProgramErrorV1> {
        ProjectionProviderDescriptorV1::new(
            ProjectionProviderKindV1::ExactText,
            ProjectionProviderPostureV1::Exact,
            ProjectionProviderCapabilitiesV1::CANDIDATE
                | ProjectionProviderCapabilitiesV1::FILTER
                | ProjectionProviderCapabilitiesV1::ORDER
                | ProjectionProviderCapabilitiesV1::MEASURE
                | ProjectionProviderCapabilitiesV1::WINDOW
                | ProjectionProviderCapabilitiesV1::OUTPUT,
            self.provider_requirement.policy_mode,
            ProjectionProviderStaticBoundsV1 {
                max_candidates: self.provider_requirement.max_candidates,
                max_output_rows: u32::from(self.max_limit),
                max_measures: u16::from(self.exact_count),
                max_input_bytes: MAX_EXACT_PREDICATE_PROGRAM_BYTES_V1 as u32,
                max_work_units: self.provider_requirement.max_work_units,
                max_state_bytes_per_row: self.provider_requirement.max_state_bytes_per_row,
                max_diagnostic_bytes: 4_096,
                retained_epochs: 8_192,
                max_catchup_lag: 100,
                max_epoch_lease_steps: 1_024,
            },
            ProjectionProviderStateIdentityV1::new(
                NonZeroU32::new(4).expect("fixed provider layout is nonzero"),
                EXACT_PREDICATE_PROVIDER_STATE_SCHEMA_HASH_V4,
            ),
        )
        .map_err(|_| ExactPredicateProgramErrorV1::InvalidProviderRequirement)
    }

    /// Complete provider capability and amplification requirement.
    #[must_use]
    pub const fn provider_requirement(&self) -> ExactProviderRequirementV1 {
        self.provider_requirement
    }

    /// Executes the independent bounded reference model.
    pub fn evaluate_reference(
        &self,
        rows: &[ExactReferenceRowV1],
        parameters: &BTreeMap<u16, ExactParameterValueV1>,
        member: ExactPredicateFamilyMemberV1,
        offset: u32,
        limit: u16,
    ) -> Result<ExactReferenceResultV1, ExactPredicateProgramErrorV1> {
        if !self.members.contains(&member)
            || offset > self.max_offset
            || limit == 0
            || limit > self.max_limit
        {
            return Err(ExactPredicateProgramErrorV1::InvalidWindow);
        }
        let order = self
            .orders
            .get(usize::from(member.order_ordinal))
            .ok_or(ExactPredicateProgramErrorV1::InvalidOrder)?;
        let mut matched = Vec::new();
        for row in rows {
            if evaluate_node(&self.predicate, row, parameters, member.presence_bits)? {
                matched.push(row);
            }
        }
        for row in &matched {
            validate_order_row(row, order)?;
        }
        matched.sort_by(|left, right| compare_rows(left, right, order));
        let total = u64::try_from(matched.len())
            .map_err(|_| ExactPredicateProgramErrorV1::BoundExceeded)?;
        let start = usize::try_from(offset)
            .map_err(|_| ExactPredicateProgramErrorV1::InvalidWindow)?
            .min(matched.len());
        let end = start.saturating_add(usize::from(limit)).min(matched.len());
        Ok(ExactReferenceResultV1 {
            total: self.exact_count.then_some(total),
            entity_keys: matched[start..end]
                .iter()
                .map(|row| row.entity_key.clone())
                .collect(),
        })
    }
}

/// Canonical nullable-order semantic family, independent of every physical provider.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactPredicateProgramV2 {
    base: ExactPredicateProgramV1,
    orders: Vec<ExactOrderProgramV2>,
    canonical_bytes: Vec<u8>,
    identity: QueryPlanHash,
}

impl ExactPredicateProgramV2 {
    /// Validates all finite bounds and seals explicit state placement into V2 bytes.
    pub fn new(
        predicate: ExactPredicateNodeV1,
        orders: Vec<ExactOrderProgramV2>,
        presence_parameter_count: u8,
        exact_count: bool,
        max_offset: u32,
        max_limit: u16,
        provider_requirement: ExactProviderRequirementV1,
    ) -> Result<Self, ExactPredicateProgramErrorV1> {
        if orders.is_empty() || orders.len() > u8::MAX as usize {
            return Err(ExactPredicateProgramErrorV1::InvalidOrder);
        }
        let compatibility_orders = orders
            .iter()
            .map(|order| {
                ExactOrderProgramV1::new(
                    order
                        .terms()
                        .iter()
                        .map(|term| {
                            ExactOrderTermV1::new(
                                term.field,
                                term.profile,
                                term.direction,
                                term.key_tie_breaker,
                            )
                        })
                        .collect(),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let base = ExactPredicateProgramV1::new(
            predicate,
            compatibility_orders,
            presence_parameter_count,
            exact_count,
            max_offset,
            max_limit,
            provider_requirement,
        )?;
        let canonical_bytes = encode_nullable_program(&base, &orders)?;
        let identity = hash_query_plan(&canonical_bytes);
        Ok(Self {
            base,
            orders,
            canonical_bytes,
            identity,
        })
    }

    /// Strictly decodes and reproduces one canonical nullable-order program.
    pub fn from_canonical_bytes(bytes: &[u8]) -> Result<Self, ExactPredicateProgramErrorV1> {
        if bytes.len() > MAX_EXACT_PREDICATE_PROGRAM_BYTES_V1 {
            return Err(ExactPredicateProgramErrorV1::BoundExceeded);
        }
        let mut reader = Reader::new(bytes);
        reader.exact(b"REPN")?;
        if reader.u16()? != EXACT_PREDICATE_PROGRAM_VERSION_V2 {
            return Err(ExactPredicateProgramErrorV1::InvalidEncoding);
        }
        let base_len = usize::try_from(reader.u32()?)
            .map_err(|_| ExactPredicateProgramErrorV1::InvalidEncoding)?;
        let base = ExactPredicateProgramV1::from_canonical_bytes(reader.take(base_len)?)?;
        let order_count = usize::from(reader.u8()?);
        let mut orders = Vec::with_capacity(order_count);
        for _ in 0..order_count {
            let term_count = usize::from(reader.u8()?);
            let mut terms = Vec::with_capacity(term_count);
            for _ in 0..term_count {
                let field = FieldId::new(reader.u32()?)
                    .ok_or(ExactPredicateProgramErrorV1::InvalidEncoding)?;
                let profile = decode_profile(reader.u8()?)?;
                let direction = match reader.u8()? {
                    1 => ExactOrderDirectionV1::Ascending,
                    2 => ExactOrderDirectionV1::Descending,
                    _ => return Err(ExactPredicateProgramErrorV1::InvalidEncoding),
                };
                let placement = match reader.u8()? {
                    1 => ExactStatePlacementV1::PresentOnlyV1,
                    2 => ExactStatePlacementV1::NullsFirstV1,
                    3 => ExactStatePlacementV1::NullsLastV1,
                    _ => return Err(ExactPredicateProgramErrorV1::InvalidEncoding),
                };
                let key = match reader.u8()? {
                    0 => false,
                    1 => true,
                    _ => return Err(ExactPredicateProgramErrorV1::InvalidEncoding),
                };
                terms.push(ExactOrderTermV2::new(
                    field, profile, direction, placement, key,
                ));
            }
            orders.push(ExactOrderProgramV2::new(terms)?);
        }
        reader.finish()?;
        let program = Self::new(
            base.predicate.clone(),
            orders,
            base.presence_parameter_count,
            base.exact_count,
            base.max_offset,
            base.max_limit,
            base.provider_requirement,
        )?;
        if program.canonical_bytes != bytes {
            return Err(ExactPredicateProgramErrorV1::InvalidEncoding);
        }
        Ok(program)
    }

    /// Canonical bytes included in plan and module identity.
    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    /// Domain-separated semantic identity.
    #[must_use]
    pub const fn identity(&self) -> QueryPlanHash {
        self.identity
    }

    /// Finite compiler-enumerated members.
    #[must_use]
    pub fn members(&self) -> &[ExactPredicateFamilyMemberV1] {
        self.base.members()
    }

    /// Independent state-aware order programs.
    #[must_use]
    pub fn orders(&self) -> &[ExactOrderProgramV2] {
        &self.orders
    }

    /// Complete normalized predicate tree.
    #[must_use]
    pub const fn predicate(&self) -> &ExactPredicateNodeV1 {
        self.base.predicate()
    }

    /// Number of compiler-declared optional-presence bits.
    #[must_use]
    pub const fn presence_parameter_count(&self) -> u8 {
        self.base.presence_parameter_count()
    }

    /// Whether execution returns the whole-result cardinality.
    #[must_use]
    pub const fn exact_count(&self) -> bool {
        self.base.exact_count()
    }

    /// Maximum zero-based ordinal accepted by this program.
    #[must_use]
    pub const fn max_offset(&self) -> u32 {
        self.base.max_offset()
    }

    /// Maximum page rows accepted by this program.
    #[must_use]
    pub const fn max_limit(&self) -> u16 {
        self.base.max_limit()
    }

    /// Complete provider capability and amplification requirement.
    #[must_use]
    pub const fn provider_requirement(&self) -> ExactProviderRequirementV1 {
        self.base.provider_requirement()
    }

    /// Executes the independent bounded nullable-order reference model.
    pub fn evaluate_reference(
        &self,
        rows: &[ExactReferenceRowV1],
        parameters: &BTreeMap<u16, ExactParameterValueV1>,
        member: ExactPredicateFamilyMemberV1,
        offset: u32,
        limit: u16,
    ) -> Result<ExactReferenceResultV1, ExactPredicateProgramErrorV1> {
        if !self.members().contains(&member)
            || offset > self.max_offset()
            || limit == 0
            || limit > self.max_limit()
        {
            return Err(ExactPredicateProgramErrorV1::InvalidWindow);
        }
        let order = self
            .orders
            .get(usize::from(member.order_ordinal))
            .ok_or(ExactPredicateProgramErrorV1::InvalidOrder)?;
        let mut matched = Vec::new();
        for row in rows {
            if evaluate_node(self.base.predicate(), row, parameters, member.presence_bits)? {
                validate_nullable_order_row(row, order)?;
                matched.push(row);
            }
        }
        matched.sort_by(|left, right| compare_nullable_rows(left, right, order));
        let total = u64::try_from(matched.len())
            .map_err(|_| ExactPredicateProgramErrorV1::BoundExceeded)?;
        let start = usize::try_from(offset)
            .map_err(|_| ExactPredicateProgramErrorV1::InvalidWindow)?
            .min(matched.len());
        let end = start.saturating_add(usize::from(limit)).min(matched.len());
        Ok(ExactReferenceResultV1 {
            total: self.exact_count().then_some(total),
            entity_keys: matched[start..end]
                .iter()
                .map(|row| row.entity_key.clone())
                .collect(),
        })
    }
}

/// Missing/null/value state used by the independent evaluator.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExactReferenceCellV1 {
    /// Field is absent from the row version.
    Missing,
    /// Field is explicitly null.
    Null,
    /// Present non-null scalar.
    Value(ExactScalarV1),
}

/// One reference row keyed independently from its fields.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactReferenceRowV1 {
    /// Canonical complete entity key.
    pub entity_key: Vec<ExactScalarV1>,
    /// Compiler-addressed field states.
    pub fields: BTreeMap<FieldId, ExactReferenceCellV1>,
}

/// One bound scalar or canonical set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExactParameterValueV1 {
    /// Scalar parameter.
    Scalar(ExactScalarV1),
    /// Sorted, duplicate-free membership set.
    Set(Vec<ExactScalarV1>),
}

impl ExactParameterValueV1 {
    /// Sorts and deduplicates a bounded set before hashing or evaluation.
    pub fn canonical_set(
        mut values: Vec<ExactScalarV1>,
    ) -> Result<Self, ExactPredicateProgramErrorV1> {
        if values.len() > MAX_EXACT_SET_VALUES_V1 {
            return Err(ExactPredicateProgramErrorV1::BoundExceeded);
        }
        if values.first().is_some_and(|first| {
            values
                .iter()
                .any(|value| value.profile() != first.profile())
        }) {
            return Err(ExactPredicateProgramErrorV1::TypeMismatch);
        }
        values.sort();
        values.dedup();
        Ok(Self::Set(values))
    }
}

/// Independent page and optional complete count.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactReferenceResultV1 {
    /// Complete matching population when requested.
    pub total: Option<u64>,
    /// Entity keys in the selected independent total order.
    pub entity_keys: Vec<Vec<ExactScalarV1>>,
}

/// Closed semantic-program failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExactPredicateProgramErrorV1 {
    /// Leaf operator/profile/slot mismatch.
    InvalidLeaf,
    /// Boolean, family, set, order, or artifact bound exceeded.
    BoundExceeded,
    /// Total order is incomplete or malformed.
    InvalidOrder,
    /// Runtime parameter or row profile is inconsistent with the plan.
    TypeMismatch,
    /// Member, offset, or limit is outside compiled bounds.
    InvalidWindow,
    /// Bytes are malformed, unknown, trailing, or noncanonical.
    InvalidEncoding,
    /// Compiler-owned physical provider descriptor is inconsistent.
    InvalidProviderRequirement,
}

impl fmt::Display for ExactPredicateProgramErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid exact predicate program: {self:?}")
    }
}

impl Error for ExactPredicateProgramErrorV1 {}

fn predicate_shape(
    node: &ExactPredicateNodeV1,
) -> Result<(usize, usize), ExactPredicateProgramErrorV1> {
    match node {
        ExactPredicateNodeV1::Leaf(_) => Ok((1, 1)),
        ExactPredicateNodeV1::When { child, .. } => {
            let (leaves, depth) = predicate_shape(child)?;
            Ok((leaves, depth + 1))
        }
        ExactPredicateNodeV1::And(children) | ExactPredicateNodeV1::Or(children) => {
            if children.len() < 2 || children.len() > MAX_EXACT_BOOLEAN_BRANCHES_V1 {
                return Err(ExactPredicateProgramErrorV1::BoundExceeded);
            }
            let mut leaves = 0usize;
            let mut depth = 0usize;
            for child in children {
                let (child_leaves, child_depth) = predicate_shape(child)?;
                leaves = leaves
                    .checked_add(child_leaves)
                    .ok_or(ExactPredicateProgramErrorV1::BoundExceeded)?;
                depth = depth.max(child_depth);
            }
            Ok((leaves, depth + 1))
        }
    }
}

fn validate_presence_ordinals(
    node: &ExactPredicateNodeV1,
    count: u8,
) -> Result<(), ExactPredicateProgramErrorV1> {
    match node {
        ExactPredicateNodeV1::Leaf(_) => Ok(()),
        ExactPredicateNodeV1::When {
            presence_ordinal,
            child,
        } => {
            if *presence_ordinal >= count {
                return Err(ExactPredicateProgramErrorV1::InvalidLeaf);
            }
            validate_presence_ordinals(child, count)
        }
        ExactPredicateNodeV1::And(children) | ExactPredicateNodeV1::Or(children) => {
            for child in children {
                validate_presence_ordinals(child, count)?;
            }
            Ok(())
        }
    }
}

fn evaluate_node(
    node: &ExactPredicateNodeV1,
    row: &ExactReferenceRowV1,
    parameters: &BTreeMap<u16, ExactParameterValueV1>,
    presence: u64,
) -> Result<bool, ExactPredicateProgramErrorV1> {
    match node {
        ExactPredicateNodeV1::Leaf(leaf) => evaluate_leaf(leaf, row, parameters),
        ExactPredicateNodeV1::And(children) => {
            for child in children {
                if !evaluate_node(child, row, parameters, presence)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        ExactPredicateNodeV1::Or(children) => {
            for child in children {
                if evaluate_node(child, row, parameters, presence)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        ExactPredicateNodeV1::When {
            presence_ordinal,
            child,
        } => {
            if presence & (1_u64 << presence_ordinal) == 0 {
                Ok(true)
            } else {
                evaluate_node(child, row, parameters, presence)
            }
        }
    }
}

fn evaluate_leaf(
    leaf: &ExactPredicateLeafV1,
    row: &ExactReferenceRowV1,
    parameters: &BTreeMap<u16, ExactParameterValueV1>,
) -> Result<bool, ExactPredicateProgramErrorV1> {
    let cell = row
        .fields
        .get(&leaf.field)
        .unwrap_or(&ExactReferenceCellV1::Missing);
    match leaf.operator {
        ExactPredicateOperatorV1::IsNull => return Ok(matches!(cell, ExactReferenceCellV1::Null)),
        ExactPredicateOperatorV1::IsNotNull => {
            return Ok(matches!(cell, ExactReferenceCellV1::Value(_)));
        }
        ExactPredicateOperatorV1::Exists => {
            return Ok(!matches!(cell, ExactReferenceCellV1::Missing));
        }
        _ => {}
    }
    let ExactReferenceCellV1::Value(actual) = cell else {
        return Ok(false);
    };
    if actual.profile() != leaf.profile {
        return Err(ExactPredicateProgramErrorV1::TypeMismatch);
    }
    let slot = leaf
        .value
        .ok_or(ExactPredicateProgramErrorV1::InvalidLeaf)?;
    let parameter = parameters
        .get(&match slot {
            ExactValueSlotV1::Scalar(value) | ExactValueSlotV1::Set(value) => value,
        })
        .ok_or(ExactPredicateProgramErrorV1::TypeMismatch)?;
    match (leaf.operator, parameter) {
        (ExactPredicateOperatorV1::In, ExactParameterValueV1::Set(values)) => {
            validate_set_profile(values, leaf.profile)?;
            Ok(values.binary_search(actual).is_ok())
        }
        (ExactPredicateOperatorV1::NotIn, ExactParameterValueV1::Set(values)) => {
            validate_set_profile(values, leaf.profile)?;
            Ok(values.binary_search(actual).is_err())
        }
        (operator, ExactParameterValueV1::Scalar(expected)) => {
            if expected.profile() != leaf.profile {
                return Err(ExactPredicateProgramErrorV1::TypeMismatch);
            }
            let ordering = actual.cmp(expected);
            Ok(match operator {
                ExactPredicateOperatorV1::Equal => ordering == Ordering::Equal,
                ExactPredicateOperatorV1::NotEqual => ordering != Ordering::Equal,
                ExactPredicateOperatorV1::Less => ordering == Ordering::Less,
                ExactPredicateOperatorV1::LessEqual => ordering != Ordering::Greater,
                ExactPredicateOperatorV1::Greater => ordering == Ordering::Greater,
                ExactPredicateOperatorV1::GreaterEqual => ordering != Ordering::Less,
                ExactPredicateOperatorV1::StartsWith
                | ExactPredicateOperatorV1::EndsWith
                | ExactPredicateOperatorV1::Contains => text_pair(actual, expected, operator)?,
                _ => return Err(ExactPredicateProgramErrorV1::TypeMismatch),
            })
        }
        _ => Err(ExactPredicateProgramErrorV1::TypeMismatch),
    }
}

fn validate_set_profile(
    values: &[ExactScalarV1],
    expected: ExactComparisonProfileV1,
) -> Result<(), ExactPredicateProgramErrorV1> {
    if values
        .first()
        .is_some_and(|value| value.profile() != expected)
    {
        return Err(ExactPredicateProgramErrorV1::TypeMismatch);
    }
    Ok(())
}

fn text_pair(
    actual: &ExactScalarV1,
    expected: &ExactScalarV1,
    operator: ExactPredicateOperatorV1,
) -> Result<bool, ExactPredicateProgramErrorV1> {
    let (ExactScalarV1::String(actual), ExactScalarV1::String(expected)) = (actual, expected)
    else {
        return Err(ExactPredicateProgramErrorV1::TypeMismatch);
    };
    if expected.is_empty() {
        return Err(ExactPredicateProgramErrorV1::TypeMismatch);
    }
    Ok(match operator {
        ExactPredicateOperatorV1::StartsWith => actual.starts_with(expected),
        ExactPredicateOperatorV1::EndsWith => actual.ends_with(expected),
        ExactPredicateOperatorV1::Contains => actual.contains(expected),
        _ => return Err(ExactPredicateProgramErrorV1::TypeMismatch),
    })
}

fn validate_order_row(
    row: &ExactReferenceRowV1,
    order: &ExactOrderProgramV1,
) -> Result<(), ExactPredicateProgramErrorV1> {
    for term in &order.terms {
        let Some(ExactReferenceCellV1::Value(value)) = row.fields.get(&term.field) else {
            return Err(ExactPredicateProgramErrorV1::TypeMismatch);
        };
        if value.profile() != term.profile {
            return Err(ExactPredicateProgramErrorV1::TypeMismatch);
        }
    }
    Ok(())
}

fn compare_rows(
    left: &ExactReferenceRowV1,
    right: &ExactReferenceRowV1,
    order: &ExactOrderProgramV1,
) -> Ordering {
    for term in &order.terms {
        let (Some(ExactReferenceCellV1::Value(left)), Some(ExactReferenceCellV1::Value(right))) =
            (left.fields.get(&term.field), right.fields.get(&term.field))
        else {
            return left.entity_key.cmp(&right.entity_key);
        };
        let compared = left.cmp(right);
        if compared != Ordering::Equal {
            return match term.direction {
                ExactOrderDirectionV1::Ascending => compared,
                ExactOrderDirectionV1::Descending => compared.reverse(),
            };
        }
    }
    left.entity_key.cmp(&right.entity_key)
}

fn validate_nullable_order_row(
    row: &ExactReferenceRowV1,
    order: &ExactOrderProgramV2,
) -> Result<(), ExactPredicateProgramErrorV1> {
    for term in &order.terms {
        let cell = row
            .fields
            .get(&term.field)
            .unwrap_or(&ExactReferenceCellV1::Missing);
        match cell {
            ExactReferenceCellV1::Value(value) if value.profile() == term.profile => {}
            ExactReferenceCellV1::Missing | ExactReferenceCellV1::Null
                if term.placement != ExactStatePlacementV1::PresentOnlyV1 => {}
            _ => return Err(ExactPredicateProgramErrorV1::TypeMismatch),
        }
    }
    Ok(())
}

fn compare_nullable_rows(
    left: &ExactReferenceRowV1,
    right: &ExactReferenceRowV1,
    order: &ExactOrderProgramV2,
) -> Ordering {
    for term in &order.terms {
        let left = left
            .fields
            .get(&term.field)
            .unwrap_or(&ExactReferenceCellV1::Missing);
        let right = right
            .fields
            .get(&term.field)
            .unwrap_or(&ExactReferenceCellV1::Missing);
        let compared = match (left, right) {
            (ExactReferenceCellV1::Value(left), ExactReferenceCellV1::Value(right)) => {
                let compared = left.cmp(right);
                match term.direction {
                    ExactOrderDirectionV1::Ascending => compared,
                    ExactOrderDirectionV1::Descending => compared.reverse(),
                }
            }
            (
                ExactReferenceCellV1::Missing | ExactReferenceCellV1::Null,
                ExactReferenceCellV1::Missing | ExactReferenceCellV1::Null,
            ) => Ordering::Equal,
            (ExactReferenceCellV1::Missing | ExactReferenceCellV1::Null, _) => {
                match term.placement {
                    ExactStatePlacementV1::NullsFirstV1 => Ordering::Less,
                    ExactStatePlacementV1::NullsLastV1 => Ordering::Greater,
                    ExactStatePlacementV1::PresentOnlyV1 => Ordering::Equal,
                }
            }
            (_, ExactReferenceCellV1::Missing | ExactReferenceCellV1::Null) => {
                match term.placement {
                    ExactStatePlacementV1::NullsFirstV1 => Ordering::Greater,
                    ExactStatePlacementV1::NullsLastV1 => Ordering::Less,
                    ExactStatePlacementV1::PresentOnlyV1 => Ordering::Equal,
                }
            }
        };
        if compared != Ordering::Equal {
            return compared;
        }
    }
    left.entity_key.cmp(&right.entity_key)
}

fn encode_nullable_program(
    base: &ExactPredicateProgramV1,
    orders: &[ExactOrderProgramV2],
) -> Result<Vec<u8>, ExactPredicateProgramErrorV1> {
    let mut output = Vec::new();
    output.extend_from_slice(b"REPN");
    output.extend_from_slice(&EXACT_PREDICATE_PROGRAM_VERSION_V2.to_be_bytes());
    output.extend_from_slice(
        &u32::try_from(base.canonical_bytes().len())
            .map_err(|_| ExactPredicateProgramErrorV1::BoundExceeded)?
            .to_be_bytes(),
    );
    output.extend_from_slice(base.canonical_bytes());
    output
        .push(u8::try_from(orders.len()).map_err(|_| ExactPredicateProgramErrorV1::BoundExceeded)?);
    for order in orders {
        output.push(
            u8::try_from(order.terms.len())
                .map_err(|_| ExactPredicateProgramErrorV1::BoundExceeded)?,
        );
        for term in &order.terms {
            output.extend_from_slice(&term.field.get().to_be_bytes());
            output.push(term.profile as u8);
            output.push(term.direction as u8);
            output.push(term.placement as u8);
            output.push(u8::from(term.key_tie_breaker));
        }
    }
    if output.len() > MAX_EXACT_PREDICATE_PROGRAM_BYTES_V1 {
        return Err(ExactPredicateProgramErrorV1::BoundExceeded);
    }
    Ok(output)
}

fn encode_program(
    predicate: &ExactPredicateNodeV1,
    orders: &[ExactOrderProgramV1],
    presence: u8,
    exact_count: bool,
    max_offset: u32,
    max_limit: u16,
    provider_requirement: ExactProviderRequirementV1,
) -> Result<Vec<u8>, ExactPredicateProgramErrorV1> {
    let mut output = Vec::new();
    output.extend_from_slice(b"REPF");
    output.extend_from_slice(&EXACT_PREDICATE_PROGRAM_VERSION_V1.to_be_bytes());
    output.push(presence);
    output.push(u8::from(exact_count));
    output.extend_from_slice(&max_offset.to_be_bytes());
    output.extend_from_slice(&max_limit.to_be_bytes());
    output.push(match provider_requirement.policy_mode {
        ProjectionProviderPolicyModeV1::PartitionAligned => 1,
        ProjectionProviderPolicyModeV1::PolicySubpartition => 2,
        ProjectionProviderPolicyModeV1::BoundedRowAdmission => 3,
    });
    output.extend_from_slice(&provider_requirement.max_candidates.to_be_bytes());
    output.extend_from_slice(&provider_requirement.max_work_units.to_be_bytes());
    output.extend_from_slice(&provider_requirement.max_state_bytes_per_row.to_be_bytes());
    encode_node(&mut output, predicate)?;
    output
        .push(u8::try_from(orders.len()).map_err(|_| ExactPredicateProgramErrorV1::BoundExceeded)?);
    for order in orders {
        output.push(
            u8::try_from(order.terms.len())
                .map_err(|_| ExactPredicateProgramErrorV1::BoundExceeded)?,
        );
        for term in &order.terms {
            output.extend_from_slice(&term.field.get().to_be_bytes());
            output.push(term.profile as u8);
            output.push(term.direction as u8);
            output.push(u8::from(term.key_tie_breaker));
        }
    }
    if output.len() > MAX_EXACT_PREDICATE_PROGRAM_BYTES_V1 {
        return Err(ExactPredicateProgramErrorV1::BoundExceeded);
    }
    Ok(output)
}

fn encode_node(
    output: &mut Vec<u8>,
    node: &ExactPredicateNodeV1,
) -> Result<(), ExactPredicateProgramErrorV1> {
    match node {
        ExactPredicateNodeV1::Leaf(leaf) => {
            output.push(1);
            output.extend_from_slice(&leaf.field.get().to_be_bytes());
            output.push(leaf.operator as u8);
            output.push(leaf.profile as u8);
            match leaf.value {
                None => output.push(0),
                Some(ExactValueSlotV1::Scalar(ordinal)) => {
                    output.push(1);
                    output.extend_from_slice(&ordinal.to_be_bytes());
                }
                Some(ExactValueSlotV1::Set(ordinal)) => {
                    output.push(2);
                    output.extend_from_slice(&ordinal.to_be_bytes());
                }
            }
        }
        ExactPredicateNodeV1::And(children) | ExactPredicateNodeV1::Or(children) => {
            output.push(if matches!(node, ExactPredicateNodeV1::And(_)) {
                2
            } else {
                3
            });
            output.push(
                u8::try_from(children.len())
                    .map_err(|_| ExactPredicateProgramErrorV1::BoundExceeded)?,
            );
            for child in children {
                encode_node(output, child)?;
            }
        }
        ExactPredicateNodeV1::When {
            presence_ordinal,
            child,
        } => {
            output.push(4);
            output.push(*presence_ordinal);
            encode_node(output, child)?;
        }
    }
    Ok(())
}

fn decode_node(
    reader: &mut Reader<'_>,
    depth: usize,
) -> Result<ExactPredicateNodeV1, ExactPredicateProgramErrorV1> {
    if depth >= MAX_EXACT_PREDICATE_DEPTH_V1 {
        return Err(ExactPredicateProgramErrorV1::BoundExceeded);
    }
    match reader.u8()? {
        1 => {
            let field =
                FieldId::new(reader.u32()?).ok_or(ExactPredicateProgramErrorV1::InvalidEncoding)?;
            let operator = decode_operator(reader.u8()?)?;
            let profile = decode_profile(reader.u8()?)?;
            let value = match reader.u8()? {
                0 => None,
                1 => Some(ExactValueSlotV1::Scalar(reader.u16()?)),
                2 => Some(ExactValueSlotV1::Set(reader.u16()?)),
                _ => return Err(ExactPredicateProgramErrorV1::InvalidEncoding),
            };
            Ok(ExactPredicateNodeV1::Leaf(ExactPredicateLeafV1::new(
                field, operator, profile, value,
            )?))
        }
        tag @ (2 | 3) => {
            let count = usize::from(reader.u8()?);
            if !(2..=MAX_EXACT_BOOLEAN_BRANCHES_V1).contains(&count) {
                return Err(ExactPredicateProgramErrorV1::BoundExceeded);
            }
            let mut children = Vec::with_capacity(count);
            for _ in 0..count {
                children.push(decode_node(reader, depth + 1)?);
            }
            Ok(if tag == 2 {
                ExactPredicateNodeV1::And(children)
            } else {
                ExactPredicateNodeV1::Or(children)
            })
        }
        4 => Ok(ExactPredicateNodeV1::When {
            presence_ordinal: reader.u8()?,
            child: Box::new(decode_node(reader, depth + 1)?),
        }),
        _ => Err(ExactPredicateProgramErrorV1::InvalidEncoding),
    }
}

fn decode_profile(value: u8) -> Result<ExactComparisonProfileV1, ExactPredicateProgramErrorV1> {
    match value {
        1 => Ok(ExactComparisonProfileV1::Bool),
        2 => Ok(ExactComparisonProfileV1::I64),
        3 => Ok(ExactComparisonProfileV1::U64),
        4 => Ok(ExactComparisonProfileV1::Decimal),
        5 => Ok(ExactComparisonProfileV1::Money),
        6 => Ok(ExactComparisonProfileV1::BinaryUtf8),
        7 => Ok(ExactComparisonProfileV1::Bytes),
        8 => Ok(ExactComparisonProfileV1::Timestamp),
        9 => Ok(ExactComparisonProfileV1::Date),
        10 => Ok(ExactComparisonProfileV1::Uuid),
        11 => Ok(ExactComparisonProfileV1::Enum),
        _ => Err(ExactPredicateProgramErrorV1::InvalidEncoding),
    }
}

fn decode_operator(value: u8) -> Result<ExactPredicateOperatorV1, ExactPredicateProgramErrorV1> {
    match value {
        1 => Ok(ExactPredicateOperatorV1::Equal),
        2 => Ok(ExactPredicateOperatorV1::NotEqual),
        3 => Ok(ExactPredicateOperatorV1::Less),
        4 => Ok(ExactPredicateOperatorV1::LessEqual),
        5 => Ok(ExactPredicateOperatorV1::Greater),
        6 => Ok(ExactPredicateOperatorV1::GreaterEqual),
        7 => Ok(ExactPredicateOperatorV1::In),
        8 => Ok(ExactPredicateOperatorV1::NotIn),
        9 => Ok(ExactPredicateOperatorV1::StartsWith),
        10 => Ok(ExactPredicateOperatorV1::EndsWith),
        11 => Ok(ExactPredicateOperatorV1::Contains),
        12 => Ok(ExactPredicateOperatorV1::IsNull),
        13 => Ok(ExactPredicateOperatorV1::IsNotNull),
        14 => Ok(ExactPredicateOperatorV1::Exists),
        _ => Err(ExactPredicateProgramErrorV1::InvalidEncoding),
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], ExactPredicateProgramErrorV1> {
        let end = self
            .offset
            .checked_add(count)
            .filter(|end| *end <= self.bytes.len())
            .ok_or(ExactPredicateProgramErrorV1::InvalidEncoding)?;
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn exact(&mut self, expected: &[u8]) -> Result<(), ExactPredicateProgramErrorV1> {
        (self.take(expected.len())? == expected)
            .then_some(())
            .ok_or(ExactPredicateProgramErrorV1::InvalidEncoding)
    }

    fn u8(&mut self) -> Result<u8, ExactPredicateProgramErrorV1> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, ExactPredicateProgramErrorV1> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().map_err(
            |_| ExactPredicateProgramErrorV1::InvalidEncoding,
        )?))
    }

    fn u32(&mut self) -> Result<u32, ExactPredicateProgramErrorV1> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().map_err(
            |_| ExactPredicateProgramErrorV1::InvalidEncoding,
        )?))
    }

    fn u64(&mut self) -> Result<u64, ExactPredicateProgramErrorV1> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().map_err(
            |_| ExactPredicateProgramErrorV1::InvalidEncoding,
        )?))
    }

    fn finish(self) -> Result<(), ExactPredicateProgramErrorV1> {
        (self.offset == self.bytes.len())
            .then_some(())
            .ok_or(ExactPredicateProgramErrorV1::InvalidEncoding)
    }
}
