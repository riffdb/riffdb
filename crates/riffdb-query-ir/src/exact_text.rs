//! Compiler-enumerated exact-text provider families (ADR-0131).

use riffdb_riffql_syntax::Span;
use riffdb_types::{
    ExactTextOperatorV1, FieldId, MAX_EXACT_TEXT_NEEDLE_BYTES_V1,
    MAX_EXACT_TEXT_ROWS_PER_PARTITION_V1, MAX_EXACT_TEXT_VALUE_BYTES_V1,
    ProjectionProviderCapabilitiesV1, ProjectionProviderDescriptorV1, ProjectionProviderKindV1,
    ProjectionProviderPolicyModeV1, ProjectionProviderPostureV1,
};

/// Canonical exact-text plan-family identity.
pub const EXACT_TEXT_PLAN_FAMILY_VERSION_V1: u16 = 1;
/// Maximum operator/order members in the closed V1 family.
pub const MAX_EXACT_TEXT_PLAN_MEMBERS_V1: usize = 8;

/// Closed total orders supported by the V1 exact-text family.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum ExactTextOrderV1 {
    /// Indexed value ascending, then authoritative entity-key hash ascending.
    ValueAscEntityKey = 1,
    /// Indexed value descending, then authoritative entity-key hash ascending.
    ValueDescEntityKey = 2,
}

/// Closed order mask in stable semantic-tag order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExactTextOrderSetV1(u8);

impl ExactTextOrderSetV1 {
    /// Constructs ascending/descending value-order choices.
    #[must_use]
    pub const fn from_flags(flags: [bool; 2]) -> Self {
        Self((flags[0] as u8) | ((flags[1] as u8) << 1))
    }

    /// Stable bit representation.
    #[must_use]
    pub const fn bits(self) -> u8 {
        self.0
    }

    /// Whether one total order is compiler-declared.
    #[must_use]
    pub const fn contains(self, order: ExactTextOrderV1) -> bool {
        self.0 & (1 << (order as u8 - 1)) != 0
    }
}

/// Closed operator mask in stable semantic-tag order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExactTextOperatorSetV1(u8);

impl ExactTextOperatorSetV1 {
    /// Constructs the exact equals/prefix/suffix/contains membership mask.
    #[must_use]
    pub const fn from_flags(flags: [bool; 4]) -> Self {
        Self(
            (flags[0] as u8)
                | ((flags[1] as u8) << 1)
                | ((flags[2] as u8) << 2)
                | ((flags[3] as u8) << 3),
        )
    }

    /// Stable bit representation.
    #[must_use]
    pub const fn bits(self) -> u8 {
        self.0
    }

    /// Whether one exact operator is compiler-declared.
    #[must_use]
    pub const fn contains(self, operator: ExactTextOperatorV1) -> bool {
        self.0 & (1 << (operator as u8 - 1)) != 0
    }
}

/// One member of a finite exact-text access family.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExactTextPlanMemberV1 {
    operator: ExactTextOperatorV1,
    order: ExactTextOrderV1,
}

impl ExactTextPlanMemberV1 {
    /// Exact predicate selected by one closed generated choice.
    #[must_use]
    pub const fn operator(self) -> ExactTextOperatorV1 {
        self.operator
    }

    /// Exact total order selected by one closed generated choice.
    #[must_use]
    pub const fn order(self) -> ExactTextOrderV1 {
        self.order
    }
}

/// Sealed exact-text declaration and finite access family.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactTextPlanFamilyV1 {
    field: FieldId,
    operators: ExactTextOperatorSetV1,
    orders: ExactTextOrderSetV1,
    policy_mode: ProjectionProviderPolicyModeV1,
    max_value_bytes: u16,
    max_needle_bytes: u16,
    max_candidates: u32,
    descriptor: ProjectionProviderDescriptorV1,
    source_span: Span,
    members: Vec<ExactTextPlanMemberV1>,
}

impl ExactTextPlanFamilyV1 {
    /// Validates static bounds and enumerates every legal operator exactly once.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        field: FieldId,
        operators: ExactTextOperatorSetV1,
        orders: ExactTextOrderSetV1,
        policy_mode: ProjectionProviderPolicyModeV1,
        max_value_bytes: u16,
        max_needle_bytes: u16,
        max_candidates: u32,
        descriptor: ProjectionProviderDescriptorV1,
        source_span: Span,
    ) -> Result<Self, ExactTextPlanFamilyErrorV1> {
        if operators.bits() == 0 || operators.bits() & !0x0f != 0 {
            return Err(ExactTextPlanFamilyErrorV1::EmptyOrUnknownOperatorSet);
        }
        if orders.bits() == 0 || orders.bits() & !0x03 != 0 {
            return Err(ExactTextPlanFamilyErrorV1::EmptyOrUnknownOrderSet);
        }
        if usize::from(max_value_bytes) > MAX_EXACT_TEXT_VALUE_BYTES_V1
            || usize::from(max_needle_bytes) > MAX_EXACT_TEXT_NEEDLE_BYTES_V1
            || max_value_bytes == 0
            || max_needle_bytes == 0
            || max_needle_bytes > max_value_bytes
            || max_candidates == 0
            || usize::try_from(max_candidates)
                .map_or(true, |value| value > MAX_EXACT_TEXT_ROWS_PER_PARTITION_V1)
        {
            return Err(ExactTextPlanFamilyErrorV1::StaticBound);
        }
        if descriptor.kind() != ProjectionProviderKindV1::ExactText
            || descriptor.posture() != ProjectionProviderPostureV1::Exact
            || descriptor.policy_mode() != policy_mode
        {
            return Err(ExactTextPlanFamilyErrorV1::ProviderMismatch);
        }
        let required = ProjectionProviderCapabilitiesV1::CANDIDATE
            | ProjectionProviderCapabilitiesV1::FILTER
            | ProjectionProviderCapabilitiesV1::ORDER
            | ProjectionProviderCapabilitiesV1::MEASURE
            | ProjectionProviderCapabilitiesV1::WINDOW
            | ProjectionProviderCapabilitiesV1::OUTPUT;
        if !descriptor.capabilities().contains(required)
            || descriptor.max_candidates() < max_candidates
        {
            return Err(ExactTextPlanFamilyErrorV1::ProviderMismatch);
        }
        let operator_values = [
            ExactTextOperatorV1::Equals,
            ExactTextOperatorV1::StartsWith,
            ExactTextOperatorV1::EndsWith,
            ExactTextOperatorV1::Contains,
        ];
        let orders_values = [
            ExactTextOrderV1::ValueAscEntityKey,
            ExactTextOrderV1::ValueDescEntityKey,
        ];
        let members = operator_values
            .into_iter()
            .filter(|operator| operators.contains(*operator))
            .flat_map(|operator| {
                orders_values
                    .into_iter()
                    .filter(move |order| orders.contains(*order))
                    .map(move |order| ExactTextPlanMemberV1 { operator, order })
            })
            .collect();
        Ok(Self {
            field,
            operators,
            orders,
            policy_mode,
            max_value_bytes,
            max_needle_bytes,
            max_candidates,
            descriptor,
            source_span,
            members,
        })
    }

    /// Compiler-enumerated stable operator members.
    #[must_use]
    pub fn members(&self) -> &[ExactTextPlanMemberV1] {
        &self.members
    }

    /// Original declaration span for safe compiler diagnostics.
    #[must_use]
    pub const fn source_span(&self) -> Span {
        self.source_span
    }

    /// Exact indexed field.
    #[must_use]
    pub const fn field(&self) -> FieldId {
        self.field
    }

    /// Exact provider selected at compilation.
    #[must_use]
    pub const fn descriptor(&self) -> &ProjectionProviderDescriptorV1 {
        &self.descriptor
    }

    /// Canonical separately versioned family bytes; existing query IR is untouched.
    #[must_use]
    pub fn to_canonical_bytes(&self) -> [u8; 64] {
        let mut bytes = [0; 64];
        bytes[..4].copy_from_slice(b"RXTQ");
        bytes[4..6].copy_from_slice(&EXACT_TEXT_PLAN_FAMILY_VERSION_V1.to_be_bytes());
        bytes[6..10].copy_from_slice(&self.field.get().to_be_bytes());
        bytes[10] = self.operators.bits();
        bytes[11] = self.policy_mode as u8;
        bytes[12..14].copy_from_slice(&self.max_value_bytes.to_be_bytes());
        bytes[14..16].copy_from_slice(&self.max_needle_bytes.to_be_bytes());
        bytes[16..20].copy_from_slice(&self.max_candidates.to_be_bytes());
        bytes[20..52].copy_from_slice(self.descriptor.digest().as_bytes());
        bytes[52] = self.orders.bits();
        bytes
    }
}

/// Closed plan-family construction failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExactTextPlanFamilyErrorV1 {
    /// No operator or an unassigned bit was requested.
    EmptyOrUnknownOperatorSet,
    /// No total order or an unassigned bit was requested.
    EmptyOrUnknownOrderSet,
    /// A field/needle/candidate ceiling is zero, inverted, or excessive.
    StaticBound,
    /// Descriptor family, posture, policy, capabilities, or costs disagree.
    ProviderMismatch,
}
