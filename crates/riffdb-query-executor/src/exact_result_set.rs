//! Exact-text result-set execution over one already-authorized epoch proof.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::num::NonZeroU16;

use riffdb_projection::{
    ExactPredicatePartitionIndexV4, ExactPredicatePartitionIndexV5, ExactPredicateProviderErrorV1,
    ExactPredicateResultPageV1, ExactPredicateResultPageV2, ExactTextPartitionIndexV2,
    ExactTextPartitionIndexV3, ExactTextResultRowV2, ResultSetEpochProofV1,
};
use riffdb_query_ir::{
    ExactParameterValueV1, ExactPredicateFamilyMemberV1, ExactPredicateProgramV1,
    ExactPredicateProgramV2, ExactTextPlanFamilyV1, ProjectionResultSetPlanV2,
};
use riffdb_types::{
    ApplicationRoleHash, CanonicalValue, CommitSequence,
    EXACT_PREDICATE_PROVIDER_STATE_SCHEMA_HASH_V5, ExactTextNeedleV1, ExactTextOperatorV1,
    ExactTextOrderV1, PartitionKeyHash, ProjectionGeneration, ProjectionProviderDescriptorHash,
    QueryPlanHash,
};

/// One bounded page and exact whole-result measure from the same epoch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactTextResultSetV1 {
    rows: Vec<ExactTextResultRowV2>,
    exact_total: u64,
    partition: PartitionKeyHash,
    plan_identity: QueryPlanHash,
    policy_shape_identity: ApplicationRoleHash,
    provider: ProjectionProviderDescriptorHash,
    generation: ProjectionGeneration,
    history_incarnation: u64,
    epoch: CommitSequence,
}

/// Executes one ADR-0134 indexed predicate member under one authorized epoch proof.
#[allow(clippy::too_many_arguments)]
pub fn execute_exact_predicate_result_set_v1(
    plan_identity: QueryPlanHash,
    program: &ExactPredicateProgramV1,
    proof: &ResultSetEpochProofV1,
    provider: &ExactPredicatePartitionIndexV4,
    parameters: &BTreeMap<u16, ExactParameterValueV1>,
    member: ExactPredicateFamilyMemberV1,
    offset: u32,
    limit: NonZeroU16,
) -> Result<ExactPredicateResultPageV1, ExactPredicateResultSetErrorV1> {
    let binding = provider.binding();
    if binding.plan() != plan_identity
        || binding.plan() != proof.plan_identity()
        || binding.policy_shape() != proof.policy_shape_identity()
        || provider.program().identity() != program.identity()
    {
        return Err(ExactPredicateResultSetErrorV1::PlanMismatch);
    }
    let descriptor = program
        .provider_descriptor()
        .map_err(|_| ExactPredicateResultSetErrorV1::PlanMismatch)?;
    if descriptor.digest() != binding.descriptor() {
        return Err(ExactPredicateResultSetErrorV1::PlanMismatch);
    }
    let participant = proof
        .participants()
        .find(|participant| participant.descriptor() == binding.descriptor())
        .ok_or(ExactPredicateResultSetErrorV1::EpochProofMismatch)?;
    if participant.state_schema_hash() != descriptor.state_identity().schema_hash()
        || participant.history_incarnation() != binding.history_incarnation()
        || participant.generation() != binding.generation()
        || proof.selected_epoch() != binding.frontier()
        || proof.selected_epoch() < participant.floor()
        || proof.selected_epoch() > participant.ceiling()
    {
        return Err(ExactPredicateResultSetErrorV1::EpochProofMismatch);
    }
    provider
        .result_page(parameters, member, offset, limit)
        .map_err(map_exact_predicate_provider_error)
}

/// Executes one ADR-0145 nullable-order member under one authorized V5 epoch proof.
#[allow(clippy::too_many_arguments)]
pub fn execute_nullable_exact_predicate_result_set_v1(
    plan_identity: QueryPlanHash,
    program: &ExactPredicateProgramV2,
    proof: &ResultSetEpochProofV1,
    provider: &ExactPredicatePartitionIndexV5,
    parameters: &BTreeMap<u16, ExactParameterValueV1>,
    member: ExactPredicateFamilyMemberV1,
    offset: u32,
    limit: NonZeroU16,
) -> Result<ExactPredicateResultPageV2, ExactPredicateResultSetErrorV1> {
    let binding = provider.binding();
    if binding.plan() != plan_identity
        || binding.plan() != proof.plan_identity()
        || binding.policy_shape() != proof.policy_shape_identity()
        || binding.program() != program.identity()
        || provider.program().identity() != program.identity()
    {
        return Err(ExactPredicateResultSetErrorV1::PlanMismatch);
    }
    let participant = proof
        .participants()
        .find(|participant| participant.descriptor() == binding.descriptor())
        .ok_or(ExactPredicateResultSetErrorV1::EpochProofMismatch)?;
    if participant.state_schema_hash() != EXACT_PREDICATE_PROVIDER_STATE_SCHEMA_HASH_V5
        || participant.history_incarnation() != binding.history_incarnation()
        || participant.generation() != binding.generation()
        || proof.selected_epoch() != binding.frontier()
        || proof.selected_epoch() < participant.floor()
        || proof.selected_epoch() > participant.ceiling()
    {
        return Err(ExactPredicateResultSetErrorV1::EpochProofMismatch);
    }
    provider
        .result_page(parameters, member, offset, limit)
        .map_err(map_exact_predicate_provider_error)
}

fn map_exact_predicate_provider_error(
    error: ExactPredicateProviderErrorV1,
) -> ExactPredicateResultSetErrorV1 {
    match error {
        ExactPredicateProviderErrorV1::MemberMismatch => {
            ExactPredicateResultSetErrorV1::MemberNotDeclared
        }
        ExactPredicateProviderErrorV1::WindowInvalid => {
            ExactPredicateResultSetErrorV1::WindowInvalid
        }
        ExactPredicateProviderErrorV1::ParameterInvalid
        | ExactPredicateProviderErrorV1::TypeMismatch => {
            ExactPredicateResultSetErrorV1::ParameterInvalid
        }
        ExactPredicateProviderErrorV1::FuelExhausted
        | ExactPredicateProviderErrorV1::BoundExceeded
        | ExactPredicateProviderErrorV1::StateAmplification => {
            ExactPredicateResultSetErrorV1::BoundExceeded
        }
        ExactPredicateProviderErrorV1::BindingMismatch
        | ExactPredicateProviderErrorV1::DuplicateRow
        | ExactPredicateProviderErrorV1::OrderStateInvalid
        | ExactPredicateProviderErrorV1::NonAdvancingEpoch
        | ExactPredicateProviderErrorV1::Integrity
        | ExactPredicateProviderErrorV1::UnsupportedFormat => {
            ExactPredicateResultSetErrorV1::ProviderUnavailable
        }
    }
}

/// Closed value-free ADR-0134 execution failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExactPredicateResultSetErrorV1 {
    /// Plan, semantic program, descriptor, or policy identity disagrees.
    PlanMismatch,
    /// The selected family member is not compiler declared.
    MemberNotDeclared,
    /// Offset or limit exceeds the compiler-owned window.
    WindowInvalid,
    /// Bound scalar/set values do not match the sealed program.
    ParameterInvalid,
    /// Epoch, history, generation, or state schema proof disagrees.
    EpochProofMismatch,
    /// A compiler-owned work or state bound was exceeded.
    BoundExceeded,
    /// Provider state is unavailable or internally inconsistent.
    ProviderUnavailable,
}

impl fmt::Display for ExactPredicateResultSetErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "exact predicate result set unavailable: {self:?}"
        )
    }
}

impl Error for ExactPredicateResultSetErrorV1 {}

/// Executes the additive filtered exact operation over one V3 provider epoch.
#[allow(clippy::too_many_arguments)]
pub fn execute_exact_text_filtered_result_set_v1(
    plan: &ProjectionResultSetPlanV2,
    family: &ExactTextPlanFamilyV1,
    proof: &ResultSetEpochProofV1,
    provider: &ExactTextPartitionIndexV3,
    operator: ExactTextOperatorV1,
    order: ExactTextOrderV1,
    needle: &ExactTextNeedleV1,
    filter: Option<&CanonicalValue>,
    offset: u32,
    limit: NonZeroU16,
) -> Result<ExactTextResultSetV1, ExactTextResultSetErrorV1> {
    validate_exact_execution(
        plan,
        family,
        proof,
        provider.generation(),
        provider.frontier(),
        operator,
        order,
        offset,
        limit,
    )?;
    let page = provider
        .result_page(operator, needle, filter, order, offset, limit)
        .map_err(|_| ExactTextResultSetErrorV1::ProviderUnavailable)?;
    Ok(ExactTextResultSetV1 {
        rows: page.rows().to_vec(),
        exact_total: page.exact_total(),
        partition: provider.partition(),
        plan_identity: plan.identity(),
        policy_shape_identity: proof.policy_shape_identity(),
        provider: plan.provider_digest(),
        generation: provider.generation(),
        history_incarnation: proof.history_incarnation(),
        epoch: proof.selected_epoch(),
    })
}

#[allow(clippy::too_many_arguments)]
fn validate_exact_execution(
    plan: &ProjectionResultSetPlanV2,
    family: &ExactTextPlanFamilyV1,
    proof: &ResultSetEpochProofV1,
    generation: ProjectionGeneration,
    frontier: Option<CommitSequence>,
    operator: ExactTextOperatorV1,
    order: ExactTextOrderV1,
    offset: u32,
    limit: NonZeroU16,
) -> Result<(), ExactTextResultSetErrorV1> {
    if plan.provider_digest() != family.descriptor().digest()
        || proof.plan_identity() != plan.identity()
    {
        return Err(ExactTextResultSetErrorV1::PlanMismatch);
    }
    if !family.contains_member(operator, order) {
        return Err(ExactTextResultSetErrorV1::MemberNotDeclared);
    }
    plan.bind_window(offset, limit)
        .map_err(|_| ExactTextResultSetErrorV1::WindowInvalid)?;
    let participant = proof
        .participants()
        .find(|participant| participant.descriptor() == plan.provider_digest())
        .ok_or(ExactTextResultSetErrorV1::EpochProofMismatch)?;
    if participant.state_schema_hash() != plan.provider().state_identity().schema_hash()
        || participant.generation() != generation
        || proof.selected_epoch() < participant.floor()
        || proof.selected_epoch() > participant.ceiling()
    {
        return Err(ExactTextResultSetErrorV1::EpochProofMismatch);
    }
    if frontier != Some(proof.selected_epoch()) {
        return Err(ExactTextResultSetErrorV1::SnapshotChanged);
    }
    Ok(())
}

impl ExactTextResultSetV1 {
    /// Bounded page rows in the selected total order.
    #[must_use]
    pub fn rows(&self) -> &[ExactTextResultRowV2] {
        &self.rows
    }
    /// Exact admitted count before the window.
    #[must_use]
    pub const fn exact_total(&self) -> u64 {
        self.exact_total
    }
    /// Complete policy-aligned partition.
    #[must_use]
    pub const fn partition(&self) -> PartitionKeyHash {
        self.partition
    }
    /// Exact compiler-owned plan identity.
    #[must_use]
    pub const fn plan_identity(&self) -> QueryPlanHash {
        self.plan_identity
    }
    /// Compiled policy-shape identity; never an authorization grant.
    #[must_use]
    pub const fn policy_shape_identity(&self) -> ApplicationRoleHash {
        self.policy_shape_identity
    }
    /// Pinned provider descriptor.
    #[must_use]
    pub const fn provider(&self) -> ProjectionProviderDescriptorHash {
        self.provider
    }
    /// Never-reused provider generation.
    #[must_use]
    pub const fn generation(&self) -> ProjectionGeneration {
        self.generation
    }
    /// Authoritative history incarnation.
    #[must_use]
    pub const fn history_incarnation(&self) -> u64 {
        self.history_incarnation
    }
    /// Shared count/page epoch.
    #[must_use]
    pub const fn epoch(&self) -> CommitSequence {
        self.epoch
    }
}

/// Executes one compiler-enumerated exact operation after current policy approval.
#[allow(clippy::too_many_arguments)]
pub fn execute_exact_text_result_set_v1(
    plan: &ProjectionResultSetPlanV2,
    family: &ExactTextPlanFamilyV1,
    proof: &ResultSetEpochProofV1,
    provider: &ExactTextPartitionIndexV2,
    operator: ExactTextOperatorV1,
    order: ExactTextOrderV1,
    needle: &ExactTextNeedleV1,
    offset: u32,
    limit: NonZeroU16,
) -> Result<ExactTextResultSetV1, ExactTextResultSetErrorV1> {
    if plan.provider_digest() != family.descriptor().digest()
        || proof.plan_identity() != plan.identity()
    {
        return Err(ExactTextResultSetErrorV1::PlanMismatch);
    }
    if !family.contains_member(operator, order) {
        return Err(ExactTextResultSetErrorV1::MemberNotDeclared);
    }
    plan.bind_window(offset, limit)
        .map_err(|_| ExactTextResultSetErrorV1::WindowInvalid)?;

    let descriptor = plan.provider_digest();
    let participant = proof
        .participants()
        .find(|participant| participant.descriptor() == descriptor)
        .ok_or(ExactTextResultSetErrorV1::EpochProofMismatch)?;
    if participant.state_schema_hash() != plan.provider().state_identity().schema_hash()
        || participant.generation() != provider.generation()
        || proof.selected_epoch() < participant.floor()
        || proof.selected_epoch() > participant.ceiling()
    {
        return Err(ExactTextResultSetErrorV1::EpochProofMismatch);
    }
    // V1 exact-text state retains one complete current image. It never serves
    // a prior epoch from newer bytes; a continuation observes a typed reset.
    if provider.frontier() != Some(proof.selected_epoch()) {
        return Err(ExactTextResultSetErrorV1::SnapshotChanged);
    }
    let page = provider
        .result_page(operator, needle, order, offset, limit)
        .map_err(|_| ExactTextResultSetErrorV1::ProviderUnavailable)?;
    Ok(ExactTextResultSetV1 {
        rows: page.rows().to_vec(),
        exact_total: page.exact_total(),
        partition: provider.partition(),
        plan_identity: plan.identity(),
        policy_shape_identity: proof.policy_shape_identity(),
        provider: descriptor,
        generation: provider.generation(),
        history_incarnation: proof.history_incarnation(),
        epoch: proof.selected_epoch(),
    })
}

/// Closed value-free exact result-set failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExactTextResultSetErrorV1 {
    /// Plan, family, descriptor, or proof identities disagree.
    PlanMismatch,
    /// Runtime operator/order was not compiler enumerated.
    MemberNotDeclared,
    /// Offset or limit exceeds compiler-owned maxima.
    WindowInvalid,
    /// Generation, schema, interval, or descriptor proof disagrees.
    EpochProofMismatch,
    /// Current provider state advanced since the bound proof.
    SnapshotChanged,
    /// Provider refused bounded exact execution.
    ProviderUnavailable,
}

impl fmt::Display for ExactTextResultSetErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "exact result set unavailable: {self:?}")
    }
}

impl Error for ExactTextResultSetErrorV1 {}
