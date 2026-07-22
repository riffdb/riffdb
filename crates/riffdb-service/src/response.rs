//! Versioned, transport-independent service response accounting.

use std::error::Error;
use std::fmt;

use riffdb_contract_ir::{CommandExplain, GeneratedSchemaArtifact};
use riffdb_types::{
    AdmittedActorContext, CanonicalRecord, CanonicalValue, ContractLineage, ProjectionIdentity,
    ServiceAuditTargetV1, TenantScope,
};

use crate::{
    BuildInfo, CommandToolDescriptor, CommandToolDiscoveryItem, CommitScanFence,
    CommitSubscriptionEvent, CommitView, ContractDescriptor, ContractValidationResult,
    CreateCapabilityResult, CursorToken, DeclaredOutcomeView, DeployContractResult,
    DiscoverCommandToolsResult, DiscoverResourcesResult, DiscoveryCatalogFence, DurableEventView,
    EntityView, ExecuteCommandResult, ExplainCommandResult, GetActiveContractResult,
    GetCommitResult, GetContractVersionResult, GetEntityResult, GetProjectionStatusResult,
    HealthReport, HealthResult, IndexRowView, IndexScanFence, JournaledCommandResult,
    ListPendingOutboxDeliveriesResult, NormalCreateCapabilityResult, OutboxDeliverySummary, Page,
    ProjectionPageFence, ProjectionRow, ProjectionStatusSnapshot, ProvenanceClaimsView,
    ProvenanceView, QueryProjectionResult, ReadOnlyCommandResult, ResolveCommandOutcomeResult,
    ResourceDescriptor, RevokeCapabilityResult, ScanCommitsResult, ScanIndexResult, ServiceFailure,
    StatisticsResult, SubscribeToCommitsResult, TraceProvenanceResult,
};

/// Exact POC ceiling for one API-neutral unary result or visible stream item.
pub const MAX_SERVICE_RESPONSE_BYTES: usize = 4_194_304;

/// Version of the conservative service-owned response charge.
pub const SERVICE_RESPONSE_CHARGE_VERSION: u16 = 1;

// A Protobuf field tag, a maximum-width scalar or length prefix, and nesting
// framing together consume substantially less than this reserve. Keeping one
// uniform reserve makes the accounting independent of a particular schema
// generator while leaving WP-127 an auditable upper-bound proof.
const MESSAGE_RESERVE: usize = 32;
const FIELD_RESERVE: usize = 16;
const PAGE_WRAPPER_RESERVE: usize = MESSAGE_RESERVE * 4;

/// Deterministic conservative byte charge under service response accounting v1.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ServiceResponseChargeV1(usize);

impl ServiceResponseChargeV1 {
    /// Returns the conservative charged byte count.
    #[must_use]
    pub const fn bytes(self) -> usize {
        self.0
    }

    /// Returns whether this charge fits the exact POC ceiling.
    #[must_use]
    pub const fn fits(self) -> bool {
        self.0 <= MAX_SERVICE_RESPONSE_BYTES
    }
}

/// Checked response-charge arithmetic could not represent the complete result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServiceResponseChargeOverflow;

impl fmt::Display for ServiceResponseChargeOverflow {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("service response charge overflowed")
    }
}

impl Error for ServiceResponseChargeOverflow {}

mod sealed {
    pub trait Sealed {}
}

/// Public result values implement the one accepted response-charge version.
///
/// The private supertrait keeps the service crate as the sole accounting owner.
/// Downstream crates can inspect charges but cannot publish an alternate rule.
pub trait ServiceResponseCharge: sealed::Sealed {
    /// Computes the deterministic conservative v1 charge.
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ChargeAccumulator {
    bytes: usize,
}

impl ChargeAccumulator {
    pub(crate) const fn message() -> Self {
        Self {
            bytes: MESSAGE_RESERVE,
        }
    }

    #[cfg(test)]
    pub(crate) const fn empty() -> Self {
        Self { bytes: 0 }
    }

    pub(crate) fn fields(&mut self, count: usize) -> Result<(), ServiceResponseChargeOverflow> {
        self.add_product(count, FIELD_RESERVE)
    }

    pub(crate) fn bytes(&mut self, length: usize) -> Result<(), ServiceResponseChargeOverflow> {
        self.fields(1)?;
        self.add(length)
    }

    pub(crate) fn nested<T: ServiceResponseCharge + ?Sized>(
        &mut self,
        value: &T,
    ) -> Result<(), ServiceResponseChargeOverflow> {
        self.fields(1)?;
        self.add(value.service_response_charge_v1()?.bytes())
    }

    pub(crate) fn repeated<T: ServiceResponseCharge>(
        &mut self,
        values: &[T],
    ) -> Result<(), ServiceResponseChargeOverflow> {
        for value in values {
            self.nested(value)?;
        }
        Ok(())
    }

    pub(crate) fn add(&mut self, bytes: usize) -> Result<(), ServiceResponseChargeOverflow> {
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or(ServiceResponseChargeOverflow)?;
        Ok(())
    }

    fn add_product(
        &mut self,
        count: usize,
        bytes: usize,
    ) -> Result<(), ServiceResponseChargeOverflow> {
        self.add(
            count
                .checked_mul(bytes)
                .ok_or(ServiceResponseChargeOverflow)?,
        )
    }

    pub(crate) const fn finish(self) -> ServiceResponseChargeV1 {
        ServiceResponseChargeV1(self.bytes)
    }
}

impl ServiceResponseCharge for () {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        Ok(ChargeAccumulator::message().finish())
    }
}

fn fixed_charge(fields: usize) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
    let mut charge = ChargeAccumulator::message();
    charge.fields(fields)?;
    Ok(charge.finish())
}

fn charge_lineage(
    charge: &mut ChargeAccumulator,
    lineage: &ContractLineage,
) -> Result<(), ServiceResponseChargeOverflow> {
    charge.bytes(lineage.as_bytes().len())
}

fn charge_contract_descriptor(
    charge: &mut ChargeAccumulator,
    descriptor: &ContractDescriptor,
) -> Result<(), ServiceResponseChargeOverflow> {
    charge_lineage(charge, descriptor.lineage())?;
    // Version plus three 32-byte hashes.
    charge.fields(1)?;
    charge.bytes(32)?;
    charge.bytes(32)?;
    charge.bytes(32)
}

fn charge_schema(
    charge: &mut ChargeAccumulator,
    schema: &GeneratedSchemaArtifact,
) -> Result<(), ServiceResponseChargeOverflow> {
    // Key, fixed dialect identifier, schema hash, and exact canonical JSON.
    charge.fields(2)?;
    charge.bytes("https://json-schema.org/draft/2020-12/schema".len())?;
    charge.bytes(32)?;
    charge.bytes(schema.canonical_json().len())
}

fn charge_command_explain(
    charge: &mut ChargeAccumulator,
    explain: &CommandExplain,
) -> Result<(), ServiceResponseChargeOverflow> {
    // WP-127 carries the bounded stable-ID summary and this deterministic text.
    // Four field reserves per collection entry dominate packed and nested forms.
    charge.fields(4)?;
    let entry_count = explain
        .bindings()
        .len()
        .checked_add(explain.read_fields().len())
        .and_then(|count| count.checked_add(explain.write_fields().len()))
        .and_then(|count| count.checked_add(explain.invariants().len()))
        .and_then(|count| count.checked_add(explain.events().len()))
        .and_then(|count| count.checked_add(explain.outcomes().len()))
        .ok_or(ServiceResponseChargeOverflow)?;
    charge.fields(
        entry_count
            .checked_mul(4)
            .ok_or(ServiceResponseChargeOverflow)?,
    )?;
    charge.bytes(explain.render_text().len())
}

fn charge_actor(
    charge: &mut ChargeAccumulator,
    actor: &AdmittedActorContext,
) -> Result<(), ServiceResponseChargeOverflow> {
    charge.bytes(actor.principal_id().as_str().len())?;
    charge.fields(1)?;
    match actor.tenant_scope() {
        TenantScope::Global => charge.fields(1)?,
        TenantScope::Tenant(tenant) => charge.bytes(tenant.as_bytes().len())?,
    }
    if actor.agent_session_id().is_some() {
        charge.bytes(16)?;
    }
    Ok(())
}

fn charge_declared_outcome(
    charge: &mut ChargeAccumulator,
    outcome: &DeclaredOutcomeView,
) -> Result<(), ServiceResponseChargeOverflow> {
    charge.fields(2)?;
    charge.bytes(outcome.outcome_name().as_str().len())?;
    charge.nested(outcome.value())
}

fn charge_journaled(
    charge: &mut ChargeAccumulator,
    result: &JournaledCommandResult,
) -> Result<(), ServiceResponseChargeOverflow> {
    charge.fields(6)?;
    charge_lineage(charge, result.lineage())?;
    charge.bytes(32)?;
    charge_declared_outcome(charge, result.outcome())?;
    // Reserve the fixed canonical provenance locator produced by WP-130.
    charge.bytes(56)
}

fn charge_read_only(
    charge: &mut ChargeAccumulator,
    result: &ReadOnlyCommandResult,
) -> Result<(), ServiceResponseChargeOverflow> {
    charge.fields(3)?;
    charge_lineage(charge, result.lineage())?;
    charge.bytes(32)?;
    charge_declared_outcome(charge, result.outcome())
}

fn charge_entity(
    charge: &mut ChargeAccumulator,
    entity: &EntityView,
) -> Result<(), ServiceResponseChargeOverflow> {
    charge.bytes(entity.key().as_bytes().len())?;
    charge.fields(2)?;
    charge.nested(entity.fields())
}

fn charge_projection_identity(
    charge: &mut ChargeAccumulator,
    identity: &ProjectionIdentity,
) -> Result<(), ServiceResponseChargeOverflow> {
    charge_lineage(charge, identity.contract_lineage())?;
    charge.fields(1)?;
    charge.bytes(32)
}

fn charge_projection_status(
    charge: &mut ChargeAccumulator,
    status: &ProjectionStatusSnapshot,
) -> Result<(), ServiceResponseChargeOverflow> {
    charge_projection_identity(charge, status.identity())?;
    charge.fields(2)?;
    if status.published().is_some() {
        charge.fields(3)?;
    }
    if status.candidate().is_some() {
        charge.fields(3)?;
    }
    if status.published_apply_mode().is_some() {
        charge.fields(1)?;
    }
    if status.failure().is_some() {
        charge.fields(4)?;
    }
    Ok(())
}

fn charge_claims(
    charge: &mut ChargeAccumulator,
    claims: &ProvenanceClaimsView,
) -> Result<(), ServiceResponseChargeOverflow> {
    if let Some(value) = claims.source_repository() {
        charge.bytes(value.as_bytes().len())?;
    }
    if let Some(value) = claims.source_commit() {
        charge.bytes(value.as_bytes().len())?;
    }
    if let Some(value) = claims.reason() {
        charge.bytes(value.as_bytes().len())?;
    }
    if let Some(value) = claims.approval_id() {
        charge.bytes(value.as_bytes().len())?;
    }
    Ok(())
}

fn charge_build(
    charge: &mut ChargeAccumulator,
    build: &BuildInfo,
) -> Result<(), ServiceResponseChargeOverflow> {
    charge.bytes(build.semantic_version().len())?;
    charge.bytes(build.git_revision().len())?;
    charge.bytes(build.rust_version().len())?;
    for feature in build.enabled_features() {
        charge.bytes(feature.len())?;
    }
    charge.fields(2)?;
    charge.bytes(build.mcp_protocol_baseline().len())
}

impl ServiceResponseCharge for CanonicalValue {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.fields(1)?;
        match self {
            Self::String(value) => charge.bytes(value.len())?,
            Self::Bytes(value) => charge.bytes(value.len())?,
            Self::List(values) => charge.repeated(values.values())?,
            Self::Record(record) => charge.nested(record)?,
            Self::Null
            | Self::Bool(_)
            | Self::I64(_)
            | Self::U64(_)
            | Self::Decimal(_)
            | Self::Money(_)
            | Self::Timestamp(_)
            | Self::Date(_)
            | Self::Uuid(_)
            | Self::Enum { .. } => charge.fields(3)?,
        }
        Ok(charge.finish())
    }
}

impl ServiceResponseCharge for CanonicalRecord {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        for (_, value) in self.fields() {
            charge.fields(1)?;
            charge.nested(value)?;
        }
        Ok(charge.finish())
    }
}

impl ServiceResponseCharge for ContractValidationResult {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.fields(1)?;
        if let Self::Invalid(error) = self {
            if let Some(diagnostics) = error.syntax() {
                for diagnostic in diagnostics.as_slice() {
                    charge.fields(8)?;
                    charge.bytes(diagnostic.code().as_str().len())?;
                    charge.bytes(diagnostic.code().summary().len())?;
                    if let Some(help) = diagnostic.code().help() {
                        charge.bytes(help.len())?;
                    }
                    for expected in diagnostic.expected() {
                        charge.bytes(expected.len())?;
                    }
                }
            }
            if let Some(diagnostics) = error.semantic() {
                for diagnostic in diagnostics.as_slice() {
                    charge.fields(8)?;
                    charge.bytes(diagnostic.code().as_str().len())?;
                    charge.bytes(diagnostic.code().summary().len())?;
                    if let Some(help) = diagnostic.code().help() {
                        charge.bytes(help.len())?;
                    }
                }
            }
        }
        Ok(charge.finish())
    }
}

impl ServiceResponseCharge for ExplainCommandResult {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.fields(1)?;
        if let Self::Found(explained) = self {
            charge_contract_descriptor(&mut charge, explained.contract())?;
            charge.fields(1)?;
            charge.bytes(32)?;
            charge_command_explain(&mut charge, explained.explanation())?;
            charge_schema(&mut charge, explained.input_schema())?;
            charge_schema(&mut charge, explained.outcome_schema())?;
        }
        Ok(charge.finish())
    }
}

impl ServiceResponseCharge for DeployContractResult {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.fields(1)?;
        match self {
            Self::Activated(descriptor) | Self::AlreadyActive(descriptor) => {
                charge_contract_descriptor(&mut charge, descriptor)?;
            }
            Self::ExpectedActiveVersionMismatch { actual } => {
                if actual.is_some() {
                    charge.fields(1)?;
                }
            }
            Self::BundleConflict => {}
        }
        Ok(charge.finish())
    }
}

macro_rules! contract_lookup_charge {
    ($type:ty, $present:pat => $descriptor:ident) => {
        impl ServiceResponseCharge for $type {
            fn service_response_charge_v1(
                &self,
            ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
                let mut charge = ChargeAccumulator::message();
                charge.fields(1)?;
                if let $present = self {
                    charge_contract_descriptor(&mut charge, $descriptor)?;
                }
                Ok(charge.finish())
            }
        }
    };
}

contract_lookup_charge!(GetActiveContractResult, GetActiveContractResult::Present(descriptor) => descriptor);
contract_lookup_charge!(GetContractVersionResult, GetContractVersionResult::Found(descriptor) => descriptor);

impl ServiceResponseCharge for ExecuteCommandResult {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.fields(1)?;
        match self {
            Self::Journaled(result) => charge_journaled(&mut charge, result)?,
            Self::ReadOnlyExecuted(result) => charge_read_only(&mut charge, result)?,
        }
        Ok(charge.finish())
    }
}

impl ServiceResponseCharge for ResolveCommandOutcomeResult {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.fields(1)?;
        if let Self::Found(result) = self {
            charge_journaled(&mut charge, result)?;
        }
        Ok(charge.finish())
    }
}

impl ServiceResponseCharge for GetEntityResult {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.fields(1)?;
        if let Self::Found(entity) = self {
            charge_entity(&mut charge, entity)?;
        }
        Ok(charge.finish())
    }
}

impl ServiceResponseCharge for IndexRowView {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.bytes(self.key().as_bytes().len())?;
        charge.nested(self.values())?;
        Ok(charge.finish())
    }
}

impl ServiceResponseCharge for IndexScanFence {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        fixed_charge(1)
    }
}

impl ServiceResponseCharge for ScanIndexResult {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.nested(self.page())?;
        Ok(charge.finish())
    }
}

impl ServiceResponseCharge for ProjectionRow {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.repeated(self.group())?;
        charge.nested(self.values())?;
        Ok(charge.finish())
    }
}

impl ServiceResponseCharge for ProjectionPageFence {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge_projection_identity(&mut charge, self.identity())?;
        charge.fields(3)?;
        Ok(charge.finish())
    }
}

impl ServiceResponseCharge for QueryProjectionResult {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.fields(1)?;
        match self {
            Self::Ready(ready) => {
                charge.nested(ready.data())?;
                charge.fields(2)?;
            }
            Self::WaitTimedOut { .. } | Self::Degraded { .. } => charge.fields(3)?,
            Self::Invalid { .. } => charge.fields(2)?,
        }
        Ok(charge.finish())
    }
}

impl ServiceResponseCharge for GetProjectionStatusResult {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.fields(1)?;
        if let Self::Found(status) = self {
            charge_projection_status(&mut charge, status)?;
        }
        Ok(charge.finish())
    }
}

impl ServiceResponseCharge for DurableEventView {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.fields(2)?;
        charge.nested(self.payload())?;
        Ok(charge.finish())
    }
}

impl ServiceResponseCharge for CommitView {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let snapshot = self.as_snapshot();
        let mut charge = ChargeAccumulator::message();
        charge.fields(8)?;
        charge.bytes(16)?;
        charge_lineage(&mut charge, snapshot.lineage())?;
        charge.bytes(32)?;
        charge.bytes(32)?;
        charge_actor(&mut charge, snapshot.actor())?;
        charge.bytes(32)?;
        for _ in snapshot.conflict_hashes() {
            charge.bytes(32)?;
        }
        for entity in snapshot.affected_entities() {
            charge.bytes(entity.key().as_bytes().len())?;
            charge.fields(1)?;
        }
        charge.repeated(snapshot.events())?;
        charge_declared_outcome(&mut charge, snapshot.outcome())?;
        charge.bytes(56)?;
        Ok(charge.finish())
    }
}

impl ServiceResponseCharge for GetCommitResult {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.fields(1)?;
        if let Self::Found(commit) = self {
            charge.nested(commit.as_ref())?;
        }
        Ok(charge.finish())
    }
}

impl ServiceResponseCharge for CommitScanFence {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        fixed_charge(2)
    }
}

impl ServiceResponseCharge for ScanCommitsResult {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.nested(self.page())?;
        Ok(charge.finish())
    }
}

impl ServiceResponseCharge for CommitSubscriptionEvent {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.fields(1)?;
        match self {
            Self::Commit(commit) => charge.nested(commit.as_ref())?,
            Self::Terminal(_) => charge.fields(3)?,
        }
        Ok(charge.finish())
    }
}

impl ServiceResponseCharge for SubscribeToCommitsResult {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        // Establishment releases only the opaque in-process continuation.
        fixed_charge(1)
    }
}

impl ServiceResponseCharge for ProvenanceView {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let snapshot = self.as_snapshot();
        let mut charge = ChargeAccumulator::message();
        charge.fields(7)?;
        charge.bytes(16)?;
        charge.bytes(16)?;
        charge_lineage(&mut charge, snapshot.lineage())?;
        charge.bytes(32)?;
        charge_actor(&mut charge, snapshot.actor())?;
        for entity in snapshot.affected_entities() {
            charge.bytes(entity.key().as_bytes().len())?;
            charge.fields(1)?;
        }
        charge.fields(
            snapshot
                .event_ids()
                .len()
                .checked_mul(2)
                .ok_or(ServiceResponseChargeOverflow)?,
        )?;
        charge_claims(&mut charge, snapshot.claims())?;
        Ok(charge.finish())
    }
}

impl ServiceResponseCharge for TraceProvenanceResult {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.fields(1)?;
        if let Self::Found(provenance) = self {
            charge.nested(provenance.as_ref())?;
        }
        Ok(charge.finish())
    }
}

fn charge_health_report(
    charge: &mut ChargeAccumulator,
    report: &HealthReport,
) -> Result<(), ServiceResponseChargeOverflow> {
    charge.fields(4)?;
    charge.fields(
        report
            .components()
            .len()
            .checked_mul(3)
            .ok_or(ServiceResponseChargeOverflow)?,
    )?;
    charge_build(charge, report.build())
}

impl ServiceResponseCharge for HealthResult {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.fields(1)?;
        match self {
            Self::PreBootstrap(_) => charge.fields(4)?,
            Self::Authenticated(report) => charge_health_report(&mut charge, report)?,
        }
        Ok(charge.finish())
    }
}

impl ServiceResponseCharge for StatisticsResult {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        fixed_charge(5)
    }
}

impl ServiceResponseCharge for CreateCapabilityResult {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.fields(1)?;
        match self {
            Self::Normal(result) => match result {
                NormalCreateCapabilityResult::Created { token, .. } => {
                    charge.fields(5)?;
                    charge.bytes(token.expose_secret().len())?;
                }
                NormalCreateCapabilityResult::AlreadyCreatedTokenUnavailable(_) => {
                    charge.fields(3)?;
                }
                NormalCreateCapabilityResult::CapabilityIdConflict => charge.fields(1)?,
            },
            Self::Bootstrap(_) => charge.fields(5)?,
        }
        Ok(charge.finish())
    }
}

impl ServiceResponseCharge for RevokeCapabilityResult {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        fixed_charge(6)
    }
}

impl ServiceResponseCharge for OutboxDeliverySummary {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        fixed_charge(6)
    }
}

impl ServiceResponseCharge for ListPendingOutboxDeliveriesResult {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.nested(self.page())?;
        Ok(charge.finish())
    }
}

impl ServiceResponseCharge for CommandToolDescriptor {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.bytes(self.name().as_str().len())?;
        charge.bytes(self.source_command().as_str().len())?;
        charge_lineage(&mut charge, self.lineage())?;
        charge.fields(2)?;
        charge_schema(&mut charge, self.input_schema())?;
        charge_schema(&mut charge, self.outcome_schema())?;
        Ok(charge.finish())
    }
}

impl ServiceResponseCharge for DiscoveryCatalogFence {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.fields(1)?;
        if let Self::ActiveContract {
            lineage,
            bundle_hash: _,
            version: _,
        } = self
        {
            charge.fields(2)?;
            charge_lineage(&mut charge, lineage)?;
            charge.bytes(32)?;
        }
        Ok(charge.finish())
    }
}

impl ServiceResponseCharge for CommandToolDiscoveryItem {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.fields(1)?;
        if let Self::Command(descriptor) = self {
            charge.nested(descriptor.as_ref())?;
        }
        Ok(charge.finish())
    }
}

impl ServiceResponseCharge for DiscoverCommandToolsResult {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.nested(self.page())?;
        Ok(charge.finish())
    }
}

impl ServiceResponseCharge for ResourceDescriptor {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.fields(3)?;
        // URI and MIME/class text are deterministic functions of this identity.
        // Four times the canonical key dominates their accepted textual forms.
        if let Some(target) = self.target() {
            let key = target.canonical_key();
            charge.bytes(
                key.len()
                    .checked_mul(4)
                    .ok_or(ServiceResponseChargeOverflow)?,
            )?;
        }
        if let Some(schema) = self.schema() {
            charge_schema(&mut charge, schema)?;
        }
        Ok(charge.finish())
    }
}

impl ServiceResponseCharge for DiscoverResourcesResult {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.nested(self.page())?;
        Ok(charge.finish())
    }
}

impl ServiceResponseCharge for ServiceAuditTargetV1 {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.bytes(self.canonical_key().len())?;
        Ok(charge.finish())
    }
}

macro_rules! seal_response_types {
    ($($type:ty),+ $(,)?) => {
        $(impl sealed::Sealed for $type {})+
    };
}

seal_response_types!(
    (),
    CanonicalValue,
    CanonicalRecord,
    ContractValidationResult,
    ExplainCommandResult,
    DeployContractResult,
    GetActiveContractResult,
    GetContractVersionResult,
    ExecuteCommandResult,
    ResolveCommandOutcomeResult,
    GetEntityResult,
    IndexRowView,
    IndexScanFence,
    ScanIndexResult,
    ProjectionRow,
    ProjectionPageFence,
    QueryProjectionResult,
    GetProjectionStatusResult,
    DurableEventView,
    CommitView,
    GetCommitResult,
    CommitScanFence,
    ScanCommitsResult,
    CommitSubscriptionEvent,
    SubscribeToCommitsResult,
    ProvenanceView,
    TraceProvenanceResult,
    HealthResult,
    StatisticsResult,
    CreateCapabilityResult,
    RevokeCapabilityResult,
    OutboxDeliverySummary,
    ListPendingOutboxDeliveriesResult,
    CommandToolDescriptor,
    CommandToolDiscoveryItem,
    DiscoveryCatalogFence,
    DiscoverCommandToolsResult,
    ResourceDescriptor,
    DiscoverResourcesResult,
    ServiceAuditTargetV1,
);

impl<T, F> sealed::Sealed for Page<T, F>
where
    T: ServiceResponseCharge,
    F: ServiceResponseCharge,
{
}

impl<T, F> ServiceResponseCharge for Page<T, F>
where
    T: ServiceResponseCharge,
    F: ServiceResponseCharge,
{
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.repeated(self.items())?;
        if let Some(cursor) = self.next_cursor() {
            charge.bytes(cursor.as_bytes().len())?;
        }
        charge.nested(self.observed_fence())?;
        Ok(charge.finish())
    }
}

/// Whole-item page selection under the exact response ceiling.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PageFit {
    item_count: usize,
    has_more: bool,
}

impl PageFit {
    pub(crate) const fn item_count(self) -> usize {
        self.item_count
    }

    pub(crate) const fn has_more(self) -> bool {
        self.has_more
    }
}

/// Selects the longest complete prefix that fits, reserving an opaque cursor
/// whenever the selected page is not the exact end.
pub(crate) fn fit_page_items<T, F>(
    items: &[T],
    fence: &F,
    lower_has_more: bool,
) -> Result<PageFit, ServiceFailure>
where
    T: ServiceResponseCharge,
    F: ServiceResponseCharge,
{
    fit_page_items_with_empty_progress(items, fence, lower_has_more, false)
}

/// Selects a bounded index prefix while retaining lower physical progress for
/// an intentionally empty visible page.
pub(crate) fn fit_sparse_page_items<T, F>(
    items: &[T],
    fence: &F,
    lower_has_more: bool,
) -> Result<PageFit, ServiceFailure>
where
    T: ServiceResponseCharge,
    F: ServiceResponseCharge,
{
    fit_page_items_with_empty_progress(items, fence, lower_has_more, true)
}

fn fit_page_items_with_empty_progress<T, F>(
    items: &[T],
    fence: &F,
    lower_has_more: bool,
    permit_empty_progress: bool,
) -> Result<PageFit, ServiceFailure>
where
    T: ServiceResponseCharge,
    F: ServiceResponseCharge,
{
    if items.is_empty() {
        return Ok(PageFit {
            item_count: 0,
            has_more: permit_empty_progress && lower_has_more,
        });
    }

    let mut base_without_cursor = ChargeAccumulator::message();
    base_without_cursor
        .add(PAGE_WRAPPER_RESERVE)
        .map_err(|_| ServiceFailure::ResponseTooLarge)?;
    base_without_cursor
        .nested(fence)
        .map_err(|_| ServiceFailure::ResponseTooLarge)?;

    if !lower_has_more {
        let mut exact_end = base_without_cursor;
        let all_fit = items
            .iter()
            .all(|item| exact_end.nested(item).is_ok() && exact_end.finish().fits());
        if all_fit {
            return Ok(PageFit {
                item_count: items.len(),
                has_more: false,
            });
        }
    }

    let mut base_with_cursor = base_without_cursor;
    base_with_cursor
        .bytes(
            CursorToken::from_bytes([0; crate::CURSOR_TOKEN_BYTES])
                .as_bytes()
                .len(),
        )
        .map_err(|_| ServiceFailure::ResponseTooLarge)?;

    let mut count = 0;
    let mut charge = base_with_cursor;
    for item in items {
        let mut candidate = charge;
        if candidate.nested(item).is_err() || !candidate.finish().fits() {
            break;
        }
        charge = candidate;
        count += 1;
    }

    if count == 0 {
        return Err(ServiceFailure::ResponseTooLarge);
    }
    Ok(PageFit {
        item_count: count,
        has_more: count < items.len() || lower_has_more,
    })
}

pub(crate) fn ensure_response_budget<T: ServiceResponseCharge + ?Sized>(
    result: &T,
) -> Result<(), ServiceFailure> {
    match result.service_response_charge_v1() {
        Ok(charge) if charge.fits() => Ok(()),
        Ok(_) | Err(_) => Err(ServiceFailure::ResponseTooLarge),
    }
}

pub(crate) fn ensure_subscription_establishment_budget() -> Result<(), ServiceFailure> {
    match fixed_charge(1) {
        Ok(charge) if charge.fits() => Ok(()),
        Ok(_) | Err(_) => Err(ServiceFailure::ResponseTooLarge),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::fmt::Write as _;
    use std::num::NonZeroU64;

    use riffdb_auth::CapabilityTokenText;
    use riffdb_contract_compiler::{compile_contract_source, validate_contract_source};
    use riffdb_contract_ir::{
        ExecutionClass, OutcomeSchema, RecordSchema, RecordTypeRef, SchemaArtifactKey, SchemaIr,
    };
    use riffdb_types::{
        ActorId, ActorKind, AdministrationSequence, CanonicalInputHash, CanonicalString,
        CapabilityId, CommandId, CommitSequence, ContractBundleHash, ContractPlanRootHash,
        ContractVersion, EntityKey, EntityVersion, FieldId, FrontierPosition, IndexEntryKey,
        IndexEpochPosition, LogicalTime, OutcomeId, PartitionKeyHash, PlanHash,
        ProjectionGeneration, ProjectionId, ProjectionPlanHash, ProvenanceId, RequestId,
        SourceHash, Timestamp,
    };

    use super::*;
    use crate::{
        AffectedEntityView, AuthoritativeCommitSnapshot, CapabilityIdentityView,
        CapabilityTransitionView, CommandDurability, CommitScanFence, CommitSubscriptionEndReason,
        CommitSubscriptionTerminal, ComponentHealth, ExplainedCommand, HealthComponentKind,
        HealthComponentStatus, IndexScanFence, JournaledCommandResult, JournaledCompletion,
        OperationalHealthSnapshot, OperationalStatisticsSnapshot, OutcomePlanBinding, PageLimit,
        PreBootstrapHealthReport, PreBootstrapLifecycle, QueryProjectionReady,
        ReadOnlyCommandResult, RecoveredJournaledCommandResult,
    };

    const RESPONSE_CHARGE_FIXTURE: &str = include_str!("../fixtures/response-charge-v1.tsv");
    const RESPONSE_CHARGE_FIXTURE_VERSION: u16 = 1;

    // These are the sixteen public gRPC response/item families in WP-127 plus
    // the public ProjectionStatus message embedded by projection responses.
    const WP127_RESPONSE_FAMILIES: [&str; 17] = [
        "commit_notification",
        "contract_validation",
        "create_capability",
        "deploy_contract",
        "execute_command",
        "explain_command",
        "get_active_contract",
        "get_commit",
        "get_entity",
        "get_outcome",
        "health",
        "projection_status",
        "query_projection",
        "revoke_capability",
        "scan_commits",
        "scan_index",
        "stats",
    ];

    #[derive(Clone, Copy)]
    struct ExactCharge(usize);

    impl sealed::Sealed for ExactCharge {}

    impl ServiceResponseCharge for ExactCharge {
        fn service_response_charge_v1(
            &self,
        ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
            Ok(ServiceResponseChargeV1(self.0))
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum FixtureDisposition {
        Release,
        ResponseTooLarge,
    }

    impl FixtureDisposition {
        const fn from_charge(charge: ServiceResponseChargeV1) -> Self {
            if charge.fits() {
                Self::Release
            } else {
                Self::ResponseTooLarge
            }
        }

        const fn as_str(self) -> &'static str {
            match self {
                Self::Release => "release",
                Self::ResponseTooLarge => "response_too_large",
            }
        }
    }

    #[derive(Debug, Eq, PartialEq)]
    struct FixtureCase {
        case_id: &'static str,
        family: &'static str,
        variant: &'static str,
        shape: String,
        charge_bytes: usize,
        disposition: FixtureDisposition,
    }

    impl FixtureCase {
        fn from_response<T: ServiceResponseCharge>(
            case_id: &'static str,
            family: &'static str,
            variant: &'static str,
            shape: String,
            response: &T,
        ) -> Self {
            let charge = response
                .service_response_charge_v1()
                .expect("fixture response charge must be representable");
            Self {
                case_id,
                family,
                variant,
                shape,
                charge_bytes: charge.bytes(),
                disposition: FixtureDisposition::from_charge(charge),
            }
        }
    }

    macro_rules! fixture_shape {
        ($($key:literal => $value:expr),+ $(,)?) => {{
            let mut entries = BTreeMap::new();
            $(
                assert!(
                    entries
                        .insert($key.to_owned(), ($value).to_string())
                        .is_none(),
                    "duplicate fixture-shape key"
                );
            )+
            render_fixture_shape(entries)
        }};
    }

    fn render_fixture_shape(entries: BTreeMap<String, String>) -> String {
        entries
            .into_iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join(",")
    }

    fn fixture_lineage() -> ContractLineage {
        ContractLineage::new("fixture").expect("fixture lineage is bounded")
    }

    fn fixture_contract_version() -> ContractVersion {
        ContractVersion::new(1).expect("fixture contract version is nonzero")
    }

    fn fixture_contract_descriptor() -> ContractDescriptor {
        ContractDescriptor::new(
            fixture_lineage(),
            fixture_contract_version(),
            ContractBundleHash::from_bytes([1; 32]),
            SourceHash::from_bytes([2; 32]),
            ContractPlanRootHash::from_bytes([3; 32]),
        )
    }

    fn fixture_outcome(execution_class: ExecutionClass) -> DeclaredOutcomeView {
        let command_id = CommandId::first();
        let outcome_id = OutcomeId::first();
        let schema = SchemaIr::new(Vec::new(), Vec::new(), Vec::new(), Vec::new())
            .expect("empty schema is structurally valid");
        let payload = RecordSchema::new(
            RecordTypeRef::CommandOutcome {
                command_id,
                outcome_id,
            },
            Vec::new(),
        )
        .expect("empty outcome record schema");
        let outcome_schema = OutcomeSchema::new(command_id, outcome_id, "Completed", payload)
            .expect("bounded outcome schema");
        DeclaredOutcomeView::from_checked_schema(
            OutcomePlanBinding::new(
                fixture_lineage(),
                fixture_contract_version(),
                command_id,
                PlanHash::from_bytes([4; 32]),
                execution_class,
            ),
            &schema,
            &outcome_schema,
            CanonicalRecord::new(Vec::new()).expect("empty canonical outcome"),
        )
        .expect("outcome matches its checked schema")
    }

    fn fixture_journaled(completion: JournaledCompletion) -> JournaledCommandResult {
        JournaledCommandResult::new(
            completion,
            CommitSequence::first(),
            fixture_outcome(ExecutionClass::IdempotentMutation),
            ProvenanceId::from_unix_milliseconds_and_random(1, [2; 10])
                .expect("fixture provenance UUIDv7"),
            CommandDurability::Synchronous,
        )
        .expect("fixture journaled result has a mutating outcome")
    }

    fn fixture_record(string_bytes: usize) -> CanonicalRecord {
        CanonicalRecord::new(vec![(
            FieldId::first(),
            CanonicalValue::String(
                CanonicalString::new("x".repeat(string_bytes))
                    .expect("fixture canonical string is bounded"),
            ),
        )])
        .expect("fixture record has one unique field")
    }

    fn fixture_entity_key(byte_count: usize, ordinal: u32) -> EntityKey {
        assert!((10..=4_096).contains(&byte_count));
        let mut bytes = vec![0; byte_count];
        bytes[0] = 0x45;
        bytes[1] = 0x01;
        bytes[2..6].copy_from_slice(&1_u32.to_be_bytes());
        bytes[byte_count - 4..].copy_from_slice(&ordinal.to_be_bytes());
        EntityKey::from_bytes(bytes).expect("fixture entity-key envelope is bounded")
    }

    fn fixture_index_key(byte_count: usize, ordinal: u32) -> IndexEntryKey {
        assert!((10..=4_096).contains(&byte_count));
        let mut bytes = vec![0; byte_count];
        bytes[0] = 0x49;
        bytes[1] = 0x01;
        bytes[2..6].copy_from_slice(&1_u32.to_be_bytes());
        bytes[byte_count - 4..].copy_from_slice(&ordinal.to_be_bytes());
        IndexEntryKey::from_bytes(bytes).expect("fixture index-key envelope is bounded")
    }

    fn fixture_commit(affected_entity_count: u32, entity_key_bytes: usize) -> CommitView {
        let affected_entities = (0..affected_entity_count)
            .map(|ordinal| {
                AffectedEntityView::new(
                    fixture_entity_key(entity_key_bytes, ordinal),
                    EntityVersion::first(),
                )
            })
            .collect();
        let snapshot = AuthoritativeCommitSnapshot::new(
            CommitSequence::first(),
            RequestId::from_unix_milliseconds_and_random(1, [1; 10])
                .expect("fixture request UUIDv7"),
            fixture_lineage(),
            fixture_contract_version(),
            CommandId::first(),
            PlanHash::from_bytes([4; 32]),
            CanonicalInputHash::from_bytes([5; 32]),
            AdmittedActorContext::new(
                ActorId::new("fixture-actor").expect("fixture actor is bounded"),
                ActorKind::Human,
                TenantScope::Global,
                None,
            ),
            LogicalTime::new(Timestamp::new(1, 0).expect("canonical fixture timestamp")),
            PartitionKeyHash::from_bytes([6; 32]),
            Vec::new(),
            affected_entities,
            Vec::new(),
            fixture_outcome(ExecutionClass::IdempotentMutation),
            ProvenanceId::from_unix_milliseconds_and_random(1, [2; 10])
                .expect("fixture provenance UUIDv7"),
            CommandDurability::Synchronous,
        )
        .expect("fixture authoritative commit is bounded");
        CommitView::new(snapshot)
    }

    fn invalid_validation_fixture() -> (ContractValidationResult, String) {
        let error = validate_contract_source("").expect_err("empty source is invalid");
        let diagnostics = error.syntax().expect("empty source is a syntax failure");
        let mut shape = BTreeMap::new();
        shape.insert("diagnostic_count".to_owned(), diagnostics.len().to_string());
        shape.insert("diagnostic_kind".to_owned(), "syntax".to_owned());
        for (index, diagnostic) in diagnostics.as_slice().iter().enumerate() {
            let prefix = format!("diagnostic_{index}");
            shape.insert(
                format!("{prefix}_code"),
                diagnostic.code().as_str().to_owned(),
            );
            shape.insert(
                format!("{prefix}_code_bytes"),
                diagnostic.code().as_str().len().to_string(),
            );
            shape.insert(
                format!("{prefix}_expected_count"),
                diagnostic.expected().len().to_string(),
            );
            for (expected_index, expected) in diagnostic.expected().iter().enumerate() {
                shape.insert(
                    format!("{prefix}_expected_{expected_index}_bytes"),
                    expected.len().to_string(),
                );
            }
            shape.insert(
                format!("{prefix}_help_bytes"),
                diagnostic.code().help().map_or(0, str::len).to_string(),
            );
            shape.insert(
                format!("{prefix}_span_end"),
                diagnostic.span().end().to_string(),
            );
            shape.insert(
                format!("{prefix}_span_start"),
                diagnostic.span().start().to_string(),
            );
            shape.insert(
                format!("{prefix}_summary_bytes"),
                diagnostic.code().summary().len().to_string(),
            );
        }
        (
            ContractValidationResult::Invalid(error),
            render_fixture_shape(shape),
        )
    }

    fn explained_command_fixture() -> (ExplainCommandResult, String) {
        let bundle =
            compile_contract_source(include_str!("../../../contracts/examples/budget.riff"))
                .expect("checked-in budget example compiles");
        let plan = bundle.commands().first().expect("budget has commands");
        let command_id = plan.command_id();
        let input_schema = bundle
            .schema_artifacts()
            .iter()
            .find(|artifact| artifact.key() == SchemaArtifactKey::CommandInput(command_id))
            .cloned()
            .expect("budget command input schema");
        let outcome_schema = bundle
            .schema_artifacts()
            .iter()
            .find(|artifact| artifact.key() == SchemaArtifactKey::CommandOutcomeUnion(command_id))
            .cloned()
            .expect("budget command outcome schema");
        let explanation = CommandExplain::from_plan(plan);
        let shape = fixture_shape! {
            "binding_count" => explanation.bindings().len(),
            "command_id" => command_id.get(),
            "contract_version" => bundle.contract_version().get(),
            "event_count" => explanation.events().len(),
            "input_schema_json_bytes" => input_schema.canonical_json().len(),
            "invariant_count" => explanation.invariants().len(),
            "lineage_bytes" => bundle.lineage().as_bytes().len(),
            "outcome_count" => explanation.outcomes().len(),
            "outcome_schema_json_bytes" => outcome_schema.canonical_json().len(),
            "plan_hash_bytes" => plan.plan_hash().as_bytes().len(),
            "read_field_count" => explanation.read_fields().len(),
            "rendered_explain_bytes" => explanation.render_text().len(),
            "schema_hash_bytes" => 32,
            "write_field_count" => explanation.write_fields().len()
        };
        let descriptor = ContractDescriptor::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
            bundle.source_hash(),
            bundle.plan_root_hash(),
        );
        let explained = ExplainedCommand::new(
            descriptor,
            command_id,
            plan.plan_hash(),
            explanation,
            input_schema,
            outcome_schema,
        )
        .expect("fixture explanation identities agree");
        (ExplainCommandResult::Found(Box::new(explained)), shape)
    }

    fn fixture_projection_identity() -> ProjectionIdentity {
        ProjectionIdentity::new(
            fixture_lineage(),
            ProjectionId::first(),
            ProjectionPlanHash::from_bytes([7; 32]),
        )
    }

    fn fixture_health_report() -> HealthReport {
        let components = vec![
            ComponentHealth::new(
                HealthComponentKind::AuthoritativeStorage,
                HealthComponentStatus::Healthy,
            ),
            ComponentHealth::new(HealthComponentKind::Catalog, HealthComponentStatus::Healthy),
            ComponentHealth::new(
                HealthComponentKind::CommitCoordinator,
                HealthComponentStatus::Healthy,
            ),
        ];
        let operational =
            OperationalHealthSnapshot::new(components).expect("unique health components");
        let build = BuildInfo::new(
            "0.1.0",
            "0123456789abcdef",
            "1.97.0",
            vec!["mcp".to_owned(), "redb".to_owned()],
            1,
            1,
            "2025-06-18",
        )
        .expect("fixture build metadata is canonical");
        HealthReport::new(
            Some(fixture_contract_version()),
            Some(CommitSequence::first()),
            operational,
            Timestamp::new(1, 0).expect("canonical fixture timestamp"),
            build,
        )
    }

    fn fixture_capability_transition() -> CapabilityTransitionView {
        let identity = CapabilityIdentityView::new(
            CapabilityId::from_unix_milliseconds_and_random(1, [8; 10])
                .expect("fixture capability UUIDv7"),
            NonZeroU64::new(1).expect("fixture capability revision is nonzero"),
        );
        CapabilityTransitionView::new(identity, AdministrationSequence::first())
    }

    #[allow(clippy::too_many_lines)]
    fn response_fixture_cases() -> Vec<FixtureCase> {
        let small_commit = fixture_commit(2, 24);
        let oversized_commit = fixture_commit(1_024, 4_096);
        let mut cases = Vec::new();

        cases.push(FixtureCase::from_response(
            "boundary.exact_ceiling",
            "boundary",
            "exact_ceiling",
            fixture_shape! { "synthetic_charge_bytes" => MAX_SERVICE_RESPONSE_BYTES },
            &ExactCharge(MAX_SERVICE_RESPONSE_BYTES),
        ));
        cases.push(FixtureCase::from_response(
            "boundary.one_over",
            "boundary",
            "one_over",
            fixture_shape! { "synthetic_charge_bytes" => MAX_SERVICE_RESPONSE_BYTES + 1 },
            &ExactCharge(MAX_SERVICE_RESPONSE_BYTES + 1),
        ));

        cases.push(FixtureCase::from_response(
            "commit_notification.commit_oversize",
            "commit_notification",
            "commit",
            fixture_shape! {
                "affected_entity_count" => 1_024,
                "actor_id_bytes" => 13,
                "conflict_hash_count" => 0,
                "entity_key_bytes" => 4_096,
                "event_count" => 0,
                "lineage_bytes" => 7,
                "outcome_field_count" => 0,
                "outcome_name_bytes" => 9,
                "tenant_scope" => "global"
            },
            &CommitSubscriptionEvent::Commit(Box::new(oversized_commit.clone())),
        ));
        cases.push(FixtureCase::from_response(
            "commit_notification.commit_representative",
            "commit_notification",
            "commit",
            fixture_shape! {
                "affected_entity_count" => 2,
                "actor_id_bytes" => 13,
                "conflict_hash_count" => 0,
                "entity_key_bytes" => 24,
                "event_count" => 0,
                "lineage_bytes" => 7,
                "outcome_field_count" => 0,
                "outcome_name_bytes" => 9,
                "tenant_scope" => "global"
            },
            &CommitSubscriptionEvent::Commit(Box::new(small_commit.clone())),
        ));
        cases.push(FixtureCase::from_response(
            "commit_notification.terminal",
            "commit_notification",
            "terminal",
            fixture_shape! {
                "reason" => "lifetime_elapsed",
                "resume_frontier" => "applied_through",
                "resume_sequence" => 1
            },
            &CommitSubscriptionEvent::Terminal(CommitSubscriptionTerminal::new(
                CommitSubscriptionEndReason::LifetimeElapsed,
                FrontierPosition::AppliedThrough(CommitSequence::first()),
            )),
        ));

        let (invalid_validation, invalid_validation_shape) = invalid_validation_fixture();
        cases.push(FixtureCase::from_response(
            "contract_validation.invalid_empty_source",
            "contract_validation",
            "invalid",
            invalid_validation_shape,
            &invalid_validation,
        ));
        cases.push(FixtureCase::from_response(
            "contract_validation.valid",
            "contract_validation",
            "valid",
            "none".to_owned(),
            &ContractValidationResult::Valid,
        ));

        let capability_transition = fixture_capability_transition();
        let capability_token = CapabilityTokenText::parse(&[b'A'; 43])
            .expect("43 'A' bytes are canonical base64url token text");
        cases.push(FixtureCase::from_response(
            "create_capability.created",
            "create_capability",
            "normal_created",
            fixture_shape! {
                "administration_sequence" => 1,
                "capability_id_bytes" => 16,
                "revision" => 1,
                "token_text_bytes" => 43
            },
            &CreateCapabilityResult::Normal(NormalCreateCapabilityResult::Created {
                transition: capability_transition,
                token: capability_token,
            }),
        ));

        cases.push(FixtureCase::from_response(
            "deploy_contract.activated",
            "deploy_contract",
            "activated",
            fixture_shape! {
                "bundle_hash_bytes" => 32,
                "contract_version" => 1,
                "lineage_bytes" => 7,
                "plan_root_hash_bytes" => 32,
                "source_hash_bytes" => 32
            },
            &DeployContractResult::Activated(fixture_contract_descriptor()),
        ));

        cases.push(FixtureCase::from_response(
            "execute_command.journaled",
            "execute_command",
            "journaled",
            fixture_shape! {
                "commit_sequence" => 1,
                "completion" => "committed",
                "contract_version" => 1,
                "durability" => "synchronous",
                "lineage_bytes" => 7,
                "outcome_field_count" => 0,
                "outcome_name_bytes" => 9,
                "plan_hash_bytes" => 32,
                "provenance_locator_bytes" => 56
            },
            &ExecuteCommandResult::Journaled(fixture_journaled(JournaledCompletion::Committed)),
        ));
        let read_only = ReadOnlyCommandResult::new(fixture_outcome(ExecutionClass::ReadOnly))
            .expect("fixture read-only result has a read-only outcome");
        cases.push(FixtureCase::from_response(
            "execute_command.read_only",
            "execute_command",
            "read_only",
            fixture_shape! {
                "contract_version" => 1,
                "lineage_bytes" => 7,
                "outcome_field_count" => 0,
                "outcome_name_bytes" => 9,
                "plan_hash_bytes" => 32
            },
            &ExecuteCommandResult::ReadOnlyExecuted(read_only),
        ));

        let (explained, explained_shape) = explained_command_fixture();
        cases.push(FixtureCase::from_response(
            "explain_command.found_budget",
            "explain_command",
            "found",
            explained_shape,
            &explained,
        ));

        cases.push(FixtureCase::from_response(
            "get_active_contract.present",
            "get_active_contract",
            "present",
            fixture_shape! {
                "bundle_hash_bytes" => 32,
                "contract_version" => 1,
                "lineage_bytes" => 7,
                "plan_root_hash_bytes" => 32,
                "source_hash_bytes" => 32
            },
            &GetActiveContractResult::Present(fixture_contract_descriptor()),
        ));

        cases.push(FixtureCase::from_response(
            "get_commit.found_oversize",
            "get_commit",
            "found",
            fixture_shape! {
                "affected_entity_count" => 1_024,
                "actor_id_bytes" => 13,
                "conflict_hash_count" => 0,
                "entity_key_bytes" => 4_096,
                "event_count" => 0,
                "lineage_bytes" => 7,
                "outcome_field_count" => 0,
                "outcome_name_bytes" => 9,
                "tenant_scope" => "global"
            },
            &GetCommitResult::Found(Box::new(oversized_commit)),
        ));
        cases.push(FixtureCase::from_response(
            "get_commit.found_representative",
            "get_commit",
            "found",
            fixture_shape! {
                "affected_entity_count" => 2,
                "actor_id_bytes" => 13,
                "conflict_hash_count" => 0,
                "entity_key_bytes" => 24,
                "event_count" => 0,
                "lineage_bytes" => 7,
                "outcome_field_count" => 0,
                "outcome_name_bytes" => 9,
                "tenant_scope" => "global"
            },
            &GetCommitResult::Found(Box::new(small_commit.clone())),
        ));

        cases.push(FixtureCase::from_response(
            "get_entity.found_record",
            "get_entity",
            "found",
            fixture_shape! {
                "entity_key_bytes" => 24,
                "entity_version" => 1,
                "field_count" => 1,
                "field_id" => 1,
                "field_value_kind" => "string",
                "field_value_string_bytes" => 17,
                "written_by_contract" => 1
            },
            &GetEntityResult::Found(EntityView::new(
                fixture_entity_key(24, 1),
                EntityVersion::first(),
                fixture_contract_version(),
                fixture_record(17),
            )),
        ));

        let replayed =
            RecoveredJournaledCommandResult::new(fixture_journaled(JournaledCompletion::Replayed))
                .expect("fixture outcome recovery is a replay");
        cases.push(FixtureCase::from_response(
            "get_outcome.found_replayed",
            "get_outcome",
            "found",
            fixture_shape! {
                "commit_sequence" => 1,
                "completion" => "replayed",
                "contract_version" => 1,
                "durability" => "synchronous",
                "lineage_bytes" => 7,
                "outcome_field_count" => 0,
                "outcome_name_bytes" => 9,
                "plan_hash_bytes" => 32,
                "provenance_locator_bytes" => 56
            },
            &ResolveCommandOutcomeResult::Found(Box::new(replayed)),
        ));

        cases.push(FixtureCase::from_response(
            "health.authenticated",
            "health",
            "authenticated",
            fixture_shape! {
                "active_contract_version" => 1,
                "build_feature_0_bytes" => 3,
                "build_feature_1_bytes" => 4,
                "build_feature_count" => 2,
                "component_count" => 3,
                "contract_ir_version" => 1,
                "git_revision_bytes" => 16,
                "last_commit_sequence" => 1,
                "mcp_baseline_bytes" => 10,
                "rust_version_bytes" => 6,
                "semantic_version_bytes" => 5,
                "storage_format_version" => 1
            },
            &HealthResult::Authenticated(fixture_health_report()),
        ));
        cases.push(FixtureCase::from_response(
            "health.prebootstrap",
            "health",
            "prebootstrap",
            fixture_shape! {
                "lifecycle" => "initializing_bootstrap",
                "liveness" => "true",
                "readiness" => "false"
            },
            &HealthResult::PreBootstrap(PreBootstrapHealthReport::new(
                PreBootstrapLifecycle::InitializingBootstrap,
                true,
            )),
        ));

        let projection_status = ProjectionStatusSnapshot::uninitialized(
            fixture_projection_identity(),
            FrontierPosition::AppliedThrough(CommitSequence::first()),
        );
        cases.push(FixtureCase::from_response(
            "projection_status.uninitialized",
            "projection_status",
            "found",
            fixture_shape! {
                "authoritative_frontier" => "applied_through",
                "authoritative_sequence" => 1,
                "candidate_present" => "false",
                "failure_present" => "false",
                "lifecycle" => "building",
                "lineage_bytes" => 7,
                "plan_hash_bytes" => 32,
                "projection_id" => 1,
                "published_present" => "false"
            },
            &GetProjectionStatusResult::Found(projection_status),
        ));

        let projection_frontier = FrontierPosition::AppliedThrough(CommitSequence::first());
        let projection_row = ProjectionRow::new(
            vec![CanonicalValue::String(
                CanonicalString::new("north").expect("fixture canonical string is bounded"),
            )],
            fixture_record(11),
        )
        .expect("fixture projection row is bounded");
        let projection_fence = ProjectionPageFence::new(
            fixture_projection_identity(),
            ProjectionGeneration::first(),
            projection_frontier,
        );
        let projection_page = Page::new(
            PageLimit::new(1).expect("fixture page limit"),
            vec![projection_row],
            Some(CursorToken::from_bytes([9; 16])),
            projection_fence,
        )
        .expect("fixture projection page is bounded");
        let projection_ready = QueryProjectionReady::new(projection_page, projection_frontier)
            .expect("fixture projection fence and frontier agree");
        cases.push(FixtureCase::from_response(
            "query_projection.ready_page",
            "query_projection",
            "ready",
            fixture_shape! {
                "cursor_bytes" => 16,
                "field_count" => 1,
                "field_id" => 1,
                "field_value_kind" => "string",
                "field_value_string_bytes" => 11,
                "frontier" => "applied_through",
                "frontier_sequence" => 1,
                "generation" => 1,
                "group_component_0_kind" => "string",
                "group_component_0_string_bytes" => 5,
                "group_component_count" => 1,
                "item_count" => 1,
                "lineage_bytes" => 7,
                "plan_hash_bytes" => 32,
                "projection_id" => 1
            },
            &QueryProjectionResult::Ready(projection_ready),
        ));

        cases.push(FixtureCase::from_response(
            "revoke_capability.revoked",
            "revoke_capability",
            "revoked",
            fixture_shape! {
                "administration_sequence" => 1,
                "capability_id_bytes" => 16,
                "revision" => 1
            },
            &RevokeCapabilityResult::Revoked(capability_transition),
        ));

        let commit_page = Page::new(
            PageLimit::new(1).expect("fixture page limit"),
            vec![small_commit],
            Some(CursorToken::from_bytes([10; 16])),
            CommitScanFence::new(FrontierPosition::AppliedThrough(CommitSequence::first())),
        )
        .expect("fixture commit page is bounded");
        cases.push(FixtureCase::from_response(
            "scan_commits.page_with_cursor",
            "scan_commits",
            "page",
            fixture_shape! {
                "affected_entity_count_per_item" => 2,
                "actor_id_bytes_per_item" => 13,
                "cursor_bytes" => 16,
                "entity_key_bytes" => 24,
                "fence" => "applied_through",
                "fence_sequence" => 1,
                "item_count" => 1,
                "lineage_bytes_per_item" => 7,
                "outcome_field_count_per_item" => 0,
                "outcome_name_bytes_per_item" => 9
            },
            &ScanCommitsResult::new(commit_page),
        ));

        let index_rows = vec![
            IndexRowView::new(fixture_index_key(20, 1), fixture_record(7)),
            IndexRowView::new(fixture_index_key(20, 2), fixture_record(13)),
        ];
        let index_page = Page::new(
            PageLimit::new(2).expect("fixture page limit"),
            index_rows,
            Some(CursorToken::from_bytes([11; 16])),
            IndexScanFence::new(IndexEpochPosition::BeforeFirst),
        )
        .expect("fixture index page is bounded");
        cases.push(FixtureCase::from_response(
            "scan_index.page_with_cursor",
            "scan_index",
            "page",
            fixture_shape! {
                "cursor_bytes" => 16,
                "epoch" => 1,
                "field_count_per_item" => 1,
                "field_id_per_item" => 1,
                "field_value_kind" => "string",
                "index_key_bytes_per_item" => 20,
                "item_count" => 2,
                "item_0_string_bytes" => 7,
                "item_1_string_bytes" => 13
            },
            &ScanIndexResult::new(index_page),
        ));

        let statistics = StatisticsResult::new(
            7,
            3,
            OperationalStatisticsSnapshot::new(Some(CommitSequence::first()), Some(11), Some(2)),
        )
        .expect("fixture statistics are bounded");
        cases.push(FixtureCase::from_response(
            "stats.populated",
            "stats",
            "populated",
            fixture_shape! {
                "active_commit_subscribers" => 3,
                "active_cursors" => 7,
                "known_projections" => 2,
                "last_commit_sequence" => 1,
                "pending_outbox_deliveries" => 11
            },
            &statistics,
        ));

        cases.sort_unstable_by_key(|case| case.case_id);
        assert!(
            cases
                .windows(2)
                .all(|pair| pair[0].case_id < pair[1].case_id),
            "generated fixture case IDs must be unique"
        );
        cases
    }

    fn render_response_charge_fixture(cases: &[FixtureCase]) -> String {
        let mut rendered = String::new();
        writeln!(
            rendered,
            "riffdb_response_charge_fixture_version\t{RESPONSE_CHARGE_FIXTURE_VERSION}"
        )
        .expect("writing to String cannot fail");
        writeln!(
            rendered,
            "service_response_charge_version\t{SERVICE_RESPONSE_CHARGE_VERSION}"
        )
        .expect("writing to String cannot fail");
        writeln!(rendered, "ceiling_bytes\t{MAX_SERVICE_RESPONSE_BYTES}")
            .expect("writing to String cannot fail");
        rendered.push_str(
            "case_id\tresponse_family\tresponse_variant\tshape_v1\tservice_charge_bytes\tdisposition\n",
        );
        for case in cases {
            writeln!(
                rendered,
                "{}\t{}\t{}\t{}\t{}\t{}",
                case.case_id,
                case.family,
                case.variant,
                case.shape,
                case.charge_bytes,
                case.disposition.as_str(),
            )
            .expect("writing to String cannot fail");
        }
        rendered
    }

    #[derive(Debug)]
    struct ParsedFixtureRow<'a> {
        case_id: &'a str,
        family: &'a str,
    }

    fn assert_lower_snake_token(value: &str) {
        assert!(!value.is_empty(), "fixture token must not be empty");
        assert!(
            value
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'),
            "fixture token is not lower snake-case: {value}"
        );
        assert!(
            value.as_bytes()[0].is_ascii_lowercase(),
            "fixture token must start with a lower-case letter: {value}"
        );
    }

    fn parse_canonical_usize(value: &str) -> usize {
        assert!(
            value == "0" || !value.starts_with('0'),
            "fixture integer has a leading zero: {value}"
        );
        assert!(
            value.bytes().all(|byte| byte.is_ascii_digit()),
            "fixture integer is not base-10 ASCII: {value}"
        );
        let parsed = value.parse::<usize>().expect("fixture integer fits usize");
        assert_eq!(
            parsed.to_string(),
            value,
            "fixture integer is not canonical"
        );
        parsed
    }

    fn assert_fixture_shape(shape: &str) {
        if shape == "none" {
            return;
        }
        let mut previous = None;
        for entry in shape.split(',') {
            let (key, value) = entry
                .split_once('=')
                .expect("fixture shape entry contains one key/value separator");
            assert!(!value.contains('='), "fixture shape value contains '='");
            assert_lower_snake_token(key);
            assert!(
                !value.is_empty()
                    && value.bytes().all(|byte| {
                        byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':')
                    }),
                "fixture shape value has invalid syntax: {value}"
            );
            if let Some(previous) = previous {
                assert!(
                    previous < key,
                    "fixture shape keys are not sorted or unique"
                );
            }
            previous = Some(key);
        }
    }

    fn parse_response_charge_fixture(input: &str) -> Vec<ParsedFixtureRow<'_>> {
        assert!(input.is_ascii(), "response-charge fixture must be ASCII");
        assert!(!input.contains('\r'), "response-charge fixture must use LF");
        assert!(
            input.ends_with('\n'),
            "response-charge fixture must end in LF"
        );
        let lines = input.lines().collect::<Vec<_>>();
        assert!(lines.len() >= 5, "response-charge fixture has no cases");

        let parse_metadata = |line: &str, expected_key: &str| {
            let columns = line.split('\t').collect::<Vec<_>>();
            assert_eq!(columns.len(), 2, "fixture metadata must have two columns");
            assert_eq!(columns[0], expected_key, "unexpected fixture metadata key");
            parse_canonical_usize(columns[1])
        };
        assert_eq!(
            parse_metadata(lines[0], "riffdb_response_charge_fixture_version"),
            usize::from(RESPONSE_CHARGE_FIXTURE_VERSION),
        );
        assert_eq!(
            parse_metadata(lines[1], "service_response_charge_version"),
            usize::from(SERVICE_RESPONSE_CHARGE_VERSION),
        );
        assert_eq!(
            parse_metadata(lines[2], "ceiling_bytes"),
            MAX_SERVICE_RESPONSE_BYTES,
        );
        assert_eq!(
            lines[3],
            "case_id\tresponse_family\tresponse_variant\tshape_v1\tservice_charge_bytes\tdisposition"
        );

        let mut parsed = Vec::new();
        let mut previous_case_id = None;
        for line in &lines[4..] {
            assert!(!line.is_empty(), "fixture contains an empty row");
            let columns = line.split('\t').collect::<Vec<_>>();
            assert_eq!(columns.len(), 6, "fixture case must have six columns");
            let case_id = columns[0];
            let family = columns[1];
            let variant = columns[2];
            let shape = columns[3];
            let charge = columns[4];
            let disposition = columns[5];
            let (case_family, case_variant) = case_id
                .split_once('.')
                .expect("fixture case ID has family.variant syntax");
            assert!(
                !case_variant.contains('.'),
                "fixture case ID has extra segments"
            );
            assert_lower_snake_token(case_family);
            assert_lower_snake_token(case_variant);
            assert_eq!(
                case_family, family,
                "case ID family must match family column"
            );
            assert_lower_snake_token(family);
            assert_lower_snake_token(variant);
            assert_fixture_shape(shape);
            let charge = parse_canonical_usize(charge);
            let expected_disposition = if charge <= MAX_SERVICE_RESPONSE_BYTES {
                "release"
            } else {
                "response_too_large"
            };
            assert_eq!(disposition, expected_disposition);
            if let Some(previous) = previous_case_id {
                assert!(
                    previous < case_id,
                    "fixture case IDs are not sorted or unique"
                );
            }
            previous_case_id = Some(case_id);
            parsed.push(ParsedFixtureRow { case_id, family });
        }
        parsed
    }

    #[test]
    fn response_charge_fixture_is_strict_and_recomputed_from_named_dtos() {
        let cases = response_fixture_cases();
        let rendered = render_response_charge_fixture(&cases);
        assert_eq!(
            RESPONSE_CHARGE_FIXTURE, rendered,
            "regenerate the checked fixture only after reviewing a response-charge change"
        );
        let parsed = parse_response_charge_fixture(RESPONSE_CHARGE_FIXTURE);
        assert_eq!(parsed.len(), cases.len());
    }

    #[test]
    fn response_charge_fixture_covers_the_complete_wp127_response_registry() {
        let parsed = parse_response_charge_fixture(RESPONSE_CHARGE_FIXTURE);
        let actual_families = parsed
            .iter()
            .filter(|row| row.family != "boundary")
            .map(|row| row.family)
            .collect::<BTreeSet<_>>();
        let expected_families = WP127_RESPONSE_FAMILIES.into_iter().collect::<BTreeSet<_>>();
        assert_eq!(actual_families, expected_families);
        assert!(
            parsed
                .iter()
                .any(|row| row.case_id == "boundary.exact_ceiling")
        );
        assert!(parsed.iter().any(|row| row.case_id == "boundary.one_over"));
    }

    #[test]
    fn exact_ceiling_fits_and_one_byte_over_is_withheld() {
        assert!(ensure_response_budget(&ExactCharge(MAX_SERVICE_RESPONSE_BYTES)).is_ok());
        assert!(matches!(
            ensure_response_budget(&ExactCharge(MAX_SERVICE_RESPONSE_BYTES + 1)),
            Err(ServiceFailure::ResponseTooLarge)
        ));
    }

    #[test]
    fn arithmetic_overflow_is_closed() {
        let mut charge = ChargeAccumulator::empty();
        charge
            .add(usize::MAX)
            .expect("maximum itself is representable");
        assert_eq!(charge.add(1), Err(ServiceResponseChargeOverflow));
    }

    #[test]
    fn exact_end_does_not_reserve_an_absent_cursor() {
        let base = MESSAGE_RESERVE + PAGE_WRAPPER_RESERVE + FIELD_RESERVE;
        let item = ExactCharge(MAX_SERVICE_RESPONSE_BYTES - base - FIELD_RESERVE);

        let fit = fit_page_items(&[item], &ExactCharge(0), false).expect("exact end fits");
        assert_eq!(
            fit,
            PageFit {
                item_count: 1,
                has_more: false,
            }
        );
        assert!(matches!(
            fit_page_items(&[item], &ExactCharge(0), true),
            Err(ServiceFailure::ResponseTooLarge)
        ));
    }

    #[test]
    fn whole_item_fit_withholds_the_first_item_that_would_cross_the_ceiling() {
        let items = [ExactCharge(1_500_000); 3];
        let fit = fit_page_items(&items, &ExactCharge(0), false)
            .expect("two complete items fit with a continuation");
        assert_eq!(fit.item_count(), 2);
        assert!(fit.has_more());

        assert!(matches!(
            fit_page_items(
                &[ExactCharge(MAX_SERVICE_RESPONSE_BYTES)],
                &ExactCharge(0),
                false,
            ),
            Err(ServiceFailure::ResponseTooLarge)
        ));
    }

    #[test]
    fn lower_continuation_reserves_cursor_space_after_the_last_released_item() {
        let items = [ExactCharge(64), ExactCharge(64)];
        let fit = fit_page_items(&items, &ExactCharge(0), true)
            .expect("complete lower page fits with continuation");
        assert_eq!(fit.item_count(), items.len());
        assert!(fit.has_more());
    }

    #[test]
    fn empty_sparse_page_preserves_lower_progress() {
        assert_eq!(
            fit_sparse_page_items::<ExactCharge, _>(&[], &ExactCharge(0), true)
                .expect("an empty sparse page needs no item charge"),
            PageFit {
                item_count: 0,
                has_more: true,
            }
        );
        assert_eq!(
            fit_sparse_page_items::<ExactCharge, _>(&[], &ExactCharge(0), false)
                .expect("an empty final page needs no item charge"),
            PageFit {
                item_count: 0,
                has_more: false,
            }
        );
        assert_eq!(
            fit_page_items::<ExactCharge, _>(&[], &ExactCharge(0), true)
                .expect("ordinary pages retain their dense contract"),
            PageFit {
                item_count: 0,
                has_more: false,
            }
        );
    }

    #[test]
    fn indivisible_commit_and_stream_item_are_withheld_when_oversized() {
        let command_id = CommandId::first();
        let outcome_id = OutcomeId::first();
        let lineage = ContractLineage::new("response-budget-test").expect("bounded lineage");
        let version = ContractVersion::new(1).expect("nonzero contract version");
        let schema = SchemaIr::new(Vec::new(), Vec::new(), Vec::new(), Vec::new())
            .expect("empty schema is structurally valid");
        let payload = RecordSchema::new(
            RecordTypeRef::CommandOutcome {
                command_id,
                outcome_id,
            },
            Vec::new(),
        )
        .expect("empty outcome record schema");
        let outcome_schema = OutcomeSchema::new(command_id, outcome_id, "Completed", payload)
            .expect("bounded outcome schema");
        let outcome = DeclaredOutcomeView::from_checked_schema(
            OutcomePlanBinding::new(
                lineage.clone(),
                version,
                command_id,
                PlanHash::from_bytes([1; 32]),
                ExecutionClass::IdempotentMutation,
            ),
            &schema,
            &outcome_schema,
            CanonicalRecord::new(Vec::new()).expect("empty canonical outcome"),
        )
        .expect("outcome matches its checked schema");

        let affected_entities = (0_u32..1_024)
            .map(|ordinal| {
                let mut bytes = vec![0; 4_096];
                bytes[0] = 0x45;
                bytes[1] = 0x01;
                bytes[2..6].copy_from_slice(&1_u32.to_be_bytes());
                bytes[4_092..].copy_from_slice(&ordinal.to_be_bytes());
                AffectedEntityView::new(
                    EntityKey::from_bytes(bytes).expect("bounded entity-key envelope"),
                    EntityVersion::first(),
                )
            })
            .collect();
        let sequence = CommitSequence::first();
        let snapshot = AuthoritativeCommitSnapshot::new(
            sequence,
            RequestId::from_unix_milliseconds_and_random(1, [1; 10]).expect("request UUIDv7"),
            lineage,
            version,
            command_id,
            PlanHash::from_bytes([1; 32]),
            CanonicalInputHash::from_bytes([2; 32]),
            AdmittedActorContext::new(
                ActorId::new("response-budget-test").expect("bounded actor"),
                ActorKind::Human,
                TenantScope::Global,
                None,
            ),
            LogicalTime::new(Timestamp::new(1, 0).expect("canonical timestamp")),
            PartitionKeyHash::from_bytes([3; 32]),
            Vec::new(),
            affected_entities,
            Vec::new(),
            outcome,
            ProvenanceId::from_unix_milliseconds_and_random(1, [2; 10]).expect("provenance UUIDv7"),
            CommandDurability::Synchronous,
        )
        .expect("bounded authoritative commit");
        let commit = CommitView::new(snapshot);

        assert!(matches!(
            ensure_response_budget(&GetCommitResult::Found(Box::new(commit.clone()))),
            Err(ServiceFailure::ResponseTooLarge)
        ));
        assert!(matches!(
            ensure_response_budget(&CommitSubscriptionEvent::Commit(Box::new(commit))),
            Err(ServiceFailure::ResponseTooLarge)
        ));
    }

    #[test]
    fn every_unary_result_has_compile_time_charge_coverage() {
        fn assert_charge<T: ServiceResponseCharge>() {}

        assert_charge::<ContractValidationResult>();
        assert_charge::<ExplainCommandResult>();
        assert_charge::<DeployContractResult>();
        assert_charge::<GetActiveContractResult>();
        assert_charge::<GetContractVersionResult>();
        assert_charge::<ExecuteCommandResult>();
        assert_charge::<ResolveCommandOutcomeResult>();
        assert_charge::<GetEntityResult>();
        assert_charge::<ScanIndexResult>();
        assert_charge::<QueryProjectionResult>();
        assert_charge::<GetProjectionStatusResult>();
        assert_charge::<GetCommitResult>();
        assert_charge::<ScanCommitsResult>();
        assert_charge::<SubscribeToCommitsResult>();
        assert_charge::<TraceProvenanceResult>();
        assert_charge::<HealthResult>();
        assert_charge::<StatisticsResult>();
        assert_charge::<CreateCapabilityResult>();
        assert_charge::<RevokeCapabilityResult>();
        assert_charge::<ListPendingOutboxDeliveriesResult>();
        assert_charge::<DiscoverCommandToolsResult>();
        assert_charge::<DiscoverResourcesResult>();
    }
}
