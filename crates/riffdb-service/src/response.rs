//! Versioned, transport-independent service response accounting.

use std::error::Error;
use std::fmt;

use riffdb_contract_ir::{CommandExplain, GeneratedSchemaArtifact};
use riffdb_types::{
    AdmittedActorContext, CanonicalRecord, CanonicalValue, ContractLineage, ProjectionIdentity,
    ServiceAuditTargetV1, TenantScope,
};

use crate::{
    BuildInfo, CheckSymbolicQueryResult, CheckedSymbolicQuery, CommandToolDescriptor,
    CommandToolDiscoveryItem, CommitScanFence, CommitSubscriptionEvent, CommitView,
    CompactCommandToolDescriptor, CompactCommandToolDiscoveryItem, CompactNamedQueryToolDescriptor,
    CompactResourceDescriptor, CompactResourceDescriptorRef, ContractDescriptor,
    ContractValidationResult, CreateCapabilityResult, CursorToken, DeclaredOutcomeView,
    DeployContractResult, DeployQueryModuleResult, DescribeSymbolicContractResult,
    DiscoverCommandToolsResult, DiscoverCommandToolsResultRef, DiscoverResourcesResult,
    DiscoverResourcesResultRef, DiscoveryCatalogFence, DiscoveryCatalogStateRef, DurableEventView,
    EntityView, ExecuteCommandResult, ExecuteSymbolicQueryResult, ExplainCommandResult,
    ExplainSymbolicQueryResult, GeneratedSchemaIdentity, GetActiveContractResult, GetCommitResult,
    GetContractVersionResult, GetEntityResult, GetOfflineMaintenanceOperationResult,
    GetProjectionStatusResult, HealthReport, HealthResult, IndexRowView, IndexScanFence,
    JournaledCommandResult, ListPendingOutboxDeliveriesResult, NamedQueryToolDescriptor,
    NamedQueryToolSchemaArtifact, NormalCreateCapabilityResult,
    OfflineMaintenanceOperationObservation, OfflineMaintenanceStartResult, OperationSchemaArtifact,
    OperationSchemaCatalog, OperationSchemaCatalogIdentity, OperationSchemaIdentity,
    OutboxDeliverySummary, Page, ProjectionPageFence, ProjectionRow, ProjectionStatusSnapshot,
    ProvenanceClaimsView, ProvenanceView, QueryModuleInspection, QueryProjectionResult,
    ReadOnlyCommandResult, ResolveCommandOutcomeResult, ResourceDescriptor, ResourceDescriptorRef,
    RevokeCapabilityResult, ScanCommitsResult, ScanIndexResult, SchemaBoundOutcomeRecord,
    SchemaBoundOutcomeValue, ServiceFailure, StatisticsResult, SubscribeToCommitsResult,
    SymbolicDiagnostic, SymbolicQueryIdentity, SymbolicQuerySchema, SymbolicResultField,
    SymbolicResultRecord, TraceProvenanceResult,
};

/// Exact POC ceiling for one API-neutral unary result or visible stream item.
pub const MAX_SERVICE_RESPONSE_BYTES: usize = 4_194_304;
/// Exact stricter ceiling for one full discovery result.
pub const MAX_FULL_DISCOVERY_RESPONSE_BYTES: usize = 2_621_440;
/// Exact conservative ceiling for one compact discovery item.
pub const MAX_COMPACT_DISCOVERY_ITEM_BYTES: usize = 4_096;

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

    const fn fits_limit(self, limit: usize) -> bool {
        self.0 <= limit
    }
}

/// A complete result has no releasable response charge under accounting v1.
///
/// This covers checked-arithmetic overflow and a type-local invariant ceiling,
/// such as the stricter full-discovery or compact-item bound. Both conditions
/// fail closed through the same existing service disposition.
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
    /// Computes the deterministic conservative v1 charge when the complete
    /// result also satisfies every type-local response ceiling.
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

fn charge_symbolic_identity(
    charge: &mut ChargeAccumulator,
    identity: &SymbolicQueryIdentity,
) -> Result<(), ServiceResponseChargeOverflow> {
    charge_lineage(charge, identity.lineage())?;
    charge.fields(1)?;
    charge.bytes(identity.bundle_hash().as_bytes().len())?;
    if let Some(name) = identity.name() {
        charge.bytes(name.len())?;
    }
    charge.bytes(identity.plan_hash().as_bytes().len())
}

fn charge_symbolic_schema(
    charge: &mut ChargeAccumulator,
    schema: &SymbolicQuerySchema,
) -> Result<(), ServiceResponseChargeOverflow> {
    for value in schema
        .parameters()
        .iter()
        .chain(schema.outcomes())
        .chain(schema.result_fields())
    {
        charge.bytes(value.len())?;
    }
    Ok(())
}

fn charge_checked_symbolic_query(
    charge: &mut ChargeAccumulator,
    query: &CheckedSymbolicQuery,
) -> Result<(), ServiceResponseChargeOverflow> {
    charge_symbolic_identity(charge, query.identity())?;
    charge_symbolic_schema(charge, query.schema())
}

fn charge_symbolic_diagnostic(
    charge: &mut ChargeAccumulator,
    diagnostic: &SymbolicDiagnostic,
) -> Result<(), ServiceResponseChargeOverflow> {
    charge.bytes(diagnostic.code().len())?;
    charge.bytes(diagnostic.summary().len())?;
    charge.fields(2)?;
    for symbol in diagnostic.symbols() {
        charge.bytes(symbol.len())?;
    }
    if let Some(suggestion) = diagnostic.suggestion() {
        charge.bytes(suggestion.len())?;
    }
    Ok(())
}

impl sealed::Sealed for DescribeSymbolicContractResult {}

impl ServiceResponseCharge for DescribeSymbolicContractResult {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge_lineage(&mut charge, self.lineage())?;
        charge.fields(1)?;
        charge.bytes(self.bundle_hash().as_bytes().len())?;
        charge.bytes(self.catalog().len())?;
        Ok(charge.finish())
    }
}

impl sealed::Sealed for CheckSymbolicQueryResult {}

impl ServiceResponseCharge for CheckSymbolicQueryResult {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        match self {
            Self::Valid(query) => charge_checked_symbolic_query(&mut charge, query)?,
            Self::Invalid(diagnostics) => {
                for diagnostic in diagnostics {
                    charge_symbolic_diagnostic(&mut charge, diagnostic)?;
                }
            }
        }
        Ok(charge.finish())
    }
}

impl sealed::Sealed for ExplainSymbolicQueryResult {}

impl ServiceResponseCharge for ExplainSymbolicQueryResult {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        match self {
            Self::Valid { query, lines } => {
                charge_checked_symbolic_query(&mut charge, query)?;
                for line in lines {
                    charge.bytes(line.len())?;
                }
            }
            Self::Invalid(diagnostics) => {
                for diagnostic in diagnostics {
                    charge_symbolic_diagnostic(&mut charge, diagnostic)?;
                }
            }
        }
        Ok(charge.finish())
    }
}

fn charge_symbolic_record(
    charge: &mut ChargeAccumulator,
    record: &SymbolicResultRecord,
) -> Result<(), ServiceResponseChargeOverflow> {
    charge.bytes(record.entity().len())?;
    for (name, value) in record.fields() {
        charge.bytes(name.len())?;
        charge.nested(value)?;
    }
    Ok(())
}

fn charge_symbolic_field(
    charge: &mut ChargeAccumulator,
    field: &SymbolicResultField,
) -> Result<(), ServiceResponseChargeOverflow> {
    match field {
        SymbolicResultField::One(record) => charge_symbolic_record(charge, record),
        SymbolicResultField::Maybe(record) => {
            charge.fields(1)?;
            if let Some(record) = record {
                charge_symbolic_record(charge, record)?;
            }
            Ok(())
        }
        SymbolicResultField::Many(records) => {
            for record in records {
                charge_symbolic_record(charge, record)?;
            }
            Ok(())
        }
    }
}

impl sealed::Sealed for ExecuteSymbolicQueryResult {}

impl ServiceResponseCharge for ExecuteSymbolicQueryResult {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge_symbolic_identity(&mut charge, self.identity())?;
        charge.bytes(self.outcome().len())?;
        charge.fields(1)?;
        for (name, field) in self.fields() {
            charge.bytes(name.len())?;
            charge_symbolic_field(&mut charge, field)?;
        }
        if self.next_cursor().is_some() {
            charge.bytes(crate::CURSOR_TOKEN_BYTES)?;
        }
        Ok(charge.finish())
    }
}

impl sealed::Sealed for DeployQueryModuleResult {}

impl ServiceResponseCharge for DeployQueryModuleResult {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        let module = self.module();
        charge.bytes(module.name().as_str().len())?;
        charge.fields(3)?;
        charge.bytes(module.hash().as_bytes().len())?;
        charge_lineage(&mut charge, module.contract_lineage())?;
        charge.fields(1)?;
        charge.bytes(module.contract_hash().as_bytes().len())?;
        for query in module.query_names() {
            charge.bytes(query.len())?;
        }
        Ok(charge.finish())
    }
}

impl sealed::Sealed for Option<QueryModuleInspection> {}

impl ServiceResponseCharge for Option<QueryModuleInspection> {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        if let Some(inspection) = self {
            let module = inspection.descriptor();
            charge.bytes(module.name().as_str().len())?;
            charge.bytes(module.hash().as_bytes().len())?;
            charge_lineage(&mut charge, module.contract_lineage())?;
            charge.bytes(module.contract_hash().as_bytes().len())?;
            charge.fields(4)?;
            for query in inspection.queries() {
                charge.bytes(query.name().len())?;
                charge.bytes(query.source().len())?;
            }
        }
        Ok(charge.finish())
    }
}

fn fixed_charge(fields: usize) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
    let mut charge = ChargeAccumulator::message();
    charge.fields(fields)?;
    Ok(charge.finish())
}

fn apply_type_local_charge_limit(
    charge: ServiceResponseChargeV1,
    limit: usize,
) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
    if charge.fits_limit(limit) {
        Ok(charge)
    } else {
        Err(ServiceResponseChargeOverflow)
    }
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
    charge.bytes(32)?;

    let compatibility = descriptor.compatibility();
    charge.fields(1)?;
    charge.add(MESSAGE_RESERVE)?;
    charge.fields(1)?;
    if let Some((_, parent_hash)) = compatibility.parent() {
        charge.fields(1)?;
        charge.bytes(parent_hash.as_bytes().len())?;
    }
    for entry in compatibility.code_counts() {
        charge.fields(1)?;
        charge.add(MESSAGE_RESERVE)?;
        charge.bytes(entry.code().len())?;
        charge.fields(1)?;
    }
    Ok(())
}

fn charge_schema(
    charge: &mut ChargeAccumulator,
    schema: &GeneratedSchemaArtifact,
) -> Result<(), ServiceResponseChargeOverflow> {
    charge_schema_shape(charge, schema.canonical_json().len())
}

fn charge_schema_shape(
    charge: &mut ChargeAccumulator,
    canonical_json_bytes: usize,
) -> Result<(), ServiceResponseChargeOverflow> {
    // Key, fixed dialect identifier, schema hash, and exact canonical JSON.
    charge.fields(2)?;
    charge.bytes("https://json-schema.org/draft/2020-12/schema".len())?;
    charge.bytes(32)?;
    charge.bytes(canonical_json_bytes)
}

fn charge_command_tool_identity_shape(
    charge: &mut ChargeAccumulator,
    tool_name_bytes: usize,
    source_command_bytes: usize,
    lineage_bytes: usize,
) -> Result<(), ServiceResponseChargeOverflow> {
    charge.bytes(tool_name_bytes)?;
    charge.bytes(source_command_bytes)?;
    charge.bytes(lineage_bytes)?;
    charge.fields(2)
}

fn raw_command_tool_descriptor_charge(
    tool_name_bytes: usize,
    source_command_bytes: usize,
    lineage_bytes: usize,
    input_schema_json_bytes: usize,
    outcome_schema_json_bytes: usize,
) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
    let mut charge = ChargeAccumulator::message();
    charge_command_tool_identity_shape(
        &mut charge,
        tool_name_bytes,
        source_command_bytes,
        lineage_bytes,
    )?;
    charge_schema_shape(&mut charge, input_schema_json_bytes)?;
    charge_schema_shape(&mut charge, outcome_schema_json_bytes)?;
    Ok(charge.finish())
}

fn raw_command_tool_discovery_item_charge(
    descriptor: Option<ServiceResponseChargeV1>,
) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
    let mut charge = ChargeAccumulator::message();
    charge.fields(1)?;
    if let Some(descriptor) = descriptor {
        charge.fields(1)?;
        charge.add(descriptor.bytes())?;
    }
    Ok(charge.finish())
}

fn raw_generated_schema_identity_charge(
    key_bytes: usize,
    hash_bytes: usize,
) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
    let mut charge = ChargeAccumulator::message();
    charge.bytes(key_bytes)?;
    charge.bytes(hash_bytes)?;
    Ok(charge.finish())
}

fn raw_compact_command_tool_descriptor_charge(
    tool_name_bytes: usize,
    source_command_bytes: usize,
    lineage_bytes: usize,
    input_schema: ServiceResponseChargeV1,
    outcome_schema: ServiceResponseChargeV1,
) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
    let mut charge = ChargeAccumulator::message();
    charge_command_tool_identity_shape(
        &mut charge,
        tool_name_bytes,
        source_command_bytes,
        lineage_bytes,
    )?;
    for schema in [input_schema, outcome_schema] {
        charge.fields(1)?;
        charge.add(schema.bytes())?;
    }
    Ok(charge.finish())
}

fn charge_entity_schema_resource_shape(
    charge: &mut ChargeAccumulator,
    lineage_bytes: usize,
    schema_json_bytes: usize,
) -> Result<(), ServiceResponseChargeOverflow> {
    charge.bytes(lineage_bytes)?;
    charge.fields(1)?;
    charge_schema_shape(charge, schema_json_bytes)
}

fn charge_command_resource_shape(
    charge: &mut ChargeAccumulator,
    lineage_bytes: usize,
    source_command_bytes: usize,
) -> Result<(), ServiceResponseChargeOverflow> {
    charge.bytes(lineage_bytes)?;
    charge.fields(2)?;
    charge.bytes(source_command_bytes)
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
    charge.nested(outcome.value())?;
    charge_schema_bound_names(charge, outcome.schema_bound_value())
}

fn charge_schema_bound_names(
    charge: &mut ChargeAccumulator,
    record: SchemaBoundOutcomeRecord<'_>,
) -> Result<(), ServiceResponseChargeOverflow> {
    for index in 0..record.len() {
        let field = record.field(index).ok_or(ServiceResponseChargeOverflow)?;
        charge.bytes(field.field_name().as_str().len())?;
        charge_schema_bound_value_names(
            charge,
            field.value().ok_or(ServiceResponseChargeOverflow)?,
        )?;
    }
    Ok(())
}

fn charge_schema_bound_value_names(
    charge: &mut ChargeAccumulator,
    value: SchemaBoundOutcomeValue<'_>,
) -> Result<(), ServiceResponseChargeOverflow> {
    match value {
        SchemaBoundOutcomeValue::Null | SchemaBoundOutcomeValue::Scalar(_) => Ok(()),
        SchemaBoundOutcomeValue::Enum { variant_name, .. } => {
            charge.bytes(variant_name.as_str().len())
        }
        SchemaBoundOutcomeValue::List(values) => {
            for index in 0..values.len() {
                charge_schema_bound_value_names(
                    charge,
                    values.value(index).ok_or(ServiceResponseChargeOverflow)?,
                )?;
            }
            Ok(())
        }
        SchemaBoundOutcomeValue::Record(record) => charge_schema_bound_names(charge, record),
    }
}

fn charge_journaled(
    charge: &mut ChargeAccumulator,
    result: &JournaledCommandResult,
) -> Result<(), ServiceResponseChargeOverflow> {
    charge.fields(7)?;
    charge_lineage(charge, result.lineage())?;
    charge.bytes(32)?;
    charge_declared_outcome(charge, result.outcome())?;
    // Reserve the fixed canonical provenance locator produced by WP-130.
    charge.bytes(56)?;
    charge.bytes(result.outcome_locator().canonical_uri().len())
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
        match self {
            Self::Valid => {}
            Self::Candidate(candidate) => {
                if candidate.parent_version().is_some() {
                    charge.fields(1)?;
                    charge.bytes(32)?;
                }
                charge_contract_descriptor(&mut charge, candidate.candidate())?;
                charge.bytes(candidate.canonical_bundle().len())?;
            }
            Self::Invalid(error) => {
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
            Self::InvalidSource(error) => {
                let validation = ContractValidationResult::Invalid(error.clone());
                charge.nested(&validation)?;
            }
            Self::IncompatibleCandidate(descriptor)
            | Self::Activated(descriptor)
            | Self::AlreadyActive(descriptor) => {
                charge_contract_descriptor(&mut charge, descriptor)?;
            }
            Self::ExpectedActiveVersionMismatch { actual } => {
                if actual.is_some() {
                    charge.fields(1)?;
                }
            }
            Self::ExpectedApplicationIdentityMismatch {
                actual_active,
                compiled_candidate,
            } => {
                if let Some(actual) = actual_active {
                    charge_contract_descriptor(&mut charge, actual)?;
                }
                charge_contract_descriptor(&mut charge, compiled_candidate)?;
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

impl ServiceResponseCharge for OfflineMaintenanceOperationObservation {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.bytes(self.operation_id().as_bytes().len())?;
        charge.fields(1)?;
        charge.bytes(self.backup_name().as_bytes().len())?;
        charge.bytes(self.input_hash().as_bytes().len())?;
        charge.fields(1)?;
        if self.failure().is_some() {
            charge.fields(1)?;
        }
        Ok(charge.finish())
    }
}

impl ServiceResponseCharge for OfflineMaintenanceStartResult {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.fields(1)?;
        charge.nested(self.operation())?;
        Ok(charge.finish())
    }
}

impl ServiceResponseCharge for GetOfflineMaintenanceOperationResult {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.fields(1)?;
        if let Self::Found(operation) = self {
            charge.nested(operation)?;
        }
        Ok(charge.finish())
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
        raw_command_tool_descriptor_charge(
            self.name().as_str().len(),
            self.source_command().as_str().len(),
            self.lineage().as_bytes().len(),
            self.input_schema().canonical_json().len(),
            self.outcome_schema().canonical_json().len(),
        )
    }
}

impl ServiceResponseCharge for GeneratedSchemaIdentity {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        raw_generated_schema_identity_charge(
            self.key().to_bytes().len(),
            self.schema_hash().as_bytes().len(),
        )
    }
}

impl ServiceResponseCharge for CompactCommandToolDescriptor {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        raw_compact_command_tool_descriptor_charge(
            self.name().as_str().len(),
            self.source_command().as_str().len(),
            self.lineage().as_bytes().len(),
            self.input_schema().service_response_charge_v1()?,
            self.outcome_schema().service_response_charge_v1()?,
        )
    }
}

impl ServiceResponseCharge for NamedQueryToolSchemaArtifact {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.bytes(self.schema_hash().as_bytes().len())?;
        charge.bytes(self.canonical_json().len())?;
        Ok(charge.finish())
    }
}

impl ServiceResponseCharge for NamedQueryToolDescriptor {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.bytes(self.name().len())?;
        charge.bytes(self.source_query().as_str().len())?;
        charge_lineage(&mut charge, self.lineage())?;
        charge.fields(2)?;
        charge.bytes(self.module_name().as_str().len())?;
        charge.fields(1)?;
        charge.bytes(self.module_hash().as_bytes().len())?;
        charge.nested(self.input_schema())?;
        charge.nested(self.result_schema())?;
        Ok(charge.finish())
    }
}

impl ServiceResponseCharge for CompactNamedQueryToolDescriptor {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.bytes(self.name().len())?;
        charge.bytes(self.source_query().as_str().len())?;
        charge_lineage(&mut charge, self.lineage())?;
        charge.fields(2)?;
        charge.bytes(self.module_name().as_str().len())?;
        charge.fields(1)?;
        charge.bytes(self.module_hash().as_bytes().len())?;
        charge.bytes(self.input_schema_hash().as_bytes().len())?;
        charge.bytes(self.result_schema_hash().as_bytes().len())?;
        Ok(charge.finish())
    }
}

impl ServiceResponseCharge for OperationSchemaIdentity {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.bytes(self.schema_id().len())?;
        charge.bytes(self.schema_hash().as_bytes().len())?;
        Ok(charge.finish())
    }
}

impl ServiceResponseCharge for OperationSchemaArtifact {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.nested(self.identity())?;
        charge.bytes(self.dialect().len())?;
        charge.bytes(self.canonical_json().len())?;
        Ok(charge.finish())
    }
}

impl ServiceResponseCharge for OperationSchemaCatalogIdentity {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.nested(self.command_operation_envelope())?;
        charge.nested(self.command_get_outcome_result())?;
        Ok(charge.finish())
    }
}

impl ServiceResponseCharge for OperationSchemaCatalog {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.nested(self.command_operation_envelope())?;
        charge.nested(self.command_get_outcome_result())?;
        Ok(charge.finish())
    }
}

impl ServiceResponseCharge for DiscoveryCatalogFence {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.fields(1)?;
        if let DiscoveryCatalogStateRef::ActiveContract {
            lineage,
            bundle_hash: _,
            version: _,
        } = self.state()
        {
            charge.fields(2)?;
            charge_lineage(&mut charge, lineage)?;
            charge.bytes(32)?;
            if self.active_query_module_hash().is_some() {
                charge.bytes(32)?;
            }
        }
        charge.nested(self.operation_schemas())?;
        Ok(charge.finish())
    }
}

impl ServiceResponseCharge for CommandToolDiscoveryItem {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let descriptor = match self {
            Self::Fixed(_) => None,
            Self::Command(descriptor) => Some(descriptor.service_response_charge_v1()?),
            Self::NamedQuery(descriptor) => Some(descriptor.service_response_charge_v1()?),
        };
        raw_command_tool_discovery_item_charge(descriptor)
    }
}

impl ServiceResponseCharge for CompactCommandToolDiscoveryItem {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let descriptor = match self {
            Self::Fixed(_) => None,
            Self::Command(descriptor) => Some(descriptor.service_response_charge_v1()?),
            Self::NamedQuery(descriptor) => Some(descriptor.service_response_charge_v1()?),
        };
        let result = raw_command_tool_discovery_item_charge(descriptor)?;
        if result.bytes() > MAX_COMPACT_DISCOVERY_ITEM_BYTES {
            return Err(ServiceResponseChargeOverflow);
        }
        Ok(result)
    }
}

fn raw_discovery_page_response_charge<T, F>(
    page: &Page<T, F>,
    additional_nested: Option<&dyn ServiceResponseCharge>,
) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow>
where
    T: ServiceResponseCharge,
    F: ServiceResponseCharge,
{
    let mut charge = ChargeAccumulator::message();
    charge.nested(page)?;
    if let Some(additional_nested) = additional_nested {
        charge.nested(additional_nested)?;
    }
    Ok(charge.finish())
}

impl ServiceResponseCharge for DiscoverCommandToolsResult {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let result = match self.result() {
            DiscoverCommandToolsResultRef::CatalogUnchanged(fence) => {
                let mut charge = ChargeAccumulator::message();
                charge.nested(fence)?;
                charge.finish()
            }
            DiscoverCommandToolsResultRef::Page {
                page,
                operation_schemas,
            } => apply_type_local_charge_limit(
                raw_discovery_page_response_charge(page, Some(operation_schemas))?,
                MAX_FULL_DISCOVERY_RESPONSE_BYTES,
            )?,
            DiscoverCommandToolsResultRef::CompactPage(page) => {
                raw_discovery_page_response_charge(page, None)?
            }
        };
        Ok(result)
    }
}

impl ServiceResponseCharge for ResourceDescriptor {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.fields(1)?;
        match self.resource() {
            ResourceDescriptorRef::ActiveContract | ResourceDescriptorRef::ServerHealth => {}
            ResourceDescriptorRef::ContractVersion { lineage, .. } => {
                charge_lineage(&mut charge, lineage)?;
                charge.fields(1)?;
            }
            ResourceDescriptorRef::EntitySchema {
                lineage, schema, ..
            } => {
                charge_entity_schema_resource_shape(
                    &mut charge,
                    lineage.as_bytes().len(),
                    schema.canonical_json().len(),
                )?;
            }
            ResourceDescriptorRef::CommandPlan {
                lineage,
                source_command,
                ..
            }
            | ResourceDescriptorRef::CommandDocumentation {
                lineage,
                source_command,
                ..
            } => {
                charge_command_resource_shape(
                    &mut charge,
                    lineage.as_bytes().len(),
                    source_command.as_str().len(),
                )?;
            }
            ResourceDescriptorRef::CommandOutcome {
                lineage, tool_name, ..
            } => {
                charge_lineage(&mut charge, lineage)?;
                charge.fields(1)?;
                charge.bytes(tool_name.as_str().len())?;
            }
            ResourceDescriptorRef::Commit { .. } | ResourceDescriptorRef::Provenance { .. } => {
                charge.fields(1)?
            }
            ResourceDescriptorRef::ProjectionStatus { lineage, .. } => {
                charge_lineage(&mut charge, lineage)?;
                charge.fields(1)?;
            }
        }
        Ok(charge.finish())
    }
}

impl ServiceResponseCharge for CompactResourceDescriptor {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let mut charge = ChargeAccumulator::message();
        charge.fields(1)?;
        match self.resource() {
            CompactResourceDescriptorRef::ActiveContract
            | CompactResourceDescriptorRef::ServerHealth => {}
            CompactResourceDescriptorRef::ContractVersion { lineage, .. } => {
                charge_lineage(&mut charge, lineage)?;
                charge.fields(1)?;
            }
            CompactResourceDescriptorRef::EntitySchema {
                lineage, schema, ..
            } => {
                charge.bytes(lineage.as_bytes().len())?;
                charge.fields(1)?;
                charge.nested(schema)?;
            }
            CompactResourceDescriptorRef::CommandPlan {
                lineage,
                source_command,
                ..
            }
            | CompactResourceDescriptorRef::CommandDocumentation {
                lineage,
                source_command,
                ..
            } => {
                charge_command_resource_shape(
                    &mut charge,
                    lineage.as_bytes().len(),
                    source_command.as_str().len(),
                )?;
            }
            CompactResourceDescriptorRef::CommandOutcome {
                lineage, tool_name, ..
            } => {
                charge_lineage(&mut charge, lineage)?;
                charge.fields(1)?;
                charge.bytes(tool_name.as_str().len())?;
            }
            CompactResourceDescriptorRef::Commit { .. }
            | CompactResourceDescriptorRef::Provenance { .. } => charge.fields(1)?,
            CompactResourceDescriptorRef::ProjectionStatus { lineage, .. } => {
                charge_lineage(&mut charge, lineage)?;
                charge.fields(1)?;
            }
        }
        let result = charge.finish();
        if result.bytes() > MAX_COMPACT_DISCOVERY_ITEM_BYTES {
            return Err(ServiceResponseChargeOverflow);
        }
        Ok(result)
    }
}

impl ServiceResponseCharge for DiscoverResourcesResult {
    fn service_response_charge_v1(
        &self,
    ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
        let result = match self.result() {
            DiscoverResourcesResultRef::CatalogUnchanged(fence) => {
                let mut charge = ChargeAccumulator::message();
                charge.nested(fence)?;
                charge.finish()
            }
            DiscoverResourcesResultRef::Page(page) => apply_type_local_charge_limit(
                raw_discovery_page_response_charge(page, None)?,
                MAX_FULL_DISCOVERY_RESPONSE_BYTES,
            )?,
            DiscoverResourcesResultRef::CompactPage(page) => {
                raw_discovery_page_response_charge(page, None)?
            }
        };
        Ok(result)
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
    OfflineMaintenanceOperationObservation,
    OfflineMaintenanceStartResult,
    GetOfflineMaintenanceOperationResult,
    OutboxDeliverySummary,
    ListPendingOutboxDeliveriesResult,
    CommandToolDescriptor,
    CompactCommandToolDescriptor,
    NamedQueryToolSchemaArtifact,
    NamedQueryToolDescriptor,
    CompactNamedQueryToolDescriptor,
    CommandToolDiscoveryItem,
    CompactCommandToolDiscoveryItem,
    GeneratedSchemaIdentity,
    OperationSchemaIdentity,
    OperationSchemaArtifact,
    OperationSchemaCatalogIdentity,
    OperationSchemaCatalog,
    DiscoveryCatalogFence,
    DiscoverCommandToolsResult,
    ResourceDescriptor,
    CompactResourceDescriptor,
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
    fit_page_items_with_budget(
        items,
        fence,
        lower_has_more,
        false,
        0,
        MAX_SERVICE_RESPONSE_BYTES,
    )
}

/// Fits a full command-discovery page including its mandatory operation catalog.
pub(crate) fn fit_full_command_discovery_page_items<T, F>(
    items: &[T],
    fence: &F,
    operation_schemas: &OperationSchemaCatalog,
    lower_has_more: bool,
) -> Result<PageFit, ServiceFailure>
where
    T: ServiceResponseCharge,
    F: ServiceResponseCharge,
{
    let catalog_charge = operation_schemas
        .service_response_charge_v1()
        .map_err(|_| ServiceFailure::ResponseTooLarge)?
        .bytes()
        .checked_add(FIELD_RESERVE)
        .ok_or(ServiceFailure::ResponseTooLarge)?;
    fit_page_items_with_budget(
        items,
        fence,
        lower_has_more,
        false,
        catalog_charge,
        MAX_FULL_DISCOVERY_RESPONSE_BYTES,
    )
}

/// Fits a full resource-discovery page under the stricter discovery ceiling.
pub(crate) fn fit_full_resource_discovery_page_items<T, F>(
    items: &[T],
    fence: &F,
    lower_has_more: bool,
) -> Result<PageFit, ServiceFailure>
where
    T: ServiceResponseCharge,
    F: ServiceResponseCharge,
{
    fit_page_items_with_budget(
        items,
        fence,
        lower_has_more,
        false,
        0,
        MAX_FULL_DISCOVERY_RESPONSE_BYTES,
    )
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
    fit_page_items_with_budget(
        items,
        fence,
        lower_has_more,
        true,
        0,
        MAX_SERVICE_RESPONSE_BYTES,
    )
}

fn fit_page_items_with_budget<T, F>(
    items: &[T],
    fence: &F,
    lower_has_more: bool,
    permit_empty_progress: bool,
    extra_charge: usize,
    ceiling: usize,
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
        .add(extra_charge)
        .map_err(|_| ServiceFailure::ResponseTooLarge)?;
    base_without_cursor
        .nested(fence)
        .map_err(|_| ServiceFailure::ResponseTooLarge)?;

    if !lower_has_more {
        let mut exact_end = base_without_cursor;
        let all_fit = items
            .iter()
            .all(|item| exact_end.nested(item).is_ok() && exact_end.finish().fits_limit(ceiling));
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
        if candidate.nested(item).is_err() || !candidate.finish().fits_limit(ceiling) {
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
        ExecutionClass, MAX_JSON_SCHEMA_ARTIFACT_BYTES, MAX_MCP_COMMAND_TOOL_NAME_BYTES,
        OutcomeSchema, RecordSchema, RecordTypeRef, SchemaArtifactKey, SchemaIr,
    };
    use riffdb_types::{
        ActorId, ActorKind, AdministrationSequence, ApprovalId, BackupNameV1, CanonicalInputHash,
        CanonicalString, CapabilityId, CommandId, CommitSequence, ContractBundleHash,
        ContractPlanRootHash, ContractVersion, DIGEST_SCHEME_V1, DigestKeyId, EntityKey,
        EntityVersion, EventId, FieldId, FrontierPosition, IndexEntryKey, IndexEpochPosition,
        LogicalTime, MAX_APPROVAL_ID_BYTES, MAX_CONTRACT_LINEAGE_BYTES,
        MAX_PROVENANCE_REASON_BYTES, MAX_SOURCE_COMMIT_BYTES, MAX_SOURCE_REPOSITORY_BYTES,
        OfflineMaintenanceOperationId, OfflineMaintenanceOperationKind,
        OfflineMaintenanceReplacementConfirmation, OutcomeId, PartitionKeyHash, PlanHash,
        ProjectionGeneration, ProjectionId, ProjectionPlanHash, ProvenanceId, ProvenanceReason,
        RequestId, SourceCommit, SourceHash, SourceRepository, Timestamp,
        offline_maintenance_input_hash,
    };

    use super::*;
    use crate::{
        AffectedEntityView, AuthoritativeCommitSnapshot, AuthoritativeProvenanceSnapshot,
        CapabilityIdentityView, CapabilityTransitionView, CommandDurability, CommitScanFence,
        CommitSubscriptionEndReason, CommitSubscriptionTerminal, ComponentHealth,
        DiscoverCommandToolsRequest, DiscoverResourcesRequest, ExplainedCommand,
        HealthComponentKind, HealthComponentStatus, IndexScanFence, JournaledCommandResult,
        JournaledCompletion, OperationalHealthSnapshot, OperationalStatisticsSnapshot,
        OutboxDeliveryState, OutcomePlanBinding, PageLimit, PageRequest, PreBootstrapHealthReport,
        PreBootstrapLifecycle, ProjectionFailure, ProjectionFailureCode,
        ProjectionGenerationFrontier, ProjectionLifecycle, PublishedApplyMode,
        QueryProjectionReady, ReadOnlyCommandResult, RecoveredJournaledCommandResult, SourceName,
    };

    const RESPONSE_CHARGE_FIXTURE: &str = include_str!("../fixtures/response-charge-v1.tsv");
    const RESPONSE_CHARGE_FIXTURE_VERSION: u16 = 1;

    // This registry freezes every public gRPC response/item family with a
    // variable-size or nested public encoding covered by the v1 charge ledger.
    const CHECKED_RESPONSE_FAMILIES: [&str; 25] = [
        "commit_notification",
        "contract_validation",
        "create_capability",
        "deploy_contract",
        "discover_command_tools",
        "discover_resources",
        "execute_command",
        "explain_command",
        "get_active_contract",
        "get_commit",
        "get_contract_version",
        "get_entity",
        "get_outcome",
        "health",
        "list_pending_outbox_deliveries",
        "offline_maintenance_get",
        "offline_maintenance_start",
        "operation_schema_catalog",
        "projection_status",
        "query_projection",
        "revoke_capability",
        "scan_commits",
        "scan_index",
        "stats",
        "trace_provenance",
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

    #[derive(Clone, Copy)]
    struct CommandDiscoveryItemCharge {
        tool_name_bytes: usize,
        source_command_bytes: usize,
        lineage_bytes: usize,
        input_schema_json_bytes: usize,
        outcome_schema_json_bytes: usize,
    }

    impl sealed::Sealed for CommandDiscoveryItemCharge {}

    impl ServiceResponseCharge for CommandDiscoveryItemCharge {
        fn service_response_charge_v1(
            &self,
        ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
            let descriptor = raw_command_tool_descriptor_charge(
                self.tool_name_bytes,
                self.source_command_bytes,
                self.lineage_bytes,
                self.input_schema_json_bytes,
                self.outcome_schema_json_bytes,
            )?;
            raw_command_tool_discovery_item_charge(Some(descriptor))
        }
    }

    const MAXIMUM_COMMAND_DISCOVERY_ITEM_CHARGE: CommandDiscoveryItemCharge =
        CommandDiscoveryItemCharge {
            tool_name_bytes: MAX_MCP_COMMAND_TOOL_NAME_BYTES,
            source_command_bytes: 1,
            lineage_bytes: 115,
            input_schema_json_bytes: MAX_JSON_SCHEMA_ARTIFACT_BYTES,
            outcome_schema_json_bytes: MAX_JSON_SCHEMA_ARTIFACT_BYTES,
        };

    #[derive(Clone, Copy)]
    struct ResourceDiscoveryItemCharge {
        lineage_bytes: usize,
        schema_json_bytes: usize,
    }

    impl sealed::Sealed for ResourceDiscoveryItemCharge {}

    impl ServiceResponseCharge for ResourceDiscoveryItemCharge {
        fn service_response_charge_v1(
            &self,
        ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
            let mut descriptor = ChargeAccumulator::message();
            descriptor.fields(1)?;
            charge_entity_schema_resource_shape(
                &mut descriptor,
                self.lineage_bytes,
                self.schema_json_bytes,
            )?;
            Ok(descriptor.finish())
        }
    }

    const MAXIMUM_RESOURCE_DISCOVERY_ITEM_CHARGE: ResourceDiscoveryItemCharge =
        ResourceDiscoveryItemCharge {
            lineage_bytes: MAX_CONTRACT_LINEAGE_BYTES,
            schema_json_bytes: MAX_JSON_SCHEMA_ARTIFACT_BYTES,
        };

    #[derive(Clone, Copy)]
    struct CompactCommandDiscoveryItemCharge {
        tool_name_bytes: usize,
        source_command_bytes: usize,
        lineage_bytes: usize,
    }

    impl sealed::Sealed for CompactCommandDiscoveryItemCharge {}

    impl ServiceResponseCharge for CompactCommandDiscoveryItemCharge {
        fn service_response_charge_v1(
            &self,
        ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
            let identity = raw_generated_schema_identity_charge(5, 32)?;
            let descriptor = raw_compact_command_tool_descriptor_charge(
                self.tool_name_bytes,
                self.source_command_bytes,
                self.lineage_bytes,
                identity,
                identity,
            )?;
            raw_command_tool_discovery_item_charge(Some(descriptor))
        }
    }

    const MAXIMUM_COMPACT_COMMAND_DISCOVERY_ITEM_CHARGE: CompactCommandDiscoveryItemCharge =
        CompactCommandDiscoveryItemCharge {
            tool_name_bytes: MAX_MCP_COMMAND_TOOL_NAME_BYTES,
            source_command_bytes: 1,
            lineage_bytes: 115,
        };

    const COMPACT_COMMAND_DISCOVERY_PAGE_ITEM_CHARGE: CompactCommandDiscoveryItemCharge =
        CompactCommandDiscoveryItemCharge {
            tool_name_bytes: MAX_MCP_COMMAND_TOOL_NAME_BYTES,
            source_command_bytes: 3,
            lineage_bytes: 113,
        };

    #[derive(Clone, Copy)]
    struct MaximumCompactResourceDiscoveryItemCharge;

    impl sealed::Sealed for MaximumCompactResourceDiscoveryItemCharge {}

    impl ServiceResponseCharge for MaximumCompactResourceDiscoveryItemCharge {
        fn service_response_charge_v1(
            &self,
        ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
            let mut item = ChargeAccumulator::message();
            item.fields(1)?;
            charge_command_resource_shape(&mut item, MAX_CONTRACT_LINEAGE_BYTES, 256)?;
            Ok(item.finish())
        }
    }

    #[derive(Clone, Copy)]
    struct MaximumCompactDiscoveryItemCharge;

    impl sealed::Sealed for MaximumCompactDiscoveryItemCharge {}

    impl ServiceResponseCharge for MaximumCompactDiscoveryItemCharge {
        fn service_response_charge_v1(
            &self,
        ) -> Result<ServiceResponseChargeV1, ServiceResponseChargeOverflow> {
            Ok(ServiceResponseChargeV1(MAX_COMPACT_DISCOVERY_ITEM_BYTES))
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum FixtureDisposition {
        Release,
        ResponseTooLarge,
    }

    impl FixtureDisposition {
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
            Self::from_response_with_limit(
                case_id,
                family,
                variant,
                shape,
                response,
                MAX_SERVICE_RESPONSE_BYTES,
            )
        }

        fn from_response_with_limit<T: ServiceResponseCharge>(
            case_id: &'static str,
            family: &'static str,
            variant: &'static str,
            shape: String,
            response: &T,
            limit: usize,
        ) -> Self {
            let charge = response
                .service_response_charge_v1()
                .expect("fixture response charge must be representable");
            Self::from_raw_charge_with_limit(case_id, family, variant, shape, charge, limit)
        }

        fn from_raw_charge_with_limit(
            case_id: &'static str,
            family: &'static str,
            variant: &'static str,
            shape: String,
            charge: ServiceResponseChargeV1,
            limit: usize,
        ) -> Self {
            Self {
                case_id,
                family,
                variant,
                shape,
                charge_bytes: charge.bytes(),
                disposition: if charge.bytes() <= limit {
                    FixtureDisposition::Release
                } else {
                    FixtureDisposition::ResponseTooLarge
                },
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
        ContractDescriptor::genesis(
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
        let tool_name = riffdb_contract_ir::McpCommandToolNameV2::new_checked(
            "fixture",
            "complete",
            "riffdb_cmd_fixture_complete",
        )
        .expect("fixture tool name");
        let locator = crate::OutcomeResourceLocator::mint(
            ActorId::new("fixture-principal").expect("bounded principal"),
            fixture_lineage(),
            CommandId::first(),
            &tool_name,
            crate::OutcomeLocatorDigestEvidence::new(
                DIGEST_SCHEME_V1,
                DigestKeyId::new(7).expect("nonzero digest key"),
                [0x5a; 32],
            )
            .expect("v1 digest evidence"),
        )
        .expect("canonical outcome locator");
        JournaledCommandResult::new(
            completion,
            CommitSequence::first(),
            fixture_outcome(ExecutionClass::IdempotentMutation),
            ProvenanceId::from_unix_milliseconds_and_random(1, [2; 10])
                .expect("fixture provenance UUIDv7"),
            CommandDurability::Synchronous,
            locator,
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
            "compatibility_class" => "compatible",
            "compatibility_code_count" => 0,
            "compatibility_parent_present" => false,
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
        let descriptor = ContractDescriptor::genesis(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
            bundle.source_hash(),
            bundle.plan_root_hash(),
        );
        let explained = ExplainedCommand::new(
            descriptor,
            command_id,
            bundle
                .mcp_command_names()
                .get(command_id)
                .expect("fixture command name")
                .tool_name()
                .clone(),
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

    fn maximum_discovery_fence(
        operation_schemas: &OperationSchemaCatalog,
    ) -> DiscoveryCatalogFence {
        DiscoveryCatalogFence::active_contract(
            ContractLineage::new("a".repeat(115)).expect("maximum tool-compatible lineage"),
            fixture_contract_version(),
            ContractBundleHash::from_bytes([0x11; 32]),
            operation_schemas.identity(),
        )
    }

    fn maximum_resource_discovery_fence(
        operation_schemas: &OperationSchemaCatalog,
    ) -> DiscoveryCatalogFence {
        DiscoveryCatalogFence::active_contract(
            ContractLineage::new("r".repeat(MAX_CONTRACT_LINEAGE_BYTES))
                .expect("maximum resource lineage is bounded"),
            fixture_contract_version(),
            ContractBundleHash::from_bytes([0x11; 32]),
            operation_schemas.identity(),
        )
    }

    fn representative_resource_discovery_fixture(
        operation_schemas: &OperationSchemaCatalog,
    ) -> DiscoverResourcesResult {
        let page = Page::new(
            PageLimit::new(1).expect("one-item resource page"),
            vec![ResourceDescriptor::active_contract()],
            None,
            maximum_discovery_fence(operation_schemas),
        )
        .expect("representative resource page is bounded");
        let request = DiscoverResourcesRequest::new(PageRequest::new(
            PageLimit::new(1).expect("one-item resource request"),
            None,
        ));
        DiscoverResourcesResult::page(&request, page)
            .expect("representative resource discovery is consistent")
    }

    fn fixture_provenance() -> ProvenanceView {
        fixture_provenance_with_claims(ProvenanceClaimsView::new(None, None, None, None))
    }

    fn maximum_claims_provenance() -> ProvenanceView {
        fixture_provenance_with_claims(ProvenanceClaimsView::new(
            Some(
                SourceRepository::new("r".repeat(MAX_SOURCE_REPOSITORY_BYTES))
                    .expect("maximum repository claim is bounded"),
            ),
            Some(
                SourceCommit::new("c".repeat(MAX_SOURCE_COMMIT_BYTES))
                    .expect("maximum source-commit claim is bounded"),
            ),
            Some(
                ProvenanceReason::new("p".repeat(MAX_PROVENANCE_REASON_BYTES))
                    .expect("maximum provenance reason is bounded"),
            ),
            Some(
                ApprovalId::new("a".repeat(MAX_APPROVAL_ID_BYTES))
                    .expect("maximum approval ID is bounded"),
            ),
        ))
    }

    fn fixture_provenance_with_claims(claims: ProvenanceClaimsView) -> ProvenanceView {
        let snapshot = AuthoritativeProvenanceSnapshot::new(
            ProvenanceId::from_unix_milliseconds_and_random(1, [2; 10])
                .expect("fixture provenance UUIDv7"),
            CommitSequence::first(),
            RequestId::from_unix_milliseconds_and_random(1, [1; 10])
                .expect("fixture request UUIDv7"),
            fixture_lineage(),
            fixture_contract_version(),
            CommandId::first(),
            PlanHash::from_bytes([4; 32]),
            AdmittedActorContext::new(
                ActorId::new("fixture-actor").expect("fixture actor is bounded"),
                ActorKind::Human,
                TenantScope::Global,
                None,
            ),
            LogicalTime::new(Timestamp::new(1, 0).expect("canonical fixture timestamp")),
            OutcomeId::first(),
            vec![AffectedEntityView::new(
                fixture_entity_key(24, 0),
                EntityVersion::first(),
            )],
            vec![EventId::new(CommitSequence::first(), 0)],
            claims,
        )
        .expect("fixture provenance is bounded");
        ProvenanceView::new(snapshot)
    }

    fn representative_command_discovery_fixture() -> (DiscoverCommandToolsResult, String) {
        let bundle =
            compile_contract_source(include_str!("../../../contracts/examples/budget.riff"))
                .expect("checked-in budget example compiles");
        let command = bundle.commands().first().expect("budget has commands");
        let command_name = bundle
            .mcp_command_names()
            .get(command.command_id())
            .expect("budget command has a checked MCP name");
        let input_schema = bundle
            .schema_artifacts()
            .iter()
            .find(|artifact| {
                artifact.key() == SchemaArtifactKey::CommandInput(command.command_id())
            })
            .cloned()
            .expect("budget command input schema");
        let outcome_schema = bundle
            .schema_artifacts()
            .iter()
            .find(|artifact| {
                artifact.key() == SchemaArtifactKey::CommandOutcomeUnion(command.command_id())
            })
            .cloned()
            .expect("budget command outcome schema");
        let descriptor = CommandToolDescriptor::new(
            command_name.tool_name().clone(),
            SourceName::new(command_name.source_command_name())
                .expect("compiler command source is checked"),
            bundle.lineage().clone(),
            bundle.contract_version(),
            command.command_id(),
            input_schema.clone(),
            outcome_schema.clone(),
        )
        .expect("compiler descriptor identities agree");
        let operation_schemas = OperationSchemaCatalog::accepted()
            .expect("accepted operation schemas remain self-consistent");
        let operation_envelope_bytes = operation_schemas
            .command_operation_envelope()
            .canonical_json()
            .len();
        let get_outcome_result_bytes = operation_schemas
            .command_get_outcome_result()
            .canonical_json()
            .len();
        let fence = DiscoveryCatalogFence::active_contract(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
            operation_schemas.identity(),
        );
        let page = Page::new(
            PageLimit::new(1).expect("one-item discovery page"),
            vec![CommandToolDiscoveryItem::Command(Box::new(descriptor))],
            None,
            fence,
        )
        .expect("representative discovery page is bounded");
        let request = DiscoverCommandToolsRequest::new(PageRequest::new(
            PageLimit::new(1).expect("one-item discovery request"),
            None,
        ));
        let result = DiscoverCommandToolsResult::page(&request, page, operation_schemas)
            .expect("representative discovery result is consistent");
        let shape = fixture_shape! {
            "catalog_get_outcome_schema_bytes" => get_outcome_result_bytes,
            "catalog_operation_envelope_schema_bytes" => operation_envelope_bytes,
            "cursor_present" => "false",
            "input_schema_json_bytes" => input_schema.canonical_json().len(),
            "item_count" => 1,
            "outcome_schema_json_bytes" => outcome_schema.canonical_json().len(),
            "representation" => "full"
        };
        (result, shape)
    }

    #[allow(clippy::too_many_lines)]
    fn response_fixture_cases() -> Vec<FixtureCase> {
        let small_commit = fixture_commit(2, 24);
        let oversized_commit = fixture_commit(1_024, 4_096);
        let operation_schemas = OperationSchemaCatalog::accepted()
            .expect("accepted operation schemas remain self-consistent");
        let maximum_fence = maximum_discovery_fence(&operation_schemas);
        let maximum_resource_fence = maximum_resource_discovery_fence(&operation_schemas);
        let command_exact_items = [
            MAXIMUM_COMMAND_DISCOVERY_ITEM_CHARGE,
            CommandDiscoveryItemCharge {
                tool_name_bytes: MAX_MCP_COMMAND_TOOL_NAME_BYTES,
                source_command_bytes: 1,
                lineage_bytes: 115,
                input_schema_json_bytes: 257_097,
                outcome_schema_json_bytes: 257_084,
            },
        ];
        let command_one_over_items = [
            MAXIMUM_COMMAND_DISCOVERY_ITEM_CHARGE,
            CommandDiscoveryItemCharge {
                outcome_schema_json_bytes: 257_085,
                ..command_exact_items[1]
            },
        ];
        let resource_exact_items = [
            MAXIMUM_RESOURCE_DISCOVERY_ITEM_CHARGE,
            MAXIMUM_RESOURCE_DISCOVERY_ITEM_CHARGE,
            ResourceDiscoveryItemCharge {
                lineage_bytes: MAX_CONTRACT_LINEAGE_BYTES,
                schema_json_bytes: 521_892,
            },
        ];
        let resource_one_over_items = [
            MAXIMUM_RESOURCE_DISCOVERY_ITEM_CHARGE,
            MAXIMUM_RESOURCE_DISCOVERY_ITEM_CHARGE,
            ResourceDiscoveryItemCharge {
                schema_json_bytes: 521_893,
                ..resource_exact_items[2]
            },
        ];
        let command_exact_page = Page::new(
            PageLimit::new(2).expect("exact command fixture page limit"),
            command_exact_items.to_vec(),
            Some(CursorToken::from_bytes([0x55; crate::CURSOR_TOKEN_BYTES])),
            maximum_fence.clone(),
        )
        .expect("exact command fixture page is bounded");
        let command_one_over_page = Page::new(
            PageLimit::new(2).expect("one-over command fixture page limit"),
            command_one_over_items.to_vec(),
            Some(CursorToken::from_bytes([0x55; crate::CURSOR_TOKEN_BYTES])),
            maximum_fence.clone(),
        )
        .expect("one-over command fixture page is bounded");
        let maximum_command_page = Page::new(
            PageLimit::new(1).expect("maximum command fixture page limit"),
            vec![MAXIMUM_COMMAND_DISCOVERY_ITEM_CHARGE],
            Some(CursorToken::from_bytes([0x55; crate::CURSOR_TOKEN_BYTES])),
            maximum_fence.clone(),
        )
        .expect("maximum command fixture page is bounded");
        let compact_command_item_page = Page::new(
            PageLimit::new(1).expect("compact command-item fixture page limit"),
            vec![MAXIMUM_COMPACT_COMMAND_DISCOVERY_ITEM_CHARGE],
            None,
            maximum_fence.clone(),
        )
        .expect("compact command-item fixture page is bounded");
        let compact_command_page = Page::new(
            PageLimit::new(500).expect("compact command fixture page limit"),
            vec![COMPACT_COMMAND_DISCOVERY_PAGE_ITEM_CHARGE; 500],
            Some(CursorToken::from_bytes([0x55; crate::CURSOR_TOKEN_BYTES])),
            DiscoveryCatalogFence::active_contract(
                ContractLineage::new("a".repeat(113)).expect("compact-page lineage is bounded"),
                fixture_contract_version(),
                ContractBundleHash::from_bytes([0x11; 32]),
                operation_schemas.identity(),
            ),
        )
        .expect("compact command fixture page is bounded");
        let resource_exact_page = Page::new(
            PageLimit::new(3).expect("exact resource fixture page limit"),
            resource_exact_items.to_vec(),
            Some(CursorToken::from_bytes([0x55; crate::CURSOR_TOKEN_BYTES])),
            maximum_resource_fence.clone(),
        )
        .expect("exact resource fixture page is bounded");
        let resource_one_over_page = Page::new(
            PageLimit::new(3).expect("one-over resource fixture page limit"),
            resource_one_over_items.to_vec(),
            Some(CursorToken::from_bytes([0x55; crate::CURSOR_TOKEN_BYTES])),
            maximum_resource_fence.clone(),
        )
        .expect("one-over resource fixture page is bounded");
        let maximum_resource_page = Page::new(
            PageLimit::new(1).expect("maximum resource fixture page limit"),
            vec![MAXIMUM_RESOURCE_DISCOVERY_ITEM_CHARGE],
            Some(CursorToken::from_bytes([0x55; crate::CURSOR_TOKEN_BYTES])),
            maximum_resource_fence.clone(),
        )
        .expect("maximum resource fixture page is bounded");
        let compact_resource_item_page = Page::new(
            PageLimit::new(1).expect("compact resource-item fixture page limit"),
            vec![MaximumCompactResourceDiscoveryItemCharge],
            None,
            maximum_resource_fence.clone(),
        )
        .expect("compact resource-item fixture page is bounded");
        let compact_resource_page = Page::new(
            PageLimit::new(500).expect("compact resource fixture page limit"),
            vec![MaximumCompactResourceDiscoveryItemCharge; 500],
            Some(CursorToken::from_bytes([0x55; crate::CURSOR_TOKEN_BYTES])),
            maximum_resource_fence.clone(),
        )
        .expect("compact resource fixture page is bounded");
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
        cases.push(FixtureCase::from_raw_charge_with_limit(
            "discover_command_tools.full_exact_ceiling",
            "discover_command_tools",
            "full_exact_ceiling",
            fixture_shape! {
                "cursor_bytes" => crate::CURSOR_TOKEN_BYTES,
                "item_0_input_schema_json_bytes" => MAX_JSON_SCHEMA_ARTIFACT_BYTES,
                "item_0_outcome_schema_json_bytes" => MAX_JSON_SCHEMA_ARTIFACT_BYTES,
                "item_1_input_schema_json_bytes" => 257_097,
                "item_1_outcome_schema_json_bytes" => 257_084,
                "item_count" => 2,
                "lineage_bytes" => 115,
                "representation" => "full",
                "source_command_bytes_per_item" => 1,
                "tool_name_bytes_per_item" => MAX_MCP_COMMAND_TOOL_NAME_BYTES
            },
            raw_discovery_page_response_charge(&command_exact_page, Some(&operation_schemas))
                .expect("exact command discovery charge is representable"),
            MAX_FULL_DISCOVERY_RESPONSE_BYTES,
        ));
        cases.push(FixtureCase::from_raw_charge_with_limit(
            "discover_command_tools.full_one_over",
            "discover_command_tools",
            "full_one_over",
            fixture_shape! {
                "cursor_bytes" => crate::CURSOR_TOKEN_BYTES,
                "item_0_input_schema_json_bytes" => MAX_JSON_SCHEMA_ARTIFACT_BYTES,
                "item_0_outcome_schema_json_bytes" => MAX_JSON_SCHEMA_ARTIFACT_BYTES,
                "item_1_input_schema_json_bytes" => 257_097,
                "item_1_outcome_schema_json_bytes" => 257_085,
                "item_count" => 2,
                "lineage_bytes" => 115,
                "representation" => "full",
                "source_command_bytes_per_item" => 1,
                "tool_name_bytes_per_item" => MAX_MCP_COMMAND_TOOL_NAME_BYTES
            },
            raw_discovery_page_response_charge(&command_one_over_page, Some(&operation_schemas))
                .expect("one-over command discovery charge is representable"),
            MAX_FULL_DISCOVERY_RESPONSE_BYTES,
        ));

        cases.push(FixtureCase::from_raw_charge_with_limit(
            "discover_command_tools.compact_item_max",
            "discover_command_tools",
            "compact_item_max",
            fixture_shape! {
                "command_id" => u32::MAX,
                "contract_version" => u64::MAX,
                "cursor_present" => false,
                "item_charge_bytes" => MAXIMUM_COMPACT_COMMAND_DISCOVERY_ITEM_CHARGE.service_response_charge_v1().expect("maximum compact command charge").bytes(),
                "item_count" => 1,
                "lineage_bytes" => 115,
                "representation" => "compact_observation",
                "source_command_bytes" => 1,
                "tool_name_bytes" => MAX_MCP_COMMAND_TOOL_NAME_BYTES
            },
            raw_discovery_page_response_charge(&compact_command_item_page, None)
                .expect("compact command-item response charge is representable"),
            MAX_SERVICE_RESPONSE_BYTES,
        ));
        cases.push(FixtureCase::from_raw_charge_with_limit(
            "discover_command_tools.compact_page_max",
            "discover_command_tools",
            "compact_page_max",
            fixture_shape! {
                "command_id_first" => u32::MAX - 499,
                "command_id_last" => u32::MAX,
                "contract_version" => u64::MAX,
                "cursor_bytes" => crate::CURSOR_TOKEN_BYTES,
                "item_charge_bytes" => COMPACT_COMMAND_DISCOVERY_PAGE_ITEM_CHARGE.service_response_charge_v1().expect("maximum compact command charge").bytes(),
                "item_count" => 500,
                "lineage_bytes" => 113,
                "representation" => "compact_observation",
                "source_command_bytes" => 3
            },
            raw_discovery_page_response_charge(&compact_command_page, None)
                .expect("compact command response charge is representable"),
            MAX_SERVICE_RESPONSE_BYTES,
        ));
        cases.push(FixtureCase::from_raw_charge_with_limit(
            "discover_command_tools.full_max_dynamic",
            "discover_command_tools",
            "full_max_dynamic",
            fixture_shape! {
                "command_tool_name_bytes" => MAX_MCP_COMMAND_TOOL_NAME_BYTES,
                "command_id" => u32::MAX,
                "contract_version" => u64::MAX,
                "cursor_bytes" => crate::CURSOR_TOKEN_BYTES,
                "input_schema_json_bytes" => MAX_JSON_SCHEMA_ARTIFACT_BYTES,
                "item_count" => 1,
                "lineage_bytes" => 115,
                "outcome_schema_json_bytes" => MAX_JSON_SCHEMA_ARTIFACT_BYTES,
                "representation" => "full",
                "source_command_bytes" => 1
            },
            raw_discovery_page_response_charge(&maximum_command_page, Some(&operation_schemas))
                .expect("maximum command discovery charge is representable"),
            MAX_FULL_DISCOVERY_RESPONSE_BYTES,
        ));
        let (representative_discovery, representative_discovery_shape) =
            representative_command_discovery_fixture();
        cases.push(FixtureCase::from_response(
            "discover_command_tools.full_representative",
            "discover_command_tools",
            "full_representative",
            representative_discovery_shape,
            &representative_discovery,
        ));
        let unchanged_command_request = DiscoverCommandToolsRequest::with_options(
            PageRequest::new(PageLimit::new(1).expect("one-item request"), None),
            crate::DiscoveryRepresentation::CompactObservation,
            Some(maximum_fence.clone()),
        )
        .expect("conditional compact command request");
        cases.push(FixtureCase::from_response(
            "discover_command_tools.catalog_unchanged",
            "discover_command_tools",
            "catalog_unchanged",
            fixture_shape! {
                "catalog_state" => "active_contract",
                "lineage_bytes" => 115,
                "representation" => "compact_observation"
            },
            &DiscoverCommandToolsResult::catalog_unchanged(
                &unchanged_command_request,
                maximum_fence.clone(),
            )
            .expect("matching compact command fence"),
        ));
        cases.push(FixtureCase::from_raw_charge_with_limit(
            "discover_resources.compact_item_max",
            "discover_resources",
            "compact_item_max",
            fixture_shape! {
                "command_id" => u32::MAX,
                "contract_version" => u64::MAX,
                "cursor_present" => false,
                "item_charge_bytes" => MaximumCompactResourceDiscoveryItemCharge.service_response_charge_v1().expect("maximum compact resource charge").bytes(),
                "item_count" => 1,
                "lineage_bytes" => MAX_CONTRACT_LINEAGE_BYTES,
                "representation" => "compact_observation",
                "resource_kind" => "command_plan",
                "source_command_bytes" => 256
            },
            raw_discovery_page_response_charge(&compact_resource_item_page, None)
                .expect("compact resource-item response charge is representable"),
            MAX_SERVICE_RESPONSE_BYTES,
        ));
        cases.push(FixtureCase::from_raw_charge_with_limit(
            "discover_resources.compact_page_max",
            "discover_resources",
            "compact_page_max",
            fixture_shape! {
                "command_id_first" => u32::MAX - 499,
                "command_id_last" => u32::MAX,
                "contract_version" => u64::MAX,
                "cursor_bytes" => crate::CURSOR_TOKEN_BYTES,
                "item_charge_bytes" => MaximumCompactResourceDiscoveryItemCharge.service_response_charge_v1().expect("maximum compact resource charge").bytes(),
                "item_count" => 500,
                "lineage_bytes" => MAX_CONTRACT_LINEAGE_BYTES,
                "representation" => "compact_observation",
                "resource_kind" => "command_plan",
                "source_command_bytes" => 256
            },
            raw_discovery_page_response_charge(&compact_resource_page, None)
                .expect("compact resource response charge is representable"),
            MAX_SERVICE_RESPONSE_BYTES,
        ));
        cases.push(FixtureCase::from_raw_charge_with_limit(
            "discover_resources.full_exact_ceiling",
            "discover_resources",
            "full_exact_ceiling",
            fixture_shape! {
                "cursor_bytes" => crate::CURSOR_TOKEN_BYTES,
                "item_0_schema_json_bytes" => MAX_JSON_SCHEMA_ARTIFACT_BYTES,
                "item_1_schema_json_bytes" => MAX_JSON_SCHEMA_ARTIFACT_BYTES,
                "item_2_schema_json_bytes" => 521_892,
                "item_count" => 3,
                "lineage_bytes" => MAX_CONTRACT_LINEAGE_BYTES,
                "representation" => "full",
                "resource_kind" => "entity_schema"
            },
            raw_discovery_page_response_charge(&resource_exact_page, None)
                .expect("exact resource discovery charge is representable"),
            MAX_FULL_DISCOVERY_RESPONSE_BYTES,
        ));
        cases.push(FixtureCase::from_raw_charge_with_limit(
            "discover_resources.full_one_over",
            "discover_resources",
            "full_one_over",
            fixture_shape! {
                "cursor_bytes" => crate::CURSOR_TOKEN_BYTES,
                "item_0_schema_json_bytes" => MAX_JSON_SCHEMA_ARTIFACT_BYTES,
                "item_1_schema_json_bytes" => MAX_JSON_SCHEMA_ARTIFACT_BYTES,
                "item_2_schema_json_bytes" => 521_893,
                "item_count" => 3,
                "lineage_bytes" => MAX_CONTRACT_LINEAGE_BYTES,
                "representation" => "full",
                "resource_kind" => "entity_schema"
            },
            raw_discovery_page_response_charge(&resource_one_over_page, None)
                .expect("one-over resource discovery charge is representable"),
            MAX_FULL_DISCOVERY_RESPONSE_BYTES,
        ));
        cases.push(FixtureCase::from_raw_charge_with_limit(
            "discover_resources.full_max_dynamic",
            "discover_resources",
            "full_max_dynamic",
            fixture_shape! {
                "contract_version" => u64::MAX,
                "cursor_bytes" => crate::CURSOR_TOKEN_BYTES,
                "entity_schema_json_bytes" => MAX_JSON_SCHEMA_ARTIFACT_BYTES,
                "entity_type_id" => u32::MAX,
                "item_count" => 1,
                "lineage_bytes" => MAX_CONTRACT_LINEAGE_BYTES,
                "representation" => "full",
                "resource_kind" => "entity_schema"
            },
            raw_discovery_page_response_charge(&maximum_resource_page, None)
                .expect("maximum resource discovery charge is representable"),
            MAX_FULL_DISCOVERY_RESPONSE_BYTES,
        ));
        let representative_resources =
            representative_resource_discovery_fixture(&operation_schemas);
        cases.push(FixtureCase::from_response_with_limit(
            "discover_resources.full_representative",
            "discover_resources",
            "full_representative",
            fixture_shape! {
                "cursor_present" => "false",
                "item_count" => 1,
                "representation" => "full",
                "resource_kind" => "active_contract"
            },
            &representative_resources,
            MAX_FULL_DISCOVERY_RESPONSE_BYTES,
        ));
        let unchanged_resource_request = DiscoverResourcesRequest::with_options(
            PageRequest::new(PageLimit::new(1).expect("one-item request"), None),
            crate::DiscoveryRepresentation::CompactObservation,
            Some(maximum_resource_fence.clone()),
            crate::ResourceDiscoveryKind::All,
        )
        .expect("conditional compact resource request");
        cases.push(FixtureCase::from_response(
            "discover_resources.catalog_unchanged",
            "discover_resources",
            "catalog_unchanged",
            fixture_shape! {
                "catalog_state" => "active_contract",
                "lineage_bytes" => MAX_CONTRACT_LINEAGE_BYTES,
                "representation" => "compact_observation",
                "resource_kind" => "all"
            },
            &DiscoverResourcesResult::catalog_unchanged(
                &unchanged_resource_request,
                maximum_resource_fence.clone(),
            )
            .expect("matching compact resource fence"),
        ));
        cases.push(FixtureCase::from_response(
            "operation_schema_catalog.accepted",
            "operation_schema_catalog",
            "accepted",
            fixture_shape! {
                "artifact_count" => 2,
                "get_outcome_schema_bytes" => operation_schemas.command_get_outcome_result().canonical_json().len(),
                "operation_envelope_schema_bytes" => operation_schemas.command_operation_envelope().canonical_json().len()
            },
            &operation_schemas,
        ));

        let maintenance_backup_name =
            BackupNameV1::new("before-upgrade").expect("fixture backup name");
        let maintenance_input_hash = offline_maintenance_input_hash(
            OfflineMaintenanceOperationKind::CreateBackup,
            &maintenance_backup_name,
            OfflineMaintenanceReplacementConfirmation::NotProvided,
        );
        let maintenance_operation = OfflineMaintenanceOperationObservation::new(
            OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(1, [0x31; 10])
                .expect("fixture maintenance operation UUIDv7"),
            OfflineMaintenanceOperationKind::CreateBackup,
            maintenance_backup_name.clone(),
            maintenance_input_hash,
            crate::OfflineMaintenanceObservationPhase::Accepted,
            None,
        )
        .expect("fixture maintenance observation is valid");
        cases.push(FixtureCase::from_response(
            "offline_maintenance_get.found",
            "offline_maintenance_get",
            "found",
            fixture_shape! {
                "backup_name_bytes" => maintenance_backup_name.as_bytes().len(),
                "failure_present" => "false",
                "input_hash_bytes" => maintenance_input_hash.as_bytes().len(),
                "operation_id_bytes" => maintenance_operation.operation_id().as_bytes().len(),
                "operation_kind" => "create_backup",
                "phase" => "accepted"
            },
            &GetOfflineMaintenanceOperationResult::Found(maintenance_operation.clone()),
        ));
        cases.push(FixtureCase::from_response(
            "offline_maintenance_get.not_found",
            "offline_maintenance_get",
            "not_found",
            "none".to_owned(),
            &GetOfflineMaintenanceOperationResult::NotFound,
        ));
        cases.push(FixtureCase::from_response(
            "offline_maintenance_start.accepted",
            "offline_maintenance_start",
            "accepted",
            fixture_shape! {
                "backup_name_bytes" => maintenance_backup_name.as_bytes().len(),
                "failure_present" => "false",
                "input_hash_bytes" => maintenance_input_hash.as_bytes().len(),
                "operation_id_bytes" => maintenance_operation.operation_id().as_bytes().len(),
                "operation_kind" => "create_backup",
                "phase" => "accepted"
            },
            &OfflineMaintenanceStartResult::new(
                crate::OfflineMaintenanceStartDisposition::Accepted,
                maintenance_operation,
            )
            .expect("accepted maintenance start is nonterminal"),
        ));
        let failed_maintenance_operation = OfflineMaintenanceOperationObservation::new(
            OfflineMaintenanceOperationId::from_unix_milliseconds_and_random(2, [0x32; 10])
                .expect("fixture failed maintenance operation UUIDv7"),
            OfflineMaintenanceOperationKind::RestoreBackup,
            maintenance_backup_name.clone(),
            offline_maintenance_input_hash(
                OfflineMaintenanceOperationKind::RestoreBackup,
                &maintenance_backup_name,
                OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget,
            ),
            crate::OfflineMaintenanceObservationPhase::FailedClosed,
            Some(crate::OfflineMaintenanceObservationFailure::ValidationFailed),
        )
        .expect("fixture failed maintenance observation is valid");
        cases.push(FixtureCase::from_response(
            "offline_maintenance_start.terminal_failed_closed",
            "offline_maintenance_start",
            "terminal",
            fixture_shape! {
                "backup_name_bytes" => maintenance_backup_name.as_bytes().len(),
                "failure_present" => "true",
                "input_hash_bytes" => failed_maintenance_operation.input_hash().as_bytes().len(),
                "operation_id_bytes" => failed_maintenance_operation.operation_id().as_bytes().len(),
                "operation_kind" => "restore_backup",
                "phase" => "failed_closed"
            },
            &OfflineMaintenanceStartResult::new(
                crate::OfflineMaintenanceStartDisposition::Terminal,
                failed_maintenance_operation,
            )
            .expect("terminal maintenance start has a terminal observation"),
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
                "compatibility_class" => "compatible",
                "compatibility_code_count" => 0,
                "compatibility_parent_present" => false,
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
                "outcome_locator_bytes" => 107,
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
                "compatibility_class" => "compatible",
                "compatibility_code_count" => 0,
                "compatibility_parent_present" => false,
                "contract_version" => 1,
                "lineage_bytes" => 7,
                "plan_root_hash_bytes" => 32,
                "source_hash_bytes" => 32
            },
            &GetActiveContractResult::Present(fixture_contract_descriptor()),
        ));

        cases.push(FixtureCase::from_response(
            "get_contract_version.found",
            "get_contract_version",
            "found",
            fixture_shape! {
                "bundle_hash_bytes" => 32,
                "compatibility_class" => "compatible",
                "compatibility_code_count" => 0,
                "compatibility_parent_present" => false,
                "contract_version" => 1,
                "lineage_bytes" => 7,
                "plan_root_hash_bytes" => 32,
                "source_hash_bytes" => 32
            },
            &GetContractVersionResult::Found(fixture_contract_descriptor()),
        ));
        cases.push(FixtureCase::from_response(
            "get_contract_version.not_found",
            "get_contract_version",
            "not_found",
            "none".to_owned(),
            &GetContractVersionResult::NotFound,
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
                "outcome_locator_bytes" => 107,
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

        let retry_timestamp = Timestamp::new(2, 0).expect("fixture retry timestamp");
        for (case_id, variant, state_name, state, attempts, next_attempt_at) in [
            (
                "list_pending_outbox_deliveries.pending",
                "pending",
                "pending",
                OutboxDeliveryState::Pending,
                0,
                None,
            ),
            (
                "list_pending_outbox_deliveries.retry_scheduled",
                "retry_scheduled",
                "retry_scheduled",
                OutboxDeliveryState::RetryScheduled,
                1,
                Some(retry_timestamp),
            ),
            (
                "list_pending_outbox_deliveries.delivering",
                "delivering",
                "delivering",
                OutboxDeliveryState::Delivering,
                1,
                None,
            ),
            (
                "list_pending_outbox_deliveries.dead_letter",
                "dead_letter",
                "dead_letter",
                OutboxDeliveryState::DeadLetter,
                3,
                None,
            ),
        ] {
            let outbox_page = Page::new(
                PageLimit::new(1).expect("fixture outbox page limit"),
                vec![OutboxDeliverySummary::new(
                    EventId::new(CommitSequence::first(), 0),
                    state,
                    attempts,
                    next_attempt_at,
                )],
                Some(CursorToken::from_bytes([0x44; crate::CURSOR_TOKEN_BYTES])),
                (),
            )
            .expect("fixture outbox page is bounded");
            cases.push(FixtureCase::from_response(
                case_id,
                "list_pending_outbox_deliveries",
                variant,
                fixture_shape! {
                    "attempts" => attempts,
                    "cursor_bytes" => crate::CURSOR_TOKEN_BYTES,
                    "item_count" => 1,
                    "next_attempt_present" => next_attempt_at.is_some(),
                    "state" => state_name
                },
                &ListPendingOutboxDeliveriesResult::new(outbox_page),
            ));
        }

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
        cases.push(FixtureCase::from_response(
            "projection_status.not_found",
            "projection_status",
            "not_found",
            "none".to_owned(),
            &GetProjectionStatusResult::NotFound,
        ));

        let first_generation = ProjectionGeneration::first();
        let second_generation = first_generation.checked_next().expect("second generation");
        let first_sequence = CommitSequence::first();
        let second_sequence = first_sequence.checked_next().expect("second sequence");
        let before_first =
            ProjectionGenerationFrontier::new(first_generation, FrontierPosition::BeforeFirst);
        let published = ProjectionGenerationFrontier::new(
            first_generation,
            FrontierPosition::AppliedThrough(first_sequence),
        );
        let candidate_second = ProjectionGenerationFrontier::new(
            second_generation,
            FrontierPosition::AppliedThrough(first_sequence),
        );
        let authoritative_head = FrontierPosition::AppliedThrough(second_sequence);
        let initialized_status = |highest, lifecycle, published, candidate, mode, failure| {
            ProjectionStatusSnapshot::from_initialized_control(
                fixture_projection_identity(),
                highest,
                lifecycle,
                published,
                candidate,
                mode,
                failure,
                authoritative_head,
            )
            .expect("fixture projection lifecycle is valid")
        };
        let initialized_cases = [
            (
                "projection_status.building",
                "building",
                initialized_status(
                    first_generation,
                    ProjectionLifecycle::Building,
                    None,
                    Some(before_first),
                    None,
                    None,
                ),
            ),
            (
                "projection_status.catching_up",
                "catching_up",
                initialized_status(
                    first_generation,
                    ProjectionLifecycle::CatchingUp,
                    None,
                    Some(published),
                    None,
                    None,
                ),
            ),
            (
                "projection_status.ready",
                "ready",
                initialized_status(
                    first_generation,
                    ProjectionLifecycle::Ready,
                    Some(published),
                    None,
                    Some(PublishedApplyMode::Enabled),
                    None,
                ),
            ),
            (
                "projection_status.rebuilding",
                "rebuilding",
                initialized_status(
                    second_generation,
                    ProjectionLifecycle::Rebuilding,
                    Some(published),
                    Some(candidate_second),
                    Some(PublishedApplyMode::Enabled),
                    None,
                ),
            ),
            (
                "projection_status.degraded",
                "degraded",
                initialized_status(
                    first_generation,
                    ProjectionLifecycle::Degraded,
                    Some(published),
                    None,
                    Some(PublishedApplyMode::Suspended),
                    Some(ProjectionFailure::new(
                        first_generation,
                        ProjectionFailureCode::MalformedDurableEvent,
                        Some(second_sequence),
                    )),
                ),
            ),
            (
                "projection_status.invalid",
                "invalid",
                initialized_status(
                    first_generation,
                    ProjectionLifecycle::Invalid,
                    None,
                    Some(published),
                    None,
                    Some(ProjectionFailure::new(
                        first_generation,
                        ProjectionFailureCode::StateIntegrityFailure,
                        Some(second_sequence),
                    )),
                ),
            ),
        ];
        for (case_id, lifecycle, status) in initialized_cases {
            cases.push(FixtureCase::from_response(
                case_id,
                "projection_status",
                lifecycle,
                fixture_shape! {
                    "authoritative_frontier" => "applied_through",
                    "authoritative_sequence" => 2,
                    "candidate_present" => status.candidate().is_some(),
                    "failure_present" => status.failure().is_some(),
                    "lifecycle" => lifecycle,
                    "lineage_bytes" => 7,
                    "plan_hash_bytes" => 32,
                    "projection_id" => 1,
                    "published_present" => status.published().is_some()
                },
                &GetProjectionStatusResult::Found(status),
            ));
        }

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

        cases.push(FixtureCase::from_response(
            "trace_provenance.found",
            "trace_provenance",
            "found",
            fixture_shape! {
                "actor_id_bytes" => 13,
                "affected_entity_count" => 1,
                "claim_count" => 0,
                "entity_key_bytes" => 24,
                "event_count" => 1,
                "lineage_bytes" => 7,
                "tenant_scope" => "global"
            },
            &TraceProvenanceResult::Found(Box::new(fixture_provenance())),
        ));
        cases.push(FixtureCase::from_response(
            "trace_provenance.found_max_claims",
            "trace_provenance",
            "found",
            fixture_shape! {
                "actor_id_bytes" => 13,
                "affected_entity_count" => 1,
                "approval_id_bytes" => MAX_APPROVAL_ID_BYTES,
                "claim_count" => 4,
                "entity_key_bytes" => 24,
                "event_count" => 1,
                "lineage_bytes" => 7,
                "reason_bytes" => MAX_PROVENANCE_REASON_BYTES,
                "source_commit_bytes" => MAX_SOURCE_COMMIT_BYTES,
                "source_repository_bytes" => MAX_SOURCE_REPOSITORY_BYTES,
                "tenant_scope" => "global"
            },
            &TraceProvenanceResult::Found(Box::new(maximum_claims_provenance())),
        ));
        cases.push(FixtureCase::from_response(
            "trace_provenance.not_found",
            "trace_provenance",
            "not_found",
            "none".to_owned(),
            &TraceProvenanceResult::NotFound,
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

    fn fixture_case_limit(case_id: &str) -> usize {
        if case_id.starts_with("discover_command_tools.full_")
            || case_id.starts_with("discover_resources.full_")
        {
            MAX_FULL_DISCOVERY_RESPONSE_BYTES
        } else {
            MAX_SERVICE_RESPONSE_BYTES
        }
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
            let expected_disposition = if charge <= fixture_case_limit(case_id) {
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
    fn raw_discovery_page_charge_matches_checked_public_dtos() {
        let (commands, _) = representative_command_discovery_fixture();
        let DiscoverCommandToolsResultRef::Page {
            page,
            operation_schemas,
        } = commands.result()
        else {
            panic!("representative command discovery must be a full page");
        };
        assert_eq!(
            raw_discovery_page_response_charge(page, Some(operation_schemas))
                .expect("raw command discovery charge"),
            commands
                .service_response_charge_v1()
                .expect("checked command discovery charge")
        );

        let operation_schemas =
            OperationSchemaCatalog::accepted().expect("accepted operation schemas");
        let resources = representative_resource_discovery_fixture(&operation_schemas);
        let DiscoverResourcesResultRef::Page(page) = resources.result() else {
            panic!("representative resource discovery must be a full page");
        };
        assert_eq!(
            raw_discovery_page_response_charge(page, None).expect("raw resource discovery charge"),
            resources
                .service_response_charge_v1()
                .expect("checked resource discovery charge")
        );
    }

    #[test]
    fn response_charge_fixture_covers_the_complete_checked_response_registry() {
        let parsed = parse_response_charge_fixture(RESPONSE_CHARGE_FIXTURE);
        let actual_families = parsed
            .iter()
            .filter(|row| row.family != "boundary")
            .map(|row| row.family)
            .collect::<BTreeSet<_>>();
        let expected_families = CHECKED_RESPONSE_FAMILIES
            .into_iter()
            .collect::<BTreeSet<_>>();
        assert_eq!(actual_families, expected_families);
        assert!(
            parsed
                .iter()
                .any(|row| row.case_id == "boundary.exact_ceiling")
        );
        assert!(parsed.iter().any(|row| row.case_id == "boundary.one_over"));
        for case_id in [
            "discover_command_tools.full_exact_ceiling",
            "discover_command_tools.full_one_over",
            "discover_resources.full_exact_ceiling",
            "discover_resources.full_one_over",
        ] {
            assert!(parsed.iter().any(|row| row.case_id == case_id));
        }
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
    fn full_discovery_ceiling_is_inclusive_and_whole_item_fitting_stops_before_overflow() {
        let operation_schemas = OperationSchemaCatalog::accepted().expect("accepted catalog");
        let fence = maximum_discovery_fence(&operation_schemas);
        let mut base_with_cursor = ChargeAccumulator::message();
        base_with_cursor
            .add(PAGE_WRAPPER_RESERVE)
            .expect("page framing is representable");
        base_with_cursor
            .nested(&operation_schemas)
            .expect("catalog charge is representable");
        base_with_cursor
            .nested(&fence)
            .expect("fence charge is representable");
        base_with_cursor
            .bytes(crate::CURSOR_TOKEN_BYTES)
            .expect("cursor charge is representable");
        let maximum_item_charge = MAX_FULL_DISCOVERY_RESPONSE_BYTES
            .checked_sub(base_with_cursor.finish().bytes() + FIELD_RESERVE)
            .expect("full discovery framing leaves item capacity");

        let exact = fit_full_command_discovery_page_items(
            &[ExactCharge(maximum_item_charge)],
            &fence,
            &operation_schemas,
            true,
        )
        .expect("one exact-ceiling item fits whole");
        assert_eq!(exact.item_count(), 1);
        assert!(exact.has_more());
        assert!(matches!(
            fit_full_command_discovery_page_items(
                &[ExactCharge(maximum_item_charge + 1)],
                &fence,
                &operation_schemas,
                true,
            ),
            Err(ServiceFailure::ResponseTooLarge)
        ));

        let first_only = fit_full_command_discovery_page_items(
            &[ExactCharge(maximum_item_charge), ExactCharge(17)],
            &fence,
            &operation_schemas,
            false,
        )
        .expect("the first whole item fits with a continuation");
        assert_eq!(first_only.item_count(), 1);
        assert!(first_only.has_more());
    }

    #[test]
    fn discovery_maximum_charges_leave_the_accepted_headroom() {
        let operation_schemas = OperationSchemaCatalog::accepted().expect("accepted catalog");
        assert_eq!(
            operation_schemas
                .service_response_charge_v1()
                .expect("catalog charge")
                .bytes(),
            7_864
        );
        let fence = maximum_discovery_fence(&operation_schemas);
        assert_eq!(
            MAXIMUM_COMMAND_DISCOVERY_ITEM_CHARGE
                .service_response_charge_v1()
                .expect("maximum dynamic item charge")
                .bytes(),
            2_097_884
        );
        let maximum_dynamic_page = Page::new(
            PageLimit::new(1).expect("maximum command page limit"),
            vec![MAXIMUM_COMMAND_DISCOVERY_ITEM_CHARGE],
            Some(CursorToken::from_bytes([0x55; crate::CURSOR_TOKEN_BYTES])),
            fence.clone(),
        )
        .expect("maximum command page is bounded");
        let maximum_dynamic =
            raw_discovery_page_response_charge(&maximum_dynamic_page, Some(&operation_schemas))
                .expect("maximum dynamic response charge");
        assert_eq!(maximum_dynamic.bytes(), 2_106_511);
        assert!(maximum_dynamic.bytes() <= MAX_FULL_DISCOVERY_RESPONSE_BYTES);
        let maximum_dynamic_fit = fit_full_command_discovery_page_items(
            &[MAXIMUM_COMMAND_DISCOVERY_ITEM_CHARGE],
            &fence,
            &operation_schemas,
            true,
        )
        .expect("one maximum dynamic item fits whole");
        assert_eq!(maximum_dynamic_fit.item_count(), 1);
        assert!(maximum_dynamic_fit.has_more());

        let maximum_resource_fence = maximum_resource_discovery_fence(&operation_schemas);
        assert_eq!(
            MAXIMUM_RESOURCE_DISCOVERY_ITEM_CHARGE
                .service_response_charge_v1()
                .expect("maximum resource item charge")
                .bytes(),
            1_049_068
        );
        let maximum_resource_page = Page::new(
            PageLimit::new(1).expect("maximum resource page limit"),
            vec![MAXIMUM_RESOURCE_DISCOVERY_ITEM_CHARGE],
            Some(CursorToken::from_bytes([0x55; crate::CURSOR_TOKEN_BYTES])),
            maximum_resource_fence.clone(),
        )
        .expect("maximum resource page is bounded");
        let maximum_resource = raw_discovery_page_response_charge(&maximum_resource_page, None)
            .expect("maximum resource response charge");
        assert_eq!(maximum_resource.bytes(), 1_049_956);
        assert!(maximum_resource.bytes() <= MAX_FULL_DISCOVERY_RESPONSE_BYTES);

        assert_eq!(
            MaximumCompactDiscoveryItemCharge
                .service_response_charge_v1()
                .expect("maximum compact item charge")
                .bytes(),
            MAX_COMPACT_DISCOVERY_ITEM_BYTES
        );
        let compact_items = vec![MaximumCompactDiscoveryItemCharge; 500];
        let maximum_compact_page = Page::new(
            PageLimit::new(500).expect("maximum compact page limit"),
            compact_items.clone(),
            Some(CursorToken::from_bytes([0x55; crate::CURSOR_TOKEN_BYTES])),
            fence.clone(),
        )
        .expect("maximum compact page is bounded");
        let maximum_compact_page = raw_discovery_page_response_charge(&maximum_compact_page, None)
            .expect("maximum compact page charge");
        assert_eq!(maximum_compact_page.bytes(), 2_056_731);
        assert!(maximum_compact_page.bytes() < MAX_SERVICE_RESPONSE_BYTES);
        let maximum_compact_fit = fit_page_items(&compact_items, &fence, true)
            .expect("500 maximum compact items fit with a continuation");
        assert_eq!(maximum_compact_fit.item_count(), 500);
        assert!(maximum_compact_fit.has_more());

        let maximum_resource_compact_page = Page::new(
            PageLimit::new(500).expect("maximum compact resource page limit"),
            compact_items,
            Some(CursorToken::from_bytes([0x55; crate::CURSOR_TOKEN_BYTES])),
            maximum_resource_fence,
        )
        .expect("maximum compact resource page is bounded");
        let maximum_resource_compact_page =
            raw_discovery_page_response_charge(&maximum_resource_compact_page, None)
                .expect("maximum compact resource page charge");
        assert_eq!(maximum_resource_compact_page.bytes(), 2_056_872);
        assert!(maximum_resource_compact_page.bytes() < MAX_SERVICE_RESPONSE_BYTES);

        let (representative, _) = representative_command_discovery_fixture();
        let representative_charge = representative
            .service_response_charge_v1()
            .expect("representative full page charge");
        assert!(representative_charge.bytes() <= MAX_FULL_DISCOVERY_RESPONSE_BYTES);
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
        assert_charge::<OfflineMaintenanceStartResult>();
        assert_charge::<GetOfflineMaintenanceOperationResult>();
        assert_charge::<ListPendingOutboxDeliveriesResult>();
        assert_charge::<DiscoverCommandToolsResult>();
        assert_charge::<DiscoverResourcesResult>();
    }
}
