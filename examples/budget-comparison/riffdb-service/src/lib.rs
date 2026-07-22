//! Budget comparison adapter over RiffDB's API-neutral application service.

#![forbid(unsafe_code)]

use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::pin;
use std::sync::{Arc, Barrier};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;

use riffdb_budget_comparison_core::{
    AllocateBudget, Amount, BudgetBackend, BudgetKey, BudgetOperation, BudgetOutcome, BudgetState,
    CommandObservation, ContentionObservation, ContentionWorkload, CreateBudget, GuaranteeLevel,
    GuaranteeProfile, OrganizationId,
};
use riffdb_service::{
    CommandApplication, CommandDurability, ContractSelection, ExecuteCommandRequest,
    ExecuteCommandResult, FieldSelection, GetEntityRequest, GetEntityResult, JournaledCompletion,
    QueryApplication, RequestContext, ServiceFailure, SourceName, SubmittedRecord,
};
use riffdb_types::{
    CanonicalRecord, CanonicalValue, CommitSequence, ContractLineage, ContractVersion, DecimalSpec,
    EntityKeyBuilder, EntityTypeId, FieldId, ProvenanceId,
};

const CREATE_BUDGET: &str = "CreateBudget";
const ALLOCATE_BUDGET: &str = "AllocateBudget";

/// The two application-service surfaces needed by the comparison adapter.
pub trait BudgetApplication: CommandApplication + QueryApplication {}

impl<T> BudgetApplication for T where T: CommandApplication + QueryApplication {}

/// Supplies one fresh authenticated in-process comparison context per operation.
pub trait RequestContextFactory: Send + Sync {
    /// Constructs a fresh request identity, control, and authenticated principal.
    fn next_context(&self) -> RequestContext;
}

impl<F> RequestContextFactory for F
where
    F: Fn() -> RequestContext + Send + Sync,
{
    fn next_context(&self) -> RequestContext {
        self()
    }
}

/// Stable fields of the canonical `Budget` entity used by this application.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BudgetEntityFields {
    /// `organization_id` primary-key field.
    pub organization_id: FieldId,
    /// `fiscal_year` primary-key field.
    pub fiscal_year: FieldId,
    /// `approved_amount` field.
    pub approved_amount: FieldId,
    /// `allocated_amount` field.
    pub allocated_amount: FieldId,
    /// `updated_at` field, validated but normalized out of shared observations.
    pub updated_at: FieldId,
}

/// Stable input fields of the canonical `CreateBudget` command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CreateBudgetInputFields {
    /// Caller-supplied idempotency key.
    pub idempotency_key: FieldId,
    /// Organization UUID.
    pub organization_id: FieldId,
    /// Fiscal year.
    pub fiscal_year: FieldId,
    /// Requested approval amount.
    pub approved_amount: FieldId,
}

/// Stable input fields of the canonical `AllocateBudget` command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AllocateBudgetInputFields {
    /// Caller-supplied idempotency key.
    pub idempotency_key: FieldId,
    /// Organization UUID.
    pub organization_id: FieldId,
    /// Fiscal year.
    pub fiscal_year: FieldId,
    /// Matter UUID.
    pub matter_id: FieldId,
    /// Requested allocation amount.
    pub amount: FieldId,
}

/// Stable payload fields of every declared budget-command outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BudgetOutcomeFields {
    /// `BudgetCreated.budget`.
    pub budget_created_budget: FieldId,
    /// `BudgetAlreadyExists.organization_id`.
    pub budget_exists_organization_id: FieldId,
    /// `BudgetAlreadyExists.fiscal_year`.
    pub budget_exists_fiscal_year: FieldId,
    /// `InvalidApprovedAmount.minimum`.
    pub invalid_approved_minimum: FieldId,
    /// `BudgetNotFound.organization_id`.
    pub budget_not_found_organization_id: FieldId,
    /// `BudgetNotFound.fiscal_year`.
    pub budget_not_found_fiscal_year: FieldId,
    /// `InvalidAmount.minimum`.
    pub invalid_amount_minimum: FieldId,
    /// `InsufficientBudget.approved`.
    pub insufficient_approved: FieldId,
    /// `InsufficientBudget.allocated`.
    pub insufficient_allocated: FieldId,
    /// `InsufficientBudget.requested`.
    pub insufficient_requested: FieldId,
    /// `Allocated.budget`.
    pub allocated_budget: FieldId,
    /// `Allocated.remaining`.
    pub allocated_remaining: FieldId,
}

/// Generated-application binding from source names to stable semantic IDs.
///
/// WP-125 receives this value from its checked application fixture. The adapter
/// does not load a catalog, compile source, or guess storage identifiers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BudgetContractBinding {
    lineage: ContractLineage,
    version: ContractVersion,
    budget_entity: EntityTypeId,
    entity: BudgetEntityFields,
    create: CreateBudgetInputFields,
    allocate: AllocateBudgetInputFields,
    outcomes: BudgetOutcomeFields,
}

impl BudgetContractBinding {
    /// Joins one checked generated binding for the canonical budget contract.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub const fn new(
        lineage: ContractLineage,
        version: ContractVersion,
        budget_entity: EntityTypeId,
        entity: BudgetEntityFields,
        create: CreateBudgetInputFields,
        allocate: AllocateBudgetInputFields,
        outcomes: BudgetOutcomeFields,
    ) -> Self {
        Self {
            lineage,
            version,
            budget_entity,
            entity,
            create,
            allocate,
            outcomes,
        }
    }

    /// Borrows the exact contract lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Returns the exact application contract version.
    #[must_use]
    pub const fn version(&self) -> ContractVersion {
        self.version
    }

    /// Returns the stable `Budget` entity type ID.
    #[must_use]
    pub const fn budget_entity_type(&self) -> EntityTypeId {
        self.budget_entity
    }

    /// Returns the checked field IDs of the `Budget` entity.
    #[must_use]
    pub const fn entity_fields(&self) -> BudgetEntityFields {
        self.entity
    }

    /// Returns the checked payload field IDs of declared command outcomes.
    #[must_use]
    pub const fn outcome_fields(&self) -> BudgetOutcomeFields {
        self.outcomes
    }
}

/// Journal metadata retained in addition to the normalized workload outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceCommandObservation {
    /// Backend-neutral semantic observation used by the shared oracle.
    pub observation: CommandObservation,
    /// First execution versus same-outcome replay.
    pub completion: JournaledCompletion,
    /// Original nonzero application sequence.
    pub commit_sequence: CommitSequence,
    /// Original durable provenance identity.
    pub provenance_id: ProvenanceId,
    /// Acknowledged coordinator durability.
    pub durability: CommandDurability,
}

/// RiffDB component adapter that calls only API-neutral service traits.
#[derive(Clone)]
pub struct RiffDbServiceBudgetAdapter {
    service: Arc<dyn BudgetApplication>,
    contexts: Arc<dyn RequestContextFactory>,
    binding: BudgetContractBinding,
}

impl RiffDbServiceBudgetAdapter {
    /// Creates an application adapter over one already composed in-process service.
    #[must_use]
    pub fn new(
        service: Arc<dyn BudgetApplication>,
        contexts: Arc<dyn RequestContextFactory>,
        binding: BudgetContractBinding,
    ) -> Self {
        Self {
            service,
            contexts,
            binding,
        }
    }

    /// Executes one command and retains its durable replay metadata.
    pub fn execute_with_metadata(
        &self,
        operation: &BudgetOperation,
    ) -> Result<ServiceCommandObservation, RiffDbServiceAdapterError> {
        let request = self.command_request(operation)?;
        let result = block_on(
            self.service
                .execute_command(self.contexts.next_context(), request),
        )
        .map_err(RiffDbServiceAdapterError::Service)?;
        let ExecuteCommandResult::Journaled(result) = result else {
            return Err(RiffDbServiceAdapterError::UnexpectedServiceResult);
        };
        let outcome = decode_outcome(
            &self.binding,
            result.outcome().outcome_name().as_str(),
            result.outcome().value(),
        )?;
        Ok(ServiceCommandObservation {
            observation: CommandObservation {
                operation_id: operation.operation_id().clone(),
                outcome,
            },
            completion: result.completion(),
            commit_sequence: result.commit_sequence(),
            provenance_id: result.provenance_id(),
            durability: result.durability(),
        })
    }

    /// Runs two contenders released at one process-local barrier.
    pub fn run_contention(
        &self,
        workload: &ContentionWorkload,
    ) -> Result<ContentionObservation, RiffDbServiceAdapterError> {
        let seed = self.execute(&BudgetOperation::Create(workload.seed.clone()))?;
        if !matches!(seed.outcome, BudgetOutcome::BudgetCreated { .. }) {
            return Err(RiffDbServiceAdapterError::UnexpectedServiceResult);
        }

        let barrier = Arc::new(Barrier::new(2));
        let first = spawn_contender(
            self.clone(),
            workload.contenders[0].clone(),
            Arc::clone(&barrier),
        );
        let second = spawn_contender(self.clone(), workload.contenders[1].clone(), barrier);
        let mut outcomes = vec![join_contender(first)?, join_contender(second)?];
        outcomes.sort();
        Ok(ContentionObservation {
            case_id: workload.case_id.clone(),
            outcomes,
            final_budget: self.read_budget(workload.seed.key)?,
        })
    }

    fn command_request(
        &self,
        operation: &BudgetOperation,
    ) -> Result<ExecuteCommandRequest, RiffDbServiceAdapterError> {
        let (command, input) = match operation {
            BudgetOperation::Create(command) => (CREATE_BUDGET, self.create_input(command)?),
            BudgetOperation::Allocate(command) => (ALLOCATE_BUDGET, self.allocate_input(command)?),
        };
        ExecuteCommandRequest::new(
            SourceName::new(command).map_err(|_| RiffDbServiceAdapterError::InvalidBinding)?,
            Some(self.binding.version),
            SubmittedRecord::try_from(input)
                .map_err(|_| RiffDbServiceAdapterError::InvalidRequest)?,
        )
        .map_err(|_| RiffDbServiceAdapterError::InvalidRequest)
    }

    fn create_input(
        &self,
        command: &CreateBudget,
    ) -> Result<CanonicalRecord, RiffDbServiceAdapterError> {
        let fields = self.binding.create;
        CanonicalRecord::new(vec![
            (
                fields.idempotency_key,
                CanonicalValue::string(command.idempotency_key.as_str())
                    .map_err(|_| RiffDbServiceAdapterError::InvalidRequest)?,
            ),
            (
                fields.organization_id,
                CanonicalValue::Uuid(command.key.organization_id.as_bytes()),
            ),
            (
                fields.fiscal_year,
                CanonicalValue::I64(command.key.fiscal_year),
            ),
            (
                fields.approved_amount,
                CanonicalValue::Decimal(
                    command
                        .approved_amount
                        .canonical_decimal()
                        .map_err(|_| RiffDbServiceAdapterError::InvalidRequest)?,
                ),
            ),
        ])
        .map_err(|_| RiffDbServiceAdapterError::InvalidRequest)
    }

    fn allocate_input(
        &self,
        command: &AllocateBudget,
    ) -> Result<CanonicalRecord, RiffDbServiceAdapterError> {
        let fields = self.binding.allocate;
        CanonicalRecord::new(vec![
            (
                fields.idempotency_key,
                CanonicalValue::string(command.idempotency_key.as_str())
                    .map_err(|_| RiffDbServiceAdapterError::InvalidRequest)?,
            ),
            (
                fields.organization_id,
                CanonicalValue::Uuid(command.key.organization_id.as_bytes()),
            ),
            (
                fields.fiscal_year,
                CanonicalValue::I64(command.key.fiscal_year),
            ),
            (
                fields.matter_id,
                CanonicalValue::Uuid(command.matter_id.as_bytes()),
            ),
            (
                fields.amount,
                CanonicalValue::Decimal(
                    command
                        .amount
                        .canonical_decimal()
                        .map_err(|_| RiffDbServiceAdapterError::InvalidRequest)?,
                ),
            ),
        ])
        .map_err(|_| RiffDbServiceAdapterError::InvalidRequest)
    }

    fn entity_request(
        &self,
        key: BudgetKey,
    ) -> Result<GetEntityRequest, RiffDbServiceAdapterError> {
        let mut entity_key = EntityKeyBuilder::new(self.binding.budget_entity);
        entity_key
            .push_uuid(&key.organization_id.as_bytes())
            .and_then(|builder| builder.push_i64(key.fiscal_year))
            .map_err(|_| RiffDbServiceAdapterError::InvalidRequest)?;
        let key = entity_key
            .finish()
            .map_err(|_| RiffDbServiceAdapterError::InvalidRequest)?;
        let fields = FieldSelection::new(vec![
            self.binding.entity.approved_amount,
            self.binding.entity.allocated_amount,
            self.binding.entity.updated_at,
        ])
        .map_err(|_| RiffDbServiceAdapterError::InvalidBinding)?;
        GetEntityRequest::new(
            ContractSelection::Exact {
                lineage: self.binding.lineage.clone(),
                version: self.binding.version,
            },
            self.binding.budget_entity,
            key,
            fields,
        )
        .map_err(|_| RiffDbServiceAdapterError::InvalidRequest)
    }
}

impl fmt::Debug for RiffDbServiceBudgetAdapter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RiffDbServiceBudgetAdapter([SERVICE])")
    }
}

impl BudgetBackend for RiffDbServiceBudgetAdapter {
    type Error = RiffDbServiceAdapterError;

    fn execute(&self, operation: &BudgetOperation) -> Result<CommandObservation, Self::Error> {
        self.execute_with_metadata(operation)
            .map(|observation| observation.observation)
    }

    fn read_budget(&self, key: BudgetKey) -> Result<Option<BudgetState>, Self::Error> {
        let request = self.entity_request(key)?;
        let result = block_on(
            self.service
                .get_entity(self.contexts.next_context(), request),
        )
        .map_err(RiffDbServiceAdapterError::Service)?;
        match result {
            GetEntityResult::NotFound => Ok(None),
            GetEntityResult::Found(entity) => {
                timestamp_field(entity.fields(), self.binding.entity.updated_at)?;
                Ok(Some(BudgetState {
                    key,
                    approved_amount: budget_decimal_field(
                        entity.fields(),
                        self.binding.entity.approved_amount,
                    )?,
                    allocated_amount: budget_decimal_field(
                        entity.fields(),
                        self.binding.entity.allocated_amount,
                    )?,
                }))
            }
        }
    }
}

/// Returns the explicit component-mode guarantee profile exercised by WP-125.
#[must_use]
pub const fn riffdb_service_guarantee_profile() -> GuaranteeProfile {
    GuaranteeProfile {
        adapter: "riffdb-api-neutral-service-v1",
        isolation: "compiled conflict ownership plus transaction-current dependency validation",
        durability: "coordinator-reported synchronous durability in the WP-125 preflight",
        conflict_control: "canonical RiffDB conflict capabilities for AnnualBudget",
        command_invariants: GuaranteeLevel::Matched,
        idempotency: GuaranteeLevel::Matched,
        durable_events: GuaranteeLevel::Partial,
        provenance: GuaranteeLevel::Matched,
        outbox: GuaranteeLevel::Partial,
        projections: GuaranteeLevel::Unsupported,
        authorization: GuaranteeLevel::Matched,
        timestamp_observation: "deterministic command logical time is required but normalized out",
        assumptions: &[
            "Every operation uses a fresh authenticated in-process comparison RequestContext.",
            "Commands and entity reads call only the API-neutral application service.",
            "The WP-125 harness mirrors service-returned entity snapshots because production redb authoritative-read composition belongs to WP-130; WP-135 replaces the component read provider with public process reads.",
            "Durable-event and outbox claims are inherited from the coordinator atomic record graph; WP-125 does not expose their read surfaces.",
            "Projection-frontier comparison remains deferred to the later public process runner.",
            "Correctness preflight must pass before any timing result is reported.",
        ],
    }
}

fn spawn_contender(
    adapter: RiffDbServiceBudgetAdapter,
    command: AllocateBudget,
    barrier: Arc<Barrier>,
) -> thread::JoinHandle<Result<BudgetOutcome, RiffDbServiceAdapterError>> {
    thread::spawn(move || {
        barrier.wait();
        adapter
            .execute(&BudgetOperation::Allocate(command))
            .map(|observation| observation.outcome)
    })
}

fn join_contender(
    handle: thread::JoinHandle<Result<BudgetOutcome, RiffDbServiceAdapterError>>,
) -> Result<BudgetOutcome, RiffDbServiceAdapterError> {
    handle
        .join()
        .map_err(|_| RiffDbServiceAdapterError::WorkerPanicked)?
}

fn decode_outcome(
    binding: &BudgetContractBinding,
    name: &str,
    record: &CanonicalRecord,
) -> Result<BudgetOutcome, RiffDbServiceAdapterError> {
    let fields = binding.outcomes;
    match name {
        "BudgetCreated" => Ok(BudgetOutcome::BudgetCreated {
            budget: budget_field(record, fields.budget_created_budget, binding.entity)?,
        }),
        "BudgetAlreadyExists" => Ok(BudgetOutcome::BudgetAlreadyExists {
            key: key_fields(
                record,
                fields.budget_exists_organization_id,
                fields.budget_exists_fiscal_year,
            )?,
        }),
        "InvalidApprovedAmount" => Ok(BudgetOutcome::InvalidApprovedAmount {
            minimum: minimum_decimal_field(record, fields.invalid_approved_minimum)?,
        }),
        "BudgetNotFound" => Ok(BudgetOutcome::BudgetNotFound {
            key: key_fields(
                record,
                fields.budget_not_found_organization_id,
                fields.budget_not_found_fiscal_year,
            )?,
        }),
        "InvalidAmount" => Ok(BudgetOutcome::InvalidAmount {
            minimum: minimum_decimal_field(record, fields.invalid_amount_minimum)?,
        }),
        "InsufficientBudget" => Ok(BudgetOutcome::InsufficientBudget {
            approved: budget_decimal_field(record, fields.insufficient_approved)?,
            allocated: budget_decimal_field(record, fields.insufficient_allocated)?,
            requested: budget_decimal_field(record, fields.insufficient_requested)?,
        }),
        "Allocated" => Ok(BudgetOutcome::Allocated {
            budget: budget_field(record, fields.allocated_budget, binding.entity)?,
            remaining: budget_decimal_field(record, fields.allocated_remaining)?,
        }),
        _ => Err(RiffDbServiceAdapterError::UnknownOutcome),
    }
}

fn budget_field(
    record: &CanonicalRecord,
    field: FieldId,
    entity: BudgetEntityFields,
) -> Result<BudgetState, RiffDbServiceAdapterError> {
    let CanonicalValue::Record(budget) = record_field(record, field)? else {
        return Err(RiffDbServiceAdapterError::InvalidServiceValue);
    };
    timestamp_field(budget, entity.updated_at)?;
    Ok(BudgetState {
        key: key_fields(budget, entity.organization_id, entity.fiscal_year)?,
        approved_amount: budget_decimal_field(budget, entity.approved_amount)?,
        allocated_amount: budget_decimal_field(budget, entity.allocated_amount)?,
    })
}

fn key_fields(
    record: &CanonicalRecord,
    organization: FieldId,
    fiscal_year: FieldId,
) -> Result<BudgetKey, RiffDbServiceAdapterError> {
    let CanonicalValue::Uuid(organization) = record_field(record, organization)? else {
        return Err(RiffDbServiceAdapterError::InvalidServiceValue);
    };
    let CanonicalValue::I64(fiscal_year) = record_field(record, fiscal_year)? else {
        return Err(RiffDbServiceAdapterError::InvalidServiceValue);
    };
    Ok(BudgetKey {
        organization_id: OrganizationId::from_bytes(*organization),
        fiscal_year: *fiscal_year,
    })
}

fn budget_decimal_field(
    record: &CanonicalRecord,
    field: FieldId,
) -> Result<Amount, RiffDbServiceAdapterError> {
    decimal_field(record, field, 28, 2)
}

fn minimum_decimal_field(
    record: &CanonicalRecord,
    field: FieldId,
) -> Result<Amount, RiffDbServiceAdapterError> {
    decimal_field(record, field, 2, 2)
}

fn decimal_field(
    record: &CanonicalRecord,
    field: FieldId,
    precision: u8,
    scale: u8,
) -> Result<Amount, RiffDbServiceAdapterError> {
    let CanonicalValue::Decimal(decimal) = record_field(record, field)? else {
        return Err(RiffDbServiceAdapterError::InvalidServiceValue);
    };
    let expected = DecimalSpec::new(precision, scale)
        .map_err(|_| RiffDbServiceAdapterError::InvalidServiceValue)?;
    if decimal.spec() != expected {
        return Err(RiffDbServiceAdapterError::InvalidServiceValue);
    }
    Amount::from_minor_units(decimal.coefficient())
        .map_err(|_| RiffDbServiceAdapterError::InvalidServiceValue)
}

fn timestamp_field(
    record: &CanonicalRecord,
    field: FieldId,
) -> Result<(), RiffDbServiceAdapterError> {
    if matches!(record_field(record, field)?, CanonicalValue::Timestamp(_)) {
        Ok(())
    } else {
        Err(RiffDbServiceAdapterError::InvalidServiceValue)
    }
}

fn record_field(
    record: &CanonicalRecord,
    field: FieldId,
) -> Result<&CanonicalValue, RiffDbServiceAdapterError> {
    record
        .fields()
        .binary_search_by_key(&field, |(candidate, _)| *candidate)
        .ok()
        .and_then(|index| record.fields().get(index))
        .map(|(_, value)| value)
        .ok_or(RiffDbServiceAdapterError::InvalidServiceValue)
}

fn block_on<F: Future>(future: F) -> F::Output {
    struct ThreadWake(thread::Thread);

    impl Wake for ThreadWake {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => thread::park(),
        }
    }
}

/// A redacted comparison-adapter failure category.
pub enum RiffDbServiceAdapterError {
    /// A checked application binding was incomplete or inconsistent.
    InvalidBinding,
    /// Workload input could not be represented exactly as a service request.
    InvalidRequest,
    /// The API-neutral service returned a typed failure.
    Service(ServiceFailure),
    /// A mutating budget command returned a read-only or otherwise wrong result class.
    UnexpectedServiceResult,
    /// The checked bundle returned an undeclared budget outcome name.
    UnknownOutcome,
    /// A service result did not match its checked canonical value shape.
    InvalidServiceValue,
    /// A contention worker panicked.
    WorkerPanicked,
}

impl RiffDbServiceAdapterError {
    /// Borrows the underlying safe service failure when this is a service error.
    #[must_use]
    pub const fn service_failure(&self) -> Option<&ServiceFailure> {
        match self {
            Self::Service(failure) => Some(failure),
            Self::InvalidBinding
            | Self::InvalidRequest
            | Self::UnexpectedServiceResult
            | Self::UnknownOutcome
            | Self::InvalidServiceValue
            | Self::WorkerPanicked => None,
        }
    }
}

impl fmt::Debug for RiffDbServiceAdapterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidBinding => "RiffDbServiceAdapterError::InvalidBinding",
            Self::InvalidRequest => "RiffDbServiceAdapterError::InvalidRequest",
            Self::Service(_) => "RiffDbServiceAdapterError::Service([REDACTED])",
            Self::UnexpectedServiceResult => "RiffDbServiceAdapterError::UnexpectedServiceResult",
            Self::UnknownOutcome => "RiffDbServiceAdapterError::UnknownOutcome",
            Self::InvalidServiceValue => "RiffDbServiceAdapterError::InvalidServiceValue",
            Self::WorkerPanicked => "RiffDbServiceAdapterError::WorkerPanicked",
        })
    }
}

impl fmt::Display for RiffDbServiceAdapterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidBinding => "budget contract binding is invalid",
            Self::InvalidRequest => "budget request cannot be represented exactly",
            Self::Service(_) => "RiffDB application-service operation failed",
            Self::UnexpectedServiceResult => "RiffDB returned an unexpected command result class",
            Self::UnknownOutcome => "RiffDB returned an unknown declared budget outcome",
            Self::InvalidServiceValue => "RiffDB returned an invalid budget value shape",
            Self::WorkerPanicked => "RiffDB contention worker panicked",
        })
    }
}

impl Error for RiffDbServiceAdapterError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.service_failure()
            .map(|failure| failure as &(dyn Error + 'static))
    }
}

#[cfg(test)]
mod tests {
    use riffdb_types::{CanonicalRecord, CanonicalValue, Decimal, DecimalSpec, FieldId, Timestamp};

    use super::{RiffDbServiceAdapterError, decimal_field, timestamp_field};

    #[test]
    fn service_comparison_rejects_wrong_decimal_spec_and_missing_timestamp() {
        let amount = FieldId::first();
        let updated_at = FieldId::new(2).expect("timestamp field ID");
        let wrong_spec = DecimalSpec::new(28, 3).expect("wrong decimal spec");
        let wrong_decimal = CanonicalRecord::new(vec![
            (
                amount,
                CanonicalValue::Decimal(Decimal::new(wrong_spec, 1_000).expect("decimal")),
            ),
            (
                updated_at,
                CanonicalValue::Timestamp(Timestamp::new(1, 0).expect("timestamp")),
            ),
        ])
        .expect("canonical record");
        assert!(matches!(
            decimal_field(&wrong_decimal, amount, 28, 2),
            Err(RiffDbServiceAdapterError::InvalidServiceValue)
        ));

        let missing_timestamp = CanonicalRecord::new(vec![(
            amount,
            CanonicalValue::Decimal(
                Decimal::new(DecimalSpec::new(28, 2).expect("spec"), 1_000).expect("decimal"),
            ),
        )])
        .expect("canonical record");
        assert!(matches!(
            timestamp_field(&missing_timestamp, updated_at),
            Err(RiffDbServiceAdapterError::InvalidServiceValue)
        ));
    }
}
