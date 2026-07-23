//! Public-gRPC RiffDB adapter for the shared budget comparison workload.

#![forbid(unsafe_code)]

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use riffdb_budget_comparison_core::{
    AllocateBudget as WorkloadAllocateBudget, Amount as WorkloadAmount, BudgetKey, BudgetOperation,
    BudgetOutcome, BudgetState, CommandObservation, ContentionObservation, ContentionWorkload,
    CreateBudget as WorkloadCreateBudget, OracleMismatch, ReferenceModel, SequentialObservation,
    SequentialWorkload, canonical_workload, evaluate_sequential, expected_contention_observation,
    verify_contention, verify_sequential,
};
use riffdb_client_rust::generated::legal_spend::{
    ALLOCATE_BUDGET_PLAN_HASH, AllocateBudget, AllocateBudgetOutcome, Amount, Budget,
    CONTRACT_LINEAGE, CONTRACT_VERSION, CREATE_BUDGET_PLAN_HASH, CreateBudget, CreateBudgetOutcome,
};
use riffdb_client_rust::generated::{GeneratedCommand, GeneratedCommandError};
use riffdb_client_rust::{
    AttemptBudget, BearerCredential, CallMetadata, ClientError, GeneratedExecution,
    GeneratedExecutionError, RiffDbClient, generate_request_id,
};
use riffdb_proto::{decimal_from_proto, v1};
use riffdb_types::{CommitSequence, ContractVersion, DecimalSpec, EntityKeyBuilder, EntityTypeId};
use tonic::transport::Endpoint;

const BUDGET_ENTITY_TYPE_ID: u32 = 1;
const BUDGET_UPDATED_AT_FIELD_ID: u32 = 1;
const BUDGET_APPROVED_AMOUNT_FIELD_ID: u32 = 3;
const BUDGET_ALLOCATED_AMOUNT_FIELD_ID: u32 = 5;
const CREATE_BUDGET_COMMAND_ID: u32 = 1;
const SUBSCRIPTION_LIFETIME_NANOS: u64 = 30_000_000_000;

/// A closed comparison case understood by the public evidence runner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicComparisonCase {
    /// Ordered command outcomes and final entity snapshots.
    Sequential,
    /// Two allocations released from one explicit start barrier.
    Contention,
    /// Discarded first response followed by same-key replay.
    SameKeyReplay,
}

impl PublicComparisonCase {
    /// Returns the stable child-process protocol spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Sequential => "sequential",
            Self::Contention => "contention",
            Self::SameKeyReplay => "same_key_replay",
        }
    }
}

/// The checked completion class retained from a public command response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicCommandCompletion {
    /// The submission created the durable journal entry.
    Committed,
    /// The submission resolved an existing equal idempotency identity.
    Replayed,
}

/// Public metadata retained alongside a normalized command outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicCommandMetadata {
    /// Whether this submission committed or replayed the journal entry.
    pub completion: PublicCommandCompletion,
    /// Original nonzero application commit sequence.
    pub commit_sequence: CommitSequence,
    /// Exact active contract version used by the command.
    pub contract_version: ContractVersion,
    /// Checked compiler plan identity.
    pub plan_hash: [u8; 32],
    /// Canonical public provenance resource locator.
    pub provenance_uri: String,
    /// Canonical public outcome resource locator.
    pub outcome_uri: String,
}

/// One backend-neutral outcome plus its checked public journal metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicCommandObservation {
    /// Shared comparison-oracle observation.
    pub observation: CommandObservation,
    /// Public response metadata needed to prove replay identity.
    pub metadata: PublicCommandMetadata,
}

/// Evidence that an ignored first response resolved to one notified commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SameKeyReplayEvidence {
    /// Outcome decoded from the second, replayed submission.
    pub observation: CommandObservation,
    /// Sequence published by the commit-notification stream.
    pub notified_commit_sequence: CommitSequence,
    /// Metadata returned by the second, same-key submission.
    pub replay_metadata: PublicCommandMetadata,
}

/// Checked evidence returned by one complete public comparison case.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PublicCaseEvidence {
    /// Normalized sequential observation equal to the reference model.
    Sequential(SequentialObservation),
    /// Normalized contention observation equal to both legal serializations.
    Contention(ContentionObservation),
    /// Commit-notification and replay-identity evidence.
    SameKeyReplay(SameKeyReplayEvidence),
}

/// RiffDB comparison adapter using only the public Rust client and wire types.
#[derive(Clone)]
pub struct RiffDbPublicBudgetAdapter {
    client: RiffDbClient,
    metadata: CallMetadata,
}

impl RiffDbPublicBudgetAdapter {
    /// Connects to one already bootstrapped, Budget-v1-ready public endpoint.
    pub async fn connect(
        endpoint: Endpoint,
        credential: BearerCredential,
    ) -> Result<Self, RiffDbPublicAdapterError> {
        let client = RiffDbClient::connect(endpoint)
            .await
            .map_err(|_| RiffDbPublicAdapterError::ConnectionFailed)?;
        Ok(Self {
            client,
            metadata: CallMetadata::authenticated(credential),
        })
    }

    /// Runs one canonical case and performs its complete semantic preflight.
    pub async fn run_case(
        &mut self,
        case: PublicComparisonCase,
    ) -> Result<PublicCaseEvidence, RiffDbPublicAdapterError> {
        match case {
            PublicComparisonCase::Sequential => self
                .observe_sequential()
                .await
                .map(PublicCaseEvidence::Sequential),
            PublicComparisonCase::Contention => self
                .observe_contention()
                .await
                .map(PublicCaseEvidence::Contention),
            PublicComparisonCase::SameKeyReplay => self
                .prove_same_key_replay()
                .await
                .map(PublicCaseEvidence::SameKeyReplay),
        }
    }

    /// Executes and verifies the canonical ordered workload over public gRPC.
    pub async fn observe_sequential(
        &mut self,
    ) -> Result<SequentialObservation, RiffDbPublicAdapterError> {
        let workload = canonical_workload();
        let actual = self
            .observe_sequential_workload(&workload.sequential)
            .await?;
        let expected = evaluate_sequential(&workload.sequential)
            .map_err(|_| RiffDbPublicAdapterError::InvalidWorkload)?;
        verify_sequential(&expected, &actual).map_err(map_oracle_mismatch)?;
        Ok(actual)
    }

    /// Executes and verifies the canonical two-contender workload.
    pub async fn observe_contention(
        &mut self,
    ) -> Result<ContentionObservation, RiffDbPublicAdapterError> {
        let workload = canonical_workload();
        let actual = self
            .observe_contention_workload(&workload.contention)
            .await?;
        let expected = expected_contention_observation(&workload.contention)
            .map_err(|_| RiffDbPublicAdapterError::InvalidWorkload)?;
        verify_contention(&expected, &actual).map_err(map_oracle_mismatch)?;
        Ok(actual)
    }

    /// Proves same-key resolution after deliberately ignoring the first response.
    pub async fn prove_same_key_replay(
        &mut self,
    ) -> Result<SameKeyReplayEvidence, RiffDbPublicAdapterError> {
        let workload = canonical_workload();
        let command = workload.contention.seed;
        let generated = generated_create(&command)?;

        let mut subscription_client = self.client.clone();
        let mut subscription = subscription_client
            .subscribe_commits(
                v1::SubscribeCommitsRequest {
                    request_id: fresh_request_id_bytes()?,
                    after_sequence: None,
                    maximum_lifetime_nanos: SUBSCRIPTION_LIFETIME_NANOS,
                },
                &self.metadata,
            )
            .await
            .map_err(map_client_error)?;

        let immutable = generated
            .idempotent_command()
            .map_err(map_generated_command_error)?;
        let ignored_response = self
            .client
            .execute_with_retry(&immutable, one_attempt(), &self.metadata)
            .await
            .map_err(map_client_error)?;
        drop(ignored_response);

        let notified_commit = next_matching_create_commit(&mut subscription).await?;
        let notified_sequence = CommitSequence::new(notified_commit.commit_sequence)
            .ok_or(RiffDbPublicAdapterError::InvalidResponse)?;

        let replay_execution = self
            .client
            .execute_generated(&generated, one_attempt(), &self.metadata)
            .await
            .map_err(map_generated_execution_error)?;
        let replay_outcome = *replay_execution.outcome();
        let replay = map_create_execution(&command, replay_execution)?;
        let notified_outcome = decode_notified_create_outcome(&generated, &notified_commit)?;
        let expected = ReferenceModel::new()
            .execute(&BudgetOperation::Create(command.clone()))
            .map_err(|_| RiffDbPublicAdapterError::InvalidWorkload)?;
        if replay.metadata.completion != PublicCommandCompletion::Replayed
            || replay.metadata.commit_sequence != notified_sequence
            || replay.metadata.contract_version.get() != notified_commit.contract_version
            || replay.metadata.plan_hash.as_slice() != notified_commit.plan_hash
            || replay.metadata.provenance_uri != notified_commit.provenance_uri
            || notified_commit.durability != v1::CommandDurability::Synchronous as i32
            || notified_outcome != replay_outcome
            || replay.observation != expected
        {
            return Err(RiffDbPublicAdapterError::ReplayEvidenceMismatch);
        }

        let final_budget = self.read_budget(command.key).await?;
        let BudgetOutcome::BudgetCreated { budget } = &replay.observation.outcome else {
            return Err(RiffDbPublicAdapterError::ReplayEvidenceMismatch);
        };
        if final_budget.as_ref() != Some(budget) {
            return Err(RiffDbPublicAdapterError::ReplayEvidenceMismatch);
        }

        Ok(SameKeyReplayEvidence {
            observation: replay.observation,
            notified_commit_sequence: notified_sequence,
            replay_metadata: replay.metadata,
        })
    }

    async fn observe_sequential_workload(
        &mut self,
        workload: &SequentialWorkload,
    ) -> Result<SequentialObservation, RiffDbPublicAdapterError> {
        let mut keys = BTreeSet::new();
        let mut outcomes = Vec::with_capacity(workload.operations.len());
        for operation in &workload.operations {
            keys.insert(operation.key());
            let execution = execute_operation(&mut self.client, &self.metadata, operation).await?;
            if execution.metadata.completion != PublicCommandCompletion::Committed {
                return Err(RiffDbPublicAdapterError::FreshDatabaseRequired);
            }
            outcomes.push(execution.observation);
        }

        let mut final_budgets = Vec::new();
        for key in keys {
            if let Some(budget) = self.read_budget(key).await? {
                final_budgets.push(budget);
            }
        }
        final_budgets.sort();
        Ok(SequentialObservation {
            case_id: workload.case_id.clone(),
            outcomes,
            final_budgets,
        })
    }

    async fn observe_contention_workload(
        &mut self,
        workload: &ContentionWorkload,
    ) -> Result<ContentionObservation, RiffDbPublicAdapterError> {
        let seed = execute_operation(
            &mut self.client,
            &self.metadata,
            &BudgetOperation::Create(workload.seed.clone()),
        )
        .await?;
        if seed.metadata.completion != PublicCommandCompletion::Committed {
            return Err(RiffDbPublicAdapterError::FreshDatabaseRequired);
        }
        if !matches!(
            seed.observation.outcome,
            BudgetOutcome::BudgetCreated { .. }
        ) {
            return Err(RiffDbPublicAdapterError::InvalidResponse);
        }

        let barrier = AsyncStartBarrier::new(2);
        let first_barrier = barrier.clone();
        let second_barrier = barrier;
        let mut first_client = self.client.clone();
        let mut second_client = self.client.clone();
        let metadata = self.metadata.clone();
        let first_metadata = metadata.clone();
        let first_operation = BudgetOperation::Allocate(workload.contenders[0].clone());
        let second_operation = BudgetOperation::Allocate(workload.contenders[1].clone());

        let first = async move {
            first_barrier.wait().await?;
            execute_operation(&mut first_client, &first_metadata, &first_operation).await
        };
        let second = async move {
            second_barrier.wait().await?;
            execute_operation(&mut second_client, &metadata, &second_operation).await
        };
        let (first, second) = tokio::join!(first, second);
        let first = first?;
        let second = second?;
        if first.metadata.completion != PublicCommandCompletion::Committed
            || second.metadata.completion != PublicCommandCompletion::Committed
        {
            return Err(RiffDbPublicAdapterError::FreshDatabaseRequired);
        }
        let mut outcomes = vec![first.observation.outcome, second.observation.outcome];
        outcomes.sort();

        Ok(ContentionObservation {
            case_id: workload.case_id.clone(),
            outcomes,
            final_budget: self.read_budget(workload.seed.key).await?,
        })
    }

    async fn read_budget(
        &mut self,
        key: BudgetKey,
    ) -> Result<Option<BudgetState>, RiffDbPublicAdapterError> {
        let entity_key = budget_entity_key(key)?;
        let response = self
            .client
            .get_entity(
                v1::GetEntityRequest {
                    request_id: fresh_request_id_bytes()?,
                    contract: Some(v1::ContractSelection {
                        selection: Some(v1::contract_selection::Selection::Exact(
                            v1::ExactContractSelection {
                                contract_lineage: CONTRACT_LINEAGE.to_owned(),
                                contract_version: CONTRACT_VERSION,
                            },
                        )),
                    }),
                    entity_type_id: BUDGET_ENTITY_TYPE_ID,
                    entity_key: entity_key.clone(),
                    fields: Some(v1::FieldSelection {
                        field_ids: vec![
                            BUDGET_UPDATED_AT_FIELD_ID,
                            BUDGET_APPROVED_AMOUNT_FIELD_ID,
                            BUDGET_ALLOCATED_AMOUNT_FIELD_ID,
                        ],
                    }),
                },
                &self.metadata,
            )
            .await
            .map_err(map_client_error)?;
        match response.result {
            Some(v1::get_entity_response::Result::NotFound(_)) => Ok(None),
            Some(v1::get_entity_response::Result::Found(entity)) => {
                decode_entity(key, &entity_key, entity).map(Some)
            }
            None => Err(RiffDbPublicAdapterError::InvalidResponse),
        }
    }
}

impl fmt::Debug for RiffDbPublicBudgetAdapter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RiffDbPublicBudgetAdapter([PUBLIC_CLIENT])")
    }
}

async fn execute_operation(
    client: &mut RiffDbClient,
    metadata: &CallMetadata,
    operation: &BudgetOperation,
) -> Result<PublicCommandObservation, RiffDbPublicAdapterError> {
    match operation {
        BudgetOperation::Create(command) => {
            let generated = generated_create(command)?;
            let execution = client
                .execute_generated(&generated, one_attempt(), metadata)
                .await
                .map_err(map_generated_execution_error)?;
            map_create_execution(command, execution)
        }
        BudgetOperation::Allocate(command) => {
            let generated = generated_allocate(command)?;
            let execution = client
                .execute_generated(&generated, one_attempt(), metadata)
                .await
                .map_err(map_generated_execution_error)?;
            map_allocate_execution(command, execution)
        }
    }
}

fn generated_create(
    command: &WorkloadCreateBudget,
) -> Result<CreateBudget, RiffDbPublicAdapterError> {
    Ok(CreateBudget {
        idempotency_key: command.idempotency_key.as_str().to_owned(),
        organization_id: command.key.organization_id.as_bytes(),
        fiscal_year: command.key.fiscal_year,
        approved_amount: generated_amount(command.approved_amount)?,
    })
}

fn generated_allocate(
    command: &WorkloadAllocateBudget,
) -> Result<AllocateBudget, RiffDbPublicAdapterError> {
    Ok(AllocateBudget {
        idempotency_key: command.idempotency_key.as_str().to_owned(),
        organization_id: command.key.organization_id.as_bytes(),
        fiscal_year: command.key.fiscal_year,
        matter_id: command.matter_id.as_bytes(),
        amount: generated_amount(command.amount)?,
    })
}

fn generated_amount(value: WorkloadAmount) -> Result<Amount, RiffDbPublicAdapterError> {
    Amount::from_minor_units(value.minor_units()).ok_or(RiffDbPublicAdapterError::InvalidWorkload)
}

fn map_create_execution(
    command: &WorkloadCreateBudget,
    execution: GeneratedExecution<CreateBudgetOutcome>,
) -> Result<PublicCommandObservation, RiffDbPublicAdapterError> {
    let (outcome, response) = execution.into_parts();
    let outcome = match outcome {
        CreateBudgetOutcome::BudgetCreated { budget } => BudgetOutcome::BudgetCreated {
            budget: map_budget(budget)?,
        },
        CreateBudgetOutcome::BudgetAlreadyExists {
            organization_id,
            fiscal_year,
        } => BudgetOutcome::BudgetAlreadyExists {
            key: BudgetKey {
                organization_id: riffdb_budget_comparison_core::OrganizationId::from_bytes(
                    organization_id,
                ),
                fiscal_year,
            },
        },
        CreateBudgetOutcome::InvalidApprovedAmount { minimum } => {
            BudgetOutcome::InvalidApprovedAmount {
                minimum: workload_amount(minimum)?,
            }
        }
    };
    Ok(PublicCommandObservation {
        observation: CommandObservation {
            operation_id: command.operation_id.clone(),
            outcome,
        },
        metadata: response_metadata(response, &CREATE_BUDGET_PLAN_HASH)?,
    })
}

fn map_allocate_execution(
    command: &WorkloadAllocateBudget,
    execution: GeneratedExecution<AllocateBudgetOutcome>,
) -> Result<PublicCommandObservation, RiffDbPublicAdapterError> {
    let (outcome, response) = execution.into_parts();
    let outcome = match outcome {
        AllocateBudgetOutcome::Allocated { budget, remaining } => BudgetOutcome::Allocated {
            budget: map_budget(budget)?,
            remaining: workload_amount(remaining)?,
        },
        AllocateBudgetOutcome::InvalidAmount { minimum } => BudgetOutcome::InvalidAmount {
            minimum: workload_amount(minimum)?,
        },
        AllocateBudgetOutcome::BudgetNotFound {
            organization_id,
            fiscal_year,
        } => BudgetOutcome::BudgetNotFound {
            key: BudgetKey {
                organization_id: riffdb_budget_comparison_core::OrganizationId::from_bytes(
                    organization_id,
                ),
                fiscal_year,
            },
        },
        AllocateBudgetOutcome::InsufficientBudget {
            approved,
            allocated,
            requested,
        } => BudgetOutcome::InsufficientBudget {
            approved: workload_amount(approved)?,
            allocated: workload_amount(allocated)?,
            requested: workload_amount(requested)?,
        },
    };
    Ok(PublicCommandObservation {
        observation: CommandObservation {
            operation_id: command.operation_id.clone(),
            outcome,
        },
        metadata: response_metadata(response, &ALLOCATE_BUDGET_PLAN_HASH)?,
    })
}

fn map_budget(budget: Budget) -> Result<BudgetState, RiffDbPublicAdapterError> {
    let _checked_logical_time = budget.updated_at;
    Ok(BudgetState {
        key: BudgetKey {
            organization_id: riffdb_budget_comparison_core::OrganizationId::from_bytes(
                budget.organization_id,
            ),
            fiscal_year: budget.fiscal_year,
        },
        approved_amount: workload_amount(budget.approved_amount)?,
        allocated_amount: workload_amount(budget.allocated_amount)?,
    })
}

fn workload_amount(value: Amount) -> Result<WorkloadAmount, RiffDbPublicAdapterError> {
    WorkloadAmount::from_minor_units(value.minor_units())
        .map_err(|_| RiffDbPublicAdapterError::InvalidResponse)
}

fn response_metadata(
    response: v1::ExecuteCommandResponse,
    expected_plan_hash: &[u8; 32],
) -> Result<PublicCommandMetadata, RiffDbPublicAdapterError> {
    let completion = match v1::execute_command_response::CompletionStatus::try_from(response.status)
    {
        Ok(v1::execute_command_response::CompletionStatus::Committed) => {
            PublicCommandCompletion::Committed
        }
        Ok(v1::execute_command_response::CompletionStatus::Replayed) => {
            PublicCommandCompletion::Replayed
        }
        _ => return Err(RiffDbPublicAdapterError::InvalidResponse),
    };
    let commit_sequence = CommitSequence::new(response.commit_sequence)
        .ok_or(RiffDbPublicAdapterError::InvalidResponse)?;
    let contract_version = ContractVersion::new(response.contract_version)
        .ok_or(RiffDbPublicAdapterError::InvalidResponse)?;
    let plan_hash: [u8; 32] = response
        .plan_hash
        .try_into()
        .map_err(|_| RiffDbPublicAdapterError::InvalidResponse)?;
    let outcome_uri = response
        .outcome_uri
        .ok_or(RiffDbPublicAdapterError::InvalidResponse)?;
    if contract_version.get() != CONTRACT_VERSION
        || &plan_hash != expected_plan_hash
        || response.durability_mode != "sync"
        || response.provenance_uri.is_empty()
        || outcome_uri.is_empty()
    {
        return Err(RiffDbPublicAdapterError::InvalidResponse);
    }
    Ok(PublicCommandMetadata {
        completion,
        commit_sequence,
        contract_version,
        plan_hash,
        provenance_uri: response.provenance_uri,
        outcome_uri,
    })
}

fn budget_entity_key(key: BudgetKey) -> Result<Vec<u8>, RiffDbPublicAdapterError> {
    let entity_type = EntityTypeId::new(BUDGET_ENTITY_TYPE_ID)
        .ok_or(RiffDbPublicAdapterError::InvalidWorkload)?;
    let mut builder = EntityKeyBuilder::new(entity_type);
    builder
        .push_uuid(&key.organization_id.as_bytes())
        .and_then(|builder| builder.push_i64(key.fiscal_year))
        .map_err(|_| RiffDbPublicAdapterError::InvalidWorkload)?;
    builder
        .finish()
        .map(|key| key.into_bytes())
        .map_err(|_| RiffDbPublicAdapterError::InvalidWorkload)
}

fn decode_entity(
    key: BudgetKey,
    expected_key: &[u8],
    entity: v1::Entity,
) -> Result<BudgetState, RiffDbPublicAdapterError> {
    if entity.entity_key != expected_key
        || entity.entity_version == 0
        || entity.written_by_contract_version != CONTRACT_VERSION
    {
        return Err(RiffDbPublicAdapterError::InvalidResponse);
    }
    let fields = entity
        .fields
        .as_ref()
        .ok_or(RiffDbPublicAdapterError::InvalidResponse)?;
    let ids = fields
        .fields
        .iter()
        .map(|field| field.field_id)
        .collect::<Vec<_>>();
    if ids
        != [
            Some(BUDGET_UPDATED_AT_FIELD_ID),
            Some(BUDGET_APPROVED_AMOUNT_FIELD_ID),
            Some(BUDGET_ALLOCATED_AMOUNT_FIELD_ID),
        ]
    {
        return Err(RiffDbPublicAdapterError::InvalidResponse);
    }
    if !matches!(
        value_field(fields, BUDGET_UPDATED_AT_FIELD_ID)?.kind,
        Some(v1::value::Kind::TimestampValue(_))
    ) {
        return Err(RiffDbPublicAdapterError::InvalidResponse);
    }
    Ok(BudgetState {
        key,
        approved_amount: entity_decimal(fields, BUDGET_APPROVED_AMOUNT_FIELD_ID)?,
        allocated_amount: entity_decimal(fields, BUDGET_ALLOCATED_AMOUNT_FIELD_ID)?,
    })
}

fn entity_decimal(
    fields: &v1::ValueRecord,
    field_id: u32,
) -> Result<WorkloadAmount, RiffDbPublicAdapterError> {
    let Some(v1::value::Kind::DecimalValue(decimal)) = value_field(fields, field_id)?.kind.as_ref()
    else {
        return Err(RiffDbPublicAdapterError::InvalidResponse);
    };
    let spec = DecimalSpec::new(28, 2).map_err(|_| RiffDbPublicAdapterError::InvalidResponse)?;
    let decimal =
        decimal_from_proto(decimal, spec).map_err(|_| RiffDbPublicAdapterError::InvalidResponse)?;
    WorkloadAmount::from_minor_units(decimal.coefficient())
        .map_err(|_| RiffDbPublicAdapterError::InvalidResponse)
}

fn value_field(
    fields: &v1::ValueRecord,
    field_id: u32,
) -> Result<&v1::Value, RiffDbPublicAdapterError> {
    fields
        .fields
        .iter()
        .find(|field| field.field_id == Some(field_id))
        .and_then(|field| field.value.as_ref())
        .ok_or(RiffDbPublicAdapterError::InvalidResponse)
}

async fn next_matching_create_commit(
    subscription: &mut riffdb_client_rust::CommitNotificationStream,
) -> Result<v1::Commit, RiffDbPublicAdapterError> {
    loop {
        let notification = subscription
            .message()
            .await
            .map_err(map_client_error)?
            .ok_or(RiffDbPublicAdapterError::CommitNotificationEnded)?;
        match notification.notification {
            Some(v1::commit_notification::Notification::Commit(commit))
                if commit.contract_lineage == CONTRACT_LINEAGE
                    && commit.contract_version == CONTRACT_VERSION
                    && commit.command_id == CREATE_BUDGET_COMMAND_ID
                    && commit.plan_hash.as_slice() == CREATE_BUDGET_PLAN_HASH =>
            {
                return Ok(commit);
            }
            Some(v1::commit_notification::Notification::Commit(_)) => {}
            Some(v1::commit_notification::Notification::Terminal(_)) | None => {
                return Err(RiffDbPublicAdapterError::CommitNotificationEnded);
            }
        }
    }
}

fn decode_notified_create_outcome(
    command: &CreateBudget,
    commit: &v1::Commit,
) -> Result<CreateBudgetOutcome, RiffDbPublicAdapterError> {
    let outcome = commit
        .outcome
        .as_ref()
        .ok_or(RiffDbPublicAdapterError::InvalidResponse)?;
    if outcome.outcome_id != 1 {
        return Err(RiffDbPublicAdapterError::InvalidResponse);
    }
    command
        .decode_outcome(&v1::ExecuteCommandResponse {
            status: v1::execute_command_response::CompletionStatus::Replayed as i32,
            commit_sequence: commit.commit_sequence,
            contract_version: commit.contract_version,
            plan_hash: commit.plan_hash.clone(),
            outcome_type: outcome.outcome_name.clone(),
            outcome: Some(v1::Value {
                kind: outcome.value.clone().map(v1::value::Kind::RecordValue),
            }),
            provenance_uri: commit.provenance_uri.clone(),
            durability_mode: "sync".to_owned(),
            outcome_uri: None,
        })
        .map_err(|_| RiffDbPublicAdapterError::InvalidResponse)
}

fn fresh_request_id_bytes() -> Result<Vec<u8>, RiffDbPublicAdapterError> {
    generate_request_id()
        .map(|request_id| request_id.into_bytes().to_vec())
        .map_err(|_| RiffDbPublicAdapterError::RequestIdentityUnavailable)
}

fn one_attempt() -> AttemptBudget {
    match AttemptBudget::new(1) {
        Some(attempts) => attempts,
        None => unreachable!("one is a nonzero submission bound"),
    }
}

fn map_client_error(_: ClientError) -> RiffDbPublicAdapterError {
    RiffDbPublicAdapterError::PublicCallFailed
}

fn map_generated_execution_error(_: GeneratedExecutionError) -> RiffDbPublicAdapterError {
    RiffDbPublicAdapterError::PublicCallFailed
}

fn map_generated_command_error(_: GeneratedCommandError) -> RiffDbPublicAdapterError {
    RiffDbPublicAdapterError::InvalidWorkload
}

fn map_oracle_mismatch(_: OracleMismatch) -> RiffDbPublicAdapterError {
    RiffDbPublicAdapterError::OracleMismatch
}

#[derive(Clone)]
struct AsyncStartBarrier {
    state: Arc<Mutex<StartBarrierState>>,
    parties: usize,
}

impl AsyncStartBarrier {
    fn new(parties: usize) -> Self {
        Self {
            state: Arc::new(Mutex::new(StartBarrierState {
                arrived: 0,
                released: false,
                waiters: Vec::with_capacity(parties),
            })),
            parties,
        }
    }

    fn wait(&self) -> StartBarrierWait {
        StartBarrierWait {
            barrier: self.clone(),
            registered: false,
        }
    }
}

struct StartBarrierState {
    arrived: usize,
    released: bool,
    waiters: Vec<Waker>,
}

struct StartBarrierWait {
    barrier: AsyncStartBarrier,
    registered: bool,
}

impl Future for StartBarrierWait {
    type Output = Result<(), RiffDbPublicAdapterError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let shared_state = Arc::clone(&self.barrier.state);
        let parties = self.barrier.parties;
        let mut state = match shared_state.lock() {
            Ok(state) => state,
            Err(_) => return Poll::Ready(Err(RiffDbPublicAdapterError::SynchronizationFailed)),
        };
        if state.released {
            return Poll::Ready(Ok(()));
        }
        if !self.registered {
            state.arrived = match state.arrived.checked_add(1) {
                Some(arrived) => arrived,
                None => {
                    return Poll::Ready(Err(RiffDbPublicAdapterError::SynchronizationFailed));
                }
            };
            self.registered = true;
        }
        if state.arrived == parties {
            state.released = true;
            let waiters = std::mem::take(&mut state.waiters);
            drop(state);
            for waiter in waiters {
                waiter.wake();
            }
            Poll::Ready(Ok(()))
        } else {
            if !state
                .waiters
                .iter()
                .any(|waiter| waiter.will_wake(context.waker()))
            {
                state.waiters.push(context.waker().clone());
            }
            Poll::Pending
        }
    }
}

/// A redacted, closed public-comparison adapter failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RiffDbPublicAdapterError {
    /// The configured public endpoint could not be reached.
    ConnectionFailed,
    /// A fresh public request identity could not be generated.
    RequestIdentityUnavailable,
    /// The built-in workload could not be represented by generated bindings.
    InvalidWorkload,
    /// A checked public RPC failed.
    PublicCallFailed,
    /// A successful RPC had a shape inconsistent with the checked contract.
    InvalidResponse,
    /// A supposedly fresh comparison database contained replayed workload identities.
    FreshDatabaseRequired,
    /// The normalized result differed from the shared reference model.
    OracleMismatch,
    /// The explicit contention start barrier could not make progress safely.
    SynchronizationFailed,
    /// The commit stream ended before publishing the expected command.
    CommitNotificationEnded,
    /// The replay did not retain the notified durable identity.
    ReplayEvidenceMismatch,
}

impl fmt::Display for RiffDbPublicAdapterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ConnectionFailed => "public RiffDB endpoint connection failed",
            Self::RequestIdentityUnavailable => "public request identity is unavailable",
            Self::InvalidWorkload => "canonical budget workload is invalid",
            Self::PublicCallFailed => "public RiffDB operation failed",
            Self::InvalidResponse => "public RiffDB response is inconsistent",
            Self::FreshDatabaseRequired => "budget comparison requires a fresh database",
            Self::OracleMismatch => "public RiffDB observation differs from the reference model",
            Self::SynchronizationFailed => "comparison synchronization failed",
            Self::CommitNotificationEnded => "commit notification ended before expected evidence",
            Self::ReplayEvidenceMismatch => "same-key replay identity did not match the commit",
        })
    }
}

impl Error for RiffDbPublicAdapterError {}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Wake, Waker};

    use super::*;

    #[test]
    fn public_comparison_case_spellings_are_closed() {
        assert_eq!(PublicComparisonCase::Sequential.as_str(), "sequential");
        assert_eq!(PublicComparisonCase::Contention.as_str(), "contention");
        assert_eq!(
            PublicComparisonCase::SameKeyReplay.as_str(),
            "same_key_replay"
        );
    }

    #[test]
    fn public_comparison_generated_workload_mapping_preserves_exact_fixed_scale_values() {
        let workload = canonical_workload();
        for operation in workload.sequential.operations {
            match operation {
                BudgetOperation::Create(command) => {
                    let generated = generated_create(&command).expect("generated CreateBudget");
                    assert_eq!(
                        generated.approved_amount.minor_units(),
                        command.approved_amount.minor_units()
                    );
                    assert_eq!(
                        generated.organization_id,
                        command.key.organization_id.as_bytes()
                    );
                    assert_eq!(generated.fiscal_year, command.key.fiscal_year);
                }
                BudgetOperation::Allocate(command) => {
                    let generated = generated_allocate(&command).expect("generated AllocateBudget");
                    assert_eq!(generated.amount.minor_units(), command.amount.minor_units());
                    assert_eq!(
                        generated.organization_id,
                        command.key.organization_id.as_bytes()
                    );
                    assert_eq!(generated.fiscal_year, command.key.fiscal_year);
                    assert_eq!(generated.matter_id, command.matter_id.as_bytes());
                }
            }
        }
    }

    #[test]
    fn public_comparison_start_barrier_releases_only_after_both_waiters_arrive() {
        struct WakeCounter(AtomicUsize);

        impl Wake for WakeCounter {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }

            fn wake_by_ref(self: &Arc<Self>) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let barrier = AsyncStartBarrier::new(2);
        let first_wait = barrier.wait();
        let second_wait = barrier.wait();
        let mut first_wait = std::pin::pin!(first_wait);
        let mut second_wait = std::pin::pin!(second_wait);
        let wake_counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&wake_counter));
        let mut context = Context::from_waker(&waker);

        assert!(matches!(
            first_wait.as_mut().poll(&mut context),
            Poll::Pending
        ));
        assert_eq!(wake_counter.0.load(Ordering::SeqCst), 0);
        assert!(matches!(
            first_wait.as_mut().poll(&mut context),
            Poll::Pending
        ));
        assert!(matches!(
            second_wait.as_mut().poll(&mut context),
            Poll::Ready(Ok(()))
        ));
        assert_eq!(wake_counter.0.load(Ordering::SeqCst), 1);
        assert!(matches!(
            first_wait.as_mut().poll(&mut context),
            Poll::Ready(Ok(()))
        ));
    }
}
