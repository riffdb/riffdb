//! Total mechanical conversion between public wire messages and service DTOs.

use std::num::{NonZeroU16, NonZeroU32};
use std::time::Duration;

use riffdb_auth::AuthenticationContext;
use riffdb_proto::{canonical_value_to_proto, v1};
use riffdb_service::{
    BootstrapCapabilityRequest, BootstrapCapabilityResult, CapabilityIdentityView,
    CapabilityTransitionView, CommandDurability, CommitSubscriptionEndReason,
    CommitSubscriptionEvent, CommitView, ContractDescriptor, ContractSelection, ContractSource,
    ContractValidationResult, CreateCapabilityResult, CursorToken, DeployContractRequest,
    DeployContractResult, ExecuteCommandRequest, ExecuteCommandResult, ExplainCommandRequest,
    ExplainCommandResult, FieldSelection, GetActiveContractRequest, GetActiveContractResult,
    GetCommitRequest, GetCommitResult, GetEntityRequest, GetEntityResult, HealthComponentKind,
    HealthComponentStatus, HealthRequest, HealthResult, HealthStatus, JournaledCommandResult,
    JournaledCompletion, NormalCreateCapabilityRequest, NormalCreateCapabilityResult, PageLimit,
    PageRequest, PreBootstrapLifecycle, ProjectionFailureCode, ProjectionUnavailableReason,
    QueryProjectionRequest, QueryProjectionResult, ResolveCommandOutcomeRequest,
    ResolveCommandOutcomeResult, RevokeCapabilityRequest, RevokeCapabilityResult,
    ScanCommitsRequest, ScanCommitsResult, ScanIndexRequest, ScanIndexResult, SourceName,
    StatisticsRequest, StatisticsResult, SubmittedDecimal, SubmittedEnum, SubmittedField,
    SubmittedFieldIdentity, SubmittedMoney, SubmittedRecord, SubmittedValue,
    SubscribeToCommitsRequest, ValidateContractRequest,
};
use riffdb_types::{
    ActorId, ActorKind, AdmittedActorContext, Audience, CapabilityGrantV1, CapabilityId,
    CapabilityPermissionKindV1, CapabilityPermissionV1, CapabilityPermissionsV1, CommandId,
    CommitSequence, ContractLineage, ContractVersion, CurrencyCode, Date, EntityFieldVisibilityV1,
    EntityKey, EntityTypeId, EnumTypeId, EnumVariantId, FieldId, FrontierPosition, IdempotencyKey,
    IndexEpochPosition, IndexId, PartitionKey, PartitionScopeV1, ProjectionId, RequestId,
    RevocationReasonCodeV1, ScopedPartitionV1, TenantId, TenantScope, Timestamp,
};
use tonic::Status;

/// Static message for a structurally invalid public request.
pub const INVALID_REQUEST_MESSAGE: &str = "request failed structural validation";

/// Parses one exact network-order UUIDv7 request identifier.
pub fn request_id_from_bytes(bytes: &[u8]) -> Result<RequestId, Status> {
    let bytes: [u8; 16] = bytes.try_into().map_err(|_| invalid_request())?;
    RequestId::from_bytes(bytes).map_err(|_| invalid_request())
}

/// Converts the public active-or-exact contract selector without catalog access.
pub fn contract_selection_from_proto(
    selection: v1::ContractSelection,
) -> Result<ContractSelection, Status> {
    match selection.selection.ok_or_else(invalid_request)? {
        v1::contract_selection::Selection::Active(_) => Ok(ContractSelection::Active),
        v1::contract_selection::Selection::Exact(exact) => Ok(ContractSelection::Exact {
            lineage: ContractLineage::new(exact.contract_lineage).map_err(|_| invalid_request())?,
            version: ContractVersion::new(exact.contract_version).ok_or_else(invalid_request)?,
        }),
    }
}

/// Converts checked pagination syntax into the opaque service cursor boundary.
pub fn page_request_from_proto(page: v1::PageRequest) -> Result<PageRequest, Status> {
    let limit = match page.limit {
        Some(limit) => PageLimit::new(u16::try_from(limit).map_err(|_| invalid_request())?)
            .map_err(|_| invalid_request())?,
        None => PageLimit::default(),
    };
    let cursor = match page.cursor {
        Some(bytes) => {
            let bytes: [u8; 16] = bytes.try_into().map_err(|_| invalid_request())?;
            Some(CursorToken::from_bytes(bytes))
        }
        None => None,
    };
    Ok(PageRequest::new(limit, cursor))
}

/// Converts a canonical service page request back to the exact public shape.
#[must_use]
pub fn page_request_to_proto(page: PageRequest) -> v1::PageRequest {
    v1::PageRequest {
        limit: Some(u32::from(page.limit().get().get())),
        cursor: page.cursor().map(|cursor| cursor.as_bytes().to_vec()),
    }
}

/// Converts duplicate-free public field identities into the service owner.
pub fn field_selection_from_proto(selection: v1::FieldSelection) -> Result<FieldSelection, Status> {
    let fields = selection
        .field_ids
        .into_iter()
        .map(|field| FieldId::new(field).ok_or_else(invalid_request))
        .collect::<Result<Vec<_>, _>>()?;
    FieldSelection::new(fields).map_err(|_| invalid_request())
}

/// Converts a public contract-validation request and separates transport identity.
pub fn validate_contract_request_from_proto(
    request: v1::ValidateContractRequest,
) -> Result<(RequestId, ValidateContractRequest), Status> {
    let request_id = request_id_from_bytes(&request.request_id)?;
    let source = ContractSource::new(request.source).map_err(|_| invalid_request())?;
    let request = ValidateContractRequest::new(source).map_err(|_| invalid_request())?;
    Ok((request_id, request))
}

/// Converts a public command-explanation request without resolving a catalog.
pub fn explain_command_request_from_proto(
    request: v1::ExplainCommandRequest,
) -> Result<(RequestId, ExplainCommandRequest), Status> {
    let request_id = request_id_from_bytes(&request.request_id)?;
    let contract = contract_selection_from_proto(request.contract.ok_or_else(invalid_request)?)?;
    let command = SourceName::new(request.command_name).map_err(|_| invalid_request())?;
    Ok((request_id, ExplainCommandRequest::new(contract, command)))
}

/// Converts a public deployment request and preserves expected-absence semantics.
pub fn deploy_contract_request_from_proto(
    request: v1::DeployContractRequest,
) -> Result<(RequestId, DeployContractRequest), Status> {
    let request_id = request_id_from_bytes(&request.request_id)?;
    let source = ContractSource::new(request.source).map_err(|_| invalid_request())?;
    let expected = request
        .expected_active_version
        .map(|version| ContractVersion::new(version).ok_or_else(invalid_request))
        .transpose()?;
    let request = DeployContractRequest::new(source, expected).map_err(|_| invalid_request())?;
    Ok((request_id, request))
}

/// Converts the active-contract request and separates its transport identity.
pub fn get_active_contract_request_from_proto(
    request: v1::GetActiveContractRequest,
) -> Result<(RequestId, GetActiveContractRequest), Status> {
    Ok((
        request_id_from_bytes(&request.request_id)?,
        GetActiveContractRequest,
    ))
}

/// Converts Execute input mechanically and leaves schema resolution to the service.
pub fn execute_command_request_from_proto(
    request: v1::ExecuteCommandRequest,
) -> Result<(RequestId, ExecuteCommandRequest), Status> {
    let request_id = request_id_from_bytes(&request.request_id)?;
    let command = SourceName::new(request.command_name).map_err(|_| invalid_request())?;
    let expected_contract_version = request
        .expected_contract_version
        .map(|version| ContractVersion::new(version).ok_or_else(invalid_request))
        .transpose()?;
    let input = submitted_value_from_proto(request.input.ok_or_else(invalid_request)?)?;
    let SubmittedValue::Record(input) = input else {
        return Err(invalid_request());
    };
    let request = ExecuteCommandRequest::new(command, expected_contract_version, input)
        .map_err(|_| invalid_request())?;
    Ok((request_id, request))
}

/// Converts one exact uncertainty-recovery request.
pub fn resolve_outcome_request_from_proto(
    request: v1::GetOutcomeRequest,
) -> Result<(RequestId, ResolveCommandOutcomeRequest), Status> {
    let request_id = request_id_from_bytes(&request.request_id)?;
    let lineage = ContractLineage::new(request.contract_lineage).map_err(|_| invalid_request())?;
    let command = SourceName::new(request.command_name).map_err(|_| invalid_request())?;
    let idempotency_key =
        IdempotencyKey::new(request.idempotency_key).map_err(|_| invalid_request())?;
    Ok((
        request_id,
        ResolveCommandOutcomeRequest::new(lineage, command, idempotency_key),
    ))
}

/// Converts one exact entity lookup after stage-one key-envelope validation.
pub fn get_entity_request_from_proto(
    request: v1::GetEntityRequest,
) -> Result<(RequestId, GetEntityRequest), Status> {
    let request_id = request_id_from_bytes(&request.request_id)?;
    let contract = contract_selection_from_proto(request.contract.ok_or_else(invalid_request)?)?;
    let entity_type_id = EntityTypeId::new(request.entity_type_id).ok_or_else(invalid_request)?;
    let key = EntityKey::from_bytes(request.entity_key).map_err(|_| invalid_request())?;
    let fields = field_selection_from_proto(request.fields.ok_or_else(invalid_request)?)?;
    let request = GetEntityRequest::new(contract, entity_type_id, key, fields)
        .map_err(|_| invalid_request())?;
    Ok((request_id, request))
}

/// Converts an index scan without resolving submitted prefix components.
pub fn scan_index_request_from_proto(
    request: v1::ScanIndexRequest,
) -> Result<(RequestId, ScanIndexRequest), Status> {
    let request_id = request_id_from_bytes(&request.request_id)?;
    let contract = contract_selection_from_proto(request.contract.ok_or_else(invalid_request)?)?;
    let index_id = IndexId::new(request.index_id).ok_or_else(invalid_request)?;
    let leading_components = request
        .leading_components
        .into_iter()
        .map(submitted_value_from_proto)
        .collect::<Result<Vec<_>, _>>()?;
    let fields = field_selection_from_proto(request.fields.ok_or_else(invalid_request)?)?;
    let page = page_request_from_proto(request.page.ok_or_else(invalid_request)?)?;
    let request = ScanIndexRequest::new(contract, index_id, leading_components, fields, page)
        .map_err(|_| invalid_request())?;
    Ok((request_id, request))
}

/// Converts a projection query without selecting a schema or catalog version.
pub fn query_projection_request_from_proto(
    request: v1::QueryProjectionRequest,
) -> Result<(RequestId, QueryProjectionRequest), Status> {
    let request_id = request_id_from_bytes(&request.request_id)?;
    let contract = contract_selection_from_proto(request.contract.ok_or_else(invalid_request)?)?;
    let projection_id = ProjectionId::new(request.projection_id).ok_or_else(invalid_request)?;
    let leading_components = request
        .leading_components
        .into_iter()
        .map(submitted_value_from_proto)
        .collect::<Result<Vec<_>, _>>()?;
    let required_sequence = optional_sequence(request.required_sequence)?;
    let page = page_request_from_proto(request.page.ok_or_else(invalid_request)?)?;
    let request = QueryProjectionRequest::new(
        contract,
        projection_id,
        leading_components,
        required_sequence,
        duration_from_nanos(request.wait_nanos),
        page,
    )
    .map_err(|_| invalid_request())?;
    Ok((request_id, request))
}

/// Converts a filtered entity result without reintroducing hidden fields.
pub fn get_entity_result_to_proto(
    result: &GetEntityResult,
) -> Result<v1::GetEntityResponse, Status> {
    let result = match result {
        GetEntityResult::NotFound => v1::get_entity_response::Result::NotFound(v1::Unit {}),
        GetEntityResult::Found(entity) => v1::get_entity_response::Result::Found(v1::Entity {
            entity_key: entity.key().as_bytes().to_vec(),
            entity_version: entity.entity_version().get(),
            written_by_contract_version: entity.written_by_contract().get(),
            fields: Some(canonical_record_to_public(entity.fields())?),
        }),
    };
    Ok(v1::GetEntityResponse {
        result: Some(result),
    })
}

/// Converts an already filtered authoritative index page.
pub fn scan_index_result_to_proto(
    result: &ScanIndexResult,
) -> Result<v1::ScanIndexResponse, Status> {
    let page = result.page();
    let items = page
        .items()
        .iter()
        .map(|row| {
            Ok(v1::IndexRow {
                index_entry_key: row.key().as_bytes().to_vec(),
                values: Some(canonical_record_to_public(row.values())?),
            })
        })
        .collect::<Result<Vec<_>, Status>>()?;
    Ok(v1::ScanIndexResponse {
        page: Some(v1::IndexPage {
            items,
            next_cursor: page.next_cursor().map(|cursor| cursor.as_bytes().to_vec()),
            observed_fence: Some(v1::IndexScanFence {
                position: Some(match page.observed_fence().position() {
                    IndexEpochPosition::BeforeFirst => {
                        v1::index_scan_fence::Position::BeforeFirst(v1::Unit {})
                    }
                    IndexEpochPosition::Value(epoch) => {
                        v1::index_scan_fence::Position::AppliedEpoch(epoch.get())
                    }
                }),
            }),
        }),
    })
}

/// Converts every closed projection-query result variant.
pub fn query_projection_result_to_proto(
    result: &QueryProjectionResult,
) -> Result<v1::QueryProjectionResponse, Status> {
    let result = match result {
        QueryProjectionResult::Ready(ready) => {
            let page = ready.data();
            let rows = page
                .items()
                .iter()
                .map(|row| {
                    Ok(v1::ProjectionRow {
                        group: row
                            .group()
                            .iter()
                            .map(canonical_value_to_public)
                            .collect::<Result<Vec<_>, _>>()?,
                        values: Some(canonical_record_to_public(row.values())?),
                    })
                })
                .collect::<Result<Vec<_>, Status>>()?;
            let fence = page.observed_fence();
            v1::query_projection_response::Result::Ready(v1::QueryProjectionReady {
                data: Some(v1::ProjectionPage {
                    items: rows,
                    next_cursor: page.next_cursor().map(|cursor| cursor.as_bytes().to_vec()),
                    observed_fence: Some(v1::ProjectionPageFence {
                        identity: Some(projection_identity_to_proto(fence.identity())),
                        generation: fence.generation().get(),
                        frontier: Some(frontier_to_proto(fence.frontier())),
                    }),
                }),
                frontier: Some(frontier_to_proto(ready.frontier())),
            })
        }
        QueryProjectionResult::WaitTimedOut { required, current } => {
            v1::query_projection_response::Result::WaitTimedOut(v1::QueryProjectionWaitTimedOut {
                required_sequence: required.get(),
                current: Some(frontier_to_proto(*current)),
            })
        }
        QueryProjectionResult::Degraded { current, reason } => {
            let reason = match reason {
                ProjectionUnavailableReason::Building => {
                    v1::projection_unavailable_reason::Reason::Building(v1::Unit {})
                }
                ProjectionUnavailableReason::Rebuilding => {
                    v1::projection_unavailable_reason::Reason::Rebuilding(v1::Unit {})
                }
                ProjectionUnavailableReason::Failure(code) => {
                    v1::projection_unavailable_reason::Reason::Failure(projection_failure_code(
                        *code,
                    ) as i32)
                }
            };
            v1::query_projection_response::Result::Degraded(v1::QueryProjectionDegraded {
                current: Some(frontier_to_proto(*current)),
                reason: Some(v1::ProjectionUnavailableReason {
                    reason: Some(reason),
                }),
            })
        }
        QueryProjectionResult::Invalid { reason } => {
            v1::query_projection_response::Result::Invalid(v1::QueryProjectionInvalid {
                reason: projection_failure_code(*reason) as i32,
            })
        }
    };
    Ok(v1::QueryProjectionResponse {
        result: Some(result),
    })
}

/// Converts an exact projection identity without accepting a caller hash.
#[must_use]
pub fn projection_identity_to_proto(
    identity: &riffdb_types::ProjectionIdentity,
) -> v1::ProjectionIdentity {
    v1::ProjectionIdentity {
        contract_lineage: identity.contract_lineage().as_str().to_owned(),
        projection_id: identity.projection_id().get(),
        projection_plan_hash: identity.plan_hash().as_bytes().to_vec(),
    }
}

/// Converts one exact commit lookup request.
pub fn get_commit_request_from_proto(
    request: v1::GetCommitRequest,
) -> Result<(RequestId, GetCommitRequest), Status> {
    let request_id = request_id_from_bytes(&request.request_id)?;
    let sequence = CommitSequence::new(request.commit_sequence).ok_or_else(invalid_request)?;
    Ok((request_id, GetCommitRequest::new(sequence)))
}

/// Converts one bounded commit scan request.
pub fn scan_commits_request_from_proto(
    request: v1::ScanCommitsRequest,
) -> Result<(RequestId, ScanCommitsRequest), Status> {
    let request_id = request_id_from_bytes(&request.request_id)?;
    let page = page_request_from_proto(request.page.ok_or_else(invalid_request)?)?;
    Ok((request_id, ScanCommitsRequest::new(page)))
}

/// Converts one bounded commit-subscription establishment request.
pub fn subscribe_commits_request_from_proto(
    request: v1::SubscribeCommitsRequest,
) -> Result<(RequestId, SubscribeToCommitsRequest), Status> {
    let request_id = request_id_from_bytes(&request.request_id)?;
    let after = optional_sequence(request.after_sequence)?;
    let request =
        SubscribeToCommitsRequest::new(after, Duration::from_nanos(request.maximum_lifetime_nanos))
            .map_err(|_| invalid_request())?;
    Ok((request_id, request))
}

/// Converts one filtered commit lookup result.
pub fn get_commit_result_to_proto(
    result: &GetCommitResult,
) -> Result<v1::GetCommitResponse, Status> {
    let result = match result {
        GetCommitResult::NotFound => v1::get_commit_response::Result::NotFound(v1::Unit {}),
        GetCommitResult::Found(commit) => {
            v1::get_commit_response::Result::Found(commit_to_proto(commit)?)
        }
    };
    Ok(v1::GetCommitResponse {
        result: Some(result),
    })
}

/// Converts one upper-fenced, already policy-filtered commit page.
pub fn scan_commits_result_to_proto(
    result: &ScanCommitsResult,
) -> Result<v1::ScanCommitsResponse, Status> {
    let page = result.page();
    let items = page
        .items()
        .iter()
        .map(commit_to_proto)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(v1::ScanCommitsResponse {
        page: Some(v1::CommitPage {
            items,
            next_cursor: page.next_cursor().map(|cursor| cursor.as_bytes().to_vec()),
            observed_fence: Some(frontier_to_proto(page.observed_fence().position())),
        }),
    })
}

/// Converts one post-establishment commit stream item or typed terminal item.
pub fn commit_subscription_event_to_proto(
    event: &CommitSubscriptionEvent,
) -> Result<v1::CommitNotification, Status> {
    let notification = match event {
        CommitSubscriptionEvent::Commit(commit) => {
            v1::commit_notification::Notification::Commit(commit_to_proto(commit)?)
        }
        CommitSubscriptionEvent::Terminal(terminal) => {
            let reason = match terminal.reason() {
                CommitSubscriptionEndReason::LifetimeElapsed => {
                    v1::CommitSubscriptionEndReason::LifetimeElapsed
                }
                CommitSubscriptionEndReason::Lagged => v1::CommitSubscriptionEndReason::Lagged,
                CommitSubscriptionEndReason::ScanGap => v1::CommitSubscriptionEndReason::ScanGap,
                CommitSubscriptionEndReason::PolicyDenied => {
                    v1::CommitSubscriptionEndReason::PolicyDenied
                }
                CommitSubscriptionEndReason::Cancelled => {
                    v1::CommitSubscriptionEndReason::Cancelled
                }
                CommitSubscriptionEndReason::DeadlineExceeded => {
                    v1::CommitSubscriptionEndReason::DeadlineExceeded
                }
                CommitSubscriptionEndReason::ServiceShutdown => {
                    v1::CommitSubscriptionEndReason::ServiceShutdown
                }
                CommitSubscriptionEndReason::Unavailable => {
                    v1::CommitSubscriptionEndReason::Unavailable
                }
            };
            v1::commit_notification::Notification::Terminal(v1::CommitSubscriptionTerminal {
                reason: reason as i32,
                resume_after: Some(frontier_to_proto(terminal.resume_after())),
            })
        }
    };
    Ok(v1::CommitNotification {
        notification: Some(notification),
    })
}

/// Converts one complete already-redacted semantic commit.
pub fn commit_to_proto(commit: &CommitView) -> Result<v1::Commit, Status> {
    let commit = commit.as_snapshot();
    let events = commit
        .events()
        .iter()
        .map(|event| {
            let event_id = event.event_id();
            Ok(v1::DurableEvent {
                event_id: Some(v1::EventId {
                    commit_sequence: event_id.commit_sequence().get(),
                    event_ordinal: event_id.event_ordinal(),
                }),
                event_type_id: event.event_type_id().get(),
                payload: Some(canonical_record_to_public(event.payload())?),
            })
        })
        .collect::<Result<Vec<_>, Status>>()?;
    let affected_entities = commit
        .affected_entities()
        .iter()
        .map(|entity| v1::AffectedEntity {
            entity_key: entity.key().as_bytes().to_vec(),
            entity_version: entity.entity_version().get(),
        })
        .collect();
    let outcome = commit.outcome();
    let logical_time = commit.logical_time().timestamp();
    Ok(v1::Commit {
        commit_sequence: commit.sequence().get(),
        admission_request_id: commit.admission_request_id().as_bytes().to_vec(),
        contract_lineage: commit.lineage().as_str().to_owned(),
        contract_version: commit.contract_version().get(),
        command_id: commit.command_id().get(),
        plan_hash: commit.plan_hash().as_bytes().to_vec(),
        canonical_input_hash: commit.canonical_input_hash().as_bytes().to_vec(),
        actor: Some(admitted_actor_to_proto(commit.actor())),
        logical_time: Some(v1::Timestamp {
            seconds: logical_time.seconds(),
            nanos: logical_time.nanoseconds(),
        }),
        partition_hash: commit.partition_hash().as_bytes().to_vec(),
        conflict_hashes: commit
            .conflict_hashes()
            .iter()
            .map(|hash| hash.as_bytes().to_vec())
            .collect(),
        affected_entities,
        events,
        outcome: Some(v1::DeclaredOutcome {
            outcome_id: outcome.outcome_id().get(),
            outcome_name: outcome.outcome_name().as_str().to_owned(),
            value: Some(canonical_record_to_public(outcome.value())?),
        }),
        provenance_uri: format!("riffdb://provenance/{}", commit.provenance_id()),
        durability: match commit.durability() {
            CommandDurability::Synchronous => v1::CommandDurability::Synchronous as i32,
            CommandDurability::Group => v1::CommandDurability::Group as i32,
        },
    })
}

/// Converts a service-admitted actor without trusting request actor fields.
#[must_use]
pub fn admitted_actor_to_proto(actor: &AdmittedActorContext) -> v1::AdmittedActor {
    let actor_kind = match actor.actor_kind() {
        ActorKind::Human => v1::ActorKind::Human,
        ActorKind::Agent => v1::ActorKind::Agent,
        ActorKind::Service => v1::ActorKind::Service,
    };
    let scope = match actor.tenant_scope() {
        TenantScope::Global => v1::tenant_scope::Scope::Global(v1::Unit {}),
        TenantScope::Tenant(tenant) => {
            v1::tenant_scope::Scope::TenantId(tenant.as_str().to_owned())
        }
    };
    v1::AdmittedActor {
        principal_id: actor.principal_id().as_str().to_owned(),
        actor_kind: actor_kind as i32,
        tenant_scope: Some(v1::TenantScope { scope: Some(scope) }),
        agent_session_id: actor
            .agent_session_id()
            .map(|session| session.as_bytes().to_vec()),
    }
}

/// Converts the closed projection failure registry.
#[must_use]
pub const fn projection_failure_code(code: ProjectionFailureCode) -> v1::ProjectionFailureCode {
    match code {
        ProjectionFailureCode::ArithmeticOverflow => v1::ProjectionFailureCode::ArithmeticOverflow,
        ProjectionFailureCode::MalformedDurableEvent => {
            v1::ProjectionFailureCode::MalformedDurableEvent
        }
        ProjectionFailureCode::MissingCommit => v1::ProjectionFailureCode::MissingCommit,
        ProjectionFailureCode::PlanOrSchemaUnavailable => {
            v1::ProjectionFailureCode::PlanOrSchemaUnavailable
        }
        ProjectionFailureCode::StateIntegrityFailure => {
            v1::ProjectionFailureCode::ProjectionStateIntegrity
        }
        ProjectionFailureCode::HardLimitExceeded => v1::ProjectionFailureCode::HardLimitExceeded,
    }
}

/// Converts a checked command result, including exact read-only sentinels.
pub fn execute_command_result_to_proto(
    result: &ExecuteCommandResult,
) -> Result<v1::ExecuteCommandResponse, Status> {
    match result {
        ExecuteCommandResult::Journaled(result) => journaled_command_result_to_proto(result),
        ExecuteCommandResult::ReadOnlyExecuted(result) => {
            let outcome = result.outcome();
            Ok(v1::ExecuteCommandResponse {
                status: v1::execute_command_response::CompletionStatus::ExecutedReadOnly as i32,
                commit_sequence: 0,
                contract_version: result.contract_version().get(),
                plan_hash: result.plan_hash().as_bytes().to_vec(),
                outcome_type: outcome.outcome_name().as_str().to_owned(),
                outcome: Some(record_as_public_value(outcome.value())?),
                provenance_uri: String::new(),
                durability_mode: String::new(),
            })
        }
    }
}

/// Converts a durable command result with only production durability values.
pub fn journaled_command_result_to_proto(
    result: &JournaledCommandResult,
) -> Result<v1::ExecuteCommandResponse, Status> {
    let status = match result.completion() {
        JournaledCompletion::Committed => v1::execute_command_response::CompletionStatus::Committed,
        JournaledCompletion::Replayed => v1::execute_command_response::CompletionStatus::Replayed,
    };
    let durability_mode = match result.durability() {
        CommandDurability::Synchronous => "sync",
        CommandDurability::Group => "group",
    };
    let outcome = result.outcome();
    Ok(v1::ExecuteCommandResponse {
        status: status as i32,
        commit_sequence: result.commit_sequence().get(),
        contract_version: result.contract_version().get(),
        plan_hash: result.plan_hash().as_bytes().to_vec(),
        outcome_type: outcome.outcome_name().as_str().to_owned(),
        outcome: Some(record_as_public_value(outcome.value())?),
        provenance_uri: format!("riffdb://provenance/{}", result.provenance_id()),
        durability_mode: durability_mode.to_owned(),
    })
}

/// Converts a checked outcome lookup without fabricating a first-execution result.
pub fn resolve_outcome_result_to_proto(
    result: &ResolveCommandOutcomeResult,
) -> Result<v1::GetOutcomeResponse, Status> {
    let result = match result {
        ResolveCommandOutcomeResult::NotFound => {
            v1::get_outcome_response::Result::NotFound(v1::Unit {})
        }
        ResolveCommandOutcomeResult::Found(result) => v1::get_outcome_response::Result::Found(
            journaled_command_result_to_proto(result.journaled())?,
        ),
    };
    Ok(v1::GetOutcomeResponse {
        result: Some(result),
    })
}

/// Converts checked compiler validation data into its closed public result.
pub fn contract_validation_result_to_proto(
    result: &ContractValidationResult,
) -> Result<v1::ValidateContractResponse, Status> {
    let result = match result {
        ContractValidationResult::Valid => {
            v1::validate_contract_response::Result::Valid(v1::Unit {})
        }
        ContractValidationResult::Invalid(error) => {
            let diagnostics = if let Some(syntax) = error.syntax() {
                let diagnostics = syntax
                    .as_slice()
                    .iter()
                    .map(|diagnostic| {
                        let code = diagnostic.code();
                        let span = diagnostic.span();
                        v1::SyntaxDiagnostic {
                            code: code.as_str().to_owned(),
                            summary: code.summary().to_owned(),
                            help: code.help().map(str::to_owned),
                            span: Some(v1::SourceSpan {
                                start: span.start(),
                                end: span.end(),
                            }),
                            expected: diagnostic
                                .expected()
                                .iter()
                                .map(|value| (*value).to_owned())
                                .collect(),
                        }
                    })
                    .collect();
                v1::compilation_diagnostics::Diagnostics::Syntax(v1::SyntaxDiagnosticList {
                    diagnostics,
                })
            } else if let Some(semantic) = error.semantic() {
                let diagnostics = semantic
                    .as_slice()
                    .iter()
                    .map(|diagnostic| {
                        let code = diagnostic.code();
                        let primary = diagnostic.primary_span();
                        let related_span = diagnostic.related_span().map(|span| v1::SourceSpan {
                            start: span.start(),
                            end: span.end(),
                        });
                        v1::SemanticDiagnostic {
                            code: code.as_str().to_owned(),
                            summary: code.summary().to_owned(),
                            help: code.help().map(str::to_owned),
                            primary_span: Some(v1::SourceSpan {
                                start: primary.start(),
                                end: primary.end(),
                            }),
                            related_span,
                        }
                    })
                    .collect();
                v1::compilation_diagnostics::Diagnostics::Semantic(v1::SemanticDiagnosticList {
                    diagnostics,
                })
            } else {
                return Err(invalid_service_response());
            };
            v1::validate_contract_response::Result::Invalid(v1::CompilationDiagnostics {
                diagnostics: Some(diagnostics),
            })
        }
    };
    Ok(v1::ValidateContractResponse {
        result: Some(result),
    })
}

/// Converts a checked command explanation and its generated schemas.
pub fn explain_command_result_to_proto(
    result: &ExplainCommandResult,
) -> Result<v1::ExplainCommandResponse, Status> {
    let result = match result {
        ExplainCommandResult::NotFound => {
            v1::explain_command_response::Result::NotFound(v1::Unit {})
        }
        ExplainCommandResult::Found(command) => {
            let explanation = command.explanation();
            let execution_class = match explanation.execution_class() as u8 {
                1 => v1::ExecutionClass::ReadOnly,
                2 => v1::ExecutionClass::IdempotentMutation,
                _ => return Err(invalid_service_response()),
            };
            let explanation = v1::CommandExplain {
                command_id: explanation.command_id().get(),
                execution_class: execution_class as i32,
                partition_component_count: u32::try_from(explanation.partition_component_count())
                    .map_err(|_| invalid_service_response())?,
                conflict_key_count: u32::try_from(explanation.conflict_key_count())
                    .map_err(|_| invalid_service_response())?,
                binding_ids: explanation.bindings().iter().map(|id| id.get()).collect(),
                read_fields: explanation
                    .read_fields()
                    .iter()
                    .map(|(binding, field)| v1::BindingFieldRef {
                        binding_id: binding.get(),
                        field_id: field.get(),
                    })
                    .collect(),
                write_fields: explanation
                    .write_fields()
                    .iter()
                    .map(|(binding, field)| v1::BindingFieldRef {
                        binding_id: binding.get(),
                        field_id: field.get(),
                    })
                    .collect(),
                invariant_ids: explanation.invariants().iter().map(|id| id.get()).collect(),
                event_type_ids: explanation.events().iter().map(|id| id.get()).collect(),
                outcome_ids: explanation.outcomes().iter().map(|id| id.get()).collect(),
                rendered_text: explanation.render_text(),
            };
            let schema = |artifact_tag: u8,
                          stable_id: u32,
                          hash: &[u8; 32],
                          canonical_json: &str|
             -> Result<v1::GeneratedSchemaArtifact, Status> {
                let artifact = match artifact_tag {
                    1 => v1::schema_artifact_key::Artifact::EntityId(stable_id),
                    2 => v1::schema_artifact_key::Artifact::EventTypeId(stable_id),
                    3 => v1::schema_artifact_key::Artifact::CommandInputId(stable_id),
                    4 => v1::schema_artifact_key::Artifact::CommandOutcomeUnionId(stable_id),
                    5 => v1::schema_artifact_key::Artifact::ProjectionResultId(stable_id),
                    _ => return Err(invalid_service_response()),
                };
                Ok(v1::GeneratedSchemaArtifact {
                    key: Some(v1::SchemaArtifactKey {
                        artifact: Some(artifact),
                    }),
                    dialect: "https://json-schema.org/draft/2020-12/schema".to_owned(),
                    schema_hash: hash.to_vec(),
                    canonical_json: canonical_json.to_owned(),
                })
            };
            let input = command.input_schema();
            let input_key = input.key();
            let input_schema = schema(
                input_key.tag(),
                input_key.stable_id(),
                input.hash().as_bytes(),
                input.canonical_json(),
            )?;
            let outcome = command.outcome_schema();
            let outcome_key = outcome.key();
            let outcome_schema = schema(
                outcome_key.tag(),
                outcome_key.stable_id(),
                outcome.hash().as_bytes(),
                outcome.canonical_json(),
            )?;
            v1::explain_command_response::Result::Found(v1::ExplainedCommand {
                contract: Some(contract_descriptor_to_proto(command.contract())),
                command_id: command.command_id().get(),
                plan_hash: command.plan_hash().as_bytes().to_vec(),
                explanation: Some(explanation),
                input_schema: Some(input_schema),
                outcome_schema: Some(outcome_schema),
            })
        }
    };
    Ok(v1::ExplainCommandResponse {
        result: Some(result),
    })
}

/// Converts a checked deployment result without exposing storage transitions.
#[must_use]
pub fn deploy_contract_result_to_proto(
    result: &DeployContractResult,
) -> v1::DeployContractResponse {
    let result = match result {
        DeployContractResult::Activated(descriptor) => {
            v1::deploy_contract_response::Result::Activated(contract_descriptor_to_proto(
                descriptor,
            ))
        }
        DeployContractResult::AlreadyActive(descriptor) => {
            v1::deploy_contract_response::Result::AlreadyActive(contract_descriptor_to_proto(
                descriptor,
            ))
        }
        DeployContractResult::ExpectedActiveVersionMismatch { actual } => {
            v1::deploy_contract_response::Result::ExpectedActiveVersionMismatch(
                v1::ExpectedActiveVersionMismatch {
                    actual_active_version: actual.map(ContractVersion::get),
                },
            )
        }
        DeployContractResult::BundleConflict => {
            v1::deploy_contract_response::Result::BundleConflict(v1::Unit {})
        }
    };
    v1::DeployContractResponse {
        result: Some(result),
    }
}

/// Converts the active-contract lookup result.
#[must_use]
pub fn get_active_contract_result_to_proto(
    result: &GetActiveContractResult,
) -> v1::GetActiveContractResponse {
    let result = match result {
        GetActiveContractResult::Absent => {
            v1::get_active_contract_response::Result::Absent(v1::Unit {})
        }
        GetActiveContractResult::Present(descriptor) => {
            v1::get_active_contract_response::Result::Present(contract_descriptor_to_proto(
                descriptor,
            ))
        }
    };
    v1::GetActiveContractResponse {
        result: Some(result),
    }
}

/// Separates the optional Health request identity from its empty service DTO.
pub fn health_request_from_proto(
    request: v1::HealthRequest,
) -> Result<(Option<RequestId>, HealthRequest), Status> {
    let request_id = request
        .request_id
        .as_deref()
        .map(request_id_from_bytes)
        .transpose()?;
    Ok((request_id, HealthRequest))
}

/// Converts the authenticated statistics request and its exact outer identity.
pub fn statistics_request_from_proto(
    request: v1::StatsRequest,
) -> Result<(RequestId, StatisticsRequest), Status> {
    Ok((
        request_id_from_bytes(&request.request_id)?,
        StatisticsRequest,
    ))
}

/// Converts the restricted or authenticated Health result without widening it.
#[must_use]
pub fn health_result_to_proto(result: &HealthResult) -> v1::HealthResponse {
    let result = match result {
        HealthResult::PreBootstrap(report) => {
            let lifecycle = match report.lifecycle() {
                PreBootstrapLifecycle::InitializingValidation => {
                    v1::PreBootstrapLifecycle::InitializingValidation
                }
                PreBootstrapLifecycle::InitializingBootstrap => {
                    v1::PreBootstrapLifecycle::InitializingBootstrap
                }
            };
            v1::health_response::Result::PreBootstrap(v1::PreBootstrapHealth {
                lifecycle: lifecycle as i32,
                liveness: report.liveness(),
                readiness: report.readiness(),
            })
        }
        HealthResult::Authenticated(report) => {
            let status = match report.status() {
                HealthStatus::Ready => v1::HealthStatus::Ready,
                HealthStatus::NotReady => v1::HealthStatus::NotReady,
                HealthStatus::Degraded => v1::HealthStatus::Degraded,
            };
            let components = report
                .components()
                .iter()
                .map(|component| {
                    let kind = match component.component() {
                        HealthComponentKind::AuthoritativeStorage => {
                            v1::HealthComponentKind::AuthoritativeStorage
                        }
                        HealthComponentKind::Catalog => v1::HealthComponentKind::Catalog,
                        HealthComponentKind::CommitCoordinator => {
                            v1::HealthComponentKind::CommitCoordinator
                        }
                        HealthComponentKind::Projection => v1::HealthComponentKind::Projection,
                        HealthComponentKind::Outbox => v1::HealthComponentKind::Outbox,
                    };
                    let status = match component.status() {
                        HealthComponentStatus::Healthy => v1::HealthComponentStatus::Healthy,
                        HealthComponentStatus::Degraded => v1::HealthComponentStatus::Degraded,
                        HealthComponentStatus::Unavailable => {
                            v1::HealthComponentStatus::Unavailable
                        }
                    };
                    v1::HealthComponent {
                        component: kind as i32,
                        status: status as i32,
                    }
                })
                .collect();
            let started_at = report.started_at();
            let build = report.build();
            v1::health_response::Result::Authenticated(v1::AuthenticatedHealth {
                status: status as i32,
                active_contract_version: report.active_contract_version().map(ContractVersion::get),
                last_commit_sequence: report.last_commit_sequence().map(CommitSequence::get),
                components,
                started_at: Some(v1::Timestamp {
                    seconds: started_at.seconds(),
                    nanos: started_at.nanoseconds(),
                }),
                build: Some(v1::BuildInfo {
                    semantic_version: build.semantic_version().to_owned(),
                    git_revision: build.git_revision().to_owned(),
                    rust_version: build.rust_version().to_owned(),
                    enabled_features: build.enabled_features().to_vec(),
                    storage_format_version: build.storage_format_version(),
                    contract_ir_version: build.contract_ir_version(),
                    mcp_protocol_baseline: build.mcp_protocol_baseline().to_owned(),
                }),
            })
        }
    };
    v1::HealthResponse {
        result: Some(result),
    }
}

/// Converts fixed authenticated statistics without adding extensible counters.
#[must_use]
pub fn statistics_result_to_proto(result: StatisticsResult) -> v1::StatsResponse {
    v1::StatsResponse {
        active_cursors: result.active_cursors(),
        active_commit_subscribers: u32::from(result.active_commit_subscribers()),
        last_commit_sequence: result.last_commit_sequence().map(CommitSequence::get),
        pending_outbox_deliveries: result.pending_outbox_deliveries(),
        known_projections: result.known_projections(),
    }
}

/// Converts an exact normal capability-create request using trusted server scope.
pub fn normal_create_capability_request_from_proto(
    request: v1::CreateCapabilityRequest,
    authentication: &AuthenticationContext,
) -> Result<(RequestId, NormalCreateCapabilityRequest), Status> {
    let parts = capability_create_parts(request, v1::CapabilityCreateMode::Normal)?;
    let request = NormalCreateCapabilityRequest::from_parts(
        parts.capability_id,
        authentication.database_id(),
        authentication.environment().clone(),
        parts.principal_id,
        parts.actor_kind,
        parts.requested_lifetime_seconds,
        parts.audiences,
        parts.grant,
    )
    .map_err(|_| invalid_request())?;
    Ok((parts.request_id, request))
}

/// Converts an exact bootstrap create request using the same trusted server scope.
pub fn bootstrap_capability_request_from_proto(
    request: v1::CreateCapabilityRequest,
    authentication: &AuthenticationContext,
) -> Result<(RequestId, BootstrapCapabilityRequest), Status> {
    let parts = capability_create_parts(request, v1::CapabilityCreateMode::Bootstrap)?;
    let request = BootstrapCapabilityRequest::from_parts(
        parts.capability_id,
        authentication.database_id(),
        authentication.environment().clone(),
        parts.principal_id,
        parts.actor_kind,
        parts.requested_lifetime_seconds,
        parts.audiences,
        parts.grant,
    )
    .map_err(|_| invalid_request())?;
    Ok((parts.request_id, request))
}

/// Converts one complete public grant into the canonical shared value owner.
pub fn capability_grant_from_proto(
    grant: v1::CapabilityGrant,
) -> Result<CapabilityGrantV1, Status> {
    let tenant_scope = tenant_scope_from_proto(grant.tenant_scope.ok_or_else(invalid_request)?)?;
    let partition_scope =
        partition_scope_from_proto(grant.partition_scope.ok_or_else(invalid_request)?)?;
    let permissions = grant
        .permissions
        .into_iter()
        .map(capability_permission_from_proto)
        .collect::<Result<Vec<_>, _>>()?;
    let permissions = CapabilityPermissionsV1::new(permissions).map_err(|_| invalid_request())?;
    let field_visibility = grant
        .field_visibility
        .into_iter()
        .map(|visibility| {
            let lineage =
                ContractLineage::new(visibility.contract_lineage).map_err(|_| invalid_request())?;
            let entity_type =
                EntityTypeId::new(visibility.entity_type_id).ok_or_else(invalid_request)?;
            let fields = visibility
                .field_ids
                .into_iter()
                .map(|field| FieldId::new(field).ok_or_else(invalid_request))
                .collect::<Result<Vec<_>, _>>()?;
            EntityFieldVisibilityV1::new(lineage, entity_type, fields)
                .map_err(|_| invalid_request())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let max_scan_rows =
        NonZeroU16::new(u16::try_from(grant.max_scan_rows).map_err(|_| invalid_request())?)
            .ok_or_else(invalid_request)?;
    let approval_required = grant
        .approval_required
        .into_iter()
        .map(capability_permission_kind_from_proto)
        .collect::<Result<Vec<_>, _>>()?;
    CapabilityGrantV1::new(
        tenant_scope,
        partition_scope,
        permissions,
        field_visibility,
        max_scan_rows,
        approval_required,
    )
    .map_err(|_| invalid_request())
}

/// Converts all closed normal and bootstrap capability-create results.
pub fn create_capability_result_to_proto(
    result: &CreateCapabilityResult,
) -> Result<v1::CreateCapabilityResponse, Status> {
    let result = match result {
        CreateCapabilityResult::Normal(result) => {
            let result = match result {
                NormalCreateCapabilityResult::Created { transition, token } => {
                    let token = std::str::from_utf8(token.expose_secret())
                        .map_err(|_| invalid_service_response())?
                        .to_owned();
                    v1::normal_create_capability_result::Result::Created(
                        v1::NormalCapabilityCreated {
                            transition: Some(capability_transition_to_proto(*transition)),
                            token,
                        },
                    )
                }
                NormalCreateCapabilityResult::AlreadyCreatedTokenUnavailable(identity) => {
                    v1::normal_create_capability_result::Result::AlreadyCreatedTokenUnavailable(
                        capability_identity_to_proto(*identity),
                    )
                }
                NormalCreateCapabilityResult::CapabilityIdConflict => {
                    v1::normal_create_capability_result::Result::CapabilityIdConflict(v1::Unit {})
                }
            };
            v1::create_capability_response::Result::Normal(v1::NormalCreateCapabilityResult {
                result: Some(result),
            })
        }
        CreateCapabilityResult::Bootstrap(result) => {
            let result = match result {
                BootstrapCapabilityResult::Created(transition) => {
                    v1::bootstrap_create_capability_result::Result::Created(
                        capability_transition_to_proto(*transition),
                    )
                }
                BootstrapCapabilityResult::Replayed(transition) => {
                    v1::bootstrap_create_capability_result::Result::Replayed(
                        capability_transition_to_proto(*transition),
                    )
                }
                BootstrapCapabilityResult::BootstrapConflict => {
                    v1::bootstrap_create_capability_result::Result::BootstrapConflict(v1::Unit {})
                }
            };
            v1::create_capability_response::Result::Bootstrap(v1::BootstrapCreateCapabilityResult {
                result: Some(result),
            })
        }
    };
    Ok(v1::CreateCapabilityResponse {
        result: Some(result),
    })
}

/// Converts one exact capability-revocation request.
pub fn revoke_capability_request_from_proto(
    request: v1::RevokeCapabilityRequest,
) -> Result<(RequestId, RevokeCapabilityRequest), Status> {
    let request_id = request_id_from_bytes(&request.request_id)?;
    let capability_id = capability_id_from_bytes(&request.capability_id)?;
    let reason = match v1::RevocationReason::try_from(request.reason)
        .map_err(|_| invalid_request())?
    {
        v1::RevocationReason::Requested => RevocationReasonCodeV1::Requested,
        v1::RevocationReason::Replaced => RevocationReasonCodeV1::Replaced,
        v1::RevocationReason::SuspectedCompromise => RevocationReasonCodeV1::SuspectedCompromise,
        v1::RevocationReason::PolicyChange => RevocationReasonCodeV1::PolicyChange,
        v1::RevocationReason::Unspecified => return Err(invalid_request()),
    };
    Ok((
        request_id,
        RevokeCapabilityRequest::new(capability_id, reason),
    ))
}

/// Converts every closed revocation result without leaking target facts.
#[must_use]
pub fn revoke_capability_result_to_proto(
    result: RevokeCapabilityResult,
) -> v1::RevokeCapabilityResponse {
    let result = match result {
        RevokeCapabilityResult::Revoked(transition) => {
            v1::revoke_capability_response::Result::Revoked(capability_transition_to_proto(
                transition,
            ))
        }
        RevokeCapabilityResult::AlreadyRevoked(transition) => {
            v1::revoke_capability_response::Result::AlreadyRevoked(capability_transition_to_proto(
                transition,
            ))
        }
        RevokeCapabilityResult::CapabilityNotFound => {
            v1::revoke_capability_response::Result::CapabilityNotFound(v1::Unit {})
        }
    };
    v1::RevokeCapabilityResponse {
        result: Some(result),
    }
}

/// Converts immutable contract metadata mechanically.
#[must_use]
pub fn contract_descriptor_to_proto(descriptor: &ContractDescriptor) -> v1::ContractDescriptor {
    v1::ContractDescriptor {
        contract_lineage: descriptor.lineage().as_str().to_owned(),
        contract_version: descriptor.version().get(),
        bundle_hash: descriptor.bundle_hash().as_bytes().to_vec(),
        source_hash: descriptor.source_hash().as_bytes().to_vec(),
        plan_root_hash: descriptor.plan_root_hash().as_bytes().to_vec(),
    }
}

/// Converts one structurally checked public value without selecting a schema.
pub fn submitted_value_from_proto(value: v1::Value) -> Result<SubmittedValue, Status> {
    use v1::value::Kind;

    match value.kind.ok_or_else(invalid_request)? {
        Kind::NullValue(value) if value == v1::NullValue::NullValue as i32 => {
            Ok(SubmittedValue::Null)
        }
        Kind::NullValue(_) => Err(invalid_request()),
        Kind::BoolValue(value) => Ok(SubmittedValue::Bool(value)),
        Kind::I64Value(value) => Ok(SubmittedValue::I64(value)),
        Kind::U64Value(value) => Ok(SubmittedValue::U64(value)),
        Kind::DecimalValue(value) => submitted_decimal(&value).map(SubmittedValue::Decimal),
        Kind::MoneyValue(value) => {
            let currency =
                CurrencyCode::new(value.currency.as_bytes()).map_err(|_| invalid_request())?;
            let amount = value.amount.as_ref().ok_or_else(invalid_request)?;
            Ok(SubmittedValue::Money(SubmittedMoney::new(
                currency,
                submitted_decimal(amount)?,
            )))
        }
        Kind::StringValue(value) => SubmittedValue::string(value).map_err(|_| invalid_request()),
        Kind::BytesValue(value) => SubmittedValue::bytes(value).map_err(|_| invalid_request()),
        Kind::UuidValue(value) => Ok(SubmittedValue::Uuid(
            value.try_into().map_err(|_| invalid_request())?,
        )),
        Kind::DateValue(value) => Ok(SubmittedValue::Date(Date::new(value.days_since_unix_epoch))),
        Kind::TimestampValue(value) => Ok(SubmittedValue::Timestamp(
            Timestamp::new(value.seconds, value.nanos).map_err(|_| invalid_request())?,
        )),
        Kind::EnumValue(value) => {
            let name = (!value.name.is_empty())
                .then(|| SourceName::new(value.name))
                .transpose()
                .map_err(|_| invalid_request())?;
            Ok(SubmittedValue::Enum(SubmittedEnum::new(
                EnumTypeId::new(value.type_id).ok_or_else(invalid_request)?,
                EnumVariantId::new(value.variant_id).ok_or_else(invalid_request)?,
                name,
            )))
        }
        Kind::ListValue(value) => SubmittedValue::list(
            value
                .values
                .into_iter()
                .map(submitted_value_from_proto)
                .collect::<Result<Vec<_>, _>>()?,
        )
        .map_err(|_| invalid_request()),
        Kind::RecordValue(value) => submitted_record_from_proto(value).map(SubmittedValue::Record),
    }
}

/// Converts one public record while preserving unresolved names and IDs.
pub fn submitted_record_from_proto(record: v1::ValueRecord) -> Result<SubmittedRecord, Status> {
    let fields = record
        .fields
        .into_iter()
        .map(|field| {
            let id = field
                .field_id
                .map(|id| FieldId::new(id).ok_or_else(invalid_request))
                .transpose()?;
            let name = (!field.name.is_empty())
                .then(|| SourceName::new(field.name))
                .transpose()
                .map_err(|_| invalid_request())?;
            let identity = match (id, name) {
                (Some(id), Some(name)) => SubmittedFieldIdentity::IdAndName { id, name },
                (Some(id), None) => SubmittedFieldIdentity::Id(id),
                (None, Some(name)) => SubmittedFieldIdentity::Name(name),
                (None, None) => return Err(invalid_request()),
            };
            let value = submitted_value_from_proto(field.value.ok_or_else(invalid_request)?)?;
            Ok(SubmittedField::new(identity, value))
        })
        .collect::<Result<Vec<_>, Status>>()?;
    SubmittedRecord::new(fields).map_err(|_| invalid_request())
}

/// Converts a canonical service value to its already validated public form.
pub fn canonical_value_to_public(
    value: &riffdb_types::CanonicalValue,
) -> Result<v1::Value, Status> {
    canonical_value_to_proto(value).map_err(|_| invalid_service_response())
}

/// Converts a canonical record to the public record message without reordering it.
pub fn canonical_record_to_public(
    record: &riffdb_types::CanonicalRecord,
) -> Result<v1::ValueRecord, Status> {
    let wire = canonical_value_to_public(&riffdb_types::CanonicalValue::Record(record.clone()))?;
    match wire.kind {
        Some(v1::value::Kind::RecordValue(record)) => Ok(record),
        _ => Err(invalid_service_response()),
    }
}

fn record_as_public_value(record: &riffdb_types::CanonicalRecord) -> Result<v1::Value, Status> {
    Ok(v1::Value {
        kind: Some(v1::value::Kind::RecordValue(canonical_record_to_public(
            record,
        )?)),
    })
}

/// Converts one exact application frontier, preserving the before-first sentinel.
#[must_use]
pub fn frontier_to_proto(frontier: FrontierPosition) -> v1::FrontierPosition {
    let position = match frontier {
        FrontierPosition::BeforeFirst => v1::frontier_position::Position::BeforeFirst(v1::Unit {}),
        FrontierPosition::AppliedThrough(sequence) => {
            v1::frontier_position::Position::AppliedThrough(sequence.get())
        }
    };
    v1::FrontierPosition {
        position: Some(position),
    }
}

/// Converts bounded explicit nanoseconds into a platform-independent duration.
#[must_use]
pub const fn duration_from_nanos(nanos: u64) -> Duration {
    Duration::from_nanos(nanos)
}

/// Converts an optional nonzero sequence without interpreting zero as before-first.
pub fn optional_sequence(value: Option<u64>) -> Result<Option<CommitSequence>, Status> {
    value
        .map(|value| CommitSequence::new(value).ok_or_else(invalid_request))
        .transpose()
}

/// Constructs the one static invalid-request status used by conversion failures.
#[must_use]
pub fn invalid_request() -> Status {
    Status::invalid_argument(INVALID_REQUEST_MESSAGE)
}

/// Constructs the one static internal status used for impossible service output.
#[must_use]
pub fn invalid_service_response() -> Status {
    Status::internal(crate::EMERGENCY_INTERNAL_MESSAGE)
}

struct CapabilityCreateParts {
    request_id: RequestId,
    capability_id: CapabilityId,
    principal_id: ActorId,
    actor_kind: ActorKind,
    requested_lifetime_seconds: NonZeroU32,
    audiences: Vec<Audience>,
    grant: CapabilityGrantV1,
}

fn capability_create_parts(
    request: v1::CreateCapabilityRequest,
    expected_mode: v1::CapabilityCreateMode,
) -> Result<CapabilityCreateParts, Status> {
    let mode = v1::CapabilityCreateMode::try_from(request.mode).map_err(|_| invalid_request())?;
    if mode != expected_mode || mode == v1::CapabilityCreateMode::Unspecified {
        return Err(invalid_request());
    }
    let audiences = request
        .audiences
        .into_iter()
        .map(|audience| Audience::new(audience).map_err(|_| invalid_request()))
        .collect::<Result<Vec<_>, _>>()?;
    let actor_kind =
        match v1::ActorKind::try_from(request.actor_kind).map_err(|_| invalid_request())? {
            v1::ActorKind::Human => ActorKind::Human,
            v1::ActorKind::Agent => ActorKind::Agent,
            v1::ActorKind::Service => ActorKind::Service,
            v1::ActorKind::Unspecified => return Err(invalid_request()),
        };
    Ok(CapabilityCreateParts {
        request_id: request_id_from_bytes(&request.request_id)?,
        capability_id: capability_id_from_bytes(&request.capability_id)?,
        principal_id: ActorId::new(request.principal_id).map_err(|_| invalid_request())?,
        actor_kind,
        requested_lifetime_seconds: NonZeroU32::new(request.requested_lifetime_seconds)
            .ok_or_else(invalid_request)?,
        audiences,
        grant: capability_grant_from_proto(request.grant.ok_or_else(invalid_request)?)?,
    })
}

fn capability_id_from_bytes(bytes: &[u8]) -> Result<CapabilityId, Status> {
    let bytes: [u8; 16] = bytes.try_into().map_err(|_| invalid_request())?;
    CapabilityId::from_bytes(bytes).map_err(|_| invalid_request())
}

fn tenant_scope_from_proto(scope: v1::TenantScope) -> Result<TenantScope, Status> {
    match scope.scope.ok_or_else(invalid_request)? {
        v1::tenant_scope::Scope::Global(_) => Ok(TenantScope::Global),
        v1::tenant_scope::Scope::TenantId(tenant) => TenantId::new(tenant)
            .map(TenantScope::Tenant)
            .map_err(|_| invalid_request()),
    }
}

fn partition_scope_from_proto(scope: v1::PartitionScope) -> Result<PartitionScopeV1, Status> {
    match scope.scope.ok_or_else(invalid_request)? {
        v1::partition_scope::Scope::All(_) => Ok(PartitionScopeV1::All),
        v1::partition_scope::Scope::Explicit(explicit) => {
            let partitions = explicit
                .partitions
                .into_iter()
                .map(|partition| {
                    let lineage = ContractLineage::new(partition.contract_lineage)
                        .map_err(|_| invalid_request())?;
                    let key = PartitionKey::from_bytes(partition.partition_key)
                        .map_err(|_| invalid_request())?;
                    Ok(ScopedPartitionV1::new(lineage, key))
                })
                .collect::<Result<Vec<_>, Status>>()?;
            PartitionScopeV1::explicit(partitions).map_err(|_| invalid_request())
        }
    }
}

fn capability_permission_from_proto(
    permission: v1::CapabilityPermission,
) -> Result<CapabilityPermissionV1, Status> {
    use v1::capability_permission::Permission;

    let permission = permission.permission.ok_or_else(invalid_request)?;
    let unparameterized =
        |kind| CapabilityPermissionV1::unparameterized(kind).map_err(|_| invalid_request());
    match permission {
        Permission::ValidateContract(_) => {
            unparameterized(CapabilityPermissionKindV1::ValidateContract)
        }
        Permission::ReadContract(_) => unparameterized(CapabilityPermissionKindV1::ReadContract),
        Permission::ExplainCommand(value) => {
            let (lineage, id) = lineage_scoped_id(value)?;
            Ok(CapabilityPermissionV1::ExplainCommand(
                lineage,
                CommandId::new(id).ok_or_else(invalid_request)?,
            ))
        }
        Permission::DeployContract(_) => {
            unparameterized(CapabilityPermissionKindV1::DeployContract)
        }
        Permission::InvokeCommand(value) => {
            let (lineage, id) = lineage_scoped_id(value)?;
            Ok(CapabilityPermissionV1::InvokeCommand(
                lineage,
                CommandId::new(id).ok_or_else(invalid_request)?,
            ))
        }
        Permission::ReadEntity(value) => {
            let (lineage, id) = lineage_scoped_id(value)?;
            Ok(CapabilityPermissionV1::ReadEntity(
                lineage,
                EntityTypeId::new(id).ok_or_else(invalid_request)?,
            ))
        }
        Permission::ScanIndex(value) => {
            let (lineage, id) = lineage_scoped_id(value)?;
            Ok(CapabilityPermissionV1::ScanIndex(
                lineage,
                IndexId::new(id).ok_or_else(invalid_request)?,
            ))
        }
        Permission::QueryProjection(value) => {
            let (lineage, id) = lineage_scoped_id(value)?;
            Ok(CapabilityPermissionV1::QueryProjection(
                lineage,
                ProjectionId::new(id).ok_or_else(invalid_request)?,
            ))
        }
        Permission::ReadProjectionStatus(value) => {
            let (lineage, id) = lineage_scoped_id(value)?;
            Ok(CapabilityPermissionV1::ReadProjectionStatus(
                lineage,
                ProjectionId::new(id).ok_or_else(invalid_request)?,
            ))
        }
        Permission::ReadCommit(_) => unparameterized(CapabilityPermissionKindV1::ReadCommit),
        Permission::ScanCommits(_) => unparameterized(CapabilityPermissionKindV1::ScanCommits),
        Permission::SubscribeCommits(_) => {
            unparameterized(CapabilityPermissionKindV1::SubscribeCommits)
        }
        Permission::ReadProvenance(_) => {
            unparameterized(CapabilityPermissionKindV1::ReadProvenance)
        }
        Permission::InspectOutbox(_) => unparameterized(CapabilityPermissionKindV1::InspectOutbox),
        Permission::ReadHealth(_) => unparameterized(CapabilityPermissionKindV1::ReadHealth),
        Permission::ReadStatistics(_) => {
            unparameterized(CapabilityPermissionKindV1::ReadStatistics)
        }
        Permission::CreateCapability(_) => {
            unparameterized(CapabilityPermissionKindV1::CreateCapability)
        }
        Permission::RevokeCapability(_) => {
            unparameterized(CapabilityPermissionKindV1::RevokeCapability)
        }
        Permission::AdministerCapabilities(_) => {
            unparameterized(CapabilityPermissionKindV1::AdministerCapabilities)
        }
    }
}

fn lineage_scoped_id(value: v1::LineageScopedStableId) -> Result<(ContractLineage, u32), Status> {
    Ok((
        ContractLineage::new(value.contract_lineage).map_err(|_| invalid_request())?,
        value.stable_id,
    ))
}

fn capability_permission_kind_from_proto(value: i32) -> Result<CapabilityPermissionKindV1, Status> {
    match v1::CapabilityPermissionKind::try_from(value).map_err(|_| invalid_request())? {
        v1::CapabilityPermissionKind::ValidateContract => {
            Ok(CapabilityPermissionKindV1::ValidateContract)
        }
        v1::CapabilityPermissionKind::ReadContract => Ok(CapabilityPermissionKindV1::ReadContract),
        v1::CapabilityPermissionKind::ExplainCommand => {
            Ok(CapabilityPermissionKindV1::ExplainCommand)
        }
        v1::CapabilityPermissionKind::DeployContract => {
            Ok(CapabilityPermissionKindV1::DeployContract)
        }
        v1::CapabilityPermissionKind::InvokeCommand => {
            Ok(CapabilityPermissionKindV1::InvokeCommand)
        }
        v1::CapabilityPermissionKind::ReadEntity => Ok(CapabilityPermissionKindV1::ReadEntity),
        v1::CapabilityPermissionKind::ScanIndex => Ok(CapabilityPermissionKindV1::ScanIndex),
        v1::CapabilityPermissionKind::QueryProjection => {
            Ok(CapabilityPermissionKindV1::QueryProjection)
        }
        v1::CapabilityPermissionKind::ReadProjectionStatus => {
            Ok(CapabilityPermissionKindV1::ReadProjectionStatus)
        }
        v1::CapabilityPermissionKind::ReadCommit => Ok(CapabilityPermissionKindV1::ReadCommit),
        v1::CapabilityPermissionKind::ScanCommits => Ok(CapabilityPermissionKindV1::ScanCommits),
        v1::CapabilityPermissionKind::SubscribeCommits => {
            Ok(CapabilityPermissionKindV1::SubscribeCommits)
        }
        v1::CapabilityPermissionKind::ReadProvenance => {
            Ok(CapabilityPermissionKindV1::ReadProvenance)
        }
        v1::CapabilityPermissionKind::InspectOutbox => {
            Ok(CapabilityPermissionKindV1::InspectOutbox)
        }
        v1::CapabilityPermissionKind::ReadHealth => Ok(CapabilityPermissionKindV1::ReadHealth),
        v1::CapabilityPermissionKind::ReadStatistics => {
            Ok(CapabilityPermissionKindV1::ReadStatistics)
        }
        v1::CapabilityPermissionKind::CreateCapability => {
            Ok(CapabilityPermissionKindV1::CreateCapability)
        }
        v1::CapabilityPermissionKind::RevokeCapability => {
            Ok(CapabilityPermissionKindV1::RevokeCapability)
        }
        v1::CapabilityPermissionKind::AdministerCapabilities => {
            Ok(CapabilityPermissionKindV1::AdministerCapabilities)
        }
        v1::CapabilityPermissionKind::Unspecified => Err(invalid_request()),
    }
}

fn capability_identity_to_proto(identity: CapabilityIdentityView) -> v1::CapabilityIdentity {
    v1::CapabilityIdentity {
        capability_id: identity.capability_id().as_bytes().to_vec(),
        revision: identity.revision().get(),
    }
}

fn capability_transition_to_proto(
    transition: CapabilityTransitionView,
) -> v1::CapabilityTransition {
    v1::CapabilityTransition {
        identity: Some(capability_identity_to_proto(transition.identity())),
        administration_sequence: transition.administration_sequence().get(),
    }
}

fn submitted_decimal(value: &v1::Decimal) -> Result<SubmittedDecimal, Status> {
    let scale = u8::try_from(value.scale).map_err(|_| invalid_request())?;
    SubmittedDecimal::from_minimal_twos_complement(&value.coefficient_twos_complement, scale)
        .map_err(|_| invalid_request())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request_id() -> RequestId {
        RequestId::from_unix_milliseconds_and_random(1, [7; 10]).expect("valid request ID")
    }

    fn active_contract() -> v1::ContractSelection {
        v1::ContractSelection {
            selection: Some(v1::contract_selection::Selection::Active(v1::Unit {})),
        }
    }

    fn first_page() -> v1::PageRequest {
        v1::PageRequest {
            limit: Some(5),
            cursor: None,
        }
    }

    #[test]
    fn submitted_record_preserves_redundant_identity_until_service_resolution() {
        let record = v1::ValueRecord {
            fields: vec![v1::ValueField {
                field_id: Some(7),
                name: "amount".to_owned(),
                value: Some(v1::Value {
                    kind: Some(v1::value::Kind::U64Value(42)),
                }),
            }],
        };
        let submitted = submitted_record_from_proto(record).expect("valid submitted record");
        assert_eq!(
            submitted.fields()[0]
                .identity()
                .field_id()
                .map(FieldId::get),
            Some(7)
        );
        assert_eq!(
            submitted.fields()[0]
                .identity()
                .name()
                .map(SourceName::as_str),
            Some("amount")
        );
    }

    #[test]
    fn decimal_conversion_does_not_invent_precision() {
        let submitted = submitted_value_from_proto(v1::Value {
            kind: Some(v1::value::Kind::DecimalValue(v1::Decimal {
                coefficient_twos_complement: vec![123],
                scale: 2,
            })),
        })
        .expect("structurally valid decimal");
        let SubmittedValue::Decimal(decimal) = submitted else {
            panic!("expected submitted decimal")
        };
        assert_eq!(decimal.coefficient(), 123);
        assert_eq!(decimal.scale(), 2);
    }

    #[test]
    fn scan_index_preserves_submitted_decimal_until_service_materialization() {
        let expected_request_id = request_id();
        let (actual_request_id, request) = scan_index_request_from_proto(v1::ScanIndexRequest {
            request_id: expected_request_id.as_bytes().to_vec(),
            contract: Some(active_contract()),
            index_id: 3,
            leading_components: vec![v1::Value {
                kind: Some(v1::value::Kind::DecimalValue(v1::Decimal {
                    coefficient_twos_complement: vec![123],
                    scale: 2,
                })),
            }],
            fields: Some(v1::FieldSelection { field_ids: vec![1] }),
            page: Some(first_page()),
        })
        .expect("structurally valid index scan");

        assert_eq!(actual_request_id, expected_request_id);
        let SubmittedValue::Decimal(decimal) = &request.leading_components()[0] else {
            panic!("expected submitted decimal")
        };
        assert_eq!(decimal.coefficient(), 123);
        assert_eq!(decimal.scale(), 2);
    }

    #[test]
    fn projection_query_preserves_submitted_enum_name_until_service_materialization() {
        let expected_request_id = request_id();
        let (actual_request_id, request) =
            query_projection_request_from_proto(v1::QueryProjectionRequest {
                request_id: expected_request_id.as_bytes().to_vec(),
                contract: Some(active_contract()),
                projection_id: 4,
                leading_components: vec![v1::Value {
                    kind: Some(v1::value::Kind::EnumValue(v1::EnumValue {
                        type_id: 5,
                        variant_id: 6,
                        name: "approved".to_owned(),
                    })),
                }],
                required_sequence: None,
                wait_nanos: 0,
                page: Some(first_page()),
            })
            .expect("structurally valid projection query");

        assert_eq!(actual_request_id, expected_request_id);
        let SubmittedValue::Enum(value) = &request.leading_components()[0] else {
            panic!("expected submitted enum")
        };
        assert_eq!(value.type_id().get(), 5);
        assert_eq!(value.variant_id().get(), 6);
        assert_eq!(value.name().map(SourceName::as_str), Some("approved"));
    }

    #[test]
    fn index_scan_fence_preserves_before_first_and_assigned_epoch_positions() {
        let before_first = riffdb_service::Page::new(
            PageLimit::default(),
            Vec::new(),
            None,
            riffdb_service::IndexScanFence::new(IndexEpochPosition::BeforeFirst),
        )
        .expect("valid empty page");
        let before_first = scan_index_result_to_proto(&ScanIndexResult::new(before_first))
            .expect("convert before-first fence");
        assert!(matches!(
            before_first
                .page
                .and_then(|page| page.observed_fence)
                .and_then(|fence| fence.position),
            Some(v1::index_scan_fence::Position::BeforeFirst(_))
        ));

        let epoch = riffdb_types::IndexEpoch::new(9).expect("nonzero epoch");
        let assigned = riffdb_service::Page::new(
            PageLimit::default(),
            Vec::new(),
            None,
            riffdb_service::IndexScanFence::new(IndexEpochPosition::Value(epoch)),
        )
        .expect("valid empty page");
        let assigned = scan_index_result_to_proto(&ScanIndexResult::new(assigned))
            .expect("convert assigned fence");
        assert_eq!(
            assigned
                .page
                .and_then(|page| page.observed_fence)
                .and_then(|fence| fence.position),
            Some(v1::index_scan_fence::Position::AppliedEpoch(9))
        );
    }

    #[test]
    fn request_ids_are_not_repaired_or_substituted() {
        assert!(request_id_from_bytes(&[0; 16]).is_err());
        assert!(request_id_from_bytes(&[0; 15]).is_err());
    }

    #[test]
    fn invalid_explicit_field_id_is_not_treated_as_absent() {
        let record = v1::ValueRecord {
            fields: vec![v1::ValueField {
                field_id: Some(0),
                name: "amount".to_owned(),
                value: Some(v1::Value {
                    kind: Some(v1::value::Kind::U64Value(42)),
                }),
            }],
        };
        assert!(submitted_record_from_proto(record).is_err());
    }

    #[test]
    fn impossible_service_conversion_uses_only_emergency_internal_framing() {
        let status = invalid_service_response();
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), crate::EMERGENCY_INTERNAL_MESSAGE);
        assert!(status.details().is_empty());
    }
}
