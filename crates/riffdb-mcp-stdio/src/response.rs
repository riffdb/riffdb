//! Checked public-Protobuf to common MCP presentation conversion.

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use riffdb_api_mcp::{
    McpBindingFieldReferencePresentation, McpCommandDurability, McpCommandExecutionClass,
    McpCommandExplanationFields, McpCommandExplanationPresentation, McpCompatibilityCodeCount,
    McpContractCompatibilityClass, McpContractCompatibilityPresentation,
    McpContractDescriptorPresentation, McpDynamicCommandCompletion,
    McpExplainedCommandPresentation, McpFixedResultBranch, McpFixedResultPayload,
    McpFrontierPresentation, McpGeneratedSchemaKind, McpJournaledCommandResultParts,
    McpJournaledCommandStatus, McpNaturalOutcome, McpPresentedBytes, McpPresentedField,
    McpPresentedHash, McpPresentedI64, McpPresentedTimestamp, McpPresentedU64, McpPresentedUuid,
    McpPresentedValue, McpProjectionFailureCode, McpProjectionFailurePresentation,
    McpProjectionGenerationFrontierPresentation, McpProjectionIdentityPresentation,
    McpProjectionLifecycle, McpProjectionStatusParts, McpProjectionStatusPresentation,
    McpPublishedApplyMode, McpReadOnlyCommandResultParts, McpResourceJson, McpSchemaBoundField,
    McpSchemaBoundValue, McpToolResult, McpVisibleFingerprint, SchemaDocument,
    compose_dynamic_command_result, compose_fixed_tool_result, encode_mcp_cursor,
    render_active_contract_resource, render_command_documentation, render_command_plan_resource,
    render_contract_version_resource, render_projection_status_resource,
};
use riffdb_client_rust::{app_v1, v1};
use serde::Serialize;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ResponseConversionError;

#[derive(Serialize)]
struct Unit {}

pub(crate) fn event_next(
    response: v1::ConsumeEventStreamResponse,
) -> Result<McpToolResult, ResponseConversionError> {
    let status =
        consumer_public_status(response.status.as_ref(), response.protected_status.as_ref())?;
    let disposition = consumer_disposition(response.disposition)?;
    let events = response
        .events
        .into_iter()
        .map(|delivery| {
            let event = delivery.event.ok_or(ResponseConversionError)?;
            let event_id = event.event_id.ok_or(ResponseConversionError)?;
            let expires = delivery.expires_at.ok_or(ResponseConversionError)?;
            let fields = event
                .fields
                .into_iter()
                .map(|field| {
                    Ok(serde_json::json!({
                        "name": field.name,
                        "value": presented_value(field.value.ok_or(ResponseConversionError)?)?,
                    }))
                })
                .collect::<Result<Vec<_>, ResponseConversionError>>()?;
            Ok(serde_json::json!({
                "event_id": format!("{}:{}", event_id.commit_sequence, event_id.event_ordinal),
                "event_name": event.event_name,
                "writer_contract_version": event.writer_contract_version.to_string(),
                "command_name": event.command_name,
                "actor_kind": actor_kind_name(event.actor_kind)?,
                "provenance_uri": event.provenance_uri,
                "history_incarnation": event.history_incarnation.to_string(),
                "fields": fields,
                "attempt": delivery.attempt,
                "lease_token": lower_hex(&delivery.lease_token),
                "expires_at": {"seconds": expires.seconds.to_string(), "nanos": expires.nanos},
            }))
        })
        .collect::<Result<Vec<_>, ResponseConversionError>>()?;
    compose(
        20,
        McpFixedResultBranch::EventNextCompleted,
        Some(payload_from(&serde_json::json!({
            "events": events,
            "status": status,
            "wait_timed_out": response.wait_timed_out,
            "disposition": disposition,
        }))?),
    )
}

pub(crate) fn event_mutation(
    tag: u8,
    response: v1::EventConsumerMutationResponse,
) -> Result<McpToolResult, ResponseConversionError> {
    let result = match v1::EventConsumerMutationResult::try_from(response.result)
        .map_err(|_| ResponseConversionError)?
    {
        v1::EventConsumerMutationResult::Applied => "applied",
        v1::EventConsumerMutationResult::StateChanged => "state_changed",
        v1::EventConsumerMutationResult::NotFound => "not_found",
        v1::EventConsumerMutationResult::OutstandingLease => "outstanding_lease",
        v1::EventConsumerMutationResult::StaleLease => "stale_lease",
        v1::EventConsumerMutationResult::LeaseExpired => "lease_expired",
        v1::EventConsumerMutationResult::Unspecified => return Err(ResponseConversionError),
    };
    let branch = match tag {
        21 => McpFixedResultBranch::EventAckCompleted,
        22 => McpFixedResultBranch::EventNackCompleted,
        23 => McpFixedResultBranch::EventSeekCompleted,
        27 => McpFixedResultBranch::ContextualAckCompleted,
        28 => McpFixedResultBranch::ContextualNackCompleted,
        _ => return Err(ResponseConversionError),
    };
    compose(
        tag,
        branch,
        Some(payload_from(&serde_json::json!({"result": result}))?),
    )
}

pub(crate) fn event_status(
    response: v1::GetEventStreamConsumerStatusResponse,
) -> Result<McpToolResult, ResponseConversionError> {
    use v1::get_event_stream_consumer_status_response::Result;
    let payload = match response.result.ok_or(ResponseConversionError)? {
        Result::NotFound(_) => serde_json::json!({"found": false}),
        Result::Found(status) => {
            serde_json::json!({"found": true, "status": consumer_status(&status)?})
        }
        Result::Protected(status) => serde_json::json!({
            "found": true,
            "status": consumer_public_status(None, Some(&status))?,
        }),
    };
    compose(
        24,
        McpFixedResultBranch::EventStatusCompleted,
        Some(payload_from(&payload)?),
    )
}

pub(crate) fn contextual_status(
    response: v1::GetEventStreamConsumerStatusResponse,
) -> Result<McpToolResult, ResponseConversionError> {
    use v1::get_event_stream_consumer_status_response::Result;
    let payload = match response.result.ok_or(ResponseConversionError)? {
        Result::NotFound(_) => serde_json::json!({"found": false}),
        Result::Found(status) => {
            serde_json::json!({"found": true, "status": consumer_status(&status)?})
        }
        Result::Protected(status) => serde_json::json!({
            "found": true,
            "status": consumer_public_status(None, Some(&status))?,
        }),
    };
    compose(
        29,
        McpFixedResultBranch::ContextualStatusCompleted,
        Some(payload_from(&payload)?),
    )
}

pub(crate) fn contextual_next(
    response: v1::ConsumeContextualSubscriptionResponse,
) -> Result<McpToolResult, ResponseConversionError> {
    let status =
        consumer_public_status(response.status.as_ref(), response.protected_status.as_ref())?;
    let disposition = consumer_disposition(response.disposition)?;
    let items = response
        .items
        .into_iter()
        .map(|item| {
            let delivery = item.delivery.ok_or(ResponseConversionError)?;
            let event = delivery.event.ok_or(ResponseConversionError)?;
            let event_id = event.event_id.ok_or(ResponseConversionError)?;
            let expires = delivery.expires_at.ok_or(ResponseConversionError)?;
            let fields = event
                .fields
                .into_iter()
                .map(|field| {
                    Ok(serde_json::json!({
                        "name":field.name,
                        "value":presented_value(field.value.ok_or(ResponseConversionError)?)?,
                    }))
                })
                .collect::<Result<Vec<_>, ResponseConversionError>>()?;
            let hydrations = item
                .hydrations
                .into_iter()
                .map(|hydration| {
                    let fields = hydration
                        .fields
                        .into_iter()
                        .map(|field| {
                            let cardinality = match v1::ContextualQueryCardinality::try_from(field.cardinality).ok() {
                                Some(v1::ContextualQueryCardinality::One) => "one",
                                Some(v1::ContextualQueryCardinality::Maybe) => "maybe",
                                Some(v1::ContextualQueryCardinality::Many) => "many",
                                _ => return Err(ResponseConversionError),
                            };
                            let rows = field.rows.into_iter().map(|row| {
                                let values = row.fields.into_iter().map(|value| {
                                    Ok((value.name, serde_json::to_value(presented_value(value.value.ok_or(ResponseConversionError)?)?).map_err(|_| ResponseConversionError)?))
                                }).collect::<Result<serde_json::Map<_, _>, ResponseConversionError>>()?;
                                Ok(serde_json::json!({"entity":row.entity,"fields":values}))
                            }).collect::<Result<Vec<_>, ResponseConversionError>>()?;
                            Ok(serde_json::json!({"name":field.name,"cardinality":cardinality,"rows":rows}))
                        })
                        .collect::<Result<Vec<_>, ResponseConversionError>>()?;
                    Ok(serde_json::json!({"name":hydration.name,"outcome":hydration.outcome,"fields":fields}))
                })
                .collect::<Result<Vec<_>, ResponseConversionError>>()?;
            let reactions = item.available_reactions.into_iter().map(|reaction| serde_json::json!({
                "name":reaction.name,
                "command_name":reaction.command_name,
                "command_id":reaction.command_id,
                "causation_token":base64::engine::general_purpose::STANDARD.encode(reaction.causation_token),
            })).collect::<Vec<_>>();
            Ok(serde_json::json!({
                "event":{"event_id":format!("{}:{}",event_id.commit_sequence,event_id.event_ordinal),"event_name":event.event_name,"fields":fields},
                "attempt":delivery.attempt,"lease_token":lower_hex(&delivery.lease_token),
                "expires_at":{"seconds":expires.seconds.to_string(),"nanos":expires.nanos},
                "history_incarnation":event.history_incarnation.to_string(),
                "context_head":item.context_head.to_string(),"hydrations":hydrations,"available_reactions":reactions,
            }))
        })
        .collect::<Result<Vec<_>, ResponseConversionError>>()?;
    compose(
        26,
        McpFixedResultBranch::ContextualNextCompleted,
        Some(payload_from(&serde_json::json!({
            "items":items,"status":status,"wait_timed_out":response.wait_timed_out,
            "disposition":disposition,
        }))?),
    )
}

pub(crate) fn query_watch(
    update: v1::LiveQueryUpdate,
) -> Result<McpToolResult, ResponseConversionError> {
    use v1::live_query_update::Update;
    let payload = match update.update.ok_or(ResponseConversionError)? {
        Update::Snapshot(value) => serde_json::json!({
            "type":"snapshot", "result": live_result(value.result)?,
            "frontier": live_frontier(value.frontier)?, "cursor": McpPresentedBytes::new(value.cursor),
        }),
        Update::Patch(value) => serde_json::json!({
            "type":"patch", "result_field":value.result_field,
            "operations": value.operations.into_iter().map(live_patch_operation).collect::<Result<Vec<_>, _>>()?, "frontier": live_frontier(value.frontier)?,
            "cursor": McpPresentedBytes::new(value.cursor),
        }),
        Update::Reset(value) => serde_json::json!({
            "type":"reset", "reason":live_reset_reason(value.reason)?, "result":live_result(value.result)?,
            "frontier":live_frontier(value.frontier)?, "cursor":McpPresentedBytes::new(value.cursor),
        }),
        Update::Checkpoint(value) => serde_json::json!({
            "type":"checkpoint", "frontier":live_frontier(value.frontier)?,
            "cursor":McpPresentedBytes::new(value.cursor),
        }),
        Update::Terminal(value) => serde_json::json!({
            "type":"terminal", "reason":live_terminal_reason(value.reason)?,
            "last_frontier":live_frontier(value.last_frontier)?,
        }),
    };
    compose(
        25,
        McpFixedResultBranch::QueryWatchCompleted,
        Some(payload_from(&payload)?),
    )
}

fn consumer_status(
    status: &v1::EventConsumerStatus,
) -> Result<serde_json::Value, ResponseConversionError> {
    let checkpoint = status
        .checkpoint
        .as_ref()
        .and_then(|checkpoint| checkpoint.position.as_ref())
        .ok_or(ResponseConversionError)?;
    let checkpoint = match checkpoint {
        v1::event_consumer_checkpoint::Position::BeforeFirst(_) => "before-first".to_owned(),
        v1::event_consumer_checkpoint::Position::AfterEventId(value) => {
            format!("{}:{}", value.commit_sequence, value.event_ordinal)
        }
    };
    Ok(serde_json::json!({
        "revision":status.revision.to_string(), "checkpoint":checkpoint,
        "history_incarnation":status.history_incarnation.to_string(),
        "live_leases":status.live_leases, "retries":status.retries, "dead_letters":status.dead_letters,
    }))
}

fn consumer_public_status(
    exact: Option<&v1::EventConsumerStatus>,
    protected: Option<&v1::ProtectedEventConsumerStatus>,
) -> Result<serde_json::Value, ResponseConversionError> {
    match (exact, protected) {
        (Some(status), None) => consumer_status(status),
        (None, Some(status)) if status.history_incarnation != 0 => Ok(serde_json::json!({
            "kind":"protected",
            "history_incarnation":status.history_incarnation.to_string(),
            "progress_cursor":McpPresentedBytes::new(status.progress_cursor.clone()),
        })),
        _ => Err(ResponseConversionError),
    }
}

fn consumer_disposition(value: i32) -> Result<&'static str, ResponseConversionError> {
    match v1::EventConsumerPullDisposition::try_from(value).ok() {
        Some(v1::EventConsumerPullDisposition::Ready) => Ok("ready"),
        Some(v1::EventConsumerPullDisposition::WaitTimedOut) => Ok("wait_timed_out"),
        Some(v1::EventConsumerPullDisposition::BoundedProgress) => Ok("bounded_progress"),
        _ => Err(ResponseConversionError),
    }
}

fn live_frontier(
    frontier: Option<v1::LiveQueryFrontier>,
) -> Result<serde_json::Value, ResponseConversionError> {
    let value = frontier.ok_or(ResponseConversionError)?;
    Ok(serde_json::json!({
        "history_incarnation":value.history_incarnation.to_string(),
        "application_head":value.application_head.to_string(),
    }))
}

fn live_result(
    result: Option<v1::LiveQueryResult>,
) -> Result<serde_json::Value, ResponseConversionError> {
    let result = result.ok_or(ResponseConversionError)?;
    let identity = result.identity.ok_or(ResponseConversionError)?;
    let fields = result.fields.into_iter().map(|field| {
        let records = field.records.into_iter().map(|record| {
            let fields = record.fields.ok_or(ResponseConversionError)?.fields.into_iter().map(|field| {
                Ok((field.name, serde_json::to_value(presented_value(field.value.ok_or(ResponseConversionError)?)?).map_err(|_| ResponseConversionError)?))
            }).collect::<Result<serde_json::Map<_, _>, ResponseConversionError>>()?;
            Ok(serde_json::json!({"entity":record.entity, "fields":fields}))
        }).collect::<Result<Vec<_>, ResponseConversionError>>()?;
        Ok(serde_json::json!({"name":field.name, "cardinality":live_cardinality(field.cardinality)?, "records":records}))
    }).collect::<Result<Vec<_>, ResponseConversionError>>()?;
    Ok(serde_json::json!({
        "identity": {
            "contract_lineage": identity.contract_lineage,
            "contract_version": identity.contract_version.to_string(),
            "contract_bundle_hash": lower_hex(&identity.contract_bundle_hash),
            "query_name": identity.query_name,
            "query_module_hash": lower_hex(&identity.query_module_hash),
            "query_plan_hash": lower_hex(&identity.query_plan_hash),
        },
        "outcome":result.outcome, "fields":fields
    }))
}

fn live_patch_operation(
    value: v1::LiveQueryPatchOperation,
) -> Result<serde_json::Value, ResponseConversionError> {
    use v1::live_query_patch_operation::Operation;
    Ok(match value.operation.ok_or(ResponseConversionError)? {
        Operation::Insert(value) => serde_json::json!({
            "type":"insert", "index":value.index, "record":live_record(value.record)?,
        }),
        Operation::Remove(value) => serde_json::json!({
            "type":"remove", "index":value.index, "key":live_key(value.key)?,
        }),
        Operation::Replace(value) => serde_json::json!({
            "type":"replace", "index":value.index, "record":live_record(value.record)?,
        }),
        Operation::Move(value) => serde_json::json!({
            "type":"move", "from":value.from, "to":value.to, "key":live_key(value.key)?,
        }),
    })
}

fn live_record(
    value: Option<v1::LiveQueryResultRecord>,
) -> Result<serde_json::Value, ResponseConversionError> {
    let value = value.ok_or(ResponseConversionError)?;
    Ok(serde_json::json!({
        "entity": value.entity,
        "fields": live_key(value.fields)?,
    }))
}

fn live_key(value: Option<v1::ValueRecord>) -> Result<serde_json::Value, ResponseConversionError> {
    let value = value.ok_or(ResponseConversionError)?;
    value
        .fields
        .into_iter()
        .map(|field| {
            Ok(serde_json::json!({
                "name": field.name,
                "value": presented_value(field.value.ok_or(ResponseConversionError)?)?,
            }))
        })
        .collect::<Result<Vec<_>, ResponseConversionError>>()
        .map(serde_json::Value::Array)
}

fn live_cardinality(value: i32) -> Result<&'static str, ResponseConversionError> {
    match v1::LiveQueryResultCardinality::try_from(value).map_err(|_| ResponseConversionError)? {
        v1::LiveQueryResultCardinality::One => Ok("one"),
        v1::LiveQueryResultCardinality::Maybe => Ok("maybe"),
        v1::LiveQueryResultCardinality::Many => Ok("many"),
        v1::LiveQueryResultCardinality::Unspecified => Err(ResponseConversionError),
    }
}

fn live_reset_reason(value: i32) -> Result<&'static str, ResponseConversionError> {
    match v1::LiveQueryResetReason::try_from(value).map_err(|_| ResponseConversionError)? {
        v1::LiveQueryResetReason::OutcomeChanged => Ok("outcome_changed"),
        v1::LiveQueryResetReason::DiffLimitExceeded => Ok("diff_limit_exceeded"),
        v1::LiveQueryResetReason::DefinitionChanged => Ok("definition_changed"),
        v1::LiveQueryResetReason::HistoryChanged => Ok("history_changed"),
        v1::LiveQueryResetReason::CursorExpired => Ok("cursor_expired"),
        v1::LiveQueryResetReason::Unspecified => Err(ResponseConversionError),
    }
}

fn live_terminal_reason(value: i32) -> Result<&'static str, ResponseConversionError> {
    match v1::LiveQueryTerminalReason::try_from(value).map_err(|_| ResponseConversionError)? {
        v1::LiveQueryTerminalReason::AuthorizationChanged => Ok("authorization_changed"),
        v1::LiveQueryTerminalReason::BufferPressure => Ok("buffer_pressure"),
        v1::LiveQueryTerminalReason::LifetimeExpired => Ok("lifetime_expired"),
        v1::LiveQueryTerminalReason::ServiceUnavailable => Ok("service_unavailable"),
        v1::LiveQueryTerminalReason::IntegrityFailure => Ok("integrity_failure"),
        v1::LiveQueryTerminalReason::DefinitionChanged => Ok("definition_changed"),
        v1::LiveQueryTerminalReason::Unspecified => Err(ResponseConversionError),
    }
}

fn actor_kind_name(value: i32) -> Result<&'static str, ResponseConversionError> {
    match v1::ActorKind::try_from(value).map_err(|_| ResponseConversionError)? {
        v1::ActorKind::Human => Ok("human"),
        v1::ActorKind::Agent => Ok("agent"),
        v1::ActorKind::Service => Ok("service"),
        v1::ActorKind::Unspecified => Err(ResponseConversionError),
    }
}

pub(crate) fn validate_contract(
    response: v1::ValidateContractResponse,
) -> Result<McpToolResult, ResponseConversionError> {
    use v1::validate_contract_response::Result as ProtoResult;

    match response.result.ok_or(ResponseConversionError)? {
        ProtoResult::Valid(_) => compose(1, McpFixedResultBranch::ValidateValid, None),
        ProtoResult::Invalid(diagnostics) => {
            let payload = compilation_diagnostics(diagnostics)?;
            compose(
                1,
                McpFixedResultBranch::ValidateInvalid,
                Some(payload_from(&payload)?),
            )
        }
        ProtoResult::Candidate(_) => Err(ResponseConversionError),
    }
}

fn compilation_diagnostics(
    diagnostics: v1::CompilationDiagnostics,
) -> Result<InvalidDiagnostics, ResponseConversionError> {
    use v1::compilation_diagnostics::Diagnostics;
    match diagnostics.diagnostics.ok_or(ResponseConversionError)? {
        Diagnostics::Syntax(list) => Ok(InvalidDiagnostics::Syntax {
            diagnostics: list
                .diagnostics
                .into_iter()
                .map(SyntaxDiagnostic::try_from)
                .collect::<std::result::Result<Vec<_>, _>>()?,
        }),
        Diagnostics::Semantic(list) => Ok(InvalidDiagnostics::Semantic {
            diagnostics: list
                .diagnostics
                .into_iter()
                .map(SemanticDiagnostic::try_from)
                .collect::<std::result::Result<Vec<_>, _>>()?,
        }),
    }
}

pub(crate) fn get_active_contract(
    response: v1::GetActiveContractResponse,
) -> Result<McpToolResult, ResponseConversionError> {
    use v1::get_active_contract_response::Result;

    let database = response.database_alias;
    match response.result.ok_or(ResponseConversionError)? {
        Result::Absent(_) => compose(
            2,
            McpFixedResultBranch::GetActiveAbsent,
            Some(payload_from(&ActiveContractAbsent { database })?),
        ),
        Result::Present(contract) => compose(
            2,
            McpFixedResultBranch::GetActivePresent,
            Some(payload_from(&ActiveContractPresent {
                database,
                contract: ContractDescriptor::try_from(contract)?,
            })?),
        ),
    }
}

pub(crate) fn explain_command(
    response: v1::ExplainCommandResponse,
) -> Result<McpToolResult, ResponseConversionError> {
    use v1::explain_command_response::Result;

    match response.result.ok_or(ResponseConversionError)? {
        Result::NotFound(_) => compose(3, McpFixedResultBranch::ExplainNotFound, None),
        Result::Found(explained) => {
            let command_id = explained.command_id;
            let payload = ExplainedCommand {
                contract: ContractDescriptor::try_from(
                    explained.contract.ok_or(ResponseConversionError)?,
                )?,
                command_id,
                plan_hash: hash(&explained.plan_hash)?,
                explanation: CommandExplain::try_from(
                    explained.explanation.ok_or(ResponseConversionError)?,
                )?,
                input_schema: generated_schema(
                    explained.input_schema.ok_or(ResponseConversionError)?,
                    command_id,
                    GeneratedSchemaKeyKind::CommandInput,
                )?,
                outcome_schema: generated_schema(
                    explained.outcome_schema.ok_or(ResponseConversionError)?,
                    command_id,
                    GeneratedSchemaKeyKind::CommandOutcomeUnion,
                )?,
            };
            compose(
                3,
                McpFixedResultBranch::ExplainFound,
                Some(payload_from(&payload)?),
            )
        }
    }
}

pub(crate) fn deploy_contract(
    response: v1::DeployContractResponse,
) -> Result<McpToolResult, ResponseConversionError> {
    use v1::deploy_contract_response::Result;

    match response.result.ok_or(ResponseConversionError)? {
        Result::InvalidSource(diagnostics) => compose(
            4,
            McpFixedResultBranch::DeployInvalidSource,
            Some(payload_from(&compilation_diagnostics(diagnostics)?)?),
        ),
        Result::IncompatibleCandidate(contract) => compose(
            4,
            McpFixedResultBranch::DeployIncompatibleCandidate,
            Some(payload_from(&ContractDescriptor::try_from(contract)?)?),
        ),
        Result::MigrationRequired(contract) => compose(
            4,
            McpFixedResultBranch::DeployMigrationRequired,
            Some(payload_from(&ContractDescriptor::try_from(contract)?)?),
        ),
        Result::Activated(contract) => compose(
            4,
            McpFixedResultBranch::DeployActivated,
            Some(payload_from(&ContractDescriptor::try_from(contract)?)?),
        ),
        Result::AlreadyActive(contract) => compose(
            4,
            McpFixedResultBranch::DeployAlreadyActive,
            Some(payload_from(&ContractDescriptor::try_from(contract)?)?),
        ),
        Result::ExpectedActiveVersionMismatch(mismatch) => compose(
            4,
            McpFixedResultBranch::DeployExpectedActiveVersionMismatch,
            Some(payload_from(&ExpectedActiveVersionMismatch {
                actual_active_version: mismatch.actual_active_version.map(McpPresentedU64::new),
            })?),
        ),
        Result::BundleConflict(_) => compose(4, McpFixedResultBranch::DeployBundleConflict, None),
        Result::ExpectedApplicationIdentityMismatch(_) => Err(ResponseConversionError),
    }
}

pub(crate) fn get_outcome(
    response: v1::GetOutcomeResponse,
) -> Result<McpToolResult, ResponseConversionError> {
    use v1::get_outcome_response::Result;

    match response.result.ok_or(ResponseConversionError)? {
        Result::NotFound(_) => compose(5, McpFixedResultBranch::GetOutcomeNotFound, None),
        Result::Found(response) => {
            let outcome = replayed_execution(response)?;
            compose(
                5,
                McpFixedResultBranch::GetOutcomeReplayed,
                Some(payload_from(&outcome)?),
            )
        }
    }
}

pub(crate) fn get_entity(
    response: v1::GetEntityResponse,
) -> Result<McpToolResult, ResponseConversionError> {
    use v1::get_entity_response::Result;

    match response.result.ok_or(ResponseConversionError)? {
        Result::NotFound(_) => compose(6, McpFixedResultBranch::EntityNotFound, None),
        Result::Found(entity) => {
            let fields = presented_record(entity.fields.ok_or(ResponseConversionError)?)?;
            let payload = Entity {
                entity_key: McpPresentedBytes::new(entity.entity_key),
                entity_version: McpPresentedU64::new(entity.entity_version),
                written_by_contract_version: McpPresentedU64::new(
                    entity.written_by_contract_version,
                ),
                fields,
            };
            compose(
                6,
                McpFixedResultBranch::EntityFound,
                Some(payload_from(&payload)?),
            )
        }
    }
}

pub(crate) fn scan_index(
    response: v1::ScanIndexResponse,
) -> Result<McpToolResult, ResponseConversionError> {
    let page = response.page.ok_or(ResponseConversionError)?;
    let payload = IndexPage {
        items: page
            .items
            .into_iter()
            .map(|row| {
                Ok(IndexRow {
                    index_entry_key: McpPresentedBytes::new(row.index_entry_key),
                    values: presented_record(row.values.ok_or(ResponseConversionError)?)?,
                })
            })
            .collect::<Result<Vec<_>, ResponseConversionError>>()?,
        next_cursor: cursor_text(page.next_cursor)?,
        observed_fence: index_fence(page.observed_fence.ok_or(ResponseConversionError)?)?,
    };
    compose(
        7,
        McpFixedResultBranch::ScanIndexPage,
        Some(payload_from(&payload)?),
    )
}

pub(crate) fn get_commit(
    response: v1::GetCommitResponse,
    expected_sequence: u64,
) -> Result<McpToolResult, ResponseConversionError> {
    use v1::get_commit_response::Result;

    match response.result.ok_or(ResponseConversionError)? {
        Result::NotFound(_) => compose(8, McpFixedResultBranch::CommitNotFound, None),
        Result::Found(commit) => {
            if commit.commit_sequence != expected_sequence {
                return Err(ResponseConversionError);
            }
            compose(
                8,
                McpFixedResultBranch::CommitFound,
                Some(payload_from(&Commit::try_from(commit)?)?),
            )
        }
    }
}

pub(crate) fn scan_commits(
    response: v1::ScanCommitsResponse,
) -> Result<McpToolResult, ResponseConversionError> {
    let page = response.page.ok_or(ResponseConversionError)?;
    let payload = CommitPage {
        items: page
            .items
            .into_iter()
            .map(Commit::try_from)
            .collect::<Result<Vec<_>, _>>()?,
        next_cursor: cursor_text(page.next_cursor)?,
        observed_fence: frontier(page.observed_fence.ok_or(ResponseConversionError)?)?,
    };
    compose(
        9,
        McpFixedResultBranch::CommitScanPage,
        Some(payload_from(&payload)?),
    )
}

pub(crate) fn trace_provenance(
    response: v1::TraceProvenanceResponse,
) -> Result<McpToolResult, ResponseConversionError> {
    use v1::trace_provenance_response::Result;

    match response.result.ok_or(ResponseConversionError)? {
        Result::NotFound(_) => compose(10, McpFixedResultBranch::ProvenanceNotFound, None),
        Result::Found(provenance) => compose(
            10,
            McpFixedResultBranch::ProvenanceFound,
            Some(payload_from(&Provenance::try_from(provenance)?)?),
        ),
    }
}

pub(crate) fn query_projection(
    response: v1::QueryProjectionResponse,
) -> Result<McpToolResult, ResponseConversionError> {
    use v1::query_projection_response::Result as ProtoResult;

    match response.result.ok_or(ResponseConversionError)? {
        ProtoResult::Ready(ready) => {
            let page = ready.data.ok_or(ResponseConversionError)?;
            let payload = ProjectionReady {
                data: ProjectionPage {
                    items: page
                        .items
                        .into_iter()
                        .map(|row| {
                            Ok(ProjectionRow {
                                group: row
                                    .group
                                    .into_iter()
                                    .map(presented_value)
                                    .collect::<std::result::Result<Vec<_>, _>>()?,
                                values: presented_record(
                                    row.values.ok_or(ResponseConversionError)?,
                                )?,
                            })
                        })
                        .collect::<std::result::Result<Vec<_>, ResponseConversionError>>()?,
                    next_cursor: cursor_text(page.next_cursor)?,
                    observed_fence: ProjectionFence::try_from(
                        page.observed_fence.ok_or(ResponseConversionError)?,
                    )?,
                },
                frontier: frontier(ready.frontier.ok_or(ResponseConversionError)?)?,
            };
            compose(
                11,
                McpFixedResultBranch::ProjectionReady,
                Some(payload_from(&payload)?),
            )
        }
        ProtoResult::WaitTimedOut(wait) => {
            let payload = ProjectionWaitTimedOut {
                required_sequence: McpPresentedU64::new(wait.required_sequence),
                current: frontier(wait.current.ok_or(ResponseConversionError)?)?,
            };
            compose(
                11,
                McpFixedResultBranch::ProjectionWaitTimedOut,
                Some(payload_from(&payload)?),
            )
        }
        ProtoResult::Degraded(degraded) => {
            let payload = ProjectionDegraded {
                current: frontier(degraded.current.ok_or(ResponseConversionError)?)?,
                reason: projection_unavailable_reason(
                    degraded.reason.ok_or(ResponseConversionError)?,
                )?,
            };
            compose(
                11,
                McpFixedResultBranch::ProjectionDegraded,
                Some(payload_from(&payload)?),
            )
        }
        ProtoResult::Invalid(invalid) => {
            let payload = ProjectionInvalid {
                reason: projection_failure(invalid.reason)?,
            };
            compose(
                11,
                McpFixedResultBranch::ProjectionInvalid,
                Some(payload_from(&payload)?),
            )
        }
    }
}

pub(crate) fn get_projection_status(
    response: v1::GetProjectionStatusResponse,
) -> Result<McpToolResult, ResponseConversionError> {
    use v1::get_projection_status_response::Result;

    match response.result.ok_or(ResponseConversionError)? {
        Result::NotFound(_) => compose(12, McpFixedResultBranch::ProjectionStatusNotFound, None),
        Result::Found(status) => compose(
            12,
            McpFixedResultBranch::ProjectionStatusFound,
            Some(payload_from(&ProjectionStatus::try_from(status)?)?),
        ),
    }
}

pub(crate) fn list_pending_outbox_deliveries(
    response: v1::ListPendingOutboxDeliveriesResponse,
) -> Result<McpToolResult, ResponseConversionError> {
    let page = response.page.ok_or(ResponseConversionError)?;
    let payload = OutboxPage {
        items: page
            .items
            .into_iter()
            .map(OutboxItem::try_from)
            .collect::<Result<Vec<_>, _>>()?,
        next_cursor: cursor_text(page.next_cursor)?,
    };
    compose(
        13,
        McpFixedResultBranch::OutboxPage,
        Some(payload_from(&payload)?),
    )
}

pub(crate) fn health(
    response: v1::HealthResponse,
) -> Result<McpToolResult, ResponseConversionError> {
    use v1::health_response::Result;

    let database = response.database_alias;
    let audience = response.authentication_audience;
    match response.result.ok_or(ResponseConversionError)? {
        Result::PreBootstrap(health) => {
            let payload = PreBootstrapHealth {
                database,
                lifecycle: pre_bootstrap_lifecycle(health.lifecycle)?,
                liveness: health.liveness,
                readiness: health.readiness,
            };
            compose(
                14,
                McpFixedResultBranch::HealthPreBootstrap,
                Some(payload_from(&payload)?),
            )
        }
        Result::Authenticated(health) => {
            let payload = AuthenticatedHealthTool {
                database,
                audience,
                health: AuthenticatedHealth::try_from(health)?,
            };
            compose(
                14,
                McpFixedResultBranch::HealthAuthenticated,
                Some(payload_from(&payload)?),
            )
        }
    }
}

pub(crate) fn describe_contract(
    response: app_v1::DescribeContractResponse,
) -> Result<McpToolResult, ResponseConversionError> {
    let payload = serde_json::json!({
        "contract": {
            "lineage": response.contract_lineage,
            "version": response.contract_version.to_string(),
            "bundle_hash": lower_hex(&response.contract_bundle_hash),
        },
        "catalog": response.symbolic_catalog,
    });
    compose(
        15,
        McpFixedResultBranch::ContractDescribed,
        Some(payload_from(&payload)?),
    )
}

pub(crate) fn application_catalog(
    response: app_v1::GetApplicationCatalogResponse,
) -> Result<McpToolResult, ResponseConversionError> {
    if response.schema != "riffdb.application_catalog.v1"
        || response.contract_lineage.is_empty()
        || response.contract_version == 0
    {
        return Err(ResponseConversionError);
    }
    let contract_bundle_hash = exact_hash(&response.contract_bundle_hash)?;
    let query_module_hashes = response
        .query_module_hashes
        .iter()
        .map(|hash| exact_hash(hash).map(|hash| lower_hex(&hash)))
        .collect::<Result<Vec<_>, ResponseConversionError>>()?;
    let next_cursor = response
        .next_cursor
        .as_deref()
        .map(|cursor| {
            let bytes = URL_SAFE_NO_PAD
                .decode(cursor.as_bytes())
                .map_err(|_| ResponseConversionError)?;
            let bytes: [u8; riffdb_api_mcp::MCP_CURSOR_BYTES] =
                bytes.try_into().map_err(|_| ResponseConversionError)?;
            Ok(riffdb_api_mcp::encode_mcp_cursor(bytes))
        })
        .transpose()?;
    if response.has_more != next_cursor.is_some() {
        return Err(ResponseConversionError);
    }
    let symbols = response
        .symbols
        .into_iter()
        .map(|symbol| {
            if symbol.path.is_empty()
                || symbol
                    .source_span
                    .as_ref()
                    .is_some_and(|span| span.start > span.end)
            {
                return Err(ResponseConversionError);
            }
            Ok(serde_json::json!({
                "kind": application_catalog_symbol_kind(symbol.kind)?,
                "path": symbol.path,
                "public_type": symbol.public_type,
                "source_span": symbol.source_span.map(|span| serde_json::json!({
                    "start": span.start,
                    "end": span.end,
                })),
            }))
        })
        .collect::<Result<Vec<_>, ResponseConversionError>>()?;
    let features = response
        .features
        .into_iter()
        .map(|feature| {
            Ok(serde_json::json!({
                "feature": application_catalog_feature(feature.feature)?,
                "state": application_catalog_feature_state(feature.state)?,
            }))
        })
        .collect::<Result<Vec<_>, ResponseConversionError>>()?;
    let payload = serde_json::json!({
        "schema": response.schema,
        "contract": {
            "lineage": response.contract_lineage,
            "version": response.contract_version.to_string(),
            "bundle_hash": lower_hex(&contract_bundle_hash),
        },
        "query_module_hashes": query_module_hashes,
        "symbols": symbols,
        "features": features,
        "has_more": response.has_more,
        "next_cursor": next_cursor,
    });
    compose(
        31,
        McpFixedResultBranch::ApplicationCatalogPage,
        Some(payload_from(&payload)?),
    )
}

fn application_catalog_symbol_kind(value: i32) -> Result<&'static str, ResponseConversionError> {
    match app_v1::ApplicationCatalogSymbolKind::try_from(value).ok() {
        Some(app_v1::ApplicationCatalogSymbolKind::Contract) => Ok("contract"),
        Some(app_v1::ApplicationCatalogSymbolKind::Enum) => Ok("enum"),
        Some(app_v1::ApplicationCatalogSymbolKind::Entity) => Ok("entity"),
        Some(app_v1::ApplicationCatalogSymbolKind::Field) => Ok("field"),
        Some(app_v1::ApplicationCatalogSymbolKind::Relationship) => Ok("relationship"),
        Some(app_v1::ApplicationCatalogSymbolKind::Index) => Ok("index"),
        Some(app_v1::ApplicationCatalogSymbolKind::Command) => Ok("command"),
        Some(app_v1::ApplicationCatalogSymbolKind::CommandOutcome) => Ok("command_outcome"),
        Some(app_v1::ApplicationCatalogSymbolKind::Event) => Ok("event"),
        Some(app_v1::ApplicationCatalogSymbolKind::QueryModule) => Ok("query_module"),
        Some(app_v1::ApplicationCatalogSymbolKind::Query) => Ok("query"),
        Some(app_v1::ApplicationCatalogSymbolKind::Role) => Ok("role"),
        Some(app_v1::ApplicationCatalogSymbolKind::Operation) => Ok("operation"),
        Some(app_v1::ApplicationCatalogSymbolKind::Unspecified) | None => {
            Err(ResponseConversionError)
        }
    }
}

fn application_catalog_feature(value: i32) -> Result<&'static str, ResponseConversionError> {
    match app_v1::ApplicationCatalogFeature::try_from(value).ok() {
        Some(app_v1::ApplicationCatalogFeature::OperationalOptionalPredicates) => {
            Ok("operational_optional_predicates")
        }
        Some(app_v1::ApplicationCatalogFeature::StableCursorPages) => Ok("stable_cursor_pages"),
        Some(app_v1::ApplicationCatalogFeature::NullExistencePredicates) => {
            Ok("null_existence_predicates")
        }
        Some(app_v1::ApplicationCatalogFeature::BinaryTextPrefix) => Ok("binary_text_prefix"),
        Some(app_v1::ApplicationCatalogFeature::UnicodeFoldTextPrefixV1) => {
            Ok("unicode_fold_text_prefix_v1")
        }
        Some(app_v1::ApplicationCatalogFeature::ExactAggregates) => Ok("exact_aggregates"),
        Some(app_v1::ApplicationCatalogFeature::Unspecified) | None => Err(ResponseConversionError),
    }
}

fn application_catalog_feature_state(value: i32) -> Result<&'static str, ResponseConversionError> {
    match app_v1::ApplicationCatalogFeatureState::try_from(value).ok() {
        Some(app_v1::ApplicationCatalogFeatureState::Available) => Ok("available"),
        Some(app_v1::ApplicationCatalogFeatureState::Unavailable) => Ok("unavailable"),
        Some(app_v1::ApplicationCatalogFeatureState::Unspecified) | None => {
            Err(ResponseConversionError)
        }
    }
}

pub(crate) fn check_query(
    response: app_v1::CheckQueryResponse,
) -> Result<McpToolResult, ResponseConversionError> {
    if response.diagnostics.is_empty() {
        let payload = checked_query_payload(
            response.identity.ok_or(ResponseConversionError)?,
            response.schema.ok_or(ResponseConversionError)?,
        );
        compose(
            16,
            McpFixedResultBranch::QueryCheckValid,
            Some(payload_from(&payload)?),
        )
    } else {
        compose(
            16,
            McpFixedResultBranch::QueryCheckInvalid,
            Some(payload_from(&serde_json::json!({
                "diagnostics": diagnostic_payloads(response.diagnostics)?
            }))?),
        )
    }
}

pub(crate) fn explain_query(
    response: app_v1::ExplainQueryResponse,
) -> Result<McpToolResult, ResponseConversionError> {
    if response.diagnostics.is_empty() {
        let mut payload = checked_query_payload(
            response.identity.ok_or(ResponseConversionError)?,
            response.schema.ok_or(ResponseConversionError)?,
        );
        payload
            .as_object_mut()
            .ok_or(ResponseConversionError)?
            .insert("plan".to_owned(), serde_json::json!(response.plan_lines));
        compose(
            17,
            McpFixedResultBranch::QueryExplainValid,
            Some(payload_from(&payload)?),
        )
    } else {
        compose(
            17,
            McpFixedResultBranch::QueryExplainInvalid,
            Some(payload_from(&serde_json::json!({
                "diagnostics": diagnostic_payloads(response.diagnostics)?
            }))?),
        )
    }
}

pub(crate) fn execute_query(
    response: app_v1::ExecuteQueryResponse,
) -> Result<McpToolResult, ResponseConversionError> {
    let fields = response
        .fields
        .into_iter()
        .map(symbolic_result_field)
        .collect::<Result<Vec<_>, _>>()?;
    let next_cursor = response
        .next_cursor
        .map(|cursor| {
            let bytes = URL_SAFE_NO_PAD
                .decode(cursor.as_bytes())
                .map_err(|_| ResponseConversionError)?;
            let bytes: [u8; 16] = bytes.try_into().map_err(|_| ResponseConversionError)?;
            Ok(riffdb_api_mcp::encode_mcp_cursor(bytes))
        })
        .transpose()?;
    let payload = serde_json::json!({
        "identity": symbolic_identity_payload(response.identity.ok_or(ResponseConversionError)?),
        "outcome": response.outcome,
        "application_head": response.application_head.to_string(),
        "fields": fields,
        "next_cursor": next_cursor,
    });
    compose(
        18,
        McpFixedResultBranch::QueryCompleted,
        Some(payload_from(&payload)?),
    )
}

pub(crate) fn run_command(
    response: v1::ExecuteCommandResponse,
) -> Result<McpToolResult, ResponseConversionError> {
    command_response(19, McpFixedResultBranch::CommandCompleted, response)
}

pub(crate) fn contextual_reaction(
    response: v1::ExecuteCommandResponse,
) -> Result<McpToolResult, ResponseConversionError> {
    command_response(
        30,
        McpFixedResultBranch::ContextualReactionCompleted,
        response,
    )
}

fn command_response(
    tag: u8,
    branch: McpFixedResultBranch,
    response: v1::ExecuteCommandResponse,
) -> Result<McpToolResult, ResponseConversionError> {
    let status =
        match v1::execute_command_response::CompletionStatus::try_from(response.status).ok() {
            Some(v1::execute_command_response::CompletionStatus::Committed) => "committed",
            Some(v1::execute_command_response::CompletionStatus::Replayed) => "replayed",
            Some(v1::execute_command_response::CompletionStatus::ExecutedReadOnly) => {
                "executed_read_only"
            }
            Some(v1::execute_command_response::CompletionStatus::Unspecified) | None => {
                return Err(ResponseConversionError);
            }
        };
    let payload = serde_json::json!({
        "status": status,
        "commit_sequence": (status != "executed_read_only")
            .then(|| response.commit_sequence.to_string()),
        "contract_version": response.contract_version.to_string(),
        "plan_hash": lower_hex(&response.plan_hash),
        "outcome": {
            "type": response.outcome_type,
            "value": public_natural_value(response.outcome.ok_or(ResponseConversionError)?)?,
        },
        "provenance_uri": (!response.provenance_uri.is_empty())
            .then_some(response.provenance_uri),
        "durability": (!response.durability_mode.is_empty())
            .then_some(response.durability_mode),
        "outcome_uri": response.outcome_uri,
    });
    compose(tag, branch, Some(payload_from(&payload)?))
}

fn public_natural_value(value: v1::Value) -> Result<serde_json::Value, ResponseConversionError> {
    use v1::value::Kind;
    match value.kind.ok_or(ResponseConversionError)? {
        Kind::NullValue(value) if value == v1::NullValue::NullValue as i32 => {
            Ok(serde_json::Value::Null)
        }
        Kind::NullValue(_) => Err(ResponseConversionError),
        Kind::BoolValue(value) => Ok(serde_json::Value::Bool(value)),
        Kind::I64Value(value) => Ok(serde_json::Value::String(value.to_string())),
        Kind::U64Value(value) => Ok(serde_json::Value::String(value.to_string())),
        Kind::DecimalValue(value) => {
            let (precision, scale, coefficient) = presented_decimal(value)?;
            Ok(serde_json::json!({
                "coefficient": coefficient,
                "precision": precision,
                "scale": scale,
            }))
        }
        Kind::MoneyValue(value) => {
            let currency = value.currency;
            let (precision, scale, coefficient) =
                presented_decimal(value.amount.ok_or(ResponseConversionError)?)?;
            Ok(serde_json::json!({
                "currency": currency,
                "coefficient": coefficient,
                "precision": precision,
                "scale": scale,
            }))
        }
        Kind::StringValue(value) => Ok(serde_json::Value::String(value)),
        Kind::BytesValue(value) => {
            serde_json::to_value(McpPresentedBytes::new(value)).map_err(|_| ResponseConversionError)
        }
        Kind::TimestampValue(value) => Ok(serde_json::json!({
            "seconds": value.seconds.to_string(),
            "nanos": value.nanos,
        })),
        Kind::DateValue(value) => Ok(serde_json::json!({
            "days_since_unix_epoch": value.days_since_unix_epoch,
        })),
        Kind::UuidValue(value) => {
            serde_json::to_value(uuid(&value)?).map_err(|_| ResponseConversionError)
        }
        Kind::EnumValue(value) if !value.name.is_empty() => {
            Ok(serde_json::Value::String(value.name))
        }
        Kind::EnumValue(_) => Err(ResponseConversionError),
        Kind::VectorValue(value) => {
            let McpPresentedValue::Vector { components } =
                McpPresentedValue::vector(value.components).map_err(|_| ResponseConversionError)?
            else {
                return Err(ResponseConversionError);
            };
            serde_json::to_value(components).map_err(|_| ResponseConversionError)
        }
        Kind::ListValue(values) => values
            .values
            .into_iter()
            .map(public_natural_value)
            .collect::<Result<Vec<_>, _>>()
            .map(serde_json::Value::Array),
        Kind::RecordValue(record) => {
            let mut fields = serde_json::Map::new();
            for field in record.fields {
                if field.name.is_empty()
                    || fields
                        .insert(
                            field.name,
                            public_natural_value(field.value.ok_or(ResponseConversionError)?)?,
                        )
                        .is_some()
                {
                    return Err(ResponseConversionError);
                }
            }
            Ok(serde_json::Value::Object(fields))
        }
    }
}

fn checked_query_payload(
    identity: app_v1::QueryIdentity,
    schema: app_v1::QuerySchema,
) -> serde_json::Value {
    serde_json::json!({
        "identity": symbolic_identity_payload(identity),
        "schema": {
            "parameters": schema.parameters,
            "outcomes": schema.outcomes,
            "result_fields": schema.result_fields,
        }
    })
}

fn symbolic_identity_payload(identity: app_v1::QueryIdentity) -> serde_json::Value {
    serde_json::json!({
        "contract_lineage": identity.contract_lineage,
        "contract_version": identity.contract_version.to_string(),
        "contract_bundle_hash": lower_hex(&identity.contract_bundle_hash),
        "query_name": identity.query_name,
        "plan_hash": lower_hex(&identity.plan_hash),
    })
}

fn diagnostic_payloads(
    diagnostics: Vec<app_v1::Diagnostic>,
) -> Result<Vec<serde_json::Value>, ResponseConversionError> {
    diagnostics
        .into_iter()
        .map(|diagnostic| {
            let span = diagnostic.span.ok_or(ResponseConversionError)?;
            Ok(serde_json::json!({
                "code": diagnostic.code,
                "summary": diagnostic.summary,
                "span": {"start": span.start, "end": span.end},
                "symbols": diagnostic.symbols,
                "suggestion": diagnostic.suggestion,
            }))
        })
        .collect()
}

fn symbolic_result_field(
    field: app_v1::ResultField,
) -> Result<serde_json::Value, ResponseConversionError> {
    let cardinality = match app_v1::ResultCardinality::try_from(field.cardinality).ok() {
        Some(app_v1::ResultCardinality::One) => "one",
        Some(app_v1::ResultCardinality::Maybe) => "maybe",
        Some(app_v1::ResultCardinality::Many) => "many",
        Some(app_v1::ResultCardinality::Unspecified) | None => {
            return Err(ResponseConversionError);
        }
    };
    let records = field
        .records
        .into_iter()
        .map(|record| {
            let mut prior = None;
            let fields = record
                .fields
                .into_iter()
                .map(|field| {
                    if field.name.is_empty()
                        || prior
                            .as_deref()
                            .is_some_and(|name| name >= field.name.as_str())
                    {
                        return Err(ResponseConversionError);
                    }
                    prior = Some(field.name.clone());
                    Ok(serde_json::json!({
                        "name": field.name,
                        "value": presented_value(field.value.ok_or(ResponseConversionError)?)?,
                    }))
                })
                .collect::<Result<Vec<_>, ResponseConversionError>>()?;
            if record.entity.is_empty() {
                return Err(ResponseConversionError);
            }
            Ok(serde_json::json!({"entity": record.entity, "fields": fields}))
        })
        .collect::<Result<Vec<_>, ResponseConversionError>>()?;
    Ok(serde_json::json!({
        "name": field.name,
        "cardinality": cardinality,
        "records": records,
    }))
}

fn lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

pub(crate) fn active_contract_resource(
    contract: v1::ContractDescriptor,
) -> Result<McpResourceJson, ResponseConversionError> {
    let contract = contract_presentation(contract)?;
    render_active_contract_resource(&contract).map_err(|_| ResponseConversionError)
}

pub(crate) fn contract_version_resource(
    contract: v1::ContractDescriptor,
) -> Result<McpResourceJson, ResponseConversionError> {
    let contract = contract_presentation(contract)?;
    render_contract_version_resource(&contract).map_err(|_| ResponseConversionError)
}

pub(crate) fn command_plan_resource(
    source_command: &str,
    explained: v1::ExplainedCommand,
) -> Result<McpResourceJson, ResponseConversionError> {
    let command = explained_command_presentation(source_command, explained)?;
    render_command_plan_resource(&command).map_err(|_| ResponseConversionError)
}

pub(crate) fn command_documentation_resource(
    source_command: &str,
    explained: v1::ExplainedCommand,
) -> Result<riffdb_api_mcp::McpMarkdownDocument, ResponseConversionError> {
    let command = explained_command_presentation(source_command, explained)?;
    render_command_documentation(&command).map_err(|_| ResponseConversionError)
}

pub(crate) fn command_plan_observation(
    uri: &str,
    source_command: &str,
    explained: v1::ExplainedCommand,
) -> Result<McpVisibleFingerprint, ResponseConversionError> {
    let command = explained_command_presentation(source_command, explained)?;
    McpVisibleFingerprint::command_plan_resource(uri, &command).map_err(|_| ResponseConversionError)
}

pub(crate) fn entity_schema_resource(
    artifact: v1::GeneratedSchemaArtifact,
    expected_entity_id: u32,
) -> Result<McpResourceJson, ResponseConversionError> {
    let key = artifact
        .key
        .and_then(|key| key.artifact)
        .ok_or(ResponseConversionError)?;
    if key != v1::schema_artifact_key::Artifact::EntityId(expected_entity_id)
        || artifact.dialect != "https://json-schema.org/draft/2020-12/schema"
    {
        return Err(ResponseConversionError);
    }
    SchemaDocument::from_public_generated(
        McpGeneratedSchemaKind::Entity,
        expected_entity_id,
        &artifact.schema_hash,
        artifact.canonical_json,
    )
    .map_err(|_| ResponseConversionError)?
    .to_resource_json()
    .map_err(|_| ResponseConversionError)
}

pub(crate) fn outcome_resource(
    response: v1::ExecuteCommandResponse,
    expected_uri: &str,
) -> Result<McpResourceJson, ResponseConversionError> {
    let outcome = replayed_execution(response)?;
    if outcome.outcome_uri != expected_uri {
        return Err(ResponseConversionError);
    }
    resource_json_from(&outcome)
}

pub(crate) fn commit_resource(
    commit: v1::Commit,
    expected_sequence: u64,
) -> Result<McpResourceJson, ResponseConversionError> {
    if commit.commit_sequence != expected_sequence {
        return Err(ResponseConversionError);
    }
    resource_json_from(&Commit::try_from(commit)?)
}

pub(crate) fn provenance_resource(
    provenance: v1::Provenance,
    expected_id: &[u8; 16],
) -> Result<McpResourceJson, ResponseConversionError> {
    if provenance.provenance_id.as_slice() != expected_id {
        return Err(ResponseConversionError);
    }
    resource_json_from(&Provenance::try_from(provenance)?)
}

pub(crate) fn projection_status_resource(
    status: v1::ProjectionStatus,
    expected_lineage: &str,
    expected_projection_id: u32,
) -> Result<McpResourceJson, ResponseConversionError> {
    let identity = status.identity.as_ref().ok_or(ResponseConversionError)?;
    if identity.contract_lineage != expected_lineage
        || identity.projection_id != expected_projection_id
    {
        return Err(ResponseConversionError);
    }
    let status = projection_status_presentation(status)?;
    render_projection_status_resource(&status).map_err(|_| ResponseConversionError)
}

pub(crate) fn authenticated_health_resource(
    health: v1::AuthenticatedHealth,
) -> Result<McpResourceJson, ResponseConversionError> {
    resource_json_from(&AuthenticatedHealth::try_from(health)?)
}

pub(crate) fn dynamic_command_result(
    response: v1::ExecuteCommandResponse,
    expected_contract_version: u64,
    outcome_schema: &SchemaDocument,
    result_schema: &SchemaDocument,
) -> Result<McpToolResult, ResponseConversionError> {
    if response.contract_version != expected_contract_version {
        return Err(ResponseConversionError);
    }
    let outcome = McpNaturalOutcome::from_public_response(
        response.outcome_type,
        schema_bound_value(response.outcome.ok_or(ResponseConversionError)?)?,
        outcome_schema,
    )
    .map_err(|_| ResponseConversionError)?;
    let plan_hash = exact_hash(&response.plan_hash)?;
    let completion =
        match v1::execute_command_response::CompletionStatus::try_from(response.status).ok() {
            Some(
                status @ (v1::execute_command_response::CompletionStatus::Committed
                | v1::execute_command_response::CompletionStatus::Replayed),
            ) => {
                let status = match status {
                    v1::execute_command_response::CompletionStatus::Committed => {
                        McpJournaledCommandStatus::Committed
                    }
                    v1::execute_command_response::CompletionStatus::Replayed => {
                        McpJournaledCommandStatus::Replayed
                    }
                    _ => return Err(ResponseConversionError),
                };
                let durability = match response.durability_mode.as_str() {
                    "sync" => McpCommandDurability::Synchronous,
                    "group" => McpCommandDurability::Group,
                    _ => return Err(ResponseConversionError),
                };
                McpDynamicCommandCompletion::journaled(
                    status,
                    McpJournaledCommandResultParts {
                        commit_sequence: response.commit_sequence,
                        contract_version: response.contract_version,
                        plan_hash,
                        outcome,
                        provenance_uri: response.provenance_uri,
                        durability,
                        outcome_uri: response.outcome_uri.ok_or(ResponseConversionError)?,
                    },
                )
                .map_err(|_| ResponseConversionError)?
            }
            Some(v1::execute_command_response::CompletionStatus::ExecutedReadOnly)
                if response.commit_sequence == 0
                    && response.provenance_uri.is_empty()
                    && response.durability_mode.is_empty()
                    && response.outcome_uri.is_none() =>
            {
                McpDynamicCommandCompletion::read_only(McpReadOnlyCommandResultParts {
                    contract_version: response.contract_version,
                    plan_hash,
                    outcome,
                })
                .map_err(|_| ResponseConversionError)?
            }
            Some(
                v1::execute_command_response::CompletionStatus::Unspecified
                | v1::execute_command_response::CompletionStatus::ExecutedReadOnly,
            )
            | None => return Err(ResponseConversionError),
        };
    compose_dynamic_command_result(&completion, outcome_schema, result_schema)
        .map_err(|_| ResponseConversionError)
}

fn schema_bound_value(value: v1::Value) -> Result<McpSchemaBoundValue, ResponseConversionError> {
    use v1::value::Kind;

    match value.kind.ok_or(ResponseConversionError)? {
        Kind::NullValue(value) if value == v1::NullValue::NullValue as i32 => {
            Ok(McpSchemaBoundValue::Null)
        }
        Kind::NullValue(_) => Err(ResponseConversionError),
        Kind::BoolValue(value) => Ok(McpSchemaBoundValue::Bool(value)),
        Kind::I64Value(value) => Ok(McpSchemaBoundValue::I64(value)),
        Kind::U64Value(value) => Ok(McpSchemaBoundValue::U64(value)),
        Kind::DecimalValue(value) => {
            let (coefficient, precision, scale) = schema_bound_decimal(value)?;
            Ok(McpSchemaBoundValue::Decimal {
                coefficient,
                precision,
                scale,
            })
        }
        Kind::MoneyValue(value) => {
            let (coefficient, precision, scale) =
                schema_bound_decimal(value.amount.ok_or(ResponseConversionError)?)?;
            Ok(McpSchemaBoundValue::Money {
                currency: value.currency,
                coefficient,
                precision,
                scale,
            })
        }
        Kind::StringValue(value) => Ok(McpSchemaBoundValue::String(value)),
        Kind::BytesValue(value) => Ok(McpSchemaBoundValue::Bytes(value)),
        Kind::UuidValue(value) => Ok(McpSchemaBoundValue::Uuid(
            value.try_into().map_err(|_| ResponseConversionError)?,
        )),
        Kind::DateValue(value) => Ok(McpSchemaBoundValue::Date(value.days_since_unix_epoch)),
        Kind::TimestampValue(value) => Ok(McpSchemaBoundValue::Timestamp {
            seconds: value.seconds,
            nanos: value.nanos,
        }),
        Kind::EnumValue(value)
            if value.type_id != 0 && value.variant_id != 0 && !value.name.is_empty() =>
        {
            Ok(McpSchemaBoundValue::Enum {
                type_id: value.type_id,
                variant_id: value.variant_id,
                variant_name: value.name,
            })
        }
        Kind::EnumValue(_) => Err(ResponseConversionError),
        Kind::VectorValue(value) => {
            McpSchemaBoundValue::vector(value.components).map_err(|_| ResponseConversionError)
        }
        Kind::ListValue(value) => value
            .values
            .into_iter()
            .map(schema_bound_value)
            .collect::<Result<Vec<_>, _>>()
            .map(McpSchemaBoundValue::List),
        Kind::RecordValue(value) => schema_bound_record(value).map(McpSchemaBoundValue::Record),
    }
}

fn schema_bound_record(
    record: v1::ValueRecord,
) -> Result<Vec<McpSchemaBoundField>, ResponseConversionError> {
    record
        .fields
        .into_iter()
        .map(|field| {
            McpSchemaBoundField::new(
                field.field_id.ok_or(ResponseConversionError)?,
                field.name,
                schema_bound_value(field.value.ok_or(ResponseConversionError)?)?,
            )
            .map_err(|_| ResponseConversionError)
        })
        .collect()
}

fn schema_bound_decimal(value: v1::Decimal) -> Result<(i128, u8, u8), ResponseConversionError> {
    let precision = u8::try_from(value.precision.ok_or(ResponseConversionError)?)
        .map_err(|_| ResponseConversionError)?;
    let scale = u8::try_from(value.scale).map_err(|_| ResponseConversionError)?;
    if !(1..=38).contains(&precision) || scale > precision {
        return Err(ResponseConversionError);
    }
    let coefficient = decode_minimal_i128(&value.coefficient_twos_complement)?;
    if coefficient.unsigned_abs() >= 10_u128.pow(u32::from(precision)) {
        return Err(ResponseConversionError);
    }
    Ok((coefficient, precision, scale))
}

fn contract_presentation(
    contract: v1::ContractDescriptor,
) -> Result<McpContractDescriptorPresentation, ResponseConversionError> {
    let compatibility = contract.compatibility.ok_or(ResponseConversionError)?;
    let overall = match v1::ContractCompatibilityClass::try_from(compatibility.overall).ok() {
        Some(v1::ContractCompatibilityClass::Compatible) => {
            McpContractCompatibilityClass::Compatible
        }
        Some(v1::ContractCompatibilityClass::RequiresExplicitVersion) => {
            McpContractCompatibilityClass::RequiresExplicitVersion
        }
        Some(v1::ContractCompatibilityClass::RequiresMigration) => {
            McpContractCompatibilityClass::RequiresMigration
        }
        Some(v1::ContractCompatibilityClass::Incompatible) => {
            McpContractCompatibilityClass::Incompatible
        }
        Some(v1::ContractCompatibilityClass::Unspecified) | None => {
            return Err(ResponseConversionError);
        }
    };
    let code_counts = compatibility
        .code_counts
        .into_iter()
        .map(|count| McpCompatibilityCodeCount::new(count.code, count.count))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| ResponseConversionError)?;
    let compatibility = match (
        compatibility.parent_contract_version,
        compatibility.parent_bundle_hash,
    ) {
        (None, None)
            if overall == McpContractCompatibilityClass::Compatible && code_counts.is_empty() =>
        {
            McpContractCompatibilityPresentation::genesis()
        }
        (Some(parent_version), Some(parent_hash)) => {
            McpContractCompatibilityPresentation::successor(
                parent_version,
                exact_hash(&parent_hash)?,
                overall,
                code_counts,
            )
            .map_err(|_| ResponseConversionError)?
        }
        _ => return Err(ResponseConversionError),
    };
    McpContractDescriptorPresentation::new(
        contract.contract_lineage,
        contract.contract_version,
        exact_hash(&contract.bundle_hash)?,
        exact_hash(&contract.source_hash)?,
        exact_hash(&contract.plan_root_hash)?,
        compatibility,
    )
    .map_err(|_| ResponseConversionError)
}

fn explained_command_presentation(
    source_command: &str,
    explained: v1::ExplainedCommand,
) -> Result<McpExplainedCommandPresentation, ResponseConversionError> {
    let command_id = explained.command_id;
    let tool_name = explained.tool_name.clone();
    let explanation = explained.explanation.ok_or(ResponseConversionError)?;
    if explanation.command_id != command_id {
        return Err(ResponseConversionError);
    }
    let execution_class = match v1::ExecutionClass::try_from(explanation.execution_class).ok() {
        Some(v1::ExecutionClass::ReadOnly) => McpCommandExecutionClass::ReadOnly,
        Some(v1::ExecutionClass::IdempotentMutation) => {
            McpCommandExecutionClass::IdempotentMutation
        }
        Some(v1::ExecutionClass::Unspecified) | None => return Err(ResponseConversionError),
    };
    let read_fields = explanation
        .read_fields
        .into_iter()
        .map(|reference| {
            McpBindingFieldReferencePresentation::new(reference.binding_id, reference.field_id)
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| ResponseConversionError)?;
    let write_fields = explanation
        .write_fields
        .into_iter()
        .map(|reference| {
            McpBindingFieldReferencePresentation::new(reference.binding_id, reference.field_id)
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| ResponseConversionError)?;
    let explanation = McpCommandExplanationPresentation::from_fields(McpCommandExplanationFields {
        command_id,
        execution_class,
        partition_component_count: explanation.partition_component_count,
        conflict_key_count: explanation.conflict_key_count,
        binding_ids: explanation.binding_ids,
        read_fields,
        write_fields,
        invariant_ids: explanation.invariant_ids,
        event_type_ids: explanation.event_type_ids,
        outcome_ids: explanation.outcome_ids,
        rendered_text: explanation.rendered_text,
    })
    .map_err(|_| ResponseConversionError)?;
    let input_schema = generated_schema_document(
        explained.input_schema.ok_or(ResponseConversionError)?,
        command_id,
        v1::schema_artifact_key::Artifact::CommandInputId(command_id),
        McpGeneratedSchemaKind::CommandInput,
    )?;
    let outcome_schema = generated_schema_document(
        explained.outcome_schema.ok_or(ResponseConversionError)?,
        command_id,
        v1::schema_artifact_key::Artifact::CommandOutcomeUnionId(command_id),
        McpGeneratedSchemaKind::CommandOutcomeUnion,
    )?;
    McpExplainedCommandPresentation::new(
        contract_presentation(explained.contract.ok_or(ResponseConversionError)?)?,
        source_command,
        tool_name,
        exact_hash(&explained.plan_hash)?,
        explanation,
        input_schema,
        outcome_schema,
    )
    .map_err(|_| ResponseConversionError)
}

fn generated_schema_document(
    artifact: v1::GeneratedSchemaArtifact,
    expected_id: u32,
    expected_key: v1::schema_artifact_key::Artifact,
    kind: McpGeneratedSchemaKind,
) -> Result<SchemaDocument, ResponseConversionError> {
    let key = artifact
        .key
        .and_then(|key| key.artifact)
        .ok_or(ResponseConversionError)?;
    if key != expected_key || artifact.dialect != "https://json-schema.org/draft/2020-12/schema" {
        return Err(ResponseConversionError);
    }
    SchemaDocument::from_public_generated(
        kind,
        expected_id,
        &artifact.schema_hash,
        artifact.canonical_json,
    )
    .map_err(|_| ResponseConversionError)
}

fn projection_status_presentation(
    status: v1::ProjectionStatus,
) -> Result<McpProjectionStatusPresentation, ResponseConversionError> {
    let identity = status.identity.ok_or(ResponseConversionError)?;
    let identity = McpProjectionIdentityPresentation::new(
        identity.contract_lineage,
        identity.projection_id,
        exact_hash(&identity.projection_plan_hash)?,
    )
    .map_err(|_| ResponseConversionError)?;
    let lifecycle = match v1::ProjectionLifecycle::try_from(status.lifecycle).ok() {
        Some(v1::ProjectionLifecycle::Building) => McpProjectionLifecycle::Building,
        Some(v1::ProjectionLifecycle::CatchingUp) => McpProjectionLifecycle::CatchingUp,
        Some(v1::ProjectionLifecycle::Ready) => McpProjectionLifecycle::Ready,
        Some(v1::ProjectionLifecycle::Rebuilding) => McpProjectionLifecycle::Rebuilding,
        Some(v1::ProjectionLifecycle::Degraded) => McpProjectionLifecycle::Degraded,
        Some(v1::ProjectionLifecycle::Invalid) => McpProjectionLifecycle::Invalid,
        Some(v1::ProjectionLifecycle::Unspecified) | None => return Err(ResponseConversionError),
    };
    let published = status
        .published
        .map(projection_generation_frontier)
        .transpose()?;
    let candidate = status
        .candidate
        .map(projection_generation_frontier)
        .transpose()?;
    let published_apply_mode = status
        .published_apply_mode
        .map(|mode| match v1::PublishedApplyMode::try_from(mode).ok() {
            Some(v1::PublishedApplyMode::Enabled) => Ok(McpPublishedApplyMode::Enabled),
            Some(v1::PublishedApplyMode::Suspended) => Ok(McpPublishedApplyMode::Suspended),
            Some(v1::PublishedApplyMode::Unspecified) | None => Err(ResponseConversionError),
        })
        .transpose()?;
    let failure = status
        .failure
        .map(|failure| {
            McpProjectionFailurePresentation::new(
                failure.generation,
                projection_failure_code_presentation(failure.code)?,
                failure.at_sequence,
            )
            .map_err(|_| ResponseConversionError)
        })
        .transpose()?;
    McpProjectionStatusPresentation::from_parts(McpProjectionStatusParts {
        identity,
        lifecycle,
        published,
        candidate,
        published_apply_mode,
        failure,
        authoritative_head: frontier_presentation(
            status.authoritative_head.ok_or(ResponseConversionError)?,
        )?,
    })
    .map_err(|_| ResponseConversionError)
}

fn projection_generation_frontier(
    position: v1::ProjectionGenerationFrontier,
) -> Result<McpProjectionGenerationFrontierPresentation, ResponseConversionError> {
    McpProjectionGenerationFrontierPresentation::new(
        position.generation,
        frontier_presentation(position.frontier.ok_or(ResponseConversionError)?)?,
    )
    .map_err(|_| ResponseConversionError)
}

fn frontier_presentation(
    frontier: v1::FrontierPosition,
) -> Result<McpFrontierPresentation, ResponseConversionError> {
    match frontier.position.ok_or(ResponseConversionError)? {
        v1::frontier_position::Position::BeforeFirst(_) => Ok(McpFrontierPresentation::BeforeFirst),
        v1::frontier_position::Position::AppliedThrough(sequence) => {
            Ok(McpFrontierPresentation::AppliedThrough(sequence))
        }
    }
}

fn projection_failure_code_presentation(
    code: i32,
) -> Result<McpProjectionFailureCode, ResponseConversionError> {
    match v1::ProjectionFailureCode::try_from(code).ok() {
        Some(v1::ProjectionFailureCode::ArithmeticOverflow) => {
            Ok(McpProjectionFailureCode::ArithmeticOverflow)
        }
        Some(v1::ProjectionFailureCode::MalformedDurableEvent) => {
            Ok(McpProjectionFailureCode::MalformedDurableEvent)
        }
        Some(v1::ProjectionFailureCode::MissingCommit) => {
            Ok(McpProjectionFailureCode::MissingCommit)
        }
        Some(v1::ProjectionFailureCode::PlanOrSchemaUnavailable) => {
            Ok(McpProjectionFailureCode::PlanOrSchemaUnavailable)
        }
        Some(v1::ProjectionFailureCode::ProjectionStateIntegrity) => {
            Ok(McpProjectionFailureCode::ProjectionStateIntegrity)
        }
        Some(v1::ProjectionFailureCode::HardLimitExceeded) => {
            Ok(McpProjectionFailureCode::HardLimitExceeded)
        }
        Some(v1::ProjectionFailureCode::Unspecified) | None => Err(ResponseConversionError),
    }
}

fn compose(
    tag: u8,
    branch: McpFixedResultBranch,
    payload: Option<McpFixedResultPayload>,
) -> Result<McpToolResult, ResponseConversionError> {
    compose_fixed_tool_result(tag, branch, payload).map_err(|_| ResponseConversionError)
}

fn payload_from<T: Serialize>(value: &T) -> Result<McpFixedResultPayload, ResponseConversionError> {
    McpFixedResultPayload::from_serializable(value).map_err(|_| ResponseConversionError)
}

fn resource_json_from<T: Serialize>(value: &T) -> Result<McpResourceJson, ResponseConversionError> {
    McpResourceJson::from_serializable(value).map_err(|_| ResponseConversionError)
}

#[derive(Serialize)]
#[serde(rename_all = "lowercase")]
enum InvalidDiagnostics {
    Syntax {
        diagnostics: Vec<SyntaxDiagnostic>,
    },
    Semantic {
        diagnostics: Vec<SemanticDiagnostic>,
    },
}

#[derive(Serialize)]
struct Span {
    start: u32,
    end: u32,
}

impl From<v1::SourceSpan> for Span {
    fn from(span: v1::SourceSpan) -> Self {
        Self {
            start: span.start,
            end: span.end,
        }
    }
}

#[derive(Serialize)]
struct SyntaxDiagnostic {
    code: String,
    summary: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    help: Option<String>,
    span: Span,
    expected: Vec<String>,
}

impl TryFrom<v1::SyntaxDiagnostic> for SyntaxDiagnostic {
    type Error = ResponseConversionError;

    fn try_from(value: v1::SyntaxDiagnostic) -> Result<Self, Self::Error> {
        Ok(Self {
            code: value.code,
            summary: value.summary,
            help: value.help,
            span: value.span.ok_or(ResponseConversionError)?.into(),
            expected: value.expected,
        })
    }
}

#[derive(Serialize)]
struct SemanticDiagnostic {
    code: String,
    summary: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    help: Option<String>,
    primary_span: Span,
    #[serde(skip_serializing_if = "Option::is_none")]
    related_span: Option<Span>,
}

impl TryFrom<v1::SemanticDiagnostic> for SemanticDiagnostic {
    type Error = ResponseConversionError;

    fn try_from(value: v1::SemanticDiagnostic) -> Result<Self, Self::Error> {
        Ok(Self {
            code: value.code,
            summary: value.summary,
            help: value.help,
            primary_span: value.primary_span.ok_or(ResponseConversionError)?.into(),
            related_span: value.related_span.map(Into::into),
        })
    }
}

#[derive(Serialize)]
pub(crate) struct ContractDescriptor {
    contract_lineage: String,
    contract_version: McpPresentedU64,
    bundle_hash: McpPresentedHash,
    source_hash: McpPresentedHash,
    plan_root_hash: McpPresentedHash,
}

#[derive(Serialize)]
struct ActiveContractAbsent {
    database: String,
}

#[derive(Serialize)]
struct ActiveContractPresent {
    database: String,
    contract: ContractDescriptor,
}

impl TryFrom<v1::ContractDescriptor> for ContractDescriptor {
    type Error = ResponseConversionError;

    fn try_from(value: v1::ContractDescriptor) -> Result<Self, Self::Error> {
        Ok(Self {
            contract_lineage: value.contract_lineage,
            contract_version: McpPresentedU64::new(value.contract_version),
            bundle_hash: hash(&value.bundle_hash)?,
            source_hash: hash(&value.source_hash)?,
            plan_root_hash: hash(&value.plan_root_hash)?,
        })
    }
}

#[derive(Serialize)]
struct ExpectedActiveVersionMismatch {
    #[serde(skip_serializing_if = "Option::is_none")]
    actual_active_version: Option<McpPresentedU64>,
}

#[derive(Serialize)]
struct ExplainedCommand {
    contract: ContractDescriptor,
    command_id: u32,
    plan_hash: McpPresentedHash,
    explanation: CommandExplain,
    input_schema: GeneratedSchema,
    outcome_schema: GeneratedSchema,
}

#[derive(Serialize)]
struct CommandExplain {
    command_id: u32,
    execution_class: &'static str,
    partition_component_count: u32,
    conflict_key_count: u32,
    binding_ids: Vec<u32>,
    read_fields: Vec<BindingFieldRef>,
    write_fields: Vec<BindingFieldRef>,
    invariant_ids: Vec<u32>,
    event_type_ids: Vec<u32>,
    outcome_ids: Vec<u32>,
    rendered_text: String,
}

impl TryFrom<v1::CommandExplain> for CommandExplain {
    type Error = ResponseConversionError;

    fn try_from(value: v1::CommandExplain) -> Result<Self, Self::Error> {
        Ok(Self {
            command_id: value.command_id,
            execution_class: execution_class(value.execution_class)?,
            partition_component_count: value.partition_component_count,
            conflict_key_count: value.conflict_key_count,
            binding_ids: value.binding_ids,
            read_fields: value.read_fields.into_iter().map(Into::into).collect(),
            write_fields: value.write_fields.into_iter().map(Into::into).collect(),
            invariant_ids: value.invariant_ids,
            event_type_ids: value.event_type_ids,
            outcome_ids: value.outcome_ids,
            rendered_text: value.rendered_text,
        })
    }
}

#[derive(Serialize)]
struct BindingFieldRef {
    binding_id: u32,
    field_id: u32,
}

impl From<v1::BindingFieldRef> for BindingFieldRef {
    fn from(value: v1::BindingFieldRef) -> Self {
        Self {
            binding_id: value.binding_id,
            field_id: value.field_id,
        }
    }
}

#[derive(Clone, Copy)]
enum GeneratedSchemaKeyKind {
    CommandInput,
    CommandOutcomeUnion,
}

#[derive(Serialize)]
struct GeneratedSchema {
    key: GeneratedSchemaKey,
    dialect: String,
    schema_hash: McpPresentedHash,
    schema: SchemaDocument,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum GeneratedSchemaKey {
    CommandInputId(u32),
    CommandOutcomeUnionId(u32),
}

fn generated_schema(
    artifact: v1::GeneratedSchemaArtifact,
    expected_id: u32,
    expected_kind: GeneratedSchemaKeyKind,
) -> Result<GeneratedSchema, ResponseConversionError> {
    let key = artifact
        .key
        .and_then(|key| key.artifact)
        .ok_or(ResponseConversionError)?;
    let (key, kind, actual_id) = match (expected_kind, key) {
        (
            GeneratedSchemaKeyKind::CommandInput,
            v1::schema_artifact_key::Artifact::CommandInputId(id),
        ) => (
            GeneratedSchemaKey::CommandInputId(id),
            McpGeneratedSchemaKind::CommandInput,
            id,
        ),
        (
            GeneratedSchemaKeyKind::CommandOutcomeUnion,
            v1::schema_artifact_key::Artifact::CommandOutcomeUnionId(id),
        ) => (
            GeneratedSchemaKey::CommandOutcomeUnionId(id),
            McpGeneratedSchemaKind::CommandOutcomeUnion,
            id,
        ),
        _ => return Err(ResponseConversionError),
    };
    if actual_id != expected_id
        || artifact.dialect != "https://json-schema.org/draft/2020-12/schema"
    {
        return Err(ResponseConversionError);
    }
    let schema = SchemaDocument::from_public_generated(
        kind,
        actual_id,
        &artifact.schema_hash,
        artifact.canonical_json,
    )
    .map_err(|_| ResponseConversionError)?;
    Ok(GeneratedSchema {
        key,
        dialect: artifact.dialect,
        schema_hash: hash(&artifact.schema_hash)?,
        schema,
    })
}

#[derive(Serialize)]
pub(crate) struct ReplayedExecution {
    status: &'static str,
    commit_sequence: McpPresentedU64,
    contract_version: u64,
    plan_hash: McpPresentedHash,
    outcome_type: String,
    outcome: McpPresentedValue,
    provenance_uri: String,
    durability_mode: String,
    outcome_uri: String,
}

fn replayed_execution(
    response: v1::ExecuteCommandResponse,
) -> Result<ReplayedExecution, ResponseConversionError> {
    if v1::execute_command_response::CompletionStatus::try_from(response.status).ok()
        != Some(v1::execute_command_response::CompletionStatus::Replayed)
    {
        return Err(ResponseConversionError);
    }
    let outcome = presented_value(response.outcome.ok_or(ResponseConversionError)?)?;
    if !matches!(outcome, McpPresentedValue::Record { .. }) {
        return Err(ResponseConversionError);
    }
    Ok(ReplayedExecution {
        status: "replayed",
        commit_sequence: McpPresentedU64::new(response.commit_sequence),
        contract_version: response.contract_version,
        plan_hash: hash(&response.plan_hash)?,
        outcome_type: response.outcome_type,
        outcome,
        provenance_uri: response.provenance_uri,
        durability_mode: response.durability_mode,
        outcome_uri: response.outcome_uri.ok_or(ResponseConversionError)?,
    })
}

#[derive(Serialize)]
struct Entity {
    entity_key: McpPresentedBytes,
    entity_version: McpPresentedU64,
    written_by_contract_version: McpPresentedU64,
    fields: McpPresentedValue,
}

#[derive(Serialize)]
struct IndexPage {
    items: Vec<IndexRow>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<String>,
    observed_fence: IndexFence,
}

#[derive(Serialize)]
struct IndexRow {
    index_entry_key: McpPresentedBytes,
    values: McpPresentedValue,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum IndexFence {
    BeforeFirst(Unit),
    AppliedEpoch(McpPresentedU64),
}

fn index_fence(value: v1::IndexScanFence) -> Result<IndexFence, ResponseConversionError> {
    match value.position.ok_or(ResponseConversionError)? {
        v1::index_scan_fence::Position::BeforeFirst(_) => Ok(IndexFence::BeforeFirst(Unit {})),
        v1::index_scan_fence::Position::AppliedEpoch(epoch) => {
            Ok(IndexFence::AppliedEpoch(McpPresentedU64::new(epoch)))
        }
    }
}

#[derive(Serialize)]
pub(crate) struct Commit {
    commit_sequence: McpPresentedU64,
    admission_request_id: McpPresentedUuid,
    contract_lineage: String,
    contract_version: McpPresentedU64,
    command_id: u32,
    plan_hash: McpPresentedHash,
    canonical_input_hash: McpPresentedHash,
    actor: Actor,
    logical_time: McpPresentedTimestamp,
    partition_hash: McpPresentedHash,
    conflict_hashes: Vec<McpPresentedHash>,
    affected_entities: Vec<AffectedEntity>,
    events: Vec<DurableEvent>,
    outcome: DeclaredOutcome,
    provenance_uri: String,
    durability: &'static str,
}

impl TryFrom<v1::Commit> for Commit {
    type Error = ResponseConversionError;

    fn try_from(value: v1::Commit) -> Result<Self, Self::Error> {
        Ok(Self {
            commit_sequence: McpPresentedU64::new(value.commit_sequence),
            admission_request_id: uuid(&value.admission_request_id)?,
            contract_lineage: value.contract_lineage,
            contract_version: McpPresentedU64::new(value.contract_version),
            command_id: value.command_id,
            plan_hash: hash(&value.plan_hash)?,
            canonical_input_hash: hash(&value.canonical_input_hash)?,
            actor: Actor::try_from(value.actor.ok_or(ResponseConversionError)?)?,
            logical_time: timestamp(value.logical_time.ok_or(ResponseConversionError)?),
            partition_hash: hash(&value.partition_hash)?,
            conflict_hashes: value
                .conflict_hashes
                .iter()
                .map(|value| hash(value))
                .collect::<Result<Vec<_>, _>>()?,
            affected_entities: value
                .affected_entities
                .into_iter()
                .map(AffectedEntity::from)
                .collect(),
            events: value
                .events
                .into_iter()
                .map(DurableEvent::try_from)
                .collect::<Result<Vec<_>, _>>()?,
            outcome: DeclaredOutcome::try_from(value.outcome.ok_or(ResponseConversionError)?)?,
            provenance_uri: value.provenance_uri,
            durability: durability(value.durability)?,
        })
    }
}

#[derive(Serialize)]
struct CommitPage {
    items: Vec<Commit>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<String>,
    observed_fence: Frontier,
}

#[derive(Serialize)]
struct Actor {
    principal_id: String,
    actor_kind: &'static str,
    tenant_scope: TenantScope,
    #[serde(skip_serializing_if = "Option::is_none")]
    agent_session_id: Option<McpPresentedUuid>,
}

impl TryFrom<v1::AdmittedActor> for Actor {
    type Error = ResponseConversionError;

    fn try_from(value: v1::AdmittedActor) -> Result<Self, Self::Error> {
        Ok(Self {
            principal_id: value.principal_id,
            actor_kind: actor_kind(value.actor_kind)?,
            tenant_scope: tenant_scope(value.tenant_scope.ok_or(ResponseConversionError)?)?,
            agent_session_id: value.agent_session_id.as_deref().map(uuid).transpose()?,
        })
    }
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum TenantScope {
    Global(Unit),
    TenantId(String),
}

fn tenant_scope(value: v1::TenantScope) -> Result<TenantScope, ResponseConversionError> {
    match value.scope.ok_or(ResponseConversionError)? {
        v1::tenant_scope::Scope::Global(_) => Ok(TenantScope::Global(Unit {})),
        v1::tenant_scope::Scope::TenantId(value) => Ok(TenantScope::TenantId(value)),
    }
}

#[derive(Serialize)]
struct AffectedEntity {
    entity_key: McpPresentedBytes,
    entity_version: McpPresentedU64,
}

impl From<v1::AffectedEntity> for AffectedEntity {
    fn from(value: v1::AffectedEntity) -> Self {
        Self {
            entity_key: McpPresentedBytes::new(value.entity_key),
            entity_version: McpPresentedU64::new(value.entity_version),
        }
    }
}

#[derive(Serialize)]
struct DurableEvent {
    event_id: EventId,
    event_type_id: u32,
    payload: McpPresentedValue,
}

impl TryFrom<v1::DurableEvent> for DurableEvent {
    type Error = ResponseConversionError;

    fn try_from(value: v1::DurableEvent) -> Result<Self, Self::Error> {
        Ok(Self {
            event_id: value.event_id.ok_or(ResponseConversionError)?.into(),
            event_type_id: value.event_type_id,
            payload: presented_record(value.payload.ok_or(ResponseConversionError)?)?,
        })
    }
}

#[derive(Serialize)]
struct EventId {
    commit_sequence: McpPresentedU64,
    event_ordinal: u32,
}

impl From<v1::EventId> for EventId {
    fn from(value: v1::EventId) -> Self {
        Self {
            commit_sequence: McpPresentedU64::new(value.commit_sequence),
            event_ordinal: value.event_ordinal,
        }
    }
}

#[derive(Serialize)]
struct DeclaredOutcome {
    outcome_id: u32,
    outcome_name: String,
    value: McpPresentedValue,
}

impl TryFrom<v1::DeclaredOutcome> for DeclaredOutcome {
    type Error = ResponseConversionError;

    fn try_from(value: v1::DeclaredOutcome) -> Result<Self, Self::Error> {
        Ok(Self {
            outcome_id: value.outcome_id,
            outcome_name: value.outcome_name,
            value: presented_record(value.value.ok_or(ResponseConversionError)?)?,
        })
    }
}

#[derive(Serialize)]
pub(crate) struct Provenance {
    provenance_id: McpPresentedUuid,
    commit_sequence: McpPresentedU64,
    admission_request_id: McpPresentedUuid,
    contract_lineage: String,
    contract_version: McpPresentedU64,
    command_id: u32,
    plan_hash: McpPresentedHash,
    actor: Actor,
    logical_time: McpPresentedTimestamp,
    outcome_id: u32,
    affected_entities: Vec<AffectedEntity>,
    event_ids: Vec<EventId>,
    claims: ProvenanceClaims,
}

impl TryFrom<v1::Provenance> for Provenance {
    type Error = ResponseConversionError;

    fn try_from(value: v1::Provenance) -> Result<Self, Self::Error> {
        Ok(Self {
            provenance_id: uuid(&value.provenance_id)?,
            commit_sequence: McpPresentedU64::new(value.commit_sequence),
            admission_request_id: uuid(&value.admission_request_id)?,
            contract_lineage: value.contract_lineage,
            contract_version: McpPresentedU64::new(value.contract_version),
            command_id: value.command_id,
            plan_hash: hash(&value.plan_hash)?,
            actor: Actor::try_from(value.actor.ok_or(ResponseConversionError)?)?,
            logical_time: timestamp(value.logical_time.ok_or(ResponseConversionError)?),
            outcome_id: value.outcome_id,
            affected_entities: value
                .affected_entities
                .into_iter()
                .map(AffectedEntity::from)
                .collect(),
            event_ids: value.event_ids.into_iter().map(EventId::from).collect(),
            claims: value.claims.ok_or(ResponseConversionError)?.into(),
        })
    }
}

#[derive(Serialize)]
struct ProvenanceClaims {
    #[serde(skip_serializing_if = "Option::is_none")]
    source_repository: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_commit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    approval_id: Option<String>,
}

impl From<v1::ProvenanceClaims> for ProvenanceClaims {
    fn from(value: v1::ProvenanceClaims) -> Self {
        Self {
            source_repository: value.source_repository,
            source_commit: value.source_commit,
            reason: value.reason,
            approval_id: value.approval_id,
        }
    }
}

#[derive(Serialize)]
struct ProjectionReady {
    data: ProjectionPage,
    frontier: Frontier,
}

#[derive(Serialize)]
struct ProjectionPage {
    items: Vec<ProjectionRow>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<String>,
    observed_fence: ProjectionFence,
}

#[derive(Serialize)]
struct ProjectionRow {
    group: Vec<McpPresentedValue>,
    values: McpPresentedValue,
}

#[derive(Serialize)]
struct ProjectionFence {
    identity: ProjectionIdentity,
    generation: McpPresentedU64,
    frontier: Frontier,
}

impl TryFrom<v1::ProjectionPageFence> for ProjectionFence {
    type Error = ResponseConversionError;

    fn try_from(value: v1::ProjectionPageFence) -> Result<Self, Self::Error> {
        Ok(Self {
            identity: ProjectionIdentity::try_from(value.identity.ok_or(ResponseConversionError)?)?,
            generation: McpPresentedU64::new(value.generation),
            frontier: frontier(value.frontier.ok_or(ResponseConversionError)?)?,
        })
    }
}

#[derive(Serialize)]
pub(crate) struct ProjectionIdentity {
    contract_lineage: String,
    projection_id: u32,
    projection_plan_hash: McpPresentedHash,
}

impl TryFrom<v1::ProjectionIdentity> for ProjectionIdentity {
    type Error = ResponseConversionError;

    fn try_from(value: v1::ProjectionIdentity) -> Result<Self, Self::Error> {
        Ok(Self {
            contract_lineage: value.contract_lineage,
            projection_id: value.projection_id,
            projection_plan_hash: hash(&value.projection_plan_hash)?,
        })
    }
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum Frontier {
    BeforeFirst(Unit),
    AppliedThrough(McpPresentedU64),
}

fn frontier(value: v1::FrontierPosition) -> Result<Frontier, ResponseConversionError> {
    match value.position.ok_or(ResponseConversionError)? {
        v1::frontier_position::Position::BeforeFirst(_) => Ok(Frontier::BeforeFirst(Unit {})),
        v1::frontier_position::Position::AppliedThrough(sequence) => {
            Ok(Frontier::AppliedThrough(McpPresentedU64::new(sequence)))
        }
    }
}

#[derive(Serialize)]
struct ProjectionWaitTimedOut {
    required_sequence: McpPresentedU64,
    current: Frontier,
}

#[derive(Serialize)]
struct ProjectionDegraded {
    current: Frontier,
    reason: ProjectionUnavailableReason,
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum ProjectionUnavailableReason {
    Building(Unit),
    Rebuilding(Unit),
    Failure(&'static str),
}

fn projection_unavailable_reason(
    value: v1::ProjectionUnavailableReason,
) -> Result<ProjectionUnavailableReason, ResponseConversionError> {
    match value.reason.ok_or(ResponseConversionError)? {
        v1::projection_unavailable_reason::Reason::Building(_) => {
            Ok(ProjectionUnavailableReason::Building(Unit {}))
        }
        v1::projection_unavailable_reason::Reason::Rebuilding(_) => {
            Ok(ProjectionUnavailableReason::Rebuilding(Unit {}))
        }
        v1::projection_unavailable_reason::Reason::Failure(value) => Ok(
            ProjectionUnavailableReason::Failure(projection_failure(value)?),
        ),
    }
}

#[derive(Serialize)]
struct ProjectionInvalid {
    reason: &'static str,
}

#[derive(Serialize)]
pub(crate) struct ProjectionStatus {
    identity: ProjectionIdentity,
    lifecycle: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    published: Option<GenerationFrontier>,
    #[serde(skip_serializing_if = "Option::is_none")]
    candidate: Option<GenerationFrontier>,
    #[serde(skip_serializing_if = "Option::is_none")]
    published_apply_mode: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    failure: Option<ProjectionFailure>,
    authoritative_head: Frontier,
}

impl TryFrom<v1::ProjectionStatus> for ProjectionStatus {
    type Error = ResponseConversionError;

    fn try_from(value: v1::ProjectionStatus) -> Result<Self, Self::Error> {
        Ok(Self {
            identity: ProjectionIdentity::try_from(value.identity.ok_or(ResponseConversionError)?)?,
            lifecycle: projection_lifecycle(value.lifecycle)?,
            published: value
                .published
                .map(GenerationFrontier::try_from)
                .transpose()?,
            candidate: value
                .candidate
                .map(GenerationFrontier::try_from)
                .transpose()?,
            published_apply_mode: value
                .published_apply_mode
                .map(published_apply_mode)
                .transpose()?,
            failure: value.failure.map(ProjectionFailure::try_from).transpose()?,
            authoritative_head: frontier(value.authoritative_head.ok_or(ResponseConversionError)?)?,
        })
    }
}

#[derive(Serialize)]
struct GenerationFrontier {
    generation: McpPresentedU64,
    frontier: Frontier,
}

impl TryFrom<v1::ProjectionGenerationFrontier> for GenerationFrontier {
    type Error = ResponseConversionError;

    fn try_from(value: v1::ProjectionGenerationFrontier) -> Result<Self, Self::Error> {
        Ok(Self {
            generation: McpPresentedU64::new(value.generation),
            frontier: frontier(value.frontier.ok_or(ResponseConversionError)?)?,
        })
    }
}

#[derive(Serialize)]
struct ProjectionFailure {
    generation: McpPresentedU64,
    code: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    at_sequence: Option<McpPresentedU64>,
}

impl TryFrom<v1::ProjectionFailure> for ProjectionFailure {
    type Error = ResponseConversionError;

    fn try_from(value: v1::ProjectionFailure) -> Result<Self, Self::Error> {
        Ok(Self {
            generation: McpPresentedU64::new(value.generation),
            code: projection_failure(value.code)?,
            at_sequence: value.at_sequence.map(McpPresentedU64::new),
        })
    }
}

#[derive(Serialize)]
struct OutboxPage {
    items: Vec<OutboxItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<String>,
}

#[derive(Serialize)]
struct OutboxItem {
    event_id: EventId,
    state: &'static str,
    attempts: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_attempt_at: Option<McpPresentedTimestamp>,
}

impl TryFrom<v1::OutboxDeliverySummary> for OutboxItem {
    type Error = ResponseConversionError;

    fn try_from(value: v1::OutboxDeliverySummary) -> Result<Self, Self::Error> {
        Ok(Self {
            event_id: value.event_id.ok_or(ResponseConversionError)?.into(),
            state: outbox_state(value.state)?,
            attempts: value.attempts,
            next_attempt_at: value.next_attempt_at.map(timestamp),
        })
    }
}

#[derive(Serialize)]
struct PreBootstrapHealth {
    database: String,
    lifecycle: &'static str,
    liveness: bool,
    readiness: bool,
}

#[derive(Serialize)]
struct AuthenticatedHealthTool {
    database: String,
    audience: String,
    #[serde(flatten)]
    health: AuthenticatedHealth,
}

#[derive(Serialize)]
pub(crate) struct AuthenticatedHealth {
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    active_contract_version: Option<McpPresentedU64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_commit_sequence: Option<McpPresentedU64>,
    components: Vec<HealthComponent>,
    started_at: McpPresentedTimestamp,
    build: BuildInfo,
}

impl TryFrom<v1::AuthenticatedHealth> for AuthenticatedHealth {
    type Error = ResponseConversionError;

    fn try_from(value: v1::AuthenticatedHealth) -> Result<Self, Self::Error> {
        Ok(Self {
            status: health_status(value.status)?,
            active_contract_version: value.active_contract_version.map(McpPresentedU64::new),
            last_commit_sequence: value.last_commit_sequence.map(McpPresentedU64::new),
            components: value
                .components
                .into_iter()
                .map(HealthComponent::try_from)
                .collect::<Result<Vec<_>, _>>()?,
            started_at: timestamp(value.started_at.ok_or(ResponseConversionError)?),
            build: value.build.ok_or(ResponseConversionError)?.into(),
        })
    }
}

#[derive(Serialize)]
struct HealthComponent {
    component: &'static str,
    status: &'static str,
}

impl TryFrom<v1::HealthComponent> for HealthComponent {
    type Error = ResponseConversionError;

    fn try_from(value: v1::HealthComponent) -> Result<Self, Self::Error> {
        Ok(Self {
            component: health_component(value.component)?,
            status: health_component_status(value.status)?,
        })
    }
}

#[derive(Serialize)]
struct BuildInfo {
    semantic_version: String,
    git_revision: String,
    rust_version: String,
    enabled_features: Vec<String>,
    storage_format_version: u32,
    contract_ir_version: u32,
    mcp_protocol_baseline: String,
}

impl From<v1::BuildInfo> for BuildInfo {
    fn from(value: v1::BuildInfo) -> Self {
        Self {
            semantic_version: value.semantic_version,
            git_revision: value.git_revision,
            rust_version: value.rust_version,
            enabled_features: value.enabled_features,
            storage_format_version: value.storage_format_version,
            contract_ir_version: value.contract_ir_version,
            mcp_protocol_baseline: value.mcp_protocol_baseline,
        }
    }
}

pub(crate) fn presented_record(
    record: v1::ValueRecord,
) -> Result<McpPresentedValue, ResponseConversionError> {
    let mut previous = 0_u32;
    let fields = record
        .fields
        .into_iter()
        .map(|field| {
            let field_id = field
                .field_id
                .filter(|field_id| *field_id > previous)
                .ok_or(ResponseConversionError)?;
            if !field.name.is_empty() && !is_source_name(&field.name) {
                return Err(ResponseConversionError);
            }
            previous = field_id;
            Ok(McpPresentedField {
                field_id,
                value: presented_value(field.value.ok_or(ResponseConversionError)?)?,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(McpPresentedValue::Record { fields })
}

pub(crate) fn presented_value(
    value: v1::Value,
) -> Result<McpPresentedValue, ResponseConversionError> {
    use v1::value::Kind;

    match value.kind.ok_or(ResponseConversionError)? {
        Kind::NullValue(value) if value == v1::NullValue::NullValue as i32 => {
            Ok(McpPresentedValue::Null)
        }
        Kind::NullValue(_) => Err(ResponseConversionError),
        Kind::BoolValue(value) => Ok(McpPresentedValue::Bool { value }),
        Kind::I64Value(value) => Ok(McpPresentedValue::I64 {
            value: McpPresentedI64::new(value),
        }),
        Kind::U64Value(value) => Ok(McpPresentedValue::U64 {
            value: McpPresentedU64::new(value),
        }),
        Kind::DecimalValue(value) => {
            let (precision, scale, coefficient) = presented_decimal(value)?;
            Ok(McpPresentedValue::Decimal {
                precision,
                scale,
                coefficient,
            })
        }
        Kind::MoneyValue(value) => {
            if value.currency.len() != 3
                || !value.currency.bytes().all(|byte| byte.is_ascii_uppercase())
            {
                return Err(ResponseConversionError);
            }
            let (precision, scale, coefficient) =
                presented_decimal(value.amount.ok_or(ResponseConversionError)?)?;
            Ok(McpPresentedValue::Money {
                currency: value.currency,
                precision,
                scale,
                coefficient,
            })
        }
        Kind::StringValue(value) => Ok(McpPresentedValue::String { value }),
        Kind::BytesValue(value) => Ok(McpPresentedValue::Bytes {
            value: McpPresentedBytes::new(value),
        }),
        Kind::UuidValue(value) => Ok(McpPresentedValue::Uuid {
            value: uuid(&value)?,
        }),
        Kind::DateValue(value) => Ok(McpPresentedValue::Date {
            days_since_unix_epoch: value.days_since_unix_epoch,
        }),
        Kind::TimestampValue(value) => Ok(McpPresentedValue::Timestamp {
            seconds: McpPresentedI64::new(value.seconds),
            nanos: value.nanos,
        }),
        Kind::EnumValue(value)
            if value.type_id != 0
                && value.variant_id != 0
                && (value.name.is_empty() || is_source_name(&value.name)) =>
        {
            Ok(McpPresentedValue::Enum {
                type_id: value.type_id,
                variant_id: value.variant_id,
            })
        }
        Kind::EnumValue(_) => Err(ResponseConversionError),
        Kind::VectorValue(value) => {
            McpPresentedValue::vector(value.components).map_err(|_| ResponseConversionError)
        }
        Kind::ListValue(value) => Ok(McpPresentedValue::List {
            values: value
                .values
                .into_iter()
                .map(presented_value)
                .collect::<Result<Vec<_>, _>>()?,
        }),
        Kind::RecordValue(value) => presented_record(value),
    }
}

fn presented_decimal(value: v1::Decimal) -> Result<(u8, u8, String), ResponseConversionError> {
    let precision = u8::try_from(value.precision.ok_or(ResponseConversionError)?)
        .map_err(|_| ResponseConversionError)?;
    let scale = u8::try_from(value.scale).map_err(|_| ResponseConversionError)?;
    if !(1..=38).contains(&precision) || scale > precision {
        return Err(ResponseConversionError);
    }
    let coefficient = decode_minimal_i128(&value.coefficient_twos_complement)?;
    if coefficient.unsigned_abs() >= 10_u128.pow(u32::from(precision)) {
        return Err(ResponseConversionError);
    }
    Ok((precision, scale, coefficient.to_string()))
}

fn decode_minimal_i128(bytes: &[u8]) -> Result<i128, ResponseConversionError> {
    if bytes.is_empty() || bytes.len() > 16 {
        return Err(ResponseConversionError);
    }
    if bytes.len() > 1 {
        let redundant_positive = bytes[0] == 0 && bytes[1] & 0x80 == 0;
        let redundant_negative = bytes[0] == 0xff && bytes[1] & 0x80 != 0;
        if redundant_positive || redundant_negative {
            return Err(ResponseConversionError);
        }
    }
    let mut decoded = if bytes[0] & 0x80 == 0 {
        [0_u8; 16]
    } else {
        [0xff_u8; 16]
    };
    let start = decoded.len() - bytes.len();
    decoded[start..].copy_from_slice(bytes);
    Ok(i128::from_be_bytes(decoded))
}

fn hash(value: &[u8]) -> Result<McpPresentedHash, ResponseConversionError> {
    McpPresentedHash::from_slice(value).map_err(|_| ResponseConversionError)
}

fn exact_hash(value: &[u8]) -> Result<[u8; 32], ResponseConversionError> {
    value.try_into().map_err(|_| ResponseConversionError)
}

fn is_source_name(value: &str) -> bool {
    let mut bytes = value.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
        && value.len() <= 256
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn uuid(value: &[u8]) -> Result<McpPresentedUuid, ResponseConversionError> {
    let bytes: [u8; 16] = value.try_into().map_err(|_| ResponseConversionError)?;
    Ok(McpPresentedUuid::new(bytes))
}

fn timestamp(value: v1::Timestamp) -> McpPresentedTimestamp {
    McpPresentedTimestamp {
        seconds: McpPresentedI64::new(value.seconds),
        nanos: value.nanos,
    }
}

fn cursor_text(value: Option<Vec<u8>>) -> Result<Option<String>, ResponseConversionError> {
    value
        .map(|value| {
            let cursor: [u8; 16] = value.try_into().map_err(|_| ResponseConversionError)?;
            Ok(encode_mcp_cursor(cursor))
        })
        .transpose()
}

fn actor_kind(value: i32) -> Result<&'static str, ResponseConversionError> {
    match v1::ActorKind::try_from(value).ok() {
        Some(v1::ActorKind::Human) => Ok("human"),
        Some(v1::ActorKind::Agent) => Ok("agent"),
        Some(v1::ActorKind::Service) => Ok("service"),
        Some(v1::ActorKind::Unspecified) | None => Err(ResponseConversionError),
    }
}

fn execution_class(value: i32) -> Result<&'static str, ResponseConversionError> {
    match v1::ExecutionClass::try_from(value).ok() {
        Some(v1::ExecutionClass::ReadOnly) => Ok("read_only"),
        Some(v1::ExecutionClass::IdempotentMutation) => Ok("idempotent_mutation"),
        Some(v1::ExecutionClass::Unspecified) | None => Err(ResponseConversionError),
    }
}

fn durability(value: i32) -> Result<&'static str, ResponseConversionError> {
    match v1::CommandDurability::try_from(value).ok() {
        Some(v1::CommandDurability::Synchronous) => Ok("sync"),
        Some(v1::CommandDurability::Group) => Ok("group"),
        Some(v1::CommandDurability::Unspecified) | None => Err(ResponseConversionError),
    }
}

fn projection_failure(value: i32) -> Result<&'static str, ResponseConversionError> {
    match v1::ProjectionFailureCode::try_from(value).ok() {
        Some(v1::ProjectionFailureCode::ArithmeticOverflow) => Ok("arithmetic_overflow"),
        Some(v1::ProjectionFailureCode::MalformedDurableEvent) => Ok("malformed_durable_event"),
        Some(v1::ProjectionFailureCode::MissingCommit) => Ok("missing_commit"),
        Some(v1::ProjectionFailureCode::PlanOrSchemaUnavailable) => {
            Ok("plan_or_schema_unavailable")
        }
        Some(v1::ProjectionFailureCode::ProjectionStateIntegrity) => {
            Ok("projection_state_integrity")
        }
        Some(v1::ProjectionFailureCode::HardLimitExceeded) => Ok("hard_limit_exceeded"),
        Some(v1::ProjectionFailureCode::Unspecified) | None => Err(ResponseConversionError),
    }
}

fn projection_lifecycle(value: i32) -> Result<&'static str, ResponseConversionError> {
    match v1::ProjectionLifecycle::try_from(value).ok() {
        Some(v1::ProjectionLifecycle::Building) => Ok("building"),
        Some(v1::ProjectionLifecycle::CatchingUp) => Ok("catching_up"),
        Some(v1::ProjectionLifecycle::Ready) => Ok("ready"),
        Some(v1::ProjectionLifecycle::Rebuilding) => Ok("rebuilding"),
        Some(v1::ProjectionLifecycle::Degraded) => Ok("degraded"),
        Some(v1::ProjectionLifecycle::Invalid) => Ok("invalid"),
        Some(v1::ProjectionLifecycle::Unspecified) | None => Err(ResponseConversionError),
    }
}

fn published_apply_mode(value: i32) -> Result<&'static str, ResponseConversionError> {
    match v1::PublishedApplyMode::try_from(value).ok() {
        Some(v1::PublishedApplyMode::Enabled) => Ok("enabled"),
        Some(v1::PublishedApplyMode::Suspended) => Ok("suspended"),
        Some(v1::PublishedApplyMode::Unspecified) | None => Err(ResponseConversionError),
    }
}

fn outbox_state(value: i32) -> Result<&'static str, ResponseConversionError> {
    match v1::OutboxDeliveryState::try_from(value).ok() {
        Some(v1::OutboxDeliveryState::Pending) => Ok("pending"),
        Some(v1::OutboxDeliveryState::RetryScheduled) => Ok("retry_scheduled"),
        Some(v1::OutboxDeliveryState::Delivering) => Ok("delivering"),
        Some(v1::OutboxDeliveryState::DeadLetter) => Ok("dead_letter"),
        Some(v1::OutboxDeliveryState::Unspecified) | None => Err(ResponseConversionError),
    }
}

fn pre_bootstrap_lifecycle(value: i32) -> Result<&'static str, ResponseConversionError> {
    match v1::PreBootstrapLifecycle::try_from(value).ok() {
        Some(v1::PreBootstrapLifecycle::InitializingValidation) => Ok("initializing_validation"),
        Some(v1::PreBootstrapLifecycle::InitializingBootstrap) => Ok("initializing_bootstrap"),
        Some(v1::PreBootstrapLifecycle::Unspecified) | None => Err(ResponseConversionError),
    }
}

fn health_status(value: i32) -> Result<&'static str, ResponseConversionError> {
    match v1::HealthStatus::try_from(value).ok() {
        Some(v1::HealthStatus::Ready) => Ok("ready"),
        Some(v1::HealthStatus::NotReady) => Ok("not_ready"),
        Some(v1::HealthStatus::Degraded) => Ok("degraded"),
        Some(v1::HealthStatus::Unspecified) | None => Err(ResponseConversionError),
    }
}

fn health_component(value: i32) -> Result<&'static str, ResponseConversionError> {
    match v1::HealthComponentKind::try_from(value).ok() {
        Some(v1::HealthComponentKind::AuthoritativeStorage) => Ok("authoritative_storage"),
        Some(v1::HealthComponentKind::Catalog) => Ok("catalog"),
        Some(v1::HealthComponentKind::CommitCoordinator) => Ok("commit_coordinator"),
        Some(v1::HealthComponentKind::Projection) => Ok("projection"),
        Some(v1::HealthComponentKind::Outbox) => Ok("outbox"),
        Some(v1::HealthComponentKind::VectorStaleness) => Ok("vector_staleness"),
        Some(v1::HealthComponentKind::Unspecified) | None => Err(ResponseConversionError),
    }
}

fn health_component_status(value: i32) -> Result<&'static str, ResponseConversionError> {
    match v1::HealthComponentStatus::try_from(value).ok() {
        Some(v1::HealthComponentStatus::Healthy) => Ok("healthy"),
        Some(v1::HealthComponentStatus::Degraded) => Ok("degraded"),
        Some(v1::HealthComponentStatus::Unavailable) => Ok("unavailable"),
        Some(v1::HealthComponentStatus::Unspecified) | None => Err(ResponseConversionError),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn application_catalog_preserves_only_the_symbolic_public_page() {
        let result = application_catalog(app_v1::GetApplicationCatalogResponse {
            schema: "riffdb.application_catalog.v1".to_owned(),
            contract_lineage: "Example".to_owned(),
            contract_version: 7,
            contract_bundle_hash: vec![0xab; 32],
            query_module_hashes: vec![vec![0xcd; 32]],
            symbols: vec![app_v1::ApplicationCatalogSymbol {
                kind: app_v1::ApplicationCatalogSymbolKind::Field.into(),
                path: vec!["Ticket".to_owned(), "title".to_owned()],
                public_type: Some("String<1..200>".to_owned()),
                source_span: Some(app_v1::ApplicationCatalogSourceSpan { start: 11, end: 16 }),
            }],
            features: vec![app_v1::ApplicationCatalogFeatureView {
                feature: app_v1::ApplicationCatalogFeature::BinaryTextPrefix.into(),
                state: app_v1::ApplicationCatalogFeatureState::Available.into(),
            }],
            has_more: true,
            next_cursor: Some(URL_SAFE_NO_PAD.encode([7_u8; riffdb_api_mcp::MCP_CURSOR_BYTES])),
        })
        .expect("catalog response converts");

        assert_eq!(
            result,
            McpToolResult::from_serializable(&serde_json::json!({
                "page": {
                    "schema": "riffdb.application_catalog.v1",
                    "contract": {
                        "lineage": "Example",
                        "version": "7",
                        "bundle_hash": "ab".repeat(32),
                    },
                    "query_module_hashes": ["cd".repeat(32)],
                    "symbols": [{
                        "kind": "field",
                        "path": ["Ticket", "title"],
                        "public_type": "String<1..200>",
                        "source_span": {"start": 11, "end": 16},
                    }],
                    "features": [{
                        "feature": "binary_text_prefix",
                        "state": "available",
                    }],
                    "has_more": true,
                    "next_cursor": "07".repeat(riffdb_api_mcp::MCP_CURSOR_BYTES),
                }
            }))
            .expect("expected MCP result")
        );

        assert!(
            application_catalog(app_v1::GetApplicationCatalogResponse {
                schema: "riffdb.application_catalog.v1".to_owned(),
                contract_lineage: "Example".to_owned(),
                contract_version: 7,
                contract_bundle_hash: vec![0xab; 31],
                query_module_hashes: Vec::new(),
                symbols: Vec::new(),
                features: Vec::new(),
                has_more: false,
                next_cursor: None,
            })
            .is_err()
        );
    }

    #[test]
    fn empty_event_pull_is_a_valid_completed_mcp_result() {
        let result = event_next(v1::ConsumeEventStreamResponse {
            events: Vec::new(),
            status: Some(v1::EventConsumerStatus {
                revision: 1,
                checkpoint: Some(v1::EventConsumerCheckpoint {
                    position: Some(v1::event_consumer_checkpoint::Position::BeforeFirst(
                        v1::Unit {},
                    )),
                }),
                history_incarnation: 1,
                live_leases: 0,
                retries: 0,
                dead_letters: 0,
            }),
            wait_timed_out: false,
            protected_status: None,
            disposition: v1::EventConsumerPullDisposition::Ready as i32,
        })
        .expect("empty pull response");

        assert_eq!(
            result,
            McpToolResult::from_serializable(&serde_json::json!({
                "completed": {
                    "events": [],
                    "status": {
                        "revision": "1",
                        "checkpoint": "before-first",
                        "history_incarnation": "1",
                        "live_leases": 0,
                        "retries": 0,
                        "dead_letters": 0
                    },
                    "wait_timed_out": false,
                    "disposition": "ready"
                }
            }))
            .expect("expected MCP result")
        );
    }

    fn allocate_budget_definition() -> riffdb_api_mcp::McpDynamicToolDefinition {
        let input_schema = SchemaDocument::from_public_generated(
            McpGeneratedSchemaKind::CommandInput,
            2,
            &[
                0xc7, 0x74, 0xaf, 0x50, 0xdb, 0x4c, 0x20, 0x34, 0xe4, 0x1a, 0x1b, 0x16, 0xd5, 0x5b,
                0xf7, 0x08, 0x51, 0x93, 0xd6, 0xd9, 0x65, 0xf7, 0x69, 0x81, 0xfb, 0x24, 0x4e, 0x7b,
                0x02, 0x02, 0x5a, 0x39,
            ],
            include_str!("../../../fixtures/compiler/schemas/03-00000002.json")
                .strip_suffix('\n')
                .expect("canonical fixture"),
        )
        .expect("input schema");
        let outcome_schema = SchemaDocument::from_public_generated(
            McpGeneratedSchemaKind::CommandOutcomeUnion,
            2,
            &[
                0xf7, 0x11, 0xc1, 0x59, 0x6d, 0xee, 0x5a, 0x94, 0xf0, 0x3d, 0x6b, 0x72, 0x7c, 0xc4,
                0x7e, 0xbc, 0x9a, 0x35, 0xe6, 0xf9, 0xa3, 0x5c, 0xd6, 0xa8, 0x4b, 0x52, 0xcd, 0xeb,
                0xd3, 0x85, 0x0f, 0x6c,
            ],
            include_str!("../../../fixtures/compiler/schemas/04-00000002.json")
                .strip_suffix('\n')
                .expect("canonical fixture"),
        )
        .expect("outcome schema");
        riffdb_api_mcp::McpDynamicToolDefinition::from_discovered_command(
            "riffdb_cmd_legalspend_allocatebudget",
            input_schema,
            outcome_schema,
        )
        .expect("definition")
    }

    fn read_only_invalid_amount_response() -> v1::ExecuteCommandResponse {
        v1::ExecuteCommandResponse {
            status: v1::execute_command_response::CompletionStatus::ExecutedReadOnly as i32,
            commit_sequence: 0,
            contract_version: 1,
            plan_hash: vec![0x07; 32],
            outcome_type: "InvalidAmount".to_owned(),
            outcome: Some(v1::Value {
                kind: Some(v1::value::Kind::RecordValue(v1::ValueRecord {
                    fields: vec![v1::ValueField {
                        field_id: Some(1),
                        name: "minimum".to_owned(),
                        value: Some(v1::Value {
                            kind: Some(v1::value::Kind::DecimalValue(v1::Decimal {
                                coefficient_twos_complement: vec![0],
                                scale: 2,
                                precision: Some(2),
                            })),
                        }),
                    }],
                })),
            }),
            provenance_uri: String::new(),
            durability_mode: String::new(),
            outcome_uri: None,
            history_incarnation: 1,
        }
    }

    #[test]
    fn selected_database_and_audience_survive_fixed_mcp_conversion() {
        let active = get_active_contract(v1::GetActiveContractResponse {
            result: Some(v1::get_active_contract_response::Result::Absent(
                v1::Unit {},
            )),
            database_alias: "ea".to_owned(),
        })
        .expect("active response");
        assert_eq!(
            active,
            McpToolResult::from_serializable(&serde_json::json!({"absent": {"database": "ea"}}))
                .expect("expected active result")
        );

        let health = health(v1::HealthResponse {
            result: Some(v1::health_response::Result::Authenticated(
                v1::AuthenticatedHealth {
                    status: v1::HealthStatus::Ready as i32,
                    active_contract_version: Some(1),
                    last_commit_sequence: Some(2),
                    components: Vec::new(),
                    started_at: Some(v1::Timestamp {
                        seconds: 3,
                        nanos: 4,
                    }),
                    build: Some(v1::BuildInfo {
                        semantic_version: "0.1.0".to_owned(),
                        git_revision: "test".to_owned(),
                        rust_version: "1.97.0".to_owned(),
                        enabled_features: Vec::new(),
                        storage_format_version: 1,
                        contract_ir_version: 1,
                        mcp_protocol_baseline: "2025-11-25".to_owned(),
                    }),
                    history_incarnation: 1,
                },
            )),
            database_alias: "ea".to_owned(),
            authentication_audience: "riffdb-grpc-loopback".to_owned(),
        })
        .expect("health response");
        assert_eq!(
            health,
            McpToolResult::from_serializable(&serde_json::json!({
                "authenticated": {
                    "database": "ea",
                    "audience": "riffdb-grpc-loopback",
                    "status": "ready",
                    "active_contract_version": "1",
                    "last_commit_sequence": "2",
                    "components": [],
                    "started_at": {"seconds": "3", "nanos": 4},
                    "build": {
                        "semantic_version": "0.1.0",
                        "git_revision": "test",
                        "rust_version": "1.97.0",
                        "enabled_features": [],
                        "storage_format_version": 1,
                        "contract_ir_version": 1,
                        "mcp_protocol_baseline": "2025-11-25"
                    }
                }
            }))
            .expect("expected health result")
        );
    }

    #[test]
    fn dynamic_result_uses_exact_historical_schema_and_read_only_sentinels() {
        let definition = allocate_budget_definition();
        let expected = McpToolResult::from_json_bytes(
            br#"{
                "commit_sequence": null,
                "contract_version": 1,
                "durability_mode": null,
                "outcome": {"minimum": "0.00", "type": "InvalidAmount"},
                "outcome_uri": null,
                "plan_hash": "0707070707070707070707070707070707070707070707070707070707070707",
                "provenance_uri": null,
                "status": "executed_read_only"
            }"#,
        )
        .expect("expected result");
        assert_eq!(
            dynamic_command_result(
                read_only_invalid_amount_response(),
                1,
                definition.outcome_schema(),
                definition.result_schema(),
            ),
            Ok(expected)
        );

        let mut absent_precision = read_only_invalid_amount_response();
        let Some(v1::Value {
            kind: Some(v1::value::Kind::RecordValue(record)),
        }) = absent_precision.outcome.as_mut()
        else {
            panic!("record outcome")
        };
        let Some(v1::Value {
            kind: Some(v1::value::Kind::DecimalValue(decimal)),
        }) = record.fields[0].value.as_mut()
        else {
            panic!("decimal field")
        };
        decimal.precision = None;
        assert!(
            dynamic_command_result(
                absent_precision,
                1,
                definition.outcome_schema(),
                definition.result_schema(),
            )
            .is_err()
        );

        let mut wrong_name = read_only_invalid_amount_response();
        let Some(v1::Value {
            kind: Some(v1::value::Kind::RecordValue(record)),
        }) = wrong_name.outcome.as_mut()
        else {
            panic!("record outcome")
        };
        record.fields[0].name = "guessed".to_owned();
        assert!(
            dynamic_command_result(
                wrong_name,
                1,
                definition.outcome_schema(),
                definition.result_schema(),
            )
            .is_err()
        );

        let mut invalid_sentinel = read_only_invalid_amount_response();
        invalid_sentinel.durability_mode = "sync".to_owned();
        assert!(
            dynamic_command_result(
                invalid_sentinel,
                1,
                definition.outcome_schema(),
                definition.result_schema(),
            )
            .is_err()
        );
        assert!(
            dynamic_command_result(
                read_only_invalid_amount_response(),
                2,
                definition.outcome_schema(),
                definition.result_schema(),
            )
            .is_err()
        );
    }

    #[test]
    fn dynamic_vector_result_preserves_committed_replayed_and_read_only_parity() {
        const VECTOR_OUTCOME_SCHEMA: &str = concat!(
            "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",",
            "\"oneOf\":[{\"additionalProperties\":false,\"properties\":{",
            "\"embedding\":{\"items\":{\"type\":\"number\"},\"maxItems\":3,",
            "\"minItems\":3,\"type\":\"array\",\"x-riffdb-vectorDimension\":3},",
            "\"type\":{\"const\":\"Embedded\"}},",
            "\"required\":[\"type\",\"embedding\"],\"type\":\"object\"}]}"
        );
        let outcome_schema = SchemaDocument::from_public_parts(
            "riffdb.generated-schema/command-outcome-union/9/v1",
            riffdb_types::hash_schema(VECTOR_OUTCOME_SCHEMA.as_bytes()).as_bytes(),
            VECTOR_OUTCOME_SCHEMA,
        )
        .expect("vector outcome schema");
        let input_source = concat!(
            "{\"$schema\":\"https://json-schema.org/draft/2020-12/schema\",",
            "\"additionalProperties\":false,\"properties\":{},",
            "\"required\":[],\"type\":\"object\"}"
        );
        let input_schema = SchemaDocument::from_public_parts(
            "riffdb.generated-schema/command-input/9/v1",
            riffdb_types::hash_schema(input_source.as_bytes()).as_bytes(),
            input_source,
        )
        .expect("vector input schema");
        let definition = riffdb_api_mcp::McpDynamicToolDefinition::from_discovered_command(
            "riffdb_cmd_vectors_embed",
            input_schema,
            outcome_schema,
        )
        .expect("vector command definition");
        let response = |status| {
            let read_only =
                status == v1::execute_command_response::CompletionStatus::ExecutedReadOnly;
            v1::ExecuteCommandResponse {
                status: status as i32,
                commit_sequence: if read_only { 0 } else { 1 },
                contract_version: 9,
                plan_hash: vec![0x44; 32],
                outcome_type: "Embedded".to_owned(),
                outcome: Some(v1::Value {
                    kind: Some(v1::value::Kind::RecordValue(v1::ValueRecord {
                        fields: vec![v1::ValueField {
                            field_id: Some(1),
                            name: "embedding".to_owned(),
                            value: Some(v1::Value {
                                kind: Some(v1::value::Kind::VectorValue(v1::VectorValue {
                                    components: vec![-0.0, 1.5, -2.25],
                                })),
                            }),
                        }],
                    })),
                }),
                provenance_uri: if read_only {
                    String::new()
                } else {
                    "riffdb://provenance/00000000-0001-7000-8000-000000000000".to_owned()
                },
                durability_mode: if read_only {
                    String::new()
                } else {
                    "sync".to_owned()
                },
                outcome_uri: (!read_only).then(|| {
                    concat!(
                        "riffdb://outcome/actor/orders/1/riffdb_cmd_orders_place/",
                        "AQAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
                    )
                    .to_owned()
                }),
                history_incarnation: 1,
            }
        };

        for status in [
            v1::execute_command_response::CompletionStatus::Committed,
            v1::execute_command_response::CompletionStatus::Replayed,
            v1::execute_command_response::CompletionStatus::ExecutedReadOnly,
        ] {
            dynamic_command_result(
                response(status),
                9,
                definition.outcome_schema(),
                definition.result_schema(),
            )
            .expect("vector dynamic result");
        }
    }

    #[test]
    fn decimal_and_money_without_public_precision_fail_closed() {
        for kind in [
            v1::value::Kind::DecimalValue(v1::Decimal {
                coefficient_twos_complement: vec![1],
                scale: 0,
                precision: None,
            }),
            v1::value::Kind::MoneyValue(v1::Money {
                currency: "USD".to_owned(),
                amount: Some(v1::Decimal {
                    coefficient_twos_complement: vec![1],
                    scale: 0,
                    precision: None,
                }),
            }),
        ] {
            assert_eq!(
                presented_value(v1::Value { kind: Some(kind) }),
                Err(ResponseConversionError)
            );
        }
    }

    #[test]
    fn decimal_precision_is_preserved_exactly() {
        assert_eq!(
            presented_value(v1::Value {
                kind: Some(v1::value::Kind::DecimalValue(v1::Decimal {
                    coefficient_twos_complement: vec![0x7b],
                    scale: 2,
                    precision: Some(4),
                })),
            }),
            Ok(McpPresentedValue::Decimal {
                precision: 4,
                scale: 2,
                coefficient: "123".to_owned(),
            })
        );
        assert!(
            presented_value(v1::Value {
                kind: Some(v1::value::Kind::DecimalValue(v1::Decimal {
                    coefficient_twos_complement: vec![0, 1],
                    scale: 0,
                    precision: Some(1),
                })),
            })
            .is_err()
        );
    }

    #[test]
    fn record_requires_increasing_ids_and_checked_redundant_names() {
        let value = |field_id, name: &str| v1::ValueField {
            field_id,
            name: name.to_owned(),
            value: Some(v1::Value {
                kind: Some(v1::value::Kind::BoolValue(true)),
            }),
        };
        assert!(
            presented_record(v1::ValueRecord {
                fields: vec![value(Some(1), ""), value(Some(2), "")],
            })
            .is_ok()
        );
        assert!(
            presented_record(v1::ValueRecord {
                fields: vec![value(Some(2), ""), value(Some(1), "")],
            })
            .is_err()
        );
        assert!(
            presented_record(v1::ValueRecord {
                fields: vec![value(Some(1), "field")],
            })
            .is_ok()
        );
        assert!(
            presented_record(v1::ValueRecord {
                fields: vec![value(Some(1), "not-a-source-name")],
            })
            .is_err()
        );
    }

    #[test]
    fn fixed_tagged_enum_accepts_checked_redundant_name() {
        assert_eq!(
            presented_value(v1::Value {
                kind: Some(v1::value::Kind::EnumValue(v1::EnumValue {
                    type_id: 2,
                    variant_id: 3,
                    name: "Approved".to_owned(),
                })),
            }),
            Ok(McpPresentedValue::Enum {
                type_id: 2,
                variant_id: 3,
            })
        );
    }
}
