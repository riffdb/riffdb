//! Wire conversion for the projected columnar query surface (CP2b).
//!
//! Kept out of `conversion.rs` to avoid merge contention on that file.

use std::time::Duration;

use riffdb_proto::{app::v1 as app_v1, canonical_value_from_proto, canonical_value_to_proto};
use riffdb_service::{
    ExecuteProjectedQueryRequest, ExecuteProjectedQueryResult, ProjectedAggregateOp,
    ProjectedColumnPredicate, ProjectedDegradedReason, ProjectedGroupBySpec, ProjectedOrderSpec,
    ProjectedQueryBody, ProjectedRebuildingReason, ProjectedSortDirection,
    SymbolicContractSelector,
};
use riffdb_types::{CanonicalValue, CommitToken, FreshnessPolicy, ProjectionFrontier, RequestId};
use tonic::Status;

use crate::conversion::{
    INVALID_REQUEST_MESSAGE, request_id_from_bytes, symbolic_contract_selector_from_proto,
};

/// Parses one projected-query request from the public wire shape.
pub fn execute_projected_query_request_from_proto(
    request: app_v1::ExecuteProjectedQueryRequest,
) -> Result<(RequestId, ExecuteProjectedQueryRequest), Status> {
    let request_id = request_id_from_bytes(&request.request_id)?;
    if request.projection_name.is_empty() || request.projection_name.len() > 256 {
        return Err(invalid_request());
    }
    let contract = symbolic_contract_selector_from_proto(request.contract)?;
    let body = projected_query_body_from_proto(request.request.ok_or_else(invalid_request)?)?;
    let freshness = freshness_policy_from_proto(request.freshness.ok_or_else(invalid_request)?)?;
    let mut domain =
        ExecuteProjectedQueryRequest::new(contract, request.projection_name, body, freshness);
    domain = domain.with_request_id(request_id);
    Ok((request_id, domain))
}

/// Encodes one projected-query outcome for the public wire.
pub fn execute_projected_query_result_to_proto(
    result: ExecuteProjectedQueryResult,
) -> Result<app_v1::ExecuteProjectedQueryResponse, Status> {
    use app_v1::execute_projected_query_response::Outcome;
    let outcome = match result {
        ExecuteProjectedQueryResult::Ready {
            fields,
            field_ids: _,
            primary_key_fields,
            rows,
            result: _,
            frontier,
            head,
            commit_token,
        } => {
            // Rows expose select cells then primary-key components under the
            // combined field name list (select first, then PK names not already
            // present in select). Wire clients read ResultRecord fields by name.
            let mut wire_fields = fields.clone();
            for pk_name in &primary_key_fields {
                if !wire_fields.iter().any(|name| name == pk_name) {
                    wire_fields.push(pk_name.clone());
                }
            }
            let mut wire_rows = Vec::with_capacity(rows.len());
            for row in rows {
                let mut record_fields = Vec::with_capacity(wire_fields.len());
                for (index, name) in fields.iter().enumerate() {
                    let value = row.cells.get(index).ok_or_else(internal_defect)?;
                    record_fields.push(app_v1::Parameter {
                        name: name.clone(),
                        value: Some(
                            canonical_value_to_proto(value).map_err(|_| invalid_request())?,
                        ),
                    });
                }
                for (index, name) in primary_key_fields.iter().enumerate() {
                    if fields.iter().any(|select| select == name) {
                        continue;
                    }
                    let value = row.primary_key.get(index).ok_or_else(internal_defect)?;
                    record_fields.push(app_v1::Parameter {
                        name: name.clone(),
                        value: Some(
                            canonical_value_to_proto(value).map_err(|_| invalid_request())?,
                        ),
                    });
                }
                wire_rows.push(app_v1::ResultRecord {
                    fields: record_fields,
                    entity: String::new(),
                });
            }
            Outcome::Ready(app_v1::ProjectedQueryReady {
                fields: wire_fields,
                rows: wire_rows,
                frontier: frontier.as_bytes().to_vec(),
                head: head.as_bytes().to_vec(),
                commit_token: commit_token
                    .map(|token| token.into_bytes())
                    .unwrap_or_default(),
            })
        }
        ExecuteProjectedQueryResult::Lagging {
            required,
            current,
            head,
            lag_sequences,
            retry_after,
        } => Outcome::Lagging(app_v1::ProjectedQueryLagging {
            required: required.as_bytes().to_vec(),
            current: current.as_bytes().to_vec(),
            head: head.as_bytes().to_vec(),
            lag_sequences,
            retry_after_nanos: retry_after
                .map(|duration| u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)),
        }),
        ExecuteProjectedQueryResult::Building {
            applied_through,
            head,
        } => Outcome::Building(app_v1::ProjectedQueryBuilding {
            applied_through: applied_through.as_bytes().to_vec(),
            head: head.as_bytes().to_vec(),
        }),
        ExecuteProjectedQueryResult::Rebuilding {
            reason,
            progress_applied,
            progress_total,
        } => Outcome::Rebuilding(app_v1::ProjectedQueryRebuilding {
            reason: rebuilding_reason_to_proto(reason).into(),
            progress_applied,
            progress_total,
        }),
        ExecuteProjectedQueryResult::Degraded {
            reason,
            current_frontier,
        } => Outcome::Degraded(app_v1::ProjectedQueryDegraded {
            reason: degraded_reason_to_proto(reason).into(),
            current: current_frontier.as_bytes().to_vec(),
        }),
        ExecuteProjectedQueryResult::Invalid {
            expected_fingerprint,
            found_fingerprint,
        } => Outcome::Invalid(app_v1::ProjectedQueryInvalid {
            expected_fingerprint: expected_fingerprint.as_bytes().to_vec(),
            found_fingerprint: found_fingerprint.as_bytes().to_vec(),
        }),
    };
    Ok(app_v1::ExecuteProjectedQueryResponse {
        outcome: Some(outcome),
    })
}

fn projected_query_body_from_proto(
    body: app_v1::ProjectedQueryBody,
) -> Result<ProjectedQueryBody, Status> {
    let org_scope = body
        .org_scope
        .ok_or_else(invalid_request)
        .and_then(|value| canonical_value_from_proto(value).map_err(|_| invalid_request()))?;
    let mut domain = ProjectedQueryBody::new(org_scope).with_select(body.select);
    let mut predicates = Vec::with_capacity(body.predicates.len());
    for predicate in body.predicates {
        predicates.push(projected_predicate_from_proto(predicate)?);
    }
    domain = domain.with_predicates(predicates);
    let mut order = Vec::with_capacity(body.order.len());
    for item in body.order {
        if item.field.is_empty() {
            return Err(invalid_request());
        }
        order.push(ProjectedOrderSpec {
            field: item.field,
            direction: if item.descending {
                ProjectedSortDirection::Desc
            } else {
                ProjectedSortDirection::Asc
            },
        });
    }
    domain = domain.with_order(order);
    if let Some(limit) = body.limit {
        domain = domain.with_limit(Some(limit as usize));
    }
    if !body.group_by.is_empty() {
        // Aggregate required when group_by is set: proto carries a single optional aggregate.
        let aggregate = body
            .aggregate
            .as_ref()
            .map(projected_aggregate_from_proto)
            .transpose()?
            .ok_or_else(invalid_request)?;
        domain = domain.with_group_by(Some(ProjectedGroupBySpec {
            keys: body.group_by,
            aggregates: vec![aggregate],
        }));
    } else if let Some(aggregate) = body.aggregate {
        domain = domain.with_aggregate(Some(projected_aggregate_from_proto(&aggregate)?));
    }
    Ok(domain)
}

fn projected_predicate_from_proto(
    predicate: app_v1::ProjectedPredicate,
) -> Result<ProjectedColumnPredicate, Status> {
    if predicate.field.is_empty() {
        return Err(invalid_request());
    }
    match predicate.kind {
        Some(app_v1::projected_predicate::Kind::Eq(value)) => {
            let value = canonical_value_from_proto(value).map_err(|_| invalid_request())?;
            Ok(ProjectedColumnPredicate::Eq {
                field: predicate.field,
                value,
            })
        }
        Some(app_v1::projected_predicate::Kind::Range(range)) => {
            let low = range
                .lower
                .map(canonical_value_from_proto)
                .transpose()
                .map_err(|_| invalid_request())?;
            let high = range
                .upper
                .map(canonical_value_from_proto)
                .transpose()
                .map_err(|_| invalid_request())?;
            // Engine Range is inclusive-low / exclusive-high. Wire inclusive flags
            // are accepted only when they match that closed engine shape.
            if !range.lower_inclusive && low.is_some() {
                return Err(invalid_request());
            }
            if range.upper_inclusive && high.is_some() {
                return Err(invalid_request());
            }
            Ok(ProjectedColumnPredicate::Range {
                field: predicate.field,
                low,
                high,
            })
        }
        None => Err(invalid_request()),
    }
}

fn projected_aggregate_from_proto(
    aggregate: &app_v1::ProjectedAggregate,
) -> Result<ProjectedAggregateOp, Status> {
    match app_v1::ProjectedAggregateOp::try_from(aggregate.op) {
        Ok(app_v1::ProjectedAggregateOp::Count) => Ok(ProjectedAggregateOp::Count),
        Ok(app_v1::ProjectedAggregateOp::Sum) => {
            if aggregate.field.is_empty() {
                return Err(invalid_request());
            }
            Ok(ProjectedAggregateOp::Sum {
                field: aggregate.field.clone(),
            })
        }
        Ok(app_v1::ProjectedAggregateOp::Min) => {
            if aggregate.field.is_empty() {
                return Err(invalid_request());
            }
            Ok(ProjectedAggregateOp::Min {
                field: aggregate.field.clone(),
            })
        }
        Ok(app_v1::ProjectedAggregateOp::Max) => {
            if aggregate.field.is_empty() {
                return Err(invalid_request());
            }
            Ok(ProjectedAggregateOp::Max {
                field: aggregate.field.clone(),
            })
        }
        Ok(app_v1::ProjectedAggregateOp::Unspecified) | Err(_) => Err(invalid_request()),
    }
}

fn freshness_policy_from_proto(
    freshness: app_v1::FreshnessPolicyProto,
) -> Result<FreshnessPolicy, Status> {
    match freshness.policy {
        Some(app_v1::freshness_policy_proto::Policy::Causal(causal)) => {
            let token =
                CommitToken::from_bytes(causal.commit_token).map_err(|_| invalid_request())?;
            Ok(FreshnessPolicy::Causal {
                token,
                max_wait: Duration::from_nanos(causal.max_wait_nanos),
            })
        }
        Some(app_v1::freshness_policy_proto::Policy::Bounded(bounded)) => {
            Ok(FreshnessPolicy::Bounded {
                max_lag_sequences: bounded.max_lag_sequences,
            })
        }
        Some(app_v1::freshness_policy_proto::Policy::Available(_)) => {
            Ok(FreshnessPolicy::Available)
        }
        None => Err(invalid_request()),
    }
}

fn rebuilding_reason_to_proto(
    reason: ProjectedRebuildingReason,
) -> app_v1::ProjectedRebuildingReason {
    match reason {
        ProjectedRebuildingReason::ReplayBudgetExceeded => {
            app_v1::ProjectedRebuildingReason::ReplayBudgetExceeded
        }
        ProjectedRebuildingReason::ExplicitRebuild => {
            app_v1::ProjectedRebuildingReason::ExplicitRebuild
        }
        ProjectedRebuildingReason::StateIntegrityFailure => {
            app_v1::ProjectedRebuildingReason::StateIntegrityFailure
        }
    }
}

fn degraded_reason_to_proto(reason: ProjectedDegradedReason) -> app_v1::ProjectedDegradedReason {
    match reason {
        ProjectedDegradedReason::ApplyLagSlo => app_v1::ProjectedDegradedReason::ApplyLagSlo,
        ProjectedDegradedReason::MaintenanceBacklog => {
            app_v1::ProjectedDegradedReason::MaintenanceBacklog
        }
        ProjectedDegradedReason::PartialInventory => {
            app_v1::ProjectedDegradedReason::PartialInventory
        }
    }
}

fn invalid_request() -> Status {
    Status::invalid_argument(INVALID_REQUEST_MESSAGE)
}

fn internal_defect() -> Status {
    Status::internal("an internal error occurred")
}

// Silence unused-import lint if CanonicalValue is only used in types.
const _: fn(&CanonicalValue) = |_| {};
const _: fn(&ProjectionFrontier) = |_| {};
const _: fn(&SymbolicContractSelector) = |_| {};
