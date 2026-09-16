//! Wire conversion for the projected columnar query surface (CP2b / CP4).
//!
//! Kept out of `conversion.rs` to avoid merge contention on that file.
//!
//! CP4 packed column-major Ready encoding lives entirely here: the service
//! result is unchanged; only the public wire shape diverges when the request
//! opts into [`app_v1::ProjectedResponseEncoding::Packed`].

use std::time::Duration;

use riffdb_proto::{
    aggregate_sum_to_proto, app::v1 as app_v1, canonical_value_from_proto, canonical_value_to_proto,
};
use riffdb_service::{
    ExecuteProjectedQueryRequest, ExecuteProjectedQueryResult, ProjectedAggregateOp,
    ProjectedAggregateValue, ProjectedColumnPredicate, ProjectedDegradedReason,
    ProjectedGroupBySpec, ProjectedOrderSpec, ProjectedQueryBody, ProjectedRebuildingReason,
    ProjectedSortDirection, QueryResult, SymbolicContractSelector,
};
use riffdb_types::{
    CanonicalValue, CommitToken, FreshnessPolicy, RequestId, canonical_value_encoded_len,
    encode_canonical_value_into,
};
use tonic::Status;

use crate::conversion::{
    INVALID_REQUEST_MESSAGE, request_id_from_bytes, symbolic_contract_selector_from_proto,
};

/// Parses one projected-query request from the public wire shape.
///
/// `response_encoding` is intentionally not lifted into the domain request:
/// packing is an adapter-only wire choice (service layer is frozen for CP4).
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

/// True when the request opted into packed column-major Ready encoding.
#[must_use]
pub fn wants_packed_response(request: &app_v1::ExecuteProjectedQueryRequest) -> bool {
    matches!(
        request
            .response_encoding
            .and_then(|value| app_v1::ProjectedResponseEncoding::try_from(value).ok()),
        Some(app_v1::ProjectedResponseEncoding::Packed)
    )
}

/// Encodes one projected-query outcome for the public wire (row-oriented Ready).
///
/// Never emits `ready_packed`, and carries no aggregate descriptors, so an
/// aggregate result is refused rather than mis-described. Prefer
/// [`execute_projected_query_result_to_proto_for_request`] when the original
/// request is available.
pub fn execute_projected_query_result_to_proto(
    result: ExecuteProjectedQueryResult,
) -> Result<app_v1::ExecuteProjectedQueryResponse, Status> {
    execute_projected_query_result_to_proto_with_encoding(result, false, &[])
}

/// Encodes one projected-query outcome using the request's response encoding.
///
/// - Aggregate/grouped result → `ready_aggregates`, whatever the encoding
/// - PACKED + row Ready → `ready_packed`
/// - ROW / absent / UNSPECIFIED + row Ready → `ready`
/// - Non-Ready outcomes are identical regardless of requested encoding
pub fn execute_projected_query_result_to_proto_for_request(
    result: ExecuteProjectedQueryResult,
    request: &app_v1::ExecuteProjectedQueryRequest,
) -> Result<app_v1::ExecuteProjectedQueryResponse, Status> {
    let aggregates = request
        .request
        .as_ref()
        .map_or(&[][..], |body| body.aggregates.as_slice());
    execute_projected_query_result_to_proto_with_encoding(
        result,
        wants_packed_response(request),
        aggregates,
    )
}

/// Encodes Ready as packed column-major when `packed` is true; otherwise row.
///
/// `request_aggregates` are the request's aggregate descriptors, echoed into
/// the aggregate arm so the response is self-describing. They must positionally
/// match the values the engine produced; a mismatch is an internal defect, not
/// a caller error, and fails closed rather than emitting a response whose
/// columns cannot be interpreted.
pub fn execute_projected_query_result_to_proto_with_encoding(
    result: ExecuteProjectedQueryResult,
    packed: bool,
    request_aggregates: &[app_v1::ProjectedAggregate],
) -> Result<app_v1::ExecuteProjectedQueryResponse, Status> {
    use app_v1::execute_projected_query_response::Outcome;
    let outcome = match result {
        ExecuteProjectedQueryResult::Ready {
            fields,
            field_ids: _,
            primary_key_fields,
            rows,
            result,
            frontier,
            head,
            commit_token,
        } => {
            let frontier = frontier.as_bytes().to_vec();
            let head = head.as_bytes().to_vec();
            let commit_token = commit_token
                .map(|token| token.into_bytes())
                .unwrap_or_default();
            // Exhaustive over every engine result shape: a new variant must
            // break this build rather than be silently dropped on the wire.
            match result {
                // Grouped fold. `fields` carries the group-key names for this
                // shape; the empty row metadata is dropped, never serialized.
                Some(QueryResult::Groups { key_fields, groups }) => {
                    if key_fields.len() != fields.len() {
                        return Err(internal_defect());
                    }
                    let mut wire_groups = Vec::with_capacity(groups.len());
                    for (keys, values) in groups {
                        if keys.len() != fields.len() {
                            return Err(internal_defect());
                        }
                        wire_groups.push(aggregate_group_to_proto(
                            &keys,
                            &values,
                            request_aggregates,
                        )?);
                    }
                    Outcome::ReadyAggregates(app_v1::ProjectedReadyAggregates {
                        group_key_fields: fields,
                        aggregates: request_aggregates.to_vec(),
                        // Emission order is the engine's encoded-group-key byte
                        // order. Never re-sorted here (ADR-0087 determinism).
                        groups: wire_groups,
                        frontier,
                        head,
                        commit_token,
                    })
                }
                // Whole-set fold: exactly one group with an empty key list.
                // There is no separate ungrouped response shape.
                Some(QueryResult::Aggregate(value)) => {
                    Outcome::ReadyAggregates(app_v1::ProjectedReadyAggregates {
                        group_key_fields: Vec::new(),
                        aggregates: request_aggregates.to_vec(),
                        groups: vec![aggregate_group_to_proto(
                            &[],
                            std::slice::from_ref(&value),
                            request_aggregates,
                        )?],
                        frontier,
                        head,
                        commit_token,
                    })
                }
                // The service flattens row results into `fields`/`rows` and
                // leaves `result` empty; a row result arriving here means the
                // service and the adapter disagree about the Ready shape.
                Some(QueryResult::Rows(_)) => return Err(internal_defect()),
                None if packed => {
                    // Column-major: PK columns first, then selected fields. One
                    // encode_canonical_value per cell into a contiguous column buffer.
                    let row_count = u32::try_from(rows.len()).map_err(|_| internal_defect())?;
                    let mut columns =
                        Vec::with_capacity(primary_key_fields.len().saturating_add(fields.len()));
                    for pk_index in 0..primary_key_fields.len() {
                        columns.push(pack_column(rows.iter().map(|row| {
                            row.primary_key.get(pk_index).ok_or_else(internal_defect)
                        }))?);
                    }
                    for cell_index in 0..fields.len() {
                        columns
                            .push(pack_column(rows.iter().map(|row| {
                                row.cells.get(cell_index).ok_or_else(internal_defect)
                            }))?);
                    }
                    Outcome::ReadyPacked(app_v1::ProjectedReadyPacked {
                        fields,
                        primary_key_fields,
                        row_count,
                        columns,
                        frontier,
                        head,
                        commit_token,
                    })
                }
                None => {
                    // Rows expose select cells then primary-key components under the
                    // combined field name list (select first, then PK names not already
                    // present in select). Wire clients read ResultRecord fields by name.
                    let emitted_primary_key_fields: Vec<_> = primary_key_fields
                        .iter()
                        .enumerate()
                        .filter(|(_, name)| !fields.contains(name))
                        .collect();
                    let mut wire_fields = fields.clone();
                    for &(_, pk_name) in &emitted_primary_key_fields {
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
                                    canonical_value_to_proto(value)
                                        .map_err(|_| invalid_request())?,
                                ),
                            });
                        }
                        for &(index, name) in &emitted_primary_key_fields {
                            let value = row.primary_key.get(index).ok_or_else(internal_defect)?;
                            record_fields.push(app_v1::Parameter {
                                name: name.clone(),
                                value: Some(
                                    canonical_value_to_proto(value)
                                        .map_err(|_| invalid_request())?,
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
                        frontier,
                        head,
                        commit_token,
                    })
                }
            }
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

/// Builds one packed column from an iterator of cell values (row-major order).
///
/// One contiguous `data` buffer per column: encode each cell once via
/// [`encode_canonical_value_into`] after a [`canonical_value_encoded_len`]
/// reserve. Offsets are `row_count + 1` u32 start/end markers.
pub(crate) fn pack_column<'a, I>(cells: I) -> Result<app_v1::PackedColumn, Status>
where
    I: Iterator<Item = Result<&'a CanonicalValue, Status>>,
{
    let mut data = Vec::new();
    let mut offsets = vec![0_u32];
    for cell in cells {
        let value = cell?;
        let needed = canonical_value_encoded_len(value).map_err(|_| internal_defect())?;
        data.reserve(needed);
        encode_canonical_value_into(&mut data, value).map_err(|_| internal_defect())?;
        let end = u32::try_from(data.len()).map_err(|_| {
            // Column data exceeds u32 offset space; response budgets should
            // prevent this, but fail closed rather than wrap.
            Status::resource_exhausted("projected packed column exceeds u32 offset space")
        })?;
        offsets.push(end);
    }
    Ok(app_v1::PackedColumn { data, offsets })
}

/// Encodes one group's key cells and aggregate cells.
///
/// `descriptors` must be positionally aligned with `values`; the alignment is
/// the only thing that makes the response interpretable, so a mismatch fails
/// closed as an internal defect.
fn aggregate_group_to_proto(
    keys: &[CanonicalValue],
    values: &[ProjectedAggregateValue],
    descriptors: &[app_v1::ProjectedAggregate],
) -> Result<app_v1::ProjectedAggregateGroup, Status> {
    if values.len() != descriptors.len() {
        return Err(internal_defect());
    }
    let mut wire_keys = Vec::with_capacity(keys.len());
    for key in keys {
        wire_keys.push(canonical_value_to_proto(key).map_err(|_| internal_defect())?);
    }
    let mut wire_values = Vec::with_capacity(values.len());
    for (value, descriptor) in values.iter().zip(descriptors) {
        wire_values.push(aggregate_value_to_proto(value, descriptor)?);
    }
    Ok(app_v1::ProjectedAggregateGroup {
        keys: wire_keys,
        values: wire_values,
    })
}

/// Encodes one aggregate cell, checking it against its request descriptor.
///
/// The arm is a function of the descriptor's op, so a Count value under a Sum
/// descriptor would silently mis-label a column. Cross-check rather than trust.
fn aggregate_value_to_proto(
    value: &ProjectedAggregateValue,
    descriptor: &app_v1::ProjectedAggregate,
) -> Result<app_v1::ProjectedAggregateValue, Status> {
    use app_v1::projected_aggregate_value::Value as WireValue;
    let op =
        app_v1::ProjectedAggregateOp::try_from(descriptor.op).map_err(|_| internal_defect())?;
    let wire = match (value, op) {
        (ProjectedAggregateValue::Count(count), app_v1::ProjectedAggregateOp::Count) => {
            WireValue::Count(*count)
        }
        (ProjectedAggregateValue::Sum(sum), app_v1::ProjectedAggregateOp::Sum) => {
            WireValue::Sum(aggregate_sum_to_proto(*sum))
        }
        (
            ProjectedAggregateValue::Scalar(scalar),
            app_v1::ProjectedAggregateOp::Min | app_v1::ProjectedAggregateOp::Max,
        ) => WireValue::Scalar(app_v1::ProjectedAggregateScalar {
            // Absent = no row contributed. A present null_value is a real NULL
            // extreme over an optional column and must stay distinguishable.
            value: scalar
                .as_ref()
                .map(canonical_value_to_proto)
                .transpose()
                .map_err(|_| internal_defect())?,
        }),
        _ => return Err(internal_defect()),
    };
    Ok(app_v1::ProjectedAggregateValue { value: Some(wire) })
}

/// Lowers one wire aggregate descriptor to its name-addressed domain form.
fn projected_aggregate_from_proto(
    aggregate: app_v1::ProjectedAggregate,
) -> Result<ProjectedAggregateOp, Status> {
    let op = app_v1::ProjectedAggregateOp::try_from(aggregate.op).map_err(|_| invalid_request())?;
    match op {
        // COUNT reads no column; a named field would be silently unread.
        app_v1::ProjectedAggregateOp::Count => {
            if !aggregate.field.is_empty() {
                return Err(invalid_request());
            }
            Ok(ProjectedAggregateOp::Count)
        }
        app_v1::ProjectedAggregateOp::Sum => Ok(ProjectedAggregateOp::Sum {
            field: aggregate_field(aggregate.field)?,
        }),
        app_v1::ProjectedAggregateOp::Min => Ok(ProjectedAggregateOp::Min {
            field: aggregate_field(aggregate.field)?,
        }),
        app_v1::ProjectedAggregateOp::Max => Ok(ProjectedAggregateOp::Max {
            field: aggregate_field(aggregate.field)?,
        }),
        app_v1::ProjectedAggregateOp::Unspecified => Err(invalid_request()),
    }
}

fn aggregate_field(field: String) -> Result<String, Status> {
    if field.is_empty() {
        return Err(invalid_request());
    }
    Ok(field)
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
    let mut aggregates = Vec::with_capacity(body.aggregates.len());
    for aggregate in body.aggregates {
        aggregates.push(projected_aggregate_from_proto(aggregate)?);
    }
    let mut keys = Vec::with_capacity(body.group_by.len());
    for key in body.group_by {
        if key.is_empty() {
            return Err(invalid_request());
        }
        keys.push(key);
    }
    if keys.is_empty() {
        // Global fold. The engine's whole-set path computes exactly one
        // function and is the only path that answers an empty matching set
        // with identity values; routing several functions through the grouping
        // path would report zero groups instead. Reject rather than diverge.
        match aggregates.len() {
            0 => {}
            1 => domain = domain.with_aggregate(aggregates.pop()),
            _ => return Err(invalid_request()),
        }
    } else {
        domain = domain.with_group_by(Some(ProjectedGroupBySpec { keys, aggregates }));
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

// Silence unused-import lint for type-only uses outside pack tests.
const _: fn(&SymbolicContractSelector) = |_| {};

#[cfg(test)]
mod aggregate_carriage_tests {
    use super::*;
    use riffdb_proto::aggregate_sum_from_proto;
    use riffdb_types::{CommitSequence, FrontierPosition, ProjectionFrontier};

    fn minimal_body() -> app_v1::ProjectedQueryBody {
        app_v1::ProjectedQueryBody {
            select: Vec::new(),
            org_scope: Some(riffdb_proto::v1::Value {
                kind: Some(riffdb_proto::v1::value::Kind::U64Value(7)),
            }),
            predicates: Vec::new(),
            order: Vec::new(),
            limit: Some(10),
            group_by: Vec::new(),
            aggregates: Vec::new(),
        }
    }

    fn descriptor(op: app_v1::ProjectedAggregateOp, field: &str) -> app_v1::ProjectedAggregate {
        app_v1::ProjectedAggregate {
            op: op as i32,
            field: field.to_owned(),
        }
    }

    fn frontier() -> ProjectionFrontier {
        ProjectionFrontier::new(
            1,
            FrontierPosition::AppliedThrough(CommitSequence::new(7).expect("nonzero")),
        )
    }

    fn aggregate_ready(
        group_key_fields: Vec<&str>,
        result: QueryResult,
    ) -> ExecuteProjectedQueryResult {
        ExecuteProjectedQueryResult::Ready {
            fields: group_key_fields
                .into_iter()
                .map(str::to_owned)
                .collect::<Vec<_>>(),
            field_ids: Vec::new(),
            primary_key_fields: Vec::new(),
            rows: Vec::new(),
            result: Some(result),
            frontier: frontier(),
            head: frontier(),
            commit_token: Some(CommitToken::new(
                1,
                CommitSequence::new(7).expect("nonzero"),
            )),
        }
    }

    fn aggregates_arm(
        response: app_v1::ExecuteProjectedQueryResponse,
    ) -> app_v1::ProjectedReadyAggregates {
        match response.outcome {
            Some(app_v1::execute_projected_query_response::Outcome::ReadyAggregates(arm)) => arm,
            other => panic!("expected ready_aggregates, got {other:?}"),
        }
    }

    fn request_with(
        aggregates: Vec<app_v1::ProjectedAggregate>,
        group_by: Vec<&str>,
        encoding: Option<app_v1::ProjectedResponseEncoding>,
    ) -> app_v1::ExecuteProjectedQueryRequest {
        let mut body = minimal_body();
        body.aggregates = aggregates;
        body.group_by = group_by.into_iter().map(str::to_owned).collect();
        app_v1::ExecuteProjectedQueryRequest {
            request: Some(body),
            response_encoding: encoding.map(|value| value as i32),
            ..Default::default()
        }
    }

    // ---------------------------------------------------------------
    // Request lowering
    // ---------------------------------------------------------------

    /// Every op lowers to its name-addressed domain form, and the field it
    /// names survives lowering (an aggregate whose column were dropped would
    /// authorize against nothing).
    #[test]
    fn every_aggregate_op_lowers_with_its_column() {
        let cases: Vec<(app_v1::ProjectedAggregateOp, &str, ProjectedAggregateOp)> = vec![
            (
                app_v1::ProjectedAggregateOp::Count,
                "",
                ProjectedAggregateOp::Count,
            ),
            (
                app_v1::ProjectedAggregateOp::Sum,
                "story_points",
                ProjectedAggregateOp::Sum {
                    field: "story_points".to_owned(),
                },
            ),
            (
                app_v1::ProjectedAggregateOp::Min,
                "title",
                ProjectedAggregateOp::Min {
                    field: "title".to_owned(),
                },
            ),
            (
                app_v1::ProjectedAggregateOp::Max,
                "title",
                ProjectedAggregateOp::Max {
                    field: "title".to_owned(),
                },
            ),
        ];
        for (wire_op, field, expected) in cases {
            let mut body = minimal_body();
            body.aggregates = vec![descriptor(wire_op, field)];
            let domain = projected_query_body_from_proto(body).expect("lowers");
            assert_eq!(domain.aggregate(), Some(&expected), "op {wire_op:?}");
            assert!(
                domain.group_by().is_none(),
                "no group keys means a whole-set aggregate"
            );
        }
    }

    /// Group keys lower in request order with every aggregate attached, and a
    /// grouped body accepts more than one function.
    #[test]
    fn group_by_lowers_keys_in_order_with_all_aggregates() {
        let mut body = minimal_body();
        body.group_by = vec!["status".to_owned(), "project_id".to_owned()];
        body.aggregates = vec![
            descriptor(app_v1::ProjectedAggregateOp::Count, ""),
            descriptor(app_v1::ProjectedAggregateOp::Sum, "story_points"),
        ];
        let domain = projected_query_body_from_proto(body).expect("lowers");
        let spec = domain.group_by().expect("grouped");
        assert_eq!(
            spec.keys,
            vec!["status".to_owned(), "project_id".to_owned()]
        );
        assert_eq!(
            spec.aggregates,
            vec![
                ProjectedAggregateOp::Count,
                ProjectedAggregateOp::Sum {
                    field: "story_points".to_owned()
                }
            ]
        );
        assert!(
            domain.aggregate().is_none(),
            "grouped bodies never set the whole-set aggregate"
        );
    }

    /// Group keys with no aggregates is the distinct-keys shape and stays legal.
    #[test]
    fn group_by_without_aggregates_lowers_to_key_only_groups() {
        let mut body = minimal_body();
        body.group_by = vec!["status".to_owned()];
        let domain = projected_query_body_from_proto(body).expect("lowers");
        let spec = domain.group_by().expect("grouped");
        assert_eq!(spec.keys, vec!["status".to_owned()]);
        assert!(spec.aggregates.is_empty());
    }

    /// Malformed descriptors fail closed instead of executing something else.
    #[test]
    fn malformed_aggregate_descriptors_are_rejected_typed() {
        // Unspecified op.
        let mut unspecified = minimal_body();
        unspecified.aggregates = vec![descriptor(app_v1::ProjectedAggregateOp::Unspecified, "")];
        assert!(projected_query_body_from_proto(unspecified).is_err());

        // Unknown enum number.
        let mut unknown = minimal_body();
        unknown.aggregates = vec![app_v1::ProjectedAggregate {
            op: 99,
            field: String::new(),
        }];
        assert!(projected_query_body_from_proto(unknown).is_err());

        // COUNT naming a column: the column would be silently unread.
        let mut counted_column = minimal_body();
        counted_column.aggregates = vec![descriptor(app_v1::ProjectedAggregateOp::Count, "title")];
        assert!(projected_query_body_from_proto(counted_column).is_err());

        // SUM/MIN/MAX without a column.
        for op in [
            app_v1::ProjectedAggregateOp::Sum,
            app_v1::ProjectedAggregateOp::Min,
            app_v1::ProjectedAggregateOp::Max,
        ] {
            let mut columnless = minimal_body();
            columnless.aggregates = vec![descriptor(op, "")];
            assert!(
                projected_query_body_from_proto(columnless).is_err(),
                "{op:?} without a column must be rejected"
            );
        }

        // Empty group key name.
        let mut empty_key = minimal_body();
        empty_key.group_by = vec![String::new()];
        assert!(projected_query_body_from_proto(empty_key).is_err());
    }

    /// Two or more functions without group keys have no faithful engine path:
    /// the grouping path answers an empty matching set with zero groups rather
    /// than identity values, so the shape is refused instead of diverging.
    #[test]
    fn multiple_global_aggregates_without_group_by_are_rejected_typed() {
        let mut body = minimal_body();
        body.aggregates = vec![
            descriptor(app_v1::ProjectedAggregateOp::Count, ""),
            descriptor(app_v1::ProjectedAggregateOp::Sum, "story_points"),
        ];
        assert!(projected_query_body_from_proto(body).is_err());

        // The same pair is accepted the moment a group key is present.
        let mut grouped = minimal_body();
        grouped.group_by = vec!["status".to_owned()];
        grouped.aggregates = vec![
            descriptor(app_v1::ProjectedAggregateOp::Count, ""),
            descriptor(app_v1::ProjectedAggregateOp::Sum, "story_points"),
        ];
        assert!(projected_query_body_from_proto(grouped).is_ok());
    }

    /// Plain row bodies keep lowering exactly as before.
    #[test]
    fn row_bodies_remain_unaffected() {
        let domain = projected_query_body_from_proto(minimal_body()).expect("lowers");
        assert!(domain.aggregate().is_none());
        assert!(domain.group_by().is_none());
        assert_eq!(domain.limit(), Some(10));
    }

    // ---------------------------------------------------------------
    // Response mapping
    // ---------------------------------------------------------------

    /// A whole-set aggregate answers with exactly one group whose key list is
    /// empty — R2's single response shape.
    #[test]
    fn whole_set_aggregate_maps_to_one_empty_key_group() {
        let request = request_with(
            vec![descriptor(app_v1::ProjectedAggregateOp::Count, "")],
            Vec::new(),
            None,
        );
        let response = execute_projected_query_result_to_proto_for_request(
            aggregate_ready(
                Vec::new(),
                QueryResult::Aggregate(ProjectedAggregateValue::Count(3)),
            ),
            &request,
        )
        .expect("encode");
        let arm = aggregates_arm(response);
        assert!(arm.group_key_fields.is_empty());
        assert_eq!(arm.aggregates.len(), 1);
        assert_eq!(arm.groups.len(), 1);
        assert!(arm.groups[0].keys.is_empty());
        assert_eq!(
            arm.groups[0].values[0].value,
            Some(app_v1::projected_aggregate_value::Value::Count(3))
        );
        assert_eq!(arm.frontier, frontier().as_bytes().to_vec());
        assert_eq!(arm.head, frontier().as_bytes().to_vec());
        assert!(!arm.commit_token.is_empty());
    }

    /// Sum rides as a scale-0 Decimal with no asserted precision, and survives
    /// both i128 extremes exactly.
    #[test]
    fn sum_carriage_is_scale_zero_decimal_and_survives_i128_extremes() {
        let request = request_with(
            vec![descriptor(
                app_v1::ProjectedAggregateOp::Sum,
                "story_points",
            )],
            Vec::new(),
            None,
        );
        for sum in [0_i128, -1, 1, i64::MAX as i128 + 1, i128::MIN, i128::MAX] {
            let response = execute_projected_query_result_to_proto_for_request(
                aggregate_ready(
                    Vec::new(),
                    QueryResult::Aggregate(ProjectedAggregateValue::Sum(sum)),
                ),
                &request,
            )
            .expect("encode");
            let arm = aggregates_arm(response);
            let Some(app_v1::projected_aggregate_value::Value::Sum(decimal)) =
                arm.groups[0].values[0].value.clone()
            else {
                panic!("sum must ride in the sum arm");
            };
            assert_eq!(decimal.scale, 0, "sum carriage is scale 0");
            assert_eq!(decimal.precision, None, "an i128 needs 39 digits; none set");
            assert!(
                (1..=16).contains(&decimal.coefficient_twos_complement.len()),
                "coefficient must be i128-wide at most"
            );
            assert_eq!(
                aggregate_sum_from_proto(&decimal).expect("decodes"),
                sum,
                "sum {sum} must round-trip exactly"
            );
        }
    }

    /// R3: an empty group's MIN/MAX is absent, and a real NULL extreme over an
    /// optional column is present-and-null. Collapsing them would report
    /// "no rows" and "the smallest value is NULL" identically.
    #[test]
    fn absent_scalar_and_null_scalar_are_distinct_on_the_wire() {
        let request = request_with(
            vec![descriptor(app_v1::ProjectedAggregateOp::Min, "title")],
            Vec::new(),
            None,
        );
        let encode = |scalar: Option<CanonicalValue>| {
            aggregates_arm(
                execute_projected_query_result_to_proto_for_request(
                    aggregate_ready(
                        Vec::new(),
                        QueryResult::Aggregate(ProjectedAggregateValue::Scalar(scalar)),
                    ),
                    &request,
                )
                .expect("encode"),
            )
            .groups[0]
                .values[0]
                .clone()
        };
        let empty_set = encode(None);
        let null_extreme = encode(Some(CanonicalValue::Null));
        assert_ne!(
            empty_set, null_extreme,
            "an empty group and a NULL extreme must not encode identically"
        );
        let Some(app_v1::projected_aggregate_value::Value::Scalar(absent)) = empty_set.value else {
            panic!("min rides in the scalar arm");
        };
        assert_eq!(absent.value, None, "empty group carries no value");
        let Some(app_v1::projected_aggregate_value::Value::Scalar(present)) = null_extreme.value
        else {
            panic!("min rides in the scalar arm");
        };
        assert_eq!(
            present.value,
            Some(riffdb_proto::v1::Value {
                kind: Some(riffdb_proto::v1::value::Kind::NullValue(
                    riffdb_proto::v1::NullValue::NullValue as i32
                )),
            }),
            "a NULL extreme carries the ordinary house null Value"
        );
    }

    /// R5: groups are emitted in the engine's order, byte for byte, with keys
    /// and values positionally aligned with their name/descriptor lists.
    #[test]
    fn grouped_results_preserve_engine_order_and_alignment() {
        let request = request_with(
            vec![
                descriptor(app_v1::ProjectedAggregateOp::Count, ""),
                descriptor(app_v1::ProjectedAggregateOp::Sum, "story_points"),
            ],
            vec!["status"],
            None,
        );
        // Deliberately NOT in collation order of the key strings: the engine
        // emits encoded-key byte order and nothing downstream may re-sort.
        let engine_order = ["open", "b", "Closed"];
        let groups = engine_order
            .iter()
            .enumerate()
            .map(|(index, key)| {
                (
                    vec![CanonicalValue::string(*key).expect("bounded")],
                    vec![
                        ProjectedAggregateValue::Count(index as u64),
                        ProjectedAggregateValue::Sum(index as i128 * 10),
                    ],
                )
            })
            .collect::<Vec<_>>();
        let response = execute_projected_query_result_to_proto_for_request(
            aggregate_ready(
                vec!["status"],
                QueryResult::Groups {
                    key_fields: vec![riffdb_types::FieldId::first()],
                    groups,
                },
            ),
            &request,
        )
        .expect("encode");
        let arm = aggregates_arm(response);
        assert_eq!(arm.group_key_fields, vec!["status".to_owned()]);
        assert_eq!(arm.aggregates.len(), 2);
        assert_eq!(arm.groups.len(), 3);
        for (index, expected_key) in engine_order.iter().enumerate() {
            assert_eq!(arm.groups[index].keys.len(), 1);
            assert_eq!(
                arm.groups[index].keys[0],
                canonical_value_to_proto(&CanonicalValue::string(*expected_key).expect("bounded"))
                    .expect("value"),
                "group {index} moved out of engine order"
            );
            assert_eq!(arm.groups[index].values.len(), 2);
            assert_eq!(
                arm.groups[index].values[0].value,
                Some(app_v1::projected_aggregate_value::Value::Count(
                    index as u64
                ))
            );
        }
    }

    /// R4: PACKED is inapplicable to an aggregate body — not an error, and not
    /// a reason to emit a row arm.
    #[test]
    fn packed_encoding_is_inapplicable_to_aggregate_bodies() {
        let count = vec![descriptor(app_v1::ProjectedAggregateOp::Count, "")];
        let packed = execute_projected_query_result_to_proto_for_request(
            aggregate_ready(
                Vec::new(),
                QueryResult::Aggregate(ProjectedAggregateValue::Count(2)),
            ),
            &request_with(
                count.clone(),
                Vec::new(),
                Some(app_v1::ProjectedResponseEncoding::Packed),
            ),
        )
        .expect("PACKED with an aggregate body is not an error");
        let row = execute_projected_query_result_to_proto_for_request(
            aggregate_ready(
                Vec::new(),
                QueryResult::Aggregate(ProjectedAggregateValue::Count(2)),
            ),
            &request_with(
                count,
                Vec::new(),
                Some(app_v1::ProjectedResponseEncoding::Row),
            ),
        )
        .expect("ROW with an aggregate body is not an error");
        assert_eq!(packed, row, "the encoding field does not change the answer");
        assert!(matches!(
            packed.outcome,
            Some(app_v1::execute_projected_query_response::Outcome::ReadyAggregates(_))
        ));
    }

    /// Descriptors that do not line up with the engine's values would mislabel
    /// every column; the adapter fails closed rather than emitting them.
    #[test]
    fn descriptor_mismatch_fails_closed() {
        // Wrong arity.
        let response = execute_projected_query_result_to_proto_for_request(
            aggregate_ready(
                Vec::new(),
                QueryResult::Aggregate(ProjectedAggregateValue::Count(1)),
            ),
            &request_with(Vec::new(), Vec::new(), None),
        );
        assert_eq!(response.unwrap_err().code(), tonic::Code::Internal);

        // Right arity, wrong op: a count value under a sum descriptor.
        let response = execute_projected_query_result_to_proto_for_request(
            aggregate_ready(
                Vec::new(),
                QueryResult::Aggregate(ProjectedAggregateValue::Count(1)),
            ),
            &request_with(
                vec![descriptor(app_v1::ProjectedAggregateOp::Sum, "n")],
                Vec::new(),
                None,
            ),
        );
        assert_eq!(response.unwrap_err().code(), tonic::Code::Internal);

        // Group key names that do not cover the emitted key cells.
        let response = execute_projected_query_result_to_proto_for_request(
            aggregate_ready(
                vec!["status"],
                QueryResult::Groups {
                    key_fields: vec![riffdb_types::FieldId::first()],
                    groups: vec![(Vec::new(), Vec::new())],
                },
            ),
            &request_with(Vec::new(), vec!["status"], None),
        );
        assert_eq!(response.unwrap_err().code(), tonic::Code::Internal);
    }

    /// The encoded aggregate arm must survive the real public wire: preflight
    /// (which now knows about field 8), structural validation, and decode. An
    /// arm that only round-trips through in-memory structs would be rejected
    /// by the transport it was built for.
    #[test]
    fn aggregate_arm_survives_public_message_preflight_and_decode() {
        use tonic_prost::prost::Message as _;

        let request = request_with(
            vec![
                descriptor(app_v1::ProjectedAggregateOp::Count, ""),
                descriptor(app_v1::ProjectedAggregateOp::Sum, "story_points"),
                descriptor(app_v1::ProjectedAggregateOp::Min, "title"),
            ],
            vec!["status"],
            None,
        );
        let response = execute_projected_query_result_to_proto_for_request(
            aggregate_ready(
                vec!["status"],
                QueryResult::Groups {
                    key_fields: vec![riffdb_types::FieldId::first()],
                    groups: vec![
                        (
                            vec![CanonicalValue::string("open").expect("bounded")],
                            vec![
                                ProjectedAggregateValue::Count(2),
                                ProjectedAggregateValue::Sum(i128::MIN),
                                ProjectedAggregateValue::Scalar(Some(
                                    CanonicalValue::string("a").expect("bounded"),
                                )),
                            ],
                        ),
                        (
                            vec![CanonicalValue::string("closed").expect("bounded")],
                            vec![
                                ProjectedAggregateValue::Count(0),
                                ProjectedAggregateValue::Sum(0),
                                // Empty group: absent extreme.
                                ProjectedAggregateValue::Scalar(None),
                            ],
                        ),
                    ],
                },
            ),
            &request,
        )
        .expect("encode");

        let bytes = response.encode_to_vec();
        let decoded =
            riffdb_proto::decode_public_message::<app_v1::ExecuteProjectedQueryResponse>(&bytes)
                .expect("the aggregate arm must pass preflight and structural validation");
        assert_eq!(decoded, response, "the wire round trip must be lossless");
        let arm = aggregates_arm(decoded);
        assert_eq!(arm.groups.len(), 2);
        assert_eq!(
            aggregate_sum_from_proto(match &arm.groups[0].values[1].value {
                Some(app_v1::projected_aggregate_value::Value::Sum(decimal)) => decimal,
                other => panic!("expected a sum, got {other:?}"),
            })
            .expect("decodes"),
            i128::MIN,
            "the widest sum survives the real wire"
        );
    }

    /// The two halves of the carriage, in one process: a request built by the
    /// real SDK builder, lowered by the real conversion, answered from a real
    /// engine result by the real encoder, and raised by the real SDK decoder.
    ///
    /// Nothing else executes server-encode into client-decode without a live
    /// daemon, and every request-anchored decode invariant — echoed key names,
    /// echoed descriptors, whole-set cardinality, group order — is checked
    /// against the request this loop actually sent.
    #[test]
    fn sdk_request_round_trips_through_the_adapter_into_the_sdk_decoder() {
        use riffdb_client_rust::{
            ApplicationContract, ApplicationUuid, ApplicationValue, ProjectedAggregate,
            ProjectedAggregateGroup, ProjectedAggregateValue as SdkAggregateValue, ProjectedQuery,
            ProjectedQueryOutcome, raise_projected_response_for_query,
        };

        let base = || {
            ProjectedQuery::new(
                ApplicationContract::Active,
                "board",
                ApplicationValue::Uuid(ApplicationUuid::from_bytes([9; 16])),
            )
            .expect("query")
        };

        // (1) Grouped with functions.
        let grouped = base()
            .group_by(vec!["status".to_owned()])
            .aggregate(ProjectedAggregate::Count)
            .aggregate(ProjectedAggregate::Sum {
                field: "story_points".to_owned(),
            });
        let wire = grouped.clone().into_wire_request().expect("wire");
        // The adapter accepts what the SDK built.
        let lowered = projected_query_body_from_proto(wire.request.clone().expect("body"))
            .expect("the adapter must accept the SDK's grouped request");
        let spec = lowered.group_by().expect("grouped");
        assert_eq!(spec.keys, vec!["status".to_owned()]);
        assert_eq!(spec.aggregates.len(), 2);

        let response = execute_projected_query_result_to_proto_for_request(
            aggregate_ready(
                vec!["status"],
                QueryResult::Groups {
                    key_fields: vec![riffdb_types::FieldId::first()],
                    groups: vec![
                        (
                            vec![CanonicalValue::string("open").expect("bounded")],
                            vec![
                                ProjectedAggregateValue::Count(4),
                                ProjectedAggregateValue::Sum(i128::MAX),
                            ],
                        ),
                        (
                            vec![CanonicalValue::string("closed").expect("bounded")],
                            vec![
                                ProjectedAggregateValue::Count(2),
                                ProjectedAggregateValue::Sum(-7),
                            ],
                        ),
                    ],
                },
            ),
            &wire,
        )
        .expect("encode");
        let outcome =
            raise_projected_response_for_query(response, &grouped).expect("the SDK must decode");
        let ProjectedQueryOutcome::ReadyAggregates {
            group_key_fields,
            aggregates,
            groups,
            ..
        } = outcome
        else {
            panic!("expected ReadyAggregates");
        };
        assert_eq!(group_key_fields, vec!["status".to_owned()]);
        assert_eq!(
            aggregates,
            vec![
                ProjectedAggregate::Count,
                ProjectedAggregate::Sum {
                    field: "story_points".to_owned()
                }
            ]
        );
        assert_eq!(
            groups,
            vec![
                ProjectedAggregateGroup {
                    keys: vec![CanonicalValue::string("open").expect("bounded")],
                    values: vec![
                        SdkAggregateValue::Count(4),
                        SdkAggregateValue::Sum(i128::MAX)
                    ],
                },
                ProjectedAggregateGroup {
                    keys: vec![CanonicalValue::string("closed").expect("bounded")],
                    values: vec![SdkAggregateValue::Count(2), SdkAggregateValue::Sum(-7)],
                },
            ],
            "every key and every value must survive the whole carriage"
        );

        // (2) Whole-set: exactly one empty-key group, end to end.
        let whole_set = base().aggregate(ProjectedAggregate::Min {
            field: "title".to_owned(),
        });
        let wire = whole_set.clone().into_wire_request().expect("wire");
        assert!(
            projected_query_body_from_proto(wire.request.clone().expect("body"))
                .expect("lowers")
                .aggregate()
                .is_some()
        );
        let response = execute_projected_query_result_to_proto_for_request(
            aggregate_ready(
                Vec::new(),
                QueryResult::Aggregate(ProjectedAggregateValue::Scalar(None)),
            ),
            &wire,
        )
        .expect("encode");
        let ProjectedQueryOutcome::ReadyAggregates { groups, .. } =
            raise_projected_response_for_query(response, &whole_set).expect("decode")
        else {
            panic!("expected ReadyAggregates");
        };
        assert_eq!(groups.len(), 1, "a whole-set fold has exactly one answer");
        assert!(groups[0].keys.is_empty());
        assert_eq!(groups[0].values, vec![SdkAggregateValue::Scalar(None)]);

        // (3) F2 — the distinct-values query: group keys, no functions. The
        // SDK built it, the adapter lowered it, and the SDK must accept the
        // key-only answer instead of refusing its own request's reply.
        let distinct = base().group_by(vec!["status".to_owned()]);
        let wire = distinct.clone().into_wire_request().expect("wire");
        let lowered = projected_query_body_from_proto(wire.request.clone().expect("body"))
            .expect("the adapter must accept a key-only grouped request");
        assert!(lowered.group_by().expect("grouped").aggregates.is_empty());
        let response = execute_projected_query_result_to_proto_for_request(
            aggregate_ready(
                vec!["status"],
                QueryResult::Groups {
                    key_fields: vec![riffdb_types::FieldId::first()],
                    groups: vec![
                        (
                            vec![CanonicalValue::string("open").expect("bounded")],
                            Vec::new(),
                        ),
                        (
                            vec![CanonicalValue::string("closed").expect("bounded")],
                            Vec::new(),
                        ),
                    ],
                },
            ),
            &wire,
        )
        .expect("encode");
        let ProjectedQueryOutcome::ReadyAggregates {
            aggregates, groups, ..
        } = raise_projected_response_for_query(response, &distinct)
            .expect("a key-only grouped answer must decode")
        else {
            panic!("expected ReadyAggregates");
        };
        assert!(aggregates.is_empty());
        assert_eq!(groups.len(), 2);
        assert!(groups.iter().all(|group| group.values.is_empty()));
    }

    /// A row-shaped engine result inside `result` means the service and the
    /// adapter disagree about the Ready shape; never emit a half-described row.
    #[test]
    fn row_result_inside_the_non_row_slot_fails_closed() {
        let response = execute_projected_query_result_to_proto_for_request(
            aggregate_ready(
                Vec::new(),
                QueryResult::Rows(riffdb_columnar::QueryRows {
                    fields: Vec::new(),
                    primary_key_fields: Vec::new(),
                    rows: Vec::new(),
                }),
            ),
            &request_with(Vec::new(), Vec::new(), None),
        );
        assert_eq!(response.unwrap_err().code(), tonic::Code::Internal);
    }
}

#[cfg(test)]
mod packed_encoding_tests {
    use super::*;
    use riffdb_columnar::QueryRow;
    use riffdb_types::{
        CommitSequence, CurrencyCode, Date, Decimal, DecimalSpec, EnumTypeId, EnumVariantId,
        FieldId, FrontierPosition, Money, ProjectionFrontier, Timestamp, decode_canonical_value,
        encode_canonical_value,
    };

    fn frontier() -> ProjectionFrontier {
        ProjectionFrontier::new(
            1,
            FrontierPosition::AppliedThrough(CommitSequence::new(7).expect("nonzero")),
        )
    }

    fn ready(
        fields: Vec<&str>,
        primary_key_fields: Vec<&str>,
        rows: Vec<QueryRow>,
    ) -> ExecuteProjectedQueryResult {
        ExecuteProjectedQueryResult::Ready {
            fields: fields.into_iter().map(str::to_owned).collect(),
            field_ids: Vec::new(),
            primary_key_fields: primary_key_fields.into_iter().map(str::to_owned).collect(),
            rows,
            result: None,
            frontier: frontier(),
            head: frontier(),
            commit_token: Some(CommitToken::new(
                1,
                CommitSequence::new(7).expect("nonzero"),
            )),
        }
    }

    fn lagging() -> ExecuteProjectedQueryResult {
        ExecuteProjectedQueryResult::Lagging {
            required: frontier(),
            current: ProjectionFrontier::new(1, FrontierPosition::BeforeFirst),
            head: frontier(),
            lag_sequences: Some(3),
            retry_after: Some(Duration::from_millis(10)),
        }
    }

    fn packed_request() -> app_v1::ExecuteProjectedQueryRequest {
        app_v1::ExecuteProjectedQueryRequest {
            response_encoding: Some(app_v1::ProjectedResponseEncoding::Packed as i32),
            ..Default::default()
        }
    }

    fn row_request() -> app_v1::ExecuteProjectedQueryRequest {
        app_v1::ExecuteProjectedQueryRequest {
            response_encoding: Some(app_v1::ProjectedResponseEncoding::Row as i32),
            ..Default::default()
        }
    }

    fn all_scalar_values() -> Vec<CanonicalValue> {
        let decimal = Decimal::new(DecimalSpec::new(5, 2).expect("spec"), -1234).expect("decimal");
        vec![
            CanonicalValue::Null,
            CanonicalValue::Bool(true),
            CanonicalValue::I64(-42),
            CanonicalValue::U64(99),
            CanonicalValue::Decimal(decimal),
            CanonicalValue::Money(Money::new(
                CurrencyCode::new("USD").expect("currency"),
                decimal,
            )),
            CanonicalValue::string("").expect("empty string"),
            CanonicalValue::string("hello").expect("string"),
            CanonicalValue::bytes(vec![]).expect("empty bytes"),
            CanonicalValue::bytes(vec![0, 255]).expect("bytes"),
            CanonicalValue::Timestamp(Timestamp::new(1, 2).expect("ts")),
            CanonicalValue::Date(Date::from_days_since_unix_epoch(100)),
            CanonicalValue::Uuid([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]),
            CanonicalValue::Enum {
                type_id: EnumTypeId::new(3).expect("type"),
                variant_id: EnumVariantId::new(1).expect("variant"),
            },
            CanonicalValue::list(vec![CanonicalValue::Bool(false), CanonicalValue::Null])
                .expect("list"),
            CanonicalValue::record(vec![
                (FieldId::new(1).expect("id"), CanonicalValue::U64(1)),
                (FieldId::new(2).expect("id"), CanonicalValue::Null),
            ])
            .expect("record"),
        ]
    }

    /// Roundtrip every supported scalar (and nested) type through pack offsets.
    #[test]
    fn pack_column_roundtrips_every_supported_canonical_type() {
        let values = all_scalar_values();
        let column = pack_column(values.iter().map(Ok)).expect("pack");
        assert_eq!(column.offsets.len(), values.len() + 1);
        assert_eq!(column.offsets[0], 0);
        assert_eq!(column.offsets[values.len()] as usize, column.data.len());
        for (index, expected) in values.iter().enumerate() {
            let start = column.offsets[index] as usize;
            let end = column.offsets[index + 1] as usize;
            let decoded = decode_canonical_value(&column.data[start..end]).expect("decode");
            assert_eq!(&decoded, expected, "cell {index}");
            // Self-encode matches the packed slice (no framing beyond canonical).
            assert_eq!(
                encode_canonical_value(expected).expect("encode"),
                column.data[start..end]
            );
        }
    }

    #[test]
    fn pack_offsets_empty_result_and_single_row_and_empty_strings() {
        // Empty result: one offset marker (0), empty data.
        let empty = pack_column(std::iter::empty()).expect("empty");
        assert_eq!(empty.offsets, vec![0]);
        assert!(empty.data.is_empty());

        // Single row with empty string.
        let empty_string = CanonicalValue::string("").expect("bounded");
        let single = pack_column(std::iter::once(Ok(&empty_string))).expect("single");
        assert_eq!(single.offsets.len(), 2);
        assert_eq!(single.offsets[0], 0);
        assert_eq!(
            decode_canonical_value(&single.data).expect("decode"),
            empty_string
        );

        // Two rows: empty string then non-empty.
        let hello = CanonicalValue::string("hi").expect("bounded");
        let two = pack_column([&empty_string, &hello].into_iter().map(Ok)).expect("two");
        assert_eq!(two.offsets.len(), 3);
        let mid = two.offsets[1] as usize;
        assert_eq!(
            decode_canonical_value(&two.data[..mid]).expect("c0"),
            empty_string
        );
        assert_eq!(decode_canonical_value(&two.data[mid..]).expect("c1"), hello);
    }

    #[test]
    fn packed_request_ready_returns_ready_packed_arm() {
        let result = ready(
            vec!["title"],
            vec!["ticket_id"],
            vec![QueryRow {
                cells: vec![CanonicalValue::string("t").expect("s")],
                primary_key: vec![CanonicalValue::Uuid([9; 16])],
            }],
        );
        let response =
            execute_projected_query_result_to_proto_for_request(result, &packed_request())
                .expect("encode");
        match response.outcome {
            Some(app_v1::execute_projected_query_response::Outcome::ReadyPacked(packed)) => {
                assert_eq!(packed.row_count, 1);
                assert_eq!(packed.fields, vec!["title".to_owned()]);
                assert_eq!(packed.primary_key_fields, vec!["ticket_id".to_owned()]);
                // PK column first, then select.
                assert_eq!(packed.columns.len(), 2);
                assert_eq!(packed.columns[0].offsets.len(), 2);
                assert_eq!(packed.columns[1].offsets.len(), 2);
            }
            other => panic!("expected ReadyPacked, got {other:?}"),
        }
    }

    #[test]
    fn row_or_absent_request_never_returns_ready_packed() {
        let result = ready(
            vec!["title", "tenant"],
            vec!["ticket_id", "tenant", "region"],
            (1..=2)
                .map(|value| QueryRow {
                    cells: vec![
                        CanonicalValue::string("t").expect("s"),
                        CanonicalValue::U64(7),
                    ],
                    // A different shadowed value proves the select cell wins;
                    // the unshadowed suffix retains its original PK position.
                    primary_key: vec![
                        CanonicalValue::U64(value),
                        CanonicalValue::U64(99),
                        CanonicalValue::U64(8),
                    ],
                })
                .collect(),
        );
        for request in [
            row_request(),
            app_v1::ExecuteProjectedQueryRequest::default(),
            app_v1::ExecuteProjectedQueryRequest {
                response_encoding: Some(app_v1::ProjectedResponseEncoding::Unspecified as i32),
                ..Default::default()
            },
        ] {
            let response =
                execute_projected_query_result_to_proto_for_request(result.clone(), &request)
                    .expect("encode");
            let Some(app_v1::execute_projected_query_response::Outcome::Ready(ready)) =
                response.outcome
            else {
                panic!("non-PACKED requests must emit row Ready");
            };
            assert_eq!(ready.fields, ["title", "tenant", "ticket_id", "region"]);
            assert_eq!(ready.rows.len(), 2);
            for (position, row) in ready.rows.iter().enumerate() {
                assert_eq!(
                    row.fields
                        .iter()
                        .map(|field| field.name.as_str())
                        .collect::<Vec<_>>(),
                    ready.fields
                );
                let values = row
                    .fields
                    .iter()
                    .map(|field| {
                        canonical_value_from_proto(field.value.clone().expect("wire value"))
                            .expect("canonical value")
                    })
                    .collect::<Vec<_>>();
                assert_eq!(
                    values,
                    [
                        CanonicalValue::string("t").expect("s"),
                        CanonicalValue::U64(7),
                        CanonicalValue::U64(position as u64 + 1),
                        CanonicalValue::U64(8)
                    ]
                );
            }
        }
    }

    #[test]
    fn non_ready_outcomes_identical_regardless_of_encoding() {
        let lag = lagging();
        let packed =
            execute_projected_query_result_to_proto_for_request(lag.clone(), &packed_request())
                .expect("packed");
        let row =
            execute_projected_query_result_to_proto_for_request(lag, &row_request()).expect("row");
        assert_eq!(packed, row);
        assert!(matches!(
            packed.outcome,
            Some(app_v1::execute_projected_query_response::Outcome::Lagging(
                _
            ))
        ));
    }

    /// Falsifiability (b): returning Ready (row arm) for a PACKED request must fail.
    #[test]
    fn arm_selection_packed_request_must_not_emit_ready_row() {
        let result = ready(vec!["title"], vec!["id"], Vec::new());
        let response =
            execute_projected_query_result_to_proto_for_request(result, &packed_request())
                .expect("encode");
        assert!(
            !matches!(
                response.outcome,
                Some(app_v1::execute_projected_query_response::Outcome::Ready(_))
            ),
            "PACKED Ready must not use the row arm"
        );
        assert!(matches!(
            response.outcome,
            Some(app_v1::execute_projected_query_response::Outcome::ReadyPacked(_))
        ));
    }

    #[test]
    fn wants_packed_response_only_for_packed_enum() {
        assert!(wants_packed_response(&packed_request()));
        assert!(!wants_packed_response(&row_request()));
        assert!(!wants_packed_response(
            &app_v1::ExecuteProjectedQueryRequest::default()
        ));
    }

    /// Multi-row multi-column pack: column order is PK then select; cell order is rows.
    #[test]
    fn multi_row_column_major_order() {
        let rows = vec![
            QueryRow {
                cells: vec![
                    CanonicalValue::string("a").expect("s"),
                    CanonicalValue::U64(1),
                ],
                primary_key: vec![CanonicalValue::Uuid([1; 16])],
            },
            QueryRow {
                cells: vec![
                    CanonicalValue::string("b").expect("s"),
                    CanonicalValue::U64(2),
                ],
                primary_key: vec![CanonicalValue::Uuid([2; 16])],
            },
        ];
        let response = execute_projected_query_result_to_proto_with_encoding(
            ready(vec!["title", "n"], vec!["id"], rows),
            true,
            &[],
        )
        .expect("encode");
        let Some(app_v1::execute_projected_query_response::Outcome::ReadyPacked(packed)) =
            response.outcome
        else {
            panic!("expected packed");
        };
        assert_eq!(packed.row_count, 2);
        assert_eq!(packed.columns.len(), 3); // id, title, n
        // PK column: uuid 1 then uuid 2
        let pk = &packed.columns[0];
        let s0 = pk.offsets[0] as usize;
        let s1 = pk.offsets[1] as usize;
        let s2 = pk.offsets[2] as usize;
        assert_eq!(
            decode_canonical_value(&pk.data[s0..s1]).expect("pk0"),
            CanonicalValue::Uuid([1; 16])
        );
        assert_eq!(
            decode_canonical_value(&pk.data[s1..s2]).expect("pk1"),
            CanonicalValue::Uuid([2; 16])
        );
        // Title column
        let titles = &packed.columns[1];
        assert_eq!(
            decode_canonical_value(
                &titles.data[titles.offsets[0] as usize..titles.offsets[1] as usize]
            )
            .expect("t0"),
            CanonicalValue::string("a").expect("s")
        );
        assert_eq!(
            decode_canonical_value(
                &titles.data[titles.offsets[1] as usize..titles.offsets[2] as usize]
            )
            .expect("t1"),
            CanonicalValue::string("b").expect("s")
        );
    }
}
