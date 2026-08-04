//! Wire conversion for the projected columnar query surface (CP2b / CP4).
//!
//! Kept out of `conversion.rs` to avoid merge contention on that file.
//!
//! CP4 packed column-major Ready encoding lives entirely here: the service
//! result is unchanged; only the public wire shape diverges when the request
//! opts into [`app_v1::ProjectedResponseEncoding::Packed`].

use std::time::Duration;

use riffdb_proto::{app::v1 as app_v1, canonical_value_from_proto, canonical_value_to_proto};
use riffdb_service::{
    ExecuteProjectedQueryRequest, ExecuteProjectedQueryResult, ProjectedColumnPredicate,
    ProjectedDegradedReason, ProjectedOrderSpec, ProjectedQueryBody, ProjectedRebuildingReason,
    ProjectedSortDirection, SymbolicContractSelector,
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
/// Never emits `ready_packed`. Prefer
/// [`execute_projected_query_result_to_proto_for_request`] when the original
/// request encoding is available.
pub fn execute_projected_query_result_to_proto(
    result: ExecuteProjectedQueryResult,
) -> Result<app_v1::ExecuteProjectedQueryResponse, Status> {
    execute_projected_query_result_to_proto_with_encoding(result, false)
}

/// Encodes one projected-query outcome using the request's response encoding.
///
/// - PACKED + Ready → `ready_packed`
/// - ROW / absent / UNSPECIFIED + Ready → `ready`
/// - Non-Ready outcomes are identical regardless of requested encoding
pub fn execute_projected_query_result_to_proto_for_request(
    result: ExecuteProjectedQueryResult,
    request: &app_v1::ExecuteProjectedQueryRequest,
) -> Result<app_v1::ExecuteProjectedQueryResponse, Status> {
    execute_projected_query_result_to_proto_with_encoding(result, wants_packed_response(request))
}

/// Encodes Ready as packed column-major when `packed` is true; otherwise row.
pub fn execute_projected_query_result_to_proto_with_encoding(
    result: ExecuteProjectedQueryResult,
    packed: bool,
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
        } if packed => {
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
                frontier: frontier.as_bytes().to_vec(),
                head: head.as_bytes().to_vec(),
                commit_token: commit_token
                    .map(|token| token.into_bytes())
                    .unwrap_or_default(),
            })
        }
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

/// Builds one packed column from an iterator of cell values (row-major order).
///
/// One contiguous `data` buffer per column: encode each cell once via
/// [`encode_canonical_value_into`] after a [`canonical_value_encoded_len`]
/// reserve. Offsets are `row_count + 1` u32 start/end markers.
fn pack_column<'a, I>(cells: I) -> Result<app_v1::PackedColumn, Status>
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
    // Aggregate and group_by results have no response carriage yet: the Ready
    // arm carries rows only, so accepting these shapes would execute them and
    // silently drop the output. Reject typed until carriage lands; the
    // engine-level surface remains available in-process.
    if !body.group_by.is_empty() || body.aggregate.is_some() {
        return Err(invalid_request());
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
            aggregate: None,
        }
    }

    /// Aggregate and group_by have no response carriage yet; accepting them
    /// would execute the query and silently drop the output on the wire.
    #[test]
    fn aggregate_and_group_by_bodies_are_rejected_typed_until_carriage_exists() {
        let mut with_aggregate = minimal_body();
        with_aggregate.aggregate = Some(app_v1::ProjectedAggregate {
            op: app_v1::ProjectedAggregateOp::Count as i32,
            field: String::new(),
        });
        assert!(projected_query_body_from_proto(with_aggregate).is_err());

        let mut with_group_by = minimal_body();
        with_group_by.group_by = vec!["status".to_owned()];
        assert!(projected_query_body_from_proto(with_group_by).is_err());

        assert!(projected_query_body_from_proto(minimal_body()).is_ok());
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
            vec!["title"],
            vec!["ticket_id"],
            vec![QueryRow {
                cells: vec![CanonicalValue::string("t").expect("s")],
                primary_key: vec![CanonicalValue::Uuid([9; 16])],
            }],
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
            assert!(
                matches!(
                    response.outcome,
                    Some(app_v1::execute_projected_query_response::Outcome::Ready(_))
                ),
                "must not emit ready_packed for non-PACKED request: {response:?}"
            );
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
