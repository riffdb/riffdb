//! Projected board path: ExecuteProjectedQuery over the generated tonic client.
//!
//! Measures the honest full wire path (not a Rust SDK facade). Request shapes
//! mirror `tests/service/projected_read_acceptance.rs` / CP2b.

use std::time::Duration;

use riffdb_api_grpc::generated_app::application_query_service_client::ApplicationQueryServiceClient;
use riffdb_app_baseline_core::{TicketRow, TicketStatus, UuidBytes};
use riffdb_client_rust::generate_request_id;
use riffdb_proto::app::v1 as app_v1;
use riffdb_proto::v1;
use riffdb_types::{CommitSequence, CommitToken};
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

use crate::RiffDbError;

/// Configured projection name registered on the harness `riffdbd`.
pub const BOARD_PROJECTION_NAME: &str = "board";

/// Board select fields (ticket_id arrives via primary-key return on the wire).
pub const BOARD_SELECT: [&str; 5] = [
    "project_id",
    "title",
    "status",
    "reporter_id",
    "assignee_id",
];

/// Comparison field order matching the compiled board page.
pub const BOARD_ROW_FIELDS: [&str; 6] = [
    "ticket_id",
    "project_id",
    "title",
    "status",
    "reporter_id",
    "assignee_id",
];

/// Catch-up wait budget for Causal readiness after seed.
pub(crate) const CATCHUP_MAX_WAIT: Duration = Duration::from_secs(120);

/// Enum identity for `TicketStatus` resolved from the deployed contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TicketStatusEnumIds {
    /// Schema enum type id.
    pub type_id: u32,
    /// `Open` variant id.
    pub open: u32,
    /// `Closed` variant id.
    pub closed: u32,
    /// `InProgress` variant id.
    pub in_progress: u32,
}

impl TicketStatusEnumIds {
    /// Maps a harness status to the wire enum identity.
    #[must_use]
    pub const fn variant_id(self, status: TicketStatus) -> u32 {
        match status {
            TicketStatus::Open => self.open,
            TicketStatus::Closed => self.closed,
            TicketStatus::InProgress => self.in_progress,
        }
    }

    /// Inverse of [`Self::variant_id`].
    pub fn parse(self, type_id: u32, variant_id: u32) -> Result<TicketStatus, RiffDbError> {
        if type_id != self.type_id {
            return Err(RiffDbError::Decode);
        }
        if variant_id == self.open {
            Ok(TicketStatus::Open)
        } else if variant_id == self.closed {
            Ok(TicketStatus::Closed)
        } else if variant_id == self.in_progress {
            Ok(TicketStatus::InProgress)
        } else {
            Err(RiffDbError::Decode)
        }
    }
}

/// Builds the projected board request (select, eq predicates, order, limit, org).
#[must_use]
pub fn build_board_projected_request(
    organization_id: UuidBytes,
    project_id: UuidBytes,
    status: TicketStatus,
    limit: u32,
    status_ids: TicketStatusEnumIds,
    freshness: app_v1::FreshnessPolicyProto,
) -> app_v1::ExecuteProjectedQueryRequest {
    app_v1::ExecuteProjectedQueryRequest {
        contract: Some(app_v1::ContractSelector {
            lineage: "TicketDesk".to_owned(),
            version: 1,
            bundle_hash: Vec::new(),
        }),
        projection_name: BOARD_PROJECTION_NAME.to_owned(),
        request: Some(app_v1::ProjectedQueryBody {
            select: BOARD_SELECT.iter().map(|name| (*name).to_owned()).collect(),
            org_scope: Some(uuid_value(organization_id)),
            predicates: vec![
                app_v1::ProjectedPredicate {
                    field: "project_id".to_owned(),
                    kind: Some(app_v1::projected_predicate::Kind::Eq(uuid_value(
                        project_id,
                    ))),
                },
                app_v1::ProjectedPredicate {
                    field: "status".to_owned(),
                    kind: Some(app_v1::projected_predicate::Kind::Eq(enum_value(
                        status_ids.type_id,
                        status_ids.variant_id(status),
                    ))),
                },
            ],
            order: vec![app_v1::ProjectedOrder {
                field: "ticket_id".to_owned(),
                descending: false,
            }],
            limit: Some(limit),
            group_by: Vec::new(),
            aggregate: None,
        }),
        freshness: Some(freshness),
        request_id: generate_request_id()
            .map(|id| id.into_bytes().to_vec())
            .unwrap_or_else(|_| vec![0; 16]),
    }
}

/// Available freshness (measured path after catch-up).
#[must_use]
pub fn freshness_available() -> app_v1::FreshnessPolicyProto {
    app_v1::FreshnessPolicyProto {
        policy: Some(app_v1::freshness_policy_proto::Policy::Available(
            app_v1::FreshnessAvailable {},
        )),
    }
}

/// Causal freshness carrying an opaque commit token.
#[must_use]
pub fn freshness_causal(token_bytes: Vec<u8>, max_wait: Duration) -> app_v1::FreshnessPolicyProto {
    app_v1::FreshnessPolicyProto {
        policy: Some(app_v1::freshness_policy_proto::Policy::Causal(
            app_v1::FreshnessCausal {
                commit_token: token_bytes,
                max_wait_nanos: u64::try_from(max_wait.as_nanos()).unwrap_or(u64::MAX),
            },
        )),
    }
}

/// Builds opaque commit-token bytes for Causal catch-up.
pub fn commit_token_bytes(
    history_incarnation: u64,
    commit_sequence: u64,
) -> Result<Vec<u8>, RiffDbError> {
    let sequence = CommitSequence::new(commit_sequence).ok_or(RiffDbError::Decode)?;
    Ok(CommitToken::new(history_incarnation, sequence).into_bytes())
}

/// Executes one projected board query over the generated ApplicationQuery client.
#[allow(clippy::too_many_arguments)]
pub async fn execute_projected_board(
    channel: &Channel,
    bearer_token: &str,
    organization_id: UuidBytes,
    project_id: UuidBytes,
    status: TicketStatus,
    limit: u32,
    status_ids: TicketStatusEnumIds,
    freshness: app_v1::FreshnessPolicyProto,
) -> Result<Vec<TicketRow>, RiffDbError> {
    let mut client = ApplicationQueryServiceClient::new(channel.clone());
    let message = build_board_projected_request(
        organization_id,
        project_id,
        status,
        limit,
        status_ids,
        freshness,
    );
    let request = authenticated_request(message, bearer_token)?;
    let response = client
        .execute_projected_query(request)
        .await
        .map_err(|status| RiffDbError::Rpc(format!("ExecuteProjectedQuery: {status}")))?
        .into_inner();
    decode_projected_board_rows(response, organization_id, status_ids)
}

/// Catch-up gate: Causal to the seed head must serve Ready.
pub async fn catchup_projected_board(
    channel: &Channel,
    bearer_token: &str,
    organization_id: UuidBytes,
    project_id: UuidBytes,
    status: TicketStatus,
    status_ids: TicketStatusEnumIds,
    commit_token: Vec<u8>,
) -> Result<(), RiffDbError> {
    let mut client = ApplicationQueryServiceClient::new(channel.clone());
    let message = build_board_projected_request(
        organization_id,
        project_id,
        status,
        1,
        status_ids,
        freshness_causal(commit_token, CATCHUP_MAX_WAIT),
    );
    let request = authenticated_request(message, bearer_token)?;
    let response = client
        .execute_projected_query(request)
        .await
        .map_err(|status| RiffDbError::Rpc(format!("projected catch-up: {status}")))?
        .into_inner();
    match response.outcome {
        Some(app_v1::execute_projected_query_response::Outcome::Ready(_)) => Ok(()),
        Some(other) => Err(RiffDbError::Rpc(format!(
            "projected catch-up did not reach Ready: {other:?}"
        ))),
        None => Err(RiffDbError::Rpc(
            "projected catch-up response missing outcome".into(),
        )),
    }
}

fn authenticated_request<T>(
    message: T,
    bearer_token: &str,
) -> Result<tonic::Request<T>, RiffDbError> {
    let mut request = tonic::Request::new(message);
    let value = MetadataValue::try_from(format!("Bearer {bearer_token}"))
        .map_err(|_| RiffDbError::Connection)?;
    request.metadata_mut().insert("authorization", value);
    Ok(request)
}

/// Decodes Ready rows into [`TicketRow`] values (named wire fields).
pub(crate) fn decode_projected_board_rows(
    response: app_v1::ExecuteProjectedQueryResponse,
    organization_id: UuidBytes,
    status_ids: TicketStatusEnumIds,
) -> Result<Vec<TicketRow>, RiffDbError> {
    let Some(app_v1::execute_projected_query_response::Outcome::Ready(ready)) = response.outcome
    else {
        return Err(RiffDbError::Rpc(format!(
            "projected board expected Ready, got {:?}",
            response.outcome
        )));
    };
    // Served select must begin with the requested select (PK names may follow).
    for (index, name) in BOARD_SELECT.iter().enumerate() {
        if ready.fields.get(index).map(String::as_str) != Some(*name) {
            return Err(RiffDbError::Rpc(format!(
                "projected board select drift at {index}: expected {name}, fields={:?}",
                ready.fields
            )));
        }
    }
    ready
        .rows
        .into_iter()
        .map(|row| decode_one_board_row(row, organization_id, status_ids))
        .collect()
}

fn decode_one_board_row(
    row: app_v1::ResultRecord,
    organization_id: UuidBytes,
    status_ids: TicketStatusEnumIds,
) -> Result<TicketRow, RiffDbError> {
    let mut ticket_id = None;
    let mut project_id = None;
    let mut title = None;
    let mut status = None;
    let mut reporter_id = None;
    let mut assignee_id = None;
    for field in row.fields {
        let value = field.value.ok_or(RiffDbError::Decode)?;
        match field.name.as_str() {
            "ticket_id" => ticket_id = Some(parse_uuid_value(&value)?),
            "project_id" => project_id = Some(parse_uuid_value(&value)?),
            "title" => title = Some(parse_string_value(&value)?),
            "status" => status = Some(parse_status_value(&value, status_ids)?),
            "reporter_id" => reporter_id = Some(parse_uuid_value(&value)?),
            "assignee_id" => assignee_id = Some(parse_uuid_value(&value)?),
            "organization_id" => {
                // PK return; harness fills org from the bind parameter.
                let _ = parse_uuid_value(&value)?;
            }
            _ => {}
        }
    }
    Ok(TicketRow {
        organization_id,
        ticket_id: ticket_id.ok_or(RiffDbError::Decode)?,
        project_id: project_id.ok_or(RiffDbError::Decode)?,
        reporter_id: reporter_id.ok_or(RiffDbError::Decode)?,
        assignee_id: assignee_id.ok_or(RiffDbError::Decode)?,
        status: status.ok_or(RiffDbError::Decode)?,
        title: title.ok_or(RiffDbError::Decode)?,
    })
}

/// Stable hex digest of board row content (field order + values).
///
/// Uses FNV-1a over the comparison field order so divergence messages stay
/// compact without an extra crypto dependency.
#[must_use]
pub fn board_rows_digest(rows: &[TicketRow]) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    let mut absorb = |bytes: &[u8]| {
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
    };
    for row in rows {
        absorb(&row.ticket_id);
        absorb(&row.project_id);
        absorb(row.title.as_bytes());
        absorb(&[status_tag(row.status)]);
        absorb(&row.reporter_id);
        absorb(&row.assignee_id);
        absorb(&[0xff]);
    }
    format!("{hash:016x}")
}

/// Run-aborting cross-path equivalence: byte-identical row content.
/// NOTE on discriminating power: predicate-skew that happens to select the
/// same seeded set (e.g. dropping the status predicate when every board-cell
/// ticket is Open) is enforced by the request-shape unit tests, NOT by this
/// live gate; the live gate's proven live-abort case is content divergence
/// (demonstrated via enum variant-id corruption: compiled=50 projected=0).
pub fn assert_board_rows_equivalent(
    compiled: &[TicketRow],
    projected: &[TicketRow],
    limit: u32,
) -> Result<(), String> {
    // Count anchor: empty-vs-empty must never pass. The board cell is seeded
    // dense, so the compiled side must serve exactly the requested limit even
    // when PostgreSQL's cross-check is skipped.
    if compiled.len() != limit as usize {
        return Err(format!(
            "measurement-integrity: board_page_projected({limit}) compiled side served              {} rows, expected exactly {limit} — dataset or query drift",
            compiled.len()
        ));
    }
    if compiled.len() != projected.len() {
        return Err(format!(
            "measurement-integrity: board_page_projected({limit}) row count \
             compiled={} projected={} digests compiled={} projected={}",
            compiled.len(),
            projected.len(),
            board_rows_digest(compiled),
            board_rows_digest(projected)
        ));
    }
    for (index, (left, right)) in compiled.iter().zip(projected.iter()).enumerate() {
        if left != right {
            return Err(format!(
                "measurement-integrity: CROSS-PATH DIVERGENCE board_page_projected({limit}) \
                 at index {index}: digests compiled={} projected={}",
                board_rows_digest(compiled),
                board_rows_digest(projected)
            ));
        }
    }
    Ok(())
}

/// Public request-construction helpers for unit tests.
pub mod request_shape {
    use super::{
        BOARD_PROJECTION_NAME, BOARD_SELECT, TicketStatusEnumIds, build_board_projected_request,
        freshness_available,
    };
    use riffdb_app_baseline_core::TicketStatus;

    /// Asserts field names, predicate mapping, order, and limit for one board request.
    #[must_use]
    pub fn inspect_board_request(
        organization_id: [u8; 16],
        project_id: [u8; 16],
        status: TicketStatus,
        limit: u32,
        status_ids: TicketStatusEnumIds,
    ) -> BoardRequestShape {
        let request = build_board_projected_request(
            organization_id,
            project_id,
            status,
            limit,
            status_ids,
            freshness_available(),
        );
        let body = request.request.expect("body");
        BoardRequestShape {
            projection_name: request.projection_name,
            select: body.select,
            predicate_fields: body
                .predicates
                .iter()
                .map(|predicate| predicate.field.clone())
                .collect(),
            order_fields: body
                .order
                .iter()
                .map(|order| (order.field.clone(), order.descending))
                .collect(),
            limit: body.limit,
            has_status_eq: body.predicates.iter().any(|predicate| {
                predicate.field == "status"
                    && matches!(
                        predicate.kind,
                        Some(super::app_v1::projected_predicate::Kind::Eq(_))
                    )
            }),
            has_project_eq: body.predicates.iter().any(|predicate| {
                predicate.field == "project_id"
                    && matches!(
                        predicate.kind,
                        Some(super::app_v1::projected_predicate::Kind::Eq(_))
                    )
            }),
        }
    }

    /// Inspectable request shape (unit tests / falsifiability).
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub struct BoardRequestShape {
        /// Projection name.
        pub projection_name: String,
        /// Select list.
        pub select: Vec<String>,
        /// Predicate field names in request order.
        pub predicate_fields: Vec<String>,
        /// Order clauses `(field, descending)`.
        pub order_fields: Vec<(String, bool)>,
        /// Limit.
        pub limit: Option<u32>,
        /// Status eq present.
        pub has_status_eq: bool,
        /// Project eq present.
        pub has_project_eq: bool,
    }

    impl BoardRequestShape {
        /// Expected production board shape for limit N.
        #[must_use]
        pub fn expected(limit: u32) -> Self {
            Self {
                projection_name: BOARD_PROJECTION_NAME.to_owned(),
                select: BOARD_SELECT.iter().map(|name| (*name).to_owned()).collect(),
                predicate_fields: vec!["project_id".to_owned(), "status".to_owned()],
                order_fields: vec![("ticket_id".to_owned(), false)],
                limit: Some(limit),
                has_status_eq: true,
                has_project_eq: true,
            }
        }
    }
}

fn uuid_value(bytes: UuidBytes) -> v1::Value {
    v1::Value {
        kind: Some(v1::value::Kind::UuidValue(bytes.to_vec())),
    }
}

fn enum_value(type_id: u32, variant_id: u32) -> v1::Value {
    v1::Value {
        kind: Some(v1::value::Kind::EnumValue(v1::EnumValue {
            type_id,
            variant_id,
            name: String::new(),
        })),
    }
}

fn parse_uuid_value(value: &v1::Value) -> Result<UuidBytes, RiffDbError> {
    match &value.kind {
        Some(v1::value::Kind::UuidValue(bytes)) if bytes.len() == 16 => {
            let mut out = [0_u8; 16];
            out.copy_from_slice(bytes);
            Ok(out)
        }
        Some(v1::value::Kind::StringValue(text)) => parse_uuid_text(text),
        _ => Err(RiffDbError::Decode),
    }
}

fn parse_uuid_text(text: &str) -> Result<UuidBytes, RiffDbError> {
    if text.len() != 36 {
        return Err(RiffDbError::Decode);
    }
    let mut out = [0_u8; 16];
    let hex = |i: usize| u8::from_str_radix(&text[i..i + 2], 16).map_err(|_| RiffDbError::Decode);
    let positions = [0, 2, 4, 6, 9, 11, 14, 16, 19, 21, 24, 26, 28, 30, 32, 34];
    for (index, start) in positions.into_iter().enumerate() {
        out[index] = hex(start)?;
    }
    Ok(out)
}

fn parse_string_value(value: &v1::Value) -> Result<String, RiffDbError> {
    match &value.kind {
        Some(v1::value::Kind::StringValue(text)) => Ok(text.clone()),
        _ => Err(RiffDbError::Decode),
    }
}

fn parse_status_value(
    value: &v1::Value,
    status_ids: TicketStatusEnumIds,
) -> Result<TicketStatus, RiffDbError> {
    match &value.kind {
        Some(v1::value::Kind::EnumValue(enum_value)) => {
            status_ids.parse(enum_value.type_id, enum_value.variant_id)
        }
        Some(v1::value::Kind::StringValue(name)) => match name.as_str() {
            "Open" => Ok(TicketStatus::Open),
            "Closed" => Ok(TicketStatus::Closed),
            "InProgress" => Ok(TicketStatus::InProgress),
            _ => Err(RiffDbError::Decode),
        },
        _ => Err(RiffDbError::Decode),
    }
}

const fn status_tag(status: TicketStatus) -> u8 {
    match status {
        TicketStatus::Open => 1,
        TicketStatus::Closed => 2,
        TicketStatus::InProgress => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::request_shape::{BoardRequestShape, inspect_board_request};
    use super::{
        TicketStatusEnumIds, assert_board_rows_equivalent, board_rows_digest, commit_token_bytes,
    };
    use riffdb_app_baseline_core::{TicketRow, TicketStatus};

    fn status_ids() -> TicketStatusEnumIds {
        TicketStatusEnumIds {
            type_id: 1,
            open: 1,
            closed: 2,
            in_progress: 3,
        }
    }

    fn sample_row(seed: u8) -> TicketRow {
        TicketRow {
            organization_id: [seed; 16],
            ticket_id: [seed.wrapping_add(1); 16],
            project_id: [seed.wrapping_add(2); 16],
            reporter_id: [seed.wrapping_add(3); 16],
            assignee_id: [seed.wrapping_add(4); 16],
            status: TicketStatus::Open,
            title: format!("t-{seed}"),
        }
    }

    #[test]
    fn request_construction_maps_board_fields_and_predicates() {
        let shape = inspect_board_request([1; 16], [2; 16], TicketStatus::Open, 450, status_ids());
        assert_eq!(shape, BoardRequestShape::expected(450));
    }

    #[test]
    fn equivalence_gate_anchors_compiled_count_to_the_limit() {
        // Empty-vs-empty (or short-vs-short) must never pass: the compiled
        // side must serve exactly the requested limit.
        let compiled = vec![sample_row(1)];
        let projected = compiled.clone();
        let err = assert_board_rows_equivalent(&compiled, &projected, 50).expect_err("anchor");
        assert!(err.contains("expected exactly 50"));
        let err = assert_board_rows_equivalent(&[], &[], 50).expect_err("empty");
        assert!(err.contains("expected exactly 50"));
    }

    #[test]
    fn equivalence_gate_detects_status_predicate_skew() {
        // Falsifiability (a): dropping status filtering widens the projected set.
        let compiled = vec![sample_row(1), sample_row(2)];
        let mut projected = compiled.clone();
        projected.push(sample_row(3)); // extra closed/other row
        let err = assert_board_rows_equivalent(&compiled, &projected, 2).expect_err("skew");
        assert!(err.contains("CROSS-PATH DIVERGENCE") || err.contains("row count"));
        assert!(err.contains("digests"));
    }

    #[test]
    fn equivalence_gate_detects_field_value_drift() {
        let compiled = vec![sample_row(1)];
        let mut projected = compiled.clone();
        projected[0].title = "mutated".to_owned();
        let err = assert_board_rows_equivalent(&compiled, &projected, 1).expect_err("drift");
        assert!(err.contains("CROSS-PATH DIVERGENCE"));
        assert_ne!(board_rows_digest(&compiled), board_rows_digest(&projected));
    }

    #[test]
    fn equivalence_gate_accepts_identical_paths() {
        let rows = vec![sample_row(9), sample_row(10)];
        assert_board_rows_equivalent(&rows, &rows, 2).expect("identical");
    }

    #[test]
    fn commit_token_bytes_round_trip_shape() {
        let bytes = commit_token_bytes(1, 42).expect("token");
        assert!(!bytes.is_empty());
        // version + incarnation + position tag + sequence
        assert!(bytes.len() >= 18);
    }
}
