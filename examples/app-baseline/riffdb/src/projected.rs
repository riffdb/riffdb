//! Projected board path: ExecuteProjectedQuery over the generated tonic client.
//!
//! Measures the honest full wire path (not a Rust SDK facade). Request shapes
//! mirror `tests/service/projected_read_acceptance.rs` / CP2b.
//!
//! CP4 adds opt-in packed column-major Ready decoding: offsets → cell slices →
//! `decode_canonical_value` → the same [`TicketRow`] construction as the row arm.

use std::time::Duration;

use riffdb_api_grpc::generated_app::application_query_service_client::ApplicationQueryServiceClient;
use riffdb_app_baseline_core::{TicketRow, TicketStatus, UuidBytes};
use riffdb_client_rust::generate_request_id;
use riffdb_proto::app::v1 as app_v1;
use riffdb_proto::v1;
use riffdb_types::{CanonicalValue, CommitSequence, CommitToken, decode_canonical_value};
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
///
/// Defaults to row encoding (absent / historical semantics).
#[must_use]
pub fn build_board_projected_request(
    organization_id: UuidBytes,
    project_id: UuidBytes,
    status: TicketStatus,
    limit: u32,
    status_ids: TicketStatusEnumIds,
    freshness: app_v1::FreshnessPolicyProto,
) -> app_v1::ExecuteProjectedQueryRequest {
    build_board_projected_request_with_encoding(
        organization_id,
        project_id,
        status,
        limit,
        status_ids,
        freshness,
        None,
    )
}

/// Like [`build_board_projected_request`] with an explicit response encoding.
#[must_use]
pub fn build_board_projected_request_with_encoding(
    organization_id: UuidBytes,
    project_id: UuidBytes,
    status: TicketStatus,
    limit: u32,
    status_ids: TicketStatusEnumIds,
    freshness: app_v1::FreshnessPolicyProto,
    response_encoding: Option<app_v1::ProjectedResponseEncoding>,
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
            aggregates: Vec::new(),
        }),
        freshness: Some(freshness),
        response_encoding: response_encoding.map(|encoding| encoding as i32),
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
    execute_projected_board_with_encoding(
        channel,
        bearer_token,
        organization_id,
        project_id,
        status,
        limit,
        status_ids,
        freshness,
        None,
    )
    .await
}

/// Executes one projected board query, optionally requesting packed encoding.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn execute_projected_board_with_encoding(
    channel: &Channel,
    bearer_token: &str,
    organization_id: UuidBytes,
    project_id: UuidBytes,
    status: TicketStatus,
    limit: u32,
    status_ids: TicketStatusEnumIds,
    freshness: app_v1::FreshnessPolicyProto,
    response_encoding: Option<app_v1::ProjectedResponseEncoding>,
) -> Result<Vec<TicketRow>, RiffDbError> {
    let mut client = ApplicationQueryServiceClient::new(channel.clone());
    let message = build_board_projected_request_with_encoding(
        organization_id,
        project_id,
        status,
        limit,
        status_ids,
        freshness,
        response_encoding,
    );
    let request = authenticated_request(message, bearer_token)?;
    let response = client
        .execute_projected_query(request)
        .await
        .map_err(|status| RiffDbError::Rpc(format!("ExecuteProjectedQuery: {status}")))?
        .into_inner();
    decode_projected_board_rows(response, organization_id, status_ids)
}

/// Executes one packed projected board query (response_encoding = PACKED).
#[allow(clippy::too_many_arguments)]
pub async fn execute_projected_board_packed(
    channel: &Channel,
    bearer_token: &str,
    organization_id: UuidBytes,
    project_id: UuidBytes,
    status: TicketStatus,
    limit: u32,
    status_ids: TicketStatusEnumIds,
    freshness: app_v1::FreshnessPolicyProto,
) -> Result<Vec<TicketRow>, RiffDbError> {
    execute_projected_board_with_encoding(
        channel,
        bearer_token,
        organization_id,
        project_id,
        status,
        limit,
        status_ids,
        freshness,
        Some(app_v1::ProjectedResponseEncoding::Packed),
    )
    .await
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

/// Decodes Ready or ReadyPacked rows into [`TicketRow`] values.
pub(crate) fn decode_projected_board_rows(
    response: app_v1::ExecuteProjectedQueryResponse,
    organization_id: UuidBytes,
    status_ids: TicketStatusEnumIds,
) -> Result<Vec<TicketRow>, RiffDbError> {
    match response.outcome {
        Some(app_v1::execute_projected_query_response::Outcome::Ready(ready)) => {
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
        Some(app_v1::execute_projected_query_response::Outcome::ReadyPacked(packed)) => {
            decode_projected_board_rows_packed(packed, organization_id, status_ids)
        }
        other => Err(RiffDbError::Rpc(format!(
            "projected board expected Ready or ReadyPacked, got {other:?}"
        ))),
    }
}

/// Block decoder: offsets → cell byte slices → decode_canonical_value → TicketRow.
pub(crate) fn decode_projected_board_rows_packed(
    packed: app_v1::ProjectedReadyPacked,
    organization_id: UuidBytes,
    status_ids: TicketStatusEnumIds,
) -> Result<Vec<TicketRow>, RiffDbError> {
    for (index, name) in BOARD_SELECT.iter().enumerate() {
        if packed.fields.get(index).map(String::as_str) != Some(*name) {
            return Err(RiffDbError::Rpc(format!(
                "projected packed select drift at {index}: expected {name}, fields={:?}",
                packed.fields
            )));
        }
    }
    let expected_columns = packed
        .primary_key_fields
        .len()
        .saturating_add(packed.fields.len());
    if packed.columns.len() != expected_columns {
        return Err(RiffDbError::Rpc(format!(
            "projected packed column count {} != pk {} + select {}",
            packed.columns.len(),
            packed.primary_key_fields.len(),
            packed.fields.len()
        )));
    }
    let row_count = packed.row_count as usize;
    for (col_index, column) in packed.columns.iter().enumerate() {
        if column.offsets.len() != row_count.saturating_add(1) {
            return Err(RiffDbError::Decode);
        }
        if column.offsets.first().copied() != Some(0) {
            return Err(RiffDbError::Decode);
        }
        let last = *column.offsets.last().unwrap_or(&0) as usize;
        if last != column.data.len() {
            return Err(RiffDbError::Decode);
        }
        // Monotonic non-decreasing offsets (empty cells allowed).
        for window in column.offsets.windows(2) {
            if window[0] > window[1] || window[1] as usize > column.data.len() {
                return Err(RiffDbError::Decode);
            }
        }
        let _ = col_index;
    }

    // Column names in pack order: PK first, then select.
    let mut column_names = Vec::with_capacity(expected_columns);
    column_names.extend(packed.primary_key_fields.iter().cloned());
    column_names.extend(packed.fields.iter().cloned());

    let mut rows = Vec::with_capacity(row_count);
    for row_index in 0..row_count {
        let mut ticket_id = None;
        let mut project_id = None;
        let mut title = None;
        let mut status = None;
        let mut reporter_id = None;
        let mut assignee_id = None;
        for (col_index, name) in column_names.iter().enumerate() {
            let column = &packed.columns[col_index];
            let start = column.offsets[row_index] as usize;
            let end = column.offsets[row_index + 1] as usize;
            let value = decode_canonical_value(&column.data[start..end])
                .map_err(|_| RiffDbError::Decode)?;
            match name.as_str() {
                "ticket_id" => ticket_id = Some(canonical_uuid(&value)?),
                "project_id" => project_id = Some(canonical_uuid(&value)?),
                "title" => title = Some(canonical_string(&value)?),
                "status" => status = Some(canonical_status(&value, status_ids)?),
                "reporter_id" => reporter_id = Some(canonical_uuid(&value)?),
                "assignee_id" => assignee_id = Some(canonical_uuid(&value)?),
                "organization_id" => {
                    let _ = canonical_uuid(&value)?;
                }
                _ => {}
            }
        }
        rows.push(TicketRow {
            organization_id,
            ticket_id: ticket_id.ok_or(RiffDbError::Decode)?,
            project_id: project_id.ok_or(RiffDbError::Decode)?,
            reporter_id: reporter_id.ok_or(RiffDbError::Decode)?,
            assignee_id: assignee_id.ok_or(RiffDbError::Decode)?,
            status: status.ok_or(RiffDbError::Decode)?,
            title: title.ok_or(RiffDbError::Decode)?,
        });
    }
    Ok(rows)
}

fn canonical_uuid(value: &CanonicalValue) -> Result<UuidBytes, RiffDbError> {
    match value {
        CanonicalValue::Uuid(bytes) => Ok(*bytes),
        CanonicalValue::String(text) => parse_uuid_text(text.as_str()),
        _ => Err(RiffDbError::Decode),
    }
}

fn canonical_string(value: &CanonicalValue) -> Result<String, RiffDbError> {
    match value {
        CanonicalValue::String(text) => Ok(text.as_str().to_owned()),
        _ => Err(RiffDbError::Decode),
    }
}

fn canonical_status(
    value: &CanonicalValue,
    status_ids: TicketStatusEnumIds,
) -> Result<TicketStatus, RiffDbError> {
    match value {
        CanonicalValue::Enum {
            type_id,
            variant_id,
        } => status_ids.parse(type_id.get(), variant_id.get()),
        CanonicalValue::String(name) => match name.as_str() {
            "Open" => Ok(TicketStatus::Open),
            "Closed" => Ok(TicketStatus::Closed),
            "InProgress" => Ok(TicketStatus::InProgress),
            _ => Err(RiffDbError::Decode),
        },
        _ => Err(RiffDbError::Decode),
    }
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
    assert_board_rows_three_way_equivalent(compiled, projected, projected, limit)
}

/// Three-way equivalence: compiled vs projected-row vs projected-packed.
///
/// All three paths must produce byte-identical rows, count-anchored at exactly
/// `limit`. Divergence aborts with digests for every path.
pub fn assert_board_rows_three_way_equivalent(
    compiled: &[TicketRow],
    projected_row: &[TicketRow],
    projected_packed: &[TicketRow],
    limit: u32,
) -> Result<(), String> {
    // Count anchor: empty-vs-empty must never pass. The board cell is seeded
    // dense, so the compiled side must serve exactly the requested limit even
    // when PostgreSQL's cross-check is skipped.
    if compiled.len() != limit as usize {
        return Err(format!(
            "measurement-integrity: board_page_projected({limit}) compiled side served \
             {} rows, expected exactly {limit} — dataset or query drift",
            compiled.len()
        ));
    }
    let c_digest = board_rows_digest(compiled);
    let r_digest = board_rows_digest(projected_row);
    let p_digest = board_rows_digest(projected_packed);
    if compiled.len() != projected_row.len() || compiled.len() != projected_packed.len() {
        return Err(format!(
            "measurement-integrity: board_page_projected({limit}) row count \
             compiled={} projected_row={} projected_packed={} digests \
             compiled={c_digest} projected_row={r_digest} projected_packed={p_digest}",
            compiled.len(),
            projected_row.len(),
            projected_packed.len(),
        ));
    }
    for (index, ((left, row), packed)) in compiled
        .iter()
        .zip(projected_row.iter())
        .zip(projected_packed.iter())
        .enumerate()
    {
        if left != row || left != packed {
            return Err(format!(
                "measurement-integrity: CROSS-PATH DIVERGENCE board_page_projected({limit}) \
                 at index {index}: digests compiled={c_digest} projected_row={r_digest} \
                 projected_packed={p_digest}"
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
        TicketStatusEnumIds, assert_board_rows_equivalent, assert_board_rows_three_way_equivalent,
        board_rows_digest, commit_token_bytes, decode_projected_board_rows_packed,
    };
    use riffdb_app_baseline_core::{TicketRow, TicketStatus};
    use riffdb_proto::app::v1 as app_v1;
    use riffdb_types::{CanonicalValue, encode_canonical_value};

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

    #[test]
    fn three_way_gate_detects_packed_divergence() {
        let compiled = vec![sample_row(1), sample_row(2)];
        let row = compiled.clone();
        let mut packed = compiled.clone();
        packed[1].title = "swapped-column-effect".to_owned();
        let err =
            assert_board_rows_three_way_equivalent(&compiled, &row, &packed, 2).expect_err("div");
        assert!(err.contains("CROSS-PATH DIVERGENCE"));
        assert!(err.contains("projected_packed"));
        assert!(err.contains("digests"));
    }

    #[test]
    fn three_way_gate_accepts_identical_paths() {
        let rows = vec![sample_row(3), sample_row(4)];
        assert_board_rows_three_way_equivalent(&rows, &rows, &rows, 2).expect("identical");
    }

    #[test]
    fn three_way_gate_anchors_count_like_pairwise() {
        let err = assert_board_rows_three_way_equivalent(&[], &[], &[], 50).expect_err("empty");
        assert!(err.contains("expected exactly 50"));
    }

    /// Falsifiability (a): corrupt one column's offsets by one → packed decode fails.
    #[test]
    fn packed_decode_rejects_corrupt_offsets() {
        let status_ids = status_ids();
        let ticket = CanonicalValue::Uuid([1; 16]);
        let title = CanonicalValue::string("t").expect("s");
        let project = CanonicalValue::Uuid([2; 16]);
        let status = CanonicalValue::Enum {
            type_id: riffdb_types::EnumTypeId::new(status_ids.type_id).expect("t"),
            variant_id: riffdb_types::EnumVariantId::new(status_ids.open).expect("v"),
        };
        let reporter = CanonicalValue::Uuid([3; 16]);
        let assignee = CanonicalValue::Uuid([4; 16]);
        let org = CanonicalValue::Uuid([5; 16]);

        let pack_one = |value: &CanonicalValue| {
            let data = encode_canonical_value(value).expect("enc");
            let end = data.len() as u32;
            app_v1::PackedColumn {
                data,
                offsets: vec![0, end],
            }
        };

        // Column order: PK first (organization_id, ticket_id), then select.
        let mut packed = app_v1::ProjectedReadyPacked {
            fields: super::BOARD_SELECT
                .iter()
                .map(|name| (*name).to_owned())
                .collect(),
            primary_key_fields: vec!["organization_id".to_owned(), "ticket_id".to_owned()],
            row_count: 1,
            columns: vec![
                pack_one(&org),
                pack_one(&ticket),
                pack_one(&project),
                pack_one(&title),
                pack_one(&status),
                pack_one(&reporter),
                pack_one(&assignee),
            ],
            frontier: Vec::new(),
            head: Vec::new(),
            commit_token: Vec::new(),
        };
        // Corrupt title column offsets by one.
        packed.columns[3].offsets[1] = packed.columns[3].offsets[1].saturating_add(1);
        let err = decode_projected_board_rows_packed(packed, [5; 16], status_ids)
            .expect_err("corrupt offsets");
        // Named failure: Decode or Rpc from length mismatch / bad cell.
        let msg = err.to_string();
        assert!(
            msg.to_lowercase().contains("decode") || msg.contains("offset"),
            "corrupt offsets must fail as a decode/offset error, got: {msg}"
        );
    }

    fn valid_packed_fixture(status_ids: TicketStatusEnumIds) -> app_v1::ProjectedReadyPacked {
        let pack_one = |value: &CanonicalValue| {
            let data = encode_canonical_value(value).expect("enc");
            let end = data.len() as u32;
            app_v1::PackedColumn {
                data,
                offsets: vec![0, end],
            }
        };
        let status = CanonicalValue::Enum {
            type_id: riffdb_types::EnumTypeId::new(status_ids.type_id).expect("t"),
            variant_id: riffdb_types::EnumVariantId::new(status_ids.open).expect("v"),
        };
        app_v1::ProjectedReadyPacked {
            fields: super::BOARD_SELECT
                .iter()
                .map(|name| (*name).to_owned())
                .collect(),
            primary_key_fields: vec!["organization_id".to_owned(), "ticket_id".to_owned()],
            row_count: 1,
            columns: vec![
                pack_one(&CanonicalValue::Uuid([5; 16])),
                pack_one(&CanonicalValue::Uuid([1; 16])),
                pack_one(&CanonicalValue::Uuid([2; 16])),
                pack_one(&CanonicalValue::string("t").expect("s")),
                pack_one(&status),
                pack_one(&CanonicalValue::Uuid([3; 16])),
                pack_one(&CanonicalValue::Uuid([4; 16])),
            ],
            frontier: Vec::new(),
            head: Vec::new(),
            commit_token: Vec::new(),
        }
    }

    /// Review probes promoted to permanent coverage: every malformed packed
    /// shape must fail closed, and the pristine fixture must decode.
    #[test]
    fn packed_decoder_rejects_hostile_shapes_and_accepts_the_baseline() {
        let ids = status_ids();
        assert!(
            decode_projected_board_rows_packed(valid_packed_fixture(ids), [5; 16], ids).is_ok()
        );

        // Truncated data: last offset beyond the buffer.
        let mut truncated = valid_packed_fixture(ids);
        truncated.columns[0].data.pop();
        assert!(decode_projected_board_rows_packed(truncated, [5; 16], ids).is_err());

        // Non-monotone offsets.
        let mut nonmono = valid_packed_fixture(ids);
        nonmono.columns[1].offsets = vec![1, 0];
        assert!(decode_projected_board_rows_packed(nonmono, [5; 16], ids).is_err());

        // Trailing garbage inside a cell (canonical decode rejects trailing bytes).
        let mut garbage = valid_packed_fixture(ids);
        garbage.columns[2].data.push(0xFF);
        let end = garbage.columns[2].data.len() as u32;
        garbage.columns[2].offsets = vec![0, end];
        assert!(decode_projected_board_rows_packed(garbage, [5; 16], ids).is_err());

        // Row-count mismatch: offsets say one row, header says two.
        let mut mismatch = valid_packed_fixture(ids);
        mismatch.row_count = 2;
        assert!(decode_projected_board_rows_packed(mismatch, [5; 16], ids).is_err());
    }
}
