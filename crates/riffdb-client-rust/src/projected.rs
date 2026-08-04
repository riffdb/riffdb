//! Typed projected-query facade over `ExecuteProjectedQuery`.
//!
//! Request construction is name-addressed (application values + field names).
//! Ready responses surface rows as canonical values; PACKED Ready is decoded
//! through a block decoder with the same offset rigor as the measurement
//! harness (monotone, closed bounds, exact cell documents).

use std::time::Duration;

use riffdb_proto::app::v1 as app_v1;
use riffdb_proto::canonical_value_from_proto;
use riffdb_types::{
    CanonicalValue, CommitToken, FreshnessPolicy, ProjectionFrontier, decode_canonical_value,
};

use crate::application::{
    ApplicationClientError, ApplicationContract, ApplicationValue, lower_value, validate_contract,
};
use crate::{CallMetadata, RiffDbClient, generate_request_id};

/// Wire encoding preference for a Ready projected-query response.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ProjectedResponseEncoding {
    /// Row-oriented `ready` arm (historical default when absent).
    #[default]
    Row,
    /// Column-major packed `ready_packed` arm.
    Packed,
}

/// Sort direction for one projected order key.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ProjectedSortDirection {
    /// Ascending (engine default).
    #[default]
    Asc,
    /// Descending.
    Desc,
}

/// Equality or range predicate addressed by projected field name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProjectedPredicate {
    /// Field equals value.
    Eq {
        /// Field name.
        field: String,
        /// Comparison value.
        value: ApplicationValue,
    },
    /// Inclusive-low / exclusive-high range (either bound optional).
    Range {
        /// Field name.
        field: String,
        /// Inclusive lower bound.
        low: Option<ApplicationValue>,
        /// Exclusive upper bound.
        high: Option<ApplicationValue>,
    },
}

/// One sort key addressed by projected field name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectedOrder {
    /// Field name (projected field or primary-key component).
    pub field: String,
    /// Sort direction.
    pub direction: ProjectedSortDirection,
}

/// Typed projected-query request builder.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectedQuery {
    contract: ApplicationContract,
    projection_name: String,
    org_scope: ApplicationValue,
    select: Vec<String>,
    predicates: Vec<ProjectedPredicate>,
    order: Vec<ProjectedOrder>,
    limit: Option<u32>,
    freshness: FreshnessPolicy,
    encoding: ProjectedResponseEncoding,
}

impl ProjectedQuery {
    /// Builds a projected query with required org scope and available freshness.
    pub fn new(
        contract: ApplicationContract,
        projection_name: impl Into<String>,
        org_scope: ApplicationValue,
    ) -> Result<Self, ApplicationClientError> {
        let projection_name = projection_name.into();
        if projection_name.is_empty() || projection_name.len() > 256 {
            return Err(ApplicationClientError::InvalidInput);
        }
        validate_contract(&contract)?;
        Ok(Self {
            contract,
            projection_name,
            org_scope,
            select: Vec::new(),
            predicates: Vec::new(),
            order: Vec::new(),
            limit: None,
            freshness: FreshnessPolicy::Available,
            encoding: ProjectedResponseEncoding::Row,
        })
    }

    /// Sets selected field names (empty = all projected fields).
    #[must_use]
    pub fn select(mut self, select: Vec<String>) -> Self {
        self.select = select;
        self
    }

    /// Sets conjunctive predicates.
    #[must_use]
    pub fn predicates(mut self, predicates: Vec<ProjectedPredicate>) -> Self {
        self.predicates = predicates;
        self
    }

    /// Sets sort keys (primary-key tie-break remains engine-owned).
    #[must_use]
    pub fn order(mut self, order: Vec<ProjectedOrder>) -> Self {
        self.order = order;
        self
    }

    /// Sets the post-sort row limit.
    #[must_use]
    pub fn limit(mut self, limit: Option<u32>) -> Self {
        self.limit = limit;
        self
    }

    /// Sets the freshness policy (Available / Bounded / Causal with token).
    #[must_use]
    pub fn freshness(mut self, freshness: FreshnessPolicy) -> Self {
        self.freshness = freshness;
        self
    }

    /// Selects row or packed Ready encoding.
    #[must_use]
    pub const fn encoding(mut self, encoding: ProjectedResponseEncoding) -> Self {
        self.encoding = encoding;
        self
    }

    /// Projection name.
    #[must_use]
    pub fn projection_name(&self) -> &str {
        &self.projection_name
    }

    /// Requested response encoding.
    #[must_use]
    pub const fn response_encoding(&self) -> ProjectedResponseEncoding {
        self.encoding
    }

    /// Builds the public wire request (for tests and advanced callers).
    pub fn into_wire_request(
        self,
    ) -> Result<app_v1::ExecuteProjectedQueryRequest, ApplicationClientError> {
        let request_id = Vec::from(
            generate_request_id()
                .map_err(|_| ApplicationClientError::IdentifierUnavailable)?
                .into_bytes(),
        );
        let mut predicates = Vec::with_capacity(self.predicates.len());
        for predicate in self.predicates {
            predicates.push(lower_predicate(predicate)?);
        }
        let order = self
            .order
            .into_iter()
            .map(|item| {
                if item.field.is_empty() || item.field.len() > 256 {
                    return Err(ApplicationClientError::InvalidInput);
                }
                Ok(app_v1::ProjectedOrder {
                    field: item.field,
                    descending: matches!(item.direction, ProjectedSortDirection::Desc),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        for name in &self.select {
            if name.is_empty() || name.len() > 256 {
                return Err(ApplicationClientError::InvalidInput);
            }
        }
        Ok(app_v1::ExecuteProjectedQueryRequest {
            contract: lower_contract_selector(self.contract),
            projection_name: self.projection_name,
            request: Some(app_v1::ProjectedQueryBody {
                select: self.select,
                org_scope: Some(lower_value(self.org_scope)?),
                predicates,
                order,
                limit: self.limit,
                group_by: Vec::new(),
                aggregate: None,
            }),
            freshness: Some(lower_freshness(self.freshness)),
            response_encoding: match self.encoding {
                ProjectedResponseEncoding::Row => {
                    Some(app_v1::ProjectedResponseEncoding::Row as i32)
                }
                ProjectedResponseEncoding::Packed => {
                    Some(app_v1::ProjectedResponseEncoding::Packed as i32)
                }
            },
            request_id,
        })
    }
}

/// One Ready projected row: select cells and primary-key components.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectedReadyRow {
    /// Select cells in `fields` order.
    pub cells: Vec<CanonicalValue>,
    /// Primary-key components in `primary_key_fields` order.
    pub primary_key: Vec<CanonicalValue>,
}

/// Closed rebuilding reason codes from the wire.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectedRebuildingReason {
    /// Replay budget exceeded.
    ReplayBudgetExceeded,
    /// Explicit rebuild requested.
    ExplicitRebuild,
    /// State integrity failure.
    StateIntegrityFailure,
}

/// Closed degraded reason codes from the wire.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectedDegradedReason {
    /// Apply lag SLO breached.
    ApplyLagSlo,
    /// Maintenance backlog.
    MaintenanceBacklog,
    /// Partial inventory.
    PartialInventory,
}

/// Typed projected-query outcome mirroring the wire oneof arms.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProjectedQueryOutcome {
    /// Published snapshot under the requested freshness policy.
    ///
    /// The select/primary-key split depends on the requested encoding, because
    /// the row-oriented wire arm carries no separate primary-key vector:
    ///
    /// * [`ProjectedResponseEncoding::Packed`] — `fields` are exactly the
    ///   selected projected fields, `primary_key_fields` are the entity
    ///   primary-key components, and each row's `cells`/`primary_key` align
    ///   with them respectively.
    /// * [`ProjectedResponseEncoding::Row`] — `primary_key_fields` is always
    ///   empty and every row's `primary_key` is empty. Primary-key names that
    ///   were not selected are appended by the server to `fields`, so they
    ///   arrive as trailing `cells`. Request `Packed` when the split matters.
    Ready {
        /// Projected field names in wire order for each row's cells.
        ///
        /// Under `Row` encoding this also carries non-selected primary-key
        /// names appended after the selected fields.
        fields: Vec<String>,
        /// Entity primary-key field names aligned with each row's primary_key.
        ///
        /// Always empty under `Row` encoding (see the variant documentation).
        primary_key_fields: Vec<String>,
        /// Matching rows (`primary_key` empty under `Row` encoding).
        rows: Vec<ProjectedReadyRow>,
        /// Served projection frontier.
        frontier: ProjectionFrontier,
        /// Application head known at serve time.
        head: ProjectionFrontier,
        /// Commit token corresponding to the served frontier when sequenced.
        commit_token: Option<CommitToken>,
    },
    /// Freshness policy not yet satisfied.
    Lagging {
        /// Required frontier derived from the causal token or bounded policy.
        required: ProjectionFrontier,
        /// Current projection frontier.
        current: ProjectionFrontier,
        /// Application head.
        head: ProjectionFrontier,
        /// Sequence distance backlog when both are sequenced.
        lag_sequences: Option<u64>,
        /// Optional retry hint.
        retry_after: Option<Duration>,
    },
    /// Catch-up before first publication.
    Building {
        /// Processed position during catch-up.
        applied_through: ProjectionFrontier,
        /// Known application head.
        head: ProjectionFrontier,
    },
    /// Rebuild in progress.
    Rebuilding {
        /// Closed reason code.
        reason: ProjectedRebuildingReason,
        /// Progress numerator.
        progress_applied: u64,
        /// Progress denominator (0 = unknown).
        progress_total: u64,
    },
    /// Degraded health while still identified.
    Degraded {
        /// Closed reason code.
        reason: ProjectedDegradedReason,
        /// Current frontier while degraded.
        current: ProjectionFrontier,
    },
    /// Durable definition fingerprint mismatch.
    Invalid {
        /// Fingerprint expected by the registered definition.
        expected_fingerprint: Vec<u8>,
        /// Fingerprint found in durable state.
        found_fingerprint: Vec<u8>,
    },
}

impl RiffDbClient {
    /// Executes one projected columnar query through the public application RPC.
    pub async fn execute_projected_query(
        &mut self,
        query: ProjectedQuery,
        metadata: &CallMetadata,
    ) -> Result<ProjectedQueryOutcome, ApplicationClientError> {
        let encoding = query.encoding;
        let request = query.into_wire_request()?;
        let response = self
            .execute_projected_query_raw(request, metadata)
            .await
            .map_err(ApplicationClientError::from)?;
        raise_projected_outcome(response, encoding)
    }
}

impl crate::StableApplicationClient {
    /// Executes one projected columnar query through the application facade.
    pub async fn execute_projected_query(
        &mut self,
        query: ProjectedQuery,
        metadata: &CallMetadata,
    ) -> Result<ProjectedQueryOutcome, ApplicationClientError> {
        self.inner.execute_projected_query(query, metadata).await
    }
}

fn lower_contract_selector(contract: ApplicationContract) -> Option<app_v1::ContractSelector> {
    match contract {
        ApplicationContract::Active => None,
        ApplicationContract::Exact {
            lineage,
            version,
            bundle_hash,
        } => Some(app_v1::ContractSelector {
            lineage,
            version,
            bundle_hash: bundle_hash.map_or_else(Vec::new, |hash| hash.to_vec()),
        }),
    }
}

fn lower_predicate(
    predicate: ProjectedPredicate,
) -> Result<app_v1::ProjectedPredicate, ApplicationClientError> {
    match predicate {
        ProjectedPredicate::Eq { field, value } => {
            if field.is_empty() || field.len() > 256 {
                return Err(ApplicationClientError::InvalidInput);
            }
            Ok(app_v1::ProjectedPredicate {
                field,
                kind: Some(app_v1::projected_predicate::Kind::Eq(lower_value(value)?)),
            })
        }
        ProjectedPredicate::Range { field, low, high } => {
            if field.is_empty() || field.len() > 256 {
                return Err(ApplicationClientError::InvalidInput);
            }
            Ok(app_v1::ProjectedPredicate {
                field,
                kind: Some(app_v1::projected_predicate::Kind::Range(
                    app_v1::ProjectedRange {
                        lower: low.map(lower_value).transpose()?,
                        upper: high.map(lower_value).transpose()?,
                        // Engine shape: inclusive-low / exclusive-high.
                        lower_inclusive: true,
                        upper_inclusive: false,
                    },
                )),
            })
        }
    }
}

fn lower_freshness(freshness: FreshnessPolicy) -> app_v1::FreshnessPolicyProto {
    let policy = match freshness {
        FreshnessPolicy::Available => {
            app_v1::freshness_policy_proto::Policy::Available(app_v1::FreshnessAvailable {})
        }
        FreshnessPolicy::Bounded { max_lag_sequences } => {
            app_v1::freshness_policy_proto::Policy::Bounded(app_v1::FreshnessBounded {
                max_lag_sequences,
            })
        }
        FreshnessPolicy::Causal { token, max_wait } => {
            app_v1::freshness_policy_proto::Policy::Causal(app_v1::FreshnessCausal {
                commit_token: token.into_bytes(),
                max_wait_nanos: u64::try_from(max_wait.as_nanos()).unwrap_or(u64::MAX),
            })
        }
    };
    app_v1::FreshnessPolicyProto {
        policy: Some(policy),
    }
}

fn raise_projected_outcome(
    response: app_v1::ExecuteProjectedQueryResponse,
    encoding: ProjectedResponseEncoding,
) -> Result<ProjectedQueryOutcome, ApplicationClientError> {
    match response.outcome {
        Some(app_v1::execute_projected_query_response::Outcome::Ready(ready)) => {
            if matches!(encoding, ProjectedResponseEncoding::Packed) {
                // Falsifiability (c): PACKED requests must not surface as Ready row.
                return Err(ApplicationClientError::InvalidResponse);
            }
            raise_ready_row(ready)
        }
        Some(app_v1::execute_projected_query_response::Outcome::ReadyPacked(packed)) => {
            if matches!(encoding, ProjectedResponseEncoding::Row) {
                // The packed arm is present only when the request opted into
                // PACKED; the opposite substitution is a protocol violation.
                return Err(ApplicationClientError::InvalidResponse);
            }
            raise_ready_packed(packed)
        }
        Some(app_v1::execute_projected_query_response::Outcome::Lagging(lagging)) => {
            Ok(ProjectedQueryOutcome::Lagging {
                required: raise_frontier(lagging.required)?,
                current: raise_frontier(lagging.current)?,
                head: raise_frontier(lagging.head)?,
                lag_sequences: lagging.lag_sequences,
                retry_after: lagging.retry_after_nanos.map(Duration::from_nanos),
            })
        }
        Some(app_v1::execute_projected_query_response::Outcome::Building(building)) => {
            Ok(ProjectedQueryOutcome::Building {
                applied_through: raise_frontier(building.applied_through)?,
                head: raise_frontier(building.head)?,
            })
        }
        Some(app_v1::execute_projected_query_response::Outcome::Rebuilding(rebuilding)) => {
            Ok(ProjectedQueryOutcome::Rebuilding {
                reason: raise_rebuilding_reason(rebuilding.reason)?,
                progress_applied: rebuilding.progress_applied,
                progress_total: rebuilding.progress_total,
            })
        }
        Some(app_v1::execute_projected_query_response::Outcome::Degraded(degraded)) => {
            Ok(ProjectedQueryOutcome::Degraded {
                reason: raise_degraded_reason(degraded.reason)?,
                current: raise_frontier(degraded.current)?,
            })
        }
        Some(app_v1::execute_projected_query_response::Outcome::Invalid(invalid)) => {
            Ok(ProjectedQueryOutcome::Invalid {
                expected_fingerprint: invalid.expected_fingerprint,
                found_fingerprint: invalid.found_fingerprint,
            })
        }
        None => Err(ApplicationClientError::InvalidResponse),
    }
}

fn raise_ready_row(
    ready: app_v1::ProjectedQueryReady,
) -> Result<ProjectedQueryOutcome, ApplicationClientError> {
    // Row wire merges select then PK names not already in select into `fields`.
    // Without an explicit PK list we treat all fields as select cells and leave
    // primary_key empty (row arm does not carry a separate PK vector).
    let fields = ready.fields;
    let mut rows = Vec::with_capacity(ready.rows.len());
    for row in ready.rows {
        let mut cells = Vec::with_capacity(fields.len());
        // Build a name → value map; missing names fail closed.
        let mut by_name = std::collections::BTreeMap::new();
        for parameter in row.fields {
            let value = parameter
                .value
                .ok_or(ApplicationClientError::InvalidResponse)?;
            let canonical = canonical_value_from_proto(value)
                .map_err(|_| ApplicationClientError::InvalidResponse)?;
            if by_name.insert(parameter.name, canonical).is_some() {
                return Err(ApplicationClientError::InvalidResponse);
            }
        }
        for name in &fields {
            let value = by_name
                .remove(name)
                .ok_or(ApplicationClientError::InvalidResponse)?;
            cells.push(value);
        }
        if !by_name.is_empty() {
            return Err(ApplicationClientError::InvalidResponse);
        }
        rows.push(ProjectedReadyRow {
            cells,
            primary_key: Vec::new(),
        });
    }
    Ok(ProjectedQueryOutcome::Ready {
        fields,
        primary_key_fields: Vec::new(),
        rows,
        frontier: raise_frontier(ready.frontier)?,
        head: raise_frontier(ready.head)?,
        commit_token: raise_optional_token(ready.commit_token)?,
    })
}

/// Block decoder: offsets → cell byte slices → decode_canonical_value.
///
/// Offset validation matches the harness packed decoder: length = row_count+1,
/// first offset 0, last offset = data.len(), monotone non-decreasing, and each
/// slice is an exact canonical document (trailing garbage rejected by decode).
///
/// `row_count` is remote input, so a nonzero count requires at least one column:
/// with a column present the offsets checks bound the row count by the actual
/// payload, and without one nothing else would.
fn raise_ready_packed(
    packed: app_v1::ProjectedReadyPacked,
) -> Result<ProjectedQueryOutcome, ApplicationClientError> {
    let expected_columns = packed
        .primary_key_fields
        .len()
        .saturating_add(packed.fields.len());
    if packed.columns.len() != expected_columns {
        return Err(ApplicationClientError::InvalidResponse);
    }
    let row_count = packed.row_count as usize;
    if packed.columns.is_empty() && row_count != 0 {
        return Err(ApplicationClientError::InvalidResponse);
    }
    for column in &packed.columns {
        validate_packed_column_offsets(column, row_count)?;
    }

    let mut rows = Vec::with_capacity(row_count);
    for row_index in 0..row_count {
        let mut primary_key = Vec::with_capacity(packed.primary_key_fields.len());
        let mut cells = Vec::with_capacity(packed.fields.len());
        for (col_index, column) in packed.columns.iter().enumerate() {
            let start = column.offsets[row_index] as usize;
            let end = column.offsets[row_index + 1] as usize;
            let value = decode_canonical_value(&column.data[start..end])
                .map_err(|_| ApplicationClientError::InvalidResponse)?;
            if col_index < packed.primary_key_fields.len() {
                primary_key.push(value);
            } else {
                cells.push(value);
            }
        }
        rows.push(ProjectedReadyRow { cells, primary_key });
    }
    Ok(ProjectedQueryOutcome::Ready {
        fields: packed.fields,
        primary_key_fields: packed.primary_key_fields,
        rows,
        frontier: raise_frontier(packed.frontier)?,
        head: raise_frontier(packed.head)?,
        commit_token: raise_optional_token(packed.commit_token)?,
    })
}

fn validate_packed_column_offsets(
    column: &app_v1::PackedColumn,
    row_count: usize,
) -> Result<(), ApplicationClientError> {
    if column.offsets.len() != row_count.saturating_add(1) {
        return Err(ApplicationClientError::InvalidResponse);
    }
    if column.offsets.first().copied() != Some(0) {
        return Err(ApplicationClientError::InvalidResponse);
    }
    let last = *column.offsets.last().unwrap_or(&0) as usize;
    if last != column.data.len() {
        return Err(ApplicationClientError::InvalidResponse);
    }
    for window in column.offsets.windows(2) {
        if window[0] > window[1] || window[1] as usize > column.data.len() {
            return Err(ApplicationClientError::InvalidResponse);
        }
    }
    Ok(())
}

fn raise_frontier(bytes: Vec<u8>) -> Result<ProjectionFrontier, ApplicationClientError> {
    ProjectionFrontier::from_bytes(bytes).map_err(|_| ApplicationClientError::InvalidResponse)
}

fn raise_optional_token(bytes: Vec<u8>) -> Result<Option<CommitToken>, ApplicationClientError> {
    if bytes.is_empty() {
        return Ok(None);
    }
    CommitToken::from_bytes(bytes)
        .map(Some)
        .map_err(|_| ApplicationClientError::InvalidResponse)
}

fn raise_rebuilding_reason(
    reason: i32,
) -> Result<ProjectedRebuildingReason, ApplicationClientError> {
    match app_v1::ProjectedRebuildingReason::try_from(reason) {
        Ok(app_v1::ProjectedRebuildingReason::ReplayBudgetExceeded) => {
            Ok(ProjectedRebuildingReason::ReplayBudgetExceeded)
        }
        Ok(app_v1::ProjectedRebuildingReason::ExplicitRebuild) => {
            Ok(ProjectedRebuildingReason::ExplicitRebuild)
        }
        Ok(app_v1::ProjectedRebuildingReason::StateIntegrityFailure) => {
            Ok(ProjectedRebuildingReason::StateIntegrityFailure)
        }
        Ok(app_v1::ProjectedRebuildingReason::Unspecified) | Err(_) => {
            Err(ApplicationClientError::InvalidResponse)
        }
    }
}

fn raise_degraded_reason(reason: i32) -> Result<ProjectedDegradedReason, ApplicationClientError> {
    match app_v1::ProjectedDegradedReason::try_from(reason) {
        Ok(app_v1::ProjectedDegradedReason::ApplyLagSlo) => {
            Ok(ProjectedDegradedReason::ApplyLagSlo)
        }
        Ok(app_v1::ProjectedDegradedReason::MaintenanceBacklog) => {
            Ok(ProjectedDegradedReason::MaintenanceBacklog)
        }
        Ok(app_v1::ProjectedDegradedReason::PartialInventory) => {
            Ok(ProjectedDegradedReason::PartialInventory)
        }
        Ok(app_v1::ProjectedDegradedReason::Unspecified) | Err(_) => {
            Err(ApplicationClientError::InvalidResponse)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_types::{CommitSequence, encode_canonical_value};

    fn pack_one(value: &CanonicalValue) -> app_v1::PackedColumn {
        let data = encode_canonical_value(value).expect("enc");
        let end = data.len() as u32;
        app_v1::PackedColumn {
            data,
            offsets: vec![0, end],
        }
    }

    fn valid_packed() -> app_v1::ProjectedReadyPacked {
        app_v1::ProjectedReadyPacked {
            fields: vec!["title".to_owned()],
            primary_key_fields: vec!["ticket_id".to_owned()],
            row_count: 1,
            columns: vec![
                pack_one(&CanonicalValue::Uuid([1; 16])),
                pack_one(&CanonicalValue::string("hello").expect("s")),
            ],
            frontier: ProjectionFrontier::new(
                1,
                riffdb_types::FrontierPosition::AppliedThrough(
                    CommitSequence::new(7).expect("seq"),
                ),
            )
            .into_bytes(),
            head: ProjectionFrontier::new(
                1,
                riffdb_types::FrontierPosition::AppliedThrough(
                    CommitSequence::new(9).expect("seq"),
                ),
            )
            .into_bytes(),
            commit_token: CommitToken::new(1, CommitSequence::new(7).expect("seq")).into_bytes(),
        }
    }

    #[test]
    fn packed_decoder_accepts_baseline_and_rejects_hostile_shapes() {
        let ok = raise_ready_packed(valid_packed()).expect("valid");
        match ok {
            ProjectedQueryOutcome::Ready {
                fields,
                primary_key_fields,
                rows,
                ..
            } => {
                assert_eq!(fields, vec!["title".to_owned()]);
                assert_eq!(primary_key_fields, vec!["ticket_id".to_owned()]);
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].primary_key, vec![CanonicalValue::Uuid([1; 16])]);
                assert_eq!(
                    rows[0].cells,
                    vec![CanonicalValue::string("hello").expect("s")]
                );
            }
            other => panic!("expected Ready, got {other:?}"),
        }

        // Truncated data: last offset beyond the buffer.
        let mut truncated = valid_packed();
        truncated.columns[0].data.pop();
        assert!(raise_ready_packed(truncated).is_err());

        // Non-monotone offsets — falsifiability (a).
        let mut nonmono = valid_packed();
        nonmono.columns[1].offsets = vec![1, 0];
        assert!(
            raise_ready_packed(nonmono).is_err(),
            "non-monotone offsets must fail closed"
        );

        // Trailing garbage inside a cell.
        let mut garbage = valid_packed();
        garbage.columns[1].data.push(0xFF);
        let end = garbage.columns[1].data.len() as u32;
        garbage.columns[1].offsets = vec![0, end];
        assert!(raise_ready_packed(garbage).is_err());

        // Row-count mismatch.
        let mut mismatch = valid_packed();
        mismatch.row_count = 2;
        assert!(raise_ready_packed(mismatch).is_err());
    }

    /// A nonzero `row_count` with no columns must not reserve remote-chosen
    /// capacity: with zero columns the per-column offset checks are vacuous, so
    /// this shape is the only path where `row_count` is unbounded by payload.
    #[test]
    fn packed_decoder_rejects_rows_without_columns() {
        let mut fabricated = valid_packed();
        fabricated.fields = Vec::new();
        fabricated.primary_key_fields = Vec::new();
        fabricated.columns = Vec::new();
        fabricated.row_count = 100_000;
        let err = raise_ready_packed(fabricated)
            .expect_err("nonzero row_count with no columns must fail closed");
        assert!(matches!(err, ApplicationClientError::InvalidResponse));

        // Genuinely empty result sets stay acceptable.
        let mut empty = valid_packed();
        empty.fields = Vec::new();
        empty.primary_key_fields = Vec::new();
        empty.columns = Vec::new();
        empty.row_count = 0;
        match raise_ready_packed(empty).expect("zero rows and zero columns") {
            ProjectedQueryOutcome::Ready { rows, fields, .. } => {
                assert!(rows.is_empty());
                assert!(fields.is_empty());
            }
            other => panic!("expected Ready, got {other:?}"),
        }
    }

    /// The monotone comparison is the sole guard against a reversed cell span.
    /// This shape passes every other offsets check (length, first == 0,
    /// last == data.len(), all offsets in bounds) and only the pairwise
    /// comparison rejects it; without that comparison the decoder slices
    /// `data[span..0]` and panics.
    #[test]
    fn packed_decoder_rejects_mid_column_offset_decrease() {
        let cell = encode_canonical_value(&CanonicalValue::Uuid([2; 16])).expect("enc");
        let span = u32::try_from(cell.len()).expect("span");
        let mut data = Vec::with_capacity(cell.len() * 3);
        for _ in 0..3 {
            data.extend_from_slice(&cell);
        }
        let mut hostile = valid_packed();
        hostile.fields = vec!["title".to_owned()];
        hostile.primary_key_fields = Vec::new();
        hostile.row_count = 3;
        hostile.columns = vec![app_v1::PackedColumn {
            data,
            offsets: vec![0, span, 0, span * 3],
        }];
        let err = raise_ready_packed(hostile)
            .expect_err("mid-column offset decrease must fail closed, not panic");
        assert!(matches!(err, ApplicationClientError::InvalidResponse));
    }

    #[test]
    fn packed_request_builder_sets_packed_encoding() {
        let query = ProjectedQuery::new(
            ApplicationContract::Exact {
                lineage: "TicketDesk".to_owned(),
                version: 1,
                bundle_hash: None,
            },
            "board",
            ApplicationValue::Uuid(crate::ApplicationUuid::from_bytes([9; 16])),
        )
        .expect("query")
        .encoding(ProjectedResponseEncoding::Packed)
        .limit(Some(50));
        let wire = query.into_wire_request().expect("wire");
        assert_eq!(
            wire.response_encoding,
            Some(app_v1::ProjectedResponseEncoding::Packed as i32)
        );
        assert_eq!(wire.projection_name, "board");
        assert_eq!(wire.request.as_ref().expect("body").limit, Some(50));
    }

    #[test]
    fn packed_encoding_rejects_ready_row_arm() {
        let ready = app_v1::ExecuteProjectedQueryResponse {
            outcome: Some(app_v1::execute_projected_query_response::Outcome::Ready(
                app_v1::ProjectedQueryReady {
                    fields: vec!["title".to_owned()],
                    rows: Vec::new(),
                    frontier: ProjectionFrontier::new(
                        1,
                        riffdb_types::FrontierPosition::BeforeFirst,
                    )
                    .into_bytes(),
                    head: ProjectionFrontier::new(1, riffdb_types::FrontierPosition::BeforeFirst)
                        .into_bytes(),
                    commit_token: Vec::new(),
                },
            )),
        };
        let err = raise_projected_outcome(ready, ProjectedResponseEncoding::Packed)
            .expect_err("row arm under packed must fail");
        assert!(matches!(err, ApplicationClientError::InvalidResponse));
    }

    #[test]
    fn row_encoding_rejects_ready_packed_arm() {
        let packed = app_v1::ExecuteProjectedQueryResponse {
            outcome: Some(
                app_v1::execute_projected_query_response::Outcome::ReadyPacked(valid_packed()),
            ),
        };
        let err = raise_projected_outcome(packed, ProjectedResponseEncoding::Row)
            .expect_err("packed arm under row must fail");
        assert!(matches!(err, ApplicationClientError::InvalidResponse));

        // The same payload remains acceptable under a PACKED request.
        let packed = app_v1::ExecuteProjectedQueryResponse {
            outcome: Some(
                app_v1::execute_projected_query_response::Outcome::ReadyPacked(valid_packed()),
            ),
        };
        assert!(raise_projected_outcome(packed, ProjectedResponseEncoding::Packed).is_ok());
    }
}
