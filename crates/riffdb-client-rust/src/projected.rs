//! Typed projected-query facade over `ExecuteProjectedQuery`.
//!
//! Request construction is name-addressed (application values + field names).
//! Ready responses surface rows as canonical values; PACKED Ready is decoded
//! through a block decoder with the same offset rigor as the measurement
//! harness (monotone, closed bounds, exact cell documents).

use std::time::Duration;

use riffdb_proto::app::v1 as app_v1;
use riffdb_proto::{aggregate_sum_from_proto, canonical_value_from_proto};
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

/// One aggregate function over a projected column.
///
/// Typed rather than `(op, field)`: `Count` reads no column, so a count that
/// names one is unrepresentable instead of rejected at the server.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProjectedAggregate {
    /// Number of matching rows.
    Count,
    /// Exact integer sum of one column.
    Sum {
        /// Field name to sum.
        field: String,
    },
    /// Smallest value of one column.
    Min {
        /// Field name to minimize.
        field: String,
    },
    /// Largest value of one column.
    Max {
        /// Field name to maximize.
        field: String,
    },
}

/// One aggregate result cell.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProjectedAggregateValue {
    /// Result of [`ProjectedAggregate::Count`].
    Count(u64),
    /// Result of [`ProjectedAggregate::Sum`], exact to the full `i128` range.
    Sum(i128),
    /// Result of [`ProjectedAggregate::Min`] / [`ProjectedAggregate::Max`].
    ///
    /// `None` means no row contributed to the group. That is distinct from
    /// `Some(CanonicalValue::Null)`, which is a real NULL extreme over an
    /// optional column.
    Scalar(Option<CanonicalValue>),
}

/// One aggregate group: key cells and aggregate cells.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectedAggregateGroup {
    /// Key cells aligned with the outcome's `group_key_fields`.
    ///
    /// Empty for the single group of a whole-set aggregate.
    pub keys: Vec<CanonicalValue>,
    /// Aggregate cells aligned with the outcome's `aggregates`.
    pub values: Vec<ProjectedAggregateValue>,
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
    group_by: Vec<String>,
    aggregates: Vec<ProjectedAggregate>,
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
            group_by: Vec::new(),
            aggregates: Vec::new(),
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

    /// Sets the declared result bound: the post-sort row limit.
    ///
    /// For a request with [`Self::group_by`] keys this is the maximum number of
    /// **groups** the query may return — grouped results are measured in groups
    /// rather than rows. Exceeding it is a typed rejection returning nothing,
    /// not a truncated set of groups.
    ///
    /// It has no effect on an aggregate without group keys: that result is one
    /// value per requested function however many rows were folded.
    #[must_use]
    pub fn limit(mut self, limit: Option<u32>) -> Self {
        self.limit = limit;
        self
    }

    /// Adds one aggregate function; repeat for several.
    ///
    /// A query with at least one aggregate answers with
    /// [`ProjectedQueryOutcome::ReadyAggregates`] instead of rows, and the
    /// requested [`ProjectedResponseEncoding`] no longer applies.
    ///
    /// Several functions require [`Self::group_by`] keys: the server computes
    /// exactly one whole-set function per query.
    #[must_use]
    pub fn aggregate(mut self, aggregate: ProjectedAggregate) -> Self {
        self.aggregates.push(aggregate);
        self
    }

    /// Sets the group-by key field names (empty = whole-set aggregate).
    #[must_use]
    pub fn group_by(mut self, keys: Vec<String>) -> Self {
        self.group_by = keys;
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

    /// The response arm this request must be answered by.
    ///
    /// Group keys alone make a request aggregate-shaped: a body with keys and
    /// no functions is the distinct-values query, which the server answers
    /// with `ready_aggregates` carrying key-only groups.
    fn response_shape(&self) -> ProjectedResponseShape {
        if self.aggregates.is_empty() && self.group_by.is_empty() {
            ProjectedResponseShape::Rows(self.encoding)
        } else {
            ProjectedResponseShape::Aggregates(ProjectedAggregateShape {
                group_by: self.group_by.clone(),
                aggregates: self.aggregates.clone(),
            })
        }
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
        for name in &self.group_by {
            if name.is_empty() || name.len() > 256 {
                return Err(ApplicationClientError::InvalidInput);
            }
        }
        // The server computes one whole-set function per query; refuse locally
        // rather than spend a round trip on a shape it must reject.
        if self.group_by.is_empty() && self.aggregates.len() > 1 {
            return Err(ApplicationClientError::InvalidInput);
        }
        let aggregates = self
            .aggregates
            .into_iter()
            .map(lower_aggregate)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(app_v1::ExecuteProjectedQueryRequest {
            contract: lower_contract_selector(self.contract),
            projection_name: self.projection_name,
            request: Some(app_v1::ProjectedQueryBody {
                select: self.select,
                org_scope: Some(lower_value(self.org_scope)?),
                predicates,
                order,
                limit: self.limit,
                group_by: self.group_by,
                aggregates,
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
    /// Published snapshot answering an aggregate or grouped request.
    ///
    /// Answers every request carrying at least one
    /// [`ProjectedAggregate`], grouped or not; a whole-set aggregate arrives
    /// as exactly one group with an empty `keys` list. The requested
    /// [`ProjectedResponseEncoding`] does not apply to this shape.
    ///
    /// `groups` is in the server's deterministic order — ascending byte order
    /// of the encoded group keys, which is not collation order of the key
    /// values. Sort client-side if a presentation order is needed.
    ReadyAggregates {
        /// Group key field names; column order for every group's `keys`.
        group_key_fields: Vec<String>,
        /// The request's aggregate functions echoed back; column order for
        /// every group's `values`.
        aggregates: Vec<ProjectedAggregate>,
        /// Groups in server order.
        groups: Vec<ProjectedAggregateGroup>,
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
        let shape = query.response_shape();
        let request = query.into_wire_request()?;
        let response = self
            .execute_projected_query_raw(request, metadata)
            .await
            .map_err(ApplicationClientError::from)?;
        raise_projected_outcome(response, shape)
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

/// Raises one wire response exactly as [`RiffDbClient::execute_projected_query`]
/// would, given the query that produced it.
///
/// Exposed so a server-side test can drive real encoder output through the real
/// client decoder in one process — the only place the two halves of the
/// carriage meet without a live daemon. Not part of the supported surface: use
/// the client method, which does this for you.
#[doc(hidden)]
pub fn raise_projected_response_for_query(
    response: app_v1::ExecuteProjectedQueryResponse,
    query: &ProjectedQuery,
) -> Result<ProjectedQueryOutcome, ApplicationClientError> {
    raise_projected_outcome(response, query.response_shape())
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

fn lower_aggregate(
    aggregate: ProjectedAggregate,
) -> Result<app_v1::ProjectedAggregate, ApplicationClientError> {
    let (op, field) = match aggregate {
        ProjectedAggregate::Count => (app_v1::ProjectedAggregateOp::Count, String::new()),
        ProjectedAggregate::Sum { field } => (app_v1::ProjectedAggregateOp::Sum, field),
        ProjectedAggregate::Min { field } => (app_v1::ProjectedAggregateOp::Min, field),
        ProjectedAggregate::Max { field } => (app_v1::ProjectedAggregateOp::Max, field),
    };
    if op != app_v1::ProjectedAggregateOp::Count && (field.is_empty() || field.len() > 256) {
        return Err(ApplicationClientError::InvalidInput);
    }
    Ok(app_v1::ProjectedAggregate {
        field,
        op: op as i32,
    })
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

/// The aggregate contract one request asked for.
///
/// Carried into decoding so the response is checked against the *request*, not
/// only against itself. Without it a server can return a self-consistent
/// answer to a different question — a `count` echoed as a `count` where a
/// `sum` was asked for reads as a plausible number for the wrong column.
#[derive(Clone, Debug, Eq, PartialEq)]
struct ProjectedAggregateShape {
    /// Requested group-by key names, in request order. Empty = whole-set.
    group_by: Vec<String>,
    /// Requested aggregate functions, in request order.
    aggregates: Vec<ProjectedAggregate>,
}

/// The single Ready arm one request may legitimately be answered by.
#[derive(Clone, Debug, Eq, PartialEq)]
enum ProjectedResponseShape {
    /// Row request; the encoding chooses between `ready` and `ready_packed`.
    Rows(ProjectedResponseEncoding),
    /// Aggregate or grouped request; only `ready_aggregates` is valid.
    Aggregates(ProjectedAggregateShape),
}

fn raise_projected_outcome(
    response: app_v1::ExecuteProjectedQueryResponse,
    shape: ProjectedResponseShape,
) -> Result<ProjectedQueryOutcome, ApplicationClientError> {
    match response.outcome {
        Some(app_v1::execute_projected_query_response::Outcome::Ready(ready)) => {
            if shape != ProjectedResponseShape::Rows(ProjectedResponseEncoding::Row) {
                // Falsifiability (c): PACKED requests must not surface as Ready
                // row, and an aggregate request must never surface as rows.
                return Err(ApplicationClientError::InvalidResponse);
            }
            raise_ready_row(ready)
        }
        Some(app_v1::execute_projected_query_response::Outcome::ReadyPacked(packed)) => {
            if shape != ProjectedResponseShape::Rows(ProjectedResponseEncoding::Packed) {
                // The packed arm is present only when the request opted into
                // PACKED; the opposite substitution is a protocol violation.
                return Err(ApplicationClientError::InvalidResponse);
            }
            raise_ready_packed(packed)
        }
        Some(app_v1::execute_projected_query_response::Outcome::ReadyAggregates(aggregates)) => {
            let ProjectedResponseShape::Aggregates(requested) = shape else {
                // A row request answered with folded values would silently
                // report a different question's answer.
                return Err(ApplicationClientError::InvalidResponse);
            };
            raise_ready_aggregates(aggregates, &requested)
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

/// Aggregate decoder, with the same fail-closed discipline as the packed one.
///
/// Nothing about a group is self-describing on its own: keys are positional
/// against `group_key_fields` and values are positional against `aggregates`.
/// Every length and every arm is therefore checked before a value is handed
/// back, so a truncated, padded, or mislabelled group is rejected rather than
/// silently re-interpreted as a different column.
///
/// The checks that matter most are against `requested`, not against the
/// response itself. A response is free to be internally consistent and still
/// answer a different question: echoing `count` where `sum` was asked for
/// yields a plausible number for the wrong column, and self-consistency cannot
/// detect it. Four request-anchored invariants close that:
///
/// * the echoed group-key names equal the requested keys, in order
/// * the echoed descriptors equal the requested functions, in order
/// * a whole-set request (no requested keys) answers with exactly one group,
///   whose key list is empty — the wire contract admits no other shape
/// * groups are strictly ascending in encoded-group-key order, which both
///   enforces the documented emission order and makes a repeated key — which
///   would double-count in any caller folding groups into a map or a total —
///   impossible
fn raise_ready_aggregates(
    aggregates: app_v1::ProjectedReadyAggregates,
    requested: &ProjectedAggregateShape,
) -> Result<ProjectedQueryOutcome, ApplicationClientError> {
    if aggregates.group_key_fields != requested.group_by {
        return Err(ApplicationClientError::InvalidResponse);
    }
    let descriptors = aggregates
        .aggregates
        .into_iter()
        .map(raise_aggregate_descriptor)
        .collect::<Result<Vec<_>, _>>()?;
    if descriptors != requested.aggregates {
        return Err(ApplicationClientError::InvalidResponse);
    }
    // A whole-set fold has exactly one answer. Zero groups would read as "no
    // result" for a COUNT that must report a number, and several would be
    // several conflicting answers to one question.
    if requested.group_by.is_empty() && aggregates.groups.len() != 1 {
        return Err(ApplicationClientError::InvalidResponse);
    }
    let mut groups = Vec::with_capacity(aggregates.groups.len());
    let mut previous_key: Option<Vec<u8>> = None;
    for group in aggregates.groups {
        if group.keys.len() != aggregates.group_key_fields.len() {
            return Err(ApplicationClientError::InvalidResponse);
        }
        if group.values.len() != descriptors.len() {
            return Err(ApplicationClientError::InvalidResponse);
        }
        let mut keys = Vec::with_capacity(group.keys.len());
        for key in group.keys {
            keys.push(
                canonical_value_from_proto(key)
                    .map_err(|_| ApplicationClientError::InvalidResponse)?,
            );
        }
        let encoded = encoded_group_key(&keys)?;
        if let Some(previous) = &previous_key
            && *previous >= encoded
        {
            return Err(ApplicationClientError::InvalidResponse);
        }
        previous_key = Some(encoded);
        let mut values = Vec::with_capacity(group.values.len());
        for (value, descriptor) in group.values.into_iter().zip(&descriptors) {
            values.push(raise_aggregate_value(value, descriptor)?);
        }
        groups.push(ProjectedAggregateGroup { keys, values });
    }
    Ok(ProjectedQueryOutcome::ReadyAggregates {
        group_key_fields: aggregates.group_key_fields,
        aggregates: descriptors,
        groups,
        frontier: raise_frontier(aggregates.frontier)?,
        head: raise_frontier(aggregates.head)?,
        commit_token: raise_optional_token(aggregates.commit_token)?,
    })
}

/// The server's group-key ordering key: per cell, a big-endian u32 length
/// followed by the canonical cell encoding.
///
/// This mirrors the framing the server orders groups by. It is reproduced here
/// rather than shared because it is a *wire contract* the decoder verifies, not
/// an implementation the client borrows: if the two ever diverge, the check
/// must fail, which is exactly what a duplicated definition gives.
fn encoded_group_key(keys: &[CanonicalValue]) -> Result<Vec<u8>, ApplicationClientError> {
    let mut encoded = Vec::new();
    for key in keys {
        let cell = riffdb_types::encode_canonical_value(key)
            .map_err(|_| ApplicationClientError::InvalidResponse)?;
        let length =
            u32::try_from(cell.len()).map_err(|_| ApplicationClientError::InvalidResponse)?;
        encoded.extend_from_slice(&length.to_be_bytes());
        encoded.extend_from_slice(&cell);
    }
    Ok(encoded)
}

fn raise_aggregate_descriptor(
    descriptor: app_v1::ProjectedAggregate,
) -> Result<ProjectedAggregate, ApplicationClientError> {
    let op = app_v1::ProjectedAggregateOp::try_from(descriptor.op)
        .map_err(|_| ApplicationClientError::InvalidResponse)?;
    match op {
        app_v1::ProjectedAggregateOp::Count => {
            if descriptor.field.is_empty() {
                Ok(ProjectedAggregate::Count)
            } else {
                Err(ApplicationClientError::InvalidResponse)
            }
        }
        app_v1::ProjectedAggregateOp::Sum => Ok(ProjectedAggregate::Sum {
            field: named_column(descriptor.field)?,
        }),
        app_v1::ProjectedAggregateOp::Min => Ok(ProjectedAggregate::Min {
            field: named_column(descriptor.field)?,
        }),
        app_v1::ProjectedAggregateOp::Max => Ok(ProjectedAggregate::Max {
            field: named_column(descriptor.field)?,
        }),
        app_v1::ProjectedAggregateOp::Unspecified => Err(ApplicationClientError::InvalidResponse),
    }
}

fn named_column(field: String) -> Result<String, ApplicationClientError> {
    if field.is_empty() {
        return Err(ApplicationClientError::InvalidResponse);
    }
    Ok(field)
}

/// Raises one aggregate cell, requiring its arm to match its descriptor.
///
/// Without the cross-check a server could return a count where a sum was asked
/// for and the caller would read a plausible number for the wrong question.
fn raise_aggregate_value(
    value: app_v1::ProjectedAggregateValue,
    descriptor: &ProjectedAggregate,
) -> Result<ProjectedAggregateValue, ApplicationClientError> {
    use app_v1::projected_aggregate_value::Value as WireValue;
    let arm = value.value.ok_or(ApplicationClientError::InvalidResponse)?;
    match (arm, descriptor) {
        (WireValue::Count(count), ProjectedAggregate::Count) => {
            Ok(ProjectedAggregateValue::Count(count))
        }
        (WireValue::Sum(decimal), ProjectedAggregate::Sum { .. }) => {
            // Scale 0, no asserted precision, minimal 1..=16 byte coefficient.
            let sum = aggregate_sum_from_proto(&decimal)
                .map_err(|_| ApplicationClientError::InvalidResponse)?;
            Ok(ProjectedAggregateValue::Sum(sum))
        }
        (
            WireValue::Scalar(scalar),
            ProjectedAggregate::Min { .. } | ProjectedAggregate::Max { .. },
        ) => {
            // Absent = empty group; present null_value = a real NULL extreme.
            let value = scalar
                .value
                .map(canonical_value_from_proto)
                .transpose()
                .map_err(|_| ApplicationClientError::InvalidResponse)?;
            Ok(ProjectedAggregateValue::Scalar(value))
        }
        _ => Err(ApplicationClientError::InvalidResponse),
    }
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
        let err = raise_projected_outcome(
            ready,
            ProjectedResponseShape::Rows(ProjectedResponseEncoding::Packed),
        )
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
        let err = raise_projected_outcome(
            packed,
            ProjectedResponseShape::Rows(ProjectedResponseEncoding::Row),
        )
        .expect_err("packed arm under row must fail");
        assert!(matches!(err, ApplicationClientError::InvalidResponse));

        // The same payload remains acceptable under a PACKED request.
        let packed = app_v1::ExecuteProjectedQueryResponse {
            outcome: Some(
                app_v1::execute_projected_query_response::Outcome::ReadyPacked(valid_packed()),
            ),
        };
        assert!(
            raise_projected_outcome(
                packed,
                ProjectedResponseShape::Rows(ProjectedResponseEncoding::Packed)
            )
            .is_ok()
        );
    }
}

#[cfg(test)]
mod aggregate_tests {
    use super::*;
    use riffdb_proto::aggregate_sum_to_proto;
    use riffdb_types::CommitSequence;

    fn frontier_bytes(sequence: u64) -> Vec<u8> {
        ProjectionFrontier::new(
            1,
            riffdb_types::FrontierPosition::AppliedThrough(
                CommitSequence::new(sequence).expect("seq"),
            ),
        )
        .into_bytes()
    }

    fn descriptor(op: app_v1::ProjectedAggregateOp, field: &str) -> app_v1::ProjectedAggregate {
        app_v1::ProjectedAggregate {
            op: op as i32,
            field: field.to_owned(),
        }
    }

    fn count_cell(count: u64) -> app_v1::ProjectedAggregateValue {
        app_v1::ProjectedAggregateValue {
            value: Some(app_v1::projected_aggregate_value::Value::Count(count)),
        }
    }

    fn sum_cell(sum: i128) -> app_v1::ProjectedAggregateValue {
        app_v1::ProjectedAggregateValue {
            value: Some(app_v1::projected_aggregate_value::Value::Sum(
                aggregate_sum_to_proto(sum),
            )),
        }
    }

    fn scalar_cell(value: Option<CanonicalValue>) -> app_v1::ProjectedAggregateValue {
        app_v1::ProjectedAggregateValue {
            value: Some(app_v1::projected_aggregate_value::Value::Scalar(
                app_v1::ProjectedAggregateScalar {
                    value: value
                        .as_ref()
                        .map(|value| riffdb_proto::canonical_value_to_proto(value).expect("value")),
                },
            )),
        }
    }

    /// One grouped payload: keys = [status], aggregates = [count, sum, min].
    fn valid_aggregates() -> app_v1::ProjectedReadyAggregates {
        app_v1::ProjectedReadyAggregates {
            group_key_fields: vec!["status".to_owned()],
            aggregates: vec![
                descriptor(app_v1::ProjectedAggregateOp::Count, ""),
                descriptor(app_v1::ProjectedAggregateOp::Sum, "story_points"),
                descriptor(app_v1::ProjectedAggregateOp::Min, "title"),
            ],
            groups: vec![
                app_v1::ProjectedAggregateGroup {
                    keys: vec![
                        riffdb_proto::canonical_value_to_proto(
                            &CanonicalValue::string("open").expect("s"),
                        )
                        .expect("value"),
                    ],
                    values: vec![
                        count_cell(2),
                        sum_cell(11),
                        scalar_cell(Some(CanonicalValue::string("a").expect("s"))),
                    ],
                },
                app_v1::ProjectedAggregateGroup {
                    keys: vec![
                        riffdb_proto::canonical_value_to_proto(
                            &CanonicalValue::string("closed").expect("s"),
                        )
                        .expect("value"),
                    ],
                    values: vec![count_cell(0), sum_cell(0), scalar_cell(None)],
                },
            ],
            frontier: frontier_bytes(7),
            head: frontier_bytes(9),
            commit_token: CommitToken::new(1, CommitSequence::new(7).expect("seq")).into_bytes(),
        }
    }

    /// The request contract `valid_aggregates()` is a valid answer to.
    fn requested() -> ProjectedAggregateShape {
        ProjectedAggregateShape {
            group_by: vec!["status".to_owned()],
            aggregates: vec![
                ProjectedAggregate::Count,
                ProjectedAggregate::Sum {
                    field: "story_points".to_owned(),
                },
                ProjectedAggregate::Min {
                    field: "title".to_owned(),
                },
            ],
        }
    }

    /// A whole-set request for a single COUNT.
    fn requested_whole_set() -> ProjectedAggregateShape {
        ProjectedAggregateShape {
            group_by: Vec::new(),
            aggregates: vec![ProjectedAggregate::Count],
        }
    }

    /// One valid whole-set answer: exactly one group with an empty key list.
    fn valid_whole_set() -> app_v1::ProjectedReadyAggregates {
        app_v1::ProjectedReadyAggregates {
            group_key_fields: Vec::new(),
            aggregates: vec![descriptor(app_v1::ProjectedAggregateOp::Count, "")],
            groups: vec![app_v1::ProjectedAggregateGroup {
                keys: Vec::new(),
                values: vec![count_cell(6)],
            }],
            frontier: frontier_bytes(7),
            head: frontier_bytes(9),
            commit_token: Vec::new(),
        }
    }

    fn aggregates_response(
        payload: app_v1::ProjectedReadyAggregates,
    ) -> app_v1::ExecuteProjectedQueryResponse {
        app_v1::ExecuteProjectedQueryResponse {
            outcome: Some(
                app_v1::execute_projected_query_response::Outcome::ReadyAggregates(payload),
            ),
        }
    }

    #[test]
    fn aggregate_decoder_accepts_baseline_payload_in_server_order() {
        let outcome = raise_ready_aggregates(valid_aggregates(), &requested()).expect("valid");
        let ProjectedQueryOutcome::ReadyAggregates {
            group_key_fields,
            aggregates,
            groups,
            commit_token,
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
                },
                ProjectedAggregate::Min {
                    field: "title".to_owned()
                },
            ]
        );
        assert_eq!(groups.len(), 2);
        // Server order is preserved verbatim; "open" precedes "closed" here
        // even though collation order is the reverse.
        assert_eq!(
            groups[0].keys,
            vec![CanonicalValue::string("open").expect("s")]
        );
        assert_eq!(groups[0].values[0], ProjectedAggregateValue::Count(2));
        assert_eq!(groups[0].values[1], ProjectedAggregateValue::Sum(11));
        assert_eq!(
            groups[0].values[2],
            ProjectedAggregateValue::Scalar(Some(CanonicalValue::string("a").expect("s")))
        );
        assert_eq!(groups[1].values[2], ProjectedAggregateValue::Scalar(None));
        assert!(commit_token.is_some());
    }

    /// Positional alignment is the only thing that makes a group readable, so
    /// every length disagreement must fail closed rather than truncate.
    #[test]
    fn aggregate_decoder_rejects_length_disagreements() {
        // One value too few for the descriptor list.
        let mut short_values = valid_aggregates();
        short_values.groups[0].values.pop();
        assert!(
            raise_ready_aggregates(short_values, &requested()).is_err(),
            "a group with fewer values than descriptors must fail closed"
        );

        // One value too many.
        let mut long_values = valid_aggregates();
        long_values.groups[1].values.push(count_cell(9));
        assert!(raise_ready_aggregates(long_values, &requested()).is_err());

        // Key cells that do not cover the key-name list.
        let mut short_keys = valid_aggregates();
        short_keys.groups[0].keys.clear();
        assert!(
            raise_ready_aggregates(short_keys, &requested()).is_err(),
            "a group missing a key cell must fail closed"
        );

        // More key cells than names.
        let mut long_keys = valid_aggregates();
        long_keys.groups[1]
            .keys
            .push(riffdb_proto::canonical_value_to_proto(&CanonicalValue::U64(1)).expect("value"));
        assert!(raise_ready_aggregates(long_keys, &requested()).is_err());
    }

    /// A value arm that disagrees with its descriptor would report a plausible
    /// number for a different question.
    #[test]
    fn aggregate_decoder_rejects_arm_descriptor_mismatch() {
        let mut swapped = valid_aggregates();
        swapped.groups[0].values[0] = sum_cell(2);
        assert!(
            raise_ready_aggregates(swapped, &requested()).is_err(),
            "a sum under a count descriptor must fail closed"
        );

        let mut scalar_for_sum = valid_aggregates();
        scalar_for_sum.groups[0].values[1] = scalar_cell(Some(CanonicalValue::U64(1)));
        assert!(raise_ready_aggregates(scalar_for_sum, &requested()).is_err());

        let mut count_for_min = valid_aggregates();
        count_for_min.groups[0].values[2] = count_cell(1);
        assert!(raise_ready_aggregates(count_for_min, &requested()).is_err());

        // An unset value oneof is not a value.
        let mut unset = valid_aggregates();
        unset.groups[0].values[0] = app_v1::ProjectedAggregateValue { value: None };
        assert!(raise_ready_aggregates(unset, &requested()).is_err());
    }

    #[test]
    fn aggregate_decoder_rejects_malformed_descriptors() {
        let mut unknown_op = valid_aggregates();
        unknown_op.aggregates[0].op = 99;
        assert!(
            raise_ready_aggregates(unknown_op, &requested()).is_err(),
            "an unknown op enum must fail closed"
        );

        let mut unspecified = valid_aggregates();
        unspecified.aggregates[0].op = app_v1::ProjectedAggregateOp::Unspecified as i32;
        assert!(raise_ready_aggregates(unspecified, &requested()).is_err());

        let mut counted_column = valid_aggregates();
        counted_column.aggregates[0].field = "title".to_owned();
        assert!(raise_ready_aggregates(counted_column, &requested()).is_err());

        let mut columnless_sum = valid_aggregates();
        columnless_sum.aggregates[1].field = String::new();
        assert!(raise_ready_aggregates(columnless_sum, &requested()).is_err());
    }

    /// Sum carriage is a closed scale-0 integer format, not a general decimal.
    #[test]
    fn aggregate_decoder_rejects_hostile_sum_decimals() {
        let oversized = app_v1::ProjectedAggregateValue {
            value: Some(app_v1::projected_aggregate_value::Value::Sum(
                riffdb_proto::v1::Decimal {
                    coefficient_twos_complement: vec![1; 17],
                    scale: 0,
                    precision: None,
                },
            )),
        };
        let mut payload = valid_aggregates();
        payload.groups[0].values[1] = oversized;
        assert!(
            raise_ready_aggregates(payload, &requested()).is_err(),
            "a 17-byte coefficient exceeds i128 and must fail closed"
        );

        let mut scaled = valid_aggregates();
        scaled.groups[0].values[1] = app_v1::ProjectedAggregateValue {
            value: Some(app_v1::projected_aggregate_value::Value::Sum(
                riffdb_proto::v1::Decimal {
                    coefficient_twos_complement: vec![1],
                    scale: 2,
                    precision: None,
                },
            )),
        };
        assert!(
            raise_ready_aggregates(scaled, &requested()).is_err(),
            "a non-zero scale is not this carriage"
        );

        let mut asserted_precision = valid_aggregates();
        asserted_precision.groups[0].values[1] = app_v1::ProjectedAggregateValue {
            value: Some(app_v1::projected_aggregate_value::Value::Sum(
                riffdb_proto::v1::Decimal {
                    coefficient_twos_complement: vec![1],
                    scale: 0,
                    precision: Some(38),
                },
            )),
        };
        assert!(raise_ready_aggregates(asserted_precision, &requested()).is_err());

        let mut empty_coefficient = valid_aggregates();
        empty_coefficient.groups[0].values[1] = app_v1::ProjectedAggregateValue {
            value: Some(app_v1::projected_aggregate_value::Value::Sum(
                riffdb_proto::v1::Decimal {
                    coefficient_twos_complement: Vec::new(),
                    scale: 0,
                    precision: None,
                },
            )),
        };
        assert!(raise_ready_aggregates(empty_coefficient, &requested()).is_err());

        // Non-minimal padding is a second encoding of the same number.
        let mut padded = valid_aggregates();
        padded.groups[0].values[1] = app_v1::ProjectedAggregateValue {
            value: Some(app_v1::projected_aggregate_value::Value::Sum(
                riffdb_proto::v1::Decimal {
                    coefficient_twos_complement: vec![0, 1],
                    scale: 0,
                    precision: None,
                },
            )),
        };
        assert!(raise_ready_aggregates(padded, &requested()).is_err());
    }

    /// Every i128 the engine can produce round-trips exactly.
    #[test]
    fn sum_round_trips_across_the_full_i128_range() {
        for sum in [
            0_i128,
            1,
            -1,
            i64::MAX as i128,
            i64::MIN as i128,
            i64::MAX as i128 + 1,
            i128::MAX,
            i128::MIN,
        ] {
            let mut payload = valid_aggregates();
            payload.groups[0].values[1] = sum_cell(sum);
            let ProjectedQueryOutcome::ReadyAggregates { groups, .. } =
                raise_ready_aggregates(payload, &requested()).expect("valid")
            else {
                panic!("expected ReadyAggregates");
            };
            assert_eq!(groups[0].values[1], ProjectedAggregateValue::Sum(sum));
        }
    }

    /// R3: absence and a real NULL extreme must stay distinguishable after
    /// decoding, not just on the wire.
    #[test]
    fn empty_group_scalar_is_distinct_from_a_null_extreme() {
        let mut absent = valid_aggregates();
        absent.groups[0].values[2] = scalar_cell(None);
        let mut null_extreme = valid_aggregates();
        null_extreme.groups[0].values[2] = scalar_cell(Some(CanonicalValue::Null));

        let value_of = |payload| {
            let ProjectedQueryOutcome::ReadyAggregates { groups, .. } =
                raise_ready_aggregates(payload, &requested()).expect("valid")
            else {
                panic!("expected ReadyAggregates");
            };
            groups[0].values[2].clone()
        };
        assert_eq!(value_of(absent), ProjectedAggregateValue::Scalar(None));
        assert_eq!(
            value_of(null_extreme),
            ProjectedAggregateValue::Scalar(Some(CanonicalValue::Null))
        );
    }

    /// Arm-versus-request cross-check, both directions.
    #[test]
    fn aggregate_arm_and_request_shape_must_agree() {
        for shape in [
            ProjectedResponseShape::Rows(ProjectedResponseEncoding::Row),
            ProjectedResponseShape::Rows(ProjectedResponseEncoding::Packed),
        ] {
            let err = raise_projected_outcome(aggregates_response(valid_aggregates()), shape)
                .expect_err("a row request must refuse folded values");
            assert!(matches!(err, ApplicationClientError::InvalidResponse));
        }
        assert!(
            raise_projected_outcome(
                aggregates_response(valid_aggregates()),
                ProjectedResponseShape::Aggregates(requested())
            )
            .is_ok()
        );

        // An aggregate request answered with rows, both row arms.
        let ready_row = app_v1::ExecuteProjectedQueryResponse {
            outcome: Some(app_v1::execute_projected_query_response::Outcome::Ready(
                app_v1::ProjectedQueryReady {
                    fields: Vec::new(),
                    rows: Vec::new(),
                    frontier: frontier_bytes(7),
                    head: frontier_bytes(7),
                    commit_token: Vec::new(),
                },
            )),
        };
        assert!(matches!(
            raise_projected_outcome(ready_row, ProjectedResponseShape::Aggregates(requested())),
            Err(ApplicationClientError::InvalidResponse)
        ));
        let ready_packed = app_v1::ExecuteProjectedQueryResponse {
            outcome: Some(
                app_v1::execute_projected_query_response::Outcome::ReadyPacked(
                    app_v1::ProjectedReadyPacked {
                        fields: Vec::new(),
                        primary_key_fields: Vec::new(),
                        row_count: 0,
                        columns: Vec::new(),
                        frontier: frontier_bytes(7),
                        head: frontier_bytes(7),
                        commit_token: Vec::new(),
                    },
                ),
            ),
        };
        assert!(matches!(
            raise_projected_outcome(
                ready_packed,
                ProjectedResponseShape::Aggregates(requested())
            ),
            Err(ApplicationClientError::InvalidResponse)
        ));
    }

    /// F4 — the echo is checked against the REQUEST, not only against itself.
    ///
    /// The reviewer's probe: a request for `Sum{story_points}` answered by a
    /// perfectly self-consistent `Count` of 9999. Every internal check passes;
    /// only the request comparison catches it, and without it the caller reads
    /// a plausible number for a column nobody asked about.
    #[test]
    fn a_self_consistent_answer_to_a_different_question_is_rejected() {
        let mislabelled = app_v1::ProjectedReadyAggregates {
            group_key_fields: Vec::new(),
            aggregates: vec![descriptor(app_v1::ProjectedAggregateOp::Count, "")],
            groups: vec![app_v1::ProjectedAggregateGroup {
                keys: Vec::new(),
                values: vec![count_cell(9999)],
            }],
            frontier: frontier_bytes(7),
            head: frontier_bytes(9),
            commit_token: Vec::new(),
        };
        let asked_for_a_sum = ProjectedAggregateShape {
            group_by: Vec::new(),
            aggregates: vec![ProjectedAggregate::Sum {
                field: "story_points".to_owned(),
            }],
        };
        assert!(
            raise_ready_aggregates(mislabelled.clone(), &asked_for_a_sum).is_err(),
            "a count echoed where a sum was requested must fail closed"
        );
        // The same payload is fine for the request it actually answers.
        assert!(raise_ready_aggregates(mislabelled, &requested_whole_set()).is_ok());

        // Descriptor order is response column order, so order must match too.
        let mut reordered = valid_aggregates();
        reordered.aggregates.swap(0, 1);
        for group in &mut reordered.groups {
            group.values.swap(0, 1);
        }
        assert!(
            raise_ready_aggregates(reordered, &requested()).is_err(),
            "descriptors reordered against the request must fail closed"
        );

        // Group-key names are equally load-bearing: they name the key columns.
        let mut renamed = valid_aggregates();
        renamed.group_key_fields = vec!["project_id".to_owned()];
        assert!(
            raise_ready_aggregates(renamed, &requested()).is_err(),
            "a different key column than the one requested must fail closed"
        );
    }

    /// F5 — a whole-set fold has exactly one answer.
    ///
    /// The reviewer's probe: zero groups (a COUNT arriving as "no result") and
    /// three groups (three conflicting answers to one question) were both
    /// accepted. The proto states the contract twice; the decoder now holds it.
    #[test]
    fn whole_set_requests_require_exactly_one_empty_key_group() {
        assert!(
            raise_ready_aggregates(valid_whole_set(), &requested_whole_set()).is_ok(),
            "the baseline whole-set answer must still decode"
        );

        let mut none = valid_whole_set();
        none.groups.clear();
        assert!(
            raise_ready_aggregates(none, &requested_whole_set()).is_err(),
            "a whole-set COUNT must never arrive as no result at all"
        );

        let mut several = valid_whole_set();
        several.groups = vec![
            app_v1::ProjectedAggregateGroup {
                keys: Vec::new(),
                values: vec![count_cell(1)],
            },
            app_v1::ProjectedAggregateGroup {
                keys: Vec::new(),
                values: vec![count_cell(2)],
            },
            app_v1::ProjectedAggregateGroup {
                keys: Vec::new(),
                values: vec![count_cell(3)],
            },
        ];
        assert!(
            raise_ready_aggregates(several, &requested_whole_set()).is_err(),
            "three conflicting whole-set answers must fail closed"
        );

        // A grouped request keeps its own cardinality freedom, including zero
        // groups, which is the correct answer to an empty matching set.
        let mut empty_grouped = valid_aggregates();
        empty_grouped.groups.clear();
        assert!(
            raise_ready_aggregates(empty_grouped, &requested()).is_ok(),
            "a grouped request may legitimately match nothing"
        );
    }

    /// F6 — strictly ascending encoded group keys.
    ///
    /// The reviewer's probe: `[zzz→5, aaa→7, zzz→11]` decoded fine, so a caller
    /// folding groups into a map or a total would double-count `zzz`. One
    /// strictly-ascending check catches both the repeat and the disorder.
    #[test]
    fn group_keys_must_be_strictly_ascending_in_encoded_order() {
        let group = |key: &str, count: u64| app_v1::ProjectedAggregateGroup {
            keys: vec![
                riffdb_proto::canonical_value_to_proto(
                    &CanonicalValue::string(key).expect("bounded"),
                )
                .expect("value"),
            ],
            values: vec![count_cell(count)],
        };
        let payload =
            |groups: Vec<app_v1::ProjectedAggregateGroup>| app_v1::ProjectedReadyAggregates {
                group_key_fields: vec!["title".to_owned()],
                aggregates: vec![descriptor(app_v1::ProjectedAggregateOp::Count, "")],
                groups,
                frontier: frontier_bytes(7),
                head: frontier_bytes(9),
                commit_token: Vec::new(),
            };
        let asked = ProjectedAggregateShape {
            group_by: vec!["title".to_owned()],
            aggregates: vec![ProjectedAggregate::Count],
        };

        // Baseline: same-length keys in ascending byte order.
        assert!(
            raise_ready_aggregates(payload(vec![group("aaa", 7), group("zzz", 5)]), &asked).is_ok(),
            "ascending keys must decode"
        );

        // The reviewer's probe: a repeated key with a run of disorder between.
        assert!(
            raise_ready_aggregates(
                payload(vec![group("zzz", 5), group("aaa", 7), group("zzz", 11)]),
                &asked
            )
            .is_err(),
            "a repeated group key would double-count in any caller folding groups"
        );

        // Plain descending, no duplicates.
        assert!(
            raise_ready_aggregates(payload(vec![group("zzz", 5), group("aaa", 7)]), &asked)
                .is_err(),
            "descending groups break the documented emission order"
        );

        // Adjacent exact duplicates.
        assert!(
            raise_ready_aggregates(payload(vec![group("aaa", 7), group("aaa", 7)]), &asked)
                .is_err()
        );

        // The order is the server's LENGTH-PREFIXED encoding, not the string
        // collation the key values would suggest: "zz" precedes "aaa" here.
        assert!(
            raise_ready_aggregates(payload(vec![group("zz", 1), group("aaa", 2)]), &asked).is_ok(),
            "shorter keys sort first under the length-prefixed encoding"
        );
        assert!(
            raise_ready_aggregates(payload(vec![group("aaa", 2), group("zz", 1)]), &asked).is_err(),
            "collation order is not the wire order"
        );
    }

    // ---------------------------------------------------------------
    // Request building
    // ---------------------------------------------------------------

    fn query() -> ProjectedQuery {
        ProjectedQuery::new(
            ApplicationContract::Exact {
                lineage: "TicketDesk".to_owned(),
                version: 1,
                bundle_hash: None,
            },
            "board",
            ApplicationValue::Uuid(crate::ApplicationUuid::from_bytes([9; 16])),
        )
        .expect("query")
    }

    #[test]
    fn aggregate_builder_lowers_every_op_and_keeps_request_order() {
        let wire = query()
            .group_by(vec!["status".to_owned()])
            .aggregate(ProjectedAggregate::Count)
            .aggregate(ProjectedAggregate::Sum {
                field: "story_points".to_owned(),
            })
            .aggregate(ProjectedAggregate::Min {
                field: "title".to_owned(),
            })
            .aggregate(ProjectedAggregate::Max {
                field: "title".to_owned(),
            })
            .into_wire_request()
            .expect("wire");
        let body = wire.request.expect("body");
        assert_eq!(body.group_by, vec!["status".to_owned()]);
        assert_eq!(
            body.aggregates
                .iter()
                .map(|item| (item.op, item.field.clone()))
                .collect::<Vec<_>>(),
            vec![
                (app_v1::ProjectedAggregateOp::Count as i32, String::new()),
                (
                    app_v1::ProjectedAggregateOp::Sum as i32,
                    "story_points".to_owned()
                ),
                (app_v1::ProjectedAggregateOp::Min as i32, "title".to_owned()),
                (app_v1::ProjectedAggregateOp::Max as i32, "title".to_owned()),
            ]
        );
    }

    /// F2 — group keys alone make a request aggregate-shaped.
    ///
    /// The distinct-values query (`group_by` with no functions) is an
    /// explicitly supported body: the proto documents it, the conversion
    /// layer lowers it, and the engine answers it with `ready_aggregates`.
    /// Deriving the expected arm from the function list alone made the SDK
    /// build a valid request and then reject the server's correct answer.
    #[test]
    fn group_keys_without_functions_expect_the_aggregate_arm() {
        let distinct = query().group_by(vec!["status".to_owned()]);
        assert_eq!(
            distinct.clone().response_shape(),
            ProjectedResponseShape::Aggregates(ProjectedAggregateShape {
                group_by: vec!["status".to_owned()],
                aggregates: Vec::new(),
            }),
            "a grouped request must not declare itself row-shaped"
        );

        // The request really is the shape the server will treat as grouped.
        let body = distinct
            .clone()
            .into_wire_request()
            .expect("wire")
            .request
            .expect("body");
        assert_eq!(body.group_by, vec!["status".to_owned()]);
        assert!(body.aggregates.is_empty());

        // And the server's key-only answer decodes rather than being refused.
        let key_only = app_v1::ProjectedReadyAggregates {
            group_key_fields: vec!["status".to_owned()],
            aggregates: Vec::new(),
            groups: vec![
                app_v1::ProjectedAggregateGroup {
                    keys: vec![
                        riffdb_proto::canonical_value_to_proto(
                            &CanonicalValue::string("closed").expect("bounded"),
                        )
                        .expect("value"),
                    ],
                    values: Vec::new(),
                },
                app_v1::ProjectedAggregateGroup {
                    keys: vec![
                        riffdb_proto::canonical_value_to_proto(
                            &CanonicalValue::string("open___").expect("bounded"),
                        )
                        .expect("value"),
                    ],
                    values: Vec::new(),
                },
            ],
            frontier: frontier_bytes(7),
            head: frontier_bytes(9),
            commit_token: Vec::new(),
        };
        let outcome = raise_projected_response_for_query(aggregates_response(key_only), &distinct)
            .expect("a grouped request must accept the grouped answer it asked for");
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
        assert!(aggregates.is_empty(), "no functions were requested");
        assert_eq!(groups.len(), 2);
        assert!(groups.iter().all(|group| group.values.is_empty()));
    }

    #[test]
    fn row_requests_still_carry_no_aggregate_vocabulary() {
        let body = query().into_wire_request().expect("wire").request;
        let body = body.expect("body");
        assert!(body.aggregates.is_empty());
        assert!(body.group_by.is_empty());
    }

    /// Refused locally rather than spending a round trip: the server computes
    /// exactly one whole-set function per query.
    #[test]
    fn multiple_aggregates_without_group_keys_are_refused_locally() {
        let err = query()
            .aggregate(ProjectedAggregate::Count)
            .aggregate(ProjectedAggregate::Sum {
                field: "story_points".to_owned(),
            })
            .into_wire_request()
            .expect_err("two whole-set functions have no server path");
        assert!(matches!(err, ApplicationClientError::InvalidInput));

        // One is fine, and so are two once a key is present.
        assert!(
            query()
                .aggregate(ProjectedAggregate::Count)
                .into_wire_request()
                .is_ok()
        );
        assert!(
            query()
                .group_by(vec!["status".to_owned()])
                .aggregate(ProjectedAggregate::Count)
                .aggregate(ProjectedAggregate::Sum {
                    field: "story_points".to_owned(),
                })
                .into_wire_request()
                .is_ok()
        );
    }

    #[test]
    fn unbounded_names_are_refused_before_the_wire() {
        let long = "x".repeat(257);
        assert!(matches!(
            query()
                .aggregate(ProjectedAggregate::Sum {
                    field: long.clone()
                })
                .into_wire_request(),
            Err(ApplicationClientError::InvalidInput)
        ));
        assert!(matches!(
            query()
                .group_by(vec![long])
                .aggregate(ProjectedAggregate::Count)
                .into_wire_request(),
            Err(ApplicationClientError::InvalidInput)
        ));
        assert!(matches!(
            query()
                .group_by(vec![String::new()])
                .aggregate(ProjectedAggregate::Count)
                .into_wire_request(),
            Err(ApplicationClientError::InvalidInput)
        ));
    }

    /// An aggregate request must expect the aggregate arm even when the caller
    /// also asked for PACKED: encoding does not apply to folded results.
    #[test]
    fn aggregate_requests_expect_the_aggregate_arm_under_any_encoding() {
        for encoding in [
            ProjectedResponseEncoding::Row,
            ProjectedResponseEncoding::Packed,
        ] {
            let query = query()
                .encoding(encoding)
                .aggregate(ProjectedAggregate::Count);
            assert_eq!(
                query.response_shape(),
                ProjectedResponseShape::Aggregates(ProjectedAggregateShape {
                    group_by: Vec::new(),
                    aggregates: vec![ProjectedAggregate::Count],
                })
            );
        }
        assert_eq!(
            query().response_shape(),
            ProjectedResponseShape::Rows(ProjectedResponseEncoding::Row)
        );
    }
}
