//! Shared whole-query cost identities and bounds.

/// Hard maximum access steps in one compiled application query.
pub const MAX_APPLICATION_QUERY_STEPS: u64 = 64;
/// Hard maximum encoded application-query result bytes.
pub const MAX_APPLICATION_QUERY_RESULT_BYTES: u64 = 4_194_304;
/// Hard maximum physical rows inspected by one application-query access step.
///
/// One row is reserved for continuation detection, so the largest result page
/// is `MAX_APPLICATION_QUERY_PAGE_ROWS`.
pub const MAX_APPLICATION_QUERY_SCANNED_ROWS: u64 = 65_535;
/// Hard maximum rows returned by one application-query page.
pub const MAX_APPLICATION_QUERY_PAGE_ROWS: u64 = MAX_APPLICATION_QUERY_SCANNED_ROWS - 1;
/// Largest page encoded by the predecessor bounded-limit V1 identity.
pub const MAX_APPLICATION_QUERY_PAGE_ROWS_BOUNDED_LIMIT_V1: u64 = 499;
/// Existing exact-vector provider partition ceiling, independent from page rows.
pub const MAX_EXACT_VECTOR_PARTITION_ROWS_V1: u64 = 500;

/// Canonical compiler-derived maximum work for one complete query request.
///
/// This value is part of the query-plan hash input. It is deliberately a
/// vector rather than a scalar so one resource cannot hide amplification in
/// another.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryCostVectorV1 {
    access_steps: u64,
    scanned_index_rows: u64,
    point_reads: u64,
    dependent_keys: u64,
    intermediate_rows: u64,
    projected_values: u64,
    encoded_result_bytes: u64,
}

impl QueryCostVectorV1 {
    /// Constructs one complete vector. Zero is valid for dimensions a plan
    /// does not use; a compiled query must still have at least one step.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub const fn new(
        access_steps: u64,
        scanned_index_rows: u64,
        point_reads: u64,
        dependent_keys: u64,
        intermediate_rows: u64,
        projected_values: u64,
        encoded_result_bytes: u64,
    ) -> Option<Self> {
        if access_steps == 0
            || access_steps > MAX_APPLICATION_QUERY_STEPS
            || encoded_result_bytes > MAX_APPLICATION_QUERY_RESULT_BYTES
        {
            return None;
        }
        Some(Self {
            access_steps,
            scanned_index_rows,
            point_reads,
            dependent_keys,
            intermediate_rows,
            projected_values,
            encoded_result_bytes,
        })
    }

    /// Constructs a zero vector for execution accounting and tests.
    #[must_use]
    pub const fn zero() -> Self {
        Self {
            access_steps: 0,
            scanned_index_rows: 0,
            point_reads: 0,
            dependent_keys: 0,
            intermediate_rows: 0,
            projected_values: 0,
            encoded_result_bytes: 0,
        }
    }

    /// Maximum access steps.
    #[must_use]
    pub const fn access_steps(self) -> u64 {
        self.access_steps
    }

    /// Maximum physically inspected index rows.
    #[must_use]
    pub const fn scanned_index_rows(self) -> u64 {
        self.scanned_index_rows
    }

    /// Maximum authoritative entity point reads, including index hydration.
    #[must_use]
    pub const fn point_reads(self) -> u64 {
        self.point_reads
    }

    /// Maximum keys produced for dependent batches.
    #[must_use]
    pub const fn dependent_keys(self) -> u64 {
        self.dependent_keys
    }

    /// Maximum rows retained between access steps.
    #[must_use]
    pub const fn intermediate_rows(self) -> u64 {
        self.intermediate_rows
    }

    /// Maximum field values shaped into the declared result.
    #[must_use]
    pub const fn projected_values(self) -> u64 {
        self.projected_values
    }

    /// Maximum encoded result bytes.
    #[must_use]
    pub const fn encoded_result_bytes(self) -> u64 {
        self.encoded_result_bytes
    }

    /// Whether every requested dimension fits this complete budget.
    #[must_use]
    pub const fn covers(self, requested: Self) -> bool {
        requested.access_steps <= self.access_steps
            && requested.scanned_index_rows <= self.scanned_index_rows
            && requested.point_reads <= self.point_reads
            && requested.dependent_keys <= self.dependent_keys
            && requested.intermediate_rows <= self.intermediate_rows
            && requested.projected_values <= self.projected_values
            && requested.encoded_result_bytes <= self.encoded_result_bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_vector_coverage_rejects_one_over_in_every_dimension() {
        let budget = QueryCostVectorV1::new(7, 11, 13, 17, 19, 23, 29).expect("budget");
        assert!(budget.covers(budget));
        let candidates = [
            QueryCostVectorV1::new(8, 11, 13, 17, 19, 23, 29),
            QueryCostVectorV1::new(7, 12, 13, 17, 19, 23, 29),
            QueryCostVectorV1::new(7, 11, 14, 17, 19, 23, 29),
            QueryCostVectorV1::new(7, 11, 13, 18, 19, 23, 29),
            QueryCostVectorV1::new(7, 11, 13, 17, 20, 23, 29),
            QueryCostVectorV1::new(7, 11, 13, 17, 19, 24, 29),
            QueryCostVectorV1::new(7, 11, 13, 17, 19, 23, 30),
        ];
        assert!(
            candidates
                .into_iter()
                .all(|candidate| { candidate.is_none_or(|candidate| !budget.covers(candidate)) })
        );
    }

    #[test]
    fn empty_work_is_a_valid_accounting_value_but_not_a_compiled_plan() {
        assert_eq!(QueryCostVectorV1::zero().access_steps(), 0);
        assert!(QueryCostVectorV1::new(0, 0, 0, 0, 0, 0, 0).is_none());
        assert!(
            QueryCostVectorV1::new(1, 0, 0, 0, 0, 0, MAX_APPLICATION_QUERY_RESULT_BYTES).is_some()
        );
        assert!(
            QueryCostVectorV1::new(1, 0, 0, 0, 0, 0, MAX_APPLICATION_QUERY_RESULT_BYTES + 1,)
                .is_none()
        );
    }

    #[test]
    fn page_rows_reserve_exactly_one_continuation_probe() {
        assert_eq!(MAX_APPLICATION_QUERY_SCANNED_ROWS, 65_535);
        assert_eq!(MAX_APPLICATION_QUERY_PAGE_ROWS, 65_534);
        assert_eq!(MAX_APPLICATION_QUERY_PAGE_ROWS_BOUNDED_LIMIT_V1, 499);
        assert_eq!(MAX_EXACT_VECTOR_PARTITION_ROWS_V1, 500);
    }
}
