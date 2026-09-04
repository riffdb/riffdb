//! Retired storage-owned query-executor census.
//!
//! WP-754 moved execution above storage. Returning no samples is deliberate:
//! publishing zero-filled legacy stages for executor work would be false
//! evidence. A later executor-owned schema must use a new version.

/// Reports whether the legacy opt-in diagnostic mode is enabled.
///
/// Non-query storage reads retain their existing sub-stage instrumentation
/// while the retired query-executor census itself publishes no samples.
pub(crate) fn query_execute_diagnostics_enabled() -> bool {
    std::env::var_os("RIFFDB_QUERY_EXECUTE_DIAGNOSTICS").is_some_and(|value| value == "1")
}

pub(crate) fn query_execute_census_v1() -> crate::QueryExecuteCensusV1 {
    crate::QueryExecuteCensusV1 {
        total_count: 0,
        windows: [crate::QueryExecuteWindowV1::default(); crate::QUERY_EXECUTE_WINDOW_COUNT_V1],
    }
}
