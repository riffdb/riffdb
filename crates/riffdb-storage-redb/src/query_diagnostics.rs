//! Retired storage-owned query-executor census.
//!
//! WP-754 moved execution above storage. Returning no samples is deliberate:
//! publishing zero-filled legacy stages for executor work would be false
//! evidence. A later executor-owned schema must use a new version.

static QUERY_EXECUTE_DIAGNOSTICS: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

#[cfg(test)]
thread_local! {
    /// Environment lookups for the diagnostic flag made on this thread, so a
    /// test can hold that the flag is read from the environment at most once.
    static ENVIRONMENT_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Reports whether the legacy opt-in diagnostic mode is enabled.
///
/// Non-query storage reads retain their existing sub-stage instrumentation
/// while the retired query-executor census itself publishes no samples.
///
/// The environment is read once per process. Every journal and checkpoint
/// point read asks, and `getenv` takes the process-wide environment lock, so
/// an uncached lookup cost every writer and idempotency read a lock and a
/// scan of the environment. The variable is set when the daemon is launched
/// and never changed while it runs.
pub(crate) fn query_execute_diagnostics_enabled() -> bool {
    *QUERY_EXECUTE_DIAGNOSTICS.get_or_init(|| {
        #[cfg(test)]
        ENVIRONMENT_READS.with(|reads| reads.set(reads.get() + 1));
        std::env::var_os("RIFFDB_QUERY_EXECUTE_DIAGNOSTICS").is_some_and(|value| value == "1")
    })
}

pub(crate) fn query_execute_census_v1() -> crate::QueryExecuteCensusV1 {
    crate::QueryExecuteCensusV1 {
        total_count: 0,
        windows: [crate::QueryExecuteWindowV1::default(); crate::QUERY_EXECUTE_WINDOW_COUNT_V1],
    }
}

#[cfg(test)]
mod tests {
    use super::{ENVIRONMENT_READS, query_execute_diagnostics_enabled};

    #[test]
    fn the_diagnostic_flag_is_read_from_the_environment_at_most_once() {
        let before = ENVIRONMENT_READS.with(std::cell::Cell::get);
        let first = query_execute_diagnostics_enabled();
        for _ in 0..64 {
            assert_eq!(query_execute_diagnostics_enabled(), first);
        }
        let reads = ENVIRONMENT_READS.with(std::cell::Cell::get) - before;
        assert!(reads <= 1, "{reads} environment reads for 65 queries");
        assert_eq!(
            first,
            std::env::var_os("RIFFDB_QUERY_EXECUTE_DIAGNOSTICS").is_some_and(|value| value == "1")
        );
    }
}
