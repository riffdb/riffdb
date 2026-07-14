//! Live PostgreSQL correctness preflight.

#![forbid(unsafe_code)]

use riffdb_budget_comparison_core::{
    canonical_workload, evaluate_sequential, expected_contention_observation, verify_contention,
    verify_sequential,
};
use riffdb_budget_comparison_postgres::{PostgresBudgetAdapter, live_database_url};

#[test]
fn postgres_live_adapter_matches_sequential_and_contention_oracles() {
    let Some(database_url) = live_database_url().expect("live-test environment is valid") else {
        eprintln!(
            "PostgreSQL live test skipped; set RIFFDB_BUDGET_POSTGRES_URL, or set \
             RIFFDB_BUDGET_POSTGRES_REQUIRED=1 to make absence fail closed"
        );
        return;
    };

    let adapter = PostgresBudgetAdapter::new(database_url).expect("database URL is bounded");
    let workload = canonical_workload();

    adapter.reset_schema().expect("schema reset succeeds");
    let actual = adapter
        .run_sequential(&workload.sequential)
        .expect("sequential PostgreSQL workload succeeds");
    let expected = evaluate_sequential(&workload.sequential).expect("reference model succeeds");
    verify_sequential(&expected, &actual).expect("sequential observations match");

    adapter.reset_schema().expect("schema reset succeeds");
    let actual_contention = adapter
        .run_contention(&workload.contention)
        .expect("barrier contention workload succeeds");
    let expected_contention = expected_contention_observation(&workload.contention)
        .expect("contention reference model succeeds");
    verify_contention(&expected_contention, &actual_contention)
        .expect("contention observation matches");

    let settings = adapter
        .probe_transaction_settings()
        .expect("transaction settings are readable");
    assert_eq!(settings.isolation, "read committed");
    assert_eq!(settings.synchronous_commit, "on");
    assert_eq!(settings.server_version_num, "180004");
    assert_eq!(settings.fsync, "on");
    assert_eq!(settings.full_page_writes, "on");
    assert_eq!(settings.lock_timeout, "5s");
    assert_eq!(settings.statement_timeout, "10s");
    assert_eq!(settings.idle_in_transaction_session_timeout, "10s");
}
