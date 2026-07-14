//! Dependency-isolation and explicit-guarantee tests.

#![forbid(unsafe_code)]

use riffdb_budget_comparison_core::{GuaranteeLevel, postgres_guarantee_profile};
use riffdb_budget_comparison_postgres::{POSTGRES_IMAGE, SCHEMA_SQL};

#[test]
fn guarantee_profile_names_every_deliberate_gap() {
    let profile = postgres_guarantee_profile();
    assert_eq!(profile.idempotency, GuaranteeLevel::Unsupported);
    assert_eq!(profile.durable_events, GuaranteeLevel::Unsupported);
    assert_eq!(profile.provenance, GuaranteeLevel::Unsupported);
    assert_eq!(profile.outbox, GuaranteeLevel::Unsupported);
    assert_eq!(profile.projections, GuaranteeLevel::Unsupported);
    assert_eq!(profile.authorization, GuaranteeLevel::Unsupported);
}

#[test]
fn postgres_baseline_is_structurally_isolated_from_production() {
    let root_manifest = include_str!("../../../Cargo.toml");
    let root_lock = include_str!("../../../Cargo.lock");
    assert!(!root_manifest.contains("budget-comparison"));
    assert!(!root_manifest.contains("postgres"));
    assert!(!root_lock.contains("name = \"postgres\""));

    let nested_manifest = include_str!("../Cargo.toml");
    let adapter_manifest = include_str!("../postgres/Cargo.toml");
    let core_manifest = include_str!("../core/Cargo.toml");
    assert!(nested_manifest.contains("members = [\"core\", \"postgres\"]"));
    assert!(adapter_manifest.contains("version = \"=0.19.14\""));
    assert!(adapter_manifest.contains("default-features = false"));
    assert!(core_manifest.contains("version = \"=1.0.150\""));
    assert!(core_manifest.contains("features = [\"std\"]"));
}

#[test]
fn schema_and_ci_image_freeze_concurrency_assumptions() {
    assert!(SCHEMA_SQL.contains("CHECK (allocated_amount >= 0.00)"));
    assert!(SCHEMA_SQL.contains("CHECK (allocated_amount <= approved_amount)"));
    assert_eq!(
        POSTGRES_IMAGE,
        "postgres:18.4-bookworm@sha256:d9c83446333daec3f0588cc709adb80c26090b7f9f0f7ec8d43c243385d79818"
    );
}
