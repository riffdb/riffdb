#![forbid(unsafe_code)]

//! Isolated, non-production Fjall comparison for RiffDB's storage contract.
//!
//! This crate intentionally does not claim to be a RiffDB storage engine. It
//! proves selected engine-substrate behavior and publishes an explicit
//! unchanged-suite failure for semantic ports that have not been implemented.

mod adapter;
mod conformance;
mod migration_ledger;

pub use adapter::{
    AdapterError, AdapterErrorKind, ComparisonDurability, ComparisonMutation, ComparisonPage,
    ComparisonSnapshot, FjallComparisonStore, FjallMigrationIdentityProbe, RiffdbTable,
};
pub use conformance::{
    CONFORMANCE_CASES, ConformanceCase, ConformanceClass, ConformanceStatus,
    overall_conformance_status, render_conformance_report,
};
pub use migration_ledger::{
    MigrationCharge, MigrationLedgerError, MigrationLedgerPage, paginate_migration_charges,
};
