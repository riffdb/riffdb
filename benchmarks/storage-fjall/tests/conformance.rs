#![forbid(unsafe_code)]

//! Frozen unchanged-suite and accepted-interface evidence.

use std::fs;
use std::path::PathBuf;

use riffdb_catalog::CatalogIndexMigrationBackend;
use riffdb_proto::durable::{
    READABLE_RECORD_SCHEMA_COUNT, READABLE_RECORD_SCHEMAS, WRITABLE_RECORD_SCHEMA_COUNT,
    WRITABLE_RECORD_SCHEMAS, readable_record_schema, writable_record_schema,
};
use riffdb_storage_api::StartupIndexMigrationPort;
use riffdb_storage_fjall_comparison::{
    CONFORMANCE_CASES, ConformanceClass, ConformanceStatus, overall_conformance_status,
    render_conformance_report,
};

const V1: &str = "riffdb.storage.v1.StoredIndexEntryV1";
const V2: &str = "riffdb.storage.v1.StoredIndexEntryV2";

#[test]
fn accepted_registry_is_exactly_thirty_seven_readable_and_thirty_two_writable() {
    assert_eq!(READABLE_RECORD_SCHEMA_COUNT, 37);
    assert_eq!(WRITABLE_RECORD_SCHEMA_COUNT, 32);
    assert_eq!(READABLE_RECORD_SCHEMAS.len(), 37);
    assert_eq!(WRITABLE_RECORD_SCHEMAS.len(), 32);
    assert!(readable_record_schema(V1).is_some());
    assert!(readable_record_schema(V2).is_some());
    assert!(writable_record_schema(V1).is_none());
    assert!(writable_record_schema(V2).is_some());
    assert_eq!(
        READABLE_RECORD_SCHEMAS
            .iter()
            .filter(|schema| schema.record_type() == V1)
            .count(),
        1
    );
    assert_eq!(
        WRITABLE_RECORD_SCHEMAS
            .iter()
            .filter(|schema| schema.record_type() == V2)
            .count(),
        1
    );
}

#[test]
fn comparison_compiles_against_the_current_sealed_trait_names() {
    fn identity_only<T: StartupIndexMigrationPort>() {}
    #[allow(dead_code)]
    fn sealed_backend<T: CatalogIndexMigrationBackend>() {}

    identity_only::<riffdb_storage_fjall_comparison::FjallMigrationIdentityProbe>();
}

#[test]
fn frozen_report_is_current_and_fail_closed() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let fixture = fs::read_to_string(manifest.join("reports/conformance-v1.tsv"))
        .expect("read frozen conformance report");
    assert_eq!(fixture, render_conformance_report());
    assert_eq!(overall_conformance_status(), ConformanceStatus::Failed);
    assert!(
        CONFORMANCE_CASES
            .iter()
            .filter(|case| case.class == ConformanceClass::Semantic)
            .all(|case| case.status == ConformanceStatus::Failed)
    );
    assert!(fixture.contains("performance_eligibility\tnot_run_due_to_conformance_failure"));
}
