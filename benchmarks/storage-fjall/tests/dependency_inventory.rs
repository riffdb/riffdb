#![forbid(unsafe_code)]

//! Frozen dependency and benchmark-eligibility evidence for WP-075.

use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;

const EXPECTED_LINUX_NORMAL_PACKAGES: &[(&str, &str)] = &[
    ("bitflags", "2.13.1"),
    ("byteorder-lite", "0.1.0"),
    ("byteview", "0.10.1"),
    ("cfg-if", "1.0.4"),
    ("compare", "0.0.6"),
    ("crossbeam-epoch", "0.9.20"),
    ("crossbeam-skiplist", "0.1.3"),
    ("crossbeam-utils", "0.8.22"),
    ("dashmap", "6.2.1"),
    ("enum_dispatch", "0.3.13"),
    ("equivalent", "1.0.2"),
    ("fastrand", "2.5.0"),
    ("fjall", "3.1.8"),
    ("flume", "0.12.0"),
    ("getrandom", "0.4.3"),
    ("hashbrown", "0.14.5"),
    ("hashbrown", "0.16.1"),
    ("interval-heap", "0.0.5"),
    ("libc", "0.2.189"),
    ("linux-raw-sys", "0.12.1"),
    ("lock_api", "0.4.14"),
    ("log", "0.4.33"),
    ("lsm-tree", "3.1.8"),
    ("once_cell", "1.21.4"),
    ("parking_lot_core", "0.9.12"),
    ("proc-macro2", "1.0.107"),
    ("quick_cache", "0.6.24"),
    ("quote", "1.0.47"),
    ("rustc-hash", "2.1.3"),
    ("rustix", "1.1.4"),
    ("scopeguard", "1.2.0"),
    ("self_cell", "1.3.0"),
    ("sfa", "1.0.0"),
    ("smallvec", "1.15.2"),
    ("spin", "0.9.9"),
    ("syn", "2.0.119"),
    ("tempfile", "3.27.0"),
    ("unicode-ident", "1.0.24"),
    ("varint-rs", "2.2.1"),
    ("xxhash-rust", "0.8.18"),
];

fn manifest_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn report_rows() -> Vec<Vec<String>> {
    let report = fs::read_to_string(manifest_root().join("reports/dependency-inventory-v1.tsv"))
        .expect("read dependency inventory");
    let header = report
        .lines()
        .position(|line| line.starts_with("package\tversion\t"))
        .expect("dependency inventory header");
    report
        .lines()
        .skip(header + 1)
        .map(|line| line.split('\t').map(str::to_owned).collect())
        .collect()
}

#[test]
fn linux_normal_dependency_inventory_is_exact_and_unique() {
    let actual = report_rows()
        .iter()
        .map(|row| {
            assert_eq!(row.len(), 8);
            (row[0].clone(), row[1].clone())
        })
        .collect::<BTreeSet<_>>();
    let expected = EXPECTED_LINUX_NORMAL_PACKAGES
        .iter()
        .map(|(name, version)| ((*name).to_owned(), (*version).to_owned()))
        .collect::<BTreeSet<_>>();
    assert_eq!(actual.len(), EXPECTED_LINUX_NORMAL_PACKAGES.len());
    assert_eq!(actual, expected);
}

#[test]
fn inventory_discloses_unsafe_and_build_boundaries() {
    let rows = report_rows();
    let fjall = rows
        .iter()
        .find(|row| row[0] == "fjall")
        .expect("Fjall inventory row");
    let lsm_tree = rows
        .iter()
        .find(|row| row[0] == "lsm-tree")
        .expect("lsm-tree inventory row");
    assert_eq!(fjall[2], "root");
    assert_eq!(fjall[7], "3");
    assert_ne!(lsm_tree[7], "0");
    assert!(rows.iter().any(|row| row[5] == "yes" && row[0] == "rustix"));
    assert!(
        rows.iter()
            .any(|row| row[6] == "yes" && row[0] == "enum_dispatch")
    );
    assert!(!rows.iter().any(|row| row[0] == "lz4_flex"));
    assert!(!rows.iter().any(|row| row[0] == "cc"));
    assert!(!rows.iter().any(|row| row[0] == "android_system_properties"));
}

#[test]
fn benchmark_eligibility_is_frozen_fail_closed() {
    let report = fs::read_to_string(manifest_root().join("reports/benchmark-eligibility-v1.tsv"))
        .expect("read benchmark eligibility");
    assert!(report.contains("report_format\triffdb_storage_benchmark_format=1"));
    assert!(report.contains("semantic_conformance\tfailed"));
    assert!(report.contains("performance_eligibility\tnot_publishable_conformance_failure"));
    assert!(report.contains("decision_evidence\tfalse"));
}
