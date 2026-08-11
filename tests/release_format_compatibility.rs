#![forbid(unsafe_code)]
//! Cross-crate release-format refusal and artifact acceptance.

use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

use riffdb_storage_api::{
    AlphaFormatEpoch, DurableFormatAction, DurableFormatIdentity, DurableFormatMarker,
    DurableFormatWriter, SafeFormatCommand, current_durable_format_manifest,
    current_durable_format_marker, encode_durable_format_marker,
};
use riffdb_storage_redb::{
    RedbDurableFormatPreflight, RedbDurableFormatPreflightError, durable_format_marker_path,
    preflight_durable_format_path,
};

static NEXT_ROOT: AtomicU64 = AtomicU64::new(1);

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("testkit must remain beneath crates/")
        .to_path_buf()
}

fn case_root(label: &str) -> PathBuf {
    let ordinal = NEXT_ROOT.fetch_add(1, Ordering::Relaxed);
    let root = repository_root()
        .join("target/release-format-compatibility")
        .join(format!("{label}-{}-{ordinal}", std::process::id()));
    if root.exists() {
        fs::remove_dir_all(&root).expect("remove exact stale test case root");
    }
    fs::create_dir_all(&root).expect("create exact test case root");
    root
}

fn inventory(root: &Path) -> Vec<(OsString, Vec<u8>)> {
    let mut rows: Vec<_> = fs::read_dir(root)
        .expect("read test inventory")
        .map(|entry| {
            let entry = entry.expect("read inventory entry");
            let name = entry.file_name();
            let bytes = fs::read(entry.path()).expect("read inventory bytes");
            (name, bytes)
        })
        .collect();
    rows.sort_by(|left, right| left.0.cmp(&right.0));
    rows
}

#[test]
fn predecessor_is_classified_for_one_exact_offline_upgrade_without_mutation() {
    let root = case_root("predecessor");
    let database = root.join("app.redb");
    fs::write(&database, b"pre-manifest database bytes").expect("write predecessor fixture");
    let before = inventory(&root);

    let result = preflight_durable_format_path(&database).expect("predecessor is declared");
    let RedbDurableFormatPreflight::OfflineUpgradeRequired {
        current,
        binary,
        action,
    } = result
    else {
        panic!("predecessor must require an offline upgrade");
    };
    assert_eq!(current.epoch(), AlphaFormatEpoch::new(1).unwrap());
    assert_eq!(current.writer(), DurableFormatWriter::new(0));
    assert_eq!(binary, current_durable_format_manifest().identity());
    assert_eq!(
        action,
        DurableFormatAction::OfflineInPlace {
            backup_required: true,
            free_space_source_multiples: 2,
            downtime_required: true,
            one_way: true,
            next_command: SafeFormatCommand::Upgrade,
        }
    );
    assert_eq!(inventory(&root), before, "preflight must be read-only");
}

#[test]
fn future_epoch_refuses_before_mutating_any_inventory_member() {
    let root = case_root("future");
    let database = root.join("app.redb");
    fs::write(&database, b"future database bytes").expect("write future fixture");
    let current = current_durable_format_marker();
    let future_identity = DurableFormatIdentity::new(
        AlphaFormatEpoch::new(current.identity().epoch().get() + 1).unwrap(),
        DurableFormatWriter::new(1),
    );
    let marker = DurableFormatMarker::new(
        future_identity,
        current.registry_digest(),
        current.compatibility_fixture_digest(),
    );
    fs::write(
        durable_format_marker_path(&database),
        encode_durable_format_marker(marker),
    )
    .expect("write future marker");
    let before = inventory(&root);

    let error = preflight_durable_format_path(&database).expect_err("future epoch must refuse");
    assert_eq!(error.current_identity(), Some(future_identity));
    assert_eq!(error.next_command(), SafeFormatCommand::UseMatchingBinary);
    assert_eq!(inventory(&root), before, "refusal must be read-only");
}

#[test]
fn corrupt_and_ambiguous_markers_refuse_before_mutation() {
    for (label, marker_bytes) in [("corrupt", vec![0xa5; 114]), ("empty", Vec::new())] {
        let root = case_root(label);
        let database = root.join("app.redb");
        fs::write(&database, b"database must survive refusal").expect("write database fixture");
        fs::write(durable_format_marker_path(&database), marker_bytes)
            .expect("write invalid marker fixture");
        let before = inventory(&root);

        let error =
            preflight_durable_format_path(&database).expect_err("invalid marker must refuse");
        match (label, error) {
            ("empty", RedbDurableFormatPreflightError::AmbiguousInventory)
            | ("corrupt", RedbDurableFormatPreflightError::Marker(_)) => {}
            (_, other) => panic!("unexpected refusal for {label}: {other:?}"),
        }
        assert_eq!(inventory(&root), before, "refusal must not repair or reset");
    }
}

#[test]
fn checked_release_manifest_and_fixture_inventory_are_current() {
    let root = repository_root();
    let output = Command::new(root.join("scripts/check-durable-format-manifest"))
        .current_dir(&root)
        .output()
        .expect("manifest check must launch");
    assert!(
        output.status.success(),
        "manifest check failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}
