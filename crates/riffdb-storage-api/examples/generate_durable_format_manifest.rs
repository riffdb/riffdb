//! Generates the exact alpha durable-format manifest and compatibility inventory.

use std::env;
use std::fmt::Write as _;
use std::fs;
use std::path::Path;

use riffdb_proto::envelope::RecordSchema;
use riffdb_storage_api::{
    DurableFormatAction, DurableFormatManifest, DurableFormatReleaseEdge,
    current_durable_format_manifest,
};
use sha2::{Digest, Sha256};

const RELEASE_PAIRS_PATH: &str = "fixtures/compatibility/release-pairs-v1.json";
const PHYSICAL_FORMATS_PATH: &str = "fixtures/compatibility/physical-formats-v1.json";
const RETIRE_RECEIPT_V2_FIXTURE_PATH: &str =
    "fixtures/compatibility/offline-maintenance-retire-receipt-v2.hex";
const INVENTORY_PATH: &str = "fixtures/compatibility/durable-fixture-inventory-v1.txt";
const MANIFEST_PATH: &str = "release/durable-format-manifest-v1.json";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let check = env::args().skip(1).any(|argument| argument == "--check");
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("storage-api crate is inside the workspace");
    let manifest = current_durable_format_manifest();
    let release_pairs = render_release_pairs(manifest);
    let physical_formats = render_physical_formats(manifest);
    let inventory = render_fixture_inventory(root, &release_pairs, &physical_formats)?;
    let fixture_digest: [u8; 32] = Sha256::digest(inventory.as_bytes()).into();
    let release_manifest = render_release_manifest(manifest, fixture_digest);

    let generated = [
        (RELEASE_PAIRS_PATH, release_pairs.as_bytes()),
        (PHYSICAL_FORMATS_PATH, physical_formats.as_bytes()),
        (INVENTORY_PATH, inventory.as_bytes()),
        (
            "crates/riffdb-storage-api/fixtures/durable-fixture-inventory-v1.txt",
            inventory.as_bytes(),
        ),
        (MANIFEST_PATH, release_manifest.as_bytes()),
    ];
    for (relative, bytes) in generated {
        let path = root.join(relative);
        if check {
            let current = fs::read(&path).unwrap_or_default();
            if current != bytes {
                return Err(
                    format!("generated durable-format artifact is stale: {relative}").into(),
                );
            }
        } else {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(path, bytes)?;
        }
    }

    if check && manifest.compatibility_fixture_digest().as_bytes() != &fixture_digest {
        return Err("compiled compatibility fixture digest is stale; rebuild and re-run".into());
    }
    Ok(())
}

fn render_physical_formats(manifest: DurableFormatManifest) -> String {
    let mut output = String::new();
    output.push_str("{\n  \"schema\": \"riffdb.physical-format-inventory/v1\",\n");
    render_number_array(
        &mut output,
        "readable_storage_versions",
        manifest.readable_storage_versions(),
    );
    render_number_array(
        &mut output,
        "writable_storage_versions",
        manifest.writable_storage_versions(),
    );
    render_number_array(
        &mut output,
        "readable_redb_layout_versions",
        manifest.readable_redb_layout_versions(),
    );
    render_number_array(
        &mut output,
        "writable_redb_layout_versions",
        manifest.writable_redb_layout_versions(),
    );
    render_number_array(
        &mut output,
        "journal_frame_versions",
        manifest.readable_journal_frame_versions(),
    );
    render_number_array(
        &mut output,
        "journal_extent_versions",
        manifest.readable_journal_extent_versions(),
    );
    render_number_array(
        &mut output,
        "backup_manifest_versions",
        manifest.readable_backup_versions(),
    );
    render_number_array(
        &mut output,
        "maintenance_receipt_versions",
        manifest.readable_receipt_versions(),
    );
    render_number_array(
        &mut output,
        "offline_maintenance_receipt_versions",
        manifest.readable_offline_maintenance_receipt_versions(),
    );
    render_number_array(
        &mut output,
        "contract_migration_check_receipt_versions",
        manifest.readable_contract_migration_check_receipt_versions(),
    );
    render_number_array(
        &mut output,
        "format_upgrade_receipt_versions",
        manifest.readable_format_upgrade_receipt_versions(),
    );
    render_number_array(
        &mut output,
        "format_marker_versions",
        manifest.readable_format_marker_versions(),
    );
    output.push_str(
        "  \"history_incarnation_record\": \"riffdb.storage.v1.StoredHistoryIncarnationV1\",\n",
    );
    output.push_str(
        "  \"validated_prefix_record\": \"riffdb.storage.v1.StoredValidatedPrefixCheckpointV1\",\n",
    );
    output.push_str("  \"retention_records\": [\"riffdb.storage.v1.StoredRetentionWatermarkV1\", \"riffdb.storage.v1.StoredHistoryTombstoneV1\"],\n");
    output.push_str("  \"authority\": \"redb checkpoint plus exact journal suffix\",\n");
    output.push_str(
        "  \"recovery_validation\": \"tests/storage_recovery/storage_recovery_matrix.rs\"\n}\n",
    );
    output
}

fn render_release_pairs(manifest: DurableFormatManifest) -> String {
    let mut output = String::new();
    output.push_str("{\n  \"schema\": \"riffdb.durable-release-pairs/v1\",\n");
    output.push_str("  \"downgrade\": \"unsupported\",\n  \"edges\": [");
    for (index, edge) in manifest.release_edges().iter().enumerate() {
        if index == 0 {
            output.push('\n');
        } else {
            output.push_str(",\n");
        }
        render_edge(&mut output, *edge);
    }
    if !manifest.release_edges().is_empty() {
        output.push('\n');
    }
    output.push_str("  ],\n  \"breaking_epochs\": []\n}\n");
    output
}

fn render_edge(output: &mut String, edge: DurableFormatReleaseEdge) {
    write!(
        output,
        "    {{\"source_release\":\"{}\",\"target_release\":\"{}\",\"source\":{{\"epoch\":{},\"writer\":{}}},\"target\":{{\"epoch\":{},\"writer\":{}}},",
        edge.source_release(),
        edge.target_release(),
        edge.source().epoch().get(),
        edge.source().writer().get(),
        edge.target().epoch().get(),
        edge.target().writer().get(),
    )
    .expect("write String");
    match edge.action() {
        DurableFormatAction::OpenCurrent => output.push_str("\"action\":\"open_current\"}"),
        DurableFormatAction::OfflineInPlace {
            backup_required,
            free_space_source_multiples,
            downtime_required,
            one_way,
            next_command,
        } => {
            write!(
                output,
                "\"action\":\"offline_in_place\",\"backup_required\":{backup_required},\"free_space_source_multiples\":{free_space_source_multiples},\"downtime_required\":{downtime_required},\"one_way\":{one_way},\"next_command\":\"{}\"}}",
                next_command.render()
            )
            .expect("write String");
        }
        DurableFormatAction::ExportReimportOnly {
            backup_required,
            downtime_required,
            next_command,
        } => {
            write!(
                output,
                "\"action\":\"export_reimport_only\",\"backup_required\":{backup_required},\"downtime_required\":{downtime_required},\"next_command\":\"{}\"}}",
                next_command.render()
            )
            .expect("write String");
        }
    }
}

fn render_fixture_inventory(
    root: &Path,
    release_pairs: &str,
    physical_formats: &str,
) -> Result<String, std::io::Error> {
    let mut entries = Vec::new();
    entries.push((
        RELEASE_PAIRS_PATH.to_owned(),
        Sha256::digest(release_pairs.as_bytes()).into(),
    ));
    entries.push((
        PHYSICAL_FORMATS_PATH.to_owned(),
        Sha256::digest(physical_formats.as_bytes()).into(),
    ));
    entries.push((
        RETIRE_RECEIPT_V2_FIXTURE_PATH.to_owned(),
        Sha256::digest(fs::read(root.join(RETIRE_RECEIPT_V2_FIXTURE_PATH))?).into(),
    ));
    for path in [
        "fixtures/replication/export-operation-v2.hex",
        "fixtures/replication/export-page-commitment-v1.hex",
        "fixtures/replication/export-page-commitment-key-v1.hex",
        "fixtures/replication/authoritative-state-catalog-v1.hex",
        "fixtures/replication/authoritative-state-catalog-v1.txt",
        "fixtures/compatibility/offline-maintenance-archive-accepted-receipt-v3.hex",
        "fixtures/compatibility/offline-maintenance-archive-selected-receipt-v3.hex",
    ] {
        entries.push((
            path.to_owned(),
            Sha256::digest(fs::read(root.join(path))?).into(),
        ));
    }
    collect_files(root, &root.join("fixtures/proto"), &mut entries, |path| {
        path.file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("durable-"))
            || path.ends_with("fixtures/proto/descriptors/riffdb-storage-v1-descriptor-set.bin")
    })?;
    collect_files(
        root,
        &root.join("fixtures/migrations/durable"),
        &mut entries,
        |_| true,
    )?;
    entries.sort_by(|left, right| left.0.cmp(&right.0));

    let mut output = String::new();
    for (path, digest) in entries {
        writeln!(output, "{}  {path}", hex(&digest)).expect("write String");
    }
    Ok(output)
}

fn collect_files(
    root: &Path,
    directory: &Path,
    entries: &mut Vec<(String, [u8; 32])>,
    select: impl Copy + Fn(&Path) -> bool,
) -> Result<(), std::io::Error> {
    if !directory.exists() {
        return Ok(());
    }
    let mut children = fs::read_dir(directory)?.collect::<Result<Vec<_>, _>>()?;
    children.sort_by_key(|entry| entry.path());
    for child in children {
        let path = child.path();
        if path.is_dir() {
            collect_files(root, &path, entries, select)?;
        } else if select(&path) {
            let relative = path
                .strip_prefix(root)
                .expect("fixture is inside workspace")
                .to_string_lossy()
                .replace('\\', "/");
            entries.push((relative, Sha256::digest(fs::read(path)?).into()));
        }
    }
    Ok(())
}

fn render_release_manifest(manifest: DurableFormatManifest, fixture_digest: [u8; 32]) -> String {
    let mut output = String::new();
    output.push_str("{\n  \"schema\": \"riffdb.durable-format-manifest/v1\",\n");
    writeln!(output, "  \"release\": \"{}\",", manifest.release()).expect("write String");
    writeln!(output, "  \"alpha_epoch\": {},", manifest.epoch().get()).expect("write String");
    writeln!(output, "  \"writer\": {},", manifest.writer().get()).expect("write String");
    writeln!(
        output,
        "  \"minimum_supported_source_release\": \"{}\",",
        manifest.minimum_supported_source_release()
    )
    .expect("write String");
    writeln!(
        output,
        "  \"maximum_supported_source_release\": \"{}\",",
        manifest.maximum_supported_source_release()
    )
    .expect("write String");
    render_number_array(
        &mut output,
        "readable_storage_versions",
        manifest.readable_storage_versions(),
    );
    render_number_array(
        &mut output,
        "writable_storage_versions",
        manifest.writable_storage_versions(),
    );
    render_number_array(
        &mut output,
        "readable_redb_layout_versions",
        manifest.readable_redb_layout_versions(),
    );
    render_number_array(
        &mut output,
        "writable_redb_layout_versions",
        manifest.writable_redb_layout_versions(),
    );
    render_number_array(
        &mut output,
        "readable_registry_versions",
        manifest.readable_registry_versions(),
    );
    render_number_array(
        &mut output,
        "writable_registry_versions",
        manifest.writable_registry_versions(),
    );
    render_number_array(
        &mut output,
        "readable_journal_frame_versions",
        manifest.readable_journal_frame_versions(),
    );
    render_number_array(
        &mut output,
        "writable_journal_frame_versions",
        manifest.writable_journal_frame_versions(),
    );
    render_number_array(
        &mut output,
        "readable_journal_extent_versions",
        manifest.readable_journal_extent_versions(),
    );
    render_number_array(
        &mut output,
        "writable_journal_extent_versions",
        manifest.writable_journal_extent_versions(),
    );
    render_number_array(
        &mut output,
        "readable_backup_versions",
        manifest.readable_backup_versions(),
    );
    render_number_array(
        &mut output,
        "writable_backup_versions",
        manifest.writable_backup_versions(),
    );
    render_number_array(
        &mut output,
        "readable_receipt_versions",
        manifest.readable_receipt_versions(),
    );
    render_number_array(
        &mut output,
        "writable_receipt_versions",
        manifest.writable_receipt_versions(),
    );
    render_number_array(
        &mut output,
        "readable_offline_maintenance_receipt_versions",
        manifest.readable_offline_maintenance_receipt_versions(),
    );
    render_number_array(
        &mut output,
        "writable_offline_maintenance_receipt_versions",
        manifest.writable_offline_maintenance_receipt_versions(),
    );
    render_number_array(
        &mut output,
        "readable_contract_migration_check_receipt_versions",
        manifest.readable_contract_migration_check_receipt_versions(),
    );
    render_number_array(
        &mut output,
        "writable_contract_migration_check_receipt_versions",
        manifest.writable_contract_migration_check_receipt_versions(),
    );
    render_number_array(
        &mut output,
        "readable_format_upgrade_receipt_versions",
        manifest.readable_format_upgrade_receipt_versions(),
    );
    render_number_array(
        &mut output,
        "writable_format_upgrade_receipt_versions",
        manifest.writable_format_upgrade_receipt_versions(),
    );
    render_number_array(
        &mut output,
        "readable_format_marker_versions",
        manifest.readable_format_marker_versions(),
    );
    render_number_array(
        &mut output,
        "writable_format_marker_versions",
        manifest.writable_format_marker_versions(),
    );
    writeln!(
        output,
        "  \"record_registry_digest\": \"{}\",",
        hex(manifest.registry_digest().as_bytes())
    )
    .expect("write String");
    render_records(&mut output, "readable_records", manifest.readable_records());
    render_records(&mut output, "writable_records", manifest.writable_records());
    writeln!(
        output,
        "  \"compatibility_fixture_digest\": \"{}\",",
        hex(&fixture_digest)
    )
    .expect("write String");
    output.push_str("  \"upgrade_table\": \"compatibility/release-pairs-v1.json\",\n");
    output
        .push_str("  \"physical_format_inventory\": \"compatibility/physical-formats-v1.json\",\n");
    output.push_str("  \"backup_required_before_upgrade\": true,\n");
    output.push_str("  \"downgrade\": \"unsupported\",\n");
    output.push_str("  \"breaking_epoch_posture\": \"export_reimport_only\",\n");
    output.push_str("  \"known_limitations\": [\"no physical downgrade\", \"no physical backup restore across incompatible epochs\"]\n}\n");
    output
}

fn render_number_array<T: std::fmt::Display>(output: &mut String, name: &str, values: &[T]) {
    write!(output, "  \"{name}\": [").expect("write String");
    for (index, value) in values.iter().enumerate() {
        if index != 0 {
            output.push_str(", ");
        }
        write!(output, "{value}").expect("write String");
    }
    output.push_str("],\n");
}

fn render_records(output: &mut String, name: &str, records: &[RecordSchema<'_>]) {
    writeln!(output, "  \"{name}\": [").expect("write String");
    for (index, record) in records.iter().enumerate() {
        writeln!(
            output,
            "    {{\"record_type\":\"{}\",\"tag\":{},\"revision\":{},\"schema_hash\":\"{}\"}}{}",
            record.record_type(),
            record.compact_tag(),
            record.schema_revision(),
            hex(record.schema_hash().as_bytes()),
            if index + 1 == records.len() { "" } else { "," }
        )
        .expect("write String");
    }
    output.push_str("  ],\n");
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    output
}
