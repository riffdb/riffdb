#![forbid(unsafe_code)]

//! Dependency and readiness-boundary guards for the concrete redb adapter.

use std::fs;
use std::path::{Path, PathBuf};

fn crate_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn read(path: impl AsRef<Path>) -> String {
    fs::read_to_string(path).expect("architecture input is readable UTF-8")
}

fn rust_sources() -> String {
    let source = crate_root().join("src");
    let mut paths = Vec::new();
    collect_rust_sources(&source, &mut paths);
    paths.sort();
    paths.into_iter().map(read).collect::<Vec<_>>().join("\n")
}

fn collect_rust_sources(path: &Path, sources: &mut Vec<PathBuf>) {
    if path.is_file() {
        if path.extension().is_some_and(|extension| extension == "rs") {
            sources.push(path.to_owned());
        }
        return;
    }
    if !path.is_dir() {
        return;
    }
    let mut entries = fs::read_dir(path)
        .expect("read architecture source directory")
        .map(|entry| entry.expect("architecture source entry").path())
        .collect::<Vec<_>>();
    entries.sort();
    for entry in entries {
        collect_rust_sources(&entry, sources);
    }
}

fn production_source(path: impl AsRef<Path>) -> String {
    read(path)
        .split("\n#[cfg(test)]\nmod tests")
        .next()
        .expect("production source prefix")
        .to_owned()
}

fn without_whitespace(source: &str) -> String {
    source.split_whitespace().collect()
}

#[test]
// req: REP-003, REC-001, STO-012
fn v3_checkpoint_receipt_rechecks_physical_audit_bound_from_its_own_pin() {
    let source = without_whitespace(&production_source(
        crate_root().join("src/validated_prefix.rs"),
    ));
    let planner = source
        .split_once("fnplan_checkpoint_receipt(")
        .unwrap()
        .1
        .split_once("fnfingerprint_from_head_table(")
        .unwrap()
        .0;
    assert!(planner.contains("base.audit_sequence_bound()!=last_audit_sequence(transaction)?.map_or(0,AdministrationSequence::get)"));
    assert!(planner.contains(
        "base.audit_sequence_bound()>frontier.administration().map_or(0,|sequence|sequence.get())"
    ));
    assert!(!planner.contains("begin_read("));
    assert!(!planner.contains("begin_write("));
}

#[test]
// req: REP-003, REC-001, STO-012
fn v3_index_migration_owners_capture_without_new_transaction_or_profile() {
    let backend = without_whitespace(&production_source(
        crate_root().join("src/startup/index_migration_backend.rs"),
    ));
    let batch = backend
        .split_once("fnapply_index_migration_batch(")
        .unwrap()
        .1
        .split_once("fnfinish_index_migration(")
        .unwrap()
        .0;
    assert_eq!(batch.matches(".begin_write()").count(), 1);
    assert_eq!(
        batch
            .matches("transaction.finish()?.commit(&self.shared)?")
            .count(),
        1
    );
    assert!(batch.contains("ChangelogAttributionV3::IndexMigrationBatch"));
    let capture = batch
        .find("OperationalWriteTransaction::from_drained(")
        .unwrap();
    assert!(batch.find("set_durability(Durability::Immediate)").unwrap() < capture);
    assert!(capture < batch.find(".open_table(SECONDARY_INDEXES)").unwrap());
    let store = without_whitespace(&production_source(crate_root().join("src/store.rs")));
    for (start, end) in [
        (
            "fnmark_index_epoch_rows_repaired(",
            "fnread_legacy_index_epoch_maxima(",
        ),
        (
            "fnmigrate_partition_index_generation_rows(",
            "fnremove_legacy_index_epoch_rows(",
        ),
        (
            "fnremove_legacy_index_epoch_rows(",
            "fnvalidate_partition_index_generation_rows(",
        ),
    ] {
        let body = store
            .split_once(start)
            .unwrap()
            .1
            .split_once(end)
            .unwrap()
            .0;
        assert_eq!(body.matches(".begin_write()").count(), 1);
        assert_eq!(
            body.matches("transaction.finish()?.commit(shared)?")
                .count(),
            1
        );
        assert!(body.contains("ChangelogAttributionV3::StorageFormatMigration"));
        assert!(!body.contains("shared.commit_durable(transaction)"));
        assert!(body.contains("set_two_phase_commit(true)"));
        assert!(body.contains("set_durability(Durability::Immediate)"));
    }
}

#[test]
// req: REP-003, REC-001, PERF-007
fn v3_startup_streams_retained_history_only_after_the_bounded_clean_return() {
    let startup = production_source(crate_root().join("src/startup.rs"));
    let begin = startup
        .split_once("fn begin_structural_evidence_for(")
        .unwrap()
        .1
        .split_once("impl StructuralEvidenceSession")
        .unwrap()
        .0;
    let clean = begin
        .find("return Ok(RedbStructuralEvidenceSession")
        .unwrap();
    let full = begin
        .find("validate_retained_history(&transaction)")
        .unwrap();
    let retention = begin
        .find("crate::retention::load_watermark(&transaction)")
        .unwrap();
    assert!(clean < full && full < retention);
    assert_eq!(begin.matches("validate_retained_history(").count(), 1);
    assert!(!begin.contains("begin_write("));
    assert!(!begin.contains(".commit("));
    let inventory = startup
        .split_once("fn validate_table_inventory(")
        .unwrap()
        .1
        .split_once("fn table_len(")
        .unwrap()
        .0;
    assert!(inventory.contains("crate::store::v3_layout::exact_current_tables"));
    assert!(inventory.contains("read_checkpoint_roots(transaction)"));
    assert!(!inventory.contains("validate_retained_history"));
    let roots = production_source(crate_root().join("src/changelog_v3_roots.rs"));
    let full = roots
        .split_once("pub(crate) fn validate_retained_history(")
        .unwrap()
        .1
        .split_once("pub(crate) fn read_checkpoint_roots_for_write(")
        .unwrap()
        .0;
    assert!(full.contains("table.iter()"));
    assert!(!full.contains(".collect"));
    assert!(!full.contains(".to_vec()"));
    assert!(!full.contains("begin_read("));
}

#[test]
// req: STO-023, REC-001, REC-002, REC-004, PERF-019
fn graceful_close_drops_immutable_classification_before_one_final_write_transaction() {
    let store_owner = production_source(crate_root().join("src/store.rs"));
    assert!(store_owner.contains("#[path = \"store_graceful_close.rs\"]"));
    let store = production_source(crate_root().join("src/store_graceful_close.rs"));
    let close = store
        .split_once("fn complete_graceful_close(")
        .expect("graceful close owner")
        .1
        .split_once("fn complete_graceful_close_barrier")
        .expect("barrier helper follows orchestrator")
        .0;
    let read = close
        .find("self.database.begin_read()")
        .expect("immutable classification view");
    let classify = close
        .find("classify_graceful_checkpoint(")
        .expect("checkpoint classification");
    let drop_read = close.find("drop(read)").expect("classification view drop");
    let begin = close
        .find("self.database.begin_write()")
        .expect("one independent final write");
    let clean = close
        .find("write_final_clean_close_lifecycle_after_barrier(write)")
        .expect("same write enters CLEAN");
    assert!(read < classify && classify < drop_read && drop_read < begin && begin < clean);
    assert_eq!(close.matches("begin_write()").count(), 1);
    assert_eq!(close.matches("begin_read()").count(), 2);
    let probe = close
        .find("follower_lifecycle::is_attached(&read)")
        .unwrap();
    let refusal = close.find("if !matches!(source, Ok(false))").unwrap();
    let barrier = close
        .find("self.complete_graceful_close_barrier()")
        .unwrap();
    // The probe's owned read is dropped by and_then before either the barrier
    // or its separate immutable checkpoint classification view is opened.
    assert!(probe < refusal && refusal < barrier && barrier < read);

    let final_clean = store
        .split_once("fn write_final_clean_close_lifecycle_after_barrier")
        .expect("final CLEAN helper")
        .1
        .split_once("fn read_commit_tail_from_write")
        .expect("bounded tail helper")
        .0;
    assert!(!final_clean.contains("begin_read()"));
    assert!(!final_clean.contains("begin_write()"));
    assert!(final_clean.contains("bounded_state_binding_hash_for_write"));

    let checkpoint = production_source(crate_root().join("src/validated_prefix.rs"));
    let classifier = checkpoint
        .split_once("pub(crate) fn classify_graceful_checkpoint")
        .expect("bounded classifier")
        .1
        .split_once("/// Builds and durably writes")
        .expect("checkpoint writer follows classifier")
        .0;
    for forbidden in [
        ".iter()",
        ".last()",
        "command_authority_head",
        "decode_command_segment",
        "decode_entity",
        "build_checkpoint_from_snapshot",
        "replace_checkpoint_entity_heads",
        ".to_vec()",
    ] {
        assert!(
            !classifier.contains(forbidden),
            "classifier must not reach population key/value path {forbidden}"
        );
    }
    let cardinalities = checkpoint
        .split_once("fn counts_from_durable_cardinalities")
        .expect("graceful cardinality helper")
        .1
        .split_once("fn retained_snapshot")
        .expect("cardinality helper end")
        .0;
    for forbidden in [".iter()", ".last()", "decode_", ".value()"] {
        assert!(
            !cardinalities.contains(forbidden),
            "graceful cardinality helper must not read population content via {forbidden}"
        );
    }
}

#[test]
// req: REP-003, REC-001, STO-012
fn v3_lifecycle_receipts_use_the_existing_final_transaction_without_recursive_rows() {
    let helper = without_whitespace(&production_source(
        crate_root().join("src/store_changelog_lifecycle.rs"),
    ));
    assert!(!helper.contains("begin_write("));
    assert!(!helper.contains(".commit("));
    assert!(!helper.contains(".insert("));
    assert!(helper.contains("ChangelogAttributionV3::CleanClose"));
    assert!(helper.contains("ChangelogAttributionV3::DirtyActivation"));
    assert!(helper.contains("Vec::new()"));
    assert!(helper.contains("read_checkpoint_roots(transaction)"));
    assert!(helper.contains("expected_allocator().allocate_one()"));
    let store = without_whitespace(&production_source(crate_root().join("src/store.rs")));
    let dirty = store
        .split_once("pub(crate)fnadvance_dirty_lifecycle_before_activation(")
        .unwrap()
        .1
        .split_once("pub(crate)fncommit_durable(")
        .unwrap()
        .0;
    assert_eq!(dirty.matches("begin_write()").count(), 1);
    assert_eq!(dirty.matches("self.commit_durable(transaction)").count(), 1);
    assert!(
        dirty.find("changelog_lifecycle::prepare(").unwrap()
            < dirty
                .find("meta.insert(META_CLEAN_CLOSE_LIFECYCLE")
                .unwrap()
    );
    assert!(
        dirty.find("receipt.stage(&transaction)").unwrap()
            < dirty.find("self.commit_durable(transaction)").unwrap()
    );
    let close = without_whitespace(&production_source(
        crate_root().join("src/store_graceful_close.rs"),
    ));
    let clean = close
        .split_once("fnwrite_final_clean_close_lifecycle_after_barrier(")
        .unwrap()
        .1
        .split_once("fnread_commit_tail_from_write(")
        .unwrap()
        .0;
    assert!(!clean.contains("begin_write("));
    assert_eq!(clean.matches("self.commit_durable(write)").count(), 1);
    assert!(
        clean.find("changelog_lifecycle::prepare(").unwrap()
            < clean
                .find("meta.insert(META_CLEAN_CLOSE_LIFECYCLE")
                .unwrap()
    );
    assert!(
        clean.find("receipt.stage(&write)").unwrap()
            < clean.find("self.commit_durable(write)").unwrap()
    );
    let after = clean.split_once("self.commit_durable(write)").unwrap().1;
    assert!(!after.contains("open_table("));
    assert!(!after.contains(".stage("));
    assert!(!after.contains(".insert("));
}

#[test]
fn production_sized_storage_tests_never_allocate_under_the_ambient_temp_directory() {
    let mut paths = Vec::new();
    for relative in [
        "src",
        "tests",
        "benches",
        "../../tests/storage_recovery",
        "../../tests/service_audit_recovery",
    ] {
        collect_rust_sources(&crate_root().join(relative), &mut paths);
    }
    for path in paths {
        if path == crate_root().join("tests/architecture.rs") {
            continue;
        }
        let source = read(&path);
        assert!(
            !source.contains("std::env::temp_dir()") && !source.contains("env::temp_dir()"),
            "production-sized redb tests must allocate below target/riffdb-test-data, not the ambient temp directory: {}",
            path.display()
        );
    }
}

#[test]
// req: STO-001
fn dependency_surface_keeps_redb_private_and_excludes_infrastructure_assemblies() {
    let manifest = read(crate_root().join("Cargo.toml"));
    assert!(manifest.contains("cap-std = { version = \"=4.0.2\", default-features = false }"));
    assert!(manifest.contains("redb = { version = \"=4.2.0\", default-features = false }"));
    assert!(manifest.contains("sha2 = { version = \"=0.11.0\", default-features = false }"));
    assert!(manifest.contains(
        "riffdb-catalog = { version = \"0.1.0\", path = \"../riffdb-catalog\", default-features = false }"
    ));
    let (dependencies, dev_dependencies) = manifest
        .split_once("[dev-dependencies]")
        .expect("redb manifest keeps normal and test-only dependencies separate");
    assert!(!dependencies.contains("riffdb-contract-compiler"));
    assert!(dev_dependencies.contains(
        "riffdb-contract-compiler = { version = \"0.1.0\", path = \"../riffdb-contract-compiler\", default-features = false }"
    ));
    for forbidden in ["criterion", "riffdb-contract-ir", "rmcp", "tokio", "tonic"] {
        assert!(
            !manifest.contains(forbidden),
            "redb adapter must not depend on {forbidden}"
        );
    }

    let public_root = read(crate_root().join("src/lib.rs"));
    assert!(!public_root.contains("pub use cap_std"));
    assert!(!public_root.contains("pub use redb"));
    assert!(!public_root.contains("extern crate redb"));
}

// req: DEP-001
#[test]
fn storage_backends_depend_only_on_storage_api_types_proto_and_catalog_driver() {
    let root = crate_root();
    let workspace = root
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    for backend in ["riffdb-storage-redb"] {
        let manifest = read(workspace.join("crates").join(backend).join("Cargo.toml"));
        let dependencies = manifest
            .split_once("[dev-dependencies]")
            .map_or(manifest.as_str(), |(production, _)| production);
        for forbidden in [
            "riffdb-query-executor",
            "riffdb-query-ir",
            "riffdb-policy",
            "riffdb-projection",
        ] {
            assert!(
                !dependencies.contains(forbidden),
                "{backend} retains forbidden production dependency {forbidden}"
            );
        }
    }
}

// req: DEP-001
#[test]
fn executor_owned_snapshot_pages_match_pre_inversion_fixtures() {
    let root = crate_root();
    let workspace = root
        .parent()
        .and_then(Path::parent)
        .expect("workspace root");
    let fixture_root = workspace.join("fixtures/query-executor/pre-inversion-v1");
    let manifest_path = fixture_root.join("manifest.sha256");
    assert!(
        manifest_path.is_file(),
        "capture the reviewed pre-inversion fixture bundle before moving execution"
    );
    let manifest = read(&manifest_path);
    assert_eq!(manifest.lines().count(), 28, "the reviewed corpus is exact");
    for line in manifest.lines() {
        let (_, name) = line
            .split_once("  ")
            .expect("sha256 manifest uses canonical two-space separation");
        assert!(
            fixture_root.join(name).is_file(),
            "manifest fixture is present: {name}"
        );
    }
    assert!(
        workspace
            .join("crates/riffdb-query-executor/src/storage_executor.rs")
            .is_file(),
        "the obligation remains fail-first until executor-over-storage exists"
    );
    let generator = read(workspace.join("scripts/generate-query-pre-inversion-fixtures"));
    assert!(generator.contains("diff -ru \"$fixture_root\" \"$generated\""));
    assert!(generator.contains("test \"$(find \"$generated\""));
    let redb_oracle = read(root.join("src/query.rs"));
    for parity in [
        "assert_eq!(inverted, snapshot)",
        "assert_eq!(inverted_second, second)",
        "assert_eq!(inverted_range_first, range_first)",
        "assert_eq!(inverted_range_second, range_second)",
        "assert_eq!(inverted_error, error)",
    ] {
        assert!(
            redb_oracle.contains(parity),
            "missing parity oracle: {parity}"
        );
    }
}

// req: DEP-001
#[test]
fn retired_storage_query_census_never_publishes_false_executor_measurements() {
    let census = riffdb_storage_redb::query_execute_census_v1();
    assert_eq!(census.total_count, 0);
    assert!(
        census
            .windows
            .iter()
            .all(|window| *window == Default::default())
    );
}

// req: DEP-002
#[test]
fn redb_startup_produces_evidence_only_and_drives_no_migration() {
    let startup = production_source(crate_root().join("src/startup.rs"));
    for forbidden in [
        "CatalogIndexMigrationInstruction",
        "read_index_migration_page",
        "apply_index_migration_batch",
        "finish_index_migration",
        "apply_index_migration_instruction",
    ] {
        assert!(
            !startup.contains(forbidden),
            "startup must produce evidence only and not own {forbidden}"
        );
    }
}

// req: PERF-019
#[test]
fn production_redb_cache_is_explicitly_bounded_on_every_authoritative_open() {
    let source = read(crate_root().join("src/store.rs"));
    assert!(
        source.contains("const REDB_CACHE_SIZE_BYTES: usize = 4 * 1024 * 1024;"),
        "the fixed page cache must leave room for the complete production graph inside PERF-019"
    );
    assert_eq!(
        source.matches("let mut builder = Builder::new();").count(),
        1,
        "authoritative opens must converge on one redb builder"
    );
    assert_eq!(
        source
            .matches("builder.set_cache_size(REDB_CACHE_SIZE_BYTES);")
            .count(),
        1,
        "the authoritative redb builder must apply exactly one fixed cache budget"
    );
}

// req: PERF-019
#[test]
fn dedicated_storage_threads_apply_the_fixed_stack_budget() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let library = std::fs::read_to_string(root.join("lib.rs")).expect("read storage library");
    assert!(library.contains("PRODUCTION_THREAD_STACK_BYTES: usize = 384 * 1024;"));
    for owner in [
        "command_segment_preparation.rs",
        "journal.rs",
        "store_journal_runtime.rs",
    ] {
        let source = std::fs::read_to_string(root.join(owner)).expect("read thread owner");
        assert!(
            source.contains(".stack_size(crate::PRODUCTION_THREAD_STACK_BYTES)"),
            "{owner} must apply the fixed production stack budget"
        );
    }
    let adapter = read(root.join("changelog.rs"));
    assert!(!adapter.contains("std::thread"));
    assert!(!adapter.contains(".spawn("));
    let compatibility =
        read(root.join("../../../tests/storage_recovery/changelog_compatibility.rs"));
    assert!(compatibility.contains("#![cfg(test)]"));
    assert_eq!(compatibility.matches(".stack_size(384 * 1024)").count(), 2);
}

#[test]
fn sha256_dependency_is_confined_to_reviewed_integrity_boundaries() {
    let source = crate_root().join("src");
    for entry in fs::read_dir(source).expect("read source directory") {
        let path = entry.expect("source entry").path();
        if path.file_name().is_some_and(|name| {
            matches!(
                name.to_str(),
                Some(
                    "backup.rs"
                        | "benchmark_support.rs"
                        | "changelog_v3.rs" // ADR-0186 exact allocator precondition, no new primitive.
                        | "fresh_locator_coverage.rs"
                        | "format_upgrade.rs"
                        | "journal.rs"
                        | "migration_stage.rs"
                )
            )
        }) || path.extension().is_none_or(|extension| extension != "rs")
        {
            continue;
        }
        assert!(
            !read(&path).contains("sha2"),
            "sha2 must remain confined to reviewed integrity boundaries: {}",
            path.display()
        );
    }
    for path in ["src/maintenance/codec.rs", "src/maintenance/store.rs"] {
        assert!(
            read(crate_root().join(path)).contains("sha2"),
            "migration artifact integrity must retain SHA-256: {path}"
        );
    }
    assert!(
        read(crate_root().join("src/journal.rs")).contains("sha2"),
        "durability journal integrity must retain SHA-256"
    );
    assert!(
        read(crate_root().join("src/format_upgrade.rs")).contains("sha2"),
        "durable-format upgrade receipts must retain SHA-256"
    );
}

#[test]
fn only_startup_imports_the_catalog_driver_and_storage_never_imports_ir() {
    let sources = rust_sources();
    for forbidden in [
        "riffdb_contract_ir",
        "riffdb_invariant",
        "ValidatedCatalogHistory",
    ] {
        assert!(
            !sources.contains(forbidden),
            "storage source must not contain {forbidden}"
        );
    }

    let source = crate_root().join("src");
    let startup = source.join("startup.rs");
    let startup_tests = source.join("startup/tests.rs");
    let migration_backend = source.join("startup/index_migration_backend.rs");
    let mut paths = Vec::new();
    collect_rust_sources(&source, &mut paths);
    for path in paths {
        if path == startup || path == startup_tests || path == migration_backend {
            continue;
        }
        assert!(
            !production_source(&path).contains("riffdb_catalog"),
            "only startup migration may import catalog: {}",
            path.display()
        );
    }

    let migration = read(migration_backend);
    assert!(migration.contains("CatalogIndexMigrationBackend"));
    assert!(!migration.contains("ValidatedCatalogHistory"));
}

#[test]
fn public_root_exports_no_migration_intermediate_or_apply_surface() {
    let public_root = read(crate_root().join("src/lib.rs"));
    for forbidden in [
        "RedbIndexMigrationPage",
        "RedbIndexMigrationRow",
        "RedbIndexMigrationBundleRead",
        "RedbIndexMigrationBatch",
        "RedbIndexMigrationExactEnd",
    ] {
        assert!(
            !public_root.contains(forbidden),
            "public backend root must not export {forbidden}"
        );
    }
}

#[test]
fn command_segment_preparation_workers_hold_no_authoritative_port() {
    let source = production_source(crate_root().join("src/command_segment_preparation.rs"));
    for forbidden in [
        "redb::",
        "RedbWriteAccess",
        "WriteTransaction",
        "ReadTransaction",
        "ApplicationSequenceAllocator",
        "DeferredCommandFence",
        "ExclusiveGate",
        "JournalRuntime",
        "StorageError",
        "assign_commit",
        "put_command",
        "publish",
    ] {
        assert!(
            !source.contains(forbidden),
            "command-segment preparation gained forbidden authority through {forbidden}"
        );
    }
    assert!(source.contains("MAX_COMMAND_SEGMENT_PREPARATION_WORKERS: usize = 8"));
    assert!(source.contains("capsules.len() <= 1 || self.worker_count == 0"));
}

#[test]
// req: PERF-007
fn only_operational_ports_implement_semantic_runtime_traits() {
    let sources = rust_sources();
    for forbidden in [
        "impl SnapshotReader for RedbStore",
        "impl AdmissionRepository for RedbStore",
        "impl ApplicationCommandTransactionPort for RedbStore",
        "impl SnapshotReader for RedbDormantPorts",
        "impl AdmissionRepository for RedbDormantPorts",
        "impl ApplicationCommandTransactionPort for RedbDormantPorts",
    ] {
        assert!(
            !sources.contains(forbidden),
            "readiness bypass: {forbidden}"
        );
    }
    assert!(sources.contains("impl SnapshotReader for RedbOperationalPorts"));
    assert!(sources.contains("impl ApplicationCommandTransactionPort for RedbOperationalPorts"));
    assert!(sources.contains("impl DeferredCommandEpochPort for RedbOperationalPorts"));
}

#[test]
fn unpublished_epoch_state_has_no_committed_result_escape_hatch() {
    let api = read(crate_root().join("../riffdb-storage-api/src/command_txn.rs"));
    let unpublished = api
        .split_once("pub struct UnpublishedAuditedBatchV1")
        .expect("unpublished batch type")
        .1
        .split_once("impl AuditedCommittedBatchV1")
        .expect("unpublished batch implementation end")
        .0;
    assert!(!unpublished.contains("AuditedCommittedBatchV1"));
    assert!(!unpublished.contains("CommittedBatchV1"));
    assert!(!unpublished.contains("impl Clone"));

    let application = production_source(crate_root().join("src/application.rs"));
    let deferred = application
        .split_once("impl DeferredNonEmptyCommandBatch for RedbNonEmptyBatch")
        .expect("redb deferred batch implementation")
        .1;
    assert!(deferred.contains("apply_unpublished"));
    assert!(!deferred.contains("commit_for_with_delta"));
}

#[test]
fn entity_history_never_commits_without_walkable_transition_capsules() {
    let application = production_source(crate_root().join("src/application.rs"));
    let direct_commit = application
        .split_once("impl NonEmptyCommandBatch for RedbNonEmptyBatch")
        .expect("redb nonempty batch implementation")
        .1
        .split_once("fn commit_with_service_audit_transitions(")
        .expect("storage-only commit implementation end")
        .0;
    let refusal = direct_commit
        .find("capsule_entity_transitions")
        .expect("WP-608 entity-transition refusal");
    let materialization = direct_commit
        .find("materialize_uncapsulated_command_rows")
        .expect("retained uncapsulated row materialization");
    assert!(
        refusal < materialization,
        "entity-bearing uncapsulated batches must refuse before durable row materialization"
    );
    assert!(direct_commit.contains("StorageErrorKind::InvariantViolation"));
}

#[test]
fn terminal_staging_reuses_the_same_transaction_current_admission_proof() {
    let application = production_source(crate_root().join("src/application.rs"));
    let apply = application
        .split_once("fn apply_record_set(")
        .expect("record staging function")
        .1
        .split_once("\nfn stage_application_allocator(")
        .expect("record staging body")
        .0;
    assert!(!apply.contains("read_admission("));
    assert!(!apply.contains("matching_admissions("));

    let candidate = application
        .split_once("fn recheck_admission(")
        .expect("candidate transaction-current recheck")
        .1
        .split_once("impl CommandCandidateStateRead")
        .expect("candidate recheck body")
        .0;
    assert!(candidate.contains("read_candidate_admission("));
    assert!(candidate.contains("matching_candidate_admissions("));
}

#[test]
// req: PERF-019
fn startup_fast_path_does_not_rebuild_whole_history_accelerators() {
    let startup = production_source(crate_root().join("src/startup.rs"));
    let samples = startup
        .split_once("fn run_checkpoint_sample_windows(")
        .expect("checkpoint sampler")
        .1
        .split_once("\nimpl RedbStructuralEvidenceSession")
        .expect("checkpoint sampler end")
        .0;
    assert!(!samples.contains("command_capsule_cache_from_segments"));
    assert!(!samples.contains("command_audit_cache_from_segments"));
    assert!(samples.contains("inspect_event_row"));
    assert!(samples.contains("inspect_audit_row"));

    let cursor = startup
        .split_once("fn next_structural_row_raw(")
        .expect("structural cursor")
        .1
        .split_once("\nfn inspect_table_row_from_bytes(")
        .expect("structural cursor end")
        .0;
    assert!(
        !cursor.contains("ensure_command_cache"),
        "opening an empty checkpoint-truncated audit phase must not decode command history"
    );

    let index = startup
        .split_once("fn inspect_index_row(")
        .expect("index inspector")
        .1
        .split_once("\nfn inspect_epoch_row(")
        .expect("index inspector end")
        .0;
    assert!(index.contains("binding_bundles.contains"));
    assert!(!index.contains("open_table"));
}

#[test]
fn startup_full_validation_reuses_exact_embedded_command_authority() {
    let startup = production_source(crate_root().join("src/startup.rs"));
    let cache = startup
        .split_once("fn ensure_command_cache(")
        .expect("command cache")
        .1
        .split_once("\n    fn ensure_entity_chains_built(")
        .expect("command cache end")
        .0;
    assert!(cache.contains("crate::command_prefix::decode_segment"));
    assert!(cache.contains("embedded_authority.insert(sequence)"));
    assert!(cache.contains("return Err(corrupt())"));

    let commit = startup
        .split_once("fn inspect_commit_row(")
        .expect("commit inspector")
        .1
        .split_once("\nfn inspect_provenance_row(")
        .expect("commit inspector end")
        .0;
    assert!(commit.contains("embedded_command_authority.contains"));
    assert!(commit.contains("command_capsule_graph_is_reciprocal"));
    assert!(!commit.contains("plan_bundle_exists"));

    let audit = startup
        .split_once("fn inspect_cached_command_audit_row(")
        .expect("cached audit inspector")
        .1
        .split_once("\nfn inspect_audit_row(")
        .expect("cached audit inspector end")
        .0;
    assert!(audit.contains("embedded_command_authority.contains"));
    assert!(!audit.contains("command_member_at"));
}

#[test]
fn startup_publication_validation_uses_one_shared_audit_pass() {
    let startup = production_source(crate_root().join("src/startup.rs"));
    let builder = startup
        .split_once("fn build_publication_audit_cache(")
        .expect("publication cache builder")
        .1
        .split_once("\nfn inspect_event_consumer_row(")
        .expect("publication cache builder end")
        .0;
    assert_eq!(builder.matches("audit.iter()").count(), 1);
    assert!(builder.contains("decode_non_command_administration_audit"));
    assert!(builder.contains("record.administration_sequence() != sequence"));
    assert!(builder.contains("MAX_STARTUP_EVIDENCE_INDEX_BYTES"));

    let dispatch = startup
        .split_once("fn inspect_structural_forward(")
        .expect("structural dispatch")
        .1
        .split_once("\n    fn ensure_command_cache(")
        .expect("structural dispatch end")
        .0;
    assert!(dispatch.contains("ensure_publication_audit_cache"));
    assert!(dispatch.contains("publication_audits: self.publication_audits"));

    for helper in [
        "fn bundle_has_activation_cached(",
        "fn query_module_has_activation_cached(",
        "fn query_module_record_is_reciprocal_cached(",
        "fn catalog_record_is_reciprocal_cached(",
        "fn active_catalog_matches_last_activation_cached(",
        "fn inspect_reactive_module_row_cached(",
    ] {
        let body = startup
            .split_once(helper)
            .unwrap_or_else(|| panic!("missing cached helper {helper}"))
            .1
            .split_once("\nfn ")
            .unwrap_or_else(|| panic!("missing cached helper end {helper}"))
            .0;
        assert!(!body.contains("open_table(AUDIT)"), "{helper}");
        assert!(
            !body.contains("decode_non_command_administration_audit"),
            "{helper}"
        );
    }
}

#[test]
fn checkpoint_prefix_skips_before_building_the_command_history_cache() {
    let startup = production_source(crate_root().join("src/startup.rs"));
    let dispatch = startup
        .split_once("fn inspect_structural_forward(")
        .expect("structural dispatch")
        .1
        .split_once("\n    fn ensure_command_cache(")
        .expect("structural dispatch end")
        .0;
    let terminal_branch = dispatch
        .split_once("if phase == 8")
        .expect("terminal phase")
        .1
        .split_once("if let Some(checkpoint)")
        .expect("terminal phase end")
        .0;
    assert!(!terminal_branch.contains("ensure_command_cache"));
    assert!(dispatch.contains("if phase == 21"));
    assert!(!dispatch.contains("matches!(phase, 21 | 22)"));
    assert!(!startup.contains("fn inspect_cached_command_audit_request_row("));

    let request_index = startup
        .split_once("fn inspect_audit_by_request_row(")
        .expect("audit request inspector")
        .1
        .split_once("\nconst MAX_STARTUP_EVIDENCE_INDEX_BYTES")
        .expect("audit request inspector end")
        .0;
    assert!(request_index.contains("service_link_is_valid"));
    assert!(!request_index.contains("ensure_command_cache"));

    let terminal = startup
        .split_once("fn inspect_terminal_row_with_census(")
        .expect("terminal inspector")
        .1
        .split_once("\n    fn inspect_entity_row_with_chains(")
        .expect("terminal inspector end")
        .0;
    let checkpoint_skip = terminal
        .find("class.is_prefix_outcome")
        .expect("checkpoint terminal classification");
    let command_cache = terminal
        .find("self.ensure_command_cache()")
        .expect("suffix command cache");
    assert!(checkpoint_skip < command_cache);

    let allocator = startup
        .split_once("fn administration_allocator_matches(")
        .expect("administration allocator")
        .1
        .split_once("\nfn read_active_pointer(")
        .expect("administration allocator end")
        .0;
    assert!(allocator.contains("checkpoint.retained.next_administration_sequence"));
    assert!(allocator.contains("Excluded(lower.as_slice()), Unbounded"));
    assert!(allocator.contains("derived.keys().copied().peekable()"));
    assert!(!startup.contains("fn command_audit_cache_from_segments("));

    let header_dispatch = dispatch
        .split_once("if position == 0")
        .expect("header dispatch")
        .1
        .split_once("if self.structural_cursors.is_none()")
        .expect("header dispatch end")
        .0;
    assert!(header_dispatch.contains("self.ensure_command_cache()?"));
    assert!(header_dispatch.contains("&self.command_audits"));
}

#[test]
fn partial_contract_migration_reopen_is_confined_to_the_witness_gate() {
    let sources = rust_sources();
    assert_eq!(
        sources.matches("into_contract_migration_ports").count(),
        2,
        "the dormant-port bypass must remain one private constructor and one witnessed call"
    );

    let store = production_source(crate_root().join("src/store.rs"));
    assert!(store.contains("pub(crate) fn into_contract_migration_ports"));

    let migration = production_source(crate_root().join("src/migration_stage.rs"));
    assert!(migration.contains("pub fn resume("));
    assert!(migration.contains("RedbContractMigrationImmutableWitness"));
    assert!(migration.contains("store.into_contract_migration_ports()?"));
    assert!(
        migration.contains(
            "immutable_history_digest(&transaction, witness.v3_history)? != witness.digest"
        )
    );
    assert!(migration.contains("stage.immutable_history_digest = witness.digest"));
    assert!(migration.contains("stage.immutable_v3_history = witness.v3_history"));
    let prefix = production_source(crate_root().join("src/migration_stage_history.rs"));
    assert!(prefix.contains("validate_retained_history(transaction)"));
    assert!(prefix.contains(".validate_terminal_receipt(&receipt)"));
    for root in ["lineage", "anchor", "minimum_resume"] {
        assert!(prefix.contains(&format!("current.{root}() != expected.{root}()")));
    }
    assert!(migration.contains("validate_entity_index_structure(&stage.ports)?"));
}

#[test]
fn index_migration_rechecks_replacement_charge_before_staging_any_write() {
    let migration = read(crate_root().join("src/startup/index_migration_backend.rs"));
    let charge_check = migration
        .find("replacement.encoded_content_charge().get()")
        .expect("migration replacement charge check");
    let insert = migration[charge_check..]
        .find("table\n                    .insert(key, replacement.as_bytes())")
        .map(|offset| charge_check + offset)
        .expect("migration replacement insertion");
    assert!(charge_check < insert);
    assert!(
        migration[charge_check..insert]
            .contains("expected.conservative_v2_envelope_charge().get()")
    );
    assert!(migration[charge_check..insert].contains("return Err(invariant())"));
}

#[test]
fn startup_session_holds_at_most_one_structural_read_transaction() {
    // ADR-0073: one read transaction for the structural pass only (savepoint pin).
    // It must be Option-wrapped and cleared when structural finishes; it must not
    // outlive the session.
    let startup = read(crate_root().join("src/startup.rs"));
    let session = startup
        .split_once("pub struct RedbStructuralEvidenceSession {")
        .expect("startup session declaration")
        .1
        .split_once("\n}")
        .expect("startup session body")
        .0;
    assert_eq!(
        session.matches("ReadTransaction").count(),
        1,
        "session body must own at most one snapshot pin"
    );
    assert!(session.contains("validation_read: Option<ReadTransaction>"));
    assert!(!session.contains("transaction: ReadTransaction"));
    assert!(session.contains("durable_commit_epoch: u64"));
    // The completed session drops the pin before its final continuity read.
    assert!(startup.contains("self.structural_finished = true"));
    assert!(
        startup.contains("self.historical_tables = None;\n        self.validation_read = None")
    );
    assert!(startup.contains("StructuralEvidencePage::ExactEnd"));
    assert!(startup.contains("fn open_snapshot_read(&self) -> Result<ReadTransaction"));
}

#[test]
fn every_live_database_engine_commit_routes_through_the_epoch_boundary() {
    let source_dir = crate_root().join("src");
    for entry in fs::read_dir(&source_dir).expect("read source directory") {
        let path = entry.expect("source entry").path();
        if path.extension().is_none_or(|extension| extension != "rs")
            || path.file_name().is_some_and(|name| {
                name.to_string_lossy().ends_with("_tests.rs")
                    || name == "store.rs"
                    // Extracted store-owned graceful-close boundary; its sole
                    // final write commits through `SharedRedb::commit_durable`.
                    || name == "store_graceful_close.rs"
                    // Sole follower frame/ack owner, checked explicitly below.
                    || name == "store_follower.rs"
                    // Derived persistence under that same move-only owner.
                    || name == "store_follower_projection.rs"
                    // The extracted store-owned checkpoint is checked below:
                    // one begin, one commit_durable, no native commit bypass.
                    || name == "store_journal_checkpoint.rs"
                    // Closed private store-child control owner, checked below.
                    || name == "changelog_source_control_transaction.rs"
                    || name == "startup.rs"
                    // Owned dormant activation is checked explicitly below.
                    || name == "startup_v3_activation.rs"
                    || name == "fixtures.rs"
                    || name == "benchmark_support.rs"
                    // Checkpoint write uses SharedRedb::commit_durable (same epoch
                    // boundary as store migration helpers).
                    || name == "validated_prefix.rs"
            })
        {
            continue;
        }
        let compact = without_whitespace(&production_source(&path));
        assert!(
            !compact.contains(".database.begin_write("),
            "live database writes must enter through store/startup epoch boundaries: {}",
            path.display()
        );
    }

    let store = without_whitespace(&production_source(source_dir.join("store.rs")));
    assert_eq!(store.matches("transaction.commit()").count(), 1);
    assert!(store.contains("fncommit_durable("));
    assert!(store.contains("self.shared.commit_durable(transaction)?"));
    assert!(store.contains("self.shared.commit_durable(transaction)"));
    let follower = without_whitespace(&production_source(source_dir.join("store_follower.rs")));
    assert_eq!(
        follower
            .matches("self.shared.database.begin_write()")
            .count(),
        2
    );
    assert_eq!(
        follower
            .matches("self.shared.commit_durable(write)?")
            .count(),
        2
    );
    assert_eq!(follower.matches(".commit()").count(), 0);
    assert_eq!(
        follower
            .matches("set_durability(Durability::Immediate)")
            .count(),
        2
    );
    assert_eq!(
        follower.matches("shared.mutation_gate.acquire()?").count(),
        2
    );
    assert!(!follower.contains("RedbOperationalPorts"));
    assert!(!follower.contains("complete_graceful_close("));
    assert!(!follower.contains("allocate_one("));
    let projection = without_whitespace(&production_source(
        source_dir.join("store_follower_projection.rs"),
    ));
    assert!(projection.contains("implRedbFollowerApplier{"));
    assert_eq!(
        projection
            .matches("self.shared.database.begin_write()")
            .count(),
        1
    );
    assert_eq!(
        projection
            .matches("self.shared.commit_durable(write)?")
            .count(),
        1
    );
    assert_eq!(
        projection
            .matches("shared.mutation_gate.acquire()?")
            .count(),
        1
    );
    assert_eq!(
        projection
            .matches("set_durability(Durability::Immediate)")
            .count(),
        1
    );
    assert_eq!(projection.matches(".commit()").count(), 0);
    assert!(!projection.contains("RedbOperationalPorts"));
    assert!(!projection.contains("allocate_one("));
    let replay = without_whitespace(&production_source(source_dir.join("projection_replay.rs")));
    assert!(!replay.contains("begin_write("));
    assert!(!replay.contains(".commit("));
    assert!(!replay.contains("commit_durable("));
    assert_eq!(replay.matches(".insert(").count(), 2);
    assert!(replay.contains("letmutrows=write.open_table(PROJECTION_STATE)"));
    assert!(replay.contains("write.open_table(PROJECTION_APPLIED).map_err(invalid)?.insert("));
    let recovery = without_whitespace(&production_source(
        source_dir.join("store_follower_recovery.rs"),
    ));
    assert!(!recovery.contains("pubapplier:"));
    assert!(!recovery.contains("Deref"));
    assert!(!recovery.contains("ChangelogFollowerApplyPortV3"));
    assert!(!recovery.contains("begin_write("));
    assert!(!recovery.contains("commit_durable("));
    assert!(!recovery.contains("RedbOperationalPorts"));
    assert!(recovery.contains("letshared=Arc::clone(&self.0.shared)"));
    assert!(recovery.contains("self.session.finish_bootstrap_catalog_preflight(end)?"));
    assert!(recovery.contains("RedbFollowerStore(RedbStore{shared:self.applier.shared"));
    let activation = without_whitespace(&production_source(
        source_dir.join("startup_v3_activation.rs"),
    ));
    assert_eq!(
        activation.matches("shared.database.begin_write()").count(),
        1
    );
    assert_eq!(
        activation
            .matches("shared.commit_durable(transaction)")
            .count(),
        1
    );
    assert_eq!(activation.matches("transaction.commit()").count(), 0);
    assert!(activation.contains("changelog_v3_activation::stage_validated("));
    let checkpoint = without_whitespace(&production_source(
        source_dir.join("store_journal_checkpoint.rs"),
    ));
    assert_eq!(checkpoint.matches("self.database.begin_write()").count(), 1);
    assert_eq!(
        checkpoint
            .matches("self.commit_durable(transaction)")
            .count(),
        1
    );
    assert_eq!(checkpoint.matches("transaction.commit()").count(), 0);
    assert!(checkpoint.contains("set_durability(Durability::Immediate)"));
    let control = without_whitespace(&production_source(
        source_dir.join("changelog_source_control_transaction.rs"),
    ));
    assert_eq!(
        control
            .matches("self.shared.database.begin_write()")
            .count(),
        1
    );
    assert_eq!(control.matches("shared.commit_durable(raw)").count(), 1);
    assert_eq!(control.matches(".commit()").count(), 0);
    assert!(control.contains("set_durability(Durability::Immediate)"));
    assert!(control.contains("shared.mutation_gate.acquire()?"));
    assert!(control.contains("shared.disable_and_fence_fresh_locator_coverage()"));
    let deferred = store
        .split_once("fnapply_unpublished(")
        .expect("closed deferred commit path")
        .1
        .split_once("fninvalidate_transient_indexes(")
        .expect("deferred commit path end")
        .0;
    assert_eq!(deferred.matches("transaction.commit()").count(), 0);
    assert!(deferred.contains("ifself.transaction.take().is_some()"));
    let service_audit = store
        .split_once("fnsubmit_service_audit(")
        .expect("closed deferred service-audit path")
        .1
        .split_once("fninvalidate_transient_indexes(")
        .expect("deferred service-audit path end")
        .0;
    assert_eq!(service_audit.matches("transaction.commit()").count(), 0);
    assert!(service_audit.contains("ifself.transaction.take().is_some()"));
    assert!(!store.contains("set_durability(Durability::None)"));

    let deferred_begin = store
        .split_once("implRedbDurabilityEpoch{")
        .expect("durability epoch implementation")
        .1
        .split_once("pub(crate)fnbegin_write(")
        .expect("deferred writer entry")
        .1
        .split_once("pub(crate)fnseal(")
        .expect("deferred writer entry end")
        .0;
    assert!(deferred_begin.contains("transaction:None"));
    assert!(!deferred_begin.contains("database.begin_write()"));

    let audit_begin = store
        .split_once("pub(crate)fnbegin_deferred_service_audit_write(")
        .expect("deferred service-audit writer entry")
        .1
        .split_once("pub(crate)fnacquire_indexed_read_lease(")
        .expect("deferred service-audit writer entry end")
        .0;
    assert!(audit_begin.contains("transaction:None"));
    assert!(!audit_begin.contains("database.begin_write()"));

    let startup = without_whitespace(&format!(
        "{}\n{}",
        production_source(source_dir.join("startup.rs")),
        production_source(source_dir.join("startup/index_migration_backend.rs")),
    ));
    assert!(!startup.contains("transaction.commit()"));
    assert_eq!(startup.matches("commit_durable(transaction)?").count(), 0);
    assert_eq!(
        startup
            .matches("transaction.finish()?.commit(&self.shared)?")
            .count(),
        1
    );
    let receipt = without_whitespace(&production_source(source_dir.join("changelog_v3_write.rs")));
    let sealed_commit = receipt
        .split_once("pub(crate)fncommit(self,shared:&SharedRedb)")
        .expect("sealed receipt commit owner")
        .1
        .split_once("pub(crate)fncommit_for_test")
        .expect("test-only direct commit boundary")
        .0;
    assert_eq!(
        sealed_commit
            .matches("shared.commit_durable(self.transaction)?")
            .count(),
        1
    );
    assert!(!sealed_commit.contains("transaction.commit()"));
}

#[test]
// req: REP-003, REC-001, STO-012
fn captured_immediate_owner_keeps_one_transaction_and_no_raw_mutation_escape() {
    let source = without_whitespace(&production_source(
        crate_root().join("src/changelog_v3_write.rs"),
    ));
    let captured = source
        .split_once("implCapturedImmediateWrite{")
        .unwrap()
        .1
        .split_once("pub(crate)structPreparedHistoryAdvance")
        .unwrap()
        .0;
    assert_eq!(captured.matches("database.begin_write()").count(), 1);
    let finish = captured.split_once("pub(crate)fnfinish(").unwrap().1;
    assert!(!finish.contains("begin_write("));
    assert!(!finish.contains(".commit("));
    assert!(finish.contains("PreparedImmediateReceipt{transaction:self.transaction,}"));
    assert!(!captured.contains("->&WriteTransaction"));
    assert!(!captured.contains("Result<WriteTransaction"));
    let store = without_whitespace(&production_source(crate_root().join("src/store.rs")));
    assert!(store.contains(
        "typeOperationalWriteTransaction=crate::changelog_v3_write::CapturedImmediateWrite;"
    ));
    let direct = store
        .split_once("pub(crate)fnbegin_attributed_write(")
        .unwrap()
        .1
        .split_once("pub(crate)fnarm_exact_empty_fresh_locator_coverage_for_test(")
        .unwrap()
        .0;
    assert_eq!(direct.matches("database.begin_write()").count(), 1);
    assert!(
        direct.find("apply_published_journal_suffix(").unwrap()
            < direct
                .find("OperationalWriteTransaction::from_drained(")
                .unwrap()
    );
    let commit = store
        .split_once("fncommit_with_observations(")
        .unwrap()
        .1
        .split_once("pub(crate)fnapply_unpublished(")
        .unwrap()
        .0;
    assert!(
        commit.find("transaction.finish()").unwrap()
            < commit.find("prepared.commit(&self.shared)").unwrap()
    );
    assert!(commit.contains("refresh_durable_read_frontier()"));
    assert!(commit.contains("finish_journal_checkpoint(runtime)"));
    assert!(commit.contains("state.apply_delta(delta)"));
    let capture = without_whitespace(&production_source(
        crate_root().join("src/changelog_v3_capture.rs"),
    ));
    assert!(!capture.contains("DerefMutfor"));
    assert!(!capture.contains("pub(crate)fntable_mut("));
    assert!(capture.contains("self.capture.record("));
}

#[test]
fn operational_transaction_helpers_use_the_store_owned_transaction_type() {
    for module in [
        "application.rs",
        "administration.rs",
        "consumer.rs",
        "columnar_projection_control.rs",
        "migration_stage.rs",
    ] {
        let source = production_source(crate_root().join("src").join(module));
        assert!(
            source.contains("OperationalWriteTransaction"),
            "{module} must use the single store-owned transaction type"
        );
        assert!(
            !source.contains("redb::WriteTransaction"),
            "{module} must not pin operational helpers to the raw engine transaction"
        );
    }
}

#[test]
// req: REP-003, STO-012
fn direct_operation_owners_name_closed_changelog_attributions_before_admission() {
    for (module, expected) in [
        ("application.rs", "DirectApplicationOrServiceAuditGroup"),
        ("administration.rs", "CapabilityAdministration"),
        ("consumer.rs", "EventConsumerTransition"),
        ("derived.rs", "OutboxTransition"),
        (
            "columnar_projection_control.rs",
            "ColumnarProjectionControl",
        ),
        (
            "application_installation.rs",
            "ApplicationInstallationCampaign",
        ),
        ("application_export.rs", "ApplicationExportOperation"),
        ("migration_stage.rs", "ContractMigrationCutover"),
    ] {
        let source = production_source(crate_root().join("src").join(module));
        assert!(
            source.contains("begin_attributed_write("),
            "{module} must name its operation before writer admission"
        );
        assert!(source.contains(&format!("ChangelogAttributionV3::{expected}")));
    }
}

#[test]
fn deferred_epoch_proves_one_private_frontier_before_durability() {
    let store = without_whitespace(&production_source(crate_root().join("src/store.rs")));
    let record = store
        .split_once("pub(crate)fnrecord_journal_mutations(")
        .expect("journal mutation fanout")
        .1
        .split_once("pub(crate)fntransaction(")
        .expect("journal mutation fanout end")
        .0;
    assert!(record.contains("formutationin&mutations{stage.apply(mutation)?;"));
    assert!(record.contains(".extend(mutations)"));

    let epoch = store
        .split_once("implRedbDurabilityEpoch{")
        .expect("durability epoch")
        .1;
    let equivalence = epoch
        .split_once("fnprivate_frontier_is_equivalent(&self)->bool{")
        .expect("frontier equivalence proof")
        .1
        .split_once("fnhas_unpublished_state(")
        .expect("frontier equivalence proof end")
        .0;
    for required in [
        "prior.checked_next()!=Some(batch.first_commit_sequence())",
        "command_count==self.command_count",
        "prior_last==self.last_sequence",
        "self.journal_mutation_groups==self.applied.len()",
        "Some(stage.mutation_count())",
    ] {
        assert!(equivalence.contains(required), "missing `{required}`");
    }
    let seal = epoch
        .split_once("pub(crate)fnseal(mutself)")
        .expect("epoch seal")
        .1
        .split_once("implDropforRedbDurabilityEpoch")
        .expect("epoch seal end")
        .0;
    assert!(seal.contains("if!self.private_frontier_is_equivalent(){"));
    assert!(seal.contains("self.shared.fence_writes();"));
}

#[test]
fn sealed_command_audit_evidence_is_confined_to_post_staging_application_commits() {
    let source_dir = crate_root().join("src");
    let application = without_whitespace(&production_source(source_dir.join("application.rs")));
    let stage = application
        .split_once("fnstage(self,mutrecords:AtomicCommandRecordSet,)")
        .expect("command stage typestate")
        .1
        .split_once("impl_candidate_chain!(RedbEmptyBatch)")
        .expect("command stage end")
        .0;
    let physical_insert = stage
        .find("apply_record_set(&mutcore,&records,encoded)?")
        .expect("complete graph physical insert");
    let sealed_evidence = stage
        .find("core.staged.push(records.into_staged_evidence())")
        .expect("sealed post-staging evidence");
    assert!(physical_insert < sealed_evidence);
    assert_eq!(
        application
            .matches("stage_command_service_audit_group_in_write(")
            .count(),
        2,
        "only direct and deferred successful command commits use sealed links"
    );

    let administration =
        without_whitespace(&production_source(source_dir.join("administration.rs")));
    let sealed_entry = administration
        .split_once("fnstage_command_service_audit_group_in_write(")
        .expect("sealed audit entry")
        .1
        .split_once("fnstage_service_audit_group_with_command_evidence(")
        .expect("sealed audit entry end")
        .0;
    assert!(!sealed_entry.contains("decode_commit_with_event_table"));
    assert!(!sealed_entry.contains("decode_provenance_record_v1"));

    let all_sources = without_whitespace(&rust_sources());
    assert_eq!(
        all_sources
            .matches("stage_command_service_audit_group_in_write(")
            .count(),
        3,
        "one definition and exactly two application call sites"
    );
}

#[test]
fn administration_writes_preserve_a_startup_proof_without_history_rescans() {
    let administration = without_whitespace(&production_source(
        crate_root().join("src/administration.rs"),
    ));
    let tail = administration
        .split_once("fnvalidate_administration_tail(")
        .expect("tail validator")
        .1
        .split_once("fnvalidate_administration_tail_readonly(")
        .expect("tail validator end")
        .0;
    assert!(tail.contains(".last()"));
    assert!(tail.contains("access.command_audit_record(expected_last)"));
    assert!(!tail.contains(".iter()"));

    let readonly_tail = administration
        .split_once("fnvalidate_administration_tail_readonly(")
        .expect("read-only tail validator")
        .1
        .split_once("fnvalidate_administration_stream_readonly(")
        .expect("read-only tail validator end")
        .0;
    assert!(readonly_tail.contains(".last()"));
    assert!(readonly_tail.contains("ports.indexed_command_audit(expected_last)"));
    assert!(!readonly_tail.contains(".iter()"));

    let full_read = administration
        .split_once("fnvalidate_administration_stream_readonly(")
        .expect("read validator")
        .1
        .split_once("fnallocate_sequences(")
        .expect("read validator end")
        .0;
    assert!(full_read.contains("read_administration_record_readonly(ports,transaction,current)"));
    assert!(full_read.contains("current.checked_next()"));

    let append = administration
        .split_once("implServiceAuditAppendRepositoryforRedbOperationalPorts")
        .expect("service audit repository")
        .1
        .split_once("fnprincipal_matches_observation(")
        .expect("service audit repository end")
        .0;
    assert!(append.contains("validate_administration_tail(&access)?"));
    assert!(!append.contains("validate_administration_table"));
}

#[test]
fn pipelined_writers_resolve_command_audits_from_the_unpublished_exact_index() {
    let source_dir = crate_root().join("src");
    let store = without_whitespace(&production_source(source_dir.join("store.rs")));
    let lookup = store
        .split_once("pub(crate)fncommand_audit_record(")
        .expect("command audit lookup")
        .1
        .split_once("pub(crate)constfnretains_journal_mutations(")
        .expect("command audit lookup end")
        .0;
    assert!(lookup.contains("unpublished_command_indexes"));
    assert!(lookup.contains(".command_audit_record(sequence)"));

    let administration =
        without_whitespace(&production_source(source_dir.join("administration.rs")));
    let tail = administration
        .split_once("fnvalidate_administration_tail(")
        .expect("tail validator")
        .1
        .split_once("fncommand_audit_at_transaction_tail")
        .expect("tail fallback")
        .0;
    let exact = tail
        .find("access.command_audit_record(expected_last)?")
        .expect("exact unpublished-aware audit lookup");
    let physical = tail
        .find(".read_command_value(JournalTable::Audit,key.as_slice())?")
        .expect("composite physical audit lookup");
    assert!(exact < physical);
}

#[test]
fn the_checkpoint_builder_fixture_reads_row_counts_and_counts_every_terminal_failure_once() {
    let source_dir = crate_root().join("src");
    let checkpoint = without_whitespace(&production_source(source_dir.join("validated_prefix.rs")));
    // The write path selects the metadata-derived counts; nothing in it selects
    // the reference walk, whose only production role is the guarded fallback.
    let write = checkpoint
        .split_once("fnwrite_validated_prefix_checkpoint(")
        .expect("checkpoint write")
        .1
        .split_once("fnbuild_checkpoint_from_snapshot(")
        .expect("checkpoint write end")
        .0;
    assert!(write.contains("CheckpointCountSource::DurableLengths{"));
    assert!(!write.contains("CheckpointCountSource::Walked"));
    // Every count class must be derivable without a pass: the derivation reads
    // table row counts and the census only.
    let derived = checkpoint
        .split_once("fncounts_from_durable_lengths(")
        .expect("count derivation")
        .1
        .split_once("fnevent_keyed_table_ends_at_or_below(")
        .expect("count derivation end")
        .0;
    assert!(!derived.contains(".iter()"));
    assert_eq!(derived.matches("table_row_count(transaction,").count(), 8);
    assert!(derived.contains("checked_sub(execution_failed_rows)"));

    // One writer of a terminal ExecutionFailed row, and it commits through the
    // one path that censuses it. A second uncounted writer would drift the
    // derived idempotency count and make the NEXT open refuse.
    let application = without_whitespace(&production_source(source_dir.join("application.rs")));
    assert_eq!(
        application
            .matches("encode_execution_failed_v1(&terminal)")
            .count(),
        1
    );
    assert_eq!(application.matches("commit_execution_failure()").count(), 1);
    let store = without_whitespace(&production_source(source_dir.join("store.rs")));
    assert_eq!(
        store
            .matches("note_terminal_execution_failure_row()")
            .count(),
        1
    );
    // The census is seeded exactly where the checkpoint write gate opens, so a
    // checkpoint can never be built from an unseeded census.
    let startup = without_whitespace(&production_source(source_dir.join("startup.rs")));
    assert!(startup.contains(
        "seed_terminal_execution_failure_rows(self.terminal_execution_failure_rows);\
         self.shared.set_startup_validation_clean(true);"
    ));
}

#[test]
fn an_exact_current_checkpoint_returns_before_opening_a_write_transaction() {
    let source_dir = crate_root().join("src");
    let checkpoint = without_whitespace(&production_source(source_dir.join("validated_prefix.rs")));
    let write = checkpoint
        .split_once("fnwrite_validated_prefix_checkpoint(")
        .expect("checkpoint write")
        .1
        .split_once("///ReturnstrueonlywhenthealreadyvalidatedV2proof")
        .expect("checkpoint write end")
        .0;
    let exact = write
        .find("exact_current_checkpoint_exists(")
        .expect("exact-current decision");
    assert!(write[..exact].contains("purpose==CheckpointPurpose::TestFixture&&"));
    let early_return = write[exact..]
        .find("returnOk(())")
        .map(|offset| exact + offset)
        .expect("exact-current early return");
    assert!(exact < early_return);
    // Both mutually exclusive owners must remain after the exact no-op: the
    // V3 sealed owner opens its one transaction, or the inactive raw lane does.
    for begin in [
        "PreparedImmediateReceipt::apply(",
        "shared.database.begin_write()",
    ] {
        let begin_write = write.find(begin).expect("checkpoint write transaction");
        let commit_hook = write[begin_write..]
            .find("shared.before_test_commit(RedbTestOperation::ValidatedPrefixCheckpoint)")
            .map(|offset| begin_write + offset)
            .expect("this lane's checkpoint commit hook");
        assert!(early_return < begin_write);
        assert!(begin_write < commit_hook);
    }
}

#[test]
// req: REP-003, REC-001, STO-012
fn v3_prefix_checkpoint_plans_from_its_builder_pin_and_uses_one_sealed_commit() {
    let source = without_whitespace(&production_source(
        crate_root().join("src/validated_prefix.rs"),
    ));
    let writer = source
        .split_once("pub(crate)fnwrite_validated_prefix_checkpoint(")
        .unwrap()
        .1
        .split_once("fnexact_current_checkpoint_exists(")
        .unwrap()
        .0;
    assert!(
        writer
            .find("build_checkpoint_from_snapshot(&transaction")
            .unwrap()
            < writer
                .find("plan_checkpoint_receipt(&transaction,&checkpoint)")
                .unwrap()
    );
    assert!(
        writer
            .find("plan_checkpoint_receipt(&transaction,&checkpoint)")
            .unwrap()
            < writer.find("drop(transaction)").unwrap()
    );
    let active = writer
        .split_once("ifv3{ifletSome(receipt)=receipt{")
        .unwrap()
        .1
        .split_once("letencoded=")
        .unwrap()
        .0;
    assert_eq!(
        active.matches("PreparedImmediateReceipt::apply(").count(),
        1
    );
    assert!(active.contains("RedbCommitProfile::Hardened"));
    assert_eq!(active.matches("prepared.commit(shared)").count(), 1);
    assert!(!active.contains("begin_write("));
    assert!(active.contains("before_test_commit(RedbTestOperation::ValidatedPrefixCheckpoint)"));
    assert!(active.contains("after_test_commit(RedbTestOperation::ValidatedPrefixCheckpoint)"));
    assert!(active.contains("returnOk(())"));
    let owner = without_whitespace(&production_source(
        crate_root().join("src/changelog_v3_write.rs"),
    ));
    let apply = owner
        .split_once("implPreparedImmediateReceipt{")
        .unwrap()
        .1
        .split_once("pub(crate)fncommit(")
        .unwrap()
        .0;
    assert_eq!(apply.matches("database.begin_write()").count(), 1);
    assert!(
        apply
            .find("check_predecessor(&transaction,mutation)")
            .unwrap()
            < apply.find("apply_mutation(&transaction,mutation)").unwrap()
    );
    assert!(!apply.contains(".commit("));
}

#[test]
fn validated_prefix_entity_heads_are_anchored_at_s_and_advanced_through_the_suffix() {
    let source_dir = crate_root().join("src");
    let checkpoint = without_whitespace(&production_source(source_dir.join("validated_prefix.rs")));
    let write = checkpoint
        .split_once("fnwrite_validated_prefix_checkpoint(")
        .expect("checkpoint write")
        .1
        .split_once("fnbuild_checkpoint_from_snapshot(")
        .expect("checkpoint write end")
        .0;
    let snapshot = write
        .find("replace_checkpoint_entity_heads(&write,&checkpoint)?")
        .expect("atomic checkpoint-head snapshot");
    let publish = write
        .find("meta.insert(META_VALIDATED_PREFIX_CHECKPOINT")
        .expect("checkpoint publication");
    assert!(
        snapshot < publish,
        "head snapshot must precede meta publication"
    );

    let load = checkpoint
        .split_once("fnload_active_checkpoint(")
        .expect("checkpoint load")
        .1
        .split_once("fnsample_window_starts(")
        .expect("checkpoint load end")
        .0;
    assert!(load.contains("load_checkpoint_entity_heads(transaction,&checkpoint_v2)?"));
    assert!(
        !load.contains("entity_transition_proof(transaction)"),
        "an at-S checkpoint must never be compared directly with current heads"
    );

    let startup = without_whitespace(&production_source(source_dir.join("startup.rs")));
    let chains = startup
        .split_once("fnbuild_entity_chains(")
        .expect("entity chain builder")
        .1
        .split_once("fnapply_entity_transitions_to_startup_chains(")
        .expect("entity chain builder end")
        .0;
    assert!(chains.contains("transition_heads.insert(target,head)"));
    assert!(chains.contains("Excluded(lower.as_slice())"));
}

#[test]
fn live_migration_preflight_reads_the_published_overlay_not_the_checkpoint_alone() {
    // ADR-0104 section 2: every operational scan and point read of a table
    // named by `JournalTable` captures one published checkpoint-plus-overlay
    // view. `RedbReadAccess` still derefs to the checkpoint root, so opening
    // `ENTITIES` or `IDEMPOTENCY_PENDING` on a read access reads the
    // checkpoint alone and silently omits every transition that is durable in
    // the journal suffix but not yet checkpointed.
    //
    // Contract-migration preflight is the fail-closed gate over predecessor
    // rows. Reading it from the checkpoint alone lets a migration be admitted
    // against rows it never inspected, which is corruption acceptance rather
    // than a stale read. ADR-0104 section 1 routes every standard-profile
    // command, including an idle singleton, through that suffix, so the
    // omission is the ordinary case and not a rare race.
    let migration_stage = without_whitespace(&production_source(
        crate_root().join("src/migration_stage.rs"),
    ));

    let scan = migration_stage
        .split_once("fnscan_rows(")
        .expect("preflight row scan")
        .1
        .split_once("fntarget_exists(")
        .expect("preflight row scan end")
        .0;
    assert!(scan.contains("ports.begin_composite_read()"));
    assert!(scan.contains("read_range_to(JournalTable::Entities,"));
    assert!(
        !scan.contains("open_table(ENTITIES)"),
        "a checkpoint-only entity scan cannot refuse an invalid predecessor that is still journal-suffix durable"
    );

    let exists = migration_stage
        .split_once("fntarget_exists(")
        .expect("preflight target probe")
        .1
        .split_once("fnhas_pending_admissions(")
        .expect("preflight target probe end")
        .0;
    assert!(exists.contains("ports.begin_composite_read()"));
    assert!(exists.contains("read_value(JournalTable::Entities,"));
    assert!(!exists.contains("open_table(ENTITIES)"));

    let admissions = migration_stage
        .split_once("fnhas_pending_admissions(")
        .expect("preflight admission probe")
        .1
        .split_once("fnread_active(")
        .expect("preflight admission probe end")
        .0;
    assert!(admissions.contains("ports.begin_composite_read()"));
    assert!(admissions.contains("read_range_to(JournalTable::IdempotencyPending,"));
    assert!(
        !admissions.contains("open_table(IDEMPOTENCY_PENDING)"),
        "an unresolved admission that is only journal-suffix durable must still hold the migration closed"
    );

    // The open-ended overlay merge these reads depend on must keep merging the
    // overlay when the upper bound is absent; a checkpoint-only fallback there
    // would reintroduce the same omission one level down.
    let store = without_whitespace(&production_source(crate_root().join("src/store.rs")));
    let range = store
        .split_once("pub(crate)fnread_range_to(")
        .expect("open-ended overlay range")
        .1
        .split_once("pub(crate)fnapplication_frontier(")
        .expect("open-ended overlay range end")
        .0;
    assert!(range.contains("Self::Composite(view)=self"));
    assert!(range.contains("merge_bounded(table.composite(),start_inclusive,end_exclusive,"));
}

#[test]
// req: REP-003, REC-001
fn legacy_changelog_emitters_have_no_production_exports() {
    let exports = production_source(crate_root().join("src/lib.rs"));
    let production = production_source(crate_root().join("src/changelog.rs"));
    for name in [
        "start_changelog_emitter",
        "RedbChangelogEmitter",
        "derive_frame",
    ] {
        assert!(
            !exports.contains(name),
            "legacy emitter must not be a production export: {name}"
        );
        assert!(
            !production.contains(name),
            "legacy framing belongs only to compatibility tests: {name}"
        );
    }
    assert!(production.contains("changelog_receipts_v3"));
}

#[test]
// req: REP-003, REC-001, PERF-007, STO-012
fn source_controls_are_crate_private_closed_barrier_owners_without_application_exports() {
    let source = production_source(crate_root().join("src/store_changelog_source_control.rs"));
    let transaction =
        production_source(crate_root().join("src/changelog_source_control_transaction.rs"));
    let pruning = production_source(crate_root().join("src/changelog_source_control_retention.rs"));
    let attachment = production_source(crate_root().join("src/changelog_bootstrap_attachment.rs"));
    let exports = production_source(crate_root().join("src/lib.rs"));
    assert!(source.contains("pub(crate) struct ReplicationSourceControl"));
    assert!(!exports.contains("ReplicationSourceControl"));
    for name in [
        "replication_source_control",
        "register",
        "advance_acknowledgement",
        "reclaim_history",
    ] {
        assert!(source.contains(&format!("pub(crate) fn {name}(")));
        assert!(!source.contains(&format!("pub fn {name}(")));
    }
    for check in [
        "previous.durable_epoch >= current.durable_epoch",
        "previous.publication == current.publication",
        "Kind::Bootstrap",
        "self.observed = Some(write.commit()?)",
    ] {
        assert!(source.contains(check));
    }
    for owner in [
        "mutation_gate.acquire()",
        "checkpoint_published_journal_suffix_for_barrier()",
        "shared.commit_durable(raw)",
        "refresh_durable_read_frontier()",
        "abort_fresh_locator_rebase",
        "finish_fresh_locator_rebase",
        "validate_retained_history_for_write",
        "CleanCloseState::Clean(_)",
    ] {
        assert!(transaction.contains(owner));
    }
    assert!(pruning.contains("MAX_RECLAIMED_RECEIPTS: u64 = 256"));
    assert!(pruning.contains("maximum.min(hold.fence().sequence().get())"));
    assert!(!pruning.contains("CapturedImmediateWrite"));
    assert!(!pruning.contains("open_table(ENTITIES)"));
    assert!(!source.contains("fn remove"));
    assert!(attachment.contains("pub(crate) fn attach_bootstrap("));
    assert!(!attachment.contains("pub fn attach_bootstrap("));
    assert!(!attachment.contains("fn remove"));
    assert!(!attachment.contains("CapturedImmediateWrite"));
    assert!(!attachment.contains("open_table(ENTITIES)"));
}

#[test]
// req: REP-003, REC-001
fn the_changelog_emitter_can_only_read_published_durable_snapshots() {
    // ADR-0100 §2 made structural. The emitter derives frames from published
    // durable state only; this pin proves the module has no other way to read.
    // The `RedbPublishedSnapshot` adapter at the top of the file is the single
    // bridge, and it is constructed only at a publication site.
    let emitter = production_source(crate_root().join("src/changelog.rs"));
    let adapter = emitter
        .split_once("impl PublishedDurableSnapshot for RedbPublishedSnapshot")
        .expect("the published-snapshot adapter opens the module")
        .1;
    let compatibility = std::fs::read_to_string(
        crate_root().join("../../tests/storage_recovery/changelog_compatibility.rs"),
    )
    .expect("the retained legacy compatibility emitter");
    assert!(compatibility.contains("#![cfg(test)]"));
    let derivation = compatibility
        .split_once("enum EmitterMessage")
        .expect("legacy derivation begins after the canonical test codec imports")
        .1;

    for forbidden in [
        // Writer-private state and the roots that carry it.
        "private_composite_frontier",
        "unpublished_command_indexes",
        "durable_read_frontier",
        "composite_publication",
        "SharedRedb",
        "RedbDurabilityEpoch",
        "RedbWriteAccess",
        // Any way to open a read of its own, rather than being handed one.
        "begin_read",
        "begin_write",
        "begin_operational_read",
        "begin_composite_operational_read",
        "open_table",
        "ReadTransaction",
        // Journal bytes and composite-overlay internals are not a wire format
        // (ADR-0101 §6, ADR-0104 §8).
        "JournalFrame",
        "EncodedJournalFrame",
        "encoded_frame_bytes",
        "CompositeMutationV1",
        "FrozenCompositeOverlay",
        "RedbCompositeReadView",
        "overlay(",
    ] {
        assert!(
            !adapter.contains(forbidden),
            "the production snapshot adapter must not reach `{forbidden}`"
        );
        assert!(
            !derivation.contains(forbidden),
            "the changelog derivation must not reach `{forbidden}`"
        );
    }

    // The gate is present and fails closed on anything but the exact frontier.
    assert!(derivation.contains("fn assert_snapshot_is_the_published_frontier"));
    assert!(derivation.contains("ChangelogResyncReasonV1::UnpublishedStateVisible"));
    assert!(derivation.contains("fn derive_frame"));

    // Every publication site hands over the snapshot it just installed, after
    // the swap, and no site hands over a writer-private root.
    let store = production_source(crate_root().join("src/store.rs"));
    assert_eq!(
        store.matches("observe_changelog_publication(").count(),
        4,
        "one definition plus exactly three publication sites"
    );
    let observer = store
        .split_once("fn observe_changelog_publication")
        .expect("the publication observer helper")
        .1
        .split_once("\n    }")
        .expect("observer helper end")
        .0;
    assert!(observer.contains("RedbPublishedSnapshot::new"));
    assert!(!observer.contains("private_composite_frontier"));
    for site in ["fn publish_direct", "fn publish_fenced"] {
        let body = store.split(site).nth(1).expect("publication site");
        assert!(
            !body
                .split("observe_changelog_publication")
                .next()
                .expect("prefix before the observation")
                .contains("private_composite_frontier"),
            "{site} must never hand a writer-private root to the observer"
        );
    }
}

#[test]
// req: REP-003, REC-001, STO-012
fn v3_restore_reset_uses_original_hardened_transaction_and_catalogued_local_state() {
    let backup = production_source(crate_root().join("src/backup.rs"));
    let stamp = backup
        .split_once("pub fn stamp_history_incarnation(")
        .unwrap()
        .1
        .split_once("pub(crate) fn stamp_retention_watermark(")
        .unwrap()
        .0;
    assert_eq!(stamp.matches("begin_write(").count(), 1);
    assert_eq!(stamp.matches("transaction.commit()").count(), 1);
    assert!(!stamp.contains("begin_read("));
    assert!(stamp.contains("set_two_phase_commit(true)"));
    assert!(stamp.contains("Durability::Immediate"));
    assert!(
        stamp.find("PreparedRestoreAnchor::prepare").unwrap()
            < stamp.find("if current == Some(incarnation)").unwrap()
    );
    assert!(
        stamp.find("PreparedRestoreAnchor::prepare").unwrap() < stamp.find("meta.insert(").unwrap()
    );
    assert!(
        stamp.find("anchor.stage(&transaction)").unwrap()
            < stamp.find("transaction.commit()").unwrap()
    );
    let reset = production_source(crate_root().join("src/backup_v3.rs"));
    for forbidden in ["begin_read(", "begin_write(", ".commit(", "Database::open"] {
        assert!(!reset.contains(forbidden));
    }
    assert!(reset.contains("validate_retained_history_for_write(transaction)"));
    assert!(reset.contains("delete_table(HISTORY)"));
    assert!(reset.contains("delete_table(SOURCE_HOLDS)"));
    assert_eq!(reset.matches("delete_table(").count(), 2);
    assert!(reset.contains("ReplicationTransferV1::SourceOnly"));
    assert!(reset.contains("ChangelogAttributionV3::RestoreAnchor"));
}

#[test]
// req: REP-003, PERF-007, REC-001
fn v3_receipt_cursor_has_no_writer_or_journal_io_capability() {
    let source = production_source(crate_root().join("src/changelog_v3_cursor.rs"));
    for forbidden in [
        "SharedRedb",
        "begin_read(",
        "begin_write(",
        "mutation_gate",
        "journal_runtime",
        "JournalLane",
        "std::fs",
        "std::net",
        "commit_durable(",
    ] {
        assert!(
            !source.contains(forbidden),
            "published receipt cursor reaches {forbidden}"
        );
    }
    assert!(source.contains("root: Arc<CheckpointRoot>"));
    assert!(source.contains("mutations: Arc<[CompositeMutationV1]>"));
    assert!(source.contains("MAX_JOURNAL_SUFFIX_TRANSITIONS"));
    assert!(source.contains("MAX_JOURNAL_SUFFIX_BYTES"));
    assert!(source.contains("Arc::try_unwrap"));
    let store = production_source(crate_root().join("src/store.rs"));
    assert_eq!(store.matches("append_changelog_source(").count(), 2);
    assert_eq!(
        store
            .matches("let checkpoint_mutations: Arc<[riffdb_storage_api::CompositeMutationV1]>")
            .count(),
        2
    );
}

#[test]
// req: REP-003, PERF-007
fn v3_authority_state_cursor_keeps_only_the_catalog_and_immutable_publication_pin() {
    let source = production_source(crate_root().join("src/changelog_v3_state.rs"));
    for forbidden in [
        "SharedRedb",
        "begin_read(",
        "begin_write(",
        "mutation_gate",
        "journal_runtime",
        "JournalLane",
        "std::fs",
        "std::net",
        "commit_durable(",
    ] {
        assert!(
            !source.contains(forbidden),
            "authority state cursor reaches {forbidden}"
        );
    }
    assert!(source.contains("N::ALL"));
    assert!(source.contains("root: Arc<CheckpointRoot>"));
    assert!(source.contains("view: Option<Arc<RedbCompositeReadView>>"));
    assert!(source.contains("MAX_COMPOSITE_OVERLAY_TRANSITIONS"));
    assert!(source.contains("Class::ReplicatedAuthoritative"));
    assert!(source.contains("EndNamespace"));
    assert!(source.contains("failure: Option<ChangelogCursorErrorV3>"));
}

#[test]
// req: REP-003, REC-001, STO-012
fn watermark_receipt_validates_before_noop_and_seals_the_original_hardened_transaction() {
    let backup = production_source(crate_root().join("src/backup.rs"));
    let stamp = backup
        .split_once("pub(crate) fn stamp_retention_watermark(")
        .unwrap()
        .1
        .split_once("pub(crate) fn apply_restored_retention_watermark(")
        .unwrap()
        .0;
    assert_eq!(stamp.matches("begin_write(").count(), 1);
    assert_eq!(stamp.matches("transaction.commit()").count(), 1);
    assert!(!stamp.contains("begin_read("));
    assert!(stamp.contains("set_two_phase_commit(true)"));
    assert!(stamp.contains("Durability::Immediate"));
    assert!(
        stamp.find("watermark_history(&transaction)").unwrap()
            < stamp.find("transaction.abort()").unwrap()
    );
    assert!(
        stamp
            .find("prepare_watermark_receipt(&transaction")
            .unwrap()
            < stamp.find("meta.insert(").unwrap()
    );
    assert!(
        stamp.find("receipt.stage(&transaction)").unwrap()
            < stamp.find("transaction.commit()").unwrap()
    );
    let helper = production_source(crate_root().join("src/backup_v3.rs"));
    let watermark = helper
        .split_once("pub(super) fn watermark_history(")
        .unwrap()
        .1;
    for forbidden in [
        "begin_read(",
        "begin_write(",
        ".commit(",
        "Database::open",
        "delete_table(",
    ] {
        assert!(!watermark.contains(forbidden));
    }
    assert!(watermark.contains("validate_retained_history_for_write(transaction)"));
    assert!(watermark.contains("AuthoritativeMutationV3::replace("));
    assert!(watermark.contains("ChangelogAttributionV3::RetentionPrune"));
}

#[test]
fn offline_retention_mutations_require_the_journal_rebase_witness() {
    let retention = without_whitespace(&production_source(crate_root().join("src/retention.rs")));
    assert_eq!(
        retention.matches("prepare_offline_retention()?").count(),
        4,
        "hold add/remove, projection administration, and prune each require the internal witness"
    );
    let prune = retention
        .split_once("pubfnprune_to(")
        .expect("offline prune implementation")
        .1
        .split_once("fnbefore_commit(")
        .expect("offline prune end")
        .0;
    let prepare = prune
        .find("prepare_offline_retention()?")
        .expect("prune preparation");
    let first_write = prune
        .find("begin_durable_write(database)?")
        .expect("checkpoint deletion write");
    assert!(
        prepare < first_write,
        "the journal witness must exist before checkpoint deletion or any prune write"
    );
    assert!(
        !prune.contains("Database::open(&self.database_path)"),
        "prune must not bypass ordinary journal recovery with a direct redb open"
    );
}

#[test]
// req: REP-003, REC-001, STO-012
fn v3_prune_captures_both_original_transactions_and_streams_the_same_tombstone_pin() {
    let retention = without_whitespace(&production_source(crate_root().join("src/retention.rs")));
    let prune = retention
        .split_once("pubfnprune_to(")
        .unwrap()
        .1
        .split_once("fnbefore_commit(")
        .unwrap()
        .0;
    assert_eq!(prune.matches("begin_durable_write(database)?").count(), 2);
    assert_eq!(prune.matches("WriteTransaction::from_drained(").count(), 2);
    assert_eq!(
        prune
            .matches("ChangelogAttributionV3::RetentionPrune")
            .count(),
        2
    );
    assert_eq!(prune.matches("letwrite=write.finish()?").count(), 2);
    assert_eq!(prune.matches("write.commit(&store.shared)?").count(), 2);
    assert!(!prune.contains("commit_durable("));
    let digest = retention
        .split_once("fndigest_and_count_range(")
        .unwrap()
        .1
        .split_once("enumTombstoneSink")
        .unwrap()
        .0;
    assert_eq!(
        digest
            .matches("walk_tombstone_preimage(write,first,last,")
            .count(),
        2
    );
    assert!(digest.contains("ContentHasher::new(HashDomain::Schema,length)"));
    assert!(!digest.contains("Vec::new()"));
    assert!(!digest.contains("begin_read("));
    assert!(!digest.contains("begin_write("));
    let plans = retention
        .split_once("fnrewrite_pruned_command_segments(")
        .unwrap()
        .1
        .split_once("pub(crate)fnverify_tombstone_chain(")
        .unwrap()
        .0;
    assert_eq!(
        plans.matches("RetainedPrunePlanBudget::default()").count(),
        2
    );
    assert_eq!(plans.matches("budget.charge(").count(), 5);
}

#[test]
// req: REP-003, REC-001, STO-012
fn offline_hold_receipts_seal_the_original_prepared_transaction_once() {
    let retention = without_whitespace(&production_source(crate_root().join("src/retention.rs")));
    for (start, end) in [
        ("pubfnadd_hold(", "pubfnremove_hold("),
        ("pubfnremove_hold(", "pubfndetach_projection("),
        ("fnadminister_projection_hold(", "pubfnprune_to("),
    ] {
        let body = retention
            .split_once(start)
            .unwrap()
            .1
            .split_once(end)
            .unwrap()
            .0;
        let prepare = body.find("prepare_offline_retention()?").unwrap();
        let begin = body
            .find("OperationalWriteTransaction::from_drained(")
            .unwrap();
        let mutation = body.find("write.open_table(META)").unwrap();
        let before = body
            .find("self.before_commit(RedbTestOperation::RetentionHold)?")
            .unwrap();
        let commit = body.find("write.finish()?.commit(&store.shared)?").unwrap();
        let after = body
            .find("self.after_commit(RedbTestOperation::RetentionHold)")
            .unwrap();
        assert!(
            prepare < begin
                && begin < mutation
                && mutation < before
                && before < commit
                && commit < after
        );
        assert_eq!(body.matches("begin_durable_write(").count(), 1);
        assert_eq!(body.matches(".commit(").count(), 1);
        assert!(!body.contains("commit_durable(write)"));
        assert!(body.contains("ChangelogAttributionV3::RetentionHold"));
    }
    let store = without_whitespace(&production_source(crate_root().join("src/store.rs")));
    let prepare = store
        .split_once("pub(crate)fnprepare_offline_retention(")
        .unwrap()
        .1
        .split_once("pub(crate)fninto_contract_migration_ports(")
        .unwrap()
        .0;
    assert!(prepare.contains("validate_retained_history(&transaction)"));
    assert!(!prepare.contains("begin_write("));
}

#[test]
fn entity_cache_preserves_exact_observed_bytes_for_journal_before_images() {
    let source = production_source(crate_root().join("src/application.rs"));
    assert!(source.contains("entity_observation_bytes: BTreeMap<EntityTarget, Option<Vec<u8>>>"));
    assert!(source.contains(
        "let (observation, encoded) = entity_observation_from_access(&core.access, target)?;"
    ));
    assert!(
        source.contains(".entity_observation_bytes\n        .insert(target.clone(), encoded);")
    );
    assert!(source.contains("let proven_current = observation_bytes\n            .get(target)"));
    assert!(source.contains("observation_bytes.insert(target.clone(), next_bytes);"));
    assert!(
        !source.contains("encode_entity_record_v1(record).map_err(codec_error)?"),
        "the before-image must remain the exact bytes observed from storage, including legacy envelopes"
    );
}

#[test]
fn proof_skipping_is_confined_to_typed_redb_command_staging() {
    let application = production_source(crate_root().join("src/application.rs"));
    let composite = production_source(crate_root().join("src/composite_view.rs"));
    let store = production_source(crate_root().join("src/store.rs"));
    assert!(application.contains("put_proven_command_value"));
    assert!(application.contains("delete_proven_command_value"));
    assert!(composite.contains("apply_with_proven_current"));
    assert!(store.contains("apply_with_proven_current"));

    for relative in ["src/recovery.rs", "src/startup.rs"] {
        let path = crate_root().join(relative);
        if path.exists() {
            let candidate = production_source(path);
            assert!(
                !candidate.contains("apply_with_proven_current"),
                "recovery and startup must retain the fully validating boundary"
            );
        }
    }
}

#[test]
fn checkpoint_pay_once_apply_is_confined_to_live_same_process_materialization() {
    let journal = production_source(crate_root().join("src/journal.rs"));
    let store = production_source(crate_root().join("src/store.rs"));
    let checkpoint = production_source(crate_root().join("src/store_journal_checkpoint.rs"));
    assert!(journal.contains("fn apply_validated_composite_mutation("));
    assert_eq!(
        format!("{store}\n{checkpoint}")
            .matches("apply_validated_composite_mutation(&transaction, mutation)")
            .count(),
        1,
        "only the live checkpoint materializer may consume retained mutation proofs"
    );

    let materializer = checkpoint
        .split_once("fn materialize_checkpoint_batch(")
        .expect("checkpoint materializer")
        .1;
    assert!(materializer.contains("apply_validated_composite_mutation"));
    assert!(materializer.contains("encoded.frame_hash()"));
    assert!(
        !materializer.contains("JournalFrame::decode"),
        "the live checkpoint must not re-prove an already validated frame per operation"
    );
    let pay_once_apply = journal
        .split_once("fn apply_validated_composite_mutation(")
        .expect("pay-once apply")
        .1
        .split_once("fn apply_meta_mutation(")
        .expect("pay-once apply end")
        .0;
    assert!(pay_once_apply.contains("mutation.expected_hash()"));

    for relative in ["src/recovery.rs", "src/startup.rs"] {
        let path = crate_root().join(relative);
        if path.exists() {
            let candidate = production_source(path);
            assert!(
                !candidate.contains("apply_validated_composite_mutation"),
                "recovery and startup must independently validate durable bytes"
            );
        }
    }
}

#[test]
// req: PERF-007, REC-001
fn journal_runtime_rotation_stays_with_the_shared_store_owner() {
    let store = production_source(crate_root().join("src/store.rs"));
    assert!(store.contains("mod journal_runtime;"));
    let runtime = production_source(crate_root().join("src/store_journal_runtime.rs"));
    assert!(runtime.contains("impl SharedRedb"));
    assert!(runtime.contains("fn journal_runtime("));
    assert!(runtime.contains("fn maybe_start_async_checkpoint("));
    assert!(runtime.contains("worker_shared.materialize_checkpoint_batch(&worker_batch)"));
    assert!(!runtime.contains("database.begin_write("));
    assert!(!runtime.contains("transaction.commit("));
    assert!(!runtime.contains("JournalFrame::decode"));
}

/// Publication moves a command segment out of the unpublished index and into the
/// published one while holding both locks. Every lookup that consults the two
/// indexes must therefore keep one continuous transient guard across both
/// probes; a guard released between them lets a concurrent publication land in
/// the gap and report a present record as absent, which the administration-tail
/// proof then classifies as corruption.
#[test]
fn paired_command_index_lookups_hold_one_transient_guard_across_both_probes() {
    let source = read(crate_root().join("src/store.rs"));
    for name in [
        "fn command_segment_tail",
        "fn command_derived_member",
        "fn command_audit_record",
        "fn service_audit_sequences_for",
    ] {
        let start = source
            .find(name)
            .unwrap_or_else(|| panic!("paired index lookup {name} is present"));
        let body = &source[start..];
        let unpublished = body
            .find("unpublished_command_indexes")
            .unwrap_or_else(|| panic!("{name} consults the unpublished index"));
        let released = body
            .find("drop(transient)")
            .unwrap_or_else(|| panic!("{name} releases its transient guard explicitly"));
        assert!(
            released > unpublished,
            "{name} must hold its transient guard until after the unpublished probe"
        );
        let published = body
            .find("transient_indexes")
            .unwrap_or_else(|| panic!("{name} consults the published index"));
        assert!(
            published < unpublished,
            "{name} must acquire the transient guard before the unpublished lock so the \
             lock order matches publication"
        );
    }
}

/// An apply that precedes the journal fence is a proven noncommit. Relabelling
/// its failure as `CommitStatusUnknown` would fence the command coordinator over
/// a retryable condition and hide the exact classification.
#[test]
fn pre_fence_apply_failures_are_not_relabelled_as_commit_status_unknown() {
    let source = read(crate_root().join("../riffdb-commit/src/command_records.rs"));
    let start = source
        .find("fn apply_group_deferred")
        .expect("deferred apply is present");
    let end = start
        + source[start..]
            .find("\n    }\n")
            .expect("deferred apply body terminates");
    let body = &source[start..end];
    assert!(
        !without_whitespace(body).contains("StorageErrorKind::CommitStatusUnknown"),
        "the pre-fence deferred apply must surface its exact storage cause instead of \
         reclassifying every failure as durability uncertainty"
    );
}

/// `SharedRedb::commit_durable` is the only lane that may advance the root an
/// operational reader can select.
///
/// A frontier-free read reuses one captured snapshot for every access taken
/// while `durable_commit_epoch` is unchanged, and only `commit_durable`
/// increments that epoch. A commit lane that opened its own write transaction
/// and committed it directly would advance redb's root without moving the
/// epoch, and a reused snapshot would then serve a root older than an
/// acknowledged write — which no behavioural test would catch, because every
/// single-access read would still look correct.
///
/// This is the complement of
/// `every_live_database_engine_commit_routes_through_the_epoch_boundary`, which
/// pins the `self.database.begin_write(` form and exempts whole files. A lane
/// that opens its own `Database` -- or one added to an exempt file -- carries
/// no `.database.` prefix and slips past that guard, so the shape asked here is
/// behavioural instead of lexical: does one function both open a write
/// transaction and commit it, without `commit_durable`?
///
/// The exceptions are named rather than left to be rediscovered. Each one
/// either owns a `Database` it opened itself against a stopped file, or runs
/// during recovery before any operational read exists.
#[test]
fn only_commit_durable_advances_a_root_an_operational_reader_can_select() {
    const RECOVERY_OR_STOPPED_FILE: &[(&str, &str)] = &[
        // Offline restore stamps, each on its own `Database::open` of a file
        // whose owning process has stopped.
        ("backup.rs", "stamp_history_incarnation"),
        ("backup.rs", "stamp_retention_watermark"),
        // Test-only downgrade fixture, likewise on a stopped database.
        ("fixtures.rs", "downgrade_all_index_rows_to_v1_fixture"),
        // Journal recovery, which runs before operational readiness is claimed.
        ("journal.rs", "replay_frames"),
    ];

    let source_root = crate_root().join("src");
    let mut found = Vec::new();
    let mut paths: Vec<PathBuf> = fs::read_dir(&source_root)
        .expect("adapter sources are readable")
        .map(|entry| entry.expect("source entry").path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "rs"))
        .collect();
    paths.sort();

    for path in paths {
        let name = path
            .file_name()
            .expect("source file name")
            .to_str()
            .expect("UTF-8 source file name")
            .to_owned();
        // Engine-mechanics microbenchmarks live behind `benchmark-support` and
        // drive their own `Database` handles; they are not an operational lane.
        if name == "benchmark_support.rs" {
            continue;
        }
        // Extracted `include!` test modules remain wholly under their owner's
        // `#[cfg(test)]` module and cannot define a production commit lane.
        if name.ends_with("_tests.rs") {
            continue;
        }
        for (function, body) in production_functions(&production_source(&path)) {
            if body.contains(".begin_write()")
                && body.contains(".commit()")
                && !body.contains("commit_durable")
            {
                found.push((name.clone(), function));
            }
        }
    }

    let expected: Vec<(String, String)> = RECOVERY_OR_STOPPED_FILE
        .iter()
        .map(|(file, function)| ((*file).to_owned(), (*function).to_owned()))
        .collect();
    found.sort();
    let mut expected = expected;
    expected.sort();
    assert_eq!(
        found, expected,
        "a redb write transaction is opened and committed without `commit_durable`. \
         If the new lane can run while operational reads are served, it must commit \
         through `commit_durable` so the durable commit epoch witnesses it; if it \
         cannot, name it above with the reason."
    );
}

/// Splits one production source into `(function name, body)` pairs, cutting at
/// each item-level `fn`. Bodies are approximate — they run to the next `fn` —
/// which is exactly what the commit-lane guard needs: it asks whether one
/// function both opens and commits a write transaction.
fn production_functions(source: &str) -> Vec<(String, String)> {
    let lines: Vec<&str> = source.lines().collect();
    let mut starts = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        let indent = line.len() - line.trim_start().len();
        if indent > 4 {
            continue;
        }
        let trimmed = line.trim_start();
        let after_visibility = trimmed
            .strip_prefix("pub(crate) ")
            .or_else(|| trimmed.strip_prefix("pub(super) "))
            .or_else(|| trimmed.strip_prefix("pub "))
            .unwrap_or(trimmed);
        let declaration = after_visibility
            .strip_prefix("const ")
            .or_else(|| after_visibility.strip_prefix("async "))
            .unwrap_or(after_visibility);
        if let Some(rest) = declaration.strip_prefix("fn ")
            && let Some(name) = rest.split(['(', '<']).next()
            && !name.is_empty()
        {
            starts.push((index, name.to_owned()));
        }
    }
    let mut functions = Vec::new();
    for (position, (index, name)) in starts.iter().enumerate() {
        let end = starts
            .get(position + 1)
            .map_or(lines.len(), |(next, _)| *next);
        functions.push((name.clone(), lines[*index..end].join("\n")));
    }
    functions
}

// req: OUT-001, OUT-002, TXN-042, REC-004, PERF-019
#[test]
fn fresh_locator_roles_are_private_affine_and_bound_to_existing_publication_edges() {
    let source_dir = crate_root().join("src");
    let coverage = production_source(source_dir.join("fresh_locator_coverage.rs"));
    for role in [
        "PrivateChainWitness",
        "CommandPublicationWitness",
        "PreservePublicationWitness",
        "DirectCommandWitness",
        "PreservingImmediatePermit",
        "PreservingImmediateWitness",
        "CoverageRebaseWitness",
    ] {
        let declaration = coverage
            .find(&format!("struct {role}"))
            .expect("affine coverage role");
        let attributes = &coverage[declaration.saturating_sub(160)..declaration];
        assert!(!attributes.contains("derive(Clone"));
        assert!(!attributes.contains("derive(Copy"));
        assert!(!attributes.contains("derive(Default"));
    }
    assert!(!coverage.contains("pub struct FreshLocatorCoverage"));
    assert!(!coverage.contains("serde"));
    assert!(
        !production_source(source_dir.join("store.rs"))
            .contains("pub fn fresh_locator_history_fallback_scans"),
        "scan-count evidence must remain crate-private"
    );

    let application = without_whitespace(&production_source(source_dir.join("application.rs")));
    let admission = application
        .split_once("fnadmit_or_resolve_group(")
        .expect("grouped admission entry")
        .1
        .split_once("fnlookup_admission(")
        .expect("grouped admission end")
        .0;
    let begin = admission
        .find("self.begin_attributed_write(")
        .expect("mutation gate");
    let arm = admission
        .find("access.arm_fresh_locator_coverage()?")
        .expect("fresh proof arm");
    let stage = admission
        .find("stage_admission_group(&access")
        .expect("admission staging");
    assert!(begin < arm && arm < stage);
    assert!(admission[begin..arm].contains("ChangelogAttributionV3::CommandAdmission"));

    let operational = application
        .split_once("fncommand_outcome_from_operational_indexes(")
        .expect("operational lookup")
        .1
        .split_once("fncommand_derived_index_covers(")
        .expect("operational lookup end")
        .0;
    let locator = operational
        .find("JournalTable::IdempotencyLocators")
        .expect("exact durable locator");
    let proof = operational
        .find("fresh_locator_proves_absence")
        .expect("public-prefix absence proof");
    let scan = operational
        .find("JournalTable::Commits")
        .expect("bounded history fallback");
    assert!(locator < proof && proof < scan);

    let store = without_whitespace(&production_source(source_dir.join("store.rs")));
    let publish = store
        .split_once("fnpublish_pending_command(")
        .expect("queued command publication")
        .1
        .split_once("fnpublish_pending_service_audit(")
        .expect("queued command publication end")
        .0;
    let successor = publish
        .find("publish_composite_successor")
        .expect("ADR-0100 successor publication");
    let coverage = publish
        .find(".publish_command(witness)")
        .expect("queued coverage publication");
    let observation = publish
        .find("observe_changelog_publication")
        .expect("ADR-0100 observation");
    assert!(successor < coverage && coverage < observation);

    for excluded in [
        "commits",
        "idempotency_locators",
        "provenance_locators",
        "audit_by_request_locators",
        "META_APPLICATION_SEQUENCE.as_bytes()",
    ] {
        assert!(
            store.contains(excluded),
            "the closed permit validator must reject {excluded}"
        );
    }
    assert!(
        production_source(source_dir.join("fresh_locator_coverage.rs"))
            .contains("riffdb-fresh-locator-preserving-permit-v1")
    );
    assert!(store.contains("canonical.windows(2).any"));
    assert!(store.contains("publication_queue.lock()"));

    assert!(store.contains("fresh_locator_expected_mutations"));
    assert!(store.contains("fresh_locator_actual_mutations"));
    assert!(store.contains("close_fresh_locator_mutation_expectations"));
}
