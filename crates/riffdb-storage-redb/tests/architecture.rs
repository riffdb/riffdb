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
    let mut paths = fs::read_dir(source)
        .expect("read source directory")
        .map(|entry| entry.expect("source entry").path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "rs"))
        .collect::<Vec<_>>();
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
fn dependency_surface_keeps_redb_private_and_excludes_infrastructure_assemblies() {
    let manifest = read(crate_root().join("Cargo.toml"));
    assert!(manifest.contains("cap-std = { version = \"=4.0.2\", default-features = false }"));
    assert!(manifest.contains("redb = { version = \"=4.1.0\", default-features = false }"));
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
    for forbidden in [
        "criterion",
        "riffdb-contract-ir",
        "riffdb-storage-memory",
        "rmcp",
        "tokio",
        "tonic",
    ] {
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

#[test]
fn sha256_dependency_is_confined_to_reviewed_integrity_boundaries() {
    let source = crate_root().join("src");
    for entry in fs::read_dir(source).expect("read source directory") {
        let path = entry.expect("source entry").path();
        if path.file_name().is_some_and(|name| {
            matches!(
                name.to_str(),
                Some("backup.rs" | "format_upgrade.rs" | "journal.rs" | "migration_stage.rs")
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
    for entry in fs::read_dir(source).expect("read source directory") {
        let path = entry.expect("source entry").path();
        if path.file_name().is_some_and(|name| name == "startup.rs")
            || path.extension().is_none_or(|extension| extension != "rs")
        {
            continue;
        }
        assert!(
            !read(&path).contains("riffdb_catalog"),
            "only startup migration may import catalog: {}",
            path.display()
        );
    }

    let startup = read(crate_root().join("src/startup.rs"));
    assert!(startup.contains("CatalogIndexMigrationBackend"));
    assert!(!startup.contains("ValidatedCatalogHistory"));
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
    assert!(cache.contains("decode_command_segment_v1"));
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
    assert!(migration.contains("stage.immutable_history_digest != witness.digest"));
    assert!(migration.contains("validate_entity_index_structure(&stage.ports)?"));
}

#[test]
fn index_migration_rechecks_replacement_charge_before_staging_any_write() {
    let startup = read(crate_root().join("src/startup.rs"));
    let charge_check = startup
        .find("replacement.encoded_content_charge().get()")
        .expect("migration replacement charge check");
    let insert = startup[charge_check..]
        .find("table\n                    .insert(key, replacement.as_bytes())")
        .map(|offset| charge_check + offset)
        .expect("migration replacement insertion");
    assert!(charge_check < insert);
    assert!(
        startup[charge_check..insert].contains("expected.conservative_v2_envelope_charge().get()")
    );
    assert!(startup[charge_check..insert].contains("return Err(invariant())"));
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
                name == "store.rs"
                    || name == "startup.rs"
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

    let startup = without_whitespace(&production_source(source_dir.join("startup.rs")));
    assert!(!startup.contains("transaction.commit()"));
    assert_eq!(startup.matches("commit_durable(transaction)?").count(), 1);
}

#[test]
fn sealed_command_audit_evidence_is_confined_to_post_staging_application_commits() {
    let source_dir = crate_root().join("src");
    let application = without_whitespace(&production_source(source_dir.join("application.rs")));
    let stage = application
        .split_once("fnstage(self,records:AtomicCommandRecordSet)")
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
fn the_shutdown_checkpoint_reads_row_counts_and_counts_every_terminal_failure_once() {
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
fn the_changelog_emitter_can_only_read_published_durable_snapshots() {
    // ADR-0100 §2 made structural. The emitter derives frames from published
    // durable state only; this pin proves the module has no other way to read.
    // The `RedbPublishedSnapshot` adapter at the top of the file is the single
    // bridge, and it is constructed only at a publication site.
    let emitter = production_source(crate_root().join("src/changelog.rs"));
    let derivation = emitter
        .split_once("impl PublishedDurableSnapshot for RedbPublishedSnapshot")
        .expect("the published-snapshot adapter opens the module")
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
