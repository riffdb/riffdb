#![cfg(target_os = "linux")]
#![forbid(unsafe_code)]

//! Cross-crate acceptance for the closed driver protocol and exact generated
//! operation catalog. Real verified-TLS transport is exercised by the
//! dependency gate `remote_deployment_rotation`; this test proves the new
//! local boundary cannot widen it.

use std::fs;

use riffdb_driver_host::{ApplicationCatalog, OperationKind, ReactiveKind};
use serde_json::Value;

fn hash32(value: &str) -> [u8; 32] {
    assert_eq!(value.len(), 64, "exact identity must be one SHA-256 digest");
    let mut result = [0_u8; 32];
    for (index, byte) in result.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .expect("lowercase hexadecimal identity");
    }
    result
}

#[test]
fn shared_driver_conformance_catalog_is_exact() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let application = root.join("fixtures/driver/conformance-app");
    let lock = fs::read(application.join("riffdb.application.lock.json")).expect("lock");
    let manifest =
        fs::read(application.join("generated/riffdb.application.exact.json")).expect("manifest");
    let operations = fs::read(application.join("generated/mcp/tools.json")).expect("operations");
    let catalog = ApplicationCatalog::from_exact_artifacts(
        &lock,
        &manifest,
        &operations,
        "default",
        "DriverConformanceApplication",
    )
    .expect("shared conformance catalog");
    assert_eq!(
        catalog
            .operation("driver_conformance_create_item")
            .expect("command")
            .kind(),
        OperationKind::Command
    );
    assert_eq!(
        catalog
            .operation("driver_conformance_item_page")
            .expect("query")
            .kind(),
        OperationKind::Query
    );
}

#[test]
fn package_first_matrix_binds_four_facades_to_one_exact_story() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let application = root.join("fixtures/driver/conformance-app");
    let matrix: Value = serde_json::from_slice(
        &fs::read(root.join("fixtures/driver/package-matrix-v1.json")).expect("matrix"),
    )
    .expect("package matrix JSON");
    assert_eq!(
        matrix["schema"], "riffdb.driver-package-matrix/v1",
        "closed matrix schema"
    );
    assert_eq!(matrix["quickstart"]["step_count"], 4);
    assert_eq!(
        matrix["quickstart"]["ordered_steps"],
        serde_json::json!(["schema", "install", "generate", "query"])
    );
    let languages = matrix["languages"].as_array().expect("languages");
    assert_eq!(languages.len(), 4);
    assert_eq!(
        languages
            .iter()
            .map(|entry| entry["language"].as_str().expect("language"))
            .collect::<Vec<_>>(),
        ["go", "python", "rust", "typescript"]
    );
    for language in languages {
        assert_eq!(language["workflow_step_count"], 4);
        assert!(
            language["package"]
                .as_str()
                .is_some_and(|package| !package.is_empty()),
            "every cell installs a named runtime package"
        );
        assert!(
            language["identity_carriage"]
                .as_str()
                .is_some_and(|mode| mode.contains("plan") || mode.contains("manifest_catalog")),
            "every cell names its exact identity carrier"
        );
    }

    let lock = fs::read(application.join("riffdb.application.lock.json")).expect("lock");
    let manifest =
        fs::read(application.join("generated/riffdb.application.exact.json")).expect("manifest");
    let operations = fs::read(application.join("generated/mcp/tools.json")).expect("operations");
    let catalog = ApplicationCatalog::from_exact_artifacts(
        &lock,
        &manifest,
        &operations,
        "default",
        "DriverConformanceApplication",
    )
    .expect("exact package conformance catalog");
    for (kind, expected_kind) in [
        ("command", OperationKind::Command),
        ("query", OperationKind::Query),
    ] {
        let identity = &matrix["operations"][kind];
        let public_name = identity["driver_operation"]["name"]
            .as_str()
            .expect("driver operation name");
        let spec = catalog.operation(public_name).expect("catalog operation");
        assert_eq!(spec.kind(), expected_kind);
        assert_eq!(
            spec.input_schema_hash(),
            identity["driver_operation"]["input_schema_hash"]
                .as_str()
                .expect("input schema hash")
        );
        assert_eq!(
            spec.plan_hash(),
            Some(hash32(identity["plan_hash"].as_str().expect("plan hash")))
        );
    }

    let forbidden = [
        "GetEntity",
        "ScanIndex",
        "entity_type_id",
        "field_id",
        "index_id",
        "riffdb_kernel",
        "riffdb_proto",
    ];
    for runner in [
        "runner/go/main.go",
        "runner/python.py",
        "runner/typescript.mjs",
    ] {
        let source = fs::read_to_string(application.join(runner)).expect("package runner");
        for needle in forbidden {
            assert!(
                !source.contains(needle),
                "{runner} contains forbidden application glue {needle}"
            );
        }
    }
}

#[test]
fn package_first_driver_runner_abi_is_documented_in_exact_order() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let exact = "SOCKET MANIFEST_HASH CATALOG_HASH DATABASE ROLE ROLE_HASH REMOTE_HASH LINEAGE VERSION BUNDLE_HASH";
    for relative in [
        "docs/sdks/GO.md",
        "docs/getting-started/AGENT-APPLICATION-QUICKSTART.md",
        "templates/application/go-main.go",
    ] {
        let source = fs::read_to_string(root.join(relative)).expect("driver runner ABI source");
        assert!(
            source.contains(exact),
            "{relative} must carry the exact development-runner argument order"
        );
    }
}

#[test]
fn generated_go_results_carry_exact_compiler_owned_operation_identity() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let application = root.join("fixtures/driver/conformance-app");
    let lock: Value = serde_json::from_slice(
        &fs::read(application.join("riffdb.application.lock.json")).expect("lock"),
    )
    .expect("lock JSON");
    let generated = fs::read_to_string(application.join("generated/go/client.go"))
        .expect("generated Go client");
    let query_hash = lock["modules"][0]["queries"][0]["plan_hash"]
        .as_str()
        .expect("query plan hash");
    let command_hash = lock["roles"][0]["definition"]["commands"]
        .as_array()
        .expect("role commands")
        .iter()
        .find(|command| command["name"] == "CreateItem")
        .and_then(|command| command["plan_hash"].as_str())
        .expect("CreateItem plan hash");

    assert!(generated.contains("type QueryIdentity struct"));
    assert!(generated.contains("Identity QueryIdentity"));
    assert!(generated.contains("ContractVersion uint64; PlanHash string"));
    assert!(generated.contains(&format!("const ItemPageQueryPlanHash = \"{query_hash}\"")));
    assert!(generated.contains(&format!("const CreateItemPlanHash = \"{command_hash}\"")));
    assert!(generated.contains("PlanHash: ItemPageQueryPlanHash"));
    assert!(generated.contains("PlanHash: CreateItemPlanHash"));
}

#[test]
fn development_runners_bind_native_configuration_and_sealed_typescript_tooling() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    for relative in ["scripts/riffdb-dev", "scripts/riffdb-dev-installed"] {
        let source = fs::read_to_string(root.join(relative)).expect("development runner");
        for name in [
            "RIFFDB_ENDPOINT",
            "RIFFDB_CREDENTIAL_FILE",
            "RIFFDB_DATABASE",
        ] {
            assert!(source.contains(name), "{relative} omits {name}");
        }
        assert!(
            source.contains("application runtime-catalog"),
            "{relative} does not derive driver metadata for sparse applications"
        );
        assert!(
            source.contains(".generation.mcp // empty"),
            "{relative} does not honor a declared MCP catalog path"
        );
        assert!(
            !source.contains(
                "operation_catalog_file = \"$application_root/generated/mcp/tools.json\""
            ),
            "{relative} still requires an optional MCP artifact"
        );
    }
    let installed =
        fs::read_to_string(root.join("scripts/riffdb-dev-installed")).expect("installed runner");
    let source = fs::read_to_string(root.join("scripts/riffdb-dev")).expect("source runner");
    assert!(
        source.contains("cargo metadata --quiet --format-version 1 --no-deps"),
        "the source runner must derive Cargo's effective target directory"
    );
    for binary in ["riffdb", "riffdbd", "riffdb-driverd", "benchmark"] {
        assert!(
            source.contains(&format!("$cargo_target_dir/$profile/{binary}")),
            "the source runner does not resolve {binary} through Cargo's effective target directory"
        );
        assert!(
            !source.contains(&format!("$repo_root/target/$profile/{binary}")),
            "the source runner still hard-codes {binary} under the repository target directory"
        );
    }
    assert!(installed.contains("tooling/typescript/bin:$PATH"));
    assert!(installed.contains("tooling/typescript/node_modules"));
    assert!(
        installed.contains("if ! \"${operator[@]}\" role bind"),
        "the installed dev runner must not let set -e discard a role-bind failure"
    );
    assert!(installed.contains("cat \"$work_root/role.json\" >&2"));
    assert!(
        installed.contains("--application \"$application_source\""),
        "installed development seeds must derive command metadata from the exact application"
    );

    let sealer = fs::read_to_string(root.join("scripts/agent-application-alpha-package-first"))
        .expect("package-first sealer");
    assert!(sealer.contains("node_modules/undici-types"));
    assert!(
        sealer.contains("--typeRoots \"$tooling_root/node_modules/@types\" --types node \"$@\""),
        "the sealed compiler must make its supplied Node declarations visible without application-owned type configuration"
    );
    assert!(
        !sealer.contains("--noEmit --types node"),
        "the tooling smoke must exercise the wrapper's default Node declaration visibility"
    );
    assert!(
        sealer.contains("metadata --offline --locked --format-version 1"),
        "the sealed package must reject a workspace-shaped generated Rust lock"
    );
    assert!(sealer.contains("runtime-subset-checksums.sha256"));
    assert!(sealer.contains("complete-distribution-checksums.sha256"));
    assert!(sealer.contains("rm \"$legacy/packages/distribution/checksums.sha256\""));
    assert!(
        sealer.contains("$legacy/.cargo/.global-cache"),
        "package-first sealer must remove Cargo's mutable global cache after its final package smoke"
    );
    let verifier = fs::read_to_string(root.join("scripts/verify-agent-alpha-runtime-subset"))
        .expect("runtime subset verifier");
    assert!(verifier.contains("sha256sum --strict --check \"$subset_inventory\""));
    assert!(verifier.contains("runtime subset retains a misleading"));
    assert!(
        verifier.contains("sealed bundle contains mutable Cargo cache state"),
        "bundle verifier must reject nondeterministic Cargo cache residue"
    );
}

#[test]
fn package_first_campaign_adds_metrics_without_rewriting_predecessor_schemas() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let evaluations = root.join("evaluations/agent-application-alpha");
    let predecessor_report: Value = serde_json::from_slice(
        &fs::read(evaluations.join("report-schema.json")).expect("v1 report schema"),
    )
    .expect("v1 report JSON schema");
    let package_report: Value = serde_json::from_slice(
        &fs::read(evaluations.join("report-schema-v2.json")).expect("v2 report schema"),
    )
    .expect("v2 report JSON schema");
    assert_eq!(
        predecessor_report["properties"]["schema"]["const"],
        "riffdb-agent-application-alpha-run/v1"
    );
    assert_eq!(
        package_report["properties"]["schema"]["const"],
        "riffdb-agent-application-alpha-run/v2"
    );
    let required = package_report["required"].as_array().expect("v2 required");
    for field in [
        "package_distribution_checksums_sha256",
        "package_installation_verified",
        "time_to_first_committed_row_seconds",
        "identity_change_ceremony_count",
        "rescue_count",
        "kernel_attempts",
        "rating",
    ] {
        assert!(
            required.iter().any(|candidate| candidate == field),
            "package-first report omitted {field}"
        );
    }
    let events: Value = serde_json::from_slice(
        &fs::read(evaluations.join("event-schema-v2.json")).expect("v2 event schema"),
    )
    .expect("v2 event JSON schema");
    let kinds = events["properties"]["kind"]["enum"]
        .as_array()
        .expect("v2 event kinds");
    for kind in [
        "package_install",
        "identity_change_ceremony",
        "rescue",
        "kernel_attempt",
    ] {
        assert!(kinds.iter().any(|candidate| candidate == kind));
    }
}

#[test]
fn row_policy_v7_catalog_exposes_only_the_selected_symbolic_role() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let application = root.join("fixtures/adapters/row-policy-conformance");
    let lock = fs::read(application.join("riffdb.application.lock.json")).expect("lock");
    let manifest =
        fs::read(application.join("generated/riffdb.application.exact.json")).expect("manifest");
    let operations = fs::read(application.join("generated/mcp/tools.json")).expect("operations");
    let catalog = ApplicationCatalog::from_exact_artifacts(
        &lock,
        &manifest,
        &operations,
        "default",
        "AdapterRowPolicyApplication",
    )
    .expect("row-policy V7 application catalog");

    assert_eq!(
        catalog
            .operation("adapter_row_policy_conformance_create_document")
            .expect("protected command")
            .kind(),
        OperationKind::Command
    );
    assert_eq!(
        catalog
            .operation("adapter_row_policy_conformance_list_documents")
            .expect("protected query")
            .kind(),
        OperationKind::Query
    );
}

#[test]
fn exact_ticketdesk_catalog_covers_commands_queries_and_reactive_actions_only() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let application = root.join("examples/ticketdesk");
    let lock = fs::read(application.join("riffdb.application.lock.json")).expect("lock");
    let manifest =
        fs::read(application.join("generated/riffdb.application.exact.json")).expect("manifest");
    let operations = fs::read(application.join("generated/mcp/tools.json")).expect("operations");
    let application_catalog = ApplicationCatalog::from_exact_artifacts(
        &lock,
        &manifest,
        &operations,
        "default",
        "TicketDeskApplication",
    )
    .expect("exact application catalog");

    assert_eq!(
        application_catalog
            .operation("ticketdesk_create_comment")
            .expect("command")
            .kind(),
        OperationKind::Command
    );
    assert_eq!(
        application_catalog
            .operation("ticketdesk_ticket_page")
            .expect("query")
            .kind(),
        OperationKind::Query
    );
    let live = application_catalog
        .operation("ticket_activity_ticket_page_watch_watch")
        .expect("live query");
    assert_eq!(live.kind(), OperationKind::Reactive);
    assert_eq!(live.reactive_kind(), Some(ReactiveKind::Watch));
    assert_eq!(live.reactive_action(), Some("watch"));
    assert!(
        application_catalog
            .operation("ticket_activity_ticket_events_next")
            .is_none(),
        "selected application role must not inherit agent stream authority"
    );

    let agent_catalog = ApplicationCatalog::from_exact_artifacts(
        &lock,
        &manifest,
        &operations,
        "default",
        "TicketDeskAgent",
    )
    .expect("exact agent catalog");
    let events = agent_catalog
        .operation("ticket_activity_ticket_events_next")
        .expect("agent event stream");
    assert_eq!(events.reactive_kind(), Some(ReactiveKind::Stream));
    assert!(agent_catalog.operation("ticketdesk_ticket_page").is_none());
    assert!(
        agent_catalog
            .operation("ticket_activity_ticket_page_watch_watch")
            .is_none()
    );

    for forbidden in [
        "GetEntity",
        "ScanIndex",
        "DeployContract",
        "CreateCapability",
        "ExecuteQueryText",
    ] {
        assert!(
            application_catalog.operation(forbidden).is_none(),
            "admitted {forbidden}"
        );
    }
}
