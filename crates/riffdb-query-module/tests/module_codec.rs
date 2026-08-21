//! Query-module identity and strict-codec acceptance tests.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_module::{
    ApplicationManifest, CompiledNamedQuery, CompiledNamedQueryPlan, ManifestErrorKind,
    NamedQuerySource, QUERY_MODULE_FORMAT_VERSION_COVERED_RESULT_V1,
    QUERY_MODULE_FORMAT_VERSION_OPERATIONAL_AGGREGATE_V1,
    QUERY_MODULE_FORMAT_VERSION_OPERATIONAL_V1, QUERY_MODULE_FORMAT_VERSION_V1, QueryModule,
    QueryModuleCandidate, QueryModuleErrorKind, QueryModuleName, QueryModuleVersion,
    generate_go_client, generate_mcp_commands, generate_mcp_tools, generate_python_client,
    generate_rust_client, generate_typescript_client,
};
use std::sync::Arc;

const CONTRACT: &str = include_str!("../../../examples/app-baseline/contracts/ticketdesk.riff");
const WORKFLOW_CONTRACT: &str =
    include_str!("../../../fixtures/workflows/compiler/valid/workflow_surface.riff");
const WORKFLOW_QUERY: &str = r#"
query GetWork(
    $organization_id: WorkItem.organization_id,
    $work_id: WorkItem.work_id,
) {
    one work from WorkItem
        where organization_id == $organization_id
            && work_id == $work_id
        else NotFound

    return Found {
        work: work {
            organization_id
            work_id
            state
            lease_owner
            lease_expires_at
            lease_fence
            lease_attempts
        }
    }

    outcomes Found | NotFound
}
"#;

fn candidate(reversed: bool) -> QueryModuleCandidate {
    let mut queries = vec![
        NamedQuerySource::new(
            "ListTickets",
            include_str!("../../../queries/ticketdesk/list_tickets.riffq"),
        )
        .expect("list source"),
        NamedQuerySource::new(
            "TicketPage",
            include_str!("../../../queries/ticketdesk/ticket_page.riffq"),
        )
        .expect("page source"),
    ];
    if reversed {
        queries.reverse();
    }
    QueryModuleCandidate::new(
        QueryModuleName::new("ticketdesk").expect("module name"),
        QueryModuleVersion::new(1).expect("module version"),
        queries,
    )
    .expect("module candidate")
}

#[test]
fn generated_mcp_tools_are_module_pinned_name_addressed_and_domain_shaped() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let module = QueryModule::compile(candidate(false), &bundle).expect("module");
    let tools = generate_mcp_tools(&module).expect("tools");

    assert_eq!(tools.len(), 2);
    assert_eq!(tools[0].name, "ticketdesk_list_tickets");
    assert_eq!(tools[1].name, "ticketdesk_ticket_page");
    assert_eq!(tools[0].operation_name, "ListTickets");
    assert_eq!(tools[1].operation_name, "TicketPage");
    assert!(
        tools
            .iter()
            .all(|tool| tool.module_hash == *module.identity().as_bytes())
    );
    let input: serde_json::Value =
        serde_json::from_str(&tools[0].input_schema).expect("input schema");
    let result: serde_json::Value =
        serde_json::from_str(&tools[1].result_schema).expect("result schema");
    assert_eq!(input["properties"]["organization_id"]["type"], "string");
    assert_eq!(input["properties"]["statuses"]["uniqueItems"], true);
    assert_eq!(
        result["oneOf"][0]["properties"]["outcome"]["const"],
        "Found"
    );
    assert_eq!(
        result["oneOf"][0]["properties"]["ticket"]["properties"]["title"]["type"],
        "string"
    );
    assert!(!tools[0].input_schema.contains("field_id"));
    assert!(!tools[0].result_schema.contains("entity_type_id"));

    let commands = generate_mcp_commands(&module, &bundle).expect("commands");
    assert_eq!(commands.len(), 11);
    assert_eq!(commands[0].name, "ticketdesk_add_project_member");
    for expected in [
        "ticketdesk_close_ticket_with_comment",
        "ticketdesk_open_ticket_with_labels",
        "ticketdesk_swap_member_roles",
    ] {
        assert!(commands.iter().any(|command| command.name == expected));
    }
    assert!(commands[0].input_schema.contains("idempotency_key"));
    assert!(commands[0].result_schema.contains("\"outcome\""));
    assert!(
        commands
            .iter()
            .all(|command| command.contract_bundle_hash == *bundle.bundle_hash().as_bytes())
    );
}

#[test]
fn identity_and_bytes_are_independent_of_input_order() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let first = QueryModule::compile(candidate(false), &bundle).expect("first module");
    let second = QueryModule::compile(candidate(true), &bundle).expect("second module");

    assert_eq!(first.identity(), second.identity());
    assert_eq!(first.canonical_bytes(), second.canonical_bytes());
    assert_eq!(first.queries()[0].name(), "ListTickets");
    assert_eq!(first.queries()[1].name(), "TicketPage");
}

#[test]
fn covered_result_module_uses_v7_and_round_trips_exactly() {
    let bundle = compile_contract_source(CONTRACT).expect("covered contract");
    let candidate = QueryModuleCandidate::new(
        QueryModuleName::new("ticketdesk_board").expect("module name"),
        QueryModuleVersion::new(1).expect("module version"),
        vec![
            NamedQuerySource::new(
                "BoardPage450",
                include_str!("../../../queries/ticketdesk/board_page_450.riffq"),
            )
            .expect("board source"),
        ],
    )
    .expect("candidate");
    let module = QueryModule::compile(candidate, &bundle).expect("covered module");
    assert_eq!(
        module.format_version(),
        QUERY_MODULE_FORMAT_VERSION_COVERED_RESULT_V1
    );
    assert!(
        module
            .query("BoardPage450")
            .and_then(CompiledNamedQuery::ordinary_program)
            .and_then(|program| program.steps()[0].covered_result_layout())
            .is_some()
    );
    let decoded = QueryModule::decode_and_validate(module.canonical_bytes(), &bundle)
        .expect("strict V7 round trip");
    assert_eq!(decoded.canonical_bytes(), module.canonical_bytes());
    assert_eq!(decoded.identity(), module.identity());

    let rust = generate_rust_client(&module, &bundle);
    assert!(rust.contains("fn decode_compact_result("));
    assert!(rust.contains("response.fields != vec![\"assignee_id\".to_owned()"));
    assert!(rust.contains("response.rows.len() > 450usize"));
    assert!(rust.contains("let [value_0, value_1, value_2, value_3, value_4, value_5]"));
    assert!(rust.contains("(1, 1, \"Open\") => \"Open\".to_owned()"));
    assert!(rust.contains("ApplicationUuid::from_bytes(bytes).into_string()"));
    assert!(!rust.contains("Ok(format!(\"{:02x}{:02x}"));
    assert!(!rust.contains("decode_compact_result(outcome: String, response: app_v1::CompactResultField) -> Result<Self::Output, ApplicationClientError> {\n        let mut fields = BTreeMap"));

    let typescript = generate_typescript_client(&module, &bundle);
    assert!(typescript.contains("function decodeBoardPage450Compact("));
    assert!(typescript.contains("compactDecoder: decodeBoardPage450Compact"));
    assert!(!typescript.contains("value.rows.map((row) => Object.fromEntries"));

    let go = generate_go_client(&module, &bundle);
    assert!(go.contains("func decodeBoardPage450Compact("));
    assert!(go.contains("options.AcceptCompactResult = true"));
    assert!(!go.contains("map[string]riffdb.Value{}; for _, row := range value.Rows"));

    let python = generate_python_client(&module, &bundle).expect("Python");
    assert!(python.contains("def _decode_board_page450_compact("));
    assert!(python.contains("accept_compact_result=True"));
    assert!(!python.contains("dict(zip(compact[\"fields\"], row))"));
}

#[test]
fn cloned_named_query_handles_share_immutable_checked_artifacts() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let module = QueryModule::compile(candidate(false), &bundle).expect("module");
    let query = module.query("TicketPage").expect("named query");

    let first_document = query.shared_document();
    let second_document = query.shared_document();
    let first_program = query.shared_ordinary_program().expect("ordinary");
    let second_program = query.shared_ordinary_program().expect("ordinary");

    assert!(Arc::ptr_eq(&first_document, &second_document));
    assert!(Arc::ptr_eq(&first_program, &second_program));
    assert_eq!(query.document(), first_document.as_ref());
    assert_eq!(
        query.ordinary_program().expect("ordinary"),
        first_program.as_ref()
    );
}

#[test]
fn operational_module_round_trips_a_complete_family_without_changing_v1_codec() {
    const OPERATIONAL_CONTRACT: &str = r#"
contract Operational version 1 {
  entity Ticket {
    key (organization_id: uuid, ticket_id: uuid)
    field status: string<32>
    field updated_at: timestamp
    index a_status (organization_id, status, updated_at, ticket_id)
    index z_all (organization_id, updated_at, ticket_id)
  }
  aggregate Tickets {
    root Ticket
    partition_by organization_id
    conflict_key (organization_id, ticket_id)
  }
}

"#;
    const QUERY: &str = r#"
query SearchTickets(
    $organization_id: Ticket.organization_id,
    $status: Ticket.status?
) {
    many tickets from Ticket
        where organization_id == $organization_id
          && when $status { status == $status }
        order by updated_at asc, ticket_id asc
        take 25
    return Found { tickets: tickets { ticket_id status updated_at } }
    outcomes Found
}
"#;
    let bundle = compile_contract_source(OPERATIONAL_CONTRACT).expect("contract");
    let operational_candidate = QueryModuleCandidate::new(
        QueryModuleName::new("operational").expect("name"),
        QueryModuleVersion::new(1).expect("version"),
        vec![NamedQuerySource::new("SearchTickets", QUERY).expect("query")],
    )
    .expect("candidate");
    let module = QueryModule::compile(operational_candidate, &bundle).expect("module");
    assert_eq!(
        u32::from_be_bytes(
            module.canonical_bytes()[20..24]
                .try_into()
                .expect("version")
        ),
        QUERY_MODULE_FORMAT_VERSION_OPERATIONAL_V1
    );
    let query = module.query("SearchTickets").expect("query");
    let CompiledNamedQueryPlan::OperationalV1(family) = query.plan() else {
        panic!("expected family");
    };
    assert_eq!(family.members().len(), 2);
    assert_eq!(query.plan().authorization(), family.authorization_union());
    assert_eq!(query.plan().identity(), family.identity().hash());
    assert!(query.ordinary_program().is_none());
    assert!(query.select_program(&[false]).is_some());
    assert!(query.select_program(&[true]).is_some());
    assert!(query.select_program(&[]).is_none());

    let rust = generate_rust_client(&module, &bundle);
    let typescript = generate_typescript_client(&module, &bundle);
    let python = generate_python_client(&module, &bundle).expect("Python");
    let go = generate_go_client(&module, &bundle);
    let mcp = generate_mcp_tools(&module).expect("MCP");
    assert!(rust.contains("pub status: Option<String>"));
    assert!(typescript.contains("readonly status?: string | null;"));
    assert!(python.contains("status: str | None = None"));
    assert!(go.contains("Status *string"));
    let input: serde_json::Value = serde_json::from_str(&mcp[0].input_schema).expect("MCP input");
    assert!(
        !input["required"]
            .as_array()
            .expect("required")
            .iter()
            .any(|name| name == "status")
    );

    let decoded = QueryModule::decode_and_validate(module.canonical_bytes(), &bundle)
        .expect("strict operational decode");
    assert_eq!(decoded, module);

    let legacy_bundle = compile_contract_source(CONTRACT).expect("legacy contract");
    let legacy = QueryModule::compile(candidate(false), &legacy_bundle).expect("legacy module");
    assert_eq!(
        u32::from_be_bytes(
            legacy.canonical_bytes()[20..24]
                .try_into()
                .expect("version")
        ),
        QUERY_MODULE_FORMAT_VERSION_V1
    );
}

#[test]
fn aggregate_module_round_trips_with_additive_v3_identity_and_name_only_explain() {
    const AGGREGATE_CONTRACT: &str = r#"
contract OperationalAggregate version 1 {
  entity Ticket {
    key (organization_id: uuid, ticket_id: uuid)
    field status: string<32>
    field story_points: i64
    field updated_at: timestamp
    index all_tickets (organization_id, updated_at, ticket_id)
  }
  aggregate Tickets {
    root Ticket
    partition_by organization_id
    conflict_key (organization_id, ticket_id)
  }
}
"#;
    const AGGREGATE_QUERY: &str = r#"
query TicketSummary($organization_id: Ticket.organization_id) {
    many tickets from Ticket
        where organization_id == $organization_id
        order by updated_at asc, ticket_id asc
        take 25
    aggregate summary from tickets {
        group by status
        count() as ticket_count
        sum(story_points) as total_points
    }
    return Found { summary: summary { status ticket_count total_points } }
    outcomes Found
}
"#;
    let bundle = compile_contract_source(AGGREGATE_CONTRACT).expect("contract");
    let candidate = QueryModuleCandidate::new(
        QueryModuleName::new("aggregate").expect("name"),
        QueryModuleVersion::new(1).expect("version"),
        vec![NamedQuerySource::new("TicketSummary", AGGREGATE_QUERY).expect("query")],
    )
    .expect("candidate");
    let module = QueryModule::compile(candidate, &bundle).expect("aggregate module");
    assert_eq!(
        module.format_version(),
        QUERY_MODULE_FORMAT_VERSION_OPERATIONAL_AGGREGATE_V1
    );
    let query = module.query("TicketSummary").expect("query");
    let family = query.operational_family().expect("family");
    assert_eq!(family.aggregates().len(), 1);
    let explain = query.explain_lines();
    assert!(explain.contains(&"operational.aggregate.summary.source=tickets".to_owned()));
    assert!(explain.contains(&"operational.aggregate.summary.maximum_groups=25".to_owned()));
    assert!(explain.contains(&"operational.aggregate.summary.measure.total_points=sum".to_owned()));
    assert!(!explain.iter().any(|line| line.contains("field_id")));

    let decoded = QueryModule::decode_and_validate(module.canonical_bytes(), &bundle)
        .expect("strict aggregate decode");
    assert_eq!(decoded, module);
}

#[test]
fn strict_decode_recompiles_against_the_exact_contract() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let module = QueryModule::compile(candidate(false), &bundle).expect("module");
    let decoded =
        QueryModule::decode_and_validate(module.canonical_bytes(), &bundle).expect("decode");
    assert_eq!(decoded.identity(), module.identity());
    assert_eq!(
        decoded.query("TicketPage").expect("named query").name(),
        "TicketPage"
    );

    let mut damaged = module.canonical_bytes().to_vec();
    let last = damaged.len() - 1;
    damaged[last] ^= 1;
    let error = QueryModule::decode_and_validate(&damaged, &bundle).expect_err("damage rejects");
    assert!(matches!(
        error.kind(),
        QueryModuleErrorKind::InvalidEncoding | QueryModuleErrorKind::IdentityMismatch
    ));
}

#[test]
fn same_version_with_changed_source_has_a_distinct_identity() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let original = QueryModule::compile(candidate(false), &bundle).expect("module");
    let changed = QueryModuleCandidate::new(
        QueryModuleName::new("ticketdesk").expect("module name"),
        QueryModuleVersion::new(1).expect("module version"),
        vec![
            NamedQuerySource::new(
                "ListTickets",
                include_str!("../../../queries/ticketdesk/list_tickets.riffq")
                    .replace("$limit: Limit = 25", "$limit: Limit = 26"),
            )
            .expect("changed source"),
        ],
    )
    .expect("changed candidate");
    let changed = QueryModule::compile(changed, &bundle).expect("changed module");
    assert_ne!(changed.identity(), original.identity());
}

#[test]
fn generated_clients_are_reproducible_name_addressed_and_identity_pinned() {
    let bundle = compile_contract_source(CONTRACT).expect("contract");
    let module = QueryModule::compile(candidate(false), &bundle).expect("module");
    let rust = generate_rust_client(&module, &bundle);
    let typescript = generate_typescript_client(&module, &bundle);
    let python = generate_python_client(&module, &bundle).expect("Python");
    let go = generate_go_client(&module, &bundle);

    assert_eq!(rust, generate_rust_client(&module, &bundle));
    assert_eq!(typescript, generate_typescript_client(&module, &bundle));
    assert_eq!(
        python,
        generate_python_client(&module, &bundle).expect("Python")
    );
    assert_eq!(go, generate_go_client(&module, &bundle));
    assert!(rust.contains("pub struct ListTicketsParams"));
    assert!(rust.contains("\"TicketPage\",\n            Some(QUERY_MODULE_HASH)"));
    assert!(rust.contains("pub struct CreateTicketInput"));
    assert!(rust.contains("pub const CONTRACT_BUNDLE_HASH"));
    assert!(rust.contains("pub async fn open_bounded_session"));
    assert!(rust.contains("ApplicationSessionIdentity::new(CONTRACT_LINEAGE.to_owned()"));
    assert!(rust.contains("impl GeneratedQuery for TicketPageQuery"));
    assert!(rust.contains("pub struct TicketDeskClient"));
    assert!(rust.contains("execute_generated_command"));
    assert!(rust.contains("fn decode_outcome"));
    assert!(rust.contains("with_options(options)"));
    assert!(typescript.contains("export interface TicketPageParams"));
    assert!(typescript.contains("queryName: \"ListTickets\""));
    assert!(typescript.contains("export interface CreateTicketInput"));
    assert!(typescript.contains("export const CONTRACT_BUNDLE_HASH"));
    assert!(typescript.contains("export function acceptsIdentity"));
    assert!(typescript.contains("export class TicketDeskClient"));
    assert!(typescript.contains("executeNamedQuery"));
    assert!(typescript.contains("executeCommand"));
    assert!(
        typescript
            .contains("driverOperation: { name: \"ticketdesk_list_tickets\", inputSchemaHash: \"")
    );
    assert!(
        typescript
            .contains("driverOperation: { name: \"ticketdesk_create_ticket\", inputSchemaHash: \"")
    );
    assert!(typescript.contains("executeCommandBatch?<I, R>"));
    assert!(typescript.contains("this.transport.executeCommandBatch"));
    assert!(typescript.contains("export class RiffDbApplicationError"));
    assert!(typescript.contains("export function decodeApplicationError"));
    assert!(typescript.contains("\"RDB-AUTH-0214\""));
    assert!(typescript.contains("RiffDB application identity mismatch"));
    assert!(go.contains("package ticketdesk"));
    assert!(go.contains("func (client *Client) TicketPage("));
    assert!(go.contains("func (client *Client) CreateTicketBatch("));
    assert!(go.contains("ticketdesk_create_ticket"));
    assert!(python.contains("class TicketDeskClient:"));
    assert!(python.contains("class AsyncTicketDeskClient:"));
    assert!(python.contains("class CreateTicketInput:"));
    assert!(python.contains("def create_ticket_batch("));
    assert!(python.contains("CONTRACT_BUNDLE_HASH: Final[str]"));
    assert!(!rust.contains("pub field_id"));
    assert!(!typescript.contains("entity_type_id"));
}

#[test]
fn generated_workflow_commands_return_symbolic_successor_revisions() {
    let bundle = compile_contract_source(WORKFLOW_CONTRACT).expect("workflow contract");
    let candidate = QueryModuleCandidate::new(
        QueryModuleName::new("workflow_surface").expect("module name"),
        QueryModuleVersion::new(1).expect("module version"),
        vec![NamedQuerySource::new("GetWork", WORKFLOW_QUERY).expect("query source")],
    )
    .expect("module candidate");
    let module = QueryModule::compile(candidate, &bundle).expect("workflow module");

    let rust = generate_rust_client(&module, &bundle);
    let typescript = generate_typescript_client(&module, &bundle);
    let python = generate_python_client(&module, &bundle).expect("Python");
    let go = generate_go_client(&module, &bundle);

    assert!(rust.contains("fn workflow_successor_revisions"));
    assert!(rust.contains("WorkflowSuccessorRevision::generated(\"work\""));
    assert!(rust.contains("self.expected_revision.checked_add(1)"));
    assert!(rust.contains("response.outcome_type != \"Started\""));
    assert!(rust.contains("response.outcome_type != \"Claimed\""));
    assert!(typescript.contains("workflowRevisions"));
    assert!(typescript.contains("binding: \"work\""));
    assert!(python.contains("workflow_revisions"));
    assert!(python.contains("WorkflowSuccessorRevision(binding=\"work\""));
    assert!(go.contains("WorkflowRevisions"));
    assert!(go.contains("Binding: \"work\""));
}

#[test]
fn application_manifest_is_canonical_bounded_and_identity_bearing() {
    let source = include_str!("../../../fixtures/application-manifests/ticketdesk-v1.json");
    let manifest = ApplicationManifest::parse(source).expect("manifest");

    assert_eq!(manifest.application_name(), "ticketdesk");
    assert_eq!(manifest.contract().lineage(), "TicketDesk");
    assert_eq!(manifest.query_modules().len(), 1);
    assert_eq!(manifest.roles().len(), 2);
    assert_eq!(
        ApplicationManifest::decode_canonical(manifest.canonical_bytes())
            .expect("canonical round trip")
            .identity(),
        manifest.identity()
    );
    assert!(
        manifest
            .source_map()
            .span("contract.source")
            .is_some_and(|span| span.start() < span.end())
    );
    assert_ne!(
        manifest
            .source_map()
            .span("roles.TicketDeskAgent.environment"),
        manifest
            .source_map()
            .span("roles.TicketDeskApplication.environment")
    );

    let noncanonical = format!("\n{source}");
    let error = ApplicationManifest::decode_canonical(noncanonical.as_bytes())
        .expect_err("strict decoder rejects noncanonical source");
    assert_eq!(error.kind(), ManifestErrorKind::NonCanonical);
}

#[test]
fn unsupported_application_manifest_fixture_fails_closed() {
    let source = include_str!("../../../fixtures/application-manifests/pre-v1-unsupported.json");
    let error = ApplicationManifest::parse(source).expect_err("v0 must remain unsupported");
    assert!(matches!(
        error.kind(),
        ManifestErrorKind::InvalidShape | ManifestErrorKind::UnsupportedVersion
    ));
}
