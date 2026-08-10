#![forbid(unsafe_code)]

//! Final P8 acceptance over the canonical reactive TicketDesk application.

use riffdb_contract_compiler::{compile_contract_source, compile_contract_successor};
use riffdb_query_ir::{ReactiveOperationPlanV1, ReactiveUpdateModeV1};
use riffdb_query_module::{
    APPLICATION_LOCK_SCHEMA_V6, APPLICATION_SOURCE_SCHEMA_V5, ApplicationLock,
    ApplicationSourceManifest, NamedQuerySource, QueryModule, QueryModuleCandidate,
    QueryModuleName, QueryModuleVersion, compile_application_role_v2, compile_reactive_source,
};
use riffdb_types::CapabilityPermissionKindV1;
use serde_json::Value;

const CONTRACT: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../examples/ticketdesk/riffdb/contract.riff"
));
const REACTIVE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../examples/ticketdesk/riffdb/reactive/ticket_activity.riffr"
));
const SOURCE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../examples/ticketdesk/riffdb.application.json"
));
const LOCK: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../examples/ticketdesk/riffdb.application.lock.json"
));
const RUST: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../examples/ticketdesk/generated/rust/client.rs"
));
const TYPESCRIPT: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../examples/ticketdesk/web/src/generated/client.ts"
));
const PYTHON: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../examples/ticketdesk/generated/python/client.py"
));
const MCP: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../examples/ticketdesk/generated/mcp/tools.json"
));

const QUERIES: [(&str, &str); 13] = [
    (
        "BoardPage200",
        include_str!("../examples/ticketdesk/riffdb/queries/board_page_200.riffq"),
    ),
    (
        "BoardPage450",
        include_str!("../examples/ticketdesk/riffdb/queries/board_page_450.riffq"),
    ),
    (
        "BoardPage50",
        include_str!("../examples/ticketdesk/riffdb/queries/board_page_50.riffq"),
    ),
    (
        "GetTicket",
        include_str!("../examples/ticketdesk/riffdb/queries/get_ticket.riffq"),
    ),
    (
        "GetUser",
        include_str!("../examples/ticketdesk/riffdb/queries/get_user.riffq"),
    ),
    (
        "ListComments",
        include_str!("../examples/ticketdesk/riffdb/queries/list_comments.riffq"),
    ),
    (
        "ListTickets",
        include_str!("../examples/ticketdesk/riffdb/queries/list_tickets.riffq"),
    ),
    (
        "ListTicketsByAssignee",
        include_str!("../examples/ticketdesk/riffdb/queries/list_tickets_by_assignee.riffq"),
    ),
    (
        "ProjectMembers",
        include_str!("../examples/ticketdesk/riffdb/queries/project_members.riffq"),
    ),
    (
        "ProjectSummary",
        include_str!("../examples/ticketdesk/riffdb/queries/project_summary.riffq"),
    ),
    (
        "TicketPage",
        include_str!("../examples/ticketdesk/riffdb/queries/ticket_page.riffq"),
    ),
    (
        "TicketPagePaged",
        include_str!("../examples/ticketdesk/riffdb/queries/ticket_page_paged.riffq"),
    ),
    (
        "TicketQueue",
        include_str!("../examples/ticketdesk/riffdb/queries/ticket_queue.riffq"),
    ),
];

fn application() -> (
    ApplicationSourceManifest,
    riffdb_contract_ir::ContractBundle,
    QueryModule,
    riffdb_query_ir::ReactiveModulePlanV1,
) {
    let source = ApplicationSourceManifest::parse(SOURCE).expect("TicketDesk Source V5");
    let contract = compile_contract_source(CONTRACT).expect("TicketDesk contract");
    let query_module = QueryModule::compile(
        QueryModuleCandidate::new(
            QueryModuleName::new("ticketdesk").expect("query module name"),
            QueryModuleVersion::new(1).expect("query module version"),
            QUERIES
                .iter()
                .map(|(name, text)| NamedQuerySource::new(*name, *text).expect("query source"))
                .collect(),
        )
        .expect("query module candidate"),
        &contract,
    )
    .expect("TicketDesk query module");
    let reactive =
        compile_reactive_source(REACTIVE, &contract, std::slice::from_ref(&query_module))
            .expect("TicketDesk reactive module");
    (source, contract, query_module, reactive)
}

#[test]
fn ticketdesk_v5_lock_freezes_partitioned_event_watches_context_and_roles() {
    let (source, contract, query_module, reactive) = application();
    assert_eq!(source.schema(), APPLICATION_SOURCE_SCHEMA_V5);
    assert_eq!(reactive.name(), "TicketActivity");
    assert_eq!(
        reactive
            .operations()
            .iter()
            .map(|operation| operation.name().as_str())
            .collect::<Vec<_>>(),
        [
            "TicketEvents",
            "TicketPageWatch",
            "TicketQueueWatch",
            "TriageTicket"
        ]
    );

    let stream = reactive.operation("TicketEvents").expect("event stream");
    let ReactiveOperationPlanV1::Stream {
        partition, events, ..
    } = stream.plan()
    else {
        panic!("TicketEvents is not a stream");
    };
    assert_eq!(partition.len(), 1);
    assert_eq!(partition[0].field(), "organization_id");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].name(), "TicketCreated");

    for (name, query, mode) in [
        ("TicketPageWatch", "TicketPage", ReactiveUpdateModeV1::Reset),
        (
            "TicketQueueWatch",
            "TicketQueue",
            ReactiveUpdateModeV1::Patch,
        ),
    ] {
        let ReactiveOperationPlanV1::Watch {
            query: dependency,
            update_mode,
            ..
        } = reactive.operation(name).expect("watch").plan()
        else {
            panic!("{name} is not a watch");
        };
        assert_eq!(dependency.query_name(), query);
        assert_eq!(*update_mode, mode);
    }

    let ReactiveOperationPlanV1::Subscription {
        hydrations,
        reactions,
        limits,
        ..
    } = reactive
        .operation("TriageTicket")
        .expect("subscription")
        .plan()
    else {
        panic!("TriageTicket is not contextual");
    };
    assert_eq!(hydrations.len(), 1);
    assert_eq!(hydrations[0].query_name(), "TicketPage");
    assert_eq!(reactions.len(), 1);
    assert_eq!(reactions[0].command_name(), "CreateComment");
    assert_eq!(
        (limits.batch(), limits.in_flight(), limits.lease_seconds()),
        (1, 4, 60)
    );

    let manifest = source
        .exact_manifest_v2(
            &contract,
            std::slice::from_ref(&query_module),
            std::slice::from_ref(&reactive),
        )
        .expect("exact reactive manifest");
    let lock = ApplicationLock::decode_canonical(LOCK).expect("canonical Lock V6");
    assert_eq!(lock.schema(), APPLICATION_LOCK_SCHEMA_V6);
    assert_eq!(lock.source_hash(), source.identity());
    assert_eq!(lock.manifest_hash(), manifest.identity());

    let seeder_role = compile_application_role_v2(
        &manifest,
        "TicketDeskSeeder",
        None,
        &contract,
        std::slice::from_ref(&query_module),
        std::slice::from_ref(&reactive),
    )
    .expect("seeder role");
    let seeder_permissions = seeder_role.internal_grant().permissions();
    assert!(seeder_permissions.contains_kind(CapabilityPermissionKindV1::InvokeCommand));
    assert!(!seeder_permissions.contains_kind(CapabilityPermissionKindV1::WatchNamedQuery));
    assert!(
        !seeder_permissions
            .contains_kind(CapabilityPermissionKindV1::ConsumeContextualSubscription)
    );

    let application_role = compile_application_role_v2(
        &manifest,
        "TicketDeskApplication",
        None,
        &contract,
        std::slice::from_ref(&query_module),
        std::slice::from_ref(&reactive),
    )
    .expect("application role");
    let app_permissions = application_role.internal_grant().permissions();
    assert!(app_permissions.contains_kind(CapabilityPermissionKindV1::WatchNamedQuery));
    assert!(
        !app_permissions.contains_kind(CapabilityPermissionKindV1::ConsumeContextualSubscription)
    );

    let agent_role = compile_application_role_v2(
        &manifest,
        "TicketDeskAgent",
        None,
        &contract,
        std::slice::from_ref(&query_module),
        std::slice::from_ref(&reactive),
    )
    .expect("agent role");
    let agent_permissions = agent_role.internal_grant().permissions();
    assert!(agent_permissions.contains_kind(CapabilityPermissionKindV1::ConsumeEventStream));
    assert!(
        agent_permissions.contains_kind(CapabilityPermissionKindV1::ConsumeContextualSubscription)
    );
    assert!(!agent_permissions.contains_kind(CapabilityPermissionKindV1::SeekEventStreamConsumer));
}

#[test]
fn generated_public_surfaces_are_exact_credential_free_and_redacted() {
    for required in [
        "TicketEventsConsumer",
        "watch_ticket_page_watch",
        "watch_ticket_queue_watch",
        "next_triage_ticket",
        "react_comment",
    ] {
        assert!(RUST.contains(required), "missing Rust surface {required}");
    }
    for required in [
        "TicketDeskReactiveClient",
        "createTicketPageWatchStore",
        "createTicketQueueWatchSseRelay",
        "nextTriageTicket",
        "reactComment",
        "\"Open\" | \"Closed\" | \"InProgress\"",
    ] {
        assert!(
            TYPESCRIPT.contains(required),
            "missing TypeScript surface {required}"
        );
    }
    for required in [
        "async def ticket_events",
        "async def watch_ticket_page_watch",
        "async def next_triage_ticket",
        "async def react_comment",
    ] {
        assert!(
            PYTHON.contains(required),
            "missing Python surface {required}"
        );
    }

    let catalog: Value = serde_json::from_str(MCP).expect("generated MCP catalog");
    let tools = catalog["reactive_tools"]
        .as_array()
        .expect("reactive tools");
    assert_eq!(tools.len(), 12);
    assert!(tools.iter().all(|tool| {
        tool["name"].as_str().is_some_and(|name| {
            name.bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        })
    }));
    assert!(
        tools
            .iter()
            .any(|tool| tool["name"] == "ticket_activity_triage_ticket_react_comment")
    );

    for artifact in [RUST, TYPESCRIPT, PYTHON, MCP] {
        for forbidden in [
            "raw_principal",
            "session_identity",
            "partition_key_hash",
            "conflict_key",
            "process_trace_id",
        ] {
            assert!(
                !artifact.contains(forbidden),
                "public artifact disclosed {forbidden}"
            );
        }
    }
    for browser in [
        include_str!("../examples/ticketdesk/web/public/index.html"),
        include_str!("../examples/ticketdesk/web/public/app.js"),
    ] {
        assert!(!browser.contains("credential"));
        assert!(!browser.contains("lease_token"));
        assert!(!browser.contains("causation_token"));
    }
}

#[test]
fn definition_and_contract_evolution_create_new_exact_identities() {
    let (_, contract, query_module, reactive) = application();
    let evolved_reactive_source = REACTIVE.replacen(
        "TicketActivity version 1",
        "TicketActivityNext version 1",
        1,
    );
    let evolved = compile_reactive_source(
        &evolved_reactive_source,
        &contract,
        std::slice::from_ref(&query_module),
    )
    .expect("reactive version evolution");
    assert_ne!(evolved.identity(), reactive.identity());
    assert_eq!(
        evolved
            .operation("TriageTicket")
            .expect("evolved operation")
            .identity(),
        reactive
            .operation("TriageTicket")
            .expect("original operation")
            .identity(),
        "operation meaning remains stable while module identity changes"
    );

    let successor_source = CONTRACT
        .replacen("contract TicketDesk version 1", "contract TicketDesk version 2", 1)
        .replacen(
            "    status: TicketStatus\n  }\n\n  aggregate Organizations",
            "    status: TicketStatus\n    triage_hint: optional<string<32>>\n  }\n\n  aggregate Organizations",
            1,
        );
    let successor = compile_contract_successor(&successor_source, &contract)
        .expect("optional event-field successor");
    assert_ne!(successor.bundle_hash(), contract.bundle_hash());
}

#[test]
fn canonical_example_sources_do_not_drift_from_the_long_lived_ticketdesk_corpus() {
    assert_eq!(
        CONTRACT,
        include_str!("../examples/app-baseline/contracts/ticketdesk.riff")
    );
    for (name, copied) in QUERIES {
        let original = match name {
            "BoardPage200" => include_str!("../queries/ticketdesk/board_page_200.riffq"),
            "BoardPage450" => include_str!("../queries/ticketdesk/board_page_450.riffq"),
            "BoardPage50" => include_str!("../queries/ticketdesk/board_page_50.riffq"),
            "GetTicket" => include_str!("../queries/ticketdesk/get_ticket.riffq"),
            "GetUser" => include_str!("../queries/ticketdesk/get_user.riffq"),
            "ListComments" => include_str!("../queries/ticketdesk/list_comments.riffq"),
            "ListTickets" => include_str!("../queries/ticketdesk/list_tickets.riffq"),
            "ListTicketsByAssignee" => {
                include_str!("../queries/ticketdesk/list_tickets_by_assignee.riffq")
            }
            "ProjectMembers" => include_str!("../queries/ticketdesk/project_members.riffq"),
            "ProjectSummary" => include_str!("../queries/ticketdesk/project_summary.riffq"),
            "TicketPage" => include_str!("../queries/ticketdesk/ticket_page.riffq"),
            "TicketPagePaged" => include_str!("../queries/ticketdesk/ticket_page_paged.riffq"),
            "TicketQueue" => include_str!("../queries/ticketdesk/ticket_queue.riffq"),
            _ => unreachable!("closed TicketDesk query inventory"),
        };
        assert_eq!(copied, original, "copied query {name} drifted");
    }
}
