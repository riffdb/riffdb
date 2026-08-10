#![cfg(target_os = "linux")]
#![forbid(unsafe_code)]

//! Cross-crate acceptance for the closed driver protocol and exact generated
//! operation catalog. Real verified-TLS transport is exercised by the
//! dependency gate `remote_deployment_rotation`; this test proves the new
//! local boundary cannot widen it.

use std::fs;

use riffdb_driver_host::{ApplicationCatalog, OperationKind, ReactiveKind};

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
