#![forbid(unsafe_code)]

//! Symbolic application-role authority tests.

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_module::{
    ApplicationManifest, ApplicationRoleErrorKind, ApplicationRoleOperationKind, NamedQuerySource,
    QueryModule, QueryModuleCandidate, QueryModuleName, QueryModuleVersion,
    compile_application_role,
};
use riffdb_types::{
    CapabilityPermissionKindV1, CapabilityPermissionV1, PartitionScopeV1, TenantId, TenantScope,
};

const CONTRACT: &str = include_str!("../../../examples/app-baseline/contracts/ticketdesk.riff");
const MANIFEST: &str = include_str!("../../../fixtures/application-manifests/ticketdesk-v1.json");
const QUERIES: [(&str, &str); 8] = [
    (
        "GetTicket",
        include_str!("../../../queries/ticketdesk/get_ticket.riffq"),
    ),
    (
        "GetUser",
        include_str!("../../../queries/ticketdesk/get_user.riffq"),
    ),
    (
        "ListComments",
        include_str!("../../../queries/ticketdesk/list_comments.riffq"),
    ),
    (
        "ListTickets",
        include_str!("../../../queries/ticketdesk/list_tickets.riffq"),
    ),
    (
        "ListTicketsByAssignee",
        include_str!("../../../queries/ticketdesk/list_tickets_by_assignee.riffq"),
    ),
    (
        "ProjectMembers",
        include_str!("../../../queries/ticketdesk/project_members.riffq"),
    ),
    (
        "ProjectSummary",
        include_str!("../../../queries/ticketdesk/project_summary.riffq"),
    ),
    (
        "TicketPage",
        include_str!("../../../queries/ticketdesk/ticket_page.riffq"),
    ),
];

fn exact_application() -> (
    ApplicationManifest,
    riffdb_contract_ir::ContractBundle,
    QueryModule,
) {
    let manifest = ApplicationManifest::decode_canonical(MANIFEST.as_bytes()).expect("manifest");
    let contract = compile_contract_source(CONTRACT).expect("contract");
    let candidate = QueryModuleCandidate::new(
        QueryModuleName::new("ticketdesk").expect("module name"),
        QueryModuleVersion::new(1).expect("module version"),
        QUERIES
            .into_iter()
            .map(|(name, source)| NamedQuerySource::new(name, source).expect("query"))
            .collect(),
    )
    .expect("candidate");
    let module = QueryModule::compile(candidate, &contract).expect("module");
    (manifest, contract, module)
}

#[test]
fn symbolic_role_lowers_only_to_exact_application_operations() {
    let (manifest, contract, module) = exact_application();
    let role = compile_application_role(&manifest, "TicketDeskAgent", None, &contract, &[module])
        .expect("role");

    assert_eq!(role.application_name(), "ticketdesk");
    assert_eq!(role.role_name(), "TicketDeskAgent");
    assert_eq!(role.environment().as_str(), "development");
    assert_eq!(role.tenant_scope(), &TenantScope::Global);
    assert_eq!(role.operations().len(), 16);
    assert!(role.operations().iter().any(|operation| {
        operation.kind() == ApplicationRoleOperationKind::Query && operation.name() == "TicketPage"
    }));

    let grant = role.internal_grant();
    assert!(matches!(grant.partition_scope(), PartitionScopeV1::All));
    assert!(grant.approval_required().is_empty());
    assert!(!grant.field_visibility().is_empty());
    assert!(grant.permissions().as_slice().iter().all(|permission| {
        matches!(
            permission,
            CapabilityPermissionV1::ExecuteNamedQuery(..)
                | CapabilityPermissionV1::InvokeCommand(..)
                | CapabilityPermissionV1::ApplicationRoleIdentity(..)
        )
    }));
    assert!(grant.permissions().as_slice().iter().any(|permission| {
        matches!(
            permission,
            CapabilityPermissionV1::ApplicationRoleIdentity(hash) if *hash == role.identity()
        )
    }));
    for forbidden in [
        CapabilityPermissionKindV1::ReadContract,
        CapabilityPermissionKindV1::ReadEntity,
        CapabilityPermissionKindV1::ScanIndex,
        CapabilityPermissionKindV1::CheckAdHocQuery,
        CapabilityPermissionKindV1::ExplainAdHocQuery,
        CapabilityPermissionKindV1::ExecuteAdHocQuery,
        CapabilityPermissionKindV1::CreateCapability,
        CapabilityPermissionKindV1::AdministerCapabilities,
    ] {
        assert!(!grant.permissions().contains_kind(forbidden));
    }
}

#[test]
fn role_identity_and_authority_fail_closed_on_substitution() {
    let (manifest, contract, module) = exact_application();
    let original = compile_application_role(
        &manifest,
        "TicketDeskAgent",
        None,
        &contract,
        std::slice::from_ref(&module),
    )
    .expect("original");
    let application = compile_application_role(
        &manifest,
        "TicketDeskApplication",
        None,
        &contract,
        std::slice::from_ref(&module),
    )
    .expect("second role");
    assert_ne!(original.identity(), application.identity());

    let stale = QueryModule::compile(
        QueryModuleCandidate::new(
            QueryModuleName::new("ticketdesk").expect("name"),
            QueryModuleVersion::new(2).expect("version"),
            QUERIES
                .into_iter()
                .map(|(name, source)| NamedQuerySource::new(name, source).expect("query"))
                .collect(),
        )
        .expect("candidate"),
        &contract,
    )
    .expect("stale module");
    let error = compile_application_role(&manifest, "TicketDeskAgent", None, &contract, &[stale])
        .expect_err("stale module rejected");
    assert_eq!(error.kind(), ApplicationRoleErrorKind::ModuleMismatch);

    let error = compile_application_role(
        &manifest,
        "TicketDeskAgent",
        Some(TenantId::new("injected").expect("tenant")),
        &contract,
        &[module],
    )
    .expect_err("scope injection rejected");
    assert_eq!(
        error.kind(),
        ApplicationRoleErrorKind::TenantBindingMismatch
    );
}
