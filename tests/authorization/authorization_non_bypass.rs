#![forbid(unsafe_code)]

//! End-to-end authorization and dependency-boundary evidence.

use std::num::NonZeroU16;

use riffdb_policy::{
    AgentSessionAdmissionPolicy, ApplicationQueryAccessRequirement, ApplicationQueryTarget,
    AuthorizationClock, AuthorizationClockError, AuthorizationError, CommandExecutionClass,
    CommandToolCandidate, CurrentAuthorizer, Decision, DiscoveryVisibility, FixedToolCandidate,
    NamedQueryToolCandidate, NoopAuthorizationTelemetry, OperationRequest, OperationTenantScope,
    PolicyCode, admit_invocation_claims,
};
use riffdb_testkit::authorization::{
    AuthorizationFixture, AuthorizationFixtureConfig, AuthorizationFixtureTimes,
};
use riffdb_types::{
    ActorId, ActorKind, AgentSessionId, AggregateTypeId, ApplicationRoleHash, Audience,
    CapabilityGrantV1, CapabilityPermissionKindV1, CapabilityPermissionV1, CapabilityPermissionsV1,
    CommandId, ContractBundleHash, ContractLineage, ContractVersion, DatabaseId,
    EntityFieldVisibilityV1, EntityTypeId, Environment, FieldId, IndexId, PartitionKey,
    PartitionKeyBuilder, PartitionScopeV1, ProjectionId, ProvenanceReason, QueryCostVectorV1,
    QueryModuleHash, QueryOperationName, QueryPlanHash, ReactiveModuleHash, ReactiveOperationName,
    ServiceIngressKindV1, SourceCommit, SourceRepository, TenantScope, Timestamp,
};

const POLICY_MANIFEST: &str = include_str!("../../crates/riffdb-policy/Cargo.toml");
const GRPC_MANIFEST: &str = include_str!("../../crates/riffdb-api-grpc/Cargo.toml");
const MCP_MANIFEST: &str = include_str!("../../crates/riffdb-api-mcp/Cargo.toml");

struct FixedClock(Timestamp);

impl AuthorizationClock for FixedClock {
    fn now(&self) -> Result<Timestamp, AuthorizationClockError> {
        Ok(self.0)
    }
}

fn timestamp(seconds: i64) -> Timestamp {
    Timestamp::new(seconds, 7).expect("valid timestamp")
}

fn database_id() -> DatabaseId {
    DatabaseId::from_unix_milliseconds_and_random(1, [0x41; 10]).expect("valid UUIDv7")
}

fn environment() -> Environment {
    Environment::new("test").expect("valid environment")
}

fn audience() -> Audience {
    Audience::new("riffdb-test").expect("valid audience")
}

fn lineage() -> ContractLineage {
    ContractLineage::new("example.contract").expect("valid lineage")
}

fn partition() -> PartitionKey {
    let mut builder = PartitionKeyBuilder::new(AggregateTypeId::first());
    builder.push_u64(7).expect("bounded component");
    builder.finish().expect("valid partition")
}

fn grant() -> CapabilityGrantV1 {
    let lineage = lineage();
    CapabilityGrantV1::new(
        TenantScope::Global,
        PartitionScopeV1::All,
        CapabilityPermissionsV1::new(vec![
            CapabilityPermissionV1::InvokeCommand(lineage.clone(), CommandId::first()),
            CapabilityPermissionV1::ReadEntity(lineage.clone(), EntityTypeId::first()),
            CapabilityPermissionV1::unparameterized(
                riffdb_types::CapabilityPermissionKindV1::ReadHealth,
            )
            .expect("unparameterized permission"),
        ])
        .expect("canonical permissions"),
        vec![
            EntityFieldVisibilityV1::new(lineage, EntityTypeId::first(), vec![FieldId::first()])
                .expect("field visibility"),
        ],
        NonZeroU16::new(37).expect("nonzero row bound"),
        Vec::new(),
    )
    .expect("valid grant")
}

fn fixture(actor_kind: ActorKind) -> AuthorizationFixture {
    fixture_with_grant(actor_kind, grant())
}

fn fixture_with_grant(actor_kind: ActorKind, grant: CapabilityGrantV1) -> AuthorizationFixture {
    AuthorizationFixture::new(AuthorizationFixtureConfig::new(
        database_id(),
        environment(),
        ActorId::new("authorization-principal").expect("valid actor ID"),
        actor_kind,
        audience(),
        AuthorizationFixtureTimes::new(timestamp(100), timestamp(200), timestamp(150)),
        grant,
    ))
    .expect("authorization fixture")
}

fn stable_application_grant() -> CapabilityGrantV1 {
    let lineage = lineage();
    CapabilityGrantV1::new(
        TenantScope::Global,
        PartitionScopeV1::All,
        CapabilityPermissionsV1::new(vec![
            CapabilityPermissionV1::InvokeCommand(lineage.clone(), CommandId::first()),
            CapabilityPermissionV1::ExecuteNamedQuery(
                lineage.clone(),
                QueryModuleHash::from_bytes([0x31; 32]),
                QueryOperationName::new("TicketPage").expect("query name"),
            ),
        ])
        .expect("canonical permissions"),
        vec![
            EntityFieldVisibilityV1::new(lineage, EntityTypeId::first(), vec![FieldId::first()])
                .expect("field visibility"),
        ],
        NonZeroU16::new(37).expect("nonzero row bound"),
        Vec::new(),
    )
    .expect("stable application grant")
}

fn catalog_test_permissions() -> CapabilityPermissionsV1 {
    use CapabilityPermissionKindV1 as Kind;

    let lineage = lineage();
    let module_hash = QueryModuleHash::from_bytes([0x31; 32]);
    let query_name = QueryOperationName::new("TicketPage").expect("query name");
    let reactive_hash = ReactiveModuleHash::from_bytes([0x51; 32]);
    let reactive_name = ReactiveOperationName::new("CatalogWitness").expect("reactive name");
    let mut permissions = [
        Kind::ValidateContract,
        Kind::ReadContract,
        Kind::DeployContract,
        Kind::ReadCommit,
        Kind::ScanCommits,
        Kind::ReadProvenance,
        Kind::InspectOutbox,
        Kind::ReadHealth,
        Kind::CheckAdHocQuery,
        Kind::ExplainAdHocQuery,
        Kind::ExecuteAdHocQuery,
        Kind::InspectVectorState,
    ]
    .into_iter()
    .map(|kind| CapabilityPermissionV1::unparameterized(kind).expect("fixed-kind witness"))
    .collect::<Vec<_>>();
    permissions.extend([
        CapabilityPermissionV1::ExplainCommand(lineage.clone(), CommandId::first()),
        CapabilityPermissionV1::InvokeCommand(lineage.clone(), CommandId::first()),
        CapabilityPermissionV1::ReadEntity(lineage.clone(), EntityTypeId::first()),
        CapabilityPermissionV1::ScanIndex(lineage.clone(), IndexId::first()),
        CapabilityPermissionV1::QueryProjection(lineage.clone(), ProjectionId::first()),
        CapabilityPermissionV1::ReadProjectionStatus(lineage.clone(), ProjectionId::first()),
        CapabilityPermissionV1::ConsumeEventStream(
            lineage.clone(),
            reactive_hash,
            reactive_name.clone(),
        ),
        CapabilityPermissionV1::SeekEventStreamConsumer(
            lineage.clone(),
            reactive_hash,
            reactive_name.clone(),
        ),
        CapabilityPermissionV1::WatchNamedQuery(
            lineage.clone(),
            reactive_hash,
            reactive_name.clone(),
        ),
        CapabilityPermissionV1::ConsumeContextualSubscription(
            lineage.clone(),
            reactive_hash,
            reactive_name,
        ),
        CapabilityPermissionV1::ExecuteNamedQuery(lineage, module_hash, query_name),
    ]);
    CapabilityPermissionsV1::new(permissions).expect("canonical exact catalog permissions")
}

fn catalog_test_grant() -> CapabilityGrantV1 {
    CapabilityGrantV1::new(
        TenantScope::Global,
        PartitionScopeV1::All,
        catalog_test_permissions(),
        Vec::new(),
        NonZeroU16::new(u16::MAX).expect("nonzero maximum"),
        Vec::new(),
    )
    .expect("closed catalog test grant")
}

fn application_query_target() -> ApplicationQueryTarget {
    ApplicationQueryTarget::new(
        lineage(),
        ContractVersion::new(1).expect("nonzero version"),
        ContractBundleHash::from_bytes([0x21; 32]),
        QueryPlanHash::from_bytes([0x41; 32]),
        ServiceIngressKindV1::InProcessTestComparison,
        OperationTenantScope::global_only(),
        partition(),
        vec![
            ApplicationQueryAccessRequirement::new(
                EntityTypeId::first(),
                None,
                vec![FieldId::first()],
                NonZeroU16::new(1).expect("nonzero bound"),
            )
            .expect("query access"),
        ],
        QueryCostVectorV1::new(1, 0, 1, 0, 1, 1, 128).expect("query cost"),
    )
    .expect("application query target")
}

fn authorize(
    fixture: &AuthorizationFixture,
    expected_environment: Environment,
    now: Timestamp,
    request: OperationRequest,
) -> Result<Decision, AuthorizationError> {
    let resolver = fixture.current_capability_resolver();
    CurrentAuthorizer::new(
        &resolver,
        &FixedClock(now),
        &NoopAuthorizationTelemetry,
        database_id(),
        expected_environment,
    )
    .authorize(fixture.authenticated_principal(), request)
}

fn allowed(decision: Decision) -> Box<riffdb_policy::AuthorizedOperation> {
    let Decision::Allow(proof) = decision else {
        panic!("expected an allow decision");
    };
    proof
}

#[test]
fn real_authentication_state_is_rechecked_at_every_policy_safe_point() {
    let fixture = fixture(ActorKind::Agent);
    let health = OperationRequest::get_health();
    assert!(
        allowed(authorize(&fixture, environment(), timestamp(160), health.clone()).unwrap())
            .request()
            == &health
    );

    assert_eq!(
        authorize(
            &fixture,
            Environment::new("other").expect("valid environment"),
            timestamp(160),
            health.clone(),
        )
        .unwrap(),
        Decision::Deny(PolicyCode::InactiveOrStaleCapability)
    );
    assert_eq!(
        authorize(&fixture, environment(), timestamp(200), health.clone()).unwrap(),
        Decision::Deny(PolicyCode::InactiveOrStaleCapability)
    );

    fixture
        .revoke_current(timestamp(170))
        .expect("revoke fixture");
    assert_eq!(
        authorize(&fixture, environment(), timestamp(171), health.clone()).unwrap(),
        Decision::Deny(PolicyCode::InactiveOrStaleCapability)
    );

    fixture.make_current_missing().expect("hide current state");
    assert_eq!(
        authorize(&fixture, environment(), timestamp(171), health),
        Err(AuthorizationError::CurrentCapabilityUnavailable)
    );
}

#[test]
fn exact_command_scope_and_field_obligations_cannot_be_bypassed() {
    let fixture = fixture(ActorKind::Agent);
    let execute = OperationRequest::execute_command(
        lineage(),
        ContractVersion::new(1).expect("nonzero version"),
        CommandId::first(),
        CommandExecutionClass::Mutation,
        partition(),
    );
    let proof = allowed(authorize(&fixture, environment(), timestamp(160), execute).unwrap());
    assert_eq!(proof.database_id(), database_id());
    assert_eq!(proof.environment(), &environment());
    assert!(proof.obligations().partition_constraint().is_some());

    let wrong_command = OperationRequest::execute_command(
        lineage(),
        ContractVersion::new(1).expect("nonzero version"),
        CommandId::new(2).expect("nonzero command"),
        CommandExecutionClass::Mutation,
        partition(),
    );
    assert_eq!(
        authorize(&fixture, environment(), timestamp(160), wrong_command).unwrap(),
        Decision::Deny(PolicyCode::MissingPermission)
    );

    let entity = OperationRequest::get_entity(
        lineage(),
        ContractVersion::new(1).expect("nonzero version"),
        EntityTypeId::first(),
        OperationTenantScope::global_only(),
        partition(),
        vec![FieldId::first(), FieldId::new(2).expect("nonzero field")],
    )
    .expect("bounded field request");
    let proof = allowed(authorize(&fixture, environment(), timestamp(160), entity).unwrap());
    assert_eq!(
        proof
            .obligations()
            .field_mask()
            .expect("entity field mask")
            .fields(),
        &[FieldId::first()]
    );
}

#[test]
fn discovery_is_visibility_only_and_each_invocation_reauthorizes() {
    let fixture = fixture(ActorKind::Agent);
    let proof = allowed(
        authorize(
            &fixture,
            environment(),
            timestamp(160),
            OperationRequest::discover_command_tools(),
        )
        .unwrap(),
    );
    let candidate = CommandToolCandidate::new(lineage(), CommandId::first());
    let visibility = proof
        .into_discovery()
        .expect("discovery proof")
        .tool_catalog(FixedToolCandidate::ALL.as_slice(), &[candidate], &[])
        .expect("bounded catalog");
    assert_eq!(visibility.command_tools(), &[DiscoveryVisibility::Visible]);

    fixture
        .revoke_current(timestamp(170))
        .expect("revoke fixture");
    let execute = OperationRequest::execute_command(
        lineage(),
        ContractVersion::new(1).expect("nonzero version"),
        CommandId::first(),
        CommandExecutionClass::Mutation,
        partition(),
    );
    assert_eq!(
        authorize(&fixture, environment(), timestamp(171), execute).unwrap(),
        Decision::Deny(PolicyCode::InactiveOrStaleCapability)
    );
}

#[test]
fn stable_application_authority_and_catalog_are_named_only() {
    let fixture = fixture_with_grant(ActorKind::Service, stable_application_grant());
    let module_hash = QueryModuleHash::from_bytes([0x31; 32]);
    let query_name = QueryOperationName::new("TicketPage").expect("query name");
    let exact = OperationRequest::execute_named_query(
        lineage(),
        module_hash,
        query_name.clone(),
        application_query_target(),
    )
    .expect("exact named request");
    allowed(authorize(&fixture, environment(), timestamp(160), exact).expect("authorization"));

    for rejected in [
        OperationRequest::execute_ad_hoc_query(application_query_target()),
        OperationRequest::execute_named_query(
            lineage(),
            QueryModuleHash::from_bytes([0x32; 32]),
            query_name.clone(),
            application_query_target(),
        )
        .expect("wrong module request"),
        OperationRequest::execute_named_query(
            lineage(),
            module_hash,
            QueryOperationName::new("OtherPage").expect("query name"),
            application_query_target(),
        )
        .expect("wrong query request"),
        OperationRequest::get_entity(
            lineage(),
            ContractVersion::new(1).expect("nonzero version"),
            EntityTypeId::first(),
            OperationTenantScope::global_only(),
            partition(),
            vec![FieldId::first()],
        )
        .expect("kernel request"),
    ] {
        assert_eq!(
            authorize(&fixture, environment(), timestamp(160), rejected)
                .expect("closed authorization"),
            Decision::Deny(PolicyCode::MissingPermission)
        );
    }

    let discovery = allowed(
        authorize(
            &fixture,
            environment(),
            timestamp(160),
            OperationRequest::discover_command_tools(),
        )
        .expect("discovery authorization"),
    )
    .into_discovery()
    .expect("discovery proof")
    .tool_catalog(
        FixedToolCandidate::ALL.as_slice(),
        &[CommandToolCandidate::new(lineage(), CommandId::first())],
        &[],
    )
    .expect("catalog");
    for hidden in [
        FixedToolCandidate::GetEntity,
        FixedToolCandidate::ScanIndex,
        FixedToolCandidate::CheckQuery,
        FixedToolCandidate::ExplainQuery,
        FixedToolCandidate::ExecuteQuery,
    ] {
        let index = FixedToolCandidate::ALL
            .iter()
            .position(|candidate| *candidate == hidden)
            .expect("candidate");
        assert_eq!(discovery.fixed_tools()[index], DiscoveryVisibility::Hidden);
    }
    assert_eq!(discovery.command_tools(), &[DiscoveryVisibility::Visible]);
}

// req: MCP-001, MCP-020, MCP-021, MCP-026, MCP-040, MCP-043, MCP-045, DX-044, DX-047, DX-049
#[test]
fn generated_catalog_role_identity_and_test_capability_have_separate_authority() {
    let command = CommandToolCandidate::new(lineage(), CommandId::first());
    let query = NamedQueryToolCandidate::new(
        lineage(),
        QueryModuleHash::from_bytes([0x31; 32]),
        QueryOperationName::new("TicketPage").expect("query name"),
    );
    let role_only = CapabilityGrantV1::new(
        TenantScope::Global,
        PartitionScopeV1::All,
        CapabilityPermissionsV1::new(vec![CapabilityPermissionV1::ApplicationRoleIdentity(
            ApplicationRoleHash::from_bytes([0x44; 32]),
        )])
        .expect("role identity"),
        Vec::new(),
        NonZeroU16::new(u16::MAX).expect("nonzero maximum"),
        Vec::new(),
    )
    .expect("role-only grant");
    let role_visibility = allowed(
        authorize(
            &fixture_with_grant(ActorKind::Service, role_only),
            environment(),
            timestamp(160),
            OperationRequest::discover_command_tools(),
        )
        .expect("role-only discovery"),
    )
    .into_discovery()
    .expect("discovery proof")
    .tool_catalog(
        FixedToolCandidate::ALL.as_slice(),
        std::slice::from_ref(&command),
        std::slice::from_ref(&query),
    )
    .expect("role candidate catalog");
    assert!(
        role_visibility
            .fixed_tools()
            .iter()
            .all(|visibility| *visibility == DiscoveryVisibility::Hidden)
    );
    assert_eq!(
        role_visibility.command_tools(),
        &[DiscoveryVisibility::Hidden]
    );
    assert_eq!(
        role_visibility.named_query_tools(),
        &[DiscoveryVisibility::Hidden]
    );

    let grant = catalog_test_grant();
    assert_eq!(grant.tenant_scope(), &TenantScope::Global);
    assert_eq!(grant.partition_scope(), &PartitionScopeV1::All);
    assert!(grant.field_visibility().is_empty());
    assert_eq!(grant.max_scan_rows().get(), u16::MAX);
    assert!(grant.approval_required().is_empty());
    assert!(grant.internal_row_policy().is_none());
    assert!(grant.internal_export().is_none());
    assert!(grant.internal_reimport().is_none());
    assert!(grant.internal_vector_inspection().is_none());

    let visibility = allowed(
        authorize(
            &fixture_with_grant(ActorKind::Service, grant),
            environment(),
            timestamp(160),
            OperationRequest::discover_command_tools(),
        )
        .expect("exact catalog discovery"),
    )
    .into_discovery()
    .expect("discovery proof")
    .tool_catalog(FixedToolCandidate::ALL.as_slice(), &[command], &[query])
    .expect("policy-filtered catalog");
    assert_eq!(
        visibility.fixed_tools(),
        vec![DiscoveryVisibility::Visible; FixedToolCandidate::ALL.len()]
    );
    assert_eq!(visibility.command_tools(), &[DiscoveryVisibility::Visible]);
    assert_eq!(
        visibility.named_query_tools(),
        &[DiscoveryVisibility::Visible]
    );
}

#[test]
fn untrusted_claims_are_discarded_except_for_explicit_agent_session_policy() {
    let claims = || {
        riffdb_policy::UntrustedInvocationClaims::new(
            Some(SourceRepository::new("private/repository").expect("bounded repository")),
            Some(SourceCommit::new("0123456789abcdef").expect("bounded commit")),
            Some(ProvenanceReason::new("caller reason").expect("bounded reason")),
            None,
            Some(
                AgentSessionId::from_unix_milliseconds_and_random(2, [0x51; 10])
                    .expect("valid UUIDv7"),
            ),
        )
    };
    let agent = fixture(ActorKind::Agent);
    let discarded = admit_invocation_claims(
        agent.authenticated_principal(),
        claims(),
        AgentSessionAdmissionPolicy::Discard,
    );
    assert!(discarded.provenance().source_repository().is_none());
    assert!(discarded.provenance().source_commit().is_none());
    assert!(discarded.provenance().reason().is_none());
    assert!(discarded.provenance().approval_id().is_none());
    assert!(discarded.agent_session_id().is_none());

    let retained = admit_invocation_claims(
        agent.authenticated_principal(),
        claims(),
        AgentSessionAdmissionPolicy::AllowForAgent,
    );
    assert!(retained.agent_session_id().is_some());
    assert!(retained.provenance().source_repository().is_none());

    let human = fixture(ActorKind::Human);
    let rejected_for_human = admit_invocation_claims(
        human.authenticated_principal(),
        claims(),
        AgentSessionAdmissionPolicy::AllowForAgent,
    );
    assert!(rejected_for_human.agent_session_id().is_none());
}

fn production_dependencies(manifest: &str) -> Vec<&str> {
    manifest
        .lines()
        .skip_while(|line| *line != "[dependencies]")
        .skip(1)
        .take_while(|line| !line.starts_with('['))
        .filter_map(|line| line.split_once('=').map(|(name, _)| name.trim()))
        .filter(|name| !name.is_empty())
        .collect()
}

#[test]
fn transports_cannot_bypass_service_owned_authorization() {
    assert_eq!(
        production_dependencies(POLICY_MANIFEST),
        [
            "riffdb-observability",
            "riffdb-auth",
            "riffdb-contract-ir",
            "riffdb-types",
        ]
    );
    for (adapter, manifest) in [("gRPC", GRPC_MANIFEST), ("MCP", MCP_MANIFEST)] {
        let dependencies = production_dependencies(manifest);
        for forbidden in [
            "riffdb-policy",
            "riffdb-commit",
            "riffdb-runtime",
            "riffdb-storage-api",
            "riffdb-storage-redb",
        ] {
            assert!(
                !dependencies.contains(&forbidden),
                "{adapter} adapter bypasses the shared service through {forbidden}"
            );
        }
    }
}
