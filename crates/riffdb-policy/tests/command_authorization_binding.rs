#![forbid(unsafe_code)]

//! Public-boundary tests for exact command authorization binding.

use std::num::NonZeroU16;

use riffdb_policy::{
    AgentSessionAdmissionPolicy, AuthorizationClock, AuthorizationClockError,
    CatalogDeploymentAuthorizationBindingError, CommandAuthorizationBindingError,
    CommandExecutionClass, CurrentAuthorizer, Decision, NoopAuthorizationTelemetry,
    OperationRequest, PolicyCode, UntrustedInvocationClaims,
};
use riffdb_testkit::authorization::{
    AuthorizationFixture, AuthorizationFixtureConfig, AuthorizationFixtureTimes,
};
use riffdb_types::{
    ActorId, ActorKind, AgentSessionId, AggregateTypeId, Audience, CapabilityGrantV1,
    CapabilityPermissionKindV1, CapabilityPermissionV1, CapabilityPermissionsV1, CommandId,
    ContractBundleHash, ContractLineage, ContractVersion, DatabaseId, Environment, PartitionKey,
    PartitionKeyBuilder, PartitionScopeV1, ProvenanceReason, SourceCommit, SourceRepository,
    TenantScope, Timestamp,
};

struct FixedClock(Timestamp);

impl AuthorizationClock for FixedClock {
    fn now(&self) -> Result<Timestamp, AuthorizationClockError> {
        Ok(self.0)
    }
}

fn timestamp(seconds: i64) -> Timestamp {
    Timestamp::new(seconds, 0).expect("valid timestamp")
}

fn database_id() -> DatabaseId {
    DatabaseId::from_unix_milliseconds_and_random(1, [0x11; 10]).expect("valid UUIDv7")
}

fn environment() -> Environment {
    Environment::new("command-binding-test").expect("bounded environment")
}

fn lineage() -> ContractLineage {
    ContractLineage::new("example.command-binding").expect("bounded lineage")
}

fn bundle_hash(seed: u8) -> ContractBundleHash {
    ContractBundleHash::from_bytes([seed; 32])
}

fn partition(value: u64) -> PartitionKey {
    let mut builder = PartitionKeyBuilder::new(AggregateTypeId::first());
    builder.push_u64(value).expect("bounded component");
    builder.finish().expect("valid partition")
}

fn session_id() -> AgentSessionId {
    AgentSessionId::from_unix_milliseconds_and_random(2, [0x22; 10]).expect("valid UUIDv7")
}

fn claims() -> UntrustedInvocationClaims {
    UntrustedInvocationClaims::new(
        Some(SourceRepository::new("private/repository").expect("bounded repository")),
        Some(SourceCommit::new("0123456789abcdef").expect("bounded commit")),
        Some(ProvenanceReason::new("caller supplied reason").expect("bounded reason")),
        None,
        Some(session_id()),
    )
}

fn fixture(actor_kind: ActorKind, principal_id: &str) -> AuthorizationFixture {
    let grant = CapabilityGrantV1::new(
        TenantScope::Global,
        PartitionScopeV1::All,
        CapabilityPermissionsV1::new(vec![
            CapabilityPermissionV1::InvokeCommand(lineage(), CommandId::first()),
            CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::DeployContract)
                .expect("unparameterized permission"),
            CapabilityPermissionV1::unparameterized(CapabilityPermissionKindV1::ReadHealth)
                .expect("unparameterized permission"),
        ])
        .expect("canonical permissions"),
        Vec::new(),
        NonZeroU16::new(10).expect("nonzero row bound"),
        Vec::new(),
    )
    .expect("valid grant");
    AuthorizationFixture::new(AuthorizationFixtureConfig::new(
        database_id(),
        environment(),
        ActorId::new(principal_id).expect("bounded principal"),
        actor_kind,
        Audience::new("riffdb-test").expect("bounded audience"),
        AuthorizationFixtureTimes::new(timestamp(100), timestamp(300), timestamp(150)),
        grant,
    ))
    .expect("valid fixture")
}

fn authorize(
    fixture: &AuthorizationFixture,
    request: OperationRequest,
) -> Result<Decision, riffdb_policy::AuthorizationError> {
    let resolver = fixture.current_capability_resolver();
    CurrentAuthorizer::new(
        &resolver,
        &FixedClock(timestamp(200)),
        &NoopAuthorizationTelemetry,
        database_id(),
        environment(),
    )
    .authorize(fixture.authenticated_principal(), request)
}

fn allowed(decision: Decision) -> Box<riffdb_policy::AuthorizedOperation> {
    let Decision::Allow(proof) = decision else {
        panic!("expected allow decision");
    };
    proof
}

#[test]
fn consuming_allow_binds_exact_command_actor_tenant_partition_and_claims() {
    let fixture = fixture(ActorKind::Agent, "authorized-agent");
    let version = ContractVersion::new(7).expect("nonzero version");
    let expected_partition = partition(41);
    let request = OperationRequest::execute_command(
        lineage(),
        version,
        CommandId::first(),
        CommandExecutionClass::Mutation,
        expected_partition.clone(),
    );
    let authorized = allowed(authorize(&fixture, request).expect("policy decision"))
        .into_command_execution(claims(), AgentSessionAdmissionPolicy::AllowForAgent)
        .expect("exact command binding");

    assert_eq!(authorized.database_id(), database_id());
    assert_eq!(authorized.environment(), &environment());
    assert_eq!(authorized.lineage(), &lineage());
    assert_eq!(authorized.version(), version);
    assert_eq!(authorized.command_id(), CommandId::first());
    assert_eq!(authorized.class(), CommandExecutionClass::Mutation);
    assert_eq!(authorized.partition().lineage(), &lineage());
    assert_eq!(authorized.partition().partition_key(), &expected_partition);
    assert_eq!(
        authorized.actor().principal_id().as_str(),
        "authorized-agent"
    );
    assert_eq!(authorized.actor().actor_kind(), ActorKind::Agent);
    assert_eq!(authorized.actor().tenant_scope(), &TenantScope::Global);
    assert_eq!(authorized.actor().agent_session_id(), Some(session_id()));
    assert!(authorized.provenance().source_repository().is_none());
    assert!(authorized.provenance().source_commit().is_none());
    assert!(authorized.provenance().reason().is_none());
    assert!(authorized.provenance().approval_id().is_none());
}

#[test]
fn allow_and_command_proofs_retain_the_exact_authorizer_boundary() {
    let fixture = fixture(ActorKind::Agent, "boundary-agent");
    let request = OperationRequest::execute_command(
        lineage(),
        ContractVersion::new(2).expect("nonzero version"),
        CommandId::first(),
        CommandExecutionClass::Mutation,
        partition(45),
    );
    let proof = allowed(authorize(&fixture, request).expect("policy decision"));

    assert_eq!(proof.database_id(), database_id());
    assert_eq!(proof.environment(), &environment());
    let proof_debug = format!("{proof:?}");
    assert_eq!(proof_debug, "AuthorizedOperation([REDACTED])");
    assert!(!proof_debug.contains(&database_id().to_string()));
    assert!(!proof_debug.contains(environment().as_str()));

    let authorized = proof
        .into_command_execution(claims(), AgentSessionAdmissionPolicy::Discard)
        .expect("exact command binding");
    assert_eq!(authorized.database_id(), database_id());
    assert_eq!(authorized.environment(), &environment());
}

#[test]
fn catalog_deployment_binding_retains_exact_authority_and_rejects_substitution() {
    let fixture = fixture(ActorKind::Service, "catalog-deployer");
    let version = ContractVersion::new(7).expect("nonzero version");
    let expected_active_version = Some(ContractVersion::new(6).expect("nonzero version"));
    let hash = bundle_hash(0x71);
    let deployment_request =
        || OperationRequest::deploy_contract(lineage(), version, hash, expected_active_version);

    let deployment = allowed(authorize(&fixture, deployment_request()).expect("policy decision"))
        .into_catalog_deployment(&lineage(), version, hash, expected_active_version)
        .expect("exact deployment binding");
    assert_eq!(deployment.database_id(), database_id());
    assert_eq!(deployment.environment(), &environment());
    assert_eq!(deployment.lineage(), &lineage());
    assert_eq!(deployment.version(), version);
    assert_eq!(deployment.bundle_hash(), hash);
    assert_eq!(
        deployment.expected_active_version(),
        expected_active_version
    );
    assert_eq!(
        deployment.authorizing_capability_id(),
        fixture.authenticated_principal().capability_id()
    );
    assert_eq!(
        deployment.authorizing_revision(),
        fixture.authenticated_principal().capability_revision()
    );
    assert_eq!(deployment.principal_id().as_str(), "catalog-deployer");
    assert_eq!(deployment.actor_kind(), ActorKind::Service);
    assert_eq!(deployment.obligations().validated_approval(), None);
    assert_eq!(
        format!("{deployment:?}"),
        "AuthorizedCatalogDeployment([REDACTED])"
    );

    assert_eq!(
        allowed(authorize(&fixture, deployment_request()).expect("policy decision"))
            .into_catalog_deployment(
                &lineage(),
                version,
                bundle_hash(0x72),
                expected_active_version,
            ),
        Err(CatalogDeploymentAuthorizationBindingError::IdentityMismatch)
    );
    assert_eq!(
        allowed(authorize(&fixture, OperationRequest::get_health()).expect("policy decision"),)
            .into_catalog_deployment(&lineage(), version, hash, expected_active_version),
        Err(CatalogDeploymentAuthorizationBindingError::OperationMismatch)
    );
}

#[test]
fn cross_database_or_environment_authorizer_boundaries_fail_closed() {
    let fixture = fixture(ActorKind::Agent, "boundary-agent");
    let request = || {
        OperationRequest::execute_command(
            lineage(),
            ContractVersion::new(2).expect("nonzero version"),
            CommandId::first(),
            CommandExecutionClass::Mutation,
            partition(46),
        )
    };
    let resolver = fixture.current_capability_resolver();
    let wrong_database =
        DatabaseId::from_unix_milliseconds_and_random(9, [0x91; 10]).expect("valid UUIDv7");
    assert_eq!(
        CurrentAuthorizer::new(
            &resolver,
            &FixedClock(timestamp(200)),
            &NoopAuthorizationTelemetry,
            wrong_database,
            environment(),
        )
        .authorize(fixture.authenticated_principal(), request())
        .expect("policy decision"),
        Decision::Deny(PolicyCode::InactiveOrStaleCapability)
    );
    assert_eq!(
        CurrentAuthorizer::new(
            &resolver,
            &FixedClock(timestamp(200)),
            &NoopAuthorizationTelemetry,
            database_id(),
            Environment::new("other-environment").expect("bounded environment"),
        )
        .authorize(fixture.authenticated_principal(), request())
        .expect("policy decision"),
        Decision::Deny(PolicyCode::InactiveOrStaleCapability)
    );
}

#[test]
fn retained_authorized_actor_cannot_be_substituted_during_claim_binding() {
    let fixture = fixture(ActorKind::Human, "authorized-human");
    let request = OperationRequest::execute_command(
        lineage(),
        ContractVersion::new(1).expect("nonzero version"),
        CommandId::first(),
        CommandExecutionClass::ReadOnly,
        partition(42),
    );
    let authorized = allowed(authorize(&fixture, request).expect("policy decision"))
        .into_command_execution(claims(), AgentSessionAdmissionPolicy::AllowForAgent)
        .expect("exact command binding");

    assert_eq!(
        authorized.actor().principal_id().as_str(),
        "authorized-human"
    );
    assert_eq!(authorized.actor().actor_kind(), ActorKind::Human);
    assert_eq!(authorized.actor().agent_session_id(), None);
    assert_eq!(authorized.class(), CommandExecutionClass::ReadOnly);
}

#[test]
fn non_command_allow_is_consumed_and_rejected() {
    let fixture = fixture(ActorKind::Service, "health-reader");
    let proof =
        allowed(authorize(&fixture, OperationRequest::get_health()).expect("policy decision"));
    assert_eq!(
        proof.into_command_execution(claims(), AgentSessionAdmissionPolicy::Discard),
        Err(CommandAuthorizationBindingError::OperationMismatch)
    );
}

#[test]
fn command_identity_mismatch_never_produces_a_binding() {
    let fixture = fixture(ActorKind::Agent, "authorized-agent");
    let wrong_command = OperationRequest::execute_command(
        lineage(),
        ContractVersion::new(1).expect("nonzero version"),
        CommandId::new(2).expect("nonzero command"),
        CommandExecutionClass::Mutation,
        partition(43),
    );
    assert_eq!(
        authorize(&fixture, wrong_command).expect("policy decision"),
        Decision::Deny(PolicyCode::MissingPermission)
    );
}

#[test]
fn command_binding_debug_and_errors_are_redaction_safe() {
    let fixture = fixture(ActorKind::Agent, "private-principal");
    let request = OperationRequest::execute_command(
        lineage(),
        ContractVersion::new(1).expect("nonzero version"),
        CommandId::first(),
        CommandExecutionClass::Mutation,
        partition(44),
    );
    let authorized = allowed(authorize(&fixture, request).expect("policy decision"))
        .into_command_execution(claims(), AgentSessionAdmissionPolicy::AllowForAgent)
        .expect("exact command binding");
    let rendered = format!("{authorized:?}");
    let database_text = database_id().to_string();
    let environment = environment();
    assert_eq!(rendered, "AuthorizedCommandExecution([REDACTED])");
    for secret in [
        "private-principal",
        database_text.as_str(),
        environment.as_str(),
        "private/repository",
        "0123456789abcdef",
        "caller supplied reason",
    ] {
        assert!(!rendered.contains(secret));
    }
    assert_eq!(
        CommandAuthorizationBindingError::ObligationMismatch.to_string(),
        "command authorization obligations are inconsistent"
    );
}
