//! Demand materialization from a real completed follower snapshot.
// req: REP-004, PRJ-005, PRJ-009
use super::*;
use crate::clocks::ProductionWallClocks;
use crate::columnar_adapter::FollowerColumnarRuntime;
use crate::config::ConfiguredProjection;
use riffdb_catalog::ValidatedContractBundle;
use riffdb_columnar::{
    ColumnarProjectionDefinition, ColumnarProjectionSpecV1, RegisteredDefinition,
};
use riffdb_service::{ColumnarLifecycle, ColumnarProjectionPort};
use riffdb_storage_api::{
    AuditPrincipalV1, CatalogActivationIntentV1, CatalogActivationResult,
    CatalogAdministrationRepository, ColumnarProjectionControlRepository,
    FreshColumnarProjectionControlV1,
};
use riffdb_types::{ActorId, ActorKind, CapabilityId, FrontierPosition, RequestId};

const CONTRACT: &str = r#"
contract FollowerColumnar version 1 {
  entity Document {
    key (organization_id: uuid, document_id: uuid)
    field title: string<64>
    vector_field embedding(4, cosine, (title), staleness_slo 60, model "embed-v1", current_version "2026-08-21", replay_age_seconds 86400, replay_bytes 1073741824, replay_backlog 100000, ann_threshold 1, recall_target_bps 9500)
  }
  aggregate Documents {
    root Document
    partition_by organization_id
    conflict_key (organization_id, document_id)
  }
  command CreateEmbeddedDocument {
    input idempotency_key: string<128>
    input organization_id: uuid
    input document_id: uuid
    input title: string<64>
    input embedding: vector<4>
    input submitted_model: string<256>
    input submitted_version: string<256>

    idempotency_key idempotency_key
    create Document(organization_id, document_id) as document
      else DocumentExists { document_id: document_id }
    set document.title = title
    embed document.embedding = embedding
      from (submitted_model, submitted_version)
    return Created { document: document }
  }
}
"#;
fn setup(ports: &mut RedbOperationalPorts) {
    let database = ports
        .published_changelog_snapshot_v3()
        .unwrap()
        .authoritative_state_v3()
        .unwrap()
        .history()
        .lineage()
        .database_id();
    crate::replication_bootstrap::tests::bootstrap(ports, database);
    let checked = ValidatedContractBundle::from_compiler_bundle(
        riffdb_contract_compiler::compile_contract_source(CONTRACT).unwrap(),
    )
    .unwrap();
    assert!(matches!(
        ports
            .activate_catalog(&CatalogActivationIntentV1::new(
                None,
                checked.to_stored().unwrap(),
                RequestId::from_unix_milliseconds_and_random(100, [0x31; 10]).unwrap(),
                AuditPrincipalV1::new(
                    ActorId::new("follower-test").unwrap(),
                    ActorKind::Human,
                    CapabilityId::from_unix_milliseconds_and_random(100, [0x32; 10]).unwrap(),
                    std::num::NonZeroU64::MIN
                ),
                Timestamp::new(1001, 0).unwrap(),
                None,
            ))
            .unwrap(),
        CatalogActivationResult::Activated { .. }
    ));
    let bundle = checked.bundle();
    let entity = bundle
        .schema()
        .entities()
        .iter()
        .find(|entity| entity.name() == "Document")
        .unwrap();
    let title = entity
        .record()
        .fields()
        .iter()
        .find(|field| field.name() == "title")
        .unwrap()
        .id();
    let embedding = entity
        .record()
        .fields()
        .iter()
        .find(|field| field.name() == "embedding")
        .unwrap()
        .id();
    let org = entity
        .record()
        .fields()
        .iter()
        .find(|field| field.name() == "organization_id")
        .unwrap()
        .id();
    let mut controls = Vec::new();
    for (name, fields, vector) in [
        ("document_board", vec![title], false),
        ("Document.embedding", vec![title, embedding], true),
    ] {
        let definition = RegisteredDefinition::register(
            ColumnarProjectionDefinition {
                name: name.to_owned(),
                entity_name: "Document".to_owned(),
                projected_fields: fields,
                org_scope_field: org,
            },
            bundle,
        )
        .unwrap();
        let spec = if vector {
            ColumnarProjectionSpecV1::for_vector(&definition, embedding, bundle).unwrap()
        } else {
            ColumnarProjectionSpecV1::for_scalar(&definition, bundle).unwrap()
        };
        controls.push(
            FreshColumnarProjectionControlV1::new(
                spec.source().clone(),
                spec.definition_fingerprint(),
                spec.hash(),
                spec.replay_limits(),
                1,
            )
            .unwrap(),
        );
    }
    ports.initialize_fresh_v1(&controls).unwrap();
}

#[tokio::test]
async fn follower_columnar_demand_builds_disposable_views_and_refuses_withdrawal() {
    let (fixture, build) = fixture_with_setup(setup).await;
    let mut receiver = build
        .publish_and_follow(fixture.path.clone())
        .await
        .unwrap();
    let (_, readers, _) = receiver.prepare_service().await.unwrap();
    let pin = readers.latest().unwrap();
    let controls = pin.snapshot().read_columnar_projection_controls().unwrap();
    let root = fixture.path.with_extension("columnar");
    std::fs::create_dir(&root).unwrap();
    let projections = [ConfiguredProjection::for_test(
        "document_board",
        "Document",
        &["title"],
        "organization_id",
    )];
    let runtime = FollowerColumnarRuntime::open(
        readers.clone(),
        &projections,
        &root,
        [0x72; 16],
        ProductionWallClocks::new().columnar_replay(),
    )
    .unwrap();
    assert_eq!(runtime.lifecycle_observation().unwrap(), (2, 0, 0));
    assert!(
        !runtime.is_healthy(),
        "cold sources must degrade columnar health"
    );
    assert!(!root.join("follower-columnar").exists());
    std::thread::scope(|scope| {
        let barrier = Arc::new(std::sync::Barrier::new(9));
        for _ in 0..8 {
            let runtime = runtime.clone();
            let barrier = barrier.clone();
            scope.spawn(move || {
                barrier.wait();
                let observed = runtime.observe("document_board").unwrap();
                assert!(!observed.has_published());
                assert_eq!(observed.lifecycle(), Some(&ColumnarLifecycle::Building));
            });
        }
        barrier.wait();
    });
    assert_eq!(runtime.lifecycle_observation().unwrap(), (1, 1, 0));
    assert!(
        !runtime.is_healthy(),
        "activating sources must degrade columnar health"
    );
    assert!(!root.join("follower-columnar").exists());
    let mut worker = runtime.worker_for_test().unwrap();
    assert!(runtime.worker_for_test().is_err());
    worker.advance().unwrap();
    let observed = runtime.observe("document_board").unwrap();
    assert!(observed.has_published());
    assert_eq!(
        observed.published_frontier().position(),
        FrontierPosition::BeforeFirst
    );
    assert_eq!(runtime.lifecycle_observation().unwrap(), (1, 1, 1));
    assert!(
        !runtime.is_healthy(),
        "the remaining cold vector source is not ready"
    );
    assert!(!root.join("follower-columnar/build").exists());
    assert_eq!(
        readers
            .latest()
            .unwrap()
            .snapshot()
            .read_columnar_projection_controls()
            .unwrap(),
        controls
    );
    assert!(
        !runtime
            .observe("Document.embedding")
            .unwrap()
            .has_published()
    );
    worker.advance().unwrap();
    let vector = runtime.observe("Document.embedding").unwrap();
    assert!(vector.has_published());
    assert_eq!(
        vector.published_frontier().position(),
        FrontierPosition::BeforeFirst
    );
    assert!(runtime.is_healthy());
    assert!(!root.join("follower-columnar/build").exists());
    assert_eq!(
        readers
            .latest()
            .unwrap()
            .snapshot()
            .read_columnar_projection_controls()
            .unwrap(),
        controls
    );
    drop(worker);
    let runtime = FollowerColumnarRuntime::open(
        readers.clone(),
        &projections,
        &root,
        [0x73; 16],
        ProductionWallClocks::new().columnar_replay(),
    )
    .unwrap();
    assert!(!runtime.observe("document_board").unwrap().has_published());
    let mut worker = runtime.worker_for_test().unwrap();
    let (reached, ready) = std::sync::mpsc::sync_channel(0);
    let (release, resume) = std::sync::mpsc::sync_channel(0);
    worker.pause_before_install(reached, resume);
    let owned = std::thread::spawn(move || {
        worker.advance().unwrap();
        worker
    });
    ready
        .recv_timeout(std::time::Duration::from_secs(30))
        .unwrap();
    assert!(!root.join("follower-columnar/build").exists());
    receiver.close().await.unwrap();
    release.send(()).unwrap();
    let worker = owned.join().unwrap();
    assert!(!runtime.is_healthy());
    assert!(runtime.observe("document_board").is_err());
    assert!(runtime.definition("document_board").is_none());
    assert!(observed.has_published()); // Retained memory never reopens admission.
    drop(worker);
    drop(pin);
}

#[tokio::test]
async fn follower_columnar_owned_worker_wakes_registered_demand_and_rebuilds_after_restart() {
    let (fixture, build) = fixture_with_setup(setup).await;
    let mut receiver = build
        .publish_and_follow(fixture.path.clone())
        .await
        .unwrap();
    let (_, readers, _) = receiver.prepare_service().await.unwrap();
    let root = fixture.path.with_extension("columnar-owned");
    std::fs::create_dir(&root).unwrap();
    let configured = [ConfiguredProjection::for_test(
        "document_board",
        "Document",
        &["title"],
        "organization_id",
    )];
    for process in [[0x71; 16], [0x72; 16]] {
        let runtime = FollowerColumnarRuntime::open(
            readers.clone(),
            &configured,
            &root,
            process,
            ProductionWallClocks::new().columnar_replay(),
        )
        .unwrap();
        assert_eq!(runtime.lifecycle_observation().unwrap(), (2, 0, 0));
        let worker =
            crate::columnar_adapter::RunningFollowerColumnarWorker::start(runtime.clone()).unwrap();
        let registered = runtime
            .notifier()
            .register("document_board".to_owned())
            .unwrap();
        let initial = runtime.observe("document_board").unwrap();
        assert!(!initial.has_published());
        assert_eq!(
            registered
                .wait(std::time::Instant::now() + std::time::Duration::from_secs(30))
                .unwrap(),
            riffdb_service::ColumnarWake::Notified
        );
        let active = runtime.observe("document_board").unwrap();
        assert!(active.has_published());
        assert_eq!(runtime.lifecycle_observation().unwrap(), (1, 1, 1));
        assert!(!root.join("follower-columnar/build").exists());
        let stopping = runtime
            .notifier()
            .register("document_board".to_owned())
            .unwrap();
        worker.stop_admission();
        assert_eq!(
            stopping
                .wait(std::time::Instant::now() + std::time::Duration::from_secs(30))
                .unwrap(),
            riffdb_service::ColumnarWake::Notified
        );
        worker.shutdown().unwrap();
        assert!(runtime.observe("document_board").is_err());
        assert!(active.has_published());
    }
    receiver.close().await.unwrap();
}

#[tokio::test]
async fn follower_columnar_cancelled_build_drains_scratch_without_publishing() {
    let (fixture, build) = fixture_with_setup(setup).await;
    let mut receiver = build
        .publish_and_follow(fixture.path.clone())
        .await
        .unwrap();
    let (_, readers, _) = receiver.prepare_service().await.unwrap();
    let controls = readers
        .latest()
        .unwrap()
        .snapshot()
        .read_columnar_projection_controls()
        .unwrap();
    let root = fixture.path.with_extension("cancelled-columnar");
    std::fs::create_dir(&root).unwrap();
    let configured = [ConfiguredProjection::for_test(
        "document_board",
        "Document",
        &["title"],
        "organization_id",
    )];
    let runtime = FollowerColumnarRuntime::open(
        readers.clone(),
        &configured,
        &root,
        [0x74; 16],
        ProductionWallClocks::new().columnar_replay(),
    )
    .unwrap();
    assert!(!runtime.observe("document_board").unwrap().has_published());
    let mut worker = runtime.worker_for_test().unwrap();
    let (reached, ready) = std::sync::mpsc::sync_channel(0);
    let (release, resume) = std::sync::mpsc::sync_channel(0);
    worker.pause_before_build(reached, resume);
    let running = std::thread::spawn(move || {
        assert!(worker.advance().is_err());
        worker
    });
    ready
        .recv_timeout(std::time::Duration::from_secs(30))
        .unwrap();
    assert!(root.join("follower-columnar/build").is_dir());
    runtime.stop();
    release.send(()).unwrap();
    let worker = running.join().unwrap();
    assert!(!root.join("follower-columnar/build").exists());
    assert!(runtime.observe("document_board").is_err());
    assert!(!runtime.is_healthy());
    assert_eq!(
        readers
            .latest()
            .unwrap()
            .snapshot()
            .read_columnar_projection_controls()
            .unwrap(),
        controls
    );
    drop(worker);
    receiver.close().await.unwrap();
}

#[tokio::test]
async fn follower_columnar_failed_source_stays_closed_until_owned_restart() {
    let (fixture, build) = fixture_with_setup(setup).await;
    let mut receiver = build
        .publish_and_follow(fixture.path.clone())
        .await
        .unwrap();
    let (_, readers, _) = receiver.prepare_service().await.unwrap();
    let controls = readers
        .latest()
        .unwrap()
        .snapshot()
        .read_columnar_projection_controls()
        .unwrap();
    let root = fixture.path.with_extension("failed-columnar");
    std::fs::create_dir(&root).unwrap();
    let area = root.join("follower-columnar");
    std::fs::write(&area, b"not a private build directory").unwrap();
    let configured = [ConfiguredProjection::for_test(
        "document_board",
        "Document",
        &["title"],
        "organization_id",
    )];
    let runtime = FollowerColumnarRuntime::open(
        readers.clone(),
        &configured,
        &root,
        [0x75; 16],
        ProductionWallClocks::new().columnar_replay(),
    )
    .unwrap();
    // Cold admission/status do not inspect or repair the bad optional material.
    assert!(!runtime.is_healthy());
    assert_eq!(
        std::fs::read(&area).unwrap(),
        b"not a private build directory"
    );
    assert!(!runtime.observe("document_board").unwrap().has_published());
    let mut worker = runtime.worker_for_test().unwrap();
    assert!(worker.advance().unwrap());
    assert!(runtime.observe("document_board").is_err());
    assert!(!runtime.is_healthy());
    std::fs::remove_file(&area).unwrap();
    for _ in 0..3 {
        assert!(runtime.observe("document_board").is_err());
        assert!(
            !worker.advance().unwrap(),
            "requests must not retry failed materialization"
        );
    }
    assert!(!area.exists());
    assert_eq!(runtime.lifecycle_observation().unwrap(), (1, 1, 0));
    drop(worker);
    let restarted = FollowerColumnarRuntime::open(
        readers.clone(),
        &configured,
        &root,
        [0x76; 16],
        ProductionWallClocks::new().columnar_replay(),
    )
    .unwrap();
    assert!(!restarted.observe("document_board").unwrap().has_published());
    let mut worker = restarted.worker_for_test().unwrap();
    assert!(worker.advance().unwrap());
    assert!(restarted.observe("document_board").unwrap().has_published());
    assert!(!root.join("follower-columnar/build").exists());
    assert_eq!(
        readers
            .latest()
            .unwrap()
            .snapshot()
            .read_columnar_projection_controls()
            .unwrap(),
        controls
    );
    drop(worker);
    receiver.close().await.unwrap();
}

struct DocAuditInput {
    request_id: RequestId,
    principal_id: ActorId,
    capability_id: CapabilityId,
    capability_revision: std::num::NonZeroU64,
    targets: riffdb_types::ServiceAuditTargetsV1,
}

impl DocAuditInput {
    fn new(request_id: RequestId) -> Self {
        Self {
            request_id,
            principal_id: ActorId::new("follower-vector-writer").unwrap(),
            capability_id: CapabilityId::from_unix_milliseconds_and_random(200, [0x41; 10])
                .unwrap(),
            capability_revision: std::num::NonZeroU64::MIN,
            targets: riffdb_types::ServiceAuditTargetsV1::empty(),
        }
    }
}

impl riffdb_commit::AdministrationAuditInputView for DocAuditInput {
    fn request_id(&self) -> &RequestId {
        &self.request_id
    }
    fn operation(&self) -> &riffdb_types::ServiceOperationV1 {
        &riffdb_types::ServiceOperationV1::ExecuteCommand
    }
    fn phase(&self) -> &riffdb_types::ServiceAuditPhaseV1 {
        &riffdb_types::ServiceAuditPhaseV1::Started
    }
    fn principal_id(&self) -> &ActorId {
        &self.principal_id
    }
    fn actor_kind(&self) -> &ActorKind {
        &ActorKind::Agent
    }
    fn capability_id(&self) -> &CapabilityId {
        &self.capability_id
    }
    fn capability_revision(&self) -> &std::num::NonZeroU64 {
        &self.capability_revision
    }
    fn ingress(&self) -> &riffdb_types::ServiceIngressKindV1 {
        &riffdb_types::ServiceIngressKindV1::Grpc
    }
    fn targets(&self) -> &riffdb_types::ServiceAuditTargetsV1 {
        &self.targets
    }
    fn approval_id(&self) -> Option<&riffdb_types::ApprovalId> {
        None
    }
    fn link(&self) -> &riffdb_types::ServiceAuditLinkV1 {
        &riffdb_types::ServiceAuditLinkV1::None
    }
}

struct DocAuthClock;
impl riffdb_policy::AuthorizationClock for DocAuthClock {
    fn now(&self) -> Result<Timestamp, riffdb_policy::AuthorizationClockError> {
        Ok(Timestamp::new(1002, 0).unwrap())
    }
}

struct DocDigestProvider(u8);
impl riffdb_idempotency::IdempotencyDigestProvider for DocDigestProvider {
    fn digest_candidates(
        &self,
        _: &riffdb_types::IdempotencyKey,
    ) -> Result<
        riffdb_idempotency::IdempotencyDigestCandidatesV1,
        riffdb_idempotency::IdempotencyDigestError,
    > {
        riffdb_idempotency::IdempotencyDigestCandidatesV1::new(vec![
            riffdb_storage_api::IdempotencyKeyDigest::from_hmac_bytes(
                DigestKeyId::new(1).unwrap(),
                [self.0; 32],
            ),
        ])
    }
}

fn authorize_doc_command(
    database: riffdb_types::DatabaseId,
    environment: &riffdb_types::Environment,
    plan: &riffdb_contract_ir::CommandPlan,
    lineage: riffdb_types::ContractLineage,
    partition: riffdb_types::PartitionKey,
) -> riffdb_policy::AuthorizedCommandExecution {
    let grant = riffdb_types::CapabilityGrantV1::new(
        riffdb_types::TenantScope::Global,
        riffdb_types::PartitionScopeV1::All,
        riffdb_types::CapabilityPermissionsV1::new(vec![
            riffdb_types::CapabilityPermissionV1::InvokeCommand(lineage.clone(), plan.command_id()),
        ])
        .unwrap(),
        Vec::new(),
        std::num::NonZeroU16::new(10).unwrap(),
        Vec::new(),
    )
    .unwrap();
    let fixture = riffdb_testkit::authorization::AuthorizationFixture::new(
        riffdb_testkit::authorization::AuthorizationFixtureConfig::new(
            database,
            environment.clone(),
            ActorId::new("follower-vector-writer").unwrap(),
            ActorKind::Agent,
            riffdb_types::Audience::new("riffdb-test").unwrap(),
            riffdb_testkit::authorization::AuthorizationFixtureTimes::new(
                Timestamp::new(900, 0).unwrap(),
                Timestamp::new(2000, 0).unwrap(),
                Timestamp::new(950, 0).unwrap(),
            ),
            grant,
        ),
    )
    .unwrap();
    let resolver = fixture.current_capability_resolver();
    let decision = riffdb_policy::CurrentAuthorizer::new(
        &resolver,
        &DocAuthClock,
        &riffdb_policy::NoopAuthorizationTelemetry,
        database,
        environment.clone(),
    )
    .authorize(
        fixture.authenticated_principal(),
        riffdb_policy::OperationRequest::execute_command(
            lineage,
            plan.contract_version(),
            plan.command_id(),
            riffdb_policy::CommandExecutionClass::Mutation,
            partition,
        ),
    )
    .unwrap();
    let riffdb_policy::Decision::Allow(proof) = decision else {
        panic!("expected Allow");
    };
    proof
        .into_command_execution(
            riffdb_policy::UntrustedInvocationClaims::new(None, None, None, None, None),
            riffdb_policy::AgentSessionAdmissionPolicy::Discard,
        )
        .unwrap()
}

struct DocPostAuthorizer {
    database: riffdb_types::DatabaseId,
    environment: riffdb_types::Environment,
    plan: riffdb_contract_ir::CommandPlan,
    lineage: riffdb_types::ContractLineage,
    partition: riffdb_types::PartitionKey,
}

impl riffdb_commit::PostEvaluationCommandAuthorizer for DocPostAuthorizer {
    fn authorize(
        &self,
    ) -> Result<
        riffdb_policy::AuthorizedCommandExecution,
        riffdb_commit::PostEvaluationAuthorizationError,
    > {
        Ok(authorize_doc_command(
            self.database,
            &self.environment,
            &self.plan,
            self.lineage.clone(),
            self.partition.clone(),
        ))
    }
}

fn prepare_embedded_document(
    ports: &RedbOperationalPorts,
    organization_id: [u8; 16],
    document_id: [u8; 16],
    title: &str,
    components: [f32; 4],
    seed: u8,
) -> riffdb_commit::CommandExecutionPreparation {
    let database = ports
        .published_changelog_snapshot_v3()
        .unwrap()
        .authoritative_state_v3()
        .unwrap()
        .history()
        .lineage()
        .database_id();
    let environment = riffdb_types::Environment::new("test").unwrap();
    let active = riffdb_catalog::ActiveCatalogSnapshot::read(ports)
        .unwrap()
        .unwrap();
    let bundle = active.bundle().bundle();
    let plan = bundle.commands().first().unwrap();
    let reference = riffdb_storage_api::ExecutablePlanRef::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
        plan.command_id(),
        plan.plan_hash(),
    );
    let resolved = riffdb_catalog::resolve_executable_plan(ports, &reference).unwrap();
    let key_text = format!("doc-{seed}");
    let by_name = std::collections::BTreeMap::from([
        (
            "idempotency_key",
            riffdb_types::CanonicalValue::string(&key_text).unwrap(),
        ),
        (
            "organization_id",
            riffdb_types::CanonicalValue::Uuid(organization_id),
        ),
        (
            "document_id",
            riffdb_types::CanonicalValue::Uuid(document_id),
        ),
        (
            "title",
            riffdb_types::CanonicalValue::string(title).unwrap(),
        ),
        (
            "embedding",
            riffdb_types::CanonicalValue::Vector(
                riffdb_types::CanonicalVector::new(components.to_vec()).unwrap(),
            ),
        ),
        (
            "submitted_model",
            riffdb_types::CanonicalValue::string("embed-v1").unwrap(),
        ),
        (
            "submitted_version",
            riffdb_types::CanonicalValue::string("2026-08-21").unwrap(),
        ),
    ]);
    let input = riffdb_types::CanonicalRecord::new(
        plan.input()
            .record()
            .fields()
            .iter()
            .map(|field| (field.id(), by_name[field.name()].clone()))
            .collect(),
    )
    .unwrap();
    let facts = riffdb_invariant::derive_input_command_facts(plan, input.clone()).unwrap();
    let caller_key = riffdb_types::IdempotencyKey::new(&key_text).unwrap();
    let scope = riffdb_idempotency::CommandIdempotencyScopeV1::new(
        database,
        environment.clone(),
        riffdb_types::TenantScope::Global,
        ActorId::new("follower-vector-writer").unwrap(),
        reference.contract_lineage().clone(),
        reference.command_id(),
    );
    let lookup = riffdb_idempotency::prepare_idempotency_lookup(
        &scope,
        &caller_key,
        &DocDigestProvider(seed),
    )
    .unwrap();
    let idempotency = riffdb_idempotency::IdempotencyInspectionExecutor::new(ports)
        .inspect(lookup)
        .unwrap()
        .confirm_input(&input, plan.idempotency_input().unwrap(), &caller_key)
        .unwrap()
        .bind_selected_plan(reference)
        .unwrap();
    let partition = facts.partition_key().clone();
    let authorization = authorize_doc_command(
        database,
        &environment,
        plan,
        bundle.lineage().clone(),
        partition.clone(),
    );
    let request_id =
        RequestId::from_unix_milliseconds_and_random(u64::from(seed) + 200, [seed; 10]).unwrap();
    let (control, _cancel) = riffdb_commit::CommandRequestControl::new(
        std::time::Instant::now() + std::time::Duration::from_secs(30),
    );
    riffdb_commit::CommandExecutionPreparation::new(
        database,
        &environment,
        resolved,
        input,
        idempotency,
        facts,
        authorization,
        request_id,
        riffdb_types::ServiceIngressKindV1::Grpc,
        control,
    )
    .unwrap()
    .with_audited_lifecycle(Box::new(DocAuditInput::new(request_id)))
    .unwrap()
    .with_post_evaluation_authorizer(Box::new(DocPostAuthorizer {
        database,
        environment,
        plan: plan.clone(),
        lineage: bundle.lineage().clone(),
        partition,
    }))
    .unwrap()
}

// req: REP-004, PRJ-005, PRJ-009, VEC-007, VEC-008, VEC-009, VEC-010
#[tokio::test]
async fn follower_vector_projection_warms_and_reuses_ann_graph_cache() {
    use crate::replication_bootstrap::projection_recovery_tests::command_support::{
        FixedAdmissionClock, IncrementingProvenanceSource, start_coordinator,
    };
    use riffdb_service::{
        VectorProjectionPort, VectorProjectionPortError, VectorProjectionRequest,
    };

    let (scope, path) =
        crate::real_storage_support::temporary_database_scope("follower-vector-ann-cache");
    let organization = [0x41; 16];
    let doc_a = [0x51; 16];
    let doc_b = [0x52; 16];
    {
        let startup = crate::startup::open_redb_startup(
            &path,
            inputs(),
            &crate::identifiers::ProductionIdentifierSources::new().database_ids(),
        )
        .unwrap();
        let (_, _, _, _, _, mut ports) = startup.into_parts();
        setup(&mut ports);
        let prep_a = prepare_embedded_document(
            &ports,
            organization,
            doc_a,
            "alpha",
            [1.0, 0.0, 0.0, 0.0],
            1,
        );
        let prep_b =
            prepare_embedded_document(&ports, organization, doc_b, "beta", [0.8, 0.6, 0.0, 0.0], 2);
        let coordinator = start_coordinator(
            ports,
            Arc::new(FixedAdmissionClock::new(Timestamp::new(1005, 0).unwrap())),
            Arc::new(IncrementingProvenanceSource::new(0x61)),
        );
        let executor = coordinator.command_executor();
        for prep in [prep_a, prep_b] {
            let result = executor
                .reserve_capacity()
                .await
                .unwrap()
                .submit(prep)
                .unwrap()
                .completion()
                .await
                .unwrap();
            assert!(matches!(
                result,
                riffdb_commit::CommandExecutionResult::Committed(_)
            ));
        }
        drop(executor);
        coordinator.shutdown().unwrap();
    }

    let mut startup = crate::startup::open_redb_startup(
        &path,
        inputs(),
        &crate::identifiers::ProductionIdentifierSources::new().database_ids(),
    )
    .unwrap();
    let _publications = startup.take_replication_publications().unwrap();
    let (_, _, _, _, _, ports) = startup.into_parts();
    let ports = Arc::new(ports);
    let repository = ports
        .bootstrap_repository(&scope.path().join("source-artifacts"))
        .unwrap();
    let hold_id = ReplicationSourceHoldIdV1::new([0x79; 16]).unwrap();
    let mut source = repository.begin(hold_id).unwrap();
    while !source.advance().unwrap() {}
    let held = source.finish().unwrap();
    let manifest = held.manifest();
    let jobs = BootstrapReceiverJobs::new();
    let mut transfer = jobs
        .begin(scope.path().join("transfer"), manifest)
        .await
        .unwrap();
    for ordinal in 1..=manifest.page_count() {
        transfer
            .append(held.read_page(ordinal).unwrap().encode().unwrap())
            .await
            .unwrap();
    }
    drop(held);
    let mut build = transfer
        .materialize(scope.path().join("candidate"), false, inputs())
        .await
        .unwrap();
    while !build.advance().await.unwrap() {}
    let follower_path = scope.path().join("follower.redb");
    let mut receiver = build
        .publish_and_follow(follower_path.clone())
        .await
        .unwrap();
    let (_, readers, _) = receiver.prepare_service().await.unwrap();
    let root = follower_path.with_extension("columnar-ann");
    std::fs::create_dir(&root).unwrap();
    let configured = [ConfiguredProjection::for_test(
        "document_board",
        "Document",
        &["title"],
        "organization_id",
    )];
    let runtime = FollowerColumnarRuntime::open(
        readers,
        &configured,
        &root,
        [0x77; 16],
        ProductionWallClocks::new().columnar_replay(),
    )
    .unwrap();

    let definition = runtime.definition("Document.embedding").unwrap();
    let vector_field = definition
        .projected_fields()
        .iter()
        .zip(definition.projected_types())
        .find_map(|(field, ty)| {
            (ty.tag() == riffdb_contract_ir::ValueTypeTag::Vector).then_some(*field)
        })
        .unwrap();
    let mut partition =
        riffdb_types::PartitionKeyBuilder::new(riffdb_types::AggregateTypeId::first());
    partition.push_uuid(&organization).unwrap();
    let request = VectorProjectionRequest::new(
        "Document.embedding".to_owned(),
        riffdb_types::ContractLineage::new("FollowerColumnar").unwrap(),
        partition.finish().unwrap(),
        riffdb_types::CanonicalValue::Uuid(organization),
        definition.entity_type_id(),
        vector_field,
        riffdb_types::EmbeddingMetadata::new("embed-v1", "2026-08-21").unwrap(),
        0,
        riffdb_types::CanonicalVector::new(vec![1.0, 0.0, 0.0, 0.0]).unwrap(),
        2,
        riffdb_types::DistanceMetric::Cosine,
        Vec::new(),
        None,
        None,
        None,
    );

    assert!(matches!(
        VectorProjectionPort::execute(&*runtime, request.clone()),
        Err(VectorProjectionPortError::Building)
    ));
    assert!(
        runtime
            .observe_with_vector_cache("Document.embedding")
            .unwrap()
            .1
            .is_none()
    );

    let mut worker = runtime.worker_for_test().unwrap();
    assert!(worker.advance().unwrap());
    let (_, Some((cache_v1, generation_v1))) = runtime
        .observe_with_vector_cache("Document.embedding")
        .unwrap()
    else {
        panic!("active follower vector LocalView must carry a NearestGraphCache");
    };
    assert_eq!(generation_v1, riffdb_types::ProjectionGeneration::first());

    let first = VectorProjectionPort::execute(&*runtime, request.clone()).unwrap();
    assert_eq!(first.nearest().rows.len(), 2);

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let current = VectorProjectionPort::execute(&*runtime, request.clone()).unwrap();
        assert_eq!(current.nearest().rows.len(), 2);
        if current.nearest().search_kind == riffdb_columnar::NearestSearchKind::Approximate {
            assert!(current.nearest().ann_stats.is_some());
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "follower vector LocalView did not warm and reuse its NearestGraphCache before deadline"
        );
        std::thread::yield_now();
    }

    let (_, Some((cache_after, generation_after))) = runtime
        .observe_with_vector_cache("Document.embedding")
        .unwrap()
    else {
        panic!("active follower vector LocalView must retain its NearestGraphCache");
    };
    assert!(Arc::ptr_eq(&cache_v1, &cache_after));
    assert_eq!(generation_after, generation_v1);

    drop(worker);
    receiver.close().await.unwrap();
}
