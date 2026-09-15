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
    vector_field embedding(4, cosine, (title), staleness_slo 60, model "embed-v1", current_version "2026-08-21", replay_age_seconds 86400, replay_bytes 1073741824, replay_backlog 100000)
  }
  aggregate Documents {
    root Document
    partition_by organization_id
    conflict_key (organization_id, document_id)
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
