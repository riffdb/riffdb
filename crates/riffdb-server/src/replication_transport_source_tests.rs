//! TLS framing, administrative service and real source artifact custody together.
// req: REP-002, REP-003, REC-001
use super::*;
use crate::replication_bootstrap::BootstrapSourceJobs;
use crate::replication_publication::ReplicationPublication;
use crate::replication_source::PublishedReplicationSource;
use riffdb_storage_api::{
    AuthoritativeStateCatalogV1, ChangelogFrameV3, ChangelogPublicationPort,
    ReadableCapabilityDigestInventory, ReadableDigestKey, ReadableIdempotencyDigestInventory,
    ReplicationBootstrapManifestV1 as Manifest, ReplicationBootstrapPageV3 as Page,
    ReplicationBootstrapTranscriptV3, StartupValidationInputs,
};
use riffdb_types::DigestKeyId;
use v1::stream_changelog_response::Item;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replication_tls_real_source_transfers_complete_bootstrap_and_attaches_exact_successor() {
    let (_scope, path) = crate::real_storage_support::temporary_database_scope("tls-source");
    let key = ReadableDigestKey::v1(DigestKeyId::new(1).unwrap());
    let startup = crate::startup::open_redb_startup(
        &path,
        StartupValidationInputs::new(
            timestamp(1000),
            ReadableCapabilityDigestInventory::new(vec![key]).unwrap(),
            ReadableIdempotencyDigestInventory::new(vec![key]).unwrap(),
        ),
        &crate::identifiers::ProductionIdentifierSources::new().database_ids(),
    )
    .unwrap();
    let (database_id, _, _, _, _, ports) = startup.into_parts();
    let authority = Authority::for_database(database_id);
    let root = path.parent().unwrap().join("source-artifacts");
    let jobs = BootstrapSourceJobs::from_repository(ports.bootstrap_repository(&root).unwrap());
    let (publisher, publications) = ReplicationPublication::channel();
    let pin = ports.published_changelog_snapshot_v3().unwrap();
    let history = pin.authoritative_state_v3().unwrap().history();
    publisher.observe_published_snapshot_v3(pin);
    let source = Arc::new(PublishedReplicationSource::new(publications, jobs));
    let route = Arc::new(Route {
        service: Arc::new(ReplicationService::new(authority.clone(), source)),
        security: CheckedGrpcSecurityContext::new(
            authority.clone(),
            AuthenticationContext::new(database_id, environment(), Audience::new("grpc").unwrap()),
            authority.keys.clone(),
        ),
        ready: AtomicBool::new(true),
    });
    let application = GrpcApplication::new(
        route,
        GrpcRequestLimits::new(Duration::from_secs(10)).unwrap(),
    );
    let (_tls_scope, mut transport, channel) = tls(&application).await;
    let mut client =
        ReplicationServiceClient::new(channel).max_decoding_message_size(32 * 1024 * 1024 + 1024);
    let make_request = || {
        let mut request = bootstrap_wire_request(false);
        let message = request.get_mut();
        message.database_id = database_id.into_bytes().to_vec();
        message.history_incarnation = history.lineage().history_incarnation();
        message.leadership_epoch = history.lineage().leadership_epoch().get();
        message.readable_format = ChangelogFrameV3::IDENTITY.to_owned();
        message.catalog_digest = AuthoritativeStateCatalogV1.digest().to_vec();
        request
    };
    let mut stream = bounded(client.stream_changelog(make_request()))
        .await
        .unwrap()
        .into_inner();
    let Some(Item::BootstrapManifest(bytes)) =
        bounded(stream.message()).await.unwrap().unwrap().item
    else {
        panic!("manifest")
    };
    let manifest = Manifest::decode(&bytes).unwrap();
    let mut transcript = ReplicationBootstrapTranscriptV3::new(manifest.fence());
    for ordinal in 1..=manifest.page_count() {
        let Some(Item::BootstrapPage(bytes)) =
            bounded(stream.message()).await.unwrap().unwrap().item
        else {
            panic!("page")
        };
        let page = Page::decode(&bytes).unwrap();
        assert_eq!(page.ordinal(), ordinal);
        transcript.observe(&page).unwrap();
    }
    assert!(bounded(stream.message()).await.unwrap().is_none());
    transcript.verify_manifest(manifest).unwrap();
    drop(stream);
    publisher.observe_published_snapshot_v3(ports.published_changelog_snapshot_v3().unwrap());
    // A full EOF resume proves the exact original manifest before attachment.
    let mut resume = make_request();
    let phase = resume.get_mut().bootstrap.as_mut().unwrap();
    phase.resume_manifest = manifest.encode().unwrap();
    phase.after_page = manifest.page_count();
    let mut stream = bounded(client.stream_changelog(resume))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        bounded(stream.message()).await.unwrap().unwrap().item,
        Some(Item::BootstrapManifest(manifest.encode().unwrap()))
    );
    assert!(bounded(stream.message()).await.unwrap().is_none());
    drop(stream);
    // Exercise the production outbound TLS peer and receiver connection, then
    // publish and persist a local acknowledgement before source attachment.
    let HostedGrpcEndpoint::Tcp(address) = transport.endpoint() else {
        panic!("TLS TCP")
    };
    let trust = _tls_scope.path().join("trusted-ca.pem");
    fs::write(&trust, include_bytes!("../tests/fixtures/test-ca.pem")).unwrap();
    fs::set_permissions(&trust, fs::Permissions::from_mode(0o444)).unwrap();
    let endpoint =
        riffdb_config::CanonicalHttpsEndpoint::parse(&format!("https://{address}")).unwrap();
    let tls_config = riffdb_config::TlsClientConfig::new(
        endpoint.clone(),
        riffdb_config::ProtectedFilePath::new(trust).unwrap(),
        endpoint.identity().clone(),
        Duration::from_secs(5),
        Duration::from_secs(30),
        std::num::NonZeroU32::MIN,
        std::num::NonZeroU32::MIN,
    )
    .unwrap();
    let peer = crate::VerifiedReplicationPeer::connect(
        tls_config,
        RawCapabilityToken::parse_canonical(TOKEN.as_bytes()).unwrap(),
        riffdb_types::DatabaseAlias::new("default").unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(format!("{peer:?}"), "VerifiedReplicationPeer([redacted])");
    let request = ReplicationRequest {
        database_id,
        history_incarnation: history.lineage().history_incarnation(),
        leadership_epoch: history.lineage().leadership_epoch().get(),
        phase: ReplicationPhase::Bootstrap {
            hold_id: *manifest.fence().hold_id().as_bytes(),
            resume_manifest: vec![],
            after_page: 0,
        },
        after_sequence: 0,
        after_hash: [0; 32],
        after_frontier: riffdb_types::DualFrontier::INITIAL,
        readable_format: ChangelogFrameV3::IDENTITY.to_owned(),
        catalog_digest: AuthoritativeStateCatalogV1.digest(),
        maximum_frame_bytes: riffdb_storage_api::MAX_CHANGELOG_FRAME_BYTES as u64,
        maximum_transitions: riffdb_storage_api::MAX_STAGED_COMMANDS as u64,
    };
    let receiver = crate::replication_bootstrap::BootstrapReceiverJobs::new();
    let receiver_path = path.parent().unwrap().join("received");
    let mut connection = receiver
        .connect(&peer, receiver_path, request.clone(), false)
        .await
        .unwrap();
    assert_eq!(connection.progress().unwrap().manifest(), manifest);
    while !connection.receive_next().await.unwrap() {}
    let transfer = connection.finish().unwrap();
    let key = ReadableDigestKey::v1(DigestKeyId::new(1).unwrap());
    let validation = StartupValidationInputs::new(
        timestamp(1000),
        ReadableCapabilityDigestInventory::new(vec![key]).unwrap(),
        ReadableIdempotencyDigestInventory::new(vec![key]).unwrap(),
    );
    let mut build = transfer
        .materialize(path.parent().unwrap().join("candidate"), false, validation)
        .await
        .unwrap();
    while !build.advance().await.unwrap() {}
    let mut applier = build
        .publish_and_activate(path.parent().unwrap().join("follower.redb"))
        .await
        .unwrap();
    let acknowledged = applier.acknowledge_durable_position().unwrap();
    assert_eq!(acknowledged, manifest.fence().history().tail());
    let mut attach = request.clone();
    attach.phase = ReplicationPhase::Attach {
        manifest: manifest.encode().unwrap(),
    };
    attach.after_sequence = acknowledged.sequence().get();
    attach.after_hash = acknowledged.history_hash();
    attach.after_frontier = acknowledged.frontier();
    let mut attached = peer.open(attach).await.unwrap();
    let Some(ReplicationItem::Frame(bytes)) = bounded(attached.next_item()).await.unwrap() else {
        panic!("peer successor")
    };
    let applied = applier.apply_frame(&bytes).unwrap();
    assert_eq!(
        applied.sequence(),
        acknowledged.sequence().checked_next().unwrap()
    );
    assert_eq!(applier.acknowledge_durable_position().unwrap(), applied);
    drop(attached);
    let frontier = |position: Option<u64>| {
        Some(v1::FrontierPosition {
            position: Some(match position {
                Some(sequence) => v1::frontier_position::Position::AppliedThrough(sequence),
                None => v1::frontier_position::Position::BeforeFirst(v1::Unit {}),
            }),
        })
    };
    let mut attachment = make_request();
    attachment.get_mut().bootstrap = None;
    attachment.get_mut().attachment = Some(v1::ReplicationBootstrapAttachment {
        manifest: manifest.encode().unwrap(),
        acknowledged: Some(v1::ReplicationPosition {
            transaction_sequence: acknowledged.sequence().get(),
            history_hash: acknowledged.history_hash().to_vec(),
            application_frontier: frontier(acknowledged.frontier().application().map(|s| s.get())),
            administration_frontier: frontier(
                acknowledged.frontier().administration().map(|s| s.get()),
            ),
        }),
    });
    let mut stream = bounded(client.stream_changelog(attachment))
        .await
        .unwrap()
        .into_inner();
    let Some(Item::Frame(bytes)) = bounded(stream.message()).await.unwrap().unwrap().item else {
        panic!("successor")
    };
    let frame = ChangelogFrameV3::decode(&bytes).unwrap();
    assert_eq!(
        frame.receipts()[0].binding().predecessor,
        Some(acknowledged.sequence())
    );
    assert_eq!(
        frame.receipts()[0].binding().prior_history_hash,
        acknowledged.history_hash()
    );
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
    drop(stream);
    // Report only a locally durable point through the real TLS wire adapter.
    publisher.observe_published_snapshot_v3(ports.published_changelog_snapshot_v3().unwrap());
    let mut claim = request;
    claim.phase = ReplicationPhase::Follower {
        hold_id: *manifest.fence().hold_id().as_bytes(),
    };
    claim.after_sequence = applied.sequence().get();
    claim.after_hash = applied.history_hash();
    claim.after_frontier = applied.frontier();
    let before = ports
        .published_changelog_snapshot_v3()
        .unwrap()
        .authoritative_state_v3()
        .unwrap()
        .history();
    let mut after = before;
    for attempt in 0..2 {
        applier.resume_stream().unwrap();
        let mut tail = peer.open(claim.clone()).await.unwrap();
        let Some(ReplicationItem::Frame(bytes)) = bounded(tail.next_item()).await.unwrap() else {
            panic!("acknowledgement tail")
        };
        let receipt = ChangelogFrameV3::decode(&bytes).unwrap();
        assert_eq!(
            receipt.receipts()[0].binding().predecessor,
            Some(applied.sequence())
        );
        if attempt == 0 {
            applier.apply_frame(&bytes).unwrap();
            applier.acknowledge_durable_position().unwrap();
            after = ports
                .published_changelog_snapshot_v3()
                .unwrap()
                .authoritative_state_v3()
                .unwrap()
                .history();
            assert_eq!(
                after.tail().sequence(),
                before.tail().sequence().checked_next().unwrap()
            );
        } else {
            assert_eq!(
                ports
                    .published_changelog_snapshot_v3()
                    .unwrap()
                    .authoritative_state_v3()
                    .unwrap()
                    .history(),
                after
            );
        }
        drop(tail);
    }
    let locally_durable = applier.durable_position().unwrap();
    applier.close().unwrap();
    publisher.observe_published_snapshot_v3(ports.published_changelog_snapshot_v3().unwrap());
    let mut following = receiver
        .reopen_follower(
            path.parent().unwrap().join("follower.redb"),
            StartupValidationInputs::new(
                timestamp(1000),
                ReadableCapabilityDigestInventory::new(vec![key]).unwrap(),
                ReadableIdempotencyDigestInventory::new(vec![key]).unwrap(),
            ),
            history.lineage(),
            manifest.fence().hold_id(),
            Some(manifest),
        )
        .await
        .unwrap();
    let applied = bounded(following.advance(&peer)).await.unwrap().unwrap();
    assert!(applied.sequence() > locally_durable.sequence());
    following.close().await.unwrap();
    drop(peer);
    drop(client);
    bounded(transport.drain_after_signal()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replication_peer_rejects_invalid_trust_before_sending_any_credential() {
    let harness = Harness::new();
    let (scope, mut transport, _channel) = tls(&harness.application).await;
    let HostedGrpcEndpoint::Tcp(address) = transport.endpoint() else {
        panic!("TLS TCP")
    };
    let endpoint =
        riffdb_config::CanonicalHttpsEndpoint::parse(&format!("https://{address}")).unwrap();
    for (index, (bytes, mode)) in [
        (
            include_bytes!("../tests/fixtures/localhost-cert.pem").as_slice(),
            0o444,
        ),
        (b"malformed trust root".as_slice(), 0o444),
        (
            include_bytes!("../tests/fixtures/test-ca.pem").as_slice(),
            0o666,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let path = scope.path().join(format!("rejected-root-{index}.pem"));
        fs::write(&path, bytes).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(mode)).unwrap();
        let config = riffdb_config::TlsClientConfig::new(
            endpoint.clone(),
            riffdb_config::ProtectedFilePath::new(path).unwrap(),
            endpoint.identity().clone(),
            Duration::from_secs(5),
            Duration::from_secs(30),
            std::num::NonZeroU32::MIN,
            std::num::NonZeroU32::MIN,
        )
        .unwrap();
        assert_eq!(
            crate::VerifiedReplicationPeer::connect(
                config,
                RawCapabilityToken::parse_canonical(TOKEN.as_bytes()).unwrap(),
                riffdb_types::DatabaseAlias::new("default").unwrap()
            )
            .await
            .unwrap_err(),
            ReplicationFailure::Unavailable
        );
        assert_eq!(harness.authority.authentications.load(Ordering::SeqCst), 0);
        assert!(harness.source.requests.lock().unwrap().is_empty());
    }
    let root = scope.path().join("valid-root.pem");
    fs::write(&root, include_bytes!("../tests/fixtures/test-ca.pem")).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o444)).unwrap();
    let config = riffdb_config::TlsClientConfig::new(
        endpoint.clone(),
        riffdb_config::ProtectedFilePath::new(root).unwrap(),
        endpoint.identity().clone(),
        Duration::from_secs(5),
        Duration::from_secs(30),
        std::num::NonZeroU32::MIN,
        std::num::NonZeroU32::MIN,
    )
    .unwrap();
    let peer = crate::VerifiedReplicationPeer::connect(
        config,
        RawCapabilityToken::parse_canonical(b"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
            .unwrap(),
        riffdb_types::DatabaseAlias::new("default").unwrap(),
    )
    .await
    .unwrap();
    let denied = peer
        .open(ReplicationRequest {
            database_id: database_id(),
            history_incarnation: 7,
            leadership_epoch: 11,
            phase: ReplicationPhase::Tail,
            after_sequence: 19,
            after_hash: [0x52; 32],
            after_frontier: riffdb_types::DualFrontier::new(
                riffdb_types::CommitSequence::new(3),
                AdministrationSequence::new(5),
            ),
            readable_format: ChangelogFrameV3::IDENTITY.to_owned(),
            catalog_digest: AuthoritativeStateCatalogV1.digest(),
            maximum_frame_bytes: 32 * 1024 * 1024,
            maximum_transitions: 256,
        })
        .await;
    let refusal = match denied {
        Err(error) => error,
        Ok(mut stream) => stream.next_item().await.err().unwrap(),
    };
    assert_eq!(refusal, ReplicationFailure::AuthorizationDenied);
    assert_eq!(harness.authority.authentications.load(Ordering::SeqCst), 1);
    assert!(harness.source.requests.lock().unwrap().is_empty());
    drop(peer);
    bounded(transport.drain_after_signal()).await.unwrap();
}
