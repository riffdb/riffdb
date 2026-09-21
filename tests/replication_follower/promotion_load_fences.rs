//! Keep real follower discovery observations across the live promotion.
use super::*;
use riffdb_client_rust::RiffDbClient;
use riffdb_storage_api::{ChangelogFrameV3, ChangelogHistoryPointV3, ChangelogLineageV3};

fn request(
    cursor: Option<Vec<u8>>,
    fence: Option<v1::DiscoveryCatalogFence>,
) -> v1::DiscoverResourcesRequest {
    v1::DiscoverResourcesRequest {
        request_id: request_id(237),
        page: Some(v1::PageRequest {
            limit: Some(1),
            cursor,
        }),
        prior_fence: fence,
        representation: v1::DiscoveryRepresentation::CompactObservation as i32,
        kind: v1::ResourceDiscoveryKind::All as i32,
    }
}

pub(super) async fn observe(
    fixture: &Fixture,
    authority: &CallMetadata,
) -> (Vec<u8>, v1::DiscoveryCatalogFence) {
    let mut client = fixture.client("follower").await;
    let page = || request(None, None);
    let first = client.discover_resources(page(), authority).await.unwrap();
    let Some(v1::discover_resources_response::Result::CompactPage(first)) = first.result else {
        panic!("follower discovery page")
    };
    let cursor = first.next_cursor.unwrap();
    // Prove this opaque handle names a real current continuation before keeping
    // an independent live handle across the cutover.
    client
        .discover_resources(request(Some(cursor), None), authority)
        .await
        .unwrap();
    let retained = client.discover_resources(page(), authority).await.unwrap();
    let Some(v1::discover_resources_response::Result::CompactPage(retained)) = retained.result
    else {
        panic!("retained follower discovery")
    };
    (
        retained.next_cursor.unwrap(),
        retained.observed_fence.unwrap(),
    )
}

pub(super) async fn assert_refused(
    fixture: &Fixture,
    authority: &CallMetadata,
    (cursor, fence): (Vec<u8>, v1::DiscoveryCatalogFence),
) {
    let mut client: RiffDbClient = fixture.client("follower").await;
    for (request, expected) in [
        (
            request(Some(cursor), None),
            riffdb_errors::PublicErrorKind::Validation,
        ),
        (
            request(None, Some(fence)),
            riffdb_errors::PublicErrorKind::HistoryIncarnationMismatch,
        ),
    ] {
        let error = client
            .discover_resources(request, authority)
            .await
            .unwrap_err();
        assert_eq!(
            error.public_error().map(|error| error.kind()),
            Some(expected)
        );
    }
}

pub(super) async fn assert_old_stream_refused(
    fixture: &Fixture,
    token: &str,
    lineage: ChangelogLineageV3,
    after: ChangelogHistoryPointV3,
) {
    use riffdb_api_grpc::generated::replication_service_client::ReplicationServiceClient;
    fn frontier(value: Option<u64>) -> Option<v1::FrontierPosition> {
        Some(v1::FrontierPosition {
            position: Some(match value {
                Some(value) => v1::frontier_position::Position::AppliedThrough(value),
                None => v1::frontier_position::Position::BeforeFirst(v1::Unit {}),
            }),
        })
    }
    let mut request = tonic::Request::new(v1::StreamChangelogRequest {
        request_id: request_id(238),
        database_id: lineage.database_id().as_bytes().to_vec(),
        history_incarnation: lineage.history_incarnation(),
        leadership_epoch: lineage.leadership_epoch().get(),
        readable_format: ChangelogFrameV3::IDENTITY.into(),
        catalog_digest: lineage.catalog_digest().to_vec(),
        after: Some(v1::ReplicationPosition {
            transaction_sequence: after.sequence().get(),
            history_hash: after.history_hash().to_vec(),
            application_frontier: frontier(after.frontier().application().map(|value| value.get())),
            administration_frontier: frontier(
                after.frontier().administration().map(|value| value.get()),
            ),
        }),
        maximum_frame_bytes: riffdb_storage_api::MAX_CHANGELOG_FRAME_BYTES as u64,
        maximum_transitions: riffdb_storage_api::MAX_STAGED_COMMANDS as u64,
        ..Default::default()
    });
    let mut credential: tonic::metadata::MetadataValue<_> =
        format!("Bearer {token}").parse().unwrap();
    credential.set_sensitive(true);
    request.metadata_mut().insert("authorization", credential);
    let channel = fixture.channel("follower").await;
    tokio::time::timeout(Duration::from_secs(30), async {
        let mut stream = ReplicationServiceClient::new(channel).stream_changelog(request).await.unwrap().into_inner();
        assert!(matches!(stream.message().await.unwrap().unwrap().item,
            Some(v1::stream_changelog_response::Item::Refusal(value)) if value == v1::ReplicationRefusal::ForeignLineage as i32));
        assert!(stream.message().await.unwrap().is_none());
    }).await.unwrap();
}
