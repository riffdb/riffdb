#![forbid(unsafe_code)]

//! End-to-end semantic acceptance for live named RiffQL queries.

use std::time::Duration;

use riffdb_proto::{decode_public_message, v1};
use riffdb_service::{
    LiveQueryResetReason, LiveQueryTerminalReason, MAX_LIVE_QUERY_BUFFERED_UPDATES,
    MAX_LIVE_QUERY_LIFETIME, MAX_LIVE_QUERY_WATCHES_PER_DATABASE,
};

const PUBLIC_VECTORS: &str = include_str!("../fixtures/proto/public-client-vectors.txt");

fn decode_hex(value: &str) -> Vec<u8> {
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            u8::from_str_radix(std::str::from_utf8(pair).expect("ASCII fixture hex"), 16)
                .expect("checked fixture hex")
        })
        .collect()
}

fn live_vector(branch: &str) -> v1::LiveQueryUpdate {
    let prefix = format!("QueryService.WatchNamedQuery stream {branch} ");
    let line = PUBLIC_VECTORS
        .lines()
        .find(|line| line.starts_with(&prefix))
        .expect("live stream compatibility vector");
    let encoded = line
        .split_whitespace()
        .nth(4)
        .expect("fixture encoded bytes");
    decode_public_message(&decode_hex(encoded)).expect("strict live update fixture")
}

#[test]
fn accepted_live_query_bounds_and_closed_end_reasons_are_frozen() {
    assert_eq!(MAX_LIVE_QUERY_WATCHES_PER_DATABASE, 128);
    assert_eq!(MAX_LIVE_QUERY_BUFFERED_UPDATES, 32);
    assert_eq!(MAX_LIVE_QUERY_LIFETIME, Duration::from_secs(15 * 60));

    let resets = [
        LiveQueryResetReason::OutcomeChanged,
        LiveQueryResetReason::DiffLimitExceeded,
        LiveQueryResetReason::DefinitionChanged,
        LiveQueryResetReason::HistoryChanged,
        LiveQueryResetReason::CursorExpired,
    ];
    assert_eq!(resets.len(), 5);

    let terminals = [
        LiveQueryTerminalReason::AuthorizationChanged,
        LiveQueryTerminalReason::BufferPressure,
        LiveQueryTerminalReason::LifetimeExpired,
        LiveQueryTerminalReason::ServiceUnavailable,
        LiveQueryTerminalReason::IntegrityFailure,
        LiveQueryTerminalReason::DefinitionChanged,
    ];
    assert_eq!(terminals.len(), 6);
}

#[test]
fn public_vectors_cover_the_closed_stream_and_complete_public_key_patch() {
    let snapshot = live_vector("snapshot");
    let patch = live_vector("patch");
    assert!(matches!(
        live_vector("reset").update,
        Some(v1::live_query_update::Update::Reset(_))
    ));
    assert!(matches!(
        live_vector("checkpoint").update,
        Some(v1::live_query_update::Update::Checkpoint(_))
    ));
    assert!(matches!(
        live_vector("terminal").update,
        Some(v1::live_query_update::Update::Terminal(_))
    ));

    let Some(v1::live_query_update::Update::Snapshot(snapshot)) = snapshot.update else {
        panic!("snapshot branch");
    };
    let snapshot_key = snapshot
        .result
        .expect("snapshot result")
        .fields
        .into_iter()
        .find(|field| field.name == "items")
        .expect("items result")
        .records
        .into_iter()
        .next()
        .expect("one fixture row")
        .fields
        .expect("row fields");
    let Some(v1::live_query_update::Update::Patch(patch)) = patch.update else {
        panic!("patch branch");
    };
    let operation = patch.operations.into_iter().next().expect("one patch");
    let Some(v1::live_query_patch_operation::Operation::Remove(remove)) = operation.operation
    else {
        panic!("remove patch");
    };
    let patch_key = remove.key.expect("complete public key");
    assert_eq!(patch_key, snapshot_key);
    assert!(
        patch_key
            .fields
            .iter()
            .all(|field| field.field_id.is_none())
    );
}
