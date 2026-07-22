//! Language-neutral request/result fixtures for every accepted public RPC.

use std::collections::BTreeSet;

use riffdb_proto::{PublicMessage, decode_public_message, v1};

const VECTORS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/proto/public-client-vectors.txt"
));

fn decode_hex(value: &str) -> Vec<u8> {
    assert_eq!(value.len() % 2, 0);
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            u8::from_str_radix(std::str::from_utf8(pair).expect("ASCII hex"), 16)
                .expect("valid hex")
        })
        .collect()
}

fn decode<M: PublicMessage>(bytes: &[u8]) {
    decode_public_message::<M>(bytes).expect("strict public fixture");
}

#[test]
fn every_client_vector_passes_its_strict_public_boundary() {
    let mut lines = VECTORS.lines();
    assert_eq!(lines.next(), Some("riffdb-public-client-vectors-v1"));
    let mut rpcs = BTreeSet::new();
    let mut request_rpcs = BTreeSet::new();
    let mut visible_rpcs = BTreeSet::new();
    let mut count = 0usize;
    for line in lines {
        let mut fields = line.split_whitespace();
        let rpc = fields.next().expect("RPC");
        let direction = fields.next().expect("direction");
        let _branch = fields.next().expect("branch");
        let message_type = fields.next().expect("message type");
        let bytes = decode_hex(fields.next().expect("hex bytes"));
        assert!(fields.next().is_none());
        rpcs.insert(rpc);
        if direction == "request" {
            request_rpcs.insert(rpc);
        } else {
            visible_rpcs.insert(rpc);
        }
        match message_type {
            "riffdb.v1.ValidateContractRequest" => decode::<v1::ValidateContractRequest>(&bytes),
            "riffdb.v1.ValidateContractResponse" => decode::<v1::ValidateContractResponse>(&bytes),
            "riffdb.v1.ExplainCommandRequest" => decode::<v1::ExplainCommandRequest>(&bytes),
            "riffdb.v1.ExplainCommandResponse" => decode::<v1::ExplainCommandResponse>(&bytes),
            "riffdb.v1.DeployContractRequest" => decode::<v1::DeployContractRequest>(&bytes),
            "riffdb.v1.DeployContractResponse" => decode::<v1::DeployContractResponse>(&bytes),
            "riffdb.v1.GetActiveContractRequest" => decode::<v1::GetActiveContractRequest>(&bytes),
            "riffdb.v1.GetActiveContractResponse" => {
                decode::<v1::GetActiveContractResponse>(&bytes)
            }
            "riffdb.v1.ExecuteCommandRequest" => decode::<v1::ExecuteCommandRequest>(&bytes),
            "riffdb.v1.ExecuteCommandResponse" => decode::<v1::ExecuteCommandResponse>(&bytes),
            "riffdb.v1.GetOutcomeRequest" => decode::<v1::GetOutcomeRequest>(&bytes),
            "riffdb.v1.GetOutcomeResponse" => decode::<v1::GetOutcomeResponse>(&bytes),
            "riffdb.v1.GetEntityRequest" => decode::<v1::GetEntityRequest>(&bytes),
            "riffdb.v1.GetEntityResponse" => decode::<v1::GetEntityResponse>(&bytes),
            "riffdb.v1.ScanIndexRequest" => decode::<v1::ScanIndexRequest>(&bytes),
            "riffdb.v1.ScanIndexResponse" => decode::<v1::ScanIndexResponse>(&bytes),
            "riffdb.v1.QueryProjectionRequest" => decode::<v1::QueryProjectionRequest>(&bytes),
            "riffdb.v1.QueryProjectionResponse" => decode::<v1::QueryProjectionResponse>(&bytes),
            "riffdb.v1.GetCommitRequest" => decode::<v1::GetCommitRequest>(&bytes),
            "riffdb.v1.GetCommitResponse" => decode::<v1::GetCommitResponse>(&bytes),
            "riffdb.v1.ScanCommitsRequest" => decode::<v1::ScanCommitsRequest>(&bytes),
            "riffdb.v1.ScanCommitsResponse" => decode::<v1::ScanCommitsResponse>(&bytes),
            "riffdb.v1.SubscribeCommitsRequest" => decode::<v1::SubscribeCommitsRequest>(&bytes),
            "riffdb.v1.CommitNotification" => decode::<v1::CommitNotification>(&bytes),
            "riffdb.v1.HealthRequest" => decode::<v1::HealthRequest>(&bytes),
            "riffdb.v1.HealthResponse" => decode::<v1::HealthResponse>(&bytes),
            "riffdb.v1.StatsRequest" => decode::<v1::StatsRequest>(&bytes),
            "riffdb.v1.StatsResponse" => decode::<v1::StatsResponse>(&bytes),
            "riffdb.v1.CreateCapabilityRequest" => decode::<v1::CreateCapabilityRequest>(&bytes),
            "riffdb.v1.CreateCapabilityResponse" => decode::<v1::CreateCapabilityResponse>(&bytes),
            "riffdb.v1.RevokeCapabilityRequest" => decode::<v1::RevokeCapabilityRequest>(&bytes),
            "riffdb.v1.RevokeCapabilityResponse" => decode::<v1::RevokeCapabilityResponse>(&bytes),
            other => panic!("unexpected client fixture type: {other}"),
        }
        count += 1;
    }
    assert_eq!(count, 67);
    assert_eq!(rpcs.len(), 16);
    assert_eq!(request_rpcs, rpcs);
    assert_eq!(visible_rpcs, rpcs);
}
