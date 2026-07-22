#![no_main]
#![forbid(unsafe_code)]

use libfuzzer_sys::fuzz_target;
use riffdb_proto::{decode_public_error, decode_public_message, decode_value, v1};

fuzz_target!(|input: &[u8]| {
    let Some((&selector, payload)) = input.split_first() else {
        return;
    };
    match selector % 36 {
        0 => {
            let _ = decode_value(payload);
        }
        1 => {
            let _ = decode_public_error(payload);
        }
        2 => decode::<v1::ValidateContractRequest>(payload),
        3 => decode::<v1::ValidateContractResponse>(payload),
        4 => decode::<v1::ExplainCommandRequest>(payload),
        5 => decode::<v1::ExplainCommandResponse>(payload),
        6 => decode::<v1::DeployContractRequest>(payload),
        7 => decode::<v1::DeployContractResponse>(payload),
        8 => decode::<v1::GetActiveContractRequest>(payload),
        9 => decode::<v1::GetActiveContractResponse>(payload),
        10 => decode::<v1::ExecuteCommandRequest>(payload),
        11 => decode::<v1::ExecuteCommandResponse>(payload),
        12 => decode::<v1::GetOutcomeRequest>(payload),
        13 => decode::<v1::GetOutcomeResponse>(payload),
        14 => decode::<v1::GetEntityRequest>(payload),
        15 => decode::<v1::GetEntityResponse>(payload),
        16 => decode::<v1::ScanIndexRequest>(payload),
        17 => decode::<v1::ScanIndexResponse>(payload),
        18 => decode::<v1::QueryProjectionRequest>(payload),
        19 => decode::<v1::QueryProjectionResponse>(payload),
        20 => decode::<v1::ProjectionStatus>(payload),
        21 => decode::<v1::GetProjectionStatusRequest>(payload),
        22 => decode::<v1::GetProjectionStatusResponse>(payload),
        23 => decode::<v1::GetCommitRequest>(payload),
        24 => decode::<v1::GetCommitResponse>(payload),
        25 => decode::<v1::ScanCommitsRequest>(payload),
        26 => decode::<v1::ScanCommitsResponse>(payload),
        27 => decode::<v1::SubscribeCommitsRequest>(payload),
        28 => decode::<v1::CommitNotification>(payload),
        29 => decode::<v1::HealthRequest>(payload),
        30 => decode::<v1::HealthResponse>(payload),
        31 => decode::<v1::StatsRequest>(payload),
        32 => decode::<v1::StatsResponse>(payload),
        33 => decode::<v1::CreateCapabilityRequest>(payload),
        34 => decode::<v1::CreateCapabilityResponse>(payload),
        35 => {
            decode::<v1::RevokeCapabilityRequest>(payload);
            decode::<v1::RevokeCapabilityResponse>(payload);
        }
        _ => unreachable!(),
    }
});

fn decode<M: riffdb_proto::PublicMessage>(input: &[u8]) {
    let _ = decode_public_message::<M>(input);
}
