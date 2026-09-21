#![forbid(unsafe_code)]
//! Fence summaries bind an exact selection and never supply promotion authority.
// req: REP-005
use prost::Message;
use riffdb_proto::{
    decode_public_message, v1, validate_fence_replication_primary_exchange as exchange,
    validate_public_message,
};

fn request() -> v1::FenceReplicationPrimaryRequest {
    let id = riffdb_types::DatabaseId::from_unix_milliseconds_and_random(1, [1; 10]).unwrap();
    v1::FenceReplicationPrimaryRequest {
        request_id: id.as_bytes().to_vec(),
        operation_id: id.as_bytes().to_vec(),
        target: Some(v1::ReplicationFollowerTarget {
            database_id: id.as_bytes().to_vec(),
            history_incarnation: 1,
            leadership_epoch: 1,
            hold_id: vec![0x71; 16],
        }),
        registration_generation: u64::MAX,
    }
}
fn receipt(request: &v1::FenceReplicationPrimaryRequest) -> v1::PrimaryFenceReceipt {
    v1::PrimaryFenceReceipt {
        operation_id: request.operation_id.clone(),
        target: request.target.clone(),
        registration_generation: request.registration_generation,
        administration_sequence: u64::MAX,
        final_application_frontier: Some(v1::FrontierPosition {
            position: Some(v1::frontier_position::Position::BeforeFirst(v1::Unit {})),
        }),
        replayed: false,
    }
}
fn response(receipt: v1::PrimaryFenceReceipt) -> v1::FenceReplicationPrimaryResponse {
    v1::FenceReplicationPrimaryResponse {
        result: Some(v1::fence_replication_primary_response::Result::Receipt(
            receipt,
        )),
    }
}
#[test]
fn primary_fence_exchange_binds_operation_lineage_hold_and_generation() {
    let request = request();
    for replayed in [false, true] {
        for head in [None, Some(1), Some(u64::MAX)] {
            let mut receipt = receipt(&request);
            receipt.replayed = replayed;
            if let Some(sequence) = head {
                receipt
                    .final_application_frontier
                    .as_mut()
                    .unwrap()
                    .position = Some(v1::frontier_position::Position::AppliedThrough(sequence));
            }
            let value = response(receipt.clone());
            exchange(&request, &value).unwrap();
            assert_eq!(
                decode_public_message::<v1::FenceReplicationPrimaryResponse>(
                    &value.encode_to_vec()
                )
                .unwrap(),
                value
            );
            for case in 0..6 {
                let mut substituted = receipt.clone();
                match case {
                    0 => substituted.operation_id[15] ^= 1,
                    1 => substituted.target.as_mut().unwrap().database_id[15] ^= 1,
                    2 => substituted.target.as_mut().unwrap().history_incarnation += 1,
                    3 => substituted.target.as_mut().unwrap().leadership_epoch += 1,
                    4 => substituted.target.as_mut().unwrap().hold_id[15] ^= 1,
                    _ => substituted.registration_generation -= 1,
                }
                let value = response(substituted);
                validate_public_message(&value).unwrap();
                assert!(exchange(&request, &value).is_err());
            }
        }
    }
}
#[test]
fn primary_fence_wire_refuses_missing_invalid_and_ambiguous_values() {
    let valid = request();
    assert_eq!(
        decode_public_message::<v1::FenceReplicationPrimaryRequest>(&valid.encode_to_vec())
            .unwrap(),
        valid
    );
    for case in 0..5 {
        let mut value = valid.clone();
        match case {
            0 => value.request_id.clear(),
            1 => value.operation_id = vec![0; 16],
            2 => value.target = None,
            3 => value.registration_generation = 0,
            _ => value.target.as_mut().unwrap().hold_id = vec![0; 16],
        }
        assert!(
            decode_public_message::<v1::FenceReplicationPrimaryRequest>(&value.encode_to_vec())
                .is_err()
        );
    }
    for case in 0..5 {
        let mut value = receipt(&valid);
        match case {
            0 => value.administration_sequence = 0,
            1 => value.registration_generation = 0,
            2 => value.final_application_frontier = None,
            3 => value.final_application_frontier.as_mut().unwrap().position = None,
            _ => {
                value.final_application_frontier.as_mut().unwrap().position =
                    Some(v1::frontier_position::Position::AppliedThrough(0))
            }
        }
        assert!(
            decode_public_message::<v1::FenceReplicationPrimaryResponse>(
                &response(value).encode_to_vec()
            )
            .is_err()
        );
    }
    for refusal in [-1, 0, 1, 2, 3, 4, i32::MAX] {
        let value = v1::FenceReplicationPrimaryResponse {
            result: Some(v1::fence_replication_primary_response::Result::Refusal(
                refusal,
            )),
        };
        assert_eq!(exchange(&valid, &value).is_ok(), (1..=3).contains(&refusal));
    }
    let mut bytes = valid.encode_to_vec();
    bytes.extend_from_slice(&[0x20, 1]); // duplicate generation
    assert!(decode_public_message::<v1::FenceReplicationPrimaryRequest>(&bytes).is_err());
    let mut bytes = response(receipt(&valid)).encode_to_vec();
    bytes.extend_from_slice(&[0x10, 1]); // competing result arm
    assert!(decode_public_message::<v1::FenceReplicationPrimaryResponse>(&bytes).is_err());
    assert!(decode_public_message::<v1::FenceReplicationPrimaryResponse>(&[]).is_err());
}
