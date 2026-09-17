#![forbid(unsafe_code)]
//! Lifecycle wire values are bounded selections, never caller-created authority.
// req: REP-006
use prost::Message;
use riffdb_proto::{decode_public_message, v1, validate_public_message};

fn request() -> v1::RegisterFollowerRequest {
    let id = riffdb_types::DatabaseId::from_unix_milliseconds_and_random(1, [1; 10]).unwrap();
    v1::RegisterFollowerRequest {
        request_id: id.as_bytes().to_vec(),
        target: Some(v1::ReplicationFollowerTarget {
            database_id: id.as_bytes().to_vec(),
            history_incarnation: 1,
            leadership_epoch: 1,
            hold_id: vec![1; 16],
        }),
        hold_budget_sequences: 1,
        expires_at_application_sequence: None,
    }
}

#[test]
fn follower_selection_requires_complete_lineage_nonzero_budget_and_exact_generation() {
    let valid = request();
    for expiry in [None, Some(1), Some(u64::MAX)] {
        let mut request = valid.clone();
        request.expires_at_application_sequence = expiry;
        assert_eq!(
            decode_public_message::<v1::RegisterFollowerRequest>(&request.encode_to_vec()).unwrap(),
            request
        );
    }
    let mut invalid = vec![];
    let mut value = valid.clone();
    value.target = None;
    invalid.push(value);
    let mut value = valid.clone();
    value.hold_budget_sequences = 0;
    invalid.push(value);
    let mut value = valid.clone();
    value.expires_at_application_sequence = Some(0);
    invalid.push(value);
    let mut value = valid.clone();
    value.target.as_mut().unwrap().database_id = vec![0; 16];
    invalid.push(value);
    let mut value = valid.clone();
    value.target.as_mut().unwrap().history_incarnation = 0;
    invalid.push(value);
    let mut value = valid.clone();
    value.target.as_mut().unwrap().leadership_epoch = 0;
    invalid.push(value);
    let mut value = valid.clone();
    value.target.as_mut().unwrap().hold_id = vec![0; 16];
    invalid.push(value);
    let mut value = valid.clone();
    value.target.as_mut().unwrap().hold_id = vec![1; 17];
    invalid.push(value);
    for value in invalid {
        assert!(validate_public_message(&value).is_err());
        assert!(
            decode_public_message::<v1::RegisterFollowerRequest>(&value.encode_to_vec()).is_err()
        );
    }
    let mut retire = v1::RetireFollowerRequest {
        request_id: valid.request_id,
        target: valid.target,
        registration_generation: 1,
    };
    assert_eq!(
        decode_public_message::<v1::RetireFollowerRequest>(&retire.encode_to_vec()).unwrap(),
        retire
    );
    retire.registration_generation = 0;
    assert!(validate_public_message(&retire).is_err());
}

#[test]
fn lifecycle_wire_refuses_duplicate_nested_fields_and_ambiguous_outcomes() {
    let value = request();
    let mut target = value.target.as_ref().unwrap().encode_to_vec();
    target.extend_from_slice(&[0x10, 0x01]); // repeated incarnation
    let mut bytes = vec![0x0a, 16];
    bytes.extend_from_slice(&value.request_id);
    bytes.extend_from_slice(&[0x12, u8::try_from(target.len()).unwrap()]);
    bytes.extend_from_slice(&target);
    bytes.extend_from_slice(&[0x18, 1]);
    assert!(decode_public_message::<v1::RegisterFollowerRequest>(&bytes).is_err());
    for replayed in [false, true] {
        let receipt = v1::FollowerAdministrationReceipt {
            administration_sequence: 9,
            registration_generation: 8,
            replayed,
        };
        let response = v1::RegisterFollowerResponse {
            result: Some(v1::register_follower_response::Result::Receipt(receipt)),
        };
        let mut bytes = response.encode_to_vec();
        bytes.extend_from_slice(&[0x10, 1]);
        assert!(decode_public_message::<v1::RegisterFollowerResponse>(&bytes).is_err());
        let bytes = response.encode_to_vec();
        assert_eq!(
            decode_public_message::<v1::RegisterFollowerResponse>(&bytes).unwrap(),
            response
        );
        let retire = decode_public_message::<v1::RetireFollowerResponse>(&bytes).unwrap();
        assert!(matches!(
            retire.result,
            Some(v1::retire_follower_response::Result::Receipt(_))
        ));
    }
    for refusal in [0, 6, -1, i32::MAX] {
        let value = v1::RegisterFollowerResponse {
            result: Some(v1::register_follower_response::Result::Refusal(refusal)),
        };
        assert!(validate_public_message(&value).is_err());
        assert!(
            decode_public_message::<v1::RetireFollowerResponse>(&value.encode_to_vec()).is_err()
        );
    }
    for sequence in [(0, 1), (1, 0)] {
        let value = v1::RegisterFollowerResponse {
            result: Some(v1::register_follower_response::Result::Receipt(
                v1::FollowerAdministrationReceipt {
                    administration_sequence: sequence.0,
                    registration_generation: sequence.1,
                    replayed: false,
                },
            )),
        };
        assert!(validate_public_message(&value).is_err());
    }
    assert!(decode_public_message::<v1::RegisterFollowerRequest>(&vec![0; 1_048_577]).is_err());
}

#[test]
fn retirement_exchange_rejects_another_generation_even_when_both_messages_are_valid() {
    let registration = request();
    let request = v1::RetireFollowerRequest {
        request_id: registration.request_id,
        target: registration.target,
        registration_generation: 12,
    };
    for replayed in [false, true] {
        let mut response = v1::RetireFollowerResponse {
            result: Some(v1::retire_follower_response::Result::Receipt(
                v1::FollowerAdministrationReceipt {
                    administration_sequence: 20,
                    registration_generation: 12,
                    replayed,
                },
            )),
        };
        riffdb_proto::validate_retire_follower_exchange(&request, &response).unwrap();
        let Some(v1::retire_follower_response::Result::Receipt(receipt)) = &mut response.result
        else {
            unreachable!()
        };
        receipt.registration_generation = 13;
        validate_public_message(&response).unwrap();
        assert_eq!(
            riffdb_proto::validate_retire_follower_exchange(&request, &response),
            Err(riffdb_proto::PublicWireError::InconsistentFields)
        );
    }
}
