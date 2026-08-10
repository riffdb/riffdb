//! Frozen local driver protocol boundary tests.

use std::collections::BTreeMap;

use riffdb_driver_host::{
    DRIVER_ERROR_REGISTRY_HASH, DRIVER_PROTOCOL_VERSION, DRIVER_VALUE_REGISTRY_HASH, DriverRequest,
    DriverResponse, DriverValue, FrameCodec, InvokeOptions, ProtocolError,
};

const HASH: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

#[test]
fn request_round_trip_is_exact_and_name_addressed() {
    let request = DriverRequest::Invoke {
        request_id: "req-1".to_owned(),
        operation: "ticketdesk_create_ticket".to_owned(),
        input_schema_hash: HASH.to_owned(),
        input: BTreeMap::from([
            ("priority".to_owned(), DriverValue::I64("2".to_owned())),
            (
                "title".to_owned(),
                DriverValue::String("Cannot sign in".to_owned()),
            ),
        ]),
        options: InvokeOptions {
            deadline_millis: 10_000,
            maximum_attempts: 3,
            read_after_commit: None,
            cursor: None,
        },
    };
    let encoded = FrameCodec::encode_request(&request).expect("encode");
    assert_eq!(
        FrameCodec::decode_request(&encoded).expect("decode"),
        request
    );
    let text = std::str::from_utf8(&encoded[4..]).expect("utf8");
    for forbidden in [
        "credential",
        "bearer",
        "endpoint",
        "entity_type_id",
        "field_id",
        "protobuf",
    ] {
        assert!(
            !text.contains(forbidden),
            "forbidden local surface: {forbidden}"
        );
    }
}

#[test]
fn handshake_registry_identities_are_frozen() {
    let request = DriverRequest::Handshake {
        request_id: "hello-1".to_owned(),
        protocol_version: DRIVER_PROTOCOL_VERSION,
        application_manifest_hash: HASH.to_owned(),
        operation_catalog_hash: HASH.to_owned(),
        contract_lineage: "Ea".to_owned(),
        contract_version: 4,
        contract_bundle_hash: HASH.to_owned(),
        value_registry_hash: DRIVER_VALUE_REGISTRY_HASH.to_owned(),
        error_registry_hash: DRIVER_ERROR_REGISTRY_HASH.to_owned(),
        database: "ea".to_owned(),
        role: "EaApplication".to_owned(),
        role_definition_hash: HASH.to_owned(),
        remote_identity_hash: HASH.to_owned(),
    };
    let encoded = FrameCodec::encode_request(&request).expect("encode");
    assert_eq!(
        FrameCodec::decode_request(&encoded).expect("decode"),
        request
    );
    let expected = include_bytes!("../../../fixtures/driver/v1/handshake-request.json");
    assert_eq!(
        &encoded[4..],
        expected.strip_suffix(b"\n").unwrap_or(expected)
    );
}

#[test]
fn structured_application_error_context_is_frozen() {
    let response = DriverResponse::Error {
        request_id: Some("req-1".to_owned()),
        code: "RDB-AUTH-0214".to_owned(),
        category: "authorization".to_owned(),
        operation: Some("ticketdesk_ticket_page".to_owned()),
        symbol_path: vec!["Ticket".to_owned(), "description".to_owned()],
        contract_lineage: Some("TicketDesk".to_owned()),
        contract_version: Some(5),
        trace_id: Some("018f0f79-7b5e-7c03-9b12-b16f57a4c998".to_owned()),
        incident_id: Some("incident-1".to_owned()),
        message: "application operation is not authorized".to_owned(),
        retryability: "not_retryable".to_owned(),
        recovery_action: "obtain_permission".to_owned(),
        outcome_uncertain: false,
    };
    let encoded = FrameCodec::encode_response(&response).expect("encode");
    assert_eq!(
        FrameCodec::decode_response(&encoded).expect("decode"),
        response
    );
    let expected = include_bytes!("../../../fixtures/driver/v1/application-error.json");
    assert_eq!(
        &encoded[4..],
        expected.strip_suffix(b"\n").unwrap_or(expected)
    );
}

#[test]
fn hostile_lengths_and_unknown_fields_fail_closed() {
    let mut oversized = Vec::from(u32::MAX.to_be_bytes());
    oversized.extend_from_slice(b"{}");
    assert_eq!(
        FrameCodec::decode_request(&oversized),
        Err(ProtocolError::InvalidFrameLength)
    );

    let body = br#"{"type":"cancel","request_id":"cancel-1","target_request_id":"req-1","endpoint":"https://attacker"}"#;
    let mut frame = Vec::from(u32::try_from(body.len()).expect("small").to_be_bytes());
    frame.extend_from_slice(body);
    assert_eq!(
        FrameCodec::decode_request(&frame),
        Err(ProtocolError::InvalidMessage)
    );
}

#[test]
fn bounded_batch_round_trip_retains_independent_items_and_checkpoint() {
    let request = DriverRequest::Batch {
        request_id: "seed-1".to_owned(),
        operation: "ticketdesk_create_comment".to_owned(),
        input_schema_hash: HASH.to_owned(),
        items: vec![
            BTreeMap::from([("body".to_owned(), DriverValue::String("first".to_owned()))]),
            BTreeMap::from([("body".to_owned(), DriverValue::String("second".to_owned()))]),
        ],
        concurrency: 2,
        checkpoint: 0,
        options: InvokeOptions {
            deadline_millis: 30_000,
            maximum_attempts: 3,
            read_after_commit: None,
            cursor: None,
        },
    };
    let frame = FrameCodec::encode_request(&request).expect("encode batch");
    assert_eq!(
        FrameCodec::decode_request(&frame).expect("decode batch"),
        request
    );
}
