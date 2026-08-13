//! Closed operator-driver protocol invariants.

use riffdb_driver_host::{
    OPERATOR_DRIVER_PROTOCOL_VERSION, OperatorDriverRequest, OperatorFrameCodec,
    OperatorProtocolError,
};

const CAMPAIGN: &str = "018f2f85-3c20-7a31-8f11-112233445566";
const OPERATION: &str = "018f2f85-3c20-7a31-8f11-112233445577";
const HASH: &str = "0101010101010101010101010101010101010101010101010101010101010101";

#[test]
fn operator_protocol_is_campaign_bound_and_disjoint_from_application_operations() {
    let handshake = OperatorDriverRequest::Handshake {
        request_id: "handshake-1".to_owned(),
        protocol_version: OPERATOR_DRIVER_PROTOCOL_VERSION,
        database: "restored".to_owned(),
        campaign_id: CAMPAIGN.to_owned(),
        portability_manifest_hash: HASH.to_owned(),
    };
    let bytes = OperatorFrameCodec::encode_request(&handshake).expect("closed handshake");
    assert_eq!(
        OperatorFrameCodec::decode_request(&bytes).expect("canonical request"),
        handshake
    );
    let text = std::str::from_utf8(&bytes).expect("JSON");
    assert!(!text.contains("invoke"));
    assert!(!text.contains("operation_catalog"));
    assert!(!text.contains("credential"));
}

#[test]
fn page_requires_exact_hash_position_and_terminal_cursor_relation() {
    let page = OperatorDriverRequest::ApplyPage {
        request_id: "page-1".to_owned(),
        export_operation_id: OPERATION.to_owned(),
        page_number: 1,
        canonical_json_lines: vec!["{\"entity\":\"Ticket\"}".to_owned()],
        next_cursor_base64: None,
        class_complete: true,
        operation_complete: true,
        page_hash_hex: HASH.to_owned(),
        maximum_attempts: 3,
    };
    assert!(OperatorFrameCodec::encode_request(&page).is_ok());

    let OperatorDriverRequest::ApplyPage {
        operation_complete,
        next_cursor_base64,
        ..
    } = &page
    else {
        unreachable!()
    };
    assert!(*operation_complete && next_cursor_base64.is_none());

    let OperatorDriverRequest::ApplyPage {
        request_id,
        export_operation_id,
        canonical_json_lines,
        next_cursor_base64,
        class_complete,
        operation_complete,
        page_hash_hex,
        maximum_attempts,
        ..
    } = page
    else {
        unreachable!()
    };
    let alternate = OperatorDriverRequest::ApplyPage {
        request_id,
        export_operation_id,
        page_number: 0,
        canonical_json_lines,
        next_cursor_base64,
        class_complete,
        operation_complete,
        page_hash_hex,
        maximum_attempts,
    };
    assert!(matches!(
        OperatorFrameCodec::encode_request(&alternate),
        Err(OperatorProtocolError::Invalid)
    ));
}

#[test]
fn alternate_json_encoding_fails_closed() {
    let bytes = "{ \"type\":\"status\",\"request_id\":\"status-1\" }";
    assert!(matches!(
        OperatorFrameCodec::decode_request(bytes.as_bytes()),
        Err(OperatorProtocolError::NonCanonical)
    ));
}
