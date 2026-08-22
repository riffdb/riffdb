#![allow(missing_docs)]

use prost::Message;
use riffdb_api_frame::{
    FRAME_HEADER_BYTES, FRAME_MAGIC_V1, FRAME_PROTOCOL_V1, Frame, FrameError, FrameIoError,
    FrameKind, read_frame, read_request, read_response, write_frame, write_request, write_response,
};
use riffdb_proto::v1;
use tokio::io::AsyncWriteExt;

fn cancel_request() -> v1::ApplicationSessionRequest {
    v1::ApplicationSessionRequest {
        correlation_id: 7,
        request: Some(v1::application_session_request::Request::Cancel(
            v1::ApplicationSessionCancel {
                target_correlation_id: 3,
            },
        )),
    }
}

fn cancellation_response() -> v1::ApplicationSessionResponse {
    v1::ApplicationSessionResponse {
        correlation_id: 7,
        response: Some(v1::application_session_response::Response::Cancellation(
            v1::ApplicationSessionCancellation {
                target_correlation_id: 3,
                disposition: v1::ApplicationSessionCancellationDisposition::NotLive as i32,
            },
        )),
    }
}

#[test]
fn request_frame_has_frozen_big_endian_header_and_round_trips() {
    let request = cancel_request();
    let frame = Frame::request(&request).expect("valid request");
    let mut encoded = Vec::new();
    frame.encode(&mut encoded);

    assert_eq!(&encoded[..8], &FRAME_MAGIC_V1);
    assert_eq!(encoded[8], FRAME_PROTOCOL_V1);
    assert_eq!(encoded[9], FrameKind::Request as u8);
    assert_eq!(&encoded[10..12], &[0, 0]);
    assert_eq!(&encoded[12..20], &7_u64.to_be_bytes());
    assert_eq!(
        &encoded[20..24],
        &(request.encoded_len() as u32).to_be_bytes()
    );
    assert_eq!(encoded.len(), FRAME_HEADER_BYTES + request.encoded_len());

    let decoded = Frame::decode_exact(&encoded).expect("decode frame");
    assert_eq!(decoded.decode_request().expect("decode request"), request);
}

#[test]
fn response_frame_round_trips_without_direction_confusion() {
    let response = cancellation_response();
    let frame = Frame::response(&response).expect("valid response");
    let mut encoded = Vec::new();
    frame.encode(&mut encoded);
    let decoded = Frame::decode_exact(&encoded).expect("decode frame");
    assert_eq!(decoded.kind(), FrameKind::Response);
    assert_eq!(
        decoded.decode_response().expect("decode response"),
        response
    );
    assert_eq!(decoded.decode_request(), Err(FrameError::DirectionMismatch));
}

#[test]
fn malformed_header_and_payload_shapes_fail_closed() {
    let frame = Frame::request(&cancel_request()).expect("valid request");
    let mut encoded = Vec::new();
    frame.encode(&mut encoded);

    let mut invalid_magic = encoded.clone();
    invalid_magic[0] ^= 1;
    assert_eq!(
        Frame::decode_exact(&invalid_magic),
        Err(FrameError::InvalidMagic)
    );

    let mut invalid_version = encoded.clone();
    invalid_version[8] = 2;
    assert_eq!(
        Frame::decode_exact(&invalid_version),
        Err(FrameError::UnsupportedVersion)
    );

    let mut invalid_kind = encoded.clone();
    invalid_kind[9] = 3;
    assert_eq!(
        Frame::decode_exact(&invalid_kind),
        Err(FrameError::UnknownKind)
    );

    let mut invalid_flags = encoded.clone();
    invalid_flags[11] = 1;
    assert_eq!(
        Frame::decode_exact(&invalid_flags),
        Err(FrameError::NonzeroFlags)
    );

    let mut zero_correlation = encoded.clone();
    zero_correlation[12..20].fill(0);
    assert_eq!(
        Frame::decode_exact(&zero_correlation),
        Err(FrameError::ZeroCorrelation)
    );

    assert_eq!(
        Frame::decode_exact(&encoded[..FRAME_HEADER_BYTES - 1]),
        Err(FrameError::Truncated)
    );
    let mut trailing = encoded.clone();
    trailing.push(0);
    assert_eq!(
        Frame::decode_exact(&trailing),
        Err(FrameError::TrailingBytes)
    );
}

#[test]
fn header_payload_correlation_mismatch_is_rejected() {
    let frame = Frame::request(&cancel_request()).expect("valid request");
    let mut encoded = Vec::new();
    frame.encode(&mut encoded);
    encoded[12..20].copy_from_slice(&8_u64.to_be_bytes());
    assert_eq!(
        Frame::decode_exact(&encoded),
        Err(FrameError::CorrelationMismatch)
    );
}

#[test]
fn invalid_public_payload_is_rejected_before_use() {
    let frame = Frame::request(&cancel_request()).expect("valid request");
    let mut encoded = Vec::new();
    frame.encode(&mut encoded);
    encoded.truncate(FRAME_HEADER_BYTES);
    encoded[20..24].copy_from_slice(&0_u32.to_be_bytes());
    assert_eq!(
        Frame::decode_exact(&encoded),
        Err(FrameError::InvalidPayload)
    );
}

#[tokio::test]
async fn fragmented_and_coalesced_frames_are_read_exactly_once() {
    let first = Frame::request(&cancel_request()).expect("first frame");
    let mut second_request = cancel_request();
    second_request.correlation_id = 8;
    let second = Frame::request(&second_request).expect("second frame");
    let mut bytes = Vec::new();
    first.encode(&mut bytes);
    second.encode(&mut bytes);

    let (mut writer, mut reader) = tokio::io::duplex(bytes.len() + 1);
    let split = FRAME_HEADER_BYTES - 3;
    writer
        .write_all(&bytes[..split])
        .await
        .expect("fragment prefix");
    writer
        .write_all(&bytes[split..])
        .await
        .expect("coalesced suffix");

    assert_eq!(read_frame(&mut reader).await.expect("first"), first);
    assert_eq!(read_frame(&mut reader).await.expect("second"), second);
}

#[tokio::test]
async fn oversized_declared_payload_fails_before_body_read() {
    let mut header = [0_u8; FRAME_HEADER_BYTES];
    header[..8].copy_from_slice(&FRAME_MAGIC_V1);
    header[8] = FRAME_PROTOCOL_V1;
    header[9] = FrameKind::Request as u8;
    header[12..20].copy_from_slice(&1_u64.to_be_bytes());
    header[20..24].copy_from_slice(&u32::MAX.to_be_bytes());
    let (mut writer, mut reader) = tokio::io::duplex(FRAME_HEADER_BYTES);
    writer.write_all(&header).await.expect("header");
    assert!(matches!(
        read_frame(&mut reader).await,
        Err(FrameIoError::InvalidFrame(FrameError::PayloadTooLarge))
    ));
}

#[tokio::test]
async fn async_writer_emits_the_canonical_frame() {
    let frame = Frame::response(&cancellation_response()).expect("response");
    let (mut writer, mut reader) = tokio::io::duplex(frame.encoded_len());
    write_frame(&mut writer, &frame).await.expect("write");
    assert_eq!(read_frame(&mut reader).await.expect("read"), frame);
}

#[tokio::test]
async fn typed_read_write_helpers_preserve_canonical_bytes_and_direction() {
    let request = cancel_request();
    let response = cancellation_response();
    let mut expected_request = Vec::new();
    Frame::request(&request)
        .expect("request")
        .encode(&mut expected_request);
    let mut expected_response = Vec::new();
    Frame::response(&response)
        .expect("response")
        .encode(&mut expected_response);

    let (mut request_writer, mut request_reader) = tokio::io::duplex(expected_request.len());
    write_request(&mut request_writer, &request)
        .await
        .expect("write request");
    assert_eq!(
        read_request(&mut request_reader)
            .await
            .expect("read request"),
        request
    );

    let (mut response_writer, mut response_reader) = tokio::io::duplex(expected_response.len());
    write_response(&mut response_writer, &response)
        .await
        .expect("write response");
    assert_eq!(
        read_response(&mut response_reader)
            .await
            .expect("read response"),
        response
    );

    let (mut wrong_writer, mut wrong_reader) = tokio::io::duplex(expected_request.len());
    wrong_writer
        .write_all(&expected_request)
        .await
        .expect("write request bytes");
    assert!(matches!(
        read_response(&mut wrong_reader).await,
        Err(FrameIoError::InvalidFrame(FrameError::DirectionMismatch))
    ));
}
