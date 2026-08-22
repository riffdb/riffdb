#![forbid(unsafe_code)]

//! Hostile and fragmented diagnostic-frame semantics.

use riffdb_api_exclusive::{
    DIAGNOSTIC_CREDENTIAL_BYTES, DIAGNOSTIC_MAGIC, DiagnosticServerStages, encode_response,
    read_open, read_request, read_response, write_open, write_request,
};
use tokio::io::{AsyncWriteExt as _, duplex};

#[tokio::test]
async fn checked_open_and_frames_survive_fragmented_duplex_carriage() {
    let (mut client, mut server) = duplex(256);
    let credential = vec![b'A'; DIAGNOSTIC_CREDENTIAL_BYTES];
    let client_task = tokio::spawn(async move {
        write_open(&mut client, &credential, b"alpha", b"exact-session")
            .await
            .unwrap();
        write_request(&mut client, b"request").await.unwrap();
        read_response(&mut client).await.unwrap()
    });
    let open = read_open(&mut server).await.unwrap();
    assert_eq!(open.credential, vec![b'A'; DIAGNOSTIC_CREDENTIAL_BYTES]);
    assert_eq!(open.database, b"alpha");
    assert_eq!(open.application_session, b"exact-session");
    assert_eq!(read_request(&mut server).await.unwrap(), b"request");
    let stages = DiagnosticServerStages {
        decode_adapt_ns: 1,
        application_service_ns: 2,
        encode_ns: 3,
        previous_write_ns: 4,
    };
    server
        .write_all(&encode_response(0, stages, b"response").unwrap())
        .await
        .unwrap();
    let response = client_task.await.unwrap();
    assert_eq!(response.status_code, 0);
    assert_eq!(response.stages, stages);
    assert_eq!(response.payload, b"response");
}

#[tokio::test]
async fn unknown_magic_and_oversized_lengths_fail_before_payload_allocation() {
    let (mut writer, mut reader) = duplex(128);
    let mut invalid = DIAGNOSTIC_MAGIC;
    invalid[0] ^= 1;
    writer.write_all(&invalid).await.unwrap();
    writer.write_all(&43_u16.to_be_bytes()).await.unwrap();
    writer.write_all(&0_u16.to_be_bytes()).await.unwrap();
    writer.write_all(&1_u16.to_be_bytes()).await.unwrap();
    assert_eq!(
        read_open(&mut reader).await.unwrap_err().kind(),
        std::io::ErrorKind::InvalidData
    );

    let (mut writer, mut reader) = duplex(16);
    writer.write_all(&u32::MAX.to_be_bytes()).await.unwrap();
    assert_eq!(
        read_request(&mut reader).await.unwrap_err().kind(),
        std::io::ErrorKind::InvalidData
    );
}

#[tokio::test]
async fn empty_and_oversized_outbound_requests_are_rejected_locally() {
    let (mut writer, _reader) = duplex(16);
    assert_eq!(
        write_request(&mut writer, &[]).await.unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );
    let oversized = vec![0_u8; riffdb_api_exclusive::MAX_DIAGNOSTIC_REQUEST_BYTES + 1];
    assert_eq!(
        write_request(&mut writer, &oversized)
            .await
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::InvalidInput
    );
}
