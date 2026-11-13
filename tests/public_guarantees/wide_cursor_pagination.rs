// req: QRY-001, DRV-005
//! End-to-end proof that cursor-paginated RiffQL queries projecting wide
//! `string<1048576>` and `bytes<1048576>` columns alongside a sibling `One`
//! entity binding dynamically split pages within the `4 MiB` response ceiling
//! and resume continuation cursors without duplicates or gaps across gRPC and
//! `riffdb-driverd`.

use std::collections::{BTreeMap, BTreeSet};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use riffdb_client_rust::ApplicationValue;
use riffdb_driver_host::{DriverResponse, DriverValue};

use crate::support::{
    CommitProfile, PRIMARY_ROLE, ServerOptions, TestResult, TestWorkspace, execute_driver_query,
    execute_grpc_command, execute_grpc_query,
};

pub(crate) async fn run() -> TestResult<()> {
    let ws = TestWorkspace::new("pg-wide-cursor")?;
    let mut server = ws.spawn_server(&ServerOptions {
        commit_profile: CommitProfile::StandardGroup,
        environment: "development",
        authorization_diagnostics: None,
    })?;
    ws.bootstrap_operator()?;
    let app = ws.lock_and_deploy("development")?;
    let mut driver = ws
        .spawn_driver_host(&app.role_credential_path, PRIMARY_ROLE, "driver-wide.sock")
        .await?;

    let (mut grpc_client, grpc_meta) = ws.connect_app_client(&app.role_credential_path).await?;
    let trace_id = "trace-wide-001";
    let expected_name = "checkout-pipeline-trace";
    let expected_summary = "8 spans with mixed small and multi-hundred-KiB payloads";

    let header_cmd = app.build_grpc_command(
        "CreateTraceHeader",
        BTreeMap::from([
            (
                "req_key".to_owned(),
                ApplicationValue::String("wide-header-v1".to_owned()),
            ),
            (
                "trace_id".to_owned(),
                ApplicationValue::String(trace_id.to_owned()),
            ),
            (
                "trace_name".to_owned(),
                ApplicationValue::String(expected_name.to_owned()),
            ),
            (
                "status_summary".to_owned(),
                ApplicationValue::String(expected_summary.to_owned()),
            ),
        ]),
    )?;
    execute_grpc_command(&mut grpc_client, &grpc_meta, header_cmd, 3).await?;

    // Total payload across the 8 spans is ~5.9 MiB (> 4 MiB single-page ceiling),
    // while `take 10 after $after` asks for up to 10 rows per page.
    let sizes: &[(usize, usize)] = &[
        (4_096, 2_048),
        (200_000, 720_000),
        (220_000, 740_000),
        (240_000, 760_000),
        (240_000, 760_000),
        (8_192, 8_192),
        (210_000, 730_000),
        (230_000, 750_000),
    ];

    let mut expected_spans = Vec::with_capacity(sizes.len());
    let mut last_seq = 0_u64;

    for (index, &(text_len, blob_len)) in sizes.iter().enumerate() {
        let seq_no = (index as i64 + 1) * 10;
        let span_id = format!("span-{:02}", index + 1);
        let ch = char::from(b'a' + (index as u8));
        let payload_text = std::iter::repeat_n(ch, text_len).collect::<String>();
        let payload_blob = vec![0xA0_u8.wrapping_add(index as u8); blob_len];

        let cmd = app.build_grpc_command(
            "RecordWideSpan",
            BTreeMap::from([
                (
                    "req_key".to_owned(),
                    ApplicationValue::String(format!("wide-span-{index}")),
                ),
                (
                    "trace_id".to_owned(),
                    ApplicationValue::String(trace_id.to_owned()),
                ),
                (
                    "span_id".to_owned(),
                    ApplicationValue::String(span_id.clone()),
                ),
                ("seq_no".to_owned(), ApplicationValue::I64(seq_no)),
                (
                    "payload_text".to_owned(),
                    ApplicationValue::String(payload_text.clone()),
                ),
                (
                    "payload_blob".to_owned(),
                    ApplicationValue::Bytes(payload_blob.clone()),
                ),
            ]),
        )?;
        let res = execute_grpc_command(&mut grpc_client, &grpc_meta, cmd, 3).await?;
        last_seq = res.commit_sequence.ok_or("missing wide span seq")?;
        expected_spans.push((span_id, seq_no, payload_text, payload_blob));
    }

    // 1. Traverse all pages over verified-TLS gRPC (`StableApplicationClient`):
    let mut grpc_collected = Vec::new();
    let mut seen_cursors = BTreeSet::new();
    let mut cursor: Option<String> = None;
    let mut grpc_pages = 0_usize;

    loop {
        if grpc_pages > 16 {
            return Err("gRPC wide cursor pagination failed to make forward progress".into());
        }
        let q = app.build_grpc_query(
            "PageWideSpansWithHeader",
            BTreeMap::from([
                (
                    "trace_id".to_owned(),
                    ApplicationValue::String(trace_id.to_owned()),
                ),
                ("limit".to_owned(), ApplicationValue::U64(10)),
            ]),
            cursor.clone(),
            if grpc_pages == 0 {
                Some(last_seq)
            } else {
                None
            },
        )?;
        let page = execute_grpc_query(&mut grpc_client, &grpc_meta, q).await?;
        grpc_pages += 1;
        assert_eq!(page.outcome, "Found");

        // Sibling `One` binding (`header`) must be present and intact on every split page.
        let header_records = &page.fields["header"].records;
        assert_eq!(header_records.len(), 1);
        assert_eq!(
            header_records[0].fields.get("trace_name"),
            Some(&ApplicationValue::String(expected_name.to_owned()))
        );
        assert_eq!(
            header_records[0].fields.get("status_summary"),
            Some(&ApplicationValue::String(expected_summary.to_owned()))
        );

        let span_records = &page.fields["spans"].records;
        assert!(
            !span_records.is_empty(),
            "non-empty trace page must emit at least one wide row before splitting"
        );
        let mut page_payload_bytes = 0_usize;
        for rec in span_records {
            let Some(ApplicationValue::String(span_id)) = rec.fields.get("span_id") else {
                return Err("missing span_id".into());
            };
            let Some(ApplicationValue::I64(seq_no)) = rec.fields.get("seq_no") else {
                return Err("missing seq_no".into());
            };
            let Some(ApplicationValue::String(text)) = rec.fields.get("payload_text") else {
                return Err("missing payload_text".into());
            };
            let Some(ApplicationValue::Bytes(blob)) = rec.fields.get("payload_blob") else {
                return Err("missing payload_blob".into());
            };
            page_payload_bytes += text.len() + blob.len();
            grpc_collected.push((span_id.clone(), *seq_no, text.clone(), blob.clone()));
        }
        assert!(
            page_payload_bytes <= 4 * 1024 * 1024,
            "page payload exceeded 4 MiB ceiling: {page_payload_bytes}"
        );

        match page.next_cursor {
            Some(next) => {
                if !seen_cursors.insert(next.clone()) {
                    return Err("continuation cursor repeated without advancing".into());
                }
                cursor = Some(next);
            }
            None => break,
        }
    }

    assert!(
        grpc_pages >= 2,
        "~5.8 MiB across 8 spans with Limit=10 must split across >= 2 pages, got {grpc_pages}"
    );
    assert_eq!(
        grpc_collected, expected_spans,
        "reassembled gRPC wide-cursor pages diverged from seeded rows"
    );

    // 2. Traverse all pages over `riffdb-driverd` Unix socket (`DriverSocketState`):
    let mut sock = driver.connect(PRIMARY_ROLE).await?;
    let mut driver_collected = Vec::new();
    let mut driver_cursor: Option<String> = None;
    let mut driver_pages = 0_usize;

    loop {
        if driver_pages > 16 {
            return Err("driver wide cursor pagination failed to advance".into());
        }
        let resp = execute_driver_query(
            &mut sock,
            &app,
            "PageWideSpansWithHeader",
            BTreeMap::from([
                (
                    "trace_id".to_owned(),
                    DriverValue::String(trace_id.to_owned()),
                ),
                ("limit".to_owned(), DriverValue::U64("10".to_owned())),
            ]),
            driver_cursor.clone(),
            if driver_pages == 0 {
                Some(last_seq)
            } else {
                None
            },
        )
        .await
        .map_err(|e| format!("driver wide query failed: {:?} {:?}", e.kind, e.details))?;
        driver_pages += 1;

        let DriverResponse::Result {
            value: DriverValue::Record(top),
            cursor: next_cursor,
            ..
        } = resp
        else {
            return Err("unexpected DriverResponse shape for wide query".into());
        };
        assert_eq!(
            top.get("outcome"),
            Some(&DriverValue::Enum("Found".to_owned()))
        );
        let Some(DriverValue::Record(header)) = top.get("header") else {
            return Err("driver response missing sibling header record".into());
        };
        assert_eq!(
            header.get("trace_name"),
            Some(&DriverValue::String(expected_name.to_owned()))
        );
        let Some(DriverValue::List(spans)) = top.get("spans") else {
            return Err("driver response missing spans list".into());
        };
        assert!(!spans.is_empty());

        for item in spans {
            let DriverValue::Record(rec) = item else {
                return Err("driver span item was not Record".into());
            };
            let Some(DriverValue::String(span_id)) = rec.get("span_id") else {
                return Err("driver span missing span_id".into());
            };
            let Some(DriverValue::I64(seq_str)) = rec.get("seq_no") else {
                return Err("driver span missing seq_no".into());
            };
            let Some(DriverValue::String(text)) = rec.get("payload_text") else {
                return Err("driver span missing payload_text".into());
            };
            let Some(DriverValue::Bytes(blob_b64)) = rec.get("payload_blob") else {
                return Err("driver span missing payload_blob".into());
            };
            let blob = BASE64.decode(blob_b64)?;
            driver_collected.push((span_id.clone(), seq_str.parse::<i64>()?, text.clone(), blob));
        }

        match next_cursor {
            Some(next) => driver_cursor = Some(next),
            None => break,
        }
    }

    assert_eq!(driver_pages, grpc_pages);
    assert_eq!(
        driver_collected, expected_spans,
        "reassembled riffdb-driverd wide-cursor pages diverged from seeded rows"
    );

    driver.shutdown().await?;
    server.shutdown()?;
    Ok(())
}
