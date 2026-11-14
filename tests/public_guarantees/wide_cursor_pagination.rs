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
    CommitProfile, McpSession, McpStdioSession, PRIMARY_ROLE, ServerOptions, TestResult,
    TestWorkspace, execute_driver_query, execute_grpc_command, execute_grpc_query,
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

    // 3. Traverse `trace-wide-001` AND an escaped-control-character sibling trace
    //    (`300,000`-byte `\x01` header + four `100,000`-byte spans, `limit = 10`)
    //    across BOTH Hosted HTTP MCP and Stdio MCP (`riffdb-mcp`).
    let mcp_tool = app.mcp_tool_name("PageWideSpansWithHeader")?;
    let hosted_mcp = McpSession::connect(ws.mcp_address(), &app.role_credential_path)?;
    let mut stdio_mcp = McpStdioSession::spawn(&ws, &app.role_credential_path)?;

    let (hosted_wide_pages, hosted_wide_spans) = collect_mcp_wide_pages(
        "hosted-mcp(wide)",
        &mcp_tool,
        trace_id,
        10,
        expected_name,
        expected_summary,
        |tool, args| hosted_mcp.call_tool(tool, args),
    )?;
    assert!(hosted_wide_pages >= 2);
    assert_eq!(hosted_wide_spans, expected_spans);

    let (stdio_wide_pages, stdio_wide_spans) = collect_mcp_wide_pages(
        "stdio-mcp(wide)",
        &mcp_tool,
        trace_id,
        10,
        expected_name,
        expected_summary,
        |tool, args| stdio_mcp.call_tool(tool, args),
    )?;
    assert_eq!(stdio_wide_pages, hosted_wide_pages);
    assert_eq!(stdio_wide_spans, expected_spans);

    // Seed escaped-sibling trace: 300,000 `\x01` control bytes expand ~13x across
    // `structuredContent` (`\u0001`) and JSON-escaped `content[0].text` (`\\u0001`)
    // (~3.9 MB), leaving room for 1 of the four 100,000-byte rows per page, not all 4.
    let escaped_trace_id = "trace-escaped-sibling-002";
    let escaped_name = "escaped-control-char-sibling-trace";
    let escaped_summary = "\x01".repeat(300_000);
    let escaped_header_cmd = app.build_grpc_command(
        "CreateTraceHeader",
        BTreeMap::from([
            (
                "req_key".to_owned(),
                ApplicationValue::String("escaped-header-v1".to_owned()),
            ),
            (
                "trace_id".to_owned(),
                ApplicationValue::String(escaped_trace_id.to_owned()),
            ),
            (
                "trace_name".to_owned(),
                ApplicationValue::String(escaped_name.to_owned()),
            ),
            (
                "status_summary".to_owned(),
                ApplicationValue::String(escaped_summary.clone()),
            ),
        ]),
    )?;
    execute_grpc_command(&mut grpc_client, &grpc_meta, escaped_header_cmd, 3).await?;

    let mut expected_escaped_spans = Vec::with_capacity(4);
    for index in 0..4_usize {
        let seq_no = (index as i64 + 1) * 10;
        let span_id = format!("esc-span-{:02}", index + 1);
        let ch = char::from(b'A' + (index as u8));
        let payload_text = std::iter::repeat_n(ch, 100_000).collect::<String>();
        let payload_blob = Vec::new();
        let cmd = app.build_grpc_command(
            "RecordWideSpan",
            BTreeMap::from([
                (
                    "req_key".to_owned(),
                    ApplicationValue::String(format!("escaped-span-{index}")),
                ),
                (
                    "trace_id".to_owned(),
                    ApplicationValue::String(escaped_trace_id.to_owned()),
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
        execute_grpc_command(&mut grpc_client, &grpc_meta, cmd, 3).await?;
        expected_escaped_spans.push((span_id, seq_no, payload_text, payload_blob));
    }

    let (hosted_esc_pages, hosted_esc_spans) = collect_mcp_wide_pages(
        "hosted-mcp(escaped-sibling,limit=10)",
        &mcp_tool,
        escaped_trace_id,
        10,
        escaped_name,
        &escaped_summary,
        |tool, args| hosted_mcp.call_tool(tool, args),
    )?;
    assert_eq!(
        hosted_esc_pages, 4,
        "300KB control-char sibling + 4x100KB rows with limit=10 must split into 4 single-row pages on Hosted MCP"
    );
    assert_eq!(hosted_esc_spans, expected_escaped_spans);

    let (stdio_esc_pages, stdio_esc_spans) = collect_mcp_wide_pages(
        "stdio-mcp(escaped-sibling,limit=10)",
        &mcp_tool,
        escaped_trace_id,
        10,
        escaped_name,
        &escaped_summary,
        |tool, args| stdio_mcp.call_tool(tool, args),
    )?;
    assert_eq!(
        stdio_esc_pages, 4,
        "300KB control-char sibling + 4x100KB rows with limit=10 must split into 4 single-row pages on Stdio MCP"
    );
    assert_eq!(stdio_esc_spans, expected_escaped_spans);

    // 4. Seed per-parent grouped aggregate (`QueryRow.nested_aggregates`) containing
    //    300,000 `\x01` control characters alongside four 100,000-character rows:
    //    `limit = 1` succeeds within 4 MiB, and `limit = 10` must split into 4
    //    single-row pages across BOTH Hosted MCP and Stdio MCP instead of failing
    //    with `RDB-MCP-INTERNAL`.
    let grouped_trace_id = "trace-grouped-agg-003";
    let grouped_bucket_id = "bucket-01";
    let grouped_bucket_title = "grouped-rollup-bucket";
    let grouped_label = "\x01".repeat(300_000);
    let bucket_cmd = app.build_grpc_command(
        "CreateTraceBucket",
        BTreeMap::from([
            (
                "req_key".to_owned(),
                ApplicationValue::String("grouped-bucket-v1".to_owned()),
            ),
            (
                "trace_id".to_owned(),
                ApplicationValue::String(grouped_trace_id.to_owned()),
            ),
            (
                "bucket_id".to_owned(),
                ApplicationValue::String(grouped_bucket_id.to_owned()),
            ),
            (
                "bucket_title".to_owned(),
                ApplicationValue::String(grouped_bucket_title.to_owned()),
            ),
        ]),
    )?;
    execute_grpc_command(&mut grpc_client, &grpc_meta, bucket_cmd, 3).await?;

    for note_idx in 1..=2_u8 {
        let note_cmd = app.build_grpc_command(
            "CreateTraceBucketNote",
            BTreeMap::from([
                (
                    "req_key".to_owned(),
                    ApplicationValue::String(format!("grouped-note-{note_idx}")),
                ),
                (
                    "trace_id".to_owned(),
                    ApplicationValue::String(grouped_trace_id.to_owned()),
                ),
                (
                    "bucket_id".to_owned(),
                    ApplicationValue::String(grouped_bucket_id.to_owned()),
                ),
                (
                    "note_id".to_owned(),
                    ApplicationValue::String(format!("note-{note_idx:02}")),
                ),
                (
                    "group_label".to_owned(),
                    ApplicationValue::String(grouped_label.clone()),
                ),
            ]),
        )?;
        execute_grpc_command(&mut grpc_client, &grpc_meta, note_cmd, 3).await?;
    }

    let mut expected_grouped_spans = Vec::with_capacity(4);
    for index in 0..4_usize {
        let seq_no = (index as i64 + 1) * 10;
        let span_id = format!("grp-span-{:02}", index + 1);
        let ch = char::from(b'E' + (index as u8));
        let payload_text = std::iter::repeat_n(ch, 100_000).collect::<String>();
        let cmd = app.build_grpc_command(
            "RecordMediumSpan",
            BTreeMap::from([
                (
                    "req_key".to_owned(),
                    ApplicationValue::String(format!("grouped-span-{index}")),
                ),
                (
                    "trace_id".to_owned(),
                    ApplicationValue::String(grouped_trace_id.to_owned()),
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
            ]),
        )?;
        execute_grpc_command(&mut grpc_client, &grpc_meta, cmd, 3).await?;
        expected_grouped_spans.push((span_id, seq_no, payload_text));
    }

    let grouped_mcp_tool = app.mcp_tool_name("PageMediumSpansWithGroupedRollup")?;

    let (hosted_grp_limit1_pages, hosted_grp_limit1_spans) = collect_mcp_grouped_rollup_pages(
        "hosted-mcp(nested-agg,limit=1)",
        &grouped_mcp_tool,
        grouped_trace_id,
        1,
        grouped_bucket_id,
        grouped_bucket_title,
        &grouped_label,
        2,
        |tool, args| hosted_mcp.call_tool(tool, args),
    )?;
    assert_eq!(hosted_grp_limit1_pages, 4);
    assert_eq!(hosted_grp_limit1_spans, expected_grouped_spans);

    let (stdio_grp_limit1_pages, stdio_grp_limit1_spans) = collect_mcp_grouped_rollup_pages(
        "stdio-mcp(nested-agg,limit=1)",
        &grouped_mcp_tool,
        grouped_trace_id,
        1,
        grouped_bucket_id,
        grouped_bucket_title,
        &grouped_label,
        2,
        |tool, args| stdio_mcp.call_tool(tool, args),
    )?;
    assert_eq!(stdio_grp_limit1_pages, 4);
    assert_eq!(stdio_grp_limit1_spans, expected_grouped_spans);

    let (hosted_grp_pages, hosted_grp_spans) = collect_mcp_grouped_rollup_pages(
        "hosted-mcp(nested-agg,limit=10)",
        &grouped_mcp_tool,
        grouped_trace_id,
        10,
        grouped_bucket_id,
        grouped_bucket_title,
        &grouped_label,
        2,
        |tool, args| hosted_mcp.call_tool(tool, args),
    )?;
    assert_eq!(
        hosted_grp_pages, 4,
        "300KB control-char nested grouped aggregate + 4x100KB rows with limit=10 must split into 4 single-row pages on Hosted MCP"
    );
    assert_eq!(hosted_grp_spans, expected_grouped_spans);

    let (stdio_grp_pages, stdio_grp_spans) = collect_mcp_grouped_rollup_pages(
        "stdio-mcp(nested-agg,limit=10)",
        &grouped_mcp_tool,
        grouped_trace_id,
        10,
        grouped_bucket_id,
        grouped_bucket_title,
        &grouped_label,
        2,
        |tool, args| stdio_mcp.call_tool(tool, args),
    )?;
    assert_eq!(
        stdio_grp_pages, 4,
        "300KB control-char nested grouped aggregate + 4x100KB rows with limit=10 must split into 4 single-row pages on Stdio MCP"
    );
    assert_eq!(stdio_grp_spans, expected_grouped_spans);

    stdio_mcp.shutdown()?;
    driver.shutdown().await?;
    server.shutdown()?;
    Ok(())
}

type MediumSpanTuple = (String, i64, String);

#[expect(
    clippy::too_many_arguments,
    reason = "test helper validates full parent bucket + nested grouped aggregate + paged span invariants"
)]
fn collect_mcp_grouped_rollup_pages(
    surface_label: &str,
    mcp_tool: &str,
    trace_id: &str,
    limit: u64,
    expected_bucket_id: &str,
    expected_bucket_title: &str,
    expected_group_label: &str,
    expected_note_count: u64,
    mut call: impl FnMut(&str, serde_json::Value) -> TestResult<serde_json::Value>,
) -> TestResult<(usize, Vec<MediumSpanTuple>)> {
    let mut collected = Vec::new();
    let mut seen_cursors = BTreeSet::new();
    let mut after: Option<String> = None;
    let mut pages = 0_usize;

    loop {
        if pages > 16 {
            return Err(format!("{surface_label} failed to make forward progress").into());
        }
        let mut args = serde_json::Map::new();
        args.insert(
            "trace_id".to_owned(),
            serde_json::Value::String(trace_id.to_owned()),
        );
        args.insert("limit".to_owned(), serde_json::Value::from(limit));
        if let Some(ref cursor) = after {
            args.insert(
                "after".to_owned(),
                serde_json::Value::String(cursor.clone()),
            );
        }
        let rpc_resp = call(mcp_tool, serde_json::Value::Object(args))?;
        let raw_bytes = serde_json::to_vec(&rpc_resp)?.len();
        if raw_bytes > 4_194_304 {
            return Err(
                format!("{surface_label} response exceeded 4 MiB: {raw_bytes} bytes").into(),
            );
        }
        if let Some(err) = rpc_resp.get("error") {
            return Err(format!("{surface_label} returned JSON-RPC error: {err}").into());
        }
        let call_result = rpc_resp
            .get("result")
            .ok_or_else(|| format!("{surface_label} missing JSON-RPC result"))?;
        if call_result
            .get("isError")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
        {
            let err_text = call_result
                .get("content")
                .and_then(serde_json::Value::as_array)
                .and_then(|arr| arr.first())
                .and_then(|item| item.get("text"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("<missing error text>");
            return Err(format!("{surface_label} returned MCP tool error: {err_text}").into());
        }
        pages += 1;

        let structured = call_result
            .get("structuredContent")
            .ok_or_else(|| format!("{surface_label} missing structuredContent"))?;
        let business = structured
            .get("result")
            .ok_or_else(|| format!("{surface_label} missing structuredContent.result"))?;
        if business.get("outcome").and_then(serde_json::Value::as_str) != Some("Found") {
            return Err(format!("{surface_label} unexpected outcome: {business}").into());
        }
        let buckets = business
            .get("buckets")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| format!("{surface_label} missing buckets array"))?;
        if buckets.len() != 1 {
            return Err(format!(
                "{surface_label} expected 1 bucket, observed {}",
                buckets.len()
            )
            .into());
        }
        let bucket = &buckets[0];
        if bucket.get("bucket_id").and_then(serde_json::Value::as_str) != Some(expected_bucket_id) {
            return Err(format!("{surface_label} bucket_id mismatch").into());
        }
        if bucket
            .get("bucket_title")
            .and_then(serde_json::Value::as_str)
            != Some(expected_bucket_title)
        {
            return Err(format!("{surface_label} bucket_title mismatch").into());
        }
        let note_groups = bucket
            .get("note_groups")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| format!("{surface_label} missing nested note_groups array"))?;
        if note_groups.len() != 1 {
            return Err(format!(
                "{surface_label} expected 1 nested note_groups row, observed {}",
                note_groups.len()
            )
            .into());
        }
        let group = &note_groups[0];
        if group.get("group_label").and_then(serde_json::Value::as_str)
            != Some(expected_group_label)
        {
            return Err(format!("{surface_label} nested group_label mismatch").into());
        }
        let note_count = group
            .get("note_count")
            .ok_or("nested note_groups missing note_count")
            .and_then(|v| {
                v.as_u64()
                    .or_else(|| v.as_str().and_then(|s| s.parse::<u64>().ok()))
                    .ok_or("nested note_count not u64")
            })?;
        if note_count != expected_note_count {
            return Err(format!(
                "{surface_label} note_count mismatch: expected {expected_note_count}, got {note_count}"
            )
            .into());
        }

        let spans = business
            .get("spans")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| format!("{surface_label} missing spans array"))?;
        if spans.is_empty() {
            return Err(format!("{surface_label} emitted empty split page").into());
        }
        for rec in spans {
            let span_id = rec
                .get("span_id")
                .and_then(serde_json::Value::as_str)
                .ok_or("MCP medium span missing span_id")?
                .to_owned();
            let seq_no = rec
                .get("seq_no")
                .ok_or("MCP medium span missing seq_no")
                .and_then(|v| {
                    v.as_i64()
                        .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
                        .ok_or("MCP medium span seq_no not i64")
                })?;
            let text = rec
                .get("payload_text")
                .and_then(serde_json::Value::as_str)
                .ok_or("MCP medium span missing payload_text")?
                .to_owned();
            collected.push((span_id, seq_no, text));
        }

        let next_cursor = structured
            .get("page")
            .and_then(|p| p.get("next_cursor"))
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
        match next_cursor {
            Some(next) => {
                if !seen_cursors.insert(next.clone()) {
                    return Err(format!("{surface_label} repeated continuation cursor").into());
                }
                after = Some(next);
            }
            None => break,
        }
    }

    Ok((pages, collected))
}

type SpanTuple = (String, i64, String, Vec<u8>);

fn collect_mcp_wide_pages(
    surface_label: &str,
    mcp_tool: &str,
    trace_id: &str,
    limit: u64,
    expected_name: &str,
    expected_summary: &str,
    mut call: impl FnMut(&str, serde_json::Value) -> TestResult<serde_json::Value>,
) -> TestResult<(usize, Vec<SpanTuple>)> {
    let mut collected = Vec::new();
    let mut seen_cursors = BTreeSet::new();
    let mut after: Option<String> = None;
    let mut pages = 0_usize;

    loop {
        if pages > 16 {
            return Err(format!("{surface_label} failed to make forward progress").into());
        }
        let mut args = serde_json::Map::new();
        args.insert(
            "trace_id".to_owned(),
            serde_json::Value::String(trace_id.to_owned()),
        );
        args.insert("limit".to_owned(), serde_json::Value::from(limit));
        if let Some(ref cursor) = after {
            args.insert(
                "after".to_owned(),
                serde_json::Value::String(cursor.clone()),
            );
        }
        let rpc_resp = call(mcp_tool, serde_json::Value::Object(args))?;
        let raw_bytes = serde_json::to_vec(&rpc_resp)?.len();
        if raw_bytes > 4_194_304 {
            return Err(
                format!("{surface_label} response exceeded 4 MiB: {raw_bytes} bytes").into(),
            );
        }
        if let Some(err) = rpc_resp.get("error") {
            return Err(format!("{surface_label} returned JSON-RPC error: {err}").into());
        }
        let call_result = rpc_resp
            .get("result")
            .ok_or_else(|| format!("{surface_label} missing JSON-RPC result"))?;
        if call_result
            .get("isError")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
        {
            let err_text = call_result
                .get("content")
                .and_then(serde_json::Value::as_array)
                .and_then(|arr| arr.first())
                .and_then(|item| item.get("text"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("<missing error text>");
            return Err(format!("{surface_label} returned MCP tool error: {err_text}").into());
        }
        pages += 1;

        let structured = call_result
            .get("structuredContent")
            .ok_or_else(|| format!("{surface_label} missing structuredContent"))?;
        let business = structured
            .get("result")
            .ok_or_else(|| format!("{surface_label} missing structuredContent.result"))?;
        if business.get("outcome").and_then(serde_json::Value::as_str) != Some("Found") {
            return Err(format!("{surface_label} unexpected outcome: {business}").into());
        }
        let header = business
            .get("header")
            .ok_or_else(|| format!("{surface_label} missing header"))?;
        if header.get("trace_name").and_then(serde_json::Value::as_str) != Some(expected_name) {
            return Err(format!("{surface_label} trace_name mismatch").into());
        }
        if header
            .get("status_summary")
            .and_then(serde_json::Value::as_str)
            != Some(expected_summary)
        {
            return Err(format!("{surface_label} status_summary mismatch").into());
        }
        let spans = business
            .get("spans")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| format!("{surface_label} missing spans array"))?;
        if spans.is_empty() {
            return Err(format!("{surface_label} emitted empty split page").into());
        }
        for rec in spans {
            let span_id = rec
                .get("span_id")
                .and_then(serde_json::Value::as_str)
                .ok_or("MCP span missing span_id")?
                .to_owned();
            let seq_no = rec
                .get("seq_no")
                .ok_or("MCP span missing seq_no")
                .and_then(|v| {
                    v.as_i64()
                        .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
                        .ok_or("MCP span seq_no not i64")
                })?;
            let text = rec
                .get("payload_text")
                .and_then(serde_json::Value::as_str)
                .ok_or("MCP span missing payload_text")?
                .to_owned();
            let blob_b64 = rec
                .get("payload_blob")
                .and_then(serde_json::Value::as_str)
                .ok_or("MCP span missing payload_blob")?;
            let blob = BASE64.decode(blob_b64)?;
            collected.push((span_id, seq_no, text, blob));
        }

        let next_cursor = structured
            .get("page")
            .and_then(|p| p.get("next_cursor"))
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
        match next_cursor {
            Some(next) => {
                if !seen_cursors.insert(next.clone()) {
                    return Err(format!("{surface_label} repeated continuation cursor").into());
                }
                after = Some(next);
            }
            None => break,
        }
    }

    Ok((pages, collected))
}
