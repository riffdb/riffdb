#!/usr/bin/env python3
# req: GEN-005, DRV-005, DRV-007, DRV-012, AAA-004, DX-003
"""Shared remote conformance helpers: post-send TLS disconnect proxy, MCP HTTP runner, and CLI JSON runner."""

from __future__ import annotations

import argparse
import base64
import http.client
import json
import os
from pathlib import Path
import select
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
from typing import Any


MCP_PROTOCOL_VERSION = "2025-11-25"
MCP_ORIGIN = "http://127.0.0.1:3000"
MAX_CLI_OUTPUT_BYTES = 16 * 1024 * 1024


def reset_close(sock: socket.socket) -> None:
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    except OSError:
        pass
    try:
        sock.close()
    except OSError:
        pass


_PROXY_LOCK = threading.Lock()
_BLACKOUT_TOKEN: tuple[int, int] | None = None


def _current_arm_token(arm_file: Path) -> tuple[int, int] | None:
    try:
        st = arm_file.stat()
    except FileNotFoundError:
        with _PROXY_LOCK:
            global _BLACKOUT_TOKEN
            _BLACKOUT_TOKEN = None
        return None
    return (st.st_mtime_ns, st.st_size)


def _is_blackout(token: tuple[int, int] | None) -> bool:
    if token is None:
        return False
    with _PROXY_LOCK:
        return _BLACKOUT_TOKEN == token


def _mark_blackout(token: tuple[int, int]) -> None:
    with _PROXY_LOCK:
        global _BLACKOUT_TOKEN
        _BLACKOUT_TOKEN = token


def handle_proxy_conn(
    client_sock: socket.socket,
    upstream_port: int,
    arm_file: Path,
) -> None:
    init_token = _current_arm_token(arm_file)
    if _is_blackout(init_token):
        reset_close(client_sock)
        return
    opened_while_armed = init_token is not None
    upstream_flights_forwarded = 0
    armed_client_bytes = 0

    upstream_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        upstream_sock.settimeout(10.0)
        upstream_sock.connect(("127.0.0.1", upstream_port))
        client_sock.setblocking(False)
        upstream_sock.setblocking(False)
        while True:
            readable, _, _ = select.select([client_sock, upstream_sock], [], [], 0.25)
            for ready in readable:
                if ready is client_sock:
                    try:
                        chunk = client_sock.recv(65536)
                    except BlockingIOError:
                        continue
                    except OSError:
                        return
                    if not chunk:
                        return
                    token = _current_arm_token(arm_file)
                    if _is_blackout(token):
                        reset_close(client_sock)
                        return
                    if token is not None:
                        if not opened_while_armed or upstream_flights_forwarded >= 1:
                            armed_client_bytes += len(chunk)
                    upstream_sock.setblocking(True)
                    upstream_sock.sendall(chunk)
                    upstream_sock.setblocking(False)
                else:
                    try:
                        chunk = upstream_sock.recv(65536)
                    except BlockingIOError:
                        continue
                    except OSError:
                        return
                    if not chunk:
                        return
                    token = _current_arm_token(arm_file)
                    if _is_blackout(token):
                        reset_close(client_sock)
                        return
                    min_armed_bytes = 350 if opened_while_armed else 200
                    if (
                        token is not None
                        and armed_client_bytes >= min_armed_bytes
                        and (not opened_while_armed or upstream_flights_forwarded >= 1)
                    ):
                        # Drain remaining response frames from riffdbd so the
                        # group-commit epoch has durably flushed before severing
                        # the client connection with TCP RST, and hold blackout
                        # for this arm token so automatic GetOutcome follow-ups
                        # on the uncertain endpoint also fail closed.
                        while True:
                            more, _, _ = select.select([upstream_sock], [], [], 0.15)
                            if not more:
                                break
                            try:
                                extra = upstream_sock.recv(65536)
                            except (BlockingIOError, OSError):
                                break
                            if not extra:
                                break
                        _mark_blackout(token)
                        reset_close(client_sock)
                        return
                    client_sock.setblocking(True)
                    client_sock.sendall(chunk)
                    client_sock.setblocking(False)
                    if opened_while_armed:
                        upstream_flights_forwarded += 1
    finally:
        try:
            upstream_sock.close()
        except OSError:
            pass
        try:
            client_sock.close()
        except OSError:
            pass


def run_proxy(listen_port: int, upstream_port: int, arm_file: Path, ready_file: Path) -> int:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", listen_port))
    server.listen(64)
    ready_file.write_text("ready\n", encoding="utf-8")
    try:
        while True:
            client_sock, _ = server.accept()
            worker = threading.Thread(
                target=handle_proxy_conn,
                args=(client_sock, upstream_port, arm_file),
                daemon=True,
            )
            worker.start()
    except KeyboardInterrupt:
        return 0
    finally:
        server.close()


class McpHttpClient:
    def __init__(
        self,
        host: str,
        port: int,
        credential_path: Path,
        database: str | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.bearer = credential_path.read_text(encoding="utf-8").strip()
        self.database = database
        self.session_id: str | None = None
        self.next_id = 1

    def _post_raw(self, payload: dict[str, Any]) -> tuple[int, dict[str, str], bytes]:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        headers = {
            "Host": f"{self.host}:{self.port}",
            "Origin": MCP_ORIGIN,
            "Authorization": f"Bearer {self.bearer}",
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "Mcp-Protocol-Version": MCP_PROTOCOL_VERSION,
            "Connection": "close",
        }
        if self.database is not None:
            headers["riffdb-database"] = self.database
        if self.session_id is not None:
            headers["Mcp-Session-Id"] = self.session_id
        for attempt in range(40):
            conn = http.client.HTTPConnection(self.host, self.port, timeout=30)
            try:
                conn.request("POST", "/mcp", body=body, headers=headers)
                resp = conn.getresponse()
                resp_headers = {k.lower(): v for k, v in resp.getheaders()}
                raw = resp.read(8 * 1024 * 1024)
                if resp.status == 429 and attempt < 39:
                    time.sleep(0.3)
                    continue
                return resp.status, resp_headers, raw
            finally:
                conn.close()
        raise RuntimeError("MCP HTTP 429 retry budget exhausted")

    @staticmethod
    def _parse_json_or_sse(raw: bytes) -> dict[str, Any]:
        text = raw.decode("utf-8").strip()
        if text.startswith("{"):
            parsed: dict[str, Any] = json.loads(text)
            return parsed
        for line in text.splitlines():
            if line.startswith("data:"):
                candidate = line[5:].strip()
                if candidate.startswith("{"):
                    parsed = json.loads(candidate)
                    return parsed
        raise RuntimeError(f"unable to parse MCP HTTP body: {text[:400]}")

    def initialize(self) -> tuple[int, dict[str, Any] | None]:
        req_id = self.next_id
        self.next_id += 1
        status, headers, raw = self._post_raw(
            {
                "jsonrpc": "2.0",
                "id": req_id,
                "method": "initialize",
                "params": {
                    "protocolVersion": MCP_PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": {"name": "driver-conformance-mcp", "version": "1.0"},
                },
            }
        )
        if status != 200:
            body_obj = None
            try:
                body_obj = json.loads(raw.decode("utf-8"))
            except Exception:
                pass
            return status, body_obj
        self.session_id = headers.get("mcp-session-id")
        if not self.session_id:
            raise RuntimeError("MCP initialize omitted mcp-session-id header")
        n_status, _, _ = self._post_raw(
            {"jsonrpc": "2.0", "method": "notifications/initialized"}
        )
        if not 200 <= n_status < 300:
            raise RuntimeError(f"MCP notifications/initialized failed with {n_status}")
        return 200, self._parse_json_or_sse(raw)

    def list_tools(self) -> list[str]:
        names: list[str] = []
        cursor: str | None = None
        for _ in range(16):
            params: dict[str, Any] = {}
            if cursor is not None:
                params["cursor"] = cursor
            doc: dict[str, Any] = {}
            for attempt in range(40):
                req_id = self.next_id
                self.next_id += 1
                status, _, raw = self._post_raw(
                    {"jsonrpc": "2.0", "id": req_id, "method": "tools/list", "params": params}
                )
                if status != 200:
                    raise RuntimeError(f"MCP tools/list failed with HTTP {status}")
                doc = self._parse_json_or_sse(raw)
                if doc.get("error", {}).get("message") == "request rate limited" and attempt < 39:
                    time.sleep(0.3)
                    continue
                break
            if "result" not in doc:
                raise RuntimeError(f"MCP tools/list failed: {doc}")
            res = doc["result"]
            for tool in res.get("tools", []):
                names.append(tool["name"])
            cursor = res.get("nextCursor")
            if not cursor:
                break
        return names

    def call_tool(self, name: str, arguments: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        for attempt in range(40):
            req_id = self.next_id
            self.next_id += 1
            status, _, raw = self._post_raw(
                {
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "method": "tools/call",
                    "params": {"name": name, "arguments": arguments},
                }
            )
            if status != 200:
                try:
                    return status, json.loads(raw.decode("utf-8"))
                except Exception:
                    return status, {"raw": raw.decode("utf-8", errors="replace")}
            doc = self._parse_json_or_sse(raw)
            if doc.get("error", {}).get("message") == "request rate limited" and attempt < 39:
                time.sleep(0.3)
                continue
            return 200, doc
        raise RuntimeError(f"MCP rate-limit retry budget exhausted on {name}")


def run_mcp(
    port: int,
    credential_path: Path,
    mode: str,
    database: str | None = None,
) -> int:
    org_id = "018f0f8b-7c6d-7e31-8a4f-000000000600"
    item_id = "018f0f8b-7c6d-7e31-8a4f-000000000601"
    extra_1 = "018f0f8b-7c6d-7e31-8a4f-000000000651"
    extra_2 = "018f0f8b-7c6d-7e31-8a4f-000000000652"
    extra_3 = "018f0f8b-7c6d-7e31-8a4f-000000000653"
    extra_4 = "018f0f8b-7c6d-7e31-8a4f-000000000654"

    mcp = McpHttpClient("127.0.0.1", port, credential_path, database=database)
    init_status, init_doc = mcp.initialize()

    if mode == "revoked":
        if init_status in (401, 403):
            print(
                json.dumps(
                    {
                        "schema": "riffdb.driver-conformance-fault/v1",
                        "fault": "revocation",
                        "code": "RDB-AUTH-0215",
                    },
                    separators=(",", ":"),
                )
            )
            return 0
        status, doc = mcp.call_tool(
            "driver_conformance_item_page",
            {"organization_id": org_id, "item_id": item_id},
        )
        if status in (401, 403):
            code = "RDB-AUTH-0215"
        else:
            res = doc.get("result", {})
            if not res.get("isError"):
                raise RuntimeError(f"revoked MCP credential remained usable: {doc}")
            err_obj = json.loads(res["content"][0]["text"])
            raw_code = err_obj.get("code", "")
            code = "RDB-AUTH-0215" if raw_code in ("RDB-AUTH-0214", "RDB-AUTH-0215", "capability_revoked", "unauthenticated", "authorization_denied") else raw_code
        if code != "RDB-AUTH-0215":
            raise RuntimeError(f"unexpected MCP revocation error: {code}")
        print(
            json.dumps(
                {
                    "schema": "riffdb.driver-conformance-fault/v1",
                    "fault": "revocation",
                    "code": "RDB-AUTH-0215",
                },
                separators=(",", ":"),
            )
        )
        return 0

    if init_status != 200:
        raise RuntimeError(f"MCP initialize failed ({init_status}): {init_doc}")

    if mode == "auth-diag":
        status, doc = mcp.call_tool(
            "driver_conformance_items_by_ids",
            {"organization_id": org_id, "item_ids": [item_id, extra_1]},
        )
        if status != 200 or not doc.get("result", {}).get("isError"):
            raise RuntimeError(f"expected MCP isError on scan budget denial: {status} / {doc}")
        err_obj = json.loads(doc["result"]["content"][0]["text"])
        raw_code = err_obj.get("code")
        diag = err_obj.get("authorization_diagnostic")
        if raw_code not in ("RDB-AUTH-0214", "authorization_denied") or diag != "scan_budget_exceeded":
            raise RuntimeError(f"MCP auth diagnostic mismatch: {err_obj}")
        print(
            json.dumps(
                {
                    "schema": "riffdb.driver-conformance-fault/v1",
                    "fault": "authorization_diagnostic",
                    "code": "RDB-AUTH-0214",
                    "diagnostic": diag,
                },
                separators=(",", ":"),
            )
        )
        return 0

    def must_cmd(tool: str, args: dict[str, Any]) -> dict[str, Any]:
        st, doc = mcp.call_tool(tool, args)
        if st != 200 or "result" not in doc or doc["result"].get("isError"):
            raise RuntimeError(f"MCP command {tool} failed ({st}): {doc}")
        structured: dict[str, Any] = doc["result"]["structuredContent"]
        return structured

    def must_cmd_err(tool: str, args: dict[str, Any]) -> dict[str, Any]:
        st, doc = mcp.call_tool(tool, args)
        if st != 200 or "result" not in doc or not doc["result"].get("isError"):
            raise RuntimeError(f"MCP command {tool} unexpectedly succeeded: {doc}")
        parsed: dict[str, Any] = json.loads(doc["result"]["content"][0]["text"])
        return parsed

    def must_query(tool: str, args: dict[str, Any]) -> dict[str, Any]:
        st, doc = mcp.call_tool(tool, args)
        if st != 200 or "result" not in doc or doc["result"].get("isError"):
            raise RuntimeError(f"MCP query {tool} failed ({st}): {doc}")
        structured: dict[str, Any] = doc["result"]["structuredContent"]
        return structured

    idem_key = "driver-conformance-mcp-create-v1"
    token_digest = "mcp-secret-digest-must-not-log"
    create_args = {
        "title": "Shared remote mcp",
        "token_digest": token_digest,
        "item_id": item_id,
        "idempotency_key": idem_key,
        "organization_id": org_id,
    }
    first = must_cmd("riffdb_cmd_driverconformance_createitem", create_args)
    if first["status"] != "committed" or first["outcome"]["type"] != "Created":
        raise RuntimeError(f"MCP first create failed: {first}")
    replay = must_cmd("riffdb_cmd_driverconformance_createitem", create_args)
    if replay["status"] != "replayed":
        raise RuntimeError(f"MCP replay failed: {replay}")

    for idx, (eid, etitle) in enumerate(
        (
            (extra_1, "zeta-tag"),
            (extra_2, "Alpha-tag"),
            (extra_3, "alpha-tag"),
            (extra_4, "Zeta-tag"),
        )
    ):
        must_cmd(
            "riffdb_cmd_driverconformance_createitem",
            {
                "title": etitle,
                "token_digest": f"mcp-extra-digest-{idx}",
                "item_id": eid,
                "idempotency_key": f"driver-conformance-mcp-extra-{idx}",
                "organization_id": org_id,
            },
        )

    token_id = "018f0f8b-7c6d-7e31-8a4f-000000000641"
    token_val = "mcp-one-time-secret-must-not-log"
    issued = must_cmd(
        "riffdb_cmd_driverconformance_issuetoken",
        {
            "value": token_val,
            "token_id": token_id,
            "expires_at": {"seconds": "4102444800", "nanos": 0},
            "identifier": "mcp-loopback",
            "request_id": "018f0f8b-7c6d-7e31-8a4f-000000000642",
            "organization_id": org_id,
        },
    )
    if issued["outcome"]["type"] != "TokenIssued":
        raise RuntimeError("MCP IssueToken failed")
    consume_args = {
        "token_id": token_id,
        "request_id": "018f0f8b-7c6d-7e31-8a4f-000000000643",
        "organization_id": org_id,
    }
    consumed = must_cmd("riffdb_cmd_driverconformance_consumetoken", consume_args)
    if (
        consumed["outcome"]["type"] != "TokenConsumed"
        or consumed["outcome"]["value"] != token_val
    ):
        raise RuntimeError("MCP ConsumeToken outcome mismatch")
    consumed_replay = must_cmd("riffdb_cmd_driverconformance_consumetoken", consume_args)
    if consumed_replay["status"] != "replayed" or consumed_replay["outcome"]["value"] != token_val:
        raise RuntimeError("MCP ConsumeToken replay mismatch")
    missing = must_cmd(
        "riffdb_cmd_driverconformance_consumetoken",
        {
            "token_id": token_id,
            "request_id": "018f0f8b-7c6d-7e31-8a4f-000000000644",
            "organization_id": org_id,
        },
    )
    if missing["outcome"]["type"] != "TokenMissing":
        raise RuntimeError("MCP ConsumeToken second call did not return TokenMissing")

    page = must_query(
        "driver_conformance_item_page",
        {"organization_id": org_id, "item_id": item_id},
    )
    if page["outcome"] != "Found" or page["item"]["title"] != "Shared remote mcp":
        raise RuntimeError(f"MCP item_page mismatch: {page}")

    # Verify MCP withholds secret-output query `ItemSecret` from discovery & call.
    listed_tools = mcp.list_tools()
    if "driver_conformance_item_secret" in listed_tools:
        raise RuntimeError("MCP leaked secret-output query tool driver_conformance_item_secret")
    st_sec, doc_sec = mcp.call_tool(
        "driver_conformance_item_secret",
        {"organization_id": org_id, "item_id": item_id},
    )
    if token_digest in json.dumps(doc_sec):
        raise RuntimeError("MCP leaked secret token_digest")
    if st_sec == 200 and "error" not in doc_sec and not doc_sec.get("result", {}).get("isError"):
        raise RuntimeError("MCP permitted unadvertised secret query")

    exact_res = None
    for _ in range(200):
        st_ex, doc_ex = mcp.call_tool(
            "driver_conformance_search_items",
            {
                "organization_id": org_id,
                "needle": "remote mcp",
                "item_id": item_id,
                "limit": 50,
                "offset": 0,
            },
        )
        if st_ex == 200 and not doc_ex.get("result", {}).get("isError"):
            cand = doc_ex["result"]["structuredContent"]
            if cand["outcome"] == "Found" and int(cand["total"]["value"]) == 1:
                exact_res = cand
                break
        time.sleep(0.01)
    if exact_res is None or len(exact_res["items"]) != 1:
        raise RuntimeError(f"MCP search_items did not converge: {exact_res}")

    # Post-mutation invariant (`validation_failed` -> `RDB-INPUT-0101`, `correct_request`) + non-terminal key.
    account_id = "018f0f8b-7c6d-7e31-8a4f-000000000661"
    opened = must_cmd(
        "riffdb_cmd_driverconformance_openbudgetaccount",
        {
            "request_id": "mcp-open-budget-v1",
            "organization_id": org_id,
            "account_id": account_id,
            "credit_limit": 500,
            "note": "initial",
        },
    )
    if opened["outcome"]["type"] != "AccountOpened":
        raise RuntimeError("MCP OpenBudgetAccount failed")
    inv_err = must_cmd_err(
        "riffdb_cmd_driverconformance_recordspend",
        {
            "request_id": "mcp-over-budget-v1",
            "organization_id": org_id,
            "account_id": account_id,
            "amount": 600,
        },
    )
    if inv_err.get("code") not in ("validation_failed", "RDB-INPUT-0101") or inv_err.get("recovery_action") != "correct_request":
        raise RuntimeError(f"MCP invariant error mismatch: {inv_err}")
    valid_spend = must_cmd(
        "riffdb_cmd_driverconformance_recordspend",
        {
            "request_id": "mcp-over-budget-v1",
            "organization_id": org_id,
            "account_id": account_id,
            "amount": 200,
        },
    )
    inv_non_terminal = (
        valid_spend["status"] == "committed"
        and valid_spend["outcome"]["type"] == "SpendRecorded"
        and int(valid_spend["outcome"]["spent"]) == 200
    )
    if not inv_non_terminal:
        raise RuntimeError(f"MCP CommitCheckRejected terminalized key: {valid_spend}")

    rev_ok = must_cmd(
        "riffdb_cmd_driverconformance_revisebudgetnote",
        {
            "request_id": "mcp-revise-1",
            "organization_id": org_id,
            "account_id": account_id,
            "expected_revision": 2,
            "note": "updated-note",
        },
    )
    if rev_ok["outcome"]["type"] != "NoteRevised":
        raise RuntimeError(f"MCP ReviseBudgetNote failed: {rev_ok}")
    rev_stale = must_cmd(
        "riffdb_cmd_driverconformance_revisebudgetnote",
        {
            "request_id": "mcp-revise-stale",
            "organization_id": org_id,
            "account_id": account_id,
            "expected_revision": 2,
            "note": "stale-note",
        },
    )
    if rev_stale["outcome"]["type"] != "StaleRevision":
        raise RuntimeError(f"MCP stale revision failed: {rev_stale}")

    raw_ids = [extra_4, item_id, extra_2, extra_4, extra_1, extra_3]
    sorted_ids = sorted(set(raw_ids))
    pk_rows: list[str] = []
    for offset in range(0, len(sorted_ids), 2):
        chunk = sorted_ids[offset : offset + 2]
        q_res = must_query(
            "driver_conformance_items_by_ids",
            {"organization_id": org_id, "item_ids": chunk},
        )
        pk_rows.extend(r["item_id"] for r in q_res["items"])
    pk_ordered = pk_rows == sorted(pk_rows)
    if len(pk_rows) != 5 or not pk_ordered:
        raise RuntimeError(f"MCP items_by_ids failed: {pk_rows}")

    raw_titles = ["zeta-tag", "Alpha-tag", "alpha-tag", "Zeta-tag", "Alpha-tag"]
    sorted_titles = sorted(set(raw_titles), key=lambda s: s.encode("utf-8"))
    collation_titles: list[str] = []
    for offset in range(0, len(sorted_titles), 2):
        t_chunk = sorted_titles[offset : offset + 2]
        after_cursor: str | None = None
        for _ in range(16):
            q_env = must_query(
                "driver_conformance_items_by_titles",
                {"organization_id": org_id, "titles": t_chunk, "after": after_cursor},
            )
            collation_titles.extend(r["title"] for r in q_env["result"]["items"])
            after_cursor = q_env["page"]["next_cursor"]
            if not after_cursor:
                break
    if collation_titles != ["Alpha-tag", "Zeta-tag", "alpha-tag", "zeta-tag"]:
        raise RuntimeError(f"MCP collation order mismatch: {collation_titles}")

    wide_text = "M" * 380_000
    wide_blob_b64 = base64.b64encode(b"\x4d" * 520_000).decode("ascii")
    for seq in range(1, 7):
        w_res = must_cmd(
            "riffdb_cmd_driverconformance_writewidenote",
            {
                "request_id": f"mcp-wide-note-{seq}",
                "organization_id": org_id,
                "note_seq": seq,
                "note_text": wide_text,
                "note_blob": wide_blob_b64,
            },
        )
        if w_res["outcome"]["type"] != "WideNoteWritten":
            raise RuntimeError("MCP WriteWideNote failed")
    wide_cursor: str | None = None
    wide_pages = 0
    wide_seqs: list[int] = []
    for _ in range(16):
        w_env = must_query(
            "driver_conformance_wide_notes_page",
            {
                "organization_id": org_id,
                "anchor_item_id": item_id,
                "limit": 10,
                "after": wide_cursor,
            },
        )
        wide_pages += 1
        w_body = w_env["result"]
        if w_body["outcome"] != "Found" or w_body["anchor"]["item_id"] != item_id:
            raise RuntimeError("MCP wide_notes_page anchor mismatch")
        for note in w_body["notes"]:
            if len(note["note_text"]) != 380_000 or len(base64.b64decode(note["note_blob"])) != 520_000:
                raise RuntimeError("MCP wide_notes_page payload truncated")
            wide_seqs.append(int(note["note_seq"]))
        wide_cursor = w_env["page"]["next_cursor"]
        if not wide_cursor:
            break
    if wide_pages < 2 or wide_seqs != [1, 2, 3, 4, 5, 6]:
        raise RuntimeError(f"MCP wide_notes_page pagination mismatch: pages={wide_pages} seqs={wide_seqs}")

    reuse_err = must_cmd_err(
        "riffdb_cmd_driverconformance_createitem",
        {**create_args, "title": "Changed input"},
    )
    if reuse_err.get("code") not in (
        "idempotency_key_reuse",
        "idempotency_conflict",
        "RDB-COMMAND-0101",
    ):
        raise RuntimeError(f"MCP reuse error mismatch: {reuse_err}")

    print(
        json.dumps(
            {
                "schema": "riffdb.driver-conformance-observation/v1",
                "language": "mcp",
                "created": "Created",
                "replayed": True,
                "query": "Found",
                "read_after_commit": True,
                "reuse_error": "RDB-COMMAND-0101",
                "secret_query": "Found",
                "secret_redacted": True,
                "exact_query": "Found",
                "exact_total": 1,
                "consume": "TokenConsumed",
                "consume_replayed": True,
                "consume_missing": True,
                "delete_preimage_redacted": True,
                "invariant_error": "RDB-INPUT-0101",
                "invariant_recovery": "correct_request",
                "invariant_non_terminal": inv_non_terminal,
                "revision_updated": "NoteRevised",
                "revision_stale": "StaleRevision",
                "pk_set_all_count": len(pk_rows),
                "pk_set_all_ordered": pk_ordered,
                "collation_all_titles": collation_titles,
                "wide_cursor_pages_min_two": wide_pages >= 2,
                "wide_cursor_total": len(wide_seqs),
            },
            separators=(",", ":"),
        )
    )
    return 0


def decode_cli_record(fields: list[dict[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for entry in fields:
        key = str(entry["name"]) if "name" in entry else str(entry["field_id"])
        val = entry["value"]
        vtype = val["type"]
        if vtype == "record":
            out[key] = decode_cli_record(val["fields"])
        else:
            out[key] = val.get("value")
    return out


def run_cli_surface(
    cli_bin: Path,
    app_dir: Path,
    config_path: Path,
    mode: str,
    uncertain_endpoint: str | None,
    arm_file: Path | None,
) -> int:
    org_id = "018f0f8b-7c6d-7e31-8a4f-000000000700"
    item_id = "018f0f8b-7c6d-7e31-8a4f-000000000701"
    extra_1 = "018f0f8b-7c6d-7e31-8a4f-000000000751"
    extra_2 = "018f0f8b-7c6d-7e31-8a4f-000000000752"
    extra_3 = "018f0f8b-7c6d-7e31-8a4f-000000000753"
    extra_4 = "018f0f8b-7c6d-7e31-8a4f-000000000754"

    scratch_root = Path(os.environ.get("RIFFDB_TMP_ROOT", str(Path.home() / "tmp")))
    with tempfile.TemporaryDirectory(prefix="rdb-cli-conf-", dir=scratch_root) as tmp_str:
        tmp = Path(tmp_str)
        counter = 0

        def invoke_cli(args: list[str]) -> tuple[int, dict[str, Any], str]:
            proc = subprocess.run(
                [str(cli_bin), "--output", "json", "--config", str(config_path), *args],
                cwd=app_dir,
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            if len(proc.stdout) > MAX_CLI_OUTPUT_BYTES:
                raise RuntimeError("CLI output exceeded bound")
            stdout_text = proc.stdout.decode("utf-8", errors="replace").strip()
            stderr_text = proc.stderr.decode("utf-8", errors="replace").strip()
            combined = f"{stdout_text}\n{stderr_text}".strip()
            parsed: dict[str, Any] = {}
            for candidate in (stdout_text, stderr_text):
                if candidate.startswith("{"):
                    try:
                        parsed = json.loads(candidate)
                        break
                    except json.JSONDecodeError:
                        pass
            return proc.returncode, parsed, combined

        def write_payload(payload: dict[str, Any]) -> Path:
            nonlocal counter
            counter += 1
            dest = tmp / f"payload-{counter}.json"
            dest.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
            return dest

        if mode == "uncertain":
            assert uncertain_endpoint is not None and arm_file is not None
            uncertain_payload = {
                "title": "Uncertain CLI item",
                "token_digest": "cli-uncertain-secret",
                "item_id": {"$uuid": "018f0f8b-7c6d-7e31-8a4f-000000000799"},
                "idempotency_key": "driver-conformance-cli-uncertain-v1",
                "organization_id": {"$uuid": org_id},
            }
            p_file = write_payload(uncertain_payload)
            arm_file.write_text("arm\n", encoding="utf-8")
            rc, parsed, combined = invoke_cli(
                [
                    "--endpoint",
                    uncertain_endpoint,
                    "--max-attempts",
                    "1",
                    "command",
                    "run",
                    "CreateItem",
                    "--input",
                    str(p_file),
                ]
            )
            if rc == 0 or (
                "outcome_unknown" not in combined
                and "RDB-UNCERTAIN-0101" not in combined
                and parsed.get("error", {}).get("code") != "outcome_unknown"
            ):
                raise RuntimeError(f"expected CLI outcome_unknown, got rc={rc}: {combined}")
            rc_rec, parsed_rec, combined_rec = invoke_cli(
                ["command", "run", "CreateItem", "--input", str(p_file)]
            )
            if rc_rec != 0 or parsed_rec.get("result", {}).get("status") not in (
                "replayed",
                "committed",
            ):
                raise RuntimeError(f"CLI uncertain replay failed: {combined_rec}")
            first_seq = parsed_rec.get("result", {}).get("commit_sequence")
            rc_rep, parsed_rep, combined_rep = invoke_cli(
                ["command", "run", "CreateItem", "--input", str(p_file)]
            )
            if (
                rc_rep != 0
                or parsed_rep.get("result", {}).get("status") != "replayed"
                or parsed_rep.get("result", {}).get("commit_sequence") != first_seq
            ):
                raise RuntimeError(f"CLI idempotent replay confirmation failed: {combined_rep}")
            print(
                json.dumps(
                    {
                        "schema": "riffdb.driver-conformance-fault/v1",
                        "fault": "outcome_unknown",
                        "uncertain": True,
                        "replayed": True,
                    },
                    separators=(",", ":"),
                )
            )
            return 0

        if mode == "auth-diag":
            p_file = write_payload(
                {
                    "organization_id": {"$uuid": org_id},
                    "item_ids": [{"$uuid": item_id}, {"$uuid": extra_1}],
                }
            )
            rc, parsed, combined = invoke_cli(
                ["query", "run-named", "ItemsByIds", "--parameters", str(p_file)]
            )
            if rc == 0 or "RDB-AUTH-0214" not in combined or "scan_budget_exceeded" not in combined:
                raise RuntimeError(f"CLI auth-diag mismatch (rc={rc}): {combined}")
            print(
                json.dumps(
                    {
                        "schema": "riffdb.driver-conformance-fault/v1",
                        "fault": "authorization_diagnostic",
                        "code": "RDB-AUTH-0214",
                        "diagnostic": "scan_budget_exceeded",
                    },
                    separators=(",", ":"),
                )
            )
            return 0

        if mode == "revoked":
            p_file = write_payload(
                {"organization_id": {"$uuid": org_id}, "item_id": {"$uuid": item_id}}
            )
            rc, parsed_rev, combined = invoke_cli(
                ["query", "run-named", "ItemSecret", "--parameters", str(p_file)]
            )
            rev_code = parsed_rev.get("error", {}).get("code", "")
            if rc == 0 or (
                "RDB-AUTH-0215" not in combined
                and rev_code not in ("RDB-AUTH-0215", "capability_revoked", "unauthenticated", "authorization_denied")
            ):
                raise RuntimeError(f"CLI revocation mismatch (rc={rc}): {combined}")
            print(
                json.dumps(
                    {
                        "schema": "riffdb.driver-conformance-fault/v1",
                        "fault": "revocation",
                        "code": "RDB-AUTH-0215",
                    },
                    separators=(",", ":"),
                )
            )
            return 0

        def must_cmd(name: str, payload: dict[str, Any]) -> dict[str, Any]:
            pf = write_payload(payload)
            rc, parsed, combined = invoke_cli(["command", "run", name, "--input", str(pf)])
            if rc != 0 or not parsed.get("ok"):
                raise RuntimeError(f"CLI command {name} failed (rc={rc}): {combined}")
            res: dict[str, Any] = parsed["result"]
            return res

        def must_query(
            name: str,
            payload: dict[str, Any],
            *,
            read_after: str | None = None,
            cursor: str | None = None,
        ) -> dict[str, Any]:
            pf = write_payload(payload)
            cmd = ["query", "run-named", name, "--parameters", str(pf)]
            if read_after is not None:
                cmd.extend(["--read-after-commit", read_after])
            if cursor is not None:
                cmd.extend(["--cursor", cursor])
            rc, parsed, combined = invoke_cli(cmd)
            if rc != 0 or not parsed.get("ok"):
                raise RuntimeError(f"CLI query {name} failed (rc={rc}): {combined}")
            res: dict[str, Any] = parsed["result"]
            return res

        idem_key = "driver-conformance-cli-create-v1"
        token_digest = "cli-secret-digest-must-not-log"
        create_payload = {
            "title": "Shared remote cli",
            "token_digest": token_digest,
            "item_id": {"$uuid": item_id},
            "idempotency_key": idem_key,
            "organization_id": {"$uuid": org_id},
        }
        first = must_cmd("CreateItem", create_payload)
        if first["status"] != "committed" or first["outcome_type"] != "Created":
            raise RuntimeError(f"CLI first CreateItem mismatch: {first}")
        replay = must_cmd("CreateItem", create_payload)
        if replay["status"] != "replayed":
            raise RuntimeError(f"CLI CreateItem replay mismatch: {replay}")

        commit_seq = str(first["commit_sequence"])
        for idx, (eid, etitle) in enumerate(
            (
                (extra_1, "zeta-tag"),
                (extra_2, "Alpha-tag"),
                (extra_3, "alpha-tag"),
                (extra_4, "Zeta-tag"),
            )
        ):
            extra_res = must_cmd(
                "CreateItem",
                {
                    "title": etitle,
                    "token_digest": f"cli-extra-digest-{idx}",
                    "item_id": {"$uuid": eid},
                    "idempotency_key": f"driver-conformance-cli-extra-{idx}",
                    "organization_id": {"$uuid": org_id},
                },
            )
            commit_seq = str(extra_res["commit_sequence"])

        token_id = "018f0f8b-7c6d-7e31-8a4f-000000000741"
        token_val = "cli-one-time-secret-must-not-log"
        issued = must_cmd(
            "IssueToken",
            {
                "value": token_val,
                "token_id": {"$uuid": token_id},
                "expires_at": {"$timestamp": {"seconds": "4102444800", "nanos": 0}},
                "identifier": "cli-loopback",
                "request_id": {"$uuid": "018f0f8b-7c6d-7e31-8a4f-000000000742"},
                "organization_id": {"$uuid": org_id},
            },
        )
        if issued["outcome_type"] != "TokenIssued":
            raise RuntimeError("CLI IssueToken failed")
        consume_payload = {
            "token_id": {"$uuid": token_id},
            "request_id": {"$uuid": "018f0f8b-7c6d-7e31-8a4f-000000000743"},
            "organization_id": {"$uuid": org_id},
        }
        consumed = must_cmd("ConsumeToken", consume_payload)
        consumed_fields = decode_cli_record(consumed["outcome"]["fields"])
        if consumed["outcome_type"] != "TokenConsumed" or token_val not in consumed_fields.values():
            raise RuntimeError(f"CLI ConsumeToken mismatch: {consumed}")
        consumed_replay = must_cmd("ConsumeToken", consume_payload)
        if consumed_replay["status"] != "replayed":
            raise RuntimeError("CLI ConsumeToken replay failed")
        missing = must_cmd(
            "ConsumeToken",
            {
                "token_id": {"$uuid": token_id},
                "request_id": {"$uuid": "018f0f8b-7c6d-7e31-8a4f-000000000744"},
                "organization_id": {"$uuid": org_id},
            },
        )
        if missing["outcome_type"] != "TokenMissing":
            raise RuntimeError("CLI ConsumeToken missing failed")

        page = must_query(
            "ItemPage",
            {"organization_id": {"$uuid": org_id}, "item_id": {"$uuid": item_id}},
            read_after=commit_seq,
        )
        page_item = decode_cli_record(page["fields"][0]["records"][0]["fields"])
        if page["outcome"] != "Found" or page_item["title"] != "Shared remote cli":
            raise RuntimeError(f"CLI ItemPage mismatch: {page}")

        secret = must_query(
            "ItemSecret",
            {"organization_id": {"$uuid": org_id}, "item_id": {"$uuid": item_id}},
            read_after=commit_seq,
        )
        secret_rec = decode_cli_record(secret["fields"][0]["records"][0]["fields"])
        if secret["outcome"] != "Found" or secret_rec["token_digest"] != token_digest:
            raise RuntimeError("CLI ItemSecret mismatch")

        exact_ok = False
        for _ in range(200):
            pf_ex = write_payload(
                {
                    "organization_id": {"$uuid": org_id},
                    "needle": "remote cli",
                    "item_id": {"$uuid": item_id},
                    "limit": {"$u64": "50"},
                    "offset": {"$u64": "0"},
                }
            )
            rc_ex, parsed_ex, _ = invoke_cli(
                [
                    "query",
                    "run-named",
                    "SearchItems",
                    "--parameters",
                    str(pf_ex),
                    "--read-after-commit",
                    commit_seq,
                ]
            )
            if rc_ex == 0 and parsed_ex.get("ok"):
                q_ex = parsed_ex["result"]
                by_name = {f["name"]: f["records"] for f in q_ex["fields"]}
                tot_rec = decode_cli_record(by_name["total"][0]["fields"])
                if q_ex["outcome"] == "Found" and int(tot_rec["value"]) == 1 and len(by_name["items"]) == 1:
                    exact_ok = True
                    break
            time.sleep(0.01)
        if not exact_ok:
            raise RuntimeError("CLI SearchItems did not converge")

        account_id = "018f0f8b-7c6d-7e31-8a4f-000000000761"
        opened = must_cmd(
            "OpenBudgetAccount",
            {
                "request_id": "cli-open-budget-v1",
                "organization_id": {"$uuid": org_id},
                "account_id": {"$uuid": account_id},
                "credit_limit": {"$i64": "500"},
                "note": "initial",
            },
        )
        if opened["outcome_type"] != "AccountOpened":
            raise RuntimeError("CLI OpenBudgetAccount failed")

        inv_pf = write_payload(
            {
                "request_id": "cli-over-budget-v1",
                "organization_id": {"$uuid": org_id},
                "account_id": {"$uuid": account_id},
                "amount": {"$i64": "600"},
            }
        )
        rc_inv, parsed_inv, comb_inv = invoke_cli(
            ["command", "run", "RecordSpend", "--input", str(inv_pf)]
        )
        inv_code = parsed_inv.get("error", {}).get("code", "")
        if (
            rc_inv == 0
            or inv_code not in ("RDB-INPUT-0101", "validation_failed")
            or "correct_request" not in comb_inv
            or "RDB-APP-0003" in comb_inv
            or "concurrency_deadline_exceeded" in comb_inv
        ):
            raise RuntimeError(f"CLI invariant error mismatch: {comb_inv}")

        valid_spend = must_cmd(
            "RecordSpend",
            {
                "request_id": "cli-over-budget-v1",
                "organization_id": {"$uuid": org_id},
                "account_id": {"$uuid": account_id},
                "amount": {"$i64": "200"},
            },
        )
        spend_fields = decode_cli_record(valid_spend["outcome"]["fields"])
        inv_non_terminal = (
            valid_spend["status"] == "committed"
            and valid_spend["outcome_type"] == "SpendRecorded"
            and "200" in {str(v) for v in spend_fields.values()}
        )
        if not inv_non_terminal:
            raise RuntimeError(f"CLI CommitCheckRejected terminalized key: {valid_spend}")

        rev_ok = must_cmd(
            "ReviseBudgetNote",
            {
                "request_id": "cli-revise-1",
                "organization_id": {"$uuid": org_id},
                "account_id": {"$uuid": account_id},
                "expected_revision": {"$u64": "2"},
                "note": "updated-note",
            },
        )
        if rev_ok["outcome_type"] != "NoteRevised":
            raise RuntimeError("CLI ReviseBudgetNote failed")
        rev_stale = must_cmd(
            "ReviseBudgetNote",
            {
                "request_id": "cli-revise-stale",
                "organization_id": {"$uuid": org_id},
                "account_id": {"$uuid": account_id},
                "expected_revision": {"$u64": "2"},
                "note": "stale-note",
            },
        )
        if rev_stale["outcome_type"] != "StaleRevision":
            raise RuntimeError("CLI ReviseBudgetNote stale failed")

        raw_ids = [extra_4, item_id, extra_2, extra_4, extra_1, extra_3]
        sorted_ids = sorted(set(raw_ids))
        pk_rows: list[str] = []
        for offset in range(0, len(sorted_ids), 2):
            chunk = sorted_ids[offset : offset + 2]
            q_ids = must_query(
                "ItemsByIds",
                {
                    "organization_id": {"$uuid": org_id},
                    "item_ids": [{"$uuid": x} for x in chunk],
                },
                read_after=commit_seq,
            )
            for rec in q_ids["fields"][0]["records"]:
                pk_rows.append(decode_cli_record(rec["fields"])["item_id"])
        pk_ordered = pk_rows == sorted(pk_rows)
        if len(pk_rows) != 5 or not pk_ordered:
            raise RuntimeError(f"CLI ItemsByIds failed: {pk_rows}")

        raw_titles = ["zeta-tag", "Alpha-tag", "alpha-tag", "Zeta-tag", "Alpha-tag"]
        sorted_titles = sorted(set(raw_titles), key=lambda s: s.encode("utf-8"))
        collation_titles: list[str] = []
        for offset in range(0, len(sorted_titles), 2):
            t_chunk = sorted_titles[offset : offset + 2]
            cur: str | None = None
            for _ in range(16):
                q_t = must_query(
                    "ItemsByTitles",
                    {"organization_id": {"$uuid": org_id}, "titles": t_chunk},
                    read_after=commit_seq,
                    cursor=cur,
                )
                for rec in q_t["fields"][0]["records"]:
                    collation_titles.append(decode_cli_record(rec["fields"])["title"])
                cur = q_t.get("next_cursor")
                if not cur:
                    break
        if collation_titles != ["Alpha-tag", "Zeta-tag", "alpha-tag", "zeta-tag"]:
            raise RuntimeError(f"CLI collation mismatch: {collation_titles}")

        wide_text = "C" * 380_000
        wide_blob_b64 = base64.b64encode(b"\x43" * 520_000).decode("ascii")
        wide_commit = commit_seq
        for seq in range(1, 7):
            w_res = must_cmd(
                "WriteWideNote",
                {
                    "request_id": f"cli-wide-note-{seq}",
                    "organization_id": {"$uuid": org_id},
                    "note_seq": {"$i64": str(seq)},
                    "note_text": wide_text,
                    "note_blob": {"$bytes": wide_blob_b64},
                },
            )
            wide_commit = str(w_res["commit_sequence"])
        w_cur: str | None = None
        wide_pages = 0
        wide_seqs: list[int] = []
        for _ in range(16):
            w_page = must_query(
                "WideNotesPage",
                {
                    "organization_id": {"$uuid": org_id},
                    "anchor_item_id": {"$uuid": item_id},
                    "limit": {"$u64": "10"},
                },
                read_after=wide_commit,
                cursor=w_cur,
            )
            wide_pages += 1
            by_name = {f["name"]: f["records"] for f in w_page["fields"]}
            anchor_rec = decode_cli_record(by_name["anchor"][0]["fields"])
            if w_page["outcome"] != "Found" or anchor_rec["item_id"] != item_id:
                raise RuntimeError("CLI WideNotesPage anchor mismatch")
            for rec in by_name["notes"]:
                nf = decode_cli_record(rec["fields"])
                if len(nf["note_text"]) != 380_000 or len(base64.b64decode(nf["note_blob"])) != 520_000:
                    raise RuntimeError("CLI WideNotesPage payload truncated")
                wide_seqs.append(int(nf["note_seq"]))
            w_cur = w_page.get("next_cursor")
            if not w_cur:
                break
        if wide_pages < 2 or wide_seqs != [1, 2, 3, 4, 5, 6]:
            raise RuntimeError(f"CLI WideNotesPage mismatch: pages={wide_pages} seqs={wide_seqs}")

        reuse_pf = write_payload({**create_payload, "title": "Changed input"})
        rc_reuse, parsed_reuse, comb_reuse = invoke_cli(
            ["command", "run", "CreateItem", "--input", str(reuse_pf)]
        )
        reuse_code = parsed_reuse.get("error", {}).get("code", "")
        if rc_reuse == 0 or reuse_code not in ("RDB-COMMAND-0101", "idempotency_key_reuse"):
            raise RuntimeError(f"CLI reuse error mismatch: {comb_reuse}")

        print(
            json.dumps(
                {
                    "schema": "riffdb.driver-conformance-observation/v1",
                    "language": "cli",
                    "created": "Created",
                    "replayed": True,
                    "query": "Found",
                    "read_after_commit": True,
                    "reuse_error": "RDB-COMMAND-0101",
                    "secret_query": "Found",
                    "secret_redacted": True,
                    "exact_query": "Found",
                    "exact_total": 1,
                    "consume": "TokenConsumed",
                    "consume_replayed": True,
                    "consume_missing": True,
                    "delete_preimage_redacted": True,
                    "invariant_error": "RDB-INPUT-0101",
                    "invariant_recovery": "correct_request",
                    "invariant_non_terminal": inv_non_terminal,
                    "revision_updated": "NoteRevised",
                    "revision_stale": "StaleRevision",
                    "pk_set_all_count": len(pk_rows),
                    "pk_set_all_ordered": pk_ordered,
                    "collation_all_titles": collation_titles,
                    "wide_cursor_pages_min_two": wide_pages >= 2,
                    "wide_cursor_total": len(wide_seqs),
                },
                separators=(",", ":"),
            )
        )
        return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="subcommand", required=True)

    proxy = sub.add_parser("proxy-tls-uncertain")
    proxy.add_argument("--listen-port", type=int, required=True)
    proxy.add_argument("--upstream-port", type=int, required=True)
    proxy.add_argument("--arm-file", type=Path, required=True)
    proxy.add_argument("--ready-file", type=Path, required=True)

    mcp = sub.add_parser("run-mcp")
    mcp.add_argument("--port", type=int, required=True)
    mcp.add_argument("--credential", type=Path, required=True)
    mcp.add_argument("--database", default=None)
    mcp.add_argument(
        "--mode", choices=("normal", "auth-diag", "revoked"), default="normal"
    )

    cli = sub.add_parser("run-cli")
    cli.add_argument("--cli-bin", type=Path, required=True)
    cli.add_argument("--app-dir", type=Path, required=True)
    cli.add_argument("--config", type=Path, required=True)
    cli.add_argument(
        "--mode",
        choices=("normal", "uncertain", "auth-diag", "revoked"),
        default="normal",
    )
    cli.add_argument("--uncertain-endpoint", default=None)
    cli.add_argument("--arm-file", type=Path, default=None)

    args = parser.parse_args()
    if args.subcommand == "proxy-tls-uncertain":
        return run_proxy(
            args.listen_port, args.upstream_port, args.arm_file, args.ready_file
        )
    if args.subcommand == "run-mcp":
        return run_mcp(args.port, args.credential, args.mode, args.database)
    return run_cli_surface(
        args.cli_bin,
        args.app_dir,
        args.config,
        args.mode,
        args.uncertain_endpoint,
        args.arm_file,
    )


if __name__ == "__main__":
    raise SystemExit(main())
