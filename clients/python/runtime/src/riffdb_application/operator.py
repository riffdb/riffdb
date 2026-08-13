from __future__ import annotations

import json
import re
import socket
import struct
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Final

OPERATOR_DRIVER_PROTOCOL_VERSION: Final = 1
_MAX_FRAME_BYTES: Final = 4 * 1_024 * 1_024 + 256 * 1_024
_HASH = re.compile(r"^[0-9a-f]{64}$")
_UUID7 = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")


@dataclass(frozen=True, slots=True)
class OperatorIdentity:
    database: str
    campaign_id: str
    portability_manifest_hash: str

    def __post_init__(self) -> None:
        if (
            not _short(self.database)
            or _UUID7.fullmatch(self.campaign_id) is None
            or _HASH.fullmatch(self.portability_manifest_hash) is None
        ):
            raise ValueError("invalid RiffDB operator identity")


@dataclass(frozen=True, slots=True)
class ReimportPage:
    export_operation_id: str
    page_number: int
    canonical_json_lines: tuple[str, ...]
    next_cursor_base64: str | None
    class_complete: bool
    operation_complete: bool
    page_hash_hex: str
    maximum_attempts: int = 3

    def __post_init__(self) -> None:
        byte_count = sum(len(line.encode("utf-8")) + 1 for line in self.canonical_json_lines)
        if (
            _UUID7.fullmatch(self.export_operation_id) is None
            or type(self.page_number) is not int
            or not 1 <= self.page_number <= 2**64 - 1
            or not 1 <= len(self.canonical_json_lines) <= 500
            or byte_count > 4 * 1_024 * 1_024
            or any(not _canonical_document(line, 64 * 1_024) for line in self.canonical_json_lines)
            or _HASH.fullmatch(self.page_hash_hex) is None
            or self.operation_complete != (self.next_cursor_base64 is None)
            or (self.operation_complete and not self.class_complete)
            or (
                self.next_cursor_base64 is not None
                and not 1 <= len(self.next_cursor_base64) <= 1_024
            )
            or not 1 <= self.maximum_attempts <= 10
        ):
            raise ValueError("invalid RiffDB operator page")


@dataclass(frozen=True, slots=True)
class ReimportOperation:
    campaign_id: str
    contract_lineage: str
    scope: str
    portability_manifest_hash: str
    export_manifest_hash: str
    export_receipt_hash: str
    source_database_id: str
    target_database_id: str
    source_rows: int
    source_pages: int
    next_page: int
    rows_applied: int
    phase: str
    failure: str | None
    canonical_reimport_receipt_json: str | None
    reimport_receipt_hash: str | None


@dataclass(frozen=True, slots=True)
class OperatorErrorDetails:
    code: str
    category: str
    message: str
    retryability: str
    recovery_action: str
    outcome_uncertain: bool


class OperatorError(Exception):
    def __init__(self, details: OperatorErrorDetails) -> None:
        self.details = details
        super().__init__(f"{details.code}: {details.message}")


class OperatorTransport:
    """Serial, campaign-bound connection to the Rust reimport operator host."""

    __slots__ = ("_closed", "_identity", "_lock", "_next_request", "_socket")

    def __init__(self, connection: socket.socket, identity: OperatorIdentity) -> None:
        self._socket = connection
        self._identity = identity
        self._next_request = 1
        self._closed = False
        self._lock = threading.Lock()

    @classmethod
    def connect(
        cls,
        socket_path: str | Path,
        identity: OperatorIdentity,
        *,
        timeout_seconds: float = 30.0,
    ) -> OperatorTransport:
        path = str(socket_path)
        if not path.startswith("/") or len(path) > 4_096 or not 0 < timeout_seconds <= 300:
            raise ValueError("invalid RiffDB operator configuration")
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        connection.settimeout(timeout_seconds)
        try:
            connection.connect(path)
            transport = cls(connection, identity)
            request_id = transport._request_id("handshake")
            response = transport._request(
                {
                    "type": "handshake",
                    "request_id": request_id,
                    "protocol_version": OPERATOR_DRIVER_PROTOCOL_VERSION,
                    "database": identity.database,
                    "campaign_id": identity.campaign_id,
                    "portability_manifest_hash": identity.portability_manifest_hash,
                },
                request_id,
            )
            if set(response) != {
                "type",
                "request_id",
                "protocol_version",
                "driver_identity",
                "database",
                "campaign_id",
                "portability_manifest_hash",
            } or (
                response["type"] != "handshake"
                or response["protocol_version"] != OPERATOR_DRIVER_PROTOCOL_VERSION
                or not _short(response["driver_identity"])
                or response["database"] != identity.database
                or response["campaign_id"] != identity.campaign_id
                or response["portability_manifest_hash"]
                != identity.portability_manifest_hash
            ):
                raise RuntimeError("RiffDB operator identity mismatch")
            return transport
        except BaseException:
            connection.close()
            raise

    def start(
        self,
        canonical_export_manifest_json: str,
        canonical_export_receipt_json: str,
        maximum_attempts: int = 3,
    ) -> ReimportOperation:
        if not _canonical_document(
            canonical_export_manifest_json, 256 * 1_024
        ) or not _canonical_document(canonical_export_receipt_json, 256 * 1_024):
            raise ValueError("invalid RiffDB operator start request")
        request_id = self._request_id("start")
        return _required_operation(
            self._request(
                {
                    "type": "start",
                    "request_id": request_id,
                    "canonical_export_manifest_json": canonical_export_manifest_json,
                    "canonical_export_receipt_json": canonical_export_receipt_json,
                    "maximum_attempts": _attempts(maximum_attempts),
                },
                request_id,
            )
        )

    def apply_page(self, page: ReimportPage) -> ReimportOperation:
        request_id = self._request_id("page")
        return _required_operation(
            self._request(
                {
                    "type": "apply_page",
                    "request_id": request_id,
                    "export_operation_id": page.export_operation_id,
                    "page_number": page.page_number,
                    "canonical_json_lines": page.canonical_json_lines,
                    "next_cursor_base64": page.next_cursor_base64,
                    "class_complete": page.class_complete,
                    "operation_complete": page.operation_complete,
                    "page_hash_hex": page.page_hash_hex,
                    "maximum_attempts": page.maximum_attempts,
                },
                request_id,
            )
        )

    def status(self) -> ReimportOperation | None:
        request_id = self._request_id("status")
        return _optional_operation(
            self._request({"type": "status", "request_id": request_id}, request_id)
        )

    def cancel(self, maximum_attempts: int = 3) -> ReimportOperation | None:
        request_id = self._request_id("cancel")
        return _optional_operation(
            self._request(
                {
                    "type": "cancel",
                    "request_id": request_id,
                    "maximum_attempts": _attempts(maximum_attempts),
                },
                request_id,
            )
        )

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._socket.close()

    def __enter__(self) -> OperatorTransport:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def _request(self, request: dict[str, object], request_id: str) -> dict[str, object]:
        if not self._lock.acquire(blocking=False):
            raise RuntimeError("RiffDB operator session is busy")
        try:
            return self._request_serial(request, request_id)
        finally:
            self._lock.release()

    def _request_serial(
        self, request: dict[str, object], request_id: str
    ) -> dict[str, object]:
        if self._closed:
            raise RuntimeError("RiffDB operator session closed")
        body = _canonical_json(request)
        if not 1 < len(body) <= _MAX_FRAME_BYTES:
            raise ValueError("invalid RiffDB operator request")
        try:
            self._socket.sendall(struct.pack(">I", len(body)) + body)
            header = _read_exact(self._socket, 4)
            length = struct.unpack(">I", header)[0]
            if not 1 < length <= _MAX_FRAME_BYTES:
                raise RuntimeError("RiffDB operator returned an invalid frame")
            raw = _read_exact(self._socket, length)
            response = json.loads(raw)
        except (OSError, UnicodeError, json.JSONDecodeError, struct.error, EOFError):
            raise RuntimeError("RiffDB operator session failed") from None
        if not isinstance(response, dict) or response.get("request_id") != request_id:
            raise RuntimeError("RiffDB operator returned an invalid request identity")
        if response.get("type") == "error":
            raise _decode_error(response)
        return response

    def _request_id(self, kind: str) -> str:
        value = f"python.operator.{kind}.{self._next_request}"
        self._next_request = 1 if self._next_request == 2**63 - 1 else self._next_request + 1
        return value


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


def _read_exact(connection: socket.socket, length: int) -> bytes:
    output = bytearray()
    while len(output) < length:
        chunk = connection.recv(length - len(output))
        if not chunk:
            raise EOFError
        output.extend(chunk)
    return bytes(output)


def _required_operation(response: dict[str, object]) -> ReimportOperation:
    operation = _optional_operation(response)
    if operation is None:
        raise RuntimeError("RiffDB operator campaign was not found")
    return operation


def _optional_operation(response: dict[str, object]) -> ReimportOperation | None:
    if set(response) == {"type", "request_id"} and response["type"] == "not_found":
        return None
    if set(response) != {"type", "request_id", "operation"} or response["type"] != "operation":
        raise RuntimeError("RiffDB operator returned an invalid response")
    value = response["operation"]
    expected = {
        "campaign_id",
        "contract_lineage",
        "scope",
        "portability_manifest_hash",
        "export_manifest_hash",
        "export_receipt_hash",
        "source_database_id",
        "target_database_id",
        "source_rows",
        "source_pages",
        "next_page",
        "rows_applied",
        "phase",
        "failure",
        "canonical_reimport_receipt_json",
        "reimport_receipt_hash",
    }
    if not isinstance(value, dict) or set(value) != expected:
        raise RuntimeError("RiffDB operator returned invalid progress")
    try:
        campaign_id = _uuid7(value["campaign_id"])
        source_database_id = _uuid7(value["source_database_id"])
        target_database_id = _uuid7(value["target_database_id"])
        portability_hash = _hash(value["portability_manifest_hash"])
        export_manifest_hash = _hash(value["export_manifest_hash"])
        export_receipt_hash = _hash(value["export_receipt_hash"])
        receipt_hash = (
            None
            if value["reimport_receipt_hash"] is None
            else _hash(value["reimport_receipt_hash"])
        )
        failure = None if value["failure"] is None else _text(value["failure"])
        receipt = value["canonical_reimport_receipt_json"]
        if receipt is not None and (
            not isinstance(receipt, str)
            or not receipt.endswith("\n")
            or not _canonical_document(receipt[:-1], 256 * 1_024 - 1)
        ):
            raise ValueError
        return ReimportOperation(
            campaign_id=campaign_id,
            contract_lineage=_text(value["contract_lineage"]),
            scope=_text(value["scope"]),
            portability_manifest_hash=portability_hash,
            export_manifest_hash=export_manifest_hash,
            export_receipt_hash=export_receipt_hash,
            source_database_id=source_database_id,
            target_database_id=target_database_id,
            source_rows=_unsigned(value["source_rows"]),
            source_pages=_unsigned(value["source_pages"]),
            next_page=_unsigned(value["next_page"]),
            rows_applied=_unsigned(value["rows_applied"]),
            phase=_text(value["phase"]),
            failure=failure,
            canonical_reimport_receipt_json=receipt,
            reimport_receipt_hash=receipt_hash,
        )
    except (TypeError, ValueError):
        raise RuntimeError("RiffDB operator returned invalid progress") from None


def _decode_error(value: dict[str, object]) -> OperatorError:
    if set(value) != {
        "type",
        "request_id",
        "code",
        "category",
        "message",
        "retryability",
        "recovery_action",
        "outcome_uncertain",
    } or type(value["outcome_uncertain"]) is not bool:
        raise RuntimeError("RiffDB operator returned an invalid error")
    return OperatorError(
        OperatorErrorDetails(
            code=_text(value["code"]),
            category=_text(value["category"]),
            message=_text(value["message"]),
            retryability=_text(value["retryability"]),
            recovery_action=_text(value["recovery_action"]),
            outcome_uncertain=value["outcome_uncertain"],
        )
    )


def _short(value: object) -> bool:
    return (
        isinstance(value, str)
        and 0 < len(value) <= 4_096
        and not any(item in value for item in ("\n", "\r", "\0"))
    )


def _text(value: object) -> str:
    if not isinstance(value, str) or not _short(value):
        raise ValueError
    return value


def _hash(value: object) -> str:
    if not isinstance(value, str) or _HASH.fullmatch(value) is None:
        raise ValueError
    return value


def _uuid7(value: object) -> str:
    if not isinstance(value, str) or _UUID7.fullmatch(value) is None:
        raise ValueError
    return value


def _unsigned(value: object) -> int:
    if not isinstance(value, str) or re.fullmatch(r"(?:0|[1-9][0-9]*)", value) is None:
        raise ValueError
    return int(value)


def _attempts(value: int) -> int:
    if type(value) is not int or not 1 <= value <= 10:
        raise ValueError("invalid RiffDB operator attempt bound")
    return value


def _canonical_document(value: str, maximum: int) -> bool:
    return (
        0 < len(value.encode("utf-8")) <= maximum
        and value.startswith("{")
        and value.endswith("}")
        and "\n" not in value
        and "\r" not in value
    )


__all__ = [
    "OPERATOR_DRIVER_PROTOCOL_VERSION",
    "OperatorError",
    "OperatorErrorDetails",
    "OperatorIdentity",
    "OperatorTransport",
    "ReimportOperation",
    "ReimportPage",
]
