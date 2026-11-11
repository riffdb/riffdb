# req: DRV-001, DRV-005, DRV-012
"""Driver-socket transport behaviour against a scripted local peer.

Every exchange is driven by explicit frames on a blocking peer thread; socket
timeouts only bound a hung test and never order events.
"""

from __future__ import annotations

import json
import socket
import struct
import tempfile
import threading
import tomllib
import unittest
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace

from riffdb_application import (
    AsyncApplicationTransport,
    AttemptBudget,
    CallMetadata,
    DatabaseAlias,
    OutcomeUnknown,
    ProtocolError,
    QueryOptions,
    SyncApplicationTransport,
    UnsupportedTransportOperation,
)

_RUNTIME_ROOT = Path(__file__).resolve().parents[1]
_DRIVER_IDENTITY = "riffdb-driverd/" + tomllib.loads(
    (_RUNTIME_ROOT / "pyproject.toml").read_text(encoding="utf-8")
)["project"]["version"]
_HANG_BOUND_SECONDS = 10.0
_METADATA = CallMetadata(database=DatabaseAlias("default"), role="writer")


def _read_frame(conn: socket.socket) -> dict[str, object] | None:
    prefix = _read_exact(conn, 4)
    if prefix is None:
        return None
    (length,) = struct.unpack(">I", prefix)
    body = _read_exact(conn, length)
    if body is None:
        raise AssertionError("client sent a truncated frame")
    value = json.loads(body)
    assert isinstance(value, dict)
    return value


def _read_exact(conn: socket.socket, length: int) -> bytes | None:
    data = b""
    while len(data) < length:
        chunk = conn.recv(length - len(data))
        if not chunk:
            if data:
                raise AssertionError("client closed mid-frame")
            return None
        data += chunk
    return data


def _write_frame(conn: socket.socket, value: dict[str, object]) -> None:
    body = json.dumps(value, separators=(",", ":")).encode()
    conn.sendall(struct.pack(">I", len(body)) + body)


def _driver_error(request_id: str | None) -> dict[str, object]:
    return {
        "type": "error",
        "request_id": request_id,
        "code": "RDB-INPUT-0101",
        "category": "input",
        "operation": "OtherCommand",
        "symbol_path": [],
        "contract_lineage": None,
        "contract_version": None,
        "trace_id": None,
        "incident_id": None,
        "message": "application input is invalid",
        "retryability": "not_retryable",
        "recovery_action": "correct_request",
        "outcome_uncertain": False,
    }


class _ScriptedDriver:
    """One local peer that runs ``script`` on a thread and records failures."""

    def __init__(self, script: Callable[[_ScriptedDriver], None]) -> None:
        self._directory = tempfile.TemporaryDirectory()
        self.path = str(Path(self._directory.name) / "driver.sock")
        self._listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._listener.bind(self.path)
        self._listener.listen(4)
        self._listener.settimeout(_HANG_BOUND_SECONDS)
        self._failure: BaseException | None = None
        self._thread = threading.Thread(target=self._run, args=(script,), daemon=True)
        self._thread.start()

    def _run(self, script: Callable[[_ScriptedDriver], None]) -> None:
        try:
            script(self)
        except BaseException as error:  # surfaced by finish()
            self._failure = error

    def accept(self) -> socket.socket:
        conn, _ = self._listener.accept()
        conn.settimeout(_HANG_BOUND_SECONDS)
        return conn

    def accept_verified(self) -> socket.socket:
        conn = self.accept()
        handshake = _read_frame(conn)
        assert handshake is not None and handshake["type"] == "handshake"
        _write_frame(
            conn,
            {
                "type": "handshake",
                "request_id": handshake["request_id"],
                "protocol_version": handshake["protocol_version"],
                "driver_identity": _DRIVER_IDENTITY,
                "application_manifest_hash": "a" * 64,
                "operation_catalog_hash": "b" * 64,
                "contract_lineage": handshake["contract_lineage"],
                "contract_version": handshake["contract_version"],
                "contract_bundle_hash": handshake["contract_bundle_hash"] or "c" * 64,
                "database": "default",
                "role": "writer",
                "role_definition_hash": "d" * 64,
                "remote_identity_hash": "e" * 64,
            },
        )
        return conn

    @staticmethod
    def read_invoke_id(conn: socket.socket) -> str:
        invoke = _read_frame(conn)
        assert invoke is not None and invoke["type"] == "invoke"
        request_id = invoke["request_id"]
        assert isinstance(request_id, str)
        return request_id

    @staticmethod
    def assert_discarded(conn: socket.socket) -> None:
        """The client closes the stream instead of sending another request."""
        assert _read_frame(conn) is None, "client reused a stream after a failed reply"
        conn.close()

    def finish(self) -> None:
        self._thread.join(_HANG_BOUND_SECONDS)
        self._listener.close()
        self._directory.cleanup()
        if self._thread.is_alive():
            raise AssertionError("scripted driver peer did not finish")
        if self._failure is not None:
            raise self._failure


def _create_order(transport: SyncApplicationTransport) -> object:
    return transport._execute_command(
        contract_lineage="orders",
        contract_version=1,
        command_name="CreateOrder",
        plan_hash="1" * 64,
        input={"name": {"kind": "string", "value": "widget"}},
        attempts=AttemptBudget(1),
    )


class DriverSocketCommandUncertaintyTests(unittest.TestCase):
    def test_unrelated_error_reply_leaves_the_command_outcome_unknown(self) -> None:
        def script(peer: _ScriptedDriver) -> None:
            conn = peer.accept_verified()
            peer.read_invoke_id(conn)
            _write_frame(conn, _driver_error("unrelated-request"))
            peer.assert_discarded(conn)

        peer = _ScriptedDriver(script)
        transport = SyncApplicationTransport.connect_driver_socket(peer.path, _METADATA)
        try:
            with self.assertRaises(OutcomeUnknown):
                _create_order(transport)
        finally:
            transport.close()
            peer.finish()

    def test_unpresentable_correlated_command_result_leaves_the_outcome_unknown(self) -> None:
        def script(peer: _ScriptedDriver) -> None:
            conn = peer.accept_verified()
            request_id = peer.read_invoke_id(conn)
            _write_frame(
                conn,
                {
                    "type": "result",
                    "request_id": request_id,
                    "value": {"type": "string", "value": "not-a-record"},
                    "application_head": 9,
                    "cursor": None,
                    "replayed": False,
                },
            )
            assert _read_frame(conn) is None
            conn.close()

        peer = _ScriptedDriver(script)
        transport = SyncApplicationTransport.connect_driver_socket(peer.path, _METADATA)
        try:
            with self.assertRaises(OutcomeUnknown):
                _create_order(transport)
        finally:
            transport.close()
            peer.finish()

    def test_malformed_reply_to_a_query_stays_a_protocol_error(self) -> None:
        def script(peer: _ScriptedDriver) -> None:
            conn = peer.accept_verified()
            peer.read_invoke_id(conn)
            conn.sendall(b"\x00\x00\x00\x02{}")
            peer.assert_discarded(conn)

        peer = _ScriptedDriver(script)
        transport = SyncApplicationTransport.connect_driver_socket(peer.path, _METADATA)
        try:
            with self.assertRaises(ProtocolError):
                transport._execute_named_query(
                    contract_lineage="orders",
                    contract_version=1,
                    contract_bundle_hash="c" * 64,
                    module_hash="2" * 64,
                    query_name="GetOrder",
                    plan_hash="3" * 64,
                    parameters={},
                    options=QueryOptions(),
                )
        finally:
            transport.close()
            peer.finish()


class DriverSocketReactiveTests(unittest.IsolatedAsyncioTestCase):
    async def test_reactive_operations_are_refused_before_any_request_is_sent(self) -> None:
        received: list[bytes] = []

        def script(peer: _ScriptedDriver) -> None:
            conn = peer.accept()
            while chunk := conn.recv(4096):
                received.append(chunk)
            conn.close()

        peer = _ScriptedDriver(script)
        transport = await AsyncApplicationTransport.connect_driver_socket(peer.path, _METADATA)
        identity = {
            "reactive_module_hash": "11" * 32,
            "operation_name": "TicketEvents",
            "parameters": {},
            "consumer_name": "worker",
        }
        item = SimpleNamespace(event_id="e", lease_token="t", history_incarnation=1)
        calls = {
            "event batch": lambda: transport._consume_event_batch(**identity),
            "event ack": lambda: transport._acknowledge_event(**identity, event_id="e"),
            "event seek": lambda: transport._seek_event_consumer(**identity, checkpoint="0"),
            "event status": lambda: transport._event_consumer_status(**identity),
            "contextual next": lambda: transport._consume_contextual_subscription(**identity),
            "contextual ack": lambda: transport._acknowledge_contextual_item(
                **identity, item=item
            ),
            "contextual status": lambda: transport._contextual_subscription_status(**identity),
            "contextual reaction": lambda: transport._execute_contextual_reaction(
                **identity, plan_hash="1" * 64, contract_version=1
            ),
            "live query": lambda: anext(
                transport._watch_named_query(operation_name="OpenTickets", cursor=None)
            ),
        }
        try:
            for label, call in calls.items():
                with self.subTest(label):
                    with self.assertRaises(UnsupportedTransportOperation) as raised:
                        await call()
                    self.assertIn("driver socket", str(raised.exception))
        finally:
            await transport.close()
            peer.finish()
        self.assertEqual(received, [], "a refused reactive call reached the peer")


if __name__ == "__main__":
    unittest.main()
