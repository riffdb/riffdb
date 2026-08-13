from __future__ import annotations

import json
import os
import shutil
import socket
import struct
import tempfile
import threading
import unittest
from pathlib import Path

from riffdb_application.operator import OperatorIdentity, OperatorTransport, ReimportPage

CAMPAIGN = "018f2f85-3c20-7a31-8f11-112233445566"
SOURCE = "018f2f85-3c20-7a31-8f11-112233445577"
TARGET = "018f2f85-3c20-7a31-8f11-112233445588"
HASH = "0101010101010101010101010101010101010101010101010101010101010101"


class OperatorTests(unittest.TestCase):
    def test_campaign_bound_surface_has_no_application_or_credential_dispatch(self) -> None:
        scratch = os.environ.get("TMPDIR")
        self.assertIsNotNone(scratch, "TMPDIR must name the protected test scratch root")
        directory = Path(tempfile.mkdtemp(prefix="riffdb-python-operator-", dir=scratch))
        path = directory / "operator.sock"
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(str(path))
        listener.listen(1)
        failures: list[BaseException] = []
        seen: list[str] = []

        def serve() -> None:
            try:
                connection, _ = listener.accept()
                with connection:
                    for index in range(3):
                        request = _read(connection)
                        self.assertNotIn("operation", request)
                        self.assertNotIn("credential", request)
                        self.assertNotIn("endpoint", request)
                        seen.append(str(request["type"]))
                        request_id = str(request["request_id"])
                        if index == 0:
                            response: dict[str, object] = {
                                "type": "handshake",
                                "request_id": request_id,
                                "protocol_version": 1,
                                "driver_identity": "riffdb-driver-host/v1",
                                "database": "restored",
                                "campaign_id": CAMPAIGN,
                                "portability_manifest_hash": HASH,
                            }
                        elif index == 1:
                            response = {
                                "type": "operation",
                                "request_id": request_id,
                                "operation": _operation(),
                            }
                        else:
                            response = {"type": "not_found", "request_id": request_id}
                        _write(connection, response)
            except BaseException as error:  # test thread must report to its owner
                failures.append(error)

        thread = threading.Thread(target=serve, name="riffdb-python-operator-test")
        thread.start()
        try:
            identity = OperatorIdentity("restored", CAMPAIGN, HASH)
            with OperatorTransport.connect(path, identity) as transport:
                progress = transport.start("{}", "{}")
                self.assertEqual(progress.rows_applied, 2)
                self.assertIsNone(transport.status())
                with self.assertRaises(ValueError):
                    ReimportPage(
                        export_operation_id=SOURCE,
                        page_number=1,
                        canonical_json_lines=("{\"entity\":\"Ticket\"}",),
                        next_cursor_base64="cursor",
                        class_complete=True,
                        operation_complete=True,
                        page_hash_hex=HASH,
                    )
            thread.join(timeout=5)
            self.assertFalse(thread.is_alive())
            self.assertEqual(failures, [])
            self.assertEqual(seen, ["handshake", "start", "status"])
        finally:
            listener.close()
            shutil.rmtree(directory)


def _read(connection: socket.socket) -> dict[str, object]:
    length = struct.unpack(">I", _read_exact(connection, 4))[0]
    value = json.loads(_read_exact(connection, length))
    if not isinstance(value, dict):
        raise TypeError("expected object")
    return value


def _read_exact(connection: socket.socket, length: int) -> bytes:
    output = bytearray()
    while len(output) < length:
        chunk = connection.recv(length - len(output))
        if not chunk:
            raise EOFError
        output.extend(chunk)
    return bytes(output)


def _write(connection: socket.socket, value: dict[str, object]) -> None:
    body = json.dumps(value, separators=(",", ":")).encode()
    connection.sendall(struct.pack(">I", len(body)) + body)


def _operation() -> dict[str, object]:
    return {
        "campaign_id": CAMPAIGN,
        "contract_lineage": "TicketDesk",
        "scope": "whole_application",
        "portability_manifest_hash": HASH,
        "export_manifest_hash": HASH,
        "export_receipt_hash": HASH,
        "source_database_id": SOURCE,
        "target_database_id": TARGET,
        "source_rows": "3",
        "source_pages": "1",
        "next_page": "2",
        "rows_applied": "2",
        "phase": "applying",
        "failure": None,
        "canonical_reimport_receipt_json": None,
        "reimport_receipt_hash": None,
    }


if __name__ == "__main__":
    unittest.main()
