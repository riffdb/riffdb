from __future__ import annotations

import re
import unittest
from pathlib import Path

from riffdb_app_baseline_python.histogram import LatencyHistogram
from riffdb_app_baseline_python.ids import format_uuid, uuid_from_ordinal
from riffdb_app_baseline_python.load import INTERACTIVE_WEIGHTS
from riffdb_app_baseline_python.schema import OBLIGATIONS, SCHEMA_SQL, safe_input_fingerprint, to_pyformat
from riffdb_app_baseline_python.seed import Scale, SeedDataset

REPO = Path(__file__).resolve().parents[4]


class IdentifierTests(unittest.TestCase):
    def test_uuid_from_ordinal_matches_rust_layout(self) -> None:
        value = uuid_from_ordinal(0x10, 0)
        self.assertEqual(len(value), 16)
        self.assertEqual(value[0], 0x10)
        self.assertEqual(value[6], 0x70)
        self.assertEqual(value[8] & 0xC0, 0x80)
        self.assertEqual(format_uuid(value), "10101010-1010-7010-8000-000000000000")

    def test_fingerprint_is_canonical_and_length_prefixed(self) -> None:
        first = safe_input_fingerprint([b"a/b", b"c"])
        second = safe_input_fingerprint([b"a", b"b/c"])
        repeated = safe_input_fingerprint([b"a/b", b"c"])
        self.assertEqual(len(first), 64)
        self.assertEqual(first, repeated)
        self.assertNotEqual(first, second)
        self.assertNotIn("a/b", first)


class SeedTests(unittest.TestCase):
    def test_smoke_and_full_shapes(self) -> None:
        smoke = SeedDataset.generate(Scale.smoke())
        self.assertEqual(len(smoke.organizations), 2)
        self.assertEqual(smoke.scale.board_dense_open, 0)
        self.assertLess(smoke.board_dense_open_count(), 50)
        probes = smoke.probes()
        self.assertEqual(len(probes.organization_id), 16)
        self.assertNotEqual(probes.ticket_id, probes.write_ticket_id)

        full = SeedDataset.generate(Scale.full())
        self.assertEqual(full.board_dense_open_count(), 600)
        self.assertEqual(len(full.tenant_probes(3)), 3)


class SchemaTests(unittest.TestCase):
    def test_schema_contains_safe_app_obligations(self) -> None:
        for table in (
            "app_permission",
            "app_idempotency",
            "app_audit",
            "app_domain_event",
            "app_outbox_intent",
        ):
            self.assertIn(f"CREATE TABLE {table}", SCHEMA_SQL)
        self.assertEqual(
            OBLIGATIONS,
            (
                "symbolic_operation_authorization",
                "idempotency_admission_and_equal_input_replay",
                "domain_mutation",
                "audit_and_provenance",
                "domain_event",
                "outbox_intent",
                "one_atomic_transaction",
            ),
        )

    def test_python_schema_tables_match_rust_adapter(self) -> None:
        rust = (REPO / "examples/app-baseline/postgres/src/lib.rs").read_text()
        rust_tables = set(re.findall(r"CREATE TABLE (\w+)", rust))
        python_tables = set(re.findall(r"CREATE TABLE (\w+)", SCHEMA_SQL))
        self.assertEqual(rust_tables, python_tables)

    def test_pyformat_rewrites_postgres_placeholders(self) -> None:
        converted = to_pyformat(
            "WHERE organization_id = $1::text::uuid AND ticket_id = $2::text::uuid LIMIT $3"
        )
        self.assertEqual(
            converted, "WHERE organization_id = %s::uuid AND ticket_id = %s::uuid LIMIT %s"
        )


class RiffDbPythonSafetyBoundaryTests(unittest.TestCase):
    def test_riffdb_driver_contains_no_python_safety_implementation(self) -> None:
        source = (
            REPO / "examples/app-baseline/python/riffdb_app_baseline_python/riffdb_app.py"
        ).read_text()
        for forbidden in (
            "app_permission",
            "app_idempotency",
            "app_audit",
            "app_domain_event",
            "app_outbox_intent",
            "PERMISSION_SQL",
            "IDEMPOTENCY_LOCK_SQL",
            "INSERT_AUDIT_SQL",
            "INSERT_EVENT_SQL",
            "INSERT_OUTBOX_SQL",
            "_safe_admit",
            "_safe_complete",
            "safe_input_fingerprint",
        ):
            self.assertNotIn(forbidden, source)
        self.assertIn("No Python safety code", source)
        self.assertIn("generated TicketDesk", source)

    def test_postgres_driver_owns_the_sql_safety_tables(self) -> None:
        source = (
            REPO / "examples/app-baseline/python/riffdb_app_baseline_python/safe_app.py"
        ).read_text()
        self.assertIn("_safe_admit", source)
        self.assertIn("_safe_complete", source)
        self.assertIn("INSERT_OUTBOX_SQL", source)

    def test_generated_ticketdesk_client_is_present(self) -> None:
        client = REPO / "examples/ticketdesk/generated/python/client.py"
        self.assertTrue(client.is_file(), client)
        text = client.read_text()
        self.assertIn("class TicketDeskClient", text)
        self.assertIn("def create_comment", text)

    def test_report_records_which_side_owns_safety(self) -> None:
        from riffdb_app_baseline_python.load import LoadReport, OpStats

        riff = LoadReport(
            backend_id="riffdb_public_grpc",
            clients=1,
            profile="interactive",
            measured_elapsed_ns=1_000_000_000,
            seed_ns=1,
            aggregate=OpStats(),
            by_op={},
            worker_completed=[0],
        ).json()
        postgres = LoadReport(
            backend_id="postgres_safe_app",
            clients=1,
            profile="interactive",
            measured_elapsed_ns=1_000_000_000,
            seed_ns=1,
            aggregate=OpStats(),
            by_op={},
            worker_completed=[0],
        ).json()
        self.assertEqual(riff["safety_owner"], "riffdbd_rust")
        self.assertEqual(postgres["safety_owner"], "python_sql")
        self.assertFalse(riff["evidentiary"])
        self.assertIn("No authorization, idempotency, audit, event, or outbox code runs in Python", " ".join(riff["notes"]))


class MixTests(unittest.TestCase):
    def test_interactive_weights_are_mostly_reads(self) -> None:
        total = sum(weight for _, weight in INTERACTIVE_WEIGHTS)
        writes = sum(
            weight
            for name, weight in INTERACTIVE_WEIGHTS
            if name in {"create_comment", "close_ticket_with_comment", "open_ticket_with_labels"}
        )
        self.assertGreater(total - writes, writes)
        names = [name for name, _ in INTERACTIVE_WEIGHTS]
        self.assertNotIn("swap_member_roles", names)


class HistogramTests(unittest.TestCase):
    def test_percentiles_track_injected_latencies(self) -> None:
        histogram = LatencyHistogram()
        for _ in range(90):
            histogram.record_ns(100_000)
        for _ in range(9):
            histogram.record_ns(1_000_000)
        histogram.record_ns(10_000_000)
        self.assertGreaterEqual(histogram.percentile_ns(50), 100_000)
        self.assertLess(histogram.percentile_ns(50), 1_000_000)
        self.assertGreaterEqual(histogram.percentile_ns(99), 1_000_000)


if __name__ == "__main__":
    unittest.main()
