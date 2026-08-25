from __future__ import annotations

import json
import unittest
from pathlib import Path

from riffdb_application import _native


CORPUS_PATH = (
    Path(__file__).resolve().parents[4]
    / "tests"
    / "drivers"
    / "protocol-conformance-v1.json"
)


def _python_value(entry: dict[str, object]) -> dict[str, object]:
    generator = entry.get("generator")
    if generator is None:
        value = entry.get("python_value")
        if not isinstance(value, dict):
            raise AssertionError(f"{entry['name']} lacks a Python value")
        return value
    if not isinstance(generator, dict):
        raise AssertionError(f"{entry['name']} has an invalid generator")
    kind = generator.get("kind")
    size = generator.get("size")
    if not isinstance(size, int) or size < 0:
        raise AssertionError(f"{entry['name']} has an invalid generator size")
    if kind == "string":
        return {"kind": "string", "value": "x" * size}
    if kind == "null_list":
        return {"kind": "list", "value": [{"kind": "null"}] * size}
    raise AssertionError(f"{entry['name']} has an unknown generator")


class DriverProtocolConformanceTests(unittest.TestCase):
    def test_existing_in_process_admission_matches_driver_host_rules(self) -> None:
        corpus = json.loads(CORPUS_PATH.read_text(encoding="utf-8"))
        self.assertEqual(corpus["schema"], "riffdb.driver-protocol-conformance/v1")
        self.assertEqual(corpus["authoritative_rule"], "riffdb-driver-host-v3")
        for entry in corpus["entries"]:
            with self.subTest(entry=entry["name"]):
                encoded = json.dumps(_python_value(entry), separators=(",", ":"))
                disposition = entry["expected"]["disposition"]
                if disposition == "accepted":
                    _native.validate_bridge_value(encoded)
                    continue
                self.assertEqual(disposition, "invalid_input")
                with self.assertRaises(_native.NativeError) as raised:
                    _native.validate_bridge_value(encoded)
                self.assertEqual(raised.exception.args[0], "invalid_input")


if __name__ == "__main__":
    unittest.main()
