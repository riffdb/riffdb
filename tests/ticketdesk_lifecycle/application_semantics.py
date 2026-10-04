# req: END-001, END-002, END-003, END-007
"""Compile the campaign's real application and its missing-fence counterexample."""

import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

CLI = Path(sys.argv.pop(1)).resolve()
APPLICATION = Path(__file__).resolve().parents[2] / "examples/ticketdesk/lifecycle"


class ApplicationSemantics(unittest.TestCase):
    def compile(self, application):
        return subprocess.run(
            [str(CLI), "application", "check", str(application), "--output", "json"],
            capture_output=True, text=True, timeout=30, check=False,
        )

    def test_actual_application_and_generated_closure_compile(self):
        result = self.compile(APPLICATION / "riffdb.application.json")
        self.assertEqual(result.returncode, 0, "actual lifecycle application must compile")

    def test_successor_added_command_has_closed_generated_schema_without_role_authority(self):
        successor = APPLICATION.parent / "lifecycle-v2"
        catalog = json.loads((successor / "generated/mcp/tools.json").read_bytes())
        command = next(command for command in catalog["commands"] if command["name"] == "ticketdesk_lifecycle_version")
        descriptor = command["mcp_descriptor"]
        schema = descriptor["inputSchema"]
        self.assertFalse(schema["additionalProperties"])
        self.assertEqual(schema["required"], ["idempotency_key", "organization_id"])
        self.assertEqual(schema["properties"]["idempotency_key"]["x-riffdb-maxUtf8Bytes"], 128)
        outcomes = descriptor["outputSchema"]["$defs"]["outcome"]["oneOf"]
        self.assertEqual({value["properties"]["type"]["const"] for value in outcomes}, {"Missing", "Version"})
        self.assertTrue(all(value["additionalProperties"] is False for value in outcomes))
        source = json.loads((successor / "riffdb.application.json").read_bytes())
        self.assertTrue(all("LifecycleVersion" not in role["commands"] for role in source["roles"]))

    def test_replacement_without_caller_fence_is_rejected_at_both_write_spans(self):
        with tempfile.TemporaryDirectory(prefix="riffdb-lifecycle-fence-") as directory:
            root = Path(directory) / "application"
            shutil.copytree(APPLICATION, root)
            # Check source semantics without letting stale lock identities mask
            # the write-evidence diagnostic being tested.
            (root / "riffdb.application.lock.json").unlink()
            contract = root / "riffdb/contract.riff"
            source = contract.read_text()
            fence = "    require current: ticket.revision == expected_revision else Stale {}\n"
            self.assertEqual(source.count(fence), 1)
            source = source.replace(fence, "")
            contract.write_text(source)
            result = self.compile(root / "riffdb.application.json")
            self.assertEqual(result.returncode, 1)
            report = json.loads(result.stderr)
            self.assertEqual(report["version"], 1)
            diagnostics = report["diagnostics"]
            self.assertEqual(len(diagnostics), 2)
            self.assertEqual({entry["code"] for entry in diagnostics}, {"RDB-C048"})
            self.assertEqual(
                [source[d["span"]["start"]:d["span"]["end"]] for d in diagnostics],
                ["ticket.status", "ticket.updated_at"],
            )
            self.assertTrue(all(d["stage"] == "contract_semantic" for d in diagnostics))
            self.assertTrue(all(d["file_change"] == "no_files_changed" for d in diagnostics))


if __name__ == "__main__":
    unittest.main()
