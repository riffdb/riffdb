# req: END-003, END-007, END-008, END-009, END-010
"""Complete synthetic joins, never a real measured receipt or qualification.

Native work supplies fixture bytes. The actual Git reads, original-file
custody, independent reconcilers, operator joins and accepted checker all run.
No synthetic receipt is published or used as native campaign evidence.
"""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from lib import ticketdesk_lifecycle_campaign_qualification as qualification
from lib.ticketdesk_lifecycle_campaign_qualification_prerequisites import PrerequisiteError
from qualification_fixture import fixture, write


class QualificationJoinTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temporary.cleanup)
        cls.value = fixture(Path(cls.temporary.name))

    def options(self):
        value = self.value
        return dict(**value.prerequisites.options, closed_sha256=value.closed_sha256,
            post_validation_sha256=value.post_validation_sha256)

    def verify(self, **changes):
        value = self.value
        options = self.options(); options.update(changes)
        return qualification.assemble(value.root, value.prerequisites.frozen, value.prerequisites.source, **options)

    def test_actual_producer_closed_reconciliation_and_assembler_join_every_original_input(self):
        receipt, proof = self.verify()
        self.assertEqual(receipt['schema'], qualification.contract.RECEIPT_SCHEMA)
        self.assertEqual(receipt['checks'].keys(), qualification.contract.CHECKS)
        self.assertTrue(all(value is True for value in receipt['checks'].values()))
        self.assertEqual(receipt['artifacts'].keys(), qualification.contract.ARTIFACTS)
        self.assertEqual(len(receipt['windows']), 96)
        self.assertEqual(len(receipt['actions']), 7)
        self.assertEqual(receipt['reclaimed_generations'], [3,4,5])
        self.assertEqual(receipt['windows'][17]['unresolved_commands'], 6000)
        self.assertEqual(receipt['windows'][18]['unresolved_commands'], 0)
        self.assertEqual(proof['closed']['sha256'], self.value.closed_sha256)
        self.assertEqual(proof['scope'], 'offline_after_original_campaign_controller_exit')
        self.assertIn('original_terminal', proof['campaign'])
        self.assertIn('operator_ledger', proof['operators'])
        self.assertIn('server-002/server.process-usage.json', proof['owners'])
        self.assertIn('server-003/server.stderr', proof['extents'])
        # The accepted in-memory receipt shape is exercised only. No fixture
        # claims that its numeric eight-hour clock was actually observed.
        self.assertFalse(self.value.closed['qualifies_complete_campaign'])
        self.assertFalse(self.value.final['qualifies_complete_campaign'])
        self.assertFalse((self.value.prerequisites.frozen/'receipt.json').exists())

    def test_changed_original_bytes_cannot_hide_behind_unchanged_successful_summaries(self):
        cases = ('memory-samples.jsonl', 'campaign-observer-observations.jsonl',
            'backups/campaign-source/database.redb', 'server-003/server.stderr', 'load.phase')
        root = self.value.root
        for name in cases:
            with self.subTest(name=name):
                path = root/name; original = path.read_bytes()
                try:
                    if name.endswith('.jsonl'):
                        lines = original.splitlines(); first = json.loads(lines[0])
                        if name.startswith('memory'): first['processes']['controller']['task_census_restarts'] += 1
                        else: first['observation']['live_updates'] += 1
                        lines[0] = json.dumps(first).encode(); path.write_bytes(b'\n'.join(lines)+b'\n')
                    elif name.endswith('server.stderr'):
                        path.write_bytes(original.replace(b'generation=3\t', b'generation=999\t'))
                    elif name == 'load.phase': path.write_bytes(b'5 2 2 application-v2.credential\n')
                    else: path.write_bytes(original+b'changed\n')
                    with self.assertRaises(PrerequisiteError): self.verify()
                finally: path.write_bytes(original)

    def test_missing_closed_input_or_forged_aggregate_is_rejected_by_independent_join(self):
        root = self.value.root; path = root/'campaign-closed-reconciliation.json'; original = path.read_bytes()
        for fault in ('missing_input', 'resource_total', 'provider_peak', 'history_total', 'owner_peak'):
            with self.subTest(fault=fault):
                changed = deepcopy(self.value.closed)
                if fault == 'missing_input': changed['inputs'].pop('provider')
                elif fault == 'resource_total': changed['resources']['windows'][0]['database_bytes'] += 1
                elif fault == 'provider_peak': changed['provider']['peak_charged_bytes'] += 1
                elif fault == 'history_total': changed['history']['windows'][0]['components']['logical'] += 1
                else: changed['owners']['rss_role_peak_upper_bounds']['server'] += 1
                try:
                    raw = write(root, path.name, changed)
                    with self.assertRaises(PrerequisiteError):
                        self.verify(closed_sha256=hashlib.sha256(raw).hexdigest())
                finally: path.write_bytes(original)

    def test_original_replay_and_terminal_resource_bounds_remain_semantic_requirements(self):
        root = self.value.root
        ledger = [json.loads(line) for line in (root/'campaign-operator.jsonl').read_text().splitlines()]
        replay = next(row for row in reversed(ledger) if row['command'] == 'command.run')
        path = root/f'operator-{replay["index"]:03}.stdout'; original = path.read_bytes()
        try:
            value = json.loads(original); value['result']['provenance_uri'] = 'mixed-original-provenance'
            write(root, path.name, value)
            with self.assertRaises(PrerequisiteError): self.verify()
        finally: path.write_bytes(original)
        path = root/'campaign-post-validation-resources.json'; original = path.read_bytes()
        try:
            value = json.loads(original)
            value['files']['derived_bytes'] = qualification.DISK_LIMITS['derived_bytes']+1
            raw = write(root, path.name, value)
            with self.assertRaises(PrerequisiteError):
                self.verify(post_validation_sha256=hashlib.sha256(raw).hexdigest())
        finally: path.write_bytes(original)

    def test_changed_acknowledgement_prefix_and_nonterminal_public_restore_refuse(self):
        root = self.value.root; path = root/'updates.witness'
        with path.open('r+b') as target:
            target.seek(24); original = target.read(8)
            try:
                target.seek(24); target.write((65538).to_bytes(8, 'big')); target.flush()
                with self.assertRaises(PrerequisiteError): self.verify()
            finally: target.seek(24); target.write(original); target.flush()
        ledger = [json.loads(line) for line in (root/'campaign-operator.jsonl').read_text().splitlines()]
        restore = next(row for row in ledger if row['command'] == 'backup.restore')
        path = root/f'operator-{restore["index"]:03}.stdout'; original = path.read_bytes()
        try:
            value = json.loads(original); value['result']['phase'] = 'accepted'
            write(root, path.name, value)
            with self.assertRaises(PrerequisiteError): self.verify()
        finally: path.write_bytes(original)
