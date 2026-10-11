# req: END-007, END-008, END-010
"""Continuous campaign observations need bounded, complete clock coverage."""
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from lib import ticketdesk_lifecycle_resources as resources

SECOND = 1000000000


class CampaignMemoryObservations(unittest.TestCase):
    roles = {"controller": 123, "server": 124, "load": 125, "observer": 126,
             "native_sampler": 127}

    def tree(self, root):
        for pid in self.roles.values():
            process = root / str(pid)
            (process / "task" / str(pid)).mkdir(parents=True)
            children = tuple(self.roles.values())[1:] if pid == 123 else ()
            (process / "task" / str(pid) / "children").write_text(" ".join(map(str, children)))
            fields = ["S"] + ["0"] * 18 + ["9"] + ["0"] * 30
            (process / "stat").write_text(f"{pid} (fixture) " + " ".join(fields) + "\n")
            (process / "status").write_text(f"State:\tS\nPid:\t{pid}\n"
                "VmRSS:\t13 kB\nVmSize:\t101 kB\nVmHWM:\t19 kB\nVmPeak:\t127 kB\n")

    def test_full_span_keeps_all_windows_without_exceeding_record_bound(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.tree(root)
            with resources.CampaignMemorySamples(root, self.roles, 86400, proc_root=root) as samples:
                for elapsed in range(0, 86401, 60):
                    samples.poll(elapsed * SECOND)
                samples.finish()
                self.assertEqual(samples.count, 1440)
                self.assertEqual(samples.coverage, [5] * 288)
            rows = [json.loads(line) for line in samples.path.read_bytes().splitlines()]
            self.assertEqual(len(rows), 1440)
            self.assertTrue(all(row["rss_bytes"] == 5 * 13 * 1024 for row in rows))
            self.assertTrue(all(row["qualifies_resource_model"] is False for row in rows))
            self.assertEqual(samples.path.stat().st_mode & 0o777, 0o600)

    def test_missing_observation_gap_refuses_before_another_record(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.tree(root)
            with resources.CampaignMemorySamples(root, self.roles, 21900, proc_root=root) as samples:
                samples.poll(0)
                samples.poll(75 * SECOND)
                with self.assertRaisesRegex(resources.ResourceError, "^resource_sample_gap$"):
                    samples.poll(150 * SECOND + 1)
                self.assertEqual(samples.count, 2)

    def test_late_first_sample_and_incomplete_terminal_clock_refuse(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.tree(root)
            with resources.CampaignMemorySamples(root, self.roles, 21900, proc_root=root) as samples:
                with self.assertRaisesRegex(resources.ResourceError, "^resource_sample_gap$"):
                    samples.poll(75 * SECOND + 1)
            with tempfile.TemporaryDirectory() as output:
                with resources.CampaignMemorySamples(output, self.roles, 21900, proc_root=root) as samples:
                    for elapsed in range(0, 21900, 60):
                        samples.poll(elapsed * SECOND)
                    with self.assertRaisesRegex(resources.ResourceError, "^resource_sample_terminal$"):
                        samples.finish()

    def test_campaign_requires_native_inventory_and_exact_window_duration(self):
        for duration in (True, 0, 1500, 21600, 21901, 86700):
            with self.subTest(duration=duration), self.assertRaises(resources.ResourceError):
                resources.CampaignMemorySamples(Path("unused"), self.roles, duration)
        with self.assertRaisesRegex(resources.ResourceError, "^resource_process_inventory$"):
            resources.CampaignMemorySamples(Path("unused"),
                {key: value for key, value in self.roles.items() if key != "native_sampler"}, 21900)

    def test_replaced_process_and_untracked_child_are_not_memory_reclamation(self):
        for replacement in (False, True):
            with self.subTest(replacement=replacement), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.tree(root)
                with resources.CampaignMemorySamples(root, self.roles, 21900, proc_root=root) as samples:
                    samples.poll(0)
                    if replacement:
                        stat = root / "124" / "stat"
                        stat.write_text(stat.read_text().replace(" 9 ", " 10 "))
                    else:
                        (root / "123" / "task" / "123" / "children").write_text("124 125 126 127 128")
                    with self.assertRaises(resources.ResourceError):
                        samples.poll(60 * SECOND)
                    self.assertEqual(samples.count, 1)


if __name__ == "__main__":
    unittest.main()
