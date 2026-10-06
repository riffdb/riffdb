# req: END-007, END-008, END-010
"""Resource evidence must refuse missing data and recycled process identities."""
import os
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from lib.ticketdesk_lifecycle_resources import (
    ProcessMemory, PersistentProcessMemory, PreparationMemorySamples, ResourceError)
from lib import ticketdesk_lifecycle_resources as resources


class ResourceObservations(unittest.TestCase):
    def fixture(self, root, *, start=9, state="S", rss="13", virtual="101", pid=123):
        process = root / str(pid)
        process.mkdir(exist_ok=True)
        # A process name can itself contain spaces and closing parentheses.
        fields = [state] + ["0"] * 18 + [str(start)] + ["0"] * 30
        (process / "stat").write_text(f"{pid} (worker ) name) " + " ".join(fields) + "\n")
        (process / "status").write_text(
            f"Name:\tworker\nState:\t{state} (sample)\nPid:\t{pid}\n"
            f"VmRSS:\t{rss} kB\nVmSize:\t{virtual} kB\nVmData:\t2 kB\n")
        return process

    def test_live_process_sample_discloses_an_upper_bound_instead_of_allocator_live_bytes(self):
        value = ProcessMemory(os.getpid()).sample()
        self.assertGreater(value["rss_bytes"], 0)
        self.assertGreaterEqual(value["allocator_bytes"], value["rss_bytes"])
        self.assertEqual(value["allocator_basis"], "whole_process_virtual_address_space_upper_bound")
        self.assertEqual(value.keys(), {"rss_bytes", "allocator_bytes", "allocator_basis"})

    def test_data_segment_mapping_is_not_reported_as_allocator_ownership(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root)
            value = ProcessMemory(123, proc_root=root).sample()
            self.assertEqual(value["rss_bytes"], 13 * 1024)
            self.assertEqual(value["allocator_bytes"], 101 * 1024)

    def test_recycled_process_identity_refuses_instead_of_looking_like_memory_reclamation(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root)
            process = ProcessMemory(123, proc_root=root)
            process.sample()
            self.fixture(root, start=10, rss="1", virtual="2")
            with self.assertRaisesRegex(ResourceError, "^resource_process_identity$"):
                process.sample()

    def test_replacement_between_identity_and_memory_reads_refuses_the_sample(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root)
            read = resources.read_bounded

            def replace_after_status(descriptor, name, maximum):
                value = read(descriptor, name, maximum)
                if name == "status":
                    self.fixture(root, start=10)
                return value

            with patch.object(resources, "read_bounded", side_effect=replace_after_status):
                with self.assertRaisesRegex(ResourceError, "^resource_process_identity$"):
                    ProcessMemory(123, proc_root=root).sample()

    def test_symlinked_measurement_and_process_directory_refuse(self):
        for mutation in ("status", "directory"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                process = self.fixture(root)
                if mutation == "status":
                    (process / "status").rename(process / "alternate")
                    (process / "status").symlink_to("alternate")
                else:
                    process.rename(root / "alternate")
                    process.symlink_to(root / "alternate", target_is_directory=True)
                with self.assertRaises(ResourceError):
                    ProcessMemory(123, proc_root=root).sample()

    def test_named_inventory_is_private_complete_and_cannot_double_count_one_process(self):
        roles = {"controller": 123, "server": 124, "load": 125, "observer": 126}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for pid in roles.values():
                self.fixture(root, pid=pid)
            value = PersistentProcessMemory(roles, proc_root=root).sample(5)
            self.assertEqual(value["processes"].keys(), roles.keys())
            self.assertIs(value["qualifies_resource_model"], False)
            self.assertEqual(value["scope"], "four_persistent_processes_excluding_transient_cli_children")
            self.assertNotIn("pid", repr(value))
            self.assertNotIn(str(root), repr(value))
            for invalid in ({"server": 124}, dict(roles, server=123), dict(roles, server=True)):
                with self.assertRaises(ResourceError):
                    PersistentProcessMemory(invalid, proc_root=root)
            (root / "126" / "status").unlink()
            with self.assertRaises(ResourceError):
                PersistentProcessMemory(roles, proc_root=root).sample(6)

    def test_sample_file_records_real_observations_and_refuses_missing_window_coverage(self):
        roles = {"controller": 123, "server": 124, "load": 125, "observer": 126}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for pid in roles.values():
                self.fixture(root, pid=pid)
            with PreparationMemorySamples(root, roles, 900, proc_root=root) as samples:
                samples.poll(0)
                samples.poll(1000000000)
                samples.poll(600000000000)
                with self.assertRaisesRegex(ResourceError, "^resource_sample_coverage$"):
                    samples.finish()
                with self.assertRaisesRegex(ResourceError, "^resource_sample_clock$"):
                    samples.poll(300000000000)
            records = [json.loads(line) for line in samples.path.read_bytes().splitlines()]
            self.assertEqual([record["elapsed_ns"] for record in records], [0, 600000000000])
            self.assertEqual(samples.path.stat().st_mode & 0o777, 0o600)
            with self.assertRaises(FileExistsError):
                with PreparationMemorySamples(root, roles, 900, proc_root=root):
                    pass

    def test_extended_preparation_requires_all_five_memory_windows(self):
        roles = {"controller": 123, "server": 124, "load": 125, "observer": 126}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for pid in roles.values():
                self.fixture(root, pid=pid)
            with PreparationMemorySamples(root, roles, 1500, proc_root=root) as samples:
                for elapsed in (0, 300000000000, 600000000000, 900000000000):
                    samples.poll(elapsed)
                with self.assertRaisesRegex(ResourceError, "^resource_sample_coverage$"):
                    samples.finish()
                samples.poll(1200000000000)
                samples.finish()
                samples.poll(1500000000000)
            self.assertEqual(len(samples.path.read_bytes().splitlines()), 5)
            for duration in (1501, 86400, True):
                with self.subTest(duration=duration):
                    with self.assertRaisesRegex(ResourceError, "^resource_sample_bound$"):
                        PreparationMemorySamples(root, roles, duration, proc_root=root)

    def test_complete_coverage_still_records_no_sample_for_a_dead_process(self):
        roles = {"controller": 123, "server": 124, "load": 125, "observer": 126}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for pid in roles.values():
                self.fixture(root, pid=pid)
            with PreparationMemorySamples(root, roles, 900, proc_root=root) as samples:
                for elapsed in (0, 300000000000, 600000000000):
                    samples.poll(elapsed)
                samples.finish()
                (root / "125" / "status").unlink()
                with self.assertRaises(ResourceError):
                    samples.poll(605000000000)
            self.assertEqual(len(samples.path.read_bytes().splitlines()), 3)

    def test_absent_zombie_duplicate_unbounded_and_partial_measurements_refuse(self):
        mutations = ("absent", "zombie", "duplicate", "wrong_pid", "missing_virtual",
                     "wrong_unit", "overflow", "negative", "inconsistent", "oversize")
        for mutation in mutations:
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                process = self.fixture(root)
                status = process / "status"
                if mutation == "absent":
                    status.unlink()
                elif mutation == "zombie":
                    self.fixture(root, state="Z")
                elif mutation == "duplicate":
                    status.write_text(status.read_text() + "VmRSS:\t1 kB\n")
                elif mutation == "wrong_pid":
                    status.write_text(status.read_text().replace("Pid:\t123", "Pid:\t124"))
                elif mutation == "missing_virtual":
                    status.write_text(status.read_text().replace("VmSize:\t101 kB\n", ""))
                elif mutation == "wrong_unit":
                    status.write_text(status.read_text().replace("13 kB", "13 MB"))
                elif mutation == "overflow":
                    self.fixture(root, virtual=str(2**64))
                elif mutation == "negative":
                    self.fixture(root, rss="-1")
                elif mutation == "inconsistent":
                    self.fixture(root, virtual="12")
                else:
                    status.write_bytes(b"x" * 65537)
                with self.assertRaises(ResourceError):
                    ProcessMemory(123, proc_root=root).sample()


if __name__ == "__main__":
    unittest.main()
