# req: END-007, END-008, END-010
"""Every observed descendant must have one known live process owner."""
import json
import os
from pathlib import Path
import sys
import select
import subprocess
import tempfile
import threading
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from lib import ticketdesk_lifecycle_resources as resources
from lib.ticketdesk_lifecycle_resources import ResourceError


class OwnedProcessObservations(unittest.TestCase):
    roles = {"controller": 123, "server": 124, "load": 125, "observer": 126,
             "native_sampler": 127}

    def fixture(self, root, pid, *, children=(), start=9):
        process = root / str(pid)
        (process / "task" / str(pid)).mkdir(parents=True, exist_ok=True)
        (process / "task" / str(pid) / "children").write_text(" ".join(map(str, children)))
        fields = ["S"] + ["0"] * 18 + [str(start)] + ["0"] * 30
        (process / "stat").write_text(f"{pid} (private worker) " + " ".join(fields) + "\n")
        (process / "status").write_text(f"State:\tS\nPid:\t{pid}\n"
            "VmRSS:\t13 kB\nVmSize:\t101 kB\nVmHWM:\t19 kB\nVmPeak:\t127 kB\n")
        return process

    def tree(self, root):
        for pid in self.roles.values():
            self.fixture(root, pid, children=tuple(self.roles.values())[1:] if pid == 123 else ())

    def test_complete_tree_reports_current_and_lifetime_peaks_without_process_identities(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.tree(root)
            sample = resources.OwnedProcessMemory(self.roles, proc_root=root).sample(5)
            self.assertEqual(sample["processes"].keys(), self.roles.keys())
            self.assertEqual(sample["rss_bytes"], 5 * 13 * 1024)
            self.assertEqual(sample["allocator_bytes"], 5 * 101 * 1024)
            self.assertEqual(sample["rss_peak_upper_bound_bytes"], 5 * 19 * 1024)
            self.assertEqual(sample["allocator_peak_upper_bound_bytes"], 5 * 127 * 1024)
            self.assertEqual(sample["rss_peak_basis"], "maximum_observed_asynchronous_kernel_high_water_counter")
            for value in sample["processes"].values():
                self.assertEqual(value["reported_rss_peak_bytes"],19*1024)
                self.assertEqual(value["later_reported_rss_peak_bytes"],19*1024)
            self.assertEqual(sample["scope"], "verified_owned_descendant_tree_at_sample")
            self.assertNotIn('"pid"', json.dumps(sample))
            self.assertNotIn(str(root), json.dumps(sample))
            self.assertNotIn("qualifies_resource_model", sample)

    def test_uninventoried_transient_cli_refuses_instead_of_disappearing_from_totals(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.tree(root)
            self.fixture(root, 123, children=tuple(self.roles.values())[1:] + (128,))
            with self.assertRaisesRegex(ResourceError, "^resource_process_inventory$"):
                resources.OwnedProcessMemory(self.roles, proc_root=root).sample(5)

    def test_asynchronous_rss_high_water_retains_raw_readings_and_running_maximum(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            process=self.fixture(root,123)
            owner=resources.ProcessMemory(123,proc_root=root)
            first,_=owner.tree_sample()
            self.assertEqual(first["rss_peak_bytes"],19*1024)
            status=process/"status"
            status.write_text(status.read_text().replace("VmHWM:\t19 kB","VmHWM:\t18 kB"))
            second,_=owner.tree_sample()
            self.assertEqual(second["rss_peak_bytes"],19*1024)
            self.assertEqual(second["reported_rss_peak_bytes"],18*1024)
            self.assertEqual(second["reported_allocator_peak_bytes"],127*1024)
            self.assertEqual(second["rss_bytes"],13*1024)

    def test_virtual_high_water_rewind_still_refuses_even_above_current_size(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            process=self.fixture(root,123)
            owner=resources.ProcessMemory(123,proc_root=root)
            owner.tree_sample()
            status=process/"status"
            status.write_text(status.read_text().replace("VmPeak:\t127 kB","VmPeak:\t126 kB"))
            with self.assertRaises(ResourceError):owner.tree_sample()

    def test_children_from_another_controller_thread_are_also_inventoried(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.tree(root)
            thread = root / "123/task/200"
            thread.mkdir()
            (thread / "children").write_text("128")
            with self.assertRaisesRegex(ResourceError, "^resource_process_inventory$"):
                resources.OwnedProcessMemory(self.roles, proc_root=root).sample(5)

    def test_disconnected_duplicate_or_cyclic_process_ownership_refuses(self):
        for mutation in ("disconnected", "duplicate", "cycle"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.tree(root)
                if mutation == "disconnected":
                    self.fixture(root, 123, children=(124, 125, 126))
                elif mutation == "duplicate":
                    self.fixture(root, 124, children=(127,))
                else:
                    self.fixture(root, 124, children=(123,))
                with self.assertRaisesRegex(ResourceError, "^resource_process_inventory$"):
                    resources.OwnedProcessMemory(self.roles, proc_root=root).sample(5)

    def test_recycled_process_missing_peak_and_peak_below_live_memory_refuse(self):
        for mutation in ("recycled", "missing", "below"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.tree(root)
                owner = resources.OwnedProcessMemory(self.roles, proc_root=root)
                owner.sample(5)
                if mutation == "recycled":
                    self.fixture(root, 127, start=10)
                else:
                    status = root / "127/status"
                    status.write_text(status.read_text().replace("VmPeak:\t127 kB\n",
                        "" if mutation == "missing" else "VmPeak:\t100 kB\n"))
                with self.assertRaises(ResourceError):
                    owner.sample(6)

    def test_child_creation_during_measurement_refuses_the_whole_sample(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.tree(root)
            original = resources.read_bounded

            def create_child(descriptor, name, maximum):
                value = original(descriptor, name, maximum)
                if name == "status":
                    self.fixture(root, 123, children=tuple(self.roles.values())[1:] + (128,))
                return value

            with patch.object(resources, "read_bounded", side_effect=create_child):
                with self.assertRaises(ResourceError):
                    resources.OwnedProcessMemory(self.roles, proc_root=root).sample(5)

    def test_real_linux_child_tree_uses_lifetime_peak_counters(self):
        children = []
        try:
            for _ in range(3):
                child = subprocess.Popen([sys.executable, "-B", "-c",
                    'import os,sys; os.write(1,b"ready\\n"); sys.stdin.buffer.read(1)'],
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
                children.append(child)
                self.assertTrue(select.select([child.stdout], [], [], 5)[0])
                self.assertEqual(os.read(child.stdout.fileno(), 6), b"ready\n")
            roles = dict(zip(("server", "load", "observer"), (child.pid for child in children)))
            roles["controller"] = os.getpid()
            sample = resources.OwnedProcessMemory(roles).sample(0)
            self.assertGreater(sample["rss_bytes"], 0)
            self.assertGreaterEqual(sample["allocator_peak_upper_bound_bytes"], sample["allocator_bytes"])
            self.assertGreaterEqual(sample["rss_peak_upper_bound_bytes"], sample["rss_bytes"])
        finally:
            for child in children:
                child.stdin.close()
                try:
                    child.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait(timeout=5)
                child.stdout.close()

    def test_thread_exit_during_task_open_restarts_the_complete_live_process_census(self):
        release = threading.Event()
        ready = threading.Event()
        identity = []

        def worker():
            identity.append(threading.get_native_id())
            ready.set()
            self.assertTrue(release.wait(5))

        thread = threading.Thread(target=worker)
        thread.start()
        self.assertTrue(ready.wait(5))
        original = os.open
        retired = False

        def retire_before_open(path, flags, *args, **kwargs):
            nonlocal retired
            if path == str(identity[0]) and not retired:
                retired = True
                release.set()
                thread.join(5)
                self.assertFalse(thread.is_alive())
                # Python join observes its thread lock before the kernel has
                # necessarily removed the task. Synchronize on actual task
                # retirement before injecting the failed directory open.
                task = Path("/proc") / str(os.getpid()) / "task" / str(identity[0])
                for _ in range(100_000):
                    if not task.exists():
                        break
                    os.sched_yield()
                else:
                    self.fail("the joined native task never retired")
            return original(path, flags, *args, **kwargs)

        try:
            with patch.object(resources.os, "open", side_effect=retire_before_open):
                sample, children = resources.ProcessMemory(os.getpid()).tree_sample()
            self.assertTrue(retired)
            self.assertEqual(sample["task_census_restarts"], 1)
            self.assertEqual(children, set())
            self.assertGreater(sample["rss_bytes"], 0)
        finally:
            release.set()
            thread.join(5)

    def test_missing_children_file_on_a_live_thread_is_not_retried(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            process = self.fixture(root, 123)
            (process / "task/123/children").unlink()
            owner = resources.ProcessMemory(123, proc_root=root)
            with self.assertRaisesRegex(ResourceError, "^resource_process_unavailable$"):
                owner.tree_sample()
            self.assertEqual(owner.task_census_restarts, 0)

    def test_census_restarts_are_bounded_and_do_not_discard_observed_children_or_identity(self):
        for mutation in ("churn", "lost_child", "replacement"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.fixture(root, 123)
                calls = []

                def changed(_directory):
                    calls.append(1)
                    if mutation == "churn" or len(calls) == 1:
                        if mutation == "replacement":
                            self.fixture(root, 123, start=10)
                        raise resources._TaskCensusChanged({128} if mutation == "lost_child" else set())
                    return set()

                owner = resources.ProcessMemory(123, proc_root=root)
                with patch.object(resources, "thread_children", side_effect=changed):
                    with self.assertRaises(ResourceError):
                        owner.tree_sample()
                self.assertLessEqual(owner.task_census_restarts, 3)
                if mutation == "churn":
                    self.assertEqual(len(calls), 3)

    def test_second_task_census_retirement_preserves_first_census_child_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root, 123)
            calls = iter(({128}, resources._TaskCensusChanged(set()), set(), set()))

            def census(_directory):
                value = next(calls)
                if isinstance(value, Exception):
                    raise value
                return value

            with patch.object(resources, "thread_children", side_effect=census):
                with self.assertRaisesRegex(ResourceError, "^resource_process_inventory$"):
                    resources.ProcessMemory(123, proc_root=root).tree_sample()

    def test_census_restart_preserves_high_water_read_before_the_retiring_thread(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            process = self.fixture(root, 123)
            calls = 0

            def census(_directory):
                nonlocal calls
                calls += 1
                if calls == 2:
                    status = process / "status"
                    status.write_text(status.read_text().replace("VmHWM:\t19 kB", "VmHWM:\t18 kB"))
                    raise resources._TaskCensusChanged(set())
                return set()

            with patch.object(resources, "thread_children", side_effect=census):
                sample, _children = resources.ProcessMemory(123, proc_root=root).tree_sample()
            self.assertEqual(sample["rss_peak_bytes"], 19 * 1024)
            self.assertEqual(sample["reported_rss_peak_bytes"], 18 * 1024)
            self.assertEqual(sample["task_census_restarts"], 1)


if __name__ == "__main__":
    unittest.main()
