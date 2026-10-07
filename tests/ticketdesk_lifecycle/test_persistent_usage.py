# req: END-007, END-008, END-010
"""Polling and restart must preserve each exact owned child's exit usage."""
import json
from pathlib import Path
import select
import signal
import subprocess
import sys
import tempfile
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'scripts'))
from lib import ticketdesk_lifecycle_persistent_usage as resources


class PersistentUsage(unittest.TestCase):
    def test_poll_reaps_peak_once_and_repeated_wait_retains_the_same_record(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'usage.json'
            child = resources.ObservedProcess([sys.executable, '-c',
                'a=bytearray(32*1024*1024)'], observation=path,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            try:
                self.assertTrue(select.select([child.pidfd], [], [], 10)[0])
                self.assertEqual(child.poll(), 0)
                encoded = path.read_bytes()
                self.assertEqual(child.wait(timeout=1), 0)
                self.assertEqual(child.poll(), 0)
                self.assertEqual(path.read_bytes(), encoded)
                value = json.loads(encoded)
                self.assertGreaterEqual(value['exit_rss_peak_bytes'], 32*1024*1024)
                self.assertTrue(value['launch_observed']); self.assertTrue(value['exit_observed'])
                self.assertIsNone(value['exit_allocator_peak_bytes'])
                self.assertFalse(value['qualifies_complete_resource_model'])
                self.assertNotIn('pid', encoded.decode()); self.assertNotIn(str(path), encoded.decode())
                self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            finally:
                if child.returncode is None:
                    child.kill(); child.wait(timeout=5)

    def test_limit_is_inherited_and_cannot_be_raised_before_exact_kill(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'usage.json'
            limit = 128*1024*1024
            code = """import os,resource,sys
assert resource.getrlimit(resource.RLIMIT_AS) == (134217728,134217728)
try:
    resource.setrlimit(resource.RLIMIT_AS,(268435456,268435456))
    raise AssertionError('hard limit raised')
except ValueError:
    pass
try:
    bytearray(256*1024*1024)
    raise AssertionError('allocation admitted')
except MemoryError:
    pass
print('refused',os.getpid(),flush=True)
sys.stdin.buffer.read()
"""
            child = resources.ObservedProcess([sys.executable,'-c',code], observation=path,
                address_space_limit_bytes=limit, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
            try:
                self.assertTrue(select.select([child.stdout],[],[],10)[0])
                self.assertEqual(child.stdout.readline(),f'refused {child.pid}\n'.encode())
                before = json.loads(path.read_bytes())
                self.assertTrue(before['address_space_proof']['enforced'])
                self.assertIsNone(before['exit_allocator_peak_upper_bound_bytes'])
                child.kill();self.assertEqual(child.wait(timeout=5),-9)
                value = json.loads(path.read_bytes())
                self.assertEqual(value['worker_exit_allocator_peak_upper_bound_bytes'],limit)
                self.assertIsNone(value['exit_allocator_peak_bytes'])
                self.assertFalse(value['qualifies_complete_resource_model'])
            finally:
                if child.returncode is None: child.kill();child.wait(timeout=5)
                child.stdin.close();child.stdout.close();child.stderr.close()

    def test_restarts_retain_distinct_kernel_bounds_and_exit_records(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            limits = (128*1024*1024,256*1024*1024)
            for index,limit in enumerate(limits):
                child = resources.ObservedProcess([sys.executable,'-c','a=bytearray(16*1024*1024)'],
                    observation=root/f'{index}.json',address_space_limit_bytes=limit)
                self.assertEqual(child.wait(timeout=10),0)
            values = [json.loads((root/f'{index}.json').read_bytes()) for index in range(2)]
            self.assertEqual(tuple(value['worker_exit_allocator_peak_upper_bound_bytes'] for value in values),limits)
            self.assertTrue(all(value['exit_rss_peak_bytes'] >= 16*1024*1024 for value in values))

    def test_missing_launch_proof_fails_and_joins_the_owned_child(self):
        from lib.ticketdesk_lifecycle_address_space import AddressSpaceLaunch
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'usage.json'
            with patch.object(AddressSpaceLaunch,'confirm',side_effect=resources.ResourceError('process_address_space_proof')):
                with self.assertRaisesRegex(resources.ResourceError,'^process_address_space_proof$'):
                    resources.ObservedProcess([sys.executable,'-c','import sys;sys.stdin.buffer.read()'],
                        observation=path,address_space_limit_bytes=128*1024*1024,stdin=subprocess.PIPE)
            value = json.loads(path.read_bytes())
            self.assertTrue(value['exit_observed'])
            self.assertIsNone(value['exit_allocator_peak_upper_bound_bytes'])
            self.assertIsNone(value['address_space_proof'])

    def test_finite_inherited_limit_covers_child_birth_before_the_launcher(self):
        scripts = str(Path(__file__).resolve().parents[2]/'scripts')
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            code = """import sys
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from lib.ticketdesk_lifecycle_persistent_usage import ObservedProcess
child = ObservedProcess([sys.executable,'-c','a=bytearray(8*1024*1024)'],
    observation=sys.argv[2],address_space_limit_bytes=128*1024*1024)
assert child.wait(timeout=10) == 0
print(Path(sys.argv[2]).read_text(),end='')
"""
            child = resources.ObservedProcess([sys.executable,'-c',code,scripts,
                str(root/'inner.json')], observation=root/'outer.json',
                address_space_limit_bytes=256*1024*1024,
                stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            try:
                self.assertEqual(child.wait(timeout=10),0,child.stderr.read().decode())
                value = json.loads(child.stdout.read())
                self.assertEqual(value['exit_allocator_peak_upper_bound_bytes'],256*1024*1024)
                self.assertEqual(value['worker_exit_allocator_peak_upper_bound_bytes'],128*1024*1024)
                self.assertEqual(value['address_space_proof']['inherited_hard_limit_bytes'],256*1024*1024)
                self.assertIsNone(value['exit_allocator_peak_bytes'])
                self.assertFalse(value['qualifies_complete_resource_model'])
            finally:
                if child.returncode is None: child.kill();child.wait(timeout=5)
                child.stdout.close();child.stderr.close()

    def test_timeout_keeps_child_owned_then_exact_kill_records_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'usage.json'
            child = resources.ObservedProcess([sys.executable, '-c',
                'import sys; print("ready",flush=True); sys.stdin.buffer.read()'],
                observation=path, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
            try:
                self.assertTrue(select.select([child.stdout], [], [], 10)[0])
                self.assertEqual(child.stdout.readline(), b'ready\n')
                with self.assertRaises(subprocess.TimeoutExpired): child.wait(timeout=.01)
                self.assertIsNone(child.returncode)
                child.kill(); self.assertEqual(child.wait(timeout=5), -signal.SIGKILL)
                value = json.loads(path.read_bytes())
                self.assertTrue(value['exit_observed']); self.assertEqual(value['exit_code'], -9)
            finally:
                if child.returncode is None: child.kill(); child.wait(timeout=5)
                child.stdin.close(); child.stdout.close()

    def test_two_incarnations_have_independent_exit_records(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for index, code in enumerate((0, 7)):
                child = resources.ObservedProcess([sys.executable, '-c',
                    f'raise SystemExit({code})'], observation=root / f'{index}.json')
                self.assertEqual(child.wait(timeout=10), code)
            self.assertEqual([json.loads((root/f'{index}.json').read_bytes())['exit_code']
                              for index in range(2)], [0, 7])

    def test_waiting_thread_does_not_block_exact_signal_or_double_reap(self):
        actual = resources.select.select
        entered = threading.Event()
        def waiting(*arguments):
            entered.set()
            return actual(*arguments)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'usage.json'
            child = resources.ObservedProcess([sys.executable, '-c',
                'import sys; sys.stdin.buffer.read()'], observation=path, stdin=subprocess.PIPE)
            results = []; failures = []
            def join():
                try: results.append(child.wait(timeout=10))
                except BaseException as error: failures.append(error)
            waiter = threading.Thread(target=join)
            try:
                with patch.object(resources.select, 'select', side_effect=waiting):
                    waiter.start(); self.assertTrue(entered.wait(5))
                    child.kill(); child.wait(timeout=5); waiter.join(timeout=5)
                self.assertFalse(waiter.is_alive()); self.assertEqual(failures, [])
                self.assertEqual(results, [-9]); self.assertEqual(child.poll(), -9)
                self.assertTrue(json.loads(path.read_bytes())['exit_observed'])
            finally:
                if child.returncode is None: child.kill(); child.wait(timeout=5)
                waiter.join(timeout=5); child.stdin.close()

    def test_invalid_usage_records_reaped_exit_and_never_signals_again(self):
        actual = resources.os.wait4
        def invalid(pid, options):
            found, status, usage = actual(pid, options)
            return found, status, SimpleNamespace(ru_maxrss=True) if found else usage
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'usage.json'
            child = resources.ObservedProcess([sys.executable, '-c', 'raise SystemExit(7)'],
                                              observation=path)
            with patch.object(resources.os, 'wait4', side_effect=invalid), \
                    patch.object(resources.signal, 'pidfd_send_signal', side_effect=AssertionError('reaped')):
                with self.assertRaisesRegex(resources.ResourceError, '^process_usage_exit$'):
                    child.wait(timeout=10)
                child.kill(); self.assertEqual(child.poll(), 7)
            value = json.loads(path.read_bytes())
            self.assertTrue(value['exit_observed']); self.assertIsNone(value['exit_rss_peak_bytes'])

    def test_failed_launch_and_evidence_collision_never_invent_exit_usage(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); path = root / 'usage.json'
            with self.assertRaises(FileNotFoundError):
                resources.ObservedProcess([str(root/'missing')], observation=path)
            value = json.loads(path.read_bytes())
            self.assertFalse(value['launch_observed']); self.assertFalse(value['exit_observed'])
            self.assertIsNone(value['exit_rss_peak_bytes'])
            with patch.object(resources.subprocess, 'Popen', side_effect=AssertionError('launch')):
                with self.assertRaises(FileExistsError):
                    resources.ObservedProcess(['unused'], observation=path)

    def test_pidfd_admission_failure_joins_the_owned_child_and_records_unknown_peak(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'usage.json'
            with patch.object(resources.os, 'pidfd_open', side_effect=OSError('admission')):
                with self.assertRaises(OSError):
                    resources.ObservedProcess([sys.executable, '-c',
                        'import sys; sys.stdin.buffer.read()'], observation=path,
                        stdin=subprocess.PIPE)
            value = json.loads(path.read_bytes())
            self.assertTrue(value['exit_observed']); self.assertEqual(value['exit_code'], -9)
            self.assertIsNone(value['exit_rss_peak_bytes'])


if __name__ == '__main__': unittest.main()
