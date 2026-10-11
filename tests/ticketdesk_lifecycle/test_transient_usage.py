# req: END-007, END-008, END-010
"""Exited children must retain per-child usage, including between-sample peaks."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'scripts'))
from lib import ticketdesk_lifecycle_transient_usage as resources


class TransientUsage(unittest.TestCase):
    def test_real_child_exit_preserves_kernel_peak_without_command_or_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            with (root/'stdout').open('xb') as output,(root/'stderr').open('xb') as errors:
                result=resources.run_transient_observed([sys.executable,'-c',
                    "a=bytearray(32*1024*1024); print('completed',flush=True)"],
                    output=output,errors=errors,environment=None,timeout=10,
                    observation=root/'usage.json')
            value=json.loads((root/'usage.json').read_bytes())
            self.assertEqual(result,0)
            self.assertEqual(value['exit_code'],0)
            self.assertGreaterEqual(value['exit_rss_peak_bytes'],32*1024*1024)
            self.assertEqual(value['exit_rss_basis'],'linux_wait4_per_child_ru_maxrss')
            self.assertTrue(value['launch_observed']);self.assertTrue(value['exit_observed'])
            self.assertFalse(value['qualifies_complete_resource_model'])
            self.assertIsNone(value['exit_allocator_peak_bytes'])
            self.assertNotIn('pid',json.dumps(value));self.assertNotIn(str(root),json.dumps(value))
            self.assertEqual((root/'usage.json').stat().st_mode & 0o777,0o600)

    def test_enforced_ceiling_survives_an_unsampled_child_exit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with (root/'stdout').open('xb') as output, (root/'stderr').open('xb') as errors:
                result = resources.run_transient_observed([sys.executable, '-c',
                    "a=bytearray(32*1024*1024); print('completed',flush=True)"],
                    output=output, errors=errors, environment=None, timeout=10,
                    observation=root/'usage.json', address_space_limit_bytes=256*1024*1024)
            value = json.loads((root/'usage.json').read_bytes())
            self.assertEqual(result, 0)
            self.assertEqual(value['worker_exit_allocator_peak_upper_bound_bytes'], 256*1024*1024)
            self.assertEqual(value['allocator_basis'], 'linux_hard_rlimit_as_upper_bound')
            self.assertIsNone(value['exit_allocator_peak_bytes'])
            self.assertFalse(value['qualifies_complete_resource_model'])

    def test_unconstrained_parent_does_not_invent_a_full_lifetime_allocator_bound(self):
        import resource
        inherited = resource.getrlimit(resource.RLIMIT_AS)[1]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with (root/'stdout').open('xb') as output, (root/'stderr').open('xb') as errors:
                result = resources.run_transient_observed([sys.executable,'-c','a=bytearray(16*1024*1024)'],
                    output=output,errors=errors,environment=None,timeout=10,
                    observation=root/'usage.json',address_space_limit_bytes=128*1024*1024)
            self.assertEqual(result,0)
            value = json.loads((root/'usage.json').read_bytes())
            expected = None if inherited == resource.RLIM_INFINITY else inherited
            self.assertEqual(value['exit_allocator_peak_upper_bound_bytes'],expected)
            self.assertFalse(value['qualifies_complete_resource_model'])

    def test_timeout_preserves_the_joined_child_exit_and_remains_a_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            with (root/'stdout').open('xb') as output,(root/'stderr').open('xb') as errors:
                with self.assertRaises(subprocess.TimeoutExpired):
                    resources.run_transient_observed([sys.executable,'-c',
                        "import signal; print('ready',flush=True); signal.pause()"],
                        output=output,errors=errors,environment=None,timeout=.5,
                        observation=root/'usage.json')
            value=json.loads((root/'usage.json').read_bytes())
            self.assertEqual((root/'stdout').read_text(),'ready\n')
            self.assertTrue(value['timed_out']);self.assertTrue(value['exit_observed'])
            self.assertEqual(value['exit_code'],-9)

    def test_observation_collision_refuses_before_launch(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);(root/'usage.json').write_text('existing evidence')
            with (root/'stdout').open('xb') as output,(root/'stderr').open('xb') as errors:
                with self.assertRaises(FileExistsError):
                    resources.run_transient_observed([sys.executable,'-c',"print('launched')"],
                        output=output,errors=errors,environment=None,timeout=10,
                        observation=root/'usage.json')
            self.assertEqual((root/'stdout').read_bytes(),b'')
            self.assertEqual((root/'usage.json').read_text(),'existing evidence')

    def test_invalid_exit_usage_still_records_reaping_and_never_signals_the_reaped_pid(self):
        actual=resources.os.wait4
        def invalid_usage(pid,options):
            found,status,usage=actual(pid,options)
            return found,status,SimpleNamespace(ru_maxrss=True) if found else usage
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            with (root/'stdout').open('xb') as output,(root/'stderr').open('xb') as errors,\
                    patch.object(resources.os,'wait4',side_effect=invalid_usage),\
                    patch.object(resources.signal,'pidfd_send_signal',side_effect=AssertionError('reaped')):
                with self.assertRaisesRegex(resources.ResourceError,'^process_usage_exit$'):
                    resources.run_transient_observed([sys.executable,'-c','raise SystemExit(7)'],
                        output=output,errors=errors,environment=None,timeout=10,
                        observation=root/'usage.json')
            value=json.loads((root/'usage.json').read_bytes())
            self.assertTrue(value['exit_observed']);self.assertEqual(value['exit_code'],7)
            self.assertIsNone(value['exit_rss_peak_bytes'])

    def test_failed_launch_remains_unknown_and_retains_no_command_text(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            with (root/'stdout').open('xb') as output,(root/'stderr').open('xb') as errors:
                with self.assertRaises(FileNotFoundError):
                    resources.run_transient_observed([str(root/'missing-executable')],
                        output=output,errors=errors,environment=None,timeout=10,
                        observation=root/'usage.json')
            value=json.loads((root/'usage.json').read_bytes())
            self.assertFalse(value['launch_observed']);self.assertFalse(value['exit_observed'])
            self.assertIsNone(value['exit_code']);self.assertIsNone(value['exit_rss_peak_bytes'])
            self.assertNotIn(str(root),json.dumps(value))

    def test_descriptor_admission_failure_still_joins_the_owned_child(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            with (root/'stdout').open('xb') as output,(root/'stderr').open('xb') as errors,\
                    patch.object(resources.os,'pidfd_open',side_effect=OSError('isolated admission refusal')):
                with self.assertRaises(OSError):
                    resources.run_transient_observed([sys.executable,'-c','import signal; signal.pause()'],
                        output=output,errors=errors,environment=None,timeout=10,
                        observation=root/'usage.json')
            value=json.loads((root/'usage.json').read_bytes())
            self.assertTrue(value['launch_observed']);self.assertTrue(value['exit_observed'])
            self.assertEqual(value['exit_code'],-9);self.assertIsNone(value['exit_rss_peak_bytes'])


if __name__=='__main__':unittest.main()
