# req: END-007, END-008, END-010
"""A failed native diagnostic check bounds its log and cancels its owned tree."""
from importlib.machinery import SourceFileLoader
from importlib.util import module_from_spec, spec_from_loader
import os
from pathlib import Path
import select
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
loader = SourceFileLoader("native_byte_boundary_check",
                         str(ROOT / "scripts/check-ticketdesk-lifecycle-native-bytes"))
checker = module_from_spec(spec_from_loader(loader.name, loader))
loader.exec_module(checker)


@unittest.skipUnless(hasattr(os, "pidfd_open"), "Linux pidfds required")
class NativeByteCancellation(unittest.TestCase):
    def exercise(self, *, timeout):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fifo = root / "ready"
            os.mkfifo(fifo, 0o600)
            ready = os.open(fifo, os.O_RDONLY | os.O_NONBLOCK)
            pidfds = []
            pids = []
            clock_calls = 0
            real_clock = time.monotonic
            started = None

            def clock():
                nonlocal clock_calls, started
                if clock_calls == 0:
                    self.assertTrue(select.select([ready], [], [], 5)[0])
                    encoded = os.read(ready, 128)
                    self.assertTrue(encoded.endswith(b"\n"))
                    pids.extend(int(value) for value in encoded.split())
                    self.assertEqual(len(pids), 2)
                    pidfds.extend(os.pidfd_open(pid, 0) for pid in pids)
                    started = real_clock()
                clock_calls += 1
                return started + 601 if timeout and clock_calls > 1 else real_clock()

            code = """
import os,signal,sys
child=os.fork()
if child==0:
    signal.pause()
    os._exit(0)
with open(sys.argv[1],'w') as ready:
    ready.write(str(os.getpid())+' '+str(child)+'\\n')
    ready.flush()
if sys.argv[2]=='overflow':
    os.write(1,b'x'*4096)
signal.pause()
"""
            try:
                with (root / "log").open("xb") as output:
                    with patch.object(checker, "MAX_LOG_BYTES", 64), \
                         patch.object(checker.time, "monotonic", side_effect=clock):
                        with self.assertRaisesRegex(ValueError,
                                "native_byte_fixture_timeout" if timeout else "native_byte_log_bound"):
                            checker.run_fixture([sys.executable, "-c", code, str(fifo),
                                "timeout" if timeout else "overflow"], dict(os.environ), output)
                self.assertEqual((root / "log").stat().st_size, 0)
                pending = set(pidfds)
                deadline = real_clock() + 5
                while pending:
                    remaining = deadline - real_clock()
                    self.assertGreater(remaining, 0)
                    exited, _, _ = select.select(list(pending), [], [], remaining)
                    self.assertTrue(exited)
                    pending.difference_update(exited)
                with self.assertRaises(ChildProcessError):
                    os.waitpid(pids[0], os.WNOHANG)
            finally:
                os.close(ready)
                for descriptor in pidfds:
                    os.close(descriptor)

    def test_log_refusal_precedes_growth_and_kills_and_joins_the_owned_group(self):
        self.exercise(timeout=False)

    def test_elapsed_bound_cancels_the_actual_owned_group_after_ready(self):
        self.exercise(timeout=True)


if __name__ == "__main__":
    unittest.main()
