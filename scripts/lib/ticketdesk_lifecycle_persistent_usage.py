# req: END-007, END-008, END-010
"""Exact owned persistent child, with wait4 accounting before any poll can reap.

This is an observation wrapper, not complete descendant or allocator evidence.
Every restart needs a distinct owner and exclusive evidence file.
"""
from contextlib import nullcontext
import json
import os
from pathlib import Path
import select
import signal
import subprocess
import threading
import time

from .ticketdesk_lifecycle_resources import MAX_U64, ProcessMemory, ResourceError, require


from .ticketdesk_lifecycle_address_space import AddressSpaceLaunch, BASIS


class ObservedProcess:
    def __init__(self, arguments, *, observation, address_space_limit_bytes=None, **options):
        descriptor = os.open(Path(observation), os.O_WRONLY | os.O_CREAT | os.O_EXCL
                             | os.O_NOFOLLOW, 0o600)
        self._evidence = os.fdopen(descriptor, 'w+b')
        self._lock = threading.Lock()
        self._child = None
        self.pidfd = None
        self._started = time.monotonic_ns()
        self._value = dict(schema='riffdb.ticketdesk-lifecycle-persistent-usage/v1',
            launch_observed=False, exit_observed=False, exit_code=None, elapsed_ns=None,
            launch_memory=None, launch_memory_refusal=None, exit_rss_peak_bytes=None,
            exit_allocator_peak_bytes=None, exit_allocator_peak_upper_bound_bytes=None,
            worker_exit_allocator_peak_upper_bound_bytes=None, address_space_proof=None, exit_rss_basis='linux_wait4_per_child_ru_maxrss',
            allocator_basis='unknown_at_exit', qualifies_complete_resource_model=False)
        try:
            launch = (AddressSpaceLaunch(arguments, address_space_limit_bytes)
                      if address_space_limit_bytes is not None else None)
            with launch if launch is not None else nullcontext():
                self._child = subprocess.Popen(launch.arguments if launch else arguments,
                    **(launch.options(options) if launch else options))
                self._value['launch_observed'] = True
                self.pidfd = os.pidfd_open(self._child.pid, 0)
                if launch:
                    self._value['address_space_proof'] = launch.confirm()
                    self._value['allocator_basis'] = BASIS
            try:
                memory = ProcessMemory(self._child.pid).tree_sample()[0]
                self._value['launch_memory'] = {field: memory[field] for field in
                    ('rss_bytes', 'allocator_bytes', 'reported_rss_peak_bytes',
                     'reported_allocator_peak_bytes')}
            except ResourceError as error:
                self._value['launch_memory_refusal'] = str(error)
            self._publish()
        except BaseException:
            try:
                if self._child is not None:
                    if self.pidfd is None:
                        self._child.kill(); self._child.wait(timeout=5)
                        self._value.update(exit_observed=True, exit_code=self._child.returncode)
                    else:
                        self.kill(); self.wait(timeout=5)
            finally:
                if self._evidence is not None:
                    self._finish()
                if self._child is not None:
                    for stream in (self._child.stdin, self._child.stdout, self._child.stderr):
                        if stream is not None: stream.close()
            raise

    @property
    def pid(self): return self._child.pid

    @property
    def returncode(self): return self._child.returncode

    @property
    def stdin(self): return self._child.stdin

    @property
    def stdout(self): return self._child.stdout

    @property
    def stderr(self): return self._child.stderr

    def _publish(self):
        self._value['elapsed_ns'] = time.monotonic_ns() - self._started
        require(0 <= self._value['elapsed_ns'] <= MAX_U64, 'process_usage_clock')
        encoded = json.dumps(self._value, sort_keys=True).encode() + b'\n'
        require(len(encoded) <= 2048, 'process_usage_bound')
        self._evidence.seek(0); self._evidence.write(encoded)
        self._evidence.truncate(); self._evidence.flush()

    def _finish(self):
        try:
            self._publish()
        finally:
            self._evidence.close(); self._evidence = None
            if self.pidfd is not None:
                os.close(self.pidfd); self.pidfd = None

    def _reap(self):
        if self._child.returncode is not None: return self._child.returncode
        pid, status, usage = os.wait4(self.pid, os.WNOHANG)
        if pid == 0: return None
        require(pid == self.pid, 'process_usage_exit')
        # Record reaping before inspecting usage: cleanup must never signal a
        # PID whose exclusively owned child has already been reaped.
        self._child.returncode = os.waitstatus_to_exitcode(status)
        self._value.update(exit_observed=True, exit_code=self._child.returncode)
        try:
            require(type(usage.ru_maxrss) is int and 0 < usage.ru_maxrss <= MAX_U64 // 1024,
                    'process_usage_exit')
            self._value['exit_rss_peak_bytes'] = usage.ru_maxrss * 1024
            if self._value['address_space_proof'] is not None:
                self._value['worker_exit_allocator_peak_upper_bound_bytes'] = self._value['address_space_proof']['limit_bytes']
                self._value['exit_allocator_peak_upper_bound_bytes'] = self._value['address_space_proof']['lifetime_upper_bound_bytes']
        finally:
            self._finish()
        return self._child.returncode

    def poll(self):
        with self._lock: return self._reap()

    def wait(self, timeout=None):
        require(type(timeout) in (int, float) and 0 <= timeout <= 1200, 'process_usage_timeout')
        deadline = time.monotonic() + timeout
        while True:
            with self._lock:
                result = self._reap()
                if result is not None: return result
                descriptor = os.dup(self.pidfd)
            try:
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not select.select([descriptor], [], [], remaining)[0]:
                    raise subprocess.TimeoutExpired('owned-process', timeout)
            finally:
                os.close(descriptor)

    def send_signal(self, number):
        with self._lock:
            if self._reap() is None:
                try: signal.pidfd_send_signal(self.pidfd, number)
                except ProcessLookupError: pass

    def terminate(self): self.send_signal(signal.SIGTERM)

    def kill(self): self.send_signal(signal.SIGKILL)
