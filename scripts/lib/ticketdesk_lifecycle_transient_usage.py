# req: END-007, END-008, END-010
"""Per-invocation child birth and wait4 exit evidence, without process identities.

The exact owned child's kernel RSS high water survives its exit. Neither this
record nor its launch-time virtual peak proves final allocator high water or a
complete descendant inventory. Missing fields remain unknown rather than zero.
"""
import json
import os
from pathlib import Path
import select
import signal
import subprocess
import time

from .ticketdesk_lifecycle_resources import ProcessMemory, ResourceError, require, MAX_U64

from contextlib import nullcontext
from .ticketdesk_lifecycle_address_space import AddressSpaceLaunch, BASIS

MAX_OBSERVATION_BYTES = 2048


def run_transient_observed(arguments, *, output, errors, environment, timeout, observation,
                           address_space_limit_bytes=None):
    require(type(timeout) in (int,float) and 0 < timeout <= 1200, 'process_usage_timeout')
    descriptor=os.open(Path(observation),os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(descriptor,'wb') as evidence:
        value=dict(schema='riffdb.ticketdesk-lifecycle-transient-usage/v1',
            launch_observed=False,exit_observed=False,timed_out=False,exit_code=None,
            elapsed_ns=None,launch_memory=None,launch_memory_refusal=None,
            exit_rss_peak_bytes=None,exit_allocator_peak_bytes=None,
            exit_allocator_peak_upper_bound_bytes=None,address_space_proof=None,
            exit_rss_basis='linux_wait4_per_child_ru_maxrss',
            allocator_basis='unknown_at_exit',qualifies_complete_resource_model=False)
        child=None
        pidfd=None
        started=time.monotonic_ns()

        def join_for(seconds):
            deadline=time.monotonic()+seconds
            while True:
                remaining=deadline-time.monotonic()
                if remaining<=0:return False
                ready,_,_=select.select([pidfd],[],[],remaining)
                if not ready:return False
                pid,status,usage=os.wait4(child.pid,os.WNOHANG)
                if pid==0:continue
                require(pid==child.pid,'process_usage_exit')
                child.returncode=os.waitstatus_to_exitcode(status)
                value.update(exit_observed=True,exit_code=child.returncode)
                require(type(usage.ru_maxrss) is int
                        and 0 < usage.ru_maxrss <= MAX_U64//1024,'process_usage_exit')
                value['exit_rss_peak_bytes']=usage.ru_maxrss*1024
                if value['address_space_proof'] is not None:
                    value['exit_allocator_peak_upper_bound_bytes'] = address_space_limit_bytes
                return True

        try:
            launch = (AddressSpaceLaunch(arguments, address_space_limit_bytes)
                      if address_space_limit_bytes is not None else None)
            with launch if launch is not None else nullcontext():
                options = dict(stdout=output, stderr=errors, env=environment)
                child = subprocess.Popen(launch.arguments if launch else arguments,
                                         **(launch.options(options) if launch else options))
                value['launch_observed'] = True
                pidfd = os.pidfd_open(child.pid, 0)
                if launch:
                    value['address_space_proof'] = launch.confirm()
                    value['allocator_basis'] = BASIS
            try:
                raw=ProcessMemory(child.pid).tree_sample()[0]
                fields=('rss_bytes','allocator_bytes','reported_rss_peak_bytes',
                        'reported_allocator_peak_bytes')
                value['launch_memory']={name:raw[name] for name in fields}
            except ResourceError as error:
                # A child may already have exited. It is still the exact child
                # reaped below; the absent live reading is explicitly unknown.
                value['launch_memory_refusal']=str(error)
            remaining=timeout-(time.monotonic_ns()-started)/1000000000
            if remaining<=0 or not join_for(remaining):
                value['timed_out']=True
                try:signal.pidfd_send_signal(pidfd,signal.SIGKILL)
                except ProcessLookupError:pass
                require(join_for(5),'process_usage_cleanup')
                raise subprocess.TimeoutExpired(arguments,timeout)
            return child.returncode
        finally:
            try:
                if child is not None and child.returncode is None:
                    if pidfd is None:
                        # Failure to acquire the exact descriptor cannot leave
                        # the exclusively owned, unreaped child running. The
                        # standard Popen cleanup joins it, but has no wait4 RSS
                        # evidence; the original failure remains a failure.
                        child.kill();child.wait(timeout=5)
                        value.update(exit_observed=True,exit_code=child.returncode)
                    else:
                        try:signal.pidfd_send_signal(pidfd,signal.SIGKILL)
                        except ProcessLookupError:pass
                        require(join_for(5),'process_usage_cleanup')
            finally:
                if pidfd is not None:os.close(pidfd)
                value['elapsed_ns']=time.monotonic_ns()-started
                require(0 <= value['elapsed_ns'] <= MAX_U64,'process_usage_clock')
                encoded=json.dumps(value,sort_keys=True).encode()+b'\n'
                require(len(encoded)<=MAX_OBSERVATION_BYTES,'process_usage_bound')
                evidence.write(encoded);evidence.flush()
