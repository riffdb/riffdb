"""Bounded private child processes for the installed rehearsal."""
from __future__ import annotations

import os
from pathlib import Path
import queue
import signal
import subprocess
import threading

MAX_OUTPUT = 8 * 1024 * 1024


def private_environment(root: Path) -> dict[str, str]:
    return {"PATH": "/usr/bin:/bin", "HOME": str(root), "LANG": "C.UTF-8", "TZ": "UTC"}


def private_write(path: Path, value: str) -> None:
    path.write_text(value)
    path.chmod(0o600)


class Process:
    def __init__(self, command, *, cwd: Path, env: dict, ready_prefix: bytes | None = None, keep_stdin=False):
        self.process = subprocess.Popen([str(item) for item in command], cwd=cwd, env=env,
                                        stdin=subprocess.PIPE if keep_stdin else subprocess.DEVNULL, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE, start_new_session=True)
        self.output = [bytearray(), bytearray()]
        self.overflow = threading.Event()
        self.ready = queue.Queue(maxsize=1)
        self.threads = []
        for index, stream in enumerate((self.process.stdout, self.process.stderr)):
            thread = threading.Thread(target=self._capture, args=(index, stream, ready_prefix), daemon=True)
            thread.start()
            self.threads.append(thread)

    def _capture(self, index, stream, ready_prefix):
        pending = bytearray()
        with stream:
            while chunk := os.read(stream.fileno(), 4096):
                if len(self.output[index]) + len(chunk) > MAX_OUTPUT:
                    self.overflow.set()
                    os.killpg(self.process.pid, signal.SIGKILL)
                    return
                self.output[index].extend(chunk)
                if ready_prefix is None:
                    continue
                pending.extend(chunk)
                if len(pending) > 65536:
                    self.overflow.set()
                    os.killpg(self.process.pid, signal.SIGKILL)
                    return
                while b"\n" in pending:
                    line, _, pending = pending.partition(b"\n")
                    if ready_prefix in line:
                        try:
                            self.ready.put_nowait(bytes(line))
                        except queue.Full:
                            pass

    def wait_ready(self, timeout=30) -> bytes:
        try:
            return self.ready.get(timeout=timeout)
        except queue.Empty:
            raise RuntimeError("process_readiness_unavailable") from None

    def wait(self, timeout=90, *, expected=0) -> tuple[bytes, bytes]:
        try:
            status = self.process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(self.process.pid, signal.SIGKILL)
            self.process.wait()
            raise RuntimeError("process_deadline") from None
        finally:
            if self.process.stdin is not None:
                self.process.stdin.close()
            for thread in self.threads:
                thread.join(timeout=5)
        if self.overflow.is_set():
            raise RuntimeError("process_output_bound")
        if expected is not None and status != expected:
            raise RuntimeError("process_exit_status")
        return bytes(self.output[0]), bytes(self.output[1])

    def stop(self, expected=0):
        if self.process.poll() is None:
            os.killpg(self.process.pid, signal.SIGTERM)
        return self.wait(timeout=15, expected=expected)

    def save_private_output(self, root: Path, name: str):
        for index, suffix in enumerate(("stdout", "stderr")):
            path = root / f"{name}.{suffix}"
            path.write_bytes(self.output[index])
            path.chmod(0o600)


def run(command, *, cwd: Path, env: dict, expected=0, timeout=90) -> tuple[bytes, bytes]:
    child = Process(command, cwd=cwd, env=env)
    try:
        return child.wait(timeout=timeout, expected=expected)
    finally:
        child.save_private_output(Path(env["HOME"]), "last-child")
