# req: END-003, END-007, END-008
"""Own one persistent Rust sampler; refuse incomplete or discontinuous evidence."""
import json
from pathlib import Path
import queue
import re
import subprocess
import threading

from .ticketdesk_lifecycle_persistent_usage import ObservedProcess

MAX_LINE_BYTES = 4096
MAX_SAMPLES = 86400
MAX_U64 = 2**64 - 1
SHUTDOWN_TIMEOUT_S = 10
FIELDS = frozenset({"schema", "index", "contract_version", "sampled_head", "statistics_head",
                    "outbox_backlog", "outbox_degraded", "history_incarnation", "applied_through",
                    "projection_generation", "projection_backlog", "projection_id",
                    "projection_plan_hash", "transport_attempts", "retries", "auxiliary_slots"})


class NativeObservationError(ValueError):
    """Closed codes never include child output, input paths or credentials."""


def require(condition, code):
    if not condition:
        raise NativeObservationError(code)


def integer(value):
    require(type(value) is int and 0 <= value <= MAX_U64, "native_sample_number")
    return value


class NativeSamples:
    """Continuity for one sampler lifetime; a declared restart needs a new owner."""

    def __init__(self, identity):
        require(type(identity) is dict and identity.keys() == {"projection_id", "projection_plan_hash"},
                "native_sample_identity")
        require(integer(identity["projection_id"]) > 0 and type(identity["projection_plan_hash"]) is str
                and re.fullmatch(r"[0-9a-f]{64}", identity["projection_plan_hash"]), "native_sample_identity")
        self.identity = dict(identity)
        self.last = None
        self.transport_attempts = 0

    def observe(self, value, *, version):
        require(type(version) is int and version in (1, 2), "native_sample_version")
        require(type(value) is dict and value.keys() == FIELDS, "native_sample_shape")
        require(value["schema"] == "riffdb.ticketdesk-lifecycle-native-observation/v1"
                and type(value["outbox_degraded"]) is bool, "native_sample_shape")
        for key in FIELDS - {"schema", "outbox_degraded", "projection_plan_hash"}:
            integer(value[key])
        require(value["index"] == (0 if self.last is None else self.last["index"] + 1)
                and value["index"] < MAX_SAMPLES, "native_sample_continuity")
        require(value["contract_version"] == version and value["projection_id"] == self.identity["projection_id"]
                and value["projection_plan_hash"] == self.identity["projection_plan_hash"], "native_sample_identity")
        require(value["sampled_head"] > 0 and value["statistics_head"] >= value["sampled_head"]
                and value["history_incarnation"] > 0 and value["projection_generation"] > 0
                and value["applied_through"] > 0
                and value["projection_backlog"] == max(value["sampled_head"] - value["applied_through"], 0),
                "native_sample_frontier")
        require(value["transport_attempts"] == 3 and value["retries"] == 0
                and value["auxiliary_slots"] == 1, "native_sample_accounting")
        if self.last is not None:
            require(value["history_incarnation"] == self.last["history_incarnation"]
                    and all(value[key] >= self.last[key] for key in
                            ("sampled_head", "statistics_head", "projection_generation", "applied_through")),
                    "native_sample_continuity")
        self.last = dict(value)
        self.transport_attempts += value["transport_attempts"]
        return dict(value)


def unique_object(pairs):
    value = {}
    for key, item in pairs:
        require(key not in value, "native_sample_json")
        value[key] = item
    return value


def parse_integer(encoded):
    require(len(encoded) <= 20, "native_sample_number")
    return integer(int(encoded))


def reject_number(_encoded):
    raise NativeObservationError("native_sample_number")


def read_message(stream):
    encoded = stream.readline(MAX_LINE_BYTES + 1)
    if not encoded:
        return None
    require(0 < len(encoded) <= MAX_LINE_BYTES and encoded.endswith(b"\n"), "native_sample_line")
    try:
        value = json.loads(encoded, object_pairs_hook=unique_object, parse_int=parse_integer,
                           parse_float=reject_number, parse_constant=reject_number)
    except (ValueError, UnicodeError, RecursionError) as error:
        raise NativeObservationError("native_sample_json") from error
    require(type(value) is dict, "native_sample_shape")
    return value


class NativeObserverProcess:
    """One child and one outstanding sample, sharing one reserved auxiliary slot.

    Missing/failed RPC output refuses the preparation, including partial attempts.
    It never fabricates successful accounting for an interrupted native sample.
    """

    def __init__(self, root, command, environment, identity):
        self.samples = NativeSamples(identity)
        self.messages = queue.Queue(maxsize=2)
        self.failure = None
        self.root = Path(root)
        self.process = None
        self.reader = None
        self.output = None
        self.last_elapsed_ns = None
        try:
            with (self.root / "native-observer.stderr").open("xb") as errors:
                self.process = ObservedProcess(command, env=environment, stdin=subprocess.PIPE,
                                                observation=self.root / "native-observer.process-usage.json",
                                                stdout=subprocess.PIPE, stderr=errors)
            self.reader = threading.Thread(target=self._read, daemon=True)
            self.reader.start()
            ready = self._next(10)
            require(ready.keys() == {"schema", "auxiliary_slots"}
                    and ready["schema"] == "riffdb.ticketdesk-lifecycle-native-observer-ready/v1"
                    and type(ready["auxiliary_slots"]) is int and ready["auxiliary_slots"] == 1,
                    "native_sample_ready")
            self.output = (self.root / "native-observations.jsonl").open("xb")
        except BaseException:
            self.close(failed=True)
            raise

    def _read(self):
        try:
            while True:
                value = read_message(self.process.stdout)
                if value is None:
                    return
                self.messages.put_nowait(value)
        except (NativeObservationError, OSError, queue.Full) as error:
            self.failure = error

    def _next(self, deadline):
        try:
            value = self.messages.get(timeout=deadline)
        except queue.Empty as error:
            raise NativeObservationError("native_sample_deadline") from error
        require(self.process.poll() is None, "native_sample_process")
        return value

    def sample(self, elapsed_ns, *, version=1):
        integer(elapsed_ns)
        require(self.last_elapsed_ns is None or elapsed_ns >= self.last_elapsed_ns, "native_sample_clock")
        require(type(version) is int and version in (1, 2)
                and self.process.poll() is None and self.failure is None and self.messages.empty(),
                "native_sample_process")
        try:
            self.process.stdin.write(f"sample {version}\n".encode())
            self.process.stdin.flush()
        except (OSError, ValueError) as error:
            raise NativeObservationError("native_sample_process") from error
        value = self.samples.observe(self._next(6), version=version)
        value["elapsed_ns"] = elapsed_ns
        self.output.write(json.dumps(value, sort_keys=True).encode() + b"\n")
        self.output.flush()
        self.last_elapsed_ns = elapsed_ns
        return value

    def close(self, *, failed=False):
        process = self.process
        allow_failure = failed
        shutdown_failed = False
        try:
            if process is not None:
                if process.poll() is None and not failed:
                    try:
                        process.stdin.write(b"stop\n")
                        process.stdin.flush()
                    except (OSError, ValueError):
                        failed = True
                        shutdown_failed = True
                if process.poll() is None and failed:
                    process.terminate()
                try:
                    process.wait(timeout=SHUTDOWN_TIMEOUT_S)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
                    shutdown_failed = True
                process.stdin.close()
                if self.reader is not None:
                    self.reader.join(timeout=5)
                    require(not self.reader.is_alive(), "native_sample_join")
                process.stdout.close()
                require(allow_failure or (not shutdown_failed and process.returncode == 0
                        and self.failure is None and self.messages.empty()),
                        "native_sample_shutdown")
        finally:
            if self.output is not None:
                self.output.close()
                self.output = None
            if process is not None and process.poll() is not None:
                self.process = None

    def __enter__(self):
        return self

    def __exit__(self, kind, _error, _traceback):
        self.close(failed=kind is not None)
        return False
