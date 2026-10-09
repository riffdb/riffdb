# req: END-007, END-008, END-009, END-010
"""Bounded control-file handover; actual fault reconciliation is separate."""
from copy import deepcopy
import json
import os
from pathlib import Path
import re

SECOND = 1000000000
MAX_U64 = 2**64 - 1
LOAD_KEYS = {"schema", "generation", "completed_commands", "completed_reads"}
OBSERVER_KEYS = {"schema", "generation", "contract_version", "history_incarnation",
                 "events_checked", "duplicates", "consumer_checkpoint", "live_head"}


class QuiescenceError(ValueError):
    """Closed handover refusal without credentials, payloads or paths."""


def require(condition, code):
    if not condition:
        raise QuiescenceError(code)


def integer(value):
    require(type(value) is int and 0 <= value <= MAX_U64, "quiescence_integer")
    return value


def fenced(function):
    def checked(self, *args, **kwargs):
        require(not self._failed, "quiescence_failed")
        try:
            return function(self, *args, **kwargs)
        except (OSError, UnicodeError, ValueError, RecursionError, TypeError) as error:
            self._failed = True
            if isinstance(error, QuiescenceError):
                raise
            raise QuiescenceError("quiescence_io") from None
    return checked


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, "quiescence_record")
        result[key] = value
    return result


class QuiescentRelease:
    def __init__(self, root, *, generation, version, incarnation):
        require(integer(generation) > 0 and integer(incarnation) > 0
                and type(version) is int and version in (1, 2), "quiescence_identity")
        self.root = Path(root)
        self.generation, self.version, self.incarnation = generation, version, incarnation
        self._failed = False
        self._state = "idle"
        self._started = self._last_ns = self._snapshot = self._release = None

    @property
    def snapshot(self):
        return deepcopy(self._snapshot)

    def _tick(self, now_ns):
        integer(now_ns)
        require(self._started is not None and self._state != "complete"
                and self._last_ns <= now_ns <= self._started + 60 * SECOND,
                "quiescence_clock")
        self._last_ns = now_ns

    def _marker(self, name):
        descriptor = os.open(self.root / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        os.close(descriptor)

    @fenced
    def begin(self, now_ns):
        integer(now_ns)
        require(self._state == "idle", "quiescence_state")
        require(not any((self.root / name).exists() for name in
                        ("load.quiesced", "observer.quiesced", "observer.restore")),
                "quiescence_stale")
        self._started = self._last_ns = now_ns
        self._marker("load.quiesce")
        self._marker("observer.quiesce")
        self._state = "pausing"

    def _ack(self, name, keys, schema):
        path = self.root / name
        if not path.exists():
            return None
        with path.open("rb") as source:
            encoded = source.read(4097)
        require(len(encoded) <= 4096, "quiescence_record")
        value = json.loads(encoded, object_pairs_hook=unique_object)
        require(type(value) is dict and value.keys() == keys and value["schema"] == schema,
                "quiescence_record")
        for key in keys - {"schema"}:
            integer(value[key])
        require(value["generation"] == self.generation, "quiescence_identity")
        return value

    @fenced
    def poll(self, now_ns):
        self._tick(now_ns)
        require(self._state == "pausing", "quiescence_state")
        load = self._ack("load.quiesced", LOAD_KEYS, "riffdb.ticketdesk-lifecycle-quiesced/v1")
        observer = self._ack("observer.quiesced", OBSERVER_KEYS,
                             "riffdb.ticketdesk-lifecycle-observer-quiesced/v1")
        if observer is not None:
            require(observer["contract_version"] == self.version
                    and observer["history_incarnation"] == self.incarnation,
                    "quiescence_identity")
            require(observer["duplicates"] == 0, "quiescence_duplicates")
        if load is None or observer is None:
            require(self._snapshot is None, "quiescence_record")
            return False
        snapshot = {"load": load, "observer": observer}
        require(self._snapshot is None or self._snapshot == snapshot, "quiescence_record")
        self._snapshot = snapshot
        return True

    @fenced
    def publish(self, now_ns, *, credential, version, incarnation, restore=False):
        require(self.poll(now_ns), "quiescence_not_ready")
        require(type(restore) is bool and type(version) is int
                and self.version <= version <= 2
                and integer(incarnation) == self.incarnation + int(restore)
                and self.generation < MAX_U64, "quiescence_release")
        require(type(credential) is str and re.fullmatch(r"[A-Za-z0-9_.-]{1,128}", credential)
                is not None and credential not in (".", "..")
                and (self.root / credential).is_file(), "quiescence_release")
        generation = self.generation + 1
        encoded = f"{generation} {version} {incarnation} {credential}\n".encode("ascii")
        pending = self.root / ".load.phase.pending"
        descriptor = os.open(pending, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as output:
            output.write(encoded)
            output.flush()
            os.fsync(output.fileno())
        if restore:
            self._marker("observer.restore")
        pending.replace(self.root / "load.phase")
        self._release = (generation, incarnation)
        self._state = "published"

    @fenced
    def resume(self, now_ns):
        self._tick(now_ns)
        require(self._state == "published", "quiescence_state")
        (self.root / "load.quiesce").unlink()
        (self.root / "observer.quiesce").unlink()
        # Keep acknowledgements while a paused load poll could republish one.
        # The restore marker must survive until the observer reads the phase.
        self._state = "resumed"

    @fenced
    def workers_resumed(self, now_ns, *, generation, incarnation, duplicates, completed_commands):
        self._tick(now_ns)
        require(self._state == "resumed" and (integer(generation), integer(incarnation))
                == self._release and integer(duplicates) == 0
                and integer(completed_commands) > self._snapshot["load"]["completed_commands"],
                "quiescence_resume")
        (self.root / "load.quiesced").unlink()
        (self.root / "observer.quiesced").unlink()
        (self.root / "observer.restore").unlink(missing_ok=True)
        self._state = "complete"
