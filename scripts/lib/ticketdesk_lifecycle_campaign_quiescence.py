# req: END-007, END-008, END-009, END-010
"""Bounded control-file handover; actual fault reconciliation is separate."""
from copy import deepcopy
import json
import os
from pathlib import Path
import re

from .ticketdesk_lifecycle_campaign_restore_clock import RestoreRecoveryClock

SECOND = 1000000000
MAX_U64 = 2**64 - 1
LOAD_KEYS = {"schema", "generation", "completed_commands", "completed_reads"}
OBSERVER_KEYS = {"schema", "generation", "contract_version", "history_incarnation",
                 "events_checked", "duplicates", "consumer_checkpoint", "live_head"}
RESUMED_KEYS = {"schema", "generation", "contract_version", "history_incarnation",
                "completed_commands", "completed_reads"}


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
    def __init__(self, root, *, generation, version, incarnation, restore_clock=None):
        require(integer(generation) > 0 and integer(incarnation) > 0
                and type(version) is int and version in (1, 2), "quiescence_identity")
        self.root = Path(root)
        self.generation, self.version, self.incarnation = generation, version, incarnation
        self._failed = False
        self._state = "idle"
        self._started = self._last_ns = self._snapshot = self._release = None
        self._release_version = None
        self._observer = None
        self._resumed = None
        self._load_paused = False
        require(restore_clock is None or isinstance(restore_clock, RestoreRecoveryClock),
                'quiescence_restore_clock')
        self._restore_clock = restore_clock

    @property
    def snapshot(self):
        return deepcopy(self._snapshot)

    @property
    def observer_snapshot(self):
        return deepcopy(self._observer)

    @property
    def resumed_snapshot(self):
        return deepcopy(self._resumed)

    def _tick(self, now_ns):
        integer(now_ns)
        deadline = self._started + 60 * SECOND if self._started is not None else 0
        if self._restore_clock is not None:
            clock = self._restore_clock
            require(not clock.failed and not clock.complete
                    and clock.preparation is not None
                    and clock.preparation['started_ns'] == self._started,
                    'quiescence_restore_clock')
            deadline = (clock.planned_loss_ns + int(clock.state == 'backup_durable') * (SECOND - 1)
                        if clock.loss_ns is None else clock.deadline_ns)
        require(self._started is not None and self._state != "complete"
                and self._last_ns <= now_ns <= deadline,
                "quiescence_clock")
        self._last_ns = now_ns

    def _marker(self, name):
        descriptor = os.open(self.root / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        os.close(descriptor)

    @fenced
    def begin(self, now_ns, *, pause_load=True):
        integer(now_ns)
        require(self._state == "idle" and type(pause_load) is bool, "quiescence_state")
        require(not any((self.root / name).exists() for name in
                        ("load.quiesced", "observer.quiesced", "observer.restore", "load.resumed")),
                "quiescence_stale")
        self._started = self._last_ns = now_ns
        if self._restore_clock is not None:
            require(pause_load and self._restore_clock.state == 'quiescing'
                    and self._restore_clock.preparation['started_ns'] == now_ns,
                    'quiescence_restore_clock')
            self._tick(now_ns)
        if pause_load:
            self._marker("load.quiesce")
            self._load_paused = True
        self._marker("observer.quiesce")
        self._state = "pausing"

    @fenced
    def pause_load(self, now_ns):
        require(not self._load_paused, "quiescence_state")
        self.poll(now_ns)
        require(self._observer is not None, "quiescence_not_ready")
        self._marker("load.quiesce")
        self._load_paused = True

    def _ack(self, name, keys, schema, *, generation=None):
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
        require(value["generation"] == (self.generation if generation is None else generation),
                "quiescence_identity")
        return value

    @fenced
    def poll(self, now_ns):
        self._tick(now_ns)
        require(self._state == "pausing", "quiescence_state")
        load = self._ack("load.quiesced", LOAD_KEYS, "riffdb.ticketdesk-lifecycle-quiesced/v1")
        observer = self._ack("observer.quiesced", OBSERVER_KEYS,
                             "riffdb.ticketdesk-lifecycle-observer-quiesced/v1")
        require(load is None or self._load_paused, "quiescence_identity")
        if observer is not None:
            require(observer["contract_version"] == self.version
                    and observer["history_incarnation"] == self.incarnation,
                    "quiescence_identity")
            require(observer["duplicates"] == 0, "quiescence_duplicates")
            require(self._observer is None or self._observer == observer, "quiescence_record")
            self._observer = observer
        else:
            require(self._observer is None, "quiescence_record")
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
        self._release_version = version
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
    def workers_resumed(self, now_ns, *, generation, incarnation, duplicates):
        self._tick(now_ns)
        require(self._state == "resumed" and (integer(generation), integer(incarnation))
                == self._release and integer(duplicates) == 0, "quiescence_resume")
        acknowledged = self._ack("load.resumed", RESUMED_KEYS,
                                 "riffdb.ticketdesk-lifecycle-resumed/v1", generation=generation)
        require(acknowledged is not None
                and acknowledged["contract_version"] == self._release_version
                and acknowledged["history_incarnation"] == incarnation
                and acknowledged["completed_commands"] > self._snapshot["load"]["completed_commands"]
                and acknowledged["completed_reads"] >= self._snapshot["load"]["completed_reads"],
                "quiescence_resume")
        self._resumed = deepcopy(acknowledged)
        (self.root / "load.quiesced").unlink()
        (self.root / "observer.quiesced").unlink()
        (self.root / "load.resumed").unlink()
        (self.root / "observer.restore").unlink(missing_ok=True)
        self._state = "complete"
