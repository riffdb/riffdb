# req: END-007, END-008, END-010
"""Serialize public CLI calls through one exact, bounded administrative owner.

Wait4 retains short-child peaks before another census can mistake their exit
for a lost persistent owner. Calls consume the fourth auxiliary slot. The
controller must sample between calls and enforce its original global gaps;
this helper supplies no database result reconciliation or qualification.
"""
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import threading
import time

from .ticketdesk_lifecycle_campaign_process_roster import CampaignProcessRoster

COMMANDS = frozenset({"server.health", "role.bind", "capability.revoke", "application.deploy",
    "event.retire", "event.consume", "event.nack", "event.seek", "backup.create", "backup.restore",
    "backup.operation"})
MAX_STDOUT = 1024**2
MAX_STDERR = 65536


class OperatorError(ValueError):
    """Closed refusal without command arguments, output, identities or paths."""


def require(condition, code):
    if not condition: raise OperatorError(code)


def unique_object(pairs):
    value = {}
    for key, item in pairs:
        require(key not in value, "campaign_operator_output")
        value[key] = item
    return value


class CampaignOperator:
    complete_campaign_checks_proved = False

    def __init__(self, root, roster, base):
        require(isinstance(roster, CampaignProcessRoster) and "operator" in roster.limits,
                "campaign_operator_owner")
        self._arguments(base)
        require(len(base) > 0, "campaign_operator_arguments")
        self.root, self.roster, self.base = Path(root), roster, list(base)
        self.output = None
        self.failed = self.busy = False
        self.count = 0
        self.last_ns = None

    @staticmethod
    def _arguments(arguments):
        require(type(arguments) is list and len(arguments) <= 128
                and all(type(arg) is str and "\x00" not in arg for arg in arguments)
                and sum(len(arg.encode()) for arg in arguments) <= 65536,
                "campaign_operator_arguments")

    def __enter__(self):
        require(self.output is None, "campaign_operator_state")
        fd = os.open(self.root / "campaign-operator.jsonl",
                     os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        self.output = os.fdopen(fd, "wb")
        return self

    def __exit__(self, _kind, _error, _traceback):
        if self.output is not None: self.output.close()
        return False

    @staticmethod
    def _drain(source, target, maximum, refusals):
        total = 0
        try:
            while chunk := source.read(4096):
                if total + len(chunk) > maximum:
                    refusals.append("campaign_operator_output")
                    # Leave the read endpoint open until the owner kills and
                    # reaps this child; a blocked producer cannot write more.
                    return
                target.write(chunk); target.flush()
                total += len(chunk)
        except OSError:
            refusals.append("campaign_operator_io")

    @staticmethod
    def _maintenance_uncertain(command, value):
        if command not in {"backup.create", "backup.restore"} or type(value) is not dict:
            return False
        if value.keys() != {"schema", "command", "ok", "error"} or not (
                value["schema"] == "riffdb.cli.output/v1" and value["command"] == command
                and value["ok"] is False):
            return False
        error = value["error"]
        return (type(error) is dict and error.keys() == {"type", "code", "message",
                "recovery_action", "maintenance_operation_id"}
            and error["type"] == "uncertain" and error["code"] == "outcome_unknown"
            and type(error["message"]) is str and len(error["message"]) <= 1024
            and error["recovery_action"] == "poll_maintenance_operation"
            and type(error["maintenance_operation_id"]) is str
            and re.fullmatch(r"[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}",
                             error["maintenance_operation_id"]) is not None)

    def run(self, command, arguments, *, elapsed_ns, timeout_s):
        require(not self.failed, "campaign_operator_failed")
        child, readers, refusals = None, [], []
        self.busy, was_busy = True, self.busy
        try:
            require(self.output is not None and not was_busy and self.count < 64,
                    "campaign_operator_state")
            require(type(command) is str and command in COMMANDS, "campaign_operator_command")
            require(type(elapsed_ns) is int and 0 <= elapsed_ns < 2**64
                    and (self.last_ns is None or elapsed_ns >= self.last_ns), "campaign_operator_clock")
            require(type(timeout_s) in (int, float) and 0 < timeout_s <= 60,
                    "campaign_operator_deadline")
            self._arguments(arguments)
            self._arguments(self.base + arguments)
            self.last_ns = elapsed_ns
            name = "operator-" + str(self.count).zfill(3)
            self.count += 1
            started = time.monotonic_ns()
            with (self.root / (name + ".stdout")).open("xb") as out, \
                    (self.root / (name + ".stderr")).open("xb") as errors:
                child = self.roster.launch("operator", self.base + arguments,
                    observation=self.root / (name + ".process-usage.json"),
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                for source, target, bound in ((child.stdout, out, MAX_STDOUT),
                                               (child.stderr, errors, MAX_STDERR)):
                    reader = threading.Thread(target=self._drain,
                        args=(source, target, bound, refusals), daemon=True)
                    readers.append(reader); reader.start()
                while child.poll() is None:
                    require(not refusals, refusals[0] if refusals else "campaign_operator_output")
                    remaining = timeout_s - (time.monotonic_ns() - started) / 10**9
                    require(remaining > 0, "campaign_operator_deadline")
                    try: child.wait(timeout=min(remaining, 0.05))
                    except subprocess.TimeoutExpired: pass
                for reader in readers: reader.join(timeout=1)
                require(not any(reader.is_alive() for reader in readers), "campaign_operator_io")
                # Exit 3 has one narrowly recognized native maintenance
                # protocol. It supplies an operation to poll, never success,
                # action reconciliation, or permission to retry the mutation.
                value, expected_exit = None, 0
                if child.poll() == 3 and not refusals:
                    try:
                        value = json.loads((self.root / (name + ".stdout")).read_bytes(),
                                           object_pairs_hook=unique_object)
                    except (ValueError, UnicodeError, RecursionError):
                        pass
                    if self._maintenance_uncertain(command, value): expected_exit = 3
                # Every other nonzero exit remains unexpected, including a
                # child that finished before its first live process census.
                self.roster.retire("operator", child, expected_exit=expected_exit)
                require(not refusals, refusals[0] if refusals else "campaign_operator_output")
            self.last_ns = elapsed_ns + time.monotonic_ns() - started
            self.roster.sample(self.last_ns)
            if expected_exit == 0:
                encoded = (self.root / (name + ".stdout")).read_bytes()
                value = json.loads(encoded, object_pairs_hook=unique_object)
                require(type(value) is dict and value.get("schema") == "riffdb.cli.output/v1"
                        and value.get("command") == command and value.get("ok") is True,
                        "campaign_operator_output")
            duration_ns = time.monotonic_ns() - started
            require(duration_ns <= timeout_s * 10**9, "campaign_operator_deadline")
            record = dict(schema="riffdb.ticketdesk-lifecycle-campaign-operator/v1",
                index=self.count - 1, command=command, elapsed_ns=elapsed_ns,
                duration_ns=duration_ns, exit_code=expected_exit,
                outcome_known=expected_exit == 0, qualifies_complete_campaign=False)
            self.output.write(json.dumps(record, sort_keys=True).encode() + b"\n"); self.output.flush()
            return value
        except BaseException as error:
            self.failed = True
            if isinstance(error, (OSError, UnicodeError, json.JSONDecodeError, RecursionError)):
                raise OperatorError("campaign_operator_io") from error
            raise
        finally:
            self.busy = False
            primary = sys.exception()
            try:
                if child is not None:
                    if child.poll() is None:
                        child.kill(); child.wait(timeout=5)
                        # This is an explicitly failed operation, never a
                        # successful action or an unexpected successful exit.
                        self.roster.retire("operator", child, expected_exit=-9)
                    for reader in readers: reader.join(timeout=1)
                    for stream in (child.stdin, child.stdout, child.stderr):
                        if stream is not None: stream.close()
            except BaseException:
                self.failed = True
                if primary is None: raise
                primary.add_note("campaign_operator_cleanup")
