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
    "event.retire", "event.consume", "event.nack", "event.seek", "projection.query", "backup.create", "backup.restore",
    "backup.operation", "command.run", "command.outcome"})
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
        self.pending_maintenance = set()

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

    @staticmethod
    def _worker_result(command, value):
        if type(value) is not dict:
            return False
        schema = "riffdb.ticketdesk-lifecycle-" + command + "/v1"
        if command == "rotation-smoke":
            keys = {"revoked_reads_refused", "revoked_replay_refused", "same_principal_replay", "state_reconciled"}
            return value.keys() == keys | {"schema"} and value["schema"] == schema and all(value[key] is True for key in keys)
        if command == "successor-authorization":
            return (value.keys() == {"schema", "contract_version", "ungranted_command_refused"}
                and value["schema"] == schema and type(value["contract_version"]) is int
                and value["contract_version"] == 2 and value["ungranted_command_refused"] is True)
        if command == "reconcile":
            return (value.keys() == {"schema", "tickets_checked", "updates", "max_in_flight"}
                and value["schema"] == "riffdb.ticketdesk-lifecycle-state-reconciliation/v1"
                and type(value["tickets_checked"]) is int and value["tickets_checked"] == 65536
                and type(value["updates"]) is int and 0 <= value["updates"] <= 8640000
                and type(value["max_in_flight"]) is int and value["max_in_flight"] == 16)
        if command == "archive-reconcile":
            return (value.keys() == {"schema", "records_checked", "encoded_bytes", "physical_bytes",
                    "payloads_reconciled", "policy_anchors_reconciled"}
                and value["schema"] == "riffdb.ticketdesk-lifecycle-archive-reconciliation/v1"
                and all(type(value[key]) is int and 0 <= value[key] < 2**64
                        for key in ("records_checked", "encoded_bytes", "physical_bytes"))
                and value["records_checked"] <= 8640000 + 65536
                and value["payloads_reconciled"] is True and value["policy_anchors_reconciled"] is True)
        return False

    @staticmethod
    def _worker_arguments(executable, kind, environment):
        require(type(kind) is str and kind in {"rotation-smoke", "reconcile", "archive-reconcile", "successor-authorization"},
                "campaign_operator_worker")
        require(type(executable) is str and type(environment) is dict and len(environment) <= 256
                and all(type(key) is str and type(value) is str and "\0" not in key + value
                        and 0 < len(key) <= 128 and len(value.encode()) <= 8192
                        for key, value in environment.items())
                and sum(len(key.encode()) + len(value.encode()) for key, value in environment.items()) <= 131072,
                "campaign_operator_environment")
    def run_worker(self, executable, kind, environment, *, elapsed_ns, timeout_s):
        self._worker_arguments(executable, kind, environment)
        return self._run(kind, [], elapsed_ns=elapsed_ns, timeout_s=timeout_s,
                         base=[executable, kind], environment=environment, worker=True)

    def run_final_archive(self, executable, environment, *, elapsed_ns, duration_s, timeout_s):
        require(type(duration_s) is int and duration_s % 300 == 0 and 73 <= duration_s // 300 <= 288
                and type(elapsed_ns) is int and elapsed_ns >= duration_s * 10**9
                and not self.roster.children, "campaign_archive_not_offline")
        self._worker_arguments(executable, "archive-reconcile", environment)
        return self._run("archive-reconcile", [], elapsed_ns=elapsed_ns, timeout_s=timeout_s,
                         base=[executable, "archive-reconcile"], environment=environment, worker=True,
                         terminal_archive=True)

    def run(self, command, arguments, *, elapsed_ns, timeout_s):
        return self._run(command, arguments, elapsed_ns=elapsed_ns, timeout_s=timeout_s,
                         base=self.base, environment=None, worker=False)

    def run_as(self, credential, command, arguments, *, elapsed_ns, timeout_s):
        require(type(credential) is str and self.base.count("--credential-file") == 1,
                "campaign_operator_credential")
        path = Path(credential)
        require(path.is_absolute() and path.is_file() and not path.is_symlink(),
                "campaign_operator_credential")
        self._arguments(arguments)
        require("--credential-file" not in arguments, "campaign_operator_credential")
        base = list(self.base)
        index = base.index("--credential-file") + 1
        require(index < len(base), "campaign_operator_credential")
        base[index] = credential
        return self._run(command, arguments, elapsed_ns=elapsed_ns, timeout_s=timeout_s,
                         base=base, environment=None, worker=False)

    def _poll_unavailable(self, command, arguments, value):
        return (command == "backup.operation" and type(arguments) is list
            and len(arguments) == 3 and arguments[:2] == ["backup", "operation"]
            and arguments[2] in self.pending_maintenance and type(value) is dict
            and value.keys() == {"schema", "command", "ok", "error"}
            and value["schema"] == "riffdb.cli.output/v1"
            and value["command"] == command and value["ok"] is False
            and type(value["error"]) is dict
            and value["error"].keys() == {"type", "code", "message"}
            and value["error"]["type"] == "client"
            and type(value["error"]["code"]) is str and type(value["error"]["message"]) is str
            and (value["error"]["code"], value["error"]["message"]) in {
                ("transport_unavailable", "the gRPC transport is unavailable"),
                ("tls_connection_failed", "the verified TLS channel could not connect")})

    def _run(self, command, arguments, *, elapsed_ns, timeout_s, base, environment, worker, terminal_archive=False):
        require(not self.failed, "campaign_operator_failed")
        child, readers, refusals = None, [], []
        self.busy, was_busy = True, self.busy
        try:
            require(self.output is not None and not was_busy and self.count < 64,
                    "campaign_operator_state")
            require(type(command) is str and (worker or command in COMMANDS), "campaign_operator_command")
            require(type(elapsed_ns) is int and 0 <= elapsed_ns < 2**64
                    and (self.last_ns is None or elapsed_ns >= self.last_ns), "campaign_operator_clock")
            require(type(terminal_archive) is bool and (not terminal_archive or
                    (worker and command == "archive-reconcile" and not self.roster.children)),
                    "campaign_archive_not_offline")
            require(type(timeout_s) in (int, float) and 0 < timeout_s <= (900 if terminal_archive else 60),
                    "campaign_operator_deadline")
            self._arguments(arguments)
            self._arguments(base + arguments)
            self.last_ns = elapsed_ns
            name = "operator-" + str(self.count).zfill(3)
            self.count += 1
            started = time.monotonic_ns()
            with (self.root / (name + ".stdout")).open("xb") as out, \
                    (self.root / (name + ".stderr")).open("xb") as errors:
                child = self.roster.launch("operator", base + arguments, env=environment,
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
                if not worker and child.poll() in (1, 3) and not refusals:
                    try:
                        value = json.loads((self.root / (name + ".stdout")).read_bytes(),
                                           object_pairs_hook=unique_object)
                    except (ValueError, UnicodeError, RecursionError):
                        pass
                    if child.poll() == 3 and self._maintenance_uncertain(command, value): expected_exit = 3
                    elif child.poll() == 1 and self._poll_unavailable(command, arguments, value): expected_exit = 1
                # ADR-0050 closes the offline listener. An exact original
                # read-only poll can remain unknown through failed verified
                # connection establishment; no peer verification or receipt
                # result is invented. Every other nonzero exit is unexpected,
                # including a
                # child that finished before its first live process census.
                self.roster.retire("operator", child, expected_exit=expected_exit)
                require(not refusals, refusals[0] if refusals else "campaign_operator_output")
            self.last_ns = elapsed_ns + time.monotonic_ns() - started
            self.roster.sample(self.last_ns)
            if expected_exit == 0:
                encoded = (self.root / (name + ".stdout")).read_bytes()
                value = json.loads(encoded, object_pairs_hook=unique_object)
                require(self._worker_result(command, value) if worker else
                        (type(value) is dict and value.get("schema") == "riffdb.cli.output/v1"
                         and value.get("command") == command and value.get("ok") is True),
                        "campaign_operator_output")
            if not worker and command in {"backup.create", "backup.restore"}:
                operation = (value["error"]["maintenance_operation_id"] if expected_exit == 3
                             else value.get("result", {}).get("maintenance_operation_id"))
                require(type(operation) is str and re.fullmatch(
                    r"[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}",
                    operation) is not None and operation not in self.pending_maintenance
                    and len(self.pending_maintenance) < 2, "campaign_operator_maintenance")
                self.pending_maintenance.add(operation)
            duration_ns = time.monotonic_ns() - started
            require(duration_ns <= timeout_s * 10**9, "campaign_operator_deadline")
            record = dict(schema="riffdb.ticketdesk-lifecycle-campaign-operator/v1",
                index=self.count - 1, command=("worker." + command if worker else command), elapsed_ns=elapsed_ns,
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
