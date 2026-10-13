# req: END-007, END-008, END-009, END-010
"""Reconcile one original public maintenance operation on the fault's clock.

Transport uncertainty is never a new mutation or a successful action. This
component owns no server process and grants no extension to the whole fault.
"""
import re

SECOND = 10**9
MAX_POLLS = 16
UUID7 = r"[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"
PHASES = {name: index for index, name in enumerate(("accepted", "draining", "offline", "artifact_published", "validating", "succeeded"))}
REQUIRED = frozenset({"status", "maintenance_operation_id", "kind", "backup_name", "input_hash", "phase"})


class MaintenanceError(ValueError):
    """Closed refusal without local paths, identities or native output."""


def require(condition, code):
    if not condition: raise MaintenanceError(code)


class CampaignMaintenance:
    def __init__(self, operator, credential, kind, name, *, started_ns, deadline_ns):
        require(type(kind) is str and kind in {"create", "restore"}
                and type(name) is str and re.fullmatch(r"[A-Za-z0-9_-]{1,64}", name) is not None,
                "campaign_maintenance_input")
        require(type(started_ns) is int and type(deadline_ns) is int
                and 0 <= started_ns < deadline_ns <= started_ns + 60 * SECOND,
                "campaign_maintenance_clock")
        self.operator, self.credential, self.kind, self.name = operator, credential, kind, name
        self.started_ns, self.deadline_ns = started_ns, deadline_ns
        self.last_ns, self.operation, self.input_hash = started_ns, None, None
        self.started = self.complete = self.failed = False
        self.polls = 0
        self.phase = None

    def _tick(self, now_ns):
        require(not self.failed, "campaign_maintenance_failed")
        require(type(now_ns) is int and self.last_ns <= now_ns < self.deadline_ns,
                "campaign_maintenance_deadline")
        self.last_ns = now_ns
        return min(5, (self.deadline_ns - now_ns) / SECOND)

    def _observe(self, value, *, polling):
        require(type(value) is dict and value.get("schema") == "riffdb.cli.output/v1"
                and value.get("command") == ("backup.operation" if polling else "backup." + self.kind)
                and type(value.get("ok")) is bool
                and value.keys() == {"schema", "command", "ok", "result" if value["ok"] else "error"}, "campaign_maintenance_output")
        if value["ok"] is False:
            if polling:
                require(value.keys() == {"schema", "command", "ok", "error"}
                    and self.operator._poll_unavailable("backup.operation",
                        ["backup", "operation", self.operation], value), "campaign_maintenance_output")
            else:
                require(self.operator._maintenance_uncertain("backup." + self.kind, value),
                        "campaign_maintenance_output")
                self.operation = value["error"]["maintenance_operation_id"]
            return False
        result = value.get("result")
        require(type(result) is dict and result.keys() == REQUIRED
                and type(result["status"]) is str and result["status"] in ({"found"} if polling else {"accepted", "already_accepted", "terminal"})
                and result["kind"] == self.kind + "_backup" and result["backup_name"] == self.name
                and type(result["maintenance_operation_id"]) is str
                and re.fullmatch(UUID7, result["maintenance_operation_id"]) is not None
                and type(result["input_hash"]) is str
                and re.fullmatch(r"[0-9a-f]{64}", result["input_hash"]) is not None
                and type(result["phase"]) is str and result["phase"] in PHASES,
                "campaign_maintenance_output")
        require(self.operation is None or result["maintenance_operation_id"] == self.operation,
                "campaign_maintenance_identity")
        require(self.input_hash is None or result["input_hash"] == self.input_hash,
                "campaign_maintenance_identity")
        require(self.phase is None or PHASES[result["phase"]] >= PHASES[self.phase],
                "campaign_maintenance_phase")
        self.phase = result["phase"]
        self.operation, self.input_hash = result["maintenance_operation_id"], result["input_hash"]
        self.complete = result["phase"] == "succeeded"
        return self.complete

    def start(self, now_ns):
        try:
            timeout = self._tick(now_ns)
            require(not self.started, "campaign_maintenance_state")
            self.started = True
            value = self.operator.run_as(self.credential, "backup." + self.kind,
                ["backup", self.kind, self.name], elapsed_ns=now_ns, timeout_s=timeout)
            self._tick(self.operator.last_ns)
            return self._observe(value, polling=False)
        except BaseException:
            self.failed = True
            raise

    def poll(self, now_ns):
        try:
            timeout = self._tick(now_ns)
            require(self.started and not self.complete and self.operation is not None and self.polls < MAX_POLLS,
                    "campaign_maintenance_state")
            self.polls += 1
            value = self.operator.run_as(self.credential, "backup.operation",
                ["backup", "operation", self.operation], elapsed_ns=now_ns, timeout_s=timeout)
            self._tick(self.operator.last_ns)
            return self._observe(value, polling=True)
        except BaseException:
            self.failed = True
            raise
