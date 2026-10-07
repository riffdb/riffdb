# req: END-006, END-007, END-008, END-009, END-010
"""Closed controller scheduling; this alone proves no actual lifecycle action."""
from importlib.machinery import SourceFileLoader
from importlib.util import module_from_spec, spec_from_loader
from pathlib import Path
from types import MappingProxyType

loader = SourceFileLoader("ticketdesk_campaign_contract",
                         str(Path(__file__).resolve().parents[1] / "check-ticketdesk-lifecycle"))
contract = module_from_spec(spec_from_loader(loader.name, loader))
loader.exec_module(contract)
SECOND = 1000000000


class TimelineError(ValueError):
    """Closed controller refusal without source identities or paths."""


def require(condition, code):
    if not condition:
        raise TimelineError(code)


def integer(value):
    require(type(value) is int and 0 <= value <= 2**64 - 1, "timeline_integer")
    return value


def fenced(function):
    def checked(self, *args, **kwargs):
        require(not self._failed, "timeline_failed")
        try:
            return function(self, *args, **kwargs)
        except TimelineError:
            self._failed = True
            raise
    return checked


class CampaignTimeline:
    def __init__(self, faults, *, duration_s):
        integer(duration_s)
        require(duration_s % contract.TARGET["window_s"] == 0
                and 73 <= duration_s // contract.TARGET["window_s"] <= 288,
                "timeline_duration")
        require(type(faults) is list and len(faults) == len(contract.ACTIONS),
                "timeline_plan")
        final_start = duration_s - 6 * 3600
        previous_end = 0
        seen, frozen = set(), []
        for fault in faults:
            require(type(fault) is dict and fault.keys() == {"kind", "start_s", "duration_s"},
                    "timeline_plan")
            kind = fault["kind"]
            require(type(kind) is str and kind in contract.ACTIONS and kind not in seen,
                    "timeline_plan")
            start = integer(fault["start_s"])
            duration = integer(fault["duration_s"])
            outage = kind in contract.OUTAGES
            require(duration == 300 if outage else 1 <= duration <= 60, "timeline_plan")
            # A whole second of dispatch/release quantization must fit before
            # the following fault and the final quiet period.
            end = start + (duration + 600 if outage else 60) + 1
            require(previous_end < start < end <= final_start, "timeline_plan_overlap")
            previous_end = end
            seen.add(kind)
            frozen.append(MappingProxyType(dict(fault)))
        self._plans = tuple(frozen)
        self._cursor = 0
        self._active = None
        self._last_ns = None
        self._actions = []
        self._duration_ns = duration_s * SECOND
        self._complete = False
        self._failed = False

    @property
    def actions(self):
        return [dict(action) for action in self._actions]

    def _tick(self, elapsed_ns):
        integer(elapsed_ns)
        require(not self._complete and (self._last_ns is None or elapsed_ns >= self._last_ns),
                "timeline_clock")
        if self._active is not None:
            started, released = self._active
            plan = self._plans[self._cursor]
            if released is None:
                require(elapsed_ns < started + (plan["duration_s"] + 1) * SECOND,
                        "timeline_injection_overrun")
            else:
                deadline = (released + 600 * SECOND if plan["kind"] in contract.OUTAGES
                            else started + 60 * SECOND)
                require(elapsed_ns <= deadline, "timeline_recovery_overrun")
        self._last_ns = elapsed_ns

    @fenced
    def due(self, elapsed_ns):
        self._tick(elapsed_ns)
        if self._active is not None or self._cursor == len(self._plans):
            return None
        plan = self._plans[self._cursor]
        if elapsed_ns < plan["start_s"] * SECOND:
            return None
        require(elapsed_ns // SECOND == plan["start_s"], "timeline_start_late")
        return plan["kind"]

    @fenced
    def start(self, kind, elapsed_ns):
        self._tick(elapsed_ns)
        require(self._active is None and self._cursor < len(self._plans), "timeline_active")
        plan = self._plans[self._cursor]
        require(kind == plan["kind"], "timeline_kind")
        require(elapsed_ns // SECOND == plan["start_s"], "timeline_start_late")
        self._active = (elapsed_ns, None)

    @fenced
    def release(self, kind, elapsed_ns):
        self._tick(elapsed_ns)
        require(self._active is not None, "timeline_active")
        plan = self._plans[self._cursor]
        require(kind == plan["kind"], "timeline_kind")
        started, released = self._active
        require(released is None and (elapsed_ns - started) // SECOND == plan["duration_s"],
                "timeline_release")
        self._active = (started, elapsed_ns)

    @fenced
    def finish(self, kind, elapsed_ns, *, reconciled):
        self._tick(elapsed_ns)
        require(self._active is not None, "timeline_active")
        plan = self._plans[self._cursor]
        require(kind == plan["kind"], "timeline_kind")
        _, released = self._active
        require(released is not None, "timeline_release_required")
        require(reconciled is True, "timeline_unreconciled")
        recovery = (elapsed_ns - released + SECOND - 1) // SECOND
        require(recovery <= (600 if kind in contract.OUTAGES else 60 - plan["duration_s"]),
                "timeline_recovery_overrun")
        self._actions.append(dict(plan, recovery_s=recovery, reconciled=True))
        self._active = None
        self._cursor += 1

    @fenced
    def finish_campaign(self, elapsed_ns):
        require(self._active is None and self._cursor == len(self._plans), "timeline_incomplete")
        self._tick(elapsed_ns)
        require(elapsed_ns >= self._duration_ns, "timeline_duration")
        self._complete = True
