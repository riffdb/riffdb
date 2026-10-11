# req: END-007, END-008, END-010
"""Exact live owners and retained exit observations across declared replacements.

This is an incomplete campaign component. The actual controller must route every
launch through this roster, preserve sample gaps and reconcile every descendant.
The frozen allocator ceiling is a conservative address-space bound, never live
allocator ownership or a claim that sequential process peaks were simultaneous.
"""
from copy import deepcopy
from pathlib import Path
import os
import resource
import threading

from .ticketdesk_lifecycle_address_space import MIN_LIMIT, MAX_LIMIT
from .ticketdesk_lifecycle_persistent_usage import ObservedProcess
from .ticketdesk_lifecycle_resources import (
    MAX_U64, OWNED_ROLES, OwnedProcessMemory, ProcessMemory, ResourceError, require,
)

REQUIRED = frozenset({"server", "load", "observer", "native_sampler"})
# One controller, four server owners, five sampler owners, the two original
# workload owners and all 64 serialized operator slots. This is a frozen
# lifetime count, not a concurrency or memory-ceiling increase.
MAX_INCARNATIONS = 1 + 4 + 5 + 2 + 64


def integer(value):
    require(type(value) is int and 0 <= value <= MAX_U64, "resource_roster_number")
    return value


class _LiveTree(OwnedProcessMemory):
    """The same exact census also runs while an explicitly retired role is absent."""

    def __init__(self, controller, owners):
        self.controller = controller
        self.owners = owners


class CampaignProcessRoster:
    def __init__(self, controller, children, limits, *, proc_root=Path("/proc")):
        require(type(controller) is int and controller == os.getpid() and os.geteuid() != 0,
                "resource_roster_controller")
        require(type(children) is dict and children.keys() == REQUIRED,
                "resource_process_inventory")
        require(type(limits) is dict and {"controller", *REQUIRED} <= limits.keys()
                and limits.keys() <= OWNED_ROLES
                and all(type(v) is int and MIN_LIMIT <= v <= MAX_LIMIT for v in limits.values()),
                "resource_roster_limits")
        hard = resource.getrlimit(resource.RLIMIT_AS)[1]
        require(type(hard) is int and hard == limits["controller"]
                and all(limit <= hard for limit in limits.values()), "resource_roster_controller")
        self.controller, self.proc_root = controller, Path(proc_root)
        self.thread = threading.get_ident()
        self.limits = dict(limits)
        self.owners = {"controller": ProcessMemory(controller, proc_root=self.proc_root)}
        memory = self.owners["controller"].tree_sample()[0]
        require(memory["allocator_peak_bytes"] <= hard, "resource_roster_controller")
        self.children = {}
        self.roles = {"controller": dict(incarnations=1,
            rss_peak_upper_bound_bytes=memory["rss_peak_bytes"],
            allocator_peak_upper_bound_bytes=hard)}
        self.count, self.retired, self.last_ns = 1, 0, None
        self.failed = False
        self.launching = False
        # At most one serialized launcher can be between fork and enforcement.
        # Other workers retain their frozen hard ceilings. Birth may inherit
        # the larger controller ceiling; do not sum birth peaks of sequential
        # launches or discard that larger bound when a worker exits.
        self.allocator_bound = sum(limits.values()) + max(
            [0, *(max(hard - limit, 0) for role, limit in limits.items() if role != "controller")])
        require(self.allocator_bound <= MAX_LIMIT, "resource_roster_limits")
        for role, child in children.items():
            self._adopt(role, child)

    def _check(self):
        require(not self.failed, "resource_roster_failed")
        require(threading.get_ident() == self.thread, "resource_roster_thread")

    def _proof(self, role, child):
        require(isinstance(child, ObservedProcess), "resource_roster_owner")
        value = child.usage
        require(value.get("launch_observed") is True, "resource_roster_launch")
        proof = value.get("address_space_proof")
        require(type(proof) is dict and proof.get("enforced") is True
                and type(proof.get("limit_bytes")) is int
                and proof["limit_bytes"] == self.limits[role]
                and type(proof.get("lifetime_upper_bound_bytes")) is int
                and 0 < proof["lifetime_upper_bound_bytes"] <= self.limits["controller"],
                "resource_roster_birth")
        return value, proof

    def _adopt(self, role, child):
        """Admit an exact owner; an existing role must have been reaped first."""
        self._check()
        try:
            require(type(role) is str and role != "controller" and role in self.limits
                    and role not in self.children and self.count < MAX_INCARNATIONS,
                    "resource_roster_owner")
            value, proof = self._proof(role, child)
            require(child.pid not in {owner.pid for owner in self.owners.values()},
                    "resource_roster_launch")
            memory = None
            if child.poll() is None:
                owner = ProcessMemory(child.pid, proc_root=self.proc_root)
                try:
                    memory = owner.tree_sample()[0]
                except ResourceError:
                    if child.poll() is None:
                        raise
                if memory is not None:
                    require(memory["allocator_peak_bytes"] <= proof["limit_bytes"], "resource_roster_birth")
                    self.owners[role] = owner
            # A short CLI child may already have exited. Its exact owner and
            # wait4 record remain pending until explicit expected-exit retirement;
            # no live census or successful operation is fabricated for it.
            self.children[role] = child
            previous = self.roles.get(role, dict(incarnations=0, rss_peak_upper_bound_bytes=0,
                                               allocator_peak_upper_bound_bytes=0))
            self.roles[role] = dict(incarnations=previous["incarnations"] + 1,
                rss_peak_upper_bound_bytes=max(previous["rss_peak_upper_bound_bytes"], memory["rss_peak_bytes"] if memory else 0),
                allocator_peak_upper_bound_bytes=max(previous["allocator_peak_upper_bound_bytes"],
                                                       proof["lifetime_upper_bound_bytes"]))
            self.count += 1
        except BaseException:
            self.failed = True
            raise

    def _spawn(self, arguments, **options):
        return ObservedProcess(arguments, **options)

    def launch(self, role, arguments, **options):
        """Serialize birth, enforcement, admission and eventual exact ownership."""
        self._check()
        require(not self.launching and type(role) is str and role in self.limits
                and role != "controller" and role not in self.children
                and self.count < MAX_INCARNATIONS
                and "address_space_limit_bytes" not in options, "resource_roster_launch")
        self.launching = True
        child = None
        try:
            child = self._spawn(arguments, address_space_limit_bytes=self.limits[role], **options)
            self._adopt(role, child)
            return child
        except BaseException:
            self.failed = True
            if child is not None and child.poll() is None:
                child.kill()
                child.wait(timeout=5)
            raise
        finally:
            self.launching = False

    def retire(self, role, child, *, expected_exit):
        self._check()
        try:
            require(type(role) is str and self.children.get(role) is child
                    and type(expected_exit) is int and -64 <= expected_exit <= 255,
                    "resource_roster_owner")
            require(child.poll() == expected_exit, "resource_roster_exit")
            value, proof = self._proof(role, child)
            peak = value.get("exit_rss_peak_bytes")
            require(value.get("exit_observed") is True
                    and type(value.get("exit_code")) is int and value["exit_code"] == expected_exit
                    and type(peak) is int and 0 < peak <= proof["lifetime_upper_bound_bytes"]
                    and type(value.get("exit_allocator_peak_upper_bound_bytes")) is int
                    and value["exit_allocator_peak_upper_bound_bytes"] == proof["lifetime_upper_bound_bytes"],
                    "resource_roster_exit")
            record = self.roles[role]
            record["rss_peak_upper_bound_bytes"] = max(record["rss_peak_upper_bound_bytes"], peak)
            self.owners.pop(role, None)
            del self.children[role]
            self.retired += 1
        except BaseException:
            self.failed = True
            raise

    def sample(self, elapsed_ns):
        self._check()
        try:
            integer(elapsed_ns)
            require(self.last_ns is None or elapsed_ns >= self.last_ns, "resource_sample_clock")
            require(not self.launching, "resource_roster_launch")
            for role, child in self.children.items():
                self._proof(role, child)
                require(child.poll() is None, "resource_roster_exit")
            value = _LiveTree(self.controller, self.owners).sample(elapsed_ns)
            for role, memory in value["processes"].items():
                require(memory["allocator_peak_bytes"] <= self.limits[role], "resource_roster_limits")
                record = self.roles[role]
                record["rss_peak_upper_bound_bytes"] = max(record["rss_peak_upper_bound_bytes"],
                                                           memory["rss_peak_bytes"])
            value.update(schema="riffdb.ticketdesk-lifecycle-campaign-roster-memory/v1",
                roles=deepcopy(self.roles), incarnations=self.count, retired_incarnations=self.retired,
                rss_peak_upper_bound_bytes=sum(r["rss_peak_upper_bound_bytes"] for r in self.roles.values()),
                allocator_peak_upper_bound_bytes=self.allocator_bound,
                allocator_peak_basis="frozen_worker_limits_plus_one_serial_launcher_birth_bound",
                qualifies_resource_model=False)
            require(value["rss_peak_upper_bound_bytes"] <= MAX_U64, "resource_measurement")
            self.last_ns = elapsed_ns
            return value
        except BaseException:
            self.failed = True
            raise
