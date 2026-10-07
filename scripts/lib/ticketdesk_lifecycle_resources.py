# req: END-007, END-008, END-010
"""Read-only Linux process observations with explicit conservative accounting.

VmSize bounds allocator ownership by including the whole virtual address space.
It also includes executable and file mappings, stacks and reserved/free address
space. It is never presented as allocated live bytes. VmData is not substituted
for an allocator measurement. A campaign must freeze this basis and its limit.
"""
import errno
import os
import json
from pathlib import Path
import re
import stat


ALLOCATOR_BASIS = "whole_process_virtual_address_space_upper_bound"
MAX_STATUS_BYTES = 65536
MAX_STAT_BYTES = 8192
MAX_U64 = 2**64 - 1
MAX_PREPARATION_SAMPLE_SECONDS = 1500
MAX_OWNED_THREADS = 512
MAX_OWNED_PROCESSES = 8
OWNED_ROLES = frozenset({"controller", "server", "load", "observer", "native_sampler",
                         "typescript_driver", "operator"})


class ResourceError(ValueError):
    """Closed diagnostics contain no process name, identity, path or input text."""


class _TaskCensusChanged(Exception):
    """An enumerated thread retired; retain every child already observed."""

    def __init__(self, children):
        self.children = frozenset(children)


def retired_task(task, name, error, children):
    if error.errno in (errno.ENOENT, errno.ESRCH):
        try:
            os.stat(name, dir_fd=task, follow_symlinks=False)
        except OSError as later:
            if later.errno in (errno.ENOENT, errno.ESRCH):
                raise _TaskCensusChanged(children) from error
    raise error


def require(condition, code):
    if not condition:
        raise ResourceError(code)


def number(encoded):
    require(re.fullmatch(rb"0|[1-9][0-9]{0,19}", encoded) is not None,
            "resource_measurement")
    value = int(encoded)
    require(value <= MAX_U64, "resource_measurement")
    return value


def read_bounded(directory, name, maximum):
    descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                         dir_fd=directory)
    with os.fdopen(descriptor, "rb") as source:
        require(stat.S_ISREG(os.fstat(source.fileno()).st_mode), "resource_measurement")
        value = source.read(maximum + 1)
    require(0 < len(value) <= maximum, "resource_measurement")
    return value


def identity(encoded, expected_pid):
    # Linux stat encloses comm in parentheses; comm can contain parentheses.
    prefix, separator, suffix = encoded.rpartition(b") ")
    pid, opening, _name = prefix.partition(b" (")
    fields = suffix.split()
    require(separator and opening and number(pid) == expected_pid
            and 20 <= len(fields) <= 64, "resource_process_identity")
    require(fields[0] in (b"R", b"S", b"D", b"T", b"t", b"I", b"W", b"P"),
            "resource_process_unavailable")
    start = number(fields[19])
    require(start > 0, "resource_process_identity")
    return start


def memory(encoded, expected_pid):
    selected = {}
    for line in encoded.splitlines():
        key, separator, value = line.partition(b":")
        if key not in (b"State", b"Pid", b"VmRSS", b"VmSize"):
            continue
        require(separator and key not in selected, "resource_measurement")
        selected[key] = value.split()
    require(selected.keys() == {b"State", b"Pid", b"VmRSS", b"VmSize"},
            "resource_measurement")
    require(len(selected[b"Pid"]) == 1 and number(selected[b"Pid"][0]) == expected_pid,
            "resource_process_identity")
    require(selected[b"State"] and selected[b"State"][0]
            in (b"R", b"S", b"D", b"T", b"t", b"I", b"W", b"P"),
            "resource_process_unavailable")
    values = []
    for key in (b"VmRSS", b"VmSize"):
        fields = selected[key]
        require(len(fields) == 2 and fields[1] == b"kB", "resource_measurement")
        size = number(fields[0]) * 1024
        require(size <= MAX_U64, "resource_measurement")
        values.append(size)
    require(0 < values[0] <= values[1], "resource_measurement")
    return {"rss_bytes": values[0], "allocator_bytes": values[1],
            "allocator_basis": ALLOCATOR_BASIS}


def memory_peaks(encoded, current):
    selected = {}
    for line in encoded.splitlines():
        key, separator, value = line.partition(b":")
        if key not in (b"VmHWM", b"VmPeak"):
            continue
        require(separator and key not in selected, "resource_measurement")
        fields = value.split()
        require(len(fields) == 2 and fields[1] == b"kB", "resource_measurement")
        selected[key] = number(fields[0]) * 1024
    require(selected.keys() == {b"VmHWM", b"VmPeak"}, "resource_measurement")
    rss, virtual = selected[b"VmHWM"], selected[b"VmPeak"]
    require(current["rss_bytes"] <= rss <= virtual <= MAX_U64
            and current["allocator_bytes"] <= virtual, "resource_measurement")
    return {"rss_peak_bytes": rss, "allocator_peak_bytes": virtual}


def thread_children(directory):
    """Inspect every bounded task, since children belong to the spawning thread."""
    task = os.open("task", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
    try:
        children = set()
        count = 0
        with os.scandir(task) as entries:
            for entry in entries:
                count += 1
                require(count <= MAX_OWNED_THREADS, "resource_process_inventory")
                tid = number(entry.name.encode())
                require(0 < tid <= 2**31 - 1, "resource_process_inventory")
                try:
                    thread = os.open(entry.name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                                     dir_fd=task)
                except OSError as error:
                    retired_task(task, entry.name, error, children)
                try:
                    try:
                        descriptor = os.open("children", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                                             dir_fd=thread)
                    except OSError as error:
                        retired_task(task, entry.name, error, children)
                    with os.fdopen(descriptor, "rb") as source:
                        require(stat.S_ISREG(os.fstat(source.fileno()).st_mode),
                                "resource_process_inventory")
                        encoded = source.read(MAX_STAT_BYTES + 1)
                    require(len(encoded) <= MAX_STAT_BYTES, "resource_process_inventory")
                    for field in encoded.split():
                        pid = number(field)
                        require(0 < pid <= 2**31 - 1 and pid not in children
                                and len(children) < MAX_OWNED_PROCESSES,
                                "resource_process_inventory")
                        children.add(pid)
                finally:
                    os.close(thread)
        require(count > 0, "resource_process_inventory")
        return children
    finally:
        os.close(task)


class ProcessMemory:
    """One owned process incarnation; replacement needs a separate owner.

    Reading stat on both sides of status refuses exit/replacement races. The
    directory descriptor pins the selected proc object during this observation.
    Start-time identity stays private and must match across subsequent samples.
    """

    def __init__(self, pid, *, proc_root=Path("/proc")):
        require(type(pid) is int and 0 < pid <= 2**31 - 1, "resource_process_identity")
        self.pid = pid
        self.root = Path(proc_root)
        self.start = None
        self.peaks = None
        self.task_census_restarts = 0

    def sample(self):
        descriptor = None
        try:
            descriptor = os.open(self.root / str(self.pid),
                                 os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            before = identity(read_bounded(descriptor, "stat", MAX_STAT_BYTES), self.pid)
            require(self.start is None or before == self.start, "resource_process_identity")
            value = memory(read_bounded(descriptor, "status", MAX_STATUS_BYTES), self.pid)
            after = identity(read_bounded(descriptor, "stat", MAX_STAT_BYTES), self.pid)
            require(before == after, "resource_process_identity")
            self.start = before
            return value
        except OSError as error:
            raise ResourceError("resource_process_unavailable") from error
        finally:
            if descriptor is not None:
                os.close(descriptor)

    def tree_sample(self):
        """Bracket memory with task-child and process-incarnation observations."""
        observed = set()
        for _attempt in range(3):
            try:
                value, children = self._tree_sample_once()
                require(observed <= children, "resource_process_inventory")
                value["task_census_restarts"] = self.task_census_restarts
                return value, children
            except _TaskCensusChanged as error:
                observed.update(error.children)
                self.task_census_restarts += 1
                require(self.task_census_restarts <= MAX_U64, "resource_process_inventory")
        raise ResourceError("resource_process_inventory")

    def _tree_sample_once(self):
        directory = None
        try:
            directory = os.open(self.root / str(self.pid),
                                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            before = identity(read_bounded(directory, "stat", MAX_STAT_BYTES), self.pid)
            require(self.start is None or before == self.start, "resource_process_identity")
            # Pin the incarnation even if a disappearing task aborts this pass.
            self.start = before
            children = thread_children(directory)
            encoded = read_bounded(directory, "status", MAX_STATUS_BYTES)
            current = memory(encoded, self.pid)
            value = dict(current, **memory_peaks(encoded, current))
            peaks = (value["rss_peak_bytes"], value["allocator_peak_bytes"])
            # RSS counters are asynchronous on Linux. Keep the actual reported
            # reading, and never lower the maximum already observed for this
            # same live incarnation. Virtual-address high water still cannot
            # rewind; replacement, missing fields and invalid units still fail.
            require(self.peaks is None or peaks[1] >= self.peaks[1], "resource_measurement")
            value["reported_rss_peak_bytes"] = peaks[0]
            value["reported_allocator_peak_bytes"] = peaks[1]
            peaks = (max(peaks[0], self.peaks[0] if self.peaks else 0), peaks[1])
            value["rss_peak_bytes"] = peaks[0]
            self.peaks = peaks
            try:
                later_children = thread_children(directory)
            except _TaskCensusChanged as error:
                raise _TaskCensusChanged(children | error.children) from error
            require(children == later_children, "resource_process_inventory")
            require(before == identity(read_bounded(directory, "stat", MAX_STAT_BYTES), self.pid),
                    "resource_process_identity")
            self.start = before
            self.peaks = peaks
            return value, children
        except OSError as error:
            raise ResourceError("resource_process_unavailable") from error
        finally:
            if directory is not None:
                os.close(directory)


class OwnedProcessMemory:
    """Known persistent owners, with an exact descendant census at each sample.

    This refuses untracked CLI children. Lifetime high-water observations cover
    peaks of the inventoried live incarnations; their sum is conservative rather
    than a claim that each peak occurred simultaneously. Transient descendants
    between censuses still require an independent launch/exit inventory. This
    collector alone never establishes the campaign resource-model check.
    """

    def __init__(self, pids, *, proc_root=Path("/proc")):
        require(type(pids) is dict and {"controller", "server", "load", "observer"} <= pids.keys()
                and pids.keys() <= OWNED_ROLES and len(pids) <= MAX_OWNED_PROCESSES,
                "resource_process_inventory")
        require(all(type(pid) is int for pid in pids.values())
                and len(set(pids.values())) == len(pids), "resource_process_inventory")
        self.controller = pids["controller"]
        self.owners = {role: ProcessMemory(pid, proc_root=proc_root) for role, pid in pids.items()}

    def sample(self, elapsed_ns):
        require(type(elapsed_ns) is int and 0 <= elapsed_ns <= MAX_U64, "resource_measurement")
        values, edges = {}, {}
        known = {owner.pid for owner in self.owners.values()}
        fields = ("rss_bytes", "allocator_bytes", "rss_peak_bytes", "allocator_peak_bytes",
                  "reported_rss_peak_bytes", "reported_allocator_peak_bytes")
        for role, owner in self.owners.items():
            sample, children = owner.tree_sample()
            require(children <= known and owner.pid not in children, "resource_process_inventory")
            edges[owner.pid] = children
            values[role] = {name: sample[name] for name in fields}
            values[role]["task_census_restarts"] = sample["task_census_restarts"]
        # Every non-root belongs to exactly one owner, and all owners connect
        # to the controller. A disconnected cycle must not look like a tree.
        parents = [pid for children in edges.values() for pid in children]
        require(len(parents) == len(set(parents))
                and set(parents) == known - {self.controller}, "resource_process_inventory")
        reached, pending = set(), [self.controller]
        while pending:
            current = pending.pop()
            require(current not in reached, "resource_process_inventory")
            reached.add(current)
            pending.extend(edges[current])
        require(reached == known, "resource_process_inventory")
        for role, owner in self.owners.items():
            later, children = owner.tree_sample()
            require(children == edges[owner.pid], "resource_process_inventory")
            for name in ("rss_peak_bytes", "allocator_peak_bytes"):
                values[role][name] = max(values[role][name], later[name])
            values[role]["later_reported_rss_peak_bytes"] = later["reported_rss_peak_bytes"]
            values[role]["later_reported_allocator_peak_bytes"] = later["reported_allocator_peak_bytes"]
            values[role]["task_census_restarts"] = later["task_census_restarts"]
        totals = {name: sum(value[name] for value in values.values()) for name in fields}
        require(all(value <= MAX_U64 for value in totals.values()), "resource_measurement")
        return {"schema": "riffdb.ticketdesk-lifecycle-owned-memory/v1", "elapsed_ns": elapsed_ns,
                "allocator_basis": ALLOCATOR_BASIS,
                "rss_peak_basis": "maximum_observed_asynchronous_kernel_high_water_counter",
                "rss_observation_basis": "asynchronous_linux_kernel_rss_counter",
                "scope": "verified_owned_descendant_tree_at_sample", "processes": values,
                "rss_bytes": totals["rss_bytes"], "allocator_bytes": totals["allocator_bytes"],
                "rss_peak_upper_bound_bytes": totals["rss_peak_bytes"],
                "allocator_peak_upper_bound_bytes": totals["allocator_peak_bytes"]}


class PersistentProcessMemory:
    """Preparation scope: four persistent owned processes, excluding CLI children.

    This diagnostic is incomplete campaign process accounting. In particular,
    transient CLI children and peaks between samples are not included. No
    qualifying resource-model check may be inferred from these observations.
    """

    def __init__(self, pids, *, proc_root=Path("/proc")):
        roles = {"controller", "server", "load", "observer"}
        require(type(pids) is dict and pids.keys() == roles, "resource_process_inventory")
        require(all(type(pid) is int for pid in pids.values())
                and len(set(pids.values())) == len(roles), "resource_process_inventory")
        self.owners = {role: ProcessMemory(pid, proc_root=proc_root)
                       for role, pid in pids.items()}

    def sample(self, elapsed_ns):
        require(type(elapsed_ns) is int and 0 <= elapsed_ns <= MAX_U64,
                "resource_measurement")
        values = {role: owner.sample() for role, owner in self.owners.items()}
        measurements = {role: {name: value[name] for name in ("rss_bytes", "allocator_bytes")}
                        for role, value in values.items()}
        require(all(sum(value[name] for value in measurements.values()) <= MAX_U64
                    for name in ("rss_bytes", "allocator_bytes")), "resource_measurement")
        return {"schema": "riffdb.ticketdesk-lifecycle-memory-preparation/v1",
                "elapsed_ns": elapsed_ns, "allocator_basis": ALLOCATOR_BASIS,
                "scope": "four_persistent_processes_excluding_transient_cli_children",
                "qualifies_resource_model": False, "processes": measurements}


class PreparationMemorySamples:
    """Caller-polled diagnostics, with no extra request slot or sampling thread."""

    def __init__(self, root, pids, duration, *, proc_root=Path("/proc")):
        require(type(duration) is int and 1 <= duration <= MAX_PREPARATION_SAMPLE_SECONDS,
                "resource_sample_bound")
        self.inventory = (OwnedProcessMemory(pids, proc_root=proc_root) if "native_sampler" in pids
                          else PersistentProcessMemory(pids, proc_root=proc_root))
        self.duration_ns = duration * 1000000000
        self.coverage = [0] * ((duration + 299) // 300)
        self.last_elapsed_ns = None
        self.last_sample_ns = None
        self.count = 0
        self.path = Path(root) / "memory-samples.jsonl"
        self.output = None

    def __enter__(self):
        descriptor = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                             0o600)
        self.output = os.fdopen(descriptor, "wb")
        return self

    def poll(self, elapsed_ns):
        require(type(elapsed_ns) is int and 0 <= elapsed_ns <= MAX_U64
                and (self.last_elapsed_ns is None or elapsed_ns >= self.last_elapsed_ns),
                "resource_sample_clock")
        self.last_elapsed_ns = elapsed_ns
        if elapsed_ns >= self.duration_ns:
            return
        if self.last_sample_ns is not None and elapsed_ns - self.last_sample_ns < 5000000000:
            return
        require(self.output is not None and self.count < 2048, "resource_sample_bound")
        value = self.inventory.sample(elapsed_ns)
        # A persistent census still cannot prove transient launch/exit coverage.
        value["qualifies_resource_model"] = False
        encoded = json.dumps(value, sort_keys=True).encode() + b"\n"
        require(len(encoded) <= 4096, "resource_sample_bound")
        self.output.write(encoded)
        self.output.flush()
        self.count += 1
        self.coverage[elapsed_ns // 300000000000] += 1
        self.last_sample_ns = elapsed_ns

    def finish(self):
        require(all(count > 0 for count in self.coverage), "resource_sample_coverage")

    def __exit__(self, _kind, _error, _traceback):
        if self.output is not None:
            self.output.close()
            self.output = None
        return False
