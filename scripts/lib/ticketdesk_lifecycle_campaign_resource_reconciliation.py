# req: END-007, END-008, END-010
"""Independently reconcile closed memory and whole-file resource observations.

This component checks the original clock and complete file-accounting basis.
Actual launch/exit custody, source identity, native history and interference
remain separate required inputs; this arithmetic cannot qualify a campaign.
"""
import hashlib
import json
import os
from pathlib import Path
import stat

from .ticketdesk_lifecycle_campaign_disk import BASIS as DISK_BASIS, FIELDS as DISK_FIELDS
from .ticketdesk_lifecycle_campaign_process_roster import MAX_INCARNATIONS, REQUIRED
from .ticketdesk_lifecycle_resources import ALLOCATOR_BASIS, OWNED_ROLES, MAX_U64
from .ticketdesk_lifecycle_load_stream import unique_object, parse_integer, reject_number

SECOND = 10**9
GAP = 75*SECOND
MEMORY_FIELDS = frozenset({"schema", "elapsed_ns", "allocator_basis", "rss_peak_basis",
    "rss_observation_basis", "scope", "processes", "rss_bytes", "allocator_bytes",
    "rss_peak_upper_bound_bytes", "allocator_peak_upper_bound_bytes", "roles", "incarnations",
    "retired_incarnations", "allocator_peak_basis", "qualifies_resource_model"})
PROCESS_FIELDS = frozenset({"rss_bytes", "allocator_bytes", "rss_peak_bytes", "allocator_peak_bytes",
    "reported_rss_peak_bytes", "reported_allocator_peak_bytes", "task_census_restarts",
    "later_reported_rss_peak_bytes", "later_reported_allocator_peak_bytes"})
ROLE_FIELDS = frozenset({"incarnations", "rss_peak_upper_bound_bytes", "allocator_peak_upper_bound_bytes"})
DISK_RECORD_FIELDS = frozenset({"schema", "elapsed_ns", "basis", "files_and_directories",
    "directories", "resources", "complete_resource_model_proved"})


class ResourceReconciliationError(ValueError):
    """Closed refusal without raw observations or machine details."""


def require(condition, code):
    if not condition: raise ResourceReconciliationError(code)


def integer(value):
    require(type(value) is int and 0 <= value <= MAX_U64, "campaign_resource_number")
    return value


def read_closed(root, name, *, maximum_rows):
    require(name in {"memory-samples.jsonl", "disk-samples.jsonl"}, "campaign_resource_input")
    root = Path(root)
    require(root.is_absolute() and root.resolve() == root, "campaign_resource_custody")
    directory = os.open(root, os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try:
        info = os.fstat(directory)
        require(info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) == 0o700,
                "campaign_resource_custody")
        descriptor = os.open(name, os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK, dir_fd=directory)
        with os.fdopen(descriptor, "rb") as source:
            before = os.fstat(source.fileno())
            require(stat.S_ISREG(before.st_mode) and before.st_uid == os.geteuid()
                    and stat.S_IMODE(before.st_mode) == 0o600 and before.st_nlink == 1
                    and 0 < before.st_size <= maximum_rows*4096, "campaign_resource_custody")
            rows, size, digest = [], 0, hashlib.sha256()
            while line := source.readline(4097):
                require(len(rows) < maximum_rows and len(line) <= 4096 and line.endswith(b"\n"),
                        "campaign_resource_bound")
                size += len(line); digest.update(line)
                rows.append(json.loads(line, object_pairs_hook=unique_object, parse_int=parse_integer,
                                       parse_float=reject_number, parse_constant=reject_number))
            after = os.fstat(source.fileno())
        named = os.stat(name, dir_fd=directory, follow_symlinks=False)
        identity = lambda value: (value.st_dev,value.st_ino,value.st_size,value.st_mtime_ns,value.st_ctime_ns)
        root_after = root.lstat()
        require(size == before.st_size and identity(before) == identity(after) == identity(named)
                and (info.st_dev,info.st_ino) == (root_after.st_dev,root_after.st_ino)
                and stat.S_IMODE(root_after.st_mode) == 0o700,
                "campaign_resource_changed")
        return rows, dict(sha256=digest.hexdigest(), bytes=size, samples=len(rows))
    except ResourceReconciliationError: raise
    except (OSError,ValueError,UnicodeError,RecursionError) as error:
        raise ResourceReconciliationError("campaign_resource_input") from error
    finally: os.close(directory)


def checked_memory(row, limits):
    require(type(row) is dict and row.keys() == MEMORY_FIELDS
            and row["schema"] == "riffdb.ticketdesk-lifecycle-campaign-roster-memory/v1"
            and row["allocator_basis"] == ALLOCATOR_BASIS
            and row["rss_peak_basis"] == "maximum_observed_asynchronous_kernel_high_water_counter"
            and row["rss_observation_basis"] == "asynchronous_linux_kernel_rss_counter"
            and row["scope"] == "verified_owned_descendant_tree_at_sample"
            and row["allocator_peak_basis"] == "frozen_worker_limits_plus_one_serial_launcher_birth_bound"
            and row["qualifies_resource_model"] is False, "campaign_resource_memory_shape")
    processes, roles = row["processes"], row["roles"]
    require(type(processes) is dict and "controller" in processes and processes.keys() <= limits.keys()
            and type(roles) is dict and {"controller",*REQUIRED} <= roles.keys()
            and processes.keys() <= roles.keys() <= limits.keys(), "campaign_resource_inventory")
    for role, value in processes.items():
        require(type(value) is dict and value.keys() == PROCESS_FIELDS, "campaign_resource_process")
        for item in value.values(): integer(item)
        require(0 < value["rss_bytes"] <= value["allocator_bytes"] <= value["allocator_peak_bytes"] <= limits[role]
                and value["rss_bytes"] <= value["rss_peak_bytes"] <= value["allocator_peak_bytes"]
                and value["rss_bytes"] <= value["reported_rss_peak_bytes"] <= value["rss_peak_bytes"]
                and value["reported_rss_peak_bytes"] <= value["reported_allocator_peak_bytes"] <= value["allocator_peak_bytes"]
                and value["later_reported_rss_peak_bytes"] <= value["rss_peak_bytes"]
                and value["later_reported_rss_peak_bytes"] <= value["later_reported_allocator_peak_bytes"] <= value["allocator_peak_bytes"],
                "campaign_resource_process")
    for role, value in roles.items():
        require(type(value) is dict and value.keys() == ROLE_FIELDS, "campaign_resource_role")
        for item in value.values(): integer(item)
        require(0 < value["incarnations"] <= MAX_INCARNATIONS
                and 0 < value["rss_peak_upper_bound_bytes"] <= value["allocator_peak_upper_bound_bytes"] <= limits["controller"],
                "campaign_resource_role")
        if role in processes:
            require(processes[role]["rss_peak_bytes"] <= value["rss_peak_upper_bound_bytes"],
                    "campaign_resource_role")
    require(integer(row["incarnations"]) == sum(value["incarnations"] for value in roles.values()) <= MAX_INCARNATIONS
            and integer(row["retired_incarnations"]) + len(processes) == row["incarnations"],
            "campaign_resource_inventory")
    for field in ("rss_bytes", "allocator_bytes"):
        require(integer(row[field]) == sum(value[field] for value in processes.values()),
                "campaign_resource_totals")
    ceiling = sum(limits.values()) + max([0,*(limits["controller"]-limit for role,limit in limits.items() if role!="controller")])
    require(integer(row["rss_peak_upper_bound_bytes"]) == sum(value["rss_peak_upper_bound_bytes"] for value in roles.values())
            and integer(row["allocator_peak_upper_bound_bytes"]) == ceiling,
            "campaign_resource_totals")
    return dict(rss_bytes=row["rss_peak_upper_bound_bytes"], allocator_bytes=ceiling)


def checked_disk(row, limits):
    require(type(row) is dict and row.keys() == DISK_RECORD_FIELDS
            and row["schema"] == "riffdb.ticketdesk-lifecycle-disk-sample/v1"
            and row["basis"] == DISK_BASIS and row["complete_resource_model_proved"] is False
            and type(row["resources"]) is dict and row["resources"].keys() == DISK_FIELDS,
            "campaign_resource_disk_shape")
    require(integer(row["directories"]) <= 1024 and row["directories"] > 0
            and row["directories"]-1 <= integer(row["files_and_directories"]) <= 8192,
            "campaign_resource_disk_inventory")
    for key,value in row["resources"].items():
        require(integer(value) <= limits[key], "campaign_resource_limit")
    return dict(row["resources"])


def reconcile(memory_rows, disk_rows, *, duration_s, process_limits, resource_limits, terminal_memory):
    require(type(duration_s) is int and duration_s%300 == 0 and 73 <= duration_s//300 <= 288,
            "campaign_resource_duration")
    require(type(process_limits) is dict and process_limits.keys() == {"controller",*REQUIRED,"operator"}
            and process_limits.keys() <= OWNED_ROLES
            and all(type(value) is int and 64*1024**2 <= value <= 32*1024**3 for value in process_limits.values())
            and all(value <= process_limits["controller"] for value in process_limits.values()),
            "campaign_resource_model")
    require(type(resource_limits) is dict and resource_limits.keys() == DISK_FIELDS|{"rss_bytes","allocator_bytes"}
            and all(type(value) is int and 0 < value <= MAX_U64 for value in resource_limits.values()),
            "campaign_resource_model")
    windows = [dict.fromkeys(resource_limits,0) for _ in range(duration_s//300)]
    for kind,rows,maximum,check in (("memory",memory_rows,2048,lambda row:checked_memory(row,process_limits)),
                                  ("disk",disk_rows,4096,lambda row:checked_disk(row,resource_limits))):
        require(type(rows) is list and 0 < len(rows) <= maximum, "campaign_resource_bound")
        coverage, previous = [0]*len(windows), None
        for row in rows:
            values = check(row); elapsed = integer(row["elapsed_ns"])
            require(elapsed <= duration_s*SECOND+GAP and (previous is None or elapsed > previous["elapsed_ns"])
                    and elapsed-(previous["elapsed_ns"] if previous else 0) <= GAP, "campaign_resource_gap")
            if kind == "memory" and previous is not None:
                require(previous["roles"].keys() <= row["roles"].keys()
                        and all(row["roles"][role][field] >= value for role,entry in previous["roles"].items()
                                for field,value in entry.items()), "campaign_resource_peak_rewind")
            if elapsed >= duration_s*SECOND: require(kind == "disk" and row is rows[-1], "campaign_resource_clock")
            index = min(elapsed//(300*SECOND),len(windows)-1); coverage[index] += 1
            for key,value in values.items():
                require(value <= resource_limits[key], "campaign_resource_limit")
                windows[index][key] = max(windows[index][key],value)
            previous = row
        require(all(coverage) and duration_s*SECOND-previous["elapsed_ns"] <= GAP,
                "campaign_resource_coverage")
        if kind == "disk": require(previous["elapsed_ns"] >= duration_s*SECOND, "campaign_resource_terminal")
    terminal = checked_memory(terminal_memory,process_limits)
    require(duration_s*SECOND <= integer(terminal_memory["elapsed_ns"]) <= duration_s*SECOND+GAP
            and terminal_memory["elapsed_ns"]-memory_rows[-1]["elapsed_ns"] <= GAP,
            "campaign_resource_terminal")
    previous = memory_rows[-1]
    require(previous["roles"].keys() <= terminal_memory["roles"].keys()
            and all(terminal_memory["roles"][role][field] >= value for role,entry in previous["roles"].items()
                    for field,value in entry.items()), "campaign_resource_peak_rewind")
    for key,value in terminal.items():
        require(value <= resource_limits[key], "campaign_resource_limit")
        windows[-1][key] = max(windows[-1][key],value)
    return dict(schema="riffdb.ticketdesk-lifecycle-resource-reconciliation/v1",
        duration_s=duration_s, windows=windows, memory_basis=ALLOCATOR_BASIS, disk_basis=DISK_BASIS,
        qualifies_complete_resource_model=False)
