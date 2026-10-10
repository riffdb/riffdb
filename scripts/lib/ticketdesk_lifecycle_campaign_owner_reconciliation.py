# req: END-007, END-008, END-010
"""Bind every closed campaign child to its actual launch and wait4 evidence.

Original preparation owners are outside this measured ledger. The original
server and observer lifetimes can include preparation and final shutdown;
their retained peaks are conservative bounds, never simultaneous measurements.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import stat

from .ticketdesk_lifecycle_address_space import BASIS
from .ticketdesk_lifecycle_load_stream import unique_object, reject_number

SECOND = 10**9
USAGE_FIELDS = frozenset({"schema", "launch_observed", "exit_observed", "exit_code", "elapsed_ns",
    "launch_memory", "launch_memory_refusal", "exit_rss_peak_bytes", "exit_allocator_peak_bytes",
    "exit_allocator_peak_upper_bound_bytes", "worker_exit_allocator_peak_upper_bound_bytes",
    "address_space_proof", "exit_rss_basis", "allocator_basis", "qualifies_complete_resource_model"})
PROOF_FIELDS = frozenset({"enforced", "limit_bytes", "launch_virtual_peak_bytes",
    "inherited_hard_limit_bytes", "lifetime_upper_bound_bytes"})
OPERATOR_FIELDS = frozenset({"schema", "index", "command", "elapsed_ns", "duration_ns", "exit_code",
    "outcome_known", "qualifies_complete_campaign"})


class OwnerReconciliationError(ValueError):
    """Closed diagnostics without private paths, arguments or credentials."""


def require(condition, code):
    if not condition: raise OwnerReconciliationError(code)


def integer(value, *, maximum=2**64-1):
    require(type(value) is int and 0 <= value <= maximum, "campaign_owner_number")
    return value


def read_private(root, name, *, maximum):
    parts = name.split("/")
    require(1 <= len(parts) <= 2 and all(re.fullmatch(r"[A-Za-z0-9_.-]{1,80}",part)
            and part not in {".",".."} for part in parts), "campaign_owner_input")
    root = Path(root)
    require(root.is_absolute() and root.resolve() == root, "campaign_owner_custody")
    directory = os.open(root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    root_info = os.fstat(directory)
    descendant_info = None
    try:
        for part in parts[:-1]:
            info = os.fstat(directory)
            require(info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) == 0o700,
                    "campaign_owner_custody")
            child = os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=directory)
            os.close(directory);directory=child
            descendant_info = os.fstat(directory)
        info = os.fstat(directory)
        require(info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) == 0o700,
                "campaign_owner_custody")
        descriptor = os.open(parts[-1],os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=directory)
        with os.fdopen(descriptor,"rb") as source:
            before = os.fstat(source.fileno())
            require(stat.S_ISREG(before.st_mode) and before.st_uid == os.geteuid() and before.st_nlink == 1
                    and stat.S_IMODE(before.st_mode) == 0o600 and 0 < before.st_size <= maximum,
                    "campaign_owner_custody")
            data = source.read(maximum+1);after=os.fstat(source.fileno())
        named = os.stat(parts[-1],dir_fd=directory,follow_symlinks=False)
        root_after = root.lstat()
        require(root_after.st_uid == os.geteuid() and stat.S_IMODE(root_after.st_mode) == 0o700,
                "campaign_owner_changed")
        if descendant_info is not None:
            descendant_after = (root/parts[0]).lstat()
            require(stat.S_ISDIR(descendant_after.st_mode) and descendant_after.st_uid == os.geteuid()
                    and stat.S_IMODE(descendant_after.st_mode) == 0o700
                    and (descendant_info.st_dev,descendant_info.st_ino)
                        == (descendant_after.st_dev,descendant_after.st_ino), "campaign_owner_changed")
        identity = lambda value:(value.st_dev,value.st_ino,value.st_size,value.st_mtime_ns,value.st_ctime_ns)
        require(len(data) == before.st_size and identity(before) == identity(after) == identity(named)
                and (root_info.st_dev,root_info.st_ino) == (root_after.st_dev,root_after.st_ino),
                "campaign_owner_changed")
        return data,dict(sha256=hashlib.sha256(data).hexdigest(),bytes=len(data))
    except OwnerReconciliationError:raise
    except OSError as error:raise OwnerReconciliationError("campaign_owner_input") from error
    finally:os.close(directory)


def signed_json_integer(encoded):
    require(len(encoded) <= 21, "campaign_owner_number")
    value=int(encoded)
    require(-64 <= value <= 2**64-1, "campaign_owner_number")
    return value


def decode(data):
    try:return json.loads(data,object_pairs_hook=unique_object,parse_int=signed_json_integer,
                          parse_float=reject_number,parse_constant=reject_number)
    except (ValueError,UnicodeError,RecursionError) as error:
        raise OwnerReconciliationError("campaign_owner_json") from error


def checked_usage(value, role, limits, *, expected_exit, maximum_lifetime_ns):
    require(type(value) is dict and value.keys() == USAGE_FIELDS
            and value["schema"] == "riffdb.ticketdesk-lifecycle-persistent-usage/v1"
            and value["launch_observed"] is True and value["exit_observed"] is True
            and type(value["exit_code"]) is int and value["exit_code"] == expected_exit
            and value["exit_rss_basis"] == "linux_wait4_per_child_ru_maxrss"
            and value["allocator_basis"] == BASIS and value["qualifies_complete_resource_model"] is False
            and value["exit_allocator_peak_bytes"] is None, "campaign_owner_usage")
    proof = value["address_space_proof"]
    require(type(proof) is dict and proof.keys() == PROOF_FIELDS and proof["enforced"] is True
            and type(proof["limit_bytes"]) is int and proof["limit_bytes"] == limits[role]
            and type(proof["inherited_hard_limit_bytes"]) is int
            and proof["inherited_hard_limit_bytes"] == limits["controller"]
            and type(proof["lifetime_upper_bound_bytes"]) is int
            and proof["lifetime_upper_bound_bytes"] == limits["controller"], "campaign_owner_birth")
    require(0 < integer(proof["launch_virtual_peak_bytes"]) <= limits[role]
            and 0 < integer(value["exit_rss_peak_bytes"]) <= limits["controller"]
            and type(value["worker_exit_allocator_peak_upper_bound_bytes"]) is int
            and value["worker_exit_allocator_peak_upper_bound_bytes"] == limits[role]
            and type(value["exit_allocator_peak_upper_bound_bytes"]) is int
            and value["exit_allocator_peak_upper_bound_bytes"] == limits["controller"]
            and 0 < integer(value["elapsed_ns"]) <= maximum_lifetime_ns, "campaign_owner_usage")
    launch=value["launch_memory"]
    if launch is None:
        require(value["launch_memory_refusal"] in {"resource_process_unavailable","resource_process_identity","resource_measurement","resource_process_inventory"},
                "campaign_owner_launch")
    else:
        require(type(launch) is dict and launch.keys() == {"rss_bytes","allocator_bytes",
                "reported_rss_peak_bytes","reported_allocator_peak_bytes"}
                and value["launch_memory_refusal"] is None, "campaign_owner_launch")
        for item in launch.values():integer(item)
        require(0 < launch["rss_bytes"] <= launch["allocator_bytes"] <= limits[role]
                and launch["rss_bytes"] <= launch["reported_rss_peak_bytes"] <= launch["reported_allocator_peak_bytes"] <= limits[role],
                "campaign_owner_launch")
    return value["exit_rss_peak_bytes"]


def operators(root, duration_s):
    data,identity=read_private(root,"campaign-operator.jsonl",maximum=64*4096)
    lines=data.splitlines(keepends=True)
    require(2 <= len(lines) <= 64 and all(len(line)<=4096 and line.endswith(b"\n") for line in lines),
            "campaign_owner_operator")
    rows=[decode(line) for line in lines];previous_end=0
    for index,row in enumerate(rows):
        require(type(row) is dict and row.keys() == OPERATOR_FIELDS
                and row["schema"] == "riffdb.ticketdesk-lifecycle-campaign-operator/v1"
                and type(row["index"]) is int and row["index"] == index
                and type(row["command"]) is str and len(row["command"])<=64
                and row["qualifies_complete_campaign"] is False, "campaign_owner_operator")
        require(type(row["exit_code"]) is int and row["exit_code"] in (0,1,3)
                and row["outcome_known"] is (row["exit_code"]==0)
                and (row["exit_code"] != 1 or row["command"] == "backup.operation")
                and (row["exit_code"] != 3 or row["command"] in ("backup.create","backup.restore")),
                "campaign_owner_operator")
        start=integer(row["elapsed_ns"]);span=integer(row["duration_ns"])
        require(start >= previous_end and 0 < span <= (900 if index==len(rows)-1 else 60)*SECOND,
                "campaign_owner_operator_clock")
        previous_end=start+span
    require(rows[-1]["command"] == "worker.archive-reconcile"
            and rows[-2]["command"] == "worker.reconcile"
            and rows[-2]["elapsed_ns"] >= duration_s*SECOND
            and all(row["command"] != "worker.archive-reconcile" for row in rows[:-1]),
            "campaign_owner_terminal")
    return rows,identity


def reconcile(root, *, duration_s, limits, terminal_memory):
    require(type(duration_s) is int and duration_s%300 == 0 and 73 <= duration_s//300 <= 288,
            "campaign_owner_duration")
    expected_roles={"controller","server","native_sampler","load","observer","operator"}
    require(type(limits) is dict and limits.keys() == expected_roles
            and all(type(v) is int and 64*1024**2 <= v <= 32*1024**3 for v in limits.values())
            and all(v <= limits["controller"] for v in limits.values()), "campaign_owner_limits")
    rows,operator_input=operators(root,duration_s)
    counts=dict(controller=1,server=4,native_sampler=5,load=1,observer=1,operator=len(rows)-2)
    require(type(terminal_memory) is dict and type(terminal_memory.get("roles")) is dict
            and terminal_memory["roles"].keys() == expected_roles
            and all(type(terminal_memory["roles"][role]) is dict
                and type(terminal_memory["roles"][role].get("incarnations")) is int
                and terminal_memory["roles"][role]["incarnations"]==count
                and type(terminal_memory["roles"][role].get("rss_peak_upper_bound_bytes")) is int
                and 0 < terminal_memory["roles"][role]["rss_peak_upper_bound_bytes"] <= limits["controller"]
                for role,count in counts.items()),
            "campaign_owner_inventory")
    names=set()
    with os.scandir(root) as entries:
        for entry in entries:
            require(len(names)<8192, "campaign_owner_inventory")
            names.add(entry.name)
    selected=[]
    for role,count in (("server",4),("native_sampler",5)):
        prefix="server" if role=="server" else "sampler"
        require({name for name in names if re.fullmatch(prefix+r"-[0-9]{3}",name)}
                == {prefix+'-'+str(i).zfill(3) for i in range(count)}, "campaign_owner_inventory")
        for index in range(count):
            file="server" if role=="server" else "native-observer"
            selected.append((role,f"{prefix}-{index:03}/{file}.process-usage.json",-9 if role=="server" and index==2 else 0,None))
    selected.extend((role,f"original-{role}/{role}.process-usage.json",0,None) for role in ("load","observer"))
    require({name for name in names if re.fullmatch(r"operator-[0-9]{3}\.process-usage\.json",name)}
            == {f"operator-{i:03}.process-usage.json" for i in range(len(rows))}, "campaign_owner_inventory")
    selected.extend(("operator",f"operator-{i:03}.process-usage.json",row["exit_code"],row["duration_ns"]) for i,row in enumerate(rows))
    peaks=dict.fromkeys(expected_roles,0);peaks["controller"]=terminal_memory["roles"]["controller"]["rss_peak_upper_bound_bytes"]
    inputs={"operator_ledger":operator_input}
    for role,name,code,span in selected:
        data,identity=read_private(root,name,maximum=2048);value=decode(data)
        peak=checked_usage(value,role,limits,expected_exit=code,
                           maximum_lifetime_ns=span if span is not None else (duration_s+2400)*SECOND)
        peaks[role]=max(peaks[role],peak);inputs[name]=identity
    return dict(schema="riffdb.ticketdesk-lifecycle-owner-reconciliation/v1",children=len(selected),
        incarnations=len(selected)+1,roles={**counts,"operator":len(rows)},rss_role_peak_upper_bounds=peaks,
        inputs=inputs,launch_and_exit_inventory_reconciled=True,qualifies_complete_resource_model=False)
