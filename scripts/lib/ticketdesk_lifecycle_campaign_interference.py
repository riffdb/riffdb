# req: END-007, END-008, END-010
"""Continuously census known competing workloads on the reserved campaign host.

This samples honest process identities; it is not an adversarial execution
monitor. The frozen policy and exclusive host reservation must accompany the
result. Missing census inputs, changed host identity or sample gaps refuse.
"""
import hashlib
import json
import os
from pathlib import Path
import re

SECOND=10**9
HEAVY=frozenset({"cargo","rustc","rustfmt","cargo-nextest","nextest","go","node","ruby",
    "fio","stress","stress-ng","sysbench","riffdb","riffdbd","riffdb-driverd","lifecycle"})
POLICY="reserved_host_known_database_compiler_and_benchmark_process_census_v1"


class InterferenceError(ValueError):
    """Closed diagnostics never expose host or competing process arguments."""


def require(condition,code):
    if not condition:raise InterferenceError(code)


def competing(name):
    return name in HEAVY or "bench" in name.lower() or "checkpoint" in name.lower()


def census(proc_root,owned):
    require(type(owned) is set and 0 < len(owned) <= 32
            and all(type(pid) is int and pid>0 for pid in owned),"campaign_interference_owners")
    observed={};races=scanned=0
    try:
        with os.scandir(proc_root) as entries:
            for entry in entries:
                scanned+=1
                require(scanned<=32768,"campaign_interference_bound")
                if not re.fullmatch(r"[1-9][0-9]{0,9}",entry.name):continue
                require(len(observed)<32768,"campaign_interference_bound")
                pid=int(entry.name);path=Path(proc_root)/entry.name
                try:
                    with (path/"stat").open("rb") as source:data=source.read(4097)
                    require(0<len(data)<=4096,"campaign_interference_input")
                    left,sep,right=data.rpartition(b") ")
                    require(sep and left.startswith(entry.name.encode()+b" ("),"campaign_interference_input")
                    name=left.split(b" (",1)[1].decode("utf-8")
                    fields=right.split()
                    require(len(fields)>=20 and len(name)<=64,"campaign_interference_input")
                    if fields[0]==b"Z":continue
                    birth=int(fields[19]);parent=int(fields[1])
                    require(birth>0 and parent>=0,"campaign_interference_input")
                    observed[pid]=(parent,birth,name)
                except FileNotFoundError:
                    if path.exists():raise
                    races+=1
        require(owned<=observed.keys(),"campaign_interference_owners")
        # The roster's separate descendant census accounts every campaign child.
        # A known heavy process outside those exact owners is competing work.
        bad=[dict(pid=pid,start_ticks=birth,program_sha256=hashlib.sha256(name.encode()).hexdigest())
             for pid,(_,birth,name) in observed.items() if pid not in owned and competing(name)]
        require(len(bad)<=32,"campaign_interference_bound")
        return dict(processes_seen=len(observed),owned_processes=len(owned),
            competing_workloads=bad,disappeared_during_census=races)
    except (OSError,ValueError,UnicodeError,IndexError) as error:
        if isinstance(error,InterferenceError):raise
        raise InterferenceError("campaign_interference_input") from error


class CampaignInterferenceSamples:
    def __init__(self,root,roster,*,duration_s,proc_root=Path("/proc")):
        require(type(duration_s) is int and duration_s%300==0 and 73<=duration_s//300<=288,
                "campaign_interference_duration")
        self.root,self.roster,self.duration,self.proc=Path(root),roster,duration_s*SECOND,Path(proc_root)
        self.output=self.last_ns=self.boot=None
        self.count=0
        self.failed=False

    def __enter__(self):
        descriptor=os.open(self.root/"campaign-interference.jsonl",
            os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
        self.output=os.fdopen(descriptor,"wb")
        return self

    def __exit__(self,*_):
        if self.output is not None:self.output.close()
        return False

    def poll(self,elapsed_ns):
        require(not self.failed and self.output is not None and not self.output.closed,
                "campaign_interference_state")
        try:
            require(type(elapsed_ns) is int and 0<=elapsed_ns<2**64
                    and (self.last_ns is None or elapsed_ns>=self.last_ns),"campaign_interference_clock")
            require(elapsed_ns-(self.last_ns or 0)<=75*SECOND,"campaign_interference_gap")
            if elapsed_ns>=self.duration:return
            if self.last_ns is not None and elapsed_ns-self.last_ns<5*SECOND:return
            require(self.count<17281,"campaign_interference_bound")
            self.roster._check()
            self.roster.sample(elapsed_ns)
            owners={owner.pid for owner in self.roster.owners.values()}
            with (self.proc/"sys/kernel/random/boot_id").open("rb") as source:boot=source.read(38)
            require(re.fullmatch(rb"[0-9a-f-]{36}\n",boot) is not None,"campaign_interference_host")
            identity=hashlib.sha256(boot).hexdigest()
            require(self.boot is None or self.boot==identity,"campaign_interference_host")
            observation=census(self.proc,owners)
            record=dict(schema="riffdb.ticketdesk-lifecycle-interference/v1",elapsed_ns=elapsed_ns,
                policy=POLICY,boot_identity_sha256=identity,**observation,qualifies_complete_campaign=False)
            encoded=json.dumps(record,sort_keys=True).encode()+b"\n"
            require(len(encoded)<=8192,"campaign_interference_bound")
            self.output.write(encoded);self.output.flush()
            self.boot,self.last_ns=identity,elapsed_ns
            self.count+=1
            require(not observation["competing_workloads"],"campaign_interference_detected")
        except BaseException:
            self.failed=True
            raise


def reconcile(rows,*,duration_s):
    require(type(duration_s) is int and duration_s%300==0 and 73<=duration_s//300<=288,
            "campaign_interference_duration")
    fields={"schema","elapsed_ns","policy","boot_identity_sha256","processes_seen","owned_processes",
        "competing_workloads","disappeared_during_census","qualifies_complete_campaign"}
    require(type(rows) is list and 0<len(rows)<=17281,"campaign_interference_bound")
    windows=[0]*(duration_s//300);previous=boot=None
    for row in rows:
        require(type(row) is dict and row.keys()==fields
                and row["schema"]=="riffdb.ticketdesk-lifecycle-interference/v1"
                and row["policy"]==POLICY and row["qualifies_complete_campaign"] is False,
                "campaign_interference_shape")
        for key in ("elapsed_ns","processes_seen","owned_processes","disappeared_during_census"):
            require(type(row[key]) is int and 0<=row[key]<2**64,"campaign_interference_number")
        elapsed=row["elapsed_ns"]
        require(elapsed<duration_s*SECOND and elapsed-(previous or 0)<=75*SECOND
                and (previous is None or elapsed>previous),"campaign_interference_gap")
        require(0<row["owned_processes"]<=32 and row["owned_processes"]<=row["processes_seen"]<=32768
                and row["disappeared_during_census"]<=32768,"campaign_interference_inventory")
        require(type(row["boot_identity_sha256"]) is str
                and re.fullmatch(r"[0-9a-f]{64}",row["boot_identity_sha256"]) is not None
                and (boot is None or boot==row["boot_identity_sha256"]),"campaign_interference_host")
        require(type(row["competing_workloads"]) is list and not row["competing_workloads"],
                "campaign_interference_detected")
        windows[elapsed//(300*SECOND)]+=1
        previous,boot=elapsed,row["boot_identity_sha256"]
    require(duration_s*SECOND-previous<=75*SECOND and all(windows),"campaign_interference_gap")
    return dict(schema="riffdb.ticketdesk-lifecycle-interference-reconciliation/v1",
        policy=POLICY,duration_s=duration_s,samples=len(rows),window_samples=windows,
        sampled_competing_workloads=0,boot_identity_sha256=boot,qualifies_complete_campaign=False)
