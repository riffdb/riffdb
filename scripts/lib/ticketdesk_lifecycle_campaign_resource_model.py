# req: END-007, END-008, END-010
"""Pre-offer resource admission for the fixed TicketDesk lifecycle workload.

Disk ceilings include intentionally retained evidence and all private harness
files. Reclaimable-history trends remain three independent native bases. These
bounds are not a measured result or permission to change a running model.
"""
import hashlib
import os
from pathlib import Path
import shutil
import stat

GIB=1024**3
DISK_LIMITS=dict(database_bytes=64*GIB,journal_bytes=16*GIB,sink_archive_bytes=8*GIB,
    backup_bytes=32*GIB,derived_bytes=4*GIB,control_and_evidence_bytes=2*GIB)
PROCESS_LIMITS=dict(controller=8*GIB,server=8*GIB,load=GIB,observer=GIB,native_sampler=GIB,operator=GIB)
PUBLIC_LIMITS={**{key:value for key,value in DISK_LIMITS.items() if key!="control_and_evidence_bytes"},
    "rss_bytes":32*GIB,"allocator_bytes":32*GIB}
RESERVE_BYTES=8*GIB
ARTIFACT_NAMES=frozenset({"riffdbd","riffdb","lifecycle","riffdb-driverd"})
MAX_ARTIFACT_BYTES=128*1024**2
MAX_ARTIFACT_SUM_BYTES=256*1024**2


class ResourceModelError(ValueError):
    """Closed refusal without artifact contents or machine details."""


def require(condition,code):
    if not condition:raise ResourceModelError(code)


def identity(info):
    return info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns,info.st_ctime_ns


def artifacts(root,directory):
    root,directory=Path(root),Path(directory)
    require(root.is_absolute() and root.resolve()==root and directory==root/"artifacts",
            "campaign_model_artifact_scope")
    try:
        for path in (root,directory):
            info=path.lstat()
            require(stat.S_ISDIR(info.st_mode) and info.st_uid==os.geteuid()
                    and stat.S_IMODE(info.st_mode)==0o700,"campaign_model_artifact_custody")
        with os.scandir(directory) as entries:
            names=set()
            for entry in entries:
                require(len(names)<4,"campaign_model_artifact_inventory")
                names.add(entry.name)
        require(names==ARTIFACT_NAMES,"campaign_model_artifact_inventory")
        values={};total=0
        for name in sorted(names):
            descriptor=os.open(directory/name,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
            with os.fdopen(descriptor,"rb") as source:
                before=os.fstat(source.fileno())
                require(stat.S_ISREG(before.st_mode) and before.st_uid==os.geteuid() and before.st_nlink==1
                        and stat.S_IMODE(before.st_mode)==0o700 and 0<before.st_size<=MAX_ARTIFACT_BYTES,
                        "campaign_model_artifact_custody")
                digest=hashlib.sha256();size=0
                while block:=source.read(65536):
                    size+=len(block)
                    require(size<=MAX_ARTIFACT_BYTES,"campaign_model_artifact_bound")
                    digest.update(block)
                after=os.fstat(source.fileno())
            named=(directory/name).lstat()
            require(size==before.st_size and identity(before)==identity(after)==identity(named),
                    "campaign_model_artifact_changed")
            bound=max(before.st_size,before.st_blocks*512)
            total+=bound
            require(total<=MAX_ARTIFACT_SUM_BYTES,"campaign_model_artifact_bound")
            values[name]=dict(sha256=digest.hexdigest(),bytes=size,file_bound_bytes=bound)
        return values
    except ResourceModelError:raise
    except OSError as error:raise ResourceModelError("campaign_model_artifact_custody") from error


def admit(root,directory,*,duration_s):
    require(type(duration_s) is int and duration_s%300==0 and 73<=duration_s//300<=288,
            "campaign_model_duration")
    frozen=artifacts(root,directory)
    try:free=shutil.disk_usage(root).free
    except OSError as error:raise ResourceModelError("campaign_model_volume") from error
    required=sum(DISK_LIMITS.values())+RESERVE_BYTES
    require(type(free) is int and free>=required,"campaign_model_free_space")
    allocator_bound=sum(PROCESS_LIMITS.values())+max(PROCESS_LIMITS["controller"]-limit
        for role,limit in PROCESS_LIMITS.items() if role!="controller")
    require(allocator_bound<=PUBLIC_LIMITS["allocator_bytes"],"campaign_model_memory")
    return dict(schema="riffdb.ticketdesk-lifecycle-resource-model/v1",duration_s=duration_s,
        resource_limits=dict(PUBLIC_LIMITS),control_and_evidence_limit_bytes=DISK_LIMITS["control_and_evidence_bytes"],
        process_limits=dict(PROCESS_LIMITS),allocator_upper_bound_bytes=allocator_bound,
        disk_reserve_bytes=RESERVE_BYTES,minimum_starting_free_bytes=required,observed_starting_free_bytes=free,
        artifacts=frozen,population=dict(organizations=64,tickets=65536,commands=duration_s*100,reads=duration_s*400),
        accounting=dict(database="whole_primary_files_including_retained_outcomes_provenance_and_free_pages",
            journal="whole_active_sealed_and_spare_files_including_protected_history",
            archive="same_durable_local_archive_including_retained_deliveries",
            backup="whole_immutable_backups_stages_and_retained_pre_restore_copies",
            derived="whole_rebuildable_projection_files",
            control="whole_private_harness_files_including_frozen_binary_copies",
            history="three_native_logical_source_and_physical_bases_remain_separate"),
        qualifies_complete_resource_model=False)
