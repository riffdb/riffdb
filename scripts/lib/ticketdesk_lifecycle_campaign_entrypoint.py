# req: END-003, END-007, END-008, END-009, END-010
"""Execute the fixed eight-hour or 24-hour controller and close its observations.

The result remains explicitly unqualified. Clean-source full CI, four-language
conformance, host/storage verification and frozen prerequisite bindings must be
observed independently before the milestone receipt may be published.
"""
import argparse
from copy import copy
import hashlib
import json
import os
from pathlib import Path
import resource
import stat
import subprocess
import sys
import tempfile
import time

from .ticketdesk_lifecycle_campaign_execution import execute
from .ticketdesk_lifecycle_campaign_reconciliation import reconcile
from .ticketdesk_lifecycle_campaign_runtime import CampaignRuntime, private_write
from .ticketdesk_lifecycle_campaign_resource_model import (
    ARTIFACT_NAMES,MAX_ARTIFACT_BYTES,MAX_ARTIFACT_SUM_BYTES,PROCESS_LIMITS,PUBLIC_LIMITS,artifacts,
)
from .ticketdesk_lifecycle_campaign_owner_reconciliation import read_private,decode
from .ticketdesk_lifecycle_campaign_restore_evidence import protocol_snapshot
from .ticketdesk_lifecycle_resources import ProcessMemory

DURATION=28800
CAMPAIGN_DURATION=86400
FAULTS=(('consumer_outage',600,300),('sink_outage',1800,300),('capability_rotation',3000,45),
    ('additive_deploy',3600,45),('graceful_restart',4200,45),('process_kill',4800,45),
    ('backup_restore',5400,45))


class CampaignEntrypointError(ValueError):
    """Closed refusal without private data or machine addresses."""


def require(condition,code):
    if not condition:raise CampaignEntrypointError(code)


def schedule():
    return [dict(kind=kind,start_s=start,duration_s=duration) for kind,start,duration in FAULTS]


def file_identity(info):
    return info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns,info.st_ctime_ns


def source_artifacts(source):
    source=Path(source)
    require(source.is_absolute() and source.resolve()==source,"campaign_entry_artifacts")
    frozen={}
    for name in sorted(ARTIFACT_NAMES):
        descriptor=os.open(source/name,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
        with os.fdopen(descriptor,"rb") as incoming:
            before=os.fstat(incoming.fileno())
            require(stat.S_ISREG(before.st_mode) and before.st_uid==os.geteuid() and before.st_nlink==1
                and 0<before.st_size<=MAX_ARTIFACT_BYTES,"campaign_entry_artifact_custody")
            digest=hashlib.sha256();size=0
            while block:=incoming.read(65536):
                size+=len(block);require(size<=MAX_ARTIFACT_BYTES,"campaign_entry_artifact_bound");digest.update(block)
            after=os.fstat(incoming.fileno())
        require(size==before.st_size and file_identity(before)==file_identity(after)
            ==file_identity((source/name).lstat()),"campaign_entry_artifact_changed")
        frozen[name]=dict(sha256=digest.hexdigest(),bytes=size)
    require(sum(value["bytes"] for value in frozen.values())<=MAX_ARTIFACT_SUM_BYTES,"campaign_entry_artifact_bound")
    return frozen


def copy_artifacts(root,source,expected):
    root,source=Path(root),Path(source)
    require(source.is_absolute() and source.resolve()==source and root.is_absolute()
            and root.resolve()==root,'campaign_entry_artifacts')
    destination=root/'artifacts';destination.mkdir(mode=0o700)
    total=0
    for name in sorted(ARTIFACT_NAMES):
        descriptor=os.open(source/name,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
        with os.fdopen(descriptor,'rb') as incoming:
            before=os.fstat(incoming.fileno())
            require(stat.S_ISREG(before.st_mode) and before.st_uid==os.geteuid()
                    and before.st_nlink==1 and 0<before.st_size<=MAX_ARTIFACT_BYTES,
                    'campaign_entry_artifact_custody')
            total+=max(before.st_size,before.st_blocks*512)
            require(total<=MAX_ARTIFACT_SUM_BYTES,'campaign_entry_artifact_bound')
            fd=os.open(destination/name,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o700)
            copied=0;digest=hashlib.sha256()
            with os.fdopen(fd,'wb') as output:
                while block:=incoming.read(65536):
                    copied+=len(block)
                    require(copied<=MAX_ARTIFACT_BYTES,'campaign_entry_artifact_bound')
                    output.write(block);digest.update(block)
                output.flush();os.fsync(output.fileno())
            after=os.fstat(incoming.fileno())
        require(copied==before.st_size and file_identity(before)==file_identity(after)
                ==file_identity((source/name).lstat()),'campaign_entry_artifact_changed')
        require(hashlib.sha256((destination/name).read_bytes()).hexdigest()==digest.hexdigest(),
                'campaign_entry_artifact_changed')
    frozen=artifacts(root,destination)
    require({name:{key:value[key] for key in ("sha256","bytes")} for name,value in frozen.items()}==expected,
        "campaign_entry_artifact_changed")
    return frozen


def prepare(root,source,repository):
    root,source,repository=Path(root),Path(source),Path(repository)
    require(root.is_absolute() and root.resolve()==root and not root.exists()
            and root.parent.is_dir() and not root.parent.is_symlink(),'campaign_entry_fresh_root')
    original=source_artifacts(source)
    # Preparation precedes every offered-window measurement. Retain its actual
    # output privately, then place both bounded logs inside the accounted root.
    with tempfile.NamedTemporaryFile(dir=root.parent,prefix='.lifecycle-control-',delete=False) as output, \
         tempfile.NamedTemporaryFile(dir=root.parent,prefix='.lifecycle-control-',delete=False) as errors:
        paths=(Path(output.name),Path(errors.name))
        command=[sys.executable,str(repository/'scripts/ticketdesk-lifecycle-smoke'),
            '--root',str(root),'--server',str(source/'riffdbd'),'--cli',str(source/'riffdb'),
            '--worker',str(source/'lifecycle'),'--prepare-campaign-control','--online-retention']
        child=subprocess.Popen(command,stdin=subprocess.DEVNULL,stdout=output,stderr=errors,
            start_new_session=True)
        started=time.monotonic()
        try:
            while child.poll() is None:
                require(time.monotonic()-started<600 and all(path.stat().st_size<=1024**2 for path in paths),
                        'campaign_entry_control_deadline')
                try:child.wait(timeout=.1)
                except subprocess.TimeoutExpired:pass
            require(child.returncode==0,'campaign_entry_control_result')
        finally:
            if child.poll() is None:
                import signal
                os.killpg(child.pid,signal.SIGTERM)
                try:child.wait(timeout=10)
                except subprocess.TimeoutExpired:os.killpg(child.pid,signal.SIGKILL);child.wait(timeout=5)
    for path,name in zip(paths,('campaign-control-preparation.stdout','campaign-control-preparation.stderr')):
        require(path.stat().st_size<=1024**2,'campaign_entry_control_bound')
        os.rename(path,root/name)
    raw,_=read_private(root,'campaign-control.json',maximum=4096)
    require(decode(raw)==dict(schema='riffdb.ticketdesk-lifecycle-control-preparation/v1',
        seeded_tickets=0,offered_updates=0,capability_lifetime_seconds=100800,
        archive_quota_encoded_bytes=8*1024**3,qualifies_campaign=False),'campaign_entry_control')
    frozen=copy_artifacts(root,source,original)
    raw,_=read_private(root,'descriptor.stdout',maximum=4096)
    return decode(raw),frozen


def post_validation(runtime,closed):
    require(not runtime.children and not runtime.roster.children,'campaign_entry_unclosed_owner')
    sample,children=ProcessMemory(os.getpid()).tree_sample()
    require(not children and sample['rss_peak_bytes']<=PROCESS_LIMITS['controller']
            and resource.getrlimit(resource.RLIMIT_AS)[1]==PROCESS_LIMITS['controller'],
            'campaign_entry_controller_bound')
    probe=copy(runtime.disk)
    probe.archive=os.open(runtime.root/'archive/archive.redb',os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    try:files,_,_=probe._snapshot(None)
    finally:os.close(probe.archive)
    # Include this final bounded record itself in control admission.
    files['control_and_evidence_bytes']+=4096
    require(all(files[key]<=runtime.disk.limits[key] for key in files),'campaign_entry_disk_bound')
    require(all(window['allocator_bytes']<=PUBLIC_LIMITS['allocator_bytes']
                for window in closed['resources']['windows']),'campaign_entry_allocator_bound')
    return dict(schema='riffdb.ticketdesk-lifecycle-post-validation-resources/v1',measured=False,
        elapsed_ns=runtime.elapsed(),controller=sample,files=files,qualifies_complete_campaign=False)


def run(root,source,repository,*,duration_s=DURATION):
    require(type(duration_s) is int and duration_s in (DURATION,CAMPAIGN_DURATION),
            'campaign_entry_duration')
    require(sys.platform=='linux' and os.geteuid()!=0,'campaign_entry_platform')
    os.umask(0o077)
    resource.setrlimit(resource.RLIMIT_AS,(PROCESS_LIMITS['controller'],PROCESS_LIMITS['controller']))
    descriptor,frozen=prepare(root,source,repository)
    runtime=CampaignRuntime(root,Path(root)/'artifacts',descriptor,duration_s=duration_s,faults=schedule())
    runtime.repository = Path(repository)
    protocol = json.dumps(protocol_snapshot(repository), sort_keys=True).encode()+b'\n'
    runtime.restore_protocol_sha256 = hashlib.sha256(protocol).hexdigest()
    private_write(root, 'campaign-restore-protocol.json', protocol)
    result=execute(runtime)
    closed=reconcile(runtime,result)
    require(artifacts(root,Path(root)/'artifacts')==frozen,'campaign_entry_artifact_changed')
    private_write(root,'campaign-closed-reconciliation.json',json.dumps(closed,sort_keys=True).encode()+b'\n')
    encoded=json.dumps(post_validation(runtime,closed),sort_keys=True).encode()+b'\n'
    require(len(encoded)<=4096,'campaign_entry_post_record_bound')
    private_write(root,'campaign-post-validation-resources.json',encoded)
    return closed


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',required=True,type=Path)
    parser.add_argument('--artifacts',required=True,type=Path)
    parser.add_argument('--mode',choices=('rehearsal','campaign'),default='rehearsal',
                        help='rehearsal runs eight hours; campaign runs 24 hours')
    args=parser.parse_args()
    repository=Path(__file__).resolve().parents[2]
    duration=DURATION if args.mode=='rehearsal' else CAMPAIGN_DURATION
    run(args.root,args.artifacts,repository,duration_s=duration)
    print(f'{duration//3600}-hour measured controller and closed observations complete; prerequisite qualification remains separate.')
    return 0
