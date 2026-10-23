# req: END-007, END-008, END-010
"""Verify original frozen prerequisites; never grant elapsed campaign credit.

The existing manifest and private prerequisite schemas remain unchanged. Input
locations are internal caller arguments, not a persisted or application-facing
interface. Producer success booleans cannot replace observed exits, original
hashes, typed reports and independently checked source/cloud relationships.
"""
from dataclasses import dataclass
from decimal import Decimal
from functools import wraps
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import stat
import subprocess
import time

from .ticketdesk_lifecycle_campaign_timeline import contract
from .ticketdesk_lifecycle_campaign_entrypoint import schedule
from .ticketdesk_lifecycle_campaign_restore_evidence import protocol_snapshot, validate_protocol
from .ticketdesk_lifecycle_campaign_resource_model import (
    ARTIFACT_NAMES, DISK_LIMITS, PROCESS_LIMITS, PUBLIC_LIMITS, RESERVE_BYTES,
)

MAX_JSON = 4*1024**2
MAX_FILE = 128*1024**2
LANGUAGES = frozenset({'rust', 'go', 'typescript', 'python'})
CI_STEPS = frozenset({'current-package-brief', 'whole-authored-branch-approved-scope',
    'whole-authored-branch-full-acceptance-plan', 'package-full-acceptance-plan',
    'accepted-decision-index-check', 'accepted-tls-native-full-recovery-matrix',
    'complete-workspace-ci-and-package-acceptance',
    'four-language-remote-conformance', 'retained-typescript-v1-v2-smoke'})
CONFORMANCE_STEPS = frozenset({'four-language-remote-conformance', 'retained-typescript-v1-v2-smoke'})
TS_REPORT = dict(schema='riffdb.ticketdesk-lifecycle-typescript-smoke/v1', named_read=True,
    partition_isolation=True, caller_fence=True, idempotency=True, state_unchanged=True,
    live_query_snapshot=True)
CONFORMANCE_REPORT = dict(schema='riffdb.driver-conformance-observation/v1',
    consume='TokenConsumed', consume_missing=True, consume_replayed=True, created='Created',
    delete_preimage_redacted=True, exact_query='Found', exact_total=1, query='Found',
    read_after_commit=True, replayed=True, reuse_error='RDB-COMMAND-0101',
    secret_query='Found', secret_redacted=True)
REVOKED_REPORT = dict(schema='riffdb.driver-conformance-fault/v1', fault='revocation', code='RDB-AUTH-0215')


class PrerequisiteError(ValueError):
    """Closed refusal without input paths, raw reports or cloud identities."""


def require(condition, code):
    if not condition: raise PrerequisiteError(code)


def closed_inputs(function):
    @wraps(function)
    def checked(*args, **kwargs):
        try: return function(*args, **kwargs)
        except PrerequisiteError: raise
        except (OSError, KeyError, TypeError, AttributeError, ValueError, ArithmeticError, RecursionError) as error:
            raise PrerequisiteError('qualification_prerequisite_input') from error
    return checked


def digest(value):
    require(type(value) is str and re.fullmatch(r'[0-9a-f]{64}', value) is not None,
        'qualification_prerequisite_digest')
    return value


def number(value, *, maximum=2**64-1):
    require(type(value) is int and 0 <= value <= maximum, 'qualification_prerequisite_number')
    return value


def exact(actual, expected, code):
    """Python equality alone would let true stand in for an observed integer."""
    require(type(actual) is type(expected), code)
    if type(expected) is dict:
        require(actual.keys() == expected.keys(), code)
        for key in expected: exact(actual[key], expected[key], code)
    elif type(expected) is list:
        require(len(actual) == len(expected), code)
        for a, b in zip(actual, expected): exact(a, b, code)
    else: require(actual == expected, code)


def decode(raw, *, elapsed_seconds=False):
    require(type(raw) is bytes and 0 < len(raw) <= MAX_JSON, 'qualification_prerequisite_json_bound')
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, 'qualification_prerequisite_duplicate')
            result[key] = value
        return result
    def integer(text):
        require(len(text) <= 21, 'qualification_prerequisite_number')
        value = int(text)
        require(-64 <= value < 2**64, 'qualification_prerequisite_number')
        return value
    def decimal(text):
        require(len(text) <= 64, 'qualification_prerequisite_number')
        return Decimal(text)
    try:
        value = json.loads(raw, object_pairs_hook=unique, parse_int=integer,
            parse_float=decimal, parse_constant=decimal)
        pending = [(value, ())]; count = 0
        while pending:
            item, path = pending.pop(); count += 1
            require(count <= 100000 and len(path) <= 32, 'qualification_prerequisite_json_bound')
            if isinstance(item, Decimal):
                location = (len(path) >= 3 and path[-1] == 'seconds' and path[-3] == 'steps'
                    and type(path[-2]) is int) or path in {('attempt_seconds',), ('whole_observation', 'attempt_seconds')}
                require(elapsed_seconds and location
                    and item.is_finite() and 0 < item <= 43200, 'qualification_prerequisite_number')
            elif type(item) is dict:
                require(len(item) <= 8192 and all(type(key) is str and 0 < len(key) <= 1024
                    for key in item), 'qualification_prerequisite_json_bound')
                pending.extend((v, (*path, k)) for k, v in item.items())
            elif type(item) is list:
                require(len(item) <= 8192, 'qualification_prerequisite_json_bound')
                pending.extend((v, (*path, k)) for k, v in enumerate(item))
            elif type(item) is str:
                require(len(item.encode()) <= MAX_JSON, 'qualification_prerequisite_json_bound')
        return value
    except PrerequisiteError: raise
    except (ValueError, UnicodeError, RecursionError) as error:
        raise PrerequisiteError('qualification_prerequisite_json') from error


@dataclass(frozen=True)
class OriginalFile:
    root: Path
    name: str


def read_original(original, *, maximum=MAX_JSON, expected_sha256=None, allow_empty=False):
    require(type(original) is OriginalFile and type(maximum) is int and 0 < maximum <= MAX_FILE
        and type(allow_empty) is bool,
        'qualification_prerequisite_input')
    root = Path(original.root); name = original.name
    require(root.is_absolute() and root.resolve() == root and type(name) is str,
        'qualification_prerequisite_input')
    parts = name.split('/')
    require(1 <= len(parts) <= 6 and all(re.fullmatch(r'[A-Za-z0-9_.-]{1,128}', part)
        and part not in {'.', '..'} for part in parts), 'qualification_prerequisite_input')
    if expected_sha256 is not None: digest(expected_sha256)
    identity = lambda info: (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
    directory = None
    try:
        directory = os.open(root, os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
        directories = [(root, os.fstat(directory))]
        for part in parts[:-1]:
            info = os.fstat(directory)
            require(info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) == 0o700,
                'qualification_prerequisite_custody')
            child = os.open(part, os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW, dir_fd=directory)
            os.close(directory); directory = child
            directories.append((directories[-1][0]/part, os.fstat(directory)))
        info = os.fstat(directory)
        require(info.st_uid == os.geteuid() and stat.S_IMODE(info.st_mode) == 0o700,
            'qualification_prerequisite_custody')
        descriptor = os.open(parts[-1], os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK, dir_fd=directory)
        with os.fdopen(descriptor, 'rb') as source:
            before = os.fstat(source.fileno())
            require(stat.S_ISREG(before.st_mode) and before.st_uid == os.geteuid() and before.st_nlink == 1
                and stat.S_IMODE(before.st_mode) in {0o400, 0o500, 0o600, 0o700}
                and int(not allow_empty) <= before.st_size <= maximum, 'qualification_prerequisite_custody')
            data = source.read(maximum+1); after = os.fstat(source.fileno())
        named = os.stat(parts[-1], dir_fd=directory, follow_symlinks=False)
        require(len(data) == before.st_size and identity(before) == identity(after) == identity(named),
            'qualification_prerequisite_changed')
        for path, opened in directories:
            named = path.lstat()
            require(stat.S_ISDIR(named.st_mode) and named.st_uid == os.geteuid()
                and stat.S_IMODE(named.st_mode) == 0o700
                and (named.st_dev, named.st_ino) == (opened.st_dev, opened.st_ino),
                'qualification_prerequisite_changed')
        proof = dict(sha256=hashlib.sha256(data).hexdigest(), bytes=len(data))
        require(expected_sha256 is None or proof['sha256'] == expected_sha256,
            'qualification_prerequisite_identity')
        return data, proof
    except PrerequisiteError: raise
    except OSError as error: raise PrerequisiteError('qualification_prerequisite_input') from error
    finally:
        if directory is not None: os.close(directory)


def source_snapshot(repository):
    """Read Git only; never change source, build, run checks or refresh a clock."""
    repository = Path(repository)
    require(repository.is_absolute() and repository.resolve() == repository,
        'qualification_prerequisite_source')
    def git(arguments, maximum):
        child = subprocess.Popen(['git', '-C', str(repository), *arguments],
            env=dict(os.environ, GIT_OPTIONAL_LOCKS='0'), stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        output = bytearray(); deadline = time.monotonic()+30
        selector = selectors.DefaultSelector(); selector.register(child.stdout, selectors.EVENT_READ)
        try:
            while True:
                remaining = deadline-time.monotonic()
                require(remaining > 0 and selector.select(remaining), 'qualification_prerequisite_source')
                chunk = os.read(child.stdout.fileno(), min(65536, maximum+1-len(output)))
                if not chunk: break
                output.extend(chunk)
                require(len(output) <= maximum, 'qualification_prerequisite_source')
            require(child.wait(timeout=max(.001, deadline-time.monotonic())) == 0,
                'qualification_prerequisite_source')
            return bytes(output)
        except subprocess.TimeoutExpired as error:
            raise PrerequisiteError('qualification_prerequisite_source') from error
        finally:
            selector.close()
            if child.poll() is None: child.kill(); child.wait()
            child.stdout.close()
    try:
        return dict(revision=git(['rev-parse', 'HEAD'], 41).decode().strip(),
            tree=git(['rev-parse', 'HEAD^{tree}'], 41).decode().strip(),
            status=git(['status', '--porcelain', '--untracked-files=all'], MAX_JSON),
            files=git(['ls-files', '-z'], MAX_JSON).decode().split('\0')[:-1])
    except (OSError, UnicodeError) as error:
        raise PrerequisiteError('qualification_prerequisite_source') from error


@closed_inputs
def verify_source(repository, inventory, workload, revision):
    snapshot = source_snapshot(repository)
    require(snapshot['revision'] == revision and snapshot['tree'] == workload['source_tree']
        and snapshot['status'] == b'' and type(inventory) is dict and 0 < len(inventory) <= 8192
        and len(snapshot['files']) == len(set(snapshot['files']))
        and set(snapshot['files']) == inventory.keys(), 'qualification_prerequisite_source')
    require(type(workload['sources']) is dict and 0 < len(workload['sources']) <= len(inventory)
        and workload['sources'].keys() <= inventory.keys(), 'qualification_prerequisite_source')
    total = 0
    for name, checksum in inventory.items():
        require(type(name) is str and 0 < len(name) <= 1024 and not name.startswith('/')
            and all(part not in {'', '.', '..'} for part in name.split('/')),
            'qualification_prerequisite_source')
        digest(checksum)
        path = Path(repository)/name
        require(path.resolve() == path and path.is_file(), 'qualification_prerequisite_source')
        with path.open('rb') as source:
            before = os.fstat(source.fileno()); total += before.st_size
            require(stat.S_ISREG(before.st_mode) and before.st_size <= MAX_FILE and total <= 1024**3,
                'qualification_prerequisite_source')
            actual = hashlib.file_digest(source, 'sha256').hexdigest()
            after = os.fstat(source.fileno())
        identity = lambda info: (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
        require(identity(before) == identity(after) == identity(path.lstat()), 'qualification_prerequisite_source')
        require(actual == checksum and (name not in workload['sources'] or workload['sources'][name] == checksum),
            'qualification_prerequisite_source')
    exact(source_snapshot(repository), snapshot, 'qualification_prerequisite_source')


@closed_inputs
def verify_steps(receipt, log_files, proofs, prefix):
    steps = receipt.get('steps')
    require(type(steps) is list and 0 < len(steps) <= 64 and type(log_files) is dict
        and len(log_files) == len(steps), 'qualification_prerequisite_ci')
    names = set(); logs = {}
    for row in steps:
        require(type(row) is dict and row.keys() == {'step', 'exit', 'seconds', 'log_sha256'}
            and type(row['step']) is str and re.fullmatch(r'[a-z0-9-]{1,128}', row['step'])
            and row['step'] not in names and type(row['exit']) is int and row['exit'] == 0
            and type(row['seconds']) in {int, Decimal} and 0 < row['seconds'] <= 43200,
            'qualification_prerequisite_ci')
        names.add(row['step'])
        require(row['step'] in log_files, 'qualification_prerequisite_ci')
        logs[row['step']], proofs[prefix+':'+row['step']] = read_original(log_files[row['step']],
            maximum=MAX_FILE, expected_sha256=row['log_sha256'], allow_empty=True)
    require(names == log_files.keys(), 'qualification_prerequisite_ci')
    return logs


@closed_inputs
def verify_ci(ci, ci_artifact, conformance, revision, log_files, proofs, repository):
    require(type(ci) is dict and ci.get('status') == 'passed' and ci.get('revision') == revision,
        'qualification_prerequisite_ci')
    exact(ci_artifact, dict(schema='riffdb.wp825-observed-complete-source-ci/v1',
        source_revision=revision, full_ci_receipt_sha256=proofs['ci_receipt']['sha256'],
        full_ci_receipt=ci), 'qualification_prerequisite_ci')
    logs = verify_steps(ci, log_files, proofs, 'ci_log')
    require(CI_STEPS <= logs.keys(), 'qualification_prerequisite_ci')
    steps = ci['steps']
    exact(conformance['steps'], [row for row in steps if row['step'] in CONFORMANCE_STEPS],
        'qualification_prerequisite_ci')
    require(conformance['schema'] == 'riffdb.wp825-observed-conformance/v1'
        and conformance['source_revision'] == revision
        and conformance['ci_receipt_sha256'] == proofs['ci_receipt']['sha256'],
        'qualification_prerequisite_ci')
    def commands(raw):
        return re.findall(r'^\s+\d+\s+(.+)$', raw.decode('utf-8'), re.MULTILINE)
    branch = commands(logs['whole-authored-branch-full-acceptance-plan'])
    package = commands(logs['package-full-acceptance-plan'])
    require(0 < len(branch) <= 64 and 0 < len(package) <= 64 and package[-1] == './scripts/ci-all',
        'qualification_prerequisite_ci_coverage')
    exact(ci.get('branch_acceptance_commands'), branch, 'qualification_prerequisite_ci_coverage')
    exact(ci.get('package_acceptance_commands'), package, 'qualification_prerequisite_ci_coverage')
    direct = []
    for name in ('scripts/ci-all', 'scripts/check-generated'):
        direct.extend((Path(repository)/name).read_text().splitlines())
    extra = [command for command in branch if command not in package and command not in direct]
    exact(ci.get('additional_branch_checks_executed'), extra, 'qualification_prerequisite_ci_coverage')
    require({name for name in logs if name.startswith('additional-branch-path-check-')}
        == {'additional-branch-path-check-'+str(index) for index in range(len(extra))},
        'qualification_prerequisite_ci_coverage')
    require(b'full_recovery_matrix: 10 passed' in logs['accepted-tls-native-full-recovery-matrix']
        and re.search(rb'\b[1-9][0-9]* passed, 0 failed, 0 skipped\.',
            logs['complete-workspace-ci-and-package-acceptance']) is not None,
        'qualification_prerequisite_ci_coverage')


@closed_inputs
def verify_reports(ci, conformance, inputs, artifacts, proofs):
    versions = ci.get('typescript_versions'); reports = ci.get('conformance_observations_sha256')
    require(type(versions) is dict and versions.keys() == {'1', '2'}
        and type(reports) is dict and reports.keys() == LANGUAGES,
        'qualification_prerequisite_reports')
    exact(conformance['retained_typescript_versions'], versions, 'qualification_prerequisite_reports')
    require(type(inputs['typescript']) is dict and inputs['typescript'].keys() == {'1', '2'}
        and type(inputs['conformance']) is dict and inputs['conformance'].keys() == LANGUAGES,
        'qualification_prerequisite_reports')
    for version, hashes in versions.items():
        files = inputs['typescript'][version]
        require(type(hashes) is dict and hashes.keys() == {'observation_sha256', 'client_sha256', 'executed_smoke_sha256'}
            and type(files) is dict and files.keys() == {'observation', 'client', 'executed_smoke'},
            'qualification_prerequisite_reports')
        for kind in files:
            raw, proofs['typescript:'+version+':'+kind] = read_original(files[kind],
                expected_sha256=hashes[kind+'_sha256'])
            if kind == 'observation': exact(decode(raw), TS_REPORT, 'qualification_prerequisite_reports')
        if version == '1':
            require(hashes['client_sha256'] == artifacts['typescript_client']['sha256'],
                'qualification_prerequisite_reports')
    for language, hashes in reports.items():
        files = inputs['conformance'][language]
        require(type(hashes) is dict and hashes.keys() == {'observation', 'revoked'}
            and type(files) is dict and files.keys() == hashes.keys(), 'qualification_prerequisite_reports')
        for kind, checksum in hashes.items():
            raw, proofs['conformance:'+language+':'+kind] = read_original(files[kind], expected_sha256=checksum)
            expected = dict(CONFORMANCE_REPORT, language=language) if kind == 'observation' else REVOKED_REPORT
            exact(decode(raw), expected, 'qualification_prerequisite_reports')


@closed_inputs
def verify_host(environment, files, proofs):
    require(environment.get('schema') == 'riffdb.wp825-prerequisite-environment/v1'
        and type(files) is dict and files.keys() == {'disk', 'instance', 'machine', 'guest'},
        'qualification_prerequisite_host')
    hashes = environment.get('cloud_evidence_sha256')
    require(type(hashes) is dict and hashes.keys() == files.keys(), 'qualification_prerequisite_host')
    values = {}
    for kind, file in files.items():
        raw, proofs['cloud:'+kind] = read_original(file, expected_sha256=hashes[kind])
        values[kind] = decode(raw)
    disk, instance, machine, guest = (values[k] for k in ('disk', 'instance', 'machine', 'guest'))
    require(all(type(v) is dict for v in values.values()), 'qualification_prerequisite_host')
    attached = [row for row in instance.get('disks', []) if type(row) is dict and row.get('source') == disk.get('selfLink')]
    require(type(disk.get('selfLink')) is str and len(disk['selfLink']) <= 2048
        and len(attached) == 1 and attached[0].get('mode') == 'READ_WRITE' and attached[0].get('type') == 'PERSISTENT'
        and disk.get('status') == 'READY' and instance.get('status') == 'RUNNING'
        and type(disk.get('type')) is str and disk['type'].rsplit('/', 1)[-1] == 'pd-balanced'
        and type(disk.get('sizeGb')) is str and disk['sizeGb'].isdecimal() and int(disk['sizeGb']) >= 500
        and type(instance.get('machineType')) is str
        and instance['machineType'].rsplit('/', 1)[-1] == 'c3d-standard-8'
        and type(machine.get('guestCpus')) is int and machine['guestCpus'] == 8
        and type(machine.get('memoryMb')) is int and machine['memoryMb'] == 32768,
        'qualification_prerequisite_host')
    disk_size_bytes = int(disk['sizeGb'])*1024**3
    # This legacy guest field comes from the enclosing block device's sysfs
    # size, not from filesystem capacity. A >=500-GiB block device is required.
    require(type(guest.get('vcpus')) is int and guest['vcpus'] == 8
        and type(guest.get('memory')) is dict and type(guest['memory'].get('MemTotal')) is int
        and 30*1024**3 <= guest['memory']['MemTotal'] <= 32*1024**3
        and type(guest.get('campaign_filesystem_disk_bytes')) is int
        and guest['campaign_filesystem_disk_bytes'] == disk_size_bytes
        and guest.get('campaign_filesystem_type') in {'ext4', 'xfs'}
        and guest.get('mapped_to_attached_cloud_disk') is True
        and type(guest.get('cpu_model')) is str and 'EPYC 9B14' in guest['cpu_model'],
        'qualification_prerequisite_host')
    minimum = sum(DISK_LIMITS.values())+RESERVE_BYTES
    require(type(environment.get('freeze_cpu_affinity_count')) is int and environment['freeze_cpu_affinity_count'] == 8
        and type(environment.get('freeze_observed_free_bytes')) is int and environment['freeze_observed_free_bytes'] >= minimum
        and environment.get('disk_class') == 'pd-balanced' and type(environment.get('vcpus')) is int
        and environment['vcpus'] == 8 and type(environment.get('nominal_ram_bytes')) is int
        and environment['nominal_ram_bytes'] == 32*1024**3
        and environment.get('durable_ssd') is True and environment.get('cloud_disk_attached_to_current_instance') is True,
        'qualification_prerequisite_host')
    return dict(vcpus=8, nominal_ram_bytes=32*1024**3, disk_class='pd-balanced',
        disk_bytes=disk_size_bytes, minimum_starting_free_bytes=minimum)


@closed_inputs
def verify_prerequisites(frozen_root, repository, *, manifest_sha256, bindings_sha256, inputs):
    """Return checked facts and original identities, never a campaign receipt."""
    require(type(inputs) is dict and inputs.keys() == {'source_manifest', 'ci_receipt', 'build_receipt',
        'restore_receipt', 'crash_receipt', 'ci_logs', 'component_logs', 'restore_proof',
        'conformance', 'typescript', 'cloud'},
        'qualification_prerequisite_inputs')
    proofs = {}; root = Path(frozen_root)
    raw, proofs['manifest'] = read_original(OriginalFile(root, 'manifest.json'), expected_sha256=manifest_sha256)
    manifest = decode(raw)
    raw, proofs['bindings'] = read_original(OriginalFile(root, 'prerequisite-bindings.json'), expected_sha256=bindings_sha256)
    bindings = decode(raw)
    require(type(bindings) is dict and bindings.get('schema') == 'riffdb.wp825-measured-prerequisite-bindings/v2'
        and bindings.get('manifest_sha256') == manifest_sha256 and bindings.get('campaign_started') is False
        and bindings.get('qualifies_complete_campaign') is False, 'qualification_prerequisite_bindings')
    revision = bindings.get('source_revision')
    require(type(revision) is str and re.fullmatch(r'[0-9a-f]{40}', revision), 'qualification_prerequisite_source')
    require(type(manifest) is dict and manifest.keys() == {'schema', 'target', 'artifacts', 'faults', 'resource_limits'}
        and manifest['schema'] == contract.MANIFEST_SCHEMA and type(manifest['artifacts']) is dict
        and manifest['artifacts'].keys() == contract.ARTIFACTS, 'qualification_prerequisite_manifest')
    exact(manifest['target'], dict(contract.TARGET), 'qualification_prerequisite_manifest')
    exact(manifest['faults'], schedule(), 'qualification_prerequisite_manifest')
    exact(manifest['resource_limits'], dict(PUBLIC_LIMITS), 'qualification_prerequisite_manifest')
    exact(bindings.get('artifact_paths'), {name:'artifacts/'+name for name in contract.ARTIFACTS},
        'qualification_prerequisite_bindings')
    artifacts = {}; values = {}
    for name, checksum in manifest['artifacts'].items():
        raw, artifacts[name] = read_original(OriginalFile(root, 'artifacts/'+name),
            maximum=MAX_FILE, expected_sha256=checksum)
        if name not in {'server', 'cli', 'rust_worker', 'typescript_client'}:
            values[name] = decode(raw, elapsed_seconds=name in {'ci', 'conformance'})
    raw, proofs['source_manifest'] = read_original(inputs['source_manifest'], expected_sha256=bindings['source_manifest_sha256'])
    inventory = decode(raw); workload = values['workload']
    require(type(workload) is dict, 'qualification_prerequisite_workload')
    require(workload.get('schema') == 'riffdb.wp825-frozen-workload/v1' and workload.get('source_revision') == revision
        and workload.get('source_manifest_sha256') == proofs['source_manifest']['sha256']
        and workload.get('actual_rehearsal_duration_s') in {28800, 86400}, 'qualification_prerequisite_workload')
    exact(workload.get('target'), dict(contract.TARGET), 'qualification_prerequisite_workload')
    exact(workload.get('application_slots'), 28, 'qualification_prerequisite_workload')
    exact(workload.get('auxiliary_slots'), 4, 'qualification_prerequisite_workload')
    verify_source(repository, inventory, workload, revision)
    require(inventory.get('release/durable-format-manifest-v1.json') == artifacts['format_manifest']['sha256'],
        'qualification_prerequisite_format')
    exact(values['fault_schedule'], schedule(), 'qualification_prerequisite_manifest')
    exact(values['resource_model'], dict(schema='riffdb.wp825-frozen-resource-model/v1',
        resource_limits=PUBLIC_LIMITS, disk_limits=DISK_LIMITS, process_limits=PROCESS_LIMITS,
        reserve_bytes=RESERVE_BYTES, minimum_free_bytes=sum(DISK_LIMITS.values())+RESERVE_BYTES),
        'qualification_prerequisite_model')
    validate_protocol(values['restore_protocol'])
    current_protocol = protocol_snapshot(repository)
    exact(values['restore_protocol'], current_protocol, 'qualification_prerequisite_protocol')
    # The unchanged entrypoint binds precisely these bytes into its clock.
    # Semantically equal compact JSON would have a different frozen SHA.
    protocol_bytes = json.dumps(current_protocol, sort_keys=True).encode()+b'\n'
    require(artifacts['restore_protocol']['sha256'] == hashlib.sha256(protocol_bytes).hexdigest(),
        'qualification_prerequisite_protocol_encoding')
    receipts = {}
    for name, binding in (('ci_receipt', 'ci_receipt_sha256'), ('build_receipt', 'build_receipt_sha256'),
            ('restore_receipt', 'whole_restore_receipt_sha256'), ('crash_receipt', 'native_crash_receipt_sha256')):
        raw, proofs[name] = read_original(inputs[name], expected_sha256=bindings[binding])
        receipts[name] = decode(raw, elapsed_seconds=True)
        require(type(receipts[name]) is dict and receipts[name].get('revision') == revision
            and receipts[name].get('status') == 'passed', 'qualification_prerequisite_lineage')
    ci, build = receipts['ci_receipt'], receipts['build_receipt']
    verify_ci(ci, values['ci'], values['conformance'], revision, inputs['ci_logs'], proofs, repository)
    hashes = bindings.get('ordinary_artifact_sha256')
    require(type(hashes) is dict and hashes.keys() == ARTIFACT_NAMES, 'qualification_prerequisite_binaries')
    exact(bindings.get('ordinary_artifact_paths'), {name:'ordinary-artifacts/'+name for name in ARTIFACT_NAMES},
        'qualification_prerequisite_binaries')
    for name, checksum in hashes.items():
        _, proofs['ordinary:'+name] = read_original(OriginalFile(root, 'ordinary-artifacts/'+name), maximum=MAX_FILE,
            expected_sha256=checksum)
    for artifact, binary in (('server', 'riffdbd'), ('cli', 'riffdb'), ('rust_worker', 'lifecycle')):
        require(artifacts[artifact]['sha256'] == hashes[binary], 'qualification_prerequisite_binaries')
    for receipt in (ci, build, receipts['restore_receipt']):
        exact(receipt.get('artifacts_sha256'), hashes, 'qualification_prerequisite_binaries')
        require(receipt.get('source_manifest_sha256') == proofs['source_manifest']['sha256'],
            'qualification_prerequisite_lineage')
    require(ci.get('build_receipt_sha256') == proofs['build_receipt']['sha256']
        and ci.get('restore_receipt_sha256') == proofs['restore_receipt']['sha256']
        and receipts['restore_receipt'].get('build_receipt_sha256') == proofs['build_receipt']['sha256']
        and receipts['crash_receipt'].get('ci_receipt_sha256') == proofs['ci_receipt']['sha256'],
        'qualification_prerequisite_lineage')
    from .ticketdesk_lifecycle_campaign_qualification_components import verify_components
    components = verify_components(receipts, inputs, proofs, revision, hashes)
    verify_reports(ci, values['conformance'], inputs, artifacts, proofs)
    require(values['environment'].get('source_revision') == revision, 'qualification_prerequisite_host')
    host = verify_host(values['environment'], inputs['cloud'], proofs)
    verify_source(repository, inventory, workload, revision)
    return dict(source_revision=revision, manifest=manifest, bindings=bindings, artifacts=artifacts,
        inputs=proofs, host=host, components=components, duration_s=workload['actual_rehearsal_duration_s'],
        source_clean=True, full_ci=True, four_language_conformance=True, typescript_smoke=True,
        qualifies_complete_campaign=False)
