# req: END-007, END-008, END-010
"""Synthetic original-input joins; these fixtures never qualify a real campaign."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from lib import ticketdesk_lifecycle_campaign_qualification_prerequisites as qualification
from lib.ticketdesk_lifecycle_campaign_restore_evidence import FILES, RULES

REVISION = 'a'*40
TREE = 'b'*40


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()+b'\n'


def fixture(root, *, fault=None, actual_repository=False):
    """Only fixed synthetic bytes and mocked Git facts, never measured evidence."""
    source, frozen, originals = (root/name for name in ('source', 'frozen', 'originals'))
    for path in (source, frozen, originals): path.mkdir(mode=0o700)
    def write(directory, name, raw):
        path = directory/name
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        path.write_bytes(raw); path.chmod(0o600)
        return qualification.OriginalFile(directory, name)
    def record(directory, name, value):
        return write(directory, name, encoded(value))
    source_bytes = {'SPEC.md': b'synthetic source only\n',
        'release/durable-format-manifest-v1.json': encoded({'synthetic_format_fixture': True}),
        'scripts/ci-all': b'./scripts/check-generated\n./scripts/check-version-topology\n',
        'scripts/check-generated': b'# synthetic checked generator\n'}
    if actual_repository:
        repository = Path(__file__).resolve().parents[2]
        source_bytes.update({name:(repository/name).read_bytes() for name in FILES})
    for name, raw in source_bytes.items(): write(source, name, raw)
    revision, tree = REVISION, TREE
    if actual_repository:
        for arguments in (['init', '--quiet'], ['add', '--', *source_bytes],
                ['-c', 'core.hooksPath=/dev/null', '-c', 'user.name=Qualification Fixture',
                 '-c', 'user.email=fixture@example.com', 'commit', '--quiet', '-m', 'synthetic fixture']):
            subprocess.run(['git', '-C', str(source), *arguments], check=True, capture_output=True)
        revision = subprocess.check_output(['git', '-C', str(source), 'rev-parse', 'HEAD'], text=True).strip()
        tree = subprocess.check_output(['git', '-C', str(source), 'rev-parse', 'HEAD^{tree}'], text=True).strip()
    inventory = {name:sha(raw) for name, raw in source_bytes.items()}
    manifest_file = record(originals, 'source-manifest.json', inventory)
    manifest_hash = sha(encoded(inventory))
    snapshot = dict(revision=revision, tree=tree, status=b'', files=list(inventory))
    protocol = dict(schema='riffdb.ticketdesk-lifecycle-restore-protocol/v1', method='primary_loss_v1',
        acknowledgement_method='acknowledged_prefix_v1', rules=dict(RULES),
        source_files={name:'c'*64 for name in FILES})
    if actual_repository: protocol = qualification.protocol_snapshot(source)
    ordinary = {name:('synthetic executable '+name+'\n').encode() for name in qualification.ARTIFACT_NAMES}
    hashes = {name:sha(raw) for name, raw in ordinary.items()}
    for name, raw in ordinary.items(): write(frozen, 'ordinary-artifacts/'+name, raw)
    binaries = {key:ordinary[name] for key, name in (('server','riffdbd'), ('cli','riffdb'), ('rust_worker','lifecycle'))}
    inputs = dict(source_manifest=manifest_file, ci_logs={}, component_logs={}, restore_proof={},
        conformance={}, typescript={}, cloud={})
    versions = {}
    for version in ('1', '2'):
        reports = dict(observation=encoded(qualification.TS_REPORT),
            client=('synthetic V'+version+' TypeScript client\n').encode(),
            executed_smoke=b'synthetic executed JavaScript fixture\n')
        if fault == 'typescript_semantics' and version == '2':
            value = deepcopy(qualification.TS_REPORT); value['partition_isolation'] = False
            reports['observation'] = encoded(value)
        inputs['typescript'][version] = {kind:write(originals, 'ts-'+version+'-'+kind, raw)
            for kind, raw in reports.items()}
        versions[version] = {kind+'_sha256':sha(raw) for kind, raw in reports.items()}
        if version == '1': binaries['typescript_client'] = reports['client']
    observed = {}
    for language in sorted(qualification.LANGUAGES):
        reports = dict(observation=encoded(dict(qualification.CONFORMANCE_REPORT, language=language)),
            revoked=encoded(qualification.REVOKED_REPORT))
        if fault == 'conformance_semantics' and language == 'go':
            value = dict(qualification.CONFORMANCE_REPORT, language=language, exact_total=True)
            reports['observation'] = encoded(value)
        inputs['conformance'][language] = {kind:write(originals, language+'-'+kind, raw)
            for kind, raw in reports.items()}
        observed[language] = {kind:sha(raw) for kind, raw in reports.items()}
    cloud = dict(
        disk=dict(selfLink='https://example.com/disks/synthetic', type='https://example.com/diskTypes/pd-balanced',
            sizeGb='500', status='READY'),
        instance=dict(machineType='https://example.com/machineTypes/c3d-standard-8', status='RUNNING',
            disks=[dict(source='https://example.com/disks/synthetic', mode='READ_WRITE', type='PERSISTENT')]),
        machine=dict(guestCpus=8, memoryMb=32768),
        guest=dict(vcpus=8, memory={'MemTotal':31*1024**3}, campaign_filesystem_disk_bytes=500*1024**3,
            campaign_filesystem_type='ext4', mapped_to_attached_cloud_disk=True, cpu_model='AMD EPYC 9B14'))
    if fault == 'cloud_attachment': cloud['instance']['disks'][0]['source'] = 'https://example.com/disks/other'
    if fault == 'host_numeric_boolean': cloud['machine']['guestCpus'] = True
    for kind, value in cloud.items(): inputs['cloud'][kind] = record(originals, 'cloud-'+kind, value)
    environment = dict(schema='riffdb.wp825-prerequisite-environment/v1', source_revision=revision,
        cloud_evidence_sha256={kind:sha(encoded(value)) for kind, value in cloud.items()},
        freeze_cpu_affinity_count=8, freeze_observed_free_bytes=135*1024**3,
        disk_class='pd-balanced', vcpus=8, nominal_ram_bytes=32*1024**3,
        durable_ssd=True, cloud_disk_attached_to_current_instance=True)
    if fault == 'host_free_space': environment['freeze_observed_free_bytes'] = 133*1024**3
    def component_steps(kind, step, raw):
        inputs['component_logs'][kind] = {step:write(originals, kind+'-'+step+'.log', raw)}
        return [dict(step=step, exit=0, seconds=0.25, log_sha256=sha(raw))]
    build = dict(status='passed', revision=revision, artifacts_sha256=hashes,
        source_manifest_sha256=manifest_hash, ordinary_shipping_profile=True,
        steps=component_steps('build', 'ordinary-four-binary-build', b'Finished `release` profile [optimized]\n'))
    if fault == 'build_profile': build['ordinary_shipping_profile'] = False
    if fault == 'build_exit': build['steps'][0]['exit'] = 1
    inputs['build_receipt'] = record(originals, 'build-receipt.json', build)
    build_hash = sha(encoded(build))
    witness = b'RDBTW001'+(2).to_bytes(8, 'big')
    for index in range(2):
        witness += b''.join(value.to_bytes(8, 'big') for value in (index, 65537+index, 1, 1))
    inputs['restore_proof']['acknowledgements'] = write(originals, 'restore-acknowledgements', witness)
    helper, helper_log = b'# synthetic restore helper; never executed\n', b''
    inputs['restore_proof']['helper'] = write(originals, 'restore-helper.py', helper)
    inputs['restore_proof']['log'] = write(originals, 'restore-helper.log', helper_log)
    observation = dict(schema='riffdb.ticketdesk-populated-public-restore-diagnostic/v2', revision=revision,
        phase='complete', recovery_clock_method='primary_loss_v1', pre_release_deadline_ns=45000000000,
        post_backup_acknowledged_commands=0, restore_succeeded_ns=37000000000,
        restored_readiness_ns=37000000001, pre_release_reconciled_ns=41000000000,
        fixed_release_ns=45000000001, first_successful_write_ns=45100000000,
        whole_backup_restore_first_write_ns=45100000001, whole_backup_restore_reconciled_ns=48000000000,
        live_archive_copied=False, complete_campaign_checks_proved=False,
        original_acknowledgement_witness_sha256=sha(witness), acknowledged_commands_before_loss=2)
    for key in ('whole_backup_restore_fault_proved', 'state_reconciled', 'application_role_renewed_by_public_operator',
            'original_acknowledgement_witness_bound', 'first_write_exact_replay',
            'original_acknowledged_identity_replayed_after_restore', 'retained_backup_and_archive_roots_used',
            'existing_replacement_inputs_durable_before_primary_loss', 'native_restore_materialization_remains_inside_recovery_clock',
            'selected_backup_durably_complete_before_primary_loss', 'empty_target_before_start'):
        observation[key] = True
    if fault == 'restore_45': observation['pre_release_reconciled_ns'] = 45000000000
    if fault == 'restore_60': observation['whole_backup_restore_reconciled_ns'] = 60000000001
    if fault == 'restore_replay': observation['original_acknowledged_identity_replayed_after_restore'] = False
    whole = dict(status='passed', revision=revision, helper_exit=0, attempt_seconds=76.533,
        artifacts_sha256=hashes, helper_sha256=sha(helper), log_sha256=sha(helper_log),
        whole_gate_deadline_ns=60000000000, recovery_clock_method='primary_loss_v1', observation=observation)
    inputs['restore_proof']['observation'] = record(originals, 'restore-observation.json', whole)
    restore = dict(status='passed', revision=revision, artifacts_sha256=hashes,
        source_manifest_sha256=manifest_hash, build_receipt_sha256=build_hash,
        whole_gate_deadline_unchanged_ns=60000000000, whole_observation=whole,
        whole_observation_receipt_sha256=sha(encoded(whole)),
        steps=component_steps('restore', 'primary-loss-whole-restore', b''))
    if fault == 'restore_lineage': restore['build_receipt_sha256'] = 'e'*64
    inputs['restore_receipt'] = record(originals, 'restore-receipt.json', restore)
    restore_hash = sha(encoded(restore))
    branch = ['./scripts/check-generated', './scripts/check-version-topology', './scripts/ci-all']
    package = ['./scripts/check-allowed-paths --wp WP-825 --range HEAD..HEAD', './scripts/ci-all']
    plan = lambda commands: ''.join('   '+str(i+1)+'  '+command+'\n' for i, command in enumerate(commands)).encode()
    ci_contents = {'whole-authored-branch-full-acceptance-plan':plan(branch), 'package-full-acceptance-plan':plan(package),
        'accepted-tls-native-full-recovery-matrix':b'full_recovery_matrix: 10 passed\n',
        'complete-workspace-ci-and-package-acceptance':b'2 passed, 0 failed, 0 skipped.\n'}
    if fault == 'native_recovery': ci_contents['accepted-tls-native-full-recovery-matrix'] = b'no actual native recovery proof\n'
    steps = []
    for name in sorted(qualification.CI_STEPS):
        raw = ci_contents.get(name, b'synthetic CI log only\n')
        inputs['ci_logs'][name] = write(originals, name+'.log', raw)
        steps.append(dict(step=name, exit=0, seconds=0.25, log_sha256=sha(raw)))
    if fault == 'ci_exit': steps[0]['exit'] = 1
    if fault == 'ci_boolean_exit': steps[0]['exit'] = False
    if fault == 'ci_duplicate': steps.append(deepcopy(steps[0]))
    ci = dict(status='passed', revision=revision, steps=steps, source_manifest_sha256=manifest_hash,
        artifacts_sha256=hashes, build_receipt_sha256=build_hash, restore_receipt_sha256=restore_hash,
        typescript_versions=versions, conformance_observations_sha256=observed,
        full_ci_passed=True, four_language_conformance_passed=True, typescript_smoke_qualified=True,
        branch_acceptance_commands=branch, package_acceptance_commands=package, additional_branch_checks_executed=[])
    if fault == 'ci_coverage': ci['branch_acceptance_commands'] = ['./scripts/ci-all']
    if fault == 'ci_lineage': ci['revision'] = 'f'*40
    if fault == 'no_original_conformance_binding': ci.pop('conformance_observations_sha256')
    if fault == 'producer_booleans_false':
        for key in ('full_ci_passed', 'four_language_conformance_passed', 'typescript_smoke_qualified'): ci[key] = False
    inputs['ci_receipt'] = record(originals, 'ci-receipt.json', ci)
    ci_hash = sha(encoded(ci))
    crash = dict(status='passed', revision=revision, ci_receipt_sha256=ci_hash,
        source_manifest_sha256=manifest_hash, actual_original_crash_cases_passed=10,
        steps=component_steps('crash', 'all-ten-original-offline-process-crashes',
            b'offline_maintenance_matrix: 10 passed\n'))
    if fault == 'crash_count': crash['actual_original_crash_cases_passed'] = 9
    inputs['crash_receipt'] = record(originals, 'crash-receipt.json', crash)
    workload = dict(schema='riffdb.wp825-frozen-workload/v1', source_revision=revision,
        source_tree=tree, actual_rehearsal_duration_s=28800, target=dict(qualification.contract.TARGET),
        application_slots=28, auxiliary_slots=4, sources=inventory, source_manifest_sha256=manifest_hash)
    resources = dict(schema='riffdb.wp825-frozen-resource-model/v1', resource_limits=qualification.PUBLIC_LIMITS,
        disk_limits=qualification.DISK_LIMITS, process_limits=qualification.PROCESS_LIMITS,
        reserve_bytes=qualification.RESERVE_BYTES,
        minimum_free_bytes=sum(qualification.DISK_LIMITS.values())+qualification.RESERVE_BYTES)
    conformance = dict(schema='riffdb.wp825-observed-conformance/v1', source_revision=revision,
        steps=[s for s in steps if s['step'] in qualification.CONFORMANCE_STEPS],
        ci_receipt_sha256=ci_hash, retained_typescript_versions=versions)
    values = dict(workload=workload, fault_schedule=qualification.schedule(), resource_model=resources,
        environment=environment, conformance=conformance, ci=dict(schema='riffdb.wp825-observed-complete-source-ci/v1',
            source_revision=revision, full_ci_receipt_sha256=ci_hash, full_ci_receipt=ci), restore_protocol=protocol)
    binaries['format_manifest'] = source_bytes['release/durable-format-manifest-v1.json']
    artifact_hashes = {}
    for name in qualification.contract.ARTIFACTS:
        raw = binaries[name] if name in binaries else encoded(values[name])
        if name == 'restore_protocol' and fault != 'protocol_encoding':
            raw = json.dumps(values[name], sort_keys=True).encode()+b'\n'
        write(frozen, 'artifacts/'+name, raw); artifact_hashes[name] = sha(raw)
    manifest = dict(schema=qualification.contract.MANIFEST_SCHEMA, target=dict(qualification.contract.TARGET),
        artifacts=artifact_hashes, faults=qualification.schedule(), resource_limits=qualification.PUBLIC_LIMITS)
    bindings = dict(schema='riffdb.wp825-measured-prerequisite-bindings/v2', source_revision=revision,
        ci_receipt_sha256=ci_hash, whole_restore_receipt_sha256=restore_hash,
        native_crash_receipt_sha256=sha(encoded(crash)), build_receipt_sha256=build_hash,
        source_manifest_sha256=manifest_hash, manifest_sha256=sha(encoded(manifest)),
        ordinary_artifact_paths={name:'ordinary-artifacts/'+name for name in hashes}, ordinary_artifact_sha256=hashes,
        artifact_paths={name:'artifacts/'+name for name in artifact_hashes}, campaign_started=False,
        qualifies_complete_campaign=False)
    record(frozen, 'manifest.json', manifest); record(frozen, 'prerequisite-bindings.json', bindings)
    options = dict(manifest_sha256=sha(encoded(manifest)), bindings_sha256=sha(encoded(bindings)), inputs=inputs)
    return SimpleFixture(source, frozen, inputs, snapshot, protocol, options, actual_repository)


class SimpleFixture:
    def __init__(self, source, frozen, inputs, snapshot, protocol, options, actual_repository=False):
        self.source, self.frozen, self.inputs = source, frozen, inputs
        self.snapshot, self.protocol, self.options = snapshot, protocol, options
        self.actual_repository = actual_repository

    def verify(self):
        if self.actual_repository:
            return qualification.verify_prerequisites(self.frozen, self.source, **self.options)
        with patch.object(qualification, 'source_snapshot', return_value=self.snapshot), \
                patch.object(qualification, 'protocol_snapshot', return_value=self.protocol):
            return qualification.verify_prerequisites(self.frozen, self.source, **self.options)


class QualificationPrerequisiteTests(unittest.TestCase):
    def test_original_frozen_input_join_proves_only_prerequisites_and_records_exact_identities(self):
        with tempfile.TemporaryDirectory() as temporary:
            value = fixture(Path(temporary)); result = value.verify()
            self.assertEqual(result['source_revision'], REVISION)
            self.assertEqual(result['duration_s'], 28800)
            self.assertEqual(set(result['artifacts']), qualification.contract.ARTIFACTS)
            self.assertEqual(result['host']['minimum_starting_free_bytes'], 134*1024**3)
            self.assertFalse(result['qualifies_complete_campaign'])
            for key in ('source_clean', 'full_ci', 'four_language_conformance', 'typescript_smoke'):
                self.assertIs(result[key], True)
            for language in qualification.LANGUAGES:
                self.assertIn('conformance:'+language+':observation', result['inputs'])
                self.assertIn('conformance:'+language+':revoked', result['inputs'])
            self.assertEqual(result['inputs']['restore_log:primary-loss-whole-restore']['bytes'], 0)
            self.assertEqual(result['components']['original_offline_crash_cases'], 10)
            self.assertEqual(result['components']['saved_restore']['pre_release_reconciled_ns'], 41000000000)

    def test_success_flags_never_override_failed_original_observation_or_supply_the_proof(self):
        for fault in ('ci_exit', 'ci_boolean_exit', 'ci_duplicate', 'ci_lineage', 'restore_lineage',
                'no_original_conformance_binding', 'typescript_semantics', 'conformance_semantics',
                'cloud_attachment', 'host_numeric_boolean', 'host_free_space', 'build_profile', 'build_exit',
                'restore_45', 'restore_60', 'restore_replay', 'crash_count', 'native_recovery', 'ci_coverage'):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as temporary:
                with self.assertRaises(qualification.PrerequisiteError): fixture(Path(temporary), fault=fault).verify()
        with tempfile.TemporaryDirectory() as temporary:
            result = fixture(Path(temporary), fault='producer_booleans_false').verify()
            self.assertTrue(result['full_ci'])
            self.assertFalse(result['qualifies_complete_campaign'])

    def test_semantically_equal_compact_protocol_json_refuses_before_starting_any_campaign(self):
        with tempfile.TemporaryDirectory() as temporary:
            value = fixture(Path(temporary), fault='protocol_encoding')
            raw = (value.frozen/'artifacts/restore_protocol').read_bytes()
            self.assertEqual(json.loads(raw), value.protocol)
            with self.assertRaisesRegex(qualification.PrerequisiteError, '^qualification_prerequisite_protocol_encoding$'):
                value.verify()

    def test_missing_tampered_or_mixed_original_files_refuse_even_when_all_flags_claim_success(self):
        for kind in ('ci_log', 'typescript', 'conformance', 'cloud', 'ordinary', 'artifact', 'source', 'missing',
                'restore_observation', 'restore_helper', 'restore_witness', 'crash_log'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as temporary:
                value = fixture(Path(temporary))
                selected = dict(ci_log=next(iter(value.inputs['ci_logs'].values())),
                    typescript=value.inputs['typescript']['2']['client'],
                    conformance=value.inputs['conformance']['rust']['revoked'], cloud=value.inputs['cloud']['disk'],
                    ordinary=qualification.OriginalFile(value.frozen, 'ordinary-artifacts/riffdbd'),
                    artifact=qualification.OriginalFile(value.frozen, 'artifacts/format_manifest'),
                    source=qualification.OriginalFile(value.source, 'SPEC.md'), missing=value.inputs['ci_receipt'],
                    restore_observation=value.inputs['restore_proof']['observation'],
                    restore_helper=value.inputs['restore_proof']['helper'], restore_witness=value.inputs['restore_proof']['acknowledgements'],
                    crash_log=next(iter(value.inputs['component_logs']['crash'].values())))[kind]
                path = selected.root/selected.name
                if kind == 'missing': path.unlink()
                else: path.write_bytes(path.read_bytes()+b'tampered\n')
                with self.assertRaises(qualification.PrerequisiteError): value.verify()

    def test_externally_fixed_roots_lineage_cleanliness_and_exact_source_inventory_are_required(self):
        for kind in ('manifest_digest', 'bindings_digest', 'revision', 'tree', 'dirty', 'extra_source'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as temporary:
                value = fixture(Path(temporary))
                if kind == 'manifest_digest': value.options['manifest_sha256'] = 'e'*64
                elif kind == 'bindings_digest': value.options['bindings_sha256'] = 'e'*64
                elif kind == 'revision': value.snapshot['revision'] = 'e'*40
                elif kind == 'tree': value.snapshot['tree'] = 'e'*40
                elif kind == 'dirty': value.snapshot['status'] = b' M SPEC.md\n'
                else: value.snapshot['files'].append('unbound-source.rs')
                with self.assertRaises(qualification.PrerequisiteError): value.verify()

    def test_duplicate_nonfinite_wrongly_placed_decimal_and_unbounded_json_refuse(self):
        bad = [b'{"status":true,"status":true}', b'{"steps":[{"seconds":NaN}]}',
            b'{"steps":[{"seconds":Infinity}]}', b'{"steps":[{"seconds":1e9999}]}',
            b'{"steps":[{"seconds":0.0}]}', b'{"bytes":0.25}', b'{"number":18446744073709551616}',
            b'['*40+b'0'+b']'*40]
        for raw in bad:
            with self.subTest(raw=raw), self.assertRaises(qualification.PrerequisiteError):
                qualification.decode(raw, elapsed_seconds=True)
        self.assertEqual(qualification.decode(b'{"steps":[{"seconds":0.25}]}', elapsed_seconds=True),
            {'steps':[{'seconds':qualification.Decimal('0.25')}]})
        with self.assertRaises(qualification.PrerequisiteError):
            qualification.decode(b'{"steps":[{"seconds":0.25}]}')

    def test_original_file_custody_refuses_links_public_permissions_empty_json_and_escaped_paths(self):
        for kind in ('symlink', 'hardlink', 'public', 'empty', 'escape'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary); path = root/'original'; path.write_bytes(b'original\n'); path.chmod(0o600)
                selected = qualification.OriginalFile(root, 'original')
                if kind == 'symlink': (root/'link').symlink_to(path); selected = qualification.OriginalFile(root, 'link')
                elif kind == 'hardlink': (root/'link').hardlink_to(path)
                elif kind == 'public': path.chmod(0o644)
                elif kind == 'empty': path.write_bytes(b'')
                else: selected = qualification.OriginalFile(root, '../original')
                with self.assertRaises(qualification.PrerequisiteError): qualification.read_original(selected)
