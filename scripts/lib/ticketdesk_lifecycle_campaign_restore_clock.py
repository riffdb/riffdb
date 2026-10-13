# req: END-001, END-008
"""One primary-loss deadline and a separately visible durable-backup preparation.

This is controller bookkeeping, not native backup validation or campaign
qualification. Public operation, artifact, acknowledgement and worker witnesses
must be observed independently by the integrating controller.
"""
from copy import deepcopy
import re

from .ticketdesk_lifecycle_campaign_maintenance import CampaignMaintenance, UUID7

SECOND = 10**9
MAX_U64 = 2**64 - 1
MAX_BACKUP_BYTES = 32 * 1024**3
MAX_ARTIFACTS = 4096
FIXED = frozenset({'database.redb', 'journal.riffextent', 'format.riffdb', 'manifest.riffdb'})
RECOVERY_PHASES = ('target_setup', 'target_admission', 'restore', 'readiness',
                   'first_write', 'original_replay', 'state_reconciliation')
RECOVERY_PHASE_ORDERS = (RECOVERY_PHASES, RECOVERY_PHASES[:4] +
                         ('original_replay', 'state_reconciliation', 'first_write'))


class RestoreClockError(ValueError):
    """Closed refusal containing no supplied paths, credentials or payloads."""


def require(condition, code):
    if not condition:
        raise RestoreClockError(code)


def number(value):
    require(type(value) is int and 0 <= value <= MAX_U64, 'restore_clock_number')
    return value


def digest(value):
    require(type(value) is str and re.fullmatch(r'[0-9a-f]{64}', value) is not None,
            'restore_clock_digest')
    return value


def fenced(function):
    def checked(self, *args, **kwargs):
        require(not self.failed, 'restore_clock_failed')
        try:
            return function(self, *args, **kwargs)
        except BaseException:
            self.failed = True
            raise
    return checked


def artifact_binding(artifacts, manifest_sha256):
    require(type(artifacts) is dict and FIXED <= artifacts.keys()
            and 4 <= len(artifacts) <= MAX_ARTIFACTS, 'restore_clock_inventory')
    total = 0
    for name, value in artifacts.items():
        require(type(name) is str, 'restore_clock_inventory')
        if name not in FIXED:
            match = re.fullmatch(r'journal\.sealed\.([0-9]{20})\.riffextent', name)
            require(match is not None and 0 < int(match[1]) <= MAX_U64,
                    'restore_clock_inventory')
        require(type(value) is dict and value.keys() == {'sha256', 'bytes'},
                'restore_clock_inventory')
        digest(value['sha256'])
        size = number(value['bytes'])
        require(0 < size <= MAX_BACKUP_BYTES, 'restore_clock_backup_bound')
        total += size
        require(total <= MAX_BACKUP_BYTES, 'restore_clock_backup_bound')
    require(artifacts['manifest.riffdb']['sha256'] == digest(manifest_sha256)
            and artifacts['manifest.riffdb']['bytes'] <= 32 * 1024**2,
            'restore_clock_manifest')
    return deepcopy(artifacts), total


class RestoreRecoveryClock:
    method = 'primary_loss_v1'
    qualifies_complete_campaign = False

    def __init__(self, planned_loss_ns):
        planned = number(planned_loss_ns)
        require(60 * SECOND <= planned <= MAX_U64 - 60 * SECOND,
                'restore_clock_schedule')
        self.planned_loss_ns = planned
        self.failed = self.complete = False
        self.state = 'idle'
        self.last_ns = None
        self.preparation = self.backup = self.loss_ns = None
        self._deadline_ns = None
        self.phases = []

    @property
    def deadline_ns(self):
        require(self._deadline_ns is not None, 'restore_clock_loss_required')
        return self._deadline_ns

    def _tick(self, now_ns):
        number(now_ns)
        require(not self.complete and (self.last_ns is None or now_ns >= self.last_ns),
                'restore_clock_rewind')
        self.last_ns = now_ns
        if self.loss_ns is not None:
            require(now_ns <= self.deadline_ns, 'restore_clock_deadline')

    @fenced
    def begin_preparation(self, now_ns):
        self._tick(now_ns)
        require(self.state == 'idle'
                and self.planned_loss_ns - 60 * SECOND <= now_ns < self.planned_loss_ns,
                'restore_clock_preparation')
        self.preparation = dict(started_ns=now_ns)
        self.state = 'quiescing'

    @fenced
    def bind_acknowledgements(self, now_ns, *, acknowledged_commands, acknowledged_frontier,
                              acknowledgement_witness_sha256):
        self._tick(now_ns)
        require(self.state == 'quiescing' and now_ns < self.planned_loss_ns,
                'restore_clock_preparation')
        require(0 < number(acknowledged_commands) <= 86400 * 100
                and number(acknowledged_frontier) > 0, 'restore_clock_acknowledgements')
        self.preparation.update(acknowledgements_bound_ns=now_ns,
            acknowledged_commands=acknowledged_commands,
            acknowledged_frontier=acknowledged_frontier,
            acknowledgement_witness_sha256=digest(acknowledgement_witness_sha256))
        self.state = 'preparing'

    @fenced
    def backup_durable(self, now_ns, operation, *, manifest_sha256, artifacts):
        self._tick(now_ns)
        require(self.state == 'preparing' and now_ns < self.planned_loss_ns,
                'restore_clock_backup_before_loss')
        require(isinstance(operation, CampaignMaintenance)
                and operation.kind == 'create' and operation.started and operation.complete
                and not operation.failed and operation.phase == 'succeeded'
                and self.preparation['started_ns'] <= operation.started_ns <= now_ns
                and operation.deadline_ns <= self.planned_loss_ns
                and type(operation.operation) is str
                and re.fullmatch(UUID7, operation.operation) is not None,
                'restore_clock_durable_operation')
        inventory, size = artifact_binding(artifacts, manifest_sha256)
        self.backup = dict(name=operation.name, maintenance_operation_id=operation.operation,
            input_hash=digest(operation.input_hash), manifest_sha256=manifest_sha256,
            operation_started_ns=operation.started_ns, operation_completed_ns=operation.last_ns,
            operation_deadline_ns=operation.deadline_ns,
            artifacts=inventory, bytes=size, durable_at_ns=now_ns)
        self.state = 'backup_durable'

    @fenced
    def primary_loss(self, now_ns, *, acknowledged_commands, acknowledgement_witness_sha256,
                     selected_manifest_sha256):
        self._tick(now_ns)
        require(self.state == 'backup_durable'
                and now_ns // SECOND == self.planned_loss_ns // SECOND,
                'restore_clock_loss_schedule')
        require(number(acknowledged_commands) == self.preparation['acknowledged_commands']
                and digest(acknowledgement_witness_sha256)
                    == self.preparation['acknowledgement_witness_sha256'],
                'restore_clock_acknowledgement_gap')
        require(digest(selected_manifest_sha256) == self.backup['manifest_sha256'],
                'restore_clock_backup_changed')
        deadline = number(now_ns + 60 * SECOND)
        self.loss_ns = now_ns
        self._deadline_ns = deadline
        self.state = 'recovering'
        return self.deadline_ns

    @fenced
    def observe(self, now_ns):
        """Unknown polls consume the original deadline without advancing success."""
        self._tick(now_ns)
        require(self.state == 'recovering', 'restore_clock_loss_required')
        return self.deadline_ns - now_ns

    @fenced
    def phase_completed(self, name, now_ns):
        self.observe(now_ns)
        candidate = tuple(phase['kind'] for phase in self.phases) + (name,)
        require(len(self.phases) < len(RECOVERY_PHASES)
                and any(order[:len(candidate)] == candidate for order in RECOVERY_PHASE_ORDERS),
                'restore_clock_phase_order')
        self.phases.append(dict(kind=name, completed_ns=now_ns))

    @fenced
    def finish(self, now_ns):
        self.observe(now_ns)
        require(len(self.phases) == len(RECOVERY_PHASES), 'restore_clock_incomplete')
        self.state = 'complete'
        self.complete = True
        return dict(schema='riffdb.ticketdesk-lifecycle-restore-clock/v2',
            method=self.method, acknowledgement_method='acknowledged_prefix_v1',
            planned_loss_ns=self.planned_loss_ns,
            preparation=deepcopy(self.preparation), backup=deepcopy(self.backup),
            loss_ns=self.loss_ns, deadline_ns=self.deadline_ns, completed_ns=now_ns,
            phases=deepcopy(self.phases), qualifies_complete_campaign=False)
