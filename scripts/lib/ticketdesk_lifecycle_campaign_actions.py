# req: END-003, END-007, END-008, END-009, END-010
"""Execute the frozen seven public lifecycle faults on the original clock.

This is an unqualified controller draft. Actual native results and the complete
resource reconciliation remain prerequisites for any rehearsal claim.
"""
import json
import os
from pathlib import Path
import stat
import sys
import threading
import time

from .ticketdesk_lifecycle_campaign_maintenance import CampaignMaintenance, MAX_POLLS
from .ticketdesk_lifecycle_campaign_quiescence import QuiescentRelease
from .ticketdesk_lifecycle_campaign_restore_clock import RestoreRecoveryClock
from .ticketdesk_lifecycle_campaign_restore_inputs import AcknowledgedPrefix, SelectedBackup
from .ticketdesk_lifecycle_faults import LocalArchiveOutage
from .ticketdesk_lifecycle_campaign_runtime import SECOND, private_write, require

REPLAY_FIELDS = ("commit_sequence", "contract_version", "plan_hash", "outcome_type", "outcome",
                 "provenance_uri", "outcome_uri", "durability_mode")


class CampaignActions:
    def __init__(self, runtime, timeline):
        self.runtime, self.timeline = runtime, timeline
        self.original = None
        self.records = []
        self.restore_clock = None
        self.restore_release = self.restore_witness = self.restore_backup = None
        self.restore_receipt = None
        self.restore_read_admission = None

    def close(self):
        for owner in (self.restore_witness, self.restore_backup):
            if owner is not None:
                owner.close()

    def prepare_restore_if_due(self, elapsed):
        runtime = self.runtime
        planned = next(plan['start_s'] * SECOND for plan in runtime.faults
                       if plan['kind'] == 'backup_restore')
        if self.restore_clock is not None or elapsed < planned - 60 * SECOND:
            return False
        clock = self.restore_clock = RestoreRecoveryClock(planned)
        clock.begin_preparation(elapsed)
        release = self.restore_release = self.quiesce(elapsed, planned, restore_clock=clock)
        count = release.snapshot['load']['completed_commands']
        def frontier():
            value = self.call('server.health', ['server', 'health'], planned)
            result = value.get('result')
            require(value.get('ok') is True and type(result) is dict
                    and result.get('status') == 'ready'
                    and result.get('active_contract_version') == str(runtime.version)
                    and result.get('history_incarnation') == str(runtime.incarnation),
                    'campaign_restore_source_health')
            number = result.get('last_commit_sequence')
            require(type(number) is str and number.isascii() and number.isdecimal()
                    and 0 < len(number) <= 20 and 0 < int(number) < 2**64,
                    'campaign_restore_source_frontier')
            return int(number)
        included = frontier()
        last_observation = time.monotonic_ns()
        def checkpoint():
            nonlocal last_observation
            runtime.remaining(planned)
            now = time.monotonic_ns()
            if now - last_observation >= SECOND:
                self.wait(lambda: True, planned, active=False)
                last_observation = now
        self.restore_witness = AcknowledgedPrefix(runtime.root, commands=count,
            duration_s=runtime.duration, frontier=included, version=runtime.version,
            incarnation=runtime.incarnation, checkpoint=checkpoint)
        clock.bind_acknowledgements(runtime.elapsed(), acknowledged_commands=count,
            acknowledged_frontier=included,
            acknowledgement_witness_sha256=self.restore_witness.sha256)
        runtime.tick(force_native=True)
        operation = self.maintenance('create', runtime.elapsed(), planned)
        require(frontier() == included and release.poll(runtime.elapsed()),
                'campaign_restore_source_changed')
        self.restore_backup = SelectedBackup(runtime.root, operation.name, checkpoint=checkpoint)
        self.restore_witness.verify_prefix(count, checkpoint)
        clock.backup_durable(runtime.elapsed(), operation,
            manifest_sha256=self.restore_backup.manifest_sha256,
            artifacts=self.restore_backup.artifacts)
        release.allow_restore_reads(runtime.elapsed())
        self.wait(lambda: release.restore_reads_started(runtime.elapsed()), planned)
        runtime.tick(force_native=True)
        private_write(runtime.root, 'campaign-restore-preparation.json', json.dumps(dict(
            schema='riffdb.ticketdesk-lifecycle-restore-preparation/v2', method=clock.method,
            acknowledgement_method=self.restore_witness.method, preparation=clock.preparation,
            backup=clock.backup, qualifies_complete_campaign=False), sort_keys=True).encode()+b'\n')
        return True

    def wait(self, condition, deadline, *, active=True, scheduled_at=None):
        runtime = self.runtime
        while True:
            runtime.remaining(deadline)
            if active:
                runtime.tick(native=scheduled_at is None or runtime.elapsed() + 7 * SECOND < scheduled_at)
            else:
                runtime.observer()
                runtime.memory.poll(runtime.elapsed())
                runtime.disk.poll(runtime.elapsed(), archive_outage=runtime.active_archive_outage)
                runtime.epochs.check_gaps(runtime.elapsed())
                require(runtime.children["load"].poll() is None
                        and runtime.children["observer"].poll() is None, "campaign_original_worker_exit")
            if condition(): return
            threading.Event().wait(min(.02, runtime.remaining(deadline)))

    def call(self, command, arguments, deadline, *, credential=None):
        runtime = self.runtime
        value = runtime.operator.run_as(str(runtime.root / (credential or "operator.credential")),
            command, arguments, elapsed_ns=runtime.elapsed(), timeout_s=min(5, runtime.remaining(deadline)))
        runtime.remaining(deadline)
        return value

    def replay(self, deadline):
        runtime = self.runtime
        if self.original is None:
            descriptor = os.open(runtime.root / "updates.witness", os.O_RDONLY | os.O_NOFOLLOW)
            with os.fdopen(descriptor, "rb") as witness:
                info = os.fstat(witness.fileno())
                require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1
                        and info.st_uid == os.geteuid() and info.st_size == 16 + 32 * runtime.duration * 100,
                        "campaign_original_witness")
                header, record = witness.read(16), witness.read(32)
            require(header[:8] == b"RDBTW001" and int.from_bytes(header[8:], "big") == runtime.duration * 100
                    and len(record) == 32, "campaign_original_witness")
            ordinal, sequence, version, incarnation = [int.from_bytes(record[index:index+8], "big")
                                                     for index in range(0, 32, 8)]
            require(ordinal == 0 and sequence > 0 and version == incarnation == 1, "campaign_original_witness")
            private_write(runtime.root, "original-command.json", json.dumps({
                "idempotency_key": "ticketdesk-lifecycle-update-0",
                "organization_id": "01900000-0000-7000-8001-000000000000",
                "ticket_id": "01900000-0000-7000-8004-000000000000",
                "expected_revision": 0, "status": "InProgress"}, sort_keys=True).encode())
            self.original = {"sequence": sequence, "result": None}
        value = self.call("command.run", ["command", "run", "UpdateTicket", "--expected-version", "1",
            "--application", str(runtime.root / "application/riffdb.application.json"),
            "--input", str(runtime.root / "original-command.json")], deadline, credential=runtime.credential)
        result = value.get("result")
        require(type(result) is dict and result.get("status") == "replayed"
                and result.get("commit_sequence") == str(self.original["sequence"])
                and result.get("contract_version") == "1" and result.get("outcome_type") == "Updated"
                and result.get("durability_mode") == "group"
                and all(key in result for key in REPLAY_FIELDS)
                and all(type(result[key]) is str and 0 < len(result[key]) <= 4096
                        for key in ("plan_hash", "provenance_uri", "outcome_uri")),
                "campaign_original_replay")
        retained = {key: result[key] for key in REPLAY_FIELDS}
        require(self.original["result"] is None or self.original["result"] == retained,
                "campaign_original_replay")
        self.original["result"] = retained

    def reconcile(self, release, deadline):
        runtime = self.runtime
        count = release.snapshot["load"]["completed_commands"]
        environment = runtime.worker_environment(RIFFDB_LIFECYCLE_COMPLETED_COMMANDS=str(count),
            RIFFDB_LIFECYCLE_QUIESCED_COMMANDS=str(count),
            RIFFDB_LIFECYCLE_RECONCILIATION_WITNESS=str(runtime.root / "updates.witness"))
        result = runtime.operator.run_worker(str(runtime.artifacts / "lifecycle"), "reconcile", environment,
            elapsed_ns=runtime.elapsed(), timeout_s=runtime.remaining(deadline))
        require(result["updates"] == count, "campaign_state_reconciliation")
        runtime.remaining(deadline)

    def quiesce(self, started, deadline, *, pause_load=True, restore_clock=None):
        runtime = self.runtime
        release = QuiescentRelease(runtime.root, generation=runtime.generation,
                                   version=runtime.version, incarnation=runtime.incarnation,
                                   restore_clock=restore_clock)
        release.begin(started, pause_load=pause_load)
        self.wait(lambda: release.poll(runtime.elapsed()) if pause_load else
                  (release.poll(runtime.elapsed()) or release.observer_snapshot is not None), deadline)
        return release

    def release_workers(self, kind, started, held_seconds, deadline, release, *, restore=False):
        runtime = self.runtime
        release.publish(runtime.elapsed(), credential=runtime.credential, version=runtime.version,
                        incarnation=runtime.incarnation, restore=restore)
        release_at = started + held_seconds * SECOND
        require(runtime.elapsed() < release_at, "campaign_frozen_injection_overrun")
        self.wait(lambda: runtime.elapsed() >= release_at, deadline, scheduled_at=release_at)
        released = runtime.elapsed()
        release.resume(released)
        self.timeline.release(kind, released)
        runtime.generation += 1
        def resumed():
            observed = runtime.observer()
            return ((runtime.root / "load.resumed").is_file()
                    and observed["release_generation"] == runtime.generation
                    and observed["history_incarnation"] == runtime.incarnation
                    and not observed["observer_quiesced"])
        self.wait(resumed, deadline)
        observed = runtime.observer()
        release.workers_resumed(runtime.elapsed(), generation=runtime.generation,
                                incarnation=runtime.incarnation, duplicates=observed["duplicates"])
        if kind == 'backup_restore':
            private_write(runtime.root, 'campaign-restore-first-write.json', json.dumps(dict(
                schema='riffdb.ticketdesk-lifecycle-restore-first-write/v1',
                elapsed_ns=runtime.elapsed(), acknowledgement=release.resumed_snapshot),
                sort_keys=True).encode()+b'\n')

    def rotate(self, release, deadline):
        runtime = self.runtime
        old = runtime.credential
        replacement = "application-rotated.credential"
        value = self.call("role.bind", ["role", "bind", str(runtime.root / "application/riffdb.application.json"),
            "--role", "TicketDeskLifecycleApplication", "--principal", "lifecycle-smoke-application",
            "--actor-kind", "service", "--audience", "riffdb-ticketdesk-lifecycle",
            "--lifetime-seconds", "100800", "--credential-output", str(runtime.root / replacement)], deadline)
        require(value["result"].get("status") == "created", "campaign_rotation_identity")
        # Public role-binding output identifies the exact replaced capability.
        bound = json.loads((runtime.root / "bind-application.stdout").read_bytes())
        capability = bound["result"]["transition"]["identity"]["capability_id"]
        revoked = self.call("capability.revoke", ["capability", "revoke", capability, "--reason", "replaced"], deadline)
        require(revoked["result"].get("status") == "revoked", "campaign_rotation_revoke")
        runtime.credential = replacement
        environment = runtime.worker_environment(RIFFDB_LIFECYCLE_REVOKED_CREDENTIAL=str(runtime.root / old),
            RIFFDB_LIFECYCLE_QUIESCED_COMMANDS=str(release.snapshot["load"]["completed_commands"]))
        runtime.operator.run_worker(str(runtime.artifacts / "lifecycle"), "rotation-smoke", environment,
            elapsed_ns=runtime.elapsed(), timeout_s=min(10, runtime.remaining(deadline)))

    def deploy(self, release, deadline):
        runtime = self.runtime
        consumer = ["--operation", "TicketEvents", "--consumer-name", "ticketdesk-lifecycle-campaign",
                    "--parameter", 'organization_id={"type":"uuid","value":"01900000-0000-7000-8001-000000000000"}']
        retired = self.call("event.retire", ["event", "retire", "--module-hash", runtime.descriptor["reactive_v1"], *consumer], deadline)
        require(retired["result"] == {"result": "applied"}, "campaign_consumer_retirement")
        source = runtime.root / "successor/riffdb.application.json"
        deployed = self.call("application.deploy", ["application", "deploy", str(source), "--lock", "riffdb.application.lock.json"], deadline)
        require(deployed["result"]["contract_version"] == "2", "campaign_additive_deploy")
        credential = "application-v2.credential"
        self.call("role.bind", ["role", "bind", str(source), "--role", "TicketDeskLifecycleApplication",
            "--principal", "lifecycle-smoke-application", "--actor-kind", "service", "--audience",
            "riffdb-ticketdesk-lifecycle", "--lifetime-seconds", "100800", "--credential-output", str(runtime.root / credential)], deadline)
        registered = self.call("event.consume", ["event", "consume", "--module-hash", runtime.descriptor["reactive_v2"],
            *consumer, "--batch-limit", "1", "--in-flight-limit", "1", "--lease-seconds", "60", "--wait-nanos", "0"],
            deadline, credential=credential)
        events = registered["result"].get("events")
        require(type(events) is list and len(events) <= 1, "campaign_consumer_registration")
        for event in events:
            require(event["history_incarnation"] == str(runtime.incarnation)
                    and int(event["event_id"].split(":")[0]) <= release.observer_snapshot["consumer_checkpoint"],
                    "campaign_consumer_registered_prefix")
            nacked = self.call("event.nack", ["event", "nack", "--module-hash", runtime.descriptor["reactive_v2"], *consumer,
                "--event-id", event["event_id"], "--lease-token", event["lease_token"],
                "--history-incarnation", event["history_incarnation"], "--retry-delay-nanos", "0"], deadline, credential=credential)
            require(nacked["result"] == {"result": "applied"}, "campaign_consumer_lease_release")
        sought = self.call("event.seek", ["event", "seek", "--module-hash", runtime.descriptor["reactive_v2"], *consumer,
            "--checkpoint", str(release.observer_snapshot["consumer_checkpoint"]) + ":0"], deadline)
        require(sought["result"] == {"result": "applied"}, "campaign_consumer_seek")
        runtime.version, runtime.credential = 2, credential
        runtime.operator.run_worker(str(runtime.artifacts / "lifecycle"), "successor-authorization",
            runtime.worker_environment(), elapsed_ns=runtime.elapsed(), timeout_s=min(10, runtime.remaining(deadline)))

    def maintenance(self, kind, started, deadline):
        runtime = self.runtime
        owner = CampaignMaintenance(runtime.operator, str(runtime.root / "maintenance.credential"), kind,
                                    "campaign-source", started_ns=started, deadline_ns=deadline)
        owner.start(runtime.elapsed())
        for index in range(MAX_POLLS):
            if owner.complete: return owner
            now = runtime.elapsed()
            # Share the remaining original budget among the remaining polls
            # and one final response interval. Unknown results consume that
            # budget; they cannot create another mutation, poll or deadline.
            until = now + (deadline - now) // (MAX_POLLS - index + 1)
            self.wait(lambda: runtime.elapsed() >= until, deadline, active=False)
            owner.poll(runtime.elapsed())
            runtime.remaining(deadline)
        require(owner.complete, "campaign_maintenance_poll_bound")
        return owner

    def replace(self, kind, started, deadline, release):
        runtime = self.runtime
        if kind == "backup_restore":
            require(isinstance(self.restore_clock, RestoreRecoveryClock)
                    and not self.restore_clock.failed
                    and self.restore_clock.state == "recovering"
                    and self.restore_clock.loss_ns == started
                    and self.restore_clock.deadline_ns == deadline,
                    "campaign_restore_backup_required")
            release.pause_restore_reads(runtime.elapsed())
            self.wait(lambda: release.restore_readers_quiesced(runtime.elapsed()), deadline, active=False)
            self.restore_read_admission = release.restore_read_witness
            private_write(runtime.root, 'campaign-restore-read-admission.json',
                json.dumps(self.restore_read_admission, sort_keys=True).encode()+b'\n')
        private_write(runtime.root, 'load.recover', f'{kind} {runtime.generation}\n'.encode('ascii'))
        runtime.stop_sampler(deadline)
        runtime.stop_server(kind, deadline)
        if kind == "backup_restore":
            runtime.close_extent_incarnation()
            for name in ("data", "projections"):
                original, retained = runtime.root / name, runtime.root / ("retained-pre-restore-" + name)
                require(not retained.exists() and original.is_dir() and not original.is_symlink(), "campaign_restore_target")
                original.rename(retained); original.mkdir(mode=0o700)
            self.restore_clock.phase_completed('target_setup', runtime.elapsed())
        output = runtime.start_server()
        if kind == "backup_restore":
            runtime.wait_recovery_admission(deadline)
            self.restore_clock.phase_completed('target_admission', runtime.elapsed())
            self.maintenance("restore", started, deadline)
            self.restore_clock.phase_completed('restore', runtime.elapsed())
            runtime.incarnation += 1
            runtime.begin_extent_incarnation()
        output.wait_ready(runtime.remaining(deadline))
        if kind == 'backup_restore':
            self.restore_clock.phase_completed('readiness', runtime.elapsed())
        if kind != "backup_restore":
            release.pause_load(runtime.elapsed())
            # Original load offers continuously through the actual process fault.
            # Quiescence begins only after the replacement is ordinarily ready.
            self.wait(lambda: release.poll(runtime.elapsed()), deadline, active=False)
        runtime.start_sampler()
        runtime.epochs.transition(kind, runtime.elapsed(), version=runtime.version,
                                  incarnation=runtime.incarnation, identity=runtime.identity)
        runtime.epochs.bind_log(runtime.server_log, runtime.elapsed())
        runtime.tick(force_native=True)

    def execute(self, plan):
        runtime = self.runtime
        release = None
        if plan['kind'] == 'backup_restore':
            require(self.restore_clock is not None and self.restore_release is not None
                    and self.restore_witness is not None and self.restore_backup is not None,
                    'campaign_restore_backup_required')
            release = self.restore_release
            require(release.poll(runtime.elapsed()), 'campaign_restore_source_changed')
            witness = self.restore_witness.verify_prefix(
                release.snapshot['load']['completed_commands'], lambda: runtime.remaining(
                    self.restore_clock.planned_loss_ns + SECOND))
            manifest = self.restore_backup.verify_selection()
            # Everything from this instant, including stopping the primary,
            # belongs to the one unchanged recovery deadline.
            started = runtime.elapsed()
            deadline = self.restore_clock.primary_loss(started,
                acknowledged_commands=release.snapshot['load']['completed_commands'],
                acknowledgement_witness_sha256=witness, selected_manifest_sha256=manifest)
        else:
            started = runtime.elapsed()
            deadline = started + 60 * SECOND
        kind = plan['kind']
        self.timeline.start(kind, started)
        if kind in ("consumer_outage", "sink_outage"):
            self.outage(plan, started)
            return
        require(plan["duration_s"] == 45, "campaign_frozen_fault_protocol")
        if release is None:
            release = self.quiesce(started, deadline, pause_load=kind not in ("graceful_restart", "process_kill"))
        if kind in ("graceful_restart", "process_kill", "backup_restore"):
            self.replace(kind, started, deadline, release)
        else:
            self.replay(deadline)
            if kind == "capability_rotation": self.rotate(release, deadline)
            elif kind == "additive_deploy":
                runtime.stop_sampler(deadline)
                self.deploy(release, deadline)
                runtime.start_sampler()
                runtime.epochs.transition(kind, runtime.elapsed(), version=runtime.version,
                                          incarnation=runtime.incarnation, identity=runtime.identity)
                runtime.tick(force_native=True)
            else: require(False, "campaign_fault_kind")
        self.replay(deadline)
        if kind == 'backup_restore':
            self.restore_clock.phase_completed('original_replay', runtime.elapsed())
        self.reconcile(release, deadline)
        if kind == 'backup_restore':
            self.restore_clock.phase_completed('state_reconciliation', runtime.elapsed())
        self.release_workers(kind, started, plan["duration_s"], deadline, release, restore=kind == "backup_restore")
        if kind in ('graceful_restart', 'process_kill', 'backup_restore'):
            require((runtime.root / 'load.recover').read_bytes() == f'{kind} {runtime.generation - 1}\n'.encode('ascii'),
                    'campaign_recovery_control')
            (runtime.root / 'load.recover').unlink()
        if kind == 'backup_restore':
            self.restore_clock.phase_completed('first_write', runtime.elapsed())
            receipt = self.restore_clock.finish(runtime.elapsed())
            receipt['protocol_sha256'] = runtime.restore_protocol_sha256
            self.restore_receipt = receipt
            private_write(runtime.root, 'campaign-restore-clock.json',
                json.dumps(receipt, sort_keys=True).encode()+b'\n')
        finished = runtime.elapsed()
        require(finished <= deadline, "campaign_fault_deadline")
        self.timeline.finish(kind, finished, reconciled=True)
        self.records.append({"kind": kind, "started_ns": started, "finished_ns": finished,
                             "original_workers_retained": True, "state_and_original_replay_reconciled": True})

    def outage(self, plan, started):
        runtime = self.runtime
        kind = plan["kind"]
        release_at, deadline = started + 300 * SECOND, started + 900 * SECOND
        before = runtime.observer()
        baseline = dict(runtime.epochs.native_last)
        paused_samples, observed = 0, None
        custody = LocalArchiveOutage(runtime.root / "archive") if kind == "sink_outage" else None
        try:
            if custody:
                custody.pause()
                runtime.active_archive_outage = custody
            else: private_write(runtime.root, "consumer.pause", b"pause\n")
            def held():
                nonlocal paused_samples, observed
                observed = runtime.observer()
                if observed["elapsed_s"] > (before if paused_samples == 0 else held.last)["elapsed_s"]:
                    if custody:
                        native = runtime.epochs.native_last
                        if (native["outbox_degraded"] and native["outbox_backlog"] > 0
                                and native["index"] > held.last_native_index):
                            paused_samples += 1
                            held.last_native_index = native["index"]
                    elif observed["consumer_paused"]:
                        if paused_samples == 0:
                            held.events, held.checkpoint = observed["events_checked"], observed["consumer_checkpoint"]
                        require(observed["events_checked"] == held.events
                                and observed["consumer_checkpoint"] == held.checkpoint,
                                "campaign_consumer_pause")
                        paused_samples += 1
                    else:
                        # The file handover may precede the original observer's
                        # first acknowledged paused sample by one publication.
                        require(paused_samples == 0, "campaign_consumer_pause")
                        held.events, held.checkpoint = observed["events_checked"], observed["consumer_checkpoint"]
                    held.last = observed
                return runtime.elapsed() >= release_at
            held.last, held.events, held.checkpoint = before, before["events_checked"], before["consumer_checkpoint"]
            held.last_native_index = baseline["index"]
            self.wait(held, deadline, scheduled_at=release_at)
            require(paused_samples >= 2 and observed["live_head"] > before["live_head"]
                    and observed["live_updates"] > before["live_updates"]
                    and runtime.epochs.native_last["sampled_head"] > baseline["sampled_head"],
                    "campaign_outage_progress")
            if custody:
                custody.resume()
                runtime.active_archive_outage = None
            else: (runtime.root / "consumer.pause").unlink()
            released = runtime.elapsed()
            target_head = runtime.epochs.native_last["sampled_head"]
            offered = min(runtime.duration * 100, released // SECOND * 100)
            target_events = 1024 + offered // 65536 * 1024 + min(offered % 65536, 1024)
            self.timeline.release(kind, released)
            def recovered():
                observed = runtime.observer()
                native = runtime.epochs.native_last
                return ((not native["outbox_degraded"] and native["outbox_backlog"] == 0
                         and native["sampled_head"] >= target_head) if custody else
                        (not observed["consumer_paused"] and observed["events_checked"] >= target_events
                         and observed["events_checked"] > held.events))
            self.wait(recovered, deadline)
            finished = runtime.elapsed()
            self.timeline.finish(kind, finished, reconciled=True)
            self.records.append({"kind": kind, "started_ns": started, "released_ns": released,
                "finished_ns": finished, "paused_samples": paused_samples,
                "original_workers_retained": True, "live_and_write_progress": True})
        finally:
            primary = sys.exception()
            try:
                if custody:
                    custody.__exit__(None, None, None)
                    runtime.active_archive_outage = None
                else: (runtime.root / "consumer.pause").unlink(missing_ok=True)
            except BaseException:
                if primary is None: raise
                primary.add_note("campaign_outage_cleanup")
