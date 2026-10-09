# req: END-003, END-007, END-008, END-009, END-010
"""Actual finite campaign owners, original load stream and one offered clock."""
from contextlib import ExitStack
import json
import os
from pathlib import Path
import stat
import sys
import subprocess
import threading
import time
import tomllib

from .ticketdesk_lifecycle_campaign_native_epochs import CampaignNativeEpochs
from .ticketdesk_lifecycle_campaign_disk import CampaignDiskSamples
from .ticketdesk_lifecycle_campaign_extents import MeasuredExtents
from .ticketdesk_lifecycle_campaign_operator import CampaignOperator
from .ticketdesk_lifecycle_campaign_process_roster import CampaignProcessRoster
from .ticketdesk_lifecycle_load_stream import CampaignLoadStream, unique_object, parse_integer, reject_number
from .ticketdesk_lifecycle_native_observer import NativeObserverProcess
from .ticketdesk_lifecycle_persistent_usage import ObservedProcess
from .ticketdesk_lifecycle_resources import CampaignMemorySamples

SECOND = 10**9
DISK_LIMITS = dict(database_bytes=16*1024**3, journal_bytes=16*1024**3,
    sink_archive_bytes=8*1024**3, backup_bytes=32*1024**3, derived_bytes=8*1024**3,
    control_and_evidence_bytes=1024**3)
LIMITS = dict(controller=8*1024**3, server=8*1024**3,
              load=1024**3, observer=1024**3, native_sampler=1024**3, operator=1024**3)


class CampaignError(ValueError):
    """Closed refusal; native payloads and host details stay in private files."""


def require(condition, code):
    if not condition: raise CampaignError(code)


def private_write(root, name, encoded):
    require(type(encoded) is bytes and len(encoded) <= 1048576, "campaign_control_bound")
    fd = os.open(Path(root)/name, os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as output:
        output.write(encoded); output.flush(); os.fsync(output.fileno())


class WorkerOutput:
    def __init__(self, child, root, kind, duration=28800):
        self.child, self.kind = child, kind
        self.ready = threading.Event()
        self.ready_seen = False
        self.latest = self.terminal = self.failure = None
        self.windows = []
        self.prior_observation = None
        self.parser = CampaignLoadStream(duration_s=duration) if kind == "load" else None
        self.path = Path(root)/(kind+".stdout")
        self.thread = threading.Thread(target=self._read, daemon=True)
        self.thread.start()

    def observation(self, value):
        integer_fields = {"elapsed_s", "events_checked", "duplicates", "consumer_checkpoint",
                          "live_head", "live_updates", "history_incarnation", "live_renewals",
                          "release_generation", "auxiliary_slots"}
        boolean_fields = {"consumer_paused", "observer_quiesced"}
        require(type(value) is dict and value.keys() == integer_fields | boolean_fields | {"schema"}
                and value["schema"] == "riffdb.ticketdesk-lifecycle-observation/v1"
                and all(type(value[key]) is int and 0 <= value[key] < 2**64 for key in integer_fields)
                and all(type(value[key]) is bool for key in boolean_fields)
                and value["duplicates"] == 0 and value["auxiliary_slots"] == 2
                and value["history_incarnation"] in (1, 2)
                and 1 <= value["release_generation"] <= 16
                and value["events_checked"] <= 8640000 + 65536,
                "campaign_worker_output")
        if self.prior_observation is not None:
            prior = self.prior_observation
            require(value["elapsed_s"] > prior["elapsed_s"]
                    and all(value[key] >= prior[key] for key in integer_fields - {"elapsed_s"}),
                    "campaign_observer_rewind")
        self.prior_observation = dict(value)
        return value

    def _read(self):
        try:
            with self.path.open("xb") as output:
                total = 0
                class Tee:
                    def readline(_self, maximum):
                        encoded = self.child.stdout.readline(maximum)
                        if encoded:
                            output.write(encoded); output.flush()
                        return encoded
                tee = Tee()
                for index in range(88203):
                    if self.kind == "load":
                        value = self.parser.read(tee)
                        if value is None: return
                        encoded = b""
                    else:
                        encoded = self.child.stdout.readline(4097)
                        if not encoded: return
                        require(len(encoded) <= 4096 and encoded.endswith(b"\n"), "campaign_worker_output")
                        if self.kind == "server":
                            value = None
                        else:
                            value = json.loads(encoded, object_pairs_hook=unique_object,
                                               parse_int=parse_integer, parse_float=reject_number,
                                               parse_constant=reject_number)
                    total += len(encoded)
                    require(total <= 64*1024**2, "campaign_worker_output")
                    output.write(encoded); output.flush()
                    if self.kind == "server":
                        if encoded.startswith(b"riffdbd-ready-v1"):
                            self.ready_seen = True; self.ready.set()
                    elif self.kind == "load":
                        schema = value["schema"]
                        if schema == "riffdb.ticketdesk-lifecycle-load-ready/v1":
                            self.ready_seen = True; self.ready.set()
                        elif schema == "riffdb.ticketdesk-lifecycle-load-probe/v1": self.windows.append(value)
                        else: self.terminal = value
                    else:
                        require(type(value) is dict, "campaign_worker_output")
                        schema = value.get("schema")
                        if schema == "riffdb.ticketdesk-lifecycle-observer-ready/v1":
                            require(value == {"schema":schema,"auxiliary_slots":2}, "campaign_worker_output")
                            self.ready_seen = True; self.ready.set()
                        else:
                            self.latest = (time.monotonic_ns(), self.observation(value))
                raise CampaignError("campaign_worker_output")
        except BaseException as error:
            self.failure = error
        finally:
            self.ready.set()

    def wait_ready(self, timeout):
        require(timeout > 0 and self.ready.wait(timeout) and self.ready_seen and self.failure is None
                and self.child.poll() is None, "campaign_worker_ready")

    def join(self, timeout=5):
        self.thread.join(timeout)
        require(not self.thread.is_alive() and self.failure is None, "campaign_worker_shutdown")


class CampaignRuntime:
    complete_campaign_checks_proved = False

    def __init__(self, root, artifacts, descriptor, *, duration_s, faults):
        self.root, self.artifacts = Path(root), Path(artifacts)
        self.duration = duration_s
        self.identity = {"projection_id":descriptor["projection_id"],
                         "projection_plan_hash":descriptor["projection_plan_hash"]}
        self.descriptor = descriptor
        self.faults = faults
        self.version = self.incarnation = self.generation = 1
        self.credential = "application.credential"
        self.children, self.outputs = {}, {}
        self.roster = self.native = self.epochs = self.memory = self.operator = None
        self.disk, self.active_archive_outage = None, None
        self.origin = None
        self.server_number = self.sampler_number = 0
        self.server_log = None
        self.extent_incarnations = []
        self.current_extents = None
        self.server_logs = []
        self.environment = dict(os.environ, RIFFDB_LIFECYCLE_CA=str(self.root/"ca.pem"),
            RIFFDB_LIFECYCLE_APPLICATION_CREDENTIAL=str(self.root/self.credential),
            RIFFDB_LIFECYCLE_OPERATOR_CREDENTIAL=str(self.root/"operator.credential"),
            RIFFDB_LIFECYCLE_SEED_CREDENTIAL=str(self.root/"seed.credential"),
            RIFFDB_LIFECYCLE_WITNESS_ROOT=str(self.root),
            RIFFDB_LIFECYCLE_LOAD_CONTROL=str(self.root),
            RIFFDB_LIFECYCLE_OBSERVER_STOP=str(self.root/"observer.stop"),
            RIFFDB_LIFECYCLE_DURATION_SECONDS=str(duration_s),
            RIFFDB_LIFECYCLE_DEFER_LIVE="1", RIFFDB_LIFECYCLE_TERMINAL_BARRIER="1")
        with (self.root/"client.toml").open("rb") as source: configuration = tomllib.load(source)["client"]
        self.environment["RIFFDB_LIFECYCLE_ENDPOINT"] = configuration["endpoint"]
        from urllib.parse import urlparse
        endpoint = urlparse(configuration["endpoint"])
        require(endpoint.scheme == "https" and endpoint.hostname == "127.0.0.1"
                and endpoint.port is not None, "campaign_listener")
        self.port = endpoint.port
        self.base = [str(self.artifacts/"riffdb"), "--config", str(self.root/"client.toml"),
                     "--credential-file", str(self.root/"operator.credential")]
        self.stack = ExitStack()

    def elapsed(self):
        require(self.origin is not None, "campaign_clock")
        return time.monotonic_ns()-self.origin

    def remaining(self, deadline_ns):
        remaining = (deadline_ns-self.elapsed())/SECOND
        require(remaining > 0, "campaign_fault_deadline")
        return remaining

    def worker_environment(self, **changes):
        return dict(self.environment,
            RIFFDB_LIFECYCLE_CONTRACT_VERSION=str(self.version),
            RIFFDB_LIFECYCLE_APPLICATION_CREDENTIAL=str(self.root/self.credential), **changes)

    def launch(self, role, arguments, directory, *, environment=None):
        directory.mkdir(mode=0o700, exist_ok=True)
        with (directory/(role+".stderr")).open("xb") as errors:
            options = dict(env=environment, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=errors,
                           observation=directory/(role+".process-usage.json"))
            child = (self.roster.launch(role, arguments, **options) if self.roster else
                     ObservedProcess(arguments, address_space_limit_bytes=LIMITS[role], **options))
        self.children[role] = child
        return child

    def start_server(self):
        directory = self.root/("server-"+str(self.server_number).zfill(3)); self.server_number += 1
        child = self.launch("server", [str(self.artifacts/"riffdbd"), "--config", str(self.root/"server.toml")], directory)
        output = WorkerOutput(child, directory, "server")
        self.outputs["server"] = output
        self.server_log = directory/"server.stderr"
        self.server_logs.append(dict(path=self.server_log, incarnation=self.incarnation, killed=False))
        return output

    def wait_recovery_admission(self, deadline_ns):
        process = self.children["server"]
        output = self.outputs["server"]
        descriptor = os.open(self.server_log, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            opened = os.fstat(descriptor)
            identity = (opened.st_dev, opened.st_ino)
            redirected = os.stat("/proc/"+str(process.pid)+"/fd/2")
            require(stat.S_ISREG(opened.st_mode) and opened.st_nlink == 1
                    and opened.st_uid == os.geteuid()
                    and identity == (redirected.st_dev, redirected.st_ino),
                    "campaign_recovery_log_owner")
            pending = b""
            consumed = 0
            prior_size = 0
            while True:
                require(process.poll() is None and not output.ready_seen,
                        "campaign_recovery_admission")
                self.remaining(deadline_ns)
                named = self.server_log.lstat()
                owned = os.fstat(descriptor)
                require((named.st_dev, named.st_ino) == identity
                        and (owned.st_dev, owned.st_ino) == identity
                        and owned.st_size >= prior_size and owned.st_size <= 65536,
                        "campaign_recovery_log_owner")
                redirected = os.stat("/proc/"+str(process.pid)+"/fd/2")
                require(identity == (redirected.st_dev, redirected.st_ino),
                        "campaign_recovery_log_owner")
                prior_size = owned.st_size
                encoded = os.read(descriptor, min(4096, 65537-consumed))
                if encoded:
                    consumed += len(encoded)
                    require(consumed <= 65536, "campaign_recovery_log_bound")
                    pending += encoded
                    while b"\n" in pending:
                        line, pending = pending.split(b"\n", 1)
                        require(len(line) <= 4096, "campaign_recovery_log_bound")
                        if line == b"riffdb-recovery-admission-v1\tready=1":
                            require(process.poll() is None and not output.ready_seen,
                                    "campaign_recovery_admission")
                            self.remaining(deadline_ns)
                            return
                    require(len(pending) <= 4096, "campaign_recovery_log_bound")
                self.memory.poll(self.elapsed())
                self.disk.poll(self.elapsed(), archive_outage=self.active_archive_outage)
                threading.Event().wait(min(.02,self.remaining(deadline_ns)))
        finally:
            os.close(descriptor)

    def start_observer(self):
        directory = self.root/"original-observer"
        child = self.launch("observer", [str(self.artifacts/"lifecycle"),"observe"], directory,
                            environment=self.worker_environment())
        output = WorkerOutput(child,directory,"observer"); self.outputs["observer"] = output
        output.wait_ready(30)

    def start_sampler(self):
        directory = self.root/("sampler-"+str(self.sampler_number).zfill(3)); self.sampler_number += 1
        directory.mkdir(mode=0o700)
        self.native = NativeObserverProcess(directory, [str(self.artifacts/"lifecycle"),"native-observer"],
            self.worker_environment(), self.identity, roster=self.roster,
            **({"address_space_limit_bytes":LIMITS["native_sampler"]} if self.roster is None else {}))
        self.children["native_sampler"] = self.native.process

    def prelude_call(self, label, arguments, *, environment=None, timeout_s=30):
        require(self.origin is None and self.roster is None and "operator" not in self.children,
                "campaign_prelude_state")
        directory = self.root / label
        child = self.launch("operator", arguments, directory, environment=environment)
        failures, readers = [], []
        with (directory / "operator.stdout").open("xb") as output:
            reader = threading.Thread(target=CampaignOperator._drain,
                args=(child.stdout, output, 4096, failures), daemon=True)
            readers.append(reader); reader.start()
            started = time.monotonic()
            try:
                while child.poll() is None:
                    require(not failures and time.monotonic() - started < timeout_s
                            and self.children["server"].poll() is None
                            and self.children["observer"].poll() is None,
                            "campaign_prelude_owner")
                    try: child.wait(timeout=min(.05, timeout_s - (time.monotonic() - started)))
                    except subprocess.TimeoutExpired: pass
                require(child.returncode == 0 and time.monotonic() - started < timeout_s,
                        "campaign_prelude_result")
                reader.join(1)
                require(not reader.is_alive() and not failures, "campaign_prelude_output")
            finally:
                primary = sys.exception()
                try:
                    if child.poll() is None: child.kill(); child.wait(timeout=5)
                    reader.join(1)
                    for stream in (child.stdin, child.stdout): stream.close()
                except BaseException:
                    if primary is None: raise
                    primary.add_note("campaign_prelude_cleanup")
        del self.children["operator"]
        value = json.loads((directory / "operator.stdout").read_bytes(), object_pairs_hook=unique_object,
                           parse_int=parse_integer, parse_float=reject_number, parse_constant=reject_number)
        require(type(value) is dict, "campaign_prelude_output")
        return value

    def prepare_population(self):
        self.start_server().wait_ready(30)
        self.start_observer()
        seeded = self.prelude_call("initial-seed", [str(self.artifacts / "lifecycle"), "seed"],
                                  environment=self.worker_environment(), timeout_s=1200)
        require(seeded == {"schema": "riffdb.ticketdesk-lifecycle-seed/v1", "organizations": 64,
                           "tickets": 65536, "max_in_flight": 16}, "campaign_seed")
        # The observer owns organization zero's consumer from before seeding.
        # This read-only readiness wait does not submit another seed mutation.
        started = time.monotonic()
        for index in range(901):
            require(time.monotonic() - started < 900, "campaign_seed_drain")
            health = self.prelude_call("seed-health-" + str(index).zfill(3), self.base + ["server", "health"])
            require(health.get("schema") == "riffdb.cli.output/v1" and health.get("command") == "server.health"
                    and health.get("ok") is True and type(health.get("result")) is dict,
                    "campaign_seed_drain")
            components = health["result"].get("components")
            require(type(components) is list and 0 < len(components) <= 16
                    and all(type(component) is dict for component in components), "campaign_seed_drain")
            outbox = [component for component in components if component.get("component") == "outbox"]
            require(len(outbox) == 1 and type(outbox[0].get("status")) is str
                    and outbox[0]["status"] in {"healthy", "degraded", "unavailable"}, "campaign_seed_drain")
            observed = self.observer()
            require(observed["events_checked"] <= 1024 and observed["release_generation"] == 1
                    and observed["history_incarnation"] == 1, "campaign_seed_observer")
            if outbox[0]["status"] == "healthy" and observed["events_checked"] == 1024: break
            threading.Event().wait(min(1, max(0, 900 - (time.monotonic() - started))))
        else: raise CampaignError("campaign_seed_drain")
        self.start_sampler()
        self.start_load()

    def start_load(self):
        private_write(self.root,"load.phase",b"1 1 1 application.credential\n")
        private_write(self.root,"live.start",b"start\n")
        directory = self.root/"original-load"
        child = self.launch("load", [str(self.artifacts/"lifecycle"),"probe"],directory,
                            environment=self.worker_environment())
        output = WorkerOutput(child,directory,"load",self.duration); self.outputs["load"] = output
        output.wait_ready(30)
        self.origin = time.monotonic_ns()
        self.roster = CampaignProcessRoster(os.getpid(),dict(self.children),LIMITS)
        self.epochs = self.stack.enter_context(CampaignNativeEpochs(self.root,self.roster,self.duration,self.identity))
        self.memory = self.stack.enter_context(CampaignMemorySamples.from_roster(self.root,self.roster,self.duration))
        self.operator = self.stack.enter_context(CampaignOperator(self.root,self.roster,self.base))
        self.disk = self.stack.enter_context(CampaignDiskSamples(self.root,self.duration,DISK_LIMITS))
        self.epochs.bind_log(self.server_log,self.elapsed())
        self.begin_extent_incarnation()
        self.tick(force_native=True)

    def observer(self):
        owner = self.outputs["observer"]
        require(owner.failure is None and owner.latest is not None
                and time.monotonic_ns()-owner.latest[0] <= 5*SECOND, "campaign_observer_gap")
        return dict(owner.latest[1])

    def tick(self, *, force_native=False, native=True, logs=True):
        require(self.outputs["load"].failure is None and self.outputs["observer"].failure is None,
                "campaign_worker_output")
        elapsed = self.elapsed()
        if self.current_extents is not None: self.current_extents.poll(elapsed)
        self.memory.poll(elapsed)
        self.disk.poll(self.elapsed(), archive_outage=self.active_archive_outage)
        if logs: self.epochs.poll_log(self.elapsed())
        self.epochs.check_gaps(self.elapsed())
        if native and self.elapsed() < self.duration*SECOND:
            elapsed = self.elapsed()
            due = force_native or self.epochs.native_last_ns is None or elapsed-self.epochs.native_last_ns >= 30*SECOND
            near_fault = any(elapsed < f["start_s"]*SECOND <= elapsed+7*SECOND for f in self.faults)
            if due and (force_native or not near_fault): self.epochs.sample_native(self.native,elapsed)
        require(self.children["load"].poll() is None and self.children["observer"].poll() is None,
                "campaign_original_worker_exit")
        return self.elapsed()

    def stop_sampler(self, deadline_ns):
        process = self.native.process
        self.remaining(deadline_ns)
        self.native.close()
        require(self.elapsed() < deadline_ns, "campaign_fault_deadline")
        self.roster.retire("native_sampler",process,expected_exit=0)
        del self.children["native_sampler"]
        self.native = None

    def stop_server(self, kind, deadline_ns):
        process = self.children["server"]
        self.server_logs[-1]["killed"] = kind == "process_kill"
        process.kill() if kind == "process_kill" else process.terminate()
        expected = -9 if kind == "process_kill" else 0
        require(process.wait(timeout=min(30,self.remaining(deadline_ns))) == expected, "campaign_server_exit")
        self.outputs["server"].join(min(5,self.remaining(deadline_ns)))
        self.roster.retire("server",process,expected_exit=expected)
        del self.children["server"]
        while not self.epochs.poll_log(self.elapsed(),retired_kind=kind):
            self.remaining(deadline_ns); threading.Event().wait(.001)

    def begin_extent_incarnation(self):
        require(self.current_extents is None and len(self.extent_incarnations) < 2
                and self.incarnation == len(self.extent_incarnations) + 1,
                "campaign_extent_identity")
        tracker = MeasuredExtents(self.root/"data",duration_s=self.duration,incarnation=self.incarnation)
        tracker.poll(self.elapsed(),force=True)
        self.extent_incarnations.append(tracker)
        self.current_extents = tracker
        self.server_logs[-1]["incarnation"] = self.incarnation

    def close_extent_incarnation(self):
        require(self.current_extents is not None, "campaign_extent_identity")
        self.current_extents.close(self.elapsed())
        self.current_extents = None

    def reconcile_extent_incarnations(self):
        require(self.current_extents is None and len(self.extent_incarnations) == 2
                and not self.roster.children, "campaign_extent_logs")
        return [tracker.reconcile([(row["path"],row["killed"]) for row in self.server_logs
                if row["incarnation"] == tracker.incarnation])
                for tracker in self.extent_incarnations]

    def close(self):
        primary, failures = sys.exception(), []
        def cleanup(action):
            try: action()
            except BaseException: failures.append("campaign_cleanup")
        cleanup(self.stack.close)
        if self.native is not None: cleanup(lambda: self.native.close(failed=True))
        for child in list(self.children.values()):
            if child.poll() is None: cleanup(child.terminate)
        for child in list(self.children.values()):
            try: child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                cleanup(child.kill); cleanup(lambda: child.wait(timeout=5))
            except BaseException: failures.append("campaign_cleanup")
            for stream in (child.stdin, child.stdout):
                if stream is not None: cleanup(stream.close)
        for output in self.outputs.values():
            output.thread.join(5)
            if output.thread.is_alive(): failures.append("campaign_cleanup")
        if failures:
            if primary is not None: primary.add_note("campaign_cleanup")
            else: raise CampaignError("campaign_cleanup")
