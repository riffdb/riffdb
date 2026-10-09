# req: END-003, END-007, END-008, END-010
"""Bind native counter epochs to exact retired and replacement process owners.

One campaign clock, coverage array and output bound survives every replacement.
This component supplies no fault execution or complete resource qualification.
"""
from contextlib import ExitStack
from functools import wraps
import json
import os
from pathlib import Path

from .ticketdesk_lifecycle_campaign_process_roster import CampaignProcessRoster, MAX_INCARNATIONS
from .ticketdesk_lifecycle_native_observer import NativeSamples, MAX_SAMPLES
from .ticketdesk_lifecycle_provider_budget import ProviderSamples, parse_sample as provider_sample
from .ticketdesk_lifecycle_source_bytes import SourceBytesSamples, parse_sample_v2, PREFIX_V2
from .ticketdesk_lifecycle_logical_bytes import LogicalBytesSamples, parse_sample as logical_sample
from .ticketdesk_lifecycle_physical_bytes import PhysicalBytesSamples, parse_sample as physical_sample

MAX_U64 = 2**64 - 1
GAPS = dict(provider=75000000000, source=95000000000,
            logical=95000000000, physical=95000000000)
PARSERS = dict(provider=provider_sample, source=parse_sample_v2,
               logical=logical_sample, physical=physical_sample)
CURSORS = dict(source=("resume",), logical=("watermark", "head"),
               physical=("watermark", "minimum_resume", "active_generation"))
KINDS = frozenset({"additive_deploy", "graceful_restart", "process_kill", "backup_restore"})


class EpochError(ValueError):
    """Closed refusal without child output, process identities or local paths."""


def require(condition, code):
    if not condition:
        raise EpochError(code)


def fenced(function):
    @wraps(function)
    def checked(self, *args, **kwargs):
        require(not self.failed, "campaign_epoch_failed")
        try:
            return function(self, *args, **kwargs)
        except OSError as error:
            self.failed = True
            raise EpochError("campaign_epoch_io") from error
        except BaseException:
            self.failed = True
            raise
    return checked


class CampaignNativeEpochs:
    complete_campaign_checks_proved = False

    def __init__(self, root, roster, duration, identity):
        require(isinstance(roster, CampaignProcessRoster), "campaign_epoch_roster")
        require(type(duration) is int and duration % 300 == 0
                and 73 <= duration // 300 <= 288, "campaign_epoch_duration")
        self.roster = roster
        self.root = Path(root)
        self.duration_ns = duration * 10**9
        self.version = self.incarnation = 1
        self.native = NativeSamples(identity)
        self.samples = dict(provider=ProviderSamples(root, duration),
            source=SourceBytesSamples(root, duration, lambda: self.incarnation),
            logical=LogicalBytesSamples(root, duration, lambda: self.incarnation),
            physical=PhysicalBytesSamples(root, duration, lambda: self.incarnation))
        self.native_count = self.native_transport_attempts = 0
        self.native_last = self.native_last_ns = None
        self.last_ns = None
        self.failed = self.complete = False
        self.stack = self.output = self.native_output = None
        self.epoch = 0
        self.floors = {}
        self.server = roster.children["server"]
        self.sampler = roster.children["native_sampler"]
        self.workers = {role: roster.children[role] for role in ("load", "observer")}
        self.owner_counts = {role: roster.roles[role]["incarnations"]
                             for role in ("server", "native_sampler")}
        self.started_counts = {name: 0 for name in self.samples}
        self.started_native = 0

    def _clock(self, elapsed_ns):
        require(type(elapsed_ns) is int and 0 <= elapsed_ns <= MAX_U64
                and (self.last_ns is None or elapsed_ns >= self.last_ns)
                and not self.complete, "campaign_epoch_clock")
        self.last_ns = elapsed_ns

    def _owners_current(self):
        self.roster._check()
        require(self.roster.children.get("server") is self.server
                and self.roster.children.get("native_sampler") is self.sampler
                and all(self.roster.children.get(role) is child for role, child in self.workers.items())
                and all(child.poll() is None for child in
                        (self.server, self.sampler, *self.workers.values())),
                "campaign_epoch_owner")

    def _write(self, stream, record):
        require(stream is not None, "campaign_epoch_output")
        encoded = json.dumps(record, sort_keys=True).encode() + b"\n"
        require(len(encoded) <= 4096, "campaign_epoch_bound")
        stream.write(encoded)
        stream.flush()

    def _record(self, kind, elapsed_ns):
        self._write(self.output, dict(schema="riffdb.ticketdesk-lifecycle-native-epoch/v1",
            epoch=self.epoch, kind=kind, elapsed_ns=elapsed_ns,
            server_incarnation=self.owner_counts["server"],
            sampler_incarnation=self.owner_counts["native_sampler"],
            contract_version=self.version, history_incarnation=self.incarnation,
            samples={name: sample.count for name, sample in self.samples.items()},
            native_samples=self.native_count, native_transport_attempts=self.native_transport_attempts,
            provider_peak_charged_bytes=self.samples["provider"].peak,
            qualifies_complete_resource_model=False))

    @fenced
    def __enter__(self):
        require(self.stack is None, "campaign_epoch_output")
        self.stack = ExitStack()
        try:
            for sample in self.samples.values(): self.stack.enter_context(sample)
            for name, attribute in (("native-observation-epochs.jsonl", "output"),
                                    ("campaign-native-observations.jsonl", "native_output")):
                descriptor = os.open(self.root / name,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
                setattr(self, attribute, self.stack.enter_context(os.fdopen(descriptor, "wb")))
            self.roster.sample(0)
            self._owners_current()
            self._record("initial", 0)
            return self
        except BaseException:
            self.stack.close()
            raise

    def _retired(self, role, child, code):
        require(child is not self.roster.children.get(role) and child.poll() == code,
                "campaign_epoch_exit")
        value, _ = self.roster._proof(role, child)
        require(value.get("exit_observed") is True and value.get("exit_code") == code,
                "campaign_epoch_exit")

    @fenced
    def transition(self, kind, elapsed_ns, *, version, incarnation, identity):
        self._clock(elapsed_ns)
        require(type(kind) is str and kind in KINDS and elapsed_ns < self.duration_ns,
                "campaign_epoch_kind")
        require(self.output is not None and self.epoch + 1 < MAX_INCARNATIONS,
                "campaign_epoch_bound")
        require(all(sample.count > self.started_counts[name] for name, sample in self.samples.items())
                and self.native_count > self.started_native, "campaign_epoch_incomplete")
        deploy, restore = kind == "additive_deploy", kind == "backup_restore"
        require(type(version) is int and version == (2 if deploy else self.version)
                and (not deploy or self.version == 1)
                and type(incarnation) is int and incarnation == self.incarnation + int(restore),
                "campaign_epoch_release")
        require(deploy or identity == self.native.identity, "campaign_epoch_identity")
        same_projection = identity == self.native.identity
        next_native = NativeSamples(identity)
        # The roster's census proves the new owners' live identity, ancestry,
        # finite launch limits and the exact absence of untracked descendants.
        self.roster.sample(elapsed_ns)
        require(all(self.roster.children.get(role) is child for role, child in self.workers.items()),
                "campaign_epoch_owner")
        next_counts = {role: self.roster.roles[role]["incarnations"] for role in self.owner_counts}
        require(next_counts["native_sampler"] == self.owner_counts["native_sampler"] + 1
                and next_counts["server"] == self.owner_counts["server"] + int(not deploy),
                "campaign_epoch_owner")
        self._retired("native_sampler", self.sampler, 0)
        if deploy:
            require(self.roster.children.get("server") is self.server, "campaign_epoch_owner")
        else:
            self._retired("server", self.server, -9 if kind == "process_kill" else 0)
            # Only process-local roots/counters reset. Keep durable frontier
            # floors through ordinary restart, and the logical head on restore.
            self.floors = {name: {key: sample.last[key] for key in
                (("head",) if restore and name == "logical" else
                 (() if restore else CURSORS.get(name, ())))}
                for name, sample in self.samples.items()}
            for sample in self.samples.values(): sample.last = None
        self.version, self.incarnation = version, incarnation
        self.native = next_native
        keys = ["sampled_head", "statistics_head"]
        if not restore and same_projection:
            keys.extend(("projection_generation", "applied_through"))
        self.native_floor = {key: self.native_last[key] for key in keys}
        self.server, self.sampler = self.roster.children["server"], self.roster.children["native_sampler"]
        self.owner_counts = next_counts
        self.epoch += 1
        self.started_counts = {name: sample.count for name, sample in self.samples.items()}
        self.started_native = self.native_count
        self._record(kind, elapsed_ns)

    @fenced
    def observe(self, name, encoded, elapsed_ns):
        self._clock(elapsed_ns)
        self._owners_current()
        require(type(name) is str and name in self.samples, "campaign_epoch_component")
        if name == "source":
            require(type(encoded) is bytes and encoded.startswith(PREFIX_V2),
                    "campaign_epoch_source_version")
        value = PARSERS[name](encoded)
        if value is None or elapsed_ns >= self.duration_ns: return
        require(all(value[key] >= floor for key, floor in self.floors.get(name, {}).items()),
                "campaign_epoch_frontier")
        self.samples[name].observe(encoded, elapsed_ns)
        self.floors.pop(name, None)

    @fenced
    def observe_native(self, value, elapsed_ns):
        self._clock(elapsed_ns)
        self._owners_current()
        require(elapsed_ns < self.duration_ns and self.native_count < MAX_SAMPLES
                and (self.native_last_ns is None or elapsed_ns > self.native_last_ns),
                "campaign_epoch_bound")
        # Validate the closed native schema before inspecting cross-owner fields.
        checked = self.native.observe(value, version=self.version)
        require(checked["history_incarnation"] == self.incarnation, "campaign_epoch_release")
        floor = getattr(self, "native_floor", {})
        keys = ("sampled_head", "statistics_head", "projection_generation", "applied_through")
        require(all(checked[key] >= floor[key] for key in keys if key in floor),
                "campaign_epoch_frontier")
        self._write(self.native_output, dict(checked, elapsed_ns=elapsed_ns))
        self.native_count += 1
        self.native_transport_attempts += checked["transport_attempts"]
        self.native_last, self.native_last_ns = checked, elapsed_ns
        self.native_floor = {}

    @fenced
    def check_gaps(self, elapsed_ns):
        self._clock(elapsed_ns)
        through = min(elapsed_ns, self.duration_ns)
        for name, sample in self.samples.items():
            code = "provider_budget_sample_gap" if name == "provider" else name + "_bytes_sample_gap"
            require(0 <= through - (sample.last_ns or 0) <= GAPS[name], code)

    @fenced
    def finish(self, elapsed_ns):
        self._clock(elapsed_ns)
        self._owners_current()
        require(self.native_count > self.started_native, "campaign_epoch_incomplete")
        result = {name: sample.finish(elapsed_ns) for name, sample in self.samples.items()}
        self.complete = True
        return dict(components=result, epochs=self.epoch + 1,
                    native_samples=self.native_count, native_transport_attempts=self.native_transport_attempts,
                    qualifies_complete_resource_model=False)

    def __exit__(self, kind, error, traceback):
        if self.stack is not None: self.stack.__exit__(kind, error, traceback)
        self.output = self.native_output = None
        return False
