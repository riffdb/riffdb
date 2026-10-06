# req: END-007, END-008, END-010
"""Read the native exact-provider pool separately from whole-process memory.

This parser grants no authority and does not establish complete campaign resource
accounting. Its source is the fixed native pool census, never VmSize or VmData.
"""
import re


PREFIX = b"[riffdbd-diag] trigger=exact_provider_lifecycle "
LIMIT_BYTES = 1024**3
FIXED_OWNER_BYTES = 512
MAX_LINE_BYTES = 2048
MAX_U64 = 2**64 - 1
FIELDS = frozenset({"limit_bytes", "fixed_owner_bytes", "owned_bytes", "reserved_bytes",
    "reserved_work", "allocations", "released_allocations", "active_owners",
    "retiring_owners", "pinned_owners", "pins", "released_owners"})


class ProviderBudgetError(ValueError):
    """Closed error: never include a diagnostic payload or machine identity."""


def require(condition, code):
    if not condition:
        raise ProviderBudgetError(code)


def parse_sample(encoded):
    require(type(encoded) is bytes, "provider_budget_shape")
    if not encoded.startswith(PREFIX):
        return None
    require(len(encoded) <= MAX_LINE_BYTES and encoded.endswith(b"\n"),
            "provider_budget_shape")
    fields = encoded[len(PREFIX):-1].split(b" ")
    require(len(fields) == len(FIELDS) + 1 and fields[0] == b"available=true",
            "provider_budget_unavailable")
    values = {}
    for field in fields[1:]:
        match = re.fullmatch(rb"([a-z_]+)=(0|[1-9][0-9]{0,19})", field)
        require(match is not None, "provider_budget_shape")
        name = match[1].decode("ascii")
        require(name in FIELDS and name not in values, "provider_budget_shape")
        value = int(match[2])
        require(value <= MAX_U64, "provider_budget_shape")
        values[name] = value
    require(values.keys() == FIELDS and values["limit_bytes"] == LIMIT_BYTES
            and values["fixed_owner_bytes"] == FIXED_OWNER_BYTES, "provider_budget_identity")
    charged = values["fixed_owner_bytes"] + values["owned_bytes"] + values["reserved_bytes"]
    require(charged <= LIMIT_BYTES, "provider_budget_limit")
    live = values["allocations"] - values["released_allocations"]
    owners = values["active_owners"] + values["retiring_owners"]
    require(live >= 0 and (live == 0) == (values["owned_bytes"] == 0)
            and (values["reserved_work"] == 0) == (values["reserved_bytes"] == 0)
            and values["reserved_work"] <= values["reserved_bytes"]
            and values["pinned_owners"] <= owners
            and values["pinned_owners"] <= values["pins"]
            and (values["pinned_owners"] == 0) == (values["pins"] == 0),
            "provider_budget_accounting")
    return dict(values, charged_bytes=charged)


class ProviderSamples:
    """One native pool incarnation, with finite output and explicit sample gaps.

    Native diagnostics are due every 60 seconds. The preparation collector
    refuses a gap over 75 seconds; it cannot replace a missed sample with zero.
    Restarted pools require a separate owner, never a silent counter reset.
    """

    def __init__(self, root, duration):
        from pathlib import Path
        require(type(duration) is int and 1 <= duration <= 86400, "provider_budget_duration")
        self.path = Path(root) / "provider-budget-samples.jsonl"
        self.duration_ns = duration * 1000000000
        self.coverage = [0] * ((duration + 299) // 300)
        self.last = None
        self.last_ns = None
        self.count = 0
        self.peak = 0
        self.output = None

    def __enter__(self):
        import os
        descriptor = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                             0o600)
        self.output = os.fdopen(descriptor, "wb")
        return self

    def observe(self, encoded, elapsed_ns):
        import json
        require(type(elapsed_ns) is int and 0 <= elapsed_ns <= MAX_U64,
                "provider_budget_clock")
        value = parse_sample(encoded)
        if value is None or elapsed_ns >= self.duration_ns:
            return
        require(self.last_ns is None or elapsed_ns > self.last_ns, "provider_budget_clock")
        require(elapsed_ns - (self.last_ns or 0) <= 75000000000, "provider_budget_sample_gap")
        if self.last is not None:
            require(all(value[name] >= self.last[name] for name in
                        ("allocations", "released_allocations", "released_owners")),
                    "provider_budget_counter_rewind")
        require(self.output is not None and self.count < 2048, "provider_budget_sample_bound")
        record = dict(value, schema="riffdb.ticketdesk-lifecycle-provider-sample/v1",
                      elapsed_ns=elapsed_ns, basis="native_exact_provider_aggregate_charge",
                      qualifies_complete_resource_model=False)
        data = json.dumps(record, sort_keys=True).encode() + b"\n"
        require(len(data) <= MAX_LINE_BYTES, "provider_budget_sample_bound")
        self.output.write(data)
        self.output.flush()
        self.last, self.last_ns = value, elapsed_ns
        self.peak = max(self.peak, value["charged_bytes"])
        self.count += 1
        self.coverage[elapsed_ns // 300000000000] += 1

    def finish(self, elapsed_ns):
        require(type(elapsed_ns) is int and self.duration_ns <= elapsed_ns <= MAX_U64,
                "provider_budget_clock")
        require(all(self.coverage), "provider_budget_sample_coverage")
        require(self.last_ns is not None and self.duration_ns - self.last_ns <= 75000000000,
                "provider_budget_sample_gap")
        return {"schema": "riffdb.ticketdesk-lifecycle-provider-observation/v1",
                "samples": self.count, "limit_bytes": LIMIT_BYTES,
                "peak_charged_bytes": self.peak,
                "basis": "native_exact_provider_aggregate_charge",
                "qualifies_complete_resource_model": False}

    def __exit__(self, _kind, _error, _traceback):
        if self.output is not None:
            self.output.close()
            self.output = None
        return False


class ProviderLog:
    """Poll only new daemon diagnostics, preserving partial-line boundaries."""

    def __init__(self, root, duration):
        from pathlib import Path
        self.path = Path(root) / "server.stderr"
        self.samples = ProviderSamples(root, duration)
        self.source = None
        self.identity = None

    def __enter__(self):
        import os
        import stat
        try:
            descriptor = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            self.source = os.fdopen(descriptor, "rb")
            info = os.fstat(self.source.fileno())
            require(stat.S_ISREG(info.st_mode), "provider_budget_log")
            self.identity = (info.st_dev, info.st_ino)
            self.source.seek(0, os.SEEK_END)
            self.samples.__enter__()
            return self
        except BaseException as error:
            if self.source is not None:
                self.source.close()
                self.source = None
            if isinstance(error, OSError):
                raise ProviderBudgetError("provider_budget_log") from error
            raise

    def poll(self, elapsed_ns):
        import stat
        require(self.source is not None, "provider_budget_log")
        try:
            info = self.path.lstat()
            require(stat.S_ISREG(info.st_mode)
                    and (info.st_dev, info.st_ino) == self.identity
                    and self.source.tell() <= info.st_size <= 16 * 1024**2, "provider_budget_log")
            for index in range(65):
                offset = self.source.tell()
                encoded = self.source.readline(4097)
                if not encoded:
                    return
                require(index < 64 and len(encoded) <= 4096, "provider_budget_log_bound")
                if not encoded.endswith(b"\n"):
                    self.source.seek(offset)
                    return
                self.samples.observe(encoded, elapsed_ns)
        except OSError as error:
            raise ProviderBudgetError("provider_budget_log") from error

    def finish(self, elapsed_ns):
        return self.samples.finish(elapsed_ns)

    def __exit__(self, kind, error, traceback):
        try:
            self.samples.__exit__(kind, error, traceback)
        finally:
            if self.source is not None:
                self.source.close()
                self.source = None
        return False
