# req: END-003, END-007, END-008
"""Read-only evidence of sealed files appearing and disappearing in one volume."""
import json
import os
from pathlib import Path
import re
import stat
import threading

MAX_LOG_BYTES = 16 * 1024 * 1024
MAX_LOG_LINE_BYTES = 4096
MAX_LOG_LINES = 262144
MAX_RETIREMENTS = 4096
RETIREMENT_FAMILY = b"riffdb-retention-extent-retired-"
RETIREMENT_PREFIX = RETIREMENT_FAMILY + b"v1\t"


class InventoryError(ValueError):
    """Closed diagnostics never include a path or directory entry."""


def require(condition, code):
    if not condition:
        raise InventoryError(code)


def parse_retirement(encoded):
    require(type(encoded) is bytes, "inventory_retirement")
    if not encoded.startswith(RETIREMENT_FAMILY):
        return None
    require(encoded.startswith(RETIREMENT_PREFIX) and len(encoded) <= 256
            and encoded.endswith(b"\n"), "inventory_retirement")
    fields = {}
    for item in encoded[len(RETIREMENT_PREFIX):-1].split(b"\t"):
        match = re.fullmatch(rb"([a-z_]+)=(0|[1-9][0-9]{0,19})", item)
        require(match is not None, "inventory_retirement")
        name, value = match[1].decode("ascii"), int(match[2])
        require(name not in fields and value <= 2**64 - 1, "inventory_retirement")
        fields[name] = value
    require(fields.keys() == {"root", "generation", "deleted", "recycled"}
            and fields["root"] > 0 and fields["root"] % 2 == 0
            and fields["generation"] > 0
            and fields["deleted"] + fields["recycled"] == 1, "inventory_retirement")
    return fields


def shutdown_diagnostics(path, *, with_retirements=False):
    """Stream a closed log, retaining only the two native shutdown record kinds.

    Three copies are enough to reject duplication; further copies retain no
    additional memory. Neither log length nor an aggregate deletion count proves
    that this observer saw any particular reclaimed generation.
    """
    found, terminal, retirements = [], [], []
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(descriptor, "rb") as source:
            before = os.fstat(source.fileno())
            require(stat.S_ISREG(before.st_mode) and before.st_size <= MAX_LOG_BYTES,
                    "inventory_census_bound")
            total, count = 0, 0
            while line := source.readline(MAX_LOG_LINE_BYTES + 1):
                total += len(line)
                count += 1
                require(total <= MAX_LOG_BYTES and count <= MAX_LOG_LINES
                        and len(line) <= MAX_LOG_LINE_BYTES and line.endswith(b"\n"),
                        "inventory_census_bound")
                retired = parse_retirement(line)
                if retired is not None:
                    require(len(retirements) < MAX_RETIREMENTS, "inventory_retirement_bound")
                    retirements.append(retired)
                if line.startswith(b"riffdb-retention-extents-v1\t") and len(found) < 3:
                    found.append(line.rstrip(b"\n"))
                if line.startswith(b"riffdb-shutdown-release-v1\t") and len(terminal) < 3:
                    terminal.append(line.rstrip(b"\n"))
            after = os.fstat(source.fileno())
            named = path.lstat()
        identity = lambda info: (info.st_dev, info.st_ino, info.st_size,
                                 info.st_mtime_ns, info.st_ctime_ns)
        require(total == before.st_size and stat.S_ISREG(named.st_mode)
                and identity(before) == identity(after) == identity(named),
                "inventory_census_changed")
        return (found, terminal, retirements) if with_retirements else (found, terminal)
    except OSError as error:
        raise InventoryError("inventory_census_unavailable") from error


class ExtentInventory:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.seen = {}
        self.present = set()
        self.removed = set()
        self.samples = 0

    def sample(self):
        require(self.directory.is_dir() and not self.directory.is_symlink(), "inventory_directory")
        present = set()
        seen = dict(self.seen)
        for index, entry in enumerate(self.directory.iterdir()):
            require(index < 8192, "inventory_entry_bound")
            match = re.fullmatch(r"ticketdesk\.redb\.riffjournal\.sealed\.([0-9]{20})", entry.name)
            if match is None:
                continue
            generation = int(match[1])
            require(0 < generation <= 2**64 - 1, "inventory_generation")
            try:
                info = entry.lstat()
            except FileNotFoundError:
                continue
            require(stat.S_ISREG(info.st_mode), "inventory_file")
            identity = (info.st_dev, info.st_ino, info.st_size)
            require(generation not in self.removed, "inventory_generation_reappeared")
            require(generation not in seen or seen[generation] == identity,
                    "inventory_identity_changed")
            require(len(seen) < 4096 or generation in seen, "inventory_generation_bound")
            seen[generation] = identity
            present.add(generation)
        self.removed.update(self.present - present)
        self.present = present
        self.seen = seen
        self.samples += 1

    def observation(self, census):
        fields = {"deleted", "recycled", "held_references", "held_readers", "reclaim_us"}
        require(type(census) is dict and census.keys() == fields
                and all(type(value) is int and 0 <= value <= 2**64 - 1 for value in census.values()),
                "inventory_census")
        require(census["deleted"] + census["recycled"] >= len(self.removed),
                "inventory_unexplained_disappearance")
        return {"schema": "riffdb.ticketdesk-lifecycle-extent-preparation/v1",
                "samples": self.samples, "observed_generations": sorted(self.seen),
                "physically_removed_generations": sorted(self.removed), "census": census}


class InventoryMonitor:
    def __init__(self, directory):
        self.inventory = ExtentInventory(directory)
        self.stopped = threading.Event()
        self.failure = None
        self.started = False
        self.thread = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        try:
            while True:
                self.inventory.sample()
                if self.stopped.wait(1):
                    break
        except (OSError, InventoryError):
            self.failure = "inventory_failed"

    def start(self):
        self.thread.start()
        self.started = True

    def close(self):
        if not self.started:
            return
        self.stopped.set()
        self.thread.join(timeout=5)
        require(not self.thread.is_alive() and self.failure is None, "inventory_failed")
        self.inventory.sample()

    def finish(self, root):
        path = Path(root) / "server.stderr"
        found, terminal, retirements = shutdown_diagnostics(path, with_retirements=True)
        # The graph and process shutdown boundaries both emit the same frozen
        # retention census. Validate both copies; never add their counters.
        require(1 <= len(found) <= 2 and len(found[0]) <= 256
                and all(line == found[0] for line in found), "inventory_census")
        census = {}
        for item in found[0].decode("ascii").split("\t")[1:]:
            name, value = item.split("=", 1)
            require(name not in census and re.fullmatch(r"0|[1-9][0-9]{0,19}", value), "inventory_census")
            census[name] = int(value)
        observation = self.inventory.observation(census)
        generations = set()
        prior_root = 0
        for retired in retirements:
            require(retired["generation"] not in generations
                    and retired["root"] >= prior_root, "inventory_retirement_identity")
            generations.add(retired["generation"])
            prior_root = retired["root"]
        require(self.inventory.removed <= generations
                and self.inventory.present.isdisjoint(generations)
                and all(sum(retired[name] for retired in retirements) == census[name]
                        for name in ("deleted", "recycled")), "inventory_retirement_reconciliation")
        observation.update(native_retired_generations=sorted(generations),
                           native_retirement_records=retirements,
                           qualifies_complete_resource_model=False)
        path = Path(root) / "extent-observation.json"
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w") as output:
            json.dump(observation, output, sort_keys=True)
            output.write("\n")
        # A healthy command path does not establish that native maintenance
        # survived. Preserve the measured inventory, then refuse an absent,
        # malformed or stopped lane. Normal owner shutdown ends in Cancelled;
        # Refused is terminal and must never be mistaken for that cancellation.
        require(1 <= len(terminal) <= 2, "inventory_retention_terminal")
        for line in terminal:
            require(len(line) <= 4096, "inventory_retention_terminal")
            fields = {}
            for item in line.split(b"\t")[1:]:
                name, separator, value = item.partition(b"=")
                require(separator and name not in fields and len(fields) < 64
                        and re.fullmatch(rb"[a-z_]+", name), "inventory_retention_terminal")
                fields[name] = value
            require(fields.get(b"online_retention") == b"cancelled",
                    "inventory_retention_terminal")
        return observation
