# req: END-003, END-007, END-008
"""Read-only evidence of sealed files appearing and disappearing in one volume."""
import json
import os
from pathlib import Path
import re
import stat
import threading


class InventoryError(ValueError):
    """Closed diagnostics never include a path or directory entry."""


def require(condition, code):
    if not condition:
        raise InventoryError(code)


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
        require(path.is_file() and not path.is_symlink() and path.stat().st_size <= 1048576,
                "inventory_census_bound")
        lines = path.read_bytes().splitlines()
        found = [line for line in lines if line.startswith(b"riffdb-retention-extents-v1\t")]
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
        path = Path(root) / "extent-observation.json"
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w") as output:
            json.dump(observation, output, sort_keys=True)
            output.write("\n")
        return observation
