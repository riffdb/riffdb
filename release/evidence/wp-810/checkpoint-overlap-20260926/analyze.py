#!/usr/bin/env python3
"""Reclassify retained sync intervals; never subtract request percentiles."""

import argparse
import hashlib
import json
from pathlib import Path


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def overlap(a, b, start, end):
    return max(0, min(b, end) - max(a, start))


def classify(groups, checkpoints, sequence_range):
    """Use half-open intervals and deduplicate physical flushes before counting."""
    require(0 < len(groups) <= 65_536, "invalid group count")
    require(0 < len(checkpoints) <= 1_024, "invalid checkpoint count")
    for index, row in enumerate(checkpoints, 1):
        require(len(row) == 9 and row[0] == index, "checkpoint identity gap")
        require(0 < row[1] <= row[2] <= row[3] and row[8] == 0,
                "incomplete or failed checkpoint")
        if index > 1:
            require(checkpoints[index - 2][3] <= row[1], "overlapping checkpoints")
    low, high = sequence_range
    require(0 < low <= high, "invalid measured sequence range")
    flushes, selected = {}, set()
    measured_groups = 0
    for index, row in enumerate(groups, 1):
        require(len(row) == 17 and row[0] == index, "group identity gap")
        require(0 < row[1] <= row[2] and row[3] == row[2] - row[1] + 1,
                "invalid command coverage")
        require(0 < row[4] <= row[5] <= row[6] <= row[10] <= row[11]
                and row[7] > 0 and row[15] == 0, "invalid group timing or failure")
        if index > 1:
            require(groups[index - 2][2] < row[1], "overlapping commands")
        interval = row[5:7]
        require(flushes.setdefault(row[7], interval) == interval,
                "inconsistent shared flush interval")
        if row[2] >= low and row[1] <= high:
            selected.add(row[7])
            measured_groups += 1
    require(selected, "no measured flushes")
    rows = []
    for identity in sorted(selected):
        start, end = flushes[identity]
        materializing, installing, details = 0, 0, []
        for checkpoint in checkpoints:
            _, registered, materialized, installed, *_ = checkpoint
            before = overlap(start, end, registered, materialized)
            after = overlap(start, end, materialized, installed)
            if before or after:
                details.append({"checkpoint": checkpoint[0],
                                "registered_ns": registered,
                                "materialized_ns": materialized,
                                "installed_ns": installed,
                                "sync_end_minus_materialized_ns": end - materialized})
            materializing += before
            installing += after
        duration = end - start
        outside = duration - materializing - installing
        require(outside >= 0, "checkpoint overlap exceeds sync interval")
        phase = ("both" if materializing and installing else
                 "materializing" if materializing else
                 "post_materialization" if installing else "no_checkpoint")
        rows.append({"flush": identity, "start_ns": start, "end_ns": end,
                     "sync_ns": duration, "phase": phase,
                     "materializing_overlap_ns": materializing,
                     "post_materialization_overlap_ns": installing,
                     "outside_checkpoint_ns": outside, "checkpoints": details})

    def counts(subset):
        return {phase: sum(row["phase"] == phase for row in subset)
                for phase in ("materializing", "post_materialization", "both", "no_checkpoint")}

    slow = [row for row in rows if row["sync_ns"] > 10_000_000]
    unregistered = [row["sync_ns"] for row in rows if row["phase"] == "no_checkpoint"]
    return {"measured_groups": measured_groups, "measured_flushes": len(rows),
            "phase_counts": counts(rows),
            "over_10_ms": counts(slow),
            "over_50_ms": counts([row for row in rows if row["sync_ns"] > 50_000_000]),
            "no_checkpoint_max_sync_ns": max(unregistered, default=None),
            "slow_flushes": sorted(slow, key=lambda row: (-row["sync_ns"], row["flush"]))}


def analyze(root, bank):
    original = json.loads(bank.read_text())
    require(original["qualified_pair"], "original pair is not qualified")
    cells = []
    for banked in original["cells"]:
        name = banked["cell"]
        require(name in ("01-disabled", "02-enabled"), "unexpected cell")
        require(banked["host_valid"], "original cell is not host valid")
        directory = root / name
        names = ("run.log", "correlated.json", "report.json", "postflight.json")
        hashes = {}
        for filename in names:
            path = directory / filename
            require(path.stat().st_size <= 16 * 1024 * 1024, "oversized evidence file")
            hashes[filename] = digest(path)
            require(hashes[filename] == banked["source_file_digests"][filename],
                    "input differs from immutable bank: " + filename)
        correlation = json.loads((directory / "correlated.json").read_text())
        require(json.loads((directory / "postflight.json").read_text())["valid"],
                "invalid postflight")
        pid = correlation["load_pid"]
        groups, checkpoints, coverage = [], [], None
        for line in (directory / "run.log").read_text().splitlines():
            parts = line.split("\t")
            if len(parts) < 3 or parts[1] != f"pid={pid}":
                continue
            if parts[0] == "riffdb-correlation-group-v1":
                groups.append(list(map(int, parts[2].split(","))))
            elif parts[0] == "riffdb-correlation-checkpoint-v1":
                checkpoints.append(list(map(int, parts[2].split(","))))
            elif parts[0] == "riffdb-correlation-coverage-v1":
                coverage = {key: int(value) for key, value in
                            (part.split("=") for part in parts[2:])}
        require(checkpoints == correlation["checkpoints"], "checkpoint trace mismatch")
        require(coverage == correlation["coverage"] and coverage["final"] == 1,
                "incomplete final coverage")
        for kind, rows in (("groups", groups), ("checkpoints", checkpoints)):
            require(coverage[kind] == coverage["reserved_" + kind] == len(rows)
                    and coverage["dropped_" + kind] == 0, "trace loss")
        result = classify(groups, checkpoints, correlation["measurement_sequence_range"])
        require(result["measured_groups"] == correlation["measured_groups"],
                "measured group count mismatch")
        cells.append({"cell": name, "input_sha256": hashes, **result})
    return {"schema": "riffdb.wp810.retained-checkpoint-overlap/v1",
            "original_bank_sha256": digest(bank),
            "source_revision": original["source_revision"],
            "daemon_sha256": original["daemon_sha256"],
            "scope": "Historical trace reanalysis, not a new measurement or candidate result. "
                     "One count per physical flush intersecting measured command sequences; "
                     "boundary groups may include warmup commands. Half-open server-clock "
                     "intervals. Post-materialization means completion-to-installed, not syscall time. "
                     "Overlap is association, not proof of kernel/device causation or request attribution.",
            "cells": cells}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("private_results", type=Path)
    parser.add_argument("original_public_summary", type=Path)
    args = parser.parse_args()
    print(json.dumps(analyze(args.private_results, args.original_public_summary), indent=2))
