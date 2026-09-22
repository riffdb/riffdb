#!/usr/bin/env python3
"""Validate and recompute WP-806's published, privacy-reduced measurements."""

import json
import math
from pathlib import Path
import re
import statistics
import sys


def require(condition, message):
    if not condition:
        raise ValueError(message)


def expected_ids():
    cells = [("write_only", 1), ("write_only", 32), ("read_only", 32), ("interactive", 32)]
    ids = set()
    for group, variants in [
        ("smoke", ["banked", "current"]),
        ("candidate-sweep", ["banked", "current", "candidate"]),
    ]:
        for variant in variants:
            for load, clients in cells:
                for rep in range(1, 4):
                    ids.add(f"{group}/{variant}-{load}-c{clients}-r{rep}.json")
    for group in ["full", "candidate-full"]:
        for load in ["write_only", "interactive"]:
            ids.add(f"{group}/{load}-c32.json")
    for group, reps in [
        ("attribution", 3), ("allocations", 1),
        ("candidate-attribution", 1), ("candidate-allocations", 1),
    ]:
        for clients in [1, 32]:
            for rep in range(1, reps + 1):
                ids.add(f"{group}/write_only-c{clients}-r{rep}.json")
    return ids


def validate(data):
    require(data["schema"] == "riffdb.wp806.write-attribution/v1", "schema")
    require(data["complete"] is True, "incomplete campaign")
    receipts = data["receipts"]
    ids = [r["id"] for r in receipts]
    require(len(ids) == len(set(ids)), "duplicate receipt")
    require(set(ids) == expected_ids(), "missing or unexpected receipt")
    for receipt in receipts:
        name = receipt["id"]
        require(re.fullmatch(r"[a-f0-9]{64}", receipt["raw_sha256"]), name + ": hash")
        require(receipt["correctness_clean"] is True, name + ": correctness")
        full = name.startswith(("full/", "candidate-full/"))
        require(len(receipt["points"]) == (6 if full else 1), name + ": point count")
        if full:
            eligibility = receipt["eligibility"]
            require(all(eligibility[k] is True for k in [
                "eligible", "stable", "correctness_clean", "resource_attribution_complete",
                "same_device_comparable", "comparison_complete",
            ]), name + ": eligibility")
            require(eligibility["non_evidentiary_window"] is False, name + ": window")
            require(eligibility["missing_required_fields"] == [], name + ": fields")
            require(receipt["postgres_durability"] == {
                "fsync": "on", "full_page_writes": "on", "server_version_num": "180004",
                "synchronous_commit": "on", "wal_sync_method": "fdatasync",
            }, name + ": PostgreSQL durability")
            for backend in ["postgres_safe_app", "riffdb_public_grpc"]:
                require(sorted(p["rep"] for p in receipt["points"]
                               if p["backend_id"] == backend) == [0, 1, 2], name + ": reps")
        for point in receipt["points"]:
            outcome = point["outcomes"]
            require(all(outcome[k] == 0 for k in ["conflict", "error", "unavailable"]), name)
            require(outcome["success"] == point["logical_ops"] > 0, name + ": successes")
            rate = point["logical_ops"] * 1e9 / point["measured_elapsed_ns"]
            require(abs(rate - point["throughput_ops_s"]) < 1.01, name + ": throughput")
        if "cpu" in receipt:
            cpu = receipt["cpu"]
            categories = cpu["exclusive_symbol_categories"].values()
            require(sum(c["period"] for c in categories) == cpu["period"] > 0, name + ": CPU")
            require(cpu["reported_event_period"] == cpu["period"], name + ": CPU coverage")
            require(cpu["lost_samples"] == 0, name + ": lost samples")
            require(cpu["event"] == "cpu-clock:u", name + ": event")
        if "census" in receipt:
            census = receipt["census"]
            require(census["commands"] == census["service_invocations"] > 0, name + ": census")
        if "allocator_status" in receipt:
            require(receipt["allocator_status"]["exit_status"] == 0, name + ": allocator")
            require(receipt["provenance_constructor_entries"] > 0, name + ": provenance")


def summary(data):
    rows = {}
    for receipt in data["receipts"]:
        group = receipt["id"].split("/")[0]
        if group not in ["candidate-sweep", "full", "candidate-full"]:
            continue
        for point in receipt["points"]:
            variant = (receipt["id"].split("/")[1].split("-")[0]
                       if group == "candidate-sweep" else point["backend_id"])
            key = (group, point["profile"], point["clients"], variant)
            rows.setdefault(key, []).append(point)
    result = []
    for key, points in sorted(rows.items()):
        row = dict(zip(["campaign", "load", "clients", "variant"], key))
        for field in ["throughput_ops_s", "aggregate_p50_ns", "aggregate_p95_ns",
                      "aggregate_p99_ns", "create_comment_p50_ns"]:
            values = [p[field] for p in points]
            if field == "create_comment_p50_ns" and key[1] == "read_only":
                require(all(v is None for v in values), "write latency in read-only cell")
                row[field] = None
                continue
            require(all(math.isfinite(v) for v in values), "nonfinite measurement")
            row[field] = {
                "values": values, "median": statistics.median(values),
                "min": min(values), "max": max(values),
            }
        result.append(row)
    return result


def main():
    default = Path(__file__).resolve().parents[1] / "docs/performance/wp-806-write-path-c3d-2026-09.json"
    require(len(sys.argv) <= 2, "usage: check-wp806-evidence.py [evidence.json]")
    data = json.loads((Path(sys.argv[1]) if len(sys.argv) == 2 else default).read_text())
    validate(data)
    computed = summary(data)
    require(data["derived_summary"] == computed, "stored summary differs from recomputation")
    sweep = {(r["load"], r["clients"], r["variant"]): r["throughput_ops_s"]["median"]
             for r in computed if r["campaign"] == "candidate-sweep"}
    ratios = {(load, clients): sweep[load, clients, "candidate"] / value
              for (load, clients, variant), value in sweep.items() if variant == "current"}
    require(all(ratio >= 0.96 for ratio in ratios.values()), "retention regression criterion")
    require(any(ratios["write_only", clients] > 1.04 for clients in [1, 32]),
            "retention benefit criterion")
    qualification = data["metadata"]["qualification"]
    require(qualification["qualified_for_full_comparison"] is True, "qualification")
    for cell in qualification["cells"]:
        require(cell["candidate_over_control"] == ratios[cell["load"], cell["clients"]],
                "qualification ratio mismatch")
    print(json.dumps({"validated_receipts": len(data["receipts"]), "summary": computed}, indent=2))


if __name__ == "__main__":
    main()
