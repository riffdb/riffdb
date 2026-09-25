#!/usr/bin/env python3
"""Recompute the pilot scores. Requires the evaluation environment's jsonschema."""

import collections
import copy
import hashlib
import json
from pathlib import Path

from jsonschema import Draft202012Validator


ROOT = Path(__file__).resolve().parent


def read(name):
    return json.loads((ROOT / name).read_text())


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def score(answer, expected, catalog):
    if expected["status"] == "unavailable":
        correct = answer.get("status") == "unavailable" and not (
            answer.get("name") or answer.get("arguments")
        )
        return {
            "selection_correct": correct,
            "arguments_correct": None,
            "schema_valid": None,
            "correct": correct,
        }
    tools = {tool["name"]: tool for tool in catalog["tools"]}
    name = answer.get("name")
    selected = answer.get("status") == "call" and name == expected["name"]
    arguments = answer.get("arguments")
    valid = False
    if name in tools and isinstance(arguments, dict):
        schema = tools[name]["inputSchema"]
        valid = Draft202012Validator(schema).is_valid(arguments)
        # The compiler's UTF-8 bounds are extension keywords, not JSON Schema
        # maxLength. Check the present string arguments against those bounds.
        for field, value in arguments.items():
            limits = schema.get("properties", {}).get(field, {})
            if isinstance(value, str):
                length = len(value.encode("utf-8"))
                valid &= length >= limits.get("x-riffdb-minUtf8Bytes", 0)
                valid &= length <= limits.get("x-riffdb-maxUtf8Bytes", length)
    exact = arguments == expected["arguments"]
    return {
        "selection_correct": selected,
        "arguments_correct": exact,
        "schema_valid": bool(valid),
        "correct": bool(selected and exact and valid),
    }


def calibration(gold, catalogs):
    catalog = catalogs["small-changed.json"]
    tasks = gold["stage2_changed"]
    for task in tasks:
        assert score(task["gold"], task["gold"], catalog)["correct"]
    stale = copy.deepcopy(tasks[0]["gold"])
    stale["name"] = "riffdb_cmd_workflowdesk_approveticket"
    assert not score(stale, tasks[0]["gold"], catalog)["correct"]
    missing = copy.deepcopy(tasks[2]["gold"])
    del missing["arguments"]["review_reference"]
    assert not score(missing, tasks[2]["gold"], catalog)["schema_valid"]
    assert not score(tasks[0]["gold"], tasks[1]["gold"], catalog)["correct"]
    oversized = copy.deepcopy(tasks[0]["gold"])
    oversized["arguments"]["reason"] = "é" * 128
    assert not score(oversized, tasks[0]["gold"], catalog)["schema_valid"]


def main():
    results = read("catalog-results.json")
    assert digest(ROOT.parents[2] / "tests/mcp/catalog_risk.rs") == results["generator_source_sha256"]
    contracts = read("catalogs/contracts.json")
    for name, expected in results["generated_contract_sha256"].items():
        assert hashlib.sha256(contracts[name].encode("utf-8")).hexdigest() == expected
    gold = read("gold.json")
    plan = read("launch-plan.json")
    assert digest(ROOT / "CATALOG-PROTOCOL.md") == plan["protocol_sha256"]
    assert digest(ROOT / "gold.json") == plan["gold_sha256"]
    catalogs = {}
    for name, receipt in gold["catalogs"].items():
        path = ROOT / "catalogs" / name
        assert digest(path) == receipt["sha256"]
        assert path.stat().st_size == receipt["bytes"]
        catalogs[name] = json.loads(path.read_text())
    calibration(gold, catalogs)
    for size, unchanged in [("small", 7), ("large", 63)]:
        before = {t["name"]: t for t in catalogs[size + "-stable.json"]["tools"]}
        after = {t["name"]: t for t in catalogs[size + "-changed.json"]["tools"]}
        assert before.keys() - after.keys() == {
            "riffdb_cmd_workflowdesk_approveticket", "riffdb_cmd_workflowdesk_cancelticket"
        }
        assert after.keys() - before.keys() == {"riffdb_cmd_workflowdesk_authorizeticket"}
        assert sum(before[k] == after[k] for k in before.keys() & after.keys()) == unchanged
    assert len(results["server_evidence"]) == 4
    for evidence in results["server_evidence"]:
        name = evidence["condition"] + ".json"
        tools = catalogs[name]["tools"]
        dynamic = sum(t["name"].startswith("riffdb_cmd_") for t in tools)
        assert evidence["dynamic_tools"] == dynamic
        assert evidence["fixed_tools"] == len(tools) - dynamic == 2
        assert evidence["total_tools"] == len(tools)
        assert evidence["json_bytes"] == gold["catalogs"][name]["bytes"]
        assert evidence["equal_observations"] == 3 and evidence["pages"] == 1
        if evidence["condition"].endswith("changed"):
            assert evidence["stale_name_refusal"]["json_rpc_code"] == -32601
            assert evidence["missing_new_input_refusal"] == {
                "json_rpc_code": -32602,
                "public_code": "required_property_missing",
                "path": "/review_reference",
            }
        if evidence["condition"].startswith("small"):
            assert evidence["hidden_name_refusal"]["json_rpc_code"] == -32601
    assert {r["subject"] for r in results["runs"]} == {
        f"selection-{i:02d}" for i in range(1, 9)
    }
    assert len(results["runs"]) == 8
    conditions = collections.Counter()
    totals = collections.Counter()
    for run in results["runs"]:
        declared = next(c for c in plan["cases"] if c["subject"] == run["subject"])
        assert (run["size"], run["transition"]) == (
            declared["size"], declared["transition"]
        )
        conditions[run["size"], run["transition"]] += 1
        assert len(run["stages"]) == 2
        assert {s["stage"] for s in run["stages"]} == {1, 2}
        for stage in run["stages"]:
            number = stage["stage"]
            assert number in [1, 2]
            task_set = "stage1" if number == 1 else "stage2_" + run["transition"]
            condition = "stable" if number == 1 else run["transition"]
            name = f"{run['size']}-{condition}.json"
            assert stage["catalog_sha256"] == gold["catalogs"][name]["sha256"]
            expected = {task["task_id"]: task["gold"] for task in gold[task_set]}
            assert len(stage["tasks"]) == 3
            assert {t["task_id"] for t in stage["tasks"]} == set(expected)
            for task in stage["tasks"]:
                computed = score(task["answer"], expected[task["task_id"]], catalogs[name])
                assert computed == task["score"]
                totals["tasks"] += 1
                totals["correct"] += computed["correct"]
                totals[f"{run['size']}/{run['transition']}/stage{number}/correct"] += computed["correct"]
    assert set(conditions) == {
        ("small", "stable"), ("large", "stable"),
        ("small", "changed"), ("large", "changed"),
    }
    assert all(count == 2 for count in conditions.values())
    assert totals["tasks"] == 48
    authoring = read("authoring-results.json")
    assert digest(ROOT / "PROTOCOL.md") == authoring["protocol_sha256"]
    assert len(authoring["runs"]) == 2
    for run in authoring["runs"]:
        assert len(run["attempts"]) == 2
        assert run["attempts"][0]["exit_status"] == 1
        assert run["attempts"][1]["exit_status"] == 0
        assert run["final_source_sha256"] == "831dee757824e98656d05920a826be240b761a8d51861df882295ba51a90f672"
        assert len(run["changes"]) == (13 if run["run"] == "author-a" else 4)
    print(json.dumps({
        "catalog_subjects": len(results["runs"]),
        "catalog_scores": dict(sorted(totals.items())),
        "authoring_subjects": len(authoring["runs"]),
        "calibration": "stale name, missing required input, wrong operation and UTF-8 overflow rejected",
    }, indent=2))


if __name__ == "__main__":
    main()
