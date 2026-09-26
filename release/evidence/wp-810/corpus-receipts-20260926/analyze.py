"""Reproduce the corpus receipt repair from retained private audit files."""
from pathlib import Path
import json
import sys

from parse_audit import digest, parse

CAUSAL = "0d9edbce0593ec1227f66fd9af2ea934ee09ecf9"
PARENT = "e6755d37cc92635f992e4e184a956735bb1f11b7"
MOVED = {"0x51c2c067", "0x51c2c200", "0x51c2c406", "0x51c2c504", "0x51c2c301", "0x51c2c41d"}


def mismatches(rows):
    return {seed for seed, row in rows.items() if row.get("receipt_mismatch")}


def check_repair(parent, child, current, fixed):
    assert not mismatches(parent)
    assert mismatches(child) == MOVED
    assert child == current, "current failures differ from the causal child"
    assert not mismatches(fixed)
    assert parent.keys() == child.keys() == fixed.keys()
    for seed in child:
        if "report" in child[seed]:
            assert child[seed]["report"] == fixed[seed]["report"], "repair changed campaign execution"
            assert child[seed]["expectation_verdicts"] == fixed[seed]["expectation_verdicts"], "repair weakened a predicate"
        else:
            assert child[seed] == fixed[seed]
    changes = {}
    for seed in sorted(MOVED):
        before, after = parent[seed], child[seed]
        assert before["exact_territory_holds"] != after["exact_territory_holds"]
        changes[seed] = {"parent_territory_holds": before["exact_territory_holds"],
                         "child_territory_holds": after["exact_territory_holds"],
                         "parent_report": before["report"], "child_report": after["report"],
                         "unchanged_predicates": after["expectation_verdicts"],
                         "repaired_receipt_state": fixed[seed]["rotation"]}
    return changes


def analyze(root):
    paths = {"parent": root / "v2-bounded-parent/audit.log",
             "child": root / "v2-bounded-child/audit.log",
             "current": root.parent / "corpus-audit-integrated-main-20260926.log",
             "fixed": root / "fixed/audit.log"}
    rows = {name: parse(path) for name, path in paths.items()}
    changes = check_repair(**rows)
    receipts = {name: json.loads((root / directory / "receipt.json").read_text())
                for name, directory in (("parent", "v2-bounded-parent"), ("child", "v2-bounded-child"), ("fixed", "fixed"))}
    assert receipts["parent"]["revision"] == PARENT and receipts["child"]["revision"] == CAUSAL
    assert receipts["parent"]["test_binary_sha256"] != receipts["child"]["test_binary_sha256"]
    assert all(r["exit_code"] == 0 for r in receipts.values())
    stability = json.loads((root / "stability/receipt.json").read_text())
    assert stability["count"] == len(stability["runs"]) == 12
    for run in stability["runs"]:
        path = root / "stability" / f"audit-{run['ordinal']:02}.log"
        assert digest(path) == run["log_sha256"] and parse(path) == rows["child"]
    gates = {"parent": root / "v2-bounded-parent/gate.log",
             "before": root.parent / "corpus-gate-integrated-main-20260926.log",
             "after": root.parent / "corpus-receipt-repair-gate-20260926.log"}
    for name in ("parent", "after"):
        assert "1 passed; 0 failed" in gates[name].read_text()
    assert "0 passed; 1 failed" in gates["before"].read_text()
    exploration = {}
    for name in ("parent", "child", "migration-parent", "migration-child"):
        path = root / name / "audit.log"
        receipt = json.loads((root / name / "receipt.json").read_text())
        exploration[name] = {"revision": receipt["revision"], "audit_sha256": digest(path),
                             "mismatched_coordinates": sorted(mismatches(parse(path)))}
    return {"schema": "riffdb.wp810-corpus-receipt-repair/v1", "causal_parent": PARENT, "causal_child": CAUSAL,
            "scope": "Integration CI repair only; no runtime, performance or WP-810 completion verdict.",
            "source_and_binary_receipts": receipts,
            "input_sha256": {name: digest(path) for name, path in paths.items()},
            "gate_sha256": {name: digest(path) for name, path in gates.items()},
            "coordinate_count": len(rows["child"]), "changed_receipts": changes,
            "audit_populations": rows, "repeatability": stability,
            "earlier_exploration": exploration,
            "history_limit": "The whole-gate history is not monotone. Initial bisection found an earlier failure period that was repaired before the causal parent. It is not used to attribute the current six mismatches; exact parent/child audits do that.",
            "preserved": ["all seeds and generator versions", "all numeric and semantic expectations", "every original rotation/restoration receipt", "successor chains", "commit-PRESENT retirement", "recovery oracle and final frontiers"]}


if __name__ == "__main__":
    assert len(sys.argv) == 3
    Path(sys.argv[2]).write_text(json.dumps(analyze(Path(sys.argv[1])), indent=2, sort_keys=True) + "\n")
