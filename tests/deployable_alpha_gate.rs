#![forbid(unsafe_code)]
// req: AFC-003, AFC-008, AFC-009, AFC-010, AFC-011, AFC-012, APE-003, APE-004, APE-005, APE-006, APE-007, APE-008, APE-009, APE-010, APE-011, APE-012, APE-014, BLK-001, BLK-002, BLK-003, BLK-004, BLK-005, BLK-010, BLK-011, BLK-012, DRV-002, DRV-003, DRV-006, DRV-009, END-005, EXP-001, EXP-005, EXP-011, EXP-012, EXP-013, NET-004, NET-006, NET-007, NET-008, NET-009, NET-011, OQ-007, OQ-011, OQ-012, OQ-013, OQ-014, PERF-009, RAP-003, RAP-004, RAP-005, RAP-006, RAP-007, RAP-008, RAP-010, RAP-011, RAP-012, RAP-013, RAP-014, RAP-015, SEC-004, SECF-001, SECF-002, SECF-003, SECF-004, SECF-005, SECF-006, SYS-002, SYS-004, WF-003, WF-004, WF-005, WF-006, WF-008, WF-009, WF-010, WF-011, WF-012, WF-013
//! Structure coverage for the final installed alpha gate.

use std::path::PathBuf;
use std::process::Command;

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(|path| path.parent())
        .expect("testkit must remain under crates/")
        .to_path_buf()
}

#[test]
fn final_gate_has_one_closed_phase_inventory() {
    let root = repository_root();
    let output = Command::new(root.join("scripts/deployable-alpha-acceptance"))
        .arg("--self-test")
        .current_dir(&root)
        .output()
        .expect("deployable alpha gate self-test must launch");
    assert!(
        output.status.success(),
        "gate inventory failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    assert_eq!(
        String::from_utf8(output.stdout).expect("gate output must be UTF-8"),
        "deployable alpha phase inventory: passed\n\
deployable alpha evidence binding: passed\n"
    );

    let gate = std::fs::read_to_string(root.join("scripts/deployable-alpha-acceptance"))
        .expect("read final gate");
    assert!(gate.contains("\"endurance_binding\""));
    assert!(gate.contains("--bind-release-receipt"));
    assert!(gate.contains("alpha-endurance-inventory-v1.json"));
    assert!(gate.contains("\"--runs\", \"6\""));
    assert!(gate.contains("EVIDENCE_PATHS"));
    assert!(gate.contains("agent-application-alpha-gate-v3.json"));
    assert!(gate.contains("evidence_inventory_sha256"));
    assert!(gate.contains("adapter_woodpecker_export"));
    let driver =
        std::fs::read_to_string(root.join("scripts/driver-conformance")).expect("read driver gate");
    let row_policy = std::fs::read_to_string(root.join("scripts/adapter-row-policy-acceptance"))
        .expect("read row-policy gate");
    for source in [&gate, &driver, &row_policy] {
        assert!(
            source.contains("better-auth,mlflow"),
            "accepted alpha row-policy inventory must name Better Auth and MLflow",
        );
        assert!(
            !source.contains("--domains payload,mlflow") && !source.contains("\"payload,mlflow\""),
            "Payload must remain a post-alpha regression instead of an alpha row-policy domain",
        );
    }
}

#[test]
fn final_gate_metadata_closes_post_freeze_safety_extensions() {
    let root = repository_root();
    let packages = std::fs::read_to_string(root.join("work_packages.yaml"))
        .expect("read work-package registry");
    // The freeze ledger also names WP-579 in an indented list. Only the
    // top-level work-package entry owns its dependencies and requirements.
    let package = packages
        .split_once("\n- id: WP-579\n")
        .expect("WP-579 must remain registered")
        .1
        .split("\n- id: WP-")
        .next()
        .expect("WP-579 block must be bounded");

    for dependency in ["WP-597", "WP-598", "WP-600"] {
        assert!(
            package.contains(dependency),
            "WP-579 omits gate-critical dependency {dependency}"
        );
    }
    for requirement in [
        "SECF-001", "SECF-002", "SECF-003", "SECF-004", "SECF-005", "END-011",
    ] {
        assert!(
            package.contains(requirement),
            "WP-579 omits gate-critical requirement {requirement}"
        );
    }
    for decision in ["ADR-0117", "ADR-0118", "ADR-0119"] {
        assert!(
            package.contains(decision),
            "WP-579 omits gate-critical decision {decision}"
        );
    }

    let roadmap =
        std::fs::read_to_string(root.join("docs/architecture/DEPLOYABLE-APPLICATION-ALPHA.md"))
            .expect("read deployable-alpha roadmap");
    assert!(roadmap.contains("Framework and secret safety"));
    assert!(roadmap.contains("WP-597–WP-598, WP-600"));
    assert!(roadmap.contains("Blog in Go, Rust, and TypeScript"));
    assert!(package.contains("campaign-03 --runs 6"));

    let receipt: serde_json::Value = serde_json::from_slice(
        &std::fs::read(root.join("release/evidence/deployable-application-alpha-v1.json"))
            .expect("read deployable-application-alpha-v1.json"),
    )
    .expect("parse deployable-application-alpha-v1.json");
    assert_eq!(
        receipt["schema"],
        "riffdb.deployable-application-alpha-gate/v1"
    );
    assert_eq!(receipt["status"], "passed");
    assert!(receipt["failed_phase"].is_null());
    assert_eq!(receipt["secrets_included"], false);
    let results = receipt["results"]
        .as_array()
        .expect("deployable alpha gate results");
    assert_eq!(results.len(), 18);
    for phase in results {
        assert_eq!(phase["status"], "passed");
        assert_eq!(phase["exit_status"], 0);
    }
    let evidence = receipt["evidence"]
        .as_object()
        .expect("deployable alpha gate evidence map");
    assert_eq!(evidence.len(), 16);
    for (key, entry) in evidence {
        let rel = entry["path"].as_str().expect("evidence path");
        let path = root.join(rel);
        assert!(
            path.is_file(),
            "missing bound evidence file for {key}: {rel}"
        );
    }
}
