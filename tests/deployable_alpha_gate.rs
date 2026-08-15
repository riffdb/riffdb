#![forbid(unsafe_code)]
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
        "deployable alpha phase inventory: passed\n"
    );

    let gate = std::fs::read_to_string(root.join("scripts/deployable-alpha-acceptance"))
        .expect("read final gate");
    assert!(gate.contains("\"endurance_binding\""));
    assert!(gate.contains("--bind-release-receipt"));
    assert!(gate.contains("alpha-endurance-inventory-v1.json"));
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
    let package = packages
        .split_once("- id: WP-579\n")
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
}
