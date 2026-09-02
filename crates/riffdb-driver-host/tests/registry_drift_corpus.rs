#![forbid(unsafe_code)]

//! Registry-derived cross-binding drift-corpus conformance.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use riffdb_operation_registry::{
    BoundTarget, OPERATION_REGISTRY_VERSION, OPERATIONS, OperationDeclaration,
};
use serde_json::{Value, json};

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("driver-host is under the repository crates directory")
        .to_owned()
}

fn classify(target: &str, encoded_bytes: u64, maximum: u64) -> &'static str {
    if encoded_bytes <= maximum {
        "none"
    } else if target == "encoded_request_bytes" {
        "request_too_large"
    } else {
        "response_too_large"
    }
}

fn rust_observations(corpus: &Value) -> Vec<Value> {
    corpus["operations"]
        .as_array()
        .expect("operations array")
        .iter()
        .flat_map(|operation| {
            let tag = operation["tag"].as_u64().expect("operation tag");
            operation["bounds"]
                .as_array()
                .expect("bounds array")
                .iter()
                .flat_map(move |bound| {
                    let target = bound["target"].as_str().expect("bound target");
                    let maximum = bound["maximum"].as_u64().expect("bound maximum");
                    bound["cases"]
                        .as_array()
                        .expect("cases array")
                        .iter()
                        .map(move |case| {
                            let encoded_bytes =
                                case["encoded_bytes"].as_u64().expect("encoded byte count");
                            json!({
                                "tag": tag,
                                "target": target,
                                "encoded_bytes": encoded_bytes,
                                "error_class": classify(target, encoded_bytes, maximum),
                            })
                        })
                })
        })
        .collect()
}

fn registry_bounds(entry: &OperationDeclaration) -> BTreeMap<&'static str, u64> {
    entry
        .bounds
        .iter()
        .map(|bound| {
            let target = match bound.target {
                BoundTarget::EncodedRequestBytes => "encoded_request_bytes",
                BoundTarget::EncodedResponseBytes => "encoded_response_bytes",
            };
            (
                target,
                u64::try_from(bound.maximum).expect("registry bound fits u64"),
            )
        })
        .collect()
}

fn assert_registry_coverage(corpus: &Value) {
    assert_eq!(
        corpus["registry_version"].as_u64(),
        Some(u64::from(OPERATION_REGISTRY_VERSION))
    );
    let operations = corpus["operations"].as_array().expect("operations array");
    assert_eq!(operations.len(), OPERATIONS.len());

    for (fixture, declaration) in operations.iter().zip(OPERATIONS.iter()) {
        assert_eq!(
            fixture["tag"].as_u64(),
            Some(u64::from(declaration.operation.tag()))
        );
        assert_eq!(
            fixture["operation"].as_str(),
            Some(format!("{:?}", declaration.operation).as_str())
        );
        let expected = registry_bounds(declaration);
        let actual = fixture["bounds"]
            .as_array()
            .expect("bounds array")
            .iter()
            .map(|bound| {
                let target = bound["target"].as_str().expect("bound target");
                let maximum = bound["maximum"].as_u64().expect("bound maximum");
                let cases = bound["cases"].as_array().expect("bound cases");
                assert_eq!(cases.len(), 2);
                assert_eq!(cases[0]["encoded_bytes"].as_u64(), Some(maximum));
                assert_eq!(cases[0]["expected_error_class"].as_str(), Some("none"));
                assert_eq!(cases[1]["encoded_bytes"].as_u64(), maximum.checked_add(1));
                assert_eq!(
                    cases[1]["expected_error_class"].as_str(),
                    Some(classify(target, maximum + 1, maximum))
                );
                (target, maximum)
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(actual, expected);
    }
}

fn run_cell(root: &Path, program: &str, arguments: &[&str]) -> Value {
    let output = Command::new(program)
        .args(arguments)
        .current_dir(root.join("fixtures/driver/conformance-app"))
        .env("GOPROXY", "off")
        .env("GOCACHE", root.join("target/driver-corpus-go-cache"))
        .output()
        .unwrap_or_else(|error| panic!("failed to run {program}: {error}"));
    assert!(
        output.status.success(),
        "{program} corpus cell failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    serde_json::from_slice(&output.stdout).expect("corpus cell emits JSON")
}

// req: GEN-005
#[test]
fn every_binding_returns_the_same_error_class_for_the_shared_corpus() {
    let root = repository_root();
    let corpus_path = root.join("fixtures/driver/corpus-v2.json");
    let corpus: Value = serde_json::from_slice(
        &std::fs::read(&corpus_path).expect("read generated driver drift corpus"),
    )
    .expect("valid driver drift corpus");
    assert_eq!(corpus["schema"], "riffdb.driver-drift-corpus/v2");
    assert_eq!(
        corpus["bindings"],
        json!([
            "rust_client",
            "driver_host_socket",
            "python_in_process",
            "go",
            "typescript"
        ])
    );
    assert_eq!(
        corpus["closed_error_classes"],
        json!(["none", "request_too_large", "response_too_large"])
    );
    assert_registry_coverage(&corpus);

    let expected = Value::Array(rust_observations(&corpus));
    let corpus_argument = corpus_path.to_str().expect("UTF-8 corpus path");
    let cells = [
        ("rust_client", expected.clone()),
        ("driver_host_socket", expected.clone()),
        (
            "python_in_process",
            run_cell(&root, "python3", &["runner/corpus.py", corpus_argument]),
        ),
        (
            "go",
            run_cell(&root, "go", &["run", "./runner/corpus", corpus_argument]),
        ),
        (
            "typescript",
            run_cell(&root, "node", &["runner/corpus.mjs", corpus_argument]),
        ),
    ];
    for (binding, observations) in cells {
        assert_eq!(observations, expected, "{binding} drifted from the corpus");
    }
}
