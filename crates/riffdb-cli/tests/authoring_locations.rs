#![forbid(unsafe_code)]
//! Exact source snapshot locations across the project authoring commands.
// req: DX-005, DX-012, ID-005
use std::process::{Command, Output, Stdio};

fn run(root: &std::path::Path, arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_riffdb"))
        .env_clear()
        .env("RIFFDB_CREDENTIAL_FILE", root.join("credential"))
        .env("RIFFDB_FORCE_STANDALONE_DEV", "1")
        .current_dir(root)
        .stdin(Stdio::null())
        .args(arguments)
        .output()
        .unwrap()
}

#[test]
fn project_commands_share_source_locations_v1_compatibility_and_v2_selection() {
    let root = tempfile::TempDir::new().unwrap();
    let created = run(root.path(), &["new", "diagnostic", "--directory", "app"]);
    assert!(
        created.status.success(),
        "{}",
        String::from_utf8_lossy(&created.stderr)
    );
    let app = root.path().join("app");
    // Non-secret synthetic presentation, generated only in the test directory.
    std::fs::write(app.join("credential"), "A".repeat(43)).unwrap();
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(
        app.join("credential"),
        std::fs::Permissions::from_mode(0o600),
    )
    .unwrap();
    std::fs::remove_file(app.join("riffdb.application.lock.json")).unwrap();
    std::fs::write(app.join("riffdb.toml"), "[client]\nendpoint = \"http://127.0.0.1:1\"\ndatabase = \"default\"\n[project]\nschema = \"riffdb.application.json\"\ngenerators = [\"rust\"]\n").unwrap();
    std::fs::write(
        app.join("riffdb/contract.riff"),
        "// α😀\r\ncontract Test secret_identifier_canary 1 {}",
    )
    .unwrap();
    for command in [
        vec!["application", "check", "--source-only"],
        vec!["application", "preview"],
        vec!["application", "lock"],
        vec!["generate"],
        vec!["push"],
        vec!["dev"],
    ] {
        for mode in ["human", "json", "json-v2"] {
            let mut args = vec!["--output", mode];
            args.extend(&command);
            let output = run(&app, &args);
            assert_eq!(
                output.status.code(),
                Some(if matches!(command[0], "application" | "dev") {
                    1
                } else {
                    2
                }),
                "{args:?}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            let text = if mode != "human" && !matches!(command[0], "application" | "dev") {
                assert!(
                    output.stderr.is_empty(),
                    "{args:?}: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
                String::from_utf8(output.stdout).unwrap()
            } else {
                assert!(output.stdout.is_empty(), "{args:?}");
                String::from_utf8(output.stderr).unwrap()
            };
            assert!(
                !text.contains("secret_identifier_canary"),
                "{args:?}: {text}"
            );
            if mode == "human" {
                assert!(
                    text.contains("riffdb/contract.riff:2:15"),
                    "{args:?}: {text}"
                );
                assert!(text.contains("found: identifier"), "{args:?}: {text}");
            } else {
                let value: serde_json::Value = serde_json::from_str(&text).unwrap();
                let entry = &value["diagnostics"][0];
                assert_eq!(entry["span"], serde_json::json!({"start":25,"end":49}));
                if mode == "json-v2" {
                    assert_eq!(value["version"], 2);
                    assert_eq!(
                        entry["location"]["start"],
                        serde_json::json!({"line":2,"column":15})
                    );
                    assert_eq!(entry["found"], "identifier");
                    assert_eq!(entry["expected"], serde_json::json!(["version"]));
                } else {
                    assert_eq!(value["version"], 1);
                    assert!(entry.get("location").is_none());
                    assert!(entry.get("found").is_none());
                    assert!(entry.get("expected").is_none());
                }
            }
        }
    }
}
