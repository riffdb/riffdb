//! Build-script toolchain and locked-dependency verification for `riffdb-server`.
//! Normal trusted Cargo/rustc toolchain boundary, not hostile compiler attestation.
use std::{env, fs, path::PathBuf, process::Command};

const COMMIT: &str = "58cf8f9c35b355199727627d07ae34a4e2846d67";
const TOKIO_CHECKSUM: &str = "b52fd3255258d26fac7264ab5a76b53c13367136dc011d3d6ad4f8ed070396a3";

fn supported_rustc(text: &str) -> bool {
    text.lines().any(|line| line == "release: 1.99.1")
        && text
            .lines()
            .any(|line| line.strip_prefix("commit-hash: ") == Some(COMMIT))
        && text
            .lines()
            .any(|line| line == "host: x86_64-unknown-linux-gnu")
}
fn locked_tokio(text: &str) -> bool {
    text.split("[[package]]").any(|record| {
        let field = |name: &str| {
            record.lines().find_map(|line| {
                line.trim()
                    .strip_prefix(name)
                    .and_then(|value| value.strip_prefix(" = \""))
                    .and_then(|value| value.strip_suffix('"'))
            })
        };
        field("name") == Some("tokio")
            && field("version") == Some("1.54.1")
            && field("checksum") == Some(TOKIO_CHECKSUM)
    })
}
fn main() {
    println!("cargo:rustc-check-cfg=cfg(riffdb_source_tokio_task_layout)");
    println!("cargo:rerun-if-changed=../../Cargo.lock");
    println!("cargo:rerun-if-changed=Cargo.toml");
    for variable in [
        "RUSTC",
        "TARGET",
        "CARGO_CFG_TOKIO_UNSTABLE",
        "CARGO_CFG_LOOM",
        "CARGO_CFG_MIRI",
        "CARGO_ENCODED_RUSTFLAGS",
        "RUSTFLAGS",
    ] {
        println!("cargo:rerun-if-env-changed={variable}");
    }
    let unsupported_cfg = [
        "CARGO_CFG_TOKIO_UNSTABLE",
        "CARGO_CFG_LOOM",
        "CARGO_CFG_MIRI",
    ]
    .iter()
    .any(|name| env::var_os(name).is_some());
    // Cargo reports --cfg flags in CARGO_CFG_*; additionally refuse the literal
    // unsupported names in supplied flags. There is no enable/override variable.
    let flags = ["CARGO_ENCODED_RUSTFLAGS", "RUSTFLAGS"]
        .iter()
        .filter_map(|name| env::var(name).ok())
        .collect::<Vec<_>>()
        .join(" ");
    if unsupported_cfg
        || ["tokio_unstable", "loom", "miri"]
            .iter()
            .any(|name| flags.contains(name))
        || env::var("TARGET").ok().as_deref() != Some("x86_64-unknown-linux-gnu")
    {
        return;
    }
    let Some(rustc) = env::var_os("RUSTC") else {
        return;
    };
    let Ok(output) = Command::new(rustc).arg("-vV").output() else {
        return;
    };
    if !output.status.success() || output.stdout.len() > 4096 {
        return;
    }
    let Ok(version) = std::str::from_utf8(&output.stdout) else {
        return;
    };
    let Some(root) = env::var_os("CARGO_MANIFEST_DIR") else {
        return;
    };
    let root = PathBuf::from(root);
    let Ok(manifest) = fs::read_to_string(root.join("Cargo.toml")) else {
        return;
    };
    let mut direct = manifest
        .lines()
        .filter(|line| line.trim().starts_with("tokio = "));
    let Some(first) = direct.next() else {
        return;
    };
    if !first.contains("version = \"=1.54.1\"")
        || direct.any(|line| !line.contains("version = \"=1.54.1\""))
    {
        return;
    }
    let Ok(lock) = fs::read_to_string(root.join("../../Cargo.lock")) else {
        return;
    };
    if supported_rustc(version) && locked_tokio(&lock) {
        println!("cargo:rustc-cfg=riffdb_source_tokio_task_layout");
    }
}
