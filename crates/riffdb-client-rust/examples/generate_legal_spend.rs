#![forbid(unsafe_code)]

//! Regenerates or verifies the canonical fixture-scoped Rust SDK bindings.
//!
//! Run `scripts/generate-contract-fixtures --check` first. That command owns
//! typed compiler and bundle validation; this example validates and renders the
//! checked text fixtures without decoding `bundle.bin`.

#[path = "../codegen/legal_spend_codegen.rs"]
mod legal_spend_codegen;

use std::env;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Rust SDK generation failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    let [mode] = arguments.as_slice() else {
        return Err(io::Error::other("usage: generate_legal_spend (--check|--write)").into());
    };
    let generated = legal_spend_codegen::generated_source().map_err(io::Error::other)?;
    let target = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/generated/legal_spend.rs");

    match mode.as_str() {
        "--check" => {
            let current = fs::read_to_string(&target)?;
            if current != generated {
                return Err(io::Error::other(format!(
                    "{} is not current; run this example with --write",
                    target.display()
                ))
                .into());
            }
            println!("Canonical Rust SDK bindings are current.");
        }
        "--write" => {
            if fs::read_to_string(&target).ok().as_deref() != Some(generated.as_str()) {
                fs::write(&target, generated)?;
            }
            println!("Wrote {}.", target.display());
        }
        _ => {
            return Err(io::Error::other("usage: generate_legal_spend (--check|--write)").into());
        }
    }
    Ok(())
}
