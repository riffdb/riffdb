#![forbid(unsafe_code)]

//! Regenerates the proposed contract-IR and JSON Schema format review artifacts.

use std::error::Error;
use std::fs;
use std::path::PathBuf;

use riffdb_contract_ir::{render_format_markdown, render_json_schema_format_markdown};

fn main() -> Result<(), Box<dyn Error>> {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    let [flag, root] = arguments.as_slice() else {
        return Err("usage: generate_format_docs --output-root <path>".into());
    };
    if flag != "--output-root" {
        return Err("usage: generate_format_docs --output-root <path>".into());
    }
    let output = PathBuf::from(root).join("crates/riffdb-contract-ir");
    fs::create_dir_all(&output)?;
    fs::write(output.join("FORMAT.md"), render_format_markdown())?;
    fs::write(
        output.join("JSON_SCHEMA_FORMAT.md"),
        render_json_schema_format_markdown(),
    )?;
    Ok(())
}
