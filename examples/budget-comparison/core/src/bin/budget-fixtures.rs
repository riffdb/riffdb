//! Deterministic writer/checker for budget comparison fixtures.

#![forbid(unsafe_code)]

use riffdb_budget_comparison_core::generated_fixtures;
use std::error::Error;
use std::fmt;
use std::fs;
use std::path::Path;

fn main() -> Result<(), FixtureCommandError> {
    let mode = match std::env::args().nth(1).as_deref() {
        Some("--check") => Mode::Check,
        Some("--write") => Mode::Write,
        _ => return Err(FixtureCommandError::Usage),
    };
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .ok_or(FixtureCommandError::WorkspacePath)?;
    for fixture in generated_fixtures().map_err(|_| FixtureCommandError::Generation)? {
        let path = workspace.join(&fixture.relative_path);
        match mode {
            Mode::Check => {
                let actual = fs::read_to_string(&path).map_err(|_| FixtureCommandError::Read)?;
                if actual != fixture.contents {
                    return Err(FixtureCommandError::Stale);
                }
            }
            Mode::Write => {
                let parent = path.parent().ok_or(FixtureCommandError::WorkspacePath)?;
                fs::create_dir_all(parent).map_err(|_| FixtureCommandError::Write)?;
                fs::write(path, fixture.contents).map_err(|_| FixtureCommandError::Write)?;
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum Mode {
    Check,
    Write,
}

#[derive(Clone, Copy, Debug)]
enum FixtureCommandError {
    Usage,
    WorkspacePath,
    Generation,
    Read,
    Write,
    Stale,
}

impl fmt::Display for FixtureCommandError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Usage => formatter.write_str("usage: budget-fixtures --check|--write"),
            Self::WorkspacePath => formatter.write_str("comparison workspace path is invalid"),
            Self::Generation => formatter.write_str("reference fixture generation failed"),
            Self::Read => formatter.write_str("could not read a checked fixture"),
            Self::Write => formatter.write_str("could not write a generated fixture"),
            Self::Stale => formatter.write_str("a checked fixture is stale"),
        }
    }
}

impl Error for FixtureCommandError {}
