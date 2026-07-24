//! Deterministic writer and checker for WP-139 fixtures.

#![forbid(unsafe_code)]

use std::error::Error;
use std::fmt;
use std::fs;
use std::path::Path;

use riffdb_budget_safety_evidence::generated_safety_fixtures;

fn main() -> Result<(), FixtureCommandError> {
    let mut arguments = std::env::args_os().skip(1);
    let mode = match arguments.next().as_deref().and_then(|value| value.to_str()) {
        Some("--check") => Mode::Check,
        Some("--write") => Mode::Write,
        _ => return Err(FixtureCommandError::Usage),
    };
    if arguments.next().is_some() {
        return Err(FixtureCommandError::Usage);
    }

    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .ok_or(FixtureCommandError::WorkspacePath)?;
    for fixture in generated_safety_fixtures().map_err(|_| FixtureCommandError::Generation)? {
        let path = workspace.join(fixture.relative_path);
        match mode {
            Mode::Check => {
                let actual = fs::read(path).map_err(|_| FixtureCommandError::Read)?;
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
        formatter.write_str(match self {
            Self::Usage => "usage: budget-safety-fixtures --check|--write",
            Self::WorkspacePath => "comparison workspace path is invalid",
            Self::Generation => "safety fixture generation failed",
            Self::Read => "could not read a checked safety fixture",
            Self::Write => "could not write a generated safety fixture",
            Self::Stale => "a checked safety fixture is stale",
        })
    }
}

impl Error for FixtureCommandError {}
