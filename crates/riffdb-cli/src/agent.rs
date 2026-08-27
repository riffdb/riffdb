//! Repository-local, generated agent rails.

use std::fs::{self, OpenOptions};
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};

use serde_json::{Map, Value};

use crate::config::ProjectConfig;

const SKILL: &[u8] = include_bytes!("../assets/agent/SKILL.md");
const AGENTS_BLOCK: &str = include_str!("../assets/agent/AGENTS.block.md");
const START_MARKER: &str = "<!-- riffdb-agent:start -->";
const END_MARKER: &str = "<!-- riffdb-agent:end -->";
const MAX_MANAGED_FILE_BYTES: usize = 1_048_576;

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum AgentInitError {
    Conflict,
    InvalidConfiguration,
    UnsafePath,
    Io,
}

impl AgentInitError {
    pub(crate) const fn code(&self) -> &'static str {
        match self {
            Self::Conflict => "agent_initialization_conflict",
            Self::InvalidConfiguration => "agent_configuration_invalid",
            Self::UnsafePath => "agent_path_unsafe",
            Self::Io => "agent_io_failed",
        }
    }

    pub(crate) const fn message(&self) -> &'static str {
        match self {
            Self::Conflict => "agent initialization conflicts with an existing managed surface",
            Self::InvalidConfiguration => "the project MCP configuration is invalid",
            Self::UnsafePath => "an agent initialization path is unsafe",
            Self::Io => "the agent rails could not be written",
        }
    }
}

pub(crate) fn initialize(project: &ProjectConfig) -> Result<(), AgentInitError> {
    let root = fs::canonicalize(project.root()).map_err(|_| AgentInitError::UnsafePath)?;
    if root != project.root() || !safe_directory(&root)? {
        return Err(AgentInitError::UnsafePath);
    }
    let skill = root.join(".agents/skills/riffdb/SKILL.md");
    let agents = root.join("AGENTS.md");
    let mcp = root.join(".mcp.json");

    let skill_bytes = preflight_exact_or_absent(&skill, SKILL)?;
    let agents_bytes = render_agents(&agents)?;
    let mcp_bytes = render_mcp(&mcp, project.endpoint(), project.database())?;

    publish(&root, &skill, &skill_bytes)?;
    publish(&root, &agents, &agents_bytes)?;
    publish(&root, &mcp, &mcp_bytes)?;
    fs::File::open(&root)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| AgentInitError::Io)
}

fn safe_directory(path: &Path) -> Result<bool, AgentInitError> {
    let metadata = fs::symlink_metadata(path).map_err(|_| AgentInitError::UnsafePath)?;
    Ok(metadata.is_dir() && !metadata.file_type().is_symlink())
}

fn preflight_exact_or_absent(path: &Path, expected: &[u8]) -> Result<Vec<u8>, AgentInitError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            Err(AgentInitError::Conflict)
        }
        Ok(_) => {
            let actual = bounded_read(path)?;
            if actual == expected {
                Ok(actual)
            } else {
                Err(AgentInitError::Conflict)
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(expected.to_vec()),
        Err(_) => Err(AgentInitError::Io),
    }
}

fn render_agents(path: &Path) -> Result<Vec<u8>, AgentInitError> {
    let existing = read_optional_regular(path)?;
    let text = std::str::from_utf8(&existing).map_err(|_| AgentInitError::Conflict)?;
    let starts = text.matches(START_MARKER).count();
    let ends = text.matches(END_MARKER).count();
    match (starts, ends) {
        (0, 0) => {
            let mut output = existing;
            if !output.is_empty() && !output.ends_with(b"\n") {
                output.push(b'\n');
            }
            if !output.is_empty() {
                output.push(b'\n');
            }
            output.extend_from_slice(AGENTS_BLOCK.as_bytes());
            Ok(output)
        }
        (1, 1) if text.contains(AGENTS_BLOCK) => Ok(existing),
        _ => Err(AgentInitError::Conflict),
    }
}

fn render_mcp(path: &Path, endpoint: &str, database: &str) -> Result<Vec<u8>, AgentInitError> {
    let existing = read_optional_regular(path)?;
    let mut root = if existing.is_empty() {
        Map::new()
    } else {
        serde_json::from_slice::<Value>(&existing)
            .ok()
            .and_then(|value| value.as_object().cloned())
            .ok_or(AgentInitError::InvalidConfiguration)?
    };
    let expected = serde_json::json!({
        "command": "riffdb-mcp",
        "args": ["--endpoint", endpoint, "--database", database]
    });
    let servers = root
        .entry("mcpServers")
        .or_insert_with(|| Value::Object(Map::new()))
        .as_object_mut()
        .ok_or(AgentInitError::InvalidConfiguration)?;
    if let Some(actual) = servers.get("riffdb") {
        if actual != &expected {
            return Err(AgentInitError::Conflict);
        }
    } else {
        servers.insert("riffdb".to_owned(), expected);
    }
    let mut output = serde_json::to_vec_pretty(&Value::Object(root))
        .map_err(|_| AgentInitError::InvalidConfiguration)?;
    output.push(b'\n');
    Ok(output)
}

fn read_optional_regular(path: &Path) -> Result<Vec<u8>, AgentInitError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            Err(AgentInitError::Conflict)
        }
        Ok(_) => bounded_read(path),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(_) => Err(AgentInitError::Io),
    }
}

fn bounded_read(path: &Path) -> Result<Vec<u8>, AgentInitError> {
    let bytes = fs::read(path).map_err(|_| AgentInitError::Io)?;
    if bytes.len() > MAX_MANAGED_FILE_BYTES {
        return Err(AgentInitError::Conflict);
    }
    Ok(bytes)
}

fn publish(root: &Path, path: &Path, expected: &[u8]) -> Result<(), AgentInitError> {
    if path.is_file() && fs::read(path).map_err(|_| AgentInitError::Io)? == expected {
        return Ok(());
    }
    let relative = path
        .strip_prefix(root)
        .map_err(|_| AgentInitError::UnsafePath)?;
    if relative
        .components()
        .any(|part| !matches!(part, std::path::Component::Normal(_)))
    {
        return Err(AgentInitError::UnsafePath);
    }
    let parent = path.parent().ok_or(AgentInitError::UnsafePath)?;
    let mut current = root.to_path_buf();
    for component in parent
        .strip_prefix(root)
        .map_err(|_| AgentInitError::UnsafePath)?
        .components()
    {
        current.push(component);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                return Err(AgentInitError::UnsafePath);
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                fs::create_dir(&current).map_err(|_| AgentInitError::Io)?;
            }
            Err(_) => return Err(AgentInitError::Io),
        }
    }
    let temporary = temporary_path(path)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(|_| AgentInitError::Io)?;
    file.write_all(expected).map_err(|_| AgentInitError::Io)?;
    file.sync_all().map_err(|_| AgentInitError::Io)?;
    fs::rename(&temporary, path).map_err(|_| AgentInitError::Io)?;
    fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| AgentInitError::Io)
}

fn temporary_path(path: &Path) -> Result<PathBuf, AgentInitError> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(AgentInitError::UnsafePath)?;
    Ok(path.with_file_name(format!(".{name}.riffdb-agent-new")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::ApplicationGenerator;
    use crate::project::{DEFAULT_PROJECT_FILE, initialize as initialize_project};

    fn project(root: &Path) -> ProjectConfig {
        initialize_project(
            root,
            Path::new(DEFAULT_PROJECT_FILE),
            Some("inventory"),
            Some("http://127.0.0.1:7443"),
            Some("inventory-db"),
            &[ApplicationGenerator::Rust],
        )
        .expect("project")
    }

    #[test]
    fn init_preserves_unmanaged_content_and_is_byte_idempotent() {
        let scratch = tempfile::TempDir::new().expect("scratch");
        fs::write(scratch.path().join("AGENTS.md"), b"# Local rules\n").expect("agents");
        fs::write(
            scratch.path().join(".mcp.json"),
            br#"{"keep":true,"mcpServers":{"other":{"command":"other"}}}"#,
        )
        .expect("mcp");
        let project = project(scratch.path());
        initialize(&project).expect("first init");
        let first = ["AGENTS.md", ".mcp.json", ".agents/skills/riffdb/SKILL.md"]
            .map(|path| fs::read(scratch.path().join(path)).expect("managed file"));
        initialize(&project).expect("second init");
        let second = ["AGENTS.md", ".mcp.json", ".agents/skills/riffdb/SKILL.md"]
            .map(|path| fs::read(scratch.path().join(path)).expect("managed file"));
        assert_eq!(first, second);
        assert!(
            String::from_utf8(first[0].clone())
                .expect("utf8")
                .starts_with("# Local rules\n\n")
        );
        let mcp: Value = serde_json::from_slice(&first[1]).expect("json");
        assert_eq!(mcp["keep"], true);
        assert_eq!(mcp["mcpServers"]["other"]["command"], "other");
        assert_eq!(
            mcp["mcpServers"]["riffdb"]["args"][1],
            "http://127.0.0.1:7443"
        );
        assert_eq!(mcp["mcpServers"]["riffdb"]["args"][3], "inventory-db");
        assert!(
            !String::from_utf8(first[1].clone())
                .expect("utf8")
                .contains("credential")
        );
    }

    #[test]
    fn conflict_is_detected_before_any_managed_file_is_written() {
        let scratch = tempfile::TempDir::new().expect("scratch");
        fs::write(
            scratch.path().join(".mcp.json"),
            br#"{"mcpServers":{"riffdb":{"command":"replacement"}}}"#,
        )
        .expect("conflict");
        let project = project(scratch.path());
        assert_eq!(initialize(&project), Err(AgentInitError::Conflict));
        assert!(!scratch.path().join("AGENTS.md").exists());
        assert!(!scratch.path().join(".agents").exists());
    }
}
