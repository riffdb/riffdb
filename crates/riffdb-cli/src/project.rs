//! Bounded project configuration and idempotent `riffdb init` publication.

use std::ffi::OsStr;
use std::fs::{self, OpenOptions};
use std::io::{self, Write as _};
use std::path::{Component, Path, PathBuf};

use riffdb_types::DatabaseAlias;

use crate::cli::ApplicationGenerator;
use crate::config::{ProjectConfig, ProjectGenerator, load_project, validate_endpoint};
use crate::scaffold::{ScaffoldError, render_project_schema};

pub(crate) const DEFAULT_PROJECT_FILE: &str = "riffdb.toml";
const MAX_PROJECT_FILES: usize = 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PushCeremony {
    Publish,
    AlreadyExact,
    AcceptanceRequired,
    AcceptanceMismatch,
}

pub(crate) fn push_ceremony(
    current: Option<&str>,
    proposed: &str,
    accepted: Option<&str>,
) -> PushCeremony {
    match current {
        None => PushCeremony::Publish,
        Some(current) if current == proposed => PushCeremony::AlreadyExact,
        Some(_) if accepted == Some(proposed) => PushCeremony::Publish,
        Some(_) if accepted.is_some() => PushCeremony::AcceptanceMismatch,
        Some(_) => PushCeremony::AcceptanceRequired,
    }
}

#[derive(Debug)]
pub(crate) enum ProjectError {
    ConfigurationInvalid,
    InitializationConflict,
    InvalidApplication,
    UnsafePath,
    Io,
    Scaffold(ScaffoldError),
}

impl ProjectError {
    pub(crate) const fn code(&self) -> &'static str {
        match self {
            Self::ConfigurationInvalid => "project_configuration_invalid",
            Self::InitializationConflict => "project_initialization_conflict",
            Self::InvalidApplication => "project_application_invalid",
            Self::UnsafePath => "project_path_unsafe",
            Self::Io => "project_io_failed",
            Self::Scaffold(_) => "project_schema_invalid",
        }
    }

    pub(crate) const fn message(&self) -> &'static str {
        match self {
            Self::ConfigurationInvalid => "riffdb.toml is missing or invalid",
            Self::InitializationConflict => {
                "project initialization would overwrite an existing file"
            }
            Self::InvalidApplication => "the project application name is invalid",
            Self::UnsafePath => "a project path is unsafe",
            Self::Io => "the project files could not be written",
            Self::Scaffold(_) => "the initial project schema could not be compiled",
        }
    }
}

pub(crate) fn load(path: &Path) -> Result<ProjectConfig, ProjectError> {
    load_project(path).map_err(|_| ProjectError::ConfigurationInvalid)
}

pub(crate) fn initialize(
    root: &Path,
    config_path: &Path,
    application: Option<&str>,
    endpoint: Option<&str>,
    database: Option<&str>,
    generators: &[ApplicationGenerator],
) -> Result<ProjectConfig, ProjectError> {
    let root = fs::canonicalize(root).map_err(|_| ProjectError::UnsafePath)?;
    let metadata = fs::symlink_metadata(&root).map_err(|_| ProjectError::UnsafePath)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(ProjectError::UnsafePath);
    }
    let config_path = absolute_project_file(&root, config_path)?;
    let application = application
        .map(str::to_owned)
        .or_else(|| {
            root.file_name()
                .and_then(OsStr::to_str)
                .map(|name| name.to_ascii_lowercase().replace('_', "-"))
        })
        .ok_or(ProjectError::InvalidApplication)?;
    let endpoint = endpoint.unwrap_or("http://127.0.0.1:7443");
    validate_endpoint(endpoint).map_err(|_| ProjectError::ConfigurationInvalid)?;
    let database = database.unwrap_or(riffdb_types::DEFAULT_DATABASE_ALIAS);
    DatabaseAlias::new(database).map_err(|_| ProjectError::ConfigurationInvalid)?;
    let generators = canonical_generators(generators)?;
    let schema_files =
        render_project_schema(&application, &generators).map_err(ProjectError::Scaffold)?;
    let config_bytes = render_config(endpoint, database, &generators);
    let mut files = Vec::with_capacity(MAX_PROJECT_FILES);
    files.push((config_path.clone(), config_bytes));
    files.extend(
        schema_files
            .into_iter()
            .map(|(path, bytes)| (root.join(path), bytes)),
    );
    if files.len() > MAX_PROJECT_FILES {
        return Err(ProjectError::Io);
    }
    preflight_files(&root, &files)?;
    for (path, bytes) in &files {
        publish_exact_file(&root, path, bytes)?;
    }
    fs::File::open(&root)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| ProjectError::Io)?;
    load(&config_path)
}

fn absolute_project_file(root: &Path, path: &Path) -> Result<PathBuf, ProjectError> {
    let candidate = if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    };
    let parent = candidate.parent().ok_or(ProjectError::UnsafePath)?;
    let parent = fs::canonicalize(parent).map_err(|_| ProjectError::UnsafePath)?;
    if parent != root || candidate.file_name().is_none() {
        return Err(ProjectError::UnsafePath);
    }
    Ok(candidate)
}

fn canonical_generators(
    values: &[ApplicationGenerator],
) -> Result<Vec<ProjectGenerator>, ProjectError> {
    let mut values = values
        .iter()
        .map(|value| ProjectGenerator::from_surface(value.surface()))
        .collect::<Vec<_>>();
    values.sort_unstable();
    values.dedup();
    if values.is_empty() || values.len() > 5 {
        return Err(ProjectError::ConfigurationInvalid);
    }
    Ok(values)
}

fn render_config(endpoint: &str, database: &str, generators: &[ProjectGenerator]) -> Vec<u8> {
    let targets = generators
        .iter()
        .map(|target| format!("\"{}\"", target.as_str()))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "[client]\nendpoint = \"{endpoint}\"\ndatabase = \"{database}\"\n\n[project]\nschema = \"riffdb.application.json\"\ngenerators = [{targets}]\n"
    )
    .into_bytes()
}

fn preflight_files(root: &Path, files: &[(PathBuf, Vec<u8>)]) -> Result<(), ProjectError> {
    for (path, expected) in files {
        ensure_beneath(root, path)?;
        match fs::symlink_metadata(path) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
                return Err(ProjectError::InitializationConflict);
            }
            Ok(_) => {
                let actual = fs::read(path).map_err(|_| ProjectError::Io)?;
                if actual != *expected {
                    return Err(ProjectError::InitializationConflict);
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(_) => return Err(ProjectError::Io),
        }
    }
    Ok(())
}

fn publish_exact_file(root: &Path, path: &Path, bytes: &[u8]) -> Result<(), ProjectError> {
    if path.is_file() {
        return Ok(());
    }
    let parent = path.parent().ok_or(ProjectError::UnsafePath)?;
    create_project_directories(root, parent)?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| {
            if error.kind() == io::ErrorKind::AlreadyExists {
                ProjectError::InitializationConflict
            } else {
                ProjectError::Io
            }
        })?;
    file.write_all(bytes).map_err(|_| ProjectError::Io)?;
    file.sync_all().map_err(|_| ProjectError::Io)?;
    fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|_| ProjectError::Io)
}

fn create_project_directories(root: &Path, parent: &Path) -> Result<(), ProjectError> {
    let relative = parent
        .strip_prefix(root)
        .map_err(|_| ProjectError::UnsafePath)?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        let Component::Normal(component) = component else {
            return Err(ProjectError::UnsafePath);
        };
        current.push(component);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                return Err(ProjectError::UnsafePath);
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                fs::create_dir(&current).map_err(|_| ProjectError::Io)?;
                fs::File::open(current.parent().ok_or(ProjectError::UnsafePath)?)
                    .and_then(|directory| directory.sync_all())
                    .map_err(|_| ProjectError::Io)?;
            }
            Err(_) => return Err(ProjectError::Io),
        }
    }
    Ok(())
}

fn ensure_beneath(root: &Path, path: &Path) -> Result<(), ProjectError> {
    let relative = path
        .strip_prefix(root)
        .map_err(|_| ProjectError::UnsafePath)?;
    if relative.as_os_str().is_empty()
        || relative
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(ProjectError::UnsafePath);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_query_module::GeneratedApplicationArtifactKind;

    // req: DX-013, DX-014, DX-015
    #[test]
    fn init_is_idempotent_in_an_existing_project_and_never_scaffolds_application_code() {
        let scratch =
            tempfile::TempDir::with_prefix("riffdb-project-init-").expect("scratch directory");
        fs::write(scratch.path().join("package.json"), b"{}\n").expect("existing project");
        let first = initialize(
            scratch.path(),
            Path::new(DEFAULT_PROJECT_FILE),
            Some("inventory"),
            None,
            None,
            &[ApplicationGenerator::Rust, ApplicationGenerator::Typescript],
        )
        .expect("first init");
        let second = initialize(
            scratch.path(),
            Path::new(DEFAULT_PROJECT_FILE),
            Some("inventory"),
            None,
            None,
            &[ApplicationGenerator::Typescript, ApplicationGenerator::Rust],
        )
        .expect("idempotent init");
        assert_eq!(first, second);
        assert!(scratch.path().join("package.json").is_file());
        assert!(scratch.path().join("riffdb.toml").is_file());
        assert!(scratch.path().join("riffdb.application.json").is_file());
        assert!(scratch.path().join("riffdb/contract.riff").is_file());
        let contract = fs::read_to_string(scratch.path().join("riffdb/contract.riff"))
            .expect("empty contract");
        assert!(!contract.contains("entity "));
        assert!(!contract.contains("command "));
        assert!(!scratch.path().join("riffdb/queries").exists());
        assert!(!scratch.path().join("src").exists());
        assert!(!scratch.path().join("Cargo.toml").exists());
        assert!(!scratch.path().join("riffdb.application.lock.json").exists());
    }

    // req: DX-014
    #[test]
    fn init_conflict_is_detected_before_any_other_file_changes() {
        let scratch =
            tempfile::TempDir::with_prefix("riffdb-project-conflict-").expect("scratch directory");
        fs::write(scratch.path().join("riffdb.application.json"), b"retain\n").expect("conflict");
        let error = initialize(
            scratch.path(),
            Path::new(DEFAULT_PROJECT_FILE),
            Some("inventory"),
            None,
            None,
            &[ApplicationGenerator::Rust],
        )
        .expect_err("conflict");
        assert!(matches!(error, ProjectError::InitializationConflict));
        assert!(!scratch.path().join("riffdb.toml").exists());
        assert!(!scratch.path().join("riffdb").exists());
        assert_eq!(
            fs::read(scratch.path().join("riffdb.application.json")).expect("retained"),
            b"retain\n"
        );
    }

    // req: DX-017
    #[test]
    fn fresh_and_noop_pushes_need_no_acceptance() {
        assert_eq!(push_ceremony(None, "22", None), PushCeremony::Publish);
        assert_eq!(
            push_ceremony(Some("22"), "22", None),
            PushCeremony::AlreadyExact
        );
    }

    // req: DX-017, DX-018, DX-048
    #[test]
    fn changed_push_requires_the_exact_proposed_identity() {
        assert_eq!(
            push_ceremony(Some("11"), "22", None),
            PushCeremony::AcceptanceRequired
        );
        assert_eq!(
            push_ceremony(Some("11"), "22", Some("33")),
            PushCeremony::AcceptanceMismatch
        );
        assert_eq!(
            push_ceremony(Some("11"), "22", Some("22")),
            PushCeremony::Publish
        );
    }

    // req: DX-016, DX-042, DX-043, DX-044, DX-047, DX-049
    #[test]
    fn project_materializes_only_selected_language_artifacts_and_keeps_present_ones_exact() {
        let scratch =
            tempfile::TempDir::with_prefix("riffdb-project-targets-").expect("scratch directory");
        let project = initialize(
            scratch.path(),
            Path::new(DEFAULT_PROJECT_FILE),
            Some("inventory"),
            None,
            None,
            &[ApplicationGenerator::Rust],
        )
        .expect("empty project");
        let preview = crate::scaffold::preview_genesis_application_lock(project.schema())
            .expect("genesis preview");
        crate::scaffold::write_project_application_lock_with_bundle(
            project.schema(),
            preview.into_contract(),
            &[GeneratedApplicationArtifactKind::Rust],
        )
        .expect("selected publication");

        assert!(scratch.path().join("generated/rust/client.rs").is_file());
        assert!(!scratch.path().join("generated/mcp/tools.json").exists());
        assert!(
            !scratch
                .path()
                .join("generated/typescript/client.ts")
                .exists()
        );
        assert!(!scratch.path().join("generated/go/client.go").exists());
        assert!(!scratch.path().join("generated/python/client.py").exists());

        crate::scaffold::generate_project_application(
            project.schema(),
            &[GeneratedApplicationArtifactKind::TypeScript],
        )
        .expect_err("undeclared target");
        assert!(
            !scratch
                .path()
                .join("generated/typescript/client.ts")
                .exists()
        );
        fs::create_dir_all(scratch.path().join("generated/typescript")).expect("unowned parent");
        fs::write(
            scratch.path().join("generated/typescript/client.ts"),
            b"unowned\n",
        )
        .expect("unowned former target");
        crate::scaffold::generate_project_application(
            project.schema(),
            &[GeneratedApplicationArtifactKind::Rust],
        )
        .expect("declared generation leaves unowned files alone");
        assert_eq!(
            fs::read(scratch.path().join("generated/typescript/client.ts")).expect("unowned file"),
            b"unowned\n"
        );

        fs::write(scratch.path().join("generated/rust/client.rs"), b"stale\n")
            .expect("corrupt retained artifact");
        let repair = crate::scaffold::generate_project_application(
            project.schema(),
            &[GeneratedApplicationArtifactKind::Rust],
        );
        assert!(
            repair.is_ok(),
            "a selected declared artifact may be repaired: {repair:?}"
        );
    }

    // req: DX-042, DX-044, DX-047, DX-049
    #[test]
    fn every_singleton_generator_declares_locks_and_materializes_only_its_surface() {
        let cases = [
            (
                ApplicationGenerator::Rust,
                GeneratedApplicationArtifactKind::Rust,
                "generated/rust/client.rs",
            ),
            (
                ApplicationGenerator::Go,
                GeneratedApplicationArtifactKind::Go,
                "generated/go/client.go",
            ),
            (
                ApplicationGenerator::Typescript,
                GeneratedApplicationArtifactKind::TypeScript,
                "generated/typescript/client.ts",
            ),
            (
                ApplicationGenerator::Python,
                GeneratedApplicationArtifactKind::Python,
                "generated/python/client.py",
            ),
            (
                ApplicationGenerator::Mcp,
                GeneratedApplicationArtifactKind::Mcp,
                "generated/mcp/tools.json",
            ),
        ];
        for (generator, artifact_kind, expected_path) in cases {
            let scratch = tempfile::TempDir::with_prefix("riffdb-project-singleton-")
                .expect("scratch directory");
            let project = initialize(
                scratch.path(),
                Path::new(DEFAULT_PROJECT_FILE),
                Some("inventory"),
                None,
                None,
                &[generator],
            )
            .expect("singleton project");
            let source_bytes = fs::read(project.schema()).expect("source");
            let source =
                riffdb_query_module::ApplicationSourceManifest::decode_canonical(&source_bytes)
                    .expect("canonical source");
            assert_eq!(
                source.schema(),
                riffdb_query_module::APPLICATION_SOURCE_SCHEMA_V7
            );
            assert_eq!(
                riffdb_query_module::GeneratedApplicationSurface::ALL
                    .into_iter()
                    .filter_map(|surface| source.generation().path(surface).map(|_| surface))
                    .collect::<Vec<_>>(),
                vec![generator_to_project(generator).surface()]
            );

            let preview = crate::scaffold::preview_genesis_application_lock(project.schema())
                .expect("genesis preview");
            crate::scaffold::write_project_application_lock_with_bundle(
                project.schema(),
                preview.into_contract(),
                &[artifact_kind],
            )
            .expect("singleton lock publication");
            assert!(scratch.path().join(expected_path).is_file());
            for surface in riffdb_query_module::GeneratedApplicationSurface::ALL {
                if surface.artifact_kind() != artifact_kind {
                    assert!(!scratch.path().join(surface.default_path()).exists());
                }
            }
        }
    }

    fn generator_to_project(generator: ApplicationGenerator) -> ProjectGenerator {
        ProjectGenerator::from_surface(generator.surface())
    }
}
