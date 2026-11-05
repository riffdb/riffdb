//! Bounded project configuration and idempotent `riffdb init` publication.

use std::ffi::OsStr;
use std::fs::{self, DirBuilder, OpenOptions};
use std::io::{self, BufRead as _, BufReader, Read as _, Write as _};
use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _, PermissionsExt as _};
use std::path::{Component, Path, PathBuf};
use std::process::{Child, Command as ProcessCommand, ExitCode, Stdio};

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
    starter: bool,
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
    let schema_files = render_project_schema(&application, &generators, starter)
        .map_err(ProjectError::Scaffold)?;
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

pub(crate) struct StandaloneDevOptions<'a> {
    pub(crate) role: &'a str,
    pub(crate) tenant: Option<&'a str>,
    pub(crate) sync_manifest: bool,
    pub(crate) sync_role: Option<&'a str>,
    pub(crate) watch: bool,
    pub(crate) run: bool,
    pub(crate) exec: Option<&'a str>,
    pub(crate) seed: bool,
    pub(crate) seed_dir: Option<&'a OsStr>,
    pub(crate) seed_concurrency: &'a str,
    pub(crate) acceptance: bool,
}

struct StandaloneDevGuard {
    server_child: Option<Child>,
    application_root: PathBuf,
    work_root: PathBuf,
}

impl Drop for StandaloneDevGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.server_child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        let dot_riffdb = self.application_root.join(".riffdb");
        let dev_dir = dot_riffdb.join("dev");
        let dot_safe = fs::symlink_metadata(&dot_riffdb)
            .map(|m| !m.file_type().is_symlink())
            .unwrap_or(false);
        let dev_safe = fs::symlink_metadata(&dev_dir)
            .map(|m| !m.file_type().is_symlink())
            .unwrap_or(false);
        if dot_safe && dev_safe {
            let _ = fs::remove_file(dev_dir.join("session.json"));
            let _ = fs::remove_file(dev_dir.join("profile.credential"));
        }
        let _ = fs::remove_dir_all(&self.work_root);
    }
}

const STANDALONE_DEV_BOOTSTRAP_REQUEST: &str = r#"{
  "principal_id": "riffdb-dev-operator",
  "actor_kind": "human",
  "requested_lifetime_seconds": 86400,
  "audiences": ["riffdb-grpc-loopback"],
  "grant": {
    "tenant_scope": {"type": "global"},
    "partition_scope": {"type": "all"},
    "permissions": [
      {"type": "read_contract"},
      {"type": "deploy_contract"},
      {"type": "read_health"},
      {"type": "administer_capabilities"}
    ],
    "field_visibility": [],
    "max_scan_rows": 1,
    "approval_required": []
  }
}
"#;

pub(crate) fn run_standalone_dev(
    application_root: &Path,
    current_executable: Option<&Path>,
    options: &StandaloneDevOptions<'_>,
) -> ExitCode {
    let Some(riffdbd_bin) =
        resolve_standalone_binary("RIFFDB_SERVER_BIN", "riffdbd", current_executable)
    else {
        eprintln!("riffdb dev requires riffdbd in the same directory or on PATH");
        return ExitCode::FAILURE;
    };
    let Some(riffdb_bin) =
        resolve_standalone_binary("RIFFDB_CLI_BIN", "riffdb", current_executable)
    else {
        eprintln!("riffdb dev requires riffdb in the same directory or on PATH");
        return ExitCode::FAILURE;
    };
    run_standalone_dev_with_binaries(application_root, &riffdbd_bin, &riffdb_bin, options)
}

pub(crate) fn run_standalone_dev_with_binaries(
    application_root: &Path,
    riffdbd_bin: &Path,
    riffdb_bin: &Path,
    options: &StandaloneDevOptions<'_>,
) -> ExitCode {
    if options.run && (options.watch || options.acceptance || options.exec.is_some()) {
        eprintln!("--run cannot be combined with --watch, --exec, or --acceptance");
        return ExitCode::from(2);
    }
    if options.exec.is_some() && (options.watch || options.acceptance) {
        eprintln!("--exec cannot be combined with --watch, --run, or --acceptance");
        return ExitCode::from(2);
    }
    if let Some(exec) = options.exec
        && (exec.is_empty() || exec.len() > 4096)
    {
        eprintln!("--exec requires a non-empty command of at most 4096 bytes");
        return ExitCode::from(2);
    }
    if !matches!(
        options.seed_concurrency,
        "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8"
    ) {
        eprintln!("seed concurrency must be in 1..8");
        return ExitCode::from(2);
    }
    let source_path = application_root.join("riffdb.application.json");
    if !source_path.is_file() {
        eprintln!("application root has no riffdb.application.json");
        return ExitCode::from(2);
    }
    let riffdbd_bin = riffdbd_bin.to_path_buf();
    let riffdb_bin = riffdb_bin.to_path_buf();

    if let Err(message) = lock_and_generate_for_dev(application_root, &source_path, options) {
        eprintln!("{message}");
        return ExitCode::FAILURE;
    }

    let exact_manifest_path = application_root.join("generated/riffdb.application.exact.json");
    let manifest_bytes = match fs::read(&exact_manifest_path) {
        Ok(bytes) => bytes,
        Err(_) => {
            eprintln!("generated/riffdb.application.exact.json is unavailable");
            return ExitCode::FAILURE;
        }
    };
    let manifest: serde_json::Value = match serde_json::from_slice(&manifest_bytes) {
        Ok(value) => value,
        Err(_) => {
            eprintln!("generated/riffdb.application.exact.json is invalid");
            return ExitCode::FAILURE;
        }
    };
    let Some(application_name) = manifest["application"].as_str() else {
        eprintln!("application manifest is missing application name");
        return ExitCode::FAILURE;
    };
    let Some(contract_rel) = manifest["contract"]["source"].as_str() else {
        eprintln!("application manifest is missing contract.source");
        return ExitCode::FAILURE;
    };
    let Some(contract_lineage) = manifest["contract"]["lineage"].as_str() else {
        eprintln!("application manifest is missing contract.lineage");
        return ExitCode::FAILURE;
    };
    let Some(contract_version) = manifest["contract"]["version"].as_u64() else {
        eprintln!("application manifest is missing contract.version");
        return ExitCode::FAILURE;
    };
    let Some(roles) = manifest["roles"].as_array() else {
        eprintln!("application manifest is missing roles");
        return ExitCode::FAILURE;
    };
    let role_entry = if options.role == "application" {
        let Some(first) = roles.first() else {
            eprintln!(
                "application manifest declares no roles; add a role to riffdb.application.json or initialize with riffdb init --starter"
            );
            return ExitCode::from(2);
        };
        first
    } else {
        let Some(found) = roles
            .iter()
            .find(|entry| entry["name"].as_str() == Some(options.role))
        else {
            eprintln!(
                "application manifest does not declare role: {}",
                options.role
            );
            return ExitCode::from(2);
        };
        found
    };
    let Some(role_symbol) = role_entry["name"].as_str() else {
        eprintln!("application role entry is missing name");
        return ExitCode::FAILURE;
    };
    let effective_tenant = match role_entry["tenant_scope"].as_str() {
        Some("global") => {
            if options.tenant.is_some() {
                eprintln!(
                    "application role {role_symbol} has global tenant_scope; remove --tenant"
                );
                return ExitCode::from(2);
            }
            String::new()
        }
        Some("tenant") => {
            let resolved = options
                .tenant
                .map(str::to_owned)
                .or_else(|| {
                    std::env::var("RIFFDB_TENANT")
                        .ok()
                        .filter(|value| !value.is_empty())
                })
                .unwrap_or_else(|| "dev-tenant".to_owned());
            if resolved.is_empty() || resolved.len() > 128 {
                eprintln!("tenant identifier must be 1..128 bytes");
                return ExitCode::from(2);
            }
            resolved
        }
        other => {
            eprintln!(
                "application role {role_symbol} has unsupported tenant_scope: {}",
                other.unwrap_or("<missing>")
            );
            return ExitCode::from(2);
        }
    };

    let Ok(work_root_buf) = create_standalone_work_dir() else {
        eprintln!("riffdb dev could not create temporary workspace");
        return ExitCode::FAILURE;
    };
    let work_root = work_root_buf.as_path();
    let mut guard = StandaloneDevGuard {
        server_child: None,
        application_root: application_root.to_path_buf(),
        work_root: work_root_buf.clone(),
    };
    let backups_dir = work_root.join("backups");
    if fs::create_dir_all(&backups_dir).is_err() {
        eprintln!("riffdb dev could not create backup directory");
        return ExitCode::FAILURE;
    }

    let (Ok(cap_key), Ok(mut idem_key)) = (random_hex_32(), random_hex_32()) else {
        eprintln!("riffdb dev could not generate digest keys");
        return ExitCode::FAILURE;
    };
    while idem_key == cap_key {
        let Ok(next) = random_hex_32() else {
            eprintln!("riffdb dev could not generate digest keys");
            return ExitCode::FAILURE;
        };
        idem_key = next;
    }
    let cap_keys_path = work_root.join("capability.keys");
    let idem_keys_path = work_root.join("idempotency.keys");
    if write_private_file(
        &cap_keys_path,
        format!("riffdb-capability-digest-keys-v1\n1:{cap_key}\n").as_bytes(),
    )
    .is_err()
        || write_private_file(
            &idem_keys_path,
            format!("riffdb-idempotency-digest-keys-v1\n1:{idem_key}\n").as_bytes(),
        )
        .is_err()
    {
        eprintln!("riffdb dev could not write digest keys");
        return ExitCode::FAILURE;
    }

    let mut server_child = match ProcessCommand::new(&riffdbd_bin)
        .arg("--database")
        .arg(work_root.join("riffdb.redb"))
        .args([
            "--listen",
            "127.0.0.1:0",
            "--environment",
            "local-dev",
            "--audience",
            "riffdb-grpc-loopback",
            "--backup-root",
        ])
        .arg(&backups_dir)
        .arg("--capability-keys")
        .arg(&cap_keys_path)
        .arg("--idempotency-keys")
        .arg(&idem_keys_path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
    {
        Ok(child) => child,
        Err(_) => {
            eprintln!("riffdb dev could not spawn riffdbd");
            return ExitCode::FAILURE;
        }
    };
    let Some(server_stdout) = server_child.stdout.take() else {
        let _ = server_child.kill();
        let _ = server_child.wait();
        eprintln!("riffdb dev could not capture riffdbd readiness");
        return ExitCode::FAILURE;
    };
    guard.server_child = Some(server_child);
    let mut ready_line = String::new();
    if BufReader::new(server_stdout)
        .read_line(&mut ready_line)
        .is_err()
    {
        eprintln!("development server did not become ready");
        return ExitCode::FAILURE;
    }
    let trimmed = ready_line.trim_end_matches(['\r', '\n']);
    let Some(address) = trimmed.strip_prefix("riffdbd-ready-v1\t") else {
        eprintln!("development server returned invalid readiness");
        return ExitCode::FAILURE;
    };
    let endpoint = format!("http://{address}");

    let bootstrap_req_path = work_root.join("bootstrap-request.json");
    if write_private_file(
        &bootstrap_req_path,
        STANDALONE_DEV_BOOTSTRAP_REQUEST.as_bytes(),
    )
    .is_err()
    {
        eprintln!("riffdb dev could not write bootstrap request");
        return ExitCode::FAILURE;
    }
    let bootstrap_cred_path = work_root.join("bootstrap.credential");
    let operator_cred_path = work_root.join("operator.credential");
    if !run_cli_json_step(
        &riffdb_bin,
        application_root,
        &[
            OsStr::new("--endpoint"),
            OsStr::new(&endpoint),
            OsStr::new("--output"),
            OsStr::new("json"),
            OsStr::new("capability"),
            OsStr::new("bootstrap"),
            OsStr::new("--request"),
            bootstrap_req_path.as_os_str(),
            OsStr::new("--generate"),
            bootstrap_cred_path.as_os_str(),
            OsStr::new("--bearer-output"),
            operator_cred_path.as_os_str(),
        ],
    ) {
        eprintln!("riffdb dev capability bootstrap failed");
        return ExitCode::FAILURE;
    }

    let contract_path = application_root.join(contract_rel);
    if !run_cli_json_step(
        &riffdb_bin,
        application_root,
        &[
            OsStr::new("--endpoint"),
            OsStr::new(&endpoint),
            OsStr::new("--output"),
            OsStr::new("json"),
            OsStr::new("--credential-file"),
            operator_cred_path.as_os_str(),
            OsStr::new("contract"),
            OsStr::new("deploy"),
            contract_path.as_os_str(),
        ],
    ) {
        eprintln!("riffdb dev contract deploy failed");
        return ExitCode::FAILURE;
    }

    if let Some(first_module) = manifest["query_modules"]
        .as_array()
        .and_then(|modules| modules.first())
    {
        let module_name = first_module["name"].as_str().unwrap_or(application_name);
        let module_version = first_module["version"].as_u64().unwrap_or(1).to_string();
        let contract_version_str = contract_version.to_string();
        if let Some(queries) = first_module["queries"].as_array()
            && !queries.is_empty()
        {
            let staged_dir = work_root.join("query-module");
            if fs::create_dir_all(&staged_dir).is_err() {
                eprintln!("riffdb dev could not stage query module");
                return ExitCode::FAILURE;
            }
            for (index, entry) in queries.iter().enumerate() {
                let Some(rel) = entry["source"].as_str() else {
                    eprintln!("query module entry is missing source path");
                    return ExitCode::FAILURE;
                };
                if fs::copy(
                    application_root.join(rel),
                    staged_dir.join(format!("{index:04}.riffq")),
                )
                .is_err()
                {
                    eprintln!("riffdb dev could not copy query source: {rel}");
                    return ExitCode::FAILURE;
                }
            }
            if !run_cli_json_step(
                &riffdb_bin,
                application_root,
                &[
                    OsStr::new("--endpoint"),
                    OsStr::new(&endpoint),
                    OsStr::new("--output"),
                    OsStr::new("json"),
                    OsStr::new("--credential-file"),
                    operator_cred_path.as_os_str(),
                    OsStr::new("query"),
                    OsStr::new("deploy"),
                    staged_dir.as_os_str(),
                    OsStr::new("--module-name"),
                    OsStr::new(module_name),
                    OsStr::new("--module-version"),
                    OsStr::new(&module_version),
                    OsStr::new("--expected-active"),
                    OsStr::new("any"),
                    OsStr::new("--contract-lineage"),
                    OsStr::new(contract_lineage),
                    OsStr::new("--contract-version"),
                    OsStr::new(&contract_version_str),
                ],
            ) {
                eprintln!("riffdb dev query deploy failed");
                return ExitCode::FAILURE;
            }
        }
    }

    if manifest["reactive_modules"]
        .as_array()
        .is_some_and(|modules| !modules.is_empty())
        && !run_cli_json_step(
            &riffdb_bin,
            application_root,
            &[
                OsStr::new("--endpoint"),
                OsStr::new(&endpoint),
                OsStr::new("--output"),
                OsStr::new("json"),
                OsStr::new("--credential-file"),
                operator_cred_path.as_os_str(),
                OsStr::new("application"),
                OsStr::new("deploy"),
                OsStr::new("riffdb.application.json"),
                OsStr::new("--lock"),
                OsStr::new("riffdb.application.lock.json"),
            ],
        )
    {
        eprintln!("riffdb dev application deploy failed");
        return ExitCode::FAILURE;
    }

    let profile_cred_path = work_root.join("profile.credential");
    let principal = format!("app:{application_name}");
    let mut bind_args: Vec<&OsStr> = vec![
        OsStr::new("--endpoint"),
        OsStr::new(&endpoint),
        OsStr::new("--output"),
        OsStr::new("json"),
        OsStr::new("--credential-file"),
        operator_cred_path.as_os_str(),
        OsStr::new("role"),
        OsStr::new("bind"),
        exact_manifest_path.as_os_str(),
        OsStr::new("--role"),
        OsStr::new(role_symbol),
    ];
    if !effective_tenant.is_empty() {
        bind_args.push(OsStr::new("--tenant"));
        bind_args.push(OsStr::new(&effective_tenant));
    }
    bind_args.extend_from_slice(&[
        OsStr::new("--principal"),
        OsStr::new(&principal),
        OsStr::new("--actor-kind"),
        OsStr::new("service"),
        OsStr::new("--lifetime-seconds"),
        OsStr::new("86400"),
        OsStr::new("--audience"),
        OsStr::new("riffdb-grpc-loopback"),
        OsStr::new("--credential-output"),
        profile_cred_path.as_os_str(),
    ]);
    if !run_cli_json_step(&riffdb_bin, application_root, &bind_args) {
        eprintln!("riffdb dev role bind failed");
        return ExitCode::FAILURE;
    }

    if let Some(seed_dir_os) = options.seed_dir {
        let seed_dir_path = Path::new(seed_dir_os);
        let resolved_seed_dir = if seed_dir_path.is_absolute() {
            seed_dir_path.to_path_buf()
        } else {
            application_root.join(seed_dir_path)
        };
        if !run_standalone_seed_dir(
            &riffdb_bin,
            application_root,
            &source_path,
            &endpoint,
            &profile_cred_path,
            work_root,
            &resolved_seed_dir,
            options.seed_concurrency,
        ) {
            return ExitCode::FAILURE;
        }
    }
    if options.seed || options.acceptance {
        let Some(seed_inputs) = manifest["seed_inputs"].as_array() else {
            eprintln!("application manifest contains no seed inputs");
            return ExitCode::FAILURE;
        };
        if seed_inputs.is_empty() {
            eprintln!("application manifest contains no seed inputs");
            return ExitCode::FAILURE;
        }
        for (idx, entry) in seed_inputs.iter().enumerate() {
            let Some(rel) = entry.as_str() else {
                eprintln!("seed input path is invalid");
                return ExitCode::FAILURE;
            };
            let seed_file = application_root.join(rel);
            if !run_standalone_seed_file(
                &riffdb_bin,
                application_root,
                &source_path,
                &endpoint,
                &profile_cred_path,
                work_root,
                &seed_file,
                &format!("manifest-{}", idx + 1),
                options.seed_concurrency,
            ) {
                return ExitCode::FAILURE;
            }
        }
        eprintln!(
            "riffdb-dev-seed-v1\t{}\tapplication-manifest",
            seed_inputs.len()
        );
    }

    if publish_dev_session_files(
        application_root,
        &profile_cred_path,
        &endpoint,
        contract_lineage,
        contract_version,
        role_symbol,
        &effective_tenant,
    )
    .is_err()
    {
        eprintln!("riffdb dev could not publish .riffdb/dev session files");
        return ExitCode::FAILURE;
    }

    println!("riffdb-dev-context-v1\tdatabase=default\trole={role_symbol}");
    println!(
        "riffdb-dev-ready-v1\t{endpoint}\t{}\t{role_symbol}",
        profile_cred_path.display()
    );
    let _ = io::stdout().flush();

    if let Some(exec_command) = options.exec {
        let mut cmd = ProcessCommand::new("sh");
        cmd.arg("-c")
            .arg(exec_command)
            .current_dir(application_root)
            .env("RIFFDB_DEV_ROOT", work_root)
            .env("RIFFDB_ENDPOINT", &endpoint)
            .env("RIFFDB_DATABASE", "default")
            .env("RIFFDB_CREDENTIAL_FILE", &profile_cred_path)
            .env("RIFFDB_TENANT", &effective_tenant);
        return match cmd.status() {
            Ok(status) if status.success() => ExitCode::SUCCESS,
            Ok(status) => ExitCode::from(
                status
                    .code()
                    .and_then(|code| u8::try_from(code).ok())
                    .unwrap_or(1),
            ),
            Err(_) => ExitCode::FAILURE,
        };
    }

    if options.run {
        if application_root.join("Cargo.toml").is_file() {
            let mut cmd = ProcessCommand::new("cargo");
            cmd.args(["run", "--quiet", "--manifest-path"])
                .arg(application_root.join("Cargo.toml"))
                .arg("--")
                .arg(&endpoint)
                .arg(&profile_cred_path)
                .arg("default")
                .current_dir(application_root)
                .env("RIFFDB_DEV_ROOT", work_root)
                .env("RIFFDB_ENDPOINT", &endpoint)
                .env("RIFFDB_DATABASE", "default")
                .env("RIFFDB_CREDENTIAL_FILE", &profile_cred_path)
                .env("RIFFDB_TENANT", &effective_tenant);
            return match cmd.status() {
                Ok(status) if status.success() => ExitCode::SUCCESS,
                Ok(status) => ExitCode::from(
                    status
                        .code()
                        .and_then(|code| u8::try_from(code).ok())
                        .unwrap_or(1),
                ),
                Err(_) => ExitCode::FAILURE,
            };
        }
        if application_root.join("pyproject.toml").is_file() {
            let pkg = application_name.replace('-', "_");
            let mut cmd = ProcessCommand::new("python3");
            cmd.args(["-m", &pkg])
                .arg(&endpoint)
                .arg(&profile_cred_path)
                .arg("default")
                .current_dir(application_root)
                .env("PYTHONPATH", "src")
                .env("RIFFDB_DEV_ROOT", work_root)
                .env("RIFFDB_ENDPOINT", &endpoint)
                .env("RIFFDB_DATABASE", "default")
                .env("RIFFDB_CREDENTIAL_FILE", &profile_cred_path)
                .env("RIFFDB_TENANT", &effective_tenant);
            return match cmd.status() {
                Ok(status) if status.success() => ExitCode::SUCCESS,
                Ok(status) => ExitCode::from(
                    status
                        .code()
                        .and_then(|code| u8::try_from(code).ok())
                        .unwrap_or(1),
                ),
                Err(_) => ExitCode::FAILURE,
            };
        }
        eprintln!(
            "standalone riffdb dev --run supports Cargo.toml or pyproject.toml, or use --exec \"<command>\""
        );
        return ExitCode::from(2);
    }

    if options.acceptance {
        return ExitCode::SUCCESS;
    }

    if let Some(mut child) = guard.server_child.take() {
        let _ = child.wait();
    }
    ExitCode::SUCCESS
}

fn lock_and_generate_for_dev(
    application_root: &Path,
    source_path: &Path,
    options: &StandaloneDevOptions<'_>,
) -> Result<(), &'static str> {
    let project_toml = application_root.join(DEFAULT_PROJECT_FILE);
    if project_toml.is_file() {
        let project = load(&project_toml).map_err(|_| "riffdb.toml is invalid")?;
        if options.sync_manifest || options.sync_role.is_some() {
            crate::scaffold::sync_application_manifest(project.schema(), options.sync_role)
                .map_err(|_| "application manifest sync failed")?;
        }
        let selected = project
            .generators()
            .iter()
            .map(|target| target.surface().artifact_kind())
            .collect::<Vec<_>>();
        crate::scaffold::generate_project_application_offline(project.schema(), &selected)
            .map(|_| ())
            .map_err(|_| "offline project generation failed")
    } else {
        if options.sync_manifest || options.sync_role.is_some() {
            crate::scaffold::sync_application_manifest(source_path, options.sync_role)
                .map_err(|_| "application manifest sync failed")?;
        }
        crate::scaffold::write_application_lock(source_path, None)
            .map_err(|_| "application lock compilation failed")?;
        crate::scaffold::generate_application(source_path, true, None)
            .map_err(|_| "application binding generation failed")
    }
}

fn resolve_standalone_binary(
    env_var: &str,
    binary_name: &str,
    current_executable: Option<&Path>,
) -> Option<PathBuf> {
    if let Some(configured) = std::env::var_os(env_var) {
        let candidate = PathBuf::from(configured);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    if let Some(exe) = current_executable {
        if exe.file_name() == Some(OsStr::new(binary_name)) && exe.is_file() {
            return Some(exe.to_path_buf());
        }
        if let Some(dir) = exe.parent() {
            let sibling = dir.join(binary_name);
            if sibling.is_file() {
                return Some(sibling);
            }
            if let Some(parent_dir) = dir.parent() {
                let parent_sibling = parent_dir.join(binary_name);
                if parent_sibling.is_file() {
                    return Some(parent_sibling);
                }
            }
        }
    }
    let path_var = std::env::var_os("PATH")?;
    std::env::split_paths(&path_var)
        .map(|dir| dir.join(binary_name))
        .find(|candidate| candidate.is_file())
}

fn create_standalone_work_dir() -> io::Result<PathBuf> {
    let base = std::env::temp_dir();
    for _ in 0..16 {
        let suffix = random_hex_32()?;
        let candidate = base.join(format!("riffdb-dev-{}", &suffix[..16]));
        match DirBuilder::new().mode(0o700).create(&candidate) {
            Ok(()) => return Ok(candidate),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "unable to allocate unique riffdb-dev directory",
    ))
}

fn random_hex_32() -> io::Result<String> {
    let mut file = fs::File::open("/dev/urandom")?;
    let mut bytes = [0u8; 32];
    file.read_exact(&mut bytes)?;
    let mut out = String::with_capacity(64);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    Ok(out)
}

fn write_private_file(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()
}

fn run_cli_json_step(riffdb_bin: &Path, cwd: &Path, args: &[&OsStr]) -> bool {
    let Ok(output) = ProcessCommand::new(riffdb_bin)
        .args(args)
        .current_dir(cwd)
        .output()
    else {
        return false;
    };
    if !output.status.success() {
        let _ = io::stderr().write_all(&output.stdout);
        let _ = io::stderr().write_all(&output.stderr);
        return false;
    }
    serde_json::from_slice::<serde_json::Value>(&output.stdout)
        .ok()
        .is_some_and(|json| json["ok"].as_bool() == Some(true))
}

#[allow(clippy::too_many_arguments)]
fn run_standalone_seed_dir(
    riffdb_bin: &Path,
    application_root: &Path,
    source_path: &Path,
    endpoint: &str,
    profile_cred_path: &Path,
    work_root: &Path,
    seed_dir: &Path,
    seed_concurrency: &str,
) -> bool {
    let Ok(entries) = fs::read_dir(seed_dir) else {
        eprintln!("seed directory is unavailable");
        return false;
    };
    let mut files = entries
        .filter_map(|entry| entry.ok().map(|item| item.path()))
        .filter(|path| path.is_file() && path.extension() == Some(OsStr::new("jsonl")))
        .collect::<Vec<_>>();
    files.sort();
    if files.is_empty() {
        eprintln!("seed directory contains no JSONL command batches");
        return false;
    }
    for (idx, file) in files.iter().enumerate() {
        if !run_standalone_seed_file(
            riffdb_bin,
            application_root,
            source_path,
            endpoint,
            profile_cred_path,
            work_root,
            file,
            &format!("external-{}", idx + 1),
            seed_concurrency,
        ) {
            return false;
        }
    }
    true
}

#[allow(clippy::too_many_arguments)]
fn run_standalone_seed_file(
    riffdb_bin: &Path,
    application_root: &Path,
    source_path: &Path,
    endpoint: &str,
    profile_cred_path: &Path,
    work_root: &Path,
    seed_file: &Path,
    namespace: &str,
    seed_concurrency: &str,
) -> bool {
    if !seed_file.is_file() {
        eprintln!("seed input is unavailable: {}", seed_file.display());
        return false;
    }
    let Some(stem) = seed_file.file_stem().and_then(OsStr::to_str) else {
        eprintln!("seed input must be named NN-CommandName.jsonl");
        return false;
    };
    let command_name = stem.split_once('-').map_or(stem, |(_, cmd)| cmd);
    if command_name.is_empty() {
        eprintln!("seed input must be named NN-CommandName.jsonl");
        return false;
    }
    let checkpoint = work_root.join(format!("seed-{namespace}.checkpoint.json"));
    run_cli_json_step(
        riffdb_bin,
        application_root,
        &[
            OsStr::new("--endpoint"),
            OsStr::new(endpoint),
            OsStr::new("--output"),
            OsStr::new("json"),
            OsStr::new("--credential-file"),
            profile_cred_path.as_os_str(),
            OsStr::new("command"),
            OsStr::new("batch"),
            OsStr::new(command_name),
            seed_file.as_os_str(),
            OsStr::new("--application"),
            source_path.as_os_str(),
            OsStr::new("--concurrency"),
            OsStr::new(seed_concurrency),
            OsStr::new("--checkpoint"),
            checkpoint.as_os_str(),
        ],
    )
}

fn publish_dev_session_files(
    application_root: &Path,
    profile_cred_path: &Path,
    endpoint: &str,
    contract_lineage: &str,
    contract_version: u64,
    role_symbol: &str,
    effective_tenant: &str,
) -> io::Result<()> {
    let dot_riffdb = application_root.join(".riffdb");
    let dev_dir = dot_riffdb.join("dev");
    if fs::symlink_metadata(&dot_riffdb)
        .map(|m| m.file_type().is_symlink())
        .unwrap_or(false)
        || fs::symlink_metadata(&dev_dir)
            .map(|m| m.file_type().is_symlink())
            .unwrap_or(false)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            ".riffdb/dev cannot be a symlink",
        ));
    }
    fs::create_dir_all(&dev_dir)?;
    fs::set_permissions(&dot_riffdb, fs::Permissions::from_mode(0o700))?;
    fs::set_permissions(&dev_dir, fs::Permissions::from_mode(0o700))?;
    let cred_bytes = fs::read(profile_cred_path)?;
    write_private_file(&dev_dir.join("profile.credential"), &cred_bytes)?;
    let mut session = serde_json::json!({
        "schema": "riffdb.dev-session/v1",
        "endpoint": endpoint,
        "database": "default",
        "credential_file": ".riffdb/dev/profile.credential",
        "contract_lineage": contract_lineage,
        "contract_version": contract_version,
        "role": role_symbol,
    });
    if !effective_tenant.is_empty()
        && let Some(obj) = session.as_object_mut()
    {
        obj.insert(
            "tenant".to_owned(),
            serde_json::Value::String(effective_tenant.to_owned()),
        );
    }
    let session_bytes = serde_json::to_vec_pretty(&session)?;
    write_private_file(&dev_dir.join("session.json"), &session_bytes)
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
            false,
        )
        .expect("first init");
        let second = initialize(
            scratch.path(),
            Path::new(DEFAULT_PROJECT_FILE),
            Some("inventory"),
            None,
            None,
            &[ApplicationGenerator::Typescript, ApplicationGenerator::Rust],
            false,
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
            false,
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
            false,
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
                false,
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

    // req: DX-013, DX-014, DX-016, DX-046
    #[test]
    fn init_starter_scaffolds_contract_query_and_role_and_generates_offline() {
        let scratch =
            tempfile::TempDir::with_prefix("riffdb-project-starter-").expect("scratch directory");
        let first = initialize(
            scratch.path(),
            Path::new(DEFAULT_PROJECT_FILE),
            Some("inventory"),
            None,
            None,
            &[ApplicationGenerator::Typescript],
            true,
        )
        .expect("starter init");
        let second = initialize(
            scratch.path(),
            Path::new(DEFAULT_PROJECT_FILE),
            Some("inventory"),
            None,
            None,
            &[ApplicationGenerator::Typescript],
            true,
        )
        .expect("idempotent starter init");
        assert_eq!(first, second);
        assert!(scratch.path().join("riffdb.toml").is_file());
        assert!(scratch.path().join("riffdb.application.json").is_file());
        assert!(scratch.path().join("riffdb/contract.riff").is_file());
        assert!(
            scratch
                .path()
                .join("riffdb/queries/item_page.riffq")
                .is_file()
        );
        let contract = fs::read_to_string(scratch.path().join("riffdb/contract.riff"))
            .expect("starter contract");
        assert!(contract.contains("entity Item"));
        assert!(contract.contains("command CreateItem"));
        let source_bytes = fs::read(first.schema()).expect("source bytes");
        let source =
            riffdb_query_module::ApplicationSourceManifest::decode_canonical(&source_bytes)
                .expect("canonical source");
        assert_eq!(source.roles().len(), 1);
        assert_eq!(source.roles()[0].name(), "InventoryApplication");
        crate::scaffold::generate_project_application(
            first.schema(),
            &[GeneratedApplicationArtifactKind::TypeScript],
        )
        .expect("offline generate from starter project");
        assert!(
            scratch
                .path()
                .join("riffdb.application.lock.json")
                .is_file()
        );
        assert!(
            scratch
                .path()
                .join("generated/typescript/client.ts")
                .is_file()
        );
    }

    // req: DX-004, DX-005, DX-014, DX-046
    #[test]
    fn standalone_dev_supervisor_boots_ephemeral_server_and_executes_command() {
        let scratch = tempfile::TempDir::with_prefix("riffdb-standalone-dev-test-")
            .expect("scratch directory");
        let app_root = scratch.path().join("inventory");
        fs::create_dir_all(&app_root).expect("create app root");
        initialize(
            &app_root,
            Path::new(DEFAULT_PROJECT_FILE),
            Some("inventory"),
            None,
            None,
            &[ApplicationGenerator::Typescript],
            true,
        )
        .expect("initialize starter project");

        let bin_dir = scratch.path().join("bin");
        fs::create_dir_all(&bin_dir).expect("create bin dir");
        let mock_riffdbd = bin_dir.join("riffdbd");
        let mock_riffdb = bin_dir.join("riffdb");
        let riffdbd_script = r#"#!/usr/bin/env sh
printf 'riffdbd-ready-v1\t127.0.0.1:47443\n'
fifo="$(dirname "$0")/riffdbd-hold.fifo"
mkfifo "$fifo" 2>/dev/null || true
read _ < "$fifo"
"#;
        let riffdb_script = r#"#!/usr/bin/env sh
out=""
prev=""
for arg in "$@"; do
    case "$prev" in
        --generate|--bearer-output|--credential-output)
            printf 'test-token\n' > "$arg"
            chmod 600 "$arg"
            ;;
    esac
    prev="$arg"
done
printf '{"ok":true}\n'
"#;
        fs::write(&mock_riffdbd, riffdbd_script).expect("write mock riffdbd");
        fs::set_permissions(&mock_riffdbd, fs::Permissions::from_mode(0o755))
            .expect("chmod mock riffdbd");
        fs::write(&mock_riffdb, riffdb_script).expect("write mock riffdb");
        fs::set_permissions(&mock_riffdb, fs::Permissions::from_mode(0o755))
            .expect("chmod mock riffdb");

        let marker = app_root.join("exec-output.json");
        let exec_cmd = format!(
            "test -f .riffdb/dev/session.json && test -f .riffdb/dev/profile.credential && cp .riffdb/dev/session.json {}",
            marker.display()
        );
        let status = run_standalone_dev_with_binaries(
            &app_root,
            &mock_riffdbd,
            &mock_riffdb,
            &StandaloneDevOptions {
                role: "application",
                tenant: None,
                sync_manifest: false,
                sync_role: None,
                watch: false,
                run: false,
                exec: Some(&exec_cmd),
                seed: false,
                seed_dir: None,
                seed_concurrency: "4",
                acceptance: false,
            },
        );
        assert_eq!(status, ExitCode::SUCCESS);
        assert!(app_root.join("riffdb.application.lock.json").is_file());
        assert!(app_root.join("generated/typescript/client.ts").is_file());
        assert!(marker.is_file());
        let session: serde_json::Value =
            serde_json::from_slice(&fs::read(&marker).expect("read captured session"))
                .expect("parse captured session");
        assert_eq!(session["schema"], "riffdb.dev-session/v1");
        assert_eq!(session["endpoint"], "http://127.0.0.1:47443");
        assert_eq!(session["database"], "default");
        assert_eq!(session["role"], "InventoryApplication");
        assert!(!app_root.join(".riffdb/dev/session.json").exists());
        assert!(!app_root.join(".riffdb/dev/profile.credential").exists());
    }
}
