//! Deterministic, application-only repository scaffolding.

use std::fmt;
use std::fs;
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};

use riffdb_contract_compiler::{compile_contract_source, compile_migration_source};
use riffdb_contract_ir::ContractBundle;
use riffdb_diagnostics::{
    AuthoringDiagnostics, AuthoringSourcePath, FileChangeDisposition, FilesystemDiagnosticClass,
};
use riffdb_query_module::{
    ApplicationLock, ApplicationManifest, ApplicationMigrationLockInput, ApplicationSourceManifest,
    ApplicationSourceTenantScope, CONTRACT_BUNDLE_ARTIFACT_PATH, EXACT_MANIFEST_ARTIFACT_PATH,
    GeneratedApplicationArtifact, GeneratedApplicationArtifactKind, GeneratedMcpCommand,
    GeneratedMcpReactiveTool, GeneratedMcpTool, GeneratedVectorInspectionTool, NamedQuerySource,
    PythonGenerationError, QueryModule, QueryModuleCandidate, QueryModuleName, QueryModuleVersion,
    ReactiveModulePlanV1, compile_application_role, compile_application_role_v2,
    compile_reactive_source, generate_go_application_client, generate_mcp_commands,
    generate_mcp_reactive_tools, generate_mcp_tools, generate_python_application_client,
    generate_python_client, generate_rust_application_client, generate_rust_client,
    generate_sdk_only_query_tools, generate_typescript_application_client,
    generate_typescript_client, generate_vector_inspection_tools,
};
use riffdb_types::{TenantId, hash_generated_artifact, hash_source};
use serde_json::json;

const CONTRACT_TEMPLATE: &str = include_str!("../assets/scaffold/application/contract.riff");
const QUERY_TEMPLATE: &str = include_str!("../assets/scaffold/application/item_page.riffq");
const SEED_TEMPLATE: &str = include_str!("../assets/scaffold/application/seed.jsonl");
const README_TEMPLATE: &str = include_str!("../assets/scaffold/application/README.md");
const AUTHORING_TEMPLATE: &str = include_str!("../assets/scaffold/application/AUTHORING.md");
const RUST_MAIN_TEMPLATE: &str = include_str!("../assets/scaffold/application/rust-main.rs");
const TYPESCRIPT_MAIN_TEMPLATE: &str =
    include_str!("../assets/scaffold/application/typescript-main.ts");
const GO_MAIN_TEMPLATE: &str = include_str!("../assets/scaffold/application/go-main.go");
const GO_MOD_TEMPLATE: &str = include_str!("../assets/scaffold/application/go.mod");
const GO_RUNTIME_SOURCE: &str = include_str!("../assets/scaffold/go-runtime/runtime.go");
const GO_RUNTIME_MOD: &str = include_str!("../assets/scaffold/go-runtime/go.mod");
const PYTHON_MAIN_TEMPLATE: &str = include_str!("../assets/scaffold/application/python-main.py");
const PYPROJECT_TEMPLATE: &str = include_str!("../assets/scaffold/application/pyproject.toml");
const CARGO_TEMPLATE: &str = include_str!("../assets/scaffold/application/Cargo.toml.asset");
const CARGO_LOCK_TEMPLATE: &str = include_str!("../assets/scaffold/application/Cargo.lock");
const PACKAGE_TEMPLATE: &str = include_str!("../assets/scaffold/application/package.json");
const PACKAGE_LOCK_BASE: &str =
    include_str!("../assets/scaffold/typescript-runtime/package-lock.json");
const TSCONFIG_TEMPLATE: &str = include_str!("../assets/scaffold/application/tsconfig.json");
const GITIGNORE_TEMPLATE: &str = include_str!("../assets/scaffold/application/gitignore");
const TYPESCRIPT_RUNTIME_JS: &str =
    include_str!("../assets/scaffold/typescript-runtime/dist/index.js");
const TYPESCRIPT_RUNTIME_TYPES: &str =
    include_str!("../assets/scaffold/typescript-runtime/dist/index.d.ts");
const TYPESCRIPT_DRIVER_JS: &str =
    include_str!("../assets/scaffold/typescript-runtime/dist/driver.js");
const TYPESCRIPT_DRIVER_TYPES: &str =
    include_str!("../assets/scaffold/typescript-runtime/dist/driver.d.ts");
const MAX_APPLICATION_NAME_BYTES: usize = 64;
const MAX_SCAFFOLD_TOP_LEVEL_ENTRIES: usize = 16;
const MAX_TYPESCRIPT_TOOLCHAIN_FILES: usize = 16_384;
const MAX_TYPESCRIPT_TOOLCHAIN_BYTES: u64 = 128 * 1_024 * 1_024;
const EXACT_MANIFEST_PATH: &str = EXACT_MANIFEST_ARTIFACT_PATH;
const DEFAULT_LOCK_PATH: &str = "riffdb.application.lock.json";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScaffoldLanguage {
    Rust,
    Go,
    Typescript,
    Python,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PinnedLockRefresh {
    Refreshed,
    ContractSourceChanged,
    NotPinned,
}

pub(crate) enum PinnedLockPreview {
    Proposed(Box<ApplicationLockPreview>),
    ContractSourceChanged,
    NotPinned,
}

pub(crate) struct ApplicationLockPreview {
    canonical_lock: Vec<u8>,
    contract: ContractBundle,
}

impl ApplicationLockPreview {
    pub(crate) fn identity(&self) -> riffdb_types::ApplicationLockHash {
        ApplicationLock::decode_canonical(&self.canonical_lock)
            .expect("compiler-produced application lock is canonical")
            .identity()
    }

    pub(crate) fn into_contract(self) -> ContractBundle {
        self.contract
    }
}

#[derive(Debug)]
pub(crate) enum ScaffoldError {
    InvalidApplicationName,
    CompileContract,
    CompileQuery,
    CompileReactive,
    CompileRole,
    Manifest,
    ApplicationSource,
    ApplicationLock,
    LockRequired,
    IdentityMismatch,
    SourceLimit,
    GenerateMcp,
    GeneratePython,
    RustDependencyLock,
    PythonWheelUnavailable,
    UnsafePath,
    Authoring(AuthoringDiagnostics),
    Io(io::Error),
}

impl fmt::Display for ScaffoldError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidApplicationName => {
                "application name must be lower-case ASCII with letters, digits, and hyphens"
            }
            Self::CompileContract => "the built-in application contract did not compile",
            Self::CompileQuery => "the built-in application query did not compile",
            Self::CompileReactive => "the application reactive module did not compile",
            Self::CompileRole => "the built-in application role did not compile",
            Self::Manifest => "the generated application manifest is invalid",
            Self::ApplicationSource => "the symbolic application source is invalid",
            Self::ApplicationLock => "the compiler-owned application lock is invalid",
            Self::LockRequired => {
                "symbolic application generation requires --locked and an exact lock"
            }
            Self::IdentityMismatch => {
                "application source no longer matches its pinned manifest identity"
            }
            Self::SourceLimit => "an application source exceeds the bounded compiler input limit",
            Self::GenerateMcp => "the generated MCP application surface is invalid",
            Self::GeneratePython => "the generated Python application surface is invalid",
            Self::RustDependencyLock => "the built-in Rust dependency lock is invalid",
            Self::PythonWheelUnavailable => {
                "a matching riffdb-application wheel is not installed; set RIFFDB_APPLICATION_WHEEL"
            }
            Self::UnsafePath => "an application path escapes the workspace or traverses a symlink",
            Self::Authoring(diagnostics) => {
                return formatter.write_str(
                    &diagnostics
                        .render_human()
                        .unwrap_or_else(|_| "authoring diagnostic rendering failed\n".to_owned()),
                );
            }
            Self::Io(_) => "the application repository could not be written",
        })
    }
}

pub(crate) fn generate_application(
    manifest_path: &Path,
    locked: bool,
    lock_path: Option<&Path>,
) -> Result<(), ScaffoldError> {
    let bytes = read_bounded(manifest_path, 1_048_576)?;
    let value: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|_| ScaffoldError::Manifest)?;
    match value.get("schema").and_then(serde_json::Value::as_str) {
        Some("riffdb.application-manifest/v1") if !locked => {
            generate_legacy_application(manifest_path)
        }
        Some(
            "riffdb.application-source/v1"
            | "riffdb.application-source/v2"
            | "riffdb.application-source/v3"
            | "riffdb.application-source/v4"
            | "riffdb.application-source/v5"
            | "riffdb.application-source/v6"
            | "riffdb.application-source/v7",
        ) if locked => generate_application_locked(
            manifest_path,
            lock_path.unwrap_or_else(|| Path::new(DEFAULT_LOCK_PATH)),
        ),
        Some(
            "riffdb.application-source/v1"
            | "riffdb.application-source/v2"
            | "riffdb.application-source/v3"
            | "riffdb.application-source/v4"
            | "riffdb.application-source/v5"
            | "riffdb.application-source/v6"
            | "riffdb.application-source/v7",
        ) => Err(ScaffoldError::LockRequired),
        _ => Err(ScaffoldError::Manifest),
    }
}

pub(crate) fn generate_project_application(
    source_path: &Path,
    selected: &[GeneratedApplicationArtifactKind],
) -> Result<(), ScaffoldError> {
    let root = source_parent(source_path);
    let lock_path = workspace_lock_path(root, None)?;
    let lock = ApplicationLock::decode_canonical(&read_bounded(
        &lock_path,
        riffdb_query_module::MAX_APPLICATION_LOCK_BYTES,
    )?)
    .map_err(|error| lock_diagnostic(&lock_path, error.kind()))?;
    let compiled = compile_for_existing_lock(source_path, root, &lock_path, &lock)?;
    check_project_compilation(root, &lock, &compiled, selected, true)?;
    for (path, bytes) in &compiled.outputs {
        if compiled_output_kind(&compiled, path).is_some_and(|kind| selected.contains(&kind)) {
            atomic_write_workspace(root, path, bytes)?;
        }
    }
    Ok(())
}

fn generate_legacy_application(manifest_path: &Path) -> Result<(), ScaffoldError> {
    let manifest_bytes = read_bounded(manifest_path, 1_048_576)?;
    let manifest = ApplicationManifest::decode_canonical(&manifest_bytes)
        .map_err(|_| ScaffoldError::Manifest)?;
    let root = source_parent(manifest_path);
    let contract_source = read_bounded_text(&root.join(manifest.contract().source()), 1_048_576)?;
    let contract =
        compile_contract_source(&contract_source).map_err(|_| ScaffoldError::CompileContract)?;
    if contract.lineage().as_str() != manifest.contract().lineage()
        || contract.contract_version().get() != manifest.contract().version()
        || contract.bundle_hash() != manifest.contract().bundle_hash()
    {
        return Err(ScaffoldError::IdentityMismatch);
    }
    let [module_manifest] = manifest.query_modules() else {
        return Err(ScaffoldError::Manifest);
    };
    let mut queries = Vec::with_capacity(module_manifest.queries().len());
    for query in module_manifest.queries() {
        let source = read_bounded_text(&root.join(query.source()), 1_048_576)?;
        queries.push(
            NamedQuerySource::new(query.name(), source).map_err(|_| ScaffoldError::CompileQuery)?,
        );
    }
    let candidate = QueryModuleCandidate::new(
        QueryModuleName::new(module_manifest.name()).map_err(|_| ScaffoldError::CompileQuery)?,
        QueryModuleVersion::new(module_manifest.version()).ok_or(ScaffoldError::CompileQuery)?,
        queries,
    )
    .map_err(|_| ScaffoldError::CompileQuery)?;
    let module =
        QueryModule::compile(candidate, &contract).map_err(|_| ScaffoldError::CompileQuery)?;
    if module.identity() != module_manifest.module_hash() {
        return Err(ScaffoldError::IdentityMismatch);
    }
    for role in manifest.roles() {
        compile_application_role(
            &manifest,
            role.name(),
            None,
            &contract,
            std::slice::from_ref(&module),
        )
        .map_err(|_| ScaffoldError::CompileRole)?;
    }
    let tools = generate_mcp_tools(&module).map_err(|_| ScaffoldError::GenerateMcp)?;
    let sdk_tools =
        generate_sdk_only_query_tools(&module).map_err(|_| ScaffoldError::GenerateMcp)?;
    let commands =
        generate_mcp_commands(&module, &contract).map_err(|_| ScaffoldError::GenerateMcp)?;
    let vector_tools = generate_vector_inspection_tools(&module, &contract)
        .map_err(|_| ScaffoldError::GenerateMcp)?;
    let generated_mcp =
        render_mcp_manifest(&manifest, &tools, &sdk_tools, &commands, &[], &vector_tools)?;
    write_file(
        root,
        manifest
            .generation()
            .rust()
            .ok_or(ScaffoldError::Manifest)?,
        generate_rust_client(&module, &contract).as_bytes(),
    )?;
    write_file(
        root,
        manifest
            .generation()
            .typescript()
            .ok_or(ScaffoldError::Manifest)?,
        generate_typescript_client(&module, &contract).as_bytes(),
    )?;
    write_file(
        root,
        manifest.generation().mcp().ok_or(ScaffoldError::Manifest)?,
        generated_mcp.as_bytes(),
    )?;
    Ok(())
}

struct CompiledSymbolicApplication {
    lock: ApplicationLock,
    outputs: Vec<(String, Vec<u8>)>,
    runtime_operation_catalog: Option<Vec<u8>>,
}

pub(crate) struct LockedApplication {
    root: PathBuf,
    manifest_path: PathBuf,
    manifest: ApplicationManifest,
    lock: ApplicationLock,
    contract: ContractBundle,
}

impl LockedApplication {
    pub(crate) fn root(&self) -> &Path {
        &self.root
    }

    pub(crate) fn manifest_path(&self) -> &Path {
        &self.manifest_path
    }

    pub(crate) const fn manifest(&self) -> &ApplicationManifest {
        &self.manifest
    }

    pub(crate) const fn lock(&self) -> &ApplicationLock {
        &self.lock
    }

    pub(crate) const fn lock_identity(&self) -> riffdb_types::ApplicationLockHash {
        self.lock.identity()
    }

    pub(crate) const fn contract(&self) -> &ContractBundle {
        &self.contract
    }
}

fn source_parent(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ApplicationCheckStatus {
    SourceOnly { seed_input_count: usize },
    ExactLock { seed_input_count: usize },
}

fn application_seed_input_count(source_path: &Path) -> Result<usize, ScaffoldError> {
    let source_bytes = read_bounded(source_path, 1_048_576)?;
    let source_text =
        std::str::from_utf8(&source_bytes).map_err(|_| ScaffoldError::ApplicationSource)?;
    let source = ApplicationSourceManifest::parse(source_text)
        .map_err(|error| application_source_diagnostic(source_path, error.kind()))?;
    Ok(source.seed_inputs().len())
}

pub(crate) fn check_application(
    source_path: &Path,
) -> Result<ApplicationCheckStatus, ScaffoldError> {
    let root = source_parent(source_path);
    if root.join(DEFAULT_LOCK_PATH).exists() {
        check_application_lock(source_path, None)?;
        Ok(ApplicationCheckStatus::ExactLock {
            seed_input_count: application_seed_input_count(source_path)?,
        })
    } else {
        let _ = compile_symbolic_application(source_path)?;
        Ok(ApplicationCheckStatus::SourceOnly {
            seed_input_count: application_seed_input_count(source_path)?,
        })
    }
}

/// Compiles only author-owned symbolic sources and application roles.
///
/// This is the explicit iterative-authoring path. It intentionally does not
/// compare or mutate the compiler-owned lock and generated artifacts.
pub(crate) fn check_application_sources(
    source_path: &Path,
) -> Result<ApplicationCheckStatus, ScaffoldError> {
    let _ = compile_symbolic_application(source_path)?;
    Ok(ApplicationCheckStatus::SourceOnly {
        seed_input_count: application_seed_input_count(source_path)?,
    })
}

pub(crate) fn preview_application_lock(source_path: &Path) -> Result<Vec<u8>, ScaffoldError> {
    if application_contract_version(source_path)? != 1 {
        return Err(ScaffoldError::IdentityMismatch);
    }
    Ok(compile_symbolic_application(source_path)?
        .lock
        .canonical_bytes()
        .to_vec())
}

pub(crate) fn preview_application_lock_with_bundle(
    source_path: &Path,
    contract: ContractBundle,
) -> Result<ApplicationLockPreview, ScaffoldError> {
    let compiled = compile_symbolic_application_with_bundle(source_path, contract)?;
    let contract = compiled_contract_bundle(&compiled)?;
    Ok(ApplicationLockPreview {
        canonical_lock: compiled.lock.canonical_bytes().to_vec(),
        contract,
    })
}

pub(crate) fn preview_genesis_application_lock(
    source_path: &Path,
) -> Result<ApplicationLockPreview, ScaffoldError> {
    if application_contract_version(source_path)? != 1 {
        return Err(ScaffoldError::IdentityMismatch);
    }
    let compiled = compile_symbolic_application(source_path)?;
    let contract = compiled_contract_bundle(&compiled)?;
    Ok(ApplicationLockPreview {
        canonical_lock: compiled.lock.canonical_bytes().to_vec(),
        contract,
    })
}

pub(crate) fn application_lock_identity(
    source_path: &Path,
    lock_path: Option<&Path>,
) -> Result<Option<riffdb_types::ApplicationLockHash>, ScaffoldError> {
    let root = source_parent(source_path);
    let lock_path = workspace_lock_path(root, lock_path)?;
    let bytes = match read_bounded(&lock_path, riffdb_query_module::MAX_APPLICATION_LOCK_BYTES) {
        Ok(bytes) => bytes,
        Err(ScaffoldError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    let lock = ApplicationLock::decode_canonical(&bytes)
        .map_err(|error| lock_diagnostic(&lock_path, error.kind()))?;
    Ok(Some(lock.identity()))
}

pub(crate) fn preview_application_lock_from_pinned_bundle(
    source_path: &Path,
    lock_path: Option<&Path>,
) -> Result<PinnedLockPreview, ScaffoldError> {
    let root = source_parent(source_path);
    let absolute_lock_path = workspace_lock_path(root, lock_path)?;
    let existing = match read_bounded(&absolute_lock_path, 4 * 1_024 * 1_024) {
        Ok(existing) => existing,
        Err(ScaffoldError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(PinnedLockPreview::NotPinned);
        }
        Err(error) => return Err(error),
    };
    let lock = ApplicationLock::decode_canonical(&existing)
        .map_err(|error| lock_diagnostic(&absolute_lock_path, error.kind()))?;
    if !matches!(
        lock.schema(),
        riffdb_query_module::APPLICATION_LOCK_SCHEMA_V3
            | riffdb_query_module::APPLICATION_LOCK_SCHEMA_V4
            | riffdb_query_module::APPLICATION_LOCK_SCHEMA_V5
            | riffdb_query_module::APPLICATION_LOCK_SCHEMA_V6
            | riffdb_query_module::APPLICATION_LOCK_SCHEMA_V7
            | riffdb_query_module::APPLICATION_LOCK_SCHEMA_V8
    ) {
        return Ok(PinnedLockPreview::NotPinned);
    }
    let artifact = lock.contract_bundle_artifact().ok_or_else(|| {
        lock_diagnostic(
            &absolute_lock_path,
            riffdb_query_module::ApplicationLockErrorKind::InvalidShape,
        )
    })?;
    let bundle_bytes =
        read_workspace_file(root, artifact.path(), riffdb_contract_ir::MAX_BUNDLE_BYTES)?;
    if hash_generated_artifact(&bundle_bytes) != artifact.content_hash() {
        return Err(lock_diagnostic(
            &absolute_lock_path,
            riffdb_query_module::ApplicationLockErrorKind::IdentityMismatch,
        ));
    }
    let contract = ContractBundle::decode(&bundle_bytes).map_err(|_| {
        lock_diagnostic(
            &absolute_lock_path,
            riffdb_query_module::ApplicationLockErrorKind::IdentityMismatch,
        )
    })?;
    let current_contract_source = application_contract_source(source_path)?;
    if hash_source(current_contract_source.as_bytes()) != contract.source_hash() {
        return Ok(PinnedLockPreview::ContractSourceChanged);
    }
    preview_application_lock_with_bundle(source_path, contract)
        .map(Box::new)
        .map(PinnedLockPreview::Proposed)
}

/// Produces the canonical V2 application source and optionally replaces only
/// the source file atomically. No compilation, generation, lock write, or
/// server operation occurs on this path.
pub(crate) fn migrate_application_source_v2(
    source_path: &Path,
    write: bool,
) -> Result<Vec<u8>, ScaffoldError> {
    let source_bytes = read_bounded(source_path, 1_048_576)?;
    let source_text =
        std::str::from_utf8(&source_bytes).map_err(|_| ScaffoldError::ApplicationSource)?;
    let parsed = ApplicationSourceManifest::parse(source_text)
        .map_err(|error| application_source_diagnostic(source_path, error.kind()))?;
    let migrated = if parsed.schema() == riffdb_query_module::APPLICATION_SOURCE_SCHEMA_V2 {
        parsed.canonical_bytes().to_vec()
    } else {
        let mut value: serde_json::Value = serde_json::from_slice(parsed.canonical_bytes())
            .map_err(|_| ScaffoldError::ApplicationSource)?;
        value["schema"] = json!(riffdb_query_module::APPLICATION_SOURCE_SCHEMA_V2);
        let generation = value
            .get_mut("generation")
            .and_then(serde_json::Value::as_object_mut)
            .ok_or(ScaffoldError::ApplicationSource)?;
        generation.insert("python".to_owned(), json!("generated/python/client.py"));
        let proposed =
            serde_json::to_string(&value).map_err(|_| ScaffoldError::ApplicationSource)?;
        ApplicationSourceManifest::parse(&proposed)
            .map_err(|error| application_source_diagnostic(source_path, error.kind()))?
            .canonical_bytes()
            .to_vec()
    };
    if write {
        atomic_write_absolute(source_path, &migrated)?;
    }
    Ok(migrated)
}

pub(crate) fn write_application_lock(
    source_path: &Path,
    lock_path: Option<&Path>,
) -> Result<(), ScaffoldError> {
    if application_contract_version(source_path)? != 1 {
        return Err(ScaffoldError::IdentityMismatch);
    }
    let compiled = compile_symbolic_application(source_path)?;
    publish_compiled_application_lock(source_path, lock_path, &compiled)
}

pub(crate) fn write_application_lock_with_bundle(
    source_path: &Path,
    lock_path: Option<&Path>,
    contract: ContractBundle,
) -> Result<(), ScaffoldError> {
    let compiled = compile_symbolic_application_with_bundle(source_path, contract)?;
    publish_compiled_application_lock(source_path, lock_path, &compiled)
}

pub(crate) fn write_project_application_lock_with_bundle(
    source_path: &Path,
    contract: ContractBundle,
    selected: &[GeneratedApplicationArtifactKind],
) -> Result<(), ScaffoldError> {
    let compiled = compile_symbolic_application_with_bundle(source_path, contract)?;
    publish_compiled_project_lock(source_path, &compiled, selected)
}

pub(crate) fn refresh_application_lock_from_pinned_bundle(
    source_path: &Path,
    lock_path: Option<&Path>,
) -> Result<PinnedLockRefresh, ScaffoldError> {
    let root = source_parent(source_path);
    let absolute_lock_path = workspace_lock_path(root, lock_path)?;
    let existing = match read_bounded(&absolute_lock_path, 4 * 1_024 * 1_024) {
        Ok(existing) => existing,
        Err(ScaffoldError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(PinnedLockRefresh::NotPinned);
        }
        Err(error) => return Err(error),
    };
    let lock = ApplicationLock::decode_canonical(&existing)
        .map_err(|error| lock_diagnostic(&absolute_lock_path, error.kind()))?;
    if !matches!(
        lock.schema(),
        riffdb_query_module::APPLICATION_LOCK_SCHEMA_V3
            | riffdb_query_module::APPLICATION_LOCK_SCHEMA_V4
            | riffdb_query_module::APPLICATION_LOCK_SCHEMA_V5
            | riffdb_query_module::APPLICATION_LOCK_SCHEMA_V6
            | riffdb_query_module::APPLICATION_LOCK_SCHEMA_V7
            | riffdb_query_module::APPLICATION_LOCK_SCHEMA_V8
    ) {
        return Ok(PinnedLockRefresh::NotPinned);
    }
    let artifact = lock.contract_bundle_artifact().ok_or_else(|| {
        lock_diagnostic(
            &absolute_lock_path,
            riffdb_query_module::ApplicationLockErrorKind::InvalidShape,
        )
    })?;
    let bundle_bytes =
        read_workspace_file(root, artifact.path(), riffdb_contract_ir::MAX_BUNDLE_BYTES)?;
    if hash_generated_artifact(&bundle_bytes) != artifact.content_hash() {
        return Err(lock_diagnostic(
            &absolute_lock_path,
            riffdb_query_module::ApplicationLockErrorKind::IdentityMismatch,
        ));
    }
    let contract = ContractBundle::decode(&bundle_bytes).map_err(|_| {
        lock_diagnostic(
            &absolute_lock_path,
            riffdb_query_module::ApplicationLockErrorKind::IdentityMismatch,
        )
    })?;
    let current_contract_source = application_contract_source(source_path)?;
    if hash_source(current_contract_source.as_bytes()) != contract.source_hash() {
        return Ok(PinnedLockRefresh::ContractSourceChanged);
    }
    let compiled = compile_symbolic_application_with_bundle(source_path, contract)?;
    publish_compiled_application_lock(source_path, lock_path, &compiled)?;
    Ok(PinnedLockRefresh::Refreshed)
}

fn publish_compiled_application_lock(
    source_path: &Path,
    lock_path: Option<&Path>,
    compiled: &CompiledSymbolicApplication,
) -> Result<(), ScaffoldError> {
    let root = source_parent(source_path);
    for (path, bytes) in &compiled.outputs {
        atomic_write_workspace(root, path, bytes)?;
    }
    let lock_path = workspace_lock_path(root, lock_path)?;
    atomic_write_absolute(&lock_path, compiled.lock.canonical_bytes())?;
    Ok(())
}

fn publish_compiled_project_lock(
    source_path: &Path,
    compiled: &CompiledSymbolicApplication,
    selected: &[GeneratedApplicationArtifactKind],
) -> Result<(), ScaffoldError> {
    let root = source_parent(source_path);
    require_selected_declared(compiled, selected)?;
    for (path, bytes) in &compiled.outputs {
        let kind = compiled_output_kind(compiled, path);
        let present = match fs::symlink_metadata(root.join(path)) {
            Ok(_) => true,
            Err(error) if error.kind() == io::ErrorKind::NotFound => false,
            Err(error) => return Err(ScaffoldError::Io(error)),
        };
        let required = kind
            .is_none_or(|kind| !selectable_artifact(kind) || selected.contains(&kind) || present);
        if required {
            atomic_write_workspace(root, path, bytes)?;
        }
    }
    let lock_path = workspace_lock_path(root, None)?;
    atomic_write_absolute(&lock_path, compiled.lock.canonical_bytes())
}

pub(crate) fn check_application_lock(
    source_path: &Path,
    lock_path: Option<&Path>,
) -> Result<(), ScaffoldError> {
    let root = source_parent(source_path);
    let lock_path = workspace_lock_path(root, lock_path)?;
    let existing = read_bounded(&lock_path, 4 * 1_024 * 1_024)?;
    let decoded = ApplicationLock::decode_canonical(&existing)
        .map_err(|error| lock_diagnostic(&lock_path, error.kind()))?;
    let compiled = compile_for_existing_lock(source_path, root, &lock_path, &decoded)?;
    if decoded.canonical_bytes() != compiled.lock.canonical_bytes() {
        return Err(lock_diagnostic(
            &lock_path,
            riffdb_query_module::ApplicationLockErrorKind::IdentityMismatch,
        ));
    }
    for (path, expected) in &compiled.outputs {
        let actual = read_workspace_file(root, path, 16 * 1_024 * 1_024)?;
        if &actual != expected {
            return Err(lock_diagnostic(
                &lock_path,
                riffdb_query_module::ApplicationLockErrorKind::IdentityMismatch,
            ));
        }
    }
    Ok(())
}

pub(crate) fn check_project_application_lock(
    source_path: &Path,
    selected: &[GeneratedApplicationArtifactKind],
) -> Result<(), ScaffoldError> {
    let root = source_parent(source_path);
    let lock_path = workspace_lock_path(root, None)?;
    let lock = ApplicationLock::decode_canonical(&read_bounded(
        &lock_path,
        riffdb_query_module::MAX_APPLICATION_LOCK_BYTES,
    )?)
    .map_err(|error| lock_diagnostic(&lock_path, error.kind()))?;
    let compiled = compile_for_existing_lock(source_path, root, &lock_path, &lock)?;
    check_project_compilation(root, &lock, &compiled, selected, false)
}

fn check_project_compilation(
    root: &Path,
    lock: &ApplicationLock,
    compiled: &CompiledSymbolicApplication,
    selected: &[GeneratedApplicationArtifactKind],
    selected_may_be_repaired: bool,
) -> Result<(), ScaffoldError> {
    require_selected_declared(compiled, selected)?;
    if lock.canonical_bytes() != compiled.lock.canonical_bytes() {
        return Err(ScaffoldError::IdentityMismatch);
    }
    for (path, expected) in &compiled.outputs {
        let kind = compiled_output_kind(compiled, path);
        if selected_may_be_repaired && kind.is_some_and(|kind| selected.contains(&kind)) {
            continue;
        }
        let selectable = kind.is_some_and(selectable_artifact);
        let required = !selectable || kind.is_some_and(|kind| selected.contains(&kind));
        let present = match fs::symlink_metadata(root.join(path)) {
            Ok(_) => true,
            Err(error) if error.kind() == io::ErrorKind::NotFound => false,
            Err(error) => return Err(ScaffoldError::Io(error)),
        };
        if !required && !present {
            continue;
        }
        let actual = read_workspace_file(root, path, 16 * 1_024 * 1_024)?;
        if actual != *expected {
            return Err(ScaffoldError::IdentityMismatch);
        }
    }
    Ok(())
}

fn require_selected_declared(
    compiled: &CompiledSymbolicApplication,
    selected: &[GeneratedApplicationArtifactKind],
) -> Result<(), ScaffoldError> {
    if selected.iter().any(|selected| {
        !selectable_artifact(*selected)
            || !compiled
                .lock
                .artifacts()
                .iter()
                .any(|artifact| artifact.kind() == *selected)
    }) {
        return Err(ScaffoldError::IdentityMismatch);
    }
    Ok(())
}

fn compiled_output_kind(
    compiled: &CompiledSymbolicApplication,
    path: &str,
) -> Option<GeneratedApplicationArtifactKind> {
    compiled
        .lock
        .artifacts()
        .iter()
        .find(|artifact| artifact.path() == path)
        .map(GeneratedApplicationArtifact::kind)
}

const fn selectable_artifact(kind: GeneratedApplicationArtifactKind) -> bool {
    matches!(
        kind,
        GeneratedApplicationArtifactKind::Rust
            | GeneratedApplicationArtifactKind::TypeScript
            | GeneratedApplicationArtifactKind::Go
            | GeneratedApplicationArtifactKind::Python
            | GeneratedApplicationArtifactKind::Mcp
    )
}

pub(crate) fn plan_application_migrations(
    source_path: &Path,
    lock_path: Option<&Path>,
) -> Result<serde_json::Value, ScaffoldError> {
    check_application_lock(source_path, lock_path)?;
    plan_application_migrations_checked(source_path, lock_path)
}

pub(crate) fn plan_project_application_migrations(
    source_path: &Path,
    selected: &[GeneratedApplicationArtifactKind],
) -> Result<serde_json::Value, ScaffoldError> {
    check_project_application_lock(source_path, selected)?;
    plan_application_migrations_checked(source_path, None)
}

fn plan_application_migrations_checked(
    source_path: &Path,
    lock_path: Option<&Path>,
) -> Result<serde_json::Value, ScaffoldError> {
    let root = source_parent(source_path);
    let lock_path = workspace_lock_path(root, lock_path)?;
    let lock = ApplicationLock::decode_canonical(&read_bounded(
        &lock_path,
        riffdb_query_module::MAX_APPLICATION_LOCK_BYTES,
    )?)
    .map_err(|error| lock_diagnostic(&lock_path, error.kind()))?;
    if !matches!(
        lock.schema(),
        riffdb_query_module::APPLICATION_LOCK_SCHEMA_V4
            | riffdb_query_module::APPLICATION_LOCK_SCHEMA_V5
            | riffdb_query_module::APPLICATION_LOCK_SCHEMA_V6
            | riffdb_query_module::APPLICATION_LOCK_SCHEMA_V7
            | riffdb_query_module::APPLICATION_LOCK_SCHEMA_V8
    ) {
        return Err(lock_diagnostic(
            &lock_path,
            riffdb_query_module::ApplicationLockErrorKind::UnsupportedVersion,
        ));
    }
    let candidate_artifact = lock.contract_bundle_artifact().ok_or_else(|| {
        lock_diagnostic(
            &lock_path,
            riffdb_query_module::ApplicationLockErrorKind::InvalidShape,
        )
    })?;
    let candidate = ContractBundle::decode(&read_workspace_file(
        root,
        candidate_artifact.path(),
        riffdb_contract_ir::MAX_BUNDLE_BYTES,
    )?)
    .map_err(|_| ScaffoldError::ApplicationLock)?;
    let mut parents = Vec::with_capacity(lock.migrations().len());
    for entry in lock.migrations() {
        let bundle = riffdb_contract_ir::MigrationBundleV1::decode(&read_workspace_file(
            root,
            entry.migration_artifact_path(),
            riffdb_contract_ir::MAX_MIGRATION_BUNDLE_BYTES_V1,
        )?)
        .map_err(|_| ScaffoldError::ApplicationLock)?;
        if bundle.bundle_hash() != entry.migration_bundle_hash()
            || bundle.parent_version() != entry.parent_version()
            || bundle.parent_bundle_hash() != entry.parent_bundle_hash()
            || bundle.candidate_version() != candidate.contract_version()
            || bundle.candidate_bundle_hash() != candidate.bundle_hash()
        {
            return Err(ScaffoldError::IdentityMismatch);
        }
        let categories = bundle
            .steps()
            .iter()
            .map(|step| migration_step_category(step.kind()))
            .collect::<std::collections::BTreeSet<_>>();
        let bounds = bundle.resource_bounds();
        parents.push(json!({
            "migration_bundle_hash": hex(bundle.bundle_hash().as_bytes()),
            "migration_bundle_path": entry.migration_artifact_path(),
            "parent_bundle_hash": hex(entry.parent_bundle_hash().as_bytes()),
            "parent_bundle_path": entry.parent_artifact_path(),
            "parent_version": entry.parent_version().get(),
            "resource_bounds": {
                "maximum_bundle_bytes": bounds.maximum_bundle_bytes(),
                "maximum_expression_nodes": bounds.maximum_expression_nodes(),
                "maximum_source_bytes": bounds.maximum_source_bytes(),
                "maximum_steps": bounds.maximum_steps(),
            },
            "source_hash": hex(entry.source_hash().as_bytes()),
            "source_path": entry.source_path(),
            "step_categories": categories,
            "step_count": bundle.steps().len(),
        }));
    }
    Ok(json!({
        "candidate_bundle_hash": hex(candidate.bundle_hash().as_bytes()),
        "candidate_version": candidate.contract_version().get(),
        "lineage": candidate.lineage().as_str(),
        "schema": "riffdb.migration-plan/v1",
        "supported_parents": parents,
    }))
}

pub(crate) struct LockedMigrationSubmission {
    candidate_bundle: Vec<u8>,
    migration_bundle: Vec<u8>,
    migration_hash: riffdb_types::MigrationBundleHash,
}

impl LockedMigrationSubmission {
    pub(crate) fn into_parts(self) -> (Vec<u8>, Vec<u8>, riffdb_types::MigrationBundleHash) {
        (
            self.candidate_bundle,
            self.migration_bundle,
            self.migration_hash,
        )
    }
}

/// Loads the exact candidate and sole direct-parent migration from a checked lock.
pub(crate) fn load_locked_migration_submission(
    source_path: &Path,
    lock_path: Option<&Path>,
    selected_migration_hash: Option<riffdb_types::MigrationBundleHash>,
) -> Result<LockedMigrationSubmission, ScaffoldError> {
    check_application_lock(source_path, lock_path)?;
    load_locked_migration_submission_checked(source_path, lock_path, selected_migration_hash)
}

pub(crate) fn load_locked_project_migration_submission(
    source_path: &Path,
    selected: &[GeneratedApplicationArtifactKind],
    selected_migration_hash: Option<riffdb_types::MigrationBundleHash>,
) -> Result<LockedMigrationSubmission, ScaffoldError> {
    check_project_application_lock(source_path, selected)?;
    load_locked_migration_submission_checked(source_path, None, selected_migration_hash)
}

fn load_locked_migration_submission_checked(
    source_path: &Path,
    lock_path: Option<&Path>,
    selected_migration_hash: Option<riffdb_types::MigrationBundleHash>,
) -> Result<LockedMigrationSubmission, ScaffoldError> {
    let root = source_parent(source_path);
    let lock_path = workspace_lock_path(root, lock_path)?;
    let lock = ApplicationLock::decode_canonical(&read_bounded(
        &lock_path,
        riffdb_query_module::MAX_APPLICATION_LOCK_BYTES,
    )?)
    .map_err(|error| lock_diagnostic(&lock_path, error.kind()))?;
    let candidate_artifact = lock
        .contract_bundle_artifact()
        .ok_or(ScaffoldError::ApplicationLock)?;
    let candidate_bundle = read_workspace_file(
        root,
        candidate_artifact.path(),
        riffdb_contract_ir::MAX_BUNDLE_BYTES,
    )?;
    let candidate =
        ContractBundle::decode(&candidate_bundle).map_err(|_| ScaffoldError::ApplicationLock)?;
    let mut matches = lock.migrations().iter().filter(|entry| {
        selected_migration_hash.is_none_or(|hash| entry.migration_bundle_hash() == hash)
    });
    let entry = matches.next().ok_or(ScaffoldError::IdentityMismatch)?;
    if matches.next().is_some() {
        return Err(ScaffoldError::IdentityMismatch);
    }
    let parent_bundle = read_workspace_file(
        root,
        entry.parent_artifact_path(),
        riffdb_contract_ir::MAX_BUNDLE_BYTES,
    )?;
    let parent =
        ContractBundle::decode(&parent_bundle).map_err(|_| ScaffoldError::ApplicationLock)?;
    if parent.bundle_hash() != entry.parent_bundle_hash()
        || parent.contract_version() != entry.parent_version()
        || parent.lineage() != candidate.lineage()
        || parent.contract_version() >= candidate.contract_version()
    {
        return Err(ScaffoldError::IdentityMismatch);
    }
    let migration_bundle = read_workspace_file(
        root,
        entry.migration_artifact_path(),
        riffdb_contract_ir::MAX_MIGRATION_BUNDLE_BYTES_V1,
    )?;
    let migration = riffdb_contract_ir::MigrationBundleV1::decode(&migration_bundle)
        .map_err(|_| ScaffoldError::ApplicationLock)?;
    if migration.bundle_hash() != entry.migration_bundle_hash()
        || migration.parent_bundle_hash() != parent.bundle_hash()
        || migration.candidate_bundle_hash() != candidate.bundle_hash()
    {
        return Err(ScaffoldError::IdentityMismatch);
    }
    Ok(LockedMigrationSubmission {
        candidate_bundle,
        migration_bundle,
        migration_hash: migration.bundle_hash(),
    })
}

fn migration_step_category(kind: &riffdb_contract_ir::MigrationStepKindV1) -> &'static str {
    use riffdb_contract_ir::MigrationStepKindV1 as Step;
    match kind {
        Step::RenameIdentity { .. } => "rename_identity",
        Step::RetireIdentity { .. } => "retire_identity",
        Step::SetField { .. } => "set_field",
        Step::ReplaceField { .. } => "replace_field",
        Step::RequireEntity { .. } => "require_entity",
        Step::RekeyEntity { .. } => "rekey_entity",
        Step::MapEnum { .. } => "map_enum",
        Step::RebuildIndex { .. } => "rebuild_index",
        Step::ValidateRelationship { .. } => "validate_relationship",
        Step::ValidateUnique { .. } => "validate_unique",
        Step::ValidateInvariant { .. } => "validate_invariant",
        Step::RebuildProjection { .. } => "rebuild_projection",
        Step::AcknowledgeRepartition { .. } => "acknowledge_repartition",
        Step::AcknowledgeAggregate { .. } => "acknowledge_aggregate",
        Step::AcknowledgeConflict { .. } => "acknowledge_conflict",
    }
}

pub(crate) fn load_locked_application(
    source_path: &Path,
    lock_path: Option<&Path>,
) -> Result<LockedApplication, ScaffoldError> {
    check_application_lock(source_path, lock_path)?;
    let root = source_parent(source_path);
    let lock_path = workspace_lock_path(root, lock_path)?;
    let lock = ApplicationLock::decode_canonical(&read_bounded(&lock_path, 4 * 1_024 * 1_024)?)
        .map_err(|error| lock_diagnostic(&lock_path, error.kind()))?;
    let compiled = compile_for_existing_lock(source_path, root, &lock_path, &lock)?;
    let manifest_path = root.join(EXACT_MANIFEST_PATH);
    let manifest =
        ApplicationManifest::decode_canonical(&read_bounded(&manifest_path, 4 * 1_024 * 1_024)?)
            .map_err(|_| ScaffoldError::Manifest)?;
    let contract = compiled_contract_bundle(&compiled).or_else(|_| {
        if application_contract_version(source_path)? != 1 {
            return Err(ScaffoldError::IdentityMismatch);
        }
        let source = read_workspace_text(root, manifest.contract().source(), 1_048_576)?;
        compile_contract_source(&source).map_err(|_| ScaffoldError::CompileContract)
    })?;
    Ok(LockedApplication {
        root: root.to_path_buf(),
        manifest_path,
        manifest,
        lock,
        contract,
    })
}

pub(crate) fn load_locked_project_application(
    source_path: &Path,
    selected: &[GeneratedApplicationArtifactKind],
) -> Result<LockedApplication, ScaffoldError> {
    check_project_application_lock(source_path, selected)?;
    load_locked_application_unchecked(source_path, None)
}

fn load_locked_application_unchecked(
    source_path: &Path,
    lock_path: Option<&Path>,
) -> Result<LockedApplication, ScaffoldError> {
    let root = source_parent(source_path);
    let lock_path = workspace_lock_path(root, lock_path)?;
    let lock = ApplicationLock::decode_canonical(&read_bounded(&lock_path, 4 * 1_024 * 1_024)?)
        .map_err(|error| lock_diagnostic(&lock_path, error.kind()))?;
    let compiled = compile_for_existing_lock(source_path, root, &lock_path, &lock)?;
    let manifest_path = root.join(EXACT_MANIFEST_PATH);
    let manifest =
        ApplicationManifest::decode_canonical(&read_bounded(&manifest_path, 4 * 1_024 * 1_024)?)
            .map_err(|_| ScaffoldError::Manifest)?;
    let contract = compiled_contract_bundle(&compiled).or_else(|_| {
        if application_contract_version(source_path)? != 1 {
            return Err(ScaffoldError::IdentityMismatch);
        }
        let source = read_workspace_text(root, manifest.contract().source(), 1_048_576)?;
        compile_contract_source(&source).map_err(|_| ScaffoldError::CompileContract)
    })?;
    Ok(LockedApplication {
        root: root.to_path_buf(),
        manifest_path,
        manifest,
        lock,
        contract,
    })
}

fn generate_application_locked(source_path: &Path, lock_path: &Path) -> Result<(), ScaffoldError> {
    let root = source_parent(source_path);
    let lock_path = workspace_lock_path(root, Some(lock_path))?;
    let existing = read_bounded(&lock_path, 4 * 1_024 * 1_024)?;
    let decoded = ApplicationLock::decode_canonical(&existing)
        .map_err(|error| lock_diagnostic(&lock_path, error.kind()))?;
    let compiled = compile_for_existing_lock(source_path, root, &lock_path, &decoded)?;
    if decoded.canonical_bytes() != compiled.lock.canonical_bytes() {
        return Err(lock_diagnostic(
            &lock_path,
            riffdb_query_module::ApplicationLockErrorKind::IdentityMismatch,
        ));
    }
    for (path, bytes) in &compiled.outputs {
        atomic_write_workspace(root, path, bytes)?;
    }
    Ok(())
}

fn compile_for_existing_lock(
    source_path: &Path,
    root: &Path,
    lock_path: &Path,
    lock: &ApplicationLock,
) -> Result<CompiledSymbolicApplication, ScaffoldError> {
    compile_for_existing_lock_mode(source_path, root, lock_path, lock, false)
}

fn compile_for_existing_lock_mode(
    source_path: &Path,
    root: &Path,
    lock_path: &Path,
    lock: &ApplicationLock,
    include_runtime_operation_catalog: bool,
) -> Result<CompiledSymbolicApplication, ScaffoldError> {
    if matches!(
        lock.schema(),
        riffdb_query_module::APPLICATION_LOCK_SCHEMA_V3
            | riffdb_query_module::APPLICATION_LOCK_SCHEMA_V4
            | riffdb_query_module::APPLICATION_LOCK_SCHEMA_V5
            | riffdb_query_module::APPLICATION_LOCK_SCHEMA_V6
            | riffdb_query_module::APPLICATION_LOCK_SCHEMA_V7
            | riffdb_query_module::APPLICATION_LOCK_SCHEMA_V8
    ) {
        let artifact = lock.contract_bundle_artifact().ok_or_else(|| {
            lock_diagnostic(
                lock_path,
                riffdb_query_module::ApplicationLockErrorKind::InvalidShape,
            )
        })?;
        let bytes =
            read_workspace_file(root, artifact.path(), riffdb_contract_ir::MAX_BUNDLE_BYTES)?;
        let contract = ContractBundle::decode(&bytes).map_err(|_| {
            lock_diagnostic(
                lock_path,
                riffdb_query_module::ApplicationLockErrorKind::IdentityMismatch,
            )
        })?;
        if include_runtime_operation_catalog {
            compile_symbolic_application_with_bundle_and_runtime_catalog(source_path, contract)
        } else {
            compile_symbolic_application_with_bundle(source_path, contract)
        }
    } else {
        if application_contract_version(source_path)? != 1 {
            return Err(lock_diagnostic(
                lock_path,
                riffdb_query_module::ApplicationLockErrorKind::IdentityMismatch,
            ));
        }
        if include_runtime_operation_catalog {
            compile_symbolic_application_mode(source_path, None, false, true)
        } else {
            compile_symbolic_application_legacy(source_path)
        }
    }
}

pub(crate) fn runtime_operation_catalog(
    source_path: &Path,
    lock_path: Option<&Path>,
) -> Result<Vec<u8>, ScaffoldError> {
    let root = source_parent(source_path);
    let lock_path = workspace_lock_path(root, lock_path)?;
    let existing = read_bounded(&lock_path, riffdb_query_module::MAX_APPLICATION_LOCK_BYTES)?;
    let decoded = ApplicationLock::decode_canonical(&existing)
        .map_err(|error| lock_diagnostic(&lock_path, error.kind()))?;
    let compiled = compile_for_existing_lock_mode(source_path, root, &lock_path, &decoded, true)?;
    if decoded.canonical_bytes() != compiled.lock.canonical_bytes() {
        return Err(lock_diagnostic(
            &lock_path,
            riffdb_query_module::ApplicationLockErrorKind::IdentityMismatch,
        ));
    }
    for (path, expected) in &compiled.outputs {
        let actual = read_workspace_file(root, path, 16 * 1_024 * 1_024)?;
        if actual != *expected {
            return Err(lock_diagnostic(
                &lock_path,
                riffdb_query_module::ApplicationLockErrorKind::IdentityMismatch,
            ));
        }
    }
    compiled
        .runtime_operation_catalog
        .ok_or(ScaffoldError::GenerateMcp)
}

pub(crate) fn write_runtime_operation_catalog(
    source_path: &Path,
    lock_path: Option<&Path>,
    output_path: &Path,
) -> Result<riffdb_types::GeneratedArtifactHash, ScaffoldError> {
    let catalog = runtime_operation_catalog(source_path, lock_path)?;
    let hash = hash_generated_artifact(&catalog);
    atomic_write_absolute(output_path, &catalog)?;
    Ok(hash)
}

fn compiled_contract_bundle(
    compiled: &CompiledSymbolicApplication,
) -> Result<ContractBundle, ScaffoldError> {
    let bytes = compiled
        .outputs
        .iter()
        .find(|(path, _)| path == CONTRACT_BUNDLE_ARTIFACT_PATH)
        .map(|(_, bytes)| bytes.as_slice())
        .ok_or(ScaffoldError::ApplicationLock)?;
    ContractBundle::decode(bytes).map_err(|_| ScaffoldError::ApplicationLock)
}

pub(crate) fn application_contract_version(source_path: &Path) -> Result<u64, ScaffoldError> {
    let source_bytes = read_bounded(source_path, 1_048_576)?;
    let source_text =
        std::str::from_utf8(&source_bytes).map_err(|_| ScaffoldError::ApplicationSource)?;
    let source = ApplicationSourceManifest::parse(source_text)
        .map_err(|error| application_source_diagnostic(source_path, error.kind()))?;
    Ok(source.contract().version())
}

pub(crate) fn application_contract_source(source_path: &Path) -> Result<String, ScaffoldError> {
    let source_bytes = read_bounded(source_path, 1_048_576)?;
    let source_text =
        std::str::from_utf8(&source_bytes).map_err(|_| ScaffoldError::ApplicationSource)?;
    let source = ApplicationSourceManifest::parse(source_text)
        .map_err(|error| application_source_diagnostic(source_path, error.kind()))?;
    read_workspace_text(
        source_parent(source_path),
        source.contract().source(),
        1_048_576,
    )
}

fn compile_symbolic_application(
    source_path: &Path,
) -> Result<CompiledSymbolicApplication, ScaffoldError> {
    compile_symbolic_application_mode(source_path, None, true, false)
}

fn compile_symbolic_application_legacy(
    source_path: &Path,
) -> Result<CompiledSymbolicApplication, ScaffoldError> {
    compile_symbolic_application_mode(source_path, None, false, false)
}

fn compile_symbolic_application_with_bundle(
    source_path: &Path,
    contract: ContractBundle,
) -> Result<CompiledSymbolicApplication, ScaffoldError> {
    compile_symbolic_application_mode(source_path, Some(contract), true, false)
}

fn compile_symbolic_application_with_bundle_and_runtime_catalog(
    source_path: &Path,
    contract: ContractBundle,
) -> Result<CompiledSymbolicApplication, ScaffoldError> {
    compile_symbolic_application_mode(source_path, Some(contract), true, true)
}

fn compile_symbolic_application_mode(
    source_path: &Path,
    contract_override: Option<ContractBundle>,
    lock_v3: bool,
    include_runtime_operation_catalog: bool,
) -> Result<CompiledSymbolicApplication, ScaffoldError> {
    let source_bytes = read_bounded(source_path, 1_048_576)?;
    let source_text =
        std::str::from_utf8(&source_bytes).map_err(|_| ScaffoldError::ApplicationSource)?;
    let source = ApplicationSourceManifest::parse(source_text)
        .map_err(|error| application_source_diagnostic(source_path, error.kind()))?;
    let root = source_parent(source_path);
    let contract_source = read_workspace_text(root, source.contract().source(), 1_048_576)?;
    let contract = if let Some(contract) = contract_override {
        if contract.source_hash() != hash_source(contract_source.as_bytes())
            || contract.lineage().as_str() != source.contract().lineage()
            || contract.contract_version().get() != source.contract().version()
        {
            return Err(lock_diagnostic(
                Path::new(DEFAULT_LOCK_PATH),
                riffdb_query_module::ApplicationLockErrorKind::IdentityMismatch,
            ));
        }
        if source.migrations().is_empty() {
            contract
        } else {
            let parent_ref = contract.parent().ok_or_else(|| {
                lock_diagnostic(
                    Path::new(DEFAULT_LOCK_PATH),
                    riffdb_query_module::ApplicationLockErrorKind::IdentityMismatch,
                )
            })?;
            let mut matching_parent = None;
            for declared in source.migrations() {
                let parent_bytes = read_workspace_file(
                    root,
                    declared.parent_bundle(),
                    riffdb_contract_ir::MAX_BUNDLE_BYTES,
                )?;
                let parent = ContractBundle::decode(&parent_bytes).map_err(|_| {
                    lock_diagnostic(
                        Path::new(declared.parent_bundle()),
                        riffdb_query_module::ApplicationLockErrorKind::IdentityMismatch,
                    )
                })?;
                if parent.contract_version() == parent_ref.contract_version()
                    && parent.bundle_hash() == parent_ref.bundle_hash()
                {
                    matching_parent = Some(declared);
                    break;
                }
            }
            let declared = matching_parent.ok_or_else(|| {
                lock_diagnostic(
                    Path::new(DEFAULT_LOCK_PATH),
                    riffdb_query_module::ApplicationLockErrorKind::IdentityMismatch,
                )
            })?;
            let parent_bytes = read_workspace_file(
                root,
                declared.parent_bundle(),
                riffdb_contract_ir::MAX_BUNDLE_BYTES,
            )?;
            let parent = ContractBundle::decode(&parent_bytes).map_err(|_| {
                lock_diagnostic(
                    Path::new(declared.parent_bundle()),
                    riffdb_query_module::ApplicationLockErrorKind::IdentityMismatch,
                )
            })?;
            let migration_source = read_workspace_text(
                root,
                declared.source(),
                riffdb_contract_ir::MAX_MIGRATION_SOURCE_BYTES_V1,
            )?;
            let (candidate, _) = riffdb_contract_compiler::compile_contract_migration_successor(
                &contract_source,
                &migration_source,
                &parent,
            )
            .map_err(|error| contract_diagnostic(declared.source(), &error))?;
            candidate
        }
    } else {
        compile_contract_source(&contract_source)
            .map_err(|error| contract_diagnostic(source.contract().source(), &error))?
    };
    let mut migration_inputs = Vec::with_capacity(source.migrations().len());
    let mut migration_outputs = Vec::with_capacity(source.migrations().len());
    for declared in source.migrations() {
        let parent_bytes = read_workspace_file(
            root,
            declared.parent_bundle(),
            riffdb_contract_ir::MAX_BUNDLE_BYTES,
        )?;
        let parent = ContractBundle::decode(&parent_bytes).map_err(|_| {
            lock_diagnostic(
                Path::new(declared.parent_bundle()),
                riffdb_query_module::ApplicationLockErrorKind::IdentityMismatch,
            )
        })?;
        let migration_source = read_workspace_text(
            root,
            declared.source(),
            riffdb_contract_ir::MAX_MIGRATION_SOURCE_BYTES_V1,
        )?;
        let migration = compile_migration_source(&migration_source, &parent, &contract)
            .map_err(|error| contract_diagnostic(declared.source(), &error))?;
        let artifact_path = format!(
            "generated/migrations/{}-to-{}.riffdb.migration.bundle",
            parent.contract_version().get(),
            contract.contract_version().get(),
        );
        migration_inputs.push(
            ApplicationMigrationLockInput::new(
                declared.source(),
                migration_source.as_bytes(),
                declared.parent_bundle(),
                &parent,
                &artifact_path,
                &migration,
            )
            .map_err(|error| lock_diagnostic(Path::new(DEFAULT_LOCK_PATH), error.kind()))?,
        );
        migration_outputs.push((artifact_path, migration.canonical_bytes().to_vec()));
    }
    let mut modules = Vec::with_capacity(source.query_modules().len());
    let mut python_query_sources = Vec::new();
    for declared in source.query_modules() {
        let mut queries = Vec::with_capacity(declared.queries().len());
        for query in declared.queries() {
            let query_source = read_workspace_text(root, query.source(), 1_048_576)?;
            python_query_sources.push((
                query.name().to_owned(),
                query.source().to_owned(),
                query_source.clone(),
            ));
            queries.push(
                NamedQuerySource::new(query.name(), query_source)
                    .map_err(|_| ScaffoldError::CompileQuery)?,
            );
        }
        let candidate = QueryModuleCandidate::new(
            QueryModuleName::new(declared.name()).map_err(|_| ScaffoldError::CompileQuery)?,
            QueryModuleVersion::new(declared.version()).ok_or(ScaffoldError::CompileQuery)?,
            queries,
        )
        .map_err(|_| ScaffoldError::CompileQuery)?;
        modules.push(QueryModule::compile(candidate, &contract).map_err(|error| {
            let path = error.query_name().and_then(|name| {
                declared
                    .queries()
                    .iter()
                    .find(|query| query.name() == name)
                    .map(|query| query.source())
            });
            query_diagnostic(path.unwrap_or("riffdb/queries"), &error)
        })?);
    }
    let mut reactive_modules = Vec::with_capacity(source.reactive_modules().len());
    let mut reactive_outputs = Vec::with_capacity(source.reactive_modules().len());
    for declared in source.reactive_modules() {
        let reactive_source = read_workspace_text(root, declared.source(), 1_048_576)?;
        let module = compile_reactive_source(&reactive_source, &contract, &modules)
            .map_err(|_| ScaffoldError::CompileReactive)?;
        if module.name() != declared.name() || module.version() != declared.version() {
            return Err(ScaffoldError::IdentityMismatch);
        }
        let path = format!(
            "generated/reactive/{}.riffdb.reactive.module",
            declared.name()
        );
        reactive_outputs.push((path, module.canonical_bytes().to_vec()));
        reactive_modules.push(module);
    }
    let exact = if matches!(
        source.schema(),
        riffdb_query_module::APPLICATION_SOURCE_SCHEMA_V4
            | riffdb_query_module::APPLICATION_SOURCE_SCHEMA_V5
            | riffdb_query_module::APPLICATION_SOURCE_SCHEMA_V6
            | riffdb_query_module::APPLICATION_SOURCE_SCHEMA_V7
    ) {
        source.exact_manifest_v2(&contract, &modules, &reactive_modules)
    } else {
        source.exact_manifest(&contract, &modules)
    }
    .map_err(|error| application_source_diagnostic(source_path, error.kind()))?;
    for role in source.roles() {
        let tenant = match role.tenant_scope() {
            ApplicationSourceTenantScope::Global => None,
            ApplicationSourceTenantScope::Tenant => {
                Some(TenantId::new("application-check").map_err(|_| ScaffoldError::CompileRole)?)
            }
        };
        let compiled_role = if matches!(
            source.schema(),
            riffdb_query_module::APPLICATION_SOURCE_SCHEMA_V4
                | riffdb_query_module::APPLICATION_SOURCE_SCHEMA_V5
                | riffdb_query_module::APPLICATION_SOURCE_SCHEMA_V6
                | riffdb_query_module::APPLICATION_SOURCE_SCHEMA_V7
        ) {
            compile_application_role_v2(
                &exact,
                role.name(),
                tenant,
                &contract,
                &modules,
                &reactive_modules,
            )
        } else {
            compile_application_role(&exact, role.name(), tenant, &contract, &modules)
        };
        compiled_role.map_err(|error| {
            let over_budget_query = (error.kind()
                == riffdb_query_module::ApplicationRoleErrorKind::RequirementLimit)
                .then(|| {
                    role.queries().iter().find_map(|query_name| {
                        modules
                            .iter()
                            .find_map(|module| module.query(query_name))
                            .filter(|query| {
                                query.plan().authorization_cost().scanned_index_rows() > 500
                            })
                            .map(|query| query.name())
                    })
                })
                .flatten();
            role_diagnostic(source_path, role.name(), over_budget_query, error.kind())
        })?;
    }
    let [module] = modules.as_slice() else {
        return Err(ScaffoldError::Manifest);
    };
    let mut outputs = vec![(
        EXACT_MANIFEST_PATH.to_owned(),
        exact.canonical_bytes().to_vec(),
    )];
    if let Some(path) = source.generation().rust() {
        outputs.push((
            path.to_owned(),
            generate_rust_application_client(module, &contract, &reactive_modules).into_bytes(),
        ));
    }
    if let Some(path) = source.generation().typescript() {
        outputs.push((
            path.to_owned(),
            generate_typescript_application_client(module, &contract, &reactive_modules)
                .into_bytes(),
        ));
    }
    let runtime_operation_catalog = if include_runtime_operation_catalog {
        Some(
            render_application_operation_catalog(&exact, module, &contract, &reactive_modules)?
                .into_bytes(),
        )
    } else {
        None
    };
    if let Some(path) = source.generation().mcp() {
        let generated_mcp = match runtime_operation_catalog.as_ref() {
            Some(catalog) => catalog.clone(),
            None => {
                render_application_operation_catalog(&exact, module, &contract, &reactive_modules)?
                    .into_bytes()
            }
        };
        outputs.push((path.to_owned(), generated_mcp));
    }
    if let Some(path) = source.generation().python() {
        outputs.push((
            path.to_owned(),
            generate_python_application_client(module, &contract, &reactive_modules)
                .map_err(|error| {
                    python_generation_diagnostic(
                        source.contract().source(),
                        &contract_source,
                        &python_query_sources,
                        &error,
                    )
                })?
                .into_bytes(),
        ));
    }
    if let Some(path) = source.generation().go() {
        outputs.push((
            path.to_owned(),
            generate_go_application_client(module, &contract, &reactive_modules).into_bytes(),
        ));
    }
    if lock_v3 {
        outputs.push((
            CONTRACT_BUNDLE_ARTIFACT_PATH.to_owned(),
            contract.canonical_bytes().to_vec(),
        ));
    }
    outputs.extend(reactive_outputs);
    let artifacts = outputs
        .iter()
        .map(|(path, bytes)| {
            let kind = if path == EXACT_MANIFEST_PATH {
                GeneratedApplicationArtifactKind::Manifest
            } else if source.generation().rust() == Some(path.as_str()) {
                GeneratedApplicationArtifactKind::Rust
            } else if source.generation().typescript() == Some(path.as_str()) {
                GeneratedApplicationArtifactKind::TypeScript
            } else if source.generation().python() == Some(path.as_str()) {
                GeneratedApplicationArtifactKind::Python
            } else if source.generation().go() == Some(path.as_str()) {
                GeneratedApplicationArtifactKind::Go
            } else if path == CONTRACT_BUNDLE_ARTIFACT_PATH {
                GeneratedApplicationArtifactKind::ContractBundle
            } else if path.ends_with(".riffdb.reactive.module") {
                GeneratedApplicationArtifactKind::ReactiveModule
            } else if source.generation().mcp() == Some(path.as_str()) {
                GeneratedApplicationArtifactKind::Mcp
            } else {
                return Err(ScaffoldError::ApplicationLock);
            };
            GeneratedApplicationArtifact::new(kind, path.clone(), bytes)
                .map_err(|error| lock_diagnostic(Path::new(DEFAULT_LOCK_PATH), error.kind()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let lock = if source.schema() == riffdb_query_module::APPLICATION_SOURCE_SCHEMA_V7 {
        ApplicationLock::compile_v8(
            &source,
            &exact,
            &contract,
            &modules,
            &reactive_modules,
            &artifacts,
            &migration_inputs,
        )
    } else if source.schema() == riffdb_query_module::APPLICATION_SOURCE_SCHEMA_V6 {
        ApplicationLock::compile_v7(
            &source,
            &exact,
            &contract,
            &modules,
            &reactive_modules,
            &artifacts,
            &migration_inputs,
        )
    } else if source.schema() == riffdb_query_module::APPLICATION_SOURCE_SCHEMA_V5 {
        ApplicationLock::compile_v6(
            &source,
            &exact,
            &contract,
            &modules,
            &reactive_modules,
            &artifacts,
            &migration_inputs,
        )
    } else if source.schema() == riffdb_query_module::APPLICATION_SOURCE_SCHEMA_V4 {
        ApplicationLock::compile_v5(
            &source,
            &exact,
            &contract,
            &modules,
            &reactive_modules,
            &artifacts,
            &migration_inputs,
        )
    } else if source.schema() == riffdb_query_module::APPLICATION_SOURCE_SCHEMA_V3 {
        ApplicationLock::compile_v4(
            &source,
            &exact,
            &contract,
            &modules,
            &artifacts,
            &migration_inputs,
        )
    } else if lock_v3 {
        ApplicationLock::compile_v3(&source, &exact, &contract, &modules, &artifacts)
    } else {
        ApplicationLock::compile(&source, &exact, &contract, &modules, &artifacts)
    }
    .map_err(|error| lock_diagnostic(Path::new(DEFAULT_LOCK_PATH), error.kind()))?;
    outputs.extend(migration_outputs);
    Ok(CompiledSymbolicApplication {
        lock,
        outputs,
        runtime_operation_catalog,
    })
}

impl std::error::Error for ScaffoldError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl ScaffoldError {
    pub(crate) fn diagnostics(&self) -> Option<&AuthoringDiagnostics> {
        match self {
            Self::Authoring(diagnostics) => Some(diagnostics),
            _ => None,
        }
    }
}

impl From<io::Error> for ScaffoldError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

fn application_source_diagnostic(
    path: &Path,
    kind: riffdb_query_module::ApplicationSourceErrorKind,
) -> ScaffoldError {
    diagnostic_path(path)
        .and_then(|path| AuthoringDiagnostics::from_application_source(path, kind).ok())
        .map_or(ScaffoldError::ApplicationSource, ScaffoldError::Authoring)
}

fn lock_diagnostic(
    path: &Path,
    kind: riffdb_query_module::ApplicationLockErrorKind,
) -> ScaffoldError {
    diagnostic_path(path)
        .and_then(|path| AuthoringDiagnostics::from_lock(path, kind).ok())
        .map_or(ScaffoldError::ApplicationLock, ScaffoldError::Authoring)
}

fn contract_diagnostic(
    path: &str,
    error: &riffdb_contract_compiler::CompilationError,
) -> ScaffoldError {
    contract_authoring_diagnostics(path, error)
        .map_or(ScaffoldError::CompileContract, ScaffoldError::Authoring)
}

/// Converts one compiler-owned contract failure through the exact authoring
/// diagnostic path shared by push/check and the local LSP surface.
pub(crate) fn contract_authoring_diagnostics(
    path: &str,
    error: &riffdb_contract_compiler::CompilationError,
) -> Option<AuthoringDiagnostics> {
    AuthoringSourcePath::new(path)
        .ok()
        .and_then(|path| AuthoringDiagnostics::from_contract(path, error).ok())
}

fn query_diagnostic(path: &str, error: &riffdb_query_module::QueryModuleError) -> ScaffoldError {
    AuthoringSourcePath::new(path)
        .ok()
        .and_then(|path| AuthoringDiagnostics::from_query_module(path, error).ok())
        .map_or(ScaffoldError::CompileQuery, ScaffoldError::Authoring)
}

fn python_generation_diagnostic(
    contract_path: &str,
    contract_source: &str,
    query_sources: &[(String, String, String)],
    error: &PythonGenerationError,
) -> ScaffoldError {
    let source_pairs = query_sources
        .iter()
        .map(|(name, _, source)| (name.as_str(), source.as_str()))
        .collect::<Vec<_>>();
    let Some(location) = error.locate(contract_source, &source_pairs) else {
        return ScaffoldError::GeneratePython;
    };
    let source_path = location.query_name().map_or(contract_path, |query_name| {
        query_sources
            .iter()
            .find_map(|(name, path, _)| (name == query_name).then_some(path.as_str()))
            .unwrap_or(contract_path)
    });
    AuthoringSourcePath::new(source_path)
        .ok()
        .and_then(|path| {
            AuthoringDiagnostics::python_name_collision(
                path,
                location.span(),
                location.symbol_path().to_vec(),
            )
            .ok()
        })
        .map_or(ScaffoldError::GeneratePython, ScaffoldError::Authoring)
}

fn role_diagnostic(
    path: &Path,
    role: &str,
    query: Option<&str>,
    kind: riffdb_query_module::ApplicationRoleErrorKind,
) -> ScaffoldError {
    diagnostic_path(path)
        .and_then(|path| match query {
            Some(query) => AuthoringDiagnostics::role_query_scan_budget(path, role, query).ok(),
            None => AuthoringDiagnostics::from_role(path, kind, Some(role)).ok(),
        })
        .map_or(ScaffoldError::CompileRole, ScaffoldError::Authoring)
}

fn filesystem_diagnostic(
    path: &Path,
    class: FilesystemDiagnosticClass,
    disposition: FileChangeDisposition,
) -> ScaffoldError {
    diagnostic_path(path)
        .and_then(|path| AuthoringDiagnostics::filesystem(path, class, disposition).ok())
        .map_or(ScaffoldError::UnsafePath, ScaffoldError::Authoring)
}

fn diagnostic_path(path: &Path) -> Option<AuthoringSourcePath> {
    let relative = if path.is_absolute() {
        path.file_name()?.to_str()?
    } else {
        path.to_str()?
    };
    AuthoringSourcePath::new(relative).ok()
}

pub(crate) fn create_application(
    application: &str,
    language: ScaffoldLanguage,
    destination: &Path,
) -> Result<(), ScaffoldError> {
    if !valid_application_name(application) {
        return Err(ScaffoldError::InvalidApplicationName);
    }
    let destination_was_empty = inspect_scaffold_destination(destination)?;
    let publish_destination = if destination_was_empty {
        fs::canonicalize(destination)?
    } else {
        destination.to_path_buf()
    };

    let contract_name = pascal(application);
    let module_name = application.replace('-', "_");
    let module_client = format!("{}Client", pascal(&module_name));
    let role_name = format!("{contract_name}Application");
    let contract_source = render(
        CONTRACT_TEMPLATE,
        application,
        &contract_name,
        &role_name,
        &module_client,
    );
    let query_source = render(
        QUERY_TEMPLATE,
        application,
        &contract_name,
        &role_name,
        &module_client,
    );
    let contract =
        compile_contract_source(&contract_source).map_err(|_| ScaffoldError::CompileContract)?;
    let candidate = QueryModuleCandidate::new(
        QueryModuleName::new(module_name.clone()).map_err(|_| ScaffoldError::CompileQuery)?,
        QueryModuleVersion::new(1).ok_or(ScaffoldError::CompileQuery)?,
        vec![
            NamedQuerySource::new("ItemPage", query_source.clone())
                .map_err(|_| ScaffoldError::CompileQuery)?,
        ],
    )
    .map_err(|_| ScaffoldError::CompileQuery)?;
    let module =
        QueryModule::compile(candidate, &contract).map_err(|_| ScaffoldError::CompileQuery)?;
    let python_generation_path = format!("src/{}/generated.py", application.replace('-', "_"));
    let generation = json!({
        "go": "generated/go/client.go",
        "mcp": "generated/mcp/tools.json",
        "python": python_generation_path,
        "rust": "generated/rust/client.rs",
        "typescript": "generated/typescript/client.ts",
    });
    let application_source = serde_json::to_string(&json!({
        "application": application,
        "contract": {
            "lineage": contract.lineage().as_str(),
            "source": "riffdb/contract.riff",
            "version": contract.contract_version().get(),
        },
        "generation": generation,
        "migrations": [],
        "query_modules": [{
            "name": module_name,
            "queries": [{
                "name": "ItemPage",
                "source": "riffdb/queries/item_page.riffq",
            }],
            "version": 1,
        }],
        "reactive_modules": [],
        "roles": [{
            "agent_subscriptions": [],
            "commands": ["CreateItem"],
            "environment": "development",
            "event_streams": [],
            "name": role_name,
            "queries": ["ItemPage"],
            "tenant_scope": "global",
            "watch_queries": [],
        }],
        "schema": "riffdb.application-source/v5",
        "seed_inputs": ["riffdb/seed/01-CreateItem.jsonl"],
    }))
    .map_err(|_| ScaffoldError::ApplicationSource)?;
    let source = ApplicationSourceManifest::parse(&application_source)
        .map_err(|_| ScaffoldError::ApplicationSource)?;
    let manifest = source
        .exact_manifest_v2(&contract, std::slice::from_ref(&module), &[])
        .map_err(|_| ScaffoldError::IdentityMismatch)?;
    compile_application_role_v2(
        &manifest,
        &role_name,
        None,
        &contract,
        std::slice::from_ref(&module),
        &[],
    )
    .map_err(|_| ScaffoldError::CompileRole)?;
    let tools = generate_mcp_tools(&module).map_err(|_| ScaffoldError::GenerateMcp)?;
    let sdk_tools =
        generate_sdk_only_query_tools(&module).map_err(|_| ScaffoldError::GenerateMcp)?;
    let commands =
        generate_mcp_commands(&module, &contract).map_err(|_| ScaffoldError::GenerateMcp)?;
    let vector_tools = generate_vector_inspection_tools(&module, &contract)
        .map_err(|_| ScaffoldError::GenerateMcp)?;
    let generated_mcp =
        render_mcp_manifest(&manifest, &tools, &sdk_tools, &commands, &[], &vector_tools)?;
    let generated_rust = generate_rust_client(&module, &contract);
    let generated_typescript = generate_typescript_client(&module, &contract);
    let generated_go = generate_go_application_client(&module, &contract, &[]);
    let generated_python =
        generate_python_client(&module, &contract).map_err(|_| ScaffoldError::GeneratePython)?;
    let mut artifacts = vec![
        GeneratedApplicationArtifact::new(
            GeneratedApplicationArtifactKind::Manifest,
            EXACT_MANIFEST_PATH,
            manifest.canonical_bytes(),
        )
        .map_err(|_| ScaffoldError::ApplicationLock)?,
        GeneratedApplicationArtifact::new(
            GeneratedApplicationArtifactKind::Rust,
            source
                .generation()
                .rust()
                .ok_or(ScaffoldError::ApplicationSource)?,
            generated_rust.as_bytes(),
        )
        .map_err(|_| ScaffoldError::ApplicationLock)?,
        GeneratedApplicationArtifact::new(
            GeneratedApplicationArtifactKind::TypeScript,
            source
                .generation()
                .typescript()
                .ok_or(ScaffoldError::ApplicationSource)?,
            generated_typescript.as_bytes(),
        )
        .map_err(|_| ScaffoldError::ApplicationLock)?,
        GeneratedApplicationArtifact::new(
            GeneratedApplicationArtifactKind::Go,
            source
                .generation()
                .go()
                .ok_or(ScaffoldError::ApplicationSource)?,
            generated_go.as_bytes(),
        )
        .map_err(|_| ScaffoldError::ApplicationLock)?,
        GeneratedApplicationArtifact::new(
            GeneratedApplicationArtifactKind::Mcp,
            source
                .generation()
                .mcp()
                .ok_or(ScaffoldError::ApplicationSource)?,
            generated_mcp.as_bytes(),
        )
        .map_err(|_| ScaffoldError::ApplicationLock)?,
    ];
    artifacts.push(
        GeneratedApplicationArtifact::new(
            GeneratedApplicationArtifactKind::Python,
            source
                .generation()
                .python()
                .ok_or(ScaffoldError::ApplicationSource)?,
            generated_python.as_bytes(),
        )
        .map_err(|_| ScaffoldError::ApplicationLock)?,
    );
    artifacts.push(
        GeneratedApplicationArtifact::new(
            GeneratedApplicationArtifactKind::ContractBundle,
            CONTRACT_BUNDLE_ARTIFACT_PATH,
            contract.canonical_bytes(),
        )
        .map_err(|_| ScaffoldError::ApplicationLock)?,
    );
    let lock = ApplicationLock::compile_v6(
        &source,
        &manifest,
        &contract,
        std::slice::from_ref(&module),
        &[],
        &artifacts,
        &[],
    )
    .map_err(|_| ScaffoldError::ApplicationLock)?;

    let parent = destination_parent(&publish_destination);
    if !parent.is_dir() {
        return Err(filesystem_diagnostic(
            destination,
            FilesystemDiagnosticClass::ParentMissing,
            FileChangeDisposition::NoFilesChanged,
        ));
    }
    let temporary = create_temporary_directory(&parent, application)?;
    let result = write_repository(
        &temporary,
        language,
        application,
        &contract_name,
        &role_name,
        &module_client,
        &contract_source,
        &query_source,
        source.canonical_bytes(),
        lock.canonical_bytes(),
        manifest.canonical_bytes(),
        &generated_rust,
        &generated_typescript,
        &generated_go,
        &generated_python,
        &generated_mcp,
        contract.canonical_bytes(),
    )
    .and_then(|()| {
        publish_scaffold(
            &temporary,
            &publish_destination,
            destination_was_empty,
            &parent,
        )
    });
    if result.is_err() && !destination_was_empty {
        let _ = fs::remove_dir_all(&temporary);
    }
    result
}

/// Renders the minimal schema package used by `riffdb init` without writing
/// application source code, package manifests, credentials, or generated
/// artifacts.
pub(crate) fn render_project_schema(
    application: &str,
    generators: &[crate::config::ProjectGenerator],
) -> Result<Vec<(PathBuf, Vec<u8>)>, ScaffoldError> {
    if !valid_application_name(application) {
        return Err(ScaffoldError::InvalidApplicationName);
    }
    let contract_name = pascal(application);
    let module_name = application.replace('-', "_");
    let contract_source = format!("contract {contract_name} version 1 {{\n}}\n");
    let contract =
        compile_contract_source(&contract_source).map_err(|_| ScaffoldError::CompileContract)?;
    let candidate = QueryModuleCandidate::new(
        QueryModuleName::new(module_name.clone()).map_err(|_| ScaffoldError::CompileQuery)?,
        QueryModuleVersion::new(1).ok_or(ScaffoldError::CompileQuery)?,
        Vec::new(),
    )
    .map_err(|_| ScaffoldError::CompileQuery)?;
    let module =
        QueryModule::compile(candidate, &contract).map_err(|_| ScaffoldError::CompileQuery)?;
    let mut generation = serde_json::Map::new();
    for generator in generators {
        let surface = generator.surface();
        generation.insert(surface.key().to_owned(), json!(surface.default_path()));
    }
    let source_text = serde_json::to_string(&json!({
        "application": application,
        "contract": {
            "lineage": contract.lineage().as_str(),
            "source": "riffdb/contract.riff",
            "version": contract.contract_version().get(),
        },
        "generation": generation,
        "migrations": [],
        "query_modules": [{
            "name": module_name,
            "queries": [],
            "version": 1,
        }],
        "reactive_modules": [],
        "roles": [],
        "schema": "riffdb.application-source/v7",
        "seed_inputs": [],
    }))
    .map_err(|_| ScaffoldError::ApplicationSource)?;
    let source = ApplicationSourceManifest::parse(&source_text)
        .map_err(|_| ScaffoldError::ApplicationSource)?;
    source
        .exact_manifest_v2(&contract, std::slice::from_ref(&module), &[])
        .map_err(|_| ScaffoldError::IdentityMismatch)?;

    Ok(vec![
        (
            PathBuf::from("riffdb/contract.riff"),
            contract_source.into_bytes(),
        ),
        (
            PathBuf::from("riffdb.application.json"),
            source.canonical_bytes().to_vec(),
        ),
    ])
}

fn destination_parent(destination: &Path) -> PathBuf {
    destination
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf()
}

fn inspect_scaffold_destination(destination: &Path) -> Result<bool, ScaffoldError> {
    let metadata = match fs::symlink_metadata(destination) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
            return Err(filesystem_diagnostic(
                destination,
                FilesystemDiagnosticClass::PermissionDenied,
                FileChangeDisposition::NoFilesChanged,
            ));
        }
        Err(error) => return Err(ScaffoldError::Io(error)),
    };
    if metadata.file_type().is_symlink() {
        return Err(filesystem_diagnostic(
            destination,
            FilesystemDiagnosticClass::Symlink,
            FileChangeDisposition::NoFilesChanged,
        ));
    }
    if !metadata.is_dir() {
        return Err(filesystem_diagnostic(
            destination,
            FilesystemDiagnosticClass::NotRegular,
            FileChangeDisposition::NoFilesChanged,
        ));
    }
    let mut entries = fs::read_dir(destination).map_err(|error| {
        if error.kind() == io::ErrorKind::PermissionDenied {
            filesystem_diagnostic(
                destination,
                FilesystemDiagnosticClass::PermissionDenied,
                FileChangeDisposition::NoFilesChanged,
            )
        } else {
            ScaffoldError::Io(error)
        }
    })?;
    if entries.next().transpose()?.is_some() {
        return Err(filesystem_diagnostic(
            destination,
            FilesystemDiagnosticClass::DestinationExists,
            FileChangeDisposition::NoFilesChanged,
        ));
    }
    Ok(true)
}

fn publish_scaffold(
    temporary: &Path,
    destination: &Path,
    destination_was_empty: bool,
    parent: &Path,
) -> Result<(), ScaffoldError> {
    if !destination_was_empty {
        return fs::rename(temporary, destination).map_err(ScaffoldError::Io);
    }

    // Revalidate immediately before publication. Existing directories retain
    // their inode, and the exact lock is withheld until every other known entry
    // has moved and the destination directory has been synchronized.
    if !inspect_scaffold_destination(destination)? {
        return Err(filesystem_diagnostic(
            destination,
            FilesystemDiagnosticClass::InterruptedStaging,
            FileChangeDisposition::StagedFilesDiscarded,
        ));
    }

    let mut entries = fs::read_dir(temporary)?
        .map(|entry| entry.map(|entry| entry.file_name()))
        .collect::<Result<Vec<_>, _>>()?;
    if entries.len() > MAX_SCAFFOLD_TOP_LEVEL_ENTRIES {
        return Err(filesystem_diagnostic(
            destination,
            FilesystemDiagnosticClass::InterruptedStaging,
            FileChangeDisposition::LockNotPublished,
        ));
    }
    sort_scaffold_entries(&mut entries);
    let lock = entries
        .pop()
        .filter(|entry| entry == DEFAULT_LOCK_PATH)
        .ok_or_else(|| {
            filesystem_diagnostic(
                destination,
                FilesystemDiagnosticClass::InterruptedStaging,
                FileChangeDisposition::LockNotPublished,
            )
        })?;
    for entry in entries {
        publish_scaffold_entry(temporary, destination, &entry)?;
    }
    fs::File::open(destination)?.sync_all()?;
    publish_scaffold_entry(temporary, destination, &lock)?;
    let publication = fs::File::open(destination)
        .and_then(|directory| directory.sync_all())
        .and_then(|()| fs::File::open(parent))
        .and_then(|directory| directory.sync_all());
    if let Err(error) = publication {
        if fs::remove_file(destination.join(DEFAULT_LOCK_PATH)).is_err() {
            return Err(ScaffoldError::Io(error));
        }
        let _ = fs::File::open(destination).and_then(|directory| directory.sync_all());
        return Err(filesystem_diagnostic(
            destination,
            FilesystemDiagnosticClass::InterruptedStaging,
            FileChangeDisposition::LockNotPublished,
        ));
    }
    let _ = fs::remove_dir(temporary);
    Ok(())
}

fn publish_scaffold_entry(
    temporary: &Path,
    destination: &Path,
    entry: &std::ffi::OsStr,
) -> Result<(), ScaffoldError> {
    let target = destination.join(entry);
    if fs::symlink_metadata(&target).is_ok() {
        return Err(filesystem_diagnostic(
            destination,
            FilesystemDiagnosticClass::InterruptedStaging,
            FileChangeDisposition::LockNotPublished,
        ));
    }
    fs::rename(temporary.join(entry), target).map_err(|_| {
        filesystem_diagnostic(
            destination,
            FilesystemDiagnosticClass::InterruptedStaging,
            FileChangeDisposition::LockNotPublished,
        )
    })
}

fn sort_scaffold_entries(entries: &mut [std::ffi::OsString]) {
    entries.sort_by(|left, right| {
        let left_is_lock = left == DEFAULT_LOCK_PATH;
        let right_is_lock = right == DEFAULT_LOCK_PATH;
        left_is_lock
            .cmp(&right_is_lock)
            .then_with(|| left.cmp(right))
    });
}

#[allow(clippy::too_many_arguments)]
fn write_repository(
    root: &Path,
    language: ScaffoldLanguage,
    application: &str,
    contract_name: &str,
    role_name: &str,
    module_client: &str,
    contract_source: &str,
    query_source: &str,
    application_source: &[u8],
    application_lock: &[u8],
    exact_manifest: &[u8],
    generated_rust: &str,
    generated_typescript: &str,
    generated_go: &str,
    generated_python: &str,
    generated_mcp: &str,
    contract_bundle: &[u8],
) -> Result<(), ScaffoldError> {
    write_file(root, "riffdb/contract.riff", contract_source.as_bytes())?;
    write_file(
        root,
        "riffdb/queries/item_page.riffq",
        query_source.as_bytes(),
    )?;
    write_file(
        root,
        "riffdb/seed/01-CreateItem.jsonl",
        render(
            SEED_TEMPLATE,
            application,
            contract_name,
            role_name,
            module_client,
        )
        .as_bytes(),
    )?;
    write_file(root, "riffdb.application.json", application_source)?;
    write_file(root, DEFAULT_LOCK_PATH, application_lock)?;
    write_file(root, EXACT_MANIFEST_PATH, exact_manifest)?;
    write_file(root, "generated/rust/client.rs", generated_rust.as_bytes())?;
    write_file(
        root,
        "generated/typescript/client.ts",
        generated_typescript.as_bytes(),
    )?;
    write_file(root, "generated/go/client.go", generated_go.as_bytes())?;
    write_file(root, "generated/mcp/tools.json", generated_mcp.as_bytes())?;
    write_file(root, CONTRACT_BUNDLE_ARTIFACT_PATH, contract_bundle)?;
    let path = format!("src/{}/generated.py", application.replace('-', "_"));
    write_file(root, &path, generated_python.as_bytes())?;
    write_file(
        root,
        "README.md",
        render(
            README_TEMPLATE,
            application,
            contract_name,
            role_name,
            module_client,
        )
        .as_bytes(),
    )?;
    write_file(root, "AUTHORING.md", AUTHORING_TEMPLATE.as_bytes())?;
    write_file(root, ".gitignore", GITIGNORE_TEMPLATE.as_bytes())?;
    match language {
        ScaffoldLanguage::Rust => {
            write_file(
                root,
                "Cargo.toml",
                render(
                    CARGO_TEMPLATE,
                    application,
                    contract_name,
                    role_name,
                    module_client,
                )
                .as_bytes(),
            )?;
            write_file(
                root,
                "Cargo.lock",
                render_cargo_lock(application, contract_name, role_name, module_client)?.as_bytes(),
            )?;
            write_file(
                root,
                "src/main.rs",
                render(
                    RUST_MAIN_TEMPLATE,
                    application,
                    contract_name,
                    role_name,
                    module_client,
                )
                .as_bytes(),
            )?;
            write_file(
                root,
                "src/generated.rs",
                b"#![allow(dead_code)]\ninclude!(\"../generated/rust/client.rs\");\n",
            )?;
        }
        ScaffoldLanguage::Go => {
            write_file(
                root,
                "go.mod",
                render(
                    GO_MOD_TEMPLATE,
                    application,
                    contract_name,
                    role_name,
                    module_client,
                )
                .as_bytes(),
            )?;
            write_file(
                root,
                "main.go",
                render(
                    GO_MAIN_TEMPLATE,
                    application,
                    contract_name,
                    role_name,
                    module_client,
                )
                .as_bytes(),
            )?;
            write_file(
                root,
                "third_party/riffdb-application/go.mod",
                GO_RUNTIME_MOD.as_bytes(),
            )?;
            write_file(
                root,
                "third_party/riffdb-application/runtime.go",
                GO_RUNTIME_SOURCE.as_bytes(),
            )?;
        }
        ScaffoldLanguage::Typescript => {
            write_file(
                root,
                "package.json",
                render(
                    PACKAGE_TEMPLATE,
                    application,
                    contract_name,
                    role_name,
                    module_client,
                )
                .as_bytes(),
            )?;
            write_file(
                root,
                "package-lock.json",
                render_package_lock(application)?.as_bytes(),
            )?;
            write_file(root, "tsconfig.json", TSCONFIG_TEMPLATE.as_bytes())?;
            write_file(
                root,
                "src/main.ts",
                render(
                    TYPESCRIPT_MAIN_TEMPLATE,
                    application,
                    contract_name,
                    role_name,
                    module_client,
                )
                .as_bytes(),
            )?;
            write_file(
                root,
                "vendor/riffdb-application/package.json",
                br#"{
  "name": "@riffdb/application",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "exports": {
    ".": "./dist/index.js"
  },
  "types": "./dist/index.d.ts"
}
"#,
            )?;
            write_file(
                root,
                "vendor/riffdb-application/dist/index.js",
                TYPESCRIPT_RUNTIME_JS.as_bytes(),
            )?;
            write_file(
                root,
                "vendor/riffdb-application/dist/index.d.ts",
                TYPESCRIPT_RUNTIME_TYPES.as_bytes(),
            )?;
            write_file(
                root,
                "vendor/riffdb-application/dist/driver.js",
                TYPESCRIPT_DRIVER_JS.as_bytes(),
            )?;
            write_file(
                root,
                "vendor/riffdb-application/dist/driver.d.ts",
                TYPESCRIPT_DRIVER_TYPES.as_bytes(),
            )?;
            write_file(
                root,
                "node_modules/@riffdb/application/package.json",
                br#"{
  "name": "@riffdb/application",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "exports": {
    ".": "./dist/index.js"
  },
  "types": "./dist/index.d.ts"
}
"#,
            )?;
            write_file(
                root,
                "node_modules/@riffdb/application/dist/index.js",
                TYPESCRIPT_RUNTIME_JS.as_bytes(),
            )?;
            write_file(
                root,
                "node_modules/@riffdb/application/dist/index.d.ts",
                TYPESCRIPT_RUNTIME_TYPES.as_bytes(),
            )?;
            write_file(
                root,
                "node_modules/@riffdb/application/dist/driver.js",
                TYPESCRIPT_DRIVER_JS.as_bytes(),
            )?;
            write_file(
                root,
                "node_modules/@riffdb/application/dist/driver.d.ts",
                TYPESCRIPT_DRIVER_TYPES.as_bytes(),
            )?;
            materialize_installed_typescript_toolchain(root)?;
        }
        ScaffoldLanguage::Python => {
            let (wheel_name, wheel_bytes) = load_python_wheel()?;
            let package_name = application.replace('-', "_");
            write_file(
                root,
                "pyproject.toml",
                render_python(
                    PYPROJECT_TEMPLATE,
                    application,
                    &package_name,
                    &wheel_name,
                    module_client,
                )
                .as_bytes(),
            )?;
            write_file(
                root,
                "uv.lock",
                render_uv_lock(application, &wheel_name, &wheel_bytes).as_bytes(),
            )?;
            write_file(root, &format!("vendor/{wheel_name}"), &wheel_bytes)?;
            write_file(root, &format!("src/{package_name}/__init__.py"), b"")?;
            write_file(
                root,
                &format!("src/{package_name}/__main__.py"),
                render_python(
                    PYTHON_MAIN_TEMPLATE,
                    application,
                    &package_name,
                    &wheel_name,
                    module_client,
                )
                .as_bytes(),
            )?;
            write_file(
                root,
                "tests/test_generated.py",
                format!(
                    "import unittest\n\n\nclass GeneratedImportTest(unittest.TestCase):\n    def test_generated_import(self) -> None:\n        import {package_name}.generated\n"
                )
                .as_bytes(),
            )?;
        }
    }
    Ok(())
}

fn render_cargo_lock(
    application: &str,
    contract_name: &str,
    role_name: &str,
    module_client: &str,
) -> Result<String, ScaffoldError> {
    let mut sections = CARGO_LOCK_TEMPLATE
        .split("\n\n")
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let header = (!sections.is_empty())
        .then(|| sections.remove(0))
        .ok_or(ScaffoldError::RustDependencyLock)?;
    let application_section = sections
        .iter()
        .position(|section| section.contains("name = \"{{APPLICATION_NAME}}\""))
        .map(|index| sections.remove(index))
        .ok_or(ScaffoldError::RustDependencyLock)?;
    if sections
        .iter()
        .any(|section| section.contains("{{APPLICATION_NAME}}"))
    {
        return Err(ScaffoldError::RustDependencyLock);
    }
    let application_section = render(
        &application_section,
        application,
        contract_name,
        role_name,
        module_client,
    );
    let mut insertion = sections.len();
    for (index, section) in sections.iter().enumerate() {
        let name = cargo_lock_package_name(section).ok_or(ScaffoldError::RustDependencyLock)?;
        if name >= application {
            insertion = index;
            break;
        }
    }
    sections.insert(insertion, application_section);
    let mut rendered = Vec::with_capacity(sections.len() + 1);
    rendered.push(header);
    rendered.extend(sections);
    Ok(rendered.join("\n\n"))
}

fn cargo_lock_package_name(section: &str) -> Option<&str> {
    section.lines().find_map(|line| {
        line.strip_prefix("name = \"")
            .and_then(|name| name.strip_suffix('"'))
    })
}

fn render_package_lock(application: &str) -> Result<String, ScaffoldError> {
    let mut lock: serde_json::Value =
        serde_json::from_str(PACKAGE_LOCK_BASE).map_err(|_| ScaffoldError::ApplicationSource)?;
    let root = lock
        .as_object_mut()
        .ok_or(ScaffoldError::ApplicationSource)?;
    root.insert("name".to_owned(), json!(application));
    let packages = root
        .get_mut("packages")
        .and_then(serde_json::Value::as_object_mut)
        .ok_or(ScaffoldError::ApplicationSource)?;
    let package = packages
        .get_mut("")
        .and_then(serde_json::Value::as_object_mut)
        .ok_or(ScaffoldError::ApplicationSource)?;
    package.insert("name".to_owned(), json!(application));
    package.insert(
        "dependencies".to_owned(),
        json!({"@riffdb/application": "file:vendor/riffdb-application"}),
    );
    packages.insert(
        "node_modules/@riffdb/application".to_owned(),
        json!({
            "resolved": "vendor/riffdb-application",
            "link": true
        }),
    );
    packages.insert(
        "vendor/riffdb-application".to_owned(),
        json!({
            "name": "@riffdb/application",
            "version": "0.1.0"
        }),
    );
    let mut rendered =
        serde_json::to_string_pretty(&lock).map_err(|_| ScaffoldError::ApplicationSource)?;
    rendered.push('\n');
    Ok(rendered)
}

fn load_python_wheel() -> Result<(String, Vec<u8>), ScaffoldError> {
    let explicit = std::env::var_os("RIFFDB_APPLICATION_WHEEL").map(PathBuf::from);
    let installed = std::env::current_exe().ok().and_then(|executable| {
        let root = executable.parent()?.parent()?;
        let directory = root.join("public/python");
        let mut wheels = fs::read_dir(directory)
            .ok()?
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name()
                    .and_then(std::ffi::OsStr::to_str)
                    .is_some_and(valid_python_wheel_name)
            })
            .collect::<Vec<_>>();
        wheels.sort();
        (wheels.len() == 1).then(|| wheels.remove(0))
    });
    let path = explicit
        .or(installed)
        .ok_or(ScaffoldError::PythonWheelUnavailable)?;
    let name = path
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .filter(|name| valid_python_wheel_name(name))
        .ok_or(ScaffoldError::UnsafePath)?
        .to_owned();
    let bytes = read_bounded(&path, 128 * 1_024 * 1_024)?;
    Ok((name, bytes))
}

fn valid_python_wheel_name(name: &str) -> bool {
    name.starts_with("riffdb_application-0.1.0-cp313-abi3-")
        && name.ends_with(".whl")
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

fn render_python(
    template: &str,
    application: &str,
    package: &str,
    wheel: &str,
    module_client: &str,
) -> String {
    template
        .replace("{{APPLICATION_NAME}}", application)
        .replace("{{PACKAGE_NAME}}", package)
        .replace("{{WHEEL_NAME}}", wheel)
        .replace("{{MODULE_CLIENT}}", module_client)
}

fn render_uv_lock(application: &str, wheel: &str, wheel_bytes: &[u8]) -> String {
    use sha2::{Digest as _, Sha256};

    let wheel_hash = hex(&Sha256::digest(wheel_bytes));
    format!(
        "version = 1\nrevision = 3\nrequires-python = \">=3.13\"\n\n\
         [[package]]\nname = \"{application}\"\nversion = \"0.1.0\"\nsource = {{ virtual = \".\" }}\n\
         dependencies = [\n    {{ name = \"riffdb-application\" }},\n]\n\n\
         [package.metadata]\nrequires-dist = [{{ name = \"riffdb-application\", path = \"vendor/{wheel}\" }}]\n\n\
         [[package]]\nname = \"riffdb-application\"\nversion = \"0.1.0\"\n\
         source = {{ path = \"vendor/{wheel}\" }}\nwheels = [\n    {{ filename = \"{wheel}\", hash = \"sha256:{wheel_hash}\" }},\n]\n"
    )
}

fn materialize_installed_typescript_toolchain(root: &Path) -> Result<(), ScaffoldError> {
    let explicit = std::env::var_os("RIFFDB_TYPESCRIPT_TOOLCHAIN").map(PathBuf::from);
    let installed = std::env::current_exe().ok().and_then(|executable| {
        executable
            .parent()
            .and_then(Path::parent)
            .map(|root| root.join("public/typescript/node_modules"))
    });
    let source = if let Some(explicit) = explicit {
        explicit
    } else if let Some(installed) = installed.filter(|path| path.is_dir()) {
        installed
    } else {
        return Ok(());
    };
    if !source.is_dir() {
        return Err(ScaffoldError::Io(io::Error::new(
            io::ErrorKind::NotFound,
            "TypeScript offline toolchain is unavailable",
        )));
    }
    let mut budget = TypeScriptCopyBudget::default();
    copy_typescript_tree(&source, &root.join("node_modules"), &source, 0, &mut budget)
}

#[derive(Default)]
struct TypeScriptCopyBudget {
    files: usize,
    bytes: u64,
}

fn copy_typescript_tree(
    source: &Path,
    destination: &Path,
    source_root: &Path,
    depth: usize,
    budget: &mut TypeScriptCopyBudget,
) -> Result<(), ScaffoldError> {
    if depth > 32 {
        return Err(ScaffoldError::SourceLimit);
    }
    let metadata = fs::symlink_metadata(source)?;
    if metadata.file_type().is_symlink() {
        let target = fs::canonicalize(source)?;
        let source_root = fs::canonicalize(source_root)?;
        if !target.starts_with(&source_root) {
            return Err(ScaffoldError::UnsafePath);
        }
        let relative_target = fs::read_link(source)?;
        if relative_target.is_absolute()
            || relative_target
                .components()
                .any(|component| matches!(component, std::path::Component::RootDir))
        {
            return Err(ScaffoldError::UnsafePath);
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            if fs::symlink_metadata(destination).is_ok() {
                return Err(ScaffoldError::UnsafePath);
            }
            symlink(relative_target, destination)?;
            return Ok(());
        }
        #[cfg(not(unix))]
        {
            let _ = (relative_target, depth, budget);
            return Err(ScaffoldError::UnsafePath);
        }
    }
    if metadata.is_dir() {
        fs::create_dir_all(destination)?;
        let mut entries = fs::read_dir(source)?.collect::<Result<Vec<_>, _>>()?;
        entries.sort_by_key(fs::DirEntry::file_name);
        for entry in entries {
            copy_typescript_tree(
                &entry.path(),
                &destination.join(entry.file_name()),
                source_root,
                depth + 1,
                budget,
            )?;
        }
        return Ok(());
    }
    if !metadata.is_file() {
        return Err(ScaffoldError::UnsafePath);
    }
    budget.files = budget
        .files
        .checked_add(1)
        .ok_or(ScaffoldError::SourceLimit)?;
    budget.bytes = budget
        .bytes
        .checked_add(metadata.len())
        .ok_or(ScaffoldError::SourceLimit)?;
    if budget.files > MAX_TYPESCRIPT_TOOLCHAIN_FILES
        || budget.bytes > MAX_TYPESCRIPT_TOOLCHAIN_BYTES
    {
        return Err(ScaffoldError::SourceLimit);
    }
    if fs::symlink_metadata(destination).is_ok() {
        return Err(ScaffoldError::UnsafePath);
    }
    fs::copy(source, destination)?;
    Ok(())
}

fn render_application_operation_catalog(
    manifest: &ApplicationManifest,
    module: &QueryModule,
    contract: &ContractBundle,
    reactive_modules: &[ReactiveModulePlanV1],
) -> Result<String, ScaffoldError> {
    let tools = generate_mcp_tools(module).map_err(|_| ScaffoldError::GenerateMcp)?;
    let sdk_tools =
        generate_sdk_only_query_tools(module).map_err(|_| ScaffoldError::GenerateMcp)?;
    let commands =
        generate_mcp_commands(module, contract).map_err(|_| ScaffoldError::GenerateMcp)?;
    let reactive_tools = reactive_modules
        .iter()
        .map(|module| generate_mcp_reactive_tools(module, contract))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| ScaffoldError::GenerateMcp)?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let vector_tools = generate_vector_inspection_tools(module, contract)
        .map_err(|_| ScaffoldError::GenerateMcp)?;
    render_mcp_manifest(
        manifest,
        &tools,
        &sdk_tools,
        &commands,
        &reactive_tools,
        &vector_tools,
    )
}

fn render_mcp_manifest(
    manifest: &ApplicationManifest,
    tools: &[GeneratedMcpTool],
    sdk_tools: &[GeneratedMcpTool],
    commands: &[GeneratedMcpCommand],
    reactive_tools: &[GeneratedMcpReactiveTool],
    vector_tools: &[GeneratedVectorInspectionTool],
) -> Result<String, ScaffoldError> {
    let tools = tools
        .iter()
        .map(render_query_registry_entry)
        .collect::<Result<Vec<_>, ScaffoldError>>()?;
    let sdk_tools = sdk_tools
        .iter()
        .map(render_query_registry_entry)
        .collect::<Result<Vec<_>, ScaffoldError>>()?;
    let commands = commands
        .iter()
        .map(|command| {
            Ok(json!({
                "name": command.name,
                "operation_name": command.operation_name,
                "title": command.title,
                "description": command.description,
                "contract_bundle_hash": hex(&command.contract_bundle_hash),
                "plan_hash": hex(&command.plan_hash),
                "input_schema": serde_json::from_str::<serde_json::Value>(&command.input_schema)
                    .map_err(|_| ScaffoldError::GenerateMcp)?,
                "result_schema": serde_json::from_str::<serde_json::Value>(&command.result_schema)
                    .map_err(|_| ScaffoldError::GenerateMcp)?,
                "annotations": {
                    "readOnlyHint": false,
                    "destructiveHint": true,
                    "idempotentHint": true,
                    "openWorldHint": false,
                },
            }))
        })
        .collect::<Result<Vec<_>, ScaffoldError>>()?;
    let reactive_tools = reactive_tools
        .iter()
        .map(|tool| {
            Ok(json!({
                "name": tool.name,
                "operation_name": tool.operation_name,
                "action": tool.action,
                "operation_kind": tool.operation_kind,
                "reaction_name": tool.reaction_name,
                "reaction_command_name": tool.reaction_command_name,
                "reaction_command_id": tool.reaction_command_id,
                "title": tool.title,
                "description": tool.description,
                "reactive_module_hash": hex(&tool.reactive_module_hash),
                "input_schema": serde_json::from_str::<serde_json::Value>(&tool.input_schema)
                    .map_err(|_| ScaffoldError::GenerateMcp)?,
                "result_schema": serde_json::from_str::<serde_json::Value>(&tool.result_schema)
                    .map_err(|_| ScaffoldError::GenerateMcp)?,
                "annotations": {
                    "readOnlyHint": tool.name.ends_with("_next")
                        || tool.name.ends_with("_status")
                        || tool.name.ends_with("_watch"),
                    "destructiveHint": tool.name.ends_with("_seek"),
                    "idempotentHint": true,
                    "openWorldHint": false,
                },
            }))
        })
        .collect::<Result<Vec<_>, ScaffoldError>>()?;
    let vector_tools = vector_tools
        .iter()
        .map(|tool| {
            Ok(json!({
                "name": tool.name,
                "entity": tool.entity,
                "field": tool.field,
                "inspection_kind": tool.inspection_kind,
                "source_queries": tool.source_queries,
                "module_hash": hex(&tool.module_hash),
                "contract_bundle_hash": hex(&tool.contract_bundle_hash),
                "input_schema": serde_json::from_str::<serde_json::Value>(&tool.input_schema)
                    .map_err(|_| ScaffoldError::GenerateMcp)?,
                "result_schema": serde_json::from_str::<serde_json::Value>(&tool.result_schema)
                    .map_err(|_| ScaffoldError::GenerateMcp)?,
                "annotations": {
                    "readOnlyHint": true,
                    "destructiveHint": false,
                    "idempotentHint": true,
                    "openWorldHint": false,
                },
            }))
        })
        .collect::<Result<Vec<_>, ScaffoldError>>()?;
    let mut value = json!({
        "schema": "riffdb-generated-application-operations/v2",
        "application_manifest_hash": hex(manifest.identity().as_bytes()),
        "tools": tools,
        "commands": commands,
        "reactive_tools": reactive_tools,
    });
    if !sdk_tools.is_empty() {
        value["schema"] =
            serde_json::Value::String("riffdb-generated-application-operations/v3".to_owned());
        value
            .as_object_mut()
            .ok_or(ScaffoldError::GenerateMcp)?
            .insert("sdk_tools".to_owned(), serde_json::Value::Array(sdk_tools));
    }
    if !vector_tools.is_empty() {
        value["schema"] =
            serde_json::Value::String("riffdb-generated-application-operations/v4".to_owned());
        value
            .as_object_mut()
            .ok_or(ScaffoldError::GenerateMcp)?
            .insert(
                "vector_tools".to_owned(),
                serde_json::Value::Array(vector_tools),
            );
    }
    let mut output =
        serde_json::to_string_pretty(&value).map_err(|_| ScaffoldError::GenerateMcp)?;
    output.push('\n');
    Ok(output)
}

fn render_query_registry_entry(
    tool: &GeneratedMcpTool,
) -> Result<serde_json::Value, ScaffoldError> {
    Ok(json!({
        "name": tool.name,
        "operation_name": tool.operation_name,
        "title": tool.title,
        "description": tool.description,
        "module_hash": hex(&tool.module_hash),
        "input_schema": serde_json::from_str::<serde_json::Value>(&tool.input_schema)
            .map_err(|_| ScaffoldError::GenerateMcp)?,
        "result_schema": serde_json::from_str::<serde_json::Value>(&tool.result_schema)
            .map_err(|_| ScaffoldError::GenerateMcp)?,
        "annotations": {
            "readOnlyHint": true,
            "destructiveHint": false,
            "idempotentHint": true,
            "openWorldHint": false,
        },
    }))
}

fn create_temporary_directory(parent: &Path, application: &str) -> Result<PathBuf, ScaffoldError> {
    for suffix in 0..128_u8 {
        let path = parent.join(format!(
            ".{application}.riffdb-new-{}-{suffix}",
            std::process::id()
        ));
        match fs::create_dir(&path) {
            Ok(()) => return Ok(path),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(ScaffoldError::Io(error)),
        }
    }
    Err(ScaffoldError::Io(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "temporary scaffold directory unavailable",
    )))
}

fn write_file(root: &Path, relative: &str, bytes: &[u8]) -> Result<(), ScaffoldError> {
    let path = root.join(relative);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, bytes)?;
    Ok(())
}

fn atomic_write_workspace(root: &Path, relative: &str, bytes: &[u8]) -> Result<(), ScaffoldError> {
    if !valid_relative_path(relative) {
        return Err(filesystem_diagnostic(
            Path::new(relative),
            FilesystemDiagnosticClass::EscapesWorkspace,
            FileChangeDisposition::LockNotPublished,
        ));
    }
    let root = fs::canonicalize(root)?;
    let path = root.join(relative);
    ensure_safe_parent(&root, &path)?;
    atomic_write_absolute(&path, bytes)
}

fn atomic_write_absolute(path: &Path, bytes: &[u8]) -> Result<(), ScaffoldError> {
    let parent = path.parent().ok_or(ScaffoldError::UnsafePath)?;
    fs::create_dir_all(parent)?;
    if fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
        return Err(filesystem_diagnostic(
            path,
            FilesystemDiagnosticClass::Symlink,
            FileChangeDisposition::LockNotPublished,
        ));
    }
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(ScaffoldError::UnsafePath)?;
    let mut temporary = None;
    for suffix in 0..128_u8 {
        let candidate = parent.join(format!(
            ".{name}.riffdb-write-{}-{suffix}",
            std::process::id()
        ));
        match fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&candidate)
        {
            Ok(file) => {
                temporary = Some((candidate, file));
                break;
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(ScaffoldError::Io(error)),
        }
    }
    let (temporary_path, mut file) = temporary.ok_or_else(|| {
        ScaffoldError::Io(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "temporary application output unavailable",
        ))
    })?;
    let result = (|| {
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temporary_path, path)?;
        fs::File::open(parent)?.sync_all()?;
        Ok::<(), io::Error>(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary_path);
    }
    result.map_err(ScaffoldError::Io)
}

fn workspace_lock_path(root: &Path, lock_path: Option<&Path>) -> Result<PathBuf, ScaffoldError> {
    let lock_path = lock_path.unwrap_or_else(|| Path::new(DEFAULT_LOCK_PATH));
    if lock_path.is_absolute() {
        return Err(filesystem_diagnostic(
            lock_path,
            FilesystemDiagnosticClass::EscapesWorkspace,
            FileChangeDisposition::NoFilesChanged,
        ));
    }
    let relative = lock_path.to_str().ok_or(ScaffoldError::UnsafePath)?;
    if !valid_relative_path(relative) {
        return Err(filesystem_diagnostic(
            lock_path,
            FilesystemDiagnosticClass::EscapesWorkspace,
            FileChangeDisposition::NoFilesChanged,
        ));
    }
    let canonical_root = fs::canonicalize(root)?;
    let path = canonical_root.join(lock_path);
    ensure_safe_parent(&canonical_root, &path)?;
    Ok(path)
}

fn read_workspace_file(
    root: &Path,
    relative: &str,
    maximum: usize,
) -> Result<Vec<u8>, ScaffoldError> {
    if !valid_relative_path(relative) {
        return Err(filesystem_diagnostic(
            Path::new(relative),
            FilesystemDiagnosticClass::EscapesWorkspace,
            FileChangeDisposition::NoFilesChanged,
        ));
    }
    let root = fs::canonicalize(root)?;
    let path = fs::canonicalize(root.join(relative))?;
    if !path.starts_with(&root) {
        return Err(filesystem_diagnostic(
            Path::new(relative),
            FilesystemDiagnosticClass::EscapesWorkspace,
            FileChangeDisposition::NoFilesChanged,
        ));
    }
    read_bounded(&path, maximum)
}

fn read_workspace_text(
    root: &Path,
    relative: &str,
    maximum: usize,
) -> Result<String, ScaffoldError> {
    String::from_utf8(read_workspace_file(root, relative, maximum)?)
        .map_err(|_| ScaffoldError::CompileQuery)
}

fn ensure_safe_parent(root: &Path, path: &Path) -> Result<(), ScaffoldError> {
    let parent = path.parent().ok_or(ScaffoldError::UnsafePath)?;
    let relative = parent
        .strip_prefix(root)
        .map_err(|_| ScaffoldError::UnsafePath)?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        current.push(component);
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(filesystem_diagnostic(
                    path,
                    FilesystemDiagnosticClass::Symlink,
                    FileChangeDisposition::LockNotPublished,
                ));
            }
            Ok(metadata) if !metadata.is_dir() => {
                return Err(filesystem_diagnostic(
                    path,
                    FilesystemDiagnosticClass::NotRegular,
                    FileChangeDisposition::LockNotPublished,
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(ScaffoldError::Io(error)),
        }
    }
    Ok(())
}

fn valid_relative_path(path: &str) -> bool {
    !path.is_empty()
        && !path.starts_with('/')
        && !path.starts_with('\\')
        && !path.contains('\\')
        && !path.split('/').any(|part| {
            part.is_empty()
                || part == "."
                || part == ".."
                || part.bytes().any(|byte| byte.is_ascii_control())
        })
}

fn read_bounded(path: &Path, maximum: usize) -> Result<Vec<u8>, ScaffoldError> {
    let metadata = fs::metadata(path)?;
    let length = usize::try_from(metadata.len()).map_err(|_| ScaffoldError::SourceLimit)?;
    if length == 0 || length > maximum {
        return Err(ScaffoldError::SourceLimit);
    }
    let bytes = fs::read(path)?;
    if bytes.len() > maximum {
        return Err(ScaffoldError::SourceLimit);
    }
    Ok(bytes)
}

fn read_bounded_text(path: &Path, maximum: usize) -> Result<String, ScaffoldError> {
    String::from_utf8(read_bounded(path, maximum)?).map_err(|_| ScaffoldError::CompileQuery)
}

fn valid_application_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_APPLICATION_NAME_BYTES
        && value
            .bytes()
            .next()
            .is_some_and(|byte| byte.is_ascii_lowercase())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && !value.ends_with('-')
        && !value.contains("--")
}

fn pascal(value: &str) -> String {
    value
        .split(['-', '_'])
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut characters = part.chars();
            characters.next().map_or_else(String::new, |first| {
                format!("{}{}", first.to_ascii_uppercase(), characters.as_str())
            })
        })
        .collect()
}

fn render(
    template: &str,
    application: &str,
    contract: &str,
    role: &str,
    module_client: &str,
) -> String {
    template
        .replace("{{APPLICATION_NAME}}", application)
        .replace("{{CONTRACT_NAME}}", contract)
        .replace("{{ROLE_NAME}}", role)
        .replace("{{MODULE_CLIENT}}", module_client)
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(output, "{byte:02x}").expect("writing to a string cannot fail");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_application_generation_uses_the_symbolic_lock_rules() {
        let base = tempfile::TempDir::with_prefix("riffdb-v4-generate-dispatch-test-")
            .expect("scratch directory");
        let source_path = base.path().join("riffdb.application.json");
        for schema in [
            "riffdb.application-source/v4",
            "riffdb.application-source/v5",
            "riffdb.application-source/v6",
        ] {
            fs::write(&source_path, format!(r#"{{"schema":"{schema}"}}"#)).expect("source");

            assert!(matches!(
                generate_application(&source_path, false, None),
                Err(ScaffoldError::LockRequired)
            ));
            assert!(matches!(
                generate_application(&source_path, true, None),
                Err(ScaffoldError::Io(error)) if error.kind() == io::ErrorKind::NotFound
            ));
        }
    }

    #[test]
    fn rejects_unsafe_or_ambiguous_names_before_writing() {
        for name in ["", "../app", "App", "app_name", "app--name", "app-"] {
            assert!(!valid_application_name(name), "{name}");
        }
        assert!(valid_application_name("order-desk2"));
    }

    #[test]
    fn v2_migration_preview_is_pure_and_write_changes_only_source() {
        let parent = tempfile::TempDir::with_prefix("riffdb-application-migrate-test-")
            .expect("scratch directory");
        let base = parent.path().join("app");
        create_application("migrate-app", ScaffoldLanguage::Rust, &base).expect("scaffold");
        let source_path = base.join("riffdb.application.json");
        let lock_path = base.join(DEFAULT_LOCK_PATH);
        let mut legacy: serde_json::Value =
            serde_json::from_slice(&fs::read(&source_path).expect("source JSON"))
                .expect("source value");
        legacy["schema"] = json!(riffdb_query_module::APPLICATION_SOURCE_SCHEMA_V1);
        legacy
            .as_object_mut()
            .expect("source object")
            .remove("migrations");
        legacy
            .as_object_mut()
            .expect("source object")
            .remove("reactive_modules");
        let generation = legacy["generation"].as_object_mut().expect("generation");
        generation.remove("go");
        generation.remove("python");
        for role in legacy["roles"].as_array_mut().expect("roles") {
            let role = role.as_object_mut().expect("role");
            role.remove("agent_subscriptions");
            role.remove("event_streams");
            role.remove("watch_queries");
        }
        let legacy = ApplicationSourceManifest::parse(
            &serde_json::to_string(&legacy).expect("legacy source"),
        )
        .expect("legacy V1");
        fs::write(&source_path, legacy.canonical_bytes()).expect("write legacy source");
        let original_source = fs::read(&source_path).expect("source");
        let original_lock = fs::read(&lock_path).expect("lock");

        let preview = migrate_application_source_v2(&source_path, false).expect("preview");
        assert_eq!(
            fs::read(&source_path).expect("unchanged source"),
            original_source
        );
        assert_eq!(fs::read(&lock_path).expect("unchanged lock"), original_lock);
        let migrated = ApplicationSourceManifest::decode_canonical(&preview).expect("canonical v2");
        assert_eq!(
            migrated.schema(),
            riffdb_query_module::APPLICATION_SOURCE_SCHEMA_V2
        );
        assert_eq!(
            migrated.generation().python(),
            Some("generated/python/client.py")
        );

        assert_eq!(
            migrate_application_source_v2(&source_path, true).expect("write"),
            preview
        );
        assert_eq!(fs::read(&source_path).expect("migrated source"), preview);
        assert_eq!(fs::read(&lock_path).expect("unchanged lock"), original_lock);
        assert!(!base.join("generated/python/client.py").exists());
    }

    #[test]
    fn python_name_collision_reports_the_offending_contract_span() {
        let parent = tempfile::TempDir::with_prefix("riffdb-python-collision-test-")
            .expect("scratch directory");
        let base = parent.path().join("app");
        create_application("collision-app", ScaffoldLanguage::Rust, &base).expect("scaffold");
        let source_path = base.join("riffdb.application.json");
        let contract_path = base.join("riffdb/contract.riff");
        let contract = fs::read_to_string(&contract_path).expect("contract");
        let contract = contract.replace(
            "contract CollisionApp version 1 {\n",
            "contract CollisionApp version 1 {\n  enum Collision { fooBar, foo_bar }\n",
        );
        fs::write(&contract_path, &contract).expect("collision contract");

        let diagnostics = match compile_symbolic_application(&source_path) {
            Err(ScaffoldError::Authoring(diagnostics)) => diagnostics,
            Ok(_) | Err(_) => panic!("expected source-spanned authoring diagnostic"),
        };
        let diagnostic = &diagnostics.as_slice()[0];
        let span = diagnostic.span().expect("source span");
        assert_eq!(diagnostic.code().as_str(), "RDB-GEN001");
        assert_eq!(
            diagnostic.stage(),
            riffdb_diagnostics::AuthoringStage::Generation
        );
        assert_eq!(diagnostic.path().as_str(), "riffdb/contract.riff");
        assert_eq!(
            &contract[span.start() as usize..span.end() as usize],
            "foo_bar"
        );
        assert_eq!(
            diagnostic.symbol_path(),
            &["enum", "Collision", "variant", "foo_bar"]
        );
    }

    #[test]
    fn rust_scaffold_lock_places_the_rendered_root_package_canonically() {
        let early = render_cargo_lock(
            "agent-alpha-smoke",
            "AgentAlphaSmoke",
            "AgentAlphaSmokeApplication",
            "AgentAlphaSmokeClient",
        )
        .expect("early lock");
        assert!(
            early
                .find("name = \"agent-alpha-smoke\"")
                .expect("root package")
                < early
                    .find("name = \"aho-corasick\"")
                    .expect("first dependency")
        );

        let later = render_cargo_lock(
            "order-desk",
            "OrderDesk",
            "OrderDeskApplication",
            "OrderDeskClient",
        )
        .expect("later lock");
        let first = later
            .find("name = \"aho-corasick\"")
            .expect("first dependency");
        let root = later.find("name = \"order-desk\"").expect("root package");
        let tokio = later.find("name = \"tokio\"").expect("later dependency");
        assert!(first < root);
        assert!(root < tokio);
    }

    #[test]
    fn scaffold_is_deterministic_and_compiled() {
        let base = tempfile::TempDir::with_prefix("riffdb-new-test-deterministic-")
            .expect("scratch directory");
        let first = base.path().join("first");
        let second = base.path().join("second");
        create_application("order-desk", ScaffoldLanguage::Rust, &first).expect("first");
        create_application("order-desk", ScaffoldLanguage::Rust, &second).expect("second");
        for relative in [
            "AUTHORING.md",
            "Cargo.lock",
            "riffdb.application.json",
            "riffdb.application.lock.json",
            "riffdb/contract.riff",
            "riffdb/queries/item_page.riffq",
            "generated/riffdb.application.exact.json",
            "generated/riffdb.contract.bundle",
            "generated/rust/client.rs",
            "generated/typescript/client.ts",
            "generated/mcp/tools.json",
        ] {
            assert_eq!(
                fs::read(first.join(relative)).expect("first file"),
                fs::read(second.join(relative)).expect("second file"),
                "{relative}"
            );
        }
        let cargo_lock = fs::read_to_string(first.join("Cargo.lock")).expect("Cargo lock");
        assert!(cargo_lock.contains("name = \"order-desk\""));
        assert!(!cargo_lock.contains("{{APPLICATION_NAME}}"));
        assert!(cargo_lock.contains(
            "checksum = \"d67f98a1f24828592f3103819ffc5572b77fd4996c493886e9877cb6d21ead92\""
        ));
        let authoring = fs::read_to_string(first.join("AUTHORING.md")).expect("authoring guide");
        assert!(authoring.contains("exactMoney(\"USD\", \"25.00\")"));
        assert!(authoring.contains("application code never decodes transport JSON"));
        let source = fs::read(first.join("riffdb.application.json")).expect("source");
        ApplicationSourceManifest::decode_canonical(&source).expect("canonical source");
        let lock = fs::read(first.join(DEFAULT_LOCK_PATH)).expect("lock");
        ApplicationLock::decode_canonical(&lock).expect("canonical lock");
        check_application_lock(&first.join("riffdb.application.json"), None).expect("check");
        generate_application(&first.join("riffdb.application.json"), true, None)
            .expect("regenerate");
        fs::write(
            first.join("riffdb/queries/item_page.riffq"),
            b"query ItemPage() { outcomes Found }\n",
        )
        .expect("change pinned source");
        assert!(matches!(
            generate_application(&first.join("riffdb.application.json"), true, None),
            Err(ScaffoldError::CompileQuery
                | ScaffoldError::IdentityMismatch
                | ScaffoldError::Authoring(_))
        ));
        assert!(create_application("order-desk", ScaffoldLanguage::Rust, &first).is_err());
    }

    #[test]
    fn go_scaffold_is_exact_offline_and_application_only() {
        let parent =
            tempfile::TempDir::with_prefix("riffdb-new-test-go-").expect("scratch directory");
        let base = parent.path().join("app");
        create_application("order-desk", ScaffoldLanguage::Go, &base).expect("Go scaffold");
        let source = ApplicationSourceManifest::decode_canonical(
            &fs::read(base.join("riffdb.application.json")).expect("source"),
        )
        .expect("canonical source");
        assert_eq!(
            source.schema(),
            riffdb_query_module::APPLICATION_SOURCE_SCHEMA_V5
        );
        assert_eq!(source.generation().go(), Some("generated/go/client.go"));
        let lock = ApplicationLock::decode_canonical(
            &fs::read(base.join(DEFAULT_LOCK_PATH)).expect("lock"),
        )
        .expect("canonical lock");
        assert_eq!(
            lock.schema(),
            riffdb_query_module::APPLICATION_LOCK_SCHEMA_V6
        );
        for required in [
            "go.mod",
            "main.go",
            "generated/go/client.go",
            "third_party/riffdb-application/go.mod",
            "third_party/riffdb-application/runtime.go",
        ] {
            assert!(base.join(required).is_file(), "missing {required}");
        }
        let generated =
            fs::read_to_string(base.join("generated/go/client.go")).expect("generated Go client");
        for forbidden in ["google.golang.org/grpc", "credential", "protobuf"] {
            assert!(
                !generated.contains(forbidden),
                "forbidden Go surface: {forbidden}"
            );
        }
        check_application_lock(&base.join("riffdb.application.json"), None)
            .expect("exact Go scaffold");
    }

    #[test]
    fn v7_go_only_application_emits_a_transient_exact_runtime_catalog() {
        let parent = tempfile::TempDir::with_prefix("riffdb-go-runtime-catalog-test-")
            .expect("scratch directory");
        let base = parent.path().join("app");
        create_application("go-catalog", ScaffoldLanguage::Go, &base).expect("Go scaffold");
        let source_path = base.join("riffdb.application.json");
        let mut source: serde_json::Value =
            serde_json::from_slice(&fs::read(&source_path).expect("source JSON"))
                .expect("source value");
        source["schema"] = json!(riffdb_query_module::APPLICATION_SOURCE_SCHEMA_V7);
        source["generation"] = json!({"go": "generated/go/client.go"});
        for role in source["roles"].as_array_mut().expect("roles") {
            role["row_policies"] = json!([]);
        }
        fs::write(
            &source_path,
            ApplicationSourceManifest::parse(&serde_json::to_string(&source).expect("source JSON"))
                .expect("V7 Go-only source")
                .canonical_bytes(),
        )
        .expect("write source");
        write_application_lock(&source_path, None).expect("write sparse lock");
        for undeclared in [
            "generated/mcp/tools.json",
            "generated/python/client.py",
            "generated/rust/client.rs",
            "generated/typescript/client.ts",
        ] {
            if base.join(undeclared).exists() {
                fs::remove_file(base.join(undeclared)).expect("remove undeclared artifact");
            }
        }

        let catalog = runtime_operation_catalog(&source_path, None).expect("runtime catalog");
        let catalog: serde_json::Value = serde_json::from_slice(&catalog).expect("catalog JSON");
        let lock = ApplicationLock::decode_canonical(
            &fs::read(base.join(DEFAULT_LOCK_PATH)).expect("lock"),
        )
        .expect("canonical lock");
        assert_eq!(
            lock.schema(),
            riffdb_query_module::APPLICATION_LOCK_SCHEMA_V8
        );
        assert!(
            lock.artifacts()
                .iter()
                .all(|artifact| artifact.kind() != GeneratedApplicationArtifactKind::Mcp)
        );
        assert_eq!(
            catalog["application_manifest_hash"],
            json!(hex(lock.manifest_hash().as_bytes()))
        );
        assert!(!base.join("generated/mcp/tools.json").exists());
    }

    #[test]
    fn scaffold_accepts_an_existing_empty_directory() {
        let parent = tempfile::TempDir::with_prefix("riffdb-new-test-existing-empty-")
            .expect("scratch directory");
        let base = parent.path().join("app");
        fs::create_dir(&base).expect("existing empty directory");

        create_application("order-desk", ScaffoldLanguage::Rust, &base)
            .expect("scaffold existing empty directory");

        check_application_lock(&base.join("riffdb.application.json"), None)
            .expect("complete exact application");
    }

    #[test]
    fn existing_directory_publication_orders_the_exact_lock_last() {
        let mut entries = vec![
            DEFAULT_LOCK_PATH.into(),
            "riffdb.application.json".into(),
            "generated".into(),
            "riffdb".into(),
        ];
        sort_scaffold_entries(&mut entries);
        assert_eq!(
            entries.last().and_then(|entry| entry.to_str()),
            Some(DEFAULT_LOCK_PATH)
        );
    }

    #[test]
    fn scaffold_rejects_files_and_nonempty_directories_without_changes() {
        let parent =
            tempfile::TempDir::with_prefix("riffdb-new-test-occupied-").expect("scratch directory");
        let file = parent.path().join("file");
        let nonempty = parent.path().join("nonempty");
        fs::write(&file, b"retain file\n").expect("fixture file");
        fs::create_dir(&nonempty).expect("fixture directory");
        fs::write(nonempty.join("retain.txt"), b"retain directory\n").expect("fixture entry");

        assert!(create_application("order-desk", ScaffoldLanguage::Rust, &file).is_err());
        assert!(create_application("order-desk", ScaffoldLanguage::Rust, &nonempty).is_err());
        assert_eq!(fs::read(&file).expect("retained file"), b"retain file\n");
        assert_eq!(
            fs::read(nonempty.join("retain.txt")).expect("retained entry"),
            b"retain directory\n"
        );
    }

    #[cfg(unix)]
    #[test]
    fn scaffold_rejects_a_destination_symlink_without_touching_its_target() {
        use std::os::unix::fs::symlink;

        let parent = tempfile::TempDir::with_prefix("riffdb-new-test-destination-symlink-")
            .expect("scratch directory");
        let target = parent.path().join("target");
        let destination = parent.path().join("destination");
        fs::create_dir(&target).expect("target");
        symlink(&target, &destination).expect("destination symlink");

        assert!(create_application("order-desk", ScaffoldLanguage::Rust, &destination).is_err());
        assert!(
            fs::read_dir(&target)
                .expect("target remains")
                .next()
                .is_none()
        );
        fs::remove_file(destination).expect("remove symlink");
    }

    #[test]
    fn application_check_is_read_only_and_lock_check_detects_artifact_drift() {
        let parent = tempfile::TempDir::with_prefix("riffdb-lock-test-read-only-")
            .expect("scratch directory");
        let base = parent.path().join("app");
        create_application("safe-app", ScaffoldLanguage::Typescript, &base).expect("scaffold");
        let source = base.join("riffdb.application.json");
        let generated = base.join("generated/typescript/client.ts");
        let lock = base.join(DEFAULT_LOCK_PATH);
        fs::remove_file(&generated).expect("remove generated");
        fs::remove_file(&lock).expect("remove lock");

        assert_eq!(
            check_application(&source).expect("read-only compile"),
            ApplicationCheckStatus::SourceOnly {
                seed_input_count: 1
            }
        );
        let preview = preview_application_lock(&source).expect("read-only lock preview");
        assert!(!generated.exists());
        assert!(!lock.exists());

        write_application_lock(&source, None).expect("write lock");
        assert_eq!(preview, fs::read(&lock).expect("written lock"));
        fs::write(&generated, b"substituted\n").expect("substitute output");
        let check_error = check_application(&source).expect_err("application check rejects drift");
        assert_eq!(
            check_error
                .diagnostics()
                .expect("structured drift")
                .as_slice()[0]
                .code()
                .as_str(),
            "RDB-AL008"
        );
        let error = check_application_lock(&source, None).expect_err("artifact drift rejected");
        let diagnostics = error
            .diagnostics()
            .expect("artifact drift has a structured public diagnostic");
        let [diagnostic] = diagnostics.as_slice() else {
            panic!("artifact drift returns exactly one diagnostic");
        };
        assert_eq!(diagnostic.code().as_str(), "RDB-AL008");
        assert_eq!(diagnostic.path().as_str(), DEFAULT_LOCK_PATH);

        assert_eq!(
            check_application_sources(&source).expect("source-only check ignores exact artifacts"),
            ApplicationCheckStatus::SourceOnly {
                seed_input_count: 1
            }
        );
        assert_eq!(
            fs::read(&generated).expect("source-only check is read-only"),
            b"substituted\n"
        );

        let source_without_seed = fs::read_to_string(&source).expect("source").replace(
            r#""seed_inputs":["riffdb/seed/01-CreateItem.jsonl"]"#,
            r#""seed_inputs":[]"#,
        );
        fs::write(&source, source_without_seed).expect("remove seed plan");
        assert_eq!(
            check_application_sources(&source).expect("seedless source-only check"),
            ApplicationCheckStatus::SourceOnly {
                seed_input_count: 0
            }
        );
    }

    #[test]
    fn row_policy_fixture_lock_round_trips_through_its_pinned_bundle() {
        let source = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../fixtures/adapters/row-policy-conformance/riffdb.application.json");
        let root = source_parent(&source);
        let lock_path = workspace_lock_path(root, None).expect("lock path");
        let decoded = ApplicationLock::decode_canonical(
            &read_bounded(&lock_path, riffdb_query_module::MAX_APPLICATION_LOCK_BYTES)
                .expect("read lock"),
        )
        .expect("decode lock");
        let artifact = decoded
            .contract_bundle_artifact()
            .expect("contract bundle artifact");
        let contract = ContractBundle::decode(
            &read_workspace_file(root, artifact.path(), riffdb_contract_ir::MAX_BUNDLE_BYTES)
                .expect("read contract bundle"),
        )
        .expect("decode contract bundle");
        let contract_source = application_contract_source(&source).expect("contract source");
        assert_eq!(
            contract.source_hash(),
            hash_source(contract_source.as_bytes()),
            "the pinned bundle must retain the exact contract source identity"
        );
        let compiled = compile_for_existing_lock(&source, root, &lock_path, &decoded)
            .expect("compile from pinned bundle");
        assert_eq!(
            decoded.canonical_bytes(),
            compiled.lock.canonical_bytes(),
            "a policy-bearing exact lock must round-trip through its pinned bundle"
        );
        check_application_lock(&source, None).expect("check exact row-policy fixture lock");
    }

    #[test]
    fn successor_lock_uses_the_parent_aware_bundle_for_check_and_deploy_identity() {
        let parent = tempfile::TempDir::with_prefix("riffdb-lock-test-successor-identity-")
            .expect("scratch directory");
        let base = parent.path().join("app");
        create_application("safe-app", ScaffoldLanguage::Rust, &base).expect("scaffold");
        let source_path = base.join("riffdb.application.json");
        let contract_path = base.join("riffdb/contract.riff");
        let genesis_source = fs::read_to_string(&contract_path).expect("genesis source");
        let genesis = compile_contract_source(&genesis_source).expect("genesis bundle");
        let successor_source = genesis_source.replacen("version 1", "version 2", 1);
        let successor =
            riffdb_contract_compiler::compile_contract_successor(&successor_source, &genesis)
                .expect("successor bundle");
        let genesis_shaped_successor = compile_contract_source(&successor_source)
            .expect("the historical broken path still compiles as genesis");
        assert_ne!(
            successor.bundle_hash(),
            genesis_shaped_successor.bundle_hash(),
            "parent allocation history is part of successor identity"
        );
        fs::write(&contract_path, successor_source).expect("successor source");
        let mut source: serde_json::Value =
            serde_json::from_slice(&fs::read(&source_path).expect("application source"))
                .expect("source JSON");
        source["contract"]["version"] = serde_json::json!(2);
        fs::write(
            &source_path,
            serde_json::to_vec(&source).expect("updated source JSON"),
        )
        .expect("updated source");

        assert!(matches!(
            write_application_lock(&source_path, None),
            Err(ScaffoldError::IdentityMismatch)
        ));
        write_application_lock_with_bundle(&source_path, None, successor.clone())
            .expect("parent-aware lock");
        check_application_lock(&source_path, None).expect("offline exact check");
        let locked = load_locked_application(&source_path, None).expect("locked successor");
        assert_eq!(locked.contract().bundle_hash(), successor.bundle_hash());
        assert_eq!(
            locked
                .contract()
                .parent()
                .map(|parent| parent.bundle_hash()),
            Some(genesis.bundle_hash())
        );
        let generated_rust = base.join("generated/rust/client.rs");
        fs::write(&generated_rust, b"substituted\n").expect("substitute generated client");
        assert!(check_application_lock(&source_path, None).is_err());
        assert_eq!(
            refresh_application_lock_from_pinned_bundle(&source_path, None)
                .expect("refresh from pinned successor bundle"),
            PinnedLockRefresh::Refreshed
        );
        check_application_lock(&source_path, None).expect("refreshed generated client is exact");
        assert_ne!(
            fs::read(&generated_rust).expect("regenerated client"),
            b"substituted\n"
        );

        let query_path = base.join("riffdb/queries/item_page.riffq");
        let query_source = fs::read_to_string(&query_path).expect("query source");
        fs::write(
            &query_path,
            query_source.replace("            created_at\n", ""),
        )
        .expect("change only the query source");
        let lock_before_query_change = fs::read(base.join(DEFAULT_LOCK_PATH)).expect("old lock");
        assert_eq!(
            refresh_application_lock_from_pinned_bundle(&source_path, None)
                .expect("refresh query identity from the same contract"),
            PinnedLockRefresh::Refreshed
        );
        assert_ne!(
            fs::read(base.join(DEFAULT_LOCK_PATH)).expect("new lock"),
            lock_before_query_change
        );
        let refreshed = load_locked_application(&source_path, None).expect("refreshed successor");
        assert_eq!(refreshed.contract().bundle_hash(), successor.bundle_hash());

        let contract_before_change =
            fs::read_to_string(&contract_path).expect("locked contract source");
        let lock_before_contract_change =
            fs::read(base.join(DEFAULT_LOCK_PATH)).expect("lock before contract change");
        fs::write(&contract_path, format!("{contract_before_change}\n"))
            .expect("change contract source bytes");
        assert_eq!(
            refresh_application_lock_from_pinned_bundle(&source_path, None)
                .expect("contract changes select the server-preview path"),
            PinnedLockRefresh::ContractSourceChanged
        );
        assert_eq!(
            fs::read(base.join(DEFAULT_LOCK_PATH)).expect("unchanged lock"),
            lock_before_contract_change
        );
        fs::write(&contract_path, contract_before_change).expect("restore contract source");

        fs::write(base.join(CONTRACT_BUNDLE_ARTIFACT_PATH), b"truncated")
            .expect("substitute pinned bundle");
        let error = check_application(&source_path).expect_err("bundle substitution rejected");
        assert_eq!(
            error
                .diagnostics()
                .expect("structured lock drift")
                .as_slice()[0]
                .code()
                .as_str(),
            "RDB-AL008"
        );
    }

    #[test]
    fn v4_lock_pins_and_plans_an_exact_direct_parent_migration() {
        let parent = tempfile::TempDir::with_prefix("riffdb-lock-test-migration-v4-")
            .expect("scratch directory");
        let base = parent.path().join("app");
        create_application("safe-app", ScaffoldLanguage::Rust, &base).expect("scaffold");
        let source_path = base.join("riffdb.application.json");
        let contract_path = base.join("riffdb/contract.riff");
        let genesis_source = fs::read_to_string(&contract_path)
            .expect("genesis source")
            .replacen(
                "contract SafeApp version 1 {",
                "contract SafeApp version 1 {\n  enum FixtureStatus { Open }",
                1,
            );
        fs::write(&contract_path, &genesis_source).expect("enum-bearing genesis source");
        let genesis = compile_contract_source(&genesis_source).expect("genesis bundle");

        fs::create_dir_all(base.join("retained")).expect("retained artifacts");
        fs::write(base.join("retained/v1.bundle"), genesis.canonical_bytes())
            .expect("retained parent bundle");
        fs::create_dir_all(base.join("riffdb/migrations")).expect("migration sources");
        let migration_source =
            "migration SafeApp from 1 to 2 { rename enum FixtureStatus to CurrentStatus }\n";
        fs::write(
            base.join("riffdb/migrations/v1-to-v2.riffm"),
            migration_source,
        )
        .expect("migration source");

        let successor_source = genesis_source
            .replacen("version 1", "version 2", 1)
            .replace("enum FixtureStatus", "enum CurrentStatus")
            .replace(
                "field created_at: timestamp",
                "field created_at: timestamp\n    index by_title (title, item_id)",
            );
        let (successor, _) = riffdb_contract_compiler::compile_contract_migration_successor(
            &successor_source,
            migration_source,
            &genesis,
        )
        .expect("rename and index successor");
        fs::write(&contract_path, successor_source).expect("successor source");

        let mut source: serde_json::Value =
            serde_json::from_slice(&fs::read(&source_path).expect("application source"))
                .expect("source JSON");
        source["schema"] = json!(riffdb_query_module::APPLICATION_SOURCE_SCHEMA_V3);
        source["contract"]["version"] = json!(2);
        source["generation"]["python"] = json!("generated/python/client.py");
        source["generation"]
            .as_object_mut()
            .expect("generation")
            .remove("go");
        source
            .as_object_mut()
            .expect("source")
            .remove("reactive_modules");
        for role in source["roles"].as_array_mut().expect("roles") {
            let role = role.as_object_mut().expect("role");
            role.remove("agent_subscriptions");
            role.remove("event_streams");
            role.remove("watch_queries");
        }
        source["migrations"] = json!([{
            "parent_bundle": "retained/v1.bundle",
            "source": "riffdb/migrations/v1-to-v2.riffm",
        }]);
        fs::write(
            &source_path,
            serde_json::to_vec(&source).expect("updated source JSON"),
        )
        .expect("updated source");

        write_application_lock_with_bundle(&source_path, None, successor)
            .expect("write migration lock");
        check_application_lock(&source_path, None).expect("offline exact check");
        let lock = ApplicationLock::decode_canonical(
            &fs::read(base.join(DEFAULT_LOCK_PATH)).expect("lock bytes"),
        )
        .expect("canonical V4 lock");
        assert_eq!(
            lock.schema(),
            riffdb_query_module::APPLICATION_LOCK_SCHEMA_V4
        );
        assert_eq!(lock.migrations().len(), 1);
        assert!(
            base.join(lock.migrations()[0].migration_artifact_path())
                .is_file()
        );

        let plan = plan_application_migrations(&source_path, None).expect("local migration plan");
        assert_eq!(plan["schema"], "riffdb.migration-plan/v1");
        assert_eq!(plan["candidate_version"], 2);
        assert_eq!(plan["supported_parents"][0]["parent_version"], 1);
        assert_eq!(plan["supported_parents"][0]["step_count"], 2);
        let categories = plan["supported_parents"][0]["step_categories"]
            .as_array()
            .expect("step categories");
        assert!(categories.contains(&json!("rename_identity")));
        assert!(categories.contains(&json!("rebuild_index")));

        fs::write(
            base.join("riffdb/migrations/v1-to-v2.riffm"),
            "migration SafeApp from 1 to 2 { acknowledge conflict ItemRoot }\n",
        )
        .expect("change migration source");
        assert!(check_application_lock(&source_path, None).is_err());
    }

    #[test]
    fn application_check_preserves_contract_diagnostic_code_path_and_span() {
        let parent = tempfile::TempDir::with_prefix("riffdb-diagnostic-test-contract-")
            .expect("scratch directory");
        let base = parent.path().join("app");
        create_application("safe-app", ScaffoldLanguage::Rust, &base).expect("scaffold");
        fs::remove_file(base.join(DEFAULT_LOCK_PATH)).expect("source-only diagnostic check");
        fs::write(
            base.join("riffdb/contract.riff"),
            b"contract SafeApp version 1 { SECRET_VALUE }\n",
        )
        .expect("invalid contract");
        let error = check_application(&base.join("riffdb.application.json"))
            .expect_err("compile must reject");
        let diagnostics = error.diagnostics().expect("structured diagnostics");
        let first = &diagnostics.as_slice()[0];

        assert_eq!(first.stage().as_str(), "contract_syntax");
        assert_eq!(first.path().as_str(), "riffdb/contract.riff");
        assert!(first.span().is_some());
        let json = diagnostics.render_json().expect("JSON");
        assert_eq!(
            json,
            include_str!(
                "../../../fixtures/application-diagnostics/contract-unexpected-token-v1.json"
            )
        );
        assert_eq!(
            diagnostics.render_human().expect("human"),
            include_str!(
                "../../../fixtures/application-diagnostics/contract-unexpected-token-v1.txt"
            )
        );
        assert!(!json.contains("SECRET_VALUE"));
        assert!(!json.contains("contract SafeApp"));
    }

    #[test]
    fn application_check_rejects_a_role_whose_complete_scan_budget_is_unsafe() {
        let parent = tempfile::TempDir::with_prefix("riffdb-role-budget-test-complete-scan-")
            .expect("scratch directory");
        let base = parent.path().join("app");
        create_application("safe-app", ScaffoldLanguage::Rust, &base).expect("scaffold");
        fs::remove_file(base.join(DEFAULT_LOCK_PATH)).expect("source-only role check");
        fs::write(
            base.join("riffdb/contract.riff"),
            br#"contract SafeApp version 1 {
  entity Item {
    key (tenant_id: uuid, item_id: uuid)
    field title: string<128>
    field created_at: timestamp
    index by_tenant (tenant_id, item_id)
  }

  aggregate ItemRoot {
    root Item
    partition_by tenant_id
    conflict_key (tenant_id)
  }

  command CreateItem {
    input idempotency_key: string<128>
    input tenant_id: uuid
    input item_id: uuid
    input title: string<128>
    idempotency_key idempotency_key
    create Item(tenant_id, item_id) as item
      else ItemExists { item_id: item_id }
    set item.title = title
    set item.created_at = tx.time
    return Created { item: item }
  }
}
"#,
        )
        .expect("write bounded contract");
        fs::write(
            base.join("riffdb/queries/item_page.riffq"),
            br#"query ItemPage(
    $tenant_id: Item.tenant_id,
    $first_limit: Limit = 25,
    $second_limit: Limit = 25,
) {
    many first from Item
        where tenant_id == $tenant_id
        order by item_id asc
        take $first_limit

    many second from Item
        where tenant_id == $tenant_id
        order by item_id asc
        take $second_limit

    return Found {
        first: first { item_id title }
        second: second { item_id title }
    }

    outcomes Found
}
"#,
        )
        .expect("write unsafe complete budget");

        let error = check_application(&base.join("riffdb.application.json"))
            .expect_err("complete worst-case scan exceeds the role bound");
        let diagnostics = error.diagnostics().expect("structured role diagnostic");
        let [diagnostic] = diagnostics.as_slice() else {
            panic!("role failure returns exactly one diagnostic");
        };
        assert_eq!(diagnostic.stage().as_str(), "role");
        assert_eq!(diagnostic.code().as_str(), "RDB-AR007");
        assert_eq!(diagnostic.symbol_path(), ["SafeAppApplication", "ItemPage"]);
        assert_eq!(
            diagnostics.render_json().expect("JSON"),
            include_str!("../../../fixtures/application-diagnostics/role-budget-v1.json")
        );
        assert_eq!(
            diagnostics.render_human().expect("human"),
            include_str!("../../../fixtures/application-diagnostics/role-budget-v1.txt")
        );
    }

    #[cfg(unix)]
    #[test]
    fn locked_generation_rejects_symlink_output_parents() {
        use std::os::unix::fs::symlink;

        let parent =
            tempfile::TempDir::with_prefix("riffdb-lock-test-symlink-").expect("scratch directory");
        let base = parent.path().join("safe-app");
        let outside = parent.path().join("outside");
        create_application("safe-app", ScaffoldLanguage::Rust, &base).expect("scaffold");
        fs::create_dir(&outside).expect("outside");
        fs::remove_dir_all(base.join("generated")).expect("remove generated");
        symlink(&outside, base.join("generated")).expect("symlink");

        assert!(matches!(
            write_application_lock(&base.join("riffdb.application.json"), None),
            Err(ScaffoldError::Authoring(_))
        ));
        assert!(
            fs::read_dir(&outside)
                .expect("outside remains")
                .next()
                .is_none()
        );
        // Explicit form of what the deleted cleanup used to observe by
        // accident (remove_file on a directory errors with EISDIR): the
        // generator must refuse without unlinking the symlink or replacing
        // it with a real directory.
        assert!(
            fs::symlink_metadata(base.join("generated"))
                .expect("generated entry survives the refusal")
                .file_type()
                .is_symlink(),
            "the rejected output parent must still be a symlink"
        );
    }
}
