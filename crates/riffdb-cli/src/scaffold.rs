//! Deterministic, application-only repository scaffolding.

use std::fmt;
use std::fs;
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};

use riffdb_contract_compiler::compile_contract_source;
use riffdb_diagnostics::{
    AuthoringDiagnostics, AuthoringSourcePath, FileChangeDisposition, FilesystemDiagnosticClass,
};
use riffdb_query_module::{
    ApplicationLock, ApplicationManifest, ApplicationSourceManifest, GeneratedApplicationArtifact,
    GeneratedApplicationArtifactKind, GeneratedMcpCommand, GeneratedMcpTool, NamedQuerySource,
    QueryModule, QueryModuleCandidate, QueryModuleName, QueryModuleVersion,
    compile_application_role, generate_mcp_commands, generate_mcp_tools, generate_rust_client,
    generate_typescript_client,
};
use serde_json::json;

const CONTRACT_TEMPLATE: &str = include_str!("../../../templates/application/contract.riff");
const QUERY_TEMPLATE: &str = include_str!("../../../templates/application/item_page.riffq");
const SEED_TEMPLATE: &str = include_str!("../../../templates/application/seed.jsonl");
const README_TEMPLATE: &str = include_str!("../../../templates/application/README.md");
const RUST_MAIN_TEMPLATE: &str = include_str!("../../../templates/application/rust-main.rs");
const TYPESCRIPT_MAIN_TEMPLATE: &str =
    include_str!("../../../templates/application/typescript-main.ts");
const CARGO_TEMPLATE: &str = include_str!("../../../templates/application/Cargo.toml");
const PACKAGE_TEMPLATE: &str = include_str!("../../../templates/application/package.json");
const PACKAGE_LOCK_BASE: &str =
    include_str!("../../../clients/typescript/runtime/package-lock.json");
const TSCONFIG_TEMPLATE: &str = include_str!("../../../templates/application/tsconfig.json");
const GITIGNORE_TEMPLATE: &str = include_str!("../../../templates/application/gitignore");
const TYPESCRIPT_RUNTIME_JS: &str =
    include_str!("../../../clients/typescript/runtime/dist/index.js");
const TYPESCRIPT_RUNTIME_TYPES: &str =
    include_str!("../../../clients/typescript/runtime/dist/index.d.ts");
const MAX_APPLICATION_NAME_BYTES: usize = 64;
const MAX_SCAFFOLD_TOP_LEVEL_ENTRIES: usize = 16;
const MAX_TYPESCRIPT_TOOLCHAIN_FILES: usize = 16_384;
const MAX_TYPESCRIPT_TOOLCHAIN_BYTES: u64 = 128 * 1_024 * 1_024;
const EXACT_MANIFEST_PATH: &str = "generated/riffdb.application.exact.json";
const DEFAULT_LOCK_PATH: &str = "riffdb.application.lock.json";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScaffoldLanguage {
    Rust,
    Typescript,
}

#[derive(Debug)]
pub(crate) enum ScaffoldError {
    InvalidApplicationName,
    CompileContract,
    CompileQuery,
    CompileRole,
    Manifest,
    ApplicationSource,
    ApplicationLock,
    LockRequired,
    IdentityMismatch,
    SourceLimit,
    GenerateMcp,
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
        Some("riffdb.application-source/v1") if locked => generate_application_locked(
            manifest_path,
            lock_path.unwrap_or_else(|| Path::new(DEFAULT_LOCK_PATH)),
        ),
        Some("riffdb.application-source/v1") => Err(ScaffoldError::LockRequired),
        _ => Err(ScaffoldError::Manifest),
    }
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
    let commands =
        generate_mcp_commands(&module, &contract).map_err(|_| ScaffoldError::GenerateMcp)?;
    let generated_mcp = render_mcp_manifest(&manifest, &tools, &commands)?;
    write_file(
        root,
        manifest.generation().rust(),
        generate_rust_client(&module, &contract).as_bytes(),
    )?;
    write_file(
        root,
        manifest.generation().typescript(),
        generate_typescript_client(&module, &contract).as_bytes(),
    )?;
    write_file(root, manifest.generation().mcp(), generated_mcp.as_bytes())?;
    Ok(())
}

struct CompiledSymbolicApplication {
    lock: ApplicationLock,
    outputs: Vec<(String, Vec<u8>)>,
}

fn source_parent(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

pub(crate) fn check_application(source_path: &Path) -> Result<(), ScaffoldError> {
    let _ = compile_symbolic_application(source_path)?;
    Ok(())
}

pub(crate) fn preview_application_lock(source_path: &Path) -> Result<Vec<u8>, ScaffoldError> {
    Ok(compile_symbolic_application(source_path)?
        .lock
        .canonical_bytes()
        .to_vec())
}

pub(crate) fn write_application_lock(
    source_path: &Path,
    lock_path: Option<&Path>,
) -> Result<(), ScaffoldError> {
    let compiled = compile_symbolic_application(source_path)?;
    let root = source_parent(source_path);
    for (path, bytes) in &compiled.outputs {
        atomic_write_workspace(root, path, bytes)?;
    }
    let lock_path = workspace_lock_path(root, lock_path)?;
    atomic_write_absolute(&lock_path, compiled.lock.canonical_bytes())?;
    Ok(())
}

pub(crate) fn check_application_lock(
    source_path: &Path,
    lock_path: Option<&Path>,
) -> Result<(), ScaffoldError> {
    let compiled = compile_symbolic_application(source_path)?;
    let root = source_parent(source_path);
    let lock_path = workspace_lock_path(root, lock_path)?;
    let existing = read_bounded(&lock_path, 4 * 1_024 * 1_024)?;
    let decoded = ApplicationLock::decode_canonical(&existing)
        .map_err(|error| lock_diagnostic(&lock_path, error.kind()))?;
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

fn generate_application_locked(source_path: &Path, lock_path: &Path) -> Result<(), ScaffoldError> {
    let compiled = compile_symbolic_application(source_path)?;
    let root = source_parent(source_path);
    let lock_path = workspace_lock_path(root, Some(lock_path))?;
    let existing = read_bounded(&lock_path, 4 * 1_024 * 1_024)?;
    let decoded = ApplicationLock::decode_canonical(&existing)
        .map_err(|error| lock_diagnostic(&lock_path, error.kind()))?;
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

fn compile_symbolic_application(
    source_path: &Path,
) -> Result<CompiledSymbolicApplication, ScaffoldError> {
    let source_bytes = read_bounded(source_path, 1_048_576)?;
    let source_text =
        std::str::from_utf8(&source_bytes).map_err(|_| ScaffoldError::ApplicationSource)?;
    let source = ApplicationSourceManifest::parse(source_text)
        .map_err(|error| application_source_diagnostic(source_path, error.kind()))?;
    let root = source_parent(source_path);
    let contract_source = read_workspace_text(root, source.contract().source(), 1_048_576)?;
    let contract = compile_contract_source(&contract_source)
        .map_err(|error| contract_diagnostic(source.contract().source(), &error))?;
    let mut modules = Vec::with_capacity(source.query_modules().len());
    for declared in source.query_modules() {
        let mut queries = Vec::with_capacity(declared.queries().len());
        for query in declared.queries() {
            let query_source = read_workspace_text(root, query.source(), 1_048_576)?;
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
    let exact = source
        .exact_manifest(&contract, &modules)
        .map_err(|error| application_source_diagnostic(source_path, error.kind()))?;
    let [module] = modules.as_slice() else {
        return Err(ScaffoldError::Manifest);
    };
    let tools = generate_mcp_tools(module).map_err(|_| ScaffoldError::GenerateMcp)?;
    let commands =
        generate_mcp_commands(module, &contract).map_err(|_| ScaffoldError::GenerateMcp)?;
    let generated_mcp = render_mcp_manifest(&exact, &tools, &commands)?;
    let outputs = vec![
        (
            EXACT_MANIFEST_PATH.to_owned(),
            exact.canonical_bytes().to_vec(),
        ),
        (
            source.generation().rust().to_owned(),
            generate_rust_client(module, &contract).into_bytes(),
        ),
        (
            source.generation().typescript().to_owned(),
            generate_typescript_client(module, &contract).into_bytes(),
        ),
        (
            source.generation().mcp().to_owned(),
            generated_mcp.into_bytes(),
        ),
    ];
    let artifacts = outputs
        .iter()
        .map(|(path, bytes)| {
            let kind = if path == EXACT_MANIFEST_PATH {
                GeneratedApplicationArtifactKind::Manifest
            } else if path == source.generation().rust() {
                GeneratedApplicationArtifactKind::Rust
            } else if path == source.generation().typescript() {
                GeneratedApplicationArtifactKind::TypeScript
            } else {
                GeneratedApplicationArtifactKind::Mcp
            };
            GeneratedApplicationArtifact::new(kind, path.clone(), bytes)
                .map_err(|error| lock_diagnostic(Path::new(DEFAULT_LOCK_PATH), error.kind()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let lock = ApplicationLock::compile(&source, &exact, &contract, &modules, &artifacts)
        .map_err(|error| lock_diagnostic(Path::new(DEFAULT_LOCK_PATH), error.kind()))?;
    Ok(CompiledSymbolicApplication { lock, outputs })
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
    AuthoringSourcePath::new(path)
        .ok()
        .and_then(|path| AuthoringDiagnostics::from_contract(path, error).ok())
        .map_or(ScaffoldError::CompileContract, ScaffoldError::Authoring)
}

fn query_diagnostic(path: &str, error: &riffdb_query_module::QueryModuleError) -> ScaffoldError {
    AuthoringSourcePath::new(path)
        .ok()
        .and_then(|path| AuthoringDiagnostics::from_query_module(path, error).ok())
        .map_or(ScaffoldError::CompileQuery, ScaffoldError::Authoring)
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
    let application_source = serde_json::to_string(&json!({
        "application": application,
        "contract": {
            "lineage": contract.lineage().as_str(),
            "source": "riffdb/contract.riff",
            "version": contract.contract_version().get(),
        },
        "generation": {
            "mcp": "generated/mcp/tools.json",
            "rust": "generated/rust/client.rs",
            "typescript": "generated/typescript/client.ts",
        },
        "query_modules": [{
            "name": module_name,
            "queries": [{
                "name": "ItemPage",
                "source": "riffdb/queries/item_page.riffq",
            }],
            "version": 1,
        }],
        "roles": [{
            "commands": ["CreateItem"],
            "environment": "development",
            "name": role_name,
            "queries": ["ItemPage"],
            "tenant_scope": "global",
        }],
        "schema": "riffdb.application-source/v1",
        "seed_inputs": ["riffdb/seed/01-CreateItem.jsonl"],
    }))
    .map_err(|_| ScaffoldError::ApplicationSource)?;
    let source = ApplicationSourceManifest::parse(&application_source)
        .map_err(|_| ScaffoldError::ApplicationSource)?;
    let manifest = source
        .exact_manifest(&contract, std::slice::from_ref(&module))
        .map_err(|_| ScaffoldError::IdentityMismatch)?;
    compile_application_role(
        &manifest,
        &role_name,
        None,
        &contract,
        std::slice::from_ref(&module),
    )
    .map_err(|_| ScaffoldError::CompileRole)?;
    let tools = generate_mcp_tools(&module).map_err(|_| ScaffoldError::GenerateMcp)?;
    let commands =
        generate_mcp_commands(&module, &contract).map_err(|_| ScaffoldError::GenerateMcp)?;
    let generated_mcp = render_mcp_manifest(&manifest, &tools, &commands)?;
    let generated_rust = generate_rust_client(&module, &contract);
    let generated_typescript = generate_typescript_client(&module, &contract);
    let artifacts = [
        GeneratedApplicationArtifact::new(
            GeneratedApplicationArtifactKind::Manifest,
            EXACT_MANIFEST_PATH,
            manifest.canonical_bytes(),
        )
        .map_err(|_| ScaffoldError::ApplicationLock)?,
        GeneratedApplicationArtifact::new(
            GeneratedApplicationArtifactKind::Rust,
            source.generation().rust(),
            generated_rust.as_bytes(),
        )
        .map_err(|_| ScaffoldError::ApplicationLock)?,
        GeneratedApplicationArtifact::new(
            GeneratedApplicationArtifactKind::TypeScript,
            source.generation().typescript(),
            generated_typescript.as_bytes(),
        )
        .map_err(|_| ScaffoldError::ApplicationLock)?,
        GeneratedApplicationArtifact::new(
            GeneratedApplicationArtifactKind::Mcp,
            source.generation().mcp(),
            generated_mcp.as_bytes(),
        )
        .map_err(|_| ScaffoldError::ApplicationLock)?,
    ];
    let lock = ApplicationLock::compile(
        &source,
        &manifest,
        &contract,
        std::slice::from_ref(&module),
        &artifacts,
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
        &generated_mcp,
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
    generated_mcp: &str,
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
    write_file(root, "generated/mcp/tools.json", generated_mcp.as_bytes())?;
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
            materialize_installed_typescript_toolchain(root)?;
        }
    }
    Ok(())
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

fn materialize_installed_typescript_toolchain(root: &Path) -> Result<(), ScaffoldError> {
    let executable = std::env::current_exe()?;
    let Some(bundle_root) = executable.parent().and_then(Path::parent) else {
        return Ok(());
    };
    let source = bundle_root.join("public/typescript/node_modules");
    if !source.is_dir() {
        return Ok(());
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

fn render_mcp_manifest(
    manifest: &ApplicationManifest,
    tools: &[GeneratedMcpTool],
    commands: &[GeneratedMcpCommand],
) -> Result<String, ScaffoldError> {
    let tools = tools
        .iter()
        .map(|tool| {
            Ok(json!({
                "name": tool.name,
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
        })
        .collect::<Result<Vec<_>, ScaffoldError>>()?;
    let commands = commands
        .iter()
        .map(|command| {
            Ok(json!({
                "name": command.name,
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
    let value = json!({
        "schema": "riffdb-generated-mcp-tools-v1",
        "application_manifest_hash": hex(manifest.identity().as_bytes()),
        "tools": tools,
        "commands": commands,
    });
    let mut output =
        serde_json::to_string_pretty(&value).map_err(|_| ScaffoldError::GenerateMcp)?;
    output.push('\n');
    Ok(output)
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
    fn rejects_unsafe_or_ambiguous_names_before_writing() {
        for name in ["", "../app", "App", "app_name", "app--name", "app-"] {
            assert!(!valid_application_name(name), "{name}");
        }
        assert!(valid_application_name("order-desk2"));
    }

    #[test]
    fn scaffold_is_deterministic_and_compiled() {
        let base = std::env::temp_dir().join(format!(
            "riffdb-new-test-{}-{}",
            std::process::id(),
            "deterministic"
        ));
        if base.exists() {
            fs::remove_dir_all(&base).expect("remove prior test directory");
        }
        fs::create_dir(&base).expect("test directory");
        let first = base.join("first");
        let second = base.join("second");
        create_application("order-desk", ScaffoldLanguage::Rust, &first).expect("first");
        create_application("order-desk", ScaffoldLanguage::Rust, &second).expect("second");
        for relative in [
            "riffdb.application.json",
            "riffdb.application.lock.json",
            "riffdb/contract.riff",
            "riffdb/queries/item_page.riffq",
            "generated/riffdb.application.exact.json",
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
        fs::remove_dir_all(base).expect("cleanup");
    }

    #[test]
    fn scaffold_accepts_an_existing_empty_directory() {
        let base = std::env::temp_dir().join(format!(
            "riffdb-new-test-{}-{}",
            std::process::id(),
            "existing-empty"
        ));
        if base.exists() {
            fs::remove_dir_all(&base).expect("remove prior test directory");
        }
        fs::create_dir(&base).expect("existing empty directory");

        create_application("order-desk", ScaffoldLanguage::Rust, &base)
            .expect("scaffold existing empty directory");

        check_application_lock(&base.join("riffdb.application.json"), None)
            .expect("complete exact application");
        fs::remove_dir_all(base).expect("cleanup");
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
        let parent = std::env::temp_dir().join(format!(
            "riffdb-new-test-{}-{}",
            std::process::id(),
            "occupied"
        ));
        if parent.exists() {
            fs::remove_dir_all(&parent).expect("remove prior test directory");
        }
        fs::create_dir(&parent).expect("test parent");
        let file = parent.join("file");
        let nonempty = parent.join("nonempty");
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
        fs::remove_dir_all(parent).expect("cleanup");
    }

    #[cfg(unix)]
    #[test]
    fn scaffold_rejects_a_destination_symlink_without_touching_its_target() {
        use std::os::unix::fs::symlink;

        let parent = std::env::temp_dir().join(format!(
            "riffdb-new-test-{}-{}",
            std::process::id(),
            "destination-symlink"
        ));
        if parent.exists() {
            fs::remove_dir_all(&parent).expect("remove prior test directory");
        }
        fs::create_dir(&parent).expect("test parent");
        let target = parent.join("target");
        let destination = parent.join("destination");
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
        fs::remove_dir_all(parent).expect("cleanup");
    }

    #[test]
    fn application_check_is_read_only_and_lock_check_detects_artifact_drift() {
        let base = std::env::temp_dir().join(format!(
            "riffdb-lock-test-{}-{}",
            std::process::id(),
            "read-only"
        ));
        if base.exists() {
            fs::remove_dir_all(&base).expect("remove prior test directory");
        }
        create_application("safe-app", ScaffoldLanguage::Typescript, &base).expect("scaffold");
        let source = base.join("riffdb.application.json");
        let generated = base.join("generated/typescript/client.ts");
        let lock = base.join(DEFAULT_LOCK_PATH);
        fs::remove_file(&generated).expect("remove generated");
        fs::remove_file(&lock).expect("remove lock");

        check_application(&source).expect("read-only compile");
        let preview = preview_application_lock(&source).expect("read-only lock preview");
        assert!(!generated.exists());
        assert!(!lock.exists());

        write_application_lock(&source, None).expect("write lock");
        assert_eq!(preview, fs::read(&lock).expect("written lock"));
        fs::write(&generated, b"substituted\n").expect("substitute output");
        let error = check_application_lock(&source, None).expect_err("artifact drift rejected");
        let diagnostics = error
            .diagnostics()
            .expect("artifact drift has a structured public diagnostic");
        let [diagnostic] = diagnostics.as_slice() else {
            panic!("artifact drift returns exactly one diagnostic");
        };
        assert_eq!(diagnostic.code().as_str(), "RDB-AL008");
        assert_eq!(diagnostic.path().as_str(), DEFAULT_LOCK_PATH);
        fs::remove_dir_all(base).expect("cleanup");
    }

    #[test]
    fn application_check_preserves_contract_diagnostic_code_path_and_span() {
        let base = std::env::temp_dir().join(format!(
            "riffdb-diagnostic-test-{}-{}",
            std::process::id(),
            "contract"
        ));
        if base.exists() {
            fs::remove_dir_all(&base).expect("remove prior test directory");
        }
        create_application("safe-app", ScaffoldLanguage::Rust, &base).expect("scaffold");
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
        fs::remove_dir_all(base).expect("cleanup");
    }

    #[cfg(unix)]
    #[test]
    fn locked_generation_rejects_symlink_output_parents() {
        use std::os::unix::fs::symlink;

        let base = std::env::temp_dir().join(format!(
            "riffdb-lock-test-{}-{}",
            std::process::id(),
            "symlink"
        ));
        let outside = std::env::temp_dir().join(format!(
            "riffdb-lock-outside-{}-{}",
            std::process::id(),
            "symlink"
        ));
        for path in [&base, &outside] {
            if path.exists() {
                fs::remove_dir_all(path).expect("remove prior test directory");
            }
        }
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
        fs::remove_file(base.join("generated")).expect("remove symlink");
        fs::remove_dir_all(base).expect("cleanup");
        fs::remove_dir_all(outside).expect("cleanup outside");
    }
}
