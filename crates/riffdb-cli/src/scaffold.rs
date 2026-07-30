//! Deterministic, application-only repository scaffolding.

use std::fmt;
use std::fs;
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};

use riffdb_contract_compiler::compile_contract_source;
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
const TSCONFIG_TEMPLATE: &str = include_str!("../../../templates/application/tsconfig.json");
const GITIGNORE_TEMPLATE: &str = include_str!("../../../templates/application/gitignore");
const MAX_APPLICATION_NAME_BYTES: usize = 64;
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
    DestinationExists,
    CompileContract,
    CompileQuery,
    CompileRole,
    Manifest,
    ApplicationSource,
    ApplicationLock,
    LockRequired,
    LockMismatch,
    IdentityMismatch,
    SourceLimit,
    GenerateMcp,
    UnsafePath,
    Io(io::Error),
}

impl fmt::Display for ScaffoldError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidApplicationName => {
                "application name must be lower-case ASCII with letters, digits, and hyphens"
            }
            Self::DestinationExists => "destination already exists; no files were changed",
            Self::CompileContract => "the built-in application contract did not compile",
            Self::CompileQuery => "the built-in application query did not compile",
            Self::CompileRole => "the built-in application role did not compile",
            Self::Manifest => "the generated application manifest is invalid",
            Self::ApplicationSource => "the symbolic application source is invalid",
            Self::ApplicationLock => "the compiler-owned application lock is invalid",
            Self::LockRequired => {
                "symbolic application generation requires --locked and an exact lock"
            }
            Self::LockMismatch => {
                "application source, lock, or generated artifacts are stale or substituted"
            }
            Self::IdentityMismatch => {
                "application source no longer matches its pinned manifest identity"
            }
            Self::SourceLimit => "an application source exceeds the bounded compiler input limit",
            Self::GenerateMcp => "the generated MCP application surface is invalid",
            Self::UnsafePath => "an application path escapes the workspace or traverses a symlink",
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
    let root = manifest_path.parent().unwrap_or_else(|| Path::new("."));
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

pub(crate) fn check_application(source_path: &Path) -> Result<(), ScaffoldError> {
    let _ = compile_symbolic_application(source_path)?;
    Ok(())
}

pub(crate) fn write_application_lock(
    source_path: &Path,
    lock_path: Option<&Path>,
) -> Result<(), ScaffoldError> {
    let compiled = compile_symbolic_application(source_path)?;
    let root = source_path.parent().unwrap_or_else(|| Path::new("."));
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
    let root = source_path.parent().unwrap_or_else(|| Path::new("."));
    let lock_path = workspace_lock_path(root, lock_path)?;
    let existing = read_bounded(&lock_path, 4 * 1_024 * 1_024)?;
    let decoded =
        ApplicationLock::decode_canonical(&existing).map_err(|_| ScaffoldError::ApplicationLock)?;
    if decoded.canonical_bytes() != compiled.lock.canonical_bytes() {
        return Err(ScaffoldError::LockMismatch);
    }
    for (path, expected) in &compiled.outputs {
        let actual = read_workspace_file(root, path, 16 * 1_024 * 1_024)?;
        if &actual != expected {
            return Err(ScaffoldError::LockMismatch);
        }
    }
    Ok(())
}

fn generate_application_locked(source_path: &Path, lock_path: &Path) -> Result<(), ScaffoldError> {
    let compiled = compile_symbolic_application(source_path)?;
    let root = source_path.parent().unwrap_or_else(|| Path::new("."));
    let lock_path = workspace_lock_path(root, Some(lock_path))?;
    let existing = read_bounded(&lock_path, 4 * 1_024 * 1_024)?;
    let decoded =
        ApplicationLock::decode_canonical(&existing).map_err(|_| ScaffoldError::ApplicationLock)?;
    if decoded.canonical_bytes() != compiled.lock.canonical_bytes() {
        return Err(ScaffoldError::LockMismatch);
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
        .map_err(|_| ScaffoldError::ApplicationSource)?;
    let root = source_path.parent().unwrap_or_else(|| Path::new("."));
    let contract_source = read_workspace_text(root, source.contract().source(), 1_048_576)?;
    let contract =
        compile_contract_source(&contract_source).map_err(|_| ScaffoldError::CompileContract)?;
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
        modules.push(
            QueryModule::compile(candidate, &contract).map_err(|_| ScaffoldError::CompileQuery)?,
        );
    }
    let exact = source
        .exact_manifest(&contract, &modules)
        .map_err(|_| ScaffoldError::IdentityMismatch)?;
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
                .map_err(|_| ScaffoldError::ApplicationLock)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let lock = ApplicationLock::compile(&source, &exact, &contract, &modules, &artifacts)
        .map_err(|_| ScaffoldError::ApplicationLock)?;
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

impl From<io::Error> for ScaffoldError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

pub(crate) fn create_application(
    application: &str,
    language: ScaffoldLanguage,
    destination: &Path,
) -> Result<(), ScaffoldError> {
    if !valid_application_name(application) {
        return Err(ScaffoldError::InvalidApplicationName);
    }
    if destination.exists() {
        return Err(ScaffoldError::DestinationExists);
    }

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

    let parent = destination.parent().unwrap_or_else(|| Path::new("."));
    if !parent.is_dir() {
        return Err(ScaffoldError::Io(io::Error::new(
            io::ErrorKind::NotFound,
            "destination parent does not exist",
        )));
    }
    let temporary = create_temporary_directory(parent, application)?;
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
    .and_then(|()| fs::rename(&temporary, destination).map_err(ScaffoldError::Io));
    if result.is_err() {
        let _ = fs::remove_dir_all(&temporary);
    }
    result
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
        }
    }
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
        return Err(ScaffoldError::UnsafePath);
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
        return Err(ScaffoldError::UnsafePath);
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
        return Err(ScaffoldError::UnsafePath);
    }
    let relative = lock_path.to_str().ok_or(ScaffoldError::UnsafePath)?;
    if !valid_relative_path(relative) {
        return Err(ScaffoldError::UnsafePath);
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
        return Err(ScaffoldError::UnsafePath);
    }
    let root = fs::canonicalize(root)?;
    let path = fs::canonicalize(root.join(relative))?;
    if !path.starts_with(&root) {
        return Err(ScaffoldError::UnsafePath);
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
                return Err(ScaffoldError::UnsafePath);
            }
            Ok(metadata) if !metadata.is_dir() => return Err(ScaffoldError::UnsafePath),
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
                | ScaffoldError::LockMismatch)
        ));
        assert!(create_application("order-desk", ScaffoldLanguage::Rust, &first).is_err());
        fs::remove_dir_all(base).expect("cleanup");
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
        assert!(!generated.exists());
        assert!(!lock.exists());

        write_application_lock(&source, None).expect("write lock");
        fs::write(&generated, b"substituted\n").expect("substitute output");
        assert!(matches!(
            check_application_lock(&source, None),
            Err(ScaffoldError::LockMismatch)
        ));
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
            Err(ScaffoldError::UnsafePath)
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
