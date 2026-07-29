//! Deterministic, application-only repository scaffolding.

use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use riffdb_contract_compiler::compile_contract_source;
use riffdb_query_module::{
    ApplicationManifest, GeneratedMcpCommand, GeneratedMcpTool, NamedQuerySource, QueryModule,
    QueryModuleCandidate, QueryModuleName, QueryModuleVersion, compile_application_role,
    generate_mcp_commands, generate_mcp_tools, generate_rust_client, generate_typescript_client,
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
    IdentityMismatch,
    SourceLimit,
    GenerateMcp,
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
            Self::IdentityMismatch => {
                "application source no longer matches its pinned manifest identity"
            }
            Self::SourceLimit => "an application source exceeds the bounded compiler input limit",
            Self::GenerateMcp => "the generated MCP application surface is invalid",
            Self::Io(_) => "the application repository could not be written",
        })
    }
}

pub(crate) fn generate_application(manifest_path: &Path) -> Result<(), ScaffoldError> {
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
    let manifest_source = serde_json::to_string(&json!({
        "application": application,
        "contract": {
            "bundle_hash": hex(contract.bundle_hash().as_bytes()),
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
            "module_hash": hex(module.identity().as_bytes()),
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
        "schema": "riffdb.application-manifest/v1",
        "seed_inputs": ["riffdb/seed/01-CreateItem.jsonl"],
    }))
    .map_err(|_| ScaffoldError::Manifest)?;
    let manifest =
        ApplicationManifest::parse(&manifest_source).map_err(|_| ScaffoldError::Manifest)?;
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
    manifest: &[u8],
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
    write_file(root, "riffdb.application.json", manifest)?;
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
            "riffdb/contract.riff",
            "riffdb/queries/item_page.riffq",
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
        let manifest = fs::read(first.join("riffdb.application.json")).expect("manifest");
        ApplicationManifest::decode_canonical(&manifest).expect("canonical manifest");
        generate_application(&first.join("riffdb.application.json")).expect("regenerate");
        fs::write(
            first.join("riffdb/queries/item_page.riffq"),
            b"query ItemPage() { outcomes Found }\n",
        )
        .expect("change pinned source");
        assert!(matches!(
            generate_application(&first.join("riffdb.application.json")),
            Err(ScaffoldError::CompileQuery | ScaffoldError::IdentityMismatch)
        ));
        assert!(create_application("order-desk", ScaffoldLanguage::Rust, &first).is_err());
        fs::remove_dir_all(base).expect("cleanup");
    }
}
