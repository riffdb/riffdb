//! Credential-less local application-authoring MCP surface.

use std::borrow::Cow;
use std::fs;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;

use rmcp::ErrorData as McpError;
use rmcp::handler::server::ServerHandler;
use rmcp::model::{
    CallToolRequestParams, CallToolResult, ContentBlock, Implementation, InitializeRequestParams,
    InitializeResult, ListResourcesResult, ListToolsResult, PaginatedRequestParams,
    ReadResourceRequestParams, ReadResourceResult, Resource, ResourceContents, ResourcesCapability,
    ServerCapabilities, Tool, ToolAnnotations, ToolsCapability,
};
use rmcp::service::{RequestContext, RoleServer};
use serde_json::{Map, Value, json};

use crate::{MCP_OUTBOUND_MESSAGE_MAX_BYTES, MCP_PROTOCOL_VERSION};

const MAX_BUILDER_PATH_BYTES: usize = 4_096;
const MAX_BUILDER_RESOURCE_BYTES: usize = 1_048_576;
const MAX_BUILDER_PROCESS_BYTES: usize = 4_194_304;

const RESOURCE_CONTRACT_LANGUAGE: &str = "riffdb-builder://reference/contract-language";
const RESOURCE_APPLICATION_SCHEMA: &str = "riffdb-builder://reference/application-schema";
const RESOURCE_RIFFQL: &str = "riffdb-builder://reference/riffql";
const RESOURCE_EXAMPLES: &str = "riffdb-builder://reference/examples";
const RESOURCE_DIAGNOSTICS: &str = "riffdb-builder://reference/diagnostics";

/// Safe, closed failure returned when local builder paths are unavailable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BuilderConfigurationError;

impl std::fmt::Display for BuilderConfigurationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("builder configuration is invalid")
    }
}

impl std::error::Error for BuilderConfigurationError {}

/// Checked credential-less builder configuration.
#[derive(Clone)]
pub struct BuilderMcpConfiguration {
    workspace: PathBuf,
    kit_root: PathBuf,
    riffdb: PathBuf,
}

impl BuilderMcpConfiguration {
    /// Checks exact local paths without reading a credential or runtime config.
    pub fn new(
        workspace: PathBuf,
        kit_root: PathBuf,
        riffdb: PathBuf,
    ) -> Result<Self, BuilderConfigurationError> {
        for path in [&workspace, &kit_root, &riffdb] {
            if path.as_os_str().is_empty()
                || path.as_os_str().as_encoded_bytes().len() > MAX_BUILDER_PATH_BYTES
                || fs::symlink_metadata(path)
                    .map(|metadata| metadata.file_type().is_symlink())
                    .unwrap_or(true)
            {
                return Err(BuilderConfigurationError);
            }
        }
        if !workspace.is_dir() || !kit_root.is_dir() || !riffdb.is_file() {
            return Err(BuilderConfigurationError);
        }
        Ok(Self {
            workspace: fs::canonicalize(workspace).map_err(|_| BuilderConfigurationError)?,
            kit_root: fs::canonicalize(kit_root).map_err(|_| BuilderConfigurationError)?,
            riffdb: fs::canonicalize(riffdb).map_err(|_| BuilderConfigurationError)?,
        })
    }
}

impl std::fmt::Debug for BuilderMcpConfiguration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("BuilderMcpConfiguration([LOCAL PATHS REDACTED])")
    }
}

/// MCP server that can only inspect or update local compiler-owned artifacts.
#[derive(Clone, Debug)]
pub struct BuilderMcpServer {
    configuration: BuilderMcpConfiguration,
}

impl BuilderMcpServer {
    /// Creates one local builder server. No credential or runtime client exists.
    #[must_use]
    pub const fn new(configuration: BuilderMcpConfiguration) -> Self {
        Self { configuration }
    }

    fn tools() -> Vec<Tool> {
        vec![
            builder_tool(
                "riffdb_builder_describe",
                "Describe the symbolic application authoring workflow.",
                false,
                empty_schema(),
            ),
            builder_tool(
                "riffdb_builder_application_check",
                "Compile and safety-check local symbolic application sources without writing.",
                false,
                empty_schema(),
            ),
            builder_tool(
                "riffdb_builder_diagnostic_explain",
                "Explain how to apply one stable authoring diagnostic.",
                false,
                object_schema(&["code"]),
            ),
            builder_tool(
                "riffdb_builder_lock_preview",
                "Compile the exact proposed lock and authority definition without writing.",
                false,
                empty_schema(),
            ),
            builder_tool(
                "riffdb_builder_lock_write",
                "Explicitly write the reviewed exact lock and generated artifacts.",
                true,
                empty_schema(),
            ),
            builder_tool(
                "riffdb_builder_generate",
                "Reproduce generated bindings from the exact current lock.",
                true,
                empty_schema(),
            ),
        ]
    }

    fn resources() -> Vec<Resource> {
        [
            (
                RESOURCE_CONTRACT_LANGUAGE,
                "Contract language v1",
                "text/markdown",
            ),
            (
                RESOURCE_APPLICATION_SCHEMA,
                "Application source schema v1",
                "application/schema+json",
            ),
            (RESOURCE_RIFFQL, "RiffQL language v1", "text/markdown"),
            (
                RESOURCE_EXAMPLES,
                "Multi-entity authoring examples",
                "text/markdown",
            ),
            (
                RESOURCE_DIAGNOSTICS,
                "Authoring diagnostics",
                "text/markdown",
            ),
        ]
        .into_iter()
        .map(|(uri, title, mime)| {
            Resource::new(uri, uri)
                .with_title(title)
                .with_description("Bounded public RiffDB authoring reference")
                .with_mime_type(mime)
        })
        .collect()
    }

    fn call(&self, name: &str, arguments: Map<String, Value>) -> Result<Value, Value> {
        match name {
            "riffdb_builder_describe" if arguments.is_empty() => Ok(json!({
                "schema": "riffdb.builder-result/v1",
                "status": "described",
                "operations": [
                    "application_check",
                    "lock_preview",
                    "lock_write",
                    "generate"
                ],
                "runtime_authority": false
            })),
            "riffdb_builder_application_check" if arguments.is_empty() => {
                self.run_application(&["application", "check"]).map(|_| {
                    json!({
                        "schema": "riffdb.builder-result/v1",
                        "status": "checked",
                        "files_changed": false
                    })
                })
            }
            "riffdb_builder_lock_preview" if arguments.is_empty() => {
                let output = self.run_application(&["application", "preview"])?;
                let lock: Value = serde_json::from_slice(&output).map_err(|_| internal_result())?;
                let current = fs::read(
                    self.configuration
                        .workspace
                        .join("riffdb.application.lock.json"),
                )
                .ok();
                Ok(json!({
                    "schema": "riffdb.builder-result/v1",
                    "status": "previewed",
                    "changed": current.as_deref() != Some(output.as_slice()),
                    "lock": lock
                }))
            }
            "riffdb_builder_lock_write" if arguments.is_empty() => self
                .run_application(&["application", "lock", "--write"])
                .map(|_| {
                    json!({
                        "schema": "riffdb.builder-result/v1",
                        "status": "written"
                    })
                }),
            "riffdb_builder_generate" if arguments.is_empty() => self
                .run_application(&["application", "generate", "--locked"])
                .map(|_| {
                    json!({
                        "schema": "riffdb.builder-result/v1",
                        "status": "generated"
                    })
                }),
            "riffdb_builder_diagnostic_explain" => {
                let code = arguments.get("code").and_then(Value::as_str).ok_or_else(|| {
                    json!({"code": "RDB-BUILDER-INPUT", "message": "diagnostic code is required"})
                })?;
                if arguments.len() != 1 || !valid_diagnostic_code(code) {
                    return Err(json!({
                        "code": "RDB-BUILDER-INPUT",
                        "message": "diagnostic code is invalid"
                    }));
                }
                Ok(json!({
                    "schema": "riffdb.builder-result/v1",
                    "status": "explained",
                    "code": code,
                    "guidance": [
                        "Use the diagnostic path, span, symbolic path, cause, and suggested fixes.",
                        "Correct author-owned symbolic source before writing a replacement lock.",
                        "Do not edit generated identities or broaden runtime authority."
                    ],
                    "reference": RESOURCE_DIAGNOSTICS
                }))
            }
            _ => Err(json!({
                "code": "RDB-BUILDER-INPUT",
                "message": "builder tool or arguments are invalid"
            })),
        }
    }

    fn run_application(&self, arguments: &[&str]) -> Result<Vec<u8>, Value> {
        let output = Command::new(&self.configuration.riffdb)
            .arg("--output")
            .arg("json")
            .args(arguments)
            .current_dir(&self.configuration.workspace)
            .env_clear()
            .stdin(Stdio::null())
            .output()
            .map_err(|_| internal_result())?;
        if output.stdout.len() > MAX_BUILDER_PROCESS_BYTES
            || output.stderr.len() > MAX_BUILDER_PROCESS_BYTES
        {
            return Err(internal_result());
        }
        if output.status.success() {
            return Ok(output.stdout);
        }
        let diagnostic =
            serde_json::from_slice::<Value>(&output.stderr).map_err(|_| internal_result())?;
        Err(diagnostic)
    }

    fn read_resource(&self, uri: &str) -> Result<(String, &'static str), McpError> {
        let (relative, mime) = match uri {
            RESOURCE_CONTRACT_LANGUAGE => ("docs/contracts/LANGUAGE.md", "text/markdown"),
            RESOURCE_APPLICATION_SCHEMA => (
                "docs/getting-started/application-source-v1.schema.json",
                "application/schema+json",
            ),
            RESOURCE_RIFFQL => ("docs/riffql/LANGUAGE.md", "text/markdown"),
            RESOURCE_EXAMPLES => (
                "docs/contracts/examples/NEGATIVE-EXAMPLES.md",
                "text/markdown",
            ),
            RESOURCE_DIAGNOSTICS => (
                "docs/getting-started/AUTHORING-DIAGNOSTICS.md",
                "text/markdown",
            ),
            _ => return Err(McpError::invalid_params("unknown builder resource", None)),
        };
        let path = self.configuration.kit_root.join(relative);
        let canonical = fs::canonicalize(&path)
            .map_err(|_| McpError::internal_error("builder resource unavailable", None))?;
        if !canonical.starts_with(&self.configuration.kit_root) {
            return Err(McpError::internal_error(
                "builder resource unavailable",
                None,
            ));
        }
        let metadata = fs::symlink_metadata(&canonical)
            .map_err(|_| McpError::internal_error("builder resource unavailable", None))?;
        if !metadata.is_file() || metadata.len() > MAX_BUILDER_RESOURCE_BYTES as u64 {
            return Err(McpError::internal_error(
                "builder resource unavailable",
                None,
            ));
        }
        let text = fs::read_to_string(canonical)
            .map_err(|_| McpError::internal_error("builder resource unavailable", None))?;
        Ok((text, mime))
    }
}

impl ServerHandler for BuilderMcpServer {
    async fn initialize(
        &self,
        request: InitializeRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<InitializeResult, McpError> {
        if request.protocol_version.as_str() != MCP_PROTOCOL_VERSION {
            return Err(McpError::invalid_params(
                "unsupported MCP protocol version",
                None,
            ));
        }
        context.peer.set_peer_info(request);
        Ok(builder_initialization_result())
    }

    async fn ping(&self, _: RequestContext<RoleServer>) -> Result<(), McpError> {
        Ok(())
    }

    async fn list_tools(
        &self,
        request: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        if request.and_then(|request| request.cursor).is_some() {
            return Err(McpError::invalid_params(
                "builder tools are not paginated",
                None,
            ));
        }
        Ok(ListToolsResult {
            meta: None,
            next_cursor: None,
            tools: Self::tools(),
        })
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        Self::tools()
            .into_iter()
            .find(|tool| tool.name.as_ref() == name)
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, McpError> {
        if request.task.is_some() || request.name.len() > 128 {
            return Err(McpError::invalid_params("invalid builder tool", None));
        }
        let server = self.clone();
        let name = request.name.into_owned();
        let arguments = request.arguments.unwrap_or_default();
        let result = tokio::task::spawn_blocking(move || server.call(&name, arguments))
            .await
            .map_err(|_| McpError::internal_error("builder operation failed", None))?;
        let (value, is_error) = match result {
            Ok(value) => (value, false),
            Err(value) => (value, true),
        };
        let text = serde_json::to_string(&value)
            .map_err(|_| McpError::internal_error("builder result unavailable", None))?;
        if text.len() > MCP_OUTBOUND_MESSAGE_MAX_BYTES {
            return Err(McpError::internal_error("builder result unavailable", None));
        }
        let mut response = if is_error {
            CallToolResult::error(vec![ContentBlock::text(text)])
        } else {
            CallToolResult::success(vec![ContentBlock::text(text)])
        };
        response.structured_content = Some(value);
        Ok(response)
    }

    async fn list_resources(
        &self,
        request: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, McpError> {
        if request.and_then(|request| request.cursor).is_some() {
            return Err(McpError::invalid_params(
                "builder resources are not paginated",
                None,
            ));
        }
        Ok(ListResourcesResult {
            meta: None,
            next_cursor: None,
            resources: Self::resources(),
        })
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResult, McpError> {
        let (text, mime) = self.read_resource(&request.uri)?;
        Ok(ReadResourceResult::new(vec![
            ResourceContents::text(text, request.uri).with_mime_type(mime),
        ]))
    }

    fn get_info(&self) -> rmcp::model::ServerInfo {
        builder_initialization_result()
    }
}

fn builder_initialization_result() -> InitializeResult {
    let mut capabilities = ServerCapabilities::default();
    capabilities.tools = Some(ToolsCapability::default());
    capabilities.resources = Some(ResourcesCapability::default());
    InitializeResult::new(capabilities).with_server_info(Implementation::new(
        "riffdb-builder",
        env!("CARGO_PKG_VERSION"),
    ))
}

fn builder_tool(
    name: &'static str,
    description: &'static str,
    mutating: bool,
    input: Map<String, Value>,
) -> Tool {
    Tool::new_with_raw(name, Some(Cow::Borrowed(description)), Arc::new(input))
        .with_raw_output_schema(Arc::new(result_schema()))
        .with_annotations(ToolAnnotations::from_raw(
            None,
            Some(!mutating),
            Some(mutating),
            Some(true),
            Some(false),
        ))
}

fn empty_schema() -> Map<String, Value> {
    json!({
        "type": "object",
        "additionalProperties": false
    })
    .as_object()
    .cloned()
    .expect("static object")
}

fn object_schema(required: &[&str]) -> Map<String, Value> {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": required,
        "properties": {
            "code": {
                "type": "string",
                "maxLength": 32
            }
        }
    })
    .as_object()
    .cloned()
    .expect("static object")
}

fn result_schema() -> Map<String, Value> {
    json!({
        "type": "object"
    })
    .as_object()
    .cloned()
    .expect("static object")
}

fn valid_diagnostic_code(code: &str) -> bool {
    let Some(suffix) = code.strip_prefix("RDB-") else {
        return false;
    };
    if !suffix.is_ascii() || suffix.len() < 4 || suffix.len() > 6 {
        return false;
    }
    let split = suffix.len() - 3;
    suffix[..split]
        .bytes()
        .all(|byte| byte.is_ascii_uppercase())
        && suffix[split..].bytes().all(|byte| byte.is_ascii_digit())
}

fn internal_result() -> Value {
    json!({
        "code": "RDB-BUILDER-INTERNAL",
        "message": "builder operation failed"
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_inventory_has_no_runtime_or_authority_operation() {
        let names = BuilderMcpServer::tools()
            .into_iter()
            .map(|tool| tool.name.into_owned())
            .collect::<Vec<_>>();
        assert_eq!(names.len(), 6);
        for forbidden in [
            "deploy",
            "bind",
            "credential",
            "command.run",
            "query.run",
            "entity",
            "storage",
            "kernel",
        ] {
            assert!(
                names.iter().all(|name| !name.contains(forbidden)),
                "{forbidden}"
            );
        }
    }

    #[test]
    fn diagnostic_code_grammar_is_closed() {
        assert!(valid_diagnostic_code("RDB-QP003"));
        assert!(valid_diagnostic_code("RDB-AS007"));
        assert!(!valid_diagnostic_code("RDB-QP"));
        assert!(!valid_diagnostic_code("RDB-qp"));
        assert!(!valid_diagnostic_code("AUTH"));
    }
}
