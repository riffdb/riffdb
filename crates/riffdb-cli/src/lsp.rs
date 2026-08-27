//! Bounded, compiler-backed Language Server Protocol surface.
//!
//! This module deliberately owns protocol framing only. Contract and RiffQL
//! acceptance remain in the first-party parsers and compilers; no editor
//! request can reach credentials, transport, or database state.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use riffdb_contract_compiler::compile_contract_source;
use riffdb_contract_syntax::ast::{AggregateItem, Declaration, EntityItem};
use riffdb_contract_syntax::{Span as ContractSpan, parse_contract};
use riffdb_diagnostics::{AuthoringDiagnostics, AuthoringSourcePath};
use riffdb_query_module::{
    ApplicationSourceManifest, NamedQuerySource, QueryModule, QueryModuleCandidate,
    QueryModuleName, QueryModuleVersion,
};
use riffdb_riffql_syntax::{Document as QueryDocument, parse_query};
use serde_json::{Value, json};

const MAX_FRAME_BYTES: usize = 1_048_576;
const MAX_HEADER_BYTES: usize = 8_192;
const MAX_OPEN_DOCUMENTS: usize = 32;
const MAX_COMPLETIONS: usize = 256;
const MAX_CANCELLED_REQUESTS: usize = 256;
const MAX_REQUEST_ID_BYTES: usize = 256;
const MAX_RESPONSE_BYTES: usize = 524_288;
const MAX_URI_BYTES: usize = 4_096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DocumentKind {
    Contract,
    Query,
    Unsupported,
}

#[derive(Clone, Debug)]
struct OpenDocument {
    text: String,
    version: i64,
    kind: DocumentKind,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct EditorSymbol {
    name: String,
    detail: String,
    uri: String,
    span: ByteSpan,
    kind: u8,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ByteSpan {
    start: usize,
    end: usize,
}

impl From<ContractSpan> for ByteSpan {
    fn from(span: ContractSpan) -> Self {
        Self {
            start: span.start() as usize,
            end: span.end() as usize,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PrimaryDiagnostic {
    code: String,
    message: String,
    span: ByteSpan,
    stage: String,
}

#[derive(Default)]
struct Server {
    root: Option<PathBuf>,
    documents: BTreeMap<String, OpenDocument>,
    cancelled: BTreeSet<String>,
    shutdown: bool,
}

/// Runs one local stdio LSP process. No configuration, credentials, or network
/// clients are constructed on this path.
pub(crate) fn run_stdio() -> ExitCode {
    let stdin = io::stdin();
    let stdout = io::stdout();
    let mut reader = BufReader::new(stdin.lock());
    let mut writer = stdout.lock();
    let mut server = Server::default();

    loop {
        let message = match read_frame(&mut reader) {
            Ok(Some(message)) => message,
            Ok(None) => return ExitCode::SUCCESS,
            Err(()) => return ExitCode::FAILURE,
        };
        let value = match serde_json::from_slice::<Value>(&message) {
            Ok(value) => value,
            Err(_) => {
                if write_value(
                    &mut writer,
                    &error_response(Value::Null, -32700, "parse error"),
                )
                .is_err()
                {
                    return ExitCode::FAILURE;
                }
                continue;
            }
        };
        let (responses, exit) = server.handle(value);
        for response in responses {
            if write_value(&mut writer, &response).is_err() {
                return ExitCode::FAILURE;
            }
        }
        if exit {
            return ExitCode::SUCCESS;
        }
    }
}

impl Server {
    fn handle(&mut self, message: Value) -> (Vec<Value>, bool) {
        let Some(object) = message.as_object() else {
            return (
                vec![error_response(Value::Null, -32600, "invalid request")],
                false,
            );
        };
        let method = object.get("method").and_then(Value::as_str);
        let id = object.get("id").cloned();
        let params = object.get("params").cloned().unwrap_or(Value::Null);
        let Some(method) = method else {
            return (
                id.map_or_else(Vec::new, |id| {
                    vec![error_response(id, -32600, "invalid request")]
                }),
                false,
            );
        };

        if method == "exit" {
            return (Vec::new(), true);
        }
        if self.shutdown {
            return (
                id.map_or_else(Vec::new, |id| {
                    vec![error_response(id, -32600, "server is shut down")]
                }),
                false,
            );
        }
        if method == "$/cancelRequest" {
            if self.cancelled.len() < MAX_CANCELLED_REQUESTS
                && let Some(cancelled) = params.get("id").and_then(request_key)
            {
                self.cancelled.insert(cancelled);
            }
            return (Vec::new(), false);
        }

        if let Some(id) = id.as_ref()
            && request_key(id).is_some_and(|key| self.cancelled.remove(&key))
        {
            return (
                vec![error_response(id.clone(), -32800, "request cancelled")],
                false,
            );
        }

        match method {
            "initialize" => {
                self.root = initialize_root(&params);
                let response = success_response(
                    id.unwrap_or(Value::Null),
                    json!({
                        "capabilities": {
                            "textDocumentSync": {"openClose": true, "change": 1},
                            "hoverProvider": true,
                            "definitionProvider": true,
                            "completionProvider": {"resolveProvider": false, "triggerCharacters": [".", "$"]}
                        },
                        "serverInfo": {"name": "riffdb-lsp", "version": env!("CARGO_PKG_VERSION")}
                    }),
                );
                (vec![response], false)
            }
            "initialized" => (Vec::new(), false),
            "shutdown" => {
                self.shutdown = true;
                (
                    vec![success_response(id.unwrap_or(Value::Null), Value::Null)],
                    false,
                )
            }
            "textDocument/didOpen" => (self.did_open(&params), false),
            "textDocument/didChange" => (self.did_change(&params), false),
            "textDocument/didClose" => (self.did_close(&params), false),
            "textDocument/hover" => (
                vec![success_response(
                    id.unwrap_or(Value::Null),
                    self.hover(&params),
                )],
                false,
            ),
            "textDocument/definition" => (
                vec![success_response(
                    id.unwrap_or(Value::Null),
                    self.definition(&params),
                )],
                false,
            ),
            "textDocument/completion" => (
                vec![success_response(
                    id.unwrap_or(Value::Null),
                    self.completion(&params),
                )],
                false,
            ),
            _ if id.is_some() => (
                vec![error_response(
                    id.unwrap_or(Value::Null),
                    -32601,
                    "method not found",
                )],
                false,
            ),
            _ => (Vec::new(), false),
        }
    }

    fn did_open(&mut self, params: &Value) -> Vec<Value> {
        let Some(document) = params.get("textDocument") else {
            return Vec::new();
        };
        let Some(uri) = checked_string(document.get("uri"), MAX_URI_BYTES) else {
            return Vec::new();
        };
        let Some(text) = checked_string(document.get("text"), MAX_FRAME_BYTES) else {
            return vec![publish_diagnostics(uri, None, Vec::new())];
        };
        let version = document.get("version").and_then(Value::as_i64).unwrap_or(0);
        if !self.documents.contains_key(uri) && self.documents.len() >= MAX_OPEN_DOCUMENTS {
            return vec![publish_diagnostics(uri, Some(version), Vec::new())];
        }
        self.documents.insert(
            uri.to_owned(),
            OpenDocument {
                text: text.to_owned(),
                version,
                kind: document_kind(uri),
            },
        );
        vec![self.diagnostics_notification(uri)]
    }

    fn did_change(&mut self, params: &Value) -> Vec<Value> {
        let Some(document) = params.get("textDocument") else {
            return Vec::new();
        };
        let Some(uri) = checked_string(document.get("uri"), MAX_URI_BYTES) else {
            return Vec::new();
        };
        let version = document.get("version").and_then(Value::as_i64).unwrap_or(0);
        let text = params
            .get("contentChanges")
            .and_then(Value::as_array)
            .and_then(|changes| (changes.len() == 1).then_some(&changes[0]))
            .and_then(|change| checked_string(change.get("text"), MAX_FRAME_BYTES));
        let Some(text) = text else {
            return Vec::new();
        };
        let Some(open) = self.documents.get_mut(uri) else {
            return Vec::new();
        };
        open.text = text.to_owned();
        open.version = version;
        vec![self.diagnostics_notification(uri)]
    }

    fn did_close(&mut self, params: &Value) -> Vec<Value> {
        let Some(uri) = params
            .get("textDocument")
            .and_then(|document| checked_string(document.get("uri"), MAX_URI_BYTES))
        else {
            return Vec::new();
        };
        self.documents.remove(uri);
        vec![publish_diagnostics(uri, None, Vec::new())]
    }

    fn diagnostics_notification(&self, uri: &str) -> Value {
        let Some(document) = self.documents.get(uri) else {
            return publish_diagnostics(uri, None, Vec::new());
        };
        let diagnostics = match document.kind {
            DocumentKind::Contract => contract_primary_diagnostics(uri, &document.text),
            DocumentKind::Query => self.query_diagnostics(uri, &document.text),
            DocumentKind::Unsupported => Vec::new(),
        };
        publish_diagnostics(
            uri,
            Some(document.version),
            diagnostics
                .into_iter()
                .map(|diagnostic| diagnostic_json(&document.text, diagnostic))
                .collect(),
        )
    }

    fn query_diagnostics(&self, uri: &str, source: &str) -> Vec<PrimaryDiagnostic> {
        let document = match parse_query(source) {
            Ok(document) => document,
            Err(diagnostics) => {
                return diagnostics
                    .as_slice()
                    .iter()
                    .map(|diagnostic| PrimaryDiagnostic {
                        code: diagnostic.code().as_str().to_owned(),
                        message: diagnostic.summary().to_owned(),
                        span: ByteSpan {
                            start: diagnostic.span().start as usize,
                            end: diagnostic.span().end as usize,
                        },
                        stage: "query_syntax".to_owned(),
                    })
                    .collect();
            }
        };
        let Some(contract) = self.local_contract(uri) else {
            return Vec::new();
        };
        let Some(name) = document.name.as_ref().map(|name| name.value.as_str()) else {
            return Vec::new();
        };
        let Ok(query) = NamedQuerySource::new(name, source) else {
            return Vec::new();
        };
        let Ok(module_name) = QueryModuleName::new("editor") else {
            return Vec::new();
        };
        let Some(version) = QueryModuleVersion::new(1) else {
            return Vec::new();
        };
        let Ok(candidate) = QueryModuleCandidate::new(module_name, version, vec![query]) else {
            return Vec::new();
        };
        match QueryModule::compile(candidate, &contract) {
            Ok(_) => Vec::new(),
            Err(error) => {
                let Ok(path) = AuthoringSourcePath::new(diagnostic_path(uri)) else {
                    return Vec::new();
                };
                AuthoringDiagnostics::from_query_module(path, &error)
                    .map(|diagnostics| authoring_primary(&diagnostics))
                    .unwrap_or_default()
            }
        }
    }

    fn local_contract(&self, query_uri: &str) -> Option<riffdb_contract_ir::ContractBundle> {
        if let Some((root, manifest)) = self.workspace_manifest() {
            let query_path = file_uri_path(query_uri)?;
            let query_relative = query_path.strip_prefix(&root).ok()?;
            let declared = manifest
                .query_modules()
                .iter()
                .flat_map(|module| module.queries())
                .any(|query| Path::new(query.source()) == query_relative);
            if !declared {
                return None;
            }
            let contract_path = checked_workspace_path(&root, manifest.contract().source())?;
            let contract_source = self
                .open_document_at(&contract_path)
                .map(|document| document.text.clone())
                .or_else(|| read_bounded(&contract_path))?;
            return compile_contract_source(&contract_source).ok();
        }

        let mut contracts = self
            .documents
            .values()
            .filter(|document| document.kind == DocumentKind::Contract)
            .filter_map(|document| compile_contract_source(&document.text).ok());
        let contract = contracts.next()?;
        contracts.next().is_none().then_some(contract)
    }

    fn workspace_manifest(&self) -> Option<(PathBuf, ApplicationSourceManifest)> {
        let root = self.root.as_ref()?.canonicalize().ok()?;
        let source = read_bounded(&root.join("riffdb.application.json"))?;
        let manifest = ApplicationSourceManifest::parse(&source).ok()?;
        Some((root, manifest))
    }

    fn open_document_at(&self, path: &Path) -> Option<&OpenDocument> {
        self.documents.iter().find_map(|(uri, document)| {
            file_uri_path(uri)
                .and_then(|candidate| candidate.canonicalize().ok())
                .filter(|candidate| candidate == path)
                .map(|_| document)
        })
    }

    fn workspace_symbols(&self) -> Vec<EditorSymbol> {
        let Some((root, manifest)) = self.workspace_manifest() else {
            return Vec::new();
        };
        let mut symbols = Vec::new();
        if let Some(path) = checked_workspace_path(&root, manifest.contract().source()) {
            let uri = path_to_file_uri(&path);
            let source = self
                .documents
                .get(&uri)
                .map(|document| document.text.clone())
                .or_else(|| read_bounded(&path));
            if let Some(source) = source {
                symbols.extend(contract_symbols(&uri, &source));
            }
        }
        for query in manifest
            .query_modules()
            .iter()
            .flat_map(|module| module.queries())
            .take(MAX_OPEN_DOCUMENTS)
        {
            if symbols.len() >= MAX_COMPLETIONS {
                break;
            }
            let Some(path) = checked_workspace_path(&root, query.source()) else {
                continue;
            };
            let uri = path_to_file_uri(&path);
            let source = self
                .documents
                .get(&uri)
                .map(|document| document.text.clone())
                .or_else(|| read_bounded(&path));
            if let Some(source) = source {
                symbols.extend(query_symbols(&uri, &source));
            }
        }
        symbols
    }

    fn all_symbols(&self) -> Vec<EditorSymbol> {
        let mut symbols = self.workspace_symbols();
        for (uri, document) in &self.documents {
            symbols.extend(document_symbols(uri, document));
            if symbols.len() >= MAX_COMPLETIONS {
                break;
            }
        }
        symbols.sort_by(|left, right| {
            left.name
                .cmp(&right.name)
                .then_with(|| left.uri.cmp(&right.uri))
                .then_with(|| left.span.start.cmp(&right.span.start))
        });
        symbols.dedup_by(|left, right| {
            left.name == right.name && left.uri == right.uri && left.span == right.span
        });
        symbols.truncate(MAX_COMPLETIONS);
        symbols
    }

    fn hover(&self, params: &Value) -> Value {
        let Some((uri, offset)) = self.request_location(params) else {
            return Value::Null;
        };
        let Some(document) = self.documents.get(uri) else {
            return Value::Null;
        };
        let Some((_, _, word)) = word_span_at(&document.text, offset) else {
            return Value::Null;
        };
        let matches = self.matching_symbols(uri, &document.text, offset, word);
        let [symbol] = matches.as_slice() else {
            return Value::Null;
        };
        json!({"contents": {"kind": "plaintext", "value": symbol.detail.clone()}})
    }

    fn definition(&self, params: &Value) -> Value {
        let Some((uri, offset)) = self.request_location(params) else {
            return Value::Null;
        };
        let Some(document) = self.documents.get(uri) else {
            return Value::Null;
        };
        let Some((_, _, word)) = word_span_at(&document.text, offset) else {
            return Value::Null;
        };
        let matches = self.matching_symbols(uri, &document.text, offset, word);
        if matches.is_empty() {
            return Value::Null;
        }
        let locations = matches
            .into_iter()
            .filter_map(|symbol| {
                self.symbol_source(&symbol).map(|source| {
                    json!({
                        "uri": symbol.uri,
                        "range": range_json(&source, symbol.span)
                    })
                })
            })
            .collect::<Vec<_>>();
        match locations.as_slice() {
            [] => Value::Null,
            [location] => location.clone(),
            _ => Value::Array(locations),
        }
    }

    fn completion(&self, _params: &Value) -> Value {
        Value::Array(
            self.all_symbols()
                .into_iter()
                .map(|symbol| {
                    json!({
                        "label": symbol.name,
                        "kind": symbol.kind,
                        "detail": symbol.detail
                    })
                })
                .collect(),
        )
    }

    fn matching_symbols(
        &self,
        uri: &str,
        source: &str,
        offset: usize,
        word: &str,
    ) -> Vec<EditorSymbol> {
        let mut matches = self
            .all_symbols()
            .into_iter()
            .filter(|symbol| symbol.name == word)
            .collect::<Vec<_>>();
        if let Some(exact) = matches
            .iter()
            .find(|symbol| {
                symbol.uri == uri && symbol.span.start <= offset && offset <= symbol.span.end
            })
            .cloned()
        {
            return vec![exact];
        }
        if let Some(owner) = qualified_owner(source, offset) {
            let entity = if document_kind(uri) == DocumentKind::Query {
                Some(
                    parse_query(source)
                        .ok()
                        .and_then(|document| {
                            document
                                .body
                                .bindings
                                .iter()
                                .find(|binding| binding.name.value.as_str() == owner)
                                .map(|binding| binding.entity.value.as_str().to_owned())
                        })
                        .unwrap_or_else(|| owner.to_owned()),
                )
            } else {
                Some(owner.to_owned())
            };
            if let Some(entity) = entity {
                let prefix = format!("{entity}.{word}:");
                matches.retain(|symbol| symbol.detail.starts_with(&prefix));
            }
        }
        matches.truncate(16);
        matches
    }

    fn symbol_source(&self, symbol: &EditorSymbol) -> Option<String> {
        self.documents
            .get(&symbol.uri)
            .map(|document| document.text.clone())
            .or_else(|| file_uri_path(&symbol.uri).and_then(|path| read_bounded(&path)))
    }

    fn request_location<'a>(&'a self, params: &'a Value) -> Option<(&'a str, usize)> {
        let uri = params
            .get("textDocument")
            .and_then(|document| checked_string(document.get("uri"), MAX_URI_BYTES))?;
        let document = self.documents.get(uri)?;
        let position = params.get("position")?;
        let line = usize::try_from(position.get("line")?.as_u64()?).ok()?;
        let character = usize::try_from(position.get("character")?.as_u64()?).ok()?;
        let offset = utf16_position_to_byte(&document.text, line, character)?;
        Some((uri, offset))
    }
}

fn contract_primary_diagnostics(uri: &str, source: &str) -> Vec<PrimaryDiagnostic> {
    let Err(error) = compile_contract_source(source) else {
        return Vec::new();
    };
    crate::scaffold::contract_authoring_diagnostics(&diagnostic_path(uri), &error)
        .map(|diagnostics| authoring_primary(&diagnostics))
        .unwrap_or_default()
}

fn authoring_primary(diagnostics: &AuthoringDiagnostics) -> Vec<PrimaryDiagnostic> {
    diagnostics
        .as_slice()
        .iter()
        .filter_map(|diagnostic| {
            let span = diagnostic.span()?;
            Some(PrimaryDiagnostic {
                code: diagnostic.code().as_str().to_owned(),
                message: diagnostic.summary().to_owned(),
                span: ByteSpan {
                    start: span.start() as usize,
                    end: span.end() as usize,
                },
                stage: diagnostic.stage().as_str().to_owned(),
            })
        })
        .collect()
}

fn document_symbols(uri: &str, document: &OpenDocument) -> Vec<EditorSymbol> {
    match document.kind {
        DocumentKind::Contract => contract_symbols(uri, &document.text),
        DocumentKind::Query => query_symbols(uri, &document.text),
        DocumentKind::Unsupported => Vec::new(),
    }
}

fn contract_symbols(uri: &str, source: &str) -> Vec<EditorSymbol> {
    if compile_contract_source(source).is_err() {
        return Vec::new();
    }
    let Ok(document) = parse_contract(source) else {
        return Vec::new();
    };
    let mut symbols = Vec::new();
    push_symbol(
        &mut symbols,
        uri,
        &document.contract.value.name.value,
        format!("contract {}", document.contract.value.name.value),
        document.contract.value.name.span.into(),
        5,
    );
    for declaration in &document.contract.value.declarations {
        match &declaration.value {
            Declaration::Entity(entity) => {
                push_symbol(
                    &mut symbols,
                    uri,
                    &entity.name.value,
                    format!("entity {}", entity.name.value),
                    entity.name.span.into(),
                    5,
                );
                for item in &entity.items {
                    match &item.value {
                        EntityItem::Key(key) => {
                            for field in &key.fields {
                                typed_symbol(
                                    &mut symbols,
                                    uri,
                                    source,
                                    &entity.name.value,
                                    &field.value.name.value,
                                    field.value.name.span,
                                    field.value.ty.span,
                                );
                            }
                        }
                        EntityItem::Field(field) => typed_symbol(
                            &mut symbols,
                            uri,
                            source,
                            &entity.name.value,
                            &field.name.value,
                            field.name.span,
                            field.ty.span,
                        ),
                        EntityItem::Index(index) => push_symbol(
                            &mut symbols,
                            uri,
                            &index.name.value,
                            format!("index {}.{}", entity.name.value, index.name.value),
                            index.name.span.into(),
                            12,
                        ),
                        EntityItem::Unique(unique) => push_symbol(
                            &mut symbols,
                            uri,
                            &unique.name.value,
                            format!("unique {}.{}", entity.name.value, unique.name.value),
                            unique.name.span.into(),
                            12,
                        ),
                        _ => {}
                    }
                }
            }
            Declaration::Event(event) => {
                push_symbol(
                    &mut symbols,
                    uri,
                    &event.name.value,
                    format!("event {}", event.name.value),
                    event.name.span.into(),
                    5,
                );
                for field in &event.fields {
                    typed_symbol(
                        &mut symbols,
                        uri,
                        source,
                        &event.name.value,
                        &field.value.name.value,
                        field.value.name.span,
                        field.value.ty.span,
                    );
                }
            }
            Declaration::Enum(enumeration) => {
                push_symbol(
                    &mut symbols,
                    uri,
                    &enumeration.name.value,
                    format!("enum {}", enumeration.name.value),
                    enumeration.name.span.into(),
                    10,
                );
                for variant in &enumeration.variants {
                    push_symbol(
                        &mut symbols,
                        uri,
                        &variant.value,
                        format!("{}.{}", enumeration.name.value, variant.value),
                        variant.span.into(),
                        20,
                    );
                }
            }
            Declaration::Command(command) => {
                push_symbol(
                    &mut symbols,
                    uri,
                    &command.name.value,
                    format!("command {}", command.name.value),
                    command.name.span.into(),
                    12,
                );
                for input in &command.inputs {
                    typed_symbol(
                        &mut symbols,
                        uri,
                        source,
                        &command.name.value,
                        &input.value.field.name.value,
                        input.value.field.name.span,
                        input.value.field.ty.span,
                    );
                }
            }
            Declaration::Aggregate(aggregate) => {
                push_symbol(
                    &mut symbols,
                    uri,
                    &aggregate.name.value,
                    format!("aggregate {}", aggregate.name.value),
                    aggregate.name.span.into(),
                    5,
                );
                for item in &aggregate.items {
                    if let AggregateItem::Invariant(invariant) = &item.value {
                        push_symbol(
                            &mut symbols,
                            uri,
                            &invariant.name.value,
                            format!(
                                "invariant {}.{}",
                                aggregate.name.value, invariant.name.value
                            ),
                            invariant.name.span.into(),
                            12,
                        );
                    }
                }
            }
            Declaration::Projection(projection) => push_symbol(
                &mut symbols,
                uri,
                &projection.name.value,
                format!("projection {}", projection.name.value),
                projection.name.span.into(),
                12,
            ),
            Declaration::Workflow(workflow) => push_symbol(
                &mut symbols,
                uri,
                &workflow.name.value,
                format!("workflow {}", workflow.name.value),
                workflow.name.span.into(),
                5,
            ),
            Declaration::PrincipalFact(fact) => typed_symbol(
                &mut symbols,
                uri,
                source,
                "principal",
                &fact.name.value,
                fact.name.span,
                fact.ty.span,
            ),
            Declaration::RowPolicy(policy) => push_symbol(
                &mut symbols,
                uri,
                &policy.name.value,
                format!(
                    "row policy {} for {}",
                    policy.name.value, policy.entity.value
                ),
                policy.name.span.into(),
                12,
            ),
        }
        if symbols.len() >= MAX_COMPLETIONS {
            break;
        }
    }
    symbols.truncate(MAX_COMPLETIONS);
    symbols
}

fn query_symbols(uri: &str, source: &str) -> Vec<EditorSymbol> {
    let Ok(document) = parse_query(source) else {
        return Vec::new();
    };
    query_document_symbols(uri, source, &document)
}

fn query_document_symbols(uri: &str, source: &str, document: &QueryDocument) -> Vec<EditorSymbol> {
    let mut symbols = Vec::new();
    if let Some(name) = &document.name {
        push_symbol(
            &mut symbols,
            uri,
            name.value.as_str(),
            format!("query {}", name.value.as_str()),
            ByteSpan {
                start: name.span.start as usize,
                end: name.span.end as usize,
            },
            12,
        );
    }
    for parameter in &document.parameters {
        let type_text = source_span(
            source,
            ByteSpan {
                start: parameter.ty.span.start as usize,
                end: parameter.ty.span.end as usize,
            },
        );
        push_symbol(
            &mut symbols,
            uri,
            parameter.name.value.as_str(),
            format!("parameter ${}: {type_text}", parameter.name.value.as_str()),
            ByteSpan {
                start: parameter.name.span.start as usize,
                end: parameter.name.span.end as usize,
            },
            13,
        );
    }
    symbols
}

fn typed_symbol(
    symbols: &mut Vec<EditorSymbol>,
    uri: &str,
    source: &str,
    owner: &str,
    name: &str,
    name_span: ContractSpan,
    type_span: ContractSpan,
) {
    let type_text = source_span(source, type_span.into());
    push_symbol(
        symbols,
        uri,
        name,
        format!("{owner}.{name}: {type_text}"),
        name_span.into(),
        8,
    );
}

fn push_symbol(
    symbols: &mut Vec<EditorSymbol>,
    uri: &str,
    name: &str,
    detail: String,
    span: ByteSpan,
    kind: u8,
) {
    if symbols.len() < MAX_COMPLETIONS && name.len() <= 256 && detail.len() <= 1_024 {
        symbols.push(EditorSymbol {
            name: name.to_owned(),
            detail,
            uri: uri.to_owned(),
            span,
            kind,
        });
    }
}

fn source_span(source: &str, span: ByteSpan) -> &str {
    source.get(span.start..span.end).unwrap_or("unknown")
}

fn document_kind(uri: &str) -> DocumentKind {
    if uri.ends_with(".riff") {
        DocumentKind::Contract
    } else if uri.ends_with(".riffq") {
        DocumentKind::Query
    } else {
        DocumentKind::Unsupported
    }
}

fn diagnostic_path(uri: &str) -> String {
    uri.rsplit('/')
        .next()
        .filter(|name| {
            !name.is_empty()
                && name.len() <= 512
                && !name.bytes().any(|byte| byte.is_ascii_control())
        })
        .unwrap_or("document.riff")
        .to_owned()
}

fn diagnostic_json(source: &str, diagnostic: PrimaryDiagnostic) -> Value {
    let byte_span = diagnostic.span;
    json!({
        "range": range_json(source, byte_span),
        "severity": 1,
        "code": diagnostic.code,
        "source": "riffdb",
        "message": diagnostic.message,
        "data": {
            "stage": diagnostic.stage,
            "byteSpan": {"start": byte_span.start, "end": byte_span.end}
        }
    })
}

fn publish_diagnostics(uri: &str, version: Option<i64>, diagnostics: Vec<Value>) -> Value {
    let mut params = serde_json::Map::new();
    params.insert("uri".to_owned(), Value::String(uri.to_owned()));
    params.insert("diagnostics".to_owned(), Value::Array(diagnostics));
    if let Some(version) = version {
        params.insert("version".to_owned(), Value::from(version));
    }
    json!({
        "jsonrpc": "2.0",
        "method": "textDocument/publishDiagnostics",
        "params": params
    })
}

fn range_json(source: &str, span: ByteSpan) -> Value {
    let start = byte_to_utf16_position(source, span.start);
    let end = byte_to_utf16_position(source, span.end);
    json!({
        "start": {"line": start.0, "character": start.1},
        "end": {"line": end.0, "character": end.1}
    })
}

fn byte_to_utf16_position(source: &str, target: usize) -> (usize, usize) {
    let target = target.min(source.len());
    let target = source
        .char_indices()
        .map(|(index, _)| index)
        .take_while(|index| *index <= target)
        .last()
        .unwrap_or(0);
    let prefix = &source[..target];
    let line = prefix.bytes().filter(|byte| *byte == b'\n').count();
    let column_source = prefix.rsplit_once('\n').map_or(prefix, |(_, tail)| tail);
    (line, column_source.encode_utf16().count())
}

fn utf16_position_to_byte(source: &str, line: usize, character: usize) -> Option<usize> {
    let mut line_start = 0usize;
    for _ in 0..line {
        let relative = source.get(line_start..)?.find('\n')?;
        line_start = line_start.checked_add(relative + 1)?;
    }
    let line_source = source
        .get(line_start..)?
        .split_once('\n')
        .map_or_else(|| &source[line_start..], |(head, _)| head);
    let mut utf16 = 0usize;
    for (offset, scalar) in line_source.char_indices() {
        if utf16 == character {
            return Some(line_start + offset);
        }
        utf16 = utf16.checked_add(scalar.len_utf16())?;
        if utf16 > character {
            return None;
        }
    }
    (utf16 == character).then_some(line_start + line_source.len())
}

fn word_span_at(source: &str, offset: usize) -> Option<(usize, usize, &str)> {
    if offset > source.len() || !source.is_char_boundary(offset) {
        return None;
    }
    let bytes = source.as_bytes();
    let mut start = offset;
    let mut end = offset;
    while start > 0 && identifier_byte(bytes[start - 1]) {
        start -= 1;
    }
    while end < bytes.len() && identifier_byte(bytes[end]) {
        end += 1;
    }
    (start < end).then(|| (start, end, &source[start..end]))
}

fn qualified_owner(source: &str, offset: usize) -> Option<&str> {
    let (start, _, _) = word_span_at(source, offset)?;
    if start == 0 || source.as_bytes().get(start - 1) != Some(&b'.') {
        return None;
    }
    let owner_end = start - 1;
    let (_, _, owner) = word_span_at(source, owner_end)?;
    Some(owner)
}

const fn identifier_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn initialize_root(params: &Value) -> Option<PathBuf> {
    params
        .get("rootUri")
        .and_then(Value::as_str)
        .filter(|uri| uri.len() <= MAX_URI_BYTES)
        .and_then(file_uri_path)
        .or_else(|| {
            params
                .get("workspaceFolders")
                .and_then(Value::as_array)
                .filter(|folders| folders.len() == 1)
                .and_then(|folders| folders[0].get("uri"))
                .and_then(Value::as_str)
                .filter(|uri| uri.len() <= MAX_URI_BYTES)
                .and_then(file_uri_path)
        })
}

fn file_uri_path(uri: &str) -> Option<PathBuf> {
    let encoded = uri.strip_prefix("file://")?;
    if !encoded.starts_with('/') {
        return None;
    }
    let mut bytes = Vec::with_capacity(encoded.len());
    let encoded = encoded.as_bytes();
    let mut index = 0usize;
    while index < encoded.len() {
        if encoded[index] == b'%' {
            let high = *encoded.get(index + 1)?;
            let low = *encoded.get(index + 2)?;
            bytes.push((hex(high)? << 4) | hex(low)?);
            index += 3;
        } else {
            bytes.push(encoded[index]);
            index += 1;
        }
    }
    let path = String::from_utf8(bytes).ok()?;
    Some(PathBuf::from(path))
}

fn path_to_file_uri(path: &Path) -> String {
    let source = path.to_string_lossy();
    let mut uri = String::from("file://");
    for byte in source.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'-' | b'_' | b'.' | b'~') {
            uri.push(char::from(byte));
        } else {
            use std::fmt::Write as _;
            let _ = write!(uri, "%{byte:02X}");
        }
    }
    uri
}

const fn hex(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn read_bounded(path: &Path) -> Option<String> {
    let metadata = std::fs::symlink_metadata(path).ok()?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || usize::try_from(metadata.len()).ok()? > MAX_FRAME_BYTES
    {
        return None;
    }
    let bytes = std::fs::read(path).ok()?;
    if bytes.len() > MAX_FRAME_BYTES {
        return None;
    }
    String::from_utf8(bytes).ok()
}

fn checked_workspace_path(root: &Path, relative: &str) -> Option<PathBuf> {
    let relative = Path::new(relative);
    if relative.is_absolute()
        || relative
            .components()
            .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        return None;
    }
    let root = root.canonicalize().ok()?;
    let candidate = root.join(relative).canonicalize().ok()?;
    candidate.starts_with(&root).then_some(candidate)
}

fn checked_string(value: Option<&Value>, maximum: usize) -> Option<&str> {
    value
        .and_then(Value::as_str)
        .filter(|value| value.len() <= maximum)
}

fn request_key(value: &Value) -> Option<String> {
    if let Some(value) = value.as_str() {
        return (value.len() <= MAX_REQUEST_ID_BYTES).then(|| format!("s:{value}"));
    }
    if let Some(value) = value.as_i64() {
        return Some(format!("i:{value}"));
    }
    value.as_u64().map(|value| format!("u:{value}"))
}

fn success_response(id: Value, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

fn error_response(id: Value, code: i32, message: &'static str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}

fn read_frame(reader: &mut impl BufRead) -> Result<Option<Vec<u8>>, ()> {
    let mut content_length = None;
    let mut header_bytes = 0usize;
    loop {
        let mut line = Vec::new();
        let read = reader.read_until(b'\n', &mut line).map_err(|_| ())?;
        if read == 0 {
            return if header_bytes == 0 { Ok(None) } else { Err(()) };
        }
        header_bytes = header_bytes.checked_add(read).ok_or(())?;
        if header_bytes > MAX_HEADER_BYTES {
            return Err(());
        }
        if line == b"\r\n" || line == b"\n" {
            break;
        }
        let line = std::str::from_utf8(&line).map_err(|_| ())?;
        if let Some(value) = line
            .trim_end_matches(['\r', '\n'])
            .strip_prefix("Content-Length:")
        {
            if content_length.is_some() {
                return Err(());
            }
            let length = value.trim().parse::<usize>().map_err(|_| ())?;
            if length == 0 || length > MAX_FRAME_BYTES {
                return Err(());
            }
            content_length = Some(length);
        }
    }
    let length = content_length.ok_or(())?;
    let mut body = vec![0u8; length];
    reader.read_exact(&mut body).map_err(|_| ())?;
    Ok(Some(body))
}

fn write_value(writer: &mut impl io::Write, value: &Value) -> Result<(), ()> {
    let body = serde_json::to_vec(value).map_err(|_| ())?;
    if body.len() > MAX_RESPONSE_BYTES {
        return Err(());
    }
    write!(writer, "Content-Length: {}\r\n\r\n", body.len()).map_err(|_| ())?;
    writer.write_all(&body).map_err(|_| ())?;
    writer.flush().map_err(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = r#"
contract Desk version 1 {
  entity Ticket {
    key (organization_id: uuid, ticket_id: uuid)
    field title: string<200>
    index by_organization (organization_id, ticket_id)
  }
  aggregate Tickets {
    root Ticket
    partition_by organization_id
    conflict_key (organization_id, ticket_id)
  }
  command CreateTicket {
    input request_id: string<128>
    input organization_id: uuid
    input ticket_id: uuid
    input title: string<200>
    idempotency_key request_id
    create Ticket(organization_id, ticket_id) as ticket else AlreadyExists {}
    set ticket.title = title
    return Created { record: ticket }
  }
}
"#;

    #[test]
    fn diagnostics_are_the_compiler_authoring_primary_tuple() {
        let broken = "contract Desk version 1 { entity Ticket { field title: string<200> } }";
        let error = compile_contract_source(broken).expect_err("missing key is invalid");
        let expected = AuthoringDiagnostics::from_contract(
            AuthoringSourcePath::new("contract.riff").expect("path"),
            &error,
        )
        .expect("bounded diagnostics");
        assert_eq!(
            contract_primary_diagnostics("file:///workspace/contract.riff", broken),
            authoring_primary(&expected)
        );
    }

    #[test]
    fn hover_definition_and_completion_use_compiled_local_symbols() {
        let uri = "file:///workspace/contract.riff";
        let mut server = Server::default();
        server.documents.insert(
            uri.to_owned(),
            OpenDocument {
                text: VALID.to_owned(),
                version: 1,
                kind: DocumentKind::Contract,
            },
        );
        let title = VALID.find("title: string").expect("title");
        let position = byte_to_utf16_position(VALID, title + 2);
        let params = json!({
            "textDocument": {"uri": uri},
            "position": {"line": position.0, "character": position.1}
        });
        assert!(
            server.hover(&params)["contents"]["value"]
                .as_str()
                .expect("hover")
                .contains("Ticket.title: string<200>")
        );
        assert_eq!(server.definition(&params)["uri"], uri);
        let completions = server.completion(&params);
        assert!(
            completions
                .as_array()
                .expect("array")
                .iter()
                .any(|item| item["label"] == "CreateTicket")
        );
    }

    #[test]
    fn protocol_bounds_full_sync_and_cancellation_are_closed() {
        let mut server = Server::default();
        for id in 0..(MAX_CANCELLED_REQUESTS * 2) {
            let cancellation = json!({
                "jsonrpc": "2.0",
                "method": "$/cancelRequest",
                "params": {"id": format!("request-{id}")}
            });
            assert!(server.handle(cancellation).0.is_empty());
        }
        assert_eq!(server.cancelled.len(), MAX_CANCELLED_REQUESTS);
        let oversized_id = "x".repeat(MAX_REQUEST_ID_BYTES + 1);
        let cancellation = json!({
            "jsonrpc": "2.0",
            "method": "$/cancelRequest",
            "params": {"id": oversized_id}
        });
        assert!(server.handle(cancellation).0.is_empty());
        assert_eq!(server.cancelled.len(), MAX_CANCELLED_REQUESTS);

        let mut server = Server::default();
        let cancellation = json!({
            "jsonrpc": "2.0",
            "method": "$/cancelRequest",
            "params": {"id": 7}
        });
        assert!(server.handle(cancellation).0.is_empty());
        let request = json!({
            "jsonrpc": "2.0",
            "id": 7,
            "method": "textDocument/completion",
            "params": {}
        });
        assert_eq!(server.handle(request).0[0]["error"]["code"], -32800);

        let oversized = "x".repeat(MAX_FRAME_BYTES + 1);
        let open = json!({
            "jsonrpc": "2.0",
            "method": "textDocument/didOpen",
            "params": {"textDocument": {
                "uri": "file:///workspace/contract.riff",
                "version": 1,
                "text": oversized
            }}
        });
        let responses = server.handle(open).0;
        assert_eq!(server.documents.len(), 0);
        assert_eq!(responses[0]["params"]["diagnostics"], json!([]));

        let shutdown = json!({"jsonrpc": "2.0", "id": 8, "method": "shutdown"});
        assert_eq!(server.handle(shutdown).0[0]["result"], Value::Null);
        let after_shutdown = json!({
            "jsonrpc": "2.0",
            "id": 9,
            "method": "textDocument/completion",
            "params": {}
        });
        assert_eq!(server.handle(after_shutdown).0[0]["error"]["code"], -32600);
        let exit = json!({"jsonrpc": "2.0", "method": "exit"});
        assert!(server.handle(exit).1);
    }

    #[test]
    fn framing_accepts_one_bounded_lsp_message() {
        let body = br#"{"jsonrpc":"2.0","id":1,"method":"shutdown"}"#;
        let frame = format!("Content-Length: {}\r\n\r\n", body.len());
        let mut input = frame.into_bytes();
        input.extend_from_slice(body);
        let mut reader = BufReader::new(input.as_slice());
        assert_eq!(read_frame(&mut reader).expect("frame"), Some(body.to_vec()));
        assert_eq!(read_frame(&mut reader).expect("eof"), None);

        let duplicate = b"Content-Length: 2\r\nContent-Length: 2\r\n\r\n{}";
        assert_eq!(read_frame(&mut BufReader::new(&duplicate[..])), Err(()));
    }

    #[test]
    fn utf16_positions_do_not_split_unicode_scalars() {
        let source = "a😀b\nvalue";
        assert_eq!(utf16_position_to_byte(source, 0, 3), Some(5));
        assert_eq!(utf16_position_to_byte(source, 0, 2), None);
        assert_eq!(byte_to_utf16_position(source, 5), (0, 3));
        assert_eq!(file_uri_path("file://remote/workspace/contract.riff"), None);
    }

    #[test]
    fn query_syntax_diagnostics_are_stable_and_bounded() {
        let server = Server::default();
        let diagnostics = server.query_diagnostics(
            "file:///workspace/list.riffq",
            "query List( { from Ticket }",
        );
        assert!(!diagnostics.is_empty());
        assert!(diagnostics.len() <= 32);
        assert!(diagnostics[0].code.starts_with("RDB-QS"));
    }

    #[test]
    fn bounded_limit_query_is_understood_by_lsp_compilation() {
        let mut server = Server::default();
        server.documents.insert(
            "file:///workspace/contract.riff".to_owned(),
            OpenDocument {
                text: VALID.to_owned(),
                version: 1,
                kind: DocumentKind::Contract,
            },
        );
        let source = r#"
query TicketPage(
  $organization_id: Ticket.organization_id,
  $limit: Limit<10> = 5,
) {
  many tickets from Ticket
    where organization_id == $organization_id
    order by ticket_id asc
    take $limit
  return Found { tickets: tickets { ticket_id title } }
  outcomes Found
}
"#;
        let diagnostics = server.query_diagnostics("file:///workspace/ticket_page.riffq", source);
        assert!(diagnostics.is_empty(), "{diagnostics:?}");
    }

    #[test]
    fn query_semantic_diagnostics_use_the_query_module_compiler() {
        let mut server = Server::default();
        server.documents.insert(
            "file:///workspace/contract.riff".to_owned(),
            OpenDocument {
                text: VALID.to_owned(),
                version: 1,
                kind: DocumentKind::Contract,
            },
        );
        let source = r#"
query Broken($organization_id: Ticket.organization_id) {
  many tickets from Ticket
    where organization_id == $organization_id
    order by ticket_id asc
    take 10
  return Found { tickets: tickets { missing_field } }
  outcomes Found
}
"#;
        let diagnostics = server.query_diagnostics("file:///workspace/broken.riffq", source);
        assert!(!diagnostics.is_empty());
        assert!(diagnostics.iter().all(|diagnostic| {
            diagnostic.code.starts_with("RDB-QP") && diagnostic.stage == "query_plan"
        }));
    }

    #[test]
    fn ambiguous_open_contracts_never_supply_query_semantics() {
        let mut server = Server::default();
        for name in ["one", "two"] {
            server.documents.insert(
                format!("file:///workspace/{name}.riff"),
                OpenDocument {
                    text: VALID.to_owned(),
                    version: 1,
                    kind: DocumentKind::Contract,
                },
            );
        }
        assert!(
            server
                .local_contract("file:///workspace/query.riffq")
                .is_none()
        );
    }

    #[test]
    fn type_expression_import_remains_linked_to_authoritative_ast() {
        let value = riffdb_contract_syntax::ast::TypeExpression::Bool;
        assert!(matches!(
            value,
            riffdb_contract_syntax::ast::TypeExpression::Bool
        ));
    }

    #[test]
    fn packaged_highlight_snapshot_corpus_is_bounded() {
        let snapshot: Value =
            serde_json::from_str(include_str!("../assets/editor/highlights-v1.json"))
                .expect("generated snapshot JSON");
        assert_eq!(snapshot["schema"], "riffdb.editor-highlight-snapshots/v1");
        let examples = snapshot["examples"].as_array().expect("examples");
        assert!(!examples.is_empty() && examples.len() <= 256);
        for example in examples {
            assert!(example["bytes"].as_u64().expect("bytes") <= MAX_FRAME_BYTES as u64);
            assert!(example["captures"].as_array().expect("captures").len() <= 131_072);
        }
    }
}
