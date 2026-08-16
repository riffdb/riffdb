//! Exact reactive-module compilation over immutable contract and query inputs.

use std::collections::{BTreeMap, BTreeSet};

use riffdb_contract_ir::ContractBundle;
use riffdb_query_compiler::{
    ReactiveCompileDiagnostic, ReactiveQueryCatalogEntry, compile_reactive_module,
};
use riffdb_query_ir::{MAX_REACTIVE_MODULE_BYTES, NamedTypeSchema, ReactiveModulePlanV1};
use riffdb_query_syntax::{Diagnostic, format_module, parse_module};
use riffdb_types::{CanonicalValue, decode_canonical_value, encode_canonical_value};

use crate::QueryModule;

/// Value-free reactive-module compilation failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReactiveModuleCompilationError {
    /// Bounded grammar diagnostics.
    Syntax(Vec<Diagnostic>),
    /// Exact-contract resolution, type, locality, or cost diagnostics.
    Semantic(Vec<ReactiveCompileDiagnostic>),
    /// Query names are ambiguous or a catalog value cannot be represented.
    InvalidQueryCatalog,
    /// Canonical artifact bytes are empty or exceed the module ceiling.
    ArtifactLimit,
    /// Recompilation did not reproduce the supplied artifact bytes.
    IdentityMismatch,
}

/// A compiler-owned reactive argument source could not be bound exactly.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReactiveArgumentBindingError;

/// Binds canonical compiler argument sources for a stream or hydration.
pub fn bind_reactive_arguments(
    arguments: &[(String, String)],
    parameters: &BTreeMap<String, CanonicalValue>,
    event_fields: &BTreeMap<String, CanonicalValue>,
) -> Result<BTreeMap<String, CanonicalValue>, ReactiveArgumentBindingError> {
    let mut bound = BTreeMap::new();
    for (target, source) in arguments {
        let value = if let Some(name) = source.strip_prefix('$') {
            parameters.get(name).cloned()
        } else if let Some(name) = source.strip_prefix("event.") {
            event_fields.get(name).cloned()
        } else if let Some((_, encoded)) = source
            .strip_prefix("literal:")
            .and_then(|value| value.rsplit_once(':'))
        {
            let bytes = decode_hex(encoded)?;
            let value = decode_canonical_value(&bytes).map_err(|_| ReactiveArgumentBindingError)?;
            let canonical =
                encode_canonical_value(&value).map_err(|_| ReactiveArgumentBindingError)?;
            (canonical.as_slice() == bytes).then_some(value)
        } else {
            None
        }
        .ok_or(ReactiveArgumentBindingError)?;
        if bound.insert(target.clone(), value).is_some() {
            return Err(ReactiveArgumentBindingError);
        }
    }
    Ok(bound)
}

fn decode_hex(value: &str) -> Result<Vec<u8>, ReactiveArgumentBindingError> {
    if !value.len().is_multiple_of(2) || value.len() > 8 * 1_024 * 1_024 {
        return Err(ReactiveArgumentBindingError);
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_nibble(pair[0]).ok_or(ReactiveArgumentBindingError)?;
            let low = hex_nibble(pair[1]).ok_or(ReactiveArgumentBindingError)?;
            Ok((high << 4) | low)
        })
        .collect()
}

const fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

/// Builds the exact query facts available to reactive compilation.
pub fn reactive_query_catalog(
    contract: &ContractBundle,
    modules: &[QueryModule],
) -> Result<Vec<ReactiveQueryCatalogEntry>, ReactiveModuleCompilationError> {
    let mut names = BTreeSet::new();
    let mut entries = Vec::new();
    for module in modules {
        if module.contract_hash() != contract.bundle_hash() {
            return Err(ReactiveModuleCompilationError::InvalidQueryCatalog);
        }
        for query in module.queries() {
            // ADR-0128 excludes secret-output queries from every reactive
            // watch and hydration surface. Omitting the dependency makes a
            // source reference fail closed at its query-name span.
            if !query.plan().secret_outputs().is_empty() {
                continue;
            }
            // Operational families require per-delivery presence selection.
            // Reactive V1 has no such identity, so it must not bind one by
            // accidentally treating a representative member as executable.
            if query.operational_family().is_some() {
                continue;
            }
            if !names.insert(query.name()) {
                return Err(ReactiveModuleCompilationError::InvalidQueryCatalog);
            }
            let mut parameters = query
                .plan()
                .schemas()
                .parameters()
                .iter()
                .map(|parameter| {
                    canonical_type(parameter.value_type())
                        .map(|ty| (parameter.name().to_owned(), ty))
                })
                .collect::<Option<Vec<_>>>()
                .ok_or(ReactiveModuleCompilationError::InvalidQueryCatalog)?;
            parameters.sort_by(|left, right| left.0.cmp(&right.0));
            entries.push(
                ReactiveQueryCatalogEntry::checked(
                    module.identity(),
                    query.name().to_owned(),
                    query.plan().identity(),
                    parameters,
                    query.plan().cost(),
                    patch_key(contract, query),
                )
                .ok_or(ReactiveModuleCompilationError::InvalidQueryCatalog)?,
            );
        }
    }
    entries.sort_by(|left, right| left.query_name().cmp(right.query_name()));
    Ok(entries)
}

/// Parses, canonicalizes, resolves, proves, and hashes one `.riffr` source.
pub fn compile_reactive_source(
    source: &str,
    contract: &ContractBundle,
    modules: &[QueryModule],
) -> Result<ReactiveModulePlanV1, ReactiveModuleCompilationError> {
    let canonical = canonicalize_reactive_source(source)?;
    let document = parse_module(&canonical).map_err(ReactiveModuleCompilationError::Syntax)?;
    let catalog = reactive_query_catalog(contract, modules)?;
    compile_reactive_module(&document, contract, &catalog)
        .map_err(ReactiveModuleCompilationError::Semantic)
}

/// Parses and deterministically formats one `.riffr` source document.
pub fn canonicalize_reactive_source(
    source: &str,
) -> Result<String, ReactiveModuleCompilationError> {
    let document = parse_module(source).map_err(ReactiveModuleCompilationError::Syntax)?;
    Ok(format_module(&document))
}

/// Returns the exact sorted query-module identities used by one compiled module.
#[must_use]
pub fn reactive_module_query_dependencies(
    module: &ReactiveModulePlanV1,
) -> Vec<riffdb_types::QueryModuleHash> {
    use riffdb_query_ir::ReactiveOperationPlanV1;

    let mut dependencies = BTreeSet::new();
    for operation in module.operations() {
        match operation.plan() {
            ReactiveOperationPlanV1::Watch { query, .. } => {
                dependencies.insert(query.module_hash());
            }
            ReactiveOperationPlanV1::Subscription { hydrations, .. } => {
                dependencies.extend(hydrations.iter().map(|query| query.module_hash()));
            }
            ReactiveOperationPlanV1::Stream { .. } => {}
        }
    }
    dependencies.into_iter().collect()
}

/// Strictly recompiles one source and byte-compares its canonical artifact.
pub fn decode_and_validate_reactive_module(
    bytes: &[u8],
    source: &str,
    contract: &ContractBundle,
    modules: &[QueryModule],
) -> Result<ReactiveModulePlanV1, ReactiveModuleCompilationError> {
    if bytes.is_empty() || bytes.len() > MAX_REACTIVE_MODULE_BYTES {
        return Err(ReactiveModuleCompilationError::ArtifactLimit);
    }
    let module = compile_reactive_source(source, contract, modules)?;
    if module.canonical_bytes() != bytes {
        return Err(ReactiveModuleCompilationError::IdentityMismatch);
    }
    Ok(module)
}

fn canonical_type(value: &NamedTypeSchema) -> Option<String> {
    match value {
        NamedTypeSchema::Scalar(name) => Some(name.clone()),
        NamedTypeSchema::Optional(inner) => Some(format!("{}?", canonical_type(inner)?)),
        NamedTypeSchema::Set(inner) => Some(format!("Set<{}>", canonical_type(inner)?)),
        NamedTypeSchema::Cursor => Some("Cursor".to_owned()),
        NamedTypeSchema::Limit => Some("Limit".to_owned()),
        NamedTypeSchema::Record(_) | NamedTypeSchema::List { .. } => None,
    }
}

fn patch_key(contract: &ContractBundle, query: &crate::CompiledNamedQuery) -> Vec<String> {
    let steps = query
        .plan()
        .representative_program()
        .steps()
        .iter()
        .filter(|step| !step.result_names().is_empty())
        .collect::<Vec<_>>();
    let [step] = steps.as_slice() else {
        return Vec::new();
    };
    let Some(entity) = contract
        .schema()
        .entities()
        .iter()
        .find(|entity| entity.id() == step.internal_entity_id())
    else {
        return Vec::new();
    };
    let mut fields = entity
        .primary_key_fields()
        .iter()
        .filter_map(|field| {
            entity
                .record()
                .field(*field)
                .map(|field| field.name().to_owned())
        })
        .collect::<Vec<_>>();
    if fields
        .iter()
        .any(|field| !step.selected_fields().contains(field))
    {
        return Vec::new();
    }
    fields.sort();
    fields
}
