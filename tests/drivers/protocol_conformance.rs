#![allow(dead_code)]

use std::collections::BTreeMap;

use riffdb_client_rust::ApplicationValue;
use serde::Deserialize;
use serde_json::{Value, json};

pub(super) const CORPUS_SOURCE: &str = include_str!("protocol-conformance-v1.json");

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Corpus {
    pub schema: String,
    pub authoritative_rule: String,
    pub entries: Vec<CorpusEntry>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CorpusEntry {
    pub name: String,
    pub driver_value: Option<Value>,
    pub python_value: Option<Value>,
    pub generator: Option<Generator>,
    pub expected: ExpectedObservation,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Generator {
    String { size: usize },
    NullList { size: usize },
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ExpectedObservation {
    pub disposition: String,
    pub graph: Option<Value>,
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct Observation {
    pub disposition: &'static str,
    pub graph: Option<Value>,
    pub request_material: Option<Vec<u8>>,
}

pub(super) fn corpus() -> Corpus {
    let corpus: Corpus = serde_json::from_str(CORPUS_SOURCE).expect("valid driver corpus");
    assert_eq!(
        corpus.schema, "riffdb.driver-protocol-conformance/v1",
        "unexpected corpus schema"
    );
    assert_eq!(
        corpus.authoritative_rule, "riffdb-driver-host-v3",
        "the existing driver-host rule is authoritative"
    );
    corpus
}

pub(super) fn driver_value(entry: &CorpusEntry) -> Value {
    match &entry.generator {
        None => entry.driver_value.clone().expect("driver value"),
        Some(Generator::String { size }) => {
            json!({"type": "string", "value": "x".repeat(*size)})
        }
        Some(Generator::NullList { size }) => {
            json!({"type": "list", "value": vec![json!({"type": "null"}); *size]})
        }
    }
}

pub(super) fn python_value(entry: &CorpusEntry) -> Value {
    match &entry.generator {
        None => entry.python_value.clone().expect("Python value"),
        Some(Generator::String { size }) => {
            json!({"kind": "string", "value": "x".repeat(*size)})
        }
        Some(Generator::NullList { size }) => {
            json!({"kind": "list", "value": vec![json!({"kind": "null"}); *size]})
        }
    }
}

pub(super) fn accepted(value: ApplicationValue) -> Observation {
    let graph = application_value_graph(value);
    let request_material = serde_json::to_vec(&graph).expect("canonical observation JSON");
    Observation {
        disposition: "accepted",
        graph: Some(graph),
        request_material: Some(request_material),
    }
}

pub(super) const fn invalid_input() -> Observation {
    Observation {
        disposition: "invalid_input",
        graph: None,
        request_material: None,
    }
}

pub(super) fn assert_observation(entry: &CorpusEntry, actual: Observation) {
    assert_eq!(
        actual.disposition, entry.expected.disposition,
        "{} disagrees with the authoritative driver-host disposition",
        entry.name
    );
    assert_eq!(
        actual.graph, entry.expected.graph,
        "{} produced a different ApplicationValue graph",
        entry.name
    );
    let expected_material = entry
        .expected
        .graph
        .as_ref()
        .map(|graph| serde_json::to_vec(graph).expect("expected request material"));
    assert_eq!(
        actual.request_material, expected_material,
        "{} produced different canonical request material",
        entry.name
    );
}

fn application_value_graph(value: ApplicationValue) -> Value {
    match value {
        ApplicationValue::Null => json!({"type": "null"}),
        ApplicationValue::Bool(value) => json!({"type": "bool", "value": value}),
        ApplicationValue::I64(value) => json!({"type": "i64", "value": value.to_string()}),
        ApplicationValue::U64(value) => json!({"type": "u64", "value": value.to_string()}),
        ApplicationValue::Decimal {
            coefficient_twos_complement,
            scale,
            precision,
        } => json!({
            "type": "decimal",
            "coefficient": hex(&coefficient_twos_complement),
            "scale": scale,
            "precision": precision,
        }),
        ApplicationValue::Money { currency, amount } => json!({
            "type": "money",
            "currency": currency,
            "amount": application_value_graph(*amount),
        }),
        ApplicationValue::String(value) => json!({"type": "string", "value": value}),
        ApplicationValue::Uuid(value) => json!({"type": "uuid", "value": value.as_str()}),
        ApplicationValue::Enum(value) => json!({"type": "enum", "value": value}),
        ApplicationValue::EnumIdentity {
            type_id,
            variant_id,
            name,
        } => json!({
            "type": "enum_identity",
            "type_id": type_id,
            "variant_id": variant_id,
            "value": name,
        }),
        ApplicationValue::Bytes(value) => json!({"type": "bytes", "value": hex(&value)}),
        ApplicationValue::Date(value) => json!({"type": "date", "value": value.to_string()}),
        ApplicationValue::Timestamp { seconds, nanos } => json!({
            "type": "timestamp",
            "seconds": seconds.to_string(),
            "nanos": nanos,
        }),
        ApplicationValue::Vector(value) => json!({
            "type": "vector",
            "component_bits": value
                .components()
                .iter()
                .map(|component| component.to_bits())
                .collect::<Vec<_>>(),
        }),
        ApplicationValue::List(values) => json!({
            "type": "list",
            "value": values
                .into_iter()
                .map(application_value_graph)
                .collect::<Vec<_>>(),
        }),
        ApplicationValue::Record(values) => json!({
            "type": "record",
            "value": values
                .into_iter()
                .map(|(name, value)| (name, application_value_graph(value)))
                .collect::<BTreeMap<_, _>>(),
        }),
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(output, "{byte:02x}").expect("String writes cannot fail");
    }
    output
}
