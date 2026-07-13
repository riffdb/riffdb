#![forbid(unsafe_code)]

//! Pure-Rust, deterministic Protobuf artifact generator.

use std::env;
use std::error::Error;
use std::fmt::Write as _;
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};

use prost::Message;
use prost_types::{DescriptorProto, FileDescriptorSet};
use protox::Compiler;
use riffdb_errors::{
    PublicError, ValidationCode, ValidationIssue, ValidationIssues, ValidationPath,
    ValidationPathSegment,
};
use riffdb_proto::{
    canonical_value_to_proto,
    envelope::{PayloadValidationError, RecordSchema, encode},
    public_error_to_proto, v1,
};
use riffdb_types::{
    CanonicalValue, ContractVersion, CurrencyCode, Date, Decimal, DecimalSpec, EnumTypeId,
    EnumVariantId, FieldId, IncidentId, Money, Timestamp,
};

const PRODUCTION_SOURCES: &[&str] = &[
    "riffdb/storage/v1/envelope.proto",
    "riffdb/v1/command.proto",
    "riffdb/v1/error.proto",
    "riffdb/v1/services.proto",
    "riffdb/v1/value.proto",
];
const PROBE_SOURCE: &str = "compatibility_probe.proto";
const PROBE_RECORD_TYPE: &str = "riffdb.testing.v1.CompatibilityProbe";
const PROBE_PAYLOAD: &[u8] = &[0x08, 0x2a];
const EXPECTED_METHODS: &[(&str, &str, bool)] = &[
    ("AdminService", "CreateCapability", false),
    ("AdminService", "Health", false),
    ("AdminService", "RevokeCapability", false),
    ("AdminService", "Stats", false),
    ("CommandService", "Execute", false),
    ("CommandService", "GetOutcome", false),
    ("CommitService", "GetCommit", false),
    ("CommitService", "ScanCommits", false),
    ("CommitService", "SubscribeCommits", true),
    ("ContractService", "DeployContract", false),
    ("ContractService", "ExplainCommand", false),
    ("ContractService", "GetActiveContract", false),
    ("ContractService", "ValidateContract", false),
    ("QueryService", "GetEntity", false),
    ("QueryService", "QueryProjection", false),
    ("QueryService", "ScanIndex", false),
];

fn main() -> Result<(), Box<dyn Error>> {
    let output_root = parse_output_root()?;
    let repository_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| io::Error::other("riffdb-proto is not inside the workspace"))?;

    let production = compile_descriptors(&repository_root.join("proto"), PRODUCTION_SOURCES)?;
    validate_service_inventory(&production)?;
    let probe = compile_descriptors(&repository_root.join("fixtures/proto"), &[PROBE_SOURCE])?;
    let probe_descriptor = probe.encode_to_vec();
    let probe_schema = RecordSchema::new(
        PROBE_RECORD_TYPE,
        &probe_descriptor,
        PROBE_PAYLOAD.len(),
        validate_probe_payload,
    )?;
    let probe_envelope = encode(&probe_schema, PROBE_PAYLOAD)?;

    generate_rust(&output_root, production.clone())?;
    write_artifact(
        &output_root,
        "fixtures/proto/descriptors/riffdb-v1-descriptor-set.bin",
        &production.encode_to_vec(),
    )?;
    write_artifact(
        &output_root,
        "fixtures/proto/descriptors/compatibility-probe-descriptor-set.bin",
        &probe_descriptor,
    )?;
    write_artifact(
        &output_root,
        "fixtures/proto/compatibility-probe-payload.bin",
        PROBE_PAYLOAD,
    )?;
    write_artifact(
        &output_root,
        "fixtures/proto/compatibility-probe-envelope.bin",
        &probe_envelope,
    )?;
    write_artifact(
        &output_root,
        "fixtures/proto/schema-inventory.txt",
        inventory(&production).as_bytes(),
    )?;
    write_artifact(
        &output_root,
        "fixtures/proto/wire-vectors.txt",
        wire_vectors()?.as_bytes(),
    )?;

    Ok(())
}

fn validate_probe_payload(payload: &[u8]) -> Result<(), PayloadValidationError> {
    if payload == PROBE_PAYLOAD {
        Ok(())
    } else {
        Err(PayloadValidationError::Malformed)
    }
}

fn parse_output_root() -> Result<PathBuf, Box<dyn Error>> {
    let mut args = env::args_os().skip(1);
    let flag = args
        .next()
        .ok_or_else(|| io::Error::other("expected --output-root <path>"))?;
    if flag != "--output-root" {
        return Err(io::Error::other("expected --output-root <path>").into());
    }
    let output_root = args
        .next()
        .ok_or_else(|| io::Error::other("missing output root"))?;
    if args.next().is_some() {
        return Err(io::Error::other("unexpected generator argument").into());
    }
    Ok(output_root.into())
}

fn compile_descriptors(
    include: &Path,
    source_names: &[&str],
) -> Result<FileDescriptorSet, Box<dyn Error>> {
    let mut compiler = Compiler::new([include])?;
    compiler
        .include_imports(true)
        .include_source_info(false)
        .open_files(source_names)?;

    let mut descriptor_set = compiler.file_descriptor_set();
    for file in &mut descriptor_set.file {
        file.source_code_info = None;
        validate_file_name(file.name())?;
    }
    descriptor_set
        .file
        .sort_by(|left, right| left.name().cmp(right.name()));
    Ok(descriptor_set)
}

fn validate_file_name(name: &str) -> Result<(), Box<dyn Error>> {
    let path = Path::new(name);
    let normalized = !name.contains('\\')
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)));
    if !normalized {
        return Err(
            io::Error::other(format!("descriptor file name is not normalized: {name}")).into(),
        );
    }
    Ok(())
}

fn validate_service_inventory(descriptor_set: &FileDescriptorSet) -> Result<(), Box<dyn Error>> {
    if descriptor_set
        .file
        .iter()
        .any(|file| !file.service.is_empty() && file.package() != "riffdb.v1")
    {
        return Err(io::Error::other("public services must remain in riffdb.v1").into());
    }

    let mut actual = descriptor_set
        .file
        .iter()
        .flat_map(|file| {
            file.service.iter().flat_map(|service| {
                service.method.iter().map(|method| {
                    (
                        service.name().to_owned(),
                        method.name().to_owned(),
                        method.input_type().to_owned(),
                        method.output_type().to_owned(),
                        method.server_streaming(),
                        method.client_streaming(),
                    )
                })
            })
        })
        .collect::<Vec<_>>();
    actual.sort();

    let expected = EXPECTED_METHODS
        .iter()
        .map(|(service, method, server_streaming)| {
            let input_type = if *method == "Execute" {
                ".riffdb.v1.ExecuteCommandRequest".to_owned()
            } else {
                format!(".riffdb.v1.{method}Request")
            };
            let output_type = match *method {
                "Execute" => ".riffdb.v1.ExecuteCommandResponse".to_owned(),
                "SubscribeCommits" => ".riffdb.v1.CommitNotification".to_owned(),
                _ => format!(".riffdb.v1.{method}Response"),
            };
            (
                (*service).to_owned(),
                (*method).to_owned(),
                input_type,
                output_type,
                *server_streaming,
                false,
            )
        })
        .collect::<Vec<_>>();

    if actual != expected {
        return Err(io::Error::other(
            "service inventory differs from the accepted five-service, sixteen-RPC baseline",
        )
        .into());
    }
    Ok(())
}

fn generate_rust(
    output_root: &Path,
    descriptor_set: FileDescriptorSet,
) -> Result<(), Box<dyn Error>> {
    let generated = output_root.join("crates/riffdb-proto/src/generated");
    fs::create_dir_all(&generated)?;
    prost_build::Config::new()
        .out_dir(generated)
        .format(true)
        .compile_fds(descriptor_set)?;
    Ok(())
}

fn write_artifact(
    output_root: &Path,
    relative_path: &str,
    bytes: &[u8],
) -> Result<(), Box<dyn Error>> {
    let path = output_root.join(relative_path);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, bytes)?;
    Ok(())
}

fn inventory(descriptor_set: &FileDescriptorSet) -> String {
    let mut output = String::from("riffdb-proto-schema-inventory-v1\n\nfiles\n");
    for file in &descriptor_set.file {
        let _ = writeln!(output, "  {}", file.name());
    }

    output.push_str("\nmessages\n");
    let mut messages = Vec::new();
    for file in &descriptor_set.file {
        collect_messages(file.package(), &file.message_type, &mut messages);
    }
    messages.sort();
    for (name, field_count) in messages {
        let phase = if field_count == 0 {
            " phase-zero-shell"
        } else {
            ""
        };
        let _ = writeln!(output, "  {name} fields={field_count}{phase}");
    }

    output.push_str("\nservices\n");
    let mut services = descriptor_set
        .file
        .iter()
        .flat_map(|file| {
            file.service
                .iter()
                .map(move |service| (format!("{}.{}", file.package(), service.name()), service))
        })
        .collect::<Vec<_>>();
    services.sort_by(|left, right| left.0.cmp(&right.0));
    for (service_name, service) in services {
        let _ = writeln!(output, "  {service_name}");
        for method in &service.method {
            let streaming = match (method.client_streaming(), method.server_streaming()) {
                (false, false) => "unary",
                (false, true) => "server-streaming",
                (true, false) => "client-streaming",
                (true, true) => "bidirectional-streaming",
            };
            let _ = writeln!(
                output,
                "    {} {} -> {} ({streaming})",
                method.name(),
                method.input_type(),
                method.output_type()
            );
        }
    }
    output
}

fn wire_vectors() -> Result<String, Box<dyn Error>> {
    let decimal_spec = DecimalSpec::new(6, 2)?;
    let decimal = Decimal::new(decimal_spec, -129)?;
    let money = Money::new(CurrencyCode::new("USD")?, Decimal::new(decimal_spec, 1234)?);
    let values = [
        ("value.null", CanonicalValue::Null),
        ("value.bool-false", CanonicalValue::Bool(false)),
        ("value.i64-minus-one", CanonicalValue::I64(-1)),
        ("value.u64-max", CanonicalValue::U64(u64::MAX)),
        ("value.decimal", CanonicalValue::Decimal(decimal)),
        ("value.money", CanonicalValue::Money(money)),
        ("value.string", CanonicalValue::string("riff")?),
        ("value.bytes", CanonicalValue::bytes([0, 0xff])?),
        ("value.uuid", CanonicalValue::Uuid([0x11; 16])),
        ("value.date", CanonicalValue::Date(Date::new(-1))),
        (
            "value.timestamp",
            CanonicalValue::Timestamp(Timestamp::new(-1, 999_999_999)?),
        ),
        (
            "value.enum",
            CanonicalValue::Enum {
                type_id: EnumTypeId::new(7),
                variant_id: EnumVariantId::new(11),
            },
        ),
        (
            "value.list",
            CanonicalValue::list(vec![CanonicalValue::Null, CanonicalValue::Bool(true)])?,
        ),
        (
            "value.record",
            CanonicalValue::record(vec![
                (FieldId::new(9), CanonicalValue::I64(2)),
                (FieldId::new(3), CanonicalValue::I64(1)),
            ])?,
        ),
    ];

    let mut output = String::from("riffdb-proto-wire-vectors-v1\n");
    for (name, value) in values {
        append_wire_vector(&mut output, name, &canonical_value_to_proto(&value)?);
    }

    let request_id = [
        0x01, 0x9b, 0xf6, 0xaa, 0xa6, 0x40, 0x7d, 0xe6, 0x89, 0xc9, 0x8a, 0x7f, 0x70, 0xbb, 0xbd,
        0x23,
    ];
    let request = v1::ExecuteCommandRequest {
        request_id: request_id.to_vec(),
        command_name: "budget.reserve".to_owned(),
        expected_contract_version: None,
        input: Some(canonical_value_to_proto(&CanonicalValue::Null)?),
    };
    append_wire_vector(&mut output, "execute.request-active", &request);
    append_wire_vector(
        &mut output,
        "execute.request-versioned",
        &v1::ExecuteCommandRequest {
            expected_contract_version: Some(7),
            ..request
        },
    );
    append_wire_vector(
        &mut output,
        "execute.response-committed",
        &v1::ExecuteCommandResponse {
            status: v1::execute_command_response::CompletionStatus::Committed as i32,
            commit_sequence: 8,
            contract_version: 7,
            plan_hash: vec![0x22; 32],
            outcome_type: "Reserved".to_owned(),
            outcome: Some(canonical_value_to_proto(&CanonicalValue::Bool(true))?),
            provenance_uri: "riffdb://provenance/019bf6aa-a640-7de6-89c9-8a7f70bbbd23".to_owned(),
            durability_mode: "sync".to_owned(),
        },
    );

    let incident_id = IncidentId::from_bytes(request_id)?;
    let path = ValidationPath::new(vec![
        ValidationPathSegment::Field(FieldId::new(3)),
        ValidationPathSegment::ListIndex(1),
    ])?;
    let errors = [
        (
            "error.validation",
            PublicError::validation(ValidationIssues::one(ValidationIssue::new(
                ValidationCode::InvalidValue,
                path,
            ))),
        ),
        ("error.idempotency", PublicError::idempotency_key_reuse()),
        ("error.authorization", PublicError::authorization_denied()),
        (
            "error.concurrency",
            PublicError::concurrency_deadline_exceeded(),
        ),
        (
            "error.contract",
            PublicError::contract_mismatch(ContractVersion::new(7)),
        ),
        ("error.storage", PublicError::storage_unavailable()),
        ("error.outcome-unknown", PublicError::outcome_unknown()),
        ("error.internal", PublicError::internal_defect(incident_id)),
    ];
    for (name, error) in errors {
        append_wire_vector(&mut output, name, &public_error_to_proto(&error));
    }
    Ok(output)
}

fn append_wire_vector(output: &mut String, name: &str, message: &impl Message) {
    let _ = write!(output, "{name} ");
    for byte in message.encode_to_vec() {
        let _ = write!(output, "{byte:02x}");
    }
    output.push('\n');
}

fn collect_messages(prefix: &str, messages: &[DescriptorProto], output: &mut Vec<(String, usize)>) {
    for message in messages {
        let full_name = if prefix.is_empty() {
            message.name().to_owned()
        } else {
            format!("{prefix}.{}", message.name())
        };
        output.push((full_name.clone(), message.field.len()));
        collect_messages(&full_name, &message.nested_type, output);
    }
}
