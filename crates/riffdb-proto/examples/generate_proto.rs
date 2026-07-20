#![forbid(unsafe_code)]

//! Pure-Rust, deterministic Protobuf artifact generator.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::error::Error;
use std::fmt::Write as _;
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};

use crc::{CRC_32_ISCSI, Crc};
use prost::Message;
use prost_types::{DescriptorProto, FileDescriptorSet};
use protox::Compiler;
use riffdb_errors::{
    PublicError, ValidationCode, ValidationIssue, ValidationIssues, ValidationPath,
    ValidationPathSegment,
};
use riffdb_proto::{
    canonical_value_to_proto,
    envelope::{
        MAX_STORED_ENVELOPE_BYTES, STORAGE_FORMAT_VERSION_V1, maximum_encoded_envelope_bytes_for,
    },
    public_error_to_proto,
    storage::v1::StoredEnvelope,
    v1,
};
use riffdb_types::{
    CanonicalValue, ContractVersion, CurrencyCode, Date, Decimal, DecimalSpec, EnumTypeId,
    EnumVariantId, ExecutionFailureCode, FieldId, IncidentId, Money, Timestamp, hash_schema,
};

const STORAGE_SOURCES: &[&str] = &[
    "riffdb/storage/v1/application.proto",
    "riffdb/storage/v1/audit.proto",
    "riffdb/storage/v1/capability.proto",
    "riffdb/storage/v1/catalog.proto",
    "riffdb/storage/v1/common.proto",
    "riffdb/storage/v1/envelope.proto",
    "riffdb/storage/v1/metadata.proto",
    "riffdb/storage/v1/outbox.proto",
    "riffdb/storage/v1/projection.proto",
];
const PRODUCTION_SOURCES: &[&str] = &[
    "riffdb/storage/v1/application.proto",
    "riffdb/storage/v1/audit.proto",
    "riffdb/storage/v1/capability.proto",
    "riffdb/storage/v1/catalog.proto",
    "riffdb/storage/v1/common.proto",
    "riffdb/storage/v1/envelope.proto",
    "riffdb/storage/v1/metadata.proto",
    "riffdb/storage/v1/outbox.proto",
    "riffdb/storage/v1/projection.proto",
    "riffdb/v1/command.proto",
    "riffdb/v1/error.proto",
    "riffdb/v1/services.proto",
    "riffdb/v1/value.proto",
];

const TINY_PAYLOAD_BOUND: usize = 8 * 1024;
const ADMISSION_PAYLOAD_BOUND: usize = 128 * 1024;
const DOCUMENT_PAYLOAD_BOUND: usize = 2 * 1024 * 1024;

#[derive(Clone, Copy)]
enum PayloadBound {
    Tiny,
    Admission,
    Document,
    EnvelopeMaximum,
}

#[derive(Clone, Copy)]
struct DurableRecord {
    source: &'static str,
    name: &'static str,
    payload_bound: PayloadBound,
}

const DURABLE_RECORDS: &[DurableRecord] = &[
    durable(
        "metadata.proto",
        "StoredStorageFormatVersionV1",
        PayloadBound::Tiny,
    ),
    durable(
        "metadata.proto",
        "StoredDatabaseIdentityV1",
        PayloadBound::Tiny,
    ),
    durable(
        "metadata.proto",
        "StoredApplicationSequenceAllocatorV1",
        PayloadBound::Tiny,
    ),
    durable(
        "metadata.proto",
        "StoredAdministrationSequenceAllocatorV1",
        PayloadBound::Tiny,
    ),
    durable(
        "catalog.proto",
        "StoredContractBundleV1",
        PayloadBound::EnvelopeMaximum,
    ),
    durable(
        "catalog.proto",
        "ActiveCatalogPointerV1",
        PayloadBound::Tiny,
    ),
    durable(
        "catalog.proto",
        "StoredCatalogAdministrationV1",
        PayloadBound::Tiny,
    ),
    durable(
        "application.proto",
        "StoredEntityRecordV1",
        PayloadBound::Document,
    ),
    durable(
        "application.proto",
        "StoredIndexEntryV1",
        PayloadBound::Document,
    ),
    durable(
        "application.proto",
        "StoredIndexEpochV1",
        PayloadBound::Tiny,
    ),
    durable(
        "application.proto",
        "StoredPendingAdmissionV1",
        PayloadBound::Admission,
    ),
    durable(
        "application.proto",
        "StoredExecutionFailedV1",
        PayloadBound::Admission,
    ),
    durable(
        "application.proto",
        "StoredOutcomeV1",
        PayloadBound::Document,
    ),
    durable(
        "application.proto",
        "StoredDurableEventV1",
        PayloadBound::Document,
    ),
    durable(
        "outbox.proto",
        "StoredOutboxIntentV1",
        PayloadBound::Document,
    ),
    durable(
        "application.proto",
        "StoredProvenanceRecordV1",
        PayloadBound::EnvelopeMaximum,
    ),
    durable(
        "application.proto",
        "StoredCommitRecordV1",
        PayloadBound::EnvelopeMaximum,
    ),
    durable(
        "capability.proto",
        "CapabilityRecordV1",
        PayloadBound::Document,
    ),
    durable(
        "capability.proto",
        "CapabilityTokenLookupV1",
        PayloadBound::Tiny,
    ),
    durable(
        "capability.proto",
        "CapabilityBootstrapMarkerV1",
        PayloadBound::Tiny,
    ),
    durable(
        "capability.proto",
        "CapabilityAdministrationAuditV1",
        PayloadBound::Tiny,
    ),
    durable(
        "audit.proto",
        "ServiceAuditRecordV1",
        PayloadBound::Admission,
    ),
    durable("outbox.proto", "StoredOutboxStatusV1", PayloadBound::Tiny),
    durable(
        "projection.proto",
        "StoredProjectionStateV1",
        PayloadBound::Document,
    ),
    durable(
        "projection.proto",
        "StoredProjectionApplyV1",
        PayloadBound::Tiny,
    ),
    durable(
        "projection.proto",
        "StoredProjectionControlV1",
        PayloadBound::Tiny,
    ),
];

const fn durable(
    source: &'static str,
    name: &'static str,
    payload_bound: PayloadBound,
) -> DurableRecord {
    DurableRecord {
        source,
        name,
        payload_bound,
    }
}
const PROBE_SOURCE: &str = "compatibility_probe.proto";
const PROBE_RECORD_TYPE: &str = "riffdb.testing.v1.CompatibilityProbe";
const PROBE_PAYLOAD: &[u8] = &[0x08, 0x2a];
const CRC_32C: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);
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

    let production_root = repository_root.join("proto");
    validate_production_source_inventory(&production_root, PRODUCTION_SOURCES)?;
    let production = compile_descriptors(&production_root, PRODUCTION_SOURCES)?;
    validate_service_inventory(&production)?;
    let storage = compile_descriptors(&production_root, STORAGE_SOURCES)?;
    let durable_registry = build_durable_registry(&storage)?;
    let probe = compile_descriptors(&repository_root.join("fixtures/proto"), &[PROBE_SOURCE])?;
    validate_record_exists(&probe, PROBE_RECORD_TYPE)?;
    let probe_descriptor = probe.encode_to_vec();
    let probe_envelope = encode_probe_envelope(&probe_descriptor)?;

    generate_rust(&output_root, production.clone())?;
    write_artifact(
        &output_root,
        "fixtures/proto/descriptors/riffdb-v1-descriptor-set.bin",
        &production.encode_to_vec(),
    )?;
    write_artifact(
        &output_root,
        "fixtures/proto/descriptors/riffdb-storage-v1-descriptor-set.bin",
        &storage.encode_to_vec(),
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
        "fixtures/proto/durable-registry.txt",
        durable_registry_fixture(&durable_registry).as_bytes(),
    )?;
    write_artifact(
        &output_root,
        "fixtures/proto/durable-schema-hashes.bin",
        &durable_schema_hashes(&durable_registry),
    )?;
    write_artifact(
        &output_root,
        "fixtures/proto/durable-record-bounds.bin",
        &durable_record_bounds(&durable_registry),
    )?;
    write_artifact(
        &output_root,
        "fixtures/proto/wire-vectors.txt",
        wire_vectors()?.as_bytes(),
    )?;

    Ok(())
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
    validate_descriptor_set(&descriptor_set, source_names)?;
    Ok(descriptor_set)
}

struct BuiltDurableRecord {
    source: &'static str,
    name: &'static str,
    record_type: String,
    descriptor_bytes: usize,
    schema_hash: [u8; 32],
    max_payload_bytes: usize,
    max_envelope_bytes: usize,
}

fn build_durable_registry(
    storage: &FileDescriptorSet,
) -> Result<Vec<BuiltDurableRecord>, Box<dyn Error>> {
    if DURABLE_RECORDS.len() != 26 {
        return Err(io::Error::other("durable registry must contain exactly 26 records").into());
    }
    if storage.file.len() != STORAGE_SOURCES.len()
        || storage
            .file
            .iter()
            .any(|file| file.package() != "riffdb.storage.v1")
    {
        return Err(io::Error::other(
            "storage descriptor must contain exactly the nine riffdb.storage.v1 sources",
        )
        .into());
    }
    let message_count = storage
        .file
        .iter()
        .map(|file| file.message_type.len())
        .sum::<usize>();
    let enum_count = storage
        .file
        .iter()
        .map(|file| file.enum_type.len())
        .sum::<usize>();
    if message_count != 75 || enum_count != 12 {
        return Err(io::Error::other(format!(
            "storage schema must contain 74 semantic messages plus StoredEnvelope and 12 enums; found {message_count} messages and {enum_count} enums"
        ))
        .into());
    }

    let files = storage
        .file
        .iter()
        .map(|file| (file.name(), file))
        .collect::<BTreeMap<_, _>>();
    let mut record_types = BTreeSet::new();
    let mut built = Vec::with_capacity(DURABLE_RECORDS.len());
    for record in DURABLE_RECORDS {
        let source = format!("riffdb/storage/v1/{}", record.source);
        let file = files.get(source.as_str()).ok_or_else(|| {
            io::Error::other(format!("durable registry source is missing: {source}"))
        })?;
        if !file
            .message_type
            .iter()
            .any(|message| message.name() == record.name)
        {
            return Err(io::Error::other(format!(
                "durable registry source {source} does not own {}",
                record.name
            ))
            .into());
        }
        let record_type = format!("riffdb.storage.v1.{}", record.name);
        if !record_types.insert(record_type.clone()) {
            return Err(io::Error::other(format!("durable registry repeats {record_type}")).into());
        }
        let descriptor = descriptor_closure(storage, &source)?;
        let descriptor = descriptor.encode_to_vec();
        let record_type_len = u16::try_from(record_type.len())?;
        let descriptor_len = u64::try_from(descriptor.len())?;
        let mut frame = Vec::with_capacity(2 + record_type.len() + 8 + descriptor.len());
        frame.extend_from_slice(&record_type_len.to_be_bytes());
        frame.extend_from_slice(record_type.as_bytes());
        frame.extend_from_slice(&descriptor_len.to_be_bytes());
        frame.extend_from_slice(&descriptor);
        let max_payload_bytes = match record.payload_bound {
            PayloadBound::Tiny => TINY_PAYLOAD_BOUND,
            PayloadBound::Admission => ADMISSION_PAYLOAD_BOUND,
            PayloadBound::Document => DOCUMENT_PAYLOAD_BOUND,
            PayloadBound::EnvelopeMaximum => maximum_payload_for_envelope(&record_type),
        };
        let max_envelope_bytes =
            maximum_encoded_envelope_bytes_for(&record_type, max_payload_bytes)?;
        built.push(BuiltDurableRecord {
            source: record.source,
            name: record.name,
            record_type,
            descriptor_bytes: descriptor.len(),
            schema_hash: hash_schema(&frame).into_bytes(),
            max_payload_bytes,
            max_envelope_bytes,
        });
    }
    Ok(built)
}

fn maximum_payload_for_envelope(record_type: &str) -> usize {
    let mut lower = 0_usize;
    let mut upper = MAX_STORED_ENVELOPE_BYTES;
    while lower < upper {
        let middle = lower + (upper - lower).div_ceil(2);
        if maximum_encoded_envelope_bytes_for(record_type, middle).is_ok() {
            lower = middle;
        } else {
            upper = middle - 1;
        }
    }
    lower
}

fn descriptor_closure(
    descriptors: &FileDescriptorSet,
    root: &str,
) -> Result<FileDescriptorSet, Box<dyn Error>> {
    let files = descriptors
        .file
        .iter()
        .map(|file| (file.name(), file))
        .collect::<BTreeMap<_, _>>();
    let mut pending = vec![root.to_owned()];
    let mut names = BTreeSet::new();
    while let Some(name) = pending.pop() {
        if !names.insert(name.clone()) {
            continue;
        }
        let file = files.get(name.as_str()).ok_or_else(|| {
            io::Error::other(format!("descriptor transitive closure is missing {name}"))
        })?;
        pending.extend(file.dependency.iter().cloned());
    }
    let closure = FileDescriptorSet {
        file: names
            .iter()
            .map(|name| {
                (*files
                    .get(name.as_str())
                    .expect("closure names came from descriptor map"))
                .clone()
            })
            .collect(),
    };
    validate_descriptor_set(&closure, &[root])?;
    Ok(closure)
}

fn durable_registry_fixture(records: &[BuiltDurableRecord]) -> String {
    let mut output = String::from("riffdb-durable-registry-v1\n");
    let _ = writeln!(output, "records {}", records.len());
    for record in records {
        let _ = write!(
            output,
            "{} source={} message={} descriptor-bytes={} max-payload-bytes={} max-envelope-bytes={} schema-hash=",
            record.record_type,
            record.source,
            record.name,
            record.descriptor_bytes,
            record.max_payload_bytes,
            record.max_envelope_bytes,
        );
        for byte in record.schema_hash {
            let _ = write!(output, "{byte:02x}");
        }
        output.push('\n');
    }
    output
}

fn durable_schema_hashes(records: &[BuiltDurableRecord]) -> Vec<u8> {
    records
        .iter()
        .flat_map(|record| record.schema_hash)
        .collect()
}

fn durable_record_bounds(records: &[BuiltDurableRecord]) -> Vec<u8> {
    let mut output = Vec::with_capacity(records.len() * 8);
    for record in records {
        output.extend_from_slice(
            &u32::try_from(record.max_payload_bytes)
                .expect("durable payload bound fits u32")
                .to_be_bytes(),
        );
        output.extend_from_slice(
            &u32::try_from(record.max_envelope_bytes)
                .expect("durable envelope bound fits u32")
                .to_be_bytes(),
        );
    }
    output
}

fn validate_descriptor_set(
    descriptor_set: &FileDescriptorSet,
    roots: &[&str],
) -> Result<(), Box<dyn Error>> {
    if descriptor_set
        .file
        .iter()
        .any(|file| file.source_code_info.is_some())
    {
        return Err(io::Error::other("canonical descriptors must omit source info").into());
    }

    let actual_names = descriptor_set
        .file
        .iter()
        .map(|file| file.name().to_owned())
        .collect::<Vec<_>>();
    let mut sorted_names = actual_names.clone();
    sorted_names.sort();
    if actual_names != sorted_names {
        return Err(io::Error::other("canonical descriptor files are not sorted").into());
    }

    let files = descriptor_set
        .file
        .iter()
        .map(|file| (file.name(), file))
        .collect::<BTreeMap<_, _>>();
    if files.len() != descriptor_set.file.len() {
        return Err(io::Error::other("descriptor set contains duplicate file names").into());
    }

    let mut pending = roots
        .iter()
        .map(|root| (*root).to_owned())
        .collect::<Vec<_>>();
    let mut closure = BTreeSet::new();
    while let Some(name) = pending.pop() {
        if !closure.insert(name.clone()) {
            continue;
        }
        let file = files.get(name.as_str()).ok_or_else(|| {
            io::Error::other(format!("descriptor transitive closure is missing {name}"))
        })?;
        pending.extend(file.dependency.iter().cloned());
    }

    let actual = files.keys().copied().collect::<BTreeSet<_>>();
    let expected = closure.iter().map(String::as_str).collect::<BTreeSet<_>>();
    if actual != expected {
        return Err(io::Error::other(
            "descriptor set is not the exact transitive closure of its roots",
        )
        .into());
    }
    Ok(())
}

fn validate_record_exists(
    descriptor_set: &FileDescriptorSet,
    record_type: &str,
) -> Result<(), Box<dyn Error>> {
    let exists = descriptor_set
        .file
        .iter()
        .any(|file| message_exists(file.package(), &file.message_type, record_type));
    if !exists {
        return Err(io::Error::other(format!(
            "descriptor set does not define durable record type {record_type}"
        ))
        .into());
    }
    Ok(())
}

fn message_exists(prefix: &str, messages: &[DescriptorProto], target: &str) -> bool {
    messages.iter().any(|message| {
        let full_name = if prefix.is_empty() {
            message.name().to_owned()
        } else {
            format!("{prefix}.{}", message.name())
        };
        full_name == target || message_exists(&full_name, &message.nested_type, target)
    })
}

fn encode_probe_envelope(descriptor_set: &[u8]) -> Result<Vec<u8>, Box<dyn Error>> {
    if descriptor_set.len() > MAX_STORED_ENVELOPE_BYTES {
        return Err(io::Error::other("probe descriptor set exceeds the hard limit").into());
    }
    let record_type_len = u16::try_from(PROBE_RECORD_TYPE.len())?;
    let descriptor_len = u64::try_from(descriptor_set.len())?;
    let mut schema_frame =
        Vec::with_capacity(2 + PROBE_RECORD_TYPE.len() + 8 + descriptor_set.len());
    schema_frame.extend_from_slice(&record_type_len.to_be_bytes());
    schema_frame.extend_from_slice(PROBE_RECORD_TYPE.as_bytes());
    schema_frame.extend_from_slice(&descriptor_len.to_be_bytes());
    schema_frame.extend_from_slice(descriptor_set);

    Ok(StoredEnvelope {
        storage_format_version: STORAGE_FORMAT_VERSION_V1,
        record_type: PROBE_RECORD_TYPE.to_owned(),
        payload: PROBE_PAYLOAD.to_vec(),
        payload_crc32c: CRC_32C.checksum(PROBE_PAYLOAD),
        schema_hash: hash_schema(&schema_frame).as_bytes().to_vec(),
    }
    .encode_to_vec())
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

fn validate_production_source_inventory(
    root: &Path,
    expected: &[&str],
) -> Result<(), Box<dyn Error>> {
    let mut actual = Vec::new();
    collect_proto_sources(root, root, &mut actual)?;
    actual.sort();
    let mut expected = expected
        .iter()
        .map(|name| (*name).to_owned())
        .collect::<Vec<_>>();
    expected.sort();
    if actual != expected {
        return Err(io::Error::other(format!(
            "production proto source inventory differs from the generator roots: expected {expected:?}, found {actual:?}"
        ))
        .into());
    }
    Ok(())
}

fn collect_proto_sources(
    root: &Path,
    directory: &Path,
    output: &mut Vec<String>,
) -> Result<(), Box<dyn Error>> {
    let mut entries = fs::read_dir(directory)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        let path = entry.path();
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            collect_proto_sources(root, &path, output)?;
        } else if path
            .extension()
            .is_some_and(|extension| extension == "proto")
        {
            if !file_type.is_file() {
                return Err(io::Error::other(format!(
                    "production proto source is not a regular file: {}",
                    path.display()
                ))
                .into());
            }
            let relative = path.strip_prefix(root)?;
            let mut segments = Vec::new();
            for component in relative.components() {
                let Component::Normal(segment) = component else {
                    return Err(io::Error::other(format!(
                        "production proto path is not normalized: {}",
                        relative.display()
                    ))
                    .into());
                };
                segments.push(
                    segment
                        .to_str()
                        .ok_or_else(|| io::Error::other("production proto path is not UTF-8"))?,
                );
            }
            output.push(segments.join("/"));
        }
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

    let actual_services = descriptor_set
        .file
        .iter()
        .flat_map(|file| file.service.iter().map(|service| service.name().to_owned()))
        .collect::<BTreeSet<_>>();
    let expected_services = EXPECTED_METHODS
        .iter()
        .map(|(service, _, _)| (*service).to_owned())
        .collect::<BTreeSet<_>>();
    let service_count = descriptor_set
        .file
        .iter()
        .map(|file| file.service.len())
        .sum::<usize>();
    if actual_services != expected_services || service_count != expected_services.len() {
        return Err(io::Error::other(
            "service descriptors differ from the accepted five-service baseline",
        )
        .into());
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
        for dependency in &file.dependency {
            let _ = writeln!(output, "    import {dependency}");
        }
    }

    output.push_str("\nmessages\n");
    let mut messages = Vec::new();
    for file in &descriptor_set.file {
        collect_messages(file.package(), &file.message_type, &mut messages);
    }
    messages.sort_by(|left, right| left.0.cmp(&right.0));
    for (name, message) in messages {
        let phase = if message.field.is_empty() && name.starts_with("riffdb.v1.") {
            " phase-zero-shell"
        } else {
            ""
        };
        let _ = writeln!(output, "  {name} fields={}{phase}", message.field.len());
        for field in &message.field {
            let cardinality = match field.label() {
                prost_types::field_descriptor_proto::Label::Optional => "optional",
                prost_types::field_descriptor_proto::Label::Repeated => "repeated",
                prost_types::field_descriptor_proto::Label::Required => "required",
            };
            let field_type = if field.type_name().is_empty() {
                scalar_type_name(field.r#type())
            } else {
                field.type_name()
            };
            let presence = if field.proto3_optional() {
                " proto3-optional"
            } else {
                ""
            };
            let oneof = field
                .oneof_index
                .and_then(|index| usize::try_from(index).ok())
                .and_then(|index| message.oneof_decl.get(index))
                .map_or(String::new(), |oneof| format!(" oneof={}", oneof.name()));
            let _ = writeln!(
                output,
                "    {} {} {cardinality} {field_type}{presence}{oneof}",
                field.number(),
                field.name(),
            );
        }
    }

    output.push_str("\nenums\n");
    let mut enums = Vec::new();
    for file in &descriptor_set.file {
        collect_enums(
            file.package(),
            &file.enum_type,
            &file.message_type,
            &mut enums,
        );
    }
    enums.sort_by(|left, right| left.0.cmp(&right.0));
    for (name, enumeration) in enums {
        let _ = writeln!(output, "  {name} values={}", enumeration.value.len());
        for value in &enumeration.value {
            let _ = writeln!(output, "    {} {}", value.number(), value.name());
        }
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
                type_id: EnumTypeId::new(7).expect("fixture enum type ID is nonzero"),
                variant_id: EnumVariantId::new(11).expect("fixture enum variant ID is nonzero"),
            },
        ),
        (
            "value.list",
            CanonicalValue::list(vec![CanonicalValue::Null, CanonicalValue::Bool(true)])?,
        ),
        (
            "value.record",
            CanonicalValue::record(vec![
                (
                    FieldId::new(9).expect("fixture field ID is nonzero"),
                    CanonicalValue::I64(2),
                ),
                (
                    FieldId::new(3).expect("fixture field ID is nonzero"),
                    CanonicalValue::I64(1),
                ),
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
        ValidationPathSegment::Field(FieldId::new(3).expect("fixture field ID is nonzero")),
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
            PublicError::contract_mismatch(
                ContractVersion::new(7).expect("fixture contract version is nonzero"),
            ),
        ),
        (
            "error.execution-arithmetic",
            PublicError::command_execution_failed(ExecutionFailureCode::ArithmeticFault),
        ),
        (
            "error.execution-resource-limit",
            PublicError::command_execution_failed(ExecutionFailureCode::ResourceLimit),
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

fn scalar_type_name(r#type: prost_types::field_descriptor_proto::Type) -> &'static str {
    use prost_types::field_descriptor_proto::Type;
    match r#type {
        Type::Double => "double",
        Type::Float => "float",
        Type::Int64 => "int64",
        Type::Uint64 => "uint64",
        Type::Int32 => "int32",
        Type::Fixed64 => "fixed64",
        Type::Fixed32 => "fixed32",
        Type::Bool => "bool",
        Type::String => "string",
        Type::Group => "group",
        Type::Message => "message",
        Type::Bytes => "bytes",
        Type::Uint32 => "uint32",
        Type::Enum => "enum",
        Type::Sfixed32 => "sfixed32",
        Type::Sfixed64 => "sfixed64",
        Type::Sint32 => "sint32",
        Type::Sint64 => "sint64",
    }
}

fn collect_messages<'a>(
    prefix: &str,
    messages: &'a [DescriptorProto],
    output: &mut Vec<(String, &'a DescriptorProto)>,
) {
    for message in messages {
        let full_name = if prefix.is_empty() {
            message.name().to_owned()
        } else {
            format!("{prefix}.{}", message.name())
        };
        output.push((full_name.clone(), message));
        collect_messages(&full_name, &message.nested_type, output);
    }
}

fn collect_enums<'a>(
    prefix: &str,
    enums: &'a [prost_types::EnumDescriptorProto],
    messages: &'a [DescriptorProto],
    output: &mut Vec<(String, &'a prost_types::EnumDescriptorProto)>,
) {
    output.extend(
        enums
            .iter()
            .map(|enumeration| (format!("{prefix}.{}", enumeration.name()), enumeration)),
    );
    for message in messages {
        let message_name = format!("{prefix}.{}", message.name());
        collect_enums(
            &message_name,
            &message.enum_type,
            &message.nested_type,
            output,
        );
    }
}
