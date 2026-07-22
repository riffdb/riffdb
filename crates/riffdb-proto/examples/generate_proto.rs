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
    canonical_value_to_proto, decode_public_message,
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
    "riffdb/v1/admin.proto",
    "riffdb/v1/command.proto",
    "riffdb/v1/commit.proto",
    "riffdb/v1/common.proto",
    "riffdb/v1/contract.proto",
    "riffdb/v1/error.proto",
    "riffdb/v1/projection.proto",
    "riffdb/v1/query.proto",
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
const SERVICE_RESPONSE_CHARGE_FIXTURE: &str =
    include_str!("../../riffdb-service/fixtures/response-charge-v1.tsv");

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
        "fixtures/proto/public-schema-hashes.txt",
        public_schema_hashes(&production).as_bytes(),
    )?;
    write_artifact(
        &output_root,
        "fixtures/proto/public-key-envelope-vectors.txt",
        public_key_envelope_vectors().as_bytes(),
    )?;
    write_artifact(
        &output_root,
        "fixtures/proto/public-client-vectors.txt",
        public_client_vectors()?.as_bytes(),
    )?;
    write_artifact(
        &output_root,
        "fixtures/proto/public-response-charge-v1.tsv",
        public_response_charge_vectors()?.as_bytes(),
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
        let _ = writeln!(output, "  {name} fields={}", message.field.len());
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

fn public_schema_hashes(descriptor_set: &FileDescriptorSet) -> String {
    let public_set = FileDescriptorSet {
        file: descriptor_set
            .file
            .iter()
            .filter(|file| file.package() == "riffdb.v1")
            .cloned()
            .collect(),
    };
    let mut entries = Vec::<(String, String, Vec<u8>)>::new();
    entries.push((
        "schema".to_owned(),
        "riffdb.v1".to_owned(),
        public_set.encode_to_vec(),
    ));
    for file in &public_set.file {
        entries.push((
            "file".to_owned(),
            file.name().to_owned(),
            file.encode_to_vec(),
        ));
        collect_public_message_hash_inputs(file.package(), &file.message_type, &mut entries);
        for enumeration in &file.enum_type {
            collect_public_enum_hash_inputs(
                &format!("{}.{}", file.package(), enumeration.name()),
                enumeration,
                &mut entries,
            );
        }
        for service in &file.service {
            let name = format!("{}.{}", file.package(), service.name());
            entries.push(("service".to_owned(), name.clone(), service.encode_to_vec()));
            for method in &service.method {
                entries.push((
                    "rpc".to_owned(),
                    format!("{name}.{}", method.name()),
                    method.encode_to_vec(),
                ));
            }
        }
    }
    entries.sort_by(|left, right| (&left.0, &left.1).cmp(&(&right.0, &right.1)));

    let mut output = String::from("riffdb-public-schema-hashes-v1\n");
    for (kind, name, descriptor) in entries {
        let mut frame = Vec::with_capacity(kind.len() + name.len() + descriptor.len() + 2);
        frame.extend_from_slice(kind.as_bytes());
        frame.push(0);
        frame.extend_from_slice(name.as_bytes());
        frame.push(0);
        frame.extend_from_slice(&descriptor);
        let _ = write!(output, "{kind} {name} ");
        for byte in hash_schema(&frame).as_bytes() {
            let _ = write!(output, "{byte:02x}");
        }
        output.push('\n');
    }
    output
}

fn public_key_envelope_vectors() -> String {
    let mut output = String::from(
        "riffdb-public-key-envelope-vectors-v1\n# kind case envelope contextual separately-carried-owner hex\n",
    );
    for (kind, purpose, minimum) in [
        ("entity", 0x45_u8, 6_usize),
        ("index", 0x49_u8, 16_usize),
        ("partition", 0x50_u8, 6_usize),
    ] {
        let mut minimum_bytes = vec![purpose, 0x01, 0, 0, 0, 7];
        minimum_bytes.resize(minimum, 0);
        append_key_vector(
            &mut output,
            kind,
            "minimum",
            "pass",
            "pass",
            7,
            &minimum_bytes,
        );

        let mut maximum = minimum_bytes.clone();
        maximum.resize(riffdb_types::MAX_KEY_BYTES, 0xa5);
        append_key_vector(&mut output, kind, "maximum", "pass", "pass", 7, &maximum);

        let mut one_over = maximum;
        one_over.push(0);
        append_key_vector(
            &mut output,
            kind,
            "one-over",
            "reject",
            "reject",
            7,
            &one_over,
        );

        let mut wrong_purpose = minimum_bytes.clone();
        wrong_purpose[0] = if purpose == 0x45 { 0x50 } else { 0x45 };
        append_key_vector(
            &mut output,
            kind,
            "wrong-purpose",
            "reject",
            "reject",
            7,
            &wrong_purpose,
        );

        let mut wrong_version = minimum_bytes.clone();
        wrong_version[1] = 0x02;
        append_key_vector(
            &mut output,
            kind,
            "unsupported-version",
            "reject",
            "reject",
            7,
            &wrong_version,
        );

        let mut zero_owner = minimum_bytes.clone();
        zero_owner[2..6].fill(0);
        append_key_vector(
            &mut output,
            kind,
            "zero-owner",
            "reject",
            "reject",
            0,
            &zero_owner,
        );

        let mut malformed_components = minimum_bytes.clone();
        malformed_components.extend_from_slice(&[0xff, 0xff, 0xff]);
        append_key_vector(
            &mut output,
            kind,
            "malformed-components-schema-unproven",
            "pass",
            "pass",
            7,
            &malformed_components,
        );

        let mut trailing = minimum_bytes.clone();
        trailing.push(0);
        append_key_vector(
            &mut output,
            kind,
            "trailing-schema-unproven",
            "pass",
            "pass",
            7,
            &trailing,
        );
    }
    let mut mismatch = vec![0x45, 0x01];
    mismatch.extend_from_slice(&7_u32.to_be_bytes());
    append_key_vector(
        &mut output,
        "entity",
        "separate-owner-mismatch",
        "pass",
        "reject",
        8,
        &mismatch,
    );
    output
}

fn append_key_vector(
    output: &mut String,
    kind: &str,
    case: &str,
    envelope: &str,
    contextual: &str,
    owner: u32,
    bytes: &[u8],
) {
    let _ = write!(output, "{kind} {case} {envelope} {contextual} {owner} ");
    for byte in bytes {
        let _ = write!(output, "{byte:02x}");
    }
    output.push('\n');
}

fn collect_public_message_hash_inputs(
    prefix: &str,
    messages: &[DescriptorProto],
    entries: &mut Vec<(String, String, Vec<u8>)>,
) {
    for message in messages {
        let name = format!("{prefix}.{}", message.name());
        entries.push(("message".to_owned(), name.clone(), message.encode_to_vec()));
        for field in &message.field {
            entries.push((
                "field".to_owned(),
                format!("{name}.{}", field.name()),
                field.encode_to_vec(),
            ));
        }
        for oneof in &message.oneof_decl {
            entries.push((
                "oneof".to_owned(),
                format!("{name}.{}", oneof.name()),
                oneof.encode_to_vec(),
            ));
        }
        for enumeration in &message.enum_type {
            collect_public_enum_hash_inputs(
                &format!("{name}.{}", enumeration.name()),
                enumeration,
                entries,
            );
        }
        collect_public_message_hash_inputs(&name, &message.nested_type, entries);
    }
}

fn collect_public_enum_hash_inputs(
    name: &str,
    enumeration: &prost_types::EnumDescriptorProto,
    entries: &mut Vec<(String, String, Vec<u8>)>,
) {
    entries.push((
        "enum".to_owned(),
        name.to_owned(),
        enumeration.encode_to_vec(),
    ));
    for value in &enumeration.value {
        entries.push((
            "enum-value".to_owned(),
            format!("{name}.{}", value.name()),
            value.encode_to_vec(),
        ));
    }
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
    append_wire_vector(
        &mut output,
        "execute.response-replayed",
        &public_execute_response(v1::execute_command_response::CompletionStatus::Replayed as i32),
    );
    append_wire_vector(
        &mut output,
        "execute.response-read-only",
        &public_execute_response(
            v1::execute_command_response::CompletionStatus::ExecutedReadOnly as i32,
        ),
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

fn public_client_vectors() -> Result<String, Box<dyn Error>> {
    let mut output = String::from("riffdb-public-client-vectors-v1\n");
    let request_id = public_request_id();
    let active = public_active_selection();
    let page = v1::PageRequest {
        limit: Some(50),
        cursor: None,
    };

    append_client_vector(
        &mut output,
        "ContractService.ValidateContract",
        "request",
        "source",
        "riffdb.v1.ValidateContractRequest",
        &v1::ValidateContractRequest {
            request_id: request_id.clone(),
            source: "entity Budget { id: uuid }".to_owned(),
        },
    );
    append_client_vector(
        &mut output,
        "ContractService.ValidateContract",
        "response",
        "valid",
        "riffdb.v1.ValidateContractResponse",
        &v1::ValidateContractResponse {
            result: Some(v1::validate_contract_response::Result::Valid(v1::Unit {})),
        },
    );
    append_client_vector(
        &mut output,
        "ContractService.ValidateContract",
        "response",
        "invalid-syntax",
        "riffdb.v1.ValidateContractResponse",
        &v1::ValidateContractResponse {
            result: Some(v1::validate_contract_response::Result::Invalid(
                v1::CompilationDiagnostics {
                    diagnostics: Some(v1::compilation_diagnostics::Diagnostics::Syntax(
                        v1::SyntaxDiagnosticList {
                            diagnostics: vec![v1::SyntaxDiagnostic {
                                code: "RDB-S004".to_owned(),
                                summary: "contract source contains an unexpected token".to_owned(),
                                help: Some(
                                    "use the grammar-version-1 spelling shown in the language reference"
                                        .to_owned(),
                                ),
                                span: Some(v1::SourceSpan { start: 0, end: 1 }),
                                expected: vec!["entity".to_owned()],
                            }],
                        },
                    )),
                },
            )),
        },
    );
    append_client_vector(
        &mut output,
        "ContractService.ValidateContract",
        "response",
        "invalid-semantic",
        "riffdb.v1.ValidateContractResponse",
        &v1::ValidateContractResponse {
            result: Some(v1::validate_contract_response::Result::Invalid(
                v1::CompilationDiagnostics {
                    diagnostics: Some(v1::compilation_diagnostics::Diagnostics::Semantic(
                        v1::SemanticDiagnosticList {
                            diagnostics: vec![v1::SemanticDiagnostic {
                                code: "RDB-C004".to_owned(),
                                summary: "a referenced declaration, field, or binding is unknown"
                                    .to_owned(),
                                help: Some(
                                    "reference an exact case-sensitive declared name".to_owned(),
                                ),
                                primary_span: Some(v1::SourceSpan { start: 0, end: 1 }),
                                related_span: None,
                            }],
                        },
                    )),
                },
            )),
        },
    );

    append_client_vector(
        &mut output,
        "ContractService.ExplainCommand",
        "request",
        "active",
        "riffdb.v1.ExplainCommandRequest",
        &v1::ExplainCommandRequest {
            request_id: request_id.clone(),
            contract: Some(active.clone()),
            command_name: "budget.reserve".to_owned(),
        },
    );
    append_client_vector(
        &mut output,
        "ContractService.ExplainCommand",
        "response",
        "not-found",
        "riffdb.v1.ExplainCommandResponse",
        &v1::ExplainCommandResponse {
            result: Some(v1::explain_command_response::Result::NotFound(v1::Unit {})),
        },
    );
    append_client_vector(
        &mut output,
        "ContractService.ExplainCommand",
        "response",
        "found",
        "riffdb.v1.ExplainCommandResponse",
        &v1::ExplainCommandResponse {
            result: Some(v1::explain_command_response::Result::Found(
                public_explained_command(),
            )),
        },
    );

    append_client_vector(
        &mut output,
        "ContractService.DeployContract",
        "request",
        "expect-absent",
        "riffdb.v1.DeployContractRequest",
        &v1::DeployContractRequest {
            request_id: request_id.clone(),
            source: "entity Budget { id: uuid }".to_owned(),
            expected_active_version: None,
        },
    );
    for (branch, result) in [
        (
            "activated",
            v1::deploy_contract_response::Result::Activated(public_contract_descriptor()),
        ),
        (
            "already-active",
            v1::deploy_contract_response::Result::AlreadyActive(public_contract_descriptor()),
        ),
        (
            "expected-version-mismatch-absent",
            v1::deploy_contract_response::Result::ExpectedActiveVersionMismatch(
                v1::ExpectedActiveVersionMismatch {
                    actual_active_version: None,
                },
            ),
        ),
        (
            "expected-version-mismatch-present",
            v1::deploy_contract_response::Result::ExpectedActiveVersionMismatch(
                v1::ExpectedActiveVersionMismatch {
                    actual_active_version: Some(1),
                },
            ),
        ),
        (
            "bundle-conflict",
            v1::deploy_contract_response::Result::BundleConflict(v1::Unit {}),
        ),
    ] {
        append_client_vector(
            &mut output,
            "ContractService.DeployContract",
            "response",
            branch,
            "riffdb.v1.DeployContractResponse",
            &v1::DeployContractResponse {
                result: Some(result),
            },
        );
    }

    append_client_vector(
        &mut output,
        "ContractService.GetActiveContract",
        "request",
        "lookup",
        "riffdb.v1.GetActiveContractRequest",
        &v1::GetActiveContractRequest {
            request_id: request_id.clone(),
        },
    );
    for (branch, result) in [
        (
            "absent",
            v1::get_active_contract_response::Result::Absent(v1::Unit {}),
        ),
        (
            "present",
            v1::get_active_contract_response::Result::Present(public_contract_descriptor()),
        ),
    ] {
        append_client_vector(
            &mut output,
            "ContractService.GetActiveContract",
            "response",
            branch,
            "riffdb.v1.GetActiveContractResponse",
            &v1::GetActiveContractResponse {
                result: Some(result),
            },
        );
    }

    let execute_request = v1::ExecuteCommandRequest {
        request_id: request_id.clone(),
        command_name: "budget.reserve".to_owned(),
        expected_contract_version: Some(1),
        input: Some(canonical_value_to_proto(&CanonicalValue::Null)?),
    };
    append_client_vector(
        &mut output,
        "CommandService.Execute",
        "request",
        "versioned",
        "riffdb.v1.ExecuteCommandRequest",
        &execute_request,
    );
    for (branch, response) in [
        ("committed", public_execute_response(1)),
        ("replayed", public_execute_response(2)),
        ("executed-read-only", public_execute_response(3)),
    ] {
        append_client_vector(
            &mut output,
            "CommandService.Execute",
            "response",
            branch,
            "riffdb.v1.ExecuteCommandResponse",
            &response,
        );
    }

    append_client_vector(
        &mut output,
        "CommandService.GetOutcome",
        "request",
        "resolve",
        "riffdb.v1.GetOutcomeRequest",
        &v1::GetOutcomeRequest {
            request_id: request_id.clone(),
            contract_lineage: "budget".to_owned(),
            command_name: "budget.reserve".to_owned(),
            idempotency_key: "client-operation-1".to_owned(),
        },
    );
    for (branch, result) in [
        (
            "not-found",
            v1::get_outcome_response::Result::NotFound(v1::Unit {}),
        ),
        (
            "found-replayed",
            v1::get_outcome_response::Result::Found(public_execute_response(2)),
        ),
    ] {
        append_client_vector(
            &mut output,
            "CommandService.GetOutcome",
            "response",
            branch,
            "riffdb.v1.GetOutcomeResponse",
            &v1::GetOutcomeResponse {
                result: Some(result),
            },
        );
    }

    append_client_vector(
        &mut output,
        "QueryService.GetEntity",
        "request",
        "active",
        "riffdb.v1.GetEntityRequest",
        &v1::GetEntityRequest {
            request_id: request_id.clone(),
            contract: Some(active.clone()),
            entity_type_id: 1,
            entity_key: public_entity_key(1),
            fields: Some(v1::FieldSelection {
                field_ids: Vec::new(),
            }),
        },
    );
    for (branch, result) in [
        (
            "not-found",
            v1::get_entity_response::Result::NotFound(v1::Unit {}),
        ),
        (
            "found",
            v1::get_entity_response::Result::Found(v1::Entity {
                entity_key: public_entity_key(1),
                entity_version: 1,
                written_by_contract_version: 1,
                fields: Some(public_empty_record()),
            }),
        ),
    ] {
        append_client_vector(
            &mut output,
            "QueryService.GetEntity",
            "response",
            branch,
            "riffdb.v1.GetEntityResponse",
            &v1::GetEntityResponse {
                result: Some(result),
            },
        );
    }

    append_client_vector(
        &mut output,
        "QueryService.ScanIndex",
        "request",
        "first-page",
        "riffdb.v1.ScanIndexRequest",
        &v1::ScanIndexRequest {
            request_id: request_id.clone(),
            contract: Some(active.clone()),
            index_id: 1,
            leading_components: Vec::new(),
            fields: Some(v1::FieldSelection {
                field_ids: Vec::new(),
            }),
            page: Some(page.clone()),
        },
    );
    append_client_vector(
        &mut output,
        "QueryService.ScanIndex",
        "response",
        "page",
        "riffdb.v1.ScanIndexResponse",
        &v1::ScanIndexResponse {
            page: Some(v1::IndexPage {
                items: vec![v1::IndexRow {
                    index_entry_key: public_index_key(1),
                    values: Some(public_empty_record()),
                }],
                next_cursor: Some(vec![0x88; 16]),
                observed_fence: Some(v1::IndexScanFence {
                    position: Some(v1::index_scan_fence::Position::AppliedEpoch(1)),
                }),
            }),
        },
    );

    append_client_vector(
        &mut output,
        "QueryService.QueryProjection",
        "request",
        "first-page",
        "riffdb.v1.QueryProjectionRequest",
        &v1::QueryProjectionRequest {
            request_id: request_id.clone(),
            contract: Some(active),
            projection_id: 1,
            leading_components: Vec::new(),
            required_sequence: None,
            wait_nanos: 0,
            page: Some(page.clone()),
        },
    );
    for (branch, result) in public_projection_results() {
        append_client_vector(
            &mut output,
            "QueryService.QueryProjection",
            "response",
            branch,
            "riffdb.v1.QueryProjectionResponse",
            &v1::QueryProjectionResponse {
                result: Some(result),
            },
        );
    }

    append_client_vector(
        &mut output,
        "CommitService.GetCommit",
        "request",
        "sequence",
        "riffdb.v1.GetCommitRequest",
        &v1::GetCommitRequest {
            request_id: request_id.clone(),
            commit_sequence: 1,
        },
    );
    for (branch, result) in [
        (
            "not-found",
            v1::get_commit_response::Result::NotFound(v1::Unit {}),
        ),
        (
            "found",
            v1::get_commit_response::Result::Found(public_commit()),
        ),
    ] {
        append_client_vector(
            &mut output,
            "CommitService.GetCommit",
            "response",
            branch,
            "riffdb.v1.GetCommitResponse",
            &v1::GetCommitResponse {
                result: Some(result),
            },
        );
    }

    append_client_vector(
        &mut output,
        "CommitService.ScanCommits",
        "request",
        "first-page",
        "riffdb.v1.ScanCommitsRequest",
        &v1::ScanCommitsRequest {
            request_id: request_id.clone(),
            page: Some(page),
        },
    );
    append_client_vector(
        &mut output,
        "CommitService.ScanCommits",
        "response",
        "page",
        "riffdb.v1.ScanCommitsResponse",
        &v1::ScanCommitsResponse {
            page: Some(v1::CommitPage {
                items: vec![public_commit()],
                next_cursor: None,
                observed_fence: Some(public_applied(1)),
            }),
        },
    );

    append_client_vector(
        &mut output,
        "CommitService.SubscribeCommits",
        "request",
        "from-head",
        "riffdb.v1.SubscribeCommitsRequest",
        &v1::SubscribeCommitsRequest {
            request_id: request_id.clone(),
            after_sequence: None,
            maximum_lifetime_nanos: 900_000_000_000,
        },
    );
    append_client_vector(
        &mut output,
        "CommitService.SubscribeCommits",
        "stream",
        "commit",
        "riffdb.v1.CommitNotification",
        &v1::CommitNotification {
            notification: Some(v1::commit_notification::Notification::Commit(
                public_commit(),
            )),
        },
    );
    for reason in 1..=8 {
        append_client_vector(
            &mut output,
            "CommitService.SubscribeCommits",
            "stream",
            &format!("terminal-{reason}"),
            "riffdb.v1.CommitNotification",
            &v1::CommitNotification {
                notification: Some(v1::commit_notification::Notification::Terminal(
                    v1::CommitSubscriptionTerminal {
                        reason,
                        resume_after: Some(public_before_first()),
                    },
                )),
            },
        );
    }

    append_client_vector(
        &mut output,
        "AdminService.Health",
        "request",
        "authenticated",
        "riffdb.v1.HealthRequest",
        &v1::HealthRequest {
            request_id: Some(request_id.clone()),
        },
    );
    append_client_vector(
        &mut output,
        "AdminService.Health",
        "response",
        "pre-bootstrap",
        "riffdb.v1.HealthResponse",
        &public_pre_bootstrap_health(),
    );
    append_client_vector(
        &mut output,
        "AdminService.Health",
        "response",
        "authenticated",
        "riffdb.v1.HealthResponse",
        &public_authenticated_health(),
    );

    append_client_vector(
        &mut output,
        "AdminService.Stats",
        "request",
        "snapshot",
        "riffdb.v1.StatsRequest",
        &v1::StatsRequest {
            request_id: request_id.clone(),
        },
    );
    append_client_vector(
        &mut output,
        "AdminService.Stats",
        "response",
        "snapshot",
        "riffdb.v1.StatsResponse",
        &v1::StatsResponse {
            active_cursors: 1,
            active_commit_subscribers: 1,
            last_commit_sequence: Some(1),
            pending_outbox_deliveries: Some(0),
            known_projections: Some(1),
        },
    );

    for mode in [
        v1::CapabilityCreateMode::Normal,
        v1::CapabilityCreateMode::Bootstrap,
    ] {
        append_client_vector(
            &mut output,
            "AdminService.CreateCapability",
            "request",
            if mode == v1::CapabilityCreateMode::Normal {
                "normal"
            } else {
                "bootstrap"
            },
            "riffdb.v1.CreateCapabilityRequest",
            &public_create_capability_request(mode),
        );
    }
    for (branch, response) in public_create_capability_results() {
        append_client_vector(
            &mut output,
            "AdminService.CreateCapability",
            "response",
            branch,
            "riffdb.v1.CreateCapabilityResponse",
            &response,
        );
    }

    append_client_vector(
        &mut output,
        "AdminService.RevokeCapability",
        "request",
        "requested",
        "riffdb.v1.RevokeCapabilityRequest",
        &v1::RevokeCapabilityRequest {
            request_id,
            capability_id: public_request_id(),
            reason: v1::RevocationReason::Requested as i32,
        },
    );
    for (branch, result) in [
        (
            "revoked",
            v1::revoke_capability_response::Result::Revoked(public_capability_transition()),
        ),
        (
            "already-revoked",
            v1::revoke_capability_response::Result::AlreadyRevoked(public_capability_transition()),
        ),
        (
            "not-found",
            v1::revoke_capability_response::Result::CapabilityNotFound(v1::Unit {}),
        ),
    ] {
        append_client_vector(
            &mut output,
            "AdminService.RevokeCapability",
            "response",
            branch,
            "riffdb.v1.RevokeCapabilityResponse",
            &v1::RevokeCapabilityResponse {
                result: Some(result),
            },
        );
    }

    Ok(output)
}

fn append_client_vector(
    output: &mut String,
    rpc: &str,
    direction: &str,
    branch: &str,
    message_type: &str,
    message: &impl Message,
) {
    let _ = write!(output, "{rpc} {direction} {branch} {message_type} ");
    for byte in message.encode_to_vec() {
        let _ = write!(output, "{byte:02x}");
    }
    output.push('\n');
}

fn public_request_id() -> Vec<u8> {
    vec![
        0x01, 0x9b, 0xf6, 0xaa, 0xa6, 0x40, 0x7d, 0xe6, 0x89, 0xc9, 0x8a, 0x7f, 0x70, 0xbb, 0xbd,
        0x23,
    ]
}

fn public_active_selection() -> v1::ContractSelection {
    v1::ContractSelection {
        selection: Some(v1::contract_selection::Selection::Active(v1::Unit {})),
    }
}

fn public_before_first() -> v1::FrontierPosition {
    v1::FrontierPosition {
        position: Some(v1::frontier_position::Position::BeforeFirst(v1::Unit {})),
    }
}

fn public_applied(sequence: u64) -> v1::FrontierPosition {
    v1::FrontierPosition {
        position: Some(v1::frontier_position::Position::AppliedThrough(sequence)),
    }
}

fn public_empty_record() -> v1::ValueRecord {
    v1::ValueRecord { fields: Vec::new() }
}

fn public_contract_descriptor() -> v1::ContractDescriptor {
    v1::ContractDescriptor {
        contract_lineage: "budget".to_owned(),
        contract_version: 1,
        bundle_hash: vec![0x11; 32],
        source_hash: vec![0x22; 32],
        plan_root_hash: vec![0x33; 32],
    }
}

fn public_schema_artifact(key: v1::schema_artifact_key::Artifact) -> v1::GeneratedSchemaArtifact {
    let canonical_json = "{}".to_owned();
    v1::GeneratedSchemaArtifact {
        key: Some(v1::SchemaArtifactKey {
            artifact: Some(key),
        }),
        dialect: "https://json-schema.org/draft/2020-12/schema".to_owned(),
        schema_hash: hash_schema(canonical_json.as_bytes()).as_bytes().to_vec(),
        canonical_json,
    }
}

fn public_explained_command() -> v1::ExplainedCommand {
    v1::ExplainedCommand {
        contract: Some(public_contract_descriptor()),
        command_id: 1,
        plan_hash: vec![0x55; 32],
        explanation: Some(v1::CommandExplain {
            command_id: 1,
            execution_class: v1::ExecutionClass::IdempotentMutation as i32,
            partition_component_count: 1,
            conflict_key_count: 1,
            binding_ids: vec![0],
            read_fields: vec![v1::BindingFieldRef {
                binding_id: 0,
                field_id: 1,
            }],
            write_fields: vec![v1::BindingFieldRef {
                binding_id: 0,
                field_id: 2,
            }],
            invariant_ids: vec![1],
            event_type_ids: vec![1],
            outcome_ids: vec![1],
            rendered_text: "command budget.reserve".to_owned(),
        }),
        input_schema: Some(public_schema_artifact(
            v1::schema_artifact_key::Artifact::CommandInputId(1),
        )),
        outcome_schema: Some(public_schema_artifact(
            v1::schema_artifact_key::Artifact::CommandOutcomeUnionId(1),
        )),
    }
}

fn public_execute_response(status: i32) -> v1::ExecuteCommandResponse {
    let read_only =
        status == v1::execute_command_response::CompletionStatus::ExecutedReadOnly as i32;
    v1::ExecuteCommandResponse {
        status,
        commit_sequence: if read_only { 0 } else { 1 },
        contract_version: 1,
        plan_hash: vec![0x66; 32],
        outcome_type: "Reserved".to_owned(),
        outcome: Some(v1::Value {
            kind: Some(v1::value::Kind::NullValue(v1::NullValue::NullValue as i32)),
        }),
        provenance_uri: if read_only {
            String::new()
        } else {
            "riffdb://provenance/019bf6aa-a640-7de6-89c9-8a7f70bbbd23".to_owned()
        },
        durability_mode: if read_only {
            String::new()
        } else {
            "sync".to_owned()
        },
    }
}

fn public_entity_key(owner: u32) -> Vec<u8> {
    let mut key = vec![0x45, 0x01];
    key.extend_from_slice(&owner.to_be_bytes());
    key
}

fn public_index_key(owner: u32) -> Vec<u8> {
    let mut key = vec![0x49, 0x01];
    key.extend_from_slice(&owner.to_be_bytes());
    key.resize(16, 0);
    key
}

fn public_projection_identity() -> v1::ProjectionIdentity {
    v1::ProjectionIdentity {
        contract_lineage: "budget".to_owned(),
        projection_id: 1,
        projection_plan_hash: vec![0x77; 32],
    }
}

fn public_projection_results() -> Vec<(&'static str, v1::query_projection_response::Result)> {
    let ready_frontier = public_applied(1);
    let ready = v1::query_projection_response::Result::Ready(v1::QueryProjectionReady {
        data: Some(v1::ProjectionPage {
            items: Vec::new(),
            next_cursor: None,
            observed_fence: Some(v1::ProjectionPageFence {
                identity: Some(public_projection_identity()),
                generation: 1,
                frontier: Some(ready_frontier),
            }),
        }),
        frontier: Some(ready_frontier),
    });
    vec![
        ("ready", ready),
        (
            "wait-timed-out",
            v1::query_projection_response::Result::WaitTimedOut(v1::QueryProjectionWaitTimedOut {
                required_sequence: 2,
                current: Some(public_applied(1)),
            }),
        ),
        (
            "degraded-building",
            v1::query_projection_response::Result::Degraded(v1::QueryProjectionDegraded {
                current: Some(public_before_first()),
                reason: Some(v1::ProjectionUnavailableReason {
                    reason: Some(v1::projection_unavailable_reason::Reason::Building(
                        v1::Unit {},
                    )),
                }),
            }),
        ),
        (
            "degraded-rebuilding",
            v1::query_projection_response::Result::Degraded(v1::QueryProjectionDegraded {
                current: Some(public_applied(1)),
                reason: Some(v1::ProjectionUnavailableReason {
                    reason: Some(v1::projection_unavailable_reason::Reason::Rebuilding(
                        v1::Unit {},
                    )),
                }),
            }),
        ),
        (
            "degraded-failure",
            v1::query_projection_response::Result::Degraded(v1::QueryProjectionDegraded {
                current: Some(public_applied(1)),
                reason: Some(v1::ProjectionUnavailableReason {
                    reason: Some(v1::projection_unavailable_reason::Reason::Failure(
                        v1::ProjectionFailureCode::MissingCommit as i32,
                    )),
                }),
            }),
        ),
        (
            "invalid",
            v1::query_projection_response::Result::Invalid(v1::QueryProjectionInvalid {
                reason: v1::ProjectionFailureCode::ProjectionStateIntegrity as i32,
            }),
        ),
    ]
}

fn public_commit() -> v1::Commit {
    v1::Commit {
        commit_sequence: 1,
        admission_request_id: public_request_id(),
        contract_lineage: "budget".to_owned(),
        contract_version: 1,
        command_id: 1,
        plan_hash: vec![0x11; 32],
        canonical_input_hash: vec![0x22; 32],
        actor: Some(v1::AdmittedActor {
            principal_id: "operator".to_owned(),
            actor_kind: v1::ActorKind::Human as i32,
            tenant_scope: Some(v1::TenantScope {
                scope: Some(v1::tenant_scope::Scope::Global(v1::Unit {})),
            }),
            agent_session_id: None,
        }),
        logical_time: Some(v1::Timestamp {
            seconds: 1,
            nanos: 2,
        }),
        partition_hash: vec![0x33; 32],
        conflict_hashes: vec![vec![0x44; 32]],
        affected_entities: vec![v1::AffectedEntity {
            entity_key: public_entity_key(1),
            entity_version: 1,
        }],
        events: vec![v1::DurableEvent {
            event_id: Some(v1::EventId {
                commit_sequence: 1,
                event_ordinal: 0,
            }),
            event_type_id: 1,
            payload: Some(public_empty_record()),
        }],
        outcome: Some(v1::DeclaredOutcome {
            outcome_id: 1,
            outcome_name: "Reserved".to_owned(),
            value: Some(public_empty_record()),
        }),
        provenance_uri: "riffdb://provenance/019bf6aa-a640-7de6-89c9-8a7f70bbbd23".to_owned(),
        durability: v1::CommandDurability::Synchronous as i32,
    }
}

fn public_pre_bootstrap_health() -> v1::HealthResponse {
    v1::HealthResponse {
        result: Some(v1::health_response::Result::PreBootstrap(
            v1::PreBootstrapHealth {
                lifecycle: v1::PreBootstrapLifecycle::InitializingValidation as i32,
                liveness: true,
                readiness: false,
            },
        )),
    }
}

fn public_authenticated_health() -> v1::HealthResponse {
    v1::HealthResponse {
        result: Some(v1::health_response::Result::Authenticated(
            v1::AuthenticatedHealth {
                status: v1::HealthStatus::Ready as i32,
                active_contract_version: Some(1),
                last_commit_sequence: Some(1),
                components: vec![v1::HealthComponent {
                    component: v1::HealthComponentKind::AuthoritativeStorage as i32,
                    status: v1::HealthComponentStatus::Healthy as i32,
                }],
                started_at: Some(v1::Timestamp {
                    seconds: 1,
                    nanos: 0,
                }),
                build: Some(v1::BuildInfo {
                    semantic_version: "0.1.0".to_owned(),
                    git_revision: "0123456".to_owned(),
                    rust_version: "1.97.0".to_owned(),
                    enabled_features: vec!["default".to_owned()],
                    storage_format_version: 1,
                    contract_ir_version: 1,
                    mcp_protocol_baseline: "2025-06-18".to_owned(),
                }),
            },
        )),
    }
}

fn public_capability_grant() -> v1::CapabilityGrant {
    v1::CapabilityGrant {
        tenant_scope: Some(v1::TenantScope {
            scope: Some(v1::tenant_scope::Scope::Global(v1::Unit {})),
        }),
        partition_scope: Some(v1::PartitionScope {
            scope: Some(v1::partition_scope::Scope::All(v1::Unit {})),
        }),
        permissions: vec![v1::CapabilityPermission {
            permission: Some(
                v1::capability_permission::Permission::AdministerCapabilities(v1::Unit {}),
            ),
        }],
        field_visibility: Vec::new(),
        max_scan_rows: 50,
        approval_required: Vec::new(),
    }
}

fn public_create_capability_request(mode: v1::CapabilityCreateMode) -> v1::CreateCapabilityRequest {
    v1::CreateCapabilityRequest {
        request_id: public_request_id(),
        mode: mode as i32,
        capability_id: public_request_id(),
        principal_id: "operator".to_owned(),
        actor_kind: v1::ActorKind::Human as i32,
        requested_lifetime_seconds: 60,
        audiences: vec!["riffdb-cli".to_owned()],
        grant: Some(public_capability_grant()),
    }
}

fn public_capability_transition() -> v1::CapabilityTransition {
    v1::CapabilityTransition {
        identity: Some(v1::CapabilityIdentity {
            capability_id: public_request_id(),
            revision: 1,
        }),
        administration_sequence: 1,
    }
}

fn public_create_capability_results() -> Vec<(&'static str, v1::CreateCapabilityResponse)> {
    vec![
        (
            "normal-created",
            v1::CreateCapabilityResponse {
                result: Some(v1::create_capability_response::Result::Normal(
                    v1::NormalCreateCapabilityResult {
                        result: Some(v1::normal_create_capability_result::Result::Created(
                            v1::NormalCapabilityCreated {
                                transition: Some(public_capability_transition()),
                                token: "A".repeat(43),
                            },
                        )),
                    },
                )),
            },
        ),
        (
            "normal-already-created-token-unavailable",
            v1::CreateCapabilityResponse {
                result: Some(v1::create_capability_response::Result::Normal(
                    v1::NormalCreateCapabilityResult {
                        result: Some(
                            v1::normal_create_capability_result::Result::AlreadyCreatedTokenUnavailable(
                                v1::CapabilityIdentity {
                                    capability_id: public_request_id(),
                                    revision: 1,
                                },
                            ),
                        ),
                    },
                )),
            },
        ),
        (
            "normal-capability-id-conflict",
            v1::CreateCapabilityResponse {
                result: Some(v1::create_capability_response::Result::Normal(
                    v1::NormalCreateCapabilityResult {
                        result: Some(
                            v1::normal_create_capability_result::Result::CapabilityIdConflict(
                                v1::Unit {},
                            ),
                        ),
                    },
                )),
            },
        ),
        (
            "bootstrap-created",
            v1::CreateCapabilityResponse {
                result: Some(v1::create_capability_response::Result::Bootstrap(
                    v1::BootstrapCreateCapabilityResult {
                        result: Some(v1::bootstrap_create_capability_result::Result::Created(
                            public_capability_transition(),
                        )),
                    },
                )),
            },
        ),
        (
            "bootstrap-replayed",
            v1::CreateCapabilityResponse {
                result: Some(v1::create_capability_response::Result::Bootstrap(
                    v1::BootstrapCreateCapabilityResult {
                        result: Some(v1::bootstrap_create_capability_result::Result::Replayed(
                            public_capability_transition(),
                        )),
                    },
                )),
            },
        ),
        (
            "bootstrap-conflict",
            v1::CreateCapabilityResponse {
                result: Some(v1::create_capability_response::Result::Bootstrap(
                    v1::BootstrapCreateCapabilityResult {
                        result: Some(
                            v1::bootstrap_create_capability_result::Result::BootstrapConflict(
                                v1::Unit {},
                            ),
                        ),
                    },
                )),
            },
        ),
    ]
}

fn public_response_charge_vectors() -> Result<String, Box<dyn Error>> {
    let mut output = String::from(
        "riffdb_public_response_charge_fixture_version\t1\nservice_response_charge_version\t1\nceiling_bytes\t4194304\ncase_id\tresponse_family\tservice_charge_bytes\tprotobuf_encoded_bytes\tdisposition\n",
    );
    let mut lines = SERVICE_RESPONSE_CHARGE_FIXTURE.lines();
    if lines.next() != Some("riffdb_response_charge_fixture_version\t1")
        || lines.next() != Some("service_response_charge_version\t1")
        || lines.next() != Some("ceiling_bytes\t4194304")
        || lines.next()
            != Some(
                "case_id\tresponse_family\tresponse_variant\tshape_v1\tservice_charge_bytes\tdisposition",
            )
    {
        return Err(io::Error::other("unexpected service response-charge fixture header").into());
    }
    for line in lines {
        let columns = line.split('\t').collect::<Vec<_>>();
        if columns.len() != 6 {
            return Err(io::Error::other("invalid service response-charge fixture row").into());
        }
        let case_id = columns[0];
        let family = columns[1];
        let charge = columns[4].parse::<usize>()?;
        let disposition = columns[5];
        let encoded = response_charge_candidate(case_id)?;
        if let Some(encoded) = &encoded {
            if encoded.len() > charge {
                return Err(io::Error::other(format!(
                    "public encoding for {case_id} exceeds service charge: {} > {charge}",
                    encoded.len()
                ))
                .into());
            }
            match disposition {
                "release" if encoded.len() <= 4_194_304 => {
                    validate_response_charge_candidate(case_id, encoded)?;
                }
                "response_too_large" if encoded.len() > 4_194_304 => {}
                _ => {
                    return Err(io::Error::other(format!(
                        "public encoding for {case_id} disagrees with service disposition"
                    ))
                    .into());
                }
            }
        } else if !case_id.starts_with("boundary.") {
            return Err(io::Error::other(format!(
                "missing public response-charge candidate for {case_id}"
            ))
            .into());
        }
        let encoded = encoded.map_or_else(|| "-".to_owned(), |bytes| bytes.len().to_string());
        let _ = writeln!(
            output,
            "{case_id}\t{family}\t{charge}\t{encoded}\t{disposition}"
        );
    }
    Ok(output)
}

fn validate_response_charge_candidate(case_id: &str, encoded: &[u8]) -> Result<(), io::Error> {
    macro_rules! decode {
        ($type:ty) => {
            decode_public_message::<$type>(encoded).map(|_| ())
        };
    }
    let result = match case_id {
        "boundary.exact_ceiling"
        | "contract_validation.invalid_empty_source"
        | "contract_validation.valid" => decode!(v1::ValidateContractResponse),
        value if value.starts_with("commit_notification.") => decode!(v1::CommitNotification),
        value if value.starts_with("create_capability.") => decode!(v1::CreateCapabilityResponse),
        value if value.starts_with("deploy_contract.") => decode!(v1::DeployContractResponse),
        value if value.starts_with("execute_command.") => decode!(v1::ExecuteCommandResponse),
        value if value.starts_with("explain_command.") => decode!(v1::ExplainCommandResponse),
        value if value.starts_with("get_active_contract.") => {
            decode!(v1::GetActiveContractResponse)
        }
        value if value.starts_with("get_commit.") => decode!(v1::GetCommitResponse),
        value if value.starts_with("get_entity.") => decode!(v1::GetEntityResponse),
        value if value.starts_with("get_outcome.") => decode!(v1::GetOutcomeResponse),
        value if value.starts_with("health.") => decode!(v1::HealthResponse),
        value if value.starts_with("projection_status.") => {
            decode!(v1::GetProjectionStatusResponse)
        }
        value if value.starts_with("query_projection.") => {
            decode!(v1::QueryProjectionResponse)
        }
        value if value.starts_with("revoke_capability.") => {
            decode!(v1::RevokeCapabilityResponse)
        }
        value if value.starts_with("scan_commits.") => decode!(v1::ScanCommitsResponse),
        value if value.starts_with("scan_index.") => decode!(v1::ScanIndexResponse),
        value if value.starts_with("stats.") => decode!(v1::StatsResponse),
        _ => {
            return Err(io::Error::other(
                "unknown releasable response-charge candidate",
            ));
        }
    };
    result.map_err(|_| io::Error::other(format!("invalid public response-charge case {case_id}")))
}

fn append_varint(output: &mut Vec<u8>, mut value: u64) {
    loop {
        let mut byte = u8::try_from(value & 0x7f).expect("seven-bit varint chunk");
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        output.push(byte);
        if value == 0 {
            return;
        }
    }
}

fn varint_length(mut value: usize) -> usize {
    let mut length = 1;
    while value >= 0x80 {
        value >>= 7;
        length += 1;
    }
    length
}

fn response_charge_boundary(total_length: usize) -> Vec<u8> {
    let mut encoded = v1::ValidateContractResponse {
        result: Some(v1::validate_contract_response::Result::Valid(v1::Unit {})),
    }
    .encode_to_vec();
    append_varint(&mut encoded, (2_047_u64 << 3) | 2);
    let fixed_length = encoded.len();
    let payload_length = (1..=10)
        .find_map(|length_bytes| {
            total_length
                .checked_sub(fixed_length + length_bytes)
                .filter(|payload| varint_length(*payload) == length_bytes)
        })
        .expect("boundary fixture length can carry one unknown field");
    append_varint(
        &mut encoded,
        u64::try_from(payload_length).expect("fixture length fits u64"),
    );
    encoded.resize(total_length, 0);
    encoded
}

fn response_charge_candidate(case_id: &str) -> Result<Option<Vec<u8>>, Box<dyn Error>> {
    let encoded = match case_id {
        "boundary.exact_ceiling" => response_charge_boundary(4_194_304),
        "boundary.one_over" => response_charge_boundary(4_194_305),
        "commit_notification.commit_oversize" => v1::CommitNotification {
            notification: Some(v1::commit_notification::Notification::Commit(
                response_charge_commit(1_024, 4_096),
            )),
        }
        .encode_to_vec(),
        "commit_notification.commit_representative" => v1::CommitNotification {
            notification: Some(v1::commit_notification::Notification::Commit(
                response_charge_commit(2, 24),
            )),
        }
        .encode_to_vec(),
        "commit_notification.terminal" => v1::CommitNotification {
            notification: Some(v1::commit_notification::Notification::Terminal(
                v1::CommitSubscriptionTerminal {
                    reason: v1::CommitSubscriptionEndReason::LifetimeElapsed as i32,
                    resume_after: Some(public_applied(1)),
                },
            )),
        }
        .encode_to_vec(),
        "contract_validation.invalid_empty_source" => v1::ValidateContractResponse {
            result: Some(v1::validate_contract_response::Result::Invalid(
                v1::CompilationDiagnostics {
                    diagnostics: Some(v1::compilation_diagnostics::Diagnostics::Syntax(
                        v1::SyntaxDiagnosticList {
                            diagnostics: vec![v1::SyntaxDiagnostic {
                                code: "RDB-S005".to_owned(),
                                summary: "contract source ended before the declaration was complete"
                                    .to_owned(),
                                help: Some(
                                    "use the grammar-version-1 spelling shown in the language reference"
                                        .to_owned(),
                                ),
                                span: Some(v1::SourceSpan { start: 0, end: 0 }),
                                expected: vec!["contract".to_owned()],
                            }],
                        },
                    )),
                },
            )),
        }
        .encode_to_vec(),
        "contract_validation.valid" => v1::ValidateContractResponse {
            result: Some(v1::validate_contract_response::Result::Valid(v1::Unit {})),
        }
        .encode_to_vec(),
        "create_capability.created" => public_create_capability_results()
            .into_iter()
            .find(|(branch, _)| *branch == "normal-created")
            .expect("normal-created fixture")
            .1
            .encode_to_vec(),
        "deploy_contract.activated" => v1::DeployContractResponse {
            result: Some(v1::deploy_contract_response::Result::Activated(
                public_contract_descriptor(),
            )),
        }
        .encode_to_vec(),
        "execute_command.journaled" => public_execute_response(
            v1::execute_command_response::CompletionStatus::Committed as i32,
        )
        .encode_to_vec(),
        "execute_command.read_only" => public_execute_response(
            v1::execute_command_response::CompletionStatus::ExecutedReadOnly as i32,
        )
        .encode_to_vec(),
        "explain_command.found_budget" => v1::ExplainCommandResponse {
            result: Some(v1::explain_command_response::Result::Found(
                response_charge_explained_command(),
            )),
        }
        .encode_to_vec(),
        "get_active_contract.present" => v1::GetActiveContractResponse {
            result: Some(v1::get_active_contract_response::Result::Present(
                public_contract_descriptor(),
            )),
        }
        .encode_to_vec(),
        "get_commit.found_oversize" => v1::GetCommitResponse {
            result: Some(v1::get_commit_response::Result::Found(
                response_charge_commit(1_024, 4_096),
            )),
        }
        .encode_to_vec(),
        "get_commit.found_representative" => v1::GetCommitResponse {
            result: Some(v1::get_commit_response::Result::Found(
                response_charge_commit(2, 24),
            )),
        }
        .encode_to_vec(),
        "get_entity.found_record" => v1::GetEntityResponse {
            result: Some(v1::get_entity_response::Result::Found(v1::Entity {
                entity_key: response_charge_entity_key(1, 24, 0),
                entity_version: 1,
                written_by_contract_version: 1,
                fields: Some(response_charge_string_record(17)),
            })),
        }
        .encode_to_vec(),
        "get_outcome.found_replayed" => v1::GetOutcomeResponse {
            result: Some(v1::get_outcome_response::Result::Found(
                public_execute_response(
                    v1::execute_command_response::CompletionStatus::Replayed as i32,
                ),
            )),
        }
        .encode_to_vec(),
        "health.authenticated" => response_charge_authenticated_health().encode_to_vec(),
        "health.prebootstrap" => v1::HealthResponse {
            result: Some(v1::health_response::Result::PreBootstrap(
                v1::PreBootstrapHealth {
                    lifecycle: v1::PreBootstrapLifecycle::InitializingBootstrap as i32,
                    liveness: true,
                    readiness: false,
                },
            )),
        }
        .encode_to_vec(),
        "projection_status.uninitialized" => v1::GetProjectionStatusResponse {
            result: Some(v1::get_projection_status_response::Result::Found(
                v1::ProjectionStatus {
                    identity: Some(response_charge_projection_identity()),
                    lifecycle: v1::ProjectionLifecycle::Building as i32,
                    published: None,
                    candidate: None,
                    published_apply_mode: None,
                    failure: None,
                    authoritative_head: Some(public_applied(1)),
                },
            )),
        }
        .encode_to_vec(),
        "query_projection.ready_page" => response_charge_projection_page().encode_to_vec(),
        "revoke_capability.revoked" => v1::RevokeCapabilityResponse {
            result: Some(v1::revoke_capability_response::Result::Revoked(
                public_capability_transition(),
            )),
        }
        .encode_to_vec(),
        "scan_commits.page_with_cursor" => v1::ScanCommitsResponse {
            page: Some(v1::CommitPage {
                items: vec![response_charge_commit(2, 24)],
                next_cursor: Some(vec![0x88; 16]),
                observed_fence: Some(public_applied(1)),
            }),
        }
        .encode_to_vec(),
        "scan_index.page_with_cursor" => response_charge_index_page().encode_to_vec(),
        "stats.populated" => v1::StatsResponse {
            active_cursors: 7,
            active_commit_subscribers: 3,
            last_commit_sequence: Some(1),
            pending_outbox_deliveries: Some(11),
            known_projections: Some(2),
        }
        .encode_to_vec(),
        _ => {
            return Err(io::Error::other(format!(
                "unknown service response-charge case {case_id}"
            ))
            .into());
        }
    };
    Ok(Some(encoded))
}

fn response_charge_entity_key(owner: u32, length: usize, ordinal: u32) -> Vec<u8> {
    let mut key = public_entity_key(owner);
    key.resize(length, 0);
    if length >= 10 {
        key[length - 4..].copy_from_slice(&ordinal.to_be_bytes());
    }
    key
}

fn response_charge_string_record(length: usize) -> v1::ValueRecord {
    v1::ValueRecord {
        fields: vec![v1::ValueField {
            field_id: Some(1),
            name: String::new(),
            value: Some(v1::Value {
                kind: Some(v1::value::Kind::StringValue("x".repeat(length))),
            }),
        }],
    }
}

fn response_charge_commit(entity_count: usize, key_length: usize) -> v1::Commit {
    let mut commit = public_commit();
    commit.contract_lineage = "budget1".to_owned();
    commit.actor.as_mut().expect("fixture actor").principal_id = "service-actor".to_owned();
    commit.conflict_hashes.clear();
    commit.events.clear();
    commit
        .outcome
        .as_mut()
        .expect("fixture outcome")
        .outcome_name = "Completed".to_owned();
    commit.affected_entities = (0..entity_count)
        .map(|ordinal| v1::AffectedEntity {
            entity_key: response_charge_entity_key(
                1,
                key_length,
                u32::try_from(ordinal).expect("fixture ordinal fits u32"),
            ),
            entity_version: 1,
        })
        .collect();
    commit
}

fn response_charge_json(length: usize) -> String {
    assert!(length >= 8);
    format!("{{\"x\":\"{}\"}}", "x".repeat(length - 8))
}

fn response_charge_explained_command() -> v1::ExplainedCommand {
    let mut explained = public_explained_command();
    explained
        .contract
        .as_mut()
        .expect("fixture descriptor")
        .contract_lineage = "budget-led".to_owned();
    let explanation = explained.explanation.as_mut().expect("fixture explain");
    explanation.binding_ids = vec![0];
    explanation.read_fields = vec![
        v1::BindingFieldRef {
            binding_id: 0,
            field_id: 1,
        },
        v1::BindingFieldRef {
            binding_id: 0,
            field_id: 2,
        },
    ];
    explanation.write_fields = vec![
        v1::BindingFieldRef {
            binding_id: 0,
            field_id: 1,
        },
        v1::BindingFieldRef {
            binding_id: 0,
            field_id: 2,
        },
        v1::BindingFieldRef {
            binding_id: 0,
            field_id: 3,
        },
    ];
    explanation.invariant_ids = vec![1, 2];
    explanation.event_type_ids.clear();
    explanation.outcome_ids = vec![1, 2, 3];
    explanation.rendered_text = "x".repeat(1_984);
    for (schema, length) in [
        (explained.input_schema.as_mut().expect("input schema"), 649),
        (
            explained.outcome_schema.as_mut().expect("outcome schema"),
            1_704,
        ),
    ] {
        schema.canonical_json = response_charge_json(length);
        schema.schema_hash = hash_schema(schema.canonical_json.as_bytes())
            .as_bytes()
            .to_vec();
    }
    explained
}

fn response_charge_authenticated_health() -> v1::HealthResponse {
    v1::HealthResponse {
        result: Some(v1::health_response::Result::Authenticated(
            v1::AuthenticatedHealth {
                status: v1::HealthStatus::Ready as i32,
                active_contract_version: Some(1),
                last_commit_sequence: Some(1),
                components: vec![
                    v1::HealthComponent {
                        component: v1::HealthComponentKind::AuthoritativeStorage as i32,
                        status: v1::HealthComponentStatus::Healthy as i32,
                    },
                    v1::HealthComponent {
                        component: v1::HealthComponentKind::Catalog as i32,
                        status: v1::HealthComponentStatus::Healthy as i32,
                    },
                    v1::HealthComponent {
                        component: v1::HealthComponentKind::CommitCoordinator as i32,
                        status: v1::HealthComponentStatus::Healthy as i32,
                    },
                ],
                started_at: Some(v1::Timestamp {
                    seconds: 1,
                    nanos: 0,
                }),
                build: Some(v1::BuildInfo {
                    semantic_version: "0.1.0".to_owned(),
                    git_revision: "0123456789abcdef".to_owned(),
                    rust_version: "1.97.0".to_owned(),
                    enabled_features: vec!["abc".to_owned(), "defg".to_owned()],
                    storage_format_version: 1,
                    contract_ir_version: 1,
                    mcp_protocol_baseline: "2025-06-18".to_owned(),
                }),
            },
        )),
    }
}

fn response_charge_projection_identity() -> v1::ProjectionIdentity {
    v1::ProjectionIdentity {
        contract_lineage: "budget1".to_owned(),
        projection_id: 1,
        projection_plan_hash: vec![0x77; 32],
    }
}

fn response_charge_projection_page() -> v1::QueryProjectionResponse {
    let frontier = public_applied(1);
    v1::QueryProjectionResponse {
        result: Some(v1::query_projection_response::Result::Ready(
            v1::QueryProjectionReady {
                data: Some(v1::ProjectionPage {
                    items: vec![v1::ProjectionRow {
                        group: vec![v1::Value {
                            kind: Some(v1::value::Kind::StringValue("group".to_owned())),
                        }],
                        values: Some(response_charge_string_record(11)),
                    }],
                    next_cursor: Some(vec![0x88; 16]),
                    observed_fence: Some(v1::ProjectionPageFence {
                        identity: Some(response_charge_projection_identity()),
                        generation: 1,
                        frontier: Some(frontier),
                    }),
                }),
                frontier: Some(frontier),
            },
        )),
    }
}

fn response_charge_index_page() -> v1::ScanIndexResponse {
    let row = |length, ordinal: u32| v1::IndexRow {
        index_entry_key: {
            let mut key = public_index_key(1);
            key.resize(20, 0);
            key[16..].copy_from_slice(&ordinal.to_be_bytes());
            key
        },
        values: Some(response_charge_string_record(length)),
    };
    v1::ScanIndexResponse {
        page: Some(v1::IndexPage {
            items: vec![row(7, 0), row(13, 1)],
            next_cursor: Some(vec![0x88; 16]),
            observed_fence: Some(v1::IndexScanFence {
                position: Some(v1::index_scan_fence::Position::AppliedEpoch(1)),
            }),
        }),
    }
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
