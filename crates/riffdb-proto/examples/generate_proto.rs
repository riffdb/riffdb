#![forbid(unsafe_code)]

//! Pure-Rust, deterministic Protobuf artifact generator.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::error::Error;
use std::fmt::Write as _;
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use crc::{CRC_32_ISCSI, Crc};
use prost::Message;
use prost_types::{DescriptorProto, FileDescriptorSet};
use protox::Compiler;
use riffdb_errors::{
    PublicError, ValidationCode, ValidationIssue, ValidationIssues, ValidationPath,
    ValidationPathSegment,
};
use riffdb_proto::{
    PublicWireError, canonical_value_to_proto, decode_public_message,
    envelope::{
        MAX_STORED_ENVELOPE_BYTES, STORAGE_FORMAT_VERSION_V1, maximum_encoded_envelope_bytes_for,
    },
    public_error_to_proto,
    storage::v1::StoredEnvelope,
    v1, validate_public_message,
};
use riffdb_types::{
    CanonicalValue, ContractVersion, CurrencyCode, Date, Decimal, DecimalSpec, EnumTypeId,
    EnumVariantId, ExecutionFailureCode, FieldId, IncidentId, MAX_CONTRACT_LINEAGE_BYTES, Money,
    Timestamp, hash_schema,
};

const STORAGE_SOURCES: &[&str] = &[
    "riffdb/storage/v1/application.proto",
    "riffdb/storage/v1/audit.proto",
    "riffdb/storage/v1/capability.proto",
    "riffdb/storage/v1/catalog.proto",
    "riffdb/storage/v1/common.proto",
    "riffdb/storage/v1/envelope.proto",
    "riffdb/storage/v1/index_v2.proto",
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
    "riffdb/storage/v1/index_v2.proto",
    "riffdb/storage/v1/metadata.proto",
    "riffdb/storage/v1/outbox.proto",
    "riffdb/storage/v1/projection.proto",
    "riffdb/v1/admin.proto",
    "riffdb/v1/command.proto",
    "riffdb/v1/commit.proto",
    "riffdb/v1/common.proto",
    "riffdb/v1/contract.proto",
    "riffdb/v1/discovery.proto",
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
    durable(
        "index_v2.proto",
        "StoredIndexEntryV2",
        PayloadBound::Document,
    ),
];

const LEGACY_DURABLE_RECORD_COUNT: usize = 26;

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
    ("AdminService", "ListPendingOutboxDeliveries", false),
    ("AdminService", "RevokeCapability", false),
    ("AdminService", "Stats", false),
    ("CommandService", "Execute", false),
    ("CommandService", "GetOutcome", false),
    ("CommitService", "GetCommit", false),
    ("CommitService", "ScanCommits", false),
    ("CommitService", "SubscribeCommits", true),
    ("CommitService", "TraceProvenance", false),
    ("ContractService", "DeployContract", false),
    ("ContractService", "DiscoverCommandTools", false),
    ("ContractService", "DiscoverResources", false),
    ("ContractService", "ExplainCommand", false),
    ("ContractService", "GetActiveContract", false),
    ("ContractService", "GetContractVersion", false),
    ("ContractService", "ValidateContract", false),
    ("QueryService", "GetEntity", false),
    ("QueryService", "GetProjectionStatus", false),
    ("QueryService", "QueryProjection", false),
    ("QueryService", "ScanIndex", false),
];
const SERVICE_RESPONSE_CHARGE_FIXTURE: &str =
    include_str!("../../riffdb-service/fixtures/response-charge-v1.tsv");
const PRE_WP137_PUBLIC_SCHEMA_HASHES: &str =
    include_str!("../../../fixtures/proto/pre-wp137-public-schema-hashes.txt");

const DISCOVERY_PAGE_BOUNDARIES: [(&str, usize, bool, &str); 4] = [
    ("empty-exact-end", 0, false, "exact_end"),
    ("one-item-exact-end", 1, false, "exact_end"),
    ("limit-500-continuation", 500, true, "continuation"),
    ("limit-500-exact-end", 500, false, "exact_end"),
];

const WP137_OPTIONAL_COVERAGE: [(&str, &str, &str); 14] = [
    (
        "riffdb.v1.CommandToolDiscoveryPage.next_cursor",
        "ContractService.DiscoverCommandTools:response:full-boundary-empty-exact-end",
        "ContractService.DiscoverCommandTools:response:full-boundary-limit-500-continuation",
    ),
    (
        "riffdb.v1.CompactCommandToolDiscoveryPage.next_cursor",
        "ContractService.DiscoverCommandTools:response:compact-boundary-empty-exact-end",
        "ContractService.DiscoverCommandTools:response:compact-boundary-limit-500-continuation",
    ),
    (
        "riffdb.v1.CompactResourceDiscoveryPage.next_cursor",
        "ContractService.DiscoverResources:response:compact-boundary-empty-exact-end",
        "ContractService.DiscoverResources:response:compact-boundary-limit-500-continuation",
    ),
    (
        "riffdb.v1.ExecuteCommandResponse.outcome_uri",
        "CommandService.Execute:response:committed-legacy-no-locator",
        "CommandService.Execute:response:committed",
    ),
    (
        "riffdb.v1.Decimal.precision",
        "CommandService.Execute:request:decimal-legacy-no-precision",
        "CommandService.Execute:request:decimal-with-precision",
    ),
    (
        "riffdb.v1.GetOutcomeRequest.outcome_uri",
        "CommandService.GetOutcome:request:resolve",
        "CommandService.GetOutcome:request:resolve-locator",
    ),
    (
        "riffdb.v1.OutboxDeliveryPage.next_cursor",
        "AdminService.ListPendingOutboxDeliveries:response:page",
        "AdminService.ListPendingOutboxDeliveries:response:page-with-cursor",
    ),
    (
        "riffdb.v1.ProvenanceClaims.approval_id",
        "CommitService.TraceProvenance:response:found-no-optional-claims",
        "CommitService.TraceProvenance:response:found",
    ),
    (
        "riffdb.v1.ProvenanceClaims.reason",
        "CommitService.TraceProvenance:response:found-no-optional-claims",
        "CommitService.TraceProvenance:response:found",
    ),
    (
        "riffdb.v1.ProvenanceClaims.source_commit",
        "CommitService.TraceProvenance:response:found-no-optional-claims",
        "CommitService.TraceProvenance:response:found",
    ),
    (
        "riffdb.v1.ProvenanceClaims.source_repository",
        "CommitService.TraceProvenance:response:found-no-optional-claims",
        "CommitService.TraceProvenance:response:found",
    ),
    (
        "riffdb.v1.ResourceDiscoveryPage.next_cursor",
        "ContractService.DiscoverResources:response:full-boundary-empty-exact-end",
        "ContractService.DiscoverResources:response:full-boundary-limit-500-continuation",
    ),
    (
        "riffdb.v1.ContractCompatibilitySummary.parent_bundle_hash",
        "ContractService.GetActiveContract:response:present",
        "ContractService.GetActiveContract:response:present-successor",
    ),
    (
        "riffdb.v1.ContractCompatibilitySummary.parent_contract_version",
        "ContractService.GetActiveContract:response:present",
        "ContractService.GetActiveContract:response:present-successor",
    ),
];

const OPERATION_SCHEMA_DIALECT: &str = "https://json-schema.org/draft/2020-12/schema";
const OPERATION_ENVELOPE_SCHEMA_ID: &str = "riffdb.command-operation-envelope/v1";
const GET_OUTCOME_RESULT_SCHEMA_ID: &str = "riffdb.command-get-outcome-result/v1";
const OPERATION_ENVELOPE_SCHEMA_PATH: &str =
    "crates/riffdb-service/schema/riffdb.command-operation-envelope-v1.schema.json";
const GET_OUTCOME_RESULT_SCHEMA_PATH: &str =
    "crates/riffdb-service/schema/riffdb.command-get-outcome-result-v1.schema.json";
const OPERATION_ENVELOPE_SCHEMA_BYTES: usize = 2_561;
const GET_OUTCOME_RESULT_SCHEMA_BYTES: usize = 4_745;
const OPERATION_ENVELOPE_SCHEMA_HASH: &str =
    "781ff93c2dbfd2ee2bec286f7810300a0fec0a170548b1405cb8ba2ac8d90398";
const GET_OUTCOME_RESULT_SCHEMA_HASH: &str =
    "4056f01c297120b06ac905f33482132a9085865975ada36e2396a61ebf19fc0d";
const OPERATION_SCHEMA_SOURCE_MAX_BYTES: usize = 65_536;
const OPERATION_SCHEMA_AGGREGATE_CHARGE_MAX_BYTES: usize = 131_584;
const OPERATION_SCHEMA_FULL_CATALOG_BYTES: usize = 7_554;
const OPERATION_SCHEMA_IDENTITY_CATALOG_BYTES: usize = 148;
const OPERATION_SCHEMA_COMPOSITION_BYTES: usize = 4_892;
const OPERATION_SCHEMA_COMPOSITION_HASH: &str =
    "7133befc751d1a60d08ab993f2d03e98a73e75accf1963ace0e636c6e55b5c77";
const REPRESENTATIVE_OUTCOME_SCHEMA_PATH: &str = "fixtures/compiler/schemas/04-00000002.json";
const REPRESENTATIVE_OUTCOME_SCHEMA_BYTES: usize = 2_393;
const REPRESENTATIVE_OUTCOME_SCHEMA_HASH: &str =
    "f711c1596dee5a94f03d6b727cc47ebc9a35e6f9a35cd6a84b52cdebd3850f6c";
const REPRESENTATIVE_BUNDLE_HASH_PATH: &str = "fixtures/compiler/bundle-hash.txt";
const REPRESENTATIVE_BUNDLE_HASH: &str =
    "8a22cd047f46682a37468c40900fd67161f1a1d52d71b221d1eb9c9caa74cf5f";

struct OperationSchemaCheckpoint {
    manifest: String,
    full_catalog: Vec<u8>,
    identity_catalog: Vec<u8>,
    representative_composition: Vec<u8>,
}

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
    let operation_schema_checkpoint = build_operation_schema_checkpoint(repository_root)?;

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
        public_client_vectors(&production)?.as_bytes(),
    )?;
    write_artifact(
        &output_root,
        "fixtures/proto/public-response-charge-v1.tsv",
        public_response_charge_vectors()?.as_bytes(),
    )?;
    write_artifact(
        &output_root,
        "fixtures/proto/operation-schema-catalog-v1.txt",
        operation_schema_checkpoint.manifest.as_bytes(),
    )?;
    write_artifact(
        &output_root,
        "fixtures/proto/operation-schema-catalog-full-v1.bin",
        &operation_schema_checkpoint.full_catalog,
    )?;
    write_artifact(
        &output_root,
        "fixtures/proto/operation-schema-catalog-identity-v1.bin",
        &operation_schema_checkpoint.identity_catalog,
    )?;
    write_artifact(
        &output_root,
        "fixtures/proto/operation-schema-composition-allocate-budget-v1.schema.json",
        &operation_schema_checkpoint.representative_composition,
    )?;
    let legacy_registry = durable_registry
        .get(..LEGACY_DURABLE_RECORD_COUNT)
        .ok_or_else(|| io::Error::other("durable registry lost its legacy prefix"))?;
    let v2_record = durable_registry
        .get(LEGACY_DURABLE_RECORD_COUNT)
        .ok_or_else(|| io::Error::other("durable registry is missing StoredIndexEntryV2"))?;
    write_artifact(
        &output_root,
        "fixtures/proto/durable-registry.txt",
        durable_registry_fixture(legacy_registry).as_bytes(),
    )?;
    write_artifact(
        &output_root,
        "fixtures/proto/durable-readable-registry.txt",
        durable_registry_fixture(&durable_registry).as_bytes(),
    )?;
    write_artifact(
        &output_root,
        "fixtures/proto/durable-writable-registry.txt",
        durable_writable_registry_fixture(&durable_registry)?.as_bytes(),
    )?;
    write_artifact(
        &output_root,
        "fixtures/proto/durable-schema-hashes.bin",
        &durable_schema_hashes(legacy_registry),
    )?;
    write_artifact(
        &output_root,
        "fixtures/proto/durable-index-v2-schema-hash.bin",
        &v2_record.schema_hash,
    )?;
    write_artifact(
        &output_root,
        "fixtures/proto/durable-record-bounds.bin",
        &durable_record_bounds(legacy_registry),
    )?;
    write_artifact(
        &output_root,
        "fixtures/proto/durable-index-v2-record-bound.bin",
        &durable_record_bounds(std::slice::from_ref(v2_record)),
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

fn build_operation_schema_checkpoint(
    repository_root: &Path,
) -> Result<OperationSchemaCheckpoint, Box<dyn Error>> {
    let envelope = read_operation_schema_source(
        repository_root,
        OPERATION_ENVELOPE_SCHEMA_PATH,
        OPERATION_ENVELOPE_SCHEMA_BYTES,
        OPERATION_ENVELOPE_SCHEMA_HASH,
    )?;
    let get_outcome = read_operation_schema_source(
        repository_root,
        GET_OUTCOME_RESULT_SCHEMA_PATH,
        GET_OUTCOME_RESULT_SCHEMA_BYTES,
        GET_OUTCOME_RESULT_SCHEMA_HASH,
    )?;
    if envelope.len() + get_outcome.len() > OPERATION_SCHEMA_AGGREGATE_CHARGE_MAX_BYTES {
        return Err(
            io::Error::other("operation schema sources exceed the aggregate charge bound").into(),
        );
    }

    let envelope_hash = hash_schema(envelope.as_bytes());
    let get_outcome_hash = hash_schema(get_outcome.as_bytes());
    let full_message = v1::OperationSchemaCatalog {
        command_operation_envelope: Some(v1::OperationSchemaArtifact {
            schema_id: OPERATION_ENVELOPE_SCHEMA_ID.to_owned(),
            dialect: OPERATION_SCHEMA_DIALECT.to_owned(),
            schema_hash: envelope_hash.as_bytes().to_vec(),
            canonical_json: envelope.clone(),
        }),
        command_get_outcome_result: Some(v1::OperationSchemaArtifact {
            schema_id: GET_OUTCOME_RESULT_SCHEMA_ID.to_owned(),
            dialect: OPERATION_SCHEMA_DIALECT.to_owned(),
            schema_hash: get_outcome_hash.as_bytes().to_vec(),
            canonical_json: get_outcome,
        }),
    };
    let full_catalog = full_message.encode_to_vec();
    if full_catalog.len() != OPERATION_SCHEMA_FULL_CATALOG_BYTES
        || full_catalog.len() > OPERATION_SCHEMA_AGGREGATE_CHARGE_MAX_BYTES
        || v1::OperationSchemaCatalog::decode(full_catalog.as_slice())? != full_message
    {
        return Err(io::Error::other("operation schema full-catalog fixture drifted").into());
    }

    let identity_message = v1::OperationSchemaCatalogIdentity {
        command_operation_envelope: Some(v1::OperationSchemaIdentity {
            schema_id: OPERATION_ENVELOPE_SCHEMA_ID.to_owned(),
            schema_hash: envelope_hash.as_bytes().to_vec(),
        }),
        command_get_outcome_result: Some(v1::OperationSchemaIdentity {
            schema_id: GET_OUTCOME_RESULT_SCHEMA_ID.to_owned(),
            schema_hash: get_outcome_hash.as_bytes().to_vec(),
        }),
    };
    let identity_catalog = identity_message.encode_to_vec();
    if identity_catalog.len() != OPERATION_SCHEMA_IDENTITY_CATALOG_BYTES
        || v1::OperationSchemaCatalogIdentity::decode(identity_catalog.as_slice())?
            != identity_message
    {
        return Err(io::Error::other("operation schema identity-catalog fixture drifted").into());
    }

    let representative_display =
        fs::read(repository_root.join(REPRESENTATIVE_OUTCOME_SCHEMA_PATH))?;
    let representative_bytes = representative_display
        .strip_suffix(b"\n")
        .ok_or_else(|| io::Error::other("representative compiler schema lost its display LF"))?;
    if representative_bytes.ends_with(b"\n")
        || representative_bytes.len() != REPRESENTATIVE_OUTCOME_SCHEMA_BYTES
        || schema_hash_hex(representative_bytes) != REPRESENTATIVE_OUTCOME_SCHEMA_HASH
    {
        return Err(io::Error::other("representative compiler outcome schema drifted").into());
    }
    let representative = std::str::from_utf8(representative_bytes)?;
    let compiler_root_prefix = format!("{{\"$schema\":\"{OPERATION_SCHEMA_DIALECT}\",");
    let representative_without_dialect = representative
        .strip_prefix(&compiler_root_prefix)
        .ok_or_else(|| {
            io::Error::other("representative compiler schema has an unexpected root dialect")
        })?;
    let mut representative_body = String::with_capacity(representative.len());
    representative_body.push('{');
    representative_body.push_str(representative_without_dialect);

    let outcome_slot = "\"$defs\":{\"outcome\":false}";
    if envelope.match_indices(outcome_slot).count() != 1 {
        return Err(
            io::Error::other("operation envelope must have one uncomposed outcome slot").into(),
        );
    }
    let mut composed_slot = String::with_capacity(outcome_slot.len() + representative_body.len());
    composed_slot.push_str("\"$defs\":{\"outcome\":");
    composed_slot.push_str(&representative_body);
    composed_slot.push('}');
    let representative_composition = envelope
        .replacen(outcome_slot, &composed_slot, 1)
        .into_bytes();
    if representative_composition.len() != OPERATION_SCHEMA_COMPOSITION_BYTES
        || schema_hash_hex(&representative_composition) != OPERATION_SCHEMA_COMPOSITION_HASH
    {
        return Err(io::Error::other("representative operation schema composition drifted").into());
    }

    let bundle_hash = fs::read_to_string(repository_root.join(REPRESENTATIVE_BUNDLE_HASH_PATH))?;
    if bundle_hash != format!("{REPRESENTATIVE_BUNDLE_HASH}\n") {
        return Err(io::Error::other("representative compiler bundle hash drifted").into());
    }

    let mut manifest = String::from("riffdb-operation-schema-catalog-v1\n");
    writeln!(manifest, "hash-scheme=1")?;
    writeln!(manifest, "hash-domain=riffdb.schema/v1")?;
    writeln!(manifest, "dialect={OPERATION_SCHEMA_DIALECT}")?;
    writeln!(manifest, "artifacts=2")?;
    writeln!(manifest, "full-catalog-bytes={}", full_catalog.len())?;
    writeln!(
        manifest,
        "identity-catalog-bytes={}",
        identity_catalog.len()
    )?;
    writeln!(
        manifest,
        "field=1 name=command_operation_envelope source={OPERATION_ENVELOPE_SCHEMA_PATH} schema_id={OPERATION_ENVELOPE_SCHEMA_ID} canonical_bytes={} schema_hash={}",
        envelope.len(),
        schema_hash_hex(envelope.as_bytes())
    )?;
    writeln!(
        manifest,
        "field=2 name=command_get_outcome_result source={GET_OUTCOME_RESULT_SCHEMA_PATH} schema_id={GET_OUTCOME_RESULT_SCHEMA_ID} canonical_bytes={} schema_hash={}",
        get_outcome_schema_len(&full_message)?,
        schema_hash_hex(
            full_message
                .command_get_outcome_result
                .as_ref()
                .ok_or_else(|| io::Error::other("full catalog lost GetOutcome schema"))?
                .canonical_json
                .as_bytes()
        )
    )?;
    writeln!(
        manifest,
        "composition_source={REPRESENTATIVE_OUTCOME_SCHEMA_PATH} canonical_bytes={REPRESENTATIVE_OUTCOME_SCHEMA_BYTES} schema_hash={REPRESENTATIVE_OUTCOME_SCHEMA_HASH}"
    )?;
    writeln!(
        manifest,
        "composition_output=fixtures/proto/operation-schema-composition-allocate-budget-v1.schema.json canonical_bytes={} schema_hash={OPERATION_SCHEMA_COMPOSITION_HASH}",
        representative_composition.len()
    )?;
    writeln!(
        manifest,
        "compiler_bundle_hash={REPRESENTATIVE_BUNDLE_HASH}"
    )?;

    Ok(OperationSchemaCheckpoint {
        manifest,
        full_catalog,
        identity_catalog,
        representative_composition,
    })
}

fn read_operation_schema_source(
    repository_root: &Path,
    relative_path: &str,
    expected_bytes: usize,
    expected_hash: &str,
) -> Result<String, Box<dyn Error>> {
    let bytes = fs::read(repository_root.join(relative_path))?;
    let dialect_member = format!("\"$schema\":\"{OPERATION_SCHEMA_DIALECT}\"");
    if bytes.len() != expected_bytes
        || bytes.len() > OPERATION_SCHEMA_SOURCE_MAX_BYTES
        || bytes.last() != Some(&b'}')
        || bytes.contains(&b'\n')
        || bytes.contains(&b'\r')
        || schema_hash_hex(&bytes) != expected_hash
    {
        return Err(
            io::Error::other(format!("operation schema source drifted: {relative_path}")).into(),
        );
    }
    let source = String::from_utf8(bytes)?;
    if source.match_indices(&dialect_member).count() != 1 {
        return Err(io::Error::other(format!(
            "operation schema source has an unexpected dialect: {relative_path}"
        ))
        .into());
    }
    Ok(source)
}

fn get_outcome_schema_len(catalog: &v1::OperationSchemaCatalog) -> Result<usize, Box<dyn Error>> {
    catalog
        .command_get_outcome_result
        .as_ref()
        .map(|artifact| artifact.canonical_json.len())
        .ok_or_else(|| io::Error::other("full catalog lost GetOutcome schema").into())
}

fn schema_hash_hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(64);
    for byte in hash_schema(bytes).as_bytes() {
        let _ = write!(output, "{byte:02x}");
    }
    output
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
    if DURABLE_RECORDS.len() != 27 {
        return Err(
            io::Error::other("readable durable registry must contain exactly 27 records").into(),
        );
    }
    if storage.file.len() != STORAGE_SOURCES.len()
        || storage
            .file
            .iter()
            .any(|file| file.package() != "riffdb.storage.v1")
    {
        return Err(io::Error::other(
            "storage descriptor must contain exactly the ten riffdb.storage.v1 sources",
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
    if message_count != 76 || enum_count != 12 {
        return Err(io::Error::other(format!(
            "storage schema must contain 75 semantic messages plus StoredEnvelope and 12 enums; found {message_count} messages and {enum_count} enums"
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

fn durable_writable_registry_fixture(
    records: &[BuiltDurableRecord],
) -> Result<String, Box<dyn Error>> {
    let legacy = records
        .get(..LEGACY_DURABLE_RECORD_COUNT)
        .ok_or_else(|| io::Error::other("durable registry lost its legacy prefix"))?;
    let v2 = records
        .get(LEGACY_DURABLE_RECORD_COUNT)
        .ok_or_else(|| io::Error::other("durable registry is missing StoredIndexEntryV2"))?;
    let writable = legacy[..8]
        .iter()
        .chain(std::iter::once(v2))
        .chain(legacy[9..].iter());

    let mut output = String::from("riffdb-durable-writable-registry-v1\n");
    let _ = writeln!(output, "records {LEGACY_DURABLE_RECORD_COUNT}");
    for record in writable {
        let _ = write!(output, "{} schema-hash=", record.record_type);
        for byte in record.schema_hash {
            let _ = write!(output, "{byte:02x}");
        }
        output.push('\n');
    }
    Ok(output)
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
            "service inventory differs from the accepted five-service, twenty-two-RPC baseline",
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
            outcome_uri: None,
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

fn public_client_vectors(descriptors: &FileDescriptorSet) -> Result<String, Box<dyn Error>> {
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
        (
            "present-successor",
            v1::get_active_contract_response::Result::Present(
                public_successor_contract_descriptor(),
            ),
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
    for (branch, precision) in [
        ("decimal-legacy-no-precision", None),
        ("decimal-with-precision", Some(3)),
    ] {
        append_client_vector(
            &mut output,
            "CommandService.Execute",
            "request",
            branch,
            "riffdb.v1.ExecuteCommandRequest",
            &v1::ExecuteCommandRequest {
                request_id: request_id.clone(),
                command_name: "budget.reserve".to_owned(),
                expected_contract_version: Some(1),
                input: Some(v1::Value {
                    kind: Some(v1::value::Kind::DecimalValue(v1::Decimal {
                        coefficient_twos_complement: vec![123],
                        scale: 2,
                        precision,
                    })),
                }),
            },
        );
    }
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
    let mut legacy_committed = public_execute_response(1);
    legacy_committed.outcome_uri = None;
    append_client_vector(
        &mut output,
        "CommandService.Execute",
        "response",
        "committed-legacy-no-locator",
        "riffdb.v1.ExecuteCommandResponse",
        &legacy_committed,
    );

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
            outcome_uri: None,
        },
    );
    append_client_vector(
        &mut output,
        "CommandService.GetOutcome",
        "request",
        "resolve-locator",
        "riffdb.v1.GetOutcomeRequest",
        &v1::GetOutcomeRequest {
            request_id: request_id.clone(),
            contract_lineage: String::new(),
            command_name: String::new(),
            idempotency_key: String::new(),
            outcome_uri: Some(public_outcome_uri()),
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

    append_client_vector(
        &mut output,
        "ContractService.GetContractVersion",
        "request",
        "exact",
        "riffdb.v1.GetContractVersionRequest",
        &v1::GetContractVersionRequest {
            request_id: public_request_id(),
            contract_lineage: "budget".to_owned(),
            contract_version: 1,
        },
    );
    for (branch, result) in [
        (
            "not-found",
            v1::get_contract_version_response::Result::NotFound(v1::Unit {}),
        ),
        (
            "found",
            v1::get_contract_version_response::Result::Found(public_contract_descriptor()),
        ),
    ] {
        append_client_vector(
            &mut output,
            "ContractService.GetContractVersion",
            "response",
            branch,
            "riffdb.v1.GetContractVersionResponse",
            &v1::GetContractVersionResponse {
                result: Some(result),
            },
        );
    }

    let full_fence = public_discovery_fence(true);
    let compact_fence = public_discovery_fence(false);
    append_client_vector(
        &mut output,
        "ContractService.DiscoverCommandTools",
        "request",
        "full-first-page",
        "riffdb.v1.DiscoverCommandToolsRequest",
        &v1::DiscoverCommandToolsRequest {
            request_id: public_request_id(),
            page: Some(v1::PageRequest {
                limit: Some(500),
                cursor: None,
            }),
            prior_fence: None,
            representation: v1::DiscoveryRepresentation::Full as i32,
        },
    );
    append_client_vector(
        &mut output,
        "ContractService.DiscoverCommandTools",
        "request",
        "compact-conditional",
        "riffdb.v1.DiscoverCommandToolsRequest",
        &v1::DiscoverCommandToolsRequest {
            request_id: public_request_id(),
            page: Some(v1::PageRequest {
                limit: Some(500),
                cursor: None,
            }),
            prior_fence: Some(compact_fence.clone()),
            representation: v1::DiscoveryRepresentation::CompactObservation as i32,
        },
    );
    append_client_vector(
        &mut output,
        "ContractService.DiscoverCommandTools",
        "response",
        "full-page",
        "riffdb.v1.DiscoverCommandToolsResponse",
        &v1::DiscoverCommandToolsResponse {
            result: Some(v1::discover_command_tools_response::Result::Page(
                v1::CommandToolDiscoveryPage {
                    items: {
                        let mut items = public_command_discovery_items(14);
                        items.push(v1::CommandToolDiscoveryItem {
                            item: Some(v1::command_tool_discovery_item::Item::CommandTool(
                                public_command_tool_descriptor(),
                            )),
                        });
                        items
                    },
                    next_cursor: None,
                    observed_fence: Some(full_fence.clone()),
                    operation_schemas: Some(public_operation_schema_catalog()),
                },
            )),
        },
    );
    append_client_vector(
        &mut output,
        "ContractService.DiscoverCommandTools",
        "response",
        "compact-page",
        "riffdb.v1.DiscoverCommandToolsResponse",
        &v1::DiscoverCommandToolsResponse {
            result: Some(v1::discover_command_tools_response::Result::CompactPage(
                v1::CompactCommandToolDiscoveryPage {
                    items: {
                        let mut items = public_compact_command_discovery_items(14);
                        items.push(v1::CompactCommandToolDiscoveryItem {
                            item: Some(v1::compact_command_tool_discovery_item::Item::CommandTool(
                                public_compact_command_tool_descriptor(),
                            )),
                        });
                        items
                    },
                    next_cursor: None,
                    observed_fence: Some(compact_fence.clone()),
                },
            )),
        },
    );
    append_client_vector(
        &mut output,
        "ContractService.DiscoverCommandTools",
        "response",
        "catalog-unchanged",
        "riffdb.v1.DiscoverCommandToolsResponse",
        &v1::DiscoverCommandToolsResponse {
            result: Some(
                v1::discover_command_tools_response::Result::CatalogUnchanged(
                    compact_fence.clone(),
                ),
            ),
        },
    );

    append_client_vector(
        &mut output,
        "QueryService.GetProjectionStatus",
        "request",
        "active",
        "riffdb.v1.GetProjectionStatusRequest",
        &v1::GetProjectionStatusRequest {
            request_id: public_request_id(),
            contract: Some(public_active_selection()),
            projection_id: 1,
        },
    );
    for (branch, result) in [
        (
            "not-found",
            v1::get_projection_status_response::Result::NotFound(v1::Unit {}),
        ),
        (
            "found-ready",
            v1::get_projection_status_response::Result::Found(public_projection_status()),
        ),
    ] {
        append_client_vector(
            &mut output,
            "QueryService.GetProjectionStatus",
            "response",
            branch,
            "riffdb.v1.GetProjectionStatusResponse",
            &v1::GetProjectionStatusResponse {
                result: Some(result),
            },
        );
    }

    append_client_vector(
        &mut output,
        "CommitService.TraceProvenance",
        "request",
        "by-sequence",
        "riffdb.v1.TraceProvenanceRequest",
        &v1::TraceProvenanceRequest {
            request_id: public_request_id(),
            selector: Some(v1::ProvenanceSelection {
                selection: Some(v1::provenance_selection::Selection::CommitSequence(1)),
            }),
        },
    );
    append_client_vector(
        &mut output,
        "CommitService.TraceProvenance",
        "request",
        "by-provenance-id",
        "riffdb.v1.TraceProvenanceRequest",
        &v1::TraceProvenanceRequest {
            request_id: public_request_id(),
            selector: Some(v1::ProvenanceSelection {
                selection: Some(v1::provenance_selection::Selection::ProvenanceId(
                    public_request_id(),
                )),
            }),
        },
    );
    for (branch, result) in [
        (
            "not-found",
            v1::trace_provenance_response::Result::NotFound(v1::Unit {}),
        ),
        (
            "found",
            v1::trace_provenance_response::Result::Found(public_provenance()),
        ),
        (
            "found-no-optional-claims",
            v1::trace_provenance_response::Result::Found(public_provenance_without_claims()),
        ),
    ] {
        append_client_vector(
            &mut output,
            "CommitService.TraceProvenance",
            "response",
            branch,
            "riffdb.v1.TraceProvenanceResponse",
            &v1::TraceProvenanceResponse {
                result: Some(result),
            },
        );
    }

    append_client_vector(
        &mut output,
        "AdminService.ListPendingOutboxDeliveries",
        "request",
        "first-page",
        "riffdb.v1.ListPendingOutboxDeliveriesRequest",
        &v1::ListPendingOutboxDeliveriesRequest {
            request_id: public_request_id(),
            page: Some(v1::PageRequest {
                limit: Some(50),
                cursor: None,
            }),
        },
    );
    for (branch, state, attempts, next_attempt_at, next_cursor) in [
        (
            "page",
            v1::OutboxDeliveryState::RetryScheduled,
            1,
            Some(v1::Timestamp {
                seconds: 2,
                nanos: 0,
            }),
            None,
        ),
        (
            "page-with-cursor",
            v1::OutboxDeliveryState::RetryScheduled,
            1,
            Some(v1::Timestamp {
                seconds: 2,
                nanos: 0,
            }),
            Some(vec![0x88; 16]),
        ),
        (
            "page-pending",
            v1::OutboxDeliveryState::Pending,
            0,
            None,
            None,
        ),
        (
            "page-delivering",
            v1::OutboxDeliveryState::Delivering,
            1,
            None,
            None,
        ),
        (
            "page-dead-letter",
            v1::OutboxDeliveryState::DeadLetter,
            3,
            None,
            None,
        ),
    ] {
        append_client_vector(
            &mut output,
            "AdminService.ListPendingOutboxDeliveries",
            "response",
            branch,
            "riffdb.v1.ListPendingOutboxDeliveriesResponse",
            &v1::ListPendingOutboxDeliveriesResponse {
                page: Some(v1::OutboxDeliveryPage {
                    items: vec![v1::OutboxDeliverySummary {
                        event_id: Some(v1::EventId {
                            commit_sequence: 1,
                            event_ordinal: 0,
                        }),
                        state: state as i32,
                        attempts,
                        next_attempt_at,
                    }],
                    next_cursor,
                }),
            },
        );
    }

    append_discover_resource_vectors(&mut output, &full_fence, &compact_fence);
    append_discovery_page_boundary_vectors(&mut output, &full_fence);
    append_wp137_public_client_registry(&mut output, descriptors)?;

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

fn message_hex(message: &impl Message) -> String {
    let mut encoded = String::new();
    for byte in message.encode_to_vec() {
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

fn append_discover_resource_vectors(
    output: &mut String,
    full_fence: &v1::DiscoveryCatalogFence,
    compact_fence: &v1::DiscoveryCatalogFence,
) {
    for (branch, representation, kind, prior_fence) in [
        (
            "full-all",
            v1::DiscoveryRepresentation::Full,
            v1::ResourceDiscoveryKind::All,
            None,
        ),
        (
            "full-concrete",
            v1::DiscoveryRepresentation::Full,
            v1::ResourceDiscoveryKind::Concrete,
            None,
        ),
        (
            "full-template",
            v1::DiscoveryRepresentation::Full,
            v1::ResourceDiscoveryKind::Template,
            None,
        ),
        (
            "compact-conditional-all",
            v1::DiscoveryRepresentation::CompactObservation,
            v1::ResourceDiscoveryKind::All,
            Some(compact_fence.clone()),
        ),
    ] {
        append_client_vector(
            output,
            "ContractService.DiscoverResources",
            "request",
            branch,
            "riffdb.v1.DiscoverResourcesRequest",
            &v1::DiscoverResourcesRequest {
                request_id: public_request_id(),
                page: Some(v1::PageRequest {
                    limit: Some(500),
                    cursor: None,
                }),
                prior_fence,
                representation: representation as i32,
                kind: kind as i32,
            },
        );
    }

    let command = || v1::CommandResource {
        contract_lineage: "budget".to_owned(),
        command_id: 1,
        contract_version: 1,
        source_command: "reserve".to_owned(),
    };
    let outcome = || v1::CommandOutcomeResource {
        contract_lineage: "budget".to_owned(),
        command_id: 1,
        tool_name: "riffdb.cmd.budget.reserve".to_owned(),
    };
    let full_items = vec![
        v1::ResourceDescriptor {
            resource: Some(v1::resource_descriptor::Resource::ActiveContract(
                v1::Unit {},
            )),
        },
        v1::ResourceDescriptor {
            resource: Some(v1::resource_descriptor::Resource::ContractVersion(
                v1::ContractVersionResource {
                    contract_lineage: "budget".to_owned(),
                    contract_version: 1,
                },
            )),
        },
        v1::ResourceDescriptor {
            resource: Some(v1::resource_descriptor::Resource::EntitySchema(
                v1::EntitySchemaResource {
                    contract_lineage: "budget".to_owned(),
                    entity_type_id: 1,
                    schema: Some(public_schema_artifact(
                        v1::schema_artifact_key::Artifact::EntityId(1),
                    )),
                },
            )),
        },
        v1::ResourceDescriptor {
            resource: Some(v1::resource_descriptor::Resource::CommandPlan(command())),
        },
        v1::ResourceDescriptor {
            resource: Some(v1::resource_descriptor::Resource::CommandDocumentation(
                command(),
            )),
        },
        v1::ResourceDescriptor {
            resource: Some(v1::resource_descriptor::Resource::CommandOutcome(outcome())),
        },
        v1::ResourceDescriptor {
            resource: Some(v1::resource_descriptor::Resource::Commit(
                v1::CommitResource {
                    target: Some(v1::commit_resource::Target::ClassTemplate(v1::Unit {})),
                },
            )),
        },
        v1::ResourceDescriptor {
            resource: Some(v1::resource_descriptor::Resource::Commit(
                v1::CommitResource {
                    target: Some(v1::commit_resource::Target::CommitSequence(1)),
                },
            )),
        },
        v1::ResourceDescriptor {
            resource: Some(v1::resource_descriptor::Resource::Provenance(
                v1::ProvenanceResource {
                    target: Some(v1::provenance_resource::Target::ClassTemplate(v1::Unit {})),
                },
            )),
        },
        v1::ResourceDescriptor {
            resource: Some(v1::resource_descriptor::Resource::Provenance(
                v1::ProvenanceResource {
                    target: Some(v1::provenance_resource::Target::ProvenanceId(
                        public_request_id(),
                    )),
                },
            )),
        },
        v1::ResourceDescriptor {
            resource: Some(v1::resource_descriptor::Resource::ProjectionStatus(
                v1::ProjectionStatusResource {
                    contract_lineage: "budget".to_owned(),
                    projection_id: 1,
                },
            )),
        },
        v1::ResourceDescriptor {
            resource: Some(v1::resource_descriptor::Resource::ServerHealth(v1::Unit {})),
        },
    ];
    append_client_vector(
        output,
        "ContractService.DiscoverResources",
        "response",
        "full-page-all-kinds",
        "riffdb.v1.DiscoverResourcesResponse",
        &v1::DiscoverResourcesResponse {
            result: Some(v1::discover_resources_response::Result::Page(
                v1::ResourceDiscoveryPage {
                    items: full_items,
                    next_cursor: None,
                    observed_fence: Some(full_fence.clone()),
                },
            )),
        },
    );

    let compact_items = vec![
        v1::CompactResourceDescriptor {
            resource: Some(v1::compact_resource_descriptor::Resource::ActiveContract(
                v1::Unit {},
            )),
        },
        v1::CompactResourceDescriptor {
            resource: Some(v1::compact_resource_descriptor::Resource::ContractVersion(
                v1::ContractVersionResource {
                    contract_lineage: "budget".to_owned(),
                    contract_version: 1,
                },
            )),
        },
        v1::CompactResourceDescriptor {
            resource: Some(v1::compact_resource_descriptor::Resource::EntitySchema(
                v1::CompactEntitySchemaResource {
                    contract_lineage: "budget".to_owned(),
                    entity_type_id: 1,
                    schema: Some(public_schema_identity(
                        v1::schema_artifact_key::Artifact::EntityId(1),
                    )),
                },
            )),
        },
        v1::CompactResourceDescriptor {
            resource: Some(v1::compact_resource_descriptor::Resource::CommandPlan(
                command(),
            )),
        },
        v1::CompactResourceDescriptor {
            resource: Some(
                v1::compact_resource_descriptor::Resource::CommandDocumentation(command()),
            ),
        },
        v1::CompactResourceDescriptor {
            resource: Some(v1::compact_resource_descriptor::Resource::CommandOutcome(
                outcome(),
            )),
        },
        v1::CompactResourceDescriptor {
            resource: Some(v1::compact_resource_descriptor::Resource::Commit(
                v1::CommitResource {
                    target: Some(v1::commit_resource::Target::ClassTemplate(v1::Unit {})),
                },
            )),
        },
        v1::CompactResourceDescriptor {
            resource: Some(v1::compact_resource_descriptor::Resource::Provenance(
                v1::ProvenanceResource {
                    target: Some(v1::provenance_resource::Target::ClassTemplate(v1::Unit {})),
                },
            )),
        },
        v1::CompactResourceDescriptor {
            resource: Some(v1::compact_resource_descriptor::Resource::ProjectionStatus(
                v1::ProjectionStatusResource {
                    contract_lineage: "budget".to_owned(),
                    projection_id: 1,
                },
            )),
        },
        v1::CompactResourceDescriptor {
            resource: Some(v1::compact_resource_descriptor::Resource::ServerHealth(
                v1::Unit {},
            )),
        },
    ];
    append_client_vector(
        output,
        "ContractService.DiscoverResources",
        "response",
        "compact-page-all-kinds",
        "riffdb.v1.DiscoverResourcesResponse",
        &v1::DiscoverResourcesResponse {
            result: Some(v1::discover_resources_response::Result::CompactPage(
                v1::CompactResourceDiscoveryPage {
                    items: compact_items,
                    next_cursor: None,
                    observed_fence: Some(compact_fence.clone()),
                },
            )),
        },
    );
    append_client_vector(
        output,
        "ContractService.DiscoverResources",
        "response",
        "catalog-unchanged",
        "riffdb.v1.DiscoverResourcesResponse",
        &v1::DiscoverResourcesResponse {
            result: Some(v1::discover_resources_response::Result::CatalogUnchanged(
                compact_fence.clone(),
            )),
        },
    );
}

fn public_boundary_command_resource(command_id: u32) -> v1::CommandResource {
    v1::CommandResource {
        contract_lineage: "budget".to_owned(),
        command_id,
        contract_version: 1,
        source_command: format!("command{command_id:04}"),
    }
}

fn public_resource_discovery_items(item_count: usize) -> Vec<v1::ResourceDescriptor> {
    (1..=item_count)
        .map(|command_id| v1::ResourceDescriptor {
            resource: Some(v1::resource_descriptor::Resource::CommandPlan(
                public_boundary_command_resource(
                    u32::try_from(command_id).expect("fixture command ID"),
                ),
            )),
        })
        .collect()
}

fn public_compact_resource_discovery_items(
    item_count: usize,
) -> Vec<v1::CompactResourceDescriptor> {
    (1..=item_count)
        .map(|command_id| v1::CompactResourceDescriptor {
            resource: Some(v1::compact_resource_descriptor::Resource::CommandPlan(
                public_boundary_command_resource(
                    u32::try_from(command_id).expect("fixture command ID"),
                ),
            )),
        })
        .collect()
}

fn append_discovery_page_boundary_vectors(output: &mut String, fence: &v1::DiscoveryCatalogFence) {
    for (suffix, item_count, cursor_present, _) in DISCOVERY_PAGE_BOUNDARIES {
        let cursor = cursor_present.then(|| vec![0x99; 16]);
        let full_branch = format!("full-boundary-{suffix}");
        append_client_vector(
            output,
            "ContractService.DiscoverCommandTools",
            "response",
            &full_branch,
            "riffdb.v1.DiscoverCommandToolsResponse",
            &v1::DiscoverCommandToolsResponse {
                result: Some(v1::discover_command_tools_response::Result::Page(
                    v1::CommandToolDiscoveryPage {
                        items: public_command_discovery_items(item_count),
                        next_cursor: cursor.clone(),
                        observed_fence: Some(fence.clone()),
                        operation_schemas: Some(public_operation_schema_catalog()),
                    },
                )),
            },
        );
        let compact_branch = format!("compact-boundary-{suffix}");
        append_client_vector(
            output,
            "ContractService.DiscoverCommandTools",
            "response",
            &compact_branch,
            "riffdb.v1.DiscoverCommandToolsResponse",
            &v1::DiscoverCommandToolsResponse {
                result: Some(v1::discover_command_tools_response::Result::CompactPage(
                    v1::CompactCommandToolDiscoveryPage {
                        items: public_compact_command_discovery_items(item_count),
                        next_cursor: cursor.clone(),
                        observed_fence: Some(fence.clone()),
                    },
                )),
            },
        );
        append_client_vector(
            output,
            "ContractService.DiscoverResources",
            "response",
            &full_branch,
            "riffdb.v1.DiscoverResourcesResponse",
            &v1::DiscoverResourcesResponse {
                result: Some(v1::discover_resources_response::Result::Page(
                    v1::ResourceDiscoveryPage {
                        items: public_resource_discovery_items(item_count),
                        next_cursor: cursor.clone(),
                        observed_fence: Some(fence.clone()),
                    },
                )),
            },
        );
        append_client_vector(
            output,
            "ContractService.DiscoverResources",
            "response",
            &compact_branch,
            "riffdb.v1.DiscoverResourcesResponse",
            &v1::DiscoverResourcesResponse {
                result: Some(v1::discover_resources_response::Result::CompactPage(
                    v1::CompactResourceDiscoveryPage {
                        items: public_compact_resource_discovery_items(item_count),
                        next_cursor: cursor,
                        observed_fence: Some(fence.clone()),
                    },
                )),
            },
        );
    }
}

fn pre_wp137_public_symbols() -> Result<BTreeSet<String>, io::Error> {
    let mut lines = PRE_WP137_PUBLIC_SCHEMA_HASHES.lines();
    if lines.next() != Some("riffdb-public-schema-hashes-v1") {
        return Err(io::Error::other(
            "unexpected pre-WP-137 public schema fixture header",
        ));
    }
    lines
        .map(|line| {
            let fields = line.split_whitespace().collect::<Vec<_>>();
            if fields.len() != 3 {
                return Err(io::Error::other(
                    "invalid pre-WP-137 public schema fixture row",
                ));
            }
            Ok(format!("{} {}", fields[0], fields[1]))
        })
        .collect()
}

fn append_wp137_public_client_registry(
    output: &mut String,
    descriptors: &FileDescriptorSet,
) -> Result<(), io::Error> {
    let baseline = pre_wp137_public_symbols()?;
    let mut registry = BTreeSet::new();
    let mut enums = Vec::new();
    for file in descriptors
        .file
        .iter()
        .filter(|file| file.package() == "riffdb.v1")
    {
        collect_enums(
            file.package(),
            &file.enum_type,
            &file.message_type,
            &mut enums,
        );
    }
    for (name, enumeration) in enums {
        let new_values = enumeration
            .value
            .iter()
            .filter(|value| !baseline.contains(&format!("enum-value {name}.{}", value.name())))
            .collect::<Vec<_>>();
        for value in new_values {
            registry.insert(format!(
                "enum-value {name} {} {}",
                value.number(),
                value.name()
            ));
        }
    }

    let optional_coverage = WP137_OPTIONAL_COVERAGE
        .into_iter()
        .map(|(field, absent, present)| (field, (absent, present)))
        .collect::<BTreeMap<_, _>>();
    let mut discovered_optional_fields = BTreeSet::new();
    let mut messages = Vec::new();
    for file in descriptors
        .file
        .iter()
        .filter(|file| file.package() == "riffdb.v1")
    {
        collect_messages(file.package(), &file.message_type, &mut messages);
    }
    for (message_name, message) in messages {
        for field in &message.field {
            let field_name = format!("{message_name}.{}", field.name());
            if field.proto3_optional() && !baseline.contains(&format!("field {field_name}")) {
                discovered_optional_fields.insert(field_name);
            }
        }
    }
    if discovered_optional_fields
        != optional_coverage
            .keys()
            .map(|field| (*field).to_owned())
            .collect()
    {
        return Err(io::Error::other(
            "WP-137 optional-field coverage registry is incomplete",
        ));
    }
    for field in discovered_optional_fields {
        let (absent, present) = optional_coverage[&field.as_str()];
        registry.insert(format!("optional {field} {absent} {present}"));
    }

    for (rpc, representation, message) in [
        (
            "ContractService.DiscoverCommandTools",
            "full",
            "riffdb.v1.CommandToolDiscoveryPage",
        ),
        (
            "ContractService.DiscoverCommandTools",
            "compact",
            "riffdb.v1.CompactCommandToolDiscoveryPage",
        ),
        (
            "ContractService.DiscoverResources",
            "full",
            "riffdb.v1.ResourceDiscoveryPage",
        ),
        (
            "ContractService.DiscoverResources",
            "compact",
            "riffdb.v1.CompactResourceDiscoveryPage",
        ),
    ] {
        for (suffix, item_count, cursor_present, end) in DISCOVERY_PAGE_BOUNDARIES {
            let cursor = if cursor_present { "present" } else { "absent" };
            registry.insert(format!(
                "page {message} {rpc}:response:{representation}-boundary-{suffix} items={item_count} cursor={cursor} end={end}"
            ));
        }
    }

    let fence = public_discovery_fence(true);
    for (enumeration, message_type, encoded) in [
        (
            "riffdb.v1.FixedToolKind",
            "riffdb.v1.DiscoverCommandToolsResponse",
            message_hex(&v1::DiscoverCommandToolsResponse {
                result: Some(v1::discover_command_tools_response::Result::CompactPage(
                    v1::CompactCommandToolDiscoveryPage {
                        items: vec![v1::CompactCommandToolDiscoveryItem {
                            item: Some(v1::compact_command_tool_discovery_item::Item::FixedTool(
                                v1::FixedToolKind::Unspecified as i32,
                            )),
                        }],
                        next_cursor: None,
                        observed_fence: Some(fence.clone()),
                    },
                )),
            }),
        ),
        (
            "riffdb.v1.DiscoveryRepresentation",
            "riffdb.v1.DiscoverCommandToolsRequest",
            message_hex(&v1::DiscoverCommandToolsRequest {
                request_id: public_request_id(),
                page: Some(v1::PageRequest {
                    limit: Some(1),
                    cursor: None,
                }),
                prior_fence: None,
                representation: v1::DiscoveryRepresentation::Unspecified as i32,
            }),
        ),
        (
            "riffdb.v1.ResourceDiscoveryKind",
            "riffdb.v1.DiscoverResourcesRequest",
            message_hex(&v1::DiscoverResourcesRequest {
                request_id: public_request_id(),
                page: Some(v1::PageRequest {
                    limit: Some(1),
                    cursor: None,
                }),
                prior_fence: None,
                representation: v1::DiscoveryRepresentation::Full as i32,
                kind: v1::ResourceDiscoveryKind::Unspecified as i32,
            }),
        ),
        (
            "riffdb.v1.OutboxDeliveryState",
            "riffdb.v1.ListPendingOutboxDeliveriesResponse",
            message_hex(&v1::ListPendingOutboxDeliveriesResponse {
                page: Some(v1::OutboxDeliveryPage {
                    items: vec![v1::OutboxDeliverySummary {
                        event_id: Some(v1::EventId {
                            commit_sequence: 1,
                            event_ordinal: 0,
                        }),
                        state: v1::OutboxDeliveryState::Unspecified as i32,
                        attempts: 0,
                        next_attempt_at: None,
                    }],
                    next_cursor: None,
                }),
            }),
        ),
    ] {
        registry.insert(format!(
            "enum-rejection {enumeration} {message_type} {encoded} invalid_enum"
        ));
    }

    output.push_str("riffdb-public-client-registry-v1\n");
    for row in registry {
        output.push_str(&row);
        output.push('\n');
    }
    Ok(())
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
    response_charge_contract_descriptor("budget")
}

fn response_charge_contract_descriptor(lineage: &str) -> v1::ContractDescriptor {
    v1::ContractDescriptor {
        contract_lineage: lineage.to_owned(),
        contract_version: 1,
        bundle_hash: vec![0x11; 32],
        source_hash: vec![0x22; 32],
        plan_root_hash: vec![0x33; 32],
        compatibility: Some(v1::ContractCompatibilitySummary {
            parent_contract_version: None,
            parent_bundle_hash: None,
            overall: v1::ContractCompatibilityClass::Compatible as i32,
            code_counts: Vec::new(),
        }),
    }
}

fn public_successor_contract_descriptor() -> v1::ContractDescriptor {
    v1::ContractDescriptor {
        contract_lineage: "budget".to_owned(),
        contract_version: 2,
        bundle_hash: vec![0x44; 32],
        source_hash: vec![0x55; 32],
        plan_root_hash: vec![0x66; 32],
        compatibility: Some(v1::ContractCompatibilitySummary {
            parent_contract_version: Some(1),
            parent_bundle_hash: Some(vec![0x11; 32]),
            overall: v1::ContractCompatibilityClass::Compatible as i32,
            code_counts: vec![v1::ContractCompatibilityCodeCount {
                code: "RDB-K010".to_owned(),
                count: 1,
            }],
        }),
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

fn public_schema_identity(key: v1::schema_artifact_key::Artifact) -> v1::GeneratedSchemaIdentity {
    let artifact = public_schema_artifact(key);
    v1::GeneratedSchemaIdentity {
        key: artifact.key,
        schema_hash: artifact.schema_hash,
    }
}

fn public_operation_schema_catalog() -> v1::OperationSchemaCatalog {
    let artifact = |schema_id: &str, canonical_json: &str| v1::OperationSchemaArtifact {
        schema_id: schema_id.to_owned(),
        dialect: OPERATION_SCHEMA_DIALECT.to_owned(),
        schema_hash: hash_schema(canonical_json.as_bytes()).as_bytes().to_vec(),
        canonical_json: canonical_json.to_owned(),
    };
    let envelope = include_str!(
        "../../riffdb-service/schema/riffdb.command-operation-envelope-v1.schema.json"
    );
    let get_outcome = include_str!(
        "../../riffdb-service/schema/riffdb.command-get-outcome-result-v1.schema.json"
    );
    v1::OperationSchemaCatalog {
        command_operation_envelope: Some(artifact(OPERATION_ENVELOPE_SCHEMA_ID, envelope)),
        command_get_outcome_result: Some(artifact(GET_OUTCOME_RESULT_SCHEMA_ID, get_outcome)),
    }
}

fn public_operation_schema_identity() -> v1::OperationSchemaCatalogIdentity {
    let catalog = public_operation_schema_catalog();
    let identity = |artifact: Option<v1::OperationSchemaArtifact>| {
        artifact.map(|artifact| v1::OperationSchemaIdentity {
            schema_id: artifact.schema_id,
            schema_hash: artifact.schema_hash,
        })
    };
    v1::OperationSchemaCatalogIdentity {
        command_operation_envelope: identity(catalog.command_operation_envelope),
        command_get_outcome_result: identity(catalog.command_get_outcome_result),
    }
}

fn public_discovery_fence(active: bool) -> v1::DiscoveryCatalogFence {
    public_discovery_fence_for(active, "budget")
}

fn public_discovery_fence_for(active: bool, lineage: &str) -> v1::DiscoveryCatalogFence {
    public_discovery_fence_for_version(active, lineage, 1)
}

fn public_discovery_fence_for_version(
    active: bool,
    lineage: &str,
    contract_version: u64,
) -> v1::DiscoveryCatalogFence {
    v1::DiscoveryCatalogFence {
        state: Some(if active {
            v1::discovery_catalog_fence::State::ActiveContract(v1::ActiveDiscoveryCatalogFence {
                contract_lineage: lineage.to_owned(),
                contract_version,
                bundle_hash: vec![0x11; 32],
            })
        } else {
            v1::discovery_catalog_fence::State::NoActiveContract(v1::Unit {})
        }),
        server_generation: vec![0x44; 16],
        operation_schemas: Some(public_operation_schema_identity()),
    }
}

fn public_command_tool_descriptor() -> v1::CommandToolDescriptor {
    v1::CommandToolDescriptor {
        tool_name: "riffdb.cmd.budget.reserve".to_owned(),
        source_command: "reserve".to_owned(),
        contract_lineage: "budget".to_owned(),
        contract_version: 1,
        command_id: 1,
        input_schema: Some(public_schema_artifact(
            v1::schema_artifact_key::Artifact::CommandInputId(1),
        )),
        outcome_schema: Some(public_schema_artifact(
            v1::schema_artifact_key::Artifact::CommandOutcomeUnionId(1),
        )),
    }
}

fn public_compact_command_tool_descriptor() -> v1::CompactCommandToolDescriptor {
    v1::CompactCommandToolDescriptor {
        tool_name: "riffdb.cmd.budget.reserve".to_owned(),
        source_command: "reserve".to_owned(),
        contract_lineage: "budget".to_owned(),
        contract_version: 1,
        command_id: 1,
        input_schema: Some(public_schema_identity(
            v1::schema_artifact_key::Artifact::CommandInputId(1),
        )),
        outcome_schema: Some(public_schema_identity(
            v1::schema_artifact_key::Artifact::CommandOutcomeUnionId(1),
        )),
    }
}

fn public_fixed_tool_kinds() -> [v1::FixedToolKind; 14] {
    [
        v1::FixedToolKind::ValidateContract,
        v1::FixedToolKind::GetActiveContract,
        v1::FixedToolKind::ExplainCommand,
        v1::FixedToolKind::DeployContract,
        v1::FixedToolKind::ResolveCommandOutcome,
        v1::FixedToolKind::GetEntity,
        v1::FixedToolKind::ScanIndex,
        v1::FixedToolKind::GetCommit,
        v1::FixedToolKind::ScanCommits,
        v1::FixedToolKind::TraceProvenance,
        v1::FixedToolKind::QueryProjection,
        v1::FixedToolKind::GetProjectionStatus,
        v1::FixedToolKind::ListPendingOutboxDeliveries,
        v1::FixedToolKind::GetHealth,
    ]
}

fn public_boundary_command_tool_descriptor(command_id: u32) -> v1::CommandToolDescriptor {
    let source_command = format!("command{command_id:04}");
    v1::CommandToolDescriptor {
        tool_name: format!("riffdb.cmd.budget.{source_command}"),
        source_command,
        contract_lineage: "budget".to_owned(),
        contract_version: 1,
        command_id,
        input_schema: Some(public_schema_artifact(
            v1::schema_artifact_key::Artifact::CommandInputId(command_id),
        )),
        outcome_schema: Some(public_schema_artifact(
            v1::schema_artifact_key::Artifact::CommandOutcomeUnionId(command_id),
        )),
    }
}

fn public_boundary_compact_command_tool_descriptor(
    command_id: u32,
) -> v1::CompactCommandToolDescriptor {
    let source_command = format!("command{command_id:04}");
    v1::CompactCommandToolDescriptor {
        tool_name: format!("riffdb.cmd.budget.{source_command}"),
        source_command,
        contract_lineage: "budget".to_owned(),
        contract_version: 1,
        command_id,
        input_schema: Some(public_schema_identity(
            v1::schema_artifact_key::Artifact::CommandInputId(command_id),
        )),
        outcome_schema: Some(public_schema_identity(
            v1::schema_artifact_key::Artifact::CommandOutcomeUnionId(command_id),
        )),
    }
}

fn public_command_discovery_items(item_count: usize) -> Vec<v1::CommandToolDiscoveryItem> {
    let fixed_tools = public_fixed_tool_kinds();
    let fixed_count = item_count.min(fixed_tools.len());
    fixed_tools
        .into_iter()
        .take(fixed_count)
        .map(|kind| v1::CommandToolDiscoveryItem {
            item: Some(v1::command_tool_discovery_item::Item::FixedTool(
                kind as i32,
            )),
        })
        .chain(
            (1..=item_count - fixed_count).map(|command_id| v1::CommandToolDiscoveryItem {
                item: Some(v1::command_tool_discovery_item::Item::CommandTool(
                    public_boundary_command_tool_descriptor(
                        u32::try_from(command_id).expect("fixture command ID"),
                    ),
                )),
            }),
        )
        .collect()
}

fn public_compact_command_discovery_items(
    item_count: usize,
) -> Vec<v1::CompactCommandToolDiscoveryItem> {
    let fixed_tools = public_fixed_tool_kinds();
    let fixed_count = item_count.min(fixed_tools.len());
    fixed_tools
        .into_iter()
        .take(fixed_count)
        .map(|kind| v1::CompactCommandToolDiscoveryItem {
            item: Some(v1::compact_command_tool_discovery_item::Item::FixedTool(
                kind as i32,
            )),
        })
        .chain((1..=item_count - fixed_count).map(|command_id| {
            v1::CompactCommandToolDiscoveryItem {
                item: Some(v1::compact_command_tool_discovery_item::Item::CommandTool(
                    public_boundary_compact_command_tool_descriptor(
                        u32::try_from(command_id).expect("fixture command ID"),
                    ),
                )),
            }
        }))
        .collect()
}

fn response_charge_schema(
    key: v1::schema_artifact_key::Artifact,
    json_bytes: usize,
) -> v1::GeneratedSchemaArtifact {
    let canonical_json = response_charge_json(json_bytes);
    v1::GeneratedSchemaArtifact {
        key: Some(v1::SchemaArtifactKey {
            artifact: Some(key),
        }),
        dialect: "https://json-schema.org/draft/2020-12/schema".to_owned(),
        schema_hash: hash_schema(canonical_json.as_bytes()).as_bytes().to_vec(),
        canonical_json,
    }
}

fn response_charge_command_tool_descriptor(
    command_id: u32,
    lineage: &str,
    source_command: &str,
    input_schema_json_bytes: usize,
    outcome_schema_json_bytes: usize,
) -> v1::CommandToolDescriptor {
    v1::CommandToolDescriptor {
        tool_name: format!("riffdb.cmd.{lineage}.{source_command}"),
        source_command: source_command.to_owned(),
        contract_lineage: lineage.to_owned(),
        contract_version: 1,
        command_id,
        input_schema: Some(response_charge_schema(
            v1::schema_artifact_key::Artifact::CommandInputId(command_id),
            input_schema_json_bytes,
        )),
        outcome_schema: Some(response_charge_schema(
            v1::schema_artifact_key::Artifact::CommandOutcomeUnionId(command_id),
            outcome_schema_json_bytes,
        )),
    }
}

fn response_charge_representative_command_tool_descriptor() -> v1::CommandToolDescriptor {
    let artifact = |key, fixture: &str| {
        let canonical_json = fixture
            .strip_suffix('\n')
            .expect("compiler schema fixture ends in one LF")
            .to_owned();
        v1::GeneratedSchemaArtifact {
            key: Some(v1::SchemaArtifactKey {
                artifact: Some(key),
            }),
            dialect: "https://json-schema.org/draft/2020-12/schema".to_owned(),
            schema_hash: hash_schema(canonical_json.as_bytes()).as_bytes().to_vec(),
            canonical_json,
        }
    };
    v1::CommandToolDescriptor {
        tool_name: "riffdb.cmd.legalspend.createbudget".to_owned(),
        source_command: "CreateBudget".to_owned(),
        contract_lineage: "LegalSpend".to_owned(),
        contract_version: 1,
        command_id: 1,
        input_schema: Some(artifact(
            v1::schema_artifact_key::Artifact::CommandInputId(1),
            include_str!("../../../fixtures/compiler/schemas/03-00000001.json"),
        )),
        outcome_schema: Some(artifact(
            v1::schema_artifact_key::Artifact::CommandOutcomeUnionId(1),
            include_str!("../../../fixtures/compiler/schemas/04-00000001.json"),
        )),
    }
}

fn response_charge_compact_command_tool_descriptor_for(
    command_id: u32,
    contract_lineage: &str,
    source_command: &str,
) -> v1::CompactCommandToolDescriptor {
    let identity = |artifact| v1::GeneratedSchemaIdentity {
        key: Some(v1::SchemaArtifactKey {
            artifact: Some(artifact),
        }),
        schema_hash: vec![0x77; 32],
    };
    v1::CompactCommandToolDescriptor {
        tool_name: format!("riffdb.cmd.{contract_lineage}.{source_command}"),
        source_command: source_command.to_owned(),
        contract_lineage: contract_lineage.to_owned(),
        contract_version: u64::MAX,
        command_id,
        input_schema: Some(identity(v1::schema_artifact_key::Artifact::CommandInputId(
            command_id,
        ))),
        outcome_schema: Some(identity(
            v1::schema_artifact_key::Artifact::CommandOutcomeUnionId(command_id),
        )),
    }
}

fn response_charge_compact_command_name(index: usize) -> String {
    assert!(index < 26 * 26 * 26);
    let letters = [
        u8::try_from(index / (26 * 26)).expect("fixture command-name component"),
        u8::try_from((index / 26) % 26).expect("fixture command-name component"),
        u8::try_from(index % 26).expect("fixture command-name component"),
    ];
    letters
        .into_iter()
        .map(|value| char::from(b'a' + value))
        .collect()
}

fn response_charge_compact_resource_descriptor_for(
    command_id: u32,
    contract_version: u64,
    lineage_bytes: usize,
    source_command_bytes: usize,
) -> v1::CompactResourceDescriptor {
    let mut source_command = format!("command_{command_id:010}");
    assert!(source_command.len() <= source_command_bytes);
    source_command.push_str(&"x".repeat(source_command_bytes - source_command.len()));
    v1::CompactResourceDescriptor {
        resource: Some(v1::compact_resource_descriptor::Resource::CommandPlan(
            v1::CommandResource {
                contract_lineage: "r".repeat(lineage_bytes),
                command_id,
                contract_version,
                source_command,
            },
        )),
    }
}

fn public_projection_status() -> v1::ProjectionStatus {
    v1::ProjectionStatus {
        identity: Some(public_projection_identity()),
        lifecycle: v1::ProjectionLifecycle::Ready as i32,
        published: Some(v1::ProjectionGenerationFrontier {
            generation: 1,
            frontier: Some(public_applied(1)),
        }),
        candidate: None,
        published_apply_mode: Some(v1::PublishedApplyMode::Enabled as i32),
        failure: None,
        authoritative_head: Some(public_applied(1)),
    }
}

fn public_provenance() -> v1::Provenance {
    v1::Provenance {
        provenance_id: public_request_id(),
        commit_sequence: 1,
        admission_request_id: public_request_id(),
        contract_lineage: "budget".to_owned(),
        contract_version: 1,
        command_id: 1,
        plan_hash: vec![0x66; 32],
        actor: Some(v1::AdmittedActor {
            principal_id: "principal".to_owned(),
            actor_kind: v1::ActorKind::Human as i32,
            tenant_scope: Some(v1::TenantScope {
                scope: Some(v1::tenant_scope::Scope::Global(v1::Unit {})),
            }),
            agent_session_id: None,
        }),
        logical_time: Some(v1::Timestamp {
            seconds: 1,
            nanos: 0,
        }),
        outcome_id: 1,
        affected_entities: vec![v1::AffectedEntity {
            entity_key: public_entity_key(1),
            entity_version: 1,
        }],
        event_ids: vec![v1::EventId {
            commit_sequence: 1,
            event_ordinal: 0,
        }],
        claims: Some(v1::ProvenanceClaims {
            source_repository: Some("repo".to_owned()),
            source_commit: Some("abc123".to_owned()),
            reason: Some("approved".to_owned()),
            approval_id: Some("approval-1".to_owned()),
        }),
    }
}

fn public_provenance_without_claims() -> v1::Provenance {
    let mut provenance = public_provenance();
    provenance.claims = Some(v1::ProvenanceClaims {
        source_repository: None,
        source_commit: None,
        reason: None,
        approval_id: None,
    });
    provenance
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
        outcome_uri: (!read_only).then(public_outcome_uri),
    }
}

fn public_outcome_uri() -> String {
    let mut digest_tuple = vec![1];
    digest_tuple.extend_from_slice(&1_u32.to_be_bytes());
    digest_tuple.extend_from_slice(&[0x77; 32]);
    format!(
        "riffdb://outcome/principal/budget/1/riffdb.cmd.budget.reserve/{}",
        URL_SAFE_NO_PAD.encode(digest_tuple)
    )
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
        "riffdb_public_response_charge_fixture_version\t1\nservice_response_charge_version\t1\nceiling_bytes\t4194304\ncase_id\tresponse_family\tresponse_variant\tshape_v1\tservice_charge_bytes\tprotobuf_encoded_bytes\tdisposition\n",
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
        let variant = columns[2];
        let shape = parse_response_charge_shape(columns[3])?;
        let charge = columns[4].parse::<usize>()?;
        let disposition = columns[5];
        let ceiling = response_charge_ceiling(case_id);
        let encoded = response_charge_candidate(case_id, variant, &shape)?;
        if let Some(encoded) = &encoded {
            if encoded.len() > charge {
                return Err(io::Error::other(format!(
                    "public encoding for {case_id} exceeds service charge: {} > {charge}",
                    encoded.len()
                ))
                .into());
            }
            match disposition {
                "release" if charge <= ceiling && encoded.len() <= ceiling => {
                    validate_response_charge_candidate(case_id, encoded, false)?;
                }
                "response_too_large" if charge > ceiling => {
                    validate_response_charge_candidate(case_id, encoded, true)?;
                }
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
            "{case_id}\t{family}\t{variant}\t{}\t{charge}\t{encoded}\t{disposition}",
            columns[3]
        );
    }
    Ok(output)
}

fn parse_response_charge_shape(shape: &str) -> Result<BTreeMap<String, String>, io::Error> {
    if shape == "none" {
        return Ok(BTreeMap::new());
    }
    let mut parsed = BTreeMap::new();
    for entry in shape.split(',') {
        let (key, value) = entry
            .split_once('=')
            .ok_or_else(|| io::Error::other("response-charge shape entry has no value"))?;
        if key.is_empty()
            || value.is_empty()
            || parsed.insert(key.to_owned(), value.to_owned()).is_some()
        {
            return Err(io::Error::other("invalid response-charge shape"));
        }
    }
    let canonical = parsed
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(",");
    if canonical != shape {
        return Err(io::Error::other("response-charge shape is not canonical"));
    }
    Ok(parsed)
}

fn response_shape_usize(shape: &BTreeMap<String, String>, key: &str) -> Result<usize, io::Error> {
    shape
        .get(key)
        .ok_or_else(|| io::Error::other(format!("response-charge shape omitted {key}")))?
        .parse::<usize>()
        .map_err(|_| io::Error::other(format!("response-charge shape has invalid {key}")))
}

fn response_shape_value<'a>(
    shape: &'a BTreeMap<String, String>,
    key: &str,
) -> Result<&'a str, io::Error> {
    shape
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| io::Error::other(format!("response-charge shape omitted {key}")))
}

fn response_shape_bool(shape: &BTreeMap<String, String>, key: &str) -> Result<bool, io::Error> {
    match response_shape_value(shape, key)? {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(io::Error::other(format!(
            "response-charge shape has invalid {key}"
        ))),
    }
}

fn response_shape_u64(shape: &BTreeMap<String, String>, key: &str) -> Result<u64, io::Error> {
    response_shape_value(shape, key)?
        .parse::<u64>()
        .map_err(|_| io::Error::other(format!("response-charge shape has invalid {key}")))
}

fn response_shape_u32(shape: &BTreeMap<String, String>, key: &str) -> Result<u32, io::Error> {
    response_shape_value(shape, key)?
        .parse::<u32>()
        .map_err(|_| io::Error::other(format!("response-charge shape has invalid {key}")))
}

fn require_response_shape_keys(
    shape: &BTreeMap<String, String>,
    expected: &[&str],
) -> Result<(), io::Error> {
    let expected_keys = expected.iter().copied().collect::<BTreeSet<_>>();
    let actual_keys = shape.keys().map(String::as_str).collect::<BTreeSet<_>>();
    if expected_keys.len() != expected.len() || actual_keys != expected_keys {
        return Err(io::Error::other(format!(
            "response-charge shape keys disagree: expected {expected_keys:?}, got {actual_keys:?}"
        )));
    }
    Ok(())
}

fn require_response_shape_value(
    shape: &BTreeMap<String, String>,
    key: &str,
    expected: &str,
) -> Result<(), io::Error> {
    let actual = response_shape_value(shape, key)?;
    if actual != expected {
        return Err(io::Error::other(format!(
            "response-charge shape {key} must be {expected}, got {actual}"
        )));
    }
    Ok(())
}

fn require_response_shape_usize(
    shape: &BTreeMap<String, String>,
    key: &str,
    expected: usize,
) -> Result<(), io::Error> {
    let actual = response_shape_usize(shape, key)?;
    if actual != expected {
        return Err(io::Error::other(format!(
            "response-charge shape {key} must be {expected}, got {actual}"
        )));
    }
    Ok(())
}

fn response_charge_expected_variant(case_id: &str) -> Option<&str> {
    match case_id {
        "commit_notification.commit_oversize" | "commit_notification.commit_representative" => {
            Some("commit")
        }
        "contract_validation.invalid_empty_source" => Some("invalid"),
        "create_capability.created" => Some("normal_created"),
        "explain_command.found_budget" => Some("found"),
        "get_commit.found_oversize" | "get_commit.found_representative" => Some("found"),
        "get_entity.found_record" => Some("found"),
        "get_outcome.found_replayed" => Some("found"),
        "projection_status.uninitialized" => Some("found"),
        "query_projection.ready_page" => Some("ready"),
        "scan_commits.page_with_cursor" | "scan_index.page_with_cursor" => Some("page"),
        "trace_provenance.found_max_claims" => Some("found"),
        _ => case_id.rsplit_once('.').map(|(_, suffix)| suffix),
    }
}

fn response_charge_ceiling(case_id: &str) -> usize {
    if case_id.starts_with("discover_command_tools.full_")
        || case_id.starts_with("discover_resources.full_")
    {
        2_621_440
    } else {
        4_194_304
    }
}

fn validate_response_charge_candidate(
    case_id: &str,
    encoded: &[u8],
    allow_message_too_large: bool,
) -> Result<(), io::Error> {
    macro_rules! decode {
        ($type:ty) => {
            if allow_message_too_large {
                <$type>::decode(encoded)
                    .map_err(|_| PublicWireError::MalformedEncoding)
                    .and_then(|message| match validate_public_message(&message) {
                        Ok(()) | Err(PublicWireError::MessageTooLarge) => Ok(()),
                        Err(error) => Err(error),
                    })
            } else {
                decode_public_message::<$type>(encoded).map(|_| ())
            }
        };
    }
    let result = match case_id {
        "boundary.exact_ceiling"
        | "boundary.one_over"
        | "contract_validation.invalid_empty_source"
        | "contract_validation.valid" => decode!(v1::ValidateContractResponse),
        value if value.starts_with("commit_notification.") => decode!(v1::CommitNotification),
        value if value.starts_with("create_capability.") => decode!(v1::CreateCapabilityResponse),
        value if value.starts_with("deploy_contract.") => decode!(v1::DeployContractResponse),
        value if value.starts_with("discover_command_tools.") => {
            decode!(v1::DiscoverCommandToolsResponse)
        }
        value if value.starts_with("operation_schema_catalog.") => {
            v1::OperationSchemaCatalog::decode(encoded)
                .map_err(|_| PublicWireError::MalformedEncoding)
                .and_then(|catalog| {
                    if catalog == public_operation_schema_catalog() {
                        Ok(())
                    } else {
                        Err(PublicWireError::InconsistentFields)
                    }
                })
        }
        value if value.starts_with("discover_resources.") => {
            decode!(v1::DiscoverResourcesResponse)
        }
        value if value.starts_with("execute_command.") => decode!(v1::ExecuteCommandResponse),
        value if value.starts_with("explain_command.") => decode!(v1::ExplainCommandResponse),
        value if value.starts_with("get_active_contract.") => {
            decode!(v1::GetActiveContractResponse)
        }
        value if value.starts_with("get_commit.") => decode!(v1::GetCommitResponse),
        value if value.starts_with("get_contract_version.") => {
            decode!(v1::GetContractVersionResponse)
        }
        value if value.starts_with("get_entity.") => decode!(v1::GetEntityResponse),
        value if value.starts_with("get_outcome.") => decode!(v1::GetOutcomeResponse),
        value if value.starts_with("health.") => decode!(v1::HealthResponse),
        value if value.starts_with("list_pending_outbox_deliveries.") => {
            decode!(v1::ListPendingOutboxDeliveriesResponse)
        }
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
        value if value.starts_with("trace_provenance.") => {
            decode!(v1::TraceProvenanceResponse)
        }
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

fn response_charge_candidate(
    case_id: &str,
    variant: &str,
    shape: &BTreeMap<String, String>,
) -> Result<Option<Vec<u8>>, Box<dyn Error>> {
    if variant.is_empty()
        || !variant
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(io::Error::other("invalid response-charge variant").into());
    }
    if response_charge_expected_variant(case_id) != Some(variant) {
        return Err(io::Error::other(format!(
            "response-charge variant disagrees with case ID {case_id}"
        ))
        .into());
    }
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
        "discover_command_tools.catalog_unchanged" => {
            require_response_shape_keys(
                shape,
                &["catalog_state", "lineage_bytes", "representation"],
            )?;
            require_response_shape_value(shape, "catalog_state", "active_contract")?;
            require_response_shape_value(shape, "representation", "compact_observation")?;
            v1::DiscoverCommandToolsResponse {
                result: Some(
                    v1::discover_command_tools_response::Result::CatalogUnchanged(
                        public_discovery_fence_for(
                            true,
                            &"a".repeat(response_shape_usize(shape, "lineage_bytes")?),
                        ),
                    ),
                ),
            }
            .encode_to_vec()
        }
        "discover_command_tools.compact_item_max" => {
            require_response_shape_keys(
                shape,
                &[
                    "command_id",
                    "contract_version",
                    "cursor_present",
                    "item_charge_bytes",
                    "item_count",
                    "lineage_bytes",
                    "representation",
                    "source_command_bytes",
                    "tool_name_bytes",
                ],
            )?;
            require_response_shape_value(shape, "representation", "compact_observation")?;
            require_response_shape_usize(shape, "item_charge_bytes", 654)?;
            require_response_shape_usize(shape, "item_count", 1)?;
            if response_shape_bool(shape, "cursor_present")? {
                return Err(io::Error::other("compact-item witness must be an exact end").into());
            }
            let command_id = response_shape_u32(shape, "command_id")?;
            let contract_version = response_shape_u64(shape, "contract_version")?;
            let lineage = "a".repeat(response_shape_usize(shape, "lineage_bytes")?);
            let source_command = "c".repeat(response_shape_usize(shape, "source_command_bytes")?);
            let mut descriptor = response_charge_compact_command_tool_descriptor_for(
                command_id,
                &lineage,
                &source_command,
            );
            descriptor.contract_version = contract_version;
            require_response_shape_usize(
                shape,
                "tool_name_bytes",
                descriptor.tool_name.len(),
            )?;
            v1::DiscoverCommandToolsResponse {
                result: Some(v1::discover_command_tools_response::Result::CompactPage(
                    v1::CompactCommandToolDiscoveryPage {
                        items: vec![v1::CompactCommandToolDiscoveryItem {
                            item: Some(
                                v1::compact_command_tool_discovery_item::Item::CommandTool(
                                    descriptor,
                                ),
                            ),
                        }],
                        next_cursor: None,
                        observed_fence: Some(public_discovery_fence_for_version(
                            true,
                            &lineage,
                            contract_version,
                        )),
                    },
                )),
            }
            .encode_to_vec()
        }
        "discover_command_tools.compact_page_max" => {
            require_response_shape_keys(
                shape,
                &[
                    "command_id_first",
                    "command_id_last",
                    "contract_version",
                    "cursor_bytes",
                    "item_charge_bytes",
                    "item_count",
                    "lineage_bytes",
                    "representation",
                    "source_command_bytes",
                ],
            )?;
            require_response_shape_value(shape, "representation", "compact_observation")?;
            require_response_shape_usize(shape, "item_charge_bytes", 654)?;
            let item_count = response_shape_usize(shape, "item_count")?;
            let first_command_id = response_shape_u32(shape, "command_id_first")?;
            let last_command_id = response_shape_u32(shape, "command_id_last")?;
            let represented_count = last_command_id
                .checked_sub(first_command_id)
                .and_then(|difference| difference.checked_add(1))
                .map(usize::try_from)
                .transpose()?
                .ok_or_else(|| io::Error::other("invalid compact command-ID range"))?;
            if represented_count != item_count {
                return Err(io::Error::other(
                    "compact command-ID range disagrees with item count",
                )
                .into());
            }
            let contract_version = response_shape_u64(shape, "contract_version")?;
            let lineage = "a".repeat(response_shape_usize(shape, "lineage_bytes")?);
            let source_command_bytes = response_shape_usize(shape, "source_command_bytes")?;
            v1::DiscoverCommandToolsResponse {
                result: Some(v1::discover_command_tools_response::Result::CompactPage(
                    v1::CompactCommandToolDiscoveryPage {
                        items: (0..item_count)
                            .map(|index| {
                                let source_command = response_charge_compact_command_name(index);
                                assert_eq!(source_command.len(), source_command_bytes);
                                let command_id = first_command_id
                                    + u32::try_from(index).expect("fixture command-ID offset");
                                let mut descriptor =
                                    response_charge_compact_command_tool_descriptor_for(
                                        command_id,
                                        &lineage,
                                        &source_command,
                                    );
                                descriptor.contract_version = contract_version;
                                v1::CompactCommandToolDiscoveryItem {
                                    item: Some(
                                        v1::compact_command_tool_discovery_item::Item::CommandTool(
                                            descriptor,
                                        ),
                                    ),
                                }
                            })
                            .collect(),
                        next_cursor: Some(vec![
                            0x55;
                            response_shape_usize(shape, "cursor_bytes")?
                        ]),
                        observed_fence: Some(public_discovery_fence_for_version(
                            true,
                            &lineage,
                            contract_version,
                        )),
                    },
                )),
            }
            .encode_to_vec()
        }
        "discover_command_tools.full_exact_ceiling"
        | "discover_command_tools.full_one_over" => {
            require_response_shape_keys(
                shape,
                &[
                    "cursor_bytes",
                    "item_0_input_schema_json_bytes",
                    "item_0_outcome_schema_json_bytes",
                    "item_1_input_schema_json_bytes",
                    "item_1_outcome_schema_json_bytes",
                    "item_count",
                    "lineage_bytes",
                    "representation",
                    "source_command_bytes_per_item",
                    "tool_name_bytes_per_item",
                ],
            )?;
            require_response_shape_value(shape, "representation", "full")?;
            require_response_shape_usize(shape, "item_count", 2)?;
            let lineage = "a".repeat(response_shape_usize(shape, "lineage_bytes")?);
            let source_command_bytes =
                response_shape_usize(shape, "source_command_bytes_per_item")?;
            let tool_name_bytes = response_shape_usize(shape, "tool_name_bytes_per_item")?;
            let mut items = Vec::with_capacity(2);
            for index in 0..2 {
                let descriptor = response_charge_command_tool_descriptor(
                    u32::try_from(index + 1).expect("fixture command ID"),
                    &lineage,
                    if index == 0 { "c" } else { "d" },
                    response_shape_usize(
                        shape,
                        &format!("item_{index}_input_schema_json_bytes"),
                    )?,
                    response_shape_usize(
                        shape,
                        &format!("item_{index}_outcome_schema_json_bytes"),
                    )?,
                );
                if descriptor.source_command.len() != source_command_bytes
                    || descriptor.tool_name.len() != tool_name_bytes
                {
                    return Err(io::Error::other(
                        "full command boundary descriptor lengths disagree with shape",
                    )
                    .into());
                }
                items.push(v1::CommandToolDiscoveryItem {
                    item: Some(v1::command_tool_discovery_item::Item::CommandTool(
                        descriptor,
                    )),
                });
            }
            v1::DiscoverCommandToolsResponse {
                result: Some(v1::discover_command_tools_response::Result::Page(
                    v1::CommandToolDiscoveryPage {
                        items,
                        next_cursor: Some(vec![0x55; response_shape_usize(shape, "cursor_bytes")?]),
                        observed_fence: Some(public_discovery_fence_for(true, &lineage)),
                        operation_schemas: Some(public_operation_schema_catalog()),
                    },
                )),
            }
            .encode_to_vec()
        }
        "discover_command_tools.full_max_dynamic" => {
            require_response_shape_keys(
                shape,
                &[
                    "command_id",
                    "command_tool_name_bytes",
                    "contract_version",
                    "cursor_bytes",
                    "input_schema_json_bytes",
                    "item_count",
                    "lineage_bytes",
                    "outcome_schema_json_bytes",
                    "representation",
                    "source_command_bytes",
                ],
            )?;
            require_response_shape_value(shape, "representation", "full")?;
            require_response_shape_usize(shape, "item_count", 1)?;
            let command_id = response_shape_u32(shape, "command_id")?;
            let contract_version = response_shape_u64(shape, "contract_version")?;
            let lineage = "a".repeat(response_shape_usize(shape, "lineage_bytes")?);
            let source_command = "c".repeat(response_shape_usize(shape, "source_command_bytes")?);
            let mut descriptor = response_charge_command_tool_descriptor(
                command_id,
                &lineage,
                &source_command,
                response_shape_usize(shape, "input_schema_json_bytes")?,
                response_shape_usize(shape, "outcome_schema_json_bytes")?,
            );
            descriptor.contract_version = contract_version;
            require_response_shape_usize(
                shape,
                "command_tool_name_bytes",
                descriptor.tool_name.len(),
            )?;
            v1::DiscoverCommandToolsResponse {
                result: Some(v1::discover_command_tools_response::Result::Page(
                    v1::CommandToolDiscoveryPage {
                        items: vec![v1::CommandToolDiscoveryItem {
                            item: Some(v1::command_tool_discovery_item::Item::CommandTool(
                                descriptor,
                            )),
                        }],
                        next_cursor: Some(vec![
                            0x55;
                            response_shape_usize(shape, "cursor_bytes")?
                        ]),
                        observed_fence: Some(public_discovery_fence_for_version(
                            true,
                            &lineage,
                            contract_version,
                        )),
                        operation_schemas: Some(public_operation_schema_catalog()),
                    },
                )),
            }
            .encode_to_vec()
        }
        "discover_command_tools.full_representative" => v1::DiscoverCommandToolsResponse {
            result: Some(v1::discover_command_tools_response::Result::Page(
                v1::CommandToolDiscoveryPage {
                    items: vec![v1::CommandToolDiscoveryItem {
                        item: Some(v1::command_tool_discovery_item::Item::CommandTool(
                            response_charge_representative_command_tool_descriptor(),
                        )),
                    }],
                    next_cursor: None,
                    observed_fence: Some(public_discovery_fence_for(true, "LegalSpend")),
                    operation_schemas: Some(public_operation_schema_catalog()),
                },
            )),
        }
        .encode_to_vec(),
        "discover_resources.catalog_unchanged" => v1::DiscoverResourcesResponse {
            result: Some(v1::discover_resources_response::Result::CatalogUnchanged(
                public_discovery_fence_for(true, &"r".repeat(MAX_CONTRACT_LINEAGE_BYTES)),
            )),
        }
        .encode_to_vec(),
        "discover_resources.compact_item_max" => {
            require_response_shape_keys(
                shape,
                &[
                    "command_id",
                    "contract_version",
                    "cursor_present",
                    "item_charge_bytes",
                    "item_count",
                    "lineage_bytes",
                    "representation",
                    "resource_kind",
                    "source_command_bytes",
                ],
            )?;
            require_response_shape_value(shape, "representation", "compact_observation")?;
            require_response_shape_value(shape, "resource_kind", "command_plan")?;
            require_response_shape_usize(shape, "item_charge_bytes", 624)?;
            require_response_shape_usize(shape, "item_count", 1)?;
            if response_shape_bool(shape, "cursor_present")? {
                return Err(io::Error::other("compact-item witness must be an exact end").into());
            }
            let command_id = response_shape_u32(shape, "command_id")?;
            let contract_version = response_shape_u64(shape, "contract_version")?;
            let lineage_bytes = response_shape_usize(shape, "lineage_bytes")?;
            v1::DiscoverResourcesResponse {
                result: Some(v1::discover_resources_response::Result::CompactPage(
                    v1::CompactResourceDiscoveryPage {
                        items: vec![response_charge_compact_resource_descriptor_for(
                            command_id,
                            contract_version,
                            lineage_bytes,
                            response_shape_usize(shape, "source_command_bytes")?,
                        )],
                        next_cursor: None,
                        observed_fence: Some(public_discovery_fence_for_version(
                            true,
                            &"r".repeat(lineage_bytes),
                            contract_version,
                        )),
                    },
                )),
            }
            .encode_to_vec()
        }
        "discover_resources.compact_page_max" => {
            require_response_shape_keys(
                shape,
                &[
                    "command_id_first",
                    "command_id_last",
                    "contract_version",
                    "cursor_bytes",
                    "item_charge_bytes",
                    "item_count",
                    "lineage_bytes",
                    "representation",
                    "resource_kind",
                    "source_command_bytes",
                ],
            )?;
            require_response_shape_value(shape, "representation", "compact_observation")?;
            require_response_shape_value(shape, "resource_kind", "command_plan")?;
            require_response_shape_usize(shape, "item_charge_bytes", 624)?;
            let item_count = response_shape_usize(shape, "item_count")?;
            let first_command_id = response_shape_u32(shape, "command_id_first")?;
            let last_command_id = response_shape_u32(shape, "command_id_last")?;
            let represented_count = last_command_id
                .checked_sub(first_command_id)
                .and_then(|difference| difference.checked_add(1))
                .map(usize::try_from)
                .transpose()?
                .ok_or_else(|| io::Error::other("invalid compact resource command-ID range"))?;
            if represented_count != item_count {
                return Err(io::Error::other(
                    "compact resource command-ID range disagrees with item count",
                )
                .into());
            }
            let contract_version = response_shape_u64(shape, "contract_version")?;
            let lineage_bytes = response_shape_usize(shape, "lineage_bytes")?;
            let source_command_bytes = response_shape_usize(shape, "source_command_bytes")?;
            v1::DiscoverResourcesResponse {
                result: Some(v1::discover_resources_response::Result::CompactPage(
                    v1::CompactResourceDiscoveryPage {
                        items: (0..item_count)
                            .map(|index| {
                                response_charge_compact_resource_descriptor_for(
                                    first_command_id
                                        + u32::try_from(index)
                                            .expect("fixture command-ID offset"),
                                    contract_version,
                                    lineage_bytes,
                                    source_command_bytes,
                                )
                            })
                            .collect(),
                        next_cursor: Some(vec![
                            0x55;
                            response_shape_usize(shape, "cursor_bytes")?
                        ]),
                        observed_fence: Some(public_discovery_fence_for_version(
                            true,
                            &"r".repeat(lineage_bytes),
                            contract_version,
                        )),
                    },
                )),
            }
            .encode_to_vec()
        }
        "discover_resources.full_exact_ceiling" | "discover_resources.full_one_over" => {
            require_response_shape_keys(
                shape,
                &[
                    "cursor_bytes",
                    "item_0_schema_json_bytes",
                    "item_1_schema_json_bytes",
                    "item_2_schema_json_bytes",
                    "item_count",
                    "lineage_bytes",
                    "representation",
                    "resource_kind",
                ],
            )?;
            require_response_shape_value(shape, "representation", "full")?;
            require_response_shape_value(shape, "resource_kind", "entity_schema")?;
            require_response_shape_usize(shape, "item_count", 3)?;
            let lineage = "r".repeat(response_shape_usize(shape, "lineage_bytes")?);
            let items = (0..3)
                .map(|index| {
                    let entity_type_id = u32::try_from(index + 1).expect("fixture entity type ID");
                    v1::ResourceDescriptor {
                        resource: Some(v1::resource_descriptor::Resource::EntitySchema(
                            v1::EntitySchemaResource {
                                contract_lineage: lineage.clone(),
                                entity_type_id,
                                schema: Some(response_charge_schema(
                                    v1::schema_artifact_key::Artifact::EntityId(entity_type_id),
                                    response_shape_usize(
                                        shape,
                                        &format!("item_{index}_schema_json_bytes"),
                                    )
                                    .expect("exact-key-checked fixture schema bytes"),
                                )),
                            },
                        )),
                    }
                })
                .collect();
            v1::DiscoverResourcesResponse {
                result: Some(v1::discover_resources_response::Result::Page(
                    v1::ResourceDiscoveryPage {
                        items,
                        next_cursor: Some(vec![0x55; response_shape_usize(shape, "cursor_bytes")?]),
                        observed_fence: Some(public_discovery_fence_for(true, &lineage)),
                    },
                )),
            }
            .encode_to_vec()
        }
        "discover_resources.full_max_dynamic" => {
            require_response_shape_keys(
                shape,
                &[
                    "contract_version",
                    "cursor_bytes",
                    "entity_schema_json_bytes",
                    "entity_type_id",
                    "item_count",
                    "lineage_bytes",
                    "representation",
                    "resource_kind",
                ],
            )?;
            require_response_shape_value(shape, "representation", "full")?;
            require_response_shape_value(shape, "resource_kind", "entity_schema")?;
            require_response_shape_usize(shape, "item_count", 1)?;
            let contract_version = response_shape_u64(shape, "contract_version")?;
            let entity_type_id = response_shape_u32(shape, "entity_type_id")?;
            let lineage = "r".repeat(response_shape_usize(shape, "lineage_bytes")?);
            let descriptor = v1::ResourceDescriptor {
                resource: Some(v1::resource_descriptor::Resource::EntitySchema(
                    v1::EntitySchemaResource {
                        contract_lineage: lineage.clone(),
                        entity_type_id,
                        schema: Some(response_charge_schema(
                            v1::schema_artifact_key::Artifact::EntityId(entity_type_id),
                            response_shape_usize(shape, "entity_schema_json_bytes")?,
                        )),
                    },
                )),
            };
            v1::DiscoverResourcesResponse {
                result: Some(v1::discover_resources_response::Result::Page(
                    v1::ResourceDiscoveryPage {
                        items: vec![descriptor],
                        next_cursor: Some(vec![
                            0x55;
                            response_shape_usize(shape, "cursor_bytes")?
                        ]),
                        observed_fence: Some(public_discovery_fence_for_version(
                            true,
                            &lineage,
                            contract_version,
                        )),
                    },
                )),
            }
            .encode_to_vec()
        }
        "discover_resources.full_representative" => v1::DiscoverResourcesResponse {
            result: Some(v1::discover_resources_response::Result::Page(
                v1::ResourceDiscoveryPage {
                    items: vec![v1::ResourceDescriptor {
                        resource: Some(v1::resource_descriptor::Resource::ActiveContract(
                            v1::Unit {},
                        )),
                    }],
                    next_cursor: None,
                    observed_fence: Some(public_discovery_fence_for(true, &"a".repeat(115))),
                },
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
        "get_contract_version.found" => v1::GetContractVersionResponse {
            result: Some(v1::get_contract_version_response::Result::Found(
                response_charge_contract_descriptor("fixture"),
            )),
        }
        .encode_to_vec(),
        "get_contract_version.not_found" => v1::GetContractVersionResponse {
            result: Some(v1::get_contract_version_response::Result::NotFound(
                v1::Unit {},
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
        value if value.starts_with("list_pending_outbox_deliveries.") => {
            let state = match response_shape_value(shape, "state")? {
                "pending" => v1::OutboxDeliveryState::Pending,
                "retry_scheduled" => v1::OutboxDeliveryState::RetryScheduled,
                "delivering" => v1::OutboxDeliveryState::Delivering,
                "dead_letter" => v1::OutboxDeliveryState::DeadLetter,
                _ => return Err(io::Error::other("unknown outbox fixture state").into()),
            };
            let next_attempt_at = match response_shape_value(shape, "next_attempt_present")? {
                "true" => Some(v1::Timestamp {
                    seconds: 2,
                    nanos: 0,
                }),
                "false" => None,
                _ => return Err(io::Error::other("invalid outbox optional shape").into()),
            };
            v1::ListPendingOutboxDeliveriesResponse {
                page: Some(v1::OutboxDeliveryPage {
                    items: vec![v1::OutboxDeliverySummary {
                        event_id: Some(v1::EventId {
                            commit_sequence: 1,
                            event_ordinal: 0,
                        }),
                        state: state as i32,
                        attempts: u32::try_from(response_shape_usize(shape, "attempts")?)?,
                        next_attempt_at,
                    }],
                    next_cursor: Some(vec![
                        0x44;
                        response_shape_usize(shape, "cursor_bytes")?
                    ]),
                }),
            }
            .encode_to_vec()
        }
        "operation_schema_catalog.accepted" => public_operation_schema_catalog().encode_to_vec(),
        "projection_status.not_found" => {
            if !shape.is_empty() {
                return Err(io::Error::other(
                    "not-found projection response must have an empty shape",
                )
                .into());
            }
            v1::GetProjectionStatusResponse {
                result: Some(v1::get_projection_status_response::Result::NotFound(
                    v1::Unit {},
                )),
            }
            .encode_to_vec()
        }
        value if value.starts_with("projection_status.") => {
            let applied = |generation, sequence| v1::ProjectionGenerationFrontier {
                generation,
                frontier: Some(public_applied(sequence)),
            };
            let before_first = |generation| v1::ProjectionGenerationFrontier {
                generation,
                frontier: Some(public_before_first()),
            };
            let (lifecycle, published, candidate, published_apply_mode, failure) = match value {
                "projection_status.uninitialized" => (
                    v1::ProjectionLifecycle::Building,
                    None,
                    None,
                    None,
                    None,
                ),
                "projection_status.building" => (
                    v1::ProjectionLifecycle::Building,
                    None,
                    Some(before_first(1)),
                    None,
                    None,
                ),
                "projection_status.catching_up" => (
                    v1::ProjectionLifecycle::CatchingUp,
                    None,
                    Some(applied(1, 1)),
                    None,
                    None,
                ),
                "projection_status.ready" => (
                    v1::ProjectionLifecycle::Ready,
                    Some(applied(1, 1)),
                    None,
                    Some(v1::PublishedApplyMode::Enabled as i32),
                    None,
                ),
                "projection_status.rebuilding" => (
                    v1::ProjectionLifecycle::Rebuilding,
                    Some(applied(1, 1)),
                    Some(applied(2, 1)),
                    Some(v1::PublishedApplyMode::Enabled as i32),
                    None,
                ),
                "projection_status.degraded" => (
                    v1::ProjectionLifecycle::Degraded,
                    Some(applied(1, 1)),
                    None,
                    Some(v1::PublishedApplyMode::Suspended as i32),
                    Some(v1::ProjectionFailure {
                        generation: 1,
                        code: v1::ProjectionFailureCode::MalformedDurableEvent as i32,
                        at_sequence: Some(2),
                    }),
                ),
                "projection_status.invalid" => (
                    v1::ProjectionLifecycle::Invalid,
                    None,
                    Some(applied(1, 1)),
                    None,
                    Some(v1::ProjectionFailure {
                        generation: 1,
                        code: v1::ProjectionFailureCode::ProjectionStateIntegrity as i32,
                        at_sequence: Some(2),
                    }),
                ),
                _ => {
                    return Err(io::Error::other("unknown projection fixture lifecycle").into());
                }
            };
            let lifecycle_name = match lifecycle {
                v1::ProjectionLifecycle::Building => "building",
                v1::ProjectionLifecycle::CatchingUp => "catching_up",
                v1::ProjectionLifecycle::Ready => "ready",
                v1::ProjectionLifecycle::Degraded => "degraded",
                v1::ProjectionLifecycle::Rebuilding => "rebuilding",
                v1::ProjectionLifecycle::Invalid => "invalid",
                v1::ProjectionLifecycle::Unspecified => unreachable!("fixture lifecycle is closed"),
            };
            if response_shape_value(shape, "authoritative_frontier")? != "applied_through"
                || response_shape_value(shape, "lifecycle")? != lifecycle_name
                || response_shape_bool(shape, "published_present")? != published.is_some()
                || response_shape_bool(shape, "candidate_present")? != candidate.is_some()
                || response_shape_bool(shape, "failure_present")? != failure.is_some()
            {
                return Err(io::Error::other(
                    "projection response candidate disagrees with service shape",
                )
                .into());
            }
            v1::GetProjectionStatusResponse {
                result: Some(v1::get_projection_status_response::Result::Found(
                    v1::ProjectionStatus {
                        identity: Some(response_charge_projection_identity_for(
                            response_shape_usize(shape, "lineage_bytes")?,
                            response_shape_usize(shape, "plan_hash_bytes")?,
                            u32::try_from(response_shape_usize(shape, "projection_id")?)?,
                        )),
                        lifecycle: lifecycle as i32,
                        published,
                        candidate,
                        published_apply_mode,
                        failure,
                        authoritative_head: Some(public_applied(u64::try_from(
                            response_shape_usize(shape, "authoritative_sequence")?,
                        )?)),
                    },
                )),
            }
            .encode_to_vec()
        }
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
        "trace_provenance.found" | "trace_provenance.found_max_claims" => {
            v1::TraceProvenanceResponse {
                result: Some(v1::trace_provenance_response::Result::Found(
                    response_charge_provenance(shape)?,
                )),
            }
            .encode_to_vec()
        }
        "trace_provenance.not_found" => {
            if !shape.is_empty() {
                return Err(io::Error::other(
                    "not-found provenance response must have an empty shape",
                )
                .into());
            }
            v1::TraceProvenanceResponse {
                result: Some(v1::trace_provenance_response::Result::NotFound(
                    v1::Unit {},
                )),
            }
            .encode_to_vec()
        }
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
    response_charge_projection_identity_for(7, 32, 1)
}

fn response_charge_projection_identity_for(
    lineage_bytes: usize,
    plan_hash_bytes: usize,
    projection_id: u32,
) -> v1::ProjectionIdentity {
    v1::ProjectionIdentity {
        contract_lineage: "p".repeat(lineage_bytes),
        projection_id,
        projection_plan_hash: vec![0x77; plan_hash_bytes],
    }
}

fn response_charge_provenance(
    shape: &BTreeMap<String, String>,
) -> Result<v1::Provenance, io::Error> {
    let claim_count = response_shape_usize(shape, "claim_count")?;
    let expected_keys = if claim_count == 4 {
        &[
            "actor_id_bytes",
            "affected_entity_count",
            "approval_id_bytes",
            "claim_count",
            "entity_key_bytes",
            "event_count",
            "lineage_bytes",
            "reason_bytes",
            "source_commit_bytes",
            "source_repository_bytes",
            "tenant_scope",
        ][..]
    } else {
        &[
            "actor_id_bytes",
            "affected_entity_count",
            "claim_count",
            "entity_key_bytes",
            "event_count",
            "lineage_bytes",
            "tenant_scope",
        ][..]
    };
    require_response_shape_keys(shape, expected_keys)?;
    if response_shape_value(shape, "tenant_scope")? != "global" {
        return Err(io::Error::other(
            "provenance response candidate disagrees with service shape",
        ));
    }
    let claims = match claim_count {
        0 => v1::ProvenanceClaims {
            source_repository: None,
            source_commit: None,
            reason: None,
            approval_id: None,
        },
        4 => v1::ProvenanceClaims {
            source_repository: Some(
                "r".repeat(response_shape_usize(shape, "source_repository_bytes")?),
            ),
            source_commit: Some("c".repeat(response_shape_usize(shape, "source_commit_bytes")?)),
            reason: Some("p".repeat(response_shape_usize(shape, "reason_bytes")?)),
            approval_id: Some("a".repeat(response_shape_usize(shape, "approval_id_bytes")?)),
        },
        _ => {
            return Err(io::Error::other(
                "provenance response candidate disagrees with service claim count",
            ));
        }
    };
    let affected_entity_count = response_shape_usize(shape, "affected_entity_count")?;
    let event_count = response_shape_usize(shape, "event_count")?;
    Ok(v1::Provenance {
        provenance_id: public_request_id(),
        commit_sequence: 1,
        admission_request_id: public_request_id(),
        contract_lineage: "p".repeat(response_shape_usize(shape, "lineage_bytes")?),
        contract_version: 1,
        command_id: 1,
        plan_hash: vec![0x66; 32],
        actor: Some(v1::AdmittedActor {
            principal_id: "a".repeat(response_shape_usize(shape, "actor_id_bytes")?),
            actor_kind: v1::ActorKind::Human as i32,
            tenant_scope: Some(v1::TenantScope {
                scope: Some(v1::tenant_scope::Scope::Global(v1::Unit {})),
            }),
            agent_session_id: None,
        }),
        logical_time: Some(v1::Timestamp {
            seconds: 1,
            nanos: 0,
        }),
        outcome_id: 1,
        affected_entities: (0..affected_entity_count)
            .map(|ordinal| v1::AffectedEntity {
                entity_key: response_charge_entity_key(
                    1,
                    response_shape_usize(shape, "entity_key_bytes")
                        .expect("checked provenance entity-key length"),
                    u32::try_from(ordinal).expect("fixture ordinal fits u32"),
                ),
                entity_version: 1,
            })
            .collect(),
        event_ids: (0..event_count)
            .map(|ordinal| v1::EventId {
                commit_sequence: 1,
                event_ordinal: u32::try_from(ordinal).expect("fixture ordinal fits u32"),
            })
            .collect(),
        claims: Some(claims),
    })
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
