//! Bundle validation, plan resolution, and deployment preparation semantics.

use std::num::NonZeroU64;
use std::ops::Range;

use riffdb_catalog::{
    ActiveCatalogSnapshot, CatalogActivationMode, CatalogErrorKind, CatalogPreparationResult,
    ValidatedContractBundle, prepare_catalog_activation, resolve_executable_plan,
};
use riffdb_contract_compiler::{compile_contract_source, compile_contract_successor};
use riffdb_contract_ir::MAX_BUNDLE_BYTES;
use riffdb_storage_api::{
    ActiveCatalogPointerV1, AuditPrincipalV1, CatalogRepository, ExecutablePlanRef,
    MAX_CATALOG_BUNDLE_BYTES, StorageError, StorageValueError, StoredContractBundleV1,
};
use riffdb_types::{
    ActorId, ActorKind, ApprovalId, CapabilityId, ContractBundleHash, ContractLineage,
    ContractVersion, PlanHash, RequestId, Timestamp,
};

const BUDGET: &str = include_str!("../../../contracts/examples/budget.riff");

struct ReadRepository {
    active: Option<ActiveCatalogPointerV1>,
    bundles: Vec<StoredContractBundleV1>,
}

impl CatalogRepository for ReadRepository {
    fn read_active_catalog(&self) -> Result<Option<ActiveCatalogPointerV1>, StorageError> {
        Ok(self.active.clone())
    }

    fn read_contract_bundle(
        &self,
        lineage: &ContractLineage,
        contract_version: ContractVersion,
    ) -> Result<Option<StoredContractBundleV1>, StorageError> {
        Ok(self
            .bundles
            .iter()
            .find(|bundle| {
                bundle.lineage() == lineage && bundle.contract_version() == contract_version
            })
            .cloned())
    }
}

fn uuid_v7(seed: u8) -> [u8; 16] {
    let mut bytes = [0; 16];
    bytes[..10].copy_from_slice(&[0x01, 0x8f, 0, 0, 0, 0, 0x70, 1, 0x80, 2]);
    bytes[15] = seed;
    bytes
}

fn principal() -> AuditPrincipalV1 {
    AuditPrincipalV1::new(
        ActorId::new("maintainer").expect("actor"),
        ActorKind::Human,
        CapabilityId::from_bytes(uuid_v7(1)).expect("capability"),
        NonZeroU64::MIN,
    )
}

fn repository(bundle: &ValidatedContractBundle) -> ReadRepository {
    let stored = bundle.to_stored().expect("stored bundle");
    ReadRepository {
        active: Some(ActiveCatalogPointerV1::from_bundle(&stored)),
        bundles: vec![stored],
    }
}

fn version_two(source: &str) -> String {
    source.replacen("version 1", "version 2", 1)
}

#[test]
fn validates_bundle_identity_and_resolves_only_the_complete_plan_reference() {
    let compiled = compile_contract_source(BUDGET).expect("budget compiles");
    let checked = ValidatedContractBundle::from_compiler_bundle(compiled).expect("catalog bundle");
    let repo = repository(&checked);
    let command = checked.bundle().commands().first().expect("command");
    let exact = ExecutablePlanRef::new(
        checked.lineage().clone(),
        checked.contract_version(),
        checked.bundle_hash(),
        command.command_id(),
        command.plan_hash(),
    );

    let resolved = resolve_executable_plan(&repo, &exact).expect("exact plan");
    assert_eq!(resolved.reference(), &exact);
    assert_eq!(resolved.plan(), command);
    let active = ActiveCatalogSnapshot::read(&repo)
        .expect("active catalog read")
        .expect("active catalog exists");
    let lineage_canary = checked.lineage().as_str();
    let hash_canary = format!("{:?}", checked.bundle_hash());
    for debug in [
        format!("{checked:?}"),
        format!("{resolved:?}"),
        format!("{active:?}"),
    ] {
        assert!(debug.contains("[REDACTED]") || debug.contains("[CHECKED]"));
        assert!(!debug.contains(lineage_canary));
        assert!(!debug.contains(&hash_canary));
    }

    let wrong_hash = ExecutablePlanRef::new(
        checked.lineage().clone(),
        checked.contract_version(),
        checked.bundle_hash(),
        command.command_id(),
        PlanHash::from_bytes([0x55; 32]),
    );
    assert_eq!(
        resolve_executable_plan(&repo, &wrong_hash)
            .expect_err("plan substitution must reject")
            .kind(),
        CatalogErrorKind::UnknownExecutablePlan
    );

    let wrong_identity = StoredContractBundleV1::new(
        checked.lineage().clone(),
        checked.contract_version(),
        ContractBundleHash::from_bytes([0x33; 32]),
        checked.bundle().canonical_bytes().to_vec(),
    )
    .expect("structurally valid storage DTO");
    assert_eq!(
        ValidatedContractBundle::from_stored(&wrong_identity)
            .expect_err("opaque identity mismatch")
            .kind(),
        CatalogErrorKind::BundleIdentityConflict
    );
}

#[test]
fn active_lineage_resolves_exact_ancestors_and_rejects_a_missing_parent_edge() {
    let first_compiled = compile_contract_source(BUDGET).expect("genesis");
    let first = ValidatedContractBundle::from_compiler_bundle(first_compiled.clone())
        .expect("checked genesis");
    let seventh_compiled = compile_contract_successor(
        &BUDGET.replacen("version 1", "version 7", 1),
        &first_compiled,
    )
    .expect("skipped application version");
    let seventh = ValidatedContractBundle::from_compiler_bundle(seventh_compiled.clone())
        .expect("checked seventh");
    let forty_second_compiled = compile_contract_successor(
        &BUDGET.replacen("version 1", "version 42", 1),
        &seventh_compiled,
    )
    .expect("second skipped application version");
    let forty_second =
        ValidatedContractBundle::from_compiler_bundle(forty_second_compiled).expect("checked tip");

    let first_command = first.bundle().commands().first().expect("command");
    let first_reference = ExecutablePlanRef::new(
        first.lineage().clone(),
        first.contract_version(),
        first.bundle_hash(),
        first_command.command_id(),
        first_command.plan_hash(),
    );
    let tip_stored = forty_second.to_stored().expect("stored tip");
    let repository = ReadRepository {
        active: Some(ActiveCatalogPointerV1::from_bundle(&tip_stored)),
        bundles: vec![
            first.to_stored().expect("stored genesis"),
            seventh.to_stored().expect("stored seventh"),
            tip_stored.clone(),
        ],
    };
    let resolved = resolve_executable_plan(&repository, &first_reference)
        .expect("exact ancestor is in the active lineage");
    assert_eq!(resolved.reference(), &first_reference);

    let missing_parent = ReadRepository {
        active: Some(ActiveCatalogPointerV1::from_bundle(&tip_stored)),
        bundles: vec![first.to_stored().expect("stored genesis"), tip_stored],
    };
    assert_eq!(
        ActiveCatalogSnapshot::read(&missing_parent)
            .expect_err("the active-to-genesis chain must be complete")
            .kind(),
        CatalogErrorKind::InvalidHistoricalEvidence
    );
}

#[test]
fn preparation_recomputes_compatibility_and_preserves_storage_retry_precedence() {
    let genesis_compiled = compile_contract_source(BUDGET).expect("genesis");
    let genesis = ValidatedContractBundle::from_compiler_bundle(genesis_compiled.clone())
        .expect("validated genesis");

    let CatalogPreparationResult::Prepared(initial) =
        prepare_catalog_activation(genesis_compiled, None, None).expect("initial preparation")
    else {
        panic!("genesis must prepare");
    };
    assert_eq!(initial.mode(), CatalogActivationMode::NewActivation);
    let intent = initial
        .into_storage_intent(
            RequestId::from_bytes(uuid_v7(2)).expect("request"),
            principal(),
            Timestamp::new(1, 0).expect("time"),
            Some(ApprovalId::new("approval-3").expect("approval")),
        )
        .expect("typed intent");
    assert_eq!(intent.expected_active_version(), None);
    assert_eq!(intent.bundle().bundle_hash(), genesis.bundle_hash());

    let repo = repository(&genesis);
    let active = ActiveCatalogSnapshot::read(&repo)
        .expect("active read")
        .expect("active exists");

    let stale = ContractVersion::new(99).expect("version");
    let replay_bundle = compile_contract_source(BUDGET).expect("same genesis");
    let CatalogPreparationResult::Prepared(replay) =
        prepare_catalog_activation(replay_bundle, Some(stale), Some(&active))
            .expect("exact active replay")
    else {
        panic!("exact pointer replay takes precedence");
    };
    assert_eq!(replay.mode(), CatalogActivationMode::ExactReplay);

    let successor = compile_contract_successor(&version_two(BUDGET), genesis.bundle())
        .expect("compatible successor");
    let CatalogPreparationResult::Prepared(prepared) = prepare_catalog_activation(
        successor,
        Some(ContractVersion::new(1).expect("version")),
        Some(&active),
    )
    .expect("successor preparation") else {
        panic!("compatible successor must prepare");
    };
    assert_eq!(prepared.mode(), CatalogActivationMode::NewActivation);

    let successor = compile_contract_successor(&version_two(BUDGET), genesis.bundle())
        .expect("compatible successor");
    assert!(matches!(
        prepare_catalog_activation(successor, None, Some(&active)).expect("typed mismatch"),
        CatalogPreparationResult::ExpectedActiveVersionMismatch { actual: Some(version) }
            if version == ContractVersion::new(1).expect("version")
    ));
}

#[test]
fn destructive_successor_rejects() {
    let genesis = compile_contract_source(BUDGET).expect("genesis");
    let checked =
        ValidatedContractBundle::from_compiler_bundle(genesis.clone()).expect("validated genesis");
    let repo = repository(&checked);
    let active = ActiveCatalogSnapshot::read(&repo)
        .expect("active read")
        .expect("active exists");

    let version_two = version_two(BUDGET);
    let projection = version_two
        .find("  projection BudgetUtilizationDaily")
        .expect("projection section");
    let destructive_source = format!("{}\n}}\n", &version_two[..projection]);
    let destructive = compile_contract_successor(&destructive_source, checked.bundle())
        .expect("compiler reports incompatible removal");
    assert_eq!(
        prepare_catalog_activation(
            destructive,
            Some(ContractVersion::new(1).expect("version")),
            Some(&active),
        )
        .expect_err("destructive activation must reject")
        .kind(),
        CatalogErrorKind::IncompatibleContract
    );
}

#[derive(Clone)]
struct RegistryLayout {
    version: usize,
    lineage: Range<usize>,
    source_contract: Range<usize>,
    count: usize,
    first: Range<usize>,
    first_id: usize,
    first_source: Range<usize>,
    first_tool_length: usize,
    first_tool: Range<usize>,
    second: Range<usize>,
    second_id: usize,
    second_source_length: usize,
    second_tool: Range<usize>,
}

#[test]
fn every_malformed_or_misbound_command_registry_rejects_at_the_catalog_boundary() {
    let genesis = compile_contract_source(BUDGET).expect("genesis");
    let canonical = genesis.canonical_bytes().to_vec();
    let layout = registry_layout(&canonical);
    let mut malformed = Vec::<(&str, Vec<u8>)>::new();

    let mut unsupported_version = canonical.clone();
    write_u32(&mut unsupported_version, layout.version, 3);
    malformed.push(("unsupported registry version", unsupported_version));

    let mut wrong_lineage = canonical.clone();
    wrong_lineage[layout.lineage.start] = b'R';
    wrong_lineage[layout.source_contract.start] = b'R';
    wrong_lineage[layout.first_tool.start + 11] = b'r';
    wrong_lineage[layout.second_tool.start + 11] = b'r';
    malformed.push(("registry lineage binding", wrong_lineage));

    let mut missing_entry = canonical.clone();
    write_u32(&mut missing_entry, layout.count, 1);
    missing_entry.drain(layout.second.clone());
    malformed.push(("missing command entry", missing_entry));

    let mut extra_entry = canonical.clone();
    write_u32(&mut extra_entry, layout.count, 3);
    malformed.push(("extra command entry", extra_entry));

    let mut duplicate_id = canonical.clone();
    let first_id = duplicate_id[layout.first_id..layout.first_id + 4].to_vec();
    duplicate_id[layout.second_id..layout.second_id + 4].copy_from_slice(&first_id);
    malformed.push(("duplicate command id", duplicate_id));

    let mut misbound_id = canonical.clone();
    let first_id = misbound_id[layout.first_id..layout.first_id + 4].to_vec();
    let second_id = misbound_id[layout.second_id..layout.second_id + 4].to_vec();
    misbound_id[layout.first_id..layout.first_id + 4].copy_from_slice(&second_id);
    misbound_id[layout.second_id..layout.second_id + 4].copy_from_slice(&first_id);
    malformed.push(("command id binding", misbound_id));

    let mut reordered = Vec::with_capacity(canonical.len());
    reordered.extend_from_slice(&canonical[..layout.first.start]);
    reordered.extend_from_slice(&canonical[layout.second.clone()]);
    reordered.extend_from_slice(&canonical[layout.first.clone()]);
    reordered.extend_from_slice(&canonical[layout.second.end..]);
    malformed.push(("noncanonical entry order", reordered));

    let mut misbound_source = canonical.clone();
    misbound_source[layout.first_source.start] = b'D';
    misbound_source[layout.first_tool.start + 22] = b'd';
    malformed.push(("source command binding", misbound_source));

    let mut wrong_derivation = canonical.clone();
    let final_byte = layout.first_tool.end - 1;
    wrong_derivation[final_byte] = b'x';
    malformed.push(("noncanonical v2 derivation", wrong_derivation));

    let mut overlength = canonical.clone();
    write_u32(&mut overlength, layout.first_tool_length, 129);
    malformed.push(("overlength tool name", overlength));

    let mut collision = Vec::new();
    collision.extend_from_slice(&canonical[..layout.second_source_length]);
    collision.extend_from_slice(&12_u32.to_be_bytes());
    collision.extend_from_slice(b"CREATEBUDGET");
    collision.extend_from_slice(&34_u32.to_be_bytes());
    collision.extend_from_slice(b"riffdb_cmd_legalspend_createbudget");
    collision.extend_from_slice(&canonical[layout.second_tool.end..]);
    malformed.push(("normalized tool-name collision", collision));

    for (case, bytes) in malformed {
        assert!(
            ValidatedContractBundle::decode(&bytes).is_err(),
            "catalog accepted {case}"
        );
    }
}

#[test]
fn persistence_accepts_the_exact_ir_bundle_ceiling_and_rejects_one_byte_over() {
    assert_eq!(MAX_CATALOG_BUNDLE_BYTES, MAX_BUNDLE_BYTES);
    let lineage = ContractLineage::new("MaximumBundle").expect("lineage");
    let version = ContractVersion::new(1).expect("version");
    let hash = ContractBundleHash::from_bytes([0x55; 32]);
    let exact =
        StoredContractBundleV1::new(lineage.clone(), version, hash, vec![0x5a; MAX_BUNDLE_BYTES])
            .expect("storage preserves a maximum-size opaque semantic bundle");
    assert_eq!(exact.canonical_bytes().len(), MAX_BUNDLE_BYTES);
    drop(exact);

    assert_eq!(
        StoredContractBundleV1::new(lineage, version, hash, vec![0x5a; MAX_BUNDLE_BYTES + 1],)
            .expect_err("one byte over the semantic ceiling"),
        StorageValueError::LimitExceeded
    );
}

fn registry_layout(bytes: &[u8]) -> RegistryLayout {
    const LINEAGE: &[u8] = b"LegalSpend";
    const FIRST_SOURCE: &[u8] = b"CreateBudget";
    const FIRST_TOOL: &[u8] = b"riffdb_cmd_legalspend_createbudget";
    const SECOND_SOURCE: &[u8] = b"AllocateBudget";
    const SECOND_TOOL: &[u8] = b"riffdb_cmd_legalspend_allocatebudget";

    let first_tool_start = bytes
        .windows(FIRST_TOOL.len())
        .rposition(|window| window == FIRST_TOOL)
        .expect("first registry tool");
    let first_tool_length = first_tool_start - 4;
    assert_eq!(read_u32(bytes, first_tool_length), FIRST_TOOL.len() as u32);
    let first_source = first_tool_length - FIRST_SOURCE.len()..first_tool_length;
    assert_eq!(&bytes[first_source.clone()], FIRST_SOURCE);
    let first_source_length = first_source.start - 4;
    assert_eq!(
        read_u32(bytes, first_source_length),
        FIRST_SOURCE.len() as u32
    );
    let first_id = first_source_length - 4;
    let count = first_id - 4;
    assert_eq!(read_u32(bytes, count), 2);
    let source_contract = count - LINEAGE.len()..count;
    assert_eq!(&bytes[source_contract.clone()], LINEAGE);
    let source_contract_length = source_contract.start - 4;
    assert_eq!(
        read_u32(bytes, source_contract_length),
        LINEAGE.len() as u32
    );
    let lineage = source_contract_length - LINEAGE.len()..source_contract_length;
    assert_eq!(&bytes[lineage.clone()], LINEAGE);
    let lineage_length = lineage.start - 4;
    assert_eq!(read_u32(bytes, lineage_length), LINEAGE.len() as u32);
    let version = lineage_length - 4;
    assert_eq!(read_u32(bytes, version), 2);

    let first_tool = first_tool_start..first_tool_start + FIRST_TOOL.len();
    let second_id = first_tool.end;
    let second_source_length = second_id + 4;
    assert_eq!(
        read_u32(bytes, second_source_length),
        SECOND_SOURCE.len() as u32
    );
    let second_source_start = second_source_length + 4;
    let second_source = second_source_start..second_source_start + SECOND_SOURCE.len();
    assert_eq!(&bytes[second_source], SECOND_SOURCE);
    let second_tool_length = second_source_start + SECOND_SOURCE.len();
    assert_eq!(
        read_u32(bytes, second_tool_length),
        SECOND_TOOL.len() as u32
    );
    let second_tool_start = second_tool_length + 4;
    let second_tool = second_tool_start..second_tool_start + SECOND_TOOL.len();
    assert_eq!(&bytes[second_tool.clone()], SECOND_TOOL);

    RegistryLayout {
        version,
        lineage,
        source_contract,
        count,
        first: first_id..first_tool.end,
        first_id,
        first_source,
        first_tool_length,
        first_tool,
        second: second_id..second_tool.end,
        second_id,
        second_source_length,
        second_tool,
    }
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes(bytes[offset..offset + 4].try_into().expect("u32 bytes"))
}

fn write_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
}
