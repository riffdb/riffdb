//! Deterministic WP-408 migration compatibility fixtures.

use std::num::NonZeroU64;

use riffdb_storage_api::{
    AuditPrincipalV1, BackupIntegrityChecksumV1, ContractMigrationAdmissionV1,
    ContractMigrationArtifactFileV1, ContractMigrationArtifactsV1, ContractMigrationJournalStepV1,
    ContractMigrationOperationArtifactsV1, ContractMigrationOperationKindV1,
    ContractMigrationReceiptPhaseV1, ContractMigrationReceiptTransitionV1,
    ContractMigrationReceiptV1, DurableKeySchemaBindingV1, EntityTarget, MigrationScanCursor,
    StoredContractMigrationJournalV1, StoredContractMigrationRecordV1,
    StoredContractWriteRetirementV1, StoredEntityRecordV1, StoredRetiredEntityRecordV1,
};
use riffdb_types::{
    ActorId, ActorKind, AdministrationSequence, ApprovalId, CanonicalRecord, CapabilityId,
    ContractBundleHash, ContractLineage, ContractMigrationInputHash, ContractMigrationOperationId,
    ContractMigrationValidationDigest, ContractVersion, DatabaseId, EntityKeyBuilder, EntityTypeId,
    EntityVersion, MigrationBundleHash, RequestId, ServiceIngressKindV1, Timestamp,
};

use crate::keys::{
    encode_contract_migration_operation_key, encode_contract_write_retirement_key,
    encode_retired_entity_key,
};
use crate::maintenance::encode_migration_receipt_fixture;

/// One generated compatibility artifact and its repository-relative name.
#[derive(Debug)]
pub struct MigrationDurableFixture {
    name: &'static str,
    bytes: Vec<u8>,
}

impl MigrationDurableFixture {
    /// Returns the fixture file name under `fixtures/migrations/durable/v1`.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        self.name
    }

    /// Borrows the exact fixture bytes.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

/// Produces all redb-owned migration V1 compatibility fixtures.
///
/// This helper exists only behind `test-fixtures`; it is not a runtime storage
/// interface. Every value is synthetic and contains no user data.
pub fn migration_durable_fixture_set() -> Result<Vec<MigrationDurableFixture>, String> {
    let database_id =
        DatabaseId::from_unix_milliseconds_and_random(1, [0x11; 10]).map_err(display)?;
    let operation_id =
        ContractMigrationOperationId::from_unix_milliseconds_and_random(2, [0x22; 10])
            .map_err(display)?;
    let capability_id =
        CapabilityId::from_unix_milliseconds_and_random(3, [0x33; 10]).map_err(display)?;
    let request_id =
        RequestId::from_unix_milliseconds_and_random(4, [0x44; 10]).map_err(display)?;
    let input_hash = ContractMigrationInputHash::from_bytes([0x55; 32]);
    let artifacts = ContractMigrationArtifactsV1::new(
        ContractBundleHash::from_bytes([0x66; 32]),
        ContractBundleHash::from_bytes([0x77; 32]),
        MigrationBundleHash::from_bytes([0x88; 32]),
    );
    let operation_artifacts = ContractMigrationOperationArtifactsV1::new(
        ContractMigrationArtifactFileV1::new(101, [0x99; 32]).map_err(display)?,
        ContractMigrationArtifactFileV1::new(202, [0xaa; 32]).map_err(display)?,
    );
    let principal = AuditPrincipalV1::new(
        ActorId::new("fixture-operator").map_err(display)?,
        ActorKind::Human,
        capability_id,
        NonZeroU64::new(7).ok_or_else(|| "fixture capability revision is zero".to_owned())?,
    );
    let approval_id = ApprovalId::new("fixture-approval").map_err(display)?;
    let admission = ContractMigrationAdmissionV1::new(
        principal.clone(),
        Some(approval_id.clone()),
        request_id,
        Timestamp::new(1_700_000_000, 123).map_err(display)?,
        ServiceIngressKindV1::Grpc,
    );
    let receipt = ContractMigrationReceiptV1::from_canonical_parts(
        database_id,
        operation_id,
        input_hash,
        artifacts,
        operation_artifacts,
        admission.clone(),
        None,
        None,
        None,
        vec![ContractMigrationReceiptTransitionV1::phase(
            ContractMigrationReceiptPhaseV1::Accepted,
        )],
    )
    .map_err(display)?;
    let check_receipt = ContractMigrationReceiptV1::from_canonical_parts_for_operation(
        ContractMigrationOperationKindV1::Check,
        database_id,
        ContractMigrationOperationId::from_unix_milliseconds_and_random(5, [0x25; 10])
            .map_err(display)?,
        ContractMigrationInputHash::from_bytes([0x56; 32]),
        artifacts,
        operation_artifacts,
        admission.clone(),
        None,
        None,
        None,
        vec![
            ContractMigrationReceiptTransitionV1::phase(ContractMigrationReceiptPhaseV1::Accepted),
            ContractMigrationReceiptTransitionV1::phase(ContractMigrationReceiptPhaseV1::Preflight),
            ContractMigrationReceiptTransitionV1::phase(ContractMigrationReceiptPhaseV1::Succeeded),
        ],
    )
    .map_err(display)?;

    let journal = StoredContractMigrationJournalV1::new(
        database_id,
        operation_id,
        input_hash,
        artifacts,
        ContractMigrationJournalStepV1::Transforming,
        MigrationScanCursor::start(),
        0,
        0,
        1,
        None,
        Vec::new(),
        None,
    )
    .map_err(display)?;
    let sequence = AdministrationSequence::first();
    let record = StoredContractMigrationRecordV1::new(
        database_id,
        operation_id,
        input_hash,
        artifacts,
        operation_artifacts,
        riffdb_types::BackupNameV1::new("pre-migration-fixture").map_err(display)?,
        BackupIntegrityChecksumV1::new(vec![0xbb; 32]).map_err(display)?,
        principal,
        Some(approval_id),
        None,
        None,
        0,
        0,
        1,
        ContractMigrationValidationDigest::from_bytes([0xcc; 32]),
        sequence,
    )
    .map_err(display)?;
    let retirement = StoredContractWriteRetirementV1::new(artifacts, operation_id, sequence);

    let entity_type = EntityTypeId::first();
    let contract_version =
        ContractVersion::new(1).ok_or_else(|| "fixture contract version is zero".to_owned())?;
    let mut key = EntityKeyBuilder::new(entity_type);
    key.push_u64(42).map_err(display)?;
    let target = EntityTarget::new(entity_type, key.finish().map_err(display)?).map_err(display)?;
    let entity = StoredEntityRecordV1::new(
        target.clone(),
        EntityVersion::first(),
        contract_version,
        DurableKeySchemaBindingV1::new(
            ContractLineage::new("Fixture").map_err(display)?,
            contract_version,
            artifacts.parent(),
        ),
        CanonicalRecord::new(Vec::new()).map_err(display)?,
    )
    .map_err(display)?;
    let entity_envelope = riffdb_storage_api::proto_codec::encode_entity_record_v1(&entity)
        .map_err(display)?
        .into_bytes();
    let retired = StoredRetiredEntityRecordV1::new(
        operation_id,
        artifacts.migration(),
        target.clone(),
        entity_envelope,
    )
    .map_err(display)?;

    let fixture = |name, bytes| MigrationDurableFixture { name, bytes };
    Ok(vec![
        fixture(
            "receipt-accepted-v1.bin",
            encode_migration_receipt_fixture(&receipt).map_err(display)?,
        ),
        fixture(
            "receipt-check-succeeded-v2.bin",
            encode_migration_receipt_fixture(&check_receipt).map_err(display)?,
        ),
        fixture(
            "journal-transforming-v1.bin",
            riffdb_storage_api::proto_codec::encode_contract_migration_journal_v1(&journal)
                .map_err(display)?
                .into_bytes(),
        ),
        fixture(
            "migration-record-v1.bin",
            riffdb_storage_api::proto_codec::encode_contract_migration_record_v1(&record)
                .map_err(display)?
                .into_bytes(),
        ),
        fixture(
            "write-retirement-v1.bin",
            riffdb_storage_api::proto_codec::encode_contract_write_retirement_v1(retirement)
                .map_err(display)?
                .into_bytes(),
        ),
        fixture(
            "retired-entity-v1.bin",
            riffdb_storage_api::proto_codec::encode_retired_entity_record_v1(&retired)
                .map_err(display)?
                .into_bytes(),
        ),
        fixture(
            "operation-key-v1.bin",
            encode_contract_migration_operation_key(operation_id).to_vec(),
        ),
        fixture(
            "write-retirement-key-v1.bin",
            encode_contract_write_retirement_key(artifacts.parent()).to_vec(),
        ),
        fixture(
            "retired-entity-key-v1.bin",
            encode_retired_entity_key(operation_id, &target)
                .map_err(|error| format!("invalid retired entity fixture key: {error:?}"))?,
        ),
        fixture(
            "registry-v1.txt",
            b"format=riffdb-migration-durable-registry-v1\nexternal=ContractMigrationReceiptV1:apply-format-v1\nexternal=ContractMigrationReceiptV1:check-format-v2\ntable=contract_migration_journal:StoredContractMigrationJournalV1\ntable=contract_migrations:StoredContractMigrationRecordV1\ntable=contract_write_retirements:StoredContractWriteRetirementV1\ntable=retired_entities:StoredRetiredEntityRecordV1\n".to_vec(),
        ),
        fixture(
            "impossible-pairs-v1.txt",
            b"format=riffdb-migration-impossible-pairs-v1\nreceipt=Succeeded,target=predecessor\nreceipt=FailedRolledBack,target=successor\nreceipt=Publishing,target=neither-predecessor-nor-successor\nretirement-without-matching-migration-record\nmigration-record-without-matching-retirement\n".to_vec(),
        ),
    ])
}

fn display(error: impl std::fmt::Display) -> String {
    error.to_string()
}
