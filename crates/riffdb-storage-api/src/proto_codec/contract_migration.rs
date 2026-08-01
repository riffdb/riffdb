//! Checked V1 durable contract-migration mappings.

use riffdb_proto::storage::v1 as wire;
use riffdb_types::{
    AdministrationSequence, ApprovalId, CommitSequence, ContractBundleHash,
    ContractMigrationInputHash, ContractMigrationJournalHash, ContractMigrationOperationId,
    ContractMigrationValidationDigest, DatabaseId, MigrationBundleHash, ProjectionId,
};

use crate::{
    BackupIntegrityChecksumV1, ContractMigrationArtifactFileV1, ContractMigrationArtifactsV1,
    ContractMigrationJournalStepV1, ContractMigrationOperationArtifactsV1, EncodedPageItem,
    MigrationScanCursor, StoredContractMigrationJournalV1, StoredContractMigrationRecordV1,
    StoredContractWriteRetirementV1, StoredRetiredEntityRecordV1,
};

use super::{
    CanonicalStoredEnvelopeV1, DurableCodecError, audit_principal_from_proto,
    audit_principal_to_proto, decode_entity_record_v1, decode_message, encode_message,
    entity_target_from_proto, entity_target_to_proto, fixed, require,
};

const JOURNAL: &str = "riffdb.storage.v1.StoredContractMigrationJournalV1";
const RECORD: &str = "riffdb.storage.v1.StoredContractMigrationRecordV1";
const RETIREMENT: &str = "riffdb.storage.v1.StoredContractWriteRetirementV1";
const RETIRED_ENTITY: &str = "riffdb.storage.v1.StoredRetiredEntityRecordV1";

fn artifacts_to_proto(
    value: ContractMigrationArtifactsV1,
) -> wire::ContractMigrationArtifactIdentityV1 {
    wire::ContractMigrationArtifactIdentityV1 {
        parent_bundle_hash: value.parent().as_bytes().to_vec(),
        candidate_bundle_hash: value.candidate().as_bytes().to_vec(),
        migration_bundle_hash: value.migration().as_bytes().to_vec(),
    }
}

fn artifacts_from_proto(
    value: wire::ContractMigrationArtifactIdentityV1,
) -> Result<ContractMigrationArtifactsV1, DurableCodecError> {
    Ok(ContractMigrationArtifactsV1::new(
        ContractBundleHash::from_bytes(fixed(value.parent_bundle_hash)?),
        ContractBundleHash::from_bytes(fixed(value.candidate_bundle_hash)?),
        MigrationBundleHash::from_bytes(fixed(value.migration_bundle_hash)?),
    ))
}

fn operation_artifacts_to_proto(
    value: ContractMigrationOperationArtifactsV1,
) -> wire::ContractMigrationOperationArtifactsV1 {
    wire::ContractMigrationOperationArtifactsV1 {
        candidate_bundle_length: value.candidate().length(),
        candidate_bundle_sha256: value.candidate().sha256().to_vec(),
        migration_bundle_length: value.migration().length(),
        migration_bundle_sha256: value.migration().sha256().to_vec(),
    }
}

fn operation_artifacts_from_proto(
    value: wire::ContractMigrationOperationArtifactsV1,
) -> Result<ContractMigrationOperationArtifactsV1, DurableCodecError> {
    Ok(ContractMigrationOperationArtifactsV1::new(
        ContractMigrationArtifactFileV1::new(
            value.candidate_bundle_length,
            fixed(value.candidate_bundle_sha256)?,
        )
        .map_err(DurableCodecError::from_storage_value)?,
        ContractMigrationArtifactFileV1::new(
            value.migration_bundle_length,
            fixed(value.migration_bundle_sha256)?,
        )
        .map_err(DurableCodecError::from_storage_value)?,
    ))
}

const fn step_to_proto(value: ContractMigrationJournalStepV1) -> i32 {
    use wire::ContractMigrationJournalStepV1 as W;
    match value {
        ContractMigrationJournalStepV1::Transforming => {
            W::ContractMigrationJournalStepTransforming as i32
        }
        ContractMigrationJournalStepV1::RebuildingProjections => {
            W::ContractMigrationJournalStepRebuildingProjections as i32
        }
        ContractMigrationJournalStepV1::Validating => {
            W::ContractMigrationJournalStepValidating as i32
        }
        ContractMigrationJournalStepV1::ReadyForCutover => {
            W::ContractMigrationJournalStepReadyForCutover as i32
        }
        ContractMigrationJournalStepV1::Complete => W::ContractMigrationJournalStepComplete as i32,
    }
}

fn step_from_proto(value: i32) -> Result<ContractMigrationJournalStepV1, DurableCodecError> {
    use wire::ContractMigrationJournalStepV1 as W;
    match W::try_from(value).map_err(|_| DurableCodecError::corrupt())? {
        W::ContractMigrationJournalStepTransforming => {
            Ok(ContractMigrationJournalStepV1::Transforming)
        }
        W::ContractMigrationJournalStepRebuildingProjections => {
            Ok(ContractMigrationJournalStepV1::RebuildingProjections)
        }
        W::ContractMigrationJournalStepValidating => Ok(ContractMigrationJournalStepV1::Validating),
        W::ContractMigrationJournalStepReadyForCutover => {
            Ok(ContractMigrationJournalStepV1::ReadyForCutover)
        }
        W::ContractMigrationJournalStepComplete => Ok(ContractMigrationJournalStepV1::Complete),
        W::ContractMigrationJournalStepUnspecified => Err(DurableCodecError::corrupt()),
    }
}

/// Encodes one canonical migration journal envelope.
pub fn encode_contract_migration_journal_v1(
    value: &StoredContractMigrationJournalV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    if value
        .computed_hash()
        .map_err(DurableCodecError::from_storage_value)?
        != value.journal_hash()
    {
        return Err(DurableCodecError::corrupt());
    }
    encode_message(
        JOURNAL,
        &wire::StoredContractMigrationJournalV1 {
            database_id: value.database_id().as_bytes().to_vec(),
            operation_id: value.operation_id().as_bytes().to_vec(),
            semantic_input_hash: value.input_hash().as_bytes().to_vec(),
            artifacts: Some(artifacts_to_proto(value.artifacts())),
            step: step_to_proto(value.step()),
            exclusive_cursor: value
                .cursor()
                .exclusive_lower_bound()
                .map(entity_target_to_proto),
            checked_rows: value.checked_rows(),
            changed_rows: value.changed_rows(),
            batch_count: value.batch_count(),
            frozen_application_frontier: value
                .frozen_application_frontier()
                .map(CommitSequence::get),
            required_projection_ids: value
                .required_projections()
                .iter()
                .map(|id| id.get())
                .collect(),
            previous_journal_hash: value.previous_hash().map(|hash| hash.as_bytes().to_vec()),
            journal_hash: value.journal_hash().as_bytes().to_vec(),
        },
    )
}

/// Decodes one canonical migration journal envelope.
pub fn decode_contract_migration_journal_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredContractMigrationJournalV1>, DurableCodecError> {
    decode_message::<wire::StoredContractMigrationJournalV1, _, _>(JOURNAL, encoded, |value| {
        let cursor = value
            .exclusive_cursor
            .map(entity_target_from_proto)
            .transpose()?
            .map_or_else(MigrationScanCursor::start, MigrationScanCursor::after);
        let projections = value
            .required_projection_ids
            .into_iter()
            .map(|id| ProjectionId::new(id).ok_or_else(DurableCodecError::corrupt))
            .collect::<Result<Vec<_>, _>>()?;
        StoredContractMigrationJournalV1::from_stored_parts(
            DatabaseId::from_bytes(fixed(value.database_id)?)
                .map_err(|_| DurableCodecError::corrupt())?,
            ContractMigrationOperationId::from_bytes(fixed(value.operation_id)?)
                .map_err(|_| DurableCodecError::corrupt())?,
            ContractMigrationInputHash::from_bytes(fixed(value.semantic_input_hash)?),
            artifacts_from_proto(require(value.artifacts)?)?,
            step_from_proto(value.step)?,
            cursor,
            value.checked_rows,
            value.changed_rows,
            value.batch_count,
            value
                .frozen_application_frontier
                .map(|sequence| {
                    CommitSequence::new(sequence).ok_or_else(DurableCodecError::corrupt)
                })
                .transpose()?,
            projections,
            value
                .previous_journal_hash
                .map(|hash| fixed(hash).map(ContractMigrationJournalHash::from_bytes))
                .transpose()?,
            ContractMigrationJournalHash::from_bytes(fixed(value.journal_hash)?),
        )
        .map_err(DurableCodecError::from_storage_value)
    })
}

/// Encodes one permanent migration record envelope.
pub fn encode_contract_migration_record_v1(
    value: &StoredContractMigrationRecordV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(
        RECORD,
        &wire::StoredContractMigrationRecordV1 {
            database_id: value.database_id().as_bytes().to_vec(),
            operation_id: value.operation_id().as_bytes().to_vec(),
            semantic_input_hash: value.input_hash().as_bytes().to_vec(),
            artifacts: Some(artifacts_to_proto(value.artifacts())),
            operation_artifacts: Some(operation_artifacts_to_proto(value.operation_artifacts())),
            source_backup_name: value.source_backup_name().as_str().to_owned(),
            source_backup_manifest_checksum: value.source_backup_manifest().as_bytes().to_vec(),
            principal: Some(audit_principal_to_proto(value.principal())),
            approval_id: value.approval_id().map(|id| id.as_str().to_owned()),
            predecessor_application_frontier: value.predecessor_frontier().map(CommitSequence::get),
            successor_application_frontier: value.successor_frontier().map(CommitSequence::get),
            checked_rows: value.checked_rows(),
            changed_rows: value.changed_rows(),
            batch_count: value.batch_count(),
            terminal_validation_digest: value.validation_digest().as_bytes().to_vec(),
            administration_sequence: value.administration_sequence().get(),
        },
    )
}

/// Decodes one permanent migration record envelope.
pub fn decode_contract_migration_record_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredContractMigrationRecordV1>, DurableCodecError> {
    decode_message::<wire::StoredContractMigrationRecordV1, _, _>(RECORD, encoded, |value| {
        StoredContractMigrationRecordV1::new(
            DatabaseId::from_bytes(fixed(value.database_id)?)
                .map_err(|_| DurableCodecError::corrupt())?,
            ContractMigrationOperationId::from_bytes(fixed(value.operation_id)?)
                .map_err(|_| DurableCodecError::corrupt())?,
            ContractMigrationInputHash::from_bytes(fixed(value.semantic_input_hash)?),
            artifacts_from_proto(require(value.artifacts)?)?,
            operation_artifacts_from_proto(require(value.operation_artifacts)?)?,
            riffdb_types::BackupNameV1::new(value.source_backup_name)
                .map_err(|_| DurableCodecError::corrupt())?,
            BackupIntegrityChecksumV1::new(value.source_backup_manifest_checksum)
                .map_err(DurableCodecError::from_storage_value)?,
            audit_principal_from_proto(require(value.principal)?)?,
            value
                .approval_id
                .map(ApprovalId::new)
                .transpose()
                .map_err(|_| DurableCodecError::corrupt())?,
            value
                .predecessor_application_frontier
                .map(|v| CommitSequence::new(v).ok_or_else(DurableCodecError::corrupt))
                .transpose()?,
            value
                .successor_application_frontier
                .map(|v| CommitSequence::new(v).ok_or_else(DurableCodecError::corrupt))
                .transpose()?,
            value.checked_rows,
            value.changed_rows,
            value.batch_count,
            ContractMigrationValidationDigest::from_bytes(fixed(value.terminal_validation_digest)?),
            AdministrationSequence::new(value.administration_sequence)
                .ok_or_else(DurableCodecError::corrupt)?,
        )
        .map_err(DurableCodecError::from_storage_value)
    })
}

/// Encodes one predecessor-write retirement envelope.
pub fn encode_contract_write_retirement_v1(
    value: StoredContractWriteRetirementV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    let artifacts = value.artifacts();
    encode_message(
        RETIREMENT,
        &wire::StoredContractWriteRetirementV1 {
            parent_bundle_hash: artifacts.parent().as_bytes().to_vec(),
            candidate_bundle_hash: artifacts.candidate().as_bytes().to_vec(),
            migration_bundle_hash: artifacts.migration().as_bytes().to_vec(),
            operation_id: value.operation_id().as_bytes().to_vec(),
            administration_sequence: value.administration_sequence().get(),
        },
    )
}

/// Decodes one predecessor-write retirement envelope.
pub fn decode_contract_write_retirement_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredContractWriteRetirementV1>, DurableCodecError> {
    decode_message::<wire::StoredContractWriteRetirementV1, _, _>(RETIREMENT, encoded, |value| {
        Ok(StoredContractWriteRetirementV1::new(
            ContractMigrationArtifactsV1::new(
                ContractBundleHash::from_bytes(fixed(value.parent_bundle_hash)?),
                ContractBundleHash::from_bytes(fixed(value.candidate_bundle_hash)?),
                MigrationBundleHash::from_bytes(fixed(value.migration_bundle_hash)?),
            ),
            ContractMigrationOperationId::from_bytes(fixed(value.operation_id)?)
                .map_err(|_| DurableCodecError::corrupt())?,
            AdministrationSequence::new(value.administration_sequence)
                .ok_or_else(DurableCodecError::corrupt)?,
        ))
    })
}

/// Encodes one retained predecessor entity envelope.
pub fn encode_retired_entity_record_v1(
    value: &StoredRetiredEntityRecordV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    let decoded = decode_entity_record_v1(value.original_entity_envelope())?;
    if decoded.value().target() != value.original_target() {
        return Err(DurableCodecError::corrupt());
    }
    encode_message(
        RETIRED_ENTITY,
        &wire::StoredRetiredEntityRecordV1 {
            operation_id: value.operation_id().as_bytes().to_vec(),
            migration_bundle_hash: value.migration().as_bytes().to_vec(),
            original_target: Some(entity_target_to_proto(value.original_target())),
            original_entity_envelope: value.original_entity_envelope().to_vec(),
        },
    )
}

/// Decodes one retained predecessor entity envelope.
pub fn decode_retired_entity_record_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredRetiredEntityRecordV1>, DurableCodecError> {
    decode_message::<wire::StoredRetiredEntityRecordV1, _, _>(RETIRED_ENTITY, encoded, |value| {
        let target = entity_target_from_proto(require(value.original_target)?)?;
        let entity = decode_entity_record_v1(&value.original_entity_envelope)?;
        if entity.value().target() != &target {
            return Err(DurableCodecError::corrupt());
        }
        StoredRetiredEntityRecordV1::new(
            ContractMigrationOperationId::from_bytes(fixed(value.operation_id)?)
                .map_err(|_| DurableCodecError::corrupt())?,
            MigrationBundleHash::from_bytes(fixed(value.migration_bundle_hash)?),
            target,
            value.original_entity_envelope,
        )
        .map_err(DurableCodecError::from_storage_value)
    })
}
