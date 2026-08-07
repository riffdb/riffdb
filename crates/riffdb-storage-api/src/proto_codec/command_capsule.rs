use riffdb_proto::storage::v1 as wire;
use riffdb_types::{
    AdministrationSequence, ApprovalId, CommitSequence, ProvenanceId, RequestId,
    ServiceAuditLinkV1, ServiceAuditPhaseV1, ServiceAuditTargetsV1,
};

use crate::{
    AffectedEntityV1, EncodedPageItem, StoredCommandAuditLocatorV1, StoredCommandAuditMemberV1,
    StoredCommandCapsuleV1, StoredCommandLocatorV1, StoredDurableEventV1, StoredProvenanceRecordV1,
    StoredServiceAuditRecordV1,
};

use super::{
    CanonicalStoredEnvelopeV1, DurableCodecError, actor_to_proto, audit_principal_from_proto,
    audit_principal_to_proto, claims_to_proto, declared_outcome_to_proto, decode_message,
    dependencies_to_proto, encode_message, event_id_to_proto, fixed, identity_to_proto,
    plan_to_proto, require, storage_result, timestamp_from_proto, timestamp_to_proto,
};
use super::{application, audit};

const COMMAND_CAPSULE: &str = "riffdb.storage.v1.StoredCommandCapsuleV1";
const COMMAND_LOCATOR: &str = "riffdb.storage.v1.StoredCommandLocatorV1";
const COMMAND_AUDIT_LOCATOR: &str = "riffdb.storage.v1.StoredCommandAuditLocatorV1";

/// Encodes one canonical successful-command capsule.
pub fn encode_command_capsule_v1(
    value: &StoredCommandCapsuleV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    let outcome = value.outcome();
    let commit = value.commit();
    let started = value.started_audit();
    let terminal = value.terminal_audit();
    debug_assert_eq!(started.request_id(), terminal.request_id());

    encode_message(
        COMMAND_CAPSULE,
        &wire::StoredCommandCapsuleV1 {
            identity: Some(identity_to_proto(outcome.identity())),
            commit_sequence: commit.commit_sequence().get(),
            admission_request_id: commit.admission_request_id().as_bytes().to_vec(),
            plan: Some(plan_to_proto(commit.plan())),
            canonical_input_hash: commit.canonical_input_hash().as_bytes().to_vec(),
            actor: Some(actor_to_proto(commit.actor())),
            logical_time: Some(timestamp_to_proto(commit.logical_time().timestamp())),
            partition_hash: commit.partition_hash().as_bytes().to_vec(),
            conflict_hashes: application::hashes_to_proto(commit.conflict_hashes()),
            declared_outcome: Some(declared_outcome_to_proto(commit.declared_outcome())),
            admitted_claims: Some(claims_to_proto(outcome.admitted_claims())),
            provenance_id: commit.provenance_id().as_bytes().to_vec(),
            durability_mode: application::durability_to_proto(commit.durability_mode()),
            partition_key: outcome.partition_key().as_bytes().to_vec(),
            causation: outcome.causation().map(application::causation_to_proto),
            read_dependencies: Some(dependencies_to_proto(commit.read_dependencies())),
            entity_references: commit
                .entity_references()
                .iter()
                .map(application::entity_reference_to_proto)
                .collect(),
            event_references: commit
                .event_references()
                .into_iter()
                .map(application::event_reference_to_proto)
                .collect(),
            outbox_event_ids: commit
                .outbox_event_ids()
                .iter()
                .copied()
                .map(event_id_to_proto)
                .collect(),
            audit: Some(wire::StoredCommandAuditInvocationV1 {
                request_id: started.request_id().as_bytes().to_vec(),
                operation: i32::from(started.operation().tag()),
                principal: started.principal().map(audit_principal_to_proto),
                ingress: i32::from(started.ingress().tag()),
                targets: started
                    .targets()
                    .as_slice()
                    .iter()
                    .map(audit::target_to_proto_v2)
                    .collect(),
                approval_id: started.approval_id().map(|value| value.as_str().to_owned()),
                started_administration_sequence: started.administration_sequence().get(),
                started_at: Some(timestamp_to_proto(started.timestamp())),
                terminal_administration_sequence: terminal.administration_sequence().get(),
                terminal_at: Some(timestamp_to_proto(terminal.timestamp())),
            }),
        },
    )
}

/// Decodes a capsule and reconstructs all five semantic views using event rows
/// loaded from the same authoritative snapshot.
pub fn decode_command_capsule_v1(
    encoded: &[u8],
    events: Vec<StoredDurableEventV1>,
) -> Result<EncodedPageItem<StoredCommandCapsuleV1>, DurableCodecError> {
    decode_message::<wire::StoredCommandCapsuleV1, _, _>(COMMAND_CAPSULE, encoded, |value| {
        let sequence =
            CommitSequence::new(value.commit_sequence).ok_or_else(DurableCodecError::corrupt)?;
        let provenance_id = ProvenanceId::from_bytes(fixed(value.provenance_id.clone())?)
            .map_err(|_| DurableCodecError::corrupt())?;
        let entity_references = value
            .entity_references
            .into_iter()
            .map(application::entity_reference_from_proto)
            .collect::<Result<Vec<_>, _>>()?;
        let event_references = value
            .event_references
            .into_iter()
            .map(application::event_reference_from_proto)
            .collect::<Result<Vec<_>, _>>()?;
        if !application::events_match_references(&event_references, &events) {
            return Err(DurableCodecError::corrupt());
        }
        let causation = value
            .causation
            .clone()
            .map(application::causation_from_proto)
            .transpose()?;

        let outcome_base = wire::StoredOutcomeV1 {
            identity: value.identity.clone(),
            commit_sequence: value.commit_sequence,
            admission_request_id: value.admission_request_id.clone(),
            plan: value.plan.clone(),
            canonical_input_hash: value.canonical_input_hash.clone(),
            actor: value.actor.clone(),
            logical_time: value.logical_time,
            partition_hash: value.partition_hash.clone(),
            conflict_hashes: value.conflict_hashes.clone(),
            declared_outcome: value.declared_outcome.clone(),
            admitted_claims: value.admitted_claims.clone(),
            provenance_id: value.provenance_id.clone(),
            durability_mode: value.durability_mode,
            partition_key: value.partition_key,
        };
        let mut outcome = application::outcome_from_proto(outcome_base)?;
        if let Some(causation) = causation {
            outcome = storage_result(outcome.with_causation(causation))?;
        }

        let commit = application::commit_from_wire_parts(
            value.commit_sequence,
            value.admission_request_id,
            value.plan,
            value.canonical_input_hash,
            value.actor,
            value.logical_time,
            value.partition_hash,
            value.conflict_hashes,
            value.read_dependencies,
            entity_references,
            events,
            value.declared_outcome,
            value.provenance_id,
            value.outbox_event_ids,
            value.durability_mode,
        )?;

        let affected_entities = commit
            .entity_references()
            .iter()
            .map(|reference| {
                AffectedEntityV1::from_stored_parts(
                    reference.target().clone(),
                    reference.entity_version(),
                )
            })
            .collect();
        let provenance = storage_result(StoredProvenanceRecordV1::new_with_causation(
            provenance_id,
            sequence,
            outcome.identity().clone(),
            commit.admission_request_id(),
            commit.plan().clone(),
            commit.canonical_input_hash(),
            commit.actor().clone(),
            commit.logical_time(),
            commit.partition_hash(),
            commit.conflict_hashes().to_vec(),
            commit.declared_outcome().outcome_id(),
            affected_entities,
            commit.event_ids(),
            outcome.admitted_claims().clone(),
            outcome.causation(),
        ))?;

        let audit = require(value.audit)?;
        let raw_targets = audit
            .targets
            .into_iter()
            .map(audit::target_from_proto_v2)
            .collect::<Result<Vec<_>, _>>()?;
        let targets = ServiceAuditTargetsV1::new(raw_targets.clone())
            .map_err(|_| DurableCodecError::corrupt())?;
        if targets.as_slice() != raw_targets {
            return Err(DurableCodecError::corrupt());
        }
        let request_id = RequestId::from_bytes(fixed(audit.request_id)?)
            .map_err(|_| DurableCodecError::corrupt())?;
        let operation = audit::operation_from_proto(audit.operation)?;
        let principal = audit
            .principal
            .map(audit_principal_from_proto)
            .transpose()?;
        let ingress = audit::ingress_from_proto(audit.ingress)?;
        let approval_id = audit
            .approval_id
            .map(ApprovalId::new)
            .transpose()
            .map_err(|_| DurableCodecError::corrupt())?;
        let started_sequence = AdministrationSequence::new(audit.started_administration_sequence)
            .ok_or_else(DurableCodecError::corrupt)?;
        let terminal_sequence = AdministrationSequence::new(audit.terminal_administration_sequence)
            .ok_or_else(DurableCodecError::corrupt)?;
        let started = storage_result(StoredServiceAuditRecordV1::from_stored_parts(
            started_sequence,
            request_id,
            timestamp_from_proto(require(audit.started_at)?)?,
            operation,
            ServiceAuditPhaseV1::Started,
            principal.clone(),
            ingress,
            targets.clone(),
            approval_id.clone(),
            ServiceAuditLinkV1::None,
        ))?;
        let terminal = storage_result(StoredServiceAuditRecordV1::from_stored_parts(
            terminal_sequence,
            request_id,
            timestamp_from_proto(require(audit.terminal_at)?)?,
            operation,
            ServiceAuditPhaseV1::Succeeded,
            principal,
            ingress,
            targets,
            approval_id,
            ServiceAuditLinkV1::Command {
                commit_sequence: sequence,
                provenance_id,
            },
        ))?;

        storage_result(StoredCommandCapsuleV1::new(
            outcome, provenance, commit, started, terminal,
        ))
    })
}

/// Encodes one successful-command lookup locator.
pub fn encode_command_locator_v1(
    value: StoredCommandLocatorV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    encode_message(
        COMMAND_LOCATOR,
        &wire::StoredCommandLocatorV1 {
            commit_sequence: value.commit_sequence().get(),
        },
    )
}

/// Decodes one successful-command lookup locator.
pub fn decode_command_locator_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredCommandLocatorV1>, DurableCodecError> {
    decode_message::<wire::StoredCommandLocatorV1, _, _>(COMMAND_LOCATOR, encoded, |value| {
        Ok(StoredCommandLocatorV1::new(
            CommitSequence::new(value.commit_sequence).ok_or_else(DurableCodecError::corrupt)?,
        ))
    })
}

/// Encodes one command-owned audit locator.
pub fn encode_command_audit_locator_v1(
    value: StoredCommandAuditLocatorV1,
) -> Result<CanonicalStoredEnvelopeV1, DurableCodecError> {
    let member = match value.member() {
        StoredCommandAuditMemberV1::Started => {
            wire::StoredCommandAuditMemberV1::StoredCommandAuditMemberStarted
        }
        StoredCommandAuditMemberV1::Terminal => {
            wire::StoredCommandAuditMemberV1::StoredCommandAuditMemberTerminal
        }
    };
    encode_message(
        COMMAND_AUDIT_LOCATOR,
        &wire::StoredCommandAuditLocatorV1 {
            commit_sequence: value.commit_sequence().get(),
            member: member as i32,
        },
    )
}

/// Decodes one command-owned audit locator.
pub fn decode_command_audit_locator_v1(
    encoded: &[u8],
) -> Result<EncodedPageItem<StoredCommandAuditLocatorV1>, DurableCodecError> {
    decode_message::<wire::StoredCommandAuditLocatorV1, _, _>(
        COMMAND_AUDIT_LOCATOR,
        encoded,
        |value| {
            let member = match wire::StoredCommandAuditMemberV1::try_from(value.member)
                .map_err(|_| DurableCodecError::corrupt())?
            {
                wire::StoredCommandAuditMemberV1::StoredCommandAuditMemberStarted => {
                    StoredCommandAuditMemberV1::Started
                }
                wire::StoredCommandAuditMemberV1::StoredCommandAuditMemberTerminal => {
                    StoredCommandAuditMemberV1::Terminal
                }
                wire::StoredCommandAuditMemberV1::StoredCommandAuditMemberUnspecified => {
                    return Err(DurableCodecError::corrupt());
                }
            };
            Ok(StoredCommandAuditLocatorV1::new(
                CommitSequence::new(value.commit_sequence)
                    .ok_or_else(DurableCodecError::corrupt)?,
                member,
            ))
        },
    )
}
