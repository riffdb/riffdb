//! Derive a private command cut, never a synthetic source receipt.
use super::*;
use crate::keys::*;
use riffdb_storage_api::{
    AdministrationSequenceAllocator, ApplicationSequenceAllocator,
    AuthoritativeMutationAccumulatorV3, AuthoritativeMutationV3 as M, AuthoritativeTransactionV3,
    CommandPrefixEvidenceV1, CommandSegmentDigestV1, StoredCommandLocatorV1,
    StoredCommandSegmentV1, proto_codec::*,
};

pub(super) fn derive(
    receipt: &AuthoritativeTransactionV3,
    stop: CommitSequence,
) -> Result<(DualFrontier, Vec<M>), StorageError> {
    let mut expected_graph = Vec::new();
    let mut selected = AuthoritativeMutationAccumulatorV3::default();
    let mut frontier = None;
    for mutation in receipt
        .mutations()
        .iter()
        .filter(|m| m.namespace() == N::Commits)
    {
        let bytes = mutation.value().ok_or_else(corrupt)?;
        let decoded = crate::command_prefix::decode_segment(bytes).map_err(|_| corrupt())?;
        let segment = decoded.value();
        if mutation.expected_hash().is_some()
            || crate::application::build_command_segment_manifest(
                segment.commands(),
                segment.first_commit_sequence(),
            )? != *segment.manifest()
        {
            return Err(corrupt());
        }
        expected_graph.push(mutation.clone());
        let mut kept = Vec::new();
        for command in segment.commands() {
            let base = command.base();
            let prefix = command.prefix_evidence().ok_or_else(corrupt)?;
            let locator =
                encode_command_locator_v1(StoredCommandLocatorV1::new(base.commit_sequence()))
                    .map_err(|_| corrupt())?;
            let mut locators = vec![
                put(
                    N::IdempotencyLocators,
                    encode_idempotency_key(
                        &base
                            .outcome()
                            .identity()
                            .storage_key()
                            .map_err(|_| corrupt())?,
                    ),
                    locator.as_bytes(),
                )?,
                put(
                    N::ProvenanceLocators,
                    &encode_provenance_key(base.provenance().provenance_id()),
                    locator.as_bytes(),
                )?,
            ];
            for audit in [base.started_audit(), base.terminal_audit()] {
                locators.push(put(
                    N::AuditByRequestLocators,
                    &encode_audit_by_request_key(
                        audit.request_id(),
                        audit.administration_sequence(),
                    ),
                    locator.as_bytes(),
                )?);
            }
            if base.commit_sequence() <= stop {
                for mutation in prefix.mutations().iter().chain(&locators) {
                    selected.record(mutation.clone()).map_err(|_| corrupt())?;
                }
                kept.push(command.clone());
                frontier = Some(prefix.covered());
            }
            expected_graph.extend(locators);
        }
        if !kept.is_empty() {
            let value = if kept.len() == segment.commands().len() {
                bytes.to_vec()
            } else {
                let manifest = crate::application::build_command_segment_manifest(
                    &kept,
                    segment.first_commit_sequence(),
                )?;
                let cut = StoredCommandSegmentV1::new(
                    segment.database_id(),
                    segment.history_incarnation(),
                    segment.predecessor_segment_digest(),
                    kept,
                    manifest,
                    CommandSegmentDigestV1::from_bytes([0; 32]),
                )
                .map_err(|_| corrupt())?;
                seal_and_encode_command_segment_v1(cut)
                    .map_err(|_| corrupt())?
                    .1
                    .as_bytes()
                    .to_vec()
            };
            selected
                .record(put(N::Commits, mutation.key(), &value)?)
                .map_err(|_| corrupt())?;
        }
    }
    let frontier = frontier
        .filter(|f| f.application() == Some(stop))
        .ok_or_else(corrupt)?;
    for namespace in [N::NextApplicationSequence, N::NextAdministrationSequence] {
        let original = receipt
            .mutations()
            .iter()
            .find(|m| m.namespace() == namespace)
            .ok_or_else(corrupt)?;
        let key = namespace.metadata_key().ok_or_else(corrupt)?.as_bytes();
        expected_graph.push(
            M::put(
                namespace,
                key,
                original.expected_hash(),
                &allocator(namespace, receipt.binding().covered_frontier)?,
            )
            .map_err(|_| corrupt())?,
        );
        selected
            .record(
                M::put(
                    namespace,
                    key,
                    original.expected_hash(),
                    &allocator(namespace, frontier)?,
                )
                .map_err(|_| corrupt())?,
            )
            .map_err(|_| corrupt())?;
    }
    expected_graph.sort_by(|a, b| (a.namespace(), a.key()).cmp(&(b.namespace(), b.key())));
    let actual = receipt
        .mutations()
        .iter()
        .filter(|m| !CommandPrefixEvidenceV1::supports_namespace(m.namespace()));
    if !actual.eq(expected_graph.iter()) {
        return Err(corrupt());
    }
    Ok((frontier, selected.finish().map_err(|_| corrupt())?))
}

fn put(namespace: N, key: &[u8], value: &[u8]) -> Result<M, StorageError> {
    M::put(namespace, key, None, value).map_err(|_| corrupt())
}

fn allocator(namespace: N, frontier: DualFrontier) -> Result<Vec<u8>, StorageError> {
    let encoded = match namespace {
        N::NextApplicationSequence => {
            let last = frontier.application().ok_or_else(corrupt)?;
            encode_application_sequence_allocator_v1(last.checked_next().map_or(
                ApplicationSequenceAllocator::Exhausted,
                ApplicationSequenceAllocator::Next,
            ))
        }
        N::NextAdministrationSequence => {
            let last = frontier.administration().ok_or_else(corrupt)?;
            encode_administration_sequence_allocator_v1(last.checked_next().map_or(
                AdministrationSequenceAllocator::Exhausted,
                AdministrationSequenceAllocator::Next,
            ))
        }
        _ => return Err(corrupt()),
    }
    .map_err(|_| corrupt())?;
    Ok(encoded.as_bytes().to_vec())
}
