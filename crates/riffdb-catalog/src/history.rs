//! Same-session exact-end historical catalog validation.

use std::fmt;
use std::sync::Arc;

use riffdb_contract_ir::{CompatibilityClass, IndexSchema};
use riffdb_storage_api::{
    EvidencePageLimit, HistoricalBundleEvidence, HistoricalEvidenceCursor, HistoricalEvidenceEnd,
    HistoricalEvidencePage, HistoricalPersistedKeyEvidenceV1, HistoricalSemanticEvidence,
    IrOpaquePersistedKeyV1, OpenSessionId, StructuralEvidenceSession,
};
use riffdb_types::{DatabaseId, IndexId};

use crate::lineage::{LineageBudget, LineageMaterializationProof};
use crate::{
    CatalogError, CatalogErrorKind, ValidatedContractBundle, validate_capability_partition,
    validate_successor_compatibility,
};

/// Process-local proof that every catalog history item was IR-validated to exact end.
///
/// Its constructor is private, it is not serializable, and it never crosses a
/// storage trait. WP-130 may only combine it with the matching structural handoff.
pub struct ValidatedCatalogHistory {
    database_id: DatabaseId,
    open_session_id: OpenSessionId,
    active: Option<ValidatedContractBundle>,
    lineage_proof: Option<Arc<LineageMaterializationProof>>,
    evidence_count: u64,
}

impl ValidatedCatalogHistory {
    /// Durable database identity bound to the exclusive startup session.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Process-local startup session identity.
    #[must_use]
    pub const fn open_session_id(&self) -> OpenSessionId {
        self.open_session_id
    }

    /// Checked active bundle, or absence before first deployment.
    #[must_use]
    pub const fn active(&self) -> Option<&ValidatedContractBundle> {
        self.active.as_ref()
    }

    /// Number of bounded evidence items consumed before exact end.
    #[must_use]
    pub const fn evidence_count(&self) -> u64 {
        self.evidence_count
    }

    /// Checks the exact database/open-session pair for readiness composition.
    #[must_use]
    pub fn matches(&self, database_id: DatabaseId, open_session_id: OpenSessionId) -> bool {
        self.database_id == database_id && self.open_session_id == open_session_id
    }
}

impl fmt::Debug for ValidatedCatalogHistory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ValidatedCatalogHistory")
            .field("database_id", &self.database_id)
            .field("open_session_id", &self.open_session_id)
            .field("active", &self.active.as_ref().map(|_| "[CHECKED]"))
            .field(
                "lineage_proof",
                &self.lineage_proof.as_ref().map(|_| "[CHECKED]"),
            )
            .field("evidence_count", &self.evidence_count)
            .finish()
    }
}

/// Catalog proof paired with storage's unforgeable historical exact-end token.
pub struct CatalogHistoryValidation<E> {
    history: ValidatedCatalogHistory,
    historical_end: E,
}

#[derive(Default)]
struct HistoricalValidationState {
    active_seen: bool,
    active: Option<ValidatedContractBundle>,
    terminal_bundle: Option<ValidatedContractBundle>,
    lineage_bundles: Vec<ValidatedContractBundle>,
    lineage_budget: LineageBudget,
    lineage_proof: Option<Arc<LineageMaterializationProof>>,
}

impl<E> CatalogHistoryValidation<E> {
    /// Borrows the opaque catalog-owned proof.
    #[must_use]
    pub const fn history(&self) -> &ValidatedCatalogHistory {
        &self.history
    }

    /// Consumes the pair for WP-130's structural finish and readiness join.
    #[must_use]
    pub fn into_parts(self) -> (ValidatedCatalogHistory, E) {
        (self.history, self.historical_end)
    }
}

/// Consumes every same-session historical page through an exact end marker.
pub fn validate_catalog_history<S: StructuralEvidenceSession>(
    session: &mut S,
) -> Result<CatalogHistoryValidation<S::HistoricalEnd>, CatalogError> {
    let database_id = session.database_id();
    let open_session_id = session.open_session_id();
    let mut cursor = HistoricalEvidenceCursor::start(database_id, open_session_id);
    let page_limit = EvidencePageLimit::new(500)
        .ok_or_else(|| CatalogError::new(CatalogErrorKind::InvalidHistoricalEvidence))?;
    let mut last_order_key: Option<Vec<u8>> = None;
    let mut state = HistoricalValidationState::default();
    let mut evidence_count = 0u64;

    let historical_end = loop {
        match session.read_historical_evidence(cursor, page_limit)? {
            HistoricalEvidencePage::Page {
                start,
                evidence,
                next,
            } => {
                let amount = u64::try_from(evidence.len())
                    .map_err(|_| CatalogError::new(CatalogErrorKind::InvalidHistoricalEvidence))?;
                if amount == 0 || amount > u64::from(page_limit.get()) {
                    return Err(CatalogError::new(
                        CatalogErrorKind::InvalidHistoricalEvidence,
                    ));
                }
                let expected_next = cursor
                    .advanced(amount)
                    .map_err(|_| CatalogError::new(CatalogErrorKind::InvalidHistoricalEvidence))?;
                if start != cursor || next == cursor || next != expected_next {
                    return Err(CatalogError::new(
                        CatalogErrorKind::InvalidHistoricalEvidence,
                    ));
                }

                for item in &evidence {
                    let order_key = historical_order_key(item);
                    if last_order_key
                        .as_ref()
                        .is_some_and(|prior| prior >= &order_key)
                    {
                        return Err(CatalogError::new(
                            CatalogErrorKind::InvalidHistoricalEvidence,
                        ));
                    }
                    validate_historical_item(session, item, &mut state)?;
                    last_order_key = Some(order_key);
                    evidence_count = evidence_count.checked_add(1).ok_or_else(|| {
                        CatalogError::new(CatalogErrorKind::InvalidHistoricalEvidence)
                    })?;
                }
                cursor = next;
            }
            HistoricalEvidencePage::ExactEnd(end) => {
                if end.cursor() != cursor {
                    return Err(CatalogError::new(
                        CatalogErrorKind::InvalidHistoricalEvidence,
                    ));
                }
                break end;
            }
        }
    };

    if !state.active_seen
        || !active_matches_terminal(state.active.as_ref(), state.terminal_bundle.as_ref())
    {
        return Err(CatalogError::new(
            CatalogErrorKind::InvalidHistoricalEvidence,
        ));
    }

    Ok(CatalogHistoryValidation {
        history: ValidatedCatalogHistory {
            database_id,
            open_session_id,
            active: state.active,
            lineage_proof: state.lineage_proof,
            evidence_count,
        },
        historical_end,
    })
}

fn validate_historical_item<S: StructuralEvidenceSession>(
    session: &mut S,
    item: &HistoricalSemanticEvidence,
    state: &mut HistoricalValidationState,
) -> Result<(), CatalogError> {
    match item {
        HistoricalSemanticEvidence::Bundle(evidence) => {
            if state.lineage_proof.is_some() {
                return Err(CatalogError::new(
                    CatalogErrorKind::InvalidHistoricalEvidence,
                ));
            }
            state
                .lineage_budget
                .push_bundle(evidence.bytes().as_bytes().len())
                .map_err(map_history_lineage_error)?;
            let bundle = validate_bundle_evidence(evidence)?;
            validate_historical_bundle_parent(&bundle, state.terminal_bundle.as_ref())?;
            state.lineage_bundles.push(bundle.clone());
            state.terminal_bundle = Some(bundle);
        }
        HistoricalSemanticEvidence::PlanReference(reference) => {
            let proof = ensure_lineage_proof(&state.lineage_bundles, &mut state.lineage_proof)?;
            let (ordinal, bundle) = proof
                .exact_member(
                    reference.contract_version(),
                    reference.contract_bundle_hash(),
                )
                .ok_or_else(|| CatalogError::new(CatalogErrorKind::UnknownExecutablePlan))?;
            bundle.resolve_plan_with_proof(reference, Arc::clone(proof), ordinal)?;
        }
        HistoricalSemanticEvidence::ActiveCatalog(observed) => {
            if state.active_seen {
                return Err(CatalogError::new(
                    CatalogErrorKind::InvalidHistoricalEvidence,
                ));
            }
            state.active_seen = true;
            state.active = match observed {
                Some(pointer) => {
                    let loaded = load_historical_bundle(
                        session,
                        pointer.lineage(),
                        pointer.version(),
                        pointer.bundle_hash(),
                    )?;
                    let proof =
                        ensure_lineage_proof(&state.lineage_bundles, &mut state.lineage_proof)?;
                    if proof
                        .exact_member(pointer.version(), pointer.bundle_hash())
                        .is_none()
                    {
                        return Err(CatalogError::new(
                            CatalogErrorKind::InvalidHistoricalEvidence,
                        ));
                    }
                    Some(loaded)
                }
                None => None,
            };
        }
        HistoricalSemanticEvidence::PersistedKey(evidence) => {
            let proof = ensure_lineage_proof(&state.lineage_bundles, &mut state.lineage_proof)?;
            validate_persisted_key(session, proof, evidence)?;
        }
        HistoricalSemanticEvidence::CapabilityPartition(evidence) => {
            let active = state
                .active
                .as_ref()
                .ok_or_else(|| CatalogError::new(CatalogErrorKind::InvalidHistoricalEvidence))?;
            validate_capability_partition(active, evidence.scoped_partition())
                .map_err(|_| CatalogError::new(CatalogErrorKind::InvalidHistoricalEvidence))?;
        }
    }
    Ok(())
}

fn validate_historical_bundle_parent(
    candidate: &ValidatedContractBundle,
    prior: Option<&ValidatedContractBundle>,
) -> Result<(), CatalogError> {
    let Some(prior) = prior else {
        if candidate.bundle().parent().is_some()
            || candidate.bundle().compatibility().overall() != CompatibilityClass::Compatible
            || !candidate.bundle().compatibility().entries().is_empty()
        {
            return Err(CatalogError::new(
                CatalogErrorKind::InvalidHistoricalEvidence,
            ));
        }
        return Ok(());
    };

    let Some(parent_reference) = candidate.bundle().parent() else {
        return Err(CatalogError::new(
            CatalogErrorKind::InvalidHistoricalEvidence,
        ));
    };
    if candidate.lineage() != prior.lineage()
        || parent_reference.contract_version() != prior.contract_version()
        || parent_reference.bundle_hash() != prior.bundle_hash()
    {
        return Err(CatalogError::new(
            CatalogErrorKind::InvalidHistoricalEvidence,
        ));
    }
    validate_successor_compatibility(candidate, prior).map_err(map_history_lineage_error)
}

fn active_matches_terminal(
    active: Option<&ValidatedContractBundle>,
    terminal: Option<&ValidatedContractBundle>,
) -> bool {
    match (active, terminal) {
        (None, None) => true,
        (Some(active), Some(terminal)) => {
            active.lineage() == terminal.lineage()
                && active.contract_version() == terminal.contract_version()
                && active.bundle_hash() == terminal.bundle_hash()
        }
        _ => false,
    }
}

fn validate_persisted_key<S: StructuralEvidenceSession>(
    session: &mut S,
    lineage_proof: &LineageMaterializationProof,
    evidence: &HistoricalPersistedKeyEvidenceV1,
) -> Result<(), CatalogError> {
    let binding = evidence.schema();
    let bundle = load_historical_bundle(
        session,
        binding.lineage(),
        binding.contract_version(),
        binding.bundle_hash(),
    )?;
    if lineage_proof.exact_binding_member(binding).is_none() {
        return Err(CatalogError::new(
            CatalogErrorKind::InvalidHistoricalEvidence,
        ));
    }

    let valid = match evidence.key() {
        IrOpaquePersistedKeyV1::Entity {
            entity_type_id,
            key,
        } => bundle
            .bundle()
            .schema()
            .entity(*entity_type_id)
            .is_some_and(|entity| entity.primary_key().decode_entity(key).is_ok()),
        IrOpaquePersistedKeyV1::IndexEntry { index_id, key } => {
            find_index(bundle.bundle().schema().entities(), *index_id)
                .is_some_and(|index| index.key_schema().decode_index(key).is_ok())
        }
        IrOpaquePersistedKeyV1::IndexRangePrefix(prefix) => {
            find_index(bundle.bundle().schema().entities(), prefix.index_id()).is_some_and(
                |index| {
                    index
                        .key_schema()
                        .decode_index_prefix(prefix.as_bytes())
                        .is_ok()
                },
            )
        }
    };
    if !valid {
        return Err(CatalogError::new(CatalogErrorKind::InvalidHistoricalKey));
    }
    Ok(())
}

fn ensure_lineage_proof<'a>(
    bundles: &[ValidatedContractBundle],
    proof: &'a mut Option<Arc<LineageMaterializationProof>>,
) -> Result<&'a Arc<LineageMaterializationProof>, CatalogError> {
    if proof.is_none() {
        let checked = LineageMaterializationProof::from_forward_bundles(bundles.to_vec())
            .map_err(map_history_lineage_error)?;
        *proof = Some(checked);
    }
    proof
        .as_ref()
        .ok_or_else(|| CatalogError::new(CatalogErrorKind::InvalidHistoricalEvidence))
}

fn map_history_lineage_error(_error: CatalogError) -> CatalogError {
    CatalogError::new(CatalogErrorKind::InvalidHistoricalEvidence)
}

fn find_index(
    entities: &[riffdb_contract_ir::EntitySchema],
    index_id: IndexId,
) -> Option<&IndexSchema> {
    entities
        .iter()
        .flat_map(riffdb_contract_ir::EntitySchema::indexes)
        .find(|index| index.id() == index_id)
}

fn load_historical_bundle<S: StructuralEvidenceSession>(
    session: &mut S,
    lineage: &riffdb_types::ContractLineage,
    version: riffdb_types::ContractVersion,
    bundle_hash: riffdb_types::ContractBundleHash,
) -> Result<ValidatedContractBundle, CatalogError> {
    let evidence = session
        .read_historical_bundle(lineage, version, bundle_hash)?
        .ok_or_else(|| CatalogError::new(CatalogErrorKind::InvalidHistoricalEvidence))?;
    validate_bundle_evidence(&evidence)
}

fn validate_bundle_evidence(
    evidence: &HistoricalBundleEvidence,
) -> Result<ValidatedContractBundle, CatalogError> {
    let bundle = ValidatedContractBundle::decode(evidence.bytes().as_bytes())?;
    if bundle.lineage() != evidence.lineage()
        || bundle.contract_version() != evidence.version()
        || bundle.bundle_hash() != evidence.bundle_hash()
    {
        return Err(CatalogError::new(
            CatalogErrorKind::InvalidHistoricalEvidence,
        ));
    }
    Ok(bundle)
}

fn historical_order_key(item: &HistoricalSemanticEvidence) -> Vec<u8> {
    let mut key = Vec::new();
    match item {
        HistoricalSemanticEvidence::Bundle(bundle) => {
            key.push(0x01);
            push_lineage(&mut key, bundle.lineage());
            key.extend_from_slice(&bundle.version().to_be_bytes());
            key.extend_from_slice(bundle.bundle_hash().as_bytes());
        }
        HistoricalSemanticEvidence::PlanReference(plan) => {
            key.push(0x02);
            push_lineage(&mut key, plan.contract_lineage());
            key.extend_from_slice(&plan.contract_version().to_be_bytes());
            key.extend_from_slice(plan.contract_bundle_hash().as_bytes());
            key.extend_from_slice(&plan.command_id().to_be_bytes());
            key.extend_from_slice(plan.command_plan_hash().as_bytes());
        }
        HistoricalSemanticEvidence::ActiveCatalog(None) => key.extend_from_slice(&[0x03, 0x00]),
        HistoricalSemanticEvidence::ActiveCatalog(Some(active)) => {
            key.extend_from_slice(&[0x03, 0x01]);
            push_lineage(&mut key, active.lineage());
            key.extend_from_slice(&active.version().to_be_bytes());
            key.extend_from_slice(active.bundle_hash().as_bytes());
        }
        HistoricalSemanticEvidence::PersistedKey(evidence) => {
            key.push(0x04);
            push_lineage(&mut key, evidence.schema().lineage());
            key.extend_from_slice(&evidence.schema().contract_version().to_be_bytes());
            key.extend_from_slice(evidence.schema().bundle_hash().as_bytes());
            let (tag, owner, bytes) = match evidence.key() {
                IrOpaquePersistedKeyV1::Entity {
                    entity_type_id,
                    key,
                } => (0x01, entity_type_id.to_be_bytes(), key.as_bytes()),
                IrOpaquePersistedKeyV1::IndexEntry { index_id, key } => {
                    (0x02, index_id.to_be_bytes(), key.as_bytes())
                }
                IrOpaquePersistedKeyV1::IndexRangePrefix(prefix) => {
                    (0x03, prefix.index_id().to_be_bytes(), prefix.as_bytes())
                }
            };
            key.push(tag);
            key.extend_from_slice(&owner);
            let length = u32::try_from(bytes.len()).unwrap_or(u32::MAX);
            key.extend_from_slice(&length.to_be_bytes());
            key.extend_from_slice(bytes);
        }
        HistoricalSemanticEvidence::CapabilityPartition(evidence) => {
            key.push(0x05);
            key.extend_from_slice(evidence.capability_id().as_bytes());
            key.extend_from_slice(&evidence.entry_ordinal().to_be_bytes());
            push_lineage(&mut key, evidence.scoped_partition().lineage());
            let partition_key = evidence.scoped_partition().partition_key();
            key.extend_from_slice(&partition_key.aggregate_type_id().to_be_bytes());
            let length = u32::try_from(partition_key.as_bytes().len()).unwrap_or(u32::MAX);
            key.extend_from_slice(&length.to_be_bytes());
            key.extend_from_slice(partition_key.as_bytes());
        }
    }
    key
}

fn push_lineage(output: &mut Vec<u8>, lineage: &riffdb_types::ContractLineage) {
    let length = u32::try_from(lineage.as_bytes().len()).unwrap_or(u32::MAX);
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(lineage.as_bytes());
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU16, NonZeroU32};

    use riffdb_storage_api::{
        CapabilityGrantV1, CapabilityPermissionKindV1, CapabilityPermissionV1,
        CapabilityPermissionsV1, CapabilityRequestedRecordV1,
        HistoricalCapabilityPartitionEvidenceV1, StoredCapabilityRecordV1,
    };
    use riffdb_types::{
        ActorId, ActorKind, AdministrationSequence, AggregateTypeId, Audience, CapabilityId,
        CapabilityTokenDigest, ContractLineage, DatabaseId, DigestKeyId, Environment,
        PartitionKeyBuilder, PartitionScopeV1, RequestId, ScopedPartitionV1, TenantScope,
        Timestamp,
    };

    use super::*;
    use crate::lineage::{MAX_ACTIVE_LINEAGE_BUNDLES_V1, MAX_ACTIVE_LINEAGE_CANONICAL_BYTES_V1};

    #[test]
    fn startup_maps_incremental_lineage_limits_to_invalid_history() {
        let mut count = LineageBudget::default();
        for _ in 0..MAX_ACTIVE_LINEAGE_BUNDLES_V1 {
            count.push_bundle(0).expect("exact candidate count");
        }
        let candidate_error = count.push_bundle(0).expect_err("candidate one over");
        assert_eq!(
            candidate_error.kind(),
            CatalogErrorKind::LineageBundleCountLimit
        );
        assert_eq!(
            map_history_lineage_error(candidate_error).kind(),
            CatalogErrorKind::InvalidHistoricalEvidence
        );

        let mut bytes = LineageBudget::default();
        bytes
            .push_bundle(MAX_ACTIVE_LINEAGE_CANONICAL_BYTES_V1)
            .expect("exact candidate bytes");
        let candidate_error = bytes.push_bundle(1).expect_err("candidate one byte over");
        assert_eq!(
            candidate_error.kind(),
            CatalogErrorKind::LineageCanonicalBytesLimit
        );
        assert_eq!(
            map_history_lineage_error(candidate_error).kind(),
            CatalogErrorKind::InvalidHistoricalEvidence
        );
    }

    #[test]
    fn capability_partition_history_order_matches_the_shared_golden_vector() {
        let mut first_key =
            PartitionKeyBuilder::new(AggregateTypeId::new(1).expect("aggregate ID"));
        first_key.push_u64(1).expect("bounded component");
        let mut second_key =
            PartitionKeyBuilder::new(AggregateTypeId::new(2).expect("aggregate ID"));
        second_key.push_u64(2).expect("bounded component");
        let mut golden_key =
            PartitionKeyBuilder::new(AggregateTypeId::new(0x0102_0304).expect("aggregate ID"));
        golden_key
            .push_u64(0x0102_0304_0506_0708)
            .expect("bounded component");
        let scope = PartitionScopeV1::explicit(vec![
            ScopedPartitionV1::new(
                ContractLineage::new("a").expect("lineage"),
                first_key.finish().expect("first key"),
            ),
            ScopedPartitionV1::new(
                ContractLineage::new("budget").expect("lineage"),
                second_key.finish().expect("second key"),
            ),
            ScopedPartitionV1::new(
                ContractLineage::new("budget").expect("lineage"),
                golden_key.finish().expect("golden key"),
            ),
        ])
        .expect("canonical explicit scope");
        let permissions = CapabilityPermissionsV1::new(vec![
            CapabilityPermissionV1::unparameterized(
                CapabilityPermissionKindV1::AdministerCapabilities,
            )
            .expect("permission"),
        ])
        .expect("permissions");
        let grant = CapabilityGrantV1::new(
            TenantScope::Global,
            scope,
            permissions,
            Vec::new(),
            NonZeroU16::MIN,
            Vec::new(),
        )
        .expect("grant");
        let uuid = [
            0x01, 0x8f, 0x00, 0x00, 0x00, 0x00, 0x70, 0x01, 0x80, 0x02, 0x11, 0x22, 0x33, 0x44,
            0x55, 0x66,
        ];
        let requested = CapabilityRequestedRecordV1::new(
            DatabaseId::from_bytes([0x01, 0x8f, 0, 0, 0, 0, 0x70, 1, 0x80, 2, 0, 0, 0, 0, 0, 1])
                .expect("database"),
            Environment::new("test").expect("environment"),
            ActorId::new("operator").expect("actor"),
            ActorKind::Human,
            NonZeroU32::new(60).expect("duration"),
            vec![Audience::new("riffdb-test").expect("audience")],
            grant,
        )
        .expect("requested capability");
        let capability = StoredCapabilityRecordV1::active(
            CapabilityId::from_bytes(uuid).expect("capability UUIDv7"),
            CapabilityTokenDigest::from_hmac_bytes(
                DigestKeyId::new(1).expect("digest key"),
                [0x31; 32],
            ),
            requested,
            Timestamp::new(10, 0).expect("issued at"),
            Timestamp::new(70, 0).expect("expires at"),
            AdministrationSequence::first(),
            RequestId::from_bytes([0x01, 0x8f, 0, 0, 0, 0, 0x70, 1, 0x80, 2, 0, 0, 0, 0, 0, 2])
                .expect("request UUIDv7"),
        )
        .expect("stored capability");
        let evidence =
            HistoricalCapabilityPartitionEvidenceV1::from_capability_entry(&capability, 2)
                .expect("third explicit entry");
        let expected = vec![
            0x05, 0x01, 0x8f, 0x00, 0x00, 0x00, 0x00, 0x70, 0x01, 0x80, 0x02, 0x11, 0x22, 0x33,
            0x44, 0x55, 0x66, 0x00, 0x02, 0x00, 0x00, 0x00, 0x06, b'b', b'u', b'd', b'g', b'e',
            b't', 0x01, 0x02, 0x03, 0x04, 0x00, 0x00, 0x00, 0x0e, 0x50, 0x01, 0x01, 0x02, 0x03,
            0x04, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
        ];

        assert_eq!(evidence.entry_ordinal(), 2);
        assert_eq!(evidence.semantic_bytes().expect("charge"), 51);
        assert_eq!(evidence.evidence_order_key(), expected);
        assert_eq!(
            historical_order_key(&HistoricalSemanticEvidence::CapabilityPartition(evidence)),
            expected
        );
    }
}
