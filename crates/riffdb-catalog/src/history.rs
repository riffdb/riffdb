//! Same-session exact-end historical catalog validation.

use std::fmt;

use riffdb_contract_ir::{CompatibilityClass, IndexSchema};
use riffdb_storage_api::{
    EvidencePageLimit, HistoricalBundleEvidence, HistoricalEvidenceCursor, HistoricalEvidenceEnd,
    HistoricalEvidencePage, HistoricalPersistedKeyEvidenceV1, HistoricalSemanticEvidence,
    IrOpaquePersistedKeyV1, OpenSessionId, StructuralEvidenceSession,
};
use riffdb_types::{DatabaseId, IndexId};

use crate::{
    CatalogError, CatalogErrorKind, ValidatedContractBundle, validate_successor_compatibility,
};

/// Process-local proof that every catalog history item was IR-validated to exact end.
///
/// Its constructor is private, it is not serializable, and it never crosses a
/// storage trait. WP-130 may only combine it with the matching structural handoff.
pub struct ValidatedCatalogHistory {
    database_id: DatabaseId,
    open_session_id: OpenSessionId,
    active: Option<ValidatedContractBundle>,
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
            .field("evidence_count", &self.evidence_count)
            .finish()
    }
}

/// Catalog proof paired with storage's unforgeable historical exact-end token.
pub struct CatalogHistoryValidation<E> {
    history: ValidatedCatalogHistory,
    historical_end: E,
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
    let mut active_seen = false;
    let mut active = None;
    let mut terminal_bundle = None;
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
                    validate_historical_item(
                        session,
                        item,
                        &mut active_seen,
                        &mut active,
                        &mut terminal_bundle,
                    )?;
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

    if !active_seen || !active_matches_terminal(active.as_ref(), terminal_bundle.as_ref()) {
        return Err(CatalogError::new(
            CatalogErrorKind::InvalidHistoricalEvidence,
        ));
    }

    Ok(CatalogHistoryValidation {
        history: ValidatedCatalogHistory {
            database_id,
            open_session_id,
            active,
            evidence_count,
        },
        historical_end,
    })
}

fn validate_historical_item<S: StructuralEvidenceSession>(
    session: &mut S,
    item: &HistoricalSemanticEvidence,
    active_seen: &mut bool,
    active: &mut Option<ValidatedContractBundle>,
    terminal_bundle: &mut Option<ValidatedContractBundle>,
) -> Result<(), CatalogError> {
    match item {
        HistoricalSemanticEvidence::Bundle(evidence) => {
            let bundle = validate_bundle_evidence(evidence)?;
            validate_historical_bundle_parent(&bundle, terminal_bundle.as_ref())?;
            *terminal_bundle = Some(bundle);
        }
        HistoricalSemanticEvidence::PlanReference(reference) => {
            load_historical_bundle(
                session,
                reference.contract_lineage(),
                reference.contract_version(),
                reference.contract_bundle_hash(),
            )?
            .resolve_plan(reference)?;
        }
        HistoricalSemanticEvidence::ActiveCatalog(observed) => {
            if *active_seen {
                return Err(CatalogError::new(
                    CatalogErrorKind::InvalidHistoricalEvidence,
                ));
            }
            *active_seen = true;
            *active = match observed {
                Some(pointer) => Some(load_historical_bundle(
                    session,
                    pointer.lineage(),
                    pointer.version(),
                    pointer.bundle_hash(),
                )?),
                None => None,
            };
        }
        HistoricalSemanticEvidence::PersistedKey(evidence) => {
            validate_persisted_key(session, evidence)?;
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
    validate_successor_compatibility(candidate, prior)
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
    evidence: &HistoricalPersistedKeyEvidenceV1,
) -> Result<(), CatalogError> {
    let binding = evidence.schema();
    let bundle = load_historical_bundle(
        session,
        binding.lineage(),
        binding.contract_version(),
        binding.bundle_hash(),
    )?;

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
    }
    key
}

fn push_lineage(output: &mut Vec<u8>, lineage: &riffdb_types::ContractLineage) {
    let length = u32::try_from(lineage.as_bytes().len()).unwrap_or(u32::MAX);
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(lineage.as_bytes());
}
