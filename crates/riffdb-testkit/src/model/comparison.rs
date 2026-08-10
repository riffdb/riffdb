//! Comparison support for [`AuthoritativeCommandModel`]: a model↔model diff
//! and a model↔store verification through the durable-inspection surface,
//! each naming the diverging family and key.
//!
//! # Normalizations (model-extension obligation, ADR-0113 Phase 1 item 4)
//!
//! The store is never compared through raw rows; every observation flows
//! through the store's own validated readers, and where a reader's surface is
//! representationally narrower than the model's map the COMPARISON normalizes
//! to the reader's shape — the model itself is never weakened:
//!
//! - **Admissions:** redb persists admissions as fused pending/terminal
//!   `IdempotencyRecordV1` rows, possibly locator-backed into command
//!   capsules. `lookup_admission` decodes them back to
//!   [`StoredAdmissionStateV1`] — the model's exact comparand — so no
//!   comparison-side normalization is needed beyond trusting that decode.
//! - **Index entries:** compared as COMPLETE [`StoredIndexEntryV2`] records —
//!   key, schema binding, covered values, and partition key — through
//!   `scan_index_filtered`, whose rows retain their exact stored binding and
//!   partition. Nothing is projected away: a row persisted under a
//!   wrong-but-catalog-valid binding (stale contract version, wrong retained
//!   bundle) is a `ValueDiffers` divergence, and a row under a DIFFERENT
//!   lineage than the declared selection is filtered out by the store's own
//!   reader and surfaces as `MissingInStore`. (The binding is
//!   catalog-membership-validated at startup, not by the reader's decode —
//!   the scan codec is a plain bounds/proto codec.)
//! - **Index epochs:** the scan surface exposes the range's
//!   [`IndexEpochPosition`] observed atomically with the rows, not the whole
//!   `StoredIndexEpochV1` record — no public reader returns that record at
//!   all, and its dropped schema binding is independently validated at
//!   startup (`HistoricalPersistedKeyEvidenceV1::from_index_epoch`). The
//!   model record is normalized to a position exactly the way the model's
//!   own prior-state checks normalize it (`BeforeFirst` when absent,
//!   `Value(epoch)` when present).
//! - **Outcomes:** the store's only public outcome read is identity-keyed
//!   (`read_stored_outcome`, which resolves the capsule/locator indirection);
//!   the model's sequence-keyed map is compared against the sequence-keyed
//!   view the inspection assembles from those identity reads.
//! - **Commits:** compared through the validated commit scan's decoded
//!   records — never through raw table row counts, which are INVALID on redb
//!   because the audited path stores segments with locator indirection.
//! - **Outbox intents:** the only public read exposing the stored intent
//!   record is `scan_pending_outbox`, whose population is intents still
//!   effectively pending. The comparison is exact in both directions, and
//!   the pending-population premise is ENFORCED rather than assumed: every
//!   inspected event's reciprocal outbox status must still be pending, or
//!   the comparison reports [`StoreDivergenceCause::NonPendingOutboxObserved`]
//!   instead of comparing a population it cannot see completely.
//!
//! Every check runs in BOTH directions, and every model entry must be covered
//! by a declared inspection target ([`StoreDivergenceCause::UncoveredModelEntry`]
//! otherwise) — an under-declared harness fails loudly instead of comparing a
//! vacuously small surface. The converse bound is a genuine reader-surface
//! limit worth stating: `UnexpectedInStore` can fire only WITHIN the declared
//! target set (there is no scan-all-entities or scan-all-admissions surface),
//! so an engine-invented record at an undeclared target is structurally
//! invisible — mitigated by deriving targets from every fixture a harness
//! touches, including interrupted ones.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use riffdb_storage_api::{
    ApplicationSequenceAllocator, EntityTarget, IdempotencyIdentityKey, IndexEpochPosition,
    OutboxStatusReadResultV1, PartitionIndexTarget, StoredEntityRecordV1, StoredIndexEntryV2,
};
use riffdb_types::{FrontierPosition, IndexEntryKey};

use crate::inspection::{DurableInspection, IndexRangeSelection};

use super::authoritative::AuthoritativeCommandModel;

/// The modeled family in which a divergence was found.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ModelFamily {
    /// The application sequence allocator (the model's frontier image).
    ApplicationSequence,
    /// Admissions keyed by canonical idempotency identity key.
    Admissions,
    /// Entity post-images keyed by entity target.
    Entities,
    /// Secondary-index entries keyed by index entry key.
    IndexEntries,
    /// Index epochs keyed by partition/index generation target.
    IndexEpochs,
    /// Terminal outcomes keyed by commit sequence.
    Outcomes,
    /// Commit records keyed by commit sequence.
    Commits,
    /// Provenance records keyed by provenance identity.
    Provenance,
    /// Durable events keyed by event identity.
    Events,
    /// Authoritative outbox intents keyed by event identity.
    OutboxIntents,
}

impl fmt::Display for ModelFamily {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ApplicationSequence => "application_sequence",
            Self::Admissions => "admissions",
            Self::Entities => "entities",
            Self::IndexEntries => "index_entries",
            Self::IndexEpochs => "index_epochs",
            Self::Outcomes => "outcomes",
            Self::Commits => "commits",
            Self::Provenance => "provenance",
            Self::Events => "events",
            Self::OutboxIntents => "outbox_intents",
        })
    }
}

/// Which side of a model↔model comparison held the diverging entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ModelDivergenceCause {
    /// The key exists only in the left model.
    OnlyInLeft,
    /// The key exists only in the right model.
    OnlyInRight,
    /// Both models hold the key with different values.
    ValueDiffers,
}

/// First divergence between two models, naming the family and key.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ModelDivergence {
    family: ModelFamily,
    key: String,
    cause: ModelDivergenceCause,
}

impl ModelDivergence {
    /// Returns the diverging family.
    #[must_use]
    pub const fn family(&self) -> ModelFamily {
        self.family
    }

    /// Borrows the rendered diverging key.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns which side diverged.
    #[must_use]
    pub const fn cause(&self) -> ModelDivergenceCause {
        self.cause
    }
}

impl fmt::Display for ModelDivergence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "model divergence in {} at {}: {:?}",
            self.family, self.key, self.cause
        )
    }
}

/// Closed cause of one named model↔store disagreement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoreDivergenceCause {
    /// The model holds an entry the store does not.
    MissingInStore,
    /// The store holds an entry the model does not.
    UnexpectedInStore,
    /// Both sides hold the key with different values.
    ValueDiffers,
    /// A model entry is covered by no declared inspection target, so the
    /// comparison cannot see it — an under-declared harness, failed loudly.
    UncoveredModelEntry,
    /// The store surfaced one key twice through a canonical read.
    DuplicateStoreEntry,
    /// The store's recovered frontier does not imply the model's allocator.
    FrontierMismatch,
    /// An identity could not produce its canonical storage key.
    InvalidKey,
    /// An inspected event's reciprocal outbox observation was not canonical
    /// pending — a delivery transition ran, or the intent is missing — so
    /// the pending-population intent comparison cannot see the complete
    /// intent population.
    NonPendingOutboxObserved,
}

/// One named model↔store disagreement: family, rendered key, and cause.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoreDivergence {
    family: ModelFamily,
    key: String,
    cause: StoreDivergenceCause,
}

impl StoreDivergence {
    /// Returns the diverging family.
    #[must_use]
    pub const fn family(&self) -> ModelFamily {
        self.family
    }

    /// Borrows the rendered diverging key.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns the closed divergence cause.
    #[must_use]
    pub const fn cause(&self) -> StoreDivergenceCause {
        self.cause
    }
}

impl fmt::Display for StoreDivergence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "model/store divergence in {} at {}: {:?}",
            self.family, self.key, self.cause
        )
    }
}

impl Error for StoreDivergence {}

/// Per-family counts of records compared equal by
/// [`verify_model_against_inspection`].
///
/// The counts exist so a harness can assert its comparison was non-trivial —
/// an oracle that compared zero records proves nothing.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StoreAgreement {
    entities: usize,
    index_entries: usize,
    index_epochs: usize,
    index_epochs_absent: usize,
    outcomes: usize,
    admissions: usize,
    commits: usize,
    provenance: usize,
    events: usize,
    outbox_intents: usize,
}

impl StoreAgreement {
    /// Present entity records compared equal.
    #[must_use]
    pub const fn entities(&self) -> usize {
        self.entities
    }

    /// Index rows compared equal (one count per covering range comparison).
    #[must_use]
    pub const fn index_entries(&self) -> usize {
        self.index_entries
    }

    /// Range epoch observations compared equal on a PRESENT epoch value.
    ///
    /// A `BeforeFirst == BeforeFirst` agreement proves nothing about epoch
    /// state, so it is tracked separately in [`Self::index_epochs_absent`] —
    /// this counter cannot be inflated by declared ranges with no epoch.
    #[must_use]
    pub const fn index_epochs(&self) -> usize {
        self.index_epochs
    }

    /// Range epoch observations that agreed on ABSENCE (`BeforeFirst`).
    #[must_use]
    pub const fn index_epochs_absent(&self) -> usize {
        self.index_epochs_absent
    }

    /// Terminal outcomes compared equal.
    #[must_use]
    pub const fn outcomes(&self) -> usize {
        self.outcomes
    }

    /// Present admission states compared equal.
    #[must_use]
    pub const fn admissions(&self) -> usize {
        self.admissions
    }

    /// Commit records compared equal.
    #[must_use]
    pub const fn commits(&self) -> usize {
        self.commits
    }

    /// Provenance records compared equal.
    #[must_use]
    pub const fn provenance(&self) -> usize {
        self.provenance
    }

    /// Durable events compared equal.
    #[must_use]
    pub const fn events(&self) -> usize {
        self.events
    }

    /// Outbox intent records compared equal.
    #[must_use]
    pub const fn outbox_intents(&self) -> usize {
        self.outbox_intents
    }
}

impl AuthoritativeCommandModel {
    /// Reports the first divergence from another model, or `None` when equal.
    ///
    /// Families are walked in a fixed order (application sequence, then each
    /// map in declaration order) and each map in canonical key order, so a
    /// failing campaign names one deterministic map and key instead of a
    /// bare boolean.
    #[must_use]
    pub fn first_divergence_from(&self, other: &Self) -> Option<ModelDivergence> {
        if self.application_sequence() != other.application_sequence() {
            return Some(ModelDivergence {
                family: ModelFamily::ApplicationSequence,
                key: "allocator".to_owned(),
                cause: ModelDivergenceCause::ValueDiffers,
            });
        }
        first_map_divergence(
            ModelFamily::Admissions,
            self.admissions(),
            other.admissions(),
            render_identity_key,
        )
        .or_else(|| {
            first_map_divergence(
                ModelFamily::Entities,
                self.entities(),
                other.entities(),
                render_entity_target,
            )
        })
        .or_else(|| {
            first_map_divergence(
                ModelFamily::IndexEntries,
                self.index_entries(),
                other.index_entries(),
                render_index_entry_key,
            )
        })
        .or_else(|| {
            first_map_divergence(
                ModelFamily::IndexEpochs,
                self.index_epochs(),
                other.index_epochs(),
                render_partition_index_target,
            )
        })
        .or_else(|| {
            first_map_divergence(
                ModelFamily::Outcomes,
                self.outcomes(),
                other.outcomes(),
                render_debug,
            )
        })
        .or_else(|| {
            first_map_divergence(
                ModelFamily::Commits,
                self.commits(),
                other.commits(),
                render_debug,
            )
        })
        .or_else(|| {
            first_map_divergence(
                ModelFamily::Provenance,
                self.provenance_records(),
                other.provenance_records(),
                render_debug,
            )
        })
        .or_else(|| {
            first_map_divergence(
                ModelFamily::Events,
                self.events(),
                other.events(),
                render_debug,
            )
        })
        .or_else(|| {
            first_map_divergence(
                ModelFamily::OutboxIntents,
                self.outbox_intents(),
                other.outbox_intents(),
                render_debug,
            )
        })
    }
}

/// Verifies one recovered store, observed through [`DurableInspection`],
/// against the model, reporting the first named disagreement.
///
/// Every family is checked in both directions and every model entry must be
/// covered by a declared inspection target; see the module comment for the
/// normalizations applied. On success the returned [`StoreAgreement`] carries
/// per-family counts of records actually compared equal.
pub fn verify_model_against_inspection(
    model: &AuthoritativeCommandModel,
    inspection: &DurableInspection,
) -> Result<StoreAgreement, StoreDivergence> {
    let mut agreement = StoreAgreement::default();

    // Application sequence: the recovered frontier must imply the model's
    // allocator exactly — `Next(n)` means "n is the NEXT sequence to assign",
    // so BeforeFirst ⇒ Next(1) and AppliedThrough(n) ⇒ Next(n + 1)
    // (Exhausted at the representable end).
    let store_allocator = match inspection.recovered_application_frontier() {
        FrontierPosition::BeforeFirst => ApplicationSequenceAllocator::initial(),
        FrontierPosition::AppliedThrough(sequence) => sequence.checked_next().map_or(
            ApplicationSequenceAllocator::Exhausted,
            ApplicationSequenceAllocator::next,
        ),
    };
    if model.application_sequence() != store_allocator {
        return Err(StoreDivergence {
            family: ModelFamily::ApplicationSequence,
            key: render_debug(&inspection.recovered_application_frontier()),
            cause: StoreDivergenceCause::FrontierMismatch,
        });
    }

    verify_admissions(model, inspection, &mut agreement)?;
    verify_outcomes(model, inspection, &mut agreement)?;
    verify_commits(model, inspection, &mut agreement)?;
    verify_entities(model, inspection, &mut agreement)?;
    verify_index_entries(model, inspection, &mut agreement)?;
    verify_index_epochs(model, inspection, &mut agreement)?;
    verify_provenance(model, inspection, &mut agreement)?;
    verify_events(model, inspection, &mut agreement)?;
    verify_outbox_intents(model, inspection, &mut agreement)?;
    Ok(agreement)
}

fn divergence(family: ModelFamily, key: String, cause: StoreDivergenceCause) -> StoreDivergence {
    StoreDivergence { family, key, cause }
}

fn verify_admissions(
    model: &AuthoritativeCommandModel,
    inspection: &DurableInspection,
    agreement: &mut StoreAgreement,
) -> Result<(), StoreDivergence> {
    let mut inspected = BTreeMap::new();
    for admission in inspection.admissions() {
        let key = admission.identity().storage_key().map_err(|_| {
            divergence(
                ModelFamily::Admissions,
                "<underivable>".to_owned(),
                StoreDivergenceCause::InvalidKey,
            )
        })?;
        if inspected.insert(key.clone(), admission).is_some() {
            return Err(divergence(
                ModelFamily::Admissions,
                render_identity_key(&key),
                StoreDivergenceCause::DuplicateStoreEntry,
            ));
        }
    }
    for (key, state) in model.admissions() {
        match inspected.remove(key) {
            None => {
                return Err(divergence(
                    ModelFamily::Admissions,
                    render_identity_key(key),
                    StoreDivergenceCause::UncoveredModelEntry,
                ));
            }
            Some(admission) => match admission.state() {
                None => {
                    return Err(divergence(
                        ModelFamily::Admissions,
                        render_identity_key(key),
                        StoreDivergenceCause::MissingInStore,
                    ));
                }
                Some(observed) if observed == state => agreement.admissions += 1,
                Some(_) => {
                    return Err(divergence(
                        ModelFamily::Admissions,
                        render_identity_key(key),
                        StoreDivergenceCause::ValueDiffers,
                    ));
                }
            },
        }
    }
    // Remaining requested identities must be absent in the store too.
    for (key, admission) in inspected {
        if admission.state().is_some() {
            return Err(divergence(
                ModelFamily::Admissions,
                render_identity_key(&key),
                StoreDivergenceCause::UnexpectedInStore,
            ));
        }
    }
    Ok(())
}

fn verify_outcomes(
    model: &AuthoritativeCommandModel,
    inspection: &DurableInspection,
    agreement: &mut StoreAgreement,
) -> Result<(), StoreDivergence> {
    let mut inspected = BTreeMap::new();
    for outcome in inspection.outcomes() {
        if inspected
            .insert(outcome.commit_sequence(), outcome)
            .is_some()
        {
            return Err(divergence(
                ModelFamily::Outcomes,
                render_debug(&outcome.commit_sequence()),
                StoreDivergenceCause::DuplicateStoreEntry,
            ));
        }
    }
    for (sequence, outcome) in model.outcomes() {
        // Coverage: every model outcome's identity is also a model admission
        // key, and verify_admissions already proved every model admission was
        // declared — so absence here is genuine store-side absence.
        match inspected.remove(sequence) {
            None => {
                return Err(divergence(
                    ModelFamily::Outcomes,
                    render_debug(sequence),
                    StoreDivergenceCause::MissingInStore,
                ));
            }
            Some(observed) if observed == outcome => agreement.outcomes += 1,
            Some(_) => {
                return Err(divergence(
                    ModelFamily::Outcomes,
                    render_debug(sequence),
                    StoreDivergenceCause::ValueDiffers,
                ));
            }
        }
    }
    if let Some((sequence, _)) = inspected.into_iter().next() {
        return Err(divergence(
            ModelFamily::Outcomes,
            render_debug(&sequence),
            StoreDivergenceCause::UnexpectedInStore,
        ));
    }
    Ok(())
}

fn verify_commits(
    model: &AuthoritativeCommandModel,
    inspection: &DurableInspection,
    agreement: &mut StoreAgreement,
) -> Result<(), StoreDivergence> {
    let mut inspected = BTreeMap::new();
    for record in inspection.commits() {
        if inspected.insert(record.commit_sequence(), record).is_some() {
            return Err(divergence(
                ModelFamily::Commits,
                render_debug(&record.commit_sequence()),
                StoreDivergenceCause::DuplicateStoreEntry,
            ));
        }
    }
    for (sequence, record) in model.commits() {
        match inspected.remove(sequence) {
            None => {
                return Err(divergence(
                    ModelFamily::Commits,
                    render_debug(sequence),
                    StoreDivergenceCause::MissingInStore,
                ));
            }
            Some(observed) if observed == record => agreement.commits += 1,
            Some(_) => {
                return Err(divergence(
                    ModelFamily::Commits,
                    render_debug(sequence),
                    StoreDivergenceCause::ValueDiffers,
                ));
            }
        }
    }
    if let Some((sequence, _)) = inspected.into_iter().next() {
        return Err(divergence(
            ModelFamily::Commits,
            render_debug(&sequence),
            StoreDivergenceCause::UnexpectedInStore,
        ));
    }
    Ok(())
}

fn verify_entities(
    model: &AuthoritativeCommandModel,
    inspection: &DurableInspection,
    agreement: &mut StoreAgreement,
) -> Result<(), StoreDivergence> {
    let mut inspected: BTreeMap<&EntityTarget, Option<&StoredEntityRecordV1>> = BTreeMap::new();
    for entity in inspection.entities() {
        if inspected.insert(entity.target(), entity.record()).is_some() {
            return Err(divergence(
                ModelFamily::Entities,
                render_entity_target(entity.target()),
                StoreDivergenceCause::DuplicateStoreEntry,
            ));
        }
    }
    for (target, record) in model.entities() {
        match inspected.remove(target) {
            None => {
                return Err(divergence(
                    ModelFamily::Entities,
                    render_entity_target(target),
                    StoreDivergenceCause::UncoveredModelEntry,
                ));
            }
            Some(None) => {
                return Err(divergence(
                    ModelFamily::Entities,
                    render_entity_target(target),
                    StoreDivergenceCause::MissingInStore,
                ));
            }
            Some(Some(observed)) if observed == record => agreement.entities += 1,
            Some(Some(_)) => {
                return Err(divergence(
                    ModelFamily::Entities,
                    render_entity_target(target),
                    StoreDivergenceCause::ValueDiffers,
                ));
            }
        }
    }
    for (target, record) in inspected {
        if record.is_some() {
            return Err(divergence(
                ModelFamily::Entities,
                render_entity_target(target),
                StoreDivergenceCause::UnexpectedInStore,
            ));
        }
    }
    Ok(())
}

/// Whether one declared range selection covers one modeled index row: same
/// index, key under the range's byte prefix, the row's partition equal to the
/// range's generation partition, and the row's binding under the declared
/// lineage — the same membership tests the store's own filtered scan applies,
/// so a model row this returns `false` for could never be returned by the
/// scan and must instead fail loudly as uncovered.
fn range_covers(
    selection: &IndexRangeSelection,
    key: &IndexEntryKey,
    row: &StoredIndexEntryV2,
) -> bool {
    let target = selection.target();
    key.index_id() == target.prefix().index_id()
        && key.as_bytes().starts_with(target.prefix().as_bytes())
        && row.partition_key() == target.generation_target().partition_key()
        && row.schema_binding().lineage() == selection.lineage()
}

fn verify_index_entries(
    model: &AuthoritativeCommandModel,
    inspection: &DurableInspection,
    agreement: &mut StoreAgreement,
) -> Result<(), StoreDivergence> {
    // Coverage first: a model row no declared range can see fails loudly.
    for (key, row) in model.index_entries() {
        if !inspection
            .index_ranges()
            .iter()
            .any(|range| range_covers(range.selection(), key, row))
        {
            return Err(divergence(
                ModelFamily::IndexEntries,
                render_index_entry_key(key),
                StoreDivergenceCause::UncoveredModelEntry,
            ));
        }
    }
    for range in inspection.index_ranges() {
        let mut observed_rows = BTreeMap::new();
        for entry in range.entries() {
            if observed_rows.insert(entry.key(), entry).is_some() {
                return Err(divergence(
                    ModelFamily::IndexEntries,
                    render_index_entry_key(entry.key()),
                    StoreDivergenceCause::DuplicateStoreEntry,
                ));
            }
        }
        for (key, row) in model.index_entries() {
            if !range_covers(range.selection(), key, row) {
                continue;
            }
            match observed_rows.remove(key) {
                None => {
                    return Err(divergence(
                        ModelFamily::IndexEntries,
                        render_index_entry_key(key),
                        StoreDivergenceCause::MissingInStore,
                    ));
                }
                // The COMPLETE stored row — key, schema binding, covered
                // values, partition key — must be field-exact; a
                // wrong-but-catalog-valid binding diverges here.
                Some(observed) if observed == row => {
                    agreement.index_entries += 1;
                }
                Some(_) => {
                    return Err(divergence(
                        ModelFamily::IndexEntries,
                        render_index_entry_key(key),
                        StoreDivergenceCause::ValueDiffers,
                    ));
                }
            }
        }
        if let Some((key, _)) = observed_rows.into_iter().next() {
            return Err(divergence(
                ModelFamily::IndexEntries,
                render_index_entry_key(key),
                StoreDivergenceCause::UnexpectedInStore,
            ));
        }
    }
    Ok(())
}

fn verify_index_epochs(
    model: &AuthoritativeCommandModel,
    inspection: &DurableInspection,
    agreement: &mut StoreAgreement,
) -> Result<(), StoreDivergence> {
    for (target, _) in model.index_epochs() {
        if !inspection
            .index_ranges()
            .iter()
            .any(|range| range.selection().target().generation_target() == target)
        {
            return Err(divergence(
                ModelFamily::IndexEpochs,
                render_partition_index_target(target),
                StoreDivergenceCause::UncoveredModelEntry,
            ));
        }
    }
    for range in inspection.index_ranges() {
        let target = range.selection().target().generation_target();
        // The scan exposes the range's epoch POSITION; normalize the model
        // record exactly as the model's own prior-state checks do.
        let modeled = model
            .index_epoch(target)
            .map_or(IndexEpochPosition::BeforeFirst, |record| {
                IndexEpochPosition::Value(record.epoch())
            });
        if modeled == range.epoch() {
            // BeforeFirst == BeforeFirst proves nothing about epoch state;
            // count it separately so the value-agreement counter cannot be
            // inflated by declared ranges with no epoch at all.
            if modeled == IndexEpochPosition::BeforeFirst {
                agreement.index_epochs_absent += 1;
            } else {
                agreement.index_epochs += 1;
            }
        } else {
            return Err(divergence(
                ModelFamily::IndexEpochs,
                render_partition_index_target(target),
                StoreDivergenceCause::ValueDiffers,
            ));
        }
    }
    Ok(())
}

fn verify_provenance(
    model: &AuthoritativeCommandModel,
    inspection: &DurableInspection,
    agreement: &mut StoreAgreement,
) -> Result<(), StoreDivergence> {
    let mut inspected = BTreeMap::new();
    for record in inspection.provenance() {
        if inspected.insert(record.provenance_id(), record).is_some() {
            return Err(divergence(
                ModelFamily::Provenance,
                render_debug(&record.provenance_id()),
                StoreDivergenceCause::DuplicateStoreEntry,
            ));
        }
    }
    for (id, record) in model.provenance_records() {
        match inspected.remove(id) {
            None => {
                return Err(divergence(
                    ModelFamily::Provenance,
                    render_debug(id),
                    StoreDivergenceCause::MissingInStore,
                ));
            }
            Some(observed) if observed == record => agreement.provenance += 1,
            Some(_) => {
                return Err(divergence(
                    ModelFamily::Provenance,
                    render_debug(id),
                    StoreDivergenceCause::ValueDiffers,
                ));
            }
        }
    }
    if let Some((id, _)) = inspected.into_iter().next() {
        return Err(divergence(
            ModelFamily::Provenance,
            render_debug(&id),
            StoreDivergenceCause::UnexpectedInStore,
        ));
    }
    Ok(())
}

fn verify_events(
    model: &AuthoritativeCommandModel,
    inspection: &DurableInspection,
    agreement: &mut StoreAgreement,
) -> Result<(), StoreDivergence> {
    let mut inspected = BTreeMap::new();
    for event in inspection.events() {
        // Enforce the outbox comparison's premise instead of assuming it:
        // every event's reciprocal delivery state must still be pending, or
        // the pending-population intent scan is not the complete population.
        let pending = match event.outbox() {
            OutboxStatusReadResultV1::Status(observation) => observation.is_pending(),
            OutboxStatusReadResultV1::AuthoritativeIntentMissing => false,
        };
        if !pending {
            return Err(divergence(
                ModelFamily::OutboxIntents,
                render_debug(&event.event().event_id()),
                StoreDivergenceCause::NonPendingOutboxObserved,
            ));
        }
        if inspected
            .insert(event.event().event_id(), event.event())
            .is_some()
        {
            return Err(divergence(
                ModelFamily::Events,
                render_debug(&event.event().event_id()),
                StoreDivergenceCause::DuplicateStoreEntry,
            ));
        }
    }
    for (id, event) in model.events() {
        match inspected.remove(id) {
            None => {
                return Err(divergence(
                    ModelFamily::Events,
                    render_debug(id),
                    StoreDivergenceCause::MissingInStore,
                ));
            }
            Some(observed) if observed == event => agreement.events += 1,
            Some(_) => {
                return Err(divergence(
                    ModelFamily::Events,
                    render_debug(id),
                    StoreDivergenceCause::ValueDiffers,
                ));
            }
        }
    }
    if let Some((id, _)) = inspected.into_iter().next() {
        return Err(divergence(
            ModelFamily::Events,
            render_debug(&id),
            StoreDivergenceCause::UnexpectedInStore,
        ));
    }
    Ok(())
}

fn verify_outbox_intents(
    model: &AuthoritativeCommandModel,
    inspection: &DurableInspection,
    agreement: &mut StoreAgreement,
) -> Result<(), StoreDivergence> {
    let mut inspected = BTreeMap::new();
    for intent in inspection.outbox_intents() {
        if inspected.insert(intent.event_id(), intent).is_some() {
            return Err(divergence(
                ModelFamily::OutboxIntents,
                render_debug(&intent.event_id()),
                StoreDivergenceCause::DuplicateStoreEntry,
            ));
        }
    }
    for (id, intent) in model.outbox_intents() {
        match inspected.remove(id) {
            None => {
                return Err(divergence(
                    ModelFamily::OutboxIntents,
                    render_debug(id),
                    StoreDivergenceCause::MissingInStore,
                ));
            }
            Some(observed) if observed == intent => agreement.outbox_intents += 1,
            Some(_) => {
                return Err(divergence(
                    ModelFamily::OutboxIntents,
                    render_debug(id),
                    StoreDivergenceCause::ValueDiffers,
                ));
            }
        }
    }
    if let Some((id, _)) = inspected.into_iter().next() {
        return Err(divergence(
            ModelFamily::OutboxIntents,
            render_debug(&id),
            StoreDivergenceCause::UnexpectedInStore,
        ));
    }
    Ok(())
}

fn first_map_divergence<'a, K, V>(
    family: ModelFamily,
    left: impl Iterator<Item = (&'a K, &'a V)>,
    right: impl Iterator<Item = (&'a K, &'a V)>,
    render: impl Fn(&K) -> String,
) -> Option<ModelDivergence>
where
    K: Ord + 'a,
    V: PartialEq + 'a,
{
    let mut right: BTreeMap<&K, &V> = right.collect();
    for (key, value) in left {
        match right.remove(key) {
            None => {
                return Some(ModelDivergence {
                    family,
                    key: render(key),
                    cause: ModelDivergenceCause::OnlyInLeft,
                });
            }
            Some(other) if other == value => {}
            Some(_) => {
                return Some(ModelDivergence {
                    family,
                    key: render(key),
                    cause: ModelDivergenceCause::ValueDiffers,
                });
            }
        }
    }
    right.into_iter().next().map(|(key, _)| ModelDivergence {
        family,
        key: render(key),
        cause: ModelDivergenceCause::OnlyInRight,
    })
}

fn hex(bytes: &[u8]) -> String {
    let mut rendered = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        rendered.push_str(&format!("{byte:02x}"));
    }
    rendered
}

fn render_identity_key(key: &IdempotencyIdentityKey) -> String {
    hex(key.as_bytes())
}

fn render_entity_target(target: &EntityTarget) -> String {
    format!(
        "{:?}/{}",
        target.entity_type_id(),
        hex(target.key().as_bytes())
    )
}

fn render_index_entry_key(key: &IndexEntryKey) -> String {
    format!("{:?}/{}", key.index_id(), hex(key.as_bytes()))
}

fn render_partition_index_target(target: &PartitionIndexTarget) -> String {
    format!(
        "{:?}/{}",
        target.index_id(),
        hex(target.partition_key().as_bytes())
    )
}

fn render_debug<T: fmt::Debug>(value: &T) -> String {
    format!("{value:?}")
}

#[cfg(test)]
mod tests {
    use riffdb_storage_api::{
        AffectedEntityV1, AffectedEpochCurrentState, AffectedIndexEpochTargets,
        AssignedCommandSequence, AtomicCommandRecordSet, CommandWriteSetPlanV1,
        CurrentIndexGenerationObservation, DeclaredOutcome, DurabilityMode,
        DurableKeySchemaBindingV1, EncodedWriteSetUpperBoundResultV1, EntityMutation,
        EntityObservation, EntityPostImage, EvaluationBudget, EventIntent, ExecutablePlanRef,
        ExpectedEntityState, IdempotencyIdentity, IdempotencyKeyDigest,
        IdempotencyLookupCandidatesV1, IndexEntryMutationV1, IndexEpochAdvanceV1,
        IndexRangePrefixBuilder, IndexRangeTarget, OutboxStatusObservationV1,
        PreEvaluationCommitContext, ReadSnapshot, RetainedMetadataV1, SnapshotRequest,
        StoredAdmittedProvenanceClaimsV1, StoredCommitRecordV1, StoredDurableEventV1,
        StoredOutboxIntentV1, StoredOutcomeV1, StoredPendingAdmissionV1, StoredProvenanceRecordV1,
        StoredReadDependenciesV1, command_write_set_upper_bound_v1, derive_event_hash_v1,
    };
    use riffdb_types::{
        ActorId, ActorKind, AdmittedActorContext, AggregateTypeId, CanonicalInputHash,
        CanonicalRecord, CanonicalValue, CommandId, CommitSequence, ContractBundleHash,
        ContractLineage, ContractVersion, DatabaseId, DigestKeyId, EntityKeyBuilder, EntityTypeId,
        EntityVersion, Environment, EventId, EventTypeId, FieldId, IndexEntryKeyBuilder, IndexId,
        LogicalTime, OutcomeId, PartitionKeyBuilder, PlanHash, ProvenanceId, RequestId, TenantId,
        TenantScope, Timestamp, hash_partition_key,
    };

    use crate::inspection::{
        InspectedAdmission, InspectedEntity, InspectedEvent, InspectedIndexRange,
    };

    use super::*;

    fn database_id() -> DatabaseId {
        DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10])
            .expect("database ID")
    }

    fn plan() -> ExecutablePlanRef {
        ExecutablePlanRef::new(
            ContractLineage::new("comparison-test").expect("lineage"),
            ContractVersion::new(1).expect("version"),
            ContractBundleHash::from_bytes([0x21; 32]),
            CommandId::new(1).expect("command"),
            PlanHash::from_bytes([0x22; 32]),
        )
    }

    fn identity(digest: u8) -> IdempotencyIdentity {
        IdempotencyIdentity::new(
            database_id(),
            Environment::new("test").expect("environment"),
            TenantScope::Tenant(TenantId::new("tenant-a").expect("tenant")),
            ActorId::new("principal-a").expect("principal"),
            ContractLineage::new("comparison-test").expect("lineage"),
            CommandId::new(1).expect("command"),
            IdempotencyKeyDigest::from_hmac_bytes(
                DigestKeyId::new(1).expect("digest key"),
                [digest; 32],
            ),
        )
    }

    fn pending(digest: u8, input_hash: u8) -> StoredPendingAdmissionV1 {
        let identity = identity(digest);
        let mut request_id = [0x30_u8; 16];
        request_id[6] = 0x70;
        request_id[8] = 0x80;
        let mut partition = PartitionKeyBuilder::new(AggregateTypeId::new(1).expect("aggregate"));
        partition.push_u64(7).expect("partition component");
        StoredPendingAdmissionV1::new(
            identity.clone(),
            CanonicalInputHash::from_bytes([input_hash; 32]),
            RequestId::from_bytes(request_id).expect("request ID"),
            plan(),
            LogicalTime::new(Timestamp::new(1_700_000_001, 0).expect("timestamp")),
            AdmittedActorContext::new(
                ActorId::new("principal-a").expect("principal"),
                ActorKind::Human,
                TenantScope::Tenant(TenantId::new("tenant-a").expect("tenant")),
                None,
            ),
            partition.finish().expect("partition key"),
            StoredAdmittedProvenanceClaimsV1::default(),
        )
        .expect("pending admission")
    }

    #[test]
    fn equal_models_report_no_divergence() {
        let mut left = AuthoritativeCommandModel::new();
        let mut right = AuthoritativeCommandModel::new();
        assert_eq!(left.first_divergence_from(&right), None);
        left.admit_pending(pending(0x01, 0x42)).expect("admit left");
        right
            .admit_pending(pending(0x01, 0x42))
            .expect("admit right");
        assert_eq!(left.first_divergence_from(&right), None);
    }

    #[test]
    fn first_divergence_names_the_admissions_map_and_key() {
        let mut left = AuthoritativeCommandModel::new();
        let right = AuthoritativeCommandModel::new();
        left.admit_pending(pending(0x01, 0x42)).expect("admit left");

        let divergence = left
            .first_divergence_from(&right)
            .expect("one-sided admission diverges");
        assert_eq!(divergence.family(), ModelFamily::Admissions);
        assert_eq!(divergence.cause(), ModelDivergenceCause::OnlyInLeft);
        assert_eq!(
            divergence.key(),
            render_identity_key(&identity(0x01).storage_key().expect("storage key")),
            "the divergence names the exact admission key"
        );
        let mirrored = right
            .first_divergence_from(&left)
            .expect("mirrored direction diverges");
        assert_eq!(mirrored.cause(), ModelDivergenceCause::OnlyInRight);
    }

    #[test]
    fn first_divergence_reports_a_differing_value_under_one_key() {
        let mut left = AuthoritativeCommandModel::new();
        let mut right = AuthoritativeCommandModel::new();
        left.admit_pending(pending(0x01, 0x42)).expect("admit left");
        // Same identity key, different canonical input hash.
        right
            .admit_pending(pending(0x01, 0x43))
            .expect("admit right");

        let divergence = left
            .first_divergence_from(&right)
            .expect("differing pending admissions diverge");
        assert_eq!(divergence.family(), ModelFamily::Admissions);
        assert_eq!(divergence.cause(), ModelDivergenceCause::ValueDiffers);
    }

    /// One complete applied command (sequence 1, entity 7, one index row, one
    /// epoch advance, one event) — the minimal model whose every family is
    /// non-empty, so each per-family test below diverges exactly one field.
    fn applied_model() -> (AuthoritativeCommandModel, IndexRangeSelection) {
        let plan = plan();
        let sequence = CommitSequence::new(1).expect("sequence");
        let record = CanonicalRecord::new(vec![(
            FieldId::new(1).expect("field"),
            CanonicalValue::U64(101),
        )])
        .expect("record");
        let entity_type = EntityTypeId::new(1).expect("entity type");
        let mut entity_key = EntityKeyBuilder::new(entity_type);
        entity_key.push_u64(7).expect("entity component");
        let entity_key = entity_key.finish().expect("entity key");
        let target = EntityTarget::new(entity_type, entity_key.clone()).expect("target");
        let index_id = IndexId::new(1).expect("index");
        let mut index_key = IndexEntryKeyBuilder::new(index_id);
        index_key.push_u64(10).expect("index component");
        let index_key = index_key.finish(entity_key).expect("index key");
        let mut prefix = IndexRangePrefixBuilder::new(index_id);
        prefix.push_u64(10).expect("prefix component");
        let mut partition = PartitionKeyBuilder::new(AggregateTypeId::new(1).expect("aggregate"));
        partition.push_u64(7).expect("partition component");
        let partition = partition.finish().expect("partition key");
        let selection = IndexRangeSelection::new(
            IndexRangeTarget::new(partition.clone(), prefix.finish()),
            plan.contract_lineage().clone(),
        );

        let pending = pending(0x01, 0x42);
        let identity = pending.identity().clone();
        let request_id = pending.admission_request_id();
        let mut provenance_bytes = [0x51_u8; 16];
        provenance_bytes[6] = 0x71;
        provenance_bytes[8] = 0x81;
        let provenance_id = ProvenanceId::from_bytes(provenance_bytes).expect("provenance ID");
        let logical_time = LogicalTime::new(Timestamp::new(1_700_000_001, 0).expect("timestamp"));
        let actor = AdmittedActorContext::new(
            ActorId::new("principal-a").expect("principal"),
            ActorKind::Human,
            TenantScope::Tenant(TenantId::new("tenant-a").expect("tenant")),
            None,
        );

        let snapshot_request =
            SnapshotRequest::new(plan.clone(), vec![target.clone()], Vec::new(), Vec::new())
                .expect("snapshot request");
        let snapshot = ReadSnapshot::new(
            &snapshot_request,
            None,
            vec![EntityObservation::Absent(target.clone())],
            Vec::new(),
            Vec::new(),
        )
        .expect("read snapshot");
        let post_image =
            EntityPostImage::new(target.clone(), plan.contract_version(), record.clone())
                .expect("post-image");
        let event_intent =
            EventIntent::new(EventTypeId::new(1).expect("event type"), record.clone())
                .expect("event intent");
        let declared_outcome =
            DeclaredOutcome::new(OutcomeId::new(1).expect("outcome"), record.clone())
                .expect("declared outcome");
        let evaluated = riffdb_storage_api::EvaluatedCommand::new(
            &snapshot,
            vec![EntityMutation::Create(post_image)],
            vec![event_intent],
            declared_outcome.clone(),
            EvaluationBudget::v1(),
        )
        .expect("evaluated");
        let partition_hash = hash_partition_key(partition.as_bytes());
        let context = PreEvaluationCommitContext::new(pending.clone(), partition_hash, Vec::new())
            .expect("context");
        let candidates =
            IdempotencyLookupCandidatesV1::new(vec![identity.clone()]).expect("candidates");
        let intent = riffdb_storage_api::CommitIntent::new_for_vacant_terminal_admission(
            context,
            candidates,
            evaluated,
            provenance_id,
        )
        .expect("intent");

        let stored_entity = StoredEntityRecordV1::new(
            target,
            EntityVersion::first(),
            plan.contract_version(),
            DurableKeySchemaBindingV1::from_plan(&plan),
            record.clone(),
        )
        .expect("stored entity");
        let mutation = riffdb_storage_api::CommittedEntityMutationV1::new(
            ExpectedEntityState::Absent,
            stored_entity,
        )
        .expect("mutation");
        let index_record = StoredIndexEntryV2::new(
            index_key,
            DurableKeySchemaBindingV1::from_plan(&plan),
            record.clone(),
            partition.clone(),
        )
        .expect("index row");
        let index_mutation = IndexEntryMutationV1::Put(index_record);
        let generation = PartitionIndexTarget::new(partition.clone(), index_id);
        let affected_targets =
            AffectedIndexEpochTargets::new(vec![generation.clone()]).expect("targets");
        let affected_current = AffectedEpochCurrentState::new(
            &affected_targets,
            vec![CurrentIndexGenerationObservation::new(
                generation.clone(),
                IndexEpochPosition::BeforeFirst,
            )],
        )
        .expect("current");
        let epoch_advance = IndexEpochAdvanceV1::new(
            generation,
            DurableKeySchemaBindingV1::from_plan(&plan),
            IndexEpochPosition::BeforeFirst,
        )
        .expect("advance");
        let upper_bound = match command_write_set_upper_bound_v1(
            &intent,
            std::slice::from_ref(&index_mutation),
            std::slice::from_ref(&epoch_advance),
        )
        .expect("upper bound")
        {
            EncodedWriteSetUpperBoundResultV1::Fits(bound) => bound,
            EncodedWriteSetUpperBoundResultV1::ExceedsAcceptedAggregateCap(_) => {
                panic!("comparison fixture write set must fit the accepted cap")
            }
        };
        let write_plan = CommandWriteSetPlanV1::new(
            &intent,
            affected_targets,
            affected_current,
            vec![index_mutation],
            vec![epoch_advance],
            upper_bound,
        )
        .expect("write plan");

        let assignment = AssignedCommandSequence::from_assigned(sequence);
        let event_id = EventId::new(sequence, 0);
        let event_type = EventTypeId::new(1).expect("event type");
        let event = StoredDurableEventV1::new(
            event_id,
            event_type,
            record.clone(),
            derive_event_hash_v1(event_id, event_type, &record).expect("event hash"),
        )
        .expect("event");
        let stored_outcome = StoredOutcomeV1::new(
            identity.clone(),
            sequence,
            request_id,
            plan.clone(),
            pending.canonical_input_hash(),
            actor.clone(),
            logical_time,
            partition.clone(),
            partition_hash,
            Vec::new(),
            declared_outcome.clone(),
            StoredAdmittedProvenanceClaimsV1::default(),
            provenance_id,
            DurabilityMode::Sync,
        )
        .expect("outcome");
        let mutations = vec![mutation];
        let provenance = StoredProvenanceRecordV1::new(
            provenance_id,
            sequence,
            identity,
            request_id,
            plan.clone(),
            pending.canonical_input_hash(),
            actor.clone(),
            logical_time,
            partition_hash,
            Vec::new(),
            declared_outcome.outcome_id(),
            vec![AffectedEntityV1::from_record(mutations[0].post_image())],
            vec![event_id],
            StoredAdmittedProvenanceClaimsV1::default(),
        )
        .expect("provenance");
        let commit = StoredCommitRecordV1::new(
            sequence,
            request_id,
            plan,
            pending.canonical_input_hash(),
            actor,
            logical_time,
            partition_hash,
            Vec::new(),
            StoredReadDependenciesV1::from_live(snapshot.read_dependencies()).expect("deps"),
            mutations
                .iter()
                .map(riffdb_storage_api::CommittedEntityReferenceV2::from_mutation)
                .collect::<Result<Vec<_>, _>>()
                .expect("references"),
            vec![event.clone()],
            declared_outcome,
            provenance_id,
            vec![event_id],
            DurabilityMode::Sync,
        )
        .expect("commit");
        let records = AtomicCommandRecordSet::new(
            assignment,
            mutations,
            write_plan,
            stored_outcome,
            provenance,
            commit,
        )
        .expect("record set");

        let mut model = AuthoritativeCommandModel::new();
        model.admit_pending(pending).expect("admit");
        model.apply_command(&records).expect("apply");
        (model, selection)
    }

    /// The faithful synthetic mirror of one model: every family reproduced
    /// exactly, so each test below diverges ONE field and asserts the exact
    /// named red through `verify_model_against_inspection` itself.
    struct SyntheticParts {
        frontier: FrontierPosition,
        commits: Vec<StoredCommitRecordV1>,
        entities: Vec<InspectedEntity>,
        provenance: Vec<StoredProvenanceRecordV1>,
        events: Vec<InspectedEvent>,
        index_ranges: Vec<InspectedIndexRange>,
        admissions: Vec<InspectedAdmission>,
        outcomes: Vec<StoredOutcomeV1>,
        outbox_intents: Vec<StoredOutboxIntentV1>,
    }

    impl SyntheticParts {
        fn faithful(model: &AuthoritativeCommandModel, selection: &IndexRangeSelection) -> Self {
            let epoch = model
                .index_epoch(selection.target().generation_target())
                .map_or(IndexEpochPosition::BeforeFirst, |record| {
                    IndexEpochPosition::Value(record.epoch())
                });
            let rows = model
                .index_entries()
                .filter(|(key, row)| range_covers(selection, key, row))
                .map(|(_, row)| row.clone())
                .collect();
            let admissions = model
                .admissions()
                .map(|(key, state)| {
                    let identity = IdempotencyIdentityKey::decode(key.as_bytes())
                        .expect("decodable identity key");
                    InspectedAdmission::synthetic(identity, Some(state.clone()))
                })
                .collect();
            Self {
                frontier: model
                    .commits()
                    .last()
                    .map_or(FrontierPosition::BeforeFirst, |(sequence, _)| {
                        FrontierPosition::AppliedThrough(*sequence)
                    }),
                commits: model.commits().map(|(_, record)| record.clone()).collect(),
                entities: model
                    .entities()
                    .map(|(target, record)| {
                        InspectedEntity::synthetic(target.clone(), Some(record.clone()))
                    })
                    .collect(),
                provenance: model
                    .provenance_records()
                    .map(|(_, record)| record.clone())
                    .collect(),
                events: model
                    .events()
                    .map(|(_, event)| {
                        InspectedEvent::synthetic(
                            event.clone(),
                            OutboxStatusReadResultV1::Status(
                                OutboxStatusObservationV1::AbsentInitialPending,
                            ),
                        )
                    })
                    .collect(),
                index_ranges: vec![InspectedIndexRange::synthetic(
                    selection.clone(),
                    epoch,
                    rows,
                )],
                admissions,
                outcomes: model.outcomes().map(|(_, value)| value.clone()).collect(),
                outbox_intents: model
                    .outbox_intents()
                    .map(|(_, value)| value.clone())
                    .collect(),
            }
        }

        fn build(self) -> DurableInspection {
            DurableInspection::synthetic(
                RetainedMetadataV1::initial(database_id()),
                self.frontier,
                self.commits,
                self.entities,
                self.provenance,
                self.events,
                self.index_ranges,
                self.admissions,
                self.outcomes,
                self.outbox_intents,
            )
        }
    }

    fn assert_family_cause(
        divergence: &StoreDivergence,
        family: ModelFamily,
        cause: StoreDivergenceCause,
    ) {
        assert_eq!(divergence.family(), family, "family: {divergence}");
        assert_eq!(divergence.cause(), cause, "cause: {divergence}");
    }

    #[test]
    fn a_faithful_synthetic_inspection_agrees_on_every_family() {
        let (model, selection) = applied_model();
        let inspection = SyntheticParts::faithful(&model, &selection).build();
        let agreement =
            verify_model_against_inspection(&model, &inspection).expect("faithful agreement");
        assert_eq!(agreement.commits(), 1);
        assert_eq!(agreement.outcomes(), 1);
        assert_eq!(agreement.events(), 1);
        assert_eq!(agreement.provenance(), 1);
        assert_eq!(agreement.outbox_intents(), 1);
        assert_eq!(agreement.entities(), 1);
        assert_eq!(agreement.index_entries(), 1);
        assert_eq!(agreement.index_epochs(), 1);
        assert_eq!(agreement.index_epochs_absent(), 0);
        assert_eq!(agreement.admissions(), 1);
    }

    #[test]
    fn a_lost_commit_names_the_commits_family() {
        let (model, selection) = applied_model();
        let mut parts = SyntheticParts::faithful(&model, &selection);
        parts.commits.clear();
        let divergence = verify_model_against_inspection(&model, &parts.build())
            .expect_err("a lost commit must diverge");
        assert_family_cause(
            &divergence,
            ModelFamily::Commits,
            StoreDivergenceCause::MissingInStore,
        );
    }

    #[test]
    fn a_lost_provenance_record_names_the_provenance_family() {
        let (model, selection) = applied_model();
        let mut parts = SyntheticParts::faithful(&model, &selection);
        parts.provenance.clear();
        let divergence = verify_model_against_inspection(&model, &parts.build())
            .expect_err("a lost provenance record must diverge");
        assert_family_cause(
            &divergence,
            ModelFamily::Provenance,
            StoreDivergenceCause::MissingInStore,
        );
    }

    #[test]
    fn a_lost_event_names_the_events_family() {
        let (model, selection) = applied_model();
        let mut parts = SyntheticParts::faithful(&model, &selection);
        parts.events.clear();
        let divergence = verify_model_against_inspection(&model, &parts.build())
            .expect_err("a lost event must diverge");
        assert_family_cause(
            &divergence,
            ModelFamily::Events,
            StoreDivergenceCause::MissingInStore,
        );
    }

    #[test]
    fn a_lost_outbox_intent_names_the_outbox_family() {
        let (model, selection) = applied_model();
        let mut parts = SyntheticParts::faithful(&model, &selection);
        parts.outbox_intents.clear();
        let divergence = verify_model_against_inspection(&model, &parts.build())
            .expect_err("a lost outbox intent must diverge");
        assert_family_cause(
            &divergence,
            ModelFamily::OutboxIntents,
            StoreDivergenceCause::MissingInStore,
        );
    }

    /// The S5 masked class, now red-capable: a row persisted under a
    /// wrong-but-catalog-valid binding (same lineage and bundle, stale
    /// contract version) must diverge on the full-record comparison, naming
    /// the index-entries family and the exact row key.
    #[test]
    fn a_wrong_but_valid_index_binding_names_the_index_entries_family() {
        let (model, selection) = applied_model();
        let mut parts = SyntheticParts::faithful(&model, &selection);
        let (key, row) = model.index_entries().next().expect("one index row");
        let tampered = StoredIndexEntryV2::new(
            key.clone(),
            DurableKeySchemaBindingV1::new(
                row.schema_binding().lineage().clone(),
                ContractVersion::new(2).expect("version"),
                plan().contract_bundle_hash(),
            ),
            row.covered_values().clone(),
            row.partition_key().clone(),
        )
        .expect("tampered row");
        parts.index_ranges = vec![InspectedIndexRange::synthetic(
            selection.clone(),
            parts.index_ranges[0].epoch(),
            vec![tampered],
        )];
        let divergence = verify_model_against_inspection(&model, &parts.build())
            .expect_err("a wrong-but-valid binding must diverge");
        assert_family_cause(
            &divergence,
            ModelFamily::IndexEntries,
            StoreDivergenceCause::ValueDiffers,
        );
        assert_eq!(
            divergence.key(),
            render_index_entry_key(key),
            "the divergence names the exact row key"
        );
    }

    #[test]
    fn a_regressed_store_epoch_names_the_index_epochs_family() {
        let (model, selection) = applied_model();
        let mut parts = SyntheticParts::faithful(&model, &selection);
        let rows = parts.index_ranges[0].entries().to_vec();
        parts.index_ranges = vec![InspectedIndexRange::synthetic(
            selection.clone(),
            IndexEpochPosition::BeforeFirst,
            rows,
        )];
        let divergence = verify_model_against_inspection(&model, &parts.build())
            .expect_err("a regressed epoch must diverge");
        assert_family_cause(
            &divergence,
            ModelFamily::IndexEpochs,
            StoreDivergenceCause::ValueDiffers,
        );
    }

    #[test]
    fn a_non_pending_outbox_observation_reports_the_unmet_premise() {
        let (model, selection) = applied_model();
        let mut parts = SyntheticParts::faithful(&model, &selection);
        let event = model.events().next().expect("one event").1.clone();
        parts.events = vec![InspectedEvent::synthetic(
            event,
            OutboxStatusReadResultV1::AuthoritativeIntentMissing,
        )];
        let divergence = verify_model_against_inspection(&model, &parts.build())
            .expect_err("a non-pending observation must refuse the comparison");
        assert_family_cause(
            &divergence,
            ModelFamily::OutboxIntents,
            StoreDivergenceCause::NonPendingOutboxObserved,
        );
    }
}
