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
//! - **Index entries:** the `scan_index` surface exposes `(key,
//!   covered_values)` per row, re-validated through the reader's decode and
//!   filtered to the target's exact partition. The model's
//!   `StoredIndexEntryV2` additionally carries the schema binding and
//!   partition key; the binding is validated by the reader's decode (not
//!   re-exposed) and partition membership is enforced by the scan's own
//!   filter, so the comparison projects the model row to the same
//!   `(key, covered_values)` pair.
//! - **Index epochs:** the scan surface exposes the range's
//!   [`IndexEpochPosition`] observed atomically with the rows, not the whole
//!   `StoredIndexEpochV1` record. The model record is normalized to a
//!   position exactly the way the model's own prior-state checks normalize it
//!   (`BeforeFirst` when absent, `Value(epoch)` when present).
//! - **Outcomes:** the store's only public outcome read is identity-keyed
//!   (`read_stored_outcome`, which resolves the capsule/locator indirection);
//!   the model's sequence-keyed map is compared against the sequence-keyed
//!   view the inspection assembles from those identity reads.
//! - **Commits:** compared through the validated commit scan's decoded
//!   records — never through raw table row counts, which are INVALID on redb
//!   because the audited path stores segments with locator indirection.
//! - **Outbox intents:** the only public read exposing the stored intent
//!   record is `scan_pending_outbox`, whose population is intents still
//!   effectively pending. The comparison is exact in both directions, which
//!   is sound while the workload performs no delivery transitions (the
//!   oracle harness never does); a delivered intent would surface as
//!   `MissingInStore` — the comparison fails closed instead of passing
//!   silently.
//!
//! Every check runs in BOTH directions, and every model entry must be covered
//! by a declared inspection target ([`StoreDivergenceCause::UncoveredModelEntry`]
//! otherwise) — an under-declared harness fails loudly instead of comparing a
//! vacuously small surface.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use riffdb_storage_api::{
    ApplicationSequenceAllocator, EntityTarget, IdempotencyIdentityKey, IndexEpochPosition,
    IndexRangeTarget, PartitionIndexTarget, StoredEntityRecordV1, StoredIndexEntryV2,
};
use riffdb_types::{FrontierPosition, IndexEntryKey};

use crate::inspection::DurableInspection;

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

    /// Range epoch observations compared equal.
    #[must_use]
    pub const fn index_epochs(&self) -> usize {
        self.index_epochs
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
    let store_allocator = match inspection.application_frontier() {
        FrontierPosition::BeforeFirst => ApplicationSequenceAllocator::initial(),
        FrontierPosition::AppliedThrough(sequence) => sequence.checked_next().map_or(
            ApplicationSequenceAllocator::Exhausted,
            ApplicationSequenceAllocator::next,
        ),
    };
    if model.application_sequence() != store_allocator {
        return Err(StoreDivergence {
            family: ModelFamily::ApplicationSequence,
            key: render_debug(&inspection.application_frontier()),
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

/// Whether one declared range target covers one modeled index row: same
/// index, key under the range's byte prefix, and the row's partition equal to
/// the range's generation partition — the same membership tests the store's
/// own scan applies.
fn range_covers(target: &IndexRangeTarget, key: &IndexEntryKey, row: &StoredIndexEntryV2) -> bool {
    key.index_id() == target.prefix().index_id()
        && key.as_bytes().starts_with(target.prefix().as_bytes())
        && row.partition_key() == target.generation_target().partition_key()
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
            .any(|range| range_covers(range.target(), key, row))
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
            if observed_rows
                .insert(entry.key(), entry.covered_values())
                .is_some()
            {
                return Err(divergence(
                    ModelFamily::IndexEntries,
                    render_index_entry_key(entry.key()),
                    StoreDivergenceCause::DuplicateStoreEntry,
                ));
            }
        }
        for (key, row) in model.index_entries() {
            if !range_covers(range.target(), key, row) {
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
                Some(observed) if observed == row.covered_values() => {
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
            .any(|range| range.target().generation_target() == target)
        {
            return Err(divergence(
                ModelFamily::IndexEpochs,
                render_partition_index_target(target),
                StoreDivergenceCause::UncoveredModelEntry,
            ));
        }
    }
    for range in inspection.index_ranges() {
        let target = range.target().generation_target();
        // The scan exposes the range's epoch POSITION; normalize the model
        // record exactly as the model's own prior-state checks do.
        let modeled = model
            .index_epoch(target)
            .map_or(IndexEpochPosition::BeforeFirst, |record| {
                IndexEpochPosition::Value(record.epoch())
            });
        if modeled == range.epoch() {
            agreement.index_epochs += 1;
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
        ExecutablePlanRef, IdempotencyIdentity, IdempotencyKeyDigest,
        StoredAdmittedProvenanceClaimsV1, StoredPendingAdmissionV1,
    };
    use riffdb_types::{
        ActorId, ActorKind, AdmittedActorContext, AggregateTypeId, CanonicalInputHash, CommandId,
        ContractBundleHash, ContractLineage, ContractVersion, DatabaseId, DigestKeyId, Environment,
        LogicalTime, PartitionKeyBuilder, PlanHash, RequestId, TenantId, TenantScope, Timestamp,
    };

    use super::*;

    fn identity(digest: u8) -> IdempotencyIdentity {
        IdempotencyIdentity::new(
            DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10])
                .expect("database ID"),
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
            ExecutablePlanRef::new(
                ContractLineage::new("comparison-test").expect("lineage"),
                ContractVersion::new(1).expect("version"),
                ContractBundleHash::from_bytes([0x21; 32]),
                CommandId::new(1).expect("command"),
                PlanHash::from_bytes([0x22; 32]),
            ),
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
}
