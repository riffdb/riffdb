// req: BLK-024, BLK-025, BLK-027, TXN-030, TXN-031
//! Independent authoritative model and canonical 256-target page comparison
//! oracle for correlated secondary-index work (ADR-0212 / WP-684).

#![allow(clippy::result_large_err)]

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;

use riffdb_contract_compiler::compile_contract_source;
use riffdb_contract_ir::{
    ContractBundle, EntitySchema, encode_operational_index_cover_v1,
    encode_operational_index_values_v1,
};
use riffdb_storage_api::{
    AuthoritativeNamespaceV1, DurableKeySchemaBindingV1, EntityTarget, ExecutablePlanRef,
    IndexEpochPosition, IndexRangePrefixBuilder, MAX_AFFECTED_INDEX_EPOCH_TARGETS,
    MAX_COMMAND_READ_TARGETS, MAX_INDEX_DELTAS, MAX_INDEX_VALIDATION_POSITIONS,
    MAX_INDEX_WORK_UNITS, MAX_READ_SNAPSHOT_BYTES, PartitionIndexTarget, StoredEntityRecordV1,
    StoredIndexEntryV2,
};
use riffdb_types::{
    CanonicalBytes, CanonicalRecord, CanonicalString, CanonicalValue, CommitSequence,
    EntityVersion, FieldId, FrontierPosition, IndexEntryKey, IndexEpoch, IndexId, PartitionKey,
    Timestamp,
};

use crate::inspection::{
    CorrelatedCorpusNamespace, CorrelatedInspectionContinuation, CorrelatedInspectionPage,
    CorrelatedNamespaceInspectionSession, CorrelatedTargetKey, CorrelatedTargetObservation,
    MAX_CORRELATED_INSPECTION_PAGES, MAX_CORRELATED_INSPECTION_TOTAL_TARGETS,
    MAX_INSPECTION_TARGETS,
};

const INDEXED_MUTATION_BULK_SOURCE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../fixtures/contracts/bulk/indexed-mutations.riff"
));

const AFFECTED_CURRENT_STATE_FIXED_BYTES_V1: usize = 4;
const AFFECTED_EPOCH_OBSERVATION_FIXED_BYTES_V1: usize = 17;
const UNIQUE_OCCUPANCY_FIXED_BYTES_V1: usize = 13;

/// Compiles the canonical `IndexedMutationBulk` contract fixture (`18 + 2 = 20` indexes).
#[must_use]
pub fn indexed_mutation_bulk_bundle() -> ContractBundle {
    compile_contract_source(INDEXED_MUTATION_BULK_SOURCE)
        .expect("canonical IndexedMutationBulk contract compiles")
}

/// Closed work-accounting budget error detected by the independent model.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CorrelatedWorkBudgetError {
    /// Collection element count was outside `1..=100`.
    CollectionCountOutOfBounds {
        /// Observed collection length.
        observed: usize,
    },
    /// Read target count (`B + R + C + G`) exceeded `MAX_COMMAND_READ_TARGETS` (`4,096`).
    ReadTargetCeilingExceeded {
        /// Observed read target count.
        observed: usize,
    },
    /// Candidate index entry delta count (`D`) exceeded `MAX_INDEX_DELTAS` (`4,096`).
    DeltaCeilingExceeded {
        /// Observed delta count.
        observed: usize,
    },
    /// Affected index-epoch target count (`A`) exceeded `MAX_AFFECTED_INDEX_EPOCH_TARGETS` (`65,535`).
    AffectedTargetCeilingExceeded {
        /// Observed affected target count.
        observed: usize,
    },
    /// Unique occupancy target count (`U`) exceeded `MAX_AFFECTED_INDEX_EPOCH_TARGETS` (`65,535`).
    UniqueTargetCeilingExceeded {
        /// Observed unique target count.
        observed: usize,
    },
    /// Total validation positions (`V`) exceeded `MAX_INDEX_VALIDATION_POSITIONS` (`65,535`).
    ValidationPositionCeilingExceeded {
        /// Observed validation position count.
        observed: usize,
    },
    /// Total correlated index work units (`W = D + A + V`) exceeded `MAX_INDEX_WORK_UNITS` (`65,535`).
    WorkUnitCeilingExceeded {
        /// Observed work unit count.
        observed: usize,
    },
    /// Total affected-epoch current-state semantic bytes exceeded `MAX_READ_SNAPSHOT_BYTES` (`16 MiB`).
    SemanticBytesCeilingExceeded {
        /// Observed semantic byte count.
        observed: usize,
    },
    /// Arithmetic overflow while summing work metrics.
    ArithmeticOverflow,
}

impl fmt::Display for CorrelatedWorkBudgetError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl Error for CorrelatedWorkBudgetError {}

/// Exact correlated-index work metrics (`D`, `A`, `U`, `V`, `W`, and semantic bytes)
/// independently computed for one command candidate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CorrelatedCommandWorkMetrics {
    binding_targets: usize,
    root_validation_targets: usize,
    cascade_targets: usize,
    range_targets: usize,
    delta_count: usize,
    affected_epoch_target_count: usize,
    unique_target_count: usize,
    validation_positions: usize,
    work_units: usize,
    affected_current_semantic_bytes: usize,
}

impl CorrelatedCommandWorkMetrics {
    /// Independently checks and computes `(V, W, affected_current_semantic_bytes)` from
    /// raw candidate dimensions.
    #[allow(clippy::too_many_arguments)]
    pub fn check_dimensions(
        binding_targets: usize,
        root_validation_targets: usize,
        cascade_targets: usize,
        range_targets: usize,
        delta_count: usize,
        affected_epoch_target_count: usize,
        unique_target_count: usize,
        affected_target_partition_bytes_sum: usize,
        unique_target_prefix_and_entry_bytes_sum: usize,
    ) -> Result<Self, CorrelatedWorkBudgetError> {
        let base_read_positions = binding_targets
            .checked_add(root_validation_targets)
            .and_then(|v| v.checked_add(cascade_targets))
            .and_then(|v| v.checked_add(range_targets))
            .ok_or(CorrelatedWorkBudgetError::ArithmeticOverflow)?;
        if base_read_positions > MAX_COMMAND_READ_TARGETS {
            return Err(CorrelatedWorkBudgetError::ReadTargetCeilingExceeded {
                observed: base_read_positions,
            });
        }
        if delta_count > MAX_INDEX_DELTAS {
            return Err(CorrelatedWorkBudgetError::DeltaCeilingExceeded {
                observed: delta_count,
            });
        }
        if affected_epoch_target_count > MAX_AFFECTED_INDEX_EPOCH_TARGETS {
            return Err(CorrelatedWorkBudgetError::AffectedTargetCeilingExceeded {
                observed: affected_epoch_target_count,
            });
        }
        if unique_target_count > MAX_AFFECTED_INDEX_EPOCH_TARGETS {
            return Err(CorrelatedWorkBudgetError::UniqueTargetCeilingExceeded {
                observed: unique_target_count,
            });
        }
        let validation_positions = base_read_positions
            .checked_add(affected_epoch_target_count)
            .and_then(|v| v.checked_add(unique_target_count))
            .ok_or(CorrelatedWorkBudgetError::ArithmeticOverflow)?;
        if validation_positions > MAX_INDEX_VALIDATION_POSITIONS {
            return Err(
                CorrelatedWorkBudgetError::ValidationPositionCeilingExceeded {
                    observed: validation_positions,
                },
            );
        }
        let work_units = validation_positions
            .checked_add(delta_count)
            .and_then(|w| w.checked_add(affected_epoch_target_count))
            .ok_or(CorrelatedWorkBudgetError::ArithmeticOverflow)?;
        if work_units > MAX_INDEX_WORK_UNITS {
            return Err(CorrelatedWorkBudgetError::WorkUnitCeilingExceeded {
                observed: work_units,
            });
        }
        let epoch_fixed = affected_epoch_target_count
            .checked_mul(AFFECTED_EPOCH_OBSERVATION_FIXED_BYTES_V1)
            .ok_or(CorrelatedWorkBudgetError::ArithmeticOverflow)?;
        let unique_fixed = unique_target_count
            .checked_mul(UNIQUE_OCCUPANCY_FIXED_BYTES_V1)
            .ok_or(CorrelatedWorkBudgetError::ArithmeticOverflow)?;
        let affected_current_semantic_bytes = AFFECTED_CURRENT_STATE_FIXED_BYTES_V1
            .checked_add(affected_target_partition_bytes_sum)
            .and_then(|b| b.checked_add(epoch_fixed))
            .and_then(|b| b.checked_add(unique_target_prefix_and_entry_bytes_sum))
            .and_then(|b| b.checked_add(unique_fixed))
            .ok_or(CorrelatedWorkBudgetError::ArithmeticOverflow)?;
        if affected_current_semantic_bytes > MAX_READ_SNAPSHOT_BYTES {
            return Err(CorrelatedWorkBudgetError::SemanticBytesCeilingExceeded {
                observed: affected_current_semantic_bytes,
            });
        }
        Ok(Self {
            binding_targets,
            root_validation_targets,
            cascade_targets,
            range_targets,
            delta_count,
            affected_epoch_target_count,
            unique_target_count,
            validation_positions,
            work_units,
            affected_current_semantic_bytes,
        })
    }

    /// Returns the number of binding read targets (`B`).
    #[must_use]
    pub const fn binding_targets(&self) -> usize {
        self.binding_targets
    }

    /// Returns the number of root validation targets (`R`).
    #[must_use]
    pub const fn root_validation_targets(&self) -> usize {
        self.root_validation_targets
    }

    /// Returns the number of cascade targets (`C`).
    #[must_use]
    pub const fn cascade_targets(&self) -> usize {
        self.cascade_targets
    }

    /// Returns the number of range targets (`G`).
    #[must_use]
    pub const fn range_targets(&self) -> usize {
        self.range_targets
    }

    /// Returns `D` (`<= 4,096`).
    #[must_use]
    pub const fn delta_count(&self) -> usize {
        self.delta_count
    }

    /// Returns `A` (`<= 65,535`).
    #[must_use]
    pub const fn affected_epoch_target_count(&self) -> usize {
        self.affected_epoch_target_count
    }

    /// Returns `U` (`<= 65,535`).
    #[must_use]
    pub const fn unique_target_count(&self) -> usize {
        self.unique_target_count
    }

    /// Returns `V` (`<= 65,535`).
    #[must_use]
    pub const fn validation_positions(&self) -> usize {
        self.validation_positions
    }

    /// Returns `W = D + A + V` (`<= 65,535`).
    #[must_use]
    pub const fn work_units(&self) -> usize {
        self.work_units
    }

    /// Returns the checked affected-epoch current-state semantic byte footprint (`<= 16 MiB`).
    #[must_use]
    pub const fn affected_current_semantic_bytes(&self) -> usize {
        self.affected_current_semantic_bytes
    }
}

/// Verifies the exact and `+1` boundary witnesses for `D`, `A`, `V`, `W`, and
/// `16 MiB` semantic bytes required by ADR-0212 (`BLK-024`, `BLK-025`, `BLK-027`,
/// `TXN-030`, `TXN-031`).
pub fn verify_work_accounting_boundary_witnesses() -> Result<(), String> {
    // 1. D = 4,096 admitted; D = 4,097 rejected.
    let exact_d = CorrelatedCommandWorkMetrics::check_dimensions(1, 0, 0, 0, 4_096, 1, 0, 32, 0)
        .map_err(|e| format!("exact D=4096 witness failed: {e}"))?;
    if exact_d.delta_count() != 4_096 || exact_d.work_units() != 4_099 {
        return Err("exact D=4096 witness produced unexpected metrics".to_owned());
    }
    if CorrelatedCommandWorkMetrics::check_dimensions(1, 0, 0, 0, 4_097, 1, 0, 32, 0)
        != Err(CorrelatedWorkBudgetError::DeltaCeilingExceeded { observed: 4_097 })
    {
        return Err("D=4097 witness did not fail with DeltaCeilingExceeded".to_owned());
    }

    // 2. A = 65,535 ceiling vs A = 65,536 rejected.
    if CorrelatedCommandWorkMetrics::check_dimensions(0, 0, 0, 0, 0, 65_536, 0, 0, 0)
        != Err(CorrelatedWorkBudgetError::AffectedTargetCeilingExceeded { observed: 65_536 })
    {
        return Err("A=65536 witness did not fail with AffectedTargetCeilingExceeded".to_owned());
    }

    // 3. V = 65,535 admitted (with B=4096, U=61439, A=0, D=0 -> V=65535, W=65535)
    //    vs V = 65,536 rejected (with B=4096, U=61440, A=0, D=0 -> V=65536).
    let exact_v =
        CorrelatedCommandWorkMetrics::check_dimensions(4_096, 0, 0, 0, 0, 0, 61_439, 0, 61_439)
            .map_err(|e| format!("exact V=65535 witness failed: {e}"))?;
    if exact_v.validation_positions() != 65_535 || exact_v.work_units() != 65_535 {
        return Err("exact V=65535 witness produced unexpected metrics".to_owned());
    }
    if CorrelatedCommandWorkMetrics::check_dimensions(4_096, 0, 0, 0, 0, 0, 61_440, 0, 61_440)
        != Err(CorrelatedWorkBudgetError::ValidationPositionCeilingExceeded { observed: 65_536 })
    {
        return Err(
            "V=65536 witness did not fail with ValidationPositionCeilingExceeded".to_owned(),
        );
    }

    // 4. W = 65,535 admitted (B=0, D=4095, A=30720, U=0 -> V=30720, W=65535)
    //    vs W = 65,536 rejected (B=0, D=4096, A=30720, U=0 -> V=30720, W=65536).
    let exact_w =
        CorrelatedCommandWorkMetrics::check_dimensions(0, 0, 0, 0, 4_095, 30_720, 0, 30_720, 0)
            .map_err(|e| format!("exact W=65535 witness failed: {e}"))?;
    if exact_w.work_units() != 65_535 {
        return Err("exact W=65535 witness produced unexpected work units".to_owned());
    }
    if CorrelatedCommandWorkMetrics::check_dimensions(0, 0, 0, 0, 4_096, 30_720, 0, 30_720, 0)
        != Err(CorrelatedWorkBudgetError::WorkUnitCeilingExceeded { observed: 65_536 })
    {
        return Err("W=65536 witness did not fail with WorkUnitCeilingExceeded".to_owned());
    }

    // 5. Semantic bytes = 16,777,216 (16 MiB) admitted vs 16,777,217 rejected.
    let exact_bytes =
        CorrelatedCommandWorkMetrics::check_dimensions(1, 0, 0, 0, 1, 1, 0, 16_777_195, 0)
            .map_err(|e| format!("exact 16 MiB semantic bytes witness failed: {e}"))?;
    if exact_bytes.affected_current_semantic_bytes() != MAX_READ_SNAPSHOT_BYTES {
        return Err("exact 16 MiB witness did not equal MAX_READ_SNAPSHOT_BYTES".to_owned());
    }
    if CorrelatedCommandWorkMetrics::check_dimensions(1, 0, 0, 0, 1, 1, 0, 16_777_196, 0)
        != Err(CorrelatedWorkBudgetError::SemanticBytesCeilingExceeded {
            observed: MAX_READ_SNAPSHOT_BYTES + 1,
        })
    {
        return Err("16 MiB + 1 witness did not fail with SemanticBytesCeilingExceeded".to_owned());
    }

    Ok(())
}

/// Deterministic input row for `SeedInitialIndexedStateRows` and `ApplyIndexedMutations`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CorrelatedIndexedMutationRow {
    /// Partition identifier (`string<26>`).
    pub partition_id: String,
    /// Mutation change identifier (`string<26>`).
    pub change_id: String,
    /// Category component (`string<128>`).
    pub category: String,
    /// Item identifier (`string<256>`).
    pub item_id: String,
    /// Relation component (`string<50>`).
    pub relation: String,
    /// Subject component (`string<512>`).
    pub subject: String,
    /// Subject category field (`string<128>`).
    pub subject_category: String,
    /// Subject identifier field (`string<256>`).
    pub subject_id: String,
    /// Subject relation field (`string<50>`).
    pub subject_relation: String,
    /// Subject kind field (`string<7>`).
    pub subject_kind: String,
    /// Operation enum variant (`true` = `Activate`, `false` = `Deactivate`).
    pub activate_operation: bool,
    /// Qualifier field (`string<256>`).
    pub qualifier: String,
    /// Optional condition context bytes (`optional<bytes<32768>>`).
    pub condition_context: Option<Vec<u8>>,
    /// Timestamp seconds since Unix epoch.
    pub inserted_at_seconds: i64,
    /// Expected active flag before `ApplyIndexedMutations` (and initial active flag on seed).
    pub expected_active: bool,
    /// Next active flag after `ApplyIndexedMutations`.
    pub next_active: bool,
    /// Expected revision before `ApplyIndexedMutations` (and initial revision on seed).
    pub expected_revision: u64,
}

impl CorrelatedIndexedMutationRow {
    /// Builds one deterministic row for `ordinal` (`0..100`) under `partition_id`.
    #[must_use]
    pub fn deterministic(
        partition_id: &str,
        ordinal: u16,
        expected_active: bool,
        next_active: bool,
        expected_revision: u64,
    ) -> Self {
        Self {
            partition_id: partition_id.to_owned(),
            change_id: format!("chg-{ordinal:04x}-r{expected_revision:02x}"),
            category: format!("document-cat-{ordinal:03}"),
            item_id: format!("item-{ordinal:04}"),
            relation: if ordinal.is_multiple_of(2) {
                "viewer".to_owned()
            } else {
                "editor".to_owned()
            },
            subject: format!("user:principal-{ordinal:04}"),
            subject_category: "user".to_owned(),
            subject_id: format!("principal-{ordinal:04}"),
            subject_relation: "member".to_owned(),
            subject_kind: "direct".to_owned(),
            activate_operation: next_active,
            qualifier: format!("qual-{}", ordinal % 7),
            condition_context: Some(vec![u8::try_from(ordinal % 251).unwrap_or(1); 32]),
            inserted_at_seconds: 1_760_000_000_i64 + i64::from(ordinal),
            expected_active,
            next_active,
            expected_revision,
        }
    }

    /// Generates a deterministic batch of `count` rows (`1..=100`).
    #[must_use]
    pub fn deterministic_batch(
        partition_id: &str,
        count: usize,
        expected_active: bool,
        next_active: bool,
        expected_revision: u64,
    ) -> Vec<Self> {
        (0..count)
            .map(|index| {
                let ordinal = u16::try_from(index).expect("bounded row ordinal");
                Self::deterministic(
                    partition_id,
                    ordinal,
                    expected_active,
                    next_active,
                    expected_revision,
                )
            })
            .collect()
    }
}

/// Closed divergence classes between `CorrelatedIndexWorkModel` and stopped-redb
/// canonical 256-target inspection pages (ADR-0212 / OBL-0212-2).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CorrelatedModelDivergence {
    /// An expected target was missing from the observed page.
    MissingTarget {
        /// Page ordinal where the missing target was detected.
        page_ordinal: u16,
        /// Canonical target key missing from observed state.
        key: CorrelatedTargetKey,
    },
    /// An extra declared target was present in the observed page.
    ExtraTarget {
        /// Page ordinal where the extra target was detected.
        page_ordinal: u16,
        /// Canonical target key unexpectedly present in observed state.
        key: CorrelatedTargetKey,
    },
    /// The same target key appeared twice in the observed page stream.
    DuplicateTarget {
        /// Page ordinal where the duplicate target appeared.
        page_ordinal: u16,
        /// Duplicated canonical target key.
        key: CorrelatedTargetKey,
    },
    /// Two adjacent targets in the observed page stream were out of canonical order.
    AdjacentReorder {
        /// Page ordinal where the reorder appeared.
        page_ordinal: u16,
        /// Preceding target key (`left > right`).
        left: CorrelatedTargetKey,
        /// Succeeding target key (`left > right`).
        right: CorrelatedTargetKey,
    },
    /// Target identity or prefix bytes differed at the same canonical position.
    WrongTargetIdentityOrPrefix {
        /// Page ordinal where the target mismatch appeared.
        page_ordinal: u16,
        /// Expected canonical target key.
        expected: CorrelatedTargetKey,
        /// Observed canonical target key.
        observed: CorrelatedTargetKey,
    },
    /// Engine-side enumeration discovered an undeclared physical row in redb.
    RedbOnlyUndeclaredTarget {
        /// Page ordinal (`0` when detected during pre-page namespace enumeration).
        page_ordinal: u16,
        /// Authoritative table containing the undeclared row.
        namespace: AuthoritativeNamespaceV1,
        /// Raw physical key bytes.
        raw_key: Vec<u8>,
    },
    /// Target identity matched, but generation epoch or entity/index post-state differed.
    WrongGenerationOrPostState {
        /// Page ordinal where the value mismatch appeared.
        page_ordinal: u16,
        /// Canonical target key whose post-state or generation epoch differed.
        key: CorrelatedTargetKey,
    },
    /// Recovered application frontier differed from the model's committed sequence.
    FrontierMismatch {
        /// Expected application frontier.
        expected: FrontierPosition,
        /// Observed application frontier.
        observed: FrontierPosition,
    },
    /// Canonical page count differed between model and observed redb inspection.
    PageCountMismatch {
        /// Expected number of canonical 256-target pages.
        expected: usize,
        /// Observed number of canonical 256-target pages.
        observed: usize,
    },
    /// Underlying durable inspection failed before comparison completed.
    InspectionFailure(String),
}

impl fmt::Display for CorrelatedModelDivergence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl Error for CorrelatedModelDivergence {}

/// Non-trivial agreement counts returned when `CorrelatedIndexWorkModel` matches
/// stopped redb across all canonical 256-target inspection pages.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CorrelatedStoreAgreement {
    page_count: usize,
    total_targets: usize,
    live_entities: usize,
    absent_entities: usize,
    live_index_entries: usize,
    absent_index_entries: usize,
    advanced_epochs: usize,
    before_first_epochs: usize,
}

impl CorrelatedStoreAgreement {
    /// Number of canonical 256-target pages verified.
    #[must_use]
    pub const fn page_count(&self) -> usize {
        self.page_count
    }

    /// Total targets verified across all pages.
    #[must_use]
    pub const fn total_targets(&self) -> usize {
        self.total_targets
    }

    /// Present entity records verified equal.
    #[must_use]
    pub const fn live_entities(&self) -> usize {
        self.live_entities
    }

    /// Absent candidate entity targets verified absent.
    #[must_use]
    pub const fn absent_entities(&self) -> usize {
        self.absent_entities
    }

    /// Present secondary-index entries verified equal.
    #[must_use]
    pub const fn live_index_entries(&self) -> usize {
        self.live_index_entries
    }

    /// Absent (deleted or uncreated) candidate secondary-index entries verified absent.
    #[must_use]
    pub const fn absent_index_entries(&self) -> usize {
        self.absent_index_entries
    }

    /// Advanced index-epoch targets verified equal.
    #[must_use]
    pub const fn advanced_epochs(&self) -> usize {
        self.advanced_epochs
    }

    /// Unadvanced (`BeforeFirst`) index-epoch targets verified equal.
    #[must_use]
    pub const fn before_first_epochs(&self) -> usize {
        self.before_first_epochs
    }
}

/// Independent authoritative model for `IndexedMutationBulk` (`SeedInitialIndexedStateRows`
/// and `ApplyIndexedMutations`).
#[derive(Clone)]
pub struct CorrelatedIndexWorkModel {
    bundle: ContractBundle,
    namespace: CorrelatedCorpusNamespace,
    partition_id: String,
    partition_key: PartitionKey,
    seed_schema_binding: DurableKeySchemaBindingV1,
    apply_schema_binding: DurableKeySchemaBindingV1,
    frontier: FrontierPosition,
    live_entities: BTreeMap<EntityTarget, StoredEntityRecordV1>,
    candidate_entities: BTreeSet<EntityTarget>,
    live_index_entries: BTreeMap<IndexEntryKey, (IndexId, Vec<u8>, StoredIndexEntryV2)>,
    candidate_index_entries: BTreeMap<IndexEntryKey, (IndexId, Vec<u8>)>,
    index_epochs: BTreeMap<PartitionIndexTarget, IndexEpochPosition>,
    declared_epoch_prefixes: BTreeMap<PartitionIndexTarget, Vec<u8>>,
}

impl CorrelatedIndexWorkModel {
    /// Creates an empty independent model for `partition_id` under `IndexedMutationBulk`.
    #[must_use]
    pub fn new(partition_id: &str) -> Self {
        let bundle = indexed_mutation_bulk_bundle();
        let lineage = bundle.lineage().clone();
        let aggregate = bundle
            .schema()
            .aggregates()
            .first()
            .expect("IndexedRecords aggregate");
        let partition_val = CanonicalValue::String(
            CanonicalString::new(partition_id.to_owned()).expect("valid partition_id string"),
        );
        let partition_key = aggregate
            .keys()
            .partition_schema()
            .encode_partition(std::slice::from_ref(&partition_val))
            .expect("encode partition key");

        let state_entity = Self::find_entity(&bundle, "IndexedState");
        let change_entity = Self::find_entity(&bundle, "MutationChange");
        let mut entity_prefixes = vec![
            (
                state_entity.id(),
                state_entity
                    .primary_key()
                    .encode_entity_prefix(std::slice::from_ref(&partition_val))
                    .expect("encode IndexedState partition prefix"),
            ),
            (
                change_entity.id(),
                change_entity
                    .primary_key()
                    .encode_entity_prefix(std::slice::from_ref(&partition_val))
                    .expect("encode MutationChange partition prefix"),
            ),
        ];
        entity_prefixes.sort();

        let mut index_ids = Vec::new();
        let mut index_epochs = BTreeMap::new();
        let mut declared_epoch_prefixes = BTreeMap::new();
        for entity in [state_entity, change_entity] {
            for index in entity.indexes() {
                index_ids.push(index.id());
                let target = PartitionIndexTarget::new(partition_key.clone(), index.id());
                let prefix_bytes = IndexRangePrefixBuilder::new(index.id())
                    .finish()
                    .as_bytes()
                    .to_vec();
                index_epochs.insert(target.clone(), IndexEpochPosition::BeforeFirst);
                declared_epoch_prefixes.insert(target, prefix_bytes);
            }
        }
        index_ids.sort();

        let namespace = CorrelatedCorpusNamespace::new(
            lineage.clone(),
            aggregate.id(),
            partition_key.clone(),
            entity_prefixes,
            index_ids,
        )
        .expect("canonical CorrelatedCorpusNamespace");

        let seed_cmd = bundle
            .commands()
            .iter()
            .find(|cmd| cmd.name() == "SeedInitialIndexedStateRows")
            .expect("SeedInitialIndexedStateRows command");
        let seed_plan_ref = ExecutablePlanRef::new(
            lineage.clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
            seed_cmd.command_id(),
            seed_cmd.plan_hash(),
        );
        let seed_schema_binding = DurableKeySchemaBindingV1::from_plan(&seed_plan_ref);

        let apply_cmd = bundle
            .commands()
            .iter()
            .find(|cmd| cmd.name() == "ApplyIndexedMutations")
            .expect("ApplyIndexedMutations command");
        let apply_plan_ref = ExecutablePlanRef::new(
            lineage,
            bundle.contract_version(),
            bundle.bundle_hash(),
            apply_cmd.command_id(),
            apply_cmd.plan_hash(),
        );
        let apply_schema_binding = DurableKeySchemaBindingV1::from_plan(&apply_plan_ref);

        Self {
            bundle,
            namespace,
            partition_id: partition_id.to_owned(),
            partition_key,
            seed_schema_binding,
            apply_schema_binding,
            frontier: FrontierPosition::BeforeFirst,
            live_entities: BTreeMap::new(),
            candidate_entities: BTreeSet::new(),
            live_index_entries: BTreeMap::new(),
            candidate_index_entries: BTreeMap::new(),
            index_epochs,
            declared_epoch_prefixes,
        }
    }

    /// Borrows the compiled `IndexedMutationBulk` bundle.
    #[must_use]
    pub const fn bundle(&self) -> &ContractBundle {
        &self.bundle
    }

    /// Borrows the declared corpus namespace.
    #[must_use]
    pub const fn namespace(&self) -> &CorrelatedCorpusNamespace {
        &self.namespace
    }

    /// Borrows the partition ID string.
    #[must_use]
    pub fn partition_id(&self) -> &str {
        &self.partition_id
    }

    /// Borrows the encoded partition key.
    #[must_use]
    pub const fn partition_key(&self) -> &PartitionKey {
        &self.partition_key
    }

    /// Returns the expected recovered application frontier.
    #[must_use]
    pub const fn expected_frontier(&self) -> FrontierPosition {
        self.frontier
    }

    /// Encodes a batch of [`CorrelatedIndexedMutationRow`] values into the canonical
    /// `CanonicalRecord` input for `SeedInitialIndexedStateRows` or `ApplyIndexedMutations`.
    pub fn encode_command_input(
        &self,
        command_name: &str,
        request_id_bytes: [u8; 16],
        rows: &[CorrelatedIndexedMutationRow],
    ) -> CanonicalRecord {
        let command = self
            .bundle
            .commands()
            .iter()
            .find(|cmd| cmd.name() == command_name)
            .expect("command exists in IndexedMutationBulk");
        let input_fields = command.input().record().fields();
        let request_id_field = input_fields
            .iter()
            .find(|f| f.name() == "request_id")
            .expect("request_id input field")
            .id();
        let mutations_field = input_fields
            .iter()
            .find(|f| f.name() == "mutations")
            .expect("mutations input field")
            .id();

        let mutation_entity = Self::find_entity(&self.bundle, "IndexedMutation");
        let operation_enum = self
            .bundle
            .schema()
            .enums()
            .iter()
            .find(|e| e.name() == "MutationOperation")
            .expect("MutationOperation enum");
        let activate_id = operation_enum
            .variants()
            .iter()
            .find(|v| v.name() == "Activate")
            .expect("Activate variant")
            .id();
        let deactivate_id = operation_enum
            .variants()
            .iter()
            .find(|v| v.name() == "Deactivate")
            .expect("Deactivate variant")
            .id();

        let elements = rows
            .iter()
            .map(|row| {
                let variant_id = if row.activate_operation {
                    activate_id
                } else {
                    deactivate_id
                };
                let record = Self::build_record_by_name(
                    mutation_entity,
                    &[
                        ("partition_id", Self::str_val(&row.partition_id)),
                        ("change_id", Self::str_val(&row.change_id)),
                        ("category", Self::str_val(&row.category)),
                        ("item_id", Self::str_val(&row.item_id)),
                        ("relation", Self::str_val(&row.relation)),
                        ("subject", Self::str_val(&row.subject)),
                        ("subject_category", Self::str_val(&row.subject_category)),
                        ("subject_id", Self::str_val(&row.subject_id)),
                        ("subject_relation", Self::str_val(&row.subject_relation)),
                        ("subject_kind", Self::str_val(&row.subject_kind)),
                        (
                            "operation",
                            CanonicalValue::Enum {
                                type_id: operation_enum.id(),
                                variant_id,
                            },
                        ),
                        ("qualifier", Self::str_val(&row.qualifier)),
                        (
                            "condition_context",
                            match &row.condition_context {
                                Some(bytes) => CanonicalValue::Bytes(
                                    CanonicalBytes::new(bytes.clone())
                                        .expect("bounded condition_context"),
                                ),
                                None => CanonicalValue::Null,
                            },
                        ),
                        (
                            "inserted_at",
                            CanonicalValue::Timestamp(
                                Timestamp::new(row.inserted_at_seconds, 0)
                                    .expect("valid Timestamp"),
                            ),
                        ),
                        ("expected_active", CanonicalValue::Bool(row.expected_active)),
                        ("next_active", CanonicalValue::Bool(row.next_active)),
                        (
                            "expected_revision",
                            CanonicalValue::U64(row.expected_revision),
                        ),
                    ],
                );
                CanonicalValue::Record(record)
            })
            .collect::<Vec<_>>();

        CanonicalRecord::new(vec![
            (request_id_field, CanonicalValue::Uuid(request_id_bytes)),
            (
                mutations_field,
                CanonicalValue::List(
                    riffdb_types::CanonicalList::new(elements).expect("bounded mutations list"),
                ),
            ),
        ])
        .expect("canonical command input record")
    }

    /// Registers candidate targets for `rows` WITHOUT applying their mutations to live state
    /// (used to verify that pre-commit crash or rolled-back candidates are completely absent).
    pub fn register_uncommitted_candidates(&mut self, rows: &[CorrelatedIndexedMutationRow]) {
        let state_entity = Self::find_entity(&self.bundle, "IndexedState");
        let change_entity = Self::find_entity(&self.bundle, "MutationChange");
        for row in rows {
            assert_eq!(row.partition_id, self.partition_id);
            let (state_target, state_seed_fields) = self.build_indexed_state_record(
                state_entity,
                row,
                row.expected_active,
                row.expected_revision,
            );
            let (_, state_mutated_fields) = self.build_indexed_state_record(
                state_entity,
                row,
                row.next_active,
                row.expected_revision + 1,
            );
            self.candidate_entities.insert(state_target.clone());
            for fields in [&state_seed_fields, &state_mutated_fields] {
                for index in state_entity.indexes() {
                    let values = encode_operational_index_values_v1(index, fields)
                        .expect("encode index values");
                    let key = index
                        .key_schema()
                        .encode_index(&values, state_target.key().clone())
                        .expect("encode index entry key");
                    let prefix_bytes = index
                        .key_schema()
                        .encode_index_prefix(&values)
                        .expect("encode index prefix")
                        .as_bytes()
                        .to_vec();
                    self.candidate_index_entries
                        .insert(key, (index.id(), prefix_bytes));
                }
            }
            let (change_target, change_fields) =
                self.build_mutation_change_record(change_entity, row);
            self.candidate_entities.insert(change_target.clone());
            for index in change_entity.indexes() {
                let values = encode_operational_index_values_v1(index, &change_fields)
                    .expect("encode change index values");
                let key = index
                    .key_schema()
                    .encode_index(&values, change_target.key().clone())
                    .expect("encode change index key");
                let prefix_bytes = index
                    .key_schema()
                    .encode_index_prefix(&values)
                    .expect("encode change index prefix")
                    .as_bytes()
                    .to_vec();
                self.candidate_index_entries
                    .insert(key, (index.id(), prefix_bytes));
            }
        }
    }

    /// Independently applies `SeedInitialIndexedStateRows` to the model and returns
    /// the checked work metrics (`D = 18 * N`, `A = 18`, `V = N + 18`, `W = 19 * N + 36`).
    pub fn apply_seed_initial_rows(
        &mut self,
        rows: &[CorrelatedIndexedMutationRow],
    ) -> Result<CorrelatedCommandWorkMetrics, CorrelatedWorkBudgetError> {
        if rows.is_empty() || rows.len() > 100 {
            return Err(CorrelatedWorkBudgetError::CollectionCountOutOfBounds {
                observed: rows.len(),
            });
        }
        let next_sequence = self.next_commit_sequence();
        let state_entity = Self::find_entity(&self.bundle, "IndexedState");
        let mut touched_epochs = BTreeSet::new();
        let mut delta_count = 0usize;

        for row in rows {
            assert_eq!(row.partition_id, self.partition_id);
            let (target, fields) = self.build_indexed_state_record(
                state_entity,
                row,
                row.expected_active,
                row.expected_revision,
            );
            let record = StoredEntityRecordV1::new(
                target.clone(),
                EntityVersion::first(),
                self.bundle.contract_version(),
                self.seed_schema_binding.clone(),
                fields.clone(),
            )
            .expect("valid seeded StoredEntityRecordV1");
            self.candidate_entities.insert(target.clone());
            self.live_entities.insert(target.clone(), record);

            for index in state_entity.indexes() {
                let values = encode_operational_index_values_v1(index, &fields)
                    .expect("encode seeded index values");
                let covered = encode_operational_index_cover_v1(index, &fields)
                    .expect("encode seeded index cover");
                let key = index
                    .key_schema()
                    .encode_index(&values, target.key().clone())
                    .expect("encode seeded index key");
                let prefix_bytes = index
                    .key_schema()
                    .encode_index_prefix(&values)
                    .expect("encode seeded index prefix")
                    .as_bytes()
                    .to_vec();
                let entry = StoredIndexEntryV2::new(
                    key.clone(),
                    self.seed_schema_binding.clone(),
                    covered,
                    self.partition_key.clone(),
                )
                .expect("valid StoredIndexEntryV2");
                self.candidate_index_entries
                    .insert(key.clone(), (index.id(), prefix_bytes.clone()));
                self.live_index_entries
                    .insert(key, (index.id(), prefix_bytes, entry));
                touched_epochs.insert(PartitionIndexTarget::new(
                    self.partition_key.clone(),
                    index.id(),
                ));
                delta_count += 1;
            }
        }

        for epoch_target in &touched_epochs {
            self.advance_epoch(epoch_target);
        }
        self.frontier = FrontierPosition::AppliedThrough(next_sequence);

        let partition_bytes_sum = touched_epochs.len() * self.partition_key.as_bytes().len();
        CorrelatedCommandWorkMetrics::check_dimensions(
            rows.len(),
            0,
            0,
            0,
            delta_count,
            touched_epochs.len(),
            0,
            partition_bytes_sum,
            0,
        )
    }

    /// Independently applies `ApplyIndexedMutations` to the model and returns
    /// the checked work metrics (`D = 38 * N`, `A = 20`, `V = 2 * N + 20`, `W = 40 * N + 40`).
    pub fn apply_indexed_mutations(
        &mut self,
        rows: &[CorrelatedIndexedMutationRow],
    ) -> Result<CorrelatedCommandWorkMetrics, CorrelatedWorkBudgetError> {
        if rows.is_empty() || rows.len() > 100 {
            return Err(CorrelatedWorkBudgetError::CollectionCountOutOfBounds {
                observed: rows.len(),
            });
        }
        let next_sequence = self.next_commit_sequence();
        let state_entity = Self::find_entity(&self.bundle, "IndexedState");
        let change_entity = Self::find_entity(&self.bundle, "MutationChange");
        let mut touched_epochs = BTreeSet::new();
        let mut delta_count = 0usize;

        for row in rows {
            assert_eq!(row.partition_id, self.partition_id);
            let (state_target, new_state_fields) = self.build_indexed_state_record(
                state_entity,
                row,
                row.next_active,
                row.expected_revision + 1,
            );
            let prior_record = self
                .live_entities
                .get(&state_target)
                .expect("IndexedState must exist before ApplyIndexedMutations")
                .clone();
            let next_version = prior_record
                .entity_version()
                .checked_next()
                .expect("next entity version");
            let updated_state = StoredEntityRecordV1::new(
                state_target.clone(),
                next_version,
                self.bundle.contract_version(),
                self.apply_schema_binding.clone(),
                new_state_fields.clone(),
            )
            .expect("valid updated StoredEntityRecordV1");
            self.candidate_entities.insert(state_target.clone());
            self.live_entities
                .insert(state_target.clone(), updated_state);

            for index in state_entity.indexes() {
                let old_values = encode_operational_index_values_v1(index, prior_record.fields())
                    .expect("encode old index values");
                let old_key = index
                    .key_schema()
                    .encode_index(&old_values, state_target.key().clone())
                    .expect("encode old index key");
                let old_prefix = index
                    .key_schema()
                    .encode_index_prefix(&old_values)
                    .expect("encode old index prefix")
                    .as_bytes()
                    .to_vec();
                let new_values = encode_operational_index_values_v1(index, &new_state_fields)
                    .expect("encode new index values");
                let new_covered = encode_operational_index_cover_v1(index, &new_state_fields)
                    .expect("encode new index cover");
                let new_key = index
                    .key_schema()
                    .encode_index(&new_values, state_target.key().clone())
                    .expect("encode new index key");
                let new_prefix = index
                    .key_schema()
                    .encode_index_prefix(&new_values)
                    .expect("encode new index prefix")
                    .as_bytes()
                    .to_vec();

                self.candidate_index_entries
                    .insert(old_key.clone(), (index.id(), old_prefix));
                self.candidate_index_entries
                    .insert(new_key.clone(), (index.id(), new_prefix.clone()));

                if old_key != new_key {
                    self.live_index_entries.remove(&old_key);
                    delta_count += 1;
                }
                let entry = StoredIndexEntryV2::new(
                    new_key.clone(),
                    self.apply_schema_binding.clone(),
                    new_covered,
                    self.partition_key.clone(),
                )
                .expect("valid updated StoredIndexEntryV2");
                self.live_index_entries
                    .insert(new_key, (index.id(), new_prefix, entry));
                delta_count += 1;
                touched_epochs.insert(PartitionIndexTarget::new(
                    self.partition_key.clone(),
                    index.id(),
                ));
            }

            let (change_target, change_fields) =
                self.build_mutation_change_record(change_entity, row);
            let change_record = StoredEntityRecordV1::new(
                change_target.clone(),
                EntityVersion::first(),
                self.bundle.contract_version(),
                self.apply_schema_binding.clone(),
                change_fields.clone(),
            )
            .expect("valid MutationChange StoredEntityRecordV1");
            self.candidate_entities.insert(change_target.clone());
            self.live_entities
                .insert(change_target.clone(), change_record);

            for index in change_entity.indexes() {
                let values = encode_operational_index_values_v1(index, &change_fields)
                    .expect("encode MutationChange index values");
                let covered = encode_operational_index_cover_v1(index, &change_fields)
                    .expect("encode MutationChange index cover");
                let key = index
                    .key_schema()
                    .encode_index(&values, change_target.key().clone())
                    .expect("encode MutationChange index key");
                let prefix_bytes = index
                    .key_schema()
                    .encode_index_prefix(&values)
                    .expect("encode MutationChange index prefix")
                    .as_bytes()
                    .to_vec();
                let entry = StoredIndexEntryV2::new(
                    key.clone(),
                    self.apply_schema_binding.clone(),
                    covered,
                    self.partition_key.clone(),
                )
                .expect("valid MutationChange StoredIndexEntryV2");
                self.candidate_index_entries
                    .insert(key.clone(), (index.id(), prefix_bytes.clone()));
                self.live_index_entries
                    .insert(key, (index.id(), prefix_bytes, entry));
                delta_count += 1;
                touched_epochs.insert(PartitionIndexTarget::new(
                    self.partition_key.clone(),
                    index.id(),
                ));
            }
        }

        for epoch_target in &touched_epochs {
            self.advance_epoch(epoch_target);
        }
        self.frontier = FrontierPosition::AppliedThrough(next_sequence);

        let partition_bytes_sum = touched_epochs.len() * self.partition_key.as_bytes().len();
        CorrelatedCommandWorkMetrics::check_dimensions(
            rows.len() * 2,
            0,
            0,
            0,
            delta_count,
            touched_epochs.len(),
            0,
            partition_bytes_sum,
            0,
        )
    }

    /// Returns the strictly ordered canonical target universe for this model
    /// combined with any engine-enumerated targets.
    pub fn comparison_universe(
        &self,
        engine_targets: &[CorrelatedTargetKey],
    ) -> Result<Vec<CorrelatedTargetKey>, CorrelatedModelDivergence> {
        let mut universe = BTreeSet::new();
        for target in &self.candidate_entities {
            universe.insert(CorrelatedTargetKey::Entity(target.clone()));
        }
        for (key, (index_id, prefix_bytes)) in &self.candidate_index_entries {
            universe.insert(CorrelatedTargetKey::IndexEntry {
                index_id: *index_id,
                prefix_bytes: prefix_bytes.clone(),
                key: key.clone(),
            });
        }
        for (target, prefix_bytes) in &self.declared_epoch_prefixes {
            universe.insert(CorrelatedTargetKey::IndexEpoch {
                target: target.clone(),
                prefix_bytes: prefix_bytes.clone(),
            });
        }

        for engine_target in engine_targets {
            if let CorrelatedTargetKey::RedbOnlyUndeclared { namespace, raw_key } = engine_target {
                return Err(CorrelatedModelDivergence::RedbOnlyUndeclaredTarget {
                    page_ordinal: 0,
                    namespace: *namespace,
                    raw_key: raw_key.clone(),
                });
            }
            if !universe.contains(engine_target) {
                return Err(CorrelatedModelDivergence::ExtraTarget {
                    page_ordinal: 0,
                    key: engine_target.clone(),
                });
            }
        }

        if universe.is_empty() || universe.len() > MAX_CORRELATED_INSPECTION_TOTAL_TARGETS {
            return Err(CorrelatedModelDivergence::InspectionFailure(
                "comparison universe target count out of bounds".to_owned(),
            ));
        }
        Ok(universe.into_iter().collect())
    }

    /// Builds the model's expected canonical 256-target inspection pages over `universe`.
    pub fn expected_pages(
        &self,
        universe: &[CorrelatedTargetKey],
    ) -> Result<Vec<CorrelatedInspectionPage>, CorrelatedModelDivergence> {
        if universe.is_empty() || universe.len() > MAX_CORRELATED_INSPECTION_TOTAL_TARGETS {
            return Err(CorrelatedModelDivergence::InspectionFailure(
                "comparison universe target count out of bounds".to_owned(),
            ));
        }
        let mut pages = Vec::new();
        for (ordinal, chunk) in universe.chunks(MAX_INSPECTION_TARGETS).enumerate() {
            if ordinal >= MAX_CORRELATED_INSPECTION_PAGES {
                return Err(CorrelatedModelDivergence::InspectionFailure(
                    "comparison universe exceeded MAX_CORRELATED_INSPECTION_PAGES".to_owned(),
                ));
            }
            let page_ordinal = u16::try_from(ordinal).expect("bounded page ordinal");
            let observations = chunk
                .iter()
                .map(|key| self.expected_observation(key))
                .collect::<Vec<_>>();
            let page = CorrelatedInspectionPage::new(page_ordinal, observations, None)
                .map_err(|e| CorrelatedModelDivergence::InspectionFailure(e.to_string()))?;
            pages.push(page);
        }
        Ok(pages)
    }

    fn expected_observation(
        &self,
        target_key: &CorrelatedTargetKey,
    ) -> CorrelatedTargetObservation {
        match target_key {
            CorrelatedTargetKey::Entity(target) => CorrelatedTargetObservation::Entity {
                target: target.clone(),
                record: self.live_entities.get(target).cloned(),
            },
            CorrelatedTargetKey::IndexEntry {
                index_id,
                prefix_bytes,
                key,
            } => CorrelatedTargetObservation::IndexEntry {
                index_id: *index_id,
                prefix_bytes: prefix_bytes.clone(),
                key: key.clone(),
                entry: self.live_index_entries.get(key).and_then(
                    |(stored_id, stored_prefix, entry)| {
                        (*stored_id == *index_id && stored_prefix == prefix_bytes)
                            .then(|| entry.clone())
                    },
                ),
            },
            CorrelatedTargetKey::IndexEpoch {
                target,
                prefix_bytes,
            } => CorrelatedTargetObservation::IndexEpoch {
                target: target.clone(),
                prefix_bytes: prefix_bytes.clone(),
                epoch: self
                    .index_epochs
                    .get(target)
                    .copied()
                    .unwrap_or(IndexEpochPosition::BeforeFirst),
            },
            CorrelatedTargetKey::RedbOnlyUndeclared { namespace, raw_key } => {
                CorrelatedTargetObservation::RedbOnlyUndeclared {
                    namespace: *namespace,
                    raw_key: raw_key.clone(),
                }
            }
        }
    }

    fn next_commit_sequence(&self) -> CommitSequence {
        match self.frontier {
            FrontierPosition::BeforeFirst => CommitSequence::first(),
            FrontierPosition::AppliedThrough(current) => {
                current.checked_next().expect("next commit sequence")
            }
        }
    }

    fn advance_epoch(&mut self, target: &PartitionIndexTarget) {
        let next = match self
            .index_epochs
            .get(target)
            .copied()
            .unwrap_or(IndexEpochPosition::BeforeFirst)
        {
            IndexEpochPosition::BeforeFirst => IndexEpochPosition::Value(IndexEpoch::first()),
            IndexEpochPosition::Value(epoch) => IndexEpochPosition::Value(
                epoch.checked_next().expect("next index generation epoch"),
            ),
        };
        self.index_epochs.insert(target.clone(), next);
    }

    fn build_indexed_state_record(
        &self,
        entity: &EntitySchema,
        row: &CorrelatedIndexedMutationRow,
        active: bool,
        revision: u64,
    ) -> (EntityTarget, CanonicalRecord) {
        let key_values = [
            Self::str_val(&row.partition_id),
            Self::str_val(&row.category),
            Self::str_val(&row.item_id),
            Self::str_val(&row.relation),
            Self::str_val(&row.subject),
        ];
        let key = entity
            .primary_key()
            .encode_entity(&key_values)
            .expect("encode IndexedState entity key");
        let target = EntityTarget::new(entity.id(), key).expect("valid IndexedState target");
        let record = Self::build_record_by_name(
            entity,
            &[
                ("partition_id", Self::str_val(&row.partition_id)),
                ("category", Self::str_val(&row.category)),
                ("item_id", Self::str_val(&row.item_id)),
                ("relation", Self::str_val(&row.relation)),
                ("subject", Self::str_val(&row.subject)),
                ("subject_category", Self::str_val(&row.subject_category)),
                ("subject_id", Self::str_val(&row.subject_id)),
                ("subject_relation", Self::str_val(&row.subject_relation)),
                ("subject_kind", Self::str_val(&row.subject_kind)),
                ("qualifier", Self::str_val(&row.qualifier)),
                (
                    "condition_context",
                    match &row.condition_context {
                        Some(bytes) => CanonicalValue::Bytes(
                            CanonicalBytes::new(bytes.clone()).expect("bounded condition_context"),
                        ),
                        None => CanonicalValue::Null,
                    },
                ),
                (
                    "inserted_at",
                    CanonicalValue::Timestamp(
                        Timestamp::new(row.inserted_at_seconds, 0).expect("valid Timestamp"),
                    ),
                ),
                ("active", CanonicalValue::Bool(active)),
                ("revision", CanonicalValue::U64(revision)),
            ],
        );
        (target, record)
    }

    fn build_mutation_change_record(
        &self,
        entity: &EntitySchema,
        row: &CorrelatedIndexedMutationRow,
    ) -> (EntityTarget, CanonicalRecord) {
        let key_values = [
            Self::str_val(&row.partition_id),
            Self::str_val(&row.change_id),
        ];
        let key = entity
            .primary_key()
            .encode_entity(&key_values)
            .expect("encode MutationChange entity key");
        let target = EntityTarget::new(entity.id(), key).expect("valid MutationChange target");
        let operation_enum = self
            .bundle
            .schema()
            .enums()
            .iter()
            .find(|e| e.name() == "MutationOperation")
            .expect("MutationOperation enum");
        let variant_id = operation_enum
            .variants()
            .iter()
            .find(|v| {
                v.name()
                    == if row.activate_operation {
                        "Activate"
                    } else {
                        "Deactivate"
                    }
            })
            .expect("MutationOperation variant")
            .id();
        let record = Self::build_record_by_name(
            entity,
            &[
                ("partition_id", Self::str_val(&row.partition_id)),
                ("change_id", Self::str_val(&row.change_id)),
                ("category", Self::str_val(&row.category)),
                ("item_id", Self::str_val(&row.item_id)),
                ("relation", Self::str_val(&row.relation)),
                ("subject", Self::str_val(&row.subject)),
                (
                    "operation",
                    CanonicalValue::Enum {
                        type_id: operation_enum.id(),
                        variant_id,
                    },
                ),
                ("qualifier", Self::str_val(&row.qualifier)),
                (
                    "condition_context",
                    match &row.condition_context {
                        Some(bytes) => CanonicalValue::Bytes(
                            CanonicalBytes::new(bytes.clone()).expect("bounded condition_context"),
                        ),
                        None => CanonicalValue::Null,
                    },
                ),
                (
                    "inserted_at",
                    CanonicalValue::Timestamp(
                        Timestamp::new(row.inserted_at_seconds, 0).expect("valid Timestamp"),
                    ),
                ),
            ],
        );
        (target, record)
    }

    fn find_entity<'a>(bundle: &'a ContractBundle, name: &str) -> &'a EntitySchema {
        bundle
            .schema()
            .entities()
            .iter()
            .find(|entity| entity.name() == name)
            .expect("declared entity in IndexedMutationBulk")
    }

    fn build_record_by_name(
        entity: &EntitySchema,
        named_fields: &[(&str, CanonicalValue)],
    ) -> CanonicalRecord {
        let mut pairs: Vec<(FieldId, CanonicalValue)> = named_fields
            .iter()
            .map(|(name, value)| {
                let field_id = entity
                    .record()
                    .fields()
                    .iter()
                    .find(|field| field.name() == *name)
                    .unwrap_or_else(|| panic!("missing field {name} on {}", entity.name()))
                    .id();
                (field_id, value.clone())
            })
            .collect();
        pairs.sort_by_key(|(id, _)| *id);
        CanonicalRecord::new(pairs).expect("canonical entity record")
    }

    fn str_val(value: &str) -> CanonicalValue {
        CanonicalValue::String(
            CanonicalString::new(value.to_owned()).expect("valid CanonicalString"),
        )
    }
    /// Returns the strictly ordered set of canonical targets that must have been
    /// sealed by the coordinator across all committed commands in this model.
    #[must_use]
    pub fn expected_sealed_targets(&self) -> BTreeSet<CorrelatedTargetKey> {
        let mut sealed = BTreeSet::new();
        for target in &self.candidate_entities {
            sealed.insert(CorrelatedTargetKey::Entity(target.clone()));
        }
        for (key, (index_id, prefix_bytes)) in &self.candidate_index_entries {
            sealed.insert(CorrelatedTargetKey::IndexEntry {
                index_id: *index_id,
                prefix_bytes: prefix_bytes.clone(),
                key: key.clone(),
            });
        }
        for (target, epoch) in &self.index_epochs {
            if *epoch != IndexEpochPosition::BeforeFirst
                && let Some(prefix_bytes) = self.declared_epoch_prefixes.get(target)
            {
                sealed.insert(CorrelatedTargetKey::IndexEpoch {
                    target: target.clone(),
                    prefix_bytes: prefix_bytes.clone(),
                });
            }
        }
        sealed
    }
}

/// Verifies that a stopped redb inspection session matches `model` across every
/// canonical 256-target page and that the coordinator's sealed target set
/// equals the model's expected sealed target set.
pub fn verify_correlated_index_work_against_redb(
    model: &CorrelatedIndexWorkModel,
    session: &CorrelatedNamespaceInspectionSession,
) -> Result<CorrelatedStoreAgreement, CorrelatedModelDivergence> {
    if session.recovered_application_frontier() != model.expected_frontier() {
        return Err(CorrelatedModelDivergence::FrontierMismatch {
            expected: model.expected_frontier(),
            observed: session.recovered_application_frontier(),
        });
    }
    let expected_sealed = model.expected_sealed_targets();
    for key in &expected_sealed {
        if !session.sealed_targets().contains(key) {
            return Err(CorrelatedModelDivergence::MissingTarget {
                page_ordinal: 0,
                key: key.clone(),
            });
        }
    }
    for key in session.sealed_targets() {
        if !expected_sealed.contains(key) {
            return Err(CorrelatedModelDivergence::ExtraTarget {
                page_ordinal: 0,
                key: key.clone(),
            });
        }
    }
    let engine_targets = session
        .engine_enumerated_targets()
        .map_err(|e| CorrelatedModelDivergence::InspectionFailure(e.to_string()))?;
    let universe = model.comparison_universe(&engine_targets)?;
    let expected_pages = model.expected_pages(&universe)?;

    let mut observed_pages = Vec::with_capacity(expected_pages.len());
    let mut continuation: Option<CorrelatedInspectionContinuation> = None;
    loop {
        let page = session
            .read_page(&universe, continuation.as_ref())
            .map_err(|e| CorrelatedModelDivergence::InspectionFailure(e.to_string()))?;
        let next = page.next().cloned();
        observed_pages.push(page);
        if observed_pages.len() > MAX_CORRELATED_INSPECTION_PAGES {
            return Err(CorrelatedModelDivergence::InspectionFailure(
                "observed page stream exceeded MAX_CORRELATED_INSPECTION_PAGES".to_owned(),
            ));
        }
        match next {
            Some(token) => continuation = Some(token),
            None => break,
        }
    }

    compare_correlated_target_pages(&expected_pages, &observed_pages)
}

/// Compares expected and observed canonical 256-target inspection pages,
/// rejecting missing targets, extra targets, duplicate targets, adjacent
/// reorders, wrong target identity/prefix bytes, redb-only undeclared targets,
/// and wrong generation or post-state values.
pub fn compare_correlated_target_pages(
    expected_pages: &[CorrelatedInspectionPage],
    observed_pages: &[CorrelatedInspectionPage],
) -> Result<CorrelatedStoreAgreement, CorrelatedModelDivergence> {
    if expected_pages.len() != observed_pages.len() {
        return Err(CorrelatedModelDivergence::PageCountMismatch {
            expected: expected_pages.len(),
            observed: observed_pages.len(),
        });
    }

    let mut agreement = CorrelatedStoreAgreement {
        page_count: observed_pages.len(),
        ..CorrelatedStoreAgreement::default()
    };
    let mut previous_observed_key: Option<CorrelatedTargetKey> = None;

    for (expected_page, observed_page) in expected_pages.iter().zip(observed_pages) {
        let page_ordinal = expected_page.page_ordinal();
        if observed_page.page_ordinal() != page_ordinal {
            return Err(CorrelatedModelDivergence::InspectionFailure(
                "page ordinal mismatch".to_owned(),
            ));
        }

        let expected_obs = expected_page.observations();
        let observed_obs = observed_page.observations();

        // First scan observed observations for undeclared rows, duplicate keys, or adjacent reorders.
        for obs in observed_obs {
            let key = obs.key();
            if let CorrelatedTargetKey::RedbOnlyUndeclared { namespace, raw_key } = key {
                return Err(CorrelatedModelDivergence::RedbOnlyUndeclaredTarget {
                    page_ordinal,
                    namespace,
                    raw_key,
                });
            }
            if let Some(prev) = &previous_observed_key {
                if &key == prev {
                    return Err(CorrelatedModelDivergence::DuplicateTarget { page_ordinal, key });
                }
                if &key < prev {
                    return Err(CorrelatedModelDivergence::AdjacentReorder {
                        page_ordinal,
                        left: prev.clone(),
                        right: key,
                    });
                }
            }
            previous_observed_key = Some(key);
        }

        if expected_obs.len() != observed_obs.len() {
            if observed_obs.len() < expected_obs.len() {
                let observed_keys = observed_obs
                    .iter()
                    .map(CorrelatedTargetObservation::key)
                    .collect::<BTreeSet<_>>();
                for exp in expected_obs {
                    let key = exp.key();
                    if !observed_keys.contains(&key) {
                        return Err(CorrelatedModelDivergence::MissingTarget { page_ordinal, key });
                    }
                }
            } else {
                let expected_keys = expected_obs
                    .iter()
                    .map(CorrelatedTargetObservation::key)
                    .collect::<BTreeSet<_>>();
                for obs in observed_obs {
                    let key = obs.key();
                    if !expected_keys.contains(&key) {
                        return Err(CorrelatedModelDivergence::ExtraTarget { page_ordinal, key });
                    }
                }
            }
        }

        for (exp, obs) in expected_obs.iter().zip(observed_obs) {
            let exp_key = exp.key();
            let obs_key = obs.key();
            if exp_key != obs_key {
                return Err(CorrelatedModelDivergence::WrongTargetIdentityOrPrefix {
                    page_ordinal,
                    expected: exp_key,
                    observed: obs_key,
                });
            }
            if exp != obs {
                return Err(CorrelatedModelDivergence::WrongGenerationOrPostState {
                    page_ordinal,
                    key: exp_key,
                });
            }
            agreement.total_targets += 1;
            match obs {
                CorrelatedTargetObservation::Entity {
                    record: Some(_), ..
                } => agreement.live_entities += 1,
                CorrelatedTargetObservation::Entity { record: None, .. } => {
                    agreement.absent_entities += 1;
                }
                CorrelatedTargetObservation::IndexEntry { entry: Some(_), .. } => {
                    agreement.live_index_entries += 1;
                }
                CorrelatedTargetObservation::IndexEntry { entry: None, .. } => {
                    agreement.absent_index_entries += 1;
                }
                CorrelatedTargetObservation::IndexEpoch {
                    epoch: IndexEpochPosition::BeforeFirst,
                    ..
                } => agreement.before_first_epochs += 1,
                CorrelatedTargetObservation::IndexEpoch {
                    epoch: IndexEpochPosition::Value(_),
                    ..
                } => agreement.advanced_epochs += 1,
                CorrelatedTargetObservation::RedbOnlyUndeclared { .. } => {}
            }
        }
    }

    Ok(agreement)
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_storage_api::AuthoritativeNamespaceV1;

    // req: BLK-024, BLK-025, BLK-027, TXN-030, TXN-031
    #[test]
    fn correlated_index_work_model_rejects_target_divergence() {
        verify_work_accounting_boundary_witnesses()
            .expect("all exact and +1 work-accounting boundary witnesses hold");

        let mut model = CorrelatedIndexWorkModel::new("part-neg-0001");
        let rows =
            CorrelatedIndexedMutationRow::deterministic_batch("part-neg-0001", 19, true, false, 1);
        let seed_metrics = model
            .apply_seed_initial_rows(&rows)
            .expect("19-row seed fits work budgets");
        assert_eq!(seed_metrics.binding_targets(), 19);
        assert_eq!(seed_metrics.delta_count(), 19 * 18);
        assert_eq!(seed_metrics.affected_epoch_target_count(), 18);

        let mutate_metrics = model
            .apply_indexed_mutations(&rows)
            .expect("19-row mutation fits work budgets");
        assert_eq!(mutate_metrics.binding_targets(), 38);
        assert_eq!(mutate_metrics.delta_count(), 19 * 38);
        assert_eq!(mutate_metrics.affected_epoch_target_count(), 20);
        assert_eq!(mutate_metrics.validation_positions(), 19 * 2 + 20);
        assert_eq!(mutate_metrics.work_units(), 19 * 40 + 40);

        let universe = model
            .comparison_universe(&[])
            .expect("canonical comparison universe");
        assert_eq!(universe.len(), 38 + 19 * 38 + 20);
        let expected_pages = model
            .expected_pages(&universe)
            .expect("expected 256-target pages");
        assert_eq!(expected_pages.len(), 4);

        let agreement = compare_correlated_target_pages(&expected_pages, &expected_pages)
            .expect("unmodified pages agree");
        assert_eq!(agreement.page_count(), 4);
        assert_eq!(agreement.total_targets(), 780);
        assert_eq!(agreement.live_entities(), 38);
        assert_eq!(agreement.live_index_entries(), 19 * 20);
        assert_eq!(agreement.absent_index_entries(), 19 * 18);
        assert_eq!(agreement.advanced_epochs(), 20);

        // 1. Missing target on page 0 -> rejected with MissingTarget.
        {
            let mut mutated = expected_pages.clone();
            let mut obs = mutated[0].observations().to_vec();
            let removed_key = obs.remove(5).key();
            mutated[0] = mutated[0].with_observations_for_divergence_test(obs);
            assert_eq!(
                compare_correlated_target_pages(&expected_pages, &mutated),
                Err(CorrelatedModelDivergence::MissingTarget {
                    page_ordinal: 0,
                    key: removed_key,
                })
            );
        }

        // 2. Extra target on page 0 -> rejected with ExtraTarget.
        {
            let mut extra_model = model.clone();
            let extra_row =
                CorrelatedIndexedMutationRow::deterministic("part-neg-0001", 999, true, false, 1);
            extra_model.register_uncommitted_candidates(&[extra_row]);
            let extra_universe = extra_model
                .comparison_universe(&[])
                .expect("extra universe");
            let extra_key = extra_universe
                .into_iter()
                .find(|k| universe.binary_search(k).is_err())
                .expect("extra target key");
            let extra_obs = extra_model.expected_observation(&extra_key);

            let mut mutated = expected_pages.clone();
            let mut obs = mutated[0].observations().to_vec();
            obs.push(extra_obs);
            obs.sort_by_key(CorrelatedTargetObservation::key);
            let inserted_key = extra_key;
            mutated[0] = mutated[0].with_observations_for_divergence_test(obs);
            assert_eq!(
                compare_correlated_target_pages(&expected_pages, &mutated),
                Err(CorrelatedModelDivergence::ExtraTarget {
                    page_ordinal: 0,
                    key: inserted_key,
                })
            );
        }

        // 3. Duplicate target -> rejected with DuplicateTarget.
        {
            let mut mutated = expected_pages.clone();
            let mut obs = mutated[1].observations().to_vec();
            let dup_key = obs[10].key();
            obs[11] = obs[10].clone();
            mutated[1] = mutated[1].with_observations_for_divergence_test(obs);
            assert_eq!(
                compare_correlated_target_pages(&expected_pages, &mutated),
                Err(CorrelatedModelDivergence::DuplicateTarget {
                    page_ordinal: 1,
                    key: dup_key,
                })
            );
        }

        // 4. Adjacent reorder -> rejected with AdjacentReorder.
        {
            let mut mutated = expected_pages.clone();
            let mut obs = mutated[1].observations().to_vec();
            let left_after_swap = obs[21].key();
            let right_after_swap = obs[20].key();
            obs.swap(20, 21);
            mutated[1] = mutated[1].with_observations_for_divergence_test(obs);
            assert_eq!(
                compare_correlated_target_pages(&expected_pages, &mutated),
                Err(CorrelatedModelDivergence::AdjacentReorder {
                    page_ordinal: 1,
                    left: left_after_swap,
                    right: right_after_swap,
                })
            );
        }

        // 5. Wrong target identity or prefix byte -> rejected with WrongTargetIdentityOrPrefix.
        {
            let mut mutated = expected_pages.clone();
            let last_page_idx = mutated.len() - 1;
            let mut obs = mutated[last_page_idx].observations().to_vec();
            let epoch_pos = obs
                .iter()
                .position(|item| matches!(item, CorrelatedTargetObservation::IndexEpoch { .. }))
                .expect("epoch target on last page");
            let expected_key = obs[epoch_pos].key();
            let CorrelatedTargetObservation::IndexEpoch {
                target,
                mut prefix_bytes,
                epoch,
            } = obs[epoch_pos].clone()
            else {
                unreachable!()
            };
            prefix_bytes.push(0xff);
            obs[epoch_pos] = CorrelatedTargetObservation::IndexEpoch {
                target,
                prefix_bytes,
                epoch,
            };
            let observed_key = obs[epoch_pos].key();
            mutated[last_page_idx] =
                mutated[last_page_idx].with_observations_for_divergence_test(obs);
            assert_eq!(
                compare_correlated_target_pages(&expected_pages, &mutated),
                Err(CorrelatedModelDivergence::WrongTargetIdentityOrPrefix {
                    page_ordinal: u16::try_from(last_page_idx).expect("page ordinal"),
                    expected: expected_key,
                    observed: observed_key,
                })
            );
        }

        // 6. Redb-only undeclared target -> rejected with RedbOnlyUndeclaredTarget.
        {
            let undeclared_raw = vec![0x49, 0x01, 0xde, 0xad, 0xbe, 0xef];
            assert_eq!(
                model.comparison_universe(&[CorrelatedTargetKey::RedbOnlyUndeclared {
                    namespace: AuthoritativeNamespaceV1::SecondaryIndexes,
                    raw_key: undeclared_raw.clone(),
                }]),
                Err(CorrelatedModelDivergence::RedbOnlyUndeclaredTarget {
                    page_ordinal: 0,
                    namespace: AuthoritativeNamespaceV1::SecondaryIndexes,
                    raw_key: undeclared_raw.clone(),
                })
            );

            let mut mutated = expected_pages.clone();
            let mut obs = mutated[0].observations().to_vec();
            obs[0] = CorrelatedTargetObservation::RedbOnlyUndeclared {
                namespace: AuthoritativeNamespaceV1::SecondaryIndexes,
                raw_key: undeclared_raw.clone(),
            };
            mutated[0] = mutated[0].with_observations_for_divergence_test(obs);
            assert_eq!(
                compare_correlated_target_pages(&expected_pages, &mutated),
                Err(CorrelatedModelDivergence::RedbOnlyUndeclaredTarget {
                    page_ordinal: 0,
                    namespace: AuthoritativeNamespaceV1::SecondaryIndexes,
                    raw_key: undeclared_raw,
                })
            );
        }

        // 7. Wrong generation or post-state value -> rejected with WrongGenerationOrPostState.
        {
            let mut mutated = expected_pages.clone();
            let last_page_idx = mutated.len() - 1;
            let mut obs = mutated[last_page_idx].observations().to_vec();
            let epoch_pos = obs
                .iter()
                .position(|item| matches!(item, CorrelatedTargetObservation::IndexEpoch { .. }))
                .expect("epoch target on last page");
            let epoch_key = obs[epoch_pos].key();
            let CorrelatedTargetObservation::IndexEpoch {
                target,
                prefix_bytes,
                ..
            } = obs[epoch_pos].clone()
            else {
                unreachable!()
            };
            obs[epoch_pos] = CorrelatedTargetObservation::IndexEpoch {
                target,
                prefix_bytes,
                epoch: IndexEpochPosition::Value(
                    IndexEpoch::new(99).expect("wrong generation epoch"),
                ),
            };
            mutated[last_page_idx] =
                mutated[last_page_idx].with_observations_for_divergence_test(obs);
            assert_eq!(
                compare_correlated_target_pages(&expected_pages, &mutated),
                Err(CorrelatedModelDivergence::WrongGenerationOrPostState {
                    page_ordinal: u16::try_from(last_page_idx).expect("page ordinal"),
                    key: epoch_key,
                })
            );
        }
    }
}
