//! Compiler-declared incremental analytical provider structures (ADR-0162, WP-714).
//!
//! Provides the closed descriptor registry and rebuildable partition state for:
//! 1. Segment-local equality and missing/null/present bitmaps,
//! 2. Retraction-aware exact mergeable rollups (`count`, `count_present`, `sum`,
//!    `mean`, `any`, `all`, and bounded counted `min`, `max`, `count_distinct`,
//!    `count_distinct_present`), and
//! 3. Maintained bounded top-K lanes with repair reserve and exact entity-key
//!    tie-breaker.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::num::{NonZeroU16, NonZeroU32};

use riffdb_types::{
    ApplicationRoleHash, CanonicalRecord, CanonicalValue, CommitSequence, EntityKey, FieldId,
    PartitionKeyHash, ProjectionGeneration, ProjectionProviderDescriptorHash, QueryPlanHash,
};

use crate::result_set_epoch::{ProviderEpochObservationV1, ProviderLifecycleV1};

/// Maximum incremental analytical structures declared per projection definition.
pub const MAX_INCREMENTAL_STRUCTURES_PER_PROJECTION_V1: usize = 8;

/// Maximum rollup groups per incremental rollup structure.
pub const MAX_INCREMENTAL_ROLLUP_GROUPS_V1: u16 = 4_096;

/// Maximum distinct values tracked per bitmap or counted-set rollup group.
pub const MAX_INCREMENTAL_DISTINCT_VALUES_V1: u16 = 4_096;

/// Maximum combined `k + repair_reserve` entries for a maintained top-K lane.
pub const MAX_INCREMENTAL_TOP_K_TOTAL_V1: u16 = 1_024;

/// Maximum state bytes across all incremental structures in one partition.
pub const MAX_INCREMENTAL_PARTITION_STATE_BYTES_V1: u32 = 4 * 1024 * 1024;

/// Maximum aggregate write-amplification factor per source row transition.
pub const MAX_INCREMENTAL_WRITE_AMPLIFICATION_V1: u16 = 16;

/// Schema hash identifying `IncrementalPartitionIndexV1` state under ADR-0124/ADR-0162.
pub const INCREMENTAL_ANALYTICAL_STATE_SCHEMA_HASH_V1: [u8; 32] = [0x62; 32];

/// Ordered wrapper around `CanonicalValue` for deterministic `BTreeMap` keys in incremental structures.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncrementalValueKeyV1(pub CanonicalValue);

impl Ord for IncrementalValueKeyV1 {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_canonical_values(&self.0, &other.0)
    }
}

impl PartialOrd for IncrementalValueKeyV1 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Non-zero identifier of a compiler-declared incremental analytical structure.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct IncrementalStructureIdV1(NonZeroU16);

impl IncrementalStructureIdV1 {
    /// Creates a checked non-zero incremental structure identifier.
    pub const fn new(value: u16) -> Option<Self> {
        match NonZeroU16::new(value) {
            Some(nz) => Some(Self(nz)),
            None => None,
        }
    }

    /// Returns the numeric structure identifier.
    #[must_use]
    pub const fn get(self) -> u16 {
        self.0.get()
    }
}

/// Closed sort direction for a maintained top-K lane.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IncrementalSortDirectionV1 {
    /// Ascending value order.
    Ascending,
    /// Descending value order.
    Descending,
}

/// Compiler-declared placement for missing/null values in a maintained top-K lane.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IncrementalNoValuePlacementV1 {
    /// Missing/null values sort before present values.
    First,
    /// Missing/null values sort after present values.
    Last,
}

/// Closed exact rollup function family (ADR-0152, ADR-0162 Decision 2.2, OQ-044..050).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IncrementalRollupFunctionV1 {
    /// Total admitted rows in the group (`count()`).
    Count,
    /// Non-`NoValue` rows for `field_id` (`count_present(field)`).
    CountPresent {
        /// Target field.
        field_id: FieldId,
    },
    /// Checked exact 64-bit integer sum (`sum(field)`).
    SumI64 {
        /// Target integer field.
        field_id: FieldId,
    },
    /// Exact mean state `{ total, count }` without division or float (`mean(field)`, OQ-047).
    MeanI64 {
        /// Target integer field.
        field_id: FieldId,
    },
    /// Boolean disjunction over required boolean field (`any(field)`, empty identity `false`, OQ-048).
    AnyBool {
        /// Target boolean field.
        field_id: FieldId,
    },
    /// Boolean conjunction over required boolean field (`all(field)`, empty identity `true`, OQ-048).
    AllBool {
        /// Target boolean field.
        field_id: FieldId,
    },
    /// Exact minimum over bounded counted ordered multiset (`min(field)`).
    MinI64 {
        /// Target integer field.
        field_id: FieldId,
    },
    /// Exact maximum over bounded counted ordered multiset (`max(field)`).
    MaxI64 {
        /// Target integer field.
        field_id: FieldId,
    },
    /// Exact distinct canonical values including `NoValue` as one class (`count_distinct(field)`, OQ-046).
    CountDistinct {
        /// Target field.
        field_id: FieldId,
    },
    /// Exact distinct present canonical values excluding `NoValue` (`count_distinct_present(field)`, OQ-046).
    CountDistinctPresent {
        /// Target field.
        field_id: FieldId,
    },
}

/// Closed incremental structure kind admitted by ADR-0162 Decision 2.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IncrementalStructureKindV1 {
    /// Segment-local equality and missing/null/present bitmaps over a low-cardinality field.
    SegmentEqualityPresenceBitmap {
        /// Indexed field.
        field_id: FieldId,
        /// Maximum distinct present values before degradation.
        max_distinct_values: NonZeroU16,
        /// Maximum estimated bitmap bytes before degradation.
        max_bitmap_bytes: NonZeroU32,
    },
    /// Retraction-aware exact mergeable rollup keyed by fixed group fields.
    ExactMergeableRollup {
        /// Canonical ordered group-by fields (empty for whole-partition rollup).
        group_fields: Vec<FieldId>,
        /// Exact rollup function.
        function: IncrementalRollupFunctionV1,
        /// Maximum distinct groups.
        max_groups: NonZeroU16,
        /// Maximum distinct values or multiset entries per group.
        max_distinct_per_group: NonZeroU16,
        /// Maximum state bytes for this rollup.
        max_state_bytes: NonZeroU32,
    },
    /// Maintained bounded top-K lane with repair reserve and entity-key tie-breaker.
    MaintainedTopKLane {
        /// Ordered value field.
        value_field: FieldId,
        /// Sort direction for present values.
        direction: IncrementalSortDirectionV1,
        /// Placement for missing or null values.
        no_value_placement: IncrementalNoValuePlacementV1,
        /// Declared top-K limit.
        k: NonZeroU16,
        /// Bounded repair reserve beyond `k` to absorb retractions exactly.
        repair_reserve: NonZeroU16,
        /// Maximum state bytes for this top-K lane.
        max_state_bytes: NonZeroU32,
    },
}

impl IncrementalStructureKindV1 {
    /// Write amplification units charged per source row mutation.
    #[must_use]
    pub const fn write_amplification_units(&self) -> u16 {
        match self {
            Self::SegmentEqualityPresenceBitmap { .. } => 1,
            Self::ExactMergeableRollup { .. } | Self::MaintainedTopKLane { .. } => 2,
        }
    }

    /// Declared state byte ceiling for this structure.
    #[must_use]
    pub const fn max_state_bytes(&self) -> u32 {
        match self {
            Self::SegmentEqualityPresenceBitmap {
                max_bitmap_bytes, ..
            } => max_bitmap_bytes.get(),
            Self::ExactMergeableRollup {
                max_state_bytes, ..
            }
            | Self::MaintainedTopKLane {
                max_state_bytes, ..
            } => max_state_bytes.get(),
        }
    }
}

/// Policy alignment bound into the incremental structure identity (ADR-0162 Decision 4, OQ-021).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IncrementalPolicyAlignmentV1 {
    /// Structure is valid for a compiler-proved role-invariant public row universe.
    RoleInvariantPublicUniverse,
    /// Structure is partitioned for one exact policy shape, capability revision, and predicate.
    PolicyAlignedSubpartition {
        /// Bound application role hash.
        role_hash: ApplicationRoleHash,
        /// Bound capability revision.
        capability_revision: u64,
        /// Required boolean/value policy field that must equal `required_value` for row admission.
        policy_field: FieldId,
        /// Required field value for row admission into this policy-aligned structure.
        required_value: CanonicalValue,
    },
}

impl IncrementalPolicyAlignmentV1 {
    /// Returns `true` if `record` is admitted into this structure's policy partition.
    #[must_use]
    pub fn admits_record(&self, record: &CanonicalRecord) -> bool {
        match self {
            Self::RoleInvariantPublicUniverse => true,
            Self::PolicyAlignedSubpartition {
                policy_field,
                required_value,
                ..
            } => record_field(record, *policy_field) == Some(required_value),
        }
    }

    /// Checks whether a query caller's role and capability revision match this structure.
    pub fn authorize_caller(
        &self,
        caller_role: ApplicationRoleHash,
        caller_capability_revision: u64,
    ) -> Result<(), IncrementalProviderErrorV1> {
        match self {
            Self::RoleInvariantPublicUniverse => Ok(()),
            Self::PolicyAlignedSubpartition {
                role_hash,
                capability_revision,
                ..
            } => {
                if *role_hash != caller_role || *capability_revision != caller_capability_revision {
                    return Err(IncrementalProviderErrorV1::PolicyAlignmentMismatch);
                }
                Ok(())
            }
        }
    }
}

/// Compiler-declared descriptor for one maintained analytical structure (ADR-0162 Decision 1).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncrementalStructureDescriptorV1 {
    structure_id: IncrementalStructureIdV1,
    descriptor_hash: ProjectionProviderDescriptorHash,
    consuming_plan_hash: QueryPlanHash,
    policy_alignment: IncrementalPolicyAlignmentV1,
    kind: IncrementalStructureKindV1,
}

impl IncrementalStructureDescriptorV1 {
    /// Validates and constructs one closed incremental-structure descriptor.
    pub fn new(
        structure_id: IncrementalStructureIdV1,
        descriptor_hash: ProjectionProviderDescriptorHash,
        consuming_plan_hash: QueryPlanHash,
        policy_alignment: IncrementalPolicyAlignmentV1,
        kind: IncrementalStructureKindV1,
    ) -> Result<Self, IncrementalProviderErrorV1> {
        validate_structure_kind(&kind)?;
        Ok(Self {
            structure_id,
            descriptor_hash,
            consuming_plan_hash,
            policy_alignment,
            kind,
        })
    }

    /// Returns the structure identifier.
    #[must_use]
    pub const fn structure_id(&self) -> IncrementalStructureIdV1 {
        self.structure_id
    }

    /// Returns the provider descriptor digest.
    #[must_use]
    pub const fn descriptor_hash(&self) -> ProjectionProviderDescriptorHash {
        self.descriptor_hash
    }

    /// Returns the permitted consuming query plan hash.
    #[must_use]
    pub const fn consuming_plan_hash(&self) -> QueryPlanHash {
        self.consuming_plan_hash
    }

    /// Returns the policy alignment of this structure.
    #[must_use]
    pub const fn policy_alignment(&self) -> &IncrementalPolicyAlignmentV1 {
        &self.policy_alignment
    }

    /// Returns the structure kind and bounds.
    #[must_use]
    pub const fn kind(&self) -> &IncrementalStructureKindV1 {
        &self.kind
    }
}

fn validate_structure_kind(
    kind: &IncrementalStructureKindV1,
) -> Result<(), IncrementalProviderErrorV1> {
    match kind {
        IncrementalStructureKindV1::SegmentEqualityPresenceBitmap {
            max_distinct_values,
            max_bitmap_bytes,
            ..
        } => {
            if max_distinct_values.get() > MAX_INCREMENTAL_DISTINCT_VALUES_V1
                || max_bitmap_bytes.get() > MAX_INCREMENTAL_PARTITION_STATE_BYTES_V1
            {
                return Err(IncrementalProviderErrorV1::ExcessiveStructureBounds);
            }
        }
        IncrementalStructureKindV1::ExactMergeableRollup {
            group_fields,
            max_groups,
            max_distinct_per_group,
            max_state_bytes,
            ..
        } => {
            if group_fields.len() > 8 {
                return Err(IncrementalProviderErrorV1::ExcessiveStructureBounds);
            }
            let mut seen = BTreeSet::new();
            for field in group_fields {
                if !seen.insert(*field) {
                    return Err(IncrementalProviderErrorV1::DuplicateGroupField(*field));
                }
            }
            if max_groups.get() > MAX_INCREMENTAL_ROLLUP_GROUPS_V1
                || max_distinct_per_group.get() > MAX_INCREMENTAL_DISTINCT_VALUES_V1
                || max_state_bytes.get() > MAX_INCREMENTAL_PARTITION_STATE_BYTES_V1
            {
                return Err(IncrementalProviderErrorV1::ExcessiveStructureBounds);
            }
        }
        IncrementalStructureKindV1::MaintainedTopKLane {
            k,
            repair_reserve,
            max_state_bytes,
            ..
        } => {
            let total = k
                .get()
                .checked_add(repair_reserve.get())
                .ok_or(IncrementalProviderErrorV1::ExcessiveStructureBounds)?;
            if total > MAX_INCREMENTAL_TOP_K_TOTAL_V1
                || max_state_bytes.get() > MAX_INCREMENTAL_PARTITION_STATE_BYTES_V1
            {
                return Err(IncrementalProviderErrorV1::ExcessiveStructureBounds);
            }
        }
    }
    Ok(())
}

/// Closed registry of incremental analytical structures declared for one projection (ADR-0162 Decision 1 & 5).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncrementalProjectionRegistryV1 {
    descriptors: BTreeMap<IncrementalStructureIdV1, IncrementalStructureDescriptorV1>,
    total_write_amplification: u16,
    total_max_state_bytes: u32,
}

impl IncrementalProjectionRegistryV1 {
    /// Validates and seals a finite registry of incremental structures for one projection.
    pub fn new(
        descriptors: Vec<IncrementalStructureDescriptorV1>,
    ) -> Result<Self, IncrementalProviderErrorV1> {
        if descriptors.is_empty()
            || descriptors.len() > MAX_INCREMENTAL_STRUCTURES_PER_PROJECTION_V1
        {
            return Err(IncrementalProviderErrorV1::InvalidStructureCount(
                descriptors.len(),
            ));
        }
        let mut map = BTreeMap::new();
        let mut total_amp: u16 = 0;
        let mut total_bytes: u32 = 0;
        for descriptor in descriptors {
            let id = descriptor.structure_id();
            total_amp = total_amp
                .checked_add(descriptor.kind().write_amplification_units())
                .ok_or(IncrementalProviderErrorV1::ExcessiveWriteAmplification)?;
            if total_amp > MAX_INCREMENTAL_WRITE_AMPLIFICATION_V1 {
                return Err(IncrementalProviderErrorV1::ExcessiveWriteAmplification);
            }
            total_bytes = total_bytes
                .checked_add(descriptor.kind().max_state_bytes())
                .ok_or(IncrementalProviderErrorV1::ExcessiveStructureBounds)?;
            if total_bytes > MAX_INCREMENTAL_PARTITION_STATE_BYTES_V1 {
                return Err(IncrementalProviderErrorV1::ExcessiveStructureBounds);
            }
            if map.insert(id, descriptor).is_some() {
                return Err(IncrementalProviderErrorV1::DuplicateStructureId(id));
            }
        }
        Ok(Self {
            descriptors: map,
            total_write_amplification: total_amp,
            total_max_state_bytes: total_bytes,
        })
    }

    /// Returns the ordered descriptors in this registry.
    #[must_use]
    pub const fn descriptors(
        &self,
    ) -> &BTreeMap<IncrementalStructureIdV1, IncrementalStructureDescriptorV1> {
        &self.descriptors
    }

    /// Returns the total write-amplification units across all declared structures.
    #[must_use]
    pub const fn total_write_amplification(&self) -> u16 {
        self.total_write_amplification
    }

    /// Returns the total declared state byte ceiling across all structures.
    #[must_use]
    pub const fn total_max_state_bytes(&self) -> u32 {
        self.total_max_state_bytes
    }

    /// Explicitly refuses any implicit cross-provider bitmap or row-ID bridge (ADR-0162 Decision 2, OQ-023).
    pub fn verify_single_structure_plan(
        &self,
        structure_id: IncrementalStructureIdV1,
        plan_hash: QueryPlanHash,
        bridged_structure: Option<IncrementalStructureIdV1>,
    ) -> Result<&IncrementalStructureDescriptorV1, IncrementalProviderErrorV1> {
        if bridged_structure.is_some() {
            return Err(IncrementalProviderErrorV1::ImplicitCrossProviderBridgeForbidden);
        }
        let descriptor = self
            .descriptors
            .get(&structure_id)
            .ok_or(IncrementalProviderErrorV1::UnknownStructureId(structure_id))?;
        if descriptor.consuming_plan_hash() != plan_hash {
            return Err(IncrementalProviderErrorV1::PlanIdentityMismatch);
        }
        Ok(descriptor)
    }
}

/// Exact mean value `{ total, count }` without division or floating-point rounding (OQ-047).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExactMeanV1 {
    /// Exact checked sum of contributing values.
    pub total: i128,
    /// Number of contributing non-`NoValue` rows.
    pub count: u64,
}

/// Closed exact output value of an incremental rollup group.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IncrementalRollupValueV1 {
    /// Count of rows (`Count`, `CountPresent`, `CountDistinct`, `CountDistinctPresent`).
    Count(u64),
    /// Optional checked 64-bit integer sum (`None` when group has zero present contributors).
    SumI64(Option<i64>),
    /// Exact mean `{ total, count }` (`count == 0` when group has zero present contributors).
    MeanI64(ExactMeanV1),
    /// Boolean aggregate result (`false` empty identity for `AnyBool`, `true` for `AllBool`).
    Bool(bool),
    /// Exact extremum (`None` when group has zero present contributors).
    ExtremumI64(Option<i64>),
}

/// Closed predicate family over a segment-local equality/presence bitmap.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IncrementalBitmapPredicateV1 {
    /// Field is missing from the record.
    IsMissing,
    /// Field is present with `CanonicalValue::Null`.
    IsNull,
    /// Field is present with a non-null value.
    IsPresent,
    /// Field equals the given non-null canonical value.
    Equals(CanonicalValue),
}

/// Reason a maintained incremental structure degraded to `ProviderLifecycleV1::Rebuilding`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IncrementalDegradedReasonV1 {
    /// Bitmap distinct-value cardinality exceeded its declared ceiling.
    BitmapCardinalityExceeded,
    /// State byte consumption exceeded the structure's declared byte bound.
    StateByteBoundExceeded,
    /// Rollup group count exceeded its declared ceiling.
    RollupGroupBoundExceeded,
    /// Rollup distinct/multiset cardinality per group exceeded its declared ceiling.
    RollupDistinctBoundExceeded,
    /// Checked integer addition overflowed during rollup maintenance.
    RollupArithmeticOverflow,
    /// Retractions exhausted the top-K repair reserve while older unseen rows existed.
    TopKReserveExhausted,
    /// Policy alignment or capability revision changed and retired the epoch.
    PolicyEpochRetired,
}

/// Typed errors returned by incremental analytical provider registration, apply, or query execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IncrementalProviderErrorV1 {
    /// Registry must declare between 1 and `MAX_INCREMENTAL_STRUCTURES_PER_PROJECTION_V1` structures.
    InvalidStructureCount(usize),
    /// Duplicate structure ID in one registry.
    DuplicateStructureId(IncrementalStructureIdV1),
    /// Duplicate field in rollup group-by list.
    DuplicateGroupField(FieldId),
    /// Declared bounds exceed static ceilings.
    ExcessiveStructureBounds,
    /// Sum of structure write-amplification units exceeds `MAX_INCREMENTAL_WRITE_AMPLIFICATION_V1`.
    ExcessiveWriteAmplification,
    /// Implicit cross-provider bitmap or row-ID bridge is forbidden (OQ-023).
    ImplicitCrossProviderBridgeForbidden,
    /// Unknown structure ID requested.
    UnknownStructureId(IncrementalStructureIdV1),
    /// Query plan hash does not match the structure's bound consuming plan.
    PlanIdentityMismatch,
    /// Caller role or capability revision does not match a policy-aligned structure.
    PolicyAlignmentMismatch,
    /// Requested structure kind does not match the query operation.
    StructureKindMismatch,
    /// Field value type in a row is incompatible with the declared rollup/top-K function.
    IncompatibleFieldType(FieldId),
    /// Commit sequence is below the current visible frontier (`PRJ-001`).
    FrontierRegression {
        /// Current frontier.
        current: CommitSequence,
        /// Attempted commit sequence.
        attempted: CommitSequence,
    },
    /// Duplicate commit sequence presented with a different `apply_hash` (`PRJ-003`).
    ApplyHashMismatch {
        /// Commit sequence.
        sequence: CommitSequence,
    },
    /// Provider generation is rebuilding or retired and cannot serve normal success.
    ProviderUnavailable {
        /// Current lifecycle state.
        lifecycle: ProviderLifecycleV1,
        /// Underlying degradation reason, if any.
        reason: Option<IncrementalDegradedReasonV1>,
    },
    /// Requested top-K limit exceeds the structure's declared `k`.
    RequestedLimitExceedsDeclaredK {
        /// Requested limit.
        requested: u16,
        /// Declared `k`.
        declared_k: u16,
    },
}

impl fmt::Display for IncrementalProviderErrorV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl Error for IncrementalProviderErrorV1 {}

/// One source row mutation fed into `IncrementalPartitionIndexV1`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IncrementalRowMutationV1 {
    /// Insert or replace `key` with `post_image`.
    Upsert {
        /// Entity primary key.
        key: EntityKey,
        /// Canonical post-image record.
        post_image: CanonicalRecord,
    },
    /// Delete/retract `key` from the partition.
    Delete {
        /// Entity primary key.
        key: EntityKey,
    },
}

// ---------------------------------------------------------------------------
// 1. Segment-local equality and missing/null/present bitmap state
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Eq, PartialEq)]
struct SegmentBitmapStateV1 {
    field_id: FieldId,
    max_distinct_values: u16,
    max_bitmap_bytes: u32,
    missing_keys: BTreeSet<EntityKey>,
    null_keys: BTreeSet<EntityKey>,
    present_keys: BTreeSet<EntityKey>,
    equality_bitmaps: BTreeMap<IncrementalValueKeyV1, BTreeSet<EntityKey>>,
}

impl SegmentBitmapStateV1 {
    fn new(
        field_id: FieldId,
        max_distinct_values: NonZeroU16,
        max_bitmap_bytes: NonZeroU32,
    ) -> Self {
        Self {
            field_id,
            max_distinct_values: max_distinct_values.get(),
            max_bitmap_bytes: max_bitmap_bytes.get(),
            missing_keys: BTreeSet::new(),
            null_keys: BTreeSet::new(),
            present_keys: BTreeSet::new(),
            equality_bitmaps: BTreeMap::new(),
        }
    }

    fn remove_row(&mut self, key: &EntityKey, old_record: &CanonicalRecord) {
        match record_field(old_record, self.field_id) {
            None => {
                self.missing_keys.remove(key);
            }
            Some(CanonicalValue::Null) => {
                self.null_keys.remove(key);
            }
            Some(val) => {
                self.present_keys.remove(key);
                let k = IncrementalValueKeyV1(val.clone());
                if let Some(set) = self.equality_bitmaps.get_mut(&k) {
                    set.remove(key);
                    if set.is_empty() {
                        self.equality_bitmaps.remove(&k);
                    }
                }
            }
        }
    }

    fn insert_row(
        &mut self,
        key: EntityKey,
        new_record: &CanonicalRecord,
    ) -> Result<(), IncrementalDegradedReasonV1> {
        match record_field(new_record, self.field_id) {
            None => {
                self.missing_keys.insert(key);
            }
            Some(CanonicalValue::Null) => {
                self.null_keys.insert(key);
            }
            Some(val) => {
                let k = IncrementalValueKeyV1(val.clone());
                if !self.equality_bitmaps.contains_key(&k)
                    && self.equality_bitmaps.len() >= usize::from(self.max_distinct_values)
                {
                    return Err(IncrementalDegradedReasonV1::BitmapCardinalityExceeded);
                }
                self.present_keys.insert(key.clone());
                self.equality_bitmaps.entry(k).or_default().insert(key);
            }
        }
        if self.estimated_bytes() > self.max_bitmap_bytes {
            return Err(IncrementalDegradedReasonV1::StateByteBoundExceeded);
        }
        Ok(())
    }

    fn estimated_bytes(&self) -> u32 {
        let key_entries = self.missing_keys.len()
            + self.null_keys.len()
            + self.present_keys.len()
            + self
                .equality_bitmaps
                .values()
                .map(BTreeSet::len)
                .sum::<usize>();
        u32::try_from(key_entries.saturating_mul(32)).unwrap_or(u32::MAX)
    }

    fn evaluate(&self, predicates: &[IncrementalBitmapPredicateV1]) -> BTreeSet<EntityKey> {
        let mut iter = predicates.iter();
        let Some(first) = iter.next() else {
            return BTreeSet::new();
        };
        let mut current = self.matching_set(first);
        for pred in iter {
            let next = self.matching_set(pred);
            current.retain(|k| next.contains(k));
        }
        current
    }

    fn matching_set(&self, predicate: &IncrementalBitmapPredicateV1) -> BTreeSet<EntityKey> {
        match predicate {
            IncrementalBitmapPredicateV1::IsMissing => self.missing_keys.clone(),
            IncrementalBitmapPredicateV1::IsNull => self.null_keys.clone(),
            IncrementalBitmapPredicateV1::IsPresent => self.present_keys.clone(),
            IncrementalBitmapPredicateV1::Equals(val) => self
                .equality_bitmaps
                .get(&IncrementalValueKeyV1(val.clone()))
                .cloned()
                .unwrap_or_default(),
        }
    }
}

// ---------------------------------------------------------------------------
// 2. Retraction-aware exact mergeable rollup state
// ---------------------------------------------------------------------------

/// Canonical group key for an incremental rollup (empty `Vec` for ungrouped whole-partition rollup).
pub type IncrementalGroupKeyV1 = Vec<Option<IncrementalValueKeyV1>>;

#[derive(Clone, Debug, Eq, PartialEq)]
enum GroupAccumulatorV1 {
    Count(u64),
    CountPresent(u64),
    SumI64 {
        sum: i64,
        present_count: u64,
    },
    MeanI64 {
        total: i128,
        count: u64,
    },
    AnyBool {
        true_count: u64,
        total_count: u64,
    },
    AllBool {
        false_count: u64,
        total_count: u64,
    },
    ExtremumI64 {
        is_max: bool,
        multiset: BTreeMap<i64, u64>,
    },
    CountDistinct {
        include_no_value: bool,
        no_value_count: u64,
        counts: BTreeMap<IncrementalValueKeyV1, u64>,
    },
}

impl GroupAccumulatorV1 {
    fn new(function: IncrementalRollupFunctionV1) -> Self {
        match function {
            IncrementalRollupFunctionV1::Count => Self::Count(0),
            IncrementalRollupFunctionV1::CountPresent { .. } => Self::CountPresent(0),
            IncrementalRollupFunctionV1::SumI64 { .. } => Self::SumI64 {
                sum: 0,
                present_count: 0,
            },
            IncrementalRollupFunctionV1::MeanI64 { .. } => Self::MeanI64 { total: 0, count: 0 },
            IncrementalRollupFunctionV1::AnyBool { .. } => Self::AnyBool {
                true_count: 0,
                total_count: 0,
            },
            IncrementalRollupFunctionV1::AllBool { .. } => Self::AllBool {
                false_count: 0,
                total_count: 0,
            },
            IncrementalRollupFunctionV1::MinI64 { .. } => Self::ExtremumI64 {
                is_max: false,
                multiset: BTreeMap::new(),
            },
            IncrementalRollupFunctionV1::MaxI64 { .. } => Self::ExtremumI64 {
                is_max: true,
                multiset: BTreeMap::new(),
            },
            IncrementalRollupFunctionV1::CountDistinct { .. } => Self::CountDistinct {
                include_no_value: true,
                no_value_count: 0,
                counts: BTreeMap::new(),
            },
            IncrementalRollupFunctionV1::CountDistinctPresent { .. } => Self::CountDistinct {
                include_no_value: false,
                no_value_count: 0,
                counts: BTreeMap::new(),
            },
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Self::Count(c) | Self::CountPresent(c) => *c == 0,
            Self::SumI64 { present_count, .. } => *present_count == 0,
            Self::MeanI64 { count, .. } => *count == 0,
            Self::AnyBool { total_count, .. } | Self::AllBool { total_count, .. } => {
                *total_count == 0
            }
            Self::ExtremumI64 { multiset, .. } => multiset.is_empty(),
            Self::CountDistinct {
                no_value_count,
                counts,
                ..
            } => *no_value_count == 0 && counts.is_empty(),
        }
    }

    fn apply_row(
        &mut self,
        function: IncrementalRollupFunctionV1,
        record: &CanonicalRecord,
        max_distinct: u16,
        is_insert: bool,
    ) -> Result<(), Result<IncrementalDegradedReasonV1, IncrementalProviderErrorV1>> {
        match (self, function) {
            (Self::Count(c), IncrementalRollupFunctionV1::Count) => {
                *c = if is_insert {
                    c.checked_add(1)
                        .ok_or(Ok(IncrementalDegradedReasonV1::RollupArithmeticOverflow))?
                } else {
                    c.saturating_sub(1)
                };
                Ok(())
            }
            (Self::CountPresent(c), IncrementalRollupFunctionV1::CountPresent { field_id }) => {
                if matches!(record_field(record, field_id), Some(v) if *v != CanonicalValue::Null) {
                    *c = if is_insert {
                        c.checked_add(1)
                            .ok_or(Ok(IncrementalDegradedReasonV1::RollupArithmeticOverflow))?
                    } else {
                        c.saturating_sub(1)
                    };
                }
                Ok(())
            }
            (
                Self::SumI64 { sum, present_count },
                IncrementalRollupFunctionV1::SumI64 { field_id },
            ) => {
                if let Some(v) = extract_optional_i64(record, field_id).map_err(Err)? {
                    if is_insert {
                        *sum = sum
                            .checked_add(v)
                            .ok_or(Ok(IncrementalDegradedReasonV1::RollupArithmeticOverflow))?;
                        *present_count = present_count
                            .checked_add(1)
                            .ok_or(Ok(IncrementalDegradedReasonV1::RollupArithmeticOverflow))?;
                    } else {
                        *sum = sum
                            .checked_sub(v)
                            .ok_or(Ok(IncrementalDegradedReasonV1::RollupArithmeticOverflow))?;
                        *present_count = present_count.saturating_sub(1);
                    }
                }
                Ok(())
            }
            (Self::MeanI64 { total, count }, IncrementalRollupFunctionV1::MeanI64 { field_id }) => {
                if let Some(v) = extract_optional_i64(record, field_id).map_err(Err)? {
                    if is_insert {
                        *total = total
                            .checked_add(i128::from(v))
                            .ok_or(Ok(IncrementalDegradedReasonV1::RollupArithmeticOverflow))?;
                        *count = count
                            .checked_add(1)
                            .ok_or(Ok(IncrementalDegradedReasonV1::RollupArithmeticOverflow))?;
                    } else {
                        *total = total
                            .checked_sub(i128::from(v))
                            .ok_or(Ok(IncrementalDegradedReasonV1::RollupArithmeticOverflow))?;
                        *count = count.saturating_sub(1);
                    }
                }
                Ok(())
            }
            (
                Self::AnyBool {
                    true_count,
                    total_count,
                },
                IncrementalRollupFunctionV1::AnyBool { field_id },
            ) => {
                let b = extract_required_bool(record, field_id).map_err(Err)?;
                if is_insert {
                    *total_count = total_count
                        .checked_add(1)
                        .ok_or(Ok(IncrementalDegradedReasonV1::RollupArithmeticOverflow))?;
                    if b {
                        *true_count = true_count
                            .checked_add(1)
                            .ok_or(Ok(IncrementalDegradedReasonV1::RollupArithmeticOverflow))?;
                    }
                } else {
                    *total_count = total_count.saturating_sub(1);
                    if b {
                        *true_count = true_count.saturating_sub(1);
                    }
                }
                Ok(())
            }
            (
                Self::AllBool {
                    false_count,
                    total_count,
                },
                IncrementalRollupFunctionV1::AllBool { field_id },
            ) => {
                let b = extract_required_bool(record, field_id).map_err(Err)?;
                if is_insert {
                    *total_count = total_count
                        .checked_add(1)
                        .ok_or(Ok(IncrementalDegradedReasonV1::RollupArithmeticOverflow))?;
                    if !b {
                        *false_count = false_count
                            .checked_add(1)
                            .ok_or(Ok(IncrementalDegradedReasonV1::RollupArithmeticOverflow))?;
                    }
                } else {
                    *total_count = total_count.saturating_sub(1);
                    if !b {
                        *false_count = false_count.saturating_sub(1);
                    }
                }
                Ok(())
            }
            (
                Self::ExtremumI64 { multiset, .. },
                IncrementalRollupFunctionV1::MinI64 { field_id }
                | IncrementalRollupFunctionV1::MaxI64 { field_id },
            ) => {
                if let Some(v) = extract_optional_i64(record, field_id).map_err(Err)? {
                    if is_insert {
                        if !multiset.contains_key(&v) && multiset.len() >= usize::from(max_distinct)
                        {
                            return Err(Ok(
                                IncrementalDegradedReasonV1::RollupDistinctBoundExceeded,
                            ));
                        }
                        let entry = multiset.entry(v).or_insert(0);
                        *entry = entry
                            .checked_add(1)
                            .ok_or(Ok(IncrementalDegradedReasonV1::RollupArithmeticOverflow))?;
                    } else if let Some(entry) = multiset.get_mut(&v) {
                        if *entry <= 1 {
                            multiset.remove(&v);
                        } else {
                            *entry -= 1;
                        }
                    }
                }
                Ok(())
            }
            (
                Self::CountDistinct {
                    no_value_count,
                    counts,
                    ..
                },
                IncrementalRollupFunctionV1::CountDistinct { field_id }
                | IncrementalRollupFunctionV1::CountDistinctPresent { field_id },
            ) => {
                match record_field(record, field_id) {
                    None | Some(CanonicalValue::Null) => {
                        if is_insert {
                            *no_value_count = no_value_count
                                .checked_add(1)
                                .ok_or(Ok(IncrementalDegradedReasonV1::RollupArithmeticOverflow))?;
                        } else {
                            *no_value_count = no_value_count.saturating_sub(1);
                        }
                    }
                    Some(val) => {
                        let k = IncrementalValueKeyV1(val.clone());
                        if is_insert {
                            if !counts.contains_key(&k) && counts.len() >= usize::from(max_distinct)
                            {
                                return Err(Ok(
                                    IncrementalDegradedReasonV1::RollupDistinctBoundExceeded,
                                ));
                            }
                            let entry = counts.entry(k).or_insert(0);
                            *entry = entry
                                .checked_add(1)
                                .ok_or(Ok(IncrementalDegradedReasonV1::RollupArithmeticOverflow))?;
                        } else if let Some(entry) = counts.get_mut(&k) {
                            if *entry <= 1 {
                                counts.remove(&k);
                            } else {
                                *entry -= 1;
                            }
                        }
                    }
                }
                Ok(())
            }
            _ => Err(Err(IncrementalProviderErrorV1::StructureKindMismatch)),
        }
    }

    fn value(&self) -> IncrementalRollupValueV1 {
        match self {
            Self::Count(c) | Self::CountPresent(c) => IncrementalRollupValueV1::Count(*c),
            Self::SumI64 { sum, present_count } => {
                if *present_count == 0 {
                    IncrementalRollupValueV1::SumI64(None)
                } else {
                    IncrementalRollupValueV1::SumI64(Some(*sum))
                }
            }
            Self::MeanI64 { total, count } => IncrementalRollupValueV1::MeanI64(ExactMeanV1 {
                total: *total,
                count: *count,
            }),
            Self::AnyBool { true_count, .. } => IncrementalRollupValueV1::Bool(*true_count > 0),
            Self::AllBool { false_count, .. } => IncrementalRollupValueV1::Bool(*false_count == 0),
            Self::ExtremumI64 { is_max, multiset } => {
                let extremum = if *is_max {
                    multiset.keys().next_back().copied()
                } else {
                    multiset.keys().next().copied()
                };
                IncrementalRollupValueV1::ExtremumI64(extremum)
            }
            Self::CountDistinct {
                include_no_value,
                no_value_count,
                counts,
            } => {
                let base = counts.len() as u64;
                let extra = u64::from(*include_no_value && *no_value_count > 0);
                IncrementalRollupValueV1::Count(base + extra)
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ExactRollupStateV1 {
    group_fields: Vec<FieldId>,
    function: IncrementalRollupFunctionV1,
    max_groups: u16,
    max_distinct_per_group: u16,
    max_state_bytes: u32,
    groups: BTreeMap<IncrementalGroupKeyV1, GroupAccumulatorV1>,
}

impl ExactRollupStateV1 {
    fn new(
        group_fields: Vec<FieldId>,
        function: IncrementalRollupFunctionV1,
        max_groups: NonZeroU16,
        max_distinct_per_group: NonZeroU16,
        max_state_bytes: NonZeroU32,
    ) -> Self {
        Self {
            group_fields,
            function,
            max_groups: max_groups.get(),
            max_distinct_per_group: max_distinct_per_group.get(),
            max_state_bytes: max_state_bytes.get(),
            groups: BTreeMap::new(),
        }
    }

    fn group_key(&self, record: &CanonicalRecord) -> IncrementalGroupKeyV1 {
        self.group_fields
            .iter()
            .map(|field_id| {
                record_field(record, *field_id)
                    .cloned()
                    .map(IncrementalValueKeyV1)
            })
            .collect()
    }

    fn apply_row(
        &mut self,
        record: &CanonicalRecord,
        is_insert: bool,
    ) -> Result<(), Result<IncrementalDegradedReasonV1, IncrementalProviderErrorV1>> {
        let key = self.group_key(record);
        if is_insert
            && !self.groups.contains_key(&key)
            && self.groups.len() >= usize::from(self.max_groups)
        {
            return Err(Ok(IncrementalDegradedReasonV1::RollupGroupBoundExceeded));
        }
        let acc = self
            .groups
            .entry(key.clone())
            .or_insert_with(|| GroupAccumulatorV1::new(self.function));
        acc.apply_row(
            self.function,
            record,
            self.max_distinct_per_group,
            is_insert,
        )?;
        if !is_insert && acc.is_empty() {
            self.groups.remove(&key);
        }
        if self.estimated_bytes() > self.max_state_bytes {
            return Err(Ok(IncrementalDegradedReasonV1::StateByteBoundExceeded));
        }
        Ok(())
    }

    fn estimated_bytes(&self) -> u32 {
        let base = self.groups.len().saturating_mul(64);
        u32::try_from(base).unwrap_or(u32::MAX)
    }

    fn snapshot_values(&self) -> BTreeMap<IncrementalGroupKeyV1, IncrementalRollupValueV1> {
        if self.group_fields.is_empty() && self.groups.is_empty() {
            let empty_acc = GroupAccumulatorV1::new(self.function);
            let mut out = BTreeMap::new();
            out.insert(Vec::new(), empty_acc.value());
            return out;
        }
        self.groups
            .iter()
            .map(|(k, v)| (k.clone(), v.value()))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// 3. Maintained bounded top-K lane with repair reserve
// ---------------------------------------------------------------------------

/// One ordered entry returned from a maintained top-K lane.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncrementalTopKEntryV1 {
    /// Entity primary key.
    pub key: EntityKey,
    /// Ordered field value (`None` when missing or `CanonicalValue::Null`).
    pub order_value: Option<CanonicalValue>,
    /// Canonical projected record.
    pub record: CanonicalRecord,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TopKSortKeyV1 {
    value: Option<CanonicalValue>,
    direction: IncrementalSortDirectionV1,
    no_value_placement: IncrementalNoValuePlacementV1,
    entity_key: EntityKey,
}

impl Ord for TopKSortKeyV1 {
    fn cmp(&self, other: &Self) -> Ordering {
        let val_cmp = match (&self.value, &other.value) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => match self.no_value_placement {
                IncrementalNoValuePlacementV1::First => Ordering::Less,
                IncrementalNoValuePlacementV1::Last => Ordering::Greater,
            },
            (Some(_), None) => match self.no_value_placement {
                IncrementalNoValuePlacementV1::First => Ordering::Greater,
                IncrementalNoValuePlacementV1::Last => Ordering::Less,
            },
            (Some(left), Some(right)) => {
                let raw = compare_canonical_values(left, right);
                match self.direction {
                    IncrementalSortDirectionV1::Ascending => raw,
                    IncrementalSortDirectionV1::Descending => raw.reverse(),
                }
            }
        };
        val_cmp.then_with(|| self.entity_key.cmp(&other.entity_key))
    }
}

impl PartialOrd for TopKSortKeyV1 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MaintainedTopKStateV1 {
    value_field: FieldId,
    direction: IncrementalSortDirectionV1,
    no_value_placement: IncrementalNoValuePlacementV1,
    k: u16,
    repair_reserve: u16,
    max_state_bytes: u32,
    entries: BTreeMap<TopKSortKeyV1, CanonicalRecord>,
    total_admitted_population: u64,
    evicted_beyond_reserve: bool,
}

impl MaintainedTopKStateV1 {
    fn new(
        value_field: FieldId,
        direction: IncrementalSortDirectionV1,
        no_value_placement: IncrementalNoValuePlacementV1,
        k: NonZeroU16,
        repair_reserve: NonZeroU16,
        max_state_bytes: NonZeroU32,
    ) -> Self {
        Self {
            value_field,
            direction,
            no_value_placement,
            k: k.get(),
            repair_reserve: repair_reserve.get(),
            max_state_bytes: max_state_bytes.get(),
            entries: BTreeMap::new(),
            total_admitted_population: 0,
            evicted_beyond_reserve: false,
        }
    }

    fn capacity(&self) -> usize {
        usize::from(self.k) + usize::from(self.repair_reserve)
    }

    fn sort_key(&self, key: EntityKey, record: &CanonicalRecord) -> TopKSortKeyV1 {
        let value = match record_field(record, self.value_field) {
            None | Some(CanonicalValue::Null) => None,
            Some(v) => Some(v.clone()),
        };
        TopKSortKeyV1 {
            value,
            direction: self.direction,
            no_value_placement: self.no_value_placement,
            entity_key: key,
        }
    }

    fn remove_row(
        &mut self,
        key: &EntityKey,
        old_record: &CanonicalRecord,
    ) -> Result<(), IncrementalDegradedReasonV1> {
        let sk = self.sort_key(key.clone(), old_record);
        self.total_admitted_population = self.total_admitted_population.saturating_sub(1);
        self.entries.remove(&sk);
        if self.evicted_beyond_reserve
            && self.entries.len() < usize::from(self.k)
            && self.total_admitted_population >= u64::from(self.k)
        {
            return Err(IncrementalDegradedReasonV1::TopKReserveExhausted);
        }
        Ok(())
    }

    fn insert_row(
        &mut self,
        key: EntityKey,
        new_record: &CanonicalRecord,
    ) -> Result<(), IncrementalDegradedReasonV1> {
        let sk = self.sort_key(key, new_record);
        self.total_admitted_population = self.total_admitted_population.saturating_add(1);
        self.entries.insert(sk, new_record.clone());
        if self.entries.len() > self.capacity()
            && let Some(last_key) = self.entries.keys().next_back().cloned()
        {
            self.entries.remove(&last_key);
            self.evicted_beyond_reserve = true;
        }
        let estimated = u32::try_from(self.entries.len().saturating_mul(96)).unwrap_or(u32::MAX);
        if estimated > self.max_state_bytes {
            return Err(IncrementalDegradedReasonV1::StateByteBoundExceeded);
        }
        Ok(())
    }

    fn top_k(
        &self,
        limit: NonZeroU16,
    ) -> Result<Vec<IncrementalTopKEntryV1>, IncrementalProviderErrorV1> {
        if limit.get() > self.k {
            return Err(IncrementalProviderErrorV1::RequestedLimitExceedsDeclaredK {
                requested: limit.get(),
                declared_k: self.k,
            });
        }
        Ok(self
            .entries
            .iter()
            .take(usize::from(limit.get()))
            .map(|(sk, rec)| IncrementalTopKEntryV1 {
                key: sk.entity_key.clone(),
                order_value: sk.value.clone(),
                record: rec.clone(),
            })
            .collect())
    }
}

// ---------------------------------------------------------------------------
// 4. Partition-scoped atomic incremental state and independent evaluators
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Eq, PartialEq)]
enum StructureInstanceV1 {
    Bitmap(SegmentBitmapStateV1),
    Rollup(ExactRollupStateV1),
    TopK(MaintainedTopKStateV1),
}

impl StructureInstanceV1 {
    fn from_kind(kind: &IncrementalStructureKindV1) -> Self {
        match kind {
            IncrementalStructureKindV1::SegmentEqualityPresenceBitmap {
                field_id,
                max_distinct_values,
                max_bitmap_bytes,
            } => Self::Bitmap(SegmentBitmapStateV1::new(
                *field_id,
                *max_distinct_values,
                *max_bitmap_bytes,
            )),
            IncrementalStructureKindV1::ExactMergeableRollup {
                group_fields,
                function,
                max_groups,
                max_distinct_per_group,
                max_state_bytes,
            } => Self::Rollup(ExactRollupStateV1::new(
                group_fields.clone(),
                *function,
                *max_groups,
                *max_distinct_per_group,
                *max_state_bytes,
            )),
            IncrementalStructureKindV1::MaintainedTopKLane {
                value_field,
                direction,
                no_value_placement,
                k,
                repair_reserve,
                max_state_bytes,
            } => Self::TopK(MaintainedTopKStateV1::new(
                *value_field,
                *direction,
                *no_value_placement,
                *k,
                *repair_reserve,
                *max_state_bytes,
            )),
        }
    }
}

/// Partition-scoped rebuildable incremental analytical provider state (ADR-0162 Decisions 1-6).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IncrementalPartitionIndexV1 {
    partition: PartitionKeyHash,
    history_incarnation: u64,
    generation: ProjectionGeneration,
    registry: IncrementalProjectionRegistryV1,
    rows: BTreeMap<EntityKey, CanonicalRecord>,
    structures: BTreeMap<IncrementalStructureIdV1, StructureInstanceV1>,
    applied_commits: BTreeMap<CommitSequence, [u8; 32]>,
    floor_sequence: Option<CommitSequence>,
    visible_frontier: Option<CommitSequence>,
    lifecycle: ProviderLifecycleV1,
    degraded_reason: Option<IncrementalDegradedReasonV1>,
}

impl IncrementalPartitionIndexV1 {
    /// Creates an empty ready partition index for the given registry and generation.
    #[must_use]
    pub fn new(
        partition: PartitionKeyHash,
        history_incarnation: u64,
        generation: ProjectionGeneration,
        registry: IncrementalProjectionRegistryV1,
    ) -> Self {
        let structures = registry
            .descriptors()
            .iter()
            .map(|(id, desc)| (*id, StructureInstanceV1::from_kind(desc.kind())))
            .collect();
        Self {
            partition,
            history_incarnation,
            generation,
            registry,
            rows: BTreeMap::new(),
            structures,
            applied_commits: BTreeMap::new(),
            floor_sequence: None,
            visible_frontier: None,
            lifecycle: ProviderLifecycleV1::Ready,
            degraded_reason: None,
        }
    }

    /// Returns the partition key hash.
    #[must_use]
    pub const fn partition(&self) -> PartitionKeyHash {
        self.partition
    }

    /// Returns the history incarnation.
    #[must_use]
    pub const fn history_incarnation(&self) -> u64 {
        self.history_incarnation
    }

    /// Returns the projection generation.
    #[must_use]
    pub const fn generation(&self) -> ProjectionGeneration {
        self.generation
    }

    /// Returns the visible frontier, if at least one commit has been published.
    #[must_use]
    pub const fn visible_frontier(&self) -> Option<CommitSequence> {
        self.visible_frontier
    }

    /// Returns the current provider lifecycle state.
    #[must_use]
    pub const fn lifecycle(&self) -> ProviderLifecycleV1 {
        self.lifecycle
    }

    /// Returns the degradation reason when `lifecycle != ProviderLifecycleV1::Ready`.
    #[must_use]
    pub const fn degraded_reason(&self) -> Option<IncrementalDegradedReasonV1> {
        self.degraded_reason
    }

    /// Publishes an ADR-0130 `ProviderEpochObservationV1` for one declared structure (`OQ-020`).
    pub fn epoch_observation(
        &self,
        structure_id: IncrementalStructureIdV1,
    ) -> Result<ProviderEpochObservationV1, IncrementalProviderErrorV1> {
        let descriptor = self
            .registry
            .descriptors()
            .get(&structure_id)
            .ok_or(IncrementalProviderErrorV1::UnknownStructureId(structure_id))?;
        let floor = self.floor_sequence.unwrap_or_else(CommitSequence::first);
        let ceiling = self.visible_frontier.unwrap_or(floor);
        ProviderEpochObservationV1::new(
            descriptor.descriptor_hash(),
            INCREMENTAL_ANALYTICAL_STATE_SCHEMA_HASH_V1,
            self.history_incarnation,
            self.generation,
            floor,
            ceiling,
            self.lifecycle,
        )
        .map_err(|_| IncrementalProviderErrorV1::ProviderUnavailable {
            lifecycle: self.lifecycle,
            reason: self.degraded_reason,
        })
    }

    /// Applies one commit's row transitions atomically to a private candidate snapshot before
    /// advancing the visible frontier (ADR-0162 Decision 3, `PRJ-001`, `PRJ-002`, `PRJ-003`).
    pub fn apply_commit(
        &mut self,
        sequence: CommitSequence,
        apply_hash: [u8; 32],
        mutations: &[IncrementalRowMutationV1],
    ) -> Result<(), IncrementalProviderErrorV1> {
        if let Some(existing_hash) = self.applied_commits.get(&sequence) {
            if *existing_hash == apply_hash {
                return Ok(());
            }
            return Err(IncrementalProviderErrorV1::ApplyHashMismatch { sequence });
        }
        if let Some(frontier) = self.visible_frontier
            && sequence < frontier
        {
            return Err(IncrementalProviderErrorV1::FrontierRegression {
                current: frontier,
                attempted: sequence,
            });
        }
        if self.lifecycle != ProviderLifecycleV1::Ready {
            return Err(IncrementalProviderErrorV1::ProviderUnavailable {
                lifecycle: self.lifecycle,
                reason: self.degraded_reason,
            });
        }

        let mut candidate_rows = self.rows.clone();
        let mut candidate_structures = self.structures.clone();

        for mutation in mutations {
            match mutation {
                IncrementalRowMutationV1::Upsert { key, post_image } => {
                    let prior = candidate_rows.insert(key.clone(), post_image.clone());
                    for (sid, desc) in self.registry.descriptors() {
                        let Some(instance) = candidate_structures.get_mut(sid) else {
                            continue;
                        };
                        if let Some(old_rec) = &prior
                            && desc.policy_alignment().admits_record(old_rec)
                            && let Err(reason) =
                                retract_from_instance(instance, desc.kind(), key, old_rec)?
                        {
                            self.lifecycle = ProviderLifecycleV1::Rebuilding;
                            self.degraded_reason = Some(reason);
                            return Err(IncrementalProviderErrorV1::ProviderUnavailable {
                                lifecycle: self.lifecycle,
                                reason: self.degraded_reason,
                            });
                        }
                        if desc.policy_alignment().admits_record(post_image)
                            && let Err(reason) = insert_into_instance(
                                instance,
                                desc.kind(),
                                key.clone(),
                                post_image,
                            )?
                        {
                            self.lifecycle = ProviderLifecycleV1::Rebuilding;
                            self.degraded_reason = Some(reason);
                            return Err(IncrementalProviderErrorV1::ProviderUnavailable {
                                lifecycle: self.lifecycle,
                                reason: self.degraded_reason,
                            });
                        }
                    }
                }
                IncrementalRowMutationV1::Delete { key } => {
                    if let Some(old_rec) = candidate_rows.remove(key) {
                        for (sid, desc) in self.registry.descriptors() {
                            let Some(instance) = candidate_structures.get_mut(sid) else {
                                continue;
                            };
                            if desc.policy_alignment().admits_record(&old_rec)
                                && let Err(reason) =
                                    retract_from_instance(instance, desc.kind(), key, &old_rec)?
                            {
                                self.lifecycle = ProviderLifecycleV1::Rebuilding;
                                self.degraded_reason = Some(reason);
                                return Err(IncrementalProviderErrorV1::ProviderUnavailable {
                                    lifecycle: self.lifecycle,
                                    reason: self.degraded_reason,
                                });
                            }
                        }
                    }
                }
            }
        }

        self.rows = candidate_rows;
        self.structures = candidate_structures;
        self.applied_commits.insert(sequence, apply_hash);
        if self.floor_sequence.is_none() {
            self.floor_sequence = Some(sequence);
        }
        self.visible_frontier = Some(sequence);
        Ok(())
    }

    /// Rebuilds the complete partition state from an authoritative row snapshot (`PRJ-004`).
    pub fn rebuild_from_rows(
        partition: PartitionKeyHash,
        history_incarnation: u64,
        generation: ProjectionGeneration,
        frontier: CommitSequence,
        apply_hash: [u8; 32],
        registry: IncrementalProjectionRegistryV1,
        rows: &BTreeMap<EntityKey, CanonicalRecord>,
    ) -> Result<Self, IncrementalProviderErrorV1> {
        let mut index = Self::new(partition, history_incarnation, generation, registry);
        let mutations: Vec<IncrementalRowMutationV1> = rows
            .iter()
            .map(|(k, v)| IncrementalRowMutationV1::Upsert {
                key: k.clone(),
                post_image: v.clone(),
            })
            .collect();
        index.apply_commit(frontier, apply_hash, &mutations)?;
        Ok(index)
    }

    /// Compacts physical state while preserving exact logical results, frontier, and generation (ADR-0162 Decision 3).
    pub fn compact(&mut self) -> Result<(), IncrementalProviderErrorV1> {
        let Some(frontier) = self.visible_frontier else {
            return Ok(());
        };
        let last_hash = self
            .applied_commits
            .get(&frontier)
            .copied()
            .unwrap_or([0u8; 32]);
        let rebuilt = Self::rebuild_from_rows(
            self.partition,
            self.history_incarnation,
            self.generation,
            frontier,
            last_hash,
            self.registry.clone(),
            &self.rows,
        )?;
        self.structures = rebuilt.structures;
        Ok(())
    }

    /// Retires the current provider epoch when capability or policy alignment changes (ADR-0162 Decision 4).
    pub fn retire_on_policy_change(&mut self) {
        self.lifecycle = ProviderLifecycleV1::Retired;
        self.degraded_reason = Some(IncrementalDegradedReasonV1::PolicyEpochRetired);
    }

    /// Evaluates a finite bitmap predicate intersection on a declared bitmap structure.
    pub fn query_bitmap(
        &self,
        structure_id: IncrementalStructureIdV1,
        plan_hash: QueryPlanHash,
        caller_role: ApplicationRoleHash,
        caller_capability_revision: u64,
        predicates: &[IncrementalBitmapPredicateV1],
    ) -> Result<BTreeSet<EntityKey>, IncrementalProviderErrorV1> {
        self.ensure_ready()?;
        let desc = self
            .registry
            .verify_single_structure_plan(structure_id, plan_hash, None)?;
        desc.policy_alignment()
            .authorize_caller(caller_role, caller_capability_revision)?;
        match self.structures.get(&structure_id) {
            Some(StructureInstanceV1::Bitmap(bitmap)) => Ok(bitmap.evaluate(predicates)),
            _ => Err(IncrementalProviderErrorV1::StructureKindMismatch),
        }
    }

    /// Reads the exact maintained rollup snapshot for a declared rollup structure.
    pub fn query_rollup(
        &self,
        structure_id: IncrementalStructureIdV1,
        plan_hash: QueryPlanHash,
        caller_role: ApplicationRoleHash,
        caller_capability_revision: u64,
    ) -> Result<BTreeMap<IncrementalGroupKeyV1, IncrementalRollupValueV1>, IncrementalProviderErrorV1>
    {
        self.ensure_ready()?;
        let desc = self
            .registry
            .verify_single_structure_plan(structure_id, plan_hash, None)?;
        desc.policy_alignment()
            .authorize_caller(caller_role, caller_capability_revision)?;
        match self.structures.get(&structure_id) {
            Some(StructureInstanceV1::Rollup(rollup)) => Ok(rollup.snapshot_values()),
            _ => Err(IncrementalProviderErrorV1::StructureKindMismatch),
        }
    }

    /// Reads the top `limit` ordered entries from a declared top-K lane.
    pub fn query_top_k(
        &self,
        structure_id: IncrementalStructureIdV1,
        plan_hash: QueryPlanHash,
        caller_role: ApplicationRoleHash,
        caller_capability_revision: u64,
        limit: NonZeroU16,
    ) -> Result<Vec<IncrementalTopKEntryV1>, IncrementalProviderErrorV1> {
        self.ensure_ready()?;
        let desc = self
            .registry
            .verify_single_structure_plan(structure_id, plan_hash, None)?;
        desc.policy_alignment()
            .authorize_caller(caller_role, caller_capability_revision)?;
        match self.structures.get(&structure_id) {
            Some(StructureInstanceV1::TopK(topk)) => topk.top_k(limit),
            _ => Err(IncrementalProviderErrorV1::StructureKindMismatch),
        }
    }

    fn ensure_ready(&self) -> Result<(), IncrementalProviderErrorV1> {
        if self.lifecycle != ProviderLifecycleV1::Ready {
            return Err(IncrementalProviderErrorV1::ProviderUnavailable {
                lifecycle: self.lifecycle,
                reason: self.degraded_reason,
            });
        }
        Ok(())
    }
}

fn retract_from_instance(
    instance: &mut StructureInstanceV1,
    kind: &IncrementalStructureKindV1,
    key: &EntityKey,
    old_record: &CanonicalRecord,
) -> Result<Result<(), IncrementalDegradedReasonV1>, IncrementalProviderErrorV1> {
    match (instance, kind) {
        (
            StructureInstanceV1::Bitmap(bitmap),
            IncrementalStructureKindV1::SegmentEqualityPresenceBitmap { .. },
        ) => {
            bitmap.remove_row(key, old_record);
            Ok(Ok(()))
        }
        (
            StructureInstanceV1::Rollup(rollup),
            IncrementalStructureKindV1::ExactMergeableRollup { .. },
        ) => match rollup.apply_row(old_record, false) {
            Ok(()) => Ok(Ok(())),
            Err(Ok(reason)) => Ok(Err(reason)),
            Err(Err(err)) => Err(err),
        },
        (
            StructureInstanceV1::TopK(topk),
            IncrementalStructureKindV1::MaintainedTopKLane { .. },
        ) => Ok(topk.remove_row(key, old_record)),
        _ => Err(IncrementalProviderErrorV1::StructureKindMismatch),
    }
}

fn insert_into_instance(
    instance: &mut StructureInstanceV1,
    kind: &IncrementalStructureKindV1,
    key: EntityKey,
    new_record: &CanonicalRecord,
) -> Result<Result<(), IncrementalDegradedReasonV1>, IncrementalProviderErrorV1> {
    match (instance, kind) {
        (
            StructureInstanceV1::Bitmap(bitmap),
            IncrementalStructureKindV1::SegmentEqualityPresenceBitmap { .. },
        ) => Ok(bitmap.insert_row(key, new_record)),
        (
            StructureInstanceV1::Rollup(rollup),
            IncrementalStructureKindV1::ExactMergeableRollup { .. },
        ) => match rollup.apply_row(new_record, true) {
            Ok(()) => Ok(Ok(())),
            Err(Ok(reason)) => Ok(Err(reason)),
            Err(Err(err)) => Err(err),
        },
        (
            StructureInstanceV1::TopK(topk),
            IncrementalStructureKindV1::MaintainedTopKLane { .. },
        ) => Ok(topk.insert_row(key, new_record)),
        _ => Err(IncrementalProviderErrorV1::StructureKindMismatch),
    }
}

// ---------------------------------------------------------------------------
// 5. Independent reference evaluators for differential oracle proofs
// ---------------------------------------------------------------------------

/// Independent reference evaluator for a segment-local bitmap predicate intersection (`OBL-0162-1`).
#[must_use]
pub fn evaluate_bitmap_predicate_reference_v1(
    rows: &BTreeMap<EntityKey, CanonicalRecord>,
    policy: &IncrementalPolicyAlignmentV1,
    field_id: FieldId,
    predicates: &[IncrementalBitmapPredicateV1],
) -> BTreeSet<EntityKey> {
    if predicates.is_empty() {
        return BTreeSet::new();
    }
    rows.iter()
        .filter(|(_, record)| policy.admits_record(record))
        .filter(|(_, record)| {
            let field_val = record_field(record, field_id);
            predicates.iter().all(|pred| match pred {
                IncrementalBitmapPredicateV1::IsMissing => field_val.is_none(),
                IncrementalBitmapPredicateV1::IsNull => field_val == Some(&CanonicalValue::Null),
                IncrementalBitmapPredicateV1::IsPresent => {
                    matches!(field_val, Some(v) if *v != CanonicalValue::Null)
                }
                IncrementalBitmapPredicateV1::Equals(target) => field_val == Some(target),
            })
        })
        .map(|(k, _)| k.clone())
        .collect()
}

/// Independent reference evaluator for an exact rollup over a materialized row map (`OBL-0162-1`).
pub fn evaluate_rollup_reference_v1(
    rows: &BTreeMap<EntityKey, CanonicalRecord>,
    policy: &IncrementalPolicyAlignmentV1,
    group_fields: &[FieldId],
    function: IncrementalRollupFunctionV1,
) -> Result<BTreeMap<IncrementalGroupKeyV1, IncrementalRollupValueV1>, IncrementalProviderErrorV1> {
    let max_groups = NonZeroU16::new(MAX_INCREMENTAL_ROLLUP_GROUPS_V1).unwrap_or(NonZeroU16::MIN);
    let max_distinct =
        NonZeroU16::new(MAX_INCREMENTAL_DISTINCT_VALUES_V1).unwrap_or(NonZeroU16::MIN);
    let max_bytes =
        NonZeroU32::new(MAX_INCREMENTAL_PARTITION_STATE_BYTES_V1).unwrap_or(NonZeroU32::MIN);
    let mut state = ExactRollupStateV1::new(
        group_fields.to_vec(),
        function,
        max_groups,
        max_distinct,
        max_bytes,
    );
    for record in rows.values() {
        if !policy.admits_record(record) {
            continue;
        }
        match state.apply_row(record, true) {
            Ok(()) => {}
            Err(Ok(reason)) => {
                return Err(IncrementalProviderErrorV1::ProviderUnavailable {
                    lifecycle: ProviderLifecycleV1::Rebuilding,
                    reason: Some(reason),
                });
            }
            Err(Err(err)) => return Err(err),
        }
    }
    Ok(state.snapshot_values())
}

/// Independent reference evaluator for a maintained top-K lane over a materialized row map (`OBL-0162-1`).
#[must_use]
pub fn evaluate_top_k_reference_v1(
    rows: &BTreeMap<EntityKey, CanonicalRecord>,
    policy: &IncrementalPolicyAlignmentV1,
    value_field: FieldId,
    direction: IncrementalSortDirectionV1,
    no_value_placement: IncrementalNoValuePlacementV1,
    limit: NonZeroU16,
) -> Vec<IncrementalTopKEntryV1> {
    let mut sorted: BTreeMap<TopKSortKeyV1, CanonicalRecord> = BTreeMap::new();
    for (key, record) in rows {
        if !policy.admits_record(record) {
            continue;
        }
        let value = match record_field(record, value_field) {
            None | Some(CanonicalValue::Null) => None,
            Some(v) => Some(v.clone()),
        };
        sorted.insert(
            TopKSortKeyV1 {
                value,
                direction,
                no_value_placement,
                entity_key: key.clone(),
            },
            record.clone(),
        );
    }
    sorted
        .into_iter()
        .take(usize::from(limit.get()))
        .map(|(sk, record)| IncrementalTopKEntryV1 {
            key: sk.entity_key,
            order_value: sk.value,
            record,
        })
        .collect()
}

fn record_field(record: &CanonicalRecord, field_id: FieldId) -> Option<&CanonicalValue> {
    record
        .fields()
        .iter()
        .find_map(|(fid, val)| (*fid == field_id).then_some(val))
}

fn extract_optional_i64(
    record: &CanonicalRecord,
    field_id: FieldId,
) -> Result<Option<i64>, IncrementalProviderErrorV1> {
    match record_field(record, field_id) {
        None | Some(CanonicalValue::Null) => Ok(None),
        Some(CanonicalValue::I64(v)) => Ok(Some(*v)),
        Some(_) => Err(IncrementalProviderErrorV1::IncompatibleFieldType(field_id)),
    }
}

fn extract_required_bool(
    record: &CanonicalRecord,
    field_id: FieldId,
) -> Result<bool, IncrementalProviderErrorV1> {
    match record_field(record, field_id) {
        Some(CanonicalValue::Bool(b)) => Ok(*b),
        _ => Err(IncrementalProviderErrorV1::IncompatibleFieldType(field_id)),
    }
}

fn compare_canonical_values(left: &CanonicalValue, right: &CanonicalValue) -> Ordering {
    match (left, right) {
        (CanonicalValue::Null, CanonicalValue::Null) => Ordering::Equal,
        (CanonicalValue::Bool(a), CanonicalValue::Bool(b)) => a.cmp(b),
        (CanonicalValue::I64(a), CanonicalValue::I64(b)) => a.cmp(b),
        (CanonicalValue::U64(a), CanonicalValue::U64(b)) => a.cmp(b),
        (CanonicalValue::String(a), CanonicalValue::String(b)) => a.cmp(b),
        (CanonicalValue::Bytes(a), CanonicalValue::Bytes(b)) => a.cmp(b),
        _ => format!("{left:?}").cmp(&format!("{right:?}")),
    }
}
