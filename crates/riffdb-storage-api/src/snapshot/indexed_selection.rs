//! Storage-owned lowering and checking of one bounded selection observation.
use super::*;
use riffdb_contract_ir::{
    CommandPlan, EntitySchema, IndexSchema, IndexedSelectionModeV1, KeySchema,
    MAX_MUTATE_ANY_CANDIDATE_WINDOW_V1, SchemaIr, encode_operational_index_cover_v1,
    encode_operational_index_values_v1,
};
use riffdb_types::CanonicalValue;

/// Exact one-entry seek and the entity/root targets determined by its result.
#[derive(Clone, Eq, PartialEq)]
pub struct IndexedSelectionRequest {
    plan_hash: riffdb_types::PlanHash,
    binding_position: usize,
    mode: IndexedSelectionModeV1,
    entity: EntitySchema,
    index: IndexSchema,
    range: IndexRangeTarget,
    roots: Vec<(usize, KeySchema)>,
    pinned_candidate: Option<IndexRangeEntry>,
}

impl IndexedSelectionRequest {
    /// Lowers a checked selector with its input-derived prefix and partition.
    pub fn from_plan(
        plan: &CommandPlan,
        schema: &SchemaIr,
        partition: PartitionKey,
        values: &[CanonicalValue],
    ) -> Result<Self, StorageValueError> {
        let (binding, selection) = plan
            .indexed_selection()
            .ok_or(StorageValueError::IdentityMismatch)?;
        if values.len() != selection.prefix_expressions().len() {
            return Err(StorageValueError::IdentityMismatch);
        }
        let entity = schema
            .entity(binding.entity_type())
            .ok_or(StorageValueError::IdentityMismatch)?;
        let index = entity
            .indexes()
            .iter()
            .find(|index| index.id() == selection.index())
            .ok_or(StorageValueError::IdentityMismatch)?;
        let prefix = index
            .key_schema()
            .encode_index_prefix(values)
            .map_err(|_| StorageValueError::IdentityMismatch)?;
        let range = IndexRangeTarget::new(
            partition,
            IndexRangePrefix {
                index_id: prefix.index_id(),
                bytes: prefix.as_bytes().to_vec(),
            },
        );
        let mut roots = Vec::new();
        for (position, read) in plan.root_validation_reads().iter().enumerate() {
            if read.key_expressions().iter().any(|id| {
                plan.expressions()
                    .dependencies(*id)
                    .is_ok_and(|deps| !deps.bindings().is_empty())
            }) {
                if read.source_binding() != binding.id() {
                    return Err(StorageValueError::IdentityMismatch);
                }
                roots.push((position, read.key_schema().clone()));
            }
        }
        Ok(Self {
            plan_hash: plan.plan_hash(),
            binding_position: binding.id().get() as usize,
            mode: selection.mode(),
            entity: entity.clone(),
            index: index.clone(),
            range,
            roots,
            pinned_candidate: None,
        })
    }

    /// Returns the compiled selection mode (`first` vs `any`).
    #[must_use]
    pub const fn mode(&self) -> IndexedSelectionModeV1 {
        self.mode
    }

    /// Returns the candidate entry pinned after non-blocking conflict acquisition (`mutate any`).
    #[must_use]
    pub const fn pinned_candidate(&self) -> Option<&IndexRangeEntry> {
        self.pinned_candidate.as_ref()
    }

    /// Pins this `mutate any` selector to one discovered candidate entry for
    /// verification under its acquired conflict lease.
    pub fn with_pinned_candidate(
        mut self,
        entry: IndexRangeEntry,
    ) -> Result<Self, StorageValueError> {
        if self.mode != IndexedSelectionModeV1::Any {
            return Err(StorageValueError::IdentityMismatch);
        }
        self.selected_target(&entry)?;
        self.pinned_candidate = Some(entry);
        Ok(self)
    }

    /// The proved partition-constrained prefix; readers must stop at its first entry.
    #[must_use]
    pub const fn range(&self) -> &IndexRangeTarget {
        &self.range
    }

    /// Decodes the one candidate's exact entity target.
    pub fn selected_target(
        &self,
        entry: &IndexRangeEntry,
    ) -> Result<EntityTarget, StorageValueError> {
        if !entry
            .key()
            .as_bytes()
            .starts_with(self.range.prefix().as_bytes())
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        let key = self
            .index
            .key_schema()
            .decode_index_entity_key(entry.key())
            .map_err(|_| StorageValueError::IdentityMismatch)?;
        self.entity
            .primary_key()
            .decode_entity(&key)
            .map_err(|_| StorageValueError::IdentityMismatch)?;
        EntityTarget::new(self.entity.id(), key)
    }

    /// Reconstructs the selected entry from logical index/cover values. Missing
    /// optional fields have the logical null used by index migration; catalog
    /// materialization must separately prove their ancestry authority. Raw row
    /// bytes and dependency hashes are never replaced by this check.
    pub fn validate_record(
        &self,
        entry: &IndexRangeEntry,
        observation: &EntityObservation,
    ) -> Result<(), StorageValueError> {
        let target = self.selected_target(entry)?;
        let EntityObservation::Present(record) = observation else {
            return Err(StorageValueError::IdentityMismatch);
        };
        if record.target() != &target {
            return Err(StorageValueError::IdentityMismatch);
        }
        // Stored records include key fields; reject a key/record disagreement too.
        let keys = self
            .entity
            .primary_key()
            .decode_entity(target.key())
            .map_err(|_| StorageValueError::IdentityMismatch)?;
        for (field, expected) in self.entity.primary_key_fields().iter().zip(keys) {
            if record
                .fields()
                .fields()
                .iter()
                .find(|(id, _)| id == field)
                .map(|(_, value)| value)
                != Some(&expected)
            {
                return Err(StorageValueError::IdentityMismatch);
            }
        }
        let missing: std::collections::BTreeSet<_> = self
            .index
            .fields()
            .iter()
            .chain(self.index.cover_fields())
            .filter(|field| {
                record
                    .fields()
                    .fields()
                    .binary_search_by_key(field, |(id, _)| id)
                    .is_err()
            })
            .copied()
            .collect();
        let logical = if missing.is_empty() {
            None
        } else {
            let mut fields = record.fields().fields().to_vec();
            for field in missing {
                if !self
                    .entity
                    .record()
                    .field(field)
                    .is_some_and(|field| field.value_type().is_optional())
                {
                    return Err(StorageValueError::IdentityMismatch);
                }
                fields.push((field, CanonicalValue::Null));
            }
            Some(
                riffdb_types::CanonicalRecord::new(fields)
                    .map_err(|_| StorageValueError::IdentityMismatch)?,
            )
        };
        let fields = logical.as_ref().unwrap_or(record.fields());
        let values = encode_operational_index_values_v1(&self.index, fields)
            .map_err(|_| StorageValueError::IdentityMismatch)?;
        let key = self
            .index
            .key_schema()
            .encode_index(&values, target.key().clone())
            .map_err(|_| StorageValueError::IdentityMismatch)?;
        let cover = encode_operational_index_cover_v1(&self.index, fields)
            .map_err(|_| StorageValueError::IdentityMismatch)?;
        if &key != entry.key() || &cover != entry.covered_values() {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(())
    }

    /// Checks one exact snapshot range and selected row before target resolution.
    pub fn selected_key<'a>(
        &self,
        snapshot: &'a ReadSnapshot,
    ) -> Result<Option<&'a EntityKey>, StorageValueError> {
        let range = snapshot
            .ranges()
            .iter()
            .find(|range| {
                range.target().prefix() == self.range.prefix()
                    && range.target().generation_target() == self.range.generation_target()
            })
            .ok_or(StorageValueError::IdentityMismatch)?;
        if range.entries().len() > 1 {
            return Err(StorageValueError::LimitExceeded);
        }
        let Some(entry) = range.entries().first() else {
            if range.target().is_point_validated() {
                return Err(StorageValueError::IdentityMismatch);
            }
            return Ok(None);
        };
        if range.target().is_point_validated() != (self.mode == IndexedSelectionModeV1::Any) {
            return Err(StorageValueError::IdentityMismatch);
        }
        if let Some(pinned) = &self.pinned_candidate
            && pinned != entry
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        let observation = snapshot
            .bindings()
            .get(self.binding_position)
            .ok_or(StorageValueError::IdentityMismatch)?;
        self.validate_record(entry, observation)?;
        Ok(Some(observation.target().key()))
    }
}

impl SnapshotRequest {
    /// Attaches the compiler-proved single selector before any storage observation.
    pub fn with_indexed_selection(
        mut self,
        selection: IndexedSelectionRequest,
    ) -> Result<Self, StorageValueError> {
        if self.indexed_selection.is_some()
            || self.plan.command_plan_hash() != selection.plan_hash
            || !self.range_targets.is_empty()
            || !self.cascade_targets.is_empty()
            || selection.binding_position > self.binding_targets.len()
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        self.range_targets.push(selection.range.clone());
        self.range_entry_limits.push(1);
        self.indexed_selection = Some(selection);
        Ok(self)
    }

    /// Builds a bounded candidate-window discovery request (`K <= 16`) for a
    /// `mutate any` selector (`ADR-0333`).
    pub fn candidate_window_request(&self) -> Result<Self, StorageValueError> {
        let selection = self
            .indexed_selection
            .as_ref()
            .ok_or(StorageValueError::IdentityMismatch)?;
        if selection.mode != IndexedSelectionModeV1::Any {
            return Err(StorageValueError::IdentityMismatch);
        }
        let mut request = self.clone();
        request.indexed_selection = None;
        request.binding_targets.clear();
        request.root_validation_targets.clear();
        request.cascade_targets.clear();
        request.range_targets = vec![selection.range.clone()];
        request.range_entry_limits = vec![MAX_MUTATE_ANY_CANDIDATE_WINDOW_V1];
        Ok(request)
    }

    /// Returns a copy of this selector request pinned to one candidate entry
    /// for verification under its acquired conflict lease (`ADR-0333`).
    pub fn with_pinned_candidate(&self, entry: IndexRangeEntry) -> Result<Self, StorageValueError> {
        let selection = self
            .indexed_selection
            .as_ref()
            .ok_or(StorageValueError::IdentityMismatch)?
            .clone()
            .with_pinned_candidate(entry)?;
        let mut request = self.clone();
        request.indexed_selection = Some(selection);
        Ok(request)
    }

    /// One selector, requiring a direct first-entry seek in the same read view.
    #[must_use]
    pub const fn indexed_selection(&self) -> Option<&IndexedSelectionRequest> {
        self.indexed_selection.as_ref()
    }

    /// Converts the observed candidate into exact entity/root reads inside the
    /// same storage view. Empty selection creates no fictitious entity target.
    pub fn resolve_indexed_selection(
        &self,
        entry: Option<&IndexRangeEntry>,
    ) -> Result<Self, StorageValueError> {
        let selection = self
            .indexed_selection
            .as_ref()
            .ok_or(StorageValueError::IdentityMismatch)?;
        let mut request = self.clone();
        request.indexed_selection = None;
        if let Some(entry) = entry {
            if selection.mode == IndexedSelectionModeV1::Any
                && let Some(range_target) = request.range_targets.first_mut()
            {
                *range_target = range_target.clone().with_point_validation(true);
            }
            let target = selection.selected_target(entry)?;
            let values = selection
                .entity
                .primary_key()
                .decode_entity(target.key())
                .map_err(|_| StorageValueError::IdentityMismatch)?;
            request
                .binding_targets
                .insert(selection.binding_position, target);
            for (position, schema) in &selection.roots {
                let components = values
                    .get(..schema.components().len())
                    .ok_or(StorageValueError::IdentityMismatch)?;
                let key = schema
                    .encode_entity(components)
                    .map_err(|_| StorageValueError::IdentityMismatch)?;
                let target = EntityTarget::new(key.entity_type_id(), key)?;
                if *position > request.root_validation_targets.len() {
                    return Err(StorageValueError::IdentityMismatch);
                }
                request.root_validation_targets.insert(*position, target);
            }
        }
        if request.binding_targets.len() + request.root_validation_targets.len() + 1
            > MAX_COMMAND_READ_TARGETS
        {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(request)
    }
}
