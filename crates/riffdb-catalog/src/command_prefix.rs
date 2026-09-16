//! Catalog-owned checks for supplied command-prefix index and vector images.

use crate::{CatalogError, CatalogErrorKind, ValidatedContractBundle};
use riffdb_storage_api::{AuthoritativeNamespaceV1 as N, StoredCommandCapsuleV2};

pub(super) fn corrupt() -> CatalogError {
    CatalogError::new(CatalogErrorKind::InvalidHistoricalEvidence)
}

/// Checks supplied secondary-index keys and derives put keys and covers from
/// their owning entity post-images using the exact retained command bundle.
/// Supplied vector images also require a production field and checked new model metadata.
/// This pure check grants no readiness, mutation or reconstruction authority;
/// complete mutation inventory and predecessor proofs remain separate.
pub fn validate_command_prefix_index_images_v1(
    bundle: &ValidatedContractBundle,
    command: &StoredCommandCapsuleV2,
) -> Result<(), CatalogError> {
    let Some(prefix) = command.prefix_evidence() else {
        return Ok(());
    };
    let plan = command.base().commit().plan();
    if bundle.lineage() != plan.contract_lineage()
        || bundle.contract_version() != plan.contract_version()
        || bundle.bundle_hash() != plan.contract_bundle_hash()
    {
        return Err(corrupt());
    }
    super::command_prefix_vector::validate_supplied_images(bundle, command)?;
    for mutation in prefix
        .mutations()
        .iter()
        .filter(|m| m.namespace() == N::SecondaryIndexes)
    {
        let key = riffdb_types::IndexEntryKey::from_bytes(mutation.key().to_vec())
            .map_err(|_| corrupt())?;
        let (entity, index) = bundle
            .bundle()
            .schema()
            .entities()
            .iter()
            .find_map(|entity| {
                entity
                    .indexes()
                    .iter()
                    .find(|index| index.id() == key.index_id())
                    .map(|index| (entity, index))
            })
            .ok_or_else(corrupt)?;
        let decoded = index
            .key_schema()
            .decode_index(&key)
            .map_err(|_| corrupt())?;
        if !command.entity_transitions().iter().any(|transition| {
            transition.target().entity_type_id() == entity.id()
                && transition.target().key() == decoded.entity_key()
        }) {
            return Err(corrupt());
        }
        let Some(bytes) = mutation.value() else {
            continue;
        };
        let entry = riffdb_storage_api::decode_index_entry_v2(bytes).map_err(|_| corrupt())?;
        let entity_key = decoded.entity_key().as_bytes();
        let offset = prefix
            .mutations()
            .binary_search_by(|row| (row.namespace(), row.key()).cmp(&(N::Entities, entity_key)))
            .map_err(|_| corrupt())?;
        let image = prefix.mutations()[offset].value().ok_or_else(corrupt)?;
        let image = riffdb_storage_api::decode_entity_record_v1(image).map_err(|_| corrupt())?;
        if image.value().target().entity_type_id() != entity.id()
            || image.value().target().key() != decoded.entity_key()
        {
            return Err(corrupt());
        }
        let fields = image.value().fields();
        let values = riffdb_contract_ir::encode_operational_index_values_v1(index, fields)
            .map_err(|_| corrupt())?;
        let expected = index
            .key_schema()
            .encode_index(&values, decoded.entity_key().clone())
            .map_err(|_| corrupt())?;
        let cover = riffdb_contract_ir::encode_operational_index_cover_v1(index, fields)
            .map_err(|_| corrupt())?;
        let partition = crate::history::derive_historical_partition(
            bundle.bundle().schema(),
            entity,
            decoded.entity_key(),
        )?;
        if expected != key
            || entry.value().covered_values() != &cover
            || entry.value().partition_key() != &partition
        {
            return Err(corrupt());
        }
    }
    Ok(())
}

/// Checks the complete secondary-index mutation inventory for one command-owned
/// entity whose actual predecessor is available. Missing historical predecessors
/// must not be represented as absent; callers skip this proof until they have one.
/// The returned iterator lazily derives required prior rows, including unchanged
/// indexes. Callers must validate every available row before granting progress.
pub fn validate_command_prefix_entity_indexes_v1<'a>(
    bundle: &'a ValidatedContractBundle,
    command: &StoredCommandCapsuleV2,
    transition: &riffdb_storage_api::CommittedEntityTransitionV1,
    prior: Option<&'a riffdb_storage_api::StoredEntityRecordV1>,
    resolved: Option<&crate::ResolvedExecutablePlan>,
) -> Result<CommandPrefixIndexPriorsV1<'a>, CatalogError> {
    let prefix = command.prefix_evidence().ok_or_else(corrupt)?;
    let plan = command.base().commit().plan();
    if bundle.lineage() != plan.contract_lineage()
        || bundle.contract_version() != plan.contract_version()
        || bundle.bundle_hash() != plan.contract_bundle_hash()
        || !command.entity_transitions().contains(transition)
    {
        return Err(corrupt());
    }
    let prior = checked_prior(command, transition, prior, resolved)?;
    let target = transition.target();
    let entity = bundle
        .bundle()
        .schema()
        .entity(target.entity_type_id())
        .ok_or_else(corrupt)?;
    let entity_offset = prefix
        .mutations()
        .binary_search_by(|row| {
            (row.namespace(), row.key()).cmp(&(N::Entities, target.key().as_bytes()))
        })
        .map_err(|_| corrupt())?;
    let next = prefix.mutations()[entity_offset]
        .value()
        .map(riffdb_storage_api::decode_entity_record_v1)
        .transpose()
        .map_err(|_| corrupt())?;
    if next
        .as_ref()
        .is_some_and(|row| row.value().target() != target)
    {
        return Err(corrupt());
    }
    let mut required = 0usize;
    for index in entity.indexes() {
        let old = prior
            .as_deref()
            .map(|row| index_image(index, row))
            .transpose()?;
        let new = next
            .as_ref()
            .map(|row| index_image(index, row.value()))
            .transpose()?;
        if old == new {
            continue;
        }
        if let Some((key, _)) = &old
            && new.as_ref().is_none_or(|(new_key, _)| new_key != key)
        {
            require_index_mutation(prefix, key, false, true)?;
            required += 1;
        }
        if let Some((key, _)) = &new {
            let replaces = old.as_ref().is_some_and(|(old_key, _)| old_key == key);
            require_index_mutation(prefix, key, true, replaces)?;
            required += 1;
        }
    }
    let mut supplied = 0usize;
    for row in prefix
        .mutations()
        .iter()
        .filter(|row| row.namespace() == N::SecondaryIndexes)
    {
        let key =
            riffdb_types::IndexEntryKey::from_bytes(row.key().to_vec()).map_err(|_| corrupt())?;
        if let Some(index) = entity
            .indexes()
            .iter()
            .find(|index| index.id() == key.index_id())
        {
            let decoded = index
                .key_schema()
                .decode_index(&key)
                .map_err(|_| corrupt())?;
            if decoded.entity_key() == target.key() {
                supplied += 1;
            }
        }
    }
    if supplied != required {
        return Err(corrupt());
    }
    let context = prior
        .map(|record| {
            crate::history::derive_historical_partition(
                bundle.bundle().schema(),
                entity,
                target.key(),
            )
            .map(|partition| (record, partition))
        })
        .transpose()?;
    Ok(CommandPrefixIndexPriorsV1 {
        indexes: entity.indexes().iter(),
        context,
    })
}

/// Lazy expectations for one entity's index predecessors. This is read-only
/// evidence, not a readiness or reconstruction capability. At most one cover
/// is derived per step; no aggregate collection of covered values is retained.
#[must_use = "validate the derived index predecessors before granting progress"]
pub struct CommandPrefixIndexPriorsV1<'a> {
    indexes: std::slice::Iter<'a, riffdb_contract_ir::IndexSchema>,
    context: Option<(
        std::borrow::Cow<'a, riffdb_storage_api::StoredEntityRecordV1>,
        riffdb_types::PartitionKey,
    )>,
}

impl Iterator for CommandPrefixIndexPriorsV1<'_> {
    type Item = Result<CommandPrefixIndexPriorV1, CatalogError>;

    fn next(&mut self) -> Option<Self::Item> {
        let (record, partition) = self.context.as_ref()?;
        let index = self.indexes.next()?;
        Some(
            index_image(index, record).map(|(key, cover)| CommandPrefixIndexPriorV1 {
                key,
                cover,
                partition: partition.clone(),
            }),
        )
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.context.is_some() {
            self.indexes.size_hint()
        } else {
            (0, Some(0))
        }
    }
}

impl std::fmt::Debug for CommandPrefixIndexPriorsV1<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("CommandPrefixIndexPriorsV1([REDACTED])")
    }
}

/// One catalog-derived expected prior row, without a claimed historical schema
/// binding. The startup history proof or preceding validated prefix owns that
/// binding; this check proves the key, covered fields and owning partition.
pub struct CommandPrefixIndexPriorV1 {
    key: riffdb_types::IndexEntryKey,
    cover: riffdb_types::CanonicalRecord,
    partition: riffdb_types::PartitionKey,
}

impl CommandPrefixIndexPriorV1 {
    /// Returns the exact key whose actual predecessor must be read.
    #[must_use]
    pub const fn key(&self) -> &riffdb_types::IndexEntryKey {
        &self.key
    }

    /// Refuses an absent or contradictory actual row. A missing historical
    /// observation is unknown, and must never be passed here as known absence.
    pub fn validate(
        &self,
        row: Option<&riffdb_storage_api::StoredIndexEntryV2>,
    ) -> Result<(), CatalogError> {
        let row = row.ok_or_else(corrupt)?;
        if row.key() != &self.key
            || row.covered_values() != &self.cover
            || row.partition_key() != &self.partition
        {
            return Err(corrupt());
        }
        Ok(())
    }
}

impl std::fmt::Debug for CommandPrefixIndexPriorV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("CommandPrefixIndexPriorV1([REDACTED])")
    }
}

fn index_image(
    index: &riffdb_contract_ir::IndexSchema,
    record: &riffdb_storage_api::StoredEntityRecordV1,
) -> Result<(riffdb_types::IndexEntryKey, riffdb_types::CanonicalRecord), CatalogError> {
    let values = riffdb_contract_ir::encode_operational_index_values_v1(index, record.fields())
        .map_err(|_| corrupt())?;
    let key = index
        .key_schema()
        .encode_index(&values, record.target().key().clone())
        .map_err(|_| corrupt())?;
    let cover = riffdb_contract_ir::encode_operational_index_cover_v1(index, record.fields())
        .map_err(|_| corrupt())?;
    Ok((key, cover))
}

fn require_index_mutation(
    prefix: &riffdb_storage_api::CommandPrefixEvidenceV1,
    key: &riffdb_types::IndexEntryKey,
    put: bool,
    has_prior: bool,
) -> Result<(), CatalogError> {
    let offset = prefix
        .mutations()
        .binary_search_by(|row| {
            (row.namespace(), row.key()).cmp(&(N::SecondaryIndexes, key.as_bytes()))
        })
        .map_err(|_| corrupt())?;
    let row = &prefix.mutations()[offset];
    if row.value().is_some() != put || row.expected_hash().is_some() != has_prior {
        return Err(corrupt());
    }
    Ok(())
}

// Raw transition identity is proved before any historical null materialization.
pub(super) fn checked_prior<'a>(
    command: &StoredCommandCapsuleV2,
    transition: &riffdb_storage_api::CommittedEntityTransitionV1,
    prior: Option<&'a riffdb_storage_api::StoredEntityRecordV1>,
    resolved: Option<&crate::ResolvedExecutablePlan>,
) -> Result<Option<std::borrow::Cow<'a, riffdb_storage_api::StoredEntityRecordV1>>, CatalogError> {
    use riffdb_storage_api::EntityChainStateV1 as State;
    let plan = command.base().commit().plan();
    match (transition.prior_state(), prior) {
        (
            State::Live {
                version,
                value_hash,
            },
            Some(record),
        ) if record.target() == transition.target()
            && record.entity_version() == version
            && riffdb_storage_api::derive_entity_record_hash_v1(record)
                .map_err(|_| corrupt())?
                == value_hash => {}
        (State::NeverExisted | State::Deleted, None) => {}
        _ => return Err(corrupt()),
    }
    // Verify the raw prior hash above, then apply the existing lineage-owned
    // logical view. Never hash the null-filled view as historical writer bytes.
    prior
        .map(|record| {
            if record.schema_binding().matches_plan(plan) {
                Ok(std::borrow::Cow::Borrowed(record))
            } else {
                let resolved = resolved
                    .filter(|resolved| resolved.reference() == plan)
                    .ok_or_else(corrupt)?;
                crate::materialization::materialize_prefix_index_predecessor(resolved, record)
                    .map(std::borrow::Cow::Owned)
                    .map_err(|_| corrupt())
            }
        })
        .transpose()
}
