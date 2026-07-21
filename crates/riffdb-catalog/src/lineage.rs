//! Bounded process-local active-lineage validation and materialization metadata.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use riffdb_contract_ir::{
    CompatibilityClass, MAX_DECLARATIONS_PER_KIND, MAX_LINEAGE_LEDGER_ENTRIES, RecordSchema,
};
use riffdb_storage_api::{ActiveCatalogPointerV1, CatalogRepository, DurableKeySchemaBindingV1};
use riffdb_types::{
    ContractBundleHash, ContractLineage, ContractVersion, EntityTypeId, EventTypeId, FieldId,
    MAX_CONTRACT_LINEAGE_BYTES,
};

use crate::{
    CatalogError, CatalogErrorKind, ValidatedContractBundle, validate_successor_compatibility,
};

pub(crate) const MAX_ACTIVE_LINEAGE_BUNDLES_V1: usize = 4_096;
pub(crate) const MAX_ACTIVE_LINEAGE_CANONICAL_BYTES_V1: usize = 64 * 1024 * 1024;
pub(crate) const MAX_LINEAGE_MATERIALIZATION_PROOF_BYTES_V1: usize = 2 * 1024 * 1024;

const LINEAGE_MATERIALIZATION_PROOF_VERSION_V1: u8 = 1;
const MAX_LINEAGE_RECORD_OWNERS_V1: usize = MAX_DECLARATIONS_PER_KIND * 2;
const MAX_LINEAGE_RECORD_FIELDS_V1: usize = MAX_LINEAGE_LEDGER_ENTRIES;
const MAX_LINEAGE_PROOF_SHAPE_CHARGE_V1: usize = 1
    + (4 + MAX_CONTRACT_LINEAGE_BYTES)
    + 4
    + (MAX_ACTIVE_LINEAGE_BUNDLES_V1 * (8 + 32))
    + 2
    + 4
    + (MAX_LINEAGE_RECORD_OWNERS_V1 * (1 + 4 + 4))
    + (MAX_LINEAGE_RECORD_FIELDS_V1 * (4 + 2));

const _: () = assert!(MAX_ACTIVE_LINEAGE_BUNDLES_V1 <= u16::MAX as usize + 1);
const _: () = assert!(LINEAGE_MATERIALIZATION_PROOF_VERSION_V1 == 1);
const _: () = assert!(MAX_LINEAGE_RECORD_OWNERS_V1 == 8_192);
const _: () = assert!(MAX_LINEAGE_RECORD_FIELDS_V1 == 262_144);
const _: () = assert!(MAX_CONTRACT_LINEAGE_BYTES == 256);
const _: () = assert!(MAX_DECLARATIONS_PER_KIND.div_ceil(8) == 512);
const _: () = assert!(MAX_LINEAGE_PROOF_SHAPE_CHARGE_V1 == 1_810_703);
const _: () =
    assert!(MAX_LINEAGE_PROOF_SHAPE_CHARGE_V1 <= MAX_LINEAGE_MATERIALIZATION_PROOF_BYTES_V1);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) enum RecordOwnerV1 {
    Entity(EntityTypeId),
    Event(EventTypeId),
}

impl RecordOwnerV1 {
    #[cfg(test)]
    const fn tag(self) -> u8 {
        match self {
            Self::Entity(_) => 0x01,
            Self::Event(_) => 0x02,
        }
    }

    #[cfg(test)]
    const fn id(self) -> u32 {
        match self {
            Self::Entity(id) => id.get(),
            Self::Event(id) => id.get(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WriterRelation {
    Ancestor,
    Exact,
    Descendant,
}

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct NullFillMask {
    canonical_bits: Vec<u8>,
}

impl NullFillMask {
    pub(crate) fn semantic_bytes(&self) -> usize {
        self.canonical_bits.len()
    }

    pub(crate) fn allows(&self, position: usize) -> bool {
        self.canonical_bits
            .get(position / 8)
            .is_some_and(|byte| byte & (1 << (position % 8)) != 0)
    }

    pub(crate) fn has_canonical_shape(&self, field_count: usize) -> bool {
        if self.canonical_bits.len() != field_count.div_ceil(8)
            || !self.canonical_bits.iter().any(|byte| *byte != 0)
        {
            return false;
        }
        let used_bits = field_count % 8;
        used_bits == 0
            || self
                .canonical_bits
                .last()
                .is_some_and(|last| last & !((1u8 << used_bits) - 1) == 0)
    }
}

impl fmt::Debug for NullFillMask {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NullFillMask")
            .field("semantic_bytes", &self.semantic_bytes())
            .field("canonical_bits", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FieldIntroduction {
    field_id: FieldId,
    introduced_at_ordinal: u16,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct OwnerFieldIntroductions {
    fields: Vec<FieldIntroduction>,
}

/// Opaque process-local proof for one exact active lineage.
///
/// The accepted deterministic semantic framing charges the fixed two-byte
/// executing-ordinal width. The shared proof stores no synthetic ordinal; each
/// resolved plan separately retains the actual ordinal of its exact bundle.
pub(crate) struct LineageMaterializationProof {
    lineage: ContractLineage,
    bundles: Box<[ValidatedContractBundle]>,
    field_introductions: BTreeMap<RecordOwnerV1, OwnerFieldIntroductions>,
    canonical_bundle_bytes: usize,
    semantic_proof_bytes: usize,
}

impl LineageMaterializationProof {
    pub(crate) fn from_forward_bundles(
        bundles: Vec<ValidatedContractBundle>,
    ) -> Result<Arc<Self>, CatalogError> {
        let first = bundles
            .first()
            .ok_or_else(|| CatalogError::new(CatalogErrorKind::ActiveCatalogMismatch))?;
        let lineage = first.lineage().clone();
        let mut budget = LineageBudget::default();

        for bundle in &bundles {
            budget.push_bundle(bundle.bundle().canonical_bytes().len())?;
            if bundle.lineage() != &lineage {
                return Err(CatalogError::new(CatalogErrorKind::ActiveCatalogMismatch));
            }
        }

        if !is_structural_genesis(first) {
            return Err(CatalogError::new(CatalogErrorKind::ActiveCatalogMismatch));
        }
        for pair in bundles.windows(2) {
            validate_forward_edge(&pair[0], &pair[1])?;
        }

        let field_introductions = derive_field_introductions(&bundles)?;
        let semantic_proof_bytes = semantic_proof_charge(
            lineage.as_bytes().len(),
            bundles.len(),
            field_introductions.len(),
            field_introductions
                .values()
                .map(|owner| owner.fields.len())
                .try_fold(0usize, usize::checked_add)
                .ok_or_else(|| {
                    CatalogError::new(CatalogErrorKind::LineageMaterializationProofLimit)
                })?,
        )?;

        Ok(Arc::new(Self {
            lineage,
            bundles: bundles.into_boxed_slice(),
            field_introductions,
            canonical_bundle_bytes: budget.canonical_bytes,
            semantic_proof_bytes,
        }))
    }

    pub(crate) fn load_active<R: CatalogRepository>(
        repository: &R,
        pointer: &ActiveCatalogPointerV1,
    ) -> Result<Arc<Self>, CatalogError> {
        let mut reverse = Vec::new();
        let mut expected_version = pointer.contract_version();
        let mut expected_hash = pointer.bundle_hash();
        let lineage = pointer.lineage();
        let mut budget = LineageBudget::default();
        let mut visited = BTreeSet::new();

        loop {
            if reverse.len() == MAX_ACTIVE_LINEAGE_BUNDLES_V1 {
                return Err(CatalogError::new(CatalogErrorKind::LineageBundleCountLimit));
            }
            record_lineage_visit(&mut visited, expected_version, expected_hash)?;
            let stored = repository
                .read_contract_bundle(lineage, expected_version)?
                .ok_or_else(|| CatalogError::new(CatalogErrorKind::ActiveCatalogMismatch))?;
            budget.push_bundle(stored.canonical_bytes().len())?;
            let bundle = ValidatedContractBundle::from_stored(&stored)?;
            if bundle.lineage() != lineage
                || bundle.contract_version() != expected_version
                || bundle.bundle_hash() != expected_hash
            {
                return Err(CatalogError::new(CatalogErrorKind::ActiveCatalogMismatch));
            }
            let parent = bundle.bundle().parent();
            reverse.push(bundle);
            let Some(parent) = parent else {
                break;
            };
            expected_version = parent.contract_version();
            expected_hash = parent.bundle_hash();
        }

        reverse.reverse();
        Self::from_forward_bundles(reverse)
    }

    pub(crate) fn extend(
        self: &Arc<Self>,
        candidate: ValidatedContractBundle,
    ) -> Result<Arc<Self>, CatalogError> {
        let mut projected = LineageBudget {
            bundle_count: self.bundles.len(),
            canonical_bytes: self.canonical_bundle_bytes,
        };
        projected.push_bundle(candidate.bundle().canonical_bytes().len())?;
        let mut bundles = self.bundles.to_vec();
        bundles.push(candidate);
        Self::from_forward_bundles(bundles)
    }

    pub(crate) fn bundle_count(&self) -> usize {
        self.bundles.len()
    }

    pub(crate) fn terminal(&self) -> &ValidatedContractBundle {
        self.bundles
            .last()
            .expect("a lineage proof always owns at least genesis")
    }

    pub(crate) fn exact_member(
        &self,
        version: ContractVersion,
        hash: ContractBundleHash,
    ) -> Option<(u16, &ValidatedContractBundle)> {
        let index = self
            .bundles
            .binary_search_by_key(&version, ValidatedContractBundle::contract_version)
            .ok()?;
        let bundle = &self.bundles[index];
        if bundle.bundle_hash() != hash {
            return None;
        }
        let ordinal = u16::try_from(index).ok()?;
        Some((ordinal, bundle))
    }

    pub(crate) fn exact_binding_member(
        &self,
        binding: &DurableKeySchemaBindingV1,
    ) -> Option<(u16, &ValidatedContractBundle)> {
        if binding.lineage() != &self.lineage {
            return None;
        }
        self.exact_member(binding.contract_version(), binding.bundle_hash())
    }

    pub(crate) fn writer_materialization(
        &self,
        owner: RecordOwnerV1,
        writer_binding: &DurableKeySchemaBindingV1,
        executing_ordinal: u16,
    ) -> Result<(WriterRelation, Option<NullFillMask>), CatalogError> {
        let (writer_ordinal, writer) = self
            .exact_binding_member(writer_binding)
            .ok_or_else(|| CatalogError::new(CatalogErrorKind::InvalidBundle))?;
        let executing = self
            .bundles
            .get(usize::from(executing_ordinal))
            .ok_or_else(|| CatalogError::new(CatalogErrorKind::UnknownExecutablePlan))?;
        if record_schema(writer, owner).is_none() || record_schema(executing, owner).is_none() {
            return Err(CatalogError::new(CatalogErrorKind::InvalidBundle));
        }

        let relation = match writer_ordinal.cmp(&executing_ordinal) {
            std::cmp::Ordering::Less => WriterRelation::Ancestor,
            std::cmp::Ordering::Equal => WriterRelation::Exact,
            std::cmp::Ordering::Greater => WriterRelation::Descendant,
        };
        if relation != WriterRelation::Ancestor {
            return Ok((relation, None));
        }

        let executing_schema = record_schema(executing, owner)
            .ok_or_else(|| CatalogError::new(CatalogErrorKind::InvalidBundle))?;
        let mut canonical_bits = vec![0u8; executing_schema.fields().len().div_ceil(8)];
        if canonical_bits.len() > 512 {
            return Err(CatalogError::new(
                CatalogErrorKind::LineageMaterializationProofLimit,
            ));
        }
        let introductions = self.field_introductions.get(&owner);
        for (position, schema_field) in executing_schema.fields().iter().enumerate() {
            let should_fill = introductions.is_some_and(|owner| {
                owner
                    .fields
                    .binary_search_by_key(&schema_field.id(), |field| field.field_id)
                    .ok()
                    .is_some_and(|index| {
                        let field = owner.fields[index];
                        field.introduced_at_ordinal > writer_ordinal
                            && field.introduced_at_ordinal <= executing_ordinal
                    })
            });
            if should_fill {
                canonical_bits[position / 8] |= 1 << (position % 8);
            }
        }
        let mask = canonical_bits
            .iter()
            .any(|byte| *byte != 0)
            .then_some(NullFillMask { canonical_bits });
        Ok((relation, mask))
    }
}

impl fmt::Debug for LineageMaterializationProof {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LineageMaterializationProof")
            .field("lineage", &"[REDACTED]")
            .field("bundle_count", &self.bundles.len())
            .field("canonical_bundle_bytes", &self.canonical_bundle_bytes)
            .field("semantic_proof_bytes", &self.semantic_proof_bytes)
            .field("bundle_identities", &"[CHECKED]")
            .field("field_introductions", &"[REDACTED]")
            .finish()
    }
}

#[derive(Default)]
pub(crate) struct LineageBudget {
    bundle_count: usize,
    canonical_bytes: usize,
}

impl LineageBudget {
    pub(crate) fn push_bundle(&mut self, canonical_bytes: usize) -> Result<(), CatalogError> {
        let next_count = self
            .bundle_count
            .checked_add(1)
            .ok_or_else(|| CatalogError::new(CatalogErrorKind::LineageBundleCountLimit))?;
        if next_count > MAX_ACTIVE_LINEAGE_BUNDLES_V1 {
            return Err(CatalogError::new(CatalogErrorKind::LineageBundleCountLimit));
        }
        let next_bytes = self
            .canonical_bytes
            .checked_add(canonical_bytes)
            .ok_or_else(|| CatalogError::new(CatalogErrorKind::LineageCanonicalBytesLimit))?;
        if next_bytes > MAX_ACTIVE_LINEAGE_CANONICAL_BYTES_V1 {
            return Err(CatalogError::new(
                CatalogErrorKind::LineageCanonicalBytesLimit,
            ));
        }
        self.bundle_count = next_count;
        self.canonical_bytes = next_bytes;
        Ok(())
    }
}

fn record_lineage_visit(
    visited: &mut BTreeSet<(ContractVersion, ContractBundleHash)>,
    version: ContractVersion,
    hash: ContractBundleHash,
) -> Result<(), CatalogError> {
    if visited.insert((version, hash)) {
        Ok(())
    } else {
        Err(CatalogError::new(CatalogErrorKind::ActiveCatalogMismatch))
    }
}

pub(crate) fn is_structural_genesis(bundle: &ValidatedContractBundle) -> bool {
    bundle.bundle().parent().is_none()
        && bundle.bundle().compatibility().overall() == CompatibilityClass::Compatible
        && bundle.bundle().compatibility().entries().is_empty()
}

fn validate_forward_edge(
    parent: &ValidatedContractBundle,
    candidate: &ValidatedContractBundle,
) -> Result<(), CatalogError> {
    let Some(parent_reference) = candidate.bundle().parent() else {
        return Err(CatalogError::new(CatalogErrorKind::ActiveCatalogMismatch));
    };
    if candidate.lineage() != parent.lineage()
        || parent_reference.contract_version() != parent.contract_version()
        || parent_reference.bundle_hash() != parent.bundle_hash()
        || validate_successor_compatibility(candidate, parent).is_err()
    {
        return Err(CatalogError::new(CatalogErrorKind::ActiveCatalogMismatch));
    }
    Ok(())
}

fn derive_field_introductions(
    bundles: &[ValidatedContractBundle],
) -> Result<BTreeMap<RecordOwnerV1, OwnerFieldIntroductions>, CatalogError> {
    let mut introductions = BTreeMap::<RecordOwnerV1, OwnerFieldIntroductions>::new();
    let mut field_count = 0usize;

    for (index, bundle) in bundles.iter().enumerate() {
        let ordinal = u16::try_from(index)
            .map_err(|_| CatalogError::new(CatalogErrorKind::LineageBundleCountLimit))?;
        let previous = index.checked_sub(1).map(|prior| &bundles[prior]);

        for entity in bundle.bundle().schema().entities() {
            register_record_fields(
                &mut introductions,
                &mut field_count,
                RecordOwnerV1::Entity(entity.id()),
                entity.record(),
                previous
                    .and_then(|prior| prior.bundle().schema().entity(entity.id()))
                    .map(riffdb_contract_ir::EntitySchema::record),
                ordinal,
            )?;
        }
        for event in bundle.bundle().schema().events() {
            register_record_fields(
                &mut introductions,
                &mut field_count,
                RecordOwnerV1::Event(event.id()),
                event.payload(),
                previous
                    .and_then(|prior| prior.bundle().schema().event(event.id()))
                    .map(riffdb_contract_ir::EventSchema::payload),
                ordinal,
            )?;
        }
    }

    Ok(introductions)
}

fn register_record_fields(
    introductions: &mut BTreeMap<RecordOwnerV1, OwnerFieldIntroductions>,
    total_fields: &mut usize,
    owner: RecordOwnerV1,
    current: &RecordSchema,
    previous: Option<&RecordSchema>,
    ordinal: u16,
) -> Result<(), CatalogError> {
    if !introductions.contains_key(&owner) && introductions.len() == MAX_LINEAGE_RECORD_OWNERS_V1 {
        return Err(CatalogError::new(
            CatalogErrorKind::LineageMaterializationProofLimit,
        ));
    }
    let entry = introductions
        .entry(owner)
        .or_insert_with(|| OwnerFieldIntroductions { fields: Vec::new() });
    for field in current.fields() {
        if entry
            .fields
            .binary_search_by_key(&field.id(), |introduction| introduction.field_id)
            .is_ok()
        {
            continue;
        }
        if previous.is_some_and(|record| record.field(field.id()).is_some()) {
            return Err(CatalogError::new(CatalogErrorKind::ActiveCatalogMismatch));
        }
        if previous.is_some() && !field.value_type().is_optional() {
            return Err(CatalogError::new(CatalogErrorKind::ActiveCatalogMismatch));
        }
        if *total_fields == MAX_LINEAGE_RECORD_FIELDS_V1 {
            return Err(CatalogError::new(
                CatalogErrorKind::LineageMaterializationProofLimit,
            ));
        }
        entry.fields.push(FieldIntroduction {
            field_id: field.id(),
            introduced_at_ordinal: ordinal,
        });
        entry.fields.sort_unstable_by_key(|field| field.field_id);
        *total_fields = total_fields
            .checked_add(1)
            .ok_or_else(|| CatalogError::new(CatalogErrorKind::LineageMaterializationProofLimit))?;
    }
    Ok(())
}

fn record_schema(bundle: &ValidatedContractBundle, owner: RecordOwnerV1) -> Option<&RecordSchema> {
    match owner {
        RecordOwnerV1::Entity(id) => bundle
            .bundle()
            .schema()
            .entity(id)
            .map(riffdb_contract_ir::EntitySchema::record),
        RecordOwnerV1::Event(id) => bundle
            .bundle()
            .schema()
            .event(id)
            .map(riffdb_contract_ir::EventSchema::payload),
    }
}

fn semantic_proof_charge(
    lineage_bytes: usize,
    bundle_count: usize,
    owner_count: usize,
    field_count: usize,
) -> Result<usize, CatalogError> {
    if lineage_bytes > MAX_CONTRACT_LINEAGE_BYTES {
        return Err(CatalogError::new(
            CatalogErrorKind::LineageMaterializationProofLimit,
        ));
    }
    if bundle_count > MAX_ACTIVE_LINEAGE_BUNDLES_V1 {
        return Err(CatalogError::new(CatalogErrorKind::LineageBundleCountLimit));
    }
    if owner_count > MAX_LINEAGE_RECORD_OWNERS_V1 || field_count > MAX_LINEAGE_RECORD_FIELDS_V1 {
        return Err(CatalogError::new(
            CatalogErrorKind::LineageMaterializationProofLimit,
        ));
    }
    let charge = 1usize
        .checked_add(4)
        .and_then(|value| value.checked_add(lineage_bytes))
        .and_then(|value| value.checked_add(4))
        .and_then(|value| value.checked_add(bundle_count.checked_mul(8 + 32)?))
        .and_then(|value| value.checked_add(2))
        .and_then(|value| value.checked_add(4))
        .and_then(|value| value.checked_add(owner_count.checked_mul(1 + 4 + 4)?))
        .and_then(|value| value.checked_add(field_count.checked_mul(4 + 2)?))
        .ok_or_else(|| CatalogError::new(CatalogErrorKind::LineageMaterializationProofLimit))?;
    checked_semantic_proof_charge(charge)
}

fn checked_semantic_proof_charge(charge: usize) -> Result<usize, CatalogError> {
    if charge > MAX_LINEAGE_MATERIALIZATION_PROOF_BYTES_V1 {
        return Err(CatalogError::new(
            CatalogErrorKind::LineageMaterializationProofLimit,
        ));
    }
    Ok(charge)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use riffdb_contract_compiler::{compile_contract_source, compile_contract_successor};
    use riffdb_contract_ir::{CompatibilityReport, ContractBundle, ParentBundleRef};
    use riffdb_storage_api::{ExecutablePlanRef, StorageError, StoredContractBundleV1};
    use riffdb_types::hash_contract_bundle;

    use crate::{ActiveCatalogSnapshot, CatalogPreparationResult, prepare_catalog_activation};

    const EMPTY_LINEAGE_SOURCE: &str = r#"
contract LineageBoundary version 1 {
}
"#;

    struct BoundaryRepository {
        active: ActiveCatalogPointerV1,
        bundles: BTreeMap<ContractVersion, StoredContractBundleV1>,
    }

    impl BoundaryRepository {
        fn at(
            lineage: &[ValidatedContractBundle],
            active_index: usize,
        ) -> Result<Self, CatalogError> {
            let active = lineage
                .get(active_index)
                .ok_or_else(|| CatalogError::new(CatalogErrorKind::ActiveCatalogMismatch))?
                .to_stored()?;
            let bundles = lineage
                .iter()
                .take(active_index + 1)
                .map(|bundle| {
                    bundle
                        .to_stored()
                        .map(|stored| (stored.contract_version(), stored))
                })
                .collect::<Result<_, _>>()?;
            Ok(Self {
                active: ActiveCatalogPointerV1::from_bundle(&active),
                bundles,
            })
        }
    }

    impl CatalogRepository for BoundaryRepository {
        fn read_active_catalog(&self) -> Result<Option<ActiveCatalogPointerV1>, StorageError> {
            Ok(Some(self.active.clone()))
        }

        fn read_contract_bundle(
            &self,
            lineage: &ContractLineage,
            contract_version: ContractVersion,
        ) -> Result<Option<StoredContractBundleV1>, StorageError> {
            Ok(self
                .bundles
                .get(&contract_version)
                .filter(|bundle| bundle.lineage() == lineage)
                .cloned())
        }
    }

    fn unchanged_successor(parent: &ContractBundle, version: u64) -> ContractBundle {
        ContractBundle::new(
            parent.compiler_version(),
            parent.lineage().clone(),
            ContractVersion::new(version).expect("positive boundary version"),
            Some(ParentBundleRef::new(
                parent.contract_version(),
                parent.bundle_hash(),
            )),
            parent.source_hash(),
            parent.ledger().clone(),
            parent.schema().clone(),
            parent.commands().to_vec(),
            parent.projections().to_vec(),
            parent.schema_artifacts().to_vec(),
            parent.mcp_command_names().clone(),
            CompatibilityReport::successor(Vec::new()).expect("no semantic change"),
        )
        .expect("unchanged valid successor")
    }

    fn boundary_lineage(bundle_count: usize) -> Vec<ValidatedContractBundle> {
        assert!(bundle_count > 0);
        let genesis = compile_contract_source(EMPTY_LINEAGE_SOURCE).expect("empty genesis");
        let mut bundles = Vec::with_capacity(bundle_count);
        bundles.push(
            ValidatedContractBundle::from_compiler_bundle(genesis.clone())
                .expect("checked genesis"),
        );
        let mut parent = genesis;
        for version in 2..=u64::try_from(bundle_count).expect("bounded fixture length") {
            let successor = unchanged_successor(&parent, version);
            bundles.push(
                ValidatedContractBundle::from_compiler_bundle(successor.clone())
                    .expect("checked successor"),
            );
            parent = successor;
        }
        bundles
    }

    fn source(version: u64, entity_note: bool, event_note: bool, event_tag: bool) -> String {
        let entity_note = if entity_note {
            "field note: optional<string<8>>"
        } else {
            ""
        };
        let event_note = if event_note {
            "note: optional<string<8>>"
        } else {
            ""
        };
        let event_tag = if event_tag {
            "tag: optional<string<8>>"
        } else {
            ""
        };
        format!(
            r#"
contract LineageProof version {version} {{
  entity Row {{ key (id: uuid) field value: i64 }}
  entity Hidden {{ key (id: uuid) field value: i64 {entity_note} }}
  event Changed {{ id: uuid {event_note} {event_tag} }}
  aggregate Rows {{ root Row partition_by id conflict_key (id) }}
  aggregate HiddenRows {{ root Hidden partition_by id conflict_key (id) }}
  command Change {{
    input idempotency_key: string<128>
    input id: uuid
    idempotency_key idempotency_key
    mutate Row(id) as row else Missing {{ id: id }}
    set row.value = 1
    emit Changed {{ id: id }}
    return ChangedOutcome {{ id: id }}
  }}
}}
"#
        )
    }

    fn checked_lineage() -> Vec<ValidatedContractBundle> {
        let first = compile_contract_source(&source(1, false, false, false)).expect("genesis");
        let second = compile_contract_successor(&source(7, true, true, false), &first)
            .expect("first additive successor");
        let third = compile_contract_successor(&source(42, true, true, true), &second)
            .expect("second additive successor");
        [first, second, third]
            .into_iter()
            .map(|bundle| {
                ValidatedContractBundle::from_compiler_bundle(bundle).expect("catalog bundle")
            })
            .collect()
    }

    fn binding(bundle: &ValidatedContractBundle) -> DurableKeySchemaBindingV1 {
        DurableKeySchemaBindingV1::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
        )
    }

    #[test]
    fn skipped_versions_and_optional_introductions_produce_exact_masks() {
        let proof = LineageMaterializationProof::from_forward_bundles(checked_lineage())
            .expect("exact lineage");
        assert_eq!(proof.bundle_count(), 3);

        let genesis = &proof.bundles[0];
        let second = &proof.bundles[1];
        let third = &proof.bundles[2];
        let hidden = third.bundle().schema().entities()[1].id();
        let event = third.bundle().schema().events()[0].id();

        let (relation, mask) = proof
            .writer_materialization(RecordOwnerV1::Entity(hidden), &binding(genesis), 2)
            .expect("ancestor entity");
        assert_eq!(relation, WriterRelation::Ancestor);
        let mask = mask.expect("one entity introduction");
        assert_eq!(mask.semantic_bytes(), 1);
        assert_eq!(mask.canonical_bits, vec![0b0000_0100]);

        let (relation, mask) = proof
            .writer_materialization(RecordOwnerV1::Event(event), &binding(genesis), 2)
            .expect("ancestor event");
        assert_eq!(relation, WriterRelation::Ancestor);
        let mask = mask.expect("two event introductions");
        assert_eq!(mask.semantic_bytes(), 1);
        assert_eq!(mask.canonical_bits, vec![0b0000_0110]);

        let (relation, mask) = proof
            .writer_materialization(RecordOwnerV1::Event(event), &binding(second), 1)
            .expect("same writer");
        assert_eq!(relation, WriterRelation::Exact);
        assert!(mask.is_none(), "same-version omission cannot be filled");

        let (relation, mask) = proof
            .writer_materialization(RecordOwnerV1::Event(event), &binding(third), 0)
            .expect("descendant writer");
        assert_eq!(relation, WriterRelation::Descendant);
        assert!(mask.is_none());

        let wrong_lineage = DurableKeySchemaBindingV1::new(
            ContractLineage::new("OtherLineage").expect("lineage"),
            genesis.contract_version(),
            genesis.bundle_hash(),
        );
        assert!(proof.exact_binding_member(&wrong_lineage).is_none());
        assert_eq!(
            proof
                .writer_materialization(RecordOwnerV1::Event(event), &wrong_lineage, 2)
                .expect_err("writer lineage is part of the exact identity")
                .kind(),
            CatalogErrorKind::InvalidBundle
        );
    }

    #[test]
    fn exact_parent_hash_and_every_forward_edge_are_required() {
        let valid = checked_lineage();
        assert!(LineageMaterializationProof::from_forward_bundles(valid.clone()).is_ok());
        assert_eq!(
            LineageMaterializationProof::from_forward_bundles(vec![
                valid[0].clone(),
                valid[0].clone(),
            ])
            .expect_err("repeated bundle")
            .kind(),
            CatalogErrorKind::ActiveCatalogMismatch
        );
        assert_eq!(
            LineageMaterializationProof::from_forward_bundles(vec![
                valid[0].clone(),
                valid[2].clone(),
            ])
            .expect_err("missing exact parent")
            .kind(),
            CatalogErrorKind::ActiveCatalogMismatch
        );

        let proof = LineageMaterializationProof::from_forward_bundles(valid.clone())
            .expect("complete proof");
        assert!(
            proof
                .exact_member(
                    valid[1].contract_version(),
                    ContractBundleHash::from_bytes([0x77; 32]),
                )
                .is_none(),
            "a version match cannot substitute a different hash"
        );

        let command = valid[0].bundle().commands().first().expect("command");
        let reference = ExecutablePlanRef::new(
            valid[0].lineage().clone(),
            valid[0].contract_version(),
            valid[0].bundle_hash(),
            command.command_id(),
            command.plan_hash(),
        );
        assert_eq!(
            valid[0]
                .resolve_plan_with_proof(&reference, Arc::clone(&proof), 1)
                .expect_err("ordinal cannot name a different proof member")
                .kind(),
            CatalogErrorKind::UnknownExecutablePlan
        );

        let mut visited = BTreeSet::new();
        record_lineage_visit(
            &mut visited,
            valid[0].contract_version(),
            valid[0].bundle_hash(),
        )
        .expect("first visit");
        assert_eq!(
            record_lineage_visit(
                &mut visited,
                valid[0].contract_version(),
                valid[0].bundle_hash(),
            )
            .expect_err("cycle/repeat detection")
            .kind(),
            CatalogErrorKind::ActiveCatalogMismatch
        );

        let unrelated = compile_contract_source(&source(1, false, false, false))
            .expect("same deterministic genesis");
        let wrong_parent_source = source(42, true, true, true);
        let wrong_parent =
            compile_contract_successor(&wrong_parent_source, &unrelated).expect("branch bundle");
        let wrong_parent =
            ValidatedContractBundle::from_compiler_bundle(wrong_parent).expect("checked branch");
        assert_eq!(
            LineageMaterializationProof::from_forward_bundles(vec![
                valid[0].clone(),
                valid[1].clone(),
                wrong_parent,
            ])
            .expect_err("wrong exact parent hash")
            .kind(),
            CatalogErrorKind::ActiveCatalogMismatch
        );
    }

    #[test]
    fn count_byte_and_semantic_charge_boundaries_use_checked_arithmetic() {
        let mut combined = LineageBudget::default();
        for _ in 0..MAX_ACTIVE_LINEAGE_BUNDLES_V1 {
            combined
                .push_bundle(MAX_ACTIVE_LINEAGE_CANONICAL_BYTES_V1 / MAX_ACTIVE_LINEAGE_BUNDLES_V1)
                .expect("exact combined bundle-count and canonical-byte limits");
        }
        assert_eq!(combined.bundle_count, MAX_ACTIVE_LINEAGE_BUNDLES_V1);
        assert_eq!(
            combined.canonical_bytes,
            MAX_ACTIVE_LINEAGE_CANONICAL_BYTES_V1
        );
        assert_eq!(
            combined.push_bundle(0).expect_err("one bundle over").kind(),
            CatalogErrorKind::LineageBundleCountLimit
        );

        // This is the production cumulative preflight used by both activation and
        // startup. Keep one bundle slot free so cap + 1 selects the byte error.
        let mut bytes = LineageBudget::default();
        let pre_cap_count = MAX_ACTIVE_LINEAGE_BUNDLES_V1 - 1;
        let equal_share = MAX_ACTIVE_LINEAGE_CANONICAL_BYTES_V1 / pre_cap_count;
        for _ in 0..pre_cap_count - 1 {
            bytes
                .push_bundle(equal_share)
                .expect("bounded canonical-byte share");
        }
        let remainder = MAX_ACTIVE_LINEAGE_CANONICAL_BYTES_V1 - equal_share * (pre_cap_count - 1);
        bytes
            .push_bundle(remainder)
            .expect("exact cumulative canonical byte count");
        assert_eq!(bytes.bundle_count, pre_cap_count);
        assert_eq!(bytes.canonical_bytes, MAX_ACTIVE_LINEAGE_CANONICAL_BYTES_V1);
        assert_eq!(
            bytes.push_bundle(1).expect_err("one byte over").kind(),
            CatalogErrorKind::LineageCanonicalBytesLimit
        );

        let maximum_shape = semantic_proof_charge(256, 4_096, 8_192, 262_144)
            .expect("accepted maximum semantic shape");
        assert_eq!(maximum_shape, 1_810_703);
        assert_eq!(
            MAX_LINEAGE_MATERIALIZATION_PROOF_BYTES_V1 - maximum_shape,
            286_449
        );
        assert_eq!(
            semantic_proof_charge(256, 4_096, 8_192, 310_000)
                .expect_err("field constituent exceeds its independent ceiling")
                .kind(),
            CatalogErrorKind::LineageMaterializationProofLimit
        );
        assert_eq!(
            checked_semantic_proof_charge(MAX_LINEAGE_MATERIALIZATION_PROOF_BYTES_V1)
                .expect("exact raw semantic proof cap"),
            MAX_LINEAGE_MATERIALIZATION_PROOF_BYTES_V1
        );
        assert_eq!(
            checked_semantic_proof_charge(MAX_LINEAGE_MATERIALIZATION_PROOF_BYTES_V1 + 1)
                .expect_err("one byte above raw semantic proof cap")
                .kind(),
            CatalogErrorKind::LineageMaterializationProofLimit
        );
        assert_eq!(
            semantic_proof_charge(257, 4_096, 8_192, 262_144)
                .expect_err("lineage bytes are independently bounded")
                .kind(),
            CatalogErrorKind::LineageMaterializationProofLimit
        );
        assert_eq!(
            semantic_proof_charge(256, 4_097, 8_192, 262_144)
                .expect_err("bundle count is independently bounded")
                .kind(),
            CatalogErrorKind::LineageBundleCountLimit
        );
        assert_eq!(
            semantic_proof_charge(256, 4_096, 8_193, 262_144)
                .expect_err("owner count is independently bounded")
                .kind(),
            CatalogErrorKind::LineageMaterializationProofLimit
        );
    }

    #[test]
    fn startup_and_activation_wire_the_exact_bundle_count_boundary() {
        let lineage = boundary_lineage(MAX_ACTIVE_LINEAGE_BUNDLES_V1 + 1);

        let before_limit = BoundaryRepository::at(&lineage, MAX_ACTIVE_LINEAGE_BUNDLES_V1 - 2)
            .expect("repository before limit");
        let active_before_limit = ActiveCatalogSnapshot::read(&before_limit)
            .expect("bounded startup")
            .expect("active catalog");
        let candidate_at_limit = lineage[MAX_ACTIVE_LINEAGE_BUNDLES_V1 - 1].bundle().clone();
        let expected_before_limit = active_before_limit.pointer().contract_version();
        let CatalogPreparationResult::Prepared(at_limit) = prepare_catalog_activation(
            candidate_at_limit,
            Some(expected_before_limit),
            Some(&active_before_limit),
        )
        .expect("activation reaching exactly 4,096 bundles") else {
            panic!("exact expected version must prepare");
        };
        assert!(
            format!("{at_limit:?}").contains("projected_lineage_bundle_count: 4096"),
            "the prepared activation must retain the checked projected lineage"
        );
        drop(at_limit);
        drop(active_before_limit);
        drop(before_limit);

        let at_limit_repository =
            BoundaryRepository::at(&lineage, MAX_ACTIVE_LINEAGE_BUNDLES_V1 - 1)
                .expect("repository at limit");
        let active_at_limit = ActiveCatalogSnapshot::read(&at_limit_repository)
            .expect("startup accepts exactly 4,096 bundles")
            .expect("active catalog");
        let over_limit = prepare_catalog_activation(
            lineage[MAX_ACTIVE_LINEAGE_BUNDLES_V1].bundle().clone(),
            Some(active_at_limit.pointer().contract_version()),
            Some(&active_at_limit),
        )
        .expect_err("activation must reject bundle 4,097");
        assert_eq!(over_limit.kind(), CatalogErrorKind::LineageBundleCountLimit);
        drop(active_at_limit);
        drop(at_limit_repository);

        let over_limit_repository = BoundaryRepository::at(&lineage, MAX_ACTIVE_LINEAGE_BUNDLES_V1)
            .expect("repository over limit");
        assert_eq!(
            ActiveCatalogSnapshot::read(&over_limit_repository)
                .expect_err("startup must reject 4,097 bundles")
                .kind(),
            CatalogErrorKind::InvalidHistoricalEvidence
        );
    }

    #[test]
    fn startup_rejects_parent_hash_substitution_and_cycle_shaped_bytes() {
        let valid = checked_lineage();
        let branch_compiled =
            compile_contract_successor(&source(7, false, true, false), valid[0].bundle())
                .expect("compatible same-version branch");
        let branch =
            ValidatedContractBundle::from_compiler_bundle(branch_compiled).expect("checked branch");
        assert_ne!(branch.bundle_hash(), valid[1].bundle_hash());

        let mut substituted = BoundaryRepository::at(&valid, 2).expect("complete repository");
        substituted.bundles.insert(
            branch.contract_version(),
            branch.to_stored().expect("stored branch"),
        );
        assert_eq!(
            ActiveCatalogSnapshot::read(&substituted)
                .expect_err("same version with another hash is not the exact parent")
                .kind(),
            CatalogErrorKind::InvalidHistoricalEvidence
        );

        let successor = &valid[1];
        let mut cycle_bytes = successor.bundle().canonical_bytes().to_vec();
        let parent = successor.bundle().parent().expect("successor parent");
        let mut exact_parent = Vec::with_capacity(40);
        exact_parent.extend_from_slice(&parent.contract_version().get().to_be_bytes());
        exact_parent.extend_from_slice(parent.bundle_hash().as_bytes());
        let parent_offset = cycle_bytes
            .windows(exact_parent.len())
            .position(|window| window == exact_parent)
            .expect("canonical parent reference");
        cycle_bytes[parent_offset..parent_offset + 8]
            .copy_from_slice(&successor.contract_version().get().to_be_bytes());
        let cycle_hash = hash_contract_bundle(&cycle_bytes);
        let cycle = StoredContractBundleV1::new(
            successor.lineage().clone(),
            successor.contract_version(),
            cycle_hash,
            cycle_bytes,
        )
        .expect("bounded cycle-shaped storage bytes");
        let cycle_repository = BoundaryRepository {
            active: ActiveCatalogPointerV1::from_bundle(&cycle),
            bundles: BTreeMap::from([(cycle.contract_version(), cycle)]),
        };
        assert_eq!(
            ActiveCatalogSnapshot::read(&cycle_repository)
                .expect_err("IR decoding rejects a non-decreasing parent before traversal")
                .kind(),
            CatalogErrorKind::InvalidBundle
        );
    }

    #[test]
    fn proof_debug_never_exposes_bundles_or_field_registry() {
        let proof = LineageMaterializationProof::from_forward_bundles(checked_lineage())
            .expect("exact lineage");
        let debug = format!("{proof:?}");
        assert!(debug.contains("bundle_identities: \"[CHECKED]\""));
        assert!(debug.contains("field_introductions: \"[REDACTED]\""));
        assert!(!debug.contains("canonical_bytes"));
        assert!(!debug.contains("ChangedOutcome"));
        assert!(!debug.contains("LineageProof"));
    }

    #[test]
    fn semantic_framing_version_and_big_endian_widths_are_frozen() {
        assert_eq!(LINEAGE_MATERIALIZATION_PROOF_VERSION_V1, 1);
        assert_eq!(u32::to_be_bytes(256).len(), 4);
        assert_eq!(u64::to_be_bytes(42).len(), 8);
        assert_eq!(u16::to_be_bytes(4_095).len(), 2);
        assert_eq!(RecordOwnerV1::Entity(EntityTypeId::first()).tag(), 0x01);
        assert_eq!(RecordOwnerV1::Event(EventTypeId::first()).tag(), 0x02);
        assert_eq!(RecordOwnerV1::Entity(EntityTypeId::first()).id(), 1);
    }

    #[test]
    fn optional_type_predicate_used_by_structural_derivation_is_exact() {
        assert!(
            riffdb_contract_ir::ValueType::optional(riffdb_contract_ir::ValueType::i64())
                .expect("optional")
                .is_optional()
        );
        assert!(!riffdb_contract_ir::ValueType::i64().is_optional());
    }
}
