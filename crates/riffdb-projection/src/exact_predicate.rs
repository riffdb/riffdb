//! Indexed exact-predicate and independent-order provider state (ADR-0134).

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::num::NonZeroU16;

use riffdb_query_ir::{
    ExactComparisonProfileV1, ExactOrderDirectionV1, ExactOrderProgramV1, ExactOrderProgramV2,
    ExactParameterValueV1, ExactPredicateFamilyMemberV1, ExactPredicateLeafV1,
    ExactPredicateNodeV1, ExactPredicateOperatorV1, ExactPredicateProgramV1,
    ExactPredicateProgramV2, ExactReferenceCellV1, ExactScalarV1, ExactStatePlacementV1,
    ExactValueSlotV1,
};
use riffdb_types::{
    ApplicationRoleHash, CanonicalRecord, CommitSequence, EntityKey, FieldId, HashDomain,
    PartitionKeyHash, ProjectionGeneration, ProjectionProviderDescriptorHash, QueryPlanHash,
    decode_canonical_value, encode_canonical_value, hash,
};

/// Additive provider-state format; V1 through V3 remain independently readable.
pub const EXACT_PREDICATE_PROVIDER_STATE_FORMAT_V4: u16 = 4;
/// Additive nullable exact-order provider-state format.
pub const EXACT_PREDICATE_PROVIDER_STATE_FORMAT_V5: u16 = 5;
/// Strict checkpoint ceiling for one bounded partition.
pub const MAX_EXACT_PREDICATE_CHECKPOINT_BYTES_V4: usize = 256 * 1024 * 1024;
/// Strict V5 checkpoint ceiling for one bounded partition.
pub const MAX_EXACT_PREDICATE_CHECKPOINT_BYTES_V5: usize = MAX_EXACT_PREDICATE_CHECKPOINT_BYTES_V4;
const CHECKPOINT_MAGIC: &[u8; 4] = b"RXPA";
const CHECKSUM_BYTES: usize = 32;

/// Immutable state binding beyond the provider-independent semantic program.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExactPredicateProviderBindingV1 {
    plan: QueryPlanHash,
    descriptor: ProjectionProviderDescriptorHash,
    policy_shape: ApplicationRoleHash,
    partition: PartitionKeyHash,
    history_incarnation: u64,
    generation: ProjectionGeneration,
    frontier: CommitSequence,
}

impl ExactPredicateProviderBindingV1 {
    /// Binds a physical generation to one compiler plan and authorized universe.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        plan: QueryPlanHash,
        program: &ExactPredicateProgramV1,
        policy_shape: ApplicationRoleHash,
        partition: PartitionKeyHash,
        history_incarnation: u64,
        generation: ProjectionGeneration,
        frontier: CommitSequence,
    ) -> Result<Self, ExactPredicateProviderErrorV1> {
        if history_incarnation == 0 {
            return Err(ExactPredicateProviderErrorV1::BindingMismatch);
        }
        let descriptor = program
            .provider_descriptor()
            .map_err(|_| ExactPredicateProviderErrorV1::BindingMismatch)?
            .digest();
        Ok(Self {
            plan,
            descriptor,
            policy_shape,
            partition,
            history_incarnation,
            generation,
            frontier,
        })
    }

    /// Compiler-owned query plan identity.
    #[must_use]
    pub const fn plan(self) -> QueryPlanHash {
        self.plan
    }

    /// Complete derived provider descriptor.
    #[must_use]
    pub const fn descriptor(self) -> ProjectionProviderDescriptorHash {
        self.descriptor
    }

    /// Policy shape used to form the complete candidate universe.
    #[must_use]
    pub const fn policy_shape(self) -> ApplicationRoleHash {
        self.policy_shape
    }

    /// Policy-aligned partition.
    #[must_use]
    pub const fn partition(self) -> PartitionKeyHash {
        self.partition
    }

    /// Authoritative history incarnation.
    #[must_use]
    pub const fn history_incarnation(self) -> u64 {
        self.history_incarnation
    }

    /// Never-reused provider generation.
    #[must_use]
    pub const fn generation(self) -> ProjectionGeneration {
        self.generation
    }

    /// Exact applied frontier represented by this state.
    #[must_use]
    pub const fn frontier(self) -> CommitSequence {
        self.frontier
    }

    fn advanced(self, frontier: CommitSequence) -> Result<Self, ExactPredicateProviderErrorV1> {
        if frontier <= self.frontier {
            return Err(ExactPredicateProviderErrorV1::NonAdvancingEpoch);
        }
        Ok(Self { frontier, ..self })
    }
}

/// Immutable V5 state binding for a nullable exact-order semantic program.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExactPredicateProviderBindingV2 {
    plan: QueryPlanHash,
    program: QueryPlanHash,
    descriptor: ProjectionProviderDescriptorHash,
    policy_shape: ApplicationRoleHash,
    partition: PartitionKeyHash,
    history_incarnation: u64,
    generation: ProjectionGeneration,
    frontier: CommitSequence,
}

impl ExactPredicateProviderBindingV2 {
    /// Binds one V5 generation to its compiler-owned nullable order and universe.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        plan: QueryPlanHash,
        program: &ExactPredicateProgramV2,
        policy_shape: ApplicationRoleHash,
        partition: PartitionKeyHash,
        history_incarnation: u64,
        generation: ProjectionGeneration,
        frontier: CommitSequence,
    ) -> Result<Self, ExactPredicateProviderErrorV1> {
        if history_incarnation == 0 {
            return Err(ExactPredicateProviderErrorV1::BindingMismatch);
        }
        let descriptor = program
            .provider_descriptor()
            .map_err(|_| ExactPredicateProviderErrorV1::BindingMismatch)?
            .digest();
        Ok(Self {
            plan,
            program: program.identity(),
            descriptor,
            policy_shape,
            partition,
            history_incarnation,
            generation,
            frontier,
        })
    }

    /// Compiler-owned query plan identity.
    #[must_use]
    pub const fn plan(self) -> QueryPlanHash {
        self.plan
    }

    /// Provider-independent nullable semantic-program identity.
    #[must_use]
    pub const fn program(self) -> QueryPlanHash {
        self.program
    }

    /// Complete V5 provider descriptor.
    #[must_use]
    pub const fn descriptor(self) -> ProjectionProviderDescriptorHash {
        self.descriptor
    }

    /// Policy shape used for admission.
    #[must_use]
    pub const fn policy_shape(self) -> ApplicationRoleHash {
        self.policy_shape
    }

    /// Policy-aligned partition.
    #[must_use]
    pub const fn partition(self) -> PartitionKeyHash {
        self.partition
    }

    /// Authoritative history incarnation.
    #[must_use]
    pub const fn history_incarnation(self) -> u64 {
        self.history_incarnation
    }

    /// Never-reused provider generation.
    #[must_use]
    pub const fn generation(self) -> ProjectionGeneration {
        self.generation
    }

    /// Exact applied frontier represented by this state.
    #[must_use]
    pub const fn frontier(self) -> CommitSequence {
        self.frontier
    }

    fn advanced(self, frontier: CommitSequence) -> Result<Self, ExactPredicateProviderErrorV1> {
        if frontier <= self.frontier {
            return Err(ExactPredicateProviderErrorV1::NonAdvancingEpoch);
        }
        Ok(Self { frontier, ..self })
    }
}

/// One complete, already-authorized provider source row.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactPredicateProviderRowV1 {
    key: EntityKey,
    fields: BTreeMap<FieldId, ExactReferenceCellV1>,
    output: CanonicalRecord,
}

impl ExactPredicateProviderRowV1 {
    /// Constructs one row without inferring missing fields from output shape.
    #[must_use]
    pub fn new(
        key: EntityKey,
        fields: BTreeMap<FieldId, ExactReferenceCellV1>,
        output: CanonicalRecord,
    ) -> Self {
        Self {
            key,
            fields,
            output,
        }
    }

    /// Canonical authoritative key.
    #[must_use]
    pub const fn key(&self) -> &EntityKey {
        &self.key
    }

    /// Complete compiler-addressed field states used by this provider.
    #[must_use]
    pub const fn fields(&self) -> &BTreeMap<FieldId, ExactReferenceCellV1> {
        &self.fields
    }

    /// Compiler-shaped, already-authorized output record.
    #[must_use]
    pub const fn output(&self) -> &CanonicalRecord {
        &self.output
    }
}

/// One checked row mutation applied atomically at a later provider frontier.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExactPredicateIndexMutationV1 {
    /// Insert or replace a complete authorized row post-image.
    Upsert(ExactPredicateProviderRowV1),
    /// Remove the row from the authorized universe and every index.
    Delete(EntityKey),
}

impl ExactPredicateIndexMutationV1 {
    fn key(&self) -> &EntityKey {
        match self {
            Self::Upsert(row) => &row.key,
            Self::Delete(key) => key,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct SparseFieldIndexV1 {
    profile: ExactComparisonProfileV1,
    missing: Vec<usize>,
    null: Vec<usize>,
    present: Vec<usize>,
    values: BTreeMap<ExactScalarV1, Vec<usize>>,
    text: BTreeMap<Vec<u8>, Vec<usize>>,
    reversed_text: BTreeMap<Vec<u8>, Vec<usize>>,
    suffixes: BTreeMap<Vec<u8>, Vec<usize>>,
}

impl SparseFieldIndexV1 {
    fn new(profile: ExactComparisonProfileV1) -> Self {
        Self {
            profile,
            missing: Vec::new(),
            null: Vec::new(),
            present: Vec::new(),
            values: BTreeMap::new(),
            text: BTreeMap::new(),
            reversed_text: BTreeMap::new(),
            suffixes: BTreeMap::new(),
        }
    }

    fn insert(
        &mut self,
        position: usize,
        cell: &ExactReferenceCellV1,
    ) -> Result<(), ExactPredicateProviderErrorV1> {
        match cell {
            ExactReferenceCellV1::Missing => self.missing.push(position),
            ExactReferenceCellV1::Null => self.null.push(position),
            ExactReferenceCellV1::Value(value) => {
                if value.profile() != self.profile {
                    return Err(ExactPredicateProviderErrorV1::TypeMismatch);
                }
                self.present.push(position);
                self.values.entry(value.clone()).or_default().push(position);
                if let ExactScalarV1::String(value) = value {
                    let bytes = value.as_bytes();
                    self.text.entry(bytes.to_vec()).or_default().push(position);
                    let mut reversed = bytes.to_vec();
                    reversed.reverse();
                    self.reversed_text
                        .entry(reversed)
                        .or_default()
                        .push(position);
                    for start in 0..bytes.len() {
                        self.suffixes
                            .entry(bytes[start..].to_vec())
                            .or_default()
                            .push(position);
                    }
                }
            }
        }
        Ok(())
    }

    fn estimated_bytes(&self) -> Result<u64, ExactPredicateProviderErrorV1> {
        let mut bytes = 0_u64;
        for positions in [&self.missing, &self.null, &self.present] {
            bytes = bytes
                .checked_add((positions.len() as u64).saturating_mul(8))
                .ok_or(ExactPredicateProviderErrorV1::BoundExceeded)?;
        }
        for (value, positions) in &self.values {
            bytes = bytes
                .checked_add(estimated_scalar_bytes(value))
                .and_then(|value| value.checked_add((positions.len() as u64).saturating_mul(8)))
                .ok_or(ExactPredicateProviderErrorV1::BoundExceeded)?;
        }
        for map in [&self.text, &self.reversed_text, &self.suffixes] {
            for (key, positions) in map {
                bytes = bytes
                    .checked_add(key.len() as u64)
                    .and_then(|value| value.checked_add((positions.len() as u64).saturating_mul(8)))
                    .ok_or(ExactPredicateProviderErrorV1::BoundExceeded)?;
            }
        }
        Ok(bytes)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ExactOrderIndexV1 {
    rows: Vec<usize>,
    fields: BTreeMap<FieldId, SparseFieldIndexV1>,
}

/// One complete exact page selected from rank summaries under one epoch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactPredicateResultPageV1 {
    rows: Vec<ExactPredicateProviderRowV1>,
    exact_total: u64,
    binding: ExactPredicateProviderBindingV1,
    member: ExactPredicateFamilyMemberV1,
}

/// One V5 nullable-order page and exact count from a single bound epoch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactPredicateResultPageV2 {
    rows: Vec<ExactPredicateProviderRowV1>,
    exact_total: u64,
    binding: ExactPredicateProviderBindingV2,
    member: ExactPredicateFamilyMemberV1,
}

impl ExactPredicateResultPageV2 {
    /// Only the requested bounded page is released.
    #[must_use]
    pub fn rows(&self) -> &[ExactPredicateProviderRowV1] {
        &self.rows
    }

    /// Whole authorized-result cardinality before the ordinal window.
    #[must_use]
    pub const fn exact_total(&self) -> u64 {
        self.exact_total
    }

    /// Complete V5 plan/policy/partition/generation/frontier binding.
    #[must_use]
    pub const fn binding(&self) -> ExactPredicateProviderBindingV2 {
        self.binding
    }

    /// Exact compiler-enumerated family member.
    #[must_use]
    pub const fn member(&self) -> ExactPredicateFamilyMemberV1 {
        self.member
    }
}

impl ExactPredicateResultPageV1 {
    /// Only the requested bounded page is released.
    #[must_use]
    pub fn rows(&self) -> &[ExactPredicateProviderRowV1] {
        &self.rows
    }

    /// Whole authorized-result cardinality before the ordinal window.
    #[must_use]
    pub const fn exact_total(&self) -> u64 {
        self.exact_total
    }

    /// Complete plan/policy/partition/generation/frontier binding.
    #[must_use]
    pub const fn binding(&self) -> ExactPredicateProviderBindingV1 {
        self.binding
    }

    /// Exact compiler-enumerated family member.
    #[must_use]
    pub const fn member(&self) -> ExactPredicateFamilyMemberV1 {
        self.member
    }
}

/// Rebuildable V4 provider state. Indexed structures are derived from canonical rows.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactPredicatePartitionIndexV4 {
    binding: ExactPredicateProviderBindingV1,
    program: ExactPredicateProgramV1,
    rows: Vec<ExactPredicateProviderRowV1>,
    orders: Vec<ExactOrderIndexV1>,
}

impl ExactPredicatePartitionIndexV4 {
    /// Rebuilds one disjoint generation after row-policy admission has completed.
    pub fn rebuild(
        binding: ExactPredicateProviderBindingV1,
        program: ExactPredicateProgramV1,
        rows: Vec<ExactPredicateProviderRowV1>,
    ) -> Result<Self, ExactPredicateProviderErrorV1> {
        if binding.descriptor
            != program
                .provider_descriptor()
                .map_err(|_| ExactPredicateProviderErrorV1::BindingMismatch)?
                .digest()
            || rows.len()
                > usize::try_from(program.provider_requirement().max_candidates())
                    .map_err(|_| ExactPredicateProviderErrorV1::BoundExceeded)?
        {
            return Err(ExactPredicateProviderErrorV1::BindingMismatch);
        }
        let mut rows = rows;
        rows.sort_by(|left, right| left.key.as_bytes().cmp(right.key.as_bytes()));
        if rows
            .windows(2)
            .any(|pair| pair[0].key.as_bytes() == pair[1].key.as_bytes())
        {
            return Err(ExactPredicateProviderErrorV1::DuplicateRow);
        }
        let profiles = referenced_profiles(&program)?;
        for row in &rows {
            if row.fields.len() > profiles.len()
                || row.fields.keys().any(|field| !profiles.contains_key(field))
            {
                return Err(ExactPredicateProviderErrorV1::Integrity);
            }
        }
        let mut orders = Vec::with_capacity(program.orders().len());
        for order in program.orders() {
            let mut ordered_rows = (0..rows.len()).collect::<Vec<_>>();
            for row in &rows {
                validate_order_row(row, order)?;
            }
            ordered_rows.sort_by(|left, right| compare_rows(&rows[*left], &rows[*right], order));
            let mut fields = profiles
                .iter()
                .map(|(field, profile)| (*field, SparseFieldIndexV1::new(*profile)))
                .collect::<BTreeMap<_, _>>();
            for (position, row_index) in ordered_rows.iter().copied().enumerate() {
                let row = &rows[row_index];
                for (field, index) in &mut fields {
                    index.insert(
                        position,
                        row.fields
                            .get(field)
                            .unwrap_or(&ExactReferenceCellV1::Missing),
                    )?;
                }
            }
            orders.push(ExactOrderIndexV1 {
                rows: ordered_rows,
                fields,
            });
        }
        let state = Self {
            binding,
            program,
            rows,
            orders,
        };
        state.validate_state_bound()?;
        Ok(state)
    }

    /// Applies one complete epoch atomically; a failure preserves prior state.
    pub fn apply(
        &mut self,
        epoch: CommitSequence,
        mutations: &[ExactPredicateIndexMutationV1],
    ) -> Result<(), ExactPredicateProviderErrorV1> {
        let mut seen = BTreeSet::new();
        let mut rows = self
            .rows
            .iter()
            .cloned()
            .map(|row| (row.key.as_bytes().to_vec(), row))
            .collect::<BTreeMap<_, _>>();
        for mutation in mutations {
            if !seen.insert(mutation.key().as_bytes().to_vec()) {
                return Err(ExactPredicateProviderErrorV1::DuplicateRow);
            }
            match mutation {
                ExactPredicateIndexMutationV1::Upsert(row) => {
                    rows.insert(row.key.as_bytes().to_vec(), row.clone());
                }
                ExactPredicateIndexMutationV1::Delete(key) => {
                    rows.remove(key.as_bytes());
                }
            }
        }
        let next = Self::rebuild(
            self.binding.advanced(epoch)?,
            self.program.clone(),
            rows.into_values().collect(),
        )?;
        *self = next;
        Ok(())
    }

    /// Rebuilds physical indexes without changing any logical provider identity.
    pub fn compact(&mut self) -> Result<(), ExactPredicateProviderErrorV1> {
        let compacted = Self::rebuild(self.binding, self.program.clone(), self.rows.clone())?;
        *self = compacted;
        Ok(())
    }

    /// Executes one sealed member using only provider-owned indexed state.
    pub fn result_page(
        &self,
        parameters: &BTreeMap<u16, ExactParameterValueV1>,
        member: ExactPredicateFamilyMemberV1,
        offset: u32,
        limit: NonZeroU16,
    ) -> Result<ExactPredicateResultPageV1, ExactPredicateProviderErrorV1> {
        if !self.program.members().contains(&member)
            || offset > self.program.max_offset()
            || limit.get() > self.program.max_limit()
        {
            return Err(ExactPredicateProviderErrorV1::WindowInvalid);
        }
        let order = self
            .orders
            .get(usize::from(member.order_ordinal()))
            .ok_or(ExactPredicateProviderErrorV1::MemberMismatch)?;
        let mut fuel = Fuel::new(self.program.provider_requirement().max_work_units());
        let selected = evaluate_node(
            self.program.predicate(),
            order,
            parameters,
            member.presence_bits(),
            &mut fuel,
        )?;
        let exact_total = selected.count();
        let positions = selected.select(offset, limit.get());
        let rows = positions
            .into_iter()
            .map(|position| {
                order
                    .rows
                    .get(position)
                    .and_then(|row| self.rows.get(*row))
                    .cloned()
                    .ok_or(ExactPredicateProviderErrorV1::Integrity)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ExactPredicateResultPageV1 {
            rows,
            exact_total,
            binding: self.binding,
            member,
        })
    }

    /// Exact immutable provider binding.
    #[must_use]
    pub const fn binding(&self) -> ExactPredicateProviderBindingV1 {
        self.binding
    }

    /// Canonical semantic program.
    #[must_use]
    pub const fn program(&self) -> &ExactPredicateProgramV1 {
        &self.program
    }

    /// Encodes canonical V4 state; physical indexes are rebuilt and re-proved on recovery.
    pub fn to_checkpoint_bytes(&self) -> Result<Vec<u8>, ExactPredicateProviderErrorV1> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(CHECKPOINT_MAGIC);
        bytes.extend_from_slice(&EXACT_PREDICATE_PROVIDER_STATE_FORMAT_V4.to_be_bytes());
        bytes.extend_from_slice(&0_u16.to_be_bytes());
        write_bytes(&mut bytes, self.program.canonical_bytes())?;
        bytes.extend_from_slice(self.binding.plan.as_bytes());
        bytes.extend_from_slice(self.binding.descriptor.as_bytes());
        bytes.extend_from_slice(self.binding.policy_shape.as_bytes());
        bytes.extend_from_slice(self.binding.partition.as_bytes());
        bytes.extend_from_slice(&self.binding.history_incarnation.to_be_bytes());
        bytes.extend_from_slice(&self.binding.generation.to_be_bytes());
        bytes.extend_from_slice(&self.binding.frontier.get().to_be_bytes());
        bytes.extend_from_slice(
            &u32::try_from(self.rows.len())
                .map_err(|_| ExactPredicateProviderErrorV1::BoundExceeded)?
                .to_be_bytes(),
        );
        for row in &self.rows {
            write_bytes(&mut bytes, row.key.as_bytes())?;
            bytes.extend_from_slice(
                &u16::try_from(row.fields.len())
                    .map_err(|_| ExactPredicateProviderErrorV1::BoundExceeded)?
                    .to_be_bytes(),
            );
            for (field, cell) in &row.fields {
                bytes.extend_from_slice(&field.to_be_bytes());
                match cell {
                    ExactReferenceCellV1::Missing => bytes.push(0),
                    ExactReferenceCellV1::Null => bytes.push(1),
                    ExactReferenceCellV1::Value(value) => {
                        bytes.push(2);
                        write_bytes(&mut bytes, &encode_scalar(value)?)?;
                    }
                }
            }
            let output =
                encode_canonical_value(&riffdb_types::CanonicalValue::Record(row.output.clone()))
                    .map_err(|_| ExactPredicateProviderErrorV1::Integrity)?;
            write_bytes(&mut bytes, &output)?;
        }
        if bytes.len().saturating_add(CHECKSUM_BYTES) > MAX_EXACT_PREDICATE_CHECKPOINT_BYTES_V4 {
            return Err(ExactPredicateProviderErrorV1::BoundExceeded);
        }
        let digest = hash(HashDomain::ExactResultCheckpoint, &bytes);
        bytes.extend_from_slice(digest.as_bytes());
        Ok(bytes)
    }

    /// Strictly decodes V4 and reconstructs every derived index from canonical rows.
    pub fn from_checkpoint_bytes(bytes: &[u8]) -> Result<Self, ExactPredicateProviderErrorV1> {
        if bytes.len() < 8 + CHECKSUM_BYTES
            || bytes.len() > MAX_EXACT_PREDICATE_CHECKPOINT_BYTES_V4
            || &bytes[..4] != CHECKPOINT_MAGIC
            || u16::from_be_bytes([bytes[4], bytes[5]]) != EXACT_PREDICATE_PROVIDER_STATE_FORMAT_V4
        {
            return Err(ExactPredicateProviderErrorV1::UnsupportedFormat);
        }
        if bytes[6..8] != [0, 0] {
            return Err(ExactPredicateProviderErrorV1::Integrity);
        }
        let payload_end = bytes
            .len()
            .checked_sub(CHECKSUM_BYTES)
            .ok_or(ExactPredicateProviderErrorV1::Integrity)?;
        let digest = hash(HashDomain::ExactResultCheckpoint, &bytes[..payload_end]);
        if bytes[payload_end..] != digest.as_bytes()[..] {
            return Err(ExactPredicateProviderErrorV1::Integrity);
        }
        let mut reader = CheckpointReader::new(&bytes[8..payload_end]);
        let program = ExactPredicateProgramV1::from_canonical_bytes(reader.bytes()?)
            .map_err(|_| ExactPredicateProviderErrorV1::Integrity)?;
        let plan = QueryPlanHash::from_bytes(reader.array()?);
        let descriptor = ProjectionProviderDescriptorHash::from_bytes(reader.array()?);
        let policy_shape = ApplicationRoleHash::from_bytes(reader.array()?);
        let partition = PartitionKeyHash::from_bytes(reader.array()?);
        let history_incarnation = reader.u64()?;
        let generation = ProjectionGeneration::new(reader.u64()?)
            .ok_or(ExactPredicateProviderErrorV1::Integrity)?;
        let frontier =
            CommitSequence::new(reader.u64()?).ok_or(ExactPredicateProviderErrorV1::Integrity)?;
        let binding = Self::checked_decoded_binding(
            plan,
            descriptor,
            policy_shape,
            partition,
            history_incarnation,
            generation,
            frontier,
            &program,
        )?;
        let count = usize::try_from(reader.u32()?)
            .map_err(|_| ExactPredicateProviderErrorV1::BoundExceeded)?;
        if count
            > usize::try_from(program.provider_requirement().max_candidates())
                .map_err(|_| ExactPredicateProviderErrorV1::BoundExceeded)?
        {
            return Err(ExactPredicateProviderErrorV1::BoundExceeded);
        }
        let mut rows = Vec::with_capacity(count);
        for _ in 0..count {
            let key = EntityKey::from_bytes(reader.bytes()?.to_vec())
                .map_err(|_| ExactPredicateProviderErrorV1::Integrity)?;
            let field_count = usize::from(reader.u16()?);
            let mut fields = BTreeMap::new();
            for _ in 0..field_count {
                let field =
                    FieldId::new(reader.u32()?).ok_or(ExactPredicateProviderErrorV1::Integrity)?;
                let cell = match reader.u8()? {
                    0 => ExactReferenceCellV1::Missing,
                    1 => ExactReferenceCellV1::Null,
                    2 => ExactReferenceCellV1::Value(decode_scalar(reader.bytes()?)?),
                    _ => return Err(ExactPredicateProviderErrorV1::Integrity),
                };
                if fields.insert(field, cell).is_some() {
                    return Err(ExactPredicateProviderErrorV1::Integrity);
                }
            }
            let riffdb_types::CanonicalValue::Record(output) =
                decode_canonical_value(reader.bytes()?)
                    .map_err(|_| ExactPredicateProviderErrorV1::Integrity)?
            else {
                return Err(ExactPredicateProviderErrorV1::Integrity);
            };
            rows.push(ExactPredicateProviderRowV1::new(key, fields, output));
        }
        reader.finish()?;
        let recovered = Self::rebuild(binding, program, rows)?;
        if recovered.to_checkpoint_bytes()? != bytes {
            return Err(ExactPredicateProviderErrorV1::Integrity);
        }
        Ok(recovered)
    }

    #[allow(clippy::too_many_arguments)]
    fn checked_decoded_binding(
        plan: QueryPlanHash,
        descriptor: ProjectionProviderDescriptorHash,
        policy_shape: ApplicationRoleHash,
        partition: PartitionKeyHash,
        history_incarnation: u64,
        generation: ProjectionGeneration,
        frontier: CommitSequence,
        program: &ExactPredicateProgramV1,
    ) -> Result<ExactPredicateProviderBindingV1, ExactPredicateProviderErrorV1> {
        let binding = ExactPredicateProviderBindingV1::new(
            plan,
            program,
            policy_shape,
            partition,
            history_incarnation,
            generation,
            frontier,
        )?;
        if binding.descriptor != descriptor {
            return Err(ExactPredicateProviderErrorV1::BindingMismatch);
        }
        Ok(binding)
    }

    fn validate_state_bound(&self) -> Result<(), ExactPredicateProviderErrorV1> {
        let mut bytes = self.program.canonical_bytes().len() as u64;
        for row in &self.rows {
            bytes = bytes
                .checked_add(row.key.as_bytes().len() as u64)
                .and_then(|value| value.checked_add((row.fields.len() as u64).saturating_mul(5)))
                .ok_or(ExactPredicateProviderErrorV1::BoundExceeded)?;
            for cell in row.fields.values() {
                if let ExactReferenceCellV1::Value(value) = cell {
                    bytes = bytes
                        .checked_add(estimated_scalar_bytes(value))
                        .ok_or(ExactPredicateProviderErrorV1::BoundExceeded)?;
                }
            }
            let output =
                encode_canonical_value(&riffdb_types::CanonicalValue::Record(row.output.clone()))
                    .map_err(|_| ExactPredicateProviderErrorV1::Integrity)?;
            bytes = bytes
                .checked_add(output.len() as u64)
                .ok_or(ExactPredicateProviderErrorV1::BoundExceeded)?;
        }
        for order in &self.orders {
            bytes = bytes
                .checked_add((order.rows.len() as u64).saturating_mul(8))
                .ok_or(ExactPredicateProviderErrorV1::BoundExceeded)?;
            for index in order.fields.values() {
                bytes = bytes
                    .checked_add(index.estimated_bytes()?)
                    .ok_or(ExactPredicateProviderErrorV1::BoundExceeded)?;
            }
        }
        let per_row = u64::from(
            self.program
                .provider_requirement()
                .max_state_bytes_per_row(),
        );
        let allowed = per_row
            .checked_mul(self.rows.len().max(1) as u64)
            .ok_or(ExactPredicateProviderErrorV1::BoundExceeded)?;
        if bytes > allowed {
            return Err(ExactPredicateProviderErrorV1::StateAmplification);
        }
        Ok(())
    }
}

/// Rebuildable V5 provider state with explicit missing/null placement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactPredicatePartitionIndexV5 {
    binding: ExactPredicateProviderBindingV2,
    program: ExactPredicateProgramV2,
    rows: Vec<ExactPredicateProviderRowV1>,
    orders: Vec<ExactOrderIndexV1>,
}

impl ExactPredicatePartitionIndexV5 {
    /// Rebuilds one disjoint V5 generation after row-policy admission.
    pub fn rebuild(
        binding: ExactPredicateProviderBindingV2,
        program: ExactPredicateProgramV2,
        rows: Vec<ExactPredicateProviderRowV1>,
    ) -> Result<Self, ExactPredicateProviderErrorV1> {
        if binding.program != program.identity()
            || rows.len()
                > usize::try_from(program.provider_requirement().max_candidates())
                    .map_err(|_| ExactPredicateProviderErrorV1::BoundExceeded)?
        {
            return Err(ExactPredicateProviderErrorV1::BindingMismatch);
        }
        let mut rows = rows;
        rows.sort_by(|left, right| left.key.as_bytes().cmp(right.key.as_bytes()));
        if rows
            .windows(2)
            .any(|pair| pair[0].key.as_bytes() == pair[1].key.as_bytes())
        {
            return Err(ExactPredicateProviderErrorV1::DuplicateRow);
        }
        let profiles = referenced_profiles_v2(&program)?;
        for row in &rows {
            if row.fields.len() > profiles.len()
                || row.fields.keys().any(|field| !profiles.contains_key(field))
            {
                return Err(ExactPredicateProviderErrorV1::Integrity);
            }
        }
        let mut orders = Vec::with_capacity(program.orders().len());
        for order in program.orders() {
            let mut ordered_rows = (0..rows.len()).collect::<Vec<_>>();
            for row in &rows {
                validate_nullable_order_row(row, order)?;
            }
            ordered_rows.sort_by(|left, right| {
                compare_nullable_provider_rows(&rows[*left], &rows[*right], order)
            });
            let mut fields = profiles
                .iter()
                .map(|(field, profile)| (*field, SparseFieldIndexV1::new(*profile)))
                .collect::<BTreeMap<_, _>>();
            for (position, row_index) in ordered_rows.iter().copied().enumerate() {
                let row = &rows[row_index];
                for (field, index) in &mut fields {
                    index.insert(
                        position,
                        row.fields
                            .get(field)
                            .unwrap_or(&ExactReferenceCellV1::Missing),
                    )?;
                }
            }
            orders.push(ExactOrderIndexV1 {
                rows: ordered_rows,
                fields,
            });
        }
        let state = Self {
            binding,
            program,
            rows,
            orders,
        };
        state.validate_state_bound()?;
        Ok(state)
    }

    /// Applies one complete epoch atomically; failure preserves prior state.
    pub fn apply(
        &mut self,
        epoch: CommitSequence,
        mutations: &[ExactPredicateIndexMutationV1],
    ) -> Result<(), ExactPredicateProviderErrorV1> {
        let mut seen = BTreeSet::new();
        let mut rows = self
            .rows
            .iter()
            .cloned()
            .map(|row| (row.key.as_bytes().to_vec(), row))
            .collect::<BTreeMap<_, _>>();
        for mutation in mutations {
            if !seen.insert(mutation.key().as_bytes().to_vec()) {
                return Err(ExactPredicateProviderErrorV1::DuplicateRow);
            }
            match mutation {
                ExactPredicateIndexMutationV1::Upsert(row) => {
                    rows.insert(row.key.as_bytes().to_vec(), row.clone());
                }
                ExactPredicateIndexMutationV1::Delete(key) => {
                    rows.remove(key.as_bytes());
                }
            }
        }
        let next = Self::rebuild(
            self.binding.advanced(epoch)?,
            self.program.clone(),
            rows.into_values().collect(),
        )?;
        *self = next;
        Ok(())
    }

    /// Rebuilds physical indexes without changing logical identity.
    pub fn compact(&mut self) -> Result<(), ExactPredicateProviderErrorV1> {
        let compacted = Self::rebuild(self.binding, self.program.clone(), self.rows.clone())?;
        *self = compacted;
        Ok(())
    }

    /// Executes one sealed member using only V5 provider-owned indexed state.
    pub fn result_page(
        &self,
        parameters: &BTreeMap<u16, ExactParameterValueV1>,
        member: ExactPredicateFamilyMemberV1,
        offset: u32,
        limit: NonZeroU16,
    ) -> Result<ExactPredicateResultPageV2, ExactPredicateProviderErrorV1> {
        if !self.program.members().contains(&member)
            || offset > self.program.max_offset()
            || limit.get() > self.program.max_limit()
        {
            return Err(ExactPredicateProviderErrorV1::WindowInvalid);
        }
        let order = self
            .orders
            .get(usize::from(member.order_ordinal()))
            .ok_or(ExactPredicateProviderErrorV1::MemberMismatch)?;
        let mut fuel = Fuel::new(self.program.provider_requirement().max_work_units());
        let selected = evaluate_node(
            self.program.predicate(),
            order,
            parameters,
            member.presence_bits(),
            &mut fuel,
        )?;
        let exact_total = selected.count();
        let rows = selected
            .select(offset, limit.get())
            .into_iter()
            .map(|position| {
                order
                    .rows
                    .get(position)
                    .and_then(|row| self.rows.get(*row))
                    .cloned()
                    .ok_or(ExactPredicateProviderErrorV1::Integrity)
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ExactPredicateResultPageV2 {
            rows,
            exact_total,
            binding: self.binding,
            member,
        })
    }

    /// Exact immutable V5 provider binding.
    #[must_use]
    pub const fn binding(&self) -> ExactPredicateProviderBindingV2 {
        self.binding
    }

    /// Canonical nullable semantic program.
    #[must_use]
    pub const fn program(&self) -> &ExactPredicateProgramV2 {
        &self.program
    }

    /// Encodes canonical V5 state; physical indexes are rebuilt on recovery.
    pub fn to_checkpoint_bytes(&self) -> Result<Vec<u8>, ExactPredicateProviderErrorV1> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(CHECKPOINT_MAGIC);
        bytes.extend_from_slice(&EXACT_PREDICATE_PROVIDER_STATE_FORMAT_V5.to_be_bytes());
        bytes.extend_from_slice(&0_u16.to_be_bytes());
        write_bytes(&mut bytes, self.program.canonical_bytes())?;
        bytes.extend_from_slice(self.binding.plan.as_bytes());
        bytes.extend_from_slice(self.binding.program.as_bytes());
        bytes.extend_from_slice(self.binding.descriptor.as_bytes());
        bytes.extend_from_slice(self.binding.policy_shape.as_bytes());
        bytes.extend_from_slice(self.binding.partition.as_bytes());
        bytes.extend_from_slice(&self.binding.history_incarnation.to_be_bytes());
        bytes.extend_from_slice(&self.binding.generation.to_be_bytes());
        bytes.extend_from_slice(&self.binding.frontier.get().to_be_bytes());
        bytes.extend_from_slice(
            &u32::try_from(self.rows.len())
                .map_err(|_| ExactPredicateProviderErrorV1::BoundExceeded)?
                .to_be_bytes(),
        );
        encode_rows(&mut bytes, &self.rows)?;
        if bytes.len().saturating_add(CHECKSUM_BYTES) > MAX_EXACT_PREDICATE_CHECKPOINT_BYTES_V5 {
            return Err(ExactPredicateProviderErrorV1::BoundExceeded);
        }
        let digest = hash(HashDomain::ExactResultCheckpoint, &bytes);
        bytes.extend_from_slice(digest.as_bytes());
        Ok(bytes)
    }

    /// Strictly decodes V5 and reconstructs every derived index.
    pub fn from_checkpoint_bytes(bytes: &[u8]) -> Result<Self, ExactPredicateProviderErrorV1> {
        if bytes.len() < 8 + CHECKSUM_BYTES
            || bytes.len() > MAX_EXACT_PREDICATE_CHECKPOINT_BYTES_V5
            || &bytes[..4] != CHECKPOINT_MAGIC
            || u16::from_be_bytes([bytes[4], bytes[5]]) != EXACT_PREDICATE_PROVIDER_STATE_FORMAT_V5
        {
            return Err(ExactPredicateProviderErrorV1::UnsupportedFormat);
        }
        if bytes[6..8] != [0, 0] {
            return Err(ExactPredicateProviderErrorV1::Integrity);
        }
        let payload_end = bytes
            .len()
            .checked_sub(CHECKSUM_BYTES)
            .ok_or(ExactPredicateProviderErrorV1::Integrity)?;
        let digest = hash(HashDomain::ExactResultCheckpoint, &bytes[..payload_end]);
        if bytes[payload_end..] != digest.as_bytes()[..] {
            return Err(ExactPredicateProviderErrorV1::Integrity);
        }
        let mut reader = CheckpointReader::new(&bytes[8..payload_end]);
        let program = ExactPredicateProgramV2::from_canonical_bytes(reader.bytes()?)
            .map_err(|_| ExactPredicateProviderErrorV1::Integrity)?;
        let plan = QueryPlanHash::from_bytes(reader.array()?);
        let program_identity = QueryPlanHash::from_bytes(reader.array()?);
        let descriptor = ProjectionProviderDescriptorHash::from_bytes(reader.array()?);
        let policy_shape = ApplicationRoleHash::from_bytes(reader.array()?);
        let partition = PartitionKeyHash::from_bytes(reader.array()?);
        let history_incarnation = reader.u64()?;
        let generation = ProjectionGeneration::new(reader.u64()?)
            .ok_or(ExactPredicateProviderErrorV1::Integrity)?;
        let frontier =
            CommitSequence::new(reader.u64()?).ok_or(ExactPredicateProviderErrorV1::Integrity)?;
        let binding = ExactPredicateProviderBindingV2::new(
            plan,
            &program,
            policy_shape,
            partition,
            history_incarnation,
            generation,
            frontier,
        )?;
        if binding.program != program_identity || binding.descriptor != descriptor {
            return Err(ExactPredicateProviderErrorV1::BindingMismatch);
        }
        let count = usize::try_from(reader.u32()?)
            .map_err(|_| ExactPredicateProviderErrorV1::BoundExceeded)?;
        if count
            > usize::try_from(program.provider_requirement().max_candidates())
                .map_err(|_| ExactPredicateProviderErrorV1::BoundExceeded)?
        {
            return Err(ExactPredicateProviderErrorV1::BoundExceeded);
        }
        let rows = decode_rows(&mut reader, count)?;
        reader.finish()?;
        let recovered = Self::rebuild(binding, program, rows)?;
        if recovered.to_checkpoint_bytes()? != bytes {
            return Err(ExactPredicateProviderErrorV1::Integrity);
        }
        Ok(recovered)
    }

    fn validate_state_bound(&self) -> Result<(), ExactPredicateProviderErrorV1> {
        validate_state_bound(
            self.program.canonical_bytes(),
            self.program
                .provider_requirement()
                .max_state_bytes_per_row(),
            &self.rows,
            &self.orders,
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BitSet {
    len: usize,
    words: Vec<u64>,
}

impl BitSet {
    fn empty(len: usize) -> Self {
        Self {
            len,
            words: vec![0; len.div_ceil(64)],
        }
    }

    fn full(len: usize) -> Self {
        let mut value = Self {
            len,
            words: vec![u64::MAX; len.div_ceil(64)],
        };
        if let Some(last) = value.words.last_mut()
            && !len.is_multiple_of(64)
        {
            *last &= (1_u64 << (len % 64)) - 1;
        }
        value
    }

    fn from_positions(
        len: usize,
        positions: &[usize],
        fuel: &mut Fuel,
    ) -> Result<Self, ExactPredicateProviderErrorV1> {
        let mut value = Self::empty(len);
        for position in positions {
            fuel.charge(1)?;
            if *position >= len {
                return Err(ExactPredicateProviderErrorV1::Integrity);
            }
            value.words[*position / 64] |= 1_u64 << (*position % 64);
        }
        Ok(value)
    }

    fn combine(
        &mut self,
        other: &Self,
        union: bool,
        fuel: &mut Fuel,
    ) -> Result<(), ExactPredicateProviderErrorV1> {
        if self.len != other.len {
            return Err(ExactPredicateProviderErrorV1::Integrity);
        }
        for (left, right) in self.words.iter_mut().zip(&other.words) {
            fuel.charge(1)?;
            if union {
                *left |= right;
            } else {
                *left &= right;
            }
        }
        Ok(())
    }

    fn subtract(
        &mut self,
        other: &Self,
        fuel: &mut Fuel,
    ) -> Result<(), ExactPredicateProviderErrorV1> {
        if self.len != other.len {
            return Err(ExactPredicateProviderErrorV1::Integrity);
        }
        for (left, right) in self.words.iter_mut().zip(&other.words) {
            fuel.charge(1)?;
            *left &= !right;
        }
        Ok(())
    }

    fn count(&self) -> u64 {
        self.words
            .iter()
            .map(|word| u64::from(word.count_ones()))
            .sum()
    }

    fn select(&self, offset: u32, limit: u16) -> Vec<usize> {
        let mut skipped = u64::from(offset);
        let mut selected = Vec::with_capacity(usize::from(limit));
        for (word_ordinal, word) in self.words.iter().copied().enumerate() {
            let count = u64::from(word.count_ones());
            if skipped >= count {
                skipped -= count;
                continue;
            }
            let mut word = word;
            while word != 0 && selected.len() < usize::from(limit) {
                let bit = word.trailing_zeros() as usize;
                if skipped == 0 {
                    selected.push(word_ordinal * 64 + bit);
                } else {
                    skipped -= 1;
                }
                word &= word - 1;
            }
            if selected.len() == usize::from(limit) {
                break;
            }
        }
        selected
    }
}

struct Fuel {
    remaining: u64,
}

impl Fuel {
    const fn new(maximum: u64) -> Self {
        Self { remaining: maximum }
    }

    fn charge(&mut self, amount: u64) -> Result<(), ExactPredicateProviderErrorV1> {
        self.remaining = self
            .remaining
            .checked_sub(amount)
            .ok_or(ExactPredicateProviderErrorV1::FuelExhausted)?;
        Ok(())
    }
}

fn evaluate_node(
    node: &ExactPredicateNodeV1,
    order: &ExactOrderIndexV1,
    parameters: &BTreeMap<u16, ExactParameterValueV1>,
    presence_bits: u64,
    fuel: &mut Fuel,
) -> Result<BitSet, ExactPredicateProviderErrorV1> {
    match node {
        ExactPredicateNodeV1::Leaf(leaf) => evaluate_leaf(leaf, order, parameters, fuel),
        ExactPredicateNodeV1::And(children) => {
            let mut result = BitSet::full(order.rows.len());
            for child in children {
                let child = evaluate_node(child, order, parameters, presence_bits, fuel)?;
                result.combine(&child, false, fuel)?;
            }
            Ok(result)
        }
        ExactPredicateNodeV1::Or(children) => {
            let mut result = BitSet::empty(order.rows.len());
            for child in children {
                let child = evaluate_node(child, order, parameters, presence_bits, fuel)?;
                result.combine(&child, true, fuel)?;
            }
            Ok(result)
        }
        ExactPredicateNodeV1::When {
            presence_ordinal,
            child,
        } => {
            if presence_bits & (1_u64 << presence_ordinal) == 0 {
                Ok(BitSet::full(order.rows.len()))
            } else {
                evaluate_node(child, order, parameters, presence_bits, fuel)
            }
        }
    }
}

fn evaluate_leaf(
    leaf: &ExactPredicateLeafV1,
    order: &ExactOrderIndexV1,
    parameters: &BTreeMap<u16, ExactParameterValueV1>,
    fuel: &mut Fuel,
) -> Result<BitSet, ExactPredicateProviderErrorV1> {
    let index = order
        .fields
        .get(&leaf.field())
        .ok_or(ExactPredicateProviderErrorV1::Integrity)?;
    if index.profile != leaf.profile() {
        return Err(ExactPredicateProviderErrorV1::TypeMismatch);
    }
    match leaf.operator() {
        ExactPredicateOperatorV1::IsNull => {
            return BitSet::from_positions(order.rows.len(), &index.null, fuel);
        }
        ExactPredicateOperatorV1::IsNotNull => {
            return BitSet::from_positions(order.rows.len(), &index.present, fuel);
        }
        ExactPredicateOperatorV1::Exists => {
            let mut result = BitSet::from_positions(order.rows.len(), &index.null, fuel)?;
            let present = BitSet::from_positions(order.rows.len(), &index.present, fuel)?;
            result.combine(&present, true, fuel)?;
            return Ok(result);
        }
        _ => {}
    }
    let slot = leaf
        .value()
        .ok_or(ExactPredicateProviderErrorV1::Integrity)?;
    let ordinal = match slot {
        ExactValueSlotV1::Scalar(ordinal) | ExactValueSlotV1::Set(ordinal) => ordinal,
    };
    let parameter = parameters
        .get(&ordinal)
        .ok_or(ExactPredicateProviderErrorV1::ParameterInvalid)?;
    match (leaf.operator(), parameter) {
        (ExactPredicateOperatorV1::In, ExactParameterValueV1::Set(values))
        | (ExactPredicateOperatorV1::NotIn, ExactParameterValueV1::Set(values)) => {
            if values
                .first()
                .is_some_and(|value| value.profile() != leaf.profile())
            {
                return Err(ExactPredicateProviderErrorV1::TypeMismatch);
            }
            let mut matches = BitSet::empty(order.rows.len());
            for value in values {
                fuel.charge(1)?;
                if let Some(positions) = index.values.get(value) {
                    let posting = BitSet::from_positions(order.rows.len(), positions, fuel)?;
                    matches.combine(&posting, true, fuel)?;
                }
            }
            if leaf.operator() == ExactPredicateOperatorV1::NotIn {
                let mut result = BitSet::from_positions(order.rows.len(), &index.present, fuel)?;
                result.subtract(&matches, fuel)?;
                Ok(result)
            } else {
                Ok(matches)
            }
        }
        (operator, ExactParameterValueV1::Scalar(value)) => {
            if value.profile() != leaf.profile() {
                return Err(ExactPredicateProviderErrorV1::TypeMismatch);
            }
            match operator {
                ExactPredicateOperatorV1::Equal | ExactPredicateOperatorV1::NotEqual => {
                    let equal = index.values.get(value).map_or_else(
                        || Ok(BitSet::empty(order.rows.len())),
                        |positions| BitSet::from_positions(order.rows.len(), positions, fuel),
                    )?;
                    if operator == ExactPredicateOperatorV1::NotEqual {
                        let mut result =
                            BitSet::from_positions(order.rows.len(), &index.present, fuel)?;
                        result.subtract(&equal, fuel)?;
                        Ok(result)
                    } else {
                        Ok(equal)
                    }
                }
                ExactPredicateOperatorV1::Less
                | ExactPredicateOperatorV1::LessEqual
                | ExactPredicateOperatorV1::Greater
                | ExactPredicateOperatorV1::GreaterEqual => {
                    let mut result = BitSet::empty(order.rows.len());
                    for (candidate, positions) in &index.values {
                        fuel.charge(1)?;
                        let ordering = candidate.cmp(value);
                        let selected = match operator {
                            ExactPredicateOperatorV1::Less => ordering == Ordering::Less,
                            ExactPredicateOperatorV1::LessEqual => ordering != Ordering::Greater,
                            ExactPredicateOperatorV1::Greater => ordering == Ordering::Greater,
                            ExactPredicateOperatorV1::GreaterEqual => ordering != Ordering::Less,
                            _ => false,
                        };
                        if selected {
                            let posting =
                                BitSet::from_positions(order.rows.len(), positions, fuel)?;
                            result.combine(&posting, true, fuel)?;
                        }
                    }
                    Ok(result)
                }
                ExactPredicateOperatorV1::StartsWith
                | ExactPredicateOperatorV1::EndsWith
                | ExactPredicateOperatorV1::Contains => {
                    let ExactScalarV1::String(needle) = value else {
                        return Err(ExactPredicateProviderErrorV1::TypeMismatch);
                    };
                    if needle.is_empty() {
                        return Err(ExactPredicateProviderErrorV1::ParameterInvalid);
                    }
                    let mut prefix = needle.as_bytes().to_vec();
                    let map = match operator {
                        ExactPredicateOperatorV1::StartsWith => &index.text,
                        ExactPredicateOperatorV1::EndsWith => {
                            prefix.reverse();
                            &index.reversed_text
                        }
                        ExactPredicateOperatorV1::Contains => &index.suffixes,
                        _ => unreachable!("closed text operator"),
                    };
                    postings_with_prefix(map, &prefix, order.rows.len(), fuel)
                }
                _ => Err(ExactPredicateProviderErrorV1::ParameterInvalid),
            }
        }
        _ => Err(ExactPredicateProviderErrorV1::ParameterInvalid),
    }
}

fn postings_with_prefix(
    map: &BTreeMap<Vec<u8>, Vec<usize>>,
    prefix: &[u8],
    row_count: usize,
    fuel: &mut Fuel,
) -> Result<BitSet, ExactPredicateProviderErrorV1> {
    let mut result = BitSet::empty(row_count);
    for (key, positions) in map.range(prefix.to_vec()..) {
        fuel.charge(1)?;
        if !key.starts_with(prefix) {
            break;
        }
        let posting = BitSet::from_positions(row_count, positions, fuel)?;
        result.combine(&posting, true, fuel)?;
    }
    Ok(result)
}

fn referenced_profiles(
    program: &ExactPredicateProgramV1,
) -> Result<BTreeMap<FieldId, ExactComparisonProfileV1>, ExactPredicateProviderErrorV1> {
    let mut profiles = BTreeMap::new();
    collect_predicate_profiles(program.predicate(), &mut profiles)?;
    for order in program.orders() {
        for term in order.terms() {
            insert_profile(&mut profiles, term.field(), term.profile())?;
        }
    }
    Ok(profiles)
}

fn referenced_profiles_v2(
    program: &ExactPredicateProgramV2,
) -> Result<BTreeMap<FieldId, ExactComparisonProfileV1>, ExactPredicateProviderErrorV1> {
    let mut profiles = BTreeMap::new();
    collect_predicate_profiles(program.predicate(), &mut profiles)?;
    for order in program.orders() {
        for term in order.terms() {
            insert_profile(&mut profiles, term.field(), term.profile())?;
        }
    }
    Ok(profiles)
}

fn collect_predicate_profiles(
    node: &ExactPredicateNodeV1,
    profiles: &mut BTreeMap<FieldId, ExactComparisonProfileV1>,
) -> Result<(), ExactPredicateProviderErrorV1> {
    match node {
        ExactPredicateNodeV1::Leaf(leaf) => insert_profile(profiles, leaf.field(), leaf.profile()),
        ExactPredicateNodeV1::When { child, .. } => collect_predicate_profiles(child, profiles),
        ExactPredicateNodeV1::And(children) | ExactPredicateNodeV1::Or(children) => {
            for child in children {
                collect_predicate_profiles(child, profiles)?;
            }
            Ok(())
        }
    }
}

fn insert_profile(
    profiles: &mut BTreeMap<FieldId, ExactComparisonProfileV1>,
    field: FieldId,
    profile: ExactComparisonProfileV1,
) -> Result<(), ExactPredicateProviderErrorV1> {
    if profiles
        .insert(field, profile)
        .is_some_and(|prior| prior != profile)
    {
        return Err(ExactPredicateProviderErrorV1::TypeMismatch);
    }
    Ok(())
}

fn validate_order_row(
    row: &ExactPredicateProviderRowV1,
    order: &ExactOrderProgramV1,
) -> Result<(), ExactPredicateProviderErrorV1> {
    for term in order.terms() {
        let Some(ExactReferenceCellV1::Value(value)) = row.fields.get(&term.field()) else {
            return Err(ExactPredicateProviderErrorV1::OrderStateInvalid);
        };
        if value.profile() != term.profile() {
            return Err(ExactPredicateProviderErrorV1::TypeMismatch);
        }
    }
    Ok(())
}

fn compare_rows(
    left: &ExactPredicateProviderRowV1,
    right: &ExactPredicateProviderRowV1,
    order: &ExactOrderProgramV1,
) -> Ordering {
    for term in order.terms() {
        let left = match left.fields.get(&term.field()) {
            Some(ExactReferenceCellV1::Value(value)) => value,
            _ => return left.key.as_bytes().cmp(right.key.as_bytes()),
        };
        let right = match right.fields.get(&term.field()) {
            Some(ExactReferenceCellV1::Value(value)) => value,
            _ => return Ordering::Equal,
        };
        let ordering = left.cmp(right);
        if ordering != Ordering::Equal {
            return match term.direction() {
                ExactOrderDirectionV1::Ascending => ordering,
                ExactOrderDirectionV1::Descending => ordering.reverse(),
            };
        }
    }
    left.key.as_bytes().cmp(right.key.as_bytes())
}

fn validate_nullable_order_row(
    row: &ExactPredicateProviderRowV1,
    order: &ExactOrderProgramV2,
) -> Result<(), ExactPredicateProviderErrorV1> {
    for term in order.terms() {
        let cell = row
            .fields
            .get(&term.field())
            .unwrap_or(&ExactReferenceCellV1::Missing);
        match cell {
            ExactReferenceCellV1::Value(value) if value.profile() == term.profile() => {}
            ExactReferenceCellV1::Missing | ExactReferenceCellV1::Null
                if term.placement() != ExactStatePlacementV1::PresentOnlyV1 => {}
            _ => return Err(ExactPredicateProviderErrorV1::OrderStateInvalid),
        }
    }
    Ok(())
}

fn compare_nullable_provider_rows(
    left: &ExactPredicateProviderRowV1,
    right: &ExactPredicateProviderRowV1,
    order: &ExactOrderProgramV2,
) -> Ordering {
    for term in order.terms() {
        let left_cell = left
            .fields
            .get(&term.field())
            .unwrap_or(&ExactReferenceCellV1::Missing);
        let right_cell = right
            .fields
            .get(&term.field())
            .unwrap_or(&ExactReferenceCellV1::Missing);
        let compared = match (left_cell, right_cell) {
            (ExactReferenceCellV1::Value(left), ExactReferenceCellV1::Value(right)) => {
                match term.direction() {
                    ExactOrderDirectionV1::Ascending => left.cmp(right),
                    ExactOrderDirectionV1::Descending => left.cmp(right).reverse(),
                }
            }
            (
                ExactReferenceCellV1::Missing | ExactReferenceCellV1::Null,
                ExactReferenceCellV1::Missing | ExactReferenceCellV1::Null,
            ) => Ordering::Equal,
            (ExactReferenceCellV1::Missing | ExactReferenceCellV1::Null, _) => {
                match term.placement() {
                    ExactStatePlacementV1::NullsFirstV1 => Ordering::Less,
                    ExactStatePlacementV1::NullsLastV1 => Ordering::Greater,
                    ExactStatePlacementV1::PresentOnlyV1 => Ordering::Equal,
                }
            }
            (_, ExactReferenceCellV1::Missing | ExactReferenceCellV1::Null) => {
                match term.placement() {
                    ExactStatePlacementV1::NullsFirstV1 => Ordering::Greater,
                    ExactStatePlacementV1::NullsLastV1 => Ordering::Less,
                    ExactStatePlacementV1::PresentOnlyV1 => Ordering::Equal,
                }
            }
        };
        if compared != Ordering::Equal {
            return compared;
        }
    }
    left.key.as_bytes().cmp(right.key.as_bytes())
}

fn encode_rows(
    bytes: &mut Vec<u8>,
    rows: &[ExactPredicateProviderRowV1],
) -> Result<(), ExactPredicateProviderErrorV1> {
    for row in rows {
        write_bytes(bytes, row.key.as_bytes())?;
        bytes.extend_from_slice(
            &u16::try_from(row.fields.len())
                .map_err(|_| ExactPredicateProviderErrorV1::BoundExceeded)?
                .to_be_bytes(),
        );
        for (field, cell) in &row.fields {
            bytes.extend_from_slice(&field.to_be_bytes());
            match cell {
                ExactReferenceCellV1::Missing => bytes.push(0),
                ExactReferenceCellV1::Null => bytes.push(1),
                ExactReferenceCellV1::Value(value) => {
                    bytes.push(2);
                    write_bytes(bytes, &encode_scalar(value)?)?;
                }
            }
        }
        let output =
            encode_canonical_value(&riffdb_types::CanonicalValue::Record(row.output.clone()))
                .map_err(|_| ExactPredicateProviderErrorV1::Integrity)?;
        write_bytes(bytes, &output)?;
    }
    Ok(())
}

fn decode_rows(
    reader: &mut CheckpointReader<'_>,
    count: usize,
) -> Result<Vec<ExactPredicateProviderRowV1>, ExactPredicateProviderErrorV1> {
    let mut rows = Vec::with_capacity(count);
    for _ in 0..count {
        let key = EntityKey::from_bytes(reader.bytes()?.to_vec())
            .map_err(|_| ExactPredicateProviderErrorV1::Integrity)?;
        let field_count = usize::from(reader.u16()?);
        let mut fields = BTreeMap::new();
        for _ in 0..field_count {
            let field =
                FieldId::new(reader.u32()?).ok_or(ExactPredicateProviderErrorV1::Integrity)?;
            let cell = match reader.u8()? {
                0 => ExactReferenceCellV1::Missing,
                1 => ExactReferenceCellV1::Null,
                2 => ExactReferenceCellV1::Value(decode_scalar(reader.bytes()?)?),
                _ => return Err(ExactPredicateProviderErrorV1::Integrity),
            };
            if fields.insert(field, cell).is_some() {
                return Err(ExactPredicateProviderErrorV1::Integrity);
            }
        }
        let riffdb_types::CanonicalValue::Record(output) = decode_canonical_value(reader.bytes()?)
            .map_err(|_| ExactPredicateProviderErrorV1::Integrity)?
        else {
            return Err(ExactPredicateProviderErrorV1::Integrity);
        };
        rows.push(ExactPredicateProviderRowV1::new(key, fields, output));
    }
    Ok(rows)
}

fn validate_state_bound(
    program_bytes: &[u8],
    max_state_bytes_per_row: u32,
    rows: &[ExactPredicateProviderRowV1],
    orders: &[ExactOrderIndexV1],
) -> Result<(), ExactPredicateProviderErrorV1> {
    let mut bytes = program_bytes.len() as u64;
    for row in rows {
        bytes = bytes
            .checked_add(row.key.as_bytes().len() as u64)
            .and_then(|value| value.checked_add((row.fields.len() as u64).saturating_mul(5)))
            .ok_or(ExactPredicateProviderErrorV1::BoundExceeded)?;
        for cell in row.fields.values() {
            if let ExactReferenceCellV1::Value(value) = cell {
                bytes = bytes
                    .checked_add(estimated_scalar_bytes(value))
                    .ok_or(ExactPredicateProviderErrorV1::BoundExceeded)?;
            }
        }
        let output =
            encode_canonical_value(&riffdb_types::CanonicalValue::Record(row.output.clone()))
                .map_err(|_| ExactPredicateProviderErrorV1::Integrity)?;
        bytes = bytes
            .checked_add(output.len() as u64)
            .ok_or(ExactPredicateProviderErrorV1::BoundExceeded)?;
    }
    for order in orders {
        bytes = bytes
            .checked_add((order.rows.len() as u64).saturating_mul(8))
            .ok_or(ExactPredicateProviderErrorV1::BoundExceeded)?;
        for index in order.fields.values() {
            bytes = bytes
                .checked_add(index.estimated_bytes()?)
                .ok_or(ExactPredicateProviderErrorV1::BoundExceeded)?;
        }
    }
    let allowed = u64::from(max_state_bytes_per_row)
        .checked_mul(rows.len().max(1) as u64)
        .ok_or(ExactPredicateProviderErrorV1::BoundExceeded)?;
    if bytes > allowed {
        return Err(ExactPredicateProviderErrorV1::StateAmplification);
    }
    Ok(())
}

fn estimated_scalar_bytes(value: &ExactScalarV1) -> u64 {
    match value {
        ExactScalarV1::String(value) => value.len() as u64,
        ExactScalarV1::Bytes(value) => value.len() as u64,
        _ => 32,
    }
}

fn encode_scalar(value: &ExactScalarV1) -> Result<Vec<u8>, ExactPredicateProviderErrorV1> {
    let mut bytes = Vec::new();
    match value {
        ExactScalarV1::Bool(value) => {
            bytes.push(1);
            bytes.push(u8::from(*value));
        }
        ExactScalarV1::I64(value) => {
            bytes.push(2);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        ExactScalarV1::U64(value) => {
            bytes.push(3);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        ExactScalarV1::Decimal { coefficient, scale } => {
            bytes.push(4);
            bytes.extend_from_slice(&coefficient.to_be_bytes());
            bytes.push(*scale);
        }
        ExactScalarV1::Money {
            currency,
            coefficient,
            scale,
        } => {
            bytes.push(5);
            bytes.extend_from_slice(currency);
            bytes.extend_from_slice(&coefficient.to_be_bytes());
            bytes.push(*scale);
        }
        ExactScalarV1::String(value) => {
            bytes.push(6);
            bytes.extend_from_slice(value.as_bytes());
        }
        ExactScalarV1::Bytes(value) => {
            bytes.push(7);
            bytes.extend_from_slice(value);
        }
        ExactScalarV1::Timestamp(value) => {
            bytes.push(8);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        ExactScalarV1::Date(value) => {
            bytes.push(9);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        ExactScalarV1::Uuid(value) => {
            bytes.push(10);
            bytes.extend_from_slice(value);
        }
        ExactScalarV1::Enum {
            type_id,
            variant_id,
        } => {
            bytes.push(11);
            bytes.extend_from_slice(&type_id.to_be_bytes());
            bytes.extend_from_slice(&variant_id.to_be_bytes());
        }
    }
    Ok(bytes)
}

fn decode_scalar(bytes: &[u8]) -> Result<ExactScalarV1, ExactPredicateProviderErrorV1> {
    let (&tag, payload) = bytes
        .split_first()
        .ok_or(ExactPredicateProviderErrorV1::Integrity)?;
    match tag {
        1 if payload.len() == 1 && payload[0] <= 1 => Ok(ExactScalarV1::Bool(payload[0] == 1)),
        2 if payload.len() == 8 => Ok(ExactScalarV1::I64(i64::from_be_bytes(
            payload
                .try_into()
                .map_err(|_| ExactPredicateProviderErrorV1::Integrity)?,
        ))),
        3 if payload.len() == 8 => Ok(ExactScalarV1::U64(u64::from_be_bytes(
            payload
                .try_into()
                .map_err(|_| ExactPredicateProviderErrorV1::Integrity)?,
        ))),
        4 if payload.len() == 17 => Ok(ExactScalarV1::Decimal {
            coefficient: i128::from_be_bytes(
                payload[..16]
                    .try_into()
                    .map_err(|_| ExactPredicateProviderErrorV1::Integrity)?,
            ),
            scale: payload[16],
        }),
        5 if payload.len() == 20 => Ok(ExactScalarV1::Money {
            currency: payload[..3]
                .try_into()
                .map_err(|_| ExactPredicateProviderErrorV1::Integrity)?,
            coefficient: i128::from_be_bytes(
                payload[3..19]
                    .try_into()
                    .map_err(|_| ExactPredicateProviderErrorV1::Integrity)?,
            ),
            scale: payload[19],
        }),
        6 => std::str::from_utf8(payload)
            .map(|value| ExactScalarV1::String(value.to_owned()))
            .map_err(|_| ExactPredicateProviderErrorV1::Integrity),
        7 => Ok(ExactScalarV1::Bytes(payload.to_vec())),
        8 if payload.len() == 16 => Ok(ExactScalarV1::Timestamp(i128::from_be_bytes(
            payload
                .try_into()
                .map_err(|_| ExactPredicateProviderErrorV1::Integrity)?,
        ))),
        9 if payload.len() == 4 => Ok(ExactScalarV1::Date(i32::from_be_bytes(
            payload
                .try_into()
                .map_err(|_| ExactPredicateProviderErrorV1::Integrity)?,
        ))),
        10 if payload.len() == 16 => Ok(ExactScalarV1::Uuid(
            payload
                .try_into()
                .map_err(|_| ExactPredicateProviderErrorV1::Integrity)?,
        )),
        11 if payload.len() == 8 => Ok(ExactScalarV1::Enum {
            type_id: u32::from_be_bytes(
                payload[..4]
                    .try_into()
                    .map_err(|_| ExactPredicateProviderErrorV1::Integrity)?,
            ),
            variant_id: u32::from_be_bytes(
                payload[4..]
                    .try_into()
                    .map_err(|_| ExactPredicateProviderErrorV1::Integrity)?,
            ),
        }),
        _ => Err(ExactPredicateProviderErrorV1::Integrity),
    }
}

fn write_bytes(output: &mut Vec<u8>, value: &[u8]) -> Result<(), ExactPredicateProviderErrorV1> {
    output.extend_from_slice(
        &u32::try_from(value.len())
            .map_err(|_| ExactPredicateProviderErrorV1::BoundExceeded)?
            .to_be_bytes(),
    );
    output.extend_from_slice(value);
    if output.len() > MAX_EXACT_PREDICATE_CHECKPOINT_BYTES_V4 {
        return Err(ExactPredicateProviderErrorV1::BoundExceeded);
    }
    Ok(())
}

struct CheckpointReader<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> CheckpointReader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, cursor: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], ExactPredicateProviderErrorV1> {
        let end = self
            .cursor
            .checked_add(length)
            .filter(|end| *end <= self.bytes.len())
            .ok_or(ExactPredicateProviderErrorV1::Integrity)?;
        let value = &self.bytes[self.cursor..end];
        self.cursor = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, ExactPredicateProviderErrorV1> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, ExactPredicateProviderErrorV1> {
        Ok(u16::from_be_bytes(
            self.take(2)?
                .try_into()
                .map_err(|_| ExactPredicateProviderErrorV1::Integrity)?,
        ))
    }

    fn u32(&mut self) -> Result<u32, ExactPredicateProviderErrorV1> {
        Ok(u32::from_be_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| ExactPredicateProviderErrorV1::Integrity)?,
        ))
    }

    fn u64(&mut self) -> Result<u64, ExactPredicateProviderErrorV1> {
        Ok(u64::from_be_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| ExactPredicateProviderErrorV1::Integrity)?,
        ))
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], ExactPredicateProviderErrorV1> {
        self.take(N)?
            .try_into()
            .map_err(|_| ExactPredicateProviderErrorV1::Integrity)
    }

    fn bytes(&mut self) -> Result<&'a [u8], ExactPredicateProviderErrorV1> {
        let length = usize::try_from(self.u32()?)
            .map_err(|_| ExactPredicateProviderErrorV1::BoundExceeded)?;
        self.take(length)
    }

    fn finish(self) -> Result<(), ExactPredicateProviderErrorV1> {
        if self.cursor == self.bytes.len() {
            Ok(())
        } else {
            Err(ExactPredicateProviderErrorV1::Integrity)
        }
    }
}

/// Closed, value-free V4 provider failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExactPredicateProviderErrorV1 {
    /// Plan, descriptor, policy, partition, or history identity disagrees.
    BindingMismatch,
    /// A row key appeared more than once in one state or epoch.
    DuplicateRow,
    /// A field or parameter does not match the compiler profile.
    TypeMismatch,
    /// An order field was missing or null despite its lineage proof.
    OrderStateInvalid,
    /// The selected member is not part of the sealed family.
    MemberMismatch,
    /// Offset or limit exceeds compiler bounds.
    WindowInvalid,
    /// Runtime input is absent, malformed, empty where forbidden, or mis-shaped.
    ParameterInvalid,
    /// Candidate, work, artifact, or arithmetic bound was exceeded.
    BoundExceeded,
    /// Derived-state amplification exceeds the compiler-owned ceiling.
    StateAmplification,
    /// Provider work exhausted the compiler-owned fuel budget.
    FuelExhausted,
    /// Catch-up attempted to reuse or regress an epoch.
    NonAdvancingEpoch,
    /// Checkpoint or derived index state is malformed or inconsistent.
    Integrity,
    /// Checkpoint format is unknown or belongs to V1 through V3.
    UnsupportedFormat,
}

impl fmt::Display for ExactPredicateProviderErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "exact predicate provider unavailable: {self:?}")
    }
}

impl Error for ExactPredicateProviderErrorV1 {}
