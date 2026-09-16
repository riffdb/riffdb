//! Shared, deny-by-default evaluation of compiler-owned row-policy plans.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;

use riffdb_auth::PrincipalFactBindingV1;
#[cfg(feature = "test-fixtures")]
use riffdb_contract_ir::SchemaIr;
use riffdb_contract_ir::{
    ContractBundle, KeySchema, RowPolicyExpressionNodeV1, RowPolicyOperandV1, RowPolicyOperationV1,
    RowPolicyPlanV1, RowPolicyValueSourceV1,
};
use riffdb_types::{
    ActorKind, CanonicalRecord, CanonicalValue, CanonicalValueHash, CapabilityId,
    CapabilityRowPolicyOperationV1, EntityKey, EntityTypeId, EventId, FieldId, IndexId,
    PartitionKey, RowPolicyName, encode_canonical_record, hash_canonical_value,
};

use crate::{
    AuthorizedApplicationExportV1, AuthorizedApplicationQuery, AuthorizedCommandExecution,
    AuthorizedOperation, AuthorizedRowPolicyAuthority,
};

/// Failure to reconstruct exact compiler-owned row-policy execution authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryRowPolicyContextErrorV1 {
    /// The current capability extension and active contract do not agree.
    StaleOrInconsistentAuthority,
    /// A protected query access has no exact read binding.
    MissingReadBinding,
    /// A compiler-declared local relationship schema is unavailable.
    InvalidRelationshipPlan,
    /// A projected candidate set was malformed, incomplete, or exceeded its fixed ceiling.
    InvalidProjectedCandidateSet,
}

/// Fixed server-owned ceiling for one projected row-policy admission pass.
///
/// This is deliberately independent of caller-visible scan budgets: hidden
/// rows cannot consume or alter that budget, while authoritative admission
/// itself remains bounded.
pub const MAX_PROJECTED_POLICY_CANDIDATES_V1: usize = 100_000;

/// One authoritative observation for a projection candidate.
///
/// Construction is hidden from public application surfaces. First-party
/// storage adapters create observations while one authoritative read snapshot
/// remains open. `None` represents a projection key whose current row no
/// longer exists and is therefore denied.
#[doc(hidden)]
pub struct ProjectedPolicyCandidateObservationV1 {
    key: EntityKey,
    current: Option<(CanonicalRecord, Vec<bool>)>,
}

impl ProjectedPolicyCandidateObservationV1 {
    /// Records one missing current row (deny).
    #[doc(hidden)]
    #[must_use]
    pub fn missing(key: EntityKey) -> Self {
        Self { key, current: None }
    }

    /// Records one current row and its exact indexed-relationship evidence.
    #[doc(hidden)]
    #[must_use]
    pub fn current(key: EntityKey, row: CanonicalRecord, relationship_exists: Vec<bool>) -> Self {
        Self {
            key,
            current: Some((row, relationship_exists)),
        }
    }
}

/// Move-only proof that one complete projected candidate set was evaluated by
/// the current compiled row policy.
///
/// The proof carries no row values and has no public constructor. It is bound
/// to one entity and the exact ordered candidate set, so a stale, partial, or
/// caller-fabricated allow list cannot be substituted at the columnar boundary.
#[derive(Eq, PartialEq)]
pub struct AuthorizedProjectedRowAdmissionV1 {
    entity: EntityTypeId,
    // Strictly sorted once at admission; owns each key once alongside its
    // decision, without requiring an observation slice to outlive the proof.
    candidates: Vec<(EntityKey, bool)>,
    admitted_count: usize,
}

impl std::fmt::Debug for AuthorizedProjectedRowAdmissionV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthorizedProjectedRowAdmissionV1")
            .field("entity", &self.entity)
            .field("candidate_count", &self.candidates.len())
            .field("admitted_count", &self.admitted_count)
            .finish()
    }
}

impl AuthorizedProjectedRowAdmissionV1 {
    /// Constructs an exact admission proof for cross-crate semantic tests.
    ///
    /// Shipped code cannot enable this constructor.
    #[cfg(feature = "test-fixtures")]
    #[doc(hidden)]
    pub fn test_fixture(
        entity: EntityTypeId,
        candidates: Vec<EntityKey>,
        admitted: Vec<EntityKey>,
    ) -> Option<Self> {
        let candidates = candidates.into_iter().collect::<BTreeSet<_>>();
        let admitted = admitted.into_iter().collect::<BTreeSet<_>>();
        (candidates.len() <= MAX_PROJECTED_POLICY_CANDIDATES_V1
            && candidates
                .iter()
                .all(|candidate| candidate.entity_type_id() == entity)
            && admitted.is_subset(&candidates))
        .then(|| Self {
            entity,
            admitted_count: admitted.len(),
            candidates: candidates
                .into_iter()
                .map(|key| {
                    let allowed = admitted.contains(&key);
                    (key, allowed)
                })
                .collect(),
        })
    }

    /// Whether the proof is bound to `entity` and the complete candidate set.
    #[doc(hidden)]
    #[must_use]
    pub fn covers(&self, entity: EntityTypeId, candidates: &BTreeSet<EntityKey>) -> bool {
        self.entity == entity
            && self.candidates.len() == candidates.len()
            && self.candidates.iter().map(|(key, _)| key).eq(candidates)
    }

    /// Whether one covered key was admitted.
    #[doc(hidden)]
    #[must_use]
    pub fn admits(&self, key: &EntityKey) -> bool {
        self.candidates
            .binary_search_by(|(candidate, _)| candidate.cmp(key))
            .is_ok_and(|position| self.candidates[position].1)
    }
}

/// Failure to reconstruct compiler-owned policy authority for a command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommandRowPolicyContextErrorV1 {
    /// The current capability extension and active contract do not agree.
    StaleOrInconsistentAuthority,
    /// A protected mutation has no binding for its exact operation.
    MissingOperationBinding,
    /// A compiler-declared local relationship schema is unavailable.
    InvalidRelationshipPlan,
}

/// One exact local index lookup derived from policy IR and trusted row/fact values.
///
/// Callers may only receive this value from [`AuthorizedQueryRowPolicyContextV1`].
/// No public request can choose its target, partition, index, or key bytes.
#[derive(Clone, Eq, PartialEq)]
pub struct AuthorizedIndexedRelationshipLookupV1 {
    target_entity: EntityTypeId,
    index_id: IndexId,
    partition: PartitionKey,
    index_prefix: Vec<u8>,
}

impl AuthorizedIndexedRelationshipLookupV1 {
    /// Compiler-selected target entity.
    #[doc(hidden)]
    #[must_use]
    pub const fn target_entity(&self) -> EntityTypeId {
        self.target_entity
    }

    /// Compiler-selected target index.
    #[doc(hidden)]
    #[must_use]
    pub const fn index_id(&self) -> IndexId {
        self.index_id
    }

    /// Exact partition derived from compiler-checked leading arguments.
    #[doc(hidden)]
    #[must_use]
    pub const fn partition(&self) -> &PartitionKey {
        &self.partition
    }

    /// Complete index-value prefix derived from trusted operands.
    #[doc(hidden)]
    #[must_use]
    pub fn index_prefix(&self) -> &[u8] {
        &self.index_prefix
    }
}

impl std::fmt::Debug for AuthorizedIndexedRelationshipLookupV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthorizedIndexedRelationshipLookupV1")
            .field("target_entity", &self.target_entity)
            .field("index_id", &self.index_id)
            .field("key", &"[REDACTED]")
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
struct AuthorizedRelationshipPlanV1 {
    target_entity: EntityTypeId,
    index_id: IndexId,
    index_schema: KeySchema,
    partition_schema: KeySchema,
    partition_width: usize,
}

#[derive(Clone, Eq, PartialEq)]
struct AuthorizedEntityPolicyV1 {
    policy: RowPolicyPlanV1,
    relationships: BTreeMap<(EntityTypeId, IndexId), AuthorizedRelationshipPlanV1>,
    primary_key_fields: Vec<FieldId>,
    primary_key: KeySchema,
}

/// Move-only transaction-current policy authority retained by the command lane.
///
/// The context contains no caller-provided predicate. It is resolved from one
/// fresh command authorization proof and the exact executable contract bundle.
#[derive(Eq, PartialEq)]
pub struct AuthorizedCommandRowPolicyContextV1 {
    authority: AuthorizedRowPolicyAuthority,
    policies: BTreeMap<EntityTypeId, AuthorizedEntityPolicyV1>,
}

impl std::fmt::Debug for AuthorizedCommandRowPolicyContextV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthorizedCommandRowPolicyContextV1")
            .field(
                "protected_entities",
                &self.policies.keys().collect::<Vec<_>>(),
            )
            .field("authority", &"[REDACTED]")
            .finish()
    }
}

impl AuthorizedCommandRowPolicyContextV1 {
    /// Exact transaction-current authority used to construct this context.
    #[doc(hidden)]
    #[must_use]
    pub const fn internal_authority(&self) -> &AuthorizedRowPolicyAuthority {
        &self.authority
    }

    /// Whether an entity is protected by the active contract.
    #[doc(hidden)]
    #[must_use]
    pub fn protects(&self, entity: EntityTypeId) -> bool {
        self.policies.contains_key(&entity)
    }

    /// Derives every exact relationship lookup needed for current and successor rows.
    #[doc(hidden)]
    pub fn relationship_lookups(
        &self,
        entity: EntityTypeId,
        operation: RowPolicyOperationV1,
        current: Option<&CanonicalRecord>,
        successor: Option<&CanonicalRecord>,
    ) -> Result<Vec<AuthorizedIndexedRelationshipLookupV1>, CommandRowPolicyContextErrorV1> {
        let selected = self.selection(entity, operation)?;
        let rows = transition_rows(operation, current, successor)
            .ok_or(CommandRowPolicyContextErrorV1::StaleOrInconsistentAuthority)?;
        let mut lookups = Vec::new();
        for row in rows {
            let probes = required_indexed_relationship_probes(
                &selected.policy,
                operation,
                row,
                self.authority.internal_principal(),
            )
            .map_err(|_| CommandRowPolicyContextErrorV1::StaleOrInconsistentAuthority)?;
            for probe in probes {
                lookups.push(lookup_from_probe(selected, probe)?);
            }
        }
        Ok(lookups)
    }

    /// Evaluates one exact transaction-current mutation transition.
    #[doc(hidden)]
    #[must_use]
    pub fn allows_transition(
        &self,
        entity: EntityTypeId,
        operation: RowPolicyOperationV1,
        current: Option<&CanonicalRecord>,
        successor: Option<&CanonicalRecord>,
        relationship_exists: &[bool],
    ) -> bool {
        let Ok(selected) = self.selection(entity, operation) else {
            return false;
        };
        let Some(rows) = transition_rows(operation, current, successor) else {
            return false;
        };
        let mut probes = Vec::new();
        for row in rows {
            let Ok(required) = required_indexed_relationship_probes(
                &selected.policy,
                operation,
                row,
                self.authority.internal_principal(),
            ) else {
                return false;
            };
            probes.extend(required);
        }
        if probes.len() != relationship_exists.len() {
            return false;
        }
        let evidence = probes
            .into_iter()
            .zip(relationship_exists)
            .map(|(probe, exists)| probe.into_evidence(*exists))
            .collect::<Vec<_>>();
        evaluate_row_transition(
            &selected.policy,
            operation,
            current,
            successor,
            self.authority.internal_principal(),
            &evidence,
        )
        .is_allowed()
    }

    fn selection(
        &self,
        entity: EntityTypeId,
        operation: RowPolicyOperationV1,
    ) -> Result<&AuthorizedEntityPolicyV1, CommandRowPolicyContextErrorV1> {
        let selected = self
            .policies
            .get(&entity)
            .ok_or(CommandRowPolicyContextErrorV1::MissingOperationBinding)?;
        let required = capability_operation(operation);
        let binding = self
            .authority
            .internal_grant()
            .bindings()
            .iter()
            .find(|binding| binding.entity_type() == entity)
            .ok_or(CommandRowPolicyContextErrorV1::MissingOperationBinding)?;
        if !binding.operations().contains(&required) {
            return Err(CommandRowPolicyContextErrorV1::MissingOperationBinding);
        }
        Ok(selected)
    }
}

/// Move-only transaction-current policy context for one authorized query.
///
/// Construction consumes only an [`AuthorizedApplicationQuery`] plus its exact
/// active contract. Principal facts and policy selection remain private and
/// cannot be supplied by an application request.
#[derive(Eq, PartialEq)]
pub struct AuthorizedQueryRowPolicyContextV1 {
    bundle: Option<riffdb_types::ContractBundleHash>,
    authority: Option<AuthorizedRowPolicyAuthority>,
    principal: PrincipalFactBindingV1,
    policies: BTreeMap<EntityTypeId, AuthorizedEntityPolicyV1>,
}

/// One compiler-anchored event candidate supplied only to first-party policy
/// execution. It is an input to evaluation, never an allow decision.
#[derive(Clone, Eq, PartialEq)]
pub struct EventPolicyCandidateV1 {
    event_id: EventId,
    source_key: EntityKey,
    read_policy: RowPolicyName,
}

impl EventPolicyCandidateV1 {
    /// Binds one durable event to its compiler-owned source key and policy.
    #[doc(hidden)]
    #[must_use]
    pub const fn new(event_id: EventId, source_key: EntityKey, read_policy: RowPolicyName) -> Self {
        Self {
            event_id,
            source_key,
            read_policy,
        }
    }

    /// Stable event identity.
    #[doc(hidden)]
    #[must_use]
    pub const fn event_id(&self) -> EventId {
        self.event_id
    }

    /// Exact compiler-owned current-row key.
    #[doc(hidden)]
    #[must_use]
    pub const fn source_key(&self) -> &EntityKey {
        &self.source_key
    }

    /// Exact selected read-policy symbol.
    #[doc(hidden)]
    #[must_use]
    pub const fn read_policy(&self) -> &RowPolicyName {
        &self.read_policy
    }
}

impl std::fmt::Debug for EventPolicyCandidateV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EventPolicyCandidateV1")
            .field("event_id", &self.event_id)
            .field("entity", &self.source_key.entity_type_id())
            .field("policy", &self.read_policy)
            .field("key", &"[REDACTED]")
            .finish()
    }
}

/// Move-only proof that one exact event may be released under current-row authority.
///
/// It binds the stable event identity and compiler-owned source key to the same
/// capability revision and current-row hash carried by the shared policy proof.
pub struct AuthorizedEventReleaseProofV1 {
    event_id: EventId,
    source_key: EntityKey,
    row_policy: RowPolicyProofV1,
}

impl std::fmt::Debug for AuthorizedEventReleaseProofV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthorizedEventReleaseProofV1")
            .field("event_id", &self.event_id)
            .field("entity", &self.source_key.entity_type_id())
            .field("authority", &"[REDACTED]")
            .finish()
    }
}

impl AuthorizedEventReleaseProofV1 {
    /// Stable event identity covered by this release proof.
    #[must_use]
    pub const fn event_id(&self) -> EventId {
        self.event_id
    }

    /// Exact current capability revision used by policy evaluation.
    #[must_use]
    pub const fn capability_revision(&self) -> NonZeroU64 {
        self.row_policy.capability_revision()
    }
}

/// Closed event-release result. Denial is indistinguishable absence to consumers.
#[derive(Debug)]
pub enum EventPolicyReleaseDecisionV1 {
    /// Exact event/key/revision/current-row proof.
    Allow(AuthorizedEventReleaseProofV1),
    /// Deny without exposing the event, source row, or failure detail publicly.
    Deny(RowPolicyDenyReasonV1),
}

impl EventPolicyReleaseDecisionV1 {
    /// Whether current authority allows release.
    #[must_use]
    pub const fn is_allowed(&self) -> bool {
        matches!(self, Self::Allow(_))
    }

    /// Whether release is denied as indistinguishable absence.
    #[must_use]
    pub const fn is_denied(&self) -> bool {
        matches!(self, Self::Deny(_))
    }
}

impl std::fmt::Debug for AuthorizedQueryRowPolicyContextV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthorizedQueryRowPolicyContextV1")
            .field(
                "protected_entities",
                &self.policies.keys().collect::<Vec<_>>(),
            )
            .field("principal_facts", &"[REDACTED]")
            .finish()
    }
}

impl AuthorizedQueryRowPolicyContextV1 {
    /// Constructs a no-relationship context for cross-crate semantic tests.
    ///
    /// Shipped code cannot enable this constructor. Relationship-bearing plans
    /// are rejected so tests cannot accidentally exercise weaker evidence.
    #[cfg(feature = "test-fixtures")]
    #[doc(hidden)]
    pub fn test_fixture(
        principal: PrincipalFactBindingV1,
        policies: Vec<RowPolicyPlanV1>,
        schema: &SchemaIr,
    ) -> Result<Self, QueryRowPolicyContextErrorV1> {
        let mut selected = BTreeMap::new();
        for policy in policies {
            if policy.rules().iter().any(|rule| {
                rule.nodes()
                    .iter()
                    .any(|node| matches!(node, RowPolicyExpressionNodeV1::IndexedExists { .. }))
            }) || selected
                .insert(policy.entity(), {
                    let entity = schema
                        .entity(policy.entity())
                        .ok_or(QueryRowPolicyContextErrorV1::InvalidRelationshipPlan)?;
                    AuthorizedEntityPolicyV1 {
                        policy,
                        relationships: BTreeMap::new(),
                        primary_key_fields: entity.primary_key_fields().to_vec(),
                        primary_key: entity.primary_key().clone(),
                    }
                })
                .is_some()
            {
                return Err(QueryRowPolicyContextErrorV1::InvalidRelationshipPlan);
            }
        }
        Ok(Self {
            bundle: None,
            authority: None,
            principal,
            policies: selected,
        })
    }

    /// Whether this exact query access is protected by a selected row policy.
    #[doc(hidden)]
    #[must_use]
    pub fn protects(&self, entity: EntityTypeId) -> bool {
        self.policies.contains_key(&entity)
    }

    /// Exact current capability identity used to construct shipped policy
    /// contexts. Test fixtures deliberately have no durable authority.
    #[doc(hidden)]
    #[must_use]
    pub fn internal_capability_identity(&self) -> Option<(CapabilityId, NonZeroU64)> {
        self.authority.as_ref().map(|authority| {
            (
                authority.internal_principal().capability_id(),
                authority.internal_principal().revision(),
            )
        })
    }

    /// Exact V4 grant which must still match at the final storage safe point.
    #[doc(hidden)]
    #[must_use]
    pub fn internal_row_policy_grant(&self) -> Option<&riffdb_types::CapabilityRowPolicyGrantV1> {
        self.authority
            .as_ref()
            .map(AuthorizedRowPolicyAuthority::internal_grant)
    }

    /// Exact current capability and selected policy identity for disposable ANN reuse.
    /// This identifies admission inputs; callers must still evaluate every row.
    /// Unbound or fixture-only contexts decline the optional optimization.
    #[doc(hidden)]
    pub fn internal_vector_cache_identity(&self, entity: EntityTypeId) -> Option<Vec<u8>> {
        let (capability, revision) = self.internal_capability_identity()?;
        let selected = self.policies.get(&entity)?;
        let grant = self.internal_row_policy_grant()?;
        // The compiled role hash is the existing authoritative policy identity,
        // not a hash substitute for the admitted vector population.
        let mut bytes = Vec::with_capacity(96 + selected.policy.name().len());
        bytes.extend_from_slice(self.bundle?.as_bytes());
        bytes.extend_from_slice(capability.as_bytes());
        bytes.extend_from_slice(&revision.get().to_be_bytes());
        bytes.extend_from_slice(grant.application_role_hash().as_bytes());
        bytes.extend_from_slice(&entity.to_be_bytes());
        bytes.extend_from_slice(selected.policy.name().as_bytes());
        Some(bytes)
    }

    /// Derives the complete bounded relationship lookups required for a row.
    #[doc(hidden)]
    pub fn relationship_lookups(
        &self,
        entity: EntityTypeId,
        row: &CanonicalRecord,
    ) -> Result<Vec<AuthorizedIndexedRelationshipLookupV1>, QueryRowPolicyContextErrorV1> {
        let Some(selected) = self.policies.get(&entity) else {
            return Ok(Vec::new());
        };
        required_indexed_relationship_probes(
            &selected.policy,
            RowPolicyOperationV1::Read,
            row,
            &self.principal,
        )
        .map_err(|_| QueryRowPolicyContextErrorV1::StaleOrInconsistentAuthority)?
        .into_iter()
        .map(|probe| {
            let relation = selected
                .relationships
                .get(&(probe.target_entity(), probe.index_id()))
                .ok_or(QueryRowPolicyContextErrorV1::InvalidRelationshipPlan)?;
            let partition = relation
                .partition_schema
                .encode_partition(
                    probe
                        .arguments()
                        .get(..relation.partition_width)
                        .ok_or(QueryRowPolicyContextErrorV1::InvalidRelationshipPlan)?,
                )
                .map_err(|_| QueryRowPolicyContextErrorV1::InvalidRelationshipPlan)?;
            let index_prefix = relation
                .index_schema
                .encode_index_prefix(probe.arguments())
                .map_err(|_| QueryRowPolicyContextErrorV1::InvalidRelationshipPlan)?
                .as_bytes()
                .to_vec();
            Ok(AuthorizedIndexedRelationshipLookupV1 {
                target_entity: relation.target_entity,
                index_id: relation.index_id,
                partition,
                index_prefix,
            })
        })
        .collect()
    }

    /// Conservative indexed-relationship dependencies of one selected read
    /// policy. Derived workers use these to invalidate membership when another
    /// entity changes; absence of a probe for one row is not independence.
    #[doc(hidden)]
    pub fn internal_relationship_entities(
        &self,
        entity: EntityTypeId,
    ) -> impl Iterator<Item = EntityTypeId> + '_ {
        self.policies
            .get(&entity)
            .into_iter()
            .flat_map(|selected| selected.relationships.keys().map(|(target, _)| *target))
    }

    /// Applies the exact selected read policy to one authoritative row and the
    /// relationship observations requested by [`Self::relationship_lookups`].
    #[doc(hidden)]
    #[must_use]
    pub fn allows(
        &self,
        entity: EntityTypeId,
        row: &CanonicalRecord,
        relationship_exists: &[bool],
    ) -> bool {
        let Some(selected) = self.policies.get(&entity) else {
            return true;
        };
        let Ok(probes) = required_indexed_relationship_probes(
            &selected.policy,
            RowPolicyOperationV1::Read,
            row,
            &self.principal,
        ) else {
            return false;
        };
        if probes.len() != relationship_exists.len() {
            return false;
        }
        let evidence = probes
            .into_iter()
            .zip(relationship_exists)
            .map(|(probe, exists)| probe.into_evidence(*exists))
            .collect::<Vec<_>>();
        evaluate_row_policy(
            &selected.policy,
            RowPolicyOperationV1::Read,
            row,
            &self.principal,
            &evidence,
        )
        .is_allowed()
    }

    /// Produces an event/key-bound proof only after the exact selected read
    /// policy accepts the authoritative current row and relationship evidence.
    #[doc(hidden)]
    #[must_use]
    pub fn authorize_event_release(
        &self,
        candidate: &EventPolicyCandidateV1,
        row: &CanonicalRecord,
        relationship_exists: &[bool],
    ) -> EventPolicyReleaseDecisionV1 {
        let event_id = candidate.event_id();
        let source_key = candidate.source_key();
        let entity = source_key.entity_type_id();
        let Some(selected) = self.policies.get(&entity) else {
            return EventPolicyReleaseDecisionV1::Deny(RowPolicyDenyReasonV1::MissingRule);
        };
        if selected.policy.name() != candidate.read_policy().as_str()
            || row_entity_key(selected, row).as_ref() != Some(source_key)
        {
            return EventPolicyReleaseDecisionV1::Deny(RowPolicyDenyReasonV1::InvalidPlanOrRow);
        }
        let Ok(probes) = required_indexed_relationship_probes(
            &selected.policy,
            RowPolicyOperationV1::Read,
            row,
            &self.principal,
        ) else {
            return EventPolicyReleaseDecisionV1::Deny(RowPolicyDenyReasonV1::InvalidPlanOrRow);
        };
        if probes.len() != relationship_exists.len() {
            return EventPolicyReleaseDecisionV1::Deny(
                RowPolicyDenyReasonV1::MissingOrInvalidRelationshipEvidence,
            );
        }
        let evidence = probes
            .into_iter()
            .zip(relationship_exists)
            .map(|(probe, exists)| probe.into_evidence(*exists))
            .collect::<Vec<_>>();
        match evaluate_row_policy(
            &selected.policy,
            RowPolicyOperationV1::Read,
            row,
            &self.principal,
            &evidence,
        ) {
            RowPolicyDecisionV1::Allow(row_policy) => {
                EventPolicyReleaseDecisionV1::Allow(AuthorizedEventReleaseProofV1 {
                    event_id,
                    source_key: source_key.clone(),
                    row_policy,
                })
            }
            RowPolicyDecisionV1::Deny(reason) => EventPolicyReleaseDecisionV1::Deny(reason),
        }
    }

    /// Evaluates a complete authoritative observation set for one projected
    /// entity and returns an opaque candidate-bound admission proof.
    ///
    /// Observations must be in strict canonical-key order, contain every
    /// candidate exactly once, and remain under the fixed server ceiling.
    /// First-party storage adapters call this only while their authoritative
    /// read snapshot remains open.
    #[doc(hidden)]
    pub fn authorize_projected_candidates(
        &self,
        entity: EntityTypeId,
        observations: Vec<ProjectedPolicyCandidateObservationV1>,
    ) -> Result<AuthorizedProjectedRowAdmissionV1, QueryRowPolicyContextErrorV1> {
        if observations.len() > MAX_PROJECTED_POLICY_CANDIDATES_V1 {
            return Err(QueryRowPolicyContextErrorV1::InvalidProjectedCandidateSet);
        }
        let mut candidates = Vec::<(EntityKey, bool)>::with_capacity(observations.len());
        let mut admitted_count = 0;
        for observation in observations {
            if observation.key.entity_type_id() != entity
                || candidates
                    .last()
                    .is_some_and(|(prior, _)| prior >= &observation.key)
            {
                return Err(QueryRowPolicyContextErrorV1::InvalidProjectedCandidateSet);
            }
            let allowed = observation
                .current
                .as_ref()
                .is_some_and(|(row, evidence)| self.allows(entity, row, evidence));
            admitted_count += usize::from(allowed);
            candidates.push((observation.key, allowed));
        }
        Ok(AuthorizedProjectedRowAdmissionV1 {
            entity,
            candidates,
            admitted_count,
        })
    }
}

/// Re-evaluates one event release at the final safe point. Event identity,
/// source key, capability revision, current row, relationships, and predicate
/// must all remain exact or release is denied.
#[doc(hidden)]
#[must_use]
pub fn revalidate_event_release_proof(
    prior: &AuthorizedEventReleaseProofV1,
    context: &AuthorizedQueryRowPolicyContextV1,
    event_id: EventId,
    source_key: &EntityKey,
    row: &CanonicalRecord,
    relationship_exists: &[bool],
) -> EventPolicyReleaseDecisionV1 {
    if prior.event_id != event_id || &prior.source_key != source_key {
        return EventPolicyReleaseDecisionV1::Deny(RowPolicyDenyReasonV1::StaleAuthority);
    }
    let Some(selected) = context.policies.get(&source_key.entity_type_id()) else {
        return EventPolicyReleaseDecisionV1::Deny(RowPolicyDenyReasonV1::StaleAuthority);
    };
    if row_entity_key(selected, row).as_ref() != Some(source_key) {
        return EventPolicyReleaseDecisionV1::Deny(RowPolicyDenyReasonV1::StaleAuthority);
    }
    let Ok(probes) = required_indexed_relationship_probes(
        &selected.policy,
        RowPolicyOperationV1::Read,
        row,
        &context.principal,
    ) else {
        return EventPolicyReleaseDecisionV1::Deny(RowPolicyDenyReasonV1::StaleAuthority);
    };
    if probes.len() != relationship_exists.len() {
        return EventPolicyReleaseDecisionV1::Deny(RowPolicyDenyReasonV1::StaleAuthority);
    }
    let evidence = probes
        .into_iter()
        .zip(relationship_exists)
        .map(|(probe, exists)| probe.into_evidence(*exists))
        .collect::<Vec<_>>();
    match revalidate_row_policy_proof(
        &prior.row_policy,
        &selected.policy,
        Some(row),
        None,
        &context.principal,
        &evidence,
    ) {
        RowPolicyDecisionV1::Allow(row_policy) => {
            EventPolicyReleaseDecisionV1::Allow(AuthorizedEventReleaseProofV1 {
                event_id,
                source_key: source_key.clone(),
                row_policy,
            })
        }
        RowPolicyDecisionV1::Deny(reason) => EventPolicyReleaseDecisionV1::Deny(reason),
    }
}

fn row_entity_key(selected: &AuthorizedEntityPolicyV1, row: &CanonicalRecord) -> Option<EntityKey> {
    let values = selected
        .primary_key_fields
        .iter()
        .map(|field_id| {
            row.fields()
                .binary_search_by_key(field_id, |(candidate, _)| *candidate)
                .ok()
                .map(|index| row.fields()[index].1.clone())
        })
        .collect::<Option<Vec<_>>>()?;
    selected.primary_key.encode_entity(&values).ok()
}

/// Resolves exact query policy context from a current authorization proof.
///
/// `None` means the capability has no V4 row-policy authority and therefore
/// the existing unprotected execution path remains exact.
#[doc(hidden)]
pub fn resolve_authorized_query_row_policy_context(
    authorization: &AuthorizedApplicationQuery,
    bundle: &ContractBundle,
) -> Result<Option<AuthorizedQueryRowPolicyContextV1>, QueryRowPolicyContextErrorV1> {
    let Some(authority) = authorization.internal_row_policy_authority() else {
        return Ok(None);
    };
    if authorization.target().lineage() != bundle.lineage()
        || authorization.target().version() != bundle.contract_version()
        || authorization.target().bundle_hash() != bundle.bundle_hash()
    {
        return Err(QueryRowPolicyContextErrorV1::StaleOrInconsistentAuthority);
    }
    let grant = authority.internal_grant();
    let mut policies = BTreeMap::new();
    for access in authorization.target().accesses() {
        let entity = access.entity_type_id();
        let protected = bundle
            .row_policies()
            .policies()
            .iter()
            .any(|policy| policy.entity() == entity);
        let binding = grant.bindings().iter().find(|binding| {
            binding.lineage() == bundle.lineage() && binding.entity_type() == entity
        });
        let Some(binding) = binding else {
            if protected {
                return Err(QueryRowPolicyContextErrorV1::MissingReadBinding);
            }
            continue;
        };
        if !binding
            .operations()
            .contains(&CapabilityRowPolicyOperationV1::Read)
        {
            return Err(QueryRowPolicyContextErrorV1::MissingReadBinding);
        }
        let policy = bundle
            .row_policies()
            .policies()
            .iter()
            .find(|policy| {
                policy.name() == binding.policy_name().as_str() && policy.entity() == entity
            })
            .cloned()
            .ok_or(QueryRowPolicyContextErrorV1::StaleOrInconsistentAuthority)?;
        let mut relationships = BTreeMap::new();
        for rule in policy
            .rules()
            .iter()
            .filter(|rule| rule.operation() == RowPolicyOperationV1::Read)
        {
            for node in rule.nodes() {
                let RowPolicyExpressionNodeV1::IndexedExists {
                    target_entity,
                    index_id,
                    ..
                } = node
                else {
                    continue;
                };
                let target = bundle
                    .schema()
                    .entity(*target_entity)
                    .ok_or(QueryRowPolicyContextErrorV1::InvalidRelationshipPlan)?;
                let index = target
                    .indexes()
                    .iter()
                    .find(|index| index.id() == *index_id)
                    .ok_or(QueryRowPolicyContextErrorV1::InvalidRelationshipPlan)?;
                let aggregate = bundle
                    .schema()
                    .aggregate_for_entity(*target_entity)
                    .ok_or(QueryRowPolicyContextErrorV1::InvalidRelationshipPlan)?;
                let root = bundle
                    .schema()
                    .entity(aggregate.root())
                    .ok_or(QueryRowPolicyContextErrorV1::InvalidRelationshipPlan)?;
                relationships.insert(
                    (*target_entity, *index_id),
                    AuthorizedRelationshipPlanV1 {
                        target_entity: *target_entity,
                        index_id: *index_id,
                        index_schema: index.key_schema().clone(),
                        partition_schema: aggregate.keys().partition_schema().clone(),
                        partition_width: root.primary_key_fields().len(),
                    },
                );
            }
        }
        let entity_schema = bundle
            .schema()
            .entity(entity)
            .ok_or(QueryRowPolicyContextErrorV1::InvalidRelationshipPlan)?;
        policies.insert(
            entity,
            AuthorizedEntityPolicyV1 {
                policy,
                relationships,
                primary_key_fields: entity_schema.primary_key_fields().to_vec(),
                primary_key: entity_schema.primary_key().clone(),
            },
        );
    }
    Ok(Some(AuthorizedQueryRowPolicyContextV1 {
        bundle: Some(bundle.bundle_hash()),
        authority: Some(authority.clone()),
        principal: authority.internal_principal().clone(),
        policies,
    }))
}

/// Resolves current-row policy for one exact authorized vector inspection.
///
/// Unlike named-query resolution, the accessed entity is not supplied by an
/// application-shaped access program. It is recovered from the already
/// authorized closed operation and must match the compiler bundle and the
/// service-resolved entity exactly.
#[doc(hidden)]
pub fn resolve_authorized_vector_inspection_row_policy_context(
    authorization: &AuthorizedOperation,
    bundle: &ContractBundle,
    entity: EntityTypeId,
) -> Result<Option<AuthorizedQueryRowPolicyContextV1>, QueryRowPolicyContextErrorV1> {
    let requirement = authorization
        .request()
        .vector_inspection_requirement()
        .ok_or(QueryRowPolicyContextErrorV1::StaleOrInconsistentAuthority)?;
    if requirement.lineage != bundle.lineage() || requirement.entity_type_id != entity {
        return Err(QueryRowPolicyContextErrorV1::StaleOrInconsistentAuthority);
    }
    let protected = bundle
        .row_policies()
        .policies()
        .iter()
        .any(|policy| policy.entity() == entity);
    let Some(authority) = authorization.internal_row_policy_authority() else {
        return if protected {
            Err(QueryRowPolicyContextErrorV1::MissingReadBinding)
        } else {
            Ok(None)
        };
    };
    let binding = authority
        .internal_grant()
        .bindings()
        .iter()
        .find(|binding| binding.lineage() == bundle.lineage() && binding.entity_type() == entity);
    let Some(binding) = binding else {
        return if protected {
            Err(QueryRowPolicyContextErrorV1::MissingReadBinding)
        } else {
            Ok(None)
        };
    };
    if !binding
        .operations()
        .contains(&CapabilityRowPolicyOperationV1::Read)
    {
        return Err(QueryRowPolicyContextErrorV1::MissingReadBinding);
    }
    let policy = bundle
        .row_policies()
        .policies()
        .iter()
        .find(|policy| policy.name() == binding.policy_name().as_str() && policy.entity() == entity)
        .cloned()
        .ok_or(QueryRowPolicyContextErrorV1::StaleOrInconsistentAuthority)?;
    let mut relationships = BTreeMap::new();
    for rule in policy
        .rules()
        .iter()
        .filter(|rule| rule.operation() == RowPolicyOperationV1::Read)
    {
        for node in rule.nodes() {
            let RowPolicyExpressionNodeV1::IndexedExists {
                target_entity,
                index_id,
                ..
            } = node
            else {
                continue;
            };
            let target = bundle
                .schema()
                .entity(*target_entity)
                .ok_or(QueryRowPolicyContextErrorV1::InvalidRelationshipPlan)?;
            let index = target
                .indexes()
                .iter()
                .find(|index| index.id() == *index_id)
                .ok_or(QueryRowPolicyContextErrorV1::InvalidRelationshipPlan)?;
            let aggregate = bundle
                .schema()
                .aggregate_for_entity(*target_entity)
                .ok_or(QueryRowPolicyContextErrorV1::InvalidRelationshipPlan)?;
            let root = bundle
                .schema()
                .entity(aggregate.root())
                .ok_or(QueryRowPolicyContextErrorV1::InvalidRelationshipPlan)?;
            relationships.insert(
                (*target_entity, *index_id),
                AuthorizedRelationshipPlanV1 {
                    target_entity: *target_entity,
                    index_id: *index_id,
                    index_schema: index.key_schema().clone(),
                    partition_schema: aggregate.keys().partition_schema().clone(),
                    partition_width: root.primary_key_fields().len(),
                },
            );
        }
    }
    let entity_schema = bundle
        .schema()
        .entity(entity)
        .ok_or(QueryRowPolicyContextErrorV1::InvalidRelationshipPlan)?;
    Ok(Some(AuthorizedQueryRowPolicyContextV1 {
        bundle: Some(bundle.bundle_hash()),
        authority: Some(authority.clone()),
        principal: authority.internal_principal().clone(),
        policies: BTreeMap::from([(
            entity,
            AuthorizedEntityPolicyV1 {
                policy,
                relationships,
                primary_key_fields: entity_schema.primary_key_fields().to_vec(),
                primary_key: entity_schema.primary_key().clone(),
            },
        )]),
    }))
}

/// Resolves the complete read-policy context for a principal-filtered export.
///
/// The entity set comes only from the exact immutable compiler bundle. A
/// whole-application operator export has no row-policy context; a principal
/// export without current V4 policy authority fails closed.
#[doc(hidden)]
pub fn resolve_authorized_application_export_row_policy_context(
    authorization: &AuthorizedApplicationExportV1,
    bundle: &ContractBundle,
) -> Result<Option<AuthorizedQueryRowPolicyContextV1>, QueryRowPolicyContextErrorV1> {
    if authorization.request().selection().lineage() != bundle.lineage() {
        return Err(QueryRowPolicyContextErrorV1::StaleOrInconsistentAuthority);
    }
    if authorization.request().selection().scope()
        == riffdb_types::CapabilityApplicationExportScopeV1::WholeApplication
    {
        return Ok(None);
    }
    let authority = authorization
        .internal_row_policy_authority()
        .ok_or(QueryRowPolicyContextErrorV1::MissingReadBinding)?;
    let policies = resolve_read_policies(
        authority,
        bundle,
        bundle.schema().entities().iter().map(|entity| entity.id()),
    )?;
    Ok(Some(AuthorizedQueryRowPolicyContextV1 {
        bundle: Some(bundle.bundle_hash()),
        authority: Some(authority.clone()),
        principal: authority.internal_principal().clone(),
        policies,
    }))
}

/// Resolves one contextual hydration policy context from the exact current
/// subscription authorization and compiler-derived hydration entity union.
///
/// The entity identities are supplied only by shared service orchestration
/// after resolving the exact reactive module and operation bound into the
/// authorization request. No transport or application request can provide
/// this list.
#[doc(hidden)]
pub fn resolve_authorized_contextual_row_policy_context(
    authorization: &AuthorizedOperation,
    bundle: &ContractBundle,
    entities: &[EntityTypeId],
) -> Result<Option<AuthorizedQueryRowPolicyContextV1>, QueryRowPolicyContextErrorV1> {
    let target = authorization
        .request()
        .contextual_subscription_target()
        .ok_or(QueryRowPolicyContextErrorV1::StaleOrInconsistentAuthority)?;
    if target.lineage() != bundle.lineage()
        || target.version() != bundle.contract_version()
        || target.bundle_hash() != bundle.bundle_hash()
        || entities.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(QueryRowPolicyContextErrorV1::StaleOrInconsistentAuthority);
    }
    let Some(authority) = authorization.internal_row_policy_authority() else {
        return Ok(None);
    };
    let policies = resolve_read_policies(authority, bundle, entities.iter().copied())?;
    Ok(Some(AuthorizedQueryRowPolicyContextV1 {
        bundle: Some(bundle.bundle_hash()),
        authority: Some(authority.clone()),
        principal: authority.internal_principal().clone(),
        policies,
    }))
}

/// Resolves the complete current-row policy context for compiler-anchored
/// trigger events in one plain or contextual durable-consumer pull.
#[doc(hidden)]
pub fn resolve_authorized_event_row_policy_context(
    authorization: &AuthorizedOperation,
    bundle: &ContractBundle,
    entities: &[EntityTypeId],
) -> Result<Option<AuthorizedQueryRowPolicyContextV1>, QueryRowPolicyContextErrorV1> {
    let target = authorization
        .request()
        .event_delivery_target()
        .ok_or(QueryRowPolicyContextErrorV1::StaleOrInconsistentAuthority)?;
    if target.lineage() != bundle.lineage()
        || target.version() != bundle.contract_version()
        || target.bundle_hash() != bundle.bundle_hash()
        || entities.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(QueryRowPolicyContextErrorV1::StaleOrInconsistentAuthority);
    }
    let Some(authority) = authorization.internal_row_policy_authority() else {
        return Ok(None);
    };
    let policies = resolve_read_policies(authority, bundle, entities.iter().copied())?;
    Ok(Some(AuthorizedQueryRowPolicyContextV1 {
        bundle: Some(bundle.bundle_hash()),
        authority: Some(authority.clone()),
        principal: authority.internal_principal().clone(),
        policies,
    }))
}

/// Resolves current-row authority for bounded operator replay or tail.
///
/// Unlike durable consumers, replay has no deployed reactive-module identity;
/// the exact active contract and symbolic selection are already resolved by
/// the catalog before this proof reaches storage. The move-only result still
/// binds the current capability revision, role, principal facts, and every
/// compiler-selected entity policy used by retained event anchors.
#[doc(hidden)]
pub fn resolve_authorized_event_replay_row_policy_context(
    authorization: &AuthorizedOperation,
    bundle: &ContractBundle,
    entities: &[EntityTypeId],
) -> Result<Option<AuthorizedQueryRowPolicyContextV1>, QueryRowPolicyContextErrorV1> {
    let (lineage, version) = authorization
        .request()
        .event_replay_contract()
        .ok_or(QueryRowPolicyContextErrorV1::StaleOrInconsistentAuthority)?;
    if lineage != bundle.lineage()
        || version != bundle.contract_version()
        || entities.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(QueryRowPolicyContextErrorV1::StaleOrInconsistentAuthority);
    }
    let Some(authority) = authorization.internal_row_policy_authority() else {
        return Ok(None);
    };
    let policies = resolve_read_policies(authority, bundle, entities.iter().copied())?;
    Ok(Some(AuthorizedQueryRowPolicyContextV1 {
        bundle: Some(bundle.bundle_hash()),
        authority: Some(authority.clone()),
        principal: authority.internal_principal().clone(),
        policies,
    }))
}

fn resolve_read_policies(
    authority: &AuthorizedRowPolicyAuthority,
    bundle: &ContractBundle,
    entities: impl IntoIterator<Item = EntityTypeId>,
) -> Result<BTreeMap<EntityTypeId, AuthorizedEntityPolicyV1>, QueryRowPolicyContextErrorV1> {
    let grant = authority.internal_grant();
    let mut policies = BTreeMap::new();
    for entity in entities {
        let protected = bundle
            .row_policies()
            .policies()
            .iter()
            .any(|policy| policy.entity() == entity);
        let binding = grant.bindings().iter().find(|binding| {
            binding.lineage() == bundle.lineage() && binding.entity_type() == entity
        });
        let Some(binding) = binding else {
            if protected {
                return Err(QueryRowPolicyContextErrorV1::MissingReadBinding);
            }
            continue;
        };
        if !binding
            .operations()
            .contains(&CapabilityRowPolicyOperationV1::Read)
        {
            return Err(QueryRowPolicyContextErrorV1::MissingReadBinding);
        }
        let policy = bundle
            .row_policies()
            .policies()
            .iter()
            .find(|policy| {
                policy.name() == binding.policy_name().as_str() && policy.entity() == entity
            })
            .cloned()
            .ok_or(QueryRowPolicyContextErrorV1::StaleOrInconsistentAuthority)?;
        let relationships = relationship_plans(bundle, &policy)
            .map_err(|_| QueryRowPolicyContextErrorV1::InvalidRelationshipPlan)?;
        let entity_schema = bundle
            .schema()
            .entity(entity)
            .ok_or(QueryRowPolicyContextErrorV1::InvalidRelationshipPlan)?;
        if policies
            .insert(
                entity,
                AuthorizedEntityPolicyV1 {
                    policy,
                    relationships,
                    primary_key_fields: entity_schema.primary_key_fields().to_vec(),
                    primary_key: entity_schema.primary_key().clone(),
                },
            )
            .is_some()
        {
            return Err(QueryRowPolicyContextErrorV1::StaleOrInconsistentAuthority);
        }
    }
    Ok(policies)
}

/// Resolves exact command policy authority from a fresh authorization proof.
///
/// `None` preserves the existing unprotected command path. A V4 authority is
/// accepted only when every selected binding resolves in the exact active
/// contract; the later commit verifier still checks the operation required by
/// each concrete mutation.
#[doc(hidden)]
pub fn resolve_authorized_command_row_policy_context(
    authorization: &AuthorizedCommandExecution,
    bundle: &ContractBundle,
) -> Result<Option<AuthorizedCommandRowPolicyContextV1>, CommandRowPolicyContextErrorV1> {
    let Some(authority) = authorization.internal_row_policy_authority() else {
        return Ok(None);
    };
    if authorization.lineage() != bundle.lineage()
        || authorization.version() != bundle.contract_version()
    {
        return Err(CommandRowPolicyContextErrorV1::StaleOrInconsistentAuthority);
    }
    let mut policies = BTreeMap::new();
    for binding in authority
        .internal_grant()
        .bindings()
        .iter()
        .filter(|binding| binding.lineage() == bundle.lineage())
    {
        let policy = bundle
            .row_policies()
            .policies()
            .iter()
            .find(|policy| {
                policy.name() == binding.policy_name().as_str()
                    && policy.entity() == binding.entity_type()
            })
            .cloned()
            .ok_or(CommandRowPolicyContextErrorV1::StaleOrInconsistentAuthority)?;
        let relationships = relationship_plans(bundle, &policy)
            .map_err(|_| CommandRowPolicyContextErrorV1::InvalidRelationshipPlan)?;
        let entity_schema = bundle
            .schema()
            .entity(binding.entity_type())
            .ok_or(CommandRowPolicyContextErrorV1::InvalidRelationshipPlan)?;
        if policies
            .insert(
                binding.entity_type(),
                AuthorizedEntityPolicyV1 {
                    policy,
                    relationships,
                    primary_key_fields: entity_schema.primary_key_fields().to_vec(),
                    primary_key: entity_schema.primary_key().clone(),
                },
            )
            .is_some()
        {
            return Err(CommandRowPolicyContextErrorV1::StaleOrInconsistentAuthority);
        }
    }
    Ok(Some(AuthorizedCommandRowPolicyContextV1 {
        authority: authority.clone(),
        policies,
    }))
}

fn relationship_plans(
    bundle: &ContractBundle,
    policy: &RowPolicyPlanV1,
) -> Result<
    BTreeMap<(EntityTypeId, IndexId), AuthorizedRelationshipPlanV1>,
    CommandRowPolicyContextErrorV1,
> {
    let mut relationships = BTreeMap::new();
    for rule in policy.rules() {
        for node in rule.nodes() {
            let RowPolicyExpressionNodeV1::IndexedExists {
                target_entity,
                index_id,
                ..
            } = node
            else {
                continue;
            };
            let target = bundle
                .schema()
                .entity(*target_entity)
                .ok_or(CommandRowPolicyContextErrorV1::InvalidRelationshipPlan)?;
            let index = target
                .indexes()
                .iter()
                .find(|index| index.id() == *index_id)
                .ok_or(CommandRowPolicyContextErrorV1::InvalidRelationshipPlan)?;
            let aggregate = bundle
                .schema()
                .aggregate_for_entity(*target_entity)
                .ok_or(CommandRowPolicyContextErrorV1::InvalidRelationshipPlan)?;
            let root = bundle
                .schema()
                .entity(aggregate.root())
                .ok_or(CommandRowPolicyContextErrorV1::InvalidRelationshipPlan)?;
            relationships.insert(
                (*target_entity, *index_id),
                AuthorizedRelationshipPlanV1 {
                    target_entity: *target_entity,
                    index_id: *index_id,
                    index_schema: index.key_schema().clone(),
                    partition_schema: aggregate.keys().partition_schema().clone(),
                    partition_width: root.primary_key_fields().len(),
                },
            );
        }
    }
    Ok(relationships)
}

fn lookup_from_probe(
    selected: &AuthorizedEntityPolicyV1,
    probe: IndexedRelationshipProbeV1,
) -> Result<AuthorizedIndexedRelationshipLookupV1, CommandRowPolicyContextErrorV1> {
    let relation = selected
        .relationships
        .get(&(probe.target_entity(), probe.index_id()))
        .ok_or(CommandRowPolicyContextErrorV1::InvalidRelationshipPlan)?;
    let partition = relation
        .partition_schema
        .encode_partition(
            probe
                .arguments()
                .get(..relation.partition_width)
                .ok_or(CommandRowPolicyContextErrorV1::InvalidRelationshipPlan)?,
        )
        .map_err(|_| CommandRowPolicyContextErrorV1::InvalidRelationshipPlan)?;
    let index_prefix = relation
        .index_schema
        .encode_index_prefix(probe.arguments())
        .map_err(|_| CommandRowPolicyContextErrorV1::InvalidRelationshipPlan)?
        .as_bytes()
        .to_vec();
    Ok(AuthorizedIndexedRelationshipLookupV1 {
        target_entity: relation.target_entity,
        index_id: relation.index_id,
        partition,
        index_prefix,
    })
}

fn transition_rows<'a>(
    operation: RowPolicyOperationV1,
    current: Option<&'a CanonicalRecord>,
    successor: Option<&'a CanonicalRecord>,
) -> Option<Vec<&'a CanonicalRecord>> {
    match operation {
        RowPolicyOperationV1::Read | RowPolicyOperationV1::Delete => current.map(|row| vec![row]),
        RowPolicyOperationV1::Create => successor.map(|row| vec![row]),
        RowPolicyOperationV1::Update => Some(vec![current?, successor?]),
    }
}

const fn capability_operation(operation: RowPolicyOperationV1) -> CapabilityRowPolicyOperationV1 {
    match operation {
        RowPolicyOperationV1::Read => CapabilityRowPolicyOperationV1::Read,
        RowPolicyOperationV1::Create => CapabilityRowPolicyOperationV1::Create,
        RowPolicyOperationV1::Update => CapabilityRowPolicyOperationV1::Update,
        RowPolicyOperationV1::Delete => CapabilityRowPolicyOperationV1::Delete,
    }
}

/// One checked observation for the single indexed relationship probe allowed by policy V1.
///
/// This is evidence gathered in the caller's authoritative snapshot or write
/// transaction. It is not a callback and cannot discover a different target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexedRelationshipEvidenceV1 {
    target_entity: EntityTypeId,
    index_id: IndexId,
    arguments: Vec<CanonicalValue>,
    exists: bool,
}

/// One exact compiler-declared relationship lookup required before evaluating
/// a row. This is produced only from the policy IR, authoritative row, and
/// transaction-current principal binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IndexedRelationshipProbeV1 {
    target_entity: EntityTypeId,
    index_id: IndexId,
    arguments: Vec<CanonicalValue>,
}

impl IndexedRelationshipProbeV1 {
    /// Target entity selected by the compiled policy.
    #[doc(hidden)]
    #[must_use]
    pub const fn target_entity(&self) -> EntityTypeId {
        self.target_entity
    }

    /// Complete declared index selected by the compiled policy.
    #[doc(hidden)]
    #[must_use]
    pub const fn index_id(&self) -> IndexId {
        self.index_id
    }

    /// Exact typed arguments derived from trusted state.
    #[doc(hidden)]
    #[must_use]
    pub fn arguments(&self) -> &[CanonicalValue] {
        &self.arguments
    }

    /// Converts one exact observation into evaluator evidence.
    #[doc(hidden)]
    #[must_use]
    pub fn into_evidence(self, exists: bool) -> IndexedRelationshipEvidenceV1 {
        IndexedRelationshipEvidenceV1::new(
            self.target_entity,
            self.index_id,
            self.arguments,
            exists,
        )
    }
}

impl IndexedRelationshipEvidenceV1 {
    /// Binds an exact compiler-declared target/index/key observation.
    #[must_use]
    pub fn new(
        target_entity: EntityTypeId,
        index_id: IndexId,
        arguments: Vec<CanonicalValue>,
        exists: bool,
    ) -> Self {
        Self {
            target_entity,
            index_id,
            arguments,
            exists,
        }
    }
}

/// Safe internal denial class. It deliberately carries no row value, fact
/// value, policy branch, or relationship key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RowPolicyDenyReasonV1 {
    /// The selected policy has no rule for the requested operation.
    MissingRule,
    /// A required current or proposed row is absent for this operation class.
    MissingRow,
    /// The trusted principal binding cannot supply a required typed fact.
    MissingOrInvalidPrincipalFact,
    /// Required exact relationship evidence is absent or inconsistent.
    MissingOrInvalidRelationshipEvidence,
    /// The closed expression did not authorize the row.
    PredicateDenied,
    /// The checked plan or row did not match its frozen structural contract.
    InvalidPlanOrRow,
    /// Capability identity/revision or row identity changed after evaluation.
    StaleAuthority,
}

/// Move-only evidence that one exact capability revision authorized the exact
/// current/proposed row identities required by an operation.
pub struct RowPolicyProofV1 {
    policy_name: String,
    entity: EntityTypeId,
    operation: RowPolicyOperationV1,
    capability_id: CapabilityId,
    capability_revision: NonZeroU64,
    current_row_hash: Option<CanonicalValueHash>,
    successor_row_hash: Option<CanonicalValueHash>,
}

impl std::fmt::Debug for RowPolicyProofV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RowPolicyProofV1")
            .field("policy_name", &self.policy_name)
            .field("entity", &self.entity)
            .field("operation", &self.operation)
            .field("capability_id", &self.capability_id)
            .field("capability_revision", &self.capability_revision)
            .field("row_values", &"[REDACTED]")
            .finish()
    }
}

impl RowPolicyProofV1 {
    /// Symbolic policy name.
    #[must_use]
    pub fn policy_name(&self) -> &str {
        &self.policy_name
    }

    /// Protected entity identity.
    #[must_use]
    pub const fn entity(&self) -> EntityTypeId {
        self.entity
    }

    /// Protected operation class.
    #[must_use]
    pub const fn operation(&self) -> RowPolicyOperationV1 {
        self.operation
    }

    /// Exact current capability revision covering the principal facts.
    #[must_use]
    pub const fn capability_revision(&self) -> NonZeroU64 {
        self.capability_revision
    }

    /// Current-row identity when the operation requires current state.
    #[must_use]
    pub const fn current_row_hash(&self) -> Option<CanonicalValueHash> {
        self.current_row_hash
    }

    /// Proposed successor identity for create/update.
    #[must_use]
    pub const fn successor_row_hash(&self) -> Option<CanonicalValueHash> {
        self.successor_row_hash
    }

    /// Exact capability identity for transaction-current revision recheck.
    #[doc(hidden)]
    #[must_use]
    pub const fn internal_capability_id(&self) -> CapabilityId {
        self.capability_id
    }
}

/// Closed row-policy result. Denial is ordinary indistinguishable absence to
/// read shapers and a no-mutation decision to command verification.
#[derive(Debug)]
pub enum RowPolicyDecisionV1 {
    /// Exact revision/row-bound proof.
    Allow(RowPolicyProofV1),
    /// Deny without disclosing protected values.
    Deny(RowPolicyDenyReasonV1),
}

impl RowPolicyDecisionV1 {
    /// Whether the closed decision authorized the row.
    #[must_use]
    pub const fn is_allowed(&self) -> bool {
        matches!(self, Self::Allow(_))
    }

    /// Whether the closed decision denied the row.
    #[must_use]
    pub const fn is_denied(&self) -> bool {
        matches!(self, Self::Deny(_))
    }
}

/// Derives the complete bounded relationship reads for one operation without
/// accepting a caller-selected target, index, or key.
#[doc(hidden)]
pub fn required_indexed_relationship_probes(
    policy: &RowPolicyPlanV1,
    operation: RowPolicyOperationV1,
    row: &CanonicalRecord,
    principal: &PrincipalFactBindingV1,
) -> Result<Vec<IndexedRelationshipProbeV1>, RowPolicyDenyReasonV1> {
    let rule = policy
        .rules()
        .iter()
        .find(|candidate| candidate.operation() == operation)
        .ok_or(RowPolicyDenyReasonV1::MissingRule)?;
    rule.nodes()
        .iter()
        .filter_map(|node| match node {
            RowPolicyExpressionNodeV1::IndexedExists {
                target_entity,
                index_id,
                arguments,
            } => Some((target_entity, index_id, arguments)),
            _ => None,
        })
        .map(|(target_entity, index_id, arguments)| {
            let arguments = arguments
                .iter()
                .map(|argument| evaluate_operand(argument, row, principal))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(IndexedRelationshipProbeV1 {
                target_entity: *target_entity,
                index_id: *index_id,
                arguments,
            })
        })
        .collect()
}

/// Evaluates one exact row for one operation. This is the primitive used by
/// read-side policy-before-shape enforcement.
#[must_use]
pub fn evaluate_row_policy(
    policy: &RowPolicyPlanV1,
    operation: RowPolicyOperationV1,
    row: &CanonicalRecord,
    principal: &PrincipalFactBindingV1,
    relationships: &[IndexedRelationshipEvidenceV1],
) -> RowPolicyDecisionV1 {
    let (current, successor) = match operation {
        RowPolicyOperationV1::Read | RowPolicyOperationV1::Delete => (Some(row), None),
        RowPolicyOperationV1::Create => (None, Some(row)),
        RowPolicyOperationV1::Update => (Some(row), Some(row)),
    };
    evaluate_row_transition(
        policy,
        operation,
        current,
        successor,
        principal,
        relationships,
    )
}

/// Evaluates the exact transaction-current and proposed state required by the
/// operation class. Update is allowed only when the same selected rule accepts
/// both rows; this prevents owner/visibility/ACL escape by construction.
#[must_use]
pub fn evaluate_row_transition(
    policy: &RowPolicyPlanV1,
    operation: RowPolicyOperationV1,
    current: Option<&CanonicalRecord>,
    successor: Option<&CanonicalRecord>,
    principal: &PrincipalFactBindingV1,
    relationships: &[IndexedRelationshipEvidenceV1],
) -> RowPolicyDecisionV1 {
    let Some(rule) = policy
        .rules()
        .iter()
        .find(|candidate| candidate.operation() == operation)
    else {
        return RowPolicyDecisionV1::Deny(RowPolicyDenyReasonV1::MissingRule);
    };
    let required = match operation {
        RowPolicyOperationV1::Read | RowPolicyOperationV1::Delete => {
            let Some(current) = current else {
                return RowPolicyDecisionV1::Deny(RowPolicyDenyReasonV1::MissingRow);
            };
            [Some(current), None]
        }
        RowPolicyOperationV1::Create => {
            let Some(successor) = successor else {
                return RowPolicyDecisionV1::Deny(RowPolicyDenyReasonV1::MissingRow);
            };
            [Some(successor), None]
        }
        RowPolicyOperationV1::Update => {
            let (Some(current), Some(successor)) = (current, successor) else {
                return RowPolicyDecisionV1::Deny(RowPolicyDenyReasonV1::MissingRow);
            };
            [Some(current), Some(successor)]
        }
    };
    for row in required.into_iter().flatten() {
        match evaluate_rule(rule, row, principal, relationships) {
            Ok(true) => {}
            Ok(false) => {
                return RowPolicyDecisionV1::Deny(RowPolicyDenyReasonV1::PredicateDenied);
            }
            Err(reason) => return RowPolicyDecisionV1::Deny(reason),
        }
    }
    let current_row_hash = match current {
        Some(row) => match row_hash(row) {
            Some(hash) => Some(hash),
            None => {
                return RowPolicyDecisionV1::Deny(RowPolicyDenyReasonV1::InvalidPlanOrRow);
            }
        },
        None => None,
    };
    let successor_row_hash = match successor {
        Some(row) => match row_hash(row) {
            Some(hash) => Some(hash),
            None => {
                return RowPolicyDecisionV1::Deny(RowPolicyDenyReasonV1::InvalidPlanOrRow);
            }
        },
        None => None,
    };
    RowPolicyDecisionV1::Allow(RowPolicyProofV1 {
        policy_name: policy.name().to_owned(),
        entity: policy.entity(),
        operation,
        capability_id: principal.capability_id(),
        capability_revision: principal.revision(),
        current_row_hash,
        successor_row_hash,
    })
}

/// Re-evaluates at the authoritative safe point and accepts only the exact
/// capability revision and row identities covered by the earlier proof.
///
/// Callers must invoke this while their transaction-current observations are
/// still protected by the commit coordinator. A changed capability, current
/// row, successor, relationship observation, or predicate produces no proof.
#[must_use]
pub fn revalidate_row_policy_proof(
    prior: &RowPolicyProofV1,
    policy: &RowPolicyPlanV1,
    current: Option<&CanonicalRecord>,
    successor: Option<&CanonicalRecord>,
    principal: &PrincipalFactBindingV1,
    relationships: &[IndexedRelationshipEvidenceV1],
) -> RowPolicyDecisionV1 {
    if prior.policy_name != policy.name()
        || prior.entity != policy.entity()
        || prior.capability_id != principal.capability_id()
        || prior.capability_revision != principal.revision()
    {
        return RowPolicyDecisionV1::Deny(RowPolicyDenyReasonV1::StaleAuthority);
    }
    let RowPolicyDecisionV1::Allow(current_proof) = evaluate_row_transition(
        policy,
        prior.operation,
        current,
        successor,
        principal,
        relationships,
    ) else {
        return RowPolicyDecisionV1::Deny(RowPolicyDenyReasonV1::StaleAuthority);
    };
    if prior.current_row_hash != current_proof.current_row_hash
        || prior.successor_row_hash != current_proof.successor_row_hash
    {
        RowPolicyDecisionV1::Deny(RowPolicyDenyReasonV1::StaleAuthority)
    } else {
        RowPolicyDecisionV1::Allow(current_proof)
    }
}

/// Filters rows before pagination, ranking, grouping, aggregation, or output
/// shaping. Denials are indistinguishable absence and no proof is retained in
/// a cursor or cached plan.
#[must_use]
pub fn filter_authorized_rows<'a, I>(
    policy: &RowPolicyPlanV1,
    principal: &PrincipalFactBindingV1,
    rows: I,
    relationships: &[IndexedRelationshipEvidenceV1],
) -> Vec<&'a CanonicalRecord>
where
    I: IntoIterator<Item = &'a CanonicalRecord>,
{
    rows.into_iter()
        .filter(|row| {
            evaluate_row_policy(
                policy,
                RowPolicyOperationV1::Read,
                row,
                principal,
                relationships,
            )
            .is_allowed()
        })
        .collect()
}

/// Counts only authorized rows, using the same pre-shape decision as pages.
#[must_use]
pub fn policy_visible_count<'a, I>(
    policy: &RowPolicyPlanV1,
    principal: &PrincipalFactBindingV1,
    rows: I,
    relationships: &[IndexedRelationshipEvidenceV1],
) -> usize
where
    I: IntoIterator<Item = &'a CanonicalRecord>,
{
    filter_authorized_rows(policy, principal, rows, relationships).len()
}

#[derive(Clone)]
enum NodeValue {
    Canonical(CanonicalValue),
    Boolean(bool),
}

fn evaluate_rule(
    rule: &riffdb_contract_ir::RowPolicyRuleV1,
    row: &CanonicalRecord,
    principal: &PrincipalFactBindingV1,
    relationships: &[IndexedRelationshipEvidenceV1],
) -> Result<bool, RowPolicyDenyReasonV1> {
    let mut values = Vec::<NodeValue>::with_capacity(rule.nodes().len());
    for node in rule.nodes() {
        let value = match node {
            RowPolicyExpressionNodeV1::Operand(operand) => {
                NodeValue::Canonical(evaluate_operand(operand, row, principal)?)
            }
            RowPolicyExpressionNodeV1::Equal { left, right } => NodeValue::Boolean(
                canonical_node(&values, *left)? == canonical_node(&values, *right)?,
            ),
            RowPolicyExpressionNodeV1::NotEqual { left, right } => NodeValue::Boolean(
                canonical_node(&values, *left)? != canonical_node(&values, *right)?,
            ),
            RowPolicyExpressionNodeV1::Not { value } => {
                NodeValue::Boolean(!boolean_node(&values, *value)?)
            }
            RowPolicyExpressionNodeV1::And { left, right } => {
                NodeValue::Boolean(boolean_node(&values, *left)? && boolean_node(&values, *right)?)
            }
            RowPolicyExpressionNodeV1::Or { left, right } => {
                NodeValue::Boolean(boolean_node(&values, *left)? || boolean_node(&values, *right)?)
            }
            RowPolicyExpressionNodeV1::In { needle, haystack } => {
                let needle = canonical_node(&values, *needle)?;
                let CanonicalValue::List(haystack) = canonical_node(&values, *haystack)? else {
                    return Err(RowPolicyDenyReasonV1::InvalidPlanOrRow);
                };
                NodeValue::Boolean(
                    haystack
                        .values()
                        .iter()
                        .any(|candidate| candidate == needle),
                )
            }
            RowPolicyExpressionNodeV1::IsNull { value, negated } => {
                let is_null = matches!(canonical_node(&values, *value)?, CanonicalValue::Null);
                NodeValue::Boolean(if *negated { !is_null } else { is_null })
            }
            RowPolicyExpressionNodeV1::IndexedExists {
                target_entity,
                index_id,
                arguments,
            } => {
                let arguments = arguments
                    .iter()
                    .map(|argument| evaluate_operand(argument, row, principal))
                    .collect::<Result<Vec<_>, _>>()?;
                let evidence = relationships.iter().find(|evidence| {
                    evidence.target_entity == *target_entity
                        && evidence.index_id == *index_id
                        && evidence.arguments == arguments
                });
                let Some(evidence) = evidence else {
                    return Err(RowPolicyDenyReasonV1::MissingOrInvalidRelationshipEvidence);
                };
                NodeValue::Boolean(evidence.exists)
            }
        };
        values.push(value);
    }
    boolean_node(&values, rule.root())
}

fn evaluate_operand(
    operand: &RowPolicyOperandV1,
    row: &CanonicalRecord,
    principal: &PrincipalFactBindingV1,
) -> Result<CanonicalValue, RowPolicyDenyReasonV1> {
    let value = match operand.source() {
        RowPolicyValueSourceV1::RowField(field) => row
            .fields()
            .binary_search_by_key(field, |(candidate, _)| *candidate)
            .ok()
            .map(|index| row.fields()[index].1.clone())
            .ok_or(RowPolicyDenyReasonV1::InvalidPlanOrRow)?,
        RowPolicyValueSourceV1::PrincipalId => CanonicalValue::Uuid(
            parse_uuid(principal.principal_id().as_str())
                .ok_or(RowPolicyDenyReasonV1::MissingOrInvalidPrincipalFact)?,
        ),
        RowPolicyValueSourceV1::PrincipalKind => {
            CanonicalValue::string(actor_kind_name(principal.actor_kind()))
                .map_err(|_| RowPolicyDenyReasonV1::InvalidPlanOrRow)?
        }
        RowPolicyValueSourceV1::PrincipalFact(name) => principal
            .internal_facts()
            .internal_fact(name)
            .map(|fact| fact.internal_value().clone())
            .ok_or(RowPolicyDenyReasonV1::MissingOrInvalidPrincipalFact)?,
        RowPolicyValueSourceV1::Constant(value) => value.clone(),
    };
    operand
        .value_type()
        .validate_value(&value)
        .map_err(|_| RowPolicyDenyReasonV1::MissingOrInvalidPrincipalFact)?;
    Ok(value)
}

fn canonical_node(
    values: &[NodeValue],
    index: u16,
) -> Result<&CanonicalValue, RowPolicyDenyReasonV1> {
    match values.get(usize::from(index)) {
        Some(NodeValue::Canonical(value)) => Ok(value),
        Some(NodeValue::Boolean(_)) | None => Err(RowPolicyDenyReasonV1::InvalidPlanOrRow),
    }
}

fn boolean_node(values: &[NodeValue], index: u16) -> Result<bool, RowPolicyDenyReasonV1> {
    match values.get(usize::from(index)) {
        Some(NodeValue::Boolean(value) | NodeValue::Canonical(CanonicalValue::Bool(value))) => {
            Ok(*value)
        }
        Some(NodeValue::Canonical(_)) | None => Err(RowPolicyDenyReasonV1::InvalidPlanOrRow),
    }
}

fn row_hash(row: &CanonicalRecord) -> Option<CanonicalValueHash> {
    encode_canonical_record(row)
        .ok()
        .map(|bytes| hash_canonical_value(&bytes))
}

const fn actor_kind_name(kind: ActorKind) -> &'static str {
    match kind {
        ActorKind::Human => "Human",
        ActorKind::Agent => "Agent",
        ActorKind::Service => "Service",
    }
}

fn parse_uuid(value: &str) -> Option<[u8; 16]> {
    if value.len() != 36
        || value
            .bytes()
            .enumerate()
            .any(|(index, byte)| matches!(index, 8 | 13 | 18 | 23) != (byte == b'-'))
    {
        return None;
    }
    let hex = value
        .bytes()
        .filter(|byte| *byte != b'-')
        .collect::<Vec<_>>();
    let mut output = [0_u8; 16];
    for (slot, pair) in output.iter_mut().zip(hex.chunks_exact(2)) {
        *slot = hex_nibble(pair[0])?
            .checked_mul(16)?
            .checked_add(hex_nibble(pair[1])?)?;
    }
    Some(output)
}

const fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod projected_admission_tests {
    use super::*;
    use riffdb_types::{
        ActorId, Audience, CapabilityPrincipalFactsV1, DatabaseId, EntityKeyBuilder, Environment,
        TenantScope, Timestamp,
    };

    fn context() -> AuthorizedQueryRowPolicyContextV1 {
        AuthorizedQueryRowPolicyContextV1 {
            bundle: None,
            authority: None,
            principal: PrincipalFactBindingV1::new(
                CapabilityId::from_unix_milliseconds_and_random(1, [5; 10]).unwrap(),
                NonZeroU64::MIN,
                DatabaseId::from_unix_milliseconds_and_random(1, [1; 10]).unwrap(),
                Environment::new("test").unwrap(),
                ActorId::new("reader").unwrap(),
                ActorKind::Human,
                vec![Audience::new("test").unwrap()],
                TenantScope::Global,
                Timestamp::new(1, 0).unwrap(),
                Timestamp::new(100, 0).unwrap(),
                CapabilityPrincipalFactsV1::empty(),
            )
            .unwrap(),
            policies: BTreeMap::new(),
        }
    }

    fn key(entity: EntityTypeId, value: u64) -> EntityKey {
        let mut key = EntityKeyBuilder::new(entity);
        key.push_u64(value).unwrap();
        key.finish().unwrap()
    }

    // req: VEC-007
    #[test]
    fn owned_sorted_admission_preserves_exact_coverage_and_absent_row_denial() {
        let entity = EntityTypeId::new(1).unwrap();
        let keys = (0..32)
            .map(|value| key(entity, value))
            .collect::<BTreeSet<_>>();
        let observations = keys
            .iter()
            .enumerate()
            .map(|(position, key)| {
                if position % 2 == 0 {
                    ProjectedPolicyCandidateObservationV1::current(
                        key.clone(),
                        CanonicalRecord::new(vec![]).unwrap(),
                        vec![],
                    )
                } else {
                    ProjectedPolicyCandidateObservationV1::missing(key.clone())
                }
            })
            .collect();
        let proof = context()
            .authorize_projected_candidates(entity, observations)
            .unwrap();
        assert!(proof.covers(entity, &keys));
        assert!(!proof.covers(EntityTypeId::new(2).unwrap(), &keys));
        for (position, key) in keys.iter().enumerate() {
            assert_eq!(proof.admits(key), position % 2 == 0);
        }
        let missing = key(entity, 100);
        assert!(!proof.admits(&missing));
        let mut different = keys.clone();
        different.pop_first();
        assert!(!proof.covers(entity, &different));
        different.insert(missing);
        assert!(!proof.covers(entity, &different));
    }

    // req: VEC-007
    #[test]
    fn owned_admission_rejects_duplicate_reversed_and_wrong_entity_candidates() {
        let entity = EntityTypeId::new(1).unwrap();
        let first = key(entity, 1);
        let second = key(entity, 2);
        for keys in [
            vec![first.clone(), first],
            vec![second, key(entity, 1)],
            vec![key(EntityTypeId::new(2).unwrap(), 1)],
        ] {
            let observations = keys
                .into_iter()
                .map(ProjectedPolicyCandidateObservationV1::missing)
                .collect();
            assert_eq!(
                context()
                    .authorize_projected_candidates(entity, observations)
                    .unwrap_err(),
                QueryRowPolicyContextErrorV1::InvalidProjectedCandidateSet
            );
        }
    }
}
