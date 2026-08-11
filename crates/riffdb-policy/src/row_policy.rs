//! Shared, deny-by-default evaluation of compiler-owned row-policy plans.

use std::num::NonZeroU64;

use riffdb_auth::PrincipalFactBindingV1;
use riffdb_contract_ir::{
    RowPolicyExpressionNodeV1, RowPolicyOperandV1, RowPolicyOperationV1, RowPolicyPlanV1,
    RowPolicyValueSourceV1,
};
use riffdb_types::{
    ActorKind, CanonicalRecord, CanonicalValue, CanonicalValueHash, CapabilityId, EntityTypeId,
    IndexId, encode_canonical_record, hash_canonical_value,
};

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
        Some(NodeValue::Boolean(value)) => Ok(*value),
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
