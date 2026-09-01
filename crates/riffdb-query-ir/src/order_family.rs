//! Closed finite compiler-owned root-order families.

use std::collections::BTreeMap;
use std::sync::Arc;

use riffdb_types::{EnumTypeId, EnumVariantId, QueryCostVectorV1, QueryPlanHash, hash_query_plan};

use crate::{
    AuthorizationEntityAccess, MAX_QUERY_ARTIFACT_BYTES, QueryAccessProgramV1, ResolvedQueryV1,
};

const MAGIC: &[u8] = b"RIFFDB-ORDER-QUERY-FAMILY\0";
/// Maximum immutable order variants in one generated operation.
pub const MAX_ORDER_FAMILY_MEMBERS_V1: usize = 32;

/// One exact enum choice and its complete immutable access program.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderFamilyMemberV1 {
    variant_name: String,
    variant_id: EnumVariantId,
    program: Arc<QueryAccessProgramV1>,
}

impl OrderFamilyMemberV1 {
    #[doc(hidden)]
    #[must_use]
    pub fn checked(
        variant_name: String,
        variant_id: EnumVariantId,
        program: QueryAccessProgramV1,
    ) -> Self {
        Self {
            variant_name,
            variant_id,
            program: Arc::new(program),
        }
    }
    /// Exact public contract variant name.
    #[must_use]
    pub fn variant_name(&self) -> &str {
        &self.variant_name
    }
    /// Stable contract variant identity.
    #[must_use]
    pub const fn variant_id(&self) -> EnumVariantId {
        self.variant_id
    }
    /// Complete immutable program selected by this variant.
    #[must_use]
    pub fn program(&self) -> &QueryAccessProgramV1 {
        &self.program
    }
    /// Shared immutable program selected by this variant.
    #[must_use]
    pub fn shared_program(&self) -> Arc<QueryAccessProgramV1> {
        Arc::clone(&self.program)
    }
}

/// Complete checked family; authorization and cost cover every selectable member.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderQueryFamilyV1 {
    surface: ResolvedQueryV1,
    selector_parameter: String,
    selector_type: EnumTypeId,
    members: Vec<OrderFamilyMemberV1>,
    authorization_union: Vec<AuthorizationEntityAccess>,
    maximum_cost: QueryCostVectorV1,
    canonical_bytes: Vec<u8>,
    identity: QueryPlanHash,
}

impl OrderQueryFamilyV1 {
    #[doc(hidden)]
    pub fn checked(
        surface: ResolvedQueryV1,
        selector_parameter: String,
        selector_type: EnumTypeId,
        mut members: Vec<OrderFamilyMemberV1>,
    ) -> Option<Self> {
        members.sort_by_key(OrderFamilyMemberV1::variant_id);
        let first = members.first()?;
        if selector_parameter.is_empty()
            || members.len() > MAX_ORDER_FAMILY_MEMBERS_V1
            || members
                .windows(2)
                .any(|pair| pair[0].variant_id >= pair[1].variant_id)
            || members.iter().any(|member| {
                member.variant_name.is_empty()
                    || member.program.contract() != surface.contract()
                    || member.program.name() != first.program.name()
                    || member.program.partition_route() != first.program.partition_route()
                    || member.program.surface().schemas() != surface.schemas()
            })
        {
            return None;
        }
        let authorization_union = authorization_union(&members)?;
        let maximum_cost = maximum_cost(&members)?;
        let mut canonical_bytes = Vec::new();
        canonical_bytes.extend_from_slice(MAGIC);
        canonical_bytes.extend_from_slice(&crate::QUERY_IR_VERSION_ORDER_FAMILY_V1.to_be_bytes());
        put(&mut canonical_bytes, surface.canonical_bytes())?;
        put(&mut canonical_bytes, selector_parameter.as_bytes())?;
        canonical_bytes.extend_from_slice(&selector_type.get().to_be_bytes());
        canonical_bytes.extend_from_slice(&u32::try_from(members.len()).ok()?.to_be_bytes());
        for member in &members {
            put(&mut canonical_bytes, member.variant_name.as_bytes())?;
            canonical_bytes.extend_from_slice(&member.variant_id.get().to_be_bytes());
            put(&mut canonical_bytes, member.program.canonical_bytes())?;
            canonical_bytes.extend_from_slice(member.program.identity().hash().as_bytes());
        }
        for value in [
            maximum_cost.access_steps(),
            maximum_cost.scanned_index_rows(),
            maximum_cost.point_reads(),
            maximum_cost.dependent_keys(),
            maximum_cost.intermediate_rows(),
            maximum_cost.projected_values(),
            maximum_cost.encoded_result_bytes(),
        ] {
            canonical_bytes.extend_from_slice(&value.to_be_bytes());
        }
        if canonical_bytes.len() > MAX_QUERY_ARTIFACT_BYTES {
            return None;
        }
        let identity = hash_query_plan(&canonical_bytes);
        Some(Self {
            surface,
            selector_parameter,
            selector_type,
            members,
            authorization_union,
            maximum_cost,
            canonical_bytes,
            identity,
        })
    }
    /// Original typed public operation surface.
    #[must_use]
    pub const fn surface(&self) -> &ResolvedQueryV1 {
        &self.surface
    }
    /// Shared exact partition-routing parameter.
    #[must_use]
    pub fn partition_parameter(&self) -> &str {
        self.members[0].program.partition_parameter()
    }
    /// Public enum parameter selecting a member.
    #[must_use]
    pub fn selector_parameter(&self) -> &str {
        &self.selector_parameter
    }
    /// Stable contract enum identity accepted by the selector.
    #[must_use]
    pub const fn selector_type(&self) -> EnumTypeId {
        self.selector_type
    }
    /// Every member in stable enum-variant order.
    #[must_use]
    pub fn members(&self) -> &[OrderFamilyMemberV1] {
        &self.members
    }
    /// Selects by fully materialized stable enum identity.
    #[must_use]
    pub fn select(
        &self,
        type_id: EnumTypeId,
        variant_id: EnumVariantId,
    ) -> Option<&OrderFamilyMemberV1> {
        (type_id == self.selector_type)
            .then(|| {
                self.members
                    .iter()
                    .find(|member| member.variant_id == variant_id)
            })
            .flatten()
    }
    /// Selects by exact public enum variant name before materialization.
    #[must_use]
    pub fn select_name(&self, name: &str) -> Option<&OrderFamilyMemberV1> {
        self.members
            .iter()
            .find(|member| member.variant_name == name)
    }
    /// Complete authority union checked before member selection.
    #[must_use]
    pub fn authorization_union(&self) -> &[AuthorizationEntityAccess] {
        &self.authorization_union
    }
    /// Componentwise maximum cost across every member.
    #[must_use]
    pub const fn maximum_cost(&self) -> QueryCostVectorV1 {
        self.maximum_cost
    }
    /// Canonical bytes sealed by the family identity.
    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }
    /// Stable complete family identity.
    #[must_use]
    pub const fn identity(&self) -> QueryPlanHash {
        self.identity
    }
}

fn authorization_union(members: &[OrderFamilyMemberV1]) -> Option<Vec<AuthorizationEntityAccess>> {
    let mut by_entity = BTreeMap::<String, Vec<AuthorizationEntityAccess>>::new();
    for access in members
        .iter()
        .flat_map(|member| member.program.authorization())
    {
        by_entity
            .entry(access.entity().to_owned())
            .or_default()
            .push(access.clone());
    }
    by_entity
        .into_values()
        .map(|accesses| AuthorizationEntityAccess::internal_union(&accesses))
        .collect()
}
fn maximum_cost(members: &[OrderFamilyMemberV1]) -> Option<QueryCostVectorV1> {
    let mut maximum = QueryCostVectorV1::zero();
    for cost in members.iter().map(|member| member.program.cost()) {
        maximum = QueryCostVectorV1::new(
            maximum.access_steps().max(cost.access_steps()),
            maximum.scanned_index_rows().max(cost.scanned_index_rows()),
            maximum.point_reads().max(cost.point_reads()),
            maximum.dependent_keys().max(cost.dependent_keys()),
            maximum.intermediate_rows().max(cost.intermediate_rows()),
            maximum.projected_values().max(cost.projected_values()),
            maximum
                .encoded_result_bytes()
                .max(cost.encoded_result_bytes()),
        )?;
    }
    Some(maximum)
}
fn put(output: &mut Vec<u8>, bytes: &[u8]) -> Option<()> {
    output.extend_from_slice(&u32::try_from(bytes.len()).ok()?.to_be_bytes());
    output.extend_from_slice(bytes);
    Some(())
}
