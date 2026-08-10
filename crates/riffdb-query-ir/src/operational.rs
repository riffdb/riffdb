//! Closed finite plan families for operational RiffQL.

use riffdb_types::{QueryCostVectorV1, QueryPlanHash, hash_query_plan};

use crate::{
    AuthorizationEntityAccess, MAX_QUERY_ARTIFACT_BYTES, QUERY_IR_VERSION_OPERATIONAL_V1,
    QueryAccessProgramV1, ResolvedQueryV1,
};

const FAMILY_MAGIC: &[u8] = b"RIFFDB-OPERATIONAL-QUERY-FAMILY\0";

/// Maximum optional-presence dimensions in one operational query.
pub const MAX_OPERATIONAL_PRESENCE_PARAMETERS: usize = 8;
/// Maximum compiler-enumerated access plans in one operational family.
pub const MAX_OPERATIONAL_PLAN_MEMBERS: usize = 1 << MAX_OPERATIONAL_PRESENCE_PARAMETERS;

/// Stable identity of one complete finite operational plan family.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OperationalQueryFamilyIdentity(QueryPlanHash);

impl OperationalQueryFamilyIdentity {
    /// Domain-separated digest over the complete canonical family.
    #[must_use]
    pub const fn hash(self) -> QueryPlanHash {
        self.0
    }
}

/// One compiler-enumerated member selected by an exact presence bit mask.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationalPlanMemberV1 {
    presence_mask: u16,
    program: QueryAccessProgramV1,
}

impl OperationalPlanMemberV1 {
    /// Presence mask in the family's ordered parameter domain.
    #[must_use]
    pub const fn presence_mask(&self) -> u16 {
        self.presence_mask
    }

    /// Complete ordinary bounded access plan for this member.
    #[must_use]
    pub const fn program(&self) -> &QueryAccessProgramV1 {
        &self.program
    }

    #[doc(hidden)]
    #[must_use]
    pub const fn checked(presence_mask: u16, program: QueryAccessProgramV1) -> Self {
        Self {
            presence_mask,
            program,
        }
    }
}

/// Complete finite compiler-owned access family for one operational query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OperationalQueryFamilyV1 {
    surface: ResolvedQueryV1,
    presence_parameters: Vec<String>,
    members: Vec<OperationalPlanMemberV1>,
    authorization_union: Vec<AuthorizationEntityAccess>,
    maximum_cost: QueryCostVectorV1,
    canonical_bytes: Vec<u8>,
    identity: OperationalQueryFamilyIdentity,
}

impl OperationalQueryFamilyV1 {
    /// Constructs and canonically seals one completely enumerated family.
    #[doc(hidden)]
    pub fn checked(
        surface: ResolvedQueryV1,
        presence_parameters: Vec<String>,
        members: Vec<OperationalPlanMemberV1>,
    ) -> Option<Self> {
        let expected_members =
            1_usize.checked_shl(u32::try_from(presence_parameters.len()).ok()?)?;
        let first = members.first()?;
        if presence_parameters.len() > MAX_OPERATIONAL_PRESENCE_PARAMETERS
            || presence_parameters.iter().any(String::is_empty)
            || presence_parameters
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
            || members.len() != expected_members
            || members.len() > MAX_OPERATIONAL_PLAN_MEMBERS
            || members
                .iter()
                .enumerate()
                .any(|(mask, member)| usize::from(member.presence_mask) != mask)
            || members.iter().any(|member| {
                member.program.contract() != surface.contract()
                    || member.program.contract() != first.program.contract()
                    || member.program.name() != first.program.name()
                    || member.program.partition_parameter() != first.program.partition_parameter()
                    || member.program.surface().schemas() != surface.schemas()
            })
        {
            return None;
        }
        let authorization_union = authorization_union(&members)?;
        let maximum_cost = maximum_cost(&members)?;
        let canonical_bytes = encode_family(
            surface.canonical_bytes(),
            &presence_parameters,
            &members,
            &authorization_union,
            maximum_cost,
        )?;
        let identity = OperationalQueryFamilyIdentity(hash_query_plan(&canonical_bytes));
        Some(Self {
            surface,
            presence_parameters,
            members,
            authorization_union,
            maximum_cost,
            canonical_bytes,
            identity,
        })
    }

    /// Original canonical typed operation surface shared by every member.
    #[must_use]
    pub const fn surface(&self) -> &ResolvedQueryV1 {
        &self.surface
    }

    /// One parameter routing every member to the same partition.
    #[must_use]
    pub fn partition_parameter(&self) -> &str {
        self.members[0].program().partition_parameter()
    }

    /// Optional parameters in canonical name order; positions define mask bits.
    #[must_use]
    pub fn presence_parameters(&self) -> &[String] {
        &self.presence_parameters
    }

    /// Every selectable member in ascending mask order.
    #[must_use]
    pub fn members(&self) -> &[OperationalPlanMemberV1] {
        &self.members
    }

    /// Resolves one member from presence choices in exact parameter order.
    #[must_use]
    pub fn select(&self, presence: &[bool]) -> Option<&OperationalPlanMemberV1> {
        if presence.len() != self.presence_parameters.len() {
            return None;
        }
        let mask = presence
            .iter()
            .enumerate()
            .fold(
                0_usize,
                |mask, (bit, set)| {
                    if *set { mask | (1_usize << bit) } else { mask }
                },
            );
        self.members.get(mask)
    }

    /// Authorization union that must be allowed before any member executes.
    #[must_use]
    pub fn authorization_union(&self) -> &[AuthorizationEntityAccess] {
        &self.authorization_union
    }

    /// Componentwise maximum cost across the finite family.
    #[must_use]
    pub const fn maximum_cost(&self) -> QueryCostVectorV1 {
        self.maximum_cost
    }

    /// Canonical family bytes covered by the identity.
    #[must_use]
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical_bytes
    }

    /// Stable family identity.
    #[must_use]
    pub const fn identity(&self) -> OperationalQueryFamilyIdentity {
        self.identity
    }
}

fn authorization_union(
    members: &[OperationalPlanMemberV1],
) -> Option<Vec<AuthorizationEntityAccess>> {
    let mut by_entity = std::collections::BTreeMap::<String, Vec<AuthorizationEntityAccess>>::new();
    for access in members
        .iter()
        .flat_map(|member| member.program().authorization())
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

fn maximum_cost(members: &[OperationalPlanMemberV1]) -> Option<QueryCostVectorV1> {
    let mut maximum = QueryCostVectorV1::zero();
    for cost in members.iter().map(|member| member.program().cost()) {
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

fn encode_family(
    surface: &[u8],
    parameters: &[String],
    members: &[OperationalPlanMemberV1],
    authorization: &[AuthorizationEntityAccess],
    maximum_cost: QueryCostVectorV1,
) -> Option<Vec<u8>> {
    let mut output = Vec::new();
    output.extend_from_slice(FAMILY_MAGIC);
    output.extend_from_slice(&QUERY_IR_VERSION_OPERATIONAL_V1.to_be_bytes());
    write_bytes(&mut output, surface)?;
    write_strings(&mut output, parameters)?;
    write_count(&mut output, members.len())?;
    for member in members {
        output.extend_from_slice(&member.presence_mask.to_be_bytes());
        write_bytes(&mut output, member.program.canonical_bytes())?;
        output.extend_from_slice(member.program.identity().hash().as_bytes());
    }
    write_count(&mut output, authorization.len())?;
    for access in authorization {
        write_text(&mut output, access.entity())?;
        write_strings(&mut output, access.fields())?;
        write_strings(&mut output, access.indexes())?;
        output.extend_from_slice(&access.maximum_rows().to_be_bytes());
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
        output.extend_from_slice(&value.to_be_bytes());
    }
    (output.len() <= MAX_QUERY_ARTIFACT_BYTES).then_some(output)
}

fn write_count(output: &mut Vec<u8>, value: usize) -> Option<()> {
    output.extend_from_slice(&u32::try_from(value).ok()?.to_be_bytes());
    Some(())
}

fn write_bytes(output: &mut Vec<u8>, value: &[u8]) -> Option<()> {
    write_count(output, value.len())?;
    output.extend_from_slice(value);
    Some(())
}

fn write_text(output: &mut Vec<u8>, value: &str) -> Option<()> {
    write_bytes(output, value.as_bytes())
}

fn write_strings(output: &mut Vec<u8>, values: &[String]) -> Option<()> {
    write_count(output, values.len())?;
    for value in values {
        write_text(output, value)?;
    }
    Some(())
}
