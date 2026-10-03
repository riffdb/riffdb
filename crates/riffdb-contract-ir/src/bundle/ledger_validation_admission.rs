//! Admit all native workspace of existing complete ledger validation.
// req: OQ-040, PRJ-004
use super::*;
use riffdb_types::exact_provider_budget_v1::{BudgetUnavailable, ProviderBudget};
use workflow_admission::allocation;

fn add(total: &mut u64, value: u64) -> Result<(), BudgetUnavailable> {
    *total = total.checked_add(value).ok_or(BudgetUnavailable)?;
    Ok(())
}
fn count(total: &mut usize, value: usize) -> Result<(), BudgetUnavailable> {
    *total = total.checked_add(value).ok_or(BudgetUnavailable)?;
    Ok(())
}
fn identity_heap(owners: &[u32], name: &str) -> Result<u64, BudgetUnavailable> {
    allocation::<u32>(owners.len())?
        .checked_add(allocation::<u8>(name.len())?)
        .ok_or(BudgetUnavailable)
}
fn growing_capacity(count: usize) -> Result<usize, BudgetUnavailable> {
    count
        .checked_mul(2)
        .map(|value| value.max(4))
        .ok_or(BudgetUnavailable)
}

pub(super) struct LedgerValidationBound {
    pub(super) required: u64,
    pub(super) expected: u64,
    pub(super) identities: u64,
    pub(super) temporary: u64,
}
impl LedgerValidationBound {
    pub(super) fn inspect(bundle: &ContractBundle) -> Result<Self, ProviderBundleEncodingErrorV1> {
        // required_lineage_allocation_namespaces starts with the closed global
        // set, then pushes one namespace per record/enum and one per command's
        // outcomes. It sorts and deduplicates without shrinking capacity.
        let mut required_count = GLOBAL_LINEAGE_ALLOCATION_TAGS.len();
        for value in [
            bundle.schema.entities().len(),
            bundle.schema.events().len(),
            bundle.schema.enums().len(),
            bundle.projections.len(),
        ] {
            count(&mut required_count, value)?;
        }
        for command in &bundle.commands {
            count(&mut required_count, 2)?; // input fields and outcome allocation
            count(&mut required_count, command.outcomes().len())?;
        }
        let mut required =
            allocation::<StableIdAllocationNamespace>(growing_capacity(required_count)?)?;
        let scoped = required_count
            .checked_sub(GLOBAL_LINEAGE_ALLOCATION_TAGS.len())
            .ok_or(BudgetUnavailable)?;
        // Every required record owner has at most two components, including
        // command outcomes. This covers paths before deduplication as well.
        add(
            &mut required,
            allocation::<u32>(2)?
                .checked_mul(u64::try_from(scoped).map_err(|_| BudgetUnavailable)?)
                .ok_or(BudgetUnavailable)?,
        )?;

        let mut expected_count = 0;
        let mut expected = 0;
        let mut temporary = 0;
        visit_expected_lineage_identities(
            &bundle.ledger,
            &bundle.schema,
            &bundle.commands,
            &bundle.projections,
            |_, _, owners, name, _| {
                count(&mut expected_count, 1)?;
                add(&mut expected, identity_heap(owners, name)?)?;
                temporary = temporary.max(allocation::<u32>(owners.len())?);
                Ok::<_, ProviderBundleEncodingErrorV1>(())
            },
        )?;
        add(
            &mut expected,
            allocation::<(StableIdentity, u32)>(growing_capacity(expected_count)?)?,
        )?;

        let mut identity_count = 0;
        let mut identities = 0;
        for identity in bundle
            .ledger
            .allocations
            .iter()
            .flat_map(|value| &value.entries)
            .map(|value| &value.identity)
            .chain(bundle.ledger.aliases.iter().map(|value| &value.identity))
        {
            count(&mut identity_count, 1)?;
            add(
                &mut identities,
                identity_heap(identity.namespace.owner_ids(), identity.name())?,
            )?;
            temporary = temporary.max(allocation::<u32>(identity.namespace.owner_ids().len())?);
        }
        add(
            &mut identities,
            plan_hash_admission::tree_bound::<StableIdentity>(identity_count)?,
        )?;
        Ok(Self {
            required,
            expected,
            identities,
            temporary,
        })
    }
    pub(super) fn maximum(&self) -> Result<u64, BudgetUnavailable> {
        // Required namespaces and expected identities remain alive while the
        // history/alias set is built. Namespace lookup clones one owner vector
        // at a time; no canonical buffer or JSON generation occurs here.
        self.required
            .checked_add(self.expected)
            .and_then(|value| value.checked_add(self.identities))
            .and_then(|value| value.checked_add(self.temporary))
            .and_then(|value| value.checked_add(32 * 1024))
            .ok_or(BudgetUnavailable)
    }
}

impl ContractBundle {
    /// Revalidate already constructed IR's complete ledger with admission.
    /// Parsing, ownership of borrowed inputs and parent history remain separate.
    #[doc(hidden)]
    pub fn validate_provider_ledger_admitted_v1(
        &self,
        budget: &ProviderBudget,
    ) -> Result<(), ProviderBundleEncodingErrorV1> {
        let sizing = budget.reserve(32 * 1024)?;
        let bound = LedgerValidationBound::inspect(self)?;
        let _work = budget.reserve(bound.maximum()?)?;
        drop(sizing);
        validate_ledger(
            &self.ledger,
            &self.schema,
            &self.commands,
            &self.projections,
        )?;
        Ok(())
    }
}
