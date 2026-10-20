//! Numerical ownership inspection leaves compiled query semantics intact.

use crate::{NamedQuerySource, QueryModule, QueryModuleCandidate};
use riffdb_contract_compiler::compile_contract_source;
use riffdb_types::{QueryModuleName, QueryModuleVersion};

mod binary_text {
    use super::*;
    const CONTRACT: &str = r#"
contract ExactUsers version 1 {
  entity User {
    key (organization_id: uuid, user_id: uuid)
    field name: string<128>
    index by_name (organization_id, name, user_id) text_key(name, binary_utf8_v1)
  }
  aggregate Users {
    root User
    partition_by organization_id
    conflict_key (organization_id, user_id)
  }
}

"#;

    const QUERY: &str = r#"
query SearchUsers(
  $organization_id: User.organization_id,
  $needle: User.name,
  $limit: Limit<499> = 50,
  $offset: u64 = 0
) {
  many users from User
    where organization_id == $organization_id && name contains $needle
    order by name asc, user_id asc
    take $limit offset $offset
  aggregate total from users { exact_count() as value }
  return Found { users: users { user_id name } total: total { value } }
  outcomes Found
}
"#;

    // req: OQ-040
    #[test]
    fn binary_text_capacity_inspection_preserves_compiled_identity_and_shared_metadata() {
        use riffdb_types::OwnedCapacityInspectionV1;
        let contract = compile_contract_source(CONTRACT).unwrap();
        let candidate = QueryModuleCandidate::new(
            QueryModuleName::new("ownership_test").unwrap(),
            QueryModuleVersion::new(1).unwrap(),
            vec![NamedQuerySource::new("SearchUsers", QUERY).unwrap()],
        )
        .unwrap();
        let module = QueryModule::compile(candidate, &contract).unwrap();
        let compiled = module.queries()[0].exact_text_result().unwrap();
        let identity = compiled.identity();
        // Test-only numerical census: admit its complete scratch before traversal.
        let capacity_test_budget_0 = riffdb_types::exact_provider_budget_v1::ProviderBudget::new();
        let mut capacity_test_work_0 = capacity_test_budget_0
            .reserve(riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
            .unwrap();
        let one = OwnedCapacityInspectionV1::preview(
            compiled,
            capacity_test_work_0.inspection_scope().unwrap(),
        )
        .unwrap();
        // Test-only numerical census: admit its complete scratch before traversal.
        let capacity_test_budget_1 = riffdb_types::exact_provider_budget_v1::ProviderBudget::new();
        let mut capacity_test_work_1 = capacity_test_budget_1
            .reserve(riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
            .unwrap();
        let two = OwnedCapacityInspectionV1::preview(
            &[compiled.clone(), compiled.clone()],
            capacity_test_work_1.inspection_scope().unwrap(),
        )
        .unwrap();
        assert!(!one.shared_allocations().is_empty());
        assert!(
            one.shared_allocations()
                .iter()
                .all(|allocation| allocation.lifecycle().is_none())
        );
        assert_eq!(
            two.shared_allocations().len(),
            one.shared_allocations().len()
        );
        assert_eq!(
            two.shared_allocations()
                .iter()
                .map(|owner| owner.bytes())
                .sum::<u64>(),
            one.shared_allocations()
                .iter()
                .map(|owner| owner.bytes())
                .sum::<u64>()
        );
        assert!(one.unshared_bytes() > 0);
        assert_eq!(compiled.identity(), identity);
        let decoded =
            QueryModule::decode_and_validate(module.canonical_bytes(), &contract).unwrap();
        assert_eq!(decoded.identity(), module.identity());
        assert_eq!(decoded.canonical_bytes(), module.canonical_bytes());
    }
}

mod tokenized_text {
    use super::*;
    const CONTRACT: &str = include_str!("../../../fixtures/compiler/text-index/contract.riff");

    const QUERY: &str = r#"
query SearchDocuments(
  $org_id: Document.org_id,
  $query: Document.title,
  $limit: Limit<499> = 50,
  $offset: u64 = 0
) {
  many documents from Document
    where org_id == $org_id
    matching(search, conjunction, $query)
    order by doc_id asc
    take $limit offset $offset
  return Found { documents: documents { doc_id title } }
  outcomes Found
}
"#;

    // req: OQ-040
    #[test]
    fn tokenized_text_capacity_inspection_preserves_compiled_identity_and_shared_metadata() {
        use riffdb_types::OwnedCapacityInspectionV1;
        let contract = compile_contract_source(CONTRACT).unwrap();
        let candidate = QueryModuleCandidate::new(
            QueryModuleName::new("ownership_test").unwrap(),
            QueryModuleVersion::new(1).unwrap(),
            vec![NamedQuerySource::new("SearchDocuments", QUERY).unwrap()],
        )
        .unwrap();
        let module = QueryModule::compile(candidate, &contract).unwrap();
        let compiled = module.queries()[0].tokenized_text_result().unwrap();
        let identity = compiled.identity();
        // Test-only numerical census: admit its complete scratch before traversal.
        let capacity_test_budget_2 = riffdb_types::exact_provider_budget_v1::ProviderBudget::new();
        let mut capacity_test_work_2 = capacity_test_budget_2
            .reserve(riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
            .unwrap();
        let one = OwnedCapacityInspectionV1::preview(
            compiled,
            capacity_test_work_2.inspection_scope().unwrap(),
        )
        .unwrap();
        // Test-only numerical census: admit its complete scratch before traversal.
        let capacity_test_budget_3 = riffdb_types::exact_provider_budget_v1::ProviderBudget::new();
        let mut capacity_test_work_3 = capacity_test_budget_3
            .reserve(riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
            .unwrap();
        let two = OwnedCapacityInspectionV1::preview(
            &[compiled.clone(), compiled.clone()],
            capacity_test_work_3.inspection_scope().unwrap(),
        )
        .unwrap();
        assert!(!one.shared_allocations().is_empty());
        assert!(
            one.shared_allocations()
                .iter()
                .all(|allocation| allocation.lifecycle().is_none())
        );
        assert_eq!(
            two.shared_allocations().len(),
            one.shared_allocations().len()
        );
        assert_eq!(
            two.shared_allocations()
                .iter()
                .map(|owner| owner.bytes())
                .sum::<u64>(),
            one.shared_allocations()
                .iter()
                .map(|owner| owner.bytes())
                .sum::<u64>()
        );
        assert!(one.unshared_bytes() > 0);
        assert_eq!(compiled.identity(), identity);
        let decoded =
            QueryModule::decode_and_validate(module.canonical_bytes(), &contract).unwrap();
        assert_eq!(decoded.identity(), module.identity());
        assert_eq!(decoded.canonical_bytes(), module.canonical_bytes());
    }
}
