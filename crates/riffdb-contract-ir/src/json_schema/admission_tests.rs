//! Admission checks use the compiler's frozen canonical artifacts.
// req: OQ-040, PRJ-004
use super::*;
use crate::ContractBundle;
use riffdb_types::exact_provider_budget_v1::{EXACT_PROVIDER_BUDGET_BYTES, ProviderBudget};

fn check(source: ProviderSchemaArtifactSourceV1<'_>, bundle: &ContractBundle) {
    {
        let budget = ProviderBudget::new();
        let _scratch = budget
            .reserve(riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
            .unwrap();
        let expected = source.expected_size(bundle.schema()).unwrap();
        let maximum = admission::json_workspace_bound(expected).unwrap();
        let _work = budget.reserve(maximum).unwrap();
        let (_, node, actual_size) = source.parts(bundle.schema()).unwrap();
        assert_eq!(expected, actual_size);
        let inventory = riffdb_types::OwnedCapacityInspectionV1::inspect(&node).unwrap();
        assert!(inventory.shared_allocations().is_empty());
        let mut encoded = String::with_capacity(expected);
        node.write(&mut encoded);
        assert_eq!(encoded.len(), expected);
        let retained_and_output = inventory.unshared_bytes() + 2 * encoded.capacity() as u64 + 4096;
        // Doubling the complete observed tree covers a simultaneous old/new
        // vector or string buffer during any one construction reallocation.
        assert!(
            2 * retained_and_output <= maximum,
            "JSON workspace must cover native container capacities and rendering"
        );
    }
    let budget = ProviderBudget::new();
    let artifact = source.generate_admitted(bundle.schema(), &budget).unwrap();
    let expected = bundle
        .schema_artifacts()
        .iter()
        .find(|value| value.key() == artifact.key())
        .unwrap();
    assert_eq!(&**artifact, expected);
    let capture = artifact.clone();
    drop(artifact);
    assert!(budget.observation().unwrap().owned_bytes > 0);
    drop(capture);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}

#[test]
fn admitted_schema_generation_preserves_every_artifact_in_the_frozen_bundles() {
    for bytes in [
        include_bytes!("../../../../fixtures/compiler/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/production-embedding/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/reimport/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/cascade/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/vector-ann/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/secret/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/text-index/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/initialized-state-transition/bundle.bin")
            .as_slice(),
        include_bytes!("../../../../fixtures/compiler/secret-reveal/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/workflow-initial/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/indexed-selection/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/command-decision/bundle.bin").as_slice(),
        include_bytes!("../../../../fixtures/compiler/aggregate-collection-budget/bundle.bin")
            .as_slice(),
    ] {
        let bundle = ContractBundle::decode(bytes).unwrap();
        let mut count = 0;
        for entity in bundle.schema().entities() {
            check(
                ProviderSchemaArtifactSourceV1::Entity {
                    id: entity.id(),
                    record: entity.record(),
                },
                &bundle,
            );
            count += 1;
        }
        for event in bundle.schema().events() {
            check(
                ProviderSchemaArtifactSourceV1::Event {
                    id: event.id(),
                    record: event.payload(),
                },
                &bundle,
            );
            count += 1;
        }
        for command in bundle.commands() {
            check(
                ProviderSchemaArtifactSourceV1::CommandInput {
                    id: command.command_id(),
                    record: command.input().record(),
                    idempotency_input: command.idempotency_input(),
                },
                &bundle,
            );
            check(
                ProviderSchemaArtifactSourceV1::CommandOutcomes {
                    id: command.command_id(),
                    outcomes: command.outcomes(),
                },
                &bundle,
            );
            count += 2;
        }
        for projection in bundle.projections() {
            let group_types = projection
                .group_schema()
                .group_components()
                .iter()
                .map(|component| component.value_type().clone())
                .collect::<Vec<_>>();
            check(
                ProviderSchemaArtifactSourceV1::ProjectionResult {
                    id: projection.projection_id(),
                    group_types: &group_types,
                    measures: projection.group_schema().measures(),
                },
                &bundle,
            );
            check(
                ProviderSchemaArtifactSourceV1::ProjectionResultFromPlan { plan: projection },
                &bundle,
            );
            count += 1;
        }
        assert_eq!(count, bundle.schema_artifacts().len());
        assert_eq!(bundle.canonical_bytes(), bytes);
    }
}

#[test]
fn admitted_schema_generation_refuses_before_json_construction_and_preserves_owner_errors() {
    let bundle =
        ContractBundle::decode(include_bytes!("../../../../fixtures/compiler/bundle.bin")).unwrap();
    let entity = &bundle.schema().entities()[0];
    let budget = ProviderBudget::new();
    let blocker = budget
        .reserve(
            EXACT_PROVIDER_BUDGET_BYTES - budget.observation().unwrap().fixed_owner_bytes - 128,
        )
        .unwrap();
    let source = ProviderSchemaArtifactSourceV1::Entity {
        id: entity.id(),
        record: entity.record(),
    };
    assert!(matches!(
        source.generate_admitted(bundle.schema(), &budget),
        Err(ProviderSchemaArtifactErrorV1::Unavailable)
    ));
    assert_eq!(budget.observation().unwrap().reservations, 1);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    drop(blocker);
    // The sizing allowance fits, but the JSON construction allowance does not.
    let blocker = budget
        .reserve(
            EXACT_PROVIDER_BUDGET_BYTES
                - budget.observation().unwrap().fixed_owner_bytes
                - 64 * 1024,
        )
        .unwrap();
    assert!(matches!(
        source.generate_admitted(bundle.schema(), &budget),
        Err(ProviderSchemaArtifactErrorV1::Unavailable)
    ));
    assert_eq!(budget.observation().unwrap().reservations, 1);
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    drop(blocker);
    let wrong = EntityTypeId::new(u32::MAX).unwrap();
    let ordinary =
        GeneratedSchemaArtifact::entity(wrong, entity.record(), bundle.schema()).unwrap_err();
    let source = ProviderSchemaArtifactSourceV1::Entity {
        id: wrong,
        record: entity.record(),
    };
    assert!(
        matches!(source.generate_admitted(bundle.schema(), &budget), Err(ProviderSchemaArtifactErrorV1::Invalid(error)) if error == ordinary)
    );
    assert_eq!(budget.observation().unwrap().owned_bytes, 0);
    assert_eq!(budget.observation().unwrap().reserved_bytes, 0);
}

#[test]
fn schema_json_workspace_covers_sparse_roots_vector_growth_and_overflow() {
    let budget = ProviderBudget::new();
    let _scratch = budget
        .reserve(riffdb_types::OWNED_CAPACITY_INSPECTION_SCRATCH_BYTES_V1)
        .unwrap();
    let _work = budget.reserve(64 * 1024 * 1024).unwrap();
    for count in [0, 1, 2, 4, 5, 8, 17, 65] {
        let mut values = Vec::new();
        for index in 0..count {
            values.push(Json::Object(BTreeMap::from([(
                format!("k{index}"),
                Json::String("value".to_owned()),
            )])));
        }
        let mut node = Json::object([
            ("array", Json::Array(values)),
            ("empty", Json::Object(BTreeMap::new())),
        ]);
        for _ in 0..32 {
            let mut values = Vec::with_capacity(4);
            values.push(node);
            node = Json::Array(values);
        }
        let inventory = riffdb_types::OwnedCapacityInspectionV1::inspect(&node).unwrap();
        let mut rendered = String::new();
        node.write(&mut rendered);
        let bound = admission::json_workspace_bound(rendered.len()).unwrap();
        assert!(2 * (inventory.unshared_bytes() + 2 * rendered.capacity() as u64 + 4096) <= bound);
    }
    assert_eq!(
        admission::json_workspace_bound(usize::MAX),
        Err(riffdb_types::exact_provider_budget_v1::BudgetUnavailable)
    );
}
