// req: CMP-013, CMP-015, TXN-001, PERF-012
#[test]
fn selection_reconstructs_migrated_logical_null_without_changing_raw_dependency_evidence() {
    use riffdb_storage_api::{IndexRangeEntry, IndexRangeObservation, IndexedSelectionRequest};
    use riffdb_types::IndexEpochPosition;
    let source = include_str!("../../../fixtures/compiler/indexed-selection/contract.riff");
    let first =
        ValidatedContractBundle::from_compiler_bundle(compile_contract_source(source).unwrap())
            .unwrap();
    let second_source = source.replace("version 1", "version 2").replace(
        "field holder: uuid",
        "field holder: uuid field note: optional<i64> field detail: optional<i64>",
    );
    let second = ValidatedContractBundle::from_compiler_bundle(
        compile_contract_successor(&second_source, first.bundle()).unwrap(),
    )
    .unwrap();
    let third_source = second_source.replace("version 2", "version 3")
        .replace("index by_available (pool_id, available, slot_id)", "index by_available (pool_id, available, slot_id) index by_note (pool_id, available, note, slot_id) presence(note) cover(detail)")
        .replace("mutate first Slot.by_available(", "mutate first Slot.by_note(");
    let third = ValidatedContractBundle::from_compiler_bundle(
        compile_contract_successor(&third_source, second.bundle()).unwrap(),
    )
    .unwrap();
    let edges = [(second.bundle_hash(), third.bundle_hash())]
        .into_iter()
        .collect();
    let proof = LineageMaterializationProof::from_historical_bundles(
        vec![first.clone(), second, third.clone()],
        &edges,
    )
    .unwrap();
    let plan = &third.bundle().commands()[0];
    let reference = ExecutablePlanRef::new(
        third.lineage().clone(),
        third.contract_version(),
        third.bundle_hash(),
        plan.command_id(),
        plan.plan_hash(),
    );
    let resolved = third.resolve_plan_with_proof(&reference, proof, 2).unwrap();
    let input = CanonicalRecord::new(
        plan.input()
            .record()
            .fields()
            .iter()
            .map(|field| {
                (
                    field.id(),
                    CanonicalValue::Uuid([if field.name() == "pool_id" { 2 } else { 3 }; 16]),
                )
            })
            .collect(),
    )
    .unwrap();
    let facts = derive_input_command_facts(plan, input.clone()).unwrap();
    let selection = IndexedSelectionRequest::from_plan(
        plan,
        third.bundle().schema(),
        facts.partition_key().clone(),
        facts.selection_prefix_values().unwrap(),
    )
    .unwrap();
    let entity = &third.bundle().schema().entities()[0];
    let values = CanonicalRecord::new(
        first.bundle().schema().entities()[0]
            .record()
            .fields()
            .iter()
            .map(|field| {
                (
                    field.id(),
                    if field.name() == "available" {
                        CanonicalValue::Bool(true)
                    } else {
                        CanonicalValue::Uuid([if field.name() == "pool_id" { 2 } else { 4 }; 16])
                    },
                )
            })
            .collect(),
    )
    .unwrap();
    let note = entity
        .record()
        .fields()
        .iter()
        .find(|field| field.name() == "note")
        .unwrap()
        .id();
    let detail = entity
        .record()
        .fields()
        .iter()
        .find(|field| field.name() == "detail")
        .unwrap()
        .id();
    let mut logical = values.fields().to_vec();
    logical.push((note, CanonicalValue::Null));
    logical.push((detail, CanonicalValue::Null));
    let logical = CanonicalRecord::new(logical).unwrap();
    let key = entity
        .primary_key()
        .encode_entity(&[CanonicalValue::Uuid([2; 16]), CanonicalValue::Uuid([4; 16])])
        .unwrap();
    let target = EntityTarget::new(entity.id(), key.clone()).unwrap();
    let index = entity
        .indexes()
        .iter()
        .find(|index| index.name() == "by_note")
        .unwrap();
    let index_values =
        riffdb_contract_ir::encode_operational_index_values_v1(index, &logical).unwrap();
    let entry = IndexRangeEntry::new(
        index.id(),
        index.key_schema().encode_index(&index_values, key).unwrap(),
        riffdb_contract_ir::encode_operational_index_cover_v1(index, &logical).unwrap(),
    )
    .unwrap();
    let raw = StoredEntityRecordV1::new(
        target,
        EntityVersion::first(),
        first.contract_version(),
        DurableKeySchemaBindingV1::new(
            first.lineage().clone(),
            first.contract_version(),
            first.bundle_hash(),
        ),
        values,
    )
    .unwrap();
    let request = SnapshotRequest::new(reference, vec![], vec![], vec![])
        .unwrap()
        .with_indexed_selection(selection.clone())
        .unwrap();
    let concrete = request.resolve_indexed_selection(Some(&entry)).unwrap();
    let snapshot = ReadSnapshot::new(
        &concrete,
        None,
        vec![EntityObservation::Present(raw.clone())],
        vec![],
        vec![
            IndexRangeObservation::new(
                selection.range().clone(),
                IndexEpochPosition::BeforeFirst,
                vec![entry],
            )
            .unwrap(),
        ],
    )
    .unwrap();
    // The same omitted field under the current writer has no ancestry authority,
    // even though its logical index key can be reconstructed. Keep this refusal.
    let invalid = StoredEntityRecordV1::new(
        raw.target().clone(),
        raw.entity_version(),
        third.contract_version(),
        DurableKeySchemaBindingV1::from_plan(request.plan()),
        raw.fields().clone(),
    )
    .unwrap();
    let invalid = ReadSnapshot::new(
        &concrete,
        None,
        vec![EntityObservation::Present(invalid)],
        vec![],
        snapshot.ranges().to_vec(),
    )
    .unwrap();
    assert_eq!(
        resolved
            .clone()
            .materialize_command_snapshot_for_input(&input, invalid)
            .expect_err("omission requires lineage authority")
            .kind(),
        CommandSnapshotMaterializationErrorKind::Integrity
    );
    let dependencies = snapshot.read_dependencies().clone();
    let current = TransactionCurrentState::new(
        &snapshot.validation_request(),
        snapshot.bindings().to_vec(),
        vec![],
        vec![riffdb_storage_api::CurrentRangeObservation::new(
            selection.range().clone(),
            IndexEpochPosition::BeforeFirst,
        )],
    )
    .unwrap();
    let CommandSnapshotMaterialization::Ready(materialized) = resolved
        .materialize_command_snapshot_for_input(&input, snapshot)
        .expect("valid historical selected row")
    else {
        panic!("bounded row")
    };
    assert_eq!(materialized.snapshot().read_dependencies(), &dependencies);
    let EntityObservation::Present(row) = &materialized.snapshot().bindings()[0] else {
        panic!("selected")
    };
    assert_eq!(row.fields(), &logical);
    assert_eq!(row.schema_binding(), raw.schema_binding());
    assert!(selection.selected_key(materialized.snapshot()).is_ok());
    assert!(matches!(
        materialized
            .materialize_transaction_current(current)
            .unwrap(),
        TransactionCurrentMaterialization::Ready(_)
    ));
}
