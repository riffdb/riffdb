//! Checked vectors cross the existing Bytes lane only at the generation boundary.
// req: REP-004, PRJ-004, PRJ-006, PRJ-009

use super::*;
use crate::{ColumnarProjectionDefinition, SegmentV2Predicate};
use riffdb_contract_compiler::compile_contract_source;
use riffdb_types::{CanonicalVector, EntityVersion, MAX_VECTOR_DIMENSION, encode_canonical_value};
use std::sync::atomic::{AtomicUsize, Ordering};

fn definition(dimension: u32) -> RegisteredDefinition {
    let source = format!(
        r#"contract VectorBytes version 1 {{
          entity Document {{
            key (org_id: uuid, doc_id: uuid)
            field title: string<64>
            vector_field embedding({dimension}, cosine, (title), staleness_slo 60)
          }}
          aggregate Documents {{
            root Document
            partition_by org_id
            conflict_key (org_id, doc_id)
          }}
        }}"#
    );
    let bundle = compile_contract_source(&source).expect("checked vector schema");
    let entity = &bundle.schema().entities()[0];
    let field = |name| {
        entity
            .record()
            .fields()
            .iter()
            .find(|f| f.name() == name)
            .unwrap()
            .id()
    };
    RegisteredDefinition::register(
        ColumnarProjectionDefinition {
            name: "vectors".into(),
            entity_name: "Document".into(),
            projected_fields: vec![field("embedding"), field("title")],
            org_scope_field: field("org_id"),
        },
        &bundle,
    )
    .unwrap()
}

fn vector(components: Vec<f32>) -> CanonicalValue {
    CanonicalValue::Vector(CanonicalVector::new(components).unwrap())
}

fn organization() -> OrgKey {
    OrgKey::from_value(&CanonicalValue::Uuid([0x31; 16])).unwrap()
}

fn row(value: CanonicalValue) -> LiveRow {
    LiveRow {
        entity_version: EntityVersion::new(1).unwrap(),
        cells: vec![value, CanonicalValue::string("kept").unwrap()],
    }
}

fn segment(definition: &RegisteredDefinition, rows: &[LiveRow]) -> SegmentV2 {
    let keys = (0..rows.len())
        .map(|i| PrimaryKeyBytes::from_entity_key_bytes((i as u64).to_be_bytes()))
        .collect::<Vec<_>>();
    let pairs = keys.iter().zip(rows).collect::<Vec<_>>();
    build_segment(
        definition,
        &logical_types(definition).unwrap(),
        1,
        ProjectionGeneration::first(),
        organization(),
        SegmentV2SegmentId::from_bytes([0x41; 16]),
        FrontierPosition::BeforeFirst,
        FrontierPosition::BeforeFirst,
        &pairs,
    )
    .expect("schema-checked vector lowering")
}

#[test]
fn canonical_vectors_round_trip_through_v2_bytes_with_nullable_and_max_dimension() {
    for (dimension, values) in [
        (3, vec![vector(vec![1.0, -2.0, 0.0])]),
        (
            MAX_VECTOR_DIMENSION,
            vec![vector(vec![0.25; MAX_VECTOR_DIMENSION as usize])],
        ),
    ] {
        let definition = definition(dimension);
        assert!(ValidatedColumnarV2Generation::supports_definition(
            &definition
        ));
        let rows = values.into_iter().map(row).collect::<Vec<_>>();
        let segment = segment(&definition, &rows);
        let bytes = SegmentV2Codec::encode(&segment).unwrap();
        let reopened = SegmentV2Codec::decode(&bytes).unwrap();
        assert_eq!(SegmentV2Codec::encode(&reopened).unwrap(), bytes);
        let decoded = decode_rows(&reopened, &definition).unwrap();
        assert_eq!(decoded.into_values().collect::<Vec<_>>(), rows);
        let column = reopened
            .columns()
            .iter()
            .find(|c| c.field_id() == definition.projected_fields()[0])
            .unwrap();
        assert_eq!(column.logical_type(), &SegmentV2LogicalType::Bytes);
        if dimension == 3 {
            let expected = vec![
                1, 14, 0, 0, 0, 3, 0x3f, 0x80, 0, 0, 0xc0, 0, 0, 0, 0, 0, 0, 0,
            ];
            assert_eq!(
                column.cells()[0],
                SegmentV2Cell::Value(CanonicalValue::bytes(expected).unwrap())
            );
        }
    }

    let required = ValueType::vector(riffdb_types::VectorDimension::new(3).unwrap());
    let optional = ValueType::optional(required.clone()).unwrap();
    let typed = [vector(vec![1.0, -2.0, 0.0]), CanonicalValue::Null];
    let cells = typed
        .iter()
        .map(|value| values::lower(value, &optional).unwrap())
        .collect();
    let template = segment(
        &definition(3),
        &[row(typed[0].clone()), row(typed[0].clone())],
    );
    let column = SegmentV2Column::new(
        template.columns()[0].field_id(),
        SegmentV2LogicalType::Bytes,
        cells,
    )
    .unwrap();
    let nullable = SegmentV2::new(
        template.identity().clone(),
        template.primary_keys().to_vec(),
        template.entity_versions().to_vec(),
        vec![column],
    )
    .unwrap();
    let reopened = SegmentV2Codec::decode(&SegmentV2Codec::encode(&nullable).unwrap()).unwrap();
    for (cell, value) in reopened.columns()[0].cells().iter().zip(typed) {
        assert_eq!(values::restore(cell, &optional).unwrap(), value);
    }
    assert_eq!(
        values::restore(&SegmentV2Cell::Null, &required),
        Err(ColumnarV2GenerationError::Invalid)
    );
    assert!(values::lower(&CanonicalValue::Null, &required).is_err());
    assert!(values::lower(&vector(vec![1.0, 2.0]), &required).is_err());

    let bytes_type = ValueType::bytes(128).unwrap();
    let ordinary_bytes =
        CanonicalValue::bytes(encode_canonical_value(&vector(vec![1.0, 2.0, 3.0])).unwrap())
            .unwrap();
    let physical = values::lower(&ordinary_bytes, &bytes_type).unwrap();
    assert_eq!(physical, SegmentV2Cell::Value(ordinary_bytes.clone()));
    assert_eq!(
        values::restore(&physical, &bytes_type).unwrap(),
        ordinary_bytes
    );
}

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "riffdb-v2-vectors-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
fn vector_generation_reopens_typed_rows_without_vector_pruning_and_compares_logically() {
    let definition = definition(3);
    let directory = Directory::new();
    let key = PrimaryKeyBytes::from_entity_key_bytes([1]);
    let mut expected = ColumnarSnapshot::empty();
    expected.delta.insert(
        organization(),
        BTreeMap::from([(key.clone(), row(vector(vec![1.0, 0.0, 0.0])))]),
    );
    let generation = ValidatedColumnarV2Generation::prepare(
        &directory.0,
        definition.clone(),
        1,
        ProjectionGeneration::first(),
        FrontierPosition::BeforeFirst,
        &expected,
    )
    .unwrap();
    let reopened = ValidatedColumnarV2Generation::open(
        &directory.0,
        definition.clone(),
        1,
        ProjectionGeneration::first(),
        FrontierPosition::BeforeFirst,
        generation.artifact_identity(),
        generation.root().physical_generation_fingerprint(),
    )
    .unwrap();
    assert_eq!(
        reopened.snapshot().segments[0].rows[&key],
        expected.delta[&organization()][&key]
    );
    let pruning = reopened.snapshot().segments[0].pruning.as_ref().unwrap();
    let vector_field = definition.projected_fields()[0];
    let absent_bytes = CanonicalValue::bytes(vec![255]).unwrap();
    assert!(!pruning.proves_no_match(vector_field, &SegmentV2Predicate::Equal(absent_bytes)));
    assert!(!pruning.proves_no_match(vector_field, &SegmentV2Predicate::IsNull));
    let scalar_field = definition.projected_fields()[1];
    assert!(pruning.proves_no_match(
        scalar_field,
        &SegmentV2Predicate::Equal(CanonicalValue::string("absent").unwrap())
    ));

    expected
        .delta
        .get_mut(&organization())
        .unwrap()
        .get_mut(&key)
        .unwrap()
        .cells[0] = vector(vec![0.0, 1.0, 0.0]);
    assert!(matches!(
        ValidatedColumnarV2Generation::prepare(
            &directory.0,
            definition,
            1,
            ProjectionGeneration::first(),
            FrontierPosition::BeforeFirst,
            &expected,
        ),
        Err(ColumnarV2GenerationError::LogicalMismatch)
    ));
}

#[test]
fn vector_payload_corruption_refuses_after_valid_segment_checksums() {
    let definition = definition(3);
    let valid = segment(&definition, &[row(vector(vec![1.0, -2.0, 0.0]))]);
    let original = encode_canonical_value(&vector(vec![1.0, -2.0, 0.0])).unwrap();
    let mut payloads = vec![
        encode_canonical_value(&CanonicalValue::U64(3)).unwrap(),
        encode_canonical_value(&vector(vec![1.0, 2.0])).unwrap(),
    ];
    let mut trailing = original.clone();
    trailing.push(0);
    payloads.push(trailing);
    for bits in [0x8000_0000u32, 0x7fc0_0000, 0x7f80_0000, 0xff80_0000] {
        let mut bytes = original.clone();
        bytes[6..10].copy_from_slice(&bits.to_be_bytes());
        payloads.push(bytes);
    }
    let mut truncated = original.clone();
    truncated.pop();
    payloads.push(truncated);
    for dimension in [0, MAX_VECTOR_DIMENSION + 1] {
        let mut bytes = original.clone();
        bytes[2..6].copy_from_slice(&dimension.to_be_bytes());
        payloads.push(bytes);
    }
    let mut cells = payloads
        .into_iter()
        .map(|bytes| SegmentV2Cell::Value(CanonicalValue::bytes(bytes).unwrap()))
        .collect::<Vec<_>>();
    cells.push(SegmentV2Cell::Missing);
    for cell in cells {
        let columns = valid
            .columns()
            .iter()
            .map(|column| {
                if column.field_id() == definition.projected_fields()[0] {
                    SegmentV2Column::new(
                        column.field_id(),
                        SegmentV2LogicalType::Bytes,
                        vec![cell.clone()],
                    )
                    .unwrap()
                } else {
                    column.clone()
                }
            })
            .collect();
        let malformed = SegmentV2::new(
            valid.identity().clone(),
            valid.primary_keys().to_vec(),
            valid.entity_versions().to_vec(),
            columns,
        )
        .unwrap();
        let bytes = SegmentV2Codec::encode(&malformed).unwrap();
        let physical = SegmentV2Codec::decode(&bytes).unwrap();
        assert_eq!(
            decode_rows(&physical, &definition),
            Err(ColumnarV2GenerationError::Invalid)
        );
    }

    let columns = valid
        .columns()
        .iter()
        .map(|column| {
            if column.field_id() == definition.projected_fields()[0] {
                SegmentV2Column::new(
                    column.field_id(),
                    SegmentV2LogicalType::U64,
                    vec![SegmentV2Cell::Value(CanonicalValue::U64(3))],
                )
                .unwrap()
            } else {
                column.clone()
            }
        })
        .collect();
    let wrong_physical_type = SegmentV2::new(
        valid.identity().clone(),
        valid.primary_keys().to_vec(),
        valid.entity_versions().to_vec(),
        columns,
    )
    .unwrap();
    assert_eq!(
        decode_rows(&wrong_physical_type, &definition),
        Err(ColumnarV2GenerationError::Invalid)
    );
}

#[test]
fn vector_lowering_retains_existing_row_and_lane_bounds() {
    let definition = definition(MAX_VECTOR_DIMENSION);
    let row = row(vector(vec![0.25; MAX_VECTOR_DIMENSION as usize]));
    let physical = values::lower(&row.cells[0], &definition.projected_types()[0]).unwrap();
    assert!(
        SegmentV2Column::new(
            definition.projected_fields()[0],
            SegmentV2LogicalType::Bytes,
            vec![SegmentV2Cell::Null; MAX_SEGMENT_V2_ROWS + 1],
        )
        .is_err()
    );
    // 1024 maximum-dimension vectors exceed the unchanged 16-MiB lane ceiling
    // once canonical framing is included, well below the row ceiling.
    let template = segment(&definition, std::slice::from_ref(&row));
    let column = SegmentV2Column::new(
        definition.projected_fields()[0],
        SegmentV2LogicalType::Bytes,
        vec![physical; 1024],
    )
    .unwrap();
    let keys = (0u64..1024)
        .map(|i| PrimaryKeyBytes::from_entity_key_bytes(i.to_be_bytes()))
        .collect();
    let excessive = SegmentV2::new(
        template.identity().clone(),
        keys,
        vec![EntityVersion::new(1).unwrap(); 1024],
        vec![column],
    )
    .unwrap();
    assert!(matches!(
        SegmentV2Codec::encode(&excessive),
        Err(crate::SegmentV2Error::BoundExceeded(_))
    ));
}

#[test]
fn vector_nearest_results_match_independent_rows_after_reopen_and_file_removal() {
    use crate::{ColumnPredicate, NearestQueryRequest, QueryBudget, nearest_query_snapshot};
    use riffdb_types::{DistanceMetric, EntityKeyBuilder};

    let definition = definition(3);
    let directory = Directory::new();
    let mut expected = ColumnarSnapshot::empty();
    for (org, id, components, title) in [
        ([0x31; 16], 1, vec![1.0, 0.0, 0.0], "visible"),
        ([0x31; 16], 2, vec![0.0, 1.0, 0.0], "visible"),
        ([0x31; 16], 3, vec![1.0, 0.0, 0.0], "excluded"),
        ([0x32; 16], 4, vec![1.0, 0.0, 0.0], "visible"),
    ] {
        let mut key = EntityKeyBuilder::new(definition.entity_type_id());
        key.push_uuid(&org).unwrap();
        key.push_uuid(&[id; 16]).unwrap();
        let key = PrimaryKeyBytes::from_entity_key_bytes(key.finish().unwrap().into_bytes());
        let mut live = row(vector(components));
        live.cells[1] = CanonicalValue::string(title).unwrap();
        expected
            .delta
            .entry(OrgKey::from_value(&CanonicalValue::Uuid(org)).unwrap())
            .or_default()
            .insert(key, live);
    }
    let prepared = ValidatedColumnarV2Generation::prepare(
        &directory.0,
        definition.clone(),
        1,
        ProjectionGeneration::first(),
        FrontierPosition::BeforeFirst,
        &expected,
    )
    .unwrap();
    let reopened = ValidatedColumnarV2Generation::open(
        &directory.0,
        definition.clone(),
        1,
        ProjectionGeneration::first(),
        FrontierPosition::BeforeFirst,
        prepared.artifact_identity(),
        prepared.root().physical_generation_fingerprint(),
    )
    .unwrap();
    fs::remove_dir_all(reopened.directory()).unwrap();
    let request = NearestQueryRequest {
        org_scope: CanonicalValue::Uuid([0x31; 16]),
        vector_field: definition.projected_fields()[0],
        query_vector: CanonicalVector::new(vec![1.0, 0.0, 0.0]).unwrap(),
        k: 10,
        metric: DistanceMetric::Cosine,
        predicates: vec![ColumnPredicate::Eq {
            field: definition.projected_fields()[1],
            value: CanonicalValue::string("visible").unwrap(),
        }],
        budget: QueryBudget::default(),
    };
    let original = nearest_query_snapshot(&definition, &expected, &request).unwrap();
    let actual = nearest_query_snapshot(&definition, reopened.snapshot(), &request).unwrap();
    assert_eq!(actual.rows.len(), 2);
    assert_eq!(actual.scanned_rows, 3);
    assert_eq!(actual.scanned_rows, original.scanned_rows);
    assert_eq!(actual.search_kind, original.search_kind);
    for (actual, expected) in actual.rows.iter().zip(&original.rows) {
        assert_eq!(actual.primary_key, expected.primary_key);
        assert_eq!(actual.cells, expected.cells);
        assert_eq!(actual.distance.to_bits(), expected.distance.to_bits());
    }
    assert_eq!(actual.rows[0].distance.to_bits(), 0.0f32.to_bits());
    assert_eq!(actual.rows[1].distance.to_bits(), 1.0f32.to_bits());
}
