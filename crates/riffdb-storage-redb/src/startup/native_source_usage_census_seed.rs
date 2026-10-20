//! Actual stored source rows/controls for numeric reconstruction only.
//! No test fixture grants publication, closure or coordinator authority.
// req: STO-012, STO-020, PRJ-004, REC-001
use super::*;
use crate::source_keys::*;
use redb::TableDefinition;
use riffdb_storage_api::{
    AuthoritativeNamespaceV3 as N, AuthoritativeTransactionBindingV3, ChangelogHistoryStateV3,
    ProjectionSourceDescriptorPartsV1, ProjectionSourceDescriptorV1,
    ProjectionSourceLifecycleV1 as Life, ProjectionSourceMaintenanceV1, ProjectionSourceObjectV1,
    ProjectionSourceRootV1, ProjectionSourceRowV1, proto_codec::*,
};
use riffdb_types::{
    CanonicalRecord, CanonicalValue, ContractLineage, FieldId, HashDomain, ProjectionGeneration,
    ProjectionGroupKeyBuilder, ProjectionId, ProjectionIdentity, ProjectionPlanHash,
    ProjectionSourceLocatorV1, hash,
};

struct StoredRows {
    rows: Vec<(Vec<u8>, Vec<u8>)>,
    descriptor: ProjectionSourceDescriptorV1,
}
fn rows(
    history: ChangelogHistoryStateV3,
    ordinal: u64,
    count: u32,
    prior: Option<riffdb_types::ContentDigest>,
) -> StoredRows {
    let identity = ProjectionIdentity::new(
        ContractLineage::new("SourceCensus").unwrap(),
        ProjectionId::first(),
        ProjectionPlanHash::from_bytes([7; 32]),
    );
    let locator = ProjectionSourceLocatorV1::new(
        history.lineage().database_id(),
        history.lineage().history_incarnation(),
        ProjectionCheckpointOrdinal::new(ordinal).unwrap(),
    )
    .unwrap();
    let through = history.tail().frontier().application().unwrap();
    let mut typed = Vec::new();
    let mut semantic_bytes = 0_u64;
    for index in 0..count {
        let mut key =
            ProjectionGroupKeyBuilder::new(identity.clone(), ProjectionGeneration::first());
        key.push_component(CanonicalValue::string(format!("group{index:04}")).unwrap())
            .unwrap();
        let key = key.finish().unwrap();
        let row = ProjectionSourceRowV1::from_stored_parts(
            identity.clone(),
            locator,
            key.components().to_vec(),
            CanonicalRecord::new(vec![(FieldId::first(), CanonicalValue::U64(1))]).unwrap(),
            through,
        )
        .unwrap();
        semantic_bytes += riffdb_projection::projection_source_row_preimage_bytes_v1(&row).unwrap();
        typed.push((key, row));
    }
    let mut root = riffdb_projection::ProjectionSourceRowRootV1::new(
        &identity,
        Some(locator),
        through,
        count,
        semantic_bytes,
    )
    .unwrap();
    let mut physical = Vec::new();
    let mut native_bytes = 0_u64;
    for (key, row) in typed {
        let mut scratch = Vec::with_capacity(
            riffdb_projection::projection_source_row_scratch_bytes_v1(&row).unwrap(),
        );
        root.append(&key, &row, &mut scratch).unwrap();
        let mut native_key = Vec::with_capacity(source_row_key_len(&key).unwrap());
        encode_source_row_key_into(&mut native_key, locator, &key).unwrap();
        let value = encode_projection_source_row_v1(&row)
            .unwrap()
            .as_bytes()
            .to_vec();
        native_bytes += (native_key.len() + value.len()) as u64;
        physical.push((native_key, value));
    }
    let row_root = root.finish().unwrap();
    let descriptor = ProjectionSourceDescriptorV1::new(ProjectionSourceDescriptorPartsV1 {
        identity,
        locator,
        applied_through: through,
        captured_source: history,
        origin: hash(
            HashDomain::ProjectionSourceOrigin,
            b"native numeric census fixture",
        ),
        prior_source_digest: prior,
        row_count: count,
        canonical_row_bytes: native_bytes,
        row_root,
    })
    .unwrap();
    StoredRows {
        rows: physical,
        descriptor,
    }
}
fn object_key(descriptor: &ProjectionSourceDescriptorV1) -> [u8; SOURCE_LOCATOR_KEY_BYTES] {
    source_object_key(descriptor.reference().locator())
}
fn insert(
    write: &crate::changelog_v3_write::CapturedImmediateWrite,
    namespace: N,
    key: &[u8],
    value: &[u8],
) {
    let table: TableDefinition<&[u8], &[u8]> = TableDefinition::new(namespace.table());
    write.open_table(table).unwrap().insert(key, value).unwrap();
}
fn publication(
    history: ChangelogHistoryStateV3,
    descriptor: &ProjectionSourceDescriptorV1,
) -> ProjectionSourceRootV1 {
    let prior = history.tail();
    ProjectionSourceRootV1::new(
        descriptor.reference().clone(),
        AuthoritativeTransactionBindingV3 {
            database_id: history.lineage().database_id(),
            history_incarnation: history.lineage().history_incarnation(),
            predecessor: Some(prior.sequence()),
            sequence: prior.sequence().checked_next().unwrap(),
            predecessor_frontier: prior.frontier(),
            covered_frontier: prior.frontier(),
            prior_history_hash: prior.history_hash(),
        },
        None,
    )
    .unwrap()
}
fn put_object(
    write: &crate::changelog_v3_write::CapturedImmediateWrite,
    rows: &StoredRows,
    life: Life,
    physical: usize,
) {
    let object = ProjectionSourceObjectV1::new(rows.descriptor.clone(), life);
    let encoded = encode_projection_source_object_v1(&object).unwrap();
    insert(
        write,
        N::ProjectionSourceObjects,
        &object_key(&rows.descriptor),
        encoded.as_bytes(),
    );
    for (key, value) in rows.rows.iter().take(physical) {
        insert(write, N::ProjectionSourceRows, key, value);
    }
}
fn table_bytes(ports: &crate::RedbOperationalPorts, namespace: N) -> u64 {
    let read = ports.shared.database.begin_read().unwrap();
    let definition: TableDefinition<&[u8], &[u8]> = TableDefinition::new(namespace.table());
    read.open_table(definition)
        .unwrap()
        .iter()
        .unwrap()
        .map(|row| {
            let (key, value) = row.unwrap();
            (key.value().len() + value.value().len()) as u64
        })
        .sum()
}
fn allocator(write: &crate::changelog_v3_write::CapturedImmediateWrite, next: u64) {
    let allocator = ProjectionCheckpointOrdinalAllocatorV1::new(
        1,
        ProjectionCheckpointOrdinalPositionV1::Next(
            ProjectionCheckpointOrdinal::new(next).unwrap(),
        ),
    )
    .unwrap();
    let encoded = encode_projection_checkpoint_ordinal_allocator_v1(&allocator).unwrap();
    write
        .open_table(crate::layout::META)
        .unwrap()
        .insert("projection_checkpoint_ordinal/v1", encoded.as_bytes())
        .unwrap();
}

#[test]
fn actual_source_census_counts_selected_superseded_retiring_and_full_candidate_in_bounded_pages() {
    let (_directory, ports) = fresh(None);
    super::join_actual_native_command_prerequisite(&ports);
    super::super::native_source_command_fixture::append(&ports, 1).unwrap();
    let history = crate::changelog_v3_cursor::capture_history_position_v1(
        &ports.begin_composite_read().unwrap(),
    )
    .unwrap();
    let selected = rows(history, 1, 130, None);
    let selected_root = publication(history, &selected.descriptor);
    let superseded = rows(history, 2, 1, None);
    let retiring = rows(history, 3, 2, None);
    let copying = rows(
        history,
        4,
        3,
        Some(selected.descriptor.reference().digest()),
    );
    let maintenance = ProjectionSourceMaintenanceV1::initial(
        copying.descriptor.clone(),
        Some(selected_root.clone()),
    )
    .unwrap();
    let first = &copying.rows[0];
    let maintenance = maintenance
        .after_copy_step(&maintenance, &[(&first.0, &first.1)])
        .unwrap();
    let _lease = ports.shared.acquire_source_usage_fixture_lease().unwrap();
    let write = crate::changelog_v3_write::CapturedImmediateWrite::begin(
        &ports.shared.database,
        crate::RedbCommitProfile::Hardened,
        ChangelogAttributionV3::ProjectionControl,
    )
    .unwrap();
    put_object(&write, &selected, Life::Published, 130);
    put_object(&write, &superseded, Life::Superseded, 1);
    put_object(&write, &retiring, Life::Collecting, 1);
    put_object(&write, &copying, Life::Copying, 1);
    let mut root_key = Vec::with_capacity(source_root_key_len(
        selected.descriptor.reference().identity(),
    ));
    encode_source_root_key_into(
        &mut root_key,
        selected.descriptor.reference().identity(),
        selected.descriptor.reference().locator(),
    )
    .unwrap();
    let root_value = encode_projection_source_root_v1(&selected_root).unwrap();
    insert(
        &write,
        N::ProjectionSourceRoots,
        &root_key,
        root_value.as_bytes(),
    );
    let maintenance_value = encode_projection_source_maintenance_v1(&maintenance).unwrap();
    insert(
        &write,
        N::ProjectionSourceMaintenance,
        &source_maintenance_key(copying.descriptor.reference().locator()),
        maintenance_value.as_bytes(),
    );
    allocator(&write, 5);
    write.finish().unwrap().commit(&ports.shared).unwrap();
    drop(_lease);
    let mut census = SourceUsageCensus::start(&ports).unwrap();
    let mut pages = 0;
    let mut saw_full_page = false;
    loop {
        let page = census.step().unwrap();
        pages += 1;
        assert!(page.rows <= 128 && page.bytes <= 4 * 1024 * 1024);
        saw_full_page |= page.rows == 128;
        if page.done {
            break;
        }
        assert!(pages < 64);
    }
    assert!(
        saw_full_page && pages > 6,
        "real 130-row prefix crosses the compiled page boundary"
    );
    let completion = census.finish().unwrap();
    let usage = completion.usage();
    drop(completion);
    let physical = [
        N::ProjectionSourceRows,
        N::ProjectionSourceObjects,
        N::ProjectionSourceRoots,
        N::ProjectionSourceMaintenance,
    ]
    .into_iter()
    .map(|namespace| table_bytes(&ports, namespace))
    .sum::<u64>()
        + actual_allocator_bytes(&ports);
    let selected_object = encode_projection_source_object_v1(&ProjectionSourceObjectV1::new(
        selected.descriptor.clone(),
        Life::Published,
    ))
    .unwrap();
    let selected_bytes = selected.descriptor.canonical_row_bytes()
        + object_key(&selected.descriptor).len() as u64
        + selected_object.as_bytes().len() as u64
        + (root_key.len() + root_value.as_bytes().len()) as u64;
    assert_eq!(usage.selected_bytes(), selected_bytes);
    let uncopied = copying
        .rows
        .iter()
        .skip(1)
        .map(|(key, value)| (key.len() + value.len()) as u64)
        .sum::<u64>();
    assert!(
        usage.total_bytes() >= physical + uncopied,
        "unselected/retiring bytes and full uncopied reservation stay charged"
    );
    assert!(usage.total_bytes() > usage.selected_bytes());
    assert!(
        usage
            .candidate_reserved_bytes()
            .is_some_and(|bytes| bytes >= copying.descriptor.canonical_row_bytes())
    );
}

#[test]
fn actual_source_census_refuses_orphan_rows_without_releasing_previous_usage() {
    let (_directory, ports) = fresh(None);
    super::join_actual_native_command_prerequisite(&ports);
    super::super::native_source_command_fixture::append(&ports, 1).unwrap();
    let history = crate::changelog_v3_cursor::capture_history_position_v1(
        &ports.begin_composite_read().unwrap(),
    )
    .unwrap();
    let orphan = rows(history, 1, 1, None);
    let _lease = ports.shared.acquire_source_usage_fixture_lease().unwrap();
    let write = crate::changelog_v3_write::CapturedImmediateWrite::begin(
        &ports.shared.database,
        crate::RedbCommitProfile::Hardened,
        ChangelogAttributionV3::ProjectionControl,
    )
    .unwrap();
    insert(
        &write,
        N::ProjectionSourceRows,
        &orphan.rows[0].0,
        &orphan.rows[0].1,
    );
    allocator(&write, 2);
    write.finish().unwrap().commit(&ports.shared).unwrap();
    drop(_lease);
    let mut census = SourceUsageCensus::start(&ports).unwrap();
    let mut failed = false;
    for _ in 0..16 {
        match census.step() {
            Ok(page) => assert!(!page.done, "orphan must not mint completeness"),
            Err(_) => {
                failed = true;
                break;
            }
        }
    }
    assert!(failed);
    assert!(census.finish().is_err());
    assert!(
        ports
            .shared
            .database
            .source_usage_domain()
            .complete_observation()
            .is_err()
    );
    assert_eq!(
        table_bytes(&ports, N::ProjectionSourceRows),
        (orphan.rows[0].0.len() + orphan.rows[0].1.len()) as u64
    );
    assert_eq!(
        ports
            .shared
            .database
            .source_usage_domain()
            .retained_uncertain_bytes()
            .unwrap(),
        Some(riffdb_storage_api::MAX_PROJECTION_SOURCE_TABLE_BYTES)
    );
}
