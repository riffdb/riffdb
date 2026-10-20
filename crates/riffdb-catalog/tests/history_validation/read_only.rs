//! Read-only catalog evidence shares all existing semantic checks.
use super::*;
use riffdb_catalog::{
    ReadOnlyCatalogHistoryEvidenceV1, ReadOnlyCatalogHistoryOutcomeV1,
    validate_catalog_history_read_only_v1,
};

// This test wrapper implements only the read port; the production offline
// wrapper must additionally own its private actual immutable session brand.
struct Reader(FakeSession);
impl ReadOnlyCatalogHistoryEvidenceV1 for Reader {
    type HistoricalEnd = FakeHistoricalEnd;
    fn database_id(&self) -> DatabaseId {
        self.0.database_id
    }
    fn open_session_id(&self) -> OpenSessionId {
        self.0.open_session_id
    }
    fn read_historical_evidence(
        &mut self,
        cursor: HistoricalEvidenceCursor,
        limit: EvidencePageLimit,
    ) -> Result<HistoricalEvidencePage<Self::HistoricalEnd>, StorageError> {
        self.0.read_historical_evidence(cursor, limit)
    }
    fn read_historical_bundle(
        &mut self,
        lineage: &ContractLineage,
        version: ContractVersion,
        hash: ContractBundleHash,
    ) -> Result<Option<HistoricalBundleEvidence>, StorageError> {
        self.0.read_historical_bundle(lineage, version, hash)
    }
    fn read_integrity_entity(
        &mut self,
        target: &EntityTarget,
    ) -> Result<Option<StoredEntityRecordV1>, StorageError> {
        self.0.read_integrity_entity(target)
    }
    fn read_integrity_unique_occupancy(
        &mut self,
        target: &UniqueIndexTarget,
    ) -> Result<UniqueOccupancyKind, StorageError> {
        self.0.read_integrity_unique_occupancy(target)
    }
}

fn complete(fault: CursorFault) -> (FakeSession, ContractBundleHash) {
    let bundle =
        ValidatedContractBundle::from_compiler_bundle(compile_contract_source(BUDGET).unwrap())
            .unwrap();
    let stored = stored_bundle(&bundle);
    let command = bundle.bundle().commands().first().unwrap();
    let plan = riffdb_storage_api::ExecutablePlanRef::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
        command.command_id(),
        command.plan_hash(),
    );
    let history = session(
        vec![
            vec![HistoricalSemanticEvidence::Bundle(stored.clone())],
            vec![HistoricalSemanticEvidence::PlanReference(plan)],
            vec![active(&bundle)],
        ],
        vec![stored],
        fault,
    );
    (history, bundle.bundle_hash())
}

// req: PRJ-004, STO-012, REC-005
#[test]
fn read_only_catalog_entry_preserves_exact_ready_observation_and_backend_end() {
    let (mut normal, expected) = complete(CursorFault::None);
    let ordinary = validate_catalog_history(&mut normal).unwrap();
    let ordinary_count = ready_history(&ordinary).evidence_count();
    let (_, ordinary_end) = ordinary.into_parts();
    let (input, _) = complete(CursorFault::None);
    let mut readonly = Reader(input);
    let checked = validate_catalog_history_read_only_v1(&mut readonly).unwrap();
    let (outcome, end) = checked.into_parts();
    let ReadOnlyCatalogHistoryOutcomeV1::Ready(proof) = outcome else {
        panic!("complete current evidence must yield the distinct read-only proof");
    };
    assert!(proof.matches(database(), OpenSessionId::new(7).unwrap()));
    assert!(!proof.matches(database(), OpenSessionId::new(8).unwrap()));
    assert_eq!(proof.evidence_count(), ordinary_count);
    assert_eq!(proof.active().unwrap().bundle_hash(), expected);
    assert_eq!(end.cursor(), ordinary_end.cursor());
    assert_eq!(readonly.0.page, normal.page);
    assert_eq!(readonly.0.bundle_reads, normal.bundle_reads);
}

// req: PRJ-004, STO-012, REC-005
#[test]
fn read_only_catalog_entry_refuses_foreign_and_nonadvancing_evidence_like_legacy() {
    for fault in [
        CursorFault::Skip,
        CursorFault::WrongStart,
        CursorFault::WrongEnd,
        CursorFault::RawRepeatEmpty,
        CursorFault::RawNonAdvancing,
    ] {
        let (mut normal, _) = complete(fault);
        let ordinary = validate_catalog_history(&mut normal).err().unwrap().kind();
        let (input, _) = complete(fault);
        let mut readonly = Reader(input);
        assert_eq!(
            validate_catalog_history_read_only_v1(&mut readonly)
                .err()
                .unwrap()
                .kind(),
            ordinary
        );
        assert_eq!(ordinary, CatalogErrorKind::InvalidHistoricalEvidence);
    }
}

// req: PRJ-004, STO-012, REC-005
#[test]
fn read_only_catalog_entry_preserves_checked_migration_status_without_capability() {
    const INDEXED: &str = r#"
contract IndexedMigration version 1 {
  entity Row { key (id: u64) field name: string<8> index ByName(name) }
  aggregate Rows { root Row partition_by id conflict_key (id) }
}
"#;
    let bundle =
        ValidatedContractBundle::from_compiler_bundle(compile_contract_source(INDEXED).unwrap())
            .unwrap();
    let stored = stored_bundle(&bundle);
    let (key, _) = migration_index_key_and_partition(&bundle);
    let build = || {
        session(
            vec![
                vec![HistoricalSemanticEvidence::Bundle(stored.clone())],
                vec![active(&bundle)],
                vec![migration_v1_evidence(&bundle, key.clone())],
            ],
            vec![stored.clone()],
            CursorFault::None,
        )
    };
    let mut normal = build();
    let ordinary = validate_catalog_history(&mut normal).unwrap();
    assert!(matches!(
        ordinary.outcome(),
        CatalogHistoryOutcome::MigrationRequired(_)
    ));
    let mut readonly = Reader(build());
    let (outcome, end) = validate_catalog_history_read_only_v1(&mut readonly)
        .unwrap()
        .into_parts();
    let ReadOnlyCatalogHistoryOutcomeV1::MigrationRequired(observation) = outcome else {
        panic!("checked legacy indexes require migration, never a ready read-only proof");
    };
    assert!(observation.matches(database(), OpenSessionId::new(7).unwrap()));
    assert!(!observation.matches(database(), OpenSessionId::new(8).unwrap()));
    assert_eq!(observation.evidence_count(), 3);
    assert_eq!(end.cursor().position(), 3);
}
