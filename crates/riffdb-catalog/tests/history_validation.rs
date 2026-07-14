//! Exact-end same-session historical catalog validation.

use riffdb_catalog::{CatalogErrorKind, ValidatedContractBundle, validate_catalog_history};
use riffdb_contract_compiler::{compile_contract_source, compile_contract_successor};
use riffdb_storage_api::{
    DormantPortBundle, DurableKeySchemaBindingV1, EntityTarget, EvidencePageLimit,
    HistoricalActiveCatalogEvidence, HistoricalBundleBytes, HistoricalBundleEvidence,
    HistoricalEvidenceCursor, HistoricalEvidenceEnd, HistoricalEvidencePage,
    HistoricalPersistedKeyEvidenceV1, HistoricalSemanticEvidence, OpenSessionId, StorageError,
    StorageErrorKind, StoredEntityRecordV1, StoredIndexEpochV1, StructuralEvidenceCursor,
    StructuralEvidenceEnd, StructuralEvidencePage, StructuralEvidenceSession,
    StructurallyDecodedIndexRangePrefixV1, StructurallyOpened,
};
use riffdb_types::{
    CanonicalRecord, CanonicalValue, ContractBundleHash, ContractLineage, ContractVersion,
    DatabaseId, EntityKey, EntityVersion, IndexEpoch, PlanHash,
};

const BUDGET: &str = include_str!("../../../contracts/examples/budget.riff");

#[derive(Clone, Copy)]
enum CursorFault {
    None,
    Skip,
    WrongStart,
    WrongEnd,
    RawRepeatEmpty,
    RawNonAdvancing,
    RawOversized,
}

struct FakeHistoricalEnd(HistoricalEvidenceCursor);

impl HistoricalEvidenceEnd for FakeHistoricalEnd {
    fn cursor(&self) -> HistoricalEvidenceCursor {
        self.0
    }
}

struct FakeStructuralEnd(StructuralEvidenceCursor);

impl StructuralEvidenceEnd for FakeStructuralEnd {
    fn cursor(&self) -> StructuralEvidenceCursor {
        self.0
    }
}

struct FakeDormantPorts;

impl DormantPortBundle for FakeDormantPorts {
    type CompletionAuthority = ();
}

struct FakeSession {
    database_id: DatabaseId,
    open_session_id: OpenSessionId,
    pages: Vec<Vec<HistoricalSemanticEvidence>>,
    page: usize,
    bundles: Vec<HistoricalBundleEvidence>,
    fault: CursorFault,
}

impl StructuralEvidenceSession for FakeSession {
    type DormantPorts = FakeDormantPorts;
    type StructuralEnd = FakeStructuralEnd;
    type HistoricalEnd = FakeHistoricalEnd;

    fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    fn open_session_id(&self) -> OpenSessionId {
        self.open_session_id
    }

    fn read_structural_evidence(
        &mut self,
        _cursor: StructuralEvidenceCursor,
        _limit: EvidencePageLimit,
    ) -> Result<StructuralEvidencePage<Self::StructuralEnd>, StorageError> {
        Err(storage_error(StorageErrorKind::InvariantViolation))
    }

    fn read_historical_evidence(
        &mut self,
        cursor: HistoricalEvidenceCursor,
        limit: EvidencePageLimit,
    ) -> Result<HistoricalEvidencePage<Self::HistoricalEnd>, StorageError> {
        if matches!(self.fault, CursorFault::RawRepeatEmpty) {
            assert_eq!(self.page, 0, "validator reread a non-advancing page");
            self.page += 1;
            return Ok(HistoricalEvidencePage::Page {
                start: cursor,
                evidence: Vec::new(),
                next: cursor,
            });
        }

        let Some(evidence) = self.pages.get(self.page).cloned() else {
            let end = if matches!(self.fault, CursorFault::WrongEnd) {
                cursor
                    .advanced(1)
                    .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?
            } else {
                cursor
            };
            return Ok(HistoricalEvidencePage::ExactEnd(FakeHistoricalEnd(end)));
        };
        self.page += 1;

        let amount = u64::try_from(evidence.len())
            .map_err(|_| storage_error(StorageErrorKind::LimitExceeded))?;
        let start = if self.page == 1 && matches!(self.fault, CursorFault::WrongStart) {
            HistoricalEvidenceCursor::start(
                self.database_id,
                OpenSessionId::new(999).expect("session"),
            )
        } else {
            cursor
        };
        let advance = if self.page == 1 && matches!(self.fault, CursorFault::Skip) {
            amount + 1
        } else {
            amount
        };
        let next = start
            .advanced(advance)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))?;
        if matches!(self.fault, CursorFault::RawNonAdvancing) {
            return Ok(HistoricalEvidencePage::Page {
                start,
                evidence,
                next: start,
            });
        }
        if matches!(self.fault, CursorFault::RawOversized) && evidence.len() > limit.get() as usize
        {
            return Ok(HistoricalEvidencePage::Page {
                start,
                evidence,
                next,
            });
        }
        HistoricalEvidencePage::page(start, evidence, next)
            .map_err(|_| storage_error(StorageErrorKind::InvariantViolation))
    }

    fn read_historical_bundle(
        &mut self,
        lineage: &ContractLineage,
        contract_version: ContractVersion,
        bundle_hash: ContractBundleHash,
    ) -> Result<Option<HistoricalBundleEvidence>, StorageError> {
        Ok(self
            .bundles
            .iter()
            .find(|bundle| {
                bundle.lineage() == lineage
                    && bundle.version() == contract_version
                    && bundle.bundle_hash() == bundle_hash
            })
            .cloned())
    }

    fn finish(
        self,
        _structural_end: Self::StructuralEnd,
        _historical_end: Self::HistoricalEnd,
    ) -> Result<StructurallyOpened<Self::DormantPorts>, StorageError> {
        Err(storage_error(StorageErrorKind::InvariantViolation))
    }
}

fn database() -> DatabaseId {
    DatabaseId::from_bytes([0x01, 0x8f, 0, 0, 0, 0, 0x70, 1, 0x80, 2, 0, 0, 0, 0, 0, 1])
        .expect("database")
}

fn stored_bundle(bundle: &ValidatedContractBundle) -> HistoricalBundleEvidence {
    HistoricalBundleEvidence::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
        HistoricalBundleBytes::new(bundle.bundle().canonical_bytes().to_vec()).expect("bytes"),
    )
}

fn active(bundle: &ValidatedContractBundle) -> HistoricalSemanticEvidence {
    HistoricalSemanticEvidence::ActiveCatalog(Some(HistoricalActiveCatalogEvidence::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
    )))
}

fn session(
    pages: Vec<Vec<HistoricalSemanticEvidence>>,
    bundles: Vec<HistoricalBundleEvidence>,
    fault: CursorFault,
) -> FakeSession {
    FakeSession {
        database_id: database(),
        open_session_id: OpenSessionId::new(7).expect("session"),
        pages,
        page: 0,
        bundles,
        fault,
    }
}

fn storage_error(kind: StorageErrorKind) -> StorageError {
    StorageError::new(kind, None)
}

#[test]
fn complete_history_resolves_every_plan_and_produces_a_session_bound_proof() {
    let bundle = ValidatedContractBundle::from_compiler_bundle(
        compile_contract_source(BUDGET).expect("budget"),
    )
    .expect("validated");
    let stored = stored_bundle(&bundle);
    let command = bundle.bundle().commands().first().expect("command");
    let plan = riffdb_storage_api::ExecutablePlanRef::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
        command.command_id(),
        command.plan_hash(),
    );
    let mut session = session(
        vec![
            vec![HistoricalSemanticEvidence::Bundle(stored.clone())],
            vec![HistoricalSemanticEvidence::PlanReference(plan)],
            vec![active(&bundle)],
        ],
        vec![stored],
        CursorFault::None,
    );

    let validation = validate_catalog_history(&mut session).expect("complete history");
    assert!(
        validation
            .history()
            .matches(database(), OpenSessionId::new(7).expect("session"))
    );
    assert_eq!(validation.history().evidence_count(), 3);
    assert_eq!(
        validation.history().active().expect("active").bundle_hash(),
        bundle.bundle_hash()
    );
}

#[test]
fn initialized_but_undeployed_catalog_has_an_exact_empty_history() {
    let mut session = session(
        vec![vec![HistoricalSemanticEvidence::ActiveCatalog(None)]],
        Vec::new(),
        CursorFault::None,
    );

    let validation = validate_catalog_history(&mut session).expect("empty exact history");
    assert!(validation.history().active().is_none());
    assert_eq!(validation.history().evidence_count(), 1);
}

#[test]
fn skipped_repeated_reordered_cross_session_and_truncated_streams_fail_closed() {
    let bundle = ValidatedContractBundle::from_compiler_bundle(
        compile_contract_source(BUDGET).expect("budget"),
    )
    .expect("validated");
    let stored = stored_bundle(&bundle);
    let bundle_item = HistoricalSemanticEvidence::Bundle(stored.clone());
    let active_item = active(&bundle);

    let cases = [
        session(
            vec![vec![bundle_item.clone()], vec![active_item.clone()]],
            vec![stored.clone()],
            CursorFault::Skip,
        ),
        session(
            vec![
                vec![bundle_item.clone()],
                vec![bundle_item.clone()],
                vec![active_item.clone()],
            ],
            vec![stored.clone()],
            CursorFault::None,
        ),
        session(
            vec![vec![active_item.clone()], vec![bundle_item.clone()]],
            vec![stored.clone()],
            CursorFault::None,
        ),
        session(
            vec![vec![bundle_item.clone()], vec![active_item.clone()]],
            vec![stored.clone()],
            CursorFault::WrongStart,
        ),
        session(
            vec![vec![bundle_item.clone()], vec![active_item.clone()]],
            vec![stored.clone()],
            CursorFault::WrongEnd,
        ),
        session(vec![vec![bundle_item]], vec![stored], CursorFault::None),
    ];

    for mut invalid in cases {
        let error = validate_catalog_history(&mut invalid)
            .err()
            .expect("invalid sequence");
        assert_eq!(error.kind(), CatalogErrorKind::InvalidHistoricalEvidence);
    }
}

#[test]
fn raw_empty_nonadvancing_and_oversized_pages_fail_closed() {
    let mut repeat_empty = session(Vec::new(), Vec::new(), CursorFault::RawRepeatEmpty);
    assert_eq!(
        validate_catalog_history(&mut repeat_empty)
            .err()
            .expect("empty non-advancing page")
            .kind(),
        CatalogErrorKind::InvalidHistoricalEvidence
    );
    assert_eq!(repeat_empty.page, 1, "invalid page must not be reread");

    let mut nonadvancing = session(
        vec![vec![HistoricalSemanticEvidence::ActiveCatalog(None)]],
        Vec::new(),
        CursorFault::RawNonAdvancing,
    );
    assert_eq!(
        validate_catalog_history(&mut nonadvancing)
            .err()
            .expect("non-advancing page")
            .kind(),
        CatalogErrorKind::InvalidHistoricalEvidence
    );

    const INDEXED: &str = r#"
contract Indexed version 1 {
  entity Row {
    key (id: u64)
    field name: string<8>
    index ByName(name)
  }
  aggregate Rows {
    root Row
    partition_by id
    conflict_key (id)
  }
}
"#;
    let bundle = ValidatedContractBundle::from_compiler_bundle(
        compile_contract_source(INDEXED).expect("indexed contract"),
    )
    .expect("validated");
    let stored = stored_bundle(&bundle);
    let entity = bundle.bundle().schema().entities().first().expect("entity");
    let oversized = (0..=500)
        .map(|id| {
            let key = entity
                .primary_key()
                .encode_entity(&[CanonicalValue::U64(id)])
                .expect("entity key");
            HistoricalSemanticEvidence::PersistedKey(entity_key_evidence(&bundle, key))
        })
        .collect();
    let mut oversized_page = session(
        vec![
            vec![HistoricalSemanticEvidence::Bundle(stored.clone())],
            vec![active(&bundle)],
            oversized,
        ],
        vec![stored],
        CursorFault::RawOversized,
    );
    assert_eq!(
        validate_catalog_history(&mut oversized_page)
            .err()
            .expect("oversized raw page")
            .kind(),
        CatalogErrorKind::InvalidHistoricalEvidence
    );
}

#[test]
fn unknown_or_hash_mismatched_plan_fails_without_active_substitution() {
    let bundle = ValidatedContractBundle::from_compiler_bundle(
        compile_contract_source(BUDGET).expect("budget"),
    )
    .expect("validated");
    let stored = stored_bundle(&bundle);
    let command = bundle.bundle().commands().first().expect("command");
    let wrong = riffdb_storage_api::ExecutablePlanRef::new(
        bundle.lineage().clone(),
        bundle.contract_version(),
        bundle.bundle_hash(),
        command.command_id(),
        PlanHash::from_bytes([0x88; 32]),
    );
    let mut session = session(
        vec![
            vec![HistoricalSemanticEvidence::Bundle(stored.clone())],
            vec![HistoricalSemanticEvidence::PlanReference(wrong)],
            vec![active(&bundle)],
        ],
        vec![stored],
        CursorFault::None,
    );
    assert_eq!(
        validate_catalog_history(&mut session)
            .err()
            .expect("unknown plan")
            .kind(),
        CatalogErrorKind::UnknownExecutablePlan
    );
}

#[test]
fn branched_bundle_history_and_nonterminal_active_pointer_fail_closed() {
    let genesis_compiled = compile_contract_source(BUDGET).expect("genesis");
    let genesis =
        ValidatedContractBundle::from_compiler_bundle(genesis_compiled).expect("validated genesis");
    let second_compiled =
        compile_contract_successor(&version(BUDGET, 2), genesis.bundle()).expect("second version");
    let second =
        ValidatedContractBundle::from_compiler_bundle(second_compiled).expect("validated second");
    let branched_third_compiled = compile_contract_successor(&version(BUDGET, 3), genesis.bundle())
        .expect("third version branched from genesis");
    let branched_third = ValidatedContractBundle::from_compiler_bundle(branched_third_compiled)
        .expect("validated branch");

    let genesis_stored = stored_bundle(&genesis);
    let second_stored = stored_bundle(&second);
    let third_stored = stored_bundle(&branched_third);
    let mut branch = session(
        vec![
            vec![HistoricalSemanticEvidence::Bundle(genesis_stored.clone())],
            vec![HistoricalSemanticEvidence::Bundle(second_stored.clone())],
            vec![HistoricalSemanticEvidence::Bundle(third_stored.clone())],
            vec![active(&branched_third)],
        ],
        vec![genesis_stored.clone(), second_stored.clone(), third_stored],
        CursorFault::None,
    );
    assert_eq!(
        validate_catalog_history(&mut branch)
            .err()
            .expect("branched activation history")
            .kind(),
        CatalogErrorKind::InvalidHistoricalEvidence
    );

    let mut rollback = session(
        vec![
            vec![HistoricalSemanticEvidence::Bundle(genesis_stored.clone())],
            vec![HistoricalSemanticEvidence::Bundle(second_stored.clone())],
            vec![active(&genesis)],
        ],
        vec![genesis_stored, second_stored],
        CursorFault::None,
    );
    assert_eq!(
        validate_catalog_history(&mut rollback)
            .err()
            .expect("active must name terminal bundle")
            .kind(),
        CatalogErrorKind::InvalidHistoricalEvidence
    );
}

#[test]
fn retained_entity_key_schema_accepts_complete_bytes_and_rejects_truncation() {
    const INDEXED: &str = r#"
contract Indexed version 1 {
  entity Row {
    key (id: u64)
    field name: string<8>
    index ByName(name)
  }
  aggregate Rows {
    root Row
    partition_by id
    conflict_key (id)
  }
}
"#;
    let bundle = ValidatedContractBundle::from_compiler_bundle(
        compile_contract_source(INDEXED).expect("indexed contract"),
    )
    .expect("validated");
    let stored = stored_bundle(&bundle);
    let entity = bundle.bundle().schema().entities().first().expect("entity");
    let complete = entity
        .primary_key()
        .encode_entity(&[CanonicalValue::U64(42)])
        .expect("entity key");
    let valid = entity_key_evidence(&bundle, complete.clone());

    let mut truncated = complete.as_bytes().to_vec();
    truncated.pop();
    let truncated = EntityKey::from_bytes(truncated).expect("structural envelope remains valid");
    let invalid = entity_key_evidence(&bundle, truncated);

    let mut valid_session = session(
        vec![
            vec![HistoricalSemanticEvidence::Bundle(stored.clone())],
            vec![active(&bundle)],
            vec![HistoricalSemanticEvidence::PersistedKey(valid)],
        ],
        vec![stored.clone()],
        CursorFault::None,
    );
    assert!(validate_catalog_history(&mut valid_session).is_ok());

    let mut invalid_session = session(
        vec![
            vec![HistoricalSemanticEvidence::Bundle(stored.clone())],
            vec![active(&bundle)],
            vec![HistoricalSemanticEvidence::PersistedKey(invalid)],
        ],
        vec![stored],
        CursorFault::None,
    );
    assert_eq!(
        validate_catalog_history(&mut invalid_session)
            .err()
            .expect("incomplete component")
            .kind(),
        CatalogErrorKind::InvalidHistoricalKey
    );
}

#[test]
fn retained_index_prefix_schema_rejects_a_partial_variable_component() {
    const INDEXED: &str = r#"
contract Indexed version 1 {
  entity Row {
    key (id: u64)
    field name: string<8>
    index ByName(name)
  }
  aggregate Rows {
    root Row
    partition_by id
    conflict_key (id)
  }
}
"#;
    let bundle = ValidatedContractBundle::from_compiler_bundle(
        compile_contract_source(INDEXED).expect("indexed contract"),
    )
    .expect("validated");
    let stored = stored_bundle(&bundle);
    let index = bundle.bundle().schema().entities()[0].indexes()[0].key_schema();
    let complete = index
        .encode_index_prefix(&[CanonicalValue::string("abc").expect("value")])
        .expect("prefix");
    let valid = index_prefix_evidence(&bundle, complete.as_bytes().to_vec());
    let mut partial = complete.as_bytes().to_vec();
    partial.pop();
    let invalid = index_prefix_evidence(&bundle, partial);

    for (evidence, succeeds) in [(valid, true), (invalid, false)] {
        let mut candidate = session(
            vec![
                vec![HistoricalSemanticEvidence::Bundle(stored.clone())],
                vec![active(&bundle)],
                vec![HistoricalSemanticEvidence::PersistedKey(evidence)],
            ],
            vec![stored.clone()],
            CursorFault::None,
        );
        let result = validate_catalog_history(&mut candidate);
        assert_eq!(result.is_ok(), succeeds);
        if !succeeds {
            assert_eq!(
                result.err().expect("partial prefix").kind(),
                CatalogErrorKind::InvalidHistoricalKey
            );
        }
    }
}

fn entity_key_evidence(
    bundle: &ValidatedContractBundle,
    key: EntityKey,
) -> HistoricalPersistedKeyEvidenceV1 {
    let entity = bundle.bundle().schema().entities().first().expect("entity");
    let record = StoredEntityRecordV1::new(
        EntityTarget::new(entity.id(), key).expect("target"),
        EntityVersion::first(),
        bundle.contract_version(),
        DurableKeySchemaBindingV1::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
        ),
        CanonicalRecord::new(Vec::new()).expect("record"),
    )
    .expect("stored row");
    HistoricalPersistedKeyEvidenceV1::from_entity(&record)
}

fn index_prefix_evidence(
    bundle: &ValidatedContractBundle,
    bytes: Vec<u8>,
) -> HistoricalPersistedKeyEvidenceV1 {
    let index = bundle.bundle().schema().entities()[0].indexes()[0].id();
    let prefix = StructurallyDecodedIndexRangePrefixV1::new(index, bytes)
        .expect("structural prefix envelope");
    let record = StoredIndexEpochV1::new(
        prefix,
        DurableKeySchemaBindingV1::new(
            bundle.lineage().clone(),
            bundle.contract_version(),
            bundle.bundle_hash(),
        ),
        IndexEpoch::first(),
    );
    HistoricalPersistedKeyEvidenceV1::from_index_epoch(&record)
}

fn version(source: &str, number: u64) -> String {
    source.replacen("version 1", &format!("version {number}"), 1)
}
