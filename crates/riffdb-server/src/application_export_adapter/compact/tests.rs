use super::*;

fn state() -> ExportState {
    super::super::tests::state()
}

fn advance(
    state: &mut ExportState,
    class: ApplicationExportClassV1,
    empty: bool,
) -> ApplicationExportPageHash {
    let rows = if empty { 0 } else { 1 };
    let bytes = rows * 64;
    let index = usize::from(class.tag() - 1);
    state.pages_released += 1;
    state.rows_released += rows;
    state.bytes_released += bytes;
    state.class_pages[index] += 1;
    state.class_rows[index] += rows;
    state.class_bytes[index] += bytes;
    state.phase = ApplicationExportPhaseV1::Exporting;
    let page_hash = ApplicationExportPageHash::from_bytes([state.pages_released as u8; 32]);
    append(state, class, page_hash, rows, bytes).unwrap();
    page_hash
}

// req: EXP-002, EXP-006, EXP-007, EXP-008, EXP-009, EXP-010
#[test]
fn compact_terminal_documents_preserve_legacy_bytes_for_every_class_and_failure() {
    for scope in [
        CapabilityApplicationExportScopeV1::WholeApplication,
        CapabilityApplicationExportScopeV1::PrincipalFiltered,
    ] {
        for portable in [false, true] {
            let mut legacy = state();
            legacy.selection = ApplicationExportSelectionV1::new(
                legacy.selection.lineage().clone(),
                scope,
                true,
                true,
                true,
                true,
            )
            .unwrap();
            if scope == CapabilityApplicationExportScopeV1::PrincipalFiltered {
                legacy.row_policy_role_hash = Some(ApplicationRoleHash::from_bytes([0x29; 32]));
                legacy.row_policy_names = vec!["visible".to_owned()];
            }
            if portable {
                legacy.portability_manifest_hash =
                    Some(ApplicationPortabilityManifestHash::from_bytes([0x37; 32]));
                legacy.workflow_quiescence = vec![WorkflowQuiescenceWireV1 {
                    workflow: "work".to_owned(),
                    entity_type_id: 1,
                    owner_field_id: 2,
                    expiry_field_id: 3,
                    checked_rows: 0,
                    quiescent_rows: 0,
                    non_quiescent_rows: 0,
                }];
            }
            let mut compact = legacy.clone();
            initialize(&mut compact).unwrap();
            let mut hashes = Vec::new();
            for class in [
                ApplicationExportClassV1::Entity,
                ApplicationExportClassV1::Event,
                ApplicationExportClassV1::Provenance,
                ApplicationExportClassV1::PublicAudit,
            ] {
                for empty in [true, false] {
                    let expected = advance(&mut legacy, class, empty);
                    assert_eq!(advance(&mut compact, class, empty), expected);
                    hashes.push(expected);
                    let head = stored(&compact).unwrap();
                    let decoded = decode_state(&head).unwrap();
                    assert_eq!(stored(&decoded).unwrap(), head);
                    assert!(decoded.page_hashes.is_empty());
                }
            }
            // Terminalization receives this list only from the independently
            // verified storage ledger; normal compact progress never retains it.
            compact.page_hashes = hashes;
            let reserve = terminal_reserve(&compact).unwrap();
            let before = replay_identity(&stored(&compact).unwrap()).unwrap().len();
            for failure in
                std::iter::once(None).chain((1..=8).map(|tag| failure_from_tag(tag).unwrap()))
            {
                let mut old = legacy.clone();
                let mut new = compact.clone();
                if let Some(failure) = failure {
                    fail_state(&mut old, failure).unwrap();
                    fail_state(&mut new, failure).unwrap();
                } else {
                    complete_state(&mut old).unwrap();
                    complete_state(&mut new).unwrap();
                }
                assert_eq!(new.manifest, old.manifest);
                assert_eq!(new.receipt, old.receipt);
                let head = stored(&new).unwrap();
                let after = replay_identity(&head).unwrap().len();
                assert!(after <= before + reserve, "terminal headroom undercharged");
                let decoded = decode_state(&head).unwrap();
                assert_eq!(decoded.manifest, old.manifest);
                assert_eq!(decoded.receipt, old.receipt);
            }
        }
    }
}

// req: EXP-006, EXP-007, EXP-008, EXP-009
#[test]
fn compact_progress_cost_and_terminal_headroom_are_bounded_at_real_page_counts() {
    let mut state = state();
    initialize(&mut state).unwrap();
    let mut hashes = Vec::new();
    let mut head_sizes = Vec::new();
    let mut last_fitting = None;
    for ordinal in 1..=4096 {
        hashes.push(advance(&mut state, ApplicationExportClassV1::Entity, false));
        let head = stored(&state).unwrap();
        let reserve = terminal_reserve(&state).unwrap();
        if check_budget(&state, &head, reserve).is_err() {
            break;
        }
        if [16, 128, 1024].contains(&ordinal) {
            head_sizes.push(replay_identity(&head).unwrap().len());
        }
        last_fitting = Some((state.clone(), hashes.clone(), reserve));
    }
    assert_eq!(
        head_sizes.len(),
        3,
        "1,024 fitting pages exercise the real codecs"
    );
    assert!(
        head_sizes[2] - head_sizes[0] < 100,
        "heads grow only by decimal counter width"
    );
    let (mut last, hashes, reserve) = last_fitting.unwrap();
    assert!(last.pages_released >= 1024 && last.pages_released < 4096);
    let original = replay_identity(&stored(&last).unwrap()).unwrap().len();
    last.page_hashes = hashes;
    for tag in 1..=8 {
        let mut terminal = last.clone();
        fail_state(&mut terminal, failure_from_tag(tag).unwrap().unwrap()).unwrap();
        let head = stored(&terminal).unwrap();
        check_budget(&terminal, &head, 0).unwrap();
        assert!(replay_identity(&head).unwrap().len() <= original + reserve);
    }
}

// req: EXP-006, EXP-007, EXP-010
#[test]
fn compact_cursor_and_replay_bind_prefix_and_immutable_authority() {
    let mut state = state();
    let legacy = stored_state(&state).unwrap();
    assert_eq!(cursor_for(&legacy).unwrap().as_bytes()[0], 1);
    initialize(&mut state).unwrap();
    advance(&mut state, ApplicationExportClassV1::Entity, false);
    let head = stored(&state).unwrap();
    let cursor = cursor_for(&head).unwrap();
    assert_eq!(cursor.as_bytes()[0], 2);
    let StoredApplicationExportOperation::Compact(ref original) = head else {
        panic!("compact");
    };
    let mut bytes = original.prefix().canonical_bytes().unwrap();
    bytes[53] ^= 1;
    let changed = StoredApplicationExportOperation::Compact(
        StoredApplicationExportOperationV2::new(
            original.operation_id(),
            original.lineage().clone(),
            original.immutable_binding().to_vec(),
            original.canonical_state().to_vec(),
            ApplicationExportLedgerPrefixV1::from_canonical_bytes(&bytes).unwrap(),
        )
        .unwrap(),
    );
    assert_ne!(cursor_for(&changed).unwrap(), cursor);
    assert_ne!(
        replay_identity(&changed).unwrap(),
        replay_identity(&head).unwrap()
    );
    let mut body: CompactWire = serde_json::from_slice(original.canonical_state()).unwrap();
    body.state.capability_revision += 1;
    let forged = StoredApplicationExportOperationV2::new(
        original.operation_id(),
        original.lineage().clone(),
        original.immutable_binding().to_vec(),
        serde_json::to_vec(&body).unwrap(),
        *original.prefix(),
    )
    .unwrap();
    assert!(decode(&forged).is_err());
}

// req: EXP-006, EXP-007, EXP-009
#[test]
fn compact_unknown_outcome_reconciles_exact_prefix_without_another_write() {
    struct Unknown {
        head: Option<StoredApplicationExportOperation>,
        writes: usize,
        checks: std::cell::Cell<usize>,
        corrupt: bool,
    }
    impl ApplicationExportLedgerRepository for Unknown {
        fn read_application_export_head(
            &self,
            _: ApplicationExportOperationId,
        ) -> Result<Option<StoredApplicationExportOperation>, StorageError> {
            Ok(self.head.clone())
        }
        fn compare_and_swap_application_export_head(
            &mut self,
            _: Option<&StoredApplicationExportOperation>,
            _: &StoredApplicationExportOperation,
            _: Option<&ApplicationExportPageCommitmentV1>,
            _: usize,
        ) -> Result<ApplicationExportOperationWriteResultV1, StorageError> {
            self.writes += 1;
            Err(StorageError::new(
                StorageErrorKind::CommitStatusUnknown,
                None,
            ))
        }
        fn verify_application_export_ledger(
            &self,
            _: &StoredApplicationExportOperationV2,
        ) -> Result<Vec<ApplicationExportPageHash>, StorageError> {
            self.checks.set(self.checks.get() + 1);
            if self.corrupt {
                Err(StorageError::new(StorageErrorKind::CorruptData, None))
            } else {
                Ok(Vec::new())
            }
        }
        fn list_application_export_heads(
            &self,
            _: usize,
        ) -> Result<Vec<StoredApplicationExportOperation>, StorageError> {
            unreachable!("recovery never inventories other operations")
        }
    }
    let mut state = state();
    initialize(&mut state).unwrap();
    let head = stored(&state).unwrap();
    let mut repository = Unknown {
        head: Some(head.clone()),
        writes: 0,
        checks: std::cell::Cell::new(0),
        corrupt: false,
    };
    assert_eq!(
        commit(&mut repository, None, &head, None, 0).unwrap(),
        ApplicationExportOperationWriteResultV1::Unchanged
    );
    assert_eq!((repository.writes, repository.checks.get()), (1, 1));
    repository.corrupt = true;
    assert_eq!(
        commit(&mut repository, None, &head, None, 0)
            .unwrap_err()
            .kind(),
        StorageErrorKind::CorruptData
    );
    repository.head = None;
    assert_eq!(
        commit(&mut repository, None, &head, None, 0)
            .unwrap_err()
            .kind(),
        StorageErrorKind::CommitStatusUnknown
    );
    assert_eq!((repository.writes, repository.checks.get()), (3, 2));
}
