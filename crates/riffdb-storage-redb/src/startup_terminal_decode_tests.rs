use super::*;

// req: REC-006, REC-007
#[test]
fn terminal_census_and_semantic_inspection_decode_once() {
    let path = TestDatabasePath::new("terminal-decode-once");
    let id = database_id(0xd6);
    let (store, bundle) = checkpointable_history_store(&path, id, 4);
    let mut session = store
        .begin_structural_evidence_for(inputs(), EvidenceOpenPurpose::OfflineIntegrityScrub)
        .unwrap();
    session.ensure_command_cache().unwrap();
    let outcome = checkpoint_count_outcome(&bundle, id, 0, CommitSequence::first());
    let outcome_key = outcome
        .identity()
        .storage_key()
        .unwrap()
        .as_bytes()
        .to_vec();
    let outcome_bytes = codec::encode_stored_outcome_v1(&outcome)
        .unwrap()
        .as_bytes()
        .to_vec();
    let failure = checkpoint_count_failure(&bundle, id, 0x7200);
    let failure_key = failure
        .pending()
        .identity()
        .storage_key()
        .unwrap()
        .as_bytes()
        .to_vec();
    let failure_bytes = codec::encode_execution_failed_v1(&failure)
        .unwrap()
        .as_bytes()
        .to_vec();
    let locator = codec::encode_command_locator_v1(
        riffdb_storage_api::StoredCommandLocatorV1::new(CommitSequence::first()),
    )
    .unwrap()
    .as_bytes()
    .to_vec();
    let cases = [
        (
            outcome_key.clone(),
            outcome_bytes.clone(),
            false,
            Some(authoritative(StructuralFindingCode::CrossLinkMismatch)),
        ),
        (failure_key, failure_bytes.clone(), true, None),
        (
            outcome_key.clone(),
            locator,
            false,
            Some(authoritative(StructuralFindingCode::MissingCrossLink)),
        ),
        (
            outcome_key,
            vec![0xff],
            false,
            Some(authoritative(StructuralFindingCode::MalformedRecord)),
        ),
        (
            vec![0xff],
            outcome_bytes,
            false,
            Some(authoritative(StructuralFindingCode::MalformedRecord)),
        ),
        (
            vec![0xff],
            failure_bytes,
            true,
            Some(authoritative(StructuralFindingCode::MalformedRecord)),
        ),
    ];
    for (key, value, failed, expected) in cases {
        let before_decodes = codec::IDEMPOTENCY_RECORD_DECODES.with(std::cell::Cell::get);
        let before_census = session.terminal_execution_failure_rows;
        let finding = session
            .inspect_terminal_row_with_census(&key, &value)
            .unwrap();
        let decodes = codec::IDEMPOTENCY_RECORD_DECODES.with(std::cell::Cell::get) - before_decodes;
        assert_eq!(
            finding, expected,
            "healthy failure, command linkage and physical-key refusal retain their semantics"
        );
        assert_eq!(
            session.terminal_execution_failure_rows,
            before_census + u64::from(failed),
            "census includes failures even when their keys are invalid"
        );
        assert_eq!(
            decodes, 1,
            "census and semantic inspection must share the one decoded terminal record"
        );
    }
}
