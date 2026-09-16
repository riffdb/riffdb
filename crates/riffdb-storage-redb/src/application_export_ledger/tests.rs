use super::*;
use crate::store::{RedbDormantPorts, RedbStore};
use riffdb_storage_api::{
    ApplicationExportLedgerPrefixV1, ApplicationExportPageOrdinalV1, DatabaseInitializationPort,
    OwnedSnapshotReader,
};
use riffdb_types::{ApplicationExportClassV1, ContractLineage, DatabaseId};

// req: EXP-006, EXP-007, EXP-009, REC-001
#[test]
fn compact_export_crash_and_unknown_outcome_keep_complete_head_page_pairs() {
    const MODE: &str = "RIFFDB_EXPORT_LEDGER_CRASH_MODE";
    const PATH: &str = "RIFFDB_EXPORT_LEDGER_CRASH_PATH";
    const TEST: &str = "application_export_ledger::tests::compact_export_crash_and_unknown_outcome_keep_complete_head_page_pairs";
    if let Ok(mode) = std::env::var(MODE) {
        let path = std::path::PathBuf::from(std::env::var_os(PATH).unwrap());
        let controller = match mode.as_str() {
            "before" => crate::hooks::RedbTestController::abort_before_commit(
                RedbTestOperation::ApplicationExportOperation,
            ),
            "after" => crate::hooks::RedbTestController::abort_after_commit(
                RedbTestOperation::ApplicationExportOperation,
            ),
            "unknown" => crate::hooks::RedbTestController::return_unknown_after_commit(
                RedbTestOperation::ApplicationExportOperation,
            ),
            _ => panic!("closed fixture mode"),
        };
        let store = RedbStore::open_with_test_controller(&path, controller).unwrap();
        let mut ports = RedbDormantPorts {
            pending_v3_activation: None,
            shared: store.shared,
        }
        .into_operational_after_catalog_validation()
        .unwrap();
        let initial = initial(0x61);
        let (next, page) = successor(&initial);
        let expected = Head::Compact(initial);
        let replacement = Head::Compact(next.clone());
        let result = ports.compare_and_swap_application_export_head(
            Some(&expected),
            &replacement,
            Some(&page),
            2048,
        );
        assert_eq!(mode, "unknown", "crash hooks must abort before returning");
        assert_eq!(
            result.unwrap_err().kind(),
            StorageErrorKind::CommitStatusUnknown
        );
        assert_eq!(
            ports
                .read_application_export_head(next.operation_id())
                .unwrap(),
            Some(replacement)
        );
        assert_eq!(
            ports.verify_application_export_ledger(&next).unwrap(),
            vec![page.page_hash()]
        );
        return;
    }
    for mode in ["before", "after", "unknown"] {
        let directory = crate::test_path::ScopedDirectory::new("export-ledger-crash");
        let path = directory.join("db.redb");
        let mut ports = open(&path);
        let initial = initial(0x61);
        let expected = Head::Compact(initial.clone());
        ports
            .compare_and_swap_application_export_head(None, &expected, None, 2048)
            .unwrap();
        drop(ports);
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", TEST])
            .env(MODE, mode)
            .env(PATH, &path)
            .stdin(std::process::Stdio::null())
            .output()
            .unwrap();
        if mode == "unknown" {
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
        } else {
            assert_eq!(
                output.status.code(),
                None,
                "abort at {mode}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        let mut ports = open(&path);
        let (next, page) = successor(&initial);
        let replacement = Head::Compact(next.clone());
        let retained = if mode == "before" {
            &expected
        } else {
            &replacement
        };
        assert_eq!(
            ports
                .read_application_export_head(initial.operation_id())
                .unwrap()
                .as_ref(),
            Some(retained)
        );
        if mode == "before" {
            assert!(
                ports
                    .verify_application_export_ledger(&initial)
                    .unwrap()
                    .is_empty()
            );
        } else {
            assert_eq!(
                ports.verify_application_export_ledger(&next).unwrap(),
                vec![page.page_hash()]
            );
        }
        assert_eq!(
            ports
                .compare_and_swap_application_export_head(
                    Some(&expected),
                    &replacement,
                    Some(&page),
                    2048
                )
                .unwrap(),
            if mode == "before" {
                WriteResult::Applied
            } else {
                WriteResult::Unchanged
            }
        );
        assert_eq!(
            ports.verify_application_export_ledger(&next).unwrap(),
            vec![page.page_hash()]
        );
    }
}

// req: EXP-006, EXP-007, EXP-009, REP-003
#[test]
fn compact_export_replay_refuses_head_only_or_orphan_or_rewritten_pages() {
    use riffdb_storage_api::{
        AuthoritativeMutationV3, AuthoritativeNamespaceV1 as N, AuthoritativeTransactionBindingV3,
        AuthoritativeTransactionV3,
    };
    let directory = crate::test_path::ScopedDirectory::new("export-ledger-replay");
    let mut ports = open(&directory.join("db.redb"));
    let initial = initial(0x61);
    let initial_head = Head::Compact(initial.clone());
    ports
        .compare_and_swap_application_export_head(None, &initial_head, None, 2048)
        .unwrap();
    let source = ports
        .open_owned_snapshot()
        .unwrap()
        .pin_derived_source_v3()
        .unwrap();
    let history = source.history();
    let binding = AuthoritativeTransactionBindingV3 {
        database_id: history.lineage().database_id(),
        history_incarnation: history.lineage().history_incarnation(),
        predecessor: Some(history.tail().sequence()),
        sequence: history.tail().sequence().checked_next().unwrap(),
        predecessor_frontier: history.tail().frontier(),
        covered_frontier: history.tail().frontier(),
        prior_history_hash: history.tail().history_hash(),
    };
    let (next, page) = successor(&initial);
    let before = encode_application_export_head(&initial_head).unwrap();
    let after = encode_application_export_head(&Head::Compact(next)).unwrap();
    let encoded_page = encode_application_export_page_commitment_v1(&page).unwrap();
    let head_mutation = AuthoritativeMutationV3::replace(
        N::ApplicationExportOperations,
        initial.operation_id().as_bytes(),
        before.as_bytes(),
        after.as_bytes(),
    )
    .unwrap();
    let page_mutation = AuthoritativeMutationV3::put(
        N::ApplicationExportPageCommitments,
        &page.canonical_key(),
        None,
        encoded_page.as_bytes(),
    )
    .unwrap();
    let rewritten_page = AuthoritativeMutationV3::put(
        N::ApplicationExportPageCommitments,
        &page.canonical_key(),
        Some([0x44; 32]),
        encoded_page.as_bytes(),
    )
    .unwrap();
    let transaction = ports.shared.database.begin_write().unwrap();
    let tables = crate::changelog_v3_write::table_inventory(&transaction).unwrap();
    let valid = AuthoritativeTransactionV3::new(
        binding,
        ChangelogAttributionV3::ApplicationExportOperation,
        vec![head_mutation.clone(), page_mutation.clone()],
    )
    .unwrap();
    validate_received(&transaction, &tables, &valid).unwrap();
    for mutations in [
        vec![head_mutation.clone()],
        vec![page_mutation],
        vec![head_mutation, rewritten_page],
    ] {
        let receipt = AuthoritativeTransactionV3::new(
            binding,
            ChangelogAttributionV3::ApplicationExportOperation,
            mutations,
        )
        .unwrap();
        assert_eq!(
            validate_received(&transaction, &tables, &receipt)
                .unwrap_err()
                .kind(),
            StorageErrorKind::CorruptData
        );
    }
    let wrong_owner = AuthoritativeTransactionV3::new(
        binding,
        ChangelogAttributionV3::OutboxTransition,
        valid.mutations().to_vec(),
    )
    .unwrap();
    assert!(validate_received(&transaction, &tables, &wrong_owner).is_err());
    transaction.abort().unwrap();
    assert_eq!(
        ports
            .read_application_export_head(initial.operation_id())
            .unwrap(),
        Some(initial_head)
    );
    assert!(
        ports
            .verify_application_export_ledger(&initial)
            .unwrap()
            .is_empty()
    );
}

fn initial(seed: u8) -> StoredApplicationExportOperationV2 {
    let operation =
        ApplicationExportOperationId::from_unix_milliseconds_and_random(1, [seed; 10]).unwrap();
    let binding = b"complete-snapshot-authority-binding".to_vec();
    let prefix = ApplicationExportLedgerPrefixV1::genesis(operation, &binding).unwrap();
    StoredApplicationExportOperationV2::new(
        operation,
        ContractLineage::new("ExportLedger").unwrap(),
        binding,
        b"accepted".to_vec(),
        prefix,
    )
    .unwrap()
}

fn successor(
    previous: &StoredApplicationExportOperationV2,
) -> (
    StoredApplicationExportOperationV2,
    ApplicationExportPageCommitmentV1,
) {
    let page = ApplicationExportPageCommitmentV1::new(
        previous.operation_id(),
        ApplicationExportPageOrdinalV1::new(u64::from(previous.prefix().pages()) + 1).unwrap(),
        ApplicationExportClassV1::Entity,
        ApplicationExportPageHash::from_bytes([0x42; 32]),
        2,
        40,
    )
    .unwrap();
    let encoded = encode_application_export_page_commitment_v1(&page).unwrap();
    let prefix = previous
        .prefix()
        .advance(&page, page.canonical_key().len() + encoded.as_bytes().len())
        .unwrap();
    (
        StoredApplicationExportOperationV2::new(
            previous.operation_id(),
            previous.lineage().clone(),
            previous.immutable_binding().to_vec(),
            b"exporting".to_vec(),
            prefix,
        )
        .unwrap(),
        page,
    )
}

fn open(path: &std::path::Path) -> RedbOperationalPorts {
    use redb::ReadableDatabase;
    let mut store = RedbStore::open(path).unwrap();
    let database = DatabaseId::from_unix_milliseconds_and_random(1, [0x21; 10]).unwrap();
    store.initialize_database(database).unwrap();
    if crate::changelog_v3_roots::validate_retained_history(
        &store.shared.database.begin_read().unwrap(),
    )
    .unwrap()
    .is_none()
    {
        // This unit fixture starts with empty authority. Production reaches
        // this boundary only through complete catalog/startup validation.
        let mut transaction = store.shared.database.begin_write().unwrap();
        crate::changelog_v3_activation::stage_validated(
            &mut transaction,
            riffdb_storage_api::ChangelogLineageV3::new(
                database,
                1,
                riffdb_storage_api::LeadershipEpochV1::initial(),
            )
            .unwrap(),
            riffdb_types::DualFrontier::INITIAL,
        )
        .unwrap();
        store.shared.commit_durable(transaction).unwrap();
    }
    RedbDormantPorts {
        pending_v3_activation: None,
        shared: store.shared,
    }
    .into_operational_after_catalog_validation()
    .unwrap()
}

// req: EXP-006, EXP-007, EXP-009, REP-003
#[test]
fn compact_export_head_and_page_are_atomic_retry_safe_and_recoverable() {
    let directory = crate::test_path::ScopedDirectory::new("export-ledger");
    let path = directory.join("db.redb");
    let mut ports = open(&path);
    assert!(
        ports
            .arm_exact_empty_fresh_locator_coverage_for_test()
            .unwrap()
    );
    let initial = initial(0x61);
    let first = Head::Compact(initial.clone());
    assert_eq!(
        ports
            .compare_and_swap_application_export_head(None, &first, None, 2048)
            .unwrap(),
        WriteResult::Applied
    );
    let source = ports
        .open_owned_snapshot()
        .unwrap()
        .pin_derived_source_v3()
        .unwrap();
    let (next, page) = successor(&initial);
    let next_head = Head::Compact(next.clone());
    assert!(
        ports
            .compare_and_swap_application_export_head(Some(&first), &next_head, None, 2048)
            .is_err()
    );
    assert_eq!(
        ports
            .read_application_export_head(initial.operation_id())
            .unwrap(),
        Some(first.clone())
    );
    assert_eq!(
        ports
            .compare_and_swap_application_export_head(Some(&first), &next_head, Some(&page), 2048)
            .unwrap(),
        WriteResult::Applied
    );
    assert_eq!(
        ports
            .compare_and_swap_application_export_head(Some(&first), &next_head, Some(&page), 2048)
            .unwrap(),
        WriteResult::Unchanged
    );
    assert_eq!(
        ports.verify_application_export_ledger(&next).unwrap(),
        vec![page.page_hash()]
    );
    assert!(ports.verify_application_export_ledger(&initial).is_err());
    let (third, third_page) = successor(&next);
    assert!(
        ports
            .compare_and_swap_application_export_head(
                Some(&first),
                &Head::Compact(third.clone()),
                Some(&third_page),
                2048
            )
            .is_err()
    );
    assert_eq!(
        ports
            .read_application_export_head(initial.operation_id())
            .unwrap(),
        Some(next_head.clone())
    );

    let after = ports
        .open_owned_snapshot()
        .unwrap()
        .pin_derived_source_v3()
        .unwrap();
    let mut cursor = after.receipts_after(&source).unwrap();
    let receipt = cursor.next_receipt().unwrap().unwrap();
    assert_eq!(
        receipt.attribution(),
        ChangelogAttributionV3::ApplicationExportOperation
    );
    assert_eq!(receipt.mutations().len(), 2);
    assert!(
        receipt
            .mutations()
            .iter()
            .any(|mutation| mutation.namespace()
                == riffdb_storage_api::AuthoritativeNamespaceV1::ApplicationExportPageCommitments)
    );
    assert!(cursor.next_receipt().unwrap().is_none());
    assert!(
        ports
            .fresh_locator_public_and_private_roles_match_for_test()
            .unwrap()
    );
    drop(cursor);
    drop(after);
    drop(source);
    drop(ports);
    let ports = open(&path);
    assert_eq!(
        ports
            .read_application_export_head(initial.operation_id())
            .unwrap(),
        Some(next_head.clone())
    );
    assert_eq!(
        ports.verify_application_export_ledger(&next).unwrap(),
        vec![page.page_hash()]
    );
    let transaction = ports.begin_read().unwrap();
    let bytes = encode_application_export_head(&next_head).unwrap();
    inspect_head(
        &transaction,
        initial.operation_id().as_bytes(),
        bytes.as_bytes(),
    )
    .unwrap();
    let bytes = encode_application_export_page_commitment_v1(&page).unwrap();
    inspect_page(&transaction, &page.canonical_key(), bytes.as_bytes()).unwrap();
}

// req: EXP-006, EXP-007, EXP-008, EXP-009, REP-003
#[test]
fn compact_export_storage_append_bytes_are_linear_through_1024_pages() {
    let directory = crate::test_path::ScopedDirectory::new("export-ledger-cost");
    let mut ports = open(&directory.join("db.redb"));
    let mut current = initial(0x65);
    ports
        .compare_and_swap_application_export_head(None, &Head::Compact(current.clone()), None, 2048)
        .unwrap();
    let source = ports
        .open_owned_snapshot()
        .unwrap()
        .pin_derived_source_v3()
        .unwrap();
    for page_number in 1..=1024 {
        let (next, page) = successor(&current);
        ports
            .compare_and_swap_application_export_head(
                Some(&Head::Compact(current)),
                &Head::Compact(next.clone()),
                Some(&page),
                page_number * 70 + 2048,
            )
            .unwrap();
        current = next;
    }
    let after = ports
        .open_owned_snapshot()
        .unwrap()
        .pin_derived_source_v3()
        .unwrap();
    let mut receipts = after.receipts_after(&source).unwrap();
    let mut bytes_per_page = Vec::new();
    while let Some(receipt) = receipts.next_receipt().unwrap() {
        assert_eq!(receipt.mutations().len(), 2, "no prior prefix rewritten");
        bytes_per_page.push(
            receipt
                .mutations()
                .iter()
                .map(|mutation| mutation.key().len() + mutation.value().unwrap().len())
                .sum::<usize>(),
        );
    }
    assert_eq!(bytes_per_page.len(), 1024);
    assert!(
        bytes_per_page
            .iter()
            .all(|charge| *charge == bytes_per_page[0])
    );
    for pages in [16, 128, 1024] {
        assert_eq!(
            bytes_per_page[..pages].iter().sum::<usize>(),
            pages * bytes_per_page[0]
        );
    }
    assert_eq!(
        ports.verify_application_export_ledger(&current).unwrap(),
        vec![ApplicationExportPageHash::from_bytes([0x42; 32]); 1024]
    );
}

// req: EXP-006, EXP-007, EXP-009
#[test]
fn compact_export_backup_restore_and_startup_refuse_broken_prefixes() {
    use redb::ReadableDatabase;
    let directory = crate::test_path::ScopedDirectory::new("export-ledger-copy");
    let source_path = directory.join("source.redb");
    let mut source = open(&source_path);
    let initial = initial(0x69);
    source
        .compare_and_swap_application_export_head(None, &Head::Compact(initial.clone()), None, 2048)
        .unwrap();
    let (next, page) = successor(&initial);
    let head = Head::Compact(next.clone());
    source
        .compare_and_swap_application_export_head(
            Some(&Head::Compact(initial)),
            &head,
            Some(&page),
            2048,
        )
        .unwrap();
    drop(source);
    use riffdb_storage_api::{
        BackupBuildMetadataV1, OfflineBackupPersistencePort, OfflineRestoreOverwritePolicyV1,
        OfflineRestorePersistencePort,
    };
    let backup = directory.join("backup");
    let target = directory.join("restored");
    let build =
        BackupBuildMetadataV1::new("0.1.0", "0123456789abcdef", "rustc-1.97.0", 1, Vec::new())
            .unwrap();
    crate::backup::RedbOfflineBackup::bind(&source_path, &backup)
        .create_offline_backup(&build)
        .unwrap();
    crate::backup::RedbOfflineRestore::bind(&backup, &target)
        .restore_offline_backup(OfflineRestoreOverwritePolicyV1::RefuseNonEmpty)
        .unwrap();
    let mut copy = open(&target.join(crate::backup::DATABASE_ARTIFACT_FILE_NAME));
    assert_eq!(
        copy.read_application_export_head(next.operation_id())
            .unwrap(),
        Some(head.clone())
    );
    assert_eq!(
        copy.verify_application_export_ledger(&next).unwrap(),
        vec![page.page_hash()]
    );
    let encoded = encode_application_export_head(&head).unwrap();
    {
        let read = copy.shared.database.begin_read().unwrap();
        inspect_head(&read, next.operation_id().as_bytes(), encoded.as_bytes()).unwrap();
    }
    // Deliberate offline fixture corruption: the production port exposes no
    // member deletion or rewrite. Each case independently restores the row.
    for corruption in 0..4 {
        let write = copy.shared.database.begin_write().unwrap();
        {
            let mut pages = write.open_table(PAGES).unwrap();
            pages.remove(page.canonical_key().as_slice()).unwrap();
            if corruption != 0 {
                let ordinal = if corruption == 1 { 2 } else { 1 };
                let wrong = ApplicationExportPageCommitmentV1::new(
                    page.operation(),
                    ApplicationExportPageOrdinalV1::new(ordinal).unwrap(),
                    page.class(),
                    ApplicationExportPageHash::from_bytes([0x71; 32]),
                    if corruption == 2 { 3 } else { page.rows() },
                    page.bytes(),
                )
                .unwrap();
                let value = encode_application_export_page_commitment_v1(&wrong).unwrap();
                pages
                    .insert(wrong.canonical_key().as_slice(), value.as_bytes())
                    .unwrap();
            }
        }
        write.commit().unwrap();
        // Raw fixture writes do not publish operational snapshots. Reopen as
        // after offline corruption so the terminal port observes these bytes.
        drop(copy);
        copy = open(&target.join(crate::backup::DATABASE_ARTIFACT_FILE_NAME));
        let read = copy.shared.database.begin_read().unwrap();
        assert!(inspect_head(&read, next.operation_id().as_bytes(), encoded.as_bytes()).is_err());
        assert!(copy.verify_application_export_ledger(&next).is_err());
        drop(read);
        let write = copy.shared.database.begin_write().unwrap();
        {
            let mut pages = write.open_table(PAGES).unwrap();
            let extra = ApplicationExportPageCommitmentV1::new(
                page.operation(),
                ApplicationExportPageOrdinalV1::new(2).unwrap(),
                page.class(),
                page.page_hash(),
                page.rows(),
                page.bytes(),
            )
            .unwrap();
            pages.remove(extra.canonical_key().as_slice()).unwrap();
            let value = encode_application_export_page_commitment_v1(&page).unwrap();
            pages
                .insert(page.canonical_key().as_slice(), value.as_bytes())
                .unwrap();
        }
        write.commit().unwrap();
    }
}
