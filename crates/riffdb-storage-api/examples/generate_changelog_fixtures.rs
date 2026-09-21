#![forbid(unsafe_code)]
//! Frozen V1/V2 compatibility vectors and the proposed V3 format vector.

use std::{env, fs, path::Path};

use riffdb_storage_api::{
    AuthoritativeMutationV3, AuthoritativeNamespaceV1, AuthoritativeStateCatalogV1,
    AuthoritativeStateCatalogV2, AuthoritativeTransactionBindingV3, AuthoritativeTransactionV3,
    ChangelogAttributionV3, ChangelogEntryClassV1, ChangelogEntryClassV2, ChangelogEntryV1,
    ChangelogEntryV2, ChangelogFrameBindingV1, ChangelogFrameBindingV2, ChangelogFrameBindingV3,
    ChangelogFrameV1, ChangelogFrameV2, ChangelogFrameV3, ChangelogHistoryPointV3,
    ChangelogHistoryStateV3, ChangelogLineageV3, ChangelogTransactionAllocator,
    ChangelogTransactionSequence, LeadershipEpochV1, ReplicationFollowerStateV3,
    ReplicationSourceHoldIdV1, ReplicationSourceHoldKindV1 as HoldKind, ReplicationSourceHoldV1,
    proto_codec::{
        encode_authoritative_state_catalog_v1, encode_authoritative_state_catalog_v2,
        encode_changelog_history_state_v3, encode_changelog_transaction_allocator_v3,
        encode_leadership_epoch_v1, encode_replication_follower_state_v3,
        encode_replication_source_hold_v1,
    },
};
use riffdb_types::{AdministrationSequence, CommitSequence, DatabaseId, DualFrontier};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    if arguments != ["--check"] && arguments != ["--write"] {
        return Err("usage: generate_changelog_fixtures --check|--write".into());
    }
    let database_id = DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10])?;
    let initial = DualFrontier::INITIAL;
    let covered = DualFrontier::new(CommitSequence::new(1), None);
    let v1 = ChangelogFrameV1::new(
        ChangelogFrameBindingV1::new(database_id, 1, [0x11; 32], [0x22; 32], true),
        initial,
        covered,
        vec![ChangelogEntryV1::new(
            ChangelogEntryClassV1::Entity,
            b"entity-key".to_vec().into_boxed_slice(),
            b"entity-post-image".to_vec().into_boxed_slice(),
        )],
    )?
    .encode()?;
    let v2 = ChangelogFrameV2::new(
        ChangelogFrameBindingV2 {
            database_id,
            history_incarnation: 1,
            chain_hash: [0x33; 32],
            journal_frame_hash: [0x44; 32],
            journaled: true,
        },
        initial,
        covered,
        vec![ChangelogEntryV2::put(
            ChangelogEntryClassV2::Entity,
            b"entity-key".to_vec(),
            b"entity-post-image".to_vec(),
        )?],
    )?
    .encode()?;
    let binding = AuthoritativeTransactionBindingV3 {
        database_id,
        history_incarnation: 1,
        predecessor: None,
        sequence: ChangelogTransactionSequence::new(1).ok_or("invalid fixture sequence")?,
        predecessor_frontier: initial,
        covered_frontier: covered,
        prior_history_hash: [0x55; 32],
    };
    let first = AuthoritativeTransactionV3::new(
        binding,
        ChangelogAttributionV3::JournaledApplicationGroup,
        vec![AuthoritativeMutationV3::put(
            AuthoritativeNamespaceV1::Entities,
            b"entity-key",
            None,
            b"entity-post-image",
        )?],
    )?;
    let second = AuthoritativeTransactionV3::new(
        AuthoritativeTransactionBindingV3 {
            predecessor: Some(binding.sequence),
            sequence: binding
                .sequence
                .checked_next()
                .ok_or("fixture sequence exhausted")?,
            predecessor_frontier: covered,
            prior_history_hash: first.history_hash()?,
            ..binding
        },
        ChangelogAttributionV3::CleanClose,
        vec![],
    )?;
    let v3 = ChangelogFrameV3::new(
        ChangelogFrameBindingV3::new(
            database_id,
            1,
            1,
            AuthoritativeStateCatalogV1.digest(),
            [0x66; 32],
        )?,
        vec![first, second],
    )?
    .encode()?;
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/replication");
    // Synthetic physical values prove codec custody, not application validation.
    // Complete the closed source set under ADR-0186 Amendment 1 before first use.
    let admission = AuthoritativeTransactionV3::new(
        AuthoritativeTransactionBindingV3 {
            covered_frontier: initial,
            prior_history_hash: [0; 32],
            ..binding
        },
        ChangelogAttributionV3::CommandAdmission,
        vec![AuthoritativeMutationV3::put(
            AuthoritativeNamespaceV1::IdempotencyPending,
            b"command-key",
            None,
            b"pending-admission",
        )?],
    )?;
    let failure_binding = AuthoritativeTransactionBindingV3 {
        predecessor: Some(binding.sequence),
        sequence: binding
            .sequence
            .checked_next()
            .ok_or("fixture sequence exhausted")?,
        covered_frontier: initial,
        prior_history_hash: admission.history_hash()?,
        ..binding
    };
    let failure_mutations = vec![
        AuthoritativeMutationV3::put(
            AuthoritativeNamespaceV1::Idempotency,
            b"command-key",
            None,
            b"execution-failed",
        )?,
        AuthoritativeMutationV3::delete_matching(
            AuthoritativeNamespaceV1::IdempotencyPending,
            b"command-key",
            b"pending-admission",
        )?,
    ];
    let failure = AuthoritativeTransactionV3::new(
        failure_binding,
        ChangelogAttributionV3::CommandExecutionFailure,
        failure_mutations.clone(),
    )?;
    let mut audited_mutations = failure_mutations;
    audited_mutations.push(AuthoritativeMutationV3::put(
        AuthoritativeNamespaceV1::Audit,
        b"audit-key",
        None,
        b"terminal-service-audit",
    )?);
    let audited_failure = AuthoritativeTransactionV3::new(
        AuthoritativeTransactionBindingV3 {
            covered_frontier: DualFrontier::new(None, Some(AdministrationSequence::first())),
            ..failure_binding
        },
        ChangelogAttributionV3::CommandExecutionFailure,
        audited_mutations,
    )?;
    let lifecycle_frame = ChangelogFrameV3::new(
        ChangelogFrameBindingV3::new(
            database_id,
            1,
            1,
            AuthoritativeStateCatalogV1.digest(),
            [0; 32],
        )?,
        vec![admission.clone(), failure.clone()],
    )?
    .encode()?;
    let admission = admission.encode()?;
    let failure = failure.encode()?;
    let audited_failure = audited_failure.encode()?;
    let catalog = encode_authoritative_state_catalog_v1(AuthoritativeStateCatalogV1)?;
    let catalog_v2 = encode_authoritative_state_catalog_v2(AuthoritativeStateCatalogV2)?;
    let epoch_first = encode_leadership_epoch_v1(LeadershipEpochV1::initial())?;
    let epoch_last =
        encode_leadership_epoch_v1(LeadershipEpochV1::new(u64::MAX).ok_or("invalid epoch")?)?;
    let allocator_first =
        encode_changelog_transaction_allocator_v3(ChangelogTransactionAllocator::initial())?;
    let allocator_last =
        encode_changelog_transaction_allocator_v3(ChangelogTransactionAllocator::Next(
            ChangelogTransactionSequence::new(u64::MAX).ok_or("invalid maximum")?,
        ))?;
    let allocator_exhausted =
        encode_changelog_transaction_allocator_v3(ChangelogTransactionAllocator::Exhausted)?;
    let lineage = ChangelogLineageV3::new(database_id, 1, LeadershipEpochV1::initial())?;
    let point = ChangelogHistoryPointV3::new(binding.sequence, [0x77; 32], covered);
    let history = encode_changelog_history_state_v3(ChangelogHistoryStateV3::new(
        lineage, point, point, point,
    )?)?;
    let detached = encode_replication_follower_state_v3(ReplicationFollowerStateV3::detached())?;
    let hold = |kind| {
        encode_replication_source_hold_v1(ReplicationSourceHoldV1::new(
            ReplicationSourceHoldIdV1::new([0x81; 16]).ok_or("invalid hold ID")?,
            kind,
            lineage,
            point,
        ))
        .map_err(Into::<Box<dyn std::error::Error>>::into)
    };
    let follower_hold = hold(HoldKind::FollowerAcknowledgement)?;
    let archive_hold = hold(HoldKind::ArchiveAcknowledgement)?;
    let bootstrap_hold = hold(HoldKind::Bootstrap)?;
    let attached = encode_replication_follower_state_v3(ReplicationFollowerStateV3::attached(
        lineage,
        point,
        Some(point),
    )?)?;
    // ADR-0232: complete canonical member/head vectors, in addition to the
    // generated schema identities and bounds. Legacy export records are frozen.
    use riffdb_storage_api::{
        ApplicationExportLedgerPrefixV1, ApplicationExportPageCommitmentV1,
        ApplicationExportPageOrdinalV1, StoredApplicationExportOperationV2,
        encode_application_export_operation_v2, encode_application_export_page_commitment_v1,
    };
    use riffdb_types::{
        ApplicationExportClassV1, ApplicationExportOperationId, ApplicationExportPageHash,
        ContractLineage,
    };
    let export_id = ApplicationExportOperationId::from_unix_milliseconds_and_random(
        1_700_000_000_000,
        [0x91; 10],
    )?;
    let export_binding = b"frozen-export-snapshot-authority-binding".to_vec();
    let export_genesis = ApplicationExportLedgerPrefixV1::genesis(export_id, &export_binding)?;
    let export_page = ApplicationExportPageCommitmentV1::new(
        export_id,
        ApplicationExportPageOrdinalV1::new(1)?,
        ApplicationExportClassV1::Entity,
        ApplicationExportPageHash::from_bytes([0x92; 32]),
        2,
        64,
    )?;
    let export_member = encode_application_export_page_commitment_v1(&export_page)?;
    let export_prefix = export_genesis.advance(
        &export_page,
        export_page.canonical_key().len() + export_member.as_bytes().len(),
    )?;
    let export_head =
        encode_application_export_operation_v2(&StoredApplicationExportOperationV2::new(
            export_id,
            ContractLineage::new("ExportLedger")?,
            export_binding,
            b"{\"phase\":\"exporting\"}".to_vec(),
            export_prefix,
        )?)?;
    for (name, bytes) in [
        ("export-operation-v2.hex", export_head.as_bytes()),
        ("export-page-commitment-v1.hex", export_member.as_bytes()),
        (
            "export-page-commitment-key-v1.hex",
            export_page.canonical_key().as_slice(),
        ),
        ("changelog-frame-v1.hex", v1.as_bytes()),
        (
            "replication-source-hold-v1-follower.hex",
            follower_hold.as_bytes(),
        ),
        (
            "replication-source-hold-v1-archive.hex",
            archive_hold.as_bytes(),
        ),
        (
            "replication-source-hold-v1-bootstrap.hex",
            bootstrap_hold.as_bytes(),
        ),
        ("changelog-frame-v2.hex", v2.as_bytes()),
        ("changelog-frame-v3.hex", v3.as_slice()),
        (
            "changelog-receipt-v3-command-admission.hex",
            admission.as_slice(),
        ),
        (
            "changelog-receipt-v3-command-execution-failure.hex",
            failure.as_slice(),
        ),
        (
            "changelog-receipt-v3-command-execution-failure-audited.hex",
            audited_failure.as_slice(),
        ),
        (
            "changelog-frame-v3-command-lifecycle.hex",
            lifecycle_frame.as_slice(),
        ),
        ("authoritative-state-catalog-v1.hex", catalog.as_bytes()),
        ("authoritative-state-catalog-v2.hex", catalog_v2.as_bytes()),
        ("leadership-epoch-v1-first.hex", epoch_first.as_bytes()),
        ("leadership-epoch-v1-last.hex", epoch_last.as_bytes()),
        ("changelog-history-state-v3.hex", history.as_bytes()),
        (
            "replication-follower-state-v3-detached.hex",
            detached.as_bytes(),
        ),
        (
            "replication-follower-state-v3-attached.hex",
            attached.as_bytes(),
        ),
        (
            "changelog-allocator-v3-first.hex",
            allocator_first.as_bytes(),
        ),
        ("changelog-allocator-v3-last.hex", allocator_last.as_bytes()),
        (
            "changelog-allocator-v3-exhausted.hex",
            allocator_exhausted.as_bytes(),
        ),
    ] {
        let hex = bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
            + "\n";
        if arguments == ["--write"] {
            fs::create_dir_all(&root)?;
            fs::write(root.join(name), hex)?;
        } else if fs::read_to_string(root.join(name))? != hex {
            return Err(format!("{name} differs from its canonical codec").into());
        }
    }
    Ok(())
}
