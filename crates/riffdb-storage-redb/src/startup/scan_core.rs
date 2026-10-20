//! One unchanged structural/historical scan core shared by closed native owners.
use super::*;

pub(super) struct EvidenceScanCoreV1<O: EvidenceScanReadOwnerV1 = Arc<SharedRedb>> {
    // Physical Source state/Box/aliases drop before the shared engine owner.
    pub(super) source_components: Option<source_row_commitment::AdmittedSourceValidationV1>,
    pub(super) source_component_start: u64,
    pub(super) catalog_validation_complete: bool,
    pub(super) cancellation: Option<Arc<AtomicBool>>,
    pub(super) shared: O,
    pub(super) lease: Option<ExclusiveLease>,
    pub(super) durable_commit_epoch: O::Observation,
    pub(super) database_id: DatabaseId,
    pub(super) open_session_id: OpenSessionId,
    pub(super) retained_metadata: RetainedMetadataV1,
    pub(super) inputs: StartupValidationInputs,
    /// Exact inactive predecessor; complete validation must precede migration.
    pub(super) v3_activation_required: bool,
    /// True only for an ADR-0157 verified clean-close bounded startup.
    pub(super) clean_close_fast: bool,
    /// True only for an ADR-0182 bounded dirty startup: journal-suffix
    /// recovery plus bounded-root validation with no population walk.
    pub(super) bounded_dirty_startup: bool,
    /// Why this session opened; the background scrub is lease-free and its
    /// continuity checks tolerate a live writer.
    pub(super) purpose: EvidenceOpenPurpose,
    /// Exact clean lifecycle consumed to DIRTY immediately before activation.
    pub(super) verified_clean_lifecycle: Option<crate::clean_close::CleanCloseLifecycle>,
    /// Which precondition declined the ADR-0157 bounded path, if it declined.
    /// `None` exactly when `clean_close_fast` is true.
    pub(super) clean_close_declined_reason: Option<crate::clean_close::CleanCloseDeclineReason>,
    /// Walk plan counts (full or suffix-adjusted under a verified checkpoint).
    pub(super) structural_counts: [u64; STRUCTURAL_TABLE_COUNT],
    /// Full table lens from the immutable snapshot (for below-S count verification).
    pub(super) full_structural_counts: [u64; STRUCTURAL_TABLE_COUNT],
    /// Additive table counts never covered by the V1 validated-prefix checkpoint.
    pub(super) additive_structural_counts: [u64; ADDITIVE_STRUCTURAL_TABLE_COUNT],
    pub(super) structural_total: u64,
    pub(super) next_structural: StructuralEvidenceCursor,
    pub(super) next_historical: HistoricalEvidenceCursor,
    pub(super) last_historical_key: Option<Vec<u8>>,
    pub(super) structural_finished: bool,
    pub(super) historical_finished: bool,
    pub(super) authoritative_finding_seen: bool,
    /// True once ANY structural finding of ANY scope was produced (including a
    /// sample-window finding that dropped the fast path). Gates the checkpoint
    /// write: a finding of any severity vetoes it (ADR-0019 A1).
    pub(super) any_finding_seen: bool,
    pub(super) saw_v1_index: bool,
    /// The one immutable snapshot pin owned by the validation session.
    ///
    /// Structural and historical validation share this snapshot while the
    /// exclusive mutation lease is retained. Historical materialization does
    /// not reopen and recheck every table for every key, and the session never
    /// owns overlapping read transactions.
    pub(super) validation_read: Option<ReadTransaction>,
    pub(super) historical_tables: Option<HistoricalMaterializationTables>,
    pub(super) structural_cursors: Option<StructuralCursors>,
    /// The bounded command history of the complete walk: a compact derived
    /// audit index plus on-demand command resolution (never the decoded
    /// whole history, which exceeded the evidence ceiling at scale).
    pub(super) command_history: command_history::CommandHistory,
    pub(super) command_audit_bytes: usize,
    pub(super) command_cache_built: bool,
    /// COMMITS rows decoded into the startup command cache (observation only).
    pub(super) command_history_rows_decoded: u64,
    /// Whether this session's source is on AuthoritativeStateCatalogV2.
    pub(super) v2_source: bool,
    /// Peak bytes held together by this session's recovery-owned structures:
    /// the historical evidence plan's locator index and the startup command
    /// cache (observation only; ADR-0182 Decision 5 bounds the dirty path).
    pub(super) recovery_owned_peak_bytes: u64,
    /// Publication facts decoded once from AUDIT and shared by catalog, query
    /// module, reactive module, and suffix-audit validation.
    pub(super) publication_audits: Option<PublicationAuditCache>,
    /// Exact valid retained bundles collected once from the pinned startup
    /// snapshot. Entity and index validation consult this bounded set instead
    /// of reopening and decoding `CONTRACT_BUNDLES` for every retained row.
    pub(super) binding_bundles: BTreeSet<riffdb_storage_api::DurableKeySchemaBindingV1>,
    /// Built at ENTITIES phase entry; dropped after orphan findings are queued.
    pub(super) entity_chains: Option<EntityChainState>,
    /// Entity chains (or their build refusal) from the command cache's
    /// single `COMMITS` pass, installed at ENTITIES phase entry (ADR-0287).
    pub(super) prebuilt_entity_chains: Option<Result<EntityChainState, StorageError>>,
    /// `COMMITS` plan references from the same pass, read by the historical
    /// evidence plan over this snapshot (ADR-0287).
    pub(super) prebuilt_commit_plans: Option<PrecollectedCommitPlans>,
    /// Non-command `AUDIT` rows decoded for publication proofs this session.
    /// One whole physical pass serves every catalog, query-module, and reactive
    /// module check; the counter excludes command locators skipped by dispatch.
    pub(super) reactive_publication_audit_decodes: u64,
    /// Bounded cross-link findings for unconsumed/orphan chains (VecDeque: O(1) drain).
    pub(super) pending_entity_orphan_findings: VecDeque<StructuralFinding>,
    pub(super) historical_plan: Option<HistoricalEvidencePlan>,
    /// Verified active checkpoint enabling prefix-skipping startup.
    pub(super) checkpoint: Option<crate::validated_prefix::ActiveCheckpoint>,
    /// Rows inspected via deterministic sample windows (test observability).
    pub(super) sampled_window_rows_inspected: u64,
    /// True when a checkpoint was verified and the fast path is active.
    pub(super) checkpoint_verified: bool,
    /// Why a present-but-unusable checkpoint was ignored (test/operator observability).
    pub(super) checkpoint_ignored_reason: Option<crate::validated_prefix::CheckpointIgnoreReason>,
    /// Walked suffix rows for range-skipped sequence-keyed tables.
    pub(super) walked_suffix_counts: [u64; STRUCTURAL_TABLE_COUNT],
    /// Prefix rows classified-and-skipped during the full walks of the
    /// non-sequence-prefixed tables (EVENT_ROUTES/IDEMPOTENCY/AUDIT_BY_REQUEST).
    pub(super) walked_prefix_counts: [u64; STRUCTURAL_TABLE_COUNT],
    /// Terminal `ExecutionFailed` rows seen in the IDEMPOTENCY phase — the whole
    /// table on both the full walk and the checkpoint fast path, which classifies
    /// every row of that table either way. Seeds the process census that lets a
    /// checkpoint's materialized-StoredOutcome-only `idempotency_count` be
    /// derived from redb's IDEMPOTENCY row count instead of a full pass.
    pub(super) terminal_execution_failure_rows: u64,
    /// The last command segment the structural walk decoded, reused by the
    /// service-audit link checks that resolve the same row again.
    pub(super) segments: SegmentMemo,
    /// The last contract bundle the commit phase validated, reused for the
    /// following rows whose stored bundle record is equal (ADR-0286).
    pub(super) bundles: crate::command_prefix::BundleMemo,
}

impl<O: EvidenceScanReadOwnerV1> EvidenceScanCoreV1<O> {
    pub(super) fn walk_complete_structural(
        &mut self,
    ) -> Result<RedbStructuralEvidenceEnd, StorageError> {
        let limit = EvidencePageLimit::new(64).ok_or_else(limit_exceeded)?;
        let mut cursor = StructuralEvidenceCursor::start(self.database_id, self.open_session_id);
        loop {
            match self.read_structural_evidence(cursor, limit)? {
                StructuralEvidencePage::Page { findings, next, .. } => {
                    if !findings.is_empty() {
                        return Err(storage_error(StorageErrorKind::CorruptData));
                    }
                    cursor = next;
                }
                StructuralEvidencePage::ExactEnd(end) => return Ok(end),
            }
        }
    }

    pub(super) fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    pub(super) fn open_session_id(&self) -> OpenSessionId {
        self.open_session_id
    }

    pub(super) fn read_structural_evidence(
        &mut self,
        cursor: StructuralEvidenceCursor,
        limit: EvidencePageLimit,
    ) -> Result<StructuralEvidencePage<RedbStructuralEvidenceEnd>, StorageError> {
        if let Err(error) = self.check_cancellation() {
            // Cancellation destroys Source ranges/DTOs/Box before its retained
            // actual Maintenance charge; the cancelled scan cannot resume.
            self.source_components = None;
            return Err(error);
        }
        if self.structural_finished || cursor != self.next_structural {
            return Err(invariant());
        }
        if cursor.position() > self.structural_total {
            return Err(invariant());
        }
        self.ensure_structural_continuity()?;
        if cursor.position() == self.structural_total {
            // Orphan findings must already have been drained on the last
            // advancing page — never emit a non-advancing page here.
            if !self.pending_entity_orphan_findings.is_empty() {
                return Err(invariant());
            }
            self.verify_checkpoint_prefix_counts()?;
            if !self.clean_close_fast && !self.bounded_dirty_startup {
                self.verify_source_commitment_work_end()?;
                source_validation::finish(
                    &self.additive_structural_counts[EXISTING_ADDITIVE_STRUCTURAL_TABLE_COUNT..],
                )?;
            }
            self.structural_finished = true;
            self.source_components = None;
            self.structural_cursors = None;
            self.entity_chains = None;
            self.publication_audits = None;
            return Ok(StructuralEvidencePage::ExactEnd(
                RedbStructuralEvidenceEnd { cursor },
            ));
        }

        let page_cap = riffdb_storage_api::MAX_INTEGRITY_FINDINGS;
        let finding_bound = u64::try_from(page_cap).map_err(|_| limit_exceeded())?;
        let remaining = self
            .structural_total
            .checked_sub(cursor.position())
            .ok_or_else(invariant)?;
        let mut inspected = remaining.min(u64::from(limit.get())).min(finding_bound);
        // Last advancing page must reserve room for the bounded orphan set so
        // findings never require a non-advancing terminal drain.
        // row_budget is provably > 0 via the compile-time guard above.
        if inspected == remaining {
            let row_budget = finding_bound.saturating_sub(ENTITY_CHAIN_ORPHAN_FINDING_CAP as u64);
            if inspected > row_budget {
                inspected = row_budget;
            }
        }
        // Preserve the existing page limit and bound every Source visit: one
        // <=2-MiB row plus its <=4-KiB local control lookups per page. Never hide
        // a population traversal inside one Object inspection.
        let source_start = self
            .structural_counts
            .iter()
            .chain(
                self.additive_structural_counts[..EXISTING_ADDITIVE_STRUCTURAL_TABLE_COUNT].iter(),
            )
            .try_fold(1_u64, |n, count| {
                n.checked_add(*count).ok_or_else(limit_exceeded)
            })?;
        if cursor.position() >= source_start {
            inspected = inspected.min(1);
        } else {
            inspected = inspected.min(source_start - cursor.position());
        }
        let mut findings = Vec::new();
        // Continuity above pins every row checked below. The walk remembers
        // only a validated exact stored bundle; command semantics are rechecked.
        let parallel = if cursor.position() < self.source_component_start {
            self.inspect_parallel_audit_page(cursor.position(), inspected)?
        } else {
            None
        };
        if let Some(results) = parallel {
            inspected = u64::try_from(results.len()).map_err(|_| limit_exceeded())?;
            for result in results {
                if let Some(finding) = result? {
                    if finding.scope() == StructuralFindingScope::Authoritative {
                        self.authoritative_finding_seen = true;
                    }
                    findings.push(finding);
                }
            }
        } else {
            for offset in 0..inspected {
                let position = cursor
                    .position()
                    .checked_add(offset)
                    .ok_or_else(limit_exceeded)?;
                if let Some(finding) = self.inspect_structural_forward(position)? {
                    if finding.scope() == StructuralFindingScope::Authoritative {
                        self.authoritative_finding_seen = true;
                    }
                    findings.push(finding);
                }
            }
        }
        let next = cursor.advanced(inspected).map_err(value_error_as_storage)?;
        let finishing = next.position() == self.structural_total;
        if finishing {
            // Drain every remaining orphan on the last advancing page.
            while let Some(finding) = self.pending_entity_orphan_findings.pop_front() {
                if findings.len() >= page_cap {
                    return Err(limit_exceeded());
                }
                if finding.scope() == StructuralFindingScope::Authoritative {
                    self.authoritative_finding_seen = true;
                }
                findings.push(finding);
            }
        } else {
            while findings.len() < page_cap
                && let Some(finding) = self.pending_entity_orphan_findings.pop_front()
            {
                if finding.scope() == StructuralFindingScope::Authoritative {
                    self.authoritative_finding_seen = true;
                }
                findings.push(finding);
            }
        }
        if !findings.is_empty() {
            self.any_finding_seen = true;
        }
        let page =
            StructuralEvidencePage::page(cursor, findings, next).map_err(value_error_as_storage)?;
        self.next_structural = next;
        Ok(page)
    }

    pub(super) fn read_historical_evidence(
        &mut self,
        cursor: HistoricalEvidenceCursor,
        limit: EvidencePageLimit,
    ) -> Result<HistoricalEvidencePage<RedbHistoricalEvidenceEnd>, StorageError> {
        if self.historical_finished || cursor != self.next_historical {
            return Err(invariant());
        }
        // Structural pin is released once structural finishes; historical
        // installs its own immutable pin for the rest of the startup session.
        self.ensure_historical_read()?;
        if self.historical_plan.is_none() {
            let transaction = self.validation_read.as_ref().ok_or_else(invariant)?;
            let plan = if self.clean_close_fast || self.bounded_dirty_startup {
                // Both bounded startup paths walk only the bounded historical
                // locators (bundles, migration edges, the active catalog); the
                // full evidence plan is the scrub's and the explicit complete
                // path's.
                let plan = build_clean_close_historical_evidence_plan(transaction)?;
                // ADR-0182 Decision 5: the dirty path's recovery-owned state
                // (the bounded locator index plus the command cache, which the
                // bounded header never builds) is bounded by the compiled
                // 64 MiB ceiling and refuses with a typed internal defect
                // rather than spilling.
                if self.bounded_dirty_startup
                    && plan.retained_bytes.saturating_add(self.command_audit_bytes)
                        > MAX_RECOVERY_OWNED_STATE_BYTES
                {
                    return Err(storage_error(StorageErrorKind::InvariantViolation));
                }
                plan
            } else {
                build_historical_evidence_plan(
                    transaction,
                    &self.inputs,
                    self.prebuilt_commit_plans.take(),
                )?
            };
            self.note_recovery_owned_bytes(plan.retained_bytes);
            self.historical_plan = Some(plan);
        }
        let requested = usize::try_from(limit.get()).map_err(|_| limit_exceeded())?;
        let transaction = self.validation_read.as_ref().ok_or_else(invariant)?;
        let materialization_tables = self.historical_tables.as_ref().ok_or_else(invariant)?;
        let mut evidence = Vec::new();
        let mut bytes = 0usize;
        let mut migration_rows = 0usize;
        let mut migration_evidence_bytes = 0usize;
        let mut migration_instruction_bytes = 0usize;
        let mut last_key = self.last_historical_key.clone();
        let plan = self.historical_plan.as_mut().ok_or_else(invariant)?;
        let mut exhausted = false;
        while evidence.len() < requested {
            let Some(next_item) = plan.next(transaction, materialization_tables)? else {
                exhausted = true;
                break;
            };
            let PendingHistoricalEvidence {
                order_key,
                evidence: item,
            } = next_item;
            if let Some(prior) = last_key.as_ref() {
                if prior > &order_key {
                    return Err(invariant());
                }
                if prior == &order_key {
                    // Preserve the old BTreeMap's order-key deduplication.
                    continue;
                }
            }
            let next_bytes = bytes
                .checked_add(historical_semantic_bytes(&item)?)
                .ok_or_else(limit_exceeded)?;
            if next_bytes > riffdb_storage_api::MAX_HISTORICAL_EVIDENCE_PAGE_BYTES {
                if evidence.is_empty() {
                    return Err(limit_exceeded());
                }
                plan.pending = Some(PendingHistoricalEvidence {
                    order_key,
                    evidence: item,
                });
                break;
            }
            if let HistoricalSemanticEvidence::IndexMigrationRow(row) = &item {
                let next_rows = migration_rows.checked_add(1).ok_or_else(limit_exceeded)?;
                let next_evidence_bytes = migration_evidence_bytes
                    .checked_add(row.evidence_page_charge())
                    .ok_or_else(limit_exceeded)?;
                let next_instruction_bytes = migration_instruction_bytes
                    .checked_add(row.instruction_page_charge())
                    .ok_or_else(limit_exceeded)?;
                if next_rows > riffdb_storage_api::MAX_INDEX_MIGRATION_PAGE_ENTRIES
                    || next_evidence_bytes > riffdb_storage_api::MAX_INDEX_MIGRATION_PAGE_BYTES
                    || next_instruction_bytes > riffdb_storage_api::MAX_INDEX_MIGRATION_PAGE_BYTES
                {
                    if evidence.is_empty() {
                        return Err(limit_exceeded());
                    }
                    plan.pending = Some(PendingHistoricalEvidence {
                        order_key,
                        evidence: item,
                    });
                    break;
                }
                migration_rows = next_rows;
                migration_evidence_bytes = next_evidence_bytes;
                migration_instruction_bytes = next_instruction_bytes;
            }
            bytes = next_bytes;
            last_key = Some(order_key);
            if matches!(
                &item,
                HistoricalSemanticEvidence::IndexMigrationRow(row) if row.row().is_v1()
            ) {
                self.saw_v1_index = true;
            }
            evidence.push(item);
        }
        if evidence.is_empty() {
            if !exhausted {
                return Err(invariant());
            }
            self.historical_finished = true;
            return Ok(HistoricalEvidencePage::ExactEnd(
                RedbHistoricalEvidenceEnd { cursor },
            ));
        }
        let count = u64::try_from(evidence.len()).map_err(|_| limit_exceeded())?;
        let next = cursor.advanced(count).map_err(value_error_as_storage)?;
        let page =
            HistoricalEvidencePage::page(cursor, evidence, next).map_err(value_error_as_storage)?;
        self.last_historical_key = last_key;
        self.next_historical = next;
        Ok(page)
    }

    pub(super) fn read_historical_bundle(
        &mut self,
        lineage: &ContractLineage,
        version: ContractVersion,
        hash: ContractBundleHash,
    ) -> Result<Option<HistoricalBundleEvidence>, StorageError> {
        self.ensure_historical_read()?;
        read_historical_bundle_from_table(
            &self
                .historical_tables
                .as_ref()
                .ok_or_else(invariant)?
                .bundles,
            lineage,
            version,
            hash,
        )
    }

    pub(super) fn read_integrity_entity(
        &mut self,
        target: &riffdb_storage_api::EntityTarget,
    ) -> Result<Option<riffdb_storage_api::StoredEntityRecordV1>, StorageError> {
        self.ensure_historical_read()?;
        let tables = self.historical_tables.as_ref().ok_or_else(invariant)?;
        crate::reads::read_entity_record(&tables.entities, target)
    }

    pub(super) fn read_integrity_unique_occupancy(
        &mut self,
        target: &UniqueIndexTarget,
    ) -> Result<UniqueOccupancyKind, StorageError> {
        self.ensure_historical_read()?;
        let table = &self
            .historical_tables
            .as_ref()
            .ok_or_else(invariant)?
            .indexes;
        let prefix = target.prefix().prefix().as_bytes();
        let upper = exclusive_prefix_end(prefix).ok_or_else(corrupt)?;
        let mut range = table
            .range::<&[u8]>((Included(prefix), Excluded(upper.as_slice())))
            .map_err(precommit_storage_error)?;
        let first = range
            .next()
            .transpose()
            .map_err(precommit_storage_error)?
            .map(|(key, _)| key.value().to_vec());
        let second = range.next().transpose().map_err(precommit_storage_error)?;
        match (first, second) {
            (None, None) => Ok(UniqueOccupancyKind::Vacant),
            (Some(key), None) if key.as_slice() == target.expected_entry().as_bytes() => {
                Ok(UniqueOccupancyKind::Owned)
            }
            (Some(_), None) => Ok(UniqueOccupancyKind::Conflict),
            _ => Err(corrupt()),
        }
    }

    pub(super) fn set_cancellation(&mut self, cancellation: Arc<AtomicBool>) {
        self.cancellation = Some(cancellation);
    }

    pub(super) fn check_cancellation(&self) -> Result<(), StorageError> {
        if self
            .cancellation
            .as_ref()
            .is_some_and(|flag| flag.load(Ordering::Acquire))
        {
            return Err(storage_error(StorageErrorKind::Unavailable));
        }
        Ok(())
    }

    pub(super) fn ensure_historical_read(&mut self) -> Result<(), StorageError> {
        self.check_cancellation()?;
        if self.validation_read.is_none() {
            // Commit plans read from an earlier snapshot never describe this one.
            self.prebuilt_commit_plans = None;
            self.validation_read = Some(self.open_snapshot_read()?);
        }
        if self.historical_tables.is_none() {
            let transaction = self.validation_read.as_ref().ok_or_else(invariant)?;
            self.historical_tables = Some(HistoricalMaterializationTables::open(transaction)?);
        }
        if self.historical_tables.is_none() {
            return Err(invariant());
        }
        Ok(())
    }

    pub(super) fn open_snapshot_read(&self) -> Result<ReadTransaction, StorageError> {
        if self.purpose != EvidenceOpenPurpose::BackgroundScrub
            && !self.shared.scan_epoch_matches(self.durable_commit_epoch)?
        {
            return Err(corrupt());
        }
        let transaction = self.shared.open_scan_read()?;
        self.verify_structural_continuity(&transaction)?;
        if self.purpose != EvidenceOpenPurpose::BackgroundScrub
            && !self.shared.scan_epoch_matches(self.durable_commit_epoch)?
        {
            return Err(corrupt());
        }
        Ok(transaction)
    }

    pub(super) fn ensure_structural_continuity(&self) -> Result<(), StorageError> {
        if self.purpose != EvidenceOpenPurpose::BackgroundScrub
            && !self.shared.scan_epoch_matches(self.durable_commit_epoch)?
        {
            return Err(corrupt());
        }
        let Some(transaction) = self.validation_read.as_ref() else {
            return Err(invariant());
        };
        self.verify_structural_continuity(transaction)
    }

    pub(super) fn verify_structural_continuity(
        &self,
        transaction: &ReadTransaction,
    ) -> Result<(), StorageError> {
        self.verify_structural_counts(transaction)?;
        self.shared.validate_scan_binding(transaction)?;
        let metadata = self.shared.scan_retained_metadata(transaction)?;
        if metadata != self.retained_metadata {
            return Err(corrupt());
        }
        Ok(())
    }

    pub(super) fn verify_structural_counts(
        &self,
        transaction: &ReadTransaction,
    ) -> Result<(), StorageError> {
        if self.purpose.is_restore_readiness() {
            let snapshot = collect_bounded_restore_snapshot(transaction)?;
            if snapshot.structural_counts != self.full_structural_counts
                || snapshot.retained_metadata != self.retained_metadata
            {
                return Err(corrupt());
            }
            return Ok(());
        }
        let meta = transaction.open_table(META).map_err(table_error)?;
        let mut counts = [0u64; STRUCTURAL_TABLE_COUNT];
        counts[0] = meta.len().map_err(precommit_storage_error)?;
        drop(meta);
        // Length-annotated so a table added to STRUCTURAL_TABLE_COUNT without a
        // row here is a compile error, not a silent zero (RT-A/RT-B lesson).
        let definitions: [TableDefinition<'static, &'static [u8], &'static [u8]>;
            STRUCTURAL_TABLE_COUNT - 1] = [
            CONTRACT_BUNDLES,
            CATALOG_ACTIVE,
            QUERY_MODULES,
            QUERY_MODULE_ACTIVE,
            ENTITIES,
            SECONDARY_INDEXES,
            INDEX_EPOCHS,
            IDEMPOTENCY,
            IDEMPOTENCY_PENDING,
            COMMITS,
            PROVENANCE,
            EVENTS,
            EVENT_ROUTES,
            OUTBOX,
            OUTBOX_STATUS,
            PROJECTION_STATE,
            PROJECTION_FRONTIER,
            PROJECTION_APPLIED,
            CAPABILITIES,
            CAPABILITY_TOKENS,
            AUDIT,
            AUDIT_BY_REQUEST,
            CONTRACT_MIGRATION_JOURNAL,
            CONTRACT_MIGRATIONS,
            CONTRACT_WRITE_RETIREMENTS,
            RETIRED_ENTITIES,
            HISTORY_TOMBSTONES,
        ];
        for (index, definition) in definitions.into_iter().enumerate() {
            counts[index + 1] = table_len(transaction, definition)?;
        }
        if counts != self.full_structural_counts {
            return Err(corrupt());
        }
        if self.clean_close_fast || self.bounded_dirty_startup {
            // Redb table lengths are metadata reads. They prove the immutable
            // snapshot did not change without walking any population table.
            return Ok(());
        }
        if self.checkpoint.is_none() && counts != self.structural_counts {
            return Err(corrupt());
        }
        let mut additive_counts = [
            table_len(transaction, REACTIVE_MODULES)?,
            table_len(transaction, EVENT_CONSUMERS)?,
            table_len(transaction, EVENT_CONSUMER_DELIVERIES)?,
            table_len(transaction, APPLICATION_INSTALLATION_CAMPAIGNS)?,
            table_len(transaction, APPLICATION_EXPORT_OPERATIONS)?,
            table_len(transaction, VECTOR_EVIDENCE)?,
            table_len(transaction, VECTOR_OBSERVATIONS)?,
            table_len(transaction, VECTOR_EVIDENCE_INDEX)?,
            table_len(transaction, APPLICATION_EXPORT_PAGE_COMMITMENTS)?,
            0,
            0,
            0,
            0,
        ];
        additive_counts[EXISTING_ADDITIVE_STRUCTURAL_TABLE_COUNT..]
            .copy_from_slice(&source_validation::counts(transaction)?);
        if additive_counts != self.additive_structural_counts {
            return Err(corrupt());
        }
        Ok(())
    }

    /// Verifies ALL EIGHT recorded below-S row counts at structural ExactEnd.
    ///
    /// Range-skipped phases (COMMITS/EVENTS/OUTBOX/OUTBOX_STATUS/AUDIT) check
    /// `full_len − walked_suffix == recorded_prefix`. The non-sequence-prefixed
    /// tables (EVENT_ROUTES/IDEMPOTENCY/AUDIT_BY_REQUEST) are fully walked with
    /// a counting skip; their classified prefix rows must equal the recorded
    /// counts exactly. Per ADR-0019 A1, a divergence after verified bindings is
    /// authoritative corruption: the recorded counts describe an immutable
    /// prefix, so the open REFUSES — exactly as full validation refuses on
    /// authoritative findings — instead of falling back.
    ///
    /// # Recovering a refusal
    ///
    /// A refusal here does not require restore-from-backup. `RetentionMaintenance::prune_to`
    /// (`crate::retention`, `prune_to`) opens the database file directly — no validated
    /// startup — and its FIRST exclusive transaction unconditionally deletes
    /// `META_VALIDATED_PREFIX_CHECKPOINT`. Running the offline retention prune
    /// therefore removes the disputed checkpoint, after which the next open finds
    /// none and runs full validation, which re-derives every count from its own
    /// walk. That is the operator lever for a count divergence, whatever produced
    /// it — a drifted process census, or rows added or lost below S while the
    /// database was closed.
    pub(super) fn verify_checkpoint_prefix_counts(&self) -> Result<(), StorageError> {
        let Some(checkpoint) = self.checkpoint.as_ref() else {
            return Ok(());
        };
        let c = checkpoint.counts;
        let range_skipped = [
            (10, c.commits_count),
            (12, c.events_count),
            (14, c.outbox_count),
            (15, c.outbox_status_count),
            (21, c.audit_count),
        ];
        for (phase, recorded_prefix) in range_skipped {
            let full = self.full_structural_counts[phase];
            let walked = self.walked_suffix_counts[phase];
            if self.structural_counts[phase] != walked {
                return Err(corrupt());
            }
            if full.saturating_sub(walked) != recorded_prefix {
                return Err(corrupt());
            }
        }
        let counted_skip_walks = [
            (13, c.event_routes_count),
            (8, c.idempotency_count),
            (22, c.audit_by_request_count),
        ];
        for (phase, recorded_prefix) in counted_skip_walks {
            if self.walked_prefix_counts[phase] != recorded_prefix {
                return Err(corrupt());
            }
        }
        Ok(())
    }

    /// Test/benchmark observability: whether a verified checkpoint drove this session.
    #[doc(hidden)]
    #[must_use]
    pub(super) fn checkpoint_verified(&self) -> bool {
        self.checkpoint_verified
    }

    /// Test/benchmark observability: whether a verified clean-close lifecycle
    /// selected the bounded readiness path. This is observation only and cannot
    /// select startup behavior.
    #[doc(hidden)]
    #[must_use]
    pub(super) fn clean_close_fast_path(&self) -> bool {
        self.clean_close_fast
    }

    /// Test/benchmark observability: whether this open took the ADR-0182
    /// bounded dirty path (journal-suffix replay plus bounded-root validation,
    /// no population walk). Observation only; it cannot select startup
    /// behavior.
    #[doc(hidden)]
    #[must_use]
    pub(super) fn bounded_dirty_startup_path(&self) -> bool {
        self.bounded_dirty_startup
    }

    /// Test/evidence observability: the total structural rows this session
    /// will walk before its exact end. The bounded startup paths report their
    /// bounded catalog totals; the complete path reports the full population
    /// total. Observation only.
    #[doc(hidden)]
    #[must_use]
    pub(super) fn structural_walk_total(&self) -> u64 {
        self.structural_total
    }

    /// Test/evidence observability: the COMMITS rows this session decoded
    /// into its startup command cache. The bounded startup paths report zero
    /// through the structural exact end. Observation only.
    #[doc(hidden)]
    #[must_use]
    pub(super) fn command_history_rows_decoded(&self) -> u64 {
        self.command_history_rows_decoded
    }

    /// Test/evidence observability: the complete walk's command history as
    /// (derived audit entries, retained bytes). The history holds only this
    /// fixed-size index, never decoded commands. Observation only.
    #[doc(hidden)]
    #[must_use]
    pub(super) fn command_history_retained(&self) -> (usize, usize) {
        (
            self.command_history.audit_entries(),
            self.command_history.retained_bytes(),
        )
    }

    /// Test/evidence observability: the peak bytes this session's
    /// recovery-owned structures held together (the historical evidence
    /// plan's locator index and the startup command cache). The bounded dirty
    /// path refuses above the compiled 64 MiB ceiling. Observation only.
    #[doc(hidden)]
    #[must_use]
    pub(super) fn recovery_owned_peak_bytes(&self) -> u64 {
        self.recovery_owned_peak_bytes
    }

    pub(super) fn note_recovery_owned_bytes(&mut self, plan_bytes: usize) {
        let held =
            u64::try_from(plan_bytes.saturating_add(self.command_audit_bytes)).unwrap_or(u64::MAX);
        self.recovery_owned_peak_bytes = self.recovery_owned_peak_bytes.max(held);
    }

    /// Test/operator observability: which precondition declined the ADR-0157
    /// bounded path on this open, or `None` when the bounded path was admitted.
    ///
    /// Answers "why is this start slow?" from the session itself, before the
    /// complete validation pass runs. Nine preconditions previously shared one
    /// observable — a very slow start — which is how this cost was misdiagnosed
    /// twice. This is observation only and cannot select startup behavior.
    #[doc(hidden)]
    #[must_use]
    pub(super) fn clean_close_declined_reason(&self) -> Option<&'static str> {
        self.clean_close_declined_reason
            .map(crate::clean_close::CleanCloseDeclineReason::as_str)
    }

    /// Test observability: rows actually inspected by deterministic sample windows.
    #[doc(hidden)]
    #[must_use]
    pub(super) fn sampled_window_rows_inspected(&self) -> u64 {
        self.sampled_window_rows_inspected
    }

    /// Test/operator observability: why a present checkpoint was ignored, if it was.
    #[doc(hidden)]
    #[must_use]
    pub(super) fn checkpoint_ignored_reason(&self) -> Option<&'static str> {
        self.checkpoint_ignored_reason
            .map(crate::validated_prefix::CheckpointIgnoreReason::as_str)
    }

    /// Benchmark observability: COMMITS rows actually walked after the checkpoint
    /// bound S (0 on the full-validation path or an empty suffix).
    #[doc(hidden)]
    #[must_use]
    pub(super) fn checkpoint_suffix_commits(&self) -> u64 {
        self.walked_suffix_counts[10]
    }

    /// Test/benchmark observability: AUDIT rows decoded to witness reactive
    /// publications. This is the non-command row count from the one physical
    /// `AUDIT` pass shared by all publication checks, never one pass per row.
    #[doc(hidden)]
    #[must_use]
    pub(super) fn reactive_publication_audit_decodes(&self) -> u64 {
        self.reactive_publication_audit_decodes
    }

    /// Test observability: terminal `ExecutionFailed` rows this walk censused.
    #[doc(hidden)]
    #[must_use]
    pub(super) fn terminal_execution_failure_rows(&self) -> u64 {
        self.terminal_execution_failure_rows
    }
    pub(super) fn open_structural_phase_range(
        &self,
        phase: usize,
        table: ReadOnlyTable<&'static [u8], &'static [u8]>,
    ) -> Result<Range<'static, &'static [u8], &'static [u8]>, StorageError> {
        let Some(checkpoint) = self.checkpoint.as_ref() else {
            return table.range::<&[u8]>(..).map_err(precommit_storage_error);
        };
        let s = checkpoint.checkpoint_commit_sequence;
        let audit_bound = checkpoint.audit_sequence_bound;
        match phase {
            10 if s > 0 => {
                let sequence = CommitSequence::new(s).ok_or_else(invariant)?;
                let key = keys::encode_application_sequence_key(sequence);
                table
                    .range::<&[u8]>((Excluded(key.as_slice()), Unbounded))
                    .map_err(precommit_storage_error)
            }
            12 | 14 | 15 if s > 0 => {
                if let Some(lower) = crate::validated_prefix::first_event_key_after(s) {
                    table
                        .range::<&[u8]>((Included(lower.as_slice()), Unbounded))
                        .map_err(precommit_storage_error)
                } else {
                    table
                        .range::<&[u8]>((Excluded(&[0xff_u8; 12][..]), Unbounded))
                        .map_err(precommit_storage_error)
                }
            }
            21 if audit_bound > 0 => {
                let key = keys::encode_audit_key(
                    riffdb_types::AdministrationSequence::new(audit_bound).ok_or_else(invariant)?,
                );
                table
                    .range::<&[u8]>((Excluded(key.as_slice()), Unbounded))
                    .map_err(precommit_storage_error)
            }
            _ => table.range::<&[u8]>(..).map_err(precommit_storage_error),
        }
    }

    pub(super) fn inspect_structural_forward(
        &mut self,
        position: u64,
    ) -> Result<Option<StructuralFinding>, StorageError> {
        // Original physical rows retain their exact phase/index mapping. The
        // separately counted Source suffix never masquerades as a table row.
        if position >= self.source_component_start {
            return self.inspect_source_commitment_work(position);
        }
        // Positions are always requested in strictly ascending order by the page loop.
        if position == 0 {
            if self.clean_close_fast {
                return Ok(None);
            }
            if self.bounded_dirty_startup {
                // ADR-0182 Decision 1: no population-sized table is enumerated
                // before readiness, so the dirty header proves only the bounded
                // header facts and never builds the command or publication
                // caches. Gap-free administration continuity and the
                // last-activation reciprocity stay with the background scrub.
                let transaction = self.validation_read.as_ref().ok_or_else(invariant)?;
                return inspect_header(transaction, self.database_id, HeaderProof::BoundedDirty);
            }
            // Allocator continuity and the later command/audit phases need the
            // same exact command graph. Build it once (suffix-only under a
            // validated checkpoint) and share it instead of decoding every
            // command segment again in the header proof.
            self.ensure_command_cache()?;
            self.ensure_publication_audit_cache()?;
            let transaction = self.validation_read.as_ref().ok_or_else(invariant)?;
            return inspect_header(
                transaction,
                self.database_id,
                HeaderProof::Complete {
                    checkpoint: self.checkpoint.as_ref(),
                    command_audits: &self.command_history,
                    publication_audits: self.publication_audits.as_ref().ok_or_else(invariant)?,
                },
            );
        }
        if self.structural_cursors.is_none() {
            self.structural_cursors = Some(StructuralCursors {
                phase: 0,
                consumed_in_phase: 0,
                next_commit_sequence: None,
                commit_sequence_initialized: false,
                meta: None,
                bytes: None,
            });
        }
        let raw = self.next_structural_row_raw()?;
        let (phase, index, key, value) = raw.parts();
        // Sanity: absolute position maps to this phase/index.
        let mut relative = position - 1;
        let mut expected_phase = None;
        for (phase_idx, count) in self
            .structural_counts
            .iter()
            .chain(self.additive_structural_counts.iter())
            .copied()
            .enumerate()
        {
            if relative < count {
                expected_phase = Some(phase_idx);
                break;
            }
            relative = relative.checked_sub(count).ok_or_else(invariant)?;
        }
        if Some(phase) != expected_phase || index != relative {
            return Err(invariant());
        }
        if phase >= source_validation::FIRST_PHASE {
            return source_validation::inspect(
                &self.shared,
                self.validation_read.as_ref().ok_or_else(invariant)?,
                self.database_id,
                phase,
                key,
                value,
            );
        }
        if phase == 0 {
            let key = std::str::from_utf8(key).map_err(|_| corrupt())?;
            return Ok(self
                .shared
                .inspect_scan_meta_row(key, value, self.database_id));
        }
        // Both bounded startup paths inspect the catalog rows self-contained;
        // the activation-audit reciprocity is the complete path's and the
        // scrub's, since it reads the whole administration stream.
        let bounded = self.clean_close_fast || self.bounded_dirty_startup;
        if bounded && phase == 1 {
            return inspect_clean_close_bundle_row(key, value);
        }
        if bounded && phase == 2 {
            return inspect_clean_close_active_catalog_row(
                self.validation_read.as_ref().ok_or_else(invariant)?,
                key,
                value,
            );
        }
        if phase == 5 {
            return self.inspect_entity_row_with_chains(key, value);
        }
        if phase == 28 {
            return self.inspect_reactive_module_row_with_witness(key, value);
        }
        if phase == 8 {
            return self.inspect_terminal_row_with_census(key, value);
        }
        if let Some(checkpoint) = self.checkpoint.as_ref()
            && crate::validated_prefix::skip_inspect_for_prefix_row(
                phase,
                key,
                checkpoint.checkpoint_commit_sequence,
                checkpoint.audit_sequence_bound,
            )
        {
            // Counting skip-walk: every classified prefix row is tallied and
            // verified against the recorded count at ExactEnd, so a vanished
            // below-S row in these full-walk tables still fails closed.
            self.walked_prefix_counts[phase] = self.walked_prefix_counts[phase].saturating_add(1);
            return Ok(None);
        }
        if phase == 10 {
            // Successful outcomes may be segment-owned with no physical
            // IDEMPOTENCY row, so phase 8 is not guaranteed to build this
            // reciprocal cache before the command-authority walk.
            self.ensure_command_cache()?;
            self.walked_suffix_counts[phase] = self.walked_suffix_counts[phase].saturating_add(1);
            let needs_initial = !self
                .structural_cursors
                .as_ref()
                .ok_or_else(invariant)?
                .commit_sequence_initialized;
            if needs_initial {
                let retained_floor = self.checkpoint.as_ref().map_or_else(
                    || {
                        self.validation_read
                            .as_ref()
                            .ok_or_else(invariant)
                            .and_then(retention_watermark_sequence)
                    },
                    |checkpoint| Ok(checkpoint.checkpoint_commit_sequence),
                )?;
                let expected = retained_floor.checked_add(1).and_then(CommitSequence::new);
                let cursors = self.structural_cursors.as_mut().ok_or_else(invariant)?;
                cursors.next_commit_sequence = expected;
                cursors.commit_sequence_initialized = true;
            }
            let expected = self
                .structural_cursors
                .as_ref()
                .ok_or_else(invariant)?
                .next_commit_sequence;
            let Some(expected) = expected else {
                return Ok(Some(authoritative(
                    StructuralFindingCode::SequenceDiscontinuity,
                )));
            };
            let transaction = self.validation_read.as_ref().ok_or_else(invariant)?;
            let (finding, last) = inspect_commit_row(
                transaction,
                &self.command_history,
                &self.bundles,
                &self.binding_bundles,
                expected,
                key,
                value,
            )?;
            self.structural_cursors
                .as_mut()
                .ok_or_else(invariant)?
                .next_commit_sequence = last.and_then(CommitSequence::checked_next);
            return Ok(finding);
        }
        if phase == 11 {
            let transaction = self.validation_read.as_ref().ok_or_else(invariant)?;
            return inspect_provenance_row(
                transaction,
                &self.command_history,
                &self.binding_bundles,
                key,
                value,
            );
        }
        if phase == 12 {
            let transaction = self.validation_read.as_ref().ok_or_else(invariant)?;
            return inspect_event_row(transaction, &self.command_history, key, value);
        }
        if phase == 13 {
            let transaction = self.validation_read.as_ref().ok_or_else(invariant)?;
            return inspect_event_route_row(transaction, &self.command_history, key, value);
        }
        if phase == 14 {
            let transaction = self.validation_read.as_ref().ok_or_else(invariant)?;
            return inspect_outbox_row(transaction, &self.command_history, key, value);
        }
        if phase == 15 {
            return inspect_outbox_status_row(
                self.validation_read.as_ref().ok_or_else(invariant)?,
                &self.command_history,
                key,
                value,
            );
        }
        if phase == 21 {
            self.ensure_command_cache()?;
        }
        if phase == 21
            && let Some(finding) = inspect_cached_command_audit_row(
                self.validation_read.as_ref().ok_or_else(invariant)?,
                &self.command_history,
                keys::decode_audit_key(key)
                    .map(|sequence| sequence.get().saturating_sub(1))
                    .unwrap_or(index),
                key,
                value,
            )?
        {
            self.walked_suffix_counts[phase] = self.walked_suffix_counts[phase].saturating_add(1);
            return Ok(finding);
        }
        let inspect_index = match phase {
            21 => keys::decode_audit_key(key)
                .map(|sequence| sequence.get().saturating_sub(1))
                .unwrap_or(index),
            _ => index,
        };
        if crate::validated_prefix::is_range_skipped_phase(phase) {
            self.walked_suffix_counts[phase] = self.walked_suffix_counts[phase].saturating_add(1);
        }
        let transaction = self.validation_read.as_ref().ok_or_else(invariant)?;
        let context = StructuralInspectContext {
            engine: &self.shared,
            inputs: &self.inputs,
            segments: &self.segments,
            binding_bundles: &self.binding_bundles,
            publication_audits: self.publication_audits.as_ref().ok_or_else(invariant)?,
            database_id: self.database_id,
        };
        let finding =
            inspect_table_row_from_bytes(transaction, &context, phase, inspect_index, key, value)?;
        Ok(finding)
    }

    pub(super) fn ensure_command_cache(&mut self) -> Result<(), StorageError> {
        if self.command_cache_built {
            return Ok(());
        }
        let floor = self
            .checkpoint
            .as_ref()
            .map_or(0, |checkpoint| checkpoint.checkpoint_commit_sequence);
        let history = self.build_command_history_with_companions(floor)?;
        self.command_audit_bytes = history.retained_bytes();
        self.command_history_rows_decoded = history.rows_decoded();
        self.command_history = history;
        let plan_bytes = self
            .historical_plan
            .as_ref()
            .map_or(0, |plan| plan.retained_bytes);
        self.note_recovery_owned_bytes(plan_bytes);
        self.command_cache_built = true;
        Ok(())
    }

    /// The command history, built in one `COMMITS` pass that also builds the
    /// entity chains and reads the historical plan's commit plans when this
    /// session still needs them (ADR-0287). Each row is decoded once for all
    /// three. The history's refusals return here as they always did; the
    /// companions' refusals are held for the point at which their own passes
    /// would have returned them.
    pub(super) fn build_command_history_with_companions(
        &mut self,
        floor: u64,
    ) -> Result<command_history::CommandHistory, StorageError> {
        let bounded = self.clean_close_fast || self.bounded_dirty_startup;
        let collect_plans = !bounded && self.historical_plan.is_none();
        // The checkpoint seed is consumed once, exactly as the ENTITIES-phase
        // build would consume it; without it that build reports the invariant.
        let entity_seed = if self.entity_chains.is_some() || self.prebuilt_entity_chains.is_some() {
            None
        } else {
            match self.checkpoint.as_mut() {
                Some(checkpoint) => checkpoint
                    .entity_heads_at_s
                    .take()
                    .map(|heads| Some((checkpoint.checkpoint_commit_sequence, heads))),
                None => Some(None),
            }
        };
        let live_entity_count = self.full_structural_counts.get(5).copied().unwrap_or(0);
        let transaction = self.validation_read.as_ref().ok_or_else(invariant)?;
        let (history, entity, plans) = single_commit_pass(
            transaction,
            floor,
            entity_seed.map(|seed| (live_entity_count, seed)),
            collect_plans,
        )?;
        self.prebuilt_entity_chains = entity;
        self.prebuilt_commit_plans = plans;
        Ok(history)
    }

    pub(super) fn ensure_publication_audit_cache(&mut self) -> Result<(), StorageError> {
        if self.publication_audits.is_some() {
            return Ok(());
        }
        let transaction = self.validation_read.as_ref().ok_or_else(invariant)?;
        let cache = build_publication_audit_cache_for_owner(transaction, &self.shared)?;
        self.reactive_publication_audit_decodes = cache.decoded_rows;
        self.publication_audits = Some(cache);
        Ok(())
    }

    pub(super) fn ensure_entity_chains_built(&mut self) -> Result<(), StorageError> {
        if self.entity_chains.is_some() {
            return Ok(());
        }
        if let Some(prebuilt) = self.prebuilt_entity_chains.take() {
            self.entity_chains = Some(prebuilt?);
            return Ok(());
        }
        let transaction = self.validation_read.as_ref().ok_or_else(invariant)?;
        let live_entity_count = self.full_structural_counts.get(5).copied().unwrap_or(0);
        let chain_head_count = transaction
            .open_table(crate::layout::ENTITY_CHAIN_HEADS)
            .map_err(table_error)?
            .len()
            .map_err(precommit_storage_error)?;
        let entity_identity_count = live_entity_count.max(chain_head_count);
        // Under a verified checkpoint the genesis COMMITS walk is replaced by
        // seeding from the fingerprint-verified exact entity heads at S and
        // advancing across the suffix only. The seed map is consumed exactly
        // once; a second build attempt under a checkpoint is an invariant error.
        let seed = match self.checkpoint.as_mut() {
            Some(checkpoint) => Some((
                checkpoint.checkpoint_commit_sequence,
                checkpoint.entity_heads_at_s.take().ok_or_else(invariant)?,
            )),
            None => None,
        };
        self.entity_chains = Some(build_entity_chains(
            transaction,
            entity_identity_count,
            seed,
        )?);
        Ok(())
    }

    pub(super) fn queue_entity_orphan_findings(&mut self) {
        let Some(state) = self.entity_chains.as_mut() else {
            return;
        };
        if state.orphans_queued {
            return;
        }
        state.orphans_queued = true;
        // Detail findings are bounded and carry no identity (StructuralFinding
        // has only scope+code). Emit at most CAP detail copies, plus one
        // aggregate when the detail list is truncated — either unconsumed+
        // reported-orphans exceed CAP, or the overflow list itself filled and
        // more map-capacity rejects were dropped (NEW-8: not on bare overflow
        // with a short detail list). Never mass-fail intact entity rows.
        let unconsumed = state.chains.values().filter(|c| !c.consumed).count();
        let detail_total = unconsumed.saturating_add(state.orphan_targets.len());
        let detail_emitted = detail_total.min(ENTITY_CHAIN_ORPHAN_REPORT_CAP);
        for _ in 0..detail_emitted {
            self.pending_entity_orphan_findings
                .push_back(authoritative(StructuralFindingCode::CrossLinkMismatch));
        }
        let detail_truncated = detail_total > ENTITY_CHAIN_ORPHAN_REPORT_CAP;
        let overflow_list_truncated =
            state.overflow && state.orphan_targets.len() >= ENTITY_CHAIN_ORPHAN_REPORT_CAP;
        if detail_truncated || overflow_list_truncated {
            self.pending_entity_orphan_findings
                .push_back(authoritative(StructuralFindingCode::CrossLinkMismatch));
        }
    }

    /// Marks the chain for one ENTITIES row as consumed (O(log N) map lookup).
    pub(super) fn mark_entity_row_seen(&mut self, key: &riffdb_types::EntityKey) {
        let Some(state) = self.entity_chains.as_mut() else {
            return;
        };
        mark_entity_chain_consumed(&mut state.chains, key);
    }

    /// Judges one retained `REACTIVE_MODULES` row against the session's shared
    /// publication evidence, built once before header validation.
    pub(super) fn inspect_reactive_module_row_with_witness(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<StructuralFinding>, StorageError> {
        let transaction = self.validation_read.as_ref().ok_or_else(invariant)?;
        inspect_reactive_module_row_cached(
            transaction,
            self.publication_audits.as_ref().ok_or_else(invariant)?,
            key,
            value,
        )
    }

    /// Inspects one IDEMPOTENCY row and censuses its terminal class.
    ///
    /// Dispatched here rather than from `inspect_table_row_from_bytes` because
    /// the checkpointed prefix skip and the terminal execution-failure census are
    /// two questions about one decode: classifying twice would add a second full
    /// protobuf pass over the largest-decode table at every open. The skip
    /// predicate is unchanged — `TerminalRowClass::is_prefix_outcome` is the same
    /// test `skip_inspect_for_prefix_row` spelled out for phase 8 — and the
    /// findings this phase produces are unchanged.
    ///
    /// The census counts EVERY `ExecutionFailed` row in the table on both paths:
    /// the skip admits only `StoredOutcome` rows at or below S, so no execution
    /// failure is ever skipped, on the fast path or the full walk.
    pub(super) fn inspect_terminal_row_with_census(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<StructuralFinding>, StorageError> {
        let record = decoded(codec::decode_idempotency_record_v1(value));
        let class = record
            .as_ref()
            .map(crate::validated_prefix::TerminalRowClass::from_record)
            .unwrap_or(crate::validated_prefix::TerminalRowClass::Undecodable);
        if class == crate::validated_prefix::TerminalRowClass::Failed {
            self.terminal_execution_failure_rows =
                self.terminal_execution_failure_rows.saturating_add(1);
        }
        if let Some(checkpoint) = self.checkpoint.as_ref()
            && class.is_prefix_outcome(checkpoint.checkpoint_commit_sequence)
        {
            // Counting skip-walk: every classified prefix row is tallied and
            // verified against the recorded count at ExactEnd, so a vanished
            // below-S row in this full-walk table still fails closed.
            self.walked_prefix_counts[8] = self.walked_prefix_counts[8].saturating_add(1);
            return Ok(None);
        }
        // Only a suffix outcome or an execution-failure row needs the complete
        // command-authority graph. Building it before the checkpoint test makes
        // every clean restart decode and clone the entire retained command
        // history merely to skip already-proved prefix outcomes.
        self.ensure_command_cache()?;
        let transaction = self.validation_read.as_ref().ok_or_else(invariant)?;
        let finding = inspect_terminal_row(
            transaction,
            &self.command_history,
            &self.binding_bundles,
            &self.inputs,
            self.database_id,
            key,
            record,
        )?;
        Ok(finding)
    }

    pub(super) fn inspect_entity_row_with_chains(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<StructuralFinding>, StorageError> {
        self.ensure_entity_chains_built()?;
        let Ok(decoded_key) = keys::decode_entity_key(key) else {
            // Raw key undecodable: cannot reconstruct a target (ledgered).
            return Ok(Some(authoritative(StructuralFindingCode::MalformedRecord)));
        };
        // Mark before any semantic early-return (NEW-2), O(log N) (NEW-6).
        self.mark_entity_row_seen(&decoded_key);

        let record = match decoded(codec::decode_entity_record_v1(value)) {
            Ok(value) => value,
            Err(code) => {
                self.maybe_queue_orphans_after_entity_row();
                return Ok(Some(authoritative(code)));
            }
        };
        // Also mark by decoded target (handles key/target mismatch rows).
        if let Some(state) = self.entity_chains.as_mut()
            && let Some(chain) = state.chains.get_mut(record.target())
        {
            chain.consumed = true;
        }
        if record.target().key() != &decoded_key {
            self.maybe_queue_orphans_after_entity_row();
            return Ok(Some(authoritative(
                StructuralFindingCode::CrossLinkMismatch,
            )));
        }
        let transaction = self.validation_read.as_ref().ok_or_else(invariant)?;
        if !self.binding_bundles.contains(record.schema_binding()) {
            self.maybe_queue_orphans_after_entity_row();
            return Ok(Some(authoritative(StructuralFindingCode::MissingCrossLink)));
        }
        // Per-entity verdict only — overflow/orphans do not mask intact rows.
        let history_ok = {
            let state = self.entity_chains.as_mut().ok_or_else(invariant)?;
            let migrations = &state.migrations;
            match state.chains.get_mut(record.target()) {
                Some(chain) => {
                    while chain.intact && chain.migration_cursor < migrations.len() {
                        match advance_entity_chain_through_migration(
                            transaction,
                            migrations,
                            record.target(),
                            chain,
                            None,
                        )? {
                            MigrationAdvance::Advanced | MigrationAdvance::Skipped => {}
                            MigrationAdvance::Invalid => chain.intact = false,
                            MigrationAdvance::Exhausted => break,
                        }
                    }
                    entity_row_matches_chain(chain, &record)
                }
                // No retained commit references this target. Under a pruned
                // floor its whole history is tombstone-covered: the row
                // validates by fingerprint-of-current shape (decode + binding
                // cross-links above), not by replay (ADR-0085 A2). Unpruned,
                // an unreferenced ENTITIES row remains corruption.
                None => state.pruned_floor > 0,
            }
        };
        self.maybe_queue_orphans_after_entity_row();
        Ok((!history_ok).then(|| authoritative(StructuralFindingCode::MissingCrossLink)))
    }

    pub(super) fn maybe_queue_orphans_after_entity_row(&mut self) {
        let entity_count = self.structural_counts.get(5).copied().unwrap_or(0);
        let consumed_in_phase = self
            .structural_cursors
            .as_ref()
            .map(|c| c.consumed_in_phase)
            .unwrap_or(0);
        if entity_count > 0 && consumed_in_phase == entity_count {
            self.queue_entity_orphan_findings();
        }
    }

    pub(super) fn next_structural_row_raw(
        &mut self,
    ) -> Result<source_validation_admission::RawStructuralRowV1, StorageError> {
        loop {
            let phase = {
                let cursors = self.structural_cursors.as_ref().ok_or_else(invariant)?;
                cursors.phase
            };
            if phase >= STARTUP_TABLE_COUNT {
                return Err(invariant());
            }
            let configured_count = self
                .structural_counts
                .iter()
                .chain(self.additive_structural_counts.iter())
                .nth(phase)
                .copied()
                .ok_or_else(invariant)?;
            if (self.clean_close_fast || phase >= source_validation::FIRST_PHASE)
                && configured_count == 0
            {
                let cursors = self.structural_cursors.as_mut().ok_or_else(invariant)?;
                cursors.meta = None;
                cursors.bytes = None;
                cursors.phase = cursors.phase.checked_add(1).ok_or_else(invariant)?;
                cursors.consumed_in_phase = 0;
                continue;
            }
            if phase == 0 {
                let need_open = self
                    .structural_cursors
                    .as_ref()
                    .ok_or_else(invariant)?
                    .meta
                    .is_none();
                if need_open {
                    let transaction = self.validation_read.as_ref().ok_or_else(invariant)?;
                    let table: ReadOnlyTable<&'static str, &'static [u8]> =
                        transaction.open_table(META).map_err(table_error)?;
                    let range = table.range::<&str>(..).map_err(precommit_storage_error)?;
                    self.structural_cursors.as_mut().ok_or_else(invariant)?.meta = Some(range);
                }
                let next = {
                    let cursors = self.structural_cursors.as_mut().ok_or_else(invariant)?;
                    cursors
                        .meta
                        .as_mut()
                        .ok_or_else(invariant)?
                        .next()
                        .transpose()
                        .map_err(precommit_storage_error)?
                };
                if let Some((key, value)) = next {
                    let cursors = self.structural_cursors.as_mut().ok_or_else(invariant)?;
                    let index = cursors.consumed_in_phase;
                    cursors.consumed_in_phase = cursors
                        .consumed_in_phase
                        .checked_add(1)
                        .ok_or_else(limit_exceeded)?;
                    let (key, value) = copy_structural_row(key.value().as_bytes(), value.value())?;
                    return Ok(source_validation_admission::RawStructuralRowV1::existing(
                        0, index, key, value,
                    ));
                }
                let cursors = self.structural_cursors.as_mut().ok_or_else(invariant)?;
                cursors.meta = None;
                cursors.phase = 1;
                cursors.consumed_in_phase = 0;
                continue;
            }
            let need_open = self
                .structural_cursors
                .as_ref()
                .ok_or_else(invariant)?
                .bytes
                .is_none();
            if need_open {
                // ENTITIES phase entry: build chains even when the table is empty so
                // commit-referenced orphans are still validated.
                if phase == 5 {
                    self.ensure_entity_chains_built()?;
                }
                let transaction = self.validation_read.as_ref().ok_or_else(invariant)?;
                let table: ReadOnlyTable<&'static [u8], &'static [u8]> = match phase {
                    1 => transaction
                        .open_table(CONTRACT_BUNDLES)
                        .map_err(table_error)?,
                    2 => transaction
                        .open_table(CATALOG_ACTIVE)
                        .map_err(table_error)?,
                    3 => transaction.open_table(QUERY_MODULES).map_err(table_error)?,
                    4 => transaction
                        .open_table(QUERY_MODULE_ACTIVE)
                        .map_err(table_error)?,
                    5 => transaction.open_table(ENTITIES).map_err(table_error)?,
                    6 => transaction
                        .open_table(SECONDARY_INDEXES)
                        .map_err(table_error)?,
                    7 => transaction.open_table(INDEX_EPOCHS).map_err(table_error)?,
                    8 => transaction.open_table(IDEMPOTENCY).map_err(table_error)?,
                    9 => transaction
                        .open_table(IDEMPOTENCY_PENDING)
                        .map_err(table_error)?,
                    10 => transaction.open_table(COMMITS).map_err(table_error)?,
                    11 => transaction.open_table(PROVENANCE).map_err(table_error)?,
                    12 => transaction.open_table(EVENTS).map_err(table_error)?,
                    13 => transaction.open_table(EVENT_ROUTES).map_err(table_error)?,
                    14 => transaction.open_table(OUTBOX).map_err(table_error)?,
                    15 => transaction.open_table(OUTBOX_STATUS).map_err(table_error)?,
                    16 => transaction
                        .open_table(PROJECTION_STATE)
                        .map_err(table_error)?,
                    17 => transaction
                        .open_table(PROJECTION_FRONTIER)
                        .map_err(table_error)?,
                    18 => transaction
                        .open_table(PROJECTION_APPLIED)
                        .map_err(table_error)?,
                    19 => transaction.open_table(CAPABILITIES).map_err(table_error)?,
                    20 => transaction
                        .open_table(CAPABILITY_TOKENS)
                        .map_err(table_error)?,
                    21 => transaction.open_table(AUDIT).map_err(table_error)?,
                    22 => transaction
                        .open_table(AUDIT_BY_REQUEST)
                        .map_err(table_error)?,
                    23 => transaction
                        .open_table(CONTRACT_MIGRATION_JOURNAL)
                        .map_err(table_error)?,
                    24 => transaction
                        .open_table(CONTRACT_MIGRATIONS)
                        .map_err(table_error)?,
                    25 => transaction
                        .open_table(CONTRACT_WRITE_RETIREMENTS)
                        .map_err(table_error)?,
                    26 => transaction
                        .open_table(RETIRED_ENTITIES)
                        .map_err(table_error)?,
                    27 => transaction
                        .open_table(HISTORY_TOMBSTONES)
                        .map_err(table_error)?,
                    28 => transaction
                        .open_table(REACTIVE_MODULES)
                        .map_err(table_error)?,
                    29 => transaction
                        .open_table(EVENT_CONSUMERS)
                        .map_err(table_error)?,
                    30 => transaction
                        .open_table(EVENT_CONSUMER_DELIVERIES)
                        .map_err(table_error)?,
                    31 => transaction
                        .open_table(APPLICATION_INSTALLATION_CAMPAIGNS)
                        .map_err(table_error)?,
                    32 => transaction
                        .open_table(APPLICATION_EXPORT_OPERATIONS)
                        .map_err(table_error)?,
                    33 => transaction
                        .open_table(VECTOR_EVIDENCE)
                        .map_err(table_error)?,
                    34 => transaction
                        .open_table(VECTOR_OBSERVATIONS)
                        .map_err(table_error)?,
                    35 => transaction
                        .open_table(VECTOR_EVIDENCE_INDEX)
                        .map_err(table_error)?,
                    36 => transaction
                        .open_table(APPLICATION_EXPORT_PAGE_COMMITMENTS)
                        .map_err(table_error)?,
                    37..=40 => transaction
                        .open_table(source_validation::definition(
                            phase - source_validation::FIRST_PHASE,
                        )?)
                        .map_err(table_error)?,
                    _ => return Err(invariant()),
                };
                let range = self.open_structural_phase_range(phase, table)?;
                self.structural_cursors
                    .as_mut()
                    .ok_or_else(invariant)?
                    .bytes = Some(range);
            }
            let next = {
                let cursors = self.structural_cursors.as_mut().ok_or_else(invariant)?;
                cursors
                    .bytes
                    .as_mut()
                    .ok_or_else(invariant)?
                    .next()
                    .transpose()
                    .map_err(precommit_storage_error)?
            };
            if let Some((key, value)) = next {
                let cursors = self.structural_cursors.as_mut().ok_or_else(invariant)?;
                let index = cursors.consumed_in_phase;
                let phase = cursors.phase;
                cursors.consumed_in_phase = cursors
                    .consumed_in_phase
                    .checked_add(1)
                    .ok_or_else(limit_exceeded)?;
                if phase >= source_validation::FIRST_PHASE {
                    return source_validation_admission::RawStructuralRowV1::source(
                        &self.shared,
                        phase,
                        index,
                        key.value(),
                        value.value(),
                    );
                }
                let (key, value) = copy_structural_row(key.value(), value.value())?;
                return Ok(source_validation_admission::RawStructuralRowV1::existing(
                    phase, index, key, value,
                ));
            }
            let cursors = self.structural_cursors.as_mut().ok_or_else(invariant)?;
            cursors.bytes = None;
            let leaving = cursors.phase;
            cursors.phase = cursors.phase.checked_add(1).ok_or_else(invariant)?;
            cursors.consumed_in_phase = 0;
            // Phase 5 is ENTITIES: queue unconsumed/orphan findings, then drop the map.
            if leaving == 5 {
                self.queue_entity_orphan_findings();
                self.entity_chains = None;
            }
        }
    }
}
