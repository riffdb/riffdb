//! Columnar engine open, apply, query, checkpoint, and compact.

use std::path::PathBuf;
use std::sync::Arc;

use riffdb_storage_api::{
    AuthoritativePointReader, AuthoritativeScanReader, MAX_SCAN_PAGE_ENTRIES, StoredEntityRecordV1,
};
use riffdb_types::{FrontierPosition, ProjectionFrontier};

use crate::apply::{ApplyProgress, ApplyState};
use crate::checkpoint::{CheckpointDir, CheckpointError, ManifestV1};
use crate::definition::{DefinitionFingerprint, RegisteredDefinition};
use crate::error::ColumnarError;
use crate::hooks::ColumnarTestController;
use crate::outcome::{ColumnarOutcome, ProjectionBuilding, ProjectionReady};
use crate::query::{ColumnarQueryRequest, QueryResult, query_snapshot};
use crate::store::{
    ColumnarSnapshot, LiveRow, OrgKey, PrimaryKeyBytes, WorkingState, project_cells,
};

/// Bounded-page builder for one complete authoritative snapshot generation.
///
/// Pages remain private until [`Self::install`] atomically replaces an engine's
/// published state. A failed or interrupted build therefore cannot expose a
/// partial generation or advance its frontier.
pub struct ColumnarSnapshotRebuild {
    definition: RegisteredDefinition,
    working: WorkingState,
}

impl ColumnarSnapshotRebuild {
    /// Starts a private generation for one exact registered definition.
    #[must_use]
    pub fn new(definition: RegisteredDefinition) -> Self {
        Self {
            definition,
            working: WorkingState::default(),
        }
    }

    /// Adds one storage-bounded authoritative entity page.
    pub fn apply_page(&mut self, records: &[StoredEntityRecordV1]) -> Result<(), ColumnarError> {
        if records.len() > MAX_SCAN_PAGE_ENTRIES {
            return Err(ColumnarError::Integrity(
                "snapshot page exceeds storage bound",
            ));
        }
        for record in records {
            if record.target().entity_type_id() != self.definition.entity_type_id() {
                return Err(ColumnarError::Integrity("snapshot entity type mismatch"));
            }
            let (org_value, cells) = project_cells(
                record.fields().fields(),
                self.definition.projected_fields(),
                self.definition.org_scope_field(),
            )?;
            let org = OrgKey::from_value(&org_value)?;
            let key =
                PrimaryKeyBytes::from_entity_key_bytes(record.target().key().as_bytes().to_vec());
            let row = LiveRow {
                entity_version: record.entity_version(),
                cells,
            };
            if let Some(existing) = self
                .working
                .delta
                .get(&org)
                .and_then(|delta| delta.get(&key))
                && existing.entity_version != row.entity_version
            {
                return Err(ColumnarError::Integrity("duplicate snapshot entity"));
            }
            self.working.upsert_live(org, key, row);
        }
        Ok(())
    }

    /// Atomically publishes the complete private generation at its exact
    /// authoritative snapshot frontier.
    pub fn install(
        mut self,
        engine: &mut ColumnarEngine,
        frontier: FrontierPosition,
    ) -> Result<(), ColumnarError> {
        if self.definition.fingerprint() != engine.definition.fingerprint() {
            return Err(ColumnarError::Integrity("snapshot definition mismatch"));
        }
        self.working.processed = frontier;
        let published = Arc::new(self.working.to_snapshot(frontier));
        engine.apply = ApplyState {
            definition: self.definition,
            working: self.working,
            published,
            deferred: std::collections::BTreeMap::new(),
            #[cfg(test)]
            publish_observer: None,
        };
        engine.has_published = true;
        Ok(())
    }
}

/// Options for opening a columnar projection directory.
#[derive(Clone, Debug)]
pub struct OpenOptions {
    /// Storage directory for segment files and MANIFEST.
    pub directory: PathBuf,
    /// History incarnation of the authoritative database (ADR-0072 / ADR-0086).
    ///
    /// Bound into every [`ProjectionFrontier`] returned by this engine so a
    /// restore can never satisfy a pre-restore causal token.
    pub history_incarnation: u64,
}

impl OpenOptions {
    /// Opens under history incarnation `1` (bootstrap default).
    #[must_use]
    pub fn new(directory: impl Into<PathBuf>) -> Self {
        Self {
            directory: directory.into(),
            history_incarnation: 1,
        }
    }

    /// Sets the history incarnation used for frontier tokens.
    #[must_use]
    pub fn with_history_incarnation(mut self, history_incarnation: u64) -> Self {
        self.history_incarnation = history_incarnation;
        self
    }
}

/// Columnar projection engine (pull-based apply API).
pub struct ColumnarEngine {
    definition: RegisteredDefinition,
    apply: ApplyState,
    checkpoint: CheckpointDir,
    /// True after at least one successful publication (including empty at BeforeFirst after open with manifest).
    has_published: bool,
    /// Durable frontier from last successful checkpoint / open.
    durable_frontier: FrontierPosition,
    /// True once a MANIFEST describing this engine's durable state exists,
    /// either loaded at open or written by a checkpoint in this process.
    has_manifest: bool,
    /// History incarnation bound into published frontiers (immutable for engine life).
    history_incarnation: u64,
}

impl ColumnarEngine {
    /// Opens or creates a projection directory for `definition`.
    ///
    /// Fingerprint mismatch against an existing MANIFEST returns
    /// [`ColumnarError::Checkpoint`] with [`CheckpointError::FingerprintMismatch`];
    /// callers may map that to [`ColumnarOutcome::Invalid`].
    pub fn open(
        definition: RegisteredDefinition,
        options: OpenOptions,
    ) -> Result<Self, ColumnarError> {
        let checkpoint = CheckpointDir::new(options.directory)?;
        let mut apply = ApplyState::new(definition.clone());
        let mut has_published = false;
        let mut durable_frontier = FrontierPosition::BeforeFirst;
        let mut has_manifest = false;

        match checkpoint.load_manifest()? {
            None => {
                // Fresh directory: building until first publication after apply.
                // Sweep stray files from a checkpoint torn before the first
                // manifest rename (orphan segments, temp manifests) so a later
                // checkpoint can never collide with them.
                checkpoint.sweep_stray_files()?;
            }
            Some(manifest) => {
                if manifest.fingerprint != definition.fingerprint() {
                    return Err(ColumnarError::Checkpoint(
                        CheckpointError::FingerprintMismatch {
                            expected: definition.fingerprint(),
                            found: manifest.fingerprint,
                        },
                    ));
                }
                let working = checkpoint.open_from_manifest(&manifest, definition.fingerprint())?;
                // Visible regresses to durable; replay will catch up.
                apply.working = working;
                apply.published = Arc::new(apply.working.to_snapshot(manifest.durable_frontier));
                has_published = true;
                durable_frontier = manifest.durable_frontier;
                has_manifest = true;
            }
        }

        Ok(Self {
            definition,
            apply,
            checkpoint,
            has_published,
            durable_frontier,
            has_manifest,
            history_incarnation: options.history_incarnation,
        })
    }

    /// Installs a test controller for crash injection (never used in production).
    #[doc(hidden)]
    pub fn install_test_controller(&mut self, controller: ColumnarTestController) {
        self.checkpoint.install_controller(controller);
    }

    /// Registered definition.
    #[must_use]
    pub fn definition(&self) -> &RegisteredDefinition {
        &self.definition
    }

    /// History incarnation bound into frontiers returned by this engine.
    #[must_use]
    pub const fn history_incarnation(&self) -> u64 {
        self.history_incarnation
    }

    /// Published snapshot (may be empty before first publication).
    #[must_use]
    pub fn published_snapshot(&self) -> Arc<ColumnarSnapshot> {
        Arc::clone(&self.apply.published)
    }

    /// Processed frontier (may lead published during holdback).
    #[must_use]
    pub fn processed_frontier(&self) -> ProjectionFrontier {
        ProjectionFrontier::new(self.history_incarnation, self.apply.working.processed)
    }

    /// Published visible frontier.
    #[must_use]
    pub fn published_frontier(&self) -> ProjectionFrontier {
        ProjectionFrontier::new(
            self.history_incarnation,
            self.apply.published.visible_frontier,
        )
    }

    /// Durable checkpoint frontier.
    #[must_use]
    pub fn durable_frontier(&self) -> ProjectionFrontier {
        ProjectionFrontier::new(self.history_incarnation, self.durable_frontier)
    }

    /// Raw position of the published frontier (without incarnation wrapper).
    #[must_use]
    pub fn published_frontier_position(&self) -> FrontierPosition {
        self.apply.published.visible_frontier
    }

    /// Deferred-set size (observability for CP3).
    #[must_use]
    pub fn deferred_set_size(&self) -> usize {
        self.apply.deferred.len()
    }

    /// Cumulative segment-rewrite amplification since this engine was opened.
    #[must_use]
    pub const fn amplification(&self) -> crate::checkpoint::ColumnarAmplification {
        self.checkpoint.amplification()
    }

    /// Live row count across published segments and the working delta.
    ///
    /// Counts with repetition across segments, so it is an upper bound on
    /// distinct rows and an exact measure of per-checkpoint rewrite cost.
    #[must_use]
    pub fn resident_segment_rows(&self) -> u64 {
        self.apply
            .working
            .segments
            .iter()
            .map(|segment| segment.rows.len() as u64)
            .sum()
    }

    /// Pulls and applies all commits currently available from `reader`.
    pub fn apply_available(
        &mut self,
        reader: &(impl AuthoritativeScanReader + AuthoritativePointReader),
    ) -> Result<ApplyProgress, ColumnarError> {
        let progress = self.apply.apply_available(reader)?;
        if self.apply.published.visible_frontier != FrontierPosition::BeforeFirst
            || (progress.caught_up && self.apply.deferred.is_empty())
        {
            // First publication may remain BeforeFirst on empty log; mark ready
            // when we have published anything or caught up with empty deferred.
            if self.apply.published.visible_frontier > FrontierPosition::BeforeFirst
                || progress.caught_up
            {
                self.has_published = true;
            }
        }
        // Publication on empty log at BeforeFirst after catch-up.
        if progress.caught_up
            && self.apply.deferred.is_empty()
            && self.apply.published.visible_frontier == self.apply.working.processed
        {
            self.has_published = true;
        }
        Ok(progress)
    }

    /// Lifecycle status for the current published state vs `head`.
    #[must_use]
    pub fn outcome(&self, head: FrontierPosition) -> ColumnarOutcome {
        if !self.has_published {
            return ColumnarOutcome::Building(ProjectionBuilding {
                applied_through: ProjectionFrontier::new(
                    self.history_incarnation,
                    self.apply.working.processed,
                ),
                head: ProjectionFrontier::new(self.history_incarnation, head),
            });
        }
        ColumnarOutcome::Ready(ProjectionReady {
            snapshot: Arc::clone(&self.apply.published),
            frontier: self.published_frontier(),
            head: ProjectionFrontier::new(self.history_incarnation, head),
            result: None,
        })
    }

    /// Executes a query against the published snapshot.
    ///
    /// Thin wrapper over [`query_snapshot`] after the building-state check.
    pub fn query(&self, request: &ColumnarQueryRequest) -> Result<QueryResult, ColumnarError> {
        if !self.has_published {
            return Err(ColumnarError::InvalidState("projection is still building"));
        }
        query_snapshot(&self.definition, &self.apply.published, request).map_err(Into::into)
    }

    /// Checkpoints the race-free visible frontier to durable storage.
    ///
    /// Flushes delta into segment files (data fsync before manifest rename).
    ///
    /// # Errors
    ///
    /// Returns [`CheckpointError::HoldbackActive`] while a supersession
    /// holdback window is open (published behind processed or a non-empty
    /// deferred set): flushing the working delta then would persist
    /// applied-but-unpublished effects of a half-visible commit. Retry after
    /// the next apply pull applies the superseding commit.
    pub fn checkpoint(&mut self) -> Result<ManifestV1, ColumnarError> {
        self.ensure_no_holdback()?;
        let durable = self.apply.published.visible_frontier;
        // The worker forces a checkpoint on a poll cadence whether or not
        // anything changed. With an empty delta no segment is produced, the
        // retained inventory is unchanged, and the durable frontier has not
        // moved, so the manifest this would write is identical to the one
        // already on disk. Skip the write and its two fsyncs rather than
        // restating durable facts. Nothing that was durable becomes
        // non-durable: the skip is taken only when a manifest already records
        // exactly this state.
        if self.has_manifest
            && self.apply.working.delta.is_empty()
            && self.durable_frontier == durable
        {
            return Ok(self.checkpoint.unchanged_manifest(
                &self.apply.working,
                self.definition.fingerprint(),
                durable,
            ));
        }
        // Only checkpoint at published frontier (race-free by D4).
        let manifest = self.checkpoint.checkpoint(
            &mut self.apply.working,
            self.definition.fingerprint(),
            durable,
        )?;
        // After checkpoint, published snapshot should reflect emptied delta + new segments.
        self.apply.published = Arc::new(self.apply.working.to_snapshot(durable));
        self.durable_frontier = durable;
        self.has_manifest = true;
        Ok(manifest)
    }

    /// Compacts the working delta into segments at the frozen published frontier (D10).
    ///
    /// Result-invariant: logical query results at the frontier are unchanged.
    /// Each org is rewritten to one segment; superseded segment files are swept
    /// on the next open.
    ///
    /// # Errors
    ///
    /// Refuses with [`CheckpointError::HoldbackActive`] during a holdback
    /// window, exactly like [`Self::checkpoint`].
    pub fn compact(&mut self) -> Result<(), ColumnarError> {
        self.ensure_no_holdback()?;
        let frontier = self.apply.published.visible_frontier;
        let _ = self.checkpoint.checkpoint(
            &mut self.apply.working,
            self.definition.fingerprint(),
            frontier,
        )?;
        self.apply.published = Arc::new(self.apply.working.to_snapshot(frontier));
        self.durable_frontier = frontier;
        self.has_manifest = true;
        Ok(())
    }

    /// Refuses durability operations while published lags processed or a
    /// deferral is outstanding (the working delta then holds effects beyond
    /// the published frontier — flushing would persist a half-applied commit).
    fn ensure_no_holdback(&self) -> Result<(), ColumnarError> {
        let published = self.apply.published.visible_frontier;
        let processed = self.apply.working.processed;
        if published != processed || !self.apply.deferred.is_empty() {
            return Err(ColumnarError::Checkpoint(CheckpointError::HoldbackActive {
                published,
                processed,
                deferred: self.apply.deferred.len(),
            }));
        }
        Ok(())
    }

    /// Definition fingerprint.
    #[must_use]
    pub fn fingerprint(&self) -> DefinitionFingerprint {
        self.definition.fingerprint()
    }

    /// Storage directory.
    #[must_use]
    pub fn directory(&self) -> &std::path::Path {
        self.checkpoint.root()
    }
}
