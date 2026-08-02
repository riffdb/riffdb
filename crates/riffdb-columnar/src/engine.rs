//! Columnar engine open, apply, query, checkpoint, and compact.

use std::path::PathBuf;
use std::sync::Arc;

use riffdb_storage_api::{AuthoritativePointReader, AuthoritativeScanReader};
use riffdb_types::FrontierPosition;

use crate::apply::{ApplyProgress, ApplyState};
use crate::checkpoint::{CheckpointDir, CheckpointError, ManifestV1};
use crate::definition::{DefinitionFingerprint, RegisteredDefinition};
use crate::error::ColumnarError;
use crate::hooks::ColumnarTestController;
use crate::outcome::{ColumnarOutcome, ProjectionBuilding, ProjectionReady};
use crate::query::{ColumnarQueryRequest, QueryResult, execute_query};
use crate::store::{ColumnarSnapshot, WorkingState};

/// Options for opening a columnar projection directory.
#[derive(Clone, Debug)]
pub struct OpenOptions {
    /// Storage directory for segment files and MANIFEST.
    pub directory: PathBuf,
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

        match checkpoint.load_manifest()? {
            None => {
                // Fresh directory: building until first publication after apply.
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
            }
        }

        Ok(Self {
            definition,
            apply,
            checkpoint,
            has_published,
            durable_frontier,
        })
    }

    /// Installs a test controller for crash injection (never used in production).
    #[doc(hidden)]
    pub fn install_test_controller(&mut self, controller: ColumnarTestController) {
        self.checkpoint.install_controller(controller);
    }

    /// Test-only protocol neuters for falsifiability transcripts.
    #[doc(hidden)]
    pub fn test_set_skip_tombstone(&mut self, enabled: bool) {
        self.apply.test_skip_tombstone = enabled;
    }

    /// Test-only: publish snapshot mid-commit.
    #[doc(hidden)]
    pub fn test_set_publish_mid_commit(&mut self, enabled: bool) {
        self.apply.test_publish_mid_commit = enabled;
    }

    /// Test-only: drop holdback (apply raced-ahead immediately).
    #[doc(hidden)]
    pub fn test_set_skip_holdback(&mut self, enabled: bool) {
        self.apply.test_skip_holdback = enabled;
    }

    /// Test-only: write manifest before segment sync (overclaim).
    #[doc(hidden)]
    pub fn test_set_manifest_before_segment_sync(&mut self, enabled: bool) {
        self.checkpoint.test_manifest_before_segment_sync = enabled;
    }

    /// Registered definition.
    #[must_use]
    pub fn definition(&self) -> &RegisteredDefinition {
        &self.definition
    }

    /// Published snapshot (may be empty before first publication).
    #[must_use]
    pub fn published_snapshot(&self) -> Arc<ColumnarSnapshot> {
        Arc::clone(&self.apply.published)
    }

    /// Processed frontier (may lead published during holdback).
    #[must_use]
    pub fn processed_frontier(&self) -> FrontierPosition {
        self.apply.working.processed
    }

    /// Published visible frontier.
    #[must_use]
    pub fn published_frontier(&self) -> FrontierPosition {
        self.apply.published.visible_frontier
    }

    /// Durable checkpoint frontier.
    #[must_use]
    pub fn durable_frontier(&self) -> FrontierPosition {
        self.durable_frontier
    }

    /// Deferred-set size (observability for CP3).
    #[must_use]
    pub fn deferred_set_size(&self) -> usize {
        self.apply.deferred.len()
    }

    /// Test observability: live-row counts recorded at each publication.
    #[doc(hidden)]
    #[must_use]
    pub fn test_publish_row_counts(&self) -> &[usize] {
        &self.apply.test_publish_row_counts
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
        if !self.has_published
            && self.apply.published.visible_frontier == FrontierPosition::BeforeFirst
            && self.apply.working.processed == FrontierPosition::BeforeFirst
        {
            return ColumnarOutcome::Building(ProjectionBuilding {
                applied_through: self.apply.working.processed,
                head,
            });
        }
        if !self.has_published {
            return ColumnarOutcome::Building(ProjectionBuilding {
                applied_through: self.apply.working.processed,
                head,
            });
        }
        ColumnarOutcome::Ready(ProjectionReady {
            snapshot: Arc::clone(&self.apply.published),
            frontier: self.apply.published.visible_frontier,
            head,
            result: None,
        })
    }

    /// Executes a query against the published snapshot.
    pub fn query(&self, request: &ColumnarQueryRequest) -> Result<QueryResult, ColumnarError> {
        if !self.has_published {
            return Err(ColumnarError::InvalidState("projection is still building"));
        }
        execute_query(&self.definition, &self.apply.published, request).map_err(Into::into)
    }

    /// Checkpoints the race-free visible frontier to durable storage.
    ///
    /// Flushes delta into segment files (data fsync before manifest rename).
    pub fn checkpoint(&mut self) -> Result<ManifestV1, ColumnarError> {
        let durable = self.apply.published.visible_frontier;
        // Only checkpoint at published frontier (race-free by D4).
        let manifest = self.checkpoint.checkpoint(
            &mut self.apply.working,
            self.definition.fingerprint(),
            durable,
        )?;
        // After checkpoint, published snapshot should reflect emptied delta + new segments.
        self.apply.published = Arc::new(self.apply.working.to_snapshot(durable));
        self.durable_frontier = durable;
        self.has_published = true;
        Ok(manifest)
    }

    /// Compacts the working delta into segments at the frozen published frontier (D10).
    ///
    /// Result-invariant: logical query results at the frontier are unchanged.
    pub fn compact(&mut self) -> Result<(), ColumnarError> {
        let frontier = self.apply.published.visible_frontier;
        let _ = self.checkpoint.checkpoint(
            &mut self.apply.working,
            self.definition.fingerprint(),
            frontier,
        )?;
        self.apply.published = Arc::new(self.apply.working.to_snapshot(frontier));
        self.durable_frontier = frontier;
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

    /// Test helper: replace working state (not for production).
    #[doc(hidden)]
    pub fn test_working_mut(&mut self) -> &mut WorkingState {
        &mut self.apply.working
    }
}
