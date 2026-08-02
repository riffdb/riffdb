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
use crate::store::ColumnarSnapshot;

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
        // Only checkpoint at published frontier (race-free by D4).
        let manifest = self.checkpoint.checkpoint(
            &mut self.apply.working,
            self.definition.fingerprint(),
            durable,
        )?;
        // After checkpoint, published snapshot should reflect emptied delta + new segments.
        self.apply.published = Arc::new(self.apply.working.to_snapshot(durable));
        self.durable_frontier = durable;
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
