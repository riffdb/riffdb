#![forbid(unsafe_code)]

//! Columnar projection engine core (ADR-0086 prototype).
//!
//! # Rebuildability
//!
//! Entity-row projections are **snapshot-rebuildable only** (ADR-0086 §1).
//! Commit records carry entity references (target, version, post-image hash)
//! without post-images. Row values are loaded from current authoritative entity
//! state via `AuthoritativePointReader::read_entity`, gated by
//! `CommittedEntityReferenceV2::matches`. Intermediate versions that raced
//! ahead of the commit being applied are never taken from current state at the
//! earlier sequence; they wait for their own commit (D4).
//!
//! # Central invariant — apply protocol (D4)
//!
//! Commits are consumed in total order from
//! `AuthoritativeScanReader::scan_commits`. For each projected entity
//! reference R in commit C:
//!
//! 1. Read the current entity record.
//! 2. If `R.matches(&record)`, apply that exact post-image into the working
//!    delta (upsert by primary key, store `entity_version`).
//! 3. If the live version is greater than `R.entity_version`, **do not** apply
//!    the newer record at C. Record the entity in a deferred set. The commit
//!    that produced the live version will apply it when reached.
//!
//! **Frontier rules:**
//!
//! - The *processed* position advances for every commit (including irrelevant
//!   ones).
//! - The *published* (visible) frontier advances to F only when the deferred
//!   set is empty after processing F — frontier holdback. Resolving a deferral
//!   jumps the published frontier forward over the held range in one atomic
//!   snapshot swap.
//! - Publication is an atomic swap of
//!   `Arc<ColumnarSnapshot { segments, delta, visible_frontier }>` **after** a
//!   commit's full effects are in the working state. Readers never observe a
//!   partially applied commit or a mid-holdback half-state.
//!
//! Projection state is derived and rebuildable. The commit log and entity state
//! are authoritative (AGENTS.md boundary #8).
//!
//! # Deletes and tombstones
//!
//! Deletes do not exist today: `EntityMutation` is Create | Replace only
//! (riffdb-storage-api), so a projected row can only ever be superseded by a
//! newer version of the same entity. Superseded-row masking therefore rests
//! entirely on the entity-version comparison in the merge path. When a delete
//! mutation lands in the storage API, tombstone masking must be reintroduced
//! here: segments are immutable, so deletes require masking rows at read/merge
//! time (a tombstone row state layered above segments), not physical removal.
//!
//! # What this crate is not
//!
//! - No public wire/RPC surface (CP2).
//! - No query grammar or parser (ADR-0087 grammar is out of CP1).
//! - No threads, tokio, timers, or runtime.
//! - No dependency on `riffdb-projection` or `riffdb-storage-redb`.

mod apply;
mod checkpoint;
mod definition;
mod engine;
mod error;
mod hnsw;
mod hooks;
mod identity;
mod manifest_v2;
pub mod nearest;
mod outcome;
mod query;
mod segment_v2;
mod store;

pub use apply::ApplyProgress;
#[doc(hidden)]
pub use apply::WorkerApplyOutcome;
pub use checkpoint::{CheckpointError, ColumnarAmplification, ManifestV1, SegmentInventoryEntry};
pub use definition::{
    COLUMNAR_LAYOUT_VERSION_V1, COLUMNAR_LAYOUT_VERSION_V2, ColumnarProjectionDefinition,
    DefinitionError, DefinitionFingerprint, LAYOUT_VERSION, ProviderDescriptorError,
    RegisteredDefinition, VectorAnnConfig, VectorProviderProfileV1,
};
pub use engine::{ColumnarEngine, ColumnarSnapshotRebuild, OpenOptions};
pub use error::{ColumnarError, StorageFailure};
pub use identity::{
    ColumnarDefinitionSemanticsV1, ColumnarIdentityError, ColumnarProjectionSpecV1,
    ColumnarSpecReplayLimitsV1, ColumnarVectorSpecExtensionV1,
    MAX_COLUMNAR_DEFINITION_SEMANTICS_V1_BYTES, MAX_COLUMNAR_PROJECTION_SPEC_PAYLOAD_V1_BYTES,
    MAX_COLUMNAR_VECTOR_EXTENSION_V1_BYTES,
};
pub use manifest_v2::{
    ColumnarManifestV2, ColumnarManifestV2Entry, MAX_COLUMNAR_MANIFEST_V2_BYTES,
    MAX_COLUMNAR_MANIFEST_V2_SEGMENTS,
};
pub use outcome::{
    ColumnarOutcome, DegradedReason, ProjectionBuilding, ProjectionDegraded, ProjectionInvalid,
    ProjectionLagging, ProjectionReady, ProjectionRebuilding, RebuildingReason,
    frontier_lag_sequences, lagging_for,
};
pub use query::{
    AggregateOp, AggregateValue, AnnExecutionStats, ColumnPredicate, ColumnarQueryRequest,
    GroupBySpec, NearestCandidate, NearestCandidateAdmission, NearestQueryAdmissionError,
    NearestQueryRequest, NearestQueryResult, NearestResultRow, NearestSearchKind, OrderSpec,
    QueryBudget, QueryError, QueryResult, QueryRow, QueryRows, SortDirection,
    nearest_query_snapshot, nearest_query_snapshot_with_admission, query_snapshot,
    query_snapshot_with_policy_admission,
};
pub use riffdb_types::ColumnarProjectionSourceV1;
pub use segment_v2::{
    COLUMNAR_ENCODING_REGISTRY_VERSION_V1, COLUMNAR_MANIFEST_FORMAT_VERSION_V1,
    COLUMNAR_MANIFEST_FORMAT_VERSION_V2, COLUMNAR_SEGMENT_FORMAT_VERSION_V2, MAX_SEGMENT_V2_BYTES,
    MAX_SEGMENT_V2_COLUMNS, MAX_SEGMENT_V2_ROWS, SegmentV2, SegmentV2Cell, SegmentV2Codec,
    SegmentV2Column, SegmentV2Error, SegmentV2Identity, SegmentV2LogicalType, SegmentV2Predicate,
    SegmentV2PruningDecision, SegmentV2SegmentId,
};
pub use store::{
    ColumnarSnapshot, LiveRow, MergedRow, OrgDelta, OrgKey, PrimaryKeyBytes, SegmentId,
    encode_org_scope_key,
};

#[doc(hidden)]
pub use hooks::{ColumnarTestBoundary, ColumnarTestController};
