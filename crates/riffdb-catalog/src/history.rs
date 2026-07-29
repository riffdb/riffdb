//! Same-session exact-end historical catalog validation.

use std::collections::VecDeque;
use std::error::Error;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use riffdb_contract_ir::{
    CompatibilityClass, EntitySchema, IndexSchema, KeyPurpose, SchemaIr, UniqueKeySchema,
};
use riffdb_invariant::{EvaluationError, ExpressionValueSource, evaluate_expression};
use riffdb_storage_api::{
    EvidencePageLimit, HistoricalBundleEvidence, HistoricalEvidenceCursor, HistoricalEvidenceEnd,
    HistoricalEvidencePage, HistoricalPersistedKeyEvidenceV1, HistoricalSemanticEvidence,
    IndexMigrationCursor, IndexMigrationRowEvidence, IndexMigrationSemanticRow,
    IndexRangePrefixBuilder, IrOpaquePersistedKeyV1, MAX_INDEX_MIGRATION_PAGE_BYTES,
    MAX_INDEX_MIGRATION_PAGE_ENTRIES, OpenSessionId, StartupIndexMigrationPort, StorageError,
    StorageValueError, StoredEntityRecordV1, StoredIndexEntryV2, StructuralEvidenceSession,
    UniqueIndexTarget, UniqueOccupancyKind,
};
use riffdb_types::{
    CanonicalValue, ContractBundleHash, ContractLineage, ContractVersion, DatabaseId, FieldId,
    IndexId, PartitionKey,
};

use crate::lineage::{LineageBudget, LineageMaterializationProof};
use crate::{
    CatalogError, CatalogErrorKind, ValidatedContractBundle, validate_capability_partition,
    validate_successor_compatibility,
};

/// Process-local proof that every catalog history item was IR-validated to exact end.
///
/// Its constructor is private, it is not serializable, and it never crosses a
/// storage trait. WP-130 may only combine it with the matching structural handoff.
pub struct ValidatedCatalogHistory {
    database_id: DatabaseId,
    open_session_id: OpenSessionId,
    active: Option<ValidatedContractBundle>,
    lineage_proof: Option<Arc<LineageMaterializationProof>>,
    evidence_count: u64,
}

impl ValidatedCatalogHistory {
    /// Durable database identity bound to the exclusive startup session.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Process-local startup session identity.
    #[must_use]
    pub const fn open_session_id(&self) -> OpenSessionId {
        self.open_session_id
    }

    /// Checked active bundle, or absence before first deployment.
    #[must_use]
    pub const fn active(&self) -> Option<&ValidatedContractBundle> {
        self.active.as_ref()
    }

    /// Number of bounded evidence items consumed before exact end.
    #[must_use]
    pub const fn evidence_count(&self) -> u64 {
        self.evidence_count
    }

    /// Checks the exact database/open-session pair for readiness composition.
    #[must_use]
    pub fn matches(&self, database_id: DatabaseId, open_session_id: OpenSessionId) -> bool {
        self.database_id == database_id && self.open_session_id == open_session_id
    }
}

impl fmt::Debug for ValidatedCatalogHistory {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ValidatedCatalogHistory")
            .field("database_id", &self.database_id)
            .field("open_session_id", &self.open_session_id)
            .field("active", &self.active.as_ref().map(|_| "[CHECKED]"))
            .field(
                "lineage_proof",
                &self.lineage_proof.as_ref().map(|_| "[CHECKED]"),
            )
            .field("evidence_count", &self.evidence_count)
            .finish()
    }
}

/// Catalog proof paired with storage's unforgeable historical exact-end token.
pub struct CatalogHistoryValidation<E> {
    outcome: CatalogHistoryOutcome,
    historical_end: E,
}

/// Closed result of consuming the complete historical evidence stream.
pub enum CatalogHistoryOutcome {
    /// Every physical index row was current V2 and catalog-valid.
    Ready(ValidatedCatalogHistory),
    /// At least one checked V1 row exists; this value cannot become readiness.
    MigrationRequired(CatalogIndexMigrationContext),
}

/// Non-readiness catalog state paired with storage's migration capability.
pub struct CatalogIndexMigrationContext {
    database_id: DatabaseId,
    open_session_id: OpenSessionId,
    bindings: Vec<HistoricalBindingRef>,
    evidence_count: u64,
    next: IndexMigrationCursor,
    last_physical_key: Option<riffdb_types::IndexEntryKey>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct HistoricalBindingRef {
    lineage: ContractLineage,
    version: ContractVersion,
    bundle_hash: ContractBundleHash,
}

type BackendBrand<B> = PhantomData<fn(B) -> B>;

#[derive(Default)]
struct IndexMigrationPageLedgers {
    evidence: usize,
    instructions: usize,
}

impl IndexMigrationPageLedgers {
    fn push(
        &mut self,
        evidence_charge: usize,
        instruction_charge: usize,
    ) -> Result<(), StorageValueError> {
        let evidence = self
            .evidence
            .checked_add(evidence_charge)
            .ok_or(StorageValueError::SizeOverflow)?;
        let instructions = self
            .instructions
            .checked_add(instruction_charge)
            .ok_or(StorageValueError::SizeOverflow)?;
        if evidence > MAX_INDEX_MIGRATION_PAGE_BYTES
            || instructions > MAX_INDEX_MIGRATION_PAGE_BYTES
        {
            return Err(StorageValueError::LimitExceeded);
        }
        self.evidence = evidence;
        self.instructions = instructions;
        Ok(())
    }
}

/// Concrete backend port consumed only by the catalog-owned migration driver.
pub trait CatalogIndexMigrationBackend: StartupIndexMigrationPort + Sized {
    /// Dormant storage value returned only after catalog and storage reach exact end.
    type Output;

    /// Consumes one catalog-minted request into a bounded page or explicit exact end.
    fn read_index_migration_page(
        self,
        request: CatalogIndexMigrationScanRequest<Self>,
    ) -> Result<CatalogIndexMigrationScan<Self>, StorageError>;

    /// Consumes one catalog-minted row request into its exact retained bundle response.
    fn read_historical_bundle(
        self,
        request: CatalogIndexMigrationBundleRequest<Self>,
    ) -> Result<CatalogIndexMigrationBundleResponse<Self>, StorageError>;

    /// Applies one complete catalog-owned page atomically and returns applied type-state.
    fn apply_index_migration_batch(
        self,
        pending: CatalogIndexMigrationPendingBatch<Self>,
    ) -> Result<CatalogIndexMigrationApplied<Self>, StorageError>;

    /// Consumes exact catalog completion into the backend's dormant output.
    fn finish_index_migration(
        self,
        completion: CatalogIndexMigrationCompletion<Self>,
    ) -> Result<Self::Output, StorageError>;
}

/// Move-only catalog driver that is inseparably branded to one concrete backend type.
pub struct CatalogIndexMigrationDriver<B> {
    context: CatalogIndexMigrationContext,
    backend: B,
    _backend: BackendBrand<B>,
}

/// Catalog-only authority to request the next physical migration page.
pub struct CatalogIndexMigrationScanRequest<B> {
    cursor: IndexMigrationCursor,
    after: Option<riffdb_types::IndexEntryKey>,
    _backend: BackendBrand<B>,
}

/// Explicit consuming result of one catalog-authorized migration scan request.
pub enum CatalogIndexMigrationScan<B> {
    /// One nonempty bounded page retaining the concrete backend port.
    Page(CatalogIndexMigrationPage<B>),
    /// Exact physical end retaining the concrete backend port.
    ExactEnd(CatalogIndexMigrationExactEnd<B>),
}

/// One nonempty codec-checked page bound to its request and concrete backend.
pub struct CatalogIndexMigrationPage<B> {
    backend: B,
    start: IndexMigrationCursor,
    next: IndexMigrationCursor,
    rows: VecDeque<IndexMigrationRowEvidence>,
    evidence_page_charge: usize,
    instruction_page_charge: usize,
    final_physical_key: riffdb_types::IndexEntryKey,
    _backend: BackendBrand<B>,
}

/// Exact scan-end response that can only consume a catalog scan request.
pub struct CatalogIndexMigrationExactEnd<B> {
    backend: B,
    cursor: IndexMigrationCursor,
    _backend: BackendBrand<B>,
}

/// One catalog-minted request for the exact bundle bound to one codec-checked row.
pub struct CatalogIndexMigrationBundleRequest<B> {
    database_id: DatabaseId,
    open_session_id: OpenSessionId,
    row: IndexMigrationRowEvidence,
    _backend: BackendBrand<B>,
}

/// Same-request bundle response retaining the row and concrete backend port.
pub struct CatalogIndexMigrationBundleResponse<B> {
    backend: B,
    row: IndexMigrationRowEvidence,
    bundle: HistoricalBundleEvidence,
    _backend: BackendBrand<B>,
}

/// Closed catalog-derived action for one exact codec-checked migration row.
///
/// ```compile_fail
/// use riffdb_catalog::{
///     CatalogIndexMigrationInstruction, CatalogIndexMigrationV2Confirm,
/// };
/// let confirmation = CatalogIndexMigrationV2Confirm { expected: panic!() };
/// let _ = CatalogIndexMigrationInstruction::V2Confirm(confirmation);
/// ```
pub enum CatalogIndexMigrationInstruction {
    /// Compare the exact V1 envelope and replace it with the derived V2 row.
    V1Rewrite(CatalogIndexMigrationV1Rewrite),
    /// Compare and confirm an exact V2 envelope without writing it.
    V2Confirm(CatalogIndexMigrationV2Confirm),
}

/// Field-private V1 rewrite material minted only after catalog derivation.
pub struct CatalogIndexMigrationV1Rewrite {
    expected: IndexMigrationRowEvidence,
    replacement: StoredIndexEntryV2,
}

/// Field-private V2 confirmation material minted only after catalog derivation.
pub struct CatalogIndexMigrationV2Confirm {
    expected: IndexMigrationRowEvidence,
}

/// Complete catalog-owned instruction batch branded to exactly one backend type.
///
/// ```compile_fail
/// use riffdb_catalog::CatalogIndexMigrationBatch;
/// fn substitute<A, B>(batch: CatalogIndexMigrationBatch<A>)
///     -> CatalogIndexMigrationBatch<B>
/// {
///     batch
/// }
/// ```
///
/// ```compile_fail
/// use std::marker::PhantomData;
/// use riffdb_catalog::CatalogIndexMigrationBatch;
/// struct LocalBackend;
/// let _batch: CatalogIndexMigrationBatch<LocalBackend> = CatalogIndexMigrationBatch {
///     start: panic!(),
///     next: panic!(),
///     instructions: Vec::new(),
///     evidence_page_charge: 0,
///     instruction_page_charge: 0,
///     final_physical_key: panic!(),
///     _backend: PhantomData,
/// };
/// ```
pub struct CatalogIndexMigrationBatch<B> {
    start: IndexMigrationCursor,
    next: IndexMigrationCursor,
    instructions: Vec<CatalogIndexMigrationInstruction>,
    evidence_page_charge: usize,
    instruction_page_charge: usize,
    final_physical_key: riffdb_types::IndexEntryKey,
    _backend: BackendBrand<B>,
}

/// Catalog continuation and exact batch awaiting one atomic backend apply.
///
/// ```compile_fail
/// use std::marker::PhantomData;
/// use riffdb_catalog::CatalogIndexMigrationPendingBatch;
/// struct LocalBackend;
/// let _pending: CatalogIndexMigrationPendingBatch<LocalBackend> =
///     CatalogIndexMigrationPendingBatch {
///         context: panic!(),
///         batch: panic!(),
///         _backend: PhantomData,
///     };
/// ```
pub struct CatalogIndexMigrationPendingBatch<B> {
    context: CatalogIndexMigrationContext,
    batch: CatalogIndexMigrationBatch<B>,
    _backend: BackendBrand<B>,
}

/// Applied catalog continuation returned only by consuming its matching pending batch.
///
/// ```compile_fail
/// use std::marker::PhantomData;
/// use riffdb_catalog::CatalogIndexMigrationApplied;
/// struct LocalBackend;
/// let _applied: CatalogIndexMigrationApplied<LocalBackend> = CatalogIndexMigrationApplied {
///     context: panic!(),
///     backend: LocalBackend,
///     _backend: PhantomData,
/// };
/// ```
pub struct CatalogIndexMigrationApplied<B> {
    context: CatalogIndexMigrationContext,
    backend: B,
    _backend: BackendBrand<B>,
}

/// Exact catalog completion branded to one concrete backend implementation.
///
/// ```compile_fail
/// use std::marker::PhantomData;
/// use riffdb_catalog::CatalogIndexMigrationCompletion;
/// struct LocalBackend;
/// let _completion: CatalogIndexMigrationCompletion<LocalBackend> =
///     CatalogIndexMigrationCompletion {
///         final_cursor: panic!(),
///         _backend: PhantomData,
///     };
/// ```
pub struct CatalogIndexMigrationCompletion<B> {
    final_cursor: IndexMigrationCursor,
    _backend: BackendBrand<B>,
}

/// Closed failure from the catalog-owned driver.
#[derive(Clone, Eq, PartialEq)]
pub enum CatalogIndexMigrationDriveError {
    /// Historical contract interpretation or identity validation failed.
    Catalog(CatalogError),
    /// The concrete backend failed a requested storage transition.
    Storage(StorageError),
}

impl CatalogIndexMigrationContext {
    /// Returns the durable database bound to the consumed validation pass.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Returns the process-local startup session bound to this context.
    #[must_use]
    pub const fn open_session_id(&self) -> OpenSessionId {
        self.open_session_id
    }

    /// Returns the number of initial historical evidence items consumed.
    #[must_use]
    pub const fn evidence_count(&self) -> u64 {
        self.evidence_count
    }
}

impl<B> CatalogIndexMigrationScanRequest<B>
where
    B: StartupIndexMigrationPort,
{
    /// Returns the exact database/session/position catalog is requesting.
    #[must_use]
    pub const fn cursor(&self) -> IndexMigrationCursor {
        self.cursor
    }

    /// Consumes this request into one checked nonempty physical-order page.
    pub fn page(
        self,
        backend: B,
        rows: Vec<IndexMigrationRowEvidence>,
        next: IndexMigrationCursor,
    ) -> Result<CatalogIndexMigrationScan<B>, StorageValueError> {
        ensure_backend_identity(&backend, self.cursor)?;
        if rows.is_empty() {
            return Err(StorageValueError::Empty);
        }
        if rows.len() > MAX_INDEX_MIGRATION_PAGE_ENTRIES {
            return Err(StorageValueError::LimitExceeded);
        }
        let count = u64::try_from(rows.len()).map_err(|_| StorageValueError::SizeOverflow)?;
        if next.database_id() != self.cursor.database_id()
            || next.open_session_id() != self.cursor.open_session_id()
            || self.cursor.advanced(count)? != next
        {
            return Err(StorageValueError::IdentityMismatch);
        }

        let mut ledgers = IndexMigrationPageLedgers::default();
        let mut prior = self.after.as_ref();
        for row in &rows {
            if prior.is_some_and(|key: &riffdb_types::IndexEntryKey| {
                key.as_bytes() >= row.physical_key().as_bytes()
            }) {
                return Err(StorageValueError::NonCanonicalOrder);
            }
            ledgers.push(row.evidence_page_charge(), row.instruction_page_charge())?;
            prior = Some(row.physical_key());
        }
        let final_physical_key = rows
            .last()
            .expect("nonempty migration page checked above")
            .physical_key()
            .clone();

        Ok(CatalogIndexMigrationScan::Page(CatalogIndexMigrationPage {
            backend,
            start: self.cursor,
            next,
            rows: rows.into(),
            evidence_page_charge: ledgers.evidence,
            instruction_page_charge: ledgers.instructions,
            final_physical_key,
            _backend: PhantomData,
        }))
    }

    /// Consumes this exact request into an explicit physical end response.
    pub fn exact_end(self, backend: B) -> Result<CatalogIndexMigrationScan<B>, StorageValueError> {
        ensure_backend_identity(&backend, self.cursor)?;
        Ok(CatalogIndexMigrationScan::ExactEnd(
            CatalogIndexMigrationExactEnd {
                backend,
                cursor: self.cursor,
                _backend: PhantomData,
            },
        ))
    }
}

impl<B> CatalogIndexMigrationBundleRequest<B>
where
    B: StartupIndexMigrationPort,
{
    /// Borrows the only codec-checked row this point read may resolve.
    #[must_use]
    pub const fn evidence(&self) -> &IndexMigrationRowEvidence {
        &self.row
    }

    /// Consumes this request with the exact bundle and continuing backend port.
    pub fn respond(
        self,
        backend: B,
        bundle: HistoricalBundleEvidence,
    ) -> Result<CatalogIndexMigrationBundleResponse<B>, StorageValueError> {
        if backend.database_id() != self.database_id
            || backend.open_session_id() != self.open_session_id
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(CatalogIndexMigrationBundleResponse {
            backend,
            row: self.row,
            bundle,
            _backend: PhantomData,
        })
    }
}

impl CatalogIndexMigrationInstruction {
    /// Borrows the exact physical key, semantic row, and observed envelope.
    #[must_use]
    pub const fn expected(&self) -> &IndexMigrationRowEvidence {
        match self {
            Self::V1Rewrite(rewrite) => &rewrite.expected,
            Self::V2Confirm(confirm) => &confirm.expected,
        }
    }

    /// Borrows the exact catalog-derived V2 post-image when this instruction writes.
    #[must_use]
    pub const fn replacement(&self) -> Option<&StoredIndexEntryV2> {
        match self {
            Self::V1Rewrite(rewrite) => Some(&rewrite.replacement),
            Self::V2Confirm(_) => None,
        }
    }
}

impl CatalogIndexMigrationV1Rewrite {
    /// Borrows the exact codec-checked V1 expectation.
    #[must_use]
    pub const fn expected(&self) -> &IndexMigrationRowEvidence {
        &self.expected
    }

    /// Borrows the exact catalog-derived V2 replacement.
    #[must_use]
    pub const fn replacement(&self) -> &StoredIndexEntryV2 {
        &self.replacement
    }
}

impl CatalogIndexMigrationV2Confirm {
    /// Borrows the exact codec-checked V2 expectation.
    #[must_use]
    pub const fn expected(&self) -> &IndexMigrationRowEvidence {
        &self.expected
    }
}

impl<B> CatalogIndexMigrationBatch<B> {
    /// Returns the exact cursor that produced this page.
    #[must_use]
    pub const fn start(&self) -> IndexMigrationCursor {
        self.start
    }

    /// Returns the continuation reached only after complete atomic apply.
    #[must_use]
    pub const fn next(&self) -> IndexMigrationCursor {
        self.next
    }

    /// Borrows one catalog-derived instruction per row in physical order.
    #[must_use]
    pub fn instructions(&self) -> &[CatalogIndexMigrationInstruction] {
        &self.instructions
    }

    /// Returns the exact checked evidence ledger charge.
    #[must_use]
    pub const fn evidence_page_charge(&self) -> usize {
        self.evidence_page_charge
    }

    /// Returns the conservative checked instruction/write ledger charge.
    #[must_use]
    pub const fn instruction_page_charge(&self) -> usize {
        self.instruction_page_charge
    }
}

impl<B> CatalogIndexMigrationPendingBatch<B>
where
    B: StartupIndexMigrationPort,
{
    /// Borrows the complete read-only batch to compare and apply atomically.
    #[must_use]
    pub const fn batch(&self) -> &CatalogIndexMigrationBatch<B> {
        &self.batch
    }

    /// Records successful atomic apply while consuming the exact pending state.
    ///
    /// A concrete backend must call this only after its all-or-none durable apply
    /// succeeds. The returned applied state is accepted only by the catalog driver.
    pub fn applied(
        mut self,
        backend: B,
    ) -> Result<CatalogIndexMigrationApplied<B>, StorageValueError> {
        ensure_backend_identity(&backend, self.batch.next)?;
        if self.context.next != self.batch.start {
            return Err(StorageValueError::IdentityMismatch);
        }
        self.context.next = self.batch.next;
        self.context.last_physical_key = Some(self.batch.final_physical_key);
        Ok(CatalogIndexMigrationApplied {
            context: self.context,
            backend,
            _backend: PhantomData,
        })
    }
}

impl<B> CatalogIndexMigrationCompletion<B> {
    /// Returns the final exact database/session/position consumed by catalog.
    #[must_use]
    pub const fn final_cursor(&self) -> IndexMigrationCursor {
        self.final_cursor
    }
}

impl<B> CatalogIndexMigrationDriver<B>
where
    B: CatalogIndexMigrationBackend,
{
    /// Binds an opaque catalog context to the one matching concrete backend port.
    pub fn new(
        context: CatalogIndexMigrationContext,
        backend: B,
    ) -> Result<Self, CatalogIndexMigrationDriveError> {
        ensure_context_backend_identity(&context, &backend)?;
        Ok(Self {
            context,
            backend,
            _backend: PhantomData,
        })
    }

    /// Consumes the complete page/bundle/apply/end protocol without exposing proofs.
    pub fn run(self) -> Result<B::Output, CatalogIndexMigrationDriveError> {
        let Self {
            mut context,
            mut backend,
            _backend: _,
        } = self;
        ensure_context_backend_identity(&context, &backend)?;

        loop {
            let request = CatalogIndexMigrationScanRequest {
                cursor: context.next,
                after: context.last_physical_key.clone(),
                _backend: PhantomData,
            };
            match backend.read_index_migration_page(request)? {
                CatalogIndexMigrationScan::Page(page) => {
                    let CatalogIndexMigrationPage {
                        backend: page_backend,
                        start,
                        next,
                        mut rows,
                        evidence_page_charge,
                        instruction_page_charge,
                        final_physical_key,
                        _backend: _,
                    } = page;
                    if start != context.next {
                        return Err(invalid_migration_history().into());
                    }
                    backend = page_backend;
                    let mut instructions = Vec::with_capacity(rows.len());
                    while let Some(row) = rows.pop_front() {
                        let request = CatalogIndexMigrationBundleRequest {
                            database_id: context.database_id,
                            open_session_id: context.open_session_id,
                            row,
                            _backend: PhantomData,
                        };
                        let response = backend.read_historical_bundle(request)?;
                        let CatalogIndexMigrationBundleResponse {
                            backend: response_backend,
                            row,
                            bundle,
                            _backend: _,
                        } = response;
                        backend = response_backend;
                        instructions
                            .push(derive_index_migration_instruction(&context, row, bundle)?);
                    }
                    let batch = CatalogIndexMigrationBatch {
                        start,
                        next,
                        instructions,
                        evidence_page_charge,
                        instruction_page_charge,
                        final_physical_key,
                        _backend: PhantomData,
                    };
                    let pending = CatalogIndexMigrationPendingBatch {
                        context,
                        batch,
                        _backend: PhantomData,
                    };
                    let applied = backend.apply_index_migration_batch(pending)?;
                    context = applied.context;
                    backend = applied.backend;
                }
                CatalogIndexMigrationScan::ExactEnd(end) => {
                    if end.cursor != context.next {
                        return Err(invalid_migration_history().into());
                    }
                    let completion = CatalogIndexMigrationCompletion {
                        final_cursor: end.cursor,
                        _backend: PhantomData,
                    };
                    return end
                        .backend
                        .finish_index_migration(completion)
                        .map_err(Into::into);
                }
            }
        }
    }
}

fn ensure_backend_identity<B>(
    backend: &B,
    cursor: IndexMigrationCursor,
) -> Result<(), StorageValueError>
where
    B: StartupIndexMigrationPort,
{
    if backend.database_id() != cursor.database_id()
        || backend.open_session_id() != cursor.open_session_id()
    {
        return Err(StorageValueError::IdentityMismatch);
    }
    Ok(())
}

fn ensure_context_backend_identity<B>(
    context: &CatalogIndexMigrationContext,
    backend: &B,
) -> Result<(), CatalogIndexMigrationDriveError>
where
    B: StartupIndexMigrationPort,
{
    if backend.database_id() != context.database_id
        || backend.open_session_id() != context.open_session_id
        || context.next.database_id() != context.database_id
        || context.next.open_session_id() != context.open_session_id
    {
        return Err(invalid_migration_history().into());
    }
    Ok(())
}

fn derive_index_migration_instruction(
    context: &CatalogIndexMigrationContext,
    row: IndexMigrationRowEvidence,
    bundle_evidence: HistoricalBundleEvidence,
) -> Result<CatalogIndexMigrationInstruction, CatalogError> {
    let binding = row.row().schema_binding();
    if !context.bindings.iter().any(|candidate| {
        candidate.lineage == *binding.lineage()
            && candidate.version == binding.contract_version()
            && candidate.bundle_hash == binding.bundle_hash()
    }) || bundle_evidence.lineage() != binding.lineage()
        || bundle_evidence.version() != binding.contract_version()
        || bundle_evidence.bundle_hash() != binding.bundle_hash()
    {
        return Err(invalid_migration_history());
    }

    let bundle = validate_bundle_evidence(&bundle_evidence)?;
    let (entity, index) = find_index_owner(bundle.bundle().schema(), row.physical_key().index_id())
        .ok_or_else(|| CatalogError::new(CatalogErrorKind::InvalidHistoricalKey))?;
    let decoded = index
        .key_schema()
        .decode_index(row.physical_key())
        .map_err(|_| CatalogError::new(CatalogErrorKind::InvalidHistoricalKey))?;
    let partition =
        derive_historical_partition(bundle.bundle().schema(), entity, decoded.entity_key())?;

    match row.row() {
        IndexMigrationSemanticRow::V1(source) => {
            let replacement = StoredIndexEntryV2::new(
                source.key().clone(),
                source.schema_binding().clone(),
                source.covered_values().clone(),
                partition,
            )
            .map_err(|_| invalid_migration_history())?;
            Ok(CatalogIndexMigrationInstruction::V1Rewrite(
                CatalogIndexMigrationV1Rewrite {
                    expected: row,
                    replacement,
                },
            ))
        }
        IndexMigrationSemanticRow::V2(source) => {
            if source.partition_key() != &partition {
                return Err(invalid_migration_history());
            }
            Ok(CatalogIndexMigrationInstruction::V2Confirm(
                CatalogIndexMigrationV2Confirm { expected: row },
            ))
        }
    }
}

fn invalid_migration_history() -> CatalogError {
    CatalogError::new(CatalogErrorKind::InvalidHistoricalEvidence)
}

impl From<CatalogError> for CatalogIndexMigrationDriveError {
    fn from(error: CatalogError) -> Self {
        Self::Catalog(error)
    }
}

impl From<StorageError> for CatalogIndexMigrationDriveError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

impl fmt::Debug for CatalogIndexMigrationDriveError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Catalog(error) => formatter.debug_tuple("Catalog").field(error).finish(),
            Self::Storage(error) => formatter.debug_tuple("Storage").field(error).finish(),
        }
    }
}

impl fmt::Display for CatalogIndexMigrationDriveError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Catalog(error) => error.fmt(formatter),
            Self::Storage(error) => error.fmt(formatter),
        }
    }
}

impl Error for CatalogIndexMigrationDriveError {}

impl<B> fmt::Debug for CatalogIndexMigrationPendingBatch<B> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CatalogIndexMigrationPendingBatch")
            .field("start", &self.batch.start)
            .field("next", &self.batch.next)
            .field("state", &"[PENDING]")
            .finish()
    }
}

impl fmt::Debug for CatalogIndexMigrationContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CatalogIndexMigrationContext")
            .field("database_id", &self.database_id)
            .field("open_session_id", &self.open_session_id)
            .field("binding_count", &self.bindings.len())
            .field("evidence_count", &self.evidence_count)
            .finish()
    }
}

impl fmt::Debug for CatalogHistoryOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ready(history) => formatter.debug_tuple("Ready").field(history).finish(),
            Self::MigrationRequired(context) => formatter
                .debug_tuple("MigrationRequired")
                .field(context)
                .finish(),
        }
    }
}

#[derive(Default)]
struct HistoricalValidationState {
    active_seen: bool,
    active: Option<ValidatedContractBundle>,
    terminal_bundle: Option<ValidatedContractBundle>,
    lineage_bundles: Vec<ValidatedContractBundle>,
    lineage_budget: LineageBudget,
    lineage_proof: Option<Arc<LineageMaterializationProof>>,
    saw_v1_index: bool,
}

impl<E> CatalogHistoryValidation<E> {
    /// Borrows the closed catalog outcome.
    #[must_use]
    pub const fn outcome(&self) -> &CatalogHistoryOutcome {
        &self.outcome
    }

    /// Consumes the pair for WP-130's exact startup-outcome join.
    #[must_use]
    pub fn into_parts(self) -> (CatalogHistoryOutcome, E) {
        (self.outcome, self.historical_end)
    }
}

/// Consumes every same-session historical page through an exact end marker.
pub fn validate_catalog_history<S: StructuralEvidenceSession>(
    session: &mut S,
) -> Result<CatalogHistoryValidation<S::HistoricalEnd>, CatalogError> {
    let database_id = session.database_id();
    let open_session_id = session.open_session_id();
    let mut cursor = HistoricalEvidenceCursor::start(database_id, open_session_id);
    let page_limit = EvidencePageLimit::new(500)
        .ok_or_else(|| CatalogError::new(CatalogErrorKind::InvalidHistoricalEvidence))?;
    let mut last_order_key: Option<Vec<u8>> = None;
    let mut state = HistoricalValidationState::default();
    let mut evidence_count = 0u64;

    let historical_end = loop {
        match session.read_historical_evidence(cursor, page_limit)? {
            HistoricalEvidencePage::Page {
                start,
                evidence,
                next,
            } => {
                let amount = u64::try_from(evidence.len())
                    .map_err(|_| CatalogError::new(CatalogErrorKind::InvalidHistoricalEvidence))?;
                if amount == 0 || amount > u64::from(page_limit.get()) {
                    return Err(CatalogError::new(
                        CatalogErrorKind::InvalidHistoricalEvidence,
                    ));
                }
                let expected_next = cursor
                    .advanced(amount)
                    .map_err(|_| CatalogError::new(CatalogErrorKind::InvalidHistoricalEvidence))?;
                if start != cursor || next == cursor || next != expected_next {
                    return Err(CatalogError::new(
                        CatalogErrorKind::InvalidHistoricalEvidence,
                    ));
                }

                for item in evidence {
                    let order_key = historical_order_key(&item);
                    if last_order_key
                        .as_ref()
                        .is_some_and(|prior| prior >= &order_key)
                    {
                        return Err(CatalogError::new(
                            CatalogErrorKind::InvalidHistoricalEvidence,
                        ));
                    }
                    validate_historical_item(session, item, &mut state)?;
                    last_order_key = Some(order_key);
                    evidence_count = evidence_count.checked_add(1).ok_or_else(|| {
                        CatalogError::new(CatalogErrorKind::InvalidHistoricalEvidence)
                    })?;
                }
                cursor = next;
            }
            HistoricalEvidencePage::ExactEnd(end) => {
                if end.cursor() != cursor {
                    return Err(CatalogError::new(
                        CatalogErrorKind::InvalidHistoricalEvidence,
                    ));
                }
                break end;
            }
        }
    };

    if !state.active_seen
        || !active_matches_terminal(state.active.as_ref(), state.terminal_bundle.as_ref())
    {
        return Err(CatalogError::new(
            CatalogErrorKind::InvalidHistoricalEvidence,
        ));
    }

    let outcome = if state.saw_v1_index {
        let bindings = state
            .lineage_bundles
            .iter()
            .map(|bundle| HistoricalBindingRef {
                lineage: bundle.lineage().clone(),
                version: bundle.contract_version(),
                bundle_hash: bundle.bundle_hash(),
            })
            .collect();
        CatalogHistoryOutcome::MigrationRequired(CatalogIndexMigrationContext {
            database_id,
            open_session_id,
            bindings,
            evidence_count,
            next: IndexMigrationCursor::start(database_id, open_session_id),
            last_physical_key: None,
        })
    } else {
        CatalogHistoryOutcome::Ready(ValidatedCatalogHistory {
            database_id,
            open_session_id,
            active: state.active,
            lineage_proof: state.lineage_proof,
            evidence_count,
        })
    };

    Ok(CatalogHistoryValidation {
        outcome,
        historical_end,
    })
}

fn validate_historical_item<S: StructuralEvidenceSession>(
    session: &mut S,
    item: HistoricalSemanticEvidence,
    state: &mut HistoricalValidationState,
) -> Result<(), CatalogError> {
    match item {
        HistoricalSemanticEvidence::Bundle(evidence) => {
            if state.lineage_proof.is_some() {
                return Err(CatalogError::new(
                    CatalogErrorKind::InvalidHistoricalEvidence,
                ));
            }
            state
                .lineage_budget
                .push_bundle(evidence.bytes().as_bytes().len())
                .map_err(map_history_lineage_error)?;
            let bundle = validate_bundle_evidence(&evidence)?;
            validate_historical_bundle_parent(&bundle, state.terminal_bundle.as_ref())?;
            state.lineage_bundles.push(bundle.clone());
            state.terminal_bundle = Some(bundle);
        }
        HistoricalSemanticEvidence::PlanReference(reference) => {
            let proof = ensure_lineage_proof(&state.lineage_bundles, &mut state.lineage_proof)?;
            let (ordinal, bundle) = proof
                .exact_member(
                    reference.contract_version(),
                    reference.contract_bundle_hash(),
                )
                .ok_or_else(|| CatalogError::new(CatalogErrorKind::UnknownExecutablePlan))?;
            bundle.resolve_plan_with_proof(&reference, Arc::clone(proof), ordinal)?;
        }
        HistoricalSemanticEvidence::ActiveCatalog(observed) => {
            if state.active_seen {
                return Err(CatalogError::new(
                    CatalogErrorKind::InvalidHistoricalEvidence,
                ));
            }
            state.active_seen = true;
            state.active = match observed {
                Some(pointer) => {
                    let loaded = load_historical_bundle(
                        session,
                        pointer.lineage(),
                        pointer.version(),
                        pointer.bundle_hash(),
                    )?;
                    let proof =
                        ensure_lineage_proof(&state.lineage_bundles, &mut state.lineage_proof)?;
                    if proof
                        .exact_member(pointer.version(), pointer.bundle_hash())
                        .is_none()
                    {
                        return Err(CatalogError::new(
                            CatalogErrorKind::InvalidHistoricalEvidence,
                        ));
                    }
                    Some(loaded)
                }
                None => None,
            };
        }
        HistoricalSemanticEvidence::PersistedKey(evidence) => {
            let proof = ensure_lineage_proof(&state.lineage_bundles, &mut state.lineage_proof)?;
            validate_persisted_key(session, proof, &evidence)?;
        }
        HistoricalSemanticEvidence::IndexMigrationRow(evidence) => {
            let proof = ensure_lineage_proof(&state.lineage_bundles, &mut state.lineage_proof)?;
            state.saw_v1_index |= validate_index_migration_row(session, proof, &evidence)?;
        }
        HistoricalSemanticEvidence::CapabilityPartition(evidence) => {
            let active = state
                .active
                .as_ref()
                .ok_or_else(|| CatalogError::new(CatalogErrorKind::InvalidHistoricalEvidence))?;
            validate_capability_partition(active, evidence.scoped_partition())
                .map_err(|_| CatalogError::new(CatalogErrorKind::InvalidHistoricalEvidence))?;
        }
    }
    Ok(())
}

fn validate_index_migration_row<S: StructuralEvidenceSession>(
    session: &mut S,
    lineage_proof: &LineageMaterializationProof,
    evidence: &IndexMigrationRowEvidence,
) -> Result<bool, CatalogError> {
    let binding = evidence.row().schema_binding();
    let bundle = load_historical_bundle(
        session,
        binding.lineage(),
        binding.contract_version(),
        binding.bundle_hash(),
    )?;
    if lineage_proof.exact_binding_member(binding).is_none() {
        return Err(CatalogError::new(
            CatalogErrorKind::InvalidHistoricalEvidence,
        ));
    }
    let (entity, index) =
        find_index_owner(bundle.bundle().schema(), evidence.physical_key().index_id())
            .ok_or_else(|| CatalogError::new(CatalogErrorKind::InvalidHistoricalKey))?;
    let decoded = index
        .key_schema()
        .decode_index(evidence.physical_key())
        .map_err(|_| CatalogError::new(CatalogErrorKind::InvalidHistoricalKey))?;
    let partition =
        derive_historical_partition(bundle.bundle().schema(), entity, decoded.entity_key())?;
    if evidence
        .row()
        .stored_partition()
        .is_some_and(|stored| stored != &partition)
    {
        return Err(CatalogError::new(
            CatalogErrorKind::InvalidHistoricalEvidence,
        ));
    }
    if let Some(unique) = bundle
        .bundle()
        .schema()
        .unique_keys()
        .iter()
        .find(|unique| unique.index_id() == index.id())
    {
        let target =
            riffdb_storage_api::EntityTarget::new(entity.id(), decoded.entity_key().clone())
                .map_err(|_| CatalogError::new(CatalogErrorKind::InvalidHistoricalKey))?;
        let record = session
            .read_integrity_entity(&target)?
            .ok_or_else(|| CatalogError::new(CatalogErrorKind::InvalidHistoricalEvidence))?;
        let expected = derive_unique_target(entity, index, unique, &record)?;
        if expected.expected_entry() != evidence.physical_key()
            || session.read_integrity_unique_occupancy(&expected)? != UniqueOccupancyKind::Owned
        {
            return Err(CatalogError::new(
                CatalogErrorKind::InvalidHistoricalEvidence,
            ));
        }
    }
    Ok(evidence.row().is_v1())
}

fn derive_historical_partition(
    schema: &SchemaIr,
    entity: &EntitySchema,
    key: &riffdb_types::EntityKey,
) -> Result<PartitionKey, CatalogError> {
    let key_values = entity
        .primary_key()
        .decode_entity(key)
        .map_err(|_| CatalogError::new(CatalogErrorKind::InvalidHistoricalKey))?;
    let aggregate = schema
        .aggregate_for_entity(entity.id())
        .ok_or_else(|| CatalogError::new(CatalogErrorKind::InvalidHistoricalEvidence))?;
    let root = schema
        .entity(aggregate.root())
        .ok_or_else(|| CatalogError::new(CatalogErrorKind::InvalidHistoricalEvidence))?;
    let root_count = root.primary_key_fields().len();
    if key_values.len() < root_count {
        return Err(CatalogError::new(CatalogErrorKind::InvalidHistoricalKey));
    }
    let values = HistoricalRootKeyValues {
        entity_type: root.id(),
        fields: root.primary_key_fields(),
        values: &key_values[..root_count],
    };
    let component = evaluate_expression(
        aggregate.keys().expressions(),
        aggregate.keys().partition_expression(),
        &values,
    )
    .map_err(|error| match error {
        EvaluationError::Arithmetic | EvaluationError::Integrity => {
            CatalogError::new(CatalogErrorKind::InvalidHistoricalEvidence)
        }
    })?;
    aggregate
        .keys()
        .partition_schema()
        .encode_partition(&[component])
        .map_err(|_| CatalogError::new(CatalogErrorKind::InvalidHistoricalEvidence))
}

struct HistoricalRootKeyValues<'a> {
    entity_type: riffdb_types::EntityTypeId,
    fields: &'a [FieldId],
    values: &'a [CanonicalValue],
}

impl ExpressionValueSource for HistoricalRootKeyValues<'_> {
    fn schema_field(
        &self,
        entity_type: riffdb_types::EntityTypeId,
        field: FieldId,
    ) -> Option<CanonicalValue> {
        if entity_type != self.entity_type {
            return None;
        }
        self.fields
            .iter()
            .position(|candidate| *candidate == field)
            .and_then(|position| self.values.get(position))
            .cloned()
    }
}

fn validate_historical_bundle_parent(
    candidate: &ValidatedContractBundle,
    prior: Option<&ValidatedContractBundle>,
) -> Result<(), CatalogError> {
    let Some(prior) = prior else {
        if candidate.bundle().parent().is_some()
            || candidate.bundle().compatibility().overall() != CompatibilityClass::Compatible
            || !candidate.bundle().compatibility().entries().is_empty()
        {
            return Err(CatalogError::new(
                CatalogErrorKind::InvalidHistoricalEvidence,
            ));
        }
        return Ok(());
    };

    let Some(parent_reference) = candidate.bundle().parent() else {
        return Err(CatalogError::new(
            CatalogErrorKind::InvalidHistoricalEvidence,
        ));
    };
    if candidate.lineage() != prior.lineage()
        || parent_reference.contract_version() != prior.contract_version()
        || parent_reference.bundle_hash() != prior.bundle_hash()
    {
        return Err(CatalogError::new(
            CatalogErrorKind::InvalidHistoricalEvidence,
        ));
    }
    validate_successor_compatibility(candidate, prior).map_err(map_history_lineage_error)
}

fn active_matches_terminal(
    active: Option<&ValidatedContractBundle>,
    terminal: Option<&ValidatedContractBundle>,
) -> bool {
    match (active, terminal) {
        (None, None) => true,
        (Some(active), Some(terminal)) => {
            active.lineage() == terminal.lineage()
                && active.contract_version() == terminal.contract_version()
                && active.bundle_hash() == terminal.bundle_hash()
        }
        _ => false,
    }
}

fn validate_persisted_key<S: StructuralEvidenceSession>(
    session: &mut S,
    lineage_proof: &LineageMaterializationProof,
    evidence: &HistoricalPersistedKeyEvidenceV1,
) -> Result<(), CatalogError> {
    let binding = evidence.schema();
    let bundle = load_historical_bundle(
        session,
        binding.lineage(),
        binding.contract_version(),
        binding.bundle_hash(),
    )?;
    if lineage_proof.exact_binding_member(binding).is_none() {
        return Err(CatalogError::new(
            CatalogErrorKind::InvalidHistoricalEvidence,
        ));
    }

    let valid = match evidence.key() {
        IrOpaquePersistedKeyV1::Entity {
            entity_type_id,
            key,
        } => {
            let Some(entity) = bundle.bundle().schema().entity(*entity_type_id) else {
                return Err(CatalogError::new(CatalogErrorKind::InvalidHistoricalKey));
            };
            if entity.primary_key().decode_entity(key).is_err() {
                return Err(CatalogError::new(CatalogErrorKind::InvalidHistoricalKey));
            }
            let unique_keys = bundle
                .bundle()
                .schema()
                .unique_keys()
                .iter()
                .filter(|unique| unique.source_entity() == entity.id())
                .collect::<Vec<_>>();
            if unique_keys.is_empty() {
                return Ok(());
            }
            let target = riffdb_storage_api::EntityTarget::new(*entity_type_id, key.clone())
                .map_err(|_| CatalogError::new(CatalogErrorKind::InvalidHistoricalKey))?;
            let record = session
                .read_integrity_entity(&target)?
                .ok_or_else(|| CatalogError::new(CatalogErrorKind::InvalidHistoricalEvidence))?;
            if record.schema_binding() != binding {
                return Err(CatalogError::new(
                    CatalogErrorKind::InvalidHistoricalEvidence,
                ));
            }
            for unique in unique_keys {
                let index = entity
                    .indexes()
                    .iter()
                    .find(|index| index.id() == unique.index_id())
                    .ok_or_else(|| {
                        CatalogError::new(CatalogErrorKind::InvalidHistoricalEvidence)
                    })?;
                let target = derive_unique_target(entity, index, unique, &record)?;
                if session.read_integrity_unique_occupancy(&target)? != UniqueOccupancyKind::Owned {
                    return Err(CatalogError::new(
                        CatalogErrorKind::InvalidHistoricalEvidence,
                    ));
                }
            }
            true
        }
        IrOpaquePersistedKeyV1::IndexRangePrefix(prefix) => {
            find_index(bundle.bundle().schema().entities(), prefix.index_id()).is_some_and(
                |index| {
                    index
                        .key_schema()
                        .decode_index_prefix(prefix.as_bytes())
                        .is_ok()
                },
            )
        }
    };
    if !valid {
        return Err(CatalogError::new(CatalogErrorKind::InvalidHistoricalKey));
    }
    Ok(())
}

fn derive_unique_target(
    entity: &EntitySchema,
    index: &IndexSchema,
    unique: &UniqueKeySchema,
    record: &StoredEntityRecordV1,
) -> Result<UniqueIndexTarget, CatalogError> {
    if unique.source_entity() != entity.id()
        || unique.index_id() != index.id()
        || unique.fields() != index.fields()
        || record.target().entity_type_id() != entity.id()
    {
        return Err(CatalogError::new(
            CatalogErrorKind::InvalidHistoricalEvidence,
        ));
    }
    let values = unique
        .fields()
        .iter()
        .map(|field| {
            record
                .fields()
                .fields()
                .binary_search_by_key(field, |(candidate, _)| *candidate)
                .ok()
                .map(|position| record.fields().fields()[position].1.clone())
                .ok_or_else(|| CatalogError::new(CatalogErrorKind::InvalidHistoricalEvidence))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let ir_prefix = index
        .key_schema()
        .encode_index_prefix(&values)
        .map_err(|_| CatalogError::new(CatalogErrorKind::InvalidHistoricalKey))?;
    let mut prefix = IndexRangePrefixBuilder::new(index.id());
    for value in &values {
        push_unique_prefix_component(&mut prefix, value)?;
    }
    let prefix = prefix.finish();
    if prefix.as_bytes() != ir_prefix.as_bytes() {
        return Err(CatalogError::new(
            CatalogErrorKind::InvalidHistoricalEvidence,
        ));
    }
    let expected = index
        .key_schema()
        .encode_index(&values, record.target().key().clone())
        .map_err(|_| CatalogError::new(CatalogErrorKind::InvalidHistoricalKey))?;
    UniqueIndexTarget::new(riffdb_storage_api::IndexRangeTarget::new(prefix), expected)
        .map_err(|_| CatalogError::new(CatalogErrorKind::InvalidHistoricalEvidence))
}

fn push_unique_prefix_component(
    builder: &mut IndexRangePrefixBuilder,
    value: &CanonicalValue,
) -> Result<(), CatalogError> {
    let result = match value {
        CanonicalValue::Bool(value) => builder.push_bool(*value),
        CanonicalValue::I64(value) => builder.push_i64(*value),
        CanonicalValue::U64(value) => builder.push_u64(*value),
        CanonicalValue::String(value) => builder.push_str(value.as_str()),
        CanonicalValue::Bytes(value) => builder.push_bytes(value.as_bytes()),
        CanonicalValue::Timestamp(value) => builder.push_timestamp(*value),
        CanonicalValue::Date(value) => builder.push_date(*value),
        CanonicalValue::Uuid(value) => builder.push_uuid(value),
        CanonicalValue::Enum { variant_id, .. } => builder.push_enum_variant(*variant_id),
        CanonicalValue::Null
        | CanonicalValue::Decimal(_)
        | CanonicalValue::Money(_)
        | CanonicalValue::List(_)
        | CanonicalValue::Record(_) => {
            return Err(CatalogError::new(
                CatalogErrorKind::InvalidHistoricalEvidence,
            ));
        }
    };
    result
        .map(|_| ())
        .map_err(|_| CatalogError::new(CatalogErrorKind::InvalidHistoricalEvidence))
}

fn ensure_lineage_proof<'a>(
    bundles: &[ValidatedContractBundle],
    proof: &'a mut Option<Arc<LineageMaterializationProof>>,
) -> Result<&'a Arc<LineageMaterializationProof>, CatalogError> {
    if proof.is_none() {
        let checked = LineageMaterializationProof::from_forward_bundles(bundles.to_vec())
            .map_err(map_history_lineage_error)?;
        *proof = Some(checked);
    }
    proof
        .as_ref()
        .ok_or_else(|| CatalogError::new(CatalogErrorKind::InvalidHistoricalEvidence))
}

fn map_history_lineage_error(_error: CatalogError) -> CatalogError {
    CatalogError::new(CatalogErrorKind::InvalidHistoricalEvidence)
}

fn find_index(
    entities: &[riffdb_contract_ir::EntitySchema],
    index_id: IndexId,
) -> Option<&IndexSchema> {
    entities
        .iter()
        .flat_map(riffdb_contract_ir::EntitySchema::indexes)
        .find(|index| index.id() == index_id)
}

fn find_index_owner(schema: &SchemaIr, index_id: IndexId) -> Option<(&EntitySchema, &IndexSchema)> {
    schema.entities().iter().find_map(|entity| {
        entity.indexes().iter().find_map(|index| {
            if index.id() != index_id {
                return None;
            }
            match index.key_schema().purpose() {
                KeyPurpose::Index {
                    entity_type,
                    index_id: owner,
                } if entity_type == entity.id() && owner == index_id => Some((entity, index)),
                _ => None,
            }
        })
    })
}

fn load_historical_bundle<S: StructuralEvidenceSession>(
    session: &mut S,
    lineage: &riffdb_types::ContractLineage,
    version: riffdb_types::ContractVersion,
    bundle_hash: riffdb_types::ContractBundleHash,
) -> Result<ValidatedContractBundle, CatalogError> {
    let evidence = session
        .read_historical_bundle(lineage, version, bundle_hash)?
        .ok_or_else(|| CatalogError::new(CatalogErrorKind::InvalidHistoricalEvidence))?;
    validate_bundle_evidence(&evidence)
}

fn validate_bundle_evidence(
    evidence: &HistoricalBundleEvidence,
) -> Result<ValidatedContractBundle, CatalogError> {
    let bundle = ValidatedContractBundle::decode(evidence.bytes().as_bytes())?;
    if bundle.lineage() != evidence.lineage()
        || bundle.contract_version() != evidence.version()
        || bundle.bundle_hash() != evidence.bundle_hash()
    {
        return Err(CatalogError::new(
            CatalogErrorKind::InvalidHistoricalEvidence,
        ));
    }
    Ok(bundle)
}

fn historical_order_key(item: &HistoricalSemanticEvidence) -> Vec<u8> {
    let mut key = Vec::new();
    match item {
        HistoricalSemanticEvidence::Bundle(bundle) => {
            key.push(0x01);
            push_lineage(&mut key, bundle.lineage());
            key.extend_from_slice(&bundle.version().to_be_bytes());
            key.extend_from_slice(bundle.bundle_hash().as_bytes());
        }
        HistoricalSemanticEvidence::PlanReference(plan) => {
            key.push(0x02);
            push_lineage(&mut key, plan.contract_lineage());
            key.extend_from_slice(&plan.contract_version().to_be_bytes());
            key.extend_from_slice(plan.contract_bundle_hash().as_bytes());
            key.extend_from_slice(&plan.command_id().to_be_bytes());
            key.extend_from_slice(plan.command_plan_hash().as_bytes());
        }
        HistoricalSemanticEvidence::ActiveCatalog(None) => key.extend_from_slice(&[0x03, 0x00]),
        HistoricalSemanticEvidence::ActiveCatalog(Some(active)) => {
            key.extend_from_slice(&[0x03, 0x01]);
            push_lineage(&mut key, active.lineage());
            key.extend_from_slice(&active.version().to_be_bytes());
            key.extend_from_slice(active.bundle_hash().as_bytes());
        }
        HistoricalSemanticEvidence::PersistedKey(evidence) => {
            key.push(0x04);
            push_lineage(&mut key, evidence.schema().lineage());
            key.extend_from_slice(&evidence.schema().contract_version().to_be_bytes());
            key.extend_from_slice(evidence.schema().bundle_hash().as_bytes());
            let (tag, owner, bytes) = match evidence.key() {
                IrOpaquePersistedKeyV1::Entity {
                    entity_type_id,
                    key,
                } => (0x01, entity_type_id.to_be_bytes(), key.as_bytes()),
                IrOpaquePersistedKeyV1::IndexRangePrefix(prefix) => {
                    (0x03, prefix.index_id().to_be_bytes(), prefix.as_bytes())
                }
            };
            key.push(tag);
            key.extend_from_slice(&owner);
            let length = u32::try_from(bytes.len()).unwrap_or(u32::MAX);
            key.extend_from_slice(&length.to_be_bytes());
            key.extend_from_slice(bytes);
        }
        HistoricalSemanticEvidence::IndexMigrationRow(evidence) => {
            key.push(0x04);
            let binding = evidence.row().schema_binding();
            push_lineage(&mut key, binding.lineage());
            key.extend_from_slice(&binding.contract_version().to_be_bytes());
            key.extend_from_slice(binding.bundle_hash().as_bytes());
            key.push(0x02);
            key.extend_from_slice(&evidence.physical_key().index_id().to_be_bytes());
            let bytes = evidence.physical_key().as_bytes();
            let length = u32::try_from(bytes.len()).unwrap_or(u32::MAX);
            key.extend_from_slice(&length.to_be_bytes());
            key.extend_from_slice(bytes);
        }
        HistoricalSemanticEvidence::CapabilityPartition(evidence) => {
            key.push(0x05);
            key.extend_from_slice(evidence.capability_id().as_bytes());
            key.extend_from_slice(&evidence.entry_ordinal().to_be_bytes());
            push_lineage(&mut key, evidence.scoped_partition().lineage());
            let partition_key = evidence.scoped_partition().partition_key();
            key.extend_from_slice(&partition_key.aggregate_type_id().to_be_bytes());
            let length = u32::try_from(partition_key.as_bytes().len()).unwrap_or(u32::MAX);
            key.extend_from_slice(&length.to_be_bytes());
            key.extend_from_slice(partition_key.as_bytes());
        }
    }
    key
}

fn push_lineage(output: &mut Vec<u8>, lineage: &riffdb_types::ContractLineage) {
    let length = u32::try_from(lineage.as_bytes().len()).unwrap_or(u32::MAX);
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(lineage.as_bytes());
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU16, NonZeroU32};

    use riffdb_storage_api::{
        CapabilityGrantV1, CapabilityPermissionKindV1, CapabilityPermissionV1,
        CapabilityPermissionsV1, CapabilityRequestedRecordV1,
        HistoricalCapabilityPartitionEvidenceV1, StoredCapabilityRecordV1,
    };
    use riffdb_types::{
        ActorId, ActorKind, AdministrationSequence, AggregateTypeId, Audience, CapabilityId,
        CapabilityTokenDigest, ContractLineage, DatabaseId, DigestKeyId, Environment,
        PartitionKeyBuilder, PartitionScopeV1, RequestId, ScopedPartitionV1, TenantScope,
        Timestamp,
    };

    use super::*;
    use crate::lineage::{MAX_ACTIVE_LINEAGE_BUNDLES_V1, MAX_ACTIVE_LINEAGE_CANONICAL_BYTES_V1};

    #[test]
    fn migration_page_ledgers_accept_equal_and_reject_equal_plus_one_independently() {
        let mut equal = IndexMigrationPageLedgers::default();
        equal
            .push(
                MAX_INDEX_MIGRATION_PAGE_BYTES,
                MAX_INDEX_MIGRATION_PAGE_BYTES,
            )
            .expect("both ledgers accept exactly four MiB");
        assert_eq!(equal.evidence, MAX_INDEX_MIGRATION_PAGE_BYTES);
        assert_eq!(equal.instructions, MAX_INDEX_MIGRATION_PAGE_BYTES);

        let mut evidence = IndexMigrationPageLedgers::default();
        evidence
            .push(MAX_INDEX_MIGRATION_PAGE_BYTES, 1)
            .expect("evidence ledger accepts its exact bound");
        assert_eq!(evidence.push(1, 1), Err(StorageValueError::LimitExceeded));
        assert_eq!(evidence.evidence, MAX_INDEX_MIGRATION_PAGE_BYTES);
        assert_eq!(evidence.instructions, 1);

        let mut instructions = IndexMigrationPageLedgers::default();
        instructions
            .push(1, MAX_INDEX_MIGRATION_PAGE_BYTES)
            .expect("instruction ledger accepts its exact bound");
        assert_eq!(
            instructions.push(1, 1),
            Err(StorageValueError::LimitExceeded)
        );
        assert_eq!(instructions.evidence, 1);
        assert_eq!(instructions.instructions, MAX_INDEX_MIGRATION_PAGE_BYTES);
    }

    #[test]
    fn startup_maps_incremental_lineage_limits_to_invalid_history() {
        let mut count = LineageBudget::default();
        for _ in 0..MAX_ACTIVE_LINEAGE_BUNDLES_V1 {
            count.push_bundle(0).expect("exact candidate count");
        }
        let candidate_error = count.push_bundle(0).expect_err("candidate one over");
        assert_eq!(
            candidate_error.kind(),
            CatalogErrorKind::LineageBundleCountLimit
        );
        assert_eq!(
            map_history_lineage_error(candidate_error).kind(),
            CatalogErrorKind::InvalidHistoricalEvidence
        );

        let mut bytes = LineageBudget::default();
        bytes
            .push_bundle(MAX_ACTIVE_LINEAGE_CANONICAL_BYTES_V1)
            .expect("exact candidate bytes");
        let candidate_error = bytes.push_bundle(1).expect_err("candidate one byte over");
        assert_eq!(
            candidate_error.kind(),
            CatalogErrorKind::LineageCanonicalBytesLimit
        );
        assert_eq!(
            map_history_lineage_error(candidate_error).kind(),
            CatalogErrorKind::InvalidHistoricalEvidence
        );
    }

    #[test]
    fn capability_partition_history_order_matches_the_shared_golden_vector() {
        let mut first_key =
            PartitionKeyBuilder::new(AggregateTypeId::new(1).expect("aggregate ID"));
        first_key.push_u64(1).expect("bounded component");
        let mut second_key =
            PartitionKeyBuilder::new(AggregateTypeId::new(2).expect("aggregate ID"));
        second_key.push_u64(2).expect("bounded component");
        let mut golden_key =
            PartitionKeyBuilder::new(AggregateTypeId::new(0x0102_0304).expect("aggregate ID"));
        golden_key
            .push_u64(0x0102_0304_0506_0708)
            .expect("bounded component");
        let scope = PartitionScopeV1::explicit(vec![
            ScopedPartitionV1::new(
                ContractLineage::new("a").expect("lineage"),
                first_key.finish().expect("first key"),
            ),
            ScopedPartitionV1::new(
                ContractLineage::new("budget").expect("lineage"),
                second_key.finish().expect("second key"),
            ),
            ScopedPartitionV1::new(
                ContractLineage::new("budget").expect("lineage"),
                golden_key.finish().expect("golden key"),
            ),
        ])
        .expect("canonical explicit scope");
        let permissions = CapabilityPermissionsV1::new(vec![
            CapabilityPermissionV1::unparameterized(
                CapabilityPermissionKindV1::AdministerCapabilities,
            )
            .expect("permission"),
        ])
        .expect("permissions");
        let grant = CapabilityGrantV1::new(
            TenantScope::Global,
            scope,
            permissions,
            Vec::new(),
            NonZeroU16::MIN,
            Vec::new(),
        )
        .expect("grant");
        let uuid = [
            0x01, 0x8f, 0x00, 0x00, 0x00, 0x00, 0x70, 0x01, 0x80, 0x02, 0x11, 0x22, 0x33, 0x44,
            0x55, 0x66,
        ];
        let requested = CapabilityRequestedRecordV1::new(
            DatabaseId::from_bytes([0x01, 0x8f, 0, 0, 0, 0, 0x70, 1, 0x80, 2, 0, 0, 0, 0, 0, 1])
                .expect("database"),
            Environment::new("test").expect("environment"),
            ActorId::new("operator").expect("actor"),
            ActorKind::Human,
            NonZeroU32::new(60).expect("duration"),
            vec![Audience::new("riffdb-test").expect("audience")],
            grant,
        )
        .expect("requested capability");
        let capability = StoredCapabilityRecordV1::active(
            CapabilityId::from_bytes(uuid).expect("capability UUIDv7"),
            CapabilityTokenDigest::from_hmac_bytes(
                DigestKeyId::new(1).expect("digest key"),
                [0x31; 32],
            ),
            requested,
            Timestamp::new(10, 0).expect("issued at"),
            Timestamp::new(70, 0).expect("expires at"),
            AdministrationSequence::first(),
            RequestId::from_bytes([0x01, 0x8f, 0, 0, 0, 0, 0x70, 1, 0x80, 2, 0, 0, 0, 0, 0, 2])
                .expect("request UUIDv7"),
        )
        .expect("stored capability");
        let evidence =
            HistoricalCapabilityPartitionEvidenceV1::from_capability_entry(&capability, 2)
                .expect("third explicit entry");
        let expected = vec![
            0x05, 0x01, 0x8f, 0x00, 0x00, 0x00, 0x00, 0x70, 0x01, 0x80, 0x02, 0x11, 0x22, 0x33,
            0x44, 0x55, 0x66, 0x00, 0x02, 0x00, 0x00, 0x00, 0x06, b'b', b'u', b'd', b'g', b'e',
            b't', 0x01, 0x02, 0x03, 0x04, 0x00, 0x00, 0x00, 0x0e, 0x50, 0x01, 0x01, 0x02, 0x03,
            0x04, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
        ];

        assert_eq!(evidence.entry_ordinal(), 2);
        assert_eq!(evidence.semantic_bytes().expect("charge"), 51);
        assert_eq!(evidence.evidence_order_key(), expected);
        assert_eq!(
            historical_order_key(&HistoricalSemanticEvidence::CapabilityPartition(evidence)),
            expected
        );
    }
}
