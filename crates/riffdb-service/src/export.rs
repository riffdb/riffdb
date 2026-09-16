//! API-neutral symbolic application-export contracts.

use std::fmt;
use std::num::NonZeroU64;

use riffdb_application::{ApplicationPortabilityManifest, PortableRecordClass};
use riffdb_policy::AuthorizedApplicationExportV1;
use riffdb_types::{
    ApplicationExportClassV1, ApplicationExportManifestHash, ApplicationExportOperationId,
    ApplicationExportPageHash, ApplicationExportReceiptHash, ApplicationExportSelectionV1,
    ApplicationExportSnapshotBindingV1, RequestId, ServiceIngressKindV1, Timestamp,
    canonical_application_export_page_preimage, hash_application_export_manifest,
    hash_application_export_page, hash_application_export_receipt,
};

use crate::{
    BoxPortCapacityPermit, PortAdmissionError, PortFuture, RequestContext, RequestControl,
    ServiceDtoError, ServiceFuture,
};

/// Maximum canonical JSON bytes in one exported record line.
pub const MAX_APPLICATION_EXPORT_JSON_LINE_BYTES: usize = 64 * 1024;
pub use riffdb_types::{MAX_APPLICATION_EXPORT_PAGE_BYTES, MAX_APPLICATION_EXPORT_PAGE_ROWS};
/// Maximum canonical bytes in a terminal manifest or receipt document.
pub const MAX_APPLICATION_EXPORT_TERMINAL_DOCUMENT_BYTES: usize = 256 * 1024;
/// Maximum opaque cursor bytes on the public surface.
pub const MAX_APPLICATION_EXPORT_CURSOR_BYTES: usize = 512;
/// Minimum caller-selected export lease.
pub const MIN_APPLICATION_EXPORT_LEASE_SECONDS: u32 = 60;
/// Maximum caller-selected export lease.
pub const MAX_APPLICATION_EXPORT_LEASE_SECONDS: u32 = 24 * 60 * 60;

/// Closed purpose of one immutable export operation.
#[derive(Clone, Eq, PartialEq)]
pub enum ApplicationExportIntentV1 {
    /// Ordinary inspection, archive, or transfer without reimport authority.
    General,
    /// Reimport-authorizing export bound to one exact adapter portability manifest.
    Portability(Box<ApplicationPortabilityManifest>),
}

impl ApplicationExportIntentV1 {
    /// Constructs a portability intent only when every mapped class is selected.
    pub fn portability(
        selection: &ApplicationExportSelectionV1,
        manifest: ApplicationPortabilityManifest,
    ) -> Result<Self, ServiceDtoError> {
        if manifest.input().contract_lineage != *selection.lineage()
            || manifest
                .input()
                .mappings
                .iter()
                .any(|mapping| match mapping.class() {
                    PortableRecordClass::Entity => !selection.entities(),
                    PortableRecordClass::Event => !selection.events(),
                })
        {
            return Err(ServiceDtoError::InvalidShape);
        }
        Ok(Self::Portability(Box::new(manifest)))
    }

    /// Exact portability manifest, absent for a general export.
    #[must_use]
    pub fn portability_manifest(&self) -> Option<&ApplicationPortabilityManifest> {
        match self {
            Self::General => None,
            Self::Portability(manifest) => Some(manifest.as_ref()),
        }
    }
}

impl fmt::Debug for ApplicationExportIntentV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::General => formatter.write_str("ApplicationExportIntentV1::General"),
            Self::Portability(manifest) => formatter
                .debug_tuple("ApplicationExportIntentV1::Portability")
                .field(&manifest.identity())
                .finish(),
        }
    }
}

/// One server-produced canonical JSON object without its JSONL newline.
#[derive(Clone, Eq, PartialEq)]
pub struct CanonicalApplicationExportJsonLine(Vec<u8>);

impl CanonicalApplicationExportJsonLine {
    /// Checks the bounded one-object carriage used by the JSONL writer.
    pub fn new(value: Vec<u8>) -> Result<Self, ServiceDtoError> {
        if value.is_empty() {
            return Err(ServiceDtoError::Empty);
        }
        if value.len() > MAX_APPLICATION_EXPORT_JSON_LINE_BYTES {
            return Err(ServiceDtoError::TooLong);
        }
        if std::str::from_utf8(&value).is_err()
            || value.contains(&b'\n')
            || value.contains(&b'\r')
            || value.first() != Some(&b'{')
            || value.last() != Some(&b'}')
        {
            return Err(ServiceDtoError::InvalidShape);
        }
        Ok(Self(value))
    }

    /// Exact canonical JSON object bytes without a trailing newline.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for CanonicalApplicationExportJsonLine {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CanonicalApplicationExportJsonLine([REDACTED])")
    }
}

/// One bounded server-produced canonical JSON manifest or receipt document.
#[derive(Clone, Eq, PartialEq)]
pub struct CanonicalApplicationExportJsonDocument(Vec<u8>);

impl CanonicalApplicationExportJsonDocument {
    /// Checks one canonical JSON object carriage.
    pub fn new(value: Vec<u8>) -> Result<Self, ServiceDtoError> {
        if value.is_empty() {
            return Err(ServiceDtoError::Empty);
        }
        if value.len() > MAX_APPLICATION_EXPORT_TERMINAL_DOCUMENT_BYTES {
            return Err(ServiceDtoError::TooLong);
        }
        if std::str::from_utf8(&value).is_err()
            || value.contains(&b'\n')
            || value.contains(&b'\r')
            || value.first() != Some(&b'{')
            || value.last() != Some(&b'}')
        {
            return Err(ServiceDtoError::InvalidShape);
        }
        Ok(Self(value))
    }

    /// Exact canonical JSON bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for CanonicalApplicationExportJsonDocument {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CanonicalApplicationExportJsonDocument([REDACTED])")
    }
}

/// Opaque server-sealed cursor. It never exposes a storage key or offset.
#[derive(Clone, Eq, PartialEq)]
pub struct ApplicationExportCursor(Vec<u8>);

impl ApplicationExportCursor {
    /// Constructs one nonempty bounded opaque cursor.
    pub fn new(value: Vec<u8>) -> Result<Self, ServiceDtoError> {
        if value.is_empty() {
            return Err(ServiceDtoError::Empty);
        }
        if value.len() > MAX_APPLICATION_EXPORT_CURSOR_BYTES {
            return Err(ServiceDtoError::TooLong);
        }
        Ok(Self(value))
    }

    /// Exact opaque bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for ApplicationExportCursor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ApplicationExportCursor([REDACTED])")
    }
}

/// Checked start-or-replay request for one immutable export selection.
#[derive(Clone, Eq, PartialEq)]
pub struct StartApplicationExportRequest {
    operation_id: ApplicationExportOperationId,
    selection: ApplicationExportSelectionV1,
    intent: ApplicationExportIntentV1,
    lease_seconds: u32,
}

impl StartApplicationExportRequest {
    /// Constructs one caller-stable, bounded export request.
    pub fn new(
        operation_id: ApplicationExportOperationId,
        selection: ApplicationExportSelectionV1,
        lease_seconds: u32,
    ) -> Result<Self, ServiceDtoError> {
        if !(MIN_APPLICATION_EXPORT_LEASE_SECONDS..=MAX_APPLICATION_EXPORT_LEASE_SECONDS)
            .contains(&lease_seconds)
        {
            return Err(ServiceDtoError::OutOfRange);
        }
        Ok(Self {
            operation_id,
            selection,
            intent: ApplicationExportIntentV1::General,
            lease_seconds,
        })
    }

    /// Constructs one caller-stable portability export bound to an exact manifest.
    pub fn new_portability(
        operation_id: ApplicationExportOperationId,
        selection: ApplicationExportSelectionV1,
        manifest: ApplicationPortabilityManifest,
        lease_seconds: u32,
    ) -> Result<Self, ServiceDtoError> {
        if !(MIN_APPLICATION_EXPORT_LEASE_SECONDS..=MAX_APPLICATION_EXPORT_LEASE_SECONDS)
            .contains(&lease_seconds)
        {
            return Err(ServiceDtoError::OutOfRange);
        }
        let intent = ApplicationExportIntentV1::portability(&selection, manifest)?;
        Ok(Self {
            operation_id,
            selection,
            intent,
            lease_seconds,
        })
    }

    /// Caller-stable operation identity.
    #[must_use]
    pub const fn operation_id(&self) -> ApplicationExportOperationId {
        self.operation_id
    }

    /// Exact subset of current V5 authority requested by the caller.
    #[must_use]
    pub const fn selection(&self) -> &ApplicationExportSelectionV1 {
        &self.selection
    }

    /// Immutable general or exact-manifest portability purpose.
    #[must_use]
    pub const fn intent(&self) -> &ApplicationExportIntentV1 {
        &self.intent
    }

    /// Requested bounded operation lease.
    #[must_use]
    pub const fn lease_seconds(&self) -> u32 {
        self.lease_seconds
    }
}

impl fmt::Debug for StartApplicationExportRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StartApplicationExportRequest([REDACTED])")
    }
}

/// One exact operation selector used by status and cancellation.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ApplicationExportOperationRequest {
    operation_id: ApplicationExportOperationId,
}

impl ApplicationExportOperationRequest {
    /// Constructs one exact selector.
    #[must_use]
    pub const fn new(operation_id: ApplicationExportOperationId) -> Self {
        Self { operation_id }
    }

    /// Caller-stable operation identity.
    #[must_use]
    pub const fn operation_id(self) -> ApplicationExportOperationId {
        self.operation_id
    }
}

impl fmt::Debug for ApplicationExportOperationRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ApplicationExportOperationRequest([REDACTED])")
    }
}

/// Checked request for the next page at one exact durable checkpoint.
#[derive(Clone, Eq, PartialEq)]
pub struct GetApplicationExportPageRequest {
    operation_id: ApplicationExportOperationId,
    cursor: ApplicationExportCursor,
    max_rows: u16,
}

impl GetApplicationExportPageRequest {
    /// Constructs one bounded page request.
    pub fn new(
        operation_id: ApplicationExportOperationId,
        cursor: ApplicationExportCursor,
        max_rows: u16,
    ) -> Result<Self, ServiceDtoError> {
        if max_rows == 0 || usize::from(max_rows) > MAX_APPLICATION_EXPORT_PAGE_ROWS {
            return Err(ServiceDtoError::OutOfRange);
        }
        Ok(Self {
            operation_id,
            cursor,
            max_rows,
        })
    }

    /// Caller-stable operation identity.
    #[must_use]
    pub const fn operation_id(&self) -> ApplicationExportOperationId {
        self.operation_id
    }

    /// Opaque exact checkpoint selector.
    #[must_use]
    pub const fn cursor(&self) -> &ApplicationExportCursor {
        &self.cursor
    }

    /// Requested row count, still subject to server byte/time bounds.
    #[must_use]
    pub const fn max_rows(&self) -> u16 {
        self.max_rows
    }
}

impl fmt::Debug for GetApplicationExportPageRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("GetApplicationExportPageRequest([REDACTED])")
    }
}

/// Closed observable lifecycle of one snapshot-bound export.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationExportPhaseV1 {
    /// Snapshot and durable initial checkpoint are accepted.
    Accepted,
    /// One or more pages have been released.
    Exporting,
    /// Manifest and complete terminal receipt are durable.
    Completed,
    /// Caller cancellation produced an incomplete terminal receipt.
    Cancelled,
    /// Lease expiry produced an incomplete terminal receipt; restart is required.
    Expired,
    /// Source, authority, or durable validation failed closed.
    FailedClosed,
}

impl ApplicationExportPhaseV1 {
    /// Whether a durable terminal receipt must be present.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Cancelled | Self::Expired | Self::FailedClosed
        )
    }
}

/// Closed public-safe reason an export did not complete.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationExportFailureV1 {
    /// Current V5 capability identity or scope changed at a safe point.
    AuthorityChanged,
    /// The pinned snapshot can no longer be served; restart is required.
    SnapshotUnavailable,
    /// Startup/source validation did not prove the source safe to export.
    SourceInvalid,
    /// The bounded operation lease expired.
    LeaseExpired,
    /// Caller explicitly cancelled the operation.
    Cancelled,
    /// A row, byte, page, time, or retained-state ceiling was reached.
    LimitExceeded,
    /// A public-safe internal incident closed the operation.
    Internal,
    /// A portability snapshot contains an active or partially cleared workflow lease.
    WorkflowNotQuiescent,
}

/// One bounded canonical JSONL page and its next opaque checkpoint.
#[derive(Clone, Eq, PartialEq)]
pub struct ApplicationExportPageV1 {
    operation_id: ApplicationExportOperationId,
    page_number: NonZeroU64,
    class: ApplicationExportClassV1,
    lines: Vec<CanonicalApplicationExportJsonLine>,
    next_cursor: Option<ApplicationExportCursor>,
    class_complete: bool,
    operation_complete: bool,
    page_hash: ApplicationExportPageHash,
}

impl ApplicationExportPageV1 {
    /// Checks page shape and derives its content identity from exact canonical lines.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        operation_id: ApplicationExportOperationId,
        page_number: NonZeroU64,
        class: ApplicationExportClassV1,
        lines: Vec<CanonicalApplicationExportJsonLine>,
        next_cursor: Option<ApplicationExportCursor>,
        class_complete: bool,
        operation_complete: bool,
    ) -> Result<Self, ServiceDtoError> {
        if lines.len() > MAX_APPLICATION_EXPORT_PAGE_ROWS {
            return Err(ServiceDtoError::TooManyItems);
        }
        let bytes = lines.iter().try_fold(0usize, |total, line| {
            total
                .checked_add(line.as_bytes().len() + 1)
                .ok_or(ServiceDtoError::TooLong)
        })?;
        if bytes > MAX_APPLICATION_EXPORT_PAGE_BYTES {
            return Err(ServiceDtoError::TooLong);
        }
        if operation_complete != next_cursor.is_none() || operation_complete && !class_complete {
            return Err(ServiceDtoError::InvalidShape);
        }
        let line_bytes = lines
            .iter()
            .map(CanonicalApplicationExportJsonLine::as_bytes)
            .collect::<Vec<_>>();
        let preimage = canonical_application_export_page_preimage(
            operation_id,
            page_number,
            class,
            &line_bytes,
            class_complete,
            operation_complete,
        )
        .map_err(|_| ServiceDtoError::TooLong)?;
        Ok(Self {
            operation_id,
            page_number,
            class,
            lines,
            next_cursor,
            class_complete,
            operation_complete,
            page_hash: hash_application_export_page(&preimage),
        })
    }

    /// Caller-stable operation identity.
    #[must_use]
    pub const fn operation_id(&self) -> ApplicationExportOperationId {
        self.operation_id
    }

    /// One-based page number within the operation.
    #[must_use]
    pub const fn page_number(&self) -> NonZeroU64 {
        self.page_number
    }

    /// Closed record class carried by this page.
    #[must_use]
    pub const fn class(&self) -> ApplicationExportClassV1 {
        self.class
    }

    /// Canonical JSON objects without trailing newlines.
    #[must_use]
    pub fn lines(&self) -> &[CanonicalApplicationExportJsonLine] {
        &self.lines
    }

    /// Opaque cursor for the next page, absent only at operation completion.
    #[must_use]
    pub const fn next_cursor(&self) -> Option<&ApplicationExportCursor> {
        self.next_cursor.as_ref()
    }

    /// Whether this page reached exact end of its record class.
    #[must_use]
    pub const fn class_complete(&self) -> bool {
        self.class_complete
    }

    /// Whether all selected classes and the terminal receipt are complete.
    #[must_use]
    pub const fn operation_complete(&self) -> bool {
        self.operation_complete
    }

    /// Domain-separated hash of the exact page content and position.
    #[must_use]
    pub const fn page_hash(&self) -> ApplicationExportPageHash {
        self.page_hash
    }
}

impl fmt::Debug for ApplicationExportPageV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApplicationExportPageV1")
            .field("operation_id", &self.operation_id)
            .field("page_number", &self.page_number)
            .field("class", &self.class)
            .field("line_count", &self.lines.len())
            .field("next_cursor", &self.next_cursor.is_some())
            .field("class_complete", &self.class_complete)
            .field("operation_complete", &self.operation_complete)
            .field("page_hash", &self.page_hash)
            .finish()
    }
}

/// Current bounded public observation of one export operation.
#[derive(Clone, Eq, PartialEq)]
pub struct ApplicationExportOperationV1 {
    operation_id: ApplicationExportOperationId,
    selection: ApplicationExportSelectionV1,
    snapshot: ApplicationExportSnapshotBindingV1,
    phase: ApplicationExportPhaseV1,
    lease_expires_at: Timestamp,
    pages_released: u64,
    rows_released: u64,
    bytes_released: u64,
    failure: Option<ApplicationExportFailureV1>,
    manifest: Option<CanonicalApplicationExportJsonDocument>,
    receipt: Option<CanonicalApplicationExportJsonDocument>,
    manifest_hash: Option<ApplicationExportManifestHash>,
    receipt_hash: Option<ApplicationExportReceiptHash>,
}

impl ApplicationExportOperationV1 {
    /// Checks terminal/nonterminal receipt and failure invariants.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        operation_id: ApplicationExportOperationId,
        selection: ApplicationExportSelectionV1,
        snapshot: ApplicationExportSnapshotBindingV1,
        phase: ApplicationExportPhaseV1,
        lease_expires_at: Timestamp,
        pages_released: u64,
        rows_released: u64,
        bytes_released: u64,
        failure: Option<ApplicationExportFailureV1>,
        manifest: Option<CanonicalApplicationExportJsonDocument>,
        receipt: Option<CanonicalApplicationExportJsonDocument>,
    ) -> Result<Self, ServiceDtoError> {
        let terminal = phase.is_terminal();
        let failed = phase != ApplicationExportPhaseV1::Completed && terminal;
        if terminal != manifest.is_some()
            || terminal != receipt.is_some()
            || failed != failure.is_some()
        {
            return Err(ServiceDtoError::InvalidShape);
        }
        let manifest_hash = manifest
            .as_ref()
            .map(|document| hash_application_export_manifest(document.as_bytes()));
        let receipt_hash = receipt
            .as_ref()
            .map(|document| hash_application_export_receipt(document.as_bytes()));
        Ok(Self {
            operation_id,
            selection,
            snapshot,
            phase,
            lease_expires_at,
            pages_released,
            rows_released,
            bytes_released,
            failure,
            manifest,
            receipt,
            manifest_hash,
            receipt_hash,
        })
    }

    /// Caller-stable operation identity.
    #[must_use]
    pub const fn operation_id(&self) -> ApplicationExportOperationId {
        self.operation_id
    }

    /// Exact selected current V5 subset.
    #[must_use]
    pub const fn selection(&self) -> &ApplicationExportSelectionV1 {
        &self.selection
    }

    /// Immutable database/history/application snapshot binding.
    #[must_use]
    pub const fn snapshot(&self) -> &ApplicationExportSnapshotBindingV1 {
        &self.snapshot
    }

    /// Current closed lifecycle phase.
    #[must_use]
    pub const fn phase(&self) -> ApplicationExportPhaseV1 {
        self.phase
    }

    /// Absolute server-owned lease deadline.
    #[must_use]
    pub const fn lease_expires_at(&self) -> Timestamp {
        self.lease_expires_at
    }

    /// Released page count.
    #[must_use]
    pub const fn pages_released(&self) -> u64 {
        self.pages_released
    }

    /// Released visible row count.
    #[must_use]
    pub const fn rows_released(&self) -> u64 {
        self.rows_released
    }

    /// Released canonical JSONL bytes including newlines.
    #[must_use]
    pub const fn bytes_released(&self) -> u64 {
        self.bytes_released
    }

    /// Public-safe terminal failure, if incomplete.
    #[must_use]
    pub const fn failure(&self) -> Option<ApplicationExportFailureV1> {
        self.failure
    }

    /// Terminal canonical manifest document.
    #[must_use]
    pub const fn manifest(&self) -> Option<&CanonicalApplicationExportJsonDocument> {
        self.manifest.as_ref()
    }

    /// Terminal canonical receipt document.
    #[must_use]
    pub const fn receipt(&self) -> Option<&CanonicalApplicationExportJsonDocument> {
        self.receipt.as_ref()
    }

    /// Terminal manifest identity.
    #[must_use]
    pub const fn manifest_hash(&self) -> Option<ApplicationExportManifestHash> {
        self.manifest_hash
    }

    /// Terminal receipt identity.
    #[must_use]
    pub const fn receipt_hash(&self) -> Option<ApplicationExportReceiptHash> {
        self.receipt_hash
    }
}

impl fmt::Debug for ApplicationExportOperationV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApplicationExportOperationV1")
            .field("operation_id", &self.operation_id)
            .field("selection", &self.selection)
            .field("snapshot", &self.snapshot)
            .field("phase", &self.phase)
            .field("lease_expires_at", &self.lease_expires_at)
            .field("pages_released", &self.pages_released)
            .field("rows_released", &self.rows_released)
            .field("bytes_released", &self.bytes_released)
            .field("failure", &self.failure)
            .field("manifest", &self.manifest.is_some())
            .field("receipt", &self.receipt.is_some())
            .field("manifest_hash", &self.manifest_hash)
            .field("receipt_hash", &self.receipt_hash)
            .finish()
    }
}

/// Start idempotency disposition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationExportStartDispositionV1 {
    /// New operation accepted.
    Accepted,
    /// Exact immutable request was already accepted.
    AlreadyAccepted,
    /// Existing exact operation is terminal.
    Terminal,
}

/// Result of one start/replay call.
#[derive(Clone, Eq, PartialEq)]
pub struct ApplicationExportStartResultV1 {
    disposition: ApplicationExportStartDispositionV1,
    operation: ApplicationExportOperationV1,
    cursor: Option<ApplicationExportCursor>,
}

impl ApplicationExportStartResultV1 {
    /// Checks that only a nonterminal operation exposes a next-page cursor.
    pub fn new(
        disposition: ApplicationExportStartDispositionV1,
        operation: ApplicationExportOperationV1,
        cursor: Option<ApplicationExportCursor>,
    ) -> Result<Self, ServiceDtoError> {
        if operation.phase().is_terminal() == cursor.is_some()
            || (disposition == ApplicationExportStartDispositionV1::Terminal
                && !operation.phase().is_terminal())
        {
            return Err(ServiceDtoError::InvalidShape);
        }
        Ok(Self {
            disposition,
            operation,
            cursor,
        })
    }

    /// Idempotency disposition.
    #[must_use]
    pub const fn disposition(&self) -> ApplicationExportStartDispositionV1 {
        self.disposition
    }

    /// Current protected operation observation.
    #[must_use]
    pub const fn operation(&self) -> &ApplicationExportOperationV1 {
        &self.operation
    }

    /// First/next cursor for a nonterminal operation.
    #[must_use]
    pub const fn cursor(&self) -> Option<&ApplicationExportCursor> {
        self.cursor.as_ref()
    }
}

/// Protected result of status or cancellation lookup.
#[derive(Clone, Eq, PartialEq)]
pub enum GetApplicationExportResultV1 {
    /// No current-authority-visible operation exists for the identity.
    NotFound,
    /// Current protected operation observation.
    Found(Box<ApplicationExportOperationV1>),
}

/// Move-only start submission carrying one fresh exact V5 safe-point proof.
pub struct AuthorizedApplicationExportStartV1 {
    request_id: RequestId,
    ingress: ServiceIngressKindV1,
    request: StartApplicationExportRequest,
    authorization: Box<AuthorizedApplicationExportV1>,
}

impl AuthorizedApplicationExportStartV1 {
    pub(crate) const fn new(
        request_id: RequestId,
        ingress: ServiceIngressKindV1,
        request: StartApplicationExportRequest,
        authorization: Box<AuthorizedApplicationExportV1>,
    ) -> Self {
        Self {
            request_id,
            ingress,
            request,
            authorization,
        }
    }

    /// Separates exact immutable input from its move-only current proof.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        RequestId,
        ServiceIngressKindV1,
        StartApplicationExportRequest,
        Box<AuthorizedApplicationExportV1>,
    ) {
        (
            self.request_id,
            self.ingress,
            self.request,
            self.authorization,
        )
    }
}

impl fmt::Debug for AuthorizedApplicationExportStartV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AuthorizedApplicationExportStartV1([REDACTED])")
    }
}

/// Move-only bounded-page submission carrying one fresh release proof.
pub struct AuthorizedApplicationExportPageV1 {
    request: GetApplicationExportPageRequest,
    authorization: Box<AuthorizedApplicationExportV1>,
}

impl AuthorizedApplicationExportPageV1 {
    pub(crate) const fn new(
        request: GetApplicationExportPageRequest,
        authorization: Box<AuthorizedApplicationExportV1>,
    ) -> Self {
        Self {
            request,
            authorization,
        }
    }

    /// Separates the exact cursor request from its current proof.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        GetApplicationExportPageRequest,
        Box<AuthorizedApplicationExportV1>,
    ) {
        (self.request, self.authorization)
    }
}

impl fmt::Debug for AuthorizedApplicationExportPageV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AuthorizedApplicationExportPageV1([REDACTED])")
    }
}

/// Move-only status/cancel submission carrying one fresh current proof.
pub struct AuthorizedApplicationExportOperationV1 {
    request: ApplicationExportOperationRequest,
    authorization: Box<AuthorizedApplicationExportV1>,
}

impl AuthorizedApplicationExportOperationV1 {
    pub(crate) const fn new(
        request: ApplicationExportOperationRequest,
        authorization: Box<AuthorizedApplicationExportV1>,
    ) -> Self {
        Self {
            request,
            authorization,
        }
    }

    /// Separates the exact operation selector from its current proof.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        ApplicationExportOperationRequest,
        Box<AuthorizedApplicationExportV1>,
    ) {
        (self.request, self.authorization)
    }
}

impl fmt::Debug for AuthorizedApplicationExportOperationV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AuthorizedApplicationExportOperationV1([REDACTED])")
    }
}

/// Closed failure after a start/page operation was submitted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationExportMutationPortErrorV1 {
    /// Caller-stable identity or cursor binds different immutable input.
    InputMismatch,
    /// Pinned snapshot or durable operation state is temporarily unavailable.
    Unavailable,
    /// A durable transition may have committed without a known response.
    OutcomeUnknown,
    /// Source or retained operation state violated an exact invariant.
    Integrity,
    /// One explicit operation bound was reached.
    LimitExceeded,
    /// Portability proof found an active or partially cleared workflow lease.
    WorkflowNotQuiescent,
}

/// Closed failure while resolving or observing an export operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationExportObservationPortErrorV1 {
    /// Durable operation state is temporarily unavailable.
    Unavailable,
    /// Source or retained operation state violated an exact invariant.
    Integrity,
}

/// Capacity reserved for one authorized start/replay.
pub type ApplicationExportStartPermitV1 = BoxPortCapacityPermit<
    AuthorizedApplicationExportStartV1,
    ApplicationExportStartResultV1,
    ApplicationExportMutationPortErrorV1,
>;
/// Capacity reserved for one authorized page release.
pub type ApplicationExportPagePermitV1 = BoxPortCapacityPermit<
    AuthorizedApplicationExportPageV1,
    ApplicationExportPageV1,
    ApplicationExportMutationPortErrorV1,
>;
/// Capacity reserved for one authorized observation.
pub type ApplicationExportObservationPermitV1 = BoxPortCapacityPermit<
    AuthorizedApplicationExportOperationV1,
    Option<ApplicationExportOperationV1>,
    ApplicationExportObservationPortErrorV1,
>;
/// Capacity reserved for one authorized cancellation transition.
pub type ApplicationExportCancelPermitV1 = BoxPortCapacityPermit<
    AuthorizedApplicationExportOperationV1,
    Option<ApplicationExportOperationV1>,
    ApplicationExportMutationPortErrorV1,
>;

/// Server-private owner of snapshots, checkpoints, pages, and receipts.
pub trait ApplicationExportCoordinatorPort: Send + Sync {
    /// Resolves only the immutable selection required for a fresh safe point.
    fn resolve_application_export_selection(
        &self,
        operation_id: ApplicationExportOperationId,
        control: &RequestControl,
    ) -> PortFuture<'_, Option<ApplicationExportSelectionV1>, ApplicationExportObservationPortErrorV1>;

    /// Reserves bounded capacity before the final start authorization check.
    fn reserve_application_export_start(
        &self,
        control: &RequestControl,
    ) -> PortFuture<'_, ApplicationExportStartPermitV1, PortAdmissionError>;

    /// Reserves bounded capacity before the final page authorization check.
    fn reserve_application_export_page(
        &self,
        control: &RequestControl,
    ) -> PortFuture<'_, ApplicationExportPagePermitV1, PortAdmissionError>;

    /// Reserves bounded capacity before a protected observation.
    fn reserve_application_export_observation(
        &self,
        control: &RequestControl,
    ) -> PortFuture<'_, ApplicationExportObservationPermitV1, PortAdmissionError>;

    /// Reserves bounded capacity before an authorized cancellation transition.
    fn reserve_application_export_cancel(
        &self,
        control: &RequestControl,
    ) -> PortFuture<'_, ApplicationExportCancelPermitV1, PortAdmissionError>;
}

/// Operator-only application-export surface.
pub trait ApplicationExportApplication: Send + Sync {
    /// Starts or exactly replays one immutable snapshot-bound operation.
    fn start_application_export(
        &self,
        context: RequestContext,
        request_id: RequestId,
        request: StartApplicationExportRequest,
    ) -> ServiceFuture<'_, ApplicationExportStartResultV1>;

    /// Releases one bounded canonical JSONL page after fresh authorization.
    fn get_application_export_page(
        &self,
        context: RequestContext,
        request_id: RequestId,
        request: GetApplicationExportPageRequest,
    ) -> ServiceFuture<'_, ApplicationExportPageV1>;

    /// Observes one protected durable checkpoint or terminal receipt.
    fn get_application_export(
        &self,
        context: RequestContext,
        request_id: RequestId,
        request: ApplicationExportOperationRequest,
    ) -> ServiceFuture<'_, GetApplicationExportResultV1>;

    /// Closes one nonterminal operation with an incomplete durable receipt.
    fn cancel_application_export(
        &self,
        context: RequestContext,
        request_id: RequestId,
        request: ApplicationExportOperationRequest,
    ) -> ServiceFuture<'_, GetApplicationExportResultV1>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_application::ApplicationPortabilityManifest;
    use riffdb_types::ApplicationExportOperationId;

    fn operation_id() -> ApplicationExportOperationId {
        ApplicationExportOperationId::from_unix_milliseconds_and_random(1, [0x42; 10])
            .expect("operation")
    }

    fn portability_manifest() -> ApplicationPortabilityManifest {
        ApplicationPortabilityManifest::decode_canonical(include_bytes!(
            "../../../fixtures/export/openfga/portability-manifest-v2.json"
        ))
        .expect("portability manifest")
    }

    #[test]
    fn portability_intent_is_an_exact_request_identity_and_requires_mapped_classes() {
        let manifest = portability_manifest();
        let lineage = manifest.input().contract_lineage.clone();
        let selected = ApplicationExportSelectionV1::new(
            lineage.clone(),
            riffdb_types::CapabilityApplicationExportScopeV1::WholeApplication,
            true,
            false,
            false,
            false,
        )
        .expect("selection");
        let request = StartApplicationExportRequest::new_portability(
            operation_id(),
            selected,
            manifest.clone(),
            MIN_APPLICATION_EXPORT_LEASE_SECONDS,
        )
        .expect("portable request");
        assert_eq!(
            request
                .intent()
                .portability_manifest()
                .map(|value| value.identity()),
            Some(manifest.identity())
        );

        let missing_entities = ApplicationExportSelectionV1::new(
            lineage,
            riffdb_types::CapabilityApplicationExportScopeV1::WholeApplication,
            false,
            true,
            false,
            false,
        )
        .expect("event-only selection");
        assert_eq!(
            StartApplicationExportRequest::new_portability(
                operation_id(),
                missing_entities,
                manifest,
                MIN_APPLICATION_EXPORT_LEASE_SECONDS,
            ),
            Err(ServiceDtoError::InvalidShape)
        );
    }

    #[test]
    fn page_hash_binds_position_class_content_and_terminal_shape() {
        let line = CanonicalApplicationExportJsonLine::new(b"{\"type\":\"Ticket\"}".to_vec())
            .expect("line");
        let cursor = ApplicationExportCursor::new(vec![0x44; 32]).expect("cursor");
        let page = ApplicationExportPageV1::new(
            operation_id(),
            NonZeroU64::new(1).expect("page"),
            ApplicationExportClassV1::Entity,
            vec![line.clone()],
            Some(cursor.clone()),
            false,
            false,
        )
        .expect("page");
        let changed = ApplicationExportPageV1::new(
            operation_id(),
            NonZeroU64::new(2).expect("page"),
            ApplicationExportClassV1::Entity,
            vec![line],
            Some(cursor),
            false,
            false,
        )
        .expect("changed page");
        assert_ne!(page.page_hash(), changed.page_hash());
        assert!(!format!("{page:?}").contains("Ticket"));
    }

    #[test]
    fn terminal_and_cursor_shapes_cannot_be_confused() {
        let line = CanonicalApplicationExportJsonLine::new(b"{}".to_vec()).expect("line");
        assert!(
            ApplicationExportPageV1::new(
                operation_id(),
                NonZeroU64::new(1).expect("page"),
                ApplicationExportClassV1::Entity,
                vec![line],
                None,
                false,
                false,
            )
            .is_err()
        );
    }
}
