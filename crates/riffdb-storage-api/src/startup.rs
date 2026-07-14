//! Exclusive structural startup evidence and dormant-port handoff.

use std::error::Error;
use std::fmt;
use std::num::{NonZeroU32, NonZeroU64};

use riffdb_types::{
    ContractBundleHash, ContractLineage, ContractVersion, DIGEST_SCHEME_V1, DatabaseId,
    DigestKeyId, EntityKey, EntityTypeId, IndexEntryKey, IndexId, Timestamp,
};

use crate::{
    DurableKeySchemaBindingV1, ExecutablePlanRef, MAX_CATALOG_BUNDLE_BYTES,
    MAX_HISTORICAL_EVIDENCE_PAGE_BYTES, MAX_INTEGRITY_FINDINGS, MAX_READABLE_DIGEST_KEYS,
    MAX_SCAN_PAGE_ENTRIES, StorageError, StorageValueError, StoredEntityRecordV1,
    StoredIndexEntryV1, StoredIndexEpochV1, StructurallyDecodedIndexRangePrefixV1,
};

/// A process-local, non-durable structural-open session identity.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OpenSessionId(NonZeroU64);

impl OpenSessionId {
    /// Creates a process-local session identity, rejecting zero.
    #[must_use]
    pub const fn new(value: u64) -> Option<Self> {
        match NonZeroU64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns its process-local numeric value.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// One nonsecret readable digest scheme/key identity.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ReadableDigestKey {
    scheme: u8,
    key_id: DigestKeyId,
}

impl ReadableDigestKey {
    /// Constructs the accepted v1 digest identity.
    #[must_use]
    pub const fn v1(key_id: DigestKeyId) -> Self {
        Self {
            scheme: DIGEST_SCHEME_V1,
            key_id,
        }
    }

    /// Reconstructs a supported readable identity from stored components.
    pub const fn new(scheme: u8, key_id: DigestKeyId) -> Result<Self, StorageValueError> {
        if scheme != DIGEST_SCHEME_V1 {
            return Err(StorageValueError::InvalidShape);
        }
        Ok(Self { scheme, key_id })
    }

    /// Returns the immutable digest scheme.
    #[must_use]
    pub const fn scheme(self) -> u8 {
        self.scheme
    }

    /// Returns the nonsecret immutable digest-key ID.
    #[must_use]
    pub const fn key_id(self) -> DigestKeyId {
        self.key_id
    }
}

macro_rules! digest_inventory {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, Debug, Eq, PartialEq)]
        pub struct $name(Vec<ReadableDigestKey>);

        impl $name {
            /// Sorts and validates one through eight readable scheme/key identities.
            pub fn new(mut entries: Vec<ReadableDigestKey>) -> Result<Self, StorageValueError> {
                if entries.is_empty() {
                    return Err(StorageValueError::Empty);
                }
                if entries.len() > MAX_READABLE_DIGEST_KEYS {
                    return Err(StorageValueError::LimitExceeded);
                }
                entries.sort_unstable();
                if entries.windows(2).any(|pair| pair[0] == pair[1]) {
                    return Err(StorageValueError::Duplicate);
                }
                Ok(Self(entries))
            }

            /// Borrows identities in canonical scheme/key-ID order.
            #[must_use]
            pub fn as_slice(&self) -> &[ReadableDigestKey] {
                &self.0
            }
        }
    };
}

digest_inventory!(
    /// Readable capability-token digest schemes and key IDs.
    ReadableCapabilityDigestInventory
);
digest_inventory!(
    /// Readable command-idempotency digest schemes and key IDs.
    ReadableIdempotencyDigestInventory
);

/// Already sampled, value-only inputs to one exclusive structural pass.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StartupValidationInputs {
    authorization_time: Timestamp,
    capability_digests: ReadableCapabilityDigestInventory,
    idempotency_digests: ReadableIdempotencyDigestInventory,
}

impl StartupValidationInputs {
    /// Constructs startup inputs without retaining clocks, providers, or secrets.
    #[must_use]
    pub const fn new(
        authorization_time: Timestamp,
        capability_digests: ReadableCapabilityDigestInventory,
        idempotency_digests: ReadableIdempotencyDigestInventory,
    ) -> Self {
        Self {
            authorization_time,
            capability_digests,
            idempotency_digests,
        }
    }

    /// Returns the one checked authorization-time sample.
    #[must_use]
    pub const fn authorization_time(&self) -> Timestamp {
        self.authorization_time
    }

    /// Borrows readable capability-token identities.
    #[must_use]
    pub const fn capability_digests(&self) -> &ReadableCapabilityDigestInventory {
        &self.capability_digests
    }

    /// Borrows readable idempotency identities.
    #[must_use]
    pub const fn idempotency_digests(&self) -> &ReadableIdempotencyDigestInventory {
        &self.idempotency_digests
    }
}

/// A checked nonzero item limit for one evidence page.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EvidencePageLimit(NonZeroU32);

impl EvidencePageLimit {
    /// Constructs a limit in `1..=500`.
    #[must_use]
    pub const fn new(value: u32) -> Option<Self> {
        if value == 0 || value as usize > MAX_SCAN_PAGE_ENTRIES {
            return None;
        }
        match NonZeroU32::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the checked item limit.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

macro_rules! evidence_position_type {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name {
            database_id: DatabaseId,
            open_session_id: OpenSessionId,
            position: u64,
        }

        impl $name {
            /// Creates the exact initial cursor for one database/open session.
            #[must_use]
            pub const fn start(database_id: DatabaseId, open_session_id: OpenSessionId) -> Self {
                Self {
                    database_id,
                    open_session_id,
                    position: 0,
                }
            }

            /// Advances the opaque position by a nonzero checked amount.
            pub fn advanced(self, amount: u64) -> Result<Self, StorageValueError> {
                if amount == 0 {
                    return Err(StorageValueError::InvalidShape);
                }
                let position = self
                    .position
                    .checked_add(amount)
                    .ok_or(StorageValueError::SizeOverflow)?;
                Ok(Self { position, ..self })
            }

            /// Returns the bound durable database identity.
            #[must_use]
            pub const fn database_id(self) -> DatabaseId {
                self.database_id
            }

            /// Returns the bound process-local open-session identity.
            #[must_use]
            pub const fn open_session_id(self) -> OpenSessionId {
                self.open_session_id
            }

            /// Returns the exact opaque position within this pass.
            #[must_use]
            pub const fn position(self) -> u64 {
                self.position
            }

            fn same_session(self, other: Self) -> bool {
                self.database_id == other.database_id
                    && self.open_session_id == other.open_session_id
            }
        }
    };
}

evidence_position_type!(
    /// Continuation for the complete storage-structural namespace scan.
    StructuralEvidenceCursor
);
evidence_position_type!(
    /// Continuation for ordered IR-opaque historical semantic evidence.
    HistoricalEvidenceCursor
);

/// Structural component owning one integrity finding.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum StructuralFindingScope {
    /// Authoritative metadata, catalog references, command state, or entity state.
    Authoritative,
    /// Rebuildable outbox delivery-status overlay.
    OutboxDelivery,
    /// Rebuildable projection state.
    Projection,
}

/// Closed safe structural finding classification.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum StructuralFindingCode {
    /// A record or key failed canonical decoding.
    MalformedRecord,
    /// A required reciprocal record or cross-link is absent.
    MissingCrossLink,
    /// Two linked records disagree.
    CrossLinkMismatch,
    /// An ordered sequence contains a gap or metadata mismatch.
    SequenceDiscontinuity,
    /// A stored digest scheme or key ID is not readable.
    DigestUnavailable,
    /// A structural hard bound is violated.
    LimitExceeded,
    /// A retained projection generation or frontier is inconsistent.
    ProjectionStateMismatch,
    /// An outbox status has no authoritative intent.
    OrphanedOutboxStatus,
}

/// One bounded redacted structural finding.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StructuralFinding {
    scope: StructuralFindingScope,
    code: StructuralFindingCode,
}

impl StructuralFinding {
    /// Constructs a safe finding without keys, values, or engine text.
    #[must_use]
    pub const fn new(scope: StructuralFindingScope, code: StructuralFindingCode) -> Self {
        Self { scope, code }
    }

    /// Returns the component whose health owns the finding.
    #[must_use]
    pub const fn scope(self) -> StructuralFindingScope {
        self.scope
    }

    /// Returns the closed finding code.
    #[must_use]
    pub const fn code(self) -> StructuralFindingCode {
        self.code
    }
}

/// Backend-owned proof that every structural namespace reached exact end.
///
/// A concrete backend exposes its token type but keeps its constructor private.
pub trait StructuralEvidenceEnd {
    /// Returns the database/session-bound final cursor.
    fn cursor(&self) -> StructuralEvidenceCursor;
}

/// One bounded structural-scan result, never an implicit end-of-scan signal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StructuralEvidencePage<E> {
    /// A non-final page and its exact next continuation.
    Page {
        /// Cursor used to request this page.
        start: StructuralEvidenceCursor,
        /// Redacted structural findings discovered on this page.
        findings: Vec<StructuralFinding>,
        /// Strictly advancing continuation for the next page.
        next: StructuralEvidenceCursor,
    },
    /// Unambiguous exact end of every structural namespace.
    ExactEnd(E),
}

impl<E> StructuralEvidencePage<E> {
    /// Validates a non-final structural page and its advancing continuation.
    pub fn page(
        start: StructuralEvidenceCursor,
        findings: Vec<StructuralFinding>,
        next: StructuralEvidenceCursor,
    ) -> Result<Self, StorageValueError> {
        if findings.len() > MAX_INTEGRITY_FINDINGS {
            return Err(StorageValueError::LimitExceeded);
        }
        if !start.same_session(next) || next.position <= start.position {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(Self::Page {
            start,
            findings,
            next,
        })
    }
}

/// Immutable canonical bundle bytes exposed without interpreting contract IR.
#[derive(Clone, Eq, PartialEq)]
pub struct HistoricalBundleBytes(Vec<u8>);

impl HistoricalBundleBytes {
    /// Constructs one bounded nonempty canonical bundle byte document.
    pub fn new(bytes: Vec<u8>) -> Result<Self, StorageValueError> {
        if bytes.is_empty() {
            return Err(StorageValueError::Empty);
        }
        if bytes.len() > MAX_CATALOG_BUNDLE_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(Self(bytes))
    }

    /// Borrows the exact stored bundle bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for HistoricalBundleBytes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HistoricalBundleBytes")
            .field("bytes", &"[REDACTED]")
            .field("length", &self.0.len())
            .finish()
    }
}

/// Storage-structural identity and bytes for one immutable historical bundle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoricalBundleEvidence {
    lineage: ContractLineage,
    version: ContractVersion,
    bundle_hash: ContractBundleHash,
    bytes: HistoricalBundleBytes,
}

impl HistoricalBundleEvidence {
    /// Constructs one IR-opaque historical bundle observation.
    #[must_use]
    pub const fn new(
        lineage: ContractLineage,
        version: ContractVersion,
        bundle_hash: ContractBundleHash,
        bytes: HistoricalBundleBytes,
    ) -> Self {
        Self {
            lineage,
            version,
            bundle_hash,
            bytes,
        }
    }

    /// Borrows the exact lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Returns the application version.
    #[must_use]
    pub const fn version(&self) -> ContractVersion {
        self.version
    }

    /// Returns the recorded immutable bundle hash.
    #[must_use]
    pub const fn bundle_hash(&self) -> ContractBundleHash {
        self.bundle_hash
    }

    /// Borrows the exact stored canonical bytes.
    #[must_use]
    pub const fn bytes(&self) -> &HistoricalBundleBytes {
        &self.bytes
    }
}

/// Storage-structural active-catalog relationship, without IR interpretation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoricalActiveCatalogEvidence {
    lineage: ContractLineage,
    version: ContractVersion,
    bundle_hash: ContractBundleHash,
}

impl HistoricalActiveCatalogEvidence {
    /// Constructs the exact recorded active relation.
    #[must_use]
    pub const fn new(
        lineage: ContractLineage,
        version: ContractVersion,
        bundle_hash: ContractBundleHash,
    ) -> Self {
        Self {
            lineage,
            version,
            bundle_hash,
        }
    }

    /// Borrows the contract lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }

    /// Returns the active application version.
    #[must_use]
    pub const fn version(&self) -> ContractVersion {
        self.version
    }

    /// Returns the active immutable bundle hash.
    #[must_use]
    pub const fn bundle_hash(&self) -> ContractBundleHash {
        self.bundle_hash
    }
}

/// One persisted key whose schema semantics remain deliberately undecoded.
#[derive(Clone, Eq, PartialEq)]
pub enum IrOpaquePersistedKeyV1 {
    /// One current entity-table key and repeated owner identity.
    Entity {
        /// Entity type expected in the envelope.
        entity_type_id: EntityTypeId,
        /// Structurally decoded opaque key bytes.
        key: EntityKey,
    },
    /// One current secondary-index-table key and repeated owner identity.
    IndexEntry {
        /// Index expected in the envelope.
        index_id: IndexId,
        /// Structurally decoded opaque complete entry bytes.
        key: IndexEntryKey,
    },
    /// One persisted index-epoch prefix key.
    IndexRangePrefix(StructurallyDecodedIndexRangePrefixV1),
}

impl IrOpaquePersistedKeyV1 {
    /// Checks the repeated entity owner without interpreting key components.
    pub fn entity(entity_type_id: EntityTypeId, key: EntityKey) -> Result<Self, StorageValueError> {
        if key.entity_type_id() != entity_type_id {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(Self::Entity {
            entity_type_id,
            key,
        })
    }

    /// Checks the repeated index owner without interpreting key components.
    pub fn index_entry(index_id: IndexId, key: IndexEntryKey) -> Result<Self, StorageValueError> {
        if key.index_id() != index_id {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(Self::IndexEntry { index_id, key })
    }

    /// Wraps an envelope-checked prefix without claiming component completeness.
    #[must_use]
    pub const fn index_range_prefix(prefix: StructurallyDecodedIndexRangePrefixV1) -> Self {
        Self::IndexRangePrefix(prefix)
    }

    fn canonical_key(&self) -> Vec<u8> {
        let (tag, owner, bytes) = match self {
            Self::Entity {
                entity_type_id,
                key,
            } => (0x01, entity_type_id.to_be_bytes(), key.as_bytes()),
            Self::IndexEntry { index_id, key } => (0x02, index_id.to_be_bytes(), key.as_bytes()),
            Self::IndexRangePrefix(prefix) => {
                (0x03, prefix.index_id().to_be_bytes(), prefix.as_bytes())
            }
        };
        let mut output = Vec::with_capacity(1 + 4 + 4 + bytes.len());
        output.push(tag);
        output.extend_from_slice(&owner);
        output.extend_from_slice(
            &u32::try_from(bytes.len())
                .expect("foundational key hard bound fits u32")
                .to_be_bytes(),
        );
        output.extend_from_slice(bytes);
        output
    }

    fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        let bytes = match self {
            Self::Entity { key, .. } => key.as_bytes(),
            Self::IndexEntry { key, .. } => key.as_bytes(),
            Self::IndexRangePrefix(prefix) => prefix.as_bytes(),
        };
        bytes
            .len()
            .checked_add(1 + 4 + 4)
            .ok_or(StorageValueError::SizeOverflow)
    }
}

impl fmt::Debug for IrOpaquePersistedKeyV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("IrOpaquePersistedKeyV1([REDACTED])")
    }
}

/// Schema-bound startup evidence for one persisted opaque key.
#[derive(Clone, Eq, PartialEq)]
pub struct HistoricalPersistedKeyEvidenceV1 {
    schema: DurableKeySchemaBindingV1,
    key: IrOpaquePersistedKeyV1,
}

impl HistoricalPersistedKeyEvidenceV1 {
    /// Derives evidence only from an entity's durable post-image binding.
    #[must_use]
    pub fn from_entity(record: &StoredEntityRecordV1) -> Self {
        Self {
            schema: record.schema_binding().clone(),
            key: IrOpaquePersistedKeyV1::Entity {
                entity_type_id: record.target().entity_type_id(),
                key: record.target().key().clone(),
            },
        }
    }

    /// Derives evidence only from an index entry's durable post-image binding.
    #[must_use]
    pub fn from_index_entry(record: &StoredIndexEntryV1) -> Self {
        Self {
            schema: record.schema_binding().clone(),
            key: IrOpaquePersistedKeyV1::IndexEntry {
                index_id: record.key().index_id(),
                key: record.key().clone(),
            },
        }
    }

    /// Derives evidence only from a persisted range-epoch post-image binding.
    #[must_use]
    pub fn from_index_epoch(record: &StoredIndexEpochV1) -> Self {
        Self {
            schema: record.schema_binding().clone(),
            key: IrOpaquePersistedKeyV1::IndexRangePrefix(record.target().clone()),
        }
    }

    /// Borrows the exact bundle whose `KeySchema` must validate the key.
    #[must_use]
    pub const fn schema(&self) -> &DurableKeySchemaBindingV1 {
        &self.schema
    }

    /// Borrows structural key bytes without semantic decoding authority.
    #[must_use]
    pub const fn key(&self) -> &IrOpaquePersistedKeyV1 {
        &self.key
    }

    fn canonical_order_key(&self) -> Vec<u8> {
        let mut output = Vec::new();
        push_lineage(&mut output, self.schema.lineage());
        output.extend_from_slice(&self.schema.contract_version().to_be_bytes());
        output.extend_from_slice(self.schema.bundle_hash().as_bytes());
        output.extend_from_slice(&self.key.canonical_key());
        output
    }

    fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        (4 + self.schema.lineage().as_bytes().len())
            .checked_add(8 + 32)
            .and_then(|value| value.checked_add(self.key.semantic_bytes().ok()?))
            .ok_or(StorageValueError::SizeOverflow)
    }
}

impl fmt::Debug for HistoricalPersistedKeyEvidenceV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HistoricalPersistedKeyEvidenceV1")
            .field("schema", &self.schema)
            .field("key", &"[REDACTED]")
            .finish()
    }
}

/// One ordered, storage-structural historical fact consumed by the catalog.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HistoricalSemanticEvidence {
    /// One immutable stored bundle and its structural identity.
    Bundle(HistoricalBundleEvidence),
    /// One exact plan reference found in authoritative durable state.
    PlanReference(ExecutablePlanRef),
    /// The singleton active relation, absent only in the accepted initialization state.
    ActiveCatalog(Option<HistoricalActiveCatalogEvidence>),
    /// One persisted key requiring exact catalog-owned historical schema validation.
    PersistedKey(HistoricalPersistedKeyEvidenceV1),
}

impl HistoricalSemanticEvidence {
    fn canonical_order_key(&self) -> Vec<u8> {
        let mut key = Vec::new();
        match self {
            Self::Bundle(bundle) => {
                key.push(0x01);
                push_lineage(&mut key, bundle.lineage());
                key.extend_from_slice(&bundle.version().to_be_bytes());
                key.extend_from_slice(bundle.bundle_hash().as_bytes());
            }
            Self::PlanReference(plan) => {
                key.push(0x02);
                push_lineage(&mut key, plan.contract_lineage());
                key.extend_from_slice(&plan.contract_version().to_be_bytes());
                key.extend_from_slice(plan.contract_bundle_hash().as_bytes());
                key.extend_from_slice(&plan.command_id().to_be_bytes());
                key.extend_from_slice(plan.command_plan_hash().as_bytes());
            }
            Self::ActiveCatalog(None) => key.extend_from_slice(&[0x03, 0x00]),
            Self::ActiveCatalog(Some(active)) => {
                key.extend_from_slice(&[0x03, 0x01]);
                push_lineage(&mut key, active.lineage());
                key.extend_from_slice(&active.version().to_be_bytes());
                key.extend_from_slice(active.bundle_hash().as_bytes());
            }
            Self::PersistedKey(evidence) => {
                key.push(0x04);
                key.extend_from_slice(&evidence.canonical_order_key());
            }
        }
        key
    }

    fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        match self {
            Self::Bundle(bundle) => bundle
                .bytes()
                .as_bytes()
                .len()
                .checked_add(1 + 4 + bundle.lineage().as_bytes().len() + 8 + 32 + 4)
                .ok_or(StorageValueError::SizeOverflow),
            Self::PlanReference(plan) => (1 + 4 + plan.contract_lineage().as_bytes().len())
                .checked_add(8 + 32 + 4 + 32)
                .ok_or(StorageValueError::SizeOverflow),
            Self::ActiveCatalog(None) => Ok(1 + 1),
            Self::ActiveCatalog(Some(active)) => (1 + 1 + 4 + active.lineage().as_bytes().len())
                .checked_add(8 + 32)
                .ok_or(StorageValueError::SizeOverflow),
            Self::PersistedKey(evidence) => evidence
                .semantic_bytes()?
                .checked_add(1)
                .ok_or(StorageValueError::SizeOverflow),
        }
    }
}

/// Backend-owned proof that historical evidence reached exact end.
///
/// A concrete backend exposes its token type but keeps its constructor private.
pub trait HistoricalEvidenceEnd {
    /// Returns the database/session-bound final cursor.
    fn cursor(&self) -> HistoricalEvidenceCursor;
}

/// One bounded historical-evidence result, never an implicit end signal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HistoricalEvidencePage<E> {
    /// A non-final ordered page and its exact continuation.
    Page {
        /// Cursor used to request this page.
        start: HistoricalEvidenceCursor,
        /// Strictly ordered IR-opaque evidence.
        evidence: Vec<HistoricalSemanticEvidence>,
        /// Strictly advancing continuation for the next page.
        next: HistoricalEvidenceCursor,
    },
    /// Unambiguous exact end of all historical evidence.
    ExactEnd(E),
}

impl<E> HistoricalEvidencePage<E> {
    /// Validates count, content bytes, ordering, and session-bound advancement.
    pub fn page(
        start: HistoricalEvidenceCursor,
        evidence: Vec<HistoricalSemanticEvidence>,
        next: HistoricalEvidenceCursor,
    ) -> Result<Self, StorageValueError> {
        if evidence.is_empty() {
            return Err(StorageValueError::Empty);
        }
        if evidence.len() > MAX_SCAN_PAGE_ENTRIES {
            return Err(StorageValueError::LimitExceeded);
        }
        if !start.same_session(next) || next.position <= start.position {
            return Err(StorageValueError::IdentityMismatch);
        }
        let mut total = 0usize;
        let mut prior_key: Option<Vec<u8>> = None;
        for item in &evidence {
            total = total
                .checked_add(item.semantic_bytes()?)
                .ok_or(StorageValueError::SizeOverflow)?;
            if total > MAX_HISTORICAL_EVIDENCE_PAGE_BYTES {
                return Err(StorageValueError::LimitExceeded);
            }
            let key = item.canonical_order_key();
            if prior_key.as_ref().is_some_and(|prior| prior >= &key) {
                return Err(StorageValueError::NonCanonicalOrder);
            }
            prior_key = Some(key);
        }
        Ok(Self::Page {
            start,
            evidence,
            next,
        })
    }
}

/// Exclusive engine-neutral startup scan session.
///
/// Implementations must withhold every operational and mutation port until
/// [`finish`](Self::finish) consumes this session with both exact-end tokens.
pub trait StructuralEvidenceSession: Sized {
    /// Backend-owned collection of still-dormant ports released after validation.
    type DormantPorts: DormantPortBundle;
    /// Backend-owned unforgeable structural exact-end token.
    type StructuralEnd: StructuralEvidenceEnd;
    /// Backend-owned unforgeable historical exact-end token.
    type HistoricalEnd: HistoricalEvidenceEnd;

    /// Returns the durable database identity bound to this session.
    fn database_id(&self) -> DatabaseId;

    /// Returns the process-local open-session identity.
    fn open_session_id(&self) -> OpenSessionId;

    /// Reads one bounded structural page from the exact supplied continuation.
    fn read_structural_evidence(
        &mut self,
        cursor: StructuralEvidenceCursor,
        limit: EvidencePageLimit,
    ) -> Result<StructuralEvidencePage<Self::StructuralEnd>, StorageError>;

    /// Reads one bounded ordered historical-evidence page.
    fn read_historical_evidence(
        &mut self,
        cursor: HistoricalEvidenceCursor,
        limit: EvidencePageLimit,
    ) -> Result<HistoricalEvidencePage<Self::HistoricalEnd>, StorageError>;

    /// Reads one exact immutable historical bundle within this startup session.
    ///
    /// Returned opaque bytes are bounded by [`MAX_CATALOG_BUNDLE_BYTES`]. This
    /// point read does not advance or otherwise alter either evidence cursor.
    /// It remains available after either stream reaches exact end, until
    /// [`finish`](Self::finish) consumes the session.
    fn read_historical_bundle(
        &mut self,
        lineage: &ContractLineage,
        contract_version: ContractVersion,
        bundle_hash: ContractBundleHash,
    ) -> Result<Option<HistoricalBundleEvidence>, StorageError>;

    /// Consumes a completely scanned session and releases dormant ports.
    fn finish(
        self,
        structural_end: Self::StructuralEnd,
        historical_end: Self::HistoricalEnd,
    ) -> Result<StructurallyOpened<Self::DormantPorts>, StorageError>;
}

/// A dormant initialized backend that can enter exactly one exclusive session.
pub trait StructuralEvidenceOpen: Sized {
    /// Exclusive session type produced by this dormant backend.
    type Session: StructuralEvidenceSession;

    /// Consumes the dormant open and begins the complete structural pass.
    fn begin_structural_evidence(
        self,
        inputs: StartupValidationInputs,
    ) -> Result<Self::Session, StorageError>;
}

/// Structurally validated, still-dormant backend ports.
///
/// This is not catalog validation and is never operational readiness. Only
/// server composition may join it with the separate catalog-owned proof.
pub struct StructurallyOpened<P> {
    database_id: DatabaseId,
    open_session_id: OpenSessionId,
    dormant_ports: P,
}

/// Backend-owned dormant ports and their unforgeable completion authority.
pub trait DormantPortBundle: Sized {
    /// Public token type with a backend-private constructor.
    type CompletionAuthority;
}

impl<P: DormantPortBundle> StructurallyOpened<P> {
    /// Constructs the handoff returned by a successfully finished backend session.
    ///
    /// The authority type must have no public constructor in a concrete backend.
    #[must_use]
    pub fn from_finished_session(
        database_id: DatabaseId,
        open_session_id: OpenSessionId,
        dormant_ports: P,
        _authority: P::CompletionAuthority,
    ) -> Self {
        Self {
            database_id,
            open_session_id,
            dormant_ports,
        }
    }
}

impl<P> StructurallyOpened<P> {
    /// Returns the durable database identity bound to the handoff.
    #[must_use]
    pub const fn database_id(&self) -> DatabaseId {
        self.database_id
    }

    /// Returns the process-local session identity bound to the handoff.
    #[must_use]
    pub const fn open_session_id(&self) -> OpenSessionId {
        self.open_session_id
    }

    /// Consumes the structural handoff into its IDs and dormant port collection.
    #[must_use]
    pub fn into_parts(self) -> (DatabaseId, OpenSessionId, P) {
        (self.database_id, self.open_session_id, self.dormant_ports)
    }
}

impl<P> fmt::Debug for StructurallyOpened<P> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StructurallyOpened")
            .field("database_id", &self.database_id)
            .field("open_session_id", &self.open_session_id)
            .field("dormant_ports", &"[DORMANT]")
            .finish()
    }
}

/// A safe marker error used when a consumer rejects an evidence sequence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EvidenceSequenceError;

impl fmt::Display for EvidenceSequenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("startup evidence sequence is incomplete or mismatched")
    }
}

impl Error for EvidenceSequenceError {}

fn push_lineage(output: &mut Vec<u8>, lineage: &ContractLineage) {
    let length =
        u32::try_from(lineage.as_bytes().len()).expect("foundational lineage bound fits u32");
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(lineage.as_bytes());
}
