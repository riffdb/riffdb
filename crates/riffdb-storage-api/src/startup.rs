//! Exclusive structural startup evidence and dormant-port handoff.

use std::error::Error;
use std::fmt;
use std::num::{NonZeroU32, NonZeroU64};

use riffdb_types::{
    CanonicalRecord, CapabilityId, ContractBundleHash, ContractLineage, ContractVersion,
    DIGEST_SCHEME_V1, DatabaseId, DigestKeyId, EntityKey, EntityTypeId, IndexEntryKey,
    MAX_CAPABILITY_PARTITIONS, PartitionKey, ScopedPartitionV1, Timestamp,
};

use crate::{
    DurableKeySchemaBindingV1, EncodedContentCharge, ExecutablePlanRef, MAX_CATALOG_BUNDLE_BYTES,
    MAX_HISTORICAL_EVIDENCE_PAGE_BYTES, MAX_INDEX_MIGRATION_PAGE_BYTES,
    MAX_INDEX_MIGRATION_PAGE_ENTRIES, MAX_INTEGRITY_FINDINGS, MAX_READABLE_DIGEST_KEYS,
    MAX_SCAN_PAGE_ENTRIES, RetainedMetadataV1, StorageError, StorageValueError,
    StoredEntityRecordV1, StoredIndexEntryV1, StoredIndexEntryV2, StoredIndexEpochV1,
    StructurallyDecodedIndexRangePrefixV1,
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

            #[allow(dead_code, reason = "not every cursor family needs local page validation")]
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
evidence_position_type!(
    /// Continuation for one exclusive physical index-row migration scan.
    IndexMigrationCursor
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

/// The checked semantic interpretation of one physical migration-scan row.
#[derive(Eq, PartialEq)]
pub enum IndexMigrationSemanticRow {
    /// A legacy decode-only row that requires a catalog-derived V2 replacement.
    V1(StoredIndexEntryV1),
    /// A current row whose stored partition must be confirmed by catalog.
    V2(StoredIndexEntryV2),
}

impl IndexMigrationSemanticRow {
    /// Borrows the complete physical key repeated by the semantic record.
    #[must_use]
    pub const fn key(&self) -> &IndexEntryKey {
        match self {
            Self::V1(row) => row.key(),
            Self::V2(row) => row.key(),
        }
    }

    /// Borrows the exact retained historical schema binding.
    #[must_use]
    pub const fn schema_binding(&self) -> &DurableKeySchemaBindingV1 {
        match self {
            Self::V1(row) => row.schema_binding(),
            Self::V2(row) => row.schema_binding(),
        }
    }

    /// Borrows the complete canonical covered values.
    #[must_use]
    pub const fn covered_values(&self) -> &CanonicalRecord {
        match self {
            Self::V1(row) => row.covered_values(),
            Self::V2(row) => row.covered_values(),
        }
    }

    /// Borrows the stored partition for a V2 row.
    #[must_use]
    pub const fn stored_partition(&self) -> Option<&PartitionKey> {
        match self {
            Self::V1(_) => None,
            Self::V2(row) => Some(row.partition_key()),
        }
    }

    /// Returns whether this row is a legacy migration source.
    #[must_use]
    pub const fn is_v1(&self) -> bool {
        matches!(self, Self::V1(_))
    }
}

impl fmt::Debug for IndexMigrationSemanticRow {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let version = match self {
            Self::V1(_) => "V1",
            Self::V2(_) => "V2",
        };
        formatter
            .debug_tuple("IndexMigrationSemanticRow")
            .field(&version)
            .finish()
    }
}

/// One move-only association proved by the canonical durable codec.
///
/// No public constructor exists. WP-065's codec is the sole production caller
/// of `from_codec_checked_parts`; a concrete backend may only move the
/// returned association into a session-bound page after reading the exact bytes.
#[derive(Eq, PartialEq)]
pub struct IndexMigrationRowEvidence {
    physical_key: IndexEntryKey,
    row: IndexMigrationSemanticRow,
    canonical_envelope: Box<[u8]>,
    evidence_page_charge: usize,
    instruction_page_charge: usize,
    conservative_v2_envelope_charge: EncodedContentCharge,
}

impl IndexMigrationRowEvidence {
    /// Binds the values already proved inseparable by the canonical codec.
    ///
    /// This remains crate-private so engines, catalog, and tests cannot combine
    /// independently obtained semantic rows and envelope bytes.
    #[allow(dead_code, reason = "WP-065 is the sole intended caller")]
    pub(crate) fn from_codec_checked_parts(
        physical_key: IndexEntryKey,
        row: IndexMigrationSemanticRow,
        canonical_envelope: Vec<u8>,
        conservative_v2_envelope_charge: EncodedContentCharge,
    ) -> Result<Self, StorageValueError> {
        if row.key() != &physical_key {
            return Err(StorageValueError::IdentityMismatch);
        }
        let exact_envelope_charge = EncodedContentCharge::new(canonical_envelope.len())
            .ok_or(StorageValueError::LimitExceeded)?;
        let evidence_page_charge =
            migration_evidence_charge(physical_key.as_bytes().len(), exact_envelope_charge.get())?;
        let instruction_page_charge = migration_instruction_charge(
            physical_key.as_bytes().len(),
            exact_envelope_charge.get(),
            conservative_v2_envelope_charge.get(),
        )?;
        if evidence_page_charge > MAX_INDEX_MIGRATION_PAGE_BYTES
            || instruction_page_charge > MAX_INDEX_MIGRATION_PAGE_BYTES
        {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(Self {
            physical_key,
            row,
            canonical_envelope: canonical_envelope.into_boxed_slice(),
            evidence_page_charge,
            instruction_page_charge,
            conservative_v2_envelope_charge,
        })
    }

    /// Borrows the exact physical secondary-index key presented to the codec.
    #[must_use]
    pub const fn physical_key(&self) -> &IndexEntryKey {
        &self.physical_key
    }

    /// Borrows the checked V1 or V2 semantic row.
    #[must_use]
    pub const fn row(&self) -> &IndexMigrationSemanticRow {
        &self.row
    }

    /// Borrows the exact observed canonical envelope bytes.
    #[must_use]
    pub fn canonical_envelope(&self) -> &[u8] {
        &self.canonical_envelope
    }

    /// Returns the exact charge used by the migration-evidence page ledger.
    #[must_use]
    pub const fn evidence_page_charge(&self) -> usize {
        self.evidence_page_charge
    }

    /// Returns the conservative charge used by the instruction/write ledger.
    #[must_use]
    pub const fn instruction_page_charge(&self) -> usize {
        self.instruction_page_charge
    }

    /// Returns the codec-proved complete V2 replacement-envelope reservation.
    #[must_use]
    pub const fn conservative_v2_envelope_charge(&self) -> EncodedContentCharge {
        self.conservative_v2_envelope_charge
    }

    fn canonical_order_key(&self) -> Vec<u8> {
        let binding = self.row.schema_binding();
        let mut output = Vec::new();
        push_lineage(&mut output, binding.lineage());
        output.extend_from_slice(&binding.contract_version().to_be_bytes());
        output.extend_from_slice(binding.bundle_hash().as_bytes());
        output.push(0x02);
        output.extend_from_slice(&self.physical_key.index_id().to_be_bytes());
        output.extend_from_slice(
            &u32::try_from(self.physical_key.as_bytes().len())
                .expect("foundational key hard bound fits u32")
                .to_be_bytes(),
        );
        output.extend_from_slice(self.physical_key.as_bytes());
        output
    }
}

impl fmt::Debug for IndexMigrationRowEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IndexMigrationRowEvidence")
            .field("row", &self.row)
            .field("physical_key", &"[REDACTED]")
            .field("canonical_envelope", &"[REDACTED]")
            .field("evidence_page_charge", &self.evidence_page_charge)
            .field("instruction_page_charge", &self.instruction_page_charge)
            .finish()
    }
}

fn migration_evidence_charge(
    physical_key_length: usize,
    canonical_envelope_length: usize,
) -> Result<usize, StorageValueError> {
    1usize
        .checked_add(4)
        .and_then(|value| value.checked_add(physical_key_length))
        .and_then(|value| value.checked_add(4))
        .and_then(|value| value.checked_add(canonical_envelope_length))
        .ok_or(StorageValueError::SizeOverflow)
}

fn migration_instruction_charge(
    physical_key_length: usize,
    expected_envelope_length: usize,
    conservative_replacement_length: usize,
) -> Result<usize, StorageValueError> {
    1usize
        .checked_add(4)
        .and_then(|value| value.checked_add(physical_key_length))
        .and_then(|value| value.checked_add(4))
        .and_then(|value| value.checked_add(expected_envelope_length))
        .and_then(|value| value.checked_add(4))
        .and_then(|value| value.checked_add(conservative_replacement_length))
        .ok_or(StorageValueError::SizeOverflow)
}

/// One IR-opaque explicit entry emitted for a qualifying durable capability.
#[derive(Clone, Eq, PartialEq)]
pub struct HistoricalCapabilityPartitionEvidenceV1 {
    capability_id: CapabilityId,
    entry_ordinal: u16,
    scoped_partition: ScopedPartitionV1,
}

impl HistoricalCapabilityPartitionEvidenceV1 {
    /// Derives one checked zero-based entry from a durable explicit capability scope.
    pub fn from_capability_entry(
        capability: &crate::StoredCapabilityRecordV1,
        entry_ordinal: usize,
    ) -> Result<Self, StorageValueError> {
        if entry_ordinal >= MAX_CAPABILITY_PARTITIONS {
            return Err(StorageValueError::LimitExceeded);
        }
        let scoped_partition = capability
            .grant()
            .partition_scope()
            .explicit_entries()
            .and_then(|entries| entries.get(entry_ordinal))
            .ok_or(StorageValueError::InvalidShape)?
            .clone();
        Ok(Self {
            capability_id: capability.capability_id(),
            entry_ordinal: u16::try_from(entry_ordinal)
                .map_err(|_| StorageValueError::LimitExceeded)?,
            scoped_partition,
        })
    }

    /// Returns the durable capability that owns this explicit entry.
    #[must_use]
    pub const fn capability_id(&self) -> CapabilityId {
        self.capability_id
    }

    /// Returns the exact zero-based position in the canonical explicit scope.
    #[must_use]
    pub const fn entry_ordinal(&self) -> u16 {
        self.entry_ordinal
    }

    /// Borrows the lineage-scoped complete partition key.
    #[must_use]
    pub const fn scoped_partition(&self) -> &ScopedPartitionV1 {
        &self.scoped_partition
    }

    /// Returns the exact process-local `0x05` historical-evidence order key.
    #[must_use]
    pub fn evidence_order_key(&self) -> Vec<u8> {
        let partition_key = self.scoped_partition.partition_key();
        let mut output = Vec::new();
        output.push(0x05);
        output.extend_from_slice(self.capability_id.as_bytes());
        output.extend_from_slice(&self.entry_ordinal.to_be_bytes());
        push_lineage(&mut output, self.scoped_partition.lineage());
        output.extend_from_slice(&partition_key.aggregate_type_id().to_be_bytes());
        output.extend_from_slice(
            &u32::try_from(partition_key.as_bytes().len())
                .expect("foundational partition-key hard bound fits u32")
                .to_be_bytes(),
        );
        output.extend_from_slice(partition_key.as_bytes());
        output
    }

    /// Returns the exact checked semantic charge for evidence-page accounting.
    pub fn semantic_bytes(&self) -> Result<usize, StorageValueError> {
        31usize
            .checked_add(self.scoped_partition.lineage().as_bytes().len())
            .and_then(|value| {
                value.checked_add(self.scoped_partition.partition_key().as_bytes().len())
            })
            .ok_or(StorageValueError::SizeOverflow)
    }
}

impl fmt::Debug for HistoricalCapabilityPartitionEvidenceV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HistoricalCapabilityPartitionEvidenceV1")
            .field("capability_id", &self.capability_id)
            .field("entry_ordinal", &self.entry_ordinal)
            .field("scoped_partition", &"[REDACTED]")
            .finish()
    }
}

/// One ordered, storage-structural historical fact consumed by the catalog.
#[derive(Debug, Eq, PartialEq)]
pub enum HistoricalSemanticEvidence {
    /// One immutable stored bundle and its structural identity.
    Bundle(HistoricalBundleEvidence),
    /// One exact plan reference found in authoritative durable state.
    PlanReference(ExecutablePlanRef),
    /// The singleton active relation, absent only in the accepted initialization state.
    ActiveCatalog(Option<HistoricalActiveCatalogEvidence>),
    /// One persisted key requiring exact catalog-owned historical schema validation.
    PersistedKey(HistoricalPersistedKeyEvidenceV1),
    /// One physical V1 or V2 index row inseparably checked by the durable codec.
    IndexMigrationRow(IndexMigrationRowEvidence),
    /// One qualifying capability's explicit partition entry.
    CapabilityPartition(HistoricalCapabilityPartitionEvidenceV1),
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
            Self::IndexMigrationRow(evidence) => {
                key.push(0x04);
                key.extend_from_slice(&evidence.canonical_order_key());
            }
            Self::CapabilityPartition(evidence) => return evidence.evidence_order_key(),
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
            Self::IndexMigrationRow(evidence) => Ok(evidence.evidence_page_charge()),
            Self::CapabilityPartition(evidence) => evidence.semantic_bytes(),
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
#[derive(Debug, Eq, PartialEq)]
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
        let mut migration_rows = 0usize;
        let mut migration_evidence_bytes = 0usize;
        let mut migration_instruction_bytes = 0usize;
        let mut prior_key: Option<Vec<u8>> = None;
        for item in &evidence {
            total = total
                .checked_add(item.semantic_bytes()?)
                .ok_or(StorageValueError::SizeOverflow)?;
            if total > MAX_HISTORICAL_EVIDENCE_PAGE_BYTES {
                return Err(StorageValueError::LimitExceeded);
            }
            if let HistoricalSemanticEvidence::IndexMigrationRow(row) = item {
                migration_rows = migration_rows
                    .checked_add(1)
                    .ok_or(StorageValueError::SizeOverflow)?;
                migration_evidence_bytes = migration_evidence_bytes
                    .checked_add(row.evidence_page_charge())
                    .ok_or(StorageValueError::SizeOverflow)?;
                migration_instruction_bytes = migration_instruction_bytes
                    .checked_add(row.instruction_page_charge())
                    .ok_or(StorageValueError::SizeOverflow)?;
                if migration_rows > MAX_INDEX_MIGRATION_PAGE_ENTRIES
                    || migration_evidence_bytes > MAX_INDEX_MIGRATION_PAGE_BYTES
                    || migration_instruction_bytes > MAX_INDEX_MIGRATION_PAGE_BYTES
                {
                    return Err(StorageValueError::LimitExceeded);
                }
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
    /// Backend-owned exclusive migration capability returned only after V1 was observed.
    type MigrationPort: StartupIndexMigrationPort;

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

    /// Reads one exact authoritative entity inside the immutable startup view.
    ///
    /// Catalog validation uses this only to prove reciprocal declared unique
    /// indexes. The read is cursor-neutral and grants no mutation authority.
    fn read_integrity_entity(
        &mut self,
        target: &crate::EntityTarget,
    ) -> Result<Option<crate::StoredEntityRecordV1>, StorageError>;

    /// Reads one exact declared-unique prefix inside the immutable startup view.
    ///
    /// Multiple physical rows are corruption and must return an error rather
    /// than an occupancy class.
    fn read_integrity_unique_occupancy(
        &mut self,
        target: &crate::UniqueIndexTarget,
    ) -> Result<crate::UniqueOccupancyKind, StorageError>;

    /// Consumes a completely scanned session into exactly one startup outcome.
    fn finish(
        self,
        structural_end: Self::StructuralEnd,
        historical_end: Self::HistoricalEnd,
    ) -> Result<StructuralOpenOutcome<Self::DormantPorts, Self::MigrationPort>, StorageError>;
}

/// Backend-owned linear startup migration capability.
///
/// This trait deliberately exposes identity only. The catalog-owned migration
/// driver supplies separately branded requests to a concrete backend; merely
/// holding this port grants no scan, point-read, mutation, or completion call.
pub trait StartupIndexMigrationPort: Sized {
    /// Returns the durable database bound to the pre-migration startup session.
    fn database_id(&self) -> DatabaseId;

    /// Returns the exact process-local startup session that observed V1.
    fn open_session_id(&self) -> OpenSessionId;
}

/// Exact result of consuming both startup exact-end authorities.
pub enum StructuralOpenOutcome<P, M> {
    /// The complete checked physical range was V2-only.
    Clean(StructurallyOpened<P>),
    /// At least one V1 row was observed; no readiness-bearing ports are released.
    MigrationRequired(M),
}

impl<P, M> fmt::Debug for StructuralOpenOutcome<P, M> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Clean(_) => formatter.write_str("StructuralOpenOutcome::Clean([CHECKED])"),
            Self::MigrationRequired(_) => {
                formatter.write_str("StructuralOpenOutcome::MigrationRequired([LINEAR])")
            }
        }
    }
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
    retained_metadata: RetainedMetadataV1,
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
    /// `retained_metadata` must be the complete value decoded from the same
    /// immutable startup snapshot. The authority type must have no public
    /// constructor in a concrete backend.
    #[must_use]
    pub fn from_finished_session(
        database_id: DatabaseId,
        open_session_id: OpenSessionId,
        retained_metadata: RetainedMetadataV1,
        dormant_ports: P,
        _authority: P::CompletionAuthority,
    ) -> Self {
        Self {
            database_id,
            open_session_id,
            retained_metadata,
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

    /// Borrows the complete retained metadata decoded by this startup session.
    #[must_use]
    pub const fn retained_metadata(&self) -> &RetainedMetadataV1 {
        &self.retained_metadata
    }

    /// Consumes the structural handoff into its IDs, metadata, and dormant ports.
    #[must_use]
    pub fn into_parts(self) -> (DatabaseId, OpenSessionId, RetainedMetadataV1, P) {
        (
            self.database_id,
            self.open_session_id,
            self.retained_metadata,
            self.dormant_ports,
        )
    }
}

impl<P> fmt::Debug for StructurallyOpened<P> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StructurallyOpened")
            .field("database_id", &self.database_id)
            .field("open_session_id", &self.open_session_id)
            .field("retained_metadata", &self.retained_metadata)
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

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU16, NonZeroU32};

    use riffdb_types::{
        ActorId, ActorKind, AdministrationSequence, AggregateTypeId, Audience, CanonicalRecord,
        CapabilityTokenDigest, ContractBundleHash, ContractVersion, EntityKeyBuilder, EntityTypeId,
        Environment, IndexEntryKeyBuilder, IndexId, PartitionKeyBuilder, RequestId, TenantScope,
    };

    use super::*;
    use crate::{
        CapabilityGrantV1, CapabilityPermissionKindV1, CapabilityPermissionV1,
        CapabilityPermissionsV1, CapabilityRequestedRecordV1, PartitionScopeV1,
        StoredCapabilityRecordV1,
    };

    const GOLDEN_CAPABILITY_ID_BYTES: [u8; 16] = [
        0x01, 0x8f, 0x00, 0x00, 0x00, 0x00, 0x70, 0x01, 0x80, 0x02, 0x11, 0x22, 0x33, 0x44, 0x55,
        0x66,
    ];

    fn uuid_v7(mut bytes: [u8; 16], seed: u8) -> [u8; 16] {
        bytes[15] = seed;
        bytes
    }

    fn partition(lineage: &ContractLineage, value: u64) -> ScopedPartitionV1 {
        let mut builder =
            PartitionKeyBuilder::new(AggregateTypeId::new(0x0102_0304).expect("aggregate type ID"));
        builder.push_u64(value).expect("partition component");
        ScopedPartitionV1::new(lineage.clone(), builder.finish().expect("partition key"))
    }

    fn stored_capability(
        capability_id: CapabilityId,
        partition_scope: PartitionScopeV1,
    ) -> StoredCapabilityRecordV1 {
        let permissions = CapabilityPermissionsV1::new(vec![
            CapabilityPermissionV1::unparameterized(
                CapabilityPermissionKindV1::AdministerCapabilities,
            )
            .expect("permission"),
        ])
        .expect("permissions");
        let grant = CapabilityGrantV1::new(
            TenantScope::Global,
            partition_scope,
            permissions,
            Vec::new(),
            NonZeroU16::MIN,
            Vec::new(),
        )
        .expect("grant");
        let requested = CapabilityRequestedRecordV1::new(
            DatabaseId::from_bytes(uuid_v7(GOLDEN_CAPABILITY_ID_BYTES, 0x71)).expect("database ID"),
            Environment::new("test").expect("environment"),
            ActorId::new("subject").expect("actor ID"),
            ActorKind::Human,
            NonZeroU32::new(60).expect("duration"),
            vec![Audience::new("riffdb-test").expect("audience")],
            grant,
        )
        .expect("requested capability");
        StoredCapabilityRecordV1::active(
            capability_id,
            CapabilityTokenDigest::from_hmac_bytes(
                DigestKeyId::new(1).expect("digest key ID"),
                [0x5a; 32],
            ),
            requested,
            Timestamp::new(10, 0).expect("issued at"),
            Timestamp::new(70, 0).expect("expires at"),
            AdministrationSequence::first(),
            RequestId::from_bytes(uuid_v7(GOLDEN_CAPABILITY_ID_BYTES, 0x72)).expect("request ID"),
        )
        .expect("stored capability")
    }

    fn synthetic_migration_row(value: u64) -> IndexMigrationRowEvidence {
        let mut entity = EntityKeyBuilder::new(EntityTypeId::first());
        entity.push_u64(value).expect("entity component");
        let mut key = IndexEntryKeyBuilder::new(IndexId::first());
        key.push_u64(value).expect("index component");
        let key = key
            .finish(entity.finish().expect("entity key"))
            .expect("index key");
        let row = StoredIndexEntryV1::new(
            key.clone(),
            DurableKeySchemaBindingV1::new(
                ContractLineage::new("migration-boundary").expect("lineage"),
                ContractVersion::new(1).expect("version"),
                ContractBundleHash::from_bytes([0x61; 32]),
            ),
            CanonicalRecord::new(Vec::new()).expect("covered values"),
        )
        .expect("synthetic semantic row");
        IndexMigrationRowEvidence::from_codec_checked_parts(
            key,
            IndexMigrationSemanticRow::V1(row),
            vec![0x01],
            EncodedContentCharge::new(1).expect("replacement charge"),
        )
        .expect("bounded synthetic WP-060 charge")
    }

    #[test]
    fn migration_evidence_and_cursor_retain_neutral_bounds_and_identity() {
        let evidence = synthetic_migration_row(7);
        assert!(evidence.evidence_page_charge() <= MAX_INDEX_MIGRATION_PAGE_BYTES);
        assert!(evidence.instruction_page_charge() <= MAX_INDEX_MIGRATION_PAGE_BYTES);
        assert!(evidence.evidence_page_charge() > 0);
        assert!(evidence.instruction_page_charge() > evidence.evidence_page_charge());

        let database_id =
            DatabaseId::from_bytes(uuid_v7(GOLDEN_CAPABILITY_ID_BYTES, 0x31)).expect("database ID");
        let session = OpenSessionId::new(2).expect("session");
        let start = IndexMigrationCursor::start(database_id, session);
        let next = start.advanced(1).expect("one-row continuation");
        assert_eq!(next.database_id(), database_id);
        assert_eq!(next.open_session_id(), session);
        assert_eq!(next.position(), 1);
        assert_eq!(start.advanced(0), Err(StorageValueError::InvalidShape));
    }

    #[test]
    fn capability_partition_evidence_freezes_order_charge_and_redaction() {
        let capability_id =
            CapabilityId::from_bytes(GOLDEN_CAPABILITY_ID_BYTES).expect("capability ID");
        let lineage = ContractLineage::new("budget").expect("lineage");
        let scope = PartitionScopeV1::explicit(vec![
            partition(&lineage, 1),
            partition(&lineage, 2),
            partition(&lineage, 0x0102_0304_0506_0708),
        ])
        .expect("explicit scope");
        let capability = stored_capability(capability_id, scope);
        let evidence =
            HistoricalCapabilityPartitionEvidenceV1::from_capability_entry(&capability, 2)
                .expect("evidence");

        let expected = vec![
            0x05, 0x01, 0x8f, 0x00, 0x00, 0x00, 0x00, 0x70, 0x01, 0x80, 0x02, 0x11, 0x22, 0x33,
            0x44, 0x55, 0x66, 0x00, 0x02, 0x00, 0x00, 0x00, 0x06, b'b', b'u', b'd', b'g', b'e',
            b't', 0x01, 0x02, 0x03, 0x04, 0x00, 0x00, 0x00, 0x0e, 0x50, 0x01, 0x01, 0x02, 0x03,
            0x04, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
        ];
        assert_eq!(evidence.capability_id(), capability_id);
        assert_eq!(evidence.entry_ordinal(), 2);
        assert_eq!(evidence.evidence_order_key(), expected);
        assert_eq!(evidence.semantic_bytes(), Ok(51));

        let debug = format!("{evidence:?}");
        assert!(debug.contains("[REDACTED]"));
        assert!(!debug.contains("budget"));
        assert!(!debug.contains("0102030405060708"));
    }

    #[test]
    fn capability_partition_evidence_derives_only_checked_explicit_ordinals() {
        let capability_id =
            CapabilityId::from_bytes(GOLDEN_CAPABILITY_ID_BYTES).expect("capability ID");
        let lineage = ContractLineage::new("ordinal-boundary").expect("lineage");
        let explicit = PartitionScopeV1::explicit(
            (0..MAX_CAPABILITY_PARTITIONS)
                .map(|value| partition(&lineage, u64::try_from(value).expect("bounded ordinal")))
                .collect(),
        )
        .expect("maximum explicit scope");
        let capability = stored_capability(capability_id, explicit);
        assert_eq!(
            HistoricalCapabilityPartitionEvidenceV1::from_capability_entry(&capability, 1023)
                .expect("maximum ordinal")
                .entry_ordinal(),
            1023
        );
        assert_eq!(
            HistoricalCapabilityPartitionEvidenceV1::from_capability_entry(&capability, 1024),
            Err(StorageValueError::LimitExceeded)
        );

        let all = stored_capability(capability_id, PartitionScopeV1::All);
        assert_eq!(
            HistoricalCapabilityPartitionEvidenceV1::from_capability_entry(&all, 0),
            Err(StorageValueError::InvalidShape)
        );
    }
}
