//! Closed release-level durable-format identity and compatibility decisions.

use std::error::Error;
use std::fmt;
use std::num::NonZeroU32;

use riffdb_proto::envelope::{RecordSchema, STORAGE_FORMAT_VERSION_V1, STORAGE_FORMAT_VERSION_V2};
use riffdb_types::SchemaHash;
use sha2::{Digest, Sha256};

use crate::{BackupManifestVersion, StorageFormatVersion};

/// Current logical writer-journal frame format.
pub const DURABILITY_JOURNAL_FRAME_VERSION: u16 = 2;
/// Current preallocated writer-journal extent format.
pub const DURABILITY_JOURNAL_EXTENT_VERSION: u16 = 3;
/// Byte-frozen create/restore maintenance receipt format.
pub const OFFLINE_MAINTENANCE_RECEIPT_VERSION_V1: u32 = 1;
/// Retire-only maintenance receipt format.
pub const OFFLINE_MAINTENANCE_RECEIPT_VERSION_V2: u32 = 2;
/// Archive-only restore receipt format.
pub const OFFLINE_MAINTENANCE_RECEIPT_VERSION_V3: u32 = 3;
/// Latest offline maintenance receipt format; V1/V2 retain their operation-specific writers.
pub const OFFLINE_MAINTENANCE_RECEIPT_VERSION: u32 = OFFLINE_MAINTENANCE_RECEIPT_VERSION_V3;
/// Current checked contract-migration receipt format.
pub const CONTRACT_MIGRATION_CHECK_RECEIPT_VERSION: u32 = 2;
/// Current checksummed offline durable-format upgrade receipt format.
pub const DURABLE_FORMAT_UPGRADE_RECEIPT_VERSION: u16 = 1;
/// Current compact durable-record registry format.
pub const DURABLE_RECORD_REGISTRY_VERSION: u32 = 2;
/// Current concrete redb table/key layout generation.
pub const REDB_LAYOUT_VERSION: u16 = 1;
/// Exact byte length of one retained format-marker sidecar.
pub const DURABLE_FORMAT_MARKER_BYTES: usize = 8 + 2 + 4 + 4 + 32 + 32 + 32;
/// Current fixed format-marker encoding version.
pub const DURABLE_FORMAT_MARKER_VERSION: u16 = 1;

const DURABLE_FORMAT_MARKER_MAGIC: [u8; 8] = *b"RDBFMT01";

const READABLE_STORAGE_VERSIONS: &[u32] = &[STORAGE_FORMAT_VERSION_V1, STORAGE_FORMAT_VERSION_V2];
const WRITABLE_STORAGE_VERSIONS: &[u32] = &[StorageFormatVersion::V2.get()];
const READABLE_REDB_LAYOUT_VERSIONS: &[u16] = &[REDB_LAYOUT_VERSION];
const WRITABLE_REDB_LAYOUT_VERSIONS: &[u16] = &[REDB_LAYOUT_VERSION];
const READABLE_REGISTRY_VERSIONS: &[u32] = &[DURABLE_RECORD_REGISTRY_VERSION];
const WRITABLE_REGISTRY_VERSIONS: &[u32] = &[DURABLE_RECORD_REGISTRY_VERSION];
const READABLE_JOURNAL_FRAME_VERSIONS: &[u16] = &[DURABILITY_JOURNAL_FRAME_VERSION];
const WRITABLE_JOURNAL_FRAME_VERSIONS: &[u16] = &[DURABILITY_JOURNAL_FRAME_VERSION];
const READABLE_JOURNAL_EXTENT_VERSIONS: &[u16] = &[DURABILITY_JOURNAL_EXTENT_VERSION];
const WRITABLE_JOURNAL_EXTENT_VERSIONS: &[u16] = &[DURABILITY_JOURNAL_EXTENT_VERSION];
const READABLE_BACKUP_VERSIONS: &[u32] = &[BackupManifestVersion::V1.get()];
const WRITABLE_BACKUP_VERSIONS: &[u32] = &[BackupManifestVersion::V1.get()];
const READABLE_RECEIPT_VERSIONS: &[u32] = &[
    OFFLINE_MAINTENANCE_RECEIPT_VERSION_V1,
    OFFLINE_MAINTENANCE_RECEIPT_VERSION_V2,
    OFFLINE_MAINTENANCE_RECEIPT_VERSION_V3,
];
const WRITABLE_RECEIPT_VERSIONS: &[u32] = READABLE_RECEIPT_VERSIONS;
const READABLE_OFFLINE_MAINTENANCE_RECEIPT_VERSIONS: &[u32] = &[
    OFFLINE_MAINTENANCE_RECEIPT_VERSION_V1,
    OFFLINE_MAINTENANCE_RECEIPT_VERSION_V2,
    OFFLINE_MAINTENANCE_RECEIPT_VERSION_V3,
];
const WRITABLE_OFFLINE_MAINTENANCE_RECEIPT_VERSIONS: &[u32] =
    READABLE_OFFLINE_MAINTENANCE_RECEIPT_VERSIONS;
const READABLE_CONTRACT_MIGRATION_CHECK_RECEIPT_VERSIONS: &[u32] =
    &[CONTRACT_MIGRATION_CHECK_RECEIPT_VERSION];
const WRITABLE_CONTRACT_MIGRATION_CHECK_RECEIPT_VERSIONS: &[u32] =
    READABLE_CONTRACT_MIGRATION_CHECK_RECEIPT_VERSIONS;
const READABLE_FORMAT_UPGRADE_RECEIPT_VERSIONS: &[u16] = &[DURABLE_FORMAT_UPGRADE_RECEIPT_VERSION];
const WRITABLE_FORMAT_UPGRADE_RECEIPT_VERSIONS: &[u16] = READABLE_FORMAT_UPGRADE_RECEIPT_VERSIONS;
const READABLE_FORMAT_MARKER_VERSIONS: &[u16] = &[DURABLE_FORMAT_MARKER_VERSION];
const WRITABLE_FORMAT_MARKER_VERSIONS: &[u16] = &[DURABLE_FORMAT_MARKER_VERSION];

const COMPATIBILITY_FIXTURE_INVENTORY: &[u8] = include_bytes!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/fixtures/durable-fixture-inventory-v1.txt"
));

/// A nonzero pre-1.0 physical-format epoch.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct AlphaFormatEpoch(NonZeroU32);

impl AlphaFormatEpoch {
    /// Reconstructs a nonzero alpha epoch.
    #[must_use]
    pub const fn new(value: u32) -> Option<Self> {
        match NonZeroU32::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the numeric epoch.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

/// The monotonically assigned writer identity within one format epoch.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DurableFormatWriter(u32);

impl DurableFormatWriter {
    /// Reconstructs a writer identity. Zero denotes the explicitly supported
    /// pre-manifest predecessor, never a current alpha writer.
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Returns the numeric writer identity.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// The bounded immutable identity read before a database may be changed.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DurableFormatIdentity {
    epoch: AlphaFormatEpoch,
    writer: DurableFormatWriter,
}

impl DurableFormatIdentity {
    /// Constructs an exact epoch/writer pair.
    #[must_use]
    pub const fn new(epoch: AlphaFormatEpoch, writer: DurableFormatWriter) -> Self {
        Self { epoch, writer }
    }

    /// Returns the alpha epoch.
    #[must_use]
    pub const fn epoch(self) -> AlphaFormatEpoch {
        self.epoch
    }

    /// Returns the writer within that epoch.
    #[must_use]
    pub const fn writer(self) -> DurableFormatWriter {
        self.writer
    }
}

/// SHA-256 digest of the complete compatibility fixture inventory.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CompatibilityFixtureDigest([u8; 32]);

impl CompatibilityFixtureDigest {
    /// Reconstructs an exact fixture digest.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Borrows the digest bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Immutable pre-open marker paired with one physical database.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurableFormatMarker {
    identity: DurableFormatIdentity,
    registry_digest: SchemaHash,
    compatibility_fixture_digest: CompatibilityFixtureDigest,
}

impl DurableFormatMarker {
    /// Constructs an exact marker from one published manifest.
    #[must_use]
    pub const fn new(
        identity: DurableFormatIdentity,
        registry_digest: SchemaHash,
        compatibility_fixture_digest: CompatibilityFixtureDigest,
    ) -> Self {
        Self {
            identity,
            registry_digest,
            compatibility_fixture_digest,
        }
    }

    /// Returns its release format identity.
    #[must_use]
    pub const fn identity(self) -> DurableFormatIdentity {
        self.identity
    }

    /// Returns the exact record-registry digest written by that release.
    #[must_use]
    pub const fn registry_digest(self) -> SchemaHash {
        self.registry_digest
    }

    /// Returns the compatibility corpus digest carried by that release.
    #[must_use]
    pub const fn compatibility_fixture_digest(self) -> CompatibilityFixtureDigest {
        self.compatibility_fixture_digest
    }
}

/// Closed failures for the fixed, checksummed format-marker encoding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurableFormatMarkerError {
    /// The marker length, magic, or numeric identity is malformed.
    Malformed,
    /// The marker encoding version is unknown.
    UnknownVersion,
    /// The marker checksum does not cover its exact prefix.
    ChecksumMismatch,
    /// A current identity carries a registry or fixture digest from another build.
    ManifestMismatch,
}

impl fmt::Display for DurableFormatMarkerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Malformed => "durable format marker is malformed",
            Self::UnknownVersion => "durable format marker version is unsupported",
            Self::ChecksumMismatch => "durable format marker checksum does not match",
            Self::ManifestMismatch => "durable format marker does not match its release manifest",
        })
    }
}

impl Error for DurableFormatMarkerError {}

/// Encodes one marker in its fixed canonical v1 representation.
#[must_use]
pub fn encode_durable_format_marker(
    marker: DurableFormatMarker,
) -> [u8; DURABLE_FORMAT_MARKER_BYTES] {
    let mut encoded = [0_u8; DURABLE_FORMAT_MARKER_BYTES];
    encoded[..8].copy_from_slice(&DURABLE_FORMAT_MARKER_MAGIC);
    encoded[8..10].copy_from_slice(&DURABLE_FORMAT_MARKER_VERSION.to_be_bytes());
    encoded[10..14].copy_from_slice(&marker.identity().epoch().get().to_be_bytes());
    encoded[14..18].copy_from_slice(&marker.identity().writer().get().to_be_bytes());
    encoded[18..50].copy_from_slice(marker.registry_digest().as_bytes());
    encoded[50..82].copy_from_slice(marker.compatibility_fixture_digest().as_bytes());
    let checksum: [u8; 32] = Sha256::digest(&encoded[..82]).into();
    encoded[82..].copy_from_slice(&checksum);
    encoded
}

/// Decodes and checks one exact marker without opening storage.
pub fn decode_durable_format_marker(
    encoded: &[u8],
) -> Result<DurableFormatMarker, DurableFormatMarkerError> {
    if encoded.len() != DURABLE_FORMAT_MARKER_BYTES || encoded[..8] != DURABLE_FORMAT_MARKER_MAGIC {
        return Err(DurableFormatMarkerError::Malformed);
    }
    if u16::from_be_bytes([encoded[8], encoded[9]]) != DURABLE_FORMAT_MARKER_VERSION {
        return Err(DurableFormatMarkerError::UnknownVersion);
    }
    let checksum: [u8; 32] = Sha256::digest(&encoded[..82]).into();
    if encoded[82..] != checksum {
        return Err(DurableFormatMarkerError::ChecksumMismatch);
    }
    let epoch = u32::from_be_bytes(
        encoded[10..14]
            .try_into()
            .map_err(|_| DurableFormatMarkerError::Malformed)?,
    );
    let Some(epoch) = AlphaFormatEpoch::new(epoch) else {
        return Err(DurableFormatMarkerError::Malformed);
    };
    let writer = u32::from_be_bytes(
        encoded[14..18]
            .try_into()
            .map_err(|_| DurableFormatMarkerError::Malformed)?,
    );
    let registry_digest = SchemaHash::from_bytes(
        encoded[18..50]
            .try_into()
            .map_err(|_| DurableFormatMarkerError::Malformed)?,
    );
    let compatibility_fixture_digest = CompatibilityFixtureDigest::from_bytes(
        encoded[50..82]
            .try_into()
            .map_err(|_| DurableFormatMarkerError::Malformed)?,
    );
    Ok(DurableFormatMarker::new(
        DurableFormatIdentity::new(epoch, DurableFormatWriter(writer)),
        registry_digest,
        compatibility_fixture_digest,
    ))
}

/// The only operator actions a format comparison may prescribe.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum SafeFormatCommand {
    /// Run the explicit offline upgrade command.
    Upgrade,
    /// Export with the source release and reimport into a new epoch.
    ExportReimport,
    /// Use the binary named by the database's retained manifest.
    UseMatchingBinary,
}

impl SafeFormatCommand {
    /// Returns stable public-safe operator guidance.
    #[must_use]
    pub const fn render(self) -> &'static str {
        match self {
            Self::Upgrade => "riffdb storage upgrade",
            Self::ExportReimport => "riffdb application export with the source release",
            Self::UseMatchingBinary => {
                "run the RiffDB binary matching the database format manifest"
            }
        }
    }
}

/// A positively selected compatible action. There is deliberately no force,
/// ignore, reset, best-effort, or caller-chosen decoder arm.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurableFormatAction {
    /// The database already matches this binary exactly.
    OpenCurrent,
    /// One declared same-epoch offline transition is required.
    OfflineInPlace {
        /// A verified backup is mandatory before mutation.
        backup_required: bool,
        /// Conservative required free space expressed as source-size multiples.
        free_space_source_multiples: u8,
        /// The database is unavailable during the transition.
        downtime_required: bool,
        /// The transition does not support downgrade.
        one_way: bool,
        /// Exact next operator command.
        next_command: SafeFormatCommand,
    },
    /// A breaking epoch is portable only through the application surface.
    ExportReimportOnly {
        /// A verified physical backup is mandatory before export.
        backup_required: bool,
        /// The transition requires downtime.
        downtime_required: bool,
        /// Exact next operator command.
        next_command: SafeFormatCommand,
    },
}

/// One supported source-to-target release edge.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurableFormatReleaseEdge {
    source_release: &'static str,
    target_release: &'static str,
    source: DurableFormatIdentity,
    target: DurableFormatIdentity,
    action: DurableFormatAction,
}

impl DurableFormatReleaseEdge {
    /// Returns the source binary release label retained for this edge.
    #[must_use]
    pub const fn source_release(self) -> &'static str {
        self.source_release
    }

    /// Returns the target binary release label that owns this edge.
    #[must_use]
    pub const fn target_release(self) -> &'static str {
        self.target_release
    }

    /// Returns the source identity.
    #[must_use]
    pub const fn source(self) -> DurableFormatIdentity {
        self.source
    }

    /// Returns the target identity.
    #[must_use]
    pub const fn target(self) -> DurableFormatIdentity {
        self.target
    }

    /// Returns the only supported action for this edge.
    #[must_use]
    pub const fn action(self) -> DurableFormatAction {
        self.action
    }
}

const CURRENT_EPOCH: AlphaFormatEpoch = AlphaFormatEpoch(NonZeroU32::MIN);
const CURRENT_WRITER: DurableFormatWriter = DurableFormatWriter(1);
const PRE_MANIFEST_RELEASE: &str = "0.1.0-pre-format-manifest";
const CURRENT_RELEASE: &str = env!("CARGO_PKG_VERSION");
const CURRENT_IDENTITY: DurableFormatIdentity =
    DurableFormatIdentity::new(CURRENT_EPOCH, CURRENT_WRITER);
const PRE_MANIFEST_IDENTITY: DurableFormatIdentity =
    DurableFormatIdentity::new(CURRENT_EPOCH, DurableFormatWriter(0));
const PRE_MANIFEST_EDGE: DurableFormatReleaseEdge = DurableFormatReleaseEdge {
    source_release: PRE_MANIFEST_RELEASE,
    target_release: CURRENT_RELEASE,
    source: PRE_MANIFEST_IDENTITY,
    target: CURRENT_IDENTITY,
    action: DurableFormatAction::OfflineInPlace {
        backup_required: true,
        free_space_source_multiples: 2,
        downtime_required: true,
        one_way: true,
        next_command: SafeFormatCommand::Upgrade,
    },
};
const RELEASE_EDGES: &[DurableFormatReleaseEdge] = &[PRE_MANIFEST_EDGE];

/// Exact generated durable-format statement carried by one release artifact.
#[derive(Clone, Copy, Debug)]
pub struct DurableFormatManifest {
    identity: DurableFormatIdentity,
    release: &'static str,
    registry_digest: SchemaHash,
    compatibility_fixture_digest: CompatibilityFixtureDigest,
}

impl DurableFormatManifest {
    /// Returns the exact epoch/writer identity.
    #[must_use]
    pub const fn identity(self) -> DurableFormatIdentity {
        self.identity
    }

    /// Returns the alpha epoch.
    #[must_use]
    pub const fn epoch(self) -> AlphaFormatEpoch {
        self.identity.epoch()
    }

    /// Returns the writer identity.
    #[must_use]
    pub const fn writer(self) -> DurableFormatWriter {
        self.identity.writer()
    }

    /// Returns the release version that owns the manifest.
    #[must_use]
    pub const fn release(self) -> &'static str {
        self.release
    }

    /// Returns readable semantic storage versions.
    #[must_use]
    pub const fn readable_storage_versions(self) -> &'static [u32] {
        READABLE_STORAGE_VERSIONS
    }

    /// Returns writable semantic storage versions.
    #[must_use]
    pub const fn writable_storage_versions(self) -> &'static [u32] {
        WRITABLE_STORAGE_VERSIONS
    }

    /// Returns readable concrete redb layout generations.
    #[must_use]
    pub const fn readable_redb_layout_versions(self) -> &'static [u16] {
        READABLE_REDB_LAYOUT_VERSIONS
    }

    /// Returns writable concrete redb layout generations.
    #[must_use]
    pub const fn writable_redb_layout_versions(self) -> &'static [u16] {
        WRITABLE_REDB_LAYOUT_VERSIONS
    }

    /// Returns readable registry versions.
    #[must_use]
    pub const fn readable_registry_versions(self) -> &'static [u32] {
        READABLE_REGISTRY_VERSIONS
    }

    /// Returns writable registry versions.
    #[must_use]
    pub const fn writable_registry_versions(self) -> &'static [u32] {
        WRITABLE_REGISTRY_VERSIONS
    }

    /// Returns readable journal-frame versions.
    #[must_use]
    pub const fn readable_journal_frame_versions(self) -> &'static [u16] {
        READABLE_JOURNAL_FRAME_VERSIONS
    }

    /// Returns writable journal-frame versions.
    #[must_use]
    pub const fn writable_journal_frame_versions(self) -> &'static [u16] {
        WRITABLE_JOURNAL_FRAME_VERSIONS
    }

    /// Returns readable journal-extent versions.
    #[must_use]
    pub const fn readable_journal_extent_versions(self) -> &'static [u16] {
        READABLE_JOURNAL_EXTENT_VERSIONS
    }

    /// Returns writable journal-extent versions.
    #[must_use]
    pub const fn writable_journal_extent_versions(self) -> &'static [u16] {
        WRITABLE_JOURNAL_EXTENT_VERSIONS
    }

    /// Returns readable backup-manifest versions.
    #[must_use]
    pub const fn readable_backup_versions(self) -> &'static [u32] {
        READABLE_BACKUP_VERSIONS
    }

    /// Returns writable backup-manifest versions.
    #[must_use]
    pub const fn writable_backup_versions(self) -> &'static [u32] {
        WRITABLE_BACKUP_VERSIONS
    }

    /// Returns readable maintenance-receipt versions.
    #[must_use]
    pub const fn readable_receipt_versions(self) -> &'static [u32] {
        READABLE_RECEIPT_VERSIONS
    }

    /// Returns writable maintenance-receipt versions.
    #[must_use]
    pub const fn writable_receipt_versions(self) -> &'static [u32] {
        WRITABLE_RECEIPT_VERSIONS
    }

    /// Returns readable offline-maintenance receipt versions.
    #[must_use]
    pub const fn readable_offline_maintenance_receipt_versions(self) -> &'static [u32] {
        READABLE_OFFLINE_MAINTENANCE_RECEIPT_VERSIONS
    }

    /// Returns writable offline-maintenance receipt versions.
    #[must_use]
    pub const fn writable_offline_maintenance_receipt_versions(self) -> &'static [u32] {
        WRITABLE_OFFLINE_MAINTENANCE_RECEIPT_VERSIONS
    }

    /// Returns readable contract-migration check-receipt versions.
    #[must_use]
    pub const fn readable_contract_migration_check_receipt_versions(self) -> &'static [u32] {
        READABLE_CONTRACT_MIGRATION_CHECK_RECEIPT_VERSIONS
    }

    /// Returns writable contract-migration check-receipt versions.
    #[must_use]
    pub const fn writable_contract_migration_check_receipt_versions(self) -> &'static [u32] {
        WRITABLE_CONTRACT_MIGRATION_CHECK_RECEIPT_VERSIONS
    }

    /// Returns readable offline format-upgrade receipt versions.
    #[must_use]
    pub const fn readable_format_upgrade_receipt_versions(self) -> &'static [u16] {
        READABLE_FORMAT_UPGRADE_RECEIPT_VERSIONS
    }

    /// Returns writable offline format-upgrade receipt versions.
    #[must_use]
    pub const fn writable_format_upgrade_receipt_versions(self) -> &'static [u16] {
        WRITABLE_FORMAT_UPGRADE_RECEIPT_VERSIONS
    }

    /// Returns readable pre-open marker versions.
    #[must_use]
    pub const fn readable_format_marker_versions(self) -> &'static [u16] {
        READABLE_FORMAT_MARKER_VERSIONS
    }

    /// Returns writable pre-open marker versions.
    #[must_use]
    pub const fn writable_format_marker_versions(self) -> &'static [u16] {
        WRITABLE_FORMAT_MARKER_VERSIONS
    }

    /// Returns every accepted durable record schema.
    #[must_use]
    pub const fn readable_records(self) -> &'static [RecordSchema<'static>] {
        &riffdb_proto::durable::READABLE_RECORD_SCHEMAS
    }

    /// Returns every current writable durable record schema.
    #[must_use]
    pub const fn writable_records(self) -> &'static [RecordSchema<'static>] {
        &riffdb_proto::durable::WRITABLE_RECORD_SCHEMAS
    }

    /// Returns the exact current record-registry digest.
    #[must_use]
    pub const fn registry_digest(self) -> SchemaHash {
        self.registry_digest
    }

    /// Returns every supported release transition.
    #[must_use]
    pub const fn release_edges(self) -> &'static [DurableFormatReleaseEdge] {
        RELEASE_EDGES
    }

    /// Returns the oldest retained source-release label accepted by this binary.
    #[must_use]
    pub const fn minimum_supported_source_release(self) -> &'static str {
        PRE_MANIFEST_RELEASE
    }

    /// Returns the newest source-release label accepted by this binary.
    #[must_use]
    pub const fn maximum_supported_source_release(self) -> &'static str {
        CURRENT_RELEASE
    }

    /// Returns the digest of the complete compatibility fixture inventory.
    #[must_use]
    pub const fn compatibility_fixture_digest(self) -> CompatibilityFixtureDigest {
        self.compatibility_fixture_digest
    }

    /// Downgrade is never part of the alpha compatibility promise.
    #[must_use]
    pub const fn downgrade_supported(self) -> bool {
        false
    }
}

/// Returns this binary's exact generated durable-format manifest, including
/// every append-only authoritative side-record schema in the fixture corpus.
#[must_use]
pub fn current_durable_format_manifest() -> DurableFormatManifest {
    let compatibility_fixture_digest = CompatibilityFixtureDigest::from_bytes(
        Sha256::digest(COMPATIBILITY_FIXTURE_INVENTORY).into(),
    );
    DurableFormatManifest {
        identity: CURRENT_IDENTITY,
        release: CURRENT_RELEASE,
        registry_digest: riffdb_proto::durable::record_registry_digest(),
        compatibility_fixture_digest,
    }
}

/// Returns the marker an initialized database written by this binary retains.
#[must_use]
pub fn current_durable_format_marker() -> DurableFormatMarker {
    let manifest = current_durable_format_manifest();
    DurableFormatMarker::new(
        manifest.identity(),
        manifest.registry_digest(),
        manifest.compatibility_fixture_digest(),
    )
}

/// Verifies a retained marker against the release graph and exact current
/// registry/fixture identity before a database is opened for mutation.
pub fn preflight_durable_format_marker(
    marker: DurableFormatMarker,
) -> Result<DurableFormatAction, DurableFormatMarkerError> {
    let action = preflight_durable_format(marker.identity())
        .map_err(|_| DurableFormatMarkerError::ManifestMismatch)?;
    if action == DurableFormatAction::OpenCurrent {
        let manifest = current_durable_format_manifest();
        if marker.registry_digest() != manifest.registry_digest()
            || marker.compatibility_fixture_digest() != manifest.compatibility_fixture_digest()
        {
            return Err(DurableFormatMarkerError::ManifestMismatch);
        }
    }
    Ok(action)
}

/// Typed, redacted incompatibility returned before any database mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurableFormatPreflightError {
    current: DurableFormatIdentity,
    binary: DurableFormatIdentity,
    next_command: SafeFormatCommand,
}

impl DurableFormatPreflightError {
    /// Returns the retained database identity.
    #[must_use]
    pub const fn current(self) -> DurableFormatIdentity {
        self.current
    }

    /// Returns this binary's identity.
    #[must_use]
    pub const fn binary(self) -> DurableFormatIdentity {
        self.binary
    }

    /// Returns the only safe next action.
    #[must_use]
    pub const fn next_command(self) -> SafeFormatCommand {
        self.next_command
    }
}

impl fmt::Display for DurableFormatPreflightError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "database format epoch {} writer {} is not supported by binary epoch {} writer {}; next: {}",
            self.current.epoch().get(),
            self.current.writer().get(),
            self.binary.epoch().get(),
            self.binary.writer().get(),
            self.next_command.render()
        )
    }
}

impl Error for DurableFormatPreflightError {}

/// Selects only a manifest-declared action for one retained identity.
pub fn preflight_durable_format(
    current: DurableFormatIdentity,
) -> Result<DurableFormatAction, DurableFormatPreflightError> {
    let manifest = current_durable_format_manifest();
    let binary = manifest.identity();
    if current == binary {
        return Ok(DurableFormatAction::OpenCurrent);
    }
    if let Some(edge) = manifest
        .release_edges()
        .iter()
        .find(|edge| edge.source == current && edge.target == binary)
    {
        return Ok(edge.action);
    }

    let next_command = if current.epoch() < binary.epoch() {
        SafeFormatCommand::ExportReimport
    } else {
        SafeFormatCommand::UseMatchingBinary
    };
    Err(DurableFormatPreflightError {
        current,
        binary,
        next_command,
    })
}
