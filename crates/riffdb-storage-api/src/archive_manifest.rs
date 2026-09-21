//! Bounded linked records of the additive external archive-manifest/v1.
//!
//! Each record describes one complete frame and preserves the verified full
//! backup fence. Walking successors needs only the prior record and one frame.
//! Decoding is structural validation, never proof of sink durability or restore
//! authorization. A repository must additionally select an exact terminal record
//! and validate the entire chain before replacing any database.
use std::fmt;

use riffdb_types::{DatabaseId, DualFrontier};
use sha2::{Digest, Sha256};

use crate::{
    ArchiveConsumerErrorV1 as Error, ArchiveFrameV1, ChangelogFrameV3, ChangelogHistoryPointV3,
    ChangelogLineageV3, ChangelogTransactionSequence, LeadershipEpochV1, MAX_CHANGELOG_FRAME_BYTES,
    MAX_STAGED_COMMANDS,
};

const MAGIC: &[u8; 8] = b"RDBARM01";
/// Exact bounded size, including the checksum. No optional-length fields exist.
pub const ARCHIVE_MANIFEST_V1_BYTES: usize = 386;

/// Explicit operator policy declaration; neither variant encrypts frame bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ArchiveEncryptionPostureV1 {
    /// The operator explicitly permits unencrypted archive storage.
    Unencrypted,
    /// Encryption is owned by the configured filesystem or remote sink policy.
    /// The codec does not attest that the external policy has been enforced.
    OperatorManaged,
}

/// One immutable external descriptor, not a database record or freshness token.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ArchiveManifestV1 {
    lineage: ChangelogLineageV3,
    full_backup_manifest_digest: [u8; 32],
    encryption_posture: ArchiveEncryptionPostureV1,
    previous_manifest_digest: Option<[u8; 32]>,
    backup_fence: ChangelogHistoryPointV3,
    before: ChangelogHistoryPointV3,
    covered: ChangelogHistoryPointV3,
    frame_bytes: u64,
    frame_digest: [u8; 32],
}
impl ArchiveManifestV1 {
    /// Sole versioned external identity admitted by ADR-0178 §7.
    pub const IDENTITY: &str = "archive-manifest/v1";

    /// Binds the first validated frame to an independently verified full backup.
    /// Callers must verify the backup digest, lineage and fence before invoking.
    pub fn first(
        frame: &ArchiveFrameV1,
        full_backup_manifest_digest: [u8; 32],
        encryption_posture: ArchiveEncryptionPostureV1,
    ) -> Result<Self, Error> {
        let result = Self {
            lineage: frame.lineage(),
            full_backup_manifest_digest,
            encryption_posture,
            previous_manifest_digest: None,
            backup_fence: frame.before(),
            before: frame.before(),
            covered: frame.covered(),
            frame_bytes: frame.as_bytes().len() as u64,
            frame_digest: frame.digest(),
        };
        result.validate()?;
        Ok(result)
    }

    /// Links a complete successor while preserving the original backup binding.
    pub fn next(&self, frame: &ArchiveFrameV1) -> Result<Self, Error> {
        if frame.lineage() != self.lineage || frame.before() != self.covered {
            return Err(Error::InvalidPosition);
        }
        let result = Self {
            previous_manifest_digest: Some(self.digest()),
            before: frame.before(),
            covered: frame.covered(),
            frame_bytes: frame.as_bytes().len() as u64,
            frame_digest: frame.digest(),
            ..*self
        };
        result.validate()?;
        Ok(result)
    }

    /// Checks an exact predecessor, refusing holes, reordering and backup changes.
    pub fn verify_predecessor(&self, previous: Option<&Self>) -> Result<(), Error> {
        let valid = match previous {
            None => self.previous_manifest_digest.is_none() && self.before == self.backup_fence,
            Some(previous) => {
                self.previous_manifest_digest == Some(previous.digest())
                    && self.lineage == previous.lineage
                    && self.full_backup_manifest_digest == previous.full_backup_manifest_digest
                    && self.encryption_posture == previous.encryption_posture
                    && self.backup_fence == previous.backup_fence
                    && self.before == previous.covered
            }
        };
        if valid {
            Ok(())
        } else {
            Err(Error::InvalidPosition)
        }
    }

    /// Checks full stored bytes, canonical V3 framing and exact receipt coverage.
    /// Cross-frame receipt ancestry is bound by the manifest predecessor chain;
    /// transport frame checksums may restart at a negotiated receipt after reconnect.
    pub fn verify_frame(&self, bytes: &[u8]) -> Result<(), Error> {
        if bytes.len() as u64 != self.frame_bytes
            || bytes.len() > MAX_CHANGELOG_FRAME_BYTES
            || <[u8; 32]>::from(Sha256::digest(bytes)) != self.frame_digest
        {
            return Err(Error::InvalidFrame);
        }
        let frame = ChangelogFrameV3::decode(bytes).map_err(|_| Error::InvalidFrame)?;
        let binding = frame.binding();
        if binding.database_id() != self.lineage.database_id()
            || binding.history_incarnation() != self.lineage.history_incarnation()
            || binding.leadership_epoch() != self.lineage.leadership_epoch().get()
            || binding.catalog_digest() != self.lineage.catalog_digest()
        {
            return Err(Error::ForeignLineage);
        }
        let first = frame
            .receipts()
            .first()
            .ok_or(Error::InvalidFrame)?
            .binding();
        let last = frame.receipts().last().ok_or(Error::InvalidFrame)?;
        if first.predecessor != Some(self.before.sequence())
            || Some(first.sequence) != self.before.sequence().checked_next()
            || first.predecessor_frontier != self.before.frontier()
            || first.prior_history_hash != self.before.history_hash()
            || ChangelogHistoryPointV3::from_receipt(last).map_err(|_| Error::InvalidFrame)?
                != self.covered
        {
            return Err(Error::InvalidPosition);
        }
        Ok(())
    }

    /// Canonical fixed-size descriptor, checksummed independently of frame bytes.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(ARCHIVE_MANIFEST_V1_BYTES);
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&1u16.to_be_bytes());
        bytes.extend_from_slice(self.lineage.database_id().as_bytes());
        bytes.extend_from_slice(&self.lineage.history_incarnation().to_be_bytes());
        bytes.extend_from_slice(&self.lineage.leadership_epoch().get().to_be_bytes());
        bytes.extend_from_slice(&self.lineage.catalog_digest());
        bytes.extend_from_slice(&self.full_backup_manifest_digest);
        bytes.push(match self.encryption_posture {
            ArchiveEncryptionPostureV1::Unencrypted => 0,
            ArchiveEncryptionPostureV1::OperatorManaged => 1,
        });
        bytes.push(u8::from(self.previous_manifest_digest.is_some()));
        bytes.extend_from_slice(&self.previous_manifest_digest.unwrap_or([0; 32]));
        for point in [self.backup_fence, self.before, self.covered] {
            bytes.extend_from_slice(&point.sequence().get().to_be_bytes());
            bytes.extend_from_slice(&point.history_hash());
            bytes.extend_from_slice(&point.frontier().to_canonical_bytes());
        }
        bytes.extend_from_slice(&self.frame_bytes.to_be_bytes());
        bytes.extend_from_slice(&self.frame_digest);
        let checksum: [u8; 32] = Sha256::digest(&bytes).into();
        bytes.extend_from_slice(&checksum);
        bytes
    }

    /// Refuses any unknown version, noncanonical field, bound or checksum failure.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() != ARCHIVE_MANIFEST_V1_BYTES {
            return Err(Error::InvalidManifest);
        }
        let end = ARCHIVE_MANIFEST_V1_BYTES - 32;
        if bytes[end..] != <[u8; 32]>::from(Sha256::digest(&bytes[..end])) {
            return Err(Error::InvalidManifest);
        }
        let mut read = Reader(&bytes[..end]);
        if read.array::<8>()? != *MAGIC || read.array::<2>()? != 1u16.to_be_bytes() {
            return Err(Error::InvalidManifest);
        }
        let database = DatabaseId::from_bytes(read.array()?).map_err(|_| Error::InvalidManifest)?;
        let incarnation = read.u64()?;
        let epoch = LeadershipEpochV1::new(read.u64()?).ok_or(Error::InvalidManifest)?;
        let lineage =
            ChangelogLineageV3::new_with_catalog(database, incarnation, epoch, read.array()?)
                .map_err(|_| Error::InvalidManifest)?;
        let full_backup_manifest_digest = read.array()?;
        let encryption_posture = match read.array::<1>()? {
            [0] => ArchiveEncryptionPostureV1::Unencrypted,
            [1] => ArchiveEncryptionPostureV1::OperatorManaged,
            _ => return Err(Error::InvalidManifest),
        };
        let has_previous = read.array::<1>()?;
        let previous = read.array::<32>()?;
        let previous_manifest_digest = match has_previous {
            [0] if previous == [0; 32] => None,
            [1] => Some(previous),
            _ => return Err(Error::InvalidManifest),
        };
        let result = Self {
            lineage,
            full_backup_manifest_digest,
            encryption_posture,
            previous_manifest_digest,
            backup_fence: read.point()?,
            before: read.point()?,
            covered: read.point()?,
            frame_bytes: read.u64()?,
            frame_digest: read.array()?,
        };
        if !read.0.is_empty() {
            return Err(Error::InvalidManifest);
        }
        result.validate()?;
        Ok(result)
    }

    fn validate(&self) -> Result<(), Error> {
        if !self.backup_fence.precedes_or_equals(self.before)
            || !self.before.precedes_or_equals(self.covered)
            || self.before.sequence() >= self.covered.sequence()
            || self.covered.sequence().get() - self.before.sequence().get()
                > MAX_STAGED_COMMANDS as u64
            || (self.previous_manifest_digest.is_none() != (self.before == self.backup_fence))
            || self.frame_bytes == 0
            || self.frame_bytes > MAX_CHANGELOG_FRAME_BYTES as u64
        {
            return Err(Error::InvalidManifest);
        }
        Ok(())
    }

    /// Hash of the complete stored manifest, including its checksum footer.
    #[must_use]
    pub fn digest(&self) -> [u8; 32] {
        Sha256::digest(self.encode()).into()
    }
    /// Exact source database and lineage.
    #[must_use]
    pub const fn lineage(&self) -> ChangelogLineageV3 {
        self.lineage
    }
    /// Full backup manifest checksum; BackupManifestV1 itself is unchanged.
    #[must_use]
    pub const fn full_backup_manifest_digest(&self) -> [u8; 32] {
        self.full_backup_manifest_digest
    }
    /// Explicit operator encryption declaration, not a cryptographic attestation.
    #[must_use]
    pub const fn encryption_posture(&self) -> ArchiveEncryptionPostureV1 {
        self.encryption_posture
    }
    /// Exact preceding manifest digest, absent only for the first frame.
    #[must_use]
    pub const fn previous_manifest_digest(&self) -> Option<[u8; 32]> {
        self.previous_manifest_digest
    }
    /// Original full backup receipt fence, preserved across the entire archive.
    #[must_use]
    pub const fn backup_fence(&self) -> ChangelogHistoryPointV3 {
        self.backup_fence
    }
    /// Exact receipt immediately before this complete frame.
    #[must_use]
    pub const fn before(&self) -> ChangelogHistoryPointV3 {
        self.before
    }
    /// Exact receipt covered by this complete frame.
    #[must_use]
    pub const fn covered(&self) -> ChangelogHistoryPointV3 {
        self.covered
    }
    /// Hard-bounded full file length.
    #[must_use]
    pub const fn frame_bytes(&self) -> u64 {
        self.frame_bytes
    }
    /// Full stored frame checksum, including the V3 footer.
    #[must_use]
    pub const fn frame_digest(&self) -> [u8; 32] {
        self.frame_digest
    }
}
impl fmt::Debug for ArchiveManifestV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ArchiveManifestV1([redacted])")
    }
}
struct Reader<'a>(&'a [u8]);
impl Reader<'_> {
    fn array<const N: usize>(&mut self) -> Result<[u8; N], Error> {
        let (head, rest) = self.0.split_at_checked(N).ok_or(Error::InvalidManifest)?;
        self.0 = rest;
        head.try_into().map_err(|_| Error::InvalidManifest)
    }
    fn u64(&mut self) -> Result<u64, Error> {
        Ok(u64::from_be_bytes(self.array()?))
    }
    fn point(&mut self) -> Result<ChangelogHistoryPointV3, Error> {
        let sequence =
            ChangelogTransactionSequence::new(self.u64()?).ok_or(Error::InvalidManifest)?;
        let hash = self.array()?;
        let frontier = DualFrontier::from_canonical_bytes(self.array()?)
            .map_err(|_| Error::InvalidManifest)?;
        Ok(ChangelogHistoryPointV3::new(sequence, hash, frontier))
    }
}
