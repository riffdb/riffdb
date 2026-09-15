//! Bounded bootstrap transcript values. These prove transfer integrity, never
//! source authorization, durable hold registration, installation or readiness.

use super::{ChangelogHistoryStateV3, ChangelogV3Error as Error, ReplicationSourceHoldIdV1};
use crate::{AuthoritativeNamespaceV1 as N, AuthoritativeStateRowV3, ReplicationAuthorityClassV1};
use sha2::{Digest, Sha256};

#[path = "bootstrap_codec.rs"]
mod codec;
#[path = "bootstrap_cursor.rs"]
mod cursor;
#[path = "bootstrap_progress.rs"]
mod progress;
pub use cursor::ReplicationBootstrapPageCursorV3;
pub use progress::{MAX_REPLICATION_BOOTSTRAP_PROGRESS_BYTES, ReplicationBootstrapProgressV1};

/// Accommodates the largest accepted complete authoritative row plus framing.
pub const MAX_REPLICATION_BOOTSTRAP_PAGE_BYTES: usize = crate::MAX_CHANGELOG_FRAME_BYTES + 512;
/// Independent row ceiling per page, including very small rows.
pub const MAX_REPLICATION_BOOTSTRAP_PAGE_ROWS: usize = 256;
/// A stage has a finite page inventory, never an unbounded filesystem walk.
pub const MAX_REPLICATION_BOOTSTRAP_PAGES: u32 = 1_048_576;
/// Hard total source/receiver stage ceiling (one TiB).
pub const MAX_REPLICATION_BOOTSTRAP_BYTES: u64 = 1 << 40;
const PAGE_FIXED_BYTES: usize = 8 + 2 + 32 + 4 + 2 + 1 + 2 + 4 + 32 + 32;

/// Exact published snapshot plus its nonzero source hold identity. Constructing
/// this value does not prove the hold has been durably installed.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ReplicationBootstrapFenceV3 {
    id: ReplicationSourceHoldIdV1,
    history: ChangelogHistoryStateV3,
}
impl ReplicationBootstrapFenceV3 {
    /// Groups already checked identity/history values; no storage capability.
    #[must_use]
    pub const fn new(id: ReplicationSourceHoldIdV1, history: ChangelogHistoryStateV3) -> Self {
        Self { id, history }
    }
    /// Exact source-local retention hold identity.
    #[must_use]
    pub const fn hold_id(self) -> ReplicationSourceHoldIdV1 {
        self.id
    }
    /// Exact lineage-shared metadata to copy at bootstrap publication.
    #[must_use]
    pub const fn history(self) -> ChangelogHistoryStateV3 {
        self.history
    }
    /// Domain-separated binding used by every page and the final manifest.
    #[must_use]
    pub fn digest(self) -> [u8; 32] {
        let mut hash = Sha256::new();
        hash.update(b"riffdb.replication-bootstrap-fence/v1\0");
        hash.update(self.id.as_bytes());
        let lineage = self.history.lineage();
        hash.update(lineage.database_id().as_bytes());
        hash.update(lineage.history_incarnation().to_be_bytes());
        hash.update(lineage.leadership_epoch().get().to_be_bytes());
        hash.update(lineage.catalog_digest());
        for point in [
            self.history.anchor(),
            self.history.minimum_resume(),
            self.history.tail(),
        ] {
            hash.update(point.sequence().get().to_be_bytes());
            hash.update(point.history_hash());
            hash.update(point.frontier().to_canonical_bytes());
        }
        hash.finalize().into()
    }
}

/// One namespace's bounded, strictly ordered rows and optional exact end.
/// Empty pages are allowed only for a namespace's exact end marker.
#[derive(Clone, Eq, PartialEq)]
pub struct ReplicationBootstrapPageV3 {
    fence_digest: [u8; 32],
    ordinal: u32,
    namespace: N,
    end_namespace: bool,
    prior_hash: [u8; 32],
    rows: Vec<AuthoritativeStateRowV3>,
}
impl ReplicationBootstrapPageV3 {
    /// Checks local shape before encoding. The transcript additionally checks
    /// the cross-page position, prior hash, key order and complete inventory.
    pub fn new(
        fence_digest: [u8; 32],
        ordinal: u32,
        namespace: N,
        end_namespace: bool,
        prior_hash: [u8; 32],
        rows: Vec<AuthoritativeStateRowV3>,
    ) -> Result<Self, Error> {
        if ordinal == 0
            || ordinal > MAX_REPLICATION_BOOTSTRAP_PAGES
            || rows.len() > MAX_REPLICATION_BOOTSTRAP_PAGE_ROWS
        {
            return Err(Error::LimitExceeded);
        }
        if namespace.class() != ReplicationAuthorityClassV1::ReplicatedAuthoritative
            || rows.iter().any(|row| row.namespace() != namespace)
        {
            return Err(Error::InvalidNamespace);
        }
        if (rows.is_empty() && !end_namespace)
            || rows.windows(2).any(|rows| rows[0].key() >= rows[1].key())
        {
            return Err(Error::InvalidEncoding);
        }
        let value = Self {
            fence_digest,
            ordinal,
            namespace,
            end_namespace,
            prior_hash,
            rows,
        };
        if value.encoded_len()? > MAX_REPLICATION_BOOTSTRAP_PAGE_BYTES {
            return Err(Error::LimitExceeded);
        }
        Ok(value)
    }
    /// One-based page ordinal under this exact fence.
    #[must_use]
    pub const fn ordinal(&self) -> u32 {
        self.ordinal
    }
    /// Closed authoritative namespace; no control or derived rows can enter.
    #[must_use]
    pub const fn namespace(&self) -> N {
        self.namespace
    }
    /// Whether this page carries the namespace's exact end marker.
    #[must_use]
    pub const fn ends_namespace(&self) -> bool {
        self.end_namespace
    }
    /// Complete canonical key/value bytes, never diagnostics.
    #[must_use]
    pub fn rows(&self) -> &[AuthoritativeStateRowV3] {
        &self.rows
    }
    /// Total framed bytes, checked independently of the row count ceiling.
    pub fn encoded_len(&self) -> Result<usize, Error> {
        self.rows.iter().try_fold(PAGE_FIXED_BYTES, |bytes, row| {
            bytes
                .checked_add(8)
                .and_then(|n| n.checked_add(row.key().len()))
                .and_then(|n| n.checked_add(row.value().len()))
                .ok_or(Error::LimitExceeded)
        })
    }
}

/// Checksummed external bootstrap receipt V1 carrying a V3 snapshot manifest.
/// It becomes publishable only after every exact namespace end was observed.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ReplicationBootstrapManifestV1 {
    fence: ReplicationBootstrapFenceV3,
    pages: u32,
    rows: u64,
    bytes: u64,
    final_hash: [u8; 32],
}
impl ReplicationBootstrapManifestV1 {
    /// Accepted external artifact identity; not a new database metadata key.
    pub const IDENTITY: &'static str = "replication_bootstrap_receipt/v1";
    /// The exact fixed snapshot and source hold covered by this manifest.
    #[must_use]
    pub const fn fence(self) -> ReplicationBootstrapFenceV3 {
        self.fence
    }
    /// Exact number of bounded page files.
    #[must_use]
    pub const fn page_count(self) -> u32 {
        self.pages
    }
    /// Exact number of transferred authoritative rows (empty namespaces count zero).
    #[must_use]
    pub const fn row_count(self) -> u64 {
        self.rows
    }
    /// Exact sum of framed page bytes.
    #[must_use]
    pub const fn total_page_bytes(self) -> u64 {
        self.bytes
    }
}

/// Constant-cardinality transfer verifier plus at most one bounded prior key.
/// No source or receiver can omit, duplicate, reorder or substitute a namespace.
/// Failure fuses the transcript; it never grants bootstrap publication authority.
pub struct ReplicationBootstrapTranscriptV3 {
    fence: ReplicationBootstrapFenceV3,
    namespace_index: usize,
    last_key: Option<Box<[u8]>>,
    pages: u32,
    rows: u64,
    bytes: u64,
    hash: [u8; 32],
    failed: bool,
}
impl ReplicationBootstrapTranscriptV3 {
    /// Starts at the first catalog-owned namespace and fence hash.
    #[must_use]
    pub fn new(fence: ReplicationBootstrapFenceV3) -> Self {
        Self {
            fence,
            namespace_index: 0,
            last_key: None,
            pages: 0,
            rows: 0,
            bytes: 0,
            hash: fence.digest(),
            failed: false,
        }
    }
    /// Verifies one complete page before advancing this process-local transcript.
    /// Callers still own local durability and cannot acknowledge from this value.
    pub fn observe(&mut self, page: &ReplicationBootstrapPageV3) -> Result<(), Error> {
        if self.failed {
            return Err(Error::InvalidEncoding);
        }
        self.failed = true;
        let expected = namespace(self.namespace_index).ok_or(Error::InvalidNamespace)?;
        if page.fence_digest != self.fence.digest()
            || page.namespace != expected
            || page.ordinal != self.pages.checked_add(1).ok_or(Error::LimitExceeded)?
            || page.prior_hash != self.hash
            || page
                .rows
                .first()
                .is_some_and(|row| self.last_key.as_deref().is_some_and(|key| row.key() <= key))
        {
            return Err(Error::PredecessorMismatch);
        }
        let encoded = page.encode()?;
        let bytes = self
            .bytes
            .checked_add(encoded.len() as u64)
            .ok_or(Error::LimitExceeded)?;
        let rows = self
            .rows
            .checked_add(page.rows.len() as u64)
            .ok_or(Error::LimitExceeded)?;
        if bytes > MAX_REPLICATION_BOOTSTRAP_BYTES
            || rows
                > u64::from(MAX_REPLICATION_BOOTSTRAP_PAGES)
                    * MAX_REPLICATION_BOOTSTRAP_PAGE_ROWS as u64
        {
            return Err(Error::LimitExceeded);
        }
        self.hash = *encoded.last_chunk::<32>().ok_or(Error::InvalidEncoding)?;
        self.pages = page.ordinal;
        self.rows = rows;
        self.bytes = bytes;
        if page.end_namespace {
            self.namespace_index += 1;
            self.last_key = None;
        } else {
            self.last_key = page.rows.last().map(|row| row.key().into());
        }
        self.failed = false;
        Ok(())
    }
    /// Requires the complete fixed inventory and exact counts/hash/fence.
    pub fn verify_manifest(
        &mut self,
        expected: ReplicationBootstrapManifestV1,
    ) -> Result<(), Error> {
        let observed = self.manifest();
        if observed.as_ref() != Ok(&expected) {
            self.failed = true;
            return Err(observed.err().unwrap_or(Error::PredecessorMismatch));
        }
        Ok(())
    }
    /// Forms the final manifest only after every exact end, never on failure.
    pub fn manifest(&self) -> Result<ReplicationBootstrapManifestV1, Error> {
        if self.failed || namespace(self.namespace_index).is_some() || self.pages == 0 {
            return Err(Error::InvalidEncoding);
        }
        Ok(ReplicationBootstrapManifestV1 {
            fence: self.fence,
            pages: self.pages,
            rows: self.rows,
            bytes: self.bytes,
            final_hash: self.hash,
        })
    }
}

fn namespace(index: usize) -> Option<N> {
    N::ALL
        .into_iter()
        .filter(|n| n.class() == ReplicationAuthorityClassV1::ReplicatedAuthoritative)
        .nth(index)
}
macro_rules! redacted {
    ($($ty:ident),+) => { $(impl std::fmt::Debug for $ty {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str(concat!(stringify!($ty), "([redacted])")) }
    })+ };
}
redacted!(
    ReplicationBootstrapFenceV3,
    ReplicationBootstrapPageV3,
    ReplicationBootstrapManifestV1,
    ReplicationBootstrapTranscriptV3
);
