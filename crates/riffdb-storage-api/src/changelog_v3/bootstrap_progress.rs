//! Bounded restart checkpoint in the external bootstrap receipt domain.
//! Local durability is the staging owner's responsibility. A checkpoint never
//! replaces the full transcript verification required before installation.
use super::*;

const PROGRESS_MAGIC: &[u8; 8] = b"RDBRBS01";
/// One bounded prior key, an embedded final manifest, and fixed framing.
pub const MAX_REPLICATION_BOOTSTRAP_PROGRESS_BYTES: usize =
    MAX_REPLICATION_BOOTSTRAP_PAGE_BYTES + 1024;

/// Exact observed page boundary, bound to the expected complete transfer.
#[derive(Clone, Eq, PartialEq)]
pub struct ReplicationBootstrapProgressV1 {
    manifest: ReplicationBootstrapManifestV1,
    pages: u32,
    rows: u64,
    bytes: u64,
    namespace_index: usize,
    last_key: Option<Box<[u8]>>,
    hash: [u8; 32],
}
impl std::fmt::Debug for ReplicationBootstrapProgressV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReplicationBootstrapProgressV1([redacted])")
    }
}
impl ReplicationBootstrapProgressV1 {
    /// Number of complete pages covered by this checkpoint.
    #[must_use]
    pub const fn page_count(&self) -> u32 {
        self.pages
    }
    /// Complete expected transfer, not merely its currently received prefix.
    #[must_use]
    pub const fn manifest(&self) -> ReplicationBootstrapManifestV1 {
        self.manifest
    }

    fn validate(&self) -> Result<(), Error> {
        let namespaces = N::ALL
            .into_iter()
            .filter(|n| n.class() == ReplicationAuthorityClassV1::ReplicatedAuthoritative)
            .count();
        if self.pages > self.manifest.pages
            || self.rows > self.manifest.rows
            || self.bytes > self.manifest.bytes
            || self.namespace_index > namespaces
            || u64::from(self.pages) < self.namespace_index as u64
            || self.rows > u64::from(self.pages) * MAX_REPLICATION_BOOTSTRAP_PAGE_ROWS as u64
            || self.bytes < u64::from(self.pages) * PAGE_FIXED_BYTES as u64
            || self.bytes > u64::from(self.pages) * MAX_REPLICATION_BOOTSTRAP_PAGE_BYTES as u64
        {
            return Err(Error::InvalidEncoding);
        }
        if self.pages == 0
            && (self.namespace_index != 0
                || self.last_key.is_some()
                || self.hash != self.manifest.fence.digest())
        {
            return Err(Error::InvalidEncoding);
        }
        if self.namespace_index == namespaces {
            if self.last_key.is_some()
                || self.pages != self.manifest.pages
                || self.rows != self.manifest.rows
                || self.bytes != self.manifest.bytes
                || self.hash != self.manifest.final_hash
            {
                return Err(Error::PredecessorMismatch);
            }
        } else if self.pages == self.manifest.pages {
            return Err(Error::InvalidEncoding);
        }
        if let Some(key) = &self.last_key {
            if self.pages == 0 || self.rows == 0 {
                return Err(Error::InvalidEncoding);
            }
            crate::changelog_v3::AuthoritativeMutationV3::validate(
                namespace(self.namespace_index).ok_or(Error::InvalidNamespace)?,
                key.as_ref(),
                0,
            )?;
        }
        Ok(())
    }

    /// Canonical checksummed V1 progress; this is external staged metadata.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        let manifest = self.manifest.encode()?;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(PROGRESS_MAGIC);
        bytes.extend_from_slice(&1u16.to_be_bytes());
        bytes.extend_from_slice(
            &u16::try_from(manifest.len())
                .map_err(|_| Error::LimitExceeded)?
                .to_be_bytes(),
        );
        bytes.extend_from_slice(&manifest);
        bytes.extend_from_slice(&self.pages.to_be_bytes());
        bytes.extend_from_slice(&self.rows.to_be_bytes());
        bytes.extend_from_slice(&self.bytes.to_be_bytes());
        bytes.extend_from_slice(
            &u16::try_from(self.namespace_index)
                .map_err(|_| Error::LimitExceeded)?
                .to_be_bytes(),
        );
        let key = self.last_key.as_deref().unwrap_or_default();
        bytes.extend_from_slice(
            &u32::try_from(key.len())
                .map_err(|_| Error::LimitExceeded)?
                .to_be_bytes(),
        );
        bytes.extend_from_slice(key);
        bytes.extend_from_slice(&self.hash);
        if bytes.len() + 32 > MAX_REPLICATION_BOOTSTRAP_PROGRESS_BYTES {
            return Err(Error::LimitExceeded);
        }
        Ok(codec::checksum(bytes))
    }
    /// Rejects malformed, oversized, noncanonical or substituted progress.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let mut read = codec::checked(
            bytes,
            PROGRESS_MAGIC,
            MAX_REPLICATION_BOOTSTRAP_PROGRESS_BYTES,
        )?;
        let len = usize::from(read.u16()?);
        let manifest = ReplicationBootstrapManifestV1::decode(read.take(len)?)?;
        let pages = read.u32()?;
        let rows = read.u64()?;
        let bytes = read.u64()?;
        let namespace_index = usize::from(read.u16()?);
        let len = usize::try_from(read.u32()?).map_err(|_| Error::LimitExceeded)?;
        let key = read.take(len)?;
        let hash = read.array()?;
        if !read.remaining.is_empty() {
            return Err(Error::InvalidEncoding);
        }
        let value = Self {
            manifest,
            pages,
            rows,
            bytes,
            namespace_index,
            last_key: (!key.is_empty()).then(|| key.into()),
            hash,
        };
        value.validate()?;
        Ok(value)
    }
}
impl ReplicationBootstrapTranscriptV3 {
    /// Captures a checked boundary. The caller must durably write its pages
    /// before recording this checkpoint and acknowledging transfer progress.
    pub fn checkpoint(
        &self,
        manifest: ReplicationBootstrapManifestV1,
    ) -> Result<ReplicationBootstrapProgressV1, Error> {
        if self.failed || self.fence != manifest.fence {
            return Err(Error::PredecessorMismatch);
        }
        let value = ReplicationBootstrapProgressV1 {
            manifest,
            pages: self.pages,
            rows: self.rows,
            bytes: self.bytes,
            namespace_index: self.namespace_index,
            last_key: self.last_key.clone(),
            hash: self.hash,
        };
        value.validate()?;
        Ok(value)
    }
    /// Resumes against the exact locally durable last page. This checks only
    /// the checkpoint boundary; publication still requires rereading all pages.
    pub fn resume(
        manifest: ReplicationBootstrapManifestV1,
        progress: ReplicationBootstrapProgressV1,
        last_page: Option<&ReplicationBootstrapPageV3>,
    ) -> Result<Self, Error> {
        progress.validate()?;
        if progress.manifest != manifest {
            return Err(Error::PredecessorMismatch);
        }
        match last_page {
            None if progress.pages == 0 => {}
            Some(page) if progress.pages != 0 => {
                let encoded = page.encode()?;
                let index = progress
                    .namespace_index
                    .checked_sub(usize::from(page.end_namespace))
                    .ok_or(Error::InvalidNamespace)?;
                let key = if page.end_namespace {
                    None
                } else {
                    page.rows.last().map(|r| r.key())
                };
                if page.fence_digest != manifest.fence.digest()
                    || page.ordinal != progress.pages
                    || namespace(index) != Some(page.namespace)
                    || encoded.last_chunk::<32>() != Some(&progress.hash)
                    || key != progress.last_key.as_deref()
                    || progress.rows < page.rows.len() as u64
                    || progress.bytes < encoded.len() as u64
                {
                    return Err(Error::PredecessorMismatch);
                }
            }
            _ => return Err(Error::PredecessorMismatch),
        }
        Ok(Self {
            fence: manifest.fence,
            pages: progress.pages,
            rows: progress.rows,
            bytes: progress.bytes,
            namespace_index: progress.namespace_index,
            last_key: progress.last_key,
            hash: progress.hash,
            failed: false,
        })
    }
}
