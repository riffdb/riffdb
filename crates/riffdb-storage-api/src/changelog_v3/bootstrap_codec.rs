//! Exact-version external bootstrap artifact codec; no database envelope change.
use super::*;
use crate::changelog_v3::receipt_codec::ReceiptReader;
use crate::proto_codec::{decode_changelog_history_state_v3, encode_changelog_history_state_v3};
const PAGE_MAGIC: &[u8; 8] = b"RDBRBP01";
const MANIFEST_MAGIC: &[u8; 8] = b"RDBRBR01";
const MAX_MANIFEST_BYTES: usize = 512;

pub(super) fn checked<'a>(
    bytes: &'a [u8],
    magic: &[u8; 8],
    limit: usize,
) -> Result<ReceiptReader<'a>, Error> {
    if bytes.len() > limit {
        return Err(Error::LimitExceeded);
    }
    let end = bytes.len().checked_sub(32).ok_or(Error::InvalidEncoding)?;
    if bytes[end..] != <[u8; 32]>::from(Sha256::digest(&bytes[..end])) {
        return Err(Error::InvalidEncoding);
    }
    let mut reader = ReceiptReader {
        remaining: &bytes[..end],
    };
    if reader.take(8)? != magic || reader.u16()? != 1 {
        return Err(Error::InvalidEncoding);
    }
    Ok(reader)
}
pub(super) fn checksum(mut bytes: Vec<u8>) -> Vec<u8> {
    let hash: [u8; 32] = Sha256::digest(&bytes).into();
    bytes.extend_from_slice(&hash);
    bytes
}
impl ReplicationBootstrapPageV3 {
    /// Canonical complete rows plus fixed fence/order/end/hash framing.
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut bytes = Vec::with_capacity(self.encoded_len()?);
        bytes.extend_from_slice(PAGE_MAGIC);
        bytes.extend_from_slice(&1u16.to_be_bytes());
        bytes.extend_from_slice(&self.fence_digest);
        bytes.extend_from_slice(&self.ordinal.to_be_bytes());
        bytes.extend_from_slice(&self.namespace.tag().to_be_bytes());
        bytes.push(u8::from(self.end_namespace));
        bytes.extend_from_slice(
            &u16::try_from(self.rows.len())
                .map_err(|_| Error::LimitExceeded)?
                .to_be_bytes(),
        );
        bytes.extend_from_slice(
            &u32::try_from(self.encoded_len()? - PAGE_FIXED_BYTES)
                .map_err(|_| Error::LimitExceeded)?
                .to_be_bytes(),
        );
        bytes.extend_from_slice(&self.prior_hash);
        for row in &self.rows {
            bytes.extend_from_slice(
                &u32::try_from(row.key().len())
                    .map_err(|_| Error::LimitExceeded)?
                    .to_be_bytes(),
            );
            bytes.extend_from_slice(
                &u32::try_from(row.value().len())
                    .map_err(|_| Error::LimitExceeded)?
                    .to_be_bytes(),
            );
            bytes.extend_from_slice(row.key());
            bytes.extend_from_slice(row.value());
        }
        Ok(checksum(bytes))
    }
    /// Rejects oversized input before hashing/allocation, unknown identities,
    /// noncanonical flags, unknown/control namespaces, bad order and trailing bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let mut read = checked(bytes, PAGE_MAGIC, MAX_REPLICATION_BOOTSTRAP_PAGE_BYTES)?;
        let fence_digest = read.array()?;
        let ordinal = read.u32()?;
        let namespace = crate::AuthoritativeStateCatalogV1
            .by_tag(read.u16()?)
            .ok_or(Error::InvalidNamespace)?;
        let end = match read.array::<1>()? {
            [0] => false,
            [1] => true,
            _ => return Err(Error::InvalidEncoding),
        };
        let count = usize::from(read.u16()?);
        let payload = usize::try_from(read.u32()?).map_err(|_| Error::LimitExceeded)?;
        let prior_hash = read.array()?;
        if payload != read.remaining.len()
            || count > MAX_REPLICATION_BOOTSTRAP_PAGE_ROWS
            || count > payload / 9
        {
            return Err(Error::InvalidEncoding);
        }
        let mut rows = Vec::with_capacity(count);
        for _ in 0..count {
            let key = usize::try_from(read.u32()?).map_err(|_| Error::LimitExceeded)?;
            let value = usize::try_from(read.u32()?).map_err(|_| Error::LimitExceeded)?;
            rows.push(AuthoritativeStateRowV3::new(
                namespace,
                read.take(key)?,
                read.take(value)?,
            )?);
        }
        if !read.remaining.is_empty() {
            return Err(Error::InvalidEncoding);
        }
        Self::new(fence_digest, ordinal, namespace, end, prior_hash, rows)
    }
}
impl ReplicationBootstrapManifestV1 {
    /// The V1 external receipt embeds the existing V3 history envelope exactly.
    pub fn encode(self) -> Result<Vec<u8>, Error> {
        let history = encode_changelog_history_state_v3(self.fence.history)
            .map_err(|_| Error::InvalidEncoding)?;
        let mut bytes = Vec::with_capacity(MAX_MANIFEST_BYTES);
        bytes.extend_from_slice(MANIFEST_MAGIC);
        bytes.extend_from_slice(&1u16.to_be_bytes());
        bytes.extend_from_slice(self.fence.id.as_bytes());
        bytes.extend_from_slice(
            &u16::try_from(history.as_bytes().len())
                .map_err(|_| Error::LimitExceeded)?
                .to_be_bytes(),
        );
        bytes.extend_from_slice(history.as_bytes());
        bytes.extend_from_slice(&self.pages.to_be_bytes());
        bytes.extend_from_slice(&self.rows.to_be_bytes());
        bytes.extend_from_slice(&self.bytes.to_be_bytes());
        bytes.extend_from_slice(&self.final_hash);
        let bytes = checksum(bytes);
        if bytes.len() > MAX_MANIFEST_BYTES {
            return Err(Error::LimitExceeded);
        }
        Ok(bytes)
    }
    /// Checks exact identity, bounded counts and the checksum. This alone is
    /// not a complete transcript proof; receivers must verify every page/end.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let mut read = checked(bytes, MANIFEST_MAGIC, MAX_MANIFEST_BYTES)?;
        let id = ReplicationSourceHoldIdV1::new(read.array()?).ok_or(Error::InvalidEncoding)?;
        let len = usize::from(read.u16()?);
        let history = *decode_changelog_history_state_v3(read.take(len)?)
            .map_err(|_| Error::InvalidEncoding)?
            .value();
        let pages = read.u32()?;
        let rows = read.u64()?;
        let bytes = read.u64()?;
        let final_hash = read.array()?;
        let namespaces = N::ALL
            .into_iter()
            .filter(|n| n.class() == ReplicationAuthorityClassV1::ReplicatedAuthoritative)
            .count() as u32;
        if !read.remaining.is_empty()
            || pages < namespaces
            || pages > MAX_REPLICATION_BOOTSTRAP_PAGES
            || rows > u64::from(pages) * MAX_REPLICATION_BOOTSTRAP_PAGE_ROWS as u64
            || bytes < u64::from(pages) * PAGE_FIXED_BYTES as u64
            || bytes > MAX_REPLICATION_BOOTSTRAP_BYTES
            || bytes > u64::from(pages) * MAX_REPLICATION_BOOTSTRAP_PAGE_BYTES as u64
        {
            return Err(Error::InvalidEncoding);
        }
        Ok(Self {
            fence: ReplicationBootstrapFenceV3::new(id, history),
            pages,
            rows,
            bytes,
            final_hash,
        })
    }
}
