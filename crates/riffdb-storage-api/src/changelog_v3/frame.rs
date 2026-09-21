use riffdb_types::{DatabaseId, DualFrontier};
use sha2::{Digest, Sha256};

use crate::{
    AuthoritativeStateCatalogV1, AuthoritativeStateCatalogV2, MAX_CHANGELOG_FRAME_BYTES,
    MAX_STAGED_COMMANDS,
};

use super::receipt_codec::ReceiptReader;
use super::{AuthoritativeTransactionV3, ChangelogV3Error, LeadershipEpochV1};

const MAGIC: &[u8; 8] = b"RDBCLF03";
const FOOTER_MAGIC: &[u8; 8] = b"RDBCLE03";
// The catalog digest discriminates the counter table. Never grow the original
// header when the accepted attribution set gains a successor member.
const ORIGINAL_SOURCE_COUNT: usize = 33;
const SOURCE_COUNT: usize = 34;
const HEADER_BYTES: usize = 166 + 4 * ORIGINAL_SOURCE_COUNT;
const FOOTER_BYTES: usize = 48;
pub(super) const FIXED_FRAME_BYTES: usize = HEADER_BYTES + FOOTER_BYTES;

// Preserve the original receipt ceiling for compatibility. Catalog-specific
// admission additionally charges the selected frame header before mutation.
pub(super) const MAX_RECEIPT_BYTES: usize = MAX_CHANGELOG_FRAME_BYTES - FIXED_FRAME_BYTES - 4;

fn catalog_source_count(catalog: [u8; 32]) -> Result<usize, ChangelogV3Error> {
    if catalog == AuthoritativeStateCatalogV1.digest() {
        Ok(ORIGINAL_SOURCE_COUNT)
    } else if catalog == AuthoritativeStateCatalogV2.digest() {
        Ok(SOURCE_COUNT)
    } else {
        Err(ChangelogV3Error::InvalidEncoding)
    }
}

pub(super) fn validate_receipt_catalog(
    receipt: &AuthoritativeTransactionV3,
    catalog: [u8; 32],
) -> Result<(), ChangelogV3Error> {
    validate_receipt_count(receipt, catalog_source_count(catalog)?)
}

fn validate_receipt_count(
    receipt: &AuthoritativeTransactionV3,
    count: usize,
) -> Result<(), ChangelogV3Error> {
    if receipt.attribution() as usize > count {
        return Err(ChangelogV3Error::InvalidEncoding);
    }
    let maximum = MAX_CHANGELOG_FRAME_BYTES - (166 + 4 * count + FOOTER_BYTES) - 4;
    if receipt.encoded_len()? > maximum {
        return Err(ChangelogV3Error::LimitExceeded);
    }
    Ok(())
}

/// Exact checked lineage, leadership, catalog, and prior-frame binding.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ChangelogFrameBindingV3 {
    database_id: DatabaseId,
    history_incarnation: u64,
    leadership_epoch: LeadershipEpochV1,
    catalog_digest: [u8; 32],
    source_count: usize,
    prior_frame_hash: [u8; 32],
}

impl ChangelogFrameBindingV3 {
    /// Refuses zero fences and catalogs outside the two accepted V3 bindings.
    pub fn new(
        database_id: DatabaseId,
        history_incarnation: u64,
        leadership_epoch: u64,
        catalog_digest: [u8; 32],
        prior_frame_hash: [u8; 32],
    ) -> Result<Self, ChangelogV3Error> {
        let leadership_epoch =
            LeadershipEpochV1::new(leadership_epoch).ok_or(ChangelogV3Error::InvalidEncoding)?;
        let source_count = catalog_source_count(catalog_digest)?;
        if history_incarnation == 0 {
            return Err(ChangelogV3Error::InvalidEncoding);
        }
        Ok(Self {
            database_id,
            history_incarnation,
            leadership_epoch,
            catalog_digest,
            source_count,
            prior_frame_hash,
        })
    }

    pub(super) const fn header_bytes(self) -> usize {
        166 + 4 * self.source_count
    }
    pub(super) const fn fixed_frame_bytes(self) -> usize {
        self.header_bytes() + FOOTER_BYTES
    }

    /// Exact database identity.
    #[must_use]
    pub const fn database_id(self) -> DatabaseId {
        self.database_id
    }
    /// Nonzero history incarnation.
    #[must_use]
    pub const fn history_incarnation(self) -> u64 {
        self.history_incarnation
    }
    /// Nonzero leadership fence.
    #[must_use]
    pub const fn leadership_epoch(self) -> u64 {
        self.leadership_epoch.get()
    }
    /// Exact catalog digest; never diagnostic data.
    #[must_use]
    pub const fn catalog_digest(self) -> [u8; 32] {
        self.catalog_digest
    }
    /// Exact prior frame checksum; never diagnostic data.
    #[must_use]
    pub const fn prior_frame_hash(self) -> [u8; 32] {
        self.prior_frame_hash
    }
}

impl std::fmt::Debug for ChangelogFrameBindingV3 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ChangelogFrameBindingV3([redacted])")
    }
}

/// Bounded nonempty group of complete V3 receipts. Never carries journal bytes.
#[derive(Clone, Eq, PartialEq)]
pub struct ChangelogFrameV3 {
    binding: ChangelogFrameBindingV3,
    receipts: Vec<AuthoritativeTransactionV3>,
}

impl ChangelogFrameV3 {
    /// The sole production-negotiable identity of this complete-authority format.
    pub const IDENTITY: &'static str = "riffdb.changelog-frame/v3";

    /// Validates every receipt's lineage, position, frontier and history-chain edge.
    /// A receipt is never split to fit the frame's independent hard ceilings.
    pub fn new(
        binding: ChangelogFrameBindingV3,
        receipts: Vec<AuthoritativeTransactionV3>,
    ) -> Result<Self, ChangelogV3Error> {
        if receipts.is_empty() {
            return Err(ChangelogV3Error::InvalidEncoding);
        }
        if receipts.len() > MAX_STAGED_COMMANDS {
            return Err(ChangelogV3Error::LimitExceeded);
        }
        let mut transitions = 0_u64;
        for (index, receipt) in receipts.iter().enumerate() {
            validate_receipt_count(receipt, binding.source_count)?;
            let row = receipt.binding();
            if row.database_id != binding.database_id
                || row.history_incarnation != binding.history_incarnation
            {
                return Err(ChangelogV3Error::PredecessorMismatch);
            }
            transitions = transitions
                .checked_add(receipt.transition_count())
                .ok_or(ChangelogV3Error::LimitExceeded)?;
            if transitions > MAX_STAGED_COMMANDS as u64 {
                return Err(ChangelogV3Error::LimitExceeded);
            }
            if index > 0 {
                let previous = &receipts[index - 1];
                let prior = previous.binding();
                if row.predecessor != Some(prior.sequence)
                    || row.predecessor_frontier != prior.covered_frontier
                    || row.prior_history_hash != previous.history_hash()?
                {
                    return Err(ChangelogV3Error::PredecessorMismatch);
                }
            }
        }
        let frame = Self { binding, receipts };
        if frame.encoded_len()? > MAX_CHANGELOG_FRAME_BYTES {
            return Err(ChangelogV3Error::LimitExceeded);
        }
        Ok(frame)
    }

    /// Checked immutable frame binding.
    #[must_use]
    pub const fn binding(&self) -> ChangelogFrameBindingV3 {
        self.binding
    }
    /// Complete receipts in exact physical transaction order.
    #[must_use]
    pub fn receipts(&self) -> &[AuthoritativeTransactionV3] {
        &self.receipts
    }

    /// Total frame bytes including checksummed header and torn-tail footer.
    pub fn encoded_len(&self) -> Result<usize, ChangelogV3Error> {
        self.receipts
            .iter()
            .try_fold(self.binding.fixed_frame_bytes(), |bytes, row| {
                bytes
                    .checked_add(4)
                    .and_then(|n| n.checked_add(row.encoded_len().ok()?))
                    .ok_or(ChangelogV3Error::LimitExceeded)
            })
    }

    fn source_counts(&self) -> Result<[u32; SOURCE_COUNT], ChangelogV3Error> {
        let mut counts = [0; SOURCE_COUNT];
        for receipt in &self.receipts {
            let slot = counts
                .get_mut(receipt.attribution() as usize - 1)
                .ok_or(ChangelogV3Error::InvalidEncoding)?;
            *slot += 1;
        }
        Ok(counts)
    }

    /// Canonical V3 only; no V1/V2 fallback or journal-format translation.
    pub fn encode(&self) -> Result<Vec<u8>, ChangelogV3Error> {
        let total = self.encoded_len()?;
        let first = self
            .receipts
            .first()
            .ok_or(ChangelogV3Error::InvalidEncoding)?
            .binding();
        let last = self
            .receipts
            .last()
            .ok_or(ChangelogV3Error::InvalidEncoding)?
            .binding();
        let mut bytes = Vec::with_capacity(total);
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&3_u16.to_be_bytes());
        bytes.extend_from_slice(self.binding.database_id.as_bytes());
        bytes.extend_from_slice(&self.binding.history_incarnation.to_be_bytes());
        bytes.extend_from_slice(&self.binding.leadership_epoch.get().to_be_bytes());
        bytes.extend_from_slice(&self.binding.catalog_digest);
        bytes.extend_from_slice(&first.predecessor.map_or(0, |p| p.get()).to_be_bytes());
        bytes.extend_from_slice(&last.sequence.get().to_be_bytes());
        bytes.extend_from_slice(&first.predecessor_frontier.to_canonical_bytes());
        bytes.extend_from_slice(&last.covered_frontier.to_canonical_bytes());
        bytes.extend_from_slice(&self.binding.prior_frame_hash);
        bytes.extend_from_slice(
            &u32::try_from(self.receipts.len())
                .map_err(|_| ChangelogV3Error::LimitExceeded)?
                .to_be_bytes(),
        );
        bytes.extend_from_slice(
            &u32::try_from(total - self.binding.fixed_frame_bytes())
                .map_err(|_| ChangelogV3Error::LimitExceeded)?
                .to_be_bytes(),
        );
        for count in self
            .source_counts()?
            .into_iter()
            .take(self.binding.source_count)
        {
            bytes.extend_from_slice(&count.to_be_bytes());
        }
        for receipt in &self.receipts {
            let encoded = receipt.encode()?;
            bytes.extend_from_slice(
                &u32::try_from(encoded.len())
                    .map_err(|_| ChangelogV3Error::LimitExceeded)?
                    .to_be_bytes(),
            );
            bytes.extend_from_slice(&encoded);
        }
        let checksum: [u8; 32] = Sha256::digest(&bytes).into();
        bytes.extend_from_slice(FOOTER_MAGIC);
        bytes.extend_from_slice(
            &u64::try_from(total)
                .map_err(|_| ChangelogV3Error::LimitExceeded)?
                .to_be_bytes(),
        );
        bytes.extend_from_slice(&checksum);
        Ok(bytes)
    }

    /// Validates checksums, exact ends, counts, identities and every receipt edge
    /// before yielding a frame. Allocation is bounded by actual input bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, ChangelogV3Error> {
        if bytes.len() > MAX_CHANGELOG_FRAME_BYTES {
            return Err(ChangelogV3Error::LimitExceeded);
        }
        if bytes.len() < HEADER_BYTES + FOOTER_BYTES {
            return Err(ChangelogV3Error::InvalidEncoding);
        }
        let end = bytes.len() - FOOTER_BYTES;
        let mut footer = ReceiptReader {
            remaining: &bytes[end..],
        };
        let checksum: [u8; 32] = Sha256::digest(&bytes[..end]).into();
        if footer.take(8)? != FOOTER_MAGIC
            || footer.u64()? != bytes.len() as u64
            || footer.take(32)? != checksum
        {
            return Err(ChangelogV3Error::InvalidEncoding);
        }
        let mut reader = ReceiptReader {
            remaining: &bytes[..end],
        };
        if reader.take(8)? != MAGIC || reader.u16()? != 3 {
            return Err(ChangelogV3Error::InvalidEncoding);
        }
        let database_id = DatabaseId::from_bytes(reader.array()?)
            .map_err(|_| ChangelogV3Error::InvalidEncoding)?;
        let incarnation = reader.u64()?;
        let epoch = reader.u64()?;
        let catalog = reader.array()?;
        let predecessor = reader.u64()?;
        let covered = reader.u64()?;
        let predecessor_frontier = DualFrontier::from_canonical_bytes(reader.array()?)
            .map_err(|_| ChangelogV3Error::InvalidEncoding)?;
        let covered_frontier = DualFrontier::from_canonical_bytes(reader.array()?)
            .map_err(|_| ChangelogV3Error::InvalidEncoding)?;
        let binding = ChangelogFrameBindingV3::new(
            database_id,
            incarnation,
            epoch,
            catalog,
            reader.array()?,
        )?;
        let count = usize::try_from(reader.u32()?).map_err(|_| ChangelogV3Error::LimitExceeded)?;
        let payload_len =
            usize::try_from(reader.u32()?).map_err(|_| ChangelogV3Error::LimitExceeded)?;
        if count > MAX_STAGED_COMMANDS {
            return Err(ChangelogV3Error::LimitExceeded);
        }
        let mut source_counts = [0; SOURCE_COUNT];
        for slot in source_counts.iter_mut().take(binding.source_count) {
            *slot = reader.u32()?;
        }
        if count == 0 || payload_len != reader.remaining.len() || count > payload_len / 164 {
            return Err(ChangelogV3Error::InvalidEncoding);
        }
        let mut receipts = Vec::with_capacity(count);
        for _ in 0..count {
            let length =
                usize::try_from(reader.u32()?).map_err(|_| ChangelogV3Error::LimitExceeded)?;
            receipts.push(AuthoritativeTransactionV3::decode(reader.take(length)?)?);
        }
        if !reader.remaining.is_empty() {
            return Err(ChangelogV3Error::InvalidEncoding);
        }
        let frame = Self::new(binding, receipts)?;
        let first = frame
            .receipts
            .first()
            .ok_or(ChangelogV3Error::InvalidEncoding)?
            .binding();
        let last = frame
            .receipts
            .last()
            .ok_or(ChangelogV3Error::InvalidEncoding)?
            .binding();
        if predecessor != first.predecessor.map_or(0, |p| p.get())
            || covered != last.sequence.get()
            || predecessor_frontier != first.predecessor_frontier
            || covered_frontier != last.covered_frontier
            || source_counts != frame.source_counts()?
        {
            return Err(ChangelogV3Error::InvalidEncoding);
        }
        Ok(frame)
    }
}

impl std::fmt::Debug for ChangelogFrameV3 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ChangelogFrameV3([redacted])")
    }
}
