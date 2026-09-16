//! Bounded page commitments and prefix accounting for ADR-0232.
//!
//! These values do not select a storage format or authorize a write. The closed
//! repository transition must supply actual encoded charges and compare the
//! complete operation head. The format gate and durable codecs own activation.

use std::{fmt, num::NonZeroU16};

use riffdb_types::{
    ApplicationExportClassV1, ApplicationExportLedgerHash, ApplicationExportOperationId,
    ApplicationExportPageHash, HashDomain, MAX_APPLICATION_EXPORT_PAGE_BYTES,
    MAX_APPLICATION_EXPORT_PAGE_ROWS, MAX_APPLICATION_EXPORT_PAGES, hash,
};

use crate::{MAX_APPLICATION_EXPORT_STATE_BYTES, StorageValueError};

/// A nonzero page position within the unchanged 4,096-page export ceiling.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ApplicationExportPageOrdinalV1(NonZeroU16);

impl ApplicationExportPageOrdinalV1 {
    /// Refuses zero, values outside the page ceiling and truncation.
    pub fn new(value: u64) -> Result<Self, StorageValueError> {
        if value == 0 || value > MAX_APPLICATION_EXPORT_PAGES as u64 {
            return Err(StorageValueError::LimitExceeded);
        }
        let value = u16::try_from(value).map_err(|_| StorageValueError::LimitExceeded)?;
        NonZeroU16::new(value)
            .map(Self)
            .ok_or(StorageValueError::InvalidShape)
    }

    /// Exact one-based ordinal.
    #[must_use]
    pub const fn get(self) -> u16 {
        self.0.get()
    }
}

/// One complete page commitment; content hashing remains the public V1 hash.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ApplicationExportPageCommitmentV1 {
    operation: ApplicationExportOperationId,
    ordinal: ApplicationExportPageOrdinalV1,
    class: ApplicationExportClassV1,
    page_hash: ApplicationExportPageHash,
    rows: u64,
    bytes: u64,
}

impl ApplicationExportPageCommitmentV1 {
    /// Checks the same row/content ceilings as the public page constructor.
    pub fn new(
        operation: ApplicationExportOperationId,
        ordinal: ApplicationExportPageOrdinalV1,
        class: ApplicationExportClassV1,
        page_hash: ApplicationExportPageHash,
        rows: u64,
        bytes: u64,
    ) -> Result<Self, StorageValueError> {
        if rows > MAX_APPLICATION_EXPORT_PAGE_ROWS as u64
            || bytes > MAX_APPLICATION_EXPORT_PAGE_BYTES as u64
        {
            return Err(StorageValueError::LimitExceeded);
        }
        if (rows == 0) != (bytes == 0) {
            return Err(StorageValueError::InvalidShape);
        }
        Ok(Self {
            operation,
            ordinal,
            class,
            page_hash,
            rows,
            bytes,
        })
    }

    /// Owning export identity.
    #[must_use]
    pub const fn operation(&self) -> ApplicationExportOperationId {
        self.operation
    }
    /// Exact one-based page position.
    #[must_use]
    pub const fn ordinal(&self) -> ApplicationExportPageOrdinalV1 {
        self.ordinal
    }
    /// Closed exported content class.
    #[must_use]
    pub const fn class(&self) -> ApplicationExportClassV1 {
        self.class
    }
    /// Unchanged public content hash.
    #[must_use]
    pub const fn page_hash(&self) -> ApplicationExportPageHash {
        self.page_hash
    }
    /// Released row total for this page.
    #[must_use]
    pub const fn rows(&self) -> u64 {
        self.rows
    }
    /// Released content byte total for this page.
    #[must_use]
    pub const fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Versioned complete canonical commitment preimage, independent of cursor
    /// and response replay bytes. Every member participates in the prefix hash.
    #[must_use]
    pub fn canonical_bytes(&self) -> [u8; 72] {
        let mut out = [0; 72];
        out[..5].copy_from_slice(b"RXPL\x01");
        out[5..21].copy_from_slice(self.operation.as_bytes());
        out[21..23].copy_from_slice(&self.ordinal.get().to_be_bytes());
        out[23] = self.class.tag();
        out[24..56].copy_from_slice(self.page_hash.as_bytes());
        out[56..64].copy_from_slice(&self.rows.to_be_bytes());
        out[64..].copy_from_slice(&self.bytes.to_be_bytes());
        out
    }

    /// Strictly decodes the complete versioned commitment; no trailing bytes,
    /// unknown class, invalid owner or out-of-budget count is accepted.
    pub fn from_canonical_bytes(bytes: &[u8]) -> Result<Self, StorageValueError> {
        if bytes.len() != 72 || &bytes[..5] != b"RXPL\x01" {
            return Err(StorageValueError::InvalidShape);
        }
        fn fixed<const N: usize>(bytes: &[u8]) -> Result<[u8; N], StorageValueError> {
            bytes
                .try_into()
                .map_err(|_| StorageValueError::InvalidShape)
        }
        Self::new(
            ApplicationExportOperationId::from_bytes(fixed(&bytes[5..21])?)
                .map_err(|_| StorageValueError::InvalidShape)?,
            ApplicationExportPageOrdinalV1::new(u64::from(u16::from_be_bytes(fixed(
                &bytes[21..23],
            )?)))?,
            ApplicationExportClassV1::from_tag(bytes[23]).ok_or(StorageValueError::InvalidShape)?,
            ApplicationExportPageHash::from_bytes(fixed(&bytes[24..56])?),
            u64::from_be_bytes(fixed(&bytes[56..64])?),
            u64::from_be_bytes(fixed(&bytes[64..72])?),
        )
    }

    /// Exact operation/ordinal key for the separately registered V1 namespace.
    /// Big-endian ordinals preserve the operation's canonical page order.
    #[must_use]
    pub fn canonical_key(&self) -> [u8; 18] {
        let mut key = [0; 18];
        key[..16].copy_from_slice(self.operation.as_bytes());
        key[16..].copy_from_slice(&self.ordinal.get().to_be_bytes());
        key
    }

    /// Proves reciprocal physical-key/record ownership after decoding.
    pub fn validate_key(&self, key: &[u8]) -> Result<(), StorageValueError> {
        if key == self.canonical_key() {
            Ok(())
        } else {
            Err(StorageValueError::IdentityMismatch)
        }
    }
}

impl fmt::Debug for ApplicationExportPageCommitmentV1 {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str("ApplicationExportPageCommitmentV1([REDACTED])")
    }
}

/// Fixed-size ordered ledger evidence. It owns no prior entries or page bytes.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ApplicationExportLedgerPrefixV1 {
    operation: ApplicationExportOperationId,
    genesis: ApplicationExportLedgerHash,
    commitment: ApplicationExportLedgerHash,
    pages: u16,
    class_pages: [u16; 4],
    class_rows: [u64; 4],
    class_bytes: [u64; 4],
    retained_bytes: usize,
}

impl ApplicationExportLedgerPrefixV1 {
    /// Fixed framing for the compact head's claimed prefix. Decoding checks
    /// shape and bounds; only `verify` or an exact atomic append proves that
    /// the corresponding retained ledger actually has this commitment.
    pub fn canonical_bytes(&self) -> Result<[u8; 163], StorageValueError> {
        let mut out = [0; 163];
        out[..5].copy_from_slice(b"RXPF\x01");
        out[5..21].copy_from_slice(self.operation.as_bytes());
        out[21..53].copy_from_slice(self.genesis.as_bytes());
        out[53..85].copy_from_slice(self.commitment.as_bytes());
        out[85..87].copy_from_slice(&self.pages.to_be_bytes());
        for index in 0..4 {
            out[87 + 2 * index..89 + 2 * index]
                .copy_from_slice(&self.class_pages[index].to_be_bytes());
            out[95 + 8 * index..103 + 8 * index]
                .copy_from_slice(&self.class_rows[index].to_be_bytes());
            out[127 + 8 * index..135 + 8 * index]
                .copy_from_slice(&self.class_bytes[index].to_be_bytes());
        }
        let retained =
            u32::try_from(self.retained_bytes).map_err(|_| StorageValueError::LimitExceeded)?;
        out[159..].copy_from_slice(&retained.to_be_bytes());
        Ok(out)
    }

    /// Strict versioned prefix decoder. Immutable genesis binding and complete
    /// ledger membership must additionally be checked by the head repository.
    pub fn from_canonical_bytes(bytes: &[u8]) -> Result<Self, StorageValueError> {
        if bytes.len() != 163 || &bytes[..5] != b"RXPF\x01" {
            return Err(StorageValueError::InvalidShape);
        }
        fn fixed<const N: usize>(bytes: &[u8]) -> Result<[u8; N], StorageValueError> {
            bytes
                .try_into()
                .map_err(|_| StorageValueError::InvalidShape)
        }
        let mut prefix = Self {
            operation: ApplicationExportOperationId::from_bytes(fixed(&bytes[5..21])?)
                .map_err(|_| StorageValueError::InvalidShape)?,
            genesis: ApplicationExportLedgerHash::from_bytes(fixed(&bytes[21..53])?),
            commitment: ApplicationExportLedgerHash::from_bytes(fixed(&bytes[53..85])?),
            pages: u16::from_be_bytes(fixed(&bytes[85..87])?),
            class_pages: [0; 4],
            class_rows: [0; 4],
            class_bytes: [0; 4],
            retained_bytes: usize::try_from(u32::from_be_bytes(fixed(&bytes[159..])?))
                .map_err(|_| StorageValueError::LimitExceeded)?,
        };
        if usize::from(prefix.pages) > MAX_APPLICATION_EXPORT_PAGES
            || prefix.retained_bytes > MAX_APPLICATION_EXPORT_STATE_BYTES
            || prefix.retained_bytes < usize::from(prefix.pages)
        {
            return Err(StorageValueError::LimitExceeded);
        }
        let mut total_pages = 0_u32;
        for index in 0..4 {
            let pages = u16::from_be_bytes(fixed(&bytes[87 + 2 * index..89 + 2 * index])?);
            let rows = u64::from_be_bytes(fixed(&bytes[95 + 8 * index..103 + 8 * index])?);
            let content = u64::from_be_bytes(fixed(&bytes[127 + 8 * index..135 + 8 * index])?);
            if rows > u64::from(pages) * MAX_APPLICATION_EXPORT_PAGE_ROWS as u64
                || content > u64::from(pages) * MAX_APPLICATION_EXPORT_PAGE_BYTES as u64
                || (rows == 0) != (content == 0)
            {
                return Err(StorageValueError::InvalidShape);
            }
            prefix.class_pages[index] = pages;
            prefix.class_rows[index] = rows;
            prefix.class_bytes[index] = content;
            total_pages += u32::from(pages);
        }
        if total_pages != u32::from(prefix.pages)
            || (prefix.pages == 0
                && (prefix.commitment != prefix.genesis || prefix.retained_bytes != 0))
        {
            return Err(StorageValueError::InvalidShape);
        }
        Ok(prefix)
    }

    /// Binds the exact immutable operation/snapshot/authority/selection bytes.
    /// The service owns their canonical interpretation and must reconstruct
    /// this same binding when decoding a retained compact head.
    pub fn genesis(
        operation: ApplicationExportOperationId,
        immutable_binding: &[u8],
    ) -> Result<Self, StorageValueError> {
        if immutable_binding.is_empty() {
            return Err(StorageValueError::Empty);
        }
        if immutable_binding.len() > MAX_APPLICATION_EXPORT_STATE_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        let length =
            u32::try_from(immutable_binding.len()).map_err(|_| StorageValueError::LimitExceeded)?;
        let mut preimage = Vec::with_capacity(21 + immutable_binding.len());
        preimage.push(0); // Genesis and append are separate uses of this domain.
        preimage.extend_from_slice(operation.as_bytes());
        preimage.extend_from_slice(&length.to_be_bytes());
        preimage.extend_from_slice(immutable_binding);
        let genesis = ledger_hash(&preimage);
        Ok(Self {
            operation,
            genesis,
            commitment: genesis,
            pages: 0,
            class_pages: [0; 4],
            class_rows: [0; 4],
            class_bytes: [0; 4],
            retained_bytes: 0,
        })
    }

    /// Advances exactly once without retaining or reading the earlier prefix.
    /// `encoded_entry_charge` includes the actual retained key and value bytes;
    /// only the storage codec/repository may supply it for a durable transition.
    pub fn advance(
        &self,
        entry: &ApplicationExportPageCommitmentV1,
        encoded_entry_charge: usize,
    ) -> Result<Self, StorageValueError> {
        if entry.operation != self.operation || entry.ordinal.get() != self.pages + 1 {
            return Err(StorageValueError::IdentityMismatch);
        }
        if usize::from(self.pages) == MAX_APPLICATION_EXPORT_PAGES {
            return Err(StorageValueError::LimitExceeded);
        }
        if encoded_entry_charge == 0 {
            return Err(StorageValueError::Empty);
        }
        let class = usize::from(entry.class.tag() - 1);
        if self.class_pages[class + 1..]
            .iter()
            .any(|pages| *pages != 0)
        {
            return Err(StorageValueError::NonCanonicalOrder);
        }
        let retained_bytes = self
            .retained_bytes
            .checked_add(encoded_entry_charge)
            .ok_or(StorageValueError::SizeOverflow)?;
        if retained_bytes > MAX_APPLICATION_EXPORT_STATE_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        let mut preimage = [0; 105];
        preimage[0] = 1;
        preimage[1..33].copy_from_slice(self.commitment.as_bytes());
        preimage[33..].copy_from_slice(&entry.canonical_bytes());
        let mut next = *self;
        next.commitment = ledger_hash(&preimage);
        next.pages += 1;
        next.class_pages[class] += 1;
        next.class_rows[class] = next.class_rows[class]
            .checked_add(entry.rows)
            .ok_or(StorageValueError::SizeOverflow)?;
        next.class_bytes[class] = next.class_bytes[class]
            .checked_add(entry.bytes)
            .ok_or(StorageValueError::SizeOverflow)?;
        next.retained_bytes = retained_bytes;
        Ok(next)
    }

    /// Charges compact head, retained ledger and terminal-document headroom to
    /// the existing aggregate durable-state ceiling before a page is released.
    pub fn check_budget(
        &self,
        encoded_head_bytes: usize,
        terminal_reserve_bytes: usize,
    ) -> Result<(), StorageValueError> {
        if encoded_head_bytes == 0 {
            return Err(StorageValueError::Empty);
        }
        let total = self
            .retained_bytes
            .checked_add(encoded_head_bytes)
            .and_then(|bytes| bytes.checked_add(terminal_reserve_bytes))
            .ok_or(StorageValueError::SizeOverflow)?;
        if total > MAX_APPLICATION_EXPORT_STATE_BYTES {
            return Err(StorageValueError::LimitExceeded);
        }
        Ok(())
    }

    /// Verifies a complete contiguous retained prefix once at terminalization
    /// or recovery, returning the original ordered public manifest hash list.
    /// The caller supplies the independently recomputed immutable genesis and
    /// exact codec charges. Extra entries are rejected before accumulating them.
    pub fn verify(
        &self,
        genesis: Self,
        entries: impl IntoIterator<Item = (ApplicationExportPageCommitmentV1, usize)>,
    ) -> Result<Vec<ApplicationExportPageHash>, StorageValueError> {
        if genesis.pages != 0
            || genesis.operation != self.operation
            || genesis.genesis != self.genesis
        {
            return Err(StorageValueError::IdentityMismatch);
        }
        let mut observed = genesis;
        let mut hashes = Vec::with_capacity(usize::from(self.pages));
        for (entry, charge) in entries {
            if observed.pages >= self.pages {
                return Err(StorageValueError::IdentityMismatch);
            }
            observed = observed.advance(&entry, charge)?;
            hashes.push(entry.page_hash);
        }
        if observed != *self {
            return Err(StorageValueError::IdentityMismatch);
        }
        Ok(hashes)
    }

    /// Exact owning operation.
    #[must_use]
    pub const fn operation(&self) -> ApplicationExportOperationId {
        self.operation
    }
    /// Immutable genesis evidence.
    #[must_use]
    pub const fn genesis_hash(&self) -> ApplicationExportLedgerHash {
        self.genesis
    }
    /// Complete ordered prefix commitment.
    #[must_use]
    pub const fn commitment(&self) -> ApplicationExportLedgerHash {
        self.commitment
    }
    /// Total retained pages, including empty pages.
    #[must_use]
    pub const fn pages(&self) -> u16 {
        self.pages
    }
    /// Page counts in Entity/Event/Provenance/PublicAudit order.
    #[must_use]
    pub const fn class_pages(&self) -> &[u16; 4] {
        &self.class_pages
    }
    /// Row totals in the same closed class order.
    #[must_use]
    pub const fn class_rows(&self) -> &[u64; 4] {
        &self.class_rows
    }
    /// Content byte totals in the same closed class order.
    #[must_use]
    pub const fn class_bytes(&self) -> &[u64; 4] {
        &self.class_bytes
    }
    /// Actual retained key/value bytes charged to the durable-state budget.
    #[must_use]
    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}

impl fmt::Debug for ApplicationExportLedgerPrefixV1 {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.write_str("ApplicationExportLedgerPrefixV1([REDACTED])")
    }
}

fn ledger_hash(bytes: &[u8]) -> ApplicationExportLedgerHash {
    ApplicationExportLedgerHash::from_bytes(
        *hash(HashDomain::ApplicationExportLedger, bytes).as_bytes(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn operation(seed: u8) -> ApplicationExportOperationId {
        ApplicationExportOperationId::from_unix_milliseconds_and_random(1, [seed; 10]).unwrap()
    }

    fn entry(ordinal: u64, class: ApplicationExportClassV1) -> ApplicationExportPageCommitmentV1 {
        ApplicationExportPageCommitmentV1::new(
            operation(1),
            ApplicationExportPageOrdinalV1::new(ordinal).unwrap(),
            class,
            ApplicationExportPageHash::from_bytes([ordinal as u8; 32]),
            2,
            40,
        )
        .unwrap()
    }

    // req: EXP-006, EXP-007, EXP-008, EXP-009
    #[test]
    fn export_ledger_prefix_is_ordered_complete_and_bound_to_immutable_genesis() {
        let initial =
            ApplicationExportLedgerPrefixV1::genesis(operation(1), b"exact-snapshot-and-authority")
                .unwrap();
        let entries = vec![
            entry(1, ApplicationExportClassV1::Entity),
            entry(2, ApplicationExportClassV1::Entity),
            entry(3, ApplicationExportClassV1::Event),
        ];
        let mut prefix = initial;
        // Synthetic codec charges isolate accounting from the future durable
        // codec integration; no storage write uses these test values.
        for entry in &entries {
            prefix = prefix.advance(entry, 100).unwrap();
        }
        assert_eq!(prefix.pages(), 3);
        assert_eq!(prefix.class_pages(), &[2, 1, 0, 0]);
        assert_eq!(prefix.class_rows(), &[4, 2, 0, 0]);
        assert_eq!(prefix.class_bytes(), &[80, 40, 0, 0]);
        assert_eq!(prefix.retained_bytes(), 300);
        let hashes = prefix
            .verify(initial, entries.iter().map(|entry| (*entry, 100)))
            .unwrap();
        assert_eq!(
            hashes,
            entries
                .iter()
                .map(|entry| entry.page_hash())
                .collect::<Vec<_>>()
        );
        for binding in [
            b"other-snapshot-and-authority".as_slice(),
            b"exact-snapshot-and-authority\0",
        ] {
            let substituted =
                ApplicationExportLedgerPrefixV1::genesis(operation(1), binding).unwrap();
            assert!(
                prefix
                    .verify(substituted, entries.iter().map(|entry| (*entry, 100)))
                    .is_err()
            );
        }
        let foreign =
            ApplicationExportLedgerPrefixV1::genesis(operation(2), b"exact-snapshot-and-authority")
                .unwrap();
        assert!(
            prefix
                .verify(foreign, entries.iter().map(|entry| (*entry, 100)))
                .is_err()
        );
        assert!(
            prefix
                .verify(initial, entries.iter().take(2).map(|entry| (*entry, 100)))
                .is_err()
        );
        assert!(
            prefix
                .verify(initial, entries.iter().rev().map(|entry| (*entry, 100)))
                .is_err()
        );
        assert!(
            prefix
                .verify(
                    initial,
                    entries
                        .iter()
                        .chain(entries.last())
                        .map(|entry| (*entry, 100))
                )
                .is_err()
        );
        assert!(
            prefix
                .verify(initial, entries.iter().map(|entry| (*entry, 99)))
                .is_err()
        );
        assert!(
            prefix
                .advance(&entry(4, ApplicationExportClassV1::Entity), 100)
                .is_err()
        );
        assert_eq!(
            initial.pages(),
            0,
            "private successors never mutate published evidence"
        );
    }

    // req: EXP-006, EXP-007, EXP-009
    #[test]
    fn export_ledger_commits_every_entry_member_and_refuses_unequal_retries() {
        let initial =
            ApplicationExportLedgerPrefixV1::genesis(operation(1), b"immutable-binding").unwrap();
        let first = entry(1, ApplicationExportClassV1::Entity);
        let prefix = initial.advance(&first, 100).unwrap();
        assert_eq!(initial.advance(&first, 100).unwrap(), prefix);
        assert!(
            prefix.advance(&first, 100).is_err(),
            "repository must reconcile a retry before advancing"
        );
        for altered in [
            ApplicationExportPageCommitmentV1 {
                operation: operation(2),
                ..first
            },
            ApplicationExportPageCommitmentV1 {
                ordinal: ApplicationExportPageOrdinalV1::new(2).unwrap(),
                ..first
            },
            ApplicationExportPageCommitmentV1 {
                class: ApplicationExportClassV1::Event,
                ..first
            },
            ApplicationExportPageCommitmentV1 {
                page_hash: ApplicationExportPageHash::from_bytes([9; 32]),
                ..first
            },
            ApplicationExportPageCommitmentV1 { rows: 3, ..first },
            ApplicationExportPageCommitmentV1 { bytes: 41, ..first },
        ] {
            assert_ne!(first.canonical_bytes(), altered.canonical_bytes());
            assert!(prefix.verify(initial, [(altered, 100)]).is_err());
        }
        assert!(!format!("{first:?} {prefix:?}").contains("immutable-binding"));
    }

    // req: EXP-006, EXP-007, EXP-009
    #[test]
    fn export_ledger_canonical_members_reject_malformed_keys_and_framing() {
        let page = entry(1, ApplicationExportClassV1::Entity);
        let encoded = page.canonical_bytes();
        assert_eq!(
            ApplicationExportPageCommitmentV1::from_canonical_bytes(&encoded).unwrap(),
            page
        );
        assert!(page.validate_key(&page.canonical_key()).is_ok());
        assert!(
            page.validate_key(&entry(2, ApplicationExportClassV1::Entity).canonical_key())
                .is_err()
        );
        assert!(
            page.canonical_key() < entry(256, ApplicationExportClassV1::Entity).canonical_key()
        );
        for length in 0..encoded.len() {
            assert!(
                ApplicationExportPageCommitmentV1::from_canonical_bytes(&encoded[..length])
                    .is_err()
            );
        }
        let mut extra = encoded.to_vec();
        extra.push(0);
        assert!(ApplicationExportPageCommitmentV1::from_canonical_bytes(&extra).is_err());
        for (start, end, replacement) in [
            (0, 1, 0),
            (4, 5, 2),
            (5, 21, 0),
            (21, 23, 0),
            (21, 23, 255),
            (23, 24, 0),
            (56, 64, 255),
            (64, 72, 255),
        ] {
            let mut invalid = encoded;
            invalid[start..end].fill(replacement);
            assert!(ApplicationExportPageCommitmentV1::from_canonical_bytes(&invalid).is_err());
        }
    }

    // req: EXP-008, EXP-010
    #[test]
    fn export_ledger_preserves_page_byte_and_terminal_headroom_bounds() {
        let initial =
            ApplicationExportLedgerPrefixV1::genesis(operation(1), b"immutable-binding").unwrap();
        for invalid in [0, 4_097, u64::MAX] {
            assert!(ApplicationExportPageOrdinalV1::new(invalid).is_err());
        }
        assert!(
            ApplicationExportPageCommitmentV1::new(
                operation(1),
                ApplicationExportPageOrdinalV1::new(1).unwrap(),
                ApplicationExportClassV1::Entity,
                ApplicationExportPageHash::from_bytes([0; 32]),
                501,
                4_000
            )
            .is_err()
        );
        let empty = ApplicationExportPageCommitmentV1::new(
            operation(1),
            ApplicationExportPageOrdinalV1::new(1).unwrap(),
            ApplicationExportClassV1::Entity,
            ApplicationExportPageHash::from_bytes([0; 32]),
            0,
            0,
        )
        .unwrap();
        assert_eq!(initial.advance(&empty, 100).unwrap().pages(), 1);
        let prefix = initial
            .advance(&entry(1, ApplicationExportClassV1::Entity), 100)
            .unwrap();
        assert!(
            prefix
                .check_budget(200, MAX_APPLICATION_EXPORT_STATE_BYTES - 300)
                .is_ok()
        );
        assert!(
            prefix
                .check_budget(200, MAX_APPLICATION_EXPORT_STATE_BYTES - 299)
                .is_err()
        );
        assert!(prefix.check_budget(usize::MAX, 1).is_err());
        assert!(
            initial
                .advance(&empty, MAX_APPLICATION_EXPORT_STATE_BYTES + 1)
                .is_err()
        );
        let mut full = initial;
        for ordinal in 1..=4_096 {
            full = full
                .advance(&entry(ordinal, ApplicationExportClassV1::Entity), 1)
                .unwrap();
        }
        assert_eq!(full.pages(), 4_096);
        assert!(
            full.advance(&entry(4_096, ApplicationExportClassV1::Entity), 1)
                .is_err()
        );
    }

    // req: EXP-006, EXP-007, EXP-008, EXP-009
    #[test]
    fn export_ledger_prefix_decoder_requires_bounded_reciprocal_totals() {
        let genesis = ApplicationExportLedgerPrefixV1::genesis(operation(1), b"binding").unwrap();
        let first = entry(1, ApplicationExportClassV1::Entity);
        let prefix = genesis.advance(&first, 100).unwrap();
        for expected in [genesis, prefix] {
            let encoded = expected.canonical_bytes().unwrap();
            assert_eq!(
                ApplicationExportLedgerPrefixV1::from_canonical_bytes(&encoded).unwrap(),
                expected
            );
            for end in 0..encoded.len() {
                assert!(
                    ApplicationExportLedgerPrefixV1::from_canonical_bytes(&encoded[..end]).is_err()
                );
            }
            let mut trailing = encoded.to_vec();
            trailing.push(0);
            assert!(ApplicationExportLedgerPrefixV1::from_canonical_bytes(&trailing).is_err());
        }
        for (start, end, replacement) in [
            (0, 1, 0),       // magic
            (4, 5, 2),       // version
            (5, 21, 0),      // invalid UUID
            (85, 87, 255),   // total page ceiling
            (87, 89, 0),     // class pages disagree with total
            (95, 103, 255),  // row ceiling
            (95, 103, 0),    // zero rows but nonzero content bytes
            (127, 135, 255), // content ceiling
            (159, 163, 255), // retained-byte ceiling
            (159, 163, 0),   // claimed entry without retained bytes
        ] {
            let mut invalid = prefix.canonical_bytes().unwrap();
            invalid[start..end].fill(replacement);
            assert!(ApplicationExportLedgerPrefixV1::from_canonical_bytes(&invalid).is_err());
        }
        let mut invalid_genesis = genesis.canonical_bytes().unwrap();
        invalid_genesis[53] ^= 1;
        assert!(ApplicationExportLedgerPrefixV1::from_canonical_bytes(&invalid_genesis).is_err());

        // Well-shaped substituted commitments still require the independent
        // full ledger proof; decoding cannot turn claimed evidence into truth.
        let mut substituted = prefix.canonical_bytes().unwrap();
        substituted[53] ^= 1;
        let substituted =
            ApplicationExportLedgerPrefixV1::from_canonical_bytes(&substituted).unwrap();
        assert!(substituted.verify(genesis, [(first, 100)]).is_err());
        assert!(prefix.verify(genesis, [(first, 99)]).is_err());
    }

    // req: EXP-006, EXP-008
    #[test]
    fn export_ledger_per_page_hash_work_is_independent_of_prefix_length() {
        let genesis =
            ApplicationExportLedgerPrefixV1::genesis(operation(1), b"exact-operation-binding")
                .unwrap();
        let mut prefix = genesis;
        let mut entries = Vec::new();
        for ordinal in 1..=1_024 {
            let page = entry(ordinal, ApplicationExportClassV1::Entity);
            assert_eq!(page.canonical_bytes().len(), 72);
            prefix = prefix.advance(&page, 100).unwrap();
            entries.push((page, 100));
            if matches!(ordinal, 16 | 128 | 1_024) {
                assert_eq!(prefix.retained_bytes(), ordinal as usize * 100);
                assert_eq!(
                    prefix
                        .verify(genesis, entries.iter().copied())
                        .unwrap()
                        .len(),
                    ordinal as usize
                );
                prefix
                    .check_budget(1_024, ordinal as usize * 70 + 2_048)
                    .unwrap();
            }
        }
    }
}
