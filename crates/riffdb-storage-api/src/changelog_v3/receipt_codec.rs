use riffdb_types::{DatabaseId, DualFrontier};
use sha2::{Digest, Sha256};

use super::frame::MAX_RECEIPT_BYTES;
use crate::{AuthoritativeStateCatalogV1, MAX_CHANGELOG_FRAME_ENTRIES};

use super::{
    AuthoritativeMutationV3, AuthoritativeTransactionBindingV3, AuthoritativeTransactionV3,
    ChangelogAttributionV3, ChangelogTransactionSequence, ChangelogV3Error,
};

const MAGIC: &[u8; 8] = b"RDBCTX03";
const HEADER_BYTES: usize = 128;
const CHECKSUM_BYTES: usize = 32;
const DELETE_VALUE_LENGTH: u32 = u32::MAX;

#[cfg(test)]
mod streaming_hash_tests {
    use super::*;

    fn frozen_frame() -> crate::ChangelogFrameV3 {
        let hex = include_str!("../../../../fixtures/replication/changelog-frame-v3.hex").trim();
        let bytes: Vec<u8> = hex
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect();
        crate::ChangelogFrameV3::decode(&bytes).unwrap()
    }

    // req: REP-003, REC-001
    #[test]
    fn streamed_receipt_body_preserves_frozen_bytes_and_checksum() {
        let frame = frozen_frame();
        for receipt in frame.receipts() {
            let encoded = receipt.encode().unwrap();
            let mut streamed = Vec::new();
            receipt
                .write_body(encoded.len(), &mut |part| streamed.extend_from_slice(part))
                .unwrap();
            assert_eq!(streamed, encoded[..encoded.len() - CHECKSUM_BYTES]);
            let expected: [u8; 32] = Sha256::digest(&streamed).into();
            assert_eq!(receipt.history_hash().unwrap(), expected);
            assert_eq!(&expected, &encoded[encoded.len() - CHECKSUM_BYTES..]);
        }
    }

    // req: REP-003, REC-001
    #[test]
    fn streamed_history_hash_preserves_empty_put_delete_and_sha_block_boundaries() {
        let frame = frozen_frame();
        let binding = frame.receipts()[0].binding();
        let empty = AuthoritativeTransactionV3::new(
            binding,
            ChangelogAttributionV3::CatalogAdministration,
            vec![],
        )
        .unwrap();
        let check = |receipt: &AuthoritativeTransactionV3| {
            let encoded = receipt.encode().unwrap();
            let body = &encoded[..encoded.len() - CHECKSUM_BYTES];
            let expected: [u8; 32] = Sha256::digest(body).into();
            assert_eq!(receipt.history_hash().unwrap(), expected);
            assert_eq!(&expected, &encoded[encoded.len() - CHECKSUM_BYTES..]);
            assert_eq!(
                AuthoritativeTransactionV3::decode(&encoded).unwrap(),
                *receipt
            );
        };
        check(&empty);
        for size in [0, 1, 31, 55, 56, 63, 64, 65, 127, 128, 255, 4096, 65536] {
            let value = vec![0x31; size];
            let mutations = vec![
                AuthoritativeMutationV3::put(
                    crate::AuthoritativeNamespaceV1::Entities,
                    b"a",
                    None,
                    &value,
                )
                .unwrap(),
                AuthoritativeMutationV3::put(
                    crate::AuthoritativeNamespaceV1::Entities,
                    b"b",
                    Some([0; 32]),
                    b"",
                )
                .unwrap(),
                AuthoritativeMutationV3::delete(
                    crate::AuthoritativeNamespaceV1::Entities,
                    b"c",
                    [0x71; 32],
                )
                .unwrap(),
            ];
            let receipt = AuthoritativeTransactionV3::new(
                binding,
                ChangelogAttributionV3::CatalogAdministration,
                mutations,
            )
            .unwrap();
            check(&receipt);
        }
    }
}

impl AuthoritativeTransactionV3 {
    /// Canonical receipt encoding. A single SHA-256 covers the full fixed header
    /// and mutation payload; its exact bytes are shared by journal sourcing and
    /// checkpoint materialization, not reconstructed from current row values.
    pub fn encode(&self) -> Result<Vec<u8>, ChangelogV3Error> {
        let total = self.encoded_len()?;
        if total > MAX_RECEIPT_BYTES {
            return Err(ChangelogV3Error::LimitExceeded);
        }
        let mut bytes = Vec::with_capacity(total);
        self.write_body(total, &mut |part| bytes.extend_from_slice(part))?;
        let checksum: [u8; 32] = Sha256::digest(&bytes).into();
        bytes.extend_from_slice(&checksum);
        Ok(bytes)
    }

    // One canonical emitter serves export encoding and checksum-only callers.
    // Borrowed fragments remain within this synchronous invocation.
    fn write_body(
        &self,
        total: usize,
        write: &mut impl FnMut(&[u8]),
    ) -> Result<(), ChangelogV3Error> {
        if total > MAX_RECEIPT_BYTES {
            return Err(ChangelogV3Error::LimitExceeded);
        }
        let binding = self.binding();
        write(MAGIC);
        write(&3_u16.to_be_bytes());
        write(binding.database_id.as_bytes());
        write(&binding.history_incarnation.to_be_bytes());
        write(&binding.predecessor.map_or(0, |v| v.get()).to_be_bytes());
        write(&binding.sequence.get().to_be_bytes());
        write(&binding.predecessor_frontier.to_canonical_bytes());
        write(&binding.covered_frontier.to_canonical_bytes());
        write(&(self.attribution() as u16).to_be_bytes());
        write(&binding.prior_history_hash);
        write(
            &u32::try_from(self.mutations().len())
                .map_err(|_| ChangelogV3Error::LimitExceeded)?
                .to_be_bytes(),
        );
        let payload_bytes = total - HEADER_BYTES - CHECKSUM_BYTES;
        write(
            &u32::try_from(payload_bytes)
                .map_err(|_| ChangelogV3Error::LimitExceeded)?
                .to_be_bytes(),
        );
        for mutation in self.mutations() {
            write_mutation(write, mutation)?;
        }
        Ok(())
    }

    /// Refuses oversized input before hashing/copying and refuses unknown,
    /// duplicate, reordered, truncated, trailing, or noncanonical content.
    pub fn decode(bytes: &[u8]) -> Result<Self, ChangelogV3Error> {
        if bytes.len() > MAX_RECEIPT_BYTES {
            return Err(ChangelogV3Error::LimitExceeded);
        }
        if bytes.len() < HEADER_BYTES + CHECKSUM_BYTES {
            return Err(ChangelogV3Error::InvalidEncoding);
        }
        let payload_end = bytes.len() - CHECKSUM_BYTES;
        let checksum: [u8; 32] = Sha256::digest(&bytes[..payload_end]).into();
        if bytes[payload_end..] != checksum {
            return Err(ChangelogV3Error::InvalidEncoding);
        }
        let mut reader = ReceiptReader {
            remaining: &bytes[..payload_end],
        };
        if reader.take(8)? != MAGIC || reader.u16()? != 3 {
            return Err(ChangelogV3Error::InvalidEncoding);
        }
        let database_id = DatabaseId::from_bytes(reader.array()?)
            .map_err(|_| ChangelogV3Error::InvalidEncoding)?;
        let history_incarnation = reader.u64()?;
        let predecessor = ChangelogTransactionSequence::new(reader.u64()?);
        let sequence = ChangelogTransactionSequence::new(reader.u64()?)
            .ok_or(ChangelogV3Error::InvalidEncoding)?;
        let predecessor_frontier = DualFrontier::from_canonical_bytes(reader.array()?)
            .map_err(|_| ChangelogV3Error::InvalidEncoding)?;
        let covered_frontier = DualFrontier::from_canonical_bytes(reader.array()?)
            .map_err(|_| ChangelogV3Error::InvalidEncoding)?;
        let attribution = ChangelogAttributionV3::from_tag(reader.u16()?)
            .ok_or(ChangelogV3Error::InvalidEncoding)?;
        let prior_history_hash = reader.array()?;
        let count = usize::try_from(reader.u32()?).map_err(|_| ChangelogV3Error::LimitExceeded)?;
        let payload_len =
            usize::try_from(reader.u32()?).map_err(|_| ChangelogV3Error::LimitExceeded)?;
        if count > MAX_CHANGELOG_FRAME_ENTRIES {
            return Err(ChangelogV3Error::LimitExceeded);
        }
        if payload_len != reader.remaining.len() || count > payload_len / 45 {
            return Err(ChangelogV3Error::InvalidEncoding);
        }
        let mut mutations = Vec::with_capacity(count);
        for _ in 0..count {
            mutations.push(decode_mutation(&mut reader)?);
        }
        if !reader.remaining.is_empty() {
            return Err(ChangelogV3Error::InvalidEncoding);
        }
        Self::new(
            AuthoritativeTransactionBindingV3 {
                database_id,
                history_incarnation,
                predecessor,
                sequence,
                predecessor_frontier,
                covered_frontier,
                prior_history_hash,
            },
            attribution,
            mutations,
        )
    }

    /// Exact receipt checksum used as the next row's prior-history hash.
    pub fn history_hash(&self) -> Result<[u8; 32], ChangelogV3Error> {
        let mut digest = Sha256::new();
        self.write_body(self.encoded_len()?, &mut |part| digest.update(part))?;
        Ok(digest.finalize().into())
    }
}

pub(crate) fn encode_mutation(
    bytes: &mut Vec<u8>,
    mutation: &AuthoritativeMutationV3,
) -> Result<(), ChangelogV3Error> {
    write_mutation(&mut |part| bytes.extend_from_slice(part), mutation)
}

fn write_mutation(
    write: &mut impl FnMut(&[u8]),
    mutation: &AuthoritativeMutationV3,
) -> Result<(), ChangelogV3Error> {
    write(&mutation.namespace().tag().to_be_bytes());
    write(&[u8::from(mutation.value().is_none())]);
    write(&[u8::from(mutation.expected_hash().is_some())]);
    write(
        &u32::try_from(mutation.key().len())
            .map_err(|_| ChangelogV3Error::LimitExceeded)?
            .to_be_bytes(),
    );
    let value_len = mutation
        .value()
        .map(|value| u32::try_from(value.len()))
        .transpose()
        .map_err(|_| ChangelogV3Error::LimitExceeded)?
        .unwrap_or(DELETE_VALUE_LENGTH);
    write(&value_len.to_be_bytes());
    write(&mutation.expected_hash().unwrap_or([0; 32]));
    write(mutation.key());
    if let Some(value) = mutation.value() {
        write(value);
    }
    Ok(())
}

pub(crate) fn decode_mutation(
    reader: &mut ReceiptReader<'_>,
) -> Result<AuthoritativeMutationV3, ChangelogV3Error> {
    let namespace = AuthoritativeStateCatalogV1
        .by_tag(reader.u16()?)
        .ok_or(ChangelogV3Error::InvalidNamespace)?;
    let operation = reader.byte()?;
    let expected_present = reader.byte()?;
    let key_len = usize::try_from(reader.u32()?).map_err(|_| ChangelogV3Error::LimitExceeded)?;
    let value_len = reader.u32()?;
    let expected_hash: [u8; 32] = reader.array()?;
    let expected = match (expected_present, expected_hash) {
        (0, hash) if hash == [0; 32] => None,
        (1, hash) => Some(hash),
        _ => return Err(ChangelogV3Error::InvalidEncoding),
    };
    let key = reader.take(key_len)?;
    let mutation = match (operation, value_len, expected) {
        (0, len, expected) if len != DELETE_VALUE_LENGTH => {
            let value =
                reader.take(usize::try_from(len).map_err(|_| ChangelogV3Error::LimitExceeded)?)?;
            AuthoritativeMutationV3::put(namespace, key, expected, value)?
        }
        (1, DELETE_VALUE_LENGTH, Some(hash)) => {
            AuthoritativeMutationV3::delete(namespace, key, hash)?
        }
        _ => return Err(ChangelogV3Error::InvalidEncoding),
    };
    Ok(mutation)
}

pub(crate) struct ReceiptReader<'a> {
    pub(crate) remaining: &'a [u8],
}

impl<'a> ReceiptReader<'a> {
    pub(crate) fn take(&mut self, count: usize) -> Result<&'a [u8], ChangelogV3Error> {
        let (value, remaining) = self
            .remaining
            .split_at_checked(count)
            .ok_or(ChangelogV3Error::InvalidEncoding)?;
        self.remaining = remaining;
        Ok(value)
    }

    pub(crate) fn array<const N: usize>(&mut self) -> Result<[u8; N], ChangelogV3Error> {
        self.take(N)?
            .try_into()
            .map_err(|_| ChangelogV3Error::InvalidEncoding)
    }

    fn byte(&mut self) -> Result<u8, ChangelogV3Error> {
        Ok(self.array::<1>()?[0])
    }
    pub(crate) fn u16(&mut self) -> Result<u16, ChangelogV3Error> {
        Ok(u16::from_be_bytes(self.array()?))
    }
    pub(crate) fn u32(&mut self) -> Result<u32, ChangelogV3Error> {
        Ok(u32::from_be_bytes(self.array()?))
    }
    pub(crate) fn u64(&mut self) -> Result<u64, ChangelogV3Error> {
        Ok(u64::from_be_bytes(self.array()?))
    }
}
