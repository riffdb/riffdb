//! Inner payload of the accepted capsule V7 evidence field. Its enclosing
//! capsule/segment owns version identity, graph binding and integrity protection.
//! This payload is never a standalone stored record, receipt or source position.

use riffdb_types::DualFrontier;

use crate::{
    ChangelogV3Error, CommandPrefixEvidenceV1, MAX_CHANGELOG_FRAME_ENTRIES, MAX_STAGED_WRITE_BYTES,
    changelog_v3::{ReceiptReader, decode_mutation, encode_mutation},
};

const HEADER_BYTES: usize = 40;

impl CommandPrefixEvidenceV1 {
    /// Encodes the bounded inner capsule payload using the existing canonical
    /// mutation representation. The enclosing record must additionally charge
    /// its complete encoded graph; this payload alone is not durable authority.
    pub fn encode_capsule_payload(&self) -> Result<Vec<u8>, ChangelogV3Error> {
        let mut bytes = Vec::with_capacity(self.semantic_bytes());
        bytes.extend_from_slice(&self.predecessor().to_canonical_bytes());
        bytes.extend_from_slice(&self.covered().to_canonical_bytes());
        bytes.extend_from_slice(
            &u32::try_from(self.mutations().len())
                .map_err(|_| ChangelogV3Error::LimitExceeded)?
                .to_be_bytes(),
        );
        for mutation in self.mutations() {
            encode_mutation(&mut bytes, mutation)?;
        }
        Ok(bytes)
    }

    /// Decodes only the inner field of a separately validated V7 capsule.
    /// Checks the byte ceiling and count against available bytes before allocating
    /// mutation storage; refuses trailing, unknown and noncanonical content.
    /// The caller must still join this evidence to its enclosing command graph.
    pub fn decode_capsule_payload(bytes: &[u8]) -> Result<Self, ChangelogV3Error> {
        if bytes.len() > MAX_STAGED_WRITE_BYTES {
            return Err(ChangelogV3Error::LimitExceeded);
        }
        if bytes.len() < HEADER_BYTES {
            return Err(ChangelogV3Error::InvalidEncoding);
        }
        let mut reader = ReceiptReader { remaining: bytes };
        let predecessor = DualFrontier::from_canonical_bytes(reader.array()?)
            .map_err(|_| ChangelogV3Error::InvalidEncoding)?;
        let covered = DualFrontier::from_canonical_bytes(reader.array()?)
            .map_err(|_| ChangelogV3Error::InvalidEncoding)?;
        let count = usize::try_from(reader.u32()?).map_err(|_| ChangelogV3Error::LimitExceeded)?;
        if count > MAX_CHANGELOG_FRAME_ENTRIES {
            return Err(ChangelogV3Error::LimitExceeded);
        }
        // A mutation has a 44-byte header and a nonempty key, even for a delete.
        if count > reader.remaining.len() / 45 {
            return Err(ChangelogV3Error::InvalidEncoding);
        }
        let mut mutations = Vec::with_capacity(count);
        for _ in 0..count {
            mutations.push(decode_mutation(&mut reader)?);
        }
        if !reader.remaining.is_empty() {
            return Err(ChangelogV3Error::InvalidEncoding);
        }
        Self::new(predecessor, covered, mutations)
    }
}
