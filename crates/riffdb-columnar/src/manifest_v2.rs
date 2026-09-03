//! Independent V2 candidate-manifest codec.
//!
//! This format is deliberately disconnected from [`crate::ColumnarEngine`]
//! publication. WP-711 owns disjoint rebuild and atomic activation.

use std::collections::BTreeSet;

use riffdb_types::{CommitSequence, FrontierPosition, ProjectionGeneration};

use crate::checkpoint::checksum_bytes;
use crate::{
    COLUMNAR_ENCODING_REGISTRY_VERSION_V1, COLUMNAR_MANIFEST_FORMAT_VERSION_V2,
    COLUMNAR_SEGMENT_FORMAT_VERSION_V2, DefinitionFingerprint, MAX_SEGMENT_V2_BYTES,
    MAX_SEGMENT_V2_ROWS, OrgKey, SegmentV2Error, SegmentV2SegmentId,
};

/// Maximum segment entries in one partition-scoped V2 candidate manifest.
pub const MAX_COLUMNAR_MANIFEST_V2_SEGMENTS: usize = 4_096;
/// Maximum complete encoded candidate-manifest bytes.
pub const MAX_COLUMNAR_MANIFEST_V2_BYTES: usize = 4 * 1024 * 1024;

const MAGIC: &[u8; 8] = b"RDBCMV2\0";
const CHECKSUM_BYTES: usize = 32;
const MAX_ORG_KEY_BYTES: usize = 1024 * 1024;

/// One immutable segment member of a V2 candidate generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnarManifestV2Entry {
    segment_id: SegmentV2SegmentId,
    frontier_start: FrontierPosition,
    frontier_end: FrontierPosition,
    row_count: usize,
    file_length: usize,
    checksum: [u8; 32],
}

impl ColumnarManifestV2Entry {
    /// Validates and constructs one exact immutable segment member.
    pub fn new(
        segment_id: SegmentV2SegmentId,
        frontier_start: FrontierPosition,
        frontier_end: FrontierPosition,
        row_count: usize,
        file_length: usize,
        checksum: [u8; 32],
    ) -> Result<Self, SegmentV2Error> {
        if row_count == 0 || row_count > MAX_SEGMENT_V2_ROWS {
            return Err(SegmentV2Error::BoundExceeded("manifest segment rows"));
        }
        if file_length == 0 || file_length > MAX_SEGMENT_V2_BYTES {
            return Err(SegmentV2Error::BoundExceeded("manifest segment bytes"));
        }
        if frontier_ordinal(frontier_start) > frontier_ordinal(frontier_end) {
            return Err(SegmentV2Error::Invalid("manifest frontier interval"));
        }
        Ok(Self {
            segment_id,
            frontier_start,
            frontier_end,
            row_count,
            file_length,
            checksum,
        })
    }

    /// Returns the exact segment identity.
    #[must_use]
    pub const fn segment_id(&self) -> SegmentV2SegmentId {
        self.segment_id
    }

    /// Returns the first represented authoritative position.
    #[must_use]
    pub const fn frontier_start(&self) -> FrontierPosition {
        self.frontier_start
    }

    /// Returns the last represented authoritative position.
    #[must_use]
    pub const fn frontier_end(&self) -> FrontierPosition {
        self.frontier_end
    }

    /// Returns the exact segment row count.
    #[must_use]
    pub const fn row_count(&self) -> usize {
        self.row_count
    }

    /// Returns the complete segment file length.
    #[must_use]
    pub const fn file_length(&self) -> usize {
        self.file_length
    }

    /// Returns the complete segment checksum.
    #[must_use]
    pub const fn checksum(&self) -> &[u8; 32] {
        &self.checksum
    }

    /// Returns the only canonical relative file name for this identity.
    #[must_use]
    pub fn file_name(&self) -> String {
        let mut name = String::with_capacity(11 + 32 + 4);
        name.push_str("seg-v2-");
        for byte in self.segment_id.as_bytes() {
            use std::fmt::Write as _;
            let _ = write!(name, "{byte:02x}");
        }
        name.push_str(".col");
        name
    }
}

/// Complete partition-scoped manifest for one disjoint V2 candidate generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ColumnarManifestV2 {
    definition_fingerprint: DefinitionFingerprint,
    history_incarnation: u64,
    generation: ProjectionGeneration,
    organization: OrgKey,
    durable_frontier: FrontierPosition,
    segments: Vec<ColumnarManifestV2Entry>,
}

impl ColumnarManifestV2 {
    /// Validates and constructs a canonical candidate manifest.
    pub fn new(
        definition_fingerprint: DefinitionFingerprint,
        history_incarnation: u64,
        generation: ProjectionGeneration,
        organization: OrgKey,
        durable_frontier: FrontierPosition,
        mut segments: Vec<ColumnarManifestV2Entry>,
    ) -> Result<Self, SegmentV2Error> {
        if history_incarnation == 0 {
            return Err(SegmentV2Error::Invalid("manifest history incarnation"));
        }
        if organization.as_bytes().len() > MAX_ORG_KEY_BYTES {
            return Err(SegmentV2Error::BoundExceeded("manifest organization bytes"));
        }
        if segments.is_empty() || segments.len() > MAX_COLUMNAR_MANIFEST_V2_SEGMENTS {
            return Err(SegmentV2Error::BoundExceeded("manifest segments"));
        }
        if segments
            .iter()
            .any(|entry| frontier_ordinal(entry.frontier_end) > frontier_ordinal(durable_frontier))
        {
            return Err(SegmentV2Error::Invalid("segment exceeds manifest frontier"));
        }
        segments.sort_by_key(ColumnarManifestV2Entry::segment_id);
        if segments
            .windows(2)
            .any(|pair| pair[0].segment_id == pair[1].segment_id)
        {
            return Err(SegmentV2Error::Invalid("duplicate manifest segment"));
        }
        Ok(Self {
            definition_fingerprint,
            history_incarnation,
            generation,
            organization,
            durable_frontier,
            segments,
        })
    }

    /// Returns the exact definition fingerprint.
    #[must_use]
    pub const fn definition_fingerprint(&self) -> DefinitionFingerprint {
        self.definition_fingerprint
    }

    /// Returns the authoritative history incarnation.
    #[must_use]
    pub const fn history_incarnation(&self) -> u64 {
        self.history_incarnation
    }

    /// Returns the disjoint projection generation.
    #[must_use]
    pub const fn generation(&self) -> ProjectionGeneration {
        self.generation
    }

    /// Returns the exact organization partition.
    #[must_use]
    pub const fn organization(&self) -> &OrgKey {
        &self.organization
    }

    /// Returns the exact recoverable frontier.
    #[must_use]
    pub const fn durable_frontier(&self) -> FrontierPosition {
        self.durable_frontier
    }

    /// Returns segment members in canonical identity order.
    #[must_use]
    pub fn segments(&self) -> &[ColumnarManifestV2Entry] {
        &self.segments
    }

    /// Encodes stable candidate-manifest bytes with a complete checksum.
    pub fn encode(&self) -> Result<Vec<u8>, SegmentV2Error> {
        let fixed = MAGIC.len() + 2 + 2 + 2 + 2 + 8 + 32 + 8 + 8 + 4 + 9 + 4;
        let org = self.organization.as_bytes().len();
        let entries = self
            .segments
            .len()
            .checked_mul(16 + 9 + 9 + 4 + 8 + 32)
            .ok_or(SegmentV2Error::BoundExceeded("manifest bytes"))?;
        let complete_length = fixed
            .checked_add(org)
            .and_then(|length| length.checked_add(entries))
            .and_then(|length| length.checked_add(CHECKSUM_BYTES))
            .ok_or(SegmentV2Error::BoundExceeded("manifest bytes"))?;
        if complete_length > MAX_COLUMNAR_MANIFEST_V2_BYTES {
            return Err(SegmentV2Error::BoundExceeded("manifest bytes"));
        }
        let mut out = Vec::with_capacity(complete_length);
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&COLUMNAR_MANIFEST_FORMAT_VERSION_V2.to_be_bytes());
        out.extend_from_slice(&COLUMNAR_SEGMENT_FORMAT_VERSION_V2.to_be_bytes());
        out.extend_from_slice(&COLUMNAR_ENCODING_REGISTRY_VERSION_V1.to_be_bytes());
        out.extend_from_slice(&0u16.to_be_bytes());
        put_u64(&mut out, complete_length, "manifest complete length")?;
        out.extend_from_slice(self.definition_fingerprint.as_bytes());
        out.extend_from_slice(&self.history_incarnation.to_be_bytes());
        out.extend_from_slice(&self.generation.to_be_bytes());
        put_bytes(
            &mut out,
            self.organization.as_bytes(),
            "manifest organization",
        )?;
        encode_frontier(&mut out, self.durable_frontier);
        put_u32(&mut out, self.segments.len(), "manifest segments")?;
        for entry in &self.segments {
            out.extend_from_slice(entry.segment_id.as_bytes());
            encode_frontier(&mut out, entry.frontier_start);
            encode_frontier(&mut out, entry.frontier_end);
            put_u32(&mut out, entry.row_count, "manifest segment rows")?;
            put_u64(&mut out, entry.file_length, "manifest segment bytes")?;
            out.extend_from_slice(&entry.checksum);
        }
        let checksum = checksum_bytes(&out);
        out.extend_from_slice(&checksum);
        if out.len() != complete_length {
            return Err(SegmentV2Error::Invalid("manifest length accounting"));
        }
        Ok(out)
    }

    /// Decodes a canonical, bounded, completely checksummed candidate manifest.
    pub fn decode(bytes: &[u8]) -> Result<Self, SegmentV2Error> {
        if bytes.len() > MAX_COLUMNAR_MANIFEST_V2_BYTES {
            return Err(SegmentV2Error::BoundExceeded("manifest bytes"));
        }
        if bytes.len() < CHECKSUM_BYTES {
            return Err(SegmentV2Error::Corrupt("truncated manifest checksum"));
        }
        let body_length = bytes.len() - CHECKSUM_BYTES;
        let (body, checksum) = bytes.split_at(body_length);
        if checksum != checksum_bytes(body) {
            return Err(SegmentV2Error::ChecksumMismatch("manifest"));
        }
        let mut reader = Reader::new(body);
        if reader.read_exact(MAGIC.len())? != MAGIC {
            return Err(SegmentV2Error::Corrupt("manifest magic"));
        }
        if reader.read_u16()? != COLUMNAR_MANIFEST_FORMAT_VERSION_V2
            || reader.read_u16()? != COLUMNAR_SEGMENT_FORMAT_VERSION_V2
            || reader.read_u16()? != COLUMNAR_ENCODING_REGISTRY_VERSION_V1
            || reader.read_u16()? != 0
        {
            return Err(SegmentV2Error::Corrupt("manifest format identity"));
        }
        if reader.read_usize()? != bytes.len() {
            return Err(SegmentV2Error::Corrupt("manifest complete length"));
        }
        let mut fingerprint = [0u8; 32];
        fingerprint.copy_from_slice(reader.read_exact(32)?);
        let history_incarnation = reader.read_u64()?;
        let generation = ProjectionGeneration::new(reader.read_u64()?)
            .ok_or(SegmentV2Error::Corrupt("manifest generation"))?;
        let organization = OrgKey::from_encoded_bytes(reader.read_bytes(MAX_ORG_KEY_BYTES)?);
        let durable_frontier = reader.read_frontier()?;
        let segment_count =
            reader.read_count(MAX_COLUMNAR_MANIFEST_V2_SEGMENTS, "manifest segments")?;
        if segment_count == 0 {
            return Err(SegmentV2Error::Corrupt("empty manifest"));
        }
        let mut segments = Vec::with_capacity(segment_count);
        let mut seen = BTreeSet::new();
        for _ in 0..segment_count {
            let mut segment_id = [0u8; 16];
            segment_id.copy_from_slice(reader.read_exact(16)?);
            let segment_id = SegmentV2SegmentId::from_bytes(segment_id);
            if !seen.insert(segment_id) {
                return Err(SegmentV2Error::Corrupt("duplicate manifest segment"));
            }
            let frontier_start = reader.read_frontier()?;
            let frontier_end = reader.read_frontier()?;
            let row_count = reader.read_count(MAX_SEGMENT_V2_ROWS, "manifest segment rows")?;
            let file_length = reader.read_usize()?;
            let mut checksum = [0u8; 32];
            checksum.copy_from_slice(reader.read_exact(32)?);
            segments.push(
                ColumnarManifestV2Entry::new(
                    segment_id,
                    frontier_start,
                    frontier_end,
                    row_count,
                    file_length,
                    checksum,
                )
                .map_err(|_| SegmentV2Error::Corrupt("manifest segment entry"))?,
            );
        }
        if !reader.remaining().is_empty() {
            return Err(SegmentV2Error::Corrupt("trailing manifest bytes"));
        }
        if segments
            .windows(2)
            .any(|pair| pair[0].segment_id >= pair[1].segment_id)
        {
            return Err(SegmentV2Error::Corrupt("manifest segment ordering"));
        }
        Self::new(
            DefinitionFingerprint::from_bytes(fingerprint),
            history_incarnation,
            generation,
            organization,
            durable_frontier,
            segments,
        )
        .map_err(|_| SegmentV2Error::Corrupt("manifest semantics"))
    }
}

fn frontier_ordinal(frontier: FrontierPosition) -> u64 {
    match frontier {
        FrontierPosition::BeforeFirst => 0,
        FrontierPosition::AppliedThrough(sequence) => sequence.get(),
    }
}

fn encode_frontier(out: &mut Vec<u8>, frontier: FrontierPosition) {
    match frontier {
        FrontierPosition::BeforeFirst => {
            out.push(0);
            out.extend_from_slice(&0u64.to_be_bytes());
        }
        FrontierPosition::AppliedThrough(sequence) => {
            out.push(1);
            out.extend_from_slice(&sequence.get().to_be_bytes());
        }
    }
}

fn put_u32(out: &mut Vec<u8>, value: usize, class: &'static str) -> Result<(), SegmentV2Error> {
    let value = u32::try_from(value).map_err(|_| SegmentV2Error::BoundExceeded(class))?;
    out.extend_from_slice(&value.to_be_bytes());
    Ok(())
}

fn put_u64(out: &mut Vec<u8>, value: usize, class: &'static str) -> Result<(), SegmentV2Error> {
    let value = u64::try_from(value).map_err(|_| SegmentV2Error::BoundExceeded(class))?;
    out.extend_from_slice(&value.to_be_bytes());
    Ok(())
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8], class: &'static str) -> Result<(), SegmentV2Error> {
    put_u32(out, bytes.len(), class)?;
    out.extend_from_slice(bytes);
    Ok(())
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn remaining(&self) -> &'a [u8] {
        &self.bytes[self.offset..]
    }

    fn read_exact(&mut self, length: usize) -> Result<&'a [u8], SegmentV2Error> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(SegmentV2Error::Corrupt("manifest offset overflow"))?;
        let bytes = self
            .bytes
            .get(self.offset..end)
            .ok_or(SegmentV2Error::Corrupt("truncated manifest"))?;
        self.offset = end;
        Ok(bytes)
    }

    fn read_u8(&mut self) -> Result<u8, SegmentV2Error> {
        self.read_exact(1)?
            .first()
            .copied()
            .ok_or(SegmentV2Error::Corrupt("manifest byte"))
    }

    fn read_u16(&mut self) -> Result<u16, SegmentV2Error> {
        let bytes: [u8; 2] = self
            .read_exact(2)?
            .try_into()
            .map_err(|_| SegmentV2Error::Corrupt("manifest u16"))?;
        Ok(u16::from_be_bytes(bytes))
    }

    fn read_u32(&mut self) -> Result<u32, SegmentV2Error> {
        let bytes: [u8; 4] = self
            .read_exact(4)?
            .try_into()
            .map_err(|_| SegmentV2Error::Corrupt("manifest u32"))?;
        Ok(u32::from_be_bytes(bytes))
    }

    fn read_u64(&mut self) -> Result<u64, SegmentV2Error> {
        let bytes: [u8; 8] = self
            .read_exact(8)?
            .try_into()
            .map_err(|_| SegmentV2Error::Corrupt("manifest u64"))?;
        Ok(u64::from_be_bytes(bytes))
    }

    fn read_usize(&mut self) -> Result<usize, SegmentV2Error> {
        usize::try_from(self.read_u64()?)
            .map_err(|_| SegmentV2Error::Corrupt("manifest usize range"))
    }

    fn read_count(&mut self, maximum: usize, class: &'static str) -> Result<usize, SegmentV2Error> {
        let value =
            usize::try_from(self.read_u32()?).map_err(|_| SegmentV2Error::BoundExceeded(class))?;
        if value > maximum {
            return Err(SegmentV2Error::BoundExceeded(class));
        }
        Ok(value)
    }

    fn read_bytes(&mut self, maximum: usize) -> Result<Vec<u8>, SegmentV2Error> {
        let length = self.read_count(maximum, "manifest length-prefixed bytes")?;
        Ok(self.read_exact(length)?.to_vec())
    }

    fn read_frontier(&mut self) -> Result<FrontierPosition, SegmentV2Error> {
        let tag = self.read_u8()?;
        let value = self.read_u64()?;
        match (tag, value) {
            (0, 0) => Ok(FrontierPosition::BeforeFirst),
            (1, value) => CommitSequence::new(value)
                .map(FrontierPosition::AppliedThrough)
                .ok_or(SegmentV2Error::Corrupt("manifest frontier")),
            _ => Err(SegmentV2Error::Corrupt("manifest frontier")),
        }
    }
}
