//! Rebuildable bounded `long_pattern_v1` provider state (ADR-0174).

use std::collections::{BTreeMap, BTreeSet};

use riffdb_types::{
    CompiledLongPatternV1, EntityKeyHash, LongPatternBoundsV1, LongPatternProfileV1,
    long_pattern_digest_v1, long_pattern_grams_v1,
};

const MAGIC: &[u8] = b"RLPV\x01";
/// Maximum opaque release bytes retained per row.
pub const MAX_LONG_PATTERN_RELEASE_BYTES_PER_ROW_V1: usize = 64 * 1024;
/// Maximum provider checkpoint bytes.
pub const MAX_LONG_PATTERN_CHECKPOINT_BYTES_V1: usize = 256 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
struct LongPatternRowV1 {
    matched: String,
    digest: [u8; 32],
    grams: Vec<[u8; 3]>,
    release: Vec<u8>,
}

/// One exact verified provider result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LongPatternResultV1 {
    key: EntityKeyHash,
    release: Vec<u8>,
}

impl LongPatternResultV1 {
    /// Canonical entity-key digest.
    #[must_use]
    pub const fn key(&self) -> EntityKeyHash {
        self.key
    }
    /// Compiler-shaped release bytes or authoritative locator.
    #[must_use]
    pub fn release(&self) -> &[u8] {
        &self.release
    }
}

/// Canonical rebuildable provider partition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LongPatternPartitionV1 {
    profile: LongPatternProfileV1,
    bounds: LongPatternBoundsV1,
    rows: BTreeMap<EntityKeyHash, LongPatternRowV1>,
    postings: BTreeMap<[u8; 3], BTreeSet<EntityKeyHash>>,
    total_matched_bytes: u64,
}

impl LongPatternPartitionV1 {
    /// Creates an empty checked provider partition.
    #[must_use]
    pub fn new(profile: LongPatternProfileV1, bounds: LongPatternBoundsV1) -> Self {
        Self {
            profile,
            bounds,
            rows: BTreeMap::new(),
            postings: BTreeMap::new(),
            total_matched_bytes: 0,
        }
    }

    /// Frozen matching profile.
    #[must_use]
    pub const fn profile(&self) -> LongPatternProfileV1 {
        self.profile
    }
    /// Complete compiler-declared bounds.
    #[must_use]
    pub const fn bounds(&self) -> LongPatternBoundsV1 {
        self.bounds
    }
    /// Current retained row count.
    #[must_use]
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Atomically inserts or replaces one derived row.
    pub fn upsert(
        &mut self,
        key: EntityKeyHash,
        source: &str,
        release: Vec<u8>,
    ) -> Result<(), LongPatternProviderErrorV1> {
        if source.len() > self.bounds.source_bytes() as usize
            || release.len() > MAX_LONG_PATTERN_RELEASE_BYTES_PER_ROW_V1
        {
            return Err(LongPatternProviderErrorV1::RowBound);
        }
        let matched = self.profile.matched_form(source);
        let grams = long_pattern_grams_v1(&matched);
        if matched.len() > self.bounds.matched_bytes() as usize
            || grams.len() > self.bounds.grams_per_row() as usize
        {
            return Err(LongPatternProviderErrorV1::RowBound);
        }
        let row = LongPatternRowV1 {
            digest: long_pattern_digest_v1(&matched),
            matched,
            grams,
            release,
        };
        let mut candidate = self.rows.clone();
        candidate.insert(key, row);
        let (postings, total_matched_bytes) = rebuild(&candidate, self.bounds)?;
        self.rows = candidate;
        self.postings = postings;
        self.total_matched_bytes = total_matched_bytes;
        Ok(())
    }

    /// Atomically removes one derived row.
    pub fn remove(&mut self, key: EntityKeyHash) -> Result<bool, LongPatternProviderErrorV1> {
        let mut candidate = self.rows.clone();
        let removed = candidate.remove(&key).is_some();
        if removed {
            let (postings, total_matched_bytes) = rebuild(&candidate, self.bounds)?;
            self.rows = candidate;
            self.postings = postings;
            self.total_matched_bytes = total_matched_bytes;
        }
        Ok(removed)
    }

    /// Selects, then exactly verifies, one complete bounded candidate set.
    ///
    /// Negated operators require an explicit compiler-authorized positive
    /// universe. Rows outside that universe cannot affect membership or work.
    pub fn query(
        &self,
        pattern: &CompiledLongPatternV1,
        universe: Option<&[EntityKeyHash]>,
    ) -> Result<Vec<LongPatternResultV1>, LongPatternProviderErrorV1> {
        if pattern.profile() != self.profile
            && !(pattern.operator().is_case_insensitive()
                && self.profile == LongPatternProfileV1::UnicodeFoldV1)
        {
            return Err(LongPatternProviderErrorV1::ProfileMismatch);
        }
        let candidates = if pattern.operator().is_negated() {
            let universe = universe.ok_or(LongPatternProviderErrorV1::UniverseRequired)?;
            if universe.windows(2).any(|pair| pair[0] >= pair[1]) {
                return Err(LongPatternProviderErrorV1::UniverseInvalid);
            }
            universe.to_vec()
        } else if let Some((first, rest)) = pattern.mandatory_grams().split_first() {
            let mut selected = self.postings.get(first).cloned().unwrap_or_default();
            for gram in rest {
                let Some(posting) = self.postings.get(gram) else {
                    selected.clear();
                    break;
                };
                selected.retain(|key| posting.contains(key));
            }
            selected.into_iter().collect()
        } else {
            self.rows.keys().copied().collect()
        };
        if candidates.len() > self.bounds.candidates() as usize {
            return Err(LongPatternProviderErrorV1::CandidateBound);
        }
        let mut verification_bytes = 0_u64;
        let mut results = Vec::new();
        for key in candidates {
            let Some(row) = self.rows.get(&key) else {
                if pattern.operator().is_negated() {
                    return Err(LongPatternProviderErrorV1::UniverseInvalid);
                }
                continue;
            };
            verification_bytes = verification_bytes
                .checked_add(row.matched.len() as u64)
                .ok_or(LongPatternProviderErrorV1::VerificationBound)?;
            if verification_bytes > self.bounds.verification_bytes() {
                return Err(LongPatternProviderErrorV1::VerificationBound);
            }
            // Digest and postings are candidate filters only. Complete matched
            // bytes decide every positive and negative result.
            if pattern.matches_matched(&row.matched) {
                results.push(LongPatternResultV1 {
                    key,
                    release: row.release.clone(),
                });
                if results.len() > self.bounds.results() as usize {
                    return Err(LongPatternProviderErrorV1::ResultBound);
                }
            }
        }
        Ok(results)
    }

    /// Canonical byte-frozen checkpoint containing values, digests, grams and release data.
    pub fn checkpoint_bytes(&self) -> Result<Vec<u8>, LongPatternProviderErrorV1> {
        let mut output = Vec::new();
        output.extend_from_slice(MAGIC);
        output.push(self.profile as u8);
        put_bounds(&mut output, self.bounds);
        put_u32(&mut output, self.rows.len())?;
        for (key, row) in &self.rows {
            output.extend_from_slice(key.as_bytes());
            put_bytes(&mut output, row.matched.as_bytes())?;
            output.extend_from_slice(&row.digest);
            put_u32(&mut output, row.grams.len())?;
            for gram in &row.grams {
                output.extend_from_slice(gram);
            }
            put_bytes(&mut output, &row.release)?;
        }
        if output.len() > MAX_LONG_PATTERN_CHECKPOINT_BYTES_V1 {
            return Err(LongPatternProviderErrorV1::CheckpointBound);
        }
        Ok(output)
    }

    /// Decodes and fully revalidates one provider checkpoint before publication.
    pub fn from_checkpoint_bytes(bytes: &[u8]) -> Result<Self, LongPatternProviderErrorV1> {
        if bytes.len() > MAX_LONG_PATTERN_CHECKPOINT_BYTES_V1 || !bytes.starts_with(MAGIC) {
            return Err(LongPatternProviderErrorV1::CheckpointInvalid);
        }
        let mut reader = Reader::new(&bytes[MAGIC.len()..]);
        let profile = LongPatternProfileV1::from_discriminant(reader.u8()?)
            .ok_or(LongPatternProviderErrorV1::CheckpointInvalid)?;
        let bounds = read_bounds(&mut reader)?;
        let count = reader.u32()? as usize;
        if count > bounds.rows() as usize {
            return Err(LongPatternProviderErrorV1::CheckpointInvalid);
        }
        let mut rows = BTreeMap::new();
        for _ in 0..count {
            let key = EntityKeyHash::from_bytes(reader.array::<32>()?);
            let matched = String::from_utf8(reader.bytes()?.to_vec())
                .map_err(|_| LongPatternProviderErrorV1::CheckpointInvalid)?;
            let digest = reader.array::<32>()?;
            let gram_count = reader.u32()? as usize;
            let mut grams = Vec::with_capacity(gram_count);
            for _ in 0..gram_count {
                grams.push(reader.array::<3>()?);
            }
            let release = reader.bytes()?.to_vec();
            if matched.len() > bounds.matched_bytes() as usize
                || release.len() > MAX_LONG_PATTERN_RELEASE_BYTES_PER_ROW_V1
                || grams != long_pattern_grams_v1(&matched)
                || digest != long_pattern_digest_v1(&matched)
                || rows
                    .insert(
                        key,
                        LongPatternRowV1 {
                            matched,
                            digest,
                            grams,
                            release,
                        },
                    )
                    .is_some()
            {
                return Err(LongPatternProviderErrorV1::CheckpointInvalid);
            }
        }
        if !reader.done() {
            return Err(LongPatternProviderErrorV1::CheckpointInvalid);
        }
        let (postings, total_matched_bytes) = rebuild(&rows, bounds)?;
        Ok(Self {
            profile,
            bounds,
            rows,
            postings,
            total_matched_bytes,
        })
    }
}

fn rebuild(
    rows: &BTreeMap<EntityKeyHash, LongPatternRowV1>,
    bounds: LongPatternBoundsV1,
) -> Result<(BTreeMap<[u8; 3], BTreeSet<EntityKeyHash>>, u64), LongPatternProviderErrorV1> {
    if rows.len() > bounds.rows() as usize {
        return Err(LongPatternProviderErrorV1::PartitionBound);
    }
    let mut postings = BTreeMap::<[u8; 3], BTreeSet<EntityKeyHash>>::new();
    let mut total_bytes = 0_u64;
    let mut posting_count = 0_u64;
    for (key, row) in rows {
        total_bytes = total_bytes
            .checked_add(row.matched.len() as u64)
            .ok_or(LongPatternProviderErrorV1::PartitionBound)?;
        for gram in &row.grams {
            postings.entry(*gram).or_default().insert(*key);
            posting_count += 1;
        }
    }
    let posting_bytes = posting_count.saturating_mul(35);
    if total_bytes > bounds.total_matched_bytes()
        || postings.len() > bounds.distinct_grams() as usize
        || posting_count > bounds.postings()
        || posting_bytes > bounds.postings_bytes()
    {
        return Err(LongPatternProviderErrorV1::PartitionBound);
    }
    Ok((postings, total_bytes))
}

fn put_bounds(output: &mut Vec<u8>, bounds: LongPatternBoundsV1) {
    for value in [
        bounds.source_bytes(),
        bounds.matched_bytes(),
        bounds.rows(),
        bounds.distinct_grams(),
        bounds.grams_per_row(),
        bounds.pattern_bytes(),
        bounds.wildcard_atoms(),
        bounds.literal_runs(),
        bounds.candidates(),
        bounds.results(),
    ] {
        output.extend_from_slice(&value.to_be_bytes());
    }
    for value in [
        bounds.total_matched_bytes(),
        bounds.postings(),
        bounds.postings_bytes(),
        bounds.verification_bytes(),
    ] {
        output.extend_from_slice(&value.to_be_bytes());
    }
}
fn read_bounds(reader: &mut Reader<'_>) -> Result<LongPatternBoundsV1, LongPatternProviderErrorV1> {
    let source = reader.u32()?;
    let matched = reader.u32()?;
    let rows = reader.u32()?;
    let distinct = reader.u32()?;
    let grams = reader.u32()?;
    let pattern_bytes = reader.u32()?;
    let wildcard_atoms = reader.u32()?;
    let literal_runs = reader.u32()?;
    let candidates = reader.u32()?;
    let results = reader.u32()?;
    let total = reader.u64()?;
    let postings = reader.u64()?;
    let posting_bytes = reader.u64()?;
    let verification = reader.u64()?;
    LongPatternBoundsV1::new(
        source,
        matched,
        rows,
        total,
        distinct,
        postings,
        posting_bytes,
        grams,
        pattern_bytes,
        wildcard_atoms,
        literal_runs,
        candidates,
        verification,
        results,
    )
    .map_err(|_| LongPatternProviderErrorV1::InvalidBounds)
}
fn put_u32(output: &mut Vec<u8>, value: usize) -> Result<(), LongPatternProviderErrorV1> {
    output.extend_from_slice(
        &u32::try_from(value)
            .map_err(|_| LongPatternProviderErrorV1::CheckpointBound)?
            .to_be_bytes(),
    );
    Ok(())
}
fn put_bytes(output: &mut Vec<u8>, bytes: &[u8]) -> Result<(), LongPatternProviderErrorV1> {
    put_u32(output, bytes.len())?;
    output.extend_from_slice(bytes);
    Ok(())
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}
impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }
    fn array<const N: usize>(&mut self) -> Result<[u8; N], LongPatternProviderErrorV1> {
        let end = self
            .offset
            .checked_add(N)
            .ok_or(LongPatternProviderErrorV1::CheckpointInvalid)?;
        let slice = self
            .bytes
            .get(self.offset..end)
            .ok_or(LongPatternProviderErrorV1::CheckpointInvalid)?;
        self.offset = end;
        slice
            .try_into()
            .map_err(|_| LongPatternProviderErrorV1::CheckpointInvalid)
    }
    fn u8(&mut self) -> Result<u8, LongPatternProviderErrorV1> {
        Ok(self.array::<1>()?[0])
    }
    fn u32(&mut self) -> Result<u32, LongPatternProviderErrorV1> {
        Ok(u32::from_be_bytes(self.array()?))
    }
    fn u64(&mut self) -> Result<u64, LongPatternProviderErrorV1> {
        Ok(u64::from_be_bytes(self.array()?))
    }
    fn bytes(&mut self) -> Result<&'a [u8], LongPatternProviderErrorV1> {
        let len = self.u32()? as usize;
        let end = self
            .offset
            .checked_add(len)
            .ok_or(LongPatternProviderErrorV1::CheckpointInvalid)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(LongPatternProviderErrorV1::CheckpointInvalid)?;
        self.offset = end;
        Ok(value)
    }
    const fn done(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

/// Closed provider refusal. It never includes values, keys, grams or patterns.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LongPatternProviderErrorV1 {
    /// Contract/provider bounds are inconsistent.
    InvalidBounds,
    /// One source row exceeds its independent bound.
    RowBound,
    /// Complete partition state exceeds a declared bound.
    PartitionBound,
    /// Query profile does not match provider state.
    ProfileMismatch,
    /// Negation omitted its authorized positive universe.
    UniverseRequired,
    /// The positive universe is noncanonical or names absent provider rows.
    UniverseInvalid,
    /// Candidate expansion exceeds its bound.
    CandidateBound,
    /// Exact verification work exceeds its byte bound.
    VerificationBound,
    /// Verified results exceed their independent bound.
    ResultBound,
    /// Encoded checkpoint exceeds its byte bound.
    CheckpointBound,
    /// Checkpoint framing, content, digest, grams, or canonical order is invalid.
    CheckpointInvalid,
}

#[cfg(test)]
mod tests {
    use super::*;
    use riffdb_types::LongPatternOperatorV1;

    fn bounds() -> LongPatternBoundsV1 {
        LongPatternBoundsV1::new(
            8_000, 144_000, 8, 1_000_000, 10_000, 50_000, 2_000_000, 10_000, 8_000, 144_000, 8_000,
            8, 1_000_000, 8,
        )
        .expect("bounds")
    }
    fn key(value: u8) -> EntityKeyHash {
        EntityKeyHash::from_bytes([value; 32])
    }

    #[test]
    fn postings_are_only_candidates_and_checkpoint_round_trips_exactly() {
        let mut state = LongPatternPartitionV1::new(LongPatternProfileV1::UnicodeFoldV1, bounds());
        state
            .upsert(key(1), "Alpha experiment", b"one".to_vec())
            .expect("row");
        state
            .upsert(key(2), "Beta EXPERIMENT", b"two".to_vec())
            .expect("row");
        let pattern = CompiledLongPatternV1::compile(
            LongPatternOperatorV1::ILike,
            LongPatternProfileV1::UnicodeFoldV1,
            "%experiment",
        )
        .expect("pattern");
        let results = state.query(&pattern, None).expect("verified");
        assert_eq!(
            results
                .iter()
                .map(LongPatternResultV1::key)
                .collect::<Vec<_>>(),
            vec![key(1), key(2)]
        );
        let bytes = state.checkpoint_bytes().expect("checkpoint");
        let restored = LongPatternPartitionV1::from_checkpoint_bytes(&bytes).expect("restore");
        assert_eq!(restored, state);
        assert_eq!(restored.checkpoint_bytes().expect("stable bytes"), bytes);
    }

    #[test]
    fn negation_is_confined_to_the_explicit_authorized_universe() {
        let mut state = LongPatternPartitionV1::new(LongPatternProfileV1::BinaryUtf8V1, bounds());
        for (id, value) in [(1, "public"), (2, "secret"), (3, "hidden")] {
            state.upsert(key(id), value, vec![id]).expect("row");
        }
        let pattern = CompiledLongPatternV1::compile(
            LongPatternOperatorV1::NotLike,
            LongPatternProfileV1::BinaryUtf8V1,
            "%secret%",
        )
        .expect("pattern");
        let results = state
            .query(&pattern, Some(&[key(1), key(2)]))
            .expect("bounded difference");
        assert_eq!(
            results
                .iter()
                .map(LongPatternResultV1::key)
                .collect::<Vec<_>>(),
            vec![key(1)]
        );
    }

    #[test]
    fn failed_replacement_is_atomic() {
        let mut state = LongPatternPartitionV1::new(LongPatternProfileV1::BinaryUtf8V1, bounds());
        state.upsert(key(1), "stable", vec![]).expect("initial");
        let before = state.checkpoint_bytes().expect("before");
        assert_eq!(
            state.upsert(key(1), &"x".repeat(8_001), vec![]),
            Err(LongPatternProviderErrorV1::RowBound)
        );
        assert_eq!(state.checkpoint_bytes().expect("after"), before);
    }
}
