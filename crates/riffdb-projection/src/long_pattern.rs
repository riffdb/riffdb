//! Rebuildable bounded `long_pattern_v1` provider state (ADR-0174).

use std::collections::{BTreeMap, BTreeSet};

use riffdb_types::{
    CompiledLongPatternV1, EntityKeyHash, LongPatternBoundsV1, LongPatternProfileV1,
    long_pattern_digest_v1, long_pattern_grams_v1,
};

const MAGIC: &[u8] = b"RLPV\x01";
/// Durable rebuildable provider-state format identity.
pub const LONG_PATTERN_PROVIDER_STATE_FORMAT_V1: u16 = 1;
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

type LongPatternPostingsV1 = BTreeMap<[u8; 3], BTreeSet<EntityKeyHash>>;

/// One exact verified provider result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LongPatternResultV1 {
    key: EntityKeyHash,
    release: Vec<u8>,
}

/// Exact bounded provider work and verified result observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LongPatternQueryObservationV1 {
    results: Vec<LongPatternResultV1>,
    scanned_rows: u64,
    verification_bytes: u64,
}

impl LongPatternQueryObservationV1 {
    /// Verified result rows in canonical key order.
    #[must_use]
    pub fn results(&self) -> &[LongPatternResultV1] {
        &self.results
    }
    /// Provider rows inspected after policy-aligned admission.
    #[must_use]
    pub const fn scanned_rows(&self) -> u64 {
        self.scanned_rows
    }
    /// Complete matched bytes charged to exact verification.
    #[must_use]
    pub const fn verification_bytes(&self) -> u64 {
        self.verification_bytes
    }
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
    postings: LongPatternPostingsV1,
    total_matched_bytes: u64,
    posting_count: u64,
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
            posting_count: 0,
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
        let previous = self.rows.get(&key);
        let rows_len = self.rows.len() + usize::from(previous.is_none());
        let retired_bytes = previous.map_or(0, |old| old.matched.len() as u64);
        let retired_postings = previous.map_or(0, |old| old.grams.len() as u64);
        let total_matched_bytes = self
            .total_matched_bytes
            .checked_sub(retired_bytes)
            .and_then(|bytes| bytes.checked_add(row.matched.len() as u64))
            .ok_or(LongPatternProviderErrorV1::PartitionBound)?;
        let posting_count = self
            .posting_count
            .checked_sub(retired_postings)
            .and_then(|count| count.checked_add(row.grams.len() as u64))
            .ok_or(LongPatternProviderErrorV1::PartitionBound)?;
        let distinct_grams = self.projected_distinct_grams(key, previous, &row.grams);
        check_partition_bounds(
            rows_len,
            total_matched_bytes,
            distinct_grams,
            posting_count,
            self.bounds,
        )?;

        // Every bound is proven before the first mutation, so a refused upsert
        // leaves the published provider exactly as it was.
        if let Some(old) = self.rows.get(&key) {
            let retired = old.grams.clone();
            self.retire_postings(key, &retired);
        }
        for gram in &row.grams {
            self.postings.entry(*gram).or_default().insert(key);
        }
        self.rows.insert(key, row);
        self.total_matched_bytes = total_matched_bytes;
        self.posting_count = posting_count;
        Ok(())
    }

    /// Atomically removes one derived row.
    pub fn remove(&mut self, key: EntityKeyHash) -> Result<bool, LongPatternProviderErrorV1> {
        let Some(row) = self.rows.remove(&key) else {
            return Ok(false);
        };
        self.total_matched_bytes = self
            .total_matched_bytes
            .saturating_sub(row.matched.len() as u64);
        self.posting_count = self.posting_count.saturating_sub(row.grams.len() as u64);
        self.retire_postings(key, &row.grams);
        Ok(true)
    }

    /// Drops one key from the postings of the grams it contributed, removing a
    /// gram entirely once no row references it.
    fn retire_postings(&mut self, key: EntityKeyHash, grams: &[[u8; 3]]) {
        for gram in grams {
            if let Some(keys) = self.postings.get_mut(gram) {
                keys.remove(&key);
                if keys.is_empty() {
                    self.postings.remove(gram);
                }
            }
        }
    }

    /// Distinct gram count the postings would carry after replacing one row,
    /// computed without materialising a candidate map.
    fn projected_distinct_grams(
        &self,
        key: EntityKeyHash,
        previous: Option<&LongPatternRowV1>,
        grams: &[[u8; 3]],
    ) -> usize {
        let successor: BTreeSet<[u8; 3]> = grams.iter().copied().collect();
        let mut affected: BTreeSet<[u8; 3]> = successor.clone();
        if let Some(old) = previous {
            affected.extend(old.grams.iter().copied());
        }
        let mut distinct = self.postings.len();
        for gram in &affected {
            let present = self.postings.contains_key(gram);
            let after = successor.contains(gram)
                || self
                    .postings
                    .get(gram)
                    .is_some_and(|keys| keys.iter().any(|existing| *existing != key));
            match (present, after) {
                (false, true) => distinct += 1,
                (true, false) => distinct -= 1,
                _ => {}
            }
        }
        distinct
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
        self.query_observed(pattern, universe)
            .map(|observation| observation.results)
    }

    /// Selects and verifies while retaining exact bounded work evidence.
    pub fn query_observed(
        &self,
        pattern: &CompiledLongPatternV1,
        universe: Option<&[EntityKeyHash]>,
    ) -> Result<LongPatternQueryObservationV1, LongPatternProviderErrorV1> {
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
        let scanned_rows = candidates.len() as u64;
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
        Ok(LongPatternQueryObservationV1 {
            results,
            scanned_rows,
            verification_bytes,
        })
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
        let (postings, total_matched_bytes, posting_count) = rebuild(&rows, bounds)?;
        Ok(Self {
            profile,
            bounds,
            rows,
            postings,
            total_matched_bytes,
            posting_count,
        })
    }
}

/// The partition ceilings, checked identically by the full rebuild a checkpoint
/// load performs and by the incremental upsert path, so neither can drift into
/// admitting a partition the other would refuse.
fn check_partition_bounds(
    rows: usize,
    total_matched_bytes: u64,
    distinct_grams: usize,
    posting_count: u64,
    bounds: LongPatternBoundsV1,
) -> Result<(), LongPatternProviderErrorV1> {
    let posting_bytes = posting_count.saturating_mul(35);
    if rows > bounds.rows() as usize
        || total_matched_bytes > bounds.total_matched_bytes()
        || distinct_grams > bounds.distinct_grams() as usize
        || posting_count > bounds.postings()
        || posting_bytes > bounds.postings_bytes()
    {
        return Err(LongPatternProviderErrorV1::PartitionBound);
    }
    Ok(())
}

/// Builds the postings for every retained row. This is the checkpoint-load
/// path, where one pass over N rows is the work being done; the upsert and
/// remove paths maintain the same state incrementally rather than calling this
/// per mutation, which made building N rows cost O(N^2).
fn rebuild(
    rows: &BTreeMap<EntityKeyHash, LongPatternRowV1>,
    bounds: LongPatternBoundsV1,
) -> Result<(LongPatternPostingsV1, u64, u64), LongPatternProviderErrorV1> {
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
    check_partition_bounds(
        rows.len(),
        total_bytes,
        postings.len(),
        posting_count,
        bounds,
    )?;
    Ok((postings, total_bytes, posting_count))
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

    const CHECKPOINT_FIXTURE: &str =
        include_str!("../../../fixtures/projection/long-pattern-provider-state-v1.txt");

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
        let expected = CHECKPOINT_FIXTURE
            .lines()
            .find_map(|line| line.strip_prefix("bytes_hex="))
            .expect("fixture bytes");
        assert_eq!(
            bytes
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
            expected
        );
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
    fn incremental_maintenance_equals_a_full_rebuild_over_the_same_rows() {
        // The upsert and remove paths maintain postings incrementally instead of
        // rebuilding every row per mutation. That is only a safe exchange if the
        // state it leaves is the state a full rebuild would have produced, so
        // this drives a sequence that overwrites, removes, re-adds and shares
        // grams between rows, and compares against rebuild directly.
        let mut state = LongPatternPartitionV1::new(LongPatternProfileV1::UnicodeFoldV1, bounds());
        for (index, source) in [
            "alpha experiment one",
            "beta experiment two",
            "gamma trial three",
            "alpha experiment one",
        ]
        .into_iter()
        .enumerate()
        {
            state
                .upsert(key(index as u8 + 1), source, vec![index as u8])
                .expect("seed row");
        }
        // Overwrite a row with different grams, so retired grams must be dropped.
        state
            .upsert(key(2), "delta unrelated words", vec![9])
            .expect("overwrite");
        // Remove a row whose grams are shared with another, and one that is not.
        assert!(state.remove(key(4)).expect("remove shared-gram row"));
        assert!(state.remove(key(3)).expect("remove distinct row"));
        // Re-add, to prove a removed key leaves no residue behind.
        state
            .upsert(key(3), "gamma trial three", vec![3])
            .expect("re-add");
        assert!(!state.remove(key(200)).expect("absent key is not an error"));

        let (postings, total_matched_bytes, posting_count) =
            rebuild(&state.rows, state.bounds).expect("rebuild the same rows");
        assert_eq!(state.postings, postings, "postings diverged from a rebuild");
        assert_eq!(state.total_matched_bytes, total_matched_bytes);
        assert_eq!(state.posting_count, posting_count);
        assert_eq!(state.postings.len(), postings.len(), "distinct gram count");
    }

    #[test]
    fn a_refused_upsert_leaves_no_partial_posting_behind() {
        // Bounds are proven before the first mutation, so a partition-bound
        // refusal must not leave the retired row's grams dropped or the
        // successor's grams half-applied.
        let narrow = LongPatternBoundsV1::new(
            8_000, 144_000, 8, 4, 10_000, 50_000, 2_000_000, 10_000, 8_000, 144_000, 8_000, 8,
            1_000_000, 8,
        )
        .expect("narrow bounds");
        let mut state = LongPatternPartitionV1::new(LongPatternProfileV1::BinaryUtf8V1, narrow);
        state.upsert(key(1), "abcd", vec![]).expect("first row");
        let before = state.checkpoint_bytes().expect("before");
        let refused = state.upsert(key(2), "the quick brown fox jumps", vec![]);
        assert!(
            refused.is_err(),
            "expected the distinct-gram ceiling to refuse this row"
        );
        assert_eq!(state.checkpoint_bytes().expect("after"), before);
        let (postings, total_matched_bytes, posting_count) =
            rebuild(&state.rows, state.bounds).expect("rebuild after refusal");
        assert_eq!(state.postings, postings);
        assert_eq!(state.total_matched_bytes, total_matched_bytes);
        assert_eq!(state.posting_count, posting_count);
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

    #[test]
    fn maximum_mlflow_value_and_checkpoint_corruption_are_fail_closed() {
        let mut state = LongPatternPartitionV1::new(LongPatternProfileV1::UnicodeFoldV1, bounds());
        let source = "A".repeat(8_000);
        state
            .upsert(key(1), &source, b"release".to_vec())
            .expect("maximum source value");
        let pattern = CompiledLongPatternV1::compile_bounded(
            LongPatternOperatorV1::Like,
            LongPatternProfileV1::UnicodeFoldV1,
            "%aaa%",
            bounds(),
        )
        .expect("bounded pattern");
        let observed = state.query_observed(&pattern, None).expect("observation");
        assert_eq!(observed.results().len(), 1);
        assert_eq!(observed.scanned_rows(), 1);
        assert_eq!(observed.verification_bytes(), 8_000);

        let bytes = state.checkpoint_bytes().expect("checkpoint");
        for truncated in [0, 1, bytes.len() / 2, bytes.len() - 1] {
            assert_eq!(
                LongPatternPartitionV1::from_checkpoint_bytes(&bytes[..truncated]),
                Err(LongPatternProviderErrorV1::CheckpointInvalid)
            );
        }
        let mut corrupted = bytes;
        let index = MAGIC.len() + 1 + (10 * 4) + (4 * 8) + 4 + 32 + 4 + source.len();
        corrupted[index] ^= 1;
        assert_eq!(
            LongPatternPartitionV1::from_checkpoint_bytes(&corrupted),
            Err(LongPatternProviderErrorV1::CheckpointInvalid)
        );
    }
}
