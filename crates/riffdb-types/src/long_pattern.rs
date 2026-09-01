//! ADR-0174 long-value exact wildcard semantics.

use sha2::{Digest, Sha256};

/// Maximum authoritative UTF-8 source bytes admitted by `long_pattern_v1`.
pub const MAX_LONG_PATTERN_SOURCE_BYTES_V1: usize = 8_000;
/// Frozen maximum expansion of an 8,000-byte source under `unicode_fold_v1`.
pub const MAX_LONG_PATTERN_MATCHED_BYTES_V1: usize = 144_000;
/// Maximum submitted pattern bytes.
pub const MAX_LONG_PATTERN_QUERY_BYTES_V1: usize = 8_000;
/// Maximum wildcard atoms after profile transformation.
pub const MAX_LONG_PATTERN_ATOMS_V1: usize = 144_000;
/// Maximum compiler-declared provider rows in one partition.
pub const MAX_LONG_PATTERN_ROWS_PER_PARTITION_V1: u32 = 65_535;

/// Complete independent resource bounds shared by contracts, providers and query plans.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LongPatternBoundsV1 {
    source_bytes: u32,
    matched_bytes: u32,
    rows: u32,
    total_matched_bytes: u64,
    distinct_grams: u32,
    postings: u64,
    postings_bytes: u64,
    grams_per_row: u32,
    pattern_bytes: u32,
    wildcard_atoms: u32,
    literal_runs: u32,
    candidates: u32,
    verification_bytes: u64,
    results: u32,
}

impl LongPatternBoundsV1 {
    /// Constructs one fully checked bound set.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        source_bytes: u32,
        matched_bytes: u32,
        rows: u32,
        total_matched_bytes: u64,
        distinct_grams: u32,
        postings: u64,
        postings_bytes: u64,
        grams_per_row: u32,
        pattern_bytes: u32,
        wildcard_atoms: u32,
        literal_runs: u32,
        candidates: u32,
        verification_bytes: u64,
        results: u32,
    ) -> Result<Self, LongPatternBoundsErrorV1> {
        let bounds = Self {
            source_bytes,
            matched_bytes,
            rows,
            total_matched_bytes,
            distinct_grams,
            postings,
            postings_bytes,
            grams_per_row,
            pattern_bytes,
            wildcard_atoms,
            literal_runs,
            candidates,
            verification_bytes,
            results,
        };
        if source_bytes == 0
            || source_bytes as usize > MAX_LONG_PATTERN_SOURCE_BYTES_V1
            || matched_bytes == 0
            || matched_bytes as usize > MAX_LONG_PATTERN_MATCHED_BYTES_V1
            || rows == 0
            || rows > MAX_LONG_PATTERN_ROWS_PER_PARTITION_V1
            || total_matched_bytes == 0
            || distinct_grams == 0
            || postings == 0
            || postings_bytes == 0
            || grams_per_row == 0
            || pattern_bytes == 0
            || pattern_bytes as usize > MAX_LONG_PATTERN_QUERY_BYTES_V1
            || wildcard_atoms == 0
            || wildcard_atoms as usize > MAX_LONG_PATTERN_ATOMS_V1
            || literal_runs == 0
            || candidates == 0
            || candidates > rows
            || verification_bytes == 0
            || results == 0
            || results > candidates
        {
            return Err(LongPatternBoundsErrorV1);
        }
        Ok(bounds)
    }
    /// Maximum source bytes.
    #[must_use]
    pub const fn source_bytes(self) -> u32 {
        self.source_bytes
    }
    /// Maximum matched bytes per row.
    #[must_use]
    pub const fn matched_bytes(self) -> u32 {
        self.matched_bytes
    }
    /// Maximum rows per partition.
    #[must_use]
    pub const fn rows(self) -> u32 {
        self.rows
    }
    /// Maximum total retained matched bytes.
    #[must_use]
    pub const fn total_matched_bytes(self) -> u64 {
        self.total_matched_bytes
    }
    /// Maximum distinct grams.
    #[must_use]
    pub const fn distinct_grams(self) -> u32 {
        self.distinct_grams
    }
    /// Maximum posting entries.
    #[must_use]
    pub const fn postings(self) -> u64 {
        self.postings
    }
    /// Maximum encoded posting bytes.
    #[must_use]
    pub const fn postings_bytes(self) -> u64 {
        self.postings_bytes
    }
    /// Maximum grams retained per row.
    #[must_use]
    pub const fn grams_per_row(self) -> u32 {
        self.grams_per_row
    }
    /// Maximum submitted pattern bytes.
    #[must_use]
    pub const fn pattern_bytes(self) -> u32 {
        self.pattern_bytes
    }
    /// Maximum transformed wildcard atoms.
    #[must_use]
    pub const fn wildcard_atoms(self) -> u32 {
        self.wildcard_atoms
    }
    /// Maximum literal runs in one wildcard pattern.
    #[must_use]
    pub const fn literal_runs(self) -> u32 {
        self.literal_runs
    }
    /// Maximum candidates verified by one query.
    #[must_use]
    pub const fn candidates(self) -> u32 {
        self.candidates
    }
    /// Maximum matched bytes verified by one query.
    #[must_use]
    pub const fn verification_bytes(self) -> u64 {
        self.verification_bytes
    }
    /// Maximum released results.
    #[must_use]
    pub const fn results(self) -> u32 {
        self.results
    }
}

/// A bound set was zero, inconsistent, or above a fixed ceiling.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LongPatternBoundsErrorV1;

/// Frozen matching profile.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum LongPatternProfileV1 {
    /// Compare canonical UTF-8 directly.
    BinaryUtf8V1 = 1,
    /// Apply the frozen Unicode fold before comparison.
    UnicodeFoldV1 = 2,
}

impl LongPatternProfileV1 {
    /// Decodes a stable profile discriminant.
    #[must_use]
    pub const fn from_discriminant(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::BinaryUtf8V1),
            2 => Some(Self::UnicodeFoldV1),
            _ => None,
        }
    }

    /// Produces the exact retained matched form.
    #[must_use]
    pub fn matched_form(self, value: &str) -> String {
        match self {
            Self::BinaryUtf8V1 => value.to_owned(),
            Self::UnicodeFoldV1 => crate::unicode_fold_v1(value),
        }
    }
}

/// Closed public operator inventory.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum LongPatternOperatorV1 {
    /// Whole-value equality under the declared profile.
    Equals = 1,
    /// Literal prefix under the declared profile.
    StartsWith = 2,
    /// Literal suffix under the declared profile.
    EndsWith = 3,
    /// Literal substring under the declared profile.
    Contains = 4,
    /// SQL-like `%` and `_` matching under binary UTF-8.
    Like = 5,
    /// SQL-like matching under `unicode_fold_v1`.
    ILike = 6,
    /// Authorized-universe difference against `Like`.
    NotLike = 7,
    /// Authorized-universe difference against `ILike`.
    NotILike = 8,
}

impl LongPatternOperatorV1 {
    /// Decodes a stable operator discriminant.
    #[must_use]
    pub const fn from_discriminant(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Equals),
            2 => Some(Self::StartsWith),
            3 => Some(Self::EndsWith),
            4 => Some(Self::Contains),
            5 => Some(Self::Like),
            6 => Some(Self::ILike),
            7 => Some(Self::NotLike),
            8 => Some(Self::NotILike),
            _ => None,
        }
    }

    /// Whether the operator interprets wildcard syntax.
    #[must_use]
    pub const fn is_wildcard(self) -> bool {
        matches!(
            self,
            Self::Like | Self::ILike | Self::NotLike | Self::NotILike
        )
    }

    /// Whether matching uses the Unicode-fold profile regardless of storage profile.
    #[must_use]
    pub const fn is_case_insensitive(self) -> bool {
        matches!(self, Self::ILike | Self::NotILike)
    }

    /// Whether result formation is an authorized positive-universe difference.
    #[must_use]
    pub const fn is_negated(self) -> bool {
        matches!(self, Self::NotLike | Self::NotILike)
    }
}

/// One compiled wildcard atom. This is process-local semantic state, not a durable codec.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PatternAtomV1 {
    Literal(char),
    One,
    Many,
}

/// Checked pattern with bounded transformed atoms and mandatory gram hints.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompiledLongPatternV1 {
    operator: LongPatternOperatorV1,
    profile: LongPatternProfileV1,
    literal: Option<String>,
    atoms: Vec<PatternAtomV1>,
    mandatory_grams: Vec<[u8; 3]>,
}

impl CompiledLongPatternV1 {
    /// Compiles one bounded literal or wildcard pattern.
    pub fn compile(
        operator: LongPatternOperatorV1,
        declared_profile: LongPatternProfileV1,
        source: &str,
    ) -> Result<Self, LongPatternErrorV1> {
        Self::compile_with_limits(
            operator,
            declared_profile,
            source,
            MAX_LONG_PATTERN_QUERY_BYTES_V1,
            MAX_LONG_PATTERN_MATCHED_BYTES_V1,
            MAX_LONG_PATTERN_ATOMS_V1,
            MAX_LONG_PATTERN_ATOMS_V1,
        )
    }

    /// Compiles against the exact immutable bounds declared by one provider.
    pub fn compile_bounded(
        operator: LongPatternOperatorV1,
        declared_profile: LongPatternProfileV1,
        source: &str,
        bounds: LongPatternBoundsV1,
    ) -> Result<Self, LongPatternErrorV1> {
        Self::compile_with_limits(
            operator,
            declared_profile,
            source,
            bounds.pattern_bytes() as usize,
            bounds.matched_bytes() as usize,
            bounds.wildcard_atoms() as usize,
            bounds.literal_runs() as usize,
        )
    }

    fn compile_with_limits(
        operator: LongPatternOperatorV1,
        declared_profile: LongPatternProfileV1,
        source: &str,
        pattern_bytes: usize,
        matched_bytes: usize,
        wildcard_atoms: usize,
        literal_runs: usize,
    ) -> Result<Self, LongPatternErrorV1> {
        if source.is_empty() || source.len() > pattern_bytes {
            return Err(LongPatternErrorV1::PatternBytes);
        }
        let profile = if operator.is_case_insensitive() {
            LongPatternProfileV1::UnicodeFoldV1
        } else {
            declared_profile
        };
        if !operator.is_wildcard() {
            let literal = profile.matched_form(source);
            if literal.len() > matched_bytes {
                return Err(LongPatternErrorV1::MatchedBytes);
            }
            let mandatory_grams = distinct_grams(literal.as_bytes());
            return Ok(Self {
                operator,
                profile,
                literal: Some(literal),
                atoms: Vec::new(),
                mandatory_grams,
            });
        }

        let runs = parse_wildcard_runs(source)?;
        if runs.len() > literal_runs {
            return Err(LongPatternErrorV1::PatternAtoms);
        }
        let mut atoms = Vec::new();
        let mut mandatory_grams = Vec::new();
        for run in runs {
            match run {
                PatternRunV1::Literal(value) => {
                    let matched = profile.matched_form(&value);
                    mandatory_grams.extend(distinct_grams(matched.as_bytes()));
                    atoms.extend(matched.chars().map(PatternAtomV1::Literal));
                }
                PatternRunV1::One => atoms.push(PatternAtomV1::One),
                PatternRunV1::Many => {
                    if !matches!(atoms.last(), Some(PatternAtomV1::Many)) {
                        atoms.push(PatternAtomV1::Many);
                    }
                }
            }
            if atoms.len() > wildcard_atoms {
                return Err(LongPatternErrorV1::PatternAtoms);
            }
        }
        mandatory_grams.sort_unstable();
        mandatory_grams.dedup();
        Ok(Self {
            operator,
            profile,
            literal: None,
            atoms,
            mandatory_grams,
        })
    }

    /// Exact operator selected by the compiled query.
    #[must_use]
    pub const fn operator(&self) -> LongPatternOperatorV1 {
        self.operator
    }

    /// Effective matching profile.
    #[must_use]
    pub const fn profile(&self) -> LongPatternProfileV1 {
        self.profile
    }

    /// Ordered distinct mandatory literal grams. Empty means bounded provider scan.
    #[must_use]
    pub fn mandatory_grams(&self) -> &[[u8; 3]] {
        &self.mandatory_grams
    }

    /// Verifies one already-profiled retained value exactly.
    #[must_use]
    pub fn matches_matched(&self, matched: &str) -> bool {
        let positive = match (self.operator, self.literal.as_deref()) {
            (LongPatternOperatorV1::Equals, Some(literal)) => matched == literal,
            (LongPatternOperatorV1::StartsWith, Some(literal)) => matched.starts_with(literal),
            (LongPatternOperatorV1::EndsWith, Some(literal)) => matched.ends_with(literal),
            (LongPatternOperatorV1::Contains, Some(literal)) => matched.contains(literal),
            _ => wildcard_matches(&self.atoms, matched),
        };
        if self.operator.is_negated() {
            !positive
        } else {
            positive
        }
    }

    /// Applies the effective profile and verifies one source value.
    #[must_use]
    pub fn matches_source(&self, source: &str) -> bool {
        self.matches_matched(&self.profile.matched_form(source))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PatternRunV1 {
    Literal(String),
    One,
    Many,
}

fn parse_wildcard_runs(source: &str) -> Result<Vec<PatternRunV1>, LongPatternErrorV1> {
    let mut runs = Vec::new();
    let mut literal = String::new();
    let mut chars = source.chars();
    while let Some(character) = chars.next() {
        match character {
            '\\' => {
                let escaped = chars.next().ok_or(LongPatternErrorV1::InvalidEscape)?;
                if !matches!(escaped, '%' | '_' | '\\') {
                    return Err(LongPatternErrorV1::InvalidEscape);
                }
                literal.push(escaped);
            }
            '%' | '_' => {
                if !literal.is_empty() {
                    runs.push(PatternRunV1::Literal(std::mem::take(&mut literal)));
                }
                runs.push(if character == '%' {
                    PatternRunV1::Many
                } else {
                    PatternRunV1::One
                });
            }
            _ => literal.push(character),
        }
    }
    if !literal.is_empty() {
        runs.push(PatternRunV1::Literal(literal));
    }
    Ok(runs)
}

fn wildcard_matches(atoms: &[PatternAtomV1], matched: &str) -> bool {
    let value = matched.chars().collect::<Vec<_>>();
    let (mut atom, mut scalar) = (0_usize, 0_usize);
    let (mut star, mut star_scalar) = (None, 0_usize);
    while scalar < value.len() {
        match atoms.get(atom) {
            Some(PatternAtomV1::Literal(expected)) if *expected == value[scalar] => {
                atom += 1;
                scalar += 1;
            }
            Some(PatternAtomV1::One) => {
                atom += 1;
                scalar += 1;
            }
            Some(PatternAtomV1::Many) => {
                star = Some(atom);
                atom += 1;
                star_scalar = scalar;
            }
            _ => {
                let Some(star_atom) = star else {
                    return false;
                };
                star_scalar += 1;
                scalar = star_scalar;
                atom = star_atom + 1;
            }
        }
    }
    while matches!(atoms.get(atom), Some(PatternAtomV1::Many)) {
        atom += 1;
    }
    atom == atoms.len()
}

/// Returns ordered distinct three-byte windows without enumerating substrings.
#[must_use]
pub fn long_pattern_grams_v1(matched: &str) -> Vec<[u8; 3]> {
    distinct_grams(matched.as_bytes())
}

fn distinct_grams(bytes: &[u8]) -> Vec<[u8; 3]> {
    let mut grams = bytes
        .windows(3)
        .map(|window| [window[0], window[1], window[2]])
        .collect::<Vec<_>>();
    grams.sort_unstable();
    grams.dedup();
    grams
}

/// Collision-resistant digest used only as a candidate filter; callers still verify bytes.
#[must_use]
pub fn long_pattern_digest_v1(matched: &str) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"RIFFDB-LONG-PATTERN-VALUE-V1\0");
    digest.update((matched.len() as u64).to_be_bytes());
    digest.update(matched.as_bytes());
    digest.finalize().into()
}

/// Closed pattern compilation refusal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LongPatternErrorV1 {
    /// Source query pattern is empty or exceeds its byte maximum.
    PatternBytes,
    /// Profile expansion exceeded the matched-value maximum.
    MatchedBytes,
    /// Transformed wildcard atom count exceeded its maximum.
    PatternAtoms,
    /// Backslash did not escape exactly `%`, `_`, or backslash.
    InvalidEscape,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wildcard_semantics_cover_scalars_escaping_folding_and_negation() {
        let like = CompiledLongPatternV1::compile(
            LongPatternOperatorV1::Like,
            LongPatternProfileV1::BinaryUtf8V1,
            r"ab%_\%",
        )
        .expect("pattern");
        assert!(like.matches_source("abλ-tail%"));
        assert!(!like.matches_source("ab%"));

        let ilike = CompiledLongPatternV1::compile(
            LongPatternOperatorV1::ILike,
            LongPatternProfileV1::BinaryUtf8V1,
            "strasse%",
        )
        .expect("folded pattern");
        assert!(ilike.matches_source("STRAßE-tail"));

        let not_like = CompiledLongPatternV1::compile(
            LongPatternOperatorV1::NotLike,
            LongPatternProfileV1::BinaryUtf8V1,
            "%secret%",
        )
        .expect("negated pattern");
        assert!(not_like.matches_source("public"));
        assert!(!not_like.matches_source("a secret value"));
    }

    #[test]
    fn invalid_escapes_and_pattern_bytes_fail_closed() {
        for pattern in [r"trailing\", r"bad\x"] {
            assert_eq!(
                CompiledLongPatternV1::compile(
                    LongPatternOperatorV1::Like,
                    LongPatternProfileV1::BinaryUtf8V1,
                    pattern,
                ),
                Err(LongPatternErrorV1::InvalidEscape)
            );
        }
        assert_eq!(
            CompiledLongPatternV1::compile(
                LongPatternOperatorV1::Contains,
                LongPatternProfileV1::BinaryUtf8V1,
                "",
            ),
            Err(LongPatternErrorV1::PatternBytes)
        );
    }

    #[test]
    fn grams_are_distinct_ordered_and_only_acceleration_hints() {
        assert_eq!(long_pattern_grams_v1("ababa"), vec![*b"aba", *b"bab"]);
        let wildcard = CompiledLongPatternV1::compile(
            LongPatternOperatorV1::Like,
            LongPatternProfileV1::BinaryUtf8V1,
            "%ab%",
        )
        .expect("short literal");
        assert!(wildcard.mandatory_grams().is_empty());
        assert!(wildcard.matches_source("zzabzz"));
    }
}
