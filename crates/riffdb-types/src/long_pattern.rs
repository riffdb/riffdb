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
    /// Linear-time Thompson NFA regular expression matching (`ADR-0339`).
    MatchesRegex = 9,
}

/// Maximum regular-expression pattern bytes admitted by `MatchesRegex` (`ADR-0339`).
pub const MAX_REGEX_PATTERN_BYTES_V1: usize = 1_024;
/// Maximum Thompson NFA states admitted for one regular expression (`ADR-0339`).
pub const MAX_REGEX_NFA_STATES_V1: usize = 256;

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
            9 => Some(Self::MatchesRegex),
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

    /// Whether the operator compiles a regular expression to a Thompson NFA.
    #[must_use]
    pub const fn is_regex(self) -> bool {
        matches!(self, Self::MatchesRegex)
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

#[derive(Clone, Debug, Eq, PartialEq)]
enum RegexCharMatcherV1 {
    Exact(char),
    Any,
    Digit,
    Word,
    Space,
    Class {
        negated: bool,
        ranges: Vec<(char, char)>,
    },
}

impl RegexCharMatcherV1 {
    fn matches(&self, ch: char) -> bool {
        match self {
            Self::Exact(expected) => *expected == ch,
            Self::Any => ch != '\n',
            Self::Digit => ch.is_ascii_digit(),
            Self::Word => ch.is_ascii_alphanumeric() || ch == '_',
            Self::Space => ch.is_whitespace(),
            Self::Class { negated, ranges } => {
                let hit = ranges.iter().any(|(start, end)| *start <= ch && ch <= *end);
                if *negated { !hit } else { hit }
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ThompsonStateV1 {
    Match {
        matcher: RegexCharMatcherV1,
        next: usize,
    },
    Split {
        out1: usize,
        out2: usize,
    },
    Jump {
        target: usize,
    },
    Accept,
}

/// Backtracking-free Thompson NFA compiled from a bounded regular expression (`ADR-0339`).
#[derive(Clone, Debug, Eq, PartialEq)]
struct CompiledThompsonNfaV1 {
    anchored_start: bool,
    anchored_end: bool,
    start_state: usize,
    states: Vec<ThompsonStateV1>,
}

/// Checked pattern with bounded transformed atoms and mandatory gram hints.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompiledLongPatternV1 {
    operator: LongPatternOperatorV1,
    profile: LongPatternProfileV1,
    literal: Option<String>,
    atoms: Vec<PatternAtomV1>,
    regex_nfa: Option<CompiledThompsonNfaV1>,
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
        if operator.is_regex() {
            if source.len() > MAX_REGEX_PATTERN_BYTES_V1 {
                return Err(LongPatternErrorV1::PatternBytes);
            }
            let (nfa, mandatory_grams) = compile_thompson_regex_v1(source, profile)?;
            return Ok(Self {
                operator,
                profile,
                literal: None,
                atoms: Vec::new(),
                regex_nfa: Some(nfa),
                mandatory_grams,
            });
        }
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
                regex_nfa: None,
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
            regex_nfa: None,
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
        if let Some(ref nfa) = self.regex_nfa {
            return nfa.find_first_span(matched).is_some();
        }
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

    /// Finds the first matched byte span `(start_byte, end_byte)` in an already-profiled string.
    #[must_use]
    pub fn find_first_match_span(&self, matched: &str) -> Option<(usize, usize)> {
        if let Some(ref nfa) = self.regex_nfa {
            return nfa.find_first_span(matched);
        }
        match (self.operator, self.literal.as_deref()) {
            (LongPatternOperatorV1::Equals, Some(literal)) if matched == literal => {
                Some((0, matched.len()))
            }
            (LongPatternOperatorV1::StartsWith, Some(literal)) if matched.starts_with(literal) => {
                Some((0, literal.len()))
            }
            (LongPatternOperatorV1::EndsWith, Some(literal)) if matched.ends_with(literal) => {
                Some((matched.len() - literal.len(), matched.len()))
            }
            (LongPatternOperatorV1::Contains, Some(literal)) => matched
                .find(literal)
                .map(|start| (start, start + literal.len())),
            _ => {
                if self.matches_matched(matched) {
                    Some((0, matched.len()))
                } else {
                    None
                }
            }
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

#[derive(Clone, Debug)]
enum RegexAstV1 {
    Empty,
    Matcher(RegexCharMatcherV1),
    Seq(Vec<RegexAstV1>),
    Alt(Vec<RegexAstV1>),
    Repeat {
        inner: Box<RegexAstV1>,
        min: usize,
        max: Option<usize>,
    },
}

fn compile_thompson_regex_v1(
    source: &str,
    profile: LongPatternProfileV1,
) -> Result<(CompiledThompsonNfaV1, Vec<[u8; 3]>), LongPatternErrorV1> {
    let mut trimmed = source;
    let anchored_start = if let Some(rest) = trimmed.strip_prefix('^') {
        trimmed = rest;
        true
    } else {
        false
    };
    let anchored_end = if trimmed.ends_with('$') && !trimmed.ends_with(r"\$") {
        trimmed = &trimmed[..trimmed.len() - 1];
        true
    } else {
        false
    };
    let chars = trimmed.chars().collect::<Vec<_>>();
    let mut pos = 0_usize;
    let ast = parse_regex_alt(&chars, &mut pos, profile)?;
    if pos != chars.len() {
        return Err(LongPatternErrorV1::InvalidRegex);
    }
    let mandatory_grams = extract_mandatory_grams_from_ast(&ast);
    let mut builder = ThompsonBuilderV1 { states: Vec::new() };
    let accept = builder.push(ThompsonStateV1::Accept)?;
    let start_state = builder.compile_node(&ast, accept)?;
    Ok((
        CompiledThompsonNfaV1 {
            anchored_start,
            anchored_end,
            start_state,
            states: builder.states,
        },
        mandatory_grams,
    ))
}

fn parse_regex_alt(
    chars: &[char],
    pos: &mut usize,
    profile: LongPatternProfileV1,
) -> Result<RegexAstV1, LongPatternErrorV1> {
    let mut branches = vec![parse_regex_seq(chars, pos, profile)?];
    while *pos < chars.len() && chars[*pos] == '|' {
        *pos += 1;
        branches.push(parse_regex_seq(chars, pos, profile)?);
    }
    if branches.len() == 1 {
        Ok(branches.pop().unwrap_or(RegexAstV1::Empty))
    } else {
        Ok(RegexAstV1::Alt(branches))
    }
}

fn parse_regex_seq(
    chars: &[char],
    pos: &mut usize,
    profile: LongPatternProfileV1,
) -> Result<RegexAstV1, LongPatternErrorV1> {
    let mut items = Vec::new();
    while *pos < chars.len() && !matches!(chars[*pos], '|' | ')') {
        let atom = parse_regex_atom(chars, pos, profile)?;
        let quantified = parse_regex_quantifier(chars, pos, atom)?;
        items.push(quantified);
    }
    if items.is_empty() {
        Ok(RegexAstV1::Empty)
    } else if items.len() == 1 {
        Ok(items.pop().unwrap_or(RegexAstV1::Empty))
    } else {
        Ok(RegexAstV1::Seq(items))
    }
}

fn parse_regex_atom(
    chars: &[char],
    pos: &mut usize,
    profile: LongPatternProfileV1,
) -> Result<RegexAstV1, LongPatternErrorV1> {
    let Some(&ch) = chars.get(*pos) else {
        return Err(LongPatternErrorV1::InvalidRegex);
    };
    match ch {
        '(' => {
            *pos += 1;
            let inner = parse_regex_alt(chars, pos, profile)?;
            if chars.get(*pos) != Some(&')') {
                return Err(LongPatternErrorV1::InvalidRegex);
            }
            *pos += 1;
            Ok(inner)
        }
        '[' => {
            *pos += 1;
            let negated = if chars.get(*pos) == Some(&'^') {
                *pos += 1;
                true
            } else {
                false
            };
            let mut ranges = Vec::new();
            while *pos < chars.len() && chars[*pos] != ']' {
                let start = parse_class_char(chars, pos, profile)?;
                if chars.get(*pos) == Some(&'-')
                    && chars.get(*pos + 1).is_some_and(|&next| next != ']')
                {
                    *pos += 1;
                    let end = parse_class_char(chars, pos, profile)?;
                    if start > end {
                        return Err(LongPatternErrorV1::InvalidRegex);
                    }
                    ranges.push((start, end));
                } else {
                    ranges.push((start, start));
                }
            }
            if chars.get(*pos) != Some(&']') || ranges.is_empty() {
                return Err(LongPatternErrorV1::InvalidRegex);
            }
            *pos += 1;
            Ok(RegexAstV1::Matcher(RegexCharMatcherV1::Class {
                negated,
                ranges,
            }))
        }
        '.' => {
            *pos += 1;
            Ok(RegexAstV1::Matcher(RegexCharMatcherV1::Any))
        }
        '\\' => {
            *pos += 1;
            let Some(&escaped) = chars.get(*pos) else {
                return Err(LongPatternErrorV1::InvalidRegex);
            };
            *pos += 1;
            match escaped {
                'd' => Ok(RegexAstV1::Matcher(RegexCharMatcherV1::Digit)),
                'w' => Ok(RegexAstV1::Matcher(RegexCharMatcherV1::Word)),
                's' => Ok(RegexAstV1::Matcher(RegexCharMatcherV1::Space)),
                '.' | '*' | '+' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '^' | '$'
                | '\\' | '-' => Ok(literal_char_ast(escaped, profile)),
                _ => Err(LongPatternErrorV1::InvalidRegex),
            }
        }
        '*' | '+' | '?' | '{' | '}' | '^' | '$' => Err(LongPatternErrorV1::InvalidRegex),
        literal => {
            *pos += 1;
            Ok(literal_char_ast(literal, profile))
        }
    }
}

fn literal_char_ast(ch: char, profile: LongPatternProfileV1) -> RegexAstV1 {
    let mut buf = [0_u8; 4];
    let s = ch.encode_utf8(&mut buf);
    let folded = profile.matched_form(s);
    let mut mapped = folded
        .chars()
        .map(|c| RegexAstV1::Matcher(RegexCharMatcherV1::Exact(c)))
        .collect::<Vec<_>>();
    if mapped.len() == 1 {
        mapped.pop().unwrap_or(RegexAstV1::Empty)
    } else {
        RegexAstV1::Seq(mapped)
    }
}

fn parse_class_char(
    chars: &[char],
    pos: &mut usize,
    profile: LongPatternProfileV1,
) -> Result<char, LongPatternErrorV1> {
    let Some(&ch) = chars.get(*pos) else {
        return Err(LongPatternErrorV1::InvalidRegex);
    };
    *pos += 1;
    let raw = if ch == '\\' {
        let Some(&esc) = chars.get(*pos) else {
            return Err(LongPatternErrorV1::InvalidRegex);
        };
        *pos += 1;
        esc
    } else {
        ch
    };
    let mut buf = [0_u8; 4];
    let folded = profile.matched_form(raw.encode_utf8(&mut buf));
    let mut iter = folded.chars();
    let first = iter.next().ok_or(LongPatternErrorV1::InvalidRegex)?;
    if iter.next().is_some() {
        return Err(LongPatternErrorV1::InvalidRegex);
    }
    Ok(first)
}

fn parse_regex_quantifier(
    chars: &[char],
    pos: &mut usize,
    atom: RegexAstV1,
) -> Result<RegexAstV1, LongPatternErrorV1> {
    match chars.get(*pos) {
        Some('*') => {
            *pos += 1;
            Ok(RegexAstV1::Repeat {
                inner: Box::new(atom),
                min: 0,
                max: None,
            })
        }
        Some('+') => {
            *pos += 1;
            Ok(RegexAstV1::Repeat {
                inner: Box::new(atom),
                min: 1,
                max: None,
            })
        }
        Some('?') => {
            *pos += 1;
            Ok(RegexAstV1::Repeat {
                inner: Box::new(atom),
                min: 0,
                max: Some(1),
            })
        }
        Some('{') => {
            *pos += 1;
            let min = parse_bounded_decimal(chars, pos)?;
            let max = if chars.get(*pos) == Some(&',') {
                *pos += 1;
                if chars.get(*pos) == Some(&'}') {
                    None
                } else {
                    Some(parse_bounded_decimal(chars, pos)?)
                }
            } else {
                Some(min)
            };
            if chars.get(*pos) != Some(&'}') {
                return Err(LongPatternErrorV1::InvalidRegex);
            }
            *pos += 1;
            if min > 32 || max.is_some_and(|m| m < min || m > 32) {
                return Err(LongPatternErrorV1::PatternAtoms);
            }
            Ok(RegexAstV1::Repeat {
                inner: Box::new(atom),
                min,
                max,
            })
        }
        _ => Ok(atom),
    }
}

fn parse_bounded_decimal(chars: &[char], pos: &mut usize) -> Result<usize, LongPatternErrorV1> {
    let start = *pos;
    let mut value = 0_usize;
    while let Some(&ch) = chars.get(*pos) {
        if let Some(digit) = ch.to_digit(10) {
            value = value
                .checked_mul(10)
                .and_then(|v| v.checked_add(digit as usize))
                .ok_or(LongPatternErrorV1::PatternAtoms)?;
            *pos += 1;
        } else {
            break;
        }
    }
    if *pos == start {
        return Err(LongPatternErrorV1::InvalidRegex);
    }
    Ok(value)
}

fn extract_mandatory_grams_from_ast(ast: &RegexAstV1) -> Vec<[u8; 3]> {
    let mut grams = Vec::new();
    match ast {
        RegexAstV1::Seq(items) => {
            let mut current_literal = String::new();
            for item in items {
                match item {
                    RegexAstV1::Matcher(RegexCharMatcherV1::Exact(ch)) => {
                        current_literal.push(*ch);
                    }
                    RegexAstV1::Repeat {
                        inner, min: 1.., ..
                    } => {
                        if let RegexAstV1::Matcher(RegexCharMatcherV1::Exact(ch)) = inner.as_ref() {
                            current_literal.push(*ch);
                            grams.extend(distinct_grams(current_literal.as_bytes()));
                            current_literal.clear();
                        } else {
                            grams.extend(distinct_grams(current_literal.as_bytes()));
                            current_literal.clear();
                            grams.extend(extract_mandatory_grams_from_ast(inner));
                        }
                    }
                    other => {
                        grams.extend(distinct_grams(current_literal.as_bytes()));
                        current_literal.clear();
                        grams.extend(extract_mandatory_grams_from_ast(other));
                    }
                }
            }
            grams.extend(distinct_grams(current_literal.as_bytes()));
        }
        RegexAstV1::Alt(branches) => {
            if let Some((first, rest)) = branches.split_first() {
                let mut common = extract_mandatory_grams_from_ast(first);
                for branch in rest {
                    let branch_grams = extract_mandatory_grams_from_ast(branch);
                    common.retain(|gram| branch_grams.contains(gram));
                }
                grams.extend(common);
            }
        }
        RegexAstV1::Repeat {
            inner, min: 1.., ..
        } => {
            grams.extend(extract_mandatory_grams_from_ast(inner));
        }
        _ => {}
    }
    grams.sort_unstable();
    grams.dedup();
    grams
}

struct ThompsonBuilderV1 {
    states: Vec<ThompsonStateV1>,
}

impl ThompsonBuilderV1 {
    fn push(&mut self, state: ThompsonStateV1) -> Result<usize, LongPatternErrorV1> {
        if self.states.len() >= MAX_REGEX_NFA_STATES_V1 {
            return Err(LongPatternErrorV1::PatternAtoms);
        }
        let id = self.states.len();
        self.states.push(state);
        Ok(id)
    }

    fn compile_node(&mut self, ast: &RegexAstV1, out: usize) -> Result<usize, LongPatternErrorV1> {
        match ast {
            RegexAstV1::Empty => Ok(out),
            RegexAstV1::Matcher(matcher) => self.push(ThompsonStateV1::Match {
                matcher: matcher.clone(),
                next: out,
            }),
            RegexAstV1::Seq(items) => {
                let mut next = out;
                for item in items.iter().rev() {
                    next = self.compile_node(item, next)?;
                }
                Ok(next)
            }
            RegexAstV1::Alt(branches) => {
                let mut iter = branches.iter().rev();
                let Some(last) = iter.next() else {
                    return Ok(out);
                };
                let mut current = self.compile_node(last, out)?;
                for branch in iter {
                    let left = self.compile_node(branch, out)?;
                    current = self.push(ThompsonStateV1::Split {
                        out1: left,
                        out2: current,
                    })?;
                }
                Ok(current)
            }
            RegexAstV1::Repeat { inner, min, max } => {
                let tail = match max {
                    None => {
                        let split_id = self.push(ThompsonStateV1::Jump { target: out })?;
                        let body = self.compile_node(inner, split_id)?;
                        self.states[split_id] = ThompsonStateV1::Split {
                            out1: body,
                            out2: out,
                        };
                        split_id
                    }
                    Some(max_val) => {
                        let mut next = out;
                        for _ in *min..*max_val {
                            let body = self.compile_node(inner, next)?;
                            next = self.push(ThompsonStateV1::Split {
                                out1: body,
                                out2: next,
                            })?;
                        }
                        next
                    }
                };
                let mut current = tail;
                for _ in 0..*min {
                    current = self.compile_node(inner, current)?;
                }
                Ok(current)
            }
        }
    }
}

impl CompiledThompsonNfaV1 {
    /// Linear-time Thompson NFA simulation with zero backtracking (`ADR-0339`).
    fn find_first_span(&self, text: &str) -> Option<(usize, usize)> {
        let char_indices = text.char_indices().collect::<Vec<_>>();
        let max_start = if self.anchored_start {
            0
        } else {
            char_indices.len()
        };
        for start_idx in 0..=max_start {
            let start_byte = char_indices
                .get(start_idx)
                .map_or(text.len(), |(byte_idx, _)| *byte_idx);
            if let Some(end_byte) = self.match_from(&char_indices, start_idx, text.len()) {
                return Some((start_byte, end_byte));
            }
            if self.anchored_start {
                break;
            }
        }
        None
    }

    fn match_from(
        &self,
        char_indices: &[(usize, char)],
        start_idx: usize,
        text_len: usize,
    ) -> Option<usize> {
        let mut current = vec![false; self.states.len()];
        let mut next = vec![false; self.states.len()];
        self.add_epsilon_closure(self.start_state, &mut current);
        let mut last_match_end: Option<usize> = None;
        if current
            .iter()
            .enumerate()
            .any(|(idx, &active)| active && matches!(self.states[idx], ThompsonStateV1::Accept))
            && (!self.anchored_end || start_idx == char_indices.len())
        {
            last_match_end = Some(char_indices.get(start_idx).map_or(text_len, |(b, _)| *b));
        }
        for pos in start_idx..char_indices.len() {
            let ch = char_indices[pos].1;
            next.fill(false);
            for (state_id, &active) in current.iter().enumerate() {
                if !active {
                    continue;
                }
                if let ThompsonStateV1::Match { matcher, next: dst } = &self.states[state_id]
                    && matcher.matches(ch)
                {
                    self.add_epsilon_closure(*dst, &mut next);
                }
            }
            std::mem::swap(&mut current, &mut next);
            if !current.iter().any(|&active| active) {
                break;
            }
            let accepted = current.iter().enumerate().any(|(idx, &active)| {
                active && matches!(self.states[idx], ThompsonStateV1::Accept)
            });
            if accepted && (!self.anchored_end || pos + 1 == char_indices.len()) {
                let end_byte = char_indices.get(pos + 1).map_or(text_len, |(b, _)| *b);
                last_match_end = Some(end_byte);
            }
        }
        last_match_end
    }

    fn add_epsilon_closure(&self, start: usize, active: &mut [bool]) {
        let mut stack = vec![start];
        while let Some(state_id) = stack.pop() {
            if active[state_id] {
                continue;
            }
            active[state_id] = true;
            match &self.states[state_id] {
                ThompsonStateV1::Split { out1, out2 } => {
                    stack.push(*out2);
                    stack.push(*out1);
                }
                ThompsonStateV1::Jump { target } => {
                    stack.push(*target);
                }
                ThompsonStateV1::Match { .. } | ThompsonStateV1::Accept => {}
            }
        }
    }
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
    /// Regular-expression syntax is invalid or unsupported (`ADR-0339`).
    InvalidRegex,
}

// Exhaustive non-durable ownership inventories for exact-provider admission.
crate::inspect_owned_capacity_copy_v1!(LongPatternOperatorV1);

crate::inspect_owned_capacity_copy_v1!(LongPatternProfileV1);

crate::inspect_owned_capacity_copy_v1!(LongPatternBoundsV1);

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
