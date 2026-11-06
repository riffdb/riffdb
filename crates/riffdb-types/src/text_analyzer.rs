//! ADR-0173's closed, versioned tokenized-text analyzers.
//!
//! Analyzer output becomes durable index identity. Both profiles are therefore
//! frozen functions: changing tokenization or normalization adds a new profile
//! and rebuilds the index; it never changes either v1 implementation in place.

use unicode_segmentation::{UnicodeSegmentation, UnicodeWords};

use crate::unicode_fold_v1;

/// Unicode revision frozen by both `standard_v1` segmentation and folding.
pub const TEXT_ANALYZER_V1_UNICODE_VERSION: (u64, u64, u64) = (17, 0, 0);
/// Immutable semantic schema digest of tokenized provider-state V1.
pub const TOKENIZED_TEXT_PROVIDER_STATE_SCHEMA_HASH_V1: [u8; 32] = [
    0x8e, 0x32, 0x7e, 0x0e, 0x53, 0xa3, 0x01, 0xe0, 0x3c, 0xe3, 0xbe, 0x04, 0xec, 0xe5, 0x7c, 0x3a,
    0x94, 0x87, 0x83, 0x94, 0x9a, 0xc1, 0x01, 0x3d, 0x52, 0x16, 0x7f, 0x88, 0xd1, 0xe4, 0xd3, 0x0c,
];

/// Closed tokenized-text analyzer vocabulary for ADR-0173 v1 and ADR-0339.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum TextAnalyzerV1 {
    /// Preserve the complete source value as exactly one term.
    KeywordV1 = 1,
    /// Unicode 17.0.0 UAX #29 word segmentation, then `unicode_fold_v1`.
    StandardV1 = 2,
    /// Unicode 17.0.0 UAX #29 word segmentation, `unicode_fold_v1`, then deterministic Porter2 English stemming (`ADR-0339`).
    EnglishStemV1 = 3,
}

/// Closed result model admitted by the declaration stage.
///
/// Ranking is a later separately shippable stage. Keeping the model explicit
/// prevents that stage from silently changing an existing index identity.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum TextSearchResultModelV1 {
    /// Exact boolean membership with no relevance ordering.
    BooleanV1 = 1,
}

impl TextSearchResultModelV1 {
    /// Canonical contract spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BooleanV1 => "boolean_v1",
        }
    }
}

impl TextAnalyzerV1 {
    /// Decodes the byte-frozen analyzer discriminant.
    #[must_use]
    pub const fn from_discriminant(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::KeywordV1),
            2 => Some(Self::StandardV1),
            3 => Some(Self::EnglishStemV1),
            _ => None,
        }
    }

    /// Canonical contract spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::KeywordV1 => "keyword_v1",
            Self::StandardV1 => "standard_v1",
            Self::EnglishStemV1 => "english_stem_v1",
        }
    }

    /// Analyzes one already-bounded source value without retaining the result.
    ///
    /// `keyword_v1` emits once even for the empty string. `standard_v1`
    /// segments before folding, as ADR-0173 specifies, and assigns consecutive
    /// zero-based positions to emitted words. `english_stem_v1` additionally
    /// applies deterministic Porter2 English suffix stripping (`ADR-0339`).
    #[must_use]
    pub fn analyze(self, value: &str) -> TextAnalyzerTermsV1<'_> {
        match self {
            Self::KeywordV1 => TextAnalyzerTermsV1 {
                inner: TextAnalyzerTermsInner::Keyword(Some(value)),
            },
            Self::StandardV1 => TextAnalyzerTermsV1 {
                inner: TextAnalyzerTermsInner::Standard(value.unicode_words().enumerate()),
            },
            Self::EnglishStemV1 => TextAnalyzerTermsV1 {
                inner: TextAnalyzerTermsInner::EnglishStem(value.unicode_words().enumerate()),
            },
        }
    }
}

/// Returns `(byte_offset, word_slice)` pairs using frozen Unicode 17.0.0 UAX #29 word segmentation (`ADR-0339`).
#[must_use]
pub fn unicode_word_spans_v1(value: &str) -> Vec<(usize, &str)> {
    value.unicode_word_indices().collect()
}

/// Deterministic, locale-independent Porter2 English stemmer operating on an
/// already `unicode_fold_v1`-normalized word (`ADR-0339`).
#[must_use]
pub fn english_stem_v1(word: &str) -> String {
    if !word.is_ascii() || word.len() <= 2 {
        return word.to_owned();
    }
    match word {
        "skis" => return "ski".to_owned(),
        "skies" => return "sky".to_owned(),
        "dying" => return "die".to_owned(),
        "lying" => return "lie".to_owned(),
        "tying" => return "tie".to_owned(),
        "idly" => return "idl".to_owned(),
        "gently" => return "gentl".to_owned(),
        "ugly" => return "ugli".to_owned(),
        "early" => return "earli".to_owned(),
        "only" => return "onli".to_owned(),
        "singly" => return "singl".to_owned(),
        "sky" | "news" | "howe" | "atlas" | "cosmos" | "bias" | "andes" => {
            return word.to_owned();
        }
        _ => {}
    }
    let mut w = word.to_owned();
    if let Some(stripped) = w.strip_prefix('\'') {
        w = stripped.to_owned();
    }
    for suffix in ["'s'", "'s", "'"] {
        if w.ends_with(suffix) {
            w.truncate(w.len() - suffix.len());
            break;
        }
    }
    if w.len() <= 2 {
        return w;
    }

    // Step 1a: plural suffixes
    if w.ends_with("sses") {
        w.truncate(w.len() - 2);
    } else if w.ends_with("ied") || w.ends_with("ies") {
        let base_len = w.len() - 3;
        w.truncate(base_len);
        if base_len > 1 {
            w.push('i');
        } else {
            w.push_str("ie");
        }
    } else if w.ends_with("us") || w.ends_with("ss") {
        // unchanged
    } else if w.ends_with('s') && w.len() > 2 {
        let before_last = &w.as_bytes()[..w.len() - 2];
        if before_last.iter().copied().any(is_porter_vowel) {
            w.pop();
        }
    }

    if matches!(
        w.as_str(),
        "inning" | "outing" | "canning" | "herring" | "earring" | "proceed" | "exceed" | "succeed"
    ) {
        return w;
    }

    let r1 = porter_r1(&w);

    // Step 1b: -eed, -eedly, -ed, -edly, -ing, -ingly
    let mut step1b_stripped = false;
    if let Some(suffix) = ["eedly", "eed"].into_iter().find(|s| w.ends_with(s)) {
        if w.len() - suffix.len() >= r1 {
            w.truncate(w.len() - suffix.len());
            w.push_str("ee");
        }
    } else if let Some(suffix) = ["ingly", "edly", "ing", "ed"]
        .into_iter()
        .find(|s| w.ends_with(s))
    {
        let stem_len = w.len() - suffix.len();
        if w.as_bytes()[..stem_len]
            .iter()
            .copied()
            .any(is_porter_vowel)
        {
            w.truncate(stem_len);
            step1b_stripped = true;
        }
    }

    if step1b_stripped {
        if w.ends_with("at") || w.ends_with("bl") || w.ends_with("iz") {
            w.push('e');
        } else if ends_with_porter_double(w.as_bytes()) {
            w.pop();
        } else if is_short_porter_word(&w, porter_r1(&w)) {
            w.push('e');
        }
    }

    // Step 1c: trailing y -> i if preceded by non-vowel not at index 0
    if w.len() > 2 && w.ends_with('y') {
        let prev = w.as_bytes()[w.len() - 2];
        if !is_porter_vowel(prev) {
            w.pop();
            w.push('i');
        }
    }

    let r1 = porter_r1(&w);
    // Step 2: R1 suffix replacements
    const STEP2_RULES: &[(&str, &str)] = &[
        ("ational", "ate"),
        ("fulness", "ful"),
        ("ousness", "ous"),
        ("iveness", "ive"),
        ("ization", "ize"),
        ("tional", "tion"),
        ("biliti", "ble"),
        ("lessli", "less"),
        ("entli", "ent"),
        ("ation", "ate"),
        ("alism", "al"),
        ("aliti", "al"),
        ("ousli", "ous"),
        ("iviti", "ive"),
        ("fulli", "ful"),
        ("enci", "ence"),
        ("anci", "ance"),
        ("abli", "able"),
        ("izer", "ize"),
        ("ator", "ate"),
        ("alli", "al"),
        ("bli", "ble"),
    ];
    for &(suffix, replacement) in STEP2_RULES {
        if w.ends_with(suffix) {
            if w.len() - suffix.len() >= r1 {
                w.truncate(w.len() - suffix.len());
                w.push_str(replacement);
            }
            break;
        }
    }

    let r1 = porter_r1(&w);
    let r2 = porter_r2(&w, r1);
    // Step 3: R1/R2 suffix replacements
    const STEP3_RULES: &[(&str, &str, bool)] = &[
        ("ational", "ate", false),
        ("tional", "tion", false),
        ("alize", "al", false),
        ("icate", "ic", false),
        ("iciti", "ic", false),
        ("ative", "", true),
        ("ical", "ic", false),
        ("ness", "", false),
        ("ful", "", false),
    ];
    for &(suffix, replacement, require_r2) in STEP3_RULES {
        if w.ends_with(suffix) {
            let bound = if require_r2 { r2 } else { r1 };
            if w.len() - suffix.len() >= bound {
                w.truncate(w.len() - suffix.len());
                w.push_str(replacement);
            }
            break;
        }
    }

    let r1 = porter_r1(&w);
    let r2 = porter_r2(&w, r1);
    // Step 4: R2 suffix removals
    const STEP4_SUFFIXES: &[&str] = &[
        "ement", "ance", "ence", "able", "ible", "ment", "ant", "ent", "ism", "ate", "iti", "ous",
        "ive", "ize", "al", "er", "ic",
    ];
    let mut step4_matched = false;
    for &suffix in STEP4_SUFFIXES {
        if w.ends_with(suffix) {
            if w.len() - suffix.len() >= r2 {
                w.truncate(w.len() - suffix.len());
            }
            step4_matched = true;
            break;
        }
    }
    if !step4_matched && w.ends_with("ion") {
        let stem_len = w.len() - 3;
        if stem_len >= r2 && stem_len > 0 && matches!(w.as_bytes()[stem_len - 1], b's' | b't') {
            w.truncate(stem_len);
        }
    }

    let r1 = porter_r1(&w);
    let r2 = porter_r2(&w, r1);
    // Step 5: final -e and -ll cleanup
    if w.ends_with('e') {
        let stem_len = w.len() - 1;
        if stem_len >= r2
            || (stem_len >= r1 && !ends_with_short_syllable(&w.as_bytes()[..stem_len]))
        {
            w.pop();
        }
    } else if w.ends_with("ll") && w.len() > r2 {
        w.pop();
    }

    w
}

const fn is_porter_vowel(b: u8) -> bool {
    matches!(b, b'a' | b'e' | b'i' | b'o' | b'u' | b'y')
}

const fn ends_with_porter_double(bytes: &[u8]) -> bool {
    if bytes.len() < 2 {
        return false;
    }
    let last = bytes[bytes.len() - 1];
    let prev = bytes[bytes.len() - 2];
    last == prev
        && matches!(
            last,
            b'b' | b'd' | b'f' | b'g' | b'm' | b'n' | b'p' | b'r' | b't'
        )
}

fn porter_r1(word: &str) -> usize {
    for prefix in ["gener", "commun", "arsen"] {
        if word.starts_with(prefix) {
            return prefix.len();
        }
    }
    let bytes = word.as_bytes();
    let mut seen_vowel = false;
    for (idx, &b) in bytes.iter().enumerate() {
        if is_porter_vowel(b) {
            seen_vowel = true;
        } else if seen_vowel {
            return idx + 1;
        }
    }
    bytes.len()
}

fn porter_r2(word: &str, r1: usize) -> usize {
    let bytes = word.as_bytes();
    if r1 >= bytes.len() {
        return bytes.len();
    }
    let mut seen_vowel = false;
    for (offset, &b) in bytes[r1..].iter().enumerate() {
        if is_porter_vowel(b) {
            seen_vowel = true;
        } else if seen_vowel {
            return r1 + offset + 1;
        }
    }
    bytes.len()
}

fn ends_with_short_syllable(bytes: &[u8]) -> bool {
    if bytes.len() == 2 {
        return is_porter_vowel(bytes[0]) && !is_porter_vowel(bytes[1]);
    }
    if bytes.len() >= 3 {
        let n = bytes.len();
        let c1 = bytes[n - 3];
        let v = bytes[n - 2];
        let c2 = bytes[n - 1];
        return !is_porter_vowel(c1)
            && is_porter_vowel(v)
            && !is_porter_vowel(c2)
            && !matches!(c2, b'w' | b'x' | b'y');
    }
    false
}

fn is_short_porter_word(word: &str, r1: usize) -> bool {
    r1 >= word.len() && ends_with_short_syllable(word.as_bytes())
}

/// One analyzed term and its zero-based token position.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnalyzedTermV1 {
    position: usize,
    term: String,
}

impl AnalyzedTermV1 {
    /// Zero-based token position in analyzer output.
    #[must_use]
    pub const fn position(&self) -> usize {
        self.position
    }

    /// Exact analyzed term stored in postings.
    #[must_use]
    pub fn term(&self) -> &str {
        &self.term
    }
}

/// Streaming analyzed terms for one source value.
#[derive(Debug)]
pub struct TextAnalyzerTermsV1<'a> {
    inner: TextAnalyzerTermsInner<'a>,
}

#[derive(Debug)]
enum TextAnalyzerTermsInner<'a> {
    Keyword(Option<&'a str>),
    Standard(std::iter::Enumerate<UnicodeWords<'a>>),
    EnglishStem(std::iter::Enumerate<UnicodeWords<'a>>),
}

impl Iterator for TextAnalyzerTermsV1<'_> {
    type Item = AnalyzedTermV1;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.inner {
            TextAnalyzerTermsInner::Keyword(value) => value.take().map(|value| AnalyzedTermV1 {
                position: 0,
                term: value.to_owned(),
            }),
            TextAnalyzerTermsInner::Standard(words) => {
                words.next().map(|(position, word)| AnalyzedTermV1 {
                    position,
                    term: unicode_fold_v1(word),
                })
            }
            TextAnalyzerTermsInner::EnglishStem(words) => words.next().map(|(position, word)| {
                let folded = unicode_fold_v1(word);
                AnalyzedTermV1 {
                    position,
                    term: english_stem_v1(&folded),
                }
            }),
        }
    }
}

// Exhaustive non-durable ownership inventories for exact-provider admission.
crate::inspect_owned_capacity_copy_v1!(TextAnalyzerV1);

#[cfg(test)]
mod tests {
    use super::TEXT_ANALYZER_V1_UNICODE_VERSION;

    #[test]
    fn segmentation_and_folding_use_the_same_unicode_revision() {
        assert_eq!(
            unicode_segmentation::UNICODE_VERSION,
            TEXT_ANALYZER_V1_UNICODE_VERSION,
            "tokenizer tables moved; add a new analyzer identity instead"
        );
        assert_eq!(
            crate::UNICODE_FOLD_V1_UNICODE_VERSION,
            (17, 0, 0),
            "fold and tokenizer revisions must remain aligned"
        );
    }
}

crate::inspect_owned_capacity_copy_v1!(TextSearchResultModelV1);
