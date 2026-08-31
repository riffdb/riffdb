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

/// Closed tokenized-text analyzer vocabulary for ADR-0173 v1.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(u8)]
pub enum TextAnalyzerV1 {
    /// Preserve the complete source value as exactly one term.
    KeywordV1 = 1,
    /// Unicode 17.0.0 UAX #29 word segmentation, then `unicode_fold_v1`.
    StandardV1 = 2,
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
            _ => None,
        }
    }

    /// Canonical contract spelling.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::KeywordV1 => "keyword_v1",
            Self::StandardV1 => "standard_v1",
        }
    }

    /// Analyzes one already-bounded source value without retaining the result.
    ///
    /// `keyword_v1` emits once even for the empty string. `standard_v1`
    /// segments before folding, as ADR-0173 specifies, and assigns consecutive
    /// zero-based positions to emitted words.
    #[must_use]
    pub fn analyze(self, value: &str) -> TextAnalyzerTermsV1<'_> {
        match self {
            Self::KeywordV1 => TextAnalyzerTermsV1 {
                inner: TextAnalyzerTermsInner::Keyword(Some(value)),
            },
            Self::StandardV1 => TextAnalyzerTermsV1 {
                inner: TextAnalyzerTermsInner::Standard(value.unicode_words().enumerate()),
            },
        }
    }
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
        }
    }
}

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
