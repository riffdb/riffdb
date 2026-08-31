//! ADR-0172: the frozen `unicode_fold_v1` text transform.
//!
//! One implementation, two consumers. The operational index encoding and the
//! exact-text provider both call [`unicode_fold_v1`]; a stored value and a
//! submitted needle are folded by the same function, or matching is not
//! symmetric.
//!
//! The transform is Unicode 17.0.0 NFKC followed by full non-Turkic case
//! folding, in that order. It is frozen: a future Unicode revision becomes
//! `unicode_fold_v2` with its own discriminant and its own rebuild, never an
//! in-place change to this one. The pin is enforced by a test rather than left
//! to a dependency bump, because index contents are a function of it.

use caseless::default_case_fold_str;
use unicode_normalization::UnicodeNormalization;

/// Maximum byte expansion charged for the frozen Unicode-fold profile.
///
/// NFKC decomposition can turn one code point into many, so a folded value can
/// be longer than its source. The compiler charges `byte_bound * this` before a
/// contract deploys, and
/// `unicode_fold::tests::the_expansion_bound_holds_for_every_scalar_value`
/// proves the ratio exhaustively against the pinned tables rather than assuming
/// it.
pub const UNICODE_FOLD_V1_MAXIMUM_EXPANSION: usize = 18;

/// Unicode revision this profile is frozen against.
///
/// ADR-0172 requires the fold to be pinned rather than to follow whatever a
/// dependency happens to provide. A bump that changed this would silently
/// re-fold every deployed text index, so the constant is asserted against the
/// linked tables in `the_pinned_unicode_version_is_exact`.
pub const UNICODE_FOLD_V1_UNICODE_VERSION: (u8, u8, u8) = (17, 0, 0);

/// Applies the frozen `unicode_fold_v1` transform.
///
/// NFKC first, then full case folding. The order is part of the specification:
/// folding before normalizing would leave compatibility forms unfolded in cases
/// where the compatibility mapping itself introduces cased characters.
///
/// The output is not reversible. Callers match on it and must hydrate results
/// from the authoritative record, never from the folded bytes.
#[must_use]
pub fn unicode_fold_v1(value: &str) -> String {
    default_case_fold_str(&value.nfkc().collect::<String>())
}

#[cfg(test)]
mod tests {
    use super::{
        UNICODE_FOLD_V1_MAXIMUM_EXPANSION, UNICODE_FOLD_V1_UNICODE_VERSION, unicode_fold_v1,
    };

    /// The pin is the point. A dependency bump that moved the Unicode revision
    /// would change what every text index contains, which the accepted record
    /// forbids doing silently.
    #[test]
    fn the_pinned_unicode_version_is_exact() {
        assert_eq!(
            unicode_normalization::UNICODE_VERSION,
            UNICODE_FOLD_V1_UNICODE_VERSION,
            "the linked Unicode tables moved; this is a new fold profile, not a bump"
        );
    }

    /// Matching is symmetric only if a value and a needle fold identically, so
    /// the transform must be idempotent: folding folded text changes nothing.
    #[test]
    fn the_transform_is_idempotent() {
        for probe in [
            "Straße",
            "ﬁle",
            "İstanbul",
            "ΣΟΦΟΣ",
            "Ⅻ",
            "ExP",
            "café",
            "cafe\u{0301}",
            "",
            "ß\u{FB03}Σ",
        ] {
            let once = unicode_fold_v1(probe);
            assert_eq!(unicode_fold_v1(&once), once, "not idempotent for {probe:?}");
        }
    }

    /// The specified behaviour, stated as pairs rather than as prose.
    #[test]
    fn the_frozen_conformance_pairs_hold() {
        for (input, expected) in [
            // Expanding fold: one code point becomes two.
            ("Straße", "strasse"),
            // NFKC compatibility: a ligature decomposes.
            ("ﬁle", "file"),
            // Non-Turkic: dotted capital I keeps its combining dot rather than
            // folding to a bare `i`. A Turkic tailoring would differ, and this
            // profile is explicitly not Turkic.
            ("İstanbul", "i\u{307}stanbul"),
            // Final sigma and medial sigma fold together.
            ("ΣΟΦΟΣ", "σοφοσ"),
            ("Σοφος", "σοφοσ"),
            // NFKC compatibility over a numeric form.
            ("Ⅻ", "xii"),
            // Ordinary case folding.
            ("ExP", "exp"),
            // Composed and decomposed forms converge.
            ("café", "café"),
            ("cafe\u{0301}", "café"),
            // Already folded text is unchanged.
            ("plain ascii", "plain ascii"),
            ("", ""),
        ] {
            assert_eq!(
                unicode_fold_v1(input),
                expected,
                "fold pair failed for {input:?}"
            );
        }
    }

    /// Composed and decomposed spellings of one string must match, which is the
    /// property an application actually depends on.
    #[test]
    fn equivalent_spellings_fold_together() {
        for (left, right) in [
            ("café", "cafe\u{0301}"),
            ("ﬁ", "fi"),
            ("Ⅻ", "XII"),
            ("ß", "SS"),
            ("ΣΟΦΟΣ", "σοφος"),
        ] {
            assert_eq!(
                unicode_fold_v1(left),
                unicode_fold_v1(right),
                "{left:?} and {right:?} must fold together"
            );
        }
    }

    /// The compiler charges `byte_bound * UNICODE_FOLD_V1_MAXIMUM_EXPANSION`
    /// before a contract deploys. If a code point expands past that ratio the
    /// charge is wrong and an index could exceed a ceiling the compiler proved
    /// it fit, so the bound is verified against the linked tables rather than
    /// trusted.
    ///
    /// Exhaustive over every scalar value, which is the only honest way to
    /// claim a maximum.
    #[test]
    fn the_expansion_bound_holds_for_every_scalar_value() {
        let mut worst_ratio = 0.0_f64;
        let mut worst_point = '\u{0}';
        for point in (0..=0x10_FFFF_u32).filter_map(char::from_u32) {
            let mut buffer = [0_u8; 4];
            let source = point.encode_utf8(&mut buffer);
            let folded = unicode_fold_v1(source);
            #[allow(clippy::cast_precision_loss)]
            let ratio = folded.len() as f64 / source.len() as f64;
            if ratio > worst_ratio {
                worst_ratio = ratio;
                worst_point = point;
            }
        }
        #[allow(clippy::cast_precision_loss)]
        let charged = UNICODE_FOLD_V1_MAXIMUM_EXPANSION as f64;
        assert!(
            worst_ratio <= charged,
            "U+{:04X} expands {worst_ratio:.2}x, over the charged {charged:.0}x",
            worst_point as u32
        );
    }
}
