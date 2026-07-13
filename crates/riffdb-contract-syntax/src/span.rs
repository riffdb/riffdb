//! Checked source spans using half-open UTF-8 byte offsets.

/// A half-open byte range into one bounded UTF-8 source document.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Span {
    start: u32,
    end: u32,
}

impl Span {
    /// The empty span at the beginning of a source document.
    pub const ZERO: Self = Self { start: 0, end: 0 };

    /// Constructs a checked span from platform-sized offsets.
    #[must_use]
    pub fn new(start: usize, end: usize) -> Option<Self> {
        if start > end {
            return None;
        }
        Some(Self {
            start: u32::try_from(start).ok()?,
            end: u32::try_from(end).ok()?,
        })
    }

    /// Constructs an empty span at a checked byte offset.
    #[must_use]
    pub fn empty(position: usize) -> Option<Self> {
        Self::new(position, position)
    }

    /// Returns the inclusive starting byte offset.
    #[must_use]
    pub const fn start(self) -> u32 {
        self.start
    }

    /// Returns the exclusive ending byte offset.
    #[must_use]
    pub const fn end(self) -> u32 {
        self.end
    }

    /// Returns the byte length of this span.
    #[must_use]
    pub const fn len(self) -> u32 {
        self.end - self.start
    }

    /// Returns whether this span contains no bytes.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.start == self.end
    }

    /// Returns the smallest span containing both inputs.
    #[must_use]
    pub const fn cover(self, other: Self) -> Self {
        Self {
            start: if self.start < other.start {
                self.start
            } else {
                other.start
            },
            end: if self.end > other.end {
                self.end
            } else {
                other.end
            },
        }
    }
}

/// A syntax value paired with its exact source span.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Spanned<T> {
    /// The source-oriented syntax value.
    pub value: T,
    /// The half-open source range that produced the value.
    pub span: Span,
}

impl<T> Spanned<T> {
    /// Pairs a syntax value with its source span.
    #[must_use]
    pub const fn new(value: T, span: Span) -> Self {
        Self { value, span }
    }

    /// Maps the value without changing its source span.
    #[must_use]
    pub fn map<U>(self, map: impl FnOnce(T) -> U) -> Spanned<U> {
        Spanned::new(map(self.value), self.span)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spans_are_checked_half_open_offsets() {
        let span = Span::new(3, 8).expect("valid span");
        assert_eq!((span.start(), span.end(), span.len()), (3, 8, 5));
        assert_eq!(Span::new(8, 3), None);
        assert!(Span::empty(4).expect("valid empty span").is_empty());
    }

    #[test]
    fn spans_reject_large_offsets_and_cover_both_ranges() {
        assert_eq!(
            Span::new(u32::MAX as usize + 1, u32::MAX as usize + 1),
            None
        );

        let left = Span::new(2, 5).expect("valid left span");
        let right = Span::new(8, 13).expect("valid right span");
        assert_eq!(left.cover(right), Span::new(2, 13).expect("valid cover"));
        assert_eq!(right.cover(left), left.cover(right));
    }
}
