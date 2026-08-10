//! Native vector search types.

use std::fmt;
use std::num::NonZeroU32;
use std::time::Duration;

/// The closed set of supported vector distance metrics.
///
/// Each metric defines how similarity is computed between vectors.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum DistanceMetric {
    /// Cosine similarity (1 - cosine_similarity as distance).
    Cosine,
    /// Euclidean (L2) distance.
    Euclidean,
    /// Negative inner (dot) product distance.
    DotProduct,
}

impl DistanceMetric {
    /// Durable format tag for versioned encoding.
    #[must_use]
    pub const fn tag(self) -> u8 {
        match self {
            Self::Cosine => 0x01,
            Self::Euclidean => 0x02,
            Self::DotProduct => 0x03,
        }
    }

    /// Decodes from a durable format tag.
    #[must_use]
    pub const fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0x01 => Some(Self::Cosine),
            0x02 => Some(Self::Euclidean),
            0x03 => Some(Self::DotProduct),
            _ => None,
        }
    }

    /// The source-language keyword for this metric.
    #[must_use]
    pub const fn keyword(self) -> &'static str {
        match self {
            Self::Cosine => "cosine",
            Self::Euclidean => "euclidean",
            Self::DotProduct => "dot_product",
        }
    }
}

impl fmt::Display for DistanceMetric {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.keyword())
    }
}

/// Maximum supported vector dimension (4,096 covers all common embedding models).
pub const MAX_VECTOR_DIMENSION: u32 = 4_096;

/// A validated positive vector dimension.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct VectorDimension(NonZeroU32);

impl VectorDimension {
    /// Creates a validated dimension. Rejects zero and values above `MAX_VECTOR_DIMENSION`.
    #[must_use]
    pub const fn new(value: u32) -> Option<Self> {
        if value == 0 || value > MAX_VECTOR_DIMENSION {
            return None;
        }
        match NonZeroU32::new(value) {
            Some(inner) => Some(Self(inner)),
            None => None,
        }
    }

    /// Returns the raw dimension value.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

impl fmt::Display for VectorDimension {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A validated positive staleness SLO duration.
///
/// The minimum resolution is one second; zero is rejected.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StalenessSlo(Duration);

impl StalenessSlo {
    /// Creates from seconds. Rejects zero.
    #[must_use]
    pub const fn from_secs(secs: u64) -> Option<Self> {
        if secs == 0 {
            return None;
        }
        Some(Self(Duration::from_secs(secs)))
    }

    /// The staleness threshold duration.
    #[must_use]
    pub const fn duration(&self) -> Duration {
        self.0
    }

    /// The staleness threshold in whole seconds.
    #[must_use]
    pub const fn as_secs(&self) -> u64 {
        self.0.as_secs()
    }
}

impl fmt::Display for StalenessSlo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}s", self.0.as_secs())
    }
}
