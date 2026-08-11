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

/// A validated, dimension-checked vector of f32 values.
///
/// Stored as authoritative entity state. Dimension is fixed at construction
/// and matches the contract-declared `VectorDimension`.
#[derive(Clone, PartialEq)]
pub struct CanonicalVector {
    /// The f32 components in declaration order.
    components: Vec<f32>,
}

impl CanonicalVector {
    /// Creates a vector from components. Returns `None` if the length is zero
    /// or exceeds `MAX_VECTOR_DIMENSION`.
    #[must_use]
    pub fn new(components: Vec<f32>) -> Option<Self> {
        let len = components.len();
        if len == 0 || len > MAX_VECTOR_DIMENSION as usize {
            return None;
        }
        Some(Self { components })
    }

    /// The number of components (the dimension).
    #[must_use]
    pub fn dimension(&self) -> u32 {
        self.components.len() as u32
    }

    /// The raw f32 components.
    #[must_use]
    pub fn components(&self) -> &[f32] {
        &self.components
    }

    /// Consumes self and returns the owned components.
    #[must_use]
    pub fn into_components(self) -> Vec<f32> {
        self.components
    }

    /// Byte size for budget accounting (4 bytes per f32 component).
    #[must_use]
    pub fn byte_size(&self) -> usize {
        self.components.len() * 4
    }
}

impl fmt::Debug for CanonicalVector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CanonicalVector(dim={},[REDACTED])", self.dimension())
    }
}

impl Eq for CanonicalVector {}

// f32 does not implement Ord, but we need it for CanonicalValue's derived traits.
// Vector equality uses bitwise comparison (same as IEEE 754 totalOrder for non-NaN).
impl std::hash::Hash for CanonicalVector {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for component in &self.components {
            state.write_u32(component.to_bits());
        }
    }
}

impl PartialOrd for CanonicalVector {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CanonicalVector {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.components
            .len()
            .cmp(&other.components.len())
            .then_with(|| {
                for (a, b) in self.components.iter().zip(other.components.iter()) {
                    let ordering = a.to_bits().cmp(&b.to_bits());
                    if ordering != std::cmp::Ordering::Equal {
                        return ordering;
                    }
                }
                std::cmp::Ordering::Equal
            })
    }
}

/// Metadata about an embedding write for staleness and model-version tracking.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EmbeddingMetadata {
    /// The model identity string (e.g. "text-embedding-3-small").
    model_identity: String,
    /// The model version string (e.g. "2024-01-25").
    model_version: String,
}

impl EmbeddingMetadata {
    /// Maximum length for model identity and version strings.
    pub const MAX_MODEL_STRING_LEN: usize = 256;

    /// Creates validated embedding metadata.
    #[must_use]
    pub fn new(
        model_identity: impl Into<String>,
        model_version: impl Into<String>,
    ) -> Option<Self> {
        let model_identity = model_identity.into();
        let model_version = model_version.into();
        if model_identity.is_empty()
            || model_identity.len() > Self::MAX_MODEL_STRING_LEN
            || model_version.is_empty()
            || model_version.len() > Self::MAX_MODEL_STRING_LEN
        {
            return None;
        }
        Some(Self {
            model_identity,
            model_version,
        })
    }

    /// The model identity.
    #[must_use]
    pub fn model_identity(&self) -> &str {
        &self.model_identity
    }

    /// The model version.
    #[must_use]
    pub fn model_version(&self) -> &str {
        &self.model_version
    }
}
