//! Native vector search types.

use std::fmt;
use std::num::NonZeroU32;

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

/// A validated positive stale-entity count threshold.
///
/// V1 compares this threshold directly with the authoritative count of stale
/// entities. Duration-based staleness is reserved for a future amendment.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StaleEntityCountThreshold(NonZeroU32);

impl StaleEntityCountThreshold {
    /// Creates a positive bounded count threshold. Zero is rejected.
    #[must_use]
    pub const fn new(value: u32) -> Option<Self> {
        match NonZeroU32::new(value) {
            Some(inner) => Some(Self(inner)),
            None => None,
        }
    }

    /// Returns the declared stale-entity count threshold.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0.get()
    }

    /// Returns whether an authoritative stale count breaches this threshold.
    #[must_use]
    pub const fn is_breached_by(self, stale_count: u64) -> bool {
        stale_count > self.0.get() as u64
    }
}

impl fmt::Display for StaleEntityCountThreshold {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} stale entities", self.0)
    }
}

/// Typed rejection of an invalid canonical vector construction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CanonicalVectorError {
    /// The component count is zero or exceeds `MAX_VECTOR_DIMENSION`.
    DimensionOutOfRange {
        /// Observed component count.
        actual: usize,
        /// Maximum supported dimension.
        maximum: u32,
    },
    /// A component is NaN or infinite. Vectors carrying non-finite
    /// components are garbage inputs and never become canonical values.
    NonFiniteComponent {
        /// Zero-based index of the first offending component.
        index: usize,
    },
}

impl fmt::Display for CanonicalVectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DimensionOutOfRange { actual, maximum } => write!(
                f,
                "vector has {actual} components; the dimension must be in 1..={maximum}"
            ),
            Self::NonFiniteComponent { index } => {
                write!(f, "vector component {index} is not a finite number")
            }
        }
    }
}

impl std::error::Error for CanonicalVectorError {}

/// A validated, dimension-checked vector of f32 values in canonical form.
///
/// Stored as authoritative entity state. Dimension is fixed at construction
/// and matches the contract-declared `VectorDimension`.
///
/// # Canonical form
///
/// Every construction path (constructor and canonical decoder) enforces one
/// canonical form: all components are finite, and negative zero is
/// canonicalized to positive zero. Within that domain IEEE `f32` equality,
/// bitwise comparison, and bitwise hashing all agree, so the derived
/// `PartialEq`, the bitwise `Hash`/`Ord` below, and the durable canonical
/// digest are mutually consistent (`a == b` implies `digest(a) == digest(b)`).
/// This discipline is what makes vectors a safe exception to the
/// no-business-floats rule in this crate.
#[derive(Clone, PartialEq)]
pub struct CanonicalVector {
    /// The f32 components in declaration order, in canonical form.
    components: Vec<f32>,
}

impl CanonicalVector {
    /// Creates a vector from components, enforcing canonical form.
    ///
    /// Rejects an empty or over-`MAX_VECTOR_DIMENSION` component list and any
    /// NaN or infinite component as typed errors; canonicalizes `-0.0` to
    /// `+0.0`.
    pub fn new(mut components: Vec<f32>) -> Result<Self, CanonicalVectorError> {
        let len = components.len();
        if len == 0 || len > MAX_VECTOR_DIMENSION as usize {
            return Err(CanonicalVectorError::DimensionOutOfRange {
                actual: len,
                maximum: MAX_VECTOR_DIMENSION,
            });
        }
        for (index, component) in components.iter_mut().enumerate() {
            if !component.is_finite() {
                return Err(CanonicalVectorError::NonFiniteComponent { index });
            }
            if *component == 0.0 {
                // Canonicalize -0.0 to +0.0 so IEEE equality and the bitwise
                // Hash/Ord/digest agree on the one canonical representation.
                *component = 0.0;
            }
        }
        Ok(Self { components })
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

// True because construction enforces canonical form: components are finite
// and -0.0 is canonicalized, so IEEE equality is reflexive here and identical
// to bitwise equality.
impl Eq for CanonicalVector {}

// Hash and Ord are bitwise over the canonical form. Within the canonical
// domain bitwise equality coincides with the derived IEEE `PartialEq`, so
// `Hash`/`Ord` are consistent with `Eq`. Note `Ord` is a total order for
// container use (length, then component bits); it is NOT a numeric order —
// vectors have no meaningful numeric order.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_entity_count_threshold_is_positive_and_strict() {
        assert!(StaleEntityCountThreshold::new(0).is_none());
        let threshold = StaleEntityCountThreshold::new(3).expect("positive threshold");
        assert_eq!(threshold.get(), 3);
        assert!(!threshold.is_breached_by(3));
        assert!(threshold.is_breached_by(4));
    }

    #[test]
    fn construction_rejects_non_finite_components_as_typed_errors() {
        for (components, index) in [
            (vec![f32::NAN], 0),
            (vec![1.0, f32::INFINITY], 1),
            (vec![1.0, 2.0, f32::NEG_INFINITY], 2),
        ] {
            assert_eq!(
                CanonicalVector::new(components),
                Err(CanonicalVectorError::NonFiniteComponent { index })
            );
        }
    }

    #[test]
    fn construction_rejects_out_of_range_dimensions() {
        assert_eq!(
            CanonicalVector::new(Vec::new()),
            Err(CanonicalVectorError::DimensionOutOfRange {
                actual: 0,
                maximum: MAX_VECTOR_DIMENSION,
            })
        );
        let oversized = vec![0.5; MAX_VECTOR_DIMENSION as usize + 1];
        assert_eq!(
            CanonicalVector::new(oversized),
            Err(CanonicalVectorError::DimensionOutOfRange {
                actual: MAX_VECTOR_DIMENSION as usize + 1,
                maximum: MAX_VECTOR_DIMENSION,
            })
        );
        assert!(CanonicalVector::new(vec![0.5; MAX_VECTOR_DIMENSION as usize]).is_ok());
    }

    #[test]
    fn negative_zero_is_canonicalized_at_construction() {
        let negative = CanonicalVector::new(vec![-0.0_f32]).expect("finite");
        let positive = CanonicalVector::new(vec![0.0_f32]).expect("finite");
        // One canonical representation: bit-identical components.
        assert_eq!(
            negative.components()[0].to_bits(),
            positive.components()[0].to_bits()
        );
        assert_eq!(negative, positive);
        assert_eq!(negative.cmp(&positive), std::cmp::Ordering::Equal);
        let hash_of = |vector: &CanonicalVector| {
            use std::hash::{Hash, Hasher};
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            vector.hash(&mut hasher);
            hasher.finish()
        };
        assert_eq!(hash_of(&negative), hash_of(&positive));
    }

    #[test]
    fn equality_is_reflexive_and_agrees_with_ordering() {
        let vector = CanonicalVector::new(vec![1.5, -2.25, 0.0]).expect("finite");
        // Reflexivity: with only finite canonical components, IEEE equality
        // is reflexive (a NaN component previously made `v == v` false).
        assert_eq!(vector, vector.clone());
        assert_eq!(vector.cmp(&vector.clone()), std::cmp::Ordering::Equal);
        let other = CanonicalVector::new(vec![1.5, -2.25, 0.5]).expect("finite");
        assert_ne!(vector, other);
        assert_ne!(vector.cmp(&other), std::cmp::Ordering::Equal);
    }
}
