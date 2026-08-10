//! Versioned execution trace hash chain (`SIM-001` evidence carrier).
//!
//! Every simulated disk operation and every fault decision folds into one
//! running FNV-1a chain. Two runs of the same seed are only accepted as
//! deterministic when their digests are byte-identical, so anything that can
//! vary — operation kind, offset, length, content, fault outcome — must feed
//! the chain. A blind spot here would let the determinism pin pass vacuously.

/// Version of the trace encoding and of the draw order of the fault schedule.
///
/// Bump this constant whenever the event encoding, the set of traced events,
/// or the PRNG consumption order changes: digests are only comparable within
/// one version, and the version itself seeds the chain so cross-version
/// digests never collide silently.
///
/// Version history:
/// - 2: refusal events became self-sufficient — closed-handle refusals fold a
///   dedicated discriminator plus the refused operation kind, and
///   stale-handle, out-of-range, and capacity events fold the full operation
///   parameters (operation kind, offset, length), so the chain's completeness
///   rests on the mechanism rather than on a soundness argument. Digest
///   sensitivity to every fault family is pinned by
///   `tests/trace_sensitivity.rs`.
/// - 1: initial format.
pub const TRACE_FORMAT_VERSION: u32 = 2;

const FNV64_OFFSET_BASIS: u64 = 0xCBF2_9CE4_8422_2325;
const FNV64_PRIME: u64 = 0x0000_0100_0000_01B3;

/// Hashes one byte slice with plain FNV-1a 64 (content digests for trace
/// events; not a running chain).
#[must_use]
pub fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut state = FNV64_OFFSET_BASIS;
    for byte in bytes {
        state ^= u64::from(*byte);
        state = state.wrapping_mul(FNV64_PRIME);
    }
    state
}

/// A running FNV-1a 64 hash chain seeded with [`TRACE_FORMAT_VERSION`].
#[derive(Clone, Debug)]
pub struct TraceHash {
    state: u64,
}

impl TraceHash {
    /// Starts a chain; the format version is the first folded value.
    #[must_use]
    pub fn new() -> Self {
        let mut chain = Self {
            state: FNV64_OFFSET_BASIS,
        };
        chain.fold_u64(u64::from(TRACE_FORMAT_VERSION));
        chain
    }

    /// Folds raw bytes into the chain.
    pub fn fold_bytes(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.state ^= u64::from(*byte);
            self.state = self.state.wrapping_mul(FNV64_PRIME);
        }
    }

    /// Folds one 64-bit value (little-endian) into the chain.
    pub fn fold_u64(&mut self, value: u64) {
        self.fold_bytes(&value.to_le_bytes());
    }

    /// Returns the current chain digest.
    #[must_use]
    pub const fn digest(&self) -> u64 {
        self.state
    }
}

impl Default for TraceHash {
    fn default() -> Self {
        Self::new()
    }
}
