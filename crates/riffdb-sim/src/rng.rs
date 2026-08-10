//! Hand-rolled seeded PRNG so the simulator never links an entropy crate.

/// The splitmix64 generator (Steele, Lea, Flood; public domain reference
/// constants). Deterministic, allocation-free, and good enough to spread a
/// 64-bit seed across fault schedules and workload draws.
#[derive(Clone, Debug)]
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    /// Creates a generator whose entire future output is fixed by `seed`.
    #[must_use]
    pub const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Returns the next 64-bit draw.
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Returns a draw in `0..bound`. `bound` must be nonzero. The modulo bias
    /// is negligible for simulation bounds and, critically, deterministic.
    pub fn next_below(&mut self, bound: u64) -> u64 {
        assert!(bound > 0, "next_below requires a nonzero bound");
        self.next_u64() % bound
    }

    /// Returns `true` with probability `numerator / denominator`. A zero
    /// denominator never fires and consumes no draw, so disabled fault arms
    /// do not perturb the seed stream of enabled ones.
    pub fn chance(&mut self, numerator: u64, denominator: u64) -> bool {
        if denominator == 0 {
            return false;
        }
        self.next_below(denominator) < numerator
    }
}
