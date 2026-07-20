//! Injected entropy used only outside deterministic command execution.

use std::{error::Error, fmt};

/// A redaction-safe failure to fill an entire entropy destination.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EntropyUnavailable;

impl fmt::Display for EntropyUnavailable {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("operating-system entropy is unavailable")
    }
}

impl Error for EntropyUnavailable {}

/// An injected source that must either fill the complete destination or fail.
pub trait EntropySource: Send + Sync {
    /// Fills the complete destination with cryptographic entropy.
    ///
    /// On failure, callers make no assumption about which bytes were changed.
    fn fill(&self, destination: &mut [u8]) -> Result<(), EntropyUnavailable>;
}

/// The production operating-system entropy source.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemEntropy;

impl EntropySource for SystemEntropy {
    fn fill(&self, destination: &mut [u8]) -> Result<(), EntropyUnavailable> {
        getrandom::fill(destination).map_err(|_| EntropyUnavailable)
    }
}
