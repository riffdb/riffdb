//! Independent process-generation entropy for public discovery fences.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

pub(crate) const SERVER_GENERATION_BYTES: usize = 16;

/// One process-local presentation fence sampled during graph activation.
pub(crate) struct ServerGenerationV1([u8; SERVER_GENERATION_BYTES]);

impl ServerGenerationV1 {
    const fn from_bytes(bytes: [u8; SERVER_GENERATION_BYTES]) -> Self {
        Self(bytes)
    }

    #[cfg(test)]
    pub(crate) const fn for_test(bytes: [u8; SERVER_GENERATION_BYTES]) -> Self {
        Self::from_bytes(bytes)
    }

    pub(crate) const fn bytes(&self) -> [u8; SERVER_GENERATION_BYTES] {
        self.0
    }
}

trait ServerGenerationEntropy: Send + Sync {
    fn fill(
        &self,
        destination: &mut [u8; SERVER_GENERATION_BYTES],
    ) -> Result<(), ServerGenerationSourceError>;
}

struct OperatingSystemServerGenerationEntropy;

impl ServerGenerationEntropy for OperatingSystemServerGenerationEntropy {
    fn fill(
        &self,
        destination: &mut [u8; SERVER_GENERATION_BYTES],
    ) -> Result<(), ServerGenerationSourceError> {
        getrandom::fill(destination).map_err(|_| ServerGenerationSourceError)
    }
}

/// The single-purpose source retained by one production graph builder.
pub(crate) struct ProductionServerGenerationSource {
    entropy: Arc<dyn ServerGenerationEntropy>,
}

impl ProductionServerGenerationSource {
    pub(crate) fn new() -> Self {
        Self {
            entropy: Arc::new(OperatingSystemServerGenerationEntropy),
        }
    }

    pub(crate) fn next_generation(
        &self,
    ) -> Result<ServerGenerationV1, ServerGenerationSourceError> {
        let mut bytes = [0_u8; SERVER_GENERATION_BYTES];
        self.entropy.fill(&mut bytes)?;
        Ok(ServerGenerationV1::from_bytes(bytes))
    }
}

impl Default for ProductionServerGenerationSource {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for ProductionServerGenerationSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProductionServerGenerationSource([REDACTED])")
    }
}

/// Closed graph-activation failure before a presentation generation exists.
#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) struct ServerGenerationSourceError;

impl fmt::Debug for ServerGenerationSourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ServerGenerationSourceError([REDACTED])")
    }
}

impl fmt::Display for ServerGenerationSourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("server process generation is unavailable")
    }
}

impl Error for ServerGenerationSourceError {}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    const SOURCE: &str = include_str!("server_generation.rs");

    fn production_source() -> &'static str {
        SOURCE
            .split_once("#[cfg(test)]\nmod tests")
            .expect("server-generation test boundary")
            .0
    }

    struct RecordingEntropy {
        result: Result<[u8; SERVER_GENERATION_BYTES], ServerGenerationSourceError>,
        calls: Mutex<usize>,
    }

    impl ServerGenerationEntropy for RecordingEntropy {
        fn fill(
            &self,
            destination: &mut [u8; SERVER_GENERATION_BYTES],
        ) -> Result<(), ServerGenerationSourceError> {
            *self.calls.lock().expect("generation calls") += 1;
            *destination = self.result?;
            Ok(())
        }
    }

    #[test]
    fn generation_uses_one_independent_complete_fill() {
        let entropy = Arc::new(RecordingEntropy {
            result: Ok([0xa5; SERVER_GENERATION_BYTES]),
            calls: Mutex::new(0),
        });
        let source = ProductionServerGenerationSource {
            entropy: entropy.clone(),
        };

        let generation = source.next_generation().expect("one generation");

        assert_eq!(generation.bytes(), [0xa5; SERVER_GENERATION_BYTES]);
        assert_eq!(*entropy.calls.lock().expect("generation calls"), 1);
    }

    #[test]
    fn entropy_failure_has_no_generation_or_retry() {
        let entropy = Arc::new(RecordingEntropy {
            result: Err(ServerGenerationSourceError),
            calls: Mutex::new(0),
        });
        let source = ProductionServerGenerationSource {
            entropy: entropy.clone(),
        };

        assert!(matches!(
            source.next_generation(),
            Err(ServerGenerationSourceError)
        ));
        assert_eq!(*entropy.calls.lock().expect("generation calls"), 1);
    }

    #[test]
    fn provider_and_error_output_never_expose_entropy() {
        assert_eq!(
            format!("{:?}", ProductionServerGenerationSource::new()),
            "ProductionServerGenerationSource([REDACTED])"
        );
        assert_eq!(
            format!("{ServerGenerationSourceError:?}"),
            "ServerGenerationSourceError([REDACTED])"
        );
        assert_eq!(
            ServerGenerationSourceError.to_string(),
            "server process generation is unavailable"
        );
    }

    #[test]
    fn production_source_has_one_non_uuid_fill_and_no_alternate_source() {
        let source = production_source();
        assert_eq!(source.matches("getrandom::fill(destination)").count(), 1);
        for forbidden in [
            "SystemTime",
            "Instant",
            "Uuid",
            "RequestId",
            "Cursor",
            "thread_rng",
            "rand::",
        ] {
            assert!(
                !source.contains(forbidden),
                "process generation must not use {forbidden}"
            );
        }
        assert!(!source.contains("impl Clone for ServerGenerationV1"));
        assert!(!source.contains("impl Copy for ServerGenerationV1"));
        assert!(!source.contains("impl fmt::Display for ServerGenerationV1"));
        assert!(!source.contains("impl fmt::Debug for ServerGenerationV1"));
    }
}
