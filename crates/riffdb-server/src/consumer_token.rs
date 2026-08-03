//! Production entropy boundary for durable consumer attempt tokens.

use riffdb_service::{EventLeaseTokenSource, EventLeaseTokenSourceError};
use riffdb_types::EventLeaseToken;

pub(crate) struct ProductionEventLeaseTokenSource;

impl EventLeaseTokenSource for ProductionEventLeaseTokenSource {
    fn generate(&self) -> Result<EventLeaseToken, EventLeaseTokenSourceError> {
        let mut bytes = [0_u8; 32];
        getrandom::fill(&mut bytes).map_err(|_| EventLeaseTokenSourceError)?;
        Ok(EventLeaseToken::from_bytes(bytes))
    }
}

impl std::fmt::Debug for ProductionEventLeaseTokenSource {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ProductionEventLeaseTokenSource([REDACTED])")
    }
}
