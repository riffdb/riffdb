//! Bounded, transport-local MCP admission rate limiting.

use std::{
    collections::BTreeMap,
    error::Error,
    fmt,
    net::IpAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use riffdb_types::{ActorId, ServiceOperationV1, TenantScope};

use crate::McpTransportKind;

const CREDIT_SCALE: u128 = 1_000_000_000;

/// Maximum total pre- and post-authentication bucket count.
pub const MAX_MCP_RATE_BUCKETS: usize = 4_096;
/// Minimum inactivity required before a bucket may be removed.
pub const MCP_RATE_BUCKET_IDLE_EXPIRY: Duration = Duration::from_secs(300);
/// Accepted maximum pre-authentication burst.
pub const MCP_PRE_AUTH_MAX_BURST: u32 = 32;
/// Accepted maximum pre-authentication refill per second.
pub const MCP_PRE_AUTH_MAX_REFILL_PER_SECOND: u32 = 8;
/// Accepted maximum post-authentication burst.
pub const MCP_POST_AUTH_MAX_BURST: u32 = 16;
/// Accepted maximum post-authentication refill per second.
pub const MCP_POST_AUTH_MAX_REFILL_PER_SECOND: u32 = 4;
/// Maximum compiler-owned dynamic tool-name bytes retained in one key.
pub const MAX_MCP_RATE_TARGET_BYTES: usize = 128;

/// Injected monotonic source used only by MCP transport admission.
pub trait McpRateClock: Send + Sync {
    /// Returns elapsed time from one process-local origin.
    fn now(&self) -> Result<Duration, McpRateClockError>;
}

/// Closed failure to sample the transport rate clock.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct McpRateClockError;

impl fmt::Display for McpRateClockError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MCP rate clock is unavailable")
    }
}

impl Error for McpRateClockError {}

/// Process-local production source for transport admission time.
pub struct SystemMcpRateClock {
    origin: Instant,
}

impl SystemMcpRateClock {
    /// Starts one independent monotonic timeline.
    #[must_use]
    pub fn new() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}

impl Default for SystemMcpRateClock {
    fn default() -> Self {
        Self::new()
    }
}

impl McpRateClock for SystemMcpRateClock {
    fn now(&self) -> Result<Duration, McpRateClockError> {
        Ok(self.origin.elapsed())
    }
}

impl fmt::Debug for SystemMcpRateClock {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SystemMcpRateClock")
    }
}

/// A bounded post-authentication operation or exact dynamic tool target.
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
pub enum McpRateTarget {
    /// One closed API-neutral service operation.
    Service(ServiceOperationV1),
    /// One compiler-owned ADR-0020 dynamic command tool name.
    CommandTool(String),
}

pub(crate) trait McpPostAuthenticationLimiter: Send + Sync {
    fn admit(
        &self,
        principal: ActorId,
        tenant: TenantScope,
        target: McpRateTarget,
    ) -> Result<(), McpRateLimitError>;
}

impl<C> McpPostAuthenticationLimiter for McpRateLimiter<C>
where
    C: McpRateClock + 'static,
{
    fn admit(
        &self,
        principal: ActorId,
        tenant: TenantScope,
        target: McpRateTarget,
    ) -> Result<(), McpRateLimitError> {
        self.admit_hosted_post_authentication(principal, tenant, target)
    }
}

/// Hosted-only process-local capability for exact post-authentication admission.
///
/// The authenticated actor and policy-resolved tenant remain encapsulated. A
/// hosted backend retains this value only in one operation invocation and calls
/// [`Self::admit`] immediately before each underlying protected service call.
#[derive(Clone)]
pub struct McpPostAuthenticationAdmission {
    limiter: Arc<dyn McpPostAuthenticationLimiter>,
    principal: ActorId,
    tenant: TenantScope,
}

impl McpPostAuthenticationAdmission {
    #[cfg_attr(not(feature = "streamable-http"), allow(dead_code))]
    pub(crate) fn new_hosted(
        limiter: Arc<dyn McpPostAuthenticationLimiter>,
        principal: ActorId,
        tenant: TenantScope,
    ) -> Self {
        Self {
            limiter,
            principal,
            tenant,
        }
    }

    /// Applies the exact hosted source/principal/tenant/target token bucket.
    pub fn admit(&self, target: McpRateTarget) -> Result<(), McpRateLimitError> {
        self.limiter
            .admit(self.principal.clone(), self.tenant.clone(), target)
    }
}

impl fmt::Debug for McpPostAuthenticationAdmission {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpPostAuthenticationAdmission([REDACTED])")
    }
}

impl McpRateTarget {
    /// Checks one compiler-owned name without normalizing or rederiving it.
    pub fn command_tool(name: impl Into<String>) -> Result<Self, McpRateLimitError> {
        let name = name.into();
        if name.is_empty()
            || name.len() > MAX_MCP_RATE_TARGET_BYTES
            || !name.bytes().all(|byte| (0x21..=0x7e).contains(&byte))
        {
            return Err(McpRateLimitError::InvalidKey);
        }
        Ok(Self::CommandTool(name))
    }
}

impl fmt::Debug for McpRateTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Service(operation) => formatter.debug_tuple("Service").field(operation).finish(),
            Self::CommandTool(_) => formatter.write_str("CommandTool([REDACTED])"),
        }
    }
}

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
enum McpRateKey {
    PreAuthentication(IpAddr),
    PostAuthentication {
        source: McpTransportKind,
        principal: ActorId,
        tenant: TenantScope,
        target: McpRateTarget,
    },
}

impl fmt::Debug for McpRateKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpRateKey([REDACTED])")
    }
}

/// Lowerable rate configuration whose ceilings are fixed by ADR-0008.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct McpRateLimitConfig {
    pre_auth_burst: u32,
    pre_auth_refill_per_second: u32,
    post_auth_burst: u32,
    post_auth_refill_per_second: u32,
}

impl McpRateLimitConfig {
    /// Creates one nonzero configuration at or below every accepted ceiling.
    pub const fn new(
        pre_auth_burst: u32,
        pre_auth_refill_per_second: u32,
        post_auth_burst: u32,
        post_auth_refill_per_second: u32,
    ) -> Result<Self, McpRateLimitError> {
        if pre_auth_burst == 0
            || pre_auth_burst > MCP_PRE_AUTH_MAX_BURST
            || pre_auth_refill_per_second == 0
            || pre_auth_refill_per_second > MCP_PRE_AUTH_MAX_REFILL_PER_SECOND
            || post_auth_burst == 0
            || post_auth_burst > MCP_POST_AUTH_MAX_BURST
            || post_auth_refill_per_second == 0
            || post_auth_refill_per_second > MCP_POST_AUTH_MAX_REFILL_PER_SECOND
        {
            return Err(McpRateLimitError::InvalidConfiguration);
        }
        Ok(Self {
            pre_auth_burst,
            pre_auth_refill_per_second,
            post_auth_burst,
            post_auth_refill_per_second,
        })
    }

    /// Returns the accepted POC defaults.
    #[must_use]
    pub const fn poc_default() -> Self {
        Self {
            pre_auth_burst: MCP_PRE_AUTH_MAX_BURST,
            pre_auth_refill_per_second: MCP_PRE_AUTH_MAX_REFILL_PER_SECOND,
            post_auth_burst: MCP_POST_AUTH_MAX_BURST,
            post_auth_refill_per_second: MCP_POST_AUTH_MAX_REFILL_PER_SECOND,
        }
    }
}

struct Bucket {
    credit: u128,
    last_refill: Duration,
    last_seen: Duration,
}

#[derive(Default)]
struct Registry {
    buckets: BTreeMap<McpRateKey, Bucket>,
    last_observed_time: Option<Duration>,
}

/// One bounded limiter shared by hosted HTTP sessions in a server process.
pub struct McpRateLimiter<C> {
    clock: C,
    config: McpRateLimitConfig,
    registry: Mutex<Registry>,
}

impl<C> McpRateLimiter<C>
where
    C: McpRateClock,
{
    /// Creates an empty limiter with fixed-ceiling configuration.
    #[must_use]
    pub const fn new(clock: C, config: McpRateLimitConfig) -> Self {
        Self {
            clock,
            config,
            registry: Mutex::new(Registry {
                buckets: BTreeMap::new(),
                last_observed_time: None,
            }),
        }
    }

    /// Applies the socket-peer-IP admission check before authentication.
    pub fn admit_pre_authentication(
        &self,
        trusted_peer_ip: IpAddr,
    ) -> Result<(), McpRateLimitError> {
        self.admit(
            McpRateKey::PreAuthentication(trusted_peer_ip),
            self.config.pre_auth_burst,
            self.config.pre_auth_refill_per_second,
        )
    }

    /// Applies the exact hosted authenticated principal/tenant/target check.
    pub fn admit_hosted_post_authentication(
        &self,
        principal: ActorId,
        tenant: TenantScope,
        target: McpRateTarget,
    ) -> Result<(), McpRateLimitError> {
        self.admit(
            McpRateKey::PostAuthentication {
                source: McpTransportKind::StreamableHttp,
                principal,
                tenant,
                target,
            },
            self.config.post_auth_burst,
            self.config.post_auth_refill_per_second,
        )
    }

    fn admit(
        &self,
        key: McpRateKey,
        burst: u32,
        refill_per_second: u32,
    ) -> Result<(), McpRateLimitError> {
        let now = self
            .clock
            .now()
            .map_err(|_| McpRateLimitError::ClockUnavailable)?;
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if registry
            .last_observed_time
            .is_some_and(|previous| now < previous)
        {
            return Err(McpRateLimitError::ClockUnavailable);
        }
        registry.last_observed_time = Some(now);

        if !registry.buckets.contains_key(&key) {
            if registry.buckets.len() >= MAX_MCP_RATE_BUCKETS {
                registry
                    .buckets
                    .retain(|_, bucket| !bucket_is_expired(now, bucket));
            }
            if registry.buckets.len() >= MAX_MCP_RATE_BUCKETS {
                return Err(McpRateLimitError::CapacityExhausted);
            }
            registry.buckets.insert(
                key.clone(),
                Bucket {
                    credit: u128::from(burst)
                        .checked_mul(CREDIT_SCALE)
                        .ok_or(McpRateLimitError::ClockUnavailable)?,
                    last_refill: now,
                    last_seen: now,
                },
            );
        }

        let bucket = registry
            .buckets
            .get_mut(&key)
            .ok_or(McpRateLimitError::CapacityExhausted)?;
        refill_bucket(bucket, now, burst, refill_per_second)?;
        bucket.last_seen = now;
        if bucket.credit < CREDIT_SCALE {
            return Err(McpRateLimitError::RateLimited);
        }
        bucket.credit -= CREDIT_SCALE;
        Ok(())
    }

    #[cfg(test)]
    fn bucket_count(&self) -> usize {
        self.registry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .buckets
            .len()
    }
}

impl<C> fmt::Debug for McpRateLimiter<C> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("McpRateLimiter([REDACTED])")
    }
}

/// Closed admission failure without key, principal, tenant, or peer data.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum McpRateLimitError {
    /// A configured value would exceed the accepted limits.
    InvalidConfiguration,
    /// An untrusted dynamic target was not bounded canonical visible ASCII.
    InvalidKey,
    /// The injected monotonic clock failed, regressed, or overflowed.
    ClockUnavailable,
    /// The bucket has no complete token.
    RateLimited,
    /// No idle bucket could be removed at the hard registry bound.
    CapacityExhausted,
}

impl fmt::Display for McpRateLimitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MCP request admission was denied")
    }
}

impl Error for McpRateLimitError {}

fn refill_bucket(
    bucket: &mut Bucket,
    now: Duration,
    burst: u32,
    refill_per_second: u32,
) -> Result<(), McpRateLimitError> {
    let elapsed = now
        .checked_sub(bucket.last_refill)
        .ok_or(McpRateLimitError::ClockUnavailable)?;
    let added = elapsed
        .as_nanos()
        .checked_mul(u128::from(refill_per_second))
        .ok_or(McpRateLimitError::ClockUnavailable)?;
    let capacity = u128::from(burst)
        .checked_mul(CREDIT_SCALE)
        .ok_or(McpRateLimitError::ClockUnavailable)?;
    bucket.credit = bucket
        .credit
        .checked_add(added)
        .ok_or(McpRateLimitError::ClockUnavailable)?
        .min(capacity);
    bucket.last_refill = now;
    Ok(())
}

fn bucket_is_expired(now: Duration, bucket: &Bucket) -> bool {
    now.checked_sub(bucket.last_seen)
        .is_some_and(|idle| idle >= MCP_RATE_BUCKET_IDLE_EXPIRY)
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    };

    use super::*;

    #[derive(Clone)]
    struct TestClock(Arc<AtomicU64>);

    impl TestClock {
        fn new(nanos: u64) -> Self {
            Self(Arc::new(AtomicU64::new(nanos)))
        }

        fn set(&self, nanos: u64) {
            self.0.store(nanos, Ordering::SeqCst);
        }
    }

    impl McpRateClock for TestClock {
        fn now(&self) -> Result<Duration, McpRateClockError> {
            Ok(Duration::from_nanos(self.0.load(Ordering::SeqCst)))
        }
    }

    fn principal(value: &str) -> ActorId {
        ActorId::new(value).expect("bounded actor")
    }

    fn target() -> McpRateTarget {
        McpRateTarget::Service(ServiceOperationV1::GetEntity)
    }

    #[test]
    fn configuration_can_lower_but_never_raise_a_ceiling() {
        assert!(McpRateLimitConfig::new(1, 1, 1, 1).is_ok());
        for values in [
            (0, 1, 1, 1),
            (33, 1, 1, 1),
            (1, 0, 1, 1),
            (1, 9, 1, 1),
            (1, 1, 0, 1),
            (1, 1, 17, 1),
            (1, 1, 1, 0),
            (1, 1, 1, 5),
        ] {
            assert_eq!(
                McpRateLimitConfig::new(values.0, values.1, values.2, values.3),
                Err(McpRateLimitError::InvalidConfiguration)
            );
        }
    }

    #[test]
    fn exact_burst_and_integer_refill_are_deterministic() {
        let clock = TestClock::new(0);
        let limiter = McpRateLimiter::new(
            clock.clone(),
            McpRateLimitConfig::new(2, 2, 1, 1).expect("lowered configuration"),
        );
        let peer = IpAddr::from([127, 0, 0, 1]);
        assert_eq!(limiter.admit_pre_authentication(peer), Ok(()));
        assert_eq!(limiter.admit_pre_authentication(peer), Ok(()));
        assert_eq!(
            limiter.admit_pre_authentication(peer),
            Err(McpRateLimitError::RateLimited)
        );
        clock.set(499_999_999);
        assert_eq!(
            limiter.admit_pre_authentication(peer),
            Err(McpRateLimitError::RateLimited)
        );
        clock.set(500_000_000);
        assert_eq!(limiter.admit_pre_authentication(peer), Ok(()));
    }

    #[test]
    fn every_hosted_post_authentication_key_component_separates_buckets() {
        let clock = TestClock::new(0);
        let limiter = McpRateLimiter::new(
            clock,
            McpRateLimitConfig::new(1, 1, 1, 1).expect("lowered configuration"),
        );
        let cases = [
            (principal("one"), TenantScope::Global, target()),
            (principal("two"), TenantScope::Global, target()),
            (
                principal("one"),
                TenantScope::Tenant(riffdb_types::TenantId::new("tenant").expect("bounded tenant")),
                target(),
            ),
            (
                principal("one"),
                TenantScope::Global,
                McpRateTarget::Service(ServiceOperationV1::GetCommit),
            ),
        ];
        for (principal, tenant, target) in cases {
            assert_eq!(
                limiter.admit_hosted_post_authentication(principal, tenant, target),
                Ok(())
            );
        }
        assert_eq!(limiter.bucket_count(), 4);
    }

    #[test]
    fn hosted_admission_capability_is_exact_bounded_and_redacted() {
        let clock = TestClock::new(0);
        let limiter = Arc::new(McpRateLimiter::new(
            clock,
            McpRateLimitConfig::new(1, 1, 1, 1).expect("lowered configuration"),
        ));
        let erased: Arc<dyn McpPostAuthenticationLimiter> = limiter;
        let admission = McpPostAuthenticationAdmission::new_hosted(
            erased,
            principal("operator"),
            TenantScope::Global,
        );

        assert_eq!(admission.admit(target()), Ok(()));
        assert_eq!(
            admission.admit(target()),
            Err(McpRateLimitError::RateLimited)
        );
        assert_eq!(
            admission.admit(McpRateTarget::Service(ServiceOperationV1::GetCommit)),
            Ok(())
        );
        assert_eq!(
            format!("{admission:?}"),
            "McpPostAuthenticationAdmission([REDACTED])"
        );
    }

    #[test]
    fn capacity_removes_only_idle_buckets_and_never_evicts_active_ones() {
        let clock = TestClock::new(0);
        let limiter = McpRateLimiter::new(clock.clone(), McpRateLimitConfig::poc_default());
        for suffix in 0..MAX_MCP_RATE_BUCKETS {
            let ip = IpAddr::from([
                10,
                ((suffix >> 16) & 0xff) as u8,
                ((suffix >> 8) & 0xff) as u8,
                (suffix & 0xff) as u8,
            ]);
            assert_eq!(limiter.admit_pre_authentication(ip), Ok(()));
        }
        assert_eq!(
            limiter.admit_pre_authentication(IpAddr::from([127, 0, 0, 1])),
            Err(McpRateLimitError::CapacityExhausted)
        );
        clock.set(MCP_RATE_BUCKET_IDLE_EXPIRY.as_nanos() as u64);
        assert_eq!(
            limiter.admit_pre_authentication(IpAddr::from([127, 0, 0, 1])),
            Ok(())
        );
        assert_eq!(limiter.bucket_count(), 1);
    }

    #[test]
    fn monotonic_regression_fails_closed_without_consuming_credit() {
        let clock = TestClock::new(2_000_000_000);
        let limiter = McpRateLimiter::new(
            clock.clone(),
            McpRateLimitConfig::new(1, 1, 1, 1).expect("lowered configuration"),
        );
        let peer = IpAddr::from([127, 0, 0, 1]);
        assert_eq!(limiter.admit_pre_authentication(peer), Ok(()));
        clock.set(1_000_000_000);
        assert_eq!(
            limiter.admit_pre_authentication(peer),
            Err(McpRateLimitError::ClockUnavailable)
        );
        clock.set(3_000_000_000);
        assert_eq!(limiter.admit_pre_authentication(peer), Ok(()));
    }

    #[test]
    fn dynamic_target_is_bounded_exact_visible_ascii_and_redacted() {
        let target =
            McpRateTarget::command_tool("riffdb.cmd.contract.command").expect("bounded name");
        assert_eq!(format!("{target:?}"), "CommandTool([REDACTED])");
        for value in ["", "contains space", "\n", &"x".repeat(129)] {
            assert_eq!(
                McpRateTarget::command_tool(value),
                Err(McpRateLimitError::InvalidKey)
            );
        }
        assert_eq!(
            McpRateLimitError::RateLimited.to_string(),
            "MCP request admission was denied"
        );
    }
}
