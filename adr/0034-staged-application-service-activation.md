# ADR-0034: Staged Application-Service Activation

- **Status:** Accepted
- **Direction approved:** 2026-07-21
- **Exact text accepted:** 2026-07-21
- **Accepted:** 2026-07-21
- **Requires:** ADR-0004, ADR-0007, ADR-0019, ADR-0025, ADR-0032
- **Amends:** ADR-0007 pre-bootstrap Health composition
- **Decision deadline:** Before WP-130 composes the production listener

The human maintainer accepted this exact decision and its authoritative-file
amendments on 2026-07-21.

## Context

The P1 process must bind the unchanged Health RPC while structural and catalog
history validation is still running. During that phase it may expose only the
restricted principal-less `InitializingValidation` result. The complete
`RiffDbService`, however, currently requires command executors and all semantic
providers. Those capabilities cannot exist until the exact structural session
has reached its end, catalog has validated that same session, and dormant
storage ports have been activated.

Starting the listener only after validation would omit required initializing
Health. Activating dormant ports early would break the startup proof. Returning
Health directly from gRPC would bypass the API-neutral service. Supplying fake
providers would manufacture authority during the most security-sensitive
lifecycle phase.

## Decision

`riffdb-service` owns a two-stage construction boundary:

```rust
pub struct InitializingRiffDbService { /* Health admission only */ }
pub struct RiffDbServiceActivator { /* move-only activation authority */ }

impl RiffDbService {
    pub fn begin_initialization() -> (
        InitializingRiffDbService,
        RiffDbServiceActivator,
        PreBootstrapHealthContextIssuer,
    );
}

impl InitializingRiffDbService {
    pub fn health(
        &self,
        context: PreBootstrapHealthContext,
        request: HealthRequest,
    ) -> ServiceFuture<'_, HealthResult>;
}

impl RiffDbServiceActivator {
    pub fn activate(
        self,
        identity: ServiceIdentity,
        process: ServiceProcessMetadata,
        executors: ServiceExecutors,
        providers: ServiceProviders,
    ) -> RiffDbService;
}
```

The three values returned by `begin_initialization` share one exact
`PreBootstrapHealthAdmission`. The initializing type carries no identity,
executor, storage, catalog, policy, audit, token, projection, outbox, cursor, or
operational capability. It has one public operation: restricted Health.

`activate` consumes the sole activation capability and installs dependencies
that WP-130 has already validated. It carries the existing admission into the
complete service; it does not create or reopen admission. Existing
`RiffDbService::compose` remains compatibility sugar implemented through
`begin_initialization` followed by `activate`.

The WP-130 lifecycle router initially targets `InitializingRiffDbService` and
atomically replaces that target with the complete service only after the
startup proofs join. Before submitting a structurally valid bootstrap request,
the router stops new bootstrap admission and closes the health issuer. Already
issued contexts fail the same acquire/release admission check after closure.
Known bootstrap success transitions to authenticated `DeploymentRequired`;
uncertain bootstrap completion stops routing and requires recovery.

No generic initialization application trait is added. No non-Health method is
implemented by the initializing type.

## Options Considered

1. **Health-only initializing type plus move-only activator:** Proposed. It
   makes unavailable authority unrepresentable and preserves one Health path.
2. **Start gRPC only after validation:** Rejected. Required initializing Health
   would not exist.
3. **Construct the full service with placeholders:** Rejected. It invents
   callable authority and makes readiness dependent on runtime convention.
4. **Return pre-bootstrap Health from the transport:** Rejected. It creates a
   privileged service bypass.
5. **Add a shared Health supertrait implemented by both stages:** Rejected for
   P1. It changes all application fakes and consumers without adding a stronger
   invariant than the narrow inherent endpoint.

## Consequences

- WP-120 gains a focused construction interface; operation semantics and its
  six public application traits do not change.
- WP-130 can bind the real listener before storage activation without holding a
  storage or policy capability in the initializing route.
- Dropping or closing the issuer revokes both initializing and activated
  pre-bootstrap contexts.
- The change is internal Rust API only. It changes no public Protobuf, durable
  record, hash, command, audit, or readiness definition.

## Testing

WP-120 tests freeze the Health-only method inventory, exact three-field result,
absence of provider access and durable audit, shared admission identity,
activation handoff, and closure of already-issued contexts. WP-130 lifecycle
tests exercise listener-before-validation, target replacement, close-vs-Health,
bootstrap success, and uncertain bootstrap shutdown without sleeps.

## Requirements and Work Packages

- **Requirements:** `SYS-004`, `API-001`, `POC-008`, `REC-002`
- **Corrects:** `WP-120`
- **Blocks:** `WP-130`
- **Final evidence:** `WP-130` and `WP-200`
