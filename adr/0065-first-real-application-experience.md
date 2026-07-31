# ADR-0065: First Real Application Experience

- **Status:** Accepted
- **Date:** 2026-07-30
- **Decision owners:** RiffDB maintainers
- **Related requirements:** `DX-001` through `DX-012`
- **Related work packages:** `WP-386` through `WP-392`
- **Amends:** ADR-0041, ADR-0055, ADR-0056, ADR-0057, ADR-0062, ADR-0063

## Context

The first installed multi-application dogfood proved the symbolic application
model but required manual server-configuration edits, source-tree searches for
JSON encodings, repeated deployment commands, manually supplied query
identities, and separate undocumented MCP authority. Several failures retained
safe behavior but were too generic for an operator or agent to correct.

The maintainer approved the complete First Real Application Experience plan on
2026-07-30, including offline database addition for user and system installs,
lock-driven deployment with optional explicit role provisioning, and database
identity on every public health and active-contract response.

## Decision

### Offline database addition

The source installer may add one canonical alias to an installer-owned
configuration while the service is stopped. This is configuration evolution,
not runtime database administration. It must preserve an existing legacy
database as alias `default`, migrate its complete backup root to the generated
sibling layout, use durable private phase state, publish configuration
atomically, and resume after interruption. Custom or ambiguous configuration,
active maintenance, unsafe filesystem state, and any path overlap fail before
publication. Runtime create, attach, detach, and drop remain unsupported.

Startup and configuration-check diagnostics may expose stable error codes,
database aliases, and path roles. They must not expose absolute paths,
credentials, submitted values, arbitrary engine prose, or internal sources.

### Natural symbolic values

Application JSON is interpreted against the selected compiled schema. Ordinary
JSON integers, strings, arrays, objects, booleans, and null are resolved to the
declared type before execution. Fixed-scale decimals and money use exact
strings; UUIDs and enum variants use public strings; timestamps use their
closed object shape. Existing tagged values remain compatibility inputs.
Kernel-style fully typed records remain confined to the explicit kernel
command surface.

Application validation failures resolve numeric compiler paths to authorized
contract symbols before release. They may report the expected public type and
submitted JSON kind, but never a submitted value or hidden schema.

### Locked deployment and authority

`riffdb application deploy` accepts only a compiler-owned exact application
lock. It verifies every artifact before remote mutation and executes the
existing contract deployment, query-module deployment, capability, and command
batch operations. It does not create a new cross-operation transaction.
Instead, it retains bounded deployment state and makes every completed stage
idempotently verifiable and resumable.

Role provisioning is explicit. It derives only the selected manifest role,
creates a short-lived application capability, retains its capability ID, and
writes separate application client and MCP configuration. Seed execution uses
that application credential through ordinary commands, never operator or
contract-author authority. Credential replacement is explicit and requires
the retained old identity.

An omitted named-query hash selects only the active query module for the
selected active or exact contract. Explicit identities continue to pin
historical artifacts. Absence of an active module and absence of the requested
query are distinct symbolic failures.

### MCP and selected database identity

Successful health and active-contract responses report the canonical selected
database alias through gRPC, CLI, SDK, hosted MCP, and stdio MCP. Authenticated
health also reports the configured public capability audience. These values
are routing/configuration facts and never enter durable identities.

An application-role MCP connection discovers only its authorized compiled
commands and deployed named queries as typed tools. The bootstrap MCP
developer credential remains limited to authoring/deployment control-plane
operations. No combined credential is created.

The stdio bridge provides an explicit doctor operation and distinguishes EOF
before initialization from a protocol-service defect. Behavior of an external
MCP host's already-running session remains outside RiffDB.

### Authoring reference

The public kit is generated from authoritative registries and documents
reserved contract words, exact name scopes, supported line comments, natural
JSON, fixed RiffQL page bounds, installed deployment order, and the
two-credential MCP model. This record does not broaden contract grammar,
RiffQL, joins, mutation authority, or storage access.

## Compatibility

The database alias and audience response fields are additive pre-alpha public
protocol changes with regenerated fixtures. Natural application JSON is an
acceptance expansion; existing tagged inputs remain valid. The database-add
command changes no durable record or `DatabaseId`. Locked application
deployment composes existing authoritative operations and persistent formats.

## Security

All convenience paths fail closed, remain bounded, and use the same service,
authorization, compiler, coordinator, and storage boundaries. Database
migration is offline and journaled. Public diagnostics contain only checked
symbols and closed classifications. Application and authoring credentials stay
separate, and no operation infers or widens a role.

## Requirements and Work Packages

- **Requirements:** `DX-001` through `DX-012`
- **Interface freeze:** `WP-386`
- **Implementation:** `WP-387` through `WP-391`
- **Final installed proof:** `WP-392`
