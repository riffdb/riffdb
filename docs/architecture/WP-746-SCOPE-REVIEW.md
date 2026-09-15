# WP-746 scope review

Status: approved by the maintainer in session on 2026-09-14: “i approve the ammendment”.
The path-only amendment is committed as `96968c96`. WP-746 remains open.

The sections below record the original review. Commit `aa33c150` subsequently
made `allowed_paths` crate scope rather than a consequence-file inventory.
The current implementation passes that checker; consequence files need no
additional scope amendment. The current implementation and verification
checkpoint is in [the lifecycle review](WP-746-FOLLOWER-LIFECYCLE-REVIEW.md).

## Original required behavior and missing ownership

REP-002 and accepted ADR-0178 section 2 require a typed `RDB-REP`
follower-mode application refusal on every command, administration, migration,
maintenance, and export surface. The public error types are closed and owned
by `crates/riffdb-errors/src/lib.rs`; neither `ApplicationErrorCode` nor
`PublicErrorKind` represents this refusal. That crate is outside WP-746's
allowed paths. Defining a separate transport error would not satisfy REP-002.

The new regression test
`error::tests::follower_mode_refusal_is_a_typed_application_error` in
`crates/riffdb-api-grpc/src/error.rs` checks the registry-owned refusal,
failed-precondition status, safe message, and exact application-envelope
round trip. It selects `RDB-REP-0101` as the proposed first replication code.
This is a wire-error test, not evidence of follower activation or write refusal
coverage across all surfaces.

The ownership trace also reaches the Rust client's exhaustive application
status mapping, MCP's exhaustive error fixture constructor, and the generated
TypeScript error registries. TypeScript rejects unknown error codes, so its
generator and generated clients must retain the typed refusal too.

## Approved separate scope commit

Added the following entries to WP-746's `allowed_paths` in
`work_packages.yaml`, before any implementation uses them:

```yaml
    crates/riffdb-errors/**,
    crates/riffdb-client-rust/src/status.rs,
    crates/riffdb-api-mcp/src/presentation.rs,
    crates/riffdb-query-module/src/generation.rs,
    templates/generators/typescript/client.ts.j2,
    clients/typescript/ticketdesk/client.ts,
    examples/ticketdesk/web/src/generated/client.ts,
    examples/app-baseline/typescript/src/ticketdesk-client.ts,
    examples/agent-alpha/domains/agent-blog/generated/typescript/client.ts,
    examples/agent-alpha/domains/agent-orders/generated/typescript/client.ts,
    examples/agent-alpha/web/src/generated/client.ts,
```

Existing allowed paths already cover the service, gRPC and Protobuf mappings,
wire schemas, fixtures, tests, scripts, and handbook updates. Generated clients
will be regenerated from their sources. The extension is limited to representing
and checking the accepted replication refusals; it adds no follower behavior
beyond ADR-0178/ADR-0186 and changes no durable encoding.

`./scripts/check-allowed-paths --wp WP-746` with the explicit error, Rust-client,
and MCP file paths rejects all three as outside the package. The checker reads
scope from the range's base commit and explicitly requires a separate scope
commit; the implementation cannot expand its own scope in the same range.

## Why review is requested

The supplied AGENTS.md says to stop and request human review when:

> A work package needs paths outside its declared scope.

`docs/CONTRIBUTING.md` says a separate scope commit needs no approval. The
explicit instructions supplied for this task take precedence; review was requested before widening scope. The maintainer has now approved
the extension above.

## Original continuation

The path amendment is committed separately. Implement the
accepted public refusal in its existing owner, and continue all WP-746
deliverables: administrative capability, publication and stream, staged
bootstrap and retention fence, sole-writer follower activation/apply, complete
startup validation and rebuild, real workload and process crash proofs,
generated artifacts, handbook updates, acceptance and closure.

## Original verification of this review change

On 2026-09-14, `./scripts/acceptance --wp WP-746 --range HEAD..HEAD`
completed with six passing steps and one failing step. Allowed paths,
formatting, scoped Clippy, handbook, file-size guard, and panic-allowance checks
passed. The scoped gRPC suite ran 135 tests: 134 passed and the new
follower-refusal test failed on the missing registry code. The direct targeted
test reproduced the same failure. Requirement coverage and `git diff --check`
also passed. The full WP-746 exit gate and `ci-all` have not been satisfied.

Package: WP-746 (guarantee). This review change adds a failing wire-error
regression and documents the scope needed to implement accepted behavior.
Production behavior and compatibility are unchanged. The updated handbook
pages are this scope review and its `docs/SUMMARY.md` entry. Remaining
hazards and follow-ups are the missing shared refusal and all activation,
bootstrap, apply, and crash-proof work listed above.
