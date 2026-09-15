# Errors and Outcomes

RiffDB separates declared business outcomes from public operational errors and
internal defect details.

## Declared outcomes

A command contract lists a closed set of outcomes. Both success and expected
business rejection are ordinary typed command results. Callers branch on the
generated outcome variant; they do not parse an error string.

The terminal outcome is persisted atomically with the command and returned on
an exact idempotent replay.

## Public errors

Public errors use a bounded typed taxonomy for conditions such as invalid
input, unauthenticated or unauthorized access, conflict, incompatible contract,
resource limit, unavailable service, deadline, and unresolved outcome. Details
contain only schema-safe, redacted fields.

`RDB-REP-0101` is the registered follower-mode refusal (`follower_mode` on the
legacy envelope), with `FAILED_PRECONDITION` transport status and
`correct_request` recovery. Follower commands and operations requiring local
authority return this outcome; policy-required durable read audit also refuses.
See the [follower verification](../architecture/WP-747-VERIFICATION.md).
Detectable scoped-token or discovery-fence history mismatches return
`RDB-HISTORY-0101`. Unresolved opaque process-local continuations, including
foreign or obsolete handles, return `RDB-CURSOR-0101` without releasing a page.

MCP invalid arguments return a redacted diagnostic with a JSON Pointer `path`,
a stable `code`, and an `expected` description. It never echoes the rejected
value. See [Application Errors](../getting-started/APPLICATION-ERRORS.md) for
language binding behavior.

## Internal failures

Internal sources and panic details remain in bounded structured tracing with an
incident ID. A public response may carry that incident ID for correlation, but
never a storage error, peer-controlled raw text, credential, or backtrace.

## Unknown outcome

`OutcomeUnknown` means bounded retry and outcome resolution could not establish
the command's terminal result. It is not a declared business outcome and does
not prove the command failed. Retain the same idempotency key and investigate
service availability before making another logical attempt.
