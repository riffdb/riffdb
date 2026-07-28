# Exit-Code Registry v1

| Code | Stable class | Meaning |
|---:|---|---|
| 0 | `result` | Any structurally valid checked public response branch, or a passed demo result, was fully rendered. |
| 1 | `failed` | A checked public/client failure or checked budget-runner/oracle failure was rendered. |
| 2 | `invalid_or_local` | Invocation, configuration, local input, credential retention, rendering, or runner protocol failed. |
| 3 | `uncertain` | A mutation may have completed and the accepted recovery path has not resolved it. |

No other exit code is emitted by a parsed command. Clap help/version exits `0`;
clap grammar failure exits `2`. Those pre-command branches write bounded text to
stderr and no stdout because no complete v1 command identity is available.

For a parsed command in JSON mode, every renderable terminal branch writes
exactly one golden-shaped object plus LF to stdout and nothing to stderr.
`ok:true` always exits `0`. `ok:false` exits according to its error class above.
The only exceptions are checked-output-model overflow and rendering failure:
both exit `2`, leave stdout empty, and write exactly the matching fixed
`.stderr` fixture. They cannot be represented by the renderer that has failed.
For human mode, success writes bounded human text to stdout; failure writes
bounded safe text to stderr. Human output never changes the exit assignment.

Every structurally valid public result oneof exits `0`, including invalid
contract diagnostics; deployment mismatch/conflict; not-found reads;
projection wait/degraded/invalid; bootstrap/capability conflict; normal-create
token unavailability; already-revoked and missing revoke targets; pre-bootstrap,
not-ready, and degraded health. Their exact `status` remains machine-visible.
Only the checked budget-runner/oracle failure is exit `1` without a server
`PublicError`; it is a fixed CLI-local handoff error.

`capability.create` already-created token unavailability is a definitive typed
server result, not exit `3`: the original capability exists but its one-time
token cannot be recovered. Any checked public `outcome_unknown` is exit `3`, as
are the SDK helper uncertainty branches for execute, bootstrap, and normal
create.

Offline maintenance start uncertainty is also exit `3` and includes only the
caller-stable maintenance operation ID plus the exact
`poll_maintenance_operation` recovery action. The operator resolves it with
`riffdb backup operation <maintenance-operation-id>`; the CLI never emits a
filesystem path.
