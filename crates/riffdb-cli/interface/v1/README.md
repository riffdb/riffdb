# RiffDB CLI v1 Interface Checkpoint

Status: **Accepted and implemented by WP-150, additively extended by WP-155
and WP-554.**

This directory freezes the accepted interface implemented by WP-150. It is
deliberately outside `src/`: no file here is production Rust and no runtime
behavior is authorized by this checkpoint alone.

The checkpoint consists of:

- `command-grammar.rs.txt`: the complete clap-shaped command and argument
  grammar for the 15 `riffdb.cli.output/v1` command identities;
- `configuration.md` and the `client-*.toml` fixtures: configuration sources,
  precedence, bounds, and a complete valid example;
- `exit-codes.md`: the complete process-exit registry and stream rules;
- `dto-declarations.rs.txt`: closed CLI input, result, and error declarations,
  including key order and omission rules;
- `../../fixtures/output-v1/*.jsonl`: exact compact LF-terminated machine
  stdout bytes for every renderable terminal branch;
- `../../fixtures/output-v1/*.stderr`: exact bounded LF-terminated emergency
  stderr bytes for output-model overflow and rendering failure; and
- `../../fixtures/output-v1/manifest.tsv`: the command, terminal branch,
  stream, exit code, byte count, and SHA-256 for every golden; and
- `checkpoint-inventory-v1.tsv`: exact repository-relative path, byte count,
  and plain SHA-256 for every other checkpoint artifact.

`../verify-checkpoint --check` deterministically rebuilds the manifest in the
accepted 15-command order, parses each JSONL with `jq`, and checks both exact
inventories, stream, LF, compact JSON, envelope/key-order, exit-code,
byte-count, and SHA-256 rules. It adds no Cargo dependency and executes no CLI
or database behavior. `--print-manifest` and `--print-inventory` print the
respective regenerated TSV without modifying the repository.

The accepted ADR-0041 envelope and scalar rules apply unchanged. Every JSONL
fixture contains exactly one JSON object and one final LF. The two `.stderr`
fixtures contain one fixed safe line and one final LF; stdout is empty for
those branches as SPEC Section 16.2 requires. Human output has the same checked
terminal model but is explicitly not a compatibility interface.

## Deliberate v1 Choices

1. Raw-key `command outcome` requires `--lineage`; the abbreviated example in
   SPEC Section 16.6 does not carry enough identity to construct the accepted
   public request. Locator lookup instead accepts only `--outcome-uri`.
2. `entity get` accepts the public stable numeric entity type ID and a canonical
   padded-base64 entity key. The public SDK currently exposes no generic,
   schema-directed entity-name/key-component builder. WP-150 must not invent a
   Budget-only encoder inside the CLI.
3. `projection query` accepts the public stable numeric projection ID. Its
   input document contains only the ordered `leading_components` Value list;
   consistency and paging controls remain explicit flags.
4. Every structurally valid public response branch is an `ok:true` result and
   exits `0`. Contract-invalid, not-found, mismatch, conflict, projection
   availability, and token-unavailable states remain typed data; the CLI does
   not reinterpret contract-first response semantics as transport errors.
5. Explicit unresolved mutation disposition exits `3`. No retained local file
   is treated as proof of server success.

Changing any command name, flag, positional argument, DTO field/order/presence,
error code/message, exit assignment, or golden byte after acceptance requires
another compatibility review.

WP-554 adds the `server.health.process_alive` success fixture. It is the
payload-free, database-blind unauthenticated liveness branch accepted by
ADR-0105; authenticated readiness fixtures retain their existing meaning.
