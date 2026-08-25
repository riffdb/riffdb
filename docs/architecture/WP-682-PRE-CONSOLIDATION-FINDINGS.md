# WP-682 Pre-consolidation Driver Findings

WP-682 began with one corpus over the existing local-socket driver path and the
existing Python in-process path. This report preserves the comparison made
before consolidation and records how each discrepancy was closed after
ADR-0148 was accepted.

## Authority and scope

ADR-0148 was accepted exactly on 2026-08-25. The first-deliverable corpus landed
before consolidation and named `riffdb-driver-host` V3 as the comparison
authority; implementation then routed the in-process path through that same
core rather than copying its rules into Python.

The corpus currently exercises ordinary and boundary examples for null,
signed and unsigned integers, UUID, bytes, timestamp, decimal, vector, nested
records and lists. For accepted entries the socket path validates a complete
framed `DriverRequest`, lowers its value, and checks the exact normalized
`ApplicationValue` graph and byte material. The Python cell submits the paired
existing bridge value through the installed `_native.validate_bridge_value`
in-process entry point and checks its closed public error class.

## Fixed findings

| Corpus entry | Driver-host V3 rule | Pre-consolidation Python result | Resolution |
|---|---|---|---|
| `host-enum-symbol-bound` | Reject an enum name outside the closed public-symbol alphabet as `invalid_input` | Accepted by the old Python parser | Shared core now rejects it identically |
| `host-string-byte-bound` | Reject a string above 262,144 bytes as `invalid_input` | Accepted under the wider 4 MiB bridge envelope | Shared core now rejects it identically |
| `host-collection-bound` | Reject a list above 4,096 items as `invalid_input` | Accepted under the wider 100,000-value bridge budget | Shared core now rejects it identically |

These findings do not create a storage or command-safety bypass: the shared
Rust client and application service remain authoritative and reject values
that do not satisfy the compiled operation. They are nevertheless real driver
protocol drift. Different bindings admit the same invalid application input at
different boundaries and can return different public error classes, contrary
to DRV-012 and the proposed DRV-015 obligation.

The authoritative correction is complete. Both cells now build the same framed
`DriverRequest` bytes, lower to the same `ApplicationValue` graph, and return
the same closed error class for every corpus entry.

## Reproduction

The authoritative socket cell is green:

```text
cargo +1.97.0 test -p riffdb-driver-host --all-features \
  existing_socket_value_path_matches_the_shared_protocol_corpus
```

The Python cell is now green after building the extension into an isolated
virtual environment:

```text
TMPDIR=<repo>/target/py-test-scratch \
  <venv>/bin/python -m unittest tests.test_protocol_conformance -v
```

No public API, wire version, durable byte, daemon requirement, or generated
artifact changed. The only behavior change is earlier rejection of the three
inputs the authoritative socket driver already rejected.
