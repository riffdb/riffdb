# WP-637 constant-time named-query frontier

WP-637 restores the allocator-backed application frontier that WP-481 intended
for ordinary named reads. It repairs the production regression introduced by
commit `53d73087`, where the current-view frontier began decoding the retained
command tail on every query. It changes no public query, transport,
authorization, freshness, durability, retention, or comparator shape.

## Decision and safety boundary

Every named query still captures one immutable current, durable, or composite
read view. Frontier resolution now reads the application-sequence allocator
through that same view and maps it as follows:

- initial `Next(1)` means `BeforeFirst`;
- `Next(n)` means the exact predecessor `n - 1`;
- `Exhausted` means `u64::MAX`.

The hot path also checks a command-authority witness without reading a command
payload. Current and durable redb roots check command-table presence. If the
command table is empty, an exact retention watermark must witness a fully
pruned head. Composite roots compare the allocator-derived head with the
published overlay head. Empty/non-empty, allocator/overlay, and fully-pruned
watermark disagreement remains `CorruptData`.

This is validate-once, not validation removal. Startup, recovery, retention,
explicit history reads, backup, replication, and changelog construction still
decode and fully validate command segments. No caller can choose the frontier
path, provide a frontier, skip authorization, or weaken a freshness fence.

The production `QueryExecutionPort` regression reopens the database, activates
the restarted/current operational path, then installs an opaque retained
command value after activation. The named point query succeeds from the exact
allocator frontier while an explicit command-head read rejects the same bytes.
This proves the ordinary query does not decode, checksum, hash, re-encode, or
allocate from retained command payloads. Separate startup and recovery tests
continue to reject corrupt retained history.

## Fixed GetTicket result

The diagnostic remains non-evidentiary and preserves PERF-018's frozen
synchronous generated-client shape. Each post-change cell used 500 measured
requests after 32 warmups. `authority_tail_bytes_max` was zero in every cell.

| host | pre mean | post mean | latency reduction | post throughput | frontier pre | frontier post |
|---|---:|---:|---:|---:|---:|---:|
| workstation | 406 us | 124 us | 69.5% | 8,041/s | 270 us | 2.58 us |
| N1 | 1,912 us | 525 us | 72.5% | 1,898/s | 1,319 us | 9.30 us |
| E2 | 2,728 us | 798 us | 70.8% | 1,248/s | 1,906 us | 14.46 us |

The c=1 throughput gain is 3.27x on the workstation, 3.66x on N1, and
3.41x on E2. All three exceed WP-636's predeclared +100% activation threshold.
The workstation result is 1.44x the separately measured 86 us safe-application
PostgreSQL GetTicket target, down from 4.72x before the repair.

The fixed-read c=32 diagnostic also moved from 21,227/s to 96,459/s on the
workstation. Post-change N1 and E2 reached 15,498/s and 12,356/s respectively.
The server execute stage fell to 11--12 us locally and 40--60 us on cloud
hardware, so the previously dominant CRC/preflight/protobuf/SHA command-tail
stack is absent from the ordinary named-read profile.

## Frozen interactive c=32 result

The public interactive comparator produced a useful scope correction. Its
server remains in the same process after seeding, with a published composite
view. That composite path already obtained its frontier from the in-memory
published overlay before WP-637, so this repair targets a state that the frozen
interactive cell does not enter.

The workstation 30-second, one-repetition diagnostic result was 25,958 ops/s
with zero failures, versus the latest pre-change 28,102 ops/s point. This is
flat within the known short-run/platform variation and does **not** satisfy the
predeclared +35% public-interactive gate. It is not credited as a product gain.
Equivalent post-change cells produced 6,698 ops/s on N1 and 6,563 ops/s on E2,
again with zero failures. Their write p50 values were approximately 23 ms and
20--22 ms respectively. These post-only cloud cells establish the current
ceiling but cannot establish a gain and are not treated as paired evidence.
The result falsifies WP-636's assumption that the fixed-read authority-tail CPU
share was also present in the active post-seed interactive cell. The remaining
interactive ceiling is dominated by the write lane: read p50 is 0.17--0.30 ms,
while its three write shapes are approximately 5.8--6.0 ms p50.

The predecessor c32 fixed-read profile consumed 39.99 core-seconds in a
10-second window (all four configured worker cores). WP-637 removes the
authority-tail work from that restarted/read-only saturation profile. It does
not reinterpret those core-seconds as active-composite work and does not claim
an Amdahl gain for the frozen mixed cell after the direct measurement falsified
that attribution.

The constant-time repair should remain because it removes a deterministic
restart regression, exceeds its c=1 gate on all three machines, and introduces
no seed, write, correctness, or tail regression. A subsequent performance
package must profile the frozen active-composite interactive cell directly;
WP-637 does not silently amend its unmet public-c32 exit criterion.

## Correctness and regression coverage

Automated coverage includes:

- empty, `Next`, and exhausted allocator mapping;
- retained authority bytes that an explicit history read rejects;
- exact fully-pruned retention-watermark authority;
- allocator/authority-presence mismatch refusal;
- an actual restarted/current `QueryExecutionPort` named point query;
- composite-view, retention, startup, backup, changelog, and 86-arm process
  recovery suites;
- unchanged application-baseline correctness and generated-client tests.

The workstation full seed remained below three seconds (2.82 s in the public
c32 run versus 3.00 s in the adjacent pre-change diagnostic), and all measured
workloads reported zero application errors, conflicts, unavailable results, or
idempotency mismatches.

## Receipts

Local receipts live outside the repository under `~/tmp/wp637`; cloud copies
remain under `~/tmp/wp637/results` on their respective VMs. They are
diagnostic-only and do not satisfy PERF-018's 90-second, three-repetition
release-evidence rule.

| receipt | SHA-256 |
|---|---|
| `workstation-post.json` | `d388e1e6897df74e4bec32045b942ca99f54e1c6265e2ebfda60ea0bd0893657` |
| `n1-post.json` | `0ca1102f395ae235987004a4fbd34d03f038379ab81416a26634130c3ff058f9` |
| `e2-post.json` | `e1359ef51fc8de1485d621f740943abc78ff21d4f4db56a818f47b29a677de16` |
| `workstation-interactive-c32.json` | `d126c62ab711822fc771d984a77f87dc70b4aec185c1561c6a6404428f526eaf` |
| `n1-interactive-c32.json` | `c687cf4d62fd52cf0367d6d60ca929510eeba23711f23b24a52710ff721fb10a` |
| `e2-interactive-c32.json` | `4c98ff7378ae354815a9002e1ca76c283566e87828c15b11ec510c06b089e8b7` |
