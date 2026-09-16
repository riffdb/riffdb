# WP-749 host observation before a new paired run

Package: WP-749 (open). Tier: surface (release evidence).
Observed 2026-09-16 after preparing revision `700265a8`.
No workload cell was launched and no performance result was produced.

The host-visible observation at the sampler's standard three-second interval
returned exit 3 / `valid: false`: unrelated processes exceeded the existing
5%-of-one-logical-CPU interference threshold, and process churn was observed.
The earlier 100-ms diagnostic sample also refused. No threshold was changed,
no unrelated process was stopped, and no failing workload was retried.

A separate bounded namespace observation found two visible processes with
`codex` as PID 1 in the ordinary tool sandbox, versus 1,125 visible processes
with `systemd` as PID 1 outside that sandbox. Namespace identifiers differed.
A sampler invoked in a restricted process namespace cannot establish absence
of interference from processes it cannot see. The two observations are not a
simultaneous CPU comparison; they establish the visibility difference.

The prior `local-paired-20260916` reports have empty active-process inventories
and do not record namespace visibility. Their successful sampler flags cannot
establish host-wide absence of interference. Preserve their measurements,
correctness results and raw flags as reported, but do not use them to claim
qualified latency, causal isolation or a passed no-regression gate. The actual
interference during those earlier cells cannot be reconstructed from this later
observation. Prior raw JSON, logs and execution receipts remain unchanged.

## Collection and reproduction

Host-visible commands used the existing `scripts/app-baseline-host-validity`
with `--interval-ms 100`, then its unchanged default interval. Both returned 3.
The namespace observations read only `/proc/self/ns/pid`, `/proc/1/comm`, and
counted numeric `/proc` entries with a 4,096-entry bound. No command arguments
or environment values were collected. `prepared-identities.json` records the
clean source and copied release binaries ready for a future run; it is not an
execution receipt.

Run `sha256sum --check SHA256SUMS` in this directory to verify retained bytes.
Future paired runs must use host-visible pre/postflight process observations,
retain all outputs, and keep the fixed workload and existing interference and
stability gates. The required cloud campaign and package closure remain open.
