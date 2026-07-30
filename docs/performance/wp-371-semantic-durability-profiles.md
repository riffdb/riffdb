# WP-371: Semantic Durability Profiles

WP-371 separates RiffDB's application acknowledgement guarantee from redb's
optional two-phase commit mechanism.

## Application guarantee

Both profiles use redb `Durability::Immediate`. RiffDB releases no application
response until the complete authoritative transaction is known committed.
Process-crash tests exercise proven-precommit absence and complete
postcommit recovery for both profiles, followed by exact same-key recovery.

The profiles differ only in the local storage threat model:

- `standard` is the default. It uses redb's checksummed one-phase commit slots
  and assumes a non-Byzantine host, kernel, and storage device.
- `hardened` enables redb's optional two-phase commit defense. It is selected
  with `--redb-commit-profile hardened`,
  `RIFFDB_REDB_COMMIT_PROFILE=hardened`, or
  `server.redb_commit_profile = "hardened"`.

An application command cannot select or downgrade this process-wide profile.
Initialization and migration remain independently hardened.

## Same-run evidence

Command:

```text
./scripts/benchmark-command-growth --assert-perf-009
```

The 2026-07-30 checked run measured the same 128-command, two-transition engine
workload:

| Profile | Elapsed |
|---|---:|
| standard / Immediate one-phase | 6.881 ms |
| hardened / Immediate two-phase | 7.131 ms |

The standard/hardened ratio was 0.9648 on this host, a measured 3.52% reduction.
This is evidence for the mechanism and environment, not a fixed performance
promise. The larger remaining opportunity is terminal admission, which removes
one complete durable transition from a newly executed synchronous command.

The real typed service-audit growth path now uses the standard profile and
remained flat across retained history: 21,048 commands/s at the first window and
18,479 commands/s after 4,096 retained command lifecycles in this run.
