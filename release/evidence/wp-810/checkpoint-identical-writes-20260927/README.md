# WP-810 byte-identical checkpoint finalization writes

The rejected 8 MiB early-sync strategy remains closed. This candidate retains
all original durability fences and tests whether finalization can avoid
writing bytes the backend already contains.

`redb-reproduction/src/main.rs` is the exact local observation program, using
the already pinned redb 4.2.0 with default features disabled. It uses a real
file and the production 4 MiB cache, forwards **all** original writes, and
checks every row through commit and reopen. `census.jsonl` retains all three
transactions; phase 1 is insertion/replacement and phase 2 is commit. The
result records the private harness manifest/lock digests without introducing
another workspace package. This is a count reproduction, not a timing result.

Production candidate proofs live in the storage adapter's
`checkpoint_rewrite_tests.rs` and `checkpoint_rewrite_engine_tests.rs`.
The existing production checkpoint and coordinator recovery schedules remain
required. Local correctness checks and exact-binary workload qualification
must pass before the fixed counterbalanced comparison begins. WP-810 remains
open; this packet does not claim a shared-tail resolution.
