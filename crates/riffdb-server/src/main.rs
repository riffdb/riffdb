// This binary root is the only compilation unit in the workspace below
// `forbid`, because the global allocator selection at the bottom of this file
// cannot be permitted under it. The `riffdb-server` library re-forbids itself,
// so the relaxation covers exactly this file and exactly one item.
#![deny(unsafe_code)]

//! Hosted `riffdbd` server process.

// The allocator is a measured choice, not a preference: on the C3D bench host
// jemalloc raised write throughput by 2.5% at one client and 13-16% at 8 to 128
// concurrent clients, reproduced across two runs, because the contention is in
// the allocator rather than in per-allocation cost. Selecting a global
// allocator is the only unsafe item in this workspace; every other crate keeps
// `unsafe_code = "forbid"`.
#[allow(unsafe_code)]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() -> std::process::ExitCode {
    riffdb_server::riffdbd_main()
}
