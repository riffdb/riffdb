#![forbid(unsafe_code)]

//! Executable Fjall engine-substrate evidence for WP-075.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use riffdb_storage_fjall_comparison::{
    ComparisonDurability, ComparisonMutation, FjallComparisonStore, RiffdbTable,
};

static NEXT_ROOT: AtomicU64 = AtomicU64::new(1);

struct TempRoot(PathBuf);

impl TempRoot {
    fn new() -> Self {
        let path = env::temp_dir().join(format!(
            "riffdb-wp075-substrate-{}-{}",
            std::process::id(),
            NEXT_ROOT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("create isolated WP-075 test directory");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn exact_table_inventory_is_created_without_extra_riffdb_tables() {
    let root = TempRoot::new();
    let store = FjallComparisonStore::open(root.path()).expect("open Fjall comparison store");
    let mut expected = RiffdbTable::ALL
        .into_iter()
        .map(|table| table.name().to_owned())
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(store.table_names(), expected);
}

#[test]
fn cross_keyspace_commit_is_atomic_and_survives_reopen() {
    let root = TempRoot::new();
    {
        let store = FjallComparisonStore::open(root.path()).expect("open Fjall comparison store");
        let mutations = [
            ComparisonMutation::put(RiffdbTable::Entities, b"entity/1", b"entity")
                .expect("bounded entity mutation"),
            ComparisonMutation::put(RiffdbTable::Commits, b"commit/1", b"commit")
                .expect("bounded commit mutation"),
            ComparisonMutation::put(RiffdbTable::Events, b"event/1", b"event")
                .expect("bounded event mutation"),
            ComparisonMutation::put(RiffdbTable::Outbox, b"outbox/1", b"event")
                .expect("bounded outbox mutation"),
        ];
        store
            .apply(&mutations, ComparisonDurability::Sync)
            .expect("synchronize atomic cross-keyspace batch");
    }

    let reopened = FjallComparisonStore::open(root.path()).expect("reopen Fjall comparison store");
    let snapshot = reopened.snapshot();
    assert_eq!(
        snapshot
            .get(RiffdbTable::Entities, b"entity/1")
            .expect("read entity"),
        Some(b"entity".to_vec())
    );
    assert_eq!(
        snapshot
            .get(RiffdbTable::Commits, b"commit/1")
            .expect("read commit"),
        Some(b"commit".to_vec())
    );
    assert_eq!(
        snapshot
            .get(RiffdbTable::Events, b"event/1")
            .expect("read event"),
        Some(b"event".to_vec())
    );
    assert_eq!(
        snapshot
            .get(RiffdbTable::Outbox, b"outbox/1")
            .expect("read outbox"),
        Some(b"event".to_vec())
    );
}

#[test]
fn owned_snapshot_retains_one_consistent_prewrite_view() {
    let root = TempRoot::new();
    let store = FjallComparisonStore::open(root.path()).expect("open Fjall comparison store");
    store
        .apply(
            &[
                ComparisonMutation::put(RiffdbTable::Meta, b"identity", b"old")
                    .expect("bounded initial value"),
            ],
            ComparisonDurability::Sync,
        )
        .expect("write initial value");
    let before = store.snapshot();
    store
        .apply(
            &[
                ComparisonMutation::put(RiffdbTable::Meta, b"identity", b"new")
                    .expect("bounded replacement"),
            ],
            ComparisonDurability::Sync,
        )
        .expect("write replacement");
    let after = store.snapshot();

    assert_eq!(
        before
            .get(RiffdbTable::Meta, b"identity")
            .expect("read old snapshot"),
        Some(b"old".to_vec())
    );
    assert_eq!(
        after
            .get(RiffdbTable::Meta, b"identity")
            .expect("read new snapshot"),
        Some(b"new".to_vec())
    );
}

#[test]
fn bounded_range_pages_have_strict_continuations_and_exact_end() {
    let root = TempRoot::new();
    let store = FjallComparisonStore::open(root.path()).expect("open Fjall comparison store");
    let mutations = (0_u8..5)
        .map(|value| {
            ComparisonMutation::put(RiffdbTable::Audit, vec![value], vec![value])
                .expect("bounded audit row")
        })
        .collect::<Vec<_>>();
    store
        .apply(&mutations, ComparisonDurability::Sync)
        .expect("write ordered audit rows");

    let snapshot = store.snapshot();
    let first = snapshot
        .scan(RiffdbTable::Audit, None, 2, 64)
        .expect("read first page");
    assert_eq!(
        first
            .rows()
            .iter()
            .map(|(key, _)| key[0])
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
    assert!(!first.exact_end());
    let second = snapshot
        .scan(RiffdbTable::Audit, first.continuation(), 2, 64)
        .expect("read second page");
    assert_eq!(
        second
            .rows()
            .iter()
            .map(|(key, _)| key[0])
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    let third = snapshot
        .scan(RiffdbTable::Audit, second.continuation(), 2, 64)
        .expect("read terminal page");
    assert_eq!(third.rows()[0].0, vec![4]);
    assert!(third.exact_end());
    assert_eq!(third.continuation(), None);
}

#[test]
fn buffered_commits_become_group_durable_at_explicit_flush() {
    let root = TempRoot::new();
    let store = FjallComparisonStore::open(root.path()).expect("open Fjall comparison store");
    for value in 0_u8..4 {
        store
            .apply(
                &[
                    ComparisonMutation::put(RiffdbTable::Audit, vec![value], vec![value])
                        .expect("bounded audit mutation"),
                ],
                ComparisonDurability::Buffered,
            )
            .expect("buffer comparison commit");
    }
    store.flush_group().expect("synchronize buffered group");
    drop(store);

    let reopened =
        FjallComparisonStore::open(root.path()).expect("reopen flushed comparison store");
    let page = reopened
        .snapshot()
        .scan(RiffdbTable::Audit, None, 8, 64)
        .expect("scan flushed rows");
    assert_eq!(page.rows().len(), 4);
    assert!(page.exact_end());
}
