//! Offline comparison of application authority and bounded archive damage.
use riffdb_storage_api::{AuthoritativeNamespaceV1 as N, AuthoritativeStateStepV3};
use std::{collections::BTreeMap, fs, path::Path};

#[derive(Debug, Eq, PartialEq)]
pub(super) struct ApplicationState {
    pub incarnation: u64,
    pub sequence: u64,
    pub rows: BTreeMap<(N, Vec<u8>), Vec<u8>>,
}
pub(super) fn application(path: &Path) -> ApplicationState {
    let ports = super::support::open_primary(path);
    let pin = ports.published_changelog_snapshot_v3().unwrap();
    let mut cursor = pin.authoritative_state_v3().unwrap();
    let history = cursor.history();
    let mut state = ApplicationState {
        incarnation: history.lineage().history_incarnation(),
        sequence: history.tail().frontier().application().unwrap().get(),
        rows: BTreeMap::new(),
    };
    let mut bytes = 0;
    for _ in 0..8192 {
        let Some(step) = cursor.next_item().unwrap() else {
            return state;
        };
        if let AuthoritativeStateStepV3::Row(row) = step {
            bytes += row.key().len() + row.value().len();
            assert!(bytes <= 32 * 1024 * 1024);
            if matches!(
                row.namespace(),
                N::Entities
                    | N::EntityChainHeads
                    | N::SecondaryIndexes
                    | N::IndexEpochs
                    | N::Idempotency
                    | N::IdempotencyPending
                    | N::Commits
                    | N::Provenance
                    | N::Events
                    | N::EventRoutes
                    | N::Outbox
            ) {
                assert!(
                    state
                        .rows
                        .insert((row.namespace(), row.key().to_vec()), row.value().to_vec())
                        .is_none()
                );
            }
        }
    }
    panic!("bounded application oracle did not reach exact end")
}
#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum Damage {
    None,
    Truncated,
    Reordered,
}
pub(super) fn damage(path: &Path, damage: Damage) {
    if damage == Damage::None {
        return;
    }
    let mut frames: Vec<_> = fs::read_dir(path)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("frame-")
        })
        .collect();
    frames.sort();
    assert!((2..256).contains(&frames.len()));
    let first = fs::read(&frames[0]).unwrap();
    let last = fs::read(frames.last().unwrap()).unwrap();
    assert!(first.len() < 32 * 1024 * 1024 && last.len() < 32 * 1024 * 1024);
    match damage {
        Damage::Truncated => fs::write(frames.last().unwrap(), &last[..last.len() - 1]).unwrap(),
        Damage::Reordered => {
            fs::write(&frames[0], last).unwrap();
            fs::write(frames.last().unwrap(), first).unwrap();
        }
        Damage::None => unreachable!(),
    }
}
