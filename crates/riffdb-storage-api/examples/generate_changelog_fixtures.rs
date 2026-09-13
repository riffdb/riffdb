#![forbid(unsafe_code)]
//! Frozen V1/V2 compatibility vectors and the proposed V3 format vector.

use std::{env, fs, path::Path};

use riffdb_storage_api::{
    AuthoritativeMutationV3, AuthoritativeNamespaceV1, AuthoritativeStateCatalogV1,
    AuthoritativeTransactionBindingV3, AuthoritativeTransactionV3, ChangelogAttributionV3,
    ChangelogEntryClassV1, ChangelogEntryClassV2, ChangelogEntryV1, ChangelogEntryV2,
    ChangelogFrameBindingV1, ChangelogFrameBindingV2, ChangelogFrameBindingV3, ChangelogFrameV1,
    ChangelogFrameV2, ChangelogFrameV3, ChangelogTransactionSequence,
};
use riffdb_types::{CommitSequence, DatabaseId, DualFrontier};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    if arguments != ["--check"] && arguments != ["--write"] {
        return Err("usage: generate_changelog_fixtures --check|--write".into());
    }
    let database_id = DatabaseId::from_unix_milliseconds_and_random(1_700_000_000_000, [0x71; 10])?;
    let initial = DualFrontier::INITIAL;
    let covered = DualFrontier::new(CommitSequence::new(1), None);
    let v1 = ChangelogFrameV1::new(
        ChangelogFrameBindingV1::new(database_id, 1, [0x11; 32], [0x22; 32], true),
        initial,
        covered,
        vec![ChangelogEntryV1::new(
            ChangelogEntryClassV1::Entity,
            b"entity-key".to_vec().into_boxed_slice(),
            b"entity-post-image".to_vec().into_boxed_slice(),
        )],
    )?
    .encode()?;
    let v2 = ChangelogFrameV2::new(
        ChangelogFrameBindingV2 {
            database_id,
            history_incarnation: 1,
            chain_hash: [0x33; 32],
            journal_frame_hash: [0x44; 32],
            journaled: true,
        },
        initial,
        covered,
        vec![ChangelogEntryV2::put(
            ChangelogEntryClassV2::Entity,
            b"entity-key".to_vec(),
            b"entity-post-image".to_vec(),
        )?],
    )?
    .encode()?;
    let binding = AuthoritativeTransactionBindingV3 {
        database_id,
        history_incarnation: 1,
        predecessor: None,
        sequence: ChangelogTransactionSequence::new(1).ok_or("invalid fixture sequence")?,
        predecessor_frontier: initial,
        covered_frontier: covered,
        prior_history_hash: [0x55; 32],
    };
    let first = AuthoritativeTransactionV3::new(
        binding,
        ChangelogAttributionV3::JournaledApplicationGroup,
        vec![AuthoritativeMutationV3::put(
            AuthoritativeNamespaceV1::Entities,
            b"entity-key",
            None,
            b"entity-post-image",
        )?],
    )?;
    let second = AuthoritativeTransactionV3::new(
        AuthoritativeTransactionBindingV3 {
            predecessor: Some(binding.sequence),
            sequence: binding
                .sequence
                .checked_next()
                .ok_or("fixture sequence exhausted")?,
            predecessor_frontier: covered,
            prior_history_hash: first.history_hash()?,
            ..binding
        },
        ChangelogAttributionV3::CleanClose,
        vec![],
    )?;
    let v3 = ChangelogFrameV3::new(
        ChangelogFrameBindingV3::new(
            database_id,
            1,
            1,
            AuthoritativeStateCatalogV1.digest(),
            [0x66; 32],
        )?,
        vec![first, second],
    )?
    .encode()?;
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/replication");
    for (name, bytes) in [
        ("changelog-frame-v1.hex", v1.as_bytes()),
        ("changelog-frame-v2.hex", v2.as_bytes()),
        ("changelog-frame-v3.hex", v3.as_slice()),
    ] {
        let hex = bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
            + "\n";
        if arguments == ["--write"] {
            fs::create_dir_all(&root)?;
            fs::write(root.join(name), hex)?;
        } else if fs::read_to_string(root.join(name))? != hex {
            return Err(format!("{name} differs from its canonical codec").into());
        }
    }
    Ok(())
}
