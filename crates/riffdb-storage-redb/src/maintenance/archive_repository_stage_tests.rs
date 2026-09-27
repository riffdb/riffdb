//! Selector staging must use the retained durable artifact, not a replacement.
// req: REP-007
use super::super::archive_repository_tests::fixture;
use super::*;
use riffdb_storage_api::ArchiveConsumerV1;
use std::{fs, os::unix::fs::symlink};

#[test]
fn archive_selector_refuses_replaced_manifest_before_publication() {
    for replace_with_symlink in [false, true] {
        let scope = crate::test_path::ScopedDirectory::new("archive-selector-source");
        let path = scope.join("archive");
        let (lineage, fence) = fixture::fixture();
        let repository = RedbArchiveRepository::open(
            &path,
            lineage,
            fence,
            [0x33; 32],
            ArchiveEncryptionPostureV1::Unencrypted,
        )
        .unwrap();
        let mut consumer = ArchiveConsumerV1::new(repository, lineage, fence);
        let first = consumer.append(fixture::frame(lineage, fence, 1)).unwrap();
        let repository = consumer.into_sink();
        let selected = fs::read(path.join(CURRENT)).unwrap();
        let (_, manifest_name) = names(fence).unwrap();
        let manifest = fs::File::open(path.join(&manifest_name)).unwrap();
        fs::rename(path.join(&manifest_name), path.join("retained-manifest")).unwrap();
        if replace_with_symlink {
            symlink("retained-manifest", path.join(&manifest_name)).unwrap();
        } else {
            // Equal bytes and length cannot stand in for the retained inode.
            fs::write(path.join(&manifest_name), &selected).unwrap();
        }
        assert_eq!(
            repository.stage_current(&manifest_name, &manifest),
            Err(Error::ResyncRequired)
        );
        assert_eq!(repository.position(), first);
        assert_eq!(fs::read(path.join(CURRENT)).unwrap(), selected);
        assert!(!path.join(CURRENT_TEMP).exists());
    }
}
