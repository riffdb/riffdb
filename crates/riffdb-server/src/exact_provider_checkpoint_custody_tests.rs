// req: OQ-040, PRJ-004
use super::CheckpointCustody;
use std::fs;
use std::io::Read;
use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};
use std::path::Path;

fn private_root() -> tempfile::TempDir {
    let root = tempfile::tempdir().unwrap();
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
    root
}
fn private_file(path: &Path, bytes: &[u8]) {
    fs::write(path, bytes).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
}
fn read(path: &Path) -> Vec<u8> {
    let mut bytes = Vec::new();
    CheckpointCustody::open(path)
        .unwrap()
        .read()
        .unwrap()
        .unwrap()
        .read_to_end(&mut bytes)
        .unwrap();
    bytes
}

#[test]
fn cleanup_refuses_replaced_objects_and_preserves_foreign_bytes() {
    let root = private_root();
    let path = root.path().join("epoch.rxts");
    let retained = root.path().join("retained");
    private_file(&path, b"owned derived bytes");
    let manifest = CheckpointCustody::open(&path)
        .unwrap()
        .owned_manifest()
        .unwrap();
    fs::rename(&path, &retained).unwrap();
    private_file(&path, b"foreign evidence");
    assert!(CheckpointCustody::open_for_cleanup(&path, &manifest).is_err());
    assert_eq!(fs::read(&path).unwrap(), b"foreign evidence");
    assert_eq!(fs::read(&retained).unwrap(), b"owned derived bytes");
}

#[test]
fn cleanup_removes_only_claimed_selected_and_partial_objects() {
    let root = private_root();
    let path = root.path().join("epoch.rxts");
    let pending = path.with_extension("pending");
    let foreign = root.path().join("foreign");
    private_file(&path, b"owned derived bytes");
    private_file(&pending, b"owned partial bytes");
    private_file(&foreign, b"foreign evidence");
    let manifest = CheckpointCustody::open(&path)
        .unwrap()
        .owned_manifest()
        .unwrap();
    CheckpointCustody::open_for_cleanup(&path, &manifest)
        .unwrap()
        .remove_owned()
        .unwrap();
    assert!(!path.exists());
    assert!(!pending.exists());
    assert_eq!(fs::read(&foreign).unwrap(), b"foreign evidence");
    // Repeating missing-object cleanup neither reconstructs nor deletes a
    // newly occupied name: it remains restricted to the original manifest.
    CheckpointCustody::open_for_cleanup(&path, &manifest)
        .unwrap()
        .remove_owned()
        .unwrap();
    private_file(&path, b"replacement foreign bytes");
    assert!(CheckpointCustody::open_for_cleanup(&path, &manifest).is_err());
    assert_eq!(fs::read(&path).unwrap(), b"replacement foreign bytes");
}

#[test]
fn cleanup_manifest_cannot_be_reused_for_another_name_or_directory() {
    let root = private_root();
    let path = root.path().join("epoch.rxts");
    private_file(&path, b"owned derived bytes");
    let manifest = CheckpointCustody::open(&path)
        .unwrap()
        .owned_manifest()
        .unwrap();
    let other = root.path().join("other.rxts");
    private_file(&other, b"foreign evidence");
    assert!(CheckpointCustody::open_for_cleanup(&other, &manifest).is_err());
    let elsewhere = private_root();
    let same_name = elsewhere.path().join("epoch.rxts");
    private_file(&same_name, b"foreign evidence");
    assert!(CheckpointCustody::open_for_cleanup(&same_name, &manifest).is_err());
    assert_eq!(fs::read(&other).unwrap(), b"foreign evidence");
    assert_eq!(fs::read(&same_name).unwrap(), b"foreign evidence");
    assert_eq!(fs::read(&path).unwrap(), b"owned derived bytes");
}

#[test]
fn custody_creates_private_roots_without_following_symlinked_components() {
    let root = private_root();
    let path = root.path().join("derived").join("exact");
    super::create_private_directory(&path).unwrap();
    assert_eq!(fs::metadata(&path).unwrap().mode() & 0o777, 0o700);
    let foreign = root.path().join("foreign");
    fs::create_dir(&foreign).unwrap();
    let alias = root.path().join("alias");
    symlink(&foreign, &alias).unwrap();
    assert!(super::create_private_directory(&alias.join("unexpected")).is_err());
    assert!(!foreign.join("unexpected").exists());
}

#[test]
fn custody_supports_relative_paths_without_changing_process_directory() {
    let root = tempfile::Builder::new().tempdir_in(".").unwrap();
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let current = std::env::current_dir().unwrap();
    let path = root
        .path()
        .strip_prefix(&current)
        .unwrap()
        .join("epoch.rxts");
    assert!(!path.is_absolute());
    CheckpointCustody::open(&path)
        .unwrap()
        .publish(b"relative derived bytes", || {}, || {})
        .unwrap();
    assert_eq!(read(&path), b"relative derived bytes");
}

#[test]
fn custody_publishes_replaces_and_reopens_one_private_link() {
    let root = private_root();
    let path = root.path().join("epoch.rxts");
    assert!(
        CheckpointCustody::open(&path)
            .unwrap()
            .read()
            .unwrap()
            .is_none()
    );
    for bytes in [b"generation one".as_slice(), b"generation two".as_slice()] {
        CheckpointCustody::open(&path)
            .unwrap()
            .publish(bytes, || {}, || {})
            .unwrap();
        assert_eq!(read(&path), bytes);
        let metadata = fs::metadata(&path).unwrap();
        assert_eq!(metadata.mode() & 0o777, 0o600);
        assert_eq!(metadata.nlink(), 1);
        assert!(!path.with_extension("pending").exists());
    }
}

#[test]
fn custody_discards_only_verified_private_partial_work() {
    let root = private_root();
    let path = root.path().join("epoch.rxts");
    private_file(&path.with_extension("pending"), b"incomplete derived bytes");
    CheckpointCustody::open(&path)
        .unwrap()
        .publish(b"complete bytes", || {}, || {})
        .unwrap();
    assert_eq!(read(&path), b"complete bytes");
}

#[test]
fn custody_rejects_selected_symlinks_and_foreign_hardlinks() {
    let root = private_root();
    let foreign = root.path().join("foreign");
    private_file(&foreign, b"foreign evidence");
    let path = root.path().join("epoch.rxts");
    symlink(&foreign, &path).unwrap();
    assert!(CheckpointCustody::open(&path).is_err());
    fs::remove_file(&path).unwrap();
    fs::hard_link(&foreign, &path).unwrap();
    assert!(CheckpointCustody::open(&path).is_err());
    assert_eq!(fs::read(&foreign).unwrap(), b"foreign evidence");
    assert_eq!(fs::metadata(&foreign).unwrap().nlink(), 2);
}

#[test]
fn custody_rejects_replaced_selection_after_sync_without_deleting_foreign_object() {
    let root = private_root();
    let path = root.path().join("epoch.rxts");
    let retained = root.path().join("retained");
    private_file(&path, b"previous derived bytes");
    let custody = CheckpointCustody::open(&path).unwrap();
    let result = custody.publish(
        b"replacement derived bytes",
        || {},
        || {
            fs::rename(&path, &retained).unwrap();
            private_file(&path, b"foreign evidence");
        },
    );
    assert!(result.is_err());
    assert_eq!(fs::read(&path).unwrap(), b"foreign evidence");
    assert_eq!(fs::read(&retained).unwrap(), b"previous derived bytes");
}

#[test]
fn custody_rejects_replaced_pending_object_after_sync() {
    let root = private_root();
    let path = root.path().join("epoch.rxts");
    let pending = path.with_extension("pending");
    let retained = root.path().join("retained");
    let result = CheckpointCustody::open(&path).unwrap().publish(
        b"derived bytes",
        || {},
        || {
            fs::rename(&pending, &retained).unwrap();
            private_file(&pending, b"foreign evidence");
        },
    );
    assert!(result.is_err());
    assert!(!path.exists());
    assert_eq!(fs::read(&pending).unwrap(), b"foreign evidence");
    assert_eq!(fs::read(&retained).unwrap(), b"derived bytes");
}

#[test]
fn custody_rejects_directory_replacement_after_sync() {
    let parent = tempfile::tempdir().unwrap();
    let root = parent.path().join("private");
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    let path = root.join("epoch.rxts");
    let retained = parent.path().join("retained");
    let result = CheckpointCustody::open(&path).unwrap().publish(
        b"derived bytes",
        || {},
        || {
            fs::rename(&root, &retained).unwrap();
            fs::create_dir(&root).unwrap();
            fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
            private_file(&path, b"foreign evidence");
        },
    );
    assert!(result.is_err());
    assert_eq!(fs::read(&path).unwrap(), b"foreign evidence");
    assert_eq!(
        fs::read(retained.join("epoch.pending")).unwrap(),
        b"derived bytes"
    );
}

#[test]
fn custody_recovers_exact_two_name_publication_without_exposing_partial_bytes() {
    let root = private_root();
    let path = root.path().join("epoch.rxts");
    let pending = path.with_extension("pending");
    private_file(&pending, b"complete synced derived bytes");
    fs::hard_link(&pending, &path).unwrap();
    assert_eq!(read(&path), b"complete synced derived bytes");
    assert!(!pending.exists());
    assert_eq!(fs::metadata(&path).unwrap().nlink(), 1);
}

#[test]
fn custody_rejects_a_third_link_without_deleting_any_name() {
    let root = private_root();
    let path = root.path().join("epoch.rxts");
    let pending = path.with_extension("pending");
    let foreign = root.path().join("foreign");
    private_file(&pending, b"foreign evidence");
    fs::hard_link(&pending, &path).unwrap();
    fs::hard_link(&pending, &foreign).unwrap();
    assert!(CheckpointCustody::open(&path).is_err());
    for name in [&path, &pending, &foreign] {
        assert_eq!(fs::read(name).unwrap(), b"foreign evidence");
        assert_eq!(fs::metadata(name).unwrap().nlink(), 3);
    }
}

#[test]
fn custody_rejects_broadened_permissions_and_symlinked_ancestors() {
    let root = private_root();
    let path = root.path().join("epoch.rxts");
    private_file(&path, b"derived bytes");
    fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(CheckpointCustody::open(&path).is_err());
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    let alias = root.path().join("alias");
    symlink(root.path(), &alias).unwrap();
    assert!(CheckpointCustody::open(&alias.join("epoch.rxts")).is_err());
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o755)).unwrap();
    assert!(CheckpointCustody::open(&path).is_err());
    assert_eq!(fs::read(&path).unwrap(), b"derived bytes");
}

#[test]
fn observed_custody_cannot_relearn_a_foreign_replacement() {
    let root = private_root();
    let path = root.path().join("epoch.rxts");
    let retained = root.path().join("retained");
    private_file(&path, b"owned derived bytes");
    let manifest = CheckpointCustody::open(&path)
        .unwrap()
        .owned_manifest()
        .unwrap();
    fs::rename(&path, &retained).unwrap();
    private_file(&path, b"foreign evidence");
    assert!(CheckpointCustody::open_bound(&path, &manifest, false).is_err());
    assert_eq!(fs::read(&path).unwrap(), b"foreign evidence");
    assert_eq!(fs::read(&retained).unwrap(), b"owned derived bytes");
}

#[test]
fn retained_manifest_pins_unlinked_file_and_directory_inodes_until_final_drop() {
    let root = private_root();
    let path = root.path().join("epoch.rxts");
    private_file(&path, b"owned evidence");
    let manifest = CheckpointCustody::open(&path)
        .unwrap()
        .owned_manifest()
        .unwrap();
    let file = manifest.selected.as_ref().unwrap();
    let file_weak = std::sync::Arc::downgrade(&file._custody);
    let directory_weak = std::sync::Arc::downgrade(&manifest.directory.custody);
    fs::remove_file(&path).unwrap();
    fs::remove_dir(root.path()).unwrap();
    assert_eq!(file._custody.metadata().unwrap().nlink(), 0);
    assert_eq!(
        manifest
            .directory
            .custody
            ._directory
            .metadata()
            .unwrap()
            .nlink(),
        0
    );
    let mut retained = Vec::new();
    file._custody
        .try_clone()
        .unwrap()
        .read_to_end(&mut retained)
        .unwrap();
    assert_eq!(retained, b"owned evidence");
    fs::create_dir(root.path()).unwrap();
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
    private_file(&path, b"replacement evidence");
    assert!(CheckpointCustody::open_for_cleanup(&path, &manifest).is_err());
    assert_eq!(fs::read(&path).unwrap(), b"replacement evidence");
    drop(manifest);
    assert!(file_weak.upgrade().is_none());
    assert!(directory_weak.upgrade().is_none());
}
