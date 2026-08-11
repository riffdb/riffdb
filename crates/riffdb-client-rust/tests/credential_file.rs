#![forbid(unsafe_code)]

//! Linux integration evidence for the public protected credential loader.

#[cfg(target_os = "linux")]
mod linux {
    use std::fs::{self, OpenOptions};
    use std::io::Write;
    use std::os::unix::fs::{FileTypeExt, OpenOptionsExt, PermissionsExt, symlink};
    use std::path::{Path, PathBuf};
    use std::process::Command;

    use riffdb_client_rust::{
        BearerCredential, BearerCredentialFileError, load_protected_bearer_credential,
    };

    const TOKEN: &str = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";

    /// Field 1 is held only for its whole-directory cleanup on `Drop`.
    struct TestDirectory(PathBuf, #[allow(dead_code)] tempfile::TempDir);

    impl TestDirectory {
        fn create() -> Self {
            let directory = tempfile::TempDir::with_prefix("riffdb-client-credential-")
                .expect("create isolated test directory");
            Self(directory.path().to_path_buf(), directory)
        }
    }

    fn write_private(path: &Path, bytes: &[u8]) {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
            .expect("create private credential file");
        file.write_all(bytes).expect("write credential file");
    }

    fn assert_protected_rejection(path: &Path) {
        assert!(matches!(
            load_protected_bearer_credential(path),
            Err(BearerCredentialFileError::ProtectedFileRejected)
        ));
    }

    #[test]
    fn exact_private_file_loads_and_compares_without_exposure() {
        let root = TestDirectory::create();
        let path = root.0.join("credential");
        write_private(&path, TOKEN.as_bytes());

        let loaded = load_protected_bearer_credential(&path).expect("protected credential");
        let expected = BearerCredential::new(TOKEN).expect("expected credential");
        assert!(loaded.has_same_presentation(&expected));
        assert!(
            !loaded.has_same_presentation(
                &BearerCredential::new("BAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8")
                    .expect("different credential")
            )
        );
        assert_eq!(format!("{loaded:?}"), "BearerCredential([REDACTED])");
    }

    #[test]
    fn final_symlink_every_forbidden_mode_bit_and_trailing_byte_fail_closed() {
        let root = TestDirectory::create();
        let path = root.0.join("credential");
        let link = root.0.join("credential-link");
        write_private(&path, TOKEN.as_bytes());
        symlink(&path, &link).expect("create final-component symlink");

        assert_protected_rejection(&link);

        for forbidden_bit in [0o001, 0o002, 0o004, 0o010, 0o020, 0o040] {
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600 | forbidden_bit))
                .expect("set one forbidden group/other mode bit");
            assert_protected_rejection(&path);
        }

        let newline = root.0.join("credential-newline");
        write_private(&newline, format!("{TOKEN}\n").as_bytes());
        assert_protected_rejection(&newline);
    }

    #[test]
    fn directory_fifo_and_device_objects_fail_before_payload_read() {
        let root = TestDirectory::create();
        let directory = root.0.join("credential-directory");
        fs::create_dir(&directory).expect("create directory object");
        assert_protected_rejection(&directory);

        let fifo = root.0.join("credential-fifo");
        let status = Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .expect("Linux test environment provides mkfifo");
        assert!(status.success(), "mkfifo must create the test object");
        assert!(
            fs::symlink_metadata(&fifo)
                .expect("FIFO metadata")
                .file_type()
                .is_fifo()
        );
        assert_protected_rejection(&fifo);

        let device = PathBuf::from("/dev/null");
        assert!(
            fs::symlink_metadata(&device)
                .expect("Linux /dev/null metadata")
                .file_type()
                .is_char_device()
        );
        assert_protected_rejection(&device);
    }

    #[test]
    fn kernel_unreadable_regular_file_fails_closed_when_permissions_are_enforced() {
        let root = TestDirectory::create();
        let path = root.0.join("credential-unreadable");
        write_private(&path, TOKEN.as_bytes());
        fs::set_permissions(&path, fs::Permissions::from_mode(0o200))
            .expect("remove owner read permission");

        if OpenOptions::new().read(true).open(&path).is_err() {
            assert_protected_rejection(&path);
        }
    }

    #[test]
    fn public_failures_expose_neither_path_nor_credential_content() {
        let root = TestDirectory::create();
        let path_canary = "credential-path-secret-canary";
        let token_canary = "credential-token-secret-canary";
        let missing = root.0.join(path_canary);
        let missing_error = load_protected_bearer_credential(&missing)
            .expect_err("missing protected file must reject");

        let invalid = root.0.join("invalid-credential");
        let mut invalid_bytes = vec![b'A'; 43];
        invalid_bytes[..token_canary.len()].copy_from_slice(token_canary.as_bytes());
        invalid_bytes[42] = b'!';
        write_private(&invalid, &invalid_bytes);
        let invalid_error = load_protected_bearer_credential(&invalid)
            .expect_err("invalid presentation must reject");

        for error in [missing_error, invalid_error] {
            let debug = format!("{error:?}");
            let display = error.to_string();
            for canary in [path_canary, token_canary, TOKEN] {
                assert!(!debug.contains(canary));
                assert!(!display.contains(canary));
            }
            assert!(std::error::Error::source(&error).is_none());
        }
    }
}

#[cfg(not(target_os = "linux"))]
#[test]
fn unsupported_platform_returns_before_any_path_access() {
    let inaccessible = std::path::Path::new("credential-path-must-not-be-opened");
    assert_eq!(
        riffdb_client_rust::load_protected_bearer_credential(inaccessible).err(),
        Some(riffdb_client_rust::BearerCredentialFileError::UnsupportedPlatform)
    );
}
