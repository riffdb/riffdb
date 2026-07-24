use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use riffdb_auth::bootstrap_secret::{
    BootstrapCredential as RetainedBootstrapCredential, SystemEntropy,
    generate_bootstrap_credential, load_bootstrap_credential_file, read_bootstrap_credential,
};
use riffdb_client_rust::{
    BearerCredential, BootstrapCallMetadata, BootstrapCredential as TransportBootstrapCredential,
    CallMetadata, load_protected_bearer_credential,
};
use zeroize::Zeroizing;

use crate::config::{EffectiveConfig, Environment};
use crate::input::{InputError, validate_path};

const TOKEN_BYTES: usize = 43;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CredentialError {
    Required,
    SourcesConflict,
    Invalid,
    BootstrapGeneration,
    BootstrapInvalid,
    Retention,
}

pub(crate) struct NormalCredential {
    pub(crate) metadata: CallMetadata,
    pub(crate) file: Option<PathBuf>,
}

pub(crate) fn normal_credential(
    config: &EffectiveConfig,
    environment: &dyn Environment,
) -> Result<NormalCredential, CredentialError> {
    let environment_token = environment.value("RIFFDB_CAPABILITY_TOKEN");
    match (&config.credential_file, environment_token) {
        (Some(_), Some(_)) => Err(CredentialError::SourcesConflict),
        (None, None) => Err(CredentialError::Required),
        (Some(path), None) => {
            let credential =
                load_protected_bearer_credential(path).map_err(|_| CredentialError::Invalid)?;
            Ok(NormalCredential {
                metadata: CallMetadata::authenticated(credential),
                file: Some(path.clone()),
            })
        }
        (None, Some(token)) => {
            let bytes = Zeroizing::new(token.into_encoded_bytes());
            if bytes.len() != TOKEN_BYTES {
                return Err(CredentialError::Invalid);
            }
            let text = std::str::from_utf8(&bytes).map_err(|_| CredentialError::Invalid)?;
            let credential = BearerCredential::new(text).map_err(|_| CredentialError::Invalid)?;
            Ok(NormalCredential {
                metadata: CallMetadata::authenticated(credential),
                file: None,
            })
        }
    }
}

pub(crate) struct BootstrapMaterial {
    pub(crate) credential: RetainedBootstrapCredential,
    pub(crate) bearer_retained: bool,
}

impl BootstrapMaterial {
    pub(crate) fn metadata(&self) -> Result<BootstrapCallMetadata, CredentialError> {
        let text = std::str::from_utf8(self.credential.token().expose_secret())
            .map_err(|_| CredentialError::BootstrapInvalid)?;
        let credential = TransportBootstrapCredential::new(text)
            .map_err(|_| CredentialError::BootstrapInvalid)?;
        Ok(BootstrapCallMetadata::new(credential))
    }
}

pub(crate) fn bootstrap_material(
    generate: Option<&OsString>,
    bootstrap_file: Option<&OsString>,
    bootstrap_stdin: bool,
    bearer_output: Option<&OsString>,
    stdin: &mut dyn Read,
) -> Result<BootstrapMaterial, CredentialError> {
    let selected = usize::from(generate.is_some())
        + usize::from(bootstrap_file.is_some())
        + usize::from(bootstrap_stdin);
    if selected != 1 {
        return Err(CredentialError::BootstrapInvalid);
    }

    let credential = if let Some(path) = generate {
        let path = checked_real_path(path)?;
        let milliseconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|duration| u64::try_from(duration.as_millis()).ok())
            .ok_or(CredentialError::BootstrapGeneration)?;
        let credential = generate_bootstrap_credential(milliseconds, &SystemEntropy)
            .map_err(|_| CredentialError::BootstrapGeneration)?;
        let document = credential.render_document();
        let _retained = retain_and_revalidate(
            &path,
            document.expose_secret(),
            &mut SystemRetention::default(),
            || load_bootstrap_credential_file(&path).map_err(|_| CredentialError::Retention),
            |retained| retained.render_document().expose_secret() == document.expose_secret(),
        )?;
        credential
    } else if let Some(path) = bootstrap_file {
        let path = checked_real_path(path)?;
        load_bootstrap_credential_file(&path).map_err(|_| CredentialError::BootstrapInvalid)?
    } else {
        read_bootstrap_credential(stdin).map_err(|_| CredentialError::BootstrapInvalid)?
    };

    let bearer_retained = if let Some(path) = bearer_output {
        let path = checked_real_path(path)?;
        let text = std::str::from_utf8(credential.token().expose_secret())
            .map_err(|_| CredentialError::BootstrapInvalid)?;
        let expected =
            BearerCredential::new(text).map_err(|_| CredentialError::BootstrapInvalid)?;
        let _reread = retain_and_revalidate(
            &path,
            credential.token().expose_secret(),
            &mut SystemRetention::default(),
            || load_protected_bearer_credential(&path).map_err(|_| CredentialError::Retention),
            |reread| reread.has_same_presentation(&expected),
        )?;
        true
    } else {
        false
    };

    Ok(BootstrapMaterial {
        credential,
        bearer_retained,
    })
}

pub(crate) fn retain_normal_token(path: &OsString, token: String) -> Result<(), CredentialError> {
    let path = checked_real_path(path)?;
    let token = Zeroizing::new(token);
    if token.len() != TOKEN_BYTES {
        return Err(CredentialError::Retention);
    }
    let expected = BearerCredential::new(token.as_str()).map_err(|_| CredentialError::Retention)?;
    let _reread = retain_and_revalidate(
        &path,
        token.as_bytes(),
        &mut SystemRetention::default(),
        || load_protected_bearer_credential(&path).map_err(|_| CredentialError::Retention),
        |reread| reread.has_same_presentation(&expected),
    )?;
    Ok(())
}

fn checked_real_path(path: &OsString) -> Result<PathBuf, CredentialError> {
    validate_path(path).map_err(|_| CredentialError::Retention)?;
    if path.as_encoded_bytes() == b"-" {
        return Err(CredentialError::Retention);
    }
    Ok(PathBuf::from(path))
}

#[cfg(test)]
fn retain_bytes(path: &Path, bytes: &[u8]) -> Result<(), CredentialError> {
    retain_bytes_with(path, bytes, &mut SystemRetention::default())
}

fn retain_and_revalidate<T>(
    path: &Path,
    bytes: &[u8],
    retention: &mut dyn RetentionBackend,
    load: impl FnOnce() -> Result<T, CredentialError>,
    matches: impl FnOnce(&T) -> bool,
) -> Result<T, CredentialError> {
    retain_bytes_with(path, bytes, retention)?;
    let retained = load()?;
    if !matches(&retained) {
        return Err(CredentialError::Retention);
    }
    Ok(retained)
}

fn retain_bytes_with(
    path: &Path,
    bytes: &[u8],
    retention: &mut dyn RetentionBackend,
) -> Result<(), CredentialError> {
    validate_path(path.as_os_str()).map_err(|_| CredentialError::Retention)?;
    retention.create(path)?;
    retention.write_all(bytes)?;
    retention.sync_file()?;
    retention.close_file()?;

    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    retention.open_directory(parent)?;
    retention.sync_directory()?;
    retention.close_directory()
}

trait RetentionBackend {
    fn create(&mut self, path: &Path) -> Result<(), CredentialError>;
    fn write_all(&mut self, bytes: &[u8]) -> Result<(), CredentialError>;
    fn sync_file(&mut self) -> Result<(), CredentialError>;
    fn close_file(&mut self) -> Result<(), CredentialError>;
    fn open_directory(&mut self, path: &Path) -> Result<(), CredentialError>;
    fn sync_directory(&mut self) -> Result<(), CredentialError>;
    fn close_directory(&mut self) -> Result<(), CredentialError>;
}

#[derive(Default)]
struct SystemRetention {
    file: Option<File>,
    directory: Option<File>,
}

impl RetentionBackend for SystemRetention {
    fn create(&mut self, path: &Path) -> Result<(), CredentialError> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true).mode(0o600);
        self.file = Some(options.open(path).map_err(|_| CredentialError::Retention)?);
        Ok(())
    }

    fn write_all(&mut self, bytes: &[u8]) -> Result<(), CredentialError> {
        self.file
            .as_mut()
            .ok_or(CredentialError::Retention)?
            .write_all(bytes)
            .map_err(|_| CredentialError::Retention)
    }

    fn sync_file(&mut self) -> Result<(), CredentialError> {
        self.file
            .as_ref()
            .ok_or(CredentialError::Retention)?
            .sync_all()
            .map_err(|_| CredentialError::Retention)
    }

    fn close_file(&mut self) -> Result<(), CredentialError> {
        let file = self.file.take().ok_or(CredentialError::Retention)?;
        drop(file);
        Ok(())
    }

    fn open_directory(&mut self, path: &Path) -> Result<(), CredentialError> {
        let directory = File::open(path).map_err(|_| CredentialError::Retention)?;
        let metadata = directory
            .metadata()
            .map_err(|_| CredentialError::Retention)?;
        if !metadata.is_dir() {
            return Err(CredentialError::Retention);
        }
        self.directory = Some(directory);
        Ok(())
    }

    fn sync_directory(&mut self) -> Result<(), CredentialError> {
        self.directory
            .as_ref()
            .ok_or(CredentialError::Retention)?
            .sync_all()
            .map_err(|_| CredentialError::Retention)
    }

    fn close_directory(&mut self) -> Result<(), CredentialError> {
        let directory = self.directory.take().ok_or(CredentialError::Retention)?;
        drop(directory);
        Ok(())
    }
}

impl From<InputError> for CredentialError {
    fn from(_: InputError) -> Self {
        Self::Retention
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::os::unix::fs::PermissionsExt as _;

    use super::*;
    use crate::cli::OutputMode;

    #[derive(Default)]
    struct TestEnvironment(BTreeMap<String, OsString>);

    impl Environment for TestEnvironment {
        fn value(&self, name: &str) -> Option<OsString> {
            self.0.get(name).cloned()
        }
    }

    fn config(file: Option<PathBuf>) -> EffectiveConfig {
        EffectiveConfig {
            endpoint: "http://127.0.0.1:7443".to_owned(),
            output: OutputMode::Json,
            max_attempts: 3,
            credential_file: file,
        }
    }

    #[test]
    fn absent_and_conflicting_normal_sources_fail_closed() {
        assert!(matches!(
            normal_credential(&config(None), &TestEnvironment::default()),
            Err(CredentialError::Required)
        ));
        let mut environment = TestEnvironment::default();
        environment.0.insert(
            "RIFFDB_CAPABILITY_TOKEN".into(),
            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".into(),
        );
        assert!(matches!(
            normal_credential(&config(Some("/tmp/token".into())), &environment),
            Err(CredentialError::SourcesConflict)
        ));
    }

    #[test]
    fn retained_file_is_exclusive_and_never_overwritten() {
        let path = std::env::temp_dir().join(format!("riffdb-cli-retain-{}", std::process::id()));
        let _ = fs::remove_file(&path);
        retain_bytes(&path, b"first").expect("first");
        assert_eq!(
            fs::metadata(&path).expect("metadata").permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            retain_bytes(&path, b"second"),
            Err(CredentialError::Retention)
        );
        assert_eq!(fs::read(&path).expect("read"), b"first");
        fs::remove_file(path).expect("cleanup");
    }

    #[test]
    fn every_durability_step_fails_closed_before_revalidation() {
        for failure in [
            RetentionStep::Create,
            RetentionStep::PartialWrite,
            RetentionStep::FileSync,
            RetentionStep::FileClose,
            RetentionStep::DirectoryOpen,
            RetentionStep::DirectorySync,
            RetentionStep::DirectoryClose,
        ] {
            let mut retention = ScriptedRetention::failing(failure);
            assert_eq!(
                retain_bytes_with(Path::new("/unused"), b"secret", &mut retention),
                Err(CredentialError::Retention),
                "{failure:?}"
            );
            assert_eq!(retention.steps.last(), Some(&failure));
            let position = RetentionStep::ALL
                .iter()
                .position(|step| *step == failure)
                .expect("known step");
            assert_eq!(
                retention.steps,
                RetentionStep::ALL[..=position],
                "continued after {failure:?}"
            );
        }

        let mut retention = ScriptedRetention::successful();
        retain_bytes_with(Path::new("/unused"), b"secret", &mut retention).expect("retained");
        assert_eq!(retention.steps, RetentionStep::ALL);
    }

    #[test]
    fn protected_reread_and_presentation_compare_fail_closed_after_sync() {
        let mut reread = ScriptedRetention::successful();
        let result = retain_and_revalidate(
            Path::new("/unused"),
            b"secret",
            &mut reread,
            || Err::<u8, _>(CredentialError::Retention),
            |_| true,
        );
        assert_eq!(result, Err(CredentialError::Retention));
        assert_eq!(reread.steps, RetentionStep::ALL);

        let mut compare = ScriptedRetention::successful();
        let result = retain_and_revalidate(
            Path::new("/unused"),
            b"secret",
            &mut compare,
            || Ok(7_u8),
            |_| false,
        );
        assert_eq!(result, Err(CredentialError::Retention));
        assert_eq!(compare.steps, RetentionStep::ALL);
    }

    #[test]
    fn containing_directory_handle_must_refer_to_a_directory() {
        let path =
            std::env::temp_dir().join(format!("riffdb-cli-parent-file-{}", std::process::id()));
        let _ = fs::remove_file(&path);
        fs::write(&path, b"not a directory").expect("regular file");
        let mut retention = SystemRetention::default();
        assert_eq!(
            retention.open_directory(&path),
            Err(CredentialError::Retention)
        );
        assert!(retention.directory.is_none());
        fs::remove_file(path).expect("cleanup");
    }

    #[test]
    fn bootstrap_document_constant_remains_exact() {
        assert_eq!(
            riffdb_auth::bootstrap_secret::BOOTSTRAP_CREDENTIAL_DOCUMENT_BYTES,
            132
        );
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum RetentionStep {
        Create,
        PartialWrite,
        FileSync,
        FileClose,
        DirectoryOpen,
        DirectorySync,
        DirectoryClose,
    }

    impl RetentionStep {
        const ALL: [Self; 7] = [
            Self::Create,
            Self::PartialWrite,
            Self::FileSync,
            Self::FileClose,
            Self::DirectoryOpen,
            Self::DirectorySync,
            Self::DirectoryClose,
        ];
    }

    struct ScriptedRetention {
        failure: Option<RetentionStep>,
        steps: Vec<RetentionStep>,
    }

    impl ScriptedRetention {
        const fn failing(failure: RetentionStep) -> Self {
            Self {
                failure: Some(failure),
                steps: Vec::new(),
            }
        }

        const fn successful() -> Self {
            Self {
                failure: None,
                steps: Vec::new(),
            }
        }

        fn step(&mut self, step: RetentionStep) -> Result<(), CredentialError> {
            self.steps.push(step);
            if self.failure == Some(step) {
                Err(CredentialError::Retention)
            } else {
                Ok(())
            }
        }
    }

    impl RetentionBackend for ScriptedRetention {
        fn create(&mut self, _path: &Path) -> Result<(), CredentialError> {
            self.step(RetentionStep::Create)
        }

        fn write_all(&mut self, _bytes: &[u8]) -> Result<(), CredentialError> {
            self.step(RetentionStep::PartialWrite)
        }

        fn sync_file(&mut self) -> Result<(), CredentialError> {
            self.step(RetentionStep::FileSync)
        }

        fn close_file(&mut self) -> Result<(), CredentialError> {
            self.step(RetentionStep::FileClose)
        }

        fn open_directory(&mut self, _path: &Path) -> Result<(), CredentialError> {
            self.step(RetentionStep::DirectoryOpen)
        }

        fn sync_directory(&mut self) -> Result<(), CredentialError> {
            self.step(RetentionStep::DirectorySync)
        }

        fn close_directory(&mut self) -> Result<(), CredentialError> {
            self.step(RetentionStep::DirectoryClose)
        }
    }
}
