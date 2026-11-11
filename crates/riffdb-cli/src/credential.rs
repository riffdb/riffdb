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
    BearerCredential, BearerCredentialFileCause, BootstrapCallMetadata,
    BootstrapCredential as TransportBootstrapCredential, CallMetadata,
    load_protected_bearer_credential, load_protected_bearer_credential_diagnosed,
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
    File(BearerCredentialFileCause),
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
            let credential = load_protected_bearer_credential_diagnosed(path)
                .map_err(|failure| CredentialError::File(failure.cause()))?;
            Ok(NormalCredential {
                metadata: CallMetadata::authenticated(credential)
                    .with_database(config.database.clone()),
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
                metadata: CallMetadata::authenticated(credential)
                    .with_database(config.database.clone()),
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
    pub(crate) fn metadata(
        &self,
        database: riffdb_types::DatabaseAlias,
    ) -> Result<BootstrapCallMetadata, CredentialError> {
        let text = std::str::from_utf8(self.credential.token().expose_secret())
            .map_err(|_| CredentialError::BootstrapInvalid)?;
        let credential = TransportBootstrapCredential::new(text)
            .map_err(|_| CredentialError::BootstrapInvalid)?;
        Ok(BootstrapCallMetadata::new(credential).with_database(database))
    }
}

pub(crate) fn bootstrap_material(
    generate: Option<&OsString>,
    bootstrap_file: Option<&OsString>,
    bootstrap_stdin: bool,
    bearer_output: Option<&OsString>,
    force: bool,
    stdin: &mut dyn Read,
) -> Result<BootstrapMaterial, CredentialError> {
    bootstrap_material_with_recovery(
        generate,
        bootstrap_file,
        bootstrap_stdin,
        bearer_output,
        force,
        stdin,
        RetentionRecoveryController::disabled(),
    )
}

fn bootstrap_material_with_recovery(
    generate: Option<&OsString>,
    bootstrap_file: Option<&OsString>,
    bootstrap_stdin: bool,
    bearer_output: Option<&OsString>,
    force: bool,
    stdin: &mut dyn Read,
    recovery: RetentionRecoveryController,
) -> Result<BootstrapMaterial, CredentialError> {
    let selected = usize::from(generate.is_some())
        + usize::from(bootstrap_file.is_some())
        + usize::from(bootstrap_stdin);
    if selected != 1 {
        return Err(CredentialError::BootstrapInvalid);
    }

    let generate_path = generate.map(checked_real_path).transpose()?;
    let bootstrap_file_path = bootstrap_file.map(checked_real_path).transpose()?;
    let bearer_output_path = bearer_output.map(checked_real_path).transpose()?;

    if let (Some(primary), Some(bearer)) = (
        generate_path.as_deref().or(bootstrap_file_path.as_deref()),
        bearer_output_path.as_deref(),
    ) && paths_alias(primary, bearer)
    {
        return Err(CredentialError::BootstrapInvalid);
    }

    let mut generated_created = false;
    let credential = if let Some(path) = generate_path.as_deref() {
        let milliseconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|duration| u64::try_from(duration.as_millis()).ok())
            .ok_or(CredentialError::BootstrapGeneration)?;
        let credential = generate_bootstrap_credential(milliseconds, &SystemEntropy)
            .map_err(|_| CredentialError::BootstrapGeneration)?;
        let document = credential.render_document();
        remove_existing_regular_file_if_forced(path, force)?;
        let _retained = retain_and_revalidate_with_recovery(
            path,
            document.expose_secret(),
            &mut SystemRetention::default(),
            recovery,
            || load_bootstrap_credential_file(path).map_err(|_| CredentialError::Retention),
            |retained| retained.render_document().expose_secret() == document.expose_secret(),
        )?;
        generated_created = true;
        credential
    } else if let Some(path) = bootstrap_file_path.as_deref() {
        load_bootstrap_credential_file(path).map_err(|_| CredentialError::BootstrapInvalid)?
    } else {
        read_bootstrap_credential(stdin).map_err(|_| CredentialError::BootstrapInvalid)?
    };

    let bearer_retained = if let Some(path) = bearer_output_path.as_deref() {
        let text = std::str::from_utf8(credential.token().expose_secret())
            .map_err(|_| CredentialError::BootstrapInvalid)?;
        let expected =
            BearerCredential::new(text).map_err(|_| CredentialError::BootstrapInvalid)?;
        if let Err(error) = remove_existing_regular_file_if_forced(path, force).and_then(|()| {
            retain_and_revalidate_with_recovery(
                path,
                credential.token().expose_secret(),
                &mut SystemRetention::default(),
                recovery,
                || load_protected_bearer_credential(path).map_err(|_| CredentialError::Retention),
                |reread| reread.has_same_presentation(&expected),
            )
            .map(|_| ())
        }) {
            if generated_created && let Some(gen_path) = generate_path.as_deref() {
                let _ = std::fs::remove_file(gen_path);
            }
            return Err(error);
        }
        true
    } else {
        false
    };

    Ok(BootstrapMaterial {
        credential,
        bearer_retained,
    })
}

fn paths_alias(left: &Path, right: &Path) -> bool {
    if left == right {
        return true;
    }
    use std::os::unix::fs::MetadataExt as _;
    if let (Ok(left_meta), Ok(right_meta)) = (
        std::fs::symlink_metadata(left),
        std::fs::symlink_metadata(right),
    ) && left_meta.dev() == right_meta.dev()
        && left_meta.ino() == right_meta.ino()
    {
        return true;
    }
    if let (Ok(left_meta), Ok(right_meta)) = (std::fs::metadata(left), std::fs::metadata(right))
        && left_meta.dev() == right_meta.dev()
        && left_meta.ino() == right_meta.ino()
    {
        return true;
    }
    if let (Ok(left_canon), Ok(right_canon)) =
        (std::fs::canonicalize(left), std::fs::canonicalize(right))
        && left_canon == right_canon
    {
        return true;
    }
    let left_parent = left
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let right_parent = right
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    if let (Ok(left_dir), Ok(right_dir), Some(left_name), Some(right_name)) = (
        std::fs::canonicalize(left_parent),
        std::fs::canonicalize(right_parent),
        left.file_name(),
        right.file_name(),
    ) && left_dir == right_dir
        && left_name == right_name
    {
        return true;
    }
    false
}

fn remove_existing_regular_file_if_forced(path: &Path, force: bool) -> Result<(), CredentialError> {
    if !force {
        return Ok(());
    }
    match std::fs::symlink_metadata(path) {
        Ok(meta) => {
            if meta.file_type().is_symlink() || !meta.file_type().is_file() {
                return Err(CredentialError::Retention);
            }
            std::fs::remove_file(path).map_err(|_| CredentialError::Retention)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err(CredentialError::Retention),
    }
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

pub(crate) fn replace_normal_token(path: &OsString, token: String) -> Result<(), CredentialError> {
    let target = checked_real_path(path)?;
    if !target.exists() {
        return retain_normal_token(path, token);
    }
    let metadata = std::fs::symlink_metadata(&target).map_err(|_| CredentialError::Retention)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(CredentialError::Retention);
    }
    let token = Zeroizing::new(token);
    if token.len() != TOKEN_BYTES {
        return Err(CredentialError::Retention);
    }
    let expected = BearerCredential::new(token.as_str()).map_err(|_| CredentialError::Retention)?;
    let mut staging_os = target.as_os_str().to_os_string();
    staging_os.push(format!(".rotate-{}", std::process::id()));
    let staging = PathBuf::from(&staging_os);
    if staging.exists() {
        let _ = std::fs::remove_file(&staging);
    }
    let _reread = retain_and_revalidate(
        &staging,
        token.as_bytes(),
        &mut SystemRetention::default(),
        || load_protected_bearer_credential(&staging).map_err(|_| CredentialError::Retention),
        |reread| reread.has_same_presentation(&expected),
    )?;
    if std::fs::rename(&staging, &target).is_err() {
        let _ = std::fs::remove_file(&staging);
        return Err(CredentialError::Retention);
    }
    let parent = target
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut retention = SystemRetention::default();
    retention.open_directory(parent)?;
    retention.sync_directory()?;
    retention.close_directory()?;
    let reread =
        load_protected_bearer_credential(&target).map_err(|_| CredentialError::Retention)?;
    if !reread.has_same_presentation(&expected) {
        return Err(CredentialError::Retention);
    }
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
    retain_and_revalidate_with_recovery(
        path,
        bytes,
        retention,
        RetentionRecoveryController::disabled(),
        load,
        matches,
    )
}

fn retain_and_revalidate_with_recovery<T>(
    path: &Path,
    bytes: &[u8],
    retention: &mut dyn RetentionBackend,
    recovery: RetentionRecoveryController,
    load: impl FnOnce() -> Result<T, CredentialError>,
    matches: impl FnOnce(&T) -> bool,
) -> Result<T, CredentialError> {
    retain_bytes_with_recovery(path, bytes, retention, recovery)?;
    let retained = load()?;
    if !matches(&retained) {
        return Err(CredentialError::Retention);
    }
    Ok(retained)
}

#[cfg(test)]
fn retain_bytes_with(
    path: &Path,
    bytes: &[u8],
    retention: &mut dyn RetentionBackend,
) -> Result<(), CredentialError> {
    retain_bytes_with_recovery(
        path,
        bytes,
        retention,
        RetentionRecoveryController::disabled(),
    )
}

fn retain_bytes_with_recovery(
    path: &Path,
    bytes: &[u8],
    retention: &mut dyn RetentionBackend,
    recovery: RetentionRecoveryController,
) -> Result<(), CredentialError> {
    validate_path(path.as_os_str()).map_err(|_| CredentialError::Retention)?;
    retention.create(path)?;
    retention.write_all(bytes)?;
    retention.sync_file()?;
    recovery.reached(RetentionRecoveryBoundary::CredentialFileSynced);
    retention.close_file()?;

    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    retention.open_directory(parent)?;
    retention.sync_directory()?;
    recovery.reached(RetentionRecoveryBoundary::ParentDirectorySynced);
    retention.close_directory()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RetentionRecoveryBoundary {
    CredentialFileSynced,
    ParentDirectorySynced,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct RetentionRecoveryController {
    #[cfg(feature = "test-fixtures")]
    armed: Option<RetentionRecoveryBoundary>,
}

impl RetentionRecoveryController {
    const fn disabled() -> Self {
        Self {
            #[cfg(feature = "test-fixtures")]
            armed: None,
        }
    }

    #[cfg(feature = "test-fixtures")]
    const fn armed(boundary: RetentionRecoveryBoundary) -> Self {
        Self {
            armed: Some(boundary),
        }
    }

    fn reached(self, boundary: RetentionRecoveryBoundary) {
        #[cfg(not(feature = "test-fixtures"))]
        let _ = boundary;

        #[cfg(feature = "test-fixtures")]
        if self.armed == Some(boundary) {
            std::process::abort();
        }
    }
}

#[cfg(feature = "test-fixtures")]
/// Closed process boundary for generated bootstrap retention.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BootstrapRetentionTestPoint {
    /// Abort after the generated credential file has been synchronized.
    CredentialFileSynced,
    /// Abort after the containing directory has been synchronized.
    ParentDirectorySynced,
}

#[cfg(feature = "test-fixtures")]
/// Safe failure returned only when an armed fixture fails before aborting.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BootstrapRetentionFixtureError;

#[cfg(feature = "test-fixtures")]
/// Runs the exact generated-bootstrap retention path with one injected abort.
///
/// A successful return means the armed abort was not reached and is therefore
/// a fixture failure. Normal CLI construction cannot call this function.
pub fn run_bootstrap_retention_fixture(
    path: &Path,
    point: BootstrapRetentionTestPoint,
) -> Result<(), BootstrapRetentionFixtureError> {
    let generate = path.as_os_str().to_os_string();
    let recovery = RetentionRecoveryController::armed(match point {
        BootstrapRetentionTestPoint::CredentialFileSynced => {
            RetentionRecoveryBoundary::CredentialFileSynced
        }
        BootstrapRetentionTestPoint::ParentDirectorySynced => {
            RetentionRecoveryBoundary::ParentDirectorySynced
        }
    });
    bootstrap_material_with_recovery(
        Some(&generate),
        None,
        false,
        None,
        false,
        &mut std::io::empty(),
        recovery,
    )
    .map(|_| ())
    .map_err(|_| BootstrapRetentionFixtureError)
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
            database: riffdb_types::DatabaseAlias::default_alias(),
            output: OutputMode::Json,
            max_attempts: 3,
            credential_file: file,
            tls: None,
            config_file: None,
            project_mode: false,
            sources: [crate::config_diagnostics::ConfigSource::Default; 10],
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
        let scratch =
            tempfile::TempDir::with_prefix("riffdb-cli-retain-").expect("scratch directory");
        let path = scratch.path().join("riffdb-cli-retain");
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
    }

    // req: DX-004, HBK-003
    #[test]
    fn bootstrap_force_validates_inputs_and_rejects_aliasing_before_removing_existing_files() {
        let scratch =
            tempfile::TempDir::with_prefix("riffdb-cli-force-").expect("scratch directory");
        let bootstrap_path = scratch.path().join("bootstrap.cred");
        let bearer_path = scratch.path().join("bearer.cred");
        let bootstrap_os = bootstrap_path.as_os_str().to_os_string();
        let bearer_os = bearer_path.as_os_str().to_os_string();

        // 1. Seed a valid bootstrap file at bootstrap_path
        let seeded = bootstrap_material(
            Some(&bootstrap_os),
            None,
            false,
            None,
            false,
            &mut std::io::empty(),
        )
        .expect("generate initial bootstrap file");
        let seeded_bytes = fs::read(&bootstrap_path).expect("read seeded bootstrap file");

        // 2. Aliasing --bootstrap-file P --bearer-output P --force must fail without deleting P
        assert!(matches!(
            bootstrap_material(
                None,
                Some(&bootstrap_os),
                false,
                Some(&bootstrap_os),
                true,
                &mut std::io::empty(),
            ),
            Err(CredentialError::BootstrapInvalid)
        ));
        assert_eq!(
            fs::read(&bootstrap_path).expect("bootstrap file preserved after aliasing rejection"),
            seeded_bytes
        );

        // 3. Aliasing --generate P --bearer-output ./P --force must fail without deleting P
        let aliased_os = scratch.path().join("./bootstrap.cred").into_os_string();
        assert!(matches!(
            bootstrap_material(
                Some(&bootstrap_os),
                None,
                false,
                Some(&aliased_os),
                true,
                &mut std::io::empty(),
            ),
            Err(CredentialError::BootstrapInvalid)
        ));
        assert_eq!(
            fs::read(&bootstrap_path).expect("bootstrap file preserved after ./P aliasing"),
            seeded_bytes
        );

        // 4. Invalid --bootstrap-file with --bearer-output existing_Q --force must preserve existing_Q
        retain_bytes(&bearer_path, seeded.credential.token().expose_secret())
            .expect("seed existing bearer file");
        let invalid_bootstrap_os = scratch.path().join("missing.cred").into_os_string();
        assert!(matches!(
            bootstrap_material(
                None,
                Some(&invalid_bootstrap_os),
                false,
                Some(&bearer_os),
                true,
                &mut std::io::empty(),
            ),
            Err(CredentialError::BootstrapInvalid)
        ));
        assert_eq!(
            fs::read(&bearer_path)
                .expect("existing bearer file preserved on invalid bootstrap input"),
            seeded.credential.token().expose_secret()
        );
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
        let scratch =
            tempfile::TempDir::with_prefix("riffdb-cli-parent-file-").expect("scratch directory");
        let path = scratch.path().join("riffdb-cli-parent-file");
        fs::write(&path, b"not a directory").expect("regular file");
        let mut retention = SystemRetention::default();
        assert_eq!(
            retention.open_directory(&path),
            Err(CredentialError::Retention)
        );
        assert!(retention.directory.is_none());
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
