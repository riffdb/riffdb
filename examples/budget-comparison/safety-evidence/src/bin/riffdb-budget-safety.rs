//! Closed process runner for the WP-139 safety evidence.

#![forbid(unsafe_code)]

use std::ffi::{OsStr, OsString};
use std::io::{self, Write};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::path::PathBuf;
use std::process::ExitCode;

use riffdb_budget_safety_evidence::{
    CHECKED_ERROR, INVALID_INVOCATION, SAFETY_EVIDENCE_SCHEMA, load_protected_postgres_url,
    render_report_jsonl, run_live_safety_evidence,
};
use riffdb_client_rust::load_protected_bearer_credential;
use tonic::transport::Endpoint;

const MAX_ARGV_BYTES: usize = 12_288;
const MAX_ENDPOINT_BYTES: usize = 512;
const MAX_PATH_BYTES: usize = 4_096;

struct Invocation {
    postgres_url_path: PathBuf,
    endpoint: Endpoint,
    credential_path: PathBuf,
}

#[derive(Clone, Copy, Debug)]
enum RunError {
    InvalidInvocation,
    CheckedFailure,
}

#[tokio::main]
async fn main() -> ExitCode {
    match run(std::env::args_os().skip(1)).await {
        Ok(success_line) => {
            if write_exact(io::stdout().lock(), success_line.as_bytes()).is_ok() {
                ExitCode::SUCCESS
            } else {
                let _ = write_exact(io::stderr().lock(), CHECKED_ERROR);
                ExitCode::from(1)
            }
        }
        Err(RunError::CheckedFailure) => {
            let _ = write_exact(io::stderr().lock(), CHECKED_ERROR);
            ExitCode::from(1)
        }
        Err(RunError::InvalidInvocation) => {
            let _ = write_exact(io::stderr().lock(), INVALID_INVOCATION);
            ExitCode::from(2)
        }
    }
}

async fn run(arguments: impl Iterator<Item = OsString>) -> Result<String, RunError> {
    let invocation = parse_invocation(arguments)?;
    let postgres_url = load_protected_postgres_url(&invocation.postgres_url_path)
        .map_err(|_| RunError::InvalidInvocation)?;
    let credential = load_protected_bearer_credential(&invocation.credential_path)
        .map_err(|_| RunError::InvalidInvocation)?;
    let report = run_live_safety_evidence(&postgres_url, invocation.endpoint, credential)
        .await
        .map_err(|_| RunError::CheckedFailure)?;
    render_report_jsonl(&report).map_err(|_| RunError::CheckedFailure)
}

fn parse_invocation(arguments: impl Iterator<Item = OsString>) -> Result<Invocation, RunError> {
    let [
        protocol_flag,
        protocol,
        postgres_flag,
        postgres_url_path,
        endpoint_flag,
        endpoint,
        credential_flag,
        credential_path,
    ] = collect_bounded_arguments(arguments)?;

    if protocol_flag.to_str() != Some("--protocol")
        || protocol.to_str() != Some(SAFETY_EVIDENCE_SCHEMA)
        || postgres_flag.to_str() != Some("--postgres-url-file")
        || endpoint_flag.to_str() != Some("--endpoint")
        || credential_flag.to_str() != Some("--credential-file")
    {
        return Err(RunError::InvalidInvocation);
    }
    let endpoint = endpoint.to_str().ok_or(RunError::InvalidInvocation)?;
    let endpoint = checked_endpoint(endpoint)?;
    checked_path(&postgres_url_path)?;
    checked_path(&credential_path)?;
    Ok(Invocation {
        postgres_url_path: PathBuf::from(postgres_url_path),
        endpoint,
        credential_path: PathBuf::from(credential_path),
    })
}

fn collect_bounded_arguments(
    arguments: impl Iterator<Item = OsString>,
) -> Result<[OsString; 8], RunError> {
    let mut bounded = Vec::with_capacity(8);
    let mut total_bytes = 0_usize;
    for argument in arguments {
        if bounded.len() == 8 {
            return Err(RunError::InvalidInvocation);
        }
        total_bytes = total_bytes
            .checked_add(platform_encoded_len(&argument))
            .ok_or(RunError::InvalidInvocation)?;
        if total_bytes > MAX_ARGV_BYTES {
            return Err(RunError::InvalidInvocation);
        }
        bounded.push(argument);
    }
    bounded.try_into().map_err(|_| RunError::InvalidInvocation)
}

fn checked_path(value: &OsStr) -> Result<(), RunError> {
    let length = platform_encoded_len(value);
    if length == 0 || length > MAX_PATH_BYTES || platform_contains_nul(value) {
        return Err(RunError::InvalidInvocation);
    }
    Ok(())
}

fn checked_endpoint(value: &str) -> Result<Endpoint, RunError> {
    if value.is_empty() || value.len() > MAX_ENDPOINT_BYTES || !value.is_ascii() {
        return Err(RunError::InvalidInvocation);
    }
    let authority = value
        .strip_prefix("http://")
        .ok_or(RunError::InvalidInvocation)?;
    let (address, port) = if let Some(bracketed) = authority.strip_prefix('[') {
        let close = bracketed.find(']').ok_or(RunError::InvalidInvocation)?;
        let host = &bracketed[..close];
        let port = bracketed[close + 1..]
            .strip_prefix(':')
            .ok_or(RunError::InvalidInvocation)?;
        let address = host
            .parse::<Ipv6Addr>()
            .map(IpAddr::V6)
            .map_err(|_| RunError::InvalidInvocation)?;
        (address, port)
    } else {
        let (host, port) = authority
            .split_once(':')
            .ok_or(RunError::InvalidInvocation)?;
        if port.contains(':') {
            return Err(RunError::InvalidInvocation);
        }
        let address = host
            .parse::<Ipv4Addr>()
            .map(IpAddr::V4)
            .map_err(|_| RunError::InvalidInvocation)?;
        (address, port)
    };
    if !address.is_loopback()
        || port.is_empty()
        || !port.bytes().all(|byte| byte.is_ascii_digit())
        || port.parse::<u16>().ok().filter(|port| *port != 0).is_none()
    {
        return Err(RunError::InvalidInvocation);
    }
    Endpoint::from_shared(value.to_owned()).map_err(|_| RunError::InvalidInvocation)
}

fn write_exact(mut writer: impl Write, bytes: &[u8]) -> io::Result<()> {
    writer.write_all(bytes)
}

#[cfg(unix)]
fn platform_encoded_len(value: &OsStr) -> usize {
    use std::os::unix::ffi::OsStrExt;

    value.as_bytes().len()
}

#[cfg(unix)]
fn platform_contains_nul(value: &OsStr) -> bool {
    use std::os::unix::ffi::OsStrExt;

    value.as_bytes().contains(&0)
}

#[cfg(windows)]
fn platform_encoded_len(value: &OsStr) -> usize {
    use std::os::windows::ffi::OsStrExt;

    value.encode_wide().count().saturating_mul(2)
}

#[cfg(windows)]
fn platform_contains_nul(value: &OsStr) -> bool {
    use std::os::windows::ffi::OsStrExt;

    value.encode_wide().any(|unit| unit == 0)
}

#[cfg(not(any(unix, windows)))]
fn platform_encoded_len(value: &OsStr) -> usize {
    value.to_string_lossy().len()
}

#[cfg(not(any(unix, windows)))]
fn platform_contains_nul(value: &OsStr) -> bool {
    value.to_string_lossy().as_bytes().contains(&0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_arguments() -> Vec<OsString> {
        [
            "--protocol",
            SAFETY_EVIDENCE_SCHEMA,
            "--postgres-url-file",
            "/tmp/riffdb-budget-postgres-url",
            "--endpoint",
            "http://127.0.0.1:7443",
            "--credential-file",
            "/tmp/riffdb-budget-credential",
        ]
        .map(OsString::from)
        .into()
    }

    #[test]
    fn exact_ordered_invocation_is_accepted() {
        assert!(parse_invocation(valid_arguments().into_iter()).is_ok());
    }

    #[test]
    fn malformed_invocation_shapes_are_rejected() {
        let mut reordered = valid_arguments();
        reordered.swap(2, 4);
        let mut wrong_protocol = valid_arguments();
        wrong_protocol[1] = OsString::from("riffdb.budget.safety-evidence/v2");
        let mut empty_path = valid_arguments();
        empty_path[3] = OsString::new();
        let mut extra = valid_arguments();
        extra.push(OsString::from("extra"));

        for arguments in [reordered, wrong_protocol, empty_path, extra] {
            assert!(parse_invocation(arguments.into_iter()).is_err());
        }
        assert!(parse_invocation(valid_arguments().into_iter().take(7)).is_err());
    }

    #[test]
    fn endpoint_requires_exact_loopback_http_authority() {
        for endpoint in [
            "http://127.0.0.1:1",
            "http://127.255.255.254:65535",
            "http://[::1]:7443",
            "http://[0:0:0:0:0:0:0:1]:7443",
        ] {
            assert!(checked_endpoint(endpoint).is_ok(), "{endpoint}");
        }
        for endpoint in [
            "https://127.0.0.1:7443",
            "http://localhost:7443",
            "http://192.0.2.1:7443",
            "http://127.0.0.1",
            "http://127.0.0.1:0",
            "http://127.0.0.1:65536",
            "http://127.0.0.1:7443/",
            "http://127.0.0.1:7443?query",
            "http://::1:7443",
        ] {
            assert!(checked_endpoint(endpoint).is_err(), "{endpoint}");
        }
    }

    #[test]
    fn endpoint_and_path_bounds_are_enforced() {
        let mut long_endpoint = valid_arguments();
        long_endpoint[5] = OsString::from(format!(
            "http://127.0.0.1:7443{}",
            "x".repeat(MAX_ENDPOINT_BYTES)
        ));
        assert!(parse_invocation(long_endpoint.into_iter()).is_err());

        for index in [3, 7] {
            let mut exact_path = valid_arguments();
            exact_path[index] = OsString::from("x".repeat(MAX_PATH_BYTES));
            assert!(parse_invocation(exact_path.into_iter()).is_ok());

            let mut long_path = valid_arguments();
            long_path[index] = OsString::from("x".repeat(MAX_PATH_BYTES + 1));
            assert!(parse_invocation(long_path.into_iter()).is_err());
        }
    }

    #[cfg(unix)]
    #[test]
    fn aggregate_argv_bound_accepts_12288_bytes_and_rejects_12289() {
        let exact = [
            OsString::from("x".repeat(MAX_ARGV_BYTES - 7)),
            OsString::from("x"),
            OsString::from("x"),
            OsString::from("x"),
            OsString::from("x"),
            OsString::from("x"),
            OsString::from("x"),
            OsString::from("x"),
        ];
        assert!(collect_bounded_arguments(exact.into_iter()).is_ok());

        let over = [
            OsString::from("x".repeat(MAX_ARGV_BYTES - 6)),
            OsString::from("x"),
            OsString::from("x"),
            OsString::from("x"),
            OsString::from("x"),
            OsString::from("x"),
            OsString::from("x"),
            OsString::from("x"),
        ];
        assert!(collect_bounded_arguments(over.into_iter()).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn non_utf8_is_allowed_only_for_paths_and_embedded_nul_is_rejected() {
        use std::os::unix::ffi::OsStringExt;

        for index in [3, 7] {
            let mut path = valid_arguments();
            path[index] = OsString::from_vec(vec![b'/', b't', b'm', b'p', b'/', 0xff]);
            assert!(parse_invocation(path.into_iter()).is_ok());

            let mut nul_path = valid_arguments();
            nul_path[index] = OsString::from_vec(vec![b'/', b't', b'm', b'p', b'/', 0]);
            assert!(parse_invocation(nul_path.into_iter()).is_err());
        }

        for index in [0, 1, 2, 4, 5, 6] {
            let mut arguments = valid_arguments();
            arguments[index] = OsString::from_vec(vec![0xff]);
            assert!(
                parse_invocation(arguments.into_iter()).is_err(),
                "non-path argument {index}"
            );
        }
    }
}
