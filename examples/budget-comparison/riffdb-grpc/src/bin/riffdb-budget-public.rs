//! Closed public-gRPC process runner for the budget comparison workload.

#![forbid(unsafe_code)]

use std::ffi::{OsStr, OsString};
use std::io::{self, Write};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::path::PathBuf;
use std::process::ExitCode;

use riffdb_budget_comparison_riffdb_grpc::{PublicComparisonCase, RiffDbPublicBudgetAdapter};
use riffdb_client_rust::load_protected_bearer_credential;
use tonic::transport::Endpoint;

const PROTOCOL: &str = "riffdb.budget.public-run/v1";
const MAX_ARGV_BYTES: usize = 8_192;
const MAX_ENDPOINT_BYTES: usize = 512;
const MAX_PATH_BYTES: usize = 4_096;

const CHECKED_ERROR: &[u8] = b"riffdb budget public run failed\n";
const INVALID_INVOCATION: &[u8] = b"riffdb budget public invocation invalid\n";
const SEQUENTIAL_SUCCESS: &[u8] = b"{\"schema\":\"riffdb.budget.public-run/v1\",\"adapter\":\"riffdb-public-grpc-v1\",\"case\":\"sequential\",\"workload_version\":1,\"status\":\"passed\"}\n";
const CONTENTION_SUCCESS: &[u8] = b"{\"schema\":\"riffdb.budget.public-run/v1\",\"adapter\":\"riffdb-public-grpc-v1\",\"case\":\"contention\",\"workload_version\":1,\"status\":\"passed\"}\n";
const SAME_KEY_REPLAY_SUCCESS: &[u8] = b"{\"schema\":\"riffdb.budget.public-run/v1\",\"adapter\":\"riffdb-public-grpc-v1\",\"case\":\"same_key_replay\",\"workload_version\":1,\"status\":\"passed\"}\n";

struct Invocation {
    case: PublicComparisonCase,
    success_line: &'static [u8],
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
            if write_exact(io::stdout().lock(), success_line).is_ok() {
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

async fn run(arguments: impl Iterator<Item = OsString>) -> Result<&'static [u8], RunError> {
    let invocation = parse_invocation(arguments)?;
    let credential = load_protected_bearer_credential(&invocation.credential_path)
        .map_err(|_| RunError::InvalidInvocation)?;
    let mut adapter = RiffDbPublicBudgetAdapter::connect(invocation.endpoint, credential)
        .await
        .map_err(|_| RunError::CheckedFailure)?;
    adapter
        .run_case(invocation.case)
        .await
        .map_err(|_| RunError::CheckedFailure)?;
    Ok(invocation.success_line)
}

fn parse_invocation(arguments: impl Iterator<Item = OsString>) -> Result<Invocation, RunError> {
    let [
        protocol_flag,
        protocol,
        case_flag,
        case,
        endpoint_flag,
        endpoint,
        credential_flag,
        credential_path,
    ] = collect_bounded_arguments(arguments)?;

    let protocol_flag = protocol_flag.to_str().ok_or(RunError::InvalidInvocation)?;
    let protocol = protocol.to_str().ok_or(RunError::InvalidInvocation)?;
    let case_flag = case_flag.to_str().ok_or(RunError::InvalidInvocation)?;
    let case = case.to_str().ok_or(RunError::InvalidInvocation)?;
    let endpoint_flag = endpoint_flag.to_str().ok_or(RunError::InvalidInvocation)?;
    let endpoint = endpoint.to_str().ok_or(RunError::InvalidInvocation)?;
    let credential_flag = credential_flag
        .to_str()
        .ok_or(RunError::InvalidInvocation)?;

    if protocol_flag != "--protocol"
        || protocol != PROTOCOL
        || case_flag != "--case"
        || endpoint_flag != "--endpoint"
        || credential_flag != "--credential-file"
    {
        return Err(RunError::InvalidInvocation);
    }

    let (case, success_line) = match case {
        "sequential" => (PublicComparisonCase::Sequential, SEQUENTIAL_SUCCESS),
        "contention" => (PublicComparisonCase::Contention, CONTENTION_SUCCESS),
        "same_key_replay" => (PublicComparisonCase::SameKeyReplay, SAME_KEY_REPLAY_SUCCESS),
        _ => return Err(RunError::InvalidInvocation),
    };
    let endpoint = checked_endpoint(endpoint)?;
    if platform_encoded_len(&credential_path) == 0
        || platform_encoded_len(&credential_path) > MAX_PATH_BYTES
        || platform_contains_nul(&credential_path)
    {
        return Err(RunError::InvalidInvocation);
    }

    Ok(Invocation {
        case,
        success_line,
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
            PROTOCOL,
            "--case",
            "sequential",
            "--endpoint",
            "http://127.0.0.1:7443",
            "--credential-file",
            "/tmp/riffdb-budget-credential",
        ]
        .map(OsString::from)
        .into()
    }

    #[test]
    fn public_comparison_exact_ordered_invocation_is_accepted() {
        let invocation = parse_invocation(valid_arguments().into_iter()).expect("invocation");
        assert_eq!(invocation.success_line, SEQUENTIAL_SUCCESS);
    }

    #[test]
    fn public_comparison_closed_case_spellings_select_exact_output() {
        for (case, expected) in [
            ("sequential", SEQUENTIAL_SUCCESS),
            ("contention", CONTENTION_SUCCESS),
            ("same_key_replay", SAME_KEY_REPLAY_SUCCESS),
        ] {
            let mut arguments = valid_arguments();
            arguments[3] = OsString::from(case);
            let invocation = parse_invocation(arguments.into_iter()).expect("invocation");
            assert_eq!(invocation.success_line, expected);
        }
    }

    #[test]
    fn public_comparison_malformed_argument_shapes_are_rejected() {
        let mut reordered = valid_arguments();
        reordered.swap(0, 2);
        let mut combined = valid_arguments();
        combined[0] = OsString::from("--protocol=riffdb.budget.public-run/v1");
        let mut duplicate = valid_arguments();
        duplicate[4] = OsString::from("--case");
        let mut unknown = valid_arguments();
        unknown[6] = OsString::from("--unknown");
        let mut empty_first_seven = valid_arguments();
        empty_first_seven[2] = OsString::new();
        let mut unknown_case = valid_arguments();
        unknown_case[3] = OsString::from("other");
        let mut empty_path = valid_arguments();
        empty_path[7] = OsString::new();
        let mut extra = valid_arguments();
        extra.push(OsString::from("extra"));

        for arguments in [
            reordered,
            combined,
            duplicate,
            unknown,
            empty_first_seven,
            unknown_case,
            empty_path,
            extra,
        ] {
            assert!(parse_invocation(arguments.into_iter()).is_err());
        }
        assert!(
            parse_invocation(valid_arguments().into_iter().take(7)).is_err(),
            "a missing final value must reject"
        );
    }

    #[test]
    fn public_comparison_endpoint_requires_exact_loopback_http_authority() {
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
    fn public_comparison_endpoint_and_path_bounds_are_enforced() {
        let mut long_endpoint = valid_arguments();
        long_endpoint[5] = OsString::from(format!("http://127.0.0.1:7443{}", "x".repeat(512)));
        assert!(parse_invocation(long_endpoint.into_iter()).is_err());

        let mut exact_path = valid_arguments();
        exact_path[7] = OsString::from("x".repeat(MAX_PATH_BYTES));
        assert!(parse_invocation(exact_path.into_iter()).is_ok());

        let mut long_path = valid_arguments();
        long_path[7] = OsString::from("x".repeat(MAX_PATH_BYTES + 1));
        assert!(parse_invocation(long_path.into_iter()).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn public_comparison_aggregate_argv_bound_accepts_8192_bytes_and_rejects_8193() {
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
    fn public_comparison_non_utf8_is_allowed_only_for_the_credential_path() {
        use std::os::unix::ffi::OsStringExt;

        let mut path = valid_arguments();
        path[7] = OsString::from_vec(vec![b'/', b't', b'm', b'p', b'/', 0xff]);
        assert!(parse_invocation(path.into_iter()).is_ok());

        for index in 0..7 {
            let mut arguments = valid_arguments();
            arguments[index] = OsString::from_vec(vec![0xff]);
            assert!(
                parse_invocation(arguments.into_iter()).is_err(),
                "first-seven argument {index}"
            );
        }

        let mut nul_path = valid_arguments();
        nul_path[7] = OsString::from_vec(vec![b'/', b't', b'm', b'p', b'/', 0]);
        assert!(parse_invocation(nul_path.into_iter()).is_err());
    }

    #[test]
    fn public_comparison_process_fixture_bytes_are_exact() {
        assert_eq!(
            include_bytes!("../../fixtures/public-run-v1-success.jsonl"),
            SEQUENTIAL_SUCCESS
        );
        assert_eq!(
            include_bytes!("../../fixtures/public-run-v1-checked-error.txt"),
            CHECKED_ERROR
        );
        assert_eq!(
            include_bytes!("../../fixtures/public-run-v1-invalid-invocation.txt"),
            INVALID_INVOCATION
        );
    }
}
