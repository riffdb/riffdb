//! Rust cell for the Better Auth bounded-cascade acceptance corpus.

#![forbid(unsafe_code)]
#![allow(dead_code, unreachable_pub)]

use std::error::Error;
use std::num::NonZeroU32;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use riffdb_client_rust::{
    AttemptBudget, CallMetadata, DatabaseAlias, StableApplicationClient,
    load_protected_bearer_credential,
};
use riffdb_config::{
    CanonicalHttpsEndpoint, ProtectedFilePath, TlsClientConfig, TlsServerIdentity,
};

#[allow(clippy::enum_variant_names, dead_code, unreachable_pub)]
mod generated {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../fixtures/adapters/better-auth/generated/rust/client.rs"
    ));
}

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Rust Better Auth cascade acceptance failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> TestResult<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run_async())
}

async fn run_async() -> TestResult<()> {
    let endpoint = required("RIFFDB_CONFORMANCE_ENDPOINT")?;
    let tls = TlsClientConfig::new(
        CanonicalHttpsEndpoint::parse(&endpoint)?,
        ProtectedFilePath::new(PathBuf::from(required("RIFFDB_CONFORMANCE_TRUST_ROOT")?))?,
        TlsServerIdentity::parse("127.0.0.1")?,
        Duration::from_secs(5),
        Duration::from_secs(30),
        NonZeroU32::new(4).ok_or("pool bound")?,
        NonZeroU32::new(64).ok_or("stream bound")?,
    )?;
    let credential = load_protected_bearer_credential(&PathBuf::from(required(
        "RIFFDB_CONFORMANCE_CREDENTIAL",
    )?))?;
    let metadata =
        CallMetadata::authenticated(credential).with_database(DatabaseAlias::default_alias());
    let transport = StableApplicationClient::connect_verified_tls(&tls).await?;
    let mut client = generated::BetterAuthAcceptanceClient::new(
        transport,
        metadata,
        AttemptBudget::new(3).ok_or("attempt budget")?,
    );

    let organization_id = id(1);
    let user_id = required("RIFFDB_BETTER_AUTH_USER_ID")?;
    let account_id = id(3);
    let session_id = id(4);
    let session_secret = "sha256:session-secret".to_owned();

    let signup = generated::CreateUserAccountSessionsInput {
        signups: vec![generated::SignupGraphInput {
            email: "owner@example.test".to_owned(),
            user_id: user_id.clone(),
            provider: "password".to_owned(),
            account_id,
            expires_at: future(),
            session_id: session_id.clone(),
            token_digest: session_secret.clone(),
            organization_id: organization_id.clone(),
            provider_account_id: "owner@example.test".to_owned(),
        }],
        request_id: id(10),
    };
    let created = client.create_user_account_sessions(signup.clone()).await?;
    expect(
        matches!(
            created.outcome,
            generated::CreateUserAccountSessionsOutcome::UserAccountSessionsCreated
        ),
        "typed signup outcome",
    )?;

    let session = client
        .get_session(generated::GetSessionParams {
            organization_id: organization_id.clone(),
            user_id: user_id.clone(),
            session_id: session_id.clone(),
        })
        .await?;
    let generated::GetSessionResult::Found(session) = session else {
        return Err("secret-bearing named session lookup was absent".into());
    };
    expect(
        session.session.token_digest == session_secret,
        "authorized secret output",
    )?;
    expect_user(&mut client, &organization_id, &user_id, true).await?;

    let token_id = id(20);
    let issued = client
        .issue_verification_token(generated::IssueVerificationTokenInput {
            user_id: user_id.clone(),
            expires_at: future(),
            request_id: id(21),
            token_digest: "sha256:verification-secret".to_owned(),
            organization_id: organization_id.clone(),
            verification_token_id: token_id.clone(),
        })
        .await?;
    expect(
        matches!(
            issued.outcome,
            generated::IssueVerificationTokenOutcome::VerificationTokenIssued { .. }
        ),
        "verification issue",
    )?;
    let consume_input = generated::ConsumeVerificationTokenInput {
        user_id: user_id.clone(),
        request_id: id(22),
        organization_id: organization_id.clone(),
        verification_token_id: token_id,
    };
    let consumed = client
        .consume_verification_token(consume_input.clone())
        .await?;
    let replayed_consume = client.consume_verification_token(consume_input).await?;
    expect(
        matches!(
            consumed.outcome,
            generated::ConsumeVerificationTokenOutcome::VerificationTokenConsumed { .. }
        ) && replayed_consume.replayed,
        "atomic token consume and replay",
    )?;

    let delete_input = generated::DeleteUsersInput {
        user_ids: vec![user_id.clone()],
        request_id: id(30),
        organization_id: organization_id.clone(),
    };
    let deleted = client.delete_users(delete_input.clone()).await?;
    let replayed_delete = client.delete_users(delete_input).await?;
    expect(
        matches!(
            deleted.outcome,
            generated::DeleteUsersOutcome::UserAccountsDeleted
        ) && replayed_delete.replayed,
        "atomic cascade and replay",
    )?;
    expect_user(&mut client, &organization_id, &user_id, false).await?;
    let session = client
        .get_session(generated::GetSessionParams {
            organization_id: organization_id.clone(),
            user_id: user_id.clone(),
            session_id,
        })
        .await?;
    expect(
        matches!(session, generated::GetSessionResult::Missing(_)),
        "session removed with user",
    )?;

    let recreated = client
        .create_user_account_sessions(generated::CreateUserAccountSessionsInput {
            request_id: id(40),
            ..signup
        })
        .await?;
    expect(
        matches!(
            recreated.outcome,
            generated::CreateUserAccountSessionsOutcome::UserAccountSessionsCreated
        ),
        "user recreation after tombstone",
    )?;
    for ordinal in 0..9_u64 {
        let issued = client
            .issue_verification_token(generated::IssueVerificationTokenInput {
                user_id: user_id.clone(),
                expires_at: future(),
                request_id: id(50 + ordinal),
                token_digest: format!("sha256:overflow-{ordinal}"),
                organization_id: organization_id.clone(),
                verification_token_id: id(70 + ordinal),
            })
            .await?;
        expect(
            matches!(
                issued.outcome,
                generated::IssueVerificationTokenOutcome::VerificationTokenIssued { .. }
            ),
            "overflow setup token",
        )?;
    }
    let overflow = client
        .delete_users(generated::DeleteUsersInput {
            user_ids: vec![user_id.clone()],
            request_id: id(90),
            organization_id: organization_id.clone(),
        })
        .await?;
    expect(
        matches!(
            overflow.outcome,
            generated::DeleteUsersOutcome::CascadeLimitExceeded
        ),
        "typed zero-mutation overflow",
    )?;
    expect_user(&mut client, &organization_id, &user_id, true).await?;

    println!(
        "{}",
        serde_json::json!({
            "schema": "riffdb.adapter-cascade-observation/v1",
            "language": "rust",
            "signup": true,
            "named_exact_read": true,
            "named_secret_read": true,
            "session_account_cleanup": true,
            "bounded_full_user_delete": true,
            "atomic_token_consume": true,
            "idempotent_replay": true,
            "typed_overflow": true,
            "overflow_zero_mutation": true,
        })
    );
    Ok(())
}

async fn expect_user(
    client: &mut generated::BetterAuthAcceptanceClient,
    organization_id: &str,
    user_id: &str,
    found: bool,
) -> TestResult<()> {
    let result = client
        .get_user(generated::GetUserParams {
            organization_id: organization_id.to_owned(),
            user_id: user_id.to_owned(),
        })
        .await?;
    expect(
        matches!(result, generated::GetUserResult::Found(_)) == found,
        "exact named user lookup",
    )
}

fn future() -> generated::TimestampValue {
    generated::TimestampValue {
        seconds: 2_000_000_000,
        nanos: 0,
    }
}

fn id(suffix: u64) -> String {
    format!("018f0f8b-7c6d-7e31-8a4f-{suffix:012x}")
}

fn required(name: &str) -> TestResult<String> {
    std::env::var(name).map_err(|_| format!("{name} is required").into())
}

fn expect(condition: bool, label: &'static str) -> TestResult<()> {
    if condition { Ok(()) } else { Err(label.into()) }
}
