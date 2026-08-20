from __future__ import annotations

import json
import os
from uuid import UUID

from client import (
    BetterAuthAcceptanceClient,
    ConsumeVerificationTokenInput,
    ConsumeVerificationTokenVerificationTokenConsumed,
    CreateUserAccountSessionsInput,
    CreateUserAccountSessionsUserAccountSessionsCreated,
    DeleteUsersCascadeLimitExceeded,
    DeleteUsersInput,
    DeleteUsersUserAccountsDeleted,
    GetSessionFound,
    GetSessionMissing,
    GetSessionParams,
    GetUserFound,
    GetUserParams,
    IssueVerificationTokenInput,
    IssueVerificationTokenVerificationTokenIssued,
    SignupGraphInput,
)
from riffdb_application import (
    AttemptBudget,
    BearerCredential,
    CallMetadata,
    DatabaseAlias,
    SyncApplicationTransport,
    Timestamp,
    VerifiedTlsConfig,
)


def uid(value: int) -> UUID:
    return UUID(f"018f0f8b-7c6d-7e31-8a4f-{value:012x}")


def require(condition: bool, label: str) -> None:
    if not condition:
        raise RuntimeError(f"Python Better Auth cascade assertion: {label}")


def main() -> None:
    organization_id = uid(101)
    user_id = UUID(os.environ["RIFFDB_BETTER_AUTH_USER_ID"])
    account_id = uid(103)
    session_id = uid(104)
    future = Timestamp(seconds=2_000_000_000, nanos=0)
    credential = BearerCredential.from_protected_file(
        os.environ["RIFFDB_CONFORMANCE_CREDENTIAL"]
    )
    metadata = CallMetadata.authenticated(credential).with_database(
        DatabaseAlias("default")
    )
    tls = VerifiedTlsConfig(
        endpoint=os.environ["RIFFDB_CONFORMANCE_ENDPOINT"],
        trust_root=os.environ["RIFFDB_CONFORMANCE_TRUST_ROOT"],
        server_name="127.0.0.1",
    )
    with SyncApplicationTransport.connect_verified_tls(tls, metadata) as transport:
        client = BetterAuthAcceptanceClient(transport, AttemptBudget(3))
        signup = CreateUserAccountSessionsInput(
            signups=(
                SignupGraphInput(
                    email="python@example.test",
                    user_id=user_id,
                    provider="password",
                    account_id=account_id,
                    expires_at=future,
                    session_id=session_id,
                    token_digest="sha256:python-session",
                    organization_id=organization_id,
                    provider_account_id="python@example.test",
                ),
            ),
            request_id=uid(110),
        )
        created = client.create_user_account_sessions(signup)
        require(
            isinstance(
                created.outcome, CreateUserAccountSessionsUserAccountSessionsCreated
            ),
            "typed signup",
        )
        session = client.get_session(
            GetSessionParams(
                organization_id=organization_id,
                user_id=user_id,
                session_id=session_id,
            )
        ).value
        require(
            isinstance(session, GetSessionFound)
            and session.session.token_digest == "sha256:python-session",
            "named secret read",
        )
        require(
            isinstance(
                client.get_user(
                    GetUserParams(
                        organization_id=organization_id, user_id=user_id
                    )
                ).value,
                GetUserFound,
            ),
            "named exact read",
        )

        token_id = uid(120)
        issued = client.issue_verification_token(
            IssueVerificationTokenInput(
                user_id=user_id,
                expires_at=future,
                request_id=uid(121),
                token_digest="sha256:python-verification",
                organization_id=organization_id,
                verification_token_id=token_id,
            )
        )
        require(
            isinstance(
                issued.outcome, IssueVerificationTokenVerificationTokenIssued
            ),
            "token issue",
        )
        consume = ConsumeVerificationTokenInput(
            user_id=user_id,
            request_id=uid(122),
            organization_id=organization_id,
            verification_token_id=token_id,
        )
        require(
            isinstance(
                client.consume_verification_token(consume).outcome,
                ConsumeVerificationTokenVerificationTokenConsumed,
            )
            and client.consume_verification_token(consume).replayed,
            "atomic consume replay",
        )

        deletion = DeleteUsersInput(
            user_ids=(user_id,),
            request_id=uid(130),
            organization_id=organization_id,
        )
        require(
            isinstance(client.delete_users(deletion).outcome, DeleteUsersUserAccountsDeleted)
            and client.delete_users(deletion).replayed,
            "cascade replay",
        )
        require(
            isinstance(
                client.get_session(
                    GetSessionParams(
                        organization_id=organization_id,
                        user_id=user_id,
                        session_id=session_id,
                    )
                ).value,
                GetSessionMissing,
            ),
            "session cleanup",
        )

        recreated = client.create_user_account_sessions(
            CreateUserAccountSessionsInput(signups=signup.signups, request_id=uid(140))
        )
        require(
            isinstance(
                recreated.outcome, CreateUserAccountSessionsUserAccountSessionsCreated
            ),
            "recreate",
        )
        for ordinal in range(9):
            issued = client.issue_verification_token(
                IssueVerificationTokenInput(
                    user_id=user_id,
                    expires_at=future,
                    request_id=uid(150 + ordinal),
                    token_digest=f"sha256:python-overflow-{ordinal}",
                    organization_id=organization_id,
                    verification_token_id=uid(170 + ordinal),
                )
            )
            require(
                isinstance(
                    issued.outcome, IssueVerificationTokenVerificationTokenIssued
                ),
                "overflow setup",
            )
        overflow = client.delete_users(
            DeleteUsersInput(
                user_ids=(user_id,),
                request_id=uid(190),
                organization_id=organization_id,
            )
        )
        require(
            isinstance(overflow.outcome, DeleteUsersCascadeLimitExceeded),
            "typed overflow",
        )
        require(
            isinstance(
                client.get_user(
                    GetUserParams(
                        organization_id=organization_id, user_id=user_id
                    )
                ).value,
                GetUserFound,
            ),
            "overflow zero mutation",
        )

    print(
        json.dumps(
            {
                "schema": "riffdb.adapter-cascade-observation/v1",
                "language": "python",
                "signup": True,
                "named_exact_read": True,
                "named_secret_read": True,
                "session_account_cleanup": True,
                "bounded_full_user_delete": True,
                "atomic_token_consume": True,
                "idempotent_replay": True,
                "typed_overflow": True,
                "overflow_zero_mutation": True,
            },
            separators=(",", ":"),
        )
    )


if __name__ == "__main__":
    main()
