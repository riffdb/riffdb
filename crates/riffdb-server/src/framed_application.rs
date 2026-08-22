//! Bounded framed generated-operation hosting over current lifecycle routes.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::future::{AbortHandle, Abortable};
use futures_util::stream::{FuturesUnordered, StreamExt};
use riffdb_api_application::{
    APPLICATION_SESSION_PROTOCOL_V1, ApplicationOperationPresentation, execute_generated_command,
    execute_generated_query, open_application_session,
};
use riffdb_api_frame::{Frame, read_frame, write_frame};
use riffdb_api_grpc::{
    EMERGENCY_INTERNAL_MESSAGE, GrpcDatabaseRoutes, SharedGrpcOperationRoute,
    status_from_application_error, status_from_application_operation_failure,
    status_from_public_error,
};
use riffdb_auth::RetainedOpaqueCredential;
use riffdb_errors::{
    ApplicationError, ApplicationErrorCode, ApplicationErrorContext, ApplicationOperation,
    PublicError,
};
use riffdb_proto::{app::v1 as app_v1, v1};
use riffdb_types::{ContractVersion, DatabaseAlias};
use tokio::io::{AsyncRead, AsyncWrite};
use tonic::Status;
use zeroize::Zeroize;

const MAX_APPLICATION_SESSION_IN_FLIGHT: usize = 128;
const MAX_APPLICATION_SESSION_WORK: u64 = 1_048_576;
const MAX_APPLICATION_SESSION_LIFETIME: Duration = Duration::from_secs(15 * 60);
const MAX_APPLICATION_SESSION_OUTPUT_STALL: Duration = Duration::from_secs(30);
const MAX_APPLICATION_SESSION_ERROR_DETAILS_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy)]
enum OperationKind {
    Command,
    Query,
}

impl OperationKind {
    const fn proto(self) -> i32 {
        match self {
            Self::Command => v1::ApplicationSessionOperationKind::Command as i32,
            Self::Query => v1::ApplicationSessionOperationKind::Query as i32,
        }
    }
}

type Operation = Pin<
    Box<
        dyn Future<Output = (u64, OperationKind, Option<v1::ApplicationSessionResponse>)>
            + Send
            + 'static,
    >,
>;

/// Serves one already trusted and protocol-selected framed byte stream.
pub(crate) async fn serve<IO>(
    stream: IO,
    routes: Arc<GrpcDatabaseRoutes>,
    operation_limit: Duration,
) where
    IO: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut reader, mut writer) = tokio::io::split(stream);
    let first = match tokio::time::timeout(operation_limit, read_frame(&mut reader)).await {
        Ok(Ok(frame)) => frame,
        Ok(Err(_)) | Err(_) => return,
    };
    let first = match first.decode_request() {
        Ok(first) => first,
        Err(_) => return,
    };
    let Some(v1::application_session_request::Request::Open(mut open)) = first.request else {
        return;
    };
    if open.protocol_version != APPLICATION_SESSION_PROTOCOL_V1
        || open.requested_max_in_flight == 0
        || open.requested_max_in_flight as usize > MAX_APPLICATION_SESSION_IN_FLIGHT
        || open.credential_presentation.is_empty()
        || open.database_alias.is_empty()
    {
        return;
    }
    let alias = match DatabaseAlias::new(open.database_alias.clone()) {
        Ok(alias) => alias,
        Err(_) => return,
    };
    let Some(lifecycle) = routes.select_alias(&alias) else {
        return;
    };
    let credential = match RetainedOpaqueCredential::new(&open.credential_presentation) {
        Ok(credential) => Arc::new(credential),
        Err(_) => return,
    };
    open.credential_presentation.zeroize();
    open.credential_presentation.clear();
    open.database_alias.clear();
    let route = SharedGrpcOperationRoute::new(lifecycle.as_ref());
    let deadline = match Instant::now().checked_add(operation_limit) {
        Some(deadline) => deadline,
        None => return,
    };
    let opened = match open_application_session(
        &route,
        ApplicationOperationPresentation::from_opaque(credential.borrow(), deadline),
        open.clone(),
    )
    .await
    {
        Ok(opened) => opened,
        Err(_) => return,
    };
    let response = v1::ApplicationSessionResponse {
        correlation_id: first.correlation_id,
        response: Some(v1::application_session_response::Response::Opened(opened)),
    };
    if !send(&mut writer, response).await {
        return;
    }

    let Some(contract) = open.contract else {
        return;
    };
    let maximum_in_flight = open.requested_max_in_flight as usize;
    let mut live = BTreeMap::<u64, (AbortHandle, OperationKind)>::new();
    let mut operations = FuturesUnordered::<Operation>::new();
    let mut accepted_work = 0_u64;
    let mut last_correlation_id = first.correlation_id;
    let mut closing = None;
    let lifetime = tokio::time::sleep(MAX_APPLICATION_SESSION_LIFETIME);
    tokio::pin!(lifetime);

    loop {
        if let Some(correlation_id) = closing
            && operations.is_empty()
        {
            let closed = v1::ApplicationSessionResponse {
                correlation_id,
                response: Some(v1::application_session_response::Response::Closed(
                    v1::ApplicationSessionClosed {},
                )),
            };
            let _ = send(&mut writer, closed).await;
            break;
        }
        tokio::select! {
            _ = &mut lifetime => break,
            completed = operations.next(), if !operations.is_empty() => {
                let Some((correlation_id, _kind, response)) = completed else { continue; };
                live.remove(&correlation_id);
                if let Some(response) = response
                    && !send(&mut writer, response).await
                {
                    break;
                }
            }
            frame = read_frame(&mut reader), if closing.is_none() => {
                let message = match frame.and_then(|frame| frame.decode_request().map_err(riffdb_api_frame::FrameIoError::InvalidFrame)) {
                    Ok(message) => message,
                    Err(_) => break,
                };
                if message.correlation_id == 0 || message.correlation_id <= last_correlation_id {
                    break;
                }
                last_correlation_id = message.correlation_id;
                let Some(request) = message.request else { break; };
                match request {
                    v1::application_session_request::Request::Cancel(cancel) => {
                        let (disposition, target) = match live.remove(&cancel.target_correlation_id) {
                            Some((abort, OperationKind::Command)) => {
                                abort.abort();
                                (
                                    v1::ApplicationSessionCancellationDisposition::CommandOutcomeUnknown,
                                    Some(failure(cancel.target_correlation_id, OperationKind::Command, Status::unknown(EMERGENCY_INTERNAL_MESSAGE))),
                                )
                            }
                            Some((abort, OperationKind::Query)) => {
                                abort.abort();
                                (
                                    v1::ApplicationSessionCancellationDisposition::QueryCancelled,
                                    Some(failure(cancel.target_correlation_id, OperationKind::Query, Status::cancelled(EMERGENCY_INTERNAL_MESSAGE))),
                                )
                            }
                            None => (v1::ApplicationSessionCancellationDisposition::NotLive, None),
                        };
                        if let Some(target) = target
                            && !send(&mut writer, target).await
                        {
                            break;
                        }
                        let response = v1::ApplicationSessionResponse {
                            correlation_id: message.correlation_id,
                            response: Some(v1::application_session_response::Response::Cancellation(
                                v1::ApplicationSessionCancellation {
                                    target_correlation_id: cancel.target_correlation_id,
                                    disposition: disposition as i32,
                                },
                            )),
                        };
                        if !send(&mut writer, response).await { break; }
                    }
                    v1::application_session_request::Request::Close(_) => {
                        closing = Some(message.correlation_id);
                    }
                    v1::application_session_request::Request::Open(_) => break,
                    operation => {
                        accepted_work = accepted_work.saturating_add(1);
                        if accepted_work > MAX_APPLICATION_SESSION_WORK { break; }
                        let kind = match scope(&operation, &contract, &open.query_module_hashes) {
                            Ok(kind) => kind,
                            Err((kind, status)) => {
                                if !send(&mut writer, failure(message.correlation_id, kind, status)).await { break; }
                                continue;
                            }
                        };
                        if operations.len() >= maximum_in_flight {
                            let status = match kind {
                                OperationKind::Command => status_from_public_error(&PublicError::overloaded()),
                                OperationKind::Query => status_from_application_error(&ApplicationError::new(
                                    ApplicationErrorCode::Overloaded,
                                    ApplicationOperation::ExecuteQuery,
                                    ApplicationErrorContext::empty(),
                                    None,
                                )),
                            };
                            if !send(&mut writer, failure(message.correlation_id, kind, status)).await { break; }
                            continue;
                        }
                        let (future, abort) = operation_future(
                            Arc::clone(&lifecycle),
                            Arc::clone(&credential),
                            operation_limit,
                            message.correlation_id,
                            operation,
                            kind,
                        );
                        live.insert(message.correlation_id, (abort, kind));
                        operations.push(future);
                    }
                }
            }
        }
    }
    for (_, (abort, _)) in live {
        abort.abort();
    }
}

fn scope(
    request: &v1::application_session_request::Request,
    contract: &app_v1::ContractSelector,
    module_hashes: &[Vec<u8>],
) -> Result<OperationKind, (OperationKind, Status)> {
    match request {
        v1::application_session_request::Request::Command(command) => {
            if command.expected_contract_version != Some(contract.version) {
                let status = ContractVersion::new(contract.version).map_or_else(
                    || Status::internal(EMERGENCY_INTERNAL_MESSAGE),
                    |version| status_from_public_error(&PublicError::contract_mismatch(version)),
                );
                Err((OperationKind::Command, status))
            } else {
                Ok(OperationKind::Command)
            }
        }
        v1::application_session_request::Request::Query(query) => {
            let selected = match query.contract.as_ref() {
                Some(selected) => selected,
                None => {
                    return Err((
                        OperationKind::Query,
                        Status::failed_precondition(EMERGENCY_INTERNAL_MESSAGE),
                    ));
                }
            };
            let code = if selected.lineage != contract.lineage
                || selected.version != contract.version
                || selected.bundle_hash != contract.bundle_hash
            {
                Some(ApplicationErrorCode::ContractMismatch)
            } else if !matches!(
                query.query,
                Some(app_v1::execute_query_request::Query::QueryName(_))
            ) {
                Some(ApplicationErrorCode::QueryInvalid)
            } else if !query.module_hash.as_ref().is_some_and(|selected| {
                module_hashes
                    .binary_search_by(|candidate| candidate.as_slice().cmp(selected.as_slice()))
                    .is_ok()
            }) {
                Some(ApplicationErrorCode::ModuleUnavailable)
            } else {
                None
            };
            match code {
                None => Ok(OperationKind::Query),
                Some(code) => Err((
                    OperationKind::Query,
                    status_from_application_error(&ApplicationError::new(
                        code,
                        ApplicationOperation::ExecuteQuery,
                        ApplicationErrorContext::empty(),
                        None,
                    )),
                )),
            }
        }
        v1::application_session_request::Request::Open(_)
        | v1::application_session_request::Request::Cancel(_)
        | v1::application_session_request::Request::Close(_) => Err((
            OperationKind::Query,
            Status::invalid_argument(EMERGENCY_INTERNAL_MESSAGE),
        )),
    }
}

fn operation_future(
    lifecycle: Arc<dyn riffdb_api_grpc::GrpcLifecycleRoute>,
    credential: Arc<RetainedOpaqueCredential>,
    operation_limit: Duration,
    correlation_id: u64,
    request: v1::application_session_request::Request,
    kind: OperationKind,
) -> (Operation, AbortHandle) {
    let (abort, registration) = AbortHandle::new_pair();
    let operation = async move {
        let deadline = Instant::now()
            .checked_add(operation_limit)
            .ok_or_else(|| Status::internal(EMERGENCY_INTERNAL_MESSAGE));
        let result = match (deadline, request) {
            (Ok(deadline), v1::application_session_request::Request::Command(command)) => {
                let route = SharedGrpcOperationRoute::new(lifecycle.as_ref());
                execute_generated_command(
                    &route,
                    ApplicationOperationPresentation::from_opaque(credential.borrow(), deadline),
                    command,
                )
                .await
                .map(|response| v1::ApplicationSessionResponse {
                    correlation_id,
                    response: Some(v1::application_session_response::Response::Command(
                        response,
                    )),
                })
                .map_err(status_from_application_operation_failure)
            }
            (Ok(deadline), v1::application_session_request::Request::Query(query)) => {
                let route = SharedGrpcOperationRoute::new(lifecycle.as_ref());
                execute_generated_query(
                    &route,
                    ApplicationOperationPresentation::from_opaque(credential.borrow(), deadline),
                    query,
                    true,
                )
                .await
                .map(|response| v1::ApplicationSessionResponse {
                    correlation_id,
                    response: Some(v1::application_session_response::Response::Query(response)),
                })
                .map_err(status_from_application_operation_failure)
            }
            (Err(status), _) => Err(status),
            (Ok(_), _) => Err(Status::invalid_argument(EMERGENCY_INTERNAL_MESSAGE)),
        };
        result.unwrap_or_else(|status| failure(correlation_id, kind, status))
    };
    let future = async move {
        let response = Abortable::new(operation, registration).await.ok();
        (correlation_id, kind, response)
    };
    (Box::pin(future), abort)
}

fn failure(
    correlation_id: u64,
    operation: OperationKind,
    status: Status,
) -> v1::ApplicationSessionResponse {
    let details = if status.details().len() <= MAX_APPLICATION_SESSION_ERROR_DETAILS_BYTES {
        status.details().to_vec()
    } else {
        Vec::new()
    };
    v1::ApplicationSessionResponse {
        correlation_id,
        response: Some(v1::application_session_response::Response::Failure(
            v1::ApplicationSessionFailure {
                operation_kind: operation.proto(),
                grpc_code: status.code() as i32,
                details,
            },
        )),
    }
}

async fn send<W>(writer: &mut W, response: v1::ApplicationSessionResponse) -> bool
where
    W: AsyncWrite + Unpin,
{
    let frame = match Frame::response(&response) {
        Ok(frame) => frame,
        Err(_) => return false,
    };
    matches!(
        tokio::time::timeout(
            MAX_APPLICATION_SESSION_OUTPUT_STALL,
            write_frame(writer, &frame)
        )
        .await,
        Ok(Ok(()))
    )
}
