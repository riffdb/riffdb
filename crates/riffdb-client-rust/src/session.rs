//! Optional bounded multiplexed transport for generated application operations.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use futures_util::stream;
use riffdb_proto::{decode_application_error, decode_public_error, v1};
use tokio::sync::{mpsc, oneshot, watch};
use tonic::{Code, Status};

use crate::status::{checked_application_status, checked_status};
use crate::{
    CallMetadata, ClientError, DetailsFreeStatus, OutcomeUnknown, ProtocolFailure,
    ProtocolFailureKind, RiffDbClient, generate_request_id,
};

/// Version of the first bounded application-session protocol.
pub const APPLICATION_SESSION_PROTOCOL_V1: u32 = 1;
/// Hard public ceiling for independently in-flight session operations.
pub const MAX_APPLICATION_SESSION_IN_FLIGHT: u16 = 128;
const OUTBOUND_SESSION_ITEMS: usize = 257;

/// Invalid local construction of one exact application-session identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApplicationSessionConfigurationError;

impl fmt::Display for ApplicationSessionConfigurationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("application session identity is invalid")
    }
}

impl std::error::Error for ApplicationSessionConfigurationError {}

/// Exact immutable application identity selected by one optional session.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationSessionIdentity {
    contract_lineage: String,
    contract_version: u64,
    contract_bundle_hash: [u8; 32],
    query_module_hashes: Vec<[u8; 32]>,
    application_lock_hash: [u8; 32],
    maximum_in_flight: u16,
}

impl ApplicationSessionIdentity {
    /// Builds one canonical exact identity. Module hashes must already be in
    /// strict byte order so generated lock drift fails locally rather than
    /// being silently normalized.
    pub fn new(
        contract_lineage: String,
        contract_version: u64,
        contract_bundle_hash: [u8; 32],
        query_module_hashes: Vec<[u8; 32]>,
        application_lock_hash: [u8; 32],
        maximum_in_flight: u16,
    ) -> Result<Self, ApplicationSessionConfigurationError> {
        if contract_lineage.is_empty()
            || contract_lineage.len() > 256
            || contract_version == 0
            || maximum_in_flight == 0
            || maximum_in_flight > MAX_APPLICATION_SESSION_IN_FLIGHT
            || query_module_hashes.len() > 32
            || query_module_hashes
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
        {
            return Err(ApplicationSessionConfigurationError);
        }
        Ok(Self {
            contract_lineage,
            contract_version,
            contract_bundle_hash,
            query_module_hashes,
            application_lock_hash,
            maximum_in_flight,
        })
    }

    fn open_request(&self, request_id: Vec<u8>) -> v1::ApplicationSessionOpen {
        v1::ApplicationSessionOpen {
            protocol_version: APPLICATION_SESSION_PROTOCOL_V1,
            contract: Some(riffdb_proto::app::v1::ContractSelector {
                lineage: self.contract_lineage.clone(),
                version: self.contract_version,
                bundle_hash: self.contract_bundle_hash.to_vec(),
            }),
            query_module_hashes: self
                .query_module_hashes
                .iter()
                .map(|hash| hash.to_vec())
                .collect(),
            application_lock_hash: self.application_lock_hash.to_vec(),
            requested_max_in_flight: u32::from(self.maximum_in_flight),
            request_id,
        }
    }

    fn matches_opened(&self, opened: &v1::ApplicationSessionOpened) -> bool {
        let Some(contract) = opened.contract.as_ref() else {
            return false;
        };
        opened.protocol_version == APPLICATION_SESSION_PROTOCOL_V1
            && contract.lineage == self.contract_lineage
            && contract.version == self.contract_version
            && contract.bundle_hash.as_slice() == self.contract_bundle_hash
            && opened.application_lock_hash.as_slice() == self.application_lock_hash
            && opened.maximum_in_flight == u32::from(self.maximum_in_flight)
            && opened.query_module_hashes.len() == self.query_module_hashes.len()
            && opened
                .query_module_hashes
                .iter()
                .zip(&self.query_module_hashes)
                .all(|(actual, expected)| actual.as_slice() == expected)
    }
}

enum PendingResponse {
    Awaiting(oneshot::Sender<v1::ApplicationSessionResponse>),
    /// Bounded tombstone for a cancellation acknowledgement. Keeping this
    /// identity until the server answers prevents a deliberately cancelled
    /// operation from being misclassified as an unknown response.
    Ignore,
    /// One controlled close acknowledgement. Existing live calls remain in
    /// the map until the server drains them before acknowledging this entry.
    Closing,
}

/// Cloneable optional session transport. It retains only bounded correlation
/// state and no durable response history or semantic authority.
#[derive(Clone)]
pub(crate) struct BoundedApplicationSession {
    outbound: mpsc::Sender<v1::ApplicationSessionRequest>,
    pending: Arc<Mutex<BTreeMap<u64, PendingResponse>>>,
    closed: Arc<AtomicBool>,
    shutdown: watch::Sender<bool>,
    next_correlation: Arc<AtomicU64>,
    metadata: CallMetadata,
    maximum_in_flight: usize,
}

impl BoundedApplicationSession {
    pub(crate) async fn open(
        client: &mut RiffDbClient,
        identity: ApplicationSessionIdentity,
        metadata: CallMetadata,
    ) -> Result<Self, ClientError> {
        let (outbound, receiver) = mpsc::channel(OUTBOUND_SESSION_ITEMS);
        let (shutdown, shutdown_receiver) = watch::channel(false);
        let opening_correlation = 1_u64;
        outbound
            .send(v1::ApplicationSessionRequest {
                correlation_id: opening_correlation,
                request: Some(v1::application_session_request::Request::Open(
                    identity.open_request(
                        generate_request_id()
                            .map_err(ClientError::IdentifierGeneration)?
                            .into_bytes()
                            .to_vec(),
                    ),
                )),
            })
            .await
            .map_err(|_| ClientError::ConnectionFailure)?;
        let outbound_stream = session_outbound_stream(receiver, shutdown_receiver);
        let mut inbound = client
            .open_application_session(outbound_stream, &metadata)
            .await?;
        let opened = inbound
            .message()
            .await
            .map_err(checked_status)?
            .ok_or(ClientError::ConnectionFailure)?;
        let Some(v1::application_session_response::Response::Opened(opened_identity)) =
            opened.response.as_ref()
        else {
            return Err(invalid_inbound());
        };
        if opened.correlation_id != opening_correlation || !identity.matches_opened(opened_identity)
        {
            return Err(invalid_inbound());
        }

        let pending = Arc::new(Mutex::new(BTreeMap::<u64, PendingResponse>::new()));
        let response_pending = Arc::clone(&pending);
        let closed = Arc::new(AtomicBool::new(false));
        let response_closed = Arc::clone(&closed);
        tokio::spawn(async move {
            while let Ok(Some(response)) = inbound.message().await {
                if !route_session_response(&response_pending, response) {
                    break;
                }
            }
            response_closed.store(true, Ordering::Release);
            close_session_pending(&response_pending);
        });

        Ok(Self {
            outbound,
            pending,
            closed,
            shutdown,
            next_correlation: Arc::new(AtomicU64::new(opening_correlation + 1)),
            metadata,
            maximum_in_flight: usize::from(identity.maximum_in_flight),
        })
    }

    pub(crate) fn matches_metadata(&self, metadata: &CallMetadata) -> bool {
        self.metadata.has_same_session_scope(metadata)
    }

    /// Starts one controlled shutdown. The server drains accepted operations
    /// before acknowledging the close. This synchronous facade deliberately
    /// does not claim that the acknowledgement has arrived when it returns.
    pub(crate) fn close(&self) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        let sent = self.pending.lock().ok().is_some_and(|mut pending| {
            let correlation_id = self.next_correlation.fetch_add(1, Ordering::Relaxed);
            if correlation_id == 0 || correlation_id == u64::MAX {
                return false;
            }
            if pending
                .insert(correlation_id, PendingResponse::Closing)
                .is_some()
            {
                return false;
            }
            if self
                .outbound
                .try_send(v1::ApplicationSessionRequest {
                    correlation_id,
                    request: Some(v1::application_session_request::Request::Close(
                        v1::ApplicationSessionClose {},
                    )),
                })
                .is_err()
            {
                pending.remove(&correlation_id);
                return false;
            }
            true
        });
        if !sent {
            let _ = self.shutdown.send(true);
            close_session_pending(&self.pending);
        }
    }

    async fn call(
        &self,
        request: v1::application_session_request::Request,
    ) -> Result<v1::ApplicationSessionResponse, ClientError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(ClientError::ConnectionFailure);
        }
        let (sender, receiver) = oneshot::channel();
        let correlation_id = {
            let mut pending = self.pending.lock().map_err(|_| invalid_inbound())?;
            if self.closed.load(Ordering::Acquire) {
                return Err(ClientError::ConnectionFailure);
            }
            if pending
                .values()
                .filter(|pending| matches!(pending, PendingResponse::Awaiting(_)))
                .count()
                >= self.maximum_in_flight
                || pending.len() >= self.maximum_in_flight.saturating_mul(2)
            {
                return Err(local_capacity());
            }
            let correlation_id = self.next_correlation.fetch_add(1, Ordering::Relaxed);
            if correlation_id == 0 || correlation_id == u64::MAX {
                self.closed.store(true, Ordering::Release);
                return Err(invalid_inbound());
            }
            if pending
                .insert(correlation_id, PendingResponse::Awaiting(sender))
                .is_some()
            {
                self.closed.store(true, Ordering::Release);
                return Err(invalid_inbound());
            }
            if self
                .outbound
                .try_send(v1::ApplicationSessionRequest {
                    correlation_id,
                    request: Some(request),
                })
                .is_err()
            {
                pending.remove(&correlation_id);
                self.closed.store(true, Ordering::Release);
                return Err(ClientError::ConnectionFailure);
            }
            correlation_id
        };
        let mut guard = PendingCallGuard {
            correlation_id,
            next_correlation: Arc::clone(&self.next_correlation),
            outbound: self.outbound.clone(),
            pending: Arc::clone(&self.pending),
            closed: Arc::clone(&self.closed),
            maximum_in_flight: self.maximum_in_flight,
            armed: true,
        };
        let response = receiver.await;
        guard.armed = false;
        response.map_err(|_| ClientError::ConnectionFailure)
    }

    pub(crate) async fn execute_command(
        &self,
        request: v1::ExecuteCommandRequest,
    ) -> Result<v1::ExecuteCommandResponse, ClientError> {
        let response = self
            .call(v1::application_session_request::Request::Command(request))
            .await?;
        match response.response {
            Some(v1::application_session_response::Response::Command(response)) => Ok(response),
            Some(v1::application_session_response::Response::Failure(failure)) => {
                Err(session_failure(failure, SessionExpected::Command))
            }
            _ => Err(invalid_inbound()),
        }
    }

    pub(crate) async fn execute_query(
        &self,
        request: riffdb_proto::app::v1::ExecuteQueryRequest,
    ) -> Result<riffdb_proto::app::v1::ExecuteQueryResponse, ClientError> {
        let response = self
            .call(v1::application_session_request::Request::Query(request))
            .await?;
        match response.response {
            Some(v1::application_session_response::Response::Query(response)) => Ok(response),
            Some(v1::application_session_response::Response::Failure(failure)) => {
                Err(session_failure(failure, SessionExpected::Query))
            }
            _ => Err(invalid_inbound()),
        }
    }
}

fn session_outbound_stream(
    receiver: mpsc::Receiver<v1::ApplicationSessionRequest>,
    shutdown: watch::Receiver<bool>,
) -> impl futures_util::Stream<Item = v1::ApplicationSessionRequest> + Send + 'static {
    stream::unfold(
        (receiver, shutdown),
        |(mut receiver, mut shutdown)| async move {
            if *shutdown.borrow() {
                return None;
            }
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    match changed {
                        Ok(()) if *shutdown.borrow() => None,
                        // Dropping the final shutdown sender is an ordinary
                        // owner drop. Drain any already-enqueued close frame
                        // before ending the request stream.
                        Ok(()) | Err(_) => receiver
                            .recv()
                            .await
                            .map(|item| (item, (receiver, shutdown))),
                    }
                }
                item = receiver.recv() => item.map(|item| (item, (receiver, shutdown))),
            }
        },
    )
}

fn route_session_response(
    pending: &Mutex<BTreeMap<u64, PendingResponse>>,
    response: v1::ApplicationSessionResponse,
) -> bool {
    let pending = pending
        .lock()
        .ok()
        .and_then(|mut pending| pending.remove(&response.correlation_id));
    match pending {
        Some(PendingResponse::Awaiting(sender)) => {
            let _ = sender.send(response);
            true
        }
        Some(PendingResponse::Ignore) => true,
        Some(PendingResponse::Closing) => matches!(
            response.response,
            Some(v1::application_session_response::Response::Closed(_))
        ),
        None => false,
    }
}

fn close_session_pending(pending: &Mutex<BTreeMap<u64, PendingResponse>>) {
    if let Ok(mut pending) = pending.lock() {
        pending.clear();
    }
}

struct PendingCallGuard {
    correlation_id: u64,
    next_correlation: Arc<AtomicU64>,
    outbound: mpsc::Sender<v1::ApplicationSessionRequest>,
    pending: Arc<Mutex<BTreeMap<u64, PendingResponse>>>,
    closed: Arc<AtomicBool>,
    maximum_in_flight: usize,
    armed: bool,
}

impl Drop for PendingCallGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let retained = self.pending.lock().ok().is_some_and(|mut pending| {
            let cancel_correlation = self.next_correlation.fetch_add(1, Ordering::Relaxed);
            if cancel_correlation == 0 || cancel_correlation == u64::MAX {
                pending.remove(&self.correlation_id);
                return false;
            }
            if !matches!(
                pending.insert(self.correlation_id, PendingResponse::Ignore),
                Some(PendingResponse::Awaiting(_))
            ) || pending.len() >= self.maximum_in_flight.saturating_mul(2)
            {
                return false;
            }
            pending.insert(cancel_correlation, PendingResponse::Ignore);
            if self
                .outbound
                .try_send(v1::ApplicationSessionRequest {
                    correlation_id: cancel_correlation,
                    request: Some(v1::application_session_request::Request::Cancel(
                        v1::ApplicationSessionCancel {
                            target_correlation_id: self.correlation_id,
                        },
                    )),
                })
                .is_err()
            {
                pending.remove(&self.correlation_id);
                pending.remove(&cancel_correlation);
                return false;
            }
            true
        });
        if !retained {
            self.closed.store(true, Ordering::Release);
        }
    }
}

#[derive(Clone, Copy)]
enum SessionExpected {
    Command,
    Query,
}

fn session_failure(
    failure: v1::ApplicationSessionFailure,
    expected: SessionExpected,
) -> ClientError {
    let expected_kind = match expected {
        SessionExpected::Command => v1::ApplicationSessionOperationKind::Command,
        SessionExpected::Query => v1::ApplicationSessionOperationKind::Query,
    };
    if v1::ApplicationSessionOperationKind::try_from(failure.operation_kind) != Ok(expected_kind) {
        return invalid_inbound();
    }
    let code = Code::from_i32(failure.grpc_code);
    if failure.details.is_empty() {
        return match (expected, code) {
            (SessionExpected::Command, Code::Unknown) => {
                ClientError::OutcomeUnknown(OutcomeUnknown)
            }
            (_, Code::Unauthenticated) => {
                ClientError::DetailsFree(DetailsFreeStatus::Unauthenticated)
            }
            (_, Code::Cancelled) => ClientError::DetailsFree(DetailsFreeStatus::Cancelled),
            (_, Code::DeadlineExceeded) => {
                ClientError::DetailsFree(DetailsFreeStatus::DeadlineExceeded)
            }
            (_, Code::Unavailable) => {
                ClientError::DetailsFree(DetailsFreeStatus::TransportUnavailable)
            }
            (_, Code::Internal) => ClientError::DetailsFree(DetailsFreeStatus::EmergencyInternal),
            _ => ClientError::ConnectionFailure,
        };
    }
    match expected {
        SessionExpected::Command => match decode_public_error(&failure.details) {
            Ok(error) => checked_status(Status::with_details(
                code,
                error.safe_message(),
                failure.details.into(),
            )),
            Err(_) => invalid_inbound(),
        },
        SessionExpected::Query => match decode_application_error(&failure.details) {
            Ok(error) => checked_application_status(Status::with_details(
                code,
                error.safe_message(),
                failure.details.into(),
            )),
            Err(_) => invalid_inbound(),
        },
    }
}

fn invalid_inbound() -> ClientError {
    ClientError::Protocol(ProtocolFailure::new(
        ProtocolFailureKind::InvalidInboundMessage,
    ))
}

fn local_capacity() -> ClientError {
    ClientError::Public(riffdb_errors::PublicError::overloaded())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_session(
        maximum_in_flight: usize,
    ) -> (
        BoundedApplicationSession,
        mpsc::Receiver<v1::ApplicationSessionRequest>,
    ) {
        let (outbound, receiver) = mpsc::channel(OUTBOUND_SESSION_ITEMS);
        let (shutdown, _shutdown_receiver) = watch::channel(false);
        (
            BoundedApplicationSession {
                outbound,
                pending: Arc::new(Mutex::new(BTreeMap::new())),
                closed: Arc::new(AtomicBool::new(false)),
                shutdown,
                next_correlation: Arc::new(AtomicU64::new(2)),
                metadata: CallMetadata::default(),
                maximum_in_flight,
            },
            receiver,
        )
    }

    #[tokio::test]
    async fn explicit_close_is_correlated_and_ends_after_the_acknowledgement() {
        use futures_util::StreamExt;

        let (outbound, receiver) = mpsc::channel(OUTBOUND_SESSION_ITEMS);
        let (shutdown, shutdown_receiver) = watch::channel(false);
        let session = BoundedApplicationSession {
            outbound,
            pending: Arc::new(Mutex::new(BTreeMap::new())),
            closed: Arc::new(AtomicBool::new(false)),
            shutdown,
            next_correlation: Arc::new(AtomicU64::new(2)),
            metadata: CallMetadata::default(),
            maximum_in_flight: 1,
        };
        let stream = session_outbound_stream(receiver, shutdown_receiver);
        tokio::pin!(stream);
        session.close();
        let close = stream.next().await.expect("controlled close request");
        assert!(matches!(
            close.request,
            Some(v1::application_session_request::Request::Close(_))
        ));
        assert!(route_session_response(
            &session.pending,
            v1::ApplicationSessionResponse {
                correlation_id: close.correlation_id,
                response: Some(v1::application_session_response::Response::Closed(
                    v1::ApplicationSessionClosed {},
                )),
            },
        ));
        assert!(session.closed.load(Ordering::Acquire));
        assert!(session.pending.lock().expect("pending").is_empty());
        drop(session);
        assert!(stream.next().await.is_none());
    }

    #[test]
    fn identity_rejects_unsorted_modules_and_unbounded_inflight() {
        let high = [9; 32];
        let low = [8; 32];
        assert!(
            ApplicationSessionIdentity::new(
                "TicketDesk".to_owned(),
                1,
                [1; 32],
                vec![high, low],
                [2; 32],
                16,
            )
            .is_err()
        );
        assert!(
            ApplicationSessionIdentity::new(
                "TicketDesk".to_owned(),
                1,
                [1; 32],
                vec![low, high],
                [2; 32],
                129,
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn responses_complete_by_correlation_not_submission_order() {
        let (session, mut outbound) = test_session(2);
        let first_session = session.clone();
        let first = tokio::spawn(async move {
            first_session
                .execute_query(riffdb_proto::app::v1::ExecuteQueryRequest::default())
                .await
        });
        let second_session = session.clone();
        let second = tokio::spawn(async move {
            second_session
                .execute_query(riffdb_proto::app::v1::ExecuteQueryRequest::default())
                .await
        });
        let first_request = outbound.recv().await.expect("first request");
        let second_request = outbound.recv().await.expect("second request");
        assert!(first_request.correlation_id < second_request.correlation_id);

        for correlation_id in [second_request.correlation_id, first_request.correlation_id] {
            assert!(route_session_response(
                &session.pending,
                v1::ApplicationSessionResponse {
                    correlation_id,
                    response: Some(v1::application_session_response::Response::Query(
                        riffdb_proto::app::v1::ExecuteQueryResponse::default(),
                    )),
                },
            ));
        }
        assert!(first.await.expect("first task").is_ok());
        assert!(second.await.expect("second task").is_ok());
    }

    #[tokio::test]
    async fn in_flight_capacity_is_bounded_and_cancellation_is_explicit() {
        let (session, mut outbound) = test_session(1);
        let first_session = session.clone();
        let first = tokio::spawn(async move {
            first_session
                .execute_query(riffdb_proto::app::v1::ExecuteQueryRequest::default())
                .await
        });
        let submitted = outbound.recv().await.expect("submitted request");
        assert!(matches!(
            session
                .execute_query(riffdb_proto::app::v1::ExecuteQueryRequest::default())
                .await,
            Err(ClientError::Public(error))
                if error.kind() == riffdb_errors::PublicErrorKind::Overloaded
        ));

        first.abort();
        let _ = first.await;
        let cancellation = outbound.recv().await.expect("cancellation request");
        let cancellation_correlation = cancellation.correlation_id;
        let Some(v1::application_session_request::Request::Cancel(cancellation)) =
            cancellation.request
        else {
            panic!("dropped operation must send cancellation")
        };
        assert_eq!(cancellation.target_correlation_id, submitted.correlation_id);
        assert!(route_session_response(
            &session.pending,
            v1::ApplicationSessionResponse {
                correlation_id: submitted.correlation_id,
                response: Some(v1::application_session_response::Response::Failure(
                    v1::ApplicationSessionFailure {
                        operation_kind: v1::ApplicationSessionOperationKind::Query as i32,
                        grpc_code: Code::Cancelled as i32,
                        details: Vec::new(),
                    },
                )),
            },
        ));
        assert!(route_session_response(
            &session.pending,
            v1::ApplicationSessionResponse {
                correlation_id: cancellation_correlation,
                response: Some(v1::application_session_response::Response::Cancellation(
                    v1::ApplicationSessionCancellation {
                        target_correlation_id: submitted.correlation_id,
                        disposition: v1::ApplicationSessionCancellationDisposition::QueryCancelled
                            as i32,
                    },
                ),),
            },
        ));
        assert!(session.pending.lock().expect("pending").is_empty());
    }

    #[tokio::test]
    async fn stream_loss_releases_all_pending_calls() {
        let (session, mut outbound) = test_session(2);
        let call_session = session.clone();
        let call = tokio::spawn(async move {
            call_session
                .execute_query(riffdb_proto::app::v1::ExecuteQueryRequest::default())
                .await
        });
        let _submitted = outbound.recv().await.expect("submitted request");
        close_session_pending(&session.pending);
        session.closed.store(true, Ordering::Release);
        assert!(matches!(
            call.await.expect("call task"),
            Err(ClientError::ConnectionFailure)
        ));
        assert!(matches!(
            session
                .execute_query(riffdb_proto::app::v1::ExecuteQueryRequest::default())
                .await,
            Err(ClientError::ConnectionFailure)
        ));
    }

    #[tokio::test]
    async fn unknown_or_duplicate_response_identity_fails_closed() {
        let (session, mut outbound) = test_session(1);
        let call_session = session.clone();
        let call = tokio::spawn(async move {
            call_session
                .execute_query(riffdb_proto::app::v1::ExecuteQueryRequest::default())
                .await
        });
        let submitted = outbound.recv().await.expect("submitted request");
        assert!(!route_session_response(
            &session.pending,
            v1::ApplicationSessionResponse {
                correlation_id: submitted.correlation_id + 1,
                response: Some(v1::application_session_response::Response::Query(
                    riffdb_proto::app::v1::ExecuteQueryResponse::default(),
                )),
            },
        ));
        close_session_pending(&session.pending);
        session.closed.store(true, Ordering::Release);
        assert!(matches!(
            call.await.expect("call task"),
            Err(ClientError::ConnectionFailure)
        ));
        assert!(matches!(
            session
                .execute_query(riffdb_proto::app::v1::ExecuteQueryRequest::default())
                .await,
            Err(ClientError::ConnectionFailure)
        ));
    }
}
