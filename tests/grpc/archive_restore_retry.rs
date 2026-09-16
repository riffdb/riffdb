//! Real transport admission to the exact archive retry capability.
// req: REP-007, AFC-007
use super::*;

struct ArchiveRetryService(Arc<ProjectionService>);
impl RestoreRetryOfflineMaintenanceApplication for ArchiveRetryService {
    fn restore_offline_backup(
        &self,
        _invocation: RestoreOfflineBackupInvocation,
    ) -> ServiceFuture<'_, OfflineMaintenanceStartResult> {
        panic!("archive retry must never dispatch ordinary restore")
    }
    fn restore_archived_backup(
        &self,
        invocation: riffdb_service::RestoreArchivedBackupInvocation,
    ) -> ServiceFuture<'_, OfflineMaintenanceStartResult> {
        riffdb_service::OfflineMaintenanceApplication::restore_archived_backup(
            self.0.as_ref(),
            invocation,
        )
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archive_restore_retry_authenticates_only_exact_archive_input() {
    let database_id = DatabaseId::from_unix_milliseconds_and_random(31, [0x31; 10]).unwrap();
    let environment = Environment::new("grpc-retry").unwrap();
    let audience = Audience::new("grpc-loopback").unwrap();
    let authenticator = Arc::new(CountingAcceptingAuthenticator {
        principal: authenticated_principal(database_id, environment.clone(), audience.clone()),
        calls: AtomicUsize::new(0),
    });
    let wire = v1::RestoreArchivedBackupRequest {
        request_id: request_id(25).into_bytes().to_vec(),
        operation_id: maintenance_operation_id(4).into_bytes().to_vec(),
        backup_name: "restore".to_owned(),
        archive_name: "daily".to_owned(),
        stop_at_sequence: Some(7),
        replacement_confirmation:
            v1::OfflineMaintenanceReplacementConfirmation::AllowReplaceNonemptyTarget as i32,
    };
    let (_, checked) =
        riffdb_api_grpc::restore_archived_backup_request_from_proto(wire.clone()).unwrap();
    let service = Arc::new(ProjectionService::new());
    let route = Arc::new(MaintenanceRoute {
        retry: Some(ArchiveRetryRoute {
            operation_id: checked.operation_id(),
            input_hash: checked.input_hash(),
            service: Arc::new(ArchiveRetryService(service.clone())),
            security: CheckedGrpcRestoreRetrySecurityContext::new(
                authenticator.clone(),
                AuthenticationContext::new(database_id, environment, audience),
            ),
        }),
        ready: None,
        recovery: None,
        security: None,
        ready_admissions: Mutex::new(Vec::new()),
        recovery_admissions: Mutex::new(Vec::new()),
        security_fetches: AtomicUsize::new(0),
    });
    let application = GrpcApplication::new(
        route.clone(),
        GrpcRequestLimits::new(Duration::from_secs(30)).unwrap(),
    );
    let incoming = TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let address = incoming.local_addr().unwrap();
    let (shutdown_sender, shutdown_receiver) = oneshot::channel();
    let server = tokio::spawn(
        Server::builder()
            .add_service(application.admin_server())
            .serve_with_incoming_shutdown(incoming, async move {
                let _ = shutdown_receiver.await;
            }),
    );
    let channel = Endpoint::from_shared(format!("http://{address}"))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = AdminServiceClient::new(channel);
    for changed in 0..4 {
        let mut request = wire.clone();
        match changed {
            0 => request.operation_id = maintenance_operation_id(5).into_bytes().to_vec(),
            1 => request.archive_name = "other".into(),
            2 => request.stop_at_sequence = None,
            _ => request.replacement_confirmation = 0,
        }
        let mut request = Request::new(request);
        authorize(&mut request);
        assert!(client.restore_archived_backup(request).await.is_err());
    }
    let mut ordinary = Request::new(v1::RestoreOfflineBackupRequest {
        request_id: wire.request_id.clone(),
        operation_id: wire.operation_id.clone(),
        backup_name: wire.backup_name.clone(),
        replacement_confirmation: wire.replacement_confirmation,
    });
    authorize(&mut ordinary);
    assert!(client.restore_offline_backup(ordinary).await.is_err());
    assert_eq!(authenticator.calls(), 0);
    assert!(service.maintenance_invocations().is_empty());
    for ordinal in [26, 27] {
        let mut request = wire.clone();
        request.request_id = request_id(ordinal).into_bytes().to_vec();
        let mut request = Request::new(request);
        authorize(&mut request);
        let response = client
            .restore_archived_backup(request)
            .await
            .unwrap()
            .into_inner();
        riffdb_proto::validate_restore_archived_backup_exchange(&wire, &response).unwrap();
    }
    assert_eq!(authenticator.calls(), 2);
    assert_eq!(route.security_fetches(), 0);
    assert_eq!(
        service.maintenance_invocations(),
        vec![
            ObservedMaintenanceInvocation::Restore {
                redacted_invocation: "RestoreArchivedBackupInvocation([REDACTED])".into(),
            };
            2
        ]
    );
    drop(client);
    shutdown_sender.send(()).unwrap();
    server.await.unwrap().unwrap();
}
