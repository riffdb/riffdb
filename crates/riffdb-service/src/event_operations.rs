//! Symbolic event description, replay, and bounded tail orchestration.

use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant};

use riffdb_catalog::{
    EventMaterializationErrorKind, EventReplayErrorKind, EventReplayPosition, ResolvedEventReplay,
    SymbolicEventDescriptor, SymbolicEventEnvelope,
};
use riffdb_errors::{
    PublicError, ValidationCode, ValidationIssue, ValidationIssues, ValidationPath,
};
use riffdb_policy::{
    AuditClass, AuthorizedOperation, OperationRequest, OutputClassification, PartitionConstraint,
};
use riffdb_types::{
    ActorKind, CanonicalValue, ContractBundleHash, ContractLineage, ContractVersion, EventId,
    PartitionScopeV1, PlanHash, ProvenanceId, RequestId, ServiceAuditLinkV1, ServiceAuditPhaseV1,
    ServiceOperationV1, TenantScope, Timestamp,
};

use crate::orchestration::{AuditScope, BegunInvocation};
use crate::wait::{ControlledWaitError, wait_with_control};
use crate::{
    AuthoritativeCommitNotification, AuthoritativeCommitSubscriptionRequest,
    AuthoritativeReadError, AuthoritativeReadinessFailure, CommitNotificationSource,
    CursorAccessError, EventReplayCursorLookup, EventReplayCursorState, EventServiceApplication,
    InternalDefect, PageLimit, PageRequest, PortAdmissionError, PortDriverStopped, RequestContext,
    RiffDbService, RiffDbServiceInner, ServiceAuditTargetMap, ServiceFailure, ServiceFuture,
    ServiceResult, ensure_response_budget,
};

/// Maximum partition components accepted by a symbolic event request.
pub const MAX_EVENT_PARTITION_COMPONENTS: usize = 32;
/// Maximum selected fields accepted by a symbolic event request.
pub const MAX_EVENT_SELECTED_FIELDS: usize = 256;
/// Maximum wait for one unary tail request.
pub const MAX_EVENT_TAIL_WAIT: Duration = Duration::from_secs(30);
const MAX_EVENT_SYMBOL_BYTES: usize = 256;
const MAX_EVENT_TAIL_ROUTE_PAGES: u16 = 256;
const MAX_EVENT_TAIL_NOTIFICATIONS: u16 = 256;

/// One exact symbolic partition component.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventPartitionComponent {
    name: String,
    value: CanonicalValue,
}

impl EventPartitionComponent {
    /// Checks and retains one symbolic partition component.
    pub fn new(name: String, value: CanonicalValue) -> Result<Self, EventRequestError> {
        check_symbol(&name)?;
        Ok(Self { name, value })
    }

    /// Borrows the exact contract field name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Borrows the canonical typed value.
    #[must_use]
    pub const fn value(&self) -> &CanonicalValue {
        &self.value
    }
}

/// One explicit event type, exact partition, and payload-field selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventSelection {
    event_name: String,
    partition: Vec<EventPartitionComponent>,
    selected_fields: Vec<String>,
}

impl EventSelection {
    /// Checks all public bounds and rejects duplicate symbolic names.
    pub fn new(
        event_name: String,
        partition: Vec<EventPartitionComponent>,
        selected_fields: Vec<String>,
    ) -> Result<Self, EventRequestError> {
        check_symbol(&event_name)?;
        if partition.is_empty()
            || partition.len() > MAX_EVENT_PARTITION_COMPONENTS
            || selected_fields.is_empty()
            || selected_fields.len() > MAX_EVENT_SELECTED_FIELDS
        {
            return Err(EventRequestError);
        }
        for (index, component) in partition.iter().enumerate() {
            if partition[..index]
                .iter()
                .any(|prior| prior.name == component.name)
            {
                return Err(EventRequestError);
            }
        }
        for (index, field) in selected_fields.iter().enumerate() {
            check_symbol(field)?;
            if selected_fields[..index].contains(field) {
                return Err(EventRequestError);
            }
        }
        Ok(Self {
            event_name,
            partition,
            selected_fields,
        })
    }

    /// Borrows the exact event symbol.
    #[must_use]
    pub fn event_name(&self) -> &str {
        &self.event_name
    }

    /// Borrows the ordered partition tuple.
    #[must_use]
    pub fn partition(&self) -> &[EventPartitionComponent] {
        &self.partition
    }

    /// Borrows the explicit selected fields.
    #[must_use]
    pub fn selected_fields(&self) -> &[String] {
        &self.selected_fields
    }
}

/// A symbolic event request was empty, duplicated, or exceeded a fixed bound.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EventRequestError;

fn check_symbol(value: &str) -> Result<(), EventRequestError> {
    if value.is_empty() || value.len() > MAX_EVENT_SYMBOL_BYTES {
        Err(EventRequestError)
    } else {
        Ok(())
    }
}

/// Exact active-event description request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeEventRequest(String);

impl DescribeEventRequest {
    /// Checks and retains one event symbol.
    pub fn new(event_name: String) -> Result<Self, EventRequestError> {
        check_symbol(&event_name)?;
        Ok(Self(event_name))
    }

    /// Borrows the event symbol.
    #[must_use]
    pub fn event_name(&self) -> &str {
        &self.0
    }
}

/// One symbolic field descriptor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventFieldDescriptor {
    name: String,
    value_type: String,
}

impl EventFieldDescriptor {
    fn from_catalog(value: &riffdb_catalog::SymbolicEventFieldDescriptor) -> Self {
        Self {
            name: value.name().to_owned(),
            value_type: value.value_type().to_owned(),
        }
    }

    /// Borrows the field name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Borrows the canonical contract type spelling.
    #[must_use]
    pub fn value_type(&self) -> &str {
        &self.value_type
    }
}

/// Active symbolic event catalog descriptor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventDescriptor {
    lineage: ContractLineage,
    version: ContractVersion,
    bundle_hash: ContractBundleHash,
    event_name: String,
    application_streamable: bool,
    partition_fields: Vec<EventFieldDescriptor>,
    payload_fields: Vec<EventFieldDescriptor>,
}

impl EventDescriptor {
    fn from_catalog(
        lineage: ContractLineage,
        version: ContractVersion,
        bundle_hash: ContractBundleHash,
        descriptor: SymbolicEventDescriptor,
    ) -> Self {
        Self {
            lineage,
            version,
            bundle_hash,
            event_name: descriptor.event_name().to_owned(),
            application_streamable: descriptor.application_streamable(),
            partition_fields: descriptor
                .partition_fields()
                .iter()
                .map(EventFieldDescriptor::from_catalog)
                .collect(),
            payload_fields: descriptor
                .payload_fields()
                .iter()
                .map(EventFieldDescriptor::from_catalog)
                .collect(),
        }
    }

    /// Borrows the active lineage.
    #[must_use]
    pub const fn lineage(&self) -> &ContractLineage {
        &self.lineage
    }
    /// Returns the active contract version.
    #[must_use]
    pub const fn version(&self) -> ContractVersion {
        self.version
    }
    /// Returns the active bundle hash.
    #[must_use]
    pub const fn bundle_hash(&self) -> ContractBundleHash {
        self.bundle_hash
    }
    /// Borrows the event name.
    #[must_use]
    pub fn event_name(&self) -> &str {
        &self.event_name
    }
    /// Returns whether application replay is available.
    #[must_use]
    pub const fn application_streamable(&self) -> bool {
        self.application_streamable
    }
    /// Borrows the ordered partition fields.
    #[must_use]
    pub fn partition_fields(&self) -> &[EventFieldDescriptor] {
        &self.partition_fields
    }
    /// Borrows all payload fields.
    #[must_use]
    pub fn payload_fields(&self) -> &[EventFieldDescriptor] {
        &self.payload_fields
    }
}

/// Exact event-description result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescribeEventResult {
    /// No active event has the requested name.
    NotFound,
    /// Active checked descriptor.
    Found(EventDescriptor),
}

/// Public bounded event replay request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayEventsRequest {
    selection: EventSelection,
    after: Option<EventId>,
    page: PageRequest,
    observed_history_incarnation: Option<u64>,
}

impl ReplayEventsRequest {
    /// Creates one exact replay request.
    pub fn new(
        selection: EventSelection,
        after: Option<EventId>,
        page: PageRequest,
    ) -> Result<Self, EventRequestError> {
        if after.is_some() && page.cursor().is_some() {
            return Err(EventRequestError);
        }
        Ok(Self {
            selection,
            after,
            page,
            observed_history_incarnation: None,
        })
    }
    /// Attaches the caller-observed restore fence.
    #[must_use]
    pub const fn with_observed_history_incarnation(mut self, observed: Option<u64>) -> Self {
        self.observed_history_incarnation = observed;
        self
    }
    /// Borrows the selection.
    #[must_use]
    pub const fn selection(&self) -> &EventSelection {
        &self.selection
    }
    /// Returns the exclusive starting event.
    #[must_use]
    pub const fn after(&self) -> Option<EventId> {
        self.after
    }
    /// Returns page controls.
    #[must_use]
    pub const fn page(&self) -> PageRequest {
        self.page
    }
    /// Returns the optional restore fence.
    #[must_use]
    pub const fn observed_history_incarnation(&self) -> Option<u64> {
        self.observed_history_incarnation
    }
}

/// Public bounded event tail request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TailEventsRequest {
    replay: ReplayEventsRequest,
    maximum_wait: Duration,
}

impl TailEventsRequest {
    /// Creates one unary long-poll request.
    pub fn new(
        replay: ReplayEventsRequest,
        maximum_wait: Duration,
    ) -> Result<Self, EventRequestError> {
        if maximum_wait.is_zero()
            || maximum_wait > MAX_EVENT_TAIL_WAIT
            || replay.page.cursor().is_some()
        {
            return Err(EventRequestError);
        }
        Ok(Self {
            replay,
            maximum_wait,
        })
    }
    /// Borrows replay selection and starting position.
    #[must_use]
    pub const fn replay(&self) -> &ReplayEventsRequest {
        &self.replay
    }
    /// Returns the maximum long-poll wait.
    #[must_use]
    pub const fn maximum_wait(&self) -> Duration {
        self.maximum_wait
    }
}

/// One selected symbolic field value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SymbolicEventField {
    name: String,
    value: CanonicalValue,
}

impl SymbolicEventField {
    fn from_catalog(value: &riffdb_catalog::SymbolicEventField) -> Self {
        Self {
            name: value.name().to_owned(),
            value: value.value().clone(),
        }
    }
    /// Borrows the field name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Borrows the canonical value.
    #[must_use]
    pub const fn value(&self) -> &CanonicalValue {
        &self.value
    }
}

/// Safe symbolic event envelope released to application protocols.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SymbolicEvent {
    event_id: EventId,
    event_name: String,
    writer_contract_version: ContractVersion,
    writer_plan_hash: PlanHash,
    command_name: String,
    request_id: RequestId,
    root_request_id: RequestId,
    causing_event_id: Option<EventId>,
    occurred_at: Timestamp,
    actor_kind: ActorKind,
    provenance_id: ProvenanceId,
    history_incarnation: u64,
    fields: Vec<SymbolicEventField>,
}

impl SymbolicEvent {
    pub(crate) fn from_catalog(value: &SymbolicEventEnvelope) -> Self {
        Self {
            event_id: value.event_id(),
            event_name: value.event_name().to_owned(),
            writer_contract_version: value.writer_contract_version(),
            writer_plan_hash: value.writer_plan_hash(),
            command_name: value.command_name().to_owned(),
            request_id: value.request_id(),
            root_request_id: value.root_request_id(),
            causing_event_id: value.causing_event_id(),
            occurred_at: value.occurred_at(),
            actor_kind: value.actor_kind(),
            provenance_id: value.provenance_id(),
            history_incarnation: value.history_incarnation(),
            fields: value
                .fields()
                .iter()
                .map(SymbolicEventField::from_catalog)
                .collect(),
        }
    }
    /// Returns authoritative event identity.
    #[must_use]
    pub const fn event_id(&self) -> EventId {
        self.event_id
    }
    /// Borrows the event name.
    #[must_use]
    pub fn event_name(&self) -> &str {
        &self.event_name
    }
    /// Returns writer contract version.
    #[must_use]
    pub const fn writer_contract_version(&self) -> ContractVersion {
        self.writer_contract_version
    }
    /// Returns writer plan hash.
    #[must_use]
    pub const fn writer_plan_hash(&self) -> PlanHash {
        self.writer_plan_hash
    }
    /// Borrows originating command name.
    #[must_use]
    pub fn command_name(&self) -> &str {
        &self.command_name
    }
    /// Returns originating request identity.
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }
    /// Returns root correlation identity.
    #[must_use]
    pub const fn root_request_id(&self) -> RequestId {
        self.root_request_id
    }
    /// Returns optional causing event.
    #[must_use]
    pub const fn causing_event_id(&self) -> Option<EventId> {
        self.causing_event_id
    }
    /// Returns deterministic occurrence time.
    #[must_use]
    pub const fn occurred_at(&self) -> Timestamp {
        self.occurred_at
    }
    /// Returns non-identifying actor class.
    #[must_use]
    pub const fn actor_kind(&self) -> ActorKind {
        self.actor_kind
    }
    /// Returns opaque provenance identity.
    #[must_use]
    pub const fn provenance_id(&self) -> ProvenanceId {
        self.provenance_id
    }
    /// Returns restore fence.
    #[must_use]
    pub const fn history_incarnation(&self) -> u64 {
        self.history_incarnation
    }
    /// Borrows only explicitly selected payload fields.
    #[must_use]
    pub fn fields(&self) -> &[SymbolicEventField] {
        &self.fields
    }
}

/// One upper-fenced replay page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventPage {
    items: Vec<SymbolicEvent>,
    next_cursor: Option<crate::CursorToken>,
    observed_upper: Option<EventId>,
    history_incarnation: u64,
}

impl EventPage {
    /// Borrows selected events in exact partition order.
    #[must_use]
    pub fn items(&self) -> &[SymbolicEvent] {
        &self.items
    }
    /// Returns the opaque continuation.
    #[must_use]
    pub const fn next_cursor(&self) -> Option<crate::CursorToken> {
        self.next_cursor
    }
    /// Returns the captured inclusive route frontier.
    #[must_use]
    pub const fn observed_upper(&self) -> Option<EventId> {
        self.observed_upper
    }
    /// Returns the restore fence.
    #[must_use]
    pub const fn history_incarnation(&self) -> u64 {
        self.history_incarnation
    }
}

/// Successful replay result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayEventsResult(EventPage);
impl ReplayEventsResult {
    /// Borrows the complete page.
    #[must_use]
    pub const fn page(&self) -> &EventPage {
        &self.0
    }
}

/// Successful unary tail result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TailEventsResult {
    page: EventPage,
    wait_timed_out: bool,
}
impl TailEventsResult {
    /// Borrows the complete page.
    #[must_use]
    pub const fn page(&self) -> &EventPage {
        &self.page
    }
    /// Reports whether the wait elapsed without a matching event.
    #[must_use]
    pub const fn wait_timed_out(&self) -> bool {
        self.wait_timed_out
    }
}

/// Lower checked replay request including the catalog-owned partition proof.
pub struct AuthoritativeEventReplayRequest {
    replay: ResolvedEventReplay,
    position: EventReplayPosition,
    limit: PageLimit,
    history_incarnation: u64,
}

impl AuthoritativeEventReplayRequest {
    /// Decomposes the move-only request for one blocking storage call.
    #[must_use]
    pub fn into_parts(self) -> (ResolvedEventReplay, EventReplayPosition, PageLimit, u64) {
        (
            self.replay,
            self.position,
            self.limit,
            self.history_incarnation,
        )
    }
}

impl std::fmt::Debug for AuthoritativeEventReplayRequest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("AuthoritativeEventReplayRequest([CHECKED])")
    }
}

impl EventServiceApplication for RiffDbService {
    fn describe_event(
        &self,
        context: RequestContext,
        request: DescribeEventRequest,
    ) -> ServiceFuture<'_, DescribeEventResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(ServiceOperationV1::DescribeEvent, ingress, async move {
            describe_event(service, context, request).await
        })
    }

    fn replay_events(
        &self,
        context: RequestContext,
        request: ReplayEventsRequest,
    ) -> ServiceFuture<'_, ReplayEventsResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(ServiceOperationV1::ReplayEvents, ingress, async move {
            replay_events(service, context, request).await
        })
    }

    fn tail_events(
        &self,
        context: RequestContext,
        request: TailEventsRequest,
    ) -> ServiceFuture<'_, TailEventsResult> {
        let service = Arc::clone(&self.inner);
        let ingress = context.ingress();
        self.spawn_operation(ServiceOperationV1::TailEvents, ingress, async move {
            tail_events(service, context, request).await
        })
    }
}

async fn prepare_catalog(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    operation: ServiceOperationV1,
) -> ServiceResult<riffdb_catalog::ActiveCatalogSnapshot> {
    match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service
            .providers
            .catalog
            .prepare_active_catalog(context.control()),
    )
    .await
    {
        Ok(Ok(Some(catalog))) => Ok(catalog),
        Ok(Ok(None)) => Err(PublicError::storage_unavailable().into()),
        Ok(Err(error)) if error.kind() == riffdb_catalog::CatalogErrorKind::Storage => {
            Err(PublicError::storage_unavailable().into())
        }
        Ok(Err(_)) => Err(service.internal_failure(operation, InternalDefect::ProofMismatch)),
        Err(ControlledWaitError::Cancelled) => Err(ServiceFailure::Cancelled),
        Err(ControlledWaitError::DeadlineExceeded) => Err(ServiceFailure::DeadlineExceeded),
    }
}

async fn describe_event(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: DescribeEventRequest,
) -> ServiceResult<DescribeEventResult> {
    const OPERATION: ServiceOperationV1 = ServiceOperationV1::DescribeEvent;
    let catalog = prepare_catalog(&service, &context, OPERATION).await?;
    let pointer = catalog.pointer();
    let begun = service
        .begin_invocation(
            &context,
            OperationRequest::describe_event(pointer.lineage().clone(), pointer.contract_version()),
            ServiceAuditTargetMap::symbolic_query(
                pointer.lineage().clone(),
                pointer.contract_version(),
            )
            .map_err(|_| service.internal_failure(OPERATION, InternalDefect::ProofMismatch))?,
            AuditScope::StandardRead,
        )
        .await?;
    let authorization = begun.reauthorize(&service, &context).await?;
    if !valid_event_authorization(&service, &authorization, OPERATION, false) {
        let failure = service.internal_failure(OPERATION, InternalDefect::ProofMismatch);
        return Err(finish_failure(&service, &context, &begun, failure).await);
    }
    let result = catalog.describe_event(request.event_name()).map_or(
        DescribeEventResult::NotFound,
        |descriptor| {
            DescribeEventResult::Found(EventDescriptor::from_catalog(
                pointer.lineage().clone(),
                pointer.contract_version(),
                pointer.bundle_hash(),
                descriptor,
            ))
        },
    );
    if let Err(failure) = ensure_response_budget(&result) {
        return Err(finish_failure(&service, &context, &begun, failure).await);
    }
    finish_success(&service, &context, &begun).await?;
    Ok(result)
}

async fn replay_events(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: ReplayEventsRequest,
) -> ServiceResult<ReplayEventsResult> {
    execute_event_read(service, context, request, None)
        .await
        .map(|(page, _)| ReplayEventsResult(page))
}

async fn tail_events(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: TailEventsRequest,
) -> ServiceResult<TailEventsResult> {
    let maximum_wait = request.maximum_wait();
    let (page, wait_timed_out) =
        execute_event_read(service, context, request.replay, Some(maximum_wait)).await?;
    Ok(TailEventsResult {
        page,
        wait_timed_out,
    })
}

async fn execute_event_read(
    service: Arc<RiffDbServiceInner>,
    context: RequestContext,
    request: ReplayEventsRequest,
    tail_wait: Option<Duration>,
) -> ServiceResult<(EventPage, bool)> {
    let operation = if tail_wait.is_some() {
        ServiceOperationV1::TailEvents
    } else {
        ServiceOperationV1::ReplayEvents
    };
    check_incarnation(&service, request.observed_history_incarnation())?;
    let catalog = prepare_catalog(&service, &context, operation).await?;
    let pointer = catalog.pointer();
    let policy_request = match operation {
        ServiceOperationV1::ReplayEvents => OperationRequest::replay_events(
            pointer.lineage().clone(),
            pointer.contract_version(),
            request.page().limit().get(),
        ),
        ServiceOperationV1::TailEvents => OperationRequest::tail_events(
            pointer.lineage().clone(),
            pointer.contract_version(),
            request.page().limit().get(),
        ),
        _ => return Err(service.internal_failure(operation, InternalDefect::ProofMismatch)),
    };
    let begun = service
        .begin_invocation(
            &context,
            policy_request,
            ServiceAuditTargetMap::symbolic_query(
                pointer.lineage().clone(),
                pointer.contract_version(),
            )
            .map_err(|_| service.internal_failure(operation, InternalDefect::ProofMismatch))?,
            AuditScope::Intrinsic,
        )
        .await?;
    if !valid_event_authorization(&service, begun.initial_authorization(), operation, true) {
        let failure = service.internal_failure(operation, InternalDefect::ProofMismatch);
        return Err(finish_failure(&service, &context, &begun, failure).await);
    }

    let lookup = EventReplayCursorLookup::new(
        pointer.lineage().clone(),
        pointer.contract_version(),
        pointer.bundle_hash(),
        request.selection().clone(),
        request.page().limit(),
    );
    let cursor_state = match request.page().cursor() {
        Some(token) => match service.cursors.resolve_event_replay(
            token,
            context.principal().principal_id(),
            &lookup,
        ) {
            Ok(state) => Some(state),
            Err(CursorAccessError::InvalidCursor) => {
                return Err(finish_failure(&service, &context, &begun, invalid_cursor()).await);
            }
            Err(CursorAccessError::Unavailable) => {
                return Err(finish_failure(
                    &service,
                    &context,
                    &begun,
                    PublicError::storage_unavailable().into(),
                )
                .await);
            }
        },
        None => None,
    };
    let authorized_limit =
        effective_limit(request.page().limit(), begun.initial_authorization())
            .ok_or_else(|| service.internal_failure(operation, InternalDefect::ProofMismatch))?;
    let initial_limit = cursor_state.as_ref().map_or(authorized_limit, |state| {
        min_page_limit(authorized_limit, state.effective_limit())
    });
    let mut position = cursor_state.as_ref().map_or(
        EventReplayPosition::Initial {
            after: request.after(),
        },
        |state| state.position(),
    );

    let _subscriber_lease = match tail_wait {
        Some(_) => match service.reserve_commit_subscriber() {
            Ok(lease) => Some(lease),
            Err(_) => {
                return Err(finish_failure(
                    &service,
                    &context,
                    &begun,
                    PublicError::storage_unavailable().into(),
                )
                .await);
            }
        },
        None => None,
    };
    let mut tail_source = match tail_wait {
        Some(_) => Some(
            establish_tail_source(
                &service,
                &context,
                &begun,
                request.after().map(EventId::commit_sequence),
            )
            .await?,
        ),
        None => None,
    };
    let tail_deadline =
        match tail_wait {
            Some(wait) => Some(Instant::now().checked_add(wait).ok_or_else(|| {
                service.internal_failure(operation, InternalDefect::ProofMismatch)
            })?),
            None => None,
        };
    let mut route_pages = 0_u16;
    let mut notifications = 0_u16;

    loop {
        route_pages = route_pages
            .checked_add(1)
            .ok_or_else(|| service.internal_failure(operation, InternalDefect::ProofMismatch))?;
        if route_pages > MAX_EVENT_TAIL_ROUTE_PAGES && tail_source.is_some() {
            return Err(finish_failure(
                &service,
                &context,
                &begun,
                ServiceFailure::ResponseTooLarge,
            )
            .await);
        }
        let (lower_page, return_limit) = read_event_page(
            &service,
            &context,
            &begun,
            &catalog,
            request.selection(),
            position,
            initial_limit,
            operation,
        )
        .await?;
        let items = lower_page
            .items()
            .iter()
            .map(SymbolicEvent::from_catalog)
            .collect::<Vec<_>>();
        if items.len() > usize::from(return_limit.get().get()) {
            let failure = service.internal_failure(operation, InternalDefect::ProofMismatch);
            return Err(finish_failure(&service, &context, &begun, failure).await);
        }

        if !items.is_empty() || tail_source.is_none() {
            let page = publish_event_page(
                &service,
                &context,
                &begun,
                lookup,
                lower_page,
                items,
                return_limit,
            )
            .await?;
            return Ok((page, false));
        }

        if let Some(continuation) = lower_page.continuation() {
            position = EventReplayPosition::Continue(continuation);
            continue;
        }

        let observed_upper = lower_page.observed_upper();
        let deadline = tail_deadline
            .ok_or_else(|| service.internal_failure(operation, InternalDefect::ProofMismatch))?;
        let source = tail_source
            .as_mut()
            .ok_or_else(|| service.internal_failure(operation, InternalDefect::ProofMismatch))?;
        match wait_for_tail_notification(
            context.control(),
            service.providers.deadline_scheduler.as_ref(),
            deadline,
            source.next(),
        )
        .await
        {
            Ok(Ok(AuthoritativeCommitNotification::Advanced(_))) => {
                notifications = notifications.checked_add(1).ok_or_else(|| {
                    service.internal_failure(operation, InternalDefect::ProofMismatch)
                })?;
                if notifications > MAX_EVENT_TAIL_NOTIFICATIONS {
                    return Err(finish_failure(
                        &service,
                        &context,
                        &begun,
                        ServiceFailure::ResponseTooLarge,
                    )
                    .await);
                }
                position = EventReplayPosition::Initial {
                    after: observed_upper,
                };
            }
            Ok(Ok(
                AuthoritativeCommitNotification::Gap { .. }
                | AuthoritativeCommitNotification::Lagged { .. }
                | AuthoritativeCommitNotification::Closed,
            ))
            | Ok(Err(_)) => {
                return Err(finish_failure(
                    &service,
                    &context,
                    &begun,
                    PublicError::storage_unavailable().into(),
                )
                .await);
            }
            Err(TailWaitError::Cancelled) => {
                return Err(finish_controlled(
                    &service,
                    &context,
                    &begun,
                    ControlledWaitError::Cancelled,
                )
                .await);
            }
            Err(TailWaitError::RequestDeadline) => {
                return Err(finish_controlled(
                    &service,
                    &context,
                    &begun,
                    ControlledWaitError::DeadlineExceeded,
                )
                .await);
            }
            Err(TailWaitError::MaximumWait) => {
                let authorization = begun.reauthorize(&service, &context).await?;
                if !valid_event_authorization(&service, &authorization, operation, true) {
                    return Err(begun.finish_authorization_denial(&service, &context).await);
                }
                let page = EventPage {
                    items: Vec::new(),
                    next_cursor: None,
                    observed_upper,
                    history_incarnation: service.identity.history_incarnation(),
                };
                if let Err(failure) = ensure_response_budget(&page) {
                    return Err(finish_failure(&service, &context, &begun, failure).await);
                }
                finish_success(&service, &context, &begun).await?;
                return Ok((page, true));
            }
        }
    }
}

async fn establish_tail_source(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
    after: Option<riffdb_types::CommitSequence>,
) -> ServiceResult<Box<dyn CommitNotificationSource>> {
    let permit = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service
            .providers
            .authoritative
            .reserve_subscribe_to_commits(context.control()),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        Ok(Err(error)) => {
            return Err(finish_admission(service, context, begun, error).await);
        }
        Err(error) => {
            return Err(finish_controlled(service, context, begun, error).await);
        }
    };
    let authorization = begun.reauthorize(service, context).await?;
    if !valid_event_authorization(
        service,
        &authorization,
        ServiceOperationV1::TailEvents,
        true,
    ) {
        let failure = service.internal_failure(
            ServiceOperationV1::TailEvents,
            InternalDefect::ProofMismatch,
        );
        return Err(finish_failure(service, context, begun, failure).await);
    }
    let lower_request = AuthoritativeCommitSubscriptionRequest::new(after, PageLimit::default())
        .map_err(|_| {
            service.internal_failure(
                ServiceOperationV1::TailEvents,
                InternalDefect::ProofMismatch,
            )
        })?;
    let receipt = match permit.submit(lower_request) {
        Ok(receipt) => receipt,
        Err(error) => {
            return Err(finish_admission(service, context, begun, error).await);
        }
    };
    let source = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        receipt,
    )
    .await
    {
        Ok(Ok(Ok(source))) => source,
        Ok(Ok(Err(error))) => {
            let failure = map_authoritative(service, ServiceOperationV1::TailEvents, error);
            return Err(finish_failure(service, context, begun, failure).await);
        }
        Ok(Err(PortDriverStopped)) => {
            let failure = service.internal_failure(
                ServiceOperationV1::TailEvents,
                InternalDefect::ProofMismatch,
            );
            return Err(finish_failure(service, context, begun, failure).await);
        }
        Err(error) => {
            return Err(finish_controlled(service, context, begun, error).await);
        }
    };
    Ok(source)
}

#[allow(clippy::too_many_arguments)]
async fn read_event_page(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
    catalog: &riffdb_catalog::ActiveCatalogSnapshot,
    selection: &EventSelection,
    position: EventReplayPosition,
    maximum_limit: PageLimit,
    operation: ServiceOperationV1,
) -> ServiceResult<(riffdb_catalog::SymbolicEventReplayPage, PageLimit)> {
    let resolved = match catalog.resolve_event_replay(
        selection.event_name(),
        selection
            .partition()
            .iter()
            .map(|component| (component.name(), component.value().clone())),
        selection.selected_fields().iter().map(String::as_str),
    ) {
        Ok(resolved) => resolved,
        Err(error) => {
            return Err(finish_failure(service, context, begun, map_event_selection(error)).await);
        }
    };
    let permit = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        service
            .providers
            .authoritative
            .reserve_replay_events(context.control()),
    )
    .await
    {
        Ok(Ok(permit)) => permit,
        Ok(Err(error)) => return Err(finish_admission(service, context, begun, error).await),
        Err(error) => return Err(finish_controlled(service, context, begun, error).await),
    };
    let authorization = begun.reauthorize(service, context).await?;
    if !valid_event_authorization(service, &authorization, operation, true) {
        let failure = service.internal_failure(operation, InternalDefect::ProofMismatch);
        return Err(finish_failure(service, context, begun, failure).await);
    }
    let current_limit = effective_limit(maximum_limit, &authorization)
        .ok_or_else(|| service.internal_failure(operation, InternalDefect::ProofMismatch))?;
    let receipt = match permit.submit(AuthoritativeEventReplayRequest {
        replay: resolved,
        position,
        limit: current_limit,
        history_incarnation: service.identity.history_incarnation(),
    }) {
        Ok(receipt) => receipt,
        Err(error) => return Err(finish_admission(service, context, begun, error).await),
    };
    let lower_page = match wait_with_control(
        context.control(),
        service.providers.deadline_scheduler.as_ref(),
        receipt,
    )
    .await
    {
        Ok(Ok(Ok(page))) => page,
        Ok(Ok(Err(error))) => {
            let failure = map_authoritative(service, operation, error);
            return Err(finish_failure(service, context, begun, failure).await);
        }
        Ok(Err(PortDriverStopped)) => {
            let failure = service.internal_failure(operation, InternalDefect::ProofMismatch);
            return Err(finish_failure(service, context, begun, failure).await);
        }
        Err(error) => return Err(finish_controlled(service, context, begun, error).await),
    };
    let return_authorization = begun.reauthorize(service, context).await?;
    if !valid_event_authorization(service, &return_authorization, operation, true) {
        return Err(begun.finish_authorization_denial(service, context).await);
    }
    let return_limit = effective_limit(current_limit, &return_authorization)
        .ok_or_else(|| service.internal_failure(operation, InternalDefect::ProofMismatch))?;
    Ok((lower_page, return_limit))
}

#[allow(clippy::too_many_arguments)]
async fn publish_event_page(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
    lookup: EventReplayCursorLookup,
    lower_page: riffdb_catalog::SymbolicEventReplayPage,
    items: Vec<SymbolicEvent>,
    return_limit: PageLimit,
) -> ServiceResult<EventPage> {
    let cursor_guard = match lower_page.continuation() {
        Some(continuation) => {
            let guard = match service.cursors.register_event_replay_unpublished(
                context.principal().principal_id(),
                lookup,
                EventReplayCursorState::new(
                    EventReplayPosition::Continue(continuation),
                    return_limit,
                ),
            ) {
                Ok(guard) => guard,
                Err(_) => {
                    return Err(finish_failure(
                        service,
                        context,
                        begun,
                        PublicError::storage_unavailable().into(),
                    )
                    .await);
                }
            };
            Some(guard)
        }
        None => None,
    };
    let page = EventPage {
        items,
        next_cursor: cursor_guard
            .as_ref()
            .map(crate::CursorPublicationGuard::token),
        observed_upper: lower_page.observed_upper(),
        history_incarnation: service.identity.history_incarnation(),
    };
    if let Err(failure) = ensure_response_budget(&page) {
        return Err(finish_failure(service, context, begun, failure).await);
    }
    finish_success(service, context, begun).await?;
    if let Some(guard) = cursor_guard {
        guard.publish();
    }
    Ok(page)
}

fn min_page_limit(left: PageLimit, right: PageLimit) -> PageLimit {
    if left <= right { left } else { right }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TailWaitError {
    Cancelled,
    RequestDeadline,
    MaximumWait,
}

pub(crate) async fn wait_for_tail_notification<F>(
    control: &crate::RequestControl,
    deadline_scheduler: &dyn crate::RequestDeadlineScheduler,
    maximum_wait_deadline: Instant,
    future: F,
) -> Result<F::Output, TailWaitError>
where
    F: Future,
{
    let mut future = Box::pin(future);
    let mut cancelled = Box::pin(control.cancelled());
    let mut request_deadline = deadline_scheduler.wait_until(control.deadline());
    let mut maximum_wait = deadline_scheduler.wait_until(maximum_wait_deadline);

    poll_fn(|context| {
        if control.is_cancelled() || Pin::as_mut(&mut cancelled).poll(context).is_ready() {
            return Poll::Ready(Err(TailWaitError::Cancelled));
        }
        if control.is_deadline_exceeded()
            || Pin::as_mut(&mut request_deadline).poll(context).is_ready()
        {
            return Poll::Ready(Err(TailWaitError::RequestDeadline));
        }
        if Instant::now() >= maximum_wait_deadline
            || Pin::as_mut(&mut maximum_wait).poll(context).is_ready()
        {
            return Poll::Ready(Err(TailWaitError::MaximumWait));
        }
        Pin::as_mut(&mut future).poll(context).map(Ok)
    })
    .await
}

fn check_incarnation(service: &RiffDbServiceInner, observed: Option<u64>) -> ServiceResult<()> {
    if observed.is_some_and(|value| value != service.identity.history_incarnation()) {
        Err(PublicError::history_incarnation_mismatch().into())
    } else {
        Ok(())
    }
}

fn map_event_selection(error: riffdb_catalog::EventReplayError) -> ServiceFailure {
    match error.kind() {
        EventReplayErrorKind::Materialization(
            EventMaterializationErrorKind::UnknownSymbol
            | EventMaterializationErrorKind::NotStreamable
            | EventMaterializationErrorKind::HardLimit,
        ) => invalid_selection(),
        EventReplayErrorKind::Materialization(EventMaterializationErrorKind::Integrity)
        | EventReplayErrorKind::Integrity => PublicError::storage_unavailable().into(),
        EventReplayErrorKind::Storage(_) => PublicError::storage_unavailable().into(),
    }
}

fn invalid_selection() -> ServiceFailure {
    PublicError::validation(ValidationIssues::one(ValidationIssue::new(
        ValidationCode::InvalidValue,
        ValidationPath::root(),
    )))
    .into()
}

fn invalid_cursor() -> ServiceFailure {
    invalid_selection()
}

fn effective_limit(requested: PageLimit, authorization: &AuthorizedOperation) -> Option<PageLimit> {
    let requested = requested.get().get();
    let effective = authorization
        .obligations()
        .row_limit()
        .map_or(requested, |limit| requested.min(limit.get()));
    PageLimit::new(effective).ok()
}

fn valid_event_authorization(
    service: &RiffDbServiceInner,
    authorization: &AuthorizedOperation,
    operation: ServiceOperationV1,
    permits_limit: bool,
) -> bool {
    let obligations = authorization.obligations();
    let obligations_are_valid = match operation {
        ServiceOperationV1::DescribeEvent => {
            obligations.audit_class().is_none()
                && obligations.output_classification() == OutputClassification::PublicMetadata
                && obligations.partition_constraint().is_none()
                && obligations.field_mask().is_none()
                && obligations.row_limit().is_none()
        }
        ServiceOperationV1::ReplayEvents | ServiceOperationV1::TailEvents => {
            obligations.audit_class() == Some(AuditClass::AdministrativeRead)
                && obligations.output_classification()
                    == OutputClassification::AdministrativeRedactedData
                && obligations.effective_tenant_scope() == &TenantScope::Global
                && obligations.partition_constraint()
                    == Some(&PartitionConstraint::Filter(PartitionScopeV1::All))
                && obligations.field_mask().is_none()
                && (permits_limit || obligations.row_limit().is_none())
        }
        _ => false,
    };
    authorization.database_id() == service.identity.database_id()
        && authorization.environment() == service.identity.environment()
        && authorization.operation() == operation
        && obligations_are_valid
}

async fn finish_success(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
) -> ServiceResult<()> {
    if begun
        .finish(
            service,
            context,
            ServiceAuditPhaseV1::Succeeded,
            ServiceAuditLinkV1::None,
        )
        .await
        .is_err()
    {
        service.note_audit_failure(begun.initial_authorization().operation());
        return Err(PublicError::storage_unavailable().into());
    }
    Ok(())
}

async fn finish_failure(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
    failure: ServiceFailure,
) -> ServiceFailure {
    if begun
        .finish(
            service,
            context,
            ServiceAuditPhaseV1::Failed,
            ServiceAuditLinkV1::None,
        )
        .await
        .is_err()
    {
        service.note_audit_failure(begun.initial_authorization().operation());
        return PublicError::storage_unavailable().into();
    }
    failure
}

async fn finish_controlled(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
    error: ControlledWaitError,
) -> ServiceFailure {
    let failure = match error {
        ControlledWaitError::Cancelled => ServiceFailure::Cancelled,
        ControlledWaitError::DeadlineExceeded => ServiceFailure::DeadlineExceeded,
    };
    if begun
        .finish(
            service,
            context,
            ServiceAuditPhaseV1::Cancelled,
            ServiceAuditLinkV1::None,
        )
        .await
        .is_err()
    {
        service.note_audit_failure(begun.initial_authorization().operation());
        return PublicError::storage_unavailable().into();
    }
    failure
}

async fn finish_admission(
    service: &RiffDbServiceInner,
    context: &RequestContext,
    begun: &BegunInvocation,
    error: PortAdmissionError,
) -> ServiceFailure {
    match error {
        PortAdmissionError::Cancelled => {
            finish_controlled(service, context, begun, ControlledWaitError::Cancelled).await
        }
        PortAdmissionError::DeadlineExceeded => {
            finish_controlled(
                service,
                context,
                begun,
                ControlledWaitError::DeadlineExceeded,
            )
            .await
        }
        PortAdmissionError::Unavailable | PortAdmissionError::Stopped => {
            finish_failure(
                service,
                context,
                begun,
                PublicError::storage_unavailable().into(),
            )
            .await
        }
    }
}

fn map_authoritative(
    service: &RiffDbServiceInner,
    operation: ServiceOperationV1,
    error: AuthoritativeReadError,
) -> ServiceFailure {
    match error {
        AuthoritativeReadError::Unavailable => PublicError::storage_unavailable().into(),
        AuthoritativeReadError::Cancelled => ServiceFailure::Cancelled,
        AuthoritativeReadError::DeadlineExceeded => ServiceFailure::DeadlineExceeded,
        AuthoritativeReadError::HistoryPruned => PublicError::history_pruned().into(),
        AuthoritativeReadError::Integrity | AuthoritativeReadError::InvalidContinuation => {
            service
                .providers
                .health
                .fail_authoritative_readiness(AuthoritativeReadinessFailure::Integrity);
            service.internal_failure(operation, InternalDefect::ProofMismatch)
        }
    }
}
