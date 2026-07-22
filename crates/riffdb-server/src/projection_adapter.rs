//! Truthful derived-projection boundary for the minimal P1 process graph.

#![allow(
    dead_code,
    reason = "WP-130 process composition installs this private provider"
)]

use std::fmt;

use riffdb_service::{
    BoxPortCapacityPermit, PortAdmissionError, PortCapacityPermit, PortFuture, PortReceipt,
    ProjectionPortError, ProjectionPortRequest, ProjectionPortResult, ProjectionQueryPort,
    ProjectionStateFence, ProjectionStatusSnapshot, ProjectionUnavailableReason, RequestControl,
    port_completion_channel,
};
use riffdb_types::{FrontierPosition, ProjectionIdentity};

/// Reports the accepted P1 state in which no derived projection worker is installed.
#[derive(Clone, Copy, Default)]
pub(crate) struct UnavailableProjectionPort;

impl ProjectionQueryPort for UnavailableProjectionPort {
    fn reserve_query_projection(
        &self,
        control: &RequestControl,
    ) -> PortFuture<
        '_,
        BoxPortCapacityPermit<ProjectionPortRequest, ProjectionPortResult, ProjectionPortError>,
        PortAdmissionError,
    > {
        let admission = checked_control(control).map(|()| {
            Box::new(UnavailableProjectionQueryPermit)
                as BoxPortCapacityPermit<
                    ProjectionPortRequest,
                    ProjectionPortResult,
                    ProjectionPortError,
                >
        });
        Box::pin(async move { admission })
    }

    fn reserve_projection_status(
        &self,
        control: &RequestControl,
    ) -> PortFuture<
        '_,
        BoxPortCapacityPermit<
            ProjectionIdentity,
            Option<ProjectionStatusSnapshot>,
            ProjectionPortError,
        >,
        PortAdmissionError,
    > {
        let admission = checked_control(control).map(|()| {
            Box::new(UnavailableProjectionStatusPermit)
                as BoxPortCapacityPermit<
                    ProjectionIdentity,
                    Option<ProjectionStatusSnapshot>,
                    ProjectionPortError,
                >
        });
        Box::pin(async move { admission })
    }
}

impl fmt::Debug for UnavailableProjectionPort {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("UnavailableProjectionPort([NOT_INSTALLED])")
    }
}

fn checked_control(control: &RequestControl) -> Result<(), PortAdmissionError> {
    if control.is_cancelled() {
        Err(PortAdmissionError::Cancelled)
    } else if control.is_deadline_exceeded() {
        Err(PortAdmissionError::DeadlineExceeded)
    } else {
        Ok(())
    }
}

struct UnavailableProjectionQueryPermit;

impl PortCapacityPermit<ProjectionPortRequest, ProjectionPortResult, ProjectionPortError>
    for UnavailableProjectionQueryPermit
{
    fn submit(
        self: Box<Self>,
        request: ProjectionPortRequest,
    ) -> Result<PortReceipt<ProjectionPortResult, ProjectionPortError>, PortAdmissionError> {
        let result = ProjectionStateFence::new(
            request.identity().clone(),
            None,
            FrontierPosition::BeforeFirst,
        )
        .map(|fence| ProjectionPortResult::Degraded {
            fence,
            reason: ProjectionUnavailableReason::Building,
        })
        .map_err(|_| ProjectionPortError::Integrity);
        let (completion, receipt) = port_completion_channel();
        completion.complete(result);
        Ok(receipt)
    }
}

struct UnavailableProjectionStatusPermit;

impl PortCapacityPermit<ProjectionIdentity, Option<ProjectionStatusSnapshot>, ProjectionPortError>
    for UnavailableProjectionStatusPermit
{
    fn submit(
        self: Box<Self>,
        identity: ProjectionIdentity,
    ) -> Result<
        PortReceipt<Option<ProjectionStatusSnapshot>, ProjectionPortError>,
        PortAdmissionError,
    > {
        let snapshot =
            ProjectionStatusSnapshot::uninitialized(identity, FrontierPosition::BeforeFirst);
        let (completion, receipt) = port_completion_channel();
        completion.complete(Ok(Some(snapshot)));
        Ok(receipt)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use riffdb_service::{PageLimit, ProjectionLifecycle, ProjectionQueryPort};
    use riffdb_types::{ContractLineage, ProjectionId, ProjectionPlanHash};

    use super::*;

    fn control() -> RequestControl {
        RequestControl::new(Instant::now() + Duration::from_secs(30)).0
    }

    fn identity() -> ProjectionIdentity {
        ProjectionIdentity::new(
            ContractLineage::new("budget").expect("lineage"),
            ProjectionId::first(),
            ProjectionPlanHash::from_bytes([3; 32]),
        )
    }

    #[tokio::test]
    async fn query_reports_building_without_inventing_projection_state() {
        let port = UnavailableProjectionPort;
        let permit = port
            .reserve_query_projection(&control())
            .await
            .expect("query admission");
        let request = ProjectionPortRequest::new(
            identity(),
            Vec::new(),
            None,
            Instant::now() + Duration::from_secs(30),
            PageLimit::default(),
            None,
        )
        .expect("projection request");
        let result = permit
            .submit(request)
            .expect("query submission")
            .completion()
            .await
            .expect("driver retained")
            .expect("typed projection result");

        assert!(matches!(
            result,
            ProjectionPortResult::Degraded {
                reason: ProjectionUnavailableReason::Building,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn status_is_uninitialized_at_before_first() {
        let port = UnavailableProjectionPort;
        let expected = identity();
        let permit = port
            .reserve_projection_status(&control())
            .await
            .expect("status admission");
        let snapshot = permit
            .submit(expected.clone())
            .expect("status submission")
            .completion()
            .await
            .expect("driver retained")
            .expect("typed status result")
            .expect("known projection status");

        assert_eq!(snapshot.identity(), &expected);
        assert_eq!(snapshot.lifecycle(), ProjectionLifecycle::Building);
        assert_eq!(snapshot.authoritative_head(), FrontierPosition::BeforeFirst);
    }

    #[tokio::test]
    async fn cancellation_is_rejected_before_a_permit_exists() {
        let (control, cancellation) = RequestControl::new(Instant::now() + Duration::from_secs(30));
        cancellation.cancel();
        let port = UnavailableProjectionPort;
        let result = port.reserve_query_projection(&control).await;
        assert!(matches!(result, Err(PortAdmissionError::Cancelled)));
    }
}
