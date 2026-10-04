//! Private, bounded recovery of one immutable generated application input.

use std::io::Read;
use std::path::Path;
use std::time::{Duration, Instant};

use riffdb_client_rust::{ApplicationClientError, ClientError, DetailsFreeStatus};

use super::Result;

const DEADLINE: Duration = Duration::from_secs(60);
const ATTEMPTS: u32 = 240;

pub(super) trait Attempt {
    type Value;

    fn submit(
        &mut self,
    ) -> impl std::future::Future<Output = std::result::Result<Self::Value, ApplicationClientError>> + Send;
}

pub(super) struct Update<'a> {
    pub(super) client: &'a mut super::current::CurrentClient,
    pub(super) input: super::application::UpdateTicketInput,
}

impl Attempt for Update<'_> {
    type Value = riffdb_client_rust::TypedCommandResult<super::application::UpdateTicketOutcome>;

    async fn submit(&mut self) -> std::result::Result<Self::Value, ApplicationClientError> {
        self.client.update_ticket(self.input.clone()).await
    }
}

pub(super) struct ReadTicket<'a> {
    pub(super) client: &'a mut super::current::CurrentClient,
    pub(super) input: super::application::GetTicketParams,
}

impl Attempt for ReadTicket<'_> {
    type Value = super::application::GetTicketResult;

    async fn submit(&mut self) -> std::result::Result<Self::Value, ApplicationClientError> {
        self.client.get_ticket(self.input.clone()).await
    }
}

fn recoverable(error: &ApplicationClientError) -> bool {
    match error {
        ApplicationClientError::Client(ClientError::OutcomeUnknown(_))
        | ApplicationClientError::Client(ClientError::DetailsFree(
            DetailsFreeStatus::TransportUnavailable,
        )) => true,
        ApplicationClientError::Client(ClientError::Application(error)) => matches!(
            error.recovery_action(),
            riffdb_errors::ApplicationRecoveryAction::Retry
                | riffdb_errors::ApplicationRecoveryAction::ResolveWithSameIdempotencyKey
        ),
        ApplicationClientError::Client(ClientError::Public(error)) => matches!(
            error.recovery_action(),
            riffdb_errors::RecoveryAction::Retry
                | riffdb_errors::RecoveryAction::ResolveWithSameIdempotencyKey
        ),
        _ => false,
    }
}

pub(super) fn planned_fault(root: Option<&Path>) -> Result<bool> {
    let Some(root) = root else {
        return Ok(false);
    };
    let path = root.join("load.recover");
    let metadata = match std::fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(_) => return Err("recovery_control"),
    };
    if !metadata.is_file() || metadata.len() > 128 {
        return Err("recovery_control");
    }
    let mut encoded = String::new();
    std::fs::File::open(path)
        .map_err(|_| "recovery_control")?
        .take(129)
        .read_to_string(&mut encoded)
        .map_err(|_| "recovery_control")?;
    if encoded.len() > 128 {
        return Err("recovery_control");
    }
    parse_fault(&encoded)?;
    Ok(true)
}

fn parse_fault(encoded: &str) -> Result<()> {
    let (kind, generation) = encoded
        .strip_suffix('\n')
        .and_then(|value| value.split_once(' '))
        .ok_or("recovery_control")?;
    let number = generation.parse::<u64>().map_err(|_| "recovery_control")?;
    if !matches!(kind, "graceful_restart" | "process_kill" | "backup_restore")
        || number == 0
        || number.to_string() != generation
    {
        return Err("recovery_control");
    }
    Ok(())
}

struct Budget {
    submissions: u32,
    recovering: bool,
}

impl Budget {
    fn failure(
        &mut self,
        error: &ApplicationClientError,
        planned: bool,
        elapsed: Duration,
    ) -> Result<()> {
        if !recoverable(error) || !(self.recovering || planned) {
            return Err("unplanned_operation_failure");
        }
        if elapsed >= DEADLINE || self.submissions >= ATTEMPTS {
            return Err("original_identity_recovery_bound");
        }
        self.recovering = true;
        self.submissions += 1;
        Ok(())
    }
}

/// Counts measured update/read submissions, each configured for one RPC.
/// The private command assembly uses native generated decoding and explicit
/// same-input resubmission without the SDK's automatic outcome lookup.
pub(super) async fn run<A: Attempt>(
    attempt: &mut A,
    root: Option<&Path>,
) -> Result<(A::Value, u32, bool)> {
    run_inner(
        attempt,
        || planned_fault(root),
        || tokio::time::sleep(Duration::from_millis(250)),
    )
    .await
}

async fn run_inner<A, P, D, F>(
    attempt: &mut A,
    mut planned: P,
    mut delay: D,
) -> Result<(A::Value, u32, bool)>
where
    A: Attempt,
    P: FnMut() -> Result<bool>,
    D: FnMut() -> F,
    F: std::future::Future<Output = ()> + Send,
{
    let started = Instant::now();
    let mut admitted = false;
    let mut budget = Budget {
        submissions: 1,
        recovering: false,
    };
    loop {
        let result =
            tokio::time::timeout(DEADLINE.saturating_sub(started.elapsed()), attempt.submit())
                .await
                .map_err(|_| "original_identity_recovery_bound")?;
        match result {
            Ok(value) => return Ok((value, budget.submissions - 1, admitted)),
            Err(error) => {
                admitted |= planned()?;
                budget.failure(&error, admitted, started.elapsed())?;
                delay().await;
            }
        }
    }
}

pub(super) async fn smoke() -> Result<()> {
    let root = std::env::var_os("RIFFDB_LIFECYCLE_LOAD_CONTROL").ok_or("recovery_control")?;
    let root = std::path::PathBuf::from(root);
    if !root.is_absolute() || root.is_symlink() || !root.is_dir() || !planned_fault(Some(&root))? {
        return Err("recovery_control");
    }
    let mut client = super::current::connect().await?;
    println!("{{\"schema\":\"riffdb.ticketdesk-lifecycle-recovery-ready/v1\"}}");
    let start = Instant::now();
    while !root.join("recovery.start").is_file() {
        if start.elapsed() >= Duration::from_secs(30) {
            return Err("recovery_start_bound");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let input = super::model::update(super::model::POPULATION)?;
    let mut retry = 0_u32;
    let (changed, retries, admitted) = run_inner(
        &mut Update {
            client: &mut client,
            input: input.clone(),
        },
        || planned_fault(Some(&root)),
        || {
            retry += 1;
            println!(
                "{{\"schema\":\"riffdb.ticketdesk-lifecycle-recovery-retry/v1\",\"retry\":{retry}}}"
            );
            tokio::time::sleep(Duration::from_millis(250))
        },
    )
    .await?;
    if !admitted || retries == 0 || changed.commit_sequence.is_none() {
        return Err("recovery_requires_interruption");
    }
    super::model::validate_update(super::model::POPULATION, &changed.outcome)?;
    let replay = client
        .update_ticket(input)
        .await
        .map_err(|_| "recovery_replay")?;
    if !replay.replayed
        || replay.outcome != changed.outcome
        || replay.commit_sequence != changed.commit_sequence
        || replay.plan_hash != changed.plan_hash
        || replay.contract_version != changed.contract_version
        || replay.outcome_uri != changed.outcome_uri
    {
        return Err("recovery_identity");
    }
    let super::application::GetTicketResult::Found(found) = client
        .get_ticket(super::application::GetTicketParams {
            organization_id: super::uuid(0x8001, 0),
            ticket_id: super::uuid(0x8004, 0),
        })
        .await
        .map_err(|_| "recovery_read")?
    else {
        return Err("recovery_read");
    };
    super::model::validate_final(super::model::POPULATION + 1, 0, &found.ticket)?;
    println!(
        "{{\"schema\":\"riffdb.ticketdesk-lifecycle-recovery-smoke/v1\",\"retries\":{retries},\"original_identity\":true,\"state_reconciled\":true}}"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uncertain() -> ApplicationClientError {
        ApplicationClientError::Client(ClientError::OutcomeUnknown(
            riffdb_client_rust::OutcomeUnknown,
        ))
    }

    // req: END-003, END-007, END-008
    #[test]
    fn recovery_requires_a_planned_fault_and_never_retries_invalid_or_unauthorized_input() {
        let mut budget = Budget {
            submissions: 1,
            recovering: false,
        };
        assert!(budget.failure(&uncertain(), false, Duration::ZERO).is_err());
        for error in [
            ApplicationClientError::InvalidInput,
            ApplicationClientError::InvalidResponse,
            ApplicationClientError::IdentifierUnavailable,
            ApplicationClientError::Client(ClientError::DetailsFree(
                DetailsFreeStatus::Unauthenticated,
            )),
            ApplicationClientError::Client(ClientError::DetailsFree(
                DetailsFreeStatus::DeadlineExceeded,
            )),
        ] {
            assert!(budget.failure(&error, true, Duration::ZERO).is_err());
        }
        budget.failure(&uncertain(), true, Duration::ZERO).unwrap();
        budget.failure(&uncertain(), false, Duration::ZERO).unwrap();
        assert_eq!(budget.submissions, 3);
        assert!(
            budget
                .failure(
                    &ApplicationClientError::InvalidResponse,
                    true,
                    Duration::ZERO
                )
                .is_err()
        );
    }

    // req: END-003, END-007, END-008
    #[test]
    fn recovery_does_not_extend_its_time_or_submission_budget() {
        let mut budget = Budget {
            submissions: 1,
            recovering: false,
        };
        assert!(budget.failure(&uncertain(), true, DEADLINE).is_err());
        for _ in 1..ATTEMPTS {
            budget.failure(&uncertain(), true, Duration::ZERO).unwrap();
        }
        assert!(budget.failure(&uncertain(), true, Duration::ZERO).is_err());
        assert_eq!(budget.submissions, ATTEMPTS);
    }

    // req: END-003, END-007, END-008
    #[test]
    fn only_canonical_restart_fault_controls_admit_operation_recovery() {
        for kind in ["graceful_restart", "process_kill", "backup_restore"] {
            parse_fault(&format!("{kind} 1\n")).unwrap();
        }
        for value in [
            "process_kill 0\n",
            "process_kill 01\n",
            "process_kill 1",
            "process_kill 1\n\n",
            "process_kill 1 extra\n",
            "sink_outage 1\n",
            "consumer_outage 1\n",
            "process_kill 18446744073709551616\n",
            " process_kill 1\n",
        ] {
            assert!(
                parse_fault(value).is_err(),
                "noncanonical fault control was admitted"
            );
        }
    }

    struct ImmutableUpdate {
        input: super::super::application::UpdateTicketInput,
        seen: Vec<super::super::application::UpdateTicketInput>,
    }

    impl Attempt for ImmutableUpdate {
        type Value = u64;

        async fn submit(&mut self) -> std::result::Result<Self::Value, ApplicationClientError> {
            self.seen.push(self.input.clone());
            if self.seen.len() == 1 {
                Err(uncertain())
            } else {
                Ok(73)
            }
        }
    }

    // req: END-003, END-007, END-008
    #[tokio::test]
    async fn recovery_keeps_the_original_key_input_and_revision_when_the_fault_marker_clears() {
        let input = super::super::model::update(0).unwrap();
        let mut attempt = ImmutableUpdate {
            input: input.clone(),
            seen: Vec::new(),
        };
        let planned = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let after_failure = std::sync::Arc::clone(&planned);
        let result = run_inner(
            &mut attempt,
            || Ok(planned.load(std::sync::atomic::Ordering::Acquire)),
            || {
                after_failure.store(false, std::sync::atomic::Ordering::Release);
                std::future::ready(())
            },
        )
        .await
        .unwrap();
        assert_eq!(result, (73, 1, true));
        assert_eq!(attempt.seen, vec![input.clone(), input]);
        assert!(!planned.load(std::sync::atomic::Ordering::Acquire));
    }
}
