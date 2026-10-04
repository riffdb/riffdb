//! Independent oracle for deterministic stable-population campaign operations.

use super::application::{GetTicketFoundTicket, UpdateTicketInput, UpdateTicketOutcome};

pub(super) const POPULATION: u64 = 65_536;
pub(super) const MAX_COMMANDS: u64 = 8_640_000;

pub(super) fn update(ordinal: u64) -> Result<UpdateTicketInput, &'static str> {
    if ordinal >= MAX_COMMANDS {
        return Err("operation_bound");
    }
    let ticket = ordinal % POPULATION;
    Ok(UpdateTicketInput {
        idempotency_key: format!("ticketdesk-lifecycle-update-{ordinal}"),
        organization_id: super::uuid(0x8001, ticket / super::TICKETS_PER_ORGANIZATION),
        ticket_id: super::uuid(0x8004, ticket),
        expected_revision: ordinal / POPULATION,
        status: status(ordinal / POPULATION + 1).to_owned(),
    })
}

pub(super) fn status(revision: u64) -> &'static str {
    if revision.is_multiple_of(2) {
        "Open"
    } else {
        "InProgress"
    }
}

pub(super) fn validate_update(
    ordinal: u64,
    outcome: &UpdateTicketOutcome,
) -> Result<(), &'static str> {
    if ordinal >= MAX_COMMANDS {
        return Err("operation_bound");
    }
    match outcome {
        UpdateTicketOutcome::Updated {
            revision,
            status: actual,
        } if *revision == ordinal / POPULATION + 1 && actual == status(*revision) => Ok(()),
        _ => Err("update_reconciliation"),
    }
}

pub(super) fn validate_final(
    updates: u64,
    ticket: u64,
    actual: &GetTicketFoundTicket,
) -> Result<(), &'static str> {
    if updates > MAX_COMMANDS || ticket >= POPULATION {
        return Err("operation_bound");
    }
    let revision = updates / POPULATION + u64::from(ticket < updates % POPULATION);
    validate_revision(ticket, revision, actual)
}

pub(super) fn validate_revision(
    ticket: u64,
    revision: u64,
    actual: &GetTicketFoundTicket,
) -> Result<(), &'static str> {
    if ticket >= POPULATION || revision > MAX_COMMANDS.div_ceil(POPULATION) {
        return Err("operation_bound");
    }
    let organization = ticket / super::TICKETS_PER_ORGANIZATION;
    if actual.revision != revision
        || actual.status != status(revision)
        || actual.ticket_id != super::uuid(0x8004, ticket)
        || actual.project_id != super::uuid(0x8003, organization)
        || actual.reporter_id != super::uuid(0x8002, organization)
        || actual.assignee_id != super::uuid(0x8002, organization)
        || actual.title != format!("Lifecycle ticket {ticket}")
    {
        return Err("state_reconciliation");
    }
    Ok(())
}

pub(super) fn validate_live(
    offered: u64,
    ticket: u64,
    actual: &GetTicketFoundTicket,
) -> Result<(), &'static str> {
    if offered > MAX_COMMANDS || ticket >= POPULATION {
        return Err("operation_bound");
    }
    let maximum = offered / POPULATION + u64::from(ticket < offered % POPULATION);
    if actual.revision > maximum {
        return Err("state_reconciliation");
    }
    let implied = if actual.revision == 0 {
        0
    } else {
        (actual.revision - 1)
            .checked_mul(POPULATION)
            .and_then(|base| base.checked_add(ticket + 1))
            .ok_or("operation_bound")?
    };
    validate_final(implied, ticket, actual)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn actual(ticket: u64, revision: u64) -> GetTicketFoundTicket {
        GetTicketFoundTicket {
            ticket_id: super::super::uuid(0x8004, ticket),
            project_id: super::super::uuid(0x8003, ticket / 1024),
            reporter_id: super::super::uuid(0x8002, ticket / 1024),
            assignee_id: super::super::uuid(0x8002, ticket / 1024),
            title: format!("Lifecycle ticket {ticket}"),
            status: status(revision).to_owned(),
            revision,
        }
    }

    // req: END-001, END-002, END-007, END-008
    #[test]
    fn stale_missing_or_substituted_update_outcomes_cannot_advance_the_oracle() {
        assert_eq!(
            validate_update(0, &UpdateTicketOutcome::Stale),
            Err("update_reconciliation")
        );
        assert_eq!(
            validate_update(
                0,
                &UpdateTicketOutcome::Updated {
                    revision: 2,
                    status: "Open".to_owned(),
                }
            ),
            Err("update_reconciliation")
        );
    }

    // req: END-001, END-002, END-003, END-007
    #[test]
    fn live_reads_allow_the_final_partial_round_but_refuse_unoffered_revisions() {
        let last_changed = MAX_COMMANDS % POPULATION - 1;
        let revision = MAX_COMMANDS / POPULATION + 1;
        assert_eq!(
            validate_live(MAX_COMMANDS, last_changed, &actual(last_changed, revision)),
            Ok(())
        );
        assert_eq!(
            validate_live(
                MAX_COMMANDS,
                last_changed + 1,
                &actual(last_changed + 1, revision)
            ),
            Err("state_reconciliation")
        );
        assert_eq!(
            validate_live(0, 0, &actual(0, 1)),
            Err("state_reconciliation")
        );
    }

    // req: END-001, END-002, END-003, END-007
    #[test]
    fn final_population_reconciliation_checks_every_round_and_partition_identity() {
        let total = POPULATION + 1;
        assert_eq!(validate_final(total, 0, &actual(0, 2)), Ok(()));
        assert_eq!(validate_final(total, 1, &actual(1, 1)), Ok(()));
        assert_eq!(
            validate_final(total, 0, &actual(0, 1)),
            Err("state_reconciliation")
        );
        assert_eq!(
            validate_final(total, 1024, &actual(0, 1)),
            Err("state_reconciliation")
        );
    }
}
