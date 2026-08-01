//! Deterministic TicketDesk seed generation.

use crate::{
    CommentRow, LabelRow, OrganizationRow, ProjectMemberRow, ProjectRow, Scale, TicketLabelRow,
    TicketRow, TicketStatus, UserRow, uuid_from_ordinal,
};

const NS_ORG: u8 = 0x10;
const NS_USER: u8 = 0x11;
const NS_PROJECT: u8 = 0x12;
const NS_TICKET: u8 = 0x13;
const NS_COMMENT: u8 = 0x14;
const NS_LABEL: u8 = 0x15;
const NS_WRITE_PROBE: u8 = 0x7f;

/// One comment used for seed and post-seed write scenarios.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommentSeed {
    /// Comment row.
    pub row: CommentRow,
    /// Idempotency key for the create command / insert identity.
    pub idempotency_key: String,
}

/// Close ticket + create closing comment in one application transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CloseTicketWithCommentSeed {
    /// Organization partition.
    pub organization_id: [u8; 16],
    /// Ticket to close.
    pub ticket_id: [u8; 16],
    /// Comment author.
    pub author_id: [u8; 16],
    /// Closing-comment identity.
    pub comment_id: [u8; 16],
    /// Closing note body.
    pub body: String,
    /// Per-sample idempotency key (each measured sample is a new durable write).
    pub idempotency_key: String,
}

/// Swap two project members' roles in one application transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SwapMemberRolesSeed {
    /// Organization partition.
    pub organization_id: [u8; 16],
    /// Project containing both members.
    pub project_id: [u8; 16],
    /// First member.
    pub user_a: [u8; 16],
    /// Second member.
    pub user_b: [u8; 16],
    /// Role written onto member A.
    pub role_a: String,
    /// Role written onto member B.
    pub role_b: String,
    /// Per-sample idempotency key (each measured sample is a new durable write).
    pub idempotency_key: String,
}

/// Atomic open-ticket operation (ticket + two label links).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenTicketWithLabelsSeed {
    /// Organization partition.
    pub organization_id: [u8; 16],
    /// New ticket id.
    pub ticket_id: [u8; 16],
    /// Project for the ticket.
    pub project_id: [u8; 16],
    /// Reporter.
    pub reporter_id: [u8; 16],
    /// Assignee.
    pub assignee_id: [u8; 16],
    /// Title.
    pub title: String,
    /// First existing label.
    pub label_a: [u8; 16],
    /// Second existing label.
    pub label_b: [u8; 16],
    /// Per-sample idempotency key (each measured sample is a new durable write).
    pub idempotency_key: String,
}

/// Complete deterministic dataset.
#[derive(Clone, Debug)]
pub struct SeedDataset {
    /// Scale used to generate the dataset.
    pub scale: Scale,
    /// Organizations.
    pub organizations: Vec<OrganizationRow>,
    /// Users.
    pub users: Vec<UserRow>,
    /// Projects.
    pub projects: Vec<ProjectRow>,
    /// Memberships.
    pub members: Vec<ProjectMemberRow>,
    /// Tickets.
    pub tickets: Vec<TicketRow>,
    /// Comments.
    pub comments: Vec<CommentRow>,
    /// Labels.
    pub labels: Vec<LabelRow>,
    /// Ticket-label links.
    pub ticket_labels: Vec<TicketLabelRow>,
}

impl SeedDataset {
    /// Builds the deterministic dataset for `scale`.
    #[must_use]
    pub fn generate(scale: Scale) -> Self {
        let mut organizations = Vec::new();
        let mut users = Vec::new();
        let mut projects = Vec::new();
        let mut members = Vec::new();
        let mut tickets = Vec::new();
        let mut comments = Vec::new();
        let mut labels = Vec::new();
        let mut ticket_labels = Vec::new();

        for org_i in 0..scale.organizations {
            let organization_id = uuid_from_ordinal(NS_ORG, u64::from(org_i));
            organizations.push(OrganizationRow {
                organization_id,
                name: format!("org-{org_i}"),
            });

            let mut org_users = Vec::with_capacity(scale.users_per_org as usize);
            for user_i in 0..scale.users_per_org {
                let ordinal = u64::from(org_i) * 1_000_000 + u64::from(user_i);
                let user_id = uuid_from_ordinal(NS_USER, ordinal);
                let user = UserRow {
                    organization_id,
                    user_id,
                    email: format!("user-{org_i}-{user_i}@example.test"),
                    display_name: format!("User {org_i}/{user_i}"),
                };
                org_users.push(user.clone());
                users.push(user);
            }

            let mut org_labels = Vec::with_capacity(scale.labels_per_org as usize);
            for label_i in 0..scale.labels_per_org {
                let ordinal = u64::from(org_i) * 1_000 + u64::from(label_i);
                let label = LabelRow {
                    organization_id,
                    label_id: uuid_from_ordinal(NS_LABEL, ordinal),
                    name: format!("label-{org_i}-{label_i}"),
                };
                org_labels.push(label.clone());
                labels.push(label);
            }

            for project_i in 0..scale.projects_per_org {
                let project_ordinal = u64::from(org_i) * 10_000 + u64::from(project_i);
                let project_id = uuid_from_ordinal(NS_PROJECT, project_ordinal);
                projects.push(ProjectRow {
                    organization_id,
                    project_id,
                    name: format!("project-{org_i}-{project_i}"),
                });

                let member_count = scale.members_per_project.min(scale.users_per_org).max(1);
                for member_i in 0..member_count {
                    let user = &org_users[member_i as usize % org_users.len()];
                    members.push(ProjectMemberRow {
                        organization_id,
                        project_id,
                        user_id: user.user_id,
                        role: if member_i == 0 { "owner" } else { "member" }.to_owned(),
                    });
                }

                for ticket_i in 0..scale.tickets_per_project {
                    let ticket_ordinal = project_ordinal * 1_000 + u64::from(ticket_i);
                    let ticket_id = uuid_from_ordinal(NS_TICKET, ticket_ordinal);
                    let reporter = &org_users[ticket_i as usize % org_users.len()];
                    let assignee = &org_users[(ticket_i as usize + 1) % org_users.len()];
                    let status = match ticket_i % 3 {
                        0 => TicketStatus::Open,
                        1 => TicketStatus::InProgress,
                        _ => TicketStatus::Closed,
                    };
                    tickets.push(TicketRow {
                        organization_id,
                        ticket_id,
                        project_id,
                        reporter_id: reporter.user_id,
                        assignee_id: assignee.user_id,
                        status,
                        title: format!("ticket-{org_i}-{project_i}-{ticket_i}"),
                    });

                    for comment_i in 0..scale.comments_per_ticket {
                        let comment_ordinal = ticket_ordinal * 100 + u64::from(comment_i);
                        let author = &org_users[comment_i as usize % org_users.len()];
                        comments.push(CommentRow {
                            organization_id,
                            comment_id: uuid_from_ordinal(NS_COMMENT, comment_ordinal),
                            ticket_id,
                            author_id: author.user_id,
                            body: format!(
                                "comment body {org_i}/{project_i}/{ticket_i}/{comment_i}"
                            ),
                        });
                    }

                    let label_count = scale.labels_per_ticket.min(scale.labels_per_org);
                    for label_i in 0..label_count {
                        let label = &org_labels[label_i as usize % org_labels.len()];
                        ticket_labels.push(TicketLabelRow {
                            organization_id,
                            ticket_id,
                            label_id: label.label_id,
                        });
                    }
                }
            }
        }

        Self {
            scale,
            organizations,
            users,
            projects,
            members,
            tickets,
            comments,
            labels,
            ticket_labels,
        }
    }

    /// Stable probe keys used by timed scenarios after seed.
    #[must_use]
    pub fn probes(&self) -> ScenarioProbes {
        let ticket = self
            .tickets
            .iter()
            .find(|ticket| ticket.status == TicketStatus::Open)
            .or_else(|| self.tickets.first())
            .expect("seed has tickets")
            .clone();
        let user = self
            .users
            .iter()
            .find(|user| user.organization_id == ticket.organization_id)
            .expect("seed has users")
            .clone();
        let project_id = ticket.project_id;
        let organization_id = ticket.organization_id;
        let assignee_id = ticket.assignee_id;
        // The close-with-comment target must be invisible to every read probe:
        // a different open ticket (so the read ticket stays open and its comment
        // list stays fixed), in a different project (so
        // `list_tickets_by_project_status` keeps its row count), and with a
        // different assignee (so `list_open_tickets_for_assignee` keeps its row
        // count when the ticket is closed). Weaker fallbacks keep `probes()`
        // working at scales that cannot satisfy the full predicate.
        let other_open_ticket = |row: &&TicketRow| {
            row.organization_id == organization_id
                && row.ticket_id != ticket.ticket_id
                && row.status == TicketStatus::Open
        };
        let close_ticket = self
            .tickets
            .iter()
            .find(|row| {
                other_open_ticket(row)
                    && row.project_id != project_id
                    && row.assignee_id != assignee_id
            })
            .or_else(|| self.tickets.iter().find(other_open_ticket))
            .cloned()
            .unwrap_or_else(|| ticket.clone());
        let project_members = self
            .members
            .iter()
            .filter(|member| {
                member.organization_id == organization_id && member.project_id == project_id
            })
            .cloned()
            .collect::<Vec<_>>();
        let member_a = project_members
            .first()
            .expect("seed has project members")
            .clone();
        let member_b = project_members
            .get(1)
            .cloned()
            .unwrap_or_else(|| member_a.clone());
        let labels = self
            .labels
            .iter()
            .filter(|label| label.organization_id == organization_id)
            .cloned()
            .collect::<Vec<_>>();
        let label_a = labels.first().expect("seed has labels").label_id;
        let label_b = labels.get(1).map(|label| label.label_id).unwrap_or(label_a);
        let write_project_id = self
            .projects
            .iter()
            .find(|project| {
                project.organization_id == organization_id && project.project_id != project_id
            })
            .map(|project| project.project_id)
            .unwrap_or(project_id);
        let write_assignee_id = self
            .users
            .iter()
            .find(|candidate| {
                candidate.organization_id == organization_id && candidate.user_id != assignee_id
            })
            .map(|candidate| candidate.user_id)
            .unwrap_or(assignee_id);
        ScenarioProbes {
            organization_id,
            project_id,
            ticket_id: ticket.ticket_id,
            user_id: user.user_id,
            assignee_id,
            open_status: TicketStatus::Open,
            write_ticket_id: close_ticket.ticket_id,
            write_author_id: user.user_id,
            write_project_id,
            write_assignee_id,
            swap_user_a: member_a.user_id,
            swap_user_b: member_b.user_id,
            write_label_a: label_a,
            write_label_b: label_b,
        }
    }
}

/// Fixed keys exercised by timed scenarios.
///
/// Read probes (`ticket_id`, `project_id`, `assignee_id`, ...) are never
/// mutated by write scenarios, so every measured sample of a read scenario
/// sees identical data. Write scenarios derive a distinct idempotency key
/// and distinct created-entity IDs per sample so both backends execute one
/// genuinely new durable write per sample (no idempotent replays and no
/// conflict-suppressed inserts).
#[derive(Clone, Debug)]
pub struct ScenarioProbes {
    /// Organization under test.
    pub organization_id: [u8; 16],
    /// Project under test (read probes only).
    pub project_id: [u8; 16],
    /// Ticket under test (read probes only).
    pub ticket_id: [u8; 16],
    /// User under test.
    pub user_id: [u8; 16],
    /// Assignee under test (read probes only).
    pub assignee_id: [u8; 16],
    /// Open status constant.
    pub open_status: TicketStatus,
    /// Ticket receiving write-scenario comments and closes.
    pub write_ticket_id: [u8; 16],
    /// Author of write-scenario comments.
    pub write_author_id: [u8; 16],
    /// Project receiving write-scenario opened tickets.
    pub write_project_id: [u8; 16],
    /// Assignee of write-scenario opened tickets.
    pub write_assignee_id: [u8; 16],
    /// First member of the role-swap pair.
    pub swap_user_a: [u8; 16],
    /// Second member of the role-swap pair.
    pub swap_user_b: [u8; 16],
    /// First label attached by the open-ticket scenario.
    pub write_label_a: [u8; 16],
    /// Second label attached by the open-ticket scenario.
    pub write_label_b: [u8; 16],
}

impl ScenarioProbes {
    /// Distinct comment insert for measured sample `sample`.
    #[must_use]
    pub fn write_comment(&self, sample: usize) -> CommentSeed {
        CommentSeed {
            row: CommentRow {
                organization_id: self.organization_id,
                comment_id: uuid_from_ordinal(NS_WRITE_PROBE, 99_100_000 + sample as u64),
                ticket_id: self.write_ticket_id,
                author_id: self.write_author_id,
                body: format!("baseline write-path comment {sample}"),
            },
            idempotency_key: format!("app-baseline-write-comment-v2-{sample}"),
        }
    }

    /// Distinct close-with-comment write for measured sample `sample`.
    ///
    /// `CloseTicketWithComment` has no open-status requirement, so re-closing
    /// the same write ticket stays a real two-entity mutation on every
    /// sample; only the created comment identity must be fresh.
    #[must_use]
    pub fn close_ticket_with_comment(&self, sample: usize) -> CloseTicketWithCommentSeed {
        CloseTicketWithCommentSeed {
            organization_id: self.organization_id,
            ticket_id: self.write_ticket_id,
            author_id: self.write_author_id,
            comment_id: uuid_from_ordinal(NS_WRITE_PROBE, 99_200_000 + sample as u64),
            body: format!("baseline close-with-comment note {sample}"),
            idempotency_key: format!("app-baseline-close-ticket-with-comment-v2-{sample}"),
        }
    }

    /// Distinct role swap for measured sample `sample`.
    ///
    /// Alternating direction by parity makes every sample a genuine value
    /// change on the same two membership rows.
    #[must_use]
    pub fn swap_member_roles(&self, sample: usize) -> SwapMemberRolesSeed {
        let (role_a, role_b) = if sample.is_multiple_of(2) {
            ("lead", "contributor")
        } else {
            ("contributor", "lead")
        };
        SwapMemberRolesSeed {
            organization_id: self.organization_id,
            project_id: self.project_id,
            user_a: self.swap_user_a,
            user_b: self.swap_user_b,
            role_a: role_a.to_owned(),
            role_b: role_b.to_owned(),
            idempotency_key: format!("app-baseline-swap-member-roles-v2-{sample}"),
        }
    }

    /// Distinct open-ticket-with-labels write for measured sample `sample`.
    #[must_use]
    pub fn open_ticket_with_labels(&self, sample: usize) -> OpenTicketWithLabelsSeed {
        OpenTicketWithLabelsSeed {
            organization_id: self.organization_id,
            ticket_id: uuid_from_ordinal(NS_WRITE_PROBE, 99_300_000 + sample as u64),
            project_id: self.write_project_id,
            reporter_id: self.write_author_id,
            assignee_id: self.write_assignee_id,
            title: format!("baseline multi-command open ticket {sample}"),
            label_a: self.write_label_a,
            label_b: self.write_label_b,
            idempotency_key: format!("app-baseline-open-ticket-with-labels-v2-{sample}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::SeedDataset;
    use crate::Scale;

    #[test]
    fn write_probes_are_disjoint_from_read_probes() {
        for scale in [Scale::smoke(), Scale::full()] {
            let dataset = SeedDataset::generate(scale);
            let probes = dataset.probes();
            assert_ne!(probes.write_ticket_id, probes.ticket_id);
            assert_ne!(probes.write_project_id, probes.project_id);
            assert_ne!(probes.write_assignee_id, probes.assignee_id);
            assert_ne!(probes.write_label_a, probes.write_label_b);
            // SwapMemberRoles must mutate two distinct memberships in one
            // command; the `probes()` fallback would otherwise alias them.
            assert_ne!(probes.swap_user_a, probes.swap_user_b);

            // Closing the write ticket must not change any read scenario's row
            // count, so it lives outside the read-probe project and belongs to
            // a different assignee.
            let write_ticket = dataset
                .tickets
                .iter()
                .find(|ticket| ticket.ticket_id == probes.write_ticket_id)
                .expect("write ticket exists in the dataset");
            assert_ne!(write_ticket.project_id, probes.project_id);
            assert_ne!(write_ticket.assignee_id, probes.assignee_id);
        }
    }

    #[test]
    fn write_probes_are_unique_per_sample_and_deterministic() {
        let dataset = SeedDataset::generate(Scale::smoke());
        let probes = dataset.probes();
        let mut keys = BTreeSet::new();
        let mut created_ids = BTreeSet::new();
        for sample in 0..100 {
            let comment = probes.write_comment(sample);
            let close = probes.close_ticket_with_comment(sample);
            let swap = probes.swap_member_roles(sample);
            let open = probes.open_ticket_with_labels(sample);

            assert!(keys.insert(comment.idempotency_key.clone()));
            assert!(keys.insert(close.idempotency_key.clone()));
            assert!(keys.insert(swap.idempotency_key.clone()));
            assert!(keys.insert(open.idempotency_key.clone()));
            assert!(comment.idempotency_key.len() < 128);
            assert!(close.idempotency_key.len() < 128);
            assert!(swap.idempotency_key.len() < 128);
            assert!(open.idempotency_key.len() < 128);

            assert!(created_ids.insert(comment.row.comment_id));
            assert!(created_ids.insert(close.comment_id));
            assert!(created_ids.insert(open.ticket_id));

            assert_eq!(comment.row.ticket_id, probes.write_ticket_id);
            assert_eq!(close.ticket_id, probes.write_ticket_id);
            assert_eq!(open.project_id, probes.write_project_id);
            assert_eq!(open.assignee_id, probes.write_assignee_id);
            assert_eq!(swap.project_id, probes.project_id);

            assert_eq!(comment, probes.write_comment(sample));
            assert_eq!(close, probes.close_ticket_with_comment(sample));
            assert_eq!(swap, probes.swap_member_roles(sample));
            assert_eq!(open, probes.open_ticket_with_labels(sample));
        }
        // Adjacent samples swap in opposite directions (a real value change
        // per sample on the same two membership rows).
        let even = probes.swap_member_roles(0);
        let odd = probes.swap_member_roles(1);
        assert_eq!(even.role_a, odd.role_b);
        assert_eq!(even.role_b, odd.role_a);
    }
}
