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

/// One comment used for seed and post-seed write scenarios.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommentSeed {
    /// Comment row.
    pub row: CommentRow,
    /// Idempotency key for the create command / insert identity.
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

                let member_count = scale
                    .members_per_project
                    .min(scale.users_per_org)
                    .max(1);
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
                            body: format!("comment body {org_i}/{project_i}/{ticket_i}/{comment_i}"),
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
        let write_comment = CommentSeed {
            row: CommentRow {
                organization_id,
                comment_id: uuid_from_ordinal(0x7f, 99_000_001),
                ticket_id: ticket.ticket_id,
                author_id: user.user_id,
                body: "baseline write-path comment".to_owned(),
            },
            idempotency_key: "app-baseline-write-comment-v1".to_owned(),
        };
        ScenarioProbes {
            organization_id,
            project_id,
            ticket_id: ticket.ticket_id,
            user_id: user.user_id,
            assignee_id,
            open_status: TicketStatus::Open,
            write_comment,
        }
    }
}

/// Fixed keys exercised by timed scenarios.
#[derive(Clone, Debug)]
pub struct ScenarioProbes {
    /// Organization under test.
    pub organization_id: [u8; 16],
    /// Project under test.
    pub project_id: [u8; 16],
    /// Ticket under test.
    pub ticket_id: [u8; 16],
    /// User under test.
    pub user_id: [u8; 16],
    /// Assignee under test.
    pub assignee_id: [u8; 16],
    /// Open status constant.
    pub open_status: TicketStatus,
    /// Comment created by the write scenario (idempotent key fixed).
    pub write_comment: CommentSeed,
}
