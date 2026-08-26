/** Deterministic TicketDesk seed matching the Rust dataset. */

import {
  NS_COMMENT,
  NS_LABEL,
  NS_ORG,
  NS_PROJECT,
  NS_TICKET,
  NS_USER,
  STATUS_CLOSED,
  STATUS_IN_PROGRESS,
  STATUS_OPEN,
  type UuidBytes,
  uuidEquals,
  uuidFromOrdinal,
} from "./ids.js";

/** Contract maximum for comment.body (string<256> in ticketdesk.riff). */
export const MAX_COMMENT_BODY_BYTES = 256;

export const FULL_BOARD_DENSE_OPEN = 600;

export interface Scale {
  organizations: number;
  usersPerOrg: number;
  projectsPerOrg: number;
  membersPerProject: number;
  ticketsPerProject: number;
  commentsPerTicket: number;
  labelsPerOrg: number;
  labelsPerTicket: number;
  boardDenseOpen: number;
  payloadBytes: number;
}

export function smokeScale(): Scale {
  return {
    organizations: 2,
    usersPerOrg: 5,
    projectsPerOrg: 3,
    membersPerProject: 3,
    ticketsPerProject: 10,
    commentsPerTicket: 3,
    labelsPerOrg: 4,
    labelsPerTicket: 2,
    boardDenseOpen: 0,
    payloadBytes: 0,
  };
}

/**
 * Production-shaped profile: a help desk with real history and real text.
 *
 * `full` seeds roughly 14,600 rows and 2,000 tickets with `payloadBytes: 0`, so
 * every index is shallow, the whole set is resident, and comment bodies are
 * short generated labels. That measures protocol and CPU cost rather than a
 * database. This tier seeds roughly 120,000 tickets and 600,000 comments across
 * 200 tenants with realistic body length.
 *
 * It is NOT the frozen PERF-018 comparator dataset and must not replace it.
 * Keep these numbers identical to `Scale::production` in the Rust core, the Go
 * harness, and the Python harness; every harness carries its own copy.
 */
export function productionScale(): Scale {
  return {
    organizations: 200,
    usersPerOrg: 40,
    projectsPerOrg: 12,
    membersPerProject: 6,
    ticketsPerProject: 50,
    commentsPerTicket: 5,
    labelsPerOrg: 12,
    labelsPerTicket: 3,
    boardDenseOpen: FULL_BOARD_DENSE_OPEN,
    payloadBytes: MAX_COMMENT_BODY_BYTES,
  };
}

export function fullScale(): Scale {
  return {
    organizations: 10,
    usersPerOrg: 50,
    projectsPerOrg: 10,
    membersPerProject: 5,
    ticketsPerProject: 20,
    commentsPerTicket: 4,
    labelsPerOrg: 5,
    labelsPerTicket: 2,
    boardDenseOpen: FULL_BOARD_DENSE_OPEN,
    payloadBytes: 0,
  };
}

export interface TicketRow {
  organizationId: UuidBytes;
  ticketId: UuidBytes;
  projectId: UuidBytes;
  reporterId: UuidBytes;
  assigneeId: UuidBytes;
  status: string;
  title: string;
}

export interface CommentRow {
  organizationId: UuidBytes;
  commentId: UuidBytes;
  ticketId: UuidBytes;
  authorId: UuidBytes;
  body: string;
}

export interface CommentSeed {
  row: CommentRow;
  idempotencyKey: string;
}

export interface CloseTicketWithCommentSeed {
  organizationId: UuidBytes;
  ticketId: UuidBytes;
  authorId: UuidBytes;
  commentId: UuidBytes;
  body: string;
  idempotencyKey: string;
}

export interface OpenTicketWithLabelsSeed {
  organizationId: UuidBytes;
  ticketId: UuidBytes;
  projectId: UuidBytes;
  reporterId: UuidBytes;
  assigneeId: UuidBytes;
  title: string;
  labelA: UuidBytes;
  labelB: UuidBytes;
  idempotencyKey: string;
}

export interface ScenarioProbes {
  organizationId: UuidBytes;
  projectId: UuidBytes;
  ticketId: UuidBytes;
  userId: UuidBytes;
  assigneeId: UuidBytes;
  boardOrganizationId: UuidBytes;
  boardProjectId: UuidBytes;
  writeTicketId: UuidBytes;
  writeAuthorId: UuidBytes;
  writeProjectId: UuidBytes;
  writeAssigneeId: UuidBytes;
  writeLabelA: UuidBytes;
  writeLabelB: UuidBytes;
}

export interface SeedDataset {
  scale: Scale;
  organizations: Array<readonly [UuidBytes, string]>;
  users: Array<readonly [UuidBytes, UuidBytes, string, string]>;
  projects: Array<readonly [UuidBytes, UuidBytes, string]>;
  members: Array<readonly [UuidBytes, UuidBytes, UuidBytes, string]>;
  tickets: TicketRow[];
  comments: CommentRow[];
  labels: Array<readonly [UuidBytes, UuidBytes, string]>;
  ticketLabels: Array<readonly [UuidBytes, UuidBytes, UuidBytes]>;
}

function sizedText(prefix: string, targetBytes: number): string {
  if (targetBytes === 0 || prefix.length >= targetBytes) return prefix;
  return prefix + "x".repeat(targetBytes - prefix.length);
}

export function generateSeed(scale: Scale): SeedDataset {
  const organizations: SeedDataset["organizations"] = [];
  const users: SeedDataset["users"] = [];
  const projects: SeedDataset["projects"] = [];
  const members: SeedDataset["members"] = [];
  const tickets: TicketRow[] = [];
  const comments: CommentRow[] = [];
  const labels: SeedDataset["labels"] = [];
  const ticketLabels: SeedDataset["ticketLabels"] = [];

  for (let orgI = 0; orgI < scale.organizations; orgI += 1) {
    const organizationId = uuidFromOrdinal(NS_ORG, orgI);
    organizations.push([organizationId, `org-${orgI}`]);
    const orgUsers: SeedDataset["users"] = [];
    for (let userI = 0; userI < scale.usersPerOrg; userI += 1) {
      const userId = uuidFromOrdinal(NS_USER, orgI * 1_000_000 + userI);
      const user = [
        organizationId,
        userId,
        `user-${orgI}-${userI}@example.test`,
        `User ${orgI}/${userI}`,
      ] as const;
      orgUsers.push(user);
      users.push(user);
    }
    const orgLabels: SeedDataset["labels"] = [];
    for (let labelI = 0; labelI < scale.labelsPerOrg; labelI += 1) {
      const label = [
        organizationId,
        uuidFromOrdinal(NS_LABEL, orgI * 1_000 + labelI),
        `label-${orgI}-${labelI}`,
      ] as const;
      orgLabels.push(label);
      labels.push(label);
    }
    for (let projectI = 0; projectI < scale.projectsPerOrg; projectI += 1) {
      const projectOrdinal = orgI * 10_000 + projectI;
      const projectId = uuidFromOrdinal(NS_PROJECT, projectOrdinal);
      projects.push([organizationId, projectId, `project-${orgI}-${projectI}`]);
      const memberCount = Math.max(1, Math.min(scale.membersPerProject, scale.usersPerOrg));
      for (let memberI = 0; memberI < memberCount; memberI += 1) {
        const user = orgUsers[memberI % orgUsers.length]!;
        members.push([organizationId, projectId, user[1], memberI === 0 ? "owner" : "member"]);
      }
      const isBoard = orgI === 0 && projectI === 0 && scale.boardDenseOpen > 0;
      const ticketCount = isBoard
        ? Math.max(scale.boardDenseOpen, scale.ticketsPerProject)
        : scale.ticketsPerProject;
      for (let ticketI = 0; ticketI < ticketCount; ticketI += 1) {
        const ticketOrdinal = projectOrdinal * 1_000 + ticketI;
        const ticketId = uuidFromOrdinal(NS_TICKET, ticketOrdinal);
        const reporter = orgUsers[ticketI % orgUsers.length]!;
        const assignee = orgUsers[(ticketI + 1) % orgUsers.length]!;
        const status = isBoard
          ? STATUS_OPEN
          : ([STATUS_OPEN, STATUS_IN_PROGRESS, STATUS_CLOSED] as const)[ticketI % 3]!;
        tickets.push({
          organizationId,
          ticketId,
          projectId,
          reporterId: reporter[1],
          assigneeId: assignee[1],
          status,
          title: sizedText(`ticket-${orgI}-${projectI}-${ticketI}`, Math.min(scale.payloadBytes, 128)),
        });
        for (let commentI = 0; commentI < scale.commentsPerTicket; commentI += 1) {
          const author = orgUsers[commentI % orgUsers.length]!;
          comments.push({
            organizationId,
            commentId: uuidFromOrdinal(NS_COMMENT, ticketOrdinal * 100 + commentI),
            ticketId,
            authorId: author[1],
            body: sizedText(
              `comment body ${orgI}/${projectI}/${ticketI}/${commentI}`,
              scale.payloadBytes,
            ),
          });
        }
        const labelCount = Math.min(scale.labelsPerTicket, scale.labelsPerOrg);
        for (let labelI = 0; labelI < labelCount; labelI += 1) {
          const label = orgLabels[labelI % orgLabels.length]!;
          ticketLabels.push([organizationId, ticketId, label[1]]);
        }
      }
    }
  }

  return {
    scale,
    organizations,
    users,
    projects,
    members,
    tickets,
    comments,
    labels,
    ticketLabels,
  };
}

function boardCell(dataset: SeedDataset): readonly [UuidBytes, UuidBytes] {
  const row = dataset.projects[0];
  if (row === undefined) throw new Error("seed has no projects");
  return [row[0], row[1]];
}

export function boardDenseOpenCount(dataset: SeedDataset): number {
  const [organizationId, projectId] = boardCell(dataset);
  return dataset.tickets.filter(
    (ticket) =>
      uuidEquals(ticket.organizationId, organizationId) &&
      uuidEquals(ticket.projectId, projectId) &&
      ticket.status === STATUS_OPEN,
  ).length;
}

export function probesFor(dataset: SeedDataset): ScenarioProbes {
  const [boardOrg, boardProject] = boardCell(dataset);
  const ticket =
    dataset.tickets.find(
      (row) =>
        row.status === STATUS_OPEN &&
        !(uuidEquals(row.organizationId, boardOrg) && uuidEquals(row.projectId, boardProject)),
    ) ?? dataset.tickets.find((row) => row.status === STATUS_OPEN);
  if (ticket === undefined) throw new Error("seed has no open ticket");
  const user = dataset.users.find((row) => uuidEquals(row[0], ticket.organizationId));
  if (user === undefined) throw new Error("seed has no user for ticket org");
  const otherOpen = (row: TicketRow): boolean =>
    uuidEquals(row.organizationId, ticket.organizationId) &&
    !uuidEquals(row.ticketId, ticket.ticketId) &&
    row.status === STATUS_OPEN;
  const closeTicket =
    dataset.tickets.find(
      (row) =>
        otherOpen(row) &&
        !uuidEquals(row.projectId, ticket.projectId) &&
        !uuidEquals(row.projectId, boardProject) &&
        !uuidEquals(row.assigneeId, ticket.assigneeId),
    ) ??
    dataset.tickets.find(
      (row) =>
        otherOpen(row) &&
        !uuidEquals(row.projectId, ticket.projectId) &&
        !uuidEquals(row.projectId, boardProject),
    ) ??
    dataset.tickets.find((row) => otherOpen(row) && !uuidEquals(row.projectId, boardProject)) ??
    dataset.tickets.find(otherOpen) ??
    ticket;
  const orgLabels = dataset.labels.filter((label) => uuidEquals(label[0], ticket.organizationId));
  const labelA = orgLabels[0]?.[1];
  const labelB = orgLabels[1]?.[1] ?? labelA;
  if (labelA === undefined || labelB === undefined) throw new Error("seed has no labels");
  const writeProjectId =
    dataset.projects.find(
      (project) =>
        uuidEquals(project[0], ticket.organizationId) &&
        !uuidEquals(project[1], ticket.projectId) &&
        !uuidEquals(project[1], boardProject),
    )?.[1] ??
    dataset.projects.find(
      (project) =>
        uuidEquals(project[0], ticket.organizationId) && !uuidEquals(project[1], boardProject),
    )?.[1] ??
    ticket.projectId;
  const writeAssigneeId =
    dataset.users.find(
      (candidate) =>
        uuidEquals(candidate[0], ticket.organizationId) &&
        !uuidEquals(candidate[1], ticket.assigneeId),
    )?.[1] ?? ticket.assigneeId;
  return {
    organizationId: ticket.organizationId,
    projectId: ticket.projectId,
    ticketId: ticket.ticketId,
    userId: user[1],
    assigneeId: ticket.assigneeId,
    boardOrganizationId: boardOrg,
    boardProjectId: boardProject,
    writeTicketId: closeTicket.ticketId,
    writeAuthorId: user[1],
    writeProjectId,
    writeAssigneeId,
    writeLabelA: labelA,
    writeLabelB: labelB,
  };
}

export function tenantProbes(dataset: SeedDataset, count: number): ScenarioProbes[] {
  return dataset.organizations.slice(0, Math.max(count, 1)).map(([organizationId, name]) =>
    probesFor({
      scale: { ...dataset.scale, organizations: 1, boardDenseOpen: 0 },
      organizations: [[organizationId, name]],
      users: dataset.users.filter((row) => uuidEquals(row[0], organizationId)),
      projects: dataset.projects.filter((row) => uuidEquals(row[0], organizationId)),
      members: dataset.members.filter((row) => uuidEquals(row[0], organizationId)),
      tickets: dataset.tickets.filter((row) => uuidEquals(row.organizationId, organizationId)),
      comments: dataset.comments.filter((row) => uuidEquals(row.organizationId, organizationId)),
      labels: dataset.labels.filter((row) => uuidEquals(row[0], organizationId)),
      ticketLabels: dataset.ticketLabels.filter((row) => uuidEquals(row[0], organizationId)),
    }),
  );
}
