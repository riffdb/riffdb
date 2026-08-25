/** RiffDB load session: generated TicketDesk client only. No TypeScript safety code. */

import { readFile } from "node:fs/promises";

import {
  DriverApplicationError,
  DriverApplicationTransport,
  DriverGeneratedApplicationTransport,
  type DriverApplicationIdentity,
} from "@riffdb/application";

import { encodeShort, formatUuid, sqlStatusToRiff, type UuidBytes } from "./ids.js";
import type {
  CloseTicketWithCommentSeed,
  CommentSeed,
  OpenTicketWithLabelsSeed,
  SeedDataset,
} from "./seed.js";
import * as generatedClient from "./ticketdesk-client.js";

export const RIFFDB_BACKEND_ID = "riffdb_public_grpc";

type TicketDeskModule = typeof generatedClient;

async function loadGenerated(): Promise<TicketDeskModule> {
  return generatedClient;
}

export function classifyRiffdb(error: unknown): string {
  if (error instanceof DriverApplicationError) {
    const code = error.details.code;
    if (code === "RDB-COMMAND-0102") return "conflict";
    if (code === "RDB-STORAGE-0101" || code === "RDB-RESOURCE-0101" || code === "RDB-CAPACITY-0101") {
      return "unavailable";
    }
    return "error";
  }
  if (error instanceof RiffDbLoadError) {
    if (error.kind === "conflict") return "conflict";
    return "error";
  }
  return "error";
}

export class RiffDbLoadError extends Error {
  override readonly name = "RiffDbLoadError";
  constructor(
    message: string,
    readonly kind: "conflict" | "error" = "error",
  ) {
    super(message);
  }
}

export interface DriverIdentityFile {
  readonly socketPath: string;
  readonly identity: DriverApplicationIdentity;
}

export async function readDriverIdentity(path: string): Promise<DriverIdentityFile> {
  const parsed = JSON.parse(await readFile(path, "utf8")) as {
    socketPath: string;
    identity: Omit<DriverApplicationIdentity, "contractVersion"> & { contractVersion: string | number };
  };
  return {
    socketPath: parsed.socketPath,
    identity: {
      ...parsed.identity,
      contractVersion: BigInt(parsed.identity.contractVersion),
    },
  };
}

function requireOutcome(actual: string, expected: string): void {
  if (actual === expected) return;
  if (actual === "CommentExists" || actual === "TicketExists" || actual === "LinkExists") {
    throw new RiffDbLoadError(`unexpected outcome ${actual}, wanted ${expected}`, "conflict");
  }
  throw new RiffDbLoadError(`unexpected outcome ${actual}, wanted ${expected}`);
}

export class RiffDbSession {
  private constructor(
    private readonly driver: DriverApplicationTransport,
    private readonly client: InstanceType<TicketDeskModule["TicketDeskClient"]>,
  ) {}

  static async connect(config: DriverIdentityFile): Promise<RiffDbSession> {
    const generated = await loadGenerated();
    const driver = await DriverApplicationTransport.connect({
      socketPath: config.socketPath,
      identity: config.identity,
    });
    const transport = new DriverGeneratedApplicationTransport(driver);
    const client = new generated.TicketDeskClient(transport, 1);
    return new RiffDbSession(driver, client);
  }

  async close(): Promise<void> {
    await this.driver.shutdown();
  }

  async prewarm(organizationId: UuidBytes, ticketId: UuidBytes): Promise<void> {
    await this.pointGetTicket(organizationId, ticketId);
  }

  async pointGetTicket(organizationId: UuidBytes, ticketId: UuidBytes): Promise<boolean> {
    const result = await this.client.getTicket({
      organization_id: formatUuid(organizationId),
      ticket_id: formatUuid(ticketId),
    });
    return result.value.outcome === "Found";
  }

  async pointGetUser(organizationId: UuidBytes, userId: UuidBytes): Promise<boolean> {
    const result = await this.client.getUser({
      organization_id: formatUuid(organizationId),
      user_id: formatUuid(userId),
    });
    return result.value.outcome === "Found";
  }

  async listTicketsByProjectStatus(
    organizationId: UuidBytes,
    projectId: UuidBytes,
    status: string,
    limit: number,
  ): Promise<void> {
    await this.client.listTickets({
      organization_id: formatUuid(organizationId),
      project_id: formatUuid(projectId),
      statuses: [sqlStatusToRiff(status)],
      limit,
    });
  }

  async listOpenTicketsForAssignee(
    organizationId: UuidBytes,
    assigneeId: UuidBytes,
    limit: number,
  ): Promise<void> {
    await this.client.listTicketsByAssignee({
      organization_id: formatUuid(organizationId),
      assignee_id: formatUuid(assigneeId),
      statuses: ["Open"],
      limit,
    });
  }

  async listCommentsForTicket(
    organizationId: UuidBytes,
    ticketId: UuidBytes,
    limit: number,
  ): Promise<void> {
    await this.client.listComments({
      organization_id: formatUuid(organizationId),
      ticket_id: formatUuid(ticketId),
      limit,
    });
  }

  async listProjectMembers(organizationId: UuidBytes, projectId: UuidBytes, _limit: number): Promise<void> {
    await this.client.projectMembers({
      organization_id: formatUuid(organizationId),
      project_id: formatUuid(projectId),
    });
  }

  async ticketDetailPage(organizationId: UuidBytes, ticketId: UuidBytes): Promise<boolean> {
    const result = await this.client.ticketPage({
      organization_id: formatUuid(organizationId),
      ticket_id: formatUuid(ticketId),
    });
    return result.value.outcome === "Found";
  }

  async createComment(comment: CommentSeed): Promise<void> {
    const result = await this.client.createComment({
      body: comment.row.body,
      author_id: formatUuid(comment.row.authorId),
      ticket_id: formatUuid(comment.row.ticketId),
      comment_id: formatUuid(comment.row.commentId),
      idempotency_key: comment.idempotencyKey,
      organization_id: formatUuid(comment.row.organizationId),
    });
    requireOutcome(result.outcome.outcome, "Created");
  }

  async closeTicketWithComment(input: CloseTicketWithCommentSeed): Promise<void> {
    const result = await this.client.closeTicketWithComment({
      body: input.body,
      author_id: formatUuid(input.authorId),
      ticket_id: formatUuid(input.ticketId),
      comment_id: formatUuid(input.commentId),
      idempotency_key: input.idempotencyKey,
      organization_id: formatUuid(input.organizationId),
    });
    requireOutcome(result.outcome.outcome, "Closed");
  }

  async openTicketWithLabels(input: OpenTicketWithLabelsSeed): Promise<void> {
    const result = await this.client.openTicketWithLabels({
      title: input.title,
      label_a: formatUuid(input.labelA),
      label_b: formatUuid(input.labelB),
      ticket_id: formatUuid(input.ticketId),
      project_id: formatUuid(input.projectId),
      assignee_id: formatUuid(input.assigneeId),
      reporter_id: formatUuid(input.reporterId),
      idempotency_key: input.idempotencyKey,
      organization_id: formatUuid(input.organizationId),
    });
    requireOutcome(result.outcome.outcome, "Created");
  }

  async seed(dataset: SeedDataset): Promise<void> {
    for (const [orgId, name] of dataset.organizations) {
      requireOutcome(
        (
          await this.client.createOrganization({
            name,
            organization_id: formatUuid(orgId),
            idempotency_key: `seed-org-${encodeShort(orgId)}`,
          })
        ).outcome.outcome,
        "Created",
      );
    }
    for (const [orgId, userId, email, display] of dataset.users) {
      requireOutcome(
        (
          await this.client.createUser({
            email,
            user_id: formatUuid(userId),
            display_name: display,
            organization_id: formatUuid(orgId),
            idempotency_key: `seed-user-${encodeShort(userId)}`,
          })
        ).outcome.outcome,
        "Created",
      );
    }
    for (const [orgId, projectId, name] of dataset.projects) {
      requireOutcome(
        (
          await this.client.createProject({
            name,
            project_id: formatUuid(projectId),
            organization_id: formatUuid(orgId),
            idempotency_key: `seed-project-${encodeShort(projectId)}`,
          })
        ).outcome.outcome,
        "Created",
      );
    }
    for (const [orgId, projectId, userId, role] of dataset.members) {
      requireOutcome(
        (
          await this.client.addProjectMember({
            role,
            user_id: formatUuid(userId),
            project_id: formatUuid(projectId),
            organization_id: formatUuid(orgId),
            idempotency_key: `seed-member-${encodeShort(projectId)}-${encodeShort(userId)}`,
          })
        ).outcome.outcome,
        "Created",
      );
    }
    for (const [orgId, labelId, name] of dataset.labels) {
      requireOutcome(
        (
          await this.client.createLabel({
            name,
            label_id: formatUuid(labelId),
            organization_id: formatUuid(orgId),
            idempotency_key: `seed-label-${encodeShort(labelId)}`,
          })
        ).outcome.outcome,
        "Created",
      );
    }
    for (const ticket of dataset.tickets) {
      requireOutcome(
        (
          await this.client.createTicket({
            title: ticket.title,
            status: sqlStatusToRiff(ticket.status),
            ticket_id: formatUuid(ticket.ticketId),
            project_id: formatUuid(ticket.projectId),
            assignee_id: formatUuid(ticket.assigneeId),
            reporter_id: formatUuid(ticket.reporterId),
            organization_id: formatUuid(ticket.organizationId),
            idempotency_key: `seed-ticket-${encodeShort(ticket.ticketId)}`,
          })
        ).outcome.outcome,
        "Created",
      );
    }
    for (const comment of dataset.comments) {
      requireOutcome(
        (
          await this.client.createComment({
            body: comment.body,
            author_id: formatUuid(comment.authorId),
            ticket_id: formatUuid(comment.ticketId),
            comment_id: formatUuid(comment.commentId),
            organization_id: formatUuid(comment.organizationId),
            idempotency_key: `seed-comment-${encodeShort(comment.commentId)}`,
          })
        ).outcome.outcome,
        "Created",
      );
    }
    for (const [orgId, ticketId, labelId] of dataset.ticketLabels) {
      requireOutcome(
        (
          await this.client.attachLabel({
            label_id: formatUuid(labelId),
            ticket_id: formatUuid(ticketId),
            organization_id: formatUuid(orgId),
            idempotency_key: `seed-link-${encodeShort(ticketId)}-${encodeShort(labelId)}`,
          })
        ).outcome.outcome,
        "Created",
      );
    }
  }
}

export class RiffDbDriver {
  readonly backendId = RIFFDB_BACKEND_ID;

  constructor(private readonly identity: DriverIdentityFile) {}

  async seed(dataset: SeedDataset): Promise<void> {
    const session = await RiffDbSession.connect(this.identity);
    try {
      await session.seed(dataset);
    } finally {
      await session.close();
    }
  }

  async openSession(): Promise<RiffDbSession> {
    return RiffDbSession.connect(this.identity);
  }
}
