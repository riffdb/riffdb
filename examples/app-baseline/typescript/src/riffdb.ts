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

export /** Bounded batch fan-out for seeding, matching the Rust harness. */
const SEED_CONCURRENCY = 128;
/** Generated batch row ceiling. */
const SEED_MAX_BATCH_ROWS = 4096;
/**
 * The driver frames a whole batch as one message bounded at 1 MiB, so the
 * generated row ceiling is not the binding constraint: 4,096 comment inputs
 * carrying 256-byte bodies encode to roughly 1.8 MB and the driver refuses the
 * frame. Target half the budget so per-row variation and framing overhead have
 * headroom, and size each phase from its own inputs rather than guessing one
 * number for eight differently shaped phases.
 */
const SEED_FRAME_TARGET_BYTES = 512 * 1024;

const RIFFDB_BACKEND_ID = "riffdb_public_grpc";

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
    // Seed through the generated bounded batch envelope, as the Rust harness
    // does. A sequential `for`/`await` loop ran at single-client rate, which is
    // invisible at `full` scale (19,220 commands) and unusable at `production`
    // scale (~1.1M): a measured run reached 1.4 GB after twenty-one minutes
    // without finishing. Every item remains an ordinary independent generated
    // command with its own durable lifecycle; only the transport is batched.
    const options = { concurrency: SEED_CONCURRENCY };
    const total =
      dataset.organizations.length + dataset.users.length + dataset.projects.length +
      dataset.members.length + dataset.labels.length + dataset.tickets.length +
      dataset.comments.length + dataset.ticketLabels.length;
    let completed = 0;
    const started = Date.now();
    process.stderr.write(
      `riffdb-seed-start\ttotal=${total}\tconcurrency=${SEED_CONCURRENCY}\n`,
    );

    // Phases respect foreign-key order.
    // JSON.stringify is a proxy for the driver's own encoding, not identical to
    // it; the half-budget target above is what absorbs the difference.
    const chunkRowsFor = (inputs: ReadonlyArray<unknown>): number => {
      if (inputs.length === 0) return SEED_MAX_BATCH_ROWS;
      const sample = Buffer.byteLength(JSON.stringify(inputs[0]), "utf8");
      if (sample <= 0) return SEED_MAX_BATCH_ROWS;
      const rows = Math.floor(SEED_FRAME_TARGET_BYTES / sample);
      return Math.max(1, Math.min(SEED_MAX_BATCH_ROWS, rows));
    };

    const runPhase = async <I, O extends { readonly outcome: string }>(
      phase: string,
      inputs: ReadonlyArray<I>,
      call: (
        chunk: ReadonlyArray<I>,
        options: generatedClient.CommandBatchOptions,
      ) => Promise<generatedClient.CommandBatchResult<O>>,
      expected: string,
    ): Promise<void> => {
      const batchRows = chunkRowsFor(inputs);
      for (let offset = 0; offset < inputs.length; offset += batchRows) {
        const chunk = inputs.slice(offset, offset + batchRows);
        const result = await call(chunk, options);
        if (result.items.length !== chunk.length) {
          throw new Error(`seed phase ${phase} returned ${result.items.length} of ${chunk.length}`);
        }
        for (const item of result.items) {
          // A batch reports a per-item failure in `error` rather than rejecting,
          // so a phase that ignored it would seed silently short.
          if (item.error !== undefined) throw item.error;
          if (item.result === undefined) {
            throw new RiffDbLoadError(`seed phase ${phase} item ${item.index} carried no result`);
          }
          requireOutcome(item.result.outcome.outcome, expected);
        }
        completed += chunk.length;
      }
      process.stderr.write(
        `riffdb-seed-progress\tphase=${phase}\tcompleted=${completed}/${total}` +
          `\tbatch_rows=${batchRows}\toverall_ms=${Date.now() - started}\n`,
      );
    };

    await runPhase("organization", dataset.organizations.map(([orgId, name]) => ({
      name, organization_id: formatUuid(orgId),
      idempotency_key: `seed-org-${encodeShort(orgId)}`,
    })), (chunk, o) => this.client.createOrganizationBatch(chunk, o), "Created");

    await runPhase("user", dataset.users.map(([orgId, userId, email, display]) => ({
      email, user_id: formatUuid(userId), display_name: display,
      organization_id: formatUuid(orgId),
      idempotency_key: `seed-user-${encodeShort(userId)}`,
    })), (chunk, o) => this.client.createUserBatch(chunk, o), "Created");

    await runPhase("project", dataset.projects.map(([orgId, projectId, name]) => ({
      name, project_id: formatUuid(projectId), organization_id: formatUuid(orgId),
      idempotency_key: `seed-project-${encodeShort(projectId)}`,
    })), (chunk, o) => this.client.createProjectBatch(chunk, o), "Created");

    await runPhase("member", dataset.members.map(([orgId, projectId, userId, role]) => ({
      role, user_id: formatUuid(userId), project_id: formatUuid(projectId),
      organization_id: formatUuid(orgId),
      idempotency_key: `seed-member-${encodeShort(projectId)}-${encodeShort(userId)}`,
    })), (chunk, o) => this.client.addProjectMemberBatch(chunk, o), "Created");

    await runPhase("label", dataset.labels.map(([orgId, labelId, name]) => ({
      name, label_id: formatUuid(labelId), organization_id: formatUuid(orgId),
      idempotency_key: `seed-label-${encodeShort(labelId)}`,
    })), (chunk, o) => this.client.createLabelBatch(chunk, o), "Created");

    await runPhase("ticket", dataset.tickets.map((ticket) => ({
      title: ticket.title, status: sqlStatusToRiff(ticket.status),
      ticket_id: formatUuid(ticket.ticketId),
      project_id: formatUuid(ticket.projectId),
      reporter_id: formatUuid(ticket.reporterId),
      assignee_id: formatUuid(ticket.assigneeId),
      organization_id: formatUuid(ticket.organizationId),
      idempotency_key: `seed-ticket-${encodeShort(ticket.ticketId)}`,
    })), (chunk, o) => this.client.createTicketBatch(chunk, o), "Created");

    await runPhase("comment", dataset.comments.map((comment) => ({
      body: comment.body, comment_id: formatUuid(comment.commentId),
      ticket_id: formatUuid(comment.ticketId),
      author_id: formatUuid(comment.authorId),
      organization_id: formatUuid(comment.organizationId),
      idempotency_key: `seed-comment-${encodeShort(comment.commentId)}`,
    })), (chunk, o) => this.client.createCommentBatch(chunk, o), "Created");

    await runPhase("ticket_label", dataset.ticketLabels.map(([orgId, ticketId, labelId]) => ({
      label_id: formatUuid(labelId), ticket_id: formatUuid(ticketId),
      organization_id: formatUuid(orgId),
      idempotency_key: `seed-link-${encodeShort(ticketId)}-${encodeShort(labelId)}`,
    })), (chunk, o) => this.client.attachLabelBatch(chunk, o), "Created");

    process.stderr.write(
      `riffdb-seed-progress\tphase=done\tcompleted=${completed}/${total}` +
        `\toverall_ms=${Date.now() - started}\n`,
    );
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
