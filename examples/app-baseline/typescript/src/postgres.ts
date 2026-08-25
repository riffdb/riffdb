/** PostgreSQL postgres_safe_app adapter. Safety lives in TypeScript SQL. */

import { Client } from "pg";

import { formatUuid, type UuidBytes } from "./ids.js";
import {
  CLOSE_TICKET_SQL,
  COUNT_TICKET_SQL,
  COUNT_USER_SQL,
  IDEMPOTENCY_LOCK_SQL,
  INSERT_AUDIT_SQL,
  INSERT_COMMENT_SQL,
  INSERT_EVENT_SQL,
  INSERT_IDEMPOTENCY_SQL,
  INSERT_LABEL_SQL,
  INSERT_MEMBER_SQL,
  INSERT_ORGANIZATION_SQL,
  INSERT_OUTBOX_SQL,
  INSERT_PROJECT_SQL,
  INSERT_TICKET_LABEL_SQL,
  INSERT_TICKET_SQL,
  INSERT_USER_SQL,
  LIST_COMMENTS_SQL,
  LIST_OPEN_TICKETS_FOR_ASSIGNEE_SQL,
  LIST_PROJECT_MEMBERS_SQL,
  LIST_TICKETS_BY_PROJECT_STATUS_SQL,
  OPEN_TICKET_SQL,
  PERMISSION_SQL,
  POSTGRES_BACKEND_ID,
  SCHEMA_SQL,
  SELECT_TICKET_SQL,
  SELECT_USER_SQL,
  TICKET_DETAIL_LABELS_SQL,
  TICKET_DETAIL_SQL,
  safeInputFingerprint,
} from "./schema.js";
import type {
  CloseTicketWithCommentSeed,
  CommentSeed,
  OpenTicketWithLabelsSeed,
  SeedDataset,
} from "./seed.js";

const CONFLICT_STATES = new Set(["23505", "23P01"]);
const UNAVAILABLE_STATES = new Set(["57P03", "53300", "57P01", "57P02", "08006", "08001", "08004"]);

const TIMED_STATEMENTS = [
  SELECT_TICKET_SQL,
  SELECT_USER_SQL,
  LIST_TICKETS_BY_PROJECT_STATUS_SQL,
  LIST_OPEN_TICKETS_FOR_ASSIGNEE_SQL,
  LIST_COMMENTS_SQL,
  LIST_PROJECT_MEMBERS_SQL,
  TICKET_DETAIL_SQL,
  TICKET_DETAIL_LABELS_SQL,
  COUNT_TICKET_SQL,
  COUNT_USER_SQL,
  INSERT_COMMENT_SQL,
  CLOSE_TICKET_SQL,
  OPEN_TICKET_SQL,
  INSERT_TICKET_LABEL_SQL,
  PERMISSION_SQL,
  IDEMPOTENCY_LOCK_SQL,
  INSERT_IDEMPOTENCY_SQL,
  INSERT_AUDIT_SQL,
  INSERT_EVENT_SQL,
  INSERT_OUTBOX_SQL,
];

export class SafeAppError extends Error {
  override readonly name = "SafeAppError";
  constructor(
    message: string,
    readonly sqlstate: string | null = null,
  ) {
    super(message);
  }
}

export function classifyPostgres(error: SafeAppError): string {
  if (error.sqlstate !== null && CONFLICT_STATES.has(error.sqlstate)) return "conflict";
  if (error.sqlstate !== null && UNAVAILABLE_STATES.has(error.sqlstate)) return "unavailable";
  return "error";
}

function dbError(error: unknown): SafeAppError {
  const sqlstate =
    typeof error === "object" && error !== null && "code" in error && typeof error.code === "string"
      ? error.code
      : null;
  return new SafeAppError(error instanceof Error ? error.message : String(error), sqlstate);
}

export class SafeAppSession {
  constructor(private readonly client: Client) {}

  static async connect(url: string): Promise<SafeAppSession> {
    let lastError: unknown;
    for (let attempt = 0; attempt < 40; attempt += 1) {
      const client = new Client({ connectionString: url });
      try {
        await client.connect();
        await client.query("SET synchronous_commit = on");
        return new SafeAppSession(client);
      } catch (error) {
        lastError = error;
        try {
          await client.end();
        } catch {
          /* ignore */
        }
        await new Promise((resolve) => setTimeout(resolve, 250));
      }
    }
    throw lastError instanceof Error ? lastError : new Error(String(lastError));
  }

  async close(): Promise<void> {
    await this.client.end();
  }

  async reset(): Promise<void> {
    await this.client.query(SCHEMA_SQL);
  }

  async prewarm(organizationId: UuidBytes, ticketId: UuidBytes): Promise<void> {
    const org = formatUuid(organizationId);
    const ticket = formatUuid(ticketId);
    for (const statement of TIMED_STATEMENTS) {
      try {
        await this.client.query(statement, prewarmParams(statement, org, ticket));
      } catch {
        try {
          await this.client.query("ROLLBACK");
        } catch {
          /* ignore */
        }
      }
    }
  }

  async seed(dataset: SeedDataset): Promise<void> {
    await this.client.query("BEGIN");
    try {
      for (const [orgId, name] of dataset.organizations) {
        await this.client.query(INSERT_ORGANIZATION_SQL, [formatUuid(orgId), name]);
      }
      for (const [orgId, userId, email, display] of dataset.users) {
        await this.client.query(INSERT_USER_SQL, [
          formatUuid(orgId),
          formatUuid(userId),
          email,
          display,
        ]);
      }
      for (const [orgId, projectId, name] of dataset.projects) {
        await this.client.query(INSERT_PROJECT_SQL, [
          formatUuid(orgId),
          formatUuid(projectId),
          name,
        ]);
      }
      for (const [orgId, projectId, userId, role] of dataset.members) {
        await this.client.query(INSERT_MEMBER_SQL, [
          formatUuid(orgId),
          formatUuid(projectId),
          formatUuid(userId),
          role,
        ]);
      }
      for (const [orgId, labelId, name] of dataset.labels) {
        await this.client.query(INSERT_LABEL_SQL, [formatUuid(orgId), formatUuid(labelId), name]);
      }
      for (const ticket of dataset.tickets) {
        await this.client.query(INSERT_TICKET_SQL, [
          formatUuid(ticket.organizationId),
          formatUuid(ticket.ticketId),
          formatUuid(ticket.projectId),
          formatUuid(ticket.reporterId),
          formatUuid(ticket.assigneeId),
          ticket.status,
          ticket.title,
        ]);
      }
      for (const comment of dataset.comments) {
        await this.client.query(INSERT_COMMENT_SQL, [
          formatUuid(comment.organizationId),
          formatUuid(comment.commentId),
          formatUuid(comment.ticketId),
          formatUuid(comment.authorId),
          comment.body,
        ]);
      }
      for (const [orgId, ticketId, labelId] of dataset.ticketLabels) {
        await this.client.query(INSERT_TICKET_LABEL_SQL, [
          formatUuid(orgId),
          formatUuid(ticketId),
          formatUuid(labelId),
        ]);
      }
      await this.client.query("COMMIT");
    } catch (error) {
      await this.client.query("ROLLBACK");
      throw error;
    }
    // Settle the freshly bulk-loaded database before measurement. Without
    // ANALYZE the measured window starts with no planner statistics and an
    // autoanalyze can fire inside it, which is the comparator's dominant
    // run-to-run variance source; CHECKPOINT then moves the seed's dirty pages
    // out of that window. This mirrors the Rust and Python harnesses.
    try {
      await this.client.query("VACUUM (ANALYZE)");
      await this.client.query("CHECKPOINT");
    } catch {
      try {
        await this.client.query("ROLLBACK");
      } catch {
        /* ignore */
      }
    }
  }

  async pointGetTicket(organizationId: UuidBytes, ticketId: UuidBytes): Promise<boolean> {
    const result = await this.exec(SELECT_TICKET_SQL, [
      formatUuid(organizationId),
      formatUuid(ticketId),
    ]);
    return result.rowCount !== null && result.rowCount > 0;
  }

  async pointGetUser(organizationId: UuidBytes, userId: UuidBytes): Promise<boolean> {
    const result = await this.exec(SELECT_USER_SQL, [formatUuid(organizationId), formatUuid(userId)]);
    return result.rowCount !== null && result.rowCount > 0;
  }

  async listTicketsByProjectStatus(
    organizationId: UuidBytes,
    projectId: UuidBytes,
    status: string,
    limit: number,
  ): Promise<void> {
    await this.exec(LIST_TICKETS_BY_PROJECT_STATUS_SQL, [
      formatUuid(organizationId),
      formatUuid(projectId),
      status,
      limit,
    ]);
  }

  async listOpenTicketsForAssignee(
    organizationId: UuidBytes,
    assigneeId: UuidBytes,
    limit: number,
  ): Promise<void> {
    await this.exec(LIST_OPEN_TICKETS_FOR_ASSIGNEE_SQL, [
      formatUuid(organizationId),
      formatUuid(assigneeId),
      limit,
    ]);
  }

  async listCommentsForTicket(
    organizationId: UuidBytes,
    ticketId: UuidBytes,
    limit: number,
  ): Promise<void> {
    await this.exec(LIST_COMMENTS_SQL, [formatUuid(organizationId), formatUuid(ticketId), limit]);
  }

  async listProjectMembers(
    organizationId: UuidBytes,
    projectId: UuidBytes,
    _limit: number,
  ): Promise<void> {
    await this.exec(LIST_PROJECT_MEMBERS_SQL, [
      formatUuid(organizationId),
      formatUuid(projectId),
      _limit,
    ]);
  }

  async ticketDetailPage(organizationId: UuidBytes, ticketId: UuidBytes): Promise<boolean> {
    const row = await this.exec(TICKET_DETAIL_SQL, [
      formatUuid(organizationId),
      formatUuid(ticketId),
    ]);
    await this.exec(TICKET_DETAIL_LABELS_SQL, [
      formatUuid(organizationId),
      formatUuid(ticketId),
    ]);
    return row.rowCount !== null && row.rowCount > 0;
  }

  async createComment(comment: CommentSeed): Promise<void> {
    const fingerprint = safeInputFingerprint([
      comment.row.commentId,
      comment.row.ticketId,
      comment.row.authorId,
      Buffer.from(comment.row.body),
    ]);
    await this.transact(async () => {
      if (await this.safeAdmit(comment.idempotencyKey, "create_comment", comment.row.organizationId, fingerprint)) {
        return;
      }
      await this.exec(INSERT_COMMENT_SQL, [
        formatUuid(comment.row.organizationId),
        formatUuid(comment.row.commentId),
        formatUuid(comment.row.ticketId),
        formatUuid(comment.row.authorId),
        comment.row.body,
      ]);
      await this.safeComplete(
        comment.idempotencyKey,
        "create_comment",
        comment.row.organizationId,
        "CommentCreated",
      );
    });
  }

  async closeTicketWithComment(input: CloseTicketWithCommentSeed): Promise<void> {
    const fingerprint = safeInputFingerprint([
      input.ticketId,
      input.commentId,
      input.authorId,
      Buffer.from(input.body),
    ]);
    await this.transact(async () => {
      if (
        await this.safeAdmit(
          input.idempotencyKey,
          "close_ticket_with_comment",
          input.organizationId,
          fingerprint,
        )
      ) {
        return;
      }
      const ticketOk = await this.exec(COUNT_TICKET_SQL, [
        formatUuid(input.organizationId),
        formatUuid(input.ticketId),
      ]);
      const authorOk = await this.exec(COUNT_USER_SQL, [
        formatUuid(input.organizationId),
        formatUuid(input.authorId),
      ]);
      if (Number(ticketOk.rows[0]?.count ?? 0) === 0 || Number(authorOk.rows[0]?.count ?? 0) === 0) {
        throw new SafeAppError("decode");
      }
      await this.exec(CLOSE_TICKET_SQL, [formatUuid(input.organizationId), formatUuid(input.ticketId)]);
      await this.exec(INSERT_COMMENT_SQL, [
        formatUuid(input.organizationId),
        formatUuid(input.commentId),
        formatUuid(input.ticketId),
        formatUuid(input.authorId),
        input.body,
      ]);
      await this.safeComplete(
        input.idempotencyKey,
        "close_ticket_with_comment",
        input.organizationId,
        "TicketClosedWithComment",
      );
    });
  }

  async openTicketWithLabels(input: OpenTicketWithLabelsSeed): Promise<void> {
    const fingerprint = safeInputFingerprint([
      input.ticketId,
      input.projectId,
      input.reporterId,
      input.assigneeId,
      Buffer.from(input.title),
      input.labelA,
      input.labelB,
    ]);
    await this.transact(async () => {
      if (
        await this.safeAdmit(
          input.idempotencyKey,
          "open_ticket_with_labels",
          input.organizationId,
          fingerprint,
        )
      ) {
        return;
      }
      await this.exec(OPEN_TICKET_SQL, [
        formatUuid(input.organizationId),
        formatUuid(input.ticketId),
        formatUuid(input.projectId),
        formatUuid(input.reporterId),
        formatUuid(input.assigneeId),
        input.title,
      ]);
      await this.exec(INSERT_TICKET_LABEL_SQL, [
        formatUuid(input.organizationId),
        formatUuid(input.ticketId),
        formatUuid(input.labelA),
      ]);
      await this.exec(INSERT_TICKET_LABEL_SQL, [
        formatUuid(input.organizationId),
        formatUuid(input.ticketId),
        formatUuid(input.labelB),
      ]);
      await this.safeComplete(
        input.idempotencyKey,
        "open_ticket_with_labels",
        input.organizationId,
        "TicketOpenedWithLabels",
      );
    });
  }

  private async exec(statement: string, params: unknown[] = []) {
    try {
      return await this.client.query(statement, params);
    } catch (error) {
      throw dbError(error);
    }
  }

  private async transact(body: () => Promise<void>): Promise<void> {
    try {
      await this.client.query("BEGIN");
      await body();
      await this.client.query("COMMIT");
    } catch (error) {
      try {
        await this.client.query("ROLLBACK");
      } catch {
        /* ignore */
      }
      if (error instanceof SafeAppError) throw error;
      throw dbError(error);
    }
  }

  private async safeAdmit(
    idempotencyKey: string,
    operation: string,
    organizationId: UuidBytes,
    fingerprint: string,
  ): Promise<boolean> {
    const authorized = await this.exec(PERMISSION_SQL, [operation]);
    if (authorized.rowCount === 0) throw new SafeAppError("decode");
    const existing = await this.exec(IDEMPOTENCY_LOCK_SQL, [idempotencyKey]);
    const orgText = formatUuid(organizationId);
    const row = existing.rows[0] as
      | { operation: string; organization_id: string; input_fingerprint: string }
      | undefined;
    if (row !== undefined) {
      if (
        row.operation !== operation ||
        row.organization_id !== orgText ||
        row.input_fingerprint !== fingerprint
      ) {
        throw new SafeAppError("decode");
      }
      await this.exec(INSERT_AUDIT_SQL, [idempotencyKey, operation, orgText, "replayed"]);
      return true;
    }
    await this.exec(INSERT_IDEMPOTENCY_SQL, [idempotencyKey, operation, orgText, fingerprint]);
    return false;
  }

  private async safeComplete(
    idempotencyKey: string,
    operation: string,
    organizationId: UuidBytes,
    eventType: string,
  ): Promise<void> {
    const orgText = formatUuid(organizationId);
    const eventId = `${operation}/${idempotencyKey}`;
    await this.exec(INSERT_AUDIT_SQL, [idempotencyKey, operation, orgText, "committed"]);
    await this.exec(INSERT_EVENT_SQL, [eventId, idempotencyKey, eventType, orgText]);
    await this.exec(INSERT_OUTBOX_SQL, [eventId]);
  }
}

export class PostgresDriver {
  readonly backendId = POSTGRES_BACKEND_ID;

  constructor(private readonly url: string) {}

  async seed(dataset: SeedDataset): Promise<void> {
    const session = await SafeAppSession.connect(this.url);
    try {
      await session.reset();
      await session.seed(dataset);
    } finally {
      await session.close();
    }
  }

  async openSession(): Promise<SafeAppSession> {
    return SafeAppSession.connect(this.url);
  }
}

function prewarmParams(statement: string, org: string, ticket: string): unknown[] {
  const placeholders = (statement.match(/\$/g) ?? []).length;
  if (placeholders === 0) return [];
  if (statement.includes("LIMIT $4")) return [org, ticket, "open", 1];
  if (statement.includes("LIMIT $3")) return [org, ticket, 1];
  if (statement.includes("principal = 'app-baseline'")) return ["point_get_ticket"];
  if (statement.includes("idempotency_key = $1")) return ["prewarm"];
  if (placeholders === 1) return [org];
  if (placeholders >= 2) {
    const filled = [org, ticket, ...Array.from({ length: placeholders - 2 }, () => org)];
    return filled.slice(0, placeholders);
  }
  return [];
}
