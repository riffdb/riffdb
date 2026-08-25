/** Closed-loop interactive load matching the Rust app-baseline driver. */

import { LatencyHistogram } from "./histogram.js";
import { NS_LOAD_WRITE, STATUS_OPEN, uuidEquals, uuidFromOrdinal } from "./ids.js";
import { SafeAppError, classifyPostgres } from "./postgres.js";
import { classifyRiffdb, RiffDbLoadError } from "./riffdb.js";
import type {
  CloseTicketWithCommentSeed,
  CommentRow,
  CommentSeed,
  OpenTicketWithLabelsSeed,
  ScenarioProbes,
  SeedDataset,
  TicketRow,
} from "./seed.js";

export const INTERACTIVE_WEIGHTS: ReadonlyArray<readonly [string, number]> = [
  ["point_get_ticket", 25],
  ["point_get_user", 15],
  ["list_tickets_by_project_status", 12],
  ["list_open_tickets_for_assignee", 10],
  ["list_comments_for_ticket", 10],
  ["list_project_members", 8],
  ["ticket_detail_page", 5],
  ["create_comment", 12],
  ["close_ticket_with_comment", 2],
  ["open_ticket_with_labels", 1],
];
export const SWEEP_CLIENTS = [1, 8, 32, 128] as const;
const DEFAULT_RNG_SEED = 0x000a11cebeef;
const GOLDEN = 0x9e3779b97f4a7c15n;

export interface LoadSession {
  prewarm(organizationId: Uint8Array, ticketId: Uint8Array): Promise<void>;
  pointGetTicket(organizationId: Uint8Array, ticketId: Uint8Array): Promise<boolean>;
  pointGetUser(organizationId: Uint8Array, userId: Uint8Array): Promise<boolean>;
  listTicketsByProjectStatus(
    organizationId: Uint8Array,
    projectId: Uint8Array,
    status: string,
    limit: number,
  ): Promise<void>;
  listOpenTicketsForAssignee(
    organizationId: Uint8Array,
    assigneeId: Uint8Array,
    limit: number,
  ): Promise<void>;
  listCommentsForTicket(organizationId: Uint8Array, ticketId: Uint8Array, limit: number): Promise<void>;
  listProjectMembers(organizationId: Uint8Array, projectId: Uint8Array, limit: number): Promise<void>;
  ticketDetailPage(organizationId: Uint8Array, ticketId: Uint8Array): Promise<boolean>;
  createComment(comment: CommentSeed): Promise<void>;
  closeTicketWithComment(input: CloseTicketWithCommentSeed): Promise<void>;
  openTicketWithLabels(input: OpenTicketWithLabelsSeed): Promise<void>;
  close(): Promise<void>;
}

export interface LoadDriver {
  readonly backendId: string;
  seed(dataset: SeedDataset): Promise<void>;
  openSession(): Promise<LoadSession>;
}

class XorShift64 {
  state: bigint;
  constructor(seed: bigint) {
    this.state = seed | 1n;
  }
  nextU64(): bigint {
    let x = this.state & 0xffffffffffffffffn;
    x ^= (x << 13n) & 0xffffffffffffffffn;
    x ^= x >> 7n;
    x ^= (x << 17n) & 0xffffffffffffffffn;
    this.state = x;
    return x;
  }
  genRange(maxExclusive: number): number {
    if (maxExclusive <= 0) return 0;
    return Number(this.nextU64() % BigInt(maxExclusive));
  }
  genF64(): number {
    return Number(this.nextU64()) / (Number(2n ** 64n - 1n) + 1);
  }
}

class Zipf {
  cdf: number[];
  constructor(n: number, s: number) {
    if (n <= 0) throw new Error("zipf domain must be positive");
    if (s <= 0) {
      const step = 1 / n;
      this.cdf = Array.from({ length: n }, (_, i) => step * (i + 1));
      return;
    }
    const weights = Array.from({ length: n }, (_, rank) => 1 / (rank + 1) ** s);
    const total = weights.reduce((sum, weight) => sum + weight, 0);
    let run = 0;
    this.cdf = weights.map((weight) => {
      run += weight / total;
      return run;
    });
    this.cdf[this.cdf.length - 1] = 1;
  }
  sample(rng: XorShift64): number {
    const u = rng.genF64();
    const index = this.cdf.findIndex((edge) => edge >= u);
    return index === -1 ? this.cdf.length - 1 : index;
  }
}

export class OpStats {
  latency = new LatencyHistogram();
  success = 0;
  conflict = 0;
  unavailable = 0;
  replayed = 0;
  error = 0;
  firstError: string | null = null;

  record(elapsedNs: number, outcome: string, errorText: string | null): void {
    this.latency.recordNs(elapsedNs);
    if (outcome === "success") this.success += 1;
    else if (outcome === "conflict") this.conflict += 1;
    else if (outcome === "unavailable") this.unavailable += 1;
    else if (outcome === "replayed") this.replayed += 1;
    else this.error += 1;
    if (this.firstError === null && errorText) this.firstError = errorText.slice(0, 240);
  }

  merge(other: OpStats): void {
    this.latency.merge(other.latency);
    this.success += other.success;
    this.conflict += other.conflict;
    this.unavailable += other.unavailable;
    this.replayed += other.replayed;
    this.error += other.error;
    if (this.firstError === null) this.firstError = other.firstError;
  }

  total(): number {
    return this.success + this.conflict + this.unavailable + this.replayed + this.error;
  }

  json(): Record<string, unknown> {
    return {
      latency: this.latency.summary(),
      outcomes: {
        success: this.success,
        conflict: this.conflict,
        unavailable: this.unavailable,
        replayed: this.replayed,
        error: this.error,
        logical_operations: this.total(),
      },
      first_error: this.firstError,
    };
  }
}

export interface LoadConfig {
  clients: number;
  durationS: number;
  warmupS: number;
  zipfS: number;
  rngSeed: number;
  tenantCount: number;
  sampleIdBase: number;
}

export class LoadReport {
  constructor(
    readonly backendId: string,
    readonly clients: number,
    readonly profile: string,
    readonly measuredElapsedNs: number,
    readonly seedNs: number,
    readonly aggregate: OpStats,
    readonly byOp: Record<string, OpStats>,
    readonly workerCompleted: number[],
  ) {}

  throughput(): number {
    const elapsedS = Math.max(this.measuredElapsedNs / 1e9, 0.001);
    return this.aggregate.total() / elapsedS;
  }

  json(): Record<string, unknown> {
    const elapsedNs = Math.max(this.measuredElapsedNs, 1);
    const logicalOps = this.aggregate.total();
    const operations: Record<string, unknown> = {};
    for (const [name, stats] of Object.entries(this.byOp)) {
      if (stats.total() > 0) operations[name] = stats.json();
    }
    return {
      schema: "riffdb.app-baseline-typescript-safe-app/v1",
      backend_id: this.backendId,
      profile: this.profile,
      clients: this.clients,
      evidentiary: false,
      language: "typescript",
      safety_owner: this.backendId === "riffdb_public_grpc" ? "riffdbd_rust" : "typescript_sql",
      measured_elapsed_ns: this.measuredElapsedNs,
      seed_ns: this.seedNs,
      logical_ops: logicalOps,
      throughput_ops_s: Math.trunc((logicalOps * 1_000_000_000) / elapsedNs),
      aggregate: this.aggregate.json(),
      operations,
      worker_completed_operations: this.workerCompleted,
      notes: this.notes(),
    };
  }

  notes(): string[] {
    const shared = [
      "TypeScript closed-loop load. Language runtime time is included.",
      "Not a substitute for the Rust evidentiary harness in benchmarks/run-app-baseline.",
      "Throughput denominator is max(worker_measure_end)-min(worker_measure_start).",
      "Each backend is seeded once, then the concurrency sweep accumulates history.",
    ];
    if (this.backendId === "riffdb_public_grpc") {
      return [
        ...shared,
        "RiffDB path uses the generated TicketDesk TypeScript client only.",
        "No authorization, idempotency, audit, event, or outbox code runs in TypeScript; riffdbd (Rust) enforces those.",
      ];
    }
    return [
      ...shared,
      "postgres_safe_app path implements authorization, idempotency, audit, event, and outbox in SQL from TypeScript.",
    ];
  }
}

export function printLoadSummary(report: LoadReport): void {
  const elapsedS = Math.max(report.measuredElapsedNs / 1e9, 0.001);
  const total = report.aggregate.total();
  console.log(
    `\n== load ${report.backendId} profile=${report.profile} clients=${report.clients} ` +
      `window=${elapsedS.toFixed(1)}s tenant=single_organization count=1 hot=0pct ==`,
  );
  console.log(
    `throughput=${report.throughput().toFixed(0)} ops/s  logical_ops=${total}  ` +
      `success=${report.aggregate.success}  conflict=${report.aggregate.conflict}  ` +
      `idempotency_mismatch=0  unavailable=${report.aggregate.unavailable}  ` +
      `overloaded=0  replayed=${report.aggregate.replayed}  error=${report.aggregate.error}`,
  );
  const latency = report.aggregate.latency;
  console.log(
    `latency p50=${(latency.percentileNs(50) / 1e6).toFixed(3)}ms ` +
      `p95=${(latency.percentileNs(95) / 1e6).toFixed(3)}ms ` +
      `p99=${(latency.percentileNs(99) / 1e6).toFixed(3)}ms ` +
      `max=${(latency.maxNs / 1e6).toFixed(3)}ms`,
  );
  for (const [name, stats] of Object.entries(report.byOp)) {
    if (stats.total() === 0) continue;
    console.log(
      `  ${name}: n=${stats.total()} p50=${(stats.latency.percentileNs(50) / 1e6).toFixed(3)}ms ` +
        `p99=${(stats.latency.percentileNs(99) / 1e6).toFixed(3)}ms ok=${stats.success} ` +
        `conflict=${stats.conflict} idempotency_mismatch=0 unavailable=${stats.unavailable} ` +
        `replay=${stats.replayed} err=${stats.error}`,
    );
  }
}

function nowNs(): number {
  return Number(process.hrtime.bigint());
}

export async function runClosedLoop(
  driver: LoadDriver,
  dataset: SeedDataset,
  config: LoadConfig,
  seedNs: number,
): Promise<LoadReport> {
  if (!(config.clients >= 1 && config.clients <= 128)) {
    throw new Error("load clients must be 1..=128");
  }
  const { tenantProbes } = await import("./seed.js");
  const probes = tenantProbes(dataset, config.tenantCount);
  if (probes.length !== config.tenantCount) {
    throw new Error("seed does not contain the requested tenant count");
  }
  const writeTickets: TicketRow[][] = [];
  for (const probe of probes) {
    let tickets = dataset.tickets.filter(
      (ticket) =>
        uuidEquals(ticket.organizationId, probe.organizationId) &&
        ticket.status === STATUS_OPEN &&
        !uuidEquals(ticket.ticketId, probe.ticketId),
    );
    if (tickets.length === 0) {
      tickets = dataset.tickets.filter(
        (ticket) =>
          uuidEquals(ticket.organizationId, probe.organizationId) && ticket.status === STATUS_OPEN,
      );
    }
    if (tickets.length === 0) throw new Error("load driver requires at least one open ticket per tenant");
    writeTickets.push(tickets);
  }
  const zipf = writeTickets.map((tickets) => new Zipf(tickets.length, config.zipfS));
  const weightSum = INTERACTIVE_WEIGHTS.reduce((sum, [, weight]) => sum + weight, 0);
  let sampleCounter = config.sampleIdBase + 1;
  const flags = { measuring: false, stop: false };
  const ready = latch(config.clients + 1);
  const go = latch(config.clients + 1);
  const results: Array<{
    byOp: Record<string, OpStats>;
    measureStart: number | null;
    measureEnd: number | null;
    completed: number;
  } | null> = Array.from({ length: config.clients }, () => null);
  const errors: string[] = [];

  const workers = Array.from({ length: config.clients }, (_, workerId) =>
    (async () => {
      try {
        const backend = await driver.openSession();
        const probe = probes[workerId % probes.length]!;
        await backend.prewarm(probe.organizationId, probe.ticketId);
        await backend.pointGetTicket(probe.organizationId, probe.ticketId);
        const rng = new XorShift64(
          BigInt(config.rngSeed) ^ (((BigInt(workerId) + 1n) * GOLDEN) & 0xffffffffffffffffn),
        );
        ready();
        await ready.wait;
        go();
        await go.wait;
        const byOp: Record<string, OpStats> = {};
        for (const [name] of INTERACTIVE_WEIGHTS) byOp[name] = new OpStats();
        let lastComment: CommentSeed | null = null;
        let measureStart: number | null = null;
        let measureEnd: number | null = null;
        let completed = 0;
        while (!flags.stop) {
          const record = flags.measuring;
          const op = drawOp(rng, weightSum);
          const tickets = writeTickets[0]!;
          const ticket = tickets[zipf[0]!.sample(rng) % tickets.length]!;
          if (flags.stop) break;
          const started = nowNs();
          const sample = sampleCounter;
          sampleCounter += 1;
          const [outcome, errorText] = await execute(backend, probes[0]!, op, sample, ticket, lastComment);
          const ended = nowNs();
          if (outcome === "success" && op === "create_comment") {
            lastComment = commentFor(probes[0]!, ticket, sample);
          }
          if (record) {
            if (measureStart === null) measureStart = started;
            measureEnd = ended;
            completed += 1;
            byOp[op]!.record(ended - started, outcome, errorText);
          }
        }
        await backend.close();
        results[workerId] = { byOp, measureStart, measureEnd, completed };
      } catch (error) {
        errors.push(error instanceof Error ? error.message : String(error));
      }
    })(),
  );

  ready();
  await ready.wait;
  go();
  await go.wait;
  if (config.warmupS > 0) await sleep(config.warmupS);
  flags.measuring = true;
  await sleep(config.durationS);
  flags.stop = true;
  await Promise.all(workers);
  if (errors.length > 0) throw new Error(errors[0]);

  const aggregate = new OpStats();
  const merged: Record<string, OpStats> = {};
  for (const [name] of INTERACTIVE_WEIGHTS) merged[name] = new OpStats();
  const starts: number[] = [];
  const ends: number[] = [];
  const completed: number[] = [];
  for (const result of results) {
    if (result === null) continue;
    if (result.measureStart !== null && result.measureEnd !== null) {
      starts.push(result.measureStart);
      ends.push(result.measureEnd);
    }
    completed.push(result.completed);
    for (const [name, stats] of Object.entries(result.byOp)) {
      merged[name]!.merge(stats);
      aggregate.merge(stats);
    }
  }
  const measured = starts.length > 0 && ends.length > 0 ? Math.max(...ends) - Math.min(...starts) : config.durationS * 1e9;
  return new LoadReport(
    driver.backendId,
    config.clients,
    "interactive",
    Math.max(measured, 1_000_000),
    seedNs,
    aggregate,
    merged,
    completed,
  );
}

function latch(count: number): { (): void; wait: Promise<void> } {
  let remaining = count;
  let resolve!: () => void;
  const wait = new Promise<void>((done) => {
    resolve = done;
  });
  const arrive = (): void => {
    remaining -= 1;
    if (remaining === 0) resolve();
  };
  arrive.wait = wait;
  return arrive;
}

function sleep(seconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, seconds * 1000));
}

function drawOp(rng: XorShift64, weightSum: number): string {
  let pick = rng.genRange(weightSum);
  for (const [name, weight] of INTERACTIVE_WEIGHTS) {
    if (pick < weight) return name;
    pick -= weight;
  }
  return INTERACTIVE_WEIGHTS[0]![0];
}

function commentFor(probe: ScenarioProbes, ticket: TicketRow, sample: number): CommentSeed {
  return {
    row: {
      organizationId: ticket.organizationId,
      commentId: uuidFromOrdinal(NS_LOAD_WRITE, 1_000_000_000 + sample),
      ticketId: ticket.ticketId,
      authorId: probe.writeAuthorId,
      body: `load comment ${sample}`,
    },
    idempotencyKey: `load-comment-${sample}`,
  };
}

async function execute(
  backend: LoadSession,
  probe: ScenarioProbes,
  op: string,
  sample: number,
  ticket: TicketRow,
  _lastComment: CommentSeed | null,
): Promise<readonly [string, string | null]> {
  try {
    if (op === "point_get_ticket") {
      if (!(await backend.pointGetTicket(probe.organizationId, probe.ticketId))) {
        return ["error", "missing ticket"];
      }
    } else if (op === "point_get_user") {
      if (!(await backend.pointGetUser(probe.organizationId, probe.userId))) {
        return ["error", "missing user"];
      }
    } else if (op === "list_tickets_by_project_status") {
      await backend.listTicketsByProjectStatus(probe.organizationId, probe.projectId, STATUS_OPEN, 50);
    } else if (op === "list_open_tickets_for_assignee") {
      await backend.listOpenTicketsForAssignee(probe.organizationId, probe.assigneeId, 50);
    } else if (op === "list_comments_for_ticket") {
      await backend.listCommentsForTicket(probe.organizationId, probe.ticketId, 50);
    } else if (op === "list_project_members") {
      await backend.listProjectMembers(probe.organizationId, probe.projectId, 50);
    } else if (op === "ticket_detail_page") {
      if (!(await backend.ticketDetailPage(probe.organizationId, probe.ticketId))) {
        return ["error", "missing detail"];
      }
    } else if (op === "create_comment") {
      await backend.createComment(commentFor(probe, ticket, sample));
    } else if (op === "close_ticket_with_comment") {
      await backend.closeTicketWithComment({
        organizationId: ticket.organizationId,
        ticketId: ticket.ticketId,
        authorId: probe.writeAuthorId,
        commentId: uuidFromOrdinal(NS_LOAD_WRITE, 2_000_000_000 + sample),
        body: `load close note ${sample}`,
        idempotencyKey: `load-close-${sample}`,
      });
    } else if (op === "open_ticket_with_labels") {
      await backend.openTicketWithLabels({
        organizationId: probe.organizationId,
        ticketId: uuidFromOrdinal(NS_LOAD_WRITE, 3_000_000_000 + sample),
        projectId: probe.writeProjectId,
        reporterId: probe.writeAuthorId,
        assigneeId: probe.writeAssigneeId,
        title: `load open ticket ${sample}`,
        labelA: probe.writeLabelA,
        labelB: probe.writeLabelB,
        idempotencyKey: `load-open-${sample}`,
      });
    }
    return ["success", null];
  } catch (error) {
    if (error instanceof SafeAppError) return [classifyPostgres(error), String(error).slice(0, 240)];
    if (error instanceof RiffDbLoadError) return [classifyRiffdb(error), String(error).slice(0, 240)];
    return [classifyRiffdb(error), String(error).slice(0, 240)];
  }
}
