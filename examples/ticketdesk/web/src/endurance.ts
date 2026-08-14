import { lstat, readFile, rename, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { performance } from "node:perf_hooks";

import {
  DriverApplicationTransport,
  DriverGeneratedApplicationTransport,
  type DriverApplicationIdentity,
} from "@riffdb/application";

import {
  TicketDeskClient,
  TicketDeskReactiveClient,
  type TicketEventsDelivery,
} from "./generated/client.js";

const TENANTS = ["tenant_alpha", "tenant_beta", "tenant_gamma", "tenant_delta"] as const;
const REQUIRED_COVERAGE = [
  "cold_keys", "events", "hot_keys", "live_queries",
  "multiple_tenants", "reads", "workflows", "writes",
] as const;
type Workload = "events" | "live_queries" | "reads" | "workflows" | "writes";
type Tenant = typeof TENANTS[number];
const LATENCY_BOUNDS_US = [
  50, 100, 200, 400, 800, 1_600, 3_200, 6_400,
  12_800, 25_600, 51_200, 102_400, 204_800, 409_600, 819_200, Number.MAX_SAFE_INTEGER,
] as const;

interface IdentityFile {
  readonly applicationManifestHash: string;
  readonly operationCatalogHash: string;
  readonly contractLineage: string;
  readonly contractVersion: number;
  readonly contractBundleHash: string;
  readonly database: string;
  readonly role: string;
  readonly roleDefinitionHash: string;
  readonly remoteIdentityHash: string;
}

interface ConnectedClient {
  readonly generated: TicketDeskClient;
  readonly reactive: TicketDeskReactiveClient;
  readonly driver: DriverApplicationTransport;
}

class Metrics {
  readonly #path: string;
  readonly #started = Math.floor(Date.now() / 1000);
  #logicalOperations = 0;
  #retainedBytes = 0;
  #eventsEmitted = 0;
  #consumerAcknowledgements = 0;
  #publishing: Promise<void> = Promise.resolve();
  readonly #workloads: Record<Workload, number> = {
    events: 0, live_queries: 0, reads: 0, workflows: 0, writes: 0,
  };
  readonly #tenants: Record<Tenant, number> = {
    tenant_alpha: 0, tenant_beta: 0, tenant_gamma: 0, tenant_delta: 0,
  };
  readonly #latencyCounts = Array.from({ length: LATENCY_BOUNDS_US.length }, () => 0);

  public constructor(path: string) { this.#path = path; }

  public eventEmitted(): void { this.#eventsEmitted += 1; }

  public consumerAcknowledged(): void { this.#consumerAcknowledgements += 1; }

  public async record(workload: Workload, tenant: Tenant, retainedBytes: number, startedAt: number): Promise<void> {
    this.#logicalOperations += 1;
    this.#retainedBytes += retainedBytes;
    this.#workloads[workload] += 1;
    this.#tenants[tenant] += 1;
    const latencyUs = Math.max(0, Math.floor((performance.now() - startedAt) * 1_000));
    const bucket = LATENCY_BOUNDS_US.findIndex((bound) => latencyUs <= bound);
    this.#latencyCounts[bucket < 0 ? this.#latencyCounts.length - 1 : bucket]! += 1;
    if (this.#logicalOperations % 64 === 0) await this.publish();
  }

  public async publish(): Promise<void> {
    const value = `${JSON.stringify({
      schema: "riffdb.alpha-endurance-worker/v1",
      language: "typescript",
      pid: process.pid,
      started_unix_seconds: this.#started,
      logical_operations: this.#logicalOperations,
      transport_attempts: this.#logicalOperations,
      declared_retries: 0,
      error_count: 0,
      events_emitted: this.#eventsEmitted,
      consumer_acknowledgements: this.#consumerAcknowledgements,
      modeled_retained_bytes: this.#retainedBytes,
      latency_bounds_us: LATENCY_BOUNDS_US,
      latency_counts: this.#latencyCounts,
      workloads: this.#workloads,
      tenants: this.#tenants,
    })}\n`;
    const next = `${this.#path}.next`;
    this.#publishing = this.#publishing.then(async () => {
      await writeFile(next, value, { mode: 0o600 });
      await rename(next, this.#path);
    });
    await this.#publishing;
  }
}

const configuration = await requireEnvironment();
const environmentRoot = join(configuration.artifactRoot, "environment-v1");
const metrics = new Metrics(join(environmentRoot, "metrics", "typescript.json"));
await metrics.publish();
const delayMilliseconds = Math.max(1, Math.floor((1_000 * configuration.clients) / configuration.maximumRate));
await Promise.race(TENANTS.map((tenant, index) => runClient(tenant, index, delayMilliseconds)));

async function requireEnvironment(): Promise<{
  readonly artifactRoot: string;
  readonly clients: number;
  readonly maximumRate: number;
  readonly seed: bigint;
}> {
  if (required("RIFFDB_ENDURANCE_LANGUAGE") !== "typescript") throw new Error("TypeScript endurance language differs from the closed manifest");
  const artifactRoot = required("RIFFDB_ENDURANCE_ARTIFACT_ROOT");
  if (!artifactRoot.startsWith("/")) throw new Error("RIFFDB_ENDURANCE_ARTIFACT_ROOT must be absolute");
  const rootStat = await lstat(artifactRoot);
  if (!rootStat.isDirectory() || rootStat.isSymbolicLink()) throw new Error("RIFFDB_ENDURANCE_ARTIFACT_ROOT must be a non-symlink directory");
  const tenants = JSON.parse(required("RIFFDB_ENDURANCE_TENANTS_JSON")) as unknown;
  if (!Array.isArray(tenants) || tenants.length !== TENANTS.length
      || tenants.some((tenant, index) => tenant !== TENANTS[index])) {
    throw new Error("TypeScript endurance tenants differ from the closed manifest");
  }
  const coverage = JSON.parse(required("RIFFDB_ENDURANCE_WORKLOAD_COVERAGE_JSON")) as unknown;
  if (!Array.isArray(coverage) || REQUIRED_COVERAGE.some((name) => !coverage.includes(name))) {
    throw new Error("TypeScript endurance workload coverage is incomplete");
  }
  return {
    artifactRoot,
    clients: boundedNumber("RIFFDB_ENDURANCE_CLIENTS", 4, 4),
    maximumRate: boundedNumber("RIFFDB_ENDURANCE_MAXIMUM_OPERATIONS_PER_SECOND", 1, 1_024),
    seed: BigInt(required("RIFFDB_ENDURANCE_SEED")),
  };
}

function required(name: string): string {
  const value = process.env[name];
  if (value === undefined || value.length < 1 || value.length > 16_384 || value.includes("\0")) {
    throw new Error(`${name} is required`);
  }
  return value;
}

function boundedNumber(name: string, minimum: number, maximum: number): number {
  const value = Number(required(name));
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) throw new Error(`${name} is outside its checked bound`);
  return value;
}

async function connect(role: "seeder" | "application" | "agent"): Promise<ConnectedClient> {
  const wire = JSON.parse(await readFile(join(environmentRoot, `${role}.identity.json`), "utf8")) as IdentityFile;
  const identity: DriverApplicationIdentity = {
    applicationManifestHash: wire.applicationManifestHash,
    operationCatalogHash: wire.operationCatalogHash,
    contractLineage: wire.contractLineage,
    contractVersion: BigInt(wire.contractVersion),
    contractBundleHash: wire.contractBundleHash,
    database: wire.database,
    role: wire.role,
    roleDefinitionHash: wire.roleDefinitionHash,
    remoteIdentityHash: wire.remoteIdentityHash,
  };
  const driver = await DriverApplicationTransport.connect({
    socketPath: join(environmentRoot, `${role}.sock`), identity,
  });
  const transport = new DriverGeneratedApplicationTransport(driver);
  return { generated: new TicketDeskClient(transport, 1), reactive: new TicketDeskReactiveClient(transport), driver };
}

async function runClient(tenant: Tenant, index: number, delayMilliseconds: number): Promise<never> {
  const seeder = await connect("seeder");
  const application = await connect("application");
  const agent = await connect("agent");
  try {
    const namespace = configuration.seed + 2n;
    const organizationId = id(10n, BigInt(index));
    const userId = id(namespace, 100n + BigInt(index));
    const projectId = id(namespace, 200n + BigInt(index));
    const hotTicketId = id(namespace, 300n + BigInt(index));
    await seedClient(seeder.generated, application.generated, tenant, index, organizationId, userId, projectId, hotTicketId);
    const eventParameters = { organization_id: organizationId };
    const eventConsumer = `endurance-typescript-events-${index}`;
    const triageConsumer = `endurance-typescript-triage-${index}`;
    for (let counter = 0; ; counter += 1) {
      let operationStarted = performance.now();
      const slot = counter % 100;
      if (slot < 35) {
        const result = await application.generated.ticketPage({ organization_id: organizationId, ticket_id: hotTicketId });
        if (result.value.outcome !== "Found") throw new Error("TypeScript endurance read lost its hot ticket");
        await metrics.record("reads", tenant, 0, operationStarted);
      } else if (slot < 60) {
        await application.generated.createComment({
          body: `typescript endurance comment ${counter}`, author_id: userId,
          ticket_id: hotTicketId, comment_id: id(namespace, 10_000n + BigInt(index) * 1_000_000n + BigInt(counter)),
          idempotency_key: `endurance-typescript-comment-${index}-${counter}`, organization_id: organizationId,
        });
        await metrics.record("writes", tenant, 512, operationStarted);
      } else if (slot < 70) {
        const batch = await agent.reactive.nextTriageTicket(eventParameters, triageConsumer, 0);
        await metrics.record("workflows", tenant, 0, operationStarted);
        operationStarted = performance.now();
        const item = batch.items[0];
        if (item !== undefined) {
          await agent.reactive.reactComment(eventParameters, triageConsumer, item, {
            body: "typescript contextual endurance reaction", author_id: item.delivery.event.reporter_id,
            ticket_id: item.delivery.event.ticket_id,
            comment_id: id(namespace, 20_000n + BigInt(index) * 1_000_000n + BigInt(counter)),
            idempotency_key: `endurance-typescript-reaction-${index}-${counter}`, organization_id: organizationId,
          });
          metrics.consumerAcknowledged();
          await metrics.record("workflows", tenant, 512, operationStarted);
        }
      } else if (slot < 80) {
        const stream = agent.reactive.ticketEvents(eventParameters, eventConsumer, {
          batchLimit: 1, inFlightLimit: 4, leaseSeconds: 60, maximumWaitMs: 0,
        });
        const iterator = stream[Symbol.asyncIterator]();
        const result = await iterator.next();
        await iterator.return?.();
        if (result.done === true) throw new Error("TypeScript event stream ended before a batch");
        await metrics.record("events", tenant, 0, operationStarted);
        operationStarted = performance.now();
        const delivery: TicketEventsDelivery | undefined = result.value.events[0];
        if (delivery !== undefined) {
          await agent.reactive.ackTicketEvents(eventParameters, eventConsumer, delivery);
          metrics.consumerAcknowledged();
          await metrics.record("events", tenant, 0, operationStarted);
        }
      } else {
        const abort = new AbortController();
        const timer = setTimeout(() => abort.abort(), 5_000);
        try {
          const stream = application.reactive.watchTicketQueueWatch({ organization_id: organizationId, project_id: projectId }, undefined, abort.signal);
          const iterator = stream[Symbol.asyncIterator]();
          const result = await iterator.next();
          await iterator.return?.();
          if (result.done === true) throw new Error("TypeScript live query ended before its snapshot");
          await metrics.record("live_queries", tenant, 0, operationStarted);
        } finally {
          clearTimeout(timer);
        }
      }
      if (slot === 69) {
        operationStarted = performance.now();
        const ordinal = BigInt(Math.floor(counter / 100) % 4_096);
      const created = await application.generated.createTicket({
          title: `TypeScript cold ticket ${index}-${ordinal}`, status: "Open",
          ticket_id: id(namespace, 30_000n + BigInt(index) * 4_096n + ordinal), project_id: projectId,
          assignee_id: userId, reporter_id: userId,
          idempotency_key: `endurance-typescript-cold-${index}-${ordinal}`, organization_id: organizationId,
      });
      if (!created.replayed && created.outcome.outcome === "Created") metrics.eventEmitted();
      await metrics.record("workflows", tenant, 768, operationStarted);
      }
      await new Promise<void>((resolve) => setTimeout(resolve, delayMilliseconds));
    }
  } finally {
    await Promise.all([seeder.driver.shutdown(), application.driver.shutdown(), agent.driver.shutdown()]);
  }
}

async function seedClient(
  seeder: TicketDeskClient, application: TicketDeskClient, tenant: Tenant, index: number,
  organizationId: string, userId: string, projectId: string, ticketId: string,
): Promise<void> {
  let operationStarted = performance.now();
  await seeder.createOrganization({ name: `Endurance ${tenant}`, organization_id: organizationId, idempotency_key: `endurance-organization-${tenant}` });
  await metrics.record("writes", tenant, 512, operationStarted);
  operationStarted = performance.now();
  await seeder.createUser({ email: `typescript-${index}@${tenant}.example.test`, user_id: userId, display_name: `TypeScript endurance ${index}`, idempotency_key: `endurance-typescript-user-${index}`, organization_id: organizationId });
  await metrics.record("writes", tenant, 512, operationStarted);
  operationStarted = performance.now();
  await seeder.createProject({ name: `TypeScript endurance ${index}`, project_id: projectId, idempotency_key: `endurance-typescript-project-${index}`, organization_id: organizationId });
  await metrics.record("writes", tenant, 512, operationStarted);
  operationStarted = performance.now();
  const created = await application.createTicket({ title: `TypeScript hot ticket ${index}`, status: "Open", ticket_id: ticketId, project_id: projectId, assignee_id: userId, reporter_id: userId, idempotency_key: `endurance-typescript-hot-${configuration.seed}-${index}`, organization_id: organizationId });
  if (!created.replayed && created.outcome.outcome === "Created") metrics.eventEmitted();
  await metrics.record("writes", tenant, 768, operationStarted);
  await metrics.publish();
}

function id(namespace: bigint, value: bigint): string {
  const prefix = (namespace & 0xffff_ffffn).toString(16).padStart(8, "0");
  const suffix = (value & 0xffff_ffff_ffffn).toString(16).padStart(12, "0");
  return `${prefix}-0000-8000-8000-${suffix}`;
}
