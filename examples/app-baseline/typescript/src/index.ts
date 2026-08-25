/** CLI: node dist/index.js --backend postgres|riffdb|both ... */

import { mkdir, writeFile } from "node:fs/promises";
import { dirname } from "node:path";
import { fileURLToPath } from "node:url";

import { LoadReport, SWEEP_CLIENTS, printLoadSummary, runClosedLoop, type LoadConfig } from "./load.js";
import { PostgresDriver } from "./postgres.js";
import { RiffDbDriver, readDriverIdentity } from "./riffdb.js";
import { OBLIGATIONS } from "./schema.js";
import { fullScale, generateSeed, smokeScale, type Scale } from "./seed.js";

function argValue(argv: string[], name: string): string | undefined {
  const index = argv.indexOf(name);
  if (index === -1) return undefined;
  return argv[index + 1];
}

function hasFlag(argv: string[], name: string): boolean {
  return argv.includes(name);
}

export async function main(argv = process.argv.slice(2)): Promise<number> {
  const backend = argValue(argv, "--backend") ?? "both";
  if (backend !== "postgres" && backend !== "riffdb" && backend !== "both") {
    console.error("backend must be postgres|riffdb|both");
    return 2;
  }
  const scale: Scale = hasFlag(argv, "--full") ? fullScale() : smokeScale();
  const dataset = generateSeed(scale);
  const sweep = hasFlag(argv, "--load-concurrency-sweep");
  const clients = sweep
    ? [...SWEEP_CLIENTS]
    : [Math.max(1, Number(argValue(argv, "--load-clients") ?? 8))];
  const durationS = Number(argValue(argv, "--load-duration-secs") ?? 5);
  const warmupS = Number(argValue(argv, "--load-warmup-secs") ?? 1);
  const zipfS = Number(argValue(argv, "--load-zipf-s") ?? 1);
  const drivers: Array<PostgresDriver | RiffDbDriver> = [];
  if (backend === "postgres" || backend === "both") {
    const url = argValue(argv, "--postgres-url") ?? process.env.RIFFDB_APP_BASELINE_POSTGRES_URL;
    if (!url) {
      console.error("missing --postgres-url or RIFFDB_APP_BASELINE_POSTGRES_URL");
      return 2;
    }
    drivers.push(new PostgresDriver(url));
  }
  if (backend === "riffdb" || backend === "both") {
    const identityPath =
      argValue(argv, "--riffdb-identity") ?? process.env.RIFFDB_TYPESCRIPT_DRIVER_IDENTITY;
    if (!identityPath) {
      console.error("missing --riffdb-identity or RIFFDB_TYPESCRIPT_DRIVER_IDENTITY");
      return 2;
    }
    drivers.push(new RiffDbDriver(await readDriverIdentity(identityPath)));
  }

  const allReports: LoadReport[] = [];
  for (const driver of drivers) {
    console.log(`=== backend ${driver.backendId} seed ===`);
    const seedStarted = Number(process.hrtime.bigint());
    await driver.seed(dataset);
    const seedNs = Number(process.hrtime.bigint()) - seedStarted;
    console.log(`seed_ns=${seedNs}`);
    let sampleBase = 0;
    for (const clientCount of clients) {
      const config: LoadConfig = {
        clients: clientCount,
        durationS,
        warmupS,
        zipfS,
        rngSeed: 0x000a11cebeef,
        tenantCount: 1,
        sampleIdBase: sampleBase,
      };
      const report = await runClosedLoop(driver, dataset, config, seedNs);
      printLoadSummary(report);
      allReports.push(report);
      sampleBase += 1_000_000_000;
    }
  }

  console.log("\n== comparison curve ==");
  console.log(
    `${"backend".padEnd(24)} ${"clients".padStart(8)} ${"ops/s".padStart(12)} ${"p50_ms".padStart(10)} ${"p99_ms".padStart(10)} ${"write_p50_ms".padStart(12)}`,
  );
  for (const report of allReports) {
    const write = report.byOp.create_comment;
    const writeP50 =
      write !== undefined && write.total() > 0 ? write.latency.percentileNs(50) / 1e6 : 0;
    console.log(
      `${report.backendId.padEnd(24)} ${String(report.clients).padStart(8)} ${report.throughput().toFixed(0).padStart(12)} ` +
        `${(report.aggregate.latency.percentileNs(50) / 1e6).toFixed(3).padStart(10)} ` +
        `${(report.aggregate.latency.percentileNs(99) / 1e6).toFixed(3).padStart(10)} ` +
        `${writeP50.toFixed(3).padStart(12)}`,
    );
  }

  const payload = {
    schema: "riffdb.app-baseline-typescript-safe-app/v1",
    evidentiary: false,
    language: "typescript",
    postgres_obligations: [...OBLIGATIONS],
    scale: {
      organizations: scale.organizations,
      users_per_org: scale.usersPerOrg,
      projects_per_org: scale.projectsPerOrg,
      tickets_per_project: scale.ticketsPerProject,
      board_dense_open: scale.boardDenseOpen,
    },
    curve: allReports.map((report) => report.json()),
    notes: [
      "Same TypeScript interactive mix against postgres_safe_app and generated RiffDB client.",
      "Postgres implements authorization/idempotency/audit/event/outbox in TypeScript SQL.",
      "RiffDB uses the generated TicketDesk client with no TypeScript safety code; riffdbd (Rust) enforces those.",
      "Not a substitute for the Rust evidentiary harness in benchmarks/run-app-baseline.",
    ],
  };
  const output = argValue(argv, "--output");
  if (output !== undefined) {
    await mkdir(dirname(output), { recursive: true });
    await writeFile(output, `${JSON.stringify(payload, null, 2)}\n`);
    console.log(`wrote ${output}`);
  }
  return 0;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  void main().then((code) => process.exit(code));
}
