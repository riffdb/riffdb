import {
  createTicketPageWatchSseRelay,
  createTicketPageWatchStore,
  createTicketQueueWatchSseRelay,
  createTicketQueueWatchStore,
  type LiveQueryUpdate,
  type TicketPageResult,
  type TicketQueueResult,
} from "./generated/client.js";

const queue = {
  outcome: "Found",
  tickets: [],
} satisfies TicketQueueResult;
const page = { outcome: "NotFound" } satisfies TicketPageResult;

const queueStoreA = createTicketQueueWatchStore();
const queueStoreB = createTicketQueueWatchStore();
const queueObservedA: Array<TicketQueueResult | undefined> = [];
const queueObservedB: Array<TicketQueueResult | undefined> = [];
queueStoreA.subscribe((value) => queueObservedA.push(value));
queueStoreB.subscribe((value) => queueObservedB.push(value));
await Promise.all([
  queueStoreA.connect(queueUpdates(queue)),
  queueStoreB.connect(queueUpdates(queue)),
]);
assert(queueObservedA.length === 2 && queueObservedB.length === 2, "two queue browsers did not converge and clear");
assert(queueObservedA[0]?.outcome === "Found" && queueObservedA[1] === undefined, "queue browser retained revoked data");
assert(queueObservedB[0]?.outcome === "Found" && queueObservedB[1] === undefined, "second queue browser retained revoked data");

const pageStore = createTicketPageWatchStore();
const pageObserved: Array<TicketPageResult | undefined> = [];
pageStore.subscribe((value) => pageObserved.push(value));
await pageStore.connect(pageUpdates(page));
assert(pageObserved[0]?.outcome === "NotFound" && pageObserved[1] === undefined, "detail browser retained revoked data");

const failedStore = createTicketQueueWatchStore();
const failedObserved: Array<TicketQueueResult | undefined> = [];
failedStore.subscribe((value) => failedObserved.push(value));
let failedWatch = false;
try {
  await failedStore.connect(snapshotThenFail(queue));
} catch {
  failedWatch = true;
}
assert(failedWatch, "failed watch did not surface its transport failure");
assert(
  failedObserved[0]?.outcome === "Found" && failedObserved[1] === undefined,
  "failed watch retained protected state",
);

const queueFrames = await collect(createTicketQueueWatchSseRelay(async () => true, snapshotOnly(queue)));
const pageFrames = await collect(createTicketPageWatchSseRelay(async () => true, snapshotOnly(page)));
assert(
  queueFrames.length === 2 && pageFrames.length === 2,
  "generated SSE relays did not terminate a completed upstream watch",
);
assert(queueFrames[0]?.startsWith("event: snapshot\ndata: ") === true, "queue snapshot frame missing");
assert(pageFrames[0]?.startsWith("event: snapshot\ndata: ") === true, "page snapshot frame missing");
assert(
  queueFrames[1] === 'event: terminal\ndata: {"type":"terminal","reason":"service_unavailable"}\n\n',
  "completed upstream watch did not clear queue state",
);

const deniedFrames = await collect(
  createTicketQueueWatchSseRelay(async () => false, snapshotOnly(queue)),
);
assert(
  deniedFrames.length === 1
    && deniedFrames[0] === 'event: terminal\ndata: {"type":"terminal","reason":"authorization_changed"}\n\n',
  "SSE relay did not clear state when application authorization ended",
);

process.stdout.write("TicketDesk generated two-browser relay acceptance passed.\n");

async function* queueUpdates(value: TicketQueueResult): AsyncIterable<LiveQueryUpdate<TicketQueueResult>> {
  yield snapshot(value);
  yield terminal();
}

async function* pageUpdates(value: TicketPageResult): AsyncIterable<LiveQueryUpdate<TicketPageResult>> {
  yield snapshot(value);
  yield terminal();
}

async function* snapshotOnly<T>(value: T): AsyncIterable<LiveQueryUpdate<T>> {
  yield snapshot(value);
}

async function* snapshotThenFail<T>(value: T): AsyncIterable<LiveQueryUpdate<T>> {
  yield snapshot(value);
  throw new Error("injected watch failure");
}

function snapshot<T>(value: T): LiveQueryUpdate<T> {
  return { type: "snapshot", value, cursor: "AQIDBA==", applicationHead: 1n, historyIncarnation: 1n };
}

function terminal<T>(): LiveQueryUpdate<T> {
  return { type: "terminal", reason: "authorization_changed" };
}

async function collect(values: AsyncIterable<string>): Promise<string[]> {
  const frames: string[] = [];
  for await (const value of values) {
    frames.push(value);
  }
  return frames;
}

function assert(condition: boolean, message: string): asserts condition {
  if (!condition) throw new Error(message);
}
