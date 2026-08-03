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

const queueFrames = await collect(createTicketQueueWatchSseRelay(async () => true, snapshotOnly(queue)));
const pageFrames = await collect(createTicketPageWatchSseRelay(async () => true, snapshotOnly(page)));
assert(queueFrames === 1 && pageFrames === 1, "generated SSE relays did not emit one authorized frame");

let denied = false;
try {
  await collect(createTicketQueueWatchSseRelay(async () => false, snapshotOnly(queue)));
} catch {
  denied = true;
}
assert(denied, "SSE relay did not reauthorize before delivery");

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

function snapshot<T>(value: T): LiveQueryUpdate<T> {
  return { type: "snapshot", value, cursor: "AQIDBA==", applicationHead: 1n, historyIncarnation: 1n };
}

function terminal<T>(): LiveQueryUpdate<T> {
  return { type: "terminal", reason: "authorization_changed" };
}

async function collect(values: AsyncIterable<string>): Promise<number> {
  let count = 0;
  for await (const value of values) {
    assert(value.startsWith("event: snapshot\ndata: "), "invalid SSE frame");
    count += 1;
  }
  return count;
}

function assert(condition: boolean, message: string): asserts condition {
  if (!condition) throw new Error(message);
}
