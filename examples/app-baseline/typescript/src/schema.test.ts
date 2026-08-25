import assert from "node:assert/strict";
import test from "node:test";

import { LatencyHistogram } from "./histogram.js";

test("percentiles track injected latencies", () => {
  const histogram = new LatencyHistogram();
  for (let index = 0; index < 90; index += 1) histogram.recordNs(100_000);
  for (let index = 0; index < 9; index += 1) histogram.recordNs(1_000_000);
  histogram.recordNs(10_000_000);
  assert.ok(histogram.percentileNs(50) >= 100_000);
  assert.ok(histogram.percentileNs(50) < 1_000_000);
  assert.ok(histogram.percentileNs(99) >= 1_000_000);
});
