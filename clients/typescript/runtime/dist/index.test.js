import assert from "node:assert/strict";
import test from "node:test";
import { CliApplicationTransport } from "./index.js";
test("transport configuration is bounded before any process is started", () => {
    assert.throws(() => new CliApplicationTransport({ riffdbPath: "", endpoint: "http://127.0.0.1:1", credentialFile: "credential" }), /invalid RiffDB application transport configuration/);
});
