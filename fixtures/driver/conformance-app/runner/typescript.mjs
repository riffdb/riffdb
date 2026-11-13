import { readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

const runtime = await import(pathToFileURL(process.env.RIFFDB_CONFORMANCE_TYPESCRIPT_RUNTIME));
const generated = await import(pathToFileURL(process.env.RIFFDB_CONFORMANCE_TYPESCRIPT_GENERATED));
const {
  DriverApplicationError,
  DriverApplicationTransport,
  DriverGeneratedApplicationTransport,
} = runtime;
const { DriverConformanceClient } = generated;

async function loadIdentity(filePath) {
  const doc = JSON.parse(await readFile(filePath, "utf8"));
  return { ...doc, contractVersion: BigInt(doc.contractVersion) };
}

const identity = await loadIdentity(process.env.RIFFDB_CONFORMANCE_IDENTITY);
const organizationId = "018f0f8b-7c6d-7e31-8a4f-000000000300";
const itemId = "018f0f8b-7c6d-7e31-8a4f-000000000301";

if (process.env.RIFFDB_CONFORMANCE_EXPECT_UNCERTAIN === "1") {
  const uncertainIdentity = await loadIdentity(process.env.RIFFDB_CONFORMANCE_UNCERTAIN_IDENTITY);
  const uncertainDriver = await DriverApplicationTransport.connect({
    socketPath: process.env.RIFFDB_CONFORMANCE_UNCERTAIN_SOCKET,
    identity: uncertainIdentity,
  });
  let isUncertain = false;
  const uncertainInput = {
    title: "Uncertain TypeScript item",
    token_digest: "ts-uncertain-secret",
    item_id: "018f0f8b-7c6d-7e31-8a4f-000000000399",
    idempotency_key: "driver-conformance-typescript-uncertain-v1",
    organization_id: organizationId,
  };
  try {
    const uncertainClient = new DriverConformanceClient(
      new DriverGeneratedApplicationTransport(uncertainDriver),
      1,
    );
    await uncertainClient.itemPage({ organization_id: organizationId, item_id: itemId });
    await writeFile(process.env.RIFFDB_CONFORMANCE_ARM_FILE, "arm\n");
    try {
      await uncertainClient.createItem(uncertainInput);
    } catch (error) {
      if (
        error instanceof DriverApplicationError
        && error.details.code === "RDB-UNCERTAIN-0101"
        && error.details.outcomeUncertain === true
      ) {
        isUncertain = true;
      }
    }
  } finally {
    await uncertainDriver.shutdown();
  }
  if (!isUncertain) {
    throw new Error("expected TypeScript RDB-UNCERTAIN-0101 outcomeUncertain error");
  }
  const recoveryDriver = await DriverApplicationTransport.connect({
    socketPath: process.env.RIFFDB_CONFORMANCE_SOCKET,
    identity,
  });
  try {
    const recoveryClient = new DriverConformanceClient(
      new DriverGeneratedApplicationTransport(recoveryDriver),
      3,
    );
    const recovered = await recoveryClient.createItem(uncertainInput);
    if (!recovered.replayed) {
      throw new Error("TypeScript uncertain recovery did not replay committed outcome");
    }
    console.log(JSON.stringify({
      schema: "riffdb.driver-conformance-fault/v1",
      fault: "outcome_unknown",
      uncertain: true,
      replayed: true,
    }));
  } finally {
    await recoveryDriver.shutdown();
  }
} else {
  const driver = await DriverApplicationTransport.connect({
    socketPath: process.env.RIFFDB_CONFORMANCE_SOCKET,
    identity,
  });
  try {
    const client = new DriverConformanceClient(new DriverGeneratedApplicationTransport(driver), 3);

    if (process.env.RIFFDB_CONFORMANCE_RESTART_SYNC) {
      const syncDir = process.env.RIFFDB_CONFORMANCE_RESTART_SYNC;
      const before = await client.itemPage({ organization_id: organizationId, item_id: itemId });
      if (before.value.outcome !== "Found") {
        throw new Error("TypeScript pre-restart itemPage did not find item");
      }
      await writeFile(path.join(syncDir, "typescript.ready"), "ready\n");
      await readFile(path.join(syncDir, "typescript.resume"), "utf8");
      const after = await client.itemPage({ organization_id: organizationId, item_id: itemId });
      if (after.value.outcome !== "Found") {
        throw new Error("TypeScript post-restart itemPage did not find item");
      }
      console.log(JSON.stringify({
        schema: "riffdb.driver-conformance-fault/v1",
        fault: "driver_restart_reconnect",
        reconnected: true,
      }));
    } else if (process.env.RIFFDB_CONFORMANCE_EXPECT_AUTH_DIAG === "1") {
      let code;
      let diagnostic;
      try {
        await client.itemsByIds({
          organization_id: organizationId,
          item_ids: [itemId, "018f0f8b-7c6d-7e31-8a4f-000000000351"],
        });
      } catch (error) {
        if (error instanceof DriverApplicationError) {
          code = error.details.code;
          diagnostic = error.details.authorizationDiagnostic;
        }
      }
      if (code !== "RDB-AUTH-0214" || diagnostic !== "scan_budget_exceeded") {
        throw new Error(`TypeScript auth diagnostic error lost semantic details: ${code} / ${diagnostic}`);
      }
      console.log(JSON.stringify({
        schema: "riffdb.driver-conformance-fault/v1",
        fault: "authorization_diagnostic",
        code,
        diagnostic,
      }));
    } else if (process.env.RIFFDB_CONFORMANCE_EXPECT_REVOKED === "1") {
      let code;
      try {
        await client.itemSecret({ organization_id: organizationId, item_id: itemId });
      } catch (error) {
        if (error instanceof DriverApplicationError) code = error.details.code;
      }
      if (code !== "RDB-AUTH-0215") {
        throw new Error("TypeScript revocation error lost semantic details");
      }
      console.log(JSON.stringify({
        schema: "riffdb.driver-conformance-fault/v1",
        fault: "revocation",
        code,
      }));
    } else {
      const idempotencyKey = "driver-conformance-typescript-create-v1";
      const tokenDigest = "typescript-secret-digest-must-not-log";
      const input = {
        title: "Shared remote TypeScript",
        token_digest: tokenDigest,
        item_id: itemId,
        idempotency_key: idempotencyKey,
        organization_id: organizationId,
      };
      const first = await client.createItem(input);
      if (first.replayed || first.commitSequence === undefined || first.outcome.outcome !== "Created") {
        throw new Error("first TypeScript command did not create the item");
      }
      const replay = await client.createItem(input);
      if (!replay.replayed) throw new Error("second TypeScript command did not replay");

      const extraItems = [
        ["018f0f8b-7c6d-7e31-8a4f-000000000351", "zeta-tag"],
        ["018f0f8b-7c6d-7e31-8a4f-000000000352", "Alpha-tag"],
        ["018f0f8b-7c6d-7e31-8a4f-000000000353", "alpha-tag"],
        ["018f0f8b-7c6d-7e31-8a4f-000000000354", "Zeta-tag"],
      ];
      let commitSeq = first.commitSequence;
      for (let idx = 0; idx < extraItems.length; idx += 1) {
        const [extraId, extraTitle] = extraItems[idx];
        const res = await client.createItem({
          title: extraTitle,
          token_digest: `typescript-extra-digest-${idx}`,
          item_id: extraId,
          idempotency_key: `driver-conformance-typescript-extra-${idx}`,
          organization_id: organizationId,
        });
        if (res.commitSequence === undefined) throw new Error("TypeScript extra item creation failed");
        commitSeq = res.commitSequence;
      }

      const tokenId = "018f0f8b-7c6d-7e31-8a4f-000000000331";
      const tokenValue = "typescript-one-time-secret-must-not-log";
      const issued = await client.issueToken({
        value: tokenValue,
        token_id: tokenId,
        expires_at: { seconds: 4102444800n, nanos: 0 },
        identifier: "typescript-loopback",
        request_id: "018f0f8b-7c6d-7e31-8a4f-000000000332",
        organization_id: organizationId,
      });
      if (issued.outcome.outcome !== "TokenIssued") {
        throw new Error("TypeScript token issue did not create the token");
      }
      const consumeInput = {
        token_id: tokenId,
        request_id: "018f0f8b-7c6d-7e31-8a4f-000000000333",
        organization_id: organizationId,
      };
      const consumed = await client.consumeToken(consumeInput);
      const redactedConsumed = generated.redactConsumeTokenOutcome(consumed.outcome);
      if (
        consumed.outcome.outcome !== "TokenConsumed"
        || consumed.outcome.value !== tokenValue
        || consumed.outcome.token_id !== tokenId
        || consumed.outcome.identifier !== "typescript-loopback"
        || JSON.stringify(redactedConsumed).includes(tokenValue)
      ) {
        throw new Error("TypeScript deleted preimage or redacted diagnostic was incorrect");
      }
      const consumedReplay = await client.consumeToken(consumeInput);
      if (
        !consumedReplay.replayed
        || consumedReplay.outcome.outcome !== "TokenConsumed"
        || consumedReplay.outcome.value !== tokenValue
      ) {
        throw new Error("TypeScript token consume did not replay the persisted preimage");
      }
      const missing = await client.consumeToken({
        token_id: tokenId,
        request_id: "018f0f8b-7c6d-7e31-8a4f-000000000334",
        organization_id: organizationId,
      });
      if (missing.outcome.outcome !== "TokenMissing") {
        throw new Error("TypeScript second consumer did not observe the atomic delete");
      }

      const page = await client.itemPage(
        { organization_id: organizationId, item_id: itemId },
        { readAfterCommit: commitSeq },
      );
      if (
        page.value.outcome !== "Found"
        || page.value.item.item_id !== itemId
        || page.value.item.title !== "Shared remote TypeScript"
      ) {
        throw new Error("TypeScript read-after-commit returned the wrong item");
      }
      const secret = await client.itemSecret(
        { organization_id: organizationId, item_id: itemId },
        { readAfterCommit: commitSeq },
      );
      const redacted = generated.redactItemSecretResult(secret.value);
      if (
        secret.value.outcome !== "Found"
        || secret.value.secret.token_digest !== tokenDigest
        || JSON.stringify(redacted).includes(tokenDigest)
      ) {
        throw new Error("TypeScript secret query value or redaction helper was incorrect");
      }

      let exact;
      for (let attempt = 0; attempt < 200; attempt += 1) {
        try {
          exact = await client.searchItems(
            { organization_id: organizationId, needle: "remote TypeScript", item_id: itemId, limit: 50, offset: 0n },
            { readAfterCommit: commitSeq },
          );
          break;
        } catch (error) {
          if (
            !(error instanceof DriverApplicationError)
            || (error.details.code !== "RDB-QUERY-0102" && error.details.code !== "RDB-PROJECTION-0103")
          ) {
            throw error;
          }
          await new Promise((resolve) => setTimeout(resolve, 10));
        }
      }
      if (exact === undefined) {
        throw new Error("TypeScript exact provider did not become ready within the retry bound");
      }
      if (
        exact.value.outcome !== "Found"
        || exact.value.total.value !== 1n
        || exact.value.items.length !== 1
        || exact.value.items[0].item_id !== itemId
        || exact.value.items[0].organization_id !== organizationId
      ) {
        throw new Error("TypeScript exact page and whole-population total diverged");
      }

      // Post-mutation invariant (`RDB-INPUT-0101`, `correct_request`) + non-terminal idempotency key.
      const accountId = "018f0f8b-7c6d-7e31-8a4f-000000000361";
      const opened = await client.openBudgetAccount({
        request_id: "ts-open-budget-v1",
        organization_id: organizationId,
        account_id: accountId,
        credit_limit: 500n,
        note: "initial",
      });
      if (opened.outcome.outcome !== "AccountOpened") {
        throw new Error("TypeScript openBudgetAccount failed");
      }
      let invariantError;
      let invariantRecovery;
      try {
        await client.recordSpend({
          request_id: "ts-over-budget-v1",
          organization_id: organizationId,
          account_id: accountId,
          amount: 600n,
        });
      } catch (error) {
        if (error instanceof DriverApplicationError) {
          invariantError = error.details.code;
          invariantRecovery = error.details.recoveryAction;
        }
      }
      if (invariantError !== "RDB-INPUT-0101" || invariantRecovery !== "correct_request") {
        throw new Error(`TypeScript invariant error lost semantic details: ${invariantError} / ${invariantRecovery}`);
      }
      const validSpend = await client.recordSpend({
        request_id: "ts-over-budget-v1",
        organization_id: organizationId,
        account_id: accountId,
        amount: 200n,
      });
      const invariantNonTerminal = (
        !validSpend.replayed
        && validSpend.outcome.outcome === "SpendRecorded"
        && validSpend.outcome.spent === 200n
      );
      if (!invariantNonTerminal) {
        throw new Error("TypeScript CommitCheckRejected persisted terminal idempotency state");
      }

      // Optimistic workflow revision update + stale-revision conflict.
      const revOk = await client.reviseBudgetNote({
        request_id: "ts-revise-1",
        organization_id: organizationId,
        account_id: accountId,
        expected_revision: 2n,
        note: "updated-note",
      });
      if (revOk.outcome.outcome !== "NoteRevised") {
        throw new Error("TypeScript reviseBudgetNote at revision 2 failed");
      }
      const revStale = await client.reviseBudgetNote({
        request_id: "ts-revise-stale",
        organization_id: organizationId,
        account_id: accountId,
        expected_revision: 2n,
        note: "stale-note",
      });
      if (revStale.outcome.outcome !== "StaleRevision") {
        throw new Error("TypeScript stale expected_revision did not return StaleRevision");
      }

      // `<query>All` PK-set chunking & ascending UUID order.
      const allByIds = await client.itemsByIdsAll(
        {
          organization_id: organizationId,
          item_ids: [
            "018f0f8b-7c6d-7e31-8a4f-000000000354",
            itemId,
            "018f0f8b-7c6d-7e31-8a4f-000000000352",
            "018f0f8b-7c6d-7e31-8a4f-000000000354",
            "018f0f8b-7c6d-7e31-8a4f-000000000351",
            "018f0f8b-7c6d-7e31-8a4f-000000000353",
          ],
        },
        16,
        { readAfterCommit: commitSeq },
      );
      let pkSetAllOrdered = true;
      for (let i = 1; i < allByIds.value.items.length; i += 1) {
        if (!(allByIds.value.items[i - 1].item_id < allByIds.value.items[i].item_id)) {
          pkSetAllOrdered = false;
        }
      }
      if (allByIds.value.items.length !== 5 || !pkSetAllOrdered) {
        throw new Error("TypeScript itemsByIdsAll count or ordering failed");
      }

      // `<query>All` `binary_utf8_v1` collation ordering.
      const allByTitles = await client.itemsByTitlesAll(
        {
          organization_id: organizationId,
          titles: ["zeta-tag", "Alpha-tag", "alpha-tag", "Zeta-tag", "Alpha-tag"],
        },
        16,
        { readAfterCommit: commitSeq },
      );
      const collationAllTitles = allByTitles.value.items.map((row) => row.title);
      if (
        collationAllTitles.length !== 4
        || collationAllTitles[0] !== "Alpha-tag"
        || collationAllTitles[1] !== "Zeta-tag"
        || collationAllTitles[2] !== "alpha-tag"
        || collationAllTitles[3] !== "zeta-tag"
      ) {
        throw new Error(`TypeScript itemsByTitlesAll collation order diverged: ${JSON.stringify(collationAllTitles)}`);
      }

      // Wide-row + sibling-binding cursor continuation across >= 2 pages.
      const wideText = "T".repeat(380_000);
      const wideBlob = new Uint8Array(520_000).fill(0x54);
      let wideCommit;
      for (let seq = 1n; seq <= 6n; seq += 1n) {
        const written = await client.writeWideNote({
          request_id: `ts-wide-note-${seq}`,
          organization_id: organizationId,
          note_seq: seq,
          note_text: wideText,
          note_blob: wideBlob,
        });
        if (written.commitSequence === undefined) {
          throw new Error("TypeScript writeWideNote failed");
        }
        wideCommit = written.commitSequence;
      }
      let widePageCount = 0;
      const observedSeqs = [];
      for await (const widePage of client.wideNotesPagePages(
        { organization_id: organizationId, anchor_item_id: itemId, limit: 10 },
        16,
        { readAfterCommit: wideCommit },
      )) {
        widePageCount += 1;
        if (widePage.value.outcome !== "Found" || widePage.value.anchor.item_id !== itemId) {
          throw new Error("TypeScript wideNotesPagePages sibling anchor mismatched");
        }
        for (const note of widePage.value.notes) {
          if (note.note_text.length !== 380_000 || note.note_blob.byteLength !== 520_000) {
            throw new Error("TypeScript wideNotesPagePages payload truncated");
          }
          observedSeqs.push(Number(note.note_seq));
        }
      }
      const wideCursorPagesMinTwo = widePageCount >= 2;
      if (!wideCursorPagesMinTwo || observedSeqs.length !== 6) {
        throw new Error(`TypeScript wideNotesPagePages pagination failed: pages=${widePageCount} total=${observedSeqs.length}`);
      }
      for (let i = 0; i < observedSeqs.length; i += 1) {
        if (observedSeqs[i] !== i + 1) {
          throw new Error(`TypeScript wideNotesPagePages sequence mismatch: ${JSON.stringify(observedSeqs)}`);
        }
      }

      let reuseError;
      try {
        await client.createItem({ ...input, title: "Changed input" });
      } catch (error) {
        if (error instanceof DriverApplicationError) reuseError = error.details.code;
      }
      if (reuseError !== "RDB-COMMAND-0101") {
        throw new Error("TypeScript reuse error lost semantic details");
      }
      console.log(JSON.stringify({
        schema: "riffdb.driver-conformance-observation/v1",
        language: "typescript",
        created: "Created",
        replayed: true,
        query: "Found",
        read_after_commit: true,
        reuse_error: reuseError,
        secret_query: "Found",
        secret_redacted: true,
        exact_query: "Found",
        exact_total: 1,
        consume: "TokenConsumed",
        consume_replayed: true,
        consume_missing: true,
        delete_preimage_redacted: true,
        invariant_error: invariantError,
        invariant_recovery: invariantRecovery,
        invariant_non_terminal: invariantNonTerminal,
        revision_updated: "NoteRevised",
        revision_stale: "StaleRevision",
        pk_set_all_count: allByIds.value.items.length,
        pk_set_all_ordered: pkSetAllOrdered,
        collation_all_titles: collationAllTitles,
        wide_cursor_pages_min_two: wideCursorPagesMinTwo,
        wide_cursor_total: observedSeqs.length,
      }));
    }
  } finally {
    await driver.shutdown();
  }
}
