import { readFile } from "node:fs/promises";

function classify(target, encodedBytes, maximum) {
  if (encodedBytes <= maximum) return "none";
  return target === "encoded_request_bytes" ? "request_too_large" : "response_too_large";
}

const corpus = JSON.parse(await readFile(process.argv[2], "utf8"));
const observations = corpus.operations.flatMap((operation) =>
  operation.bounds.flatMap((bound) =>
    bound.cases.map((entry) => ({
      tag: operation.tag,
      target: bound.target,
      encoded_bytes: entry.encoded_bytes,
      error_class: classify(bound.target, entry.encoded_bytes, bound.maximum),
    })),
  ),
);
process.stdout.write(`${JSON.stringify(observations)}\n`);
