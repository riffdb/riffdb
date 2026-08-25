/** Deterministic UUIDv7-shaped identifiers matching the Rust seed. */

export type UuidBytes = Uint8Array;

export const NS_ORG = 0x10;
export const NS_USER = 0x11;
export const NS_PROJECT = 0x12;
export const NS_TICKET = 0x13;
export const NS_COMMENT = 0x14;
export const NS_LABEL = 0x15;
export const NS_WRITE_PROBE = 0x7f;
export const NS_LOAD_WRITE = 0x7e;

export const STATUS_OPEN = "open";
export const STATUS_CLOSED = "closed";
export const STATUS_IN_PROGRESS = "in_progress";

export function uuidFromOrdinal(namespace: number, ordinal: number): UuidBytes {
  const data = new Uint8Array(16).fill(namespace & 0xff);
  data[6] = 0x70 | (namespace & 0x0f);
  const view = new DataView(data.buffer);
  view.setBigUint64(8, BigInt(ordinal) & 0xffffffffffffffffn);
  data[8] = 0x80 | (data[8]! & 0x3f);
  return data;
}

export function formatUuid(value: UuidBytes): string {
  const hexed = Buffer.from(value).toString("hex");
  return `${hexed.slice(0, 8)}-${hexed.slice(8, 12)}-${hexed.slice(12, 16)}-${hexed.slice(16, 20)}-${hexed.slice(20, 32)}`;
}

export function encodeShort(value: UuidBytes): string {
  return Buffer.from(value.subarray(12, 16)).toString("hex");
}

export function sqlStatusToRiff(status: string): string {
  const mapped = { open: "Open", closed: "Closed", in_progress: "InProgress" }[status];
  if (mapped === undefined) throw new Error(`unknown ticket status ${status}`);
  return mapped;
}

export function uuidEquals(left: UuidBytes, right: UuidBytes): boolean {
  if (left.length !== right.length) return false;
  for (let index = 0; index < left.length; index += 1) {
    if (left[index] !== right[index]) return false;
  }
  return true;
}
