interface Env {
  LOOPS_FORM_ENDPOINT: string;
  TURNSTILE_SECRET_KEY: string;
}

interface WaitlistBody {
  email: string;
  turnstileToken: string;
  website: string;
}

interface TurnstileResult {
  success?: boolean;
  action?: string;
}

type Fetcher = (
  input: RequestInfo | URL,
  init?: RequestInit,
) => Promise<Response>;

const MAX_REQUEST_BYTES = 4_096;
const MAX_PROVIDER_RESPONSE_BYTES = 4_096;
const MAX_EMAIL_BYTES = 254;
const MAX_TOKEN_BYTES = 2_048;
const TURNSTILE_VERIFY_URL =
  "https://challenges.cloudflare.com/turnstile/v0/siteverify";
const ALLOWED_KEYS = new Set(["email", "turnstileToken", "website"]);

const messages = {
  accepted: "Check your inbox to confirm your place on the early-access list.",
  invalid: "Enter a valid email address and try again.",
  verification: "Verification failed. Refresh the page and try again.",
  unavailable: "Signup is temporarily unavailable. Please try again shortly.",
} as const;

function json(
  status: number,
  body: { ok: boolean; code: string; message: string },
): Response {
  return Response.json(body, {
    status,
    headers: {
      "Cache-Control": "no-store",
      "Content-Type": "application/json; charset=utf-8",
      "X-Content-Type-Options": "nosniff",
    },
  });
}

function byteLength(value: string): number {
  return new TextEncoder().encode(value).byteLength;
}

function isEmail(value: string): boolean {
  return (
    byteLength(value) <= MAX_EMAIL_BYTES &&
    /^[^\s@]+@[^\s@]+\.[^\s@]+$/u.test(value) &&
    !value.includes("..")
  );
}

function parseBody(text: string): WaitlistBody | null {
  if (byteLength(text) > MAX_REQUEST_BYTES) return null;

  let input: unknown;
  try {
    input = JSON.parse(text);
  } catch {
    return null;
  }

  if (typeof input !== "object" || input === null || Array.isArray(input))
    return null;
  const record = input as Record<string, unknown>;
  if (Object.keys(record).some((key) => !ALLOWED_KEYS.has(key))) return null;
  if (
    typeof record.email !== "string" ||
    typeof record.turnstileToken !== "string" ||
    typeof record.website !== "string"
  ) {
    return null;
  }

  return {
    email: record.email.trim().toLowerCase(),
    turnstileToken: record.turnstileToken,
    website: record.website,
  };
}

function validLoopsEndpoint(value: string): URL | null {
  try {
    const url = new URL(value);
    const expectedPrefix = "/api/newsletter-form/";
    if (
      url.protocol !== "https:" ||
      url.hostname !== "app.loops.so" ||
      !url.pathname.startsWith(expectedPrefix) ||
      url.pathname.length <= expectedPrefix.length ||
      url.username ||
      url.password ||
      url.search ||
      url.hash
    ) {
      return null;
    }
    return url;
  } catch {
    return null;
  }
}

function sameOrigin(request: Request): boolean {
  const origin = request.headers.get("Origin");
  if (!origin) return false;
  try {
    return new URL(origin).origin === new URL(request.url).origin;
  } catch {
    return false;
  }
}

async function parseBoundedJson<T>(response: Response): Promise<T | null> {
  const text = await response.text();
  if (byteLength(text) > MAX_PROVIDER_RESPONSE_BYTES) return null;
  try {
    return JSON.parse(text) as T;
  } catch {
    return null;
  }
}

export async function handleWaitlist(
  request: Request,
  env: Env,
  fetcher: Fetcher = fetch,
): Promise<Response> {
  if (request.method !== "POST") {
    return new Response("Method not allowed", {
      status: 405,
      headers: { Allow: "POST", "Cache-Control": "no-store" },
    });
  }

  if (!sameOrigin(request)) {
    return json(403, {
      ok: false,
      code: "verification_failed",
      message: messages.verification,
    });
  }

  const contentType = request.headers
    .get("Content-Type")
    ?.split(";", 1)[0]
    .trim();
  const declaredLength = Number(request.headers.get("Content-Length") ?? "0");
  if (
    contentType !== "application/json" ||
    !Number.isFinite(declaredLength) ||
    declaredLength > MAX_REQUEST_BYTES
  ) {
    return json(400, {
      ok: false,
      code: "invalid_request",
      message: messages.invalid,
    });
  }

  const body = parseBody(await request.text());
  if (!body) {
    return json(400, {
      ok: false,
      code: "invalid_request",
      message: messages.invalid,
    });
  }

  // Give automated honeypot submissions the same apparent success without forwarding data.
  if (body.website.length > 0) {
    return json(202, {
      ok: true,
      code: "accepted",
      message: messages.accepted,
    });
  }

  if (!isEmail(body.email)) {
    return json(400, {
      ok: false,
      code: "invalid_email",
      message: messages.invalid,
    });
  }
  if (
    body.turnstileToken.length === 0 ||
    byteLength(body.turnstileToken) > MAX_TOKEN_BYTES
  ) {
    return json(400, {
      ok: false,
      code: "verification_failed",
      message: messages.verification,
    });
  }

  const loopsEndpoint = validLoopsEndpoint(env.LOOPS_FORM_ENDPOINT);
  if (!loopsEndpoint || !env.TURNSTILE_SECRET_KEY) {
    return json(503, {
      ok: false,
      code: "temporarily_unavailable",
      message: messages.unavailable,
    });
  }

  try {
    const verificationBody = new FormData();
    verificationBody.set("secret", env.TURNSTILE_SECRET_KEY);
    verificationBody.set("response", body.turnstileToken);
    const verification = await fetcher(TURNSTILE_VERIFY_URL, {
      method: "POST",
      body: verificationBody,
    });
    const verificationResult =
      await parseBoundedJson<TurnstileResult>(verification);

    if (
      !verification.ok ||
      verificationResult?.success !== true ||
      verificationResult.action !== "waitlist"
    ) {
      return json(400, {
        ok: false,
        code: "verification_failed",
        message: messages.verification,
      });
    }

    const providerBody = new URLSearchParams({
      email: body.email,
      source: "riffdb.com",
    });
    const provider = await fetcher(loopsEndpoint, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body: providerBody.toString(),
    });
    const providerResult = await parseBoundedJson<{ success?: boolean }>(
      provider,
    );

    if (!provider.ok || providerResult?.success !== true) {
      return json(503, {
        ok: false,
        code: "temporarily_unavailable",
        message: messages.unavailable,
      });
    }

    return json(202, {
      ok: true,
      code: "accepted",
      message: messages.accepted,
    });
  } catch {
    return json(503, {
      ok: false,
      code: "temporarily_unavailable",
      message: messages.unavailable,
    });
  }
}

export const onRequest: PagesFunction<Env> = ({ request, env }) =>
  handleWaitlist(request, env);
