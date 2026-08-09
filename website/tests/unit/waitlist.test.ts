import { describe, expect, it, vi } from "vitest";
import { handleWaitlist } from "../../functions/api/waitlist";

const env = {
  LOOPS_FORM_ENDPOINT: "https://app.loops.so/api/newsletter-form/riffdb-test",
  TURNSTILE_SECRET_KEY: "turnstile-secret",
};

function request(
  body: Record<string, unknown>,
  init: RequestInit = {},
): Request {
  return new Request("https://riffdb.com/api/waitlist", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Origin: "https://riffdb.com",
      ...init.headers,
    },
    body: JSON.stringify(body),
    ...init,
  });
}

function validBody(
  overrides: Record<string, unknown> = {},
): Record<string, unknown> {
  return {
    email: "Developer@Example.com",
    turnstileToken: "verified-token",
    website: "",
    ...overrides,
  };
}

function successfulFetch() {
  return vi.fn(async (input: RequestInfo | URL, _init?: RequestInit) => {
    const url = input.toString();
    if (url.includes("siteverify")) {
      return Response.json({ success: true, action: "waitlist" });
    }
    return Response.json({ success: true });
  });
}

async function outcome(
  response: Response,
): Promise<{ ok: boolean; code: string; message: string }> {
  return (await response.json()) as {
    ok: boolean;
    code: string;
    message: string;
  };
}

describe("waitlist endpoint", () => {
  it("verifies the token and forwards only normalized email and source", async () => {
    const fetcher = successfulFetch();
    const response = await handleWaitlist(request(validBody()), env, fetcher);

    expect(response.status).toBe(202);
    expect(await outcome(response)).toMatchObject({
      ok: true,
      code: "accepted",
    });
    expect(fetcher).toHaveBeenCalledTimes(2);
    const providerCall = fetcher.mock.calls[1];
    expect(providerCall[0].toString()).toBe(env.LOOPS_FORM_ENDPOINT);
    expect(providerCall[1]?.body).toBe(
      "email=developer%40example.com&source=riffdb.com",
    );
    expect(providerCall[1]?.body).not.toContain("verified-token");
  });

  it("requires POST", async () => {
    const response = await handleWaitlist(
      new Request("https://riffdb.com/api/waitlist", { method: "GET" }),
      env,
      successfulFetch(),
    );
    expect(response.status).toBe(405);
    expect(response.headers.get("Allow")).toBe("POST");
  });

  it("requires a same-origin browser request", async () => {
    for (const origin of [undefined, "https://attacker.example"]) {
      const headers: Record<string, string> = {
        "Content-Type": "application/json",
      };
      if (origin) headers.Origin = origin;
      const response = await handleWaitlist(
        request(validBody(), { headers }),
        env,
        successfulFetch(),
      );
      expect(response.status).toBe(403);
      expect((await outcome(response)).code).toBe("verification_failed");
    }
  });

  it("rejects malformed, unexpected, and oversized input", async () => {
    const malformed = new Request("https://riffdb.com/api/waitlist", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Origin: "https://riffdb.com",
      },
      body: "{",
    });
    const unexpected = request(validBody({ role: "admin" }));
    const oversized = request(
      validBody({ email: `${"a".repeat(4_100)}@example.com` }),
    );

    for (const candidate of [malformed, unexpected, oversized]) {
      const response = await handleWaitlist(candidate, env, successfulFetch());
      expect(response.status).toBe(400);
      expect((await outcome(response)).code).toBe("invalid_request");
    }
  });

  it("rejects invalid email without calling external services", async () => {
    const fetcher = successfulFetch();
    const response = await handleWaitlist(
      request(validBody({ email: "not-an-email" })),
      env,
      fetcher,
    );
    expect(response.status).toBe(400);
    expect((await outcome(response)).code).toBe("invalid_email");
    expect(fetcher).not.toHaveBeenCalled();
  });

  it("absorbs honeypot submissions without forwarding them", async () => {
    const fetcher = successfulFetch();
    const response = await handleWaitlist(
      request(validBody({ website: "bot.example" })),
      env,
      fetcher,
    );
    expect(response.status).toBe(202);
    expect((await outcome(response)).code).toBe("accepted");
    expect(fetcher).not.toHaveBeenCalled();
  });

  it("rejects failed, malformed, and wrong-action Turnstile results generically", async () => {
    const results = [
      Response.json({ success: false, action: "waitlist" }),
      new Response("not json"),
      Response.json({ success: true, action: "login" }),
    ];

    for (const turnstileResponse of results) {
      const fetcher = vi.fn(async () => turnstileResponse.clone());
      const response = await handleWaitlist(request(validBody()), env, fetcher);
      expect(response.status).toBe(400);
      expect((await outcome(response)).code).toBe("verification_failed");
      expect(fetcher).toHaveBeenCalledTimes(1);
    }
  });

  it("maps provider failure and rate limiting to one non-enumerating outcome", async () => {
    for (const providerResponse of [
      Response.json({ success: false }, { status: 400 }),
      new Response("", { status: 429 }),
    ]) {
      const fetcher = vi
        .fn()
        .mockResolvedValueOnce(
          Response.json({ success: true, action: "waitlist" }),
        )
        .mockResolvedValueOnce(providerResponse.clone());
      const response = await handleWaitlist(request(validBody()), env, fetcher);
      expect(response.status).toBe(503);
      expect(await outcome(response)).toEqual({
        ok: false,
        code: "temporarily_unavailable",
        message: "Signup is temporarily unavailable. Please try again shortly.",
      });
    }
  });

  it("fails closed on missing or unsafe runtime configuration", async () => {
    for (const candidate of [
      { ...env, TURNSTILE_SECRET_KEY: "" },
      { ...env, LOOPS_FORM_ENDPOINT: "https://attacker.example/collect" },
      {
        ...env,
        LOOPS_FORM_ENDPOINT: "http://app.loops.so/api/newsletter-form/test",
      },
      {
        ...env,
        LOOPS_FORM_ENDPOINT: "https://app.loops.so/api/newsletter-form/",
      },
    ]) {
      const response = await handleWaitlist(
        request(validBody()),
        candidate,
        successfulFetch(),
      );
      expect(response.status).toBe(503);
      expect((await outcome(response)).code).toBe("temporarily_unavailable");
    }
  });

  it("does not expose provider details when fetch throws", async () => {
    const response = await handleWaitlist(
      request(validBody()),
      env,
      vi.fn(async () => {
        throw new Error("secret provider failure");
      }),
    );
    const body = await outcome(response);
    expect(response.status).toBe(503);
    expect(JSON.stringify(body)).not.toContain("secret provider failure");
  });
});
