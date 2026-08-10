import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";

test.beforeEach(async ({ page }) => {
  await page.route("https://challenges.cloudflare.com/**", (route) =>
    route.abort(),
  );
});

for (const path of ["/", "/privacy/", "/404.html"]) {
  test(`${path} has no automatically detectable accessibility violations`, async ({
    page,
  }) => {
    await page.goto(path);
    const results = await new AxeBuilder({ page }).analyze();
    expect(results.violations).toEqual([]);
  });
}

test("semantic path remains accessible in dark mode", async ({ page }) => {
  await page.emulateMedia({ colorScheme: "dark" });
  await page.goto("/");
  const results = await new AxeBuilder({ page })
    .include(".path-card")
    .analyze();
  expect(results.violations).toEqual([]);
});

test("home presents the product thesis and honest POC boundary", async ({
  page,
}) => {
  await page.goto("/");
  const mark = page.locator("[data-riffdb-mark]").first();
  await expect(mark).toHaveAttribute("src", /^data:image\/svg\+xml/);
  await expect(page.locator('link[rel="icon"]')).toHaveAttribute(
    "href",
    (await mark.getAttribute("src")) ?? "missing-mark",
  );
  await expect(page.getByRole("heading", { level: 1 })).toContainText(
    "Vibe fast",
  );
  await expect(
    page.getByRole("heading", {
      name: "Agents make assembly cheap. They do not make seams correct.",
    }),
  ).toBeVisible();
  await expect(
    page.getByText("Beyond the current POC, the product direction brings"),
  ).toBeVisible();
  await expect(
    page.getByText("standalone, local-only Rust proof of concept"),
  ).toBeVisible();
  await expect(
    page.getByRole("link", { name: "Explore on GitHub" }),
  ).toHaveAttribute("href", "https://github.com/riffdb/riffdb");
  await expect(
    page.getByRole("link", { name: "Browse the Rust API" }),
  ).toHaveAttribute(
    "href",
    "https://docs.riffdb.com/api/rust/riffdb_client_rust/index.html",
  );
  await expect(
    page.getByRole("link", { name: "Why RiffDB exists" }),
  ).toHaveAttribute("href", "https://docs.riffdb.com/VISION.html");
  await expect(page.locator("main")).not.toContainText("—");
  await expect(
    page.getByText("Copyright © 2026 Kevin O'Shea and O'Shea & Sons, LLC."),
  ).toBeVisible();
  await expect(
    page.getByRole("link", { name: "O'Shea & Sons, LLC" }),
  ).toHaveAttribute("href", "https://osheaandsons.com/");
});

test("waitlist form renders a generic successful outcome", async ({ page }) => {
  await page.route("**/api/waitlist", (route) =>
    route.fulfill({
      status: 202,
      contentType: "application/json",
      body: JSON.stringify({
        ok: true,
        code: "accepted",
        message:
          "Check your inbox to confirm your place on the early-access list.",
      }),
    }),
  );
  await page.goto("/");
  await page.locator("[data-waitlist-form]").evaluate((form) => {
    const token = document.createElement("input");
    token.type = "hidden";
    token.name = "cf-turnstile-response";
    token.value = "test-token";
    form.append(token);
  });
  await page.getByLabel("Work email").fill("developer@example.com");
  await page.getByRole("button", { name: "Join early access" }).click();
  await expect(page.locator("[data-form-status]")).toHaveText(
    "Check your inbox to confirm your place on the early-access list.",
  );
});

test("keyboard users can skip navigation and reach the primary content", async ({
  page,
}) => {
  await page.goto("/");
  await page.keyboard.press("Tab");
  await expect(
    page.getByRole("link", { name: "Skip to content" }),
  ).toBeFocused();
  await page.keyboard.press("Enter");
  await expect(page.locator("#main-content")).toBeFocused();
});

test("layout does not overflow the viewport", async ({ page }) => {
  await page.goto("/");
  const sizes = await page.evaluate(() => ({
    documentWidth: document.documentElement.scrollWidth,
    viewportWidth: document.documentElement.clientWidth,
  }));
  expect(sizes.documentWidth).toBeLessThanOrEqual(sizes.viewportWidth);
});
