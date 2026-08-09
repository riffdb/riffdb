# RiffDB website

The public marketing site for [riffdb.com](https://riffdb.com). It is an Astro
static build with one Cloudflare Pages Function at `POST /api/waitlist`.

## Local development

Use the versions in `.nvmrc` and `package.json`, then run:

```bash
npm ci
npm run dev
```

The site builds without configuration, but leaves the signup form disabled.
For local signup development, copy `.dev.vars.example` to `.dev.vars`, export
the example `PUBLIC_TURNSTILE_SITE_KEY`, and run the static site and Pages
Function together with Wrangler:

```bash
PUBLIC_TURNSTILE_SITE_KEY=1x00000000000000000000AA npm run build
npx wrangler pages dev dist
```

The checked example uses Cloudflare's published always-pass testing keys. Never
use those keys for the production widget.

## Verification

```bash
npm run check
npm test
npm run build
npm run test:e2e
```

The browser suite starts its own production preview with the public testing
sitekey and replaces the Turnstile and waitlist network boundaries.

## Production configuration

Create a Cloudflare Pages Direct Upload project named `riffdb`, attach
`riffdb.com`, and configure a Cloudflare redirect rule from `www.riffdb.com` to
the same path on the apex domain. Set these runtime bindings on both production
and any trusted preview environment:

- `LOOPS_FORM_ENDPOINT`: the `https://app.loops.so/api/newsletter-form/...`
  endpoint for the RiffDB form.
- `TURNSTILE_SECRET_KEY`: an encrypted Pages secret for a Turnstile widget
  restricted to `riffdb.com` with action `waitlist`.

Set the GitHub Actions repository variable `PUBLIC_TURNSTILE_SITE_KEY` to that
widget's public key. The protected `production` environment requires:

- `CLOUDFLARE_ACCOUNT_ID`
- `CLOUDFLARE_API_TOKEN`, scoped to edit only the RiffDB Pages project

Set the repository variable `CLOUDFLARE_DEPLOY_ENABLED=true` only after the
project, runtime bindings, custom domain, Loops form, and double opt-in email
are ready. Until then, pushes still run the full website verification job but
skip publication. A maintainer can also use the manual workflow trigger.

The Loops form must have double opt-in enabled and subscribe confirmed contacts
to the early-access mailing list. `hello@riffdb.com` must accept privacy and
deletion requests before production signup is enabled.
