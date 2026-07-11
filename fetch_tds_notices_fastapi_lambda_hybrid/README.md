# fetch_tds_notices_fastapi_lambda_hybrid

TDS notice-fetch **hybrid** worker Lambda (FastAPI fleet).

TRACES retired its HTML login (the portal is now a Flutter canvas). This worker:

1. **Logs in via the NEW portal's JSON API** (`traces-app.tdscpc.gov.in`,
   2captcha) → Keycloak JWT. Captcha attempts are burst-fired in parallel with a
   staggered `generateCaptcha` (concurrent fetches collide on the portal's
   session binding).
2. **Bridges that JWT into headless Chrome** — CDP-injects `Authorization` +
   `RefreshToken`, GETs `preauthV2.xhtml`; the browser follows the 302 and the
   OLD portal (`traces61.tdscpc.gov.in`) mints the authenticated session.
3. **Scrapes** the demand summary + each FY's quarter notices (quarters fetched
   in parallel inside the lambda, capped by `TDS_QUARTER_FETCH_CONCURRENCY`,
   each its own headless Chrome bridged from the same JWT) and runs the
   **Justification-Report** request/status/download flow.
4. **Returns the result via the worker webhook** to the FastAPI backend — the
   SAME compat route GST/IT use
   (`/api/method/fin_buddy.features.lambda_webhooks.update_worker_result`,
   `portal_type="tds"`). The backend persists via its existing
   `get_or_create_quarter`; **this Lambda never touches Postgres or S3** (JR
   files come back as base64 in the payload for the backend to store).

This is a STANDALONE module — the login + scrape + JR logic is ported verbatim
from the tested backend (`app/services/tds_traces_api.py` +
`app/events/tds_gov.py`); only DB-persist became JSON-return.

## AWS config

- **Region / account:** `ap-south-1` / `020895663185` (FastAPI fleet).
- **Runtime:** `python3.8` (matches the Chrome layer build).
- **Memory / timeout:** `3008 MB` / `900 s` (parallel headless Chromes + the
  multi-step JR flow).
- **Layers** (set on create):
  - `arn:aws:lambda:ap-south-1:020895663185:layer:headless-chrome-selenium:1`
    — Chromium (`/opt/headless-chromium`), chromedriver (`/opt/chromedriver`),
    fonts (`/opt/etc/fonts`), selenium.
  - `arn:aws:lambda:ap-south-1:020895663185:layer:requests-python:1` — requests.
  - `bs4` is imported for the table parse; if not in the layer, bundle it (the
    deploy currently relies on the layer providing it like the GST worker).
- **IAM role:** reuse the selenium workers' role
  (`role/service-role/fetch-notice-income-tax-role-97w35p21`) unless a
  dedicated one is preferred. No S3 perms strictly needed (backend stores JR).
- **Env vars:** `CAPTCHA_API_KEY` (2captcha), `WEBHOOK_BASE_URL` (fallback
  callback base), `TDS_QUARTER_FETCH_CONCURRENCY` (default 2).

## Deploy

```sh
# build only (no AWS):
BUILD_ONLY=1 ./deploy.sh
# first-time create (needs the role) + later updates:
ROLE_ARN=arn:aws:iam::020895663185:role/service-role/fetch-notice-income-tax-role-97w35p21 ./deploy.sh
```

`deploy.sh` ships ONLY the module (selenium/requests/bs4 from layers), attaches
the two layers on create, sets 3008/900. Re-runs `update-function-code` after.
It does NOT set env vars / IAM — do that once (command printed at the end).

## Event shape (from the orchestrator / a direct test invoke)

```json
{
  "client_name": "TDS-CLT-...",
  "tan": "<TAN>",
  "username": "<TRACES user id>",
  "password": "<password>",
  "with_jr": true,
  "webhook_config": { "url": "<fastapi base>", "log_name": "...", "api_key": "...", "api_secret": "..." }
}
```

## Orchestration

The orchestrator `parallel_fetching_fastapi_lambda` invokes this worker (async,
`Event`) for every client in the event's `tds_clients` list — giving
cross-client parallelism. The FastAPI backend builds that event behind a TDS
"fetch via lambda" flag; **in-process Selenium remains the default** until the
flag is switched after side-by-side validation.

> FastAPI fleet only — has nothing to do with the Frappe og_app TDS scraper or
> the `parallel_fetching_lambda` / `fetch_gst_notices_lambda` (og_app) functions.
