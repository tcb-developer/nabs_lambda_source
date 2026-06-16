# fetch_gst_notices_api_lambda

API-based (pure-requests) GST notice-fetch worker Lambda. Sibling of
`fetch_gst_notices_fastapi_lambda` (the Selenium worker) — **same event
contract, same webhook callback, same S3 layout**; only the fetch engine
differs (no Chrome, no Selenium layer).

## Files
- `fetch_gst_notices_api_lambda.py` — handler + self-contained requests engine
  (verbatim port of `backend/app/features/fetch_gst_notices_requests.py`, made
  `app.*`-free).
- `gstr3a_pdf.py` — reportlab GSTR-3A PDF builder (copied from
  `backend/app/features/gstr3a_pdf.py`; reportlab-only, no `app.*`).
- `deploy.sh` — build the zip + create/update the function (no secrets inside).

## Event contract (from the orchestrator)
```json
{
  "username": "<gst username>",
  "password": "<gst password>",
  "client_name": "<GSTClient.id>",
  "organization_id": "<org id>",          // REQUIRED — used in the S3 key
  "gstin": "<gstin>",                      // optional
  "gst_file_download_concurrency": 5,      // optional
  "webhook_config": { "url": "...", "api_key": "...", "api_secret": "...", "log_name": "..." }
}
```
Returns `{statusCode, body}` and fires the worker webhook to
`<WEBHOOK_BASE_URL>/api/method/fin_buddy.features.lambda_webhooks.update_worker_result`
— exactly like the Selenium worker, so the FastAPI receiver persists it
unchanged.

## S3 layout
`FAPI_GST_API_Notices/<organization_id>/<client_id>/<notice_id>/<file>` — the
same key the in-app engine writes and the frontend expects.

## Runtime env vars (set on the function, NOT in code)
| Var | Purpose |
|---|---|
| `S3_BUCKET_NAME` | target bucket (default `nabsprodbucket`) |
| `AWS_REGION` | region (default `ap-south-1`) |
| `CAPTCHA_API_KEY` | 2Captcha key for login captcha |
| `WEBHOOK_BASE_URL` | FastAPI base URL for the result callback |

AWS credentials should come from the **execution role** (needs `s3:PutObject`
on the bucket). Do NOT try to set `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`
as Lambda env vars — they are reserved and, set without `AWS_SESSION_TOKEN`,
break the role's temporary creds. For a non-role override (local runs) use the
non-reserved `GST_S3_ACCESS_KEY` / `GST_S3_SECRET_KEY`.

## Dependencies (bundled by deploy.sh)
`requests`, `reportlab`, `2captcha-python`. `boto3` is provided by the Lambda
runtime and is intentionally not vendored. **No Chrome/Selenium layer needed.**

## How it is selected
The orchestrator `parallel_fetching_fastapi_lambda` is method-aware: when its
event carries `gst_fetch_method="api"` it invokes THIS worker; otherwise it
invokes the Selenium worker. The FastAPI backend sets that flag from
**System Configuration → "Default Fetch Method" = api**
(`AppSettings.gst_default_fetch_method`). Both workers coexist; switching is a
config change, not a redeploy.

## Deploy
```bash
# first time (needs an execution role):
ROLE_ARN=arn:aws:iam::<acct>:role/<lambda-exec-role> ./deploy.sh
# subsequent updates:
./deploy.sh
# just build the zip, no AWS:
BUILD_ONLY=1 ./deploy.sh
```
Then set the env vars (see the tail of `deploy.sh`).
