#!/usr/bin/env bash
#
# Build + deploy the TDS HYBRID notice-fetch Lambda
# (fetch_tds_notices_fastapi_lambda_hybrid).
#
# Hybrid = new-portal JSON API login (2captcha) → preauthV2 browser bridge →
# old-portal (traces61) headless-Selenium scrape of demand/quarter notices +
# Justification Reports. Selenium + Chromium come from Lambda LAYERS (the same
# proven layers the GST selenium worker uses) — so this script does NOT bundle
# selenium/requests; it ships ONLY our module. The layers are attached on
# create-function (see CREATE config below).
#
# This script ONLY packages + ships code. NO secrets here. Provide AWS creds via
# your AWS CLI profile/role; provide the function's runtime secrets (2Captcha
# key, webhook base URL) via --environment (console or the command printed at
# the end). Re-running is safe: create on first run, update-code after.
#
# FastAPI fleet only. Region ap-south-1, account 020895663185. Do NOT confuse
# with the Frappe og_app lambdas.
#
# Usage:
#   ./deploy.sh                 # build zip + create/update function
#   BUILD_ONLY=1 ./deploy.sh    # just produce the zip, don't touch AWS
#
set -euo pipefail

# ---- Config (override via env) --------------------------------------------
FUNCTION_NAME="${FUNCTION_NAME:-fetch_tds_notices_fastapi_lambda_hybrid}"
REGION="${AWS_REGION:-ap-south-1}"
# py3.8 matches the headless-chrome-selenium layer build (same as the GST
# selenium worker, which runs py3.8 with this layer).
RUNTIME="${RUNTIME:-python3.8}"
HANDLER="fetch_tds_notices_fastapi_lambda_hybrid.lambda_handler"
# JR + parallel quarters can run long; give a generous budget (Lambda max 900).
TIMEOUT="${TIMEOUT:-900}"            # seconds
# In-lambda parallel quarters each spawn a headless Chrome (~300-500MB) — size
# up so 2 concurrent browsers fit comfortably.
MEMORY="${MEMORY:-3008}"            # MB
ROLE_ARN="${ROLE_ARN:-}"            # REQUIRED for first-time create-function
# Lambda layers — the SAME proven layers the GST selenium worker uses.
CHROME_LAYER="${CHROME_LAYER:-arn:aws:lambda:ap-south-1:020895663185:layer:headless-chrome-selenium:1}"
REQUESTS_LAYER="${REQUESTS_LAYER:-arn:aws:lambda:ap-south-1:020895663185:layer:requests-python:1}"

HERE="$(cd "$(dirname "$0")" && pwd)"
BUILD="$HERE/.build"
ZIP="$HERE/fetch_tds_notices_fastapi_lambda_hybrid.zip"

echo "==> Cleaning previous build"
rm -rf "$BUILD" "$ZIP"
mkdir -p "$BUILD"

echo "==> Copying source (module only — selenium/requests/bs4 come from layers)"
cp "$HERE/fetch_tds_notices_fastapi_lambda_hybrid.py" "$BUILD/"

echo "==> Zipping"
( cd "$BUILD" && zip -qr "$ZIP" . )
echo "    built: $ZIP ($(du -h "$ZIP" | cut -f1))"

if [[ "${BUILD_ONLY:-0}" == "1" ]]; then
  echo "==> BUILD_ONLY set — skipping AWS deploy."
  exit 0
fi

echo "==> Checking whether function exists"
if aws lambda get-function --function-name "$FUNCTION_NAME" --region "$REGION" >/dev/null 2>&1; then
  echo "==> Updating code"
  aws lambda update-function-code \
      --function-name "$FUNCTION_NAME" --region "$REGION" \
      --zip-file "fileb://$ZIP" >/dev/null
  echo "==> Updating configuration (handler/runtime/timeout/memory)"
  aws lambda update-function-configuration \
      --function-name "$FUNCTION_NAME" --region "$REGION" \
      --handler "$HANDLER" --runtime "$RUNTIME" \
      --timeout "$TIMEOUT" --memory-size "$MEMORY" >/dev/null
  echo "    NOTE: layers + env vars are NOT changed by an update run; set them"
  echo "    once at create (below) or in the console."
else
  if [[ -z "$ROLE_ARN" ]]; then
    echo "ERROR: function does not exist and ROLE_ARN is not set."
    echo "       Set ROLE_ARN=arn:aws:iam::020895663185:role/<lambda-exec-role>"
    echo "       (the GST/IT selenium workers use"
    echo "        role/service-role/fetch-notice-income-tax-role-97w35p21) and re-run."
    exit 1
  fi
  echo "==> Creating function (with the Chrome + requests layers)"
  aws lambda create-function \
      --function-name "$FUNCTION_NAME" --region "$REGION" \
      --runtime "$RUNTIME" --handler "$HANDLER" \
      --role "$ROLE_ARN" --timeout "$TIMEOUT" --memory-size "$MEMORY" \
      --layers "$CHROME_LAYER" "$REQUESTS_LAYER" \
      --zip-file "fileb://$ZIP" >/dev/null
fi

echo
echo "==> Done. Set the runtime environment variables (NOT in this script):"
cat <<'ENVHELP'
    aws lambda update-function-configuration \
      --function-name fetch_tds_notices_fastapi_lambda_hybrid --region ap-south-1 \
      --environment 'Variables={
          CAPTCHA_API_KEY=<2captcha key>,
          WEBHOOK_BASE_URL=<fastapi base url>,
          TDS_QUARTER_FETCH_CONCURRENCY=2
      }'

  Notes:
   - AWS credentials: prefer the function's execution role (no AWS_ACCESS_KEY_ID
     / AWS_SECRET_ACCESS_KEY env vars).
   - Layers (set on create): headless-chrome-selenium (Chromium + chromedriver +
     selenium at /opt) and requests-python. An UPDATE run does not re-attach
     them — if you ever recreate, pass --layers again.
   - The orchestrator (parallel_fetching_fastapi_lambda) invokes this worker for
     every client in the event's `tds_clients` list. The FastAPI backend builds
     that event behind its TDS "fetch via lambda" flag (in-process Selenium
     stays the default until the flag is switched).
ENVHELP
