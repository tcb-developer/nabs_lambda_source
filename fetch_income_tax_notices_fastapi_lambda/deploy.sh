#!/usr/bin/env bash
#
# Build + deploy the FastAPI-only Income-Tax notice-fetch Lambda
# (fetch_income_tax_notices_fastapi_lambda).
#
# This is a DUPLICATE of the shared `fetch_income_tax_notices_lambda` (which
# stays untouched and continues to serve og_app/Frappe). The duplicate carries
# the FastAPI-only speed fixes:
#   - per-notice processing parallelised (ThreadPoolExecutor)
#   - per-notice sleep(1) removed
#   - login sleep(12) replaced with an adaptive short-poll
# Only the FastAPI orchestrator (parallel_fetching_fastapi_lambda) is pointed at
# this function; og_app's orchestrator keeps invoking the original. So deploying
# / updating this function CANNOT affect og_app.
#
# Config is mirrored from the live shared lambda (captured in _aws_config.json):
#   runtime python3.9, x86_64, role fetch-notice-income-tax-role-97w35p21,
#   timeout 900, memory 256, layer selenium-python:1, NO env vars (the worker's
#   AWS creds + S3 bucket are constants inside the .py, same as the original).
#
# The Selenium runtime comes ENTIRELY from the layer — there are NO pip deps to
# bundle, so the zip is just the single .py (boto3/requests are provided by the
# runtime + layer). This is why there is no `pip install` step here.
#
# This script ships CODE ONLY. It contains no secrets. Provide AWS creds via
# your normal AWS CLI profile/role (must be account 020895663185).
#
# Prereqs: awscli v2 configured, zip.
#
# Usage:
#   ./deploy.sh                # build zip + create/update function
#   BUILD_ONLY=1 ./deploy.sh   # just produce the zip, don't touch AWS
#
set -euo pipefail

# ---- Config (mirrors the shared IT lambda; override via env) ---------------
FUNCTION_NAME="${FUNCTION_NAME:-fetch_income_tax_notices_fastapi_lambda}"
REGION="${AWS_REGION:-ap-south-1}"
RUNTIME="${RUNTIME:-python3.9}"
# Handler module name MUST match the .py filename (not the original lambda's).
HANDLER="fetch_income_tax_notices_fastapi_lambda.lambda_handler"
TIMEOUT="${TIMEOUT:-900}"           # seconds (mirrors shared lambda)
MEMORY="${MEMORY:-256}"             # MB (mirrors shared lambda)
ARCH="${ARCH:-x86_64}"
# Selenium runtime layer — same ARN the shared lambda uses.
LAYER_ARN="${LAYER_ARN:-arn:aws:lambda:ap-south-1:020895663185:layer:selenium-python:1}"
# Execution role — same role the shared lambda uses (has S3 + logs perms).
ROLE_ARN="${ROLE_ARN:-arn:aws:iam::020895663185:role/service-role/fetch-notice-income-tax-role-97w35p21}"

HERE="$(cd "$(dirname "$0")" && pwd)"
BUILD="$HERE/.build"
ZIP="$HERE/fetch_income_tax_notices_fastapi_lambda.zip"

echo "==> Cleaning previous build"
rm -rf "$BUILD" "$ZIP"
mkdir -p "$BUILD"

echo "==> Copying source (code only — Selenium comes from the layer)"
cp "$HERE/fetch_income_tax_notices_fastapi_lambda.py" "$BUILD/"

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
  aws lambda wait function-updated --function-name "$FUNCTION_NAME" --region "$REGION"
  echo "==> Updating configuration (handler/runtime/timeout/memory/layer)"
  aws lambda update-function-configuration \
      --function-name "$FUNCTION_NAME" --region "$REGION" \
      --handler "$HANDLER" --runtime "$RUNTIME" \
      --timeout "$TIMEOUT" --memory-size "$MEMORY" \
      --layers "$LAYER_ARN" >/dev/null
  aws lambda wait function-updated --function-name "$FUNCTION_NAME" --region "$REGION"
else
  echo "==> Creating function"
  aws lambda create-function \
      --function-name "$FUNCTION_NAME" --region "$REGION" \
      --runtime "$RUNTIME" --handler "$HANDLER" \
      --role "$ROLE_ARN" --timeout "$TIMEOUT" --memory-size "$MEMORY" \
      --architectures "$ARCH" \
      --layers "$LAYER_ARN" \
      --zip-file "fileb://$ZIP" >/dev/null
  aws lambda wait function-active --function-name "$FUNCTION_NAME" --region "$REGION"
fi

echo
echo "==> Done. Deployed: $FUNCTION_NAME ($REGION)"
echo "    No env vars are set (the worker's AWS creds + S3 bucket are code"
echo "    constants, identical to the shared lambda). The role provides the"
echo "    runtime AWS permissions."
echo
echo "    Next: point the FastAPI orchestrator at this function —"
echo "      INCOME_TAX_LAMBDA_FUNCTION_NAME = \"$FUNCTION_NAME\""
echo "    in lambda_source/parallel_fetching_fastapi_lambda/parallel_fetching.py,"
echo "    then redeploy that orchestrator. og_app's orchestrator stays on the"
echo "    original shared lambda."
