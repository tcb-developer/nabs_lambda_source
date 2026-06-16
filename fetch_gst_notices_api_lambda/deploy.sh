#!/usr/bin/env bash
#
# Build + deploy the API-based GST notice-fetch Lambda
# (fetch_gst_notices_api_lambda). Pure-requests worker — NO Chrome/Selenium
# layer needed (that's the whole point of this sibling).
#
# This script ONLY packages + ships code. It does NOT contain any secrets.
# Provide AWS creds via your normal AWS CLI profile/role, and the function's
# runtime secrets (2Captcha key, webhook base URL) via --environment below
# OR the AWS console. Re-running is safe: create-function on first run,
# update-function-code + update-function-configuration after.
#
# Prereqs: awscli v2 configured (`aws configure` / SSO), python3, pip.
#
# Usage:
#   ./deploy.sh                      # build zip + create/update function
#   BUILD_ONLY=1 ./deploy.sh         # just produce the zip, don't touch AWS
#
set -euo pipefail

# ---- Config (override via env) --------------------------------------------
FUNCTION_NAME="${FUNCTION_NAME:-fetch_gst_notices_api_lambda}"
REGION="${AWS_REGION:-ap-south-1}"
RUNTIME="${RUNTIME:-python3.12}"
HANDLER="fetch_gst_notices_api_lambda.lambda_handler"
TIMEOUT="${TIMEOUT:-300}"            # seconds (parallel fetch of one client)
MEMORY="${MEMORY:-1024}"            # MB
ROLE_ARN="${ROLE_ARN:-}"            # REQUIRED for first-time create-function
# pip target platform for Linux-x86_64 Lambda wheels (reportlab ships compiled
# parts; building on mac/arm without this yields incompatible binaries).
PIP_PLATFORM="${PIP_PLATFORM:-manylinux2014_x86_64}"
PY_ABI="${PY_ABI:-cp312}"

HERE="$(cd "$(dirname "$0")" && pwd)"
BUILD="$HERE/.build"
ZIP="$HERE/fetch_gst_notices_api_lambda.zip"

echo "==> Cleaning previous build"
rm -rf "$BUILD" "$ZIP"
mkdir -p "$BUILD"

echo "==> Copying source"
cp "$HERE/fetch_gst_notices_api_lambda.py" "$BUILD/"
cp "$HERE/gstr3a_pdf.py" "$BUILD/"

echo "==> Installing dependencies into the bundle (Linux x86_64 wheels)"
# boto3 is provided by the Lambda runtime — do NOT vendor it (keeps the zip
# small). requests, reportlab, twocaptcha (2captcha-python) ARE bundled.
# --platform + --only-binary=:all: forces manylinux wheels so a build on
# mac/arm produces a Lambda-compatible (Linux x86_64) bundle.
python3 -m pip install --quiet --target "$BUILD" \
    --platform "$PIP_PLATFORM" --python-version "${PY_ABI#cp}" \
    --implementation cp --abi "$PY_ABI" --only-binary=:all: \
    "requests>=2.31.0" "reportlab>=4.0.0" "2captcha-python>=1.2.0"

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
  echo "    NOTE: set/verify env vars in the console or via"
  echo "    update-function-configuration --environment (see below)."
else
  if [[ -z "$ROLE_ARN" ]]; then
    echo "ERROR: function does not exist and ROLE_ARN is not set."
    echo "       Set ROLE_ARN=arn:aws:iam::<acct>:role/<lambda-exec-role> and re-run."
    exit 1
  fi
  echo "==> Creating function"
  aws lambda create-function \
      --function-name "$FUNCTION_NAME" --region "$REGION" \
      --runtime "$RUNTIME" --handler "$HANDLER" \
      --role "$ROLE_ARN" --timeout "$TIMEOUT" --memory-size "$MEMORY" \
      --zip-file "fileb://$ZIP" >/dev/null
fi

echo
echo "==> Done. Set the runtime environment variables (NOT in this script):"
cat <<'ENVHELP'
    aws lambda update-function-configuration \
      --function-name fetch_gst_notices_api_lambda --region ap-south-1 \
      --environment 'Variables={
          S3_BUCKET_NAME=nabsprodbucket,
          AWS_REGION=ap-south-1,
          CAPTCHA_API_KEY=<2captcha key>,
          WEBHOOK_BASE_URL=<fastapi base url>
      }'

  Notes:
   - AWS credentials: prefer the function's execution role (no AWS_ACCESS_KEY_ID
     / AWS_SECRET_ACCESS_KEY env vars). The role must allow s3:PutObject on the
     bucket.
   - The orchestrator (parallel_fetching_fastapi_lambda) routes to this worker
     when its event carries gst_fetch_method="api". The FastAPI backend sets
     that from System Configuration → "Default Fetch Method" = api.
ENVHELP
