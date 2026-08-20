#!/usr/bin/env bash
#
# Build + deploy the post-fetch S3 cleanup worker (s3_cleanup_lambda).
# Pure boto3 — the Lambda runtime provides it, so the zip is just the module.
#
# Usage:
#   ./deploy.sh                          # create/update the function
#   ROLE_ARN=arn:aws:iam::...:role/x ./deploy.sh   # required on first create
#
set -euo pipefail

FUNCTION_NAME="${FUNCTION_NAME:-s3_cleanup_lambda}"
REGION="${AWS_REGION:-ap-south-1}"
RUNTIME="${RUNTIME:-python3.12}"
HANDLER="s3_cleanup_lambda.lambda_handler"
TIMEOUT="${TIMEOUT:-120}"
MEMORY="${MEMORY:-256}"
ROLE_ARN="${ROLE_ARN:-}"

HERE="$(cd "$(dirname "$0")" && pwd)"
ZIP="$HERE/s3_cleanup_lambda.zip"

rm -f "$ZIP"
( cd "$HERE" && zip -qj "$ZIP" s3_cleanup_lambda.py )
echo "built: $ZIP ($(du -h "$ZIP" | cut -f1))"

if aws lambda get-function --function-name "$FUNCTION_NAME" --region "$REGION" >/dev/null 2>&1; then
  echo "==> updating code"
  aws lambda update-function-code \
      --function-name "$FUNCTION_NAME" --region "$REGION" \
      --zip-file "fileb://$ZIP" >/dev/null
else
  if [[ -z "$ROLE_ARN" ]]; then
    echo "ERROR: first-time create needs ROLE_ARN"; exit 1
  fi
  echo "==> creating function"
  aws lambda create-function \
      --function-name "$FUNCTION_NAME" --region "$REGION" \
      --runtime "$RUNTIME" --handler "$HANDLER" \
      --role "$ROLE_ARN" --timeout "$TIMEOUT" --memory-size "$MEMORY" \
      --zip-file "fileb://$ZIP" >/dev/null
fi
echo "==> done"
