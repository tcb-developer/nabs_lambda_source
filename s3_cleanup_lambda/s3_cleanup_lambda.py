"""s3_cleanup_lambda — delete queued storage objects AFTER a fetch run.

Invoked asynchronously (Event) by the NoticeAI backend when a parallel-fetch
run reaches a terminal state, with the removable-files ledger batch:

    {"bucket": "nabsprodbucket", "keys": ["2026/04/13/…pdf", …]}

Design rules (product decision, 20-08-2026):
  - Notice fetching NEVER deletes inline; this worker is the only deleter,
    and it runs entirely off the fetch path, so fetch speed is unaffected.
  - Deleting a key that no longer exists is a SUCCESS (S3 semantics agree:
    delete_objects reports it deleted) — the ledger may legitimately contain
    keys whose objects were already gone.
  - The bucket is versioned: every delete places a delete marker, and the
    noncurrent version survives 180 days — a wrongly-queued key is
    recoverable for six months.

No third-party dependencies — boto3 ships with the Lambda runtime.
"""
import json
import logging

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DEFAULT_BUCKET = "nabsprodbucket"
CHUNK = 1000  # delete_objects hard limit per call


def lambda_handler(event, context):
    if isinstance(event, str):
        event = json.loads(event)
    bucket = (event.get("bucket") or DEFAULT_BUCKET).strip()
    keys = [k for k in (event.get("keys") or []) if isinstance(k, str) and k.strip()]

    # Refuse anything that is not OUR bucket — this worker must never become
    # a generic delete-anything primitive.
    if bucket != DEFAULT_BUCKET:
        logger.error("refusing foreign bucket %r", bucket)
        return {"status": "refused", "reason": "foreign bucket"}
    if not keys:
        return {"status": "ok", "deleted": 0, "errors": 0}

    s3 = boto3.client("s3")
    deleted, errors = 0, []
    for i in range(0, len(keys), CHUNK):
        chunk = keys[i:i + CHUNK]
        resp = s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": k} for k in chunk], "Quiet": False},
        )
        deleted += len(resp.get("Deleted") or [])
        for e in resp.get("Errors") or []:
            errors.append({"key": e.get("Key"), "code": e.get("Code")})

    logger.info(
        "cleanup: requested=%d deleted=%d errors=%d",
        len(keys), deleted, len(errors),
    )
    if errors:
        logger.error("cleanup errors (first 20): %s", errors[:20])
    return {"status": "ok", "deleted": deleted, "errors": len(errors)}
