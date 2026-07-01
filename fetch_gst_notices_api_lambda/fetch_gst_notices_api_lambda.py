"""AWS Lambda — GST notice fetch via the DIRECT PORTAL API (pure requests).

Sibling of fetch_gst_notices_fastapi_lambda (the Selenium worker): SAME event
contract, SAME webhook callback, SAME S3 key layout — only the fetch engine
differs. Here there is NO browser: login is a plain JSON POST to
/services/authenticate (captcha solved via 2Captcha), and notices are pulled
through the portal's own authed JSON APIs. This is a verbatim port of the
in-app engine `app/features/fetch_gst_notices_requests.py` made self-contained
(no `app.*` imports) so it ships as one Lambda file.

Event (from the parallel_fetching orchestrator, fire-and-forget):
    {
      "username": "<gst username>",
      "password": "<gst password>",
      "client_name": "<GSTClient.id>",
      "organization_id": "<org id>",            # REQUIRED for the S3 key
      "gstin": "<gstin>",                        # optional, used in result
      "gst_file_download_concurrency": 5,        # optional
      "webhook_config": {...}                    # optional, callback to FastAPI
    }

Returns the SAME shape the Selenium worker returns and fires the SAME worker
webhook, so the FastAPI receiver persists it unchanged via
`lambda_webhooks.update_worker_result` → `_process_gst_notices`.

Two header rules (cracked during research, baked in):
  - List/chain JSON APIs  → send the `at:` header (the AuthToken value).
  - Document/summary GETs  → send a valid Referer, do NOT send `at:`
    (it 403s "Session Expired"); the AuthToken cookie authenticates them.

Runtime config via ENV (NO hardcoded secrets in this file):
  S3_BUCKET_NAME, AWS_REGION, CAPTCHA_API_KEY, WEBHOOK_BASE_URL
  (AWS credentials resolve from the Lambda execution role by default; explicit
  AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env vars are honoured if present.)
"""

import base64
import json
import logging
import os
import re
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore.client import Config
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Runtime config (env only) ---------------------------------------------
S3_BUCKET = os.environ.get("S3_BUCKET_NAME", "nabsprodbucket")
AWS_REGION = os.environ.get("AWS_REGION", "ap-south-1")
CAPTCHA_API_KEY = os.environ.get("CAPTCHA_API_KEY", "")

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/148.0.0.0 Safari/537.36")
SERVICES = "https://services.gst.gov.in"
RETURN = "https://return.gst.gov.in"

# Portal session keep-alive — refreshes the authed session during a long fetch
# so it doesn't expire mid-run. (Self-contained copy of the backend's
# app/features/gst_keepalive.py — this is a separate codebase and cannot import
# app.*; keep the two in sync.)
KEEPALIVE_URL = f"{SERVICES}/litserv/auth/api/keepalive"
KEEPALIVE_INTERVAL_SECONDS = 60


class _KeepAliveThread:
    """Background daemon pinging the portal keep-alive on a requests.Session.

    The session is thread-safe for concurrent GETs, so a background thread can
    keep it warm while the row pool downloads. Best-effort: a failed ping is
    swallowed, never raised.
    """

    def __init__(self, session, interval=KEEPALIVE_INTERVAL_SECONDS):
        self._session = session
        self._interval = max(5, int(interval or KEEPALIVE_INTERVAL_SECONDS))
        self._stop = threading.Event()
        self._thread = None

    def _ping_once(self):
        try:
            r = self._session.get(
                KEEPALIVE_URL, timeout=15,
                headers={"Accept": "application/json, text/plain, */*"})
            logger.info("GST keep-alive ping -> HTTP %s", r.status_code)
        except Exception as e:
            logger.info("GST keep-alive ping failed (ignored): %s", e)

    def _run(self):
        while not self._stop.wait(self._interval):
            self._ping_once()

    def start(self):
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run, name="gst-keepalive", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None

# S3 root for the direct-API GST fetcher (MUST match the in-app engine + FE):
#   FAPI_GST_API_Notices/<organization_id>/<client_id>/<notice_id>/<file>
GST_API_S3_ROOT = "FAPI_GST_API_Notices"

# Per-case attachment-download concurrency (overridable per invocation via the
# event's `gst_file_download_concurrency`). Default 5 matches System Config.
_GST_FILE_DOWNLOAD_CONCURRENCY = 5

# Bounded retry for transient failures (network blip / portal 5xx / empty body
# / timeout). A genuine 4xx is NOT retried.
_RETRY_ATTEMPTS = 3
_RETRY_BACKOFF = 1.2  # seconds, ×attempt

# Login gets its OWN, smaller attempt budget: each attempt fetches a fresh
# captcha and solves it via 2Captcha (which costs money and mis-reads ~10-15%
# of the time), so a captcha miss or transient portal blip should be retried —
# but a GENUINE wrong-credential rejection must fail fast (no point burning
# captcha solves, and repeated wrong passwords risk a GST portal lockout).
_LOGIN_ATTEMPTS = 3
_LOGIN_BACKOFF = 1.5  # seconds, ×attempt

_s3_client = None


# Per-invocation S3 config from the event payload (s3_config), sourced from
# FastAPI System Configuration. configure_s3() applies it before any upload.
_S3_CONFIG = {}


def configure_s3(s3_config):
    """Apply event-provided S3 config (keys/region/bucket) from System
    Configuration. Resets the cached client. No-op if empty."""
    global _S3_CONFIG, _s3_client, S3_BUCKET, AWS_REGION
    if not s3_config:
        return
    _S3_CONFIG = dict(s3_config)
    if _S3_CONFIG.get("region"):
        AWS_REGION = _S3_CONFIG["region"]
    if _S3_CONFIG.get("bucket"):
        S3_BUCKET = _S3_CONFIG["bucket"]
    _s3_client = None


def _get_s3_client():
    """Shared S3 client.

    Credential precedence (never hardcoded):
      1. event s3_config (System Configuration, via configure_s3) — these are
         permanent IAM-user keys (access + secret, no session token), valid to
         hand to boto3 directly.
      2. GST_S3_ACCESS_KEY / GST_S3_SECRET_KEY env vars (explicit local override).
      3. boto3 default chain — on Lambda the execution role's temporary creds.
    We must NOT hand-pick AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY from the env
    while dropping AWS_SESSION_TOKEN — that yields an invalid credential."""
    global _s3_client
    if _s3_client is None:
        kwargs = {"region_name": AWS_REGION, "config": Config(signature_version="s3v4")}
        ak = _S3_CONFIG.get("aws_access_key_id") or os.environ.get("GST_S3_ACCESS_KEY")
        sk = _S3_CONFIG.get("aws_secret_access_key") or os.environ.get("GST_S3_SECRET_KEY")
        if ak and sk:
            kwargs["aws_access_key_id"] = ak
            kwargs["aws_secret_access_key"] = sk
        _s3_client = boto3.client("s3", **kwargs)
    return _s3_client


# ---------------------------------------------------------------------------
# Shared helpers — ported verbatim from app/features/fetch_gst_notices.py
# (kept self-contained; do not import app.*).
# ---------------------------------------------------------------------------

def _safe_name(s):
    return re.sub(r"[^A-Za-z0-9._-]+", "_", s or "unnamed").strip("._-")


def _clean_date(date_str):
    """DD/MM/YYYY (or epoch-ms) -> ISO date string. Accepts both because the
    portal's case/task APIs return some dates as epoch-milliseconds."""
    if date_str is None:
        return None
    from datetime import datetime, timezone
    if isinstance(date_str, (int, float)) and not isinstance(date_str, bool):
        try:
            ms = int(date_str)
            if ms > 0:
                return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).date().isoformat()
        except Exception:
            return None
        return None
    if isinstance(date_str, str):
        s = date_str.strip()
        if not s or s.lower() == "na":
            return None
        if s.isdigit() and len(s) >= 12:
            try:
                return datetime.fromtimestamp(int(s) / 1000, tz=timezone.utc).date().isoformat()
            except Exception:
                pass
        try:
            return datetime.strptime(s, "%d/%m/%Y").date().isoformat()
        except Exception:
            try:
                return datetime.fromisoformat(s).date().isoformat()
            except Exception:
                return None
    return None


def _clean_amount(amount):
    if isinstance(amount, str):
        if amount and amount.lower() != "na":
            try:
                return float(amount)
            except Exception:
                return 0.0
        return 0.0
    try:
        return float(amount)
    except Exception:
        return 0.0


def _doc_ext(data, mime, doc_name):
    """Real extension from magic bytes -> mime -> docName suffix. None only
    when the bytes are an HTML error page (treat as a miss)."""
    head = data[:8]
    if head[:4] == b"%PDF":
        return ".pdf"
    if head[:3] == b"\xff\xd8\xff":
        return ".jpg"
    if head[:8] == b"\x89PNG\r\n\x1a\n":
        return ".png"
    if head[:4] == b"PK\x03\x04":
        ext = os.path.splitext(doc_name)[1].lower()
        return ext if ext else ".zip"
    if head[:5] == b"{\\rtf":
        return ".rtf"
    low = data[:200].lower()
    if head[:1] == b"<" or b"<html" in low:
        return None
    mime = (mime or "").lower()
    mime_map = {
        "application/pdf": ".pdf", "image/jpeg": ".jpg", "image/png": ".png",
        "image/tiff": ".tiff", "application/msword": ".doc",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
        "application/vnd.ms-excel": ".xls",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    }
    for k, ext in mime_map.items():
        if k in mime:
            return ext
    name_ext = os.path.splitext(doc_name)[1].lower()
    if name_ext:
        return name_ext
    return ".bin" if data else None


def _content_type_for(ext):
    return {
        ".pdf": "application/pdf", ".jpg": "image/jpeg", ".png": "image/png",
        ".tiff": "image/tiff", ".doc": "application/msword",
        ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ".xls": "application/vnd.ms-excel",
        ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ".rtf": "application/rtf", ".zip": "application/zip",
    }.get(ext, "application/octet-stream")


def _parse_doc_descriptors(item_json_str):
    """Walk an itemJson string for every `dcupdtls` descriptor."""
    try:
        parsed = json.loads(item_json_str)
    except Exception:
        return []
    found = []

    def walk(obj):
        if isinstance(obj, dict):
            if "dcupdtls" in obj:
                v = obj["dcupdtls"]
                if isinstance(v, dict):
                    found.append(v)
                elif isinstance(v, list):
                    for item in v:
                        if isinstance(item, dict):
                            if "id" in item or "docName" in item:
                                found.append(item)
                            else:
                                walk(item)
            for v in obj.values():
                walk(v)
        elif isinstance(obj, list):
            for item in obj:
                walk(item)

    walk(parsed)
    return found


def _parse_item_dates(item_json_str):
    """Pull notice-level dates + section out of a case-item's itemJson.

    (Self-contained copy of the backend app/features/fetch_gst_notices.py
    helper — keep in sync.) Issue Date <- todtls.dt; Due Date <- sdtls.*.duedt;
    Section <- sdtls.*.sec. The row's top-level insertDate/updateDate are only
    save timestamps, NOT the notice dates. Returns DD/MM/YYYY strings ("" when
    absent); the caller cleans via _clean_date.
    """
    out = {"issue_date": "", "due_date": "", "section": ""}
    try:
        parsed = json.loads(item_json_str)
    except Exception:
        return out
    if not isinstance(parsed, dict):
        return out
    todtls = parsed.get("todtls")
    if isinstance(todtls, dict):
        out["issue_date"] = todtls.get("dt") or ""
    sdtls = parsed.get("sdtls")
    if isinstance(sdtls, dict):
        for sub in sdtls.values():
            if not isinstance(sub, dict):
                continue
            if not out["due_date"] and sub.get("duedt"):
                out["due_date"] = sub.get("duedt") or ""
            if not out["section"] and sub.get("sec"):
                out["section"] = sub.get("sec") or ""
    return out


def upload_file_to_s3(file_content, s3_key, content_type="application/octet-stream"):
    """Upload bytes to S3. Returns the key on success, None on failure."""
    try:
        client = _get_s3_client()
        client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=file_content,
                          ContentType=content_type)
        return s3_key
    except Exception as e:
        logger.error("Failed to upload S3 key %s: %s", s3_key, str(e))
        return None


# ---------------------------------------------------------------------------
# GSTR-3A PDF — reportlab port (verbatim notice text per retTyp).
# Lazy import so a missing reportlab layer fails only the GSTR-3A rows, not
# the whole fetch.
# ---------------------------------------------------------------------------

def build_gstr3a_pdf(summary, dt_of_issue):
    from gstr3a_pdf import build_gstr3a_pdf as _impl
    return _impl(summary, dt_of_issue)


# ---------------------------------------------------------------------------
# 2Captcha
# ---------------------------------------------------------------------------

def _solve_captcha(png_bytes):
    if not CAPTCHA_API_KEY:
        logger.error("CAPTCHA_API_KEY env not set")
        return None
    try:
        from twocaptcha import TwoCaptcha
        return (TwoCaptcha(CAPTCHA_API_KEY).normal(base64.b64encode(png_bytes).decode())
                .get("code", "") or "").strip()
    except Exception as e:
        logger.error("2captcha solve failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Login + transport (pure requests, with bounded transient retry)
# ---------------------------------------------------------------------------

def _is_bad_credentials(status, body_text):
    """True ONLY when the portal clearly rejected the USERNAME/PASSWORD, so we
    fail fast (retrying a wrong password wastes captcha solves). Everything else
    — including a wrong/expired captcha — is retryable.

    The GST portal returns a precise `errorCode` (verified live against the
    /services/authenticate endpoint):
      - AUTH_9002  → wrong username/password  → bad credentials (DO NOT retry)
      - SWEB_9000  → wrong/expired captcha    → transient       (RETRY)
    Match ONLY AUTH_9002 (plus its sibling AUTH_9003 = account locked, which is
    also not a transient and must not be retried). Default = retryable."""
    t = (body_text or "").lower()
    return ("auth_9002" in t) or ("auth_9003" in t)


def _login_once(username, password):
    """A single login attempt. Returns (session, None) on success, or
    (None, reason) where reason is "bad_credentials" (do NOT retry) or
    "transient" (safe to retry with a fresh captcha)."""
    s = requests.Session()
    s.headers.update({"User-Agent": UA,
                      "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8"})
    try:
        s.get(f"{SERVICES}/services/login", timeout=20)
        cap = s.get(
            f"{SERVICES}/services/captcha?rnd=0.{int(time.time()*1000) % 99999}",
            timeout=20)
    except requests.RequestException as e:
        logger.error("GST login: pre-auth request failed: %s", e)
        return None, "transient"
    if cap.status_code != 200 or not cap.content:
        logger.error("GST login: captcha fetch failed (%s)", cap.status_code)
        return None, "transient"
    code = _solve_captcha(cap.content)
    if not code:
        logger.error("GST login: captcha not solved")
        return None, "transient"
    mfp = json.dumps({
        "VERSION": "2.1",
        "MFP": {"Browser": {"UserAgent": UA, "CookieEnabled": True}},
        "MESC": {"mesc": "mi=2"},
    })
    try:
        r = s.post(f"{SERVICES}/services/authenticate",
                   json={"username": username, "password": password, "captcha": code,
                         "mFP": mfp, "deviceID": None, "type": "username"},
                   headers={"Content-Type": "application/json;charset=UTF-8",
                            "Accept": "application/json, text/plain, */*",
                            "Origin": SERVICES, "Referer": f"{SERVICES}/services/login"},
                   timeout=30)
    except requests.RequestException as e:
        logger.error("GST login: authenticate request failed: %s", e)
        return None, "transient"
    tok = s.cookies.get("AuthToken")
    if r.status_code == 200 and tok:
        s._at = tok
        return s, None
    logger.error("GST login: authenticate %s %s", r.status_code, r.text[:150])
    if _is_bad_credentials(r.status_code, r.text):
        return None, "bad_credentials"
    return None, "transient"


def _login(username, password):
    """Login with bounded retry. A captcha miss / portal blip is retried with a
    fresh captcha each attempt; a clear wrong-credential rejection fails fast.
    Returns an authed Session (with `_at` stashed) or None."""
    for attempt in range(1, _LOGIN_ATTEMPTS + 1):
        s, reason = _login_once(username, password)
        if s is not None:
            if attempt > 1:
                logger.warning("GST login: succeeded on attempt %d", attempt)
            return s
        if reason == "bad_credentials":
            logger.error("GST login: bad credentials — not retrying")
            return None
        if attempt < _LOGIN_ATTEMPTS:
            logger.warning("GST login: transient failure, retrying (%d/%d)",
                           attempt, _LOGIN_ATTEMPTS)
            time.sleep(_LOGIN_BACKOFF * attempt)
    logger.error("GST login: all %d attempts failed (transient)", _LOGIN_ATTEMPTS)
    return None


def _looks_like_json(text):
    """Cheap check: does the body look like a JSON value? Under load the GST
    portal sometimes returns HTTP 200 with an EMPTY or HTML body — those are
    NOT parseable and must not be trusted as a 200 success."""
    if not text:
        return False
    t = text.lstrip()
    return bool(t) and t[0] in "[{\"-0123456789tfn"  # array/obj/str/num/bool/null


def _loads(text, default):
    """Safe json.loads — returns `default` on empty / non-JSON, OR when the
    parsed value's type doesn't match the expected container (`default`).
    The portal sometimes returns a bare JSON STRING (e.g. an error message)
    which is valid JSON but not a list/dict; iterating it then crashed with
    "'str' object has no attribute 'get'". Guarding the type prevents that."""
    if not _looks_like_json(text):
        return default
    try:
        val = json.loads(text)
    except (ValueError, TypeError):
        return default
    # Only accept the same container kind the caller asked for.
    if isinstance(default, list) and not isinstance(val, list):
        return default
    if isinstance(default, dict) and not isinstance(val, dict):
        return default
    return val


def _post(s, url, body):
    """POST a JSON chain API with bounded transient retry.

    Treats an EMPTY / non-JSON 200 body as RETRYABLE — under bulk load the GST
    portal returns HTTP 200 with an empty body, which previously slipped
    through (200 == success) and then crashed json.loads downstream. We now
    re-attempt; if every attempt is still empty, the caller's _loads() turns it
    into an empty list/dict (treated as "no notices") rather than an error."""
    last = (0, "")
    for attempt in range(1, _RETRY_ATTEMPTS + 1):
        try:
            r = s.post(url, json=body, timeout=45, headers={
                "Content-Type": "application/json;charset=UTF-8",
                "Accept": "application/json, text/plain, */*",
                "Origin": SERVICES, "Referer": f"{SERVICES}/services/auth/notices",
                "at": getattr(s, "_at", ""),
            })
            last = (r.status_code, r.text)
            if r.status_code == 200 and _looks_like_json(r.text):
                return last
            if 400 <= r.status_code < 500:
                return last  # genuine client error — don't retry
            # 200-but-empty/non-JSON, or 5xx → fall through to retry
        except requests.RequestException as e:
            last = (0, str(e))
        if attempt < _RETRY_ATTEMPTS:
            time.sleep(_RETRY_BACKOFF * attempt)
    return last


def _download(s, url, referer):
    """GET a document with bounded transient retry."""
    last = (0, "", b"")
    for attempt in range(1, _RETRY_ATTEMPTS + 1):
        try:
            r = s.get(url, timeout=60, headers={"Accept": "application/pdf,*/*",
                                                "Referer": referer})
            last = (r.status_code, r.headers.get("Content-Type", ""), r.content)
            if r.status_code == 200 and r.content:
                return last
            if 400 <= r.status_code < 500:
                return last
        except requests.RequestException:
            last = (0, "", b"")
        if attempt < _RETRY_ATTEMPTS:
            time.sleep(_RETRY_BACKOFF * attempt)
    return last


def _upload(data, org_id, client_id, ref_id, doc_name, mime):
    ext = _doc_ext(data, mime, doc_name)
    if ext is None or not data:
        return None
    stem = os.path.splitext(doc_name)[0] or doc_name or "doc"
    file_name = _safe_name(stem) + ext
    key = (f"{GST_API_S3_ROOT}/{org_id}/{_safe_name(client_id)}/"
           f"{_safe_name(ref_id)}/{file_name}")
    if not upload_file_to_s3(data, key, _content_type_for(ext)):
        return None
    return {"file_url": key, "file_name": file_name}


def _gstin_from(notices, fallback):
    for row in notices:
        for k in ("gstin", "gstIn", "gstinId"):
            v = row.get(k) if isinstance(row, dict) else None
            if v:
                return v
    return fallback or ""


# GSTIN check-digit (mod-36) — the 15th char is a checksum over the first 14,
# so given the state code + the login username (= the GSTIN's PAN+entity
# middle) the full valid GSTIN is reconstructable WITHOUT storing it.
_GSTIN_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"


def _gstin_check_digit(first14):
    factor, total, cp_len = 2, 0, len(_GSTIN_ALPHABET)
    for ch in reversed(first14.upper()):
        addend = factor * _GSTIN_ALPHABET.index(ch)
        addend = (addend // cp_len) + (addend % cp_len)
        total += addend
        factor = 1 if factor == 2 else 2
    return _GSTIN_ALPHABET[(cp_len - (total % cp_len)) % cp_len]


def _gstin_from_username(username, state_cd="06"):
    u = (username or "").strip().upper()
    if not re.fullmatch(r"[A-Z]{5}[0-9]{4}[A-Z][0-9A-Z]", u):
        return None
    first14 = "%s%sZ" % (state_cd, u)
    return first14 + _gstin_check_digit(first14)


def _resolve_session_gstin(s, notices, username, db_gstin):
    """Resolve the session's GSTIN without requiring it to be stored:
    row/db value first, else reconstruct from the username and verify
    against the case-task list, scanning state codes until accepted."""
    g = _gstin_from(notices, db_gstin)
    if g:
        return g
    seen = set()
    for state in ["06"] + ["%02d" % n for n in range(1, 39)]:
        cand = _gstin_from_username(username, state)
        if not cand or cand in seen:
            continue
        seen.add(cand)
        st2, body2 = _post(
            s, f"{SERVICES}/litserv/auth/api/case/task/get", {"gstIn": cand})
        if st2 == 200 and isinstance(_loads(body2, None), list):
            return cand
    return ""


# ---------------------------------------------------------------------------
# Core fetch — verbatim port of fetch_gst_notices_requests (3-phase parallel).
# `org_id` comes from the event (the lambda does not touch the DB).
# ---------------------------------------------------------------------------

def process_gst_notices(client_name, username, password, org_id, gstin_db=None,
                        concurrency=None):
    concurrency = concurrency or _GST_FILE_DOWNLOAD_CONCURRENCY
    stats = {"notices": 0, "additional": 0, "gstr3a": 0,
             "files_uploaded": 0, "failures": []}
    out_notices = []
    out_additional = []

    if not org_id:
        return {"success": False, "error": "organization_id missing in event",
                "response": {}, "stats": stats}

    s = _login(username, password)
    if not s:
        return {"success": False, "error": "GST requests-login failed",
                "response": {}, "stats": stats}

    # Phase 0 — list APIs. _loads is safe: an empty/non-JSON 200 (portal
    # overload) becomes [] instead of crashing the whole client fetch.
    st, body = _post(s, f"{SERVICES}/services/auth/api/get/notices", {"onLoad": True})
    notices = _loads(body, []) if st == 200 else []
    # GSTIN is resolved FROM THE SESSION — NOT a stored requirement. The
    # login username is the GSTIN's PAN+entity middle, so the full GSTIN is
    # reconstructed (checksum computed, state code verified) when no row/event
    # value is present. (Earlier this silently produced "0 additional notices"
    # when the event gstin was blank.)
    gstin = _resolve_session_gstin(s, notices, username, gstin_db)
    if not gstin:
        st2, case_tasks = None, []
        stats["failures"].append(
            "Could not resolve this client's GSTIN from the session — "
            "additional-notices list skipped"
        )
    else:
        st2, body2 = _post(s, f"{SERVICES}/litserv/auth/api/case/task/get", {"gstIn": gstin})
        case_tasks = _loads(body2, []) if st2 == 200 else []
        # The portal reports errors INSIDE a 200 as an object with an
        # `error` key — never treat that as an empty list silently.
        if isinstance(case_tasks, dict):
            err = (case_tasks.get("error") or {})
            stats["failures"].append(
                "case/task/get returned an error: "
                f"{err.get('error_cd') or ''} {err.get('message') or ''}".strip()
            )
            case_tasks = []
    # Observability: a non-200 here used to SILENTLY produce "0 additional
    # notices" while the run still reported success — indistinguishable from
    # "the portal genuinely has none". Record the list call's status + counts
    # (surfaced on the timing page) and a failure line on errors.
    stats["notices_listed"] = len(notices)
    stats["case_task_http"] = st2
    stats["case_tasks_listed"] = len(case_tasks)
    if st2 is not None and st2 != 200:
        stats["failures"].append(
            f"case/task/get HTTP {st2} — additional-notices list unavailable"
        )
    if not notices and not case_tasks:
        # Login SUCCEEDED but the portal has no notices for this client. This is
        # a genuine zero-notice result, NOT a failure — return Success with
        # empty lists so the client is not logged to the Failed Login tab and
        # the timing page shows Success (0 notices) rather than Failed. Real
        # login/HTTP errors are handled earlier (`_login` -> success:False).
        stats["per_notice"] = []
        stats["engine"] = "requests"
        stats["no_notices"] = True
        return {
            "success": True, "error": None,
            "msg": "GST notices fetched (no notices on portal)",
            "client_name": client_name,
            "username": gstin,
            "notices": [],
            "additional_notices": [],
            "response": {
                "client_name": client_name, "username": gstin,
                "notices": [], "additional_notices": [],
            },
            "stats": stats,
        }

    # Folder → group classification. The GST portal decides a folder's real
    # category from the case-type + folder CODE (its caseconfig.json), NOT the
    # folder's display name — e.g. for a DRC-03 Voluntary-Payment case
    # (caseTypeCd "ADJVP") the "ORDRS" folder renders orderVp.html, which is an
    # ACKNOWLEDGEMENT, not a normal order. Classifying purely on the folder NAME
    # ("ORDERS" → orders) mis-filed those acks under Orders. So classify on the
    # folder CODE, with the case-type-specific overrides the portal itself uses.
    _FOLDER_CODE_TO_GROUP = {
        "NOTCE": "notices", "NOTAC": "notices",
        "REPLY": "replies",
        "ORDRS": "orders",
        "INTIM": "intimations", "INORD": "intimations",
        "APLCN": "applications",
        "ACKIN": "ack_intimations",
    }
    _FOLDER_OVERRIDE = {
        ("ADJVP", "ORDRS"): "ack_intimations",
    }
    _FOLDER_NAME_TO_GROUP = {
        "NOTICES": "notices", "REPLIES": "replies", "ORDERS": "orders",
        "INTIMATIONS": "intimations", "APPLICATIONS": "applications",
        "ACK./INTIMATION": "ack_intimations", "ACK/INTIMATION": "ack_intimations",
    }

    def _classify_folder(case_type_cd, folder_code, folder_name):
        """Map a case folder to our group using the portal's own logic:
        case-type override first, then folder CODE, then folder NAME."""
        fc = (folder_code or "").strip().upper()
        ct = (case_type_cd or "").strip().upper()
        if (ct, fc) in _FOLDER_OVERRIDE:
            return _FOLDER_OVERRIDE[(ct, fc)]
        if fc in _FOLDER_CODE_TO_GROUP:
            return _FOLDER_CODE_TO_GROUP[fc]
        return _FOLDER_NAME_TO_GROUP.get((folder_name or "").strip().upper(), "notices")

    # A document descriptor (dcupdtls) carries a type CODE in `ty`. The portal
    # UI renders a human "Type of Documents" label from it (adjudicationctrl.js),
    # e.g. ty "REGC" → "Intimation Of Voluntary Payment DRC-03". Port the known
    # codes; unknown codes fall back to the folder name so Type is never blank.
    _DOC_TY_TO_LABEL = {
        "REGC": "Intimation Of Voluntary Payment DRC-03",
    }

    def _doc_type_label(ty, folder_name, item_type=""):
        """Prefer the itemJson's own `type` text (e.g. "Issue Acknowledgement"),
        then a mapped `ty` code, then the folder name as a last resort."""
        if item_type and str(item_type).strip():
            return str(item_type).strip()
        t = (ty or "").strip().upper()
        if t in _DOC_TY_TO_LABEL:
            return _DOC_TY_TO_LABEL[t]
        return (folder_name or "").strip() or (ty or "")

    def _do_normal(row):
        oid = row.get("noticeOrderId") or "x"
        doc_id, appln = row.get("docId"), row.get("applnId")
        notice_letter, fails, files = None, [], 0
        if doc_id and appln:
            st_, mime_, data_ = _download(
                s, f"{SERVICES}/document/{doc_id}/{appln}",
                f"{SERVICES}/services/auth/notices")
            if st_ == 200 and data_:
                notice_letter = _upload(data_, org_id, client_name, oid,
                                        f"{oid}.pdf", mime_ or "application/pdf")
                files = 1 if notice_letter else 0
            else:
                fails.append(f"phase1 {oid}: HTTP {st_}")
        return {"kind": "notice", "files": files, "fails": fails, "row": {
            "ref_id": oid, "type": row.get("type"), "issued_by": row.get("issuedBy"),
            "description": row.get("descr") or "",
            "issue_date": _clean_date(row.get("dtOfIssue")),
            "due_date": _clean_date(row.get("dueDate")),
            "amount": _clean_amount(row.get("amount", 0)),
            "appln_id": appln, "doc_id": doc_id, "notice_letter": notice_letter}}

    def _do_gstr3a(row):
        oid, appdef, dt = row.get("noticeOrderId"), row.get("appDefId"), row.get("dtOfIssue")
        if not (oid and appdef and dt):
            return None
        r = None
        for attempt in range(1, _RETRY_ATTEMPTS + 1):
            try:
                r = s.get(f"{RETURN}/returns/auth/api/gstr3a/summary",
                          params={"defaulter_id": appdef, "order_id": oid}, timeout=30,
                          headers={"Accept": "application/json, text/plain, */*",
                                   "Referer": f"{RETURN}/returns/auth/gstr3a"})
                if r.status_code == 200 and "json" in (r.headers.get("Content-Type") or ""):
                    break
                if 400 <= r.status_code < 500:
                    break
            except requests.RequestException:
                r = None
            if attempt < _RETRY_ATTEMPTS:
                time.sleep(_RETRY_BACKOFF * attempt)
        if r is None or r.status_code != 200 or "json" not in (r.headers.get("Content-Type") or ""):
            return {"kind": "gstr3a", "files": 0,
                    "fails": [f"gstr3a {oid}: summary HTTP {getattr(r,'status_code','-')}"], "row": None}
        summary = (r.json() or {}).get("data")
        if not summary:
            return None
        try:
            pdf = build_gstr3a_pdf(summary, dt)
        except Exception as e:
            return {"kind": "gstr3a", "files": 0, "fails": [f"gstr3a {oid}: pdf {e}"], "row": None}
        nl = _upload(pdf, org_id, client_name, oid, f"{oid}.pdf", "application/pdf")
        return {"kind": "gstr3a", "files": 1 if nl else 0, "fails": [], "row": {
            "ref_id": oid, "type": row.get("type") or "GSTR-3A",
            "issued_by": row.get("issuedBy"), "description": row.get("descr") or "",
            "issue_date": _clean_date(dt), "due_date": _clean_date(row.get("dueDate")),
            "amount": 0.0, "appln_id": None, "doc_id": None, "notice_letter": nl}}

    def _do_case_task(row):
        ref, cid, arn = row.get("refId"), row.get("caseId"), row.get("arn")
        ctype, gid = row.get("caseTpeCd"), row.get("gstIn")
        groups = {k: [] for k in ("notices", "replies", "orders",
                                  "intimations", "applications", "ack_intimations")}
        files = 0
        if cid and arn and ctype and gid:
            st_, body_ = _post(s, f"{SERVICES}/litserv/auth/api/case/folder",
                               {"caseId": cid, "gstid": gid, "caseTypeCd": ctype})
            flat = []
            if st_ == 200:
                for f in _loads(body_, []):
                    fname = f.get("caseFolderTypeName", "?")
                    fcode = f.get("caseFolderTypeCd", "")
                    # Classify the folder ONCE (case-type + folder code aware) so
                    # e.g. an ADJVP "ORDRS" ack lands in ack_intimations, not orders.
                    grp = _classify_folder(ctype, fcode, fname)
                    s2, b2 = _post(s, f"{SERVICES}/litserv/auth/api/case/folder/items",
                                   {"caseFolderId": f.get("caseFolderId")})
                    if s2 != 200:
                        continue
                    for it in _loads(b2, []):
                        ij = it.get("itemJson", "")
                        if isinstance(ij, str) and ij:
                            # itemJson sometimes carries a ready human doc-type
                            # in its top-level "type" (e.g. "Issue Acknowledgement").
                            item_type = ""
                            try:
                                item_type = (_loads(ij, {}) or {}).get("type") or ""
                            except Exception:
                                item_type = ""
                            # Notice-level dates + section from the itemJson.
                            dates = _parse_item_dates(ij)
                            for d in _parse_doc_descriptors(ij):
                                if d.get("id") and d.get("docName"):
                                    dtype = _doc_type_label(d.get("ty"), fname, item_type)
                                    flat.append((fname, grp, d, dtype, dates))
            if flat:
                ids = list({str(d["id"]) for _, _, d, _, _ in flat})
                s3_, b3 = _post(s, f"{SERVICES}/litserv/auth/api/usr/getEncrypDocIds",
                                {"arn": arn, "docIdList": ids})
                enc = _loads(b3, {}) if s3_ == 200 else {}

                def fetch_one(item):
                    fname, grp, d, dtype, dates = item
                    eh = enc.get(str(d.get("id")))
                    if not eh:
                        return None
                    url = f"{SERVICES}/downloadhb/download/new?docId={d['id']}&arn={arn}&eh={eh}"
                    st2_, mime2, data2 = _download(s, url, f"{SERVICES}/services/auth/notices")
                    ext = _doc_ext(data2, mime2, d.get("docName") or "")
                    if not (st2_ == 200 and data2 and ext is not None):
                        return None
                    att = _upload(data2, org_id, client_name, ref,
                                  d.get("docName") or str(d["id"]), mime2)
                    return (fname, grp, d, dtype, att, dates) if att else None

                with ThreadPoolExecutor(max_workers=max(1, concurrency)) as ex:
                    for res in ex.map(fetch_one, flat):
                        if not res:
                            continue
                        fname, grp, d, dtype, att, dates = res
                        # `type` keeps the folder name (back-compat); `document_type`
                        # is the portal's human "Type of Documents" label.
                        item = {"type": fname,
                                "document_type": dtype,
                                "attachments": [att]}
                        nm = d.get("docName")
                        if grp == "replies":
                            item["arn"] = nm
                            item["reply_date"] = _clean_date(dates.get("issue_date"))
                        elif grp == "orders":
                            item["order_number"] = nm
                            item["order_date"] = _clean_date(dates.get("issue_date"))
                        else:
                            item["reference_number"] = nm
                            # Notice-level dates from the itemJson.
                            item["issue_date"] = _clean_date(dates.get("issue_date"))
                            item["due_date"] = _clean_date(dates.get("due_date"))
                            item["section"] = dates.get("section") or ""
                        groups[grp].append(item)
                        files += 1
        return {"kind": "additional", "files": files, "fails": [], "row": {
            "ref_id": ref, "arn": arn, "type": row.get("caseTypeName") or ctype,
            "issue_date": _clean_date(
                row.get("assignmentDt") or row.get("assgnDtStr") or row.get("dtOfIssue")
            ),
            "description": row.get("taskDesc") or "",
            "case_details": {
                "reference_number": arn, "case_id": str(cid or ""), "gstin": gid,
                "case_creation_date": _clean_date(
                    row.get("assignmentDt") or row.get("insertTimeStamp")
                    or row.get("assgnDtStr")
                ),
                "status": row.get("status"), **groups}}}

    # Build the row-task list: Normal notices + GSTR-3A + Additional case-tasks
    # all fan out in ONE pool (the validated parallel-fetch flow).
    tasks = []
    for row in notices:
        tasks.append((_do_gstr3a, row) if row.get("applnCd") == "APL3A"
                     else (_do_normal, row))
    for row in case_tasks:
        tasks.append((_do_case_task, row))

    def _timed(fn, row):
        _t = time.monotonic()
        out = fn(row)
        if out is not None:
            out["seconds"] = round(time.monotonic() - _t, 2)
        return out

    row_workers = max(1, concurrency)
    results = []
    phase_t0 = time.monotonic()
    files_count = 0
    # Keep the portal session warm for the duration of the download pool so it
    # doesn't expire mid-fetch (background thread pings the keep-alive endpoint).
    _keepalive = _KeepAliveThread(s)
    _keepalive.start()
    try:
        with ThreadPoolExecutor(max_workers=row_workers) as ex:
            futures = [ex.submit(_timed, fn, row) for fn, row in tasks]
            for fut in as_completed(futures):
                try:
                    r = fut.result()
                    if r is not None:
                        results.append(r)
                        files_count += r.get("files", 0)
                except Exception as e:
                    stats["failures"].append(f"row task error: {e}")
    finally:
        _keepalive.stop()
    stats["fetch_seconds"] = round(time.monotonic() - phase_t0, 2)

    per_notice = []
    for r in results:
        stats["files_uploaded"] += r.get("files", 0)
        stats["failures"].extend(r.get("fails", []))
        row_out = r.get("row")
        ref = (row_out or {}).get("ref_id", "?")
        per_notice.append({"ref_id": ref, "kind": r["kind"],
                           "files": r.get("files", 0),
                           "seconds": r.get("seconds")})
        if r["kind"] == "additional":
            stats["additional"] += 1
            if row_out:
                out_additional.append(row_out)
        elif r["kind"] == "gstr3a":
            if row_out:
                out_notices.append(row_out)
                stats["gstr3a"] += 1
        else:
            stats["notices"] += 1
            if row_out:
                out_notices.append(row_out)
    stats["per_notice"] = per_notice
    stats["engine"] = "requests"

    return {
        "success": True, "error": None,
        "msg": "GST notices fetched",
        "client_name": client_name,
        "username": gstin,
        "notices": out_notices,
        "additional_notices": out_additional,
        # Also nest under `response` so a receiver expecting either shape works
        # (the Selenium worker returns top-level notices/additional_notices).
        "response": {
            "client_name": client_name, "username": gstin,
            "notices": out_notices, "additional_notices": out_additional,
        },
        "stats": stats,
    }


# ---------------------------------------------------------------------------
# Worker webhook — identical contract to the Selenium worker.
# ---------------------------------------------------------------------------

def send_worker_webhook(webhook_config, client_name, portal_type, result, execution_time):
    webhook_config = webhook_config or {}
    callback_url = webhook_config.get("worker_callback_url")
    base_url = webhook_config.get("url")
    if not callback_url and not base_url:
        env_base = (os.environ.get("WEBHOOK_BASE_URL") or "").strip().rstrip("/")
        if env_base:
            base_url = env_base
            logger.info("webhook_config missing url; using WEBHOOK_BASE_URL env for %s", client_name)
    if not callback_url and not base_url:
        logger.error("No webhook URL for %s; skipping callback.", client_name)
        return False
    if not callback_url:
        callback_url = base_url.rstrip("/") + "/api/method/fin_buddy.features.lambda_webhooks.update_worker_result"
    headers = {"Content-Type": "application/json"}
    if webhook_config.get("api_key") and webhook_config.get("api_secret"):
        headers["Authorization"] = f"token {webhook_config['api_key']}:{webhook_config['api_secret']}"
    payload = {
        "log_name": webhook_config.get("log_name", ""),
        "client_name": client_name,
        "portal_type": portal_type,
        "result": json.dumps(result, default=str),
        "execution_time": execution_time,
    }
    for attempt in range(2):
        try:
            response = requests.post(callback_url, headers=headers, json=payload, timeout=120)
            if response.status_code == 200:
                logger.info("Webhook sent successfully for %s", client_name)
                return True
            logger.error("Webhook failed for %s: %s", client_name, response.status_code)
        except Exception as e:
            logger.error("Webhook error for %s attempt %s: %s", client_name, attempt + 1, str(e))
        if attempt < 1:
            time.sleep(5)
    return False


# ---------------------------------------------------------------------------
# Handler — SAME event/return contract as the Selenium worker.
# ---------------------------------------------------------------------------

def lambda_handler(event, context):
    """AWS Lambda handler — API-based GST notice fetch.

    Event:
      {username, password, client_name, organization_id, gstin?,
       gst_file_download_concurrency?, webhook_config?}
    """
    handler_start_time = time.time()
    try:
        # Apply S3 credentials/config from the event (System Configuration)
        # before any upload.
        configure_s3(event.get("s3_config"))

        username = event.get("username")
        password = event.get("password")
        client_name = event.get("client_name")
        org_id = event.get("organization_id") or event.get("org_id")
        gstin_db = event.get("gstin")

        concurrency = None
        try:
            requested = int(event.get("gst_file_download_concurrency") or 0)
            if requested > 0:
                concurrency = max(1, min(requested, 100))
        except (TypeError, ValueError):
            logger.warning("ignoring invalid gst_file_download_concurrency=%r",
                           event.get("gst_file_download_concurrency"))

        if not username or not password or not client_name:
            return {"statusCode": 400, "body": json.dumps({
                "error": "Missing required parameters: username, password, or client_name"})}
        if not org_id:
            return {"statusCode": 400, "body": json.dumps({
                "error": "Missing required parameter: organization_id"})}

        logger.info("Starting API GST notice fetch for client: %s", client_name)
        result = process_gst_notices(client_name, username, password, org_id,
                                     gstin_db=gstin_db, concurrency=concurrency)

        response = {
            "statusCode": 200 if result["success"] else 401,
            "body": json.dumps(result, default=str),
        }

        webhook_config = event.get("webhook_config")
        if webhook_config:
            execution_time = round(time.time() - handler_start_time, 2)
            worker_result = {
                "success": True,
                "client_info": {"client_name": client_name, "portal": "gst", "username": username},
                "function_name": "fetch_gst_notices_api_lambda",
                "response": response,
                "status_code": response["statusCode"],
                "execution_time_seconds": execution_time,
            }
            send_worker_webhook(webhook_config, client_name, "gst", worker_result, execution_time)

        return response

    except Exception as e:
        logger.error("Lambda handler error: %s", str(e))
        logger.error(traceback.format_exc())
        error_response = {"statusCode": 500, "body": json.dumps({
            "error": str(e), "traceback": traceback.format_exc()})}
        webhook_config = event.get("webhook_config")
        if webhook_config:
            execution_time = round(time.time() - handler_start_time, 2)
            worker_result = {
                "success": False,
                "client_info": {"client_name": event.get("client_name", "Unknown"), "portal": "gst"},
                "function_name": "fetch_gst_notices_api_lambda",
                "response": error_response,
                "error": str(e),
                "execution_time_seconds": execution_time,
            }
            send_worker_webhook(webhook_config, event.get("client_name", "Unknown"), "gst",
                                worker_result, execution_time)
        return error_response


if __name__ == "__main__":
    test_event = {
        "username": "27GSTIN1234567",
        "password": "test_password",
        "client_name": "GST-CLT-001",
        "organization_id": "org-test",
    }
    print(json.dumps(lambda_handler(test_event, None), indent=2, default=str))
