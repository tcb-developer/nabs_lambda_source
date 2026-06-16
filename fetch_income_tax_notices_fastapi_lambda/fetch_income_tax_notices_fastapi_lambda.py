import requests
from requests.adapters import HTTPAdapter
try:
    from urllib3.util.retry import Retry
except Exception:  # very old urllib3 layout
    from requests.packages.urllib3.util.retry import Retry
import base64
import time
from datetime import datetime, timezone
import os
import json
import traceback
import logging
import boto3
from botocore.client import Config
import mimetypes
import random
import string
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# constants
base_url = "https://eportal.incometax.gov.in/iec"

headers = {"Accept": "application/json, text/plain, */*", "Content-Type": "application/json", 'User-Agent': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36", "Host": "eportal.incometax.gov.in", "Referer":"https://eportal.incometax.gov.in/iec/foservices/", "Sec-Ch-Ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"', "Sec-Fetch-Mode": "cors", "Sec-Fetch-Mode": "same-origin", "Sec-Ch-Ua-Platform": "Linux", "Sec-Ch-Ua-Mobile": "?0"}

# (connect, read) timeout. A short CONNECT timeout means a dropped/refused
# connection fails fast (~6s) instead of hanging on the old single 30s value;
# the read timeout stays generous for slow document streaming.
timeout_sec = (6, 45)

MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds between retries

# ---------------------------------------------------------------------------
# Shared HTTP session — connection pooling + keep-alive.
#
# The Income Tax portal closes idle/new connections aggressively
# ("RemoteDisconnected"). The worker previously used bare requests.get/post,
# which opened a fresh TCP+TLS connection for EVERY call (login, list, and
# every /document/... download). Each rejected connect could hang up to the
# old 30s timeout before retrying — the dominant cost of a slow fetch.
#
# A single pooled Session reuses connections (keep-alive) across all calls,
# so the portal sees far fewer new-connection attempts and the
# RemoteDisconnected storm collapses. urllib3 Retry adds transport-level
# retries (with backoff) for exactly the dropped-connection / 5xx cases,
# on TOP of the existing app-level retry loops (belt-and-braces).
#
# pool_maxsize is sized above the per-notice ThreadPoolExecutor worker count
# (min(5,...)) so concurrent downloads don't contend for a single connection.
# ---------------------------------------------------------------------------
_http_session = None
_http_session_lock = threading.Lock()


def http_session():
    """Get or create the shared pooled requests.Session (thread-safe init)."""
    global _http_session
    if _http_session is None:
        with _http_session_lock:
            if _http_session is None:
                s = requests.Session()
                # The retried-methods kwarg was renamed `method_whitelist` →
                # `allowed_methods` in urllib3 1.26. The lambda runtime layer
                # may ship either, so try the new name and fall back.
                _retry_kw = dict(
                    total=2,                 # transport-level retries (app loops add more)
                    connect=2,
                    read=2,
                    backoff_factor=1,        # 0s, 1s, 2s between transport retries
                    status_forcelist=(500, 502, 503, 504),
                    raise_on_status=False,
                )
                try:
                    retry = Retry(allowed_methods=frozenset(["GET", "POST"]), **_retry_kw)
                except TypeError:
                    retry = Retry(method_whitelist=frozenset(["GET", "POST"]), **_retry_kw)
                adapter = HTTPAdapter(
                    pool_connections=10,
                    pool_maxsize=20,
                    max_retries=retry,
                )
                s.mount("https://", adapter)
                s.mount("http://", adapter)
                _http_session = s
    return _http_session

# AWS S3 Configuration. Credentials are NOT hardcoded — they come from the
# Lambda execution role by default (the role carries finbuddy-lambda-s3-policy).
# Explicit keys are honoured only if AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
# are set as env vars (local/dev), otherwise boto3 resolves the role creds.
AWS_REGION = os.environ.get("AWS_REGION", "ap-south-1")
S3_BUCKET = os.environ.get("S3_BUCKET_NAME", "nabsprodbucket")

# Shared S3 client (reused across uploads to avoid per-call initialization overhead)
_s3_client = None

def _get_s3_client():
    """Get or create a shared S3 client (saves ~0.5s per upload vs creating new client each time).
    Uses the Lambda execution role's credentials unless explicit AWS keys are
    provided via env vars — never hardcoded."""
    global _s3_client
    if _s3_client is None:
        kwargs = {"region_name": AWS_REGION, "config": Config(signature_version='s3v4')}
        ak = os.environ.get("AWS_ACCESS_KEY_ID")
        sk = os.environ.get("AWS_SECRET_ACCESS_KEY")
        if ak and sk:
            kwargs["aws_access_key_id"] = ak
            kwargs["aws_secret_access_key"] = sk
        _s3_client = boto3.client('s3', **kwargs)
    return _s3_client


def retry_request(method, url, max_retries=MAX_RETRIES, **kwargs):
    """Make an HTTP request with retry logic for transient failures."""
    kwargs.setdefault('timeout', timeout_sec)
    last_error = None
    _sess = http_session()
    for attempt in range(1, max_retries + 1):
        try:
            if method == 'post':
                resp = _sess.post(url, **kwargs)
            else:
                resp = _sess.get(url, **kwargs)
            resp.raise_for_status()
            return resp
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, requests.exceptions.HTTPError) as e:
            last_error = e
            if attempt < max_retries:
                logger.warning(f"Request attempt {attempt}/{max_retries} failed for {url}: {str(e)}. Retrying in {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY * attempt)  # exponential backoff
            else:
                logger.error(f"Request failed after {max_retries} attempts for {url}: {str(e)}")
                raise


def make_date(milliseconds, frappe_date=None):
    """Convert milliseconds timestamp to date string"""
    second = int(milliseconds) / 1000
    if frappe_date:
        return datetime.fromtimestamp(second).strftime('%Y-%m-%d')
    else:
        return datetime.fromtimestamp(second).strftime('%d-%b-%Y')


def timestamp_to_datetime(timestamp):
    """Convert timestamp to datetime object"""
    try:
        timestamp = int(timestamp)
        dt_obj = datetime.fromtimestamp(timestamp / 1000)
        return dt_obj
    except Exception as e:
        logger.error(f"Error converting timestamp to datetime: {e}")
        return None


def date_from_timestamp(timestamp):
    """Convert timestamp to date"""
    try:
        timestamp = int(timestamp)
        if timestamp:
            dt_obj = timestamp_to_datetime(timestamp)
            if dt_obj:
                return dt_obj.date().strftime('%Y-%m-%d')
        return None
    except Exception:
        return None


def key_generator(file_name):
    """Generate unique S3 key for file"""
    file_name = file_name.replace(" ", "_")

    regex = re.compile('[^0-9a-zA-Z._-]')
    file_name = regex.sub('', file_name)

    key = "".join(
        random.choice(string.ascii_uppercase + string.digits) for _ in range(8)
    )

    today = datetime.now()
    year = today.strftime("%Y")
    month = today.strftime("%m")
    day = today.strftime("%d")

    final_key = (
        year
        + "/"
        + month
        + "/"
        + day
        + "/"
        + key
        + "_"
        + file_name
    )
    return final_key


def upload_to_s3(file_path, file_name="", cleanup=True):
    """Upload file to S3 and return file info with presigned URL.
    Uses shared S3 client for performance. Optionally deletes local file after upload."""
    file_url = None

    content_type, _ = mimetypes.guess_type(file_path)
    if not content_type:
        content_type = "application/octet-stream"

    s3_client = _get_s3_client()

    if not file_name:
        file_name = os.path.basename(file_path)
    key = key_generator(file_name)

    try:
        s3_client.upload_file(
            file_path, S3_BUCKET, key,
            ExtraArgs={
                "ContentType": content_type,
                "Metadata": {
                    "ContentType": content_type,
                    "file_name": file_name
                }
            }
        )

        params = {
            'Bucket': S3_BUCKET,
            'Key': key,
        }

        file_url = s3_client.generate_presigned_url(
            'get_object',
            Params=params,
            ExpiresIn=604799,
        )

        # Get file size for proper File record
        try:
            file_size = os.path.getsize(file_path)
        except OSError:
            file_size = 0

        # Clean up local file to free /tmp space
        if cleanup:
            try:
                os.remove(file_path)
            except OSError:
                pass
    except Exception as e:
        logger.error(f"Error uploading to S3: {str(e)}")
        file_size = 0

    return {"file_name": file_name, "file_url": file_url, "content_hash": key, "s3_key": key, "file_size": file_size}


def login_user_via_apis(username, password, client_name):
    """Login to Income Tax portal and return session cookies"""
    main_response = None
    full_name = None
    errors = []

    try:
        # Step 1: Verify username
        res_1 = http_session().post(
            f"{base_url}/loginapi/login",
            headers=headers, timeout=timeout_sec,
            json={
                "entity": username, "serviceName": "wLoginService"
            })

        res_1.raise_for_status()
        res_1_data = res_1.json()

        if res_1_data.get('messages') and res_1_data.get('messages')[0].get('type') == "ERROR":
            error_msg = res_1_data.get('messages')[0].get('desc', 'Username Verifying API Error')
            logger.error(error_msg)
            errors.append({"step": "username_verification", "error": error_msg})
            return None, errors

        # Step 2: Login with password
        login_data = res_1_data.copy()
        login_data.pop('header', None)
        login_data.pop('messages', None)
        login_data.update({
            'pass': base64.b64encode(password.encode()).decode(),
            'passValdtnFlg': None,
            'serviceName': "loginService",
            'imagePath': None,
            'imgByte': None
        })

        # The Income Tax portal needs a moment to process the reqId from step 1
        # before the password login is accepted, AND the login POST is
        # single-use: it consumes that reqId. A previous "adaptive poll" here
        # re-POSTed the same login_data several times — but the FIRST POST
        # consumes the reqId, so every retry hit "Request is not authenticated"
        # and clobbered the real (empty) first response, failing EVERY client.
        # Keep the shared lambda's behaviour verbatim: wait once, POST once.
        time.sleep(12)  # Wait for Income Tax API to process reqId

        main_response = http_session().post(
            f"{base_url}/loginapi/login",
            headers=headers,
            timeout=timeout_sec,
            json=login_data
        )

        main_response.raise_for_status()
        res_2_data = main_response.json()
        full_name = res_2_data.get('fullName')
        msg_body = res_2_data.get('messages', [])

        # Handle "Session already active" case
        if not full_name and msg_body:
            if any(msg.get('desc') == 'Session already active' for msg in msg_body):
                continue_data = {
                    "aadhaarMobileValidated": res_2_data.get('aadhaarMobileValidated', "false"),
                    "clientIp": res_2_data.get('clientIp', ""),
                    "contactEmail": res_2_data.get('contactEmail', ""),
                    "contactMobile": res_2_data.get('contactMobile', ""),
                    "contactPan": res_2_data.get('contactPan', ""),
                    "contactResCd": res_2_data.get('contactResCd', ""),
                    "dtoService": "LOGIN",
                    "email": res_2_data.get('email', ""),
                    "entity": username,
                    "entityType": res_2_data.get('entityType', "PAN"),
                    "exemptedPan": res_2_data.get('exemptedPan', "false"),
                    "forgnDirEmailId": "",
                    "lastLoginSuccessFlag": "true",
                    "mobileNo": res_2_data.get('mobileNo', ""),
                    "otpGenerationFlag": "true",
                    "otpValdtnFlg": "true",
                    "pass": None,
                    "passValdtnFlg": "true",
                    "remark": "Continue",
                    "reqId": res_2_data.get('reqId', ""),
                    "role": res_2_data.get('role', ""),
                    "secAccssMsg": "",
                    "secLoginOptions": "",
                    "serviceName": "loginService",
                    "uidValdtnFlg": "true",
                    "userConsent": "",
                    "userType": res_2_data.get('userType', "")
                }

                logger.info("Session already active, creating new session")

                main_response = http_session().post(
                    f"{base_url}/loginapi/login",
                    headers=headers,
                    timeout=timeout_sec,
                    json=continue_data
                )
                main_response.raise_for_status()
                res_3_data = main_response.json()
                full_name = res_3_data.get('fullName')

        if main_response and full_name:
            logger.info(f"Login successful for {full_name}")
            return main_response, []
        else:
            error_msg = "Login Failed, due to some Income Tax APIs. Try Again Later!"
            if msg_body:
                error_msg = msg_body[0].get('desc', error_msg)

            logger.error(error_msg)
            errors.append({"step": "login", "error": error_msg})
            return None, errors

    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP Error during login: {e}")
        errors.append({"step": "login", "error": f"HTTP Error: {str(e)}"})
        return None, errors
    except Exception as e:
        logger.error(f"Error during login: {e}")
        errors.append({"step": "login", "error": str(e)})
        return None, errors


def refresh_login_cookies(login_cookies, username, password, client_name):
    """Re-login if session expired, returns new cookies or original if re-login fails"""
    try:
        new_cookies, login_errors = login_user_via_apis(username, password, client_name)
        if new_cookies:
            logger.info(f"Session refreshed successfully for {client_name}")
            return new_cookies
    except Exception as e:
        logger.error(f"Session refresh failed for {client_name}: {e}")
    return login_cookies


def _download_one_document(doc_id, link_text, full_file_path, file_info,
                           cookies_box, dl_lock, username, password,
                           client_name, label="file"):
    """Download a SINGLE document by id and upload it to S3, mutating + returning
    `file_info`. Shared by the reply-file and notice-file paths (previously two
    ~identical inline loops) so file-level parallelism can call it from a pool.

    Thread-safety: only reads the shared `cookies_box["cookies"]`, and on a
    session refresh swaps it under `dl_lock`. It does NOT touch any caller list
    (reply_files / notice_letter assignment stays on the calling thread), so
    concurrent calls for different files of one notice don't race.

    Mirrors the original per-file retry semantics verbatim: 3 attempts, 401/403
    → refresh-login-and-retry, non-200 → backoff-and-retry, success → save +
    S3 upload.
    """
    if not full_file_path:
        file_info['downloaded'] = False
        file_info['skipped'] = True
        return file_info

    download_success = False
    for dl_attempt in range(1, 4):  # 3 attempts
        try:
            file_res = http_session().get(
                f"{base_url}/document/{doc_id}",
                headers=headers, timeout=timeout_sec,
                cookies=cookies_box["cookies"].cookies,
            )

            # Session expired - refresh and retry
            if file_res.status_code in (401, 403) and password:
                logger.warning(f"Session expired (HTTP {file_res.status_code}) on {label}, refreshing login...")
                with dl_lock:
                    cookies_box["cookies"] = refresh_login_cookies(
                        cookies_box["cookies"], username, password, client_name)
                continue

            # IT portal custom error or server error - retry
            if file_res.status_code != 200:
                logger.warning(f"{label} download attempt {dl_attempt}/3 failed: {link_text}, Status Code: {file_res.status_code}")
                if dl_attempt < 3:
                    time.sleep(dl_attempt * 2)
                continue

            # Success - save file
            with open(full_file_path, 'wb') as notice_f:
                notice_f.write(file_res.content)
            logger.info(f"File saved: {link_text}")
            file_info['downloaded'] = True
            download_success = True

            # Upload to S3
            try:
                s3_result = upload_to_s3(full_file_path, link_text)
                file_info['s3_url'] = s3_result.get('file_url')
                file_info['s3_key'] = s3_result.get('content_hash')
                logger.info(f"{label} uploaded to S3: {link_text}")
            except Exception as s3_error:
                logger.error(f"Error uploading {label} to S3: {s3_error}")
                file_info['s3_error'] = str(s3_error)
            break  # success, exit retry loop

        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            logger.warning(f"{label} download attempt {dl_attempt}/3 connection error: {link_text}: {e}")
            if dl_attempt < 3:
                time.sleep(dl_attempt * 2)
        except Exception as e:
            logger.error(f"{label} download unexpected error: {link_text}: {e}")
            break  # don't retry unknown errors

    if not download_success:
        logger.error(f"{label} download FAILED after 3 attempts: {link_text}")
        file_info['downloaded'] = False
        file_info['error'] = 'Failed after 3 download attempts'
    return file_info


def fetch_e_proceedings(login_cookies, username, client_name, page_no, download_dir=".", downloaded_files=None, fyi=False, third_party=False, existing_notice_ids=None, password=None, file_download_concurrency=3):
    """Fetch e-proceedings data and return as dict"""
    if downloaded_files is None:
        downloaded_files = []
    existing_notice_ids = existing_notice_ids or set()

    proceeding_status_flag = "FYI" if fyi else "FYA"
    proceeding_type_flag = "thirdParty" if third_party else "self"
    proceeding_type_label = "Of Other PAN/TAN" if third_party else "Self"

    # Create download directory if it doesn't exist
    if download_dir and not os.path.exists(download_dir):
        try:
            os.makedirs(download_dir, exist_ok=True)
            logger.info(f"Created download directory: {download_dir}")
        except Exception as e:
            logger.error(f"Failed to create download directory {download_dir}: {e}")

    e_proceedings_data = []
    errors = []

    try:
        res_4 = retry_request('post', f"{base_url}/returnservicesapi/auth/getEntity", json={
            "serviceName": "eProceedingsPaginatedService",
            "pan": username,
            "prcdngStatusFlag": proceeding_status_flag,
            "prcdngTypeFlag": proceeding_type_flag,
            "pageConfig": {
                "pageSize": 50,
                "pageNo": page_no,
                "searchTerm": "",
                "sortBy": "createdDt",
                "sortAsc": False,
                "filters": {}
            },
            "header": {
                "formName": "FO-041_PCDNG"
            }},
            headers=headers, cookies=login_cookies.cookies)

        res_4_data = res_4.json()
        e_proceedings_group = (res_4_data.get('eProceedingPaginatedRequests') or []) if res_4_data else []

        if e_proceedings_group:
            for epro in e_proceedings_group:
                try:
                    assessment_year = epro.get("assessmentYear", "")
                    financial_year = epro.get("financialYr", "")

                    try:
                        if assessment_year:
                            assessment_year = str(assessment_year)
                            assessment_year = assessment_year + (f"-{int(assessment_year[-2:]) + 1}") if len(assessment_year) > 2 else ""

                        if financial_year:
                            financial_year = str(financial_year)
                            financial_year = financial_year + (f"-{int(financial_year[-2:]) + 1}") if len(financial_year) > 2 else ""
                    except Exception:
                        pass

                    e_proceeding_document_data = {
                        "client": client_name,
                        "proceeding_name": epro.get("proceedingName"),
                        "assessment_year": assessment_year,
                        "financial_year": financial_year,
                        "fyi": fyi,
                        "proceeding_type": proceeding_type_label,
                        "status_map": [],
                    }

                    # Add third-party fields if applicable
                    if third_party:
                        e_proceeding_document_data["name_of_assessee"] = epro.get("nameOfAssesse", "")
                        e_proceeding_document_data["pan"] = epro.get("pan", "")

                    # Determine proceeding status
                    e_pro_status = ""
                    pro_type = epro.get('proceedingType', "") or ""
                    pro_status = epro.get('proceedingStatus', "") or ""
                    if pro_type == 'I' and pro_status == 'O':
                        e_pro_status = "OPEN"
                    elif pro_type == 'C' and pro_status == 'O':
                        e_pro_status = "PENDING"
                    elif pro_type == 'D' and pro_status == 'O':
                        e_pro_status = "PENDING"

                    e_proceeding_document_data['proceeding_status'] = e_pro_status

                    for status_line in epro.get("proceedingStatusDetails", []):
                        e_proceeding_document_data['status_map'].append({
                            'status': status_line.get('status'),
                            'date': make_date(status_line.get('date')),
                        })

                    # Fetch proceeding details with retry
                    proceeding_req_id = epro.get("proceedingReqId")
                    res_5 = retry_request('post', f"{base_url}/returnservicesapi/auth/getEntity", json={
                        "serviceName": "eProceedingDetailsService",
                        "proceedingReqId": proceeding_req_id,
                        "pan": username,
                        "header": {
                            "formName": "FO-041_PCDNG"
                        }
                    }, cookies=login_cookies.cookies)
                    res_5_data = res_5.json()

                    if not res_5_data:
                        logger.warning(f"No details returned for proceeding {proceeding_req_id}, skipping")
                        continue

                    # Check if ALL notices in this proceeding are already known — skip entire proceeding if so
                    all_notice_ids = [f"{n.get('proceedingReqId')}-{n.get('headerSeqNo')}" for n in res_5_data]
                    new_notices = [nid for nid in all_notice_ids if nid not in existing_notice_ids]
                    if not new_notices:
                        logger.info(f"SKIP entire proceeding {proceeding_req_id}: all {len(all_notice_ids)} notices already in Frappe")
                        continue

                    logger.info(f"Found {len(res_5_data)} notices in proceeding ({len(new_notices)} new, {len(all_notice_ids) - len(new_notices)} existing)")

                    # SPEEDUP (FastAPI lambda): the shared lambda processed
                    # notices ONE AT A TIME with a `time.sleep(1)` between each.
                    # Here each notice is INDEPENDENT (own getEntity/saveEntity/
                    # docMap + per-file downloads), so we run them concurrently
                    # via a small ThreadPoolExecutor. The loop body is extracted
                    # verbatim into `_process_single_notice` and returns its own
                    # `notice_document_data`; the shared lists (`e_proceedings_data`,
                    # `errors`) are appended on the main thread, and the two
                    # genuinely-shared mutables (`downloaded_files` dedup set and
                    # `login_cookies` on session-refresh) are guarded by locks.
                    _dl_lock = threading.Lock()
                    _cookies_box = {"cookies": login_cookies}

                    def _process_single_notice(notice):
                        local_errors = []
                        unique_e_pro_notice_id = f"{notice.get('proceedingReqId')}-{notice.get('headerSeqNo')}"

                        # Skip notices that already exist in Frappe (saves API calls + download time)
                        if unique_e_pro_notice_id in existing_notice_ids:
                            logger.info(f"SKIP notice {unique_e_pro_notice_id} (already in Frappe)")
                            return None, local_errors

                        # Create a separate dict for each notice (not shared across iterations)
                        notice_document_data = dict(e_proceeding_document_data)
                        notice_document_data['status_map'] = list(e_proceeding_document_data['status_map'])

                        notice_document_data['unique_e_pro_id'] = unique_e_pro_notice_id
                        notice_document_data['notice_communication_reference_id'] = notice.get('documentReferenceId')
                        notice_document_data['notice_din'] = notice.get('documentIdentificationNumber')
                        notice_document_data['notice_section'] = notice.get('noticeSection')

                        if notice.get('responseDueDate', None):
                            notice_document_data['response_due_date'] = make_date(notice.get('responseDueDate'), frappe_date=True)

                        if notice.get('issuedOn', None):
                            notice_document_data['notice_sent_date'] = make_date(notice.get('issuedOn'), frappe_date=True)

                        # Store reply files info
                        notice_document_data['reply_files'] = []

                        # Download response files if submitted
                        if notice.get('isSubmitted') and notice.get('isSubmitted') == 'Y':
                            try:
                                res_6 = retry_request('post', f"{base_url}/returnservicesapi/auth/getEntity", json={
                                    "serviceName": "itbaResponseService",
                                    "headerSeqNo": notice.get('headerSeqNo'),
                                    "pan": username,
                                    "header": {
                                        "formName": "FO-041_PCDNG"
                                    }
                                }, cookies=_cookies_box["cookies"].cookies, headers=headers)

                                res_6_data = res_6.json()
                                remark_notice_list = res_6_data.get('respRemrkAttLst', None)

                                if remark_notice_list:
                                    # Collect every reply attachment first, then
                                    # download them in parallel (bounded). The
                                    # download/S3 work is the shared helper; the
                                    # list/dict assignment stays on this thread.
                                    _reply_tasks = []
                                    for remark in remark_notice_list:
                                        for d in remark.get('attachmentLst', None):
                                            if not d.get('docId'):
                                                continue

                                            file_name = d.get('attachmentName', None)
                                            file_base_name, extension = os.path.splitext(file_name)
                                            link_text = f"{file_base_name}-{d.get('docId')}{extension}"
                                            full_file_path = os.path.join(download_dir, link_text) if download_dir else None

                                            file_info = {
                                                'file_name': link_text,
                                                'file_path': full_file_path,
                                                'doc_id': d.get('docId'),
                                                'original_name': file_name,
                                                'is_acknowledgement': "acknowledgement" in file_name.lower()
                                            }
                                            _reply_tasks.append((d.get('docId'), link_text, full_file_path, file_info))

                                    def _do_reply(task):
                                        _doc_id, _link, _path, _fi = task
                                        if download_dir and _link not in downloaded_files:
                                            _download_one_document(
                                                _doc_id, _link, _path, _fi,
                                                _cookies_box, _dl_lock, username, password,
                                                client_name, label="Reply file")
                                        else:
                                            _fi['downloaded'] = False
                                            _fi['skipped'] = True
                                        return _fi

                                    if _reply_tasks:
                                        _rfw = max(1, min(file_download_concurrency, len(_reply_tasks)))
                                        with ThreadPoolExecutor(max_workers=_rfw) as _rfp:
                                            for _fi in _rfp.map(_do_reply, _reply_tasks):
                                                if not _fi['is_acknowledgement']:
                                                    notice_document_data['reply_files'].append(_fi)
                                                else:
                                                    notice_document_data['response_acknowledgement'] = _fi

                            except requests.exceptions.HTTPError as e:
                                error_msg = f"Not able to download Response Files: {str(e)}"
                                logger.error(error_msg)
                                local_errors.append({"type": "response_files", "notice": unique_e_pro_notice_id, "error": error_msg})
                            except Exception as e:
                                error_msg = f"Error downloading response files: {str(e)}"
                                logger.error(error_msg)
                                local_errors.append({"type": "response_files", "notice": unique_e_pro_notice_id, "error": error_msg})

                        # Download NOTICE FILE
                        try:
                            notice_pdf_payload = {
                                "serviceName": "noticeletterpdf",
                                "headerSeqNo": notice.get("headerSeqNo"),
                                "procdngReqId": notice.get("proceedingReqId"),
                                "loggedInUserId": username,
                                "header": {
                                    "formName": "FO-041_PCDNG"
                                }
                            }
                            res_7 = retry_request('post', f"{base_url}/returnservicesapi/auth/saveEntity",
                                json=notice_pdf_payload, headers=headers, cookies=_cookies_box["cookies"].cookies)

                            res_7_data = res_7.json()
                            documents_res_7 = res_7_data.get("docMap", None)

                            # Retry with backoff if docMap is empty (portal intermittently returns empty)
                            if not documents_res_7:
                                for docmap_attempt in range(1, 4):
                                    wait_time = docmap_attempt * 2  # 2s, 4s, 6s
                                    logger.warning(f"Empty docMap for notice {unique_e_pro_notice_id}, retry {docmap_attempt}/3 after {wait_time}s...")
                                    time.sleep(wait_time)
                                    res_7 = retry_request('post', f"{base_url}/returnservicesapi/auth/saveEntity",
                                        json=notice_pdf_payload, headers=headers, cookies=_cookies_box["cookies"].cookies)
                                    res_7_data = res_7.json()
                                    documents_res_7 = res_7_data.get("docMap", None)
                                    if documents_res_7:
                                        logger.info(f"docMap received on retry {docmap_attempt} for notice {unique_e_pro_notice_id}")
                                        break
                                if not documents_res_7:
                                    logger.warning(f"docMap still empty after 3 retries for notice {unique_e_pro_notice_id}")

                            if documents_res_7:
                                # Prep every notice document, then download in
                                # parallel (bounded) via the shared helper. The
                                # `notice_letter` assignment keeps the original
                                # "last document wins" behaviour (docMap usually
                                # has one file).
                                _notice_tasks = []
                                for document_id, file_name in documents_res_7.items():
                                    if not document_id:
                                        continue

                                    if "gz" in file_name:
                                        file_name = file_name[:-3]

                                    logger.info(f"NOTICE Found-{file_name}")
                                    file_base_name, extension = os.path.splitext(file_name)
                                    link_text = f"{file_base_name}-{document_id}{extension}"
                                    full_file_path = os.path.join(download_dir, link_text) if download_dir else None

                                    notice_file_info = {
                                        'file_name': link_text,
                                        'file_path': full_file_path,
                                        'doc_id': document_id,
                                        'original_name': file_name
                                    }
                                    _notice_tasks.append((document_id, link_text, full_file_path, notice_file_info))

                                def _do_notice(task):
                                    _doc_id, _link, _path, _fi = task
                                    if download_dir and _link not in downloaded_files:
                                        _download_one_document(
                                            _doc_id, _link, _path, _fi,
                                            _cookies_box, _dl_lock, username, password,
                                            client_name, label="Notice file")
                                    else:
                                        _fi['downloaded'] = False
                                        _fi['skipped'] = True
                                    return _fi

                                if _notice_tasks:
                                    _nfw = max(1, min(file_download_concurrency, len(_notice_tasks)))
                                    with ThreadPoolExecutor(max_workers=_nfw) as _nfp:
                                        for _fi in _nfp.map(_do_notice, _notice_tasks):
                                            notice_document_data['notice_letter'] = _fi

                        except requests.exceptions.HTTPError as e:
                            error_msg = f"Not able to download Notice File: {str(e)}"
                            logger.error(error_msg)
                            local_errors.append({"type": "notice_file", "notice": unique_e_pro_notice_id, "error": error_msg})
                        except Exception as e:
                            error_msg = f"Error downloading notice file: {str(e)}"
                            logger.error(error_msg)
                            local_errors.append({"type": "notice_file", "notice": unique_e_pro_notice_id, "error": error_msg})

                        # Return this notice's data (collected on the main thread).
                        return notice_document_data, local_errors

                    # Run the notices of THIS proceeding concurrently. Each call
                    # is independent; results + errors are merged on this thread
                    # so the shared lists stay race-free. Bounded to a small pool
                    # to stay gentle on the IT portal.
                    _NOTICE_WORKERS = min(5, max(1, len(res_5_data)))
                    with ThreadPoolExecutor(max_workers=_NOTICE_WORKERS) as _notice_pool:
                        _futs = [_notice_pool.submit(_process_single_notice, n) for n in res_5_data]
                        for _f in as_completed(_futs):
                            try:
                                _ndata, _nerrs = _f.result()
                            except Exception as _e:
                                logger.error(f"Notice worker crashed: {_e}")
                                errors.append({"type": "notice_worker", "error": str(_e)})
                                continue
                            if _nerrs:
                                errors.extend(_nerrs)
                            if _ndata is not None:
                                e_proceedings_data.append(_ndata)

                except requests.exceptions.HTTPError as e:
                    error_msg = f"HTTP Error fetching E Proceeding details: {str(e)}"
                    logger.error(error_msg)
                    errors.append({"type": "e_proceeding_details", "error": error_msg})
                except Exception as e:
                    error_msg = f"Error processing E Proceeding: {str(e)}"
                    logger.error(error_msg)
                    errors.append({"type": "e_proceeding_processing", "error": error_msg})

        return {
            "count": len(e_proceedings_group),
            "data": e_proceedings_data,
            "errors": errors
        }

    except requests.exceptions.HTTPError as e:
        error_msg = f"HTTP Error fetching E Proceedings: {str(e)}"
        logger.error(error_msg)
        return {
            "count": 0,
            "data": [],
            "errors": [{"type": "e_proceedings_fetch", "error": error_msg}]
        }
    except Exception as e:
        error_msg = f"Error fetching E Proceedings: {str(e)}"
        logger.error(error_msg)
        return {
            "count": 0,
            "data": [],
            "errors": [{"type": "e_proceedings_fetch", "error": error_msg}]
        }


def fetch_demands(login_cookies, username, client_name, download_dir=".", downloaded_files=None, password=None):
    """Fetch demands data and return as dict"""
    if downloaded_files is None:
        downloaded_files = []

    # Create download directory if it doesn't exist
    if download_dir and not os.path.exists(download_dir):
        try:
            os.makedirs(download_dir, exist_ok=True)
            logger.info(f"Created download directory: {download_dir}")
        except Exception as e:
            logger.error(f"Failed to create download directory {download_dir}: {e}")

    demands_data = []
    errors = []

    try:
        res = retry_request('post',
            f"{base_url}/servicesapi/auth/getEntity",
            json={
                "pan": username,
                "serviceName": "outstandingDemand"
            },
            headers=headers,
            cookies=login_cookies.cookies
        )

        res_data = res.json()
        demand_list = (res_data.get("demandList") or []) if res_data else []

        logger.info(f"Found {len(demand_list)} demands")

        for demand in demand_list:
            demand_document_data = {
                'client': client_name,
                'demand_reference_no': demand.get('din'),
                'assessment_year': demand.get('itrAy'),
                'response_type': demand.get('responseType'),
                'section_code': demand.get('sectionCode'),
                'rectification_rights': demand.get('rectificationRights'),
                'outstanding_demand_amount': str(demand.get('orignalOutStDemandAmount')),
                'notice_status': demand.get('currentStatus'),
                'mode_of_service': demand.get('modeOfService', "-"),
                "date_of_demand_raised": make_date(demand.get("dateOfDemandraised", None), frappe_date=True) if demand.get("dateOfDemandraised") else None,
                "date_of_service_notice": date_from_timestamp(demand.get("dateOfServiceNotice", None)),
                "interest_start_date": date_from_timestamp(demand.get("intrestStartDate", None)),
                "response_submitted_date": date_from_timestamp(demand.get("responseSubmitted", None)),
            }

            # Download demand file if available
            demand_details = demand.get('demandIntimationDetails') or {}
            out_file_path = demand_details.get('outFilePath')

            if out_file_path:
                extension = os.path.splitext(out_file_path)[1] or ".pdf"
                din_value = demand.get('din', f"{client_name}-unknown-din")
                unique_file_name = f"{din_value}-{demand.get('itrAy')}{extension}"
                full_file_path = os.path.join(download_dir, unique_file_name) if download_dir else None

                file_info = {
                    'file_name': unique_file_name,
                    'file_path': full_file_path,
                    'original_path': out_file_path
                }

                if download_dir and unique_file_name not in downloaded_files:
                    try:
                        order_id = base64.b64encode(out_file_path.encode()).decode()
                        file_res = http_session().post(f"{base_url}/document/order", json={
                            "order": order_id
                        }, headers=headers, timeout=timeout_sec, cookies=login_cookies.cookies)

                        # Session expired - try refresh and retry
                        if file_res.status_code in (401, 403) and password:
                            logger.warning(f"Session expired (HTTP {file_res.status_code}), refreshing login for demand download...")
                            login_cookies = refresh_login_cookies(login_cookies, username, password, client_name)
                            file_res = http_session().post(f"{base_url}/document/order", json={
                                "order": order_id
                            }, headers=headers, timeout=timeout_sec, cookies=login_cookies.cookies)

                        file_res.raise_for_status()

                        if file_res and file_res.status_code == 200:
                            file_bytes = file_res.content
                            with open(full_file_path, "wb") as f:
                                f.write(file_bytes)
                            logger.info(f"Demand file saved: {unique_file_name}")
                            file_info['downloaded'] = True

                            # Upload to S3
                            try:
                                s3_result = upload_to_s3(full_file_path, unique_file_name)
                                file_info['s3_url'] = s3_result.get('file_url')
                                file_info['s3_key'] = s3_result.get('content_hash')
                                logger.info(f"Demand file uploaded to S3: {unique_file_name}")
                            except Exception as s3_error:
                                logger.error(f"Error uploading demand file to S3: {s3_error}")
                                file_info['s3_error'] = str(s3_error)
                        else:
                            logger.warning(f"Failed to download demand file: {file_res.status_code}")
                            file_info['downloaded'] = False
                    except Exception as e:
                        error_msg = f"Error downloading demand file: {str(e)}"
                        logger.error(error_msg)
                        file_info['downloaded'] = False
                        file_info['error'] = str(e)
                        errors.append({"type": "demand_file", "din": demand.get('din'), "error": error_msg})
                else:
                    file_info['downloaded'] = False
                    file_info['skipped'] = True

                demand_document_data['file_info'] = file_info

            demands_data.append(demand_document_data)

        return {
            "count": len(demands_data),
            "data": demands_data,
            "errors": errors
        }

    except requests.exceptions.HTTPError as e:
        error_msg = f"HTTP Error fetching demands: {str(e)}"
        logger.error(error_msg)
        return {
            "count": 0,
            "data": [],
            "errors": [{"type": "demands_fetch", "error": error_msg}]
        }
    except Exception as e:
        error_msg = f"Error fetching demands: {str(e)}"
        logger.error(error_msg)
        return {
            "count": 0,
            "data": [],
            "errors": [{"type": "demands_fetch", "error": error_msg}]
        }


def fetch_income_tax_notices_via_api(username, password, client_name, downloaded_files=None, existing_notice_ids=None, file_download_concurrency=3):
    """
    Main function to fetch Income Tax notices and demands

    Args:
        username: PAN number
        password: Portal password
        client_name: Client identifier
        downloaded_files: List of already downloaded file names (optional)
        existing_notice_ids: Set of unique_e_pro_id values already in Frappe (skip re-downloading)
        file_download_concurrency: max parallel document downloads within ONE notice
            (bounded; default 3 — the IT portal RemoteDisconnects under load)

    Returns:
        dict: {
            'success': bool,
            'login_errors': list,
            'demands': dict,
            'e_proceedings': list,
            'total_pages_fetched': int,
            'errors': list,
            'client_name': str,
            'username': str
        }
    """
    if downloaded_files is None:
        downloaded_files = []
    existing_notice_ids_set = set(existing_notice_ids or [])

    # Static download directory for Lambda /tmp
    download_dir = "/tmp"

    result = {
        'success': False,
        'login_errors': [],
        'demands': {},
        'e_proceedings': [],
        'total_pages_fetched': 0,
        'errors': [],
        'client_name': client_name,
        'username': username
    }

    # Login
    login_cookies, login_errors = login_user_via_apis(username, password, client_name)

    if not login_cookies:
        result['login_errors'] = login_errors
        return result

    result['success'] = True

    # Fetch demands
    try:
        demands_result = fetch_demands(login_cookies, username, client_name, download_dir, downloaded_files, password=password)
        result['demands'] = demands_result
        if demands_result.get('errors'):
            result['errors'].extend(demands_result['errors'])
    except Exception as e:
        error_msg = f"Error in fetch_demands: {str(e)}"
        logger.error(error_msg)
        result['errors'].append({"type": "demands", "error": error_msg})

    # Fetch e-proceedings (paginated) for all 4 combinations
    proceeding_variants = [
        {"fyi": False, "third_party": False, "label": "FYA/Self"},
        {"fyi": True, "third_party": False, "label": "FYI/Self"},
        {"fyi": False, "third_party": True, "label": "FYA/ThirdParty"},
        {"fyi": True, "third_party": True, "label": "FYI/ThirdParty"},
    ]

    for variant in proceeding_variants:
        page_no = 1
        while True:
            try:
                e_proceedings_result = fetch_e_proceedings(
                    login_cookies, username, client_name, page_no, download_dir, downloaded_files,
                    fyi=variant["fyi"], third_party=variant["third_party"],
                    existing_notice_ids=existing_notice_ids_set,
                    password=password,
                    file_download_concurrency=file_download_concurrency
                )
                logger.info(f"Fetched Page-{page_no} E Proceedings ({variant['label']})")

                result['e_proceedings'].extend(e_proceedings_result.get('data', []))
                if e_proceedings_result.get('errors'):
                    result['errors'].extend(e_proceedings_result['errors'])

                result['total_pages_fetched'] += 1

                # Check if more pages exist
                if not e_proceedings_result.get('count') or e_proceedings_result['count'] != 50:
                    logger.info(f"No more pages for {variant['label']}.")
                    break

                page_no += 1
            except Exception as e:
                error_msg = f"Error fetching e-proceedings page {page_no} ({variant['label']}): {str(e)}"
                logger.error(error_msg)
                result['errors'].append({"type": "e_proceedings_page", "page": page_no, "variant": variant['label'], "error": error_msg})
                break

    return result


# Lambda handler function

def send_worker_webhook(webhook_config, client_name, portal_type, result, execution_time):
    """Send individual worker results back to Frappe webhook"""
    if not webhook_config or not webhook_config.get("url"):
        return False
    
    callback_url = webhook_config.get("worker_callback_url") or (webhook_config["url"] + "/api/method/fin_buddy.features.lambda_webhooks.update_worker_result")
    
    headers = {"Content-Type": "application/json"}
    if webhook_config.get("api_key") and webhook_config.get("api_secret"):
        headers["Authorization"] = f"token {webhook_config['api_key']}:{webhook_config['api_secret']}"
    
    payload = {
        "log_name": webhook_config.get("log_name", ""),
        "client_name": client_name,
        "portal_type": portal_type,
        "result": json.dumps(result, default=str),
        "execution_time": execution_time
    }
    
    for attempt in range(2):
        try:
            response = requests.post(callback_url, headers=headers, json=payload, timeout=120)
            if response.status_code == 200:
                logger.info(f"Webhook sent successfully for {client_name}")
                return True
            logger.error(f"Webhook failed for {client_name}: {response.status_code}")
        except Exception as e:
            logger.error(f"Webhook error for {client_name} attempt {attempt+1}: {str(e)}")
        if attempt < 1:
            time.sleep(5)
    
    return False


def lambda_handler(event, context):
    """
    AWS Lambda handler function

    Expected event structure:
    {
        "username": "PAN_NUMBER",
        "password": "PASSWORD",
        "client_name": "client_name",
        "downloaded_files": []  # Optional list of already downloaded files
    }
    """
    handler_start_time = time.time()
    try:
        username = event.get('username')
        password = event.get('password')
        client_name = event.get('client_name')
        downloaded_files = event.get('downloaded_files', [])
        existing_notice_ids = event.get('existing_notice_ids', [])
        # Bounded per-notice file-download parallelism (System-Config tunable,
        # forwarded by the orchestrator). Default 3 — conservative because the
        # IT portal RemoteDisconnects under load.
        file_download_concurrency = int(event.get('income_tax_file_download_concurrency') or 3)

        if not username or not password or not client_name:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'Missing required parameters: username, password, or client_name'
                })
            }

        # Fetch data
        result = fetch_income_tax_notices_via_api(
            username=username,
            password=password,
            client_name=client_name,
            downloaded_files=downloaded_files,
            existing_notice_ids=existing_notice_ids,
            file_download_concurrency=file_download_concurrency
        )

        response = {
            'statusCode': 200 if result['success'] else 401,
            'body': json.dumps(result, default=str)
        }

        # Send results directly to Frappe webhook (fire-and-forget architecture)
        webhook_config = event.get('webhook_config')
        if webhook_config:
            execution_time = round(time.time() - handler_start_time, 2)
            worker_result = {
                'success': True,
                'client_info': {'client_name': client_name, 'portal': 'income_tax', 'username': username},
                'function_name': 'fetch_income_tax_notices_lambda',
                'response': response,
                'status_code': response['statusCode'],
                'execution_time_seconds': execution_time
            }
            send_worker_webhook(webhook_config, client_name, 'income_tax', worker_result, execution_time)

        return response

    except Exception as e:
        logger.error(f"Lambda handler error: {str(e)}")
        logger.error(traceback.format_exc())
        error_response = {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'traceback': traceback.format_exc()
            })
        }

        # Send error to Frappe webhook
        webhook_config = event.get('webhook_config')
        if webhook_config:
            execution_time = round(time.time() - handler_start_time, 2)
            worker_result = {
                'success': False,
                'client_info': {'client_name': event.get('client_name', 'Unknown'), 'portal': 'income_tax'},
                'function_name': 'fetch_income_tax_notices_lambda',
                'response': error_response,
                'error': str(e),
                'execution_time_seconds': execution_time
            }
            send_worker_webhook(webhook_config, event.get('client_name', 'Unknown'), 'income_tax', worker_result, execution_time)

        return error_response


# For local testing
if __name__ == "__main__":
    # Example usage
    test_event = {
        "username": "AADCH3199K",
        "password": "aarya@2004",
        "client_name": "IN-TAX-CLT-07460",
        "download_dir": "./downloads",
    }

    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2, default=str))

    # Save JSON data to file
    if result.get("statusCode") == 200:
        username = test_event.get("username")
        client_name = test_event.get("client_name")
        current_date = datetime.now().strftime("%Y-%m-%d")
        json_filename = f"{username}_{client_name}_{current_date}.json"
        json_filepath = os.path.join(test_event.get("download_dir", "."), json_filename)

        # Parse the body string to make it more readable
        result_to_save = result.copy()
        if isinstance(result_to_save.get("body"), str):
            try:
                result_to_save["body"] = json.loads(result_to_save["body"])
            except json.JSONDecodeError:
                pass

        with open(json_filepath, "w") as json_file:
            json.dump(result_to_save, json_file, indent=2, default=str)
        logger.info(f"JSON data saved to: {json_filepath}")
