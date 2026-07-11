"""
TDS notice fetching — HYBRID Lambda (FastAPI fleet).

TRACES retired its HTML login (the portal is now a Flutter canvas), so login is
done against the NEW portal's JSON API (2captcha), and the resulting Keycloak JWT
is bridged into headless Chrome (CDP Authorization + RefreshToken headers →
preauthV2.xhtml 302) so the OLD portal (traces61) — where the demand / quarter /
justification pages still live — loads authenticated. The demand/quarter scrape
+ Justification-Report flow then run as headless Selenium.

This is a STANDALONE module (no FastAPI / SQLAlchemy / app imports). The hybrid
login + scrape logic is ported verbatim from the tested backend
(app/services/tds_traces_api.py + app/events/tds_gov.py). Scraped results are
returned to the FastAPI backend via the worker webhook (the same compat route
GST/IT use: /api/method/fin_buddy.features.lambda_webhooks.update_worker_result);
the backend persists them via its existing get_or_create_quarter. The Lambda
does NOT touch Postgres.

Requires the Lambda Layers:
  - headless-chrome-selenium  (/opt/headless-chromium, /opt/chromedriver, /opt/etc/fonts)
  - requests-python           (requests + 2captcha HTTP)
Env: CAPTCHA_API_KEY (2captcha), WEBHOOK_BASE_URL (fallback callback base).
Sized for in-lambda parallel quarters (≈3008MB / 900s).

FastAPI fleet only — has nothing to do with the Frappe og_app TDS scraper.
"""
import base64
import concurrent.futures
import json
import logging
import os
import re
import tempfile
import time
import traceback
import uuid
import zipfile

import requests

try:
    from bs4 import BeautifulSoup
except Exception:  # bs4 ships in the layer; allow import-time tolerance for tooling
    BeautifulSoup = None

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import Select, WebDriverWait
    from selenium.common.exceptions import NoSuchElementException, TimeoutException
except Exception:  # selenium ships in the layer; allow import-time tolerance for tooling
    webdriver = None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("tds_hybrid_lambda")
logger.setLevel(logging.INFO)


# ===========================================================================
# Endpoints + constants (ported from tds_traces_api.py)
# ===========================================================================
API_BASE = "https://traces-app.tdscpc.gov.in"
GENERATE_CAPTCHA_URL = f"{API_BASE}/loginservice/api/auth/generateCaptcha"
LOGIN_URL = f"{API_BASE}/loginservice/api/auth/login"
OLD_PORTAL_BASE = "https://traces61.tdscpc.gov.in"
PREAUTH_BRIDGE_URL = f"{OLD_PORTAL_BASE}/app/preauthV2.xhtml"

_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36"
    ),
    "Origin": "https://traces.tdscpc.gov.in",
    "Referer": "https://traces.tdscpc.gov.in/",
    "Accept": "*/*",
    "Accept-Language": "en-GB,en;q=0.6",
}

_CAPTCHA_MAX_POLLS = 30
_CAPTCHA_POLL_SECONDS = 3
_LOGIN_CAPTCHA_BURST = 3
_LOGIN_CAPTCHA_ROUNDS = 3
_LOGIN_DEADLINE_SECONDS = 180
_CAPTCHA_FETCH_STAGGER_SECONDS = 1.5
_CAPTCHA_RETRY_MARKERS = ("verification code", "captcha", "does not match with image")

# In-lambda quarter parallelism (Q3 = small cap). Each quarter worker is its own
# headless Chrome bridged from the same JWT. 2 fits the 3008MB-sized lambda.
_QUARTER_CONCURRENCY = int(os.environ.get("TDS_QUARTER_FETCH_CONCURRENCY") or 2)

# JR zip password template + quarter→TRACES dropdown value (ported from tds_gov).
_JR_QUARTER_VALUE_MAP = {"Q1": "3", "Q2": "4", "Q3": "5", "Q4": "6"}


class TracesApiError(Exception):
    def __init__(self, message, status_code=None):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


class TracesLoginFailed(TracesApiError):
    pass


class TracesCaptchaError(TracesApiError):
    pass


# ===========================================================================
# New-portal API login (ported verbatim from tds_traces_api.py)
# ===========================================================================
def _new_session():
    s = requests.Session()
    s.headers.update(_BROWSER_HEADERS)
    return s


def fetch_captcha(session):
    try:
        r = session.get(GENERATE_CAPTCHA_URL, timeout=30)
    except requests.RequestException as e:
        raise TracesCaptchaError(f"generateCaptcha request failed: {e}")
    if r.status_code != 200:
        raise TracesCaptchaError(f"generateCaptcha HTTP {r.status_code}", status_code=r.status_code)
    try:
        body = r.json()
    except ValueError:
        raise TracesCaptchaError("generateCaptcha returned non-JSON")
    image_b64 = body.get("image")
    captcha_id = body.get("id")
    if not image_b64 or not captcha_id:
        raise TracesCaptchaError("generateCaptcha missing image/id")
    return image_b64, captcha_id


def solve_captcha_base64(image_b64):
    api_key = os.environ.get("CAPTCHA_API_KEY")
    if not api_key:
        raise TracesCaptchaError("CAPTCHA_API_KEY not configured")
    try:
        submit = requests.post(
            "http://2captcha.com/in.php",
            data={
                "key": api_key, "method": "base64", "body": image_b64,
                # TRACES captcha = 6-char mixed-case alnum, validated case-sensitively.
                "regsense": 1, "numeric": 0, "min_len": 6, "max_len": 6,
                "language": 2, "phrase": 0,
            },
            timeout=30,
        )
    except requests.RequestException as e:
        raise TracesCaptchaError(f"2captcha submit failed: {e}")
    text = (submit.text or "").strip()
    if not text.startswith("OK|"):
        raise TracesCaptchaError(f"2captcha submit rejected: {text[:80]}")
    cid = text.split("|", 1)[1]
    result_url = f"http://2captcha.com/res.php?key={api_key}&action=get&id={cid}"
    for _ in range(_CAPTCHA_MAX_POLLS):
        time.sleep(_CAPTCHA_POLL_SECONDS)
        try:
            res = requests.get(result_url, timeout=30)
        except requests.RequestException:
            continue
        body = (res.text or "").strip()
        if body == "CAPCHA_NOT_READY":
            continue
        if body.startswith("OK|"):
            return body.split("|", 1)[1].strip()
        raise TracesCaptchaError(f"2captcha solve error: {body[:80]}")
    raise TracesCaptchaError("2captcha timed out")


def _extract_token(body):
    if not isinstance(body, dict):
        return None
    for key in ("access_token", "accessToken", "token", "jwt", "id_token"):
        v = body.get(key)
        if isinstance(v, str) and v.count(".") == 2:
            return v
    for outer in ("authTokenDto", "data", "result", "payload"):
        inner = body.get(outer)
        if isinstance(inner, dict):
            for key in ("accessToken", "access_token", "token", "jwt"):
                v = inner.get(key)
                if isinstance(v, str) and v.count(".") == 2:
                    return v
    return None


def _extract_refresh(body):
    if not isinstance(body, dict):
        return None
    for key in ("refreshToken", "refresh_token", "RefreshToken"):
        v = body.get(key)
        if isinstance(v, str) and v:
            return v
    for outer in ("authTokenDto", "data", "result", "payload"):
        inner = body.get(outer)
        if isinstance(inner, dict):
            for key in ("refreshToken", "refresh_token", "RefreshToken"):
                v = inner.get(key)
                if isinstance(v, str) and v:
                    return v
    return None


def _extract_error(body):
    if not isinstance(body, dict):
        return None
    for key in ("message", "error", "errorMessage", "msg", "statusMessage"):
        v = body.get(key)
        if isinstance(v, str) and v:
            return v
    return None


def _one_login_attempt(tan, password, sub_user_pan_id, n, start_delay=0.0):
    if start_delay:
        time.sleep(start_delay)
    try:
        session = _new_session()
        image_b64, captcha_id = fetch_captcha(session)
        solved = solve_captcha_base64(image_b64)
    except TracesApiError as e:
        return {"ok": False, "retryable": True, "error": TracesLoginFailed(str(e))}

    payload = {
        "userId": tan, "userType": "Deductor", "subUserPanId": sub_user_pan_id or "",
        "password": password, "captcha": solved, "captchaId": captcha_id,
    }
    try:
        r = session.post(LOGIN_URL, json=payload,
                         headers={"Content-Type": "application/json; charset=utf-8"}, timeout=45)
    except requests.RequestException as e:
        return {"ok": False, "retryable": True, "error": TracesLoginFailed(f"login request failed: {e}")}
    try:
        body = r.json()
    except ValueError:
        body = {"_raw_text": (r.text or "")[:500]}

    if r.status_code == 200:
        token = _extract_token(body)
        if token:
            return {"ok": True, "token": token, "refresh": _extract_refresh(body), "raw": body}
        return {"ok": False, "retryable": False, "error": TracesLoginFailed(
            _extract_error(body) or "login succeeded but no token returned", status_code=200)}

    msg = _extract_error(body) or f"login HTTP {r.status_code}"
    is_captcha = any(m in msg.lower() for m in _CAPTCHA_RETRY_MARKERS)
    if is_captcha:
        logger.info("TRACES login attempt %d: captcha miss", n)
    return {"ok": False, "retryable": is_captcha, "error": TracesLoginFailed(msg, status_code=r.status_code)}


def api_login(tan, password, sub_user_pan_id=""):
    """Burst-parallel captcha login (staggered fetch). Returns (access, refresh)."""
    if not tan or not password:
        raise TracesLoginFailed("Missing TAN or password")
    last_error = None
    deadline = time.monotonic() + _LOGIN_DEADLINE_SECONDS
    for rnd in range(1, _LOGIN_CAPTCHA_ROUNDS + 1):
        if time.monotonic() > deadline:
            logger.warning("TRACES login: overall deadline exceeded; giving up")
            break
        fatal_error = None
        with concurrent.futures.ThreadPoolExecutor(max_workers=_LOGIN_CAPTCHA_BURST) as ex:
            futures = [
                ex.submit(_one_login_attempt, tan, password, sub_user_pan_id,
                          (rnd - 1) * _LOGIN_CAPTCHA_BURST + i + 1,
                          i * _CAPTCHA_FETCH_STAGGER_SECONDS)
                for i in range(_LOGIN_CAPTCHA_BURST)
            ]
            try:
                for fut in concurrent.futures.as_completed(futures):
                    try:
                        outcome = fut.result()
                    except Exception as e:
                        last_error = TracesLoginFailed(f"login attempt errored: {e}")
                        continue
                    if outcome.get("ok"):
                        return outcome["token"], outcome.get("refresh")
                    last_error = outcome.get("error")
                    if not outcome.get("retryable"):
                        fatal_error = outcome.get("error")
                        break
            finally:
                for fut in futures:
                    fut.cancel()
        if fatal_error:
            raise fatal_error
        if rnd < _LOGIN_CAPTCHA_ROUNDS:
            logger.info("TRACES login burst %d/%d all missed captcha; retrying", rnd, _LOGIN_CAPTCHA_ROUNDS)
    raise last_error or TracesLoginFailed("login failed after burst retries")


# ===========================================================================
# Headless Chrome (ported from fetch_gst_notices_fastapi_lambda) — with the
# parallel-safe tweak: NO --single-process (it breaks multiple concurrent
# Chromes, which the in-lambda parallel quarters need).
# ===========================================================================
def setup_chrome_options(download_dir, tmp_folder):
    options = Options()
    is_lambda = os.environ.get("AWS_LAMBDA_FUNCTION_NAME")
    os.environ["TZ"] = "Asia/Kolkata"
    if is_lambda:
        options.binary_location = "/opt/headless-chromium"
        os.environ["FONTCONFIG_PATH"] = "/opt/etc/fonts"
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        # NOTE: deliberately NOT --single-process — we run multiple Chromes in
        # parallel (per-quarter) and single-process crashes with >1 browser.
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--hide-scrollbars")
        options.add_argument("--ignore-certificate-errors")
        options.add_argument("--window-size=1280,1696")
        options.add_argument(f"--user-data-dir={tmp_folder}/user-data")
        options.add_argument(f"--data-path={tmp_folder}/data-path")
        options.add_argument(f"--homedir={tmp_folder}")
        options.add_argument(f"--disk-cache-dir={tmp_folder}/cache-dir")
        options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36")
    else:
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--window-size=1920,1080")
        options.add_argument(f"--user-data-dir={tmp_folder}/user-data")
    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": False,
        "plugins.always_open_pdf_externally": True,
        "profile.default_content_setting_values.automatic_downloads": 1,
    }
    options.add_experimental_option("prefs", prefs)
    return options


def setup_driver(download_dir):
    tmp_folder = f"/tmp/{uuid.uuid4()}"
    for sub in ["", "/user-data", "/data-path", "/cache-dir"]:
        os.makedirs(tmp_folder + sub, exist_ok=True)
    options = setup_chrome_options(download_dir, tmp_folder)
    if os.environ.get("AWS_LAMBDA_FUNCTION_NAME"):
        driver = webdriver.Chrome("/opt/chromedriver", options=options)
    else:
        driver = webdriver.Chrome(options=options)
    return driver


def bridge_into_driver(driver, access, refresh):
    """CDP-inject Authorization + RefreshToken, GET preauthV2 (Chrome follows the
    302 → authenticated old-portal session), clear headers, confirm dashboard.
    Ported verbatim from tds_gov.bridge_into_driver."""
    try:
        driver.execute_cdp_cmd("Network.enable", {})
        driver.execute_cdp_cmd("Network.setExtraHTTPHeaders", {"headers": {
            "Authorization": f"Bearer {access}",
            "RefreshToken": refresh or "",
            "Origin": "https://traces.tdscpc.gov.in",
            "Referer": "https://traces.tdscpc.gov.in/",
            "X-Requested-With": "XMLHttpRequest",
            "X-Bank-Code": "null",
            "X-Everify": "N",
        }})
    except Exception as ce:
        logger.error("CDP bridge header set failed: %s", str(ce))
        return False
    driver.get(PREAUTH_BRIDGE_URL)
    time.sleep(4)
    try:
        driver.execute_cdp_cmd("Network.setExtraHTTPHeaders", {"headers": {}})
    except Exception:
        pass
    driver.get(f"{OLD_PORTAL_BASE}/app/ded/dashboard.xhtml")
    time.sleep(3)
    page = driver.page_source or ""
    return "Welcome" in page and not re.search(r"Welcome\s*\(\s*\)", page)


# ===========================================================================
# Demand / quarter scrape — ported verbatim from tds_gov.py.
# The ONLY semantic change vs the backend source: instead of persisting each
# quarter via get_or_create_quarter(db, ...), every scraped quarter is BUILT
# into a JSON dict and collected; the entry function returns a structure the
# webhook ships back to FastAPI, which persists via its existing helper.
# Selenium IDs / XPaths / waits / sleeps / the per-quarter match fix / the
# parallel _fetch_one worker are carried AS-IS.
# ===========================================================================
def extract_demand_table(driver):
    """Extract data from the demand table."""
    try:
        wait = WebDriverWait(driver, 20)

        table = wait.until(
            EC.presence_of_element_located((By.ID, "fyAmtList"))
        )

        table_html = table.get_attribute('outerHTML')
        soup = BeautifulSoup(table_html, 'html.parser')

        data = {}

        rows = soup.find_all('tr', class_='ui-widget-content jqgrow ui-row-ltr')
        for row in rows:
            cells = row.find_all('td')
            if len(cells) >= 3:
                financial_year = cells[0].get('title', '')
                manual_demand = float(cells[1].get('title', '0.00'))
                processed_demand = float(cells[2].get('title', '0.00'))

                data[financial_year] = {
                    'financial_year': financial_year,
                    'manual_demand': manual_demand,
                    'processed_demand': processed_demand,
                }

        return data

    except Exception as e:
        logger.error("Error extracting table data: %s", str(e))
        return {}


def get_prior_years_table_data(driver):
    """Extract data from the prior years demand table."""
    try:
        wait = WebDriverWait(driver, 20)

        table = wait.until(
            EC.presence_of_element_located((By.ID, "priorList"))
        )

        table_html = table.get_attribute('outerHTML')
        soup = BeautifulSoup(table_html, 'html.parser')

        data = {}

        rows = soup.select('tr.ui-widget-content.jqgrow')
        for row in rows:
            cells = row.find_all('td')
            if len(cells) >= 3:
                financial_year = cells[0].get('title', '')
                manual_demand = float(cells[1].get('title', '0.00'))
                processed_demand = float(cells[2].get('title', '0.00'))

                data[financial_year] = {
                    'financial_year': financial_year,
                    'manual_demand': manual_demand,
                    'processed_demand': processed_demand,
                }

        return data

    except Exception as e:
        logger.error("Error extracting prior years table data: %s", str(e))
        return {}


def _capture_quarter_detail_html(driver) -> str:
    """Collect the quarter detail page's tables into one HTML blob: the
    token/order tables (div#tokenNumberDet), the Type-of-Default breakdown
    (userList w750), and the deductee-PAN-counts table (userList marginTop10
    w398). Each lookup is best-effort. Shared by the serial + parallel paths."""
    parts = []
    for xpath in (
        "//div[@id='tokenNumberDet']/div[1]/table",
        "//div[@id='tokenNumberDet']/div[2]/table",
    ):
        try:
            parts.append(driver.find_element(By.XPATH, xpath).get_attribute("outerHTML"))
        except Exception:
            pass
    try:
        tables = driver.find_elements(By.XPATH, "//table[@class='userList w750']")
        if tables:
            parts.append(tables[0].get_attribute("outerHTML"))
    except Exception:
        pass
    try:
        parts.append(
            driver.find_element(
                By.XPATH, "//table[@class='userList marginTop10 w398']"
            ).get_attribute("outerHTML")
        )
    except Exception:
        pass
    return "</br></br>".join(parts)


def extract_quarter_detail_fields(driver) -> dict:
    """Parse the quarter detail page (demsummary.xhtml) for the structured
    fields it exposes beyond the demand-summary row.

    The first 'userList w550' table is laid out as a header row + a value row:
        Statement Token Number | Order Passed Date
        <Regular Statement> <token>  <dd-Mon-yyyy>
    We read the token (digits/X mask) and the order-passed date. Both are
    best-effort — a missing table just yields None (the caller logs + skips).
    """
    out = {"statement_token": None, "order_passed_date": None}
    try:
        tbl = driver.find_element(By.XPATH, "(//table[@class='userList w550'])[1]")
        text = tbl.text or ""
    except Exception:
        return out

    # Statement token: a long digit/X-masked run (e.g. 7700XXXXXXX7094).
    m = re.search(r"\b([0-9X]{10,15})\b", text)
    if m:
        out["statement_token"] = m.group(1)
    # Order passed date: dd-Mon-yyyy (e.g. 05-Mar-2020).
    m = re.search(r"\b(\d{2}-[A-Za-z]{3}-\d{4})\b", text)
    if m:
        out["order_passed_date"] = m.group(1)
    return out


def process_quarters(driver, fy_details, access, refresh):
    """Process all quarters for a specific financial year and return a list of
    quarter JSON dicts.

    Ported verbatim from tds_gov.process_quarters (the FIXED version): resolve
    each quarter's DIRECT detail URL by per-row quarter-text match, then fetch
    each quarter (parallel with own-bridge workers, capped by
    _QUARTER_CONCURRENCY, plus a serial fallback). The ONLY change vs the source
    is the persistence step: instead of get_or_create_quarter(db, ...) writing a
    row, each fetched quarter is returned as a JSON dict.

    `access`/`refresh` are the login JWT, threaded through explicitly (the
    backend stashed them on driver._tds_jwt; here they are passed in) so each
    parallel worker can bridge its own driver from the SAME login — no
    re-captcha.
    """
    wait = WebDriverWait(driver, 10)
    year_text = fy_details.get("financial_year")

    try:
        wait.until(EC.visibility_of_element_located((By.ID, "demandsumFY1")))

        quarters_table = driver.find_element(By.ID, "demandsumFY1")
        table_rows = quarters_table.find_elements(By.XPATH, ".//tr[contains(@class, 'odd') or contains(@class, 'even')]")

        logger.info("Found %d quarters for %s", len(table_rows), year_text)

        quarters_data = []

        for row in table_rows:
            try:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) >= 3:
                    quarter_text = cells[0].find_element(By.XPATH, ".//span").text
                    form_type = cells[1].find_element(By.XPATH, ".//span").text
                    net_payable = cells[2].find_element(By.XPATH, ".//span").text

                    quarters_data.append({
                        'quarter': quarter_text,
                        'form_type': form_type,
                        'net_payable': net_payable,
                    })
            except Exception as e:
                logger.error("Error processing table row: %s", str(e))

        # Resolve each quarter's DIRECT detail URL (the demsummary.xhtml link in
        # its row). Each quarter row has two anchors in td[1] — the empty
        # manual-demand link and the real quarter link whose text is the quarter
        # ("Q1"…). We take the latter's href, which is a deterministic,
        # directly-navigable URL (…/demsummary.xhtml?fy=…&quarterdesc=Q1&qr=…).
        # Matching by text (not a flat index) is what fixed the repeated-quarter
        # bug; capturing the URL is what lets us fetch quarters in parallel.
        for data in quarters_data:
            qtext = data['quarter']
            for r in driver.find_elements(
                By.XPATH,
                "//table[@id='demandsumFY1']//tr[contains(@class,'odd') or contains(@class,'even')]",
            ):
                tds = r.find_elements(By.TAG_NAME, "td")
                if not tds or tds[0].text.strip() != qtext:
                    continue
                for a in r.find_elements(By.XPATH, ".//td[1]//a"):
                    if a.text.strip() == qtext:
                        data['detail_url'] = a.get_attribute("href")
                        break
                break

        # In-lambda quarter parallelism cap.
        concurrency = _QUARTER_CONCURRENCY

        targets = [d for d in quarters_data if d.get('detail_url')]

        def build_quarter_data(detail_html, statement_token, order_passed_date, data):
            return {
                'fy': year_text,
                'manual_demand': fy_details.get("manual_demand"),
                'processed_demand': fy_details.get("processed_demand"),
                'quarter': data['quarter'],
                'form_type': data['form_type'],
                'net_payable': data['net_payable'],
                'demand_detail_html': detail_html,
                'statement_token': statement_token,
                'order_passed_date': order_passed_date,
            }

        # --- Parallel path: one worker (own Chrome + own bridge) per quarter,
        #     capped by _QUARTER_CONCURRENCY. Each worker bridges its own driver
        #     from the SAME login JWT (no re-captcha). ---
        if access and concurrency > 1 and len(targets) > 1:

            def _fetch_one(data, start_delay=0.0):
                # Small stagger so N Chrome instances don't all launch on the
                # exact same tick (Selenium-Manager driver resolution + the
                # browser cold-start contend otherwise).
                if start_delay:
                    time.sleep(start_delay)
                wdriver = None
                try:
                    wdriver = setup_driver(f"/tmp/tdsq_{data['quarter']}_{uuid.uuid4().hex[:8]}")
                    # Bridge with a couple of retries — a freshly-launched Chrome
                    # occasionally misses the preauthV2 redirect on the first try.
                    bridged = False
                    for attempt in range(3):
                        if bridge_into_driver(wdriver, access, refresh):
                            bridged = True
                            break
                        logger.warning("Quarter %s: bridge attempt %d/3 failed, retrying",
                                       data['quarter'], attempt + 1)
                        time.sleep(2)
                    if not bridged:
                        logger.error("Quarter %s: worker bridge failed after retries", data['quarter'])
                        return None
                    wdriver.get(data['detail_url'])
                    time.sleep(3)
                    detail_html = _capture_quarter_detail_html(wdriver)
                    statement_token = order_passed_date = None
                    try:
                        det = extract_quarter_detail_fields(wdriver)
                        statement_token = det.get("statement_token")
                        order_passed_date = det.get("order_passed_date")
                    except Exception:
                        pass
                    return build_quarter_data(detail_html, statement_token, order_passed_date, data)
                except Exception as e:
                    logger.error("Parallel quarter %s failed: %s", data['quarter'], str(e))
                    return None
                finally:
                    if wdriver:
                        try:
                            wdriver.quit()
                        except Exception:
                            pass

            from concurrent.futures import ThreadPoolExecutor, as_completed
            fy_quarters = []
            max_workers = min(concurrency, len(targets))
            logger.info("Fetching %d quarters in parallel (max_workers=%d) for %s",
                        len(targets), max_workers, year_text)
            with ThreadPoolExecutor(max_workers=max_workers) as ex:
                # Stagger launches ~1.5s apart to avoid Chrome cold-start contention.
                futures = [ex.submit(_fetch_one, d, idx * 1.5) for idx, d in enumerate(targets)]
                for fut in as_completed(futures):
                    r = fut.result()
                    if r:
                        fy_quarters.append(r)
            return fy_quarters

        # --- Serial fallback: navigate the main driver to each detail URL. ---
        fy_quarters = []
        for data in targets:
            try:
                quarter_text = data['quarter']
                logger.info("Processing quarter details: %s", quarter_text)
                driver.get(data['detail_url'])
                time.sleep(3)
                complete_table_html = _capture_quarter_detail_html(driver)
                statement_token = order_passed_date = None
                try:
                    detail = extract_quarter_detail_fields(driver)
                    statement_token = detail.get("statement_token")
                    order_passed_date = detail.get("order_passed_date")
                except Exception as de:
                    logger.warning("Quarter %s: detail-field parse failed: %s", quarter_text, str(de))

                fy_quarters.append(
                    build_quarter_data(complete_table_html, statement_token, order_passed_date, data)
                )
            except Exception as e:
                logger.error("Error processing quarter %s: %s", data['quarter'], str(e))

        return fy_quarters

    except Exception as e:
        logger.error("Error processing quarters for %s: %s", year_text, str(e))
        traceback.print_exc()
        return None


def process_financial_years(driver, access, refresh):
    """Process all financial years (including Prior Years) from the dashboard and
    return a list of per-FY JSON dicts (each with its quarters list).

    Ported verbatim from tds_gov.process_financial_years — the navigation /
    Prior-Years expansion / per-year loop are unchanged; the only difference is
    that the scraped quarters are collected into a returned structure instead of
    being persisted to the DB.
    """
    wait = WebDriverWait(driver, 20)

    table_fy_data = extract_demand_table(driver)

    financial_years = []

    try:
        financial_year_links = driver.find_elements(By.XPATH, "//table[@id='fyAmtList']/tbody/tr/td[@aria-describedby='fyAmtList_finYr']")

        years_to_process = []
        prior_years_exists = False
        prior_years_row_elm = None

        for link in financial_year_links:
            if link.text == "Prior Years":
                prior_years_exists = True
                prior_years_row_elm = link
            else:
                years_to_process.append(link.text)

        logger.info("Years to process: %s", years_to_process)

        if prior_years_exists and prior_years_row_elm:
            driver.execute_script("arguments[0].click();", prior_years_row_elm)
            time.sleep(3)

            prior_year_data = get_prior_years_table_data(driver)
            table_fy_data.update(prior_year_data)

            financial_year_links = driver.find_elements(By.XPATH, "//table[@id='priorList']/tbody/tr/td[@aria-describedby='priorList_finYr']")
            for link in financial_year_links:
                years_to_process.append(link.text)

        for year_text in years_to_process:
            logger.info("Processing financial year: %s", year_text)

            row = table_fy_data.get(year_text)

            if row:
                fin_year = year_text.split('-')[0]
                url = f"https://traces61.tdscpc.gov.in/app/ded/viewdemandsum.xhtml?fy={fin_year}"

                driver.get(url)
                time.sleep(3)

                fy_quarters = process_quarters(driver, row, access, refresh)

                if fy_quarters:
                    logger.info("Successfully processed quarters for %s", year_text)
                else:
                    logger.warning("Failed to process quarters for %s", year_text)

                financial_years.append({
                    'financial_year': row.get('financial_year'),
                    'manual_demand': row.get('manual_demand'),
                    'processed_demand': row.get('processed_demand'),
                    'quarters': fy_quarters or [],
                })

                driver.back()
                time.sleep(3)

        return financial_years

    except Exception as e:
        logger.error("Error processing financial years: %s", str(e))
        traceback.print_exc()
        return financial_years


def scrape_demand_and_quarters(driver, access, refresh):
    """Navigate to the outstanding-demand dashboard and scrape every FY/quarter.

    Folds tds_gov.navigate_to_dashboard's entry navigation into the scrape, then
    delegates to process_financial_years. Returns the list of per-FY dicts.
    """
    driver.get("https://traces61.tdscpc.gov.in/app/ded/outstandingdemand.xhtml")
    time.sleep(5)
    return process_financial_years(driver, access, refresh)


# ===========================================================================
# Logout — ported verbatim from tds_gov.logout_user.
# ===========================================================================
def logout_user(driver):
    """Logout from TDS portal."""
    try:
        logger.info("Logging out from TDS portal...")
        driver.execute_script("document.body.scrollTop = 0; document.documentElement.scrollTop = 0;")
        time.sleep(1)

        logout_link = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((
            By.XPATH,
            "//li[@class='last_menu']/a[@title='Logout']"
        )))
        logout_link.click()
        time.sleep(3)
        return True
    except Exception as e:
        logger.error("Logout failed: %s", str(e))
        return False


# ===========================================================================
# Justification Report (JR) flow — ported verbatim from tds_gov.py.
# The ONLY change vs the backend source: the DB-persist + S3-upload steps are
# replaced with returning the JR file bytes as base64 in the JSON (the backend
# uploads to S3 via its existing helper) plus the request_number / authcode /
# message fields. Selenium IDs / XPaths / waits are carried AS-IS.
#
# In the lambda the per-quarter JR inputs (token_number, KYC fields, etc.) are
# NOT persisted; the backend supplies any user-entered KYC values it already
# holds. The lambda drives the request-and-download path from a plain quarter
# dict (`q`) carrying whatever fields the backend passed for that quarter.
# ===========================================================================
def _format_date_ddmmmyyyy(d):
    """Frappe's format_date(d, 'dd-MMM-yyyy') — e.g. 15-Jun-2024. Returns ''."""
    if not d:
        return ""
    try:
        return d.strftime("%d-%b-%Y")
    except Exception:
        return str(d)


def _accept_alert_if_present(driver):
    """Accept a JS alert if one popped up (TRACES KYC checkboxes raise alerts)."""
    try:
        alert = driver.switch_to.alert
        logger.info("Alert detected: %s", alert.text)
        alert.accept()
    except Exception:
        pass


def go_to_justification_report_request_form(driver, q):
    """Open the JR request form and select FY / quarter / form-type.

    Ported verbatim; host = traces61.tdscpc.gov.in. `q` is a quarter dict with
    financial_year / quarter / form_type (was quarter.financial_year etc).
    """
    try:
        driver.get("https://traces61.tdscpc.gov.in/app/ded/justrepdwnld.xhtml")
        time.sleep(3)

        fin_year_select = Select(WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "finYr"))
        ))
        fy = q.get("financial_year") or ""
        year = fy.split("-")[0]
        fin_year_select.select_by_value(year)

        quarter_select = Select(WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "qrtr"))
        ))
        quarter_value = _JR_QUARTER_VALUE_MAP.get(q.get("quarter"))
        if quarter_value:
            quarter_select.select_by_value(quarter_value)

        form_type_select = Select(WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "frmType"))
        ))
        if q.get("form_type"):
            form_type_select.select_by_value(q["form_type"])

        go_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "download_justReport"))
        )
        go_button.click()
    except Exception as e:
        logger.error("Error in go_to_justification_report_request_form: %s", str(e))


def fill_values_for_justification_report(driver, q):
    """Fill the TRACES KYC/JR-request form from the quarter's values, submit, and
    capture the new request number + authcode.

    Ported from fill_values_for_justification_report(). The DB write of
    authcode_value / request_number is replaced with stamping them back onto the
    `q` dict so the caller can return them. Returns (ok, message).
    """
    try:
        time.sleep(3)

        token_number_field = driver.find_element(By.ID, "token")
        cin_bin_check = driver.find_element(By.ID, "cinbinCheck")
        bk_entry_check = driver.find_element(By.ID, "bkEntryCheck")
        bsr = driver.find_element(By.ID, "bsr")
        dtoftaxdep = driver.find_element(By.ID, "dtoftaxdep")
        csn = driver.find_element(By.ID, "csn")
        chlnamt = driver.find_element(By.ID, "chlnamt")
        cdrecnum = driver.find_element(By.ID, "cdrecnum")
        pan_amt_check = driver.find_element(By.ID, "panAmtCheck")

        pan_1 = driver.find_element(By.ID, "pan1")
        pan_2 = driver.find_element(By.ID, "pan2")
        pan_3 = driver.find_element(By.ID, "pan3")
        amt_1 = driver.find_element(By.ID, "amt1")
        amt_2 = driver.find_element(By.ID, "amt2")
        amt_3 = driver.find_element(By.ID, "amt3")

        click_KYC = driver.find_element(By.ID, "clickKYC")

        if q.get("token_number"):
            token_number_field.send_keys(q["token_number"])

        if q.get("cin_check_1"):
            cin_bin_check.click()
            _accept_alert_if_present(driver)

        if q.get("cin_check_2"):
            bk_entry_check.click()
            _accept_alert_if_present(driver)

        driver.execute_script("arguments[0].scrollIntoView(true);", bsr)

        if q.get("bsr_code_receipt_number"):
            bsr.send_keys(q["bsr_code_receipt_number"])

        if q.get("date_on_which_tax_deposited"):
            dtoftaxdep.send_keys(_format_date_ddmmmyyyy(q["date_on_which_tax_deposited"]))

        if q.get("challan_serial_number"):
            csn.send_keys(q["challan_serial_number"])

        if q.get("challan_amount"):
            chlnamt.send_keys(str(q["challan_amount"]))

        if q.get("cd_record_number"):
            cdrecnum.send_keys(q["cd_record_number"])

        if q.get("pan_amount_check"):
            pan_amt_check.click()
            _accept_alert_if_present(driver)

        if q.get("pan_1"):
            pan_1.send_keys(q["pan_1"])
        if q.get("pan_2"):
            pan_2.send_keys(q["pan_2"])
        if q.get("pan_3"):
            pan_3.send_keys(q["pan_3"])
        if q.get("amt_1"):
            amt_1.send_keys(str(q["amt_1"]))
        if q.get("amt_2"):
            amt_2.send_keys(str(q["amt_2"]))
        if q.get("amt_3"):
            amt_3.send_keys(str(q["amt_3"]))

        time.sleep(2)
        driver.execute_script("arguments[0].scrollIntoView(true);", click_KYC)
        click_KYC.click()
        _accept_alert_if_present(driver)

        time.sleep(3)

        # If the form surfaced a validation summary, bail with its text.
        try:
            error_div = WebDriverWait(driver, 5).until(
                EC.visibility_of_element_located((By.ID, "err_Summary"))
            )
            error_text = error_div.get_attribute("innerText").strip() or "Invalid Details"
            logger.error("JR form error: %s", error_text)
            return False, error_text
        except Exception:
            pass

        authcode_element = driver.find_element(By.ID, "authcode")
        authcode_value = authcode_element.get_attribute("value")

        proceed_transaction_btn = driver.find_element(By.ID, "redirect")
        proceed_transaction_btn.click()

        go_to_justification_report_request_form(driver, q)

        request_number = ""
        try:
            wait = WebDriverWait(driver, 10)
            div_element = wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.margintop20"))
            )
            h5_element = div_element.find_element(By.TAG_NAME, "h5")
            match = re.search(r"Request Number is\s+(\d+)", h5_element.text)
            if match:
                request_number = match.group(1)
                logger.info("Extracted request number: %s", request_number)
        except Exception as e:
            logger.error("Error extracting request number: %s", str(e))

        q["authcode_value"] = authcode_value
        q["request_number"] = request_number

        return True, "Justification report request submitted successfully."

    except Exception as e:
        logger.error("Error in fill_values_for_justification_report: %s", str(e))
        logger.error("JR fill traceback: %s", traceback.format_exc())
        return False, "Invalid Details and Technical Issue."


def find_latest_download(directory, timeout=30):
    """Poll a download directory for the newest completed file. Carried AS-IS."""
    try:
        end_time = time.time() + timeout
        while time.time() < end_time:
            files = [
                os.path.join(directory, f)
                for f in os.listdir(directory)
                if os.path.isfile(os.path.join(directory, f)) and not f.endswith(".crdownload")
            ]
            if files:
                return max(files, key=os.path.getctime)
            time.sleep(1)
        return None
    except Exception as e:
        logger.error("Error finding latest download: %s", str(e))
        return None


def check_is_justification_report_available(driver, user_download_dir, q):
    """If a request number exists, look it up and download when 'Available';
    otherwise open the request form and submit a fresh request.

    Ported from check_is_justification_report_available(). The DB attach + S3
    upload + unzip steps are replaced with reading the downloaded file's bytes
    and stamping them (base64) onto the `q` dict so the caller returns them; the
    backend uploads to S3 + unzips via its existing helpers. Returns
    (ok, message).
    """
    try:
        if q.get("request_number"):
            driver.get("https://traces61.tdscpc.gov.in/app/ded/filedownload.xhtml")
            time.sleep(3)

            request_number_input = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "reqNo"))
            )
            request_number_input.clear()
            request_number_input.send_keys(q["request_number"])

            go_button = driver.find_element(By.ID, "getListByReqId")
            go_button.click()
            time.sleep(2)

            try:
                error_message = driver.find_element(By.ID, "message")
                if "Invalid Request Number" in error_message.text:
                    return False, f"Invalid Request Number: {q['request_number']}"
            except NoSuchElementException:
                pass

            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//table[contains(@id, 'reqList')]"))
                )
                request_row = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((
                        By.XPATH,
                        f"//td[@aria-describedby='reqList_reqNo' and @title='{q['request_number']}']/../..",
                    ))
                )
                status_cell = request_row.find_element(
                    By.XPATH, ".//td[@aria-describedby='reqList_status']"
                )
                if status_cell.get_attribute("title") == "Available":
                    request_row.click()
                    http_download_button = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.ID, "downloadhttp"))
                    )
                    http_download_button.click()
                    time.sleep(5)

                    downloaded_file_path = find_latest_download(user_download_dir)
                    if downloaded_file_path:
                        # DB→JSON: return the downloaded JR file as base64 so the
                        # backend can upload it to S3 + unzip via its existing
                        # helpers (the lambda does not persist).
                        try:
                            with open(downloaded_file_path, "rb") as f:
                                content = f.read()
                            q["jr_file_name"] = os.path.basename(downloaded_file_path)
                            q["jr_file_b64"] = base64.b64encode(content).decode("ascii")
                        except Exception as fe:
                            logger.error("Failed reading JR file bytes: %s", str(fe))
                    else:
                        logger.error(
                            "Failed to download JR file for %s", q["request_number"]
                        )
                    return True, "Justification report downloaded successfully."

                return (
                    False,
                    f"Report not available for request: {q['request_number']}, "
                    f"report status is {status_cell.get_attribute('title')}",
                )
            except TimeoutException:
                return False, f"Request number {q['request_number']} not found in table"

        # No request number yet → open the form and submit a fresh request.
        go_to_justification_report_request_form(driver, q)
        return fill_values_for_justification_report(driver, q)

    except Exception as e:
        logger.error("Error in check_is_justification_report_available: %s", str(e))
        return False, "Technical Error While Processing For Justification Report."


def process_justification_reports(access, refresh, financial_years):
    """Run the JR request/download flow for every scraped quarter.

    Drives a single fresh bridged driver (the JR pages are navigated serially;
    each quarter's request → download is independent). The JR result fields
    (request_number, authcode_value, last_justification_message, and — when a
    file downloaded — jr_file_b64 + jr_file_name) are written back onto each
    quarter dict in place. Best-effort: a per-quarter failure is logged and the
    message is stamped; it never aborts the others.
    """
    download_dir = f"/tmp/tds_jr_{uuid.uuid4().hex[:8]}"
    os.makedirs(download_dir, exist_ok=True)
    driver = None
    try:
        driver = setup_driver(download_dir)
        if not bridge_into_driver(driver, access, refresh):
            logger.error("JR: bridge did not yield an authenticated session")
            return
        for fy in financial_years:
            for q in fy.get("quarters", []):
                try:
                    ok, message = check_is_justification_report_available(driver, download_dir, q)
                    q["last_justification_message"] = message
                    logger.info("JR for %s %s: ok=%s msg=%s",
                                fy.get("financial_year"), q.get("quarter"), ok, message)
                except Exception as e:
                    logger.error("JR error for %s %s: %s",
                                 fy.get("financial_year"), q.get("quarter"), str(e))
                    q["last_justification_message"] = "Technical Error While Processing For Justification Report."
    except Exception as e:
        logger.error("Error in process_justification_reports: %s", str(e))
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


# ===========================================================================
# Worker webhook — COPIED VERBATIM from fetch_gst_notices_lambda.py so the
# backend's existing compat_update_worker_result receives an identical payload.
# ===========================================================================
def send_worker_webhook(webhook_config, client_name, portal_type, result, execution_time):
    """Send individual worker results back to the FastAPI webhook.

    URL resolution priority (first non-empty wins):
        1. webhook_config["worker_callback_url"]  — fully-qualified, set by
           the orchestrator when it wants a custom endpoint.
        2. webhook_config["url"] + standard path  — the common case; the
           orchestrator passes the base URL from the backend's
           `WEBHOOK_BASE_URL` env var via the event payload.
        3. WEBHOOK_BASE_URL env var on THIS lambda  — last-resort fallback
           so a redeployed orchestrator that forgot to inject the
           webhook_config can still reach the backend (defence in depth).
           Set on the lambda's environment configuration so it ships with
           the same value the backend uses.

    Returns False (and logs) when none of the three is set — caller treats
    a False return as a no-op.
    """
    webhook_config = webhook_config or {}

    # 1 + 2 — try the event-supplied URLs first.
    callback_url = webhook_config.get("worker_callback_url")
    base_url = webhook_config.get("url")

    # 3 — fall back to the lambda's own env var if the event omitted both.
    if not callback_url and not base_url:
        env_base = (os.environ.get("WEBHOOK_BASE_URL") or "").strip().rstrip("/")
        if env_base:
            base_url = env_base
            logger.info(
                f"webhook_config missing url; falling back to "
                f"WEBHOOK_BASE_URL env for {client_name}"
            )

    if not callback_url and not base_url:
        logger.error(
            f"No webhook URL available for {client_name} — neither event "
            f"nor WEBHOOK_BASE_URL env was set; skipping callback."
        )
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


# ===========================================================================
# Orchestration entry point.
# ===========================================================================
def process_tds_notices(client_name, tan, username, password, with_jr=True):
    """Run the full TDS hybrid fetch for one client and return a result dict.

    Flow: api_login (new portal) → setup_driver + bridge_into_driver (old
    portal) → scrape FYs/quarters (parallel) → optional JR → return a JSON
    structure the webhook ships to FastAPI (which persists via its existing
    get_or_create_quarter). Never touches a DB.

    Returns:
        {"success": True, "client_name", "financial_years": [
            {"financial_year","manual_demand","processed_demand","quarters":[
                {"quarter","form_type","net_payable","statement_token",
                 "order_passed_date","demand_detail_html",
                 # JR fields (present when with_jr and the flow ran):
                 "request_number","authcode_value","last_justification_message",
                 "jr_file_b64","jr_file_name"},
                ...]},
            ...]}
        or {"success": False, "error": "..."} on login/scrape failure.
    """
    driver = None
    try:
        # 1. New-portal API login → access + refresh JWT (captcha solved inside).
        try:
            access, refresh = api_login(tan or username, password)
        except TracesApiError as e:
            logger.error("TDS API login failed for %s: %s", client_name, str(e))
            return {"success": False, "client_name": client_name, "error": str(e)}
        if not access:
            return {"success": False, "client_name": client_name,
                    "error": "login succeeded but no token returned"}

        # 2. Bridge the main driver into an authenticated old-portal session.
        driver = setup_driver(f"/tmp/tds_main_{uuid.uuid4().hex[:8]}")
        if not bridge_into_driver(driver, access, refresh):
            return {"success": False, "client_name": client_name,
                    "error": "bridge did not yield an authenticated session"}

        logger.info("TDS hybrid login successful for %s", client_name)

        # 3. Scrape every FY/quarter (parallel quarters bridge their own drivers
        #    from the same JWT — passed through explicitly).
        financial_years = scrape_demand_and_quarters(driver, access, refresh)

        # 4. Optional Justification-Report request/download per quarter.
        if with_jr and financial_years:
            try:
                process_justification_reports(access, refresh, financial_years)
            except Exception as e:
                logger.error("JR phase failed for %s: %s", client_name, str(e))

        return {
            "success": True,
            "client_name": client_name,
            "financial_years": financial_years or [],
        }

    except Exception as e:
        logger.error("process_tds_notices failed for %s: %s", client_name, str(e))
        logger.error(traceback.format_exc())
        return {"success": False, "client_name": client_name, "error": str(e)}
    finally:
        if driver:
            try:
                logout_user(driver)
            except Exception:
                pass
            try:
                driver.quit()
            except Exception:
                pass


def lambda_handler(event, context):
    """AWS Lambda handler for TDS hybrid notice fetching.

    Expected event structure:
    {
        "client_name": "TDS-CLT-12345",
        "tan": "DELT12345A",
        "username": "TRACES_USERNAME",
        "password": "PASSWORD",
        "with_jr": true,                  # optional, default True
        "webhook_config": { ... }         # optional; routes results to backend
    }

    Returns dict {statusCode, body}. On webhook_config present, also posts the
    worker result to the backend's compat route (same as GST/IT).
    """
    handler_start_time = time.time()
    try:
        client_name = event.get('client_name')
        tan = event.get('tan')
        username = event.get('username')
        password = event.get('password')
        with_jr = event.get('with_jr', True)

        if not (tan or username) or not password or not client_name:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'Missing required parameters: client_name, password, and (tan or username)'
                })
            }

        logger.info("Starting TDS hybrid notice fetch for client: %s", client_name)

        result = process_tds_notices(client_name, tan, username, password, with_jr=with_jr)

        response = {
            'statusCode': 200 if result.get('success') else 401,
            'body': json.dumps(result, default=str)
        }

        # Send results directly to the FastAPI webhook (fire-and-forget).
        webhook_config = event.get('webhook_config')
        if webhook_config:
            execution_time = round(time.time() - handler_start_time, 2)
            worker_result = {
                'success': bool(result.get('success')),
                'client_info': {'client_name': client_name, 'portal': 'tds', 'username': username},
                'function_name': 'fetch_tds_notices_fastapi_lambda_hybrid',
                'response': response,
                'status_code': response['statusCode'],
                'execution_time_seconds': execution_time,
            }
            if not result.get('success'):
                worker_result['error'] = result.get('error')
            send_worker_webhook(webhook_config, client_name, 'tds', worker_result, execution_time)

        return response

    except Exception as e:
        logger.error("Lambda handler error: %s", str(e))
        logger.error(traceback.format_exc())
        error_response = {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'traceback': traceback.format_exc()
            })
        }

        webhook_config = event.get('webhook_config')
        if webhook_config:
            execution_time = round(time.time() - handler_start_time, 2)
            worker_result = {
                'success': False,
                'client_info': {'client_name': event.get('client_name', 'Unknown'), 'portal': 'tds'},
                'function_name': 'fetch_tds_notices_fastapi_lambda_hybrid',
                'response': error_response,
                'error': str(e),
                'execution_time_seconds': execution_time,
            }
            send_worker_webhook(webhook_config, event.get('client_name', 'Unknown'), 'tds', worker_result, execution_time)

        return error_response


# For local testing
if __name__ == "__main__":
    test_event = {
        "client_name": "TDS-CLT-001",
        "tan": "DELT12345A",
        "username": "DELT12345A",
        "password": "test_password",
        "with_jr": True,
    }

    out = lambda_handler(test_event, None)
    print(json.dumps(out, indent=2, default=str))
