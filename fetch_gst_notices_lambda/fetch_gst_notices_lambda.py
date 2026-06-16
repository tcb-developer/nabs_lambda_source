"""
AWS Lambda function for fetching GST notices using Selenium

This Lambda function uses headless Chrome to scrape GST portal notices.
Requires Lambda Layer with Chromium and Selenium.

Lambda Layer ARN (use appropriate region):
- chrome-aws-lambda or selenium-chromium layer
"""

import time
from datetime import datetime
import os
import re
import json
import traceback
import logging
import boto3
from botocore.client import Config
import mimetypes
import random
import string
import requests
import tempfile
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Selenium imports - these will work when Lambda Layer is attached
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.by import By
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.common.action_chains import ActionChains
    from selenium.common.exceptions import TimeoutException, NoSuchElementException
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    logger.warning("Selenium not available - running in test mode")

# AWS S3 Configuration. Credentials are NOT hardcoded — they come from the
# Lambda execution role by default (the role carries finbuddy-lambda-s3-policy).
# Explicit keys are honoured only if AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
# are set as env vars (local/dev), otherwise boto3 resolves the role creds.
AWS_REGION = os.environ.get("AWS_REGION", "ap-south-1")
S3_BUCKET = os.environ.get("S3_BUCKET_NAME", "nabsprodbucket")

# 2Captcha API Key
CAPTCHA_API_KEY = "72808e06303338084d893648f0146162"

# Shared S3 client (reused across uploads to avoid per-call initialization overhead)
_s3_client = None

def _get_s3_client():
    """Get or create a shared S3 client (saves ~0.5s per upload vs creating new client each time)"""
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


def get_frappe_date(date_string):
    """Convert date string to Frappe format (YYYY-MM-DD)"""
    if not date_string or not isinstance(date_string, str):
        return None

    date_string = date_string.strip()

    if date_string.upper() in ('NA', 'N/A', 'NONE', '-', ''):
        return None

    formats_to_try = [
        "%d-%b-%Y",  # 08-Jun-2022
        "%d/%m/%Y",  # 02/02/2024
        "%Y-%m-%d"   # Already in Frappe format
    ]

    for date_format in formats_to_try:
        try:
            parsed_date = datetime.strptime(date_string, date_format)
            return parsed_date.strftime("%Y-%m-%d")
        except ValueError:
            continue

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

    final_key = f"{year}/{month}/{day}/{key}_{file_name}"
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

        # Clean up local file to free /tmp space (Lambda has 512MB /tmp)
        if cleanup:
            try:
                os.remove(file_path)
            except OSError:
                pass
    except Exception as e:
        logger.error(f"Error uploading to S3: {str(e)}")
        file_size = 0

    return {"file_name": file_name, "file_url": file_url, "content_hash": key, "s3_key": key, "file_size": file_size}


def setup_chrome_options(download_dir, tmp_folder):
    """Configure Chrome options for Lambda environment"""
    options = Options()

    # Check if running in Lambda environment
    is_lambda = os.environ.get('AWS_LAMBDA_FUNCTION_NAME')

    # Set timezone to IST so Chrome renders dates in Indian timezone
    # (GST portal uses browser timezone for date display via JavaScript)
    os.environ["TZ"] = "Asia/Kolkata"

    if is_lambda:
        # Lambda Layer paths
        options.binary_location = "/opt/headless-chromium"
        # Set FONTCONFIG_PATH for font rendering in Lambda
        os.environ["FONTCONFIG_PATH"] = "/opt/etc/fonts"

        # Chrome arguments matching Lambda layer's proven configuration
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--single-process")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--hide-scrollbars")
        options.add_argument("--enable-logging")
        options.add_argument("--ignore-certificate-errors")
        options.add_argument("--log-level=0")
        options.add_argument("--v=0")
        options.add_argument(f"--window-size=1280,1696")
        options.add_argument(f"--user-data-dir={tmp_folder}/user-data")
        options.add_argument(f"--data-path={tmp_folder}/data-path")
        options.add_argument(f"--homedir={tmp_folder}")
        options.add_argument(f"--disk-cache-dir={tmp_folder}/cache-dir")
        options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36")
    else:
        # Local/server Chrome arguments
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-setuid-sandbox")
        options.add_argument("--disable-software-rasterizer")
        options.add_argument("--disable-extensions")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-web-security")
        options.add_argument("--allow-running-insecure-content")
        options.add_argument("--ignore-certificate-errors")
        options.add_argument("--homedir=/tmp")
        options.add_argument("--disk-cache-dir=/tmp/cache-dir")
        options.add_experimental_option("useAutomationExtension", False)
        options.add_experimental_option('excludeSwitches', ['enable-automation'])

    prefs = {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": False,
        "safebrowsing.disable_download_protection": True,
        "profile.default_content_setting_values.automatic_downloads": 1,
        "plugins.always_open_pdf_externally": True,
    }
    options.add_experimental_option("prefs", prefs)

    return options


def setup_driver(download_dir):
    """Setup Chrome WebDriver for Lambda"""
    # Create unique tmp folder for Chrome data
    tmp_folder = f"/tmp/{uuid.uuid4()}"
    for sub in ["", "/user-data", "/data-path", "/cache-dir"]:
        os.makedirs(tmp_folder + sub, exist_ok=True)

    options = setup_chrome_options(download_dir, tmp_folder)

    # Check if running in Lambda environment
    if os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
        driver = webdriver.Chrome("/opt/chromedriver", options=options)
    else:
        # For local testing
        driver = webdriver.Chrome(options=options)

    return driver




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

def solve_captcha(captcha_image_path):
    """Solve CAPTCHA using 2Captcha API"""
    try:
        # Upload CAPTCHA
        with open(captcha_image_path, "rb") as f:
            files = {"file": f}
            data = {"key": CAPTCHA_API_KEY, "method": "post"}
            response = requests.post("http://2captcha.com/in.php", files=files, data=data, timeout=30)

        if not response.text.startswith("OK"):
            logger.error(f"Error uploading CAPTCHA: {response.text}")
            return None

        captcha_id = response.text.split("|")[1]

        # Wait for solution
        result_url = f"http://2captcha.com/res.php?key={CAPTCHA_API_KEY}&action=get&id={captcha_id}"
        for attempt in range(40):
            time.sleep(3 + attempt * 0.5)
            result_response = requests.get(result_url, timeout=10)

            if result_response.text.startswith("OK"):
                return result_response.text.split("|")[1]
            elif "CAPCHA_NOT_READY" not in result_response.text:
                logger.error(f"CAPTCHA error: {result_response.text}")
                return None

        logger.error("CAPTCHA solving timeout")
        return None

    except Exception as e:
        logger.error(f"Error solving CAPTCHA: {str(e)}")
        return None


def login_user(driver, username, password, download_dir):
    """Login to GST portal"""
    response_data = {"success": False, "msg": "Something went wrong!", "error": ""}

    try:
        wait = WebDriverWait(driver, 30)

        driver.get("https://services.gst.gov.in/services/login")

        # Wait for page to load
        wait.until(lambda d: d.execute_script('return document.readyState') == 'complete')

        # Enter Username
        username_field = wait.until(EC.presence_of_element_located((By.ID, "username")))
        username_field.clear()
        username_field.send_keys(username)

        # Wait for CAPTCHA image to fully render (matching sequential flow)
        time.sleep(5)

        # Wait for CAPTCHA image element to appear
        wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@id="imgCaptcha"]')))

        # Capture and solve CAPTCHA
        captcha_path = os.path.join(download_dir, "captcha.png")
        captcha_element = wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@id="imgCaptcha"]')))
        captcha_element.screenshot(captcha_path)

        captcha_text = solve_captcha(captcha_path)
        if not captcha_text:
            response_data["error"] = "Failed to solve CAPTCHA"
            return response_data

        # Enter CAPTCHA
        captcha_input = wait.until(EC.visibility_of_element_located((By.XPATH, '//*[@id="captcha"]')))
        captcha_input.clear()
        captcha_input.send_keys(captcha_text.strip())

        # Enter Password
        password_field = wait.until(EC.visibility_of_element_located((By.ID, "user_pass")))
        password_field.clear()
        password_field.send_keys(password)

        # Click Login
        login_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "button.btn-primary")))
        try:
            login_btn.click()
        except:
            driver.execute_script("arguments[0].click();", login_btn)

        # Wait for either dashboard (success) or error message (failure) — replaces fixed time.sleep(8)
        try:
            WebDriverWait(driver, 30).until(
                lambda d: d.find_elements(By.CSS_SELECTOR, "p.tp-dash-ttl")
                or d.find_elements(By.XPATH, "//div[contains(@class, 'err') and .//div[contains(@class, 'alert alert-danger')]] | //span[contains(@class, 'err')]")
            )
        except:
            pass

        # Check for login errors
        try:
            login_error = driver.find_element(By.XPATH, "//div[contains(@class, 'err') and .//div[contains(@class, 'alert alert-danger')]] | //span[contains(@class, 'err')]")
            if login_error and login_error.text:
                response_data["error"] = login_error.text
                return response_data
        except:
            pass

        # Check for dashboard
        try:
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "p.tp-dash-ttl")))
            response_data["success"] = True
            response_data["msg"] = "Login successful!"
            return response_data
        except:
            response_data["error"] = "Dashboard not visible after login"
            return response_data

    except Exception as e:
        response_data["error"] = f"Login failed: {str(e)}"
        logger.error(traceback.format_exc())
        return response_data


def navigate_to_notices(driver):
    """Navigate to notices page. Returns no_records=True if page loaded but has no notices."""
    try:
        wait = WebDriverWait(driver, 30)

        notices_link = wait.until(
            EC.presence_of_element_located((By.XPATH, "//a[@href='//services.gst.gov.in/services/auth/notices' and text()='View Notices and Orders']"))
        )
        driver.execute_script("arguments[0].click();", notices_link)

        # Wait for either the notices table OR a "no records" indicator
        try:
            WebDriverWait(driver, 15).until(
                lambda d: (
                    d.find_elements(By.CSS_SELECTOR, 'table[data-ng-table="searchPagination"]')
                    or d.find_elements(By.XPATH, "//*[contains(translate(text(),'NORECRD','norecrd'),'no record')]")
                    or d.find_elements(By.CSS_SELECTOR, '.no-data, .empty-state, .no-record')
                )
            )
        except Exception:
            pass

        # Check if the table exists
        tables = driver.find_elements(By.CSS_SELECTOR, 'table[data-ng-table="searchPagination"]')
        if tables:
            return {"success": True, "no_records": False}

        # Table not found - check if page loaded with "no records" message
        page_text = (driver.page_source or "").lower()
        if "no record" in page_text or "no data available" in page_text:
            logger.warning("[DIAG] Notices page loaded but has no records")
            return {"success": True, "no_records": True}

        # Check if we are on the notices page by URL
        current_url = driver.current_url or ""
        if "notices" in current_url.lower():
            logger.warning(f"[DIAG] On notices page (URL: {current_url}) but no table - treating as no records")
            return {"success": True, "no_records": True}

        logger.error(f"[DIAG] Navigation uncertain - URL: {current_url}")
        return {"success": False, "error": "Navigation completed but notices page not detected"}

    except Exception as e:
        logger.error(f"Navigation error: {str(e)}")
        return {"success": False, "error": str(e)}


def find_latest_download(directory, timeout=30):
    """Find the latest downloaded file"""
    try:
        end_time = time.time() + timeout
        while time.time() < end_time:
            files = [os.path.join(directory, f) for f in os.listdir(directory)
                     if os.path.isfile(os.path.join(directory, f)) and not f.endswith(".crdownload")]
            if files:
                return max(files, key=os.path.getctime)
            time.sleep(1)
        return None
    except Exception:
        return None


def get_downloaded_files(user_dir):
    """Get list of downloaded files"""
    try:
        return [x for x in os.listdir(user_dir) if os.path.isfile(os.path.join(user_dir, x))]
    except:
        return []


def click_100_button(driver):
    """Click the 100 items per page button"""
    try:
        wait = WebDriverWait(driver, 10)
        button_100 = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[./span[text()='100']]")))
        button_100.click()
        return True
    except:
        try:
            driver.execute_script("arguments[0].click();", driver.find_element(By.XPATH, "//button[./span[text()='100']]"))
            return True
        except:
            return False


def _wait_for_file_stable(file_path, max_wait=5):
    """Wait for file size to stabilize (Chrome may still be writing). Returns final size."""
    prev_size = -1
    for _ in range(max_wait * 2):  # check every 0.5s
        try:
            curr_size = os.path.getsize(file_path)
            if curr_size > 0 and curr_size == prev_size:
                return curr_size
            prev_size = curr_size
        except OSError:
            pass
        time.sleep(0.5)
    try:
        return os.path.getsize(file_path)
    except OSError:
        return 0


def _navigate_back_to_notices(driver, context_msg):
    """Navigate back to notices page using 3-strategy recovery.
    Returns True if successful, False if session lost."""
    notices_url = "https://services.gst.gov.in/services/auth/notices"
    table_css = 'table[data-ng-table="searchPagination"]'
    row_css = 'tr[ng-repeat="detail in $data"]'

    def _verify_notices_page():
        """Check current page is the notices page with rows."""
        current_url = driver.current_url
        if "error" in current_url or "accessdenied" in current_url:
            logger.warning(f"On error page: {current_url} ({context_msg})")
            return False
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, table_css))
            )
            click_100_button(driver)
            time.sleep(1)  # Brief settle after pagination change
            verify_rows = driver.find_elements(By.CSS_SELECTOR, row_css)
            if len(verify_rows) > 0:
                logger.info(f"Notices page OK. Found {len(verify_rows)} rows ({context_msg})")
                return True
            logger.warning(f"Table found but 0 rows ({context_msg})")
            return False
        except Exception:
            return False

    # Strategy 1: Browser back button (preserves Angular state)
    try:
        logger.info(f"Strategy 1: browser back ({context_msg})")
        driver.back()
        time.sleep(1)
        if _verify_notices_page():
            return True
    except Exception as e:
        logger.warning(f"Browser back failed: {str(e)} ({context_msg})")

    # Strategy 2: Click "Notices and Orders" link
    try:
        logger.info(f"Strategy 2: click 'Notices and Orders' link ({context_msg})")
        notices_link = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'Notices and Orders') or contains(text(), 'Notices & Orders')]"))
        )
        driver.execute_script("arguments[0].click();", notices_link)
        time.sleep(1)
        if _verify_notices_page():
            return True
    except Exception as e:
        logger.warning(f"Click link failed: {str(e)} ({context_msg})")

    # Strategy 3: Direct URL navigation (last resort)
    for attempt in range(1, 3):
        try:
            logger.info(f"Strategy 3 attempt {attempt}: direct URL ({context_msg})")
            driver.get(notices_url)
            time.sleep(2)
            if _verify_notices_page():
                return True
        except Exception as e:
            logger.warning(f"Direct URL attempt {attempt} failed: {str(e)} ({context_msg})")
            time.sleep(3)

    logger.error(f"All navigation strategies failed ({context_msg})")
    return False


def extract_notice_data(driver, download_dir, client_name, existing_ref_ids=None, existing_phase1_ref_ids=None):
    """Extract data from the merged notices table (handles both phase1 and phase2 rows).
    GST portal merged 'Notices and Orders' + 'Additional Notices and Orders' into one table (Feb 2026).
    Phase1 rows = simple notice download, returned as 'notices'.
    Phase2 rows = case detail view with attachments, returned as 'additional_notices'.
    existing_ref_ids: list of phase2 ref_ids already in Frappe (with case_details) — these are skipped.
    existing_phase1_ref_ids: list of phase1 ref_ids already in Frappe (with notice_letter) — download skipped.

    Returns: (notices_data, additional_notices_data)
    """
    existing_ref_ids_set = set(existing_ref_ids or [])
    existing_phase1_ref_ids_set = set(existing_phase1_ref_ids or [])
    try:
        wait = WebDriverWait(driver, 20)

        # Wait for the new merged table
        table_css = 'table[data-ng-table="searchPagination"]'
        table = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, table_css)))

        # Click "100" pagination button to load all rows
        click_100_button(driver)
        # Wait for rows to appear after pagination change
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, row_css))
            )
        except:
            pass  # May have no rows

        # Find all data rows
        row_css = 'tr[ng-repeat="detail in $data"]'
        rows = driver.find_elements(By.CSS_SELECTOR, row_css)
        logger.warning(f"[DIAG] Found {len(rows)} rows in merged notices table for {client_name}")

        # First pass: Classify and extract basic data from all rows
        all_row_data = []
        for i, row in enumerate(rows):
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) < 6:
                continue

            try:
                notice_id_el = cols[0].find_elements(By.CSS_SELECTOR, 'div[data-ng-bind="detail.noticeOrderId"]')
                notice_id = notice_id_el[0].text.strip() if notice_id_el else cols[0].text.strip()
            except Exception:
                notice_id = cols[0].text.strip()

            notice_type = cols[1].text.strip()
            description = cols[2].text.strip()
            issue_date_text = cols[3].text.strip()
            due_date_text = cols[4].text.strip()
            logger.warning(f"[DIAG] Row {i} raw dates: issue='{issue_date_text}' -> {get_frappe_date(issue_date_text)}, due='{due_date_text}' -> {get_frappe_date(due_date_text)}")

            # Determine phase from Action column (cols[5])
            action_col = cols[5]
            phase2_divs = action_col.find_elements(By.CSS_SELECTOR, 'div[data-ng-if*="phase2"]')

            if phase2_divs:
                source_type = "phase2"
            else:
                click_view_links = action_col.find_elements(By.CSS_SELECTOR, 'a[ng-click*="clickView"]')
                source_type = "phase2" if click_view_links else "phase1"

            row_data = {
                'index': i,
                'ref_id': notice_id,
                'type': notice_type,
                'description': description,
                'issue_date': get_frappe_date(issue_date_text),
                'due_date': get_frappe_date(due_date_text),
                'source_type': source_type,
            }
            all_row_data.append(row_data)

        phase1_count = len([r for r in all_row_data if r['source_type'] == 'phase1'])
        phase2_count = len([r for r in all_row_data if r['source_type'] == 'phase2'])
        # Count new vs existing notices
        new_phase1 = [r for r in all_row_data if r['source_type'] == 'phase1' and r['ref_id'] not in existing_phase1_ref_ids_set]
        new_phase2 = [r for r in all_row_data if r['source_type'] == 'phase2' and r['ref_id'] not in existing_ref_ids_set]
        logger.warning(f"[DIAG] Classified: {phase1_count} phase1 ({len(new_phase1)} new), {phase2_count} phase2 ({len(new_phase2)} new). Skipping {len(existing_phase1_ref_ids_set)} phase1 + {len(existing_ref_ids_set)} phase2 existing.")

        # EARLY EXIT: if all notices are already in Frappe, return metadata only (no downloads needed)
        if not new_phase1 and not new_phase2:
            logger.warning(f"[DIAG] ALL NOTICES UNCHANGED for {client_name} — skipping entire download phase")
            notices_data = []
            for row_data in all_row_data:
                if row_data['source_type'] == 'phase1':
                    notices_data.append({
                        'ref_id': row_data['ref_id'], 'type': row_data['type'],
                        'description': row_data['description'],
                        'issue_date': row_data['issue_date'], 'due_date': row_data['due_date'],
                        'notice_letter': {},
                    })
            add_notices_data = []
            for row_data in all_row_data:
                if row_data['source_type'] == 'phase2':
                    add_notices_data.append({
                        'ref_id': row_data['ref_id'], 'type': row_data['type'],
                        'description': row_data['description'],
                        'issue_date': row_data['issue_date'], 'due_date': row_data['due_date'],
                        'case_details': {},
                    })
            logger.warning(f"[DIAG] SUMMARY: {len(notices_data)} phase1 + {len(add_notices_data)} phase2 notices for {client_name} (ALL SKIPPED — unchanged)")
            return notices_data, add_notices_data

        # Process Phase 1 rows (simple notice download)
        notices_data = []
        expected_row_count = len(rows)

        for row_data in all_row_data:
            if row_data['source_type'] != 'phase1':
                continue

            ref_id = row_data['ref_id']
            ref_id_safe = ref_id.replace("/", "-").replace(" ", "") if ref_id and ref_id != "-" and "/" in ref_id else ref_id

            notice_data = {
                'ref_id': ref_id,
                'type': row_data['type'],
                'description': row_data['description'],
                'issue_date': row_data['issue_date'],
                'due_date': row_data['due_date'],
                'notice_letter': {},
            }

            # Skip download for notices that already have notice_letter in Frappe
            if ref_id in existing_phase1_ref_ids_set:
                logger.info(f"SKIP phase1 download: {ref_id} (already has notice_letter in Frappe)")
                notices_data.append(notice_data)
                continue

            # Download notice file
            notice_file_name = f"GST-NTR-ODR-{ref_id_safe}-{client_name}.pdf" if ref_id_safe else None

            try:
                current_rows = driver.find_elements(By.CSS_SELECTOR, row_css)

                # If pagination reset (fewer rows than expected), re-click 100 button
                if len(current_rows) < expected_row_count:
                    logger.info(f"Pagination reset detected ({len(current_rows)} vs {expected_row_count} rows). Re-clicking 100 button.")
                    click_100_button(driver)
                    time.sleep(1)
                    current_rows = driver.find_elements(By.CSS_SELECTOR, row_css)
                    logger.info(f"After re-click: {len(current_rows)} rows")

                if row_data['index'] >= len(current_rows):
                    logger.warning(f"Phase1 row index {row_data['index']} out of range ({len(current_rows)} rows) for {ref_id}, skipping download")
                    notices_data.append(notice_data)
                    continue
                current_row = current_rows[row_data['index']]

                # Find download link directly in the row instead of relying on column index
                download_links = current_row.find_elements(By.CSS_SELECTOR, 'a[target="_blank"], a[href*="download"], a[ng-click*="download"]')
                if not download_links:
                    # Fallback: find any <a> tag in the last column
                    current_cols = current_row.find_elements(By.TAG_NAME, "td")
                    if current_cols:
                        download_links = current_cols[-1].find_elements(By.TAG_NAME, "a")

                if not download_links:
                    logger.warning(f"No download link found for phase1 notice {ref_id}")
                    notices_data.append(notice_data)
                    continue

                download_link = download_links[0]
                files_before = set(os.listdir(download_dir))
                driver.execute_script("arguments[0].click();", download_link)

                file_path = wait_for_specific_file_download(download_dir, notice_file_name, files_before, timeout=60)
                if not file_path:
                    file_path = find_latest_download(download_dir)
                if file_path and notice_file_name:
                    new_file_path = os.path.join(os.path.dirname(file_path), notice_file_name)
                    if file_path != new_file_path:
                        os.rename(file_path, new_file_path)
                    file_path = new_file_path

                    # Upload to S3
                    file_url = upload_to_s3(file_path, notice_file_name)
                    notice_data["notice_letter"] = file_url
                    logger.warning(f"[DIAG] Phase1 downloaded OK: {ref_id} -> {notice_file_name}")
                else:
                    logger.warning(f"[DIAG] Phase1 download MISSED: {ref_id}, file_path={file_path}, notice_file_name={notice_file_name}")

            except Exception as e:
                logger.warning(f"Error downloading phase1 notice file for {ref_id}: {str(e)}")

            notices_data.append(notice_data)

        logger.info(f"Processed {len(notices_data)} phase1 notices")

        # Process Phase 2 rows (case detail view)
        add_notices_data = []
        phase2_rows = [rd for rd in all_row_data if rd['source_type'] == 'phase2']

        for rd in phase2_rows:
            add_notices_data.append({
                'ref_id': rd['ref_id'],
                'type': rd['type'],
                'description': rd['description'],
                'issue_date': rd['issue_date'],
                'due_date': rd['due_date'],
                'case_details': {},
            })

        # Second pass for phase2: click View for each row to get case details
        skipped_count = 0
        session_alive = True
        for idx, row_data in enumerate(phase2_rows):
            if not session_alive:
                logger.warning(f"Skipping phase2 notice {idx+1}/{len(phase2_rows)}: {row_data['ref_id']} (session lost)")
                continue

            # Skip notices that already exist in Frappe with case_details populated
            if row_data['ref_id'] in existing_ref_ids_set:
                skipped_count += 1
                logger.info(f"SKIP phase2 {idx+1}/{len(phase2_rows)}: {row_data['ref_id']} (already in Frappe)")
                continue

            try:
                current_rows = driver.find_elements(By.CSS_SELECTOR, row_css)
                logger.info(f"Re-fetched {len(current_rows)} rows. Need index {row_data['index']} for {row_data['ref_id']}")

                if row_data['index'] >= len(current_rows):
                    logger.error(f"Row index {row_data['index']} out of range ({len(current_rows)} rows). Trying to recover.")
                    if _navigate_back_to_notices(driver, f"index recovery for {row_data['ref_id']}"):
                        current_rows = driver.find_elements(By.CSS_SELECTOR, row_css)
                        if row_data['index'] >= len(current_rows):
                            logger.error(f"Row index {row_data['index']} still out of range after recovery. Skipping.")
                            continue
                    else:
                        logger.error(f"Session lost. Cannot recover. Skipping remaining phase2 notices.")
                        session_alive = False
                        continue

                current_row = current_rows[row_data['index']]
                current_cols = current_row.find_elements(By.TAG_NAME, "td")
                action_col = current_cols[5]

                view_link = action_col.find_element(By.XPATH, ".//a[contains(text(), 'View')]")
                logger.warning(f"[DIAG] Clicking View for phase2 notice {idx+1}/{len(phase2_rows)}: {row_data['ref_id']}")

                try:
                    driver.execute_script("arguments[0].click();", view_link)
                except Exception:
                    view_link.click()

                yellowbar_xpath = "//yellowbar[contains(@detail, 'Case ID')]"
                WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.XPATH, yellowbar_xpath))
                )

                # Force a full page reload to clear any stale Angular SPA state
                # This prevents the tab content from showing data from the previous case
                driver.refresh()
                time.sleep(1)
                WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.XPATH, yellowbar_xpath))
                )

                case_detail_info = extract_notice_details(driver)
                logger.warning(f"[DIAG] Case detail page: case_id='{case_detail_info.get('case_id', '')}', expected ref_id='{row_data['ref_id']}'")
                case_detail_info['case_creation_date'] = get_frappe_date(case_detail_info.get('case_creation_date', ''))
                case_detail_info['ref_id'] = row_data['ref_id']

                case_details = download_attachments(driver, download_dir, case_detail_info, client_name)
                add_notices_data[idx]['case_details'] = case_details
                # Summarize what was found in this phase2 notice
                att_counts = {k: len(v) for k, v in case_details.items() if isinstance(v, list)}
                logger.warning(f"[DIAG] Phase2 {row_data['ref_id']} case_details sections: {att_counts}")

                if not _navigate_back_to_notices(driver, f"after processing {row_data['ref_id']}"):
                    logger.error(f"Session lost after processing {row_data['ref_id']}. Remaining phase2 notices will be skipped.")
                    session_alive = False

            except Exception as e:
                logger.error(f"Error processing phase2 notice {idx+1}: {row_data['ref_id']}: {str(e)}")
                if not _navigate_back_to_notices(driver, f"error recovery for {row_data['ref_id']}"):
                    logger.error(f"Session lost during error recovery. Skipping remaining.")
                    session_alive = False

        logger.warning(f"[DIAG] SUMMARY: {len(notices_data)} phase1 + {len(add_notices_data)} phase2 notices for {client_name} (skipped {skipped_count} existing phase2)")
        return notices_data, add_notices_data

    except Exception as e:
        logger.error(f"Notice data extraction failed for {client_name}: {str(e)}")
        logger.error(traceback.format_exc())
        return [], []


def extract_notice_details(driver):
    """Extract notice details from detail page"""
    detail_info = {}

    fields = {
        "case_id": "Case ID",
        "gstin": "GSTIN/UIN/Temporary ID",
        "case_creation_date": "Date Of Application/Case Creation",
        "status": "Status"
    }

    for key, label in fields.items():
        try:
            value = driver.find_element(By.XPATH, f"//span[contains(text(),'{label}')]/following-sibling::p/b").text.strip()
            detail_info[key] = value
        except:
            detail_info[key] = ''

    return detail_info


def wait_for_specific_file_download(directory, expected_filename, files_before, timeout=30):
    """Wait for a specific file to download"""
    start_time = time.time()
    while time.time() - start_time < timeout:
        current_files = set(os.listdir(directory))
        new_files = current_files - files_before
        complete_files = [f for f in new_files if not f.endswith('.crdownload') and not f.endswith('.tmp')]

        if complete_files:
            for file in complete_files:
                if file == expected_filename or expected_filename.lower() in file.lower():
                    return os.path.join(directory, file)
            if len(complete_files) == 1:
                return os.path.join(directory, complete_files[0])

        time.sleep(0.5)

    return None


def _wait_for_stable_rows(driver, row_xpath, max_checks=4, interval=1):
    """Wait for DOM row count to stabilize (no ghost rows from tab transitions).
    Returns the stable row list, or empty list if no rows found."""
    prev_count = -1
    stable_rows = []
    for _ in range(max_checks):
        time.sleep(interval)
        stable_rows = driver.find_elements(By.XPATH, row_xpath)
        curr_count = len(stable_rows)
        if curr_count == prev_count:
            return stable_rows  # Count is stable
        prev_count = curr_count
    return stable_rows


def process_case_details_section(driver, download_dir, ref_id, client_id, section_name, section_xpath, row_xpath, file_prefix):
    """Generic function to process case details sections (notices, replies, orders, etc.)"""
    results = []

    try:
        try:
            WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, row_xpath)))
        except Exception:
            logger.warning(f"[DIAG] Section '{section_name}' has no rows (WebDriverWait timeout) for {ref_id}")
            return results

        # Wait for row count to stabilize (ghost rows from tab transition disappear)
        rows = _wait_for_stable_rows(driver, row_xpath)
        if not rows:
            return results

        num_rows = len(rows)
        logger.warning(f"[DIAG] Processing {num_rows} stable rows in {section_name} for {ref_id}")
        row_index = 0
        for row_idx in range(num_rows):
            try:
                # Re-fetch rows each iteration to avoid stale element references
                current_rows = driver.find_elements(By.XPATH, row_xpath)
                if row_idx >= len(current_rows):
                    logger.warning(f"Row {row_idx} out of range ({len(current_rows)} rows) in {section_name} for {ref_id}")
                    continue
                row = current_rows[row_idx]

                if "No Records Found" in row.text:
                    logger.warning(f"[DIAG] 'No Records Found' in {section_name} row {row_idx} for {ref_id}, skipping")
                    continue

                def safe_get_text(xpath, default=""):
                    try:
                        elements = row.find_elements(By.XPATH, xpath)
                        return elements[0].text.strip() if elements else default
                    except:
                        return default

                data = {
                    'row_index': row_index,
                    'ref_id': ref_id,
                    'attachments': []
                }

                # Extract fields based on section type
                if section_name == "notices":
                    data['type'] = safe_get_text(".//td[2]/span[@title='Type of Notice'] | .//td[1]/span[@title='Type']")
                    data['reference_number'] = safe_get_text(".//td[contains(@title, 'Reference Number')]/span | .//td/span[contains(@title, 'Notice No.')]")
                    data['issue_date'] = get_frappe_date(safe_get_text(".//td[contains(@title, 'Issue Date')]/span | .//td/span[contains(@title, 'Issued on')]"))
                    data['due_date'] = get_frappe_date(safe_get_text(".//td[contains(@title, 'Due Date to Reply')]/span"))
                    data['personal_hearing'] = safe_get_text(".//td[contains(@title, 'Personal Hearing')]//span")
                    data['section'] = safe_get_text(".//td[contains(@title, 'Section Number')]/span")
                    data['financial_year_from'] = safe_get_text(".//td[contains(@title, 'From')]/span")
                    data['financial_year_to'] = safe_get_text(".//td[contains(@title, 'To')]/span")
                    logger.warning(f"[DIAG] Notice row {row_idx} for {ref_id}: ref='{data.get('reference_number','')}', type='{data.get('type','')}'")


                elif section_name == "replies":
                    data['type'] = safe_get_text(".//td[2]/span[@title='Order Category'] | .//td[1]/span[@title='Type']")
                    data['reply_filed_against'] = safe_get_text(".//td[2]/span[@title='Reply filed Against']")
                    data['reply_date'] = get_frappe_date(safe_get_text(".//td[3]/span[@title='Reply Date/Ph']"))
                    data['personal_hearing'] = safe_get_text(".//td[4]/span[@title='Option for Personal Hearing']")

                elif section_name == "orders":
                    # Dump ALL columns for diagnostic to discover actual column structure
                    all_tds = row.find_elements(By.XPATH, ".//td")
                    td_dump = []
                    for j, td in enumerate(all_tds):
                        td_text = td.text.strip()[:80].replace('\n', ' ')
                        # Also check spans with their title attributes
                        spans = td.find_elements(By.XPATH, ".//span[@title]")
                        span_info = ""
                        if spans:
                            span_info = " spans=[" + ",".join(f"'{s.get_attribute('title')}':'{s.text.strip()[:40]}'" for s in spans) + "]"
                        td_dump.append(f"td[{j+1}]='{td_text}'{span_info}")
                    logger.warning(f"[DIAG] Order row {row_idx} COLUMN DUMP for {ref_id}: {' | '.join(td_dump)}")

                    # Extract order data - try all possible column positions
                    # Strategy: scan all tds to find type, order_number, and order_date
                    data['type'] = ''
                    data['order_number'] = ''
                    data['order_date'] = None

                    for j, td in enumerate(all_tds):
                        spans = td.find_elements(By.XPATH, ".//span[@title]")
                        for span in spans:
                            title = (span.get_attribute('title') or '').strip()
                            text = span.text.strip()
                            if title in ('Order Category', 'Type', 'Type of Order'):
                                data['type'] = text
                            elif title in ('Order/Reference Number', 'Order Number', 'Reference Number'):
                                data['order_number'] = text
                            elif title in ('Date of Order', 'Order Date', 'Date'):
                                data['order_date'] = get_frappe_date(text)

                    # Fallback: if titled spans didn't work, try positional extraction
                    if not data['type'] and not data['order_number']:
                        # Try to identify columns by content pattern
                        for j, td in enumerate(all_tds):
                            text = td.text.strip()
                            if not text:
                                continue
                            # Date pattern
                            if not data['order_date'] and get_frappe_date(text):
                                data['order_date'] = get_frappe_date(text)
                            # Reference ID pattern (starts with ZD/ZA or contains digits)
                            elif not data['order_number'] and (text.startswith('ZD') or text.startswith('ZA')):
                                data['order_number'] = text
                            # Everything else that isn't a download button = likely order type
                            elif not data['type'] and not text.startswith('ZD') and not text.startswith('ZA') and not get_frappe_date(text) and len(text) > 2:
                                data['type'] = text

                    logger.warning(f"[DIAG] Order row {row_idx} EXTRACTED for {ref_id}: type='{data['type']}', order_number='{data['order_number']}', order_date='{data['order_date']}'")

                elif section_name == "intimations":
                    data['type'] = safe_get_text(".//td[1]")
                    data['reference_number'] = safe_get_text(".//td[2]")
                    data['issue_date'] = get_frappe_date(safe_get_text(".//td[3]"))
                    data['due_date'] = get_frappe_date(safe_get_text(".//td[4]"))
                    data['section'] = safe_get_text(".//td[5]")

                elif section_name == "ack_intimations":
                    data['type'] = safe_get_text(".//td[1]")
                    data['reference_number'] = safe_get_text(".//td[2]")
                    data['date'] = get_frappe_date(safe_get_text(".//td[3]"))

                elif section_name == "applications":
                    data['type'] = safe_get_text(".//td[1]")

                row_index += 1

                # Find and process attachments
                attachment_links = row.find_elements(By.XPATH, ".//a[@download-doc-secure]")
                num_attachments = len(attachment_links)
                logger.warning(f"[DIAG] {section_name} row {row_idx} for {ref_id}: {num_attachments} attachment links found")

                # Use background threads for S3 uploads so they overlap with next download
                s3_futures = []
                s3_executor = ThreadPoolExecutor(max_workers=3)

                for att_idx in range(num_attachments):
                    try:
                        # Re-fetch row and attachment links to avoid stale references
                        current_rows = driver.find_elements(By.XPATH, row_xpath)
                        if row_idx >= len(current_rows):
                            logger.warning(f"Row {row_idx} stale during attachment download in {section_name}")
                            break
                        fresh_row = current_rows[row_idx]
                        fresh_links = fresh_row.find_elements(By.XPATH, ".//a[@download-doc-secure]")
                        if att_idx >= len(fresh_links):
                            logger.warning(f"Attachment {att_idx} out of range ({len(fresh_links)} links) in {section_name}")
                            break
                        attachment = fresh_links[att_idx]

                        file_name = attachment.text.strip()
                        if not file_name:
                            logger.warning(f"[DIAG] Attachment {att_idx} in {section_name} row {row_idx} for {ref_id} has EMPTY file_name, skipping")
                            continue

                        name_without_ext = os.path.splitext(file_name)[0]
                        notice_file_name = f"{name_without_ext}-{att_idx+1}-GST-ADD-{file_prefix}-{ref_id}-{client_id}.pdf"

                        files_before = set(os.listdir(download_dir))

                        driver.execute_script("arguments[0].scrollIntoView(true);", attachment)
                        time.sleep(0.2)
                        driver.execute_script("arguments[0].click();", attachment)

                        downloaded_file_path = wait_for_specific_file_download(download_dir, file_name, files_before)

                        if downloaded_file_path:
                            new_file_path = os.path.join(os.path.dirname(downloaded_file_path), notice_file_name)
                            os.rename(downloaded_file_path, new_file_path)

                            # Upload to S3 in background thread (overlaps with next download)
                            future = s3_executor.submit(upload_to_s3, new_file_path, notice_file_name)
                            s3_futures.append((future, att_idx, file_name))
                        else:
                            logger.warning(f"[DIAG] Attachment MISSED: {section_name} row {row_idx} att {att_idx} '{file_name}' for {ref_id} - download timed out")

                    except Exception as e:
                        logger.warning(f"[DIAG] Attachment ERROR: {section_name} row {row_idx} att {att_idx} for {ref_id}: {str(e)}")

                # Wait for all background S3 uploads to complete before moving to next row
                for future, att_idx, file_name in s3_futures:
                    try:
                        file_url = future.result(timeout=60)
                        data["attachments"].append(file_url)
                        logger.warning(f"[DIAG] Attachment OK: {section_name} row {row_idx} att {att_idx} '{file_name}' for {ref_id}")
                    except Exception as e:
                        logger.warning(f"[DIAG] Attachment S3 ERROR: {section_name} row {row_idx} att {att_idx} '{file_name}' for {ref_id}: {str(e)}")
                s3_executor.shutdown(wait=False)

                results.append(data)

            except Exception as e:
                logger.warning(f"Error processing row: {str(e)}")
                continue

    except Exception as e:
        logger.warning(f"[DIAG] Error processing {section_name} for {ref_id}: {str(e)}")

    total_att = sum(len(r.get('attachments', [])) for r in results)
    logger.warning(f"[DIAG] Section '{section_name}' for {ref_id}: {len(results)} rows, {total_att} attachments downloaded")
    return results


def download_attachments(driver, download_dir, case_details_info, client_id):
    """Download all attachments from case details"""
    ref_id = case_details_info.get("ref_id")
    case_details = {
        "notices": [],
        "replies": [],
        "orders": [],
        "applications": [],
        "ack_intimations": [],
        "intimations": [],
    }
    case_details.update(case_details_info)

    try:
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//div[@class='list-group']/a"))
            )
        except Exception:
            pass  # tabs may not exist for some case details
        attachment_cells = driver.find_elements(By.XPATH, "//div[@class='list-group']/a")

        logger.warning(f"[DIAG] download_attachments: found {len(attachment_cells)} tabs for {ref_id}")
        if attachment_cells and ref_id:
            num_tabs = len(attachment_cells)
            for tab_idx in range(num_tabs):
                try:
                    # Re-fetch tabs each iteration to avoid stale element references
                    current_cells = driver.find_elements(By.XPATH, "//div[@class='list-group']/a")
                    if tab_idx >= len(current_cells):
                        logger.warning(f"Tab index {tab_idx} out of range ({len(current_cells)} tabs)")
                        break
                    cell = current_cells[tab_idx]
                    cell_text = str(getattr(cell, "text", "")).strip()
                    logger.warning(f"[DIAG] Clicking tab {tab_idx}/{num_tabs}: '{cell_text}' for {ref_id}")
                    driver.execute_script("arguments[0].click();", cell)
                    time.sleep(1)  # Brief settle for Angular tab switch
                except Exception as tab_err:
                    logger.warning(f"Error clicking tab {tab_idx}: {str(tab_err)}")
                    continue

                row_xpath = "//tr[contains(@ng-repeat, 'user in $data')] | //tr[contains(@ng-repeat, 'notice in $data')] | //tr[@ng-repeat='order in $data']"

                if cell_text == "NOTICES":
                    case_details["notices"] = process_case_details_section(
                        driver, download_dir, ref_id, client_id, "notices", "", row_xpath, "NTC"
                    )
                elif cell_text == 'REPLIES':
                    case_details["replies"] = process_case_details_section(
                        driver, download_dir, ref_id, client_id, "replies", "", row_xpath, "RPY"
                    )
                elif cell_text == 'ORDERS':
                    case_details["orders"] = process_case_details_section(
                        driver, download_dir, ref_id, client_id, "orders", "", row_xpath, "ODR"
                    )
                elif cell_text == 'APPLICATIONS':
                    app_xpath = "//tr[@data-ng-show='applgrid.length > 0' and @ng-repeat='appl in applgrid']"
                    case_details["applications"] = process_case_details_section(
                        driver, download_dir, ref_id, client_id, "applications", "", app_xpath, "APP"
                    )
                elif cell_text == 'ACK./INTIMATION':
                    case_details["ack_intimations"] = process_case_details_section(
                        driver, download_dir, ref_id, client_id, "ack_intimations", "", row_xpath, "ACK"
                    )
                elif cell_text == 'INTIMATIONS':
                    int_xpath = "//tbody/tr[not(td[@colspan='7' and contains(text(), 'No Records Found')])]"
                    case_details["intimations"] = process_case_details_section(
                        driver, download_dir, ref_id, client_id, "intimations", "", int_xpath, "ITK"
                    )

        return case_details

    except Exception as e:
        logger.error(f"Error downloading attachments: {str(e)}")
        return case_details


def extract_gst_additional_notice_data(driver, download_dir, client_id):
    """DEPRECATED: GST portal merged 'Additional Notices and Orders' into the main table (Feb 2026).
    Phase2 rows are now handled by extract_notice_data() which returns them as additional_notices.
    This function is kept for backward compatibility but returns an empty list.
    """
    logger.info("extract_gst_additional_notice_data() is deprecated — phase2 notices are now extracted by extract_notice_data()")
    return []


def process_gst_notices(client_name, username, password, existing_ref_ids=None, existing_phase1_ref_ids=None):
    """Main function to process GST notices"""
    response_data = {
        "success": False,
        "msg": "Something went wrong",
        "error": "",
        "client_name": client_name,
        "username": username,
        "notices": [],
        "additional_notices": []
    }

    driver = None
    download_dir = "/tmp/gst_downloads"

    try:
        # Create download directory
        os.makedirs(download_dir, exist_ok=True)

        # Setup driver
        driver = setup_driver(download_dir)

        # Login with retry (up to 3 attempts for CAPTCHA failures)
        max_login_attempts = 3
        login_response = None
        for login_attempt in range(1, max_login_attempts + 1):
            login_response = login_user(driver, username, password, download_dir)
            if login_response["success"]:
                if login_attempt > 1:
                    logger.warning(f"[DIAG] LOGIN SUCCEEDED on attempt {login_attempt} for {client_name}")
                break

            login_error = login_response.get("error", "")
            is_captcha_error = any(phrase in login_error for phrase in [
                "Failed to solve CAPTCHA",
                "Enter valid Letters shown",
                "CAPTCHA",
            ])

            if not is_captcha_error or login_attempt == max_login_attempts:
                # Non-CAPTCHA error or final attempt — give up
                response_data["error"] = login_error or "Login failed"
                response_data["msg"] = "Login Error"
                logger.warning(f"[DIAG] LOGIN FAILED for {client_name} (user={username}) after {login_attempt} attempts: {response_data['error']}")
                return response_data

            # CAPTCHA error — retry with fresh page load
            logger.warning(f"[DIAG] CAPTCHA failed for {client_name} on attempt {login_attempt}, retrying...")
            time.sleep(2)

        # Navigate to notices (with Chrome crash recovery)
        nav_response = navigate_to_notices(driver)
        if not nav_response["success"]:
            nav_error = nav_response.get("error", "")
            # Chrome crash: empty "Message:" or very short error text
            is_chrome_crash = (
                nav_error.strip() in ["", "Message:", "Message: "]
                or ("Message:" in nav_error and len(nav_error.strip()) < 20)
            )
            
            if is_chrome_crash:
                logger.warning(f"[DIAG] Chrome crash detected for {client_name} during navigation. Retrying with fresh driver...")
                # Kill the dead driver
                try:
                    driver.quit()
                except:
                    pass
                driver = None
                
                # Retry up to 2 times with fresh driver
                for nav_retry in range(1, 3):
                    logger.warning(f"[DIAG] Navigation retry {nav_retry}/2 for {client_name}")
                    try:
                        driver = setup_driver(download_dir)
                        
                        # Re-login with retry
                        retry_login_ok = False
                        for retry_login_attempt in range(1, max_login_attempts + 1):
                            retry_login_resp = login_user(driver, username, password, download_dir)
                            if retry_login_resp["success"]:
                                retry_login_ok = True
                                break
                            retry_error = retry_login_resp.get("error", "")
                            is_captcha = any(p in retry_error for p in ["Failed to solve CAPTCHA", "Enter valid Letters shown", "CAPTCHA"])
                            if not is_captcha or retry_login_attempt == max_login_attempts:
                                break
                            logger.warning(f"[DIAG] CAPTCHA failed on nav retry login attempt {retry_login_attempt} for {client_name}")
                            time.sleep(2)
                        
                        if not retry_login_ok:
                            logger.warning(f"[DIAG] Re-login failed on nav retry {nav_retry} for {client_name}")
                            try:
                                driver.quit()
                            except:
                                pass
                            driver = None
                            continue
                        
                        # Retry navigation
                        nav_response = navigate_to_notices(driver)
                        if nav_response["success"]:
                            logger.warning(f"[DIAG] Navigation succeeded on retry {nav_retry} for {client_name}")
                            break
                        else:
                            logger.warning(f'[DIAG] Navigation still failed on retry {nav_retry} for {client_name}: {nav_response.get("error", "")}')
                            try:
                                driver.quit()
                            except:
                                pass
                            driver = None
                    except Exception as retry_ex:
                        logger.error(f"[DIAG] Nav retry {nav_retry} exception for {client_name}: {str(retry_ex)}")
                        if driver:
                            try:
                                driver.quit()
                            except:
                                pass
                            driver = None
                
                # Check if navigation finally succeeded
                if not nav_response["success"]:
                    response_data["error"] = f'Navigation failed after retries: {nav_response.get("error", "Chrome crash")}'
                    response_data["msg"] = "Navigation Error"
                    return response_data
            else:
                response_data["error"] = nav_error or "Navigation failed"
                response_data["msg"] = "Navigation Error"
                return response_data

        # Check if the page has no records (client has no notices on portal)
        if nav_response.get("no_records"):
            logger.warning(f"[DIAG] {client_name}: Login OK but no notices on portal — marking as success with 0 notices")
            response_data["success"] = True
            response_data["msg"] = "Login successful. No notices found on portal."
            response_data["notices"] = []
            response_data["additional_notices"] = []
        else:
            # Brief settle before extraction (navigation already waited for table)
            time.sleep(1)

            # Extract notice data (unified: handles both phase1 and phase2 from merged table)
            notices_data, additional_notices_data = extract_notice_data(driver, download_dir, client_name, existing_ref_ids=existing_ref_ids or [], existing_phase1_ref_ids=existing_phase1_ref_ids or [])

            response_data["success"] = True
            response_data["msg"] = "Notices fetched successfully!"
            response_data["notices"] = notices_data
            response_data["additional_notices"] = additional_notices_data

            # Final summary diagnostic
            p1_with_file = sum(1 for n in notices_data if n.get('notice_letter', {}).get('file_url'))
            p1_without_file = len(notices_data) - p1_with_file
            logger.warning(f"[DIAG] FINAL for {client_name}: Phase1 {p1_with_file} with file, {p1_without_file} without file, Phase2 {len(additional_notices_data)} notices")

    except Exception as e:
        response_data["error"] = str(e)
        response_data["traceback"] = traceback.format_exc()
        logger.error(f"Error processing GST notices: {str(e)}")
        logger.error(traceback.format_exc())

    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass

    return response_data


def lambda_handler(event, context):
    """
    AWS Lambda handler function for GST notice fetching

    Expected event structure:
    {
        "username": "GST_USERNAME",
        "password": "PASSWORD",
        "client_name": "GST-CLT-12345"
    }

    Returns:
        dict: Lambda response with statusCode and body
    """
    handler_start_time = time.time()
    try:
        username = event.get('username')
        password = event.get('password')
        client_name = event.get('client_name')
        existing_ref_ids = event.get('existing_ref_ids', [])
        existing_phase1_ref_ids = event.get('existing_phase1_ref_ids', [])

        if not username or not password or not client_name:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'Missing required parameters: username, password, or client_name'
                })
            }

        logger.info(f"Starting GST notice fetch for client: {client_name}")

        # Process notices
        result = process_gst_notices(client_name, username, password, existing_ref_ids=existing_ref_ids, existing_phase1_ref_ids=existing_phase1_ref_ids)

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
                'client_info': {'client_name': client_name, 'portal': 'gst', 'username': username},
                'function_name': 'fetch_gst_notices_lambda',
                'response': response,
                'status_code': response['statusCode'],
                'execution_time_seconds': execution_time
            }
            send_worker_webhook(webhook_config, client_name, 'gst', worker_result, execution_time)

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
                'client_info': {'client_name': event.get('client_name', 'Unknown'), 'portal': 'gst'},
                'function_name': 'fetch_gst_notices_lambda',
                'response': error_response,
                'error': str(e),
                'execution_time_seconds': execution_time
            }
            send_worker_webhook(webhook_config, event.get('client_name', 'Unknown'), 'gst', worker_result, execution_time)

        return error_response


# For local testing
if __name__ == "__main__":
    test_event = {
        "username": "27GSTIN1234567",
        "password": "test_password",
        "client_name": "GST-CLT-001"
    }

    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2, default=str))
