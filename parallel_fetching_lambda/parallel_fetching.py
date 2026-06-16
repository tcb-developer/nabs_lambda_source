"""
Parallel Notice Fetching Lambda Orchestrator

This Lambda function orchestrates parallel execution of notice fetching for multiple clients.
It invokes GST and Income Tax notice fetching Lambda functions concurrently.
Supports configurable batch sizes, delays between client starts, and fallback tracking.
"""

import json
import logging
import boto3
import requests
from botocore.client import Config
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback
from datetime import datetime
import time

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# AWS Lambda client
lambda_client = boto3.client(
    'lambda',
    region_name='ap-south-1',
    config=Config(read_timeout=900, connect_timeout=900)
)

# Lambda function names (update these with your actual Lambda function names)
GST_LAMBDA_FUNCTION_NAME = "fetch_gst_notices_lambda"  # Update with actual name
INCOME_TAX_LAMBDA_FUNCTION_NAME = "fetch_income_tax_notices_lambda"  # Update with actual name

# Webhook configuration (for sending results back to Frappe)
FRAPPE_WEBHOOK_URL = None  # Will be passed in event
FRAPPE_API_KEY = None  # Will be passed in event
FRAPPE_API_SECRET = None  # Will be passed in event


def send_webhook_callback(webhook_url, api_key, api_secret, log_name, results, orchestrator_callback_url=None):
    """
    Send results back to Frappe via webhook with retry logic.

    Uses 5-minute timeout per attempt and retries up to 2 times.
    This stays well within Lambda's 15-minute limit while giving
    Frappe enough time to process large result sets.

    Args:
        webhook_url: Frappe site URL
        api_key: Frappe API key
        api_secret: Frappe API secret
        log_name: Parallel Fetch Log document name
        results: Results dictionary
    """
    if not webhook_url:
        logger.warning("No webhook URL provided, skipping callback")
        return False

    callback_url = orchestrator_callback_url or f"{webhook_url}/api/method/fin_buddy.features.lambda_webhooks.update_parallel_fetch_log"

    headers = {
        "Content-Type": "application/json"
    }
    if api_key and api_secret:
        headers["Authorization"] = f"token {api_key}:{api_secret}"

    payload = {
        "log_name": log_name,
        "results": json.dumps(results, default=str)
    }

    max_attempts = 2
    timeout_seconds = 300  # 5 minutes per attempt

    for attempt in range(1, max_attempts + 1):
        try:
            logger.info(f"Sending webhook callback to {callback_url} (attempt {attempt}/{max_attempts}, timeout={timeout_seconds}s)")

            response = requests.post(
                callback_url,
                headers=headers,
                json=payload,
                timeout=timeout_seconds
            )

            if response.status_code == 200:
                logger.info("Webhook callback sent successfully")
                return True
            else:
                logger.error(f"Webhook callback failed: {response.status_code} - {response.text}")
                if attempt < max_attempts:
                    logger.info(f"Retrying in 10 seconds...")
                    time.sleep(10)

        except Exception as e:
            logger.error(f"Webhook attempt {attempt} error: {str(e)}")
            if attempt < max_attempts:
                logger.info(f"Retrying in 10 seconds...")
                time.sleep(10)
            else:
                logger.error(traceback.format_exc())

    logger.error(f"Webhook callback failed after {max_attempts} attempts")
    return False


def invoke_lambda_async(function_name, payload, client_info):
    """
    Invoke a Lambda function using fire-and-forget (Event) invocation.
    
    Each worker Lambda sends its results directly back to Frappe via webhook.
    The orchestrator does NOT wait for results — it just fires all workers
    and exits. This eliminates the 15-minute orchestrator timeout bottleneck.

    Args:
        function_name: Name of the Lambda function to invoke
        payload: Event payload for the Lambda function (must include webhook_config)
        client_info: Dict containing client information for tracking

    Returns:
        dict: Invocation status (not the actual processing result)
    """
    try:
        logger.info(f"Invoking {function_name} for client {client_info.get('client_name')} (fire-and-forget)")

        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='Event',  # Fire-and-forget — worker sends results via webhook
            Payload=json.dumps(payload)
        )

        status_code = response.get('StatusCode', 0)
        success = status_code == 202  # 202 = Event invocation accepted

        if success:
            logger.info(f"Successfully triggered {function_name} for {client_info.get('client_name')}")
        else:
            logger.error(f"Failed to trigger {function_name} for {client_info.get('client_name')}: status={status_code}")

        return {
            'success': success,
            'client_info': client_info,
            'function_name': function_name,
            'status_code': status_code,
            'execution_time_seconds': 0  # Not tracking — worker reports its own time
        }

    except Exception as e:
        logger.error(f"Error invoking {function_name} for client {client_info.get('client_name')}: {str(e)}")
        logger.error(traceback.format_exc())

        return {
            'success': False,
            'client_info': client_info,
            'function_name': function_name,
            'error': str(e),
            'traceback': traceback.format_exc(),
            'execution_time_seconds': 0
        }


def process_income_tax_client(client):
    """
    Process a single Income Tax client

    Args:
        client: Dict with keys - client_name, username, password, downloaded_files (optional), webhook_config

    Returns:
        dict: Result of the Lambda invocation
    """
    payload = {
        'username': client.get('username'),
        'password': client.get('password'),
        'client_name': client.get('client_name'),
        'downloaded_files': client.get('downloaded_files', []),
        'webhook_config': client.get('webhook_config')
    }

    client_info = {
        'client_name': client.get('client_name'),
        'portal': 'income_tax',
        'username': client.get('username')
    }

    return invoke_lambda_async(INCOME_TAX_LAMBDA_FUNCTION_NAME, payload, client_info)


def process_income_tax_client_with_delay(client, delay_seconds):
    """
    Process a single Income Tax client with initial delay

    Args:
        client: Dict with keys - client_name, username, password, downloaded_files (optional)
        delay_seconds: Seconds to wait before starting the actual processing

    Returns:
        dict: Result of the Lambda invocation
    """
    # Wait for the specified delay before starting
    if delay_seconds > 0:
        logger.info(f"Income Tax client {client.get('client_name')}: waiting {delay_seconds}s before starting")
        time.sleep(delay_seconds)

    logger.info(f"Income Tax client {client.get('client_name')}: starting Lambda invocation now")
    return process_income_tax_client(client)


def process_gst_client(client):
    """
    Process a single GST client

    Args:
        client: Dict with keys - client_name, username, password, webhook_config

    Returns:
        dict: Result of the Lambda invocation
    """
    payload = {
        'username': client.get('username'),
        'password': client.get('password'),
        'client_name': client.get('client_name'),
        'webhook_config': client.get('webhook_config')
    }

    client_info = {
        'client_name': client.get('client_name'),
        'portal': 'gst',
        'username': client.get('username')
    }

    return invoke_lambda_async(GST_LAMBDA_FUNCTION_NAME, payload, client_info)


def process_gst_client_with_delay(client, delay_seconds):
    """
    Process a single GST client with initial delay

    Args:
        client: Dict with keys - client_name, username, password
        delay_seconds: Seconds to wait before starting the actual processing

    Returns:
        dict: Result of the Lambda invocation
    """
    # Wait for the specified delay before starting
    if delay_seconds > 0:
        logger.info(f"GST client {client.get('client_name')}: waiting {delay_seconds}s before starting")
        time.sleep(delay_seconds)

    logger.info(f"GST client {client.get('client_name')}: starting Lambda invocation now")
    return process_gst_client(client)


def _process_batch(batch_clients, batch_number, total_batches, results, enable_fallback, inter_client_delay):
    """
    Process a single batch of clients sequentially within the batch,
    waiting for each Lambda invocation to complete before starting the next.

    Args:
        batch_clients: List of (portal_type, client, process_fn) tuples
        batch_number: Current batch number (1-based)
        total_batches: Total number of batches
        results: Shared results dict to append to
        enable_fallback: Whether to track failed clients
        inter_client_delay: Seconds to wait between clients within the batch
    """
    for idx, (portal_type, client, process_fn) in enumerate(batch_clients):
        client_name = client.get('client_name')
        logger.info(f"Batch {batch_number}/{total_batches} - Processing client {idx + 1}/{len(batch_clients)}: {client_name} ({portal_type})")

        # Add delay between clients within a batch (not before the first one)
        if idx > 0 and inter_client_delay > 0:
            time.sleep(inter_client_delay)

        try:
            result = process_fn(client)

            if portal_type == 'income_tax':
                results['income_tax_results'].append(result)
            else:
                results['gst_results'].append(result)

            if result.get('success'):
                results['summary']['successful'] += 1
                logger.info(f"SUCCESS: {client_name} ({portal_type})")
            else:
                results['summary']['failed'] += 1
                logger.warning(f"FAILED: {client_name} ({portal_type}) - {result.get('error', 'Unknown error')}")

                if enable_fallback:
                    results['failed_clients'].append({
                        'client_name': client_name,
                        'portal_type': portal_type,
                        'error': result.get('error', 'Unknown error')
                    })

        except Exception as e:
            logger.error(f"Error processing {client_name} ({portal_type}): {str(e)}")
            results['summary']['failed'] += 1

            error_result = {
                'success': False,
                'client_info': {'client_name': client_name, 'portal': portal_type},
                'portal': portal_type,
                'error': f"Execution error: {str(e)}",
                'traceback': traceback.format_exc(),
                'execution_time_seconds': 0
            }

            if portal_type == 'income_tax':
                results['income_tax_results'].append(error_result)
            else:
                results['gst_results'].append(error_result)

            if enable_fallback:
                results['failed_clients'].append({
                    'client_name': client_name,
                    'portal_type': portal_type,
                    'error': str(e)
                })


def lambda_handler(event, context):
    """
    AWS Lambda handler function for parallel notice fetching.

    Uses controlled batch processing: processes clients in small batches,
    running each batch in parallel but waiting for the batch to complete
    before starting the next one. This prevents AWS throttling and
    avoids overwhelming government portals.

    Expected event structure:
    {
        "income_tax_clients": [
            {
                "client_name": "IN-TAX-CLT-12345",
                "username": "PANCARD123",
                "password": "password123",
                "downloaded_files": []  # Optional
            }
        ],
        "gst_clients": [
            {
                "client_name": "GST-CLT-67890",
                "username": "GSTIN12345",
                "password": "password456"
            }
        ],
        "max_workers": 5,  # Optional, concurrent Lambdas per batch (default 5)
        "batch_size": 5,  # Optional, clients per batch (default 5, should be <= concurrency limit)
        "income_tax_delay_seconds": 2,  # Optional, delay between clients within a batch
        "gst_delay_seconds": 3,  # Optional, delay between clients within a batch
        "inter_batch_delay_seconds": 5,  # Optional, delay between batches
        "enable_fallback": true,  # Optional, whether to track failed clients for fallback
        "webhook_config": {  # Optional, for callback
            "url": "https://your-frappe-site.com",
            "api_key": "your_api_key",
            "api_secret": "your_api_secret",
            "log_name": "PFL-00001"
        }
    }

    Returns:
        dict: Aggregated results from all client processing
    """
    start_time = datetime.now()

    try:
        # Extract client lists from event
        income_tax_clients = event.get('income_tax_clients', [])
        gst_clients = event.get('gst_clients', [])

        # Batch size controls how many clients are processed at once
        # Should be <= Lambda concurrency limit to avoid throttling
        batch_size = int(event.get('batch_size', 5))
        max_workers = int(event.get('max_workers', batch_size))

        # Delay between clients within a batch (to avoid portal rate limiting)
        income_tax_delay = float(event.get('income_tax_delay_seconds', 2))
        gst_delay = float(event.get('gst_delay_seconds', 3))

        # Delay between batches (to let previous batch fully complete)
        inter_batch_delay = float(event.get('inter_batch_delay_seconds', 5))

        # Extract fallback setting
        enable_fallback = event.get('enable_fallback', True)

        # Extract webhook config for callback
        webhook_config = event.get('webhook_config', {})

        logger.info(f"Starting batch-controlled fetching for {len(income_tax_clients)} Income Tax clients and {len(gst_clients)} GST clients")
        logger.info(f"Configuration: batch_size={batch_size}, max_workers={max_workers}, IT_delay={income_tax_delay}s, GST_delay={gst_delay}s, inter_batch_delay={inter_batch_delay}s, fallback={enable_fallback}")

        if not income_tax_clients and not gst_clients:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': 'No clients provided. Please provide income_tax_clients or gst_clients array.'
                })
            }

        # Results storage
        results = {
            'income_tax_results': [],
            'gst_results': [],
            'failed_clients': [],
            'summary': {
                'total_clients': len(income_tax_clients) + len(gst_clients),
                'income_tax_clients_count': len(income_tax_clients),
                'gst_clients_count': len(gst_clients),
                'successful': 0,
                'failed': 0,
                'execution_time_seconds': 0,
                'batch_size': batch_size,
                'income_tax_delay_seconds': income_tax_delay,
                'gst_delay_seconds': gst_delay
            }
        }

        # Inject webhook_config into each client so workers can callback directly
        for client in income_tax_clients:
            client['webhook_config'] = webhook_config
        for client in gst_clients:
            client['webhook_config'] = webhook_config

        # Build a unified list of all clients with their processing functions and delays
        all_clients = []
        for client in income_tax_clients:
            all_clients.append(('income_tax', client, process_income_tax_client, income_tax_delay))
        for client in gst_clients:
            all_clients.append(('gst', client, process_gst_client, gst_delay))

        # Split into batches
        batches = []
        for i in range(0, len(all_clients), batch_size):
            batches.append(all_clients[i:i + batch_size])

        total_batches = len(batches)
        logger.info(f"Split {len(all_clients)} clients into {total_batches} batch(es) of up to {batch_size}")

        # Process each batch: run clients within a batch in parallel,
        # but wait for the entire batch to complete before starting the next
        for batch_idx, batch in enumerate(batches):
            batch_number = batch_idx + 1
            logger.info(f"Starting batch {batch_number}/{total_batches} with {len(batch)} clients")

            # Wait between batches (not before the first one)
            if batch_idx > 0 and inter_batch_delay > 0:
                logger.info(f"Waiting {inter_batch_delay}s before starting batch {batch_number}")
                time.sleep(inter_batch_delay)

            # Run clients in this batch in parallel using ThreadPoolExecutor
            # Each thread invokes one Lambda synchronously
            futures = []
            future_to_client = {}

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                for client_idx, (portal_type, client, process_fn, delay) in enumerate(batch):
                    client_name = client.get('client_name')

                    # Stagger submissions within the batch
                    client_delay = client_idx * delay

                    if portal_type == 'income_tax':
                        future = executor.submit(
                            process_income_tax_client_with_delay,
                            client,
                            client_delay
                        )
                    else:
                        future = executor.submit(
                            process_gst_client_with_delay,
                            client,
                            client_delay
                        )

                    futures.append(future)
                    future_to_client[future] = {
                        'portal_type': portal_type,
                        'client_name': client_name
                    }
                    logger.info(f"Batch {batch_number}: submitted {client_name} ({portal_type}) with {client_delay}s delay")

                # Wait for ALL clients in this batch to complete before moving on
                for future in as_completed(futures):
                    client_meta = future_to_client[future]
                    portal_type = client_meta['portal_type']
                    client_name = client_meta['client_name']

                    try:
                        result = future.result(timeout=900)

                        if portal_type == 'income_tax':
                            results['income_tax_results'].append(result)
                        else:
                            results['gst_results'].append(result)

                        if result.get('success'):
                            results['summary']['successful'] += 1
                            logger.info(f"SUCCESS: {client_name} ({portal_type})")
                        else:
                            results['summary']['failed'] += 1
                            logger.warning(f"FAILED: {client_name} ({portal_type}) - {result.get('error', 'Unknown error')}")

                            if enable_fallback:
                                results['failed_clients'].append({
                                    'client_name': client_name,
                                    'portal_type': portal_type,
                                    'error': result.get('error', 'Unknown error')
                                })

                    except Exception as e:
                        logger.error(f"Error collecting result for {client_name} ({portal_type}): {str(e)}")
                        results['summary']['failed'] += 1

                        error_result = {
                            'success': False,
                            'client_info': {'client_name': client_name, 'portal': portal_type},
                            'portal': portal_type,
                            'error': f"Future execution error: {str(e)}",
                            'traceback': traceback.format_exc(),
                            'execution_time_seconds': 0
                        }

                        if portal_type == 'income_tax':
                            results['income_tax_results'].append(error_result)
                        else:
                            results['gst_results'].append(error_result)

                        if enable_fallback:
                            results['failed_clients'].append({
                                'client_name': client_name,
                                'portal_type': portal_type,
                                'error': str(e)
                            })

            logger.info(f"Batch {batch_number}/{total_batches} completed. Running totals: {results['summary']['successful']} success, {results['summary']['failed']} failed")

        # Calculate execution time
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        results['summary']['execution_time_seconds'] = execution_time
        results['summary']['start_time'] = start_time.isoformat()
        results['summary']['end_time'] = end_time.isoformat()
        results['summary']['failed_client_count'] = len(results['failed_clients'])
        results['summary']['total_batches'] = total_batches

        logger.info(f"All batches completed in {execution_time} seconds")
        logger.info(f"Final summary: {results['summary']['successful']} successful, {results['summary']['failed']} failed out of {results['summary']['total_clients']} total")

        if results['failed_clients']:
            logger.info(f"Failed clients for fallback: {[c['client_name'] for c in results['failed_clients']]}")

        # NOTE: In fire-and-forget mode, each worker Lambda sends its own results
        # directly to Frappe. The orchestrator only reports invocation stats.
        logger.info(f"All workers triggered. Each worker will send results directly to Frappe via webhook.")

        return {
            'statusCode': 200,
            'body': json.dumps(results, default=str)
        }

    except Exception as e:
        logger.error(f"Lambda handler error: {str(e)}")
        logger.error(traceback.format_exc())

        # Send error callback to Frappe
        if 'webhook_config' in event and event.get('webhook_config'):
            error_results = {
                'summary': {
                    'successful': 0,
                    'failed': len(event.get('income_tax_clients', [])) + len(event.get('gst_clients', [])),
                    'execution_time_seconds': 0
                },
                'failed_clients': [],
                'error': str(e),
                'traceback': traceback.format_exc()
            }

            for client in event.get('income_tax_clients', []):
                error_results['failed_clients'].append({
                    'client_name': client.get('client_name'),
                    'portal_type': 'income_tax',
                    'error': str(e)
                })
            for client in event.get('gst_clients', []):
                error_results['failed_clients'].append({
                    'client_name': client.get('client_name'),
                    'portal_type': 'gst',
                    'error': str(e)
                })

            send_webhook_callback(
                webhook_url=event['webhook_config'].get('url'),
                api_key=event['webhook_config'].get('api_key'),
                api_secret=event['webhook_config'].get('api_secret'),
                log_name=event['webhook_config'].get('log_name'),
                results=error_results,
                orchestrator_callback_url=event['webhook_config'].get('orchestrator_callback_url')
            )

        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'traceback': traceback.format_exc()
            })
        }


# For local testing
if __name__ == "__main__":
    # Example usage
    test_event = {
        "income_tax_clients": [
            {
                "client_name": "IN-TAX-CLT-07460",
                "username": "AADCH3199K",
                "password": "aarya@2004",
                "downloaded_files": []
            },
            {
                "client_name": "IN-TAX-CLT-07461",
                "username": "PANCARD456",
                "password": "password456",
                "downloaded_files": []
            }
        ],
        "gst_clients": [
            {
                "client_name": "GST-CLT-001",
                "username": "27GSTIN1234567",
                "password": "gstpass123"
            },
            {
                "client_name": "GST-CLT-002",
                "username": "29GSTIN7654321",
                "password": "gstpass456"
            }
        ],
        "max_workers": 5
    }

    result = lambda_handler(test_event, None)
    print(json.dumps(result, indent=2, default=str))
