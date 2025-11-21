import json
import boto3
import requests
import time
import base64
import random
from datetime import datetime
from collections import defaultdict
from botocore.exceptions import ClientError

# ==========================================
# CONFIGURATION
# ==========================================
REGION_NAME = "us-west-2"
BUCKET_NAME = "Your bucket name"
SECRET_ARN = "ARN for secret manager"

# S3 Paths
BASE_PREFIX = "klaviyo-events-data"
STATE_FILE_KEY = f"{BASE_PREFIX}/last-run/state.json"
EVENTS_PREFIX = f"{BASE_PREFIX}/events"
METRICS_PREFIX = f"{BASE_PREFIX}/metrics"

# Klaviyo Config
KLAVIYO_BASE_URL = "https://a.klaviyo.com/api"
KLAVIYO_REVISION = "2024-07-15"
# BACKFILL START: If no state file is found, we start from here.
INITIAL_HISTORY_DATE = "2023-01-01T00:00:00Z" 

# Clients
S3_CLIENT = boto3.client('s3', region_name=REGION_NAME)
SECRETS_CLIENT = boto3.client('secretsmanager', region_name=REGION_NAME)

# ==========================================
# 1. SECRET MANAGEMENT
# ==========================================
def get_klaviyo_api_key():
    """Retrieves API Key from Secrets Manager."""
    try:
        response = SECRETS_CLIENT.get_secret_value(SecretId=SECRET_ARN)
        if 'SecretString' in response:
            secret_data = response['SecretString']
            try:
                secret_dict = json.loads(secret_data)
                return secret_dict.get('Klaviyo RO API key')
            except json.JSONDecodeError:
                return secret_data
        else:
            return base64.b64decode(response['SecretBinary'])
    except ClientError as e:
        print(f"❌ Error retrieving secret: {e}")
        raise e

# ==========================================
# 2. STATE MANAGEMENT
# ==========================================
def get_last_checkpoint():
    """
    Reads the last processed timestamp from S3. 
    Returns INITIAL_HISTORY_DATE if no file exists (Triggering Backfill).
    """
    try:
        response = S3_CLIENT.get_object(Bucket=BUCKET_NAME, Key=STATE_FILE_KEY)
        state = json.loads(response['Body'].read().decode('utf-8'))
        print(f"🔄 Found Checkpoint: {state.get('last_timestamp')}")
        return state.get('last_timestamp')
    except S3_CLIENT.exceptions.NoSuchKey:
        print(f"🆕 No Checkpoint found. Starting Backfill from {INITIAL_HISTORY_DATE}")
        return INITIAL_HISTORY_DATE

def update_checkpoint(latest_timestamp):
    """Updates the state file in S3."""
    state = {
        "last_timestamp": latest_timestamp, 
        "updated_at": datetime.utcnow().isoformat()
    }
    S3_CLIENT.put_object(
        Bucket=BUCKET_NAME,
        Key=STATE_FILE_KEY,
        Body=json.dumps(state),
        ContentType='application/json'
    )
    print(f"💾 State Updated: {latest_timestamp}")

# ==========================================
# 3. METRICS LOGIC (Snapshot)
# ==========================================
def process_metrics_snapshot(headers):
    """
    Fetches ALL current metrics and saves them to a 'Today' partition.
    This satisfies the requirement: 'Get metrics first'.
    """
    print("📊 STEP 1: Fetching Metrics Snapshot...")
    
    url = f"{KLAVIYO_BASE_URL}/metrics/"
    all_metrics = []
    
    # Loop to get all metrics (usually 1 or 2 pages max)
    while url:
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            items = data.get('data', [])
            all_metrics.extend(items)
            
            url = data.get('links', {}).get('next')
        except Exception as e:
            print(f"❌ Error fetching metrics: {e}")
            raise e

    if not all_metrics:
        print("   No metrics found.")
        return

    # Save to S3 partitioned by CURRENT PROCESSING DATE
    # Even if we are backfilling events from 2023, we save metrics to 'Today'
    # because these are the definitions valid right now.
    now = datetime.now()
    
    s3_key = (
        f"{METRICS_PREFIX}/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"metrics_snapshot_{now.strftime('%H%M%S')}.json"
    )

    # Save as NDJSON
    ndjson_data = '\n'.join([json.dumps(record) for record in all_metrics])

    S3_CLIENT.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=ndjson_data,
        ContentType='application/json'
    )
    print(f"✅ Saved {len(all_metrics)} metrics to {s3_key}")

# ==========================================
# 4. EVENTS LOGIC (Incremental)
# ==========================================
def upload_events_batch(events):
    """
    Splits events by their ACTUAL OCCURRENCE DATE and writes to partitions.
    """
    if not events:
        return

    events_by_date = defaultdict(list)
    for event in events:
        # event['attributes']['datetime'] example: 2025-11-21T14:30:00Z
        # We extract the date part: 2025-11-21
        evt_date = event['attributes']['datetime'][:10] 
        events_by_date[evt_date].append(event)

    timestamp_id = datetime.now().strftime('%H%M%S')
    
    for date_str, batch in events_by_date.items():
        y, m, d = date_str.split('-')
        
        s3_key = (
            f"{EVENTS_PREFIX}/"
            f"year={y}/month={m}/day={d}/"
            f"klaviyo_events_{timestamp_id}.json"
        )
        
        ndjson_data = '\n'.join([json.dumps(record) for record in batch])
        
        S3_CLIENT.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=ndjson_data,
            ContentType='application/json'
        )
        print(f"   -> Wrote {len(batch)} events for date {date_str}")

# ==========================================
# 5. Exponential Backoff for Rate Limits
# ==========================================
def get_with_backoff(url, headers, params):
    """Handles rate-limited requests with exponential backoff."""
    retries = 0
    while retries < 5:  # retry up to 5 times
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 429:  # Rate limit hit
            sleep_time = min(2**retries + random.uniform(0, 1), 60)  # Exponential backoff with jitter
            print(f"⚠️ Rate limit hit, sleeping for {sleep_time:.2f} seconds...")
            time.sleep(sleep_time)
            retries += 1
        else:
            return response
    raise Exception("Max retries reached")

def process_incremental_events(headers, start_timestamp, context):
    print(f"🚀 STEP 2: Starting Events Fetch (Newer than {start_timestamp})")
    
    url = f"{KLAVIYO_BASE_URL}/events/"
    
    # 'sort=datetime' ensures we get Oldest events first.
    # This is critical for backfilling 2 years of data safely.
    params = {
        "filter": f"greater-than(datetime,{start_timestamp})",
        "sort": "datetime", 
        "page[size]": 100
    }

    max_timestamp_seen = start_timestamp
    total_processed = 0
    
    while url:
        try:
            response = get_with_backoff(url, headers, params)  # Use exponential backoff here

            response.raise_for_status()
            data = response.json()
            events = data.get('data', [])
            
            if events:
                # 1. Find the latest timestamp in this batch to update state later
                batch_latest_ts = events[-1]['attributes']['datetime']
                if batch_latest_ts > max_timestamp_seen:
                    max_timestamp_seen = batch_latest_ts

                # 2. Write to S3 (Partitioned by Event Date)
                upload_events_batch(events)
                total_processed += len(events)

            url = data.get('links', {}).get('next')
            
            # TIMEOUT PROTECTION
            # If backfill is massive, Lambda might timeout. 
            # We stop if < 1 min remains, save state, and continue on next run.
            if context.get_remaining_time_in_millis() < 60000:
                print("⚠️ Time limit approaching. Stopping cleanly to save state.")
                break

        except Exception as e:
            print(f"❌ Error in events loop: {str(e)}")
            raise e

    return total_processed, max_timestamp_seen

# ==========================================
# MAIN HANDLER
# ==========================================
def lambda_handler(event, context):
    # 1. Credentials
    api_key = get_klaviyo_api_key()
    if not api_key: raise Exception("API Key missing")

    headers = {
        "accept": "application/json",
        "revision": KLAVIYO_REVISION,
        "Authorization": f"Klaviyo-API-Key {api_key}"
    }

    # 2. METRICS FIRST (As requested)
    # We fetch the full list of metrics available TODAY.
    process_metrics_snapshot(headers)

    # 3. EVENTS SECOND
    # We fetch events starting from where we left off.
    start_timestamp = get_last_checkpoint()
    count, new_watermark = process_incremental_events(headers, start_timestamp, context)

    # 4. UPDATE STATE
    if count > 0:
        update_checkpoint(new_watermark)
        return {
            "statusCode": 200,
            "body": f"Success. Metrics snapshot saved. Processed {count} events. New Watermark: {new_watermark}"
        }
    
    return {
        "statusCode": 200,
        "body": "Success. Metrics snapshot saved. No new events found."
    }
