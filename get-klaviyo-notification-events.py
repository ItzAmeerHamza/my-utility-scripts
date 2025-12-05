import sys
import json
import boto3
import requests
import time
import base64
import random
from datetime import datetime, timedelta

# ==========================================
# CONFIGURATION
# ==========================================
REGION_NAME = "us-west-2"
BUCKET_NAME = "YOUR BUCKET NAME"
SECRET_ARN = "SECRET MANAGER ARN"

# S3 Paths
BASE_PREFIX = "klaviyo-events-data/testing/"
EVENTS_PREFIX = f"{BASE_PREFIX}/events"
METRICS_PREFIX = f"{BASE_PREFIX}/metrics"

# Klaviyo Config
KLAVIYO_BASE_URL = "https://a.klaviyo.com/api"
KLAVIYO_REVISION = "2024-07-15"

# ==========================================
# 📅 HARDCODED DATE (EDIT HERE)
# ==========================================
# This determines the Start Date. 
# The script will automatically calculate the End Date as (Target + 1 Day)
TARGET_DATE = '2025-12-01'

# Clients
S3_CLIENT = boto3.client('s3', region_name=REGION_NAME)
SECRETS_CLIENT = boto3.client('secretsmanager', region_name=REGION_NAME)

# ==========================================
# 1. SECRET MANAGEMENT
# ==========================================
def get_klaviyo_api_key():
    try:
        response = SECRETS_CLIENT.get_secret_value(SecretId=SECRET_ARN)
        if 'SecretString' in response:
            secret_dict = json.loads(response['SecretString'])
            return secret_dict.get('Klaviyo RO API key')
        return base64.b64decode(response['SecretBinary'])
    except Exception as e:
        print(f"❌ Error retrieving secret: {e}")
        raise e

# ==========================================
# 2. METRICS LOGIC
# ==========================================
def process_metrics_snapshot(headers):
    """
    Fetches ALL current metrics.
    """
    print("📊 Fetching Metrics Snapshot...")
    url = f"{KLAVIYO_BASE_URL}/metrics/"
    all_metrics = []
    
    while url:
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            all_metrics.extend(data.get('data', []))
            url = data.get('links', {}).get('next')
        except Exception as e:
            print(f"❌ Error fetching metrics: {e}")
            raise e

    if not all_metrics:
        return

    # Save to S3
    now = datetime.now()
    s3_key = f"{METRICS_PREFIX}/year={now.year}/month={now.month:02d}/day={now.day:02d}/metrics_{now.strftime('%H%M%S')}.json"

    ndjson_data = '\n'.join([json.dumps(record) for record in all_metrics])
    S3_CLIENT.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=ndjson_data, ContentType='application/json')
    print(f"✅ Saved {len(all_metrics)} metrics.")

# ==========================================
# 3. EVENTS LOGIC
# ==========================================
def upload_to_s3(events, target_date_str):
    """
    Writes a batch of events to S3.
    """
    if not events: return

    # Parse date for folder structure
    dt = datetime.strptime(target_date_str, "%Y-%m-%d")
    timestamp_id = datetime.now().strftime('%H%M%S')
    
    # Path: klaviyo-events-data/events/year=YYYY/month=MM/day=DD/...
    s3_key = (
        f"{EVENTS_PREFIX}/"
        f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
        f"events_batch_{timestamp_id}_{random.randint(100,999)}.json"
    )
    
    ndjson_data = '\n'.join([json.dumps(record) for record in events])
    
    S3_CLIENT.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=ndjson_data, ContentType='application/json')
    print(f"   -> Saved batch of {len(events)} events to {s3_key}")

def get_with_backoff(url, headers, params):
    retries = 0
    while retries < 5:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 429:
            sleep_time = min(2**retries + random.uniform(0, 1), 60)
            print(f"⚠️ Rate limit, sleeping {sleep_time:.2f}s...")
            time.sleep(sleep_time)
            retries += 1
        else:
            return response
    raise Exception("Max retries reached")

def process_single_date(headers, target_date):
    """
    Fetches exactly 24 hours of data for the target_date.
    """
    
    # 1. Define Time Window: [TargetDate 00:00:00, TargetDate+1 00:00:00)
    start_dt = datetime.strptime(target_date, "%Y-%m-%d")
    end_dt = start_dt + timedelta(days=1)
    
    # Formatting exactly as requested
    start_iso = start_dt.strftime("%Y-%m-%dT00:00:00Z")
    end_iso = end_dt.strftime("%Y-%m-%dT00:00:00Z")
    
    print(f"🚀 Processing Date: {target_date}")
    print(f"⏰ Window: {start_iso} to {end_iso}")

    url = f"{KLAVIYO_BASE_URL}/events/"
    
    # 2. Filter: >= Start AND < End
    filter_string = f"greater-or-equal(datetime,{start_iso}),less-than(datetime,{end_iso})"
    
    params = {
        "filter": filter_string,
        "sort": "datetime", 
        "page[size]": 100
    }

    total_processed = 0
    
    while url:
        try:
            # 3. Request Data
            response = get_with_backoff(url, headers, params)
            
            params = None # Clear params after first request

            response.raise_for_status()
            data = response.json()
            events = data.get('data', [])
            
            if events:
                upload_to_s3(events, target_date)
                total_processed += len(events)

            url = data.get('links', {}).get('next')

        except Exception as e:
            print(f"❌ Error processing {target_date}: {str(e)}")
            raise e

    return total_processed
