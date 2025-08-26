import boto3
import json
import os
import requests
import datetime
import time
from typing import Optional, Dict, Any

# --- Configuration ---
SECRET_ARN = "arn:aws:secretsmanager:us-west-2:221490242148:secret:OneSignal-oO2RWA"
BUCKET_NAME = "datalake-landingzone-221490242148-us-west-2"
S3_BASE_PREFIX = "onesignal_raw_messages_data"

# Metadata storage
S3_BOOKMARK_KEY = f"{S3_BASE_PREFIX}/_metadata/next_time_offset_cursor.txt"
S3_LAST_RUN_KEY = f"{S3_BASE_PREFIX}/_metadata/last_successful_run.json"

# For the very first run, start with historical date
HISTORICAL_START_DATE = "2024-01-01T00:00:00.000Z"

API_URL = "https://api.onesignal.com/notifications"
API_LIMIT = 50

# Initialize AWS clients
secrets_client = boto3.client("secretsmanager")
s3_client = boto3.client("s3")

def get_last_run_bookmark() -> str:
    """Reads the next_time_offset cursor from the S3 bookmark file."""
    try:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=S3_BOOKMARK_KEY)
        last_run_cursor = response['Body'].read().decode('utf-8').strip()
        print(f"Found bookmark. Resuming with cursor: {last_run_cursor}")
        return last_run_cursor
    except s3_client.exceptions.NoSuchKey:
        print(f"Bookmark not found. Starting with historical date: {HISTORICAL_START_DATE}")
        return HISTORICAL_START_DATE
    except Exception as e:
        print(f"Error reading bookmark: {str(e)}. Falling back to historical date.")
        return HISTORICAL_START_DATE

def update_last_run_bookmark(new_cursor: Optional[str]) -> None:
    """Writes the new next_time_offset cursor to the S3 bookmark file."""
    if not new_cursor:
        print("No new cursor to update. Bookmark remains unchanged.")
        return
    
    try:
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=S3_BOOKMARK_KEY,
            Body=str(new_cursor)
        )
        print(f"Updated bookmark cursor to: {new_cursor}")
    except Exception as e:
        print(f"Error updating bookmark: {str(e)}")
        raise

def save_run_metadata(total_records: int, start_cursor: str, end_cursor: Optional[str]) -> None:
    """Save metadata about the current run for monitoring and debugging."""
    run_metadata = {
        "run_timestamp": datetime.datetime.utcnow().isoformat() + "Z",
        "total_records_fetched": total_records,
        "start_cursor": start_cursor,
        "end_cursor": end_cursor,
        "status": "completed"
    }
    
    try:
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=S3_LAST_RUN_KEY,
            Body=json.dumps(run_metadata, indent=2)
        )
        print(f"Saved run metadata: {total_records} records processed")
    except Exception as e:
        print(f"Error saving run metadata: {str(e)}")

def generate_s3_path(batch_number: int) -> str:
    """Generate organized S3 path with date partitioning."""
    now = datetime.datetime.utcnow()
    
    # Create hierarchical folder structure: year/month/day
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    
    # Add batch timestamp for uniqueness
    timestamp_str = now.strftime("%Y%m%dT%H%M%S")
    filename = f"batch_{batch_number:03d}_{timestamp_str}.json"
    
    s3_path = f"{S3_BASE_PREFIX}/data/year={year}/month={month}/day={day}/{filename}"
    return s3_path

def save_batch_to_s3(notifications: list, batch_number: int) -> str:
    """Save a batch of notifications to S3 with organized folder structure."""
    s3_key = generate_s3_path(batch_number)
    
    # Add metadata to the batch
    batch_data = {
        "metadata": {
            "batch_number": batch_number,
            "record_count": len(notifications),
            "ingestion_timestamp": datetime.datetime.utcnow().isoformat() + "Z",
            "source": "OneSignal API"
        },
        "data": notifications
    }
    
    try:
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(batch_data, indent=2).encode("utf-8"),
            ContentType="application/json"
        )
        print(f"Successfully saved batch {batch_number} ({len(notifications)} records) to {s3_key}")
        return s3_key
    except Exception as e:
        print(f"Error saving batch {batch_number} to S3: {str(e)}")
        raise

def fetch_onesignal_data(api_key: str, app_id: str, start_cursor: str) -> Dict[str, Any]:
    """Fetch data from OneSignal API with proper error handling."""
    headers = {"Authorization": f"Key {api_key}"}
    
    current_time_offset = start_cursor
    total_fetched = 0
    batch_number = 1
    last_known_cursor = None
    saved_files = []
    
    print(f"Starting incremental data fetch from cursor: {start_cursor}")
    
    while current_time_offset:
        params = {
            "app_id": app_id,
            "limit": API_LIMIT,
            "time_offset": current_time_offset
        }
        
        print(f"Fetching batch {batch_number} with params: {params}")
        
        try:
            response = requests.get(API_URL, headers=headers, params=params, timeout=30)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            error_msg = f"API request failed for batch {batch_number}: {str(e)}"
            print(error_msg)
            raise Exception(error_msg)

        data = response.json()
        notifications = data.get("notifications", [])
        next_cursor = data.get("next_time_offset")

        # Update last known cursor before checking if notifications is empty
        if next_cursor:
            last_known_cursor = next_cursor
        
        if not notifications:
            print(f"No more notifications found at batch {batch_number}. Ending pagination.")
            # If this is the first batch and it's empty, use current cursor
            if total_fetched == 0:
                last_known_cursor = current_time_offset
            break

        # Save the batch to S3
        try:
            s3_path = save_batch_to_s3(notifications, batch_number)
            saved_files.append(s3_path)
        except Exception as e:
            print(f"Failed to save batch {batch_number}: {str(e)}")
            raise

        # Update counters
        batch_size = len(notifications)
        total_fetched += batch_size
        print(f"Batch {batch_number}: {batch_size} notifications (Total: {total_fetched})")
        
        # Prepare for next iteration
        current_time_offset = next_cursor
        batch_number += 1
        
        # Rate limiting - be nice to the API
        time.sleep(1)
    
    return {
        "total_fetched": total_fetched,
        "last_cursor": last_known_cursor,
        "saved_files": saved_files,
        "batches_processed": batch_number - 1
    }

def lambda_handler(event, context):
    """Main Lambda handler function."""
    try:
        # Get API credentials
        secret_response = secrets_client.get_secret_value(SecretId=SECRET_ARN)
        secret_data = json.loads(secret_response["SecretString"])
        api_key = secret_data["api_key"]
        app_id = secret_data["app_id"]
        
        # Get starting cursor for incremental processing
        start_cursor = get_last_run_bookmark()
        
        # Fetch data from OneSignal API
        result = fetch_onesignal_data(api_key, app_id, start_cursor)
        
        # Update bookmark for next run (incremental processing)
        if result["last_cursor"]:
            update_last_run_bookmark(result["last_cursor"])
        
        # Save run metadata for monitoring
        save_run_metadata(
            total_records=result["total_fetched"],
            start_cursor=start_cursor,
            end_cursor=result["last_cursor"]
        )
        
        # Return success response
        response_body = {
            "status": "success",
            "message": f"Successfully processed {result['total_fetched']} notifications in {result['batches_processed']} batches",
            "total_records": result["total_fetched"],
            "batches_processed": result["batches_processed"],
            "files_saved": len(result["saved_files"]),
            "next_cursor": result["last_cursor"]
        }
        
        print(f"Lambda execution completed successfully: {json.dumps(response_body)}")
        
        return {
            "statusCode": 200,
            "body": json.dumps(response_body)
        }
        
    except Exception as e:
        error_msg = f"Lambda execution failed: {str(e)}"
        print(error_msg)
        
        # Save error metadata
        error_metadata = {
            "run_timestamp": datetime.datetime.utcnow().isoformat() + "Z",
            "status": "failed",
            "error": str(e),
            "start_cursor": get_last_run_bookmark()
        }
        
        try:
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=S3_LAST_RUN_KEY,
                Body=json.dumps(error_metadata, indent=2)
            )
        except:
            pass  # Don't fail on metadata save error
        
        return {
            "statusCode": 500,
            "body": json.dumps({"error": error_msg})
        }
