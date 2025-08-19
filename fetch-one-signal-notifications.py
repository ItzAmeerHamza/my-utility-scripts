import boto3
import json
import os
import requests
import datetime
import time

# --- Configuration ---
# Use the Secret ARN from your previous function
SECRET_ARN = "arn:aws:secretsmanager:us-west-2:221490242148:secret:OneSignal-oO2RWA"

# S3 bucket name and prefix
BUCKET_NAME = "datalake-landingzone-221490242148-us-west-2"
S3_PREFIX = "onesignal_raw_messages_data/"


# Location for the bookmark file to store the state
S3_BOOKMARK_KEY = os.path.join(S3_PREFIX, "_metadata/last_run_timestamp.txt")

# OneSignal API Configuration
API_URL = "https://api.onesignal.com/notifications"
API_LIMIT = 50 # Max limit per OneSignal API documentation

# Initialize AWS clients
secrets_client = boto3.client("secretsmanager")
s3_client = boto3.client("s3")

def get_last_run_timestamp():
    """Reads the last run timestamp from the S3 bookmark file."""
    try:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=S3_BOOKMARK_KEY)
        last_run_ts = response['Body'].read().decode('utf-8')
        print(f"Found bookmark. Last run timestamp: {last_run_ts}")
        return int(last_run_ts)
    except s3_client.exceptions.NoSuchKey:
        print("Bookmark not found. This is the first run (or a historical load).")
        return None

def update_last_run_timestamp(new_timestamp):
    """Writes the new latest timestamp to the S3 bookmark file."""
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=S3_BOOKMARK_KEY,
        Body=str(new_timestamp)
    )
    print(f"Updated bookmark timestamp to: {new_timestamp}")

def lambda_handler(event, context):
    """
    Main function to fetch data incrementally from OneSignal and save to S3.
    """
    # 1. Get credentials from AWS Secrets Manager
    secret_response = secrets_client.get_secret_value(SecretId=SECRET_ARN)
    secret_data = json.loads(secret_response["SecretString"])
    api_key = secret_data["api_key"]
    app_id = secret_data["app_id"]
    headers = {"Authorization": f"Basic {api_key}"} # Note: OneSignal docs often use Basic auth for REST API Key

    # 2. Get the last run timestamp from our bookmark
    last_run_ts = get_last_run_timestamp()

    # 3. Fetch data in a loop to handle pagination
    offset = 0
    total_fetched = 0
    latest_timestamp_in_batch = last_run_ts if last_run_ts is not None else 0

    while True:
        # Prepare API parameters
        params = {
            "app_id": app_id,
            "limit": API_LIMIT,
            "offset": offset
        }
        
        # For incremental runs, use the 'sent_after' filter
        if last_run_ts:
            params["sent_after"] = last_run_ts

        print(f"Fetching data with params: {params}")
        response = requests.get(API_URL, headers=headers, params=params)

        if response.status_code != 200:
            print(f"Error fetching data: {response.status_code} - {response.text}")
            return {"statusCode": response.status_code, "body": response.text}

        data = response.json()
        notifications = data.get("notifications", [])
        
        if not notifications:
            print("No new notifications found. Ending run.")
            break

        # Process the fetched data
        num_in_batch = len(notifications)
        total_fetched += num_in_batch
        print(f"Fetched {num_in_batch} notifications in this batch.")

        # Find the latest timestamp in the current batch to update the bookmark later
        # The 'completed_at' field is a Unix timestamp, perfect for this.
        max_ts_in_batch = max(n['completed_at'] for n in notifications if n.get('completed_at'))
        if max_ts_in_batch > latest_timestamp_in_batch:
            latest_timestamp_in_batch = max_ts_in_batch

        # Save this batch to a unique file in S3
        timestamp_str = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        s3_key = os.path.join(S3_PREFIX, f"data_{timestamp_str}_offset_{offset}.json")
        
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json.dumps(notifications).encode("utf-8")
        )
        print(f"Successfully saved batch to {s3_key}")

        # Check if we need to continue paginating
        if num_in_batch < API_LIMIT:
            print("Last page reached. Ending pagination.")
            break
        
        # Prepare for the next page
        offset += API_LIMIT
        time.sleep(1) # Be kind to the API

    # 4. Update the bookmark if new data was fetched
    if total_fetched > 0 and latest_timestamp_in_batch > (last_run_ts if last_run_ts else 0):
        update_last_run_timestamp(latest_timestamp_in_batch)
    else:
        print("No new data to update bookmark with.")

    return {
        "statusCode": 200,
        "body": json.dumps({"message": f"Successfully fetched {total_fetched} total notifications."})
    }

# To run locally (ensure AWS credentials are configured)
if __name__ == "__main__":
    lambda_handler(None, None)
