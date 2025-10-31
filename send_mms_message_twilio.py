import os
import json
import boto3
from datetime import datetime
from twilio.rest import Client
from twilio.base.exceptions import TwilioRestException

# --- Twilio Credentials ---
# Ensure these are set in your Lambda Environment Variables
TWILIO_ACCOUNT_SID = os.environ.get("TWILIO_ACCOUNT_SID", "AAAAAAAAAAA")
TWILIO_AUTH_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN", "bbbbbbbbbbbb")
# Set this to your MMS‑enabled US/Canada long‑code (not the toll‑free number)
TWILIO_PHONE_NUMBER = os.environ.get("TWILIO_PHONE_NUMBER", "+1234567890")

# --- S3 Datalake ---
# Ensure this is set in your Lambda Environment Variables
# e.g., "datalake-landingzone-221490242148-us-west-2"
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "datalake-landingzone-221490242148-us-west-2")

# Initialize AWS and Twilio clients
s3 = boto3.client("s3")
client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)


def _log_to_s3(bucket, context, input_body, status, log_data):
    """
    Logs the *EXECUTION* of the lambda to S3.
    Example path: s3://[bucket]/logs/YYYY/MM/DD/[aws_request_id].json
    """
    if not bucket:
        print("Warning: S3_BUCKET_NAME environment variable not set. Skipping EXECUTION log.")
        return

    try:
        now = datetime.utcnow()
        # Create a partitioned S3 key for the execution log
        s3_key = f"logs/{now.strftime('%Y/%m/%d')}/{context.aws_request_id}.json"

        # Construct the full log payload
        log_payload = {
            "invocation_id": context.aws_request_id,
            "log_timestamp_utc": now.isoformat(),
            "lambda_function_name": context.function_name,
            "status": status,
            "input_body": input_body,
            "outcome": log_data
        }

        # Write the log file to S3
        s3.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=json.dumps(log_payload),
            ContentType="application/json"
        )
        print(f"Successfully logged EXECUTION to S3: s3://{bucket}/{s3_key}")

    except Exception as e:
        # Don't fail the main lambda function if logging fails
        print(f"Error: Failed to log EXECUTION to S3. {str(e)}")


def _log_sent_message_to_s3(bucket, unique_filename, payload):
    """
    Logs the actual SENT MESSAGE data to S3, mirroring the n8n "replies" path.
    Example path: s3://[bucket]/hamza-twilio-sent-data/year=YYYY/month=MM/day=DD/[unique_filename].json
    """
    if not bucket:
        print("Warning: S3_BUCKET_NAME environment variable not set. Skipping SENT MESSAGE log.")
        return

    try:
        now = datetime.utcnow()
        # Create the datalake-partitioned path
        s3_key = (
            f"hamza-twilio-sent-data/"
            f"year={now.strftime('%Y')}/"
            f"month={now.strftime('%m')}/"
            f"day={now.strftime('%d')}/"
            f"{unique_filename}"
        )

        s3.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=json.dumps(payload),
            ContentType="application/json"
        )
        print(f"Successfully logged SENT MESSAGE to S3: s3://{bucket}/{s3_key}")
        
    except Exception as e:
        # Don't fail the main lambda function if logging fails
        print(f"Error: Failed to log SENT MESSAGE to S3. {str(e)}")


def _generate_availability_sentence(data_body):
    """Build a human-readable availability string based on boolean flags."""
    availability_map = {
        "weekdays_morning":    "weekday mornings (6am-10am)",
        "weekdays_midday":     "weekday mid-days (10am-2pm)",
        "weekdays_afternoon":  "weekday afternoons (2pm-6pm)",
        "weekdays_evening":    "weekday evenings (6pm-9pm)",
        "saturday_morning":    "Saturday mornings (6am-10am)",
        "saturday_midday":     "Saturday mid-days (10am-2pm)",
        "saturday_afternoon":  "Saturday afternoons (2pm-6pm)",
        "saturday_evening":    "Saturday evenings (6pm-9pm)",
        "sunday_morning":      "Sunday mornings (6am-10am)",
        "sunday_midday":       "Sunday mid-days (10am-2pm)",
        "sunday_afternoon":    "Sunday afternoons (2pm-6pm)",
        "sunday_evening":      "Sunday evenings (6pm-9pm)",
    }
    slots = [text for key, text in availability_map.items() if data_body.get(key)]
    if not slots:
        return "I've shared that your availability is flexible."
    if len(slots) == 1:
        return f"I've shared that you're available {slots[0]}."
    if len(slots) == 2:
        return f"I've shared that you're available {slots[0]} or {slots[1]}."
    return "I've shared that you're available " + ", ".join(slots[:-1]) + f" or {slots[-1]}."


def lambda_handler(event, context):
    print("Event received:", json.dumps(event))

    # Parse JSON body if necessary
    body = event.get("body", {})
    if isinstance(body, str):
        try:
            body = json.loads(body)
        except Exception as e:
            error_msg = {"error": f"Invalid JSON: {e}"}
            _log_to_s3(S3_BUCKET_NAME, context, event.get("body"), "failure", error_msg) # Execution log
            return {"statusCode": 400, "body": json.dumps(error_msg)}

    # Extract fields
    consumer_phone = body.get("consumer_phone")
    consumer_name  = body.get("consumer_name")  # This can be None
    agent_name     = body.get("agent_name")
    agent_phone    = body.get("agent_phone")

    # 1. MODIFIED VALIDATION: consumer_name is no longer required
    if not all([consumer_phone, agent_name, agent_phone]):
        error_msg = {"error": "Missing required fields: consumer_phone, agent_name, agent_phone"} # <-- MODIFIED
        _log_to_s3(S3_BUCKET_NAME, context, body, "failure", error_msg) # Execution log
        return {"statusCode": 400, "body": json.dumps(error_msg)}

    # 2. MODIFIED FRIENDLY NAME: Use phone number as fallback
    friendly_name = f"Conversation between {consumer_name or consumer_phone} and {agent_name}" # <-- MODIFIED

    # Delete previous conversation for this consumer only (so a new lead starts fresh)
    try:
        existing_convs = client.conversations.v1.participant_conversations.list(address=consumer_phone)
        for conv in existing_convs:
            print(f"Deleting previous conversation for consumer: {conv.conversation_sid}")
            client.conversations.v1.conversations(conv.conversation_sid).delete()
    except TwilioRestException as e:
        if e.status != 404:
            print(f"Warning: A Twilio error occurred while deleting conversations: {e}")

    # Create a new conversation
    try:
        conv = client.conversations.v1.conversations.create(friendly_name=friendly_name)
        conv_sid = conv.sid
    except Exception as e:
        error_msg = {"error": f"Failed to create conversation: {str(e)}"}
        _log_to_s3(S3_BUCKET_NAME, context, body, "failure", error_msg) # Execution log
        return {"statusCode": 500, "body": json.dumps(error_msg)}

    # 1) Add the chat participant first (projected address)
    chat_identity = f"agent:{agent_phone}"
    try:
        client.conversations.v1.conversations(conv_sid).participants.create(
            identity=chat_identity,
            messaging_binding_projected_address=TWILIO_PHONE_NUMBER
        )
    except Exception as e:
        error_msg = {"error": f"Failed to add chat participant: {str(e)}"}
        _log_to_s3(S3_BUCKET_NAME, context, body, "failure", error_msg) # Execution log
        return {"statusCode": 500, "body": json.dumps(error_msg)}

    # 2) Add consumer and agent as SMS participants (address only)
    try:
        client.conversations.v1.conversations(conv_sid).participants.create(
            messaging_binding_address=consumer_phone
        )
        client.conversations.v1.conversations(conv_sid).participants.create(
            messaging_binding_address=agent_phone
        )
    except Exception as e:
        error_msg = {"error": f"Failed to add SMS participants: {str(e)}"}
        _log_to_s3(S3_BUCKET_NAME, context, body, "failure", error_msg) # Execution log
        return {"statusCode": 500, "body": json.dumps(error_msg)}

    # 3. MODIFIED MESSAGE: Conditionally create the greeting
    greeting = f"Hi {consumer_name}, " if consumer_name else "" # <-- ADDED
    availability_sentence = _generate_availability_sentence(body)
    
    intro_message = (
        f"{greeting}Congratulations! I'm connecting you with {agent_name}, " # <-- MODIFIED
        f"who'll be helping you with your cash offer. {availability_sentence}\n\n"
        f"{agent_name} is excited to get started and can often deliver an offer within just 24 hours.\n\n"
        'Please reply "Yes" to confirm you\'ve received this message.'
    )
    try:
        # Capture the message resource and timestamp
        sent_at_utc = datetime.utcnow()
        message_resource = client.conversations.v1.conversations(conv_sid).messages.create(
            author=chat_identity,
            body=intro_message
        )
        message_sid = message_resource.sid # This will be an 'IM...' SID

    except Exception as e:
        error_msg = {"error": f"Failed to send message: {str(e)}"}
        _log_to_s3(S3_BUCKET_NAME, context, body, "failure", error_msg) # Execution log
        return {"statusCode": 500, "body": json.dumps(error_msg)}

    print("Conversation created:", conv_sid)
    
    # --- Log the Sent Message to Datalake (as TWO records) ---
    
    # This is the common data for both sent message logs
    # We use the same field names as your "reply" data for easy querying
    common_payload = {
        "message_sid": message_sid,
        "conversation_sid": conv_sid,
        "text_send": intro_message,       
        "customer_name": consumer_name,   
        "sender_number": TWILIO_PHONE_NUMBER,
        "received_at": sent_at_utc.isoformat(),
        "media_count": "0",
        "direction": "outbound-api", 
        
        # Set geo fields to null to match the reply schema
        "recipient_country": None,
        "recipient_state": None,
        "sender_zip": None,
        "sender_state": None,
        "sender_city": None,
        "sender_country": None
    }

    # 1. Create log payload for the CONSUMER
    consumer_log = common_payload.copy()
    consumer_log["recipient_number"] = consumer_phone
    # We create a unique filename to prevent overwrites
    consumer_filename = f"{message_sid}-{consumer_phone.replace('+', '')}.json"

    # 2. Create log payload for the AGENT
    agent_log = common_payload.copy()
    agent_log["recipient_number"] = agent_phone
    agent_filename = f"{message_sid}-{agent_phone.replace('+', '')}.json"

    # 3. Save both files to S3
    _log_sent_message_to_s3(S3_BUCKET_NAME, consumer_filename, consumer_log)
    _log_sent_message_to_s3(S3_BUCKET_NAME, agent_filename, agent_log)
    
    # --- END ---

    # Log success (Execution Log)
    success_data = {
        "conversation_sid": conv_sid,
        "message_sid": message_sid,
        "message": intro_message
    }
    _log_to_s3(S3_BUCKET_NAME, context, body, "success", success_data)

    return {
        "statusCode": 200,
        "body": json.dumps(success_data)
    }

#########################################
           # Input Payload #
#########################################
# {
#   "body": "{\"consumer_phone\": \"++1111111111\",\"consumer_name\": \"Hamza\",\"agent_name\": \"Santiago\",\"agent_phone\": \"+2222222\",\"is_flexible\": \"true\",\"message\": \"You can visit anytime\",\"weekdays_morning\": true,\"weekdays_midday\": false,\"weekdays_afternoon\": false,\"weekdays_evening\": false,\"saturday_morning\": false,\"saturday_midday\": false,\"saturday_afternoon\": false,\"saturday_evening\": false,\"sunday_morning\": false,\"sunday_midday\": false,\"sunday_afternoon\": false,\"sunday_evening\": false}"
# }
