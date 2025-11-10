import os
import json
from twilio.rest import Client
from twilio.base.exceptions import TwilioRestException

# --- Initialize Twilio Client ---
# Load credentials from Lambda Environment Variables
try:
    account_sid = os.environ.get("TWILIO_ACCOUNT_SID", "AAAAAAAA")
    auth_token = os.environ.get("TWILIO_AUTH_TOKEN", "bbbbb")
    brand_number = os.environ.get("TWILIO_PHONE_NUMBER", "+1234567890")
    
    # --- NEW: Your internal phone number ---
    # Set this to your personal phone number to receive the notification
    internal_notify_phone = os.environ.get("INTERNAL_NOTIFY_PHONE", "+14154126965")
    
    client = Client(account_sid, auth_token)
except KeyError as e:
    print(f"FATAL: Missing environment variable: {e}")
    raise

# --- Time Slot Logic (Copied from your other function) ---
# This is needed to calculate the {appointment_time} variable
TIME_SLOT_PRIORITY = [
    "weekdays_morning", "weekdays_midday", "weekdays_afternoon", "weekdays_evening",
    "saturday_morning", "saturday_midday", "saturday_afternoon", "saturday_evening",
    "sunday_morning", "sunday_midday", "sunday_afternoon", "sunday_evening"
]

PRETTY_TIME_NAMES = {
    "weekdays_morning": "Weekdays - Morning",
    "weekdays_midday": "Weekdays - Midday",
    "weekdays_afternoon": "Weekdays - Afternoon",
    "weekdays_evening": "Weekdays - Evening",
    "saturday_morning": "Saturday - Morning",
    "saturday_midday": "Saturday - Midday",
    "saturday_afternoon": "Saturday - Afternoon",
    "saturday_evening": "Saturday - Evening",
    "sunday_morning": "Sunday - Morning",
    "sunday_midday": "Sunday - Midday",
    "sunday_afternoon": "Sunday - Afternoon",
    "sunday_evening": "Sunday - Evening"
}

def find_first_appointment_time(event_payload):
    """
    Finds the first 'True' time slot based on the priority list.
    """
    for slot_key in TIME_SLOT_PRIORITY:
        if event_payload.get(slot_key) is True:
            return PRETTY_TIME_NAMES.get(slot_key, slot_key)
            
    # Default as per your previous logic
    return "Weekdays - Afternoon"

# --- Main Handler ---

def lambda_handler(event, context):
    """
    Lambda handler to send ONLY the final internal notification.
    """
    
    print(f"Received event: {json.dumps(event)}")
    
    try:
        # --- 1. Parse Input Data ---
        # We map your payload fields to the template variables
        customer_name = event['consumer_name']
        customer_phone = event['consumer_phone']
        address_line_1 = event['address_line'] # Renaming for the template
        zip_code = event['zip_code']
        agent_name = event['agent_name']
        agent_phone = event['agent_phone']
        
        # Calculate appointment time using the same logic
        appointment_time = find_first_appointment_time(event)

    except KeyError as e:
        print(f"Error: Missing required key in event data: {e}")
        return {
            'statusCode': 400,
            'body': json.dumps(f"Missing required data key: {e}")
        }
    except Exception as e:
        print(f"Error parsing data: {e}")
        return {
            'statusCode': 400,
            'body': json.dumps(f"Error parsing input data: {e}")
        }

    # --- 2. Build the Message Body ---
    # Using your exact template and f-strings
    message_body = (
        f"New appointment confirmed.\n"
        f"{customer_name} ({customer_phone})\n"
        f"{address_line_1}, {zip_code}\n"
        f"Agent: {agent_name} \n"
        f"Phone: ({agent_phone})\n"
        f"Time: {appointment_time}."
    )
    
    print(f"Constructed internal notification for {internal_notify_phone}")
        
    # --- 3. Send the Internal Notification ---
    try:
        message = client.messages.create(
            body=message_body,
            from_=brand_number,
            to=internal_notify_phone # Sending to your internal number
        )
        
        print(f"  > Success (SID: {message.sid})")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Internal notification sent successfully!',
                'sid': message.sid
            })
        }
        
    except TwilioRestException as e:
        print(f"Twilio API Error: {e.msg}")
        return {
            'statusCode': 500,
            'body': json.dumps({ 'message': f"Failed to send message: {e.msg}" })
        }
    except Exception as e:
        print(f"Unexpected Error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"An unexpected error occurred: {e}")
        }
