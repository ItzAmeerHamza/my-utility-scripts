import os
import json
from twilio.rest import Client
from twilio.base.exceptions import TwilioRestException

# --- Initialize Twilio Client ---
# Load credentials from Lambda Environment Variables
# NOTE: We only need the variables for this one step
try:
    account_sid = os.environ.get("TWILIO_ACCOUNT_SID", "AAAAAAAAAAAAAA")
    auth_token = os.environ.get("TWILIO_AUTH_TOKEN", "bbbbbbbbb")
    brand_number = os.environ.get("TWILIO_PHONE_NUMBER", "+1234567890")
    
    client = Client(account_sid, auth_token)
except KeyError as e:
    print(f"FATAL: Missing environment variable: {e}")
    # This will fail the Lambda load if credentials aren't set.
    raise

# --- Time Slot Logic ---
# 1. This list defines the "priority" for checking.
TIME_SLOT_PRIORITY = [
    "weekdays_morning", "weekdays_midday", "weekdays_afternoon", "weekdays_evening",
    "saturday_morning", "saturday_midday", "saturday_afternoon", "saturday_evening",
    "sunday_morning", "sunday_midday", "sunday_afternoon", "sunday_evening"
]

# 2. This dictionary maps the JSON keys to "pretty" names for the SMS
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
        # Check if the key exists AND its value is True
        if event_payload.get(slot_key) is True:
            # Return the "pretty name" for the SMS
            return PRETTY_TIME_NAMES.get(slot_key, slot_key)
            
    # If no 'True' slot is found, return your specified default.
    return "Weekdays - Afternoon"

# --- Main Handler ---

def lambda_handler(event, context):
    """
    Lambda handler to send ONLY the Step 1 pretext message to the agent.
    """

    print(f"Received event: {json.dumps(event)}")

    try:
        # --- 1. Parse Input Data ---
        agent_full_name = event['agent_name']
        agent_phone = event['agent_phone']
        agent_first_name = agent_full_name.split(' ')[0] # "Robert"
        
        customer_name = event['consumer_name']
        
        address = event['address_line']
        zip_code = event['zip_code']
        beds = event['beds']
        baths = event['baths']
        
        # Get appointment time using our helper function
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
    message_body_agent = (
        f"Hi {agent_first_name},\n\n"
        f"You've got a cash offer appointment scheduled with {customer_name} "
        f"at {address}, {zip_code}.\n\n"
        f"Property: {beds} Beds / {baths} Baths\n"
        f"Appointment Window: {appointment_time}\n\n"
        "Please text the homeowner the time you will arrive and confirm. "
        "There's no need to ask for their availability — the appointment "
        "has already been set."
    )
    
    print(f"Constructed message for {agent_phone}: \n{message_body_agent}")
        
    # --- 3. Send the Message (Step 1 Only) ---
    try:
        msg1 = client.messages.create(
            body=message_body_agent,
            from_=brand_number,
            to=agent_phone
        )
        print(f"  > Success (SID: {msg1.sid})")
        
        # Return a simple success response
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Agent pretext message sent successfully!',
                'sid': msg1.sid,
                'status': msg1.status
            })
        }
        
    except TwilioRestException as e:
        # Handle Twilio API errors
        print(f"Twilio API Error: {e.msg}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f"Failed to send message: {e.msg}"
            })
        }
    except Exception as e:
        # Handle other unexpected errors
        print(f"Unexpected Error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"An unexpected error occurred: {e}")
        }
