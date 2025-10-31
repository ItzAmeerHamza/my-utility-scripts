import os
import json
from twilio.rest import Client
from twilio.base.exceptions import TwilioRestException

# --- Twilio Credentials ---
TWILIO_ACCOUNT_SID = os.environ.get("TWILIO_ACCOUNT_SID", "AAAAAAAAA")
TWILIO_AUTH_TOKEN = os.environ.get("TWILIO_AUTH_TOKEN", "bbbbbbbbbb")
# Set this to your MMS‑enabled US/Canada long‑code (not the toll‑free number)
TWILIO_PHONE_NUMBER = os.environ.get("TWILIO_PHONE_NUMBER", "+1234567890")

client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)

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
            return {"statusCode": 400, "body": json.dumps({"error": f"Invalid JSON: {e}"})}

    # Extract required fields
    consumer_phone = body.get("consumer_phone")
    consumer_name  = body.get("consumer_name")
    agent_name     = body.get("agent_name")
    agent_phone    = body.get("agent_phone")

    if not all([consumer_phone, consumer_name, agent_name, agent_phone]):
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Missing required fields: consumer_phone, consumer_name, agent_name, agent_phone"})
        }

    friendly_name = f"Conversation between {consumer_name} and {agent_name}"

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
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Failed to create conversation: {str(e)}"})
        }

    # 1) Add the chat participant first (projected address)
    chat_identity = f"agent:{agent_phone}"
    try:
        client.conversations.v1.conversations(conv_sid).participants.create(
            identity=chat_identity,
            messaging_binding_projected_address=TWILIO_PHONE_NUMBER  # e.g. +14647333804
        )
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Failed to add chat participant: {str(e)}"})
        }

    # 2) Add consumer and agent as SMS participants (address only)
    try:
        client.conversations.v1.conversations(conv_sid).participants.create(
            messaging_binding_address=consumer_phone
        )
        client.conversations.v1.conversations(conv_sid).participants.create(
            messaging_binding_address=agent_phone
        )
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Failed to add SMS participants: {str(e)}"})
        }

    # 3) Compose and send the intro message from the chat participant
    availability_sentence = _generate_availability_sentence(body)
    intro_message = (
        f"Hi {consumer_name}, Congratulations! I'm connecting you with {agent_name}, "
        f"who'll be helping you with your cash offer. {availability_sentence}\n\n"
        f"{agent_name} is excited to get started and can often deliver an offer within just 24 hours.\n\n"
        'Please reply "Yes" to confirm you\'ve received this message.'
    )
    try:
        client.conversations.v1.conversations(conv_sid).messages.create(
            author=chat_identity,
            body=intro_message
        )
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Failed to send message: {str(e)}"})
        }

    print("Conversation created:", conv_sid)
    return {
        "statusCode": 200,
        "body": json.dumps({
            "conversation_sid": conv_sid,
            "message": intro_message
        })
    }
