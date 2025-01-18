import json
import logging

import boto3
import requests
from botocore.exceptions import BotoCoreError

# Config
TELEGRAM_TOKEN = "7701970803:AAHMBH5xrO_bD7jPZxxqQcRlm8tlsGkkjEs"
TELEGRAM_CHAT_ID = "-4731796983"

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"


def send_telegram_message(message):
    """
    Sends a message to the configured Telegram chat using the Telegram API.
    Splits the message into smaller parts if it exceeds the maximum length.
    """
    url = f"{TELEGRAM_API_URL}/sendMessage"
    max_length = 3500  # Safer limit for long messages
    start = 0

    while start < len(message):
        # Find the end of the current chunk
        end = min(start + max_length, len(message))
        if end < len(message):
            newline_index = message.rfind("\n", start, end)  # Try to break at a newline
            if newline_index > start:
                end = newline_index

        chunk = message[start:end]
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": chunk,
        }
        try:
            response = requests.post(url, json=payload)
            if response.status_code == 200:
                logger.debug(f"Message chunk sent to Telegram: {chunk}")
            else:
                raise RuntimeError(f"Failed to send message chunk. Status: {response.status_code}, Response: {response.text}")
        except requests.RequestException as e:
            raise RuntimeError(f"An error occurred while sending a message to Telegram: {str(e)}")

        # Move to the next chunk
        start = end


def send_telegram_image(bucket_name, s3_key, caption=None):
    """
    Downloads an image from S3 and sends it to Telegram using the Telegram API.
    """
    s3 = boto3.client('s3')
    download_path = f"/tmp/{s3_key.split('/')[-1]}"
    try:
        # Download the file from S3
        s3.download_file(bucket_name, s3_key, download_path)
        logger.debug(f"Image downloaded from S3: {bucket_name}/{s3_key} to {download_path}")

        url = f"{TELEGRAM_API_URL}/sendPhoto"
        with open(download_path, 'rb') as image:
            files = {"photo": image}
            data = {"chat_id": TELEGRAM_CHAT_ID, "caption": caption}
            response = requests.post(url, files=files, data=data)
            if response.status_code == 200:
                logger.debug(f"Image sent to Telegram: {bucket_name}/{s3_key}")
            else:
                raise RuntimeError(f"Failed to send image to Telegram. Response: {response.text}")
    except BotoCoreError as e:
        raise RuntimeError(f"Failed to download image from S3: {str(e)}")
    except requests.RequestException as e:
        raise RuntimeError(f"An error occurred while sending an image to Telegram: {str(e)}")


def lambda_handler(event, context):
    """
    Lambda function entry point for sending Telegram messages or images.
    """
    try:
        logger.info(f"Received event: {json.dumps(event, indent=2)}")

        action = event.get('action')

        if action == 'send_message':
            message = event.get('message')
            if not message:
                raise ValueError("'message' must be provided for 'send_message' action.")
            send_telegram_message(message)

        elif action == 'send_image':
            bucket_name = event.get('bucket_name')
            s3_key = event.get('s3_key')
            caption = event.get('caption', None)

            if not bucket_name or not s3_key:
                raise ValueError("Both 'bucket_name' and 's3_key' must be provided for 'send_image' action.")

            send_telegram_image(bucket_name, s3_key, caption)

        else:
            raise ValueError("Invalid action '{action}' specified. Use 'send_message' or 'send_image'.")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Telegram action completed successfully.",
                "event_type": action
            })
        }

    except ValueError as e:
        logger.error({str(e)})
        return {
            "statusCode": 400,
            "body": json.dumps({
                "error": "A value error occurred while processing the request.",
                "details": str(e)
            })
        }
    except RuntimeError as e:
        logger.error(f"RuntimeError: {str(e)}")
        return {
            "statusCode": 502,
            "body": json.dumps({
                "error": str(e)
            })
        }
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": "An unexpected error occurred while processing the request.",
                "details": str(e)
            })
        }
