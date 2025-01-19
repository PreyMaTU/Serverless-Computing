import json
import logging
from datetime import timezone
from zoneinfo import ZoneInfo

import boto3
from botocore.exceptions import BotoCoreError, ClientError

# Config
s3 = boto3.client('s3')
lambda_client = boto3.client('lambda')
dynamodb = boto3.client('dynamodb')

BUCKET_NAME = "heatmap-bucket-agrisense"
TELEGRAM_LAMBDA_ARN = 'arn:aws:lambda:eu-north-1:881490115333:function:Telegram_Communication'

EVENT_IDEMPOTENCY_TABLE = 'EventIdempotencyTable'
EVENT_IDEMPOTENCY_FUNCTION_NAME = "DeliveryVisualizationFunction"

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def is_event_processed(pk, sk):
    """
    Check if the event with the given primary and sort key has already been processed.
    """
    try:
        response = dynamodb.get_item(
            TableName=EVENT_IDEMPOTENCY_TABLE,
            Key={
                'pk': {'S': pk},
                'sk': {'S': sk}
            }
        )
        return 'Item' in response
    except Exception as e:
        raise e


def mark_event_as_processed(pk, sk):
    """
    Mark the event with the given primary and sort key as processed.
    """
    try:
        dynamodb.put_item(
            TableName=EVENT_IDEMPOTENCY_TABLE,
            Item={
                'pk': {'S': pk},
                'sk': {'S': sk}
            }
        )
    except Exception as e:
        raise e

def format_timestamp(timestamp):
    """
    Formats the timestamp in a more readable format for both UTC and local time.
    """
    utc_time = timestamp.astimezone(timezone.utc)
    utc_formatted = utc_time.strftime("%A, %d %B %Y at %H:%M:%S UTC")

    local_time = timestamp.astimezone(ZoneInfo("Europe/Vienna"))
    local_formatted = local_time.strftime("%A, %d %B %Y at %H:%M:%S %Z")

    return utc_formatted, local_formatted


def get_latest_heatmap():
    """
    Fetches the latest heatmap file from the S3 bucket.
    """
    try:
        response = s3.list_objects_v2(Bucket=BUCKET_NAME)
        files = response.get('Contents', [])
        if not files:
            raise FileNotFoundError("No heatmap files available in the bucket.")
        latest_file = max(files, key=lambda x: x['LastModified'])
        logger.debug(f"Latest heatmap: {latest_file['Key']} (Last Modified: {latest_file['LastModified']})")
        return latest_file['Key'], latest_file['LastModified']
    except (BotoCoreError, ClientError) as e:
        raise RuntimeError(f"Failed to retrieve heatmap from S3: {str(e)}")


def lambda_handler(event, context):
    """
    Lambda function to fetch the latest heatmap and send it via Telegram.
    """
    try:
        logger.info(f"Received event: {json.dumps(event, indent=2)}")

        # Extract event details
        sequencer = event['Records'][0]['s3']['object']['sequencer']
        pk = EVENT_IDEMPOTENCY_FUNCTION_NAME
        sk = sequencer

        logger.debug(f"Processing event with sequencer: {sequencer}")

        # Check idempotency
        if is_event_processed(pk, sk):
            logger.info(f"Event with sequencer {sequencer} already processed for {pk}.")
            return {
                "statusCode": 200,
                "body": json.dumps({"message": "Event already processed."})
            }


        latest_key, last_modified = get_latest_heatmap()
        utc_formatted, local_formatted = format_timestamp(last_modified)

        caption = (
            f"The latest heatmap visualization of your field conditions is now available.\n\n"
            f"🌐 UTC Time: {utc_formatted}\n"
            f"🕒 Local Time: {local_formatted}\n\n"
            f"Check for updates on soil moisture and temperature to plan your next steps!"
        )

        response = lambda_client.invoke(
            FunctionName=TELEGRAM_LAMBDA_ARN,
            InvocationType='RequestResponse',
            Payload=json.dumps({
                "action": "send_image",
                "bucket_name": BUCKET_NAME,
                "s3_key": latest_key,
                "caption": caption
            })
        )
        logger.debug(f"Telegram Lambda invoked successfully: {response}")

        # Mark the event as processed
        mark_event_as_processed(pk, sk)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": f"Heatmap {latest_key} sent successfully.",
                "utc_timestamp": utc_formatted
            })
        }

    except FileNotFoundError as e:
        logger.error(f"FileNotFoundError: {str(e)}")
        return {
            "statusCode": 404,
            "body": json.dumps({
                "error": str(e)
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
                "error": "An unexpected error occurred.",
                "details": str(e)
            })
        }
