import json
import logging
import boto3
from botocore.exceptions import BotoCoreError, ClientError

# Config
s3 = boto3.client('s3')
lambda_client = boto3.client('lambda')
BUCKET_NAME = "heatmap-bucket-agrisense"
TELEGRAM_LAMBDA_ARN = 'arn:aws:lambda:eu-north-1:881490115333:function:Telegram_Communication'

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


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

        latest_key, last_modified = get_latest_heatmap()
        timestamp = last_modified.strftime("%Y-%m-%d %H:%M:%S")
        caption = (f"The latest heatmap visualization of your field conditions is now available. "
                   f"This map was generated on {timestamp}. "
                   f"Check for updates on soil moisture and temperature to plan your next steps!")

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

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": f"Heatmap {latest_key} sent successfully.",
                "timestamp": timestamp
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
