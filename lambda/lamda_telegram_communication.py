import json
import logging
import telegram

# Config
TELEGRAM_TOKEN = "7701970803:AAHMBH5xrO_bD7jPZxxqQcRlm8tlsGkkjEs"
TELEGRAM_CHAT_ID = "-4790922611"

bot = telegram.Bot(token=TELEGRAM_TOKEN)

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


def send_telegram_message(message):
    """
    Sends a message to the configured Telegram chat.
    """
    try:
        bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message)
        logger.debug(f"Message sent to Telegram: {message}")
    except telegram.error.TelegramError as e:
        raise e


def send_telegram_image(image_path, caption=None):
    """
    Sends an image to the configured Telegram chat.
    """
    try:
        with open(image_path, 'rb') as image:
            bot.send_photo(chat_id=TELEGRAM_CHAT_ID, photo=image, caption=caption)
            logger.debug(f"Image sent to Telegram: {image_path}")
    except telegram.error.TelegramError as e:
        raise e


def lambda_handler(event, context):
    """
    Lambda function entry point for sending Telegram messages or images.
    The event should specify the action ('send_message' or 'send_image').
    """
    try:
        logger.info(f"Received event: {json.dumps(event, indent=2)}")

        action = event.get('action')

        if action == 'send_message':
            message = event.get('message', 'Default message')
            send_telegram_message(message)

        elif action == 'send_image':
            image_path = event.get('image_path')
            caption = event.get('caption', None)
            if not image_path:
                raise ValueError("Image path must be provided for 'send_image' action.")
            send_telegram_image(image_path, caption)

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
    except telegram.error.TelegramError as e:
        logger.error(f"TelegramError: {str(e)}")
        return {
            "statusCode": 502,
            "body": json.dumps({
                "error": "Failed to process the Telegram request.",
                "details": str(e)
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
