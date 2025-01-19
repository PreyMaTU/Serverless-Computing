# Telegram Communication Function

### Requirements:

- `requirements.txt` includes:
    - `boto3`
    - `requests`
- **Additional setup required:**
    - **Install the `requests` library locally in a folder and upload this folder along with the Lambda function as a
      ZIP file to AWS.**

### Configuration Variables:

- `TELEGRAM_TOKEN`: The token for authenticating with the Telegram Bot API.
- `TELEGRAM_CHAT_ID`: The chat ID of the Telegram group where messages will be sent.
