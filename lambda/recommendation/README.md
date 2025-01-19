# Recommendation Function

## Requirements

- `requirements.txt` includes:
    - `boto3`
- **No additional libraries need to be uploaded to AWS, as `boto3` is included in the default Lambda runtime.**

## Configuration Variables

- `TELEGRAM_LAMBDA_ARN`: The ARN of the Telegram Communication Lambda function (default:
  `'arn:aws:lambda:eu-north-1:881490115333:function:Telegram_Communication'`).
- `TIME_WINDOW_MINUTES`: The time window for analysis in minutes (default: `30`).

### Sensor Type Configuration

- Configuration for supported sensor types (`MQTT-Master`, `IoT-2000`, `sensormatic`) with thresholds for:
    - `soil_moisture`
    - `temperature`
    - `humidity`
- Messages for low and high threshold violations are provided.
