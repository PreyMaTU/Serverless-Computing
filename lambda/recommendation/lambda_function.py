import json
import logging
from datetime import datetime, timedelta

import boto3

# General Config
dynamodb = boto3.client('dynamodb')
lambda_client = boto3.client('lambda')
SENSOR_DATA_TABLE = 'Sensordata'

TELEGRAM_LAMBDA_ARN = 'arn:aws:lambda:eu-north-1:881490115333:function:Telegram_Communication'

EVENT_IDEMPOTENCY_TABLE = 'EventIdempotencyTable'
EVENT_IDEMPOTENCY_FUNCTION_NAME = 'RecommendationFunction'

TIME_WINDOW_MINUTES = 30  # Time windows for the analysis = now - TIME_WINDOW_MINUTES -> analysis in DB

# Sensor Type Config
SENSOR_CONFIG = {
    "MQTT-Master": {
        "parameters": {
            "soil_moisture": {
                "min": 30,
                "max": 95,
                "low_message": "{location}: Soil moisture is critically low ({value}%). Consider watering immediately.",
                "high_message": "{location}: Soil moisture is too high ({value}%). Avoid overwatering."
            },
            "temperature": {
                "min": -7,
                "max": 30,
                "low_message": "{location}: Soil sensor detected low temperature ({value}°C). Frost protection may be required.",
                "high_message": "{location}: Soil sensor detected high temperature ({value}°C). Consider shading or cooling measures."
            }
        }
    },
    "IoT-2000": {
        "parameters": {
            "humidity": {
                "min": 30,
                "max": 95,
                "low_message": "{location}: Weather station reports low humidity ({value}%). Monitor conditions closely.",
                "high_message": "{location}: Weather station reports high humidity ({value}%). Take precautions."
            },
            "temperature": {
                "min": -7,
                "max": 30,
                "low_message": "{location}: Weather station reports low temperature ({value}°C). Frost protection recommended.",
                "high_message": "{location}: Weather station reports high temperature ({value}°C). Cooling measures advised."
            }
        }
    },
    "sensormatic": {
        "parameters": {
            "humidity": {
                "min": 30,
                "max": 95,
                "low_message": "{location}: Sensormatic reports low humidity ({value}%). Monitor conditions closely.",
                "high_message": "{location}: Sensormatic reports high humidity ({value}%). Take precautions."
            },
            "temperature": {
                "min": -7,
                "max": 30,
                "low_message": "{location}: Sensormatic reports low temperature ({value}°C). Frost protection may be required.",
                "high_message": "{location}: Sensormatic reports high temperature ({value}°C). Cooling measures advised."
            }
        }
    }
}

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


def invoke_telegram_lambda(action, payload):
    """Invoke the Telegram Lambda function with the specified action and payload."""
    try:
        response = lambda_client.invoke(
            FunctionName=TELEGRAM_LAMBDA_ARN,
            InvocationType='RequestResponse',
            Payload=json.dumps({"action": action, **payload})
        )
        logger.debug(f"Telegram Lambda invoked with action: {action}, response: {response}")
    except Exception as e:
        raise e


def generate_combined_recommendations(sensor_items):
    """Generate a combined recommendation message for all sensors."""
    recommendations = []
    for sensor_data in sensor_items:
        sensor_type = sensor_data['sensor_type']['S']
        location = sensor_data['location']['M']
        latitude = location['lat']['N']
        longitude = location['lon']['N']
        location_string = f"Sensor (Lat {latitude}, Lon {longitude})"
        measurements = sensor_data['measurements']['M']

        if sensor_type not in SENSOR_CONFIG:
            continue

        sensor_params = SENSOR_CONFIG[sensor_type]["parameters"]
        for param, config in sensor_params.items():
            if param in measurements:
                value = float(measurements[param]["N"])
                if value < config["min"]:
                    recommendations.append(
                        f"{config['low_message'].format(location=location_string, value=value)}"
                    )
                elif value > config["max"]:
                    recommendations.append(
                        f"{config['high_message'].format(location=location_string, value=value)}"
                    )

    # Testing Purposes
    if not recommendations:
        return "(TESTING!) ✅ All sensors are operating within normal parameters."

    return "\n\n".join(recommendations)


def get_recent_sensor_data(trigger_time):
    """Fetch sensor data from the DynamoDB table based on the configured timeframe."""
    try:
        start_time_epoch = int((trigger_time - timedelta(minutes=TIME_WINDOW_MINUTES)).timestamp())

        response = dynamodb.scan(
            TableName=SENSOR_DATA_TABLE,
            FilterExpression='#ts >= :start_time',
            ExpressionAttributeNames={
                '#ts': 'timestamp'  # Alias for "timestamp"
            },
            ExpressionAttributeValues={
                ':start_time': {'N': str(start_time_epoch)}
            }
        )
        return response.get('Items', [])
    except Exception as e:
        logger.error(f"Error querying DynamoDB: {str(e)}")
        raise


def lambda_handler(event, context):
    """Lambda function to fetch data from DynamoDB and evaluate/analyze recommendations."""
    try:
        logger.info(f"Received event: {json.dumps(event, indent=2)}")

        # Extract the EventBridge event ID
        event_id = event['id']
        pk = EVENT_IDEMPOTENCY_FUNCTION_NAME
        sk = event_id

        # Check idempotency
        if is_event_processed(pk, sk):
            logger.info(f"Event with ID {event_id} already processed for {pk}.")
            return {
                "statusCode": 200,
                "body": json.dumps({"message": "Event already processed."})
            }

        # Get trigger time from event bridge trigger
        trigger_time_str = event['time']
        trigger_time = datetime.fromisoformat(trigger_time_str.replace("Z", "+00:00"))

        sensor_items = get_recent_sensor_data(trigger_time)

        combined_message = generate_combined_recommendations(sensor_items)
        if combined_message:
            invoke_telegram_lambda(
                action='send_message',
                payload={"message": combined_message}
            )
            logger.info("Recommendations sent to Telegram.")
        else:
            logger.info("No recommendations to send.")

        # Mark the event as processed
        mark_event_as_processed(pk, sk)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Recommendations processed and sent successfully."
            })
        }

    except ValueError as e:
        logger.error({str(e)})
        return {
            "statusCode": 400,
            "body": json.dumps({
                "error": "A value error occurred while processing the recommendations.",
                "details": str(e)
            })
        }
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": "An unexpected error occurred while processing the recommendations.",
                "details": str(e)
            })
        }
