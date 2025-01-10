import json
import logging
import boto3

# General Config
DYNAMODB_TABLE = 'Sensordata'
TELEGRAM_LAMBDA_ARN = 'arn:aws:lambda:region:account-id:function:lamda_telegram_communication'

# Sensor Type Config
SENSOR_CONFIG = {
    "MQTT-Master": {
        "parameters": {
            "soil_moisture": {
                "min": 30,
                "max": 80,
                "low_message": "{location}: Soil moisture is critically low ({value}%). Consider watering immediately.",
                "high_message": "{location}: Soil moisture is too high ({value}%). Avoid overwatering."
            },
            "temperature": {
                "min": 0,
                "max": 30,
                "low_message": "{location}: Soil sensor detected low temperature ({value}°C). Frost protection may be required.",
                "high_message": "{location}: Soil sensor detected high temperature ({value}°C). Consider shading or cooling measures."
            }
        }
    },
    "IoT-2000": {
        "parameters": {
            "humidity": {
                "min": 20,
                "max": 80,
                "low_message": "{location}: Weather station reports low humidity ({value}%). Monitor conditions closely.",
                "high_message": "{location}: Weather station reports high humidity ({value}%). Take precautions."
            },
            "temperature": {
                "min": 0,
                "max": 30,
                "low_message": "{location}: Weather station reports low temperature ({value}°C). Frost protection recommended.",
                "high_message": "{location}: Weather station reports high temperature ({value}°C). Cooling measures advised."
            }
        }
    },
    "sensormatic": {
        "parameters": {
            "humidity": {
                "min": 20,
                "max": 80,
                "low_message": "{location}: Sensormatic reports low humidity ({value}%). Monitor conditions closely.",
                "high_message": "{location}: Sensormatic reports high humidity ({value}%). Take precautions."
            },
            "temperature": {
                "min": 0,
                "max": 30,
                "low_message": "{location}: Sensormatic reports low temperature ({value}°C). Frost protection may be required.",
                "high_message": "{location}: Sensormatic reports high temperature ({value}°C). Cooling measures advised."
            }
        }
    }
}

dynamodb = boto3.client('dynamodb')
lambda_client = boto3.client('lambda')

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


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


def process_sensor_data(sensor_type, location_string, measurements):
    """Process sensor data based on the sensor type configuration."""
    if sensor_type not in SENSOR_CONFIG:
        raise ValueError(f"Unknown sensor type: '{sensor_type}'.")

    sensor_params = SENSOR_CONFIG[sensor_type]["parameters"]
    for param, config in sensor_params.items():
        if param in measurements:
            value = float(measurements[param]["N"])
            if value < config["min"]:
                invoke_telegram_lambda(
                    action='send_message',
                    payload={"message": config["low_message"].format(location=location_string, value=value)}
                )
            elif value > config["max"]:
                invoke_telegram_lambda(
                    action='send_message',
                    payload={"message": config["high_message"].format(location=location_string, value=value)}
                )


def check_and_notify(sensor_data):
    """Analyze sensor data and send recommendations."""
    sensor_type = sensor_data['sensor_type']['S']
    location = sensor_data['location']['M']
    latitude = location['lat']['N']
    longitude = location['lon']['N']
    location_string = f"Location (Lat: {latitude}, Lon: {longitude})"
    measurements = sensor_data['measurements']['M']

    process_sensor_data(sensor_type, location_string, measurements)


def lambda_handler(event, context):
    """Lambda function to fetch data from DynamoDB and evaluate/analyze recommendations."""
    try:
        logger.info(f"Received event: {json.dumps(event, indent=2)}")

        response = dynamodb.scan(TableName=DYNAMODB_TABLE)
        sensor_items = response.get('Items', [])

        for sensor_data in sensor_items:
            check_and_notify(sensor_data)

        logger.debug("Recommendations evaluated.")
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Recommendations processed successfully."})
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
