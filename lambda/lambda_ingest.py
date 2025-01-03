import json
import boto3
from datetime import datetime

# Initialize the DynamoDB client
dynamodb = boto3.client('dynamodb')

# DynamoDB table name
TABLE_NAME = 'Sensordata'

def lambda_handler(event, context):
    """
    Handles incoming events from AWS IoT Core, logs the data, and saves it into DynamoDB.
    """
    try:
        # Log the event data
        print("Received event:", json.dumps(event, indent=2))
        
        # Extract data from the event
        location = event.get('location')
        humidity = event.get('humidity', -1)
        timestamp = event.get('timestamp')

        if not location or not timestamp:
            raise ValueError(f"Error: Bad message {event}")

        timestamp = int(datetime.fromisoformat(timestamp).timestamp())

        # Save data into DynamoDB
        dynamodb.put_item(
            TableName = TABLE_NAME,
            Item = {
                'location': {'S': location},
                'timestamp': {'N': str(timestamp)},
                'humidity': {'N': str(humidity)}
            }
        )
        
        print(f"Data saved to DynamoDB for location {location} at timestamp {timestamp}")
       
        return {
            'statusCode': 200,
            'body': json.dumps('Data saved successfully!')
        }
    except Exception as e:
        print("Error:", str(e))
        return {
            'statusCode': 500,
            'body': json.dumps('Error processing data!')
        }
