import json
import boto3
from datetime import datetime

# Initialize the DynamoDB client
dynamodb = boto3.client('dynamodb')

# DynamoDB table name
TABLE_NAME = 'Sensordata'

def parse_geo_location_string( s: str ):
    if not s:
        raise ValueError('Expected geo location string')

    parts= s.split('/')
    if len(parts) != 2 or not parts[0].endswith('N') or not parts[1].endswith('E'):
        raise ValueError('Bad geo location string')
    
    # Lat, Lon
    return float(parts[0][:-1]), float(parts[1][:-1])

def normalize_sensor_data( event ):
    sensor_type= event.get('sensor_type')
    sensor_id= event.get('sensor_id')
    timestamp= event.get('timestamp')
    
    if not sensor_id or not timestamp or not sensor_type:
        raise ValueError(f"Error: Bad message {event}")

    timestamp = int(datetime.fromisoformat(timestamp).timestamp())

    if sensor_type == 'IoT-2000':
        return {
            'sensor_type': {'S': sensor_type},
            'sensor_id': {'S': sensor_id},
            'timestamp': { 'N': str(timestamp) },
            'location': {
                'M' : {
                    'lon': {'N': str(event.get('location').get('lon'))},
                    'lat': {'N': str(event.get('location').get('lat'))}
                }
            },
            'measurements': {
                'M' : {
                    'humidity': {'N': str(event.get('humidity'))},         # 0-100%
                    'temperature': {'N': str(event.get('temperature'))}    # °C
                }
            }
        }
    elif sensor_type == 'sensormatic':
        lat, lon= parse_geo_location_string(event.get('geo_position'))
        return {
            'sensor_type': {'S': sensor_type},
            'sensor_id': {'S': sensor_id},
            'timestamp': {'N': str(timestamp)},
            'location': {
                'M' : {
                    'lon': {'N': str(lon)},
                    'lat': {'N': str(lat)}
                }
            },
            'measurements': {
                'M' : {
                    'humidity': {'N': str(event.get('humidity')*100)},               # 0-1 --> 0-100%
                    'temperature': {'N': str(event.get('temperature') - 273.15)}     # °K --> °C
                }
            }
        }
    elif sensor_type == 'MQTT-Master':
        return {
            'sensor_type': {'S': sensor_type},
            'sensor_id': {'S': sensor_id},
            'timestamp': {'N': str(timestamp)},
            'location': {
                'M' : {
                    'lon': {'N': str(event.get('location').get('longitude'))},
                    'lat': {'N': str(event.get('location').get('latitude'))}
                }
            },
            'measurements': {
                'M' : {
                    'soil_moisture': {'N': str(event.get('soil_moisture')*100)},    # 0-1 --> 0-100%
                    'temperature': {'N': str(event.get('temperature'))}    # °C
                }
            }
        }
    else:
        raise ValueError('Unknown sensor type')


def lambda_handler(event, context):
    """
    Handles incoming events from AWS IoT Core, logs the data, and saves it into DynamoDB.
    """
    try:
        # Log the event data
        print("Received event:", json.dumps(event, indent=2))
        
        normalized_data= normalize_sensor_data(event)

        # Save data into DynamoDB
        dynamodb.put_item(
            TableName = TABLE_NAME,
            Item = normalized_data
        )
        
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
