
import random
import json

SENSOR_TYPES= [
  'IoT-2000', 'sensormatic', 'MQTT-Master'
]

class Sensor:
  def __init__(self, json_object, id_prefix= ''):
    self.longitude= json_object['geometry']['coordinates'][0]
    self.latitude= json_object['geometry']['coordinates'][1]

    self.humidity_data= json_object['properties']['parameters']['RH2M']['data']
    self.temperature_data= json_object['properties']['parameters']['T2M']['data']

    self.sensor_id= id_prefix + self.create_unique_id()
    self.sensor_type= self.select_random_sensor_type()

  def select_random_sensor_type( self ):
    return random.choice( SENSOR_TYPES )
  
  def geo_position_string( self ):
    return f'{self.latitude}N/{self.longitude}E'

  def create_unique_id( self ):
    hash_number= hash( self.geo_position_string() )
    hex_number= hex( hash_number )[3:]
    return f'sensor_{hex_number}'

  def get_data_by_index( self, timestamp, index ):
    humidity= self.humidity_data[index] if index < len(self.humidity_data) else -1
    temperature= self.temperature_data[index] if index < len(self.temperature_data) else -1

    return self.format_data( timestamp, humidity, temperature )
  
  def format_data( self, timestamp, humidity, temperature ):
    if self.sensor_type == 'IoT-2000':
      return {
        'sensor_type': self.sensor_type,
        'sensor_id': self.sensor_id,
        'timestamp': timestamp,
        'location': {
          'lon': self.longitude,
          'lat': self.latitude,
        },
        'humidity': humidity,         # 0-100%
        'temperature': temperature    # °C
      }
    elif self.sensor_type == 'sensormatic':
      return {
        'sensor_type': self.sensor_type,
        'sensor_id': self.sensor_id,
        'timestamp': timestamp,
        'geo_position': self.geo_position_string(),
        'humidity': humidity / 100,           # 0-1
        'temperature': temperature + 273.15   # °K
      }
    elif self.sensor_type == 'MQTT-Master':
      return {
        'sensor_type': self.sensor_type,
        'sensor_id': self.sensor_id,
        'timestamp': timestamp,
        'location': {
          'longitude': self.longitude,
          'latitude': self.latitude,
        },
        'soil_moisture': humidity / 100, # 0-1
        'temperature': temperature
      }
    else:
      raise ValueError('Unknown sensor type')


def create_sensors_from_data_file( file_path, id_prefix ):
  with open(file_path, 'r') as file:
    data = json.load(file)
    timestamps= data['timestamps']
    features= data['features']

    sensors= [ Sensor(feature, id_prefix) for feature in features ]

    return timestamps, sensors
