import time
import numpy as np
import iot_core as ic
from datetime import datetime
from sensor import create_sensors_from_data_file

# MQTT Broker Configuration
BROKER = "a20x14oot4bls2-ats.iot.eu-north-1.amazonaws.com"  # Replace with your MQTT broker address
PORT = 8883                  # Replace with your MQTT broker port (default is 1883)
TOPIC = "sdk/test/python"    # Replace with your desired topic
ROOT_CERT_FILE = "./AmazonRootCA1.pem"      # Root certificate authority, comes from AWS with a long, long name
CERT_FILE = "./certs/Vanek_Laptop.cert.pem"
KEY_FILE = "./certs/Vanek_Laptop.private.key"
CLIENT_ID = "basicPubSub"

# Sinusoidal Data Configuration
AMPLITUDE = 70.0              # Amplitude of the sine wave
FREQUENCY = 1.0              # Frequency of the sine wave (Hz)
SAMPLE_RATE_PER_SENSOR = 1/120    # Number of samples per second per sensor



def main():
    timestamps, sensors= create_sensors_from_data_file("./data/INCA analysis - large domain Datensatz_20250101T0000_20250103T2300.json")

    ic.connect_to_iot_core(BROKER, PORT, ROOT_CERT_FILE, CERT_FILE, KEY_FILE, CLIENT_ID)

    index= -1
    for timestamp in timestamps:
        index+= 1

        for sensor in sensors:
            payload = sensor.get_data_by_index(timestamp, index)
            print("Publishing message {} to topic '{}': {}".format( index, TOPIC, payload))
            
            ic.publish_to_iot_core( TOPIC, payload )
            
            wait_time= 1 / (SAMPLE_RATE_PER_SENSOR * len(sensors))
            time.sleep(wait_time)  # Maintain the sample rate

        
    ic.disconnect_from_iot_core()


if __name__ == '__main__':
    main()
