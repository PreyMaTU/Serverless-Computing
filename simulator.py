import time
import iot_core as ic
from sensor import create_sensors_from_data_file
from argparse import ArgumentParser

# MQTT Broker Configuration
BROKER = "azie6gjpxjyi1-ats.iot.eu-central-1.amazonaws.com"  # Replace with your MQTT broker address
PORT = 8883  # Replace with your MQTT broker port (default is 1883)
TOPIC = "sdk/test/python"  # Replace with your desired topic
ROOT_CERT_FILE = "./AmazonRootCA1.pem"  # Root certificate authority, comes from AWS with a long, long name
CERT_FILE = "./certs/slc_test_simulator.cert.pem"
KEY_FILE = "./certs/slc_test_simulator.private.key"
CLIENT_ID = "basicPubSub"

# Sensor Data Configuration
SENSOR_ID_PREFIX = ''
SAMPLE_RATE_PER_SENSOR = 1 / 120  # Number of samples per second per sensor


def configure():
    parser = ArgumentParser(prog='IoT Core Simulator', description='Simulates multiple IoT sensors')
    parser.add_argument('-c', '--count', type=int, default=float('inf'))

    return parser.parse_args()


def send_loop(timestamps, sensors, count):
    index = -1
    msg_id = -1
    for timestamp in timestamps:
        index += 1

        for sensor in sensors:
            msg_id += 1

            if msg_id + 1 > count:
                print('Done sending {} messages', count)
                return

            payload = sensor.get_data_by_index(timestamp, index)
            print("Publishing message {} (row {}) to topic '{}': {}".format(msg_id, index, TOPIC, payload))

            ic.publish_to_iot_core(TOPIC, payload)

            wait_time = 1 / (SAMPLE_RATE_PER_SENSOR * len(sensors))
            time.sleep(wait_time)  # Maintain the sample rate


def main():
    config = configure()

    timestamps, sensors = create_sensors_from_data_file(
        "./data/INCA analysis - large domain Datensatz_20250101T0000_20250103T2300.json", SENSOR_ID_PREFIX)

    ic.connect_to_iot_core(BROKER, PORT, ROOT_CERT_FILE, CERT_FILE, KEY_FILE, CLIENT_ID)

    send_loop(timestamps, sensors, config.count)

    ic.disconnect_from_iot_core()


if __name__ == '__main__':
    main()
