import time
import iot_core as ic
from sensor import create_sensors_from_data_file
from argparse import ArgumentParser
from datetime import datetime, timezone

# MQTT Broker Configuration
BROKER = "a86hzqaw9f6v0-ats.iot.eu-north-1.amazonaws.com"  # Replace with your MQTT broker address
PORT = 8883  # Replace with your MQTT broker port (default is 1883)
TOPIC = "sdk/test/python"  # Replace with your desired topic
ROOT_CERT_FILE = "./AmazonRootCA1.pem"  # Root certificate authority, comes from AWS with a long, long name
CERT_FILE = "./certs/Simulator.cert.pem"
KEY_FILE = "./certs/Simulator.private.key"
CLIENT_ID = "basicPubSub"

# Sensor Data Configuration
SENSOR_ID_PREFIX = ""
SAMPLE_RATE_PER_SENSOR = 1 / 120  # Number of samples per second per sensor


def configure():
    parser = ArgumentParser(prog='IoT Core Simulator', description='Simulates multiple IoT sensors')
    parser.add_argument('-c', '--count', type=int, default=float('inf'))
    parser.add_argument('-s', '--silent', action='store_true')
    parser.add_argument('-t', '--time', type=str, default=None)

    return parser.parse_args()

def offset_timestamps( timestamps, offset_date ):
    # Nothing to offset
    if offset_date is None or len(timestamps) == 0:
        return timestamps
    
    # Offset to today
    if offset_date == 'now':
        offset_date= datetime.now(timezone.utc)

    # Offset to provided date
    else:
        offset_date= datetime.fromisoformat( offset_date )

    # Parse timestamps and compute offset
    timestamps= [ datetime.fromisoformat(ts) for ts in timestamps ]
    offset= offset_date - timestamps[0]

    print(f'Adding timestamp offset of {offset}: {timestamps[0]} -> {timestamps[0]+offset}')

    # Add the offset to all timestamps and convert them back into ISO strings
    return [ (ts+ offset).isoformat() for ts in timestamps ]

def send_loop(timestamps, sensors, count, silent):
    start_time = time.time()

    index = -1
    msg_id = -1
    for timestamp in timestamps:
        index += 1

        for sensor in sensors:
            msg_id += 1

            if msg_id + 1 > count:
                print(f"Done sending {count} messages")

                end_time = time.time()
                return msg_id, end_time - start_time

            payload = sensor.get_data_by_index(timestamp, index)
            if not silent:
                print(
                    f"Publishing message {msg_id} (row {index}) to topic '{TOPIC}': {payload}"
                )

            ic.publish_to_iot_core(TOPIC, payload)

            wait_time = 1 / (SAMPLE_RATE_PER_SENSOR * len(sensors))
            time.sleep(wait_time)  # Maintain the sample rate

    end_time = time.time()
    return msg_id + 1, end_time - start_time


def main():
    config = configure()

    timestamps, sensors = create_sensors_from_data_file(
        "./data/INCA analysis - large domain Datensatz_20250101T0000_20250103T2300.json",
        SENSOR_ID_PREFIX,
    )

    timestamps= offset_timestamps(timestamps, config.time)

    ic.connect_to_iot_core(BROKER, PORT, ROOT_CERT_FILE, CERT_FILE, KEY_FILE, CLIENT_ID)

    message_count, runtime = send_loop(timestamps, sensors, config.count, config.silent)

    ic.disconnect_from_iot_core()

    print(
        f"Sent {message_count} messages in {round(runtime, 2)}s ({round(60* message_count/runtime)} msg/min)"
    )


if __name__ == "__main__":
    main()
