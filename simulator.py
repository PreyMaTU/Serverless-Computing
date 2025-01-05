import time
import numpy as np
import iot_core as ic
from datetime import datetime

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
SAMPLE_RATE = 1              # Number of samples per second
DURATION = 5* 60             # Duration of data transmission (seconds)


# Function to generate sinusoidal data
def generate_sinusoidal_data(amplitude, frequency, sample_rate, duration):
    t = np.linspace(0, duration, int(sample_rate * duration), endpoint=False)
    return t, amplitude * (1 + np.sin(2 * np.pi * frequency * t)) / 2


def main():
    # Generate sinusoidal data
    t, data = generate_sinusoidal_data(AMPLITUDE, FREQUENCY, SAMPLE_RATE, DURATION)

    ic.connect_to_iot_core(BROKER, PORT, ROOT_CERT_FILE, CERT_FILE, KEY_FILE, CLIENT_ID)


    for i, value in enumerate(data):
        # payload = {"time": t[i], "value": value}
        payload = {
            'timestamp': datetime.now().isoformat(),
            'location': '48.225861N/16.409139E',
            'humidity': float(value)
        }
        print("Publishing message {} to topic '{}': {}".format( i, TOPIC, payload))
        
        ic.publish_to_iot_core( TOPIC, payload )

        time.sleep(1 / SAMPLE_RATE)  # Maintain the sample rate
        
    ic.disconnect_from_iot_core()


if __name__ == '__main__':
    main()
