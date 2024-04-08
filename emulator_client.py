import json
import time
import pandas as pd
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
from AWSIoTPythonSDK.exception.AWSIoTExceptions import connectError

# Modify these parameters accordingly
device_st = 2
device_end = 4  # Make sure this is inclusive of the last device you want to process
data_path = "/Users/raza/Desktop/data2/vehicle{}.csv"
certificate_formatter = "/Users/raza/Desktop/certificates/SelfDrivingCar{}/SelfDrivingCar{}.certificate.pem"
key_formatter = "/Users/raza/Desktop/certificates/SelfDrivingCar{}/SelfDrivingCar{}.private.key"
endpoint = "a3k1k7t0ebtho9-ats.iot.us-east-1.amazonaws.com"
root_ca = "/Users/raza/Desktop/certificates/root-CA.crt"

def init_mqtt_client(device_id, cert, key):
    client = AWSIoTMQTTClient(str(device_id))
    client.configureEndpoint(endpoint, 8883)
    client.configureCredentials(root_ca, key, cert)
    client.configureOfflinePublishQueueing(-1)
    client.configureDrainingFrequency(2)
    client.configureConnectDisconnectTimeout(10)
    client.configureMQTTOperationTimeout(5)
    return client

def publish_emission_data(client, device_id, data):
    topic = f"device/data/{device_id}"
    for index, row in data.iterrows():
        payload = json.dumps({"device_id": device_id, "CO2": row['vehicle_CO2']})
        print(f"Publishing to topic {topic}: {payload}")
        client.publishAsync(topic, payload, 0)
        time.sleep(1)  # Throttle publishing to avoid hitting limits

print("Initializing MQTT Clients and Publishing Data...")
for device_id in range(device_st, device_end + 1):  # Adjusted to include device_end in the range
    cert = certificate_formatter.format(device_id, device_id)
    key = key_formatter.format(device_id, device_id)
    client = init_mqtt_client(device_id, cert, key)
    
    try:
        client.connect()
        print(f"Successfully connected device {device_id}")
        try:
            # Load only the first 20 rows of the CSV file
            data = pd.read_csv(data_path.format(device_id)).head(20)
            publish_emission_data(client, device_id, data)
        except FileNotFoundError as e:
            print(f"File not found for device_id {device_id}: {e}")
    except connectError as e:
        print(f"Failed to connect device {device_id}: {str(e)}")
    except Exception as e:
        print(f"An unexpected error occurred for device {device_id}: {str(e)}")
    finally:
        client.disconnect()
        time.sleep(2)  # Throttle disconnection to avoid hitting limits
