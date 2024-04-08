import json
import pandas as pd
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
from AWSIoTPythonSDK.exception.AWSIoTExceptions import connectError

# Modify these parameters accordingly
device_start = 0
device_end = 4  # Make sure it processes devices 0 through 4
data_path_template = "/Users/raza/Desktop/data2/vehicle{}.csv"
certificate_base_path = "/Users/raza/Desktop/certificates/SelfDrivingCar{}"
endpoint = "a3k1k7t0ebtho9-ats.iot.us-east-1.amazonaws.com"
root_ca = "/Users/raza/Desktop/certificates/root-CA.crt"

def init_mqtt_client(device_id):
    cert_path = f"{certificate_base_path.format(device_id)}/SelfDrivingCar{device_id}.certificate.pem"
    key_path = f"{certificate_base_path.format(device_id)}/SelfDrivingCar{device_id}.private.key"
    
    client = AWSIoTMQTTClient(str(device_id))
    client.configureEndpoint(endpoint, 8883)
    client.configureCredentials(root_ca, key_path, cert_path)
    client.configureOfflinePublishQueueing(-1)
    client.configureDrainingFrequency(2)
    client.configureConnectDisconnectTimeout(10)
    client.configureMQTTOperationTimeout(5)
    
    return client

def publish_final_max_co2(client, device_id, max_co2):
    topic = f"device/data/{device_id}"
    payload = json.dumps({"device_id": device_id, "CO2": max_co2, "is_final": True})
    print(f"Publishing final max CO2 to topic {topic}: {payload}")
    client.publishAsync(topic, payload, 0)

if __name__ == "__main__":
    for device_id in range(device_start, device_end + 1):
        try:
            client = init_mqtt_client(device_id)
            client.connect()
            print(f"Successfully connected device {device_id}")
            
            data_path = data_path_template.format(device_id)
            data = pd.read_csv(data_path)
            max_co2 = data['vehicle_CO2'].max()  # Find the maximum CO2 value in the dataset
            publish_final_max_co2(client, device_id, max_co2)
        except FileNotFoundError as e:
            print(f"File not found for device_id {device_id}: {e}")
        except connectError as e:
            print(f"Failed to connect device {device_id}: {str(e)}")
        except Exception as e:
            print(f"An unexpected error occurred for device {device_id}: {str(e)}")
        finally:
            client.disconnect()
            print(f"Disconnected device {device_id}")
