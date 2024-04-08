# Import SDK packages
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time
import pandas as pd
import json  # This import was missing

# Modify the following parameters
# Starting and end index - adjusted to the available data
device_st = 2
device_end = 5  # Updated to reflect the actual data available

# Path to the dataset - This is correctly set
data_path = "/Users/raza/Desktop/data2/vehicle{}.csv"

# Path to your certificates - Adjusted to reflect the structured naming convention
certificate_formatter = "/Users/raza/Desktop/certificates/SelfDrivingCar{}/SelfDrivingCar{}.certificate.pem"
key_formatter = "/Users/raza/Desktop/certificates/SelfDrivingCar{}/SelfDrivingCar{}.private.key"

class MQTTClient:
    def __init__(self, device_id, cert, key):
        # For certificate based connection
        self.device_id = str(device_id)
        self.client = AWSIoTMQTTClient(self.device_id)
        # Broker address
        self.client.configureEndpoint("a3k1k7t0ebtho9-ats.iot.us-east-1.amazonaws.com", 8883)
        self.client.configureCredentials("/Users/raza/Desktop/certificates/root-CA.crt", key, cert)
        self.client.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
        self.client.configureDrainingFrequency(2)  # Draining: 2 Hz
        self.client.configureConnectDisconnectTimeout(10)  # 10 sec
        self.client.configureMQTTOperationTimeout(5)  # 5 sec

    def customOnMessage(self, message):
        print("Client {} received payload {} from topic {}".format(self.device_id, message.payload, message.topic))

    # Define the customPubackCallback method
    def customPubackCallback(self, mid):
        print(f"Publish acknowledgment received for message ID: {mid}")

    def publish(self, payload="payload"):
        # Example topic
        topic = "device/data/{}".format(self.device_id)
        print(f"Publishing to topic {topic}: {payload}")
        # Note: publishAsync's ackCallback parameter should match a defined method
        self.client.publishAsync(topic, payload, 0, ackCallback=self.customPubackCallback)

print("Loading vehicle data...")
data = []
for i in range(device_st, device_end):
    try:
        a = pd.read_csv(data_path.format(i))
        data.append(a)
    except FileNotFoundError:
        print(f"File not found: {data_path.format(i)}")

print("Initializing MQTTClients...")
clients = []
for device_id in range(device_st, device_end):
    cert = certificate_formatter.format(device_id, device_id)
    key = key_formatter.format(device_id, device_id)
    client = MQTTClient(device_id, cert, key)
    client.client.connect()
    clients.append(client)

while True:
    print("Send now? (s) | Disconnect all? (d)")
    x = input().strip().lower()
    if x == "s":
        for i, c in enumerate(clients):
            payload = json.dumps({"device_id": i, "data": "Example payload"})  # Customize your payload
            c.publish(payload)
    elif x == "d":
        for c in clients:
            c.client.disconnect()
        print("All devices disconnected")
        break  # Exit the loop to end the script
    else:
        print("Wrong key pressed")
    time.sleep(3)
