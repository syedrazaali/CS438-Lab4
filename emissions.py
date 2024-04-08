import paho.mqtt.client as mqtt
import ssl
import os

# Choose one of the created things' certificates and keys
thing_name = "SelfDrivingCar2"
certificates_base_directory = '/Users/raza/Desktop/certificates'
thing_directory = os.path.join(certificates_base_directory, thing_name)

cert_path = os.path.join(thing_directory, f'{thing_name}.certificate.pem')
key_path = os.path.join(thing_directory, f'{thing_name}.private.key')
ca_path = "/Users/raza/Desktop/certificates/root-CA.crt"

# Your AWS IoT Core endpoint
mqtt_endpoint = "a3k1k7t0ebtho9-ats.iot.us-east-1.amazonaws.com"
mqtt_topic = "vehicle/emissions/result/+"

def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {str(rc)}")
    client.subscribe(mqtt_topic)

def on_message(client, userdata, msg):
    print(f"Topic: {msg.topic} | Message: {str(msg.payload.decode('utf-8'))}")

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.tls_set(ca_certs=ca_path, certfile=cert_path, keyfile=key_path, tls_version=ssl.PROTOCOL_TLSv1_2)
client.connect(mqtt_endpoint, 8883, 60)

client.loop_forever()
