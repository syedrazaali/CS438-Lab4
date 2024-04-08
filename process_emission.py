import json
import logging
import greengrasssdk

# Initialize the logger
logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

# Create a Greengrass SDK client
client = greengrasssdk.client('iot-data')

def lambda_handler(event, context):
    try:
        # Parse the incoming message payload
        co2_emission = json.loads(event['message'])['CO2']  # Adjust based on actual event structure

        # Log the received CO2 emission value for debugging
        logger.info(f'Received CO2 emission: {co2_emission}')

        # For simplicity, assuming co2_emission directly contains the CO2 value
        # In real-world applications, you might need to aggregate or process multiple values

        # Publish the result back to an MQTT topic
        result_topic = 'vehicle/emissions/result'
        payload = json.dumps({'max_CO2': co2_emission})  # In this example, simply echoing back the received value
        client.publish(topic=result_topic, payload=payload)
        logger.info(f'Published CO2 emission to topic {result_topic}: {co2_emission}')

    except Exception as e:
        logger.error(f'Error processing emissions data: {str(e)}')
        # Publish the error message for debugging
        error_topic = 'vehicle/emissions/error'
        client.publish(topic=error_topic, payload=json.dumps({'error': str(e)}))

    return {'status': 'complete'}
