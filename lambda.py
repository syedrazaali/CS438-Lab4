import boto3
import json
import logging
from decimal import Decimal

# Initialize logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('VehicleEmissions')  # Make sure this is the correct table name
iot_client = boto3.client('iot-data')

def lambda_handler(event, context):
    # Ensure device_id is treated as a string
    device_id = str(event['device_id'])
    co2_emission = Decimal(str(event['CO2']))  # Convert CO2 emission to Decimal
    
    try:
        # Retrieve the current max CO2 value from DynamoDB
        response = table.get_item(Key={'device_id': device_id})
        current_max = response.get('Item', {}).get('maxCO2', Decimal('0'))
        
        # Compare and update if the new value is higher
        if co2_emission > current_max:
            table.put_item(Item={'device_id': device_id, 'maxCO2': co2_emission})
            current_max = co2_emission
        
        # Publish the current max back to the vehicle
        iot_client.publish(
            topic=f'vehicle/maxCO2/{device_id}',
            payload=json.dumps({'device_id': device_id, 'max_CO2': str(current_max)})
        )
    except Exception as e:
        logger.error(f"Error processing emissions data: {str(e)}")
