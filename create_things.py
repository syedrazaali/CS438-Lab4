import boto3
import os

# AWS IoT client initialization
thingClient = boto3.client('iot', region_name='us-east-1')

# Base directory where certificates will be stored
certificates_base_directory = '/Users/raza/Desktop/certificates'

# Policy and Thing Group configurations
defaultPolicyName = 'SelfDrivingCarPolicy'
thingGroupName = 'Cars'
numThings = 10  # Adjusted to create 5 Things

def create_thing(thingName):
    thingResponse = thingClient.create_thing(thingName=thingName)
    print(f"Created Thing: {thingName}")
    return thingResponse['thingArn']

def create_certificate(thingName):
    certResponse = thingClient.create_keys_and_certificate(setAsActive=True)
    print(f"Created certificate for: {thingName}")
    return certResponse['certificateArn'], certResponse['certificatePem'], certResponse['keyPair']

def save_keys_and_certificate(certificateArn, certificatePem, keyPair, thingName):
    directory = os.path.join(certificates_base_directory, thingName)
    os.makedirs(directory, exist_ok=True)
    # Saving certificates and keys
    with open(os.path.join(directory, f'{thingName}.certificate.pem'), 'w') as cert_file:
        cert_file.write(certificatePem)
    with open(os.path.join(directory, f'{thingName}.private.key'), 'w') as priv_file:
        priv_file.write(keyPair['PrivateKey'])
    with open(os.path.join(directory, f'{thingName}.public.key'), 'w') as pub_file:
        pub_file.write(keyPair['PublicKey'])
    print(f"Saved certificate and keys for: {thingName}")

def attach_policy_certificate(certificateArn):
    thingClient.attach_policy(policyName=defaultPolicyName, target=certificateArn)

def attach_thing_principal(thingName, certificateArn):
    thingClient.attach_thing_principal(thingName=thingName, principal=certificateArn)

def add_thing_to_group(thingArn):
    thingClient.add_thing_to_thing_group(thingGroupName=thingGroupName, thingArn=thingArn)

def get_highest_thing_number():
    highest_num = -1  # Changed to start from -1
    paginator = thingClient.get_paginator('list_things')
    for page in paginator.paginate():
        for thing in page['things']:
            if thing['thingName'].startswith("SelfDrivingCar"):
                num_part = thing['thingName'][len("SelfDrivingCar"):]
                if num_part.isdigit() and int(num_part) > highest_num:
                    highest_num = int(num_part)
    return highest_num

def create_things(num):
    start_num = get_highest_thing_number() + 1
    for i in range(start_num, start_num + num):
        thingName = f"SelfDrivingCar{i}"
        thingArn = create_thing(thingName)
        certificateArn, certificatePem, keyPair = create_certificate(thingName)
        save_keys_and_certificate(certificateArn, certificatePem, keyPair, thingName)
        attach_policy_certificate(certificateArn)
        attach_thing_principal(thingName, certificateArn)
        add_thing_to_group(thingArn)
        print(f"Successfully added {thingName} to group: {thingGroupName}")

if __name__ == "__main__":
    create_things(numThings)
