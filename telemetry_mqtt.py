import os
import random
import time
from azure.iot.device import IoTHubDeviceClient, Message
from azure.iot.hub import IoTHubRegistryManager
import json

# Connection string for IoT Hub with management permissions
IOTHUB_CONNECTION_STRING = "HostName=iothubdevuae.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=TgNmv49DIduLOsnHU7ccaESSOcXnpKu9UAIoTOMlm0s="

# Device ID for the new device
DEVICE_ID = "MyDevice"

# Define telemetry message template
TEMPERATURE = 20.0
HUMIDITY = 60
MSG_TXT = '{{"temperature": {temperature},"humidity": {humidity}}}'

def create_device(iothub_connection_string, device_id):
    registry_manager = IoTHubRegistryManager(iothub_connection_string)
    
    try:
        # Create a new device or retrieve an existing one
        device = registry_manager.create_device_with_sas(device_id, None, None, 'enabled')
        print(f"Device created: {device.device_id}")
        return device.device_id
    except Exception as e:
        print(f"Error creating device: {e}")
        return None

def get_device_connection_string(iothub_connection_string, device_id):
    registry_manager = IoTHubRegistryManager(iothub_connection_string)
    
    try:
        device = registry_manager.get_device(device_id)
        connection_string = f"HostName=iothubdevuae.azure-devices.net;DeviceId={device.device_id};SharedAccessKey={device.authentication.symmetric_key.primary_key}"
        return connection_string
    except Exception as e:
        print(f"Error fetching device connection string: {e}")
        return None

def run_telemetry_sample(client):
    print("IoT Hub device sending periodic messages")
    
    while True:
        temperature = TEMPERATURE + (random.random() * 15)
        humidity = HUMIDITY + (random.random() * 20)
        
        # Create a dictionary for the message payload
        msg_payload = {
            "temperature": temperature,
            "humidity": humidity
        }
        
        # Convert the dictionary to a JSON string
        msg_txt_formatted = json.dumps(msg_payload)
        message = Message(msg_txt_formatted)

        # Set content type and encoding explicitly
        message.content_type = "application/json"
        message.content_encoding = "utf-8"
        
        # Add custom application property
        message.custom_properties["temperatureAlert"] = "true" if temperature > 30 else "false"
        
        # Send the message
        print(f"Sending message: {msg_txt_formatted}")
        client.send_message(message)
        print("Message successfully sent")
        
        time.sleep(10)


def main():
    # Create device in IoT Hub
    device_id = create_device(IOTHUB_CONNECTION_STRING, DEVICE_ID)
    if device_id is None:
        print("Failed to create device. Exiting.")
        return
    
    # Get device connection string
    device_connection_string = get_device_connection_string(IOTHUB_CONNECTION_STRING, device_id)
    if device_connection_string is None:
        print("Failed to get device connection string. Exiting.")
        return

    print("IoT Hub Quickstart - Simulated device with MQTT")
    print("Press Ctrl-C to exit")
    
    # Create IoT Hub Device Client using MQTT protocol
    client = IoTHubDeviceClient.create_from_connection_string(device_connection_string)
    
    try:
        run_telemetry_sample(client)
    except KeyboardInterrupt:
        print("IoTHubClient sample stopped by user")
    finally:
        print("Shutting down IoTHubClient")
        client.shutdown()

if __name__ == '__main__':
    main()
