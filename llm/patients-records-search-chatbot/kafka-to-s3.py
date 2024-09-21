from confluent_kafka import Producer
import os
import json
import csv
import requests
import confluent_kafka.admin

conf        = {'bootstrap.servers': '******:9092'}

kafka_admin = confluent_kafka.admin.AdminClient(conf)

new_topic   = confluent_kafka.admin.NewTopic('topic_name', 3, 3)

kafka_admin.create_topics([new_topic,]) 

with open('patients-data.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    data = [row for row in reader]

with open('patients-data.json', 'w') as jsonfile:
    json.dump(data, jsonfile)

# Kafka producer configuration
kafka_config = {
    'bootstrap.servers': '******:9092'
}

producer = Producer(kafka_config)
topic = 'patients-dataset'

def delivery_report(err, msg):
    """ Callback for delivery reports """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def send_file_to_kafka(file_path):
    with open(file_path, 'rb') as file:
        file_content = file.read()
        # Send the file content to Kafka
        producer.produce(topic, file_content, callback=delivery_report)
        producer.flush()

if __name__ == "__main__":
    file_path = 'patients-data.json'
    send_file_to_kafka(file_path)

# Kafka Connect REST API endpoint
KAFKA_CONNECT_URL = 'http://******:8083/connectors'

# S3 Sink Connector configuration
connector_config = {
    "name": "s3-sink-from-kafka",  
    "config": {
        "connector.class": "io.confluent.connect.s3.S3SinkConnector",
        "tasks.max": "1",
        "topics": "patients-dataset",  
        "s3.bucket.name": "****",
        "s3.region": "****", 
        "aws.access.key.id": "****", 
        "aws.secret.access.key": "*******",
        "s3.part.size": "5242880",
        "flush.size": "3",
        "storage.class": "io.confluent.connect.s3.storage.S3Storage",
        "format.class": "io.confluent.connect.s3.format.json.JsonFormat", 
        "schema.generator.class": "io.confluent.connect.storage.hive.schema.DefaultSchemaGenerator",
        "partitioner.class": "io.confluent.connect.storage.partitioner.DefaultPartitioner",
        "s3.compression.type": "gzip",
        "format.class": "io.confluent.connect.s3.format.bytearray.ByteArrayFormat", 
        "key.converter": "org.apache.kafka.connect.storage.StringConverter", 
        "value.converter": "org.apache.kafka.connect.converters.ByteArrayConverter", 
    }
}

def deploy_connector(config):
    headers = {'Content-Type': 'application/json'}
    response = requests.post(KAFKA_CONNECT_URL, headers=headers, data=json.dumps(config))
    
    if response.status_code == 201:
        print(f"Connector {config['name']} deployed successfully.")
    elif response.status_code == 409:
        print(f"Connector {config['name']} already exists. Updating configuration...")
        update_connector(config['name'], config)
    else:
        print(f"Failed to deploy connector: {response.text}")

# Function to update an existing connector configuration
def update_connector(connector_name, config):
    update_url = f"{KAFKA_CONNECT_URL}/{connector_name}/config"
    headers = {'Content-Type': 'application/json'}
    response = requests.put(update_url, headers=headers, data=json.dumps(config['config']))
    
    if response.status_code == 200:
        print(f"Connector {connector_name} updated successfully.")
    else:
        print(f"Failed to update connector: {response.text}")

# Main function to deploy or update Kafka Connect S3 Sink
if __name__ == '__main__':
    deploy_connector(connector_config)
