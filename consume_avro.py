import json
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

# Kafka and Schema Registry configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC_NAME = "user-topic"
GROUP_ID = "avro-consumer-group"

# Load Avro schema from a .avsc file
def load_schema_from_file(schema_file_path):
    with open(schema_file_path, 'r') as file:
        return json.load(file)

schema_file_path = "user.avsc"
user_schema = load_schema_from_file(schema_file_path)
user_schema = json.dumps(user_schema)

# Callback function to handle consumed messages
def process_message(key, value):
    print(f"Consumed message:")
    print(f"  Key: {key}")
    print(f"  Value: {value}")

# Initialize Schema Registry client
schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Initialize AvroDeserializer
avro_deserializer = AvroDeserializer(schema_registry_client, user_schema)

# Configure Consumer
consumer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest',  # Start from the beginning of the topic if no offset is committed
    'enable.auto.commit': True
}

consumer = Consumer(consumer_conf)

# Subscribe to the topic
consumer.subscribe([TOPIC_NAME])

# Poll for messages
try:
    print(f"Consuming messages from topic '{TOPIC_NAME}'...")
    while True:
        msg = consumer.poll(1.0)  # Poll for messages with a 1-second timeout
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        # Deserialize the value
        try:
            value = avro_deserializer(msg.value(), SerializationContext(TOPIC_NAME, MessageField.VALUE))
            key = msg.key().decode('utf-8') if msg.key() else None
            process_message(key, value)
        except Exception as e:
            print(f"Failed to deserialize message: {e}")
except KeyboardInterrupt:
    print("\nExiting...")
finally:
    consumer.close()