import json
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

# Kafka and Schema Registry configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC_NAME = "user-topic"

# Load Avro schema from a .avsc file
def load_schema_from_file(schema_file_path):
    with open(schema_file_path, 'r') as file:
        return json.load(file)

schema_file_path = "user.avsc"
user_schema = load_schema_from_file(schema_file_path)
user_schema = json.dumps(user_schema)

# Callback function for delivery report
def delivery_report(err, msg):
    if err:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Record successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

# Initialize Schema Registry client
schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Initialize AvroSerializer
avro_serializer = AvroSerializer(schema_registry_client, user_schema)

# Initialize Producer
producer_conf = {'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS}
producer = Producer(producer_conf)

# Produce a message
user = {"id": 1, "name": "John Doe", "email": "john.doe@example.com"}
key = "user-1"

try:
    # Serialize the value using AvroSerializer
    value = avro_serializer(user, SerializationContext(TOPIC_NAME, MessageField.VALUE))
    
    # Produce the message
    producer.produce(
        topic=TOPIC_NAME,
        key=key,
        value=value,
        on_delivery=delivery_report
    )
    producer.flush()
except Exception as e:
    print(f"Failed to produce message: {e}")