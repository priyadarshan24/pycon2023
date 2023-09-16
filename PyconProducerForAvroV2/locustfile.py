from uuid import uuid4

import io
import avro
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from locust import HttpUser, task, constant, TaskSet
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

print("Hello, world!")

# Define your Avro schema
avro_schema_str = """
{
  "type": "record",
  "name": "example",
  "fields": [
    {"name": "name", "type": "string"},
    {"name": "age", "type": "int"},
    {"name": "dob", "type": "string"},
    {"name": "address", "type": "string"}
  ]
}
"""

# Create an AvroSchema object
# avro_schema = avro.loads(avro_schema_str)
avro_schema = avro.schema.parse(avro_schema_str)

def user_to_dict(message, ctx):
    return dict(name=message.name,
                message=message.age,
                dob=message.dob,
                address=message.address
                )


class KafkaProducerAvroUser(HttpUser):

    @task
    def produce(self):
        print("Inside run for producer for avro")
        # Define an Avro message using your schema
        avro_message = {
            "name": "John Doe",
            "age": 30,
            "dob": "1990-01-01",
            "address": "123 Main St"
        }

        producer = Producer({
            'bootstrap.servers': 'localhost:9092',
            'security.protocol': 'plaintext'
        })

        # Serialize the Avro data
        # Create a binary encoder
        writer = avro.io.DatumWriter(avro_schema)

        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)

        # Serialize the Avro data
        writer.write(avro_message, encoder)
        avro_bytes = bytes_writer.getvalue()
        print("Printing avroBytes", avro_bytes.__sizeof__())
        string_serializer = StringSerializer('utf_8')
        producer.produce('my-topic-avro', key=string_serializer(str(uuid4())), value=avro_bytes, callback=self.acked)
        producer.flush()

        # schema_registry_conf = {'url': 'http://localhost:8081'}
        # schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        #
        # avro_serializer = AvroSerializer(schema_registry_client,
        #                                  avro_schema_str,
        #                                  user_to_dict)

        # Create a Kafka Producer configuration
        # producer_config = {
        #     'bootstrap.servers': 'localhost:9092',
        #     'value.serializer': avro_serializer
        # }
        # producer = AvroProducer({
        #
        #     'bootstrap.servers': 'localhost:9092',
        #     'security.protocol': 'plaintext'
        # })
        #
        # string_serializer = StringSerializer('utf_8')
        #
        # producer.produce(topic="my-topic-avro",
        #                  key=string_serializer(str(uuid4())),
        #                  value=avro_serializer(avro_message, SerializationContext("my-topic-avro", MessageField.VALUE)),
        #                  on_delivery=self.acked)

        # producer.produce(topic="my-topic-avro", key="key", value=avro_message, callback=self.acked)
        producer.flush()

    def acked(self, error, msg):
        if error is not None:
            print("#Failed to deliver message: %s: %s" % (str(msg), str(self)))
        else:
            print("AvroMessage", msg.value())

# class KafkaConsumerTask(locust.TaskSet):
#
#     def __init__(self, parent: "User"):
#         print("Inside init for Consumer")
#         super().__init__(parent)
#         self.consumer = None
#
#     def on_start(self):
#         print("Inside on_start for Consumer")
#         self.consumer = Consumer({
#             'bootstrap_servers': 'localhost:9092',
#             'group_id': 'my-group',
#         })

#     def on_stop(self):
#         self.consumer.close()
#
#     @task
#     def run(self):
#         print("Inside run for consumer")
#         message = self.consumer.poll(timeout=1)
#         if message:
#             print(message.value)
#
#
# class KafkaUser(locust.HttpUser):
#     print("Inside producer")
#     tasks = {KafkaProducerTask: 1}
#
#
# class KafkaConsumerUser(locust.HttpUser):
#     print("Inside consumer")
#     tasks = {KafkaConsumerTask: 1}
