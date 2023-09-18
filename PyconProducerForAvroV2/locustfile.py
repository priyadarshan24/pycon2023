from uuid import uuid4

import io
import avro.io
import avro.schema
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer
# from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from locust import HttpUser, task, constant, TaskSet, events

# Define your Avro schema
avro_schema_str = """
{
  "type": "record",
  "name": "example",
  "fields": [
    {"name": "name", "type": "string"},
    {"name": "age", "type": "int"},
    {"name": "dob", "type": "string"},
    {"name": "address", "type": "string"},
    {"name": "profile_summary", "type": "string"},
    {"name": "profile_details", "type": "string"}
  ]
}
"""

# Create an AvroSchema object
# avro_schema = avro.loads(avro_schema_str)
avro_schema = avro.schema.parse(avro_schema_str)
writer = avro.io.DatumWriter(avro_schema)

producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'security.protocol': 'plaintext'
})


class Person:
    def __init__(self, name, age, dob, address, profile_summary, profile_details):
        self.name = name
        self.age = age
        self.dob = dob
        self.address = address
        self.profile_summary = profile_summary
        self.profile_details = profile_details


class KafkaProducerAvroUser(HttpUser):

    # Serialize the Avro data
    # Create a binary encoder

    @task
    def produce(self):

        person = Person("John Doe", 30, "1990-01-01",
                        "123 Main St,Main St,Main St,Main St,Main St,California, Bay Area, at MyHome",
                        "I am enthusiastic Software Engineer",
                        "I want to learn and implement the fundamental of software engineering with best of my knowledge so that I keep excelling in ho I build software systems")
        avro_message = person.__dict__

        # Serialize the Avro data
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer.write(avro_message, encoder)
        avro_bytes = bytes_writer.getvalue()
        # print("Printing avroBytes##", len(avro_bytes), avro_bytes)
        string_serializer = StringSerializer('utf_8')

        producer.produce('my-topic-avro', key=string_serializer(str(uuid4())), value=avro_bytes, callback=self.acked)
        producer.flush()

    def acked(self, error, msg):
        if error is not None:
            print("#Failed to deliver message: %s: %s" % (str(msg), str(self)))
        else:
            # print("AvroMessage", msg.value(), msg.latency())
            # print("AvroMessage##", msg.value())
            # print("MessageSize##", msg.__sizeof__())
            events.request.fire(request_type="ENQUEUE",
                                name="my-topic-avro",
                                response_time=msg.latency() * 1000,
                                response_length=len(msg.value()))
