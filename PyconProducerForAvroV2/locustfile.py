from uuid import uuid4

import io
import avro.io
import avro.schema
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer
# from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from locust import HttpUser, task, constant, TaskSet, events

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

writer = avro.io.DatumWriter(avro_schema)
bytes_writer = io.BytesIO()
encoder = avro.io.BinaryEncoder(bytes_writer)


class KafkaProducerAvroUser(HttpUser):

    # Serialize the Avro data
    # Create a binary encoder

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
        writer.write(avro_message, encoder)
        avro_bytes = bytes_writer.getvalue()
        # print("Printing avroBytes", avro_bytes.__sizeof__())
        string_serializer = StringSerializer('utf_8')
        producer.produce('my-topic-avro', key=string_serializer(str(uuid4())), value=avro_bytes, callback=self.acked)
        producer.flush()

    def acked(self, error, msg):
        if error is not None:
            print("#Failed to deliver message: %s: %s" % (str(msg), str(self)))
        else:
            # print("AvroMessage", msg.value(), msg.latency())
            events.request.fire(request_type="ENQUEUE",
                                name="my-topic-avro",
                                response_time=msg.latency(),
                                response_length=msg.__sizeof__())

