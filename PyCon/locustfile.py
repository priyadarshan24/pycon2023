from uuid import uuid4

from confluent_kafka.serialization import StringSerializer
from locust import HttpUser, task, constant, TaskSet, events
import json
from confluent_kafka import Producer, Consumer


class KafkaProducerUser(HttpUser):

    @task
    def produce(self):
        # print("Inside run for producer")
        message = json.dumps({
            "name": "John Doe",
            "age": 30,
            "dob": "1990-01-01",
            "address": "123 Main St"
        })

        producer = Producer({
            'bootstrap.servers': 'localhost:9092',
            'security.protocol': 'plaintext'
        })

        # print("Printing message in Bytes::", message.__sizeof__())
        string_serializer = StringSerializer('utf_8')
        producer.produce(topic="my-topic", key=string_serializer(str(uuid4())), value=message, callback=self.acked)
        producer.flush()

    def acked(self, error, msg):
        if error is not None:
            print("#Failed to deliver message: %s: %s" % (str(msg), str(self)))
        else:
            # print("Printing message##", msg.value(), msg.latency())
            events.request.fire(request_type="ENQUEUE",
                                name="my-topic",
                                response_time=msg.latency(),
                                response_length=msg.__sizeof__())
