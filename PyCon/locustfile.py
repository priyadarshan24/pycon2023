from uuid import uuid4

from confluent_kafka.serialization import StringSerializer
from locust import HttpUser, task, constant, TaskSet, events
import json
from confluent_kafka import Producer, Consumer


class Person:
    def __init__(self, name, age, dob, address, profile_summary,profile_details):
        self.name = name
        self.age = age
        self.dob = dob
        self.address = address
        self.profile_summary = profile_summary
        self.profile_details = profile_details


class KafkaProducerUser(HttpUser):

    @task
    def produce(self):
        person = Person("John Doe", 30, "1990-01-01", "123 Main St,Main St,Main St,Main St,Main St,California, Bay Area, at MyHome", "I am enthusiastic Software Engineer", "I want to learn and implement the fundamental of software engineering with best of my knowledge so that I keep excelling in ho I build software systems")
        # print("Inside run for producer")
        # message = json.dumps({
        #     "name": "John Doe",
        #     "age": 30,
        #     "dob": "1990-01-01",
        #     "address": "123 Main St"
        # })

        message = json.dumps(person.__dict__)

        producer = Producer({
            'bootstrap.servers': 'localhost:9092',
            'security.protocol': 'plaintext'
        })

        print("Printing message in Bytes@@@@::", len(message))
        string_serializer = StringSerializer('utf_8')
        producer.produce(topic="my-topic", key=string_serializer(str(uuid4())), value=message, callback=self.acked)
        producer.flush()

    def acked(self, error, msg):
        if error is not None:
            print("#Failed to deliver message: %s: %s" % (str(msg), str(self)))
        else:
            print("Printing message##@@", msg.value(), len(msg.value()))
            events.request.fire(request_type="ENQUEUE",
                                name="my-topic",
                                response_time=msg.latency()*1000,
                                response_length=len(msg.value()))
