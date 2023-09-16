from locust import HttpUser, task, constant, TaskSet
import json
from confluent_kafka import Producer, Consumer
import random

print("Hello, world!")


class KafkaConsumerUser(HttpUser):

    @task
    def consumer(self):
        print("Inside run for consumer")
        consumer = Consumer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': '123',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': 'true',
        })

        topic = 'my-topic'  # Replace with the topic you want to consume from
        consumer.subscribe([topic])

        message = consumer.poll(timeout=40)
        print("message", message.value())
        if message:
            print(message.value)

        consumer.close()
