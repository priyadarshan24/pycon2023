from locust import HttpUser, task, constant, TaskSet, events
from confluent_kafka import Producer, Consumer

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

        topic = 'my-topic-v3'  # Replace with the topic you want to consume from
        consumer.subscribe([topic])

        message = consumer.poll(timeout=20)
        consumer.consume()

        if message:
            print("message##", message.value)
            print("latency##", message.latency.)
            events.request.fire(request_type="DEQUEUE",
                                name="my-topic-v3",
                                response_time=message.latency() * 1000,
                                response_length=len(message.value()))

        consumer.close()
