from locust import HttpUser, task, constant, TaskSet
import json
from confluent_kafka import Producer, Consumer

print("Hello, world!")


class KafkaProducerUser(HttpUser):

    @task
    def produce(self):
        print("Inside run for producer")
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

        print("Printing message in Bytes::", message.__sizeof__())
        producer.produce(topic="my-topic", key="key", value=message, callback=self.acked)
        producer.flush()

    def acked(self, error, msg):
        if error is not None:
            print("#Failed to deliver message: %s: %s" % (str(msg), str(self)))
        else:
            print(msg.value())

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
