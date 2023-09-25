from locust import User, task, between, events
from confluent_kafka import Consumer, KafkaError
from confluent_kafka import TopicPartition
import time


class KafkaConsumerUser(User):
    wait_time = between(1, 5)  # Define a wait time range between tasks

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        print("I am called")
        self.consumer = Consumer({
            'bootstrap.servers': 'localhost:9092',
            'group.id': 'my-group-v2',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': 'false'
        })
        self.consumer.subscribe(['my-topic-avro-v3'])  # Replace with the Kafka topic to consume

    @task
    def consume_kafka_message(self):
        while True:
            print("Polling started::")
            poll_start_time = time.time()
            message = self.consumer.poll(20)
            if message is None:
                print("no msg received")
                continue
            if message.error():
                print(f"Error while consuming message: {message.error()}")
            else:
                # Process the Kafka message
                payload = message.value()
                msg_received_time = time.time()
                events.request.fire(request_type="DEQUEUE",
                                    name="my-topic-avro-v3",
                                    response_time=msg_received_time-poll_start_time,
                                    response_length=len(message.value()))
                print("payload", payload)
                # Add your message processing logic here
                pass


if __name__ == '__main__':
    KafkaConsumerUser().run()
