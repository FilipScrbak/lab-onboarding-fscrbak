from confluent_kafka import Consumer, KafkaException
from google.cloud import pubsub_v1
import pandas as pd

#TODO hide all vital info in secrets or variables

US_TOPIC: str = "us-sales-dev"
BR_TOPIC: str = "br-sales-dev"



config = {
    "bootstrap.servers": "157.90.229.141:9092",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "SCRAM-SHA-512",
    "sasl.username": "admin-user",
    "sasl.password": "K9r3A1f4t",
    "group.id": "fscrbak1",
    "auto.offset.reset": "earliest",
    "enable.ssl.certificate.verification": False,
    "enable.auto.commit": False,
    "max.poll.interval.ms": 600000
}


def publish_to_pub_sub(payload):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path("syntio-onboarding-dev", "fscrbak_sales_src")
    future = publisher.publish(topic_path, payload.encode("utf-8"))
    print(future.result())



def consume(topic: str):
    consumer = Consumer(config)
    consumer.subscribe([topic])

    print(f"Consuming messages from topic: {topic}")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                raise KafkaException(msg.error())

            return msg


    except KeyboardInterrupt:
        print("Stopping consumer...")

    finally:
        consumer.close()

#todo define process_message
def main():
    msg_us = consume(US_TOPIC).value().decode('utf-8')
    msg_br = consume(BR_TOPIC).value().decode('utf-8')

    publish_to_pub_sub(msg_us)
    #publish_to_pub_sub(msg_br)


if __name__ == "__main__":
    main()