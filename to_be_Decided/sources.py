from confluent_kafka import Consumer, KafkaException
from google.cloud import pubsub_v1
import pandas as pd
import json

#TODO hide all vital info in secrets or variables

US_KAFKA_TOPIC: str = "us-sales-dev"
BR_KAFKA_TOPIC: str = "br-sales-dev"

US_PUBSUB_TOPIC: str = "fscrbak_sales_src"
BR_PUBSUB_TOPIC: str = "fscrbak_sales_src_br"



config = {
    "bootstrap.servers": "157.90.229.141:9092",
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "SCRAM-SHA-512",
    "sasl.username": "admin-user",
    "sasl.password": "K9r3A1f4t",
    "group.id": "fscrbak2",
    "auto.offset.reset": "earliest",
    "enable.ssl.certificate.verification": False,
    "enable.auto.commit": False,
    "max.poll.interval.ms": 600000
}


def publish_to_pub_sub(payload: str, pubsub_topic:str):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path("syntio-onboarding-dev", pubsub_topic)
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
    msg_us: str = consume(US_KAFKA_TOPIC).value().decode('utf-8')
    msg_br: str = consume(BR_KAFKA_TOPIC).value().decode('utf-8')

    publish_to_pub_sub(msg_us, US_PUBSUB_TOPIC)

    msg_br_as_list: list[dict[str,str]] = json.loads(msg_br)
    for br_json in msg_br_as_list:
        publish_to_pub_sub(json.dumps(br_json), BR_PUBSUB_TOPIC)


if __name__ == "__main__":
    main()