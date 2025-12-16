from confluent_kafka import Consumer, KafkaException
from google.cloud import pubsub_v1
import json

# TODO hide all vital info in secrets or variables

TOPICS: list[str] = ["us-sales-dev", "br-sales-dev"]

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
    "max.poll.interval.ms": 600000,
}


def publish_to_pub_sub(payload: str, pubsub_topic: str):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path("syntio-onboarding-dev", pubsub_topic)
    future = publisher.publish(topic_path, payload.encode("utf-8"))
    print(future.result())


def process_message(messages, topic):
    for message in messages:
        if topic == "us-sales-dev":
            print(f"processing us messages. number of messages: {len(messages)}")
            msg_us: str = message.value().decode("utf-8")
            publish_to_pub_sub(msg_us, US_PUBSUB_TOPIC)
        else:
            print(f"processing br messages. number of messages: {len(messages)}")
            msg_br: str = message.value().decode("utf-8")
            msg_br_as_list: list[dict[str, str]] = json.loads(msg_br)
            for br_json in msg_br_as_list:
                publish_to_pub_sub(json.dumps(br_json), BR_PUBSUB_TOPIC)


def consume(topic: str, consumer: Consumer):
    print(f"Consuming messages from topic: {topic}")
    messages = []
    counter: int = 0
    try:
        while True:
            if counter > 10:
                return messages
            msg = consumer.poll(1.0)

            if msg is None:
                counter += 1
                continue
            else:
                messages.append(msg)

            if msg.error():
                raise KafkaException(msg.error())

    except KeyboardInterrupt:
        print("Stopping consumer...")

    finally:
        consumer.close()


def main():
    for topic in TOPICS:
        consumer = Consumer(config)
        consumer.subscribe([topic])
        messages = consume(topic, consumer)
        process_message(messages, topic)


if __name__ == "__main__":
    main()
