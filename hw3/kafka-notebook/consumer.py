import argparse
from kafka import KafkaConsumer

def create_consumer(max_messages=None):
    print("Connecting to Kafka brokers")
    consumer = KafkaConsumer("hw3_topic",
                             group_id="itmo_hw3_group",
                             bootstrap_servers='kafka:9092',
                             auto_offset_reset='earliest',
                             enable_auto_commit=True)

    message_count = 0
    for message in consumer:
        print(message)

        message_count += 1

        if max_messages and message_count >= max_messages:
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Kafka Consumer Example")
    parser.add_argument("--message-limit", type=int, help="Maximum number of messages to consume", default=None)
    args = parser.parse_args()

    create_consumer(max_messages=args.message_limit)
