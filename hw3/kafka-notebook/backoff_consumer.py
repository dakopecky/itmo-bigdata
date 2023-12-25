import time
from functools import wraps
import argparse
from kafka import KafkaConsumer


def backoff(tries, sleep):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            ntries = tries
            while ntries > 1:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    print(f"Error: {e}, waiting {sleep} seconds before retry...")
                    time.sleep(sleep)
                    ntries -= 1
            return func(*args, **kwargs)
        return wrapper
    return decorator


@backoff(tries=10, sleep=10)
def connect_kafka_consumer():
    try:
        consumer = KafkaConsumer("hw3_topic",
                                 group_id="itmo_hw3_group",
                                 bootstrap_servers='kafka:9092',
                                 auto_offset_reset='earliest',
                                 enable_auto_commit=True)
        print("Connected to Kafka")
        return consumer
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        raise


def message_handler(value):
    print(value)


def create_consumer(max_messages=None):
    consumer = connect_kafka_consumer()

    print("Consumer setup complete. Listening for messages...")
    message_count = 0
    for message in consumer:
        try:
            message_handler(message.value)
            message_count += 1
            if max_messages and message_count >= max_messages:
                break
        except Exception as e:
            print(f"Error processing message: {e}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Kafka Consumer with Backoff for Connection")
    parser.add_argument("--message-limit", type=int, help="Maximum number of messages to consume", default=None)
    args = parser.parse_args()

    create_consumer(max_messages=args.message_limit)
