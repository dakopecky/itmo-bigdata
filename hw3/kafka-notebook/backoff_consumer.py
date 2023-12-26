import time
import random
from functools import wraps
import argparse
from kafka import KafkaConsumer
from json import loads

def backoff(tries, delay, backoff_factor=1.5):
    """
    Backoff decorator that retries the function upon failure.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            ntries, ndelay = tries, delay
            while ntries > 1:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    print(f"Error: {e}, retrying in {ndelay} seconds...")
                    time.sleep(ndelay)
                    ntries -= 1
                    ndelay *= backoff_factor
            return func(*args, **kwargs)
        return wrapper
    return decorator

@backoff(tries=10, delay=2)
def message_handler(value):
    """
    Process the message and simulate an error with a 30% probability.
    """
    if random.random() < 0.3:
        raise ValueError("Simulated processing error")
    print(f"Processed message: {value}")

def connect_kafka_consumer():
    try:
        consumer = KafkaConsumer("hw3_topic",
                                 group_id="itmo_hw3_group",
                                 bootstrap_servers='kafka:9092',
                                 auto_offset_reset='earliest',
                                 enable_auto_commit=True,
                                 value_deserializer=lambda x: loads(x.decode('utf-8')))
        print("Connected to Kafka")
        return consumer
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        raise

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
    parser = argparse.ArgumentParser(description="Kafka Consumer with Backoff for Message Handling")
    parser.add_argument("--message-limit", type=int, help="Maximum number of messages to consume", default=None)
    args = parser.parse_args()

    create_consumer(max_messages=args.message_limit)
