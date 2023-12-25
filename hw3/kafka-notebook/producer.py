import argparse
import random
from json import dumps
from time import sleep
from kafka import KafkaProducer, errors

def write_data(producer, data_cnt):
    topic = "hw3_topic"
    for i in range(data_cnt):
        device_id = random.randint(1, 10)
        temperature = random.uniform(60, 110) + 273
        execution_time = i * 5
        cur_data = {"device_id": device_id, "temperature": temperature, "execution_time": execution_time}
        producer.send(topic,
                      key=dumps(device_id).encode('utf-8'),
                      value=cur_data)
        print(f"Data was sent to topic [{topic}]: {cur_data}")
        sleep(1)

def create_producer():
    print("Connecting to Kafka brokers")
    try:
        producer = KafkaProducer(bootstrap_servers=['kafka:9092'],
                                 value_serializer=lambda x: dumps(x).encode('utf-8'),
                                 acks=1)
        print("Connected to Kafka")
        return producer
    except errors.NoBrokersAvailable:
        print("Waiting for brokers to become available")
        raise RuntimeError("Failed to connect to brokers within retry limit")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Kafka Producer")
    parser.add_argument("--message-count", type=int, help="Number of messages to produce", default=10)
    args = parser.parse_args()

    producer = create_producer()
    write_data(producer, args.message_count)

