import time
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


def produce_data(producer, topic, data):
    for _, row in data.iterrows():
        print(f"Producing message: {row.to_json()}")
        message = row.to_json().encode('utf-8')
        producer.send(topic, message)
        time.sleep(1)  # Simulate near real-time


def create_kafka_producer(bootstrap_servers, retries=10, delay=10):
    """
    Try to create a Kafka producer, retrying if no brokers are available.
    :param bootstrap_servers: Kafka bootstrap servers
    :param retries: Number of retries before giving up
    :param delay: Delay between retries in seconds
    :return: KafkaProducer instance
    """
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                # value_serializer=lambda v: v.encode('utf-8')
            )
            return producer
        except NoBrokersAvailable:
            print(f"Kafka broker not available. Retrying in {delay} seconds... (Attempt {attempt + 1}/{retries})")
            time.sleep(delay)
    raise Exception("Failed to connect to Kafka after multiple retries.")


if __name__ == "__main__":
    kafka_producer = create_kafka_producer('kafka:9092', retries=100, delay=10)

    bus_data = pd.read_csv('/data/bus_traffic_real-time_data.csv', on_bad_lines='skip')
    print("I'm about to produce")
    produce_data(kafka_producer, 'bus_traffic_data', bus_data)

    kafka_producer.flush()
    kafka_producer.close()
