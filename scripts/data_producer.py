import time
import pandas as pd
from kafka import KafkaProducer


def read_csv(file_path):
    return pd.read_csv(file_path)


def produce_data(producer, topic, data):
    for _, row in data.iterrows():
        message = row.to_json().encode('utf-8')
        producer.send(topic, message)
        time.sleep(0.001)  # Simulate near real-time


if __name__ == "__main__":
    kafka_producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: v.encode('utf-8')
    )

    bus_data = read_csv('../data/bus_traffic_real-time_data.csv')
    produce_data(kafka_producer, 'bus_traffic_data', bus_data)

    kafka_producer.flush()
    kafka_producer.close()
