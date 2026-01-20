from kafka import KafkaProducer
import json
import time

def get_kafka_producer(max_retries=10, sleep_duration=5):
	print("Connecting to Kafka at kafka:9092")
	for i in range(max_retries):
		try:
			producer = KafkaProducer(
                bootstrap_servers=['kafka:9092'],
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
			print("Connected to Kafka successfully!")
			return producer
		except Exception as e:
			print(f"Connection attempt {i+1}/{max_retries} failed. Will retry in {sleep_duration} seconds.")
			time.sleep(sleep_duration)
	raise Exception("Could not connect to Kafka")

