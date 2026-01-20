from datetime import datetime
import random
import time
import os
import sys

# Path Definition
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from kafka_utils import get_kafka_producer
import config 

CARS_PATH = config.CARS_PATH

def generate_events(spark):
    '''
    This function produces 20 car events every 1 sec
    '''  
    print("Loading cars from Parquet...")
    cars_df = spark.read.parquet(CARS_PATH)
    cars_list = cars_df.collect()
    print(f"Loaded {len(cars_list)} cars.")
    
    producer = get_kafka_producer()

    try:
        while True:
            for car in cars_list:
                event_time = datetime.now().isoformat()
                car_id = car['car_id']
                event = {
                    "event_id": f"{car_id}|{event_time}",
                    "car_id": car_id,
                    "event_time": event_time,
                    "speed": random.randint(0, 200),
                    "rpm": random.randint(0, 8000),
                    "gear": random.randint(1, 7)
                }

                producer.send(config.DATA_GENERATOR_TOPIC, value=event)
            
            producer.flush()

            current_time = datetime.now().strftime("%H:%M:%S")
            print(f"[{current_time}] Sent batch of {len(cars_list)} cars to Kafka.")    
            
            time.sleep(1)

    except KeyboardInterrupt:
        print("Session stopped by the user.")
        
    finally:
        if 'producer' in locals():
            producer.close()
            print("Kafka producer closed.")