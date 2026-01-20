import config
from spark_utils import get_spark_session
from generators.DataGenerator import generate_events
from config import initialize_kafka_topics

def main():
    print("Starting Traffic Producer...")
    spark = get_spark_session("TrafficProducer")

    initialize_kafka_topics(spark)

    # Infinite Loop
    print(f"Sending raw events to topic: {config.DATA_GENERATOR_TOPIC}")
    generate_events(spark)

if __name__ == "__main__":
    main()