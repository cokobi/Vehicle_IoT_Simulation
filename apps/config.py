import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 1. Infrastructure Settings
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

# 2. Paths
DATA_ROOT = os.getenv("DATA_ROOT_PATH", "s3a://spark/data")
DIMS_PATH = f"{DATA_ROOT}/dims"
CARS_PATH = f"{DIMS_PATH}/cars"

# 3. Cars in Inventory
CARS_INVENTORY = 20

# 4. Kaka Topics
DATA_GENERATOR_TOPIC = "sensors-sample"
DATA_ENRICHMENT_TOPIC = "samples-enriched"
ALERT_TOPIC = "alert-data"

# 5. Initiator function for Kafka-Spark Topics
def initialize_kafka_topics(spark):
    """
    Sends a dummy message to all required Kafka topics to ensure they are created.
    This prevents 'UnknownTopicOrPartitionException' for the consumers.
    """
    print("Initializing Kafka Topics")

    topics_to_create = [
        DATA_GENERATOR_TOPIC,  
        DATA_ENRICHMENT_TOPIC, 
        ALERT_TOPIC            
    ]

    for topic in topics_to_create:
        try:
            # create temporary df with a single message
            df = spark.createDataFrame([("init_topic",)], ["value"])
            df.write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka:9092") \
                .option("topic", topic) \
                .save()
            print(f"Topic '{topic}' is ready.")  
        except Exception as e:
            print(f"Warning: Could not initialize topic '{topic}'. Error: {e}")