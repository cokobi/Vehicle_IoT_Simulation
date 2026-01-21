import config
import boto3
from botocore.exceptions import ClientError
import sys
from spark_utils import get_spark_session
from generators.ModelCreation import static_data_generator
from generators.CarsGenerator import generate_cars_dataset

def init_s3_bucket():
    """
    Creates the 'spark' bucket in MinIO if it doesn't exist.
    """
    print("--- Initializing MinIO Bucket ---")
    
    # MINIO S3 Client Setup
   
    s3_client = boto3.client(
        "s3",
        endpoint_url=config.MINIO_ENDPOINT,
        aws_access_key_id=config.MINIO_ACCESS_KEY,
        aws_secret_access_key=config.MINIO_SECRET_KEY
    )

    bucket_name = "spark"

    try:
        # check if the bucket already exists
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
    except ClientError:
        # if it doesn't exist - create it
        print(f"Creating bucket '{bucket_name}'...")
        try:
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' created successfully.")
        except Exception as e:
            print(f"Failed to create bucket: {e}")
            sys.exit(1)

# Initiator function for Kafka-Spark Topics
def initialize_kafka_topics(spark):
    """
    Sends a dummy message to all required Kafka topics to ensure they are created.
    This prevents 'UnknownTopicOrPartitionException' for the consumers.
    """
    print("Initializing Kafka Topics")

    topics_to_create = [
        config.DATA_GENERATOR_TOPIC,  
        config.DATA_ENRICHMENT_TOPIC, 
        config.ALERT_TOPIC            
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

def main():

    print("Initializing System Infrastructure...")
    # 1. Initialize MinIO Bucket
    init_s3_bucket()

    # 2. Get Spark Session
    spark = get_spark_session("InitKafkaTopics")

    # 3. Initialize Kafka Topics
    print("Initializing Kafka Topics...")
    initialize_kafka_topics(spark)
    
    # 4. Create Car Models & Colors (Reference Data)
    print("Generating Static Data - Models & Colors")
    static_data_generator(spark)

    # 5. Create Cars Inventory (The active fleet)
    print(f"Generating Fleet of {config.CARS_INVENTORY} Cars")
    generate_cars_dataset(spark, config.CARS_INVENTORY)

    print("System Initialization Complete. Data is ready in MinIO.")
    spark.stop()

if __name__ == "__main__":
    main()