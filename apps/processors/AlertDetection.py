import sys
import os
from pyspark.sql.functions import col, from_json, to_json, struct

# Path Definition
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

import config
import schema
from spark_utils import get_spark_session

def run_detection_service():
    '''
    Reads enriched data from Kafka and detects anomalies (Speeding/Wrong Gear)
    '''
    spark = get_spark_session("AlertDetectionService")

    print("Starting Alert Logic...")

    # Read from Enriched Topic
    print(f"Listening to topic: {config.DATA_ENRICHMENT_TOPIC}")
    enriched_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", config.DATA_ENRICHMENT_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # Parse using the ENRICHED Schema
    parsed_stream = enriched_stream.select(
        from_json(col("value").cast("string"), schema.KAFKA_ENRICHED_SCHEMA).alias("data")
    ).select("data.*")

    # Define Alert Logic - Speed over 200 OR Gear not as expected OR RPM is greater than 6000
    alerts_df = parsed_stream.filter(
        (col("speed") > 200) | 
        (col("gear") != col("expected_gear")) |
        (col("rpm") > 6000)
    )

    print(f"Publishing Alerts to topic: {config.ALERT_TOPIC}")

    # Send to Kafka

    query_detection = alerts_df \
        .select(to_json(struct("*")).alias("value")) \
        .writeStream \
        .queryName("Write_Alerts_To_Kafka") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", config.ALERT_TOPIC) \
        .option("checkpointLocation", f"{config.DATA_ROOT}/checkpoints/alerts") \
        .outputMode("append") \
        .start()

    # Output Alerts to Console
    # query_ = alerts_df \
    #     .select("event_time", "car_id", "brand_name", "speed", "gear", "expected_gear", "driver_id", "rpm") \
    #     .writeStream \
    #     .format("console") \
    #     .option("truncate", "false") \
    #     .outputMode("append") \
    #     .start()

    query_detection.awaitTermination()

if __name__ == "__main__":
    run_detection_service()