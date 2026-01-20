import os
import sys
from pyspark.sql.functions import col, from_json, round, to_json, struct, broadcast
from datetime import datetime

# Path Setup
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

import config
import schema

def enrich_cars_data(spark, cars_df, colors_df, models_df):
    '''
    This function enriches the car events data with driver IDs, 
    car's model and color, and calculates expected gear.
    Finally, it writes the enriched data back to Kafka.
    '''
    
    # ReadStream from Kafka
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", 'kafka:9092') \
        .option("subscribe", config.DATA_GENERATOR_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # Parse JSON - take the binary 'value' column, cast to string, and parse with schema
    parsed_stream = raw_stream.select(
        from_json(col("value").cast("string"), schema.KAFKA_RAW_SCHEMA).alias("data")
    ).select("data.*").alias("events")
    
    # Perform Joins & Enrichment
    cars = cars_df.alias("cars")
    colors = colors_df.alias("colors")
    models = models_df.alias("models")

    enriched_df = parsed_stream \
        .join(broadcast(cars), col("events.car_id") == col("cars.car_id"), "left") \
        .join(broadcast(colors), col("cars.color_id") == col("colors.color_id"), "left") \
        .join(broadcast(models), col("cars.model_id") == col("models.model_id"), "left") \
        .select(
            col("events.event_id"),
            col("events.car_id"),
            col("events.event_time"),
            col("events.speed"),
            col("events.rpm"),
            col("events.gear"),
            col("cars.driver_id"),
            col("models.car_brand").alias("brand_name"),
            col("models.car_model").alias("model_name"),
            col("colors.color_name"),
            round(col("events.speed") / 30).cast("int").alias("expected_gear")
        )

    # Write to Kafka   
    query = enriched_df \
        .select(to_json(struct("*")).alias("value")) \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", config.DATA_ENRICHMENT_TOPIC) \
        .option("checkpointLocation", f"{config.DATA_ROOT}/checkpoints/enrichment") \
        .outputMode("append") \
        .start()

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Enrichment Stream Started...")

    query.awaitTermination()