import sys
import os
from pyspark.sql.functions import col, from_json, window, count, max, sum, when

# Path Definition
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

import config
import schema
from spark_utils import get_spark_session

def print_batch(df, epoch_id):
    """
    This function is triggered for every micro-batch.
    It clears the console screen and prints the fresh DataFrame, 
    creating a 'Dashboard' effect.
    """
    # ANSI escape code to clear the terminal screen (works in Docker/Linux)
    # print("\033[H\033[J", end="") 
    
    print(f"--- Alert Report | Batch: {epoch_id} ---")
    
    # Check if DataFrame is empty
    row_count = df.count()
    if row_count == 0:
        print("Waiting for data...")
        print("Check list:")
        print("  1. Is Producer running? (Terminal 4)")
        print("  2. Is Detector running? (Terminal 2)")
        return
    
    # Get the latest window only
    latest_window_row = df.orderBy(col("window.start").desc()).limit(1)

    row = latest_window_row.first()
    window_start = row["window"]["start"]
    print(f"Window Time: {window_start}")
    print("-" * 30)

    clean_df = latest_window_row.select(
                         "num_of_rows", 
                         "num_of_black", 
                         "num_of_white", 
                         "num_of_silver",
                         "maximum_speed", 
                         "maximum_gear",
                         "maximum_rpm"
                         )

    # Print the DataFrame to the console without truncating values
    clean_df.show(truncate=False)

def run_counter_service():
    print("Starting Alert Counter Service (Console Output)...")
    spark = get_spark_session("AlertCounterService")

    # Reduce log verbosity for cleaner console output
    spark.sparkContext.setLogLevel("ERROR")

    # Read Stream from 'alert-data' topic
    alerts_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", config.ALERT_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON (Using Enriched Schema because the data structure hasn't changed)
    parsed_alerts = alerts_stream.select(
        from_json(col("value").cast("string"), schema.KAFKA_ENRICHED_SCHEMA).alias("data")
    ).select("data.*")

    parsed_alerts = parsed_alerts.withColumn("event_time", col("event_time").cast("timestamp"))

    # Aggregation Logic:
    # Window: 10 minutes
    # Metrics: Count Total, Count Colors, Max values

    counts_df = parsed_alerts \
        .withWatermark("event_time", "30 seconds") \
        .groupBy(
            window(col("event_time"), "10 minutes"),
            # col("brand_name")
        ) \
        .agg(
            count("*").alias("num_of_rows"),
            sum(when(col("color_name") == "Black", 1).otherwise(0)).alias("num_of_black"),
            sum(when(col("color_name") == "White", 1).otherwise(0)).alias("num_of_white"),
            sum(when(col("color_name") == "Silver", 1).otherwise(0)).alias("num_of_silver"),
            max("speed").alias("maximum_speed"),
            max("gear").alias("maximum_gear"),
            max("rpm").alias("maximum_rpm")
        )
    
    # Write Stream (To Console)
    print("--- Alerts Statistics from the Last 10 minutes: ---")

    query = counts_df \
        .writeStream \
        .outputMode("update") \
        .trigger(processingTime='10 minutes') \
        .foreachBatch(print_batch) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    run_counter_service()