import sys
import os
import pandas as pd
from pyspark.sql.functions import col, from_json, window, count, max, sum, when

# Path Definition to import local modules
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

import config
import schema
from spark_utils import get_spark_session

# Use the path defined in config.py
CSV_PATH = config.DASHBOARD_DATA_PATH

def print_batch(df, epoch_id):
    print(f"\n--- Global Alert Summary | Batch: {epoch_id} ---")
    
    if df.count() == 0:
        print("Waiting for data...")
        return

    # 1. Convert Spark DataFrame to Pandas (Efficient for small aggregated data)
    pdf = df.toPandas()
    
    if not pdf.empty:
        # 2. Data Cleaning & Formatting
        # Extract the 'start' time from the Spark window struct object
        pdf['window_start'] = pdf['window'].apply(lambda x: x['start'])
        pdf = pdf.drop(columns=['window'])
        
        # Reorder columns to put timestamp first
        cols = ['window_start'] + [c for c in pdf.columns if c != 'window_start']
        pdf = pdf[cols]

        # 3. Save to CSV (Appended for Streamlit to read)
        # Write header only if file does not exist
        header = not os.path.exists(CSV_PATH)
        pdf.to_csv(CSV_PATH, mode='a', header=header, index=False)
        print(f"Data saved to {CSV_PATH}")

        # 4. Console Output (Show only the latest window for clarity)
        # Sort by time and pick the most recent one
        latest_row = pdf.sort_values(by='window_start', ascending=False).iloc[0]
        
        print(f"Window Time: {latest_row['window_start']}")
        print("-" * 30)
        # Print as a nice horizontal table
        print(latest_row.to_frame().T.to_string(index=False))

def run_counter_service():
    print("Starting Global Alert Counter (Real-Time Dashboard Mode)...")
    
    # Clean up old dashboard data on startup
    if os.path.exists(CSV_PATH):
        try:
            os.remove(CSV_PATH)
            print("Deleted old dashboard data file.")
        except OSError:
            print("Could not delete old file, continuing...")

    spark = get_spark_session("AlertCounterService")
    spark.sparkContext.setLogLevel("ERROR")

    # Read from Kafka
    alerts_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", config.ALERT_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON Data
    parsed_alerts = alerts_stream.select(
        from_json(col("value").cast("string"), schema.KAFKA_ENRICHED_SCHEMA).alias("data")
    ).select("data.*")

    # Ensure event_time is a timestamp
    parsed_alerts = parsed_alerts.withColumn("event_time", col("event_time").cast("timestamp"))

    # Aggregation Logic
    # Using a short 1-minute window for a dynamic dashboard experience
    counts_df = parsed_alerts \
        .withWatermark("event_time", "30 seconds") \
        .groupBy(
            window(col("event_time"), "1 minute")
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
    
    # Write Stream
    # OutputMode 'update' allows us to see changes in real-time before the window closes
    query = counts_df \
        .writeStream \
        .outputMode("update") \
        .trigger(processingTime='1 seconds') \
        .foreachBatch(print_batch) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    run_counter_service()