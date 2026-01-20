from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define the schema for cars Dataset
CARS_SCHEMA = StructType([
        StructField("car_id", StringType(), False),
        StructField("driver_id", StringType(), False),
        StructField("model_id", IntegerType(), False),
        StructField("color_id", IntegerType(), False)
    ])

# Define the schema for the Raw car events data
KAFKA_RAW_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("car_id", StringType(), True),
    StructField("event_time", StringType(), True), # Received as String, will be converted to Timestamp later
    StructField("speed", IntegerType(), True),
    StructField("rpm", IntegerType(), True),
    StructField("gear", IntegerType(), True)
])

# Define the schema for Enricher car events data
KAFKA_ENRICHED_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("car_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("speed", IntegerType(), True),
    StructField("rpm", IntegerType(), True),
    StructField("gear", IntegerType(), True),
    StructField("driver_id", StringType(), True),
    StructField("brand_name", StringType(), True),
    StructField("model_name", StringType(), True),
    StructField("color_name", StringType(), True),
    StructField("expected_gear", IntegerType(), True),
])