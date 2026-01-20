import random
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import sys
import os

# Path Definition
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

import config
from schema import CARS_SCHEMA

def generate_random_id(length):
    '''
    This function generates a random n digits id
    '''
    range_start = 10**(length-1)
    range_end = (10**length) - 1
    return str(random.randint(range_start, range_end))

def generate_cars_dataset(spark, num_rows):
    '''
    This function gets a live spark session, generates cars data and writes it to a parquet file in MINIO (S3)
    '''
    
    print(f"   [CarsGenerator] Generating {num_rows} cars...")
    
    data = []

    for i in range(num_rows):
        car_row = {
            "car_id": generate_random_id(7),
            "driver_id": generate_random_id(9),
            "model_id": random.randint(1, 7),
            "color_id": random.randint(1, 7)
        }
        data.append(car_row)
    
    df = spark.createDataFrame(data, schema=CARS_SCHEMA)

    output_path = f"{config.DIMS_PATH}/cars"

    df.coalesce(1).write.mode("overwrite").parquet(output_path)

    print(f"   [CarsGenerator] Saved to: {output_path}")