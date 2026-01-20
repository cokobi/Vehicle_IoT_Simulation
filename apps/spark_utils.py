from pyspark.sql import SparkSession
import config
import os

ACCESS_KEY = os.getenv("MINIO_USER", "minioadmin") 
SECRET_KEY = os.getenv("MINIO_PASSWORD", "minioadmin")
MINIO_ENDPOINT = config.MINIO_ENDPOINT

def get_spark_session(app_name):

    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
    
    return spark

