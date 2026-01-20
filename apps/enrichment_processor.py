import config
from spark_utils import get_spark_session
from generators.DataEnrichment import enrich_cars_data

def main():
    print("Starting Enrichment Processor...")
    spark = get_spark_session("EnrichmentService")

    # Load Static Data (Lookup Tables)
    print("Loading Reference Data from MinIO")
    cars_df = spark.read.parquet(config.CARS_PATH).alias("cars").cache()
    colors_df = spark.read.parquet(f"{config.DIMS_PATH}/car_colors").alias("colors").cache()
    models_df = spark.read.parquet(f"{config.DIMS_PATH}/car_models").alias("models").cache()
    
    # Materialize Cache
    print(f"Cached: {cars_df.count()} Cars, {colors_df.count()} Colors, {models_df.count()} Models")

    # Run Logic
    enrich_cars_data(spark, cars_df, colors_df, models_df)

if __name__ == "__main__":
    main()
