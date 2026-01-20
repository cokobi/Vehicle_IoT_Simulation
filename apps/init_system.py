import config
from spark_utils import get_spark_session
from generators.ModelCreation import static_data_generator
from generators.CarsGenerator import generate_cars_dataset

def main():
    spark = get_spark_session("InitStaticData")

    # Create Car Models & Colors (Reference Data)
    print("Generating Models & Colors")
    static_data_generator(spark)

    # Create Cars Inventory (The active fleet)
    print(f"Generating Fleet of {config.CARS_INVENTORY} Cars")
    generate_cars_dataset(spark, config.CARS_INVENTORY)

    print("System Initialization Complete. Data is ready in MinIO.")
    spark.stop()

if __name__ == "__main__":
    main()