import pandas as pd
import sys
import os

# Path Definition
current_dir = os.path.dirname(os.path.abspath(__file__)) # apps/generators
parent_dir = os.path.dirname(current_dir) # apps
sys.path.append(parent_dir)

import config

def static_data_generator(spark):
    '''
    The function gets a live SparkSession and writes datasets to MINIO
    '''
    models = [
        {"model_id":1, "car_brand":"Mazda", "car_model":"3"},
        {"model_id":2, "car_brand":"Mazda", "car_model":"6"},
        {"model_id":3, "car_brand":"Toyota", "car_model":"Corolla"},
        {"model_id":4, "car_brand":"Hyundai", "car_model":"i20"},
        {"model_id":5, "car_brand":"Kia", "car_model":"Sportage"},
        {"model_id":6, "car_brand":"Kia", "car_model":"Rio"},
        {"model_id":7, "car_brand":"Kia", "car_model":"Picanto"}
    ]

    colors = [
        {"color_id":1, "color_name":"Black"},
        {"color_id":2, "color_name":"Red"},
        {"color_id":3, "color_name":"Gray"},
        {"color_id":4, "color_name":"White"},
        {"color_id":5, "color_name":"Green"},
        {"color_id":6, "color_name":"Blue"},
        {"color_id":7, "color_name":"Pink"}
    ]

    pdf_models = pd.DataFrame(models)
    pdf_colors = pd.DataFrame(colors)

    print(f"   [ModelCreation] Writing Models to: {config.DIMS_PATH}/car_models")
    spark.createDataFrame(pdf_models).write.mode("overwrite").parquet(f"{config.DIMS_PATH}/car_models")

    print(f"   [ModelCreation] Writing Colors to: {config.DIMS_PATH}/car_colors")
    spark.createDataFrame(pdf_colors).write.mode("overwrite").parquet(f"{config.DIMS_PATH}/car_colors")

    print("   [ModelCreation] Done!")