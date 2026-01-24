import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 1. Infrastructure Settings
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

# 2. Paths
DATA_ROOT = os.getenv("DATA_ROOT_PATH", "s3a://spark/data")
DIMS_PATH = f"{DATA_ROOT}/dims"
CARS_PATH = f"{DIMS_PATH}/cars"

# 3. Cars in Inventory
CARS_INVENTORY = 20

# 4. Kaka Topics
DATA_GENERATOR_TOPIC = "sensors-sample"
DATA_ENRICHMENT_TOPIC = "samples-enriched"
ALERT_TOPIC = "alert-data"

# 5. Path for saving dashboard data
DASHBOARD_DATA_PATH = "/home/jovyan/apps/dashboard_data.csv"