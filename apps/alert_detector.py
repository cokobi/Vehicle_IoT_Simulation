import sys
import os

# This service will:
    # - Read enriched data from Kafka
    # - Filter for speed/gear violations
    # - Publish the violations to the 'alert-data' topic

# Path Setup
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import the business logic from the processors folder
from processors.AlertDetection import run_detection_service

if __name__ == "__main__":
    print("Launching Alert Detector Service...")
    run_detection_service()