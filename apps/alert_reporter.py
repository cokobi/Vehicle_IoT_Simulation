import sys
import os

# This service will:
# - Read violation data from the 'alert-data' topic
# - Perform windowed aggregations (10 minutes)
# - Print the statistics to the console

# Path Setup
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import the business logic from the processors folder
from processors.AlertCounter import run_counter_service

if __name__ == "__main__":
    print("Launching Alert Reporter Service...")   
    run_counter_service()