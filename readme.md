```markdown
# Real-Time Traffic Anomaly Detection System

A scalable, Microservices-based data engineering project that simulates vehicle traffic, processes streams in real-time using **Apache Spark Structured Streaming** and **Kafka**, and detects driving anomalies (overspeeding, wrong gear usage).

## Architecture Overview

The system is built as a pipeline of decoupled services:

1.  **Data Generator:** Simulates raw car telemetry (speed, rpm, gear, location) and pushes to Kafka (`raw-data`).
2.  **Enrichment Service:** Consumes raw data, joins it with static reference data (Car Models, Colors) stored in **MinIO**, and pushes enriched data to Kafka (`samples-enriched`).
3.  **Detection Service:** Consumes enriched data, filters for anomalies (Speed > 120 km/h OR Wrong Gear), and pushes alerts to Kafka (`alert-data`).
4.  **Reporting Service:** Consumes alerts, performs windowed aggregations (10-minute windows), and prints a live dashboard to the console.

**Tech Stack:** Python, Apache Spark, Apache Kafka, MinIO (S3), Docker.

---

## How to Run

### Prerequisites
* Docker & Docker Compose installed.
* Create .env file in the project's folder with the following settings:
# MinIO Configuration (Cahnge according to your credentioals)
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin

# Spark S3 Configuration (Cahnge according to your credentioals)
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
AWS_ENDPOINT_URL=http://minio:9000

### Step 1: Start Infrastructure
Start the Kafka, Spark, and MinIO containers.
```bash
docker-compose up -d

```

### Step 2: System Initialization

Initialize static data in MinIO (bucket creation, reference data upload) and create necessary Kafka topics.

```bash
docker-compose exec jupyter spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /home/jovyan/apps/init_system.py

```

### Step 3: Launch Microservices (Open separate terminals)

**Terminal 1: Enrichment Service**

```bash
docker-compose exec jupyter spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /home/jovyan/apps/enrichment_processor.py

```

**Terminal 2: Alert Detector**

```bash
docker-compose exec jupyter spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /home/jovyan/apps/alert_detector.py

```

**Terminal 3: Alert Reporter (Dashboard)**

```bash
docker-compose exec jupyter spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /home/jovyan/apps/alert_reporter.py

```

### Step 4: Start Data Simulation

Once all listeners are up, start the data generator to simulate traffic.

**Terminal 4: Raw Data Producer**

```bash
docker-compose exec jupyter spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /home/jovyan/apps/raw_data_producer.py

```

---

## Dashboard Example

The **Alert Reporter** displays real-time aggregated statistics every 5 seconds for the current 10-minute window:

```text
--- Alert Report | Batch: 15 ---
+------------------------------------------+-----------+------------+------------+-------------+-------------+------------+-----------+
|window                                    |num_of_rows|num_of_black|num_of_white|num_of_silver|maximum_speed|maximum_gear|maximum_rpm|
+------------------------------------------+-----------+------------+------------+-------------+-------------+------------+-----------+
|{start: 2026-01-20..., end: 2026-01-20...}|42         |12          |15          |5            |165          |6           |6500       |
+------------------------------------------+-----------+------------+------------+-------------+-------------+------------+-----------+

```

## Project Structure

```text
SPARKMIDPROJECT/
├── apps/
│   ├── generators/          # Logic for data generation (Cars, Models, etc.)
│   ├── processors/          # Business logic (AlertCounting, AlertDetection)
│   ├── init_system.py       # Setup script (MinIO & Kafka init)
│   ├── raw_data_producer.py # Entry point for traffic simulation
│   ├── alert_detector.py    # Runner for detection service
│   ├── alert_reporter.py    # Runner for reporting service
│   ├── enrichment_processor.py
│   ├── config.py            # Central configuration
│   ├── schema.py            # Data schemas
│   └── ...
├── dags/                    # Airflow DAGs (if applicable)
├── data/                    # Local data storage / checkpoints
├── docker/                  # Docker configurations
├── logs/                    # Application logs
├── notebooks/               # Jupyter Notebooks for exploration
├── docker-compose.yml       # Container orchestration
└── requirements.txt         # Python dependencies

```

```

```