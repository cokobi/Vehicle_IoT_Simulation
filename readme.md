```markdown
# Real-Time Traffic Anomaly Detection System

A scalable, Microservices-based data engineering project that simulates vehicle traffic, processes streams in real-time using **Apache Spark Structured Streaming** and **Kafka**, and detects driving anomalies (overspeeding, wrong gear usage).

## Architecture Overview

The system is built as a pipeline of decoupled services:

1.  **Data Generator:** Simulates raw car telemetry (speed, rpm, gear, location) and pushes to Kafka (`raw-data`).
2.  **Enrichment Service:** Consumes raw data, joins it with static reference data (Car Models, Colors) stored in **MinIO**, and pushes enriched data to Kafka (`samples-enriched`).
3.  **Detection Service:** Consumes enriched data, filters for anomalies (Speed > 120 km/h OR Wrong Gear), and pushes alerts to Kafka (`alert-data`).
4.  **Aggregation Service:** Consumes alerts, calculates windowed statistics (e.g., max speed, violation counts), and sinks data to local storage for the dashboard.
5.  **Dashboard UI:** A **Streamlit** app that reads the aggregated data and visualizes real-time metrics using **Plotly**.

**Tech Stack:** Python, Apache Spark, Apache Kafka, MinIO (S3), Docker, Streamlit, Plotly.

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

**Terminal 3: Alert Aggregator (Spark Job) Calculates stats and updates the dashboard data source.**

```bash
docker-compose exec jupyter spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /home/jovyan/apps/processors/AlertCounter.py

```

### Step 4: Start Data Visualization (Streamlit)

Terminal 4: Dashboard UI Run this command and then open http://localhost:8501 in your browser.

```bash
docker-compose exec jupyter streamlit run /home/jovyan/apps/dashboard.py --server.port 8501 --server.address 0.0.0.0

```

### Step 5: Start Traffic Simulation
Terminal 5: Raw Data Producer Once all services are running, start generating traffic data.

```bash
docker-compose exec jupyter spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /home/jovyan/apps/raw_data_producer.py

```

---

## Dashboard Features
The project includes a Real-Time Control Center dashboard built with Streamlit and Plotly, featuring:

* High-Contrast Dark Mode: Optimized for operational monitoring.
* Live Telemetry: Animated Speedometer and RPM Gauges showing the latest critical events.
* KPI Cards: Real-time metrics for Velocity, Total Session Violations, and Engine Stress Events.
* Visual Trends: Heatmap-style bar charts showing alert volume over the last 30 minutes.
* Data Inspection: Expandable raw data view with visual progress bars for speed analysis.

## Project Structure

```text
SPARKMIDPROJECT/
├── apps/
│   ├── generators/          # Logic for data generation (Cars, Models, etc.)
│   ├── processors/          # Business logic (AlertCounting, AlertDetection)
│   │   └── AlertCounter.py  # Spark job for dashboard aggregation
│   ├── init_system.py       # Setup script (MinIO & Kafka init)
│   ├── raw_data_producer.py # Entry point for traffic simulation
│   ├── alert_detector.py    # Runner for detection service
│   ├── enrichment_processor.py
│   ├── dashboard.py         # Streamlit Visualization App
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