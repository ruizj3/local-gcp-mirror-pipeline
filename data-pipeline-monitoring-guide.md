# Data Platform Verification and Monitoring Guide

Follow these operational steps to start your local data streaming infrastructure, monitor real-time pipeline metrics, and query the underlying data warehouse.

---

## 🚀 Step 1: Start the Streaming Data Drivers
To populate your architecture with live traffic, you must execute both the data ingestion producer and the processing engine. Open two separate terminal windows on your host machine, navigate to your project directory, activate your virtual environment, and run the following:

### Terminal Window A: Launch Inbound Event Ingestion
This script simulates real-time client traffic by generating synthetic clickstream payloads and publishing them directly into your local broker.
```bash
source .venv/bin/activate
python kafka/mock_producer.py
```
* **Expected Output:** A live, continuous terminal stream of telemetry data logs:  
  `📡 Sent event: evt_104829 | view_item -> PROD_213`

### Terminal Window B: Launch the Real-Time Processing Engine
This script triggers PySpark Structured Streaming to consume the inbound broker messages, apply window transformations, and handle transactional commits.
```bash
source .venv/bin/activate
python spark/jobs/streaming_ingest.py
```
* **Expected Output:** After driver coordination loads, structured micro-batch execution metrics will print every 60 seconds:  
  `📦 Writing micro-batch #1 to PostgreSQL analytics warehouse...`

---

## 📊 Step 2: Pipeline Logging & Metric Verification
Your running applications act as your local monitoring infrastructure framework:
* **Throughput Metrics:** Monitored via the active output blocks printing to **Terminal Window B**. Each micro-batch confirms successful stream calculation states.
* **Orchestration Health:** Monitored via the **Apache Airflow Dashboard** at `http://localhost:8085`. Navigate to the DAG grid view to track task instance execution logs, retries, and task execution durations.

---

## 🗄️ Step 3: Query the Database Data Warehouse

### Option A: Adminer Web UI (Mirrors the BigQuery Console query editor)
Open `http://localhost:8081` in your browser and log in with:
- **System:** PostgreSQL
- **Server:** `postgres`
- **Username:** `pipeline_user`
- **Password:** `pipeline_password`
- **Database:** `analytics_warehouse`

Use the **SQL command** tab to run the queries below and browse results in-browser.

### Option B: psql via Docker
You can also inspect your data metrics directly inside your isolated storage container without installing external third-party graphical database tools.

Open a **third terminal window** and connect to your database instance:
```bash
docker exec -it local-gcp-mirror-pipeline-postgres-1 psql -U pipeline_user -d analytics_warehouse

SELECT * FROM aggregated_device_metrics ORDER BY window_start DESC LIMIT 10;
SELECT * FROM daily_executive_device_summary;
```
*(Note: If your local container names differ slightly, run `docker ps` to verify your target PostgreSQL container name).*

Once your terminal prompt updates to `analytics_warehouse=#`, execute these standard analytical SQL statements:

### 1. Audit Raw Events (Populated by PySpark, one row per event, no aggregation delay)
```sql
SELECT * FROM raw_events ORDER BY event_timestamp DESC LIMIT 10;
```

### 2. Audit Live Streaming Aggregates (Populated by PySpark, delayed ~3min by window + watermark)
```sql
SELECT * FROM aggregated_device_metrics ORDER BY window_start DESC LIMIT 10;
```

### 3. Audit Historical Rollup Summaries (Populated by Apache Airflow DAG)
```sql
SELECT * FROM daily_executive_device_summary;
```

### 4. Exit the Database Console Connection
```sql
\q
```