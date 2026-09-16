# Real-Time Event-Driven Data Platform (GCP Architecture Mirror)

An independent, production-grade, zero-cloud-cost data platform built entirely on local container infrastructure. This architecture simulates the distributed data ingestion, stream processing, workflow orchestration, and analytical storage paradigms tested on the **Google Cloud Certified Professional Data Engineer** examination.

---

## 🏗️ Architectural Mapping

This project intentionally substitutes proprietary Google Cloud managed products with their native, open-source structural equivalents to master platform engineering concepts under heavy loads without cloud spend:

| Google Cloud Platform Component | Local Open-Source Substitute | Architectural Responsibility |
| :--- | :--- | :--- |
| **Google Cloud Pub/Sub** | Apache Kafka | Fault-tolerant asynchronous message broker for ingestion |
| **Google Cloud Dataflow (Apache Beam)** | PySpark Structured Streaming | Micro-batch parsing, tumbling window calculations, and late-data watermarking |
| **Google Cloud Cloud Composer (Airflow)** | Apache Airflow 2.7 | Batch management, multi-stage DAG task dependencies, and database optimization |
| **Google Cloud BigQuery / Cloud SQL** | PostgreSQL 15 | Relational analytical warehouse structured with optimized dimensional metrics |
| **GCP Cloud IAM & VPC Networks** | Docker Networking & Env Configurations | Secure internal routing using isolated containers and service accounts mapping |

---

## 🧬 Platform Components & Code Layout

- **`/kafka/mock_producer.py`**: An asynchronous message streaming simulator that generates a continuous, real-time volume of multi-device e-commerce event traffic payloads (`view_item`, `add_to_cart`, `purchase`).
- **`/spark/jobs/streaming_ingest.py`**: A robust PySpark Structured Streaming consumer engine mapping structural JSON telemetry schemas, establishing a **2-minute fault tolerance watermark**, grouping streaming records into **1-minute tumbling windows**, and handling analytical state commits via distributed checkpoint configurations.
- **`/airflow/dags/data_pipeline_dag.py`**: A multi-stage orchestration workflow handling transactional database verification safeguards, automated historical data rollup aggregation queries, and active partition lifecycle purging strategies matching GCP cost-governance practices.

---

## 🛠️ Infrastructure Core Stack Setup

### 1. Prerequisites & Virtual Environment Initialization
Ensure you have Docker Desktop and Python 3.10+ installed on your host system:

```bash
# Clone the repository and initialize the Python runtime environment
python3 -m venv .venv
source .venv/bin/activate

# Install locked client-side messaging and computing frameworks
pip install -r requirements.txt
```

### 2. Booting the Multi-Container Cluster
Spin up your message brokers, orchestration engine, and storage layer securely inside Docker:

```bash
docker-compose up -d
```
*Verify containers are running cleanly using `docker-compose ps`.*

---

## 🚀 Execution & Verification Pipelines

To view the live, end-to-end data pipeline operations processing telemetries across your system architecture, launch three separate terminal windows with your virtual environment (`.venv`) activated:

### Step A: Initialize Inbound Messaging Ingestion
Start the real-time event generator to pump synthetic clickstream payloads into the local message broker:
```bash
python kafka/mock_producer.py
```

### Step B: Launch the Real-Time Transformation Stream
Execute the streaming analytics application. PySpark will automatically connect to Kafka, apply schema parsing structures, calculate window matrices, and pipe streams directly into PostgreSQL:
```bash
python spark/jobs/streaming_ingest.py
```

### Step C: Execute Batch Governance & Analytical Orchestration
1. Open your web browser and navigate to the Airflow UI at `http://localhost:8080` (Credentials: `admin`/`admin`).
2. Navigate to **Admin** ➡️ **Connections** and create a connection with ID `postgres_warehouse` pointing to host `postgres`, port `5432`, database `analytics_warehouse`, user `pipeline_user`, and password `pipeline_password`.
3. Un-toggle the active status switch for the `warehouse_daily_aggregation_pipeline` DAG and trigger it manually to audit analytical views and run partition expirations.

---

## 🎯 Exam Blueprint Concepts Mastered

- **Data Lifecycle Governance:** Mastered data retention protocols by building a custom Airflow pipeline worker task that actively purges metric instances older than 30 days—directly demonstrating core mastery of **BigQuery table partition expiration rules**.
- **Late-Arriving Stream Processing:** Integrated structured state mechanics (`.withWatermark()`) inside PySpark to establish dynamic event time boundaries, handling late network telemetries exactly how **Google Cloud Dataflow (Apache Beam)** handles out-of-order streams.
- **Reliability & Recovery Design:** Utilized isolated streaming `checkpointLocation` paths to manage transactional system states, mirroring high-availability cloud migration architectures.

📥 1. Data Ingestion & Messaging (Concept: GCP Pub/Sub)Asynchronous Decoupling: How a message broker absorbs bursts of incoming traffic so downstream engines don't crash.Topic vs. Subscription: Organizing streams into channels (topics) and understanding how consumer groups read from them.At-Least-Once Delivery: Why message brokers guarantee data won't be lost, and why your downstream code must handle occasional duplicate messages (idempotency).
⚙️ 2. Stream Processing (Concept: GCP Dataflow / Apache Beam)Event Time vs. Processing Time: The difference between when an action actually happened on a user's phone vs. when it arrived at your server.Streaming Windows:Tumbling (Fixed): Non-overlapping time blocks (e.g., every 60 seconds cleanly).Sliding (Hopping): Overlapping time blocks (e.g., 5-minute windows that recalculate every 1 minute).Watermarking: Setting a structural timeline threshold to tell the system when to stop waiting for late data and close a window.Stateful Checkpointing: Saving pipeline metadata to a persistent directory so the pipeline can resume processing instantly if a cluster node fails.
🗄️ 3. Storage Optimization & Analytics (Concept: GCP BigQuery)Dimensional Modeling: Structuring warehouses into Star Schemas (Fact tables for transactions, Dimension tables for users/devices) to maximize analytical speed.Partitioning: Splitting massive tables by a date column so queries only scan data within a specific timeframe, dramatically cutting costs.Clustering: Sorting data within those partitions based on frequently filtered columns (like device or event_type) to speed up search lookups.
🎼 4. Orchestration & Maintenance (Concept: GCP Cloud Composer / Airflow)DAG Design: Building Directed Acyclic Graphs to ensure tasks execute in a strict, logical sequence (e.g., Don't calculate reports until the warehouse table is verified).Idempotency in ETL: Designing tasks so that if a daily pipeline fails halfway through and reruns, it won't accidentally double-count or corrupt your warehouse data.Data Governance / Lifecycle: Automatically pruning or archiving old data to minimize underlying infrastructure storage costs.