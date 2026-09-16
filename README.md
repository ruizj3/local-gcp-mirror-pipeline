# Local Data Pipeline Platform (GCP Architecture Mirror)

This project functions as a local, zero-cost framework matching the core architectural paradigms tested on the Google Cloud Professional Data Engineer certification exam.

## Architecture Mapping
- **Ingestion:** Apache Kafka (Concept: **GCP Pub/Sub**)
- **Transformation:** Python / PySpark / Apache Beam (Concept: **GCP Dataflow / Dataproc**)
- **Orchestration:** Apache Airflow (Concept: **GCP Cloud Composer**)
- **Data Warehouse:** PostgreSQL (Concept: **GCP BigQuery / Cloud SQL**)

## Getting Started
1. Run `docker-compose up -d` to launch the environment.
2. Access the Airflow UI at `http://localhost:8080` (Credentials: admin/admin).