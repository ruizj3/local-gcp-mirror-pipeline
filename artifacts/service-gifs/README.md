# Pipeline GIFs

Infrastructure startup GIFs:

- `zookeeper.gif`
- `kafka.gif`
- `postgres.gif`
- `adminer.gif`
- `airflow.gif`

Live pipeline output:

- `producer-live.gif` shows transactions emitted by `kafka/mock_producer.py`.
- `worker-postgres-live.gif` shows rows persisted by Spark into PostgreSQL.
- `adminer-fraud-events.gif` shows the Adminer fraud-event query result.
- `airflow-dag.gif` shows the Airflow DAG graph UI.

Regenerate live output with:

    ./scripts/capture_pipeline_output_gifs.sh artifacts/service-gifs
