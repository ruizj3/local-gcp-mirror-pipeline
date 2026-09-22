FROM python:3.10-slim

# PySpark needs a JRE at runtime.
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --no-cache-dir pyspark==3.4.1

COPY spark/jobs/streaming_ingest.py ./spark/jobs/streaming_ingest.py

CMD ["python", "spark/jobs/streaming_ingest.py"]
