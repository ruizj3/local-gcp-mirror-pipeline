FROM eclipse-temurin:17-jre-jammy

# The base image supplies the Java runtime required by PySpark.
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN python3 -m pip install --no-cache-dir pyspark==3.4.1

COPY spark/jobs/streaming_ingest.py ./spark/jobs/streaming_ingest.py

CMD ["python3", "spark/jobs/streaming_ingest.py"]
