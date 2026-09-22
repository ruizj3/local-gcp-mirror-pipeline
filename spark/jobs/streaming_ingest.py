import os
from urllib.parse import urlparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, BooleanType, TimestampType
)

# Env vars let this run unchanged against Docker Compose (localhost) or Render
# (private service/database hostnames) without code changes. DATABASE_URL (as provided
# by Render's `fromDatabase: property: connectionString`) takes precedence if set.
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

_database_url = os.environ.get("DATABASE_URL")
if _database_url:
    _parsed = urlparse(_database_url)
    DB_HOST = _parsed.hostname
    DB_PORT = str(_parsed.port or 5432)
    DB_NAME = _parsed.path.lstrip("/")
    DB_USER = _parsed.username
    DB_PASSWORD = _parsed.password
else:
    DB_HOST = os.environ.get("DB_HOST", "127.0.0.1")
    DB_PORT = os.environ.get("DB_PORT", "5433")
    DB_NAME = os.environ.get("DB_NAME", "analytics_warehouse")
    DB_USER = os.environ.get("DB_USER", "pipeline_user")
    DB_PASSWORD = os.environ.get("DB_PASSWORD", "pipeline_password")

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
JDBC_PROPERTIES = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver"
}

def main():
    print("🚀 Initializing PySpark Structured Streaming Engine...")

    # 1. Initialize Spark Session with Kafka and Postgres drivers
    # Note: Spark automatically downloads these package coordinators at runtime
    spark = SparkSession.builder \
        .appName("GCP-Mirror-Streaming-Ingestion") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.postgresql:postgresql:42.6.0") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 2. Define the Incoming Data Schema (Matches kafka/mock_producer.py)
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("timestamp", StringType(), True),  # Read as string first to parse cleanly
        StructField("user_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("device", StringType(), True),
        StructField("is_new_device", BooleanType(), True),
        StructField("payment_method", StringType(), True),
        StructField("country", StringType(), True),
        StructField("ip_country", StringType(), True),
        StructField("account_age_days", IntegerType(), True),
        StructField("time_since_last_txn_seconds", DoubleType(), True),
        StructField("txn_count_last_1h", IntegerType(), True),
        StructField("distance_from_home_km", DoubleType(), True),
        StructField("is_fraud", IntegerType(), True),
        StructField("fraud_scenario", StringType(), True),
    ])

    # 3. Read Stream from Local Kafka Broker
    print("📡 Connecting to Kafka topic: 'transactions'...")
    kafka_raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", "transactions") \
        .option("startingOffsets", "latest") \
        .load()

    # 4. Extract and Parse JSON Payloads
    # Kafka sends data as a binary value stream; we cast it to string and parse via schema
    parsed_df = kafka_raw_df \
        .selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("event_timestamp", col("timestamp").cast(TimestampType()))

    # 5. Apply Analytical Aggregation (Tumbling 1-Minute Windows)
    # Tracks transaction volume and fraud counts per merchant category for monitoring dashboards.
    aggregated_df = parsed_df \
        .withWatermark("event_timestamp", "2 minutes") \
        .groupBy(
            window(col("event_timestamp"), "1 minute"),
            col("merchant_category")
        ) \
        .agg({"event_id": "count", "is_fraud": "sum"}) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("merchant_category"),
            col("count(event_id)").alias("total_transactions"),
            col("sum(is_fraud)").alias("fraud_transactions")
        )

    # 6. Raw Event Sink (Bronze layer: every labeled transaction landed untouched, this is the
    # table the ML training pipeline reads from / exports out of).
    raw_events_df = parsed_df.select(
        "event_id", "event_timestamp", "user_id", "amount", "merchant_category", "device",
        "is_new_device", "payment_method", "country", "ip_country", "account_age_days",
        "time_since_last_txn_seconds", "txn_count_last_1h", "distance_from_home_km",
        "is_fraud", "fraud_scenario"
    )

    def write_raw_events(batch_df, batch_id):
        """Persists every parsed transaction as-is, independent of the aggregation sink's failures/lag."""
        if batch_df.count() > 0:
            print(f"🧾 Writing raw micro-batch #{batch_id} ({batch_df.count()} transactions) to fraud_events...")
            batch_df.write \
                .format("jdbc") \
                .option("url", JDBC_URL) \
                .option("dbtable", "fraud_events") \
                .options(**JDBC_PROPERTIES) \
                .mode("append") \
                .save()

    # 7. Aggregated Sink (Silver layer: 1-minute rollups, delayed ~3min by window + watermark)
    def write_to_postgres(batch_df, batch_id):
        """Writes streaming micro-batches out to our relational analytics warehouse."""
        if batch_df.count() > 0:
            print(f"📦 Writing micro-batch #{batch_id} to PostgreSQL analytics warehouse...")
            batch_df.write \
                .format("jdbc") \
                .option("url", JDBC_URL) \
                .option("dbtable", "aggregated_fraud_metrics") \
                .options(**JDBC_PROPERTIES) \
                .mode("append") \
                .save()

    # 8. Start both streaming queries (isolated checkpoints so one sink's failure can't stall the other)
    print("⏳ Streaming queries active. Awaiting micro-batches...")
    raw_query = raw_events_df.writeStream \
        .foreachBatch(write_raw_events) \
        .option("checkpointLocation", "./spark/checkpoints/fraud_events") \
        .start()

    aggregated_query = aggregated_df.writeStream \
        .foreachBatch(write_to_postgres) \
        .option("checkpointLocation", "./spark/checkpoints/fraud_pipeline") \
        .start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
