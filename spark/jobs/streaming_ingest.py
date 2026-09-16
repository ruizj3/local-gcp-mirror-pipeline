import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

JDBC_URL = "jdbc:postgresql://127.0.0.1:5433/analytics_warehouse"
JDBC_PROPERTIES = {
    "user": "pipeline_user",
    "password": "pipeline_password",
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

    # 2. Define the Incoming Data Schema (Matches mock_producer.py)
    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("timestamp", StringType(), True),  # Read as string first to parse cleanly
        StructField("user_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("device", StringType(), True)
    ])

    # 3. Read Stream from Local Kafka Broker
    print("📡 Connecting to Kafka topic: 'ecommerce-events'...")
    kafka_raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "ecommerce-events") \
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
    # This architecture aggregates total sales volumes and transactions per device type.
    aggregated_df = parsed_df \
        .withWatermark("event_timestamp", "2 minutes") \
        .groupBy(
            window(col("event_timestamp"), "1 minute"),
            col("device"),
            col("event_type")
        ) \
        .count() \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("device"),
            col("event_type"),
            col("count").alias("total_events")
        )

    # 6. Raw Event Sink (Bronze layer: every event landed untouched, no aggregation/watermark delay)
    raw_events_df = parsed_df.select(
        "event_id", "event_timestamp", "user_id", "event_type", "product_id", "price", "device"
    )

    def write_raw_events(batch_df, batch_id):
        """Persists every parsed event as-is, independent of the aggregation sink's failures/lag."""
        if batch_df.count() > 0:
            print(f"🧾 Writing raw micro-batch #{batch_id} ({batch_df.count()} events) to raw_events...")
            batch_df.write \
                .format("jdbc") \
                .option("url", JDBC_URL) \
                .option("dbtable", "raw_events") \
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
                .option("dbtable", "aggregated_device_metrics") \
                .options(**JDBC_PROPERTIES) \
                .mode("append") \
                .save()

    # 8. Start both streaming queries (isolated checkpoints so one sink's failure can't stall the other)
    print("⏳ Streaming queries active. Awaiting micro-batches...")
    raw_query = raw_events_df.writeStream \
        .foreachBatch(write_raw_events) \
        .option("checkpointLocation", "./spark/checkpoints/raw_events") \
        .start()

    aggregated_query = aggregated_df.writeStream \
        .foreachBatch(write_to_postgres) \
        .option("checkpointLocation", "./spark/checkpoints/ecommerce_pipeline") \
        .start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
