"""
Exactly-Once Kafka → Delta Lake with PySpark Structured Streaming
=================================================================
Handbook: R06 streaming-handbook | Module 09: End-to-End Exactly-Once
Cross-links:
  - Architecture: https://github.com/santoshjammi29/data-system-design-handbook/tree/main/modules/06_realtime_event_driven_lakehouse
  - Empirical Lab: https://github.com/santoshjammi29/spark-experiments/tree/main/spark/1_data_skew
  - Interview Defense: https://github.com/santoshjammi29/data-engineering-interview-handbook/blob/main/pillars/16_apache_flink_streaming.md
  - Web Practice: https://moderndataengineer.uk/practice/streaming
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, expr, window
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, TimestampType, DoubleType
)

# ── Schema Definition ─────────────────────────────────────────────────────────
PAYMENT_SCHEMA = StructType([
    StructField("payment_id",   StringType(),    nullable=False),
    StructField("account_id",   StringType(),    nullable=False),
    StructField("amount",       DoubleType(),    nullable=False),
    StructField("currency",     StringType(),    nullable=True),
    StructField("event_time",   TimestampType(), nullable=False),
    StructField("merchant_id",  StringType(),    nullable=True),
    StructField("status",       StringType(),    nullable=True),
])

# ── SparkSession with RocksDB State Store ─────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("ExactlyOnce_Kafka_to_Delta")
    # Off-heap RocksDB state store (production recommended)
    .config("spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
    .config("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")
    .config("spark.sql.shuffle.partitions", "200")
    # Trigger-once style checkpointing
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

KAFKA_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC     = "payments.raw"
CHECKPOINT_DIR  = "dbfs:/checkpoints/payments_exactly_once"
DELTA_PATH      = "dbfs:/lakehouse/silver/payments"


def read_kafka_stream():
    """Read from Kafka with idempotent consumer offsets."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("kafka.isolation.level", "read_committed")  # EOS: skip uncommitted
        .option("kafka.enable.auto.commit", "false")
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_and_enrich(raw_df):
    """Deserialize JSON payload and add audit columns."""
    return (
        raw_df
        .select(
            from_json(col("value").cast("string"), PAYMENT_SCHEMA).alias("data"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .select(
            "data.*",
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_timestamp"),
            current_timestamp().alias("processed_at"),
        )
        # Watermark: tolerate 10-minute late arrivals
        .withWatermark("event_time", "10 minutes")
    )


def write_to_delta(df):
    """Write to Delta Lake with exactly-once guarantees via idempotent sink."""
    return (
        df.writeStream
        .format("delta")
        .outputMode("append")
        # Checkpoint ensures exactly-once: re-plays from last committed offset
        .option("checkpointLocation", CHECKPOINT_DIR)
        # Delta Lake idempotent write: txn.appId + txn.version
        .option("txnAppId", "payments-streaming-job")
        .option("txnVersion", "1")
        .trigger(processingTime="30 seconds")
        .start(DELTA_PATH)
    )


if __name__ == "__main__":
    raw   = read_kafka_stream()
    clean = parse_and_enrich(raw)
    query = write_to_delta(clean)

    print(f"Streaming query started: {query.id}")
    print(f"Checkpoint: {CHECKPOINT_DIR}")
    print(f"Delta sink: {DELTA_PATH}")

    query.awaitTermination()
