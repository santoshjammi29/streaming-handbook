from pyspark.sql import SparkSession

def start_stream_to_delta(spark: SparkSession, kafka_servers: str, topic: str, delta_path: str, checkpoint_path: str):
    # Stream Kafka events into Delta Lake table
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()

    query = df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(processingTime="10 seconds") \
        .start(delta_path)

    return query
