"""Read NASA access-log records from Kafka and display parsed micro-batches."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_trunc, length, regexp_extract, to_timestamp, trim, when


LOG_PATTERN = r'^(\S+) \S+ \S+ \[([^\]]+)\] "(\S+)\s+(\S+)(?:\s+[^\"]+)?" (\d{3}) (\d+|-)$'

spark = SparkSession.builder.appName("NASAAccessLogStream").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

messages = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "nasa-access-logs")
    .option("startingOffsets", "earliest")
    .load()
)

raw = messages.selectExpr("CAST(value AS STRING) AS raw_log", "partition", "offset", "timestamp AS ingested_at")
parsed = raw.select(
    "raw_log",
    "partition",
    "offset",
    "ingested_at",
    regexp_extract("raw_log", LOG_PATTERN, 1).alias("host"),
    regexp_extract("raw_log", LOG_PATTERN, 2).alias("timestamp_text"),
    regexp_extract("raw_log", LOG_PATTERN, 3).alias("method"),
    regexp_extract("raw_log", LOG_PATTERN, 4).alias("path"),
    regexp_extract("raw_log", LOG_PATTERN, 5).cast("int").alias("status"),
    regexp_extract("raw_log", LOG_PATTERN, 6).alias("bytes_text"),
).withColumn(
    "event_time", to_timestamp("timestamp_text", "dd/MMM/yyyy:HH:mm:ss Z")
).withColumn(
    "response_bytes", when(col("bytes_text") != "-", col("bytes_text").cast("long"))
).withColumn(
    "valid", (length(trim(col("host"))) > 0) & col("event_time").isNotNull()
)

def analyze_batch(batch, batch_id):
    """Inspect one micro-batch; these counts are per batch, not cumulative."""
    batch.persist()
    try:
        total = batch.count()
        valid = batch.filter(col("valid"))
        valid_count = valid.count()
        (
            valid.select("partition", "offset", "host", "event_time", "ingested_at", "method", "path", "status", "response_bytes", "raw_log")
            .write.format("org.apache.spark.sql.cassandra")
            .options(keyspace="log_analysis", table="events_by_partition")
            .mode("append")
            .save()
        )
        print(f"\nBatch {batch_id}: {total} messages, {valid_count} valid, {total - valid_count} invalid", flush=True)
        print(f"Repeated raw lines in this batch: {total - batch.select('raw_log').distinct().count()}", flush=True)
        print("Requests by hour (UTC):", flush=True)
        valid.groupBy(date_trunc("hour", "event_time").alias("hour_utc")).count().orderBy("hour_utc").show(20, truncate=False)
        print("Requests by HTTP status:", flush=True)
        valid.groupBy("status").count().orderBy("status").show(20, truncate=False)
        print("Most requested paths:", flush=True)
        valid.groupBy("path").count().orderBy(col("count").desc(), "path").show(10, truncate=False)
        if total != valid_count:
            print("Sample invalid records:", flush=True)
            batch.filter(~col("valid")).select("partition", "offset", "raw_log").show(10, truncate=False)
    finally:
        batch.unpersist()


query = (
    parsed.writeStream.foreachBatch(analyze_batch)
    .option("checkpointLocation", "/checkpoints/nasa-cassandra-v2")
    .start()
)
query.awaitTermination()
