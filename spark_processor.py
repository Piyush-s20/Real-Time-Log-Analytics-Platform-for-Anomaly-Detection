# spark_processor.py (v3)
# Reads logs from Kafka, scores them with the Isolation Forest model in a
# vectorized pandas UDF and writes the results to Elasticsearch.

import json
import logging
import os
import threading
import time
import urllib.request

import joblib
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, length, pandas_udf, to_timestamp
from pyspark.sql.types import (BooleanType, DoubleType, IntegerType, StringType,
                               StructField, StructType)

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "logs")
KAFKA_SERVER = os.getenv("KAFKA_SERVER", "kafka:29092")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "latest")
ELASTICSEARCH_NODE = os.getenv("ELASTICSEARCH_NODE", "elasticsearch")
ELASTICSEARCH_PORT = os.getenv("ELASTICSEARCH_PORT", "9200")
ELASTICSEARCH_INDEX = os.getenv("ELASTICSEARCH_INDEX", "log_analytics")
MODEL_PATH = os.getenv("MODEL_PATH", "/opt/bitnami/spark/models/isolation_forest_model.joblib")
# Both paths are bind-mounted from the host (see docker-compose.yml)
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/opt/spark-data/checkpoints/log_analytics")
PROGRESS_LOG = os.getenv("PROGRESS_LOG", "/opt/spark-data/metrics/stream_progress.jsonl")
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "5 seconds")
MAX_OFFSETS_PER_TRIGGER = os.getenv("MAX_OFFSETS_PER_TRIGGER")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("spark_processor")

LOG_SCHEMA = StructType([
    StructField("timestamp", StringType(), True),
    StructField("level", StringType(), True),
    StructField("source", StringType(), True),
    StructField("message", StringType(), True),
    StructField("request_time_ms", IntegerType(), True),
    StructField("injected_anomaly", BooleanType(), True),
])

PREDICTION_SCHEMA = StructType([
    StructField("is_anomaly", IntegerType(), True),
    StructField("anomaly_score", DoubleType(), True),
])


# es-hadoop writes TimestampType as epoch millis, which dynamic mapping would store as
# "long". An explicit mapping makes it a real date field for Kibana.
INDEX_TEMPLATE = {
    "index_patterns": [f"{ELASTICSEARCH_INDEX}*"],
    "template": {"mappings": {"properties": {
        "timestamp": {"type": "date"},
        "level": {"type": "keyword"},
        "source": {"type": "keyword"},
        "message": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
        "request_time_ms": {"type": "integer"},
        "injected_anomaly": {"type": "boolean"},
        "is_anomaly": {"type": "integer"},
        "anomaly_score": {"type": "float"},
    }}},
}


def ensure_index_template():
    """Creates/updates the index template before the first write (idempotent)."""
    url = f"http://{ELASTICSEARCH_NODE}:{ELASTICSEARCH_PORT}/_index_template/{ELASTICSEARCH_INDEX}"
    request = urllib.request.Request(url, data=json.dumps(INDEX_TEMPLATE).encode("utf-8"), method="PUT",
                                     headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=30) as response:
        logger.info("Index template '%s' installed: %s", ELASTICSEARCH_INDEX, response.read().decode())


def create_spark_session():
    """Creates and configures a Spark Session."""
    return (
        SparkSession.builder
        .appName("RealTimeLogAnomalyDetection")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()  # Packages are submitted via scripts/submit_spark_job.sh
    )


def make_anomaly_udf(model_broadcast):
    """Builds a vectorized UDF that scores a whole Arrow batch at once."""

    @pandas_udf(PREDICTION_SCHEMA)
    def predict_anomaly(message: pd.Series, request_time: pd.Series, message_length: pd.Series) -> pd.DataFrame:
        result = pd.DataFrame({
            "is_anomaly": pd.Series([None] * len(message), dtype="Int32"),
            "anomaly_score": pd.Series([np.nan] * len(message), dtype="float64"),
        })
        # Rows with missing fields (e.g. malformed JSON) cannot be scored -> null
        valid = message.notna() & request_time.notna() & message_length.notna()
        if not valid.any():
            return result
        try:
            payload = model_broadcast.value
            message_features = payload["vectorizer"].transform(message[valid]).toarray()
            numerical = np.column_stack([request_time[valid].astype(float), message_length[valid].astype(float)])
            features = np.hstack([message_features, payload["scaler"].transform(numerical)])

            predictions = payload["model"].predict(features)
            scores = payload["model"].decision_function(features)
            result.loc[valid.values, "is_anomaly"] = (predictions == -1).astype("int32")
            result.loc[valid.values, "anomaly_score"] = scores
        except Exception:
            # Do not hide failures behind fake "normal" predictions: log and emit nulls
            logging.getLogger("spark_processor.udf").exception(
                "Anomaly prediction failed for a batch of %d rows", int(valid.sum()))
        if (~valid).any():
            logging.getLogger("spark_processor.udf").warning(
                "%d rows had missing fields and were not scored", int((~valid).sum()))
        return result

    return predict_anomaly


def log_progress(query, path, poll_seconds=5):
    """Appends each new micro-batch progress report to a JSONL file."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    last_batch = None
    while query.isActive:
        for progress in query.recentProgress:
            if last_batch is not None and progress["batchId"] <= last_batch:
                continue
            last_batch = progress["batchId"]
            record = {
                "timestamp": progress["timestamp"],
                "batchId": progress["batchId"],
                "numInputRows": progress["numInputRows"],
                "inputRowsPerSecond": progress.get("inputRowsPerSecond"),
                "processedRowsPerSecond": progress.get("processedRowsPerSecond"),
                "durationMs": progress.get("durationMs"),
            }
            with open(path, "a") as f:
                f.write(json.dumps(record) + "\n")
            if progress["numInputRows"]:
                logger.info("Batch %s: %s rows, input %.1f rows/s, processed %.1f rows/s",
                            record["batchId"], record["numInputRows"],
                            record["inputRowsPerSecond"] or 0, record["processedRowsPerSecond"] or 0)
        time.sleep(poll_seconds)


def main():
    """Main function to run the Spark Streaming application."""
    spark = create_spark_session()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    logger.info("Spark Session created. Loading model from %s", MODEL_PATH)
    model_broadcast = sc.broadcast(joblib.load(MODEL_PATH))
    logger.info("Model loaded and broadcast successfully.")
    anomaly_udf = make_anomaly_udf(model_broadcast)
    ensure_index_template()

    # --- Read from Kafka ---
    reader = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
    )
    if MAX_OFFSETS_PER_TRIGGER:
        reader = reader.option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
    kafka_stream_df = reader.load()

    # --- Process the Stream ---
    processed_df = (
        kafka_stream_df.select(from_json(col("value").cast("string"), LOG_SCHEMA).alias("log"))
        .select("log.*")
        # Real timestamp type so Kibana can use it as the time field
        .withColumn("timestamp", to_timestamp(col("timestamp")))
        .withColumn("message_length", length(col("message")))
        .withColumn("prediction", anomaly_udf(col("message"), col("request_time_ms"), col("message_length")))
        .select("*", "prediction.*")
        .drop("prediction", "message_length")
    )

    # --- Write to Elasticsearch ---
    es_query = (
        processed_df.writeStream
        .outputMode("append")
        .format("org.elasticsearch.spark.sql")
        .option("es.nodes", ELASTICSEARCH_NODE)
        .option("es.port", ELASTICSEARCH_PORT)
        .option("es.nodes.wan.only", "true")
        .option("es.resource", ELASTICSEARCH_INDEX)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )

    # PySpark 3.3 has no Python StreamingQueryListener, so poll progress in a thread
    threading.Thread(target=log_progress, args=(es_query, PROGRESS_LOG), daemon=True).start()

    logger.info("Streaming to Elasticsearch index '%s' at %s:%s. Waiting for data...",
                ELASTICSEARCH_INDEX, ELASTICSEARCH_NODE, ELASTICSEARCH_PORT)
    es_query.awaitTermination()


if __name__ == "__main__":
    main()
