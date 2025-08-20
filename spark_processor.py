from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, length
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import joblib
import numpy as np

# --- Configuration ---
KAFKA_TOPIC = "logs"
KAFKA_SERVER = "kafka:29092"
ELASTICSEARCH_NODE = "elasticsearch"
ELASTICSEARCH_PORT = "9200"
ELASTICSEARCH_INDEX = "log_analytics"
MODEL_PATH = "/opt/bitnami/spark/models/isolation_forest_model.joblib"

def create_spark_session():
    """Creates and configures a Spark Session."""
    return (
        SparkSession.builder
        .appName("RealTimeLogAnomalyDetection")
        .getOrCreate() # Packages are now submitted via command line
    )

def main():
    """Main function to run the Spark Streaming application."""
    spark = create_spark_session()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    print("Spark Session created. Loading model...")

    model_payload = joblib.load(MODEL_PATH)
    model_broadcast = sc.broadcast(model_payload)
    print("Model loaded and broadcasted successfully.")

    log_schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("level", StringType(), True),
        StructField("source", StringType(), True),
        StructField("message", StringType(), True),
        StructField("request_time_ms", IntegerType(), True)
    ])

    # --- Anomaly Prediction UDF (Updated) ---
    def predict_anomaly(message, request_time, message_length):
        """
        UDF to predict anomalies. Now includes message_length.
        """
        try:
            payload = model_broadcast.value
            model = payload['model']
            vectorizer = payload['vectorizer']
            scaler = payload['scaler']

            message_feature = vectorizer.transform([message]).toarray()
            # Scale both numerical features together
            numerical_features = scaler.transform([[request_time, message_length]])
            
            features = np.hstack([message_feature, numerical_features])
            
            prediction = model.predict(features)[0]
            score = model.decision_function(features)[0]
            
            # Return both the prediction (0 or 1) and the raw score
            return (1 if prediction == -1 else 0, float(score))
        except Exception as e:
            return (0, 0.0)

    # Define a schema for the UDF's return type (a struct with two fields)
    udf_return_schema = StructType([
        StructField("is_anomaly", IntegerType(), False),
        StructField("anomaly_score", FloatType(), False)
    ])
    anomaly_udf = udf(predict_anomaly, udf_return_schema)

    # --- Read from Kafka ---
    kafka_stream_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # --- Process the Stream (Updated) ---
    processed_df = (
        kafka_stream_df.select(from_json(col("value").cast("string"), log_schema).alias("log"))
        .select("log.*")
        # 1. Add the message_length column
        .withColumn("message_length", length(col("message")))
        # 2. Apply the UDF and get back a struct
        .withColumn("prediction", anomaly_udf(col("message"), col("request_time_ms"), col("message_length")))
        # 3. Flatten the struct into top-level columns
        .select("*", "prediction.*")
        .drop("prediction", "message_length") # Clean up intermediate columns
    )

    # --- Write to Elasticsearch ---
    es_query = (
        processed_df.writeStream
        .outputMode("append")
        .format("org.elasticsearch.spark.sql")
        .option("es.resource", ELASTICSEARCH_INDEX)
        .option("checkpointLocation", "/tmp/log_analytics_checkpoint")
        .start()
    )

    print(f"Streaming to Elasticsearch index '{ELASTICSEARCH_INDEX}'. Waiting for data...")
    es_query.awaitTermination()

if __name__ == "__main__":
    main()
