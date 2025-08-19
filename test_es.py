# test_es.py
# A simple script to test the connection between Spark and Elasticsearch.

from pyspark.sql import SparkSession

def main():
    """Tests writing a simple DataFrame to Elasticsearch."""
    
    spark = (
        SparkSession.builder
        .appName("ElasticsearchConnectionTest")
        .config("spark.es.nodes", "elasticsearch")
        .config("spark.es.port", "9200")
        .config("spark.es.nodes.wan.only", "true")
        .getOrCreate()
    )
    
    sc = spark.sparkContext
    sc.setLogLevel("INFO") # Set to INFO to see more details

    print("\n--- Spark Session Created. Creating Test DataFrame. ---\n")

    # Create a simple DataFrame
    test_data = [("test_user", "This is a test message.")]
    columns = ["user", "message"]
    df = spark.createDataFrame(test_data, columns)

    print("\n--- DataFrame Created. Attempting to write to Elasticsearch... ---\n")
    df.show()

    try:
        # Try to write the DataFrame to a new index
        df.write \
          .format("org.elasticsearch.spark.sql") \
          .option("es.resource", "test_index/_doc") \
          .mode("overwrite") \
          .save()
        
        print("\n--- SUCCESS! Successfully wrote data to Elasticsearch. ---\n")

    except Exception as e:
        print("\n--- FAILED to write to Elasticsearch. See error below: ---\n")
        print(e)
    
    spark.stop()

if __name__ == "__main__":
    main()
