# Spark image with the Python libraries the anomaly detection job needs.
# bitnami/spark:3.3.1 was removed from Docker Hub in 2025; bitnamilegacy/spark:3.3.1
# is the same image republished by Bitnami.
FROM bitnamilegacy/spark:3.3.1

USER root
COPY requirements-spark.txt /tmp/requirements-spark.txt
RUN pip install --no-cache-dir -r /tmp/requirements-spark.txt && rm /tmp/requirements-spark.txt
USER 1001
