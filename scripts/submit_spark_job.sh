#!/usr/bin/env bash
# Submits spark_processor.py to the standalone cluster (spark://spark-master:7077).
# The driver runs inside the spark-master container in client mode.
#
# Usage: ./scripts/submit_spark_job.sh            (runs in the foreground)
#        STARTING_OFFSETS=earliest ./scripts/submit_spark_job.sh
set -euo pipefail

# Stop Git Bash on Windows from rewriting /opt/... paths
export MSYS_NO_PATHCONV=1

# Connectors compatible with Spark 3.3.1 / Scala 2.12 and Elasticsearch 8.5.0
PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.5.0"

# Allocate a TTY only when run interactively (so it also works from CI / background jobs)
TTY_FLAGS="-i"
[ -t 0 ] && [ -t 1 ] && TTY_FLAGS="-it"

docker exec $TTY_FLAGS \
  -e STARTING_OFFSETS="${STARTING_OFFSETS:-latest}" \
  -e TRIGGER_INTERVAL="${TRIGGER_INTERVAL:-5 seconds}" \
  ${MAX_OFFSETS_PER_TRIGGER:+-e MAX_OFFSETS_PER_TRIGGER="$MAX_OFFSETS_PER_TRIGGER"} \
  spark-master \
  spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --packages "$PACKAGES" \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    --conf spark.executor.memory=1g \
    --conf spark.cores.max=4 \
    --conf spark.sql.shuffle.partitions=4 \
    /opt/spark-apps/spark_processor.py
