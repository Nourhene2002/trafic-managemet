#!/bin/bash

set -euo pipefail

SPARK_MASTER="spark://spark-master:7077"
KAFKA_PACKAGE="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
JOB_PATH="/opt/spark/jobs/traffic_streaming.py"

echo ""
echo "============================================================"
echo "  SOUMISSION DU JOB SPARK STREAMING"
echo "============================================================"
echo ""

if ! docker ps --format '{{.Names}}' | grep -q '^spark-master$'; then
  echo "Le conteneur spark-master n'est pas demarre."
  echo "Lancez d'abord : docker compose up -d --build"
  exit 1
fi

echo "Verification de kafka-python-ng dans spark-master..."
docker exec spark-master python3 -c "from kafka import KafkaProducer; print('kafka-python-ng ok')"

echo "Nettoyage des checkpoints et preparation du cache Ivy..."
docker exec spark-master bash -lc "rm -rf /tmp/checkpoints /tmp/.ivy2 && mkdir -p /tmp/checkpoints /tmp/.ivy2/cache"

echo "Soumission du job Spark..."
echo ""

docker exec spark-master bash -lc "
  env HOME=/tmp /opt/spark/bin/spark-submit \
    --master '$SPARK_MASTER' \
    --packages '$KAFKA_PACKAGE' \
    --conf 'spark.jars.ivy=/tmp/.ivy2' \
    --conf 'spark.sql.streaming.checkpointLocation=/tmp/checkpoints' \
    --conf 'spark.sql.shuffle.partitions=1' \
    --conf 'spark.default.parallelism=1' \
    --conf 'spark.streaming.stopGracefullyOnShutdown=true' \
    --driver-memory 1g \
    --executor-memory 1g \
    --executor-cores 1 \
    '$JOB_PATH'
"
