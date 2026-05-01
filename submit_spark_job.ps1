$ErrorActionPreference = "Stop"

$sparkMaster = "spark://spark-master:7077"
$kafkaPackage = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
$jobPath = "/opt/spark/jobs/traffic_streaming.py"

Write-Host ""
Write-Host "============================================================"
Write-Host "  SOUMISSION DU JOB SPARK STREAMING"
Write-Host "============================================================"
Write-Host ""

$sparkRunning = docker ps --format '{{.Names}}' | Select-String '^spark-master$'
if (-not $sparkRunning) {
    throw "Le conteneur spark-master n'est pas demarre. Lancez d'abord: docker compose up -d --build"
}

Write-Host "Verification de kafka-python-ng dans spark-master..."
docker exec spark-master python3 -c "from kafka import KafkaProducer; print('kafka-python-ng ok')"

Write-Host "Nettoyage des checkpoints et preparation du cache Ivy..."
docker exec spark-master bash -lc "rm -rf /tmp/checkpoints /tmp/.ivy2 && mkdir -p /tmp/checkpoints /tmp/.ivy2/cache"

Write-Host "Soumission du job Spark..."
Write-Host ""

docker exec spark-master bash -lc "env HOME=/tmp /opt/spark/bin/spark-submit --master $sparkMaster --packages $kafkaPackage --conf spark.jars.ivy=/tmp/.ivy2 --conf spark.sql.streaming.checkpointLocation=/tmp/checkpoints --conf spark.sql.shuffle.partitions=1 --conf spark.default.parallelism=1 --conf spark.streaming.stopGracefullyOnShutdown=true --driver-memory 1g --executor-memory 1g --executor-cores 1 $jobPath"
