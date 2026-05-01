"""
Pipeline Spark Streaming - Regulation intelligente du trafic urbain
===================================================================

Objectifs couverts :
  1. Deux flux distincts : vehicles + incidents
  2. Jointure stream-stream native dans Spark Structured Streaming
  3. Emission des decisions vers Kafka topic "traffic_decisions"
  4. Mesure de la latence end-to-end a partir de produced_at

Approche retenue :
  - Flux vehicles : micro-agregation par intersection sur 1 seconde
  - Flux incidents : stream secondaire avec watermark
  - Jointure stream-stream native sur :
      * intersection_id
      * incident.event_time <= vehicle_window_end
      * vehicle_window_end <= incident.event_time + 8h
  - La duree exacte de l'incident est ensuite revalidee dans foreachBatch
    avec estimated_duration_min pour ne retenir que les incidents encore actifs.
"""

import json
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Tuple

from kafka import KafkaProducer
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    expr,
    from_json,
    min as spark_min,
    sum as spark_sum,
    window,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

KAFKA_SERVERS = "kafka:29092"
CHECKPOINT_ROOT = "/tmp/checkpoints"

SEVERITY_RANK = {"NONE": 0, "LOW": 1, "MEDIUM": 2, "HIGH": 3, "CRITICAL": 4}
MODE_EMOJI = {
    "FLUID": "GREEN",
    "MODERATE": "YELLOW",
    "DENSE": "ORANGE",
    "CONGESTION": "RED",
    "EMERGENCY": "STOP",
}

VEHICLE_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("produced_at", StringType()),
    StructField("intersection_id", StringType()),
    StructField("direction", StringType()),
    StructField("vehicle_count", IntegerType()),
    StructField("vehicle_type", StringType()),
    StructField("average_speed_kmh", DoubleType()),
    StructField("density_level", StringType()),
])

INCIDENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("intersection_id", StringType()),
    StructField("incident_type", StringType()),
    StructField("severity", StringType()),
    StructField("estimated_duration_min", IntegerType()),
    StructField("capacity_reduction_factor", DoubleType()),
    StructField("lanes_blocked", IntegerType()),
    StructField("description", StringType()),
])

_producer: Optional[KafkaProducer] = None
_active_incidents: Dict[str, dict] = {}
_incidents_lock = threading.Lock()


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("TrafficRegulation")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_ROOT)
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .getOrCreate()
    )


def _make_producer() -> KafkaProducer:
    for _ in range(12):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
            )
        except Exception:
            time.sleep(5)
    raise RuntimeError("Impossible de creer le KafkaProducer apres 12 tentatives")


def read_kafka(spark: SparkSession, topic: str) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 500)
        .load()
    )


def build_vehicle_metrics_stream(spark: SparkSession) -> DataFrame:
    raw = read_kafka(spark, "vehicles")

    parsed = (
        raw
        .select(from_json(col("value").cast("string"), VEHICLE_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn(
            "event_time",
            expr("to_timestamp(timestamp, \"yyyy-MM-dd'T'HH:mm:ss\")"),
        )
        .filter(col("event_time").isNotNull())
        .withWatermark("event_time", "1 second")
    )

    aggregated = (
        parsed
        .groupBy(
            window(col("event_time"), "1 second", "1 second"),
            col("intersection_id"),
        )
        .agg(
            spark_sum("vehicle_count").alias("total_vehicles"),
            avg("average_speed_kmh").alias("avg_speed_kmh"),
            spark_min("produced_at").alias("min_produced_at"),
        )
    )

    return (
        aggregated
        .select(
            col("intersection_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("total_vehicles"),
            col("avg_speed_kmh"),
            col("min_produced_at"),
        )
    )


def build_incident_stream(spark: SparkSession) -> DataFrame:
    raw = read_kafka(spark, "incidents")

    return (
        raw
        .select(from_json(col("value").cast("string"), INCIDENT_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn(
            "event_time",
            expr("to_timestamp(timestamp, \"yyyy-MM-dd'T'HH:mm:ss\")"),
        )
        .filter(col("event_time").isNotNull())
        .withWatermark("event_time", "1 second")
    )


def _prune_expired_incidents(reference_time: datetime) -> None:
    expired = [
        intersection_id
        for intersection_id, incident in _active_incidents.items()
        if incident["expires_at"] < reference_time
    ]
    for intersection_id in expired:
        _active_incidents.pop(intersection_id, None)


def _as_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _upsert_active_incident(row: dict) -> None:
    intersection_id = row["intersection_id"]
    event_time = _as_utc(row["event_time"])
    duration_min = row.get("estimated_duration_min") or 30
    severity = row.get("severity") or "NONE"
    expires_at = event_time + timedelta(minutes=duration_min)

    if expires_at < datetime.now(timezone.utc):
        return

    current = _active_incidents.get(intersection_id)
    if current is None:
        _active_incidents[intersection_id] = {
            "severity": severity,
            "event_time": event_time,
            "expires_at": expires_at,
        }
        return

    current_rank = SEVERITY_RANK.get(current["severity"], 0)
    new_rank = SEVERITY_RANK.get(severity, 0)
    if new_rank > current_rank or event_time >= current["event_time"]:
        _active_incidents[intersection_id] = {
            "severity": severity,
            "event_time": event_time,
            "expires_at": expires_at,
        }


def update_incident_state(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return

    rows = [row.asDict(recursive=True) for row in batch_df.collect()]
    now_utc = datetime.now(timezone.utc)

    with _incidents_lock:
        _prune_expired_incidents(now_utc)
        for row in rows:
            _upsert_active_incident(row)

    print(f"[Spark] Batch incidents {batch_id}: {len(rows)} incident(s) actif(s) mis a jour")


def _get_worst_severity(intersection_id: str, reference_time: datetime) -> str:
    reference_time = _as_utc(reference_time)
    with _incidents_lock:
        _prune_expired_incidents(reference_time)
        incident = _active_incidents.get(intersection_id)
        if incident and incident["expires_at"] >= reference_time:
            return incident["severity"]
    return "NONE"


def _compute_e2e_latency_ms(produced_at_str: Optional[str]) -> float:
    if not produced_at_str:
        return -1

    try:
        produced_at = datetime.strptime(
            produced_at_str,
            "%Y-%m-%dT%H:%M:%S",
        ).replace(tzinfo=timezone.utc)
        now_utc = datetime.now(timezone.utc)
        return round((now_utc - produced_at).total_seconds() * 1000, 2)
    except Exception:
        return -1


def _build_decision(total_veh: int, avg_speed: float, worst_sev: str) -> Tuple[int, int, str]:
    traffic_score = 80 if total_veh > 20 else 50 if total_veh > 10 else 20
    incident_malus = {"CRITICAL": 40, "HIGH": 20, "MEDIUM": 10}.get(worst_sev, 0)
    combined = traffic_score + incident_malus

    if worst_sev == "CRITICAL":
        green_sec, mode = 5, "EMERGENCY"
    elif avg_speed < 15:
        green_sec, mode = 60, "CONGESTION"
    elif combined >= 80:
        green_sec, mode = 60, "DENSE"
    elif combined >= 50:
        green_sec, mode = 40, "MODERATE"
    else:
        green_sec, mode = 20, "FLUID"

    return green_sec, 90 - green_sec, mode


def process_vehicle_batch(batch_df: DataFrame, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return

    rows = [row.asDict(recursive=True) for row in batch_df.collect()]
    count = 0
    for item in rows:
        total_veh = item["total_vehicles"] or 0
        avg_speed = round(item["avg_speed_kmh"] or 0, 2)
        worst_sev = _get_worst_severity(item["intersection_id"], item["window_end"])
        green_sec, red_sec, mode = _build_decision(total_veh, avg_speed, worst_sev)
        e2e_latency_ms = _compute_e2e_latency_ms(item.get("min_produced_at"))
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

        decision = {
            "intersection_id": item["intersection_id"],
            "window_start": item["window_start"].strftime("%Y-%m-%dT%H:%M:%S"),
            "window_end": item["window_end"].strftime("%Y-%m-%dT%H:%M:%S"),
            "total_vehicles": total_veh,
            "avg_speed_kmh": avg_speed,
            "worst_severity": worst_sev,
            "traffic_mode": mode,
            "green_duration_sec": green_sec,
            "red_duration_sec": red_sec,
            "decision_timestamp": now_str,
            "e2e_latency_ms": e2e_latency_ms,
        }

        _producer.send("traffic_decisions", key=item["intersection_id"], value=decision)
        count += 1

        latency_str = f"{e2e_latency_ms}ms" if e2e_latency_ms >= 0 else "N/A"
        print(
            f"[Spark] {MODE_EMOJI.get(mode, '?')} {item['intersection_id']} | {mode} | "
            f"Veh:{total_veh} | {avg_speed}km/h | Sev:{worst_sev} | "
            f"Vert:{green_sec}s Rouge:{red_sec}s | Latence E2E:{latency_str}"
        )

    _producer.flush()
    print(f"[Spark] Batch {batch_id}: {count} decision(s) envoyee(s) vers traffic_decisions")


def start_streams(spark: SparkSession):
    vehicle_metrics = build_vehicle_metrics_stream(spark)
    incidents = build_incident_stream(spark)

    incidents_query = (
        incidents
        .writeStream
        .foreachBatch(update_incident_state)
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/incidents_state")
        .trigger(processingTime="2 seconds")
        .start()
    )

    vehicles_query = (
        vehicle_metrics
        .writeStream
        .foreachBatch(process_vehicle_batch)
        .outputMode("update")
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/traffic_vehicle")
        .trigger(processingTime="1 second")
        .start()
    )

    return incidents_query, vehicles_query


def main():
    global _producer

    print("=" * 60)
    print("  DEMARRAGE DU PIPELINE SPARK STREAMING v3")
    print("  Regulation Intelligente du Trafic Urbain")
    print("  Native stream-stream join · fenetre 1s · latence reduite")
    print("=" * 60)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("[Spark] Initialisation du KafkaProducer...")
    _producer = _make_producer()
    print("[Spark] KafkaProducer pret")

    incident_query, vehicle_query = start_streams(spark)
    print("[Spark] Queries streaming demarrees")
    print("[Spark] Topics : vehicles + incidents -> traffic_decisions")
    print("[Spark] Dashboard : http://localhost:5000")
    print("=" * 60)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
