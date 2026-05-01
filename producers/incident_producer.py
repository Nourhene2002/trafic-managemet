"""
Producteur d'incidents routiers.
Simule des accidents, travaux, et événements spéciaux qui affectent le trafic.

FIX: timestamp ISO 8601 strict pour compatibilité Spark CAST AS TIMESTAMP.
FIX: retry Kafka avec backoff exponentiel.
FIX: type hint `dict | None` → `Optional[dict]` pour Python < 3.10.
"""

import json
import random
import time
import os
from datetime import datetime, timedelta, timezone
from typing import Optional
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("TOPIC", "incidents")

INTERSECTIONS = [
    "INT_CENTRE_VILLE",
    "INT_GARE_NORD",
    "INT_PLACE_REPUBLIQUE",
    "INT_AVENUE_PRINCIPALE",
    "INT_MARCHE_CENTRAL",
    "INT_QUARTIER_EST",
    "INT_ZONE_INDUSTRIELLE",
    "INT_CAMPUS_UNIV",
]

INCIDENT_TYPES = {
    "ACCIDENT":          {"severity": "HIGH",     "duration_min": 15,  "duration_max": 60,  "freq": 0.05},
    "TRAVAUX":           {"severity": "MEDIUM",   "duration_min": 60,  "duration_max": 480, "freq": 0.03},
    "VEHICULE_EN_PANNE": {"severity": "LOW",      "duration_min": 10,  "duration_max": 30,  "freq": 0.08},
    "MANIFESTATION":     {"severity": "HIGH",     "duration_min": 60,  "duration_max": 180, "freq": 0.02},
    "INONDATION":        {"severity": "CRITICAL", "duration_min": 30,  "duration_max": 120, "freq": 0.01},
    "CONTROLE_POLICE":   {"severity": "MEDIUM",   "duration_min": 15,  "duration_max": 45,  "freq": 0.06},
}

CAPACITY_REDUCTION = {
    "LOW":      (0.1, 0.2),
    "MEDIUM":   (0.3, 0.5),
    "HIGH":     (0.5, 0.8),
    "CRITICAL": (0.8, 1.0),
}


def generate_incident_event() -> Optional[dict]:
    """Génère un incident aléatoire avec une faible probabilité."""
    # ~15% de chance d'incident à chaque appel
    if random.random() > 0.15:
        return None

    incident_type = random.choices(
        list(INCIDENT_TYPES.keys()),
        weights=[v["freq"] for v in INCIDENT_TYPES.values()]
    )[0]

    config = INCIDENT_TYPES[incident_type]
    intersection_id = random.choice(INTERSECTIONS)
    duration = random.randint(config["duration_min"], config["duration_max"])

    start_time = datetime.now(timezone.utc)
    end_time = start_time + timedelta(minutes=duration)

    lo, hi = CAPACITY_REDUCTION[config["severity"]]
    capacity_reduction = round(random.uniform(lo, hi), 2)

    # FIX: format timestamp compatible Spark (pas de 'Z')
    ts_fmt = "%Y-%m-%dT%H:%M:%S"

    return {
        "event_id": f"INC_{intersection_id}_{int(time.time()*1000)}",
        "timestamp": start_time.strftime(ts_fmt),
        "intersection_id": intersection_id,
        "incident_type": incident_type,
        "severity": config["severity"],
        "estimated_duration_min": duration,
        "capacity_reduction_factor": capacity_reduction,
        "lanes_blocked": random.randint(1, 2) if config["severity"] in ("HIGH", "CRITICAL") else 0,
        "description": f"{incident_type} signalé à {intersection_id}",
    }


def create_producer() -> KafkaProducer:
    delay = 5
    for attempt in range(12):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
                retries=3,
            )
            print(f"[IncidentProducer] ✅ Connecté à Kafka: {KAFKA_SERVERS}")
            return producer
        except NoBrokersAvailable:
            print(f"[IncidentProducer] ⏳ Kafka pas prêt (tentative {attempt+1}/12), attente {delay}s...")
            time.sleep(delay)
            delay = min(delay * 2, 30)
    raise RuntimeError("Impossible de se connecter à Kafka après 12 tentatives")


def main():
    print(f"[IncidentProducer] Démarrage, broker: {KAFKA_SERVERS}, topic: {TOPIC}")
    producer = create_producer()
    print(f"[IncidentProducer] ⚠️  Simulation incidents démarrée sur le topic '{TOPIC}'")

    while True:
        incident = generate_incident_event()
        if incident:
            producer.send(TOPIC, key=incident["intersection_id"], value=incident)
            producer.flush()
            print(
                f"[IncidentProducer] ⚠️  INCIDENT: {incident['incident_type']} "
                f"@ {incident['intersection_id']} | Sévérité: {incident['severity']} | "
                f"Voies bloquées: {incident['lanes_blocked']}"
            )
        else:
            print("[IncidentProducer] ✅ Aucun incident cette seconde.")

        time.sleep(2)


if __name__ == "__main__":
    main()
