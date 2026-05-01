"""
Producteur de données de véhicules.
Simule des capteurs à chaque intersection qui comptent les véhicules en temps réel.

FIX: timestamp ISO 8601 strict (sans 'Z', remplacé par +00:00) pour Spark CAST AS TIMESTAMP.
FIX: retry Kafka avec backoff exponentiel.
"""

import json
import random
import time
import os
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("TOPIC", "vehicles")

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

DIRECTIONS = ["NORD", "SUD", "EST", "OUEST"]
VEHICLE_TYPES = ["voiture", "camion", "moto", "bus", "velo"]
VEHICLE_WEIGHTS = [0.6, 0.1, 0.1, 0.1, 0.1]


def get_traffic_density_factor():
    """Simule les heures de pointe (7h-9h et 17h-19h)."""
    hour = datetime.now().hour
    if 7 <= hour <= 9 or 17 <= hour <= 19:
        return random.uniform(1.5, 2.5)
    elif 12 <= hour <= 14:
        return random.uniform(1.0, 1.5)
    elif 0 <= hour <= 5:
        return random.uniform(0.1, 0.3)
    return random.uniform(0.5, 1.0)


def generate_vehicle_event(intersection_id: str) -> dict:
    density_factor = get_traffic_density_factor()
    vehicle_count = int(random.randint(0, 30) * density_factor)

    # FIX: Spark CAST AS TIMESTAMP accepte "2024-01-01T10:00:00" (sans Z)
    # On utilise isoformat() qui produit le bon format
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    return {
        "event_id": f"VEH_{intersection_id}_{int(time.time()*1000)}",
        "timestamp": ts,
        "produced_at": ts,  # horodatage d'origine pour latence end-to-end
        "intersection_id": intersection_id,
        "direction": random.choice(DIRECTIONS),
        "vehicle_count": vehicle_count,
        "vehicle_type": random.choices(VEHICLE_TYPES, VEHICLE_WEIGHTS)[0],
        "average_speed_kmh": round(
            max(5.0, random.gauss(30.0 / max(density_factor, 0.1), 10.0)), 2
        ),
        "density_level": (
            "HIGH" if vehicle_count > 20
            else "MEDIUM" if vehicle_count > 10
            else "LOW"
        ),
    }


def create_producer() -> KafkaProducer:
    """Crée un producer Kafka avec retry exponentiel."""
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
            print(f"[VehicleProducer] ✅ Connecté à Kafka: {KAFKA_SERVERS}")
            return producer
        except NoBrokersAvailable:
            print(f"[VehicleProducer] ⏳ Kafka pas prêt (tentative {attempt+1}/12), attente {delay}s...")
            time.sleep(delay)
            delay = min(delay * 2, 30)
    raise RuntimeError("Impossible de se connecter à Kafka après 12 tentatives")


def main():
    print(f"[VehicleProducer] Démarrage, broker: {KAFKA_SERVERS}, topic: {TOPIC}")
    producer = create_producer()
    print(f"[VehicleProducer] 🚗 Simulation démarrée sur le topic '{TOPIC}'")

    while True:
        for intersection_id in INTERSECTIONS:
            event = generate_vehicle_event(intersection_id)
            producer.send(TOPIC, key=intersection_id, value=event)
            print(
                f"[VehicleProducer] {intersection_id} → "
                f"{event['vehicle_count']} véhicules | "
                f"{event['density_level']} | "
                f"{event['average_speed_kmh']} km/h"
            )

        producer.flush()
        time.sleep(1)


if __name__ == "__main__":
    main()
