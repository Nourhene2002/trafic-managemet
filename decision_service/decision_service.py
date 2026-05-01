"""
Service de décision - Monitoring et API REST
============================================
Consomme les décisions Spark depuis 'traffic_decisions'
et expose une API + dashboard HTML pour visualiser en temps réel.

FIX: Retry Kafka avec backoff.
FIX: Parsing timestamp Spark (format "YYYY-MM-DD HH:MM:SS.ffffff").
FIX: Thread-safe state management.
"""

import json
import time
import os
import threading
from datetime import datetime, timezone
from collections import deque
from flask import Flask, jsonify, Response

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
INPUT_TOPIC   = os.getenv("INPUT_TOPIC", "traffic_decisions")

app = Flask(__name__)

# State partagé entre le thread Kafka et Flask
traffic_state   = {}
latency_history = deque(maxlen=1000)
state_lock      = threading.Lock()

MODE_EMOJI = {
    "FLUID":      "🟢",
    "MODERATE":   "🟡",
    "DENSE":      "🟠",
    "CONGESTION": "🔴",
    "EMERGENCY":  "⛔",
}
SEVERITY_EMOJI = {
    "NONE":     "⚪",
    "LOW":      "🟡",
    "MEDIUM":   "🟠",
    "HIGH":     "🔴",
    "CRITICAL": "⛔",
}


def parse_spark_timestamp(ts_str: str) -> datetime:
    """
    FIX: Spark sérialise les timestamps en plusieurs formats possibles :
    - "2024-03-24T10:15:32.123456"
    - "2024-03-24 10:15:32.123456"
    - "2024-03-24 10:15:32"
    On gère tous ces cas.
    """
    ts_str = ts_str.strip()
    # Normaliser le séparateur
    ts_str = ts_str.replace(" ", "T")
    # Essayer avec et sans microsecondes
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(ts_str, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise ValueError(f"Format timestamp non reconnu: {ts_str!r}")


def create_consumer() -> KafkaConsumer:
    """Crée un consumer Kafka avec retry exponentiel."""
    delay = 5
    for attempt in range(12):
        try:
            consumer = KafkaConsumer(
                INPUT_TOPIC,
                bootstrap_servers=KAFKA_SERVERS,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                group_id="decision-service-monitor",
                auto_offset_reset="latest",
                enable_auto_commit=True,
                consumer_timeout_ms=-1,   # Bloquer indéfiniment
            )
            print(f"[DecisionService] ✅ Connecté à Kafka | topic: {INPUT_TOPIC}")
            return consumer
        except NoBrokersAvailable:
            print(f"[DecisionService] ⏳ Kafka pas prêt (tentative {attempt+1}/12), attente {delay}s...")
            time.sleep(delay)
            delay = min(delay * 2, 30)
    raise RuntimeError("Impossible de se connecter à Kafka après 12 tentatives")


def consume_decisions():
    """Thread de consommation Kafka — tourne en arrière-plan."""
    consumer = create_consumer()

    for message in consumer:
        try:
            decision = message.value
            intersection_id = decision.get("intersection_id", "UNKNOWN")

            # Calcul latence end-to-end
            latency_ms = -1
            ts_str = decision.get("decision_timestamp")
            if ts_str:
                try:
                    ts = parse_spark_timestamp(ts_str)
                    now = datetime.now(timezone.utc)
                    latency_ms = round((now - ts).total_seconds() * 1000, 2)
                except Exception as e:
                    print(f"[DecisionService] ⚠️  Erreur calcul latence: {e}")

            decision["latency_ms"]     = latency_ms                          # Kafka-consumer
            decision["e2e_latency_ms"] = decision.get("e2e_latency_ms", -1)  # end-to-end (Spark)
            decision["received_at"]    = datetime.now(timezone.utc).isoformat()

            # Suivi SLA sur la latence E2E (la plus significative)
            e2e = decision["e2e_latency_ms"]
            with state_lock:
                if e2e >= 0:
                    latency_history.append(e2e)
                traffic_state[intersection_id] = decision

            mode = decision.get("traffic_mode", "?")
            print(
                f"[DecisionService] {MODE_EMOJI.get(mode, '⚪')} {intersection_id} "
                f"| Mode: {mode} | "
                f"Vert: {decision.get('green_duration_sec')}s | "
                f"Véhicules: {decision.get('total_vehicles')} | "
                f"Latence Kafka: {latency_ms}ms | Latence E2E: {e2e}ms"
            )

        except Exception as e:
            print(f"[DecisionService] ❌ Erreur traitement message: {e}")


# ─────────────────────────────────────────────
# API REST
# ─────────────────────────────────────────────

@app.route("/api/intersections", methods=["GET"])
def get_all_intersections():
    with state_lock:
        data = list(traffic_state.values())
    return jsonify({
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "total_intersections": len(data),
        "intersections": data,
    })


@app.route("/api/intersections/<intersection_id>", methods=["GET"])
def get_intersection(intersection_id: str):
    with state_lock:
        data = traffic_state.get(intersection_id)
    if data is None:
        return jsonify({"error": "Intersection non trouvée"}), 404
    return jsonify(data)


@app.route("/api/stats/latency", methods=["GET"])
def get_latency_stats():
    with state_lock:
        hist = list(latency_history)

    if not hist:
        return jsonify({"message": "Pas encore de données de latence"})

    hist_pos = [l for l in hist if l >= 0]
    if not hist_pos:
        return jsonify({"message": "Données de latence invalides"})

    sorted_hist = sorted(hist_pos)
    n = len(sorted_hist)

    return jsonify({
        "sample_count": n,
        "avg_latency_ms":  round(sum(hist_pos) / n, 2),
        "min_latency_ms":  round(sorted_hist[0], 2),
        "max_latency_ms":  round(sorted_hist[-1], 2),
        "p95_latency_ms":  round(sorted_hist[int(n * 0.95)], 2),
        "p99_latency_ms":  round(sorted_hist[min(int(n * 0.99), n - 1)], 2),
        "sla_3sec_compliance_pct": round(
            sum(1 for l in hist_pos if l < 3000) / n * 100, 2
        ),
    })


@app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status": "ok",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "intersections_monitored": len(traffic_state),
    })


@app.route("/", methods=["GET"])
def dashboard():
    """Dashboard HTML simple pour visualisation en temps réel."""
    html = """<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta http-equiv="refresh" content="3">
<title>🚦 Traffic Dashboard</title>
<style>
  body { font-family: monospace; background: #111; color: #eee; padding: 20px; }
  h1 { color: #4fc; }
  table { border-collapse: collapse; width: 100%; }
  th { background: #333; padding: 8px; text-align: left; }
  td { padding: 6px 8px; border-bottom: 1px solid #333; }
  .FLUID { color: #4fc; }
  .MODERATE { color: #fa0; }
  .DENSE { color: #f80; }
  .CONGESTION { color: #f44; }
  .EMERGENCY { color: #f00; font-weight: bold; }
</style>
</head>
<body>
<h1>🚦 Régulation Intelligente du Trafic Urbain</h1>
<p>Actualisation auto toutes les 3 secondes | <a href="/api/stats/latency" style="color:#4fc">Latences API</a></p>
<table>
<tr>
  <th>Intersection</th><th>Mode</th><th>Véhicules</th>
  <th>Vitesse moy.</th><th>Sévérité incident</th>
  <th>Feu vert</th><th>Feu rouge</th><th>Latence Kafka</th><th>Latence E2E</th>
</tr>
"""
    with state_lock:
        rows = list(traffic_state.values())

    if not rows:
        html += "<tr><td colspan='8'>⏳ En attente des données Spark...</td></tr>"
    else:
        for d in sorted(rows, key=lambda x: x.get("intersection_id", "")):
            mode = d.get("traffic_mode", "?")
            html += f"""<tr>
  <td>{d.get('intersection_id', '?')}</td>
  <td class="{mode}">{MODE_EMOJI.get(mode, '')} {mode}</td>
  <td>{d.get('total_vehicles', '?')}</td>
  <td>{round(d.get('avg_speed_kmh') or 0, 1)} km/h</td>
  <td>{SEVERITY_EMOJI.get(d.get('worst_severity',''), '')} {d.get('worst_severity','?')}</td>
  <td style="color:#4fc">{d.get('green_duration_sec','?')}s</td>
  <td style="color:#f44">{d.get('red_duration_sec','?')}s</td>
  <td>{d.get('latency_ms','?')}ms</td>
  <td style="color:#fc4">{d.get('e2e_latency_ms','?')}ms</td>
</tr>"""

    html += "</table></body></html>"
    return Response(html, mimetype="text/html")


if __name__ == "__main__":
    consumer_thread = threading.Thread(target=consume_decisions, daemon=True)
    consumer_thread.start()
    print("[DecisionService] 🚦 API démarrée sur http://0.0.0.0:5000")
    print(f"[DecisionService] 📡 Écoute du topic Kafka: {INPUT_TOPIC}")
    app.run(host="0.0.0.0", port=5000, debug=False)
