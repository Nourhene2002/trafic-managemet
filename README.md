# Traffic Fixed

Projet de simulation et de régulation intelligente du trafic urbain en temps réel.

Ce système combine **Kafka**, **Spark Structured Streaming**, **Docker Compose** et un **dashboard Flask** pour simuler des flux de circulation, détecter des incidents routiers et produire des décisions de régulation sur plusieurs intersections.

## Stack technique

- Docker / Docker Compose
- Apache Kafka
- Apache Spark Structured Streaming
- Python 3.11
- Flask
- Kafka UI

## Services

- `zookeeper` : coordination Kafka
- `kafka` : bus d’événements temps réel
- `kafka-ui` : interface de visualisation des topics Kafka
- `spark-master` : nœud principal Spark
- `spark-worker` : exécution des traitements Spark
- `vehicle-producer` : simulation des flux de véhicules
- `incident-producer` : simulation des incidents routiers
- `decision-service` : API REST + dashboard temps réel

## Fonctionnement

### 1. Producteur véhicules

Le service `vehicle-producer` génère en continu :

- identifiant d’intersection
- nombre de véhicules
- vitesse moyenne
- direction
- type de véhicule
- niveau de densité

### 2. Producteur incidents

Le service `incident-producer` génère aléatoirement :

- accidents
- travaux
- véhicules en panne
- manifestations
- inondations
- contrôles de police

Chaque incident possède une sévérité et une durée estimée.

### 3. Traitement Spark

Le job Spark :

- consomme les topics `vehicles` et `incidents`
- agrège les données véhicules par fenêtre de 1 seconde
- maintient l’état des incidents actifs
- calcule un mode de trafic par intersection
- détermine la durée des feux verts et rouges
- publie les décisions sur `traffic_decisions`

### 4. Service de décision

Le `decision-service` :

- consomme les décisions générées par Spark
- stocke l’état courant par intersection
- expose une API REST
- affiche un dashboard HTML en temps réel
- mesure la latence end-to-end

## Modes de trafic

Selon le volume, la vitesse moyenne et la gravité des incidents, le système classe les intersections en :

- `FLUID`
- `MODERATE`
- `DENSE`
- `CONGESTION`
- `EMERGENCY`

## Prérequis

- Docker Desktop
- Docker Compose v2
- environ 6 Go de RAM disponibles

## Lancement

### 1. Démarrer l’infrastructure

```bash
docker compose down -v
docker compose up -d --build
docker compose ps
```

### 2. Lancer le job Spark

Sous PowerShell :

```powershell
.\submit_spark_job.ps1
```

Sous Bash :

```bash
bash submit_spark_job.sh
```

## Interfaces disponibles

- Dashboard : `http://localhost:5000`
- API intersections : `http://localhost:5000/api/intersections`
- API latence : `http://localhost:5000/api/stats/latency`
- Kafka UI : `http://localhost:8090`
- Spark Master UI : `http://localhost:8080`
- Spark Worker UI : `http://localhost:8081`

## Exemple de sortie

Le système produit des décisions de ce type :

```text
[Spark] ORANGE INT_CENTRE_VILLE | DENSE | Veh:24 | 28.5km/h | Sev:NONE | Vert:60s Rouge:30s
```

## Cas d’usage

Ce projet peut servir pour :

- démonstration Big Data / streaming temps réel
- projet académique autour de Kafka et Spark
- prototype de smart city
- expérimentation sur la régulation adaptative du trafic

## Points forts

- pipeline temps réel complet
- simulation multi-sources
- architecture conteneurisée
- dashboard web intégré
- suivi de latence end-to-end
- séparation claire entre ingestion, traitement et restitution
