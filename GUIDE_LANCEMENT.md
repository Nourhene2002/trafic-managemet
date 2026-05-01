# Guide de Lancement - Traffic Fixed

Ce projet simule et regule le trafic urbain en temps reel avec :

- `vehicle-producer` -> topic Kafka `vehicles`
- `incident-producer` -> topic Kafka `incidents`
- `Spark Streaming` -> jointure stream-stream native + decisions
- `decision-service` -> dashboard + API REST

Pipeline complet :

`vehicles + incidents -> Kafka -> Spark -> traffic_decisions -> dashboard`

## Prerequis

- Docker Desktop demarre
- Docker Compose v2
- 6 Go de RAM minimum

## Lancement complet

### Option A - Windows / PowerShell

**Terminal 1**

```powershell
cd C:\Users\Adem\Desktop\traffic-fixed
docker compose down -v
docker compose up -d --build
docker compose ps
```

Attendez que :

- `zookeeper` soit `healthy`
- `kafka` soit `healthy`
- `spark-master` soit `Up`
- `spark-worker` soit `Up`
- `decision-service` soit `Up`
- `vehicle-producer` soit `Up`
- `incident-producer` soit `Up`

**Terminal 2**

```powershell
cd C:\Users\Adem\Desktop\traffic-fixed
.\submit_spark_job.ps1
```

Laissez ce terminal ouvert.

Au premier lancement, Spark telecharge les dependances Kafka. Cela peut prendre 1 a 3 minutes.

**Terminal 3**

```powershell
docker logs -f decision-service
```

**Terminal 4**

```powershell
Invoke-RestMethod http://localhost:5000/api/intersections | ConvertTo-Json -Depth 6
```

### Option B - Bash

**Terminal 1**

```bash
cd traffic-fixed
docker compose down -v
docker compose up -d --build
docker compose ps
```

**Terminal 2**

```bash
bash submit_spark_job.sh
```

**Terminal 3**

```bash
docker logs -f decision-service
```

## Ce que vous devez voir

Dans le terminal Spark :

```text
DEMARRAGE DU PIPELINE SPARK STREAMING v3
[Spark] KafkaProducer pret
[Spark] Query native stream-stream demarree
```

Puis ensuite des lignes du type :

```text
[Spark] ORANGE INT_CENTRE_VILLE | DENSE | Veh:24 | 28.5km/h | Sev:NONE | Vert:60s Rouge:30s | Latence E2E:1850.22ms
```

Dans `decision-service` :

```text
[DecisionService] ... | Mode: DENSE | ...
```

## Interfaces

- Dashboard : `http://localhost:5000`
- API intersections : `http://localhost:5000/api/intersections`
- API latence : `http://localhost:5000/api/stats/latency`
- Kafka UI : `http://localhost:8090`
- Spark Master UI : `http://localhost:8080`
- Spark Worker UI : `http://localhost:8081`

## Arret propre

1. Arretez le terminal Spark avec `Ctrl+C`
2. Arretez les terminaux `docker logs -f` avec `Ctrl+C`
3. Puis :

```powershell
docker compose down
```

Pour un reset complet :

```powershell
docker compose down -v
```

## Verification rapide

Verifier les producteurs :

```powershell
docker logs --tail 20 vehicle-producer
docker logs --tail 20 incident-producer
```

Verifier les decisions :

```powershell
docker logs --tail 50 decision-service
```

Verifier les topics :

```powershell
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list
```

Verifier que `traffic_decisions` avance :

```powershell
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:29092 --topic traffic_decisions
```

## Si le dashboard reste vide

1. Verifiez que le job Spark tourne encore dans le terminal `submit_spark_job`
2. Verifiez les logs :

```powershell
docker logs --tail 100 decision-service
docker logs --tail 50 vehicle-producer
docker logs --tail 50 incident-producer
```

3. Verifiez l'API :

```powershell
Invoke-RestMethod http://localhost:5000/api/intersections | ConvertTo-Json -Depth 6
```

4. Verifiez si Spark produit bien :

```powershell
docker exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic traffic_decisions --from-beginning --max-messages 3
```

## Notes importantes

- `docker compose up -d --build` ne lance pas le job Spark automatiquement.
- Il faut toujours lancer ensuite `submit_spark_job.sh` ou `submit_spark_job.ps1`.
- L'image Spark est construite avec `kafka-python-ng`, donc il n'y a plus d'installation manuelle a faire.
- La version actuelle utilise une vraie jointure stream-stream native dans Spark.
