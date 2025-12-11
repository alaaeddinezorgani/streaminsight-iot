# StreamInsight IoT (local demo)

Prereqs:
- Docker & Docker Compose (v2)
- 8GB RAM (recommended)

1. Build & start services
   `docker compose up --build -d`

2. Vérifier :
   - Redpanda Kafka: localhost:9092 (utilise rpk/kafka clients)
   - InfluxDB UI: http://localhost:8086 (user: admin / adminpass)
   - Grafana: http://localhost:3000 (admin/admin)

3. Logs:
   `docker compose logs -f producer`
   `docker compose logs -f spark-job`

4. Stop:
   `docker compose down -v`

Notes:
- Si le spark-job plante parce que `spark-sql-kafka` ne se télécharge pas, relancer `docker compose up --build spark-job`.
- Ajuste les limites mémoire dans docker-compose si nécessaire.

