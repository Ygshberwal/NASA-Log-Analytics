# NASA log analytics

A local streaming pipeline that answers three questions about web traffic: how many requests arrived, which paths were popular, and which HTTP statuses were returned? It combines a replay of July 1995 Kennedy Space Center access logs with current requests made by a local app to NASA's homepage. The latter measures this app's traffic, not all visitors to nasa.gov.

```text
NASA archive sample or local request → NiFi → Kafka → Spark → Cassandra → Dash
```

The live view groups Kafka arrivals from the last 15 minutes. Hourly and daily views group requests by their original timestamp (UTC). Each Cassandra row is keyed by Kafka partition and offset, so Spark can retry writing a message without adding another row. The demo uses one partition and computes chart totals in memory.

## Screenshots

### Live View (Last 15 Minutes)

![Live dashboard showing 5 events received in the last 15 minutes](<images/live-ingestion.png>)
*Real-time ingestion rate of requests received by Kafka per minute (UTC).*

![HTTP status codes for the last 15 minutes](<images/live-status.png>)
*HTTP status code breakdown (`200` and `502`) for requests in the last 15 minutes.*

![Top requested paths in the last 15 minutes](<images/live-paths.png>)
*Most frequently requested paths in the last 15 minutes.*

### Historical View (All Stored Events)

![Overall HTTP status codes across all stored events](<images/overall-status.png>)
*Overall HTTP status code distribution across the full dataset.*

![Overall top 10 requested paths](<images/overall-paths.png>)
*Top 10 most requested paths across all stored logs.*

## Run locally (PowerShell)

Install Docker Desktop with Linux containers and Python 3. From a fresh checkout, copy `.env.example` to `.env` and set `NIFI_PASSWORD` to a unique password of at least 12 characters. Open Docker Desktop and start the services:

```powershell
docker compose up -d --build nifi kafka cassandra nasa-web dashboard
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic nasa-access-logs --partitions 1 --replication-factor 1
Get-Content -Raw cassandra/schema.cql | docker compose exec -T cassandra cqlsh
docker compose --profile processing up -d spark
```

Kafka and Cassandra may take a minute to accept connections. If topic or schema creation reports connection refused, retry it before starting Spark. Spark downloads its connectors on first startup.

### Configure ingestion once

Open [NiFi](https://localhost:8443/nifi) and sign in as `learner` with your `.env` password. Its local HTTPS certificate is self-signed. Create these processors and connections:

| Processor | Settings | Relationship |
|---|---|---|
| GetFile | Input Directory `/tmp/nasa-input`; Keep Source File `false` | `success` → SplitText |
| SplitText | Line Split Count `1` | `splits` → PublishKafka; auto-terminate `original` and `failure` |
| PublishKafka_2_6 | Kafka Brokers `kafka:9092`; Topic Name `nasa-access-logs` | Auto-terminate `success` and `failure` |

Start all three processors. This local demo discards failed flow files; check processor bulletins if input goes missing.

### See the result

1. Open the [request page](http://localhost:8081/) and click **Make a request to nasa.gov**.
2. Open the [dashboard](http://localhost:8050/) and select **Live**. After processing and the next 10-second refresh, the request contributes to the current minute's bar.
3. Optional: run `python prepare_nasa_logs.py` to replay the first 1,000 archive lines. Replaying again produces new Kafka messages. Use **Historical: hourly** to see requests by their 1995 event time.

If the chart stays empty, check `docker compose --profile processing ps -a`, `docker compose logs --tail 50 spark`, and the NiFi processor states. Use `docker compose --profile processing stop` to pause the demo while retaining containers, flow configuration, and data. Resume with `docker compose --profile processing start`.

[Original ProjectPro problem statement](https://www.projectpro.io/project-use-case/real-time-log-processing-using-streaming-architecture) · [NASA archive description and attribution](https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html)
