# Divvy Trips Kafka Event Stream

This repository contains a Docker-based pipeline that reads the Chicago Divvy Q4 2019 bike-share dataset and streams every trip record into an Apache Kafka cluster as JSON messages.

It is built to illustrate: message production, replicated Kafka topics, startup ordering, and a fully containerised data flow managed via Docker Compose.

---

## What’s Included

| Component            | Technology                   | Purpose                                                  |
| -------------------- | ---------------------------- | -------------------------------------------------------- |
| Dataset              | `Divvy_Trips_2019_Q4.csv`    | Source of trip events from Chicago Q4 2019               |
| Producer app         | Python 3.11 + `kafka-python` | Reads CSV lines, converts them to JSON, and sends them   |
| Kafka cluster        | Apache Kafka 7.5 (2 brokers) | Retains and distributes streamed trip events             |
| Coordination service | Apache ZooKeeper             | Keeps Kafka metadata consistent and manages leader state |
| Topic setup helper   | `kafka-init` container       | Creates required topics before the producer begins       |
| Monitoring UI        | Kafka UI (Provectus)         | Lets you inspect topics, partitions, and message content |
| Orchestration        | Docker Compose               | Boots the entire stack with a single command             |

---

## Event Format

Each row from the CSV is transformed into a JSON payload and published to Kafka. The producer uses `trip_id` as the message key, ensuring events for the same trip route consistently to the same partition.

```json
{
  "trip_id": 25223640,
  "start_time": "2019-10-01 00:01:46",
  "end_time": "2019-10-01 00:17:16",
  "bikeid": 2167,
  "tripduration": 930.0,
  "from_station_id": 199,
  "from_station_name": "Wabash Ave & Grand Ave",
  "to_station_id": 84,
  "to_station_name": "Milwaukee Ave & Grand Ave",
  "usertype": "Subscriber",
  "gender": "Male",
  "birthyear": "1990"
}
```

The pipeline also normalises numeric fields, converting comma-formatted durations like `"2,350.0"` into plain floating-point numbers.

---

## System Topology

```
+-------------------------------------------------------------+
|                     Docker network: kafka-net               |
|                                                             |
|  +----------+     +-----------+     +-----------+          |
|  | ZooKeeper|<----| Broker1   |     | Broker2   |          |
|  | :2181    |<----| :29092    |     | :29093    |          |
|  +----------+     +-----+-----+     +-----+-----+          |
|                         |                 |                 |
|                    +----+-----------------+----+            |
|                    |   Topic1  /  Topic2       |            |
|                    |   3 partitions, RF=2      |            |
|                    +---------------------------+            |
|                                 |                           |
|          +-----------------+  +------------------------+     |
|          | Kafka UI       |  | Producer container      |     |
|          | :8080          |  | (streams CSV into Kafka)|     |
|          +-----------------+  +------------------------+     |
+-------------------------------------------------------------+
```

- The stack runs two Kafka brokers to support replication and keep data available if one broker fails.
- A dedicated init service creates `Topic1` and `Topic2` with three partitions and replication factor 2 before the producer starts.
- Kafka UI is available at `http://localhost:8080` for exploring the stream and verifying message delivery.

---

## Repository Layout

```
.
├── Divvy_Trips_2019_Q4.csv    # Bike-share dataset used as input
├── docker-compose.yml         # Docker Compose stack definition
└── producer/
    ├── Dockerfile             # Builds the producer container
    ├── producer.py            # CSV-to-Kafka producer script
    └── requirements.txt       # Python dependency list
```

---

## Requirements

- Docker Desktop (includes Docker Compose v2)

Everything needed to run the pipeline is packaged in containers, so no local Python or Kafka install is required.

---

## Run the Pipeline

```bash
# Clone repository and enter the project directory
git clone <repo-url>
cd Lab3

# Build and start the Kafka cluster plus producer
docker compose up --build
```

Once the stack is running, open the dashboard at `http://localhost:8080` and inspect `Topic1` or `Topic2` under the Topics view.

The script prints progress every 1,000 records and reports a final summary after it finishes processing the CSV.

To stop and remove all containers:

```bash
docker compose down
```

---

## Customisation

The producer is configured through environment variables in `docker-compose.yml`:

| Variable                  | Default                         | Purpose                                        |
| ------------------------- | ------------------------------- | ---------------------------------------------- |
| `KAFKA_BOOTSTRAP_SERVERS` | `broker1:29092,broker2:29093`   | Kafka broker connection string                 |
| `CSV_FILE`                | `/data/Divvy_Trips_2019_Q4.csv` | Input CSV path inside the producer container   |
| `TOPIC1`                  | `Topic1`                        | Name of the first destination topic            |
| `TOPIC2`                  | `Topic2`                        | Name of the second destination topic           |
| `MESSAGE_DELAY`           | `0.1`                           | Seconds to wait between publishing each record |

Set `MESSAGE_DELAY=0` in the compose file to publish as fast as possible, or use a larger value to slow the stream for easier observation.

---

## Host Ports

| Service   | Host port | Description                |
| --------- | --------- | -------------------------- |
| Broker1   | `9092`    | External Kafka access      |
| Broker2   | `9093`    | External Kafka access      |
| ZooKeeper | `2181`    | Kafka cluster coordination |
| Kafka UI  | `8080`    | Web monitoring interface   |
