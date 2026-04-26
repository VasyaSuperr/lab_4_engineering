import csv
import json
import os
import time
import logging
from dataclasses import dataclass
from typing import Dict, Any

from kafka import KafkaProducer
from kafka.errors import KafkaError


# ---------------- LOGGING ----------------
# Налаштування логів для відслідковування процесу
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("kafka-streamer")


# ---------------- CONFIG ----------------
# Конфігурація, яка береться з environment змінних
@dataclass
class Config:
    brokers: list
    csv_path: str
    topic_a: str
    topic_b: str
    delay: float

    @staticmethod
    def load():
        # Завантаження налаштувань (Kafka, файл, топіки)
        return Config(
            brokers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(","),
            csv_path=os.getenv("CSV_FILE", "Divvy_Trips_2019_Q4.csv"),
            topic_a=os.getenv("TOPIC1", "Topic1"),
            topic_b=os.getenv("TOPIC2", "Topic2"),
            delay=float(os.getenv("MESSAGE_DELAY", "0.1")),
        )


# ---------------- KAFKA HELPERS ----------------
class KafkaClient:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.producer = None

    def wait_until_ready(self, attempts=15, pause=5):
        # Перевіряємо чи Kafka запущена і доступна
        for i in range(attempts):
            try:
                tmp = KafkaProducer(bootstrap_servers=self.cfg.brokers, api_version=(2, 5, 0))
                tmp.close()
                log.info("Kafka is available.")
                return
            except KafkaError as e:
                log.warning("Kafka not ready (%s). Retry %d/%d", e, i + 1, attempts)
                time.sleep(pause)

        raise RuntimeError("Kafka unavailable after retries")

    def connect(self):
        # Створення producer'а для відправки повідомлень
        self.producer = KafkaProducer(
            bootstrap_servers=self.cfg.brokers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: str(k).encode(),
            acks="all",
            retries=3,
            api_version=(2, 5, 0),
        )

    def send(self, topic: str, key: Any, value: dict):
        # Відправка повідомлення в Kafka топік
        self.producer.send(topic, key=key, value=value)

    def flush(self):
        # Примусова відправка всіх накопичених повідомлень
        self.producer.flush()

    def close(self):
        # Закриття з'єднання з Kafka
        self.producer.close()


# ---------------- TRANSFORM ----------------
def safe_int(v, default=0):
    # Безпечне перетворення в int (щоб не падало при помилках)
    try:
        return int(v)
    except:
        return default


def safe_float(v):
    # Безпечне перетворення в float
    try:
        return float(str(v).replace(",", ""))
    except:
        return 0.0


def transform(row: Dict[str, str]) -> Dict[str, Any]:
    # Перетворення CSV рядка в структурований JSON-об'єкт
    return {
        "trip_id": safe_int(row.get("trip_id")),
        "start_time": row.get("start_time", ""),
        "end_time": row.get("end_time", ""),
        "bikeid": safe_int(row.get("bikeid")),
        "tripduration": safe_float(row.get("tripduration")),
        "from_station_id": safe_int(row.get("from_station_id")),
        "from_station_name": row.get("from_station_name", ""),
        "to_station_id": safe_int(row.get("to_station_id")),
        "to_station_name": row.get("to_station_name", ""),
        "usertype": row.get("usertype", ""),
        "gender": row.get("gender", ""),
        "birthyear": row.get("birthyear", ""),
    }


# ---------------- MAIN PIPELINE ----------------
def run():
    # Завантажуємо конфіг і підключаємо Kafka
    cfg = Config.load()
    kafka = KafkaClient(cfg)

    kafka.wait_until_ready()  # чекаємо Kafka
    kafka.connect()           # створюємо producer

    log.info("Reading file: %s", cfg.csv_path)

    counter = 0

    # Читаємо CSV файл построчно
    with open(cfg.csv_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            msg = transform(row)  # конвертація рядка в JSON
            key = msg["trip_id"]

            # відправляємо в 2 різні топіки
            for topic in (cfg.topic_a, cfg.topic_b):
                kafka.send(topic, key, msg)

            counter += 1

            # кожні 1000 повідомлень робимо flush (щоб гарантовано відправити)
            if counter % 1000 == 0:
                kafka.flush()
                log.info("Processed %d records", counter)

            time.sleep(cfg.delay)  # затримка між повідомленнями

    # фінальне завершення
    kafka.flush()
    kafka.close()

    log.info(
        "Finished. Total: %d messages sent to %s and %s",
        counter, cfg.topic_a, cfg.topic_b
    )


if __name__ == "__main__":
    run()