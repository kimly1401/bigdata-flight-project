import csv
import json
import time
from kafka import KafkaProducer

# =========================
# CONFIG
# =========================
CSV_FILE = "data/flights.csv"     # đường dẫn CSV của bạn
KAFKA_TOPIC = "flight_topic"
BOOTSTRAP_SERVERS = "localhost:9092"

TOTAL_BATCHES = 10
SLEEP_BETWEEN_BATCH = 15          # giây, có thể tăng nếu Spark chậm

# =========================
# INIT KAFKA PRODUCER
# =========================
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=50
)

# =========================
# COUNT TOTAL ROWS (KHÔNG TÍNH HEADER)
# =========================
with open(CSV_FILE, "r", encoding="utf-8") as f:
    total_rows = sum(1 for _ in f) - 1

BATCH_SIZE = total_rows // TOTAL_BATCHES

print("===================================")
print(f"TOTAL ROWS       : {total_rows}")
print(f"TOTAL BATCHES    : {TOTAL_BATCHES}")
print(f"ROWS PER BATCH   : {BATCH_SIZE}")
print("===================================")

# =========================
# PRODUCE DATA BY BATCH
# =========================
current_batch = 1
row_count = 0
batch_count = 0

with open(CSV_FILE, "r", encoding="utf-8") as csvfile:
    reader = csv.DictReader(csvfile)

    for row in reader:
        producer.send(KAFKA_TOPIC, value=row)

        row_count += 1
        batch_count += 1

        # Khi đủ 1 batch
        if batch_count >= BATCH_SIZE and current_batch < TOTAL_BATCHES:
            producer.flush()

            print(f"[BATCH {current_batch}] Sent {batch_count} records "
                  f"(Total sent: {row_count})")

            current_batch += 1
            batch_count = 0

            print(f"Sleeping {SLEEP_BETWEEN_BATCH}s before next batch...\n")
            time.sleep(SLEEP_BETWEEN_BATCH)

    # Flush batch cuối
    producer.flush()
    print(f"[BATCH {current_batch}] Sent last {batch_count} records "
          f"(TOTAL: {row_count})")

producer.close()

print("===================================")
print("PRODUCER FINISHED ALL BATCHES")
print("===================================")
