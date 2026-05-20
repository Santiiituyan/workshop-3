import pandas as pd
import json
import time
import os
from kafka import KafkaProducer

# ------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------
KAFKA_BROKER  = "localhost:9092"
TOPIC         = "happiness-predictions"
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
INPUT_FILE = os.path.join(PROJECT_ROOT, "data", "processed", "happiness_ml_ready.csv")
DELAY_SECONDS = 0.5   # Delay between events to simulate real streaming

# Features included in each Kafka event
# Must match the schema required by the workshop
FEATURES = ["gdp", "family", "health", "freedom", "generosity", "corruption"]


# ------------------------------------------------------------------
# Functions
# ------------------------------------------------------------------
def build_producer() -> KafkaProducer:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("latin-1"),
    )
    print(f"Producer connected to broker: {KAFKA_BROKER}")
    print(f"Target topic               : {TOPIC}\n")
    return producer


def build_event(row: pd.Series) -> dict:
    return {
        "country":                str(row["country"]),
        "year":                   int(row["year"]),
        "gdp":                    float(row["gdp"]),
        "family":                 float(row["family"]),
        "health":                 float(row["health"]),
        "freedom":                float(row["freedom"]),
        "generosity":             float(row["generosity"]),
        "corruption":             float(row["corruption"]),
        "actual_happiness_score": float(row["happiness_score"]),
    }


def run():
    print("\n========== KAFKA PRODUCER START ==========\n")

    # Load dataset
    df = pd.read_csv(INPUT_FILE)
    print(f"Loaded dataset — {len(df)} records to stream\n")

    # Connect to Kafka
    producer = build_producer()

    sent    = 0
    failed  = 0

    for idx, row in df.iterrows():
        try:
            event = build_event(row)
            producer.send(TOPIC, value=event)
            sent += 1
            print(f"[{sent:04d}] Sent → country={event['country']} | year={event['year']} | score={event['actual_happiness_score']}")
            time.sleep(DELAY_SECONDS)

        except Exception as e:
            failed += 1
            print(f"[ERROR] Failed to send record {idx}: {e}")

    # Flush ensures all buffered messages are sent before exiting
    producer.flush()
    producer.close()

    print(f"\n========== PRODUCER SUMMARY ==========")
    print(f"Total sent   : {sent}")
    print(f"Total failed : {failed}")
    print("========== KAFKA PRODUCER END ==========\n")


# ------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------
if __name__ == "__main__":
    run()
