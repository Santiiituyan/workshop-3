import json
import os
import joblib
from datetime import datetime, timezone
import pandas as pd

from kafka import KafkaConsumer
from sqlalchemy import create_engine, text

# ------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------
KAFKA_BROKER = "localhost:9092"
TOPIC        = "happiness-predictions"
GROUP_ID     = "happiness-consumer-group"

DB_URL       = "postgresql://etl_user:etl_pass@localhost:5432/etl_db"

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)

MODELS_DIR = os.path.join(PROJECT_ROOT, "models")

MODEL_PATH  = os.path.join(MODELS_DIR, "model.pkl")
SCALER_PATH = os.path.join(MODELS_DIR, "scaler.pkl")

# Feature order must match feature_engineering.py and producer.py
FEATURES     = ["gdp", "family", "health", "freedom", "generosity", "corruption"]


# ------------------------------------------------------------------
# Database
# ------------------------------------------------------------------
def get_engine():
    """Create and return a SQLAlchemy engine."""
    engine = create_engine(DB_URL)
    print(f"Database connected: {DB_URL}")
    return engine
 
 
def insert_raw_event(engine, event: dict, raw_message: str) -> int:
    query = text("""
        INSERT INTO raw_happiness_events (
            country, year, gdp, family, health, freedom,
            generosity, corruption, actual_happiness_score,
            raw_message, processing_status, received_at
        ) VALUES (
            :country, :year, :gdp, :family, :health, :freedom,
            :generosity, :corruption, :actual_happiness_score,
            :raw_message, 'VALID', :received_at
        )
        RETURNING raw_event_id
    """)
 
    with engine.begin() as conn:
        result = conn.execute(query, {
            "country":                event["country"],
            "year":                   event["year"],
            "gdp":                    event["gdp"],
            "family":                 event["family"],
            "health":                 event["health"],
            "freedom":                event["freedom"],
            "generosity":             event["generosity"],
            "corruption":             event["corruption"],
            "actual_happiness_score": event["actual_happiness_score"],
            "raw_message":            raw_message,
            "received_at":            datetime.now(timezone.utc),
        })
        return result.fetchone()[0]
 
 
def upsert_dim_country(engine, country_name: str) -> int:
    with engine.begin() as conn:
        result = conn.execute(
            text("SELECT country_id FROM dim_country WHERE country_name = :name"),
            {"name": country_name}
        ).fetchone()
        if result:
            return result[0]
        return conn.execute(
            text("INSERT INTO dim_country (country_name) VALUES (:name) RETURNING country_id"),
            {"name": country_name}
        ).fetchone()[0]
 
 
def upsert_dim_date(engine, year: int) -> int:
    with engine.begin() as conn:
        result = conn.execute(
            text("SELECT date_id FROM dim_date WHERE year = :year"),
            {"year": year}
        ).fetchone()
        if result:
            return result[0]
        return conn.execute(
            text("INSERT INTO dim_date (year) VALUES (:year) RETURNING date_id"),
            {"year": year}
        ).fetchone()[0]
 
 
def insert_prediction(engine, raw_event_id: int, event: dict, predicted_score: float) -> None:
    actual_score     = float(event["actual_happiness_score"])
    prediction_error = abs(actual_score - predicted_score)
    country_id       = upsert_dim_country(engine, event["country"])
    date_id          = upsert_dim_date(engine, int(event["year"]))
 
    query = text("""
        INSERT INTO fact_predictions (
            raw_event_id, country_id, date_id,
            actual_score, predicted_score, prediction_error,
            prediction_timestamp
        ) VALUES (
            :raw_event_id, :country_id, :date_id,
            :actual_score, :predicted_score, :prediction_error,
            :prediction_timestamp
        )
    """)
 
    with engine.begin() as conn:
        conn.execute(query, {
            "raw_event_id":         raw_event_id,
            "country_id":           country_id,
            "date_id":              date_id,
            "actual_score":         actual_score,
            "predicted_score":      predicted_score,
            "prediction_error":     prediction_error,
            "prediction_timestamp": datetime.now(timezone.utc),
        })
 
 
# ------------------------------------------------------------------
# Main consumer loop
# ------------------------------------------------------------------
def run():
    print("\n========== KAFKA CONSUMER START ==========\n")
 
    # Load model
    model = joblib.load(MODEL_PATH)
    print(f"Model loaded: {MODEL_PATH}\n")
 
    # Connect to database
    engine = get_engine()
 
    # Connect to Kafka
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode("latin-1", errors="replace")),
    )
    print(f"Consumer connected — listening on topic: {TOPIC}\n")
 
    processed = 0
 
    for message in consumer:
        try:
            event       = message.value
            raw_message = json.dumps(event)
            processed  += 1
 
            # Step 1 — Store raw event
            raw_event_id = insert_raw_event(engine, event, raw_message)
 
            # Step 2 — Predict directly from event features (already scaled by producer)
            feature_vector = pd.DataFrame([[float(event[f]) for f in FEATURES]], columns=FEATURES)
            predicted_score = float(model.predict(feature_vector)[0])
 
            # Step 3 — Store prediction
            insert_prediction(engine, raw_event_id, event, predicted_score)
 
            actual_score = float(event["actual_happiness_score"])
            error        = abs(actual_score - predicted_score)
 
            print(f"[{processed:04d}] {event['country']} ({event['year']}) "
                  f"| predicted={predicted_score:.3f} "
                  f"| actual={actual_score:.3f} "
                  f"| error={error:.3f}")
 
        except Exception as e:
            print(f"[ERROR] message {processed}: {e}")
 
 
# ------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------
if __name__ == "__main__":
    run()