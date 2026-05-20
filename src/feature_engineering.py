import pandas as pd
import joblib
import os
from sklearn.preprocessing import StandardScaler

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)

PROCESSED_DIR  = os.path.join(PROJECT_ROOT, "data", "processed")
MODELS_DIR     = os.path.join(PROJECT_ROOT, "models")

INPUT_FILE     = os.path.join(PROCESSED_DIR, "happiness_unified.csv")
OUTPUT_FILE    = os.path.join(PROCESSED_DIR, "happiness_ml_ready.csv")
SCALER_FILE    = os.path.join(MODELS_DIR,    "scaler.pkl")

# Features used for training — order matters for the Kafka consumer
FEATURES = ["gdp", "family", "health", "freedom", "generosity", "corruption"]
TARGET   = "happiness_score"


# ------------------------------------------------------------------
# Functions
# ------------------------------------------------------------------
def load_unified(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    print(f"Loaded unified dataset — shape: {df.shape}")
    return df


def scale_features(df: pd.DataFrame) -> tuple[pd.DataFrame, StandardScaler]:
    X      = df[FEATURES].copy()
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    X_scaled = pd.DataFrame(X_scaled, columns=FEATURES)

    print(f"Scaling applied — mean ≈ 0, std ≈ 1 for all features")
    print(f"Feature std before scaling:\n{df[FEATURES].std().round(4).to_string()}")

    return X_scaled, scaler


def build_ml_dataset(df: pd.DataFrame, X_scaled: pd.DataFrame) -> pd.DataFrame:
    df_ml = X_scaled.copy()
    df_ml[TARGET]    = df[TARGET].values
    df_ml["country"] = df["country"].values
    df_ml["year"]    = df["year"].values

    # Reorder: traceability | features | target
    df_ml = df_ml[["country", "year"] + FEATURES + [TARGET]]
    return df_ml


def run_feature_engineering():
    print("\n========== FEATURE ENGINEERING PIPELINE START ==========\n")

    os.makedirs(PROCESSED_DIR, exist_ok=True)
    os.makedirs(MODELS_DIR,    exist_ok=True)

    # Load
    df = load_unified(INPUT_FILE)

    # Scale
    X_scaled, scaler = scale_features(df)

    # Build ML-ready dataset
    df_ml = build_ml_dataset(df, X_scaled)
    print(f"\nML-ready dataset shape : {df_ml.shape}")
    print(f"Columns                : {list(df_ml.columns)}")

    # Save ML-ready dataset
    df_ml.to_csv(OUTPUT_FILE, index=False)
    print(f"\nSaved ML-ready dataset to : {OUTPUT_FILE}")

    # Save scaler — required by the Kafka consumer to scale incoming events
    joblib.dump(scaler, SCALER_FILE)
    print(f"Saved scaler to           : {SCALER_FILE}")

    print("\n========== FEATURE ENGINEERING PIPELINE END ==========\n")


# ------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------
if __name__ == "__main__":
    run_feature_engineering()
