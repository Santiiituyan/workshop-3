import pandas as pd
import numpy as np
import joblib
import os

from sklearn.ensemble        import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics         import mean_absolute_error, mean_squared_error, r2_score

# ------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)

PROCESSED_DIR = os.path.join(PROJECT_ROOT, "data", "processed")
MODELS_DIR    = os.path.join(PROJECT_ROOT, "models")

INPUT_FILE    = os.path.join(PROCESSED_DIR, "happiness_ml_ready.csv")
MODEL_FILE    = os.path.join(MODELS_DIR,    "model.pkl")

# Feature order must match the order used in feature_engineering.py
# The Kafka consumer will send features in this exact order
FEATURES     = ["gdp", "family", "health", "freedom", "generosity", "corruption"]
TARGET       = "happiness_score"
RANDOM_STATE = 42
TEST_SIZE    = 0.30


def load_ml_ready(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    print(f"Loaded ML-ready dataset — shape: {df.shape}")
    return df


def split_data(df: pd.DataFrame):
    """
    Split dataset into training and test sets.
    70% training / 30% testing as suggested by the workshop.
    """
    X = df[FEATURES]
    y = df[TARGET]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=TEST_SIZE,
        random_state=RANDOM_STATE
    )

    print(f"Training set : {X_train.shape[0]} rows ({(1 - TEST_SIZE) * 100:.0f}%)")
    print(f"Test set     : {X_test.shape[0]}  rows ({TEST_SIZE * 100:.0f}%)")

    return X_train, X_test, y_train, y_test


def train_model(X_train, y_train) -> RandomForestRegressor:
    """
    Train a RandomForestRegressor.

    Selected over LinearRegression and DecisionTree based on evaluation:
    - MAE  = 0.4069  (best among candidates)
    - RMSE = 0.5237  (best among candidates)
    - R²   = 0.780   (best among candidates)
    """
    model = RandomForestRegressor(n_estimators=100, random_state=RANDOM_STATE)
    model.fit(X_train, y_train)
    print("Model trained — RandomForestRegressor(n_estimators=100)")
    return model


def evaluate_model(model, X_test, y_test) -> dict:
    """Evaluate the model on the test set and print metrics."""
    y_pred = model.predict(X_test)

    mae  = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    r2   = r2_score(y_test, y_pred)

    metrics = {"MAE": mae, "RMSE": rmse, "R2": r2}

    print("\n=== Model Evaluation ===")
    print(f"MAE  : {mae:.4f}")
    print(f"RMSE : {rmse:.4f}")
    print(f"R²   : {r2:.4f}")

    return metrics


def verify_saved_model(path: str, X_test, y_test) -> None:
    loaded = joblib.load(path)
    sample = X_test.iloc[:3]
    preds  = loaded.predict(sample)

    print("\nVerification — predictions from loaded model:")
    for i, pred in enumerate(preds):
        print(f"  Sample {i + 1}: predicted={pred:.4f} | actual={y_test.iloc[i]:.4f}")


def run_model_training():
    print("\n========== MODEL TRAINING PIPELINE START ==========\n")

    os.makedirs(MODELS_DIR, exist_ok=True)

    # Load
    df = load_ml_ready(INPUT_FILE)

    # Split
    X_train, X_test, y_train, y_test = split_data(df)

    # Train
    model = train_model(X_train, y_train)

    # Evaluate
    evaluate_model(model, X_test, y_test)

    # Save model
    joblib.dump(model, MODEL_FILE)
    print(f"\nModel saved to: {MODEL_FILE}")

    # Verify
    verify_saved_model(MODEL_FILE, X_test, y_test)

    print("\n========== MODEL TRAINING PIPELINE END ==========\n")


# ------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------
if __name__ == "__main__":
    run_model_training()
