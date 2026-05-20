import sys
import time

# ------------------------------------------------------------------
# Import the run() function from each ETL module
# ------------------------------------------------------------------
from cleaning           import run_cleaning
from feature_engineering import run_feature_engineering
from model_training     import run_model_training


# ------------------------------------------------------------------
# Pipeline steps definition
# ------------------------------------------------------------------
STEPS = [
    ("Step 1 — Data Cleaning & Harmonization", run_cleaning),
    ("Step 2 — Feature Engineering",           run_feature_engineering),
    ("Step 3 — Model Training",                run_model_training),
]


# ------------------------------------------------------------------
# Orchestrator
# ------------------------------------------------------------------
def run_pipeline():
    print("\n" + "=" * 60)
    print("   ETL PIPELINE — World Happiness Streaming")
    print("=" * 60)

    total_start = time.time()

    for i, (name, step_fn) in enumerate(STEPS, start=1):
        print(f"\n[{i}/{len(STEPS)}] {name}")
        print("-" * 60)

        step_start = time.time()

        try:
            step_fn()
        except Exception as e:
            print(f"\n[FATAL] Pipeline failed at: {name}")
            print(f"        Error: {e}")
            sys.exit(1)

        elapsed = time.time() - step_start
        print(f"[{i}/{len(STEPS)}] Completed in {elapsed:.2f}s")

    total_elapsed = time.time() - total_start

    print("\n" + "=" * 60)
    print(f"   PIPELINE FINISHED — total time: {total_elapsed:.2f}s")
    print("=" * 60)
    print("\nNext steps:")
    print("  1. docker compose up -d")
    print("  2. python kafka/consumer.py   (terminal 1)")
    print("  3. python kafka/producer.py   (terminal 2)")
    print()


# ------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------
if __name__ == "__main__":
    run_pipeline()
