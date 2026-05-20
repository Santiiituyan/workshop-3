import pandas as pd
import os

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))


PROJECT_ROOT = os.path.dirname(CURRENT_DIR)

RAW_DIR       = os.path.join(PROJECT_ROOT, "data", "raw")
PROCESSED_DIR = os.path.join(PROJECT_ROOT, "data", "processed")
OUTPUT_FILE   = os.path.join(PROCESSED_DIR, "happiness_unified.csv")

UNIFIED_SCHEMA = [
    "country",
    "year",
    "happiness_score",
    "gdp",
    "family",
    "health",
    "freedom",
    "generosity",
    "corruption",
]

# Column rename mapping per year — original name -> unified name
RENAME_MAP = {
    "2015": {
        "Country":                       "country",
        "Happiness Score":               "happiness_score",
        "Economy (GDP per Capita)":      "gdp",
        "Family":                        "family",
        "Health (Life Expectancy)":      "health",
        "Freedom":                       "freedom",
        "Generosity":                    "generosity",
        "Trust (Government Corruption)": "corruption",
    },
    "2016": {
        "Country":                       "country",
        "Happiness Score":               "happiness_score",
        "Economy (GDP per Capita)":      "gdp",
        "Family":                        "family",
        "Health (Life Expectancy)":      "health",
        "Freedom":                       "freedom",
        "Generosity":                    "generosity",
        "Trust (Government Corruption)": "corruption",
    },
    "2017": {
        "Country":                        "country",
        "Happiness.Score":                "happiness_score",
        "Economy..GDP.per.Capita.":       "gdp",
        "Family":                         "family",
        "Health..Life.Expectancy.":       "health",
        "Freedom":                        "freedom",
        "Generosity":                     "generosity",
        "Trust..Government.Corruption.":  "corruption",
    },
    "2018": {
        "Country or region":             "country",
        "Score":                         "happiness_score",
        "GDP per capita":                "gdp",
        "Social support":                "family",
        "Healthy life expectancy":       "health",
        "Freedom to make life choices":  "freedom",
        "Generosity":                    "generosity",
        "Perceptions of corruption":     "corruption",
    },
    "2019": {
        "Country or region":             "country",
        "Score":                         "happiness_score",
        "GDP per capita":                "gdp",
        "Social support":                "family",
        "Healthy life expectancy":       "health",
        "Freedom to make life choices":  "freedom",
        "Generosity":                    "generosity",
        "Perceptions of corruption":     "corruption",
    },
}


# ------------------------------------------------------------------
# Functions
# ------------------------------------------------------------------
def load_raw(year: str) -> pd.DataFrame:
    path = os.path.join(RAW_DIR, f"{year}.csv")
    df   = pd.read_csv(path)
    print(f"  [{year}] Loaded — shape: {df.shape}")
    return df


def clean_dataset(df: pd.DataFrame, year: str) -> pd.DataFrame:
    df = df.copy()

    # Step 1 — Rename columns
    df = df.rename(columns=RENAME_MAP[year])

    # Step 2 — Keep only unified columns
    available = [col for col in UNIFIED_SCHEMA if col != "year"]
    df = df[available]

    # Step 3 — Add year
    df["year"] = int(year)

    # Step 4 — Fill nulls with median
    numeric_cols = df.select_dtypes(include="number").columns
    for col in numeric_cols:
        if df[col].isnull().sum() > 0:
            median_val = df[col].median()
            df[col]    = df[col].fillna(median_val)
            print(f"  [{year}] Filled '{col}' null with median={median_val:.4f}")

    # Step 5 — Enforce data types
    df["year"]            = df["year"].astype(int)
    df["happiness_score"] = df["happiness_score"].astype(float)
    df["gdp"]             = df["gdp"].astype(float)
    df["family"]          = df["family"].astype(float)
    df["health"]          = df["health"].astype(float)
    df["freedom"]         = df["freedom"].astype(float)
    df["generosity"]      = df["generosity"].astype(float)
    df["corruption"]      = df["corruption"].astype(float)

    # Step 6 — Strip whitespace from country names
    df["country"] = df["country"].str.strip()

    # Reorder to match unified schema
    df = df[UNIFIED_SCHEMA]

    print(f"  [{year}] Cleaned  — shape: {df.shape} | nulls: {df.isnull().sum().sum()}")
    return df


def validate_schema(df: pd.DataFrame, year: str) -> None:
    if list(df.columns) != UNIFIED_SCHEMA:
        raise ValueError(
            f"[{year}] Schema mismatch.\n"
            f"  Expected: {UNIFIED_SCHEMA}\n"
            f"  Got:      {list(df.columns)}"
        )
    if df.isnull().sum().sum() > 0:
        raise ValueError(f"[{year}] Null values remain after cleaning.")


def run_cleaning():
    print("\n========== CLEANING PIPELINE START ==========\n")

    os.makedirs(PROCESSED_DIR, exist_ok=True)

    cleaned_frames = []

    for year in ["2015", "2016", "2017", "2018", "2019"]:
        print(f"--- Processing {year} ---")
        raw     = load_raw(year)
        cleaned = clean_dataset(raw, year)
        validate_schema(cleaned, year)
        cleaned_frames.append(cleaned)
        print()

    # Merge all years into a single unified dataset
    df_unified = pd.concat(cleaned_frames, ignore_index=True)

    print(f"Unified dataset shape : {df_unified.shape}")
    print(f"Years present         : {sorted(df_unified['year'].unique())}")
    print(f"Total nulls           : {df_unified.isnull().sum().sum()}")
    print(f"Total duplicates      : {df_unified.duplicated().sum()}")

    # Save to processed folder
    df_unified.to_csv(OUTPUT_FILE, index=False)
    print(f"\nSaved to: {OUTPUT_FILE}")
    print("\n========== CLEANING PIPELINE END ==========\n")


# ------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------
if __name__ == "__main__":
    run_cleaning()
