# Streaming ETL with Apache Kafka and Machine Learning
**Course:** ETL (G01) — Workshop 3  
**Program:** Data Engineering and Artificial Intelligence  
**Universidad Autónoma de Occidente**

---

## Table of Contents
- [Streaming ETL with Apache Kafka and Machine Learning](#streaming-etl-with-apache-kafka-and-machine-learning)
  - [Table of Contents](#table-of-contents)
  - [1. Project Description](#1-project-description)
  - [2. General Architecture](#2-general-architecture)
  - [3. Folder Structure](#3-folder-structure)
  - [4. Part A — Data Profiling and Machine Learning](#4-part-a--data-profiling-and-machine-learning)
    - [Step 1 — Exploratory Data Analysis](#step-1--exploratory-data-analysis)
    - [Step 2 — Data Cleaning and Harmonization](#step-2--data-cleaning-and-harmonization)
    - [Step 3 — Feature Engineering](#step-3--feature-engineering)
    - [Step 4 — Model Training and Selection](#step-4--model-training-and-selection)
  - [5. Part B — Streaming ETL with Apache Kafka](#5-part-b--streaming-etl-with-apache-kafka)
    - [Step 5 — Kafka Producer](#step-5--kafka-producer)
    - [Step 6 — Kafka Consumer](#step-6--kafka-consumer)
  - [6. Part C — Prediction Storage and Analytics](#6-part-c--prediction-storage-and-analytics)
    - [Step 7 — Database Design](#step-7--database-design)
    - [Step 8 — Load Raw Events and Prediction Results](#step-8--load-raw-events-and-prediction-results)
    - [Step 9 — Dashboard and KPIs](#step-9--dashboard-and-kpis)
    - [KPI 1 — Average Prediction Error](#kpi-1--average-prediction-error)
    - [KPI 2 — Total Predictions and Average Score by Country](#kpi-2--total-predictions-and-average-score-by-country)
    - [KPI 3 — Actual Score vs Predicted Score](#kpi-3--actual-score-vs-predicted-score)
    - [KPI 4 — Average Actual Score vs Average Predicted Score by Year](#kpi-4--average-actual-score-vs-average-predicted-score-by-year)
  - [7. Execution Instructions](#7-execution-instructions)
    - [Prerequisites](#prerequisites)
    - [1. Clone and set up environment](#1-clone-and-set-up-environment)
    - [2. Place raw data files](#2-place-raw-data-files)
    - [3. Run the offline ETL pipeline](#3-run-the-offline-etl-pipeline)
    - [4. Start infrastructure](#4-start-infrastructure)
    - [5. Run the streaming pipeline](#5-run-the-streaming-pipeline)
    - [6. Connect the dashboard](#6-connect-the-dashboard)
  - [8. Technical Requirements](#8-technical-requirements)

---

## 1. Project Description

This project implements a full end-to-end streaming ETL pipeline that integrates historical World Happiness Report data (2015–2019), trains a regression model to predict happiness scores, and deploys it as a real-time inference service using Apache Kafka.

The pipeline is divided into two main processes:

**Offline process** — runs once to prepare data and train the model:
```
Raw CSV Files → EDA → Cleaning → Feature Engineering → Model Training → model.pkl
```

**Streaming process** — runs continuously to generate real-time predictions:
```
CSV Data → Kafka Producer → Kafka Topic → Kafka Consumer → PostgreSQL → Dashboard
```

The focus of this project is **pipeline integration, clean architecture, and reproducibility** — not model accuracy optimization.

---

## 2. General Architecture

```
OFFLINE PROCESS
───────────────────────────────────────────────
data/raw/
  2015.csv
  2016.csv          ──► cleaning.py ──► happiness_unified.csv
  2017.csv
  2018.csv                              feature_engineering.py ──► happiness_ml_ready.csv
  2019.csv                                                          scaler.pkl

                                        model_training.py ──► model.pkl

STREAMING PROCESS
───────────────────────────────────────────────
happiness_ml_ready.csv
       │
       ▼
  producer.py  ──►  Kafka Topic: happiness-predictions
                              │
                              ▼
                        consumer.py
                              │
                    ┌─────────┴──────────┐
                    ▼                    ▼
          raw_happiness_events    fact_predictions
                                        │
                                        ▼
                                   Dashboard & KPIs
```

---

## 3. Folder Structure

```
project/
│
├── data/
│   ├── raw/                    ← original CSV files (2015–2019)
│   ├── processed/              ← cleaned and ML-ready datasets
│   └── streaming/
│
├── notebooks/
│   └── eda.ipynb               ← exploratory data analysis
│
├── etl/
│   ├── __init__.py
│   ├── cleaning.py             ← Step 2: cleaning and harmonization
│   ├── feature_engineering.py  ← Step 3: feature selection and scaling
│   └── model_training.py       ← Step 4: model training and serialization
│
├── kafka/
│   ├── producer.py             ← Step 5: streams events to Kafka
│   └── consumer.py             ← Step 6: inference and DB storage
│
├── models/
│   ├── model.pkl               ← serialized RandomForestRegressor
│   └── scaler.pkl              ← fitted StandardScaler
│
├── sql/
│   ├── create_tables.sql       ← database schema
│   └── kpis.sql                ← dashboard KPI queries
│
├── dashboards/                 ← screenshots and dashboard files
│
├── main.py                     ← single entry point for the ETL pipeline
├── docker-compose.yml          ← Kafka + Zookeeper + PostgreSQL
├── requirements.txt
└── README.md
```

---

## 4. Part A — Data Profiling and Machine Learning

### Step 1 — Exploratory Data Analysis

EDA was performed on all five datasets using `notebooks/eda.ipynb`.

**Key findings:**

| Year | Rows | Schema issues | Nulls |
|------|------|---------------|-------|
| 2015 | 158  | `Happiness Score` (spaces), `Economy (GDP per Capita)`, has `Region` | None |
| 2016 | 157  | Same as 2015, adds `Lower/Upper Confidence Interval` | None |
| 2017 | 155  | Dot notation: `Happiness.Score`, `Economy..GDP.per.Capita.` | None |
| 2018 | 156  | `Country or region`, `Score`, `Social support` | 1 null in `Perceptions of corruption` |
| 2019 | 156  | Same schema as 2018 | None |

**Schema differences identified:**
- The column for country name uses three different names across years: `Country` (2015–2017), `Country or region` (2018–2019).
- The happiness score column uses three different names: `Happiness Score` (2015–2016), `Happiness.Score` (2017), `Score` (2018–2019).
- The GDP column uses `Economy (GDP per Capita)` in 2015–2016, `Economy..GDP.per.Capita.` in 2017 (dot notation), and `GDP per capita` in 2018–2019.
- The family/social support column is called `Family` in 2015–2017 and `Social support` in 2018–2019.
- The corruption column is called `Trust (Government Corruption)` in 2015–2017 and `Perceptions of corruption` in 2018–2019.
- The `Region` column only exists in 2015 and 2016.
- Extra columns exclusive to certain years: `Standard Error` (2015), `Lower/Upper Confidence Interval` (2016), `Whisker.high/low` and `Dystopia.Residual` (2017), `Overall rank` (2018–2019).

**No duplicate rows** were found in any year.

---

### Step 2 — Data Cleaning and Harmonization

Implemented in `etl/cleaning.py`.

The cleaning function processes each year independently and applies the following steps in order:

1. Rename columns using a per-year mapping to the unified schema.
2. Keep only the unified schema columns, dropping everything else.
3. Add a `year` column as an integer.
4. Fill missing values with the column median.
5. Enforce explicit data types for all columns.
6. Strip whitespace from country names.

**Unified schema after cleaning:**

```
country | year | happiness_score | gdp | family | health | freedom | generosity | corruption
```

**Cleaning decisions and justifications:**

| Decision | Justification |
|----------|---------------|
| Drop `Happiness Rank` / `Overall rank` | This column is directly derived from `happiness_score` — including it as a feature would cause **target leakage** since a model trained with rank would be using information that depends on the target variable. |
| Drop `Region` | Only present in 2015 and 2016. Cannot be reliably recovered for 2017–2019 without an external source. Including it would create a column with 60% missing data in the unified dataset, which is not recoverable through imputation for a categorical variable of this nature. |
| Drop `Standard Error` | Only present in 2015. Not a predictive feature — it describes the uncertainty of the happiness score measurement, not a happiness driver. |
| Drop `Dystopia Residual` | Only present in 2015–2017. It is a component used internally by the World Happiness Report methodology to construct the score, so including it would constitute target leakage. |
| Drop `Whisker.high` / `Whisker.low` | Only present in 2017. These are confidence interval bounds derived from the score, not independent features. |
| Drop `Lower/Upper Confidence Interval` | Only present in 2016. Same reasoning as above. |
| Fill 1 null in `corruption` (2018) | Only 1 missing value out of 156 rows (0.6%). Dropping the row would lose a valid country record. Filling with the column median is a standard, conservative approach that introduces minimal bias. |
| Add `year` column | Required for temporal traceability after merging, and included in the Kafka event schema. |
| Standardize to snake_case | All five datasets use different naming conventions. A unified snake_case schema ensures consistent programmatic access across the entire pipeline. |

**Result:** A single unified file `data/processed/happiness_unified.csv` with 782 records (sum of all years) and zero nulls.

---

### Step 3 — Feature Engineering

Implemented in `etl/feature_engineering.py`.

**Feature selection:**

All six available numeric features were selected for the model. The decision was based on Pearson correlation with `happiness_score` computed on the unified dataset:

| Feature | Correlation with `happiness_score` | Decision | Justification |
|---------|-----------------------------------|----------|---------------|
| `gdp` | **0.789** | ✅ Selected | Strongest predictor. Economic prosperity is consistently the top driver of happiness across all years. |
| `health` | **0.743** | ✅ Selected | Strong predictor. Life expectancy is a direct proxy for quality of life. |
| `family` | **0.649** | ✅ Selected | Strong predictor. Social support is a well-established happiness driver in the WHR methodology. |
| `freedom` | **0.551** | ✅ Selected | Moderate-strong predictor. Freedom to make life choices has consistent impact across regions. |
| `corruption` | **0.397** | ✅ Selected | Moderate predictor. Low corruption perception contributes meaningfully to trust and happiness. |
| `generosity` | **0.138** | ✅ Selected | Weak correlation but kept because: (1) it is part of the official WHR framework, (2) it is present in the Kafka event schema, and (3) the workshop focuses on pipeline integration, not feature selection optimization. |
| `year` | — | ❌ Dropped | Not a happiness predictor — it is a temporal identifier. Including it would introduce spurious patterns. |
| `country` | — | ❌ Dropped | Categorical variable with 150+ unique values. Encoding it would add high cardinality noise without meaningful predictive value in a simple regression model. |

**Scaling decision:**

`StandardScaler` was applied to all selected features before training.

The features have noticeably different standard deviations (ranging from 0.10 for `corruption` to 0.41 for `gdp`). Without scaling, features with larger magnitudes — particularly `gdp` — would dominate the model's decision boundaries in algorithms sensitive to feature scale. `StandardScaler` transforms each feature to mean=0 and std=1, ensuring all features contribute proportionally.

The scaler is fitted on the full dataset and saved as `models/scaler.pkl`. The Kafka producer applies this scaler to each record before streaming it, so the consumer can feed features directly into the model without any additional transformation.

**Outputs:**
- `data/processed/happiness_ml_ready.csv` — scaled features + unscaled target + traceability columns
- `models/scaler.pkl` — fitted scaler used by the producer

---

### Step 4 — Model Training and Selection

Implemented in `etl/model_training.py`.

All three suggested models were trained and evaluated using the same 70/30 train/test split with `random_state=42` to ensure reproducibility and a fair comparison.

**Evaluation results:**

| Model | MAE | RMSE | R² | Notes |
|-------|-----|------|----|-------|
| LinearRegression | 0.4486 | 0.5843 | 0.727 | Solid baseline, assumes linear relationships |
| **RandomForestRegressor** | **0.4069** | **0.5237** | **0.780** | ✅ **Selected** |
| DecisionTreeRegressor | 0.6002 | 0.7891 | 0.501 | Overfits — worst performance on test set |

**Model selected: `RandomForestRegressor(n_estimators=100, random_state=42)`**

**Justification:**
- Achieves the **lowest MAE (0.4069)** — smallest average prediction error per record.
- Achieves the **lowest RMSE (0.5237)** — most robust against large individual errors.
- Achieves the **highest R² (0.780)** — explains 78% of the variance in happiness scores on unseen data.
- The `DecisionTreeRegressor` shows clear overfitting: it fits training data perfectly but degrades significantly on the test set (R²=0.501), making it unreliable for streaming inference.
- `RandomForestRegressor` captures non-linear relationships between features (e.g., the interaction between GDP and health) that `LinearRegression` cannot model.
- With `n_estimators=100` and default parameters, it remains simple enough to align with the workshop's emphasis on **pipeline integration over model complexity**.

**Output:** `models/model.pkl`

---

## 5. Part B — Streaming ETL with Apache Kafka

The streaming infrastructure runs on Docker Compose with three services: Zookeeper, Kafka Broker, and PostgreSQL. All services share a common network `etl_network`.

```bash
docker compose up -d
```

### Step 5 — Kafka Producer

Implemented in `kafka/producer.py`.

The producer reads `data/processed/happiness_ml_ready.csv` (scaled features) and streams one record at a time to the Kafka topic `happiness-predictions` as a JSON event.

**Event format sent to Kafka:**

```json
{
    "country": "Colombia",
    "year": 2019,
    "gdp": 1.2,
    "family": 0.8,
    "health": 0.9,
    "freedom": 0.6,
    "generosity": 0.3,
    "corruption": 0.1,
    "actual_happiness_score": 6.2
}
```

**Key implementation details:**
- Events are serialized as UTF-8 encoded JSON using `json.dumps(v, ensure_ascii=False).encode("utf-8")`. The `ensure_ascii=False` flag preserves special characters in country names (e.g., accented characters) without escaping them.
- A `0.5` second delay between events simulates a real-time data stream.
- `producer.flush()` is called at the end to ensure all buffered messages are delivered before the process exits.
- Features are sent in their scaled form so the consumer can feed them directly into the model without any additional transformation.

```python
# kafka/producer.py — core streaming loop
for idx, row in df.iterrows():
    event = build_event(row)
    producer.send(TOPIC, value=event)
    time.sleep(DELAY_SECONDS)

producer.flush()
producer.close()
```

---

### Step 6 — Kafka Consumer

Implemented in `kafka/consumer.py`.

The consumer listens continuously on the `happiness-predictions` topic. For each message received it performs three steps in strict order: store the raw event, generate a prediction, store the prediction result.

**Key implementation details:**
- `auto_offset_reset="earliest"` ensures the consumer reads all messages from the beginning of the topic if no prior offset exists for the group.
- `group_id` allows Kafka to track which messages have already been processed.
- The model is loaded once at startup (`model.pkl`) and reused for every prediction, avoiding repeated disk reads.
- Features are extracted from the event in the exact order defined by `FEATURES`, matching the order used during training.

```python
# kafka/consumer.py — core processing loop
for message in consumer:
    event       = message.value
    raw_message = json.dumps(event)

    # Step 1 — Store raw event before any processing
    raw_event_id = insert_raw_event(engine, event, raw_message)

    # Step 2 — Predict directly (features already scaled by producer)
    feature_vector  = [[float(event[f]) for f in FEATURES]]
    predicted_score = float(model.predict(feature_vector)[0])

    # Step 3 — Store prediction result linked to the raw event
    insert_prediction(engine, raw_event_id, event, predicted_score)
```

**Why store the raw event first?**

The workshop explicitly requires storing the original Kafka message in `raw_happiness_events` before any transformation or prediction. This design supports:
- **Traceability** — every prediction can be linked back to the exact event that produced it via `raw_event_id`.
- **Auditing** — the original message is preserved unchanged regardless of what the pipeline does with it.
- **Reprocessing** — if prediction logic changes in the future, raw events can be replayed without needing to re-stream from the source.

---

## 6. Part C — Prediction Storage and Analytics

### Step 7 — Database Design

Defined in `sql/create_tables.sql`. The schema follows a small star model with one raw table, one fact table, and two dimension tables.

<img width="998" height="684" alt="PBIDesktop_TwCNTMBvVz" src="https://github.com/user-attachments/assets/a451b2d3-de9d-4fb6-8d01-9a9aa1582674" />


**Design decisions:**
- `raw_happiness_events` stores every Kafka event exactly as received, with a `processing_status` field for pipeline monitoring.
- `fact_predictions` only contains valid predictions and links back to the raw event via `raw_event_id`, preserving full traceability.
- `dim_country` and `dim_date` are populated incrementally using upsert logic in the consumer — no pre-loading required.
- Indexes are created on `country_id`, `date_id`, and `prediction_timestamp` to support fast dashboard queries.

---

### Step 8 — Load Raw Events and Prediction Results

The consumer inserts data into the database in two steps per message:

1. `insert_raw_event()` — inserts the original Kafka message into `raw_happiness_events` and returns the `raw_event_id`.
2. `insert_prediction()` — inserts the prediction result into `fact_predictions`, referencing the `raw_event_id` from step 1.

This guarantees that every prediction in `fact_predictions` can always be traced back to its original raw event.

---

### Step 9 — Dashboard and KPIs

<img width="1429" height="802" alt="PBIDesktop_FfOJPR3PQZ" src="https://github.com/user-attachments/assets/d5d670ea-ff90-4f66-89e5-34c31ee7a8e1" />

The dashboard connects directly to PostgreSQL and queries `fact_predictions` joined with the dimension tables. It does **not** read from CSV files.

KPI queries are defined in `sql/kpis.sql`.

**Required KPIs:**

| KPI | Query logic |
|-----|-------------|
| Average prediction error | `AVG(prediction_error)` from `fact_predictions` |
| Predictions by country | `COUNT(*)` and `AVG(prediction_error)` grouped by `dim_country.country_name` |
| Predicted vs actual score | `actual_score` and `predicted_score` per prediction for scatter plot |
| Prediction trends over time | `AVG(actual_score)` and `AVG(predicted_score)` grouped by `dim_date.year` |

---

### KPI 1 — Average Prediction Error

**Visual:** Card (single metric)  
**Value:** `0.22`  
**Query:** `SELECT ROUND(AVG(prediction_error)::NUMERIC, 4) FROM fact_predictions`

The global average absolute error across all 782 predictions is **0.22 points** on the happiness score scale (0–10). This means the model's predictions deviate from the actual scores by 0.22 points on average, which is consistent with the RMSE of 0.52 obtained during training on the test set. The difference is expected since the dashboard aggregates all years including the training set records.

---

### KPI 2 — Total Predictions and Average Score by Country

**Visual:** Map (bubble map with country coordinates)  
**Query:** `COUNT(*)` and `AVG(predicted_score)` grouped by `dim_country.country_name`

Each bubble on the map represents a country. Bubble size reflects the number of predictions recorded for that country across all years (2015–2019). Countries with data across all five years appear with larger bubbles. The map provides a geographic overview of coverage and allows identifying regional happiness patterns — Northern Europe and Oceania consistently show higher predicted scores, while Sub-Saharan Africa shows lower values.

---

### KPI 3 — Actual Score vs Predicted Score

**Visual:** Scatter plot  
**Query:** `actual_score` and `predicted_score` per prediction from `fact_predictions`

Each point represents one prediction. The dashed diagonal line represents perfect prediction (predicted = actual). Points close to the diagonal indicate accurate predictions. The scatter plot shows that the model performs well across the full score range (approximately 2.5 to 8.0), with slightly higher dispersion in the mid-range scores (4.0–6.0), which is consistent with the RandomForestRegressor's behavior on the test set (R²=0.780). No systematic over- or under-prediction bias is visible.

---

### KPI 4 — Average Actual Score vs Average Predicted Score by Year

**Visual:** Line and bar combo chart  
**Query:** `AVG(actual_score)`, `AVG(predicted_score)`, and `AVG(prediction_error)` grouped by `dim_date.year`

The chart shows three series over the years 2015–2019:
- **Blue line** — average actual happiness score per year
- **Orange dotted line** — average predicted happiness score per year
- **Red bars** — average prediction error per year

## 7. Execution Instructions

### Prerequisites
- Python 3.10+
- Docker and Docker Compose
- Virtual environment

### 1. Clone and set up environment

```bash
git clone <repository_url>
cd project

python -m venv venv
source venv/bin/activate        # Mac/Linux
venv\Scripts\activate           # Windows

pip install -r requirements.txt
```

### 2. Place raw data files

Download the CSV files from [Kaggle](https://www.kaggle.com/datasets/unsdsn/world-happiness)

Copy the five CSV files into `data/raw/`:
```
data/raw/2015.csv
data/raw/2016.csv
data/raw/2017.csv
data/raw/2018.csv
data/raw/2019.csv
```

### 3. Run the offline ETL pipeline

This single command runs cleaning, feature engineering and model training in order:

```bash
python main.py
```

Expected output:
```
[1/3] Step 1 — Data Cleaning & Harmonization    ... Completed
[2/3] Step 2 — Feature Engineering              ... Completed
[3/3] Step 3 — Model Training                   ... Completed

PIPELINE FINISHED
```

Artifacts generated:
- `data/processed/happiness_unified.csv`
- `data/processed/happiness_ml_ready.csv`
- `models/scaler.pkl`
- `models/model.pkl`

### 4. Start infrastructure

```bash
docker compose up -d
```

This starts Zookeeper (port 2181), Kafka (port 9092), and PostgreSQL (port 5432). The database tables are created automatically from `sql/create_tables.sql` on first startup.

### 5. Run the streaming pipeline

Open two terminals:

```bash
# Terminal 1 — start consumer first
python kafka/consumer.py

# Terminal 2 — start producer
python kafka/producer.py
```

The consumer will print each prediction as it arrives:
```
[0001] Switzerland (2015) | predicted=7.412 | actual=7.587 | error=0.175
[0002] Iceland (2015)     | predicted=7.389 | actual=7.561 | error=0.172
```

### 6. Connect the dashboard

Connect your BI tool (Power BI, Looker Studio, or Tableau) to PostgreSQL:

```
Host     : localhost
Port     : 5432
Database : etl_db
User     : etl_user
Password : etl_pass
```

Use the queries in `sql/kpis.sql` as data sources for each KPI visualization.

---

## 8. Technical Requirements

| Component | Technology |
|-----------|-----------|
| Language | Python 3.10+ |
| Data processing | pandas, numpy |
| Machine learning | scikit-learn, joblib |
| Streaming | Apache Kafka, kafka-python |
| Database | PostgreSQL 16, SQLAlchemy, psycopg2 |
| Infrastructure | Docker, Docker Compose |
| Visualization | Power BI / Looker Studio / Tableau |
| Notebook | Jupyter |
