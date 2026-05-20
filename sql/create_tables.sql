CREATE TABLE IF NOT EXISTS raw_happiness_events (
    raw_event_id            SERIAL          PRIMARY KEY,
    country                 VARCHAR(100),
    year                    SMALLINT,
    gdp                     NUMERIC(8, 4),
    family                  NUMERIC(8, 4),
    health                  NUMERIC(8, 4),
    freedom                 NUMERIC(8, 4),
    generosity              NUMERIC(8, 4),
    corruption              NUMERIC(8, 4),
    actual_happiness_score  NUMERIC(8, 4),
    raw_message             TEXT,                           -- original JSON string from Kafka
    processing_status       VARCHAR(30)     NOT NULL,       -- VALID | INVALID_SCHEMA | INVALID_VALUES | PREDICTION_ERROR
    received_at             TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);


-- -------------------------------------------------------------
-- DIMENSION — Country
-- One row per unique country name.
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_country (
    country_id      SERIAL          PRIMARY KEY,
    country_name    VARCHAR(100)    NOT NULL UNIQUE
);


-- -------------------------------------------------------------
-- DIMENSION — Date
-- One row per year present in the dataset.
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_date (
    date_id     SERIAL      PRIMARY KEY,
    year        SMALLINT    NOT NULL UNIQUE
);


-- -------------------------------------------------------------
-- FACT TABLE — Predictions
-- One row per valid prediction.
-- Links back to the original raw event via raw_event_id.
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_predictions (
    prediction_id           SERIAL          PRIMARY KEY,
    raw_event_id            INTEGER         NOT NULL REFERENCES raw_happiness_events(raw_event_id),
    country_id              INTEGER         NOT NULL REFERENCES dim_country(country_id),
    date_id                 INTEGER         NOT NULL REFERENCES dim_date(date_id),
    actual_score            NUMERIC(8, 4)   NOT NULL,
    predicted_score         NUMERIC(8, 4)   NOT NULL,
    prediction_error        NUMERIC(8, 4)   NOT NULL,   -- ABS(actual - predicted)
    prediction_timestamp    TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);


-- -------------------------------------------------------------
-- INDEXES — improve dashboard query performance
-- -------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_raw_status
    ON raw_happiness_events(processing_status);

CREATE INDEX IF NOT EXISTS idx_raw_country
    ON raw_happiness_events(country);

CREATE INDEX IF NOT EXISTS idx_fact_country
    ON fact_predictions(country_id);

CREATE INDEX IF NOT EXISTS idx_fact_date
    ON fact_predictions(date_id);

CREATE INDEX IF NOT EXISTS idx_fact_timestamp
    ON fact_predictions(prediction_timestamp);
