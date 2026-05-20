-- -------------------------------------------------------------
-- KPI 1 — Average prediction error (global)
-- -------------------------------------------------------------
SELECT
    ROUND(AVG(prediction_error)::NUMERIC, 4)  AS avg_prediction_error,
    ROUND(MIN(prediction_error)::NUMERIC, 4)  AS min_prediction_error,
    ROUND(MAX(prediction_error)::NUMERIC, 4)  AS max_prediction_error,
    COUNT(*)                                   AS total_predictions
FROM fact_predictions;


-- -------------------------------------------------------------
-- KPI 2 — Predictions by country
-- Total predictions and average error per country
-- -------------------------------------------------------------
SELECT
    c.country_name,
    COUNT(*)                                        AS total_predictions,
    ROUND(AVG(f.prediction_error)::NUMERIC, 4)      AS avg_error,
    ROUND(AVG(f.actual_score)::NUMERIC, 4)          AS avg_actual_score,
    ROUND(AVG(f.predicted_score)::NUMERIC, 4)       AS avg_predicted_score
FROM fact_predictions   f
JOIN dim_country        c ON c.country_id = f.country_id
GROUP BY c.country_name
ORDER BY total_predictions DESC;


-- -------------------------------------------------------------
-- KPI 3 — Predicted vs actual score (per prediction)
-- Used for scatter plot in the dashboard
-- -------------------------------------------------------------
SELECT
    f.prediction_id,
    c.country_name,
    d.year,
    f.actual_score,
    f.predicted_score,
    f.prediction_error,
    f.prediction_timestamp
FROM fact_predictions   f
JOIN dim_country        c ON c.country_id = f.country_id
JOIN dim_date           d ON d.date_id    = f.date_id
ORDER BY f.prediction_timestamp ASC;


-- -------------------------------------------------------------
-- KPI 4 — Prediction trends over time (by year)
-- Average actual vs predicted score per year
-- -------------------------------------------------------------
SELECT
    d.year,
    COUNT(*)                                        AS total_predictions,
    ROUND(AVG(f.actual_score)::NUMERIC, 4)          AS avg_actual_score,
    ROUND(AVG(f.predicted_score)::NUMERIC, 4)       AS avg_predicted_score,
    ROUND(AVG(f.prediction_error)::NUMERIC, 4)      AS avg_error
FROM fact_predictions   f
JOIN dim_date           d ON d.date_id = f.date_id
GROUP BY d.year
ORDER BY d.year ASC;