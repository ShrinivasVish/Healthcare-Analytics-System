-- 1. Rank Providers Based on Total Treatments
DROP TABLE IF EXISTS gold.provider_treatment_rank;
CREATE TABLE gold.provider_treatment_rank AS
SELECT 
    p.provider_sk AS provider_id,
    p.full_name AS provider_full_name,
    COUNT(f.fact_treatment_sk) AS total_treatments
FROM gold.fact_treatments f
JOIN gold.dim_providers p ON f.provider_sk = p.provider_sk
GROUP BY p.provider_sk, p.full_names
ORDER BY total_treatments DESC;



-- 2. Rank Providers Based on Treatment Success Rate
DROP TABLE IF EXISTS gold.provider_success_rate_rank;
CREATE TABLE gold.provider_success_rate_rank AS
SELECT 
    p.provider_sk AS provider_id,
    p.full_name AS provider_full_name,
    COUNT(f.fact_treatment_sk) AS total_treatments,
    COUNT(CASE WHEN f.treatment_type_and_outcome_status_sk = 1 THEN 1 END) * 100.0 / COUNT(f.fact_treatment_sk) AS success_rate
FROM gold.fact_treatments f
JOIN gold.dim_providers p ON f.provider_sk = p.provider_sk
GROUP BY p.provider_sk, p.full_name
ORDER BY success_rate DESC;



-- 3. Monthly Trends in Treatment Success Rates
DROP TABLE IF EXISTS gold.monthly_treatment_trends;
CREATE TABLE gold.monthly_treatment_trends AS
SELECT 
    d.year,
    d.month,
    COUNT(f.fact_treatment_sk) AS total_treatments,
    COUNT(CASE WHEN f.treatment_type_and_outcome_status_sk = 1 THEN 1 END) * 100.0 / COUNT(f.fact_treatment_sk) AS success_rate
FROM gold.fact_treatments f
JOIN gold.dim_dates d ON f.start_date_sk = d.date_sk
GROUP BY d.year, d.month
ORDER BY d.year DESC, d.month DESC;



-- 4. Geographical Distribution of Treatments
DROP TABLE IF EXISTS gold.geographical_treatment_distribution;
CREATE TABLE gold.geographical_treatment_distribution AS
SELECT 
    l.country,
    l.state,
    l.city,
    COUNT(f.fact_treatment_sk) AS total_treatments
FROM gold.fact_treatments f
JOIN gold.dim_locations l ON f.location_sk = l.location_sk
GROUP BY l.country, l.state, l.city
ORDER BY total_treatments DESC;



-- 5. Summary Metrics - Average Treatment Cost
DROP TABLE IF EXISTS gold.summary_avg_treatment_cost;
CREATE TABLE gold.summary_avg_treatment_cost AS
SELECT 
    p.provider_sk AS provider_id,
    p.full_name AS provider_full_name,
    AVG(f.cost) AS avg_treatment_cost
FROM gold.fact_treatments f
JOIN gold.dim_providers p ON f.provider_sk = p.provider_sk
GROUP BY p.provider_sk, p.full_name;



-- 6. Summary Metrics - Total Treatments Per City
DROP TABLE IF EXISTS gold.summary_total_treatments_per_city;
CREATE TABLE gold.summary_total_treatments_per_city AS
SELECT 
    l.city,
    COUNT(f.fact_treatment_sk) AS total_treatments
FROM gold.fact_treatments f
JOIN gold.dim_locations l ON f.location_sk = l.location_sk
GROUP BY l.city
ORDER BY total_treatments DESC;



-- 7. Summary Metrics - Provider Success Rate

DROP TABLE IF EXISTS gold.summary_provider_success_rates;
CREATE TABLE gold.summary_provider_success_rates AS
SELECT 
    p.provider_sk AS provider_id,
    p.full_name AS provider_full_name,
    COUNT(f.fact_treatment_sk) AS total_treatments,
    COUNT(CASE WHEN f.treatment_type_and_outcome_status_sk = 1 THEN 1 END) * 100.0 / COUNT(f.fact_treatment_sk) AS success_rate
FROM gold.fact_treatments f
JOIN gold.dim_providers p ON f.provider_sk = p.provider_sk
GROUP BY p.provider_sk, p.full_name;