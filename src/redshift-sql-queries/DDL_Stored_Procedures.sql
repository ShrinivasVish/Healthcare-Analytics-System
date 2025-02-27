CREATE OR REPLACE PROCEDURE silver.sp_populate_dim_dates()
LANGUAGE plpgsql
AS $$
BEGIN
    MERGE INTO gold.dim_dates
    USING (
        SELECT
            ROW_NUMBER() OVER (ORDER BY date_value) AS date_sk,
            date_value AS date,
            TO_CHAR(date_value, 'Day') AS day_of_week,
            TO_CHAR(date_value, 'Month') AS month,
            EXTRACT(QUARTER FROM date_value) AS quarter,
            EXTRACT(YEAR FROM date_value) AS year,
            CASE WHEN EXTRACT(DOW FROM date_value) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
        FROM (
            SELECT DISTINCT date_column::DATE AS date_value
            FROM (
                SELECT treatment_start_date AS date_column FROM silver.tbl_healthcare_analytics_data
                UNION
                SELECT treatment_completion_date FROM silver.tbl_healthcare_analytics_data
                UNION
                SELECT treatment_outcome_date FROM silver.tbl_healthcare_analytics_data
                UNION
                SELECT metadata_added_at::DATE FROM silver.tbl_healthcare_analytics_data
                UNION
                SELECT metadata_modified_at::DATE FROM silver.tbl_healthcare_analytics_data
            ) AS all_dates
        ) AS distinct_dates
    ) AS staging_dates
    ON gold.dim_dates.date = staging_dates.date
    WHEN MATCHED THEN
        UPDATE SET date = staging_dates.date
    WHEN NOT MATCHED THEN
        INSERT (date_sk, date, day_of_week, month, quarter, year, is_weekend)
        VALUES (staging_dates.date_sk, staging_dates.date, staging_dates.day_of_week, staging_dates.month,
                staging_dates.quarter, staging_dates.year, staging_dates.is_weekend);
END;
$$;



CREATE OR REPLACE PROCEDURE silver.sp_populate_dim_times()
LANGUAGE plpgsql
AS $$
BEGIN
    MERGE INTO gold.dim_times
    USING (
        SELECT
            ROW_NUMBER() OVER (ORDER BY hours, minutes, seconds) AS time_sk,
            hours,
            minutes,
            seconds
        FROM (
            SELECT DISTINCT
                EXTRACT(HOUR FROM time_value) AS hours,
                EXTRACT(MINUTE FROM time_value) AS minutes,
                EXTRACT(SECOND FROM time_value) AS seconds
            FROM (
                SELECT treatment_start_date::TIME AS time_value FROM silver.tbl_healthcare_analytics_data
                UNION
                SELECT treatment_completion_date::TIME FROM silver.tbl_healthcare_analytics_data
                UNION
                SELECT treatment_outcome_date::TIME FROM silver.tbl_healthcare_analytics_data
                UNION
                SELECT metadata_added_at::TIME FROM silver.tbl_healthcare_analytics_data
                UNION
                SELECT metadata_modified_at::TIME FROM silver.tbl_healthcare_analytics_data
            ) AS distinct_times
        ) AS unique_times
    ) AS source
    ON gold.dim_times.hours = source.hours
       AND gold.dim_times.minutes = source.minutes
       AND gold.dim_times.seconds = source.seconds

    WHEN MATCHED THEN
        UPDATE SET hours = gold.dim_times.hours  -- No actual change, but required by Redshift

    WHEN NOT MATCHED THEN
        INSERT (time_sk, hours, minutes, seconds)
        VALUES (source.time_sk, source.hours, source.minutes, source.seconds);
END;
$$;



CREATE OR REPLACE PROCEDURE silver.sp_populate_dim_treatment_types_and_outcome_statuses()
LANGUAGE plpgsql
AS $$
BEGIN
    MERGE INTO gold.dim_treatment_types_and_outcome_statuses
    USING (
        SELECT DISTINCT 
            treatment_type_and_outcome_status_id,
            treatment_type,
            treatment_outcome_status
        FROM silver.tbl_healthcare_analytics_data
    ) AS source
    ON gold.dim_treatment_types_and_outcome_statuses.treatment_type_and_outcome_status_sk = source.treatment_type_and_outcome_status_id
    
    WHEN MATCHED THEN
        UPDATE SET 
            treatment_type = source.treatment_type,
            treatment_outcome_status = source.treatment_outcome_status
    
    WHEN NOT MATCHED THEN
        INSERT (treatment_type_and_outcome_status_sk, treatment_type, treatment_outcome_status)
        VALUES (
            source.treatment_type_and_outcome_status_id, 
            source.treatment_type, 
            source.treatment_outcome_status
        );
END;
$$;



CREATE OR REPLACE PROCEDURE silver.sp_populate_dim_locations()
LANGUAGE plpgsql
AS $$
BEGIN
    MERGE INTO gold.dim_locations
    USING (
        SELECT DISTINCT
            location_id,  -- Assuming location_id is the surrogate key from silver table
            location_country,
            location_state,
            location_city
        FROM silver.tbl_healthcare_analytics_data
    ) AS staging_locations
    ON gold.dim_locations.location_sk = staging_locations.location_id
    WHEN MATCHED THEN
        UPDATE SET
            country = staging_locations.location_country,
            state = staging_locations.location_state,
            city = staging_locations.location_city
    WHEN NOT MATCHED THEN
        INSERT (location_sk, country, state, city)
        VALUES (
            staging_locations.location_id,
            staging_locations.location_country,
            staging_locations.location_state,
            staging_locations.location_city
        );
END;
$$;



CREATE OR REPLACE PROCEDURE silver.sp_populate_dim_specialities()
LANGUAGE plpgsql
AS $$
BEGIN
    MERGE INTO gold.dim_specialities
    USING (
        SELECT DISTINCT
            provider_speciality_id,  -- Assuming speciality_id is the surrogate key from silver table
            provider_speciality_name
        FROM silver.tbl_healthcare_analytics_data
    ) AS source
    ON gold.dim_specialities.speciality_sk = source.provider_speciality_id
    
    WHEN MATCHED THEN
        UPDATE SET
            speciality_name = source.provider_speciality_name
    
    WHEN NOT MATCHED THEN
        INSERT (speciality_sk, speciality_name)
        VALUES (
            source.provider_speciality_id,
            source.provider_speciality_name
        );
END;
$$;



CREATE OR REPLACE PROCEDURE silver.sp_populate_dim_providers()
LANGUAGE plpgsql
AS $$
BEGIN
    MERGE INTO gold.dim_providers
    USING (
        SELECT DISTINCT
            provider_id,
            provider_full_name,
            provider_speciality_id,
            provider_affiliated_hospital,
            location_id
        FROM silver.tbl_healthcare_analytics_data
    ) AS source
    ON gold.dim_providers.provider_sk = source.provider_id
    
    WHEN MATCHED THEN
        UPDATE SET
            full_name = source.provider_full_name,
            speciality_sk = source.provider_speciality_id,
            affiliated_hospital = source.provider_affiliated_hospital,
            location_sk = source.location_id
    
    WHEN NOT MATCHED THEN
        INSERT (provider_sk, full_name, speciality_sk, affiliated_hospital, location_sk)
        VALUES (
            source.provider_id,
            source.provider_full_name,
            source.provider_speciality_id,
            source.provider_affiliated_hospital,
            source.location_id
        );
END;
$$;



CREATE OR REPLACE PROCEDURE silver.sp_populate_dim_patients()
LANGUAGE plpgsql
AS $$
BEGIN
    MERGE INTO gold.dim_patients
    USING (
        SELECT DISTINCT
            patient_id,
            patient_full_name,
            patient_gender,
            patient_age
        FROM silver.tbl_healthcare_analytics_data
    ) AS source
    ON gold.dim_patients.patient_sk = source.patient_id
    
    WHEN MATCHED THEN
        UPDATE SET
            full_name = source.patient_full_name,
            gender = source.patient_gender,
            age = source.patient_age
    
    WHEN NOT MATCHED THEN
        INSERT (patient_sk, full_name, gender, age)
        VALUES (
            source.patient_id,
            source.patient_full_name,
            source.patient_gender,
            source.patient_age
        );
END;
$$;



CREATE OR REPLACE PROCEDURE silver.sp_populate_dim_diseases()
LANGUAGE plpgsql
AS $$
BEGIN
    MERGE INTO gold.dim_diseases
    USING (
        SELECT DISTINCT
            disease_id,
            disease_speciality_id,
            disease_name,
            disease_type,
            disease_severity,
            disease_transmission_mode,
            disease_mortality_rate
        FROM silver.tbl_healthcare_analytics_data
    ) AS source_data
    ON gold.dim_diseases.disease_sk = source_data.disease_id
    WHEN MATCHED THEN
        UPDATE SET
            speciality_sk = source_data.disease_speciality_id,
            name = source_data.disease_name,
            type = source_data.disease_type,
            severity = source_data.disease_severity,
            transmission_mode = source_data.disease_transmission_mode,
            mortality_rate = source_data.disease_mortality_rate
    WHEN NOT MATCHED THEN
        INSERT (disease_sk, speciality_sk, name, type, severity, transmission_mode, mortality_rate)
        VALUES (
            source_data.disease_id,
            source_data.disease_speciality_id,
            source_data.disease_name,
            source_data.disease_type,
            source_data.disease_severity,
            source_data.disease_transmission_mode,
            source_data.disease_mortality_rate
        );
END;
$$;



CREATE OR REPLACE PROCEDURE silver.sp_populate_fact_treatments()
LANGUAGE plpgsql
AS $$
BEGIN
    MERGE INTO gold.fact_treatments 
    USING (
        SELECT DISTINCT
            t.treatment_id,
            t.treatment_type_and_outcome_status_id,
            t.provider_id,
            t.location_id,
            t.patient_id,
            t.disease_id,
            t.provider_speciality_id,
            d.start_date_sk,
            d.completion_date_sk,
            d.outcome_date_sk,
            tm.start_time_sk,
            tm.completion_time_sk,
            tm.outcome_time_sk,
            t.treatment_duration_in_days,
            t.treatment_cost
        FROM silver.tbl_healthcare_analytics_data t
        JOIN (
            SELECT DISTINCT
                treatment_start_date,
                treatment_completion_date,
                treatment_outcome_date,
                DATE_PART(EPOCH, treatment_start_date)::INT AS start_date_sk,
                CASE 
                    WHEN treatment_completion_date IS NOT NULL 
                    THEN DATE_PART(EPOCH, treatment_completion_date)::INT 
                    ELSE NULL 
                END AS completion_date_sk,
                CASE 
                    WHEN treatment_outcome_date IS NOT NULL 
                    THEN DATE_PART(EPOCH, treatment_outcome_date)::INT 
                    ELSE NULL 
                END AS outcome_date_sk
            FROM silver.tbl_healthcare_analytics_data
            WHERE treatment_start_date IS NOT NULL
        ) d
            ON t.treatment_start_date = d.treatment_start_date
            AND COALESCE(t.treatment_completion_date, '1900-01-01'::DATE) = COALESCE(d.treatment_completion_date, '1900-01-01'::DATE)
            AND COALESCE(t.treatment_outcome_date, '1900-01-01'::DATE) = COALESCE(d.treatment_outcome_date, '1900-01-01'::DATE)
        JOIN (
            SELECT DISTINCT
                treatment_start_date,
                treatment_completion_date,
                treatment_outcome_date,
                (EXTRACT(HOUR FROM treatment_start_date::TIME) * 3600 + 
                 EXTRACT(MINUTE FROM treatment_start_date::TIME) * 60 + 
                 EXTRACT(SECOND FROM treatment_start_date::TIME))::INT AS start_time_sk,
                CASE 
                    WHEN treatment_completion_date IS NOT NULL 
                    THEN (EXTRACT(HOUR FROM treatment_completion_date::TIME) * 3600 + 
                          EXTRACT(MINUTE FROM treatment_completion_date::TIME) * 60 + 
                          EXTRACT(SECOND FROM treatment_completion_date::TIME))::INT 
                    ELSE NULL 
                END AS completion_time_sk,
                CASE 
                    WHEN treatment_outcome_date IS NOT NULL 
                    THEN (EXTRACT(HOUR FROM treatment_outcome_date::TIME) * 3600 + 
                          EXTRACT(MINUTE FROM treatment_outcome_date::TIME) * 60 + 
                          EXTRACT(SECOND FROM treatment_outcome_date::TIME))::INT 
                    ELSE NULL 
                END AS outcome_time_sk
            FROM silver.tbl_healthcare_analytics_data
            WHERE treatment_start_date IS NOT NULL
        ) tm
            ON t.treatment_start_date = tm.treatment_start_date
            AND COALESCE(t.treatment_completion_date, '1900-01-01'::DATE) = COALESCE(tm.treatment_completion_date, '1900-01-01'::DATE)
            AND COALESCE(t.treatment_outcome_date, '1900-01-01'::DATE) = COALESCE(tm.treatment_outcome_date, '1900-01-01'::DATE)
    ) source_data
    ON gold.fact_treatments.fact_treatment_sk = source_data.treatment_id
    WHEN MATCHED THEN
        UPDATE SET
            treatment_type_and_outcome_status_sk = source_data.treatment_type_and_outcome_status_id,
            provider_sk = source_data.provider_id,
            location_sk = source_data.location_id,
            patient_sk = source_data.patient_id,
            disease_sk = source_data.disease_id,
            speciality_sk = source_data.provider_speciality_id,
            start_date_sk = source_data.start_date_sk,
            completion_date_sk = source_data.completion_date_sk,
            outcome_date_sk = source_data.outcome_date_sk,
            start_time_sk = source_data.start_time_sk,
            completion_time_sk = source_data.completion_time_sk,
            outcome_time_sk = source_data.outcome_time_sk,
            duration_in_days = source_data.treatment_duration_in_days,
            cost = source_data.treatment_cost
    WHEN NOT MATCHED THEN
        INSERT (
            fact_treatment_sk,
            treatment_type_and_outcome_status_sk,
            provider_sk,
            location_sk,
            patient_sk,
            disease_sk,
            speciality_sk,
            start_date_sk,
            completion_date_sk,
            outcome_date_sk,
            start_time_sk,
            completion_time_sk,
            outcome_time_sk,
            duration_in_days,
            cost
        ) VALUES (
            source_data.treatment_id,
            source_data.treatment_type_and_outcome_status_id,
            source_data.provider_id,
            source_data.location_id,
            source_data.patient_id,
            source_data.disease_id,
            source_data.provider_speciality_id,
            source_data.start_date_sk,
            source_data.completion_date_sk,
            source_data.outcome_date_sk,
            source_data.start_time_sk,
            source_data.completion_time_sk,
            source_data.outcome_time_sk,
            source_data.treatment_duration_in_days,
            source_data.treatment_cost
        );
END $$;