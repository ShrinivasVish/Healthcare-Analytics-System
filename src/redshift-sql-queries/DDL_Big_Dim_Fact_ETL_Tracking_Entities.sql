CREATE TABLE silver.tbl_healthcare_analytics_data (
    treatment_id BIGINT PRIMARY KEY,
    treatment_start_date TIMESTAMP NOT NULL,
    treatment_completion_date TIMESTAMP,
    treatment_outcome_status VARCHAR(50) ENCODE zstd,
    treatment_outcome_date TIMESTAMP,
    treatment_duration_in_days INT ENCODE az64,
    treatment_cost DECIMAL(18,2) ENCODE zstd,
    treatment_type VARCHAR(50) ENCODE zstd,

    provider_id BIGINT NOT NULL ENCODE az64,
    provider_full_name VARCHAR(255) ENCODE zstd,
    provider_speciality_id INT ENCODE az64,
    provider_speciality_name VARCHAR(255) ENCODE zstd,
    provider_affiliated_hospital VARCHAR(255) ENCODE zstd,

    location_id BIGINT NOT NULL ENCODE az64,
    location_country VARCHAR(100) ENCODE zstd,
    location_state VARCHAR(100) ENCODE zstd,
    location_city VARCHAR(100) ENCODE zstd,

    patient_id BIGINT NOT NULL ENCODE az64,
    patient_full_name VARCHAR(255) ENCODE zstd,
    patient_gender VARCHAR(255) ENCODE lzo,
    patient_age INT ENCODE az64,

    disease_id BIGINT NOT NULL ENCODE az64,
    disease_speciality_id INT ENCODE az64,
    disease_name VARCHAR(255) ENCODE zstd,
    disease_type VARCHAR(100) ENCODE zstd,
    disease_severity VARCHAR(50) ENCODE zstd,
    disease_transmission_mode VARCHAR(100) ENCODE zstd,
    disease_mortality_rate DECIMAL(5,2) ENCODE az64,

    metadata_added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ENCODE az64,
    metadata_modified_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ENCODE az64,

    treatment_type_and_outcome_status_id INT ENCODE az64
)
DISTSTYLE KEY
DISTKEY(patient_id)
SORTKEY(treatment_start_date, treatment_completion_date);



CREATE TABLE gold.dim_dates (
    date_sk INT PRIMARY KEY ENCODE az64,
    date DATE ENCODE az64,
    day_of_week VARCHAR(10) ENCODE lzo,
    month VARCHAR(10) ENCODE lzo,
    quarter INT ENCODE az64,
    year INT ENCODE az64,
    is_weekend BOOLEAN
)
DISTSTYLE EVEN
SORTKEY(year, month);



CREATE TABLE gold.dim_times (
    time_sk INT PRIMARY KEY ENCODE az64,
    hours INT ENCODE az64,
    minutes INT ENCODE az64,
    seconds INT ENCODE az64
)
DISTSTYLE EVEN
SORTKEY AUTO;



CREATE TABLE gold.dim_treatment_types_and_outcome_statuses (
    treatment_type_and_outcome_status_sk INT PRIMARY KEY ENCODE az64,
    treatment_type VARCHAR(50) ENCODE zstd,
    treatment_outcome_status VARCHAR(50) ENCODE zstd
);



CREATE TABLE gold.dim_locations (
    location_sk INT PRIMARY KEY ENCODE az64,
    country VARCHAR(100) ENCODE zstd,
    state VARCHAR(100) ENCODE zstd,
    city VARCHAR(100) ENCODE zstd
)
DISTSTYLE EVEN;



CREATE TABLE gold.dim_specialities (
    speciality_sk INT PRIMARY KEY ENCODE az64,
    speciality_name VARCHAR(255) ENCODE zstd
);



CREATE TABLE gold.dim_providers (
    provider_sk INT PRIMARY KEY ENCODE az64,
    full_name VARCHAR(255) ENCODE zstd,
    speciality_sk INT ENCODE az64 REFERENCES gold.dim_specialities(speciality_sk),
    affiliated_hospital VARCHAR(255) ENCODE zstd,
    location_sk INT ENCODE az64 REFERENCES gold.dim_locations(location_sk)
)
DISTSTYLE EVEN
INTERLEAVED SORTKEY (location_sk, provider_sk);



CREATE TABLE gold.dim_patients (
    patient_sk INT PRIMARY KEY ENCODE az64,
    full_name VARCHAR(255) ENCODE zstd,
    gender VARCHAR(10) ENCODE lzo,
    age INT ENCODE az64
)
DISTSTYLE EVEN;



CREATE TABLE gold.dim_diseases (
    disease_sk INT PRIMARY KEY ENCODE az64,
    speciality_sk INT ENCODE az64 REFERENCES gold.dim_specialities(speciality_sk),
    name VARCHAR(255) ENCODE zstd,
    type VARCHAR(100) ENCODE zstd,
    severity VARCHAR(50) ENCODE zstd,
    transmission_mode VARCHAR(100) ENCODE zstd,
    mortality_rate DECIMAL(5,2) ENCODE az64
)
DISTSTYLE EVEN;



CREATE TABLE gold.fact_treatments (
    fact_treatment_sk INT PRIMARY KEY ENCODE az64,
    treatment_type_and_outcome_status_sk INT ENCODE az64 REFERENCES gold.dim_treatment_types_and_outcome_statuses(treatment_type_and_outcome_status_sk),
    provider_sk INT ENCODE az64 REFERENCES gold.dim_providers(provider_sk),
    location_sk INT ENCODE az64 REFERENCES gold.dim_locations(location_sk),
    patient_sk INT ENCODE az64 REFERENCES gold.dim_patients(patient_sk),
    disease_sk INT ENCODE az64 REFERENCES gold.dim_diseases(disease_sk),
    speciality_sk INT ENCODE az64 REFERENCES gold.dim_specialities(speciality_sk),

    start_date_sk INT ENCODE az64 REFERENCES gold.dim_dates(date_sk),
    completion_date_sk INT ENCODE az64 REFERENCES gold.dim_dates(date_sk),
    outcome_date_sk INT ENCODE az64 REFERENCES gold.dim_dates(date_sk),
    
    start_time_sk INT ENCODE az64 REFERENCES gold.dim_times(time_sk),
    completion_time_sk INT ENCODE az64 REFERENCES gold.dim_times(time_sk),
    outcome_time_sk INT ENCODE az64 REFERENCES gold.dim_times(time_sk),
    
    duration_in_days INT ENCODE az64,
    cost DECIMAL(18,2) ENCODE zstd
)
DISTSTYLE KEY
DISTKEY(provider_sk)
SORTKEY(start_date_sk, completion_date_sk);



CREATE TABLE gold.etl_tracker (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,  
    source_layer VARCHAR(50) NOT NULL,   
    destination_layer VARCHAR(50) NOT NULL,  
    source_table VARCHAR(200) NOT NULL,  
    destination_table VARCHAR(100) NOT NULL,  
    status VARCHAR(20) NOT NULL,  
    source_count BIGINT DEFAULT NULL,  -- Count of records in source table
    destination_count BIGINT DEFAULT NULL,  -- Count of records in destination table
    reconciliation_status VARCHAR(20) DEFAULT NULL,  -- 'MATCH' or 'MISMATCH'
    completion_timestamp TIMESTAMP DEFAULT NULL,  
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  
);