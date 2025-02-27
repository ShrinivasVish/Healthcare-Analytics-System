import os
import json
import boto3
import time

# Initialize the Redshift Data API client
client = boto3.client("redshift-data")

# Environment variables
DATABASE = os.environ["REDSHIFT_DB"]
DB_USER = os.environ["DB_USER"]
CLUSTER_ID = os.environ["REDSHIFT_CLUSTER_ID"]

# List of queries (DROP + CREATE + INSERT)
ANALYTICAL_QUERIES = [
    "DROP TABLE IF EXISTS gold.provider_treatment_rank; CREATE TABLE gold.provider_treatment_rank AS SELECT p.provider_sk AS provider_id, p.full_name AS provider_full_name, COUNT(f.fact_treatment_sk) AS total_treatments FROM gold.fact_treatments f JOIN gold.dim_providers p ON f.provider_sk = p.provider_sk GROUP BY p.provider_sk, p.full_names ORDER BY total_treatments DESC;",
    "DROP TABLE IF EXISTS gold.provider_success_rate_rank; CREATE TABLE gold.provider_success_rate_rank AS SELECT p.provider_sk AS provider_id, p.full_name AS provider_full_name, COUNT(f.fact_treatment_sk) AS total_treatments, COUNT(CASE WHEN f.treatment_type_and_outcome_status_sk = 1 THEN 1 END) * 100.0 / COUNT(f.fact_treatment_sk) AS success_rate FROM gold.fact_treatments f JOIN gold.dim_providers p ON f.provider_sk = p.provider_sk GROUP BY p.provider_sk, p.full_name ORDER BY success_rate DESC;",
    "DROP TABLE IF EXISTS gold.monthly_treatment_trends; CREATE TABLE gold.monthly_treatment_trends AS SELECT d.year, d.month, COUNT(f.fact_treatment_sk) AS total_treatments, COUNT(CASE WHEN f.treatment_type_and_outcome_status_sk = 1 THEN 1 END) * 100.0 / COUNT(f.fact_treatment_sk) AS success_rate FROM gold.fact_treatments f JOIN gold.dim_dates d ON f.start_date_sk = d.date_sk GROUP BY d.year, d.month ORDER BY d.year DESC, d.month DESC;",
    "DROP TABLE IF EXISTS gold.geographical_treatment_distribution; CREATE TABLE gold.geographical_treatment_distribution AS SELECT l.country, l.state, l.city, COUNT(f.fact_treatment_sk) AS total_treatments FROM gold.fact_treatments f JOIN gold.dim_locations l ON f.location_sk = l.location_sk GROUP BY l.country, l.state, l.city ORDER BY total_treatments DESC;",
    "DROP TABLE IF EXISTS gold.summary_avg_treatment_cost; CREATE TABLE gold.summary_avg_treatment_cost AS SELECT p.provider_sk AS provider_id, p.full_name AS provider_full_name, AVG(f.cost) AS avg_treatment_cost FROM gold.fact_treatments f JOIN gold.dim_providers p ON f.provider_sk = p.provider_sk GROUP BY p.provider_sk, p.full_name;",
    "DROP TABLE IF EXISTS gold.summary_total_treatments_per_city; CREATE TABLE gold.summary_total_treatments_per_city AS SELECT l.city, COUNT(f.fact_treatment_sk) AS total_treatments FROM gold.fact_treatments f JOIN gold.dim_locations l ON f.location_sk = l.location_sk GROUP BY l.city ORDER BY total_treatments DESC;",
    "DROP TABLE IF EXISTS gold.summary_provider_success_rates; CREATE TABLE gold.summary_provider_success_rates AS SELECT p.provider_sk AS provider_id, p.full_name AS provider_full_name, COUNT(f.fact_treatment_sk) AS total_treatments, COUNT(CASE WHEN f.treatment_type_and_outcome_status_sk = 1 THEN 1 END) * 100.0 / COUNT(f.fact_treatment_sk) AS success_rate FROM gold.fact_treatments f JOIN gold.dim_providers p ON f.provider_sk = p.provider_sk GROUP BY p.provider_sk, p.full_name;",
]


def execute_query(query):
    """Execute a single SQL query using the Redshift Data API."""
    response = client.execute_statement(
        ClusterIdentifier=CLUSTER_ID, Database=DATABASE, DbUser=DB_USER, Sql=query
    )
    return response["Id"]  # Query execution ID


def check_query_status(query_id):
    """Check the status of a Redshift Data API query execution."""
    while True:
        response = client.describe_statement(Id=query_id)
        status = response["Status"]

        if status in ["FAILED", "FINISHED", "ABORTED"]:
            return response  # Return full response if finished
        time.sleep(2)  # Wait before checking again


def lambda_handler(event, context):
    """Lambda handler to execute Redshift analytical queries."""
    execution_results = []

    for query in ANALYTICAL_QUERIES:
        try:
            query_id = execute_query(query)
            result = check_query_status(query_id)

            if result["Status"] == "FAILED":
                execution_results.append(
                    {
                        "query_id": query_id,
                        "status": "FAILED",
                        "error": result.get("Error", "Unknown error"),
                    }
                )
            else:
                execution_results.append({"query_id": query_id, "status": "SUCCESS"})
        except Exception as e:
            execution_results.append(
                {"query": query, "status": "FAILED", "error": str(e)}
            )

    return {"statusCode": 200, "body": json.dumps(execution_results)}
