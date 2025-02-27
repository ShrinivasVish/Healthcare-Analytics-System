import json
import boto3
import os
import time
import logging

# Redshift Data API client
redshift_client = boto3.client("redshift-data")

# Get environment variables
cluster_id = os.environ["REDSHIFT_CLUSTER_ID"]
database = os.environ["REDSHIFT_DB"]
db_user = os.environ["DB_USER"]

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def fetch_query_results(statement_id):
    """Fetch results of a completed query"""
    try:
        response = redshift_client.get_statement_result(Id=statement_id)
        records = response.get("Records", [])

        if not records:
            return 0  # Default to 0 if no records found

        return int(records[0][0]["longValue"])  # Extract the count from the first row
    except Exception as e:
        logger.error(f"Error fetching query results: {str(e)}")
        return None


def execute_redshift_query(query, fetch_result=False):
    """Executes a query in Amazon Redshift using Data API"""
    try:
        logger.info(f"Executing query: {query}")  # Log the query being executed
        response = redshift_client.execute_statement(
            ClusterIdentifier=cluster_id, Database=database, DbUser=db_user, Sql=query
        )
        statement_id = response["Id"]

        # If fetch_result is requested, wait for query completion and fetch results
        if fetch_result:
            status = wait_for_redshift_query(statement_id)
            if status == "FINISHED":
                return fetch_query_results(
                    statement_id
                )  # Function to retrieve query results
            else:
                logger.error(f"Query {query} failed with status: {status}")
                return None

        return statement_id
    except Exception as e:
        logger.error(f"Redshift query execution failed: {str(e)}")
        return None


def wait_for_redshift_query(statement_id):
    """Waits for Redshift query to complete"""
    while True:
        response = redshift_client.describe_statement(Id=statement_id)
        status = response["Status"]
        logger.info(f"Query execution status: {status}")  # Log the status

        if status in ["FINISHED", "FAILED", "ABORTED"]:
            return status

        time.sleep(2)


def lambda_handler(event, context):
    try:
        steps = event["steps"]
        destination_table = None

        # **Step 1: Test Redshift Connection**
        test_query = "SELECT 1;"
        logger.info("Testing Redshift connection by executing: SELECT 1;")
        test_query_id = execute_redshift_query(test_query)
        if not test_query_id:
            raise Exception(
                "Test query execution failed! Lambda cannot connect to Redshift."
            )
        test_status = wait_for_redshift_query(test_query_id)
        if test_status != "FINISHED":
            raise Exception(f"Test query failed with status: {test_status}")
        logger.info(
            "Test query executed successfully! Proceeding with transformations."
        )

        for step in steps:
            logger.info(f"Executing step: {step}")

            source_tables = step["source_table"].split(
                ", "
            )  # Handle multiple source tables
            destination_table = step["destination_table"]
            merge_query = step["merge_query"]

            logger.info(
                f"Source Tables: {source_tables}, Destination Table: {destination_table}"
            )

            total_source_count = 0
            for source_table in source_tables:
                count_query = f"SELECT COUNT(*) FROM {source_table};"
                source_count = execute_redshift_query(count_query, fetch_result=True)
                total_source_count += source_count if source_count else 0

            start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

            insert_start_query = f"""
                INSERT INTO gold.etl_tracker (source_layer, destination_layer, source_table, destination_table, status, completion_timestamp, source_count)
                VALUES ('silver', 'gold', '{step["source_table"]}', '{destination_table}', 'STARTED', '{start_time}', {total_source_count});
            """
            execute_redshift_query(insert_start_query)

            # **Execute the Merge Query**
            transformation_id = execute_redshift_query(merge_query)
            if not transformation_id:
                raise Exception(
                    f"MERGE query execution failed for {destination_table}!"
                )

            status = wait_for_redshift_query(transformation_id)
            if status != "FINISHED":
                raise Exception(
                    f"MERGE query for {destination_table} failed with status {status}!"
                )

            # **Get Destination Table Count After Transformation**
            destination_count_query = f"SELECT COUNT(*) FROM {destination_table};"
            destination_count = execute_redshift_query(
                destination_count_query, fetch_result=True
            )
            destination_count = destination_count if destination_count else 0

            # **Determine Reconciliation Status**
            reconciliation_status = (
                "MATCH" if total_source_count == destination_count else "MISMATCH"
            )

            end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

            update_success_query = f"""
                UPDATE gold.etl_tracker
                SET status = 'SUCCESS', completion_timestamp = '{end_time}', 
                    destination_count = {destination_count}, reconciliation_status = '{reconciliation_status}'
                WHERE destination_table = '{destination_table}' 
                    AND source_table = '{step["source_table"]}' 
                    AND status = 'STARTED';
            """
            execute_redshift_query(update_success_query)

        return {
            "status": "Transformations Completed",
            "details": "All steps executed successfully with reconciliation.",
        }

    except Exception as e:
        logger.error(f"Error occurred in transformation: {str(e)}")
        error_time = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

        if destination_table:
            update_failure_query = f"""
                UPDATE gold.etl_tracker
                SET status = 'FAILED', completion_timestamp = '{error_time}'
                WHERE destination_table = '{destination_table}' 
                    AND status = 'STARTED';
            """
            execute_redshift_query(update_failure_query)

        return {"status": "Error", "message": str(e)}
