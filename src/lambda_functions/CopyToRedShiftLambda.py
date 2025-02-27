import json
import boto3
import os
import logging
import time

# Initialize Logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    try:
        # Log received event
        logger.info(f"Received Event: {json.dumps(event, indent=2)}")

        # Extract S3 event details
        s3_bucket = event["s3_bucket"]
        s3_key = event["s3_key"]

        logger.info(f"Processing file from bucket: {s3_bucket}, key: {s3_key}")

        # Only process .parquet files
        if not s3_key.endswith(".parquet"):
            logger.info(f"Skipping non-parquet file: {s3_key}")
            return {"status": "skipped"}

        # Ensure the file is in the 'silver-layer/' prefix
        silver_prefix = "silver-layer/"
        if not s3_key.startswith(silver_prefix):
            logger.info(
                f"Skipping file {s3_key} because it is not under the {silver_prefix} prefix."
            )
            return {"status": "skipped"}

        # List objects in the silver-layer/ prefix
        s3_client = boto3.client("s3")
        list_response = s3_client.list_objects_v2(
            Bucket=s3_bucket, Prefix=silver_prefix
        )

        if "Contents" not in list_response or len(list_response["Contents"]) == 0:
            logger.info("No objects found under the silver-layer/ prefix.")
            return {"status": "skipped"}

        # Find the latest object by the LastModified attribute
        latest_object = max(
            list_response["Contents"], key=lambda obj: obj["LastModified"]
        )
        if latest_object["Key"] != s3_key:
            logger.info(
                f"Skipping file {s3_key} because it is not the latest. Latest file is: {latest_object['Key']}"
            )
            return {"status": "skipped"}

        # Environment variables for Redshift parameters
        cluster_id = os.environ["REDSHIFT_CLUSTER_ID"]
        database = os.environ["REDSHIFT_DB"]
        db_user = os.environ["DB_USER"]
        table = os.environ["TABLE"]  # e.g., tbl_healthcare_analytics_data
        iam_role = os.environ["REDSHIFT_ROLE"]  # IAM role with S3 access

        # **Step 1: Get Source Count Before Loading**
        source_count_query = f"SELECT COUNT(*) FROM staging.{table};"
        redshift_data = boto3.client("redshift-data")

        source_count = None
        destination_count = None

        # **Step 2: Insert Tracking Record (ETL Start)**
        start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        insert_start_query = f"""
            INSERT INTO gold.etl_tracker (source_layer, destination_layer, source_table, destination_table, status, completion_timestamp, source_count)
            VALUES ('staging', 'silver', '{table}', '{table}', 'STARTED', '{start_time}', {source_count if source_count is not None else 'NULL'});
        """

        try:
            redshift_data.execute_statement(
                ClusterIdentifier=cluster_id,
                Database=database,
                DbUser=db_user,
                Sql=insert_start_query,
            )
        except Exception as e:
            logger.error(f"Error inserting ETL tracking record: {str(e)}")

        # **Step 3: Execute COPY Command to Load Data into Silver**
        copy_sql = f"""
            COPY {table}
            FROM 's3://{s3_bucket}/{s3_key}'
            IAM_ROLE '{iam_role}'
            FORMAT AS PARQUET;
        """

        try:
            copy_response = redshift_data.execute_statement(
                ClusterIdentifier=cluster_id,
                Database=database,
                DbUser=db_user,
                Sql=copy_sql,
            )
            copy_query_id = copy_response["Id"]
            logger.info(f"Redshift COPY initiated, statement id: {copy_query_id}")
        except Exception as e:
            logger.error(f"Error executing COPY: {str(e)}")
            raise e

        # **Step 4: Wait for Copy Completion**
        time.sleep(10)  # Wait for Redshift COPY to process
        copy_status_response = redshift_data.describe_statement(Id=copy_query_id)

        if copy_status_response["Status"] != "FINISHED":
            raise Exception(
                f"COPY operation failed with status {copy_status_response['Status']}"
            )

        # **Step 5: Get Destination Count After Loading**
        destination_count_query = f"SELECT COUNT(*) FROM {table};"

        try:
            dest_count_response = redshift_data.execute_statement(
                ClusterIdentifier=cluster_id,
                Database=database,
                DbUser=db_user,
                Sql=destination_count_query,
            )
            query_id = dest_count_response["Id"]
            time.sleep(5)  # Wait for query execution

            # Fetch the result
            fetch_response = redshift_data.get_statement_result(Id=query_id)
            destination_count = int(fetch_response["Records"][0][0]["longValue"])
        except Exception as e:
            logger.error(f"Error fetching destination count: {str(e)}")
            destination_count = None  # Set to None if error occurs

        # **Step 6: Update ETL Tracker with Completion Details**
        end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
        reconciliation_status = (
            "MATCH" if source_count == destination_count else "MISMATCH"
        )

        update_success_query = f"""
            UPDATE gold.etl_tracker
            SET status = 'SUCCESS', completion_timestamp = '{end_time}', 
                destination_count = {destination_count if destination_count is not None else 'NULL'}, 
                reconciliation_status = '{reconciliation_status}'
            WHERE destination_table = '{table}' 
                AND source_table = '{table}' 
                AND status = 'STARTED';
        """

        try:
            redshift_data.execute_statement(
                ClusterIdentifier=cluster_id,
                Database=database,
                DbUser=db_user,
                Sql=update_success_query,
            )
        except Exception as e:
            logger.error(f"Error updating ETL tracking record: {str(e)}")

        return {"status": "success", "statement_id": copy_query_id}

    except Exception as e:
        logger.error(f"Error in CopyToRedShiftLambda: {str(e)}")
        error_time = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())

        # **Step 7: Mark ETL Tracker as Failed in Case of Error**
        update_failure_query = f"""
            UPDATE gold.etl_tracker
            SET status = 'FAILED', completion_timestamp = '{error_time}'
            WHERE destination_table = '{table}' 
                AND source_table = '{table}' 
                AND status = 'STARTED';
        """

        try:
            redshift_data.execute_statement(
                ClusterIdentifier=cluster_id,
                Database=database,
                DbUser=db_user,
                Sql=update_failure_query,
            )
        except Exception as e:
            logger.error(f"Error updating failure status in ETL tracker: {str(e)}")

        return {"status": "error", "message": str(e)}
