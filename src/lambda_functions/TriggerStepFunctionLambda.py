import json
import boto3
import os
import logging

# Get Step Function ARN from environment variable
step_function_arn = os.environ['STEP_FUNCTION_ARN']

# Set up logging for better traceability
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info(f"Received Event: {json.dumps(event, indent=2)}")  

    try:
        s3_bucket = event["Records"][0]["s3"]["bucket"]["name"]
        s3_key = event["Records"][0]["s3"]["object"]["key"]
        logger.info(f"Bucket: {s3_bucket}")
        logger.info(f"File: {s3_key}")
        # return {"message": f"Processed {key} from {bucket}"}

        # Create a Step Functions client
        step_client = boto3.client('stepfunctions')
        
        # Prepare the input for the Step Function
        step_function_input = {
            "s3_event": event,  # Pass the entire S3 event structure
            "s3_bucket": s3_bucket,
            "s3_key": s3_key,
            "steps": [
                {
                    "source_table": "silver.tbl_healthcare_analytics_data",
                    "destination_table": "gold.dim_dates",
                    "merge_query": "CALL silver.sp_populate_dim_dates();"
                },
                {
                    "source_table": "silver.tbl_healthcare_analytics_data",
                    "destination_table": "gold.dim_times",
                    "merge_query": "CALL silver.sp_populate_dim_times();"
                },
                {
                    "source_table": "silver.tbl_healthcare_analytics_data",
                    "destination_table": "gold.dim_treatment_types_and_outcome_statuses",
                    "merge_query": "CALL silver.sp_populate_dim_treatment_types_and_outcome_statuses();"
                },
                {
                    "source_table": "silver.tbl_healthcare_analytics_data",
                    "destination_table": "gold.dim_locations",
                    "merge_query": "CALL silver.sp_populate_dim_locations();"
                },
                {
                    "source_table": "silver.tbl_healthcare_analytics_data",
                    "destination_table": "gold.dim_specialities",
                    "merge_query": "CALL silver.sp_populate_dim_specialities();"
                },
                {
                    "source_table": "silver.tbl_healthcare_analytics_data",
                    "destination_table": "gold.dim_providers",
                    "merge_query": "CALL silver.sp_populate_dim_providers();"
                },
                {
                    "source_table": "silver.tbl_healthcare_analytics_data",
                    "destination_table": "gold.dim_patients",
                    "merge_query": "CALL silver.sp_populate_dim_patients();"
                },
                {
                    "source_table": "silver.tbl_healthcare_analytics_data",
                    "destination_table": "gold.dim_diseases",
                    "merge_query": "CALL silver.sp_populate_dim_diseases();"
                },
                {
                    "source_table": "gold.dim_dates, gold.dim_times, gold.dim_treatment_types_and_outcome_statuses, gold.dim_locations, gold.dim_specialities, gold.dim_providers, gold.dim_patients, gold.dim_diseases",
                    "destination_table": "gold.fact_treatments",
                    "merge_query": "CALL silver.sp_populate_fact_treatments();"
                }
            ]
        }
        
        # Log the Step Function input for verification
        logger.info(f"Step Function input: {json.dumps(step_function_input)}")

        # Start the Step Function execution
        logger.info(f"Invoking the StepFunction.")
        response = step_client.start_execution(
            stateMachineArn=step_function_arn,
            input=json.dumps(step_function_input)
        )
        
        # Log the response from Step Function execution
        logger.info(f"Step Function started successfully: {response['executionArn']}")
        
        # Return success response with execution ARN
        return {
            "status": "Step Function Started",
            "executionArn": response["executionArn"]
        }
    
    except KeyError as e:
        return {"error": f"Missing key: {e}"}
        
        # Return error response
        return {
            "status": "Error",
            "message": str(e)
        }