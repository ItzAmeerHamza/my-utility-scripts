import boto3

def lambda_handler(event, context):
    # Initialize the Glue client
    glue_client = boto3.client('glue')

    # *** IMPORTANT: Replace with the actual name of your Glue job ***
    glue_job_name = 'oneSignal_landtoUCT_internal'

    try:
        print(f"Starting Glue job: {glue_job_name}")

        # Start the Glue job
        response = glue_client.start_job_run(JobName=glue_job_name)

        job_run_id = response['JobRunId']
        print(f"Successfully started Glue job run with ID: {job_run_id}")

        return {
            'statusCode': 200,
            'body': f"Glue job {glue_job_name} started successfully with run ID {job_run_id}."
        }
    except Exception as e:
        print(f"Error starting Glue job: {e}")
        raise e
