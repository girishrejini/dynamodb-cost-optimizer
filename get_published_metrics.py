import boto3
import csv
import time
import os

def get_published_metrics(params):
    
    # Create Athena client
    client = boto3.client('athena')
    
    # Execute the query
    start_query_execution_response = client.start_query_execution(
        QueryString = params['query'],
        QueryExecutionContext = {
            'Database' : params['database']
        },
        ResultConfiguration = {
            'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']
        },
    )

    # Extract the query execution id
    query_execution_id = start_query_execution_response['QueryExecutionId']

    # Set starting/default value of the query execution status for the polling check (see While loop below)
    query_execution_status = 'RUNNING'

    # Set starting/default value of the query execution status check attempt. Keep track of total number of attempts made to check on query execution state (helpful for debugging purposes)
    query_execution_status_check_attempt = 1

    while (query_execution_status == 'RUNNING'):

        # Fetch query details
        query_results_response = client.get_query_execution(
            QueryExecutionId = query_execution_id
        )

        # Extract the query execution state
        query_execution_status = query_results_response['QueryExecution']['Status']['State']

        print ('Attempt ' +  str(query_execution_status_check_attempt) + ': Current status of query - ' + query_execution_status)

        if (query_execution_status == 'FAILED') or (query_execution_status == 'CANCELLED') :

            #TODO: Maybe return null? Have to keep it consistent to make it easy for the caller to handle success and unsuccessful results
            return query_execution_status
        elif query_execution_status == 'SUCCEEDED':
            
            # Query execution succeeded. 
            
            # Extract the output location
            query_results_output_location = query_results_response['QueryExecution']['ResultConfiguration']['OutputLocation']

            # Fetch the actual query results
            query_results = client.get_query_results(
                QueryExecutionId = query_execution_id
            )

            # Extract the result set from the query results
            query_results_result_set = query_results['ResultSet']

            # Return the output location and the result set
            return query_results_output_location, query_results_result_set 

        else:
            query_execution_status_check_attempt +=1