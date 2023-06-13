import time
from decimal import Decimal

import boto3
import pandas as pd

pd.set_option('display.max_columns', None)


athena_client = boto3.client('athena', region_name='us-east-1')


def query_athena_and_get_results(query, database, output_location):
    # Start the query execution
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': database
        },
        ResultConfiguration={
            'OutputLocation': output_location
        }
    )

    # Get the query execution ID
    query_execution_id = response['QueryExecutionId']

    # Wait for the query to finish
    while True:
        response = athena_client.get_query_execution(
            QueryExecutionId=query_execution_id
        )
        status = response['QueryExecution']['Status']['State']

        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break

        time.sleep(5)  # Wait for 5 seconds before checking the status again

    # Check if the query succeeded
    if status != 'SUCCEEDED':
        raise Exception(f"Query execution failed. Status: {status}. Response: {response}")

    # Get the first response page
    response = athena_client.get_query_results(
        QueryExecutionId=query_execution_id
    )

    columns = [col['Label'] for col in response['ResultSet']['ResultSetMetadata']['ColumnInfo']]
    dtypes = [col['Type'] for col in response['ResultSet']['ResultSetMetadata']['ColumnInfo']]

    # Retrieve the query results
    query_results = []
    for row in response['ResultSet']['Rows'][1:]:
        query_results.append([data['VarCharValue'] if 'VarCharValue' in data else '' for data in row['Data']])

    next_token = response.get('NextToken')

    # Repeat while there is next page token
    while next_token:
        response = athena_client.get_query_results(
            QueryExecutionId=query_execution_id,
            NextToken=next_token,
        )

        for row in response['ResultSet']['Rows'][1:]:
            query_results.append([data['VarCharValue'] if 'VarCharValue' in data else '' for data in row['Data']])

        # Check if there are more results available
        next_token = response.get('NextToken')

    # Create a Pandas DataFrame with the query results
    df = pd.DataFrame(query_results, columns=columns)

    # Convert column data types
    for col, dtype in zip(columns, dtypes):
        if dtype.startswith('varchar'):
            df[col] = df[col].astype(str)
        elif dtype == 'integer':
            df[col] = df[col].astype(int)
        elif dtype == 'boolean':
            df[col] = df[col].astype(bool)
        elif dtype == 'float':
            df[col] = df[col].astype(float)
        elif dtype == 'double':
            df[col] = df[col].astype(float)
        elif dtype == 'decimal':
           df[col] = df[col].apply(lambda x: Decimal(x))
        # Add more data type mappings as needed

    return df

# def query_athena_and_get_results(query, database, output_location):
#
#     # Start the query execution
#     response = athena_client.start_query_execution(
#         QueryString=query,
#         QueryExecutionContext={
#             'Database': database
#         },
#         ResultConfiguration={
#             'OutputLocation': output_location
#         }
#     )
#
#     # Get the query execution ID
#     query_execution_id = response['QueryExecutionId']
#
#     # Wait for the query to finish
#     while True:
#         response = athena_client.get_query_execution(
#             QueryExecutionId=query_execution_id
#         )
#         status = response['QueryExecution']['Status']['State']
#
#         if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
#             break
#
#         time.sleep(5)  # Wait for 5 seconds before checking the status again
#
#     # Check if the query succeeded
#     if status != 'SUCCEEDED':
#         raise Exception(f"Query execution failed. Status: {status}. Response: {response}")
#
#     # Retrieve the query results
#     response = athena_client.get_query_results(
#         QueryExecutionId=query_execution_id
#     )
#
#     # Parse the query results into a Pandas DataFrame
#     columns = [col['Label'] for col in response['ResultSet']['ResultSetMetadata']['ColumnInfo']]
#     rows = []
#
#     for row in response['ResultSet']['Rows'][1:]:
#         rows.append([data['VarCharValue'] if 'VarCharValue' in data else '' for data in row['Data']])
#
#     df = pd.DataFrame(rows, columns=columns)
#
#     return df


if __name__ == '__main__':
    # query = """
    # SELECT *
    # FROM "sales"
    # WHERE year = 2023 and month = 6 and day = 10
    # limit 10;
    # """
    # database = 'ziki_analytics'
    # s3_output = 's3://ziki-athena-query-results/athena-results/'
    # df = query_athena_and_get_results(query, database, s3_output)
    # print(df)

    query = """
    SELECT * 
    FROM "sales"
    ;
    """
    database = 'ziki_analytics'
    s3_output = 's3://ziki-athena-query-results/athena-results/'
    df = query_athena_and_get_results(query, database, s3_output)
    print(df)