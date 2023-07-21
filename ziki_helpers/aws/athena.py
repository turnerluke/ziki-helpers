import time
from decimal import Decimal
from warnings import warn

import boto3
import pandas as pd

pd.set_option('display.max_columns', None)


athena_client = boto3.client('athena', region_name='us-east-1')


def repair_table(table_name: str, database: str) -> None:
    """
    Repair a table in Athena. Needs to be run when a table has folders added to S3 for a new entry in the schema:
    ie. a new year, month, day, etc.
    """
    repair_command = f"MSCK REPAIR TABLE {table_name}"
    query_athena_wait_for_success(repair_command, database, f's3://ziki-athena-query-results/repair-table-{table_name}/')


def query_athena_wait_for_success(query: str, database: str, output_location: str = None) -> str:
    # Start the query execution
    params = {'QueryString': query, 'QueryExecutionContext': {'Database': database}}
    if output_location is not None:
        params['ResultConfiguration'] = {'OutputLocation': output_location}
    response = athena_client.start_query_execution(
        **params
    )

    # Get the query execution ID
    query_execution_id = response['QueryExecutionId']
    while 'QueryExecution' not in response:
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200
        response = athena_client.get_query_execution(
            QueryExecutionId=query_execution_id
        )
        #return query_execution_id

    status = response['QueryExecution']['Status']['State']

    # Wait for the query to finish
    while status in ['QUEUED', 'RUNNING']:
        response = athena_client.get_query_execution(
            QueryExecutionId=query_execution_id
        )
        status = response['QueryExecution']['Status']['State']

        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break

        time.sleep(5)  # Wait for 5 seconds before checking the status again

    # Check if the query succeeded
    if status != 'SUCCEEDED':
        error_msg = response['QueryExecution']['Status'].get('StateChangeReason')
        raise Exception(f"Query execution failed.\nStatus: {status}\nError:\n{error_msg}")

    return query_execution_id


def get_athena_result_from_execution_id(query_execution_id):
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

    return query_results, columns, dtypes


def query_athena_and_get_results(query, database, output_location):
    warn("query_athena_and_get_results is deprecated. Use query_athena_get_results_as_df instead.")
    return query_athena_get_results_as_df(query, database, output_location)


def query_athena_get_results_as_df(
        query: str,
        database: str = 'ziki_analytics',
        output_location: str = 's3://ziki-athena-query-results/athena-results/'
) -> pd.DataFrame:
    """

    """
    query_execution_id = query_athena_wait_for_success(query, database, output_location)
    query_results, columns, dtypes = get_athena_result_from_execution_id(query_execution_id)

    # Create a Pandas DataFrame with the query results
    df = pd.DataFrame(query_results, columns=columns)
    print(df)

    # Convert column data types
    for col, dtype in zip(columns, dtypes):
        print(col, dtype)
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



if __name__ == '__main__':
    import datetime as dt
    last_sunday = dt.date.today() - dt.timedelta(days=dt.date.today().weekday() + 1)

    early_cutoff = last_sunday - dt.timedelta(days=4 * 7)

    # Get sales by location & week ending
    q = """
    WITH earliest_dates AS (
      SELECT
        location,
        MIN(CAST(CAST(year AS VARCHAR) || '-' || CAST(month AS VARCHAR) || '-' || CAST(day AS VARCHAR) AS DATE)) AS earliest_date
      FROM
        sales
      GROUP BY
        location
    ),
    sales_with_days_since_opening AS (
      SELECT
        s.location,
        SUM(s.gross) AS total_gross,
        date_diff('day', e.earliest_date, CAST(CAST(s.year AS VARCHAR) || '-' || CAST(s.month AS VARCHAR) || '-' || CAST(s.day AS VARCHAR) AS DATE)) AS days_since_opening
      FROM
        sales s
      JOIN
        earliest_dates e ON s.location = e.location
      GROUP BY
        s.location,
        e.earliest_date
    )
    SELECT
      location,
      total_gross,
      days_since_opening
    FROM
      sales_with_days_since_opening
    ORDER BY
      location;

    """

    database = 'ziki_analytics'
    s3_output = 's3://ziki-athena-query-results/athena-results/'

    df = query_athena_get_results_as_df(q, database, s3_output)

    print(df)