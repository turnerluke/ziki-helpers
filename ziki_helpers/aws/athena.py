import time
from decimal import Decimal
from warnings import warn

import boto3
import pandas as pd

pd.set_option('display.max_columns', None)


athena_client = boto3.client('athena', region_name='us-east-1')

"""
########################################################################################################################
                                                    Helpers
########################################################################################################################
"""

def decimal_if_number(x):
    try:
        return Decimal(x)
    except:
        return Decimal(0)


def int_if_number(x):
    try:
        return int(x)
    except:
        return 0


"""
########################################################################################################################
                                                    Athena
########################################################################################################################
"""


def repair_table(table_name: str, database: str) -> None:
    """
    Repair a table in Athena. Needs to be run when a table has folders added to S3 for a new entry in the schema:
    ie. a new year, month, day, etc.
    """
    repair_command = f"MSCK REPAIR TABLE {table_name}"
    query_athena_wait_for_success(
        repair_command,
        database,
        f's3://ziki-athena-query-results/repair-table-{table_name}/'
    )


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

    status = response['QueryExecution']['Status']['State']

    # Wait for the query to finish
    while status in ['QUEUED', 'RUNNING']:
        response = athena_client.get_query_execution(
            QueryExecutionId=query_execution_id
        )
        status = response['QueryExecution']['Status']['State']

        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break

        time.sleep(3)  # Wait for 5 seconds before checking the status again

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

    # Convert column data types
    for col, dtype in zip(columns, dtypes):
        if dtype.startswith('varchar'):
            df[col] = df[col].astype(str)
        elif dtype in ['integer', 'tinyint', 'smallint', 'bigint', 'int']:
            df[col] = df[col].apply(int_if_number)
        elif dtype == 'boolean':
            df[col].apply(lambda x: x.strip().upper() == 'TRUE')
        elif dtype in ['float', 'double']:
            df[col] = df[col].astype(float)
        elif dtype == 'decimal':
            df[col] = df[col].apply(decimal_if_number)
        # Add more data type mappings as needed

    return df


# TODO: Implement this
# def athena2pandas(dtype: str, dtype_backend: Optional[str] = None) -> str:  # pylint: disable=too-many-return-statements
#     """Athena to Pandas data types conversion."""
#     dtype = dtype.lower()
#     if dtype == "tinyint":
#         return "Int8" if dtype_backend != "pyarrow" else "int8[pyarrow]"
#     if dtype == "smallint":
#         return "Int16" if dtype_backend != "pyarrow" else "int16[pyarrow]"
#     if dtype in ("int", "integer"):
#         return "Int32" if dtype_backend != "pyarrow" else "int32[pyarrow]"
#     if dtype == "bigint":
#         return "Int64" if dtype_backend != "pyarrow" else "int64[pyarrow]"
#     if dtype in ("float", "real"):
#         return "float32" if dtype_backend != "pyarrow" else "double[pyarrow]"
#     if dtype == "double":
#         return "float64" if dtype_backend != "pyarrow" else "double[pyarrow]"
#     if dtype == "boolean":
#         return "boolean" if dtype_backend != "pyarrow" else "bool[pyarrow]"
#     if (dtype == "string") or dtype.startswith("char") or dtype.startswith("varchar"):
#         return "string" if dtype_backend != "pyarrow" else "string[pyarrow]"
#     if dtype in ("timestamp", "timestamp with time zone"):
#         return "datetime64" if dtype_backend != "pyarrow" else "date64[pyarrow]"
#     if dtype == "date":
#         return "date" if dtype_backend != "pyarrow" else "date32[pyarrow]"
#     if dtype.startswith("decimal"):
#         return "decimal" if dtype_backend != "pyarrow" else "double[pyarrow]"
#     if dtype in ("binary", "varbinary"):
#         return "bytes" if dtype_backend != "pyarrow" else "binary[pyarrow]"
#     if dtype in ("array", "row", "map"):
#         return "object"
#     if dtype == "geometry":
#         return "string"
#     raise exceptions.UnsupportedType(f"Unsupported Athena type: {dtype}")




if __name__ == '__main__':
    q = """
        SELECT * FROM upsert_test_no_table
    """

    df = query_athena_get_results_as_df(q, database='toast-dev')