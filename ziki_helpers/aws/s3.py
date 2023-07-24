import os
import gzip
import json
import io
import datetime as dt

import pyarrow.parquet as pq
import pandas as pd
import boto3

s3 = boto3.client('s3')


def read_from_s3(bucket_name, file_name):
    # read the file
    obj = s3.get_object(Bucket=bucket_name, Key=file_name)
    file_content = obj['Body'].read()

    # Check if the file is compressed in Gzip format
    if is_gzip(file_content):
        # If the file is compressed in Gzip format, decompress it
        file_content = gzip.decompress(file_content)

    # Decode the file content as utf-8
    decoded_content = file_content.decode('utf-8')

    return decoded_content.strip()


def is_parquet(file_content):
    try:
        # Create a file-like object from the file content
        file_obj = io.BytesIO(file_content)

        # Attempt to open the Parquet file
        pq.ParquetFile(file_obj)
        return True
    except Exception:
        return False


def is_gzip(file_content):
    """
    Check if the file is compressed in Gzip format.
    :param file_content:
    :return:
    """
    return file_content[:2] == b'\x1f\x8b'


def write_to_s3(bucket_name, file_name, data):
    if type(data) != str:
        data = json.dumps(data)
    # upload the file
    s3.put_object(Body=data, Bucket=bucket_name, Key=file_name)


def save_df_as_csv(df, bucket_name, file_key):
    # Convert dataframe to CSV string
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)

    # Save the CSV string to S3 bucket
    response = s3.put_object(
        Body=csv_buffer.getvalue(),
        Bucket=bucket_name,
        Key=file_key
    )

    # Check if the file was successfully saved
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200, "File was not successfully saved to S3"


def save_df_as_parquet(df: pd.DataFrame, bucket_name: str, file_key: str, compression: str = 'snappy') -> None:
    # Convert dataframe to io string
    buffer = io.StringIO()

    # Save df to the buffer
    df.to_parquet(buffer, compression=compression, index=False)

    # Save the CSV string to S3 bucket
    response = s3.put_object(
        Body=buffer.getvalue(),
        Bucket=bucket_name,
        Key=file_key
    )

    # Check if the file was successfully saved
    assert response['ResponseMetadata']['HTTPStatusCode'] == 200, "File was not successfully saved to S3"


def get_csv_as_df(bucket_name, csv_path):
    """
    Read a CSV file from S3 and return a Pandas DataFrame.
    :param bucket_name: S3 bucket name
    :param csv_path: S3 path to the CSV file
    :return: Pandas DataFrame
    """
    obj = s3.get_object(Bucket=bucket_name, Key=csv_path)
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))

    return df


def get_parquet_as_df(bucket_name: str, parquet_path: str) -> pd.DataFrame:
    # Read the file
    obj = s3.get_object(Bucket=bucket_name, Key=parquet_path)
    file_content = obj['Body'].read()

    # Check if the file is in Parquet format
    if is_parquet(file_content):
        # Read the Parquet file
        table = pq.read_table(io.BytesIO(file_content))
        df = table.to_pandas()
        return df
    else:
        # File is not in Parquet format
        raise AssertionError("File is not in Parquet format")


def dataframe_to_s3_with_date_partition(df: pd.DataFrame, bucket_name: str, tablename: str, date: dt.date, filename='data') -> None:
    """
    Save a Pandas DataFrame to S3 with a date partition.
    :param df:
    :param bucket_name:
    :param tablename:
    :param date:
    :return:
    """
    # Unpack Date
    yr = date.year
    mo = date.month
    day = date.day

    # Save to S3, with year, month, day partitions
    filepath = f'{tablename}/year={yr}/month={mo}/day={day}/{filename}.parquet.gzip'
    save_df_as_parquet(df, bucket_name, filepath, compression='gzip')


def download_files_with_prefix(bucket_name: str, prefix: str) -> None:
    """
    Download all files with a certain prefix from an S3 bucket.
    :param bucket_name:
    :param prefix:
    :return:
    """
    # Get the list of files
    files = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)['Contents']

    # Download each file
    for file in files:
        s3.download_file(bucket_name, file['Key'], file['Key'])


if __name__ == '__main__':
    download_files_with_prefix('debug-dbd-stream-outputs', 'dynamodb_stream_event')