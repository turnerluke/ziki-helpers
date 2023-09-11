import datetime as dt
from typing import Union, Any, List, Dict

import boto3
from boto3.dynamodb.conditions import Key

JSONType = Union[str, int, float, bool, None, Dict[str, Any], List[Any]]

# Get deserializer to change DynamoDB format to JSON
boto3.resource('dynamodb')
deserializer = boto3.dynamodb.types.TypeDeserializer()

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')


def get_entire_table(table: Union[str, boto3.resource('dynamodb').Table]) -> JSONType:
    """
    Query an entire table in DynamoDB with the scan scripts.
    :param table:
    :return:
    """
    # Get the table if passed as a string
    if type(table) == str:
        table = dynamodb.Table(table)

    # Query the table
    data = []
    response = table.scan()
    data.extend(response['Items'])
    while 'LastEvaluatedKey' in response:
        response = table.scan(
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        data.extend(response['Items'])
    return data


def query_on_business_date(table: Union[str, boto3.resource('dynamodb').Table], date: int) -> JSONType:
    """
    Query either orders or labor on a range of business dates.
    Business date needs to be set as a secondary index for both of these.
    Business data must be passed in as a number in the format YYYYMMDD.

    :param table: str, "orders" or "labor"
    :param date: dt.date
    :return:
    """
    if type(table) == str:
        table = dynamodb.Table(table)

    data = []
    response = table.query(
        # Add the name of the index you want to use in your query.
        IndexName="businessDate-index",
        KeyConditionExpression=Key('businessDate').eq(date),
    )
    data.extend(response['Items'])
    while 'LastEvaluatedKey' in response:
        response = table.query(
            IndexName="businessDate-index",
            KeyConditionExpression=Key('businessDate').eq(date),
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        data.extend(response['Items'])

    return data


def query_between_business_dates(table: Union[str, boto3.resource('dynamodb').Table], start_date: dt.datetime, end_date: dt.datetime) -> JSONType:
    """
    Query either orders or labor on a range of business dates.
    Business date needs to be set as a secondary index for both of these.
    Business data must be passed in as a number in the format YYYYMMDD.

    :param table: str, "orders" or "labor"
    :param start_date: dt.date
    :param end_date: dt.date
    :return:
    """
    # Get a list of dates inclusive between start & end
    date_range = [int((start_date + dt.timedelta(days=days)).strftime('%Y%m%d')) for days in
                  range(int((end_date - start_date) / dt.timedelta(days=1)) + 1)]

    # Get the table, if the name is passed in as a string
    if type(table) == str:
        table = dynamodb.Table(table)

    # Get the data for each date in the range
    data = []
    for date in date_range:
        data.extend(query_on_business_date(table, date))
    return data
