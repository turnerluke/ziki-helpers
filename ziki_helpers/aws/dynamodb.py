import datetime as dt

import boto3
from boto3.dynamodb.conditions import Key

# Get deserializer to change DynamoDB format to JSON
boto3.resource('dynamodb')
deserializer = boto3.dynamodb.types.TypeDeserializer()

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')


def get_entire_table(table):
    """
    Query an entire table in DynamoDB with the scan operation.
    :param table:
    :return:
    """
    if type(table) == str:
        table = dynamodb.Table(table)
    data = []
    response = table.scan()
    data.extend(response['Items'])
    while 'LastEvaluatedKey' in response:
        response = table.scan(
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        data.extend(response['Items'])
    return data


def query_on_business_date(table, date):
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


def query_between_business_dates(table_name, start_date, end_date):
    """
    Query either orders or labor on a range of business dates.
    Business date needs to be set as a secondary index for both of these.
    Business data must be passed in as a number in the format YYYYMMDD.

    :param table_name: str, "orders" or "labor"
    :param start_date: dt.date
    :param end_date: dt.date
    :return:
    """
    date_range = [int((start_date + dt.timedelta(days=days)).strftime('%Y%m%d')) for days in
                  range(int((end_date - start_date) / dt.timedelta(days=1)) + 1)]
    data = []
    table = dynamodb.Table(table_name)
    for date in date_range:
        data.extend(query_on_business_date(table, date))
    return data


if __name__ == '__main__':
    start = dt.datetime(2023, 1, 1)
    end = dt.datetime(2023, 5, 24)
    data = query_on_business_date('labor', start, end)
