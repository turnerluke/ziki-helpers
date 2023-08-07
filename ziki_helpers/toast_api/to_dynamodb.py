import os
import datetime as dt
from zoneinfo import ZoneInfo
import json
import requests
from decimal import Decimal
import calendar

import boto3
from toast_auth import ToastToken

import ziki_helpers.config.settings
from ziki_helpers.aws.s3 import read_from_s3, write_to_s3
from ziki_helpers.aws.dynamodb import get_entire_table
from ziki_helpers.toast_api.connector import ToastConnector

# Stores the last time orders were written to DynamoDB
S3_BUCKET = 'ziki-dataflow'


# This is the amount of time overlapped for writing entries by time.
# IE orders are wrote to Noon -> End time is (12:00)
# Next time orders are wrote, it'll start at end time - TIME_OVERLAP_BUFFER
# Eventually this should be decreased to 0, but for now it's 3 hours until we're sure about writing capabilities
TIME_OVERLAP_BUFFER = dt.timedelta(hours=4)
us_central_timezone = ZoneInfo("America/Chicago")


def get_current_time_given_timezone(timezone: ZoneInfo = us_central_timezone) -> dt.datetime:
    return dt.datetime.now(timezone)


def get_date_range(start_date: dt.date, end_date: dt.date) -> list[str]:
    """
    Get a list of dates in the format YYYYMMDD from a start and end date. Inclusive of start and end.
    :param start_date:
    :param end_date:
    :return:
    """
    date_range = [(start_date + dt.timedelta(days=days)).strftime('%Y%m%d') for days in
                  range(int((end_date - start_date) / dt.timedelta(days=1)) + 1)]
    return date_range


def to_hashable(obj):
    """Recursively convert a JSON object to a hashable representation."""
    if isinstance(obj, list):
        return tuple(to_hashable(item) for item in obj)
    elif isinstance(obj, dict):
        return tuple((key, to_hashable(value)) for key, value in obj.items())
    else:
        return obj


def remove_duplicates(json_list: list[dict]) -> list[dict]:
    """Remove duplicates from a list of JSON dictionaries."""
    seen = set()
    unique = []
    for json_dict in json_list:
        hashable_dict = to_hashable(json_dict)
        if hashable_dict not in seen:
            seen.add(hashable_dict)
            unique.append(json_dict)
    return unique


def date_int_to_dashed_string(date: int) -> str:
    """
    Convert a date in integer form (YYYYMMDD) to a dashed string (YYYY-MM-DD).
    """
    return dt.datetime.strptime(str(date), '%Y%m%d').strftime('%Y-%m-%d')


def location_id_from_date(location_info: list[dict], date: int) -> str:
    """

    :param location_info: dict

    :param date: form YYYYMMDD
    :return:
    """
    date = date_int_to_dashed_string(date)
    for loc in location_info:
        if loc['startDate'] is None or loc['startDate'] <= date:
            if loc['endDate'] is None or loc['endDate'] >= date:
                return loc['id']

    raise ValueError(f'No location found for date {date}.\nLocation info:\n{location_info}')


def get_start_and_end_of_last_month() -> tuple[dt.date, dt.date]:
    last_month = dt.date.today().replace(day=1) - dt.timedelta(days=1)
    start, end = get_month_start_and_end(last_month.year, last_month.month)
    return start, end


def get_month_start_and_end(year: int, month: int) -> tuple[dt.date, dt.date]:
    start = dt.date(year, month, 1)
    end = start.replace(
        day=calendar.monthrange(year, month)[1]
    )
    return start, end


def get_start_and_end_of_last_week() -> tuple[dt.date, dt.date]:
    """Get the Monday and Sunday of last week."""
    # Get last sunday
    sun = dt.date.today() - dt.timedelta(days=dt.date.today().weekday() + 1)
    # Get last monday
    mon = sun - dt.timedelta(days=6)

    return mon, sun


class ToastDataFlow(ToastConnector):

    def __init__(self):
        super().__init__()
        self.locations = get_entire_table('locations')

        # Change location ids to integers
        self.locations = [
            {
                **location,
                'info': [
                    {
                        **loc,
                        'id': int(loc['id']),
                    } for loc in location['info']
                ]
            } for location in self.locations
        ]

    def write_orders_by_business_date(self, business_date: int) -> None:
        print(f"Business Date: {date_int_to_dashed_string(business_date)}")

        table = boto3.resource('dynamodb', region_name='us-east-1').Table('orders')
        with table.batch_writer() as batch:
            for location in self.locations:
                if not location['info'][0]['address']:  # Ignore guid placeholders for future locations
                    continue
                if len(location['info']) == 1:
                    location_id = location['info'][0]['id']
                else:
                    location_id = location_id_from_date(location['info'], business_date)
                data = self.get_orders_by_business_date(business_date, location['guid'])
                for order in data:
                    order['location'] = location_id
                    item = json.loads(json.dumps(order), parse_float=Decimal)
                    batch.put_item(
                        Item=item
                    )
        print('Done')

    def write_yesterday_orders(self):
        yesterday = dt.date.today() - dt.timedelta(days=1)
        business_date = int(yesterday.strftime('%Y%m%d'))
        self.write_orders_by_business_date(business_date)

    def write_last_month_orders(self) -> None:
        start_date, end_date = get_start_and_end_of_last_month()
        self.write_orders_by_date_range(start_date, end_date)

    def write_last_week_orders(self) -> None:
        start_date, end_date = get_start_and_end_of_last_week()
        self.write_orders_by_date_range(start_date, end_date)

    def write_orders_by_date_range(self, start: dt.date, end: dt.date) -> None:
        dates = get_date_range(start, end)
        for date in dates:
            self.write_orders_by_business_date(date)

    def write_yesterday_labor(self) -> None:
        yesterday = dt.date.today() - dt.timedelta(days=1)
        business_date = int(yesterday.strftime('%Y%m%d'))

        self.write_labor_by_business_date(business_date)

    def write_last_month_labor(self) -> None:
        start_date, end_date = get_start_and_end_of_last_month()
        self.write_labor_by_date_range(start_date, end_date)

    def write_last_week_labor(self) -> None:
        start_date, end_date = get_start_and_end_of_last_week()
        self.write_labor_by_date_range(start_date, end_date)

    def write_labor_by_date_range(self, start: dt.date, end: dt.date) -> None:
        dates = get_date_range(start, end)
        for date in dates:
            self.write_labor_by_business_date(date)

    def write_labor_by_business_date(self, business_date: int) -> None:
        print(f"Business Date: {date_int_to_dashed_string(business_date)}")
        table = boto3.resource('dynamodb', region_name='us-east-1').Table('labor')

        for location in self.locations:
            if not location['info'][0]['address']:
                continue

            location_guid = location['guid']

            data = self.get_labor_by_business_date(business_date, location_guid)

            if len(location['info']) == 1:
                location_id = location['info'][0]['id']
            else:
                location_id = location_id_from_date(location['info'], business_date)

            with table.batch_writer() as batch:
                for entry in data:
                    entry['location'] = location_id
                    entry['businessDate'] = int(entry['businessDate'])
                    item = json.loads(json.dumps(entry), parse_float=Decimal)
                    batch.put_item(
                        Item=item
                    )

    def write_orders_between_times(self, start: dt.datetime, end: dt.datetime) -> None:

        table = boto3.resource('dynamodb', region_name='us-east-1').Table('orders')

        for location in self.locations:
            if not location['info'][0]['address']:
                continue
            location_guid = location['guid']
            print("location: ", location['info'][-1]['id'])

            data = self.get_orders_between_times(start, end, location_guid)
            with table.batch_writer() as batch:
                for order in data:
                    if len(location['info']) == 1:
                        location_id = location['info'][0]['id']
                    else:
                        location_id = location_id_from_date(location['info'], order['businessDate'])
                    order['location'] = location_id
                    item = json.loads(json.dumps(order), parse_float=Decimal)
                    batch.put_item(
                        Item=item
                    )

            print("Done with location: ", location['info'][-1]['id'])
            print()

        print('Done')

    def write_orders_to_now(self):
        # Get the last updated time from S3
        start = dt.datetime.fromisoformat(read_from_s3(S3_BUCKET, 'last_updated_time_orders.txt')) - TIME_OVERLAP_BUFFER
        end = get_current_time_given_timezone()

        # Write the orders between the last updated time and now
        self.write_orders_between_times(start, end)

        # Write the last updated time to S3
        write_to_s3(S3_BUCKET, 'last_updated_time_orders.txt', end.isoformat(timespec='milliseconds'))

    def write_labor_between_times(self, start: dt.datetime, end: dt.datetime) -> None:
        table = boto3.resource('dynamodb', region_name='us-east-1').Table('labor')

        for location in self.locations:
            if not location['info'][0]['address']:
                continue

            location_guid = location['guid']
            print("location: ", location['info'][-1]['id'])

            data = self.get_labor_between_times(start, end, location_guid)

            with table.batch_writer() as batch:
                for entry in data:
                    if len(location['info']) == 1:
                        location_id = location['info'][0]['id']
                    else:
                        location_id = location_id_from_date(location['info'], entry['businessDate'])
                    entry['location'] = location_id
                    entry['businessDate'] = int(entry['businessDate'])
                    item = json.loads(json.dumps(entry), parse_float=Decimal)
                    batch.put_item(
                        Item=item
                    )

            print("Done with location: ", location['info'][-1]['id'])
            print()

        print('Done')

    def write_labor_to_now(self):
        # Get the last updated time from S3
        start = dt.datetime.fromisoformat(read_from_s3(S3_BUCKET, 'last_updated_time_labor.txt')) - TIME_OVERLAP_BUFFER
        end = get_current_time_given_timezone()

        # Write the labor between the last updated time and now
        self.write_labor_between_times(start, end)

        # Write the last updated time to S3
        write_to_s3(S3_BUCKET, 'last_updated_time_labor.txt', end.isoformat(timespec='milliseconds'))

    def update_mappings(self):
        start = (dt.datetime.fromisoformat(read_from_s3(S3_BUCKET, 'last_updated_time_mappings.txt')) - \
                 dt.timedelta(days=3)).isoformat(timespec='milliseconds')
        # start = "2021-01-01T00:00:00.000+0000"

        for mapping, _type in [('dining_options', 'config'), ('alternate_payments', 'config'), ('employees', 'labor'), ('jobs', 'labor')]:
            table = boto3.resource('dynamodb', region_name='us-east-1').Table(mapping)
            data = []
            for location in self.locations:
                if not location['info'][0]['address']:
                    continue
                if _type == 'config':
                    response = self.get_config_mappings(mapping, location['guid'], start)
                elif _type == 'labor':
                    response = self.get_labor_mappings(mapping, location['guid'])
                data += response

            data = remove_duplicates(data)
            with table.batch_writer() as batch:
                for item in data:
                    if item['guid'] is not None:
                        batch.put_item(
                            Item=json.loads(json.dumps(item), parse_float=Decimal)
                        )
        now = get_current_time_given_timezone()
        write_to_s3(
            S3_BUCKET,
            'last_updated_time_mappings.txt',
            now.isoformat(timespec='milliseconds')
        )


if __name__ == '__main__':
    flow = ToastDataFlow()
    # flow.write_yesterday_orders()
    flow.write_orders_to_now()

    # start = dt.date(2023, 7 , 30)
    # end = dt.date(2023, 8, 3)
    # flow.write_orders_by_date_range(start, end)
    # flow.write_last_week_orders()
    # end = dt.datetime.now(us_central_timezone)
    # write_to_s3(S3_BUCKET, 'last_updated_time_orders.txt', end.isoformat(timespec='milliseconds'))
