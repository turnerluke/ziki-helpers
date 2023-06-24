import os
import datetime as dt
import pytz
import json
import requests
from decimal import Decimal

import boto3
from toast_auth import ToastToken

import ziki_helpers.config.settings
from ziki_helpers.aws.s3 import read_from_s3, write_to_s3
from ziki_helpers.aws.dynamodb import get_entire_table

TOAST_API_SERVER = os.environ.get('TOAST_API_SERVER')

# Toast API endpoints
orders_url = f'{TOAST_API_SERVER}/orders/v2/ordersBulk'
labor_url = f'{TOAST_API_SERVER}/labor/v1/timeEntries'
dining_option_url = f'{TOAST_API_SERVER}/config/v2/diningOptions'

# Stores the last time orders were written to DynamoDB
S3_BUCKET = 'ziki-dataflow'


# This is the amount of time overlapped for writing entries by time.
# IE orders are wrote to Noon -> End time is (12:00)
# Next time orders are wrote, it'll start at end time - TIME_OVERLAP_BUFFER
# Eventually this should be decreased to 0, but for now it's 3 hours until we're sure about writing capabilities
TIME_OVERLAP_BUFFER = dt.timedelta(hours=3)

# Settings for toast_auth
os.environ['BUCKET_NAME'] = 'ziki-analytics-config'
os.environ['FILE_NAME'] = 'toast_token.json'


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


class ToastDataFlow:

    def __init__(self):
        self.locations = get_entire_table('locations')
        self.toast_token = ToastToken('s3')

    def write_orders_by_business_date(self, business_date: int) -> None:
        print(f"Business Date: {date_int_to_dashed_string(business_date)}")

        table = boto3.resource('dynamodb', region_name='us-east-1').Table('orders')
        with table.batch_writer() as batch:
            for location in self.locations:
                if not location['info'][0]['address']:  # Ignore guid placeholders for future locations
                    continue

                location_guid = location['guid']
                print("location: ", location['info'][-1]['id'])
                page = 1

                while True:
                    print("Page: ", page)
                    # Query the orders
                    query = {
                        "businessDate": business_date,
                        "page": str(page),
                        "pageSize": "100",
                    }
                    headers = {
                        **self.toast_token,
                        "Toast-Restaurant-External-ID": location_guid,
                    }

                    response = requests.get(orders_url, headers=headers, params=query).json()

                    print('Query Size: ', len(response))
                    if type(response) == dict:
                        print("Error Response Occurred: ")
                        print(response)
                        raise ValueError

                    if len(location['info']) == 1:
                        location_id = location['info'][0]['id']
                    else:
                        location_id = location_id_from_date(location['info'], business_date)

                    for order in response:
                        order['location'] = location_id
                        item = json.loads(json.dumps(order), parse_float=Decimal)
                        batch.put_item(
                            Item=item
                        )

                    if len(response) < 100:
                        break
                    else:
                        page += 1
                print("Done with location: ", location['info'][-1]['id'])
                print()

        print('Done')

    def write_yesterday_orders(self):
        yesterday = dt.date.today() - dt.timedelta(days=1)
        business_date = int(yesterday.strftime('%Y%m%d'))
        self.write_orders_by_business_date(business_date)

    def write_orders_by_date_range(self, start: dt.date, end: dt.date) -> None:
        dates = get_date_range(start, end)
        for date in dates:
            self.write_orders_by_business_date(date)

    def write_yesterday_labor(self):
        yesterday = dt.date.today() - dt.timedelta(days=1)
        business_date = int(yesterday.strftime('%Y%m%d'))

        self.write_labor_by_business_date(business_date)

    def write_labor_by_date_range(self, start: dt.date, end: dt.date) -> None:
        dates = get_date_range(start, end)
        for date in dates:
            self.write_labor_by_business_date(date)

    def write_labor_by_business_date(self, business_date: int):
        print(f"Business Date: {date_int_to_dashed_string(business_date)}")
        table = boto3.resource('dynamodb', region_name='us-east-1').Table('labor')

        for location in self.locations:
            if not location['info'][0]['address']:
                continue

            location_guid = location['guid']
            print("location: ", location['info'][-1]['id'])
            # Query the orders
            query = {
                "businessDate": business_date,
            }
            headers = {
                **self.toast_token,
                "Toast-Restaurant-External-ID": location_guid,
            }

            response = requests.get(labor_url, headers=headers, params=query).json()

            print('Query Size: ', len(response))
            if type(response) == dict:
                print("Error Response Occurred: ")
                print(response)
                raise ValueError

            if len(location['info']) == 1:
                location_id = location['info'][0]['id']
            else:
                location_id = location_id_from_date(location['info'], business_date)

            with table.batch_writer() as batch:
                for entry in response:
                    entry['location'] = location_id
                    entry['businessDate'] = int(entry['businessDate'])
                    item = json.loads(json.dumps(entry), parse_float=Decimal)
                    batch.put_item(
                        Item=item
                    )
            print("Done with location: ", location['info'][-1]['id'])

        print('Done')

    def write_orders_between_times(self, start: dt.datetime, end: dt.datetime) -> None:
        start = start.isoformat(timespec='milliseconds')
        end = end.isoformat(timespec='milliseconds')
        table = boto3.resource('dynamodb', region_name='us-east-1').Table('orders')

        for location in self.locations:
            if not location['info'][0]['address']:
                continue
            location_guid = location['guid']
            print("location: ", location['info'][-1]['id'])
            page = 1

            while True:

                print("Page: ", page)
                # Query the orders
                query = {
                    "startDate": start,
                    "endDate": end,
                    "page": str(page),
                    "pageSize": "100",
                }
                headers = {
                    **self.toast_token,
                    "Toast-Restaurant-External-ID": location_guid,
                }

                response = requests.get(orders_url, headers=headers, params=query).json()

                print('Query Size: ', len(response))
                if type(response) == dict:
                    print("Error Response Occurred: ")
                    print(response)
                    raise ValueError
                with table.batch_writer() as batch:
                    for order in response:
                        if len(location['info']) == 1:
                            location_id = location['info'][0]['id']
                        else:
                            location_id = location_id_from_date(location['info'], order['businessDate'])
                        order['location'] = location_id
                        item = json.loads(json.dumps(order), parse_float=Decimal)
                        batch.put_item(
                            Item=item
                        )

                if len(response) < 100:
                    break
                else:
                    page += 1
            print("Done with location: ", location['info'][-1]['id'])
            print()

        print('Done')

    def write_orders_to_now(self):
        # Get the last updated time from S3
        start = dt.datetime.fromisoformat(read_from_s3(S3_BUCKET, 'last_updated_time_orders.txt')) - TIME_OVERLAP_BUFFER
        end = pytz.timezone('US/Central').localize(dt.datetime.now())

        # Write the orders between the last updated time and now
        self.write_orders_between_times(start, end)

        # Write the last updated time to S3
        write_to_s3(S3_BUCKET, 'last_updated_time_orders.txt', end.isoformat(timespec='milliseconds'))

    def write_labor_between_times(self, start: dt.datetime, end: dt.datetime) -> None:
        table = boto3.resource('dynamodb', region_name='us-east-1').Table('labor')

        for location in self.locations:
            if not location['info'][0]['address']:
                continue
            time_window_start = start
            time_window_end = time_window_start
            location_guid = location['guid']
            print("location: ", location['info'][-1]['id'])

            while time_window_end < end:
                time_window_end = time_window_start + dt.timedelta(days=30)
                # Query time entries
                query = {
                    "modifiedStartDate": time_window_start.isoformat(timespec='milliseconds'),
                    "modifiedEndDate": time_window_end.isoformat(timespec='milliseconds'),
                }
                headers = {
                    **self.toast_token,
                    "Toast-Restaurant-External-ID": location_guid,
                }

                response = requests.get(labor_url, headers=headers, params=query).json()

                print('Query Size: ', len(response))
                if type(response) == dict:
                    print("Error Response Occurred: ")
                    print(response)
                    raise ValueError
                if response:
                    with table.batch_writer() as batch:
                        for entry in response:
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
                time_window_start += dt.timedelta(days=30)

            print("Done with location: ", location['info'][-1]['id'])
            print()

        print('Done')

    def write_labor_to_now(self):
        # Get the last updated time from S3
        start = dt.datetime.fromisoformat(read_from_s3(S3_BUCKET, 'last_updated_time_labor.txt')) - TIME_OVERLAP_BUFFER
        end = pytz.timezone('US/Central').localize(dt.datetime.now())

        # Write the labor between the last updated time and now
        self.write_labor_between_times(start, end)

        # Write the last updated time to S3
        write_to_s3(S3_BUCKET, 'last_updated_time_labor.txt', end.isoformat(timespec='milliseconds'))

    def update_mappings(self):
        # start = (dt.datetime.fromisoformat(read_from_s3(S3_BUCKET, 'last_updated_time_mappings.txt')) - \
        #          dt.timedelta(days=3)).isoformat(timespec='milliseconds')
        start = "2021-01-01T00:00:00.000+0000"
        for mapping_name in ['dining_options', 'alternate_payments', 'employees']:
            print("Mapping: ", mapping_name)
            table = boto3.resource('dynamodb', region_name='us-east-1').Table(mapping_name)
            match mapping_name:
                case "dining_options": url = f"{TOAST_API_SERVER}/config/v2/diningOptions"
                case "alternate_payments": url = f"{TOAST_API_SERVER}/config/v2/alternatePaymentTypes"
                case "employees": url = f"{TOAST_API_SERVER}/labor/v1/employees"
                case _: raise ValueError("Invalid mapping name")

            query = {
                "lastModified": start,
            }

            data = []
            for location in self.locations:
                if not location['info'][0]['address']:
                    continue
                location_guid = location['guid']
                headers = {
                    "Toast-Restaurant-External-ID": location_guid,
                    **self.toast_token
                }

                response = requests.get(url, headers=headers, params=query)
                assert response.status_code == 200, 'Request Failed'
                data.extend(response.json())

            data = remove_duplicates(data)

            print("Total number of items: ", len(data))
            # Write to dynamoDB
            with table.batch_writer() as batch:
                for item in data:
                    if item['guid'] is not None:
                        batch.put_item(
                            Item=json.loads(json.dumps(item), parse_float=Decimal)
                        )

            print("Done with mapping: ", mapping_name)
        write_to_s3(
            S3_BUCKET,
            'last_updated_time_mappings.txt',
            pytz.timezone('US/Central').localize(dt.datetime.now()).isoformat(timespec='milliseconds')
        )


if __name__ == '__main__':
    flow = ToastDataFlow()
    flow.write_yesterday_labor()
