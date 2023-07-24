import os
import datetime as dt
import pytz
import json
import requests
from decimal import Decimal
from typing import Any, Union
import re

import pandas as pd
import boto3
from toast_auth import ToastToken

import ziki_helpers.config.settings
from ziki_helpers.aws.s3 import read_from_s3, write_to_s3
from ziki_helpers.aws.dynamodb import get_entire_table


def is_iso_datetime(input_string):
    try:
        dt.datetime.fromisoformat(input_string)
        return True
    except ValueError:
        return False


def is_camel_case(input_str):
    return re.match(r'^[a-z]+(?:[A-Z][a-z]*)*$', input_str) is not None


def is_snake_case(input_str):
    return re.match(r'^[a-z]+(?:_[a-z]+)*$', input_str) is not None


def snake_case_to_camel_case(snake_str: str) -> str:
    words = snake_str.split('_')
    camel_case = words[0] + ''.join(word.capitalize() for word in words[1:])
    return camel_case


TOAST_API_SERVER = os.environ.get('TOAST_API_SERVER')

# Toast API endpoints
orders_url = f'{TOAST_API_SERVER}/orders/v2/ordersBulk'
labor_url = f'{TOAST_API_SERVER}/labor/v1/timeEntries'
config_url = f"{TOAST_API_SERVER}/config/v2"
dining_option_url = f'{TOAST_API_SERVER}/config/v2/diningOptions'
labor_base_url = f"{TOAST_API_SERVER}/labor/v1"
alternate_payments_url = f"{TOAST_API_SERVER}/config/v2/alternatePaymentTypes"
menu_url = f"{TOAST_API_SERVER}/menus/v2/menus"
menu_items_url = f"{TOAST_API_SERVER}/config/v2/menuItems"
inventory_url = f"{TOAST_API_SERVER}/stock/v1/inventory"
last_updated_url = f"{TOAST_API_SERVER}/menus/v2/metadata"


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


class ToastConnector:

    def __init__(self):
        self.toast_token = ToastToken('s3')
        self.location_guid = None
        self.menu_cache = dict()

    def get_orders_by_business_date(self, business_date: int, location_guid: Union[str, None] = None) -> list[dict[str, Any]]:
        data = []
        # Query the orders
        page = 1
        while True:
            print("Page: ", page)
            # Query the orders
            query = {
                "businessDate": business_date,
                "page": str(page),
                "pageSize": "100",
            }

            response = requests.get(orders_url, headers=self.headers(location_guid), params=query)
            assert response.status_code == 200, f"API call failed. Response.\n{response}"
            response = response.json()
            print('Query Size: ', len(response))

            data += response

            if len(response) < 100:
                break
            else:
                page += 1
        return data

    def get_labor_by_business_date(self, business_date: int, location_guid: Union[str, None] = None) -> list[dict[str, Any]]:
        # Query the labor
        query = {
            "businessDate": business_date,
        }

        response = requests.get(labor_url, headers=self.headers(location_guid), params=query)
        assert response.status_code == 200, f"API call failed. Response.\n{response}"

        return response.json()

    def get_orders_between_times(self, start: dt.datetime, end: dt.datetime, location_guid: Union[str, None] = None) -> list[dict[str, Any]]:
        data = []
        page = 1
        while True:
            print("Page: ", page)
            # Query the orders
            query = {
                "startDate": start.isoformat(timespec='milliseconds'),
                "endDate": end.isoformat(timespec='milliseconds'),
                "page": str(page),
                "pageSize": "100",
            }

            response = requests.get(orders_url, headers=self.headers(location_guid), params=query)
            assert response.status_code == 200, f"API call failed. Response.\n{response}"
            response = response.json()

            data += response
            if len(response) < 100:
                break
            else:
                page += 1
        return data

    def get_labor_between_times(self, start: dt.datetime, end: dt.datetime, location_guid: Union[str, None] = None) -> list[dict[str, Any]]:
        data = []
        # Because this query doesn't have pagination, we need to break it up into 30 day chunks
        # Initialize the time window
        time_window_start = start
        time_window_end = time_window_start

        while time_window_end < end:
            # Move the ending time window up by 30 days (the max allowed by toast)
            time_window_end = time_window_start + dt.timedelta(days=30)
            # Query time entries
            query = {
                "modifiedStartDate": time_window_start.isoformat(timespec='milliseconds'),
                "modifiedEndDate": time_window_end.isoformat(timespec='milliseconds'),
            }

            response = requests.get(labor_url, headers=self.headers(location_guid), params=query)
            assert response.status_code == 200, f"API call failed. Response.\n{response}"

            data += response.json()

            # Move the start time window to the end
            time_window_start += dt.timedelta(days=30)

        return data

    # def get_dining_options(self, location_guid: Union[str, None] = None, start_time: Union[str, dt.datetime, None] = None) -> list[dict[str, Any]]:
    #
    #     # # Preprocess start time
    #     # if start_time is None:
    #     #     start_time = "2021-01-01T00:00:00.000+0000"
    #     # else:
    #     #     if type(start_time) == dt.datetime:
    #     #         start_time = start_time.isoformat(timespec='milliseconds')
    #     #     elif type(start_time) == str:
    #     #         assert is_iso_datetime(start_time), f"Start time is not a valid ISO datetime string.\n{start_time}"
    #     #     else:
    #     #         raise TypeError(f"Start time must be a datetime object or ISO datetime string.\n{start_time}")
    #     # query = {
    #     #     "lastModified": start_time,
    #     # }
    #     response = requests.get(dining_option_url, headers=self.headers(location_guid), params=query)
    #     assert response.status_code == 200, f"API call failed. Response.\n{response}"
    #     data = response.json()
    #
    #     return data

    # def get_alternative_payments(self, location_guid: Union[str, None] = None, start_time: Union[str, dt.datetime, None] = None) -> list[dict[str, Any]]:
    #
    #     response = requests.get(alternate_payments_url, headers=self.headers(location_guid))
    #     assert response.status_code == 200, f"API call failed. Response.\n{response}"
    #     data = response.json()
    #
    #     return data

    def get_config_mappings(self, config: str, location_guid: Union[str, None] = None, start_time: Union[str, dt.datetime, None] = None) -> list[dict[str, Any]]:
        # Preprocess start time
        if start_time is None:
            start_time = "2021-01-01T00:00:00.000+0000"
        else:
            if type(start_time) == dt.datetime:
                start_time = start_time.isoformat(timespec='milliseconds')
            elif type(start_time) == str:
                assert is_iso_datetime(start_time), f"Start time is not a valid ISO datetime string.\n{start_time}"
            else:
                raise TypeError(f"Start time must be a datetime object or ISO datetime string.\n{start_time}")
        query = {
            "lastModified": start_time,
        }

        if is_snake_case(config):
            config = snake_case_to_camel_case(config)
        if config == 'alternatePayments':
            url = config_url + '/' + 'alternatePaymentTypes'  # Toast makes it impossible to maintain consistency
        else:
            url = config_url + '/' + config

        response = requests.get(url, headers=self.headers(location_guid), params=query)
        assert response.status_code == 200, f"API call failed. Response.\n{response}"
        data = response.json()
        return data

    def get_labor_mappings(self, labor_config, location_guid: Union[str, None] = None, ids: Union[list[str], None] = None) -> list[dict[str, Any]]:

        assert labor_config.islower(), "Labor config must be lowercase."
        if ids is not None:
            assert len(ids) <= 100, "Maximum of 100 IDs can be queried at once."
            query = {
                labor_config[:-1] + "Ids":  # Remove the 's' from the end of the config name (ie: employees -> employeeIds)
                    ",".join(ids),
            }
        else:
            query = {}

        url = labor_base_url + '/' + labor_config

        response = requests.get(url, headers=self.headers(location_guid), params=query)
        assert response.status_code == 200, f"API call failed. Response.\n{response}"
        data = response.json()
        return data

    def get_item_name_from_guid(self, item_guid: str, location_guid: Union[str, None] = None) -> str:
        # Get the menu item name from the item guid
        url = menu_items_url + '/' + item_guid
        response = requests.get(url, headers=self.headers(location_guid))
        assert response.status_code == 200, f"API call failed. Response.\n{response}"
        data = response.json()
        return data['name']

    def get_out_of_stock_guids(self, location_guid: Union[str, None] = None) -> list[str]:
        query = {"status": "OUT_OF_STOCK"}

        # Get the out of stock item guids
        response = requests.get(inventory_url, headers=self.headers(location_guid), params=query)
        assert response.status_code == 200, f"API call failed. Response.\n{response}"
        data = response.json()
        return list(set([item['guid'] for item in data]))

    def get_menu(self, location_guid: Union[str, None] = None) -> list[dict[str, Any]]:
        # Adjust location guid
        if location_guid is not None:
            self.location_guid = location_guid

        # Get menu from cache if it exists
        if self.location_guid in self.menu_cache:
            return self.menu_cache[location_guid]

        # Query menu API
        response = requests.get(menu_url, headers=self.headers(location_guid))
        assert response.status_code == 200, f"API call failed. Response.\n{response}"
        menu = response.json()

        # Add to menu cache
        self.menu_cache[location_guid] = menu

        return menu

    def item_guid_is_modifier(self, item_guid: str, location_guid: Union[str, None] = None) -> bool:
        if location_guid is not None:
            self.location_guid = location_guid

        # Get menu as DataFrame
        menu_df = pd.DataFrame([self.get_menu()])

        # Check if item guid is in modifier options
        mod_options = menu_df['modifierOptionReferences'].apply(pd.Series).stack().apply(pd.Series)
        mod_option_guids = mod_options['guid']
        return item_guid in mod_option_guids.values

    def get_modifier_info(self, item_guid: str, location_guid: Union[str, None] = None) -> dict:
        """
        Returns the modifier information for a given modifier GUID.
        :return:
        """
        if location_guid is not None:
            self.location_guid = location_guid

        menu_df = pd.DataFrame([self.get_menu()])

        mod_options = menu_df['modifierOptionReferences'].apply(pd.Series).stack().apply(pd.Series)
        mod_groups = menu_df['modifierGroupReferences'].apply(pd.Series).stack().apply(pd.Series)

        # Get modifier info
        mod_info = mod_options.loc[mod_options['guid'] == item_guid]
        mod_id = mod_info['referenceId'].values[0]
        mod_name = mod_info['name'].values[0]

        # Get modifier group info
        mod_group = mod_groups.loc[mod_groups['modifierOptionReferences'].apply(lambda x: mod_id in x)]
        mod_group_name = mod_group['name'].values[0]
        mod_group_id = mod_group['referenceId'].values[0]

        # Get menu items w/ modifier group
        menus = menu_df['menus'].apply(pd.Series).stack().apply(pd.Series)
        menu_groups = menus['menuGroups'].apply(pd.Series).stack().apply(pd.Series)
        menu_items = menu_groups['menuItems'].apply(pd.Series).stack().apply(pd.Series)
        out_of_order_items = menu_items.loc[menu_items['modifierGroupReferences'].apply(lambda x: mod_group_id in x)]
        menu_items = out_of_order_items['name'].tolist()

        return {'modifier_name': mod_name, 'modifier_group_name': mod_group_name, 'menu_items': menu_items}

    def headers(self, location_guid: Union[str, None] = None) -> dict[str, str]:
        # Adjust location guid if specified
        if location_guid is not None:
            self.location_guid = location_guid
        assert self.location_guid is not None, "Location GUID must be set before headers can be retrieved."

        # Return structured headers
        return {
            **self.toast_token,
            "Toast-Restaurant-External-ID": location_guid,
        }


if __name__ == '__main__':
    # Test the API
    conn = ToastConnector()
    location_guid = "1cc71734-609b-433d-828e-de40ce017f27"
    data = conn.get_labor_mappings("jobs", location_guid)
    print(data)