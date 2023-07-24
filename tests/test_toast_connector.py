from ziki_helpers.toast_api.connector import ToastConnector, is_iso_datetime, is_camel_case, is_snake_case, snake_case_to_camel_case
from ziki_helpers.aws.dynamodb import get_entire_table

import pandas as pd

conn = ToastConnector()

locations = get_entire_table('locations')
soco = [loc for loc in locations if
        loc['info'][0]['verboseName'] == 'South Congress'][0]
soco_guid = soco['guid']


def test_string_helpers():

    assert is_iso_datetime('2021-06-01T00:00:00-04:00')
    assert not is_iso_datetime('2021-13-01T00:00:00-04:00:00')

    assert is_camel_case('testString')
    assert not is_camel_case('TestString')
    assert not is_camel_case('test_string')

    assert is_snake_case('test_string')
    assert not is_snake_case('TestString')
    assert not is_snake_case('testString')

    assert snake_case_to_camel_case('test_string') == 'testString'


def test_get_orders_by_business_date():
    data = conn.get_orders_by_business_date(20220201, soco_guid)
    assert data, 'No data returned'
    assert len(data) > 10, 'Data returned is too small'
    df = pd.DataFrame(data)
    assert 'guid' in df.columns, 'guid column not in data'


def test_get_labor_by_business_date():

    data = conn.get_labor_by_business_date(20220301, soco_guid)

    assert data, 'No data returned'
    assert len(data) >= 1, 'Data returned is too small'
    df = pd.DataFrame(data)
    assert 'guid' in df.columns, 'guid column not in data'