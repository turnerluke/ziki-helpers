import json
import datetime as dt
import decimal

import pandas as pd

from ziki_helpers.aws.dynamodb import get_entire_table


def time_entries_and_start_dates_from_labor_data(data: list[dict], start_dates: pd.DataFrame = None) -> \
        tuple[pd.DataFrame, pd.DataFrame]:
    labor = pd.DataFrame(data)

    # Remove deleted
    if 'deleted' in labor.columns:
        labor = labor.loc[~labor['deleted']]
        if labor.empty:
            return pd.DataFrame(), start_dates

        # Sanity checks
        assert labor['deletedDate'].isna().all(), 'Deleted entries remain'
        assert (~labor['deleted']).all(), 'Deleted entries remain'
        assert labor['shiftReference'].isna().all(), 'Shift reference showed up.'

    # Trim to relevant columns
    labor = labor[
        ['location', 'employeeReference', 'jobReference', 'inDate', 'outDate', 'businessDate', 'hourlyWage',
         'regularHours', 'overtimeHours', 'guid']
    ]

    # Decimal Columns
    decimal_cols = ['hourlyWage', 'regularHours', 'overtimeHours']
    for col in decimal_cols:
        labor[col] = labor[col].fillna(0)
        labor[col] = labor[col].apply(lambda x: decimal.Decimal(x).quantize(decimal.Decimal('0.00')))

    # Unpack employee and job guids from reference objects
    labor['employeeGuid'] = labor['employeeReference'].apply(pd.Series)['guid']
    job_reference = labor['jobReference'].apply(pd.Series)
    if 'guid' in job_reference.columns:
        labor['jobGuid'] = job_reference['guid']
    else:
        labor['jobGuid'] = None
    # Fill nulls
    labor['jobGuid'] = labor['jobGuid'].fillna('')
    labor = labor.drop(columns=['employeeReference', 'jobReference'])

    # Get employee info
    employees = get_entire_table('employees')
    employees = pd.DataFrame(employees)
    employees = employees[
        ['guid', 'v2EmployeeGuid', 'chosenName', 'firstName', 'lastName', 'wageOverrides', 'jobReferences']
    ]

    # Merge employee info
    labor = labor.merge(
        employees[['guid', 'chosenName', 'firstName', 'lastName']],
        how='left',
        left_on='employeeGuid',
        right_on='guid',
        suffixes=('TimeEntry', 'Employee')
    )
    labor = labor.drop(columns=['guidEmployee'])
    labor['chosenName'] = labor['chosenName'].fillna('')
    # Strip whitespace
    labor['chosenName'] = labor['chosenName'].str.strip()
    labor['firstName'] = labor['firstName'].str.strip()
    labor['lastName'] = labor['lastName'].str.strip()


    # Calculate pay
    labor['regularPay'] = (
            labor['regularHours'] * labor['hourlyWage']
    ).apply(lambda x: decimal.Decimal(x).quantize(decimal.Decimal('0.00')))
    labor['overtimePay'] = (
            labor['overtimeHours'] * labor['hourlyWage'] * decimal.Decimal(1.5)
    ).apply(lambda x: decimal.Decimal(x).quantize(decimal.Decimal('0.00')))

    # Business date integer to datetime
    labor['businessDate'] = pd.to_datetime(labor['businessDate'], format='%Y%m%d')

    if start_dates is None:
        # Get start dates for each employee
        start_dates = labor[
            ['employeeGuid', 'businessDate']
        ].groupby('employeeGuid').min().reset_index().rename(
            columns={'businessDate': 'startDate'}
        )
    else:
        start_dates['startDate'] = pd.to_datetime(start_dates['startDate'], format='%Y-%m-%d')

    if (~labor['employeeGuid'].isin(start_dates['employeeGuid'].unique())).any():
        # Get start dates for each employee, not already in the table
        new_start_dates = labor.loc[
            ~labor['employeeGuid'].isin(start_dates['employeeGuid'].unique())
        ][
            ['employeeGuid', 'businessDate']
        ].groupby('employeeGuid').min().reset_index().rename(
            columns={'businessDate': 'startDate'}
        )
        start_dates = pd.concat([start_dates, new_start_dates], axis=0, ignore_index=True)

    # Get start dates for each employee
    labor = labor.merge(
        start_dates,
        how='left',
        on='employeeGuid'
    )

    # Denote training entries
    labor['isTraining'] = labor['businessDate'] < (labor['startDate'] + dt.timedelta(days=14))
    labor = labor.drop(columns=['startDate'])
    labor['isTraining'] = labor['isTraining'].astype(int)

    # Convert businessDate to str of YYYY-MM-DD format
    labor['businessDate'] = labor['businessDate'].dt.strftime('%Y-%m-%d')

    return labor, start_dates


if __name__ == '__main__':
    from ziki_helpers.aws.dynamodb import query_on_business_date
    data = query_on_business_date('labor', 20230709)
    labor, start_dates = time_entries_and_start_dates_from_labor_data(data)