import json
import decimal
import warnings
from typing import Union

import pandas as pd

from ziki_helpers.aws.dynamodb import get_entire_table

warnings.simplefilter(action='ignore', category=FutureWarning)


class DecimalEncoder(json.JSONEncoder):
    """Helper class to convert DataFrame column of JSON to strings."""
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)


def date_string_from_int(date_int: int) -> str:
    """Convert a date integer (YYYYMMDD) to a date string (YYYY-MM-DD)."""
    date_str = str(date_int)
    return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"


def get_dining_options_mapping() -> dict[str, str]:
    """Get a mapping of dining option GUIDs to names."""
    dining_options = get_entire_table('dining_options')
    return {option['guid']: option['name'] for option in dining_options}


def process_orders(orders: pd.DataFrame) -> Union[pd.DataFrame, None]:
    """Performs preliminary processing of orders."""
    # Get Necessary Mappings
    dining_options_mapping = get_dining_options_mapping()

    # Trim down to necessary columns
    orders = orders[
        ['location', 'businessDate', 'estimatedFulfillmentDate', 'guid', 'diningOption', 'checks', 'voided', 'deleted']
    ].copy()

    # Location to integer, comes through DBD as a decimal
    orders['location'] = orders['location'].astype(int)

    # Remove voided and deleted orders
    orders = orders.loc[
        ~orders['voided'] & ~orders['deleted']
        ]
    orders = orders.drop(columns=['voided', 'deleted'])

    if orders.empty or orders['diningOption'].apply(pd.Series).empty:
        return None

    # Map diningOption
    orders['diningOption'] = orders['diningOption'].apply(pd.Series)['guid']
    orders['diningOption'] = orders['diningOption'].replace(dining_options_mapping)

    # Remove Deferred Orders (Gift Cards)
    if not orders.loc[orders['diningOption'].isna()].empty:
        deferred_order_idx = orders.loc[
            orders['diningOption'].isna(),
            'checks'
        ].apply(pd.Series).stack().apply(pd.Series)[
            'selections'
        ].apply(pd.Series).stack().apply(pd.Series)[
            'deferred'
        ].index.get_level_values(0)
        orders = orders.drop(deferred_order_idx)

    return orders


def get_check_mask(checks: pd.DataFrame) -> pd.DataFrame:
    """Get a mask of valid checks."""
    def valid_check(check):
        if type(check) == dict:
            if check['voided'] | check['deleted']:
                return False
            return True
        return False

    check_mask = checks.applymap(valid_check)
    return check_mask


def get_no_checks_idxs(check_mask: pd.DataFrame) -> pd.Index:
    """Get the indices of orders with no checks."""
    no_checks_idxs = check_mask.loc[check_mask.sum(axis=1) == 0].index
    return no_checks_idxs


def get_check_paid_mask(checks: pd.DataFrame) -> pd.DataFrame:
    """Get a mask of checks that have been paid."""
    def check_paid(check):
        if type(check) == dict:
            return check['paymentStatus'] == 'PAID'
        return False

    check_paid_mask = checks.applymap(check_paid)
    return check_paid_mask


def keep_valid_payments(payments: pd.DataFrame) -> Union[pd.DataFrame, None]:
    """Keep only valid payments."""
    def payment_valid(payment):
        if type(payment) == dict:
            return payment['paymentStatus'] == 'CAPTURED'
        return False

    # Keep only CAPTURED payments
    payments_mask = payments.applymap(payment_valid)
    payments = payments.mask(~payments_mask).stack().apply(pd.Series)

    if payments.empty:
        return None
    return payments


def get_full_refund_payments(payments: pd.DataFrame) -> Union[pd.DataFrame, None]:
    """Get payments with full refunds."""
    full_refunds = payments.loc[payments['refundStatus'] == 'FULL']

    if full_refunds.empty:
        return None
    return full_refunds


def get_partial_refund_payments(payments: pd.DataFrame) -> Union[pd.DataFrame, None]:
    """Get payments with partial refunds."""
    partial_refunds = payments.loc[payments['refundStatus'] == 'PARTIAL']

    if partial_refunds.empty:
        return None
    return partial_refunds


def remove_voided_payments_from_orders_checks(orders: pd.DataFrame, checks: pd.DataFrame, payments: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Drop voided payments (already gone from payments)
    voided_payments_idx = set(orders.index) - set(payments.index)
    orders = orders.drop(voided_payments_idx)
    checks = checks.drop(voided_payments_idx)
    return orders, checks


def keep_one_valid_check_orders(orders: pd.DataFrame, checks: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Remove orders with no checks
    check_mask = get_check_mask(checks)
    no_checks_idxs = get_no_checks_idxs(check_mask)

    checks = checks.drop(no_checks_idxs)
    orders = orders.drop(no_checks_idxs)
    check_mask = check_mask.drop(no_checks_idxs)

    multi_check_idxs = check_mask.loc[check_mask.sum(axis=1) > 1].index

    # Process orders with multiple checks
    if not multi_check_idxs.empty:
        check_paid_mask = get_check_paid_mask(checks.loc[multi_check_idxs])
        assert (check_paid_mask.sum(axis=1) == 1).all(), "Multi valid checks without exactly one paid."
        check_mask.loc[multi_check_idxs] = check_paid_mask

    assert (check_mask.sum(axis=1) == 1).all(), 'Not exactly one valid check'

    checks = checks.mask(~check_mask).stack().droplevel(level=1).apply(pd.Series)

    return orders, checks


def get_gratuitites_from_checks(checks: pd.DataFrame) -> pd.Series:
    try:
        service_charges = checks['appliedServiceCharges'].apply(pd.Series).stack().apply(pd.Series)
        gratuities = service_charges.loc[service_charges['gratuity']]['chargeAmount'].groupby(level=0).sum()
    except KeyError:
        gratuities = pd.Series(0, index=checks.index)
    return gratuities


def remove_voided_selections(selections: pd.DataFrame, orders: pd.DataFrame, payments: pd.DataFrame) \
        -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    def non_voided_selection(selection):
        if type(selection) == dict:
            return not bool(selection['voided'])
        return False

    # Remove voided selections, trim to only valid
    selections_mask = selections.applymap(non_voided_selection)
    voided_selections = selections.loc[(~selections_mask).all(axis=1)].index
    selections = selections.mask(~selections_mask)

    # Drop where all selections are voided
    orders = orders.drop(voided_selections)
    #checks = checks.drop(voided_selections)
    payments = payments.drop(voided_selections)
    selections = selections.drop(voided_selections)
    return orders, payments, selections


def sales_and_payments_from_raw_order_data(data: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = pd.DataFrame(data)
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    orders = process_orders(df)
    if orders is None:
        return pd.DataFrame(), pd.DataFrame()

    # Get checks from orders
    checks = orders['checks'].apply(pd.Series)
    orders = orders.drop(columns=['checks'])

    # Keep only orders with exactly one valid check
    orders, checks = keep_one_valid_check_orders(orders, checks)

    # End if empty
    if orders.empty and checks.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Get gratuities from checks
    gratuities = get_gratuitites_from_checks(checks)

    # Trim checks to necessary columns
    checks = checks[
        ['payments', 'amount', 'totalAmount', 'selections', 'taxAmount']
    ]
    # TODO: Discounts from appliedDiscounts

    # Payments
    payments = checks['payments'].apply(pd.Series)
    # Remove bad payments
    payments = keep_valid_payments(payments)

    if payments is None:
        return pd.DataFrame(), pd.DataFrame()

    # Trim down to necessary columns
    payments = payments[
        ['originalProcessingFee', 'refundStatus', 'voidInfo', 'checkGuid', 'orderGuid', 'amount', 'tipAmount', 'guid', 'refund', 'paidBusinessDate']
    ]

    # Sanity check
    assert payments['voidInfo'].isna().all(), 'Voided Payments Remain'

    # Remove full refunds
    full_refunds = get_full_refund_payments(payments)
    if full_refunds is not None:
        # Remove orders, checks and payments with full refunds, as if they never happened
        refund_idx = full_refunds.index.get_level_values(0)

        orders = orders.drop(refund_idx)
        checks = checks.drop(refund_idx)
        payments = payments.drop(refund_idx)

    # Subtract partial refunds
    partial_refunds = get_partial_refund_payments(payments)
    if partial_refunds is not None:
        # Subtract the tip and amount from payments with partial refunds
        partial_refunds = partial_refunds['refund'].apply(pd.Series)[['tipRefundAmount', 'refundAmount']]
        payments.loc[partial_refunds.index.get_level_values(0), 'tipAmount'] -= partial_refunds['tipRefundAmount']
        payments.loc[partial_refunds.index.get_level_values(0), 'amount'] -= partial_refunds['refundAmount']

    # ???
    payments = pd.concat([
        payments[['originalProcessingFee', 'amount', 'tipAmount']].groupby(level=0).sum(),
        payments[['checkGuid', 'orderGuid', 'guid', 'paidBusinessDate']].groupby(level=0).head(1).droplevel(level=1)
    ], axis=1)

    # Add gratuity to payments
    payments = payments.join(gratuities.rename('gratuity'), how='left')
    payments['gratuity'] = payments['gratuity'].fillna(0)

    # Drop voided payments (already gone from payments)
    orders, checks = remove_voided_payments_from_orders_checks(orders, checks, payments)

    # End if empty
    if orders.empty and checks.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Selections
    selections = checks['selections'].apply(pd.Series)

    # End if empty
    if selections.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Remove voided selections
    if not selections.empty:
        orders, payments, selections = remove_voided_selections(selections, orders, payments)

    selections = selections.stack().apply(pd.Series)

    # Sanity Checks
    assert (~selections['deferred']).all(), "Deferred selections"
    assert selections['voidReason'].isna().all(), "Remaining Voids"
    assert (~selections['voided']).all(), "Remaining Voids"

    # Trim to relevant columns
    selections = selections[['preDiscountPrice', 'displayName', 'modifiers', 'quantity', 'tax']]

    # TODO: Someday analyze discounts
    # discounts = selections['appliedDiscounts'].apply(pd.Series).stack().apply(pd.Series)

    # Modifiers
    mods = selections['modifiers'].apply(pd.Series).stack().apply(pd.Series)

    # Sanity checks
    if not mods.empty:
        assert (~mods['deferred']).all(), 'Deferred modifiers'
        assert mods['voidReason'].isna().all(), 'Voided Modifiers'
        assert (~mods['voided']).all(), 'Voided modifiers'
        # assert (~mods['modifiers'].astype(bool)).all(), 'Modified modifiers'
        # TODO: Handle modified modifiers
        # Split the "modifiers" that are actually just new items out into the items df
        # Adjust the cost of each

        # Remove request messages
        mods = mods.loc[mods['selectionType'] != 'SPECIAL_REQUEST']
        # Trim to relevant columns
        mods = mods[['preDiscountPrice', 'displayName', 'quantity']]
        # Return the modifiers to a list of concise dictionaries
        mods = mods.apply(dict, axis=1).unstack().apply(lambda x: [val for val in x if not pd.isnull(val)], axis=1)

        # Add mods back to their selection, fill nulls with [] and dump to json
        selections['modifiers'] = mods
        selections['modifiers'] = selections['modifiers'].apply(lambda d: d if isinstance(d, list) else [])
        selections['modifiers'] = selections['modifiers'].apply(lambda x: json.dumps(x, cls=DecimalEncoder))
    else:
        selections['modifiers'] = selections['modifiers'].apply(lambda x: json.dumps([], cls=DecimalEncoder))

    # Merge selections and modifiers
    sales = selections.reset_index().merge(
        orders.reset_index(),
        left_on='level_0',
        right_on='index',
        how='inner',
        suffixes=('_selections', '_orders')
    )

    # Drop unnecessary columns
    sales = sales.drop(columns=['level_0', 'level_1', 'index'])

    # Better column names
    sales = sales.rename(
        columns=
        {
            'preDiscountPrice': 'gross',
            'displayName': 'item',
        }
    )

    # Add locations back to payments
    payments['location'] = orders['location']

    # Change businessDate integer to YYYY-MM-DD format
    sales['businessDate'] = sales['businessDate'].apply(date_string_from_int)
    payments['paidBusinessDate'] = payments['paidBusinessDate'].apply(date_string_from_int)

    # Decimals
    sales_decimal_cols = ['gross', 'tax']
    for col in sales_decimal_cols:
        sales[col] = sales[col].apply(lambda x: decimal.Decimal(x).quantize(decimal.Decimal('0.00')))
    payments_decimal_cols = ['amount', 'tipAmount', 'originalProcessingFee', 'gratuity', 'originalProcessingFee']
    for col in payments_decimal_cols:
        payments[col] = payments[col].apply(lambda x: decimal.Decimal(x).quantize(decimal.Decimal('0.00')))

    return sales, payments

#
# if __name__ == '__main__':
#     # Get some data
#     import datetime as dt
#     # Add parent directory to path
#     import sys
#     sys.path.append('..')
#     from aws.dynamodb import query_between_business_dates
#
#     table_name = 'orders'
#     start_date = dt.date(2022, 2, 2)
#     end_date = dt.date(2022, 2, 2)
#     data = query_between_business_dates(table_name, start_date, end_date)
#
#     sales, payments = sales_and_payments_from_raw_order_data(data)
#     print(data)