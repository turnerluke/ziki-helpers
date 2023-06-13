import json
import decimal
import warnings

import pandas as pd

from ziki_helpers.aws.dynamodb import get_entire_table

warnings.simplefilter(action='ignore', category=FutureWarning)


class DecimalEncoder(json.JSONEncoder):
    """Helper class to convert DataFrame column of JSON to strings."""
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)


def sales_and_payments_from_raw_order_data(data) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = pd.DataFrame(data)
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Get Necessary Mappings
    dining_options_mapping = {option['guid']: option['name'] for option in get_entire_table('dining_options')}

    # Trim down to necessary columns
    orders = df[
        ['location', 'businessDate', 'estimatedFulfillmentDate', 'guid', 'diningOption', 'checks', 'voided', 'deleted']
    ]

    # Remove voided and deleted orders
    orders = orders.loc[
        ~orders['voided'] & ~orders['deleted']
    ]
    orders = orders.drop(columns=['voided', 'deleted'])

    if orders.empty or orders['diningOption'].apply(pd.Series).empty:
        return pd.DataFrame(), pd.DataFrame()

    # Map diningOption
    orders['diningOption'] = orders['diningOption'].apply(pd.Series)['guid']
    orders['diningOption'] = orders['diningOption'].replace(dining_options_mapping)

    # Remove Deferred Orders (Gift Cards)
    if not orders.loc[orders['diningOption'].isna()].empty:
        deferred_order_idx = orders.loc[orders['diningOption'].isna()]['checks'].apply(pd.Series).stack().apply(pd.Series)['selections'].apply(pd.Series).stack().apply(pd.Series)['deferred'].index.get_level_values(0)
        orders = orders.drop(deferred_order_idx)

    # Checks
    checks = orders['checks'].apply(pd.Series)
    orders = orders.drop(columns=['checks'])

    def valid_check(check):
        if type(check) == dict:
            if check['voided'] | check['deleted']:
                return False
            return True
        return False

    check_mask = checks.applymap(valid_check)
    no_checks_idxs = check_mask.loc[check_mask.sum(axis=1) == 0].index
    checks = checks.drop(no_checks_idxs)
    orders = orders.drop(no_checks_idxs)
    check_mask = check_mask.drop(no_checks_idxs)

    def check_paid(check):
        if type(check) == dict:
            return check['paymentStatus'] == 'PAID'
        return False

    multi_check_idxs = check_mask.loc[check_mask.sum(axis=1) > 1].index

    if not multi_check_idxs.empty:
        paid_mask = checks.loc[multi_check_idxs].applymap(check_paid)
        assert (paid_mask.sum(axis=1) == 1).all(), "Multi valid checks without exactly one paid."
        check_mask.loc[multi_check_idxs] = paid_mask

    assert (check_mask.sum(axis=1) == 1).all(), 'Not exactly one valid check'

    checks = checks.mask(~check_mask).stack().droplevel(level=1).apply(pd.Series)  # Check index shared exactly with orders

    # Trim down to necessary columns
    checks = checks[['payments', 'amount', 'totalAmount', 'selections', 'taxAmount']]  # TODO: Discounts from appliedDiscounts

    # Payments
    payments = checks['payments'].apply(pd.Series)

    def payment_valid(payment):
        if type(payment) == dict:
            return payment['paymentStatus'] == 'CAPTURED'
        return False

    # Keep only CAPTURED payments
    payments_mask = payments.applymap(payment_valid)
    payments = payments.mask(~payments_mask).stack().apply(pd.Series)

    if payments.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Trim down to necessary columns
    payments = payments[['originalProcessingFee', 'refundStatus', 'voidInfo', 'checkGuid', 'orderGuid', 'amount', 'tipAmount', 'guid', 'refund', 'paidBusinessDate']]

    # Sanity check
    assert payments['voidInfo'].isna().all(), 'Voided Payments Remain'

    # Remove full refunds
    refund_idx = payments.loc[payments['refundStatus'] == 'FULL'].index.get_level_values(0)

    orders = orders.drop(refund_idx)
    checks = checks.drop(refund_idx)
    payments = payments.drop(refund_idx)

    # Subtract partial refunds
    if not payments.loc[payments['refundStatus'] == 'PARTIAL'].empty:
        partial_refunds = payments.loc[payments['refundStatus'] == 'PARTIAL']['refund'].apply(pd.Series)[['tipRefundAmount', 'refundAmount']]
        payments.loc[partial_refunds.index.get_level_values(0), 'tipAmount'] -= partial_refunds['tipRefundAmount']
        payments.loc[partial_refunds.index.get_level_values(0), 'amount'] -= partial_refunds['refundAmount']

    payments = pd.concat([
        payments[['originalProcessingFee', 'amount', 'tipAmount']].groupby(level=0).sum(),
        payments[['checkGuid', 'orderGuid', 'guid', 'paidBusinessDate']].groupby(level=0).head(1).droplevel(level=1)
    ], axis=1)

    # Drop voided payments (already gone from payments)
    voided_payments_idx = set(orders.index) - set(payments.index)
    orders = orders.drop(voided_payments_idx)
    checks = checks.drop(voided_payments_idx)

    # Selections
    selections = checks['selections'].apply(pd.Series)

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
        assert (~mods['modifiers'].astype(bool)).all(), 'Modified modifiers'

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

    # Change businessDate integer to datetime
    sales['businessDate'] = pd.to_datetime(sales['businessDate'], format='%Y%m%d')
    payments['paidBusinessDate'] = pd.to_datetime(payments['paidBusinessDate'], format='%Y%m%d')

    return sales, payments