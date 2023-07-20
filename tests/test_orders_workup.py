import pickle

from ziki_helpers.toast_data.orders import sales_and_payments_from_raw_order_data


def test_empty_order_workup():
    data = []
    sales, payments = sales_and_payments_from_raw_order_data(data)
    assert sales.empty
    assert payments.empty


def test_order_workup():
    # Unpickle raw data
    data = pickle.load(open('data/orders.pkl', 'rb'))
    # Workup
    sales, payments = sales_and_payments_from_raw_order_data(data)

    assert len(sales) == 2331
    assert len(payments) == 1240
