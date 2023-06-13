import requests
import time
from pprint import pprint
import json

from config.settings import TOAST_API_SERVER, TOAST_TOKEN


location_guids = [
    '09d274cf-eccd-43c5-a80e-3558b97ed801',
    '43d940ec-9949-4b12-97a9-dcfe52b8241b',
    '3b16bab1-fe74-44fa-89dc-512b74488c82',
    'b3bbbebb-22fc-4bee-b16a-202772cf1c1d',
    'cea1d8fb-5bd9-4277-9c22-cc379d6c6a26',
    '78789e4a-80bb-4fac-8936-2cc2ca9cc4cd',
    'e48cc058-f8ef-42ed-8487-53ceb869c582',
    'c2e94323-14ce-47a1-a835-ef54c91a7f6a',
    '1cc71734-609b-433d-828e-de40ce017f27',
    '844d1d01-3f00-42dd-9bea-bb5c11184c31',
    'a91edaab-b05a-4274-806f-44b6262b7f0b',
]


def query_orders_all(start_date: str = None, end_date: str = None) -> list:
    url = f'{TOAST_API_SERVER}/orders/v2/ordersBulk'
    output = []

    for location in location_guids:
        headers = {
            "Toast-Restaurant-External-ID": location,
            **TOAST_TOKEN,
        }
        page = 1
        while True:
            query = {
                "startDate": start_date,
                "endDate": end_date,
                "page": str(page),
                "pageSize": "100",
            }

            response = requests.get(url, headers=headers, params=query)
            data = response.json()
            output.extend(data)

            if len(data) < 100:
                break
            else:
                page += 1
    with open('orders.json', 'w') as file:
        json.dump(output, file, indent=4)


if __name__ == "__main__":
    #query_orders_all(
    #    start_date="2021-01-01T05:00:00.000-0600",
    #    end_date="2023-04-04T05:00:00.000-0600",
    #)
    print(TOAST_API_SERVER)