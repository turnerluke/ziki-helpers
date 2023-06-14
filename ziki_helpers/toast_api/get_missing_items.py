# import requests
# from pprint import pprint
#
# from info.locations import location_guids, addresses
# from config.settings import TOAST_API_SERVER, TOAST_TOKEN
#
#
# def get_item_name_from_guid(item_guid: str, location_guid: str) -> str:
#     """
#     Returns the name of a menu item (or modifier) given its GUID and location GUID.
#     :param item_guid:
#     :param location_guid:
#     :return:
#     """
#     headers = {
#         "Toast-Restaurant-External-ID": location_guid,
#         **TOAST_TOKEN
#     }
#     url = f"{TOAST_API_SERVER}/config/v2/menuItems/" + item_guid
#
#     response = requests.get(url, headers=headers)
#     data = response.json()
#     return data['name']
#
#
# for location, address in zip(location_guids, addresses):
#     print("__"*20)
#     print("Location:    ", address)
#     headers = {
#       "Toast-Restaurant-External-ID": location,
#       **TOAST_TOKEN
#     }
#
#     # Get all guids of out-of-stock items
#     url = f"{TOAST_API_SERVER}/stock/v1/inventory"
#
#     query = {
#       "status": "OUT_OF_STOCK"
#     }
#     response = requests.get(url, headers=headers, params=query)
#     out_of_stock_data = response.json()
#     guids = [item['guid'] for item in out_of_stock_data]
#
#     # Checks for the GUIDs in menu items
#     for guid in guids:
#         name = get_item_name_from_guid(guid, location)
#
#         print("Out of stock GUID :    ", guid)
#         print("Name:             :    ", name)
#         print("\n")
#
