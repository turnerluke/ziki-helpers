

import pandas as pd

import gspread
from pydrive.auth import GoogleAuth

from config.settings import GSPREAD_CREDENTIALS, CLIENT_SECRETS_FILE

# 'client_config_file' is the location of the 'client_secrets.json' file downloaded from the Google Drive API
GoogleAuth.DEFAULT_SETTINGS['client_config_file'] = CLIENT_SECRETS_FILE

pd.options.mode.chained_assignment = None  # default='warn'

gc = gspread.service_account(filename=GSPREAD_CREDENTIALS)


def get_gspreadsheet(ss_name: str) -> pd.DataFrame:
    spreadsheet = gc.open(ss_name)
    return spreadsheet


def get_gsheet_as_df(sheetname: str) -> pd.DataFrame:
    ss = get_gspreadsheet(sheetname)
    wkshts = ss.worksheets()
    dfs = [pd.DataFrame(wksht.get_all_records()) for wksht in wkshts]
    df = pd.concat(dfs, axis='index', ignore_index=True)
    return df


def get_worksheet_as_df(sheetname: str, worksheet: str) -> pd.DataFrame:
    ss = get_gspreadsheet(sheetname)
    wksht = ss.worksheet(worksheet)
    df = pd.DataFrame(wksht.get_all_records())
    return df


def get_all_spreadsheet_names():
    gc = gspread.service_account(filename=GSPREAD_CREDENTIALS)
    return [sheet['name'] for sheet in gc.list_spreadsheet_files()]


if __name__ == '__main__':
    print(get_all_spreadsheet_names())
