import pandas as pd

from gspread import Spreadsheet
from gspread.exceptions import SpreadsheetNotFound

from .auth import get_authenticated_gspread_client

pd.options.mode.chained_assignment = None

gc = get_authenticated_gspread_client()


def get_gspreadsheet(ss_name: str) -> Spreadsheet:
    """Given a Google Sheet name, returns the sheet as a gspread object."""
    try:
        spreadsheet = gc.open(ss_name)
        return spreadsheet
    except SpreadsheetNotFound as e:
        raise ValueError(f"Spreadsheet: {ss_name} not found. \n"
                         f"Likely needs to be shared with: turner-gspread@ziki-analytics.iam.gserviceaccount.com")


def get_gsheet_as_df(sheetname: str) -> pd.DataFrame:
    """Given a Google Sheet name, returns the sheet as a pandas DataFrame. Concatenates all worksheets."""
    ss = get_gspreadsheet(sheetname)
    wkshts = ss.worksheets()
    dfs = [pd.DataFrame(wksht.get_all_records()) for wksht in wkshts]
    df = pd.concat(dfs, axis='index', ignore_index=True)
    return df


def get_worksheet_as_df(sheetname: str, worksheet: str) -> pd.DataFrame:
    """Given a Google Sheet name and worksheet name, returns the worksheet as a pandas DataFrame."""
    ss = get_gspreadsheet(sheetname)
    wksht = ss.worksheet(worksheet)
    df = pd.DataFrame(wksht.get_all_records())
    return df


def df_to_worksheet(df: pd.DataFrame, sheetname: str, worksheet: str, clear_old_data=True):
    """Given a pandas DataFrame, writes it to a Google Sheet."""
    ss = get_gspreadsheet(sheetname)
    wksht = ss.worksheet(worksheet)
    if clear_old_data:
        wksht.clear()
    wksht.update([df.columns.values.tolist()] + df.values.tolist())


def get_select_worksheets_as_df(sheetname: str, worksheets: list[str]) -> pd.DataFrame:
    """Given a Google Sheet name and worksheet names, returns the worksheets as a pandas DataFrame. Concatenates all worksheets."""
    ss = get_gspreadsheet(sheetname)
    wkshts = [ss.worksheet(worksheet) for worksheet in worksheets]
    dfs = [pd.DataFrame(wksht.get_all_records()) for wksht in wkshts]
    df = pd.concat(dfs, axis='index', ignore_index=True)
    return df


def get_all_spreadsheet_names():
    return [sheet['name'] for sheet in gc.list_spreadsheet_files()]
