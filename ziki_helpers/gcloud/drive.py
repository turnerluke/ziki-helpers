"""
local_drive_access.get.py

Container for all functions retrieving files from Google Drive.
"""
import os

import pandas as pd

from .auth import get_authenticated_pydrive_client

drive = get_authenticated_pydrive_client()


def get_csv_as_df_from_drive(f_title: str, drive_file_id: str, **kwargs) -> pd.DataFrame:
    """
    Given a csv's file name in Google Drive, returns the csv as a pandas DataFrame.
    The CSV must be in the 'Cleaned_Data' folder in Google Drive.

    :param f_title: string, name of csv file WITHOUT .csv ening
    :return: pandas.DataFrame of csv
    """
    f_title += '.csv'

    # Query all Google Drive files for exact title
    file_list = drive.ListFile(
        {'q': f"title = '{f_title}' and '{drive_file_id}' in parents and trashed = false"}
    ).GetList()

    assert len(file_list) == 1, \
        f"{len(file_list)} files found for file: {f_title}.\nFiles:\n{[f['title'] for f in file_list]}"

    f_id = file_list[0]['id']

    file = drive.CreateFile({'id': f_id})  # This doesn't create a file, instead it gets the file object given the 'id'
    file.GetContentFile(f_title)  # Downloads the file locally

    df = pd.read_csv(f_title, dtype=kwargs.get('dtype_dict'))

    # Delete the downloaded file
    os.remove(f_title)

    return df


def get_all_csvs_from_drive_folder(folder_id: str, **kwargs) -> pd.DataFrame:
    """
    Given a folder ID of a Google Drive folder, unpacks all files in that folder (Assumed to be .csvs),
    merges them into a single dataframe.
    :param folder_id:
    :return:
    """

    file_list =     drive.ListFile({'q': f"'{folder_id}' in parents and trashed = false"}).GetList()

    for file in file_list:
        # Download each file
        file.GetContentFile(file['title'])

    f_names = [file['title'] for file in file_list]

    dfs = [pd.read_csv(f_name, dtype=kwargs.get('dtype_dict'), encoding="ISO-8859-1") for f_name in f_names]
    df = pd.concat(dfs, axis='index', ignore_index=True)

    # Delete the downloaded files
    for f_name in f_names:
        os.remove(f_name)

    return df


def upload_to_drive(file_name: str, parent_folder_id: str) -> None:
    """
    Given a file name (accessible from path) and a parent folder id (in Google Drive) uploads the file
    :param file_name:
    :param parent_folder_id:
    :return:
    """
    # First check if a file with the same name already exists in the folder, if so delete it
    file_list = drive.ListFile({'q': f"'{parent_folder_id}' in parents and trashed=false"}).GetList()
    for file in file_list:
        if file['title'] == file_name:
            # Delete the existing file with the same name
            file.Delete()
            break
    # Create a new file and upload it
    gfile = drive.CreateFile({'parents': [{'id': parent_folder_id}]})
    gfile.SetContentFile(file_name)
    gfile.Upload()


if __name__ == '__main__':
    df = get_csv_as_df_from_drive('sales', '1zBrUiKQRm1nyWh_gn-KC8snqPCEugFNt')
    print(df.head())