from typing import Union

from googleapiclient.discovery import build

import gslides
from gslides import Presentation

from .auth import get_authenticated_google_credentials

gslides.initialize_credentials(get_authenticated_google_credentials())


def get_presentation_from_id(slides_id: str) -> gslides.Presentation:
    """
    Initialize Google Slides from a presentation ID.

    :param slides_id: Google Slides presentation ID
    :return: Google Slides presentation
    """
    return Presentation.get(slides_id)


def fill_presentation_template(presentation: gslides.Presentation, data: dict) -> None:
    """
    Fill a Google Slides presentation with data.

    :param presentation: Google Slides presentation
    :param data: data to fill presentation with
    :return:
    """
    presentation.template(mapping=data)


def copy_slides_presentation_return_new_id(
        tempate_id: str,
        new_presentation_name: str,
        email='turner@ziki.kitchen'
        ) -> Union[str, None]:

    # Authenticate and create the API client
    credentials = get_authenticated_google_credentials()
    drive_service = build('drive', 'v3', credentials=credentials)

    # Copy the presentation using the Drive API
    drive_response = drive_service.files().copy(fileId=tempate_id, body={'name': new_presentation_name}).execute()
    new_presentation_id = drive_response['id']

    # Update the copied presentation's ownership and permissions
    drive_service.permissions().create(
        fileId=new_presentation_id,
        body={'type': 'user', 'role': 'writer', 'emailAddress': email}
    ).execute()
    return new_presentation_id
