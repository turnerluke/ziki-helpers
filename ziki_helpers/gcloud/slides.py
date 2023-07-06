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
    slides_service = build('slides', 'v1', credentials=credentials)
    drive_service = build('drive', 'v3', credentials=credentials)

    # Copy the presentation using the Drive API
    drive_response = drive_service.files().copy(fileId=tempate_id, body={'name': new_presentation_name}).execute()
    new_presentation_id = drive_response['id']

    # Update the copied presentation's ownership and permissions
    drive_service.permissions().create(
        fileId=new_presentation_id,
        body={'type': 'user', 'role': 'writer', 'emailAddress': 'turner@ziki.kitchen'}
    ).execute()

    # Get the slides from the original presentation
    presentation = slides_service.presentations().get(presentationId=tempate_id).execute()
    slides = presentation['slides']

    # Iterate over slides and copy the content to the new presentation
    for slide in slides:
        slide_id = slide['objectId']

        # Get the page elements from the original slide
        page_elements = slides_service.presentations().pages().get(
            presentationId=tempate_id,
            pageObjectId=slide_id
        ).execute()['pageElements']

        #Create a blank slide in the new presentation
        new_slide = slides_service.presentations().batchUpdate(
            presentationId=new_presentation_id,
            body={
                'requests': [{
                    'createSlide': {
                        'objectId': slide_id,
                        'insertionIndex': '0',
                        'slideLayoutReference': {
                            'predefinedLayout': 'BLANK'
                        }
                    }
                }]
            }
        ).execute()['replies'][0]['createSlide']['objectId']

        # Copy the content from the original slide to the new slide
        slides_service.presentations().pages().batchUpdate(
            presentationId=new_presentation_id,
            body={
                'requests': [{
                    'duplicateObject': {
                        'objectId': element['objectId'],
                        'objectIds': {slide_id: new_slide}
                    }
                } for element in page_elements]
            }
        ).execute()

    return new_presentation_id
