
import gslides


from .auth import get_authenticated_google_credentials

gslides.initialize_credentials(get_authenticated_google_credentials())


def get_presentation_from_id(slides_id: str) -> gslides.Presentation:
    """
    Initialize Google Slides from a presentation ID.

    :param slides_id: Google Slides presentation ID
    :return: Google Slides presentation
    """
    return gslides.Presentation(slides_id)


def fill_presentation_template(presentation: gslides.Presentation, data: dict) -> None:
    """
    Fill a Google Slides presentation with data.

    :param presentation: Google Slides presentation
    :param data: data to fill presentation with
    :return:
    """
    presentation.template(mapping=data)