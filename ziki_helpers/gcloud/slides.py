
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


if __name__ == '__main__':
    p = get_presentation_from_id('1CnKLGE8eseP8vbT3f3uqEpksXEk5YC7277H4a6rX7SM')
    p.template(mapping={'test': 'SUCCESS'})