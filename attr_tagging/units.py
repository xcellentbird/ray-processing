import functools
import logging
from io import BytesIO
from pathlib import Path

import requests


@functools.lru_cache()
def read_badwords_list():
    badwords_file_url = 'https://raw.githubusercontent.com/LDNOOBW/List-of-Dirty-Naughty-Obscene-and-Otherwise-Bad-Words/master/en'

    response = requests.get(badwords_file_url)
    if response.status_code == 200:
        badwords_list = BytesIO(response.content).read().decode('utf-8').splitlines()

        return badwords_list
    else:
        logging.warning(f'Badwords list could not be downloaded from {badwords_file_url}')

    return []


def validate_image_url(mapper):
    def wrapper(image, image_url, alt_text):
        if image is None:
            return None

        return mapper(image, image_url, alt_text)

    return wrapper


@validate_image_url
def get_image_url_suffix(image, image_url, alt_text):
    return Path(image_url).suffix


@validate_image_url
def get_image_size(image, image_url, alt_text):
    return image.shape


def image_exists(image, image_url, alt_text):
    return image is not None


def contains_badwords(image, image_url, alt_text):
    badwords = read_badwords_list()
    return any(badword in alt_text for badword in badwords)
