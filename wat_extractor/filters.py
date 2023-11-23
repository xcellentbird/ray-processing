from io import BytesIO
import requests
from PIL import Image


def is_in_alt(_link):
    if _link.get('alt'):
        return True
    else:
        return False


def is_img_src(_link):
    return _link['path'] == 'IMG@/src'


def is_valid_url(_link):
    return _link['url'].startswith('http')
