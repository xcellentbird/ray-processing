from dataclasses import asdict
from dataclasses import dataclass

import pyarrow as pa


@dataclass
class ImageAltData:
    image_url: str
    alt_text: str

    @classmethod
    def arrow_schema(cls):
        image_url_field = pa.field('image_url', pa.string())
        alt_text_field = pa.field('alt_text', pa.string())

        schema = pa.schema([image_url_field, alt_text_field])

        return schema

    def to_dict(self):
        return asdict(self)
