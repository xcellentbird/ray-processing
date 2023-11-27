import asyncio
from collections import defaultdict
from io import BytesIO
from typing import Dict

import numpy as np
import ray
from PIL import Image
from aiohttp import ClientSession

from attr_tagging.units import get_image_url_suffix, get_image_size, image_exists, contains_badwords


class BatchMapper:
    mappers = {
        'image_exists': image_exists,
        'image_extension': get_image_url_suffix,
        'image_size': get_image_size,
        'contains_badwords': contains_badwords
    }

    @staticmethod
    async def read_image(image_url):
        async with ClientSession() as session:
            try:
                async with session.get(image_url) as response:
                    if response.status == 200:
                        image_data = await response.read()
                        return Image.open(BytesIO(image_data))
            except Exception as e:
                pass
        return None

    @classmethod
    def batch_map(cls, batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        image_urls = batch['image_url']
        alt_texts = batch['alt_text']

        loop = asyncio.get_event_loop()
        images = loop.run_until_complete(
            asyncio.gather(*(cls.read_image(url) for url in image_urls))
        )

        new_columns = defaultdict(list)
        for image, image_url, alt_text in zip(images, image_urls, alt_texts):
            for col_name, mapper in cls.mappers.items():
                new_columns[col_name].append(mapper(image, image_url, alt_text))

        batch.update(new_columns)

        return batch


class AttrTagProcess:
    @property
    def batch_mapper(self):
        return BatchMapper.batch_map

    def execute(self, input_dataset_path, save_path):
        ds = ray.data.read_parquet(input_dataset_path)
        tagged_ds = ds.map_batches(self.batch_mapper, batch_size=20)
        tagged_ds.write_parquet(save_path)
